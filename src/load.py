import structlog
import polars as pl
import pandas as pd
from sqlalchemy import create_engine
import io
import os

logger = structlog.get_logger()

SILVER_DIR = "/opt/airflow/data/silver"

def upsert_table(engine, df, table_name, schema, pk_columns):
    """Helper function to perform UPSERT into PostgreSQL using temp table and fast COPY."""
    if isinstance(df, pl.DataFrame):
        pdf = df.to_pandas()
    else:
        pdf = df.copy()

    temp_table = f"temp_{table_name}"
    
    cols = [f'"{c}"' for c in pdf.columns]
    cols_str = ", ".join(cols)
    pk_str = ", ".join([f'"{pk}"' for pk in pk_columns])
    
    non_pk_cols = [c for c in pdf.columns if c not in pk_columns]
    if non_pk_cols:
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols])
        on_conflict = f"ON CONFLICT ({pk_str}) DO UPDATE SET {update_str}"
    else:
        on_conflict = f"ON CONFLICT ({pk_str}) DO NOTHING"
        
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table_name} INCLUDING DEFAULTS);")
        
        buf = io.StringIO()
        pdf.to_csv(buf, index=False, na_rep="")
        buf.seek(0)
        
        cur.copy_expert(f"COPY {temp_table} ({cols_str}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')", buf)
        
        upsert_sql = f"""
        INSERT INTO {schema}.{table_name} ({cols_str})
        SELECT {cols_str} FROM {temp_table}
        {on_conflict};
        DROP TABLE IF EXISTS {temp_table};
        """
        cur.execute(upsert_sql)
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()

    logger.info(f"Upserted {len(pdf)} rows into {schema}.{table_name}")

def load_to_gold(**kwargs):
    """
    Load data from Silver layer to Gold layer (PostgreSQL Data Mart).
    Uses SQLAlchemy for UPSERT insertion.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    db_url = "postgresql+psycopg2://admin:password@datamart:5432/ecommerce"
    engine = create_engine(db_url)
    
    try:
        # Nạp các bảng Dimension trước
        log.info("Loading Dimension: dim_products...")
        products_df = pl.read_parquet(f"{SILVER_DIR}/dim_products.parquet")
        upsert_table(engine, products_df, "dim_products", "gold", ["product_id"])

        log.info("Loading Dimension: dim_sellers...")
        sellers_df = pl.read_parquet(f"{SILVER_DIR}/dim_sellers.parquet")
        upsert_table(engine, sellers_df, "dim_sellers", "gold", ["seller_id"])

        log.info("Loading Dimension: dim_customers...")
        customers_df = pl.read_parquet(f"{SILVER_DIR}/dim_customers.parquet")
        upsert_table(engine, customers_df, "dim_customers", "gold", ["customer_id"])

        # Nạp các bảng Fact sau
        log.info("Loading Fact: fct_order_items...")
        items_df = pl.read_parquet(f"{SILVER_DIR}/fct_order_items.parquet")
        items_pdf = items_df.to_pandas()
        
        # Cast các cột thời gian về datetime64[ns]
        date_cols = [
            "shipping_limit_date", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date", 
            "order_estimated_delivery_date"
        ]
        for c in date_cols:
            if c in items_pdf.columns:
                items_pdf[c] = pd.to_datetime(items_pdf[c], errors='coerce')
        
        # Lọc đảm bảo toàn vẹn khóa ngoại tham chiếu đến Dimension
        valid_prod_ids = set(products_df["product_id"].to_list())
        valid_seller_ids = set(sellers_df["seller_id"].to_list())
        valid_cust_ids = set(customers_df["customer_id"].to_list())
        
        items_pdf = items_pdf[
            items_pdf["product_id"].isin(valid_prod_ids) &
            items_pdf["seller_id"].isin(valid_seller_ids) &
            items_pdf["customer_id"].isin(valid_cust_ids)
        ]
        upsert_table(engine, items_pdf, "fct_order_items", "gold", ["order_id", "order_item_id"])

        log.info("Loading Fact: fct_order_payments...")
        payments_df = pl.read_parquet(f"{SILVER_DIR}/fct_order_payments.parquet")
        payments_pdf = payments_df.to_pandas()
        payments_pdf = payments_pdf[payments_pdf["customer_id"].isin(valid_cust_ids)]
        upsert_table(engine, payments_pdf, "fct_order_payments", "gold", ["order_id", "payment_sequential"])

        log.info("Loading Fact: fct_order_reviews...")
        reviews_df = pl.read_parquet(f"{SILVER_DIR}/fct_order_reviews.parquet")
        reviews_pdf = reviews_df.to_pandas()
        for c in ["review_creation_date", "review_answer_timestamp"]:
            if c in reviews_pdf.columns:
                reviews_pdf[c] = pd.to_datetime(reviews_pdf[c], errors='coerce')
        reviews_pdf = reviews_pdf[reviews_pdf["customer_id"].isin(valid_cust_ids)]
        upsert_table(engine, reviews_pdf, "fct_order_reviews", "gold", ["review_id", "order_id"])
        
        log.info("load_to_gold_success")
                 
    except Exception as e:
        log.exception("load_to_gold_failed")
        raise e

if __name__ == "__main__":
    load_to_gold()