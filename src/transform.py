import structlog
import polars as pl
import os
from pathlib import Path

logger = structlog.get_logger()

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
DLQ_DIR = "/opt/airflow/data/dlq"

def clean_to_silver(**kwargs):
    """
    Clean data from Bronze layer (Parquet) and load into Silver layer (Parquet).
    Implement Data Contracts and DLQ for invalid data.
    """
    run_id = kwargs.get('dag_run', None)
    log = logger.bind(run_id=run_id.run_id if run_id else "manual")
    
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(DLQ_DIR, exist_ok=True)
    
    try:
        # Đọc các file parquet từ BRONZE_DIR
        customers_df = pl.read_parquet(f"{BRONZE_DIR}/customers_dataset.parquet")
        orders_df = pl.read_parquet(f"{BRONZE_DIR}/orders_dataset.parquet")
        order_items_df = pl.read_parquet(f"{BRONZE_DIR}/order_items_dataset.parquet")
        products_df = pl.read_parquet(f"{BRONZE_DIR}/products_dataset.parquet")
        sellers_df = pl.read_parquet(f"{BRONZE_DIR}/sellers_dataset.parquet")
        geolocation_df = pl.read_parquet(f"{BRONZE_DIR}/geolocation_dataset.parquet")
        payments_df = pl.read_parquet(f"{BRONZE_DIR}/order_payments_dataset.parquet")
        reviews_df = pl.read_parquet(f"{BRONZE_DIR}/order_reviews_dataset.parquet")
        translation_df = pl.read_parquet(f"{BRONZE_DIR}/product_category_name_translation.parquet")

        # Chuẩn bị bảng geolocation tổng hợp theo zip code prefix
        geo_agg = (
            geolocation_df
            .with_columns(pl.col("geolocation_zip_code_prefix").cast(pl.String))
            .group_by("geolocation_zip_code_prefix")
            .agg([
                pl.col("geolocation_lat").mean().alias("lat"),
                pl.col("geolocation_lng").mean().alias("lng"),
            ])
        )

        # 1. Clean Customers
        log.info("Cleaning Customers...")
        dim_customers = (
            customers_df
            .unique(subset=["customer_id"])
            .with_columns([
                pl.col("customer_zip_code_prefix").cast(pl.String),
                pl.col("customer_city").str.to_lowercase(),
                pl.col("customer_state").str.to_uppercase(),
            ])
            .join(
                geo_agg,
                left_on="customer_zip_code_prefix",
                right_on="geolocation_zip_code_prefix",
                how="left"
            )
            .rename({"lat": "customer_lat", "lng": "customer_lng"})
            .select([
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
                "customer_lat",
                "customer_lng",
            ])
        )
        dim_customers.write_parquet(f"{SILVER_DIR}/dim_customers.parquet")
        log.info(f"dim_customers saved: {dim_customers.shape[0]} rows")

        # 2. Clean Sellers
        log.info("Cleaning Sellers...")
        dim_sellers = (
            sellers_df
            .filter(pl.col("seller_id").is_not_null())
            .unique(subset=["seller_id"])
            .with_columns([
                pl.col("seller_zip_code_prefix").cast(pl.String),
                pl.col("seller_city").str.to_lowercase(),
                pl.col("seller_state").str.to_uppercase(),
            ])
            .join(
                geo_agg,
                left_on="seller_zip_code_prefix",
                right_on="geolocation_zip_code_prefix",
                how="left"
            )
            .rename({"lat": "seller_lat", "lng": "seller_lng"})
            .select([
                "seller_id",
                "seller_zip_code_prefix",
                "seller_city",
                "seller_state",
                "seller_lat",
                "seller_lng",
            ])
        )
        dim_sellers.write_parquet(f"{SILVER_DIR}/dim_sellers.parquet")
        log.info(f"dim_sellers saved: {dim_sellers.shape[0]} rows")

        # 3. Clean Products
        log.info("Cleaning Products...")
        # Đổi tên cột lỗi chính tả (lenght -> length)
        rename_map = {}
        for old, new in [("product_name_lenght", "product_name_length"), 
                         ("product_description_lenght", "product_description_length")]:
            if old in products_df.columns:
                rename_map[old] = new
        
        products_renamed = products_df.rename(rename_map) if rename_map else products_df

        # Điền null bằng forward fill và join với bản dịch tiếng Anh
        dim_products = (
            products_renamed
            .unique(subset=["product_id"])
            .fill_null(strategy="forward")
            .join(
                translation_df,
                on="product_category_name",
                how="left"
            )
            .with_columns(
                pl.coalesce(["product_category_name_english", "product_category_name"]).alias("product_category_name_english")
            )
            .select([
                "product_id",
                "product_category_name_english",
                "product_name_length",
                "product_description_length",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ])
        )
        dim_products.write_parquet(f"{SILVER_DIR}/dim_products.parquet")
        log.info(f"dim_products saved: {dim_products.shape[0]} rows")

        # 4. Transform Fact Order Items
        log.info("Transforming Fact Order Items...")
        # Lọc các đơn hàng hợp lệ: nếu delivered thì phải có đủ ngày giao hàng thực tế
        orders_valid = orders_df.filter(
            ~((pl.col("order_status") == "delivered") & pl.col("order_delivered_customer_date").is_null())
        )

        fct_items_joined = order_items_df.join(
            orders_valid.select([
                "order_id", "customer_id", "order_status",
                "order_purchase_timestamp", "order_approved_at",
                "order_delivered_carrier_date", "order_delivered_customer_date",
                "order_estimated_delivery_date"
            ]),
            on="order_id",
            how="inner"
        )

        # Áp dụng Data Contract: price > 0 và freight_value >= 0
        valid_contract = (pl.col("price") > 0) & (pl.col("freight_value") >= 0)
        fct_items_valid = fct_items_joined.filter(valid_contract).unique(subset=["order_id", "order_item_id"])
        fct_items_invalid = fct_items_joined.filter(~valid_contract)

        # Lưu record hợp lệ vào Silver
        fct_items_valid.write_parquet(f"{SILVER_DIR}/fct_order_items.parquet")
        log.info(f"fct_order_items saved: {fct_items_valid.shape[0]} rows")

        # Ghi các record vi phạm contract vào DLQ
        if fct_items_invalid.shape[0] > 0:
            dlq_path = f"{DLQ_DIR}/fct_order_items_dlq.parquet"
            fct_items_invalid.write_parquet(dlq_path)
            log.warning(f"Sent {fct_items_invalid.shape[0]} invalid records to DLQ: {dlq_path}")

        # 5. Transform Fact Order Payments
        log.info("Transforming Fact Order Payments...")
        fct_payments = (
            payments_df
            .join(orders_df.select(["order_id", "customer_id"]), on="order_id", how="inner")
            .unique(subset=["order_id", "payment_sequential"])
            .select([
                "order_id",
                "payment_sequential",
                "customer_id",
                "payment_type",
                "payment_installments",
                "payment_value",
            ])
        )
        fct_payments.write_parquet(f"{SILVER_DIR}/fct_order_payments.parquet")
        log.info(f"fct_order_payments saved: {fct_payments.shape[0]} rows")

        # 6. Transform Fact Order Reviews
        log.info("Transforming Fact Order Reviews...")
        fct_reviews = (
            reviews_df
            .join(orders_df.select(["order_id", "customer_id"]), on="order_id", how="inner")
            .unique(subset=["review_id", "order_id"])
            .select([
                "review_id",
                "order_id",
                "customer_id",
                "review_score",
                "review_comment_title",
                "review_comment_message",
                "review_creation_date",
                "review_answer_timestamp",
            ])
        )
        fct_reviews.write_parquet(f"{SILVER_DIR}/fct_order_reviews.parquet")
        log.info(f"fct_order_reviews saved: {fct_reviews.shape[0]} rows")

        # Kiểm tra tỷ lệ lỗi DLQ để quyết định pass_quality
        total_items = fct_items_joined.shape[0]
        invalid_items_count = fct_items_invalid.shape[0]
        pass_quality = (invalid_items_count / total_items < 0.1) if total_items > 0 else True
        
        if 'ti' in kwargs and kwargs['ti'] is not None:
            kwargs['ti'].xcom_push(key="pass_quality", value=pass_quality)
        
        log.info("transform_silver_success", pass_quality=pass_quality)
                 
    except Exception as e:
        log.exception("transform_silver_failed")
        raise e

def check_quality(**kwargs):
    """
    Perform Data Quality checks on Silver data (inspired by Great Expectations).
    """
    ti = kwargs.get('ti', None)
    passed = ti.xcom_pull(task_ids="transform_to_silver", key="pass_quality") if ti else True
    log = logger.bind(task="check_quality")
    
    try:
        # 1. Đọc file dim_customers.parquet
        dim_cust_path = f"{SILVER_DIR}/dim_customers.parquet"
        if not os.path.exists(dim_cust_path):
            log.error("dim_customers.parquet not found!")
            return "alert_quality"
            
        dim_cust = pl.read_parquet(dim_cust_path)
        
        # 2. Kiểm tra số lượng dòng (> 0)
        if dim_cust.shape[0] == 0:
            log.error("Quality check failed: dim_customers has 0 rows")
            passed = False
            
        # 3. Kiểm tra xem có customer_id nào bị null không
        null_cust_count = dim_cust.filter(pl.col("customer_id").is_null()).shape[0]
        if null_cust_count > 0:
            log.error(f"Quality check failed: dim_customers has {null_cust_count} null customer_ids")
            passed = False
            
        # 4. Kiểm tra tính duy nhất của customer_id
        unique_cust_count = dim_cust.select("customer_id").n_unique()
        if unique_cust_count != dim_cust.shape[0]:
            log.error(f"Quality check failed: customer_id is not unique ({unique_cust_count} != {dim_cust.shape[0]})")
            passed = False

        # 5. Kiểm tra fct_order_items.parquet
        fct_items_path = f"{SILVER_DIR}/fct_order_items.parquet"
        if os.path.exists(fct_items_path):
            fct_items = pl.read_parquet(fct_items_path)
            valid_statuses = [
                "delivered", "shipped", "invoiced", "processing",
                "approved", "canceled", "unavailable", "created"
            ]
            invalid_status_count = fct_items.filter(~pl.col("order_status").is_in(valid_statuses)).shape[0]
            if invalid_status_count > 0:
                log.error(f"Quality check failed: {invalid_status_count} items have invalid order_status")
                passed = False

        if passed:
            log.info("All Data Quality checks passed successfully!")
            return "load_to_gold"
        else:
            log.warning("Data Quality checks failed. Routing to alert.")
            return "alert_quality"
            
    except Exception as e:
        log.exception(f"Data Quality check encountered an error: {e}")
        return "alert_quality"

if __name__ == "__main__":
    # For local testing
    clean_to_silver(ti=type('obj', (object,), {'xcom_push': lambda **kw: None})())