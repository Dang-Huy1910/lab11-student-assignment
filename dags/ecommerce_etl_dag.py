from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

# Add src to path so airflow can import it
sys.path.append('/opt/airflow/')

from src.extract import extract_to_bronze
from src.transform import clean_to_silver
from src.load import load_to_gold

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data_team@example.com'],
}

def print_execution_report(**kwargs):
    """
    Generate a summary report of the pipeline execution (Monitoring step).
    """
    print("="*50)
    print(" 🚀 E-COMMERCE ETL PIPELINE EXECUTION REPORT 🚀")
    print("="*50)
    print(f"DAG Run ID: {kwargs['dag_run'].run_id}")
    print(f"Logical Date: {kwargs['logical_date']}")
    print("Status: SUCCESS ✅")
    print("Pipeline has successfully extracted Data Lake files,")
    print("transformed and loaded data into Gold PostgreSQL Data Mart,")
    print("and passed Great Expectations Checkpoint (dim_customers.warning).")
    print("Data Docs HTML reports updated successfully.")
    print("="*50)

with DAG(
    'ecommerce_end_to_end_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='End-to-End ETL with Great Expectations Quality Check on Gold'
) as dag:
    
    # 1. Start task
    start = EmptyOperator(
        task_id='start'
    )
    
    # 2. Extract to Bronze layer
    extract = PythonOperator(
        task_id='extract_to_bronze',
        python_callable=extract_to_bronze
    )
    
    # 3. Transform to Silver layer
    transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=clean_to_silver
    )
    
    # 4. Load to Gold Data Mart (PostgreSQL Star Schema)
    load = PythonOperator(
        task_id='load_to_gold',
        python_callable=load_to_gold
    )
    
    # 5. Great Expectations Checkpoint validation on Gold tables
    gx_run_gold_checkpoint = BashOperator(
        task_id='gx_run_gold_checkpoint',
        bash_command='cd /opt/airflow && great_expectations checkpoint run my_gold_checkpoint'
    )
    
    # 6. Execution Report
    report = PythonOperator(
        task_id='execution_report',
        python_callable=print_execution_report
    )
    
    # 7. End task
    end = EmptyOperator(
        task_id='end'
    )
    
    # =========================================================================
    # Task Dependencies (Dataflow Graph)
    # =========================================================================
    start >> extract >> transform >> load >> gx_run_gold_checkpoint >> report >> end
