from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import sys

# Add src to path so airflow can import it
sys.path.append('/opt/airflow/')

from src.extract import extract_to_bronze
from src.transform import clean_to_silver, check_quality
from src.load import load_to_gold

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
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
    print("passed Data Contracts (Great Expectations),")
    print("and loaded into PostgreSQL Star Schema Data Mart.")
    print("="*50)

with DAG(
    'ecommerce_end_to_end_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='End-to-End ETL for E-commerce Data using Medallion Architecture'
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
    
    # 4. Branching on Data Quality Check
    branch = BranchPythonOperator(
        task_id='check_quality',
        python_callable=check_quality
    )
    
    # 5. Load to Gold Data Mart (PostgreSQL Star Schema)
    load = PythonOperator(
        task_id='load_to_gold',
        python_callable=load_to_gold
    )
    
    # 6. Execution Report
    report = PythonOperator(
        task_id='execution_report',
        python_callable=print_execution_report
    )
    
    # 7. Alert Quality (khi Data Quality bị vi phạm)
    alert = EmptyOperator(
        task_id='alert_quality'
    )
    
    # 8. End task
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )
    
    # =========================================================================
    # Task Dependencies (Dataflow Graph)
    # =========================================================================
    # Flow 1: Tuyến tính ban đầu
    start >> extract >> transform >> branch
    
    # Flow 2 (Nhánh thành công): Tiếp tục load vào Gold, in báo cáo và kết thúc
    branch >> load >> report >> end
    
    # Flow 3 (Nhánh thất bại): Báo động Data Quality và kết thúc
    branch >> alert >> end
