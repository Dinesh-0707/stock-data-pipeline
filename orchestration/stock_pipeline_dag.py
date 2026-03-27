from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'dinesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='End to end stock data pipeline',
    schedule='0 18 * * 1-5',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stocks', 'finance', 'etl'],
) as dag:

    ingest_task = BashOperator(
        task_id='ingest_stock_data',
        bash_command='python /opt/airflow/ingestion/fetch_stock_data.py',
    )

    transform_task = BashOperator(
        task_id='transform_to_silver',
        bash_command='python /opt/airflow/transformation/transform_stock_data.py',
    )

    gold_task = BashOperator(
        task_id='build_gold_layer',
        bash_command='python /opt/airflow/transformation/build_gold_layer.py',
    )

    ingest_task >> transform_task >> gold_task
