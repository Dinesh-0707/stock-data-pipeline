from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dinesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The run that fires at 18:00 on a weekday is for that same day, which is the
# end of its data interval. Every task gets the same date so re-running a DAG
# run rewrites the same files instead of creating new ones.
RUN_DATE = '{{ data_interval_end | ds_nodash }}'
PROJECT_DIR = '/opt/airflow'
DBT = '/opt/airflow/dbt_venv/bin/dbt'

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
        bash_command=f'python ingestion/fetch_stock_data.py --run-date {RUN_DATE}',
        cwd=PROJECT_DIR,
    )

    transform_task = BashOperator(
        task_id='transform_to_silver',
        bash_command=f'python transformation/transform_stock_data.py --run-date {RUN_DATE}',
        cwd=PROJECT_DIR,
    )

    gold_task = BashOperator(
        task_id='build_gold_layer',
        bash_command='python transformation/build_gold_layer.py',
        cwd=PROJECT_DIR,
    )

    delta_task = BashOperator(
        task_id='merge_into_delta',
        bash_command='python transformation/delta_lake.py',
        cwd=PROJECT_DIR,
    )

    dbt_task = BashOperator(
        task_id='dbt_build',
        # dbt build runs the models and their tests in dependency order
        bash_command=f'{DBT} build --project-dir stock_analytics --profiles-dir stock_analytics',
        cwd=PROJECT_DIR,
    )

    ingest_task >> transform_task >> [gold_task, delta_task, dbt_task]
