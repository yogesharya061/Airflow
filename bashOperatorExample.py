"""
Dag info : bashOperatorExample
--------
This dag is to show how to use Airflow BashOperator by copying some data using s3cmd command
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Declaring global parameters
source_file = {
  "source_path": "s3a://my_bucket/raw_data/test.csv",
  "file_name": "sample.csv"
}

# DAG declaration
default_args = {
    "depends_on_past": False,
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": dag_failure_alert,
    "execution_timeout": timedelta(hours=24),
}

dag = DAG(
    'bashOperatorExample',
    default_args=default_args,
    start_date=datetime(2022, 12, 20),
    schedule_interval='0 2 * * *',
    catchup=False,
    concurrency=1,
    doc_md=__doc__,
    tags=['python']
)

# Task to copy data from s3 to local
copy_results_from_s3_to_local = BashOperator(
    task_id='copy-results-from-s3-to-local',
    dag=dag,
    bash_command=f's3cmd get {source_file['source_path']} /tmp/{source_file['file_name']}'
)

# Task to remove data from local
remove_results_from_local = BashOperator(
    task_id='remove-results-from-local',
    dag=dag,
    bash_command=f'rm -f /tmp/{source_file['file_name']}'
)

# Ordering the tasks. Remove file must be run only if copy task completes successfully.
copy_results_from_s3_to_local >> remove_results_from_local
