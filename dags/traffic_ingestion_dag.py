from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hcmut_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'traffic_data_ingestion',
    default_args=default_args,
    description='Thu thap du lieu giao thong TPHCM, TomTom va Thoi tiet',
    schedule_interval='*/3 * * * *', # Chạy mỗi 3 phút
    catchup=False,
    tags=['ingestion', 'traffic', 'realtime'],
) as dag:

    SCRIPT_DIR = '/opt/airflow/src/ingestion'

    task_hcm_traffic = BashOperator(
        task_id='fetch_hcm_traffic',
        bash_command=f'python {SCRIPT_DIR}/fetch_hcm_traffic.py ',
    )

    task_tomtom = BashOperator(
        task_id='fetch_tomtom',
        bash_command=f'python {SCRIPT_DIR}/fetch_tomtom.py ',
    )

    task_weather = BashOperator(
        task_id='fetch_weather',
        bash_command=f'python {SCRIPT_DIR}/fetch_weather.py ',
    )

    [task_hcm_traffic, task_tomtom, task_weather]