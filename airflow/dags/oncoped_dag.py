from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'oncoped',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_datasus_po():
    os.system("python /opt/airflow/scripts/extract/datasus/fetch_datasus_po.py")

def process_datasus_po():
    os.system("python /opt/airflow/scripts/extract/datasus/process_datasus_po.py")


def load_raw_to_bucket():
    os.system("python /opt/airflow/scripts/load/load_raw_to_bucket.py")

def load_raw_to_kaggle():
    os.system("python /opt/airflow/scripts/load/load_raw_to_kaggle.py")


with DAG(
    'oncoped_weekly_pipeline',
    default_args=default_args,
    description='Pipeline semanal oncoped-360',
    schedule_interval="0 3 * * 0",  # todo domingo às 03:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fetch_datasus_po = PythonOperator(
        task_id='fetch_datasus_po',
        python_callable=fetch_datasus_po
    )

    process_datasus_po = PythonOperator(
        task_id='process_datasus_po',
        python_callable=process_datasus_po
    )

    sync_raw_to_bucket = PythonOperator(
        task_id='load_raw_to_bucket',
        python_callable=load_raw_to_bucket
    )

    sync_raw_to_kaggle = PythonOperator(
        task_id='load_raw_to_kaggle',
        python_callable=load_raw_to_kaggle
    )


    fetch_datasus_po >> process_datasus_po >> sync_raw_to_bucket >> sync_raw_to_kaggle