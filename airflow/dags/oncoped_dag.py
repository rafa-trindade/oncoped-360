from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import subprocess

default_args = {
    'owner': 'oncoped',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SCRIPTS_PATH = "/opt/airflow/scripts"

# ----------------------------
# Fetch / Process OncoDados
# ----------------------------

def fetch_datasus_po(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "extract/datasus/fetch_datasus_po.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="arquivos_atualizados", value=updated)
    return updated

def process_datasus_po_if_updated(ti=None):
    arquivos_atualizados = ti.xcom_pull(key="arquivos_atualizados", task_ids="fetch_datasus_po")
    if arquivos_atualizados:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'extract/datasus/process_datasus_po.py')}")
    else:
        print("[INFO] Nenhuma atualização nos arquivos DBC. Pulando processamento.")

# ----------------------------
# Sync Raw to Bucket / Kaggle
# ----------------------------

def load_raw_to_bucket(ti=None):
    result = subprocess.run(
        ["python", os.path.join(SCRIPTS_PATH, "load/load_raw_to_bucket.py")],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    print(result.stderr)
    updated = result.returncode == 0
    ti.xcom_push(key="bucket_updated", value=updated)
    return updated

def load_raw_to_kaggle_if_bucket_updated(ti=None):
    bucket_updated = ti.xcom_pull(key="bucket_updated", task_ids="load_raw_to_bucket")
    if bucket_updated:
        os.system(f"python {os.path.join(SCRIPTS_PATH, 'load/load_raw_to_kaggle.py')}")
    else:
        print("[INFO] Nenhum arquivo novo enviado ao bucket. Pulando upload para Kaggle.")

# ----------------------------
# DAG
# ----------------------------

with DAG(
    'oncoped_weekly_pipeline',
    default_args=default_args,
    description='Pipeline semanal oncoped-360',
    schedule_interval="0 3 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fetch_task_datasus_po = PythonOperator(
        task_id='fetch_datasus_po',
        python_callable=fetch_datasus_po
    )

    process_task_datasus_po = PythonOperator(
        task_id='process_datasus_po',
        python_callable=process_datasus_po_if_updated
    )

    sync_raw_to_bucket = PythonOperator(
        task_id='load_raw_to_bucket',
        python_callable=load_raw_to_bucket
    )

    sync_raw_to_kaggle = PythonOperator(
        task_id='load_raw_to_kaggle',
        python_callable=load_raw_to_kaggle_if_bucket_updated
    )

    fetch_task_datasus_po >> process_task_datasus_po >> sync_raw_to_bucket >> sync_raw_to_kaggle
