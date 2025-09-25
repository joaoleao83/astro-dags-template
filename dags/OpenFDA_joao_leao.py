from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# ====== CONFIG ======
GCP_PROJECT  = "meu-projeto-471401"
BQ_DATASET   = "openfda"
BQ_TABLE     = "openfda_history_hourly"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
GCS_BUCKET   = "meu-bucket-openfda"   # 👈 ajuste aqui para o seu bucket
GCS_OBJECT   = "openfda/weekly_sum.json"
LOCAL_FILE   = "/tmp/weekly_sum.json"
# ====================


def fetch_openfda_data(**kwargs):
    ti = kwargs['ti']
    exec_dt = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    year, month = exec_dt.year, exec_dt.month

    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}{(datetime(year, month, 1) + timedelta(days=31)).replace(day=1) - timedelta(days=1):%d}"
    query_url = f"https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"

    response = requests.get(query_url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['results'])
        df['time'] = pd.to_datetime(df['time'])
        weekly_sum = df.groupby(pd.Grouper(key='time', freq='W'))['count'].sum().reset_index()
        weekly_sum["time"] = weekly_sum["time"].astype(str)

        # Salva local em JSON NDJSON
        weekly_sum.to_json(LOCAL_FILE, orient="records", lines=True)
    else:
        # Se não vier nada, cria arquivo vazio
        pd.DataFrame([]).to_json(LOCAL_FILE, orient="records", lines=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_openfda_data_monthly',
    default_args=default_args,
    description='Retrieve OpenFDA data monthly and store in BigQuery',
    schedule='@monthly',
    start_date=datetime(2020, 11, 1),
    catchup=True,
    max_active_tasks=1,
    concurrency=1
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_openfda_data',
        python_callable=fetch_openfda_data,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=LOCAL_FILE,
        dst=GCS_OBJECT,
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )

    save_to_bigquery_task = BigQueryInsertJobOperator(
        task_id="save_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_OBJECT}"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",  # ou "WRITE_TRUNCATE"
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
            }
        },
        location=BQ_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data_task >> upload_to_gcs_task >> save_to_bigquery_task
