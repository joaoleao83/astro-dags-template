from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# ====== CONFIG ======
GCP_PROJECT  = "meu-projeto-471401"
BQ_DATASET   = "openfda"
BQ_TABLE     = "openfda_history_hourly"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
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
    else:
        weekly_sum = pd.DataFrame([])

    # Envia o DataFrame como dicionário no XCom
    ti.xcom_push(key="openfda_data", value=weekly_sum.to_dict(orient="records"))


def save_to_bigquery(**kwargs):
    from google.cloud import bigquery
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids="fetch_openfda_data", key="openfda_data")

    if records:
        df = pd.DataFrame(records)

        client = bigquery.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job = client.load_table_from_dataframe(
            df,
            table_id,
            location=BQ_LOCATION,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            ),
        )
        job.result()  # Espera terminar


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
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_openfda_data',
        python_callable=fetch_openfda_data,
    )

    save_data_task = PythonOperator(
        task_id="save_to_bigquery",
        python_callable=save_to_bigquery,
    )

    fetch_data_task >> save_data_task
