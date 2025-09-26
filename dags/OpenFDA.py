"""
OpenFDA -> BigQuery (events weekly)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# ====== CONFIG ======
GCP_PROJECT = "disciplinacd-470814"     # <--- troque se necessário
BQ_DATASET  = "openfda"                 # será criado fora da DAG (ou crie manualmente)
BQ_TABLE    = "events_weekly"           # destino final
GCP_CONN_ID = "google_cloud_default"    # conexão do Airflow
# ====================

def generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    last_day = ((datetime(year, month, 1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)).day
    end_date = f"{year}{month:02d}{last_day:02d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22ibuprofen%22+AND+receivedate:[{start_date}+TO+{end_date}]"
        "&count=receivedate"
    )

def fetch_openfda_data():
    ctx = get_current_context()
    execution_date = ctx["dag_run"].execution_date
    year, month = execution_date.year, execution_date.month

    url = generate_query_url(year, month)
    r = requests.get(url, timeout=60)

    if r.status_code == 200:
        data = r.json()
        df = pd.DataFrame(data.get("results", []))
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            weekly = (
                df.groupby(pd.Grouper(key="time", freq="W"))["count"]
                  .sum()
                  .reset_index()
                  .rename(columns={"time": "week_start", "count": "events"})
            )
            weekly["week_start"] = weekly["week_start"].dt.date.astype(str)
        else:
            weekly = pd.DataFrame(columns=["week_start", "events"])
    else:
        weekly = pd.DataFrame(columns=["week_start", "events"])

    ctx["ti"].xcom_push(key="openfda_weekly", value=weekly.to_dict(orient="list"))

def save_to_bigquery():
    ctx = get_current_context()
    data_dict = ctx["ti"].xcom_pull(task_ids="fetch_openfda_data", key="openfda_weekly")
    if not data_dict:
        return

    df = pd.DataFrame.from_dict(data_dict)

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client: bigquery.Client = hook.get_client(project_id=GCP_PROJECT)

    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("week_start", "DATE"),
            bigquery.SchemaField("events", "INTEGER"),
        ],
    )
    # Converte tipos para garantir compatibilidade
    if not df.empty:
        df = df.astype({"week_start": "string", "events": "Int64"}).fillna({"events": 0})
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # espera terminar

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openfda_to_bigquery_weekly",
    description="Fetch OpenFDA ibuprofen events and load weekly sums to BigQuery",
    schedule="@monthly",               # rode mensalmente; mude se quiser
    start_date=datetime(2020, 11, 1),
    catchup=False,                     # mude para True se quiser backfill
    max_active_tasks=1,
    default_args=default_args,
    tags=["openfda", "bigquery"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_openfda_data",
        python_callable=fetch_openfda_data,
    )
    load = PythonOperator(
        task_id="save_to_bigquery",
        python_callable=save_to_bigquery,
    )
    fetch >> load




