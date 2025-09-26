"""
OpenFDA -> BigQuery (events weekly)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
import pandas as pd
import requests
import pendulum

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# ====== CONFIG ======
GCP_PROJECT = "disciplinacd-470814"
BQ_DATASET  = "openfda"
BQ_TABLE    = "events_weekly"
GCP_CONN_ID = "google_cloud_default"
# ====================

# defaults da DAG (opcional, mas recomendado)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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

    # Airflow 2.x: use data_interval_start (ou logical_date)
    start = ctx["data_interval_start"]
    year, month = start.year, start.month

    url = generate_query_url(year, month)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    data = r.json()
    df = pd.DataFrame(data.get("results", []))

    if df.empty:
        ctx["ti"].xcom_push(key="openfda_weekly", value={"week_start": [], "events": []})
        return

    df["time"] = pd.to_datetime(df["time"], utc=True)

    weekly = (
        df.groupby(pd.Grouper(key="time", freq="W"))["count"]
          .sum()
          .reset_index()
          .rename(columns={"time": "week_start", "count": "events"})
    )
    weekly["week_start"] = weekly["week_start"].dt.date  # BQ espera DATE

    ctx["ti"].xcom_push(key="openfda_weekly", value=weekly.to_dict(orient="list"))

def save_to_bigquery():
    ctx = get_current_context()
    data_dict = ctx["ti"].xcom_pull(task_ids="fetch_openfda_data", key="openfda_weekly")
    if not data_dict:
        return

    df = pd.DataFrame.from_dict(data_dict)
    if df.empty:
        return

    # Tipagem compatível com schema
    df["week_start"] = pd.to_datetime(df["week_start"]).dt.date
    df["events"] = pd.to_numeric(df["events"]).fillna(0).astype("Int64")

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
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

with DAG(
    dag_id="openfda_to_bigquery_weekly",
    description="Fetch OpenFDA ibuprofen events and load weekly sums to BigQuery",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,             # True se quiser backfill
    max_active_tasks=1,
    default_args=default_args,
    tags=["openfda", "bigquery"],
) as dag:
    fetch = PythonOperator(task_id="fetch_openfda_data", python_callable=fetch_openfda_data)
    load  = PythonOperator(task_id="save_to_bigquery",   python_callable=save_to_bigquery)
    fetch >> load






