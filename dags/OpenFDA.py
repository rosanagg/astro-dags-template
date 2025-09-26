"""
OpenFDA -> BigQuery (events daily)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, get_current_context
from datetime import datetime, timedelta
import pandas as pd
import requests, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# ====== CONFIG ======
GCP_PROJECT = "disciplinacd-470814"
BQ_DATASET  = "openfda"
BQ_TABLE    = "events_daily"       # <-- nova tabela diária
GCP_CONN_ID = "google_cloud_default"
# ====================

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1, "retry_delay": timedelta(minutes=5)}

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "openfda-airflow/1.0"})
    retry = Retry(total=6, connect=3, read=3, status=6, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = _session()

def _url_for_day(day_str: str) -> str:
    # day_str = "YYYYMMDD"
    return ("https://api.fda.gov/drug/event.json"
            f"?search=patient.drug.medicinalproduct:%22ibuprofen%22"
            f"+AND+receivedate:[{day_str}+TO+{day_str}]"
            "&count=receivedate")

def fetch_openfda_daily():
    ctx = get_current_context()
    day = ctx["data_interval_start"].date()     # dia alvo da run (@daily)
    day_str = day.strftime("%Y%m%d")
    url = _url_for_day(day_str)

    r = SESSION.get(url, timeout=60)
    if r.status_code == 429:
        ra = r.headers.get("Retry-After")
        if ra:
            try: time.sleep(min(int(ra), 60))
            except Exception: time.sleep(5)
            r = SESSION.get(url, timeout=60)
    if r.status_code == 404:
        data = {"results": []}
    else:
        r.raise_for_status()
        data = r.json()

    df = pd.DataFrame(data.get("results", []))  # columns: time, count (se houver)
    if df.empty:
        print(f"[openfda] {day}: 0 registros")
        ctx["ti"].xcom_push(key="openfda_daily", value={"day": [], "events": []})
        return

    df["day"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True).dt.date
    df = df.rename(columns={"count": "events"})[["day", "events"]]
    print(f"[openfda] {day}: {len(df)} linha(s). Exemplo:\n{df.head().to_string(index=False)}")
    ctx["ti"].xcom_push(key="openfda_daily", value=df.to_dict(orient="list"))

def has_rows() -> bool:
    ctx = get_current_context()
    d = ctx["ti"].xcom_pull(task_ids="fetch_openfda_daily", key="openfda_daily") or {}
    ok = bool(d.get("day"))
    print(f"[openfda] seguir para BQ? {ok}")
    return ok

def save_to_bigquery():
    ctx = get_current_context()
    d = ctx["ti"].xcom_pull(task_ids="fetch_openfda_daily", key="openfda_daily") or {}
    df = pd.DataFrame.from_dict(d)
    if df.empty:
        print("[openfda] df vazio — nada a carregar")
        return

    df["day"] = pd.to_datetime(df["day"]).dt.date
    df["events"] = pd.to_numeric(df["events"]).fillna(0).astype("Int64")

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client: bigquery.Client = hook.get_client(project_id=GCP_PROJECT)

    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("day", "DATE"),
            bigquery.SchemaField("events", "INTEGER"),
        ],
    )
    print(f"[openfda] carregando {len(df)} linha(s) para {table_id}")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

with DAG(
    dag_id="openfda_to_bigquery_daily",
    description="Fetch OpenFDA ibuprofen events and load daily counts to BigQuery",
    schedule="@daily",                      # <-- diário
    start_date=datetime(2025, 9, 1),        # ajuste como quiser
    catchup=True,                           # True para backfill; False se desejar só daqui pra frente
    max_active_tasks=1,
    default_args=default_args,
    tags=["openfda", "bigquery", "daily"],
) as dag:
    fetch = PythonOperator(task_id="fetch_openfda_daily", python_callable=fetch_openfda_daily)
    check = ShortCircuitOperator(task_id="has_rows", python_callable=has_rows)
    load  = PythonOperator(task_id="save_to_bigquery", python_callable=save_to_bigquery)
    fetch >> check >> load













