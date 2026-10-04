from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)

CONN_ID = "postgres_earthquakes"  # define la conexión por AIRFLOW_CONN_POSTGRES_EARTHQUAKES

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="earthquake_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 9, 30),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=["/opt/airflow/sql"],  # monta ./sql en el contenedor
    tags=["earthquakes", "grafana", "portfolio"],
)
def earthquake_pipeline():

    @task
    def extract_and_load(data_interval_start=None, data_interval_end=None):
        """RAW: API USGS -> raw.earthquakes (upsert por id, idempotente)."""
        from src.load.earthquakes_loader import fetch_features, load_features

        # Solape de 1h: re-ejecutar un run o recuperar un hueco no duplica datos
        since = (data_interval_start - timedelta(hours=1)).isoformat()
        features = fetch_features(updatedafter=since)
        return load_features(features)  # nº de filas insertadas/actualizadas

    def sql_task(task_id, path):
        return SQLExecuteQueryOperator(task_id=task_id, conn_id=CONN_ID, sql=path)

    stage = sql_task("stage_clean_and_quarantine", "staging/earthquakes_clean.sql")
    fact = sql_task("build_fct_earthquakes", "analytics/fct_earthquakes.sql")
    geo = sql_task("enrich_geo", "analytics/enrich_fct_geo.sql")
    agg = sql_task("build_agg_daily", "analytics/agg_earthquakes_daily.sql")

    dq_future = SQLCheckOperator(
        task_id="dq_no_future_events",
        conn_id=CONN_ID,
        sql="SELECT count(*) = 0 FROM analytics.fct_earthquakes "
            "WHERE event_time > now() + interval '5 minutes'",
    )
    dq_mag = SQLCheckOperator(
        task_id="dq_magnitude_in_range",
        conn_id=CONN_ID,
        sql="SELECT count(*) = 0 FROM analytics.fct_earthquakes "
            "WHERE magnitude NOT BETWEEN -2 AND 10",
    )
    log_run = sql_task("log_run_metrics", "ops/log_run.sql")

    extract_and_load() >> stage >> fact >> geo >> [agg, dq_future, dq_mag] >> log_run


earthquake_pipeline()
