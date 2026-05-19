"""
DAG d'orchestration : TMDB → Data Lake → Spark → Snowflake → DBT

Étapes :
  1. extract_tmdb         : pagination TMDB → Parquet RAW (MinIO)
  2. spark_staging        : nettoyage + dédup → Parquet STAGING (MinIO)
  3. spark_curated        : jointures + enrichissement → Parquet CURATED (MinIO)
  4. snowflake_load       : PUT + COPY INTO → tables RAW.* (Snowflake)
  5. dbt_run              : star schema dans MARTS.*
  6. dbt_test             : tests qualité (51 tests)
  7. notify_success       : log final

Spark tourne dans son propre container (apache/spark:3.5.1-python3) lancé depuis
Airflow via le socket Docker monté en volume. PROJECT_HOST_PATH est le chemin
HOST du projet (à passer en variable d'environnement, voir docker-compose.yml).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Permet d'importer ingestion.* et snowflake_load.*
sys.path.insert(0, "/opt/airflow")

from ingestion.extract_tmdb import run as run_extract  # noqa: E402
from snowflake_load.load import run as run_snowflake_load  # noqa: E402

# ─── Configuration ────────────────────────────────────────────────────────────

PROJECT_HOST   = os.environ["PROJECT_HOST_PATH"]
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "finalpipelinev1_default")
SPARK_IMAGE    = "apache/spark:3.5.1-python3"
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "maelz",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def spark_docker_command(script: str) -> str:
    """
    Génère la commande shell pour lancer un script Spark dans un container.
    INGESTION_DATE est rendu via Jinja ({{ ds }}) au moment du templating Airflow.
    """
    return dedent(f"""
        docker run --rm --user 0 \\
          --network {DOCKER_NETWORK} \\
          -v "{PROJECT_HOST}:/app" -w /app \\
          -e MINIO_ENDPOINT=http://minio:9000 \\
          -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY \\
          -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY \\
          -e MINIO_BUCKET=$MINIO_BUCKET \\
          -e INGESTION_DATE={{{{ ds }}}} \\
          -e PYTHONPATH=/app \\
          {SPARK_IMAGE} \\
          /opt/spark/bin/spark-submit \\
            --packages {SPARK_PACKAGES} \\
            {script}
    """).strip()

def py_extract(ds: str, **_):
    """Wrapper PythonOperator : injecte INGESTION_DATE depuis le contexte Airflow."""
    os.environ["INGESTION_DATE"] = ds
    return run_extract()

def py_snowflake_load(ds: str, **_):
    os.environ["INGESTION_DATE"] = ds
    return run_snowflake_load()

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="tmdb_pipeline",
    description="Pipeline TMDB → MinIO → Spark → Snowflake → DBT",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,           # déclenchement manuel pour la soutenance
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "data-engineering", "final"],
) as dag:

    extract_movies = PythonOperator(
        task_id="extract_tmdb",
        python_callable=py_extract,
    )

    spark_staging = BashOperator(
        task_id="spark_staging",
        bash_command=spark_docker_command("spark/staging.py"),
    )

    spark_curated = BashOperator(
        task_id="spark_curated",
        bash_command=spark_docker_command("spark/curated.py"),
    )

    snowflake_load = PythonOperator(
        task_id="snowflake_load",
        python_callable=py_snowflake_load,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "[OK] Pipeline TMDB terminé pour ds={{ ds }}"',
    )

    (
        extract_movies
        >> spark_staging
        >> spark_curated
        >> snowflake_load
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> notify_success
    )
