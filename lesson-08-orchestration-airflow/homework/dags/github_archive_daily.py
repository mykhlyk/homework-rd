"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize
    from gh_sensor import GHArchiveSensor   # ваш custom sensor із plugins/

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.gh_etl import download, validate, load_to_duckdb, summarize
from gh_sensor import GHArchiveSensor 

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


def download_archive(ds, **_):
    """Завантажити годину за logical date ds; шлях штовхаємо в XCom (return)."""
    path = download(ds, LANDING_DIR)
    print(f"download_archive: {path}")
    return path


def validate_file(ti, **_):
    """Валідувати файл, шлях беремо з XCom (не качаємо повторно)."""
    path = ti.xcom_pull(task_ids="download_archive")
    validate(path)


def load_to_duckdb_task(ti, ds, **_):
    """Завантажити у raw.github_events_raw; шлях — з XCom, день — logical date ds."""
    path = ti.xcom_pull(task_ids="download_archive")
    rows = load_to_duckdb(path, ds, DB_PATH)
    print(f"load_to_duckdb: {rows} rows for {ds}")
    return rows


def notify_completion(ds, **_):
    """Підсумок за день ds з таблиці."""
    summary = summarize(ds, DB_PATH)
    print(f"notify_completion: {summary}")


with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",           
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["homework", "github", "etl"],
) as dag:
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )
    download_archive_t = PythonOperator(
        task_id="download_archive",
        python_callable=download_archive,
    )
    validate_file_t = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
    )
    load_to_duckdb_t = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb_task,
    )
    notify_completion_t = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    (
        check_availability
        >> download_archive_t
        >> validate_file_t
        >> load_to_duckdb_t
        >> notify_completion_t
    )