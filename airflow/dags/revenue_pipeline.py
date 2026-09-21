from pathlib import Path
from datetime import datetime

from airflow.sdk import dag, task
from utils.clickhouse_manager import ClickHouseManager
from utils.sql_runner import execute_sql_file
from utils.validators import validate_revenue_data

CH_HOST = "clickhouse"
CH_PORT = 9000

SQL_DIR = Path("/opt/airflow/dags/sql/revenue")

@dag(
    dag_id="revenue_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["revenue", "analytics"],
)
def revenue_pipeline():

    @task
    def run_sql_file(filename: str) -> None:
        file_path = SQL_DIR / filename
        execute_sql_file(file_path=file_path, host=CH_HOST, port=CH_PORT)

    @task
    def validate_revenue() -> None:
        file_path = SQL_DIR / "07_validation.sql"
        validate_revenue_data(file_path=file_path, host=CH_HOST, port=CH_PORT)

    create_transaction_facts = run_sql_file.override(task_id="create_transaction_facts")("01_create_transaction_facts.sql")
    load_transaction_facts = run_sql_file.override(task_id="load_transaction_facts")("02_load_transaction_facts.sql")
    create_daily_revenue = run_sql_file.override(task_id="create_daily_revenue")("03_create_daily_revenue.sql")
    load_daily_revenue = run_sql_file.override(task_id="load_daily_revenue")("04_load_daily_revenue.sql")
    create_category_revenue = run_sql_file.override(task_id="create_category_revenue")("05_create_category_revenue.sql")
    load_category_revenue = run_sql_file.override(task_id="load_category_revenue")("06_load_category_revenue.sql")
    
    validate_revenue_task = validate_revenue()

    create_transaction_facts >> load_transaction_facts
    load_transaction_facts >> create_daily_revenue >> load_daily_revenue
    load_transaction_facts >> create_category_revenue >> load_category_revenue
    [load_daily_revenue, load_category_revenue] >> validate_revenue_task

pipeline = revenue_pipeline()