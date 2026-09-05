from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.clickhouse_manager import ClickHouseManager


CH_HOST = "clickhouse"
CH_PORT = 9000

SQL_DIR = Path("/opt/airflow/sql/revenue")


def run_sql_file(filename: str) -> None:
    sql = (SQL_DIR / filename).read_text()

    manager = ClickHouseManager(
        host=CH_HOST,
        port=CH_PORT,
    )

    statements = sql.split(";")

    for statement in statements:
        if statement.strip():
            manager.execute_query(statement)


with DAG(
    dag_id="revenue_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["revenue", "analytics"],
) as dag:

    create_transaction_facts = PythonOperator(
        task_id="create_transaction_facts",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "01_create_transaction_facts.sql",
        },
    )

    load_transaction_facts = PythonOperator(
        task_id="load_transaction_facts",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "02_load_transaction_facts.sql",
        },
    )

    create_daily_revenue = PythonOperator(
        task_id="create_daily_revenue",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "03_create_daily_revenue.sql",
        },
    )

    load_daily_revenue = PythonOperator(
        task_id="load_daily_revenue",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "04_load_daily_revenue.sql",
        },
    )

    create_category_revenue = PythonOperator(
        task_id="create_category_revenue",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "05_create_category_revenue.sql",
        },
    )

    load_category_revenue = PythonOperator(
        task_id="load_category_revenue",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "06_load_category_revenue.sql",
        },
    )

    validate_revenue = PythonOperator(
        task_id="validate_revenue",
        python_callable=run_sql_file,
        op_kwargs={
            "filename": "07_validation.sql",
        },
    )

    create_transaction_facts >> load_transaction_facts

    load_transaction_facts >> create_daily_revenue >> load_daily_revenue
    load_transaction_facts >> create_category_revenue >> load_category_revenue

    [load_daily_revenue, load_category_revenue] >> validate_revenue
