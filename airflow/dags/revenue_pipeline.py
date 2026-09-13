from pathlib import Path
from datetime import datetime

from airflow.sdk import dag, task
from utils.clickhouse_manager import ClickHouseManager

CH_HOST = "clickhouse"
CH_PORT = 9000

# Set SQL directory to match the required Airflow dags structure
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
        """Reads and executes a SQL file statement by statement."""
        sql = (SQL_DIR / filename).read_text()

        manager = ClickHouseManager(
            host=CH_HOST,
            port=CH_PORT,
        )

        statements = [s for s in sql.split(";") if s.strip()]

        for statement in statements:
            manager.execute_query(statement)

    @task
    def validate_revenue() -> None:
        """Reads validation queries from 07_validation.sql and evaluates them."""
        sql = (SQL_DIR / "07_validation.sql").read_text()
        
        # Split the 4 validation queries
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        manager = ClickHouseManager(
            host=CH_HOST,
            port=CH_PORT,
        )

        # 1. Row count validation (1st query in 07_validation.sql)
        row_counts = manager.execute_query(statements[0])
        raw_count, fact_count = row_counts[0]

        if raw_count != fact_count:
            raise ValueError(f"Row count mismatch: raw={raw_count}, fact={fact_count}")

        # 2. Missing category validation (2nd query in 07_validation.sql)
        missing_category_count = manager.execute_query(statements[1])[0][0]

        if missing_category_count != 0:
            raise ValueError(f"Missing merchant categories: {missing_category_count}")

        # 3. Completed revenue validation (3rd query in 07_validation.sql)
        revenue_sums = manager.execute_query(statements[2])
        raw_completed_revenue, fact_completed_revenue = revenue_sums[0]

        if abs(raw_completed_revenue - fact_completed_revenue) > 0.01:
            raise ValueError(
                f"Completed revenue mismatch: raw={raw_completed_revenue}, fact={fact_completed_revenue}"
            )

        # 4. Daily and category reconciliation (4th query in 07_validation.sql)
        # Returns rows only if there is a mismatch
        mismatched_rows = manager.execute_query(statements[3])

        if len(mismatched_rows) > 0:
            raise ValueError(f"Daily/category reconciliation failed for {len(mismatched_rows)} days")


    # Initialize SQL tasks with overrides for unique task_ids
    create_transaction_facts = run_sql_file.override(task_id="create_transaction_facts")("01_create_transaction_facts.sql")
    load_transaction_facts = run_sql_file.override(task_id="load_transaction_facts")("02_load_transaction_facts.sql")
    create_daily_revenue = run_sql_file.override(task_id="create_daily_revenue")("03_create_daily_revenue.sql")
    load_daily_revenue = run_sql_file.override(task_id="load_daily_revenue")("04_load_daily_revenue.sql")
    create_category_revenue = run_sql_file.override(task_id="create_category_revenue")("05_create_category_revenue.sql")
    load_category_revenue = run_sql_file.override(task_id="load_category_revenue")("06_load_category_revenue.sql")
    
    validate_revenue_task = validate_revenue()

    # Define task dependencies
    create_transaction_facts >> load_transaction_facts
    load_transaction_facts >> create_daily_revenue >> load_daily_revenue
    load_transaction_facts >> create_category_revenue >> load_category_revenue
    [load_daily_revenue, load_category_revenue] >> validate_revenue_task

pipeline = revenue_pipeline()