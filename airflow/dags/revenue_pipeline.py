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


def validate_revenue() -> None:
    manager = ClickHouseManager(
        host=CH_HOST,
        port=CH_PORT,
    )

    raw_count = manager.execute_query(
        "SELECT count() FROM raw.transactions"
    )[0][0]

    fact_count = manager.execute_query(
        "SELECT count() FROM analytics.transaction_facts"
    )[0][0]

    if raw_count != fact_count:
        raise ValueError(
            f"Row count mismatch: raw={raw_count}, fact={fact_count}"
        )

    print(
        f"Row count validation passed: raw={raw_count}, fact={fact_count}"
    )

    missing_category_count = manager.execute_query(
        """
        SELECT count()
        FROM analytics.transaction_facts
        WHERE merchant_category = ''
        """
    )[0][0]

    if missing_category_count != 0:
        raise ValueError(
            f"Missing merchant categories: {missing_category_count}"
        )

    print(
        f"Merchant category validation passed: "
        f"missing={missing_category_count}"
    )

    invalid_status_count = manager.execute_query(
        """
        SELECT count()
        FROM analytics.transaction_facts
        WHERE transaction_status NOT IN (
            'completed',
            'failed',
            'refunded'
        )
        """
    )[0][0]

    if invalid_status_count != 0:
        raise ValueError(
            f"Invalid transaction statuses: {invalid_status_count}"
        )

    print(
        f"Transaction status validation passed: "
        f"invalid={invalid_status_count}"
    )
    raw_completed_revenue = manager.execute_query(
        """
        SELECT sum(amount)
        FROM raw.transactions
        WHERE lower(trim(status)) = 'completed'
        """
    )[0][0]

    fact_completed_revenue = manager.execute_query(
        """
        SELECT sum(amount)
        FROM analytics.transaction_facts
        WHERE transaction_status = 'completed'
        """
    )[0][0]

    if abs(raw_completed_revenue - fact_completed_revenue) > 0.01:
        raise ValueError(
            "Completed revenue mismatch: "
            f"raw={raw_completed_revenue}, "
            f"fact={fact_completed_revenue}"
        )

    print(
        "Completed revenue validation passed: "
        f"raw={raw_completed_revenue}, "
        f"fact={fact_completed_revenue}"
    )


    daily_category_mismatch_count = manager.execute_query(
        """
        SELECT count()
        FROM
        (
            SELECT
                d.transaction_date
            FROM analytics.daily_revenue AS d
            LEFT JOIN
            (
                SELECT
                    transaction_date,
                    sum(completed_revenue) AS category_completed_revenue,
                    sum(transaction_count) AS category_transaction_count
                FROM analytics.category_revenue
                GROUP BY transaction_date
            ) AS c
                ON d.transaction_date = c.transaction_date
            WHERE
                abs(
                    d.completed_revenue
                    - c.category_completed_revenue
                ) > 0.01
                OR
                d.transaction_count != c.category_transaction_count
        )
        """
    )[0][0]

    if daily_category_mismatch_count != 0:
        raise ValueError(
            "Daily/category reconciliation failed: "
            f"{daily_category_mismatch_count} mismatched days"
        )

    print(
        "Daily/category reconciliation passed: "
        f"mismatched_days={daily_category_mismatch_count}"
    )


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

    validate_revenue_task = PythonOperator(
        task_id="validate_revenue",
        python_callable=validate_revenue,
    )

    create_transaction_facts >> load_transaction_facts

    load_transaction_facts >> create_daily_revenue >> load_daily_revenue
    load_transaction_facts >> create_category_revenue >> load_category_revenue

    [load_daily_revenue, load_category_revenue] >> validate_revenue_task