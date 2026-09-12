from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from utils.clickhouse_manager import ClickHouseManager


CH_HOST = "clickhouse"
CH_PORT = 9000
SQL_DIR = Path(__file__).parent / "sql"

REPEATED_FAILURE_THRESHOLD = 2
HIGH_USER_DAY_VOLUME_THRESHOLD = 3
LARGE_AMOUNT_QUANTILE = 0.99


def _clickhouse() -> ClickHouseManager:
    return ClickHouseManager(host=CH_HOST, port=CH_PORT)


@dag(
    dag_id="fraud_detection",
    description="Build explainable transaction-risk facts, alerts, and summaries.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=str(SQL_DIR),
    params={
        "large_amount_quantile": LARGE_AMOUNT_QUANTILE,
        "repeated_failure_threshold": REPEATED_FAILURE_THRESHOLD,
        "high_user_day_volume_threshold": HIGH_USER_DAY_VOLUME_THRESHOLD,
    },
    default_args={
        "owner": "analytics-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["analytics", "fraud", "clickhouse"],
    doc_md="""
    Builds the Member 4 fraud-detection models from the existing banking seed data.

    An alert is an explainable operational review signal, **not confirmed fraud**.
    The rules are: amount above the source dataset's exact 99th percentile, at
    least two failures by a user in one day, a refunded transaction, or at least
    three transactions by a user in one day.
    """,
)
def fraud_detection_dag() -> None:
    @task
    def check_source_data() -> dict[str, int]:
        db = _clickhouse()

        table_count = db.execute_query("""
            SELECT count()
            FROM system.tables
            WHERE database = 'raw'
              AND name IN ('transactions', 'merchants')
        """)[0][0]

        if table_count != 2:
            raise AirflowException(
                "Required raw tables are missing. Run seed_banking_raw_data first."
            )

        transaction_count = db.execute_query(
            "SELECT count() FROM raw.transactions"
        )[0][0]
        merchant_count = db.execute_query(
            "SELECT count() FROM raw.merchants"
        )[0][0]

        if transaction_count == 0 or merchant_count == 0:
            raise AirflowException(
                "Required raw tables are empty. Run seed_banking_raw_data first."
            )

        return {
            "transactions": int(transaction_count),
            "merchants": int(merchant_count),
        }

    @task
    def ensure_analytics_database() -> None:
        _clickhouse().execute_query("CREATE DATABASE IF NOT EXISTS analytics")

    @task(templates_exts=[".sql"])
    def run_clickhouse_model(sql: str) -> None:
        _clickhouse().execute_query(sql)

    @task(templates_exts=[".sql"])
    def validate_models(sql: str) -> dict[str, dict[str, int]]:
        """Reconcile model grains and rule outputs before declaring success."""
        db = _clickhouse()
        results: dict[str, dict[str, int]] = {}
        failed_checks: list[str] = []

        for check_name, actual, expected in db.execute_query(sql):
            actual_value = int(actual)
            expected_value = int(expected)
            results[check_name] = {
                "actual": actual_value,
                "expected": expected_value,
            }
            if actual_value != expected_value:
                failed_checks.append(check_name)

        if failed_checks:
            raise AirflowException(
                "Fraud model validation failed: " + ", ".join(failed_checks)
            )

        return results

    source_data = check_source_data()
    analytics_database = ensure_analytics_database()
    transaction_facts = run_clickhouse_model.override(
        task_id="build_transaction_risk_facts"
    )(sql="fraud_detection/transaction_risk_facts.sql")
    alerts = run_clickhouse_model.override(task_id="build_fraud_alerts")(
        sql="fraud_detection/fraud_alerts.sql"
    )
    summary = run_clickhouse_model.override(task_id="build_fraud_risk_summary")(
        sql="fraud_detection/fraud_risk_summary.sql"
    )
    validation = validate_models(sql="fraud_detection/validate_fraud_models.sql")

    source_data >> analytics_database >> transaction_facts >> alerts >> summary >> validation


fraud_detection_dag()
