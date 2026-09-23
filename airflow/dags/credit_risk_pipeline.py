"""Run the credit-risk SQL transformations and validations in ClickHouse."""

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

from utils.clickhouse_manager import ClickHouseManager
from utils.sql_runner import execute_sql_file


CH_HOST = "clickhouse"
CH_PORT = 9000
DAG_DIRECTORY = Path(__file__).resolve().parent
QUERY_DIRECTORY = DAG_DIRECTORY / "queries" / "credit_risk"
TRANSFORMATION_SQL_PATH = QUERY_DIRECTORY / "transformations.sql"
VALIDATION_SQL_PATH = QUERY_DIRECTORY / "validations.sql"

logger = logging.getLogger(__name__)


@dag(
    dag_id="credit_risk_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analytics", "credit-risk"],
    doc_md="""
    ## Credit Risk & Loan Eligibility

    Builds the three ClickHouse credit-risk models and validates their results.
    The supplied `loan_eligible` field remains the source decision. The activity
    segment is an explainable review label, not an ML prediction.
    """,
)
def credit_risk_pipeline() -> None:
    @task
    def run_transformations() -> None:
        """Execute all model statements in their SQL-file dependency order."""
        db = ClickHouseManager(host=CH_HOST, port=CH_PORT)
        execute_sql_file(db, TRANSFORMATION_SQL_PATH)
        logger.info("Credit-risk transformations completed successfully")

    @task
    def run_validations() -> None:
        """Execute validation SQL and fail if any required check is false."""
        db = ClickHouseManager(host=CH_HOST, port=CH_PORT)
        results = execute_sql_file(db, VALIDATION_SQL_PATH)

        if len(results) != 7:
            raise ValueError(
                f"Expected 7 validation queries in {VALIDATION_SQL_PATH}, "
                f"but found {len(results)}. Update this task if the file changes."
            )

        validation_rows = []
        for position, query_result in enumerate(results, start=1):
            if not query_result:
                raise ValueError(f"Validation query {position} returned no rows")
            validation_rows.append(query_result[0])

        row_count, unique_users = validation_rows[0]
        raw_credit_rows, modeled_credit_rows = validation_rows[1]
        raw_eligible, modeled_eligible = validation_rows[2]
        (
            profile_customers,
            summary_customers,
            profile_eligible,
            summary_eligible,
        ) = validation_rows[3]
        (eligibility_rule_mismatches,) = validation_rows[4]
        (credit_rows_without_user,) = validation_rows[5]
        raw_transaction_rows, modeled_transaction_rows = validation_rows[6]

        checks = {
            "one profile per user": row_count == unique_users,
            "raw and modeled credit rows match": (
                raw_credit_rows == modeled_credit_rows
            ),
            "raw and modeled eligible counts match": (
                raw_eligible == modeled_eligible
            ),
            "profile and summary customer counts match": (
                profile_customers == summary_customers
            ),
            "profile and summary eligible counts match": (
                profile_eligible == summary_eligible
            ),
            "supplied eligibility flag matches its documented rule": (
                eligibility_rule_mismatches == 0
            ),
            "every credit row has a matching user": credit_rows_without_user == 0,
            "raw and modeled transaction counts match": (
                raw_transaction_rows == modeled_transaction_rows
            ),
        }

        failed_checks = [name for name, passed in checks.items() if not passed]
        if failed_checks:
            raise ValueError("Validation failed: " + "; ".join(failed_checks))

        logger.info(
            "All validations passed: users=%s, eligible_users=%s, transactions=%s",
            row_count,
            modeled_eligible,
            modeled_transaction_rows,
        )

    transformations = run_transformations()
    validations = run_validations()

    transformations >> validations


credit_risk_pipeline()
