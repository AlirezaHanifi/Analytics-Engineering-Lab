"""Run the credit-risk SQL transformations and validations in ClickHouse."""

import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from utils.clickhouse_manager import ClickHouseManager


CH_HOST = "clickhouse"
CH_PORT = 9000
SQL_DIRECTORY = Path("/opt/airflow/scripts")
TRANSFORMATION_SQL_PATH = SQL_DIRECTORY / "credit_risk_transformations.sql"
VALIDATION_SQL_PATH = SQL_DIRECTORY / "credit_risk_validation.sql"

logger = logging.getLogger(__name__)


def read_sql_statements(path: Path) -> list[str]:
    """Read a SQL file and return its executable statements in file order.

    The ClickHouse Python driver executes one statement at a time. The SQL
    files also contain comments with semicolons, so line comments are removed
    before the file is split on statement-ending semicolons.
    """
    if not path.is_file():
        raise FileNotFoundError(
            f"SQL file not found: {path}. Check the Docker scripts volume mount."
        )

    sql = path.read_text(encoding="utf-8")
    sql_without_comments = "\n".join(
        line.split("--", maxsplit=1)[0] for line in sql.splitlines()
    )

    statements = [
        statement.strip()
        for statement in sql_without_comments.split(";")
        if statement.strip()
    ]

    if not statements:
        raise ValueError(f"No executable SQL statements found in {path}")

    return statements


def clickhouse_connection() -> ClickHouseManager:
    """Create a fresh ClickHouse connection for an Airflow task."""
    return ClickHouseManager(host=CH_HOST, port=CH_PORT)


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
        db = clickhouse_connection()
        statements = read_sql_statements(TRANSFORMATION_SQL_PATH)

        logger.info(
            "Running %s statements from %s",
            len(statements),
            TRANSFORMATION_SQL_PATH,
        )

        for position, statement in enumerate(statements, start=1):
            logger.info(
                "Running transformation statement %s of %s",
                position,
                len(statements),
            )
            db.execute_query(statement)

        logger.info("Credit-risk transformations completed successfully")

    @task
    def run_validations() -> None:
        """Execute validation SQL and fail if any required check is false."""
        db = clickhouse_connection()
        statements = read_sql_statements(VALIDATION_SQL_PATH)

        if len(statements) != 7:
            raise ValueError(
                f"Expected 7 validation queries in {VALIDATION_SQL_PATH}, "
                f"but found {len(statements)}. Update this task if the file changes."
            )

        results = []
        for position, statement in enumerate(statements, start=1):
            logger.info(
                "Running validation query %s of %s",
                position,
                len(statements),
            )
            query_result = db.execute_query(statement)
            if not query_result:
                raise ValueError(f"Validation query {position} returned no rows")
            results.append(query_result[0])

        row_count, unique_users = results[0]
        raw_credit_rows, modeled_credit_rows = results[1]
        raw_eligible, modeled_eligible = results[2]
        (
            profile_customers,
            summary_customers,
            profile_eligible,
            summary_eligible,
        ) = results[3]
        (eligibility_rule_mismatches,) = results[4]
        (credit_rows_without_user,) = results[5]
        raw_transaction_rows, modeled_transaction_rows = results[6]

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
