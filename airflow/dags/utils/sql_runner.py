"""Utilities for reading and executing version-controlled SQL files."""

import logging
from pathlib import Path
from typing import Any

from utils.clickhouse_manager import ClickHouseManager


logger = logging.getLogger(__name__)


def read_sql_statements(path: Path) -> list[str]:
    """Read a SQL file and return its executable statements in file order.

    The ClickHouse Python driver executes one statement at a time. Line
    comments are removed before splitting so semicolons in comments are not
    mistaken for statement boundaries.
    """
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")

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


def execute_sql_file(
    db: ClickHouseManager,
    path: Path,
) -> list[list[tuple[Any, ...]]]:
    """Execute every statement in a SQL file and return each result set."""
    statements = read_sql_statements(path)
    results: list[list[tuple[Any, ...]]] = []

    for position, statement in enumerate(statements, start=1):
        logger.info(
            "Executing SQL statement %s of %s from %s",
            position,
            len(statements),
            path,
        )
        results.append(db.execute_query(statement))

    return results
