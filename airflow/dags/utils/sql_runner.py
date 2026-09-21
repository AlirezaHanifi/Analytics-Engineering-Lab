from pathlib import Path
from utils.clickhouse_manager import ClickHouseManager

def execute_sql_file(file_path: Path, host: str, port: int) -> None:
    """Executes a SQL file statement by statement."""
    sql = file_path.read_text()
    
    manager = ClickHouseManager(host=host, port=port)
    statements = [s for s in sql.split(";") if s.strip()]
    
    for statement in statements:
        manager.execute_query(statement)