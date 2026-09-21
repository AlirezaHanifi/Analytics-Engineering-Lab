from pathlib import Path
from utils.clickhouse_manager import ClickHouseManager

def validate_revenue_data(file_path: Path, host: str, port: int) -> None:
    sql = file_path.read_text()
    
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    manager = ClickHouseManager(host=host, port=port)

    row_counts = manager.execute_query(statements[0])
    raw_count, fact_count = row_counts[0]
    if raw_count != fact_count:
        raise ValueError(f"Row count mismatch: raw={raw_count}, fact={fact_count}")

    missing_category_count = manager.execute_query(statements[1])[0][0]
    if missing_category_count != 0:
        raise ValueError(f"Missing merchant categories: {missing_category_count}")

    revenue_sums = manager.execute_query(statements[2])
    raw_completed_revenue, fact_completed_revenue = revenue_sums[0]
    if abs(raw_completed_revenue - fact_completed_revenue) > 0.01:
        raise ValueError(f"Completed revenue mismatch: raw={raw_completed_revenue}, fact={fact_completed_revenue}")

    mismatched_rows = manager.execute_query(statements[3])
    if len(mismatched_rows) > 0:
        raise ValueError(f"Daily/category reconciliation failed for {len(mismatched_rows)} days")