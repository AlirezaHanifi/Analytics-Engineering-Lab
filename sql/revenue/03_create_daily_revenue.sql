CREATE TABLE IF NOT EXISTS analytics.daily_revenue
(
    transaction_date Date,
    completed_revenue Float64,
    transaction_count UInt64,
    average_transaction_amount Float64,
    failed_count UInt64,
    refunded_count UInt64
)
ENGINE = MergeTree
ORDER BY transaction_date;
