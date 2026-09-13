CREATE TABLE IF NOT EXISTS analytics.category_revenue
(
    transaction_date Date,
    merchant_category LowCardinality(String),
    total_transaction_count UInt64,
    completed_count UInt64,
    failed_count UInt64,
    refunded_count UInt64,
    completed_revenue Float64,
    refunded_amount Float64,
    failed_amount Float64,
    net_revenue Float64,
    avg_completed_amount Float64,
    avg_refunded_amount Float64
)
ENGINE = MergeTree()
ORDER BY (transaction_date, merchant_category);