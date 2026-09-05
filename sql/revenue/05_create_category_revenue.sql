CREATE TABLE IF NOT EXISTS analytics.category_revenue
(
    transaction_date Date,
    merchant_category String,
    completed_revenue Float64,
    transaction_count UInt64,
    average_transaction_amount Float64,
    failed_count UInt64,
    refunded_count UInt64
)
ENGINE = MergeTree
ORDER BY (transaction_date, merchant_category);
