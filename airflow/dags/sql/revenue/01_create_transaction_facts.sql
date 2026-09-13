CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.transaction_facts
(
    transaction_id UUID,
    user_id UInt32,
    amount Float64,
    transaction_time DateTime,
    transaction_date Date,
    transaction_status LowCardinality(String),
    merchant_id UInt32,
    merchant_name String,
    merchant_category LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (transaction_date, merchant_id, transaction_id);