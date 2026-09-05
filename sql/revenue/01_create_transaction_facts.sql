CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.transaction_facts
(
    transaction_id UUID,
    user_id UInt32,
    amount Float32,
    transaction_time DateTime,
    transaction_date Date,
    transaction_status String,
    merchant_id UInt32,
    merchant_name String,
    merchant_category String
)
ENGINE = MergeTree
ORDER BY (transaction_date, merchant_id, transaction_id);
