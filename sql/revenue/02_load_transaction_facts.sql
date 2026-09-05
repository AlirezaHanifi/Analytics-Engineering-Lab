TRUNCATE TABLE analytics.transaction_facts;

INSERT INTO analytics.transaction_facts
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.transaction_time,
    toDate(t.transaction_time) AS transaction_date,
    t.status AS transaction_status,
    t.merchant_id,
    m.merchant_name,
    m.category AS merchant_category
FROM raw.transactions AS t
LEFT JOIN raw.merchants AS m
    ON t.merchant_id = m.merchant_id;
