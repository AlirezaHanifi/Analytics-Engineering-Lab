TRUNCATE TABLE analytics.category_revenue;

INSERT INTO analytics.category_revenue
SELECT
    transaction_date,
    merchant_category,
    sumIf(amount, transaction_status = 'completed') AS completed_revenue,
    countIf(transaction_status = 'completed') AS transaction_count,
    avgIf(amount, transaction_status = 'completed') AS average_transaction_amount,
    countIf(transaction_status = 'failed') AS failed_count,
    countIf(transaction_status = 'refunded') AS refunded_count
FROM analytics.transaction_facts
GROUP BY
    transaction_date,
    merchant_category
ORDER BY
    transaction_date,
    merchant_category;
