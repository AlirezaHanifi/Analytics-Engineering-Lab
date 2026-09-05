TRUNCATE TABLE analytics.category_revenue;

INSERT INTO analytics.category_revenue
SELECT
    transaction_date,
    merchant_category,
    sumIf(amount, transaction_status = 'Completed') AS completed_revenue,
    countIf(transaction_status = 'Completed') AS transaction_count,
    avgIf(amount, transaction_status = 'Completed') AS average_transaction_amount,
    countIf(transaction_status = 'Failed') AS failed_count,
    countIf(transaction_status = 'Refunded') AS refunded_count
FROM analytics.transaction_facts
GROUP BY
    transaction_date,
    merchant_category
ORDER BY
    transaction_date,
    merchant_category;
