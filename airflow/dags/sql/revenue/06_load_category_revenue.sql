TRUNCATE TABLE analytics.category_revenue;

INSERT INTO analytics.category_revenue
SELECT
    transaction_date,
    merchant_category,
    count() AS total_transaction_count,
    countIf(transaction_status = 'completed') AS completed_count,
    countIf(transaction_status = 'failed') AS failed_count,
    countIf(transaction_status = 'refunded') AS refunded_count,
    sumIf(amount, transaction_status = 'completed') AS completed_revenue,
    sumIf(amount, transaction_status = 'refunded') AS refunded_amount,
    sumIf(amount, transaction_status = 'failed') AS failed_amount,
    sumIf(amount, transaction_status = 'completed') - sumIf(amount, transaction_status = 'refunded') AS net_revenue,
    if(isNaN(avgIf(amount, transaction_status = 'completed')), 0, avgIf(amount, transaction_status = 'completed')) AS avg_completed_amount,
    if(isNaN(avgIf(amount, transaction_status = 'refunded')), 0, avgIf(amount, transaction_status = 'refunded')) AS avg_refunded_amount
FROM analytics.transaction_facts
GROUP BY
    transaction_date,
    merchant_category
ORDER BY
    transaction_date,
    merchant_category;