SELECT
    (SELECT count() FROM raw.transactions) AS raw_transaction_count,
    (SELECT count() FROM analytics.transaction_facts) AS fact_transaction_count;


SELECT
    countIf(merchant_category = '') AS missing_merchant_category_count
FROM analytics.transaction_facts;


SELECT
    (
        SELECT sum(amount)
        FROM raw.transactions
        WHERE status = 'Completed'
    ) AS raw_completed_revenue,
    (
        SELECT sum(amount)
        FROM analytics.transaction_facts
        WHERE transaction_status = 'Completed'
    ) AS fact_completed_revenue;




SELECT
    d.transaction_date,
    d.completed_revenue AS daily_completed_revenue,
    c.category_completed_revenue,
    d.transaction_count AS daily_transaction_count,
    c.category_transaction_count
FROM analytics.daily_revenue AS d
LEFT JOIN
(
    SELECT
        transaction_date,
        sum(completed_revenue) AS category_completed_revenue,
        sum(transaction_count) AS category_transaction_count
    FROM analytics.category_revenue
    GROUP BY transaction_date
) AS c
    ON d.transaction_date = c.transaction_date
WHERE
    d.completed_revenue != c.category_completed_revenue
    OR d.transaction_count != c.category_transaction_count;
