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
        WHERE lower(trim(status)) = 'completed'
    ) AS raw_completed_revenue,
    (
        SELECT sum(amount)
        FROM analytics.transaction_facts
        WHERE transaction_status = 'completed'
    ) AS fact_completed_revenue;

SELECT
    d.transaction_date,
    d.completed_revenue AS daily_completed_revenue,
    c.category_completed_revenue,
    d.net_revenue AS daily_net_revenue,
    c.category_net_revenue,
    d.total_transaction_count AS daily_transaction_count,
    c.category_transaction_count
FROM analytics.daily_revenue AS d
LEFT JOIN
(
    SELECT
        transaction_date,
        sum(completed_revenue) AS category_completed_revenue,
        sum(net_revenue) AS category_net_revenue,
        sum(total_transaction_count) AS category_transaction_count
    FROM analytics.category_revenue
    GROUP BY transaction_date
) AS c
    ON d.transaction_date = c.transaction_date
WHERE
    abs(d.completed_revenue - c.category_completed_revenue) > 0.01
    OR abs(d.net_revenue - c.category_net_revenue) > 0.01
    OR d.total_transaction_count != c.category_transaction_count;