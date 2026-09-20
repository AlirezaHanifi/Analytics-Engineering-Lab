CREATE OR REPLACE TABLE analytics.fraud_risk_summary
ENGINE = MergeTree()
ORDER BY (alert_date, merchant_category, alert_reason)
AS
WITH
    reason_rows AS
    (
        SELECT
            transaction_date,
            merchant_category,
            transaction_id,
            amount,
            is_failed,
            is_refunded,
            arrayJoin(alert_reasons) AS alert_reason
        FROM analytics.fraud_alerts
    ),
    source_counts AS
    (
        SELECT
            transaction_date,
            merchant_category,
            count() AS source_transaction_count
        FROM analytics.transaction_risk_facts
        GROUP BY transaction_date, merchant_category
    )
SELECT
    r.transaction_date AS alert_date,
    r.merchant_category,
    r.alert_reason,
    toUInt64(count()) AS alert_count,
    toUInt64(uniqExact(r.transaction_id)) AS distinct_transaction_count,
    toUInt64(any(s.source_transaction_count)) AS source_transaction_count,
    toFloat64(count()) / nullIf(toFloat64(any(s.source_transaction_count)), 0) AS alert_rate,
    toUInt64(sum(r.is_failed)) AS failed_count,
    toUInt64(sum(r.is_refunded)) AS refunded_count,
    toFloat64(sum(r.amount)) AS transaction_amount
FROM reason_rows AS r
INNER JOIN source_counts AS s
    ON r.transaction_date = s.transaction_date
    AND r.merchant_category = s.merchant_category
GROUP BY
    r.transaction_date,
    r.merchant_category,
    r.alert_reason
