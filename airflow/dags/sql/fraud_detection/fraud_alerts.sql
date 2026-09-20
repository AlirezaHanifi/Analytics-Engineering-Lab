CREATE OR REPLACE TABLE analytics.fraud_alerts
ENGINE = MergeTree()
ORDER BY (transaction_date, user_id, transaction_id)
AS
WITH prepared_alerts AS
(
    SELECT
        *,
        arrayFilter(
            reason -> reason != '',
            [
                if(is_unusually_large = 1, 'unusually_large_amount', ''),
                if(is_repeated_failure = 1, 'repeated_failed_payment', ''),
                if(is_refunded = 1, 'refunded_payment', ''),
                if(is_high_user_day_volume = 1, 'high_user_day_volume', '')
            ]
        ) AS alert_reasons
    FROM analytics.transaction_risk_facts
)
SELECT
    transaction_id,
    user_id,
    merchant_id,
    merchant_name,
    merchant_category,
    amount,
    amount_band,
    transaction_time,
    transaction_date,
    normalized_status,
    is_failed,
    is_refunded,
    user_day_transaction_count,
    user_day_failed_count,
    unusually_large_amount_threshold,
    alert_reasons,
    arrayStringConcat(alert_reasons, ', ') AS alert_reason
FROM prepared_alerts
WHERE notEmpty(alert_reasons)
