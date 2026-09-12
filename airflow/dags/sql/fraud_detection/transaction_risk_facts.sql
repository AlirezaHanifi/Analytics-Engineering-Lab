CREATE OR REPLACE TABLE analytics.transaction_risk_facts
ENGINE = MergeTree()
ORDER BY (transaction_date, user_id, transaction_id)
AS
WITH
    normalized_transactions AS
    (
        SELECT
            t.transaction_id,
            t.user_id,
            t.merchant_id,
            toFloat64(t.amount) AS amount,
            t.transaction_time,
            toDate(t.transaction_time) AS transaction_date,
            lowerUTF8(trim(t.status)) AS normalized_status,
            if(empty(trim(m.merchant_name)), 'Unknown', trim(m.merchant_name)) AS merchant_name,
            if(empty(trim(m.category)), 'Unknown', trim(m.category)) AS merchant_category
        FROM raw.transactions AS t
        LEFT JOIN raw.merchants AS m
            ON t.merchant_id = m.merchant_id
    ),
    user_day_metrics AS
    (
        SELECT
            *,
            toUInt32(count() OVER (
                PARTITION BY user_id, transaction_date
            )) AS user_day_transaction_count,
            toUInt32(sum(toUInt8(normalized_status = 'failed')) OVER (
                PARTITION BY user_id, transaction_date
            )) AS user_day_failed_count
        FROM normalized_transactions
    ),
    amount_threshold AS
    (
        SELECT
            quantileExact({{ params.large_amount_quantile }})(toFloat64(amount)) AS threshold
        FROM raw.transactions
    )
SELECT
    transaction_id,
    user_id,
    merchant_id,
    merchant_name,
    merchant_category,
    amount,
    transaction_time,
    transaction_date,
    normalized_status,
    multiIf(
        amount < 50, 'small',
        amount < 200, 'medium',
        amount < 500, 'large',
        'very_large'
    ) AS amount_band,
    toUInt8(normalized_status = 'failed') AS is_failed,
    toUInt8(normalized_status = 'refunded') AS is_refunded,
    user_day_transaction_count,
    user_day_failed_count,
    toFloat64(amount_threshold.threshold) AS unusually_large_amount_threshold,
    toUInt8(amount > amount_threshold.threshold) AS is_unusually_large,
    toUInt8(
        normalized_status = 'failed'
        AND user_day_failed_count >= {{ params.repeated_failure_threshold }}
    ) AS is_repeated_failure,
    toUInt8(
        user_day_transaction_count >= {{ params.high_user_day_volume_threshold }}
    ) AS is_high_user_day_volume
FROM user_day_metrics
CROSS JOIN amount_threshold
