SELECT
    'fact_rows_match_source' AS check_name,
    (SELECT count() FROM analytics.transaction_risk_facts) AS actual,
    (SELECT count() FROM raw.transactions) AS expected

UNION ALL

SELECT
    'facts_preserve_transaction_grain',
    (SELECT count() FROM analytics.transaction_risk_facts),
    (SELECT uniqExact(transaction_id) FROM analytics.transaction_risk_facts)

UNION ALL

SELECT
    'all_merchants_resolved',
    (
        SELECT countIf(merchant_category = 'Unknown')
        FROM analytics.transaction_risk_facts
    ),
    toUInt64(0)

UNION ALL

SELECT
    'every_alert_maps_to_source',
    (
        SELECT count()
        FROM analytics.fraud_alerts
        WHERE transaction_id NOT IN
        (
            SELECT transaction_id
            FROM raw.transactions
        )
    ),
    toUInt64(0)

UNION ALL

SELECT
    'every_alert_has_a_reason',
    (
        SELECT countIf(empty(alert_reasons) OR empty(alert_reason))
        FROM analytics.fraud_alerts
    ),
    toUInt64(0)

UNION ALL

SELECT
    'alerts_preserve_transaction_grain',
    (SELECT count() FROM analytics.fraud_alerts),
    (SELECT uniqExact(transaction_id) FROM analytics.fraud_alerts)

UNION ALL

SELECT
    'alerts_match_rule_union',
    (SELECT count() FROM analytics.fraud_alerts),
    (
        SELECT count()
        FROM analytics.transaction_risk_facts
        WHERE is_unusually_large = 1
           OR is_repeated_failure = 1
           OR is_refunded = 1
           OR is_high_user_day_volume = 1
    )

UNION ALL

SELECT
    'unusually_large_rule_reconciles',
    (
        SELECT countIf(has(alert_reasons, 'unusually_large_amount'))
        FROM analytics.fraud_alerts
    ),
    (
        SELECT countIf(is_unusually_large = 1)
        FROM analytics.transaction_risk_facts
    )

UNION ALL

SELECT
    'repeated_failure_rule_reconciles',
    (
        SELECT countIf(has(alert_reasons, 'repeated_failed_payment'))
        FROM analytics.fraud_alerts
    ),
    (
        SELECT countIf(is_repeated_failure = 1)
        FROM analytics.transaction_risk_facts
    )

UNION ALL

SELECT
    'refunded_payment_rule_reconciles',
    (
        SELECT countIf(has(alert_reasons, 'refunded_payment'))
        FROM analytics.fraud_alerts
    ),
    (
        SELECT countIf(is_refunded = 1)
        FROM analytics.transaction_risk_facts
    )

UNION ALL

SELECT
    'high_volume_rule_reconciles',
    (
        SELECT countIf(has(alert_reasons, 'high_user_day_volume'))
        FROM analytics.fraud_alerts
    ),
    (
        SELECT countIf(is_high_user_day_volume = 1)
        FROM analytics.transaction_risk_facts
    )

UNION ALL

SELECT
    'every_rule_has_an_example',
    (
        SELECT uniqExact(reason)
        FROM analytics.fraud_alerts
        ARRAY JOIN alert_reasons AS reason
    ),
    toUInt64(4)

UNION ALL

SELECT
    'summary_matches_exploded_reasons',
    (
        SELECT coalesce(sum(alert_count), 0)
        FROM analytics.fraud_risk_summary
    ),
    (
        SELECT coalesce(sum(length(alert_reasons)), 0)
        FROM analytics.fraud_alerts
    )

UNION ALL

SELECT
    'daily_category_summary_reconciles',
    (
        SELECT count()
        FROM
        (
            SELECT
                toUInt8(1) AS expected_present,
                transaction_date AS alert_date,
                merchant_category,
                arrayJoin(alert_reasons) AS alert_reason,
                count() AS alert_count,
                uniqExact(transaction_id) AS distinct_transaction_count,
                sum(is_failed) AS failed_count,
                sum(is_refunded) AS refunded_count,
                sum(amount) AS transaction_amount
            FROM analytics.fraud_alerts
            GROUP BY alert_date, merchant_category, alert_reason
        ) AS expected_summary
        FULL OUTER JOIN
        (
            SELECT *, toUInt8(1) AS actual_present
            FROM analytics.fraud_risk_summary
        ) AS actual_summary
            ON expected_summary.alert_date = actual_summary.alert_date
            AND expected_summary.merchant_category = actual_summary.merchant_category
            AND expected_summary.alert_reason = actual_summary.alert_reason
        -- Presence flags catch missing groups even when their metrics are zero.
        WHERE coalesce(expected_summary.expected_present, 0) = 0
           OR coalesce(actual_summary.actual_present, 0) = 0
           OR coalesce(expected_summary.alert_count, 0)
                != coalesce(actual_summary.alert_count, 0)
           OR coalesce(expected_summary.distinct_transaction_count, 0)
                != coalesce(actual_summary.distinct_transaction_count, 0)
           OR coalesce(expected_summary.failed_count, 0)
                != coalesce(actual_summary.failed_count, 0)
           OR coalesce(expected_summary.refunded_count, 0)
                != coalesce(actual_summary.refunded_count, 0)
           OR abs(
                coalesce(expected_summary.transaction_amount, 0)
                - coalesce(actual_summary.transaction_amount, 0)
           ) > 0.01
    ),
    toUInt64(0)
