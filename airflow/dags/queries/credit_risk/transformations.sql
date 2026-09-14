-- Credit Risk & Loan Eligibility
-- ClickHouse transformation models

CREATE DATABASE IF NOT EXISTS analytics;


-- ============================================================
-- 1. USER CREDIT FACTS
-- One row per user combining credit profile, customer
-- attributes, and transaction behavior.
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.user_credit_facts
(
    user_id UInt32,
    score_date Date,
    credit_score UInt16,
    risk_band String,
    loan_eligible UInt8,
    income_band String,
    monthly_spend Float32,
    age UInt8,
    join_date Date,
    baseline_credit_score UInt16,
    transaction_count UInt64,
    completed_spend Float64,
    failed_count UInt64,
    most_recent_transaction_date DateTime,
    activity_segment String
)
ENGINE = MergeTree
ORDER BY user_id;

TRUNCATE TABLE analytics.user_credit_facts;

INSERT INTO analytics.user_credit_facts
SELECT
    c.user_id,
    c.score_date,
    c.credit_score,
    c.risk_band,
    c.loan_eligible,
    c.income_band,
    c.monthly_spend,
    u.age,
    u.join_date,
    u.baseline_credit_score,
    t.transaction_count,
    round(t.completed_spend, 2) AS completed_spend,
    t.failed_count,
    t.most_recent_transaction_date,
    -- Review segment: Low < 20, Medium 20-29, High >= 30.
    -- This is an explainable rule, not an ML prediction.
    multiIf(
        t.transaction_count < 20, 'Low',
        t.transaction_count < 30, 'Medium',
        'High'
    ) AS activity_segment
FROM raw.user_credit_scores AS c
LEFT JOIN raw.users AS u
    ON c.user_id = u.user_id
LEFT JOIN
(
    SELECT
        user_id,
        count() AS transaction_count,
        sumIf(amount, status = 'Completed') AS completed_spend,
        countIf(status = 'Failed') AS failed_count,
        max(transaction_time) AS most_recent_transaction_date
    FROM raw.transactions
    GROUP BY user_id
) AS t
    ON c.user_id = t.user_id;


-- ============================================================
-- 2. LATEST USER CREDIT PROFILE
-- Current decision-ready profile. The source currently contains
-- one supplied score_date row per user; no score history is
-- invented here.
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.latest_user_credit_profile
(
    user_id UInt32,
    score_date Date,
    credit_score UInt16,
    risk_band String,
    loan_eligible UInt8,
    income_band String,
    monthly_spend Float32,
    age UInt8,
    join_date Date,
    baseline_credit_score UInt16,
    transaction_count UInt64,
    completed_spend Float64,
    failed_count UInt64,
    most_recent_transaction_date DateTime,
    activity_segment String
)
ENGINE = MergeTree
ORDER BY user_id;

TRUNCATE TABLE analytics.latest_user_credit_profile;

INSERT INTO analytics.latest_user_credit_profile
SELECT *
FROM analytics.user_credit_facts;


-- ============================================================
-- 3. CREDIT RISK SUMMARY
-- Aggregated by risk band and income band.
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.credit_risk_summary
(
    risk_band String,
    income_band String,
    customer_count UInt64,
    average_score Float64,
    eligible_count UInt64,
    eligibility_rate Float64,
    completed_spend Float64
)
ENGINE = MergeTree
ORDER BY (risk_band, income_band);

TRUNCATE TABLE analytics.credit_risk_summary;

INSERT INTO analytics.credit_risk_summary
SELECT
    risk_band,
    income_band,
    count() AS customer_count,
    round(avg(credit_score), 2) AS average_score,
    countIf(loan_eligible = 1) AS eligible_count,
    round(
        100.0 * countIf(loan_eligible = 1) / count(),
        2
    ) AS eligibility_rate,
    round(sum(completed_spend), 2) AS completed_spend
FROM analytics.latest_user_credit_profile
GROUP BY
    risk_band,
    income_band;
