-- Credit Risk & Loan Eligibility
-- Validation queries


-- ============================================================
-- 1. CHECK ONE CREDIT PROFILE PER USER
-- ============================================================

SELECT
    count() AS row_count,
    uniqExact(user_id) AS unique_users
FROM analytics.latest_user_credit_profile;


-- ============================================================
-- 2. CHECK THAT CREDIT ROWS WERE NOT LOST
-- ============================================================

SELECT
    (SELECT count()
     FROM raw.user_credit_scores) AS raw_credit_rows,

    (SELECT count()
     FROM analytics.user_credit_facts) AS modeled_credit_rows;


-- ============================================================
-- 3. CHECK ELIGIBILITY RECONCILIATION
-- ============================================================

SELECT
    (SELECT countIf(loan_eligible = 1)
     FROM raw.user_credit_scores) AS raw_eligible,

    (SELECT countIf(loan_eligible = 1)
     FROM analytics.latest_user_credit_profile) AS modeled_eligible;


-- ============================================================
-- 4. CHECK SUMMARY RECONCILIATION
-- ============================================================

SELECT
    (SELECT count()
     FROM analytics.latest_user_credit_profile) AS profile_customers,

    (SELECT sum(customer_count)
     FROM analytics.credit_risk_summary) AS summary_customers,

    (SELECT countIf(loan_eligible = 1)
     FROM analytics.latest_user_credit_profile) AS profile_eligible,

    (SELECT sum(eligible_count)
     FROM analytics.credit_risk_summary) AS summary_eligible;


-- ============================================================
-- 5. CHECK THE SUPPLIED ELIGIBILITY RULE
--
-- Expected synthetic rule:
-- credit_score >= 640
-- AND income_band IN ('Medium-High', 'High')
--
-- A result of 0 means that loan_eligible agrees with the rule
-- for every source row.
-- ============================================================

SELECT count() AS eligibility_rule_mismatches
FROM raw.user_credit_scores
WHERE loan_eligible !=
    (
        (credit_score >= 640)
        AND income_band IN ('Medium-High', 'High')
    );


-- ============================================================
-- 6. CHECK THAT EVERY CREDIT ROW HAS A MATCHING USER
-- ============================================================

SELECT count() AS credit_rows_without_user
FROM raw.user_credit_scores
WHERE user_id NOT IN
    (
        SELECT user_id
        FROM raw.users
    );


-- ============================================================
-- 7. CHECK TRANSACTION AGGREGATION RECONCILIATION
-- ============================================================

SELECT
    (SELECT count()
     FROM raw.transactions) AS raw_transaction_rows,

    (SELECT sum(transaction_count)
     FROM analytics.user_credit_facts) AS modeled_transaction_rows;
