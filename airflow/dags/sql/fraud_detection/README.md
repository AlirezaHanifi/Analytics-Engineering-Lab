# Fraud detection data model

This domain produces explainable operational-review alerts. A flagged row is a
candidate for investigation and must not be presented as confirmed fraud.

## Model grains

- `analytics.transaction_risk_facts`: one row per `raw.transactions.transaction_id`.
  It normalizes transaction status, joins merchant context, and calculates the
  deterministic features used by the alert rules.
- `analytics.fraud_alerts`: one row per flagged transaction. `alert_reasons` is
  an array so one transaction can retain multiple explanations without changing
  the transaction grain. `alert_reason` is the display-friendly joined value.
- `analytics.fraud_risk_summary`: one row per transaction date, merchant
  category, and individual alert reason. Because a transaction can have multiple
  reasons, reason-level counts are not additive to the unique alert total.

## Alert rules

1. `unusually_large_amount`: amount is greater than the exact 99th percentile
   of the current raw transaction dataset. The calculated threshold is retained
   in the fact table for auditability.
2. `repeated_failed_payment`: the transaction failed and the same user has at
   least two failed transactions on that date.
3. `refunded_payment`: normalized transaction status is `refunded`.
4. `high_user_day_volume`: the user has at least three transactions on that date.

The thresholds are Airflow DAG parameters and are rendered into
`transaction_risk_facts.sql`. They are deterministic for a given raw dataset.

## Rerun behavior

Each model uses `CREATE OR REPLACE TABLE ... AS SELECT`. A rerun replaces the
previous modeled result instead of appending rows, preventing duplicate facts or
alerts. The DAG permits only one active run to avoid concurrent replacements.

## Validation

`validate_fraud_models.sql` checks source-to-fact row reconciliation, transaction
grain, merchant coverage, source mapping for every alert, non-empty reasons,
each individual rule, rule example coverage, and summary reconciliation at the
date/category/reason grain. Airflow fails the validation task if any actual and
expected values differ.
