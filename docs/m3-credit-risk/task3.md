# Credit Risk & Loan Eligibility

## Overview

This pipeline demonstrates how supplied customer credit information and
transaction behavior can be used to support lending review.

The supplied `loan_eligible` field remains the source lending decision.
Transaction behavior is used only as additional review context and does not
replace the supplied eligibility decision.

## Eligibility Rule

The synthetic source data defines a customer as eligible when:

- `credit_score >= 640`
- `income_band IN ('Medium-High', 'High')`

No new ML credit score or eligibility prediction is created.

## Activity Segment

Transaction activity is categorized as:

- Low: fewer than 20 transactions
- Medium: 20–29 transactions
- High: 30 or more transactions

This segment is a descriptive review feature, not an ML prediction.

## Data Models

The pipeline creates three ClickHouse analytics tables:

- `analytics.user_credit_facts`
- `analytics.latest_user_credit_profile`
- `analytics.credit_risk_summary`

## Validation Results

The pipeline reconciliation produced:

- Source credit profiles: 5,000
- Modeled credit profiles: 5,000
- Unique modeled users: 5,000
- Source eligible users: 1,055
- Modeled eligible users: 1,055
- Eligibility rule mismatches: 0

The summary model also reconciles to 5,000 customers and 1,055 eligible
customers.

## Example Lending Review

User `2745` has:

- Credit score: 688
- Income band: Medium-High
- Eligibility: Eligible
- Activity segment: Medium
- Transaction count: 29
- Failed transactions: 13
- Failed transaction rate: 44.83%
- Completed spend: 825.61
- Monthly spend: 1,971.20

The customer satisfies the supplied eligibility rule because the credit score
is at least 640 and the income band is Medium-High. However, 13 of 29
transactions failed.

The failed-transaction behavior does not change the supplied eligibility
decision, but it provides additional context that a lender may wish to review.

## Dashboard

The Metabase **Credit Risk** dashboard includes:

- Eligible customer count
- Eligibility rate
- Average credit score
- Credit score distribution
- Customer distribution by risk band
- Income and risk-band segmentation
- Eligibility rate by income band
- Transaction activity and completed-spend comparison by eligibility
- Customer-level credit review table