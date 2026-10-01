-- Staging model: the clean, de-duplicated, analytics-ready view of bronze.
-- Equivalent to a dbt staging model (materialised as a view).
--
-- Responsibilities:
--   1. Exclude records flagged as natural-key duplicates.
--   2. Derive the UTC calendar date used for daily grain aggregation.
--   3. Expose typed, renamed columns so marts never touch raw_transactions.
--
-- Quarantined records are physically absent from raw_transactions, so they are
-- excluded from every downstream aggregate by construction, not by a filter.

CREATE OR REPLACE VIEW stg_transactions AS
SELECT
    transaction_id,
    account_id,
    transaction_date                             AS transaction_timestamp,
    CAST(transaction_date AS DATE)               AS transaction_date,
    amount,
    currency,
    transaction_type,
    merchant_name,
    merchant_category,
    status,
    country_code,
    natural_key_hash,
    ingestion_run_id,
    ingestion_timestamp
FROM raw_transactions
WHERE is_duplicate = FALSE;