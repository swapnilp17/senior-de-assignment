-- Mart: daily_account_summary
-- Grain: one row per (account_id, transaction_date)
--
-- Rules (per the assessment brief):
--   * Only status = 'completed' is included.
--   * Quarantined records are excluded (never reach raw_transactions).
--   * Duplicates are excluded (filtered in stg_transactions).
--
-- Idempotency: CREATE OR REPLACE TABLE fully rebuilds the mart from its inputs.
-- Running it N times with unchanged inputs yields byte-identical results, except
-- `updated_at` which is an intentional freshness/audit marker.
--
-- Note on `currencies`: amounts are NOT FX-converted. Sums mix currencies, so the
-- currencies array is exposed to make that explicit to consumers. See README
-- "Known limitations" - a production version would convert to a reporting
-- currency using a daily FX rate dimension.

CREATE OR REPLACE TABLE daily_account_summary AS

WITH completed AS (
    SELECT *
    FROM stg_transactions
    WHERE status = 'completed'
),

-- Rank categories by total spend per account-day.
-- Tie-break on merchant_category ASC makes the winner deterministic.
category_spend AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        SUM(amount) AS category_amount,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_date
            ORDER BY SUM(amount) DESC, merchant_category ASC
        ) AS category_rank
    FROM completed
    GROUP BY account_id, transaction_date, merchant_category
),

top_category AS (
    SELECT account_id, transaction_date, merchant_category AS top_category
    FROM category_spend
    WHERE category_rank = 1
),

aggregated AS (
    SELECT
        account_id,
        transaction_date,
        CAST(COALESCE(SUM(CASE WHEN transaction_type = 'debit'  THEN amount END), 0) AS DECIMAL(18,2)) AS total_debit_amount,
        CAST(COALESCE(SUM(CASE WHEN transaction_type = 'credit' THEN amount END), 0) AS DECIMAL(18,2)) AS total_credit_amount,
        COUNT(*)                                   AS transaction_count,
        COUNT(DISTINCT merchant_name)              AS distinct_merchants,
        -- Sorted so the string is stable across runs (idempotency).
        LIST_SORT(LIST(DISTINCT currency))         AS currency_list
    FROM completed
    GROUP BY account_id, transaction_date
)

SELECT
    a.account_id,
    a.transaction_date,
    a.total_debit_amount,
    a.total_credit_amount,
    CAST(a.total_credit_amount - a.total_debit_amount AS DECIMAL(18,2)) AS net_amount,
    a.transaction_count,
    a.distinct_merchants,
    t.top_category,
    ARRAY_TO_STRING(a.currency_list, ',')          AS currencies,
    CAST(NOW() AT TIME ZONE 'UTC' AS TIMESTAMP)    AS updated_at
FROM aggregated a
LEFT JOIN top_category t
       ON a.account_id = t.account_id
      AND a.transaction_date = t.transaction_date
ORDER BY a.account_id, a.transaction_date;