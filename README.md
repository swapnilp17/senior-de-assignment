# Senior Data Engineer Take-Home — Payment Transactions Pipeline

A production-minded ingestion and transformation pipeline that fetches payment
transactions from a REST API, validates and quarantines defective records,
persists a raw/bronze layer, produces a daily account-level summary, and
supports watermark-based incremental ingestion.

---

## 1. Results at a glance

Produced by a real end-to-end run against the assessment API:

| Metric | Value |
|---|---|
| Records fetched (full load) | **352** |
| Valid records persisted to bronze | **349** |
| Records quarantined | **3** |
| Natural-key duplicates flagged | **5** |
| `daily_account_summary` rows | **238** |
| Distinct accounts | **20** |
| High-water mark after run 1 | `2024-03-30 21:01:36` |
| Records fetched (incremental run 2) | **15** (vs 352 — the API filter works) |
| Row count after run 2 | **349 — unchanged** ✅ |
| Declarative data tests | **22 / 22 passing** |
| Unit + integration tests | **110 / 110 passing** |
| Idempotency check | **PASS** (identical checksum across rebuilds) |

---

## 2. Technology choice and rationale

The brief recommends Databricks Community Edition but permits documented
alternatives. This solution uses:

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.13 | No PySpark dependency needed at this data volume |
| Warehouse | **DuckDB** | Zero-install embedded OLAP engine, real SQL, ACID, runs anywhere |
| Transformation | **SQL models + a dbt-compatible project layout** | Same mental model as dbt without the dependency |
| Testing | **pytest** + a YAML-driven data-test runner | Covers both code logic and data quality |

### Why not dbt?

`dbt-core` does not yet officially support Python 3.13 (supported range is
3.9–3.12). Rather than pin an older interpreter, the project keeps the dbt
*structure and semantics* and implements the two commands it needs:

| dbt concept | This repo |
|---|---|
| `models/staging/*.sql` | Same path, same purpose |
| `models/marts/*.sql` | Same path, same purpose |
| `schema.yml` tests | Same file, same keys (`not_null`, `unique`, `accepted_values`) |
| `dbt run` | `python -m scripts.run_models` (explicit DAG order) |
| `dbt test` | `python -m scripts.run_tests` (reads `schema.yml`) |
| `dbt docs` | Column descriptions in `schema.yml` + the lineage section below |

Migrating to real dbt is a profile change plus wrapping model bodies in
`{{ config() }}` / `{{ ref() }}` — the SQL itself is unchanged.

---

## 3. Setup from a clean environment

```bash
git clone <repo-url>
cd senior-de-assignment

python -m venv .venv
# Windows
.\.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt

cp .env.example .env     # then fill in the API credentials
```

### Environment variables

| Variable | Purpose |
|---|---|
| `ASSESSMENT_API_BASE_URL` | API base URL |
| `ASSESSMENT_API_KEY` | Sent in the `apikey` header |
| `ASSESSMENT_AUTH_TOKEN` | Sent as `Authorization: Bearer <token>` |
| `API_PAGE_SIZE` | Pagination page size (default 200) |
| `API_MAX_RETRIES` | Retry attempts before failing (default 5) |
| `API_TIMEOUT_SECONDS` | Per-request timeout (default 30) |
| `DUCKDB_PATH` | Warehouse file location |
| `WATERMARK_LOOKBACK_DAYS` | Late-arriving-data overlap (default 2) |
| `STRICT_DATE_WINDOW` | Quarantine timestamps outside 2024 Q1 |
| `LOG_LEVEL` | Logging verbosity |

**No secrets are committed.** `.env` is git-ignored; `.env.example` is a
placeholder template. Credentials are never hardcoded — `config.py` raises
`ConfigError` if a required variable is missing.

> **Note:** if your project lives inside a cloud-synced folder (OneDrive,
> Google Drive), point `DUCKDB_PATH` at a path *outside* it. Sync clients hold
> file locks that DuckDB cannot acquire.

---

## 4. How to run

```bash
# Task 1 — full ingestion into bronze + quarantine
python -m ingestion.ingest_transactions --reset

# Task 3 — incremental ingestion using the stored watermark
python -m ingestion.incremental_ingest

# Task 2 — build staging + mart models
python -m scripts.run_models

# Declarative data tests (the `dbt test` equivalent)
python -m scripts.run_tests

# Prove the transformation is idempotent
python -m scripts.check_idempotency

# Unit + integration tests
pytest

# Export sample outputs for review
python -m scripts.export_outputs
```

---

## 5. Architecture

```
          ┌─────────────────────┐
          │  REST API           │  offset pagination + transaction_date filter
          └──────────┬──────────┘
                     │  retry w/ exponential backoff + jitter (429, 5xx, timeouts)
          ┌──────────▼──────────┐
          │  api_client.py      │
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  validation.py      │  12 contract rules + natural-key hash
          └─────┬──────────┬────┘
         valid  │          │  invalid
    ┌───────────▼──┐   ┌───▼────────────────────────┐
    │ raw_         │   │ quarantine_transactions    │
    │ transactions │   │ (error_reason, ingestion_  │
    │ (bronze)     │   │  timestamp, raw_payload)   │
    └───────┬──────┘   └────────────────────────────┘
            │  is_duplicate = false
    ┌───────▼────────────┐
    │ stg_transactions   │  view
    └───────┬────────────┘
            │  status = 'completed'
    ┌───────▼────────────────┐
    │ daily_account_summary  │  table — one row per account_id + date
    └────────────────────────┘

    Side tables: ingestion_watermark (HWM)  •  ingestion_runs (audit)
```

### Lineage

| Model | Depends on | Materialisation |
|---|---|---|
| `raw_transactions` | REST API | table (bronze) |
| `quarantine_transactions` | REST API | table (dead-letter) |
| `stg_transactions` | `raw_transactions` | view |
| `daily_account_summary` | `stg_transactions` | table |

---

## 6. Validation strategy

Each record is checked against **every** rule before a verdict is issued —
validation never short-circuits, so a record with four defects reports all four.

| Field | Rules enforced |
|---|---|
| `transaction_id` | Required, format `TXN-NNNN` |
| `account_id` | Required, format `ACC-NNNN` |
| `transaction_date` | Required, strict ISO-8601 UTC (`T` separator, `Z` suffix), **real calendar date**, within the expected 2024 Q1 window |
| `amount` | Required, numeric, **strictly > 0**, max 2 decimal places |
| `currency` | Required, **case-sensitive** enum of 7 codes |
| `transaction_type` | Required, **case-sensitive** `debit` / `credit` |
| `merchant_name` | Required, **non-empty and not whitespace-only** |
| `merchant_category` | Required, **case-sensitive** enum of 12 values |
| `status` | Required, **case-sensitive** enum of 4 values |
| `country_code` | Required, 2 uppercase letters, **and an officially assigned ISO 3166-1 alpha-2 code** (validated against `pycountry`) |

Four rules deserve emphasis because format checks alone would miss them:

1. **`amount > 0` is exclusive.** `0.00` is invalid, not just negatives.
2. **Enums are case-sensitive.** `Completed` and `Credit` are rejected.
3. **Country codes must be *assigned*.** `UK` and `EN` match `^[A-Z]{2}$` but
   are not ISO 3166-1 codes (the UK is `GB`). The brief states explicitly that
   "format alone is not sufficient".
4. **Dates must be real.** `2024-11-31T14:22:00Z` passes a regex but November
   has 30 days.

### Quarantine

Invalid records go to `quarantine_transactions` with:

- `error_reason` — all violations, semicolon-separated, with the offending value
- `error_count` — number of distinct violations
- `raw_payload` — the untouched JSON for replay after a fix
- `ingestion_timestamp`, `ingestion_run_id` — provenance
- `quarantine_key` — SHA-256 of the canonical payload, used as the primary key
  so re-runs cannot create duplicate quarantine rows

**Actual captured defects (3 records, 10 distinct violations):**

| Defects in record |
|---|
| `transaction_date: invalid_iso8601_utc_format ('2024-04-15 09:30:00')` · `amount: not_strictly_positive ('-127.5')` · `status: invalid_status ('Completed')` · `merchant_name: empty_or_whitespace_only` |
| `transaction_date: not_a_real_calendar_date ('2024-11-31T14:22:00Z')` · `merchant_category: invalid_merchant_category ('fast_food')` · `country_code: not_an_assigned_iso3166_code ('UK')` |
| `amount: not_strictly_positive ('0.0')` · `transaction_type: invalid_transaction_type ('Credit')` · `merchant_category: invalid_merchant_category ('Finance')` · `country_code: not_an_assigned_iso3166_code ('EN')` |

Nothing is dropped and nothing crashes the run. See
[`outputs/quarantine_sample.csv`](outputs/quarantine_sample.csv).

---

## 7. Duplicate handling — decision and rationale

**Decision: flag, do not delete.**

A `natural_key_hash` (SHA-256) is computed over all contract fields **except
`transaction_id`**, matching the brief's definition exactly. Two columns are
maintained on the bronze table:

- `is_duplicate` — boolean flag
- `duplicate_of_transaction_id` — pointer to the surviving record

**Survivor rule:** earliest `transaction_date`, tie-broken by lowest
`transaction_id`. Fully deterministic.

**Why flag rather than delete:**

1. Bronze stays a faithful, replayable copy of the source. "What did the API
   actually send us?" remains answerable.
2. The dedup rule is a *business* decision. If it turns out to be wrong, it is
   corrected with one `UPDATE` — no re-ingestion needed.
3. Duplicate rate becomes a measurable data-quality metric rather than an
   invisible deletion.
4. Downstream consumers are never exposed to them: `stg_transactions` filters
   `is_duplicate = false`, so every mart excludes them automatically.

`recompute_duplicates()` runs over the **entire** table after each load, not
just the current batch, so the result is identical regardless of batch
boundaries or arrival order.

**Result:** 5 duplicate records detected. See
[`outputs/duplicate_pairs_sample.csv`](outputs/duplicate_pairs_sample.csv) —
each pair shares every field except `transaction_id`.

---

## 8. Incremental ingestion and watermark strategy

### Watermark store

`ingestion_watermark` holds one row per source:

| Column | Meaning |
|---|---|
| `source_name` | Logical source identifier |
| `watermark_value` | Max `transaction_date` successfully ingested |
| `effective_request_from` | What the last run actually requested (watermark − lookback) |
| `last_run_id` / `last_run_at` / `last_run_status` | Audit trail |
| `records_ingested` | Volume for the last run |

### Behaviour

| Scenario | Behaviour |
|---|---|
| **First run** | No watermark exists → full load, no date filter. Watermark set to max ingested `transaction_date`. |
| **Subsequent run** | Requests `transaction_date=gte.(watermark − lookback)` using the **API's native filter**. |
| **No new data** | Returns only the lookback overlap. Upserts are no-ops, row count unchanged, watermark holds. Run succeeds. |
| **Failure** | Watermark is **not** advanced. The next run safely re-reads the same window rather than silently skipping records. |
| **Watermark regression** | Guarded — the watermark never moves backwards. |

### Lookback window — yes, 2 days, and here is why

The API filter is on `transaction_date` (**event time**), not an API-side
ingestion timestamp. A record created late but *dated* earlier would be
permanently invisible to a strict `>= watermark` filter.

Re-reading a 2-day overlap costs almost nothing because **every write is an
upsert on the primary key**, so re-processing is a no-op. This trades a small
amount of redundant I/O for a meaningful reduction in silent data loss —
the right trade for a financial dataset.

2 days is a deliberate placeholder. In production this would be derived from
observed lag: measure the distribution of `ingested_at − transaction_date`
and set the window at roughly the p99.

**Better long-term fix:** if the source exposed a monotonic `updated_at` or a
change-data-capture feed, the watermark would track *that* instead, and the
lookback could shrink to near zero.

### Proof from the actual runs

```
run_mode     fetched valid quar dups  previous_watermark   new_watermark
full         352     349   3    5     None                 2024-03-30 21:01:36
incremental   15      13   2    5     2024-03-30 21:01:36  2024-03-30 21:01:36

Row count before run 2: 349
Row count after  run 2: 349   ← no duplicate rows inserted
```

352 → 15 fetched confirms the native API filter is genuinely narrowing the
request. See [`outputs/watermark_run1.json`](outputs/watermark_run1.json) and
[`outputs/watermark_run2.json`](outputs/watermark_run2.json).

---

## 9. `daily_account_summary`

Grain: **one row per `account_id` + `transaction_date`**.

| Field | Description |
|---|---|
| `account_id` | Account identifier |
| `transaction_date` | UTC calendar date truncated from the timestamp |
| `total_debit_amount` | Sum of debit amounts, completed only |
| `total_credit_amount` | Sum of credit amounts, completed only |
| `net_amount` | `total_credit_amount − total_debit_amount` |
| `transaction_count` | Count of completed transactions |
| `distinct_merchants` | Distinct `merchant_name` values that day |
| `top_category` | Merchant category with the highest total spend that day |
| `currencies` | Distinct currencies that day, comma-separated |
| `updated_at` | UTC timestamp when the row was last computed |

**Filters applied:** `status = 'completed'` only; duplicates excluded via
`stg_transactions`; quarantined records are physically absent from bronze and
therefore excluded by construction, not by a filter.

**`top_category` determinism:** ranked by `SUM(amount) DESC` with a
`merchant_category ASC` tie-break, so ties never flip between runs.

### Idempotency

`CREATE OR REPLACE TABLE` rebuilds the mart from its inputs. Verified
mechanically:

```
Before rebuild : rows=238  checksum=2302923243264956972778
After  rebuild : rows=238  checksum=2302923243264956972778
IDEMPOTENCY PASS
```

`updated_at` is excluded from the checksum — it is an audit field whose purpose
is to change.

---

## 10. Testing approach

Two complementary layers:

### Declarative data tests — `python -m scripts.run_tests`

22 tests defined in `models/marts/schema.yml` using dbt-compatible syntax:
`not_null`, `unique`, `accepted_values`, `positive_value`, `non_negative`,
`unique_combination`, plus two custom reconciliation tests:

- **`net_amount_is_consistent`** — `net_amount` must always equal credit minus debit
- **`summary_totals_match_source`** — the mart's total `transaction_count` must
  equal the number of completed non-duplicate rows in staging

### Unit and integration tests — `pytest`

110 tests across four files:

| File | Coverage |
|---|---|
| `test_validation.py` | Every schema rule, including case-sensitivity, exclusive-minimum amount, whitespace-only names, impossible calendar dates, unassigned country codes, and multi-error reporting |
| `test_dedup.py` | Natural-key construction, amount normalisation, survivor selection, determinism, upsert idempotency |
| `test_api_client.py` | Pagination termination (short page and empty page), offset increments, stable ordering, retry on 408/429/5xx, immediate failure on 4xx, retry exhaustion, native date filter, auth headers |
| `test_summary.py` | Grain uniqueness, completed-only filtering, duplicate exclusion, net amount, distinct merchants, `top_category` ranking, account isolation, rebuild idempotency |

Both suites run offline — HTTP is mocked with `responses`, SQL runs against
in-memory DuckDB.

---

## 11. Outputs

| File | Contents |
|---|---|
| `outputs/quarantine_sample.csv` | All 3 quarantined records with reasons and payloads |
| `outputs/daily_account_summary_sample.csv` | 100 sample mart rows |
| `outputs/duplicate_pairs_sample.csv` | All 10 rows involved in duplicate groups |
| `outputs/raw_transactions_sample.csv` | 50 sample bronze rows |
| `outputs/ingestion_runs.csv` | Full audit log of both runs |
| `outputs/watermark_run1.json` | Watermark store after the full load |
| `outputs/watermark_run2.json` | Watermark store after the incremental run |
| `outputs/pipeline_summary.json` | Aggregate counts + quarantine reason breakdown |

---

## 12. Known limitations and next steps

| Limitation | What I would do with more time |
|---|---|
| **Amounts are not FX-converted.** `total_debit_amount` sums mixed currencies. The `currencies` column makes this visible but does not solve it. | Add an FX rate dimension and emit both native and reporting-currency measures. |
| **Full-refresh mart.** Rebuilding all 238 rows is trivial now but will not scale. | Incremental materialisation partitioned by `transaction_date`, merging only affected account-days. |
| **Model DAG order is explicit**, not parsed from `ref()`. | Real dbt once Python 3.13 is supported, or a small DAG parser. |
| **Single-threaded ingestion.** | Concurrent page fetching with a bounded worker pool; keyset pagination instead of offset. |
| **No orchestration.** | Airflow/Dagster DAG with retries, SLAs, and alerting. |
| **Local DuckDB file.** | Object storage (Parquet/Iceberg) plus a shared warehouse. |
| **No schema-drift detection.** | Fail loudly and quarantine the batch when unexpected fields appear. |
| **2024 Q1 date window is hardcoded.** | Config-driven, or replaced with a statistical outlier check. |
| **Secrets in `.env`.** Fine for an assessment. | A managed secret store (AWS Secrets Manager, Vault) with rotation. |

---

## 13. AI tool usage disclosure

An AI assistant was used for scaffolding boilerplate (DDL, argparse wiring,
test parametrisation) and as a reviewer to challenge edge cases.

**Everything was independently verified:**

- Every schema rule was cross-checked against `transactions_schema.json` and
  the brief, not accepted on assertion. The `country_code` "assigned, not just
  formatted" requirement and the `amount` exclusive-minimum were caught this way.
- All 110 unit tests were run and the one genuine failure
  (`test_no_date_filter_on_full_load` — an over-broad substring assertion I
  wrote, not a code bug) was diagnosed and fixed rather than deleted.
- Idempotency was proven mechanically with a checksum comparison, not assumed.
- The incremental run was executed against the live API and the row count
  verified to be unchanged.
- A real environment failure (a cloud-sync client holding a lock on the DuckDB
  file) was diagnosed and resolved by relocating the warehouse.

Design decisions — flag-don't-delete for duplicates, the 2-day lookback
rationale, the survivor rule, and the choice to document the dbt alternative
rather than downgrade Python — are mine, and I can defend each of them.

See [`docs/product_platform_note.md`](docs/product_platform_note.md) for the
product and platform judgment note.