# Product and Platform Judgment Note

**Project:** Payment transactions ingestion and `daily_account_summary` data product
**Author:** Swapnil Pujari
**Scope:** What I would challenge, monitor, and generalise before this runs in production.

---

## 1. Assumptions I would challenge before productionising

| # | Assumption baked into this build | Why it worries me | How I would resolve it |
|---|---|---|---|
| 1 | **`transaction_date` is a safe incremental cursor.** | It is *event* time, not *ingestion* time. A record created today but dated last week is invisible to a `>= watermark` filter. My 2-day lookback mitigates this; it does not eliminate it. | Ask the source team for a monotonic `updated_at` or a CDC feed. Until then, measure the real lag distribution and set the lookback at p99 rather than guessing. |
| 2 | **Amounts in different currencies can be summed.** | `total_debit_amount` currently adds USD, EUR, GBP, CHF, AUD and CAD together. That number is **not meaningful** as money. I exposed a `currencies` column to make it visible, but a PM could still misread the total. | Introduce an FX rate dimension and emit both native-currency and reporting-currency measures. Until that exists, I would rename the field or add a warning in the column description. This is my single biggest concern. |
| 3 | **A `reversed` transaction should be excluded.** | The brief says completed-only, so I complied. But a reversal usually *negates* an earlier completed transaction. Excluding both the original and the reversal would be correct; excluding only the reversal overstates net position. | Confirm reversal semantics with the payments domain owner. This is a correctness question, not a preference. |
| 4 | **Duplicates are genuine defects.** | Two identical transactions a second apart might be a double-charge bug — or a customer legitimately buying two coffees. The natural key cannot distinguish them. | Validate the dedup rule against a known-good reconciliation from finance before trusting it. I flag rather than delete precisely because I am not confident enough to destroy data. |
| 5 | **The 2024 Q1 date window is a real constraint.** | I quarantine timestamps outside it, which is right for this fixed dataset but would quarantine everything the day real data arrives. | Make it config-driven, or replace it with a statistical outlier check (e.g. more than N days from now). |
| 6 | **The schema is stable.** | `additionalProperties: false` means a new upstream field silently becomes an "unexpected field" error. | Treat schema drift as a first-class alert, not a per-record failure — new field detected should page the owner, not quarantine the batch. |
| 7 | **Full-refresh of the mart is acceptable.** | At 238 rows, trivially true. At 238 million, it is not. | Incremental materialisation partitioned by date before volume grows. |

---

## 2. Production monitoring and alerting

The `ingestion_runs` table already captures everything needed to drive this —
it exists specifically so operations are measurable, not guessed at.

### Pipeline health

| Signal | Alert condition | Severity | Why |
|---|---|---|---|
| Run status | Any `status = 'failed'` | **P1 page** | Pipeline is down |
| Freshness | No successful run in 26h (daily SLA + 2h grace) | **P1 page** | Silent stall is worse than a loud failure |
| Duration | > 3× the 7-day median | P2 ticket | Source degradation or a pagination loop |
| Watermark | Unchanged for 2 consecutive runs *while* the source has new data | P2 ticket | Catches a stuck cursor, the nastiest silent failure |

### Data quality

| Signal | Alert condition | Severity | Why |
|---|---|---|---|
| Quarantine rate | > 2% of a batch, or 3× the 7-day baseline | **P1** | Upstream contract likely broke |
| New error reason | An `error_reason` never seen before | P2 | Early warning of schema drift |
| Duplicate rate | Step change vs baseline | P2 | Possible upstream retry storm |
| Volume anomaly | Daily count outside ±3σ of the trailing 30 days | P2 | Partial extract or silent truncation |
| Reconciliation | `summary_totals_match_source` fails | **P1 — block publish** | The mart no longer agrees with its source |
| Zero-row load | A scheduled run ingests 0 records on a business day | P2 | Usually an auth or filter bug, not genuinely empty |

### API behaviour

Retry count per run, 429 frequency, p95 latency, and auth failures. A rising
retry count is a leading indicator of an outage — it tells you something is
wrong *before* the pipeline actually fails.

### Principle

**Alert on symptoms users feel, not on every anomaly.** A failed run at 02:00
that self-heals on retry at 02:05 is a dashboard entry, not a page. Stale data
at 09:00 when analysts open their dashboards is a page. Alert fatigue is a real
operational risk — every alert I add must have a documented runbook action, or
it gets deleted.

---

## 3. Making this reusable for the next 10 APIs

Roughly 80% of this codebase is already source-agnostic. The reusable core:

- **Config loader** — environment-driven, validates on startup, fails fast
- **HTTP client** — pagination, retry with exponential backoff and jitter,
  timeout handling, auth injection
- **Validation engine** — takes a schema, returns structured violations
- **Storage layer** — idempotent upsert, quarantine, watermark, run audit
- **Test runner** — YAML-driven declarative data tests

### Target shape: configuration, not code

A new source should be a **declarative spec plus a schema file**, not a new
Python module:

```yaml
source: payments.transactions
connection:
  base_url_env: PAYMENTS_API_BASE_URL
  auth: {type: bearer, token_env: PAYMENTS_API_TOKEN}
pagination: {strategy: offset, page_size: 200, order_by: [transaction_date, transaction_id]}
incremental:
  cursor_field: transaction_date
  filter_template: "{field}=gte.{value}"
  lookback: {days: 2}
contract: contracts/payments_transactions.yml
natural_key: {exclude: [transaction_id]}
quarantine: {strategy: route, retain_payload: true}
```

### Changes required to get there

1. **Extract validation rules into declarative contract files.** Today the
   rules live in Python. They should be YAML/JSON Schema so a domain expert
   can change an enum without touching code or waiting for a deploy.
2. **Make pagination pluggable.** Offset works here; others need cursor,
   link-header, or keyset. One interface, several strategies.
3. **Make auth pluggable.** Bearer, API key, OAuth2 client-credentials, mTLS.
4. **Publish as an internal package** (`dataplatform-ingest`) with semantic
   versioning, so bug fixes propagate to all 10 sources at once instead of
   being copy-pasted 10 times.
5. **Scaffold generator** — `ingest new-source payments` produces the spec,
   contract stub, model skeleton and test skeleton.
6. **Shared observability** — every source emits the same run metrics to the
   same tables, so one dashboard covers all 10. This is the real multiplier:
   the second source should take a day, not a week.

### The honest caveat

I would resist generalising after the *second* source. Two data points are not
a pattern. The right time is around the third or fourth, when the genuinely
common parts are obvious and the premature abstractions have revealed
themselves. Building the framework first is how platform teams end up
maintaining a DSL that nobody wants to use.

---

## 4. What makes `daily_account_summary` trustworthy

Trust is not a property of correct SQL. It is a property of **users being able
to verify claims themselves**.

| Trust dimension | How it is established |
|---|---|
| **Correctness** | 22 declarative data tests plus 110 unit tests, all passing. A reconciliation test asserts the mart's `transaction_count` equals completed non-duplicate rows in staging — the mart cannot silently drift from its source. |
| **Completeness** | Quarantined records are counted and published, not hidden. A user can see that 3 of 352 records were rejected and read exactly why. "We dropped nothing silently" is a verifiable claim here. |
| **Freshness** | `updated_at` on every row, plus a published SLA (daily by 06:00 UTC) and a freshness alert when it is missed. |
| **Consistency** | Idempotency proven mechanically by checksum, not asserted. The same inputs always produce the same outputs. |
| **Transparency** | Known limitations are documented *in the README*, including the mixed-currency problem. A caveat users discover themselves destroys trust; a caveat you tell them first builds it. |
| **Ownership** | A named team, a support channel, and a documented escalation path. "Who do I ask?" must have an answer within 10 seconds. |
| **Stability** | Semantic versioning on the contract. Breaking changes get a deprecation window, not a surprise. |

**The practical test:** when an analyst says "this number looks wrong," how
long does it take to prove it right or wrong? Today, with run audit, quarantine
reasons, and reconciliation tests, that is minutes. That response time *is* the
trust.

---

## 5. Exposing lineage, ownership, documentation and quality status

| Need | Mechanism |
|---|---|
| **Lineage** | Column-level lineage from the API through bronze, staging and mart, published to a catalog (DataHub / OpenMetadata / Unity Catalog). Every row already carries `ingestion_run_id`, so any value traces back to the exact API call that produced it. |
| **Ownership** | Owner team, Slack channel and on-call rota as model metadata, surfaced in the catalog and in the BI tool — not buried in a README nobody opens. |
| **Documentation** | Column descriptions live in `schema.yml` next to the tests, so they are reviewed in the same pull request as the logic. Documentation that lives elsewhere rots. |
| **Quality status** | A health badge on the dataset in the catalog and BI tool: last test run, pass rate, freshness, current quarantine rate. Analysts should see "⚠️ quality checks failed 2h ago" *before* they build a slide from the data. |
| **Change communication** | Contract versioning plus automated notification to subscribers on breaking change. Consumers should never learn about a schema change from a broken dashboard. |
| **Self-service debugging** | Quarantine samples and run history exposed read-only, so an analyst can answer "why is yesterday low?" without filing a ticket. Every question they can answer alone is a ticket the platform team does not handle. |

---

## 6. Trade-offs made because of the time limit

| Trade-off | Rationale | Cost |
|---|---|---|
| **DuckDB instead of Databricks** | Zero setup, real SQL, runs anywhere, reviewer can execute it in two minutes. The brief permits documented alternatives. | No distributed-compute demonstration. |
| **SQL models + dbt-compatible layout instead of dbt** | `dbt-core` does not yet support Python 3.13. Keeping dbt's structure and semantics preserved the value without burning time on an interpreter downgrade. | Not literally dbt. Migration is a profile change plus `ref()` wrapping. |
| **Flag duplicates rather than resolve them** | Flagging is reversible; deletion is not. Without domain confirmation, destroying data is the wrong default. | Consumers must know to filter — handled in staging. |
| **Full-refresh mart** | 238 rows. Incremental complexity would be unjustified. | Will not scale; documented as a next step. |
| **No orchestrator** | Two CLI entrypoints are clearer for a reviewer than an Airflow DAG they cannot run. | No scheduling, backfill or dependency management demonstrated. |
| **No FX conversion** | Needs a rate source and a business decision on reporting currency. | The biggest correctness gap. Documented prominently rather than quietly shipped. |
| **Explicit model DAG order** | Two models. A `ref()` parser would be over-engineering. | Will not scale past a handful of models. |
| **Single-threaded ingestion** | 352 records. Concurrency would add risk, not value. | Would need a worker pool at real volume. |

**The meta trade-off:** I prioritised *provable* correctness — real runs, real
numbers, passing tests, a mechanical idempotency check — over breadth of
features. A reviewer can verify every claim in this submission in under five
minutes. I would rather defend a smaller, honest, fully-working solution than
a larger one with unverified parts.

---

## 7. AI tool usage and verification

I used an AI assistant for boilerplate scaffolding (DDL, argparse wiring, test
parametrisation) and as an adversarial reviewer to stress-test edge cases.

**How I verified rather than trusted:**

- Cross-checked every validation rule against `transactions_schema.json` and
  the brief. The `country_code` "officially assigned, not merely well-formatted"
  requirement and the `amount` exclusive-minimum were caught in that review.
- Ran all 110 tests. The single failure
  (`test_no_date_filter_on_full_load`) was an over-broad substring assertion in
  the *test*, not a bug in the code — the URL legitimately contains
  `transaction_date` inside the `order` parameter. I diagnosed it and tightened
  the assertion to check for `transaction_date=gte.` instead of deleting the test.
- Proved idempotency with a checksum comparison across rebuilds rather than
  assuming `CREATE OR REPLACE` was sufficient.
- Executed the incremental run against the live API and confirmed the row count
  was unchanged at 349.
- Diagnosed a real environment failure (a cloud-sync client holding an OS-level
  lock on the DuckDB file) and resolved it by relocating the warehouse.

**My position on accountability:** AI accelerated typing; it did not make
decisions. The architectural choices — flag-don't-delete, the lookback
rationale, the deterministic survivor rule, surfacing the mixed-currency
problem rather than hiding it — are mine, and I can defend each one under
questioning. Where I am uncertain (reversal semantics, whether duplicates are
genuine defects), I have said so explicitly rather than presenting a guess as a
conclusion.