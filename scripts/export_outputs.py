"""Export sample outputs to outputs/ so a reviewer can inspect results
without rerunning the pipeline (explicitly requested in the brief).
"""
from __future__ import annotations

import json
import sys

from ingestion.config import configure_logging, load_settings
from ingestion.storage import Warehouse

EXPORTS = {
    "quarantine_sample.csv": """
        SELECT transaction_id, error_count, error_reason,
               ingestion_run_id, ingestion_timestamp, raw_payload
        FROM quarantine_transactions
        ORDER BY error_count DESC, transaction_id
    """,
    "daily_account_summary_sample.csv": """
        SELECT * FROM daily_account_summary
        ORDER BY account_id, transaction_date
        LIMIT 100
    """,
    "duplicate_pairs_sample.csv": """
        SELECT transaction_id, is_duplicate, duplicate_of_transaction_id,
               account_id, transaction_date, amount, currency,
               merchant_name, merchant_category, status, country_code
        FROM raw_transactions
        WHERE natural_key_hash IN (
            SELECT natural_key_hash FROM raw_transactions WHERE is_duplicate
        )
        ORDER BY natural_key_hash, transaction_id
    """,
    "raw_transactions_sample.csv": """
        SELECT transaction_id, account_id, transaction_date, amount, currency,
               transaction_type, merchant_name, merchant_category, status,
               country_code, is_duplicate, ingestion_run_id, ingestion_timestamp
        FROM raw_transactions
        ORDER BY transaction_date
        LIMIT 50
    """,
    "ingestion_runs.csv": "SELECT * FROM ingestion_runs ORDER BY started_at",
}


def main() -> int:
    configure_logging()
    settings = load_settings()
    out = settings.outputs_dir
    out.mkdir(parents=True, exist_ok=True)

    with Warehouse(settings.duckdb_path) as wh:
        for filename, sql in EXPORTS.items():
            target = (out / filename).as_posix()
            wh.con.execute(f"COPY ({sql}) TO '{target}' (HEADER, DELIMITER ',')")
            n = wh.con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
            print(f"  wrote {filename:40s} ({n} rows)")

        # Quarantine reason breakdown - useful observability evidence.
        reasons = wh.con.execute("""
            SELECT error_reason, COUNT(*) AS record_count
            FROM quarantine_transactions
            GROUP BY error_reason ORDER BY record_count DESC
        """).fetchall()

        stats = wh.con.execute("""
            SELECT (SELECT COUNT(*) FROM raw_transactions),
                   (SELECT COUNT(*) FROM raw_transactions WHERE is_duplicate),
                   (SELECT COUNT(*) FROM quarantine_transactions),
                   (SELECT COUNT(*) FROM daily_account_summary),
                   (SELECT COUNT(DISTINCT account_id) FROM daily_account_summary)
        """).fetchone()

    summary = {
        "raw_transactions": stats[0],
        "raw_flagged_duplicate": stats[1],
        "quarantined": stats[2],
        "daily_account_summary_rows": stats[3],
        "distinct_accounts": stats[4],
        "quarantine_reasons": [
            {"error_reason": r[0], "record_count": r[1]} for r in reasons
        ],
    }
    (out / "pipeline_summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    print(f"  wrote pipeline_summary.json")
    print(f"\n{json.dumps(summary, indent=2, default=str)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())