"""Prove the transformation layer is idempotent.

Computes a content checksum of daily_account_summary, rebuilds the models,
then recomputes. Identical checksums prove that a rerun does not change
results unless the input data changes.

`updated_at` is deliberately excluded from the checksum - it is an audit
field whose entire purpose is to change on every rebuild.
"""
from __future__ import annotations

import subprocess
import sys

from ingestion.config import load_settings
from ingestion.storage import Warehouse

CHECKSUM_SQL = """
SELECT
    COUNT(*) AS row_count,
    SUM(HASH(
        account_id,
        transaction_date,
        total_debit_amount,
        total_credit_amount,
        net_amount,
        transaction_count,
        distinct_merchants,
        COALESCE(top_category, ''),
        currencies
    )) AS content_checksum
FROM daily_account_summary
"""


def checksum() -> tuple[int, int]:
    settings = load_settings()
    with Warehouse(settings.duckdb_path) as wh:
        return wh.con.execute(CHECKSUM_SQL).fetchone()


def main() -> int:
    before = checksum()
    print(f"Before rebuild : rows={before[0]}  checksum={before[1]}")

    subprocess.run([sys.executable, "-m", "scripts.run_models"], check=True)

    after = checksum()
    print(f"After rebuild  : rows={after[0]}  checksum={after[1]}")

    if before == after:
        print("\nIDEMPOTENCY PASS - rebuild produced identical content.\n")
        return 0
    print("\nIDEMPOTENCY FAIL - content changed across rebuilds.\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())