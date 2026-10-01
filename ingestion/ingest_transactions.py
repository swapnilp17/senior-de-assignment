"""Task 1 entrypoint - full ingestion with data quality handling.

Usage:
    python -m ingestion.ingest_transactions
    python -m ingestion.ingest_transactions --reset
"""
from __future__ import annotations

import argparse
import json
import sys

from ingestion.config import configure_logging, load_settings
from ingestion.pipeline import run_pipeline, write_watermark_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Full ingestion of transactions (bronze layer).")
    parser.add_argument("--reset", action="store_true",
                        help="Drop the local warehouse first (clean full reload).")
    parser.add_argument("--snapshot", default="watermark_run1.json",
                        help="Filename for the watermark snapshot in outputs/.")
    args = parser.parse_args()

    configure_logging()
    settings = load_settings()

    if args.reset and settings.duckdb_path.exists():
        settings.duckdb_path.unlink()
        print(f"Removed existing warehouse at {settings.duckdb_path}")

    report = run_pipeline(mode="full", settings=settings)
    write_watermark_snapshot(settings, args.snapshot)

    print("\n=== RUN 1 (FULL LOAD) ===")
    print(json.dumps(report.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())