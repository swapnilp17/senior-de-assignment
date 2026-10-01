"""Task 3 entrypoint - incremental ingestion driven by the stored watermark.

Usage:
    python -m ingestion.incremental_ingest
    python -m ingestion.incremental_ingest --snapshot watermark_run2.json
"""
from __future__ import annotations

import argparse
import json
import sys

from ingestion.config import configure_logging, load_settings
from ingestion.pipeline import run_pipeline, write_watermark_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental ingestion using the watermark.")
    parser.add_argument("--snapshot", default="watermark_run2.json",
                        help="Filename for the watermark snapshot in outputs/.")
    args = parser.parse_args()

    configure_logging()
    settings = load_settings()

    report = run_pipeline(mode="incremental", settings=settings)
    write_watermark_snapshot(settings, args.snapshot)

    print("\n=== RUN 2 (INCREMENTAL) ===")
    print(json.dumps(report.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())