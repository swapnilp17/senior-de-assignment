"""Execute SQL models in dependency order (the `dbt run` equivalent).

Models are plain .sql files. Order is explicit rather than parsed from ref()
calls - a deliberate simplification given the two-model DAG. See README.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from ingestion.config import PROJECT_ROOT, configure_logging, load_settings
from ingestion.storage import Warehouse

LOGGER = logging.getLogger(__name__)

# Explicit DAG: staging before marts.
MODEL_ORDER = [
    "models/staging/stg_transactions.sql",
    "models/marts/daily_account_summary.sql",
]


def main() -> int:
    configure_logging()
    settings = load_settings()

    with Warehouse(settings.duckdb_path) as wh:
        wh.initialise()
        for rel_path in MODEL_ORDER:
            path = PROJECT_ROOT / rel_path
            sql = path.read_text(encoding="utf-8")
            LOGGER.info("Running model %s", rel_path)
            wh.con.execute(sql)

        rows = wh.con.execute("SELECT COUNT(*) FROM daily_account_summary").fetchone()[0]
        accounts = wh.con.execute(
            "SELECT COUNT(DISTINCT account_id) FROM daily_account_summary"
        ).fetchone()[0]

    print(f"\nModels built OK. daily_account_summary: {rows} rows across {accounts} accounts.")
    return 0


if __name__ == "__main__":
    sys.exit(main())