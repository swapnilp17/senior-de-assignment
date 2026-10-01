"""DuckDB persistence: raw (bronze), quarantine, watermark and run audit."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import duckdb

LOGGER = logging.getLogger(__name__)

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS raw_transactions (
        transaction_id              VARCHAR       NOT NULL,
        account_id                  VARCHAR       NOT NULL,
        transaction_date            TIMESTAMP     NOT NULL,
        transaction_date_raw        VARCHAR       NOT NULL,
        amount                      DECIMAL(18,2) NOT NULL,
        currency                    VARCHAR       NOT NULL,
        transaction_type            VARCHAR       NOT NULL,
        merchant_name               VARCHAR       NOT NULL,
        merchant_category           VARCHAR       NOT NULL,
        status                      VARCHAR       NOT NULL,
        country_code                VARCHAR       NOT NULL,
        natural_key_hash            VARCHAR       NOT NULL,
        is_duplicate                BOOLEAN       NOT NULL DEFAULT FALSE,
        duplicate_of_transaction_id VARCHAR,
        source_system               VARCHAR       NOT NULL,
        source_payload              VARCHAR       NOT NULL,
        ingestion_run_id            VARCHAR       NOT NULL,
        ingestion_timestamp         TIMESTAMP     NOT NULL,
        PRIMARY KEY (transaction_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS quarantine_transactions (
        quarantine_key      VARCHAR   NOT NULL,
        transaction_id      VARCHAR,
        error_reason        VARCHAR   NOT NULL,
        error_count         INTEGER   NOT NULL,
        raw_payload         VARCHAR   NOT NULL,
        source_system       VARCHAR   NOT NULL,
        ingestion_run_id    VARCHAR   NOT NULL,
        ingestion_timestamp TIMESTAMP NOT NULL,
        PRIMARY KEY (quarantine_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion_watermark (
        source_name            VARCHAR NOT NULL,
        watermark_value        TIMESTAMP,
        effective_request_from TIMESTAMP,
        last_run_id            VARCHAR,
        last_run_at            TIMESTAMP,
        last_run_status        VARCHAR,
        records_ingested       BIGINT,
        PRIMARY KEY (source_name)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id                    VARCHAR NOT NULL,
        source_name               VARCHAR NOT NULL,
        run_mode                  VARCHAR NOT NULL,
        requested_from            TIMESTAMP,
        started_at                TIMESTAMP NOT NULL,
        ended_at                  TIMESTAMP,
        status                    VARCHAR NOT NULL,
        records_fetched           BIGINT DEFAULT 0,
        records_valid             BIGINT DEFAULT 0,
        records_quarantined       BIGINT DEFAULT 0,
        records_duplicate_flagged BIGINT DEFAULT 0,
        previous_watermark        TIMESTAMP,
        new_watermark             TIMESTAMP,
        error_message             VARCHAR,
        PRIMARY KEY (run_id)
    );
    """,
]

RAW_COLUMNS = (
    "transaction_id", "account_id", "transaction_date", "transaction_date_raw", "amount",
    "currency", "transaction_type", "merchant_name", "merchant_category", "status",
    "country_code", "natural_key_hash", "source_system", "source_payload",
    "ingestion_run_id", "ingestion_timestamp",
)
QUARANTINE_COLUMNS = (
    "quarantine_key", "transaction_id", "error_reason", "error_count", "raw_payload",
    "source_system", "ingestion_run_id", "ingestion_timestamp",
)


class Warehouse:
    """Thin wrapper over DuckDB with idempotent write helpers."""

    def __init__(self, database: Path | str = ":memory:") -> None:
        self.database = str(database)
        self.con = duckdb.connect(self.database)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "Warehouse":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def initialise(self) -> None:
        for stmt in DDL_STATEMENTS:
            self.con.execute(stmt)
        LOGGER.info("Warehouse initialised at %s", self.database)

    def upsert_raw(self, rows: Sequence[dict[str, Any]]) -> int:
        """Delete-then-insert on the primary key => safe to re-run forever."""
        if not rows:
            return 0
        ids = [r["transaction_id"] for r in rows]
        ph = ",".join("?" for _ in ids)
        self.con.execute(f"DELETE FROM raw_transactions WHERE transaction_id IN ({ph})", ids)
        self.con.executemany(
            f"INSERT INTO raw_transactions ({', '.join(RAW_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in RAW_COLUMNS)})",
            [[r[c] for c in RAW_COLUMNS] for r in rows],
        )
        return len(rows)

    def upsert_quarantine(self, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        keys = [r["quarantine_key"] for r in rows]
        ph = ",".join("?" for _ in keys)
        self.con.execute(f"DELETE FROM quarantine_transactions WHERE quarantine_key IN ({ph})", keys)
        self.con.executemany(
            f"INSERT INTO quarantine_transactions ({', '.join(QUARANTINE_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in QUARANTINE_COLUMNS)})",
            [[r[c] for c in QUARANTINE_COLUMNS] for r in rows],
        )
        return len(rows)

    def recompute_duplicates(self) -> int:
        """Recompute flags over the WHOLE table so the outcome is identical
        regardless of batch boundaries or arrival order."""
        self.con.execute(
            """
            UPDATE raw_transactions AS t
            SET is_duplicate = (r.rn > 1),
                duplicate_of_transaction_id = CASE WHEN r.rn > 1 THEN r.survivor_id END
            FROM (
                SELECT transaction_id,
                       ROW_NUMBER() OVER (PARTITION BY natural_key_hash
                                          ORDER BY transaction_date, transaction_id) AS rn,
                       FIRST_VALUE(transaction_id) OVER (PARTITION BY natural_key_hash
                                          ORDER BY transaction_date, transaction_id) AS survivor_id
                FROM raw_transactions
            ) AS r
            WHERE t.transaction_id = r.transaction_id
            """
        )
        return self.con.execute(
            "SELECT COUNT(*) FROM raw_transactions WHERE is_duplicate"
        ).fetchone()[0]

    def max_transaction_date(self) -> datetime | None:
        return self.con.execute("SELECT MAX(transaction_date) FROM raw_transactions").fetchone()[0]

    def get_watermark(self, source_name: str) -> datetime | None:
        row = self.con.execute(
            "SELECT watermark_value FROM ingestion_watermark WHERE source_name = ?", [source_name]
        ).fetchone()
        return row[0] if row else None

    def update_watermark(self, source_name: str, **kw: Any) -> None:
        self.con.execute("DELETE FROM ingestion_watermark WHERE source_name = ?", [source_name])
        self.con.execute(
            """INSERT INTO ingestion_watermark
               (source_name, watermark_value, effective_request_from, last_run_id,
                last_run_at, last_run_status, records_ingested)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [source_name, kw.get("watermark_value"), kw.get("effective_request_from"),
             kw.get("last_run_id"), kw.get("last_run_at"), kw.get("last_run_status"),
             kw.get("records_ingested", 0)],
        )

    def start_run(self, **kw: Any) -> None:
        self.con.execute(
            """INSERT INTO ingestion_runs
               (run_id, source_name, run_mode, requested_from, started_at, status, previous_watermark)
               VALUES (?, ?, ?, ?, ?, 'running', ?)""",
            [kw["run_id"], kw["source_name"], kw["run_mode"], kw.get("requested_from"),
             kw["started_at"], kw.get("previous_watermark")],
        )

    def finish_run(self, run_id: str, **kw: Any) -> None:
        self.con.execute(
            """UPDATE ingestion_runs
               SET ended_at=?, status=?, records_fetched=?, records_valid=?,
                   records_quarantined=?, records_duplicate_flagged=?, new_watermark=?, error_message=?
               WHERE run_id=?""",
            [kw.get("ended_at"), kw.get("status"), kw.get("records_fetched", 0),
             kw.get("records_valid", 0), kw.get("records_quarantined", 0),
             kw.get("records_duplicate_flagged", 0), kw.get("new_watermark"),
             kw.get("error_message"), run_id],
        )