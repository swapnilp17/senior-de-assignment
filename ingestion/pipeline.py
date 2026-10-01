"""Orchestration: fetch -> validate -> quarantine -> persist -> dedup -> watermark.

A single run function serves both the full backfill and the incremental mode so
there is exactly one code path to reason about and test.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ingestion.api_client import TransactionsAPIClient
from ingestion.config import Settings, load_settings
from ingestion.storage import Warehouse
from ingestion.validation import (
    API_METADATA_FIELDS,
    natural_key_hash,
    parse_timestamp,
    payload_hash,
    validate_record,
)

LOGGER = logging.getLogger(__name__)
SOURCE_NAME = "supabase.transactions"


def _utcnow() -> datetime:
    """Naive UTC - DuckDB TIMESTAMP columns are timezone-free by design."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class RunReport:
    run_id: str
    run_mode: str
    source_name: str
    requested_from: str | None
    records_fetched: int
    records_valid: int
    records_quarantined: int
    records_duplicate_flagged: int
    previous_watermark: str | None
    new_watermark: str | None
    lookback_days: int
    duration_seconds: float
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_pipeline(
    mode: str = "full",
    settings: Settings | None = None,
    warehouse: Warehouse | None = None,
    client: TransactionsAPIClient | None = None,
) -> RunReport:
    """Execute one ingestion run.

    mode="full"        -> no date filter, fetch everything
    mode="incremental" -> fetch transaction_date >= (watermark - lookback)
    """
    settings = settings or load_settings()
    owns_warehouse = warehouse is None
    wh = warehouse or Warehouse(settings.duckdb_path)
    wh.initialise()

    started_at = _utcnow()
    run_id = f"{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    previous_watermark = wh.get_watermark(SOURCE_NAME)

    # ---- decide the request window -------------------------------------- #
    requested_from: datetime | None = None
    if mode == "incremental":
        if previous_watermark is None:
            LOGGER.warning("No watermark present - incremental run degrades to a full load.")
        else:
            # Lookback window: the API filter is on transaction_date (event time),
            # not an API-side ingestion timestamp. A record that is created late
            # but dated earlier would be invisible to a strict `>= watermark`
            # filter, so we deliberately re-read a small overlap. Re-reads are
            # free because every write is an upsert on the primary key.
            requested_from = previous_watermark - timedelta(days=settings.lookback_days)
            LOGGER.info(
                "Incremental: watermark=%s lookback=%sd -> requesting transaction_date >= %s",
                previous_watermark, settings.lookback_days, requested_from,
            )

    wh.start_run(run_id=run_id, source_name=SOURCE_NAME, run_mode=mode,
                 requested_from=requested_from, started_at=started_at,
                 previous_watermark=previous_watermark)

    api = client or TransactionsAPIClient(settings)
    valid_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    fetched = 0

    try:
        for payload in api.iter_transactions(since=requested_from):
            fetched += 1
            # Strip Supabase internal columns before contract validation.
            record = {k: v for k, v in payload.items() if k not in API_METADATA_FIELDS}
            errors = validate_record(record, strict_window=settings.strict_date_window)
            now = _utcnow()

            if errors:
                quarantine_rows.append({
                    "quarantine_key": payload_hash(payload),
                    "transaction_id": payload.get("transaction_id"),
                    "error_reason": "; ".join(errors),
                    "error_count": len(errors),
                    "raw_payload": json.dumps(payload, sort_keys=True, default=str),
                    "source_system": SOURCE_NAME,
                    "ingestion_run_id": run_id,
                    "ingestion_timestamp": now,
                })
                continue

            valid_rows.append({
                "transaction_id": record["transaction_id"],
                "account_id": record["account_id"],
                "transaction_date": parse_timestamp(record["transaction_date"]).replace(tzinfo=None),
                "transaction_date_raw": record["transaction_date"],
                "amount": record["amount"],
                "currency": record["currency"],
                "transaction_type": record["transaction_type"],
                "merchant_name": record["merchant_name"],
                "merchant_category": record["merchant_category"],
                "status": record["status"],
                "country_code": record["country_code"],
                "natural_key_hash": natural_key_hash(record),
                "source_system": SOURCE_NAME,
                "source_payload": json.dumps(payload, sort_keys=True, default=str),
                "ingestion_run_id": run_id,
                "ingestion_timestamp": now,
            })

        wh.upsert_raw(valid_rows)
        wh.upsert_quarantine(quarantine_rows)
        duplicates = wh.recompute_duplicates()

        # Advance the watermark only from records that actually landed in bronze,
        # and never move it backwards.
        new_watermark = wh.max_transaction_date() or previous_watermark
        if previous_watermark and new_watermark and new_watermark < previous_watermark:
            new_watermark = previous_watermark

        ended_at = _utcnow()
        wh.update_watermark(
            SOURCE_NAME,
            watermark_value=new_watermark,
            effective_request_from=requested_from,
            last_run_id=run_id,
            last_run_at=ended_at,
            last_run_status="success",
            records_ingested=len(valid_rows),
        )
        wh.finish_run(
            run_id, ended_at=ended_at, status="success", records_fetched=fetched,
            records_valid=len(valid_rows), records_quarantined=len(quarantine_rows),
            records_duplicate_flagged=duplicates, new_watermark=new_watermark,
        )

        report = RunReport(
            run_id=run_id, run_mode=mode, source_name=SOURCE_NAME,
            requested_from=requested_from.isoformat() if requested_from else None,
            records_fetched=fetched, records_valid=len(valid_rows),
            records_quarantined=len(quarantine_rows), records_duplicate_flagged=duplicates,
            previous_watermark=previous_watermark.isoformat() if previous_watermark else None,
            new_watermark=new_watermark.isoformat() if new_watermark else None,
            lookback_days=settings.lookback_days if mode == "incremental" else 0,
            duration_seconds=round((ended_at - started_at).total_seconds(), 3),
            status="success",
        )
        LOGGER.info("Run complete: %s", report.to_dict())
        return report

    except Exception as exc:
        # The watermark is NOT advanced on failure, so the next run safely
        # re-reads the same window instead of silently skipping records.
        LOGGER.exception("Run %s failed", run_id)
        wh.finish_run(run_id, ended_at=_utcnow(), status="failed",
                      records_fetched=fetched, records_valid=len(valid_rows),
                      records_quarantined=len(quarantine_rows),
                      error_message=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if owns_warehouse:
            wh.close()


def write_watermark_snapshot(settings: Settings, filename: str) -> dict[str, Any]:
    """Dump the watermark table + last run stats to outputs/ for the reviewer."""
    with Warehouse(settings.duckdb_path) as wh:
        wh.initialise()
        row = wh.con.execute(
            "SELECT * FROM ingestion_watermark WHERE source_name = ?", [SOURCE_NAME]
        ).fetchone()
        cols = [d[0] for d in wh.con.description]
        watermark = dict(zip(cols, row)) if row else {}

        run_row = wh.con.execute(
            "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        run_cols = [d[0] for d in wh.con.description]
        last_run = dict(zip(run_cols, run_row)) if run_row else {}

        counts = wh.con.execute(
            """SELECT (SELECT COUNT(*) FROM raw_transactions),
                      (SELECT COUNT(*) FROM raw_transactions WHERE is_duplicate),
                      (SELECT COUNT(*) FROM quarantine_transactions)"""
        ).fetchone()

    snapshot = {
        "watermark_store": watermark,
        "last_run": last_run,
        "table_counts": {
            "raw_transactions": counts[0],
            "raw_transactions_flagged_duplicate": counts[1],
            "quarantine_transactions": counts[2],
        },
    }
    path = settings.outputs_dir / filename
    path.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
    LOGGER.info("Watermark snapshot written to %s", path)
    return snapshot