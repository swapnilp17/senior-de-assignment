"""Tests for natural-key duplicate detection and survivor selection."""
from __future__ import annotations

from datetime import datetime

from ingestion.storage import Warehouse
from ingestion.validation import natural_key, natural_key_hash

BASE = {
    "transaction_id": "TXN-0001",
    "account_id": "ACC-1001",
    "transaction_date": "2024-01-15T08:23:11Z",
    "amount": 142.50,
    "currency": "USD",
    "transaction_type": "debit",
    "merchant_name": "Amazon",
    "merchant_category": "e-commerce",
    "status": "completed",
    "country_code": "US",
}


def test_natural_key_excludes_transaction_id():
    assert "transaction_id" not in natural_key(BASE)


def test_same_business_facts_different_id_collide():
    """The exact duplicate definition from the brief: identical in all
    fields except transaction_id."""
    a = dict(BASE, transaction_id="TXN-0001")
    b = dict(BASE, transaction_id="TXN-9999")
    assert natural_key_hash(a) == natural_key_hash(b)


def test_amount_is_normalised_to_two_decimals():
    """100.5 and '100.50' represent the same money."""
    assert natural_key_hash(dict(BASE, amount=100.5)) == \
           natural_key_hash(dict(BASE, amount="100.50"))


def test_any_other_field_difference_breaks_the_key():
    for field, other in [
        ("account_id", "ACC-9999"),
        ("transaction_date", "2024-02-20T08:23:11Z"),
        ("amount", 999.99),
        ("currency", "EUR"),
        ("transaction_type", "credit"),
        ("merchant_name", "Other Shop"),
        ("merchant_category", "groceries"),
        ("status", "pending"),
        ("country_code", "GB"),
    ]:
        assert natural_key_hash(BASE) != natural_key_hash(dict(BASE, **{field: other})), field


# ---------------------------------------------------------------------- #
def _row(txn_id: str, day: int, nk: str) -> dict:
    return {
        "transaction_id": txn_id,
        "account_id": "ACC-1001",
        "transaction_date": datetime(2024, 1, day, 8, 0, 0),
        "transaction_date_raw": f"2024-01-{day:02d}T08:00:00Z",
        "amount": 100.00,
        "currency": "USD",
        "transaction_type": "debit",
        "merchant_name": "Amazon",
        "merchant_category": "e-commerce",
        "status": "completed",
        "country_code": "US",
        "natural_key_hash": nk,
        "source_system": "test",
        "source_payload": "{}",
        "ingestion_run_id": "run-1",
        "ingestion_timestamp": datetime(2024, 1, 1),
    }


def test_survivor_is_earliest_date_then_lowest_id():
    with Warehouse(":memory:") as wh:
        wh.initialise()
        wh.upsert_raw([
            _row("TXN-0003", 15, "nk1"),
            _row("TXN-0001", 15, "nk1"),
            _row("TXN-0002", 16, "nk1"),
            _row("TXN-0009", 15, "nk2"),
        ])
        assert wh.recompute_duplicates() == 2

        rows = dict(wh.con.execute(
            "SELECT transaction_id, is_duplicate FROM raw_transactions"
        ).fetchall())
        assert rows["TXN-0001"] is False   # earliest date + lowest id -> survivor
        assert rows["TXN-0003"] is True
        assert rows["TXN-0002"] is True
        assert rows["TXN-0009"] is False   # different natural key


def test_duplicate_points_at_its_survivor():
    with Warehouse(":memory:") as wh:
        wh.initialise()
        wh.upsert_raw([_row("TXN-0001", 15, "nk1"), _row("TXN-0002", 15, "nk1")])
        wh.recompute_duplicates()
        pointer = wh.con.execute(
            "SELECT duplicate_of_transaction_id FROM raw_transactions "
            "WHERE transaction_id = 'TXN-0002'"
        ).fetchone()[0]
        assert pointer == "TXN-0001"


def test_recompute_is_deterministic_and_repeatable():
    with Warehouse(":memory:") as wh:
        wh.initialise()
        wh.upsert_raw([_row("TXN-0001", 15, "nk1"), _row("TXN-0002", 15, "nk1")])
        assert wh.recompute_duplicates() == 1
        assert wh.recompute_duplicates() == 1


def test_upsert_raw_is_idempotent():
    """Re-inserting the same rows must not grow the table."""
    with Warehouse(":memory:") as wh:
        wh.initialise()
        rows = [_row("TXN-0001", 15, "nk1"), _row("TXN-0002", 16, "nk2")]
        wh.upsert_raw(rows)
        wh.upsert_raw(rows)
        wh.upsert_raw(rows)
        assert wh.con.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0] == 2