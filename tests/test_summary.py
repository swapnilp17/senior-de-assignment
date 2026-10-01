"""End-to-end tests for daily_account_summary on controlled, known data.

These assert the business rules from the brief: completed-only, duplicates
excluded, correct grain, correct top_category, and idempotent rebuilds.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from ingestion.config import PROJECT_ROOT
from ingestion.storage import Warehouse

STG_SQL = (PROJECT_ROOT / "models" / "staging" / "stg_transactions.sql").read_text(encoding="utf-8")
MART_SQL = (PROJECT_ROOT / "models" / "marts" / "daily_account_summary.sql").read_text(encoding="utf-8")

CHECKSUM_SQL = """
SELECT SUM(HASH(account_id, transaction_date, total_debit_amount,
                total_credit_amount, net_amount, transaction_count,
                distinct_merchants, top_category, currencies))
FROM daily_account_summary
"""


def row(txn_id, account, day, amount, ttype, status="completed",
        merchant="Amazon", category="e-commerce", currency="USD", nk=None):
    return {
        "transaction_id": txn_id,
        "account_id": account,
        "transaction_date": datetime(2024, 1, day, 10, 0, 0),
        "transaction_date_raw": f"2024-01-{day:02d}T10:00:00Z",
        "amount": amount,
        "currency": currency,
        "transaction_type": ttype,
        "merchant_name": merchant,
        "merchant_category": category,
        "status": status,
        "country_code": "US",
        "natural_key_hash": nk or txn_id,
        "source_system": "test",
        "source_payload": "{}",
        "ingestion_run_id": "r1",
        "ingestion_timestamp": datetime(2024, 1, 1),
    }


@pytest.fixture
def wh():
    with Warehouse(":memory:") as w:
        w.initialise()
        w.upsert_raw([
            row("TXN-0001", "ACC-1001", 15, 100.00, "debit",  merchant="Amazon",  category="e-commerce"),
            row("TXN-0002", "ACC-1001", 15,  40.00, "debit",  merchant="Tesco",   category="groceries"),
            row("TXN-0003", "ACC-1001", 15, 250.00, "credit", merchant="Payroll", category="payroll"),
            row("TXN-0004", "ACC-1001", 15, 999.00, "debit",  status="pending"),
            row("TXN-0005", "ACC-1001", 15, 500.00, "debit",  status="failed"),
            row("TXN-0006", "ACC-1001", 15, 300.00, "debit",  status="reversed"),
            row("TXN-0007", "ACC-1001", 16,  10.00, "debit",  currency="EUR"),
            row("TXN-0008", "ACC-2002", 15,  20.00, "debit"),
            # natural-key duplicate of TXN-0001 (same nk, different id)
            row("TXN-0009", "ACC-1001", 15, 100.00, "debit", nk="TXN-0001"),
        ])
        w.recompute_duplicates()
        w.con.execute(STG_SQL)
        w.con.execute(MART_SQL)
        yield w


def fetch(wh, account, day):
    return wh.con.execute(
        "SELECT * FROM daily_account_summary WHERE account_id=? AND transaction_date=?",
        [account, f"2024-01-{day:02d}"],
    ).df().iloc[0]


def test_grain_is_one_row_per_account_per_day(wh):
    assert wh.con.execute("SELECT COUNT(*) FROM daily_account_summary").fetchone()[0] == 3


def test_grain_has_no_duplicate_keys(wh):
    dupes = wh.con.execute(
        "SELECT COUNT(*) FROM (SELECT account_id, transaction_date "
        "FROM daily_account_summary GROUP BY 1,2 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dupes == 0


def test_only_completed_transactions_are_included(wh):
    r = fetch(wh, "ACC-1001", 15)
    assert r["transaction_count"] == 3                 # pending/failed/reversed excluded
    assert float(r["total_debit_amount"]) == 140.00    # 100 + 40 only


def test_duplicates_are_excluded(wh):
    """TXN-0009 shares a natural key with TXN-0001 and must not be counted."""
    assert float(fetch(wh, "ACC-1001", 15)["total_debit_amount"]) == 140.00


def test_credit_debit_and_net_amount(wh):
    r = fetch(wh, "ACC-1001", 15)
    assert float(r["total_credit_amount"]) == 250.00
    assert float(r["net_amount"]) == 110.00            # 250 - 140


def test_distinct_merchants(wh):
    assert fetch(wh, "ACC-1001", 15)["distinct_merchants"] == 3


def test_top_category_is_highest_total_spend(wh):
    """payroll=250 beats e-commerce=100 and groceries=40."""
    assert fetch(wh, "ACC-1001", 15)["top_category"] == "payroll"


def test_currencies_column(wh):
    assert fetch(wh, "ACC-1001", 15)["currencies"] == "USD"
    assert fetch(wh, "ACC-1001", 16)["currencies"] == "EUR"


def test_accounts_are_isolated(wh):
    assert float(fetch(wh, "ACC-2002", 15)["total_debit_amount"]) == 20.00


def test_updated_at_is_populated(wh):
    assert fetch(wh, "ACC-1001", 15)["updated_at"] is not None


def test_rebuild_is_idempotent(wh):
    before = wh.con.execute(CHECKSUM_SQL).fetchone()[0]
    wh.con.execute(MART_SQL)
    wh.con.execute(MART_SQL)
    assert wh.con.execute(CHECKSUM_SQL).fetchone()[0] == before