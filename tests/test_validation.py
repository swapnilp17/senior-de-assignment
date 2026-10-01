"""Unit tests for the schema contract validator.

Covers every rule in transactions_schema.json, with particular attention to
the rules the dataset deliberately violates: exclusive-minimum amount,
case-sensitive enums, whitespace-only merchant names, impossible calendar
dates, and country codes that match the format but are not assigned.
"""
from __future__ import annotations

import pytest

from ingestion.validation import validate_record

VALID = {
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


def make(**overrides):
    return {**VALID, **overrides}


def test_valid_record_passes():
    assert validate_record(VALID) == []


# --------------------------------------------------------------- amount --- #
@pytest.mark.parametrize("amount", [0, 0.0, -1, -127.5, "0.00"])
def test_amount_must_be_strictly_positive(amount):
    """Schema uses exclusiveMinimum: 0, so zero is invalid, not just negatives."""
    errors = validate_record(make(amount=amount))
    assert any("not_strictly_positive" in e for e in errors)


def test_amount_rejects_more_than_two_decimals():
    errors = validate_record(make(amount="10.123"))
    assert any("more_than_two_decimal_places" in e for e in errors)


def test_amount_rejects_non_numeric():
    assert any("not_numeric" in e for e in validate_record(make(amount="abc")))


# ---------------------------------------------------------------- enums --- #
@pytest.mark.parametrize("value", ["usd", "US", "EURO", "XYZ", ""])
def test_currency_enum_is_case_sensitive(value):
    assert any("invalid_currency_code" in e for e in validate_record(make(currency=value)))


@pytest.mark.parametrize("value", ["DEBIT", "Debit", "Credit", "withdrawal"])
def test_transaction_type_enum_is_case_sensitive(value):
    errors = validate_record(make(transaction_type=value))
    assert any("invalid_transaction_type" in e for e in errors)


@pytest.mark.parametrize("value", ["Completed", "COMPLETED", "done", "ok"])
def test_status_enum_is_case_sensitive(value):
    assert any("invalid_status" in e for e in validate_record(make(status=value)))


@pytest.mark.parametrize("value", ["ecommerce", "fast_food", "Finance", "groceries "])
def test_merchant_category_enum(value):
    errors = validate_record(make(merchant_category=value))
    assert any("invalid_merchant_category" in e for e in errors)


# -------------------------------------------------------- merchant_name --- #
@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_merchant_name_rejects_blank_or_whitespace(value):
    """A string of spaces passes a length check but must still fail."""
    errors = validate_record(make(merchant_name=value))
    assert any("empty_or_whitespace_only" in e for e in errors)


# --------------------------------------------------------- country_code --- #
@pytest.mark.parametrize("value", ["UK", "EN", "ZZ", "XX"])
def test_country_code_must_be_officially_assigned(value):
    """These match ^[A-Z]{2}$ but are not assigned ISO 3166-1 codes."""
    errors = validate_record(make(country_code=value))
    assert any("not_an_assigned_iso3166_code" in e for e in errors)


@pytest.mark.parametrize("value", ["us", "USA", "U", "1S"])
def test_country_code_format(value):
    assert validate_record(make(country_code=value)) != []


@pytest.mark.parametrize("value", ["US", "GB", "DE", "JP", "AU", "FR", "NL", "ES", "CA"])
def test_valid_country_codes_pass(value):
    assert validate_record(make(country_code=value)) == []


# ----------------------------------------------------- transaction_date --- #
@pytest.mark.parametrize("value", [
    "2024-04-15 09:30:00",       # space separator, no Z
    "2024-01-15T08:23:11",       # missing Z
    "15/01/2024",                # wrong format entirely
    "2024-01-15T08:23:11+00:00", # offset instead of Z
    "2024-13-01T00:00:00Z",      # month 13
])
def test_transaction_date_must_be_strict_iso_utc(value):
    errors = validate_record(make(transaction_date=value))
    assert any("invalid_iso8601_utc_format" in e for e in errors)


def test_transaction_date_rejects_impossible_calendar_date():
    """November has 30 days - passes the regex but is not a real date."""
    errors = validate_record(make(transaction_date="2024-11-31T14:22:00Z"))
    assert any("not_a_real_calendar_date" in e for e in errors)


def test_strict_window_can_be_disabled():
    out_of_window = make(transaction_date="2029-01-15T08:23:11Z")
    assert validate_record(out_of_window, strict_window=True) != []
    assert validate_record(out_of_window, strict_window=False) == []


# ------------------------------------------------------------ id format --- #
@pytest.mark.parametrize("value", ["TXN0001", "txn-0001", "0001", ""])
def test_transaction_id_format(value):
    assert any("transaction_id" in e for e in validate_record(make(transaction_id=value)))


@pytest.mark.parametrize("value", ["ACC1001", "acc-1001", "ACC-101", "ACC-10011"])
def test_account_id_format(value):
    assert any("account_id" in e for e in validate_record(make(account_id=value)))


# ------------------------------------------------------- required / null --- #
@pytest.mark.parametrize("field", list(VALID))
def test_missing_required_field_is_caught(field):
    record = {k: v for k, v in VALID.items() if k != field}
    assert any(f"{field}: missing_required_field" in e for e in validate_record(record))


def test_null_value_is_caught():
    assert any("missing_required_field" in e for e in validate_record(make(amount=None)))


# ------------------------------------------------------------ behaviour --- #
def test_multiple_violations_are_all_reported():
    """Validation must not short-circuit on the first error."""
    errors = validate_record(make(amount=0, currency="usd",
                                  status="Completed", country_code="UK"))
    assert len(errors) >= 4


def test_api_metadata_fields_are_ignored():
    """Supabase internal columns must not be flagged as unexpected."""
    record = {**VALID, "id": 446, "created_at": "2024-01-01T00:00:00Z"}
    assert validate_record(record) == []


def test_genuinely_unexpected_field_is_flagged():
    assert any("unexpected_field" in e for e in validate_record({**VALID, "bogus": 1}))