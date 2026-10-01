"""Contract validation + natural-key derivation for transaction records.

Mirrors transactions_schema.json. Each violation produces a human-readable
reason string that is persisted to the quarantine layer.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pycountry

REQUIRED_FIELDS: tuple[str, ...] = (
    "transaction_id", "account_id", "transaction_date", "amount", "currency",
    "transaction_type", "merchant_name", "merchant_category", "status", "country_code",
)

API_METADATA_FIELDS = {"id", "created_at", "inserted_at", "updated_at"}

TRANSACTION_ID_RE = re.compile(r"^TXN-[A-Za-z0-9]+$")
ACCOUNT_ID_RE = re.compile(r"^ACC-\d{4}$")
TIMESTAMP_RE = re.compile(
    r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$"
)

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"}
ALLOWED_TRANSACTION_TYPES = {"debit", "credit"}
ALLOWED_STATUSES = {"completed", "pending", "failed", "reversed"}
ALLOWED_CATEGORIES = {
    "e-commerce", "travel", "food_and_beverage", "groceries", "electronics", "retail",
    "entertainment", "health", "transportation", "home_and_garden", "payroll", "transfer",
}
ASSIGNED_COUNTRY_CODES = {c.alpha_2 for c in pycountry.countries}

DATA_WINDOW_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATA_WINDOW_END = datetime(2024, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

NATURAL_KEY_FIELDS: tuple[str, ...] = tuple(f for f in REQUIRED_FIELDS if f != "transaction_id")


def parse_timestamp(value: str) -> datetime:
    """Parse a strict ISO-8601 UTC timestamp. Raises ValueError if malformed."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _as_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def validate_record(record: dict[str, Any], *, strict_window: bool = True) -> list[str]:
    """Return a list of violations. Empty list means the record is valid."""
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"{field}: missing_required_field")

    for field in sorted(set(record) - set(REQUIRED_FIELDS) - API_METADATA_FIELDS):
        errors.append(f"{field}: unexpected_field")

    def present(f: str) -> bool:
        return record.get(f) is not None

    if present("transaction_id"):
        v = record["transaction_id"]
        if not isinstance(v, str) or not TRANSACTION_ID_RE.match(v):
            errors.append(f"transaction_id: invalid_format (expected TXN-NNNN, got {v!r})")

    if present("account_id"):
        v = record["account_id"]
        if not isinstance(v, str) or not ACCOUNT_ID_RE.match(v):
            errors.append(f"account_id: invalid_format (expected ACC-NNNN, got {v!r})")

    if present("transaction_date"):
        v = record["transaction_date"]
        if not isinstance(v, str) or not TIMESTAMP_RE.match(v):
            errors.append(f"transaction_date: invalid_iso8601_utc_format (got {v!r})")
        else:
            try:
                parsed = parse_timestamp(v)
            except ValueError:
                errors.append(f"transaction_date: not_a_real_calendar_date (got {v!r})")
            else:
                if strict_window and not (DATA_WINDOW_START <= parsed <= DATA_WINDOW_END):
                    errors.append(f"transaction_date: outside_expected_window_2024Q1 (got {v!r})")

    if present("amount"):
        v = record["amount"]
        if isinstance(v, bool) or not isinstance(v, (int, float, str, Decimal)):
            errors.append(f"amount: not_numeric (got {v!r})")
        else:
            try:
                amount = _as_decimal(v)
            except (InvalidOperation, ValueError):
                errors.append(f"amount: not_numeric (got {v!r})")
            else:
                if amount <= 0:
                    errors.append(f"amount: not_strictly_positive (got {v!r})")
                elif amount.as_tuple().exponent < -2:
                    errors.append(f"amount: more_than_two_decimal_places (got {v!r})")

    for field, allowed, code in (
        ("currency", ALLOWED_CURRENCIES, "invalid_currency_code"),
        ("transaction_type", ALLOWED_TRANSACTION_TYPES, "invalid_transaction_type"),
        ("merchant_category", ALLOWED_CATEGORIES, "invalid_merchant_category"),
        ("status", ALLOWED_STATUSES, "invalid_status"),
    ):
        if present(field):
            v = record[field]
            if not isinstance(v, str) or v not in allowed:
                errors.append(f"{field}: {code} (got {v!r})")

    if present("merchant_name"):
        v = record["merchant_name"]
        if not isinstance(v, str) or not v.strip():
            errors.append(f"merchant_name: empty_or_whitespace_only (got {v!r})")

    if present("country_code"):
        v = record["country_code"]
        if not isinstance(v, str) or len(v) != 2 or not v.isascii() or not v.isupper():
            errors.append(f"country_code: invalid_format (got {v!r})")
        elif v not in ASSIGNED_COUNTRY_CODES:
            errors.append(f"country_code: not_an_assigned_iso3166_code (got {v!r})")

    return errors


def natural_key(record: dict[str, Any]) -> str:
    """Business key: every contract field EXCEPT transaction_id."""
    parts = []
    for field in NATURAL_KEY_FIELDS:
        value = record.get(field)
        if field == "amount" and value is not None:
            try:
                value = str(_as_decimal(value).quantize(Decimal("0.01")))
            except (InvalidOperation, ValueError):
                value = str(value)
        parts.append(f"{field}={value!s}")
    return "|".join(parts)


def natural_key_hash(record: dict[str, Any]) -> str:
    return hashlib.sha256(natural_key(record).encode("utf-8")).hexdigest()


def payload_hash(payload: dict[str, Any]) -> str:
    """Stable identity for a raw payload, used as the quarantine primary key."""
    canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()