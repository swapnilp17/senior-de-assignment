"""Tests for pagination and retry behaviour using mocked HTTP responses."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import responses

from ingestion.api_client import ApiError, TransactionsAPIClient, to_api_timestamp
from ingestion.config import Settings

BASE = "https://example.test/rest/v1"


def settings(page_size: int = 2, max_retries: int = 3) -> Settings:
    return Settings(
        api_base_url=BASE,
        api_key="k",
        auth_token="t",
        duckdb_path=Path(":memory:"),
        outputs_dir=Path("."),
        page_size=page_size,
        max_retries=max_retries,
        backoff_base_seconds=0.0,   # no real sleeping in tests
        backoff_max_seconds=0.0,
        request_timeout_seconds=5.0,
    )


def rec(i: int) -> dict:
    return {"transaction_id": f"TXN-{i:04d}"}


@responses.activate
def test_pagination_stops_on_short_page():
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(1), rec(2)], status=200)
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(3)], status=200)
    rows = list(TransactionsAPIClient(settings()).iter_transactions())
    assert len(rows) == 3
    assert len(responses.calls) == 2


@responses.activate
def test_pagination_stops_on_empty_page():
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(1), rec(2)], status=200)
    responses.add(responses.GET, f"{BASE}/transactions", json=[], status=200)
    assert len(list(TransactionsAPIClient(settings()).iter_transactions())) == 2


@responses.activate
def test_record_count_is_not_hardcoded():
    """Client must handle an arbitrary number of pages."""
    for _ in range(5):
        responses.add(responses.GET, f"{BASE}/transactions", json=[rec(1), rec(2)], status=200)
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(99)], status=200)
    assert len(list(TransactionsAPIClient(settings()).iter_transactions())) == 11


@responses.activate
def test_offset_increments_across_pages():
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(1), rec(2)], status=200)
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(3)], status=200)
    list(TransactionsAPIClient(settings()).iter_transactions())
    assert "offset=0" in responses.calls[0].request.url
    assert "offset=2" in responses.calls[1].request.url


@responses.activate
def test_stable_ordering_is_requested():
    """Offset pagination without a deterministic sort can skip or repeat rows."""
    responses.add(responses.GET, f"{BASE}/transactions", json=[], status=200)
    list(TransactionsAPIClient(settings()).iter_transactions())
    assert "order=transaction_date.asc" in responses.calls[0].request.url


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
@responses.activate
def test_retries_then_succeeds(status):
    responses.add(responses.GET, f"{BASE}/transactions", json={"m": "err"}, status=status)
    responses.add(responses.GET, f"{BASE}/transactions", json=[rec(1)], status=200)
    assert len(list(TransactionsAPIClient(settings()).iter_transactions())) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_non_retryable_4xx_raises_immediately():
    responses.add(responses.GET, f"{BASE}/transactions", json={"m": "unauthorised"}, status=401)
    with pytest.raises(ApiError, match="Non-retryable"):
        list(TransactionsAPIClient(settings()).iter_transactions())
    assert len(responses.calls) == 1


@responses.activate
def test_retries_are_exhausted_and_raise():
    for _ in range(3):
        responses.add(responses.GET, f"{BASE}/transactions", json={"m": "err"}, status=503)
    with pytest.raises(ApiError, match="Exhausted"):
        list(TransactionsAPIClient(settings(max_retries=3)).iter_transactions())
    assert len(responses.calls) == 3


@responses.activate
def test_watermark_filter_is_sent_as_native_api_param():
    responses.add(responses.GET, f"{BASE}/transactions", json=[], status=200)
    since = datetime(2024, 2, 1, tzinfo=timezone.utc)
    list(TransactionsAPIClient(settings()).iter_transactions(since=since))
    url = responses.calls[0].request.url
    assert "transaction_date=gte.2024-02-01T00%3A00%3A00Z" in url


@responses.activate
def test_no_date_filter_on_full_load():
    """A full load must not send a transaction_date filter.

    Note: the URL still contains 'transaction_date' as part of the `order`
    parameter, so we assert specifically on the filter predicate 'gte.'.
    """
    responses.add(responses.GET, f"{BASE}/transactions", json=[], status=200)
    list(TransactionsAPIClient(settings()).iter_transactions(since=None))
    url = responses.calls[0].request.url
    assert "transaction_date=gte." not in url
    assert "order=transaction_date.asc" in url   # ordering is still applied


@responses.activate
def test_auth_headers_are_sent():
    responses.add(responses.GET, f"{BASE}/transactions", json=[], status=200)
    list(TransactionsAPIClient(settings()).iter_transactions())
    headers = responses.calls[0].request.headers
    assert headers["apikey"] == "k"
    assert headers["Authorization"] == "Bearer t"


def test_timestamp_rendering():
    ts = datetime(2024, 3, 30, 21, 1, 36, tzinfo=timezone.utc)
    assert to_api_timestamp(ts) == "2024-03-30T21:01:36Z"