"""REST client for the assessment transactions API (Supabase / PostgREST)."""
from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from typing import Any, Iterator

import requests

from ingestion.config import Settings

LOGGER = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
MAX_PAGES_SAFETY = 10_000


class ApiError(RuntimeError):
    """Non-retryable API failure, or retries exhausted."""


def to_api_timestamp(value: datetime) -> str:
    """Render a datetime in the strict ISO-8601 UTC form the API expects."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


class TransactionsAPIClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self._s = settings
        self._session = session or requests.Session()
        self._session.headers.update({
            "apikey": settings.api_key,
            "Authorization": f"Bearer {settings.auth_token}",
            "Accept": "application/json",
        })

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), self._s.backoff_max_seconds)
            except ValueError:
                pass
        delay = min(self._s.backoff_base_seconds * (2 ** (attempt - 1)), self._s.backoff_max_seconds)
        return delay + random.uniform(0, delay * 0.25)

    def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{self._s.api_base_url}/{path.lstrip('/')}"
        last_error = "unknown"

        for attempt in range(1, self._s.max_retries + 1):
            retry_after = None
            try:
                resp = self._session.get(url, params=params, timeout=self._s.request_timeout_seconds)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.warning("Transport error %s/%s: %s", attempt, self._s.max_retries, last_error)
            else:
                if resp.status_code == 200:
                    return resp.json()
                last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                if resp.status_code not in RETRYABLE_STATUS:
                    raise ApiError(f"Non-retryable response from {url} -> {last_error}")
                retry_after = resp.headers.get("Retry-After")
                LOGGER.warning("Retryable %s/%s: %s", attempt, self._s.max_retries, last_error)

            if attempt < self._s.max_retries:
                delay = self._backoff(attempt, retry_after)
                LOGGER.info("Backing off %.2fs", delay)
                time.sleep(delay)

        raise ApiError(f"Exhausted {self._s.max_retries} attempts for {url}. Last error -> {last_error}")

    def iter_transactions(self, since: datetime | None = None) -> Iterator[dict[str, Any]]:
        """Yield every transaction page by page.

        Stops when a page returns fewer rows than the requested limit.
        The total record count is never hardcoded.
        """
        limit, offset, pages = self._s.page_size, 0, 0

        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                # Stable ordering is mandatory: offset pagination without a
                # deterministic sort can silently skip or repeat rows.
                "order": "transaction_date.asc,transaction_id.asc",
                "select": "*",
            }
            if since is not None:
                params["transaction_date"] = f"gte.{to_api_timestamp(since)}"

            page = self._get("transactions", params)
            pages += 1
            LOGGER.info("Page %s (offset=%s) -> %s records", pages, offset, len(page))

            yield from page

            if len(page) < limit:
                LOGGER.info("Pagination complete after %s page(s)", pages)
                return

            offset += limit
            if pages >= MAX_PAGES_SAFETY:
                raise ApiError("Safety page limit reached - suspected pagination loop.")