"""Typed settings loaded from environment variables / .env (never hardcoded)."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


class ConfigError(RuntimeError):
    """Raised when mandatory configuration is missing."""


def _require(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value or value == "changeme":
        raise ConfigError(
            f"Missing required env var '{name}'. Copy .env.example to .env and fill it in."
        )
    return value


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _bool(name: str, default: bool) -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str
    auth_token: str
    duckdb_path: Path
    outputs_dir: Path
    page_size: int = 200
    max_retries: int = 5
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 30.0
    request_timeout_seconds: float = 30.0
    lookback_days: int = 2
    strict_date_window: bool = True


def load_settings() -> Settings:
    duckdb_path = PROJECT_ROOT / os.getenv("DUCKDB_PATH", "warehouse/transactions.duckdb")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_dir = PROJECT_ROOT / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    api_key = _require("ASSESSMENT_API_KEY")
    return Settings(
        api_base_url=_require("ASSESSMENT_API_BASE_URL").rstrip("/"),
        api_key=api_key,
        auth_token=(os.getenv("ASSESSMENT_AUTH_TOKEN") or "").strip() or api_key,
        duckdb_path=duckdb_path,
        outputs_dir=outputs_dir,
        page_size=_int("API_PAGE_SIZE", 200),
        max_retries=_int("API_MAX_RETRIES", 5),
        request_timeout_seconds=float(os.getenv("API_TIMEOUT_SECONDS", "30")),
        lookback_days=_int("WATERMARK_LOOKBACK_DAYS", 2),
        strict_date_window=_bool("STRICT_DATE_WINDOW", True),
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=(os.getenv("LOG_LEVEL", "INFO")).upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )