from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    database_url: str
    wallet_scan_interval_seconds: int
    manual_scan_cooldown_seconds: int
    dust_threshold: Decimal
    dust_clear_confirmation_scans: int
    default_chain: str
    chain_api_base_url: str | None
    chain_api_key: str | None
    chain_request_timeout_seconds: int
    token_transfer_lookback_limit: int
    log_level: str
    api_host: str
    api_port: int


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _get_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name)
    return Decimal(raw if raw else default)


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./data/wallet_agent.db"),
        wallet_scan_interval_seconds=_get_int("WALLET_SCAN_INTERVAL_SECONDS", 60),
        manual_scan_cooldown_seconds=_get_int("MANUAL_SCAN_COOLDOWN_SECONDS", 30),
        dust_threshold=_get_decimal("DUST_THRESHOLD", "0.000001"),
        dust_clear_confirmation_scans=_get_int("DUST_CLEAR_CONFIRMATION_SCANS", 2),
        default_chain=os.getenv("DEFAULT_CHAIN", "ethereum").lower(),
        chain_api_base_url=os.getenv("CHAIN_API_BASE_URL") or None,
        chain_api_key=os.getenv("CHAIN_API_KEY") or None,
        chain_request_timeout_seconds=_get_int("CHAIN_REQUEST_TIMEOUT_SECONDS", 20),
        token_transfer_lookback_limit=_get_int("TOKEN_TRANSFER_LOOKBACK_LIMIT", 100),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        api_host=os.getenv("API_HOST", "127.0.0.1"),
        api_port=_get_int("API_PORT", 8000),
    )

