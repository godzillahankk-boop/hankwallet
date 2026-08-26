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
    legacy_wallet_scan_enabled: bool
    manual_scan_cooldown_seconds: int
    dust_threshold: Decimal
    dust_clear_confirmation_scans: int
    default_chain: str
    chain_api_base_url: str | None
    chain_api_key: str | None
    chain_rpc_url: str | None
    chain_token_search_symbols: tuple[str, ...]
    chain_request_timeout_seconds: int
    token_transfer_lookback_limit: int
    log_level: str
    api_host: str
    api_port: int
    gmgn_enabled: bool
    gmgn_api_base_url: str
    gmgn_api_key: str | None
    gmgn_private_key_path: str | None
    gmgn_request_timeout_seconds: int
    price_guardian_enabled: bool
    price_scan_interval_seconds: int
    price_monitor_min_usd_value: Decimal
    price_excluded_symbols: tuple[str, ...]
    price_history_retention_hours: int
    price_alert_5m_percent: Decimal
    price_alert_15m_percent: Decimal
    price_alert_60m_percent: Decimal
    price_alert_escalation_step_percent: Decimal
    price_alert_reset_ratio: Decimal
    price_holdings_max_pages: int
    price_wallet_concurrency: int


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _get_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name)
    return Decimal(raw if raw else default)


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    load_dotenv(os.path.expanduser("~/.config/gmgn/.env"), override=False)
    load_dotenv()
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./data/wallet_agent.db"),
        wallet_scan_interval_seconds=_get_int("WALLET_SCAN_INTERVAL_SECONDS", 60),
        legacy_wallet_scan_enabled=_get_bool("LEGACY_WALLET_SCAN_ENABLED", False),
        manual_scan_cooldown_seconds=_get_int("MANUAL_SCAN_COOLDOWN_SECONDS", 30),
        dust_threshold=_get_decimal("DUST_THRESHOLD", "0.000001"),
        dust_clear_confirmation_scans=_get_int("DUST_CLEAR_CONFIRMATION_SCANS", 2),
        default_chain=os.getenv("DEFAULT_CHAIN", "ethereum").lower(),
        chain_api_base_url=os.getenv("CHAIN_API_BASE_URL") or None,
        chain_api_key=os.getenv("CHAIN_API_KEY") or None,
        chain_rpc_url=os.getenv("CHAIN_RPC_URL") or None,
        chain_token_search_symbols=tuple(
            symbol.strip()
            for symbol in os.getenv("CHAIN_TOKEN_SEARCH_SYMBOLS", "").split(",")
            if symbol.strip()
        ),
        chain_request_timeout_seconds=_get_int("CHAIN_REQUEST_TIMEOUT_SECONDS", 20),
        token_transfer_lookback_limit=_get_int("TOKEN_TRANSFER_LOOKBACK_LIMIT", 100),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        api_host=os.getenv("API_HOST", "127.0.0.1"),
        api_port=_get_int("API_PORT", 8000),
        gmgn_enabled=_get_bool("GMGN_ENABLED", False),
        gmgn_api_base_url=os.getenv("GMGN_API_BASE_URL", "https://openapi.gmgn.ai"),
        gmgn_api_key=os.getenv("GMGN_API_KEY") or None,
        gmgn_private_key_path=os.getenv("GMGN_PRIVATE_KEY_PATH") or None,
        gmgn_request_timeout_seconds=_get_int("GMGN_REQUEST_TIMEOUT_SECONDS", 20),
        price_guardian_enabled=_get_bool("PRICE_GUARDIAN_ENABLED", True),
        price_scan_interval_seconds=_get_int("PRICE_SCAN_INTERVAL_SECONDS", 60),
        price_monitor_min_usd_value=_get_decimal("PRICE_MONITOR_MIN_USD_VALUE", "5"),
        price_excluded_symbols=tuple(
            symbol.strip().upper()
            for symbol in os.getenv("PRICE_EXCLUDED_SYMBOLS", "USDG,USDC,USDT,ETH,WETH").split(",")
            if symbol.strip()
        ),
        price_history_retention_hours=_get_int("PRICE_HISTORY_RETENTION_HOURS", 24),
        price_alert_5m_percent=_get_decimal("PRICE_ALERT_5M_PERCENT", "10"),
        price_alert_15m_percent=_get_decimal("PRICE_ALERT_15M_PERCENT", "20"),
        price_alert_60m_percent=_get_decimal("PRICE_ALERT_60M_PERCENT", "30"),
        price_alert_escalation_step_percent=_get_decimal(
            "PRICE_ALERT_ESCALATION_STEP_PERCENT", "10"
        ),
        price_alert_reset_ratio=_get_decimal("PRICE_ALERT_RESET_RATIO", "0.5"),
        price_holdings_max_pages=_get_int("PRICE_HOLDINGS_MAX_PAGES", 10),
        price_wallet_concurrency=_get_int("PRICE_WALLET_CONCURRENCY", 3),
    )
