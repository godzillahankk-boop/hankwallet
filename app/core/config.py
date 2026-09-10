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
    attention_engine_enabled: bool
    attention_smart_money_interval_seconds: int
    attention_kol_interval_seconds: int
    attention_market_signal_interval_seconds: int
    attention_token_snapshot_interval_seconds: int
    attention_top_holder_interval_seconds: int
    attention_token_snapshot_batch_size: int
    attention_top_holder_batch_size: int
    attention_feed_window_minutes: int
    attention_event_aggregation_minutes: int
    attention_warning_cooldown_minutes: int
    attention_critical_cooldown_minutes: int
    attention_token_snapshot_due_seconds: int = 600
    attention_token_snapshot_dispatch_seconds: int = 120
    attention_top_holder_due_seconds: int = 900
    attention_top_holder_dispatch_seconds: int = 60
    price_guardian_alerts_enabled: bool = False
    twitterapi_io_api_key: str | None = None
    social_x_enabled: bool = False
    social_x_provider: str = "twitterapi_io"
    social_x_rule_refresh_seconds: int = 300
    social_x_filter_interval_seconds: int = 300
    social_x_rule_min_update_interval_seconds: int = 1800
    social_x_rule_max_value_chars: int = 240
    social_x_rule_shard_state_path: str = "data/twitter_social_rule_shards.json"
    social_x_warmup_grace_seconds: int = 120
    social_x_kol_config_path: str = "config/social_kols.json"
    social_x_websocket_url: str = "wss://ws.twitterapi.io/twitter/tweet/websocket"
    social_kol_auto_verify_enabled: bool = False
    deepseek_api_key: str | None = None
    deepseek_base_url: str = "https://api.deepseek.com"
    social_kol_verify_model: str = "deepseek-chat"
    social_kol_verify_max_concurrency: int = 3
    social_x_discovery_enabled: bool = False
    social_x_discovery_due_seconds: int = 900
    social_x_discovery_max_due_seconds: int = 14400
    social_x_discovery_bootstrap_lookback_seconds: int = 900
    social_x_discovery_dispatch_seconds: int = 60
    social_x_discovery_batch_size: int = 3
    social_x_discovery_max_pages: int = 1
    social_x_discovery_overlap_seconds: int = 120
    social_x_discovery_cashtag_enabled: bool = False
    social_kol_verify_batch_size: int = 20
    social_shadow_report_seconds: int = 300
    social_memory_enabled: bool = False
    social_memory_triage_model: str = "deepseek-chat"
    report_timezone: str = "Asia/Shanghai"
    daily_report_price_tolerance_minutes: int = 90


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
        attention_engine_enabled=_get_bool("ATTENTION_ENGINE_ENABLED", True),
        attention_smart_money_interval_seconds=_get_int("ATTENTION_SMART_MONEY_INTERVAL_SECONDS", 60),
        attention_kol_interval_seconds=_get_int("ATTENTION_KOL_INTERVAL_SECONDS", 60),
        attention_market_signal_interval_seconds=_get_int("ATTENTION_MARKET_SIGNAL_INTERVAL_SECONDS", 120),
        attention_token_snapshot_interval_seconds=_get_int("ATTENTION_TOKEN_SNAPSHOT_INTERVAL_SECONDS", 600),
        attention_top_holder_interval_seconds=_get_int("ATTENTION_TOP_HOLDER_INTERVAL_SECONDS", 900),
        attention_token_snapshot_batch_size=_get_int("ATTENTION_TOKEN_SNAPSHOT_BATCH_SIZE", 3),
        attention_top_holder_batch_size=_get_int("ATTENTION_TOP_HOLDER_BATCH_SIZE", 1),
        attention_feed_window_minutes=_get_int("ATTENTION_FEED_WINDOW_MINUTES", 15),
        attention_event_aggregation_minutes=_get_int("ATTENTION_EVENT_AGGREGATION_MINUTES", 5),
        attention_warning_cooldown_minutes=_get_int("ATTENTION_WARNING_COOLDOWN_MINUTES", 30),
        attention_critical_cooldown_minutes=_get_int("ATTENTION_CRITICAL_COOLDOWN_MINUTES", 60),
        attention_token_snapshot_due_seconds=_get_int(
            "ATTENTION_TOKEN_SNAPSHOT_DUE_SECONDS",
            _get_int("ATTENTION_TOKEN_SNAPSHOT_INTERVAL_SECONDS", 600),
        ),
        attention_token_snapshot_dispatch_seconds=_get_int("ATTENTION_TOKEN_SNAPSHOT_DISPATCH_SECONDS", 120),
        attention_top_holder_due_seconds=_get_int(
            "ATTENTION_TOP_HOLDER_DUE_SECONDS",
            _get_int("ATTENTION_TOP_HOLDER_INTERVAL_SECONDS", 900),
        ),
        attention_top_holder_dispatch_seconds=_get_int("ATTENTION_TOP_HOLDER_DISPATCH_SECONDS", 60),
        price_guardian_alerts_enabled=_get_bool("PRICE_GUARDIAN_ALERTS_ENABLED", False),
        twitterapi_io_api_key=os.getenv("TWITTERAPI_IO_API_KEY") or None,
        social_x_enabled=_get_bool("SOCIAL_X_ENABLED", False),
        social_x_provider=os.getenv("SOCIAL_X_PROVIDER", "twitterapi_io"),
        social_x_rule_refresh_seconds=_get_int("SOCIAL_X_RULE_REFRESH_SECONDS", 300),
        social_x_filter_interval_seconds=max(60, _get_int("SOCIAL_X_FILTER_INTERVAL_SECONDS", 300)),
        social_x_rule_min_update_interval_seconds=_get_int("SOCIAL_X_RULE_MIN_UPDATE_INTERVAL_SECONDS", 1800),
        social_x_rule_max_value_chars=_get_int("SOCIAL_X_RULE_MAX_VALUE_CHARS", 240),
        social_x_rule_shard_state_path=os.getenv(
            "SOCIAL_X_RULE_SHARD_STATE_PATH",
            "data/twitter_social_rule_shards.json",
        ),
        social_x_warmup_grace_seconds=_get_int("SOCIAL_X_WARMUP_GRACE_SECONDS", 120),
        social_x_kol_config_path=os.getenv("SOCIAL_X_KOL_CONFIG_PATH", "config/social_kols.json"),
        social_x_websocket_url=os.getenv(
            "SOCIAL_X_WEBSOCKET_URL",
            "wss://ws.twitterapi.io/twitter/tweet/websocket",
        ),
        social_kol_auto_verify_enabled=_get_bool("SOCIAL_KOL_AUTO_VERIFY_ENABLED", False),
        deepseek_api_key=os.getenv("DEEPSEEK_API_KEY") or None,
        deepseek_base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        social_kol_verify_model=os.getenv("SOCIAL_KOL_VERIFY_MODEL", "deepseek-chat"),
        social_kol_verify_max_concurrency=_get_int("SOCIAL_KOL_VERIFY_MAX_CONCURRENCY", 3),
        social_x_discovery_enabled=_get_bool("SOCIAL_X_DISCOVERY_ENABLED", False),
        social_x_discovery_due_seconds=_get_int("SOCIAL_X_DISCOVERY_DUE_SECONDS", 900),
        social_x_discovery_max_due_seconds=_get_int("SOCIAL_X_DISCOVERY_MAX_DUE_SECONDS", 14400),
        social_x_discovery_bootstrap_lookback_seconds=_get_int(
            "SOCIAL_X_DISCOVERY_BOOTSTRAP_LOOKBACK_SECONDS",
            900,
        ),
        social_x_discovery_dispatch_seconds=_get_int("SOCIAL_X_DISCOVERY_DISPATCH_SECONDS", 60),
        social_x_discovery_batch_size=_get_int("SOCIAL_X_DISCOVERY_BATCH_SIZE", 3),
        social_x_discovery_max_pages=_get_int("SOCIAL_X_DISCOVERY_MAX_PAGES", 1),
        social_x_discovery_overlap_seconds=_get_int("SOCIAL_X_DISCOVERY_OVERLAP_SECONDS", 120),
        social_x_discovery_cashtag_enabled=_get_bool("SOCIAL_X_DISCOVERY_CASHTAG_ENABLED", False),
        social_kol_verify_batch_size=_get_int("SOCIAL_KOL_VERIFY_BATCH_SIZE", 20),
        social_shadow_report_seconds=_get_int("SOCIAL_SHADOW_REPORT_SECONDS", 300),
        social_memory_enabled=_get_bool("SOCIAL_MEMORY_ENABLED", False),
        social_memory_triage_model=os.getenv("SOCIAL_MEMORY_TRIAGE_MODEL", "deepseek-chat"),
        report_timezone=os.getenv("REPORT_TIMEZONE", "Asia/Shanghai"),
        daily_report_price_tolerance_minutes=_get_int("DAILY_REPORT_PRICE_TOLERANCE_MINUTES", 90),
    )
