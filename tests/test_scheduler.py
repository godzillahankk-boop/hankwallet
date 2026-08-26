from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from app.core.config import Settings
from app.core.scheduler import build_scheduler


def settings() -> Settings:
    return Settings(
        telegram_bot_token="test-token",
        database_url="sqlite:///:memory:",
        wallet_scan_interval_seconds=60,
        legacy_wallet_scan_enabled=False,
        manual_scan_cooldown_seconds=30,
        dust_threshold=Decimal("0.000001"),
        dust_clear_confirmation_scans=2,
        default_chain="robinhood",
        chain_api_base_url=None,
        chain_api_key=None,
        chain_rpc_url=None,
        chain_token_search_symbols=(),
        chain_request_timeout_seconds=20,
        token_transfer_lookback_limit=100,
        log_level="INFO",
        api_host="127.0.0.1",
        api_port=8000,
        gmgn_enabled=True,
        gmgn_api_base_url="https://openapi.gmgn.ai",
        gmgn_api_key="test-key",
        gmgn_private_key_path=None,
        gmgn_request_timeout_seconds=20,
        price_guardian_enabled=True,
        price_scan_interval_seconds=60,
        price_monitor_min_usd_value=Decimal("5"),
        price_excluded_symbols=("USDG", "USDC", "USDT", "ETH", "WETH"),
        price_history_retention_hours=24,
        price_alert_5m_percent=Decimal("10"),
        price_alert_15m_percent=Decimal("20"),
        price_alert_60m_percent=Decimal("30"),
        price_alert_escalation_step_percent=Decimal("10"),
        price_alert_reset_ratio=Decimal("0.5"),
        price_holdings_max_pages=10,
        price_wallet_concurrency=3,
    )


def test_legacy_wallet_scan_disabled_does_not_register_old_job() -> None:
    scheduler = build_scheduler(settings(), None, None, price_guardian_service=None)

    assert scheduler.get_job("scan_active_wallets") is None


def test_legacy_wallet_scan_enabled_registers_old_job() -> None:
    scheduler = build_scheduler(
        replace(settings(), legacy_wallet_scan_enabled=True),
        None,
        None,
        price_guardian_service=None,
    )

    assert scheduler.get_job("scan_active_wallets") is not None
