from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from app.core.config import Settings
from app.core.scheduler import build_scheduler
from app.main import validate_runtime_settings


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
        attention_engine_enabled=True,
        attention_smart_money_interval_seconds=60,
        attention_kol_interval_seconds=60,
        attention_market_signal_interval_seconds=120,
        attention_token_snapshot_interval_seconds=600,
        attention_top_holder_interval_seconds=900,
        attention_token_snapshot_batch_size=3,
        attention_top_holder_batch_size=1,
        attention_feed_window_minutes=15,
        attention_event_aggregation_minutes=5,
        attention_warning_cooldown_minutes=30,
        attention_critical_cooldown_minutes=60,
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


def test_attention_engine_registers_independent_jobs() -> None:
    service = object()
    app_settings = replace(
        settings(),
        attention_token_snapshot_due_seconds=600,
        attention_token_snapshot_dispatch_seconds=120,
        attention_top_holder_due_seconds=900,
        attention_top_holder_dispatch_seconds=60,
    )
    scheduler = build_scheduler(
        app_settings,
        None,
        None,
        price_guardian_service=None,
        attention_engine_service=service,
    )

    assert scheduler.get_job("scan_attention_smart_money") is not None
    assert scheduler.get_job("scan_attention_kol") is not None
    assert scheduler.get_job("scan_attention_market_signals") is not None
    assert scheduler.get_job("scan_attention_token_snapshots") is not None
    assert scheduler.get_job("scan_attention_top_holders") is not None
    assert scheduler.get_job("scan_attention_token_snapshots").trigger.interval.total_seconds() == 120
    assert scheduler.get_job("scan_attention_top_holders").trigger.interval.total_seconds() == 60


def test_attention_requires_price_guardian_enabled() -> None:
    app_settings = replace(
        settings(),
        attention_engine_enabled=True,
        price_guardian_enabled=False,
        price_guardian_alerts_enabled=False,
    )

    try:
        validate_runtime_settings(app_settings)
    except RuntimeError as exc:
        assert "ATTENTION_ENGINE_ENABLED requires PRICE_GUARDIAN_ENABLED" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_attention_with_price_guardian_alerts_disabled_is_valid() -> None:
    app_settings = replace(
        settings(),
        attention_engine_enabled=True,
        price_guardian_enabled=True,
        price_guardian_alerts_enabled=False,
    )

    validate_runtime_settings(app_settings)
