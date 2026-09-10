from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import logging

import pytest

from app.core.config import Settings
from app.core.scheduler import build_scheduler, report_social_shadow
from app.main import build_social_auto_verification_components, build_social_memory_components, validate_runtime_settings
from app.services.social_memory_service import SocialMemoryService
from app.services.twitterapi_io_client import TwitterApiIoClient
from app.services.twitterapi_io_profile_provider import TwitterApiIoSocialProfileProvider


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


def test_social_cleanup_job_only_registers_when_social_enabled() -> None:
    service = object()

    disabled = build_scheduler(settings(), None, None, social_event_service=service)
    enabled = build_scheduler(
        replace(settings(), social_x_enabled=True),
        None,
        None,
        social_event_service=service,
    )

    assert disabled.get_job("cleanup_social_events") is None
    assert enabled.get_job("cleanup_social_events") is not None
    assert enabled.get_job("cleanup_social_events").trigger.interval.total_seconds() == 86400


def test_social_discovery_job_requires_social_and_discovery_enabled() -> None:
    service = object()

    disabled = build_scheduler(
        settings(),
        None,
        None,
        social_discovery_service=service,
    )
    social_only = build_scheduler(
        replace(settings(), social_x_enabled=True, social_x_discovery_enabled=False),
        None,
        None,
        social_discovery_service=service,
    )
    enabled = build_scheduler(
        replace(
            settings(),
            social_x_enabled=True,
            social_x_discovery_enabled=True,
            social_x_discovery_dispatch_seconds=45,
        ),
        None,
        None,
        social_discovery_service=service,
    )

    assert disabled.get_job("scan_social_discovery") is None
    assert social_only.get_job("scan_social_discovery") is None
    assert enabled.get_job("scan_social_discovery") is not None
    assert enabled.get_job("scan_social_discovery").trigger.interval.total_seconds() == 45


def test_social_disabled_blocks_paid_social_jobs_but_keeps_attention_jobs() -> None:
    scheduler = build_scheduler(
        replace(settings(), social_x_enabled=False, social_x_discovery_enabled=True),
        None,
        None,
        price_guardian_service=None,
        attention_engine_service=object(),
        social_event_service=object(),
        social_discovery_service=object(),
        social_shadow_reporter=object(),
    )

    assert scheduler.get_job("cleanup_social_events") is None
    assert scheduler.get_job("scan_social_discovery") is None
    assert scheduler.get_job("report_social_shadow") is None
    assert scheduler.get_job("scan_attention_smart_money") is not None


def test_social_shadow_report_job_only_registers_when_social_enabled() -> None:
    reporter = object()

    disabled = build_scheduler(settings(), None, None, social_shadow_reporter=reporter)
    enabled = build_scheduler(
        replace(settings(), social_x_enabled=True, social_shadow_report_seconds=17),
        None,
        None,
        social_shadow_reporter=reporter,
    )

    assert disabled.get_job("report_social_shadow") is None
    assert enabled.get_job("report_social_shadow") is not None
    assert enabled.get_job("report_social_shadow").trigger.interval.total_seconds() == 17


@pytest.mark.asyncio
async def test_social_shadow_report_failure_is_isolated(caplog) -> None:
    class FailingReporter:
        def report(self) -> None:
            raise RuntimeError("report down")

    with caplog.at_level(logging.WARNING):
        await report_social_shadow(FailingReporter())

    assert "social shadow report failed" in caplog.text


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


def test_social_auto_verification_components_respect_feature_flags() -> None:
    client = TwitterApiIoClient("twitter-key", base_url="https://twitterapi.test")
    logger = logging.getLogger("test")

    social_disabled = build_social_auto_verification_components(
        replace(settings(), social_x_enabled=False, social_x_discovery_enabled=True, social_kol_auto_verify_enabled=True, deepseek_api_key="deepseek"),
        client,
        logger,
    )
    discovery_disabled = build_social_auto_verification_components(
        replace(settings(), social_x_enabled=True, social_x_discovery_enabled=False, social_kol_auto_verify_enabled=True, deepseek_api_key="deepseek"),
        client,
        logger,
    )
    auto_disabled = build_social_auto_verification_components(
        replace(settings(), social_x_enabled=True, social_x_discovery_enabled=True, social_kol_auto_verify_enabled=False, deepseek_api_key="deepseek"),
        client,
        logger,
    )

    assert social_disabled.auto_verify_enabled is False
    assert discovery_disabled.auto_verify_enabled is False
    assert auto_disabled.auto_verify_enabled is False
    assert social_disabled.profile_provider is None
    assert discovery_disabled.classifier is None
    assert auto_disabled.profile_provider is None


def test_social_auto_verification_wires_provider_and_classifier_when_key_exists() -> None:
    client = TwitterApiIoClient("twitter-key", base_url="https://twitterapi.test")

    components = build_social_auto_verification_components(
        replace(
            settings(),
            social_x_enabled=True,
            social_x_discovery_enabled=True,
            social_kol_auto_verify_enabled=True,
            deepseek_api_key="deepseek-key",
        ),
        client,
        logging.getLogger("test"),
    )

    assert components.auto_verify_enabled is True
    assert isinstance(components.profile_provider, TwitterApiIoSocialProfileProvider)
    assert components.classifier is not None


def test_social_auto_verification_missing_deepseek_key_degrades_without_blocking_discovery(caplog) -> None:
    client = TwitterApiIoClient("twitter-key", base_url="https://twitterapi.test")

    with caplog.at_level(logging.WARNING):
        components = build_social_auto_verification_components(
            replace(
                settings(),
                social_x_enabled=True,
                social_x_discovery_enabled=True,
                social_kol_auto_verify_enabled=True,
                deepseek_api_key=None,
            ),
            client,
            logging.getLogger("test"),
        )

    assert components.auto_verify_enabled is False
    assert components.profile_provider is None
    assert components.classifier is None
    assert "DEEPSEEK_API_KEY is missing" in caplog.text


def test_social_memory_components_default_disabled() -> None:
    component = build_social_memory_components(settings(), object(), logging.getLogger("test"))

    assert component is None


def test_social_memory_components_missing_deepseek_key_degrades_without_blocking_social(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        component = build_social_memory_components(
            replace(settings(), social_memory_enabled=True, deepseek_api_key=None),
            object(),
            logging.getLogger("test"),
        )

    assert component is None
    assert "SOCIAL_MEMORY_ENABLED=true but DEEPSEEK_API_KEY missing" in caplog.text


def test_social_memory_components_wire_deepseek_adapter_when_key_exists() -> None:
    component = build_social_memory_components(
        replace(
            settings(),
            social_memory_enabled=True,
            deepseek_api_key="deepseek-key",
            deepseek_base_url="https://deepseek.test",
            social_memory_triage_model="deepseek-chat",
        ),
        object(),
        logging.getLogger("test"),
    )

    assert isinstance(component, SocialMemoryService)
    assert component.triage_adapter is not None
