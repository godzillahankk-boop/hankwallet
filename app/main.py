from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

from app.bot.handlers import build_application
from app.core.config import load_settings
from app.core.scheduler import build_scheduler
from app.db.database import init_db, make_engine, make_session_factory
from app.services.chain_client import build_chain_client
from app.services.attention_engine_service import AttentionEngineService
from app.services.gmgn_client import GmgnClient
from app.services.monitoring_service import MonitoringService
from app.services.price_guardian_service import PriceGuardianService
from app.services.social_discovery_service import SocialDiscoveryService
from app.services.social_event_service import SocialEventService
from app.services.social_kol_service import DeepSeekSocialKOLClassifier, SocialKOLService
from app.services.social_memory_service import DeepSeekSocialSemanticTriageAdapter, SocialMemoryService
from app.services.social_shadow_reporter import SocialShadowReporter
from app.services.social_watch_registry import SocialWatchRegistry
from app.services.twitterapi_io_client import TwitterApiIoClient
from app.services.twitterapi_io_profile_provider import TwitterApiIoSocialProfileProvider
from app.services.twitterapi_io_social_ingestion import (
    TwitterApiIoRuleManager,
    TwitterApiIoSocialIngestor,
)
from app.utils.logger import setup_logging

api_app = FastAPI(title="Wallet Agent", version="0.1.0")


@dataclass(frozen=True)
class SocialAutoVerificationComponents:
    profile_provider: TwitterApiIoSocialProfileProvider | None
    classifier: DeepSeekSocialKOLClassifier | None
    auto_verify_enabled: bool


@api_app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def run() -> None:
    settings = load_settings()
    validate_runtime_settings(settings)
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    logger.info("App startup")

    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)

    chain_client = build_chain_client(
        settings.chain_api_base_url,
        settings.chain_api_key,
        settings.chain_rpc_url,
        settings.chain_token_search_symbols,
        settings.chain_request_timeout_seconds,
    )

    telegram_app_holder = {}

    async def notify(chat_id: int, text: str, reply_markup=None) -> None:
        telegram_app = telegram_app_holder["app"]
        await telegram_app.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

    monitoring_service = MonitoringService(
        session_factory=session_factory,
        chain_client=chain_client,
        settings=settings,
        notifier=notify,
    )
    gmgn_client = None
    price_guardian_service = None
    attention_engine_service = None
    social_event_service = None
    social_discovery_service = None
    social_ingestor = None
    social_shadow_reporter = None
    if settings.gmgn_enabled and (settings.price_guardian_enabled or settings.attention_engine_enabled):
        try:
            gmgn_client = GmgnClient.from_settings(settings)
            gmgn_client.min_request_interval_seconds = max(
                gmgn_client.min_request_interval_seconds,
                1.0,
            )
            if settings.attention_engine_enabled:
                attention_engine_service = AttentionEngineService(
                    session_factory=session_factory,
                    gmgn_client=gmgn_client,
                    settings=settings,
                    notifier=notify,
                )
            if settings.price_guardian_enabled:
                price_guardian_service = PriceGuardianService(
                    session_factory=session_factory,
                    gmgn_client=gmgn_client,
                    settings=settings,
                    notifier=notify,
                    price_attention_trigger=attention_engine_service.handle_price_snapshot_update
                    if attention_engine_service
                    else None,
                )
        except Exception as exc:
            logger.exception("GMGN error GMGN-backed services disabled during startup: %s", exc)
    if settings.social_x_enabled:
        try:
            if settings.social_x_provider != "twitterapi_io":
                raise RuntimeError(f"Unsupported SOCIAL_X_PROVIDER={settings.social_x_provider}")
            if not settings.twitterapi_io_api_key:
                raise RuntimeError("TWITTERAPI_IO_API_KEY is required when SOCIAL_X_ENABLED=true")
            twitter_client = TwitterApiIoClient(settings.twitterapi_io_api_key)
            social_memory_service = build_social_memory_components(settings, session_factory, logger)
            social_event_service = SocialEventService(session_factory, memory_processor=social_memory_service)
            social_kol_service = SocialKOLService(session_factory)
            social_kol_service.bootstrap_fixed_kols(settings.social_x_kol_config_path)
            social_registry = SocialWatchRegistry(
                session_factory,
                kol_config_path=settings.social_x_kol_config_path,
            )
            verification = None
            if settings.social_x_discovery_enabled:
                verification = build_social_auto_verification_components(settings, twitter_client, logger)
                social_discovery_service = SocialDiscoveryService(
                    session_factory=session_factory,
                    search_client=twitter_client,
                    kol_service=social_kol_service,
                    event_service=social_event_service,
                    due_seconds=settings.social_x_discovery_due_seconds,
                    max_due_seconds=settings.social_x_discovery_max_due_seconds,
                    bootstrap_lookback_seconds=settings.social_x_discovery_bootstrap_lookback_seconds,
                    batch_size=settings.social_x_discovery_batch_size,
                    max_pages=settings.social_x_discovery_max_pages,
                    overlap_seconds=settings.social_x_discovery_overlap_seconds,
                    min_usd_value=settings.price_monitor_min_usd_value,
                    cashtag_enabled=settings.social_x_discovery_cashtag_enabled,
                    profile_provider=verification.profile_provider,
                    classifier=verification.classifier,
                    auto_verify_enabled=verification.auto_verify_enabled,
                    verify_batch_size=settings.social_kol_verify_batch_size,
                    verify_max_concurrency=settings.social_kol_verify_max_concurrency,
                )
            rule_manager = TwitterApiIoRuleManager(
                twitter_client,
                interval_seconds=settings.social_x_filter_interval_seconds,
                max_value_chars=settings.social_x_rule_max_value_chars,
                min_update_interval_seconds=settings.social_x_rule_min_update_interval_seconds,
                shard_state_path=settings.social_x_rule_shard_state_path,
            )
            social_ingestor = TwitterApiIoSocialIngestor(
                api_key=settings.twitterapi_io_api_key,
                registry=social_registry,
                event_service=social_event_service,
                rule_manager=rule_manager,
                ws_url=settings.social_x_websocket_url,
                rule_refresh_seconds=settings.social_x_rule_refresh_seconds,
                warmup_grace_seconds=settings.social_x_warmup_grace_seconds,
            )
            social_shadow_reporter = SocialShadowReporter(
                session_factory=session_factory,
                min_usd_value=settings.price_monitor_min_usd_value,
                stream_ingestor=social_ingestor,
                discovery_service=social_discovery_service,
                twitter_client=twitter_client,
                deepseek_classifier=verification.classifier if verification else None,
            )
        except Exception as exc:
            logger.warning("Social X disabled during startup: %s", exc)
    telegram_app = build_application(
        settings,
        session_factory,
        monitoring_service,
        price_guardian_service,
        attention_engine_service,
    )
    telegram_app_holder["app"] = telegram_app
    scheduler = build_scheduler(
        settings,
        session_factory,
        monitoring_service,
        price_guardian_service,
        attention_engine_service,
        social_event_service,
        social_discovery_service,
        social_shadow_reporter,
    )

    await telegram_app.initialize()
    await telegram_app.start()
    if telegram_app.updater:
        await telegram_app.updater.start_polling()
    logger.info("Telegram startup")

    scheduler.start()
    logger.info("Scheduler startup")
    if social_ingestor:
        try:
            await social_ingestor.start()
            logger.info("Social X stream startup")
        except Exception as exc:
            logger.warning("Social X stream failed to start: %s", exc)

    server = uvicorn.Server(
        uvicorn.Config(
            api_app,
            host=settings.api_host,
            port=settings.api_port,
            log_level=settings.log_level.lower(),
        )
    )
    try:
        await server.serve()
    finally:
        scheduler.shutdown(wait=False)
        if telegram_app.updater:
            await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()
        if gmgn_client:
            await gmgn_client.aclose()
        if social_ingestor:
            await social_ingestor.stop()
        if social_shadow_reporter:
            social_shadow_reporter.report_final()
        await chain_client.aclose()


def validate_runtime_settings(settings) -> None:
    if settings.attention_engine_enabled and not settings.price_guardian_enabled:
        raise RuntimeError("ATTENTION_ENGINE_ENABLED requires PRICE_GUARDIAN_ENABLED in V0.4")


def build_social_auto_verification_components(
    settings,
    twitter_client: TwitterApiIoClient,
    logger: logging.Logger,
) -> SocialAutoVerificationComponents:
    if (
        not settings.social_x_enabled
        or not settings.social_x_discovery_enabled
        or not settings.social_kol_auto_verify_enabled
    ):
        return SocialAutoVerificationComponents(None, None, False)
    if not settings.deepseek_api_key:
        logger.warning(
            "SOCIAL_KOL_AUTO_VERIFY_ENABLED=true but DEEPSEEK_API_KEY is missing; "
            "Social discovery will save candidates without auto verification"
        )
        return SocialAutoVerificationComponents(None, None, False)
    return SocialAutoVerificationComponents(
        TwitterApiIoSocialProfileProvider(twitter_client),
        DeepSeekSocialKOLClassifier(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            model=settings.social_kol_verify_model,
        ),
        True,
    )


def build_social_memory_components(settings, session_factory, logger):  # noqa: ANN001
    if not settings.social_memory_enabled:
        return None
    if not settings.deepseek_api_key:
        logger.warning("SOCIAL_MEMORY_ENABLED=true but DEEPSEEK_API_KEY missing; Social Memory triage disabled")
        return None
    return SocialMemoryService(
        session_factory,
        triage_adapter=DeepSeekSocialSemanticTriageAdapter(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            model=settings.social_memory_triage_model,
        ),
    )


def main() -> None:
    asyncio.run(run())
