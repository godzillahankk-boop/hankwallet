from __future__ import annotations

import asyncio
import logging

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
from app.utils.logger import setup_logging

api_app = FastAPI(title="Wallet Agent", version="0.1.0")


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

    async def notify(chat_id: int, text: str) -> None:
        telegram_app = telegram_app_holder["app"]
        await telegram_app.bot.send_message(chat_id=chat_id, text=text)

    monitoring_service = MonitoringService(
        session_factory=session_factory,
        chain_client=chain_client,
        settings=settings,
        notifier=notify,
    )
    gmgn_client = None
    price_guardian_service = None
    attention_engine_service = None
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
    )

    await telegram_app.initialize()
    await telegram_app.start()
    if telegram_app.updater:
        await telegram_app.updater.start_polling()
    logger.info("Telegram startup")

    scheduler.start()
    logger.info("Scheduler startup")

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
        await chain_client.aclose()


def validate_runtime_settings(settings) -> None:
    if settings.attention_engine_enabled and not settings.price_guardian_enabled:
        raise RuntimeError("ATTENTION_ENGINE_ENABLED requires PRICE_GUARDIAN_ENABLED in V0.4")


def main() -> None:
    asyncio.run(run())
