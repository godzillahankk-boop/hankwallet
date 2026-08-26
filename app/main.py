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
from app.services.monitoring_service import MonitoringService
from app.utils.logger import setup_logging

api_app = FastAPI(title="Wallet Agent", version="0.1.0")


@api_app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def run() -> None:
    settings = load_settings()
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
    telegram_app = build_application(settings, session_factory, monitoring_service)
    telegram_app_holder["app"] = telegram_app
    scheduler = build_scheduler(settings, session_factory, monitoring_service)

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
        await chain_client.aclose()


def main() -> None:
    asyncio.run(run())
