from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.database import session_scope
from app.services.attention_engine_service import AttentionEngineService
from app.services.monitoring_service import MonitoringService
from app.services.price_guardian_service import PriceGuardianService
from app.services.wallet_service import WalletService

logger = logging.getLogger(__name__)


def build_scheduler(
    settings: Settings,
    session_factory: sessionmaker,
    monitoring_service: MonitoringService,
    price_guardian_service: PriceGuardianService | None = None,
    attention_engine_service: AttentionEngineService | None = None,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    if settings.legacy_wallet_scan_enabled:
        scheduler.add_job(
            scan_active_wallets,
            "interval",
            seconds=settings.wallet_scan_interval_seconds,
            args=[session_factory, monitoring_service],
            id="scan_active_wallets",
            max_instances=1,
            coalesce=True,
        )
    if settings.price_guardian_enabled and price_guardian_service:
        scheduler.add_job(
            scan_price_guardian,
            "interval",
            seconds=settings.price_scan_interval_seconds,
            args=[price_guardian_service],
            id="scan_price_guardian",
            max_instances=1,
            coalesce=True,
        )
    if settings.attention_engine_enabled and attention_engine_service:
        scheduler.add_job(
            scan_attention_smart_money,
            "interval",
            seconds=settings.attention_smart_money_interval_seconds,
            args=[attention_engine_service],
            id="scan_attention_smart_money",
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            scan_attention_kol,
            "interval",
            seconds=settings.attention_kol_interval_seconds,
            args=[attention_engine_service],
            id="scan_attention_kol",
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            scan_attention_market_signals,
            "interval",
            seconds=settings.attention_market_signal_interval_seconds,
            args=[attention_engine_service],
            id="scan_attention_market_signals",
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            scan_attention_token_snapshots,
            "interval",
            seconds=settings.attention_token_snapshot_dispatch_seconds,
            args=[attention_engine_service],
            id="scan_attention_token_snapshots",
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            scan_attention_top_holders,
            "interval",
            seconds=settings.attention_top_holder_dispatch_seconds,
            args=[attention_engine_service],
            id="scan_attention_top_holders",
            max_instances=1,
            coalesce=True,
        )
    return scheduler


async def scan_active_wallets(
    session_factory: sessionmaker,
    monitoring_service: MonitoringService,
) -> None:
    try:
        with session_scope(session_factory) as session:
            wallet_ids = [wallet.id for wallet in WalletService(session).get_active_wallets()]
        for wallet_id in wallet_ids:
            try:
                await monitoring_service.scan_wallet(wallet_id, reason="scheduler")
            except Exception as exc:
                logger.exception("Scheduler Error wallet_id=%s: %s", wallet_id, exc)
    except Exception as exc:
        logger.exception("Scheduler Error active wallet scan failed: %s", exc)


async def scan_price_guardian(price_guardian_service: PriceGuardianService) -> None:
    try:
        await price_guardian_service.scan_active_wallets()
    except Exception as exc:
        logger.exception("Scheduler Error price guardian scan failed: %s", exc)


async def scan_attention_smart_money(attention_engine_service: AttentionEngineService) -> None:
    try:
        await attention_engine_service.scan_smart_money_feed()
    except Exception as exc:
        logger.exception("Scheduler Error attention smart money scan failed: %s", exc)


async def scan_attention_kol(attention_engine_service: AttentionEngineService) -> None:
    try:
        await attention_engine_service.scan_kol_feed()
    except Exception as exc:
        logger.exception("Scheduler Error attention KOL scan failed: %s", exc)


async def scan_attention_market_signals(attention_engine_service: AttentionEngineService) -> None:
    try:
        await attention_engine_service.scan_market_signals()
    except Exception as exc:
        logger.exception("Scheduler Error attention market signal scan failed: %s", exc)


async def scan_attention_token_snapshots(attention_engine_service: AttentionEngineService) -> None:
    try:
        await attention_engine_service.scan_token_snapshots()
    except Exception as exc:
        logger.exception("Scheduler Error attention token snapshot scan failed: %s", exc)


async def scan_attention_top_holders(attention_engine_service: AttentionEngineService) -> None:
    try:
        await attention_engine_service.scan_top_holder_snapshots()
    except Exception as exc:
        logger.exception("Scheduler Error attention top holder scan failed: %s", exc)
