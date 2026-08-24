from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.database import session_scope
from app.services.monitoring_service import MonitoringService
from app.services.wallet_service import WalletService

logger = logging.getLogger(__name__)


def build_scheduler(
    settings: Settings,
    session_factory: sessionmaker,
    monitoring_service: MonitoringService,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        scan_active_wallets,
        "interval",
        seconds=settings.wallet_scan_interval_seconds,
        args=[session_factory, monitoring_service],
        id="scan_active_wallets",
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

