from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import Wallet  # noqa: E402
from app.services.daily_report_service import DailyReportService, format_daily_report_debug  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Daily Report V0.1 probe")
    parser.add_argument("--wallet-id", type=int, action="append", dest="wallet_ids")
    args = parser.parse_args()

    settings = load_settings()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    wallet_ids = args.wallet_ids or _active_wallet_ids(session_factory)
    service = DailyReportService(
        session_factory,
        report_timezone=settings.report_timezone,
        price_tolerance_minutes=settings.daily_report_price_tolerance_minutes,
    )
    report = service.build_yesterday_report(wallet_ids)
    print(format_daily_report_debug(report))


def _active_wallet_ids(session_factory) -> list[int]:
    with session_scope(session_factory) as session:
        return list(
            session.scalars(
                select(Wallet.id).where(Wallet.is_active.is_(True)).order_by(Wallet.id.asc())
            )
        )


if __name__ == "__main__":
    main()
