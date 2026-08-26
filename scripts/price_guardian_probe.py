from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import load_settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import User, Wallet
from app.services.gmgn_client import GmgnClient
from app.services.price_guardian_service import (
    DOWN,
    PriceAlert,
    PriceMovement,
    PriceGuardianService,
    format_price_alert,
    format_balance,
    format_price,
    format_usd,
)
from app.utils.address import is_valid_evm_address, normalize_evm_address


async def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Price Guardian monitoring scope")
    parser.add_argument("wallet_address")
    parser.add_argument("--chain", default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--test-alert", action="store_true")
    args = parser.parse_args()

    if not args.once and not args.test_alert:
        raise SystemExit("Use --once or --test-alert")
    if not is_valid_evm_address(args.wallet_address):
        raise SystemExit("Invalid EVM wallet address")

    settings = load_settings()
    chain = (args.chain or settings.default_chain).lower()
    if args.test_alert:
        await _send_test_alert(settings, normalize_evm_address(args.wallet_address), chain)
        return
    with tempfile.TemporaryDirectory(prefix="wallet-agent-price-probe-") as temp_dir:
        engine = make_engine(f"sqlite:///{Path(temp_dir) / 'probe.db'}")
        init_db(engine)
        session_factory = make_session_factory(engine)
        wallet_id = _ensure_probe_wallet(session_factory, normalize_evm_address(args.wallet_address), chain)

        gmgn_client = GmgnClient.from_settings(settings)
        service = PriceGuardianService(session_factory, gmgn_client, settings, notifier=None)
        try:
            with session_scope(session_factory) as session:
                wallet = session.get(Wallet, wallet_id)
                if not wallet:
                    raise SystemExit("Probe wallet not found")
                holdings = await service.fetch_all_holdings(wallet.chain, wallet.address)
            candidates = service.classify_holdings(holdings)
            print("=== PRICE GUARDIAN HOLDINGS ===")
            for candidate in candidates:
                holding = candidate.holding
                print(
                    " | ".join(
                        [
                            holding.symbol or "-",
                            holding.contract_address or "-",
                            f"price={format_price(holding.current_price_usd) if holding.current_price_usd is not None else '-'}",
                            f"balance={format_balance(holding.balance) if holding.balance is not None else '-'}",
                            f"usd_value={format_usd(holding.usd_value) if holding.usd_value is not None else '-'}",
                            candidate.reason,
                        ]
                    )
                )
        finally:
            await gmgn_client.aclose()


def _ensure_probe_wallet(session_factory, address: str, chain: str) -> int:
    with session_scope(session_factory) as session:
        user = session.scalar(select(User).where(User.telegram_user_id == 0))
        if user is None:
            user = User(telegram_user_id=0, telegram_chat_id=0)
            session.add(user)
            session.flush()
        wallet = session.scalar(
            select(Wallet).where(
                Wallet.user_id == user.id,
                Wallet.chain == chain,
                Wallet.address == address,
            )
        )
        if wallet is None:
            wallet = Wallet(user_id=user.id, chain=chain, address=address, is_active=True)
            session.add(wallet)
            session.flush()
        return wallet.id


async def _send_test_alert(settings, wallet_address: str, chain: str) -> None:
    from telegram import Bot

    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    with session_scope(session_factory) as session:
        wallet = session.scalar(
            select(Wallet).where(
                Wallet.address == wallet_address,
                Wallet.chain == chain,
                Wallet.is_active.is_(True),
            )
        )
        if not wallet:
            raise SystemExit("Wallet not found in local database. Add it in Telegram first.")
        chat_id = wallet.user.telegram_chat_id

    message = build_test_alert_message()
    async with Bot(settings.telegram_bot_token) as bot:
        await bot.send_message(chat_id=chat_id, text=message)
    print("test alert sent")


def build_test_alert_message() -> str:
    alert = PriceAlert(
        wallet_id=0,
        chat_id=0,
        token_address="test",
        symbol="TEST",
        price_usd=Decimal("0.0000003712"),
        balance=Decimal("249045.7425"),
        usd_value=Decimal("56.04"),
        movements=[PriceMovement(window_minutes=5, change_pct=Decimal("-12.5"), direction=DOWN)],
        watch_state_id=0,
    )
    return "\n".join(
        [
            "🧪 Price Guardian 测试提醒",
            "",
            "这是通知链路测试，不代表真实行情异动。",
            "",
            format_price_alert(alert),
        ]
    )


if __name__ == "__main__":
    asyncio.run(main())
