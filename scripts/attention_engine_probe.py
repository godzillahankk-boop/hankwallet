from __future__ import annotations

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select
from telegram import Bot

from app.core.config import load_settings
from app.db.database import make_engine, make_session_factory, session_scope
from app.db.models import TokenWatchState, Wallet
from app.services import attention_scoring as scoring
from app.services.attention_engine_service import AttentionEngineService, format_attention_alert
from app.services.gmgn_client import GmgnClient
from app.utils.address import is_valid_evm_address, normalize_evm_address


async def main() -> None:
    args = parse_args()
    settings = load_settings()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)

    gmgn_client = GmgnClient.from_settings(settings)
    try:
        service = AttentionEngineService(session_factory, gmgn_client, settings)
        wallet_id, chat_id, symbol = find_wallet_for_token(session_factory, args.token)
        assessment = await service.simulate_assessment(
            wallet_id,
            normalize_evm_address(args.token),
            symbol=symbol,
            family_scores={
                scoring.PRICE: args.price_score,
                scoring.HOLDER: args.holder_score,
                scoring.SMART_MONEY: args.smart_money_score,
                scoring.KOL: args.kol_score,
                scoring.LIQUIDITY: args.liquidity_score,
            },
            usd_value=Decimal(str(args.usd_value)),
            direction=args.direction,
        )
        text = "\n".join(["🧪 Attention Engine 测试提醒", "", format_attention_alert(assessment)])
        print(text)
        if args.send_telegram:
            bot = Bot(settings.telegram_bot_token)
            await bot.send_message(chat_id=chat_id, text=text)
            print()
            print("Telegram test alert sent.")
    finally:
        await gmgn_client.aclose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate Attention Engine scoring without writing DB data")
    parser.add_argument("--token", required=True)
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--price-score", type=int, default=30)
    parser.add_argument("--holder-score", type=int, default=20)
    parser.add_argument("--smart-money-score", type=int, default=10)
    parser.add_argument("--kol-score", type=int, default=0)
    parser.add_argument("--liquidity-score", type=int, default=0)
    parser.add_argument("--usd-value", type=str, default="100")
    parser.add_argument("--direction", default=scoring.NEGATIVE, choices=[scoring.POSITIVE, scoring.NEGATIVE, scoring.MIXED, scoring.NEUTRAL])
    args = parser.parse_args()
    if not args.simulate:
        raise SystemExit("This probe currently supports --simulate only.")
    if not is_valid_evm_address(args.token):
        raise SystemExit("Invalid EVM token address")
    return args


def find_wallet_for_token(session_factory, token: str) -> tuple[int, int, str]:
    normalized = normalize_evm_address(token)
    with session_scope(session_factory) as session:
        row = session.execute(
            select(TokenWatchState, Wallet)
            .join(Wallet, Wallet.id == TokenWatchState.wallet_id)
            .where(
                TokenWatchState.token_address == normalized,
                TokenWatchState.active.is_(True),
                Wallet.is_active.is_(True),
            )
            .order_by(TokenWatchState.last_seen_at.desc())
        ).first()
        if not row:
            raise SystemExit("No active watched position found for this token.")
        state, wallet = row
        return wallet.id, wallet.user.telegram_chat_id, state.symbol or normalized


if __name__ == "__main__":
    asyncio.run(main())
