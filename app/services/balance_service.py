from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import User, Wallet, WalletTokenBalance
from app.utils.time import utc_now


class BalanceService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_user_balances(
        self, telegram_user_id: int, limit: int = 25
    ) -> list[WalletTokenBalance]:
        balances = list(
            self.session.scalars(
                select(WalletTokenBalance)
                .join(Wallet)
                .join(User)
                .where(
                    User.telegram_user_id == telegram_user_id,
                    Wallet.is_active.is_(True),
                    WalletTokenBalance.token_amount > 0,
                )
            )
        )
        balances.sort(
            key=lambda item: (
                Decimal(str(item.usd_value or "0")),
                Decimal(str(item.token_amount or "0")),
            ),
            reverse=True,
        )
        return balances[:limit]

    def wallet_has_recent_snapshot(self, wallet_id: int, max_age_seconds: int) -> bool:
        checked_times = list(
            self.session.scalars(
                select(WalletTokenBalance.last_checked_at).where(
                    WalletTokenBalance.wallet_id == wallet_id,
                    WalletTokenBalance.token_amount > 0,
                )
            )
        )
        if not checked_times:
            return False
        newest = max(checked_times)
        return newest >= utc_now() - timedelta(seconds=max_age_seconds)
