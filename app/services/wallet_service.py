from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import User, Wallet
from app.utils.address import is_valid_evm_address, normalize_evm_address


class WalletService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_or_create_user(self, telegram_user_id: int, telegram_chat_id: int) -> User:
        user = self.session.scalar(
            select(User).where(User.telegram_user_id == telegram_user_id)
        )
        if user:
            user.telegram_chat_id = telegram_chat_id
            return user
        user = User(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
        )
        self.session.add(user)
        self.session.flush()
        return user

    def add_wallet(
        self,
        telegram_user_id: int,
        telegram_chat_id: int,
        address: str,
        chain: str,
        name: str | None = None,
    ) -> Wallet:
        if not is_valid_evm_address(address):
            raise ValueError("invalid_wallet_address")
        user = self.get_or_create_user(telegram_user_id, telegram_chat_id)
        normalized = normalize_evm_address(address)
        existing = self.session.scalar(
            select(Wallet).where(
                Wallet.user_id == user.id,
                Wallet.chain == chain,
                Wallet.address == normalized,
            )
        )
        if existing:
            raise ValueError("wallet_already_exists")
        wallet = Wallet(user_id=user.id, address=normalized, chain=chain, name=name)
        self.session.add(wallet)
        self.session.flush()
        return wallet

    def list_wallets(self, telegram_user_id: int) -> list[Wallet]:
        user = self.session.scalar(
            select(User).where(User.telegram_user_id == telegram_user_id)
        )
        if not user:
            return []
        return list(
            self.session.scalars(
                select(Wallet)
                .where(Wallet.user_id == user.id, Wallet.is_active.is_(True))
                .order_by(Wallet.created_at.asc())
            )
        )

    def get_active_wallets(self) -> list[Wallet]:
        return list(
            self.session.scalars(
                select(Wallet).where(Wallet.is_active.is_(True)).order_by(Wallet.id.asc())
            )
        )

    def deactivate_wallet(self, telegram_user_id: int, address: str, chain: str) -> bool:
        normalized = normalize_evm_address(address)
        wallet = self.session.scalar(
            select(Wallet)
            .join(User)
            .where(
                User.telegram_user_id == telegram_user_id,
                Wallet.chain == chain,
                Wallet.address == normalized,
                Wallet.is_active.is_(True),
            )
        )
        if not wallet:
            return False
        wallet.is_active = False
        return True

