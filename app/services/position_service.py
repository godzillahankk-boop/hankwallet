from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Position, PositionTransaction, Token
from app.services.chain_client import TokenBalance
from app.services.transaction_parser import ParsedTokenEvent
from app.utils.time import utc_now


class PositionService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_or_create_token(self, balance_or_event: TokenBalance | ParsedTokenEvent) -> Token:
        token_address = (
            balance_or_event.contract_address
            if isinstance(balance_or_event, TokenBalance)
            else balance_or_event.token_address
        )
        chain = balance_or_event.chain if isinstance(balance_or_event, TokenBalance) else "ethereum"
        token = self.session.scalar(
            select(Token).where(
                Token.chain == chain,
                Token.contract_address == token_address,
            )
        )
        if token:
            return token

        symbol = (
            balance_or_event.symbol
            if isinstance(balance_or_event, TokenBalance)
            else balance_or_event.token_symbol
        )
        name = (
            balance_or_event.name
            if isinstance(balance_or_event, TokenBalance)
            else balance_or_event.token_name
        )
        decimals = (
            balance_or_event.decimals
            if isinstance(balance_or_event, TokenBalance)
            else balance_or_event.token_decimals
        )
        token = Token(
            chain=chain,
            contract_address=token_address,
            symbol=symbol,
            name=name,
            decimals=decimals,
        )
        self.session.add(token)
        self.session.flush()
        return token

    def get_open_position(self, wallet_id: int, token_id: int) -> Position | None:
        return self.session.scalar(
            select(Position).where(
                Position.wallet_id == wallet_id,
                Position.token_id == token_id,
                Position.status == "OPEN",
            )
        )

    def get_or_create_open_position(
        self,
        wallet_id: int,
        token: Token,
        current_balance: Decimal,
        opened_at: datetime | None,
    ) -> tuple[Position, bool]:
        position = self.get_open_position(wallet_id, token.id)
        if position:
            return position, False
        position = Position(
            wallet_id=wallet_id,
            token_id=token.id,
            status="OPEN",
            token_amount=current_balance,
            last_balance=current_balance,
            opened_at=opened_at or utc_now(),
            last_checked_at=utc_now(),
        )
        self.session.add(position)
        self.session.flush()
        return position, True

    def get_or_create_ignored_position(
        self,
        wallet_id: int,
        token: Token,
        current_balance: Decimal,
    ) -> Position:
        position = self.session.scalar(
            select(Position).where(
                Position.wallet_id == wallet_id,
                Position.token_id == token.id,
                Position.status == "IGNORED",
            )
        )
        if position:
            position.token_amount = current_balance
            position.last_balance = current_balance
            position.last_checked_at = utc_now()
            return position
        position = Position(
            wallet_id=wallet_id,
            token_id=token.id,
            status="IGNORED",
            token_amount=current_balance,
            last_balance=current_balance,
            last_checked_at=utc_now(),
        )
        self.session.add(position)
        self.session.flush()
        return position

    def record_event(self, position: Position, event: ParsedTokenEvent) -> bool:
        existing = self.session.scalar(
            select(PositionTransaction.id).where(
                PositionTransaction.position_id == position.id,
                PositionTransaction.tx_hash == event.tx_hash,
                PositionTransaction.tx_type == event.tx_type,
            )
        )
        if existing:
            return False
        tx = PositionTransaction(
            position_id=position.id,
            tx_hash=event.tx_hash,
            tx_type=event.tx_type,
            token_amount=event.token_amount,
            quote_token=event.quote_token,
            quote_amount=event.quote_amount,
            block_number=event.block_number,
            tx_timestamp=event.tx_timestamp,
        )
        self.session.add(tx)
        self.session.flush()
        return True

    def list_open_positions_for_wallet(self, wallet_id: int) -> list[Position]:
        return list(
            self.session.scalars(
                select(Position)
                .where(Position.wallet_id == wallet_id, Position.status == "OPEN")
                .order_by(Position.opened_at.asc())
            )
        )

    def list_user_open_positions(self, telegram_user_id: int) -> list[Position]:
        from app.db.models import User, Wallet

        return list(
            self.session.scalars(
                select(Position)
                .join(Wallet)
                .join(User)
                .where(User.telegram_user_id == telegram_user_id, Position.status == "OPEN")
                .order_by(Position.opened_at.asc())
            )
        )
