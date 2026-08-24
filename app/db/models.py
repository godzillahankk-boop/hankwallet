from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    telegram_chat_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    wallets: Mapped[list["Wallet"]] = relationship(back_populates="user")


class Wallet(Base):
    __tablename__ = "wallets"
    __table_args__ = (UniqueConstraint("user_id", "chain", "address"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    address: Mapped[str] = mapped_column(String(64), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_scanned_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    user: Mapped["User"] = relationship(back_populates="wallets")
    positions: Mapped[list["Position"]] = relationship(back_populates="wallet")


class Token(Base):
    __tablename__ = "tokens"
    __table_args__ = (UniqueConstraint("chain", "contract_address"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    contract_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    decimals: Mapped[int] = mapped_column(Integer, default=18, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    positions: Mapped[list["Position"]] = relationship(back_populates="token")


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    token_id: Mapped[int] = mapped_column(ForeignKey("tokens.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="OPEN")
    token_amount: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False, default=0)
    avg_cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    invested_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    realized_pnl_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    unrealized_pnl_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_balance: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False, default=0)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    dust_below_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    wallet: Mapped["Wallet"] = relationship(back_populates="positions")
    token: Mapped["Token"] = relationship(back_populates="positions")
    transactions: Mapped[list["PositionTransaction"]] = relationship(
        back_populates="position"
    )


class PositionTransaction(Base):
    __tablename__ = "position_transactions"
    __table_args__ = (UniqueConstraint("position_id", "tx_hash", "tx_type"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    position_id: Mapped[int] = mapped_column(ForeignKey("positions.id"), nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(96), nullable=False)
    tx_type: Mapped[str] = mapped_column(String(32), nullable=False)
    token_amount: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False, default=0)
    quote_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    quote_amount: Mapped[Optional[float]] = mapped_column(Numeric(38, 18), nullable=True)
    usd_value: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    gas_fee: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    block_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tx_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    position: Mapped["Position"] = relationship(back_populates="transactions")


class MonitoringRun(Base):
    __tablename__ = "monitoring_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    tokens_found: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    positions_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    positions_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    positions_closed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
