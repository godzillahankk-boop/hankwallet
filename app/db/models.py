from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
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


class WalletTokenBalance(Base):
    __tablename__ = "wallet_token_balances"
    __table_args__ = (UniqueConstraint("wallet_id", "asset_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_key: Mapped[str] = mapped_column(String(96), nullable=False)
    contract_address: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    decimals: Mapped[int] = mapped_column(Integer, default=18, nullable=False)
    token_amount: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False, default=0)
    exchange_rate_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 10), nullable=True)
    usd_value: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    is_native: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_checked_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
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


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"
    __table_args__ = (
        Index("ix_price_snapshots_wallet_token_observed", "wallet_id", "token_address", "observed_at"),
        Index("ix_price_snapshots_observed_at", "observed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    price_usd: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False)
    balance: Mapped[float] = mapped_column(Numeric(38, 18), nullable=False)
    usd_value: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class PriceAlertState(Base):
    __tablename__ = "price_alert_states"
    __table_args__ = (
        UniqueConstraint("wallet_id", "token_address", "window_minutes", "direction"),
        Index("ix_price_alert_states_wallet_token", "wallet_id", "token_address"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    window_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    last_alert_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_alert_change_pct: Mapped[Optional[float]] = mapped_column(Numeric(12, 4), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )


class TokenWatchState(Base):
    __tablename__ = "token_watch_states"
    __table_args__ = (
        Index("ix_token_watch_states_wallet_token_active", "wallet_id", "token_address", "active"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class TokenIntelligenceSnapshot(Base):
    __tablename__ = "token_intelligence_snapshots"
    __table_args__ = (
        Index(
            "ix_token_intel_snapshots_wallet_token_observed",
            "wallet_id",
            "token_address",
            "observed_at",
        ),
        Index("ix_token_intel_snapshots_observed_at", "observed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    market_cap_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    liquidity_usd: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    holder_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class TopHolderSnapshot(Base):
    __tablename__ = "top_holder_snapshots"
    __table_args__ = (
        Index(
            "ix_top_holder_snapshots_wallet_token_observed",
            "wallet_id",
            "token_address",
            "observed_at",
        ),
        Index("ix_top_holder_snapshots_holder", "wallet_id", "token_address", "holder_address"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    holder_address: Mapped[str] = mapped_column(String(64), nullable=False)
    balance: Mapped[Optional[float]] = mapped_column(Numeric(38, 18), nullable=True)
    hold_percentage: Mapped[Optional[float]] = mapped_column(Numeric(18, 10), nullable=True)
    usd_value: Mapped[Optional[float]] = mapped_column(Numeric(24, 8), nullable=True)
    dropped_out_top20: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class TopHolderPeak(Base):
    __tablename__ = "top_holder_peaks"
    __table_args__ = (
        UniqueConstraint("wallet_id", "token_address", "holder_address", "watch_started_at"),
        Index("ix_top_holder_peaks_wallet_token", "wallet_id", "token_address"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    holder_address: Mapped[str] = mapped_column(String(64), nullable=False)
    watch_started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    peak_balance: Mapped[Optional[float]] = mapped_column(Numeric(38, 18), nullable=True)
    peak_hold_percentage: Mapped[Optional[float]] = mapped_column(Numeric(18, 10), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class IntelligenceEvent(Base):
    __tablename__ = "intelligence_events"
    __table_args__ = (
        UniqueConstraint("event_fingerprint"),
        Index("ix_intelligence_events_wallet_token_at", "wallet_id", "token_address", "event_at"),
        Index("ix_intelligence_events_family", "family", "event_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    family: Mapped[str] = mapped_column(String(32), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    severity_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    event_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class AttentionAssessment(Base):
    __tablename__ = "attention_assessments"
    __table_args__ = (
        Index("ix_attention_assessments_wallet_token_at", "wallet_id", "token_address", "assessed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    assessed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    price_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    holder_breadth_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    top_holder_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    holder_cluster_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    holder_family_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    smart_money_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    kol_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    liquidity_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    primary_family: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    primary_event_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    position_exposure_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    abnormality_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    secondary_family_1: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    secondary_family_1_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    secondary_family_2: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    secondary_family_2_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    secondary_signal_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    base_attention_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    dev_modifier: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    final_attention_score: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    attention_level: Mapped[str] = mapped_column(String(16), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    should_notify: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class AttentionAlertState(Base):
    __tablename__ = "attention_alert_states"
    __table_args__ = (
        UniqueConstraint("wallet_id", "token_address"),
        Index("ix_attention_alert_states_wallet_token", "wallet_id", "token_address"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet_id: Mapped[int] = mapped_column(ForeignKey("wallets.id"), nullable=False)
    token_address: Mapped[str] = mapped_column(String(64), nullable=False)
    last_notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_attention_level: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    last_final_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_direction: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    family_scores_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
