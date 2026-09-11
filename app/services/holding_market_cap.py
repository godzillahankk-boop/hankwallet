from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.services.gmgn_client import GmgnHolding


@dataclass(frozen=True)
class HoldingMarketContext:
    wallet_id: int
    token_address: str
    current_market_cap_usd: Decimal
    entry_market_cap_usd: Decimal | None
    effective_avg_cost_usd: Decimal | None
    current_price_usd: Decimal
    total_supply: Decimal
    observed_at: datetime | None
    watch_started_at: datetime | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "entry_market_cap_usd": _str_or_none(self.entry_market_cap_usd),
            "current_market_cap_usd": str(self.current_market_cap_usd),
            "effective_avg_cost_usd": _str_or_none(self.effective_avg_cost_usd),
            "current_price_usd": str(self.current_price_usd),
            "total_supply": str(self.total_supply),
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "watch_started_at": self.watch_started_at.isoformat() if self.watch_started_at else None,
        }


class HoldingMarketContextCache:
    def __init__(self) -> None:
        self._contexts: dict[tuple[int, str], HoldingMarketContext] = {}

    def set(self, context: HoldingMarketContext) -> None:
        self._contexts[(context.wallet_id, context.token_address.lower())] = context

    def delete(self, wallet_id: int, token_address: str) -> None:
        self._contexts.pop((wallet_id, token_address.lower()), None)

    def get(
        self,
        wallet_id: int,
        token_address: str,
        *,
        watch_started_at: datetime | None,
        now: datetime,
        max_age_seconds: int,
    ) -> HoldingMarketContext | None:
        context = self._contexts.get((wallet_id, token_address.lower()))
        if not context:
            return None
        if context.watch_started_at != watch_started_at:
            return None
        if context.observed_at is None:
            return None
        if (now - context.observed_at).total_seconds() > max_age_seconds:
            return None
        return context


def build_holding_market_context(
    *,
    wallet_id: int,
    token_address: str,
    holding: GmgnHolding,
    observed_at: datetime | None,
    watch_started_at: datetime | None,
) -> HoldingMarketContext | None:
    price = holding.current_price_usd
    total_supply = holding.total_supply
    if price is None or price <= 0 or total_supply is None or total_supply <= 0:
        return None
    current_market_cap = price * total_supply
    effective_avg_cost: Decimal | None = None
    entry_market_cap: Decimal | None = None
    if (
        holding.balance is not None
        and holding.balance > 0
        and holding.usd_value is not None
        and holding.unrealized_profit_usd is not None
    ):
        remaining_cost = holding.usd_value - holding.unrealized_profit_usd
        effective_avg_cost = remaining_cost / holding.balance
        entry_market_cap = effective_avg_cost * total_supply
    return HoldingMarketContext(
        wallet_id=wallet_id,
        token_address=token_address,
        current_market_cap_usd=current_market_cap,
        entry_market_cap_usd=entry_market_cap,
        effective_avg_cost_usd=effective_avg_cost,
        current_price_usd=price,
        total_supply=total_supply,
        observed_at=observed_at,
        watch_started_at=watch_started_at,
    )


def _str_or_none(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
