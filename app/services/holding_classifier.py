from __future__ import annotations

from decimal import Decimal

from app.services.gmgn_client import GmgnHolding


def is_trading_position(holding: GmgnHolding) -> bool:
    if holding.balance is None or holding.balance <= 0:
        return False
    if holding.buy_tx_count is not None:
        return holding.buy_tx_count > 0
    if holding.historical_bought_cost_usd is not None:
        return holding.historical_bought_cost_usd > Decimal("0")
    return False
