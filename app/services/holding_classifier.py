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


def is_canonical_trading_position(
    holding: GmgnHolding,
    excluded_symbols: tuple[str, ...] = (),
) -> bool:
    excluded = {symbol.upper() for symbol in excluded_symbols}
    return (
        holding.contract_address is not None
        and (holding.symbol or "").upper() not in excluded
        and is_trading_position(holding)
    )


def is_display_position(
    holding: GmgnHolding,
    min_usd_value: Decimal,
    excluded_symbols: tuple[str, ...] = (),
) -> bool:
    return (
        is_canonical_trading_position(holding, excluded_symbols)
        and holding.usd_value is not None
        and holding.usd_value >= min_usd_value
    )


def is_below_threshold_position(
    holding: GmgnHolding,
    min_usd_value: Decimal,
    excluded_symbols: tuple[str, ...] = (),
) -> bool:
    return (
        is_canonical_trading_position(holding, excluded_symbols)
        and holding.usd_value is not None
        and holding.usd_value < min_usd_value
    )
