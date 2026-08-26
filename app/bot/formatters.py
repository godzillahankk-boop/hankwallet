from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


def decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def format_holding_quantity(value: Decimal | None) -> str:
    if value is None:
        return "--"
    if abs(value) >= Decimal("100000"):
        return f"{_one_decimal(value / Decimal('1000')):,}k"
    return f"{_one_decimal(value):,}"


def format_holding_usd(value: Decimal | None) -> str:
    if value is None:
        return "$--"
    return f"${_one_decimal(value):,}"


def holding_pnl_pct(
    usd_value: Decimal | None,
    unrealized_profit: Decimal | None,
) -> Decimal | None:
    if usd_value is None or unrealized_profit is None:
        return None
    estimated_current_cost = usd_value - unrealized_profit
    if estimated_current_cost <= 0:
        return None
    return (unrealized_profit / estimated_current_cost) * Decimal("100")


def format_holding_pnl_pct(
    usd_value: Decimal | None,
    unrealized_profit: Decimal | None,
) -> str:
    pct = holding_pnl_pct(usd_value, unrealized_profit)
    if pct is None:
        return "--"
    rounded = _one_decimal(pct)
    sign = "+" if rounded >= 0 else ""
    return f"{sign}{rounded}%"


def _one_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
