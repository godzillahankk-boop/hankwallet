from __future__ import annotations

from decimal import Decimal


PRICE_QUALITY_VALID = "VALID"
PRICE_QUALITY_PENDING = "PENDING"
PRICE_QUALITY_OUTLIER = "OUTLIER"

PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP = "single_snapshot_jump"
PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT = "confirmed_next_snapshot"
PRICE_QUALITY_REASON_REVERTED_NEXT_SNAPSHOT = "reverted_next_snapshot"
PRICE_QUALITY_REASON_AMBIGUOUS_NEXT_SNAPSHOT = "ambiguous_next_snapshot"


def price_quality_jump_threshold(effective_5m_price_threshold: Decimal) -> Decimal:
    threshold = effective_5m_price_threshold * Decimal("2")
    if threshold < Decimal("20"):
        return Decimal("20")
    if threshold > Decimal("40"):
        return Decimal("40")
    return threshold


def percent_change(current: Decimal, baseline: Decimal) -> Decimal | None:
    if baseline <= 0:
        return None
    return ((current - baseline) / baseline) * Decimal("100")


def is_close_price(current: Decimal, baseline: Decimal, tolerance_pct: Decimal = Decimal("10")) -> bool:
    change = percent_change(current, baseline)
    return change is not None and abs(change) <= tolerance_pct
