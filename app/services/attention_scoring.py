from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


IGNORE = "IGNORE"
NOTICE = "NOTICE"
WARNING = "WARNING"
CRITICAL = "CRITICAL"

POSITIVE = "positive"
NEGATIVE = "negative"
MIXED = "mixed"
NEUTRAL = "neutral"

PRICE = "price"
HOLDER = "holder"
SMART_MONEY = "smart_money"
KOL = "kol"
LIQUIDITY = "liquidity"


@dataclass(frozen=True)
class SecondarySignal:
    family_1: str | None
    family_1_score: int
    family_2: str | None
    family_2_score: int
    score: int


def position_exposure_score(usd_value: Decimal | None) -> int:
    if usd_value is None or usd_value < Decimal("5"):
        return 0
    if usd_value < Decimal("20"):
        return 5
    if usd_value < Decimal("50"):
        return 8
    if usd_value < Decimal("100"):
        return 10
    if usd_value < Decimal("250"):
        return 14
    if usd_value < Decimal("500"):
        return 17
    return 20


def price_threshold(window_minutes: int, market_cap: Decimal, liquidity: Decimal | None, direction: str) -> Decimal:
    if market_cap < Decimal("50000"):
        base = {5: Decimal("25"), 15: Decimal("35"), 60: Decimal("50")}[window_minutes]
    elif market_cap < Decimal("250000"):
        base = {5: Decimal("20"), 15: Decimal("30"), 60: Decimal("40")}[window_minutes]
    elif market_cap < Decimal("1000000"):
        base = {5: Decimal("15"), 15: Decimal("22"), 60: Decimal("32")}[window_minutes]
    elif market_cap < Decimal("10000000"):
        base = {5: Decimal("10"), 15: Decimal("15"), 60: Decimal("25")}[window_minutes]
    else:
        base = {5: Decimal("7"), 15: Decimal("12"), 60: Decimal("20")}[window_minutes]

    if liquidity is not None and market_cap > 0:
        ratio = liquidity / market_cap
        if ratio < Decimal("0.01"):
            base *= Decimal("1.5")
        elif ratio < Decimal("0.03"):
            base *= Decimal("1.3")
        elif ratio < Decimal("0.08"):
            base *= Decimal("1.15")
        elif ratio >= Decimal("0.20"):
            base *= Decimal("0.9")
    if direction == NEGATIVE:
        base *= Decimal("0.85")
    return base


def price_impact_score(
    change_pct: Decimal,
    window_minutes: int,
    market_cap: Decimal | None,
    liquidity: Decimal | None,
) -> int:
    if market_cap is None or market_cap <= 0 or change_pct == 0:
        return 0
    direction = POSITIVE if change_pct > 0 else NEGATIVE
    threshold = price_threshold(window_minutes, market_cap, liquidity, direction)
    magnitude = abs(change_pct)
    if magnitude < threshold:
        return 0
    multiple = magnitude / threshold
    if multiple >= Decimal("2"):
        return 40
    if multiple >= Decimal("1.5"):
        return 35
    return 30


def abnormality_score(current_abs_change: Decimal | None, historical_abs_changes: list[Decimal]) -> int:
    if current_abs_change is None:
        return 0
    if len(historical_abs_changes) < 20:
        return 5
    p90 = percentile(historical_abs_changes, Decimal("0.90"))
    p95 = percentile(historical_abs_changes, Decimal("0.95"))
    p99 = percentile(historical_abs_changes, Decimal("0.99"))
    if current_abs_change >= p99:
        return 15
    if current_abs_change >= p95:
        return 10
    if current_abs_change >= p90:
        return 5
    return 0


def percentile(values: list[Decimal], pct: Decimal) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        return Decimal("0")
    index = int(((Decimal(len(ordered) - 1) * pct).to_integral_value(rounding=ROUND_HALF_UP)))
    return ordered[max(0, min(index, len(ordered) - 1))]


def holder_breadth_score(current: int | None, baseline: int | None) -> int:
    if current is None or baseline is None or baseline <= 0:
        return 0
    delta = current - baseline
    if not _holder_abs_gate(abs(delta), baseline):
        return 0
    change_pct = (Decimal(delta) / Decimal(baseline)) * Decimal("100")
    if change_pct >= 0:
        if change_pct < Decimal("10"):
            return 0
        if change_pct < Decimal("20"):
            return 5
        if change_pct < Decimal("40"):
            return 10
        if change_pct < Decimal("70"):
            return 15
        if change_pct < Decimal("100"):
            return 18
        return 20
    drop = abs(change_pct)
    if drop < Decimal("5"):
        return 0
    if drop < Decimal("10"):
        return 5
    if drop < Decimal("20"):
        return 10
    if drop < Decimal("30"):
        return 15
    if drop < Decimal("50"):
        return 18
    return 20


def _holder_abs_gate(abs_delta: int, baseline: int) -> bool:
    if baseline < 100:
        return abs_delta >= 15
    if baseline < 500:
        return abs_delta >= 30
    if baseline < 2000:
        return abs_delta >= 75
    return abs_delta >= 150


def top_holder_reduction_score(
    current_balance: Decimal | None,
    peak_balance: Decimal | None,
    peak_hold_percentage: Decimal | None,
    confirmed_closed: bool = False,
) -> int:
    if peak_balance is None or peak_balance <= 0:
        return 0
    if peak_hold_percentage is None or peak_hold_percentage < Decimal("0.01"):
        return 0
    if confirmed_closed:
        return 40
    if current_balance is None:
        return 0
    current = current_balance
    reduction = Decimal("1") - (current / peak_balance)
    if reduction < Decimal("0.20"):
        return 0
    if reduction < Decimal("0.30"):
        return 15
    if reduction < Decimal("0.50"):
        return 25
    if reduction < Decimal("0.80"):
        return 35
    return 38


def holder_cluster_score(reductions: list[Decimal], total_baseline: Decimal | None, total_current: Decimal | None) -> int:
    if sum(1 for reduction in reductions if reduction >= Decimal("0.20")) >= 3:
        return 30
    return 0


def smart_money_score(
    distinct_wallets: int,
    net_flow_usd: Decimal,
    liquidity_usd: Decimal | None,
    close_count: int,
    activity_count: int,
) -> int:
    if activity_count <= 0:
        return 0
    threshold_1 = max(Decimal("1000"), (liquidity_usd or Decimal("0")) * Decimal("0.01"))
    threshold_2 = max(Decimal("3000"), (liquidity_usd or Decimal("0")) * Decimal("0.03"))
    threshold_3 = max(Decimal("5000"), (liquidity_usd or Decimal("0")) * Decimal("0.05"))
    abs_flow = abs(net_flow_usd)
    if (distinct_wallets >= 5 or close_count >= 2) and abs_flow >= threshold_3:
        return 40
    if distinct_wallets >= 4 and abs_flow >= threshold_2:
        return 35
    if distinct_wallets >= 3 and abs_flow >= threshold_1:
        return 25
    return 10


def kol_score(distinct_traders: int, close_count: int, activity_count: int, call_count: int = 0) -> int:
    if close_count >= 2:
        return 40
    if distinct_traders >= 3 and activity_count >= 3:
        return 35
    if distinct_traders >= 2 and activity_count >= 2:
        return 28
    if activity_count >= 1:
        return 18
    if call_count >= 1:
        return 8
    return 0


def liquidity_score(current: Decimal | None, baseline: Decimal | None, migration_status: str | None = None) -> int:
    if migration_status == "migration":
        return 0
    if current is None or baseline is None or baseline <= 0:
        return 0
    drop = Decimal("1") - (current / baseline)
    if drop < Decimal("0.20"):
        return 0
    if drop < Decimal("0.30"):
        return 25
    if drop < Decimal("0.50"):
        return 35
    return 40


def holder_family_score(breadth: int, top_holder: int, cluster: int) -> int:
    return max(breadth, top_holder, cluster)


def secondary_signal_score(family_scores: dict[str, int], primary_family: str | None) -> SecondarySignal:
    secondary = [
        (family, score)
        for family, score in sorted(family_scores.items(), key=lambda item: item[1], reverse=True)
        if family != primary_family and score > 0
    ]
    first_family, first_score = secondary[0] if secondary else (None, 0)
    second_family, second_score = secondary[1] if len(secondary) > 1 else (None, 0)
    score = int(min(Decimal("20"), Decimal(first_score) * Decimal("0.6") + Decimal(second_score) * Decimal("0.3")))
    return SecondarySignal(first_family, first_score, second_family, second_score, score)


def attention_level(final_score: int) -> str:
    if final_score >= 75:
        return CRITICAL
    if final_score >= 55:
        return WARNING
    if final_score >= 40:
        return NOTICE
    return IGNORE


def final_attention_score(base_attention_score: int, dev_modifier: int) -> int:
    return base_attention_score + dev_modifier


def combine_direction(directions: list[str]) -> str:
    if MIXED in directions:
        return MIXED
    meaningful = {direction for direction in directions if direction in {POSITIVE, NEGATIVE}}
    if POSITIVE in meaningful and NEGATIVE in meaningful:
        return MIXED
    if POSITIVE in meaningful:
        return POSITIVE
    if NEGATIVE in meaningful:
        return NEGATIVE
    return NEUTRAL
