from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from app.services.gmgn_client import parse_holding
from app.services.holding_market_cap import build_holding_market_context
from app.utils.time import utc_now


def holding(
    *,
    price: str | None = "0.000075",
    total_supply: str | None = "1000000000",
    balance: str = "2000000",
    usd_value: str | None = "150",
    unrealized_profit: str | None = "50",
):
    item = {
        "balance": balance,
        "usd_value": usd_value,
        "history_total_buys": 1,
        "token": {
            "token_address": "0x39dbed3a2bd333467115de45665cc57f813c4571",
            "symbol": "WORMBRAIN",
        },
    }
    if price is not None:
        item["token"]["price"] = price
    if total_supply is not None:
        item["token"]["total_supply"] = total_supply
    if unrealized_profit is not None:
        item["unrealized_profit"] = unrealized_profit
    return parse_holding("robinhood", item)


def test_parse_holding_reads_token_total_supply() -> None:
    parsed = holding(total_supply="123456789")

    assert parsed.total_supply == Decimal("123456789")


def test_holding_market_context_current_and_entry_market_cap() -> None:
    now = utc_now()
    context = build_holding_market_context(
        wallet_id=1,
        token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
        holding=holding(),
        observed_at=now,
        watch_started_at=now - timedelta(minutes=5),
    )

    assert context is not None
    assert context.current_market_cap_usd == Decimal("75000.000000")
    assert context.effective_avg_cost_usd == Decimal("0.00005")
    assert context.entry_market_cap_usd == Decimal("50000.00000")


def test_holding_market_context_negative_unrealized_pnl() -> None:
    context = build_holding_market_context(
        wallet_id=1,
        token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
        holding=holding(usd_value="75", unrealized_profit="-25"),
        observed_at=utc_now(),
        watch_started_at=utc_now(),
    )

    assert context is not None
    assert context.entry_market_cap_usd == Decimal("50000.00000")


def test_holding_market_context_missing_entry_inputs() -> None:
    no_balance = build_holding_market_context(
        wallet_id=1,
        token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
        holding=holding(balance="0"),
        observed_at=utc_now(),
        watch_started_at=utc_now(),
    )
    no_unrealized = build_holding_market_context(
        wallet_id=1,
        token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
        holding=holding(unrealized_profit=None),
        observed_at=utc_now(),
        watch_started_at=utc_now(),
    )

    assert no_balance is not None
    assert no_balance.entry_market_cap_usd is None
    assert no_unrealized is not None
    assert no_unrealized.entry_market_cap_usd is None


def test_holding_market_context_missing_supply_or_price_is_unavailable() -> None:
    assert (
        build_holding_market_context(
            wallet_id=1,
            token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
            holding=holding(total_supply=None),
            observed_at=utc_now(),
            watch_started_at=utc_now(),
        )
        is None
    )
    assert (
        build_holding_market_context(
            wallet_id=1,
            token_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
            holding=holding(price=None),
            observed_at=utc_now(),
            watch_started_at=utc_now(),
        )
        is None
    )
