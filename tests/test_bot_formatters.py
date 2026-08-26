from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from app.bot.formatters import (
    format_holding_pnl_pct,
    format_holding_quantity,
    format_holding_usd,
)
from app.bot.messages import balances_message, gmgn_holdings_message
from app.services.gmgn_client import parse_holding


def test_format_holding_quantity_plain_one_decimal() -> None:
    assert format_holding_quantity(Decimal("164.263")) == "164.3"
    assert format_holding_quantity(Decimal("74931.057")) == "74,931.1"


def test_format_holding_quantity_k_suffix() -> None:
    assert format_holding_quantity(Decimal("249045")) == "249.0k"
    assert format_holding_quantity(Decimal("13432423")) == "13,432.4k"


def test_format_holding_usd_one_decimal() -> None:
    assert format_holding_usd(Decimal("19.487")) == "$19.5"
    assert format_holding_usd(Decimal("1236.58")) == "$1,236.6"


def test_format_holding_pnl_pct_positive_negative_and_missing() -> None:
    assert format_holding_pnl_pct(Decimal("112.6"), Decimal("12.6")) == "+12.6%"
    assert format_holding_pnl_pct(Decimal("91.7"), Decimal("-8.3")) == "-8.3%"
    assert format_holding_pnl_pct(Decimal("19.5"), None) == "--"


def test_format_holding_pnl_pct_estimated_cost_must_be_positive() -> None:
    assert format_holding_pnl_pct(Decimal("10"), Decimal("10")) == "--"
    assert format_holding_pnl_pct(Decimal("10"), Decimal("12")) == "--"


def test_balances_message_uses_readable_holding_format() -> None:
    text = balances_message(
        [
            SimpleNamespace(
                symbol="PONS",
                contract_address="0x39dbed3a2bd333467115de45665cc57f813c4571",
                asset_key="0x39dbed3a2bd333467115de45665cc57f813c4571",
                token_amount=Decimal("164.26343560313"),
                usd_value=Decimal("19.39786911"),
            )
        ]
    )

    assert text == "\n".join(
        [
            "💼 当前持仓",
            "",
            "🟢 PONS",
            "数量：164.3",
            "金额：$19.4",
            "持仓盈亏：--",
        ]
    )


def gmgn_holding(
    *,
    symbol: str = "PONS",
    balance: str = "164.26343560313",
    usd_value: str = "19.39786911",
    unrealized_profit: str | None = "4.39786911",
    buys: int = 1,
    realized_profit: str | None = "999",
):
    item = {
        "balance": balance,
        "usd_value": usd_value,
        "history_total_buys": buys,
        "history_bought_cost": "15" if buys else "0",
        "history_total_sells": 0,
        "token": {
            "symbol": symbol,
            "token_address": "0x39dbed3a2bd333467115de45665cc57f813c4571",
            "decimals": 18,
        },
    }
    if unrealized_profit is not None:
        item["unrealized_profit"] = unrealized_profit
    if realized_profit is not None:
        item["realized_profit"] = realized_profit
    return parse_holding("robinhood", item)


def test_gmgn_holdings_message_hides_no_buy_airdrop() -> None:
    text = gmgn_holdings_message([gmgn_holding(symbol="$SPURDO", buys=0)], Decimal("5"))

    assert text == "当前没有检测到正在监控的持仓。"


def test_gmgn_holdings_message_shows_buy_position_with_unrealized_pnl() -> None:
    text = gmgn_holdings_message([gmgn_holding()], Decimal("5"))

    assert text == "\n".join(
        [
            "💼 当前持仓",
            "（已隐藏金额<$5代币）",
            "",
            "🟢 PONS",
            "数量：164.3",
            "金额：$19.4",
            "持仓盈亏：+29.3%",
        ]
    )


def test_gmgn_holdings_message_missing_unrealized_pnl_is_unknown() -> None:
    text = gmgn_holdings_message([gmgn_holding(unrealized_profit=None)], Decimal("5"))

    assert "持仓盈亏：--" in text


def test_gmgn_holdings_message_does_not_use_realized_profit_for_current_pnl() -> None:
    text = gmgn_holdings_message(
        [gmgn_holding(unrealized_profit=None, realized_profit="999")],
        Decimal("5"),
    )

    assert "持仓盈亏：--" in text


def test_gmgn_holdings_message_min_value_boundary() -> None:
    text = gmgn_holdings_message(
        [
            gmgn_holding(symbol="ABOVE", usd_value="5.01"),
            gmgn_holding(symbol="AT", usd_value="5.00"),
            gmgn_holding(symbol="BELOW", usd_value="4.99"),
            gmgn_holding(symbol="UNKNOWN", usd_value=None),
        ],
        Decimal("5"),
    )

    assert "🟢 ABOVE" in text
    assert "🟢 AT" in text
    assert "🟢 BELOW" not in text
    assert "🟢 UNKNOWN" not in text


def test_gmgn_holdings_message_threshold_title_uses_config_value() -> None:
    text = gmgn_holdings_message([gmgn_holding(usd_value="12")], Decimal("10"))

    assert "（已隐藏金额<$10代币）" in text


def test_gmgn_holdings_message_all_trading_positions_below_threshold() -> None:
    text = gmgn_holdings_message([gmgn_holding(usd_value="4.99")], Decimal("5"))

    assert text == "\n".join(
        [
            "💼 当前持仓",
            "（已隐藏金额<$5代币）",
            "",
            "当前没有达到关注金额的持仓。",
        ]
    )
