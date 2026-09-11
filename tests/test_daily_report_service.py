from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.bot.handlers import daily_report
from app.bot.keyboards import BTN_DAILY_REPORT, main_menu_keyboard
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import Position, PositionTransaction, PriceSnapshot, Token, WalletTokenBalance
from app.services.daily_report_service import (
    BUY,
    SELL,
    DailyReportService,
    format_daily_report,
)
from app.services.transaction_parser import is_quote_token
from app.services.wallet_service import WalletService

WALLET = "0x1111111111111111111111111111111111111111"
TOKEN = "0x3333333333333333333333333333333333333333"
TOKEN_2 = "0x4444444444444444444444444444444444444444"


def make_ctx(tmp_path, *, now: datetime | None = None):
    engine = make_engine(f"sqlite:///{tmp_path}/daily_report.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        wallet_id = wallet.id
    service = DailyReportService(
        session_factory,
        report_timezone="Asia/Shanghai",
        price_tolerance_minutes=120,
        now_fn=lambda: now or datetime(2026, 9, 7, 4, 0, tzinfo=UTC).replace(tzinfo=None),
    )
    return session_factory, wallet_id, service


def add_position(session, wallet_id: int, token_address: str = TOKEN, symbol: str = "SIRIUS") -> Position:
    token = session.scalar(
        select(Token).where(Token.chain == "robinhood", Token.contract_address == token_address)
    )
    if not token:
        token = Token(
            chain="robinhood",
            contract_address=token_address,
            symbol=symbol,
            name=symbol,
            decimals=18,
        )
        session.add(token)
        session.flush()
    position = Position(
        wallet_id=wallet_id,
        token_id=token.id,
        status="OPEN",
        token_amount=Decimal("0"),
        last_balance=Decimal("0"),
        opened_at=datetime(2026, 9, 5, 1, 0),
    )
    session.add(position)
    session.flush()
    return position


def add_balance(
    session,
    wallet_id: int,
    token_address: str = TOKEN,
    symbol: str = "SIRIUS",
    amount: str = "10",
) -> None:
    session.add(
        WalletTokenBalance(
            wallet_id=wallet_id,
            chain="robinhood",
            asset_key=token_address,
            contract_address=token_address,
            symbol=symbol,
            name=symbol,
            decimals=18,
            token_amount=Decimal(amount),
            usd_value=None,
            last_checked_at=datetime(2026, 9, 7, 3, 55),
        )
    )


def add_tx(
    session,
    position: Position,
    tx_hash: str,
    tx_type: str,
    timestamp: datetime,
    token_amount: str,
    *,
    quote_token: str | None = "USDG",
    quote_amount: str | None = None,
    usd_value: str | None = None,
    gas_fee: str | None = "0.1",
) -> None:
    session.add(
        PositionTransaction(
            position_id=position.id,
            tx_hash=tx_hash,
            tx_type=tx_type,
            token_amount=Decimal(token_amount),
            quote_token=quote_token,
            quote_amount=Decimal(quote_amount) if quote_amount is not None else None,
            usd_value=Decimal(usd_value) if usd_value is not None else None,
            gas_fee=Decimal(gas_fee) if gas_fee is not None else None,
            tx_timestamp=timestamp,
        )
    )


def add_price(
    session,
    wallet_id: int,
    observed_at: datetime,
    price: str,
    *,
    token_address: str = TOKEN,
    symbol: str = "SIRIUS",
    quality_status: str = "VALID",
) -> None:
    session.add(
        PriceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=token_address,
            symbol=symbol,
            price_usd=Decimal(price),
            balance=Decimal("0"),
            usd_value=None,
            observed_at=observed_at,
            quality_status=quality_status,
            source_provider="gmgn",
            source_endpoint="/v1/user/wallet_holdings",
            source_field="item.price_usd",
        )
    )


def yesterday_bounds(service: DailyReportService):
    window = service.yesterday_window(service.now_fn(), service._zone)
    return window.start_utc, window.end_utc


def test_daily_report_timezone_uses_report_timezone_boundaries(tmp_path) -> None:
    _, _, service = make_ctx(tmp_path)

    start, end = yesterday_bounds(service)

    assert start == datetime(2026, 9, 5, 16, 0)
    assert end == datetime(2026, 9, 6, 16, 0)


def test_daily_report_buy_sell_aggregation_and_pnl(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="12")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50", gas_fee="0.5")
        add_tx(session, position, "0xsell", SELL, start + timedelta(hours=2), "2", quote_amount="30", gas_fee="0.3")
        add_tx(session, position, "0xafter", BUY, end + timedelta(hours=1), "2", quote_amount="20")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    report = service.build_yesterday_report([wallet_id])
    token = report.tokens[0]

    assert token.buy_usd == Decimal("50")
    assert token.sell_usd == Decimal("30")
    assert token.net_trade_usd == Decimal("20")
    assert token.buy_count == 1
    assert token.sell_count == 1
    assert token.token_bought_amount == Decimal("5")
    assert token.token_sold_amount == Decimal("2")
    assert token.net_token_change_from_trades == Decimal("3")
    assert token.end.balance == Decimal("10")
    assert token.start.balance == Decimal("7")
    assert token.gross_daily_pnl == Decimal("30")
    assert token.attributed_gas_usd == Decimal("0.8")
    assert token.net_daily_pnl == Decimal("29.2")


def test_daily_report_net_seller(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="8")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "1", quote_amount="10")
        add_tx(session, position, "0xsell", SELL, start + timedelta(hours=2), "4", quote_amount="48")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    report = service.build_yesterday_report([wallet_id])

    assert report.tokens[0].net_trade_usd == Decimal("-38")
    assert "净卖出｜-$38.0" in format_daily_report(report)


def test_quote_asset_does_not_become_report_token(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, _ = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        token = Token(chain="robinhood", contract_address="native", symbol="ETH", name="ETH", decimals=18)
        session.add(token)
        session.flush()
        position = Position(wallet_id=wallet_id, token_id=token.id, status="OPEN")
        session.add(position)
        session.flush()
        add_tx(session, position, "0xeth", BUY, start + timedelta(hours=1), "1", quote_token="USDG", quote_amount="100")

    report = service.build_yesterday_report([wallet_id])

    assert is_quote_token("ETH")
    assert report.tokens == []


def test_transfer_bridge_claim_airdrop_do_not_make_report_without_trade(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, _ = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_tx(session, position, "0xin", "TRANSFER_IN", start + timedelta(hours=1), "5", usd_value="50")
        add_tx(session, position, "0xbridge", "BRIDGE_OUT", start + timedelta(hours=2), "1", usd_value="10")
        add_tx(session, position, "0xclaim", "CLAIM", start + timedelta(hours=3), "1", usd_value="10")
        add_tx(session, position, "0xairdrop", "AIRDROP", start + timedelta(hours=4), "1", usd_value="10")

    report = service.build_yesterday_report([wallet_id])

    assert report.tokens == []


def test_external_transfer_adjusts_pnl_for_reportable_token(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="11")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")
        add_tx(session, position, "0xin", "TRANSFER_IN", start + timedelta(hours=2), "1", usd_value="9")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.external_in_value_usd == Decimal("9")
    assert token.gross_daily_pnl == Decimal("23")


def test_missing_historical_price_makes_pnl_unavailable_not_zero(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, _ = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="5")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.gross_daily_pnl is None
    assert token.net_daily_pnl is None
    assert "昨日净收益｜暂无法计算" in format_daily_report(service.build_yesterday_report([wallet_id]))


def test_price_outside_tolerance_is_unavailable(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, _ = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="5")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")
        add_price(session, wallet_id, start - timedelta(hours=3), "10")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.start.price_usd is None
    assert token.gross_daily_pnl is None


def test_outlier_price_snapshot_is_not_used_for_boundary_price(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="5")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")
        add_price(session, wallet_id, start, "999", quality_status="OUTLIER")
        add_price(session, wallet_id, start + timedelta(minutes=1), "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.start.price_usd == Decimal("10.000000000000000000")


def test_eth_quote_without_usd_value_keeps_trade_usd_unavailable(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="5")
        add_tx(
            session,
            position,
            "0xbuyeth",
            BUY,
            start + timedelta(hours=1),
            "5",
            quote_token="ETH",
            quote_amount="0.05",
        )
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.buy_usd is None
    assert token.gross_daily_pnl is None


def test_current_balance_can_fallback_to_position_last_balance(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        position.last_balance = Decimal("5")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.end.balance == Decimal("5")
    assert token.start.balance == Decimal("0")


def test_after_period_transfer_replay_reconstructs_end_balance(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="4")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50")
        add_tx(session, position, "0xouttoday", "TRANSFER_OUT", end + timedelta(hours=1), "1", usd_value="12")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.end.balance == Decimal("5")
    assert token.start.balance == Decimal("0")


def test_gas_missing_marks_net_pnl_unavailable(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="5")
        add_tx(session, position, "0xbuy", BUY, start + timedelta(hours=1), "5", quote_amount="50", gas_fee=None)
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    token = service.build_yesterday_report([wallet_id]).tokens[0]

    assert token.gas_data_complete is False
    assert token.gross_daily_pnl == Decimal("10")
    assert token.net_daily_pnl is None


def test_same_tx_gas_counted_once_for_token(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        position = add_position(session, wallet_id)
        add_balance(session, wallet_id, amount="3")
        add_tx(session, position, "0xswap", BUY, start + timedelta(hours=1), "5", quote_amount="50", gas_fee="0.7")
        add_price(session, wallet_id, start, "10")
        add_price(session, wallet_id, end, "12")

    report = service.build_yesterday_report([wallet_id])

    assert report.tokens[0].attributed_gas_usd == Decimal("0.7")
    assert report.total_gas_usd == Decimal("0.7")


def test_multi_target_tx_gas_unallocated_once(tmp_path) -> None:
    session_factory, wallet_id, service = make_ctx(tmp_path)
    start, end = yesterday_bounds(service)
    with session_scope(session_factory) as session:
        first = add_position(session, wallet_id, TOKEN, "AAA")
        second = add_position(session, wallet_id, TOKEN_2, "BBB")
        add_balance(session, wallet_id, TOKEN, "AAA", "5")
        add_balance(session, wallet_id, TOKEN_2, "BBB", "5")
        add_tx(session, first, "0xmulti", BUY, start + timedelta(hours=1), "5", quote_amount="50", gas_fee="1.2")
        add_tx(session, second, "0xmulti", BUY, start + timedelta(hours=1), "5", quote_amount="50", gas_fee="1.2")
        add_price(session, wallet_id, start, "10", token_address=TOKEN, symbol="AAA")
        add_price(session, wallet_id, end, "12", token_address=TOKEN, symbol="AAA")
        add_price(session, wallet_id, start, "10", token_address=TOKEN_2, symbol="BBB")
        add_price(session, wallet_id, end, "12", token_address=TOKEN_2, symbol="BBB")

    report = service.build_yesterday_report([wallet_id])

    assert report.unallocated_gas_usd == Decimal("1.2")
    assert report.total_gas_usd == Decimal("1.2")


def test_no_trades_report_message(tmp_path) -> None:
    _, wallet_id, service = make_ctx(tmp_path)

    text = format_daily_report(service.build_yesterday_report([wallet_id]))

    assert "昨日没有检测到买入或卖出操作。" in text


def test_main_menu_contains_daily_report_button() -> None:
    keyboard = main_menu_keyboard()

    assert BTN_DAILY_REPORT == "📊 查看昨日日报（待）"
    assert any(button.text == BTN_DAILY_REPORT for row in keyboard.keyboard for button in row)


@pytest.mark.asyncio
async def test_daily_report_handler_replies_without_sending_telegram_push(tmp_path) -> None:
    session_factory, _, _ = make_ctx(tmp_path)
    message = SimpleNamespace(replies=[])

    async def reply_text(text, reply_markup=None):
        message.replies.append((text, reply_markup))

    message.reply_text = reply_text
    update = SimpleNamespace(effective_user=SimpleNamespace(id=1), message=message)
    context = SimpleNamespace(
        application=SimpleNamespace(
            bot_data={
                "settings": SimpleNamespace(
                    report_timezone="Asia/Shanghai",
                    daily_report_price_tolerance_minutes=120,
                ),
                "session_factory": session_factory,
            }
        )
    )

    await daily_report(update, context)

    assert len(message.replies) == 1
    assert message.replies[0][0] == "📊 昨日日报功能开发中，暂未正式上线。"


def test_daily_report_business_code_remains_available(tmp_path) -> None:
    _, _, service = make_ctx(tmp_path)

    report = service.build_yesterday_report([])

    assert format_daily_report(report).startswith("📊 昨日日报")
