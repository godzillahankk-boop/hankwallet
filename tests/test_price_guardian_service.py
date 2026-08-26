from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import PriceAlertState, PriceSnapshot, TokenWatchState
from app.services.gmgn_client import GmgnPage, parse_holding
from app.services.price_guardian_service import (
    DOWN,
    UP,
    PriceGuardianService,
    format_price,
)
from app.services.wallet_service import WalletService
from app.utils.time import utc_now
from scripts.price_guardian_probe import build_test_alert_message

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
WALLET_2 = "0x1111111111111111111111111111111111111111"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
TOKEN_2 = "0x39dbed3a2bd333467115de45665cc57f813c4571"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


class FakeGmgnClient:
    def __init__(self, holdings_by_wallet: dict[str, list[dict]] | None = None, fail: bool = False) -> None:
        self.holdings_by_wallet = holdings_by_wallet or {}
        self.fail = fail
        self.calls = 0

    async def get_wallet_holdings_pages(self, query: dict, *, max_pages: int = 1):
        self.calls += 1
        if self.fail:
            raise RuntimeError("gmgn_down")
        wallet = query["wallet_address"]
        return [GmgnPage(items=self.holdings_by_wallet.get(wallet, []), next_cursor=None, raw={})]


@pytest.fixture
def service_ctx(tmp_path):
    app_settings = settings(tmp_path)
    engine = make_engine(app_settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    sent: list[tuple[int, str]] = []

    async def notify(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        wallet_id = wallet.id
        wallet_2 = WalletService(session).add_wallet(2, 200, WALLET_2, "robinhood")
        wallet_2_id = wallet_2.id

    return app_settings, session_factory, sent, wallet_id, wallet_2_id


def settings(tmp_path) -> Settings:
    return Settings(
        telegram_bot_token="test-token",
        database_url=f"sqlite:///{tmp_path}/wallet_agent_test.db",
        wallet_scan_interval_seconds=60,
        legacy_wallet_scan_enabled=False,
        manual_scan_cooldown_seconds=30,
        dust_threshold=Decimal("0.000001"),
        dust_clear_confirmation_scans=2,
        default_chain="robinhood",
        chain_api_base_url=None,
        chain_api_key=None,
        chain_rpc_url=None,
        chain_token_search_symbols=(),
        chain_request_timeout_seconds=20,
        token_transfer_lookback_limit=100,
        log_level="INFO",
        api_host="127.0.0.1",
        api_port=8000,
        gmgn_enabled=True,
        gmgn_api_base_url="https://openapi.gmgn.ai",
        gmgn_api_key="test-key",
        gmgn_private_key_path=None,
        gmgn_request_timeout_seconds=20,
        price_guardian_enabled=True,
        price_scan_interval_seconds=60,
        price_monitor_min_usd_value=Decimal("5"),
        price_excluded_symbols=("USDG", "USDC", "USDT", "ETH", "WETH"),
        price_history_retention_hours=24,
        price_alert_5m_percent=Decimal("10"),
        price_alert_15m_percent=Decimal("20"),
        price_alert_60m_percent=Decimal("30"),
        price_alert_escalation_step_percent=Decimal("10"),
        price_alert_reset_ratio=Decimal("0.5"),
        price_holdings_max_pages=10,
        price_wallet_concurrency=3,
    )


def make_service(session_factory, app_settings, gmgn_client, sent):
    async def notify(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    return PriceGuardianService(session_factory, gmgn_client, app_settings, notify)


def make_failing_service(session_factory, app_settings, gmgn_client):
    async def notify(chat_id: int, text: str) -> None:
        raise RuntimeError("telegram_down")

    return PriceGuardianService(session_factory, gmgn_client, app_settings, notify)


def holding(
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    price: str | None = "0.0002",
    balance: str = "249045.742548186937738568",
    usd_value: str | None = "57.08",
    buys: int = 1,
    unrealized_profit: str | None = "7.08",
) -> dict:
    item = {
        "balance": balance,
        "history_total_buys": buys,
        "history_total_sells": 0,
        "history_bought_cost": "50" if buys else "0",
        "history_sold_income": "0",
        "token": {
            "token_address": token,
            "symbol": symbol,
            "name": symbol,
            "decimals": 18,
        },
    }
    if price is not None:
        item["token"]["price"] = price
    if usd_value is not None:
        item["usd_value"] = usd_value
    if unrealized_profit is not None:
        item["unrealized_profit"] = unrealized_profit
    return item


def add_baseline(session_factory, wallet_id: int, token: str, price: str, minutes: int, symbol: str = "WINK") -> None:
    with session_scope(session_factory) as session:
        observed_at = utc_now() - timedelta(minutes=minutes)
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token,
                TokenWatchState.active.is_(True),
            )
        )
        if watch is None:
            session.add(
                TokenWatchState(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=token,
                    symbol=symbol,
                    active=True,
                    started_at=observed_at - timedelta(minutes=1),
                    last_seen_at=observed_at,
                )
            )
        elif watch.started_at > observed_at:
            watch.started_at = observed_at - timedelta(minutes=1)
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                price_usd=Decimal(price),
                balance=Decimal("100"),
                usd_value=Decimal("10"),
                observed_at=observed_at,
            )
        )


def add_baseline_at(
    session_factory,
    wallet_id: int,
    token: str,
    price: str,
    observed_at,
    symbol: str = "WINK",
) -> None:
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token,
                TokenWatchState.active.is_(True),
            )
        )
        if watch is None:
            session.add(
                TokenWatchState(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=token,
                    symbol=symbol,
                    active=True,
                    started_at=observed_at - timedelta(minutes=1),
                    last_seen_at=observed_at,
                )
            )
        elif watch.started_at > observed_at:
            watch.started_at = observed_at - timedelta(minutes=1)
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                price_usd=Decimal(price),
                balance=Decimal("100"),
                usd_value=Decimal("10"),
                observed_at=observed_at,
            )
        )


def count_snapshots(session_factory, token: str = TOKEN) -> int:
    with session_scope(session_factory) as session:
        return len(
            list(
                session.scalars(
                    select(PriceSnapshot).where(PriceSnapshot.token_address == token)
                )
            )
        )


@pytest.mark.asyncio
async def test_holding_auto_enters_monitor_and_new_holding_records_snapshot(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient({WALLET: [holding()]})
    service = make_service(session_factory, app_settings, gmgn, sent)

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 1
    assert result.snapshots_saved == 1
    assert count_snapshots(session_factory) == 1
    with session_scope(session_factory) as session:
        watches = list(session.scalars(select(TokenWatchState)))
    assert len(watches) == 1
    assert watches[0].active is True


@pytest.mark.asyncio
async def test_existing_holding_updates_watch_without_duplicate(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding()]}), sent)

    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.00021")]})
    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        watches = list(session.scalars(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN)))
        assert len(watches) == 1
        assert watches[0].active is True


def test_excluded_symbol_and_low_value_and_none_price_are_filtered(service_ctx) -> None:
    app_settings, session_factory, sent, _, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient(), sent)

    candidates = service.classify_holdings(
        [
            parse_holding("robinhood", holding(token=USDG, symbol="USDG", usd_value="59")),
            parse_holding("robinhood", holding(token=TOKEN_2, symbol="XDOG", usd_value="0.13")),
            parse_holding("robinhood", holding(token=TOKEN, symbol="WINK", price=None)),
        ]
    )

    assert [candidate.reason for candidate in candidates] == [
        "SKIPPED_EXCLUDED_SYMBOL",
        "SKIPPED_BELOW_MIN_VALUE",
        "SKIPPED_NO_PRICE",
    ]


def test_no_buy_history_holding_is_not_monitored(service_ctx) -> None:
    app_settings, session_factory, sent, _, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient(), sent)

    candidate = service.classify_holding(
        parse_holding("robinhood", holding(symbol="$SPURDO", buys=0, usd_value="2.34"))
    )

    assert candidate.monitored is False
    assert candidate.reason == "SKIPPED_NO_BUY_HISTORY"


def test_buy_history_holding_is_monitored(service_ctx) -> None:
    app_settings, session_factory, sent, _, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient(), sent)

    candidate = service.classify_holding(parse_holding("robinhood", holding(buys=1)))

    assert candidate.monitored is True
    assert candidate.reason == "MONITORED"


@pytest.mark.asyncio
async def test_zero_price_saves_no_snapshot_or_alert(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="0", usd_value="10")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.snapshots_saved == 0
    assert sent == []


@pytest.mark.asyncio
async def test_no_buy_history_airdrop_does_not_trigger_price_alert(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="80", buys=0, usd_value="20")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    assert result.alerts_sent == 0
    assert sent == []


@pytest.mark.asyncio
async def test_5m_up_and_down_alerts(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)

    await service.scan_wallet(wallet_id)

    assert len(sent) == 1
    assert "快速上涨" in sent[0][1]
    sent.clear()
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="89")]})

    await service.scan_wallet(wallet_id)

    assert len(sent) == 1
    assert "快速下跌" in sent[0][1]


@pytest.mark.asyncio
async def test_insufficient_history_does_not_alert(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)

    result = await service.scan_wallet(wallet_id)

    assert result.snapshots_saved == 1
    assert sent == []


@pytest.mark.asyncio
async def test_duplicate_suppression_escalation_reset_and_realert(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)

    await service.scan_wallet(wallet_id)
    assert len(sent) == 1
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="114")]})
    await service.scan_wallet(wallet_id)
    assert len(sent) == 1
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="122")]})
    await service.scan_wallet(wallet_id)
    assert len(sent) == 2
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="104")]})
    await service.scan_wallet(wallet_id)
    with session_scope(session_factory) as session:
        state = session.scalar(
            select(PriceAlertState).where(
                PriceAlertState.wallet_id == wallet_id,
                PriceAlertState.token_address == TOKEN,
                PriceAlertState.window_minutes == 5,
                PriceAlertState.direction == UP,
            )
        )
        assert state is not None
        assert state.active is False
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="111")]})
    await service.scan_wallet(wallet_id)
    assert len(sent) == 3


@pytest.mark.asyncio
async def test_telegram_success_marks_state_and_failure_does_not(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)

    success_result = await service.scan_wallet(wallet_id)

    assert success_result.alerts_sent == 1
    with session_scope(session_factory) as session:
        state = session.scalar(
            select(PriceAlertState).where(
                PriceAlertState.wallet_id == wallet_id,
                PriceAlertState.token_address == TOKEN,
                PriceAlertState.window_minutes == 5,
                PriceAlertState.direction == UP,
            )
        )
        assert state is not None
        assert state.active is True
        assert state.last_alert_change_pct == Decimal("11.0000")


@pytest.mark.asyncio
async def test_telegram_failure_does_not_mark_success_and_allows_retry(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_failing_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="111")]}),
    )

    failed_result = await service.scan_wallet(wallet_id)

    assert failed_result.alerts_sent == 0
    assert failed_result.errors
    with session_scope(session_factory) as session:
        state = session.scalar(select(PriceAlertState).where(PriceAlertState.token_address == TOKEN))
        assert state is None

    retry_service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="111")]}),
        sent,
    )
    retry_result = await retry_service.scan_wallet(wallet_id)

    assert retry_result.alerts_sent == 1
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_multi_window_single_message_and_independent_token_messages(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    for minutes in (5, 15, 60):
        add_baseline(session_factory, wallet_id, TOKEN, "100", minutes)
    add_baseline(session_factory, wallet_id, TOKEN_2, "100", 5, "PONS")
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient(
            {
                WALLET: [
                    holding(price="135"),
                    holding(token=TOKEN_2, symbol="PONS", price="112", usd_value="20"),
                ]
            }
        ),
        sent,
    )

    await service.scan_wallet(wallet_id)

    assert len(sent) == 2
    assert "5分钟" in sent[0][1]
    assert "15分钟" in sent[0][1]
    assert "1小时" in sent[0][1]


@pytest.mark.asyncio
async def test_gmgn_failure_does_not_send_false_alert_or_stop(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient(fail=True), sent)

    result = await service.scan_wallet(wallet_id)
    scheduler_result = await service.scan_active_wallets()

    assert result.errors
    assert scheduler_result.errors
    assert sent == []


@pytest.mark.asyncio
async def test_gmgn_failure_does_not_end_active_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding()]}), sent)
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient(fail=True)

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True


@pytest.mark.asyncio
async def test_token_removed_from_holdings_stops_price_alert(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: []}), sent)

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    assert sent == []
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is False


@pytest.mark.asyncio
async def test_holdings_success_and_token_disappears_ends_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding()]}), sent)
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: []})

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is False
        assert watch.ended_at is not None


@pytest.mark.asyncio
async def test_below_min_value_stops_monitor_but_keeps_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(usd_value="5.10")]}), sent)
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="4.90")]})

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True


@pytest.mark.asyncio
async def test_min_value_boundary_controls_price_monitor(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(usd_value="4.99")]}),
        sent,
    )

    below_result = await service.scan_wallet(wallet_id)

    assert below_result.monitored_tokens == 0
    assert below_result.snapshots_saved == 0
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="5.00")]})

    at_threshold_result = await service.scan_wallet(wallet_id)

    assert at_threshold_result.monitored_tokens == 1
    assert at_threshold_result.snapshots_saved == 1


@pytest.mark.asyncio
async def test_value_recovery_reenters_price_monitor_without_new_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(usd_value="5.10")]}), sent)
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="4.90")]})
    below_result = await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="5.00")]})

    recovered_result = await service.scan_wallet(wallet_id)

    assert below_result.monitored_tokens == 0
    assert recovered_result.monitored_tokens == 1
    with session_scope(session_factory) as session:
        watches = list(session.scalars(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN)))
        assert len(watches) == 1
        assert watches[0].active is True


@pytest.mark.asyncio
async def test_rebuy_creates_new_watch_resets_alert_and_ignores_old_snapshot(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    await service.scan_wallet(wallet_id)
    assert len(sent) == 1
    service.gmgn_client = FakeGmgnClient({WALLET: []})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="122")]})

    result = await service.scan_wallet(wallet_id)

    assert result.alerts_sent == 0
    with session_scope(session_factory) as session:
        watches = list(
            session.scalars(
                select(TokenWatchState)
                .where(TokenWatchState.token_address == TOKEN)
                .order_by(TokenWatchState.id)
            )
        )
        assert len(watches) == 2
        assert watches[-1].active is True
        state = session.scalar(select(PriceAlertState).where(PriceAlertState.token_address == TOKEN))
        assert state is not None
        assert state.active is False
        assert state.last_alert_change_pct is None


@pytest.mark.asyncio
async def test_baseline_tolerance_accepts_near_target_and_rejects_far_snapshot(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    now = utc_now()
    add_baseline_at(session_factory, wallet_id, TOKEN, "100", now - timedelta(minutes=5, seconds=74))
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(price="111")]}), sent)

    await service.scan_wallet(wallet_id)
    assert len(sent) == 1

    sent.clear()
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    # Use a second token to avoid the state created above and verify far baseline is skipped.
    far_service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(token=TOKEN_2, symbol="PONS", price="111", usd_value="20")]}),
        sent,
    )
    add_baseline_at(session_factory, wallet_id, TOKEN_2, "100", now - timedelta(minutes=7))

    await far_service.scan_wallet(wallet_id)
    assert sent == []


@pytest.mark.asyncio
async def test_price_guardian_does_not_call_blockscout_or_rpc(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient({WALLET: [holding()]})
    service = make_service(session_factory, app_settings, gmgn, sent)

    await service.scan_wallet(wallet_id)

    assert gmgn.calls == 1


@pytest.mark.asyncio
async def test_multiple_wallets_send_to_correct_chat(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, wallet_2_id = service_ctx
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    add_baseline(session_factory, wallet_2_id, TOKEN_2, "100", 5, "PONS")
    service = make_service(
        session_factory,
        replace(app_settings, price_wallet_concurrency=1),
        FakeGmgnClient(
            {
                WALLET: [holding(price="111")],
                WALLET_2: [holding(token=TOKEN_2, symbol="PONS", price="89", usd_value="20")],
            }
        ),
        sent,
    )

    await service.scan_active_wallets()

    assert sorted(chat_id for chat_id, _ in sent) == [100, 200]


def test_small_price_formatting() -> None:
    assert format_price(Decimal("0.0000003712")) == "0.0000003712"


@pytest.mark.asyncio
async def test_test_telegram_message_does_not_modify_price_alert_state(service_ctx) -> None:
    app_settings, session_factory, sent, _, _ = service_ctx
    message = build_test_alert_message()

    assert "🧪 Price Guardian 测试提醒" in message
    assert "不代表真实行情异动" in message
    assert count_snapshots(session_factory) == 0
    with session_scope(session_factory) as session:
        assert list(session.scalars(select(PriceAlertState))) == []


@pytest.mark.asyncio
async def test_snapshot_cleanup(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient(), sent)
    with session_scope(session_factory) as session:
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1"),
                balance=Decimal("1"),
                usd_value=Decimal("1"),
                observed_at=utc_now() - timedelta(hours=25),
            )
        )

    deleted = await service.cleanup_old_snapshots()

    assert deleted == 1
    assert count_snapshots(session_factory) == 0
