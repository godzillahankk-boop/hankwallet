from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import AttentionAlertState, AttentionAssessment, PriceAlertState, PriceSnapshot, TokenWatchState
from app.services.attention_engine_service import AttentionEngineService
from app.services.gmgn_client import GmgnPage, parse_holding
from app.services.price_guardian_service import (
    DOWN,
    UP,
    PriceGuardianService,
    format_price,
)
from app.services.price_quality import (
    PRICE_QUALITY_OUTLIER,
    PRICE_QUALITY_PENDING,
    PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT,
    PRICE_QUALITY_REASON_REVERTED_NEXT_SNAPSHOT,
    PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP,
    PRICE_QUALITY_VALID,
    price_quality_jump_threshold,
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


class ConcurrentGmgnClient:
    def __init__(self, holdings_by_wallet: dict[str, list[dict]]) -> None:
        self.holdings_by_wallet = holdings_by_wallet
        self.calls = 0
        self.active_total = 0
        self.max_active_total = 0
        self.active_by_wallet: dict[str, int] = {}
        self.max_active_by_wallet: dict[str, int] = {}

    async def get_wallet_holdings_pages(self, query: dict, *, max_pages: int = 1):
        wallet = query["wallet_address"]
        self.calls += 1
        self.active_total += 1
        self.max_active_total = max(self.max_active_total, self.active_total)
        self.active_by_wallet[wallet] = self.active_by_wallet.get(wallet, 0) + 1
        self.max_active_by_wallet[wallet] = max(
            self.max_active_by_wallet.get(wallet, 0),
            self.active_by_wallet[wallet],
        )
        try:
            await asyncio.sleep(0.02)
            return [GmgnPage(items=self.holdings_by_wallet.get(wallet, []), next_cursor=None, raw={})]
        finally:
            self.active_by_wallet[wallet] -= 1
            self.active_total -= 1


class FakeAttentionGmgn:
    pass


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
        price_guardian_alerts_enabled=True,
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
        attention_engine_enabled=True,
        attention_smart_money_interval_seconds=60,
        attention_kol_interval_seconds=60,
        attention_market_signal_interval_seconds=120,
        attention_token_snapshot_interval_seconds=600,
        attention_top_holder_interval_seconds=900,
        attention_token_snapshot_batch_size=3,
        attention_top_holder_batch_size=1,
        attention_feed_window_minutes=15,
        attention_event_aggregation_minutes=5,
        attention_warning_cooldown_minutes=30,
        attention_critical_cooldown_minutes=60,
    )


def make_service(session_factory, app_settings, gmgn_client, sent):
    async def notify(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    return PriceGuardianService(session_factory, gmgn_client, app_settings, notify)


def make_failing_service(session_factory, app_settings, gmgn_client):
    async def notify(chat_id: int, text: str) -> None:
        raise RuntimeError("telegram_down")

    return PriceGuardianService(session_factory, gmgn_client, app_settings, notify)


def make_service_with_price_trigger(session_factory, app_settings, gmgn_client, sent, trigger):
    async def notify(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    return PriceGuardianService(
        session_factory,
        gmgn_client,
        app_settings,
        notify,
        price_attention_trigger=trigger,
    )


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


def cost_only_holding(
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    price: str | None = "0.0002",
    balance: str = "249045.742548186937738568",
    usd_value: str | None = "57.08",
    historical_bought_cost: str = "50",
) -> dict:
    item = {
        "balance": balance,
        "history_bought_cost": historical_bought_cost,
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


def price_snapshots(session_factory, token: str = TOKEN) -> list[PriceSnapshot]:
    with session_scope(session_factory) as session:
        return list(
            session.scalars(
                select(PriceSnapshot)
                .where(PriceSnapshot.token_address == token)
                .order_by(PriceSnapshot.observed_at.asc(), PriceSnapshot.id.asc())
            )
        )


def attention_assessments(session_factory, token: str = TOKEN) -> list[AttentionAssessment]:
    with session_scope(session_factory) as session:
        return list(
            session.scalars(
                select(AttentionAssessment)
                .where(AttentionAssessment.token_address == token)
                .order_by(AttentionAssessment.assessed_at.asc(), AttentionAssessment.id.asc())
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
async def test_scan_wallet_reports_canonical_trading_positions_without_extra_gmgn_fetch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient(
        {
            WALLET: [
                holding(symbol="ROBBIE"),
                holding(token=TOKEN_2, symbol="WOOD", usd_value="4.50"),
                holding(token="0x3333333333333333333333333333333333333333", symbol="UNKNOWN_VALUE", usd_value=None),
                holding(token=USDG, symbol="USDG", usd_value="100"),
                holding(token="0x2222222222222222222222222222222222222222", symbol="AIRDROP", buys=0, usd_value="40"),
            ]
        }
    )
    service = make_service(session_factory, app_settings, gmgn, sent)

    result = await service.scan_wallet(wallet_id)

    assert gmgn.calls == 1
    assert result.holdings_found == 5
    assert result.trading_positions_found == 3
    assert result.display_positions == 1
    assert result.below_threshold_positions == 1
    assert result.position_symbols == ["ROBBIE"]
    with session_scope(session_factory) as session:
        watches = list(session.scalars(select(TokenWatchState).order_by(TokenWatchState.symbol.asc())))
        snapshots = list(session.scalars(select(PriceSnapshot).order_by(PriceSnapshot.symbol.asc())))
    assert [watch.symbol for watch in watches] == ["ROBBIE", "UNKNOWN_VALUE", "WOOD"]
    assert [snapshot.symbol for snapshot in snapshots] == ["ROBBIE", "UNKNOWN_VALUE", "WOOD"]


@pytest.mark.asyncio
async def test_display_position_does_not_require_current_price(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(symbol="NOPRICE", price=None, usd_value="20")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.trading_positions_found == 1
    assert result.display_positions == 1
    assert result.position_symbols == ["NOPRICE"]
    assert result.monitored_tokens == 0
    assert result.snapshots_saved == 0
    assert count_snapshots(session_factory) == 0


@pytest.mark.asyncio
async def test_same_wallet_price_guardian_scans_share_wallet_lock(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = ConcurrentGmgnClient({WALLET: [holding()]})
    service = make_service(session_factory, app_settings, gmgn, sent)

    await asyncio.gather(service.scan_wallet(wallet_id), service.scan_wallet(wallet_id))

    assert gmgn.calls == 2
    assert gmgn.max_active_by_wallet[WALLET] == 1


@pytest.mark.asyncio
async def test_different_wallet_price_guardian_scans_can_overlap(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, wallet_2_id = service_ctx
    gmgn = ConcurrentGmgnClient({WALLET: [holding()], WALLET_2: [holding()]})
    service = make_service(session_factory, app_settings, gmgn, sent)

    await asyncio.gather(service.scan_wallet(wallet_id), service.scan_wallet(wallet_2_id))

    assert gmgn.calls == 2
    assert gmgn.max_active_total == 2


def test_price_quality_jump_threshold_clamps_effective_price_threshold() -> None:
    assert price_quality_jump_threshold(Decimal("5")) == Decimal("20")
    assert price_quality_jump_threshold(Decimal("15")) == Decimal("30")
    assert price_quality_jump_threshold(Decimal("30")) == Decimal("40")


@pytest.mark.asyncio
async def test_suspicious_single_step_price_jump_is_pending_and_does_not_trigger_attention(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    triggered: list[tuple[int, str]] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        triggered.append((trigger_wallet_id, token_address))

    gmgn = FakeGmgnClient({WALLET: [holding(price="0.230", usd_value="23")]})
    service = make_service_with_price_trigger(session_factory, app_settings, gmgn, sent, trigger)
    await service.scan_wallet(wallet_id)
    triggered.clear()

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.435", usd_value="43.5")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_PENDING,
    ]
    assert snapshots[-1].quality_reason == PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP
    assert triggered == []


@pytest.mark.asyncio
async def test_ai_bad_price_replay_marks_single_snapshot_outlier_and_avoids_reverse_fake_trigger(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    triggered: list[tuple[int, str]] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        triggered.append((trigger_wallet_id, token_address))

    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="0.23056119", usd_value="37.61")]}),
        sent,
        trigger,
    )
    await service.scan_wallet(wallet_id)
    triggered.clear()

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.43582597", usd_value="71.08")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.23464219", usd_value="38.27")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.23484438", usd_value="38.30")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_OUTLIER,
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert snapshots[1].quality_reason == PRICE_QUALITY_REASON_REVERTED_NEXT_SNAPSHOT
    assert len(triggered) == 2
    assert all(token == TOKEN for _, token in triggered)


@pytest.mark.asyncio
async def test_suspicious_price_jump_confirmed_by_next_snapshot_becomes_valid(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    triggered: list[tuple[int, str]] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        triggered.append((trigger_wallet_id, token_address))

    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="0.230", usd_value="23")]}),
        sent,
        trigger,
    )
    await service.scan_wallet(wallet_id)
    triggered.clear()

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.435", usd_value="43.5")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.440", usd_value="44")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert snapshots[1].quality_reason == PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
    assert snapshots[2].quality_reason == PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
    assert triggered == [(wallet_id, TOKEN)]


@pytest.mark.asyncio
async def test_confirmed_pending_below_five_preserves_usd_threshold_crossing(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    attention = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings)
    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="1.00", usd_value="6")]}),
        sent,
        attention.handle_price_snapshot_update,
    )
    await service.scan_wallet(wallet_id)

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.40", usd_value="2.4")]})
    await service.scan_wallet(wallet_id)
    assert attention_assessments(session_factory) == []

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.41", usd_value="2.46")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assessments = attention_assessments(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert snapshots[1].quality_reason == PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
    assert len(assessments) == 1
    evidence = json.loads(assessments[0].evidence_json)
    assert evidence["assessment_trigger"] == "usd_threshold_crossing"
    assert assessments[0].position_exposure_score == 0


@pytest.mark.asyncio
async def test_pending_reverts_to_above_min_does_not_create_crossing_assessment(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    attention = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings)
    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="1.00", usd_value="6")]}),
        sent,
        attention.handle_price_snapshot_update,
    )
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.40", usd_value="2.4")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="1.02", usd_value="6.1")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_OUTLIER,
        PRICE_QUALITY_VALID,
    ]
    assert attention_assessments(session_factory) == []


@pytest.mark.asyncio
async def test_confirmed_pending_when_already_below_five_does_not_create_crossing_assessment(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    attention = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings)
    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="1.00", usd_value="3")]}),
        sent,
        attention.handle_price_snapshot_update,
    )
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.40", usd_value="1.2")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.41", usd_value="1.23")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert snapshots[1].quality_reason == PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
    assert attention_assessments(session_factory) == []


@pytest.mark.asyncio
async def test_confirmed_pending_crossing_does_not_use_previous_watch_session(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    attention = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings)
    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="1.00", usd_value="6")]}),
        sent,
        attention.handle_price_snapshot_update,
    )
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.40", usd_value="2.4")]})
    await service.scan_wallet(wallet_id)
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        watch.active = False
        watch.ended_at = utc_now()

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.41", usd_value="2.46")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_PENDING,
        PRICE_QUALITY_VALID,
    ]
    assert attention_assessments(session_factory) == []


@pytest.mark.asyncio
async def test_confirmed_pending_rise_above_five_does_not_emit_crossing_flag(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    trigger_kwargs: list[dict] = []

    async def trigger(trigger_wallet_id: int, token_address: str, **kwargs) -> None:
        trigger_kwargs.append(kwargs)

    service = make_service_with_price_trigger(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="1.00", usd_value="3")]}),
        sent,
        trigger,
    )
    await service.scan_wallet(wallet_id)
    trigger_kwargs.clear()
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="2.70", usd_value="8")]})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="2.75", usd_value="8.2")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert trigger_kwargs == [{}]


@pytest.mark.asyncio
async def test_normal_price_move_stays_valid_and_source_metadata_is_saved(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="0.230", usd_value="23")]}),
        sent,
    )
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.240", usd_value="24")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_VALID,
    ]
    assert snapshots[-1].source_provider == "gmgn"
    assert snapshots[-1].source_endpoint == "/v1/user/wallet_holdings"
    assert snapshots[-1].source_field == "token.price"


@pytest.mark.asyncio
async def test_new_watch_session_does_not_confirm_old_pending_snapshot(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="0.230", usd_value="23")]}),
        sent,
    )
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.435", usd_value="43.5")]})
    await service.scan_wallet(wallet_id)
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        watch.active = False
        watch.ended_at = utc_now()

    service.gmgn_client = FakeGmgnClient({WALLET: [holding(price="0.440", usd_value="44")]})
    await service.scan_wallet(wallet_id)

    snapshots = price_snapshots(session_factory)
    assert [snapshot.quality_status for snapshot in snapshots] == [
        PRICE_QUALITY_VALID,
        PRICE_QUALITY_PENDING,
        PRICE_QUALITY_VALID,
    ]


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
async def test_airdrop_does_not_create_watch_state(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(buys=0, usd_value="20")]}),
        sent,
    )

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        assert list(session.scalars(select(TokenWatchState))) == []


@pytest.mark.asyncio
async def test_historical_cost_without_buy_count_creates_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [cost_only_holding(historical_bought_cost="50")]}),
        sent,
    )

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True


@pytest.mark.asyncio
async def test_below_five_real_buy_keeps_watch_but_not_monitor(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(buys=1, usd_value="2.00")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    assert result.snapshots_saved == 1
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True
        snapshot = session.scalar(select(PriceSnapshot).where(PriceSnapshot.token_address == TOKEN))
        assert snapshot is not None
        assert snapshot.usd_value == Decimal("2.000000")


@pytest.mark.asyncio
async def test_excluded_symbol_does_not_create_watch_state(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(token=USDG, symbol="USDG", buys=1, usd_value="59")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    assert result.snapshots_saved == 0
    with session_scope(session_factory) as session:
        assert list(session.scalars(select(TokenWatchState))) == []


@pytest.mark.asyncio
async def test_no_price_trading_position_keeps_watch_without_snapshot_or_attention(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price=None, buys=1, usd_value="20")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 0
    assert result.snapshots_saved == 0
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True
        assert list(session.scalars(select(PriceSnapshot))) == []


@pytest.mark.asyncio
async def test_price_attention_trigger_runs_after_snapshot_commit(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient({WALLET: [holding()]})
    observed: list[int] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        with session_scope(session_factory) as session:
            snapshots = list(
                session.scalars(
                    select(PriceSnapshot).where(
                        PriceSnapshot.wallet_id == trigger_wallet_id,
                        PriceSnapshot.token_address == token_address,
                    )
                )
            )
            observed.append(len(snapshots))

    service = make_service_with_price_trigger(session_factory, app_settings, gmgn, sent, trigger)

    result = await service.scan_wallet(wallet_id)

    assert result.snapshots_saved == 1
    assert observed == [1]


@pytest.mark.asyncio
async def test_scan_wallet_can_initialize_baseline_without_attention_trigger(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient({WALLET: [holding()]})
    triggered: list[tuple[int, str]] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        triggered.append((trigger_wallet_id, token_address))

    service = make_service_with_price_trigger(session_factory, app_settings, gmgn, sent, trigger)

    result = await service.scan_wallet(wallet_id, send_alerts=False, trigger_attention=False)

    assert result.snapshots_saved == 1
    assert triggered == []
    assert sent == []


@pytest.mark.asyncio
async def test_price_attention_trigger_failure_does_not_drop_snapshot_or_stop_other_tokens(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    gmgn = FakeGmgnClient(
        {
            WALLET: [
                holding(token=TOKEN, symbol="WINK"),
                holding(token=TOKEN_2, symbol="PONS"),
            ]
        }
    )
    called: list[str] = []

    async def trigger(trigger_wallet_id: int, token_address: str) -> None:
        called.append(token_address)
        if token_address == TOKEN:
            raise RuntimeError("attention_trigger_down")

    service = make_service_with_price_trigger(session_factory, app_settings, gmgn, sent, trigger)

    result = await service.scan_wallet(wallet_id)

    assert result.snapshots_saved == 2
    assert result.errors == ["attention_trigger_down"]
    assert called == sorted([TOKEN, TOKEN_2])
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(PriceSnapshot)))) == 2


@pytest.mark.asyncio
async def test_airdrop_then_real_buy_starts_watch_at_real_buy_scan(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(buys=0, usd_value="20")]}),
        sent,
    )
    await service.scan_wallet(wallet_id)
    with session_scope(session_factory) as session:
        assert list(session.scalars(select(TokenWatchState))) == []
    before_buy_scan = utc_now()
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(buys=1, usd_value="20")]})

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True
        assert watch.started_at >= before_buy_scan


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
async def test_alerts_disabled_still_saves_snapshot_and_watch_without_notifier_call(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    app_settings = replace(app_settings, price_guardian_alerts_enabled=False)
    add_baseline(session_factory, wallet_id, TOKEN, "100", 5)
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient({WALLET: [holding(price="111")]}),
        sent,
    )

    result = await service.scan_wallet(wallet_id)

    assert result.monitored_tokens == 1
    assert result.snapshots_saved == 1
    assert result.alerts_sent == 0
    assert sent == []
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        assert watch is not None
        assert watch.active is True
        assert session.scalar(select(PriceAlertState).where(PriceAlertState.token_address == TOKEN)) is None


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
    app_settings = replace(
        app_settings,
        price_alert_5m_percent=Decimal("5"),
        price_alert_15m_percent=Decimal("7"),
        price_alert_60m_percent=Decimal("9"),
    )
    for minutes in (5, 15, 60):
        add_baseline(session_factory, wallet_id, TOKEN, "100", minutes)
    add_baseline(session_factory, wallet_id, TOKEN_2, "100", 5, "PONS")
    service = make_service(
        session_factory,
        app_settings,
        FakeGmgnClient(
            {
                WALLET: [
                    holding(price="119"),
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
async def test_successful_zero_balance_holding_ends_watch(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding()]}), sent)
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(balance="0", buys=1, usd_value="0")]})

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
    assert below_result.snapshots_saved == 1
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="5.00")]})

    at_threshold_result = await service.scan_wallet(wallet_id)

    assert at_threshold_result.monitored_tokens == 1
    assert at_threshold_result.snapshots_saved == 1


@pytest.mark.asyncio
async def test_low_value_snapshots_keep_current_value_and_recover_attention_eligibility(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding(usd_value="8.00")]}), sent)
    first = await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="4.00")]})
    second = await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding(usd_value="6.00")]})
    third = await service.scan_wallet(wallet_id)

    assert first.monitored_tokens == 1
    assert second.monitored_tokens == 0
    assert third.monitored_tokens == 1
    with session_scope(session_factory) as session:
        snapshots = list(
            session.scalars(
                select(PriceSnapshot)
                .where(PriceSnapshot.token_address == TOKEN)
                .order_by(PriceSnapshot.id.asc())
            )
        )
        assert [snapshot.usd_value for snapshot in snapshots] == [
            Decimal("8.000000"),
            Decimal("4.000000"),
            Decimal("6.000000"),
        ]
        watches = list(session.scalars(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN)))
        assert len(watches) == 1
        assert watches[0].active is True


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
async def test_rebuy_resets_attention_alert_state(service_ctx) -> None:
    app_settings, session_factory, sent, wallet_id, _ = service_ctx
    service = make_service(session_factory, app_settings, FakeGmgnClient({WALLET: [holding()]}), sent)
    await service.scan_wallet(wallet_id)
    with session_scope(session_factory) as session:
        session.add(
            AttentionAlertState(
                wallet_id=wallet_id,
                token_address=TOKEN,
                last_attention_level="CRITICAL",
                last_final_score=82,
                last_direction="negative",
                family_scores_json="{}",
                last_notified_at=utc_now(),
            )
        )
    service.gmgn_client = FakeGmgnClient({WALLET: []})
    await service.scan_wallet(wallet_id)
    service.gmgn_client = FakeGmgnClient({WALLET: [holding()]})

    await service.scan_wallet(wallet_id)

    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)) is None


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
