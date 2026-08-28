from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import (
    AttentionAlertState,
    AttentionAssessment,
    IntelligenceEvent,
    PriceSnapshot,
    TokenIntelligenceSnapshot,
    TokenWatchState,
    TopHolderPeak,
    TopHolderSnapshot,
)
from app.services import attention_scoring as scoring
from app.services.attention_engine_service import AttentionEngineService, WatchedToken, fingerprint_event
from app.services.gmgn_client import GmgnHolder, GmgnMarketSignal, GmgnTrackTrade
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
TOKEN_2 = "0x39dbed3a2bd333467115de45665cc57f813c4571"


class FakeAttentionGmgn:
    def __init__(
        self,
        trades: list[GmgnTrackTrade] | None = None,
        signals: list[GmgnMarketSignal] | None = None,
    ) -> None:
        self.trades = trades or []
        self.signals = signals or []
        self.track_calls = 0
        self.signal_calls = 0

    async def get_track_feed(self, feed_type: str, *, chain: str, limit: int = 50):
        self.track_calls += 1
        return self.trades

    async def get_market_signals(self, chain: str, *, groups):
        self.signal_calls += 1
        return self.signals


@pytest.fixture
def ctx(tmp_path):
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
    return app_settings, session_factory, sent, notify, wallet_id


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


def add_watched(session_factory, wallet_id: int, token: str = TOKEN, usd_value: str = "50", symbol: str = "WINK") -> None:
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                active=True,
                started_at=now - timedelta(hours=2),
                last_seen_at=now,
            )
        )
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                price_usd=Decimal("0.01"),
                balance=Decimal("5000"),
                usd_value=Decimal(usd_value),
                observed_at=now,
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=now,
            )
        )


def end_active_watch(session_factory, wallet_id: int, token: str = TOKEN, ended_at=None) -> None:
    ended_at = ended_at or utc_now()
    with session_scope(session_factory) as session:
        state = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token,
                TokenWatchState.active.is_(True),
            )
        )
        assert state is not None
        state.active = False
        state.ended_at = ended_at


def start_watch_session(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    started_at=None,
) -> None:
    started_at = started_at or utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                active=True,
                started_at=started_at,
                last_seen_at=started_at,
            )
        )


def add_price_snapshot(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    price: str = "0.01",
    usd_value: str = "50",
    observed_at=None,
) -> None:
    observed_at = observed_at or utc_now()
    with session_scope(session_factory) as session:
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                price_usd=Decimal(price),
                balance=Decimal("5000"),
                usd_value=Decimal(usd_value),
                observed_at=observed_at,
            )
        )


def add_token_intel(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    holders: int = 500,
    liquidity: str = "100000",
    market_cap: str = "1000000",
    observed_at=None,
) -> None:
    observed_at = observed_at or utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                market_cap_usd=Decimal(market_cap),
                liquidity_usd=Decimal(liquidity),
                holder_count=holders,
                observed_at=observed_at,
            )
        )


def trade(
    token: str = TOKEN,
    wallet: str = "0x2222222222222222222222222222222222222222",
    side: str = "sell",
    usd: str = "6000",
    open_or_close: int | None = 1,
    timestamp: int | None = None,
) -> GmgnTrackTrade:
    return GmgnTrackTrade(
        chain="robinhood",
        wallet_address=wallet,
        token_address=token,
        symbol="WINK",
        side=side,
        token_amount=Decimal("1000"),
        usd_value=Decimal(usd),
        price_usd=Decimal("0.01"),
        timestamp=timestamp if timestamp is not None else int(utc_now().replace(tzinfo=UTC).timestamp()),
        tx_hash=f"0x{wallet[-4:]}{side}",
        open_or_close=open_or_close,
        wallet_tags=["smart_degen"],
        twitter_username=None,
        twitter_name=None,
        raw={},
    )


def signal(
    token: str = TOKEN,
    signal_type: int = 12,
    event_id: str = "sig-1",
    trigger_at: int | None = None,
) -> GmgnMarketSignal:
    return GmgnMarketSignal(
        chain="robinhood",
        event_id=event_id,
        token_address=token,
        signal_type=signal_type,
        trigger_at=trigger_at if trigger_at is not None else int(utc_now().replace(tzinfo=UTC).timestamp()),
        trigger_market_cap_usd=Decimal("1000000"),
        market_cap_usd=Decimal("1000000"),
        liquidity_usd=Decimal("100000"),
        holder_count=500,
        raw={},
    )


def holder(address: str, balance: str | None, hold_percentage: str) -> GmgnHolder:
    return GmgnHolder(
        chain="robinhood",
        wallet_address=address,
        balance=Decimal(balance) if balance is not None else None,
        hold_percentage=Decimal(hold_percentage),
        usd_value=None,
        avg_cost_usd=None,
        realized_profit_usd=None,
        unrealized_profit_usd=None,
        buy_tx_count=None,
        sell_tx_count=None,
        start_holding_at=None,
        tags=[],
        maker_token_tags=[],
        twitter_name=None,
        twitter_username=None,
        raw={},
    )


@pytest.mark.asyncio
async def test_feed_only_processes_watched_positions(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    gmgn = FakeAttentionGmgn([trade(TOKEN), trade(TOKEN_2)])
    service = AttentionEngineService(session_factory, gmgn, app_settings, notify)

    result = await service.scan_smart_money_feed()

    assert result.events_created == 1
    with session_scope(session_factory) as session:
        events = list(session.scalars(select(IntelligenceEvent)))
        assert len(events) == 1
        assert events[0].token_address == TOKEN


@pytest.mark.asyncio
async def test_below_five_dollars_does_not_enter_attention(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="4.99")
    gmgn = FakeAttentionGmgn([trade(TOKEN)])
    service = AttentionEngineService(session_factory, gmgn, app_settings, notify)

    result = await service.scan_smart_money_feed()

    assert result.events_created == 0
    assert result.assessments_created == 0


@pytest.mark.asyncio
async def test_event_dedupe(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    same = trade(TOKEN)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn([same, same]), app_settings, notify)

    result = await service.scan_smart_money_feed()

    assert result.events_created == 1
    assert result.assessments_created == 1
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(IntelligenceEvent)))) == 1


@pytest.mark.asyncio
async def test_market_signal_does_not_score_smart_money_or_kol(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(signals=[signal(TOKEN, 12, "smart-signal"), signal(TOKEN, 20, "kol-signal")]),
        app_settings,
        notify,
    )

    result = await service.scan_market_signals()

    assert result.events_created == 2
    assert result.assessments_created == 1
    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 0
        assert assessment.kol_score == 0


@pytest.mark.asyncio
async def test_smart_money_uses_same_direction_wallets_and_net_flow(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="buy", usd="1000"),
        trade(wallet="0x0000000000000000000000000000000000000002", side="buy", usd="1000"),
        trade(wallet="0x0000000000000000000000000000000000000003", side="sell", usd="1000"),
        trade(wallet="0x0000000000000000000000000000000000000004", side="sell", usd="1000"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_smart_money_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 0


@pytest.mark.asyncio
async def test_smart_money_scores_net_buy_flow_not_gross_buy_volume(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="buy", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000002", side="buy", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000003", side="buy", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000004", side="sell", usd="2750"),
        trade(wallet="0x0000000000000000000000000000000000000005", side="sell", usd="2750"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_smart_money_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 10
        assert assessment.direction == scoring.POSITIVE


@pytest.mark.asyncio
async def test_smart_money_scores_net_sell_flow_not_gross_sell_volume(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="sell", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000002", side="sell", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000003", side="sell", usd="2000"),
        trade(wallet="0x0000000000000000000000000000000000000004", side="buy", usd="2750"),
        trade(wallet="0x0000000000000000000000000000000000000005", side="buy", usd="2750"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_smart_money_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 10
        assert assessment.direction == scoring.NEGATIVE


@pytest.mark.asyncio
async def test_smart_money_direction_prefers_net_flow(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        *[
            trade(wallet=f"0x00000000000000000000000000000000000000{i:02x}", side="buy", usd="1")
            for i in range(10)
        ],
        trade(wallet="0x0000000000000000000000000000000000000099", side="sell", usd="6000"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_smart_money_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 10
        assert assessment.direction == scoring.NEGATIVE


@pytest.mark.asyncio
async def test_kol_uses_same_direction_traders(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="buy", usd="100"),
        trade(wallet="0x0000000000000000000000000000000000000002", side="sell", usd="100"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_kol_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.kol_score == 18
        assert assessment.direction == scoring.MIXED


@pytest.mark.asyncio
async def test_buy_trade_never_gets_close_bonus(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="buy", usd="6000", open_or_close=1),
        trade(wallet="0x0000000000000000000000000000000000000002", side="buy", usd="6000", open_or_close=1),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_smart_money_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.smart_money_score == 10


@pytest.mark.asyncio
async def test_close_bonus_disabled_until_gmgn_semantics_confirmed(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="sell", usd="6000", open_or_close=1),
        trade(wallet="0x0000000000000000000000000000000000000002", side="sell", usd="6000", open_or_close=1),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    await service.scan_kol_feed()

    with session_scope(session_factory) as session:
        assessment = session.scalar(select(AttentionAssessment).order_by(AttentionAssessment.id.desc()))
        assert assessment.kol_score == 28


@pytest.mark.asyncio
async def test_feed_scan_assesses_each_affected_token_once(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    trades = [
        trade(wallet="0x0000000000000000000000000000000000000001", side="sell", usd="3000"),
        trade(wallet="0x0000000000000000000000000000000000000002", side="sell", usd="3000"),
    ]
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, notify)

    result = await service.scan_smart_money_feed()

    assert result.events_created == 2
    assert result.assessments_created == 1


def test_warning_cooldown_and_escalation(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        now = utc_now()
        assert service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.WARNING,
            60,
            scoring.NEGATIVE,
            {scoring.PRICE: 30},
            now,
        )
        session.add(
            AttentionAlertState(
                wallet_id=wallet_id,
                token_address=TOKEN,
                last_notified_at=now,
                last_attention_level=scoring.WARNING,
                last_final_score=60,
                last_direction=scoring.NEGATIVE,
                family_scores_json=json.dumps({scoring.PRICE: 30}),
            )
        )
        session.flush()
        assert not service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.WARNING,
            70,
            scoring.NEGATIVE,
            {scoring.PRICE: 30},
            now + timedelta(minutes=1),
        )
        assert service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.WARNING,
            75,
            scoring.NEGATIVE,
            {scoring.PRICE: 30},
            now + timedelta(minutes=1),
        )
        assert service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.CRITICAL,
            76,
            scoring.NEGATIVE,
            {scoring.PRICE: 30},
            now + timedelta(minutes=1),
        )


def test_repeated_critical_respects_cooldown(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        now = utc_now()
        session.add(
            AttentionAlertState(
                wallet_id=wallet_id,
                token_address=TOKEN,
                last_notified_at=now,
                last_attention_level=scoring.CRITICAL,
                last_final_score=80,
                last_direction=scoring.NEGATIVE,
                family_scores_json=json.dumps({scoring.PRICE: 40, scoring.SMART_MONEY: 25}),
            )
        )
        session.flush()
        assert not service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.CRITICAL,
            80,
            scoring.NEGATIVE,
            {scoring.PRICE: 40, scoring.SMART_MONEY: 25},
            now + timedelta(minutes=10),
        )
        assert service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.CRITICAL,
            80,
            scoring.NEGATIVE,
            {scoring.PRICE: 40, scoring.SMART_MONEY: 25},
            now + timedelta(minutes=61),
        )


def test_abnormality_uses_same_price_window(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(PriceSnapshot).delete()
        for minutes_ago in range(60, 0, -1):
            session.add(
                PriceSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="WINK",
                    price_usd=Decimal("1"),
                    balance=Decimal("50"),
                    usd_value=Decimal("50"),
                    observed_at=now - timedelta(minutes=minutes_ago),
                )
            )
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("2"),
                balance=Decimal("25"),
                usd_value=Decimal("50"),
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        abnormality = service._abnormality(session, wallet_id, TOKEN, Decimal("100"), 60)

    assert abnormality == 5


@pytest.mark.asyncio
async def test_holder_breadth_direction_positive_and_negative(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=now - timedelta(minutes=60),
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=1000,
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    positive = await service.assess_token(wallet_id, TOKEN)

    assert positive.holder_breadth_score == 20
    assert positive.direction == scoring.POSITIVE
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=5000,
                observed_at=now - timedelta(minutes=60),
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=2500,
                observed_at=now,
            )
        )

    negative = await service.assess_token(wallet_id, TOKEN)

    assert negative.holder_breadth_score == 20
    assert negative.direction == scoring.NEGATIVE


def test_token_snapshot_baseline_is_relative_to_latest_snapshot(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
        baseline = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("100000"),
            holder_count=500,
            observed_at=now - timedelta(minutes=80),
        )
        latest = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("100000"),
            holder_count=1000,
            observed_at=now - timedelta(minutes=20),
        )
        session.add_all([baseline, latest])
        session.flush()

        score, direction = service._holder_breadth(session, wallet_id, TOKEN, latest)

    assert score == 20
    assert direction == scoring.POSITIVE


@pytest.mark.asyncio
async def test_holder_family_direction_mixed_for_breadth_and_top_reduction(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=now - timedelta(minutes=60),
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=1000,
                observed_at=now,
            )
        )
        watched = service._watched_token(session, wallet_id, TOKEN)
    service._save_top_holder_snapshots(watched, [holder("0xholder1", "100", "0.05")])
    service._save_top_holder_snapshots(watched, [holder("0xholder1", "70", "0.04")])

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment.holder_breadth_score == 20
    assert assessment.top_holder_score == 25
    assert assessment.holder_family_score == 25
    assert assessment.direction == scoring.MIXED


def test_stagger_limits_due_tokens(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    app_settings = replace(app_settings, attention_token_snapshot_batch_size=1)
    tokens = [
        TOKEN,
        TOKEN_2,
        "0x0000000000000000000000000000000000000003",
        "0x0000000000000000000000000000000000000004",
        "0x0000000000000000000000000000000000000005",
    ]
    for index, token_address in enumerate(tokens):
        add_watched(session_factory, wallet_id, token_address, symbol=f"T{index}")
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    selected: list[str] = []
    for _ in tokens:
        due = service._due_watched_tokens(
            TokenIntelligenceSnapshot,
            app_settings.attention_token_snapshot_interval_seconds,
            app_settings.attention_token_snapshot_batch_size,
        )
        assert len(due) == 1
        selected.append(due[0].token_address)
        with session_scope(session_factory) as session:
            session.add(
                TokenIntelligenceSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=due[0].token_address,
                    symbol=due[0].symbol,
                    market_cap_usd=Decimal("1000000"),
                    liquidity_usd=Decimal("100000"),
                    holder_count=500,
                    observed_at=utc_now(),
                )
            )

    assert set(selected) == set(tokens)


def test_token_snapshot_due_dispatch_rotates_15_tokens_without_repeating_before_due(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    app_settings = replace(
        app_settings,
        attention_token_snapshot_due_seconds=600,
        attention_token_snapshot_dispatch_seconds=120,
        attention_token_snapshot_batch_size=3,
    )
    tokens = [f"0x{i:040x}" for i in range(1, 16)]
    for index, token_address in enumerate(tokens):
        add_watched(session_factory, wallet_id, token_address, symbol=f"T{index}")
    with session_scope(session_factory) as session:
        session.query(TokenIntelligenceSnapshot).delete()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    selected: list[str] = []
    for _ in range(5):
        due = service._due_watched_tokens(
            TokenIntelligenceSnapshot,
            app_settings.attention_token_snapshot_due_seconds,
            app_settings.attention_token_snapshot_batch_size,
        )
        assert len(due) == 3
        assert not set(selected).intersection({token.token_address for token in due})
        selected.extend(token.token_address for token in due)
        with session_scope(session_factory) as session:
            for token in due:
                session.add(
                    TokenIntelligenceSnapshot(
                        wallet_id=wallet_id,
                        chain="robinhood",
                        token_address=token.token_address,
                        symbol=token.symbol,
                        market_cap_usd=Decimal("1000000"),
                        liquidity_usd=Decimal("100000"),
                        holder_count=500,
                        observed_at=utc_now(),
                    )
                )

    assert set(selected) == set(tokens)
    assert (
        service._due_watched_tokens(
            TokenIntelligenceSnapshot,
            app_settings.attention_token_snapshot_due_seconds,
            app_settings.attention_token_snapshot_batch_size,
        )
        == []
    )


def test_top_holder_due_dispatch_rotates_15_tokens_without_repeating_before_due(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    app_settings = replace(
        app_settings,
        attention_top_holder_due_seconds=900,
        attention_top_holder_dispatch_seconds=60,
        attention_top_holder_batch_size=1,
    )
    tokens = [f"0x{i:040x}" for i in range(1, 16)]
    for index, token_address in enumerate(tokens):
        add_watched(session_factory, wallet_id, token_address, symbol=f"T{index}")
    with session_scope(session_factory) as session:
        session.query(TopHolderSnapshot).delete()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    selected: list[str] = []
    for _ in range(15):
        due = service._due_watched_tokens(
            TopHolderSnapshot,
            app_settings.attention_top_holder_due_seconds,
            app_settings.attention_top_holder_batch_size,
        )
        assert len(due) == 1
        assert due[0].token_address not in selected
        selected.append(due[0].token_address)
        with session_scope(session_factory) as session:
            session.add(
                TopHolderSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=due[0].token_address,
                    symbol=due[0].symbol,
                    holder_address=f"0xholder{len(selected)}",
                    balance=Decimal("100"),
                    hold_percentage=Decimal("0.01"),
                    usd_value=None,
                    dropped_out_top20=False,
                    observed_at=utc_now(),
                )
            )

    assert set(selected) == set(tokens)
    assert (
        service._due_watched_tokens(
            TopHolderSnapshot,
            app_settings.attention_top_holder_due_seconds,
            app_settings.attention_top_holder_batch_size,
        )
        == []
    )


def test_top_holder_peak_is_bound_to_watch_session(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    old_started_at = utc_now() - timedelta(days=2)
    with session_scope(session_factory) as session:
        session.add(
            TopHolderPeak(
                wallet_id=wallet_id,
                token_address=TOKEN,
                holder_address="0xholder1",
                watch_started_at=old_started_at,
                peak_balance=Decimal("1000"),
                peak_hold_percentage=Decimal("0.05"),
            )
        )
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "100", "0.005")])

    with session_scope(session_factory) as session:
        peaks = list(
            session.scalars(
                select(TopHolderPeak).where(
                    TopHolderPeak.wallet_id == wallet_id,
                    TopHolderPeak.token_address == TOKEN,
                    TopHolderPeak.holder_address == "0xholder1",
                )
            )
        )
        assert len(peaks) == 2
        assert service._top_holder_score(session, wallet_id, TOKEN) == 0


def test_top_holder_peak_percentage_updates_independently(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "100", "0.02")])
    service._save_top_holder_snapshots(watched, [holder("0xholder1", "90", "0.05")])

    with session_scope(session_factory) as session:
        peak = session.scalar(
            select(TopHolderPeak).where(
                TopHolderPeak.wallet_id == wallet_id,
                TopHolderPeak.token_address == TOKEN,
                TopHolderPeak.holder_address == "0xholder1",
                TopHolderPeak.watch_started_at == watched.watch_started_at,
            )
        )
        assert peak.peak_balance == Decimal("100.000000000000000000")
        assert peak.peak_hold_percentage == Decimal("0.0500000000")


def test_dropped_out_top20_holder_is_preserved_as_evidence_not_close(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "1000", "0.05")])
    service._save_top_holder_snapshots(watched, [holder("0xholder2", "900", "0.02")])

    with session_scope(session_factory) as session:
        dropped = list(
            session.scalars(
                select(TopHolderSnapshot).where(
                    TopHolderSnapshot.wallet_id == wallet_id,
                    TopHolderSnapshot.token_address == TOKEN,
                    TopHolderSnapshot.holder_address == "0xholder1",
                    TopHolderSnapshot.dropped_out_top20.is_(True),
                )
            )
        )
        assert len(dropped) == 1
        assert service._top_holder_score(session, wallet_id, TOKEN) == 0


def test_none_balance_top_holder_does_not_score_or_cluster(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "100", "0.05")])
    service._save_top_holder_snapshots(
        watched,
        [
            holder("0xholder1", None, "0.05"),
            holder("0xholder2", None, "0.05"),
            holder("0xholder3", None, "0.05"),
        ],
    )

    with session_scope(session_factory) as session:
        assert service._top_holder_score(session, wallet_id, TOKEN) == 0
        assert service._holder_cluster(session, wallet_id, TOKEN) == 0


def test_confirmed_zero_balance_top_holder_close_scores_40(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "100", "0.05")])
    service._save_top_holder_snapshots(watched, [holder("0xholder1", "0", "0.00")])

    with session_scope(session_factory) as session:
        assert service._top_holder_score(session, wallet_id, TOKEN) == 40


@pytest.mark.asyncio
async def test_dropped_out_top20_enters_attention_evidence(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        watched = service._watched_token(session, wallet_id, TOKEN)

    service._save_top_holder_snapshots(watched, [holder("0xholder1", "1000", "0.05")])
    service._save_top_holder_snapshots(watched, [holder("0xholder2", "900", "0.02")])

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.top_holder_score != 40
    assert evidence["dropped_out_top20_count"] == 1
    assert evidence["dropped_out_top20"][0]["holder_address"] == "0xholder1"
    assert evidence["dropped_out_top20"][0]["peak_hold_percentage"] == "0.0500000000"


def test_new_session_does_not_read_old_price_latest_or_baseline(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.query(TokenWatchState).delete()
        session.query(PriceSnapshot).delete()
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=False,
                started_at=now - timedelta(minutes=20),
                last_seen_at=now - timedelta(minutes=3),
                ended_at=now - timedelta(minutes=3),
            )
        )
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1"),
                balance=Decimal("50"),
                usd_value=Decimal("50"),
                observed_at=now - timedelta(minutes=5),
            )
        )
        new_start = now - timedelta(minutes=2)
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=new_start,
                last_seen_at=new_start,
            )
        )
        session.flush()

        assert service._latest_price_snapshot(session, wallet_id, TOKEN, new_start) is None
        assert service._watched_token(session, wallet_id, TOKEN).usd_value is None

        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("2"),
                balance=Decimal("25"),
                usd_value=Decimal("50"),
                observed_at=now,
            )
        )
        latest_intel = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("100000"),
            holder_count=500,
            observed_at=now,
        )
        session.add(latest_intel)
        session.flush()

        price_score, _, _, _ = service._price_family(session, wallet_id, TOKEN, latest_intel, new_start)

    assert price_score == 0


def test_new_session_does_not_read_old_token_intelligence_or_baselines(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    new_start = now - timedelta(minutes=20)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.query(TokenWatchState).delete()
        session.query(TokenIntelligenceSnapshot).delete()
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=new_start,
                last_seen_at=new_start,
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=now - timedelta(hours=1),
            )
        )
        session.flush()
        assert service._latest_token_snapshot(session, wallet_id, TOKEN, new_start) is None

        latest = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("50000"),
            holder_count=1000,
            observed_at=now,
        )
        session.add(latest)
        session.flush()
        holder_score, _ = service._holder_breadth(session, wallet_id, TOKEN, latest, new_start)
        liquidity_score, _ = service._liquidity_family(session, wallet_id, TOKEN, latest, new_start)

    assert holder_score == 0
    assert liquidity_score == 0


def test_new_session_does_not_read_old_top_holder_snapshots(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    now = utc_now()
    with session_scope(session_factory) as session:
        old_state = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        old_started = old_state.started_at
    service._save_top_holder_snapshots(
        WatchedToken(wallet_id, 100, "robinhood", TOKEN, "WINK", Decimal("50"), old_started),
        [holder("0xholder1", "1000", "0.05")],
    )
    end_active_watch(session_factory, wallet_id, TOKEN, now - timedelta(minutes=1))
    start_watch_session(session_factory, wallet_id, started_at=now)
    add_price_snapshot(session_factory, wallet_id, observed_at=now + timedelta(seconds=1))

    with session_scope(session_factory) as session:
        assert service._top_holder_score(session, wallet_id, TOKEN) == 0
        assert service._holder_cluster(session, wallet_id, TOKEN) == 0


def test_new_session_snapshot_due_ignores_old_session_last_snapshot(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    new_start = now - timedelta(seconds=10)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.query(TokenWatchState).delete()
        session.query(PriceSnapshot).delete()
        session.query(TokenIntelligenceSnapshot).delete()
        session.query(TopHolderSnapshot).delete()
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=new_start,
                last_seen_at=new_start,
            )
        )
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("0.01"),
                balance=Decimal("5000"),
                usd_value=Decimal("50"),
                observed_at=now,
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=new_start - timedelta(seconds=20),
            )
        )
        session.add(
            TopHolderSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                holder_address="0xholder1",
                balance=Decimal("100"),
                hold_percentage=Decimal("0.01"),
                usd_value=None,
                dropped_out_top20=False,
                observed_at=new_start - timedelta(seconds=20),
            )
        )

    token_due = service._due_watched_tokens(TokenIntelligenceSnapshot, 600, 1)
    holder_due = service._due_watched_tokens(TopHolderSnapshot, 900, 1)

    assert [item.token_address for item in token_due] == [TOKEN]
    assert [item.token_address for item in holder_due] == [TOKEN]


def test_old_smart_and_kol_events_do_not_score_new_session(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    new_start = now - timedelta(minutes=2)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    start_watch_session(session_factory, wallet_id, started_at=new_start)
    add_price_snapshot(session_factory, wallet_id, observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    with session_scope(session_factory) as session:
        for family, source in ((scoring.SMART_MONEY, "gmgn_smartmoney"), (scoring.KOL, "gmgn_kol")):
            for index in range(3):
                session.add(
                    IntelligenceEvent(
                        wallet_id=wallet_id,
                        chain="robinhood",
                        token_address=TOKEN,
                        symbol="WINK",
                        family=family,
                        event_type="old_trade",
                        direction=scoring.NEGATIVE,
                        severity_score=0,
                        source=source,
                        source_event_id=f"{source}-{index}",
                        event_fingerprint=f"{source}-{index}",
                        event_at=new_start - timedelta(minutes=1),
                        detected_at=now,
                        payload_json=json.dumps({"wallet": f"0x{index}", "usd_value": "6000"}),
                    )
                )
        smart_score, _ = service._feed_family(session, wallet_id, TOKEN, scoring.SMART_MONEY)
        kol_score, _ = service._feed_family(session, wallet_id, TOKEN, scoring.KOL)

    assert smart_score == 0
    assert kol_score == 0


@pytest.mark.asyncio
async def test_pre_watch_feed_and_signal_are_not_inserted_for_current_session(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    new_start = now - timedelta(minutes=1)
    start_watch_session(session_factory, wallet_id, started_at=new_start)
    add_price_snapshot(session_factory, wallet_id, observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    old_ts = int((new_start - timedelta(seconds=30)).replace(tzinfo=UTC).timestamp())
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(
            trades=[trade(TOKEN, wallet="0x0000000000000000000000000000000000000001", timestamp=old_ts)],
            signals=[signal(TOKEN, 12, "old-signal", old_ts)],
        ),
        app_settings,
        notify,
    )

    feed_result = await service.scan_smart_money_feed()
    signal_result = await service.scan_market_signals()

    assert feed_result.events_created == 0
    assert feed_result.assessments_created == 0
    assert signal_result.events_created == 0
    assert signal_result.assessments_created == 0


@pytest.mark.asyncio
async def test_old_market_signal_not_in_new_session_evidence(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    new_start = now - timedelta(minutes=1)
    start_watch_session(session_factory, wallet_id, started_at=new_start)
    add_price_snapshot(session_factory, wallet_id, observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    with session_scope(session_factory) as session:
        session.add(
            IntelligenceEvent(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                family=scoring.PRICE,
                event_type="market_signal",
                direction=scoring.POSITIVE,
                severity_score=0,
                source="gmgn_market_signal",
                source_event_id="old-sig",
                event_fingerprint="old-sig",
                event_at=new_start - timedelta(seconds=30),
                detected_at=now,
                payload_json=json.dumps({"signal_type": 6}),
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert evidence["recent_events"] == []


def test_latest_attention_for_symbol_only_returns_current_session(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    old_start = now - timedelta(hours=2)
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=False,
                started_at=old_start,
                last_seen_at=now - timedelta(minutes=10),
                ended_at=now - timedelta(minutes=10),
            )
        )
        session.add(
            AttentionAssessment(
                wallet_id=wallet_id,
                token_address=TOKEN,
                symbol="WINK",
                assessed_at=now - timedelta(minutes=30),
                final_attention_score=80,
                attention_level=scoring.CRITICAL,
                direction=scoring.NEGATIVE,
            )
        )
        new_start = now - timedelta(minutes=1)
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=new_start,
                last_seen_at=new_start,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assert service.latest_assessment_for_symbol(1, "WINK") == []

    with session_scope(session_factory) as session:
        session.add(
            AttentionAssessment(
                wallet_id=wallet_id,
                token_address=TOKEN,
                symbol="WINK",
                assessed_at=now,
                final_attention_score=35,
                attention_level=scoring.IGNORE,
                direction=scoring.NEUTRAL,
            )
        )

    latest = service.latest_assessment_for_symbol(1, "WINK")
    assert len(latest) == 1
    assert latest[0].final_attention_score == 35


@pytest.mark.asyncio
async def test_attention_notify_failure_for_one_token_does_not_stop_other_token(monkeypatch, ctx) -> None:
    monkeypatch.setattr(
        "app.services.attention_engine_service.ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS",
        (0, 0, 0),
    )
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="600")
    add_watched(session_factory, wallet_id, TOKEN_2, usd_value="600", symbol="PONS")
    trades = []
    for index in range(5):
        trades.append(
            trade(
                token=TOKEN,
                wallet=f"0x00000000000000000000000000000000000000{index + 1:02x}",
                side="buy",
                usd="1200",
            )
        )
        trades.append(
            trade(
                token=TOKEN_2,
                wallet=f"0x00000000000000000000000000000000000001{index + 1:02x}",
                side="buy",
                usd="1200",
            )
        )

    async def flaky_notify(chat_id: int, text: str) -> None:
        if "PONS" in text:
            raise RuntimeError("telegram_down_for_pons")
        sent.append((chat_id, text))

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(trades), app_settings, flaky_notify)

    result = await service.scan_smart_money_feed()

    assert result.events_created == 10
    assert result.assessments_created == 2
    assert result.notifications_sent == 1
    assert result.errors == ["telegram_down_for_pons"]
    assert len(sent) == 1
    assert "WINK" in sent[0][1]
    with session_scope(session_factory) as session:
        wink_state = session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN))
        pons_state = session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN_2))
        assert wink_state is not None
        assert pons_state is None


@pytest.mark.asyncio
async def test_attention_notification_retries_and_marks_after_success(monkeypatch, ctx) -> None:
    monkeypatch.setattr(
        "app.services.attention_engine_service.ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS",
        (0, 0, 0),
    )
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    attempts = 0

    async def eventually_success(chat_id: int, text: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("temporary_telegram_error")
        sent.append((chat_id, text))

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, eventually_success)
    assessment = await service.simulate_assessment(
        wallet_id,
        TOKEN,
        symbol="WINK",
        family_scores={scoring.PRICE: 40},
        usd_value=Decimal("600"),
        direction=scoring.NEGATIVE,
    )

    assert await service._notify_assessment(assessment) is True

    assert attempts == 3
    assert len(sent) == 1
    with session_scope(session_factory) as session:
        state = session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN))
        assert state is not None
        assert state.last_attention_level == scoring.WARNING


@pytest.mark.asyncio
async def test_attention_notification_three_failures_do_not_mark_and_allow_later_retry(monkeypatch, ctx) -> None:
    monkeypatch.setattr(
        "app.services.attention_engine_service.ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS",
        (0, 0, 0),
    )
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)

    async def always_fail(chat_id: int, text: str) -> None:
        raise RuntimeError("telegram_still_down")

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, always_fail)
    assessment = await service.simulate_assessment(
        wallet_id,
        TOKEN,
        symbol="WINK",
        family_scores={scoring.PRICE: 40},
        usd_value=Decimal("600"),
        direction=scoring.NEGATIVE,
    )

    with pytest.raises(RuntimeError, match="telegram_still_down"):
        await service._notify_assessment(assessment)

    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)) is None
        assert service._should_notify(
            session,
            wallet_id,
            TOKEN,
            scoring.CRITICAL,
            80,
            scoring.NEGATIVE,
            {scoring.PRICE: 40},
            utc_now(),
        )


@pytest.mark.asyncio
async def test_stale_session_assessment_is_not_sent_or_marked(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    assessment = await service.simulate_assessment(
        wallet_id,
        TOKEN,
        symbol="WINK",
        family_scores={scoring.PRICE: 40},
        usd_value=Decimal("600"),
        direction=scoring.NEGATIVE,
    )
    with session_scope(session_factory) as session:
        state = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN,
                TokenWatchState.active.is_(True),
            )
        )
        state.active = False
        state.ended_at = assessment.assessed_at
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=assessment.assessed_at + timedelta(seconds=1),
                last_seen_at=assessment.assessed_at + timedelta(seconds=1),
            )
        )

    assert await service._notify_assessment(assessment) is False

    assert sent == []
    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)) is None


@pytest.mark.asyncio
async def test_current_session_assessment_sends_and_marks(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    assessment = await service.simulate_assessment(
        wallet_id,
        TOKEN,
        symbol="WINK",
        family_scores={scoring.PRICE: 40},
        usd_value=Decimal("600"),
        direction=scoring.NEGATIVE,
    )

    assert await service._notify_assessment(assessment) is True

    assert len(sent) == 1
    with session_scope(session_factory) as session:
        state = session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN))
        assert state is not None
        assert state.last_direction == scoring.NEGATIVE


@pytest.mark.asyncio
async def test_price_snapshot_update_without_price_score_does_not_assess(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="50", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.98", usd_value="49", observed_at=now)
    add_token_intel(
        session_factory,
        wallet_id,
        market_cap="32033",
        liquidity="14788",
        observed_at=now,
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is None
    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAssessment)) is None


@pytest.mark.asyncio
async def test_price_snapshot_update_with_price_score_assesses_and_notifies(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="600", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.65", usd_value="390", observed_at=now)
    add_token_intel(
        session_factory,
        wallet_id,
        market_cap="32033",
        liquidity="14788",
        observed_at=now,
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score > 0
    assert assessment.should_notify is True
    assert len(sent) == 1
    evidence = json.loads(assessment.evidence_json)
    assert evidence["assessment_trigger"] == "price_threshold"


@pytest.mark.asyncio
async def test_usd_threshold_crossing_allows_one_below_min_assessment(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="12", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(
        session_factory,
        wallet_id,
        market_cap="32033",
        liquidity="14788",
        observed_at=now,
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score == 40
    assert assessment.position_exposure_score == 0
    evidence = json.loads(assessment.evidence_json)
    assert evidence["assessment_trigger"] == "usd_threshold_crossing"


@pytest.mark.asyncio
async def test_below_min_without_crossing_does_not_price_trigger(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="2", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.5", usd_value="1", observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is None
    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAssessment)) is None


@pytest.mark.asyncio
async def test_below_min_first_session_snapshot_is_not_crossing(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=1))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is None


@pytest.mark.asyncio
async def test_usd_threshold_crossing_does_not_use_old_watch_session_previous_snapshot(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    old_start = now - timedelta(minutes=20)
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=False,
                started_at=old_start,
                last_seen_at=now - timedelta(minutes=2),
                ended_at=now - timedelta(minutes=2),
            )
        )
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="12", observed_at=now - timedelta(minutes=5))
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=1))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is None


@pytest.mark.asyncio
async def test_autism_incident_regression_crossing_price_and_smart_money_warns(ctx, monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.attention_engine_service.ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS",
        (0,),
    )
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, token=TOKEN, symbol="Autism", started_at=now - timedelta(minutes=10))
    add_price_snapshot(
        session_factory,
        wallet_id,
        token=TOKEN,
        symbol="Autism",
        price="0.000032253613",
        usd_value="12.85",
        observed_at=now - timedelta(minutes=5),
    )
    add_price_snapshot(
        session_factory,
        wallet_id,
        token=TOKEN,
        symbol="Autism",
        price="0.0000043569864",
        usd_value="1.74",
        observed_at=now,
    )
    add_token_intel(
        session_factory,
        wallet_id,
        token=TOKEN,
        symbol="Autism",
        market_cap="32033.17",
        liquidity="14788.223660170645",
        observed_at=now,
    )
    with session_scope(session_factory) as session:
        for index in range(8):
            session.add(
                IntelligenceEvent(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="Autism",
                    family=scoring.SMART_MONEY,
                    event_type="smartmoney_trade",
                    direction=scoring.NEGATIVE,
                    severity_score=0,
                    source="gmgn_smartmoney",
                    source_event_id=f"autism-sell-{index}",
                    event_fingerprint=f"autism-sell-{index}",
                    event_at=now - timedelta(seconds=20 + index),
                    detected_at=now,
                    payload_json=json.dumps(
                        {
                            "wallet": f"0x{index:040x}",
                            "side": "sell",
                            "usd_value": str(Decimal("2316.60") / Decimal("8")),
                        }
                    ),
                )
            )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score == 40
    assert assessment.smart_money_score == 25
    assert assessment.attention_level in {scoring.WARNING, scoring.CRITICAL}
    assert assessment.should_notify is True
    assert len(sent) == 1
    assert "Autism" in sent[0][1]


@pytest.mark.asyncio
async def test_crossing_does_not_force_warning_without_other_strength(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="12", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(
        session_factory,
        wallet_id,
        market_cap="32033",
        liquidity="14788",
        observed_at=now,
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score == 40
    assert assessment.attention_level == scoring.NOTICE
    assert assessment.should_notify is False
    assert sent == []


@pytest.mark.asyncio
async def test_crossing_is_one_time_while_value_stays_below_min(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="12", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(session_factory, wallet_id, market_cap="32033", liquidity="14788", observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    first = await service.handle_price_snapshot_update(wallet_id, TOKEN)
    add_price_snapshot(session_factory, wallet_id, price="0.18", usd_value="1.8", observed_at=now + timedelta(minutes=1))
    second = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert first is not None
    assert second is None
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(AttentionAssessment)))) == 1


@pytest.mark.asyncio
async def test_recovery_above_min_restores_normal_price_trigger(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", usd_value="12", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="0.2", usd_value="2", observed_at=now)
    add_token_intel(session_factory, wallet_id, market_cap="32033", liquidity="14788", observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    crossing = await service.handle_price_snapshot_update(wallet_id, TOKEN)
    add_price_snapshot(session_factory, wallet_id, price="0.6", usd_value="6", observed_at=now + timedelta(minutes=5))
    recovered = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert crossing is not None
    assert recovered is not None
    assert recovered.price_score > 0
    evidence = json.loads(recovered.evidence_json)
    assert evidence["assessment_trigger"] == "price_threshold"


def test_fingerprint_is_stable() -> None:
    assert fingerprint_event("a", "b", 1) == fingerprint_event("a", "b", 1)
    assert fingerprint_event("a", "b", 1) != fingerprint_event("a", "b", 2)
