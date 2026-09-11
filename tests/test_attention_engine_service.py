from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
    SocialEvent,
    SocialIdentity,
    SocialMemory,
    TokenIntelligenceSnapshot,
    TokenWatchState,
    TopHolderPeak,
    TopHolderSnapshot,
)
from app.services import attention_scoring as scoring
from app.services.attention_engine_service import (
    AttentionEngineService,
    FeedFamilyFact,
    MARKET_EXPANSION,
    MINORITY_DRIVEN,
    NO_CLEAR_CHANGE,
    SIGNAL_DIVERGENCE,
    STRUCTURAL_DETERIORATION,
    SUPPORT_EMERGING,
    WatchedToken,
    build_attention_copy_markup,
    build_position_intelligence,
    fingerprint_event,
    format_attention_alert,
    format_compact_usd,
    format_directional_trigger_title,
    short_address,
    _format_window,
    _feed_primary_display_direction,
)
from app.services.gmgn_client import GmgnHolder, GmgnMarketSignal, GmgnTokenOverview, GmgnTrackTrade
from app.services.price_quality import PRICE_QUALITY_OUTLIER, PRICE_QUALITY_PENDING, PRICE_QUALITY_VALID
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService
from app.services.social_memory_service import SocialMemoryService
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet
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


class FakeOverviewGmgn(FakeAttentionGmgn):
    def __init__(self, overview: GmgnTokenOverview) -> None:
        super().__init__()
        self.overview = overview
        self.overview_calls = 0

    async def get_token_overview(self, chain: str, token_address: str):
        self.overview_calls += 1
        return self.overview


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
    quality_status: str = "VALID",
    quality_reason: str | None = None,
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
                quality_status=quality_status,
                quality_reason=quality_reason,
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


def add_social_event(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    tweet_id: str,
    author_id: str | None = None,
    username: str = "kol",
    author_type: str = AUTHOR_KOL,
    posted_at=None,
    match_type: str = "direct_ca",
) -> None:
    posted = posted_at or utc_now()
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        session.add(
            SocialEvent(
                wallet_id=wallet_id,
                watch_state_id=watch.id,
                chain=watch.chain,
                token_address=token,
                symbol=symbol,
                provider="twitterapi_io",
                provider_event_id=tweet_id,
                author_id=author_id,
                author_username=username,
                author_type=author_type,
                posted_at=posted,
                received_at=posted + timedelta(seconds=2),
                ingestion_type="stream",
                post_type="original",
                text="$WINK",
                match_type=match_type,
                matched_value=token,
                tweet_url=f"https://x.com/{username}/status/{tweet_id}",
            )
        )


def add_social_memory(
    session_factory,
    *,
    token: str = TOKEN,
    symbol: str = "WINK",
    tweet_id: str,
    significance: str = "medium",
    event_time=None,
    author_type: str = AUTHOR_PROJECT_X,
    url: str | None = None,
) -> None:
    event_at = event_time or utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialMemory(
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                project_identity="project",
                project_key=f"robinhood:{token}",
                source_author_id="project-author",
                source_username="ProjectUser",
                source_author_type=author_type,
                provider="twitterapi_io",
                tweet_id=tweet_id,
                tweet_url=url or f"https://x.com/ProjectUser/status/{tweet_id}",
                event_time=event_at,
                category="development",
                summary="Project announced a meaningful update.",
                significance=significance,
                confidence="high",
                information_scope="project",
                triage_version="test",
            )
        )


def social_tweet(
    tweet_id: str,
    *,
    token: str = TOKEN,
    username: str = "kol",
    author_id: str | None = "author-1",
    text: str = "$WINK",
    created_at=None,
) -> NormalizedTweet:
    created = created_at or datetime.now(UTC)
    return NormalizedTweet(
        provider="twitterapi_io",
        tweet_id=tweet_id,
        author_id=author_id,
        author_username=username,
        author_name=username,
        author_followers=10000,
        text=text,
        created_at=created,
        detected_at=created + timedelta(seconds=2),
        is_reply=False,
        in_reply_to_id=None,
        in_reply_to_username=None,
        conversation_id=tweet_id,
        is_quote=False,
        quoted_tweet_id=None,
        quoted_tweet=None,
        is_retweet=False,
        retweeted_tweet_id=None,
        retweeted_tweet=None,
        like_count=1,
        retweet_count=0,
        reply_count=0,
        quote_count=0,
        view_count=100,
        token_matches=[TokenMatch("direct_ca", token.lower(), token)],
        raw={},
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


def add_intelligence_event(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    family: str = scoring.SMART_MONEY,
    source: str = "gmgn_smartmoney",
    direction: str = scoring.POSITIVE,
    wallet: str = "0x1111111111111111111111111111111111111111",
    usd: str | None = "1000",
    event_at=None,
) -> None:
    event_at = event_at or utc_now()
    payload = {"wallet": wallet}
    if usd is not None:
        payload["usd_value"] = usd
    with session_scope(session_factory) as session:
        session.add(
            IntelligenceEvent(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token,
                symbol="WINK",
                family=family,
                event_type="trade",
                direction=direction,
                severity_score=0,
                source=source,
                source_event_id=None,
                event_fingerprint=fingerprint_event(wallet_id, token, family, source, wallet, direction, usd, event_at),
                event_at=event_at,
                detected_at=event_at,
                payload_json=json.dumps(payload),
            )
        )


def add_top10_group(
    session_factory,
    wallet_id: int,
    *,
    token: str = TOKEN,
    observed_at,
    share_each: str,
    prefix: str,
) -> None:
    with session_scope(session_factory) as session:
        for index in range(10):
            session.add(
                TopHolderSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=token,
                    symbol="WINK",
                    holder_address=f"0x{prefix}{index}",
                    balance=Decimal("100"),
                    hold_percentage=Decimal(share_each),
                    usd_value=None,
                    dropped_out_top20=False,
                    observed_at=observed_at,
                )
            )


def notification_assessment(
    wallet_id: int,
    token: str = TOKEN,
    *,
    symbol: str = "WINK",
    final_score: int = 65,
    level: str = scoring.WARNING,
    direction: str = scoring.NEGATIVE,
    price_score: int = 40,
    holder_score: int = 0,
    smart_score: int = 0,
    kol_score: int = 0,
    liquidity_score: int = 0,
    assessed_at=None,
) -> AttentionAssessment:
    return AttentionAssessment(
        wallet_id=wallet_id,
        token_address=token,
        symbol=symbol,
        assessed_at=assessed_at or utc_now(),
        price_score=price_score,
        holder_breadth_score=0,
        top_holder_score=0,
        holder_cluster_score=0,
        holder_family_score=holder_score,
        smart_money_score=smart_score,
        kol_score=kol_score,
        liquidity_score=liquidity_score,
        primary_family=scoring.PRICE,
        primary_event_score=price_score,
        position_exposure_score=20,
        abnormality_score=5,
        secondary_family_1=None,
        secondary_family_1_score=0,
        secondary_family_2=None,
        secondary_family_2_score=0,
        secondary_signal_score=0,
        base_attention_score=final_score,
        dev_modifier=0,
        final_attention_score=final_score,
        attention_level=level,
        direction=direction,
        should_notify=True,
        evidence_json=json.dumps({"display_facts": {"price": {"window_minutes": 5, "change_pct": "30"}}}),
    )


def _test_feed_fact_dict(fact) -> dict[str, object]:
    return {
        "window_minutes": fact.window_minutes,
        "buy_wallets": fact.buy_wallets,
        "sell_wallets": fact.sell_wallets,
        "net_wallets": fact.net_directional_wallets,
        "buy_usd": str(fact.buy_usd) if fact.buy_usd is not None else None,
        "sell_usd": str(fact.sell_usd) if fact.sell_usd is not None else None,
        "net_usd": str(fact.net_usd) if fact.net_usd is not None else None,
        "usd_complete": fact.usd_complete,
    }


def position_scores(
    *,
    price: int = 0,
    holder: int = 0,
    smart: int = 0,
    kol: int = 0,
    liquidity: int = 0,
) -> dict[str, int]:
    return {
        "price_score": price,
        "holder_breadth_score": holder,
        "smart_money_score": smart,
        "kol_score": kol,
        "liquidity_score": liquidity,
    }


def position_facts(
    *,
    price: str | None = None,
    holder: str | None = None,
    top10: str | None = None,
    smart_wallets: int | None = None,
    smart_usd: str | None = None,
    kol_wallets: int | None = None,
    kol_usd: str | None = None,
    liquidity: str | None = None,
) -> dict[str, object]:
    facts: dict[str, object] = {}
    if price is not None:
        facts["price"] = {"window_minutes": 5, "change_pct": price}
    if holder is not None:
        facts["holder_count"] = {
            "window_minutes": 30,
            "baseline_count": 500,
            "current_count": 500 + int(Decimal(holder)),
            "delta_count": int(Decimal(holder)),
            "change_pct": holder,
        }
    if top10 is not None:
        facts["top10"] = {"window_minutes": 15, "baseline_share": "0.42", "current_share": "0.50", "change_pct": top10}
    if smart_wallets is not None:
        facts["smart_money"] = {
            "window_minutes": 15,
            "buy_wallets": max(smart_wallets, 0),
            "sell_wallets": abs(min(smart_wallets, 0)),
            "net_wallets": smart_wallets,
            "net_usd": smart_usd,
            "usd_complete": smart_usd is not None,
        }
    if kol_wallets is not None:
        facts["kol"] = {
            "window_minutes": 15,
            "buy_wallets": max(kol_wallets, 0),
            "sell_wallets": abs(min(kol_wallets, 0)),
            "net_wallets": kol_wallets,
            "net_usd": kol_usd,
            "usd_complete": kol_usd is not None,
        }
    if liquidity is not None:
        facts["liquidity"] = {"window_minutes": 60, "baseline_usd": "100000", "current_usd": "90000", "change_pct": liquidity}
    return facts


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
                observed_at=now - timedelta(minutes=30),
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
    positive_evidence = json.loads(positive.evidence_json)
    assert positive_evidence["primary_signal"] == "holder_breadth"
    assert positive_evidence["primary_direction"] == scoring.POSITIVE
    assert "持币人数增加｜Positive｜ATT" in format_attention_alert(positive)
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
                observed_at=now - timedelta(minutes=30),
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
    negative_evidence = json.loads(negative.evidence_json)
    assert negative_evidence["primary_signal"] == "holder_breadth"
    assert negative_evidence["primary_direction"] == scoring.NEGATIVE
    assert "持币人数减少｜Negative｜ATT" in format_attention_alert(negative)


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
            observed_at=now - timedelta(minutes=50),
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
                observed_at=now - timedelta(minutes=30),
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
    evidence = json.loads(assessment.evidence_json)
    assert evidence["primary_signal"] == "top_holder_reduction"
    assert evidence["primary_direction"] == scoring.NEGATIVE
    assert "重要大户减仓｜Mixed｜ATT" in format_attention_alert(assessment)
    assert "筹码集中" not in format_attention_alert(assessment)
    assert "筹码分散" not in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_holder_breadth_uses_30m_baseline_not_60m(ctx) -> None:
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
                holder_count=950,
                observed_at=now - timedelta(minutes=30),
            )
        )
        latest = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("100000"),
            holder_count=1000,
            observed_at=now,
        )
        session.add(latest)
        session.flush()
        service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
        score, direction = service._holder_breadth(session, wallet_id, TOKEN, latest)
        fact = service._holder_count_fact(session, wallet_id, TOKEN, latest)

    assert score == 0
    assert direction == scoring.NEUTRAL
    assert fact is not None
    assert fact.window_minutes == 30
    assert fact.baseline_count == 950


@pytest.mark.asyncio
async def test_holder_count_display_fact_omitted_without_30m_baseline(ctx) -> None:
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

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.holder_breadth_score == 0
    assert "holder_count" not in evidence["display_facts"]


@pytest.mark.parametrize(
    ("baseline", "current", "expected"),
    [
        (500, 542, "+8.4%"),
        (500, 450, "-10%"),
        (500, 500, "0%"),
    ],
)
def test_holder_count_display_fact_formats_percent(ctx, baseline: int, current: int, expected: str) -> None:
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
                holder_count=baseline,
                observed_at=now - timedelta(minutes=30),
            )
        )
        latest = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("100000"),
            holder_count=current,
            observed_at=now,
        )
        session.add(latest)
        session.flush()
        service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
        display_facts = service._display_facts(session, wallet_id, TOKEN, None, latest, 0, None, None, None)

    text = format_attention_alert(
        make_alert_assessment(evidence={"display_facts": {"holder_count": display_facts["holder_count"]}})
    )
    assert f"• 持仓人数｜30m {expected}" in text


def test_top10_display_fact_uses_aggregate_share_and_actual_window(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    now = utc_now()
    previous = now - timedelta(minutes=15)
    with session_scope(session_factory) as session:
        for index in range(10):
            session.add(
                TopHolderSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="WINK",
                    holder_address=f"0xold{index}",
                    balance=Decimal("100"),
                    hold_percentage=Decimal("0.042"),
                    usd_value=None,
                    dropped_out_top20=False,
                    observed_at=previous,
                )
            )
        for index in range(10):
            session.add(
                TopHolderSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="WINK",
                    holder_address=f"0xnew{index}",
                    balance=Decimal("100"),
                    hold_percentage=Decimal("0.0307"),
                    usd_value=None,
                    dropped_out_top20=False,
                    observed_at=now,
                )
            )
        session.add(
            TopHolderSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                holder_address="0xdropped",
                balance=None,
                hold_percentage=Decimal("0.99"),
                usd_value=None,
                dropped_out_top20=True,
                observed_at=now,
            )
        )
        session.flush()
        watch_started_at = service._watch_started_at(session, wallet_id, TOKEN)
        fact = service._top10_fact(session, wallet_id, TOKEN, watch_started_at)

    assert fact is not None
    assert fact["window_minutes"] == 15
    assert Decimal(str(fact["baseline_share"])) == Decimal("0.420")
    assert Decimal(str(fact["current_share"])) == Decimal("0.3070")
    assert Decimal(str(fact["change_pct"])).quantize(Decimal("0.1")) == Decimal("-26.9")


def test_top10_display_fact_does_not_cross_watch_sessions(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.query(TokenWatchState).delete()
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=False,
                started_at=now - timedelta(hours=1),
                last_seen_at=now - timedelta(minutes=20),
                ended_at=now - timedelta(minutes=20),
            )
        )
        for index in range(10):
            session.add(
                TopHolderSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="WINK",
                    holder_address=f"0xold{index}",
                    balance=Decimal("100"),
                    hold_percentage=Decimal("0.05"),
                    usd_value=None,
                    dropped_out_top20=False,
                    observed_at=now - timedelta(minutes=30),
                )
            )
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                active=True,
                started_at=now - timedelta(minutes=1),
                last_seen_at=now,
            )
        )
        watch_started_at = service._watch_started_at(session, wallet_id, TOKEN)
        fact = service._top10_fact(session, wallet_id, TOKEN, watch_started_at)

    assert fact is None


@pytest.mark.parametrize(
    ("window_minutes", "previous_each", "current_each", "expected"),
    [
        (15, "0.0500", "0.0495", False),  # -1%
        (16, "0.0500", "0.04865", False),  # -2.7%
        (15, "0.0500", "0.05495", False),  # +9.9%
        (15, "0.0500", "0.0550", True),  # +10%
        (15, "0.0500", "0.0450", True),  # -10%
        (60, "0.0500", "0.0600", True),
        (61, "0.0500", "0.0600", False),
        (1609, "0.0500", "0.0350", False),
    ],
)
def test_top10_display_fact_requires_meaningful_fresh_change(
    ctx,
    window_minutes: int,
    previous_each: str,
    current_each: str,
    expected: bool,
) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=1700))
    add_top10_group(
        session_factory,
        wallet_id,
        observed_at=now - timedelta(minutes=window_minutes),
        share_each=previous_each,
        prefix=f"old{window_minutes}",
    )
    add_top10_group(
        session_factory,
        wallet_id,
        observed_at=now,
        share_each=current_each,
        prefix=f"new{window_minutes}",
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        watch_started_at = service._watch_started_at(session, wallet_id, TOKEN)
        fact = service._top10_fact(session, wallet_id, TOKEN, watch_started_at)

    assert (fact is not None) is expected


def test_invalid_top10_fact_does_not_enter_display_or_position_intelligence(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=30))
    add_top10_group(session_factory, wallet_id, observed_at=now - timedelta(minutes=15), share_each="0.0500", prefix="old")
    add_top10_group(session_factory, wallet_id, observed_at=now, share_each="0.0495", prefix="new")
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        watch_started_at = service._watch_started_at(session, wallet_id, TOKEN)
        assert service._top10_fact(session, wallet_id, TOKEN, watch_started_at) is None
        facts = service._display_facts(session, wallet_id, TOKEN, None, None, 0, None, None, watch_started_at)

    assert "top10" not in facts
    result = build_position_intelligence(position_scores(), facts)
    assert "top10" not in result.positive_drivers
    assert "top10" not in result.negative_drivers


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [(5, "5m"), (15, "15m"), (30, "30m"), (60, "1h"), (120, "2h"), (135, "2h15m")],
)
def test_format_window(minutes: int, expected: str) -> None:
    assert _format_window(minutes) == expected


def test_feed_display_fact_net_wallets_and_usd_can_disagree(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(10):
        add_intelligence_event(
            session_factory,
            wallet_id,
            direction=scoring.POSITIVE,
            wallet=f"0xbuy{index}",
            usd="200",
            event_at=now,
        )
    for index in range(2):
        add_intelligence_event(
            session_factory,
            wallet_id,
            direction=scoring.NEGATIVE,
            wallet=f"0xsell{index}",
            usd="7000",
            event_at=now,
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        fact = service._feed_family_fact(session, wallet_id, TOKEN, scoring.SMART_MONEY)

    assert fact is not None
    assert fact.net_directional_wallets == 8
    assert fact.net_usd == Decimal("-12000")
    text = format_attention_alert(
        make_alert_assessment(evidence={"display_facts": {"smart_money": _test_feed_fact_dict(fact)}})
    )
    assert "• 聪明钱｜15m +8钱包 -$12k" in text


def test_feed_display_fact_omits_usd_when_incomplete(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    add_intelligence_event(session_factory, wallet_id, direction=scoring.POSITIVE, wallet="0xbuy1", usd="1000", event_at=now)
    add_intelligence_event(session_factory, wallet_id, direction=scoring.POSITIVE, wallet="0xbuy2", usd=None, event_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        fact = service._feed_family_fact(session, wallet_id, TOKEN, scoring.SMART_MONEY)

    assert fact is not None
    assert fact.usd_complete is False
    text = format_attention_alert(
        make_alert_assessment(evidence={"display_facts": {"smart_money": _test_feed_fact_dict(fact)}})
    )
    assert "• 聪明钱｜15m +2钱包" in text
    assert "$" not in text.split("• 聪明钱｜15m +2钱包", 1)[1].splitlines()[0]


def test_kol_display_fact_net_wallets_and_usd(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(3):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.POSITIVE,
            wallet=f"0xkolbuy{index}",
            usd="1000",
            event_at=now,
        )
    add_intelligence_event(
        session_factory,
        wallet_id,
        family=scoring.KOL,
        source="gmgn_kol",
        direction=scoring.NEGATIVE,
        wallet="0xkolsell",
        usd="1800",
        event_at=now,
    )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        fact = service._feed_family_fact(session, wallet_id, TOKEN, scoring.KOL)

    assert fact is not None
    assert fact.net_directional_wallets == 2
    assert fact.net_usd == Decimal("1200")
    text = format_attention_alert(
        make_alert_assessment(evidence={"display_facts": {"kol": _test_feed_fact_dict(fact)}})
    )
    assert "• KOL｜15m +2钱包 +$1.2k" in text


@pytest.mark.asyncio
async def test_kol_primary_display_direction_uses_negative_net_usd_when_scoring_mixed(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(3):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.POSITIVE,
            wallet=f"0xkolbuy{index}",
            usd="100",
            event_at=now,
        )
    for index in range(6):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.NEGATIVE,
            wallet=f"0xkolsell{index}",
            usd="316.666666666666666667",
            event_at=now,
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.primary_family == scoring.KOL
    assert assessment.kol_score == 35
    assert assessment.direction == scoring.MIXED
    assert assessment.final_attention_score == 45
    assert evidence["primary_direction"] == scoring.NEGATIVE
    assert evidence["position_intelligence"]["label"] == NO_CLEAR_CHANGE
    assert "KOL资金流出｜Mixed｜ATT 45" in format_attention_alert(assessment)
    assert "KOL资金异动｜Mixed｜ATT 45" not in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_kol_primary_display_direction_uses_positive_net_usd_when_scoring_mixed(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(6):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.POSITIVE,
            wallet=f"0xkolbuy{index}",
            usd="500",
            event_at=now,
        )
    for index in range(3):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.NEGATIVE,
            wallet=f"0xkolsell{index}",
            usd="466.666666666666666667",
            event_at=now,
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.primary_family == scoring.KOL
    assert assessment.kol_score == 35
    assert assessment.direction == scoring.MIXED
    assert evidence["primary_direction"] == scoring.POSITIVE
    assert "KOL资金流入｜Mixed｜ATT 45" in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_kol_primary_display_direction_prefers_usd_over_wallet_count(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(11):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.POSITIVE,
            wallet=f"0xkolbuy{index}",
            usd="100",
            event_at=now,
        )
    for index in range(3):
        add_intelligence_event(
            session_factory,
            wallet_id,
            family=scoring.KOL,
            source="gmgn_kol",
            direction=scoring.NEGATIVE,
            wallet=f"0xkolsell{index}",
            usd="4366.666666666666666667",
            event_at=now,
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.kol_score == 35
    assert assessment.direction == scoring.MIXED
    assert evidence["display_facts"]["kol"]["net_wallets"] == 8
    assert Decimal(evidence["display_facts"]["kol"]["net_usd"]) < 0
    assert evidence["primary_direction"] == scoring.NEGATIVE
    assert "KOL资金流出｜Mixed｜ATT 45" in format_attention_alert(assessment)


def test_feed_primary_display_direction_uses_wallets_when_usd_incomplete() -> None:
    negative = FeedFamilyFact(
        family=scoring.KOL,
        score=35,
        direction=scoring.MIXED,
        window_minutes=15,
        buy_wallets=3,
        sell_wallets=6,
        net_directional_wallets=-3,
        buy_usd=None,
        sell_usd=None,
        net_usd=None,
        usd_complete=False,
        buy_activity_count=3,
        sell_activity_count=6,
    )
    positive = FeedFamilyFact(
        family=scoring.KOL,
        score=35,
        direction=scoring.MIXED,
        window_minutes=15,
        buy_wallets=6,
        sell_wallets=3,
        net_directional_wallets=3,
        buy_usd=None,
        sell_usd=None,
        net_usd=None,
        usd_complete=False,
        buy_activity_count=6,
        sell_activity_count=3,
    )

    assert _feed_primary_display_direction(negative, scoring.MIXED) == scoring.NEGATIVE
    assert _feed_primary_display_direction(positive, scoring.MIXED) == scoring.POSITIVE


def test_feed_primary_display_direction_falls_back_when_net_zero() -> None:
    fact = FeedFamilyFact(
        family=scoring.KOL,
        score=35,
        direction=scoring.MIXED,
        window_minutes=15,
        buy_wallets=3,
        sell_wallets=3,
        net_directional_wallets=0,
        buy_usd=Decimal("1000"),
        sell_usd=Decimal("1000"),
        net_usd=Decimal("0"),
        usd_complete=True,
        buy_activity_count=3,
        sell_activity_count=3,
    )

    assert _feed_primary_display_direction(fact, scoring.MIXED) == scoring.MIXED
    assert format_directional_trigger_title(scoring.KOL, scoring.MIXED, None, None) == "KOL资金异动"


def test_smart_money_primary_display_direction_uses_same_usd_first_rule() -> None:
    fact = FeedFamilyFact(
        family=scoring.SMART_MONEY,
        score=35,
        direction=scoring.MIXED,
        window_minutes=15,
        buy_wallets=8,
        sell_wallets=2,
        net_directional_wallets=6,
        buy_usd=Decimal("1000"),
        sell_usd=Decimal("13000"),
        net_usd=Decimal("-12000"),
        usd_complete=True,
        buy_activity_count=8,
        sell_activity_count=2,
    )

    assert _feed_primary_display_direction(fact, scoring.MIXED) == scoring.NEGATIVE
    assert format_directional_trigger_title(scoring.SMART_MONEY, scoring.NEGATIVE, None, None) == "聪明钱流出"


def test_liquidity_display_fact_uses_1h_baseline(ctx) -> None:
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
        latest = TokenIntelligenceSnapshot(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            market_cap_usd=Decimal("1000000"),
            liquidity_usd=Decimal("96800"),
            holder_count=500,
            observed_at=now,
        )
        session.add(latest)
        session.flush()
        service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
        fact = service._liquidity_fact(session, wallet_id, TOKEN, latest)

    assert fact is not None
    assert fact.window_minutes == 60
    assert fact.change_pct == Decimal("-3.200")


@pytest.mark.asyncio
async def test_liquidity_display_fact_omitted_without_1h_baseline(ctx) -> None:
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
                liquidity_usd=Decimal("96800"),
                holder_count=500,
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert "liquidity" not in evidence["display_facts"]


@pytest.mark.asyncio
async def test_price_display_uses_scored_5m_window(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(PriceSnapshot).delete()
        session.query(TokenIntelligenceSnapshot).delete()
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
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1.3"),
                balance=Decimal("50"),
                usd_value=Decimal("65"),
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
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.price_score == 40
    assert evidence["display_facts"]["price"]["window_minutes"] == 5
    assert "• 价格｜5m +30%" in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_price_display_uses_scored_15m_window(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(PriceSnapshot).delete()
        session.query(TokenIntelligenceSnapshot).delete()
        for minutes, price in ((15, "1"), (5, "1.26"), (0, "1.3")):
            session.add(
                PriceSnapshot(
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="WINK",
                    price_usd=Decimal(price),
                    balance=Decimal("50"),
                    usd_value=Decimal("65"),
                    observed_at=now - timedelta(minutes=minutes),
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
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.price_score == 40
    assert evidence["display_facts"]["price"]["window_minutes"] == 15
    assert "• 价格｜15m +30%" in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_price_display_context_does_not_change_zero_price_score(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(PriceSnapshot).delete()
        session.query(TokenIntelligenceSnapshot).delete()
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
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1.02"),
                balance=Decimal("50"),
                usd_value=Decimal("51"),
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
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.price_score == 0
    assert evidence["display_facts"]["price"]["window_minutes"] == 5
    assert "• 价格｜5m +2%" in format_attention_alert(assessment)


@pytest.mark.parametrize("quality_status", [PRICE_QUALITY_PENDING, PRICE_QUALITY_OUTLIER])
def test_pending_or_outlier_price_snapshot_cannot_be_price_baseline(ctx, quality_status: str) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    watch_started_at = now - timedelta(minutes=10)
    start_watch_session(session_factory, wallet_id, started_at=watch_started_at)
    add_price_snapshot(
        session_factory,
        wallet_id,
        price="1",
        usd_value="50",
        observed_at=now - timedelta(minutes=5),
        quality_status=quality_status,
    )
    add_price_snapshot(
        session_factory,
        wallet_id,
        price="1.9",
        usd_value="95",
        observed_at=now,
        quality_status=PRICE_QUALITY_VALID,
    )
    add_token_intel(session_factory, wallet_id, market_cap="1000000", liquidity="100000", observed_at=now)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        latest_intel = service._latest_token_snapshot(session, wallet_id, TOKEN, watch_started_at)
        price_score, direction, change, window = service._price_family(
            session,
            wallet_id,
            TOKEN,
            latest_intel,
            watch_started_at,
        )
        baseline = service._baseline_price_snapshot(session, wallet_id, TOKEN, now, 5, watch_started_at)

    assert baseline is None
    assert price_score == 0
    assert direction == scoring.NEUTRAL
    assert change is None
    assert window is None


def test_pending_and_outlier_price_snapshots_do_not_enter_historical_abnormality(ctx, monkeypatch) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    watch_started_at = now - timedelta(hours=2)
    start_watch_session(session_factory, wallet_id, started_at=watch_started_at)
    add_price_snapshot(session_factory, wallet_id, price="1", observed_at=now - timedelta(minutes=55))
    add_price_snapshot(
        session_factory,
        wallet_id,
        price="2",
        observed_at=now - timedelta(minutes=50),
        quality_status=PRICE_QUALITY_PENDING,
    )
    add_price_snapshot(session_factory, wallet_id, price="1", observed_at=now - timedelta(minutes=35))
    add_price_snapshot(
        session_factory,
        wallet_id,
        price="2",
        observed_at=now - timedelta(minutes=30),
        quality_status=PRICE_QUALITY_OUTLIER,
    )
    add_price_snapshot(session_factory, wallet_id, price="1.2", observed_at=now, quality_status=PRICE_QUALITY_VALID)
    captured: dict[str, list[Decimal]] = {}

    def capture_abnormality(current_abs_change, historical_abs_changes):
        captured["changes"] = historical_abs_changes
        return 5

    monkeypatch.setattr(scoring, "abnormality_score", capture_abnormality)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    with session_scope(session_factory) as session:
        result = service._abnormality(session, wallet_id, TOKEN, Decimal("20"), 5, watch_started_at)

    assert result == 5
    assert captured["changes"] == []


@pytest.mark.asyncio
async def test_ai_single_snapshot_outlier_does_not_create_false_price_attention(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    watch_started_at = now - timedelta(minutes=20)
    start_watch_session(session_factory, wallet_id, token=TOKEN, symbol="AI", started_at=watch_started_at)
    add_token_intel(
        session_factory,
        wallet_id,
        token=TOKEN,
        symbol="AI",
        market_cap="227000000",
        liquidity="4210000",
        observed_at=now,
    )
    for minutes, price, status in (
        (5, "0.22786337", PRICE_QUALITY_VALID),
        (4, "0.23056119", PRICE_QUALITY_VALID),
        (3, "0.43582597", PRICE_QUALITY_OUTLIER),
        (2, "0.23464219", PRICE_QUALITY_VALID),
        (1, "0.23484438", PRICE_QUALITY_VALID),
    ):
        add_price_snapshot(
            session_factory,
            wallet_id,
            token=TOKEN,
            symbol="AI",
            price=price,
            usd_value="38",
            observed_at=now - timedelta(minutes=minutes),
            quality_status=status,
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.handle_price_snapshot_update(wallet_id, TOKEN)

    assert assessment is None
    with session_scope(session_factory) as session:
        latest = service._latest_price_snapshot(session, wallet_id, TOKEN, watch_started_at)
        previous = service._previous_price_snapshot(session, wallet_id, TOKEN, watch_started_at, latest.observed_at)
        latest_intel = service._latest_token_snapshot(session, wallet_id, TOKEN, watch_started_at)
        price_score, direction, change, window = service._price_family(
            session,
            wallet_id,
            TOKEN,
            latest_intel,
            watch_started_at,
        )

    assert Decimal(str(latest.price_usd)).quantize(Decimal("0.00000001")) == Decimal("0.23484438")
    assert Decimal(str(previous.price_usd)).quantize(Decimal("0.00000001")) == Decimal("0.23464219")
    assert price_score == 0
    assert direction == scoring.NEUTRAL
    assert change is None
    assert window is None


def test_format_compact_usd() -> None:
    assert format_compact_usd(Decimal("420")) == "$420"
    assert format_compact_usd(Decimal("4820"), signed=True) == "+$4.8k"
    assert format_compact_usd(Decimal("-12340"), signed=True) == "-$12.3k"
    assert format_compact_usd(Decimal("1250000"), signed=True) == "+$1.25m"


def test_position_intelligence_structural_deterioration_price_holder_smart() -> None:
    result = build_position_intelligence(
        position_scores(price=40, holder=10, smart=25),
        position_facts(price="-32", holder="-18", smart_wallets=-3, smart_usd="-8000"),
    )

    assert result.label == STRUCTURAL_DETERIORATION
    assert result.label_cn == "结构性恶化"
    assert result.summary == "价格下跌且持仓人数、资金流同步走弱，结构性风险上升。"


def test_position_intelligence_structural_deterioration_top10_liquidity() -> None:
    result = build_position_intelligence(
        position_scores(price=40, liquidity=25),
        position_facts(price="-28", top10="22", liquidity="-31"),
    )

    assert result.label == STRUCTURAL_DETERIORATION
    assert result.summary == "价格走弱，同时筹码集中度提高、流动性下降，结构性恶化。"


def test_position_intelligence_market_expansion_holder_smart() -> None:
    result = build_position_intelligence(
        position_scores(price=40, holder=10, smart=25),
        position_facts(price="28", holder="24", smart_wallets=4, smart_usd="11000"),
    )

    assert result.label == MARKET_EXPANSION
    assert result.summary == "价格走强，持仓人数与资金流同步改善，市场扩散增强。"


def test_position_intelligence_market_expansion_top10_liquidity() -> None:
    result = build_position_intelligence(
        position_scores(price=40, holder=10),
        position_facts(price="32", holder="18", top10="-15", liquidity="17"),
    )

    assert result.label == MARKET_EXPANSION
    assert result.summary == "价格上涨同时筹码趋于分散，市场扩散结构增强。"


def test_position_intelligence_minority_driven_top10_holder_neutral() -> None:
    result = build_position_intelligence(
        position_scores(price=40),
        position_facts(price="50", top10="35", holder="0"),
    )

    assert result.label == MINORITY_DRIVEN
    assert result.summary == "价格上涨但筹码趋于集中，市场扩散有限，偏少数资金推动。"


def test_position_intelligence_minority_driven_smart_outflow() -> None:
    result = build_position_intelligence(
        position_scores(price=40, smart=25),
        position_facts(price="35", top10="20", smart_wallets=-4, smart_usd="-7000"),
    )

    assert result.label == MINORITY_DRIVEN
    assert result.summary == "价格上涨但筹码趋于集中，聪明钱净流出，偏少数资金推动。"


def test_position_intelligence_smart_inflow_without_top10_not_minority_driven() -> None:
    result = build_position_intelligence(
        position_scores(price=40, smart=25),
        position_facts(price="35", top10="0", smart_wallets=3, smart_usd="7000"),
    )

    assert result.label == NO_CLEAR_CHANGE


def test_position_intelligence_support_emerging() -> None:
    result = build_position_intelligence(
        position_scores(price=40, smart=25),
        position_facts(price="-25", smart_wallets=4, smart_usd="6000", liquidity="0"),
    )

    assert result.label == SUPPORT_EMERGING
    assert result.summary == "价格回落，但聪明钱出现净流入，短线存在承接。"


def test_position_intelligence_structural_priority_over_support() -> None:
    result = build_position_intelligence(
        position_scores(price=40, holder=10, smart=25, liquidity=25),
        position_facts(price="-25", holder="-20", smart_wallets=4, smart_usd="6000", liquidity="-31"),
    )

    assert result.label == STRUCTURAL_DETERIORATION


def test_position_intelligence_signal_divergence_price_holder_smart() -> None:
    result = build_position_intelligence(
        position_scores(price=40, holder=10, smart=25),
        position_facts(price="25", holder="15", smart_wallets=-4, smart_usd="-8000"),
    )

    assert result.label == SIGNAL_DIVERGENCE
    assert result.summary == "价格上涨，但聪明钱净流出，当前价格与资金信号分化。"


def test_position_intelligence_signal_divergence_holder_top10() -> None:
    result = build_position_intelligence(
        position_scores(holder=10),
        position_facts(holder="15", top10="20"),
    )

    assert result.label == SIGNAL_DIVERGENCE
    assert result.summary == "持仓人数增长，但筹码集中度提高，结构信号分化。"


def test_position_intelligence_no_clear_change_only_price_or_no_data() -> None:
    price_only = build_position_intelligence(position_scores(price=40), position_facts(price="25"))
    no_data = build_position_intelligence(position_scores(), {})

    assert price_only.label == NO_CLEAR_CHANGE
    assert price_only.summary == "当前主要是价格异动，暂未看到明显结构共振。"
    assert no_data.label == NO_CLEAR_CHANGE
    assert no_data.summary == "当前有效结构信号有限，暂未看到明显共振。"


@pytest.mark.parametrize(
    ("change", "positive", "negative", "neutral"),
    [
        ("9.9", [], [], ["top10"]),
        ("10", [], ["top10"], []),
        ("-10", ["top10"], [], []),
    ],
)
def test_position_intelligence_top10_threshold(change: str, positive: list[str], negative: list[str], neutral: list[str]) -> None:
    result = build_position_intelligence(position_scores(), position_facts(top10=change))

    assert result.positive_drivers == positive
    assert result.negative_drivers == negative
    assert result.neutral_drivers == neutral


@pytest.mark.asyncio
async def test_position_intelligence_is_frozen_in_assessment_evidence(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.query(PriceSnapshot).delete()
        session.query(TokenIntelligenceSnapshot).delete()
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
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1.3"),
                balance=Decimal("50"),
                usd_value=Decimal("65"),
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
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert evidence["position_intelligence"]["label"] == NO_CLEAR_CHANGE
    assert evidence["position_intelligence"]["summary"] == "当前主要是价格异动，暂未看到明显结构共振。"
    assert evidence["primary_family"] == scoring.PRICE
    assert evidence["primary_direction"] == scoring.POSITIVE
    assert evidence["primary_signal"] is None


@pytest.mark.asyncio
async def test_top_holder_reduction_assessment_freezes_primary_signal(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    started_at = now - timedelta(minutes=10)
    start_watch_session(session_factory, wallet_id, started_at=started_at)
    holder = "0x0000000000000000000000000000000000000abc"
    with session_scope(session_factory) as session:
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1"),
                balance=Decimal("50"),
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
                observed_at=now,
            )
        )
        session.add(
            TopHolderPeak(
                wallet_id=wallet_id,
                token_address=TOKEN,
                holder_address=holder,
                peak_balance=Decimal("1000"),
                peak_hold_percentage=Decimal("0.02"),
                watch_started_at=started_at,
            )
        )
        session.add(
            TopHolderSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                holder_address=holder,
                balance=Decimal("650"),
                hold_percentage=Decimal("0.013"),
                observed_at=now,
            )
        )
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.primary_family == scoring.HOLDER
    assert evidence["primary_direction"] == scoring.NEGATIVE
    assert evidence["primary_signal"] == "top_holder_reduction"
    text = format_attention_alert(assessment)
    assert "重要大户减仓｜Negative｜ATT" in text
    assert "筹码集中" not in text
    assert "筹码分散" not in text


@pytest.mark.asyncio
async def test_holder_cluster_reduction_assessment_freezes_primary_signal(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    started_at = now - timedelta(minutes=10)
    start_watch_session(session_factory, wallet_id, started_at=started_at)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    with session_scope(session_factory) as session:
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="WINK",
                price_usd=Decimal("1"),
                balance=Decimal("50"),
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
                observed_at=now,
            )
        )
        watched = service._watched_token(session, wallet_id, TOKEN)
    service._save_top_holder_snapshots(
        watched,
        [
            holder("0xholder1", "1000", "0.05"),
            holder("0xholder2", "900", "0.04"),
            holder("0xholder3", "800", "0.03"),
        ],
    )
    service._save_top_holder_snapshots(
        watched,
        [
            holder("0xholder1", "750", "0.04"),
            holder("0xholder2", "675", "0.03"),
            holder("0xholder3", "600", "0.02"),
        ],
    )

    assessment = await service.assess_token(wallet_id, TOKEN)
    evidence = json.loads(assessment.evidence_json)

    assert assessment.primary_family == scoring.HOLDER
    assert assessment.top_holder_score == 15
    assert assessment.holder_cluster_score == 30
    assert assessment.holder_family_score == 30
    assert assessment.direction == scoring.NEGATIVE
    assert evidence["primary_signal"] == "holder_cluster_reduction"
    assert evidence["primary_direction"] == scoring.NEGATIVE
    text = format_attention_alert(assessment)
    assert "多名大户减仓｜Negative｜ATT" in text
    assert "筹码集中" not in text


def test_format_attention_alert_uses_frozen_position_intelligence() -> None:
    text = format_attention_alert(
        make_alert_assessment(
            evidence={
                "display_facts": {"price": {"window_minutes": 5, "change_pct": "32.6"}},
                "position_intelligence": {
                    "label": MINORITY_DRIVEN,
                    "label_cn": "少数资金推动",
                    "summary": "固定判断，不重新读取数据库。",
                    "positive_drivers": ["price"],
                    "negative_drivers": ["top10"],
                    "neutral_drivers": [],
                },
            }
        )
    )

    assert "判断：固定判断，不重新读取数据库。" in text
    assert "label_cn" not in text
    assert "少数资金推动：" not in text


def test_position_intelligence_does_not_change_assessment_scoring_fields() -> None:
    assessment = make_alert_assessment(final_score=72, direction=scoring.MIXED)
    before = (
        assessment.final_attention_score,
        assessment.should_notify,
        assessment.direction,
        assessment.primary_family,
        assessment.price_score,
        assessment.holder_family_score,
        assessment.smart_money_score,
        assessment.kol_score,
        assessment.liquidity_score,
    )

    text = format_attention_alert(assessment)
    after = (
        assessment.final_attention_score,
        assessment.should_notify,
        assessment.direction,
        assessment.primary_family,
        assessment.price_score,
        assessment.holder_family_score,
        assessment.smart_money_score,
        assessment.kol_score,
        assessment.liquidity_score,
    )

    assert "判断：" in text
    assert after == before


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
async def test_same_token_concurrent_notifications_are_serialized_and_rechecked(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    calls = 0

    async def slow_notify(chat_id: int, text: str, reply_markup=None) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        sent.append((chat_id, text))

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, slow_notify)
    first = notification_assessment(wallet_id)
    second = notification_assessment(wallet_id)

    results = await asyncio.gather(service._notify_assessment(first), service._notify_assessment(second))

    assert results.count(True) == 1
    assert results.count(False) == 1
    assert calls == 1
    assert len(sent) == 1
    with session_scope(session_factory) as session:
        states = list(session.scalars(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)))
        assert len(states) == 1


@pytest.mark.asyncio
async def test_serialized_recheck_still_allows_critical_and_score_upgrade(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)

    warning = notification_assessment(wallet_id, final_score=55, level=scoring.WARNING, price_score=30)
    critical = notification_assessment(wallet_id, final_score=80, level=scoring.CRITICAL, price_score=40)
    assert await service._notify_assessment(warning) is True
    assert await service._notify_assessment(critical) is True

    with session_scope(session_factory) as session:
        session.query(AttentionAlertState).delete()
    sent.clear()

    warning = notification_assessment(wallet_id, final_score=55, level=scoring.WARNING, price_score=30)
    upgraded = notification_assessment(wallet_id, final_score=70, level=scoring.WARNING, price_score=30)
    assert await service._notify_assessment(warning) is True
    assert await service._notify_assessment(upgraded) is True

    assert len(sent) == 2


@pytest.mark.asyncio
async def test_different_token_notifications_do_not_share_global_lock(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, token=TOKEN, symbol="WINK")
    add_watched(session_factory, wallet_id, token=TOKEN_2, symbol="PONS")
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    delivered: list[str] = []

    async def notify_with_blocked_first(chat_id: int, text: str, reply_markup=None) -> None:
        if "WINK" in text:
            first_started.set()
            await release_first.wait()
            delivered.append("WINK")
            return
        delivered.append("PONS")

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify_with_blocked_first)
    first = notification_assessment(wallet_id, TOKEN, symbol="WINK")
    second = notification_assessment(wallet_id, TOKEN_2, symbol="PONS")

    first_task = asyncio.create_task(service._notify_assessment(first))
    await first_started.wait()
    second_task = asyncio.create_task(service._notify_assessment(second))
    await asyncio.sleep(0.05)

    assert delivered == ["PONS"]
    release_first.set()
    assert await first_task is True
    assert await second_task is True
    assert delivered == ["PONS", "WINK"]


@pytest.mark.asyncio
async def test_failed_same_token_notification_does_not_mark_and_later_assessment_retries(monkeypatch, ctx) -> None:
    monkeypatch.setattr(
        "app.services.attention_engine_service.ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS",
        (0, 0, 0),
    )
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    attempts = 0

    async def fail_then_succeed(chat_id: int, text: str, reply_markup=None) -> None:
        nonlocal attempts
        attempts += 1
        if attempts <= 3:
            raise RuntimeError("telegram_down")
        sent.append((chat_id, text))

    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, fail_then_succeed)
    first = notification_assessment(wallet_id)
    second = notification_assessment(wallet_id)

    with pytest.raises(RuntimeError, match="telegram_down"):
        await service._notify_assessment(first)
    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)) is None

    assert await service._notify_assessment(second) is True
    assert attempts == 4
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_stale_session_assessment_waiting_for_lock_is_not_sent_or_marked(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify)
    assessment = notification_assessment(wallet_id)
    lock = service._notification_lock(wallet_id, TOKEN)
    await lock.acquire()
    try:
        task = asyncio.create_task(service._notify_assessment(assessment))
        await asyncio.sleep(0)
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
    finally:
        lock.release()

    assert await task is False
    assert sent == []
    with session_scope(session_factory) as session:
        assert session.scalar(select(AttentionAlertState).where(AttentionAlertState.token_address == TOKEN)) is None


def make_alert_assessment(
    *,
    symbol: str = "$ROBBIE",
    primary_family: str | None = scoring.PRICE,
    direction: str = scoring.MIXED,
    final_score: int = 70,
    level: str = scoring.WARNING,
    token_address: str = "0xb6ec6e6e58354d956ef1a2ed6693d5c7056298d2",
    evidence: dict[str, object] | None = None,
    primary_direction: str = scoring.POSITIVE,
    primary_signal: str | None = None,
) -> AttentionAssessment:
    evidence = evidence or {
        "primary_direction": primary_direction,
        "primary_signal": primary_signal,
        "price_change_pct": "17.9",
        "display_facts": {
            "price": {
                "window_minutes": 5,
                "change_pct": "17.9",
                "current_price": "0.01",
                "baseline_price": "0.00848",
            },
            "holder_count": {
                "window_minutes": 30,
                "baseline_count": 500,
                "current_count": 542,
                "delta_count": 42,
                "change_pct": "8.4",
            },
            "top10": {
                "window_minutes": 15,
                "baseline_share": "0.420",
                "current_share": "0.307",
                "change_pct": "-26.9047619",
            },
            "smart_money": {
                "window_minutes": 15,
                "buy_wallets": 8,
                "sell_wallets": 2,
                "net_wallets": 6,
                "buy_usd": "10000",
                "sell_usd": "5180",
                "net_usd": "4820",
                "usd_complete": True,
            },
            "kol": {
                "window_minutes": 15,
                "buy_wallets": 3,
                "sell_wallets": 1,
                "net_wallets": 2,
                "buy_usd": "2200",
                "sell_usd": "1000",
                "net_usd": "1200",
                "usd_complete": True,
            },
            "liquidity": {
                "window_minutes": 60,
                "baseline_usd": "100000",
                "current_usd": "96800",
                "change_pct": "-3.2",
            },
        },
        "position_intelligence": {
            "label": NO_CLEAR_CHANGE,
            "label_cn": "暂无明显结构变化",
            "summary": "当前主要是价格异动，暂未看到明显结构共振。",
            "positive_drivers": ["price", "top10"],
            "negative_drivers": [],
            "neutral_drivers": ["holder_count", "liquidity"],
        },
    }
    return AttentionAssessment(
        wallet_id=1,
        token_address=token_address,
        symbol=symbol,
        assessed_at=utc_now(),
        price_score=40 if primary_family == scoring.PRICE else 0,
        holder_family_score=25 if primary_family == scoring.HOLDER else 0,
        smart_money_score=25 if primary_family == scoring.SMART_MONEY else 0,
        kol_score=28 if primary_family == scoring.KOL else 0,
        liquidity_score=35 if primary_family == scoring.LIQUIDITY else 0,
        primary_family=primary_family,
        primary_event_score=40,
        final_attention_score=final_score,
        attention_level=level,
        direction=direction,
        evidence_json=json.dumps(evidence),
    )


def test_format_attention_alert_removes_redundant_title_and_internal_level() -> None:
    text = format_attention_alert(make_alert_assessment(level=scoring.CRITICAL))

    assert "$ROBBIE 持仓异动" not in text
    assert text.startswith("🟠 $ROBBIE\n\n价格上涨｜Mixed｜ATT 70")
    assert "WARNING" not in text
    assert "CRITICAL" not in text
    assert "主要触发" not in text
    assert "Holder Score" not in text
    assert "Smart Money Score" not in text
    assert "KOL Score" not in text
    assert "Liquidity Score" not in text
    assert "• 价格｜5m +17.9%" in text
    assert "• 持仓人数｜30m +8.4%" in text
    assert "• Top10｜15m -26.9%" in text
    assert "• 聪明钱｜15m +6钱包 +$4.8k" in text
    assert "• KOL｜15m +2钱包 +$1.2k" in text
    assert "• 流动性｜1h -3.2%" in text
    assert "判断：当前主要是价格异动，暂未看到明显结构共振。" in text
    assert "暂无明显结构变化：" not in text
    assert text.index("• 价格｜5m +17.9%") < text.index("• 流动性｜1h -3.2%")
    assert text.index("• 流动性｜1h -3.2%") < text.index("• 持仓人数｜30m +8.4%")
    assert text.index("• 持仓人数｜30m +8.4%") < text.index("• Top10｜15m -26.9%")
    assert text.index("• Top10｜15m -26.9%") < text.index("• 聪明钱｜15m +6钱包 +$4.8k")
    assert text.index("• 聪明钱｜15m +6钱包 +$4.8k") < text.index("• KOL｜15m +2钱包 +$1.2k")


@pytest.mark.parametrize(
    ("direction", "expected_emoji"),
    [
        (scoring.POSITIVE, "🟢"),
        (scoring.NEGATIVE, "🔴"),
        (scoring.MIXED, "🟠"),
        (scoring.NEUTRAL, "⚪️"),
        ("unknown", "⚪️"),
    ],
)
def test_format_attention_alert_direction_emoji_is_explicit(direction, expected_emoji) -> None:
    text = format_attention_alert(make_alert_assessment(direction=direction))

    assert text.splitlines()[0].startswith(f"{expected_emoji} $ROBBIE")


def test_format_attention_alert_orders_display_facts_and_omits_missing() -> None:
    text = format_attention_alert(
        make_alert_assessment(
            evidence={
                "display_facts": {
                    "liquidity": {"window_minutes": 60, "change_pct": "-3.2"},
                    "smart_money": {
                        "window_minutes": 15,
                        "net_wallets": 0,
                        "net_usd": "8000",
                        "usd_complete": True,
                    },
                    "price": {"window_minutes": 5, "change_pct": "4.2"},
                    "top10": {"window_minutes": 30, "change_pct": "0"},
                }
            }
        )
    )

    price_index = text.index("• 价格｜5m +4.2%")
    liquidity_index = text.index("• 流动性｜1h -3.2%")
    top10_index = text.index("• Top10｜30m 0%")
    smart_index = text.index("• 聪明钱｜15m 0钱包 +$8k")
    assert price_index < liquidity_index < top10_index < smart_index
    assert "持仓人数" not in text
    assert "KOL｜" not in text


@pytest.mark.parametrize(
    ("family", "direction", "signal", "trigger", "expected"),
    [
        (scoring.PRICE, scoring.POSITIVE, None, None, "价格上涨"),
        (scoring.PRICE, scoring.NEGATIVE, None, None, "价格下跌"),
        (scoring.HOLDER, scoring.POSITIVE, "holder_breadth", None, "持币人数增加"),
        (scoring.HOLDER, scoring.NEGATIVE, "holder_breadth", None, "持币人数减少"),
        (scoring.HOLDER, scoring.NEGATIVE, "top_holder_reduction", None, "重要大户减仓"),
        (scoring.HOLDER, scoring.NEGATIVE, "holder_cluster_reduction", None, "多名大户减仓"),
        (scoring.SMART_MONEY, scoring.POSITIVE, None, None, "聪明钱流入"),
        (scoring.SMART_MONEY, scoring.NEGATIVE, None, None, "聪明钱流出"),
        (scoring.KOL, scoring.POSITIVE, None, None, "KOL资金流入"),
        (scoring.KOL, scoring.NEGATIVE, None, None, "KOL资金流出"),
        (scoring.LIQUIDITY, scoring.POSITIVE, None, None, "流动性增加"),
        (scoring.LIQUIDITY, scoring.NEGATIVE, None, None, "流动性减少"),
        ("comprehensive", scoring.POSITIVE, None, None, "综合走强"),
        ("comprehensive", scoring.NEGATIVE, None, None, "综合走弱"),
        ("comprehensive", scoring.MIXED, None, None, "信号分化"),
        (scoring.PRICE, scoring.NEGATIVE, None, "usd_threshold_crossing", "持仓价值跌破$5"),
    ],
)
def test_format_directional_trigger_title_mappings(family, direction, signal, trigger, expected) -> None:
    assert format_directional_trigger_title(family, direction, trigger, signal) == expected


@pytest.mark.parametrize(
    ("distinct_kols", "expected"),
    [(0, 0), (1, 12), (2, 20), (3, 28), (4, 32), (5, 36), (6, 36), (7, 40), (9, 40)],
)
def test_social_kol_heat_score_table(distinct_kols, expected) -> None:
    assert scoring.social_kol_heat_score(distinct_kols) == expected


@pytest.mark.parametrize(
    ("significance", "expected"),
    [("low", 15), ("medium", 28), ("high", 40), (None, 0)],
)
def test_social_dev_update_score_table(significance, expected) -> None:
    assert scoring.social_dev_update_score(significance) == expected


@pytest.mark.asyncio
async def test_high_dev_memory_adds_modifier_and_crosses_warning_at_50_usd(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="50")
    add_social_memory(session_factory, tweet_id="high-dev-modifier", significance="high")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["highest_dev_significance"] == "high"
    assert assessment.base_attention_score == 50
    assert assessment.dev_modifier == 5
    assert evidence["dev_modifier_reason"] == "high_dev_update"
    assert assessment.final_attention_score == 55
    assert assessment.attention_level == scoring.WARNING
    assert assessment.should_notify is True


@pytest.mark.asyncio
async def test_low_and_medium_dev_memory_do_not_add_modifier(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="50")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    add_social_memory(session_factory, tweet_id="low-dev-no-modifier", significance="low")
    low = await service.assess_token(wallet_id, TOKEN)
    assert low is not None
    assert low.dev_modifier == 0
    assert low.final_attention_score == 25

    with session_scope(session_factory) as session:
        session.query(SocialMemory).delete()
    add_social_memory(session_factory, tweet_id="medium-dev-no-modifier", significance="medium")
    medium = await service.assess_token(wallet_id, TOKEN)
    assert medium is not None
    assert medium.dev_modifier == 0
    assert medium.final_attention_score == 38


@pytest.mark.asyncio
async def test_pure_social_kol_heat_does_not_add_dev_modifier(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="50")
    for index in range(7):
        add_social_event(
            session_factory,
            wallet_id,
            tweet_id=f"pure-kol-{index}",
            author_id=f"pure-kol-{index}",
            username=f"kol{index}",
        )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["social_score"] == 40
    assert evidence["social"]["highest_dev_significance"] is None
    assert assessment.dev_modifier == 0
    assert assessment.final_attention_score == 50
    assert assessment.attention_level == scoring.NOTICE


@pytest.mark.parametrize(
    ("usd_value", "expected_final", "expected_level", "expected_notify"),
    [
        ("20", 53, scoring.NOTICE, False),
        ("50", 55, scoring.WARNING, True),
        ("250", 62, scoring.WARNING, True),
    ],
)
@pytest.mark.asyncio
async def test_high_dev_modifier_matrix_for_representative_exposures(
    ctx,
    usd_value,
    expected_final,
    expected_level,
    expected_notify,
) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value=usd_value)
    add_social_memory(session_factory, tweet_id=f"high-dev-{usd_value}", significance="high")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.dev_modifier == 5
    assert assessment.final_attention_score == expected_final
    assert assessment.attention_level == expected_level
    assert assessment.should_notify is expected_notify


@pytest.mark.asyncio
async def test_high_dev_modifier_applies_when_social_is_not_primary_without_changing_direction(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="1.5", observed_at=now)
    add_token_intel(session_factory, wallet_id, market_cap="1000000", liquidity="100000", observed_at=now)
    add_social_memory(session_factory, tweet_id="price-primary-high-dev", significance="high", event_time=now)
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score == 40
    assert assessment.primary_family == scoring.PRICE
    assert assessment.dev_modifier == 5
    assert assessment.direction == scoring.POSITIVE
    assert "价格上涨｜Positive" in format_attention_alert(assessment)


@pytest.mark.asyncio
async def test_high_dev_memory_outside_attention_window_does_not_add_modifier(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=30))
    add_price_snapshot(session_factory, wallet_id, observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    add_social_memory(
        session_factory,
        tweet_id="old-window-high-dev",
        significance="high",
        event_time=now - timedelta(minutes=20),
    )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["meaningful_dev_updates"] == 0
    assert assessment.dev_modifier == 0


@pytest.mark.asyncio
async def test_social_attention_distinct_kol_scores_and_dedupes_same_author(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    add_social_event(session_factory, wallet_id, tweet_id="tweet-1", author_id="a1", username="Nancy", posted_at=now)
    add_social_event(session_factory, wallet_id, tweet_id="tweet-2", author_id="a1", username="Nancy", posted_at=now)
    add_social_event(session_factory, wallet_id, tweet_id="tweet-3", author_id="a2", username="Lookonchain", posted_at=now, match_type="direct_cashtag")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["unique_kols"] == 2
    assert evidence["social"]["kol_posts"] == 3
    assert evidence["social"]["x_kol_heat_score"] == 20
    assert evidence["family_scores"][scoring.SOCIAL] == 20


@pytest.mark.asyncio
async def test_social_attention_dev_memory_uses_social_memory_not_raw_project_event(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    add_social_event(session_factory, wallet_id, tweet_id="project-gm", author_type=AUTHOR_PROJECT_X, username="ProjectUser")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    raw_only = await service.assess_token(wallet_id, TOKEN)
    assert raw_only is not None
    raw_evidence = json.loads(raw_only.evidence_json)
    assert raw_evidence["social"]["meaningful_dev_updates"] == 0
    assert raw_evidence["family_scores"][scoring.SOCIAL] == 0
    assert raw_only.dev_modifier == 0

    add_social_memory(session_factory, tweet_id="memory-low", significance="low")
    low = await service.assess_token(wallet_id, TOKEN)
    assert low is not None
    low_evidence = json.loads(low.evidence_json)
    assert low_evidence["social"]["meaningful_dev_updates"] == 1
    assert low_evidence["social"]["highest_dev_significance"] == "low"
    assert low_evidence["family_scores"][scoring.SOCIAL] == 15


@pytest.mark.asyncio
async def test_social_attention_dev_memory_isolated_by_current_watch_session(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=20))
    add_price_snapshot(session_factory, wallet_id, observed_at=now - timedelta(minutes=10))
    add_token_intel(session_factory, wallet_id, observed_at=now - timedelta(minutes=10))
    add_social_memory(
        session_factory,
        tweet_id="session-a-dev-memory",
        significance="high",
        event_time=now - timedelta(minutes=5),
    )
    add_social_event(
        session_factory,
        wallet_id,
        tweet_id="session-a-kol",
        author_id="old-session-kol",
        posted_at=now - timedelta(minutes=5),
    )
    end_active_watch(session_factory, wallet_id, ended_at=now - timedelta(minutes=2))
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=1))
    add_price_snapshot(session_factory, wallet_id, observed_at=now)
    add_token_intel(session_factory, wallet_id, observed_at=now)
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    new_session_before_memory = await service.assess_token(wallet_id, TOKEN)

    assert new_session_before_memory is not None
    evidence = json.loads(new_session_before_memory.evidence_json)
    assert evidence["social"]["meaningful_dev_updates"] == 0
    assert evidence["social"]["dev_update_score"] == 0
    assert evidence["social"]["unique_kols"] == 0
    assert evidence["social"]["x_kol_heat_score"] == 0
    assert evidence["family_scores"][scoring.SOCIAL] == 0
    assert new_session_before_memory.dev_modifier == 0
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(SocialMemory)))) == 1

    add_social_memory(
        session_factory,
        tweet_id="session-b-dev-memory",
        significance="high",
        event_time=now,
    )

    new_session_after_memory = await service.assess_token(wallet_id, TOKEN)

    assert new_session_after_memory is not None
    new_evidence = json.loads(new_session_after_memory.evidence_json)
    assert new_evidence["social"]["meaningful_dev_updates"] == 1
    assert new_evidence["social"]["dev_update_score"] == 40
    assert new_evidence["social"]["unique_kols"] == 0
    assert new_evidence["family_scores"][scoring.SOCIAL] == 40
    assert new_session_after_memory.dev_modifier == 5
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(SocialMemory)))) == 2


@pytest.mark.asyncio
async def test_social_attention_dev_significance_and_tie_prioritize_dev(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    for index in range(7):
        add_social_event(
            session_factory,
            wallet_id,
            tweet_id=f"kol-{index}",
            author_id=f"author-{index}",
            username=f"kol{index}",
            posted_at=now,
        )
    add_social_memory(session_factory, tweet_id="memory-medium", significance="medium", event_time=now)
    add_social_memory(
        session_factory,
        tweet_id="memory-high",
        significance="high",
        event_time=now + timedelta(seconds=1),
        url="https://x.com/ProjectUser/status/memory-high",
    )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["unique_kols"] == 7
    assert evidence["social"]["meaningful_dev_updates"] == 2
    assert evidence["social"]["highest_dev_significance"] == "high"
    assert evidence["social"]["social_score"] == 40
    assert evidence["primary_family"] == scoring.SOCIAL
    assert evidence["primary_signal"] == "dev_project_update"
    assert assessment.primary_family == scoring.SOCIAL
    assert assessment.dev_modifier == 5
    assert assessment.direction == scoring.NEUTRAL
    assert "• 社媒｜15m KOL+7 DEV+2" in format_attention_alert(assessment)
    markup = build_attention_copy_markup(assessment)
    assert markup.inline_keyboard[1][0].text == "🔗 查看DEV更新"
    assert markup.inline_keyboard[1][0].url == "https://x.com/ProjectUser/status/memory-high"


@pytest.mark.asyncio
async def test_social_dev_button_uses_latest_memory_within_highest_significance_tier(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    add_social_memory(
        session_factory,
        tweet_id="older-high",
        significance="high",
        event_time=now - timedelta(minutes=4),
        url="https://x.com/testdev/status/high",
    )
    add_social_memory(
        session_factory,
        tweet_id="newer-medium",
        significance="medium",
        event_time=now,
        url="https://x.com/testdev/status/medium",
    )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["highest_dev_significance"] == "high"
    assert evidence["social"]["dev_update_score"] == 40
    assert assessment.dev_modifier == 5
    assert evidence["social"]["latest_dev_tweet_url"] == "https://x.com/testdev/status/high"
    markup = build_attention_copy_markup(assessment)
    assert markup.inline_keyboard[1][0].url == "https://x.com/testdev/status/high"

    with session_scope(session_factory) as session:
        session.query(SocialMemory).delete()

    frozen_markup = build_attention_copy_markup(assessment)
    assert frozen_markup.inline_keyboard[1][0].url == "https://x.com/testdev/status/high"


@pytest.mark.asyncio
async def test_social_dev_button_uses_latest_high_when_newer_medium_exists(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    add_social_memory(
        session_factory,
        tweet_id="high-1",
        significance="high",
        event_time=now - timedelta(minutes=6),
        url="https://x.com/testdev/status/high-1",
    )
    add_social_memory(
        session_factory,
        tweet_id="high-2",
        significance="high",
        event_time=now - timedelta(minutes=2),
        url="https://x.com/testdev/status/high-2",
    )
    add_social_memory(
        session_factory,
        tweet_id="medium-newer",
        significance="medium",
        event_time=now,
        url="https://x.com/testdev/status/medium-newer",
    )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["highest_dev_significance"] == "high"
    assert evidence["social"]["latest_dev_tweet_url"] == "https://x.com/testdev/status/high-2"
    assert build_attention_copy_markup(assessment).inline_keyboard[1][0].url == "https://x.com/testdev/status/high-2"


@pytest.mark.asyncio
async def test_social_dev_button_uses_latest_medium_when_medium_is_highest(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    now = utc_now()
    add_social_memory(
        session_factory,
        tweet_id="medium-1",
        significance="medium",
        event_time=now - timedelta(minutes=3),
        url="https://x.com/testdev/status/medium-1",
    )
    add_social_memory(
        session_factory,
        tweet_id="medium-2",
        significance="medium",
        event_time=now,
        url="https://x.com/testdev/status/medium-2",
    )
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    evidence = json.loads(assessment.evidence_json)
    assert evidence["social"]["highest_dev_significance"] == "medium"
    assert evidence["social"]["latest_dev_tweet_url"] == "https://x.com/testdev/status/medium-2"
    assert assessment.dev_modifier == 0
    assert build_attention_copy_markup(assessment).inline_keyboard[1][0].url == "https://x.com/testdev/status/medium-2"


@pytest.mark.asyncio
async def test_social_can_be_secondary_without_changing_price_direction(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="1.4", observed_at=now)
    add_token_intel(session_factory, wallet_id, market_cap="1000000", liquidity="100000", observed_at=now)
    for index in range(3):
        add_social_event(session_factory, wallet_id, tweet_id=f"kol-secondary-{index}", author_id=f"a{index}")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.primary_family == scoring.PRICE
    assert assessment.secondary_family_1 == scoring.SOCIAL
    assert assessment.direction == scoring.POSITIVE


def test_social_primary_titles() -> None:
    assert (
        format_directional_trigger_title(scoring.SOCIAL, scoring.NEUTRAL, None, "social_kol_heat")
        == "社媒热度上升"
    )
    assert (
        format_directional_trigger_title(scoring.SOCIAL, scoring.NEUTRAL, None, "dev_project_update")
        == "DEV推特更新"
    )


@pytest.mark.asyncio
async def test_social_display_lines_for_kol_only_and_dev_only(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    for index in range(3):
        add_social_event(session_factory, wallet_id, tweet_id=f"kol-display-{index}", author_id=f"display-{index}")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    kol_only = await service.assess_token(wallet_id, TOKEN)
    assert kol_only is not None
    assert "• 社媒｜15m KOL+3" in format_attention_alert(kol_only)

    with session_scope(session_factory) as session:
        session.query(SocialEvent).delete()
    add_social_memory(session_factory, tweet_id="dev-only", significance="medium")

    dev_only = await service.assess_token(wallet_id, TOKEN)
    assert dev_only is not None
    assert "• 社媒｜15m DEV+1" in format_attention_alert(dev_only)


@pytest.mark.asyncio
async def test_social_event_service_triggers_kol_and_keep_memory_only(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id)
    triggered: list[tuple[str, object | None]] = []

    class FakeMemoryProcessor:
        def __init__(self, keep: bool) -> None:
            self.keep = keep

        def process_event(self, event):  # noqa: ANN001
            return object() if self.keep else None

    service = SocialEventService(
        session_factory,
        memory_processor=FakeMemoryProcessor(keep=False),
        social_update_callback=lambda event, memory: triggered.append((event.author_type, memory)),
    )
    service.create_event_if_relevant(
        social_tweet("kol-trigger", username="kol", author_id="kol-author"),
        known_kol_usernames={"kol"},
    )
    service.create_event_if_relevant(
        social_tweet("project-no-memory", username="ProjectUser", author_id="project-author"),
    )

    assert [item[0] for item in triggered] == [AUTHOR_KOL]

    with session_scope(session_factory) as session:
        identity = SocialIdentity(
            chain="robinhood",
            token_address=TOKEN,
            symbol="WINK",
            identity_type="project_x",
            value="@ProjectUser",
            normalized_value="projectuser",
            source="test",
            source_field="test",
            confidence="HIGH",
            evidence_json=None,
            is_active=True,
            first_seen_at=utc_now(),
            last_verified_at=utc_now(),
            valid_from=utc_now(),
        )
        session.add(identity)

    keep_service = SocialEventService(
        session_factory,
        memory_processor=FakeMemoryProcessor(keep=True),
        social_update_callback=lambda event, memory: triggered.append((event.author_type, memory)),
    )
    keep_service.create_event_if_relevant(
        social_tweet("project-keep", username="ProjectUser", author_id="project-author"),
    )

    assert triggered[-1][0] == AUTHOR_PROJECT_X
    assert triggered[-1][1] is not None


@pytest.mark.asyncio
async def test_handle_social_update_assesses_and_notifies_through_attention_cooldown(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    add_watched(session_factory, wallet_id, usd_value="600")
    for index in range(7):
        add_social_event(session_factory, wallet_id, tweet_id=f"notify-social-{index}", author_id=f"notify-{index}")
    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )

    assessment = await service.handle_social_update(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.should_notify is True
    assert sent
    assert "社媒热度上升" in sent[0][1]


@pytest.mark.asyncio
async def test_social_query_failure_fails_closed_and_existing_attention_still_assesses(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, price="1", observed_at=now - timedelta(minutes=5))
    add_price_snapshot(session_factory, wallet_id, price="1.4", observed_at=now)
    add_token_intel(session_factory, wallet_id, market_cap="1000000", liquidity="100000", observed_at=now)

    class FailingSocialEvents:
        def build_social_facts(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("social down")

    class FailingSocialMemory:
        def get_recent_memories(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("memory down")

    service = AttentionEngineService(
        session_factory,
        FakeAttentionGmgn(),
        app_settings,
        notify,
        social_event_service=FailingSocialEvents(),
        social_memory_service=FailingSocialMemory(),
    )

    assessment = await service.assess_token(wallet_id, TOKEN)

    assert assessment is not None
    assert assessment.price_score > 0
    assert assessment.dev_modifier == 0
    evidence = json.loads(assessment.evidence_json)
    assert evidence["family_scores"][scoring.SOCIAL] == 0


@pytest.mark.parametrize(
    ("family", "signal", "expected"),
    [
        (scoring.PRICE, None, "价格异动"),
        (scoring.HOLDER, "holder_breadth", "持币人数异动"),
        (scoring.HOLDER, "holder_structure", "筹码异动"),
        (scoring.SMART_MONEY, None, "聪明钱异动"),
        (scoring.KOL, None, "KOL资金异动"),
        (scoring.LIQUIDITY, None, "流动性异动"),
        ("comprehensive", None, "综合异动"),
    ],
)
def test_format_directional_trigger_title_unknown_direction_fallbacks(family, signal, expected) -> None:
    assert format_directional_trigger_title(family, None, None, signal) == expected


def test_format_attention_alert_uses_primary_trigger_not_overall_direction() -> None:
    text = format_attention_alert(
        make_alert_assessment(
            primary_family=scoring.PRICE,
            primary_direction=scoring.POSITIVE,
            direction=scoring.MIXED,
        )
    )
    assert "价格上涨｜Mixed｜ATT 70" in text
    assert "信号分化｜Mixed｜ATT 70" not in text


def test_format_attention_alert_primary_smart_not_overall_positive() -> None:
    text = format_attention_alert(
        make_alert_assessment(
            primary_family=scoring.SMART_MONEY,
            primary_direction=scoring.NEGATIVE,
            direction=scoring.POSITIVE,
        )
    )
    assert "聪明钱流出｜Positive｜ATT 70" in text
    assert "综合走强｜Positive｜ATT 70" not in text


def test_format_attention_alert_old_assessment_missing_primary_direction_fallback() -> None:
    text = format_attention_alert(
        make_alert_assessment(
            primary_family=scoring.PRICE,
            evidence={"display_facts": {"price": {"window_minutes": 5, "change_pct": "18.6"}}},
        )
    )
    assert "价格异动｜Mixed｜ATT 70" in text


def test_short_address_and_copy_button() -> None:
    ca = "0xb6ec6e6e58354d956ef1a2ed6693d5c7056298d2"
    assessment = make_alert_assessment(token_address=ca)

    assert short_address(ca) == "0xb6e...8d2"
    markup = build_attention_copy_markup(assessment)
    button = markup.inline_keyboard[0][0]
    assert button.text == "📋 0xb6e...8d2"
    assert button.copy_text is not None
    assert button.copy_text.text == ca


@pytest.mark.asyncio
async def test_attention_notification_sends_reply_markup(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    captured: list[tuple[int, str, object]] = []

    async def notify_with_markup(chat_id: int, text: str, reply_markup=None) -> None:
        captured.append((chat_id, text, reply_markup))

    add_watched(session_factory, wallet_id)
    service = AttentionEngineService(session_factory, FakeAttentionGmgn(), app_settings, notify_with_markup)
    assessment = await service.simulate_assessment(
        wallet_id,
        TOKEN,
        symbol="WINK",
        family_scores={scoring.PRICE: 40},
        usd_value=Decimal("600"),
        direction=scoring.NEGATIVE,
    )

    assert await service._notify_assessment(assessment) is True

    assert len(captured) == 1
    _, text, reply_markup = captured[0]
    assert "WINK" in text
    assert reply_markup is not None
    button = reply_markup.inline_keyboard[0][0]
    assert button.text == f"📋 {short_address(TOKEN)}"
    assert button.copy_text.text == TOKEN


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


@pytest.mark.asyncio
async def test_token_snapshot_scan_syncs_social_identity_without_extra_gmgn_calls(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10), symbol="ROBBIE")
    add_price_snapshot(session_factory, wallet_id, observed_at=now, usd_value="50", symbol="ROBBIE")
    overview = GmgnTokenOverview(
        chain="robinhood",
        token_address=TOKEN,
        symbol="ROBBIE",
        name="ROBBIE",
        price_usd=Decimal("1"),
        market_cap_usd=Decimal("1000000"),
        fdv_usd=None,
        liquidity_usd=Decimal("100000"),
        holder_count=500,
        top10_holder_rate=None,
        smart_money_count=None,
        kol_count=None,
        creator_address="0xbfcc000000000000000000000000000000000001",
        created_at=None,
        twitter="@RobbieOnRH",
        telegram=None,
        website=None,
        raw={},
    )
    gmgn = FakeOverviewGmgn(overview)
    service = AttentionEngineService(session_factory, gmgn, app_settings, notify)

    async def no_assessment(wallet_id: int, token_address: str):  # noqa: ANN001
        return None

    service.assess_token = no_assessment
    result = await service.scan_token_snapshots()

    assert result.gmgn_calls == 1
    assert gmgn.overview_calls == 1
    with session_scope(session_factory) as session:
        identities = list(session.scalars(select(SocialIdentity).order_by(SocialIdentity.identity_type.asc())))
        snapshots = list(session.scalars(select(TokenIntelligenceSnapshot)))
    assert {(row.identity_type, row.normalized_value) for row in identities} == {
        ("dev_wallet", "0xbfcc000000000000000000000000000000000001"),
        ("project_x", "robbieonrh"),
    }
    assert len(snapshots) == 1


@pytest.mark.asyncio
async def test_social_identity_sync_failure_does_not_block_token_snapshot(ctx) -> None:
    app_settings, session_factory, sent, notify, wallet_id = ctx
    now = utc_now()
    start_watch_session(session_factory, wallet_id, started_at=now - timedelta(minutes=10))
    add_price_snapshot(session_factory, wallet_id, observed_at=now, usd_value="50")
    overview = GmgnTokenOverview(
        chain="robinhood",
        token_address=TOKEN,
        symbol="WINK",
        name="WINK",
        price_usd=Decimal("1"),
        market_cap_usd=Decimal("1000000"),
        fdv_usd=None,
        liquidity_usd=Decimal("100000"),
        holder_count=500,
        top10_holder_rate=None,
        smart_money_count=None,
        kol_count=None,
        creator_address=None,
        created_at=None,
        twitter="@Wink",
        telegram=None,
        website=None,
        raw={},
    )

    class FailingIdentityService:
        def sync_from_token_overview(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("identity down")

    service = AttentionEngineService(
        session_factory,
        FakeOverviewGmgn(overview),
        app_settings,
        notify,
        social_identity_service=FailingIdentityService(),
    )

    async def no_assessment(wallet_id: int, token_address: str):  # noqa: ANN001
        return None

    service.assess_token = no_assessment
    result = await service.scan_token_snapshots()

    assert result.errors is None
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(TokenIntelligenceSnapshot)))) == 1


def test_fingerprint_is_stable() -> None:
    assert fingerprint_event("a", "b", 1) == fingerprint_event("a", "b", 1)
    assert fingerprint_event("a", "b", 1) != fingerprint_event("a", "b", 2)
