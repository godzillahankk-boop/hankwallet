from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import sessionmaker
from telegram import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup

from app.bot.formatters import format_market_cap
from app.core.config import Settings
from app.db.database import session_scope
from app.db.models import (
    AttentionAlertState,
    AttentionAssessment,
    IntelligenceEvent,
    PriceSnapshot,
    TokenIntelligenceSnapshot,
    TokenWatchState,
    TopHolderPeak,
    TopHolderSnapshot,
    Wallet,
)
from app.services import attention_scoring as scoring
from app.services.gmgn_client import (
    GmgnClient,
    GmgnHolder,
    GmgnMarketSignal,
    GmgnTokenOverview,
    GmgnTrackTrade,
)
from app.services.holding_market_cap import HoldingMarketContext, HoldingMarketContextCache
from app.services.price_quality import PRICE_QUALITY_VALID
from app.services.social_event_service import SocialEventService
from app.services.social_identity_service import SocialIdentityService
from app.services.social_memory_service import SocialMemoryService
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

Notifier = Callable[..., Awaitable[None]]

MARKET_SIGNAL_GROUPS = [
    {"signal_type": [6, 7]},
    {"signal_type": [12]},
    {"signal_type": [20]},
]

ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS = (0, 1, 2)
HOLDER_COUNT_WINDOW_MINUTES = 30
LIQUIDITY_WINDOW_MINUTES = 60
TOP10_DISPLAY_MIN_ABS_CHANGE_PCT = Decimal("10")
TOP10_DISPLAY_MAX_WINDOW_MINUTES = 60
CONTINUED_EXTREME_PRICE_MOVE_THRESHOLD_PCT = Decimal("50")
CONTINUED_EXTREME_PRICE_MOVE_COOLDOWN_MINUTES = 15

MARKET_EXPANSION = "market_expansion"
MINORITY_DRIVEN = "minority_driven"
STRUCTURAL_DETERIORATION = "structural_deterioration"
SUPPORT_EMERGING = "support_emerging"
SIGNAL_DIVERGENCE = "signal_divergence"
NO_CLEAR_CHANGE = "no_clear_change"

POSITION_LABELS_CN = {
    MARKET_EXPANSION: "市场扩散增强",
    MINORITY_DRIVEN: "少数资金推动",
    STRUCTURAL_DETERIORATION: "结构性恶化",
    SUPPORT_EMERGING: "出现承接",
    SIGNAL_DIVERGENCE: "信号分化",
    NO_CLEAR_CHANGE: "暂无明显结构变化",
}


@dataclass(frozen=True)
class WatchedToken:
    wallet_id: int
    chat_id: int
    chain: str
    token_address: str
    symbol: str | None
    usd_value: Decimal | None
    watch_started_at: datetime


@dataclass
class AttentionRunResult:
    events_created: int = 0
    assessments_created: int = 0
    notifications_sent: int = 0
    gmgn_calls: int = 0
    errors: list[str] | None = None

    def add_error(self, error: Exception) -> None:
        if self.errors is None:
            self.errors = []
        self.errors.append(str(error))


@dataclass(frozen=True)
class NotificationDecision:
    should_notify: bool
    reason: str
    notification_context: dict[str, Any]


@dataclass(frozen=True)
class HolderCountFact:
    score: int
    direction: str
    window_minutes: int
    baseline_count: int
    current_count: int
    delta_count: int
    change_pct: Decimal


@dataclass(frozen=True)
class FeedFamilyFact:
    family: str
    score: int
    direction: str
    window_minutes: int
    buy_wallets: int
    sell_wallets: int
    net_directional_wallets: int
    buy_usd: Decimal | None
    sell_usd: Decimal | None
    net_usd: Decimal | None
    usd_complete: bool
    buy_activity_count: int
    sell_activity_count: int


@dataclass(frozen=True)
class LiquidityFact:
    score: int
    direction: str
    window_minutes: int
    baseline_usd: Decimal
    current_usd: Decimal
    change_pct: Decimal


@dataclass(frozen=True)
class StructureSignal:
    family: str
    direction: str
    strength: int
    value: str | None
    reason: str


@dataclass(frozen=True)
class PositionIntelligenceResult:
    label: str
    label_cn: str
    summary: str
    positive_drivers: list[str]
    negative_drivers: list[str]
    neutral_drivers: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "label_cn": self.label_cn,
            "summary": self.summary,
            "positive_drivers": self.positive_drivers,
            "negative_drivers": self.negative_drivers,
            "neutral_drivers": self.neutral_drivers,
        }


@dataclass(frozen=True)
class SocialAttentionFact:
    window_minutes: int
    unique_kols: int
    kol_posts: int
    meaningful_dev_updates: int
    highest_dev_significance: str | None
    latest_dev_tweet_url: str | None
    latest_dev_memory_id: int | None
    x_kol_heat_score: int
    dev_update_score: int
    social_score: int
    primary_social_signal: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_minutes": self.window_minutes,
            "unique_kols": self.unique_kols,
            "kol_posts": self.kol_posts,
            "meaningful_dev_updates": self.meaningful_dev_updates,
            "highest_dev_significance": self.highest_dev_significance,
            "latest_dev_tweet_url": self.latest_dev_tweet_url,
            "latest_dev_memory_id": self.latest_dev_memory_id,
            "x_kol_heat_score": self.x_kol_heat_score,
            "dev_update_score": self.dev_update_score,
            "social_score": self.social_score,
            "primary_social_signal": self.primary_social_signal,
        }


class AttentionEngineService:
    def __init__(
        self,
        session_factory: sessionmaker,
        gmgn_client: GmgnClient,
        settings: Settings,
        notifier: Notifier | None = None,
        social_identity_service: SocialIdentityService | None = None,
        social_event_service: SocialEventService | None = None,
        social_memory_service: SocialMemoryService | None = None,
        market_context_cache: HoldingMarketContextCache | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.gmgn_client = gmgn_client
        self.settings = settings
        self.notifier = notifier
        self.social_identity_service = social_identity_service or SocialIdentityService(session_factory)
        self.social_event_service = social_event_service
        self.social_memory_service = social_memory_service
        self.market_context_cache = market_context_cache or HoldingMarketContextCache()
        self._notification_locks: dict[tuple[int, str], asyncio.Lock] = {}

    def set_social_services(
        self,
        *,
        social_event_service: SocialEventService | None = None,
        social_memory_service: SocialMemoryService | None = None,
    ) -> None:
        self.social_event_service = social_event_service
        self.social_memory_service = social_memory_service

    async def scan_smart_money_feed(self) -> AttentionRunResult:
        return await self._scan_feed("smartmoney", scoring.SMART_MONEY)

    async def scan_kol_feed(self) -> AttentionRunResult:
        return await self._scan_feed("kol", scoring.KOL)

    async def scan_market_signals(self) -> AttentionRunResult:
        result = AttentionRunResult()
        watched = self._watched_tokens_by_contract()
        if not watched:
            return result
        try:
            signals = await self.gmgn_client.get_market_signals(
                self.settings.default_chain,
                groups=MARKET_SIGNAL_GROUPS,
            )
            result.gmgn_calls += 1
        except Exception as exc:
            logger.warning("Attention Market Signal scan failed: %s", exc)
            result.add_error(exc)
            return result
        affected: set[tuple[int, str]] = set()
        for signal in signals:
            token = signal.token_address
            if not token or token not in watched:
                continue
            family, direction = self._market_signal_family(signal)
            event_at = _from_timestamp(signal.trigger_at)
            for watched_token in watched[token]:
                if event_at < watched_token.watch_started_at:
                    continue
                event = self._event_from_market_signal(watched_token, signal, family, direction)
                if self._insert_event(event):
                    result.events_created += 1
                    affected.add((watched_token.wallet_id, token))
        await self._assess_affected(affected, result)
        return result

    async def scan_token_snapshots(self) -> AttentionRunResult:
        result = AttentionRunResult()
        for watched_token in self._due_watched_tokens(
            TokenIntelligenceSnapshot,
            self.settings.attention_token_snapshot_due_seconds,
            self.settings.attention_token_snapshot_batch_size,
        ):
            try:
                overview = await self.gmgn_client.get_token_overview(
                    watched_token.chain,
                    watched_token.token_address,
                )
                result.gmgn_calls += 1
                self._save_token_snapshot(watched_token, overview)
                self._sync_social_identity_from_overview(watched_token, overview)
                assessment = await self.assess_token(watched_token.wallet_id, watched_token.token_address)
                if assessment:
                    result.assessments_created += 1
                    if assessment.should_notify and await self._notify_assessment(assessment):
                        result.notifications_sent += 1
            except Exception as exc:
                logger.warning("Attention token snapshot failed token=%s: %s", watched_token.token_address, exc)
                result.add_error(exc)
        await self.cleanup_old_snapshots()
        return result

    async def scan_top_holder_snapshots(self) -> AttentionRunResult:
        result = AttentionRunResult()
        for watched_token in self._due_watched_tokens(
            TopHolderSnapshot,
            self.settings.attention_top_holder_due_seconds,
            self.settings.attention_top_holder_batch_size,
        ):
            try:
                holders = await self.gmgn_client.get_token_holders(
                    watched_token.chain,
                    watched_token.token_address,
                    limit=20,
                    order_by="amount_percentage",
                    direction="desc",
                )
                result.gmgn_calls += 1
                self._save_top_holder_snapshots(watched_token, holders)
                assessment = await self.assess_token(watched_token.wallet_id, watched_token.token_address)
                if assessment:
                    result.assessments_created += 1
                    if assessment.should_notify and await self._notify_assessment(assessment):
                        result.notifications_sent += 1
            except Exception as exc:
                logger.warning("Attention top holder snapshot failed token=%s: %s", watched_token.token_address, exc)
                result.add_error(exc)
        await self.cleanup_old_snapshots()
        return result

    async def assess_token(
        self,
        wallet_id: int,
        token_address: str,
        *,
        allow_below_min_value: bool = False,
        assessment_trigger: str | None = None,
    ) -> AttentionAssessment | None:
        now = utc_now()
        with session_scope(self.session_factory) as session:
            watched = self._watched_token(session, wallet_id, token_address)
            if not watched or watched.usd_value is None:
                return None
            if watched.usd_value < self.settings.price_monitor_min_usd_value and not allow_below_min_value:
                return None
            latest_price = self._latest_price_snapshot(session, wallet_id, token_address, watched.watch_started_at)
            latest_intel = self._latest_token_snapshot(session, wallet_id, token_address, watched.watch_started_at)
            price_score, price_direction, price_change, price_window = self._price_family(
                session,
                wallet_id,
                token_address,
                latest_intel,
                watched.watch_started_at,
            )
            abnormality = self._abnormality(
                session,
                wallet_id,
                token_address,
                price_change,
                price_window,
                watched.watch_started_at,
            )
            holder_breadth, holder_breadth_direction = self._holder_breadth(
                session,
                wallet_id,
                token_address,
                latest_intel,
                watched.watch_started_at,
            )
            top_holder = self._top_holder_score(session, wallet_id, token_address)
            cluster = self._holder_cluster(session, wallet_id, token_address)
            holder_family = scoring.holder_family_score(holder_breadth, top_holder, cluster)
            holder_direction = self._holder_family_direction(
                holder_breadth,
                holder_breadth_direction,
                top_holder,
                cluster,
            )
            smart_fact = self._feed_family_fact(session, wallet_id, token_address, scoring.SMART_MONEY)
            smart_score, smart_direction = _feed_score_direction(smart_fact)
            kol_fact = self._feed_family_fact(session, wallet_id, token_address, scoring.KOL)
            kol_score, kol_direction = _feed_score_direction(kol_fact)
            liq_score, liq_direction = self._liquidity_family(
                session,
                wallet_id,
                token_address,
                latest_intel,
                watched.watch_started_at,
            )
            social_fact = self._social_attention_fact(watched)
            social_score = social_fact.social_score if social_fact else 0
            family_scores = {
                scoring.PRICE: price_score,
                scoring.HOLDER: holder_family,
                scoring.SMART_MONEY: smart_score,
                scoring.KOL: kol_score,
                scoring.LIQUIDITY: liq_score,
                scoring.SOCIAL: social_score,
            }
            assessment_scores = {
                "price_score": price_score,
                "holder_breadth_score": holder_breadth,
                "top_holder_score": top_holder,
                "holder_cluster_score": cluster,
                "holder_family_score": holder_family,
                "smart_money_score": smart_score,
                "kol_score": kol_score,
                "liquidity_score": liq_score,
                "social_score": social_score,
            }
            primary_family, primary_score = max(family_scores.items(), key=lambda item: item[1])
            if primary_score == 0:
                primary_family = None
            secondary = scoring.secondary_signal_score(family_scores, primary_family)
            exposure = scoring.position_exposure_score(watched.usd_value)
            base = min(100, primary_score + exposure + abnormality + secondary.score)
            dev_modifier = _dev_modifier_for_social_fact(social_fact)
            dev_modifier_reason = "high_dev_update" if dev_modifier else None
            final = scoring.final_attention_score(base, dev_modifier)
            level = scoring.attention_level(final)
            direction = scoring.combine_direction(
                [
                    price_direction if price_score else scoring.NEUTRAL,
                    holder_direction if holder_family else scoring.NEUTRAL,
                    smart_direction if smart_score else scoring.NEUTRAL,
                    kol_direction if kol_score else scoring.NEUTRAL,
                    liq_direction if liq_score else scoring.NEUTRAL,
                ]
            )
            primary_signal = _primary_signal_for_family(
                primary_family,
                holder_breadth,
                top_holder,
                cluster,
                social_fact,
            )
            primary_direction = _primary_direction_for_family(
                primary_family,
                {
                    scoring.PRICE: price_direction,
                    scoring.HOLDER: _holder_primary_direction(primary_signal, holder_breadth_direction),
                    scoring.SMART_MONEY: _feed_primary_display_direction(smart_fact, smart_direction),
                    scoring.KOL: _feed_primary_display_direction(kol_fact, kol_direction),
                    scoring.LIQUIDITY: liq_direction,
                    scoring.SOCIAL: scoring.NEUTRAL,
                },
                direction,
            )
            market_context = self.market_context_cache.get(
                wallet_id,
                token_address,
                watch_started_at=watched.watch_started_at,
                now=now,
                max_age_seconds=180,
            )
            evidence = self._evidence(
                session,
                wallet_id,
                token_address,
                latest_price,
                latest_intel,
                family_scores,
                price_change,
                price_window,
                watched.watch_started_at,
                assessment_trigger,
                assessment_scores,
                primary_family,
                primary_direction,
                primary_signal,
                social_fact,
                dev_modifier_reason,
                market_context,
            )
            notification_decision = self._notification_decision(
                session,
                wallet_id,
                token_address,
                level,
                final,
                direction,
                family_scores,
                now,
                current_price_usd=_decimal_or_none(latest_price.price_usd) if latest_price else None,
                watch_started_at=watched.watch_started_at,
            )
            evidence["notification_context"] = notification_decision.notification_context
            should_notify = notification_decision.should_notify
            if level in {scoring.WARNING, scoring.CRITICAL}:
                logger.info(
                    "Attention notification decision wallet_id=%s token=%s score=%s level=%s reason=%s",
                    wallet_id,
                    token_address,
                    final,
                    level,
                    notification_decision.reason,
                )
            assessment = AttentionAssessment(
                wallet_id=wallet_id,
                token_address=token_address,
                symbol=watched.symbol,
                assessed_at=now,
                price_score=price_score,
                holder_breadth_score=holder_breadth,
                top_holder_score=top_holder,
                holder_cluster_score=cluster,
                holder_family_score=holder_family,
                smart_money_score=smart_score,
                kol_score=kol_score,
                liquidity_score=liq_score,
                primary_family=primary_family,
                primary_event_score=primary_score,
                position_exposure_score=exposure,
                abnormality_score=abnormality,
                secondary_family_1=secondary.family_1,
                secondary_family_1_score=secondary.family_1_score,
                secondary_family_2=secondary.family_2,
                secondary_family_2_score=secondary.family_2_score,
                secondary_signal_score=secondary.score,
                base_attention_score=base,
                dev_modifier=dev_modifier,
                final_attention_score=final,
                attention_level=level,
                direction=direction,
                should_notify=should_notify,
                evidence_json=json.dumps(evidence, ensure_ascii=False, default=str),
            )
            session.add(assessment)
            session.flush()
            session.expunge(assessment)
            return assessment

    async def handle_price_snapshot_update(
        self,
        wallet_id: int,
        token_address: str,
        *,
        confirmed_pending_usd_threshold_crossing: bool = False,
    ) -> AttentionAssessment | None:
        trigger: str | None = None
        allow_below_min_value = False
        with session_scope(self.session_factory) as session:
            watched = self._watched_token(session, wallet_id, token_address)
            if not watched:
                return None
            latest_price = self._latest_price_snapshot(session, wallet_id, token_address, watched.watch_started_at)
            if not latest_price:
                return None
            current_usd = _decimal_or_none(latest_price.usd_value)
            if current_usd is None:
                return None
            previous = self._previous_price_snapshot(
                session,
                wallet_id,
                token_address,
                watched.watch_started_at,
                latest_price.observed_at,
            )
            previous_usd = _decimal_or_none(previous.usd_value if previous else None)
            min_usd = self.settings.price_monitor_min_usd_value
            if current_usd >= min_usd:
                latest_intel = self._latest_token_snapshot(session, wallet_id, token_address, watched.watch_started_at)
                price_score, _, _, _ = self._price_family(
                    session,
                    wallet_id,
                    token_address,
                    latest_intel,
                    watched.watch_started_at,
                )
                if price_score <= 0:
                    return None
                trigger = "price_threshold"
            elif (
                confirmed_pending_usd_threshold_crossing
                or previous_usd is not None
                and previous_usd >= min_usd
            ):
                allow_below_min_value = True
                trigger = "usd_threshold_crossing"
            else:
                return None

        assessment = await self.assess_token(
            wallet_id,
            token_address,
            allow_below_min_value=allow_below_min_value,
            assessment_trigger=trigger,
        )
        if assessment and assessment.should_notify:
            await self._notify_assessment(assessment)
        return assessment

    async def handle_social_update(
        self,
        wallet_id: int,
        token_address: str,
    ) -> AttentionAssessment | None:
        assessment = await self.assess_token(
            wallet_id,
            token_address,
            assessment_trigger="social_update",
        )
        if assessment and assessment.should_notify:
            await self._notify_assessment(assessment)
        return assessment

    async def simulate_assessment(
        self,
        wallet_id: int,
        token_address: str,
        *,
        symbol: str,
        family_scores: dict[str, int],
        usd_value: Decimal,
        direction: str,
    ) -> AttentionAssessment:
        secondary = scoring.secondary_signal_score(
            family_scores,
            max(family_scores.items(), key=lambda item: item[1])[0],
        )
        primary_family, primary_score = max(family_scores.items(), key=lambda item: item[1])
        exposure = scoring.position_exposure_score(usd_value)
        base = min(100, primary_score + exposure + 5 + secondary.score)
        final = scoring.final_attention_score(base, 0)
        return AttentionAssessment(
            wallet_id=wallet_id,
            token_address=token_address,
            symbol=symbol,
            assessed_at=utc_now(),
            price_score=family_scores.get(scoring.PRICE, 0),
            holder_breadth_score=0,
            top_holder_score=0,
            holder_cluster_score=0,
            holder_family_score=family_scores.get(scoring.HOLDER, 0),
            smart_money_score=family_scores.get(scoring.SMART_MONEY, 0),
            kol_score=family_scores.get(scoring.KOL, 0),
            liquidity_score=family_scores.get(scoring.LIQUIDITY, 0),
            primary_family=primary_family,
            primary_event_score=primary_score,
            position_exposure_score=exposure,
            abnormality_score=5,
            secondary_family_1=secondary.family_1,
            secondary_family_1_score=secondary.family_1_score,
            secondary_family_2=secondary.family_2,
            secondary_family_2_score=secondary.family_2_score,
            secondary_signal_score=secondary.score,
            base_attention_score=base,
            dev_modifier=0,
            final_attention_score=final,
            attention_level=scoring.attention_level(final),
            direction=direction,
            should_notify=True,
            evidence_json=json.dumps({"simulate": True}, ensure_ascii=False),
        )

    async def send_test_assessment(self, assessment: AttentionAssessment, chat_id: int) -> bool:
        if not self.notifier:
            raise RuntimeError("telegram_notifier_not_configured")
        await self._call_notifier(chat_id, format_attention_alert(assessment), build_attention_copy_markup(assessment))
        return True

    def latest_assessment_for_symbol(self, telegram_user_id: int, symbol: str) -> list[AttentionAssessment]:
        target = symbol.strip().upper()
        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    select(AttentionAssessment)
                    .join(Wallet, Wallet.id == AttentionAssessment.wallet_id)
                    .join(
                        TokenWatchState,
                        (TokenWatchState.wallet_id == AttentionAssessment.wallet_id)
                        & (TokenWatchState.token_address == AttentionAssessment.token_address)
                        & (TokenWatchState.active.is_(True)),
                    )
                    .where(Wallet.user.has(telegram_user_id=telegram_user_id))
                    .where(func.upper(AttentionAssessment.symbol) == target)
                    .where(AttentionAssessment.assessed_at >= TokenWatchState.started_at)
                    .order_by(AttentionAssessment.assessed_at.desc())
                )
            )
            assessments = [row[0] for row in rows]
            seen: set[tuple[int, str]] = set()
            result = []
            for assessment in assessments:
                key = (assessment.wallet_id, assessment.token_address)
                if key in seen:
                    continue
                seen.add(key)
                session.expunge(assessment)
                result.append(assessment)
            return result

    async def _scan_feed(self, feed_type: str, family: str) -> AttentionRunResult:
        result = AttentionRunResult()
        watched = self._watched_tokens_by_contract()
        if not watched:
            return result
        try:
            trades = await self.gmgn_client.get_track_feed(feed_type, chain=self.settings.default_chain, limit=200)
            result.gmgn_calls += 1
        except Exception as exc:
            logger.warning("Attention %s feed scan failed: %s", feed_type, exc)
            result.add_error(exc)
            return result
        affected: set[tuple[int, str]] = set()
        for trade in trades:
            token = trade.token_address
            if not token or token not in watched:
                continue
            event_at = _from_timestamp(trade.timestamp)
            for watched_token in watched[token]:
                if event_at < watched_token.watch_started_at:
                    continue
                event = self._event_from_trade(watched_token, trade, family, feed_type)
                if self._insert_event(event):
                    result.events_created += 1
                    affected.add((watched_token.wallet_id, token))
        await self._assess_affected(affected, result)
        return result

    async def _assess_affected(self, affected: set[tuple[int, str]], result: AttentionRunResult) -> None:
        for wallet_id, token_address in sorted(affected):
            try:
                assessment = await self.assess_token(wallet_id, token_address)
                if not assessment:
                    continue
                result.assessments_created += 1
                if assessment.should_notify and await self._notify_assessment(assessment):
                    result.notifications_sent += 1
            except Exception as exc:
                logger.warning(
                    "Attention affected token failed wallet_id=%s token=%s: %s",
                    wallet_id,
                    token_address,
                    exc,
                )
                result.add_error(exc)
                continue

    def _watched_tokens_by_contract(self) -> dict[str, list[WatchedToken]]:
        watched: dict[str, list[WatchedToken]] = defaultdict(list)
        with session_scope(self.session_factory) as session:
            for state, wallet, snapshot in self._watched_token_rows(session):
                usd_value = _decimal_or_none(snapshot.usd_value)
                if usd_value is None or usd_value < self.settings.price_monitor_min_usd_value:
                    continue
                watched[state.token_address].append(
                    WatchedToken(
                        wallet_id=wallet.id,
                        chat_id=wallet.user.telegram_chat_id,
                        chain=state.chain,
                        token_address=state.token_address,
                        symbol=state.symbol,
                        usd_value=usd_value,
                        watch_started_at=state.started_at,
                    )
                )
        return dict(watched)

    def _due_watched_tokens(self, snapshot_model, interval_seconds: int, limit: int) -> list[WatchedToken]:
        cutoff = utc_now() - timedelta(seconds=interval_seconds)
        due: list[tuple[datetime | None, WatchedToken]] = []
        with session_scope(self.session_factory) as session:
            for state, wallet, snapshot in self._watched_token_rows(session):
                usd_value = _decimal_or_none(snapshot.usd_value)
                if usd_value is None or usd_value < self.settings.price_monitor_min_usd_value:
                    continue
                last_at = session.scalar(
                    select(func.max(snapshot_model.observed_at)).where(
                        snapshot_model.wallet_id == wallet.id,
                        snapshot_model.token_address == state.token_address,
                        snapshot_model.observed_at >= state.started_at,
                    )
                )
                if last_at and last_at > cutoff:
                    continue
                due.append(
                    (
                        last_at,
                        WatchedToken(
                            wallet_id=wallet.id,
                            chat_id=wallet.user.telegram_chat_id,
                            chain=state.chain,
                            token_address=state.token_address,
                            symbol=state.symbol,
                            usd_value=usd_value,
                            watch_started_at=state.started_at,
                        ),
                    )
                )
        due.sort(
            key=lambda item: (
                item[0] is not None,
                item[0] or datetime.min,
                item[1].wallet_id,
                item[1].token_address,
            )
        )
        return [watched for _, watched in due[: max(1, limit)]]

    def _watched_token_rows(self, session):
        latest_price_observed = (
            select(func.max(PriceSnapshot.observed_at))
            .where(
                PriceSnapshot.wallet_id == TokenWatchState.wallet_id,
                PriceSnapshot.token_address == TokenWatchState.token_address,
                PriceSnapshot.observed_at >= TokenWatchState.started_at,
                _valid_price_snapshot_clause(),
            )
            .correlate(TokenWatchState)
            .scalar_subquery()
        )
        return session.execute(
            select(TokenWatchState, Wallet, PriceSnapshot)
            .join(Wallet, Wallet.id == TokenWatchState.wallet_id)
            .join(
                PriceSnapshot,
                (PriceSnapshot.wallet_id == TokenWatchState.wallet_id)
                & (PriceSnapshot.token_address == TokenWatchState.token_address)
                & (PriceSnapshot.observed_at == latest_price_observed)
                & _valid_price_snapshot_clause(),
            )
            .where(TokenWatchState.active.is_(True), Wallet.is_active.is_(True))
            .order_by(TokenWatchState.last_seen_at.asc(), TokenWatchState.id.asc())
        )

    def _watched_token(self, session, wallet_id: int, token_address: str) -> WatchedToken | None:
        row = session.execute(
            select(TokenWatchState, Wallet)
            .join(Wallet, Wallet.id == TokenWatchState.wallet_id)
            .where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token_address,
                TokenWatchState.active.is_(True),
                Wallet.is_active.is_(True),
            )
        ).first()
        if not row:
            return None
        state, wallet = row
        snapshot = self._latest_price_snapshot(session, wallet_id, token_address, state.started_at)
        usd_value = _decimal_or_none(snapshot.usd_value if snapshot else None)
        return WatchedToken(
            wallet_id=wallet.id,
            chat_id=wallet.user.telegram_chat_id,
            chain=state.chain,
            token_address=state.token_address,
            symbol=state.symbol,
            usd_value=usd_value,
            watch_started_at=state.started_at,
        )

    def _save_token_snapshot(self, watched: WatchedToken, overview: GmgnTokenOverview) -> None:
        with session_scope(self.session_factory) as session:
            session.add(
                TokenIntelligenceSnapshot(
                    wallet_id=watched.wallet_id,
                    chain=watched.chain,
                    token_address=watched.token_address,
                    symbol=overview.symbol or watched.symbol,
                    market_cap_usd=overview.market_cap_usd,
                    liquidity_usd=overview.liquidity_usd,
                    holder_count=overview.holder_count,
                    observed_at=utc_now(),
                )
            )

    def _sync_social_identity_from_overview(
        self,
        watched: WatchedToken,
        overview: GmgnTokenOverview,
    ) -> None:
        try:
            self.social_identity_service.sync_from_token_overview(watched, overview)
        except Exception as exc:
            logger.warning(
                "Social identity sync failed token=%s: %s",
                watched.token_address,
                exc,
            )

    def _save_top_holder_snapshots(self, watched: WatchedToken, holders: list[GmgnHolder]) -> None:
        now = utc_now()
        current_addresses: set[str] = set()
        with session_scope(self.session_factory) as session:
            for holder in holders[:20]:
                if not holder.wallet_address:
                    continue
                current_addresses.add(holder.wallet_address)
                session.add(
                    TopHolderSnapshot(
                        wallet_id=watched.wallet_id,
                        chain=watched.chain,
                        token_address=watched.token_address,
                        symbol=watched.symbol,
                        holder_address=holder.wallet_address,
                        balance=holder.balance,
                        hold_percentage=holder.hold_percentage,
                        usd_value=holder.usd_value,
                        dropped_out_top20=False,
                        observed_at=now,
                    )
                )
                peak = session.scalar(
                    select(TopHolderPeak).where(
                        TopHolderPeak.wallet_id == watched.wallet_id,
                        TopHolderPeak.token_address == watched.token_address,
                        TopHolderPeak.holder_address == holder.wallet_address,
                        TopHolderPeak.watch_started_at == watched.watch_started_at,
                    )
                )
                if not peak:
                    peak = TopHolderPeak(
                        wallet_id=watched.wallet_id,
                        token_address=watched.token_address,
                        holder_address=holder.wallet_address,
                        watch_started_at=watched.watch_started_at,
                        peak_balance=holder.balance,
                        peak_hold_percentage=holder.hold_percentage,
                    )
                    session.add(peak)
                    continue
                balance = _decimal_or_none(holder.balance)
                peak_balance = _decimal_or_none(peak.peak_balance)
                if balance is not None and (peak_balance is None or balance > peak_balance):
                    peak.peak_balance = balance
                hold_percentage = _decimal_or_none(holder.hold_percentage)
                peak_hold_percentage = _decimal_or_none(peak.peak_hold_percentage)
                if hold_percentage is not None and (peak_hold_percentage is None or hold_percentage > peak_hold_percentage):
                    peak.peak_hold_percentage = hold_percentage
            important_dropped = list(
                session.scalars(
                    select(TopHolderPeak).where(
                        TopHolderPeak.wallet_id == watched.wallet_id,
                        TopHolderPeak.token_address == watched.token_address,
                        TopHolderPeak.watch_started_at == watched.watch_started_at,
                        TopHolderPeak.peak_hold_percentage >= Decimal("0.01"),
                    )
                )
            )
            for peak in important_dropped:
                if peak.holder_address in current_addresses:
                    continue
                session.add(
                    TopHolderSnapshot(
                        wallet_id=watched.wallet_id,
                        chain=watched.chain,
                        token_address=watched.token_address,
                        symbol=watched.symbol,
                        holder_address=peak.holder_address,
                        balance=None,
                        hold_percentage=None,
                        usd_value=None,
                        dropped_out_top20=True,
                        observed_at=now,
                    )
                )

    def _insert_event(self, event: IntelligenceEvent) -> bool:
        with session_scope(self.session_factory) as session:
            exists = session.scalar(
                select(IntelligenceEvent.id).where(
                    IntelligenceEvent.event_fingerprint == event.event_fingerprint
                )
            )
            if exists:
                return False
            session.add(event)
        return True

    def _event_from_trade(self, watched: WatchedToken, trade: GmgnTrackTrade, family: str, source: str) -> IntelligenceEvent:
        side = (trade.side or "").lower()
        direction = scoring.POSITIVE if side == "buy" else scoring.NEGATIVE if side == "sell" else scoring.NEUTRAL
        event_at = _from_timestamp(trade.timestamp)
        payload = {
            "wallet": trade.wallet_address,
            "side": side,
            "token_amount": str(trade.token_amount) if trade.token_amount is not None else None,
            "usd_value": str(trade.usd_value) if trade.usd_value is not None else None,
            "price_usd": str(trade.price_usd) if trade.price_usd is not None else None,
            "tx_hash": trade.tx_hash,
            "open_or_close": trade.open_or_close,
            "wallet_tags": trade.wallet_tags,
            "twitter_username": trade.twitter_username,
        }
        fingerprint = fingerprint_event(
            source,
            trade.tx_hash,
            trade.wallet_address,
            watched.token_address,
            side,
            str(trade.token_amount),
            str(watched.wallet_id),
        )
        return IntelligenceEvent(
            wallet_id=watched.wallet_id,
            chain=watched.chain,
            token_address=watched.token_address,
            symbol=trade.symbol or watched.symbol,
            family=family,
            event_type=f"{source}_trade",
            direction=direction,
            severity_score=0,
            source=f"gmgn_{source}",
            source_event_id=trade.tx_hash,
            event_fingerprint=fingerprint,
            event_at=event_at,
            detected_at=utc_now(),
            payload_json=json.dumps(payload, ensure_ascii=False),
        )

    def _event_from_market_signal(
        self,
        watched: WatchedToken,
        signal: GmgnMarketSignal,
        family: str,
        direction: str,
    ) -> IntelligenceEvent:
        source_id = signal.event_id or f"{signal.signal_type}:{signal.token_address}:{signal.trigger_at}"
        fingerprint = fingerprint_event("gmgn_market_signal", source_id, str(watched.wallet_id))
        return IntelligenceEvent(
            wallet_id=watched.wallet_id,
            chain=watched.chain,
            token_address=watched.token_address,
            symbol=watched.symbol,
            family=family,
            event_type="market_signal",
            direction=direction,
            severity_score=0,
            source="gmgn_market_signal",
            source_event_id=source_id,
            event_fingerprint=fingerprint,
            event_at=_from_timestamp(signal.trigger_at),
            detected_at=utc_now(),
            payload_json=json.dumps(
                {
                    "signal_type": signal.signal_type,
                    "trigger_market_cap": str(signal.trigger_market_cap_usd),
                    "market_cap": str(signal.market_cap_usd),
                    "liquidity": str(signal.liquidity_usd),
                    "holder_count": signal.holder_count,
                },
                ensure_ascii=False,
            ),
        )

    def _market_signal_family(self, signal: GmgnMarketSignal) -> tuple[str, str]:
        if signal.signal_type in {6, 7}:
            return scoring.PRICE, scoring.POSITIVE
        if signal.signal_type == 12:
            return scoring.SMART_MONEY, scoring.POSITIVE
        if signal.signal_type == 20:
            return scoring.KOL, scoring.POSITIVE
        return scoring.PRICE, scoring.NEUTRAL

    def _latest_price_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at: datetime | None = None,
    ) -> PriceSnapshot | None:
        query = select(PriceSnapshot).where(
            PriceSnapshot.wallet_id == wallet_id,
            PriceSnapshot.token_address == token_address,
            _valid_price_snapshot_clause(),
        )
        if watch_started_at is not None:
            query = query.where(PriceSnapshot.observed_at >= watch_started_at)
        return session.scalar(query.order_by(PriceSnapshot.observed_at.desc()))

    def _previous_price_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at: datetime,
        before_observed_at: datetime,
    ) -> PriceSnapshot | None:
        return session.scalar(
            select(PriceSnapshot)
            .where(
                PriceSnapshot.wallet_id == wallet_id,
                PriceSnapshot.token_address == token_address,
                PriceSnapshot.observed_at >= watch_started_at,
                PriceSnapshot.observed_at < before_observed_at,
                _valid_price_snapshot_clause(),
            )
            .order_by(PriceSnapshot.observed_at.desc())
        )

    def _latest_token_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at: datetime | None = None,
    ) -> TokenIntelligenceSnapshot | None:
        query = select(TokenIntelligenceSnapshot).where(
            TokenIntelligenceSnapshot.wallet_id == wallet_id,
            TokenIntelligenceSnapshot.token_address == token_address,
        )
        if watch_started_at is not None:
            query = query.where(TokenIntelligenceSnapshot.observed_at >= watch_started_at)
        return session.scalar(query.order_by(TokenIntelligenceSnapshot.observed_at.desc()))

    def _baseline_token_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        minutes: int,
        observed_at: datetime | None = None,
        watch_started_at: datetime | None = None,
    ) -> TokenIntelligenceSnapshot | None:
        target = (observed_at or utc_now()) - timedelta(minutes=minutes)
        lower_bound = target - timedelta(minutes=10)
        if watch_started_at is not None:
            lower_bound = max(lower_bound, watch_started_at)
        candidates = list(
            session.scalars(
                select(TokenIntelligenceSnapshot).where(
                    TokenIntelligenceSnapshot.wallet_id == wallet_id,
                    TokenIntelligenceSnapshot.token_address == token_address,
                    TokenIntelligenceSnapshot.observed_at >= lower_bound,
                    TokenIntelligenceSnapshot.observed_at <= target + timedelta(minutes=10),
                )
            )
        )
        if not candidates:
            return None
        return min(candidates, key=lambda snapshot: abs((snapshot.observed_at - target).total_seconds()))

    def _price_family(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> tuple[int, str, Decimal | None, int | None]:
        if not latest_intel:
            return 0, scoring.NEUTRAL, None, None
        market_cap = _decimal_or_none(latest_intel.market_cap_usd)
        liquidity = _decimal_or_none(latest_intel.liquidity_usd)
        best_score = 0
        best_direction = scoring.NEUTRAL
        best_change: Decimal | None = None
        best_window: int | None = None
        latest = self._latest_price_snapshot(session, wallet_id, token_address, watch_started_at)
        if not latest or latest.price_usd is None:
            return 0, scoring.NEUTRAL, None, None
        latest_price = _decimal_or_none(latest.price_usd)
        if latest_price is None or latest_price <= 0:
            return 0, scoring.NEUTRAL, None, None
        for window in (5, 15, 60):
            baseline = self._baseline_price_snapshot(
                session,
                wallet_id,
                token_address,
                latest.observed_at,
                window,
                watch_started_at,
            )
            if not baseline:
                continue
            base_price = _decimal_or_none(baseline.price_usd)
            if base_price is None or base_price <= 0:
                continue
            change = ((latest_price - base_price) / base_price) * Decimal("100")
            score = scoring.price_impact_score(change, window, market_cap, liquidity)
            if score > best_score:
                best_score = score
                best_direction = scoring.POSITIVE if change > 0 else scoring.NEGATIVE
                best_change = change
                best_window = window
        return best_score, best_direction, best_change, best_window

    def _baseline_price_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        observed_at: datetime,
        window: int,
        watch_started_at: datetime | None = None,
    ) -> PriceSnapshot | None:
        target = observed_at - timedelta(minutes=window)
        tolerance = timedelta(seconds={5: 75, 15: 90, 60: 120}.get(window, 90))
        lower_bound = target - tolerance
        if watch_started_at is not None:
            lower_bound = max(lower_bound, watch_started_at)
        candidates = list(
            session.scalars(
                select(PriceSnapshot).where(
                    PriceSnapshot.wallet_id == wallet_id,
                    PriceSnapshot.token_address == token_address,
                    PriceSnapshot.observed_at >= lower_bound,
                    PriceSnapshot.observed_at <= target + tolerance,
                    _valid_price_snapshot_clause(),
                )
            )
        )
        if not candidates:
            return None
        return min(candidates, key=lambda snapshot: abs((snapshot.observed_at - target).total_seconds()))

    def _abnormality(
        self,
        session,
        wallet_id: int,
        token_address: str,
        current_change: Decimal | None,
        window_minutes: int | None,
        watch_started_at: datetime | None = None,
    ) -> int:
        if current_change is None or window_minutes is None:
            return 0
        latest = self._latest_price_snapshot(session, wallet_id, token_address, watch_started_at)
        if not latest:
            return 0
        rows = list(
            session.scalars(
                select(PriceSnapshot)
                .where(
                    PriceSnapshot.wallet_id == wallet_id,
                    PriceSnapshot.token_address == token_address,
                    PriceSnapshot.observed_at >= latest.observed_at - timedelta(hours=24),
                    PriceSnapshot.observed_at <= latest.observed_at,
                    _valid_price_snapshot_clause(),
                )
                .order_by(PriceSnapshot.observed_at.asc())
            )
        )
        changes: list[Decimal] = []
        for row in rows:
            if row.observed_at >= latest.observed_at:
                continue
            price = _decimal_or_none(row.price_usd)
            if price is None or price <= 0:
                continue
            baseline = self._nearest_price_snapshot_from_rows(rows, row.observed_at, window_minutes)
            if not baseline:
                continue
            base_price = _decimal_or_none(baseline.price_usd)
            if base_price is None or base_price <= 0:
                continue
            changes.append(abs(((price - base_price) / base_price) * Decimal("100")))
        return scoring.abnormality_score(abs(current_change), changes)

    def _nearest_price_snapshot_from_rows(
        self,
        rows: list[PriceSnapshot],
        observed_at: datetime,
        window: int,
    ) -> PriceSnapshot | None:
        target = observed_at - timedelta(minutes=window)
        tolerance = timedelta(seconds={5: 75, 15: 90, 60: 120}.get(window, 90))
        candidates = [
            row
            for row in rows
            if target - tolerance <= row.observed_at <= target + tolerance
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda snapshot: abs((snapshot.observed_at - target).total_seconds()))

    def _holder_breadth(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> tuple[int, str]:
        fact = self._holder_count_fact(session, wallet_id, token_address, latest_intel, watch_started_at)
        if not fact:
            return 0, scoring.NEUTRAL
        return fact.score, fact.direction

    def _holder_count_fact(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> HolderCountFact | None:
        if not latest_intel:
            return None
        baseline = self._baseline_token_snapshot(
            session,
            wallet_id,
            token_address,
            HOLDER_COUNT_WINDOW_MINUTES,
            latest_intel.observed_at,
            watch_started_at,
        )
        if latest_intel.holder_count is None or not baseline or baseline.holder_count is None or baseline.holder_count <= 0:
            return None
        score = scoring.holder_breadth_score(
            latest_intel.holder_count,
            baseline.holder_count,
        )
        delta = latest_intel.holder_count - baseline.holder_count
        change_pct = (Decimal(delta) / Decimal(baseline.holder_count)) * Decimal("100") if baseline.holder_count else Decimal("0")
        direction = scoring.NEUTRAL
        if latest_intel.holder_count > baseline.holder_count:
            direction = scoring.POSITIVE
        elif latest_intel.holder_count < baseline.holder_count:
            direction = scoring.NEGATIVE
        return HolderCountFact(
            score=score,
            direction=direction if score else scoring.NEUTRAL,
            window_minutes=HOLDER_COUNT_WINDOW_MINUTES,
            baseline_count=baseline.holder_count,
            current_count=latest_intel.holder_count,
            delta_count=delta,
            change_pct=change_pct,
        )

    def _holder_family_direction(self, breadth: int, breadth_direction: str, top_holder: int, cluster: int) -> str:
        directions: list[str] = []
        if breadth >= 10 and breadth_direction != scoring.NEUTRAL:
            directions.append(breadth_direction)
        if top_holder >= 10:
            directions.append(scoring.NEGATIVE)
        if cluster >= 10:
            directions.append(scoring.NEGATIVE)
        return scoring.combine_direction(directions)

    def _top_holder_score(self, session, wallet_id: int, token_address: str) -> int:
        watch_started_at = self._watch_started_at(session, wallet_id, token_address)
        if not watch_started_at:
            return 0
        latest_at = session.scalar(
            select(func.max(TopHolderSnapshot.observed_at)).where(
                TopHolderSnapshot.wallet_id == wallet_id,
                TopHolderSnapshot.token_address == token_address,
                TopHolderSnapshot.observed_at >= watch_started_at,
            )
        )
        if not latest_at:
            return 0
        score = 0
        snapshots = list(
            session.scalars(
                select(TopHolderSnapshot).where(
                    TopHolderSnapshot.wallet_id == wallet_id,
                    TopHolderSnapshot.token_address == token_address,
                    TopHolderSnapshot.observed_at == latest_at,
                )
            )
        )
        for snapshot in snapshots:
            if snapshot.dropped_out_top20:
                continue
            peak = session.scalar(
                select(TopHolderPeak).where(
                    TopHolderPeak.wallet_id == wallet_id,
                    TopHolderPeak.token_address == token_address,
                    TopHolderPeak.holder_address == snapshot.holder_address,
                    TopHolderPeak.watch_started_at == watch_started_at,
                )
            )
            if not peak:
                continue
            current_balance = _decimal_or_none(snapshot.balance)
            score = max(
                score,
                scoring.top_holder_reduction_score(
                    current_balance,
                    _decimal_or_none(peak.peak_balance),
                    _decimal_or_none(peak.peak_hold_percentage),
                    confirmed_closed=current_balance is not None and current_balance <= 0,
                ),
            )
        return score

    def _holder_cluster(self, session, wallet_id: int, token_address: str) -> int:
        watch_started_at = self._watch_started_at(session, wallet_id, token_address)
        if not watch_started_at:
            return 0
        latest_at = session.scalar(
            select(func.max(TopHolderSnapshot.observed_at)).where(
                TopHolderSnapshot.wallet_id == wallet_id,
                TopHolderSnapshot.token_address == token_address,
                TopHolderSnapshot.observed_at >= watch_started_at,
            )
        )
        if not latest_at:
            return 0
        reductions: list[Decimal] = []
        snapshots = list(
            session.scalars(
                select(TopHolderSnapshot).where(
                    TopHolderSnapshot.wallet_id == wallet_id,
                    TopHolderSnapshot.token_address == token_address,
                    TopHolderSnapshot.observed_at == latest_at,
                )
            )
        )
        for snapshot in snapshots:
            if snapshot.dropped_out_top20:
                continue
            peak = session.scalar(
                select(TopHolderPeak).where(
                    TopHolderPeak.wallet_id == wallet_id,
                    TopHolderPeak.token_address == token_address,
                    TopHolderPeak.holder_address == snapshot.holder_address,
                    TopHolderPeak.watch_started_at == watch_started_at,
                )
            )
            if not peak:
                continue
            peak_balance = _decimal_or_none(peak.peak_balance)
            current_balance = _decimal_or_none(snapshot.balance)
            if current_balance is None:
                continue
            if not peak_balance or peak_balance <= 0:
                continue
            if (_decimal_or_none(peak.peak_hold_percentage) or Decimal("0")) >= Decimal("0.01"):
                reductions.append(Decimal("1") - (current_balance / peak_balance))
        return scoring.holder_cluster_score(reductions, None, None)

    def _watch_started_at(self, session, wallet_id: int, token_address: str) -> datetime | None:
        return session.scalar(
            select(TokenWatchState.started_at).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token_address,
                TokenWatchState.active.is_(True),
            )
        )

    def _feed_family(self, session, wallet_id: int, token_address: str, family: str) -> tuple[int, str]:
        fact = self._feed_family_fact(session, wallet_id, token_address, family)
        return _feed_score_direction(fact)

    def _feed_family_fact(self, session, wallet_id: int, token_address: str, family: str) -> FeedFamilyFact | None:
        watch_started_at = self._watch_started_at(session, wallet_id, token_address)
        if not watch_started_at:
            return None
        cutoff = max(
            utc_now() - timedelta(minutes=self.settings.attention_feed_window_minutes),
            watch_started_at,
        )
        source = "gmgn_smartmoney" if family == scoring.SMART_MONEY else "gmgn_kol"
        events = list(
            session.scalars(
                select(IntelligenceEvent).where(
                    IntelligenceEvent.wallet_id == wallet_id,
                    IntelligenceEvent.token_address == token_address,
                    IntelligenceEvent.family == family,
                    IntelligenceEvent.source == source,
                    IntelligenceEvent.event_at >= cutoff,
                )
            )
        )
        if not events:
            return None
        buy_wallets: set[str] = set()
        sell_wallets: set[str] = set()
        buy_usd = Decimal("0")
        sell_usd = Decimal("0")
        usd_complete = True
        buy_activity_count = 0
        sell_activity_count = 0
        for event in events:
            payload = _json_loads(event.payload_json)
            wallet = payload.get("wallet")
            usd = _decimal_or_none(payload.get("usd_value"))
            if usd is None:
                usd_complete = False
                usd = Decimal("0")
            if event.direction == scoring.POSITIVE:
                if wallet:
                    buy_wallets.add(str(wallet))
                buy_usd += usd
                buy_activity_count += 1
            elif event.direction == scoring.NEGATIVE:
                if wallet:
                    sell_wallets.add(str(wallet))
                sell_usd += usd
                sell_activity_count += 1
        latest_intel = self._latest_token_snapshot(session, wallet_id, token_address, watch_started_at)
        liquidity = _decimal_or_none(latest_intel.liquidity_usd if latest_intel else None)
        net_flow = buy_usd - sell_usd
        buy_score = 0
        sell_score = 0
        if family == scoring.SMART_MONEY:
            buy_score = (
                scoring.smart_money_score(len(buy_wallets), net_flow, liquidity, 0, buy_activity_count)
                if net_flow > 0
                else 0
            )
            sell_score = (
                scoring.smart_money_score(len(sell_wallets), abs(net_flow), liquidity, 0, sell_activity_count)
                if net_flow < 0
                else 0
            )
        else:
            buy_score = scoring.kol_score(len(buy_wallets), 0, buy_activity_count)
            sell_score = scoring.kol_score(len(sell_wallets), 0, sell_activity_count)
        score = 0
        direction = scoring.NEUTRAL
        if buy_score > sell_score:
            score = buy_score
            direction = scoring.POSITIVE
        elif sell_score > buy_score:
            score = sell_score
            direction = scoring.NEGATIVE
        elif buy_score > 0:
            score = buy_score
            direction = scoring.MIXED
        return FeedFamilyFact(
            family=family,
            score=score,
            direction=direction,
            window_minutes=self.settings.attention_feed_window_minutes,
            buy_wallets=len(buy_wallets),
            sell_wallets=len(sell_wallets),
            net_directional_wallets=len(buy_wallets) - len(sell_wallets),
            buy_usd=buy_usd if usd_complete else None,
            sell_usd=sell_usd if usd_complete else None,
            net_usd=net_flow if usd_complete else None,
            usd_complete=usd_complete,
            buy_activity_count=buy_activity_count,
            sell_activity_count=sell_activity_count,
        )

    def _liquidity_family(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> tuple[int, str]:
        fact = self._liquidity_fact(session, wallet_id, token_address, latest_intel, watch_started_at)
        if not fact:
            return 0, scoring.NEUTRAL
        return fact.score, fact.direction

    def _liquidity_fact(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> LiquidityFact | None:
        if not latest_intel:
            return None
        baseline = self._baseline_token_snapshot(
            session,
            wallet_id,
            token_address,
            LIQUIDITY_WINDOW_MINUTES,
            latest_intel.observed_at,
            watch_started_at,
        )
        current = _decimal_or_none(latest_intel.liquidity_usd)
        baseline_value = _decimal_or_none(baseline.liquidity_usd if baseline else None)
        if current is None or baseline_value is None or baseline_value <= 0:
            return None
        score = scoring.liquidity_score(
            current,
            baseline_value,
            None,
        )
        change_pct = ((current - baseline_value) / baseline_value) * Decimal("100")
        return LiquidityFact(
            score=score,
            direction=scoring.NEGATIVE if score else scoring.NEUTRAL,
            window_minutes=LIQUIDITY_WINDOW_MINUTES,
            baseline_usd=baseline_value,
            current_usd=current,
            change_pct=change_pct,
        )

    def _display_facts(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_price,
        latest_intel,
        price_score: int,
        price_change: Decimal | None,
        price_window: int | None,
        watch_started_at: datetime | None,
        social_fact: SocialAttentionFact | None = None,
    ) -> dict[str, Any]:
        facts: dict[str, Any] = {}
        price_fact = self._price_display_fact(
            session,
            wallet_id,
            token_address,
            latest_price,
            latest_intel,
            price_score,
            price_change,
            price_window,
            watch_started_at,
        )
        if price_fact:
            facts["price"] = price_fact
        holder_fact = self._holder_count_fact(session, wallet_id, token_address, latest_intel, watch_started_at)
        if holder_fact:
            facts["holder_count"] = {
                "window_minutes": holder_fact.window_minutes,
                "baseline_count": holder_fact.baseline_count,
                "current_count": holder_fact.current_count,
                "delta_count": holder_fact.delta_count,
                "change_pct": str(holder_fact.change_pct),
            }
        top10_fact = self._top10_fact(session, wallet_id, token_address, watch_started_at)
        if top10_fact:
            facts["top10"] = top10_fact
        smart_fact = self._feed_family_fact(session, wallet_id, token_address, scoring.SMART_MONEY)
        if smart_fact:
            facts["smart_money"] = _feed_fact_to_dict(smart_fact)
        kol_fact = self._feed_family_fact(session, wallet_id, token_address, scoring.KOL)
        if kol_fact:
            facts["kol"] = _feed_fact_to_dict(kol_fact)
        liquidity_fact = self._liquidity_fact(session, wallet_id, token_address, latest_intel, watch_started_at)
        if liquidity_fact:
            facts["liquidity"] = {
                "window_minutes": liquidity_fact.window_minutes,
                "baseline_usd": str(liquidity_fact.baseline_usd),
                "current_usd": str(liquidity_fact.current_usd),
                "change_pct": str(liquidity_fact.change_pct),
            }
        if social_fact and (social_fact.unique_kols > 0 or social_fact.meaningful_dev_updates > 0):
            facts["social"] = {
                "window_minutes": social_fact.window_minutes,
                "unique_kols": social_fact.unique_kols,
                "kol_posts": social_fact.kol_posts,
                "meaningful_dev_updates": social_fact.meaningful_dev_updates,
            }
        return facts

    def _social_attention_fact(self, watched: WatchedToken) -> SocialAttentionFact | None:
        window_minutes = self.settings.attention_feed_window_minutes
        unique_kols = 0
        kol_posts = 0
        if self.social_event_service is not None:
            try:
                event_facts = self.social_event_service.build_social_facts(
                    watched.wallet_id,
                    watched.token_address,
                    window_minutes,
                )
                unique_kols = event_facts.unique_kols
                kol_posts = event_facts.kol_posts
            except Exception as exc:  # noqa: BLE001 - social must fail closed.
                logger.warning(
                    "Social KOL facts failed wallet_id=%s token=%s: %s",
                    watched.wallet_id,
                    watched.token_address,
                    exc,
                )
        memories = []
        if self.social_memory_service is not None:
            try:
                memory_since = max(
                    utc_now() - timedelta(minutes=window_minutes),
                    watched.watch_started_at,
                )
                memories = self.social_memory_service.get_recent_memories(
                    chain=watched.chain,
                    token_address=watched.token_address,
                    since=memory_since,
                    limit=50,
                )
            except Exception as exc:  # noqa: BLE001 - social must fail closed.
                logger.warning(
                    "Social memory facts failed wallet_id=%s token=%s: %s",
                    watched.wallet_id,
                    watched.token_address,
                    exc,
                )
                memories = []
        x_kol_score = scoring.social_kol_heat_score(unique_kols)
        highest_significance = _highest_social_significance(
            [memory.significance for memory in memories]
        )
        dev_score = scoring.social_dev_update_score(highest_significance)
        social_score = max(x_kol_score, dev_score)
        if dev_score == social_score and dev_score > 0:
            primary_signal = "dev_project_update"
        elif x_kol_score > 0:
            primary_signal = "social_kol_heat"
        else:
            primary_signal = None
        highest_tier_memories = [
            memory
            for memory in memories
            if highest_significance is not None and (memory.significance or "").lower() == highest_significance
        ]
        latest_memory = max(highest_tier_memories, key=lambda memory: (memory.event_time, memory.id), default=None)
        return SocialAttentionFact(
            window_minutes=window_minutes,
            unique_kols=unique_kols,
            kol_posts=kol_posts,
            meaningful_dev_updates=len(memories),
            highest_dev_significance=highest_significance,
            latest_dev_tweet_url=latest_memory.tweet_url if latest_memory else None,
            latest_dev_memory_id=latest_memory.id if latest_memory else None,
            x_kol_heat_score=x_kol_score,
            dev_update_score=dev_score,
            social_score=social_score,
            primary_social_signal=primary_signal,
        )

    def _price_display_fact(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_price,
        latest_intel,
        price_score: int,
        price_change: Decimal | None,
        price_window: int | None,
        watch_started_at: datetime | None,
    ) -> dict[str, str | int] | None:
        if not latest_price:
            return None
        latest_value = _decimal_or_none(latest_price.price_usd)
        if latest_value is None or latest_value <= 0:
            return None
        if price_score > 0 and price_change is not None and price_window is not None:
            baseline = self._baseline_price_snapshot(
                session,
                wallet_id,
                token_address,
                latest_price.observed_at,
                price_window,
                watch_started_at,
            )
            baseline_value = _decimal_or_none(baseline.price_usd if baseline else None)
            return {
                "window_minutes": price_window,
                "change_pct": str(price_change),
                "current_price": str(latest_value),
                "baseline_price": str(baseline_value) if baseline_value is not None else None,
            }
        for window in (5, 15, 60):
            baseline = self._baseline_price_snapshot(
                session,
                wallet_id,
                token_address,
                latest_price.observed_at,
                window,
                watch_started_at,
            )
            baseline_value = _decimal_or_none(baseline.price_usd if baseline else None)
            if baseline_value is None or baseline_value <= 0:
                continue
            change = ((latest_value - baseline_value) / baseline_value) * Decimal("100")
            return {
                "window_minutes": window,
                "change_pct": str(change),
                "current_price": str(latest_value),
                "baseline_price": str(baseline_value),
            }
        return None

    def _top10_fact(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at: datetime | None,
    ) -> dict[str, str | int] | None:
        if not watch_started_at:
            return None
        latest_at = session.scalar(
            select(func.max(TopHolderSnapshot.observed_at)).where(
                TopHolderSnapshot.wallet_id == wallet_id,
                TopHolderSnapshot.token_address == token_address,
                TopHolderSnapshot.observed_at >= watch_started_at,
            )
        )
        if not latest_at:
            return None
        previous_at = session.scalar(
            select(func.max(TopHolderSnapshot.observed_at)).where(
                TopHolderSnapshot.wallet_id == wallet_id,
                TopHolderSnapshot.token_address == token_address,
                TopHolderSnapshot.observed_at >= watch_started_at,
                TopHolderSnapshot.observed_at < latest_at,
            )
        )
        if not previous_at:
            return None
        current_share = self._top10_share_at(session, wallet_id, token_address, latest_at)
        previous_share = self._top10_share_at(session, wallet_id, token_address, previous_at)
        if current_share is None or previous_share is None or previous_share <= 0:
            return None
        change_pct = ((current_share - previous_share) / previous_share) * Decimal("100")
        window_minutes = int(round((latest_at - previous_at).total_seconds() / 60))
        if window_minutes > TOP10_DISPLAY_MAX_WINDOW_MINUTES:
            return None
        if abs(change_pct) < TOP10_DISPLAY_MIN_ABS_CHANGE_PCT:
            return None
        return {
            "window_minutes": window_minutes,
            "baseline_share": str(previous_share),
            "current_share": str(current_share),
            "change_pct": str(change_pct),
        }

    def _top10_share_at(self, session, wallet_id: int, token_address: str, observed_at: datetime) -> Decimal | None:
        shares = [
            _decimal_or_none(snapshot.hold_percentage)
            for snapshot in session.scalars(
                select(TopHolderSnapshot).where(
                    TopHolderSnapshot.wallet_id == wallet_id,
                    TopHolderSnapshot.token_address == token_address,
                    TopHolderSnapshot.observed_at == observed_at,
                    TopHolderSnapshot.dropped_out_top20.is_(False),
                    TopHolderSnapshot.hold_percentage.is_not(None),
                )
            )
        ]
        clean = sorted((share for share in shares if share is not None), reverse=True)
        if not clean:
            return None
        return sum(clean[:10], Decimal("0"))

    def _evidence(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_price,
        latest_intel,
        family_scores: dict[str, int],
        price_change: Decimal | None,
        price_window: int | None,
        watch_started_at: datetime | None = None,
        assessment_trigger: str | None = None,
        assessment_scores: dict[str, int] | None = None,
        primary_family: str | None = None,
        primary_direction: str | None = None,
        primary_signal: str | None = None,
        social_fact: SocialAttentionFact | None = None,
        dev_modifier_reason: str | None = None,
        market_context: HoldingMarketContext | None = None,
    ) -> dict[str, Any]:
        cutoff = utc_now() - timedelta(minutes=self.settings.attention_event_aggregation_minutes)
        if watch_started_at is not None:
            cutoff = max(cutoff, watch_started_at)
        events = list(
            session.scalars(
                select(IntelligenceEvent)
                .where(
                    IntelligenceEvent.wallet_id == wallet_id,
                    IntelligenceEvent.token_address == token_address,
                    IntelligenceEvent.event_at >= cutoff,
                )
                .order_by(IntelligenceEvent.event_at.desc())
            )
        )
        dropped_out_top20 = self._dropped_out_top20_evidence(session, wallet_id, token_address)
        display_facts = self._display_facts(
            session,
            wallet_id,
            token_address,
            latest_price,
            latest_intel,
            family_scores.get(scoring.PRICE, 0),
            price_change,
            price_window,
            watch_started_at,
            social_fact,
        )
        position_intelligence = build_position_intelligence(
            assessment_scores or {"price_score": family_scores.get(scoring.PRICE, 0)},
            display_facts,
        )
        return {
            "assessment_trigger": assessment_trigger,
            "primary_family": primary_family,
            "primary_direction": primary_direction,
            "primary_signal": primary_signal,
            "family_scores": family_scores,
            "social": social_fact.to_dict() if social_fact else None,
            "dev_modifier_reason": dev_modifier_reason,
            "market_cap_context": market_context.to_dict() if market_context else None,
            "display_facts": display_facts,
            "position_intelligence": position_intelligence.to_dict(),
            "price_change_pct": str(price_change) if price_change is not None else None,
            "price_window_minutes": price_window,
            "current_price": str(latest_price.price_usd) if latest_price else None,
            "usd_value": str(latest_price.usd_value) if latest_price else None,
            "market_cap": str(latest_intel.market_cap_usd) if latest_intel else None,
            "liquidity": str(latest_intel.liquidity_usd) if latest_intel else None,
            "holder_count": latest_intel.holder_count if latest_intel else None,
            "dropped_out_top20_count": len(dropped_out_top20),
            "dropped_out_top20": dropped_out_top20,
            "recent_events": [
                {
                    "family": event.family,
                    "type": event.event_type,
                    "direction": event.direction,
                    "at": event.event_at.isoformat(),
                    "payload": _json_loads(event.payload_json),
                }
                for event in events[:10]
            ],
        }

    def _dropped_out_top20_evidence(self, session, wallet_id: int, token_address: str) -> list[dict[str, str | None]]:
        watch_started_at = self._watch_started_at(session, wallet_id, token_address)
        if not watch_started_at:
            return []
        latest_at = session.scalar(
            select(func.max(TopHolderSnapshot.observed_at)).where(
                TopHolderSnapshot.wallet_id == wallet_id,
                TopHolderSnapshot.token_address == token_address,
                TopHolderSnapshot.observed_at >= watch_started_at,
            )
        )
        if not latest_at:
            return []
        snapshots = list(
            session.scalars(
                select(TopHolderSnapshot).where(
                    TopHolderSnapshot.wallet_id == wallet_id,
                    TopHolderSnapshot.token_address == token_address,
                    TopHolderSnapshot.observed_at == latest_at,
                    TopHolderSnapshot.dropped_out_top20.is_(True),
                )
            )
        )
        evidence: list[dict[str, str | None]] = []
        for snapshot in snapshots:
            peak = session.scalar(
                select(TopHolderPeak).where(
                    TopHolderPeak.wallet_id == wallet_id,
                    TopHolderPeak.token_address == token_address,
                    TopHolderPeak.holder_address == snapshot.holder_address,
                    TopHolderPeak.watch_started_at == watch_started_at,
                )
            )
            if not peak:
                continue
            evidence.append(
                {
                    "holder_address": snapshot.holder_address,
                    "peak_hold_percentage": str(peak.peak_hold_percentage)
                    if peak.peak_hold_percentage is not None
                    else None,
                }
            )
        return evidence

    def _should_notify(
        self,
        session,
        wallet_id: int,
        token_address: str,
        level: str,
        final_score: int,
        direction: str,
        family_scores: dict[str, int],
        now: datetime,
        *,
        current_price_usd: Decimal | None = None,
        watch_started_at: datetime | None = None,
    ) -> bool:
        return self._notification_decision(
            session,
            wallet_id,
            token_address,
            level,
            final_score,
            direction,
            family_scores,
            now,
            current_price_usd=current_price_usd,
            watch_started_at=watch_started_at,
        ).should_notify

    def _notification_decision(
        self,
        session,
        wallet_id: int,
        token_address: str,
        level: str,
        final_score: int,
        direction: str,
        family_scores: dict[str, int],
        now: datetime,
        *,
        current_price_usd: Decimal | None = None,
        watch_started_at: datetime | None = None,
    ) -> NotificationDecision:
        context = {
            "current_price_usd": str(current_price_usd) if current_price_usd is not None else None,
            "last_notified_price_usd": None,
            "price_change_since_last_notification_pct": None,
            "continued_extreme_price_move": False,
        }
        if level not in {scoring.WARNING, scoring.CRITICAL}:
            return NotificationDecision(False, "below_warning", context)
        state = session.scalar(
            select(AttentionAlertState).where(
                AttentionAlertState.wallet_id == wallet_id,
                AttentionAlertState.token_address == token_address,
            )
        )
        if not state or not state.last_notified_at:
            return NotificationDecision(True, "no_previous_alert", context)
        previous_scores = _json_loads(state.family_scores_json)
        state_meta = _notification_state_meta(previous_scores)
        extreme_context = self._continued_extreme_price_context(
            state_meta,
            current_price_usd,
            watch_started_at,
            now,
            level,
            family_scores,
        )
        context.update(extreme_context)
        if state.last_attention_level == scoring.WARNING and level == scoring.CRITICAL:
            return NotificationDecision(True, "level_escalation", context)
        if state.last_final_score is not None and final_score - state.last_final_score >= 15:
            return NotificationDecision(True, "score_jump", context)
        if state.last_direction and state.last_direction != direction and direction != scoring.NEUTRAL:
            return NotificationDecision(True, "direction_change", context)
        if _has_new_strong_family(previous_scores, family_scores):
            return NotificationDecision(True, "new_strong_family", context)
        if context["continued_extreme_price_move"]:
            return NotificationDecision(True, "continued_extreme_price_move", context)
        if level == scoring.CRITICAL:
            if now - state.last_notified_at >= timedelta(minutes=self.settings.attention_critical_cooldown_minutes):
                return NotificationDecision(True, "critical_cooldown_expired", context)
            return NotificationDecision(False, "critical_cooldown", context)
        if now - state.last_notified_at >= timedelta(minutes=self.settings.attention_warning_cooldown_minutes):
            return NotificationDecision(True, "warning_cooldown_expired", context)
        return NotificationDecision(False, "warning_cooldown", context)

    def _continued_extreme_price_context(
        self,
        state_meta: dict[str, Any],
        current_price_usd: Decimal | None,
        watch_started_at: datetime | None,
        now: datetime,
        level: str,
        family_scores: dict[str, int],
    ) -> dict[str, Any]:
        context = {
            "current_price_usd": str(current_price_usd) if current_price_usd is not None else None,
            "last_notified_price_usd": None,
            "price_change_since_last_notification_pct": None,
            "continued_extreme_price_move": False,
        }
        if level != scoring.CRITICAL:
            return context
        if int(family_scores.get(scoring.PRICE, 0) or 0) != 40:
            return context
        if not _same_watch_started_at(state_meta.get("watch_started_at"), watch_started_at):
            return context
        previous_price = _decimal_or_none(state_meta.get("last_notified_price_usd"))
        if previous_price is None or previous_price <= 0:
            return context
        context["last_notified_price_usd"] = str(previous_price)
        if current_price_usd is None or current_price_usd <= 0:
            return context
        change_pct = ((current_price_usd / previous_price) - Decimal("1")) * Decimal("100")
        context["price_change_since_last_notification_pct"] = str(change_pct)
        if abs(change_pct) < CONTINUED_EXTREME_PRICE_MOVE_THRESHOLD_PCT:
            return context
        last_extreme_at = _parse_iso_datetime(state_meta.get("last_extreme_price_notified_at"))
        if last_extreme_at and now - last_extreme_at < timedelta(minutes=CONTINUED_EXTREME_PRICE_MOVE_COOLDOWN_MINUTES):
            return context
        context["continued_extreme_price_move"] = True
        return context

    async def _notify_assessment(self, assessment: AttentionAssessment) -> bool:
        if not self.notifier:
            return False
        lock = self._notification_lock(assessment.wallet_id, assessment.token_address)
        async with lock:
            with session_scope(self.session_factory) as session:
                wallet = session.get(Wallet, assessment.wallet_id)
                if not wallet:
                    return False
                if not self._assessment_matches_current_watch(session, assessment):
                    logger.info(
                        "Attention Telegram skipped stale assessment wallet_id=%s token=%s assessed_at=%s",
                        assessment.wallet_id,
                        assessment.token_address,
                        assessment.assessed_at,
                    )
                    return False
                notification_decision = self._notification_decision(
                    session,
                    assessment.wallet_id,
                    assessment.token_address,
                    assessment.attention_level,
                    assessment.final_attention_score,
                    assessment.direction,
                    _family_scores_from_assessment(assessment),
                    utc_now(),
                    current_price_usd=_assessment_current_price_usd(assessment),
                    watch_started_at=self._current_watch_started_at(session, assessment.wallet_id, assessment.token_address),
                )
                if not notification_decision.should_notify:
                    logger.info(
                        "Attention notification suppressed after serialized recheck wallet_id=%s token=%s score=%s level=%s reason=%s",
                        assessment.wallet_id,
                        assessment.token_address,
                        assessment.final_attention_score,
                        assessment.attention_level,
                        notification_decision.reason,
                    )
                    return False
                logger.info(
                    "Attention notification allowed after serialized recheck wallet_id=%s token=%s score=%s level=%s reason=%s price_change_since_last_notification=%s",
                    assessment.wallet_id,
                    assessment.token_address,
                    assessment.final_attention_score,
                    assessment.attention_level,
                    notification_decision.reason,
                    notification_decision.notification_context.get("price_change_since_last_notification_pct"),
                )
                chat_id = wallet.user.telegram_chat_id
            await self._send_notification_with_retry(
                chat_id,
                format_attention_alert(assessment),
                assessment,
                build_attention_copy_markup(assessment),
            )
            if not self._mark_notified(assessment, notification_decision.reason):
                logger.info(
                    "Attention notification delivered but mark skipped stale assessment wallet_id=%s token=%s",
                    assessment.wallet_id,
                    assessment.token_address,
                )
                return False
            logger.info(
                "Attention Telegram delivery success wallet_id=%s token=%s score=%s level=%s",
                assessment.wallet_id,
                assessment.token_address,
                assessment.final_attention_score,
                assessment.attention_level,
            )
            return True

    def _notification_lock(self, wallet_id: int, token_address: str) -> asyncio.Lock:
        key = (wallet_id, token_address.lower())
        lock = self._notification_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._notification_locks[key] = lock
        return lock

    async def _send_notification_with_retry(
        self,
        chat_id: int,
        text: str,
        assessment: AttentionAssessment,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        last_error: Exception | None = None
        for attempt, delay in enumerate(ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                await self._call_notifier(chat_id, text, reply_markup)
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Attention Telegram delivery failed wallet_id=%s token=%s attempt=%s/%s: %s",
                    assessment.wallet_id,
                    assessment.token_address,
                    attempt,
                    len(ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS),
                    exc,
                )
        if last_error:
            raise last_error
        raise RuntimeError("attention_notification_failed")

    async def _call_notifier(
        self,
        chat_id: int,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        if reply_markup is not None and _notifier_accepts_reply_markup(self.notifier):
            await self.notifier(chat_id, text, reply_markup=reply_markup)
            return
        await self.notifier(chat_id, text)

    def _mark_notified(self, assessment: AttentionAssessment, notification_reason: str | None = None) -> bool:
        with session_scope(self.session_factory) as session:
            watch_started_at = self._current_watch_started_at(session, assessment.wallet_id, assessment.token_address)
            if not watch_started_at or assessment.assessed_at < watch_started_at:
                return False
            state = session.scalar(
                select(AttentionAlertState).where(
                    AttentionAlertState.wallet_id == assessment.wallet_id,
                    AttentionAlertState.token_address == assessment.token_address,
                )
            )
            if not state:
                state = AttentionAlertState(
                    wallet_id=assessment.wallet_id,
                    token_address=assessment.token_address,
                )
                session.add(state)
            state.last_notified_at = utc_now()
            state.last_attention_level = assessment.attention_level
            state.last_final_score = assessment.final_attention_score
            state.last_direction = assessment.direction
            previous_meta = _notification_state_meta(_json_loads(state.family_scores_json))
            state.family_scores_json = json.dumps(
                _family_scores_with_notification_meta(
                    assessment,
                    previous_meta,
                    watch_started_at,
                    notification_reason,
                    state.last_notified_at,
                ),
                ensure_ascii=False,
            )
            return True

    def _assessment_matches_current_watch(self, session, assessment: AttentionAssessment) -> bool:
        started_at = self._current_watch_started_at(session, assessment.wallet_id, assessment.token_address)
        return bool(started_at and assessment.assessed_at >= started_at)

    def _current_watch_started_at(self, session, wallet_id: int, token_address: str) -> datetime | None:
        return session.scalar(
            select(TokenWatchState.started_at).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token_address,
                TokenWatchState.active.is_(True),
            )
        )

    async def cleanup_old_snapshots(self) -> None:
        cutoff = utc_now() - timedelta(days=7)
        with session_scope(self.session_factory) as session:
            session.execute(delete(TokenIntelligenceSnapshot).where(TokenIntelligenceSnapshot.observed_at < cutoff))
            session.execute(delete(TopHolderSnapshot).where(TopHolderSnapshot.observed_at < cutoff))


def fingerprint_event(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _valid_price_snapshot_clause():
    return or_(
        PriceSnapshot.quality_status.is_(None),
        PriceSnapshot.quality_status == PRICE_QUALITY_VALID,
    )


def format_attention_alert(assessment: AttentionAssessment) -> str:
    symbol = assessment.symbol or assessment.token_address[:10]
    emoji = _direction_emoji(assessment.direction)
    evidence = _json_loads(assessment.evidence_json)
    trigger_title = format_directional_trigger_title(
        assessment.primary_family,
        evidence.get("primary_direction"),
        evidence.get("assessment_trigger"),
        evidence.get("primary_signal"),
    )
    lines = [f"{emoji} {symbol}"]
    market_cap_line = _market_cap_context_line(evidence.get("market_cap_context"))
    if market_cap_line:
        lines.append(market_cap_line)
    lines.extend(
        [
            "",
            f"{trigger_title}｜{_direction_label(assessment.direction)}｜ATT {assessment.final_attention_score}",
            "",
            "近况：",
        ]
    )
    for fact_line in _display_fact_lines(evidence.get("display_facts")):
        lines.append(fact_line)
    position_intelligence = evidence.get("position_intelligence")
    if isinstance(position_intelligence, dict) and position_intelligence.get("summary"):
        lines.extend(
            [
                "",
                f"判断：{position_intelligence['summary']}",
            ]
        )
    return "\n".join(lines)


def _market_cap_context_line(context: Any) -> str | None:
    if not isinstance(context, dict):
        return None
    current = context.get("current_market_cap_usd")
    if current in (None, ""):
        return None
    return (
        f"持仓市值：{format_market_cap(context.get('entry_market_cap_usd'))}"
        f" ｜ 当前市值：{format_market_cap(current)}"
    )


def _direction_emoji(direction: str | None) -> str:
    if direction == scoring.POSITIVE:
        return "🟢"
    if direction == scoring.NEGATIVE:
        return "🔴"
    if direction == scoring.MIXED:
        return "🟠"
    return "⚪️"


def build_attention_copy_markup(assessment: AttentionAssessment) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                f"📋 {short_address(assessment.token_address)}",
                copy_text=CopyTextButton(assessment.token_address),
            )
        ]
    ]
    evidence = _json_loads(assessment.evidence_json)
    social = evidence.get("social")
    if (
        isinstance(social, dict)
        and int(social.get("meaningful_dev_updates") or 0) > 0
        and isinstance(social.get("latest_dev_tweet_url"), str)
        and social["latest_dev_tweet_url"].startswith(("http://", "https://"))
    ):
        rows.append([InlineKeyboardButton("🔗 查看DEV更新", url=social["latest_dev_tweet_url"])])
    return InlineKeyboardMarkup(rows)


def short_address(address: str) -> str:
    if not address.startswith("0x") or len(address) <= 8:
        return address
    return f"{address[:5]}...{address[-3:]}"


def format_attention_debug(assessment: AttentionAssessment) -> str:
    return "\n".join(
        [
            f"🧭 {assessment.symbol or assessment.token_address} Attention",
            "",
            f"Final：{assessment.final_attention_score}",
            f"Level：{assessment.attention_level}",
            f"Direction：{assessment.direction}",
            "",
            f"Price：{assessment.price_score}",
            f"Holder Breadth：{assessment.holder_breadth_score}",
            f"TOP Holder：{assessment.top_holder_score}",
            f"Holder Cluster：{assessment.holder_cluster_score}",
            f"Holder Family：{assessment.holder_family_score}",
            f"Smart Money：{assessment.smart_money_score}",
            f"KOL：{assessment.kol_score}",
            f"Liquidity：{assessment.liquidity_score}",
            "",
            f"Primary：{assessment.primary_family} {assessment.primary_event_score}",
            f"Position：{assessment.position_exposure_score}",
            f"Abnormality：{assessment.abnormality_score}",
            f"Secondary：{assessment.secondary_signal_score}",
            f"Dev Modifier：{assessment.dev_modifier}",
        ]
    )


def _from_timestamp(value: int | None) -> datetime:
    if value is None:
        return utc_now()
    return datetime.fromtimestamp(value, UTC).replace(tzinfo=None)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def build_position_intelligence(
    assessment_scores: dict[str, int],
    display_facts: dict[str, Any] | None,
) -> PositionIntelligenceResult:
    facts = display_facts if isinstance(display_facts, dict) else {}
    signals = _structure_signals(assessment_scores, facts)
    by_family = {signal.family: signal for signal in signals}
    positive_drivers = _driver_names(signals, scoring.POSITIVE)
    negative_drivers = _driver_names(signals, scoring.NEGATIVE)
    neutral_drivers = _driver_names(signals, scoring.NEUTRAL)
    core_families = {"holder_count", "top10", scoring.SMART_MONEY, scoring.LIQUIDITY}
    core_positive = [family for family in positive_drivers if family in core_families]
    core_negative = [family for family in negative_drivers if family in core_families]
    price_direction = _direction_for(by_family, scoring.PRICE)
    smart_direction = _direction_for(by_family, scoring.SMART_MONEY)
    holder_direction = _direction_for(by_family, "holder_count")
    top10_direction = _direction_for(by_family, "top10")

    if price_direction == scoring.NEGATIVE and len(core_negative) >= 2:
        label = STRUCTURAL_DETERIORATION
    elif price_direction == scoring.POSITIVE and len(core_positive) >= 2 and not core_negative:
        label = MARKET_EXPANSION
    elif (
        price_direction == scoring.POSITIVE
        and top10_direction == scoring.NEGATIVE
        and (holder_direction != scoring.POSITIVE or smart_direction == scoring.NEGATIVE)
    ):
        label = MINORITY_DRIVEN
    elif (
        price_direction == scoring.NEGATIVE
        and smart_direction == scoring.POSITIVE
        and len(core_negative) < 2
    ):
        label = SUPPORT_EMERGING
    elif positive_drivers and negative_drivers:
        label = SIGNAL_DIVERGENCE
    else:
        label = NO_CLEAR_CHANGE

    return PositionIntelligenceResult(
        label=label,
        label_cn=POSITION_LABELS_CN[label],
        summary=_position_summary(label, by_family),
        positive_drivers=positive_drivers,
        negative_drivers=negative_drivers,
        neutral_drivers=neutral_drivers,
    )


def _structure_signals(assessment_scores: dict[str, int], display_facts: dict[str, Any]) -> list[StructureSignal]:
    signals: list[StructureSignal] = []
    price_change = _fact_change(display_facts.get("price"))
    price_score = int(assessment_scores.get("price_score", 0) or 0)
    if price_score > 0 and price_change is not None and price_change != 0:
        signals.append(
            StructureSignal(
                family=scoring.PRICE,
                direction=scoring.POSITIVE if price_change > 0 else scoring.NEGATIVE,
                strength=price_score,
                value=str(price_change),
                reason="price_up" if price_change > 0 else "price_down",
            )
        )
    elif isinstance(display_facts.get("price"), dict):
        signals.append(StructureSignal(scoring.PRICE, scoring.NEUTRAL, 0, str(price_change) if price_change is not None else None, "price_context"))

    holder_change = _fact_change(display_facts.get("holder_count"))
    holder_score = int(assessment_scores.get("holder_breadth_score", 0) or 0)
    if holder_score > 0 and holder_change is not None and holder_change != 0:
        signals.append(
            StructureSignal(
                family="holder_count",
                direction=scoring.POSITIVE if holder_change > 0 else scoring.NEGATIVE,
                strength=holder_score,
                value=str(holder_change),
                reason="holder_count_expanded" if holder_change > 0 else "holder_count_contracting",
            )
        )
    elif isinstance(display_facts.get("holder_count"), dict):
        signals.append(
            StructureSignal("holder_count", scoring.NEUTRAL, holder_score, str(holder_change) if holder_change is not None else None, "holder_count_context")
        )

    top10_change = _fact_change(display_facts.get("top10"))
    if top10_change is not None:
        direction = scoring.NEUTRAL
        reason = "top10_stable"
        if top10_change >= Decimal("10"):
            direction = scoring.NEGATIVE
            reason = "concentration_increased"
        elif top10_change <= Decimal("-10"):
            direction = scoring.POSITIVE
            reason = "concentration_decreased"
        signals.append(
            StructureSignal(
                family="top10",
                direction=direction,
                strength=int(abs(top10_change)),
                value=str(top10_change),
                reason=reason,
            )
        )

    smart_signal = _feed_structure_signal(scoring.SMART_MONEY, assessment_scores.get("smart_money_score", 0), display_facts.get("smart_money"))
    if smart_signal:
        signals.append(smart_signal)
    kol_signal = _feed_structure_signal(scoring.KOL, assessment_scores.get("kol_score", 0), display_facts.get("kol"))
    if kol_signal:
        signals.append(kol_signal)

    liquidity_change = _fact_change(display_facts.get("liquidity"))
    liquidity_score = int(assessment_scores.get("liquidity_score", 0) or 0)
    if liquidity_score > 0:
        signals.append(
            StructureSignal(
                family=scoring.LIQUIDITY,
                direction=scoring.NEGATIVE,
                strength=liquidity_score,
                value=str(liquidity_change) if liquidity_change is not None else None,
                reason="liquidity_deteriorated",
            )
        )
    elif liquidity_change is not None and liquidity_change >= Decimal("10"):
        signals.append(
            StructureSignal(
                family=scoring.LIQUIDITY,
                direction=scoring.POSITIVE,
                strength=10,
                value=str(liquidity_change),
                reason="liquidity_improved",
            )
        )
    elif isinstance(display_facts.get("liquidity"), dict):
        signals.append(
            StructureSignal(scoring.LIQUIDITY, scoring.NEUTRAL, 0, str(liquidity_change) if liquidity_change is not None else None, "liquidity_context")
        )
    return signals


def _feed_structure_signal(family: str, score_value: int | None, fact: Any) -> StructureSignal | None:
    if not isinstance(fact, dict):
        return None
    score_int = int(score_value or 0)
    direction = scoring.NEUTRAL
    value: str | None = None
    net_usd = _decimal_or_none(fact.get("net_usd")) if fact.get("usd_complete") else None
    if score_int > 0:
        if net_usd is not None and net_usd != 0:
            direction = scoring.POSITIVE if net_usd > 0 else scoring.NEGATIVE
            value = str(net_usd)
        else:
            net_wallets = int(fact.get("net_wallets", 0) or 0)
            if net_wallets > 0:
                direction = scoring.POSITIVE
            elif net_wallets < 0:
                direction = scoring.NEGATIVE
            value = str(net_wallets)
    return StructureSignal(
        family=family,
        direction=direction,
        strength=score_int,
        value=value,
        reason=_feed_reason(family, direction),
    )


def _feed_reason(family: str, direction: str) -> str:
    prefix = "smart_money" if family == scoring.SMART_MONEY else "kol"
    if direction == scoring.POSITIVE:
        return f"{prefix}_net_inflow"
    if direction == scoring.NEGATIVE:
        return f"{prefix}_net_outflow"
    return f"{prefix}_context"


def _fact_change(fact: Any) -> Decimal | None:
    if not isinstance(fact, dict):
        return None
    return _decimal_or_none(fact.get("change_pct"))


def _driver_names(signals: list[StructureSignal], direction: str) -> list[str]:
    return [signal.family for signal in _sorted_signals(signals) if signal.direction == direction]


def _sorted_signals(signals: list[StructureSignal]) -> list[StructureSignal]:
    order = {
        scoring.PRICE: 0,
        scoring.SMART_MONEY: 1,
        "holder_count": 2,
        "top10": 3,
        scoring.LIQUIDITY: 4,
        scoring.KOL: 5,
    }
    return sorted(signals, key=lambda signal: order.get(signal.family, 99))


def _direction_for(signals: dict[str, StructureSignal], family: str) -> str:
    signal = signals.get(family)
    return signal.direction if signal else scoring.NEUTRAL


def _position_summary(label: str, signals: dict[str, StructureSignal]) -> str:
    price = _direction_for(signals, scoring.PRICE)
    smart = _direction_for(signals, scoring.SMART_MONEY)
    holder = _direction_for(signals, "holder_count")
    top10 = _direction_for(signals, "top10")
    liquidity = _direction_for(signals, scoring.LIQUIDITY)
    kol = _direction_for(signals, scoring.KOL)
    if label == STRUCTURAL_DETERIORATION:
        if holder == scoring.NEGATIVE and smart == scoring.NEGATIVE:
            return "价格下跌且持仓人数、资金流同步走弱，结构性风险上升。"
        if top10 == scoring.NEGATIVE and liquidity == scoring.NEGATIVE:
            return "价格走弱，同时筹码集中度提高、流动性下降，结构性恶化。"
        return "价格走弱，多项结构信号同步转弱，结构性风险上升。"
    if label == MARKET_EXPANSION:
        if holder == scoring.POSITIVE and smart == scoring.POSITIVE:
            return "价格走强，持仓人数与资金流同步改善，市场扩散增强。"
        if top10 == scoring.POSITIVE and liquidity == scoring.POSITIVE:
            return "价格上涨同时筹码趋于分散，市场扩散结构增强。"
        return "价格走强，多项结构信号同步改善，市场扩散增强。"
    if label == MINORITY_DRIVEN:
        if smart == scoring.NEGATIVE:
            return "价格上涨但筹码趋于集中，聪明钱净流出，偏少数资金推动。"
        return "价格上涨但筹码趋于集中，市场扩散有限，偏少数资金推动。"
    if label == SUPPORT_EMERGING:
        if holder == scoring.POSITIVE:
            return "价格走弱，但持仓人数与资金仍有承接。"
        return "价格回落，但聪明钱出现净流入，短线存在承接。"
    if label == SIGNAL_DIVERGENCE:
        if price == scoring.POSITIVE and smart == scoring.NEGATIVE:
            return "价格上涨，但聪明钱净流出，当前价格与资金信号分化。"
        if holder == scoring.POSITIVE and top10 == scoring.NEGATIVE:
            return "持仓人数增长，但筹码集中度提高，结构信号分化。"
        if price == scoring.NEGATIVE and kol == scoring.POSITIVE:
            return "价格走弱，但KOL资金净流入，当前信号分化。"
        return "多项结构信号方向不一致，当前信号分化。"
    if price in {scoring.POSITIVE, scoring.NEGATIVE}:
        return "当前主要是价格异动，暂未看到明显结构共振。"
    return "当前有效结构信号有限，暂未看到明显共振。"


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _notification_state_meta(state_json: dict[str, Any]) -> dict[str, Any]:
    meta = state_json.get("_meta") if isinstance(state_json, dict) else None
    return meta if isinstance(meta, dict) else {}


def _assessment_current_price_usd(assessment: AttentionAssessment) -> Decimal | None:
    evidence = _json_loads(assessment.evidence_json)
    notification_context = evidence.get("notification_context")
    if isinstance(notification_context, dict):
        price = _decimal_or_none(notification_context.get("current_price_usd"))
        if price is not None:
            return price
    return _decimal_or_none(evidence.get("current_price"))


def _family_scores_with_notification_meta(
    assessment: AttentionAssessment,
    previous_meta: dict[str, Any],
    watch_started_at: datetime,
    notification_reason: str | None,
    notified_at: datetime | None,
) -> dict[str, Any]:
    scores: dict[str, Any] = _family_scores_from_assessment(assessment)
    meta = dict(previous_meta) if _same_watch_started_at(previous_meta.get("watch_started_at"), watch_started_at) else {}
    current_price = _assessment_current_price_usd(assessment)
    if current_price is not None and current_price > 0:
        meta["last_notified_price_usd"] = str(current_price)
    if notification_reason == "continued_extreme_price_move" and notified_at is not None:
        meta["last_extreme_price_notified_at"] = notified_at.isoformat()
    meta["watch_started_at"] = watch_started_at.isoformat()
    scores["_meta"] = meta
    return scores


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _same_watch_started_at(meta_value: Any, watch_started_at: datetime | None) -> bool:
    if watch_started_at is None:
        return False
    parsed = _parse_iso_datetime(meta_value)
    return parsed == watch_started_at


def _has_new_strong_family(previous_scores: dict[str, Any], current_scores: dict[str, int]) -> bool:
    for family, score in current_scores.items():
        previous = int(previous_scores.get(family, 0) or 0)
        if score >= 30 and previous < 30:
            return True
    return False


def _family_scores_from_assessment(assessment: AttentionAssessment) -> dict[str, int]:
    scores = {
        scoring.PRICE: assessment.price_score,
        scoring.HOLDER: assessment.holder_family_score,
        scoring.SMART_MONEY: assessment.smart_money_score,
        scoring.KOL: assessment.kol_score,
        scoring.LIQUIDITY: assessment.liquidity_score,
    }
    evidence = _json_loads(assessment.evidence_json)
    family_scores = evidence.get("family_scores")
    if isinstance(family_scores, dict):
        scores[scoring.SOCIAL] = int(family_scores.get(scoring.SOCIAL, 0) or 0)
    return scores


def _primary_direction_for_family(
    primary_family: str | None,
    family_directions: dict[str, str],
    assessment_direction: str,
) -> str:
    if primary_family is None:
        return assessment_direction if assessment_direction in {scoring.POSITIVE, scoring.NEGATIVE, scoring.MIXED} else scoring.NEUTRAL
    return family_directions.get(primary_family, scoring.NEUTRAL)


def _primary_signal_for_family(
    primary_family: str | None,
    holder_breadth: int,
    top_holder: int,
    cluster: int,
    social_fact: SocialAttentionFact | None = None,
) -> str | None:
    if primary_family == scoring.SOCIAL:
        return social_fact.primary_social_signal if social_fact else None
    if primary_family != scoring.HOLDER:
        return None
    holder_max = max(holder_breadth, top_holder, cluster)
    if holder_max <= 0:
        return None
    if holder_breadth == holder_max:
        return "holder_breadth"
    if top_holder == holder_max:
        return "top_holder_reduction"
    return "holder_cluster_reduction"


def _highest_social_significance(values: list[str | None]) -> str | None:
    rank = {"low": 1, "medium": 2, "high": 3}
    best: str | None = None
    best_rank = 0
    for value in values:
        normalized = (value or "").lower()
        current_rank = rank.get(normalized, 0)
        if current_rank > best_rank:
            best = normalized
            best_rank = current_rank
    return best


def _dev_modifier_for_social_fact(social_fact: SocialAttentionFact | None) -> int:
    if social_fact and social_fact.highest_dev_significance == "high":
        return 5
    return 0


def _holder_primary_direction(primary_signal: str | None, holder_breadth_direction: str) -> str:
    if primary_signal == "holder_breadth":
        return holder_breadth_direction
    if primary_signal in {"top_holder_reduction", "holder_cluster_reduction"}:
        return scoring.NEGATIVE
    return scoring.NEUTRAL


def _feed_score_direction(fact: FeedFamilyFact | None) -> tuple[int, str]:
    if not fact:
        return 0, scoring.NEUTRAL
    return fact.score, fact.direction


def _feed_primary_display_direction(fact: FeedFamilyFact | None, scoring_direction: str) -> str:
    if fact:
        if fact.usd_complete and fact.net_usd is not None:
            if fact.net_usd > 0:
                return scoring.POSITIVE
            if fact.net_usd < 0:
                return scoring.NEGATIVE
        if fact.net_directional_wallets > 0:
            return scoring.POSITIVE
        if fact.net_directional_wallets < 0:
            return scoring.NEGATIVE
    return scoring_direction


def _direction_label(direction: str) -> str:
    return {
        scoring.POSITIVE: "Positive",
        scoring.NEGATIVE: "Negative",
        scoring.MIXED: "Mixed",
        scoring.NEUTRAL: "Neutral",
    }.get(direction, direction)


def format_directional_trigger_title(
    primary_family: str | None,
    primary_direction: Any,
    assessment_trigger: str | None = None,
    primary_signal: Any = None,
) -> str:
    if assessment_trigger == "usd_threshold_crossing":
        return "持仓价值跌破$5"
    direction = primary_direction if primary_direction in {scoring.POSITIVE, scoring.NEGATIVE, scoring.MIXED} else None
    signal = primary_signal if isinstance(primary_signal, str) else None
    if primary_family == scoring.PRICE:
        return _directional_title(direction, "价格上涨", "价格下跌", "价格异动")
    if primary_family == scoring.HOLDER:
        if signal == "top_holder_reduction":
            return "重要大户减仓"
        if signal == "holder_cluster_reduction":
            return "多名大户减仓"
        if signal == "holder_structure":
            return "筹码异动"
        return _directional_title(direction, "持币人数增加", "持币人数减少", "持币人数异动")
    if primary_family == scoring.SMART_MONEY:
        return _directional_title(direction, "聪明钱流入", "聪明钱流出", "聪明钱异动")
    if primary_family == scoring.KOL:
        return _directional_title(direction, "KOL资金流入", "KOL资金流出", "KOL资金异动")
    if primary_family == scoring.LIQUIDITY:
        return _directional_title(direction, "流动性增加", "流动性减少", "流动性异动")
    if primary_family == scoring.SOCIAL:
        if signal == "dev_project_update":
            return "DEV推特更新"
        if signal == "social_kol_heat":
            return "社媒热度上升"
        return "社媒异动"
    if primary_family in (None, "comprehensive"):
        if direction == scoring.POSITIVE:
            return "综合走强"
        if direction == scoring.NEGATIVE:
            return "综合走弱"
        if direction == scoring.MIXED:
            return "信号分化"
        return "综合异动"
    if primary_family == "dev":
        return "DEV推特更新"
    return "综合异动"


def _directional_title(direction: str | None, positive: str, negative: str, fallback: str) -> str:
    if direction == scoring.POSITIVE:
        return positive
    if direction == scoring.NEGATIVE:
        return negative
    return fallback


def _display_fact_lines(display_facts: Any) -> list[str]:
    if not isinstance(display_facts, dict):
        return []
    lines: list[str] = []
    price = display_facts.get("price")
    if isinstance(price, dict) and price.get("change_pct") is not None and price.get("window_minutes") is not None:
        lines.append(
            f"• 价格｜{_format_window(int(price['window_minutes']))} {_format_pct(Decimal(str(price['change_pct'])))}"
        )
    liquidity = display_facts.get("liquidity")
    if (
        isinstance(liquidity, dict)
        and liquidity.get("change_pct") is not None
        and liquidity.get("window_minutes") is not None
    ):
        lines.append(
            f"• 流动性｜{_format_window(int(liquidity['window_minutes']))} {_format_pct(Decimal(str(liquidity['change_pct'])))}"
        )
    holder = display_facts.get("holder_count")
    if isinstance(holder, dict) and holder.get("change_pct") is not None and holder.get("window_minutes") is not None:
        lines.append(
            f"• 持仓人数｜{_format_window(int(holder['window_minutes']))} {_format_pct(Decimal(str(holder['change_pct'])))}"
        )
    top10 = display_facts.get("top10")
    if isinstance(top10, dict) and top10.get("change_pct") is not None and top10.get("window_minutes") is not None:
        lines.append(
            f"• Top10｜{_format_window(int(top10['window_minutes']))} {_format_pct(Decimal(str(top10['change_pct'])))}"
        )
    smart = display_facts.get("smart_money")
    smart_line = _feed_display_line("聪明钱", smart)
    if smart_line:
        lines.append(smart_line)
    kol = display_facts.get("kol")
    kol_line = _feed_display_line("KOL", kol)
    if kol_line:
        lines.append(kol_line)
    social = display_facts.get("social")
    social_line = _social_display_line(social)
    if social_line:
        lines.append(social_line)
    return lines


def _social_display_line(fact: Any) -> str | None:
    if not isinstance(fact, dict) or fact.get("window_minutes") is None:
        return None
    unique_kols = int(fact.get("unique_kols") or 0)
    dev_updates = int(fact.get("meaningful_dev_updates") or 0)
    if unique_kols <= 0 and dev_updates <= 0:
        return None
    parts = [f"• 社媒｜{_format_window(int(fact['window_minutes']))}"]
    if unique_kols > 0:
        parts.append(f"KOL+{unique_kols}")
    if dev_updates > 0:
        parts.append(f"DEV+{dev_updates}")
    return " ".join(parts)


def _feed_display_line(label: str, fact: Any) -> str | None:
    if not isinstance(fact, dict):
        return None
    if fact.get("net_wallets") is None or fact.get("window_minutes") is None:
        return None
    parts = [
        f"• {label}｜{_format_window(int(fact['window_minutes']))}",
        f"{_format_signed_int(int(fact['net_wallets']))}钱包",
    ]
    if fact.get("usd_complete") and fact.get("net_usd") is not None:
        parts.append(format_compact_usd(Decimal(str(fact["net_usd"])), signed=True))
    return " ".join(parts)


def _format_window(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes}m"
    hours, remainder = divmod(minutes, 60)
    if remainder == 0:
        return f"{hours}h"
    return f"{hours}h{remainder}m"


def _format_signed_int(value: int) -> str:
    return f"+{value}" if value > 0 else str(value)


def _trigger_reason_label(family: str | None) -> str:
    return {
        scoring.PRICE: "价格异动",
        scoring.HOLDER: "Holder异动",
        scoring.SMART_MONEY: "Smart Money异动",
        scoring.KOL: "KOL异动",
        scoring.LIQUIDITY: "流动性异动",
        None: "综合异动",
    }.get(family, "综合异动")


def _notifier_accepts_reply_markup(notifier: Notifier | None) -> bool:
    if notifier is None:
        return False
    try:
        parameters = inspect.signature(notifier).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == "reply_markup"
        for parameter in parameters
    )


def _format_pct(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    rounded = value.quantize(Decimal("0.1"))
    text = f"{rounded:f}"
    if text.endswith(".0"):
        text = text[:-2]
    return f"{sign}{text}%"


def format_compact_usd(value: Decimal, *, signed: bool = False) -> str:
    sign = ""
    if signed and value > 0:
        sign = "+"
    elif value < 0:
        sign = "-"
    absolute = abs(value)
    if absolute >= Decimal("1000000"):
        amount = _trim_decimal(absolute / Decimal("1000000"), Decimal("0.01"))
        suffix = "m"
    elif absolute >= Decimal("1000"):
        amount = _trim_decimal(absolute / Decimal("1000"), Decimal("0.1"))
        suffix = "k"
    else:
        amount = _trim_decimal(absolute, Decimal("1"))
        suffix = ""
    return f"{sign}${amount}{suffix}"


def _trim_decimal(value: Decimal, quantum: Decimal) -> str:
    rounded = value.quantize(quantum)
    text = f"{rounded:f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _feed_fact_to_dict(fact: FeedFamilyFact) -> dict[str, Any]:
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
