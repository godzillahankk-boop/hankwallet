from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.orm import sessionmaker

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
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

Notifier = Callable[[int, str], Awaitable[None]]

MARKET_SIGNAL_GROUPS = [
    {"signal_type": [6, 7]},
    {"signal_type": [12]},
    {"signal_type": [20]},
]

ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS = (0, 1, 2)


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


class AttentionEngineService:
    def __init__(
        self,
        session_factory: sessionmaker,
        gmgn_client: GmgnClient,
        settings: Settings,
        notifier: Notifier | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.gmgn_client = gmgn_client
        self.settings = settings
        self.notifier = notifier

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
            smart_score, smart_direction = self._feed_family(session, wallet_id, token_address, scoring.SMART_MONEY)
            kol_score, kol_direction = self._feed_family(session, wallet_id, token_address, scoring.KOL)
            liq_score, liq_direction = self._liquidity_family(
                session,
                wallet_id,
                token_address,
                latest_intel,
                watched.watch_started_at,
            )
            family_scores = {
                scoring.PRICE: price_score,
                scoring.HOLDER: holder_family,
                scoring.SMART_MONEY: smart_score,
                scoring.KOL: kol_score,
                scoring.LIQUIDITY: liq_score,
            }
            primary_family, primary_score = max(family_scores.items(), key=lambda item: item[1])
            if primary_score == 0:
                primary_family = None
            secondary = scoring.secondary_signal_score(family_scores, primary_family)
            exposure = scoring.position_exposure_score(watched.usd_value)
            base = min(100, primary_score + exposure + abnormality + secondary.score)
            dev_modifier = 0
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
            )
            should_notify = self._should_notify(session, wallet_id, token_address, level, final, direction, family_scores, now)
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

    async def handle_price_snapshot_update(self, wallet_id: int, token_address: str) -> AttentionAssessment | None:
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
            elif previous_usd is not None and previous_usd >= min_usd:
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
        await self.notifier(chat_id, format_attention_alert(assessment))
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
                & (PriceSnapshot.observed_at == latest_price_observed),
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
        if not latest_intel:
            return 0, scoring.NEUTRAL
        baseline = self._baseline_token_snapshot(
            session,
            wallet_id,
            token_address,
            60,
            latest_intel.observed_at,
            watch_started_at,
        )
        score = scoring.holder_breadth_score(
            latest_intel.holder_count,
            baseline.holder_count if baseline else None,
        )
        if score == 0 or latest_intel.holder_count is None or not baseline or baseline.holder_count is None:
            return score, scoring.NEUTRAL
        if latest_intel.holder_count > baseline.holder_count:
            return score, scoring.POSITIVE
        if latest_intel.holder_count < baseline.holder_count:
            return score, scoring.NEGATIVE
        return score, scoring.NEUTRAL

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
        watch_started_at = self._watch_started_at(session, wallet_id, token_address)
        if not watch_started_at:
            return 0, scoring.NEUTRAL
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
            return 0, scoring.NEUTRAL
        buy_wallets: set[str] = set()
        sell_wallets: set[str] = set()
        buy_usd = Decimal("0")
        sell_usd = Decimal("0")
        buy_activity_count = 0
        sell_activity_count = 0
        for event in events:
            payload = _json_loads(event.payload_json)
            wallet = payload.get("wallet")
            usd = _decimal_or_none(payload.get("usd_value")) or Decimal("0")
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
        if family == scoring.SMART_MONEY:
            net_flow = buy_usd - sell_usd
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
            if buy_score > sell_score:
                return buy_score, scoring.POSITIVE
            if sell_score > buy_score:
                return sell_score, scoring.NEGATIVE
            if buy_score > 0:
                return buy_score, scoring.MIXED
            return 0, scoring.NEUTRAL
        buy_score = scoring.kol_score(len(buy_wallets), 0, buy_activity_count)
        sell_score = scoring.kol_score(len(sell_wallets), 0, sell_activity_count)
        if buy_score > sell_score:
            return buy_score, scoring.POSITIVE
        if sell_score > buy_score:
            return sell_score, scoring.NEGATIVE
        if buy_score > 0:
            return buy_score, scoring.MIXED
        return 0, scoring.NEUTRAL

    def _liquidity_family(
        self,
        session,
        wallet_id: int,
        token_address: str,
        latest_intel,
        watch_started_at: datetime | None = None,
    ) -> tuple[int, str]:
        if not latest_intel:
            return 0, scoring.NEUTRAL
        baseline = self._baseline_token_snapshot(
            session,
            wallet_id,
            token_address,
            60,
            latest_intel.observed_at,
            watch_started_at,
        )
        score = scoring.liquidity_score(
            _decimal_or_none(latest_intel.liquidity_usd),
            _decimal_or_none(baseline.liquidity_usd if baseline else None),
            None,
        )
        return score, scoring.NEGATIVE if score else scoring.NEUTRAL

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
        return {
            "assessment_trigger": assessment_trigger,
            "family_scores": family_scores,
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
    ) -> bool:
        if level not in {scoring.WARNING, scoring.CRITICAL}:
            return False
        state = session.scalar(
            select(AttentionAlertState).where(
                AttentionAlertState.wallet_id == wallet_id,
                AttentionAlertState.token_address == token_address,
            )
        )
        if not state or not state.last_notified_at:
            return True
        previous_scores = _json_loads(state.family_scores_json)
        if state.last_attention_level == scoring.WARNING and level == scoring.CRITICAL:
            return True
        if state.last_final_score is not None and final_score - state.last_final_score >= 15:
            return True
        if state.last_direction and state.last_direction != direction and direction != scoring.NEUTRAL:
            return True
        if _has_new_strong_family(previous_scores, family_scores):
            return True
        if level == scoring.CRITICAL:
            return now - state.last_notified_at >= timedelta(minutes=self.settings.attention_critical_cooldown_minutes)
        return now - state.last_notified_at >= timedelta(minutes=self.settings.attention_warning_cooldown_minutes)

    async def _notify_assessment(self, assessment: AttentionAssessment) -> bool:
        if not self.notifier:
            return False
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
            chat_id = wallet.user.telegram_chat_id
        await self._send_notification_with_retry(chat_id, format_attention_alert(assessment), assessment)
        if not self._mark_notified(assessment):
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

    async def _send_notification_with_retry(
        self,
        chat_id: int,
        text: str,
        assessment: AttentionAssessment,
    ) -> None:
        last_error: Exception | None = None
        for attempt, delay in enumerate(ATTENTION_NOTIFICATION_RETRY_DELAYS_SECONDS, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                await self.notifier(chat_id, text)
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

    def _mark_notified(self, assessment: AttentionAssessment) -> bool:
        with session_scope(self.session_factory) as session:
            if not self._assessment_matches_current_watch(session, assessment):
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
            state.family_scores_json = json.dumps(_family_scores_from_assessment(assessment), ensure_ascii=False)
            return True

    def _assessment_matches_current_watch(self, session, assessment: AttentionAssessment) -> bool:
        started_at = session.scalar(
            select(TokenWatchState.started_at).where(
                TokenWatchState.wallet_id == assessment.wallet_id,
                TokenWatchState.token_address == assessment.token_address,
                TokenWatchState.active.is_(True),
            )
        )
        return bool(started_at and assessment.assessed_at >= started_at)

    async def cleanup_old_snapshots(self) -> None:
        cutoff = utc_now() - timedelta(days=7)
        with session_scope(self.session_factory) as session:
            session.execute(delete(TokenIntelligenceSnapshot).where(TokenIntelligenceSnapshot.observed_at < cutoff))
            session.execute(delete(TopHolderSnapshot).where(TopHolderSnapshot.observed_at < cutoff))


def fingerprint_event(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def format_attention_alert(assessment: AttentionAssessment) -> str:
    symbol = assessment.symbol or assessment.token_address[:10]
    emoji = "🔴" if assessment.direction == scoring.NEGATIVE else "🟠" if assessment.direction == scoring.MIXED else "🟢"
    lines = [
        f"{emoji} {symbol} 持仓异动",
        "",
        f"Attention：{assessment.final_attention_score}｜{assessment.attention_level}",
        f"方向：{_direction_label(assessment.direction)}",
        "",
        "近况：",
    ]
    evidence = _json_loads(assessment.evidence_json)
    if evidence.get("price_change_pct"):
        lines.append(f"• 价格 {_format_pct(Decimal(str(evidence['price_change_pct'])))}")
    lines.append(f"• Holder Score {assessment.holder_family_score}")
    lines.append(f"• Smart Money Score {assessment.smart_money_score}")
    lines.append(f"• KOL Score {assessment.kol_score}")
    lines.append(f"• Liquidity Score {assessment.liquidity_score}")
    lines.extend(
        [
            "",
            f"主要触发：{assessment.primary_family or 'none'}",
            "当前仅提示异动事实，不构成交易建议。",
        ]
    )
    return "\n".join(lines)


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


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _has_new_strong_family(previous_scores: dict[str, Any], current_scores: dict[str, int]) -> bool:
    for family, score in current_scores.items():
        previous = int(previous_scores.get(family, 0) or 0)
        if score >= 30 and previous < 30:
            return True
    return False


def _family_scores_from_assessment(assessment: AttentionAssessment) -> dict[str, int]:
    return {
        scoring.PRICE: assessment.price_score,
        scoring.HOLDER: assessment.holder_family_score,
        scoring.SMART_MONEY: assessment.smart_money_score,
        scoring.KOL: assessment.kol_score,
        scoring.LIQUIDITY: assessment.liquidity_score,
    }


def _direction_label(direction: str) -> str:
    return {
        scoring.POSITIVE: "Positive",
        scoring.NEGATIVE: "Negative",
        scoring.MIXED: "Mixed",
        scoring.NEUTRAL: "Neutral",
    }.get(direction, direction)


def _format_pct(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value.quantize(Decimal('0.1'))}%"
