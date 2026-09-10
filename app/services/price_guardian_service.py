from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.database import session_scope
from app.db.models import (
    AttentionAlertState,
    PriceAlertState,
    PriceSnapshot,
    TokenIntelligenceSnapshot,
    TokenWatchState,
    Wallet,
)
from app.services import attention_scoring
from app.services.gmgn_client import GmgnClient, GmgnHolding
from app.services.holding_classifier import (
    is_below_threshold_position,
    is_canonical_trading_position,
    is_display_position,
    is_trading_position,
)
from app.services.price_quality import (
    PRICE_QUALITY_OUTLIER,
    PRICE_QUALITY_PENDING,
    PRICE_QUALITY_REASON_AMBIGUOUS_NEXT_SNAPSHOT,
    PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT,
    PRICE_QUALITY_REASON_REVERTED_NEXT_SNAPSHOT,
    PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP,
    PRICE_QUALITY_VALID,
    is_close_price,
    percent_change,
    price_quality_jump_threshold,
)
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

Notifier = Callable[[int, str], Awaitable[None]]
PriceAttentionTrigger = Callable[..., Awaitable[object]]

WINDOWS = (5, 15, 60)
UP = "UP"
DOWN = "DOWN"


@dataclass(frozen=True)
class MonitorCandidate:
    holding: GmgnHolding
    monitored: bool
    reason: str


@dataclass(frozen=True)
class PriceMovement:
    window_minutes: int
    change_pct: Decimal
    direction: str


@dataclass(frozen=True)
class PriceAlert:
    wallet_id: int
    chat_id: int
    token_address: str
    symbol: str
    price_usd: Decimal
    balance: Decimal
    usd_value: Decimal | None
    movements: list[PriceMovement]
    watch_state_id: int


@dataclass
class PriceGuardianResult:
    wallets_scanned: int = 0
    holdings_found: int = 0
    trading_positions_found: int = 0
    display_positions: int = 0
    below_threshold_positions: int = 0
    position_symbols: list[str] = field(default_factory=list)
    monitored_tokens: int = 0
    snapshots_saved: int = 0
    alerts_sent: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PriceQualityResult:
    current_status: str
    resolved_pending: bool = False
    resolved_pending_status: str | None = None
    previous_valid_before_pending_usd_value: Decimal | None = None


class PriceGuardianService:
    def __init__(
        self,
        session_factory: sessionmaker,
        gmgn_client: GmgnClient,
        settings: Settings,
        notifier: Notifier | None = None,
        price_attention_trigger: PriceAttentionTrigger | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.gmgn_client = gmgn_client
        self.settings = settings
        self.notifier = notifier
        self.price_attention_trigger = price_attention_trigger
        self._scan_lock = asyncio.Lock()
        self._wallet_semaphore = asyncio.Semaphore(max(settings.price_wallet_concurrency, 1))
        self._wallet_locks: dict[int, asyncio.Lock] = {}

    async def scan_active_wallets(self) -> PriceGuardianResult:
        if self._scan_lock.locked():
            logger.info("Price Guardian scan skipped because previous run is active")
            return PriceGuardianResult()
        async with self._scan_lock:
            return await self._scan_active_wallets_locked()

    async def _scan_active_wallets_locked(self) -> PriceGuardianResult:
        result = PriceGuardianResult()
        logger.info("Price Guardian scan started")
        with session_scope(self.session_factory) as session:
            wallet_ids = list(
                session.scalars(select(Wallet.id).where(Wallet.is_active.is_(True)))
            )
        tasks = [self._scan_wallet_guarded(wallet_id) for wallet_id in wallet_ids]
        for wallet_result in await asyncio.gather(*tasks):
            result.wallets_scanned += wallet_result.wallets_scanned
            result.holdings_found += wallet_result.holdings_found
            result.trading_positions_found += wallet_result.trading_positions_found
            result.display_positions += wallet_result.display_positions
            result.below_threshold_positions += wallet_result.below_threshold_positions
            result.position_symbols.extend(wallet_result.position_symbols)
            result.monitored_tokens += wallet_result.monitored_tokens
            result.snapshots_saved += wallet_result.snapshots_saved
            result.alerts_sent += wallet_result.alerts_sent
            result.errors.extend(wallet_result.errors)
        await self.cleanup_old_snapshots()
        logger.info(
            "Price Guardian scan finished wallets=%s holdings=%s monitored=%s snapshots=%s alerts=%s errors=%s",
            result.wallets_scanned,
            result.holdings_found,
            result.monitored_tokens,
            result.snapshots_saved,
            result.alerts_sent,
            len(result.errors),
        )
        return result

    async def _scan_wallet_guarded(self, wallet_id: int) -> PriceGuardianResult:
        async with self._wallet_semaphore:
            try:
                return await self.scan_wallet(wallet_id)
            except Exception as exc:
                logger.exception("Price Guardian wallet failed wallet_id=%s: %s", wallet_id, exc)
                return PriceGuardianResult(errors=[str(exc)])

    async def scan_wallet(
        self,
        wallet_id: int,
        *,
        send_alerts: bool = True,
        trigger_attention: bool = True,
    ) -> PriceGuardianResult:
        lock = self._wallet_locks.setdefault(wallet_id, asyncio.Lock())
        async with lock:
            return await self._scan_wallet_locked(
                wallet_id,
                send_alerts=send_alerts,
                trigger_attention=trigger_attention,
            )

    async def _scan_wallet_locked(
        self,
        wallet_id: int,
        *,
        send_alerts: bool,
        trigger_attention: bool,
    ) -> PriceGuardianResult:
        result = PriceGuardianResult(wallets_scanned=1)
        with session_scope(self.session_factory) as session:
            wallet = session.scalar(
                select(Wallet).where(Wallet.id == wallet_id, Wallet.is_active.is_(True))
            )
            if not wallet:
                return result
            chain = wallet.chain
            address = wallet.address
            chat_id = wallet.user.telegram_chat_id

        try:
            holdings = await self.fetch_all_holdings(chain, address)
        except Exception as exc:
            logger.warning("GMGN error wallet_id=%s: %s", wallet_id, exc)
            result.errors.append(str(exc))
            return result

        result.holdings_found = len(holdings)
        for holding in holdings:
            if not is_canonical_trading_position(
                holding,
                self.settings.price_excluded_symbols,
            ):
                continue
            result.trading_positions_found += 1
            if is_below_threshold_position(
                holding,
                self.settings.price_monitor_min_usd_value,
                self.settings.price_excluded_symbols,
            ):
                result.below_threshold_positions += 1
            if is_display_position(
                holding,
                self.settings.price_monitor_min_usd_value,
                self.settings.price_excluded_symbols,
            ):
                result.display_positions += 1
                result.position_symbols.append(
                    holding.symbol or holding.contract_address or "UNKNOWN"
                )
        now = utc_now()
        alerts: list[PriceAlert] = []
        price_attention_tokens: dict[tuple[int, str], bool] = {}
        with session_scope(self.session_factory) as session:
            wallet = session.scalar(
                select(Wallet).where(Wallet.id == wallet_id, Wallet.is_active.is_(True))
            )
            if not wallet:
                return result
            trading_contracts = self._trading_contracts(holdings)
            self._sync_watch_states(session, wallet, holdings, trading_contracts, now)
            for holding in holdings:
                if not self._is_watchable_trading_holding(holding):
                    continue
                if (
                    holding.current_price_usd is None
                    or holding.current_price_usd <= 0
                    or holding.balance is None
                ):
                    continue
                watch_state = self._active_watch_state(session, wallet.id, holding.contract_address)
                if not watch_state:
                    continue
                snapshot = PriceSnapshot(
                    wallet_id=wallet.id,
                    chain=wallet.chain,
                    token_address=holding.contract_address,
                    symbol=holding.symbol,
                    price_usd=holding.current_price_usd,
                    balance=holding.balance,
                    usd_value=holding.usd_value,
                    observed_at=now,
                    source_provider="gmgn",
                    source_endpoint="/v1/user/wallet_holdings",
                    source_field=holding.price_source_field,
                )
                quality_result = self._apply_price_quality(
                    session,
                    wallet.id,
                    holding.contract_address,
                    watch_state,
                    snapshot,
                )
                session.add(snapshot)
                result.snapshots_saved += 1
                if snapshot.quality_status == PRICE_QUALITY_VALID:
                    key = (wallet.id, holding.contract_address)
                    price_attention_tokens[key] = price_attention_tokens.get(key, False) or (
                        quality_result.resolved_pending
                        and quality_result.resolved_pending_status == PRICE_QUALITY_VALID
                        and quality_result.previous_valid_before_pending_usd_value is not None
                        and quality_result.previous_valid_before_pending_usd_value
                        >= self.settings.price_monitor_min_usd_value
                        and holding.usd_value is not None
                        and holding.usd_value < self.settings.price_monitor_min_usd_value
                    )
                candidate = self.classify_holding(holding)
                if not candidate.monitored:
                    continue
                result.monitored_tokens += 1
                token_alert = (
                    self._evaluate_alerts(session, wallet, holding, watch_state, now)
                    if (
                        send_alerts
                        and self.settings.price_guardian_alerts_enabled
                        and snapshot.quality_status == PRICE_QUALITY_VALID
                    )
                    else None
                )
                if token_alert:
                    alerts.append(token_alert)
            logger.info(
                "Price Guardian wallet scanned wallet_id=%s holdings=%s monitored=%s snapshots=%s",
                wallet_id,
                result.holdings_found,
                result.monitored_tokens,
                result.snapshots_saved,
            )

        if trigger_attention and self.price_attention_trigger:
            for (trigger_wallet_id, token_address), confirmed_pending_crossing in sorted(price_attention_tokens.items()):
                try:
                    if confirmed_pending_crossing:
                        await self.price_attention_trigger(
                            trigger_wallet_id,
                            token_address,
                            confirmed_pending_usd_threshold_crossing=True,
                        )
                    else:
                        await self.price_attention_trigger(trigger_wallet_id, token_address)
                except Exception as exc:
                    logger.warning(
                        "Price Attention trigger failed wallet_id=%s token=%s: %s",
                        trigger_wallet_id,
                        token_address,
                        exc,
                    )
                    result.errors.append(str(exc))

        if send_alerts and self.settings.price_guardian_alerts_enabled:
            for alert in alerts:
                try:
                    await self._send_alert(alert)
                except Exception as exc:
                    logger.exception("Telegram delivery failed wallet_id=%s: %s", alert.wallet_id, exc)
                    result.errors.append(str(exc))
                    continue
                self._mark_alert_delivered(alert)
                result.alerts_sent += 1
        return result

    async def fetch_all_holdings(self, chain: str, wallet_address: str) -> list[GmgnHolding]:
        holdings: list[GmgnHolding] = []
        cursor: str | None = None
        for page_index in range(self.settings.price_holdings_max_pages):
            page = await self.gmgn_client.get_wallet_holdings_pages(
                {
                    "chain": chain,
                    "wallet_address": wallet_address,
                    "limit": 50,
                    "order_by": "usd_value",
                    "direction": "desc",
                    "hide_airdrop": "true",
                    "hide_closed": "false",
                    **({"cursor": cursor} if cursor else {}),
                },
                max_pages=1,
            )
            if not page:
                break
            holdings.extend(
                self._parse_holding(chain, item) for current_page in page for item in current_page.items
            )
            cursor = page[-1].next_cursor
            if not cursor:
                break
            if page_index == self.settings.price_holdings_max_pages - 1:
                logger.warning(
                    "Price Guardian holdings pagination reached max_pages=%s wallet=%s",
                    self.settings.price_holdings_max_pages,
                    wallet_address,
                )
        return holdings

    def classify_holdings(self, holdings: list[GmgnHolding]) -> list[MonitorCandidate]:
        return [self.classify_holding(holding) for holding in holdings]

    def classify_holding(self, holding: GmgnHolding) -> MonitorCandidate:
        symbol = (holding.symbol or "").upper()
        if symbol in self.settings.price_excluded_symbols:
            return MonitorCandidate(holding, False, "SKIPPED_EXCLUDED_SYMBOL")
        if not holding.contract_address:
            return MonitorCandidate(holding, False, "SKIPPED_NO_CONTRACT")
        if holding.balance is None or holding.balance <= 0:
            return MonitorCandidate(holding, False, "SKIPPED_NO_BALANCE")
        if not is_trading_position(holding):
            return MonitorCandidate(holding, False, "SKIPPED_NO_BUY_HISTORY")
        if holding.usd_value is None or holding.usd_value < self.settings.price_monitor_min_usd_value:
            return MonitorCandidate(holding, False, "SKIPPED_BELOW_MIN_VALUE")
        if holding.current_price_usd is None:
            return MonitorCandidate(holding, False, "SKIPPED_NO_PRICE")
        if holding.current_price_usd <= 0:
            return MonitorCandidate(holding, False, "SKIPPED_NON_POSITIVE_PRICE")
        return MonitorCandidate(holding, True, "MONITORED")

    def _trading_contracts(self, holdings: list[GmgnHolding]) -> set[str]:
        held: set[str] = set()
        for holding in holdings:
            if self._is_watchable_trading_holding(holding):
                held.add(holding.contract_address)
        return held

    def _is_watchable_trading_holding(self, holding: GmgnHolding) -> bool:
        return is_canonical_trading_position(
            holding,
            self.settings.price_excluded_symbols,
        )

    def _sync_watch_states(
        self,
        session,
        wallet: Wallet,
        holdings: list[GmgnHolding],
        trading_contracts: set[str],
        observed_at,
    ) -> None:
        holdings_by_contract = {
            holding.contract_address: holding
            for holding in holdings
            if holding.contract_address in trading_contracts
        }
        active_states = list(
            session.scalars(
                select(TokenWatchState).where(
                    TokenWatchState.wallet_id == wallet.id,
                    TokenWatchState.active.is_(True),
                )
            )
        )
        for state in active_states:
            if state.token_address not in trading_contracts:
                state.active = False
                state.ended_at = observed_at
        for contract, holding in holdings_by_contract.items():
            state = self._active_watch_state(session, wallet.id, contract)
            if state:
                state.symbol = holding.symbol
                state.last_seen_at = observed_at
                continue
            state = TokenWatchState(
                wallet_id=wallet.id,
                chain=wallet.chain,
                token_address=contract,
                symbol=holding.symbol,
                active=True,
                started_at=observed_at,
                last_seen_at=observed_at,
            )
            session.add(state)
            session.flush()
            self._reset_alert_states_for_token(session, wallet.id, contract, observed_at)
            self._reset_attention_alert_state_for_token(session, wallet.id, contract)

    def _active_watch_state(
        self, session, wallet_id: int, token_address: str
    ) -> TokenWatchState | None:
        return session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == token_address,
                TokenWatchState.active.is_(True),
            )
        )

    def _evaluate_alerts(
        self,
        session,
        wallet: Wallet,
        holding: GmgnHolding,
        watch_state: TokenWatchState,
        observed_at,
    ) -> PriceAlert | None:
        if not holding.contract_address or holding.current_price_usd is None or holding.balance is None:
            return None
        movements: list[PriceMovement] = []
        thresholds = self._thresholds()
        for window_minutes, threshold in thresholds.items():
            baseline = self._baseline_snapshot(
                session,
                wallet.id,
                holding.contract_address,
                watch_state.started_at,
                observed_at,
                window_minutes,
            )
            if not baseline or baseline.price_usd is None or baseline.price_usd <= 0:
                continue
            change_pct = ((holding.current_price_usd - baseline.price_usd) / baseline.price_usd) * Decimal("100")
            direction = UP if change_pct > 0 else DOWN
            self._reset_opposite_if_needed(
                session,
                wallet.id,
                holding.contract_address,
                window_minutes,
                direction,
                observed_at,
            )
            if self._should_alert(
                session,
                wallet.id,
                holding.contract_address,
                window_minutes,
                direction,
                change_pct,
                threshold,
            ):
                movements.append(
                    PriceMovement(
                        window_minutes=window_minutes,
                        change_pct=change_pct,
                        direction=direction,
                    )
                )
            self._reset_if_recovered(
                session,
                wallet.id,
                holding.contract_address,
                window_minutes,
                change_pct,
                threshold,
                observed_at,
            )
        if not movements:
            return None
        return PriceAlert(
            wallet_id=wallet.id,
            chat_id=wallet.user.telegram_chat_id,
            token_address=holding.contract_address,
            symbol=holding.symbol or holding.contract_address,
            price_usd=holding.current_price_usd,
            balance=holding.balance,
            usd_value=holding.usd_value,
            movements=movements,
            watch_state_id=watch_state.id,
        )

    def _baseline_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at,
        observed_at,
        window_minutes: int,
    ) -> PriceSnapshot | None:
        target = observed_at - timedelta(minutes=window_minutes)
        tolerance = timedelta(seconds=self._baseline_tolerance_seconds(window_minutes))
        lower_bound = max(target - tolerance, watch_started_at)
        upper_bound = target + tolerance
        candidates = list(
            session.scalars(
                select(PriceSnapshot)
                .where(
                    PriceSnapshot.wallet_id == wallet_id,
                    PriceSnapshot.token_address == token_address,
                    PriceSnapshot.observed_at >= lower_bound,
                    PriceSnapshot.observed_at <= upper_bound,
                    _valid_price_snapshot_clause(),
                )
            )
        )
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda snapshot: abs((snapshot.observed_at - target).total_seconds()),
        )

    def _should_alert(
        self,
        session,
        wallet_id: int,
        token_address: str,
        window_minutes: int,
        direction: str,
        change_pct: Decimal,
        threshold: Decimal,
    ) -> bool:
        if abs(change_pct) < threshold:
            return False
        state = self._find_state(session, wallet_id, token_address, window_minutes, direction)
        if state is None or not state.active or state.last_alert_change_pct is None:
            logger.info(
                "Price alert triggered symbol_token=%s window=%s change_pct=%s",
                token_address,
                window_minutes,
                change_pct,
            )
            return True
        previous = abs(Decimal(str(state.last_alert_change_pct)))
        if abs(change_pct) - previous >= self.settings.price_alert_escalation_step_percent:
            logger.info(
                "Price alert escalation token=%s window=%s change_pct=%s",
                token_address,
                window_minutes,
                change_pct,
            )
            return True
        return False

    def _reset_if_recovered(
        self,
        session,
        wallet_id: int,
        token_address: str,
        window_minutes: int,
        change_pct: Decimal,
        threshold: Decimal,
        observed_at,
    ) -> None:
        if abs(change_pct) >= threshold * self.settings.price_alert_reset_ratio:
            return
        for direction in (UP, DOWN):
            state = self._find_state(session, wallet_id, token_address, window_minutes, direction)
            if state and state.active:
                state.active = False
                state.updated_at = observed_at

    def _reset_opposite_if_needed(
        self,
        session,
        wallet_id: int,
        token_address: str,
        window_minutes: int,
        direction: str,
        observed_at,
    ) -> None:
        opposite = DOWN if direction == UP else UP
        state = self._find_state(session, wallet_id, token_address, window_minutes, opposite)
        if state and state.active:
            state.active = False
            state.updated_at = observed_at

    def _get_or_create_state(
        self,
        session,
        wallet_id: int,
        token_address: str,
        window_minutes: int,
        direction: str,
    ) -> PriceAlertState:
        state = self._find_state(session, wallet_id, token_address, window_minutes, direction)
        if state:
            return state
        state = PriceAlertState(
            wallet_id=wallet_id,
            token_address=token_address,
            window_minutes=window_minutes,
            direction=direction,
            active=False,
        )
        session.add(state)
        session.flush()
        return state

    def _reset_alert_states_for_token(
        self, session, wallet_id: int, token_address: str, observed_at
    ) -> None:
        states = list(
            session.scalars(
                select(PriceAlertState).where(
                    PriceAlertState.wallet_id == wallet_id,
                    PriceAlertState.token_address == token_address,
                )
            )
        )
        for state in states:
            state.active = False
            state.last_alert_at = None
            state.last_alert_change_pct = None
            state.updated_at = observed_at

    def _reset_attention_alert_state_for_token(
        self, session, wallet_id: int, token_address: str
    ) -> None:
        session.execute(
            delete(AttentionAlertState).where(
                AttentionAlertState.wallet_id == wallet_id,
                AttentionAlertState.token_address == token_address,
            )
        )

    def _find_state(
        self,
        session,
        wallet_id: int,
        token_address: str,
        window_minutes: int,
        direction: str,
    ) -> PriceAlertState | None:
        return session.scalar(
            select(PriceAlertState).where(
                PriceAlertState.wallet_id == wallet_id,
                PriceAlertState.token_address == token_address,
                PriceAlertState.window_minutes == window_minutes,
                PriceAlertState.direction == direction,
            )
        )

    @staticmethod
    def _mark_alerted(state: PriceAlertState, change_pct: Decimal, observed_at) -> None:
        state.active = True
        state.last_alert_at = observed_at
        state.last_alert_change_pct = change_pct
        state.updated_at = observed_at

    async def _send_alert(self, alert: PriceAlert) -> None:
        if not self.notifier:
            raise RuntimeError("telegram_notifier_not_configured")
        await self.notifier(alert.chat_id, format_price_alert(alert))
        logger.info("Telegram delivery success wallet_id=%s token=%s", alert.wallet_id, alert.token_address)

    def _mark_alert_delivered(self, alert: PriceAlert) -> None:
        delivered_at = utc_now()
        with session_scope(self.session_factory) as session:
            for movement in alert.movements:
                state = self._get_or_create_state(
                    session,
                    alert.wallet_id,
                    alert.token_address,
                    movement.window_minutes,
                    movement.direction,
                )
                self._mark_alerted(state, movement.change_pct, delivered_at)

    async def cleanup_old_snapshots(self) -> int:
        cutoff = utc_now() - timedelta(hours=self.settings.price_history_retention_hours)
        with session_scope(self.session_factory) as session:
            result = session.execute(delete(PriceSnapshot).where(PriceSnapshot.observed_at < cutoff))
            return int(result.rowcount or 0)

    def _thresholds(self) -> dict[int, Decimal]:
        return {
            5: self.settings.price_alert_5m_percent,
            15: self.settings.price_alert_15m_percent,
            60: self.settings.price_alert_60m_percent,
        }

    def _apply_price_quality(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_state: TokenWatchState,
        snapshot: PriceSnapshot,
    ) -> PriceQualityResult:
        current_price = Decimal(str(snapshot.price_usd))
        pending = self._latest_pending_snapshot(session, wallet_id, token_address, watch_state.started_at)
        if pending:
            return self._resolve_pending_snapshot(
                session,
                wallet_id,
                token_address,
                watch_state,
                pending,
                snapshot,
                current_price,
            )

        previous_valid = self._latest_valid_snapshot(session, wallet_id, token_address, watch_state.started_at)
        if not previous_valid:
            snapshot.quality_status = PRICE_QUALITY_VALID
            return PriceQualityResult(current_status=PRICE_QUALITY_VALID)
        previous_price = Decimal(str(previous_valid.price_usd))
        change = percent_change(current_price, previous_price)
        threshold = self._quality_jump_threshold(
            session,
            wallet_id,
            token_address,
            watch_state.started_at,
            current_price,
            previous_price,
        )
        if change is not None and abs(change) >= threshold:
            snapshot.quality_status = PRICE_QUALITY_PENDING
            snapshot.quality_reason = PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP
            return PriceQualityResult(current_status=PRICE_QUALITY_PENDING)
        snapshot.quality_status = PRICE_QUALITY_VALID
        return PriceQualityResult(current_status=PRICE_QUALITY_VALID)

    def _resolve_pending_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_state: TokenWatchState,
        pending: PriceSnapshot,
        snapshot: PriceSnapshot,
        current_price: Decimal,
    ) -> PriceQualityResult:
        pending_price = Decimal(str(pending.price_usd))
        previous_valid = self._latest_valid_snapshot(
            session,
            wallet_id,
            token_address,
            watch_state.started_at,
            before_observed_at=pending.observed_at,
        )
        if not previous_valid:
            pending.quality_status = PRICE_QUALITY_VALID
            pending.quality_reason = PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
            snapshot.quality_status = PRICE_QUALITY_VALID
            return PriceQualityResult(
                current_status=PRICE_QUALITY_VALID,
                resolved_pending=True,
                resolved_pending_status=PRICE_QUALITY_VALID,
            )

        previous_price = Decimal(str(previous_valid.price_usd))
        previous_valid_usd = _decimal_or_none(previous_valid.usd_value)
        if is_close_price(current_price, pending_price):
            pending.quality_status = PRICE_QUALITY_VALID
            pending.quality_reason = PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
            snapshot.quality_status = PRICE_QUALITY_VALID
            snapshot.quality_reason = PRICE_QUALITY_REASON_CONFIRMED_NEXT_SNAPSHOT
            return PriceQualityResult(
                current_status=PRICE_QUALITY_VALID,
                resolved_pending=True,
                resolved_pending_status=PRICE_QUALITY_VALID,
                previous_valid_before_pending_usd_value=previous_valid_usd,
            )

        if is_close_price(current_price, previous_price) and not is_close_price(current_price, pending_price):
            pending.quality_status = PRICE_QUALITY_OUTLIER
            pending.quality_reason = PRICE_QUALITY_REASON_REVERTED_NEXT_SNAPSHOT
            snapshot.quality_status = PRICE_QUALITY_VALID
            return PriceQualityResult(
                current_status=PRICE_QUALITY_VALID,
                resolved_pending=True,
                resolved_pending_status=PRICE_QUALITY_OUTLIER,
                previous_valid_before_pending_usd_value=previous_valid_usd,
            )

        pending.quality_status = PRICE_QUALITY_OUTLIER
        pending.quality_reason = PRICE_QUALITY_REASON_AMBIGUOUS_NEXT_SNAPSHOT
        threshold = self._quality_jump_threshold(
            session,
            wallet_id,
            token_address,
            watch_state.started_at,
            current_price,
            previous_price,
        )
        change = percent_change(current_price, previous_price)
        if change is not None and abs(change) >= threshold:
            snapshot.quality_status = PRICE_QUALITY_PENDING
            snapshot.quality_reason = PRICE_QUALITY_REASON_SINGLE_SNAPSHOT_JUMP
        else:
            snapshot.quality_status = PRICE_QUALITY_VALID
        return PriceQualityResult(
            current_status=snapshot.quality_status,
            resolved_pending=True,
            resolved_pending_status=PRICE_QUALITY_OUTLIER,
            previous_valid_before_pending_usd_value=previous_valid_usd,
        )

    def _latest_valid_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at,
        before_observed_at=None,
    ) -> PriceSnapshot | None:
        query = select(PriceSnapshot).where(
            PriceSnapshot.wallet_id == wallet_id,
            PriceSnapshot.token_address == token_address,
            PriceSnapshot.observed_at >= watch_started_at,
            _valid_price_snapshot_clause(),
        )
        if before_observed_at is not None:
            query = query.where(PriceSnapshot.observed_at < before_observed_at)
        return session.scalar(query.order_by(PriceSnapshot.observed_at.desc()))

    def _latest_pending_snapshot(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at,
    ) -> PriceSnapshot | None:
        return session.scalar(
            select(PriceSnapshot)
            .where(
                PriceSnapshot.wallet_id == wallet_id,
                PriceSnapshot.token_address == token_address,
                PriceSnapshot.observed_at >= watch_started_at,
                PriceSnapshot.quality_status == PRICE_QUALITY_PENDING,
            )
            .order_by(PriceSnapshot.observed_at.desc())
        )

    def _quality_jump_threshold(
        self,
        session,
        wallet_id: int,
        token_address: str,
        watch_started_at,
        current_price: Decimal,
        previous_price: Decimal,
    ) -> Decimal:
        latest_intel = session.scalar(
            select(TokenIntelligenceSnapshot)
            .where(
                TokenIntelligenceSnapshot.wallet_id == wallet_id,
                TokenIntelligenceSnapshot.token_address == token_address,
                TokenIntelligenceSnapshot.observed_at >= watch_started_at,
            )
            .order_by(TokenIntelligenceSnapshot.observed_at.desc())
        )
        market_cap = Decimal(str(latest_intel.market_cap_usd)) if latest_intel and latest_intel.market_cap_usd else None
        liquidity = Decimal(str(latest_intel.liquidity_usd)) if latest_intel and latest_intel.liquidity_usd else None
        direction = attention_scoring.POSITIVE if current_price > previous_price else attention_scoring.NEGATIVE
        effective_threshold = (
            attention_scoring.price_threshold(5, market_cap, liquidity, direction)
            if market_cap is not None and market_cap > 0
            else self.settings.price_alert_5m_percent
        )
        return price_quality_jump_threshold(effective_threshold)

    @staticmethod
    def _baseline_tolerance_seconds(window_minutes: int) -> int:
        return {5: 75, 15: 90, 60: 120}.get(window_minutes, 90)

    @staticmethod
    def _parse_holding(chain: str, item: dict) -> GmgnHolding:
        from app.services.gmgn_client import parse_holding

        return parse_holding(chain, item)


def format_price_alert(alert: PriceAlert) -> str:
    primary = max(alert.movements, key=lambda movement: abs(movement.change_pct))
    emoji = "🚀" if primary.direction == UP else "🔴"
    direction_text = "快速上涨" if primary.direction == UP else "快速下跌"
    lines = [f"{emoji} {alert.symbol} {direction_text}", ""]
    for movement in sorted(alert.movements, key=lambda item: item.window_minutes):
        lines.append(f"{_window_label(movement.window_minutes)}：{_format_pct(movement.change_pct)}")
    lines.extend(
        [
            "",
            f"当前价格：${format_price(alert.price_usd)}",
            f"当前持仓：{format_balance(alert.balance)} {alert.symbol}",
        ]
    )
    if alert.usd_value is not None:
        lines.append(f"当前价值：${format_usd(alert.usd_value)}")
    return "\n".join(lines)


def format_price(value: Decimal) -> str:
    if value >= Decimal("1"):
        return f"{value.quantize(Decimal('0.01')):f}"
    text = f"{value.normalize():f}"
    if "." not in text:
        return text
    decimals = text.split(".", 1)[1]
    leading_zeroes = len(decimals) - len(decimals.lstrip("0"))
    places = min(max(leading_zeroes + 4, 6), 12)
    return f"{value:.{places}f}".rstrip("0").rstrip(".")


def format_balance(value: Decimal) -> str:
    if value == 0:
        return "0"
    abs_value = abs(value)
    if abs_value >= Decimal("1000000"):
        return f"{value.quantize(Decimal('1')):,}"
    if abs_value >= Decimal("1000"):
        return f"{value.quantize(Decimal('0.01')):,}".rstrip("0").rstrip(".")
    if abs_value >= Decimal("1"):
        return f"{value.quantize(Decimal('0.0001')):,}".rstrip("0").rstrip(".")
    return f"{value.normalize():f}".rstrip("0").rstrip(".")


def format_usd(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01')):,.2f}"


def _format_pct(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value.quantize(Decimal('0.1'))}%"


def _window_label(window_minutes: int) -> str:
    if window_minutes == 60:
        return "1小时"
    return f"{window_minutes}分钟"


def _valid_price_snapshot_clause():
    return or_(
        PriceSnapshot.quality_status.is_(None),
        PriceSnapshot.quality_status == PRICE_QUALITY_VALID,
    )


def _decimal_or_none(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
