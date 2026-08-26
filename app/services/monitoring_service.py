from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.database import session_scope
from app.db.models import MonitoringRun, Position, Token, User, Wallet, WalletTokenBalance
from app.services.chain_client import ChainClient, TokenBalance
from app.services.position_service import PositionService
from app.services.transaction_parser import (
    ParsedTokenEvent,
    looks_like_spam_token,
    parse_wallet_token_events,
)
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

Notifier = Callable[[int, str], Awaitable[None]]


@dataclass
class ScanResult:
    wallet_id: int
    tokens_found: int = 0
    positions_created: int = 0
    positions_updated: int = 0
    positions_closed: int = 0
    ignored_tokens: int = 0
    new_positions: list[str] = field(default_factory=list)
    closed_positions: list[str] = field(default_factory=list)
    skipped_due_to_lock: bool = False
    error_message: str | None = None


class MonitoringService:
    def __init__(
        self,
        session_factory: sessionmaker,
        chain_client: ChainClient,
        settings: Settings,
        notifier: Notifier | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.chain_client = chain_client
        self.settings = settings
        self.notifier = notifier
        self._locks: dict[int, asyncio.Lock] = {}

    async def scan_wallet(self, wallet_id: int, reason: str = "scheduler") -> ScanResult:
        lock = self._locks.setdefault(wallet_id, asyncio.Lock())
        if lock.locked():
            logger.info("Wallet Scan skipped because lock is active wallet_id=%s", wallet_id)
            return ScanResult(wallet_id=wallet_id, skipped_due_to_lock=True)

        async with lock:
            return await self._scan_wallet_locked(wallet_id, reason)

    async def refresh_wallet_balances(self, wallet_id: int) -> int:
        with session_scope(self.session_factory) as session:
            wallet = session.scalar(
                select(Wallet).where(Wallet.id == wallet_id, Wallet.is_active.is_(True))
            )
            if not wallet:
                return 0
            chain = wallet.chain
            address = wallet.address

        native_balance = await self._get_native_balance(chain, address)
        balances = await self.chain_client.get_token_balances(chain, address)

        with session_scope(self.session_factory) as session:
            wallet = session.scalar(
                select(Wallet).where(Wallet.id == wallet_id, Wallet.is_active.is_(True))
            )
            if not wallet:
                return 0
            self._sync_balance_snapshots(
                session,
                wallet,
                [native_balance] + balances if native_balance else balances,
            )
        return len(balances) + (1 if native_balance else 0)

    async def _scan_wallet_locked(self, wallet_id: int, reason: str) -> ScanResult:
        started_at = utc_now()
        result = ScanResult(wallet_id=wallet_id)
        chat_id: int | None = None
        logger.info("Wallet Scan Start wallet_id=%s reason=%s", wallet_id, reason)

        with session_scope(self.session_factory) as session:
            wallet = session.get(Wallet, wallet_id)
            if not wallet or not wallet.is_active:
                result.error_message = "wallet_not_found_or_inactive"
                return result
            run = MonitoringRun(wallet_id=wallet.id, started_at=started_at, status="RUNNING")
            session.add(run)
            session.flush()

        try:
            with session_scope(self.session_factory) as session:
                wallet = session.scalar(
                    select(Wallet).where(Wallet.id == wallet_id, Wallet.is_active.is_(True))
                )
                if not wallet:
                    result.error_message = "wallet_not_found_or_inactive"
                    return result
                chat_id = wallet.user.telegram_chat_id
                native_balance = await self._get_native_balance(wallet.chain, wallet.address)
                balances = await self.chain_client.get_token_balances(wallet.chain, wallet.address)
                result.tokens_found = len(balances)
                self._sync_balance_snapshots(
                    session,
                    wallet,
                    [native_balance] + balances if native_balance else balances,
                )
                try:
                    transfers = await self.chain_client.get_token_transfers(
                        wallet.chain,
                        wallet.address,
                        limit=self.settings.token_transfer_lookback_limit,
                    )
                except Exception as exc:
                    logger.warning(
                        "Token transfer fetch failed wallet_id=%s; continuing with balances only: %s",
                        wallet.id,
                        exc,
                    )
                    transfers = []
                events = parse_wallet_token_events(wallet.address, transfers)

                position_service = PositionService(session)
                balances_by_token = {
                    balance.contract_address: balance for balance in balances
                }
                for balance in balances:
                    position_service.get_or_create_token(balance)

                await self._apply_events(
                    session,
                    position_service,
                    wallet,
                    balances_by_token,
                    events,
                    result,
                )
                if balances or events:
                    self._sync_open_positions_with_balances(
                        session,
                        wallet.id,
                        balances_by_token,
                        result,
                    )
                elif position_service.list_open_positions_for_wallet(wallet.id):
                    logger.warning(
                        "Wallet Scan returned zero token balances; keeping existing open positions wallet_id=%s",
                        wallet.id,
                    )
                wallet.last_scanned_at = utc_now()

            await self._send_scan_notifications(chat_id, result, reason)
            self._finish_run(wallet_id, started_at, "SUCCESS", result)
        except Exception as exc:
            logger.exception("Wallet Scan failed wallet_id=%s: %s", wallet_id, exc)
            result.error_message = str(exc)
            self._finish_run(wallet_id, started_at, "FAILED", result)
            if chat_id and self.notifier:
                try:
                    await self.notifier(
                        chat_id,
                        "\n".join(
                            [
                                "⚠️ 钱包扫描失败",
                                "",
                                result.error_message,
                            ]
                        ),
                    )
                except Exception as notify_exc:
                    logger.exception("Telegram Error failure notice failed: %s", notify_exc)
        logger.info(
            "Wallet Scan End wallet_id=%s tokens=%s created=%s updated=%s closed=%s",
            wallet_id,
            result.tokens_found,
            result.positions_created,
            result.positions_updated,
            result.positions_closed,
        )
        return result

    async def _get_native_balance(
        self, chain: str, wallet_address: str
    ) -> TokenBalance | None:
        try:
            if hasattr(self.chain_client, "get_native_token_balance"):
                return await self.chain_client.get_native_token_balance(chain, wallet_address)
            amount = await self.chain_client.get_native_balance(chain, wallet_address)
            return TokenBalance(
                chain=chain,
                contract_address="native",
                symbol="ETH",
                name="ETH",
                decimals=18,
                balance=amount,
                is_native=True,
            )
        except Exception as exc:
            logger.warning("Native balance fetch failed chain=%s: %s", chain, exc)
            return None

    def _sync_balance_snapshots(
        self,
        session,
        wallet: Wallet,
        balances: list[TokenBalance],
    ) -> None:
        checked_at = utc_now()
        for balance in balances:
            asset_key = "native" if balance.is_native else balance.contract_address
            snapshot = session.scalar(
                select(WalletTokenBalance).where(
                    WalletTokenBalance.wallet_id == wallet.id,
                    WalletTokenBalance.asset_key == asset_key,
                )
            )
            if not snapshot:
                snapshot = WalletTokenBalance(
                    wallet_id=wallet.id,
                    chain=wallet.chain,
                    asset_key=asset_key,
                    last_checked_at=checked_at,
                )
                session.add(snapshot)
            snapshot.chain = wallet.chain
            snapshot.contract_address = None if balance.is_native else balance.contract_address
            snapshot.symbol = balance.symbol
            snapshot.name = balance.name
            snapshot.decimals = balance.decimals
            snapshot.token_amount = balance.balance
            snapshot.exchange_rate_usd = balance.exchange_rate_usd
            snapshot.usd_value = balance.usd_value
            snapshot.is_native = balance.is_native
            snapshot.last_checked_at = checked_at

    async def _apply_events(
        self,
        session,
        position_service: PositionService,
        wallet: Wallet,
        balances_by_token: dict[str, TokenBalance],
        events: list[ParsedTokenEvent],
        result: ScanResult,
    ) -> None:
        for event in events:
            try:
                balance = balances_by_token.get(event.token_address)
                current_balance = balance.balance if balance else Decimal("0")
                token = self._get_or_create_token_from_event(
                    session, wallet.chain, event
                )

                if event.tx_type == "BUY" and current_balance > self.settings.dust_threshold:
                    position, created = position_service.get_or_create_open_position(
                        wallet.id,
                        token,
                        current_balance,
                        event.tx_timestamp if isinstance(event.tx_timestamp, datetime) else None,
                    )
                    recorded = position_service.record_event(position, event)
                    changed = self._set_position_balance(position, current_balance)
                    if created:
                        result.positions_created += 1
                        result.new_positions.append(token.symbol or token.contract_address)
                    elif changed or recorded:
                        result.positions_updated += 1

                elif event.tx_type == "SELL":
                    position = position_service.get_open_position(wallet.id, token.id)
                    if not position:
                        continue
                    recorded = position_service.record_event(position, event)
                    changed = self._set_position_balance(position, current_balance)
                    if changed or recorded:
                        result.positions_updated += 1

                elif event.tx_type in {"TRANSFER_IN", "AIRDROP", "UNKNOWN"}:
                    if (
                        current_balance <= self.settings.dust_threshold
                        or looks_like_spam_token(token.symbol, token.name)
                    ):
                        position_service.get_or_create_ignored_position(
                            wallet.id, token, current_balance
                        )
                        result.ignored_tokens += 1
            except Exception as exc:
                logger.exception(
                    "Parsing Error wallet_id=%s tx=%s token=%s: %s",
                    wallet.id,
                    event.tx_hash,
                    event.token_address,
                    exc,
                )

    def _sync_open_positions_with_balances(
        self,
        session,
        wallet_id: int,
        balances_by_token: dict[str, TokenBalance],
        result: ScanResult,
    ) -> None:
        open_positions = list(
            session.scalars(
                select(Position)
                .join(Token)
                .where(Position.wallet_id == wallet_id, Position.status == "OPEN")
            )
        )
        for position in open_positions:
            balance = balances_by_token.get(position.token.contract_address)
            current_balance = balance.balance if balance else Decimal("0")
            was_closed = self._update_close_state(position, current_balance)
            if was_closed:
                result.positions_closed += 1
                result.closed_positions.append(
                    position.token.symbol or position.token.contract_address
                )

    def _update_close_state(self, position: Position, current_balance: Decimal) -> bool:
        changed = self._set_position_balance(position, current_balance)
        if current_balance <= self.settings.dust_threshold:
            position.dust_below_count += 1
            if position.dust_below_count >= self.settings.dust_clear_confirmation_scans:
                position.status = "CLOSED"
                position.closed_at = utc_now()
                position.last_checked_at = utc_now()
                return True
        else:
            position.dust_below_count = 0
        if changed:
            position.last_checked_at = utc_now()
        return False

    def _set_position_balance(self, position: Position, current_balance: Decimal) -> bool:
        previous = Decimal(str(position.last_balance or "0"))
        if previous == current_balance and Decimal(str(position.token_amount or "0")) == current_balance:
            position.last_checked_at = utc_now()
            return False
        position.token_amount = current_balance
        position.last_balance = current_balance
        position.last_checked_at = utc_now()
        return True

    def _get_or_create_token_from_event(
        self, session, chain: str, event: ParsedTokenEvent
    ) -> Token:
        token = session.scalar(
            select(Token).where(
                Token.chain == chain,
                Token.contract_address == event.token_address,
            )
        )
        if token:
            return token
        token = Token(
            chain=chain,
            contract_address=event.token_address,
            symbol=event.token_symbol,
            name=event.token_name,
            decimals=event.token_decimals,
        )
        session.add(token)
        session.flush()
        return token

    def _finish_run(
        self,
        wallet_id: int,
        started_at: datetime,
        status: str,
        result: ScanResult,
    ) -> None:
        with session_scope(self.session_factory) as session:
            run = session.scalar(
                select(MonitoringRun)
                .where(
                    MonitoringRun.wallet_id == wallet_id,
                    MonitoringRun.started_at == started_at,
                )
                .order_by(MonitoringRun.id.desc())
            )
            if not run:
                return
            run.finished_at = utc_now()
            run.status = status
            run.tokens_found = result.tokens_found
            run.positions_created = result.positions_created
            run.positions_updated = result.positions_updated
            run.positions_closed = result.positions_closed
            run.error_message = result.error_message

    async def _send_scan_notifications(
        self, chat_id: int, result: ScanResult, reason: str
    ) -> None:
        if not self.notifier:
            return
        try:
            if reason == "first_scan":
                lines = [
                    "✅ 首次扫描完成",
                    "",
                    "检测到：",
                    f"真实持仓：{result.positions_created}",
                    f"忽略Token：{result.ignored_tokens}",
                ]
                if result.new_positions:
                    lines.append("")
                    lines.extend(f"🟢 {symbol}" for symbol in result.new_positions)
                await self.notifier(chat_id, "\n".join(lines))
            elif result.new_positions:
                for symbol in result.new_positions:
                    await self.notifier(
                        chat_id,
                        "\n".join(
                            [
                                "🆕 检测到新持仓",
                                "",
                                f"Token：{symbol}",
                                "",
                                "已自动加入监控。",
                            ]
                        ),
                    )
            for symbol in result.closed_positions:
                await self.notifier(
                    chat_id,
                    "\n".join(
                        [
                            "✅ 检测到持仓已清仓",
                            "",
                            symbol,
                            "",
                            "最后持仓数量：0",
                            "",
                            "已停止该Token的持仓监控。",
                        ]
                    ),
                )
        except Exception as exc:
            logger.exception("Telegram Error notification failed: %s", exc)
