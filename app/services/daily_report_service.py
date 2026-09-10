from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import or_, select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import Position, PositionTransaction, PriceSnapshot, Token, User, Wallet, WalletTokenBalance
from app.services.transaction_parser import is_quote_token
from app.utils.time import utc_now

BUY = "BUY"
SELL = "SELL"
EXTERNAL_IN_TYPES = {"TRANSFER_IN", "BRIDGE_IN", "CLAIM", "AIRDROP", "MINT", "REWARD"}
EXTERNAL_OUT_TYPES = {"TRANSFER_OUT", "BRIDGE_OUT"}
FLOW_TYPES = {BUY, SELL, *EXTERNAL_IN_TYPES, *EXTERNAL_OUT_TYPES}
STABLE_QUOTE_SYMBOLS = {"USDC", "USDT", "USDG", "DAI"}
VALID_PRICE_QUALITY = "VALID"


@dataclass(frozen=True)
class ReportWindow:
    display_date: str
    timezone: str
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class NormalizedLedgerTransaction:
    wallet_id: int
    chain: str
    token_address: str
    symbol: str | None
    tx_hash: str
    tx_type: str
    tx_timestamp: datetime
    token_amount: Decimal
    quote_token: str | None = None
    quote_amount: Decimal | None = None
    usd_value: Decimal | None = None
    gas_fee_usd: Decimal | None = None


@dataclass
class BoundaryValue:
    balance: Decimal | None = None
    price_usd: Decimal | None = None
    position_value_usd: Decimal | None = None
    balance_source: str | None = None
    price_source: str | None = None
    price_snapshot_id: int | None = None
    complete: bool = False


@dataclass
class DailyTokenReport:
    chain: str
    token_address: str
    symbol: str | None
    wallet_ids: set[int] = field(default_factory=set)
    buy_usd: Decimal | None = Decimal("0")
    sell_usd: Decimal | None = Decimal("0")
    net_trade_usd: Decimal | None = Decimal("0")
    buy_count: int = 0
    sell_count: int = 0
    token_bought_amount: Decimal = Decimal("0")
    token_sold_amount: Decimal = Decimal("0")
    net_token_change_from_trades: Decimal = Decimal("0")
    external_in_value_usd: Decimal | None = Decimal("0")
    external_out_value_usd: Decimal | None = Decimal("0")
    attributed_gas_usd: Decimal | None = Decimal("0")
    explicit_additional_fees_usd: Decimal = Decimal("0")
    start: BoundaryValue = field(default_factory=BoundaryValue)
    end: BoundaryValue = field(default_factory=BoundaryValue)
    gross_daily_pnl: Decimal | None = None
    net_daily_pnl: Decimal | None = None
    pnl_unavailable_reasons: list[str] = field(default_factory=list)
    gas_data_complete: bool = True
    fee_data_complete: bool = True


@dataclass
class DailyReport:
    window: ReportWindow
    wallet_ids: list[int]
    tokens: list[DailyTokenReport]
    total_buy_usd: Decimal | None
    total_sell_usd: Decimal | None
    total_net_trade_usd: Decimal | None
    total_gas_usd: Decimal | None
    total_net_daily_pnl: Decimal | None
    unallocated_gas_usd: Decimal | None = Decimal("0")
    gas_data_complete: bool = True
    fee_data_complete: bool = True
    normalized_transactions: list[NormalizedLedgerTransaction] = field(default_factory=list)

    @property
    def has_trades(self) -> bool:
        return bool(self.tokens)


class DailyReportService:
    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        report_timezone: str = "Asia/Shanghai",
        price_tolerance_minutes: int = 90,
        now_fn=utc_now,
    ) -> None:
        self.session_factory = session_factory
        self.report_timezone = report_timezone
        self.price_tolerance = timedelta(minutes=price_tolerance_minutes)
        self.now_fn = now_fn
        self._zone = _safe_zoneinfo(report_timezone)

    def build_yesterday_report_for_telegram_user(self, telegram_user_id: int) -> DailyReport:
        with session_scope(self.session_factory) as session:
            user = session.scalar(select(User).where(User.telegram_user_id == telegram_user_id))
            wallet_ids = (
                list(
                    session.scalars(
                        select(Wallet.id)
                        .where(Wallet.user_id == user.id, Wallet.is_active.is_(True))
                        .order_by(Wallet.id.asc())
                    )
                )
                if user
                else []
            )
        return self.build_yesterday_report(wallet_ids)

    def build_yesterday_report(self, wallet_ids: list[int]) -> DailyReport:
        window = self.yesterday_window(self.now_fn(), self._zone)
        if not wallet_ids:
            return _empty_report(window, [])
        with session_scope(self.session_factory) as session:
            day_txs = self._transactions(session, wallet_ids, window.start_utc, window.end_utc)
            reportable_keys = sorted(
                {
                    (tx.chain, tx.token_address)
                    for tx in day_txs
                    if tx.tx_type in {BUY, SELL} and not is_quote_token(tx.symbol)
                }
            )
            if not reportable_keys:
                return _empty_report(window, wallet_ids, day_txs)
            after_txs = self._transactions(session, wallet_ids, window.end_utc, self.now_fn())
            tokens: list[DailyTokenReport] = []
            for chain, token_address in reportable_keys:
                token_txs = [tx for tx in day_txs if tx.chain == chain and tx.token_address == token_address]
                after_token_txs = [
                    tx for tx in after_txs if tx.chain == chain and tx.token_address == token_address
                ]
                tokens.append(
                    self._build_token_report(
                        session,
                        wallet_ids,
                        chain,
                        token_address,
                        token_txs,
                        after_token_txs,
                        window,
                    )
                )
            unallocated_gas = self._attribute_gas(tokens, day_txs)
            for token in tokens:
                self._compute_pnl(token)
            return self._build_report_totals(window, wallet_ids, tokens, day_txs, unallocated_gas)

    @staticmethod
    def yesterday_window(now_utc: datetime, zone: ZoneInfo) -> ReportWindow:
        aware_now = _aware_utc(now_utc)
        local_date = aware_now.astimezone(zone).date() - timedelta(days=1)
        start_local = datetime.combine(local_date, time.min, tzinfo=zone)
        end_local = start_local + timedelta(days=1)
        return ReportWindow(
            display_date=start_local.strftime("%m/%d"),
            timezone=zone.key,
            start_utc=start_local.astimezone(UTC).replace(tzinfo=None),
            end_utc=end_local.astimezone(UTC).replace(tzinfo=None),
        )

    def _transactions(
        self,
        session,
        wallet_ids: list[int],
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[NormalizedLedgerTransaction]:
        rows = session.execute(
            select(PositionTransaction, Position, Token)
            .join(Position, PositionTransaction.position_id == Position.id)
            .join(Token, Position.token_id == Token.id)
            .where(
                Position.wallet_id.in_(wallet_ids),
                PositionTransaction.tx_timestamp.is_not(None),
                PositionTransaction.tx_timestamp >= start_utc,
                PositionTransaction.tx_timestamp < end_utc,
                PositionTransaction.tx_type.in_(FLOW_TYPES),
            )
            .order_by(PositionTransaction.tx_timestamp.asc(), PositionTransaction.id.asc())
        ).all()
        return [
            NormalizedLedgerTransaction(
                wallet_id=position.wallet_id,
                chain=token.chain,
                token_address=token.contract_address,
                symbol=token.symbol,
                tx_hash=tx.tx_hash,
                tx_type=tx.tx_type,
                tx_timestamp=tx.tx_timestamp,
                token_amount=_decimal(tx.token_amount),
                quote_token=tx.quote_token,
                quote_amount=_decimal_or_none(tx.quote_amount),
                usd_value=self._transaction_usd_value(tx),
                gas_fee_usd=_decimal_or_none(tx.gas_fee),
            )
            for tx, position, token in rows
        ]

    def _build_token_report(
        self,
        session,
        wallet_ids: list[int],
        chain: str,
        token_address: str,
        token_txs: list[NormalizedLedgerTransaction],
        after_token_txs: list[NormalizedLedgerTransaction],
        window: ReportWindow,
    ) -> DailyTokenReport:
        symbol = next((tx.symbol for tx in token_txs if tx.symbol), None)
        report = DailyTokenReport(chain=chain, token_address=token_address, symbol=symbol)
        report.wallet_ids = {tx.wallet_id for tx in token_txs}
        for tx in token_txs:
            self._apply_day_transaction(report, tx)
        report.net_token_change_from_trades = report.token_bought_amount - report.token_sold_amount
        report.net_trade_usd = _subtract_optional(report.buy_usd, report.sell_usd)
        end_balance = self._reconstruct_end_balance(
            session,
            wallet_ids,
            chain,
            token_address,
            after_token_txs,
        )
        report.end.balance = end_balance
        report.end.balance_source = "current_balance_reverse_replay" if end_balance is not None else None
        day_delta = self._token_delta(token_txs)
        if end_balance is not None:
            report.start.balance = end_balance - day_delta
            report.start.balance_source = "end_balance_reverse_replay"
            report.start.complete = True
            report.end.complete = True
        start_price = self._nearest_price(session, wallet_ids, token_address, window.start_utc)
        end_price = self._nearest_price(session, wallet_ids, token_address, window.end_utc)
        _apply_price(report.start, start_price)
        _apply_price(report.end, end_price)
        if report.start.balance is not None and report.start.price_usd is not None:
            report.start.position_value_usd = report.start.balance * report.start.price_usd
        if report.end.balance is not None and report.end.price_usd is not None:
            report.end.position_value_usd = report.end.balance * report.end.price_usd
        return report

    def _apply_day_transaction(self, report: DailyTokenReport, tx: NormalizedLedgerTransaction) -> None:
        if tx.tx_type == BUY:
            report.buy_count += 1
            report.token_bought_amount += tx.token_amount
            report.buy_usd = _add_optional(report.buy_usd, tx.usd_value)
        elif tx.tx_type == SELL:
            report.sell_count += 1
            report.token_sold_amount += tx.token_amount
            report.sell_usd = _add_optional(report.sell_usd, tx.usd_value)
        elif tx.tx_type in EXTERNAL_IN_TYPES:
            report.external_in_value_usd = _add_optional(report.external_in_value_usd, tx.usd_value)
        elif tx.tx_type in EXTERNAL_OUT_TYPES:
            report.external_out_value_usd = _add_optional(report.external_out_value_usd, tx.usd_value)

    def _reconstruct_end_balance(
        self,
        session,
        wallet_ids: list[int],
        chain: str,
        token_address: str,
        after_txs: list[NormalizedLedgerTransaction],
    ) -> Decimal | None:
        current = self._current_balance(session, wallet_ids, chain, token_address)
        if current is None:
            return None
        return current - self._token_delta(after_txs)

    def _current_balance(
        self,
        session,
        wallet_ids: list[int],
        chain: str,
        token_address: str,
    ) -> Decimal | None:
        balances = list(
            session.scalars(
                select(WalletTokenBalance).where(
                    WalletTokenBalance.wallet_id.in_(wallet_ids),
                    WalletTokenBalance.chain == chain,
                    WalletTokenBalance.contract_address == token_address,
                )
            )
        )
        if balances:
            return sum((_decimal(balance.token_amount) for balance in balances), Decimal("0"))
        positions = list(
            session.scalars(
                select(Position)
                .join(Token)
                .where(
                    Position.wallet_id.in_(wallet_ids),
                    Token.chain == chain,
                    Token.contract_address == token_address,
                )
            )
        )
        if not positions:
            return None
        return sum((_decimal(position.last_balance) for position in positions), Decimal("0"))

    @staticmethod
    def _token_delta(transactions: list[NormalizedLedgerTransaction]) -> Decimal:
        total = Decimal("0")
        for tx in transactions:
            if tx.tx_type in {BUY, *EXTERNAL_IN_TYPES}:
                total += tx.token_amount
            elif tx.tx_type in {SELL, *EXTERNAL_OUT_TYPES}:
                total -= tx.token_amount
        return total

    def _nearest_price(
        self,
        session,
        wallet_ids: list[int],
        token_address: str,
        target: datetime,
    ) -> PriceSnapshot | None:
        lower = target - self.price_tolerance
        upper = target + self.price_tolerance
        candidates = list(
            session.scalars(
                select(PriceSnapshot).where(
                    PriceSnapshot.wallet_id.in_(wallet_ids),
                    PriceSnapshot.token_address == token_address,
                    PriceSnapshot.observed_at >= lower,
                    PriceSnapshot.observed_at <= upper,
                    or_(
                        PriceSnapshot.quality_status.is_(None),
                        PriceSnapshot.quality_status == VALID_PRICE_QUALITY,
                    ),
                )
            )
        )
        if not candidates:
            return None
        return min(candidates, key=lambda snapshot: abs((snapshot.observed_at - target).total_seconds()))

    def _transaction_usd_value(self, tx: PositionTransaction) -> Decimal | None:
        stored = _decimal_or_none(tx.usd_value)
        if stored is not None:
            return stored
        quote = (tx.quote_token or "").upper()
        quote_amount = _decimal_or_none(tx.quote_amount)
        if quote in STABLE_QUOTE_SYMBOLS and quote_amount is not None:
            return quote_amount
        return None

    def _attribute_gas(
        self,
        token_reports: list[DailyTokenReport],
        day_txs: list[NormalizedLedgerTransaction],
    ) -> Decimal:
        reports_by_key = {(report.chain, report.token_address): report for report in token_reports}
        tx_groups: dict[str, list[NormalizedLedgerTransaction]] = {}
        unallocated = Decimal("0")
        for tx in day_txs:
            if tx.tx_type in {BUY, SELL} and (tx.chain, tx.token_address) in reports_by_key:
                tx_groups.setdefault(tx.tx_hash.lower(), []).append(tx)
        for group in tx_groups.values():
            gas_values = [tx.gas_fee_usd for tx in group if tx.gas_fee_usd is not None]
            keys = {(tx.chain, tx.token_address) for tx in group}
            gas = gas_values[0] if gas_values else None
            if gas is None:
                for key in keys:
                    reports_by_key[key].gas_data_complete = False
                continue
            if len(keys) == 1:
                report = reports_by_key[next(iter(keys))]
                report.attributed_gas_usd = _add_optional(report.attributed_gas_usd, gas)
            else:
                unallocated += gas
        return unallocated

    def _compute_pnl(self, report: DailyTokenReport) -> None:
        missing = []
        if report.start.position_value_usd is None:
            missing.append("start_position_value")
        if report.end.position_value_usd is None:
            missing.append("end_position_value")
        if report.buy_usd is None:
            missing.append("buy_cost")
        if report.sell_usd is None:
            missing.append("sell_proceeds")
        if report.external_in_value_usd is None:
            missing.append("external_in_value")
        if report.external_out_value_usd is None:
            missing.append("external_out_value")
        report.pnl_unavailable_reasons = missing
        if missing:
            report.gross_daily_pnl = None
            report.net_daily_pnl = None
            return
        report.gross_daily_pnl = (
            report.end.position_value_usd
            - report.start.position_value_usd
            + report.sell_usd
            - report.buy_usd
            - report.external_in_value_usd
            + report.external_out_value_usd
        )
        if not report.gas_data_complete or report.attributed_gas_usd is None:
            report.net_daily_pnl = None
            if "gas" not in report.pnl_unavailable_reasons:
                report.pnl_unavailable_reasons.append("gas")
            return
        report.net_daily_pnl = (
            report.gross_daily_pnl
            - report.attributed_gas_usd
            - report.explicit_additional_fees_usd
        )

    def _build_report_totals(
        self,
        window: ReportWindow,
        wallet_ids: list[int],
        tokens: list[DailyTokenReport],
        day_txs: list[NormalizedLedgerTransaction],
        unallocated_gas_usd: Decimal,
    ) -> DailyReport:
        total_buy = _sum_optional([token.buy_usd for token in tokens])
        total_sell = _sum_optional([token.sell_usd for token in tokens])
        total_trade = _subtract_optional(total_buy, total_sell)
        total_token_gas = _sum_optional([token.attributed_gas_usd for token in tokens])
        gas_complete = all(token.gas_data_complete for token in tokens)
        total_net_pnl = _sum_optional([token.net_daily_pnl for token in tokens])
        if total_net_pnl is not None:
            total_net_pnl -= unallocated_gas_usd
        if total_token_gas is not None:
            total_token_gas += unallocated_gas_usd
        return DailyReport(
            window=window,
            wallet_ids=wallet_ids,
            tokens=sorted(tokens, key=lambda token: abs(token.net_trade_usd or Decimal("0")), reverse=True),
            total_buy_usd=total_buy,
            total_sell_usd=total_sell,
            total_net_trade_usd=total_trade,
            total_gas_usd=total_token_gas if gas_complete else None,
            total_net_daily_pnl=total_net_pnl if gas_complete else None,
            unallocated_gas_usd=unallocated_gas_usd,
            gas_data_complete=gas_complete,
            fee_data_complete=all(token.fee_data_complete for token in tokens),
            normalized_transactions=day_txs,
        )


def format_daily_report(report: DailyReport) -> str:
    lines = [f"📊 昨日日报｜{report.window.display_date}", ""]
    if not report.has_trades:
        lines.append("昨日没有检测到买入或卖出操作。")
        return "\n".join(lines)
    lines.extend(
        [
            "总览",
            f"• 交易币种｜{len(report.tokens)}",
            f"• 买入｜{_format_usd(report.total_buy_usd)}",
            f"• 卖出｜{_format_usd(report.total_sell_usd)}",
            f"• {_net_trade_label(report.total_net_trade_usd)}｜{_format_signed_usd(report.total_net_trade_usd)}",
            f"• Gas/手续费｜{_format_fee(report.total_gas_usd)}",
            f"• 昨日净收益｜{_format_signed_usd(report.total_net_daily_pnl)}",
            "",
        ]
    )
    for token in report.tokens:
        symbol = token.symbol or token.token_address[:10]
        lines.extend(
            [
                symbol,
                f"• 买入｜{_format_usd(token.buy_usd)}",
                f"• 卖出｜{_format_usd(token.sell_usd)}",
                f"• {_net_trade_label(token.net_trade_usd)}｜{_format_signed_usd(token.net_trade_usd)}",
                f"• 收益｜{_format_signed_usd(token.gross_daily_pnl)}",
                f"• Gas/手续费｜{_format_fee(token.attributed_gas_usd if token.gas_data_complete else None)}",
                f"• 净收益｜{_format_signed_usd(token.net_daily_pnl)}",
                f"• 交易｜买{token.buy_count} / 卖{token.sell_count}",
                f"• 24:00持仓｜{_format_usd(token.end.position_value_usd)}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def format_daily_report_debug(report: DailyReport) -> str:
    lines = [
        f"Daily Report Probe {report.window.display_date} tz={report.window.timezone}",
        f"window_utc={report.window.start_utc.isoformat()} -> {report.window.end_utc.isoformat()}",
        "",
        "normalized_transactions:",
    ]
    if report.normalized_transactions:
        for tx in report.normalized_transactions:
            lines.append(
                " ".join(
                    [
                        tx.tx_timestamp.isoformat(),
                        f"wallet={tx.wallet_id}",
                        f"type={tx.tx_type}",
                        f"symbol={tx.symbol or '-'}",
                        f"token={tx.token_address}",
                        f"amount={_plain(tx.token_amount)}",
                        f"usd={_plain_or_unavailable(tx.usd_value)}",
                        f"gas={_plain_or_unavailable(tx.gas_fee_usd)}",
                        f"tx={tx.tx_hash}",
                    ]
                )
            )
    else:
        lines.append("NONE")
    lines.append("")
    lines.append("per_token_aggregation:")
    for token in report.tokens:
        lines.append(
            " ".join(
                [
                    f"{token.symbol or '-'}",
                    f"token={token.token_address}",
                    f"buy_usd={_plain_or_unavailable(token.buy_usd)}",
                    f"sell_usd={_plain_or_unavailable(token.sell_usd)}",
                    f"net_trade={_plain_or_unavailable(token.net_trade_usd)}",
                    f"bought={_plain(token.token_bought_amount)}",
                    f"sold={_plain(token.token_sold_amount)}",
                ]
            )
        )
        lines.append(
            " ".join(
                [
                    "boundaries",
                    f"start_balance={_plain_or_unavailable(token.start.balance)}",
                    f"start_price={_plain_or_unavailable(token.start.price_usd)}",
                    f"end_balance={_plain_or_unavailable(token.end.balance)}",
                    f"end_price={_plain_or_unavailable(token.end.price_usd)}",
                ]
            )
        )
        lines.append(
            " ".join(
                [
                    "pnl",
                    f"gross={_plain_or_unavailable(token.gross_daily_pnl)}",
                    f"gas_complete={token.gas_data_complete}",
                    f"net={_plain_or_unavailable(token.net_daily_pnl)}",
                    f"unavailable={','.join(token.pnl_unavailable_reasons) or '-'}",
                ]
            )
        )
    lines.extend(["", "telegram_preview:", format_daily_report(report)])
    return "\n".join(lines)


def _empty_report(
    window: ReportWindow,
    wallet_ids: list[int],
    transactions: list[NormalizedLedgerTransaction] | None = None,
) -> DailyReport:
    return DailyReport(
        window=window,
        wallet_ids=wallet_ids,
        tokens=[],
        total_buy_usd=Decimal("0"),
        total_sell_usd=Decimal("0"),
        total_net_trade_usd=Decimal("0"),
        total_gas_usd=Decimal("0"),
        total_net_daily_pnl=Decimal("0"),
        normalized_transactions=transactions or [],
    )


def _apply_price(boundary: BoundaryValue, snapshot: PriceSnapshot | None) -> None:
    if snapshot is None:
        return
    boundary.price_usd = _decimal(snapshot.price_usd)
    boundary.price_snapshot_id = snapshot.id
    boundary.price_source = snapshot.source_endpoint or "price_snapshots"


def _safe_zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value) -> Decimal:
    return Decimal(str(value or "0"))


def _decimal_or_none(value) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _add_optional(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None:
        return None
    return left + right


def _subtract_optional(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None:
        return None
    return left - right


def _sum_optional(values: list[Decimal | None]) -> Decimal | None:
    total = Decimal("0")
    for value in values:
        if value is None:
            return None
        total += value
    return total


def _net_trade_label(value: Decimal | None) -> str:
    if value is None:
        return "净买/卖"
    return "净买入" if value >= 0 else "净卖出"


def _format_usd(value: Decimal | None) -> str:
    if value is None:
        return "暂无法计算"
    return f"${_money(value):,}"


def _format_signed_usd(value: Decimal | None) -> str:
    if value is None:
        return "暂无法计算"
    rounded = _money(value)
    sign = "+" if rounded >= 0 else "-"
    return f"{sign}${abs(rounded):,}"


def _format_fee(value: Decimal | None) -> str:
    if value is None:
        return "暂无法计算"
    rounded = _money(value)
    return f"-${abs(rounded):,}" if rounded != 0 else "$0.0"


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def _plain(value: Decimal) -> str:
    return f"{value.normalize():f}"


def _plain_or_unavailable(value: Decimal | None) -> str:
    return "UNAVAILABLE" if value is None else _plain(value)
