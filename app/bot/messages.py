from __future__ import annotations

from decimal import Decimal

from app.bot.formatters import (
    decimal_or_none,
    format_holding_pnl_pct,
    format_holding_usd,
    format_market_cap,
)
from app.db.models import Position, Wallet, WalletTokenBalance
from app.services.gmgn_client import GmgnHolding
from app.services.holding_classifier import (
    is_below_threshold_position,
    is_canonical_trading_position,
    is_display_position,
)
from app.services.holding_market_cap import build_holding_market_context
from app.services.price_guardian_service import PriceGuardianResult
from app.utils.address import short_address


START_MESSAGE = "🛡 Wallet Agent"
ADD_WALLET_PROMPT = "请输入需要监控的钱包地址："
NO_POSITIONS_MESSAGE = "当前没有检测到正在监控的持仓。"
NO_WALLETS_MESSAGE = "当前还没有添加钱包。"
SETTINGS_MESSAGE = "⚙️ 当前版本设置通过 .env 配置。"


def wallet_added_message(address: str) -> str:
    return "\n".join(
        [
            "✅ 钱包添加成功",
            "",
            "地址：",
            short_address(address),
            "",
            "正在进行首次持仓扫描。",
        ]
    )


def duplicate_wallet_message() -> str:
    return "这个钱包已经添加过。"


def invalid_wallet_message() -> str:
    return "钱包地址格式不合法，请输入 0x 开头的 EVM 地址。"


def wallet_list_message(wallets: list[Wallet]) -> str:
    if not wallets:
        return NO_WALLETS_MESSAGE
    lines = ["📋 我的钱包", ""]
    for wallet in wallets:
        name = f"{wallet.name} " if wallet.name else ""
        lines.append(f"{name}{wallet.chain}: {short_address(wallet.address)}")
    return "\n".join(lines)


def positions_message(positions: list[Position]) -> str:
    if not positions:
        return NO_POSITIONS_MESSAGE
    lines = ["💼 当前持仓", ""]
    for position in positions:
        symbol = position.token.symbol or short_address(position.token.contract_address)
        lines.append(f"🟢 {symbol}")
        lines.append(format_amount(Decimal(str(position.token_amount or '0'))))
        lines.append("")
    return "\n".join(lines).strip()


def balances_message(balances: list[WalletTokenBalance]) -> str:
    if not balances:
        return NO_POSITIONS_MESSAGE
    lines = ["💼 当前持仓", ""]
    for balance in balances:
        symbol = balance.symbol or short_address(balance.contract_address or balance.asset_key)
        amount = decimal_or_none(balance.token_amount)
        usd_value = decimal_or_none(balance.usd_value)
        unrealized_profit = decimal_or_none(getattr(balance, "unrealized_profit", None))
        lines.append(f"🟢 {symbol}")
        lines.append(f"金额：{format_holding_usd(usd_value)}")
        lines.append("持仓市值：--")
        lines.append(f"持仓盈亏：{format_holding_pnl_pct(usd_value, unrealized_profit)}")
        lines.append("")
    return "\n".join(lines).strip()


def gmgn_holdings_message(
    holdings: list[GmgnHolding],
    min_usd_value: Decimal,
    excluded_symbols: tuple[str, ...] = (),
) -> str:
    trading_holdings = [
        holding
        for holding in holdings
        if is_canonical_trading_position(holding, excluded_symbols)
    ]
    if not trading_holdings:
        return NO_POSITIONS_MESSAGE
    display_holdings = [
        holding
        for holding in trading_holdings
        if is_display_position(holding, min_usd_value, excluded_symbols)
    ]
    below_threshold_holdings = [
        holding
        for holding in trading_holdings
        if is_below_threshold_position(holding, min_usd_value, excluded_symbols)
    ]
    lines = ["💼 当前持仓"]
    if below_threshold_holdings:
        lines.append(f"（已隐藏金额<{format_threshold_usd(min_usd_value)}代币）")
    lines.append("")
    if not display_holdings:
        lines.append("当前没有达到关注金额的持仓。")
        return "\n".join(lines).strip()
    for holding in display_holdings:
        symbol = holding.symbol or short_address(holding.contract_address or "")
        market_context = build_holding_market_context(
            wallet_id=0,
            token_address=holding.contract_address or "",
            holding=holding,
            observed_at=None,
            watch_started_at=None,
        )
        lines.append(f"🟢 {symbol}")
        lines.append(f"金额：{format_holding_usd(holding.usd_value)}")
        lines.append(
            "持仓市值："
            + format_market_cap(
                market_context.entry_market_cap_usd if market_context else None
            )
        )
        lines.append(
            f"持仓盈亏：{format_holding_pnl_pct(holding.usd_value, holding.unrealized_profit_usd)}"
        )
        lines.append("")
    return "\n".join(lines).strip()


def format_threshold_usd(value: Decimal) -> str:
    rounded = value.quantize(Decimal("0.1"))
    text = f"{rounded:,.1f}"
    if text.endswith(".0"):
        text = text[:-2]
    return f"${text}"


def scan_done_message(created: int, updated: int, closed: int) -> str:
    return "\n".join(
        [
            "🔄 扫描完成",
            "",
            f"新增持仓：{created}",
            f"更新持仓：{updated}",
            f"清仓：{closed}",
        ]
    )


def gmgn_scan_failure_message() -> str:
    return "\n".join(
        [
            "⚠️ GMGN 持仓扫描失败",
            "",
            "钱包已经保存，稍后可以重新扫描。",
        ]
    )


def gmgn_first_scan_done_message(result: PriceGuardianResult, min_usd_value: Decimal) -> str:
    lines = [
        "✅ 首次扫描完成",
        "",
        f"已识别当前持仓：{result.trading_positions_found}",
        f"已加入监控：{result.trading_positions_found}",
    ]
    if result.below_threshold_positions:
        lines.append(
            f"已隐藏金额<{format_threshold_usd(min_usd_value)}持仓：{result.below_threshold_positions}"
        )
    if result.position_symbols:
        lines.append("")
        lines.extend(f"🟢 {symbol}" for symbol in result.position_symbols)
    return "\n".join(lines)


def gmgn_manual_scan_done_message(result: PriceGuardianResult, min_usd_value: Decimal) -> str:
    lines = [
        "🔄 扫描完成",
        "",
        f"当前持仓：{result.trading_positions_found}",
        f"监控中：{result.trading_positions_found}",
    ]
    if result.below_threshold_positions:
        lines.append(
            f"隐藏<{format_threshold_usd(min_usd_value)}：{result.below_threshold_positions}"
        )
    if result.position_symbols:
        lines.append("")
        lines.extend(f"🟢 {symbol}" for symbol in result.position_symbols)
    return "\n".join(lines)


def format_amount(amount: Decimal) -> str:
    if amount == 0:
        return "0"
    quantum = Decimal("0.000000000001")
    normalized = amount.quantize(quantum).normalize()
    text = f"{normalized:f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    int_part, _, frac_part = text.partition(".")
    grouped = f"{int(int_part):,}" if int_part not in {"", "-"} else int_part
    return f"{grouped}.{frac_part}" if frac_part else grouped
