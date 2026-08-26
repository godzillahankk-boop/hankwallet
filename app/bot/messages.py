from __future__ import annotations

from decimal import Decimal

from app.db.models import Position, Wallet, WalletTokenBalance
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
        lines.append(f"🟢 {symbol}")
        lines.append(format_amount(Decimal(str(balance.token_amount or '0'))))
        if balance.usd_value:
            lines.append(f"${format_amount(Decimal(str(balance.usd_value)))}")
        lines.append("")
    return "\n".join(lines).strip()


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
