from __future__ import annotations

from telegram import ReplyKeyboardMarkup


BTN_POSITIONS = "💼 我的持仓"
BTN_WALLETS = "👛 钱包管理"
BTN_NEW_WALLET = "➕ 新增钱包"
BTN_SCAN = "🔄 立即扫描"
BTN_DAILY_REPORT = "📊 查看昨日日报（待）"
BTN_SETTINGS = "⚙️ 设置"
BTN_ADD_WALLET = "➕ 添加钱包"
BTN_LIST_WALLETS = "📋 我的钱包"
BTN_DELETE_WALLET = "🗑 删除钱包"
BTN_BACK = "⬅️ 返回"


def main_menu_keyboard(has_wallet: bool = True) -> ReplyKeyboardMarkup:
    wallet_button = BTN_WALLETS if has_wallet else BTN_NEW_WALLET
    return ReplyKeyboardMarkup(
        [
            [BTN_POSITIONS, wallet_button],
            [BTN_SCAN, BTN_DAILY_REPORT],
            [BTN_SETTINGS],
        ],
        resize_keyboard=True,
    )


def wallet_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [BTN_ADD_WALLET, BTN_LIST_WALLETS],
            [BTN_DELETE_WALLET, BTN_BACK],
        ],
        resize_keyboard=True,
    )
