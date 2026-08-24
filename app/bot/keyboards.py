from __future__ import annotations

from telegram import ReplyKeyboardMarkup


BTN_POSITIONS = "💼 我的持仓"
BTN_WALLETS = "👛 钱包管理"
BTN_SCAN = "🔄 立即扫描"
BTN_SETTINGS = "⚙️ 设置"
BTN_ADD_WALLET = "➕ 添加钱包"
BTN_LIST_WALLETS = "📋 我的钱包"
BTN_DELETE_WALLET = "🗑 删除钱包"
BTN_BACK = "⬅️ 返回"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [BTN_POSITIONS, BTN_WALLETS],
            [BTN_SCAN, BTN_SETTINGS],
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

