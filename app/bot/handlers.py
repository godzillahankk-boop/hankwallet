from __future__ import annotations

import logging
import time

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from telegram import ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.bot.keyboards import (
    BTN_ADD_WALLET,
    BTN_BACK,
    BTN_DELETE_WALLET,
    BTN_LIST_WALLETS,
    BTN_POSITIONS,
    BTN_SCAN,
    BTN_SETTINGS,
    BTN_WALLETS,
    main_menu_keyboard,
    wallet_menu_keyboard,
)
from app.bot.messages import (
    ADD_WALLET_PROMPT,
    SETTINGS_MESSAGE,
    START_MESSAGE,
    balances_message,
    duplicate_wallet_message,
    gmgn_holdings_message,
    invalid_wallet_message,
    scan_done_message,
    wallet_added_message,
    wallet_list_message,
)
from app.core.config import Settings
from app.db.database import session_scope
from app.db.models import User, Wallet
from app.services.balance_service import BalanceService
from app.services.gmgn_client import GmgnHolding
from app.services.monitoring_service import MonitoringService
from app.services.price_guardian_service import PriceGuardianService
from app.services.wallet_service import WalletService

logger = logging.getLogger(__name__)

ADDING_WALLET = 1
DELETING_WALLET = 2


def build_application(
    settings: Settings,
    session_factory: sessionmaker,
    monitoring_service: MonitoringService,
    price_guardian_service: PriceGuardianService | None = None,
) -> Application:
    application = Application.builder().token(settings.telegram_bot_token).build()
    application.bot_data["session_factory"] = session_factory
    application.bot_data["monitoring_service"] = monitoring_service
    application.bot_data["price_guardian_service"] = price_guardian_service
    application.bot_data["settings"] = settings
    application.bot_data["manual_scan_at"] = {}

    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        ConversationHandler(
            entry_points=[
                MessageHandler(filters.Regex(f"^{BTN_ADD_WALLET}$"), add_wallet_start),
                MessageHandler(filters.Regex(f"^{BTN_DELETE_WALLET}$"), delete_wallet_start),
            ],
            states={
                ADDING_WALLET: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, add_wallet_finish)
                ],
                DELETING_WALLET: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, delete_wallet_finish)
                ],
            },
            fallbacks=[MessageHandler(filters.Regex(f"^{BTN_BACK}$"), back_to_main)],
        )
    )
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_WALLETS}$"), wallet_menu))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_LIST_WALLETS}$"), list_wallets))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_POSITIONS}$"), list_positions))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_SCAN}$"), manual_scan))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_SETTINGS}$"), settings_message))
    application.add_handler(MessageHandler(filters.Regex(f"^{BTN_BACK}$"), back_to_main))
    application.add_error_handler(error_handler)
    return application


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    session_factory = context.application.bot_data["session_factory"]
    with session_scope(session_factory) as session:
        WalletService(session).get_or_create_user(
            update.effective_user.id, update.effective_chat.id
        )
    await update.message.reply_text(START_MESSAGE, reply_markup=main_menu_keyboard())


async def wallet_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("👛 钱包管理", reply_markup=wallet_menu_keyboard())


async def add_wallet_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text(ADD_WALLET_PROMPT, reply_markup=ReplyKeyboardRemove())
    return ADDING_WALLET


async def add_wallet_finish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.effective_user or not update.effective_chat or not update.message:
        return ConversationHandler.END
    settings: Settings = context.application.bot_data["settings"]
    session_factory = context.application.bot_data["session_factory"]
    address = update.message.text or ""
    try:
        with session_scope(session_factory) as session:
            wallet = WalletService(session).add_wallet(
                telegram_user_id=update.effective_user.id,
                telegram_chat_id=update.effective_chat.id,
                address=address,
                chain=settings.default_chain,
            )
            wallet_id = wallet.id
            wallet_address = wallet.address
    except ValueError as exc:
        if str(exc) == "wallet_already_exists":
            await update.message.reply_text(
                duplicate_wallet_message(), reply_markup=main_menu_keyboard()
            )
        else:
            await update.message.reply_text(
                invalid_wallet_message(), reply_markup=main_menu_keyboard()
            )
        return ConversationHandler.END

    await update.message.reply_text(
        wallet_added_message(wallet_address), reply_markup=main_menu_keyboard()
    )
    monitoring_service: MonitoringService = context.application.bot_data[
        "monitoring_service"
    ]
    context.application.create_task(
        monitoring_service.scan_wallet(wallet_id, reason="first_scan")
    )
    return ConversationHandler.END


async def list_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    session_factory = context.application.bot_data["session_factory"]
    with session_scope(session_factory) as session:
        wallets = WalletService(session).list_wallets(update.effective_user.id)
        text = wallet_list_message(wallets)
    await update.message.reply_text(text, reply_markup=wallet_menu_keyboard())


async def delete_wallet_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text(
            "请输入要删除的钱包地址：",
            reply_markup=ReplyKeyboardRemove(),
        )
    return DELETING_WALLET


async def delete_wallet_finish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.effective_user or not update.message:
        return ConversationHandler.END
    settings: Settings = context.application.bot_data["settings"]
    session_factory = context.application.bot_data["session_factory"]
    removed = False
    with session_scope(session_factory) as session:
        removed = WalletService(session).deactivate_wallet(
            update.effective_user.id,
            update.message.text or "",
            settings.default_chain,
        )
    text = "✅ 钱包已删除。" if removed else "没有找到这个钱包。"
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def list_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    settings: Settings = context.application.bot_data["settings"]
    session_factory = context.application.bot_data["session_factory"]
    monitoring_service: MonitoringService = context.application.bot_data[
        "monitoring_service"
    ]
    price_guardian_service: PriceGuardianService | None = context.application.bot_data.get(
        "price_guardian_service"
    )
    wallet_ids: list[int] = []
    wallet_refs: list[tuple[int, str, str]] = []
    balances = []
    stale_wallet_ids: list[int] = []
    with session_scope(session_factory) as session:
        user = session.scalar(
            select(User).where(User.telegram_user_id == update.effective_user.id)
        )
        if user:
            wallet_ids = list(
                session.scalars(
                    select(Wallet.id).where(
                        Wallet.user_id == user.id, Wallet.is_active.is_(True)
                    )
                )
            )
            wallet_refs = list(
                session.execute(
                    select(Wallet.id, Wallet.chain, Wallet.address).where(
                        Wallet.user_id == user.id, Wallet.is_active.is_(True)
                    )
                )
            )
    if price_guardian_service and wallet_refs:
        try:
            holdings = await fetch_gmgn_position_holdings(wallet_refs, price_guardian_service)
            text = gmgn_holdings_message(holdings, settings.price_monitor_min_usd_value)
            await update.message.reply_text(text, reply_markup=main_menu_keyboard())
            return
        except Exception as exc:
            logger.exception("GMGN holdings refresh failed for positions view: %s", exc)
            await update.message.reply_text(
                "\n".join(["⚠️ GMGN 持仓刷新失败", "", str(exc)]),
                reply_markup=main_menu_keyboard(),
            )
            return
    with session_scope(session_factory) as session:
        balance_service = BalanceService(session)
        balances = balance_service.list_user_balances(update.effective_user.id)
        stale_wallet_ids = [
            wallet_id
            for wallet_id in wallet_ids
            if not balance_service.wallet_has_recent_snapshot(
                wallet_id,
                max(settings.wallet_scan_interval_seconds * 2, 120),
            )
        ]
    if balances:
        text = balances_message(balances)
        await update.message.reply_text(text, reply_markup=main_menu_keyboard())
        for wallet_id in stale_wallet_ids:
            context.application.create_task(
                _refresh_wallet_balances_safely(monitoring_service, wallet_id)
            )
        return
    for wallet_id in wallet_ids:
        try:
            await monitoring_service.refresh_wallet_balances(wallet_id)
        except Exception as exc:
            logger.exception("Wallet balance refresh failed wallet_id=%s: %s", wallet_id, exc)
            await update.message.reply_text(
                "\n".join(["⚠️ 持仓刷新失败", "", str(exc)]),
                reply_markup=main_menu_keyboard(),
            )
            return
    with session_scope(session_factory) as session:
        balances = BalanceService(session).list_user_balances(update.effective_user.id)
        text = balances_message(balances)
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def fetch_gmgn_position_holdings(
    wallet_refs: list[tuple[int, str, str]],
    price_guardian_service: PriceGuardianService,
) -> list[GmgnHolding]:
    holdings: list[GmgnHolding] = []
    for _, chain, address in wallet_refs:
        holdings.extend(await price_guardian_service.fetch_all_holdings(chain, address))
    return holdings


async def _refresh_wallet_balances_safely(
    monitoring_service: MonitoringService, wallet_id: int
) -> None:
    try:
        await monitoring_service.refresh_wallet_balances(wallet_id)
    except Exception as exc:
        logger.exception("Wallet background balance refresh failed wallet_id=%s: %s", wallet_id, exc)


async def manual_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    settings: Settings = context.application.bot_data["settings"]
    now = time.time()
    last_by_user = context.application.bot_data["manual_scan_at"]
    last_scan_at = last_by_user.get(update.effective_user.id, 0)
    if now - last_scan_at < settings.manual_scan_cooldown_seconds:
        await update.message.reply_text(
            f"手动扫描太频繁，请 {settings.manual_scan_cooldown_seconds} 秒后再试。",
            reply_markup=main_menu_keyboard(),
        )
        return
    last_by_user[update.effective_user.id] = now

    session_factory = context.application.bot_data["session_factory"]
    monitoring_service: MonitoringService = context.application.bot_data[
        "monitoring_service"
    ]
    wallet_ids: list[int] = []
    with session_scope(session_factory) as session:
        user = session.scalar(
            select(User).where(User.telegram_user_id == update.effective_user.id)
        )
        if user:
            wallet_ids = list(
                session.scalars(
                    select(Wallet.id).where(
                        Wallet.user_id == user.id, Wallet.is_active.is_(True)
                    )
                )
            )
    created = updated_count = closed = 0
    for wallet_id in wallet_ids:
        result = await monitoring_service.scan_wallet(wallet_id, reason="manual")
        if result.error_message:
            await update.message.reply_text(
                "\n".join(["⚠️ 钱包扫描失败", "", result.error_message]),
                reply_markup=main_menu_keyboard(),
            )
            return
        created += result.positions_created
        updated_count += result.positions_updated
        closed += result.positions_closed

    await update.message.reply_text(
        scan_done_message(created, updated_count, closed),
        reply_markup=main_menu_keyboard(),
    )


async def settings_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(SETTINGS_MESSAGE, reply_markup=main_menu_keyboard())


async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text(START_MESSAGE, reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Telegram update error", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "⚠️ 系统处理失败，请稍后再试。",
            reply_markup=main_menu_keyboard(),
        )
