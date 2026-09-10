from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from app.bot.handlers import add_wallet_finish, fetch_gmgn_position_holdings, manual_scan
from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import Wallet
from app.services.gmgn_client import parse_holding
from app.services.price_guardian_service import PriceGuardianResult
from app.services.wallet_service import WalletService


def settings(tmp_path) -> Settings:
    return Settings(
        telegram_bot_token="test-token",
        database_url=f"sqlite:///{tmp_path}/wallet_agent_test.db",
        wallet_scan_interval_seconds=60,
        legacy_wallet_scan_enabled=False,
        manual_scan_cooldown_seconds=30,
        dust_threshold=Decimal("0.000001"),
        dust_clear_confirmation_scans=2,
        default_chain="robinhood",
        chain_api_base_url=None,
        chain_api_key=None,
        chain_rpc_url=None,
        chain_token_search_symbols=(),
        chain_request_timeout_seconds=20,
        token_transfer_lookback_limit=100,
        log_level="INFO",
        api_host="127.0.0.1",
        api_port=8000,
        gmgn_enabled=True,
        gmgn_api_base_url="https://openapi.gmgn.ai",
        gmgn_api_key="test-key",
        gmgn_private_key_path=None,
        gmgn_request_timeout_seconds=20,
        price_guardian_enabled=True,
        price_guardian_alerts_enabled=True,
        price_scan_interval_seconds=60,
        price_monitor_min_usd_value=Decimal("5"),
        price_excluded_symbols=("USDG", "USDC", "USDT", "ETH", "WETH"),
        price_history_retention_hours=24,
        price_alert_5m_percent=Decimal("10"),
        price_alert_15m_percent=Decimal("20"),
        price_alert_60m_percent=Decimal("30"),
        price_alert_escalation_step_percent=Decimal("10"),
        price_alert_reset_ratio=Decimal("0.5"),
        price_holdings_max_pages=10,
        price_wallet_concurrency=3,
        attention_engine_enabled=False,
        attention_smart_money_interval_seconds=60,
        attention_kol_interval_seconds=60,
        attention_market_signal_interval_seconds=120,
        attention_token_snapshot_interval_seconds=600,
        attention_top_holder_interval_seconds=900,
        attention_token_snapshot_batch_size=3,
        attention_top_holder_batch_size=1,
        attention_feed_window_minutes=15,
        attention_event_aggregation_minutes=5,
        attention_warning_cooldown_minutes=30,
        attention_critical_cooldown_minutes=60,
    )


class FakeTelegramUser:
    id = 1


class FakeTelegramChat:
    id = 100


class FakeMessage:
    def __init__(self, text: str) -> None:
        self.text = text
        self.replies: list[str] = []

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append(text)


class FakeUpdate:
    def __init__(self, text: str) -> None:
        self.effective_user = FakeTelegramUser()
        self.effective_chat = FakeTelegramChat()
        self.message = FakeMessage(text)


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, *, chat_id: int, text: str, **kwargs) -> None:
        self.messages.append((chat_id, text))


class FakeApplication:
    def __init__(self, bot_data: dict) -> None:
        self.bot_data = bot_data
        self.bot = FakeBot()
        self.tasks = []

    def create_task(self, coro):
        self.tasks.append(coro)
        return coro


class FakeContext:
    def __init__(self, application: FakeApplication) -> None:
        self.application = application


class FakeMonitoringService:
    def __init__(self) -> None:
        self.calls = 0

    async def scan_wallet(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("legacy monitoring scanner must not be used")


class FakeScanPriceGuardianService:
    def __init__(self, result: PriceGuardianResult | None = None) -> None:
        self.result = result or PriceGuardianResult(
            wallets_scanned=1,
            holdings_found=193,
            trading_positions_found=5,
            display_positions=5,
            below_threshold_positions=0,
            position_symbols=["ROBBIE", "WOOD", "STREAM", "p402", "BUY"],
            monitored_tokens=5,
            snapshots_saved=5,
        )
        self.calls: list[tuple[int, bool, bool]] = []

    async def scan_wallet(
        self,
        wallet_id: int,
        *,
        send_alerts: bool = True,
        trigger_attention: bool = True,
    ) -> PriceGuardianResult:
        self.calls.append((wallet_id, send_alerts, trigger_attention))
        return self.result


class FakePriceGuardianService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def fetch_all_holdings(self, chain: str, address: str):
        self.calls.append((chain, address))
        return [
            parse_holding(
                chain,
                {
                    "balance": "1",
                    "history_total_buys": 1,
                    "token": {
                        "symbol": "BUY",
                        "token_address": "0x1111111111111111111111111111111111111111",
                    },
                },
            )
        ]


@pytest.mark.asyncio
async def test_fetch_gmgn_position_holdings_uses_price_guardian_not_blockscout() -> None:
    service = FakePriceGuardianService()

    holdings = await fetch_gmgn_position_holdings(
        [(1, "robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67")],
        service,
    )

    assert service.calls == [("robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67")]
    assert holdings[0].symbol == "BUY"


@pytest.fixture
def handler_ctx(tmp_path):
    app_settings = settings(tmp_path)
    engine = make_engine(app_settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    monitoring = FakeMonitoringService()
    price_guardian = FakeScanPriceGuardianService()
    app = FakeApplication(
        {
            "settings": app_settings,
            "session_factory": session_factory,
            "monitoring_service": monitoring,
            "price_guardian_service": price_guardian,
            "manual_scan_at": {},
        }
    )
    return app_settings, session_factory, monitoring, price_guardian, app, FakeContext(app)


@pytest.mark.asyncio
async def test_add_wallet_first_scan_uses_gmgn_canonical_result(handler_ctx) -> None:
    _, session_factory, monitoring, price_guardian, app, context = handler_ctx
    update = FakeUpdate("0x0e712f06daeab2e866b1477923764af2fc1a9f67")

    await add_wallet_finish(update, context)
    await app.tasks[0]

    with session_scope(session_factory) as session:
        wallet_id = session.scalar(select(Wallet.id))
    assert price_guardian.calls == [(wallet_id, False, False)]
    assert monitoring.calls == 0
    assert "✅ 首次扫描完成" in app.bot.messages[0][1]
    assert "已识别当前持仓：5" in app.bot.messages[0][1]
    assert "忽略Token" not in app.bot.messages[0][1]
    assert "ROBBIE" in app.bot.messages[0][1]


@pytest.mark.asyncio
async def test_add_wallet_first_scan_displays_position_without_monitor_eligible_price(
    handler_ctx,
) -> None:
    _, _, _, price_guardian, app, context = handler_ctx
    price_guardian.result = PriceGuardianResult(
        wallets_scanned=1,
        holdings_found=1,
        trading_positions_found=1,
        display_positions=1,
        position_symbols=["NOPRICE"],
        monitored_tokens=0,
        snapshots_saved=0,
    )
    update = FakeUpdate("0x0e712f06daeab2e866b1477923764af2fc1a9f67")

    await add_wallet_finish(update, context)
    await app.tasks[0]

    assert "已识别当前持仓：1" in app.bot.messages[0][1]
    assert "🟢 NOPRICE" in app.bot.messages[0][1]
    assert "隐藏<$5" not in app.bot.messages[0][1]


@pytest.mark.asyncio
async def test_add_wallet_first_scan_does_not_show_legacy_count_when_blockscout_would_find_many(
    handler_ctx,
) -> None:
    _, session_factory, monitoring, price_guardian, app, context = handler_ctx
    update = FakeUpdate("0x0e712f06daeab2e866b1477923764af2fc1a9f67")

    await add_wallet_finish(update, context)
    await app.tasks[0]

    assert monitoring.calls == 0
    assert price_guardian.calls
    assert "已识别当前持仓：5" in app.bot.messages[0][1]
    assert "188" not in app.bot.messages[0][1]


@pytest.mark.asyncio
async def test_manual_scan_uses_gmgn_canonical_path(handler_ctx) -> None:
    _, session_factory, monitoring, price_guardian, _, context = handler_ctx
    with session_scope(session_factory) as session:
        WalletService(session).add_wallet(
            telegram_user_id=1,
            telegram_chat_id=100,
            address="0x0e712f06daeab2e866b1477923764af2fc1a9f67",
            chain="robinhood",
        )
    update = FakeUpdate("🔄 立即扫描")

    await manual_scan(update, context)

    assert monitoring.calls == 0
    assert len(price_guardian.calls) == 1
    assert price_guardian.calls[0][1:] == (False, False)
    assert "🔄 扫描完成" in update.message.replies[0]
    assert "当前持仓：5" in update.message.replies[0]
    assert "新增持仓" not in update.message.replies[0]


@pytest.mark.asyncio
async def test_manual_scan_displays_position_without_monitor_eligible_price(handler_ctx) -> None:
    _, session_factory, _, price_guardian, _, context = handler_ctx
    price_guardian.result = PriceGuardianResult(
        wallets_scanned=1,
        holdings_found=1,
        trading_positions_found=1,
        display_positions=1,
        position_symbols=["NOPRICE"],
        monitored_tokens=0,
        snapshots_saved=0,
    )
    with session_scope(session_factory) as session:
        WalletService(session).add_wallet(
            telegram_user_id=1,
            telegram_chat_id=100,
            address="0x0e712f06daeab2e866b1477923764af2fc1a9f67",
            chain="robinhood",
        )
    update = FakeUpdate("🔄 立即扫描")

    await manual_scan(update, context)

    assert "当前持仓：1" in update.message.replies[0]
    assert "🟢 NOPRICE" in update.message.replies[0]
    assert "隐藏<$5" not in update.message.replies[0]


@pytest.mark.asyncio
async def test_manual_scan_gmgn_failure_does_not_fallback_to_legacy(handler_ctx) -> None:
    _, session_factory, monitoring, price_guardian, _, context = handler_ctx
    price_guardian.result = PriceGuardianResult(wallets_scanned=1, errors=["gmgn_down"])
    with session_scope(session_factory) as session:
        WalletService(session).add_wallet(
            telegram_user_id=1,
            telegram_chat_id=100,
            address="0x0e712f06daeab2e866b1477923764af2fc1a9f67",
            chain="robinhood",
        )
    update = FakeUpdate("🔄 立即扫描")

    await manual_scan(update, context)

    assert monitoring.calls == 0
    assert "⚠️ GMGN 持仓扫描失败" in update.message.replies[0]
    assert "真实持仓" not in update.message.replies[0]
