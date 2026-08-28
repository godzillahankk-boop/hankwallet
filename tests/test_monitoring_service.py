from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import Position, PositionTransaction, User, Wallet, WalletTokenBalance
from app.services.chain_client import ChainTransaction, TokenBalance, TokenTransfer
from app.services.monitoring_service import MonitoringService
from app.services.wallet_service import WalletService

WALLET = "0x1111111111111111111111111111111111111111"
DEX = "0x2222222222222222222222222222222222222222"
FOMA = "0x3333333333333333333333333333333333333333"
USDC = "0x4444444444444444444444444444444444444444"
DUST = "0x5555555555555555555555555555555555555555"


@dataclass
class FakeChainClient:
    balances: list[TokenBalance] = field(default_factory=list)
    transfers: list[TokenTransfer] = field(default_factory=list)
    fail_transfers: bool = False

    async def get_native_balance(self, chain: str, wallet_address: str) -> Decimal:
        return Decimal("0")

    async def get_token_balances(
        self, chain: str, wallet_address: str
    ) -> list[TokenBalance]:
        return self.balances

    async def get_token_transfers(
        self, chain: str, wallet_address: str, limit: int = 100
    ) -> list[TokenTransfer]:
        if self.fail_transfers:
            raise RuntimeError("transfer_api_down")
        return self.transfers[:limit]

    async def get_transaction(self, chain: str, tx_hash: str) -> ChainTransaction | None:
        return None

    async def get_transaction_receipt(self, chain: str, tx_hash: str) -> dict | None:
        return None

    async def aclose(self) -> None:
        return None


def settings(tmp_path) -> Settings:
    return Settings(
        telegram_bot_token="test-token",
        database_url=f"sqlite:///{tmp_path}/wallet_agent_test.db",
        wallet_scan_interval_seconds=60,
        legacy_wallet_scan_enabled=False,
        manual_scan_cooldown_seconds=30,
        dust_threshold=Decimal("0.000001"),
        dust_clear_confirmation_scans=2,
        default_chain="ethereum",
        chain_api_base_url=None,
        chain_api_key=None,
        chain_rpc_url=None,
        chain_token_search_symbols=(),
        chain_request_timeout_seconds=20,
        token_transfer_lookback_limit=100,
        log_level="INFO",
        api_host="127.0.0.1",
        api_port=8000,
        gmgn_enabled=False,
        gmgn_api_base_url="https://openapi.gmgn.ai",
        gmgn_api_key=None,
        gmgn_private_key_path=None,
        gmgn_request_timeout_seconds=20,
        price_guardian_enabled=True,
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
        attention_engine_enabled=True,
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


def make_service(tmp_path, chain: FakeChainClient):
    app_settings = settings(tmp_path)
    engine = make_engine(app_settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "ethereum")
        wallet_id = wallet.id
    service = MonitoringService(session_factory, chain, app_settings)
    return service, session_factory, wallet_id


def balance(token: str, symbol: str, amount: str) -> TokenBalance:
    return TokenBalance(
        chain="ethereum",
        contract_address=token,
        symbol=symbol,
        name=symbol,
        decimals=18,
        balance=Decimal(amount),
    )


def transfer(
    tx_hash: str,
    token: str,
    symbol: str,
    from_address: str,
    to_address: str,
    amount: str,
) -> TokenTransfer:
    return TokenTransfer(
        chain="ethereum",
        tx_hash=tx_hash,
        token_address=token,
        token_symbol=symbol,
        token_name=symbol,
        token_decimals=18,
        from_address=from_address,
        to_address=to_address,
        amount=Decimal(amount),
        block_number=1,
        timestamp=None,
    )


def buy_tx(tx_hash: str, amount: str = "1000") -> list[TokenTransfer]:
    return [
        transfer(tx_hash, USDC, "USDC", WALLET, DEX, "10"),
        transfer(tx_hash, FOMA, "FOMA", DEX, WALLET, amount),
    ]


def sell_tx(tx_hash: str, amount: str = "400") -> list[TokenTransfer]:
    return [
        transfer(tx_hash, FOMA, "FOMA", WALLET, DEX, amount),
        transfer(tx_hash, USDC, "USDC", DEX, WALLET, "4"),
    ]


@pytest.mark.asyncio
async def test_buy_creates_open_position(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], buy_tx("0xbuy1"))
    service, session_factory, wallet_id = make_service(tmp_path, chain)

    result = await service.scan_wallet(wallet_id, reason="test")

    assert result.positions_created == 1
    with session_scope(session_factory) as session:
        position = session.scalar(select(Position))
        assert position is not None
        assert position.status == "OPEN"
        assert Decimal(str(position.token_amount)) == Decimal("1000")


@pytest.mark.asyncio
async def test_position_increase_does_not_create_duplicate_position(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], buy_tx("0xbuy1"))
    service, session_factory, wallet_id = make_service(tmp_path, chain)
    await service.scan_wallet(wallet_id, reason="test")

    chain.balances = [balance(FOMA, "FOMA", "1500")]
    chain.transfers = buy_tx("0xbuy1") + buy_tx("0xbuy2", "500")
    result = await service.scan_wallet(wallet_id, reason="test")

    assert result.positions_created == 0
    with session_scope(session_factory) as session:
        positions = list(session.scalars(select(Position)))
        assert len([p for p in positions if p.status == "OPEN"]) == 1
        assert Decimal(str(positions[0].token_amount)) == Decimal("1500")


@pytest.mark.asyncio
async def test_partial_sell_keeps_position_open(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], buy_tx("0xbuy1"))
    service, session_factory, wallet_id = make_service(tmp_path, chain)
    await service.scan_wallet(wallet_id, reason="test")

    chain.balances = [balance(FOMA, "FOMA", "600")]
    chain.transfers = buy_tx("0xbuy1") + sell_tx("0xsell1", "400")
    await service.scan_wallet(wallet_id, reason="test")

    with session_scope(session_factory) as session:
        position = session.scalar(select(Position).where(Position.status == "OPEN"))
        assert position is not None
        assert Decimal(str(position.token_amount)) == Decimal("600")


@pytest.mark.asyncio
async def test_position_closes_after_two_dust_scans(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], buy_tx("0xbuy1"))
    service, session_factory, wallet_id = make_service(tmp_path, chain)
    await service.scan_wallet(wallet_id, reason="test")

    chain.balances = []
    chain.transfers = sell_tx("0xsell1", "1000")
    first = await service.scan_wallet(wallet_id, reason="test")
    second = await service.scan_wallet(wallet_id, reason="test")

    assert first.positions_closed == 0
    assert second.positions_closed == 1
    with session_scope(session_factory) as session:
        position = session.scalar(select(Position))
        assert position.status == "CLOSED"


@pytest.mark.asyncio
async def test_duplicate_scan_does_not_duplicate_transaction(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], buy_tx("0xbuy1"))
    service, session_factory, wallet_id = make_service(tmp_path, chain)

    await service.scan_wallet(wallet_id, reason="test")
    await service.scan_wallet(wallet_id, reason="test")

    with session_scope(session_factory) as session:
        tx_count = len(list(session.scalars(select(PositionTransaction))))
        position_count = len(list(session.scalars(select(Position))))
        assert tx_count == 1
        assert position_count == 1


@pytest.mark.asyncio
async def test_dust_transfer_in_does_not_create_open_position(tmp_path) -> None:
    chain = FakeChainClient(
        [balance(DUST, "AIRDROP", "0.0000001")],
        [transfer("0xdust1", DUST, "AIRDROP", DEX, WALLET, "0.0000001")],
    )
    service, session_factory, wallet_id = make_service(tmp_path, chain)

    result = await service.scan_wallet(wallet_id, reason="test")

    assert result.positions_created == 0
    with session_scope(session_factory) as session:
        open_positions = list(
            session.scalars(select(Position).where(Position.status == "OPEN"))
        )
        ignored_positions = list(
            session.scalars(select(Position).where(Position.status == "IGNORED"))
        )
        assert open_positions == []
        assert len(ignored_positions) == 1


@pytest.mark.asyncio
async def test_transfer_failure_keeps_scan_successful_and_updates_balance_snapshot(tmp_path) -> None:
    chain = FakeChainClient([balance(FOMA, "FOMA", "1000")], fail_transfers=True)
    service, session_factory, wallet_id = make_service(tmp_path, chain)

    result = await service.scan_wallet(wallet_id, reason="test")

    assert result.error_message is None
    assert result.tokens_found == 1
    with session_scope(session_factory) as session:
        snapshot = session.scalar(
            select(WalletTokenBalance).where(WalletTokenBalance.contract_address == FOMA)
        )
        assert snapshot is not None
        assert snapshot.symbol == "FOMA"
        assert Decimal(str(snapshot.token_amount)) == Decimal("1000")
