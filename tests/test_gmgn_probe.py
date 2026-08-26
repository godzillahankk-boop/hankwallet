from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.chain_client import TokenBalance
from app.services.gmgn_client import parse_holding
from scripts.gmgn_probe import compare_holding_balance

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


class FakeRpcClient:
    def __init__(
        self,
        *,
        erc20_balance: Decimal | None = None,
        native_balance: Decimal | None = None,
        fail_rpc: bool = False,
    ) -> None:
        self.erc20_balance = erc20_balance
        self.native_balance = native_balance
        self.fail_rpc = fail_rpc
        self.erc20_calls = 0
        self.native_calls = 0
        self.blockscout_list_calls = 0

    async def get_erc20_token_balance(
        self,
        chain: str,
        wallet_address: str,
        token_address: str,
        decimals: int | None = None,
    ) -> TokenBalance:
        self.erc20_calls += 1
        if self.fail_rpc:
            raise RuntimeError("rpc_down")
        return TokenBalance(
            chain=chain,
            contract_address=token_address,
            symbol=None,
            name=None,
            decimals=decimals or 18,
            balance=self.erc20_balance or Decimal("0"),
        )

    async def get_native_balance(self, chain: str, wallet_address: str) -> Decimal:
        self.native_calls += 1
        if self.fail_rpc:
            raise RuntimeError("rpc_down")
        return self.native_balance or Decimal("0")

    async def get_token_balances(self, chain: str, wallet_address: str):
        self.blockscout_list_calls += 1
        raise AssertionError("Blockscout token list must not be used for GMGN RPC comparison")


@pytest.mark.asyncio
async def test_gmgn_balance_match() -> None:
    client = FakeRpcClient(erc20_balance=Decimal("249045.742548186937738568"))
    holding = parse_holding(
        "robinhood",
        {"token": {"token_address": TOKEN, "symbol": "WINK", "decimals": 18}, "balance": "249045.742548186937738568"},
    )

    result = await compare_holding_balance("robinhood", WALLET, holding, client)

    assert result.status == "MATCH"
    assert result.diff == Decimal("0E-18")
    assert client.erc20_calls == 1
    assert client.blockscout_list_calls == 0


@pytest.mark.asyncio
async def test_gmgn_balance_mismatch() -> None:
    client = FakeRpcClient(erc20_balance=Decimal("10"))
    holding = parse_holding(
        "robinhood",
        {"token": {"token_address": TOKEN, "symbol": "WINK", "decimals": 18}, "balance": "11"},
    )

    result = await compare_holding_balance("robinhood", WALLET, holding, client)

    assert result.status == "DATA_SOURCE_MISMATCH"
    assert result.diff == Decimal("1")


@pytest.mark.asyncio
async def test_rpc_error_is_not_treated_as_zero() -> None:
    client = FakeRpcClient(fail_rpc=True)
    holding = parse_holding(
        "robinhood",
        {"token": {"token_address": TOKEN, "symbol": "WINK", "decimals": 18}, "balance": "11"},
    )

    result = await compare_holding_balance("robinhood", WALLET, holding, client)

    assert result.status == "RPC_ERROR"
    assert result.rpc_balance is None
    assert result.diff is None


@pytest.mark.asyncio
async def test_native_eth_uses_native_balance_not_erc20() -> None:
    client = FakeRpcClient(native_balance=Decimal("0.186549321218685747"))
    holding = parse_holding(
        "robinhood",
        {
            "token": {"token_address": "0x0000000000000000000000000000000000000000", "symbol": "ETH", "decimals": 18},
            "balance": "0.186549321218685747",
        },
    )

    result = await compare_holding_balance("robinhood", WALLET, holding, client)

    assert result.status == "MATCH"
    assert client.native_calls == 1
    assert client.erc20_calls == 0


@pytest.mark.asyncio
async def test_missing_contract_is_skipped() -> None:
    client = FakeRpcClient(erc20_balance=Decimal("1"))
    holding = parse_holding("robinhood", {"token": {"symbol": "UNKNOWN"}, "balance": "1"})

    result = await compare_holding_balance("robinhood", WALLET, holding, client)

    assert result.status == "SKIPPED_NO_CONTRACT"
    assert client.erc20_calls == 0
    assert client.native_calls == 0
