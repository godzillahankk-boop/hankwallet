from decimal import Decimal

import pytest

from app.services.chain_client import BlockscoutChainClient


@pytest.mark.asyncio
async def test_blockscout_token_balance_accepts_address_hash(monkeypatch) -> None:
    client = BlockscoutChainClient("https://example.test")

    async def fake_get_paginated_items(path, params, limit=None):
        return [
            {
                "token": {
                    "address_hash": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
                    "decimals": "6",
                    "symbol": "USDG",
                    "name": "Global Dollar",
                },
                "value": "59913282",
            }
        ]

    monkeypatch.setattr(client, "_get_paginated_items", fake_get_paginated_items)

    balances = await client.get_token_balances("robinhood", "0xwallet")

    assert len(balances) == 1
    assert balances[0].contract_address == "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
    assert balances[0].balance == Decimal("59.913282")
    await client.aclose()


@pytest.mark.asyncio
async def test_blockscout_token_balance_uses_rpc_balance_when_configured(monkeypatch) -> None:
    client = BlockscoutChainClient("https://example.test", rpc_url="https://rpc.example.test")

    async def fake_get_paginated_items(path, params, limit=None):
        return [
            {
                "token": {
                    "address_hash": "0x8ad5a580c4215086dec828d8626b95a06d7d00cc",
                    "decimals": "18",
                    "symbol": "WINK",
                    "name": "WINK",
                    "exchange_rate": "0.00019016",
                },
                "value": "31263963255605349557091",
            }
        ]

    async def fake_rpc(method, params):
        assert method == "eth_call"
        assert params[0]["to"] == "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
        return hex(195245079808140984102976)

    monkeypatch.setattr(client, "_get_paginated_items", fake_get_paginated_items)
    monkeypatch.setattr(client, "_rpc", fake_rpc)

    balances = await client.get_token_balances(
        "robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
    )

    assert len(balances) == 1
    assert balances[0].balance == Decimal("195245.079808140984102976")
    assert balances[0].usd_value == Decimal("37.12780437631608953702191616")
    await client.aclose()


@pytest.mark.asyncio
async def test_blockscout_token_balance_merges_symbol_search_fallback(monkeypatch) -> None:
    client = BlockscoutChainClient(
        "https://example.test",
        rpc_url="https://rpc.example.test",
        token_search_symbols=("DTF",),
    )

    async def fake_get_paginated_items(path, params, limit=None):
        return []

    async def fake_get(path, params=None):
        assert path == "/api/v2/search"
        return {
            "items": [
                {
                    "type": "token",
                    "name": "Down to Finance",
                    "symbol": "DTF",
                    "address_hash": "0xeE5576Fa1Bcaa380e591D01245f406f3f384eb01",
                }
            ]
        }

    async def fake_rpc(method, params):
        assert method == "eth_call"
        data = params[0]["data"]
        if data == "0x313ce567":
            return "0x12"
        return hex(2491843580796623431134)

    monkeypatch.setattr(client, "_get_paginated_items", fake_get_paginated_items)
    monkeypatch.setattr(client, "_get", fake_get)
    monkeypatch.setattr(client, "_rpc", fake_rpc)

    balances = await client.get_token_balances(
        "robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
    )

    assert len(balances) == 1
    assert balances[0].symbol == "DTF"
    assert balances[0].contract_address == "0xee5576fa1bcaa380e591d01245f406f3f384eb01"
    assert balances[0].balance == Decimal("2491.843580796623431134")
    await client.aclose()


@pytest.mark.asyncio
async def test_blockscout_token_transfer_accepts_address_hash(monkeypatch) -> None:
    client = BlockscoutChainClient("https://example.test")

    async def fake_get_paginated_items(path, params, limit=None):
        return [
            {
                "transaction_hash": "0xabc",
                "from": {"hash": "0x2222222222222222222222222222222222222222"},
                "to": {"hash": "0x1111111111111111111111111111111111111111"},
                "block_number": 1,
                "timestamp": "2026-08-25T10:36:17.000000Z",
                "token": {
                    "address_hash": "0x532c5583671870723CEEf573600208aF49c87c54",
                    "decimals": "9",
                    "symbol": "CNPY",
                    "name": "Canopy Finance",
                },
                "total": {"value": "1234567890"},
            }
        ]

    monkeypatch.setattr(client, "_get_paginated_items", fake_get_paginated_items)

    transfers = await client.get_token_transfers(
        "robinhood", "0x1111111111111111111111111111111111111111"
    )

    assert len(transfers) == 1
    assert transfers[0].token_address == "0x532c5583671870723ceef573600208af49c87c54"
    assert transfers[0].amount == Decimal("1.23456789")
    await client.aclose()
