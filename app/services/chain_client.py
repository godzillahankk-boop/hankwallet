from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

import httpx

from app.utils.address import normalize_evm_address

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TokenBalance:
    chain: str
    contract_address: str
    symbol: str | None
    name: str | None
    decimals: int
    balance: Decimal


@dataclass(frozen=True)
class TokenTransfer:
    chain: str
    tx_hash: str
    token_address: str
    token_symbol: str | None
    token_name: str | None
    token_decimals: int
    from_address: str | None
    to_address: str | None
    amount: Decimal
    block_number: int | None
    timestamp: datetime | None


@dataclass(frozen=True)
class ChainTransaction:
    tx_hash: str
    from_address: str | None
    to_address: str | None
    native_value: Decimal
    timestamp: datetime | None
    block_number: int | None


class ChainClient(Protocol):
    async def get_native_balance(self, chain: str, wallet_address: str) -> Decimal:
        ...

    async def get_token_balances(
        self, chain: str, wallet_address: str
    ) -> list[TokenBalance]:
        ...

    async def get_token_transfers(
        self, chain: str, wallet_address: str, limit: int = 100
    ) -> list[TokenTransfer]:
        ...

    async def get_transaction(self, chain: str, tx_hash: str) -> ChainTransaction | None:
        ...

    async def get_transaction_receipt(self, chain: str, tx_hash: str) -> dict | None:
        ...

    async def aclose(self) -> None:
        ...


class BlockscoutChainClient:
    """Small adapter for Blockscout v2 style explorer APIs."""

    def __init__(
        self,
        base_url: str | None,
        api_key: str | None = None,
        timeout_seconds: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/") if base_url else None
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=timeout_seconds)

    async def get_native_balance(self, chain: str, wallet_address: str) -> Decimal:
        if not self.base_url:
            return Decimal("0")
        data = await self._get(f"/api/v2/addresses/{wallet_address}")
        return self._scale_raw_decimal(data.get("coin_balance", "0"), 18)

    async def get_token_balances(
        self, chain: str, wallet_address: str
    ) -> list[TokenBalance]:
        if not self.base_url:
            logger.warning("CHAIN_API_BASE_URL is not configured; token balances are empty")
            return []
        data = await self._get(f"/api/v2/addresses/{wallet_address}/tokens", {"type": "ERC-20"})
        items = data.get("items", data if isinstance(data, list) else [])
        balances: list[TokenBalance] = []
        for item in items:
            try:
                token = item.get("token") or item
                decimals = int(token.get("decimals") or item.get("decimals") or 18)
                raw_value = item.get("value") or item.get("balance") or "0"
                contract = token.get("address") or item.get("contract_address")
                if not contract:
                    continue
                balances.append(
                    TokenBalance(
                        chain=chain,
                        contract_address=normalize_evm_address(contract),
                        symbol=token.get("symbol"),
                        name=token.get("name"),
                        decimals=decimals,
                        balance=self._scale_raw_decimal(raw_value, decimals),
                    )
                )
            except Exception as exc:
                logger.exception("Parsing Error token balance: %s", exc)
        return balances

    async def get_token_transfers(
        self, chain: str, wallet_address: str, limit: int = 100
    ) -> list[TokenTransfer]:
        if not self.base_url:
            logger.warning("CHAIN_API_BASE_URL is not configured; token transfers are empty")
            return []
        data = await self._get(
            f"/api/v2/addresses/{wallet_address}/token-transfers",
            {"type": "ERC-20"},
        )
        items = data.get("items", data if isinstance(data, list) else [])
        transfers: list[TokenTransfer] = []
        for item in items[:limit]:
            try:
                token = item.get("token") or {}
                total = item.get("total") or {}
                raw_value = total.get("value") or item.get("value") or "0"
                decimals = int(token.get("decimals") or item.get("token_decimals") or 18)
                tx_hash = (
                    item.get("transaction_hash")
                    or item.get("tx_hash")
                    or (item.get("transaction") or {}).get("hash")
                )
                token_address = token.get("address") or item.get("token_address")
                if not tx_hash or not token_address:
                    continue
                transfers.append(
                    TokenTransfer(
                        chain=chain,
                        tx_hash=tx_hash.lower(),
                        token_address=normalize_evm_address(token_address),
                        token_symbol=token.get("symbol"),
                        token_name=token.get("name"),
                        token_decimals=decimals,
                        from_address=self._address_value(item.get("from")),
                        to_address=self._address_value(item.get("to")),
                        amount=self._scale_raw_decimal(raw_value, decimals),
                        block_number=self._int_or_none(item.get("block_number")),
                        timestamp=self._parse_time(item.get("timestamp")),
                    )
                )
            except Exception as exc:
                logger.exception("Parsing Error token transfer: %s", exc)
        return transfers

    async def get_transaction(self, chain: str, tx_hash: str) -> ChainTransaction | None:
        if not self.base_url:
            return None
        try:
            data = await self._get(f"/api/v2/transactions/{tx_hash}")
            return ChainTransaction(
                tx_hash=tx_hash.lower(),
                from_address=self._address_value(data.get("from")),
                to_address=self._address_value(data.get("to")),
                native_value=self._scale_raw_decimal(data.get("value", "0"), 18),
                timestamp=self._parse_time(data.get("timestamp")),
                block_number=self._int_or_none(data.get("block")),
            )
        except Exception as exc:
            logger.exception("API Error transaction %s: %s", tx_hash, exc)
            return None

    async def get_transaction_receipt(self, chain: str, tx_hash: str) -> dict | None:
        if not self.base_url:
            return None
        try:
            return await self._get(f"/api/v2/transactions/{tx_hash}/raw-trace")
        except Exception as exc:
            logger.exception("API Error receipt %s: %s", tx_hash, exc)
            return None

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _get(self, path: str, params: dict | None = None) -> dict:
        if not self.base_url:
            return {}
        request_params = dict(params or {})
        if self.api_key:
            request_params["apikey"] = self.api_key
        url = f"{self.base_url}{path}"
        response = await self.client.get(url, params=request_params)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _scale_raw_decimal(raw_value: object, decimals: int) -> Decimal:
        value = Decimal(str(raw_value or "0"))
        return value / (Decimal(10) ** decimals)

    @staticmethod
    def _parse_time(raw: object) -> datetime | None:
        if not raw:
            return None
        if isinstance(raw, int):
            return datetime.fromtimestamp(raw, tz=timezone.utc).replace(tzinfo=None)
        if isinstance(raw, str):
            value = raw.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(value).astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                return None
        return None

    @staticmethod
    def _address_value(raw: object) -> str | None:
        if raw is None:
            return None
        if isinstance(raw, dict):
            value = raw.get("hash") or raw.get("address")
        else:
            value = str(raw)
        return normalize_evm_address(value) if value else None

    @staticmethod
    def _int_or_none(raw: object) -> int | None:
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None


def build_chain_client(
    base_url: str | None,
    api_key: str | None,
    timeout_seconds: int,
) -> BlockscoutChainClient:
    return BlockscoutChainClient(base_url, api_key, timeout_seconds)

