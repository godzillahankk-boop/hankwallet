from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

import httpx

from app.utils.address import normalize_evm_address

logger = logging.getLogger(__name__)


class ChainClientError(RuntimeError):
    pass


class ChainClientConfigurationError(ChainClientError):
    pass


@dataclass(frozen=True)
class TokenBalance:
    chain: str
    contract_address: str
    symbol: str | None
    name: str | None
    decimals: int
    balance: Decimal
    exchange_rate_usd: Decimal | None = None
    usd_value: Decimal | None = None
    is_native: bool = False


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
        rpc_url: str | None = None,
        token_search_symbols: tuple[str, ...] = (),
        timeout_seconds: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/") if base_url else None
        self.api_key = api_key
        self.rpc_url = rpc_url.rstrip("/") if rpc_url else None
        self.token_search_symbols = token_search_symbols
        self.client = httpx.AsyncClient(timeout=timeout_seconds)
        self.max_pages = 10

    async def get_native_balance(self, chain: str, wallet_address: str) -> Decimal:
        return (await self.get_native_token_balance(chain, wallet_address)).balance

    async def get_native_token_balance(
        self, chain: str, wallet_address: str
    ) -> TokenBalance:
        if not self.base_url and not self.rpc_url:
            raise ChainClientConfigurationError("CHAIN_API_BASE_URL is not configured")
        data = {}
        exchange_rate = None
        if self.base_url:
            data = await self._get(f"/api/v2/addresses/{wallet_address}")
            exchange_rate = self._decimal_or_none(data.get("exchange_rate"))
        if self.rpc_url:
            raw_balance = await self._rpc("eth_getBalance", [wallet_address, "latest"])
            balance = self._scale_hex_decimal(raw_balance, 18)
        else:
            balance = self._scale_raw_decimal(data.get("coin_balance", "0"), 18)
        symbol = self._native_symbol(chain)
        return TokenBalance(
            chain=chain,
            contract_address="native",
            symbol=symbol,
            name=symbol,
            decimals=18,
            balance=balance,
            exchange_rate_usd=exchange_rate,
            usd_value=self._usd_value(balance, exchange_rate),
            is_native=True,
        )

    async def get_token_balances(
        self, chain: str, wallet_address: str
    ) -> list[TokenBalance]:
        if not self.base_url:
            raise ChainClientConfigurationError("CHAIN_API_BASE_URL is not configured")
        items = await self._get_paginated_items(
            f"/api/v2/addresses/{wallet_address}/tokens",
            {"type": "ERC-20"},
        )
        balances: list[TokenBalance] = []
        for item in items:
            try:
                token = item.get("token") or item
                decimals = int(token.get("decimals") or item.get("decimals") or 18)
                raw_value = item.get("value") or item.get("balance") or "0"
                balance = self._scale_raw_decimal(raw_value, decimals)
                exchange_rate = self._decimal_or_none(token.get("exchange_rate"))
                contract = (
                    token.get("address")
                    or token.get("address_hash")
                    or item.get("contract_address")
                    or item.get("address_hash")
                )
                if not contract:
                    continue
                balances.append(
                    TokenBalance(
                        chain=chain,
                        contract_address=normalize_evm_address(contract),
                        symbol=token.get("symbol"),
                        name=token.get("name"),
                        decimals=decimals,
                        balance=balance,
                        exchange_rate_usd=exchange_rate,
                        usd_value=self._usd_value(balance, exchange_rate),
                    )
                )
            except Exception as exc:
                logger.exception("Parsing Error token balance: %s", exc)
        if self.rpc_url:
            balances = await self._correct_token_balances_with_rpc(balances, wallet_address)
        if self.rpc_url and self.token_search_symbols:
            balances = await self._merge_symbol_search_balances(
                chain,
                wallet_address,
                balances,
            )
        return balances

    async def get_token_transfers(
        self, chain: str, wallet_address: str, limit: int = 100
    ) -> list[TokenTransfer]:
        if not self.base_url:
            raise ChainClientConfigurationError("CHAIN_API_BASE_URL is not configured")
        items = await self._get_paginated_items(
            f"/api/v2/addresses/{wallet_address}/token-transfers",
            {"type": "ERC-20"},
            limit=limit,
        )
        transfers: list[TokenTransfer] = []
        for item in items:
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
                token_address = (
                    token.get("address")
                    or token.get("address_hash")
                    or item.get("token_address")
                    or item.get("address_hash")
                )
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
            raise ChainClientConfigurationError("CHAIN_API_BASE_URL is not configured")
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
            raise ChainClientConfigurationError("CHAIN_API_BASE_URL is not configured")
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
        request_params = self._clean_params(params or {})
        if self.api_key:
            request_params["apikey"] = self.api_key
        url = f"{self.base_url}{path}"
        last_error: ChainClientError | None = None
        for attempt in range(3):
            try:
                response = await self.client.get(url, params=request_params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                last_error = ChainClientError(
                    f"Chain API HTTP {exc.response.status_code} for {path}"
                )
            except httpx.HTTPError:
                last_error = ChainClientError(f"Chain API request failed for {path}")
            if attempt < 2:
                await asyncio.sleep(0.8 * (attempt + 1))
        raise last_error or ChainClientError(f"Chain API request failed for {path}")

    async def _rpc(self, method: str, params: list) -> str:
        if not self.rpc_url:
            raise ChainClientConfigurationError("CHAIN_RPC_URL is not configured")
        last_error: ChainClientError | None = None
        for attempt in range(3):
            try:
                response = await self.client.post(
                    self.rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                )
                response.raise_for_status()
                data = response.json()
                if data.get("error"):
                    raise ChainClientError(f"Chain RPC error for {method}")
                return data.get("result", "0x0")
            except ChainClientError as exc:
                last_error = exc
            except httpx.HTTPStatusError as exc:
                last_error = ChainClientError(
                    f"Chain RPC HTTP {exc.response.status_code} for {method}"
                )
            except httpx.HTTPError:
                last_error = ChainClientError(f"Chain RPC request failed for {method}")
            if attempt < 2:
                await asyncio.sleep(0.8 * (attempt + 1))
        raise last_error or ChainClientError(f"Chain RPC request failed for {method}")

    async def _correct_token_balances_with_rpc(
        self, balances: list[TokenBalance], wallet_address: str
    ) -> list[TokenBalance]:
        semaphore = asyncio.Semaphore(8)

        async def correct(balance: TokenBalance) -> TokenBalance:
            try:
                async with semaphore:
                    raw = await self._rpc(
                        "eth_call",
                        [
                            {
                                "to": balance.contract_address,
                                "data": self._balance_of_call_data(wallet_address),
                            },
                            "latest",
                        ],
                    )
                current_balance = self._scale_hex_decimal(raw, balance.decimals)
                return TokenBalance(
                    chain=balance.chain,
                    contract_address=balance.contract_address,
                    symbol=balance.symbol,
                    name=balance.name,
                    decimals=balance.decimals,
                    balance=current_balance,
                    exchange_rate_usd=balance.exchange_rate_usd,
                    usd_value=self._usd_value(current_balance, balance.exchange_rate_usd),
                    is_native=balance.is_native,
                )
            except Exception as exc:
                logger.warning(
                    "Chain RPC balanceOf failed token=%s: %s",
                    balance.contract_address,
                    exc,
                )
                return balance

        return list(await asyncio.gather(*(correct(balance) for balance in balances)))

    async def _merge_symbol_search_balances(
        self,
        chain: str,
        wallet_address: str,
        balances: list[TokenBalance],
    ) -> list[TokenBalance]:
        by_address = {balance.contract_address: balance for balance in balances}
        for symbol in self.token_search_symbols:
            try:
                candidates = await self._search_token_candidates(symbol)
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate["contract_address"] not in by_address
                ]
                found = await self._read_positive_search_balances(
                    chain,
                    wallet_address,
                    candidates,
                )
                for balance in found:
                    by_address[balance.contract_address] = balance
            except Exception as exc:
                logger.warning("Token search fallback failed symbol=%s: %s", symbol, exc)
        return list(by_address.values())

    async def _read_positive_search_balances(
        self,
        chain: str,
        wallet_address: str,
        candidates: list[dict],
    ) -> list[TokenBalance]:
        semaphore = asyncio.Semaphore(12)

        async def read_candidate(candidate: dict) -> TokenBalance | None:
            contract = candidate["contract_address"]
            try:
                async with semaphore:
                    raw = await asyncio.wait_for(
                        self._rpc(
                            "eth_call",
                            [
                                {
                                    "to": contract,
                                    "data": self._balance_of_call_data(wallet_address),
                                },
                                "latest",
                            ],
                        ),
                        timeout=8,
                    )
                raw_amount = int(raw, 16)
                if raw_amount <= 0:
                    return None
                decimals = await self._get_token_decimals(contract)
                current_balance = Decimal(raw_amount) / (Decimal(10) ** decimals)
                return TokenBalance(
                    chain=chain,
                    contract_address=contract,
                    symbol=candidate["symbol"],
                    name=candidate["name"],
                    decimals=decimals,
                    balance=current_balance,
                )
            except Exception as exc:
                logger.debug("Token search candidate skipped token=%s: %s", contract, exc)
                return None

        results = await asyncio.gather(*(read_candidate(candidate) for candidate in candidates))
        return [balance for balance in results if balance is not None]

    async def _search_token_candidates(self, symbol: str) -> list[dict]:
        data = await self._get("/api/v2/search", {"q": symbol})
        items = data.get("items", []) if isinstance(data, dict) else []
        candidates: list[dict] = []
        for item in items:
            item_symbol = item.get("symbol")
            contract = item.get("address_hash") or item.get("address")
            if (
                item.get("type") == "token"
                and item_symbol
                and item_symbol.upper() == symbol.upper()
                and contract
            ):
                candidates.append(
                    {
                        "contract_address": normalize_evm_address(contract),
                        "symbol": item_symbol,
                        "name": item.get("name"),
                    }
                )
        return candidates

    async def _get_token_decimals(self, contract_address: str) -> int:
        try:
            raw = await self._rpc(
                "eth_call",
                [{"to": contract_address, "data": "0x313ce567"}, "latest"],
            )
            return int(raw, 16)
        except Exception as exc:
            logger.warning("Chain RPC decimals failed token=%s: %s", contract_address, exc)
            return 18

    async def _get_paginated_items(
        self, path: str, params: dict | None = None, limit: int | None = None
    ) -> list[dict]:
        base_params = self._clean_params(params or {})
        request_params = dict(base_params)
        items: list[dict] = []
        for _ in range(self.max_pages):
            try:
                data = await self._get(path, request_params)
            except ChainClientError:
                if items:
                    logger.warning(
                        "Chain API pagination stopped early for %s after %s items",
                        path,
                        len(items),
                    )
                    return items[:limit] if limit is not None else items
                raise
            if isinstance(data, list):
                items.extend(data)
                break
            page_items = data.get("items", [])
            if isinstance(page_items, list):
                items.extend(page_items)
                if limit is not None and len(items) >= limit:
                    return items[:limit]
            next_page_params = data.get("next_page_params")
            if not next_page_params:
                break
            request_params = dict(base_params)
            request_params.update(self._clean_params(next_page_params))
        return items

    @staticmethod
    def _clean_params(params: dict) -> dict:
        return {key: value for key, value in params.items() if value not in (None, "")}

    @staticmethod
    def _scale_raw_decimal(raw_value: object, decimals: int) -> Decimal:
        value = Decimal(str(raw_value or "0"))
        return value / (Decimal(10) ** decimals)

    @staticmethod
    def _scale_hex_decimal(raw_value: object, decimals: int) -> Decimal:
        value = int(str(raw_value or "0x0"), 16)
        return Decimal(value) / (Decimal(10) ** decimals)

    @staticmethod
    def _balance_of_call_data(wallet_address: str) -> str:
        address = wallet_address.lower().removeprefix("0x").rjust(64, "0")
        return "0x70a08231" + address

    @staticmethod
    def _decimal_or_none(raw_value: object) -> Decimal | None:
        if raw_value in (None, ""):
            return None
        try:
            return Decimal(str(raw_value))
        except Exception:
            return None

    @staticmethod
    def _usd_value(amount: Decimal, exchange_rate: Decimal | None) -> Decimal | None:
        if exchange_rate is None:
            return None
        return amount * exchange_rate

    @staticmethod
    def _native_symbol(chain: str) -> str:
        return {"robinhood": "ETH", "ethereum": "ETH"}.get(chain, "ETH")

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
    rpc_url: str | None,
    token_search_symbols: tuple[str, ...],
    timeout_seconds: int,
) -> BlockscoutChainClient:
    return BlockscoutChainClient(
        base_url,
        api_key,
        rpc_url,
        token_search_symbols,
        timeout_seconds,
    )
