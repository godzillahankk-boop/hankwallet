from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from app.core.config import Settings
from app.utils.address import normalize_evm_address

logger = logging.getLogger(__name__)


class GmgnClientError(RuntimeError):
    pass


class GmgnConfigurationError(GmgnClientError):
    pass


class GmgnAuthError(GmgnClientError):
    pass


class GmgnApiError(GmgnClientError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        api_code: int | None = None,
        api_error: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.api_code = api_code
        self.api_error = api_error


@dataclass(frozen=True)
class GmgnHolding:
    chain: str
    token_name: str | None
    symbol: str | None
    contract_address: str | None
    decimals: int | None
    balance: Decimal | None
    current_price_usd: Decimal | None
    usd_value: Decimal | None
    cost_usd: Decimal | None
    avg_cost_usd: Decimal | None
    realized_profit_usd: Decimal | None
    unrealized_profit_usd: Decimal | None
    total_profit_usd: Decimal | None
    historical_bought_cost_usd: Decimal | None
    historical_sold_income_usd: Decimal | None
    buy_tx_count: int | None
    sell_tx_count: int | None
    last_active_time: int | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnActivity:
    chain: str
    activity_type: str | None
    token_name: str | None
    symbol: str | None
    contract_address: str | None
    token_amount: Decimal | None
    price: Decimal | None
    price_usd: Decimal | None
    # GMGN activity cost/cost_usd is the transaction-level USD value in observed Robinhood responses.
    transaction_value_usd: Decimal | None
    cost_usd: Decimal | None
    quote_token: str | None
    quote_amount: Decimal | None
    from_address: str | None
    to_address: str | None
    tx_hash: str | None
    timestamp: int | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnStats:
    chain: str
    wallet_address: str
    realized_profit_usd: Decimal | None
    unrealized_profit_usd: Decimal | None
    total_profit_usd: Decimal | None
    win_rate: Decimal | None
    buy_tx_count: int | None
    sell_tx_count: int | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnPage:
    items: list[dict[str, Any]]
    next_cursor: str | None
    raw: Any = field(repr=False)


class GmgnClient:
    """GMGN OpenAPI portfolio client.

    This client intentionally exposes only read/portfolio methods. It does not
    implement swap, order, transfer, or any trading endpoint.
    """

    def __init__(
        self,
        api_key: str | None,
        private_key_pem: str | None = None,
        base_url: str = "https://openapi.gmgn.ai",
        timeout_seconds: int = 20,
    ) -> None:
        self.api_key = api_key
        self.private_key_pem = private_key_pem
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=timeout_seconds)

    @classmethod
    def from_settings(cls, settings: Settings) -> GmgnClient:
        private_key_pem = _load_private_key_pem(settings.gmgn_private_key_path)
        return cls(
            api_key=settings.gmgn_api_key,
            private_key_pem=private_key_pem,
            base_url=settings.gmgn_api_base_url,
            timeout_seconds=settings.gmgn_request_timeout_seconds,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def get_wallet_holdings(
        self,
        chain: str,
        wallet_address: str,
        *,
        limit: int = 20,
        cursor: str | None = None,
        order_by: str = "usd_value",
        direction: str = "desc",
        hide_airdrop: bool = False,
        hide_closed: bool = False,
        max_pages: int = 1,
    ) -> list[GmgnHolding]:
        query: dict[str, Any] = {
            "chain": chain,
            "wallet_address": wallet_address,
            "limit": limit,
            "order_by": order_by,
            "direction": direction,
            "hide_airdrop": str(hide_airdrop).lower(),
            "hide_closed": str(hide_closed).lower(),
        }
        if cursor:
            query["cursor"] = cursor
        pages = await self.get_wallet_holdings_pages(query, max_pages=max_pages)
        return [parse_holding(chain, item) for page in pages for item in page.items]

    async def get_wallet_holdings_pages(
        self,
        query: dict[str, Any],
        *,
        max_pages: int = 1,
    ) -> list[GmgnPage]:
        return await self._collect_pages(
            "GET",
            "/v1/user/wallet_holdings",
            query,
            signed=True,
            max_pages=max_pages,
        )

    async def get_wallet_activity(
        self,
        chain: str,
        wallet_address: str,
        *,
        token_address: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
        activity_types: list[str] | None = None,
        max_pages: int = 1,
    ) -> list[GmgnActivity]:
        query: dict[str, Any] = {
            "chain": chain,
            "wallet_address": wallet_address,
            "limit": limit,
        }
        if token_address:
            query["token_address"] = token_address
        if cursor:
            query["cursor"] = cursor
        if activity_types:
            query["type"] = activity_types
        pages = await self._collect_pages(
            "GET",
            "/v1/user/wallet_activity",
            query,
            signed=False,
            max_pages=max_pages,
        )
        return [parse_activity(chain, item) for page in pages for item in page.items]

    async def get_wallet_stats(
        self,
        chain: str,
        wallet_address: str,
        *,
        period: str = "7d",
    ) -> list[GmgnStats]:
        data = await self.get_wallet_stats_raw(chain, wallet_address, period=period)
        items = _extract_items(data)
        if not items and isinstance(data, dict):
            items = [data]
        return [parse_stats(chain, wallet_address, item) for item in items]

    async def get_wallet_stats_raw(
        self,
        chain: str,
        wallet_address: str,
        *,
        period: str = "7d",
    ) -> Any:
        return await self._request(
            "GET",
            "/v1/user/wallet_stats",
            {"chain": chain, "wallet_address": [wallet_address], "period": period},
            signed=False,
        )

    async def get_wallet_token_balance(
        self, chain: str, wallet_address: str, token_address: str
    ) -> dict[str, Any]:
        data = await self._request(
            "GET",
            "/v1/user/wallet_token_balance",
            {
                "chain": chain,
                "wallet_address": wallet_address,
                "token_address": token_address,
            },
            signed=False,
        )
        return data if isinstance(data, dict) else {"data": data}

    async def _collect_pages(
        self,
        method: str,
        path: str,
        query: dict[str, Any],
        *,
        signed: bool,
        max_pages: int,
    ) -> list[GmgnPage]:
        pages: list[GmgnPage] = []
        current_query = dict(query)
        for _ in range(max_pages):
            data = await self._request(method, path, current_query, signed=signed)
            page = GmgnPage(
                items=_extract_items(data),
                next_cursor=_extract_next_cursor(data),
                raw=data,
            )
            pages.append(page)
            if not page.next_cursor:
                break
            current_query["cursor"] = page.next_cursor
        return pages

    async def _request(
        self,
        method: str,
        path: str,
        query: dict[str, Any],
        *,
        signed: bool,
        body: dict[str, Any] | None = None,
    ) -> Any:
        if not self.api_key:
            raise GmgnConfigurationError("GMGN_API_KEY is not configured")
        query_with_auth = {
            **query,
            "timestamp": int(time.time()),
            "client_id": str(uuid.uuid4()),
        }
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = {
            "X-APIKEY": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": "wallet-agent/0.2",
        }
        if signed:
            if not self.private_key_pem:
                raise GmgnConfigurationError("GMGN signing private key is not configured")
            headers["X-Signature"] = _sign_message(
                _build_message(path, query_with_auth, body_str, query_with_auth["timestamp"]),
                self.private_key_pem,
            )
        url = f"{self.base_url}{path}"
        params = _query_pairs(query_with_auth)
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                response = await self.client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    content=body_str if body is not None else None,
                )
                return await self._parse_response(method, path, response)
            except GmgnApiError as exc:
                if exc.status_code in {401, 403}:
                    raise GmgnAuthError(str(exc)) from exc
                if exc.status_code == 429 and attempt == 0:
                    await asyncio.sleep(1)
                    last_error = exc
                    continue
                raise
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                raise GmgnClientError(f"GMGN request failed for {path}") from None
        raise GmgnClientError(f"GMGN request failed for {path}: {last_error}") from None

    async def _parse_response(
        self, method: str, path: str, response: httpx.Response
    ) -> Any:
        try:
            data = response.json()
        except Exception:
            raise GmgnApiError(
                f"GMGN {method} {path} returned non-JSON response",
                status_code=response.status_code,
            ) from None
        if response.status_code >= 400:
            raise GmgnApiError(
                f"GMGN {method} {path} failed with HTTP {response.status_code}",
                status_code=response.status_code,
                api_code=_int_or_none(data.get("code")) if isinstance(data, dict) else None,
                api_error=str(data.get("error")) if isinstance(data, dict) and data.get("error") else None,
            )
        if isinstance(data, dict) and data.get("code") not in (0, None):
            raise GmgnApiError(
                f"GMGN {method} {path} failed with code {data.get('code')}",
                status_code=response.status_code,
                api_code=_int_or_none(data.get("code")),
                api_error=str(data.get("error")) if data.get("error") else None,
            )
        return data.get("data") if isinstance(data, dict) and "data" in data else data


def parse_holding(chain: str, item: dict[str, Any]) -> GmgnHolding:
    token = _token_obj(item)
    contract = _first(
        item,
        token,
        "token_address",
        "contract_address",
        "address",
        "ca",
        "tokenAddress",
    )
    return GmgnHolding(
        chain=chain,
        token_name=_str_or_none(_first(item, token, "token_name", "name", "tokenName")),
        symbol=_str_or_none(_first(item, token, "symbol", "token_symbol", "tokenSymbol")),
        contract_address=_normalize_or_none(contract),
        decimals=_int_or_none(_first(item, token, "decimals", "decimal")),
        balance=_decimal_or_none(_first(item, token, "balance", "amount", "token_amount", "tokenAmount")),
        current_price_usd=_decimal_or_none(_first(item, token, "price", "price_usd", "current_price_usd")),
        usd_value=_decimal_or_none(_first(item, token, "usd_value", "value_usd", "value")),
        cost_usd=_decimal_or_none(_first(item, token, "cost", "cost_usd")),
        avg_cost_usd=_decimal_or_none(_first(item, token, "avg_cost", "avg_cost_usd")),
        realized_profit_usd=_decimal_or_none(_first(item, token, "realized_profit", "realized_profit_usd")),
        unrealized_profit_usd=_decimal_or_none(_first(item, token, "unrealized_profit", "unrealized_profit_usd")),
        total_profit_usd=_decimal_or_none(_first(item, token, "total_profit", "total_profit_usd")),
        historical_bought_cost_usd=_decimal_or_none(_first(item, token, "history_bought_cost", "historical_bought_cost", "historical_bought_cost_usd")),
        historical_sold_income_usd=_decimal_or_none(_first(item, token, "history_sold_income", "historical_sold_income", "historical_sold_income_usd")),
        buy_tx_count=_int_or_none(_first(item, token, "buy_tx_count", "buy_count", "buys", "history_total_buys")),
        sell_tx_count=_int_or_none(_first(item, token, "sell_tx_count", "sell_count", "sells", "history_total_sells")),
        last_active_time=_int_or_none(_first(item, token, "last_active_timestamp", "last_active_time", "last_trade_timestamp")),
        raw=item,
    )


def parse_activity(chain: str, item: dict[str, Any]) -> GmgnActivity:
    token = _token_obj(item)
    quote = _quote_obj(item)
    contract = _first(item, token, "token_address", "contract_address", "address", "ca")
    quote_contract = (
        item.get("quote_address")
        or item.get("quote_token_address")
        or quote.get("token_address")
        or quote.get("address")
    )
    quote_symbol = item.get("quote_symbol") or quote.get("symbol")
    transaction_value_usd = _decimal_or_none(
        _first(item, {}, "cost_usd", "cost", "usd_value", "value_usd")
    )
    return GmgnActivity(
        chain=chain,
        activity_type=_str_or_none(_first(item, {}, "type", "event_type", "activity_type", "side")),
        token_name=_str_or_none(_first(item, token, "token_name", "name")),
        symbol=_str_or_none(_first(item, token, "symbol", "token_symbol")),
        contract_address=_normalize_or_none(contract),
        token_amount=_decimal_or_none(_first(item, token, "amount", "token_amount", "tokenAmount")),
        price=_decimal_or_none(_first(item, token, "price")),
        price_usd=_decimal_or_none(_first(item, token, "price_usd", "usd_price")),
        transaction_value_usd=transaction_value_usd,
        cost_usd=transaction_value_usd,
        quote_token=_normalize_or_none(quote_contract) or _str_or_none(quote_symbol),
        quote_amount=_decimal_or_none(
            item.get("quote_amount") if item.get("quote_amount") not in (None, "") else quote.get("amount")
        ),
        from_address=_normalize_or_none(_first(item, {}, "from", "from_address", "sender")),
        to_address=_normalize_or_none(_first(item, {}, "to", "to_address", "receiver")),
        tx_hash=_str_or_none(_first(item, {}, "tx_hash", "txHash", "hash", "transaction_hash")),
        timestamp=_int_or_none(_first(item, {}, "timestamp", "time", "block_timestamp")),
        raw=item,
    )


def parse_stats(chain: str, wallet_address: str, item: dict[str, Any]) -> GmgnStats:
    nested = _stats_sources(item)
    return GmgnStats(
        chain=chain,
        wallet_address=wallet_address,
        realized_profit_usd=_decimal_or_none(_first_from_sources(nested, "realized_profit", "realized_pnl", "realized_profit_usd")),
        unrealized_profit_usd=_decimal_or_none(_first_from_sources(nested, "unrealized_profit", "unrealized_pnl", "unrealized_profit_usd")),
        total_profit_usd=_decimal_or_none(_first_from_sources(nested, "total_profit", "total_pnl", "profit", "total_profit_usd")),
        win_rate=_decimal_or_none(_first_from_sources(nested, "win_rate", "winrate")),
        buy_tx_count=_int_or_none(_first_from_sources(nested, "buy_tx_count", "buy_count", "buys", "buy")),
        sell_tx_count=_int_or_none(_first_from_sources(nested, "sell_tx_count", "sell_count", "sells", "sell")),
        raw=item,
    )


def _build_message(
    path: str, query_params: dict[str, Any], body: str, timestamp: int
) -> str:
    sorted_qs = "&".join(
        f"{_quote(str(key))}={_quote(str(value))}"
        for key, value in sorted(_query_pairs(query_params), key=lambda pair: (pair[0], str(pair[1])))
    )
    return f"{path}:{sorted_qs}:{body}:{timestamp}"


def _sign_message(message: str, private_key_pem: str) -> str:
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise GmgnConfigurationError(
            "cryptography is required for GMGN signed holdings requests"
        ) from exc
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )
    signature = private_key.sign(message.encode("utf-8"))
    return base64.b64encode(signature).decode("ascii")


def _load_private_key_pem(path_value: str | None) -> str | None:
    direct_value = os.getenv("GMGN_PRIVATE_KEY")
    if direct_value:
        return direct_value.replace("\\n", "\n")
    if not path_value:
        return None
    path = Path(path_value).expanduser()
    if not path.exists():
        raise GmgnConfigurationError("GMGN_PRIVATE_KEY_PATH does not exist")
    content = path.read_text(encoding="utf-8")
    match = re.search(
        r"(-----BEGIN PRIVATE KEY-----[\s\S]+?-----END PRIVATE KEY-----)",
        content,
    )
    return match.group(1) if match else content


def _query_pairs(query: dict[str, Any]) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, list | tuple):
            for item in value:
                pairs.append((key, item))
        else:
            pairs.append((key, value))
    return pairs


def _quote(value: str) -> str:
    return quote(value, safe="-_.!~*'()")


def _extract_items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("items", "list", "data", "holdings", "activities", "result", "rows"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_items(value)
            if nested:
                return nested
    return []


def _extract_next_cursor(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    for key in ("next_cursor", "nextCursor", "cursor"):
        value = data.get(key)
        if value:
            return str(value)
    page = data.get("page") or data.get("pagination")
    if isinstance(page, dict):
        for key in ("next_cursor", "nextCursor", "cursor"):
            value = page.get(key)
            if value:
                return str(value)
    return None


def _token_obj(item: dict[str, Any]) -> dict[str, Any]:
    value = item.get("token") or item.get("token_info") or item.get("base_token")
    return value if isinstance(value, dict) else {}


def _quote_obj(item: dict[str, Any]) -> dict[str, Any]:
    value = item.get("quote") or item.get("quote_token") or item.get("quote_info")
    return value if isinstance(value, dict) else {}


def _stats_sources(item: dict[str, Any]) -> list[dict[str, Any]]:
    sources = [item]
    for key in ("pnl_stat", "stats", "statistics", "period", "summary"):
        value = item.get(key)
        if isinstance(value, dict):
            sources.append(value)
    return sources


def _first(item: dict[str, Any], nested: dict[str, Any], *keys: str) -> Any:
    for source in (item, nested):
        for key in keys:
            if key in source and source[key] not in (None, ""):
                return source[key]
    return None


def _first_from_sources(sources: list[dict[str, Any]], *keys: str) -> Any:
    for source in sources:
        for key in keys:
            if key in source and source[key] not in (None, ""):
                return source[key]
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _str_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _normalize_or_none(value: Any) -> str | None:
    raw = _str_or_none(value)
    if not raw:
        return None
    if raw.startswith("0x") and len(raw) == 42:
        return normalize_evm_address(raw)
    return raw
