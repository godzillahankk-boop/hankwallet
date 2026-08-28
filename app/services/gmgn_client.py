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


class GmgnRateLimitError(GmgnApiError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 429,
        api_code: int | None = None,
        api_error: str | None = None,
        retry_after_seconds: float | None = None,
        limit: str | None = None,
        remaining: str | None = None,
        reset: str | None = None,
    ) -> None:
        super().__init__(
            message,
            status_code=status_code,
            api_code=api_code,
            api_error=api_error,
        )
        self.retry_after_seconds = retry_after_seconds
        self.limit = limit
        self.remaining = remaining
        self.reset = reset


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


@dataclass(frozen=True)
class GmgnTokenOverview:
    chain: str
    token_address: str | None
    symbol: str | None
    name: str | None
    price_usd: Decimal | None
    market_cap_usd: Decimal | None
    fdv_usd: Decimal | None
    liquidity_usd: Decimal | None
    holder_count: int | None
    top10_holder_rate: Decimal | None
    smart_money_count: int | None
    kol_count: int | None
    creator_address: str | None
    created_at: int | None
    twitter: str | None
    telegram: str | None
    website: str | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnTokenSecurity:
    chain: str
    token_address: str | None
    is_honeypot: Any
    is_open_source: Any
    owner: str | None
    ownership_renounced: Any
    buy_tax: Decimal | None
    sell_tax: Decimal | None
    top10_holder_rate: Decimal | None
    rug_ratio: Decimal | None
    risk_flags: dict[str, Any]
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnLiquidity:
    chain: str
    token_address: str | None
    pool_address: str | None
    dex: str | None
    liquidity_usd: Decimal | None
    base_reserve: Decimal | None
    quote_reserve: Decimal | None
    quote_address: str | None
    quote_symbol: str | None
    price_usd: Decimal | None
    volume_24h_usd: Decimal | None
    buy_volume_24h_usd: Decimal | None
    sell_volume_24h_usd: Decimal | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnHolder:
    chain: str
    wallet_address: str | None
    balance: Decimal | None
    hold_percentage: Decimal | None
    usd_value: Decimal | None
    avg_cost_usd: Decimal | None
    realized_profit_usd: Decimal | None
    unrealized_profit_usd: Decimal | None
    buy_tx_count: int | None
    sell_tx_count: int | None
    start_holding_at: int | None
    tags: list[str]
    maker_token_tags: list[str]
    twitter_name: str | None
    twitter_username: str | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnTrackTrade:
    chain: str | None
    wallet_address: str | None
    token_address: str | None
    symbol: str | None
    side: str | None
    token_amount: Decimal | None
    usd_value: Decimal | None
    price_usd: Decimal | None
    timestamp: int | None
    tx_hash: str | None
    open_or_close: int | None
    wallet_tags: list[str]
    twitter_username: str | None
    twitter_name: str | None
    raw: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class GmgnMarketSignal:
    chain: str
    event_id: str | None
    token_address: str | None
    signal_type: int | None
    trigger_at: int | None
    trigger_market_cap_usd: Decimal | None
    market_cap_usd: Decimal | None
    liquidity_usd: Decimal | None
    holder_count: int | None
    raw: dict[str, Any] = field(repr=False)


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
        min_request_interval_seconds: float = 0.0,
        rate_limit_max_retries: int = 3,
        rate_limit_backoff_seconds: tuple[float, ...] = (2.0, 5.0, 10.0),
    ) -> None:
        self.api_key = api_key
        self.private_key_pem = private_key_pem
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=timeout_seconds)
        self.min_request_interval_seconds = min_request_interval_seconds
        self.rate_limit_max_retries = rate_limit_max_retries
        self.rate_limit_backoff_seconds = rate_limit_backoff_seconds
        self._request_lock = asyncio.Lock()
        self._last_request_at = 0.0

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

    async def get_token_overview(self, chain: str, token_address: str) -> GmgnTokenOverview:
        data = await self.get_token_overview_raw(chain, token_address)
        return parse_token_overview(chain, data if isinstance(data, dict) else {"data": data})

    async def get_token_overview_raw(self, chain: str, token_address: str) -> Any:
        return await self._request(
            "GET",
            "/v1/token/info",
            {"chain": chain, "address": token_address},
            signed=False,
        )

    async def get_token_security(self, chain: str, token_address: str) -> GmgnTokenSecurity:
        data = await self.get_token_security_raw(chain, token_address)
        return parse_token_security(chain, data if isinstance(data, dict) else {"data": data})

    async def get_token_security_raw(self, chain: str, token_address: str) -> Any:
        return await self._request(
            "GET",
            "/v1/token/security",
            {"chain": chain, "address": token_address},
            signed=False,
        )

    async def get_token_pool_info(self, chain: str, token_address: str) -> GmgnLiquidity:
        data = await self.get_token_pool_info_raw(chain, token_address)
        return parse_liquidity(chain, data if isinstance(data, dict) else {"data": data})

    async def get_token_pool_info_raw(self, chain: str, token_address: str) -> Any:
        return await self._request(
            "GET",
            "/v1/token/pool_info",
            {"chain": chain, "address": token_address},
            signed=False,
        )

    async def get_token_holders(
        self,
        chain: str,
        token_address: str,
        *,
        limit: int = 20,
        order_by: str = "amount_percentage",
        direction: str = "desc",
        tag: str | None = None,
    ) -> list[GmgnHolder]:
        data = await self.get_token_holders_raw(
            chain,
            token_address,
            limit=limit,
            order_by=order_by,
            direction=direction,
            tag=tag,
        )
        return [parse_holder(chain, item) for item in _extract_items(data)]

    async def get_token_holders_raw(
        self,
        chain: str,
        token_address: str,
        *,
        limit: int = 20,
        order_by: str = "amount_percentage",
        direction: str = "desc",
        tag: str | None = None,
    ) -> Any:
        query: dict[str, Any] = {
            "chain": chain,
            "address": token_address,
            "limit": limit,
            "order_by": order_by,
            "direction": direction,
        }
        if tag:
            query["tag"] = tag
        return await self._request(
            "GET",
            "/v1/market/token_top_holders",
            query,
            signed=False,
        )

    async def get_track_feed(
        self,
        feed_type: str,
        *,
        chain: str,
        limit: int = 50,
    ) -> list[GmgnTrackTrade]:
        data = await self.get_track_feed_raw(feed_type, chain=chain, limit=limit)
        return [parse_track_trade(item) for item in _extract_items(data)]

    async def get_track_feed_raw(self, feed_type: str, *, chain: str, limit: int = 50) -> Any:
        if feed_type not in {"kol", "smartmoney"}:
            raise ValueError("feed_type must be 'kol' or 'smartmoney'")
        return await self._request(
            "GET",
            f"/v1/user/{feed_type}",
            {"chain": chain, "limit": limit},
            signed=False,
        )

    async def get_market_signals(
        self,
        chain: str,
        groups: list[dict[str, Any]] | None = None,
    ) -> list[GmgnMarketSignal]:
        data = await self.get_market_signals_raw(chain, groups=groups)
        return [parse_market_signal(chain, item) for item in _extract_items(data)]

    async def get_market_signals_raw(
        self,
        chain: str,
        groups: list[dict[str, Any]] | None = None,
    ) -> Any:
        return await self._request(
            "POST",
            "/v1/market/token_signal",
            {},
            signed=False,
            body={"chain": chain, "groups": groups or [{}]},
        )

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
        max_attempts = max(1, self.rate_limit_max_retries + 1)
        for attempt in range(max_attempts):
            try:
                await self._throttle_request()
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
                if isinstance(exc, GmgnRateLimitError) and attempt < max_attempts - 1:
                    delay = self._rate_limit_delay(exc, attempt)
                    logger.warning(
                        "GMGN rate limited path=%s retry_after=%s limit=%s remaining=%s reset=%s attempt=%s",
                        path,
                        delay,
                        exc.limit,
                        exc.remaining,
                        exc.reset,
                        attempt + 1,
                    )
                    await asyncio.sleep(delay)
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

    async def _throttle_request(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        async with self._request_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_at
            wait_seconds = self.min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_at = time.monotonic()

    def _rate_limit_delay(self, exc: GmgnRateLimitError, attempt: int) -> float:
        if exc.retry_after_seconds is not None:
            return max(0.0, exc.retry_after_seconds)
        if exc.reset:
            reset_delay = _reset_delay_seconds(exc.reset)
            if reset_delay is not None:
                return reset_delay
        if self.rate_limit_backoff_seconds:
            index = min(attempt, len(self.rate_limit_backoff_seconds) - 1)
            return self.rate_limit_backoff_seconds[index]
        return 2.0

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
        if response.status_code == 429:
            rate_headers = _rate_limit_headers(response)
            raise GmgnRateLimitError(
                f"GMGN {method} {path} rate limited with HTTP 429",
                api_code=_int_or_none(data.get("code")) if isinstance(data, dict) else None,
                api_error=str(data.get("error")) if isinstance(data, dict) and data.get("error") else None,
                retry_after_seconds=_retry_after_seconds(response.headers.get("Retry-After")),
                limit=rate_headers.get("limit"),
                remaining=rate_headers.get("remaining"),
                reset=rate_headers.get("reset"),
            )
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


def parse_token_overview(chain: str, item: dict[str, Any]) -> GmgnTokenOverview:
    price = _dict_or_empty(item.get("price"))
    pool = _dict_or_empty(item.get("pool"))
    dev = _dict_or_empty(item.get("dev"))
    link = _dict_or_empty(item.get("link"))
    stat = _dict_or_empty(item.get("stat"))
    tags_stat = _dict_or_empty(item.get("wallet_tags_stat"))
    address = _first(item, {}, "address", "token_address", "contract_address")
    price_usd = _decimal_or_none(_first(price, item, "price", "price_usd"))
    market_cap = _decimal_or_none(_first(item, price, "market_cap", "marketcap", "mcap"))
    if market_cap is None and price_usd is not None:
        supply = _decimal_or_none(_first(item, {}, "circulating_supply", "total_supply"))
        market_cap = price_usd * supply if supply is not None else None
    return GmgnTokenOverview(
        chain=chain,
        token_address=_normalize_or_none(address),
        symbol=_str_or_none(_first(item, {}, "symbol", "token_symbol")),
        name=_str_or_none(_first(item, {}, "name", "token_name")),
        price_usd=price_usd,
        market_cap_usd=market_cap,
        fdv_usd=_decimal_or_none(_first(item, price, "fdv", "fully_diluted_value", "fully_diluted_market_cap")),
        liquidity_usd=_decimal_or_none(_first(item, pool, "liquidity", "liquidity_usd")),
        holder_count=_int_or_none(_first(item, stat, "holder_count", "holders")),
        top10_holder_rate=_decimal_or_none(_first(stat, dev, "top_10_holder_rate", "top10_holder_rate")),
        smart_money_count=_int_or_none(
            _first(tags_stat, item, "smart_degen_wallets", "smart_degen_count", "smart_money_count")
        ),
        kol_count=_int_or_none(_first(tags_stat, item, "renowned_wallets", "renowned_count", "kol_count")),
        creator_address=_normalize_or_none(_first(dev, item, "creator_address", "creator")),
        created_at=_int_or_none(_first(item, {}, "creation_timestamp", "created_at", "created_timestamp")),
        twitter=_str_or_none(_first(link, item, "twitter_username", "twitter")),
        telegram=_str_or_none(_first(link, item, "telegram")),
        website=_str_or_none(_first(link, item, "website")),
        raw=item,
    )


def parse_token_security(chain: str, item: dict[str, Any]) -> GmgnTokenSecurity:
    risk_keys = (
        "renounced_mint",
        "renounced_freeze_account",
        "blacklist",
        "is_blacklisted",
        "pause",
        "pausable",
        "can_mint",
        "mintable",
        "is_wash_trading",
        "rat_trader_amount_rate",
        "bundler_trader_amount_rate",
        "sniper_count",
        "burn_status",
        "dev_team_hold_rate",
        "creator_balance_rate",
        "creator_token_status",
        "suspected_insider_hold_rate",
    )
    return GmgnTokenSecurity(
        chain=chain,
        token_address=_normalize_or_none(_first(item, {}, "address", "token_address", "contract_address")),
        is_honeypot=_first(item, {}, "is_honeypot", "honeypot"),
        is_open_source=_first(item, {}, "open_source", "is_open_source"),
        owner=_normalize_or_none(_first(item, {}, "owner", "owner_address")),
        ownership_renounced=_first(item, {}, "owner_renounced", "ownership_renounced", "is_renounced"),
        buy_tax=_decimal_or_none(_first(item, {}, "buy_tax")),
        sell_tax=_decimal_or_none(_first(item, {}, "sell_tax")),
        top10_holder_rate=_decimal_or_none(_first(item, {}, "top_10_holder_rate", "top10_holder_rate")),
        rug_ratio=_decimal_or_none(_first(item, {}, "rug_ratio")),
        risk_flags={key: item.get(key) for key in risk_keys if key in item},
        raw=item,
    )


def parse_liquidity(chain: str, item: dict[str, Any]) -> GmgnLiquidity:
    pool = _dict_or_empty(item.get("pool"))
    price = _dict_or_empty(item.get("price"))
    return GmgnLiquidity(
        chain=chain,
        token_address=_normalize_or_none(_first(item, pool, "base_address", "token_address", "address")),
        pool_address=_normalize_or_none(_first(item, pool, "pool_address", "address")),
        dex=_str_or_none(_first(item, pool, "exchange", "dex")),
        liquidity_usd=_decimal_or_none(_first(item, pool, "liquidity", "liquidity_usd")),
        base_reserve=_decimal_or_none(_first(item, pool, "base_reserve")),
        quote_reserve=_decimal_or_none(_first(item, pool, "quote_reserve")),
        quote_address=_normalize_or_none(_first(item, pool, "quote_address")),
        quote_symbol=_str_or_none(_first(item, pool, "quote_symbol")),
        price_usd=_decimal_or_none(_first(item, pool, "price", "price_usd")),
        volume_24h_usd=_decimal_or_none(_first(item, price, "volume_24h", "volume24h")),
        buy_volume_24h_usd=_decimal_or_none(_first(item, price, "buy_volume_24h", "buyVolume24h")),
        sell_volume_24h_usd=_decimal_or_none(_first(item, price, "sell_volume_24h", "sellVolume24h")),
        raw=item,
    )


def parse_holder(chain: str, item: dict[str, Any]) -> GmgnHolder:
    return GmgnHolder(
        chain=chain,
        wallet_address=_normalize_or_none(_first(item, {}, "address", "wallet_address", "maker")),
        balance=_decimal_or_none(_first(item, {}, "balance", "amount_cur")),
        hold_percentage=_decimal_or_none(_first(item, {}, "amount_percentage", "hold_percentage")),
        usd_value=_decimal_or_none(_first(item, {}, "usd_value")),
        avg_cost_usd=_decimal_or_none(_first(item, {}, "avg_cost", "avg_cost_usd")),
        realized_profit_usd=_decimal_or_none(_first(item, {}, "realized_profit", "realized_profit_usd")),
        unrealized_profit_usd=_decimal_or_none(_first(item, {}, "unrealized_profit", "unrealized_profit_usd")),
        buy_tx_count=_int_or_none(_first(item, {}, "buy_tx_count_cur", "buy_tx_count", "buy_count")),
        sell_tx_count=_int_or_none(_first(item, {}, "sell_tx_count_cur", "sell_tx_count", "sell_count")),
        start_holding_at=_int_or_none(_first(item, {}, "start_holding_at", "start_holding_timestamp")),
        tags=_list_of_str(item.get("tags")),
        maker_token_tags=_list_of_str(item.get("maker_token_tags")),
        twitter_name=_str_or_none(_first(item, {}, "twitter_name", "name")),
        twitter_username=_str_or_none(_first(item, {}, "twitter_username")),
        raw=item,
    )


def parse_track_trade(item: dict[str, Any]) -> GmgnTrackTrade:
    token = _token_obj(item)
    maker_info = _dict_or_empty(item.get("maker_info"))
    return GmgnTrackTrade(
        chain=_str_or_none(_first(item, {}, "chain")),
        wallet_address=_normalize_or_none(_first(item, maker_info, "maker", "address", "wallet_address")),
        token_address=_normalize_or_none(_first(item, token, "base_address", "address", "token_address")),
        symbol=_str_or_none(_first(token, item, "symbol", "base_symbol")),
        side=_str_or_none(_first(item, {}, "side", "type")),
        token_amount=_decimal_or_none(_first(item, {}, "token_amount", "base_amount")),
        usd_value=_decimal_or_none(_first(item, {}, "amount_usd", "cost_usd", "usd_value")),
        price_usd=_decimal_or_none(_first(item, {}, "price_usd")),
        timestamp=_int_or_none(_first(item, {}, "timestamp", "time")),
        tx_hash=_str_or_none(_first(item, {}, "transaction_hash", "tx_hash", "hash")),
        open_or_close=_int_or_none(_first(item, {}, "is_open_or_close")),
        wallet_tags=_list_of_str(maker_info.get("tags")),
        twitter_username=_str_or_none(_first(maker_info, {}, "twitter_username")),
        twitter_name=_str_or_none(_first(maker_info, {}, "twitter_name", "name")),
        raw=item,
    )


def parse_market_signal(chain: str, item: dict[str, Any]) -> GmgnMarketSignal:
    cur_data = _dict_or_empty(item.get("cur_data"))
    return GmgnMarketSignal(
        chain=chain,
        event_id=_str_or_none(_first(item, {}, "id", "event_id", "signal_id")),
        token_address=_normalize_or_none(_first(item, {}, "token_address", "address")),
        signal_type=_int_or_none(_first(item, {}, "signal_type")),
        trigger_at=_int_or_none(_first(item, {}, "trigger_at")),
        trigger_market_cap_usd=_decimal_or_none(_first(item, {}, "trigger_mc", "first_trigger_mc")),
        market_cap_usd=_decimal_or_none(_first(item, {}, "market_cap")),
        liquidity_usd=_decimal_or_none(_first(cur_data, item, "liquidity")),
        holder_count=_int_or_none(_first(cur_data, item, "holder_count")),
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
    for key in (
        "items",
        "list",
        "rank",
        "signals",
        "tokens",
        "followings",
        "data",
        "holdings",
        "activities",
        "result",
        "rows",
    ):
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


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _reset_delay_seconds(value: str) -> float | None:
    try:
        reset_at = float(value)
    except ValueError:
        return None
    if reset_at <= 0:
        return None
    return max(0.0, reset_at - time.time())


def _rate_limit_headers(response: httpx.Response) -> dict[str, str | None]:
    lower_headers = {key.lower(): value for key, value in response.headers.items()}
    return {
        "limit": _first_header(lower_headers, "x-ratelimit-limit", "ratelimit-limit", "x-rate-limit-limit"),
        "remaining": _first_header(
            lower_headers,
            "x-ratelimit-remaining",
            "ratelimit-remaining",
            "x-rate-limit-remaining",
        ),
        "reset": _first_header(lower_headers, "x-ratelimit-reset", "ratelimit-reset", "x-rate-limit-reset"),
    }


def _first_header(headers: dict[str, str], *names: str) -> str | None:
    for name in names:
        value = headers.get(name)
        if value not in (None, ""):
            return value
    return None


def _token_obj(item: dict[str, Any]) -> dict[str, Any]:
    value = item.get("token") or item.get("token_info") or item.get("base_token")
    return value if isinstance(value, dict) else {}


def _quote_obj(item: dict[str, Any]) -> dict[str, Any]:
    value = item.get("quote") or item.get("quote_token") or item.get("quote_info")
    return value if isinstance(value, dict) else {}


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_of_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in (None, "")]


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
