from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import load_settings
from app.services.gmgn_client import (
    GmgnApiError,
    GmgnClient,
    GmgnClientError,
    GmgnConfigurationError,
    GmgnRateLimitError,
    GmgnHolding,
    GmgnMarketSignal,
    parse_holder,
    parse_market_signal,
    parse_track_trade,
)
from app.services.holding_classifier import is_trading_position
from app.utils.address import is_valid_evm_address, normalize_evm_address, short_address

DEFAULT_WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
DEFAULT_TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"

CAPABILITY_ENDPOINTS = {
    "smartmoney-feed": ["/v1/user/smartmoney"],
    "kol-feed": ["/v1/user/kol"],
    "market-signal": ["/v1/market/token_signal"],
    "smartmoney-holder": ["/v1/market/token_top_holders"],
    "kol-holder": ["/v1/market/token_top_holders"],
}

TOKEN_CAPABILITIES = {"smartmoney-holder", "kol-holder"}
FEED_CAPABILITIES = {"smartmoney-feed", "kol-feed", "market-signal"}


@dataclass
class ProbeStatus:
    capability: str
    available: bool
    endpoint: str
    item_count: int | None = None
    error: str | None = None


async def main() -> None:
    args = parse_args()
    settings = load_settings()
    client = GmgnClient.from_settings(settings)
    client.min_request_interval_seconds = args.request_delay
    statuses: list[ProbeStatus] = []
    try:
        capabilities = parse_capability_selection(args.capability, args.capabilities)
        tokens = list(dict.fromkeys(normalize_evm_address(token) for token in args.token))
        if needs_token(capabilities) and not tokens:
            tokens = await select_tokens_from_wallet(
                client,
                args.chain,
                args.wallet,
                settings.price_monitor_min_usd_value,
                args.max_tokens,
            )
        if needs_token(capabilities) and not tokens:
            tokens = [DEFAULT_TOKEN]
        if not tokens:
            tokens = []

        print("=== SELECTED CAPABILITIES ===")
        for capability in capabilities:
            print(f"{capability}: {', '.join(CAPABILITY_ENDPOINTS[capability])}")

        if tokens:
            print()
            print("=== SELECTED TOKENS ===")
            for token in tokens:
                print(token)

        token_set = set(tokens)
        for capability in capabilities:
            print()
            if capability == "smartmoney-feed":
                await probe_feed(
                    client,
                    args.chain,
                    "smartmoney",
                    "SMART MONEY FEED",
                    token_set,
                    statuses,
                    args.debug_raw,
                    args.limit,
                )
            elif capability == "kol-feed":
                await probe_feed(
                    client,
                    args.chain,
                    "kol",
                    "KOL TRADE FEED",
                    token_set,
                    statuses,
                    args.debug_raw,
                    args.limit,
                )
            elif capability == "market-signal":
                await probe_market_signals(client, args.chain, token_set, statuses, args.debug_raw)
            elif capability == "smartmoney-holder":
                await probe_tagged_holders(
                    client,
                    args.chain,
                    tokens[0],
                    "SMART MONEY HOLDERS",
                    "smart_degen",
                    statuses,
                    args.debug_raw,
                    args.limit,
                )
            elif capability == "kol-holder":
                await probe_tagged_holders(
                    client,
                    args.chain,
                    tokens[0],
                    "KOL HOLDERS",
                    "renowned",
                    statuses,
                    args.debug_raw,
                    args.limit,
                )

        print()
        print("=== CAPABILITY STATUS SUMMARY ===")
        for status in statuses:
            result = "OK" if status.available else "FAILED"
            count = "" if status.item_count is None else f" items={status.item_count}"
            error = "" if not status.error else f" error={status.error}"
            print(f"{status.capability}: {result} | {status.endpoint}{count}{error}")
    finally:
        await client.aclose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe GMGN intelligence capabilities")
    parser.add_argument("--chain", default="robinhood")
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--token", action="append", default=[])
    parser.add_argument("--max-tokens", type=int, default=2)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--capability", action="append", default=[])
    parser.add_argument("--capabilities", default="")
    parser.add_argument("--request-delay", type=float, default=1.5)
    parser.add_argument("--debug-raw", action="store_true")
    args = parser.parse_args()
    if args.chain != "robinhood":
        raise SystemExit("This Phase A probe is currently scoped to --chain robinhood")
    if not is_valid_evm_address(args.wallet):
        raise SystemExit("Invalid EVM wallet address")
    for token in args.token:
        if not is_valid_evm_address(token):
            raise SystemExit(f"Invalid EVM token address: {token}")
    try:
        parse_capability_selection(args.capability, args.capabilities)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return args


def parse_capability_selection(capability_args: list[str], capabilities_csv: str) -> list[str]:
    selected: list[str] = []
    for value in capability_args:
        selected.extend(part.strip() for part in value.split(",") if part.strip())
    selected.extend(part.strip() for part in capabilities_csv.split(",") if part.strip())
    if not selected:
        raise ValueError(
            "Please select at least one capability: "
            + ", ".join(sorted(CAPABILITY_ENDPOINTS))
        )
    invalid = [capability for capability in selected if capability not in CAPABILITY_ENDPOINTS]
    if invalid:
        raise ValueError(f"Unknown capability: {', '.join(invalid)}")
    return list(dict.fromkeys(selected))


def needs_token(capabilities: list[str]) -> bool:
    return any(capability in TOKEN_CAPABILITIES for capability in capabilities)


def capability_endpoints(capabilities: list[str]) -> list[str]:
    endpoints: list[str] = []
    for capability in capabilities:
        endpoints.extend(CAPABILITY_ENDPOINTS[capability])
    return list(dict.fromkeys(endpoints))


async def select_tokens_from_wallet(
    client: GmgnClient,
    chain: str,
    wallet: str,
    min_usd_value: Decimal,
    max_tokens: int,
) -> list[str]:
    holdings = await client.get_wallet_holdings(
        chain,
        wallet,
        limit=50,
        order_by="usd_value",
        direction="desc",
        hide_airdrop=True,
        hide_closed=True,
        max_pages=3,
    )
    selected: list[str] = []
    for holding in holdings:
        if not _eligible_holding(holding, min_usd_value):
            continue
        selected.append(holding.contract_address)
        if len(selected) >= max_tokens:
            break
    return [token for token in selected if token]


def _eligible_holding(holding: GmgnHolding, min_usd_value: Decimal) -> bool:
    return (
        is_trading_position(holding)
        and holding.contract_address is not None
        and holding.current_price_usd is not None
        and holding.current_price_usd > 0
        and holding.usd_value is not None
        and holding.usd_value >= min_usd_value
    )


async def probe_tagged_holders(
    client: GmgnClient,
    chain: str,
    token: str,
    title: str,
    tag: str,
    statuses: list[ProbeStatus],
    debug_raw: bool,
    limit: int,
) -> None:
    raw = await fetch_capability(
        statuses,
        title.title(),
        "/v1/market/token_top_holders",
        lambda: client.get_token_holders_raw(
            chain,
            token,
            limit=min(limit, 100),
            order_by="amount_percentage",
            direction="desc",
            tag=tag,
        ),
    )
    print(f"=== {title} ===")
    print(f"token={token} tag={tag}")
    items = _items(raw)
    if raw is not None and not items:
        print("HTTP 200 + empty result: this token currently has no matching holders.")
    for holder in [parse_holder(chain, item) for item in items[:10]]:
        print(
            " | ".join(
                [
                    holder.wallet_address or "-",
                    f"balance={fmt(holder.balance)}",
                    f"hold_percentage={fmt(holder.hold_percentage)}",
                    f"usd={fmt(holder.usd_value)}",
                    f"avg_cost={fmt(holder.avg_cost_usd)}",
                    f"realized={fmt(holder.realized_profit_usd)}",
                    f"unrealized={fmt(holder.unrealized_profit_usd)}",
                    f"buys={holder.buy_tx_count}",
                    f"sells={holder.sell_tx_count}",
                    f"start={holder.start_holding_at}",
                    f"tags={holder.tags}",
                    f"token_tags={holder.maker_token_tags}",
                    f"twitter={holder.twitter_username or holder.twitter_name or '-'}",
                ]
            )
        )
    maybe_print_raw(debug_raw, f"RAW {title}", items[:2])


async def probe_feed(
    client: GmgnClient,
    chain: str,
    feed_type: str,
    title: str,
    watched_tokens: set[str],
    statuses: list[ProbeStatus],
    debug_raw: bool,
    limit: int,
) -> None:
    raw = await fetch_capability(
        statuses,
        title.title(),
        f"/v1/user/{feed_type}",
        lambda: client.get_track_feed_raw(feed_type, chain=chain, limit=min(limit, 200)),
    )
    print(f"=== {title} ===")
    print_raw_metadata(raw)
    items = _items(raw)
    trades = [parse_track_trade(item) for item in items]
    matching = [
        trade
        for trade in trades
        if trade.token_address and normalize_evm_address(trade.token_address) in watched_tokens
    ]
    print(f"items={len(items)} matching_selected_tokens={len(matching)}")
    for trade in (matching or trades)[:10]:
        print(
            " | ".join(
                [
                    trade.wallet_address or "-",
                    trade.side or "-",
                    trade.symbol or "-",
                    trade.token_address or "-",
                    f"amount={fmt(trade.token_amount)}",
                    f"usd={fmt(trade.usd_value)}",
                    f"price={fmt(trade.price_usd)}",
                    f"time={trade.timestamp}",
                    f"tx={trade.tx_hash or '-'}",
                    f"open_close={trade.open_or_close}",
                    f"tags={trade.wallet_tags}",
                    f"twitter={trade.twitter_username or trade.twitter_name or '-'}",
                ]
            )
        )
    maybe_print_raw(debug_raw, f"RAW {title}", items[:2])


async def probe_market_signals(
    client: GmgnClient,
    chain: str,
    watched_tokens: set[str],
    statuses: list[ProbeStatus],
    debug_raw: bool,
) -> None:
    groups = [
        {"signal_type": [6, 7]},
        {"signal_type": [12]},
        {"signal_type": [20]},
    ]
    raw = await fetch_capability(
        statuses,
        "Market Signals",
        "/v1/market/token_signal",
        lambda: client.get_market_signals_raw(chain, groups=groups),
    )
    print("=== MARKET SIGNALS ===")
    print_raw_metadata(raw)
    items = _items(raw)
    signals: list[GmgnMarketSignal] = [parse_market_signal(chain, item) for item in items]
    signal_counts: dict[int, int] = {}
    for signal in signals:
        if signal.signal_type is None:
            continue
        signal_counts[signal.signal_type] = signal_counts.get(signal.signal_type, 0) + 1
    matching = [
        signal
        for signal in signals
        if signal.token_address and normalize_evm_address(signal.token_address) in watched_tokens
    ]
    print(f"items={len(items)} matching_selected_tokens={len(matching)} signal_type_counts={signal_counts}")
    for signal in (matching or signals)[:10]:
        print(
            " | ".join(
                [
                    f"id={signal.event_id or '-'}",
                    f"type={signal.signal_type}",
                    signal.token_address or "-",
                    f"trigger_at={signal.trigger_at}",
                    f"trigger_mc={fmt(signal.trigger_market_cap_usd)}",
                    f"market_cap={fmt(signal.market_cap_usd)}",
                    f"liquidity={fmt(signal.liquidity_usd)}",
                    f"holders={signal.holder_count}",
                ]
            )
        )
    maybe_print_raw(debug_raw, "RAW MARKET SIGNALS", items[:2])


async def fetch_capability(
    statuses: list[ProbeStatus],
    capability: str,
    endpoint: str,
    fetcher,
) -> Any:
    try:
        data = await fetcher()
    except GmgnRateLimitError as exc:
        details = (
            f"{exc}; retry_after={exc.retry_after_seconds}; "
            f"limit={exc.limit}; remaining={exc.remaining}; reset={exc.reset}"
        )
        statuses.append(ProbeStatus(capability, False, endpoint, error=details))
        return None
    except (GmgnConfigurationError, GmgnApiError, GmgnClientError, ValueError) as exc:
        statuses.append(ProbeStatus(capability, False, endpoint, error=str(exc)))
        return None
    statuses.append(ProbeStatus(capability, True, endpoint, item_count=len(_items(data))))
    return data


def _items(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if not isinstance(raw, dict):
        return []
    for key in ("items", "list", "rank", "signals", "tokens", "data", "result", "rows"):
        value = raw.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _items(value)
            if nested:
                return nested
    return []


def print_raw_metadata(raw: Any) -> None:
    if not isinstance(raw, dict):
        return
    keys = sorted(str(key) for key in raw.keys())
    cursors = {
        key: raw.get(key)
        for key in ("cursor", "next_cursor", "nextCursor", "has_more", "hasMore")
        if raw.get(key) not in (None, "")
    }
    page = raw.get("page") or raw.get("pagination")
    if isinstance(page, dict):
        for key in ("cursor", "next_cursor", "nextCursor", "has_more", "hasMore"):
            if page.get(key) not in (None, ""):
                cursors[f"page.{key}"] = page.get(key)
    print(f"raw_keys={keys}")
    print(f"pagination={cursors or '-'}")


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, Decimal):
        return f"{value.normalize():f}"
    return str(value)


def maybe_print_raw(debug_raw: bool, title: str, value: Any) -> None:
    if not debug_raw:
        return
    print()
    print(f"=== {title} ===")
    print(json.dumps(redact(value), ensure_ascii=False, indent=2, default=str))


def redact(value: Any) -> Any:
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(word in key_text for word in ("apikey", "api_key", "private", "signature", "authorization")):
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = redact(item)
        return redacted
    return value


if __name__ == "__main__":
    asyncio.run(main())
