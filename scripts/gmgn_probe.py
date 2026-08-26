from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import load_settings
from app.services.chain_client import build_chain_client
from app.services.gmgn_client import (
    GmgnAuthError,
    GmgnClient,
    GmgnClientError,
    GmgnConfigurationError,
    GmgnHolding,
)
from app.utils.address import is_valid_evm_address

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


@dataclass(frozen=True)
class BalanceComparison:
    symbol: str
    contract_address: str | None
    gmgn_balance: Decimal | None
    rpc_balance: Decimal | None
    diff: Decimal | None
    status: str
    error: str | None = None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Probe GMGN Robinhood wallet data")
    parser.add_argument("wallet_address")
    parser.add_argument("--chain", default="robinhood")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--activity-limit", type=int, default=50)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--debug-raw", action="store_true")
    args = parser.parse_args()

    if not is_valid_evm_address(args.wallet_address):
        raise SystemExit("Invalid EVM wallet address")

    settings = load_settings()
    gmgn_client = GmgnClient.from_settings(settings)
    chain_client = build_chain_client(
        settings.chain_api_base_url,
        settings.chain_api_key,
        settings.chain_rpc_url,
        settings.chain_token_search_symbols,
        settings.chain_request_timeout_seconds,
    )
    try:
        holdings: list[GmgnHolding] = []
        print("=== GMGN HOLDINGS ===")
        try:
            holding_query = _holdings_query(args)
            holding_pages = await gmgn_client.get_wallet_holdings_pages(
                holding_query,
                max_pages=args.max_pages,
            )
            holdings = []
            from app.services.gmgn_client import parse_holding

            for page in holding_pages:
                holdings.extend(parse_holding(args.chain, item) for item in page.items)
            if args.debug_raw:
                print()
                print("=== RAW HOLDING SAMPLE ===")
                raw_items = [item for page in holding_pages for item in page.items][:2]
                print(_json_dump(raw_items))
            if holdings:
                for holding in holdings:
                    print(
                        " | ".join(
                            [
                                holding.symbol or "-",
                                holding.contract_address or "-",
                                f"balance={_fmt(holding.balance)}",
                                f"usd={_fmt(holding.usd_value)}",
                                f"cost={_fmt(holding.cost_usd)}",
                                f"avg_cost={_fmt(holding.avg_cost_usd)}",
                                f"realized={_fmt(holding.realized_profit_usd)}",
                                f"unrealized={_fmt(holding.unrealized_profit_usd)}",
                            ]
                        )
                    )
            else:
                print("(empty)")
        except (GmgnConfigurationError, GmgnAuthError, GmgnClientError) as exc:
            print(f"GMGN holdings failed: {exc}")

        print()
        print("=== GMGN ACTIVITY ===")
        try:
            activities = await gmgn_client.get_wallet_activity(
                args.chain,
                args.wallet_address,
                limit=args.activity_limit,
                max_pages=args.max_pages,
            )
            if activities:
                for activity in activities[: args.activity_limit]:
                    print(
                        " | ".join(
                            [
                                activity.activity_type or "-",
                                activity.symbol or "-",
                                activity.contract_address or "-",
                                f"amount={_fmt(activity.token_amount)}",
                                f"transaction_value_usd={_fmt(activity.transaction_value_usd)}",
                                f"quote={activity.quote_token or '-'}",
                                f"quote_amount={_fmt(activity.quote_amount)}",
                                f"tx={activity.tx_hash or '-'}",
                                f"time={activity.timestamp or '-'}",
                            ]
                        )
                    )
            else:
                print("(empty)")
        except (GmgnConfigurationError, GmgnAuthError, GmgnClientError) as exc:
            print(f"GMGN activity failed: {exc}")

        print()
        print("=== GMGN STATS ===")
        try:
            raw_stats = await gmgn_client.get_wallet_stats_raw(args.chain, args.wallet_address)
            if args.debug_raw:
                print()
                print("=== RAW STATS ===")
                print(_json_dump(raw_stats))
            from app.services.gmgn_client import parse_stats, _extract_items

            stat_items = _extract_items(raw_stats)
            if not stat_items and isinstance(raw_stats, dict):
                stat_items = [raw_stats]
            stats = [parse_stats(args.chain, args.wallet_address, item) for item in stat_items]
            if stats:
                for item in stats:
                    print(
                        " | ".join(
                            [
                                f"realized={_fmt(item.realized_profit_usd)}",
                                f"unrealized={_fmt(item.unrealized_profit_usd)}",
                                f"total={_fmt(item.total_profit_usd)}",
                                f"win_rate={_fmt(item.win_rate)}",
                                f"buys={item.buy_tx_count}",
                                f"sells={item.sell_tx_count}",
                            ]
                        )
                    )
            else:
                print("(empty)")
        except (GmgnConfigurationError, GmgnAuthError, GmgnClientError) as exc:
            print(f"GMGN stats failed: {exc}")

        print()
        print("=== GMGN VS RPC BALANCE SAMPLE ===")
        if holdings:
            await _print_rpc_comparison(args.chain, args.wallet_address, holdings, chain_client)
        else:
            print("(skipped: no GMGN holdings)")
    finally:
        await gmgn_client.aclose()
        await chain_client.aclose()


def _holdings_query(args: argparse.Namespace) -> dict:
    return {
        "chain": args.chain,
        "wallet_address": args.wallet_address,
        "limit": args.limit,
        "order_by": "usd_value",
        "direction": "desc",
        "hide_airdrop": "false",
        "hide_closed": "false",
    }


async def compare_holding_balance(
    chain: str,
    wallet_address: str,
    holding: GmgnHolding,
    chain_client,
    *,
    tolerance: Decimal = Decimal("0.000000000001"),
) -> BalanceComparison:
    symbol = holding.symbol or "-"
    contract = holding.contract_address
    if _is_native_holding(holding):
        try:
            rpc_balance = await chain_client.get_native_balance(chain, wallet_address)
        except Exception as exc:
            return BalanceComparison(symbol, contract, holding.balance, None, None, "RPC_ERROR", str(exc))
        return _comparison_result(symbol, contract, holding.balance, rpc_balance, tolerance)

    if not contract:
        return BalanceComparison(symbol, contract, holding.balance, None, None, "SKIPPED_NO_CONTRACT")

    try:
        rpc_token_balance = await chain_client.get_erc20_token_balance(
            chain,
            wallet_address,
            contract,
            holding.decimals,
        )
    except Exception as exc:
        return BalanceComparison(symbol, contract, holding.balance, None, None, "RPC_ERROR", str(exc))
    return _comparison_result(symbol, contract, holding.balance, rpc_token_balance.balance, tolerance)


def _comparison_result(
    symbol: str,
    contract: str | None,
    gmgn_balance: Decimal | None,
    rpc_balance: Decimal | None,
    tolerance: Decimal,
) -> BalanceComparison:
    if gmgn_balance is None or rpc_balance is None:
        return BalanceComparison(symbol, contract, gmgn_balance, rpc_balance, None, "RPC_ERROR")
    diff = abs(rpc_balance - gmgn_balance)
    status = "MATCH" if diff <= tolerance else "DATA_SOURCE_MISMATCH"
    return BalanceComparison(symbol, contract, gmgn_balance, rpc_balance, diff, status)


def _is_native_holding(holding: GmgnHolding) -> bool:
    contract = (holding.contract_address or "").lower()
    symbol = (holding.symbol or "").upper()
    return contract in {ZERO_ADDRESS, "native"} or (not contract and symbol == "ETH")


async def _print_rpc_comparison(
    chain: str,
    wallet_address: str,
    holdings: list[GmgnHolding],
    chain_client,
) -> None:
    counts: dict[str, int] = {}
    for holding in holdings[:10]:
        comparison = await compare_holding_balance(chain, wallet_address, holding, chain_client)
        counts[comparison.status] = counts.get(comparison.status, 0) + 1
        print(
            " | ".join(
                [
                    comparison.symbol,
                    comparison.contract_address or "-",
                    f"GMGN={_fmt(comparison.gmgn_balance)}",
                    f"RPC={_fmt(comparison.rpc_balance)}",
                    f"DIFF={_fmt(comparison.diff)}",
                    f"STATUS={comparison.status}",
                ]
            )
        )
    print(
        "SUMMARY | "
        + " | ".join(
            f"{key}={counts.get(key, 0)}"
            for key in ("MATCH", "DATA_SOURCE_MISMATCH", "RPC_ERROR", "SKIPPED_NO_CONTRACT")
        )
    )


def _json_dump(value) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _fmt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    text = f"{value.normalize():f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


if __name__ == "__main__":
    asyncio.run(main())
