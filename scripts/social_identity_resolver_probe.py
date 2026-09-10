from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import TokenWatchState, Wallet  # noqa: E402
from app.services.gmgn_client import GmgnClient, GmgnClientError  # noqa: E402
from app.services.social_identity_resolver import SocialIdentityResolution, resolve_social_identities  # noqa: E402


@dataclass(frozen=True)
class WatchedToken:
    wallet_id: int
    wallet_address: str
    chain: str
    token_address: str
    symbol: str | None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Social Identity Resolver Probe V0.1")
    parser.add_argument("--limit", type=int, default=0, help="Optional max active watched tokens to probe")
    parser.add_argument("--debug-raw-fields", action="store_true")
    parser.add_argument("--skip-dev-holder", action="store_true", help="Skip high-weight token_top_holders tag=dev lookup")
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=1.0,
        help="Minimum delay between GMGN requests for this probe",
    )
    args = parser.parse_args()

    settings = load_settings()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    watched = _active_watched_tokens(session_factory, limit=args.limit)
    client = GmgnClient.from_settings(settings)
    client.min_request_interval_seconds = max(client.min_request_interval_seconds, args.min_request_interval_seconds)
    resolutions: list[SocialIdentityResolution] = []
    try:
        for token in watched:
            resolution = await _probe_token(client, token, include_dev_holder=not args.skip_dev_holder)
            resolutions.append(resolution)
            _print_resolution(token, resolution, debug_raw_fields=args.debug_raw_fields)
    finally:
        await client.aclose()
    _print_summary(resolutions)


def _active_watched_tokens(session_factory, *, limit: int = 0) -> list[WatchedToken]:
    with session_scope(session_factory) as session:
        stmt = (
            select(TokenWatchState, Wallet)
            .join(Wallet, Wallet.id == TokenWatchState.wallet_id)
            .where(TokenWatchState.active.is_(True))
            .order_by(TokenWatchState.wallet_id, TokenWatchState.symbol, TokenWatchState.token_address)
        )
        if limit and limit > 0:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()
        return [
            WatchedToken(
                wallet_id=state.wallet_id,
                wallet_address=wallet.address,
                chain=state.chain,
                token_address=state.token_address,
                symbol=state.symbol,
            )
            for state, wallet in rows
        ]


async def _probe_token(
    client: GmgnClient, token: WatchedToken, *, include_dev_holder: bool = True
) -> SocialIdentityResolution:
    token_info: dict[str, Any] = {}
    dev_holder_items: list[dict[str, Any]] = []
    try:
        raw = await client.get_token_overview_raw(token.chain, token.token_address)
        token_info = raw if isinstance(raw, dict) else {}
    except GmgnClientError as exc:
        token_info = {"_probe_error": str(exc)}
    if include_dev_holder:
        try:
            raw_holders = await client.get_token_holders_raw(
                token.chain,
                token.token_address,
                limit=20,
                tag="dev",
            )
            dev_holder_items = _extract_items(raw_holders)
        except GmgnClientError as exc:
            dev_holder_items = [{"_probe_error": str(exc)}]
    return resolve_social_identities(
        token_address=token.token_address,
        symbol=token.symbol,
        token_info=token_info,
        dev_holder_items=dev_holder_items,
    )


def _print_resolution(token: WatchedToken, resolution: SocialIdentityResolution, *, debug_raw_fields: bool) -> None:
    print("=" * 30)
    print(f"TOKEN: {token.symbol or 'UNKNOWN'}")
    print(f"wallet_id: {token.wallet_id}")
    print(f"wallet_address: {token.wallet_address}")
    print(f"CA: {token.token_address}")
    if resolution.raw_fields.get("probe_error"):
        print(f"Probe Error: {resolution.raw_fields['probe_error']}")
    _print_candidates("Project X", resolution.by_type("project_x"), prefix="@")
    _print_candidates("DEV Wallet", resolution.by_type("dev_wallet"))
    _print_candidates("DEV X", resolution.by_type("dev_x"), prefix="@")
    _print_candidates("Website", resolution.by_type("website"))
    _print_candidates("Telegram", resolution.by_type("telegram"))
    _print_candidates("Discord", resolution.by_type("discord"))
    _print_candidates("Launchpad", resolution.by_type("launchpad"))
    if debug_raw_fields:
        print("Raw Identity Fields:")
        print(json.dumps(resolution.raw_fields, ensure_ascii=False, indent=2, default=str))
    print("=" * 30)


def _print_candidates(label: str, candidates: list, *, prefix: str = "") -> None:
    print(f"\n{label}:")
    if not candidates:
        print("UNKNOWN")
        return
    for candidate in candidates:
        print(f"{prefix}{candidate.value}")
        print(f"Source: {candidate.source}.{candidate.source_field}")
        print(f"Confidence: {candidate.confidence}")
        if candidate.evidence:
            print(f"Evidence: {json.dumps(candidate.evidence, ensure_ascii=False, default=str)}")


def _print_summary(resolutions: list[SocialIdentityResolution]) -> None:
    total = len(resolutions)
    probe_errors = sum(1 for item in resolutions if item.raw_fields.get("probe_error"))
    successful = total - probe_errors
    project_x = sum(1 for item in resolutions if item.by_type("project_x"))
    dev_wallet = sum(1 for item in resolutions if item.by_type("dev_wallet"))
    dev_x = sum(1 for item in resolutions if item.by_type("dev_x"))
    project_and_dev_x = sum(1 for item in resolutions if _project_x_equals_dev_x(item))
    project_only = sum(1 for item in resolutions if item.by_type("project_x") and not item.by_type("dev_x"))
    print("\n=== Identity Coverage Summary ===")
    print(f"当前Watched Tokens：{total}")
    print(f"GMGN Token Info成功：{successful} / {total} ({_pct(successful, total)})")
    print(f"GMGN Token Info失败：{probe_errors} / {total} ({_pct(probe_errors, total)})")
    print(f"识别到Project X：{project_x} / {total} ({_pct(project_x, total)})")
    print(f"识别到DEV Wallet：{dev_wallet} / {total} ({_pct(dev_wallet, total)})")
    print(f"识别到DEV X：{dev_x} / {total} ({_pct(dev_x, total)})")
    print(f"Project X = DEV X有明确证据：{project_and_dev_x}")
    print(f"只有Project X但DEV X未知：{project_only}")


def _extract_items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("items", "list", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_items(value)
            if nested:
                return nested
    return []


def _pct(value: int, total: int) -> str:
    if total <= 0:
        return "0%"
    return f"{value / total * 100:.1f}%"


def _project_x_equals_dev_x(resolution: SocialIdentityResolution) -> bool:
    project_values = {_identity_username(candidate.value) for candidate in resolution.by_type("project_x")}
    dev_values = {_identity_username(candidate.value) for candidate in resolution.by_type("dev_x")}
    return bool(project_values & dev_values)


def _identity_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


if __name__ == "__main__":
    asyncio.run(main())
