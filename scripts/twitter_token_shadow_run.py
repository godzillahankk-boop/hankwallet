from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import init_db, make_engine, make_session_factory  # noqa: E402
from app.services.twitter_token_shadow import (  # noqa: E402
    DEFAULT_TOKEN_SHADOW_PAUSE_STATE_PATH,
    DEFAULT_TOKEN_SHADOW_SHARD_STATE_PATH,
    TOKEN_SHADOW_RULE_TAG_PREFIX,
    TokenFirstRealtimeShadow,
    TwitterTokenShadowRuleManager,
)
from app.services.twitterapi_io_client import TwitterApiIoClient  # noqa: E402
from app.services.twitterapi_io_social_ingestion import SOCIAL_RULE_TAG, SOCIAL_RULE_TAG_PREFIX, TWITTERAPI_IO_WS_URL  # noqa: E402
from scripts.twitter_token_shadow_rule_control import pause_token_shadow_rules  # noqa: E402

TOKEN_IDENTITY_PROBE_PREFIX = "wallet-agent-token-identity-probe-"
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "twitter_token_shadow"


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Run isolated Token-first TwitterAPI.io Social Shadow.")
    parser.add_argument("--duration", type=int, default=1800)
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()

    settings = load_settings()
    api_key = settings.twitterapi_io_api_key or os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        raise SystemExit("TWITTERAPI_IO_API_KEY missing")
    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    client = TwitterApiIoClient(api_key)
    summary = run_token_shadow(
        client,
        api_key=api_key,
        session_factory=session_factory,
        duration_seconds=max(1, args.duration),
        interval_seconds=max(60, args.interval),
        ws_url=args.ws_url,
        output_root=Path(args.output_root),
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


def run_token_shadow(
    client: TwitterApiIoClient,
    *,
    api_key: str,
    session_factory,
    duration_seconds: int,
    interval_seconds: int,
    ws_url: str = TWITTERAPI_IO_WS_URL,
    output_root: Path | None = DEFAULT_OUTPUT_ROOT,
    websocket_runner: Callable[..., None] | None = None,
    shard_state_path: Path | str | None = ROOT / DEFAULT_TOKEN_SHADOW_SHARD_STATE_PATH,
    pause_state_path: Path | str = ROOT / DEFAULT_TOKEN_SHADOW_PAUSE_STATE_PATH,
) -> dict[str, Any]:
    started = time.monotonic()
    runtime = {
        "duration_seconds": 0.0,
        "aborted": False,
        "abort_reason": None,
        "websocket_connects": 0,
        "provider_errors": 0,
    }
    preflight = active_blocking_rule_summaries(client)
    if preflight["active_formal_social_rules"]:
        summary = _summary(
            runtime=runtime | {"aborted": True, "abort_reason": "ACTIVE_FORMAL_SOCIAL_RULES_DETECTED"},
            rules={},
            shadow_snapshot={},
            cleanup=_cleanup_summary(cleanup_verified=False),
            started=started,
        )
        return _write_summary(summary, output_root)
    if preflight["active_token_identity_probe_rules"]:
        summary = _summary(
            runtime=runtime | {"aborted": True, "abort_reason": "ACTIVE_TOKEN_IDENTITY_PROBE_RULES_DETECTED"},
            rules={},
            shadow_snapshot={},
            cleanup=_cleanup_summary(cleanup_verified=False),
            started=started,
        )
        return _write_summary(summary, output_root)
    if preflight["active_foreign_rules"]:
        summary = _summary(
            runtime=runtime | {"aborted": True, "abort_reason": "ACTIVE_FOREIGN_RULES_DETECTED"},
            rules={},
            shadow_snapshot={},
            cleanup=_cleanup_summary(cleanup_verified=False),
            started=started,
        )
        return _write_summary(summary, output_root)

    shadow = TokenFirstRealtimeShadow(session_factory=session_factory)
    desired = shadow.desired_rules()
    manager = TwitterTokenShadowRuleManager(
        client,
        interval_seconds=interval_seconds,
        shard_state_path=shard_state_path,
    )
    reconcile = None
    cleanup: dict[str, Any] = {}
    runner_error: Exception | None = None
    try:
        reconcile = asyncio.run(manager.reconcile_rules(desired, stats=shadow.stats))
        if reconcile.provider_changed and reconcile.provider_changed_at:
            shadow.last_rule_changed_at = reconcile.provider_changed_at
        runner = websocket_runner or run_websocket
        runtime["websocket_connects"] += 1
        runner(ws_url, api_key, shadow=shadow, duration_seconds=duration_seconds)
    except Exception as exc:  # noqa: BLE001 - summary and cleanup must still run.
        runtime["provider_errors"] += 1
        shadow.stats.provider_errors += 1
        runner_error = exc
    finally:
        cleanup = pause_and_verify_token_shadow_rules(client, state_path=Path(pause_state_path))
    rules = {
        "watched_token_count": len(desired.watched_tokens),
        "ca_term_count": len(desired.ca_terms),
        "cashtag_term_count": len(desired.cashtag_terms),
        "ca_shard_count": reconcile.ca_shard_count if reconcile else 0,
        "cashtag_shard_count": reconcile.cashtag_shard_count if reconcile else 0,
        "rule_creates": reconcile.created_count if reconcile else 0,
        "rule_updates": reconcile.updated_count if reconcile else 0,
        "rule_deactivations": reconcile.deactivated_count if reconcile else 0,
        "rule_unchanged": reconcile.unchanged_count if reconcile else 0,
        "provider_changed": bool(reconcile and reconcile.provider_changed),
        "provider_changed_at": reconcile.provider_changed_at.isoformat() if reconcile and reconcile.provider_changed_at else None,
    }
    runtime["duration_seconds"] = round(time.monotonic() - started, 3)
    if runner_error:
        runtime["error"] = _safe_error(runner_error, api_key)
    summary = _summary(
        runtime=runtime,
        rules=rules,
        shadow_snapshot=shadow.stats_snapshot(),
        cleanup=cleanup,
        started=started,
    )
    return _write_summary(summary, output_root)


def run_websocket(ws_url: str, api_key: str, *, shadow: TokenFirstRealtimeShadow, duration_seconds: int) -> None:
    try:
        import websocket
    except ImportError as exc:
        raise RuntimeError("websocket-client dependency is required for token shadow runner") from exc
    ws = websocket.create_connection(ws_url, header=[f"x-api-key: {api_key}"], timeout=10)
    deadline = time.monotonic() + duration_seconds
    try:
        while time.monotonic() < deadline:
            try:
                message = ws.recv()
            except (TimeoutError, websocket.WebSocketTimeoutException):
                continue
            if message:
                shadow.handle_message(message)
    finally:
        try:
            ws.close()
        except Exception:
            pass


def active_blocking_rule_summaries(client: TwitterApiIoClient) -> dict[str, list[dict[str, Any]]]:
    formal = []
    identity_probe = []
    foreign = []
    for rule in _extract_rules(client.get_filter_rules()):
        if not _is_effect(rule):
            continue
        tag = str(rule.get("tag") or "")
        if tag == SOCIAL_RULE_TAG or tag.startswith(SOCIAL_RULE_TAG_PREFIX):
            formal.append(_public_rule_summary(rule))
        elif tag.startswith(TOKEN_IDENTITY_PROBE_PREFIX):
            identity_probe.append(_public_rule_summary(rule))
        elif tag.startswith(TOKEN_SHADOW_RULE_TAG_PREFIX):
            continue
        else:
            foreign.append(_public_rule_summary(rule))
    return {
        "active_formal_social_rules": formal,
        "active_token_identity_probe_rules": identity_probe,
        "active_foreign_rules": foreign,
    }


def pause_and_verify_token_shadow_rules(client: TwitterApiIoClient, *, state_path: Path) -> dict[str, Any]:
    pause_result: dict[str, Any]
    pause_error = None
    try:
        pause_result = pause_token_shadow_rules(client, state_path=state_path)
    except Exception as exc:  # noqa: BLE001 - runner must still return a cleanup summary.
        pause_result = {"cleanup_failed": True, "errors": []}
        pause_error = _safe_error(exc, os.getenv("TWITTERAPI_IO_API_KEY", ""))
    active_after = []
    verification_error = None
    try:
        active_after = [
            _public_rule_summary(rule)
            for rule in _extract_rules(client.get_filter_rules())
            if str(rule.get("tag") or "").startswith(TOKEN_SHADOW_RULE_TAG_PREFIX) and _is_effect(rule)
        ]
    except Exception as exc:  # noqa: BLE001 - cleanup verification must be visible in summary.
        verification_error = _safe_error(exc, os.getenv("TWITTERAPI_IO_API_KEY", ""))
    cleanup_verified = not active_after and not verification_error
    cleanup_errors = list(pause_result.get("errors", []))
    if pause_error:
        cleanup_errors.append({"error": pause_error})
    cleanup_failed = bool(pause_result.get("cleanup_failed")) or bool(verification_error) or bool(pause_error)
    return {
        "cleanup_failed": cleanup_failed,
        "cleanup_errors": cleanup_errors,
        "cleanup_verified": cleanup_verified,
        "cleanup_verification_error": verification_error,
        "active_token_shadow_rules_after_cleanup": active_after,
        "safe_to_stop_monitoring": cleanup_verified and not cleanup_failed and not active_after,
    }


def _summary(
    *,
    runtime: dict[str, Any],
    rules: dict[str, Any],
    shadow_snapshot: dict[str, Any],
    cleanup: dict[str, Any],
    started: float,
) -> dict[str, Any]:
    runtime = dict(runtime)
    runtime["duration_seconds"] = runtime.get("duration_seconds") or round(time.monotonic() - started, 3)
    snapshot = {
        "runtime": runtime,
        "rules": rules,
        "delivery": {
            key: shadow_snapshot.get(key, 0)
            for key in ("provider_messages", "tweet_deliveries", "unique_tweets", "duplicates", "duplicate_ratio")
        },
        "warmup": {"startup_warmup_deliveries": shadow_snapshot.get("startup_warmup_deliveries", 0)},
        "steady": {
            key: shadow_snapshot.get(key, 0)
            for key in ("steady_deliveries", "steady_unique", "steady_duplicates", "steady_duplicate_ratio")
        },
        "matching": {
            key: shadow_snapshot.get(key, 0)
            for key in (
                "matched_tweet_hits",
                "token_match_results",
                "exact_ca_matches",
                "exact_ca_and_cashtag_matches",
                "cashtag_matches",
                "ambiguous_symbol_hits",
                "unqualified_author_hits",
                "unmatched_hits",
                "token_match_rate",
            )
        },
        "qualified_kol": {
            key: shadow_snapshot.get(key, 0)
            for key in (
                "qualified_kol_tweet_hits",
                "distinct_qualified_kol_authors",
                "token_kol_pairs",
                "tokens_with_kol_hits",
                "qualified_kol_match_rate",
            )
        },
        "steady_efficiency": {
            key: shadow_snapshot.get(key, 0)
            for key in (
                "steady_matched_tweet_hits",
                "steady_qualified_kol_tweet_hits",
                "steady_token_match_rate",
                "steady_qualified_kol_match_rate",
            )
        },
        "cleanup": cleanup,
    }
    return snapshot


def _cleanup_summary(*, cleanup_verified: bool) -> dict[str, Any]:
    return {
        "cleanup_failed": False,
        "cleanup_errors": [],
        "cleanup_verified": cleanup_verified,
        "cleanup_verification_error": None,
        "active_token_shadow_rules_after_cleanup": [],
        "safe_to_stop_monitoring": cleanup_verified,
    }


def _write_summary(summary: dict[str, Any], output_root: Path | None) -> dict[str, Any]:
    if output_root is None:
        return summary
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "summary.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    summary["output_path"] = str(path)
    return summary


def _extract_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("rules"), list):
        return [rule for rule in nested["rules"] if isinstance(rule, dict)]
    return []


def _public_rule_summary(rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "tag": str(rule.get("tag") or ""),
        "rule_id": _rule_id(rule),
        "active": _is_effect(rule),
        "interval_seconds": _interval_seconds(rule),
        "value_length": len(str(rule.get("value") or "")),
    }


def _rule_id(rule: dict[str, Any]) -> str:
    for key in ("rule_id", "ruleId", "id"):
        value = rule.get(key)
        if value:
            return str(value)
    return ""


def _interval_seconds(rule: dict[str, Any]) -> int:
    value = rule.get("interval_seconds", rule.get("intervalSeconds", 60))
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 60


def _is_effect(rule: dict[str, Any]) -> bool:
    value = rule.get("is_effect")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _safe_error(exc: Exception, api_key: str) -> str:
    message = str(exc)
    if api_key:
        message = message.replace(api_key, "***")
    return message[:300]


if __name__ == "__main__":
    main()
