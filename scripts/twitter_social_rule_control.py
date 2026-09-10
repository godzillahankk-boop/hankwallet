from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.twitterapi_io_client import TwitterApiIoClient  # noqa: E402
from app.services.twitterapi_io_social_ingestion import SOCIAL_RULE_TAG, SOCIAL_RULE_TAG_PREFIX  # noqa: E402

DEFAULT_PAUSE_STATE_PATH = ROOT / "data" / "twitter_social_paused_rules.json"

HELP_EPILOG = """
Stopping Wallet Agent or setting SOCIAL_X_ENABLED=false does not stop provider-side
Tweet Filter billing while TwitterAPI.io rules remain active.

To temporarily disable Social safely:
  1. .venv/bin/python scripts/twitter_social_rule_control.py pause
  2. keep SOCIAL_X_ENABLED=false
  3. start Wallet Agent normally
"""


class RuleControlError(RuntimeError):
    pass


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(
        description="Manage Wallet Agent formal TwitterAPI.io Social filter rules.",
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", choices=("status", "pause", "resume"))
    args = parser.parse_args()

    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        raise SystemExit("TWITTERAPI_IO_API_KEY missing")
    client = TwitterApiIoClient(api_key)

    try:
        if args.command == "status":
            result = status_formal_rules(client)
        elif args.command == "pause":
            result = pause_formal_rules(client)
        else:
            result = resume_formal_rules(client)
    except RuleControlError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, indent=2, sort_keys=True))


def status_formal_rules(client: TwitterApiIoClient) -> dict[str, Any]:
    rules = formal_rule_summaries(client.get_filter_rules())
    return {"command": "status", "rules": rules, "updated": []}


def pause_formal_rules(
    client: TwitterApiIoClient,
    *,
    state_path: Path = DEFAULT_PAUSE_STATE_PATH,
) -> dict[str, Any]:
    rules = [_normalize_rule(rule) for rule in _extract_rules(client.get_filter_rules()) if _is_formal_rule(rule)]
    updated: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    paused_at = datetime.now(UTC).isoformat()
    state = _load_pause_state(state_path)
    paused_by_id = {str(row.get("rule_id")): row for row in state.get("paused_rules", []) if row.get("rule_id")}
    for rule in rules:
        if not rule["active"]:
            continue
        try:
            client.update_filter_rule(
                rule_id=rule["rule_id"],
                tag=rule["tag"],
                value=rule["value"],
                interval_seconds=rule["interval_seconds"],
                is_effect=False,
            )
        except Exception as exc:  # noqa: BLE001 - pause must continue across independent formal rules.
            errors.append({"rule_id": rule["rule_id"], "tag": rule["tag"], "error": _safe_error(exc)})
            continue
        rule["active"] = False
        updated.append(_public_summary(rule))
        paused_by_id[rule["rule_id"]] = {
            "rule_id": rule["rule_id"],
            "tag": rule["tag"],
            "paused_at": paused_at,
        }
        _save_pause_state(state_path, list(paused_by_id.values()))
    return {
        "command": "pause",
        "rules": [_public_summary(rule) for rule in rules],
        "updated": updated,
        "errors": errors,
        "cleanup_failed": bool(errors),
    }


def resume_formal_rules(
    client: TwitterApiIoClient,
    *,
    state_path: Path = DEFAULT_PAUSE_STATE_PATH,
) -> dict[str, Any]:
    state = _load_pause_state(state_path)
    paused_rules = [row for row in state.get("paused_rules", []) if row.get("rule_id")]
    if not paused_rules:
        raise RuleControlError(f"No paused formal rule state found at {state_path}")
    current_rules = {
        rule["rule_id"]: rule
        for rule in (_normalize_rule(rule) for rule in _extract_rules(client.get_filter_rules()) if _is_formal_rule(rule))
        if rule["rule_id"]
    }
    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for paused in paused_rules:
        rule_id = str(paused["rule_id"])
        rule = current_rules.get(rule_id)
        if rule is None:
            skipped.append({"rule_id": rule_id, "tag": str(paused.get("tag") or ""), "reason": "missing_provider_rule"})
            continue
        if rule["active"]:
            skipped.append({"rule_id": rule_id, "tag": rule["tag"], "reason": "already_active"})
            continue
        client.update_filter_rule(
            rule_id=rule["rule_id"],
            tag=rule["tag"],
            value=rule["value"],
            interval_seconds=rule["interval_seconds"],
            is_effect=True,
        )
        rule["active"] = True
        updated.append(_public_summary(rule))
    _clear_pause_state(state_path)
    return {"command": "resume", "rules": [_public_summary(rule) for rule in current_rules.values()], "updated": updated, "skipped": skipped}


def _set_formal_rules_effect(client: TwitterApiIoClient, *, is_effect: bool, command: str) -> dict[str, Any]:
    rules = [_normalize_rule(rule) for rule in _extract_rules(client.get_filter_rules()) if _is_formal_rule(rule)]
    updated: list[dict[str, Any]] = []
    for rule in rules:
        if rule["active"] is is_effect:
            continue
        client.update_filter_rule(
            rule_id=rule["rule_id"],
            tag=rule["tag"],
            value=rule["value"],
            interval_seconds=rule["interval_seconds"],
            is_effect=is_effect,
        )
        rule["active"] = is_effect
        updated.append(_public_summary(rule))
    return {"command": command, "rules": [_public_summary(rule) for rule in rules], "updated": updated}


def formal_rule_summaries(data: dict[str, Any]) -> list[dict[str, Any]]:
    return [_public_summary(_normalize_rule(rule)) for rule in _extract_rules(data) if _is_formal_rule(rule)]


def _load_pause_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"paused_rules": []}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise RuleControlError(f"Malformed pause state file: {path}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("paused_rules"), list):
        raise RuleControlError(f"Malformed pause state file: {path}")
    return data


def _save_pause_state(path: Path, paused_rules: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "rule_id": str(row["rule_id"]),
            "tag": str(row["tag"]),
            "paused_at": str(row["paused_at"]),
        }
        for row in paused_rules
        if row.get("rule_id") and row.get("tag")
    ]
    path.write_text(json.dumps({"paused_rules": rows}, indent=2, sort_keys=True))


def _clear_pause_state(path: Path) -> None:
    if path.exists():
        path.unlink()


def _normalize_rule(rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "rule_id": _rule_id(rule),
        "tag": str(rule.get("tag") or ""),
        "value": str(rule.get("value") or ""),
        "interval_seconds": _interval_seconds(rule),
        "active": _is_effect(rule),
    }


def _public_summary(rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "tag": rule["tag"],
        "rule_id": rule["rule_id"],
        "active": rule["active"],
        "interval_seconds": rule["interval_seconds"],
        "value_length": len(rule["value"]),
    }


def _is_formal_rule(rule: dict[str, Any]) -> bool:
    tag = str(rule.get("tag") or "")
    return tag == SOCIAL_RULE_TAG or tag.startswith(SOCIAL_RULE_TAG_PREFIX)


def _extract_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("rules"), list):
        return [rule for rule in nested["rules"] if isinstance(rule, dict)]
    return []


def _rule_id(data: dict[str, Any]) -> str:
    for key in ("rule_id", "ruleId", "id"):
        value = data.get(key)
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


def _safe_error(exc: Exception) -> str:
    secret = os.getenv("TWITTERAPI_IO_API_KEY", "")
    message = str(exc)
    if secret:
        message = message.replace(secret, "***")
    return message[:300]


if __name__ == "__main__":
    main()
