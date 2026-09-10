from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.twitterapi_io_client import NormalizedTweet, TwitterApiIoClient  # noqa: E402
from app.services.twitterapi_io_social_ingestion import (  # noqa: E402
    SOCIAL_RULE_TAG,
    SOCIAL_RULE_TAG_PREFIX,
    TWITTERAPI_IO_WS_URL,
    TwitterApiIoError,
    parse_stream_message,
)

PROBE_RULE_TAG = "wallet-agent-cost-probe-v01"
DEFAULT_DURATION_SECONDS = 600
DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_MAX_TRACKED_TWEETS = 1000


@dataclass
class TweetDeliveryHistory:
    tweet_id: str
    author_id: str | None
    created_at: datetime | None
    first_received_at: datetime
    last_received_at: datetime
    delivery_count: int = 1
    received_offsets: list[float] = field(default_factory=lambda: [0.0])

    def add(self, received_at: datetime, offset_seconds: float) -> None:
        self.last_received_at = received_at
        self.delivery_count += 1
        self.received_offsets.append(round(offset_seconds, 3))
        if len(self.received_offsets) > 20:
            self.received_offsets = self.received_offsets[-20:]


@dataclass
class FilterCostProbeStats:
    started_at: datetime
    probe_rule_tag: str = PROBE_RULE_TAG
    max_tracked_tweets: int = DEFAULT_MAX_TRACKED_TWEETS
    provider_messages: int = 0
    tweet_deliveries: int = 0
    duplicate_deliveries: int = 0
    foreign_rule_messages: int = 0
    foreign_rule_tweet_deliveries: int = 0
    payloads: list[dict[str, Any]] = field(default_factory=list)
    tweets: OrderedDict[str, TweetDeliveryHistory] = field(default_factory=OrderedDict)

    def record_payload(
        self,
        *,
        rule_id: str | None,
        rule_tag: str | None,
        tweets: list[NormalizedTweet],
        received_at: datetime,
    ) -> None:
        self.provider_messages += 1
        self.payloads.append(
            {
                "received_at": _iso(received_at),
                "rule_id": rule_id,
                "rule_tag": rule_tag,
                "batch_size": len(tweets),
            }
        )
        if len(self.payloads) > 200:
            self.payloads = self.payloads[-200:]
        for tweet in tweets:
            self.record_tweet(tweet, received_at=received_at)

    def record_tweet(self, tweet: NormalizedTweet, *, received_at: datetime) -> None:
        self.tweet_deliveries += 1
        offset = (_aware_utc(received_at) - _aware_utc(self.started_at)).total_seconds()
        existing = self.tweets.get(tweet.tweet_id)
        if existing:
            self.duplicate_deliveries += 1
            existing.add(received_at, offset)
            self.tweets.move_to_end(tweet.tweet_id)
            return
        self.tweets[tweet.tweet_id] = TweetDeliveryHistory(
            tweet_id=tweet.tweet_id,
            author_id=tweet.author_id,
            created_at=tweet.created_at,
            first_received_at=received_at,
            last_received_at=received_at,
            received_offsets=[round(offset, 3)],
        )
        while len(self.tweets) > self.max_tracked_tweets:
            self.tweets.popitem(last=False)

    def record_foreign_payload(self, *, tweets: list[NormalizedTweet]) -> None:
        self.foreign_rule_messages += 1
        self.foreign_rule_tweet_deliveries += len(tweets)

    def summary(
        self,
        *,
        duration_seconds: float,
        interval_seconds: float,
        formal_rules_active_at_start: bool = False,
        aborted: bool = False,
        abort_reason: str | None = None,
        active_formal_rules: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        histories = list(self.tweets.values())
        duplicate_histories = [history for history in histories if history.delivery_count > 1]
        repeat_intervals = [
            interval
            for history in duplicate_histories
            for interval in _repeat_intervals(history.received_offsets)
        ]
        suspected_periodic_replay = _is_periodic_replay(repeat_intervals, interval_seconds)
        if self.foreign_rule_messages:
            conclusion = "INCONCLUSIVE_NOT_ISOLATED"
        elif _is_low_traffic(self.tweet_deliveries, len(histories), repeat_intervals):
            conclusion = "INCONCLUSIVE_LOW_TRAFFIC"
        elif suspected_periodic_replay:
            conclusion = "FILTER_PERIODIC_REPLAY"
        else:
            conclusion = "NO_PERIODIC_REPLAY_OBSERVED"
        return {
            "probe_rule_tag": self.probe_rule_tag,
            "aborted": aborted,
            "abort_reason": abort_reason,
            "formal_rules_active_at_start": formal_rules_active_at_start,
            "active_formal_rules": active_formal_rules or [],
            "duration_seconds": round(duration_seconds, 3),
            "provider_messages": self.provider_messages,
            "tweet_deliveries": self.tweet_deliveries,
            "unique_tweet_ids": len(histories),
            "duplicate_deliveries": self.duplicate_deliveries,
            "duplicate_ratio": _ratio(self.duplicate_deliveries, self.tweet_deliveries),
            "foreign_rule_messages": self.foreign_rule_messages,
            "foreign_rule_tweet_deliveries": self.foreign_rule_tweet_deliveries,
            "repeat_interval_seconds": round(median(repeat_intervals), 3) if repeat_intervals else None,
            "suspected_periodic_replay": suspected_periodic_replay,
            "conclusion": conclusion,
            "tweets": [
                {
                    "tweet_id": history.tweet_id,
                    "author_id": history.author_id,
                    "created_at": _iso(history.created_at),
                    "first_received_at": _iso(history.first_received_at),
                    "last_received_at": _iso(history.last_received_at),
                    "delivery_count": history.delivery_count,
                    "received_offsets": history.received_offsets,
                }
                for history in histories
            ],
            "payloads": self.payloads,
        }


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Isolated TwitterAPI.io Tweet Filter cost/replay probe")
    parser.add_argument("--username", action="append", required=True, help="X username to monitor; pass once or twice")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--tag", default=PROBE_RULE_TAG)
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    args = parser.parse_args()

    usernames = _normalize_usernames(args.username)
    interval = max(60, args.interval)
    duration = max(1, args.duration)
    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        print("TWITTERAPI_IO_API_KEY missing; cost probe skipped.")
        return

    client = TwitterApiIoClient(api_key)
    rule_value = " OR ".join(f"from:{username}" for username in usernames)
    summary = run_filter_cost_probe(
        client,
        api_key=api_key,
        rule_value=rule_value,
        tag=args.tag,
        interval_seconds=interval,
        duration_seconds=duration,
        ws_url=args.ws_url,
    )
    if summary.get("aborted"):
        print("ACTIVE_FORMAL_RULES_DETECTED")
        for rule in summary["active_formal_rules"]:
            print(
                "formal_rule "
                f"tag={rule['tag']} "
                f"rule_id={rule['rule_id']} "
                f"interval_seconds={rule['interval_seconds']}"
            )
        print("Run: .venv/bin/python scripts/twitter_social_rule_control.py pause")
    print(json.dumps(summary, indent=2, sort_keys=True))


def run_filter_cost_probe(
    client: TwitterApiIoClient,
    *,
    api_key: str,
    rule_value: str,
    tag: str = PROBE_RULE_TAG,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    duration_seconds: int = DEFAULT_DURATION_SECONDS,
    ws_url: str = TWITTERAPI_IO_WS_URL,
    websocket_runner: Callable[..., None] | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    started = time.monotonic()
    stats = FilterCostProbeStats(started_at=started_at, probe_rule_tag=tag)
    active_formal_rules = active_formal_rule_summaries(client)
    if active_formal_rules:
        return stats.summary(
            duration_seconds=time.monotonic() - started,
            interval_seconds=interval_seconds,
            formal_rules_active_at_start=True,
            aborted=True,
            abort_reason="ACTIVE_FORMAL_RULES_DETECTED",
            active_formal_rules=active_formal_rules,
        )
    rule = None
    runner = websocket_runner or run_websocket_probe
    try:
        rule = ensure_probe_rule(client, tag=tag, value=rule_value, interval_seconds=interval_seconds)
        runner(
            ws_url,
            api_key,
            stats=stats,
            duration_seconds=duration_seconds,
            expected_rule_tag=tag,
        )
    finally:
        if rule:
            deactivate_probe_rule(client, rule=rule, value=rule_value, interval_seconds=interval_seconds)
    return stats.summary(
        duration_seconds=time.monotonic() - started,
        interval_seconds=interval_seconds,
        formal_rules_active_at_start=False,
    )


def ensure_probe_rule(client: TwitterApiIoClient, *, tag: str, value: str, interval_seconds: int) -> dict[str, Any]:
    rules = _extract_rules(client.get_filter_rules())
    existing = next((rule for rule in rules if str(rule.get("tag")) == tag), None)
    if existing is None:
        created = client.add_filter_rule(tag=tag, value=value, interval_seconds=interval_seconds)
        rule_id = _rule_id(created)
        if not rule_id:
            raise TwitterApiIoError("probe add_rule did not return rule_id")
        existing = {"rule_id": rule_id, "tag": tag, "value": value, "is_effect": 0}
    rule_id = _rule_id(existing)
    if not rule_id:
        raise TwitterApiIoError("probe rule missing rule_id")
    if existing.get("value") != value or not _is_effect(existing) or int(float(existing.get("interval_seconds") or interval_seconds)) != interval_seconds:
        client.update_filter_rule(
            rule_id=rule_id,
            tag=tag,
            value=value,
            interval_seconds=interval_seconds,
            is_effect=True,
        )
    return {"rule_id": rule_id, "tag": tag}


def deactivate_probe_rule(
    client: TwitterApiIoClient,
    *,
    rule: dict[str, Any],
    value: str,
    interval_seconds: int,
) -> None:
    client.update_filter_rule(
        rule_id=str(rule["rule_id"]),
        tag=str(rule["tag"]),
        value=value,
        interval_seconds=interval_seconds,
        is_effect=False,
    )


def run_websocket_probe(
    ws_url: str,
    api_key: str,
    *,
    stats: FilterCostProbeStats,
    duration_seconds: int,
    expected_rule_tag: str = PROBE_RULE_TAG,
) -> None:
    try:
        import websocket
    except ImportError as exc:
        raise TwitterApiIoError("websocket-client dependency is required for filter cost probe") from exc
    ws = websocket.create_connection(ws_url, header=[f"x-api-key: {api_key}"], timeout=10)
    deadline = time.monotonic() + duration_seconds
    try:
        while time.monotonic() < deadline:
            try:
                message = ws.recv()
            except (TimeoutError, websocket.WebSocketTimeoutException):
                continue
            received_at = datetime.now(UTC)
            record_probe_stream_message(
                message,
                stats=stats,
                received_at=received_at,
                expected_rule_tag=expected_rule_tag,
            )
    finally:
        try:
            ws.close()
        except Exception:
            pass


def record_probe_stream_message(
    message: str | bytes,
    *,
    stats: FilterCostProbeStats,
    received_at: datetime,
    expected_rule_tag: str = PROBE_RULE_TAG,
) -> None:
    parsed = parse_stream_message(message, received_at=received_at)
    if parsed.event_type != "tweet":
        return
    if parsed.rule_tag != expected_rule_tag:
        stats.record_foreign_payload(tweets=parsed.tweets)
        return
    stats.record_payload(
        rule_id=parsed.rule_id,
        rule_tag=parsed.rule_tag,
        tweets=parsed.tweets,
        received_at=received_at,
    )


def _normalize_usernames(usernames: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for username in usernames:
        clean = username.strip().lstrip("@")
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(clean)
    if not 1 <= len(normalized) <= 2:
        raise SystemExit("--username requires one or two unique usernames")
    return normalized


def _extract_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("rules"), list):
        return [rule for rule in nested["rules"] if isinstance(rule, dict)]
    return []


def active_formal_rule_summaries(client: TwitterApiIoClient) -> list[dict[str, Any]]:
    return [
        _rule_summary(rule)
        for rule in _extract_rules(client.get_filter_rules())
        if _is_formal_social_rule(rule) and _is_effect(rule)
    ]


def _rule_summary(rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "tag": str(rule.get("tag") or ""),
        "rule_id": _rule_id(rule),
        "active": _is_effect(rule),
        "interval_seconds": _interval_seconds(rule),
        "value_length": len(str(rule.get("value") or "")),
    }


def _is_formal_social_rule(rule: dict[str, Any]) -> bool:
    tag = str(rule.get("tag") or "")
    return tag == SOCIAL_RULE_TAG or tag.startswith(SOCIAL_RULE_TAG_PREFIX)


def _rule_id(data: dict[str, Any]) -> str | None:
    for key in ("rule_id", "ruleId", "id"):
        value = data.get(key)
        if value:
            return str(value)
    return None


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


def _repeat_intervals(offsets: list[float]) -> list[float]:
    return [round(offsets[index] - offsets[index - 1], 3) for index in range(1, len(offsets))]


def _is_periodic_replay(intervals: list[float], interval_seconds: float) -> bool:
    if not intervals:
        return False
    tolerance = max(5.0, interval_seconds * 0.25)
    matching = [interval for interval in intervals if abs(interval - interval_seconds) <= tolerance]
    return len(matching) >= max(1, len(intervals) // 2)


def _is_low_traffic(tweet_deliveries: int, unique_tweet_ids: int, repeat_intervals: list[float]) -> bool:
    if tweet_deliveries < 3:
        return True
    if not repeat_intervals and unique_tweet_ids < 3:
        return True
    return False


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _safe_text(text: str, secret: str) -> str:
    return text.replace(secret, "***")[:500]


if __name__ == "__main__":
    main()
