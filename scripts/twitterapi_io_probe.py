from __future__ import annotations

import argparse
import json
import os
import time
import statistics
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.twitterapi_io_client import (  # noqa: E402
    DEFAULT_BASE_URL,
    NormalizedTweet,
    TwitterApiIoClient,
    TwitterApiIoError,
    normalize_tweets_from_response,
)

DEFAULT_WS_URL = "wss://ws.twitterapi.io/twitter/tweet/websocket"
DEFAULT_STREAM_TAG = "wallet-agent-probe-v01"
DEFAULT_STREAM_INTERVAL_SECONDS = 60.0
DEFAULT_RECONNECT_COOLDOWN_SECONDS = 90.0


@dataclass
class ProbeStats:
    api_calls_advanced_search: int = 0
    api_calls_tweet_filter: int = 0
    provider_errors: list[str] = field(default_factory=list)
    seen_tweet_ids: set[str] = field(default_factory=set)
    tweets_received: int = 0
    unique_tweets: int = 0
    duplicate_tweets: int = 0
    originals: int = 0
    replies: int = 0
    quotes: int = 0
    retweets: int = 0
    tweets_with_ca: int = 0
    tweets_with_cashtag: int = 0
    tweets_with_quote_token: int = 0
    unmatched_tweets: int = 0
    delays: list[float] = field(default_factory=list)
    snow_delays_ms: list[float] = field(default_factory=list)
    event_types: dict[str, int] = field(default_factory=dict)
    rule_ids: set[str] = field(default_factory=set)
    rule_tags: set[str] = field(default_factory=set)
    disconnects: list[str] = field(default_factory=list)

    def add_tweet(self, tweet: NormalizedTweet) -> bool:
        self.tweets_received += 1
        if tweet.tweet_id in self.seen_tweet_ids:
            self.duplicate_tweets += 1
            return False
        self.seen_tweet_ids.add(tweet.tweet_id)
        self.unique_tweets += 1
        if tweet.post_type == "reply":
            self.replies += 1
        elif tweet.post_type == "quote":
            self.quotes += 1
        elif tweet.post_type == "retweet":
            self.retweets += 1
        else:
            self.originals += 1
        match_types = {match.match_type for match in tweet.token_matches}
        if any(match_type.endswith("_ca") for match_type in match_types):
            self.tweets_with_ca += 1
        if any(match_type.endswith("_cashtag") for match_type in match_types):
            self.tweets_with_cashtag += 1
        if any(match_type.startswith("quote_") for match_type in match_types):
            self.tweets_with_quote_token += 1
        if not tweet.token_matches:
            self.unmatched_tweets += 1
        delay = tweet.delay_seconds()
        if delay is not None:
            self.delays.append(delay)
        return True

    def print_summary(self) -> None:
        print("\n=== Probe Summary ===")
        print(f"Tweets received: {self.tweets_received}")
        print(f"Unique tweets: {self.unique_tweets}")
        print(f"Duplicates: {self.duplicate_tweets}")
        print(f"Original: {self.originals}")
        print(f"Replies: {self.replies}")
        print(f"Quotes: {self.quotes}")
        print(f"Retweets: {self.retweets}")
        print(f"Tweets with CA: {self.tweets_with_ca}")
        print(f"Tweets with Cashtag: {self.tweets_with_cashtag}")
        print(f"Tweets with Quote Token: {self.tweets_with_quote_token}")
        print(f"Unmatched Tweets: {self.unmatched_tweets}")
        if self.delays:
            sorted_delays = sorted(self.delays)
            print(f"Average Delay: {statistics.mean(sorted_delays):.1f}s")
            print(f"P50 Delay: {_percentile(sorted_delays, 50):.1f}s")
            print(f"P95 Delay: {_percentile(sorted_delays, 95):.1f}s")
            print(f"Max Delay: {max(sorted_delays):.1f}s")
        else:
            print("Average Delay: UNKNOWN")
            print("P50 Delay: UNKNOWN")
            print("P95 Delay: UNKNOWN")
            print("Max Delay: UNKNOWN")
        print(f"Advanced Search API calls: {self.api_calls_advanced_search}")
        print(f"Tweet Filter API calls: {self.api_calls_tweet_filter}")
        print(f"Provider errors: {len(self.provider_errors)}")
        print(f"Event types: {self.event_types or 'UNKNOWN'}")
        print(f"Rule IDs: {sorted(self.rule_ids) if self.rule_ids else 'UNKNOWN'}")
        print(f"Rule tags: {sorted(self.rule_tags) if self.rule_tags else 'UNKNOWN'}")
        if self.snow_delays_ms:
            print(f"Average snow_delay_ms: {statistics.mean(self.snow_delays_ms):.1f}")
        else:
            print("Average snow_delay_ms: UNKNOWN")
        if self.disconnects:
            print("Disconnects:")
            for item in self.disconnects:
                print(f"- {item}")
        else:
            print("Disconnects: 0")
        print("Credits / cost: UNKNOWN unless read from TwitterAPI.io Dashboard")


@dataclass(frozen=True)
class StreamRule:
    rule_id: str
    tag: str
    value: str
    interval_seconds: float


@dataclass(frozen=True)
class StreamMessage:
    event_type: str | None
    rule_id: str | None
    rule_tag: str | None
    tweets: list[NormalizedTweet]
    snow_delay_ms: float | None


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="TwitterAPI.io Social Probe")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--debug-raw", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Run one Advanced Search query")
    search.add_argument("--query", required=True)
    search.add_argument("--query-type", default="Latest", choices=["Latest", "Top"])
    search.add_argument("--cursor", default="")

    search_kols = subparsers.add_parser("search-kols", help="Run Advanced Search for configured KOL usernames")
    search_kols.add_argument("--config", default=str(ROOT / "config" / "social_probe_kols.json"))
    search_kols.add_argument("--query-type", default="Latest", choices=["Latest", "Top"])

    subparsers.add_parser("rules", help="Get Tweet Filter rules")

    add_rule = subparsers.add_parser("add-rule", help="Add one inactive Tweet Filter rule")
    add_rule.add_argument("--tag", required=True)
    add_rule.add_argument("--value", required=True)
    add_rule.add_argument("--interval-seconds", type=float, default=60)

    activate = subparsers.add_parser("activate-rule", help="Update one Tweet Filter rule and set is_effect=1")
    activate.add_argument("--rule-id", required=True)
    activate.add_argument("--tag", required=True)
    activate.add_argument("--value", required=True)
    activate.add_argument("--interval-seconds", type=float, default=60)
    activate.add_argument("--inactive", action="store_true")

    stream = subparsers.add_parser("stream", help="Run a finite Tweet Filter WebSocket probe")
    stream.add_argument("--duration-minutes", type=float, default=5)
    stream.add_argument("--config", default=str(ROOT / "config" / "social_probe_kols.json"))
    stream.add_argument("--tag", default=DEFAULT_STREAM_TAG)
    stream.add_argument("--interval-seconds", type=float, default=DEFAULT_STREAM_INTERVAL_SECONDS)
    stream.add_argument("--ws-url", default=DEFAULT_WS_URL)
    stream.add_argument("--max-reconnects", type=int, default=1)
    stream.add_argument("--reconnect-cooldown-seconds", type=float, default=DEFAULT_RECONNECT_COOLDOWN_SECONDS)

    args = parser.parse_args()
    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    client = TwitterApiIoClient(
        api_key,
        base_url=args.base_url,
        timeout_seconds=args.timeout,
        max_retries=1,
    )
    stats = ProbeStats()
    try:
        if args.command == "search":
            _run_search(client, args.query, args.query_type, args.cursor, args.debug_raw, stats)
        elif args.command == "search-kols":
            for username in _load_usernames(Path(args.config)):
                _run_search(client, f"from:{username}", args.query_type, "", args.debug_raw, stats)
        elif args.command == "rules":
            data = client.get_filter_rules()
            stats.api_calls_tweet_filter += 1
            _print_rule_summary(data, args.debug_raw)
        elif args.command == "add-rule":
            data = client.add_filter_rule(tag=args.tag, value=args.value, interval_seconds=args.interval_seconds)
            stats.api_calls_tweet_filter += 1
            _print_provider_status(data, args.debug_raw)
        elif args.command == "activate-rule":
            data = client.update_filter_rule(
                rule_id=args.rule_id,
                tag=args.tag,
                value=args.value,
                interval_seconds=args.interval_seconds,
                is_effect=not args.inactive,
            )
            stats.api_calls_tweet_filter += 1
            _print_provider_status(data, args.debug_raw)
        elif args.command == "stream":
            usernames = _load_usernames(Path(args.config))
            rule_value = _rule_value_for_usernames(usernames)
            rule = _ensure_stream_rule(client, args.tag, rule_value, args.interval_seconds, stats)
            try:
                _run_stream(
                    api_key=api_key,
                    ws_url=args.ws_url,
                    duration_minutes=args.duration_minutes,
                    max_reconnects=args.max_reconnects,
                    reconnect_cooldown_seconds=args.reconnect_cooldown_seconds,
                    debug_raw=args.debug_raw,
                    stats=stats,
                )
            finally:
                _set_rule_effective(client, rule, False, stats)
                print(f"Stream rule deactivated: rule_id={rule.rule_id} tag={rule.tag}")
    except KeyboardInterrupt:
        print("\nInterrupted.")
    except TwitterApiIoError as exc:
        stats.provider_errors.append(str(exc))
        print(f"Provider error: {exc}")
        raise SystemExit(1) from exc
    finally:
        stats.print_summary()


def _run_search(
    client: TwitterApiIoClient,
    query: str,
    query_type: str,
    cursor: str,
    debug_raw: bool,
    stats: ProbeStats,
) -> None:
    print(f"\n=== Advanced Search: {query} ===")
    data = client.advanced_search(query, query_type=query_type, cursor=cursor)
    stats.api_calls_advanced_search += 1
    print("HTTP status: 200")
    print(f"status/message: {data.get('status', 'success')}/{data.get('msg') or data.get('message') or ''}")
    tweets = normalize_tweets_from_response(data, detected_at=datetime.now(tz=UTC))
    print(f"tweets count: {len(tweets)}")
    if debug_raw:
        print(json.dumps(data, ensure_ascii=False, indent=2))
    for tweet in tweets:
        if stats.add_tweet(tweet):
            _print_tweet(tweet)


def _print_tweet(tweet: NormalizedTweet) -> None:
    print("-" * 50)
    print(f"Provider: {tweet.provider}")
    print(f"Author: @{tweet.author_username or 'UNKNOWN'}")
    print(f"Tweet ID: {tweet.tweet_id}")
    print(f"Posted: {_fmt_dt(tweet.created_at)}")
    print(f"Detected: {_fmt_dt(tweet.detected_at)}")
    delay = tweet.delay_seconds()
    print(f"Delay: {delay:.1f}s" if delay is not None else "Delay: UNKNOWN")
    print(f"Type: {tweet.post_type}")
    print("Text:")
    print(tweet.text or "")
    print("Token Matches:")
    if tweet.token_matches:
        for match in tweet.token_matches:
            print(f"{match.match_type}: {match.value}")
    else:
        print("none")
    if tweet.quoted_tweet:
        print("Quoted Tweet:")
        print(json.dumps(tweet.quoted_tweet, ensure_ascii=False))
    if tweet.retweeted_tweet:
        print("Retweeted Tweet:")
        print(json.dumps(tweet.retweeted_tweet, ensure_ascii=False))
    print("Engagement:")
    print(
        "likes={likes} reposts={reposts} replies={replies} quotes={quotes} views={views}".format(
            likes=_unknown(tweet.like_count),
            reposts=_unknown(tweet.retweet_count),
            replies=_unknown(tweet.reply_count),
            quotes=_unknown(tweet.quote_count),
            views=_unknown(tweet.view_count),
        )
    )
    print("-" * 50)


def parse_stream_message(message: str | bytes, *, received_at: datetime | None = None) -> StreamMessage | None:
    payload = _parse_json_message(message)
    if not isinstance(payload, dict):
        raise TwitterApiIoError("malformed websocket message")
    event_type = _str_or_none(_first(payload, "event_type", "eventType", "type"))
    if event_type and event_type != "tweet":
        return StreamMessage(
            event_type=event_type,
            rule_id=_str_or_none(_first(payload, "rule_id", "ruleId")),
            rule_tag=_str_or_none(_first(payload, "rule_tag", "ruleTag", "tag")),
            tweets=[],
            snow_delay_ms=_float_or_none(_first(payload, "snow_delay_ms", "snowDelayMs")),
        )
    tweets = _extract_stream_tweets(payload)
    detected = received_at or datetime.now(tz=UTC)
    return StreamMessage(
        event_type=event_type,
        rule_id=_str_or_none(_first(payload, "rule_id", "ruleId")),
        rule_tag=_str_or_none(_first(payload, "rule_tag", "ruleTag", "tag")),
        tweets=normalize_tweets_from_response({"tweets": tweets}, detected_at=detected),
        snow_delay_ms=_float_or_none(_first(payload, "snow_delay_ms", "snowDelayMs")),
    )


def handle_stream_message(message: str | bytes, *, stats: ProbeStats, debug_raw: bool = False) -> None:
    received_at = datetime.now(tz=UTC)
    try:
        parsed = parse_stream_message(message, received_at=received_at)
    except TwitterApiIoError as exc:
        stats.provider_errors.append(str(exc))
        print(f"Provider error: {exc}")
        return
    if parsed is None:
        return
    event_type = parsed.event_type or "tweet"
    stats.event_types[event_type] = stats.event_types.get(event_type, 0) + 1
    if parsed.rule_id:
        stats.rule_ids.add(parsed.rule_id)
    if parsed.rule_tag:
        stats.rule_tags.add(parsed.rule_tag)
    if parsed.snow_delay_ms is not None:
        stats.snow_delays_ms.append(parsed.snow_delay_ms)
    if debug_raw:
        print(_message_preview(message))
    if event_type != "tweet":
        print(f"Ignored event_type={event_type}")
        return
    for tweet in parsed.tweets:
        if stats.add_tweet(tweet):
            _print_tweet(tweet)


def _run_stream(
    *,
    api_key: str,
    ws_url: str,
    duration_minutes: float,
    max_reconnects: int,
    reconnect_cooldown_seconds: float,
    debug_raw: bool,
    stats: ProbeStats,
) -> None:
    try:
        import websocket
    except ImportError as exc:
        raise TwitterApiIoError("websocket-client dependency is required for stream probe") from exc

    deadline = time.monotonic() + max(0.0, duration_minutes) * 60
    reconnects = 0
    while time.monotonic() < deadline:
        ws = None
        connected_at = datetime.now(tz=UTC)
        try:
            ws = websocket.create_connection(
                ws_url,
                header=[f"x-api-key: {api_key}"],
                timeout=5,
            )
            print(f"WebSocket connected_at: {_fmt_dt(connected_at)}")
            while time.monotonic() < deadline:
                try:
                    message = ws.recv()
                except (TimeoutError, websocket.WebSocketTimeoutException):
                    continue
                if not message:
                    continue
                handle_stream_message(message, stats=stats, debug_raw=debug_raw)
            return
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            disconnected_at = datetime.now(tz=UTC)
            safe_error = _mask_secret(str(exc), api_key)
            stats.disconnects.append(f"{_fmt_dt(disconnected_at)} error={safe_error}")
            print(f"WebSocket disconnected_at: {_fmt_dt(disconnected_at)} error={safe_error}")
            if reconnects >= max_reconnects or time.monotonic() >= deadline:
                return
            reconnects += 1
            sleep_seconds = min(reconnect_cooldown_seconds, max(0.0, deadline - time.monotonic()))
            print(f"Waiting {sleep_seconds:.1f}s before reconnect attempt {reconnects}/{max_reconnects}")
            time.sleep(sleep_seconds)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def _ensure_stream_rule(
    client: TwitterApiIoClient,
    tag: str,
    value: str,
    interval_seconds: float,
    stats: ProbeStats,
) -> StreamRule:
    rules_data = client.get_filter_rules()
    stats.api_calls_tweet_filter += 1
    rules = rules_data.get("rules") if isinstance(rules_data.get("rules"), list) else []
    for row in rules:
        if not isinstance(row, dict) or row.get("tag") != tag:
            continue
        rule = StreamRule(
            rule_id=str(row["rule_id"]),
            tag=tag,
            value=value,
            interval_seconds=interval_seconds,
        )
        _set_rule_effective(client, rule, True, stats)
        print(f"Reused stream rule: rule_id={rule.rule_id} tag={rule.tag}")
        return rule
    created = client.add_filter_rule(tag=tag, value=value, interval_seconds=interval_seconds)
    stats.api_calls_tweet_filter += 1
    rule_id = _str_or_none(created.get("rule_id"))
    if not rule_id:
        raise TwitterApiIoError("TwitterAPI.io add_rule did not return rule_id")
    rule = StreamRule(rule_id=rule_id, tag=tag, value=value, interval_seconds=interval_seconds)
    _set_rule_effective(client, rule, True, stats)
    print(f"Created stream rule: rule_id={rule.rule_id} tag={rule.tag}")
    return rule


def _set_rule_effective(
    client: TwitterApiIoClient,
    rule: StreamRule,
    is_effect: bool,
    stats: ProbeStats,
) -> None:
    client.update_filter_rule(
        rule_id=rule.rule_id,
        tag=rule.tag,
        value=rule.value,
        interval_seconds=rule.interval_seconds,
        is_effect=is_effect,
    )
    stats.api_calls_tweet_filter += 1
    print(f"Rule is_effect set to {1 if is_effect else 0}: rule_id={rule.rule_id}")


def _print_rule_summary(data: dict[str, Any], debug_raw: bool) -> None:
    print("HTTP status: 200")
    print(f"status/message: {data.get('status')}/{data.get('msg') or data.get('message') or ''}")
    rules = data.get("rules") if isinstance(data.get("rules"), list) else []
    print(f"rules count: {len(rules)}")
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        print(
            "rule_id={rule_id} tag={tag} interval={interval} is_effect={is_effect} value={value}".format(
                rule_id=rule.get("rule_id"),
                tag=rule.get("tag"),
                interval=rule.get("interval_seconds"),
                is_effect=rule.get("is_effect"),
                value=rule.get("value"),
            )
        )
    if debug_raw:
        print(json.dumps(data, ensure_ascii=False, indent=2))


def _print_provider_status(data: dict[str, Any], debug_raw: bool) -> None:
    print("HTTP status: 200")
    print(f"status/message: {data.get('status')}/{data.get('msg') or data.get('message') or ''}")
    if data.get("rule_id"):
        print(f"rule_id: {data.get('rule_id')}")
    if debug_raw:
        print(json.dumps(data, ensure_ascii=False, indent=2))


def _load_usernames(path: Path) -> list[str]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("KOL config must be a JSON array")
    return [str(item).lstrip("@") for item in data if str(item).strip()]


def _rule_value_for_usernames(usernames: list[str]) -> str:
    return " OR ".join(f"from:{username}" for username in usernames)


def _parse_json_message(message: str | bytes) -> Any:
    try:
        text = message.decode("utf-8") if isinstance(message, bytes) else message
        return json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TwitterApiIoError("malformed websocket message") from exc


def _extract_stream_tweets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    tweets = payload.get("tweets")
    if isinstance(tweets, list):
        return [tweet for tweet in tweets if isinstance(tweet, dict)]
    tweet = payload.get("tweet")
    if isinstance(tweet, dict):
        return [tweet]
    data = payload.get("data")
    if isinstance(data, dict):
        if isinstance(data.get("tweets"), list):
            return [tweet for tweet in data["tweets"] if isinstance(tweet, dict)]
        if isinstance(data.get("tweet"), dict):
            return [data["tweet"]]
    return []


def _message_preview(message: str | bytes) -> str:
    text = message.decode("utf-8", errors="replace") if isinstance(message, bytes) else message
    return text[:1000]


def _fmt_dt(value: datetime | None) -> str:
    if not value:
        return "UNKNOWN"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _unknown(value: int | None) -> int | str:
    return value if value is not None else "UNKNOWN"


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0
    index = round((len(values) - 1) * percentile / 100)
    return values[index]


def _first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mask_secret(value: str, secret: str) -> str:
    if not secret:
        return value
    return value.replace(secret, "***")


if __name__ == "__main__":
    main()
