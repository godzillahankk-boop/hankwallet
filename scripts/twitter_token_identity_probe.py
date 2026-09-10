from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
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

CA_PROBE_RULE_TAG = "wallet-agent-token-identity-probe-ca-v01"
CASHTAG_PROBE_RULE_TAG = "wallet-agent-token-identity-probe-cashtag-v01"
DEFAULT_DURATION_SECONDS = 600
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_MAX_SAMPLE_TWEETS = 20
CANDIDATE_METADATA_KEYWORDS = ("card", "binding", "token", "crypto", "contract", "chain", "financial")
CONFIRMED_CARD_KEYWORDS = ("card", "binding", "binding_values", "bindingvalues", "financial_card", "financialcard", "crypto_card", "cryptocard")
CHAIN_RE = re.compile(r"\b(Ethereum|ETH|Base|Solana|SOL|Robinhood)\b", re.IGNORECASE)


@dataclass
class RuleDeliveryStats:
    provider_messages: int = 0
    tweet_deliveries: int = 0
    duplicates: int = 0
    seen_tweet_ids: set[str] = field(default_factory=set)

    def record(self, tweets: list[NormalizedTweet]) -> None:
        self.provider_messages += 1
        for tweet in tweets:
            self.tweet_deliveries += 1
            if tweet.tweet_id in self.seen_tweet_ids:
                self.duplicates += 1
            else:
                self.seen_tweet_ids.add(tweet.tweet_id)

    def summary(self, prefix: str) -> dict[str, Any]:
        return {
            f"{prefix}_provider_messages": self.provider_messages,
            f"{prefix}_tweet_deliveries": self.tweet_deliveries,
            f"{prefix}_unique_tweets": len(self.seen_tweet_ids),
            f"{prefix}_duplicates": self.duplicates,
            f"{prefix}_duplicate_ratio": _ratio(self.duplicates, self.tweet_deliveries),
        }


@dataclass
class ProbeTweetRecord:
    tweet_id: str
    created_at: datetime | None
    received_at: datetime
    author_id: str | None
    author_username: str | None
    rule_ids: set[str] = field(default_factory=set)
    rule_tags: set[str] = field(default_factory=set)
    contains_exact_ca: bool = False
    contains_exact_cashtag: bool = False
    identity_evidence: dict[str, Any] = field(default_factory=dict)

    def to_sample(self) -> dict[str, Any]:
        classification = classify_rule_hits(self.rule_tags)
        return {
            "tweet_id": self.tweet_id,
            "created_at": _iso(self.created_at),
            "received_at": _iso(self.received_at),
            "author_id": self.author_id,
            "author_username": self.author_username,
            "rule_id": sorted(self.rule_ids),
            "rule_tag": sorted(self.rule_tags),
            "classification": classification,
            "contains_exact_ca": self.contains_exact_ca,
            "contains_exact_cashtag": self.contains_exact_cashtag,
            "identity_status": identity_status(self.contains_exact_ca, self.contains_exact_cashtag),
            "identity_evidence": self.identity_evidence,
        }


@dataclass
class ProbeTweetAggregate:
    tweet_id: str
    rule_tags: set[str] = field(default_factory=set)
    contains_exact_ca: bool = False
    contains_exact_cashtag: bool = False
    candidate_token_metadata_found: bool = False
    token_card_metadata_found: bool = False


@dataclass
class TokenIdentityProbeStats:
    ca: str
    symbol: str
    started_at: datetime
    max_sample_tweets: int = DEFAULT_MAX_SAMPLE_TWEETS
    ca_rule: RuleDeliveryStats = field(default_factory=RuleDeliveryStats)
    cashtag_rule: RuleDeliveryStats = field(default_factory=RuleDeliveryStats)
    foreign_rule_messages: int = 0
    foreign_rule_tweet_deliveries: int = 0
    aggregate_tweets: OrderedDict[str, ProbeTweetAggregate] = field(default_factory=OrderedDict)
    sample_tweets: OrderedDict[str, ProbeTweetRecord] = field(default_factory=OrderedDict)

    def record_payload(
        self,
        *,
        rule_id: str | None,
        rule_tag: str | None,
        tweets: list[NormalizedTweet],
        received_at: datetime,
    ) -> None:
        if rule_tag == CA_PROBE_RULE_TAG:
            self.ca_rule.record(tweets)
        elif rule_tag == CASHTAG_PROBE_RULE_TAG:
            self.cashtag_rule.record(tweets)
        else:
            self.foreign_rule_messages += 1
            self.foreign_rule_tweet_deliveries += len(tweets)
            return
        for tweet in tweets:
            self.record_tweet(tweet, rule_id=rule_id, rule_tag=rule_tag, received_at=received_at)

    def record_tweet(
        self,
        tweet: NormalizedTweet,
        *,
        rule_id: str | None,
        rule_tag: str | None,
        received_at: datetime,
    ) -> None:
        exact_ca = tweet_contains_exact_ca(tweet, self.ca)
        exact_cashtag = tweet_contains_exact_cashtag(tweet, self.symbol)
        evidence = extract_identity_evidence(tweet, ca=self.ca, symbol=self.symbol)
        aggregate = self.aggregate_tweets.get(tweet.tweet_id)
        if aggregate is None:
            aggregate = ProbeTweetAggregate(tweet_id=tweet.tweet_id)
            self.aggregate_tweets[tweet.tweet_id] = aggregate
        aggregate.contains_exact_ca = aggregate.contains_exact_ca or exact_ca
        aggregate.contains_exact_cashtag = aggregate.contains_exact_cashtag or exact_cashtag
        aggregate.candidate_token_metadata_found = (
            aggregate.candidate_token_metadata_found
            or bool(evidence.get("candidate_token_metadata_found"))
        )
        aggregate.token_card_metadata_found = aggregate.token_card_metadata_found or bool(evidence.get("token_card_metadata_found"))
        if rule_tag:
            aggregate.rule_tags.add(rule_tag)

        sample = self.sample_tweets.get(tweet.tweet_id)
        if sample is None and len(self.sample_tweets) < self.max_sample_tweets:
            sample = ProbeTweetRecord(
                tweet_id=tweet.tweet_id,
                created_at=tweet.created_at,
                received_at=received_at,
                author_id=tweet.author_id,
                author_username=tweet.author_username,
                contains_exact_ca=exact_ca,
                contains_exact_cashtag=exact_cashtag,
                identity_evidence=evidence,
            )
            self.sample_tweets[tweet.tweet_id] = sample
        if sample:
            sample.contains_exact_ca = sample.contains_exact_ca or exact_ca
            sample.contains_exact_cashtag = sample.contains_exact_cashtag or exact_cashtag
            if rule_id:
                sample.rule_ids.add(rule_id)
            if rule_tag:
                sample.rule_tags.add(rule_tag)

    def summary(
        self,
        *,
        duration_seconds: float,
        formal_rules_active_at_start: bool = False,
        active_foreign_rules: list[dict[str, Any]] | None = None,
        aborted: bool = False,
        abort_reason: str | None = None,
        active_formal_rules: list[dict[str, Any]] | None = None,
        cleanup_failed: bool = False,
        cleanup_errors: list[dict[str, Any]] | None = None,
        cleanup_verified: bool = True,
        active_probe_rules_after_cleanup: list[dict[str, Any]] | None = None,
        cleanup_verification_error: str | None = None,
    ) -> dict[str, Any]:
        records = list(self.aggregate_tweets.values())
        samples = [record.to_sample() for record in self.sample_tweets.values()]
        classifications = [classify_rule_hits(record.rule_tags) for record in records]
        identity_statuses = [
            identity_status(record.contains_exact_ca, record.contains_exact_cashtag)
            for record in records
        ]
        has_active_foreign_rules = bool(active_foreign_rules)
        cost_isolated = (
            not aborted
            and not formal_rules_active_at_start
            and not has_active_foreign_rules
            and self.foreign_rule_messages == 0
            and self.foreign_rule_tweet_deliveries == 0
        )
        probe_conclusion = "ISOLATED"
        if abort_reason == "ACTIVE_FORMAL_RULES_DETECTED":
            probe_conclusion = "ABORTED_ACTIVE_FORMAL_RULES"
        elif abort_reason == "ACTIVE_FOREIGN_RULES_DETECTED":
            probe_conclusion = "ABORTED_ACTIVE_FOREIGN_RULES"
        elif self.foreign_rule_messages or self.foreign_rule_tweet_deliveries:
            probe_conclusion = "INCONCLUSIVE_FOREIGN_RULE_ACTIVITY"
        elif cleanup_failed:
            probe_conclusion = "ISOLATED_BUT_CLEANUP_FAILED"
        elif not cleanup_verified or active_probe_rules_after_cleanup:
            probe_conclusion = "ISOLATED_BUT_CLEANUP_UNVERIFIED"
        safe_to_stop_monitoring = (
            cost_isolated
            and not cleanup_failed
            and cleanup_verified
            and not active_probe_rules_after_cleanup
        )
        summary = {
            "duration_seconds": round(duration_seconds, 3),
            "ca": self.ca.lower(),
            "symbol": self.symbol.upper(),
            "ca_rule_tag": CA_PROBE_RULE_TAG,
            "cashtag_rule_tag": CASHTAG_PROBE_RULE_TAG,
            **self.ca_rule.summary("ca"),
            **self.cashtag_rule.summary("cashtag"),
            "unique_tweet_ids": len(records),
            "ca_only_count": classifications.count("ca_only"),
            "cashtag_only_count": classifications.count("cashtag_only"),
            "both_count": classifications.count("both"),
            "exact_ca_count": sum(1 for record in records if record.contains_exact_ca),
            "exact_ca_and_cashtag_count": identity_statuses.count("exact_ca_and_cashtag"),
            "ambiguous_symbol_count": identity_statuses.count("ambiguous_symbol"),
            "candidate_token_metadata_found_count": sum(1 for record in records if record.candidate_token_metadata_found),
            "token_card_metadata_found_count": sum(1 for record in records if record.token_card_metadata_found),
            "foreign_rule_messages": self.foreign_rule_messages,
            "foreign_rule_tweet_deliveries": self.foreign_rule_tweet_deliveries,
            "cost_isolated": cost_isolated,
            "probe_conclusion": probe_conclusion,
            "formal_rules_active_at_start": formal_rules_active_at_start,
            "aborted": aborted,
            "abort_reason": abort_reason,
            "active_formal_rules": active_formal_rules or [],
            "active_foreign_rules": active_foreign_rules or [],
            "cleanup_failed": cleanup_failed,
            "cleanup_errors": cleanup_errors or [],
            "cleanup_verified": cleanup_verified,
            "active_probe_rules_after_cleanup": active_probe_rules_after_cleanup or [],
            "cleanup_verification_error": cleanup_verification_error,
            "safe_to_stop_monitoring": safe_to_stop_monitoring,
            "sample_tweets": samples[: self.max_sample_tweets],
        }
        return _sanitize_for_output(summary)


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Isolated TwitterAPI.io CA + Cashtag identity probe")
    parser.add_argument("--ca", required=True, help="Full token contract address")
    parser.add_argument("--symbol", required=True, help="Token symbol without '$'")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    args = parser.parse_args()

    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        print("TWITTERAPI_IO_API_KEY missing; token identity probe skipped.")
        return
    client = TwitterApiIoClient(api_key)
    summary = run_token_identity_probe(
        client,
        api_key=api_key,
        ca=args.ca,
        symbol=args.symbol,
        duration_seconds=max(1, args.duration),
        interval_seconds=max(60, args.interval),
        ws_url=args.ws_url,
    )
    if summary.get("aborted"):
        print(summary.get("abort_reason") or "TOKEN_IDENTITY_PROBE_ABORTED")
        if summary.get("abort_reason") == "ACTIVE_FORMAL_RULES_DETECTED":
            print("Run: .venv/bin/python scripts/twitter_social_rule_control.py pause")
    print(json.dumps(summary, indent=2, sort_keys=True))


def run_token_identity_probe(
    client: TwitterApiIoClient,
    *,
    api_key: str,
    ca: str,
    symbol: str,
    duration_seconds: int = DEFAULT_DURATION_SECONDS,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    ws_url: str = TWITTERAPI_IO_WS_URL,
    websocket_runner: Callable[..., None] | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    started = time.monotonic()
    stats = TokenIdentityProbeStats(ca=ca.lower(), symbol=symbol.upper(), started_at=started_at)
    active_formal_rules, active_foreign_rules = active_rule_isolation_summaries(client)
    if active_formal_rules:
        return stats.summary(
            duration_seconds=time.monotonic() - started,
            formal_rules_active_at_start=True,
            aborted=True,
            abort_reason="ACTIVE_FORMAL_RULES_DETECTED",
            active_formal_rules=active_formal_rules,
            cleanup_verified=False,
        )
    if active_foreign_rules:
        return stats.summary(
            duration_seconds=time.monotonic() - started,
            active_foreign_rules=active_foreign_rules,
            aborted=True,
            abort_reason="ACTIVE_FOREIGN_RULES_DETECTED",
            cleanup_verified=False,
        )
    ca_rule = None
    cashtag_rule = None
    cleanup_errors: list[dict[str, Any]] = []
    cleanup_verified = False
    active_probe_rules_after_cleanup: list[dict[str, Any]] = []
    cleanup_verification_error: str | None = None
    runner = websocket_runner or run_websocket_probe
    runner_error: Exception | None = None
    try:
        ca_rule = ensure_probe_rule(client, tag=CA_PROBE_RULE_TAG, value=ca.lower(), interval_seconds=interval_seconds)
        cashtag_rule = ensure_probe_rule(
            client,
            tag=CASHTAG_PROBE_RULE_TAG,
            value=f"${symbol.upper()}",
            interval_seconds=interval_seconds,
        )
        runner(ws_url, api_key, stats=stats, duration_seconds=duration_seconds)
    except Exception as exc:  # noqa: BLE001 - cleanup and verification must still run.
        runner_error = exc
    finally:
        cleanup_errors.extend(
            cleanup_probe_rules(
                client,
                rules=[
                    (ca_rule, ca.lower()),
                    (cashtag_rule, f"${symbol.upper()}"),
                ],
                interval_seconds=interval_seconds,
                api_key=api_key,
            )
        )
        cleanup_verified, active_probe_rules_after_cleanup, cleanup_verification_error = verify_probe_cleanup(
            client,
            api_key=api_key,
        )
    if runner_error:
        raise runner_error
    return stats.summary(
        duration_seconds=time.monotonic() - started,
        cleanup_failed=bool(cleanup_errors),
        cleanup_errors=cleanup_errors,
        cleanup_verified=cleanup_verified,
        active_probe_rules_after_cleanup=active_probe_rules_after_cleanup,
        cleanup_verification_error=cleanup_verification_error,
    )


def ensure_probe_rule(client: TwitterApiIoClient, *, tag: str, value: str, interval_seconds: int) -> dict[str, Any]:
    rules = _extract_rules(client.get_filter_rules())
    existing = next((rule for rule in rules if str(rule.get("tag")) == tag), None)
    if existing is None:
        created = client.add_filter_rule(tag=tag, value=value, interval_seconds=interval_seconds)
        rule_id = _rule_id(created)
        if not rule_id:
            raise TwitterApiIoError("token identity probe add_rule did not return rule_id")
        existing = {"rule_id": rule_id, "tag": tag, "value": value, "is_effect": 0}
    rule_id = _rule_id(existing)
    if not rule_id:
        raise TwitterApiIoError("token identity probe rule missing rule_id")
    if (
        existing.get("value") != value
        or not _is_effect(existing)
        or _interval_seconds(existing) != interval_seconds
    ):
        client.update_filter_rule(
            rule_id=rule_id,
            tag=tag,
            value=value,
            interval_seconds=interval_seconds,
            is_effect=True,
        )
    return {"rule_id": rule_id, "tag": tag}


def deactivate_probe_rule(client: TwitterApiIoClient, *, rule: dict[str, Any], value: str, interval_seconds: int) -> None:
    client.update_filter_rule(
        rule_id=str(rule["rule_id"]),
        tag=str(rule["tag"]),
        value=value,
        interval_seconds=interval_seconds,
        is_effect=False,
    )


def cleanup_probe_rules(
    client: TwitterApiIoClient,
    *,
    rules: list[tuple[dict[str, Any] | None, str]],
    interval_seconds: int,
    api_key: str = "",
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for rule, value in rules:
        if not rule:
            continue
        try:
            deactivate_probe_rule(client, rule=rule, value=value, interval_seconds=interval_seconds)
        except Exception as exc:  # noqa: BLE001 - cleanup must attempt every probe rule.
            errors.append(
                {
                    "tag": str(rule.get("tag") or ""),
                    "rule_id": str(rule.get("rule_id") or ""),
                    "error": _safe_error_message(exc, api_key=api_key),
                }
            )
    return errors


def verify_probe_cleanup(
    client: TwitterApiIoClient,
    *,
    api_key: str = "",
) -> tuple[bool, list[dict[str, Any]], str | None]:
    try:
        rules = _extract_rules(client.get_filter_rules())
    except Exception as exc:  # noqa: BLE001 - verification failure must be visible in summary.
        return False, [], _safe_error_message(exc, api_key=api_key)
    active_probe_rules = [
        _rule_summary(rule)
        for rule in rules
        if str(rule.get("tag") or "") in {CA_PROBE_RULE_TAG, CASHTAG_PROBE_RULE_TAG}
        and _is_effect(rule)
    ]
    return not active_probe_rules, active_probe_rules, None


def run_websocket_probe(
    ws_url: str,
    api_key: str,
    *,
    stats: TokenIdentityProbeStats,
    duration_seconds: int,
) -> None:
    try:
        import websocket
    except ImportError as exc:
        raise TwitterApiIoError("websocket-client dependency is required for token identity probe") from exc
    ws = websocket.create_connection(ws_url, header=[f"x-api-key: {api_key}"], timeout=10)
    deadline = time.monotonic() + duration_seconds
    try:
        while time.monotonic() < deadline:
            try:
                message = ws.recv()
            except (TimeoutError, websocket.WebSocketTimeoutException):
                continue
            record_stream_message(message, stats=stats, received_at=datetime.now(UTC))
    finally:
        try:
            ws.close()
        except Exception:
            pass


def record_stream_message(message: str | bytes, *, stats: TokenIdentityProbeStats, received_at: datetime) -> None:
    parsed = parse_stream_message(message, received_at=received_at)
    if parsed.event_type != "tweet":
        return
    stats.record_payload(
        rule_id=parsed.rule_id,
        rule_tag=parsed.rule_tag,
        tweets=parsed.tweets,
        received_at=received_at,
    )


def classify_rule_hits(rule_tags: set[str]) -> str:
    ca = CA_PROBE_RULE_TAG in rule_tags
    cashtag = CASHTAG_PROBE_RULE_TAG in rule_tags
    if ca and cashtag:
        return "both"
    if ca:
        return "ca_only"
    if cashtag:
        return "cashtag_only"
    return "foreign_or_unknown"


def identity_status(contains_exact_ca: bool, contains_exact_cashtag: bool) -> str:
    if contains_exact_ca and contains_exact_cashtag:
        return "exact_ca_and_cashtag"
    if contains_exact_ca:
        return "exact_ca"
    if contains_exact_cashtag:
        return "ambiguous_symbol"
    return "foreign_or_unknown"


def tweet_contains_exact_ca(tweet: NormalizedTweet, ca: str) -> bool:
    target = ca.lower()
    return any(match.match_type.endswith("_ca") and match.value.lower() == target for match in tweet.token_matches)


def tweet_contains_exact_cashtag(tweet: NormalizedTweet, symbol: str) -> bool:
    target = symbol.upper()
    return any(match.match_type.endswith("_cashtag") and match.value.upper() == target for match in tweet.token_matches)


def extract_identity_evidence(tweet: NormalizedTweet, *, ca: str, symbol: str) -> dict[str, Any]:
    raw = tweet.raw if isinstance(tweet.raw, dict) else {}
    text = _tweet_text_blob(tweet)
    urls = _extract_urls(raw)
    candidate_token_metadata_fields = _metadata_fields(raw, CANDIDATE_METADATA_KEYWORDS)
    token_card_fields = _metadata_fields(raw, CONFIRMED_CARD_KEYWORDS)
    return {
        "exact_ca": tweet_contains_exact_ca(tweet, ca),
        "cashtag": symbol.upper() if tweet_contains_exact_cashtag(tweet, symbol) else None,
        "chain_mentions": sorted({match.group(1) for match in CHAIN_RE.finditer(text)}, key=str.lower),
        "urls": urls,
        "mentioned_accounts": _extract_mentions(raw),
        "quoted_author": _quoted_author(raw),
        "candidate_token_metadata_found": bool(candidate_token_metadata_fields),
        "candidate_token_metadata_fields": candidate_token_metadata_fields,
        "token_card_metadata_found": bool(token_card_fields),
        "token_card_fields": token_card_fields,
        "payload_shape": _payload_shape(raw),
    }


def active_formal_rule_summaries(client: TwitterApiIoClient) -> list[dict[str, Any]]:
    active_formal, _ = active_rule_isolation_summaries(client)
    return active_formal


def active_rule_isolation_summaries(client: TwitterApiIoClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active_formal: list[dict[str, Any]] = []
    active_foreign: list[dict[str, Any]] = []
    probe_tags = {CA_PROBE_RULE_TAG, CASHTAG_PROBE_RULE_TAG}
    for rule in _extract_rules(client.get_filter_rules()):
        if not _is_effect(rule):
            continue
        if _is_formal_social_rule(rule):
            active_formal.append(_rule_summary(rule))
            continue
        if str(rule.get("tag") or "") not in probe_tags:
            active_foreign.append(_rule_summary(rule))
    return active_formal, active_foreign


def _tweet_text_blob(tweet: NormalizedTweet) -> str:
    parts = [tweet.text or ""]
    for nested in (tweet.quoted_tweet, tweet.retweeted_tweet):
        if isinstance(nested, dict):
            parts.append(str(nested.get("text") or ""))
    return "\n".join(parts)


def _extract_urls(raw: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for path in (("entities", "urls"), ("extended_entities", "urls")):
        data = _get_path(raw, path)
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    value = row.get("expanded_url") or row.get("url")
                    if isinstance(value, str):
                        urls.append(value)
    return _dedupe_strings(urls)


def _extract_mentions(raw: dict[str, Any]) -> list[str]:
    mentions = _get_path(raw, ("entities", "user_mentions"))
    values: list[str] = []
    if isinstance(mentions, list):
        for row in mentions:
            if isinstance(row, dict):
                value = row.get("screen_name") or row.get("userName") or row.get("username")
                if isinstance(value, str):
                    values.append(value)
    return _dedupe_strings(values)


def _quoted_author(raw: dict[str, Any]) -> str | None:
    quoted = raw.get("quoted_tweet") or raw.get("quotedTweet")
    if not isinstance(quoted, dict):
        return None
    author = quoted.get("author")
    if not isinstance(author, dict):
        return None
    value = author.get("userName") or author.get("username") or author.get("screen_name")
    return str(value) if value else None


def _metadata_fields(raw: dict[str, Any], keywords: tuple[str, ...]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    _walk_metadata(raw, (), fields, keywords)
    return fields[:50]


def _walk_metadata(value: Any, path: tuple[str, ...], fields: list[dict[str, Any]], keywords: tuple[str, ...]) -> None:
    if len(fields) >= 50:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            normalized_key = str(key).lower().replace("-", "_")
            if any(keyword in normalized_key for keyword in keywords):
                fields.append({"path": ".".join(child_path), "value": _safe_metadata_value(child)})
            _walk_metadata(child, child_path, fields, keywords)
    elif isinstance(value, list):
        for index, child in enumerate(value[:10]):
            _walk_metadata(child, (*path, str(index)), fields, keywords)


def _payload_shape(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _payload_shape(child) for key, child in sorted(value.items())}
    if isinstance(value, list):
        if not value:
            return []
        return [_payload_shape(value[0])]
    return type(value).__name__


def _safe_metadata_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe_metadata_value(child) for key, child in list(value.items())[:8]}
    if isinstance(value, list):
        return [_safe_metadata_value(child) for child in value[:5]]
    if isinstance(value, str):
        return value[:160]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return type(value).__name__


def _sanitize_for_output(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _sanitize_for_output(child)
            for key, child in value.items()
            if str(key).lower() not in {"authorization", "x-api-key", "api_key", "apikey", "cookie"}
        }
    if isinstance(value, list):
        return [_sanitize_for_output(child) for child in value]
    if isinstance(value, str):
        return value.replace(os.getenv("TWITTERAPI_IO_API_KEY", ""), "***") if os.getenv("TWITTERAPI_IO_API_KEY") else value
    return value


def _safe_error_message(exc: Exception, *, api_key: str = "") -> str:
    message = str(_sanitize_for_output(str(exc)))
    if api_key:
        message = message.replace(api_key, "***")
    return message[:300]


def _get_path(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = data
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _dedupe_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _extract_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("rules"), list):
        return [rule for rule in nested["rules"] if isinstance(rule, dict)]
    return []


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


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


if __name__ == "__main__":
    main()
