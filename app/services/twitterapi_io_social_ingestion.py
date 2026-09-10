from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService
from app.services.social_watch_registry import SocialWatchAccount, SocialWatchRegistry
from app.services.twitterapi_io_client import (
    NormalizedTweet,
    TwitterApiIoClient,
    TwitterApiIoError,
    normalize_tweets_from_response,
)
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

SOCIAL_RULE_TAG = "wallet-agent-social-v1"
SOCIAL_RULE_TAG_PREFIX = f"{SOCIAL_RULE_TAG}-"
DEFAULT_RULE_MAX_VALUE_CHARS = 240
DEFAULT_FILTER_INTERVAL_SECONDS = 300
DEFAULT_RULE_MIN_UPDATE_INTERVAL_SECONDS = 1800
DEFAULT_RULE_SHARD_STATE_PATH = Path("data/twitter_social_rule_shards.json")
TWITTERAPI_IO_WS_URL = "wss://ws.twitterapi.io/twitter/tweet/websocket"
STREAM_DISCONNECTED = "DISCONNECTED"
STREAM_CONNECTING = "CONNECTING"
STREAM_CONNECTED = "CONNECTED"
STREAM_BACKOFF = "BACKOFF"
DEFAULT_BACKOFF_SECONDS = (90, 120, 180)


@dataclass
class SocialIngestionStats:
    messages_received: int = 0
    tweets_received: int = 0
    unique_tweets_seen: int = 0
    warmup_ignored: int = 0
    retweets_ignored: int = 0
    unmatched_tweets: int = 0
    social_events_created: int = 0
    kol_events: int = 0
    project_events: int = 0
    dev_x_events: int = 0
    duplicates: int = 0
    provider_errors: int = 0
    disconnects: int = 0
    reconnects: int = 0
    rule_updates: int = 0
    filter_api_calls: int = 0
    rule_refresh_checks: int = 0
    registry_changes: int = 0
    rule_reconcile_runs: int = 0
    rule_creates: int = 0
    rule_deactivations: int = 0
    rule_unchanged: int = 0
    shards_changed: int = 0


@dataclass(frozen=True)
class DuplicateTweetDiagnostic:
    tweet_id: str
    author_id: str | None
    received_at: datetime
    rule_id: str | None
    rule_tag: str | None
    first_rule_id: str | None
    first_rule_tag: str | None
    same_connection: bool
    same_payload_batch: bool
    seconds_since_rule_refresh: float | None
    seconds_since_rule_update: float | None

    def to_dict(self) -> dict[str, object]:
        return {
            "tweet_id": self.tweet_id,
            "author_id": self.author_id,
            "received_at": self.received_at.isoformat(),
            "rule_id": self.rule_id,
            "rule_tag": self.rule_tag,
            "first_rule_id": self.first_rule_id,
            "first_rule_tag": self.first_rule_tag,
            "same_connection": self.same_connection,
            "same_payload_batch": self.same_payload_batch,
            "seconds_since_rule_refresh": self.seconds_since_rule_refresh,
            "seconds_since_rule_update": self.seconds_since_rule_update,
            "different_rule": (self.rule_id, self.rule_tag) != (self.first_rule_id, self.first_rule_tag),
        }


@dataclass(frozen=True)
class TwitterApiIoRule:
    rule_id: str
    tag: str
    value: str
    interval_seconds: float
    is_effect: bool


@dataclass(frozen=True)
class TwitterApiIoStreamMessage:
    event_type: str
    rule_id: str | None
    rule_tag: str | None
    tweets: list[NormalizedTweet]
    snow_delay_ms: float | None = None


class AuthorFirstCashtagShadowObserver(Protocol):
    def observe_stream_tweets(self, tweets: list[NormalizedTweet], *, received_at: datetime | None = None) -> None:
        ...

    def stats_snapshot(self) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class RuleReconcileEvent:
    tag: str
    action: str
    username_count: int
    reason: str
    occurred_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "tag": self.tag,
            "action": self.action,
            "username_count": self.username_count,
            "reason": self.reason,
            "occurred_at": self.occurred_at.isoformat(),
        }


class RuleShardStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self._memory_state: dict[str, list[str]] = {}

    def load(self) -> dict[str, list[str]]:
        if self.path is None:
            return {tag: list(usernames) for tag, usernames in self._memory_state.items()}
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text())
        except json.JSONDecodeError as exc:
            raise TwitterApiIoError(f"TwitterAPI.io rule shard state is malformed path={self.path}") from exc
        if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("shards"), dict):
            raise TwitterApiIoError(f"TwitterAPI.io rule shard state is malformed path={self.path}")
        state: dict[str, list[str]] = {}
        for tag, usernames in data["shards"].items():
            if not isinstance(tag, str) or not tag.startswith(SOCIAL_RULE_TAG_PREFIX) or not isinstance(usernames, list):
                raise TwitterApiIoError(f"TwitterAPI.io rule shard state is malformed path={self.path}")
            rows: list[str] = []
            seen = set()
            for username in usernames:
                if not isinstance(username, str) or not username.strip():
                    raise TwitterApiIoError(f"TwitterAPI.io rule shard state is malformed path={self.path}")
                clean = username.strip()
                key = clean.lower()
                if key in seen:
                    continue
                seen.add(key)
                rows.append(clean)
            state[tag] = rows
        return state

    def save(self, state: dict[str, list[str]]) -> None:
        normalized = {tag: list(usernames) for tag, usernames in sorted(state.items())}
        if self.path is None:
            self._memory_state = normalized
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"version": 1, "shards": normalized}, indent=2, sort_keys=True))


class TwitterApiIoRuleManager:
    def __init__(
        self,
        client: TwitterApiIoClient,
        *,
        tag_prefix: str = SOCIAL_RULE_TAG_PREFIX,
        legacy_tag: str = SOCIAL_RULE_TAG,
        interval_seconds: float = DEFAULT_FILTER_INTERVAL_SECONDS,
        max_value_chars: int = DEFAULT_RULE_MAX_VALUE_CHARS,
        min_update_interval_seconds: float = DEFAULT_RULE_MIN_UPDATE_INTERVAL_SECONDS,
        shard_state_path: Path | str | None = None,
        clock=utc_now,
    ) -> None:
        self.client = client
        self.tag_prefix = tag_prefix
        self.legacy_tag = legacy_tag
        self.interval_seconds = max(60, interval_seconds)
        self.max_value_chars = max(1, min(max_value_chars, 254))
        self.min_update_interval_seconds = max(0, min_update_interval_seconds)
        self.shard_state_store = RuleShardStateStore(Path(shard_state_path) if shard_state_path is not None else None)
        self.clock = clock
        self._last_fingerprint: str | None = None
        self._last_seen_fingerprint: str | None = None
        self._last_rules: list[TwitterApiIoRule] | None = None
        self._last_formal_update_at: datetime | None = None
        self._pending_accounts: list[SocialWatchAccount] | None = None
        self._pending_fingerprint: str | None = None
        self._last_desired_shards: dict[str, list[str]] = {}
        self._recent_reconcile_events: deque[RuleReconcileEvent] = deque(maxlen=100)

    async def ensure_rule(
        self,
        accounts: list[SocialWatchAccount],
        *,
        stats: SocialIngestionStats | None = None,
    ) -> TwitterApiIoRule | None:
        rules = await self.ensure_rules(accounts, stats=stats)
        return rules[0] if rules else None

    async def ensure_rules(
        self,
        accounts: list[SocialWatchAccount],
        *,
        stats: SocialIngestionStats | None = None,
    ) -> list[TwitterApiIoRule]:
        fingerprint = self._fingerprint(accounts)
        if self._last_rules is not None and self._last_fingerprint == fingerprint:
            return self._last_rules
        if stats and self._last_seen_fingerprint != fingerprint:
            stats.registry_changes += 1
        self._last_seen_fingerprint = fingerprint
        if self._should_defer_update(fingerprint):
            self._pending_accounts = list(accounts)
            self._pending_fingerprint = fingerprint
            return self._last_rules or []
        target_shards, state_changed = build_stable_rule_shards(
            accounts,
            store=self.shard_state_store,
            tag_prefix=self.tag_prefix,
            max_value_chars=self.max_value_chars,
        )
        if stats:
            stats.rule_reconcile_runs += 1
            if state_changed:
                stats.shards_changed += 1
        rules_data = await asyncio.to_thread(self.client.get_filter_rules)
        if stats:
            stats.filter_api_calls += 1
        existing_rules = _managed_rules(rules_data, self.tag_prefix, self.legacy_tag)
        if not target_shards:
            deactivated: list[TwitterApiIoRule] = []
            for rule in existing_rules.values():
                if rule.is_effect:
                    rule = await self._update_rule(rule, rule.value, False, stats)
                    if stats:
                        stats.rule_deactivations += 1
                    self._record_reconcile_event(rule.tag, "deactivate", 0, "empty_registry")
                elif stats:
                    stats.rule_unchanged += 1
                deactivated.append(rule)
            self._last_fingerprint = fingerprint
            self._last_rules = deactivated
            self._last_desired_shards = {}
            return deactivated

        desired_tags = {tag for tag, _ in target_shards}
        managed: list[TwitterApiIoRule] = []
        for tag, value in target_shards:
            username_count = _rule_username_count(value)
            existing = existing_rules.get(tag)
            if existing is None:
                created = await asyncio.to_thread(
                    self.client.add_filter_rule,
                    tag=tag,
                    value=value,
                    interval_seconds=self.interval_seconds,
                )
                if stats:
                    stats.filter_api_calls += 1
                    stats.rule_creates += 1
                rule_id = _rule_id(created)
                if not rule_id:
                    raise TwitterApiIoError("TwitterAPI.io add_rule did not return rule_id")
                existing = TwitterApiIoRule(rule_id, tag, value, self.interval_seconds, False)
                self._record_reconcile_event(tag, "create", username_count, "missing_provider_rule")
            update_reason = _rule_update_reason(existing, value, self.interval_seconds)
            if update_reason:
                existing = await self._update_rule(existing, value, True, stats)
                self._record_reconcile_event(tag, "update", username_count, update_reason)
            elif stats:
                stats.rule_unchanged += 1
            managed.append(existing)

        for tag, rule in existing_rules.items():
            if tag in desired_tags:
                continue
            if rule.is_effect:
                await self._update_rule(rule, rule.value, False, stats)
                if stats:
                    stats.rule_deactivations += 1
                self._record_reconcile_event(tag, "deactivate", _rule_username_count(rule.value), "extra_provider_shard")
            elif stats:
                stats.rule_unchanged += 1

        self._last_fingerprint = fingerprint
        self._last_rules = managed
        self._last_desired_shards = {tag: _usernames_from_rule_value(value) for tag, value in target_shards}
        self._pending_accounts = None
        self._pending_fingerprint = None
        return managed

    async def _update_rule(
        self,
        rule: TwitterApiIoRule,
        value: str,
        is_effect: bool,
        stats: SocialIngestionStats | None,
    ) -> TwitterApiIoRule:
        await asyncio.to_thread(
            self.client.update_filter_rule,
            rule_id=rule.rule_id,
            tag=rule.tag,
            value=value,
            interval_seconds=self.interval_seconds,
            is_effect=is_effect,
        )
        if stats:
            stats.filter_api_calls += 1
            stats.rule_updates += 1
        self._last_formal_update_at = self.clock()
        return TwitterApiIoRule(rule.rule_id, rule.tag, value, self.interval_seconds, is_effect)

    def _record_reconcile_event(self, tag: str, action: str, username_count: int, reason: str) -> None:
        event = RuleReconcileEvent(
            tag=tag,
            action=action,
            username_count=username_count,
            reason=reason,
            occurred_at=_aware_utc(self.clock()),
        )
        self._recent_reconcile_events.append(event)
        logger.info(
            "SOCIAL_RULE_RECONCILE tag=%s action=%s username_count=%s reason=%s occurred_at=%s",
            event.tag,
            event.action,
            event.username_count,
            event.reason,
            event.occurred_at.isoformat(),
        )

    def recent_reconcile_events(self) -> list[dict[str, object]]:
        return [event.to_dict() for event in self._recent_reconcile_events]

    @property
    def last_provider_update_at(self) -> datetime | None:
        return self._last_formal_update_at

    def _fingerprint(self, accounts: list[SocialWatchAccount]) -> str:
        return f"{SocialWatchRegistry.fingerprint(accounts)}|interval={int(self.interval_seconds)}"

    def _should_defer_update(self, fingerprint: str) -> bool:
        if self._last_rules is None or self._last_fingerprint is None:
            return False
        if self._last_fingerprint == fingerprint:
            return False
        if not any(rule.is_effect for rule in self._last_rules):
            return False
        if self._last_formal_update_at is None:
            return False
        elapsed = (_aware_utc(self.clock()) - _aware_utc(self._last_formal_update_at)).total_seconds()
        return elapsed < self.min_update_interval_seconds


class TwitterApiIoSocialIngestor:
    def __init__(
        self,
        *,
        api_key: str,
        registry: SocialWatchRegistry,
        event_service: SocialEventService,
        rule_manager: TwitterApiIoRuleManager,
        ws_url: str = TWITTERAPI_IO_WS_URL,
        rule_refresh_seconds: float = 300,
        warmup_grace_seconds: int = 120,
        backoff_seconds: tuple[int, ...] = DEFAULT_BACKOFF_SECONDS,
        max_runtime_dedupe_ids: int = 10000,
        author_cashtag_shadow: AuthorFirstCashtagShadowObserver | None = None,
    ) -> None:
        if not api_key:
            raise TwitterApiIoError("TWITTERAPI_IO_API_KEY is required")
        self.api_key = api_key
        self.registry = registry
        self.event_service = event_service
        self.rule_manager = rule_manager
        self.ws_url = ws_url
        self.rule_refresh_seconds = max(0.01, rule_refresh_seconds)
        self.warmup_grace_seconds = max(0, warmup_grace_seconds)
        self.backoff_seconds = backoff_seconds
        self.stats = SocialIngestionStats()
        self.connection_state = STREAM_DISCONNECTED
        self.connected_at: datetime | None = None
        self.last_connected_at: datetime | None = None
        self.last_disconnected_at: datetime | None = None
        self.last_message_at: datetime | None = None
        self.last_tweet_at: datetime | None = None
        self.total_disconnect_seconds: float = 0.0
        self._disconnect_gap_open = False
        self._stream_task: asyncio.Task | None = None
        self._rule_refresh_task: asyncio.Task | None = None
        self._stopping = False
        self.max_runtime_dedupe_ids = max(1, max_runtime_dedupe_ids)
        self._seen_tweet_ids: OrderedDict[str, None] = OrderedDict()
        self._tweet_delivery_index: OrderedDict[str, dict[str, object]] = OrderedDict()
        self._duplicate_samples: deque[DuplicateTweetDiagnostic] = deque(maxlen=100)
        self._current_accounts: list[SocialWatchAccount] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None
        self._message_sequence = 0
        self._connection_sequence = 0
        self._current_connection_id = 0
        self.last_rule_refresh_at: datetime | None = None
        self.last_rule_update_at: datetime | None = None
        self.author_cashtag_shadow = author_cashtag_shadow

    async def start(self) -> None:
        if (
            self._stream_task
            and not self._stream_task.done()
            and self._rule_refresh_task
            and not self._rule_refresh_task.done()
        ):
            return
        self._stopping = False
        self._loop = asyncio.get_running_loop()
        await self._preload_accounts()
        self._stream_task = asyncio.create_task(self._stream_loop(), name="twitterapi_io_social_stream")
        self._rule_refresh_task = asyncio.create_task(
            self._rule_refresh_loop(),
            name="twitterapi_io_social_rule_refresh",
        )

    async def stop(self) -> None:
        self._stopping = True
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        tasks = [task for task in (self._stream_task, self._rule_refresh_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.connection_state = STREAM_DISCONNECTED

    async def refresh_rule(self) -> TwitterApiIoRule | None:
        self.stats.rule_refresh_checks += 1
        self.last_rule_refresh_at = utc_now()
        previous_updates = self.stats.rule_updates
        self._current_accounts = self.registry.accounts()
        rules = await self.rule_manager.ensure_rules(self._current_accounts, stats=self.stats)
        if self.stats.rule_updates > previous_updates:
            self.last_rule_update_at = utc_now()
        return rules[0] if rules else None

    async def _preload_accounts(self) -> None:
        try:
            self._current_accounts = await asyncio.to_thread(self.registry.accounts)
        except Exception as exc:
            self.stats.provider_errors += 1
            logger.warning("TwitterAPI.io social registry preload failed: %s", _mask_secret(str(exc), self.api_key))

    async def handle_message(self, message: str | bytes, *, received_at: datetime | None = None) -> None:
        received = received_at or utc_now()
        self.last_message_at = received
        self.stats.messages_received += 1
        try:
            parsed = parse_stream_message(message, received_at=received)
        except TwitterApiIoError as exc:
            self.stats.provider_errors += 1
            logger.warning("TwitterAPI.io stream payload ignored: %s", exc)
            return
        if parsed.event_type == "connected":
            self._record_connected(received)
            return
        if parsed.event_type == "ping":
            return
        if parsed.event_type != "tweet":
            logger.debug("TwitterAPI.io stream event ignored event_type=%s", parsed.event_type)
            return
        self._message_sequence += 1
        batch_id = self._message_sequence
        if self.author_cashtag_shadow is not None:
            try:
                await asyncio.to_thread(
                    self.author_cashtag_shadow.observe_stream_tweets,
                    parsed.tweets,
                    received_at=received,
                )
            except Exception as exc:
                self.stats.provider_errors += 1
                logger.warning("Author-first cashtag shadow observation failed: %s", exc)
        for tweet in parsed.tweets:
            await self._handle_tweet(
                tweet,
                rule_id=parsed.rule_id,
                rule_tag=parsed.rule_tag,
                received_at=received,
                batch_id=batch_id,
            )

    async def _handle_tweet(
        self,
        tweet: NormalizedTweet,
        *,
        rule_id: str | None,
        rule_tag: str | None,
        received_at: datetime | None = None,
        batch_id: int | None = None,
    ) -> None:
        self.stats.tweets_received += 1
        received = received_at or utc_now()
        if not self._remember_tweet_id(
            tweet.tweet_id,
            tweet=tweet,
            rule_id=rule_id,
            rule_tag=rule_tag,
            received_at=received,
            batch_id=batch_id,
        ):
            self.stats.duplicates += 1
            self._record_duplicate_tweet(
                tweet,
                rule_id=rule_id,
                rule_tag=rule_tag,
                received_at=received,
                batch_id=batch_id,
            )
            return
        self.stats.unique_tweets_seen += 1
        if tweet.created_at and self.connected_at:
            created_at = _aware_utc(tweet.created_at)
            if created_at < self.connected_at - timedelta(seconds=self.warmup_grace_seconds):
                self.stats.warmup_ignored += 1
                return
        if tweet.post_type == "retweet":
            self.stats.retweets_ignored += 1
            return
        fixed_kols = SocialWatchRegistry.fixed_kol_usernames(self._current_accounts)
        try:
            created = await asyncio.to_thread(
                self.event_service.create_event_if_relevant,
                tweet,
                rule_id=rule_id,
                rule_tag=rule_tag,
                known_kol_usernames=fixed_kols,
            )
        except Exception as exc:
            self.stats.provider_errors += 1
            logger.warning("Social event ingestion failed tweet_id=%s: %s", tweet.tweet_id, exc)
            return
        if not created:
            self.stats.unmatched_tweets += 1
            return
        self.last_tweet_at = tweet.created_at
        self.stats.social_events_created += len(created)
        for event in created:
            if event.author_type == AUTHOR_KOL:
                self.stats.kol_events += 1
            elif event.author_type == AUTHOR_PROJECT_X:
                self.stats.project_events += 1
            elif event.author_type == AUTHOR_DEV_X:
                self.stats.dev_x_events += 1

    async def _stream_loop(self) -> None:
        backoff_index = 0
        while not self._stopping:
            try:
                await self._connect_once()
                backoff_index = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stopping:
                    break
                self.stats.disconnects += 1
                self._record_disconnected(utc_now())
                self.connection_state = STREAM_BACKOFF
                logger.warning("TwitterAPI.io social stream disconnected: %s", _mask_secret(str(exc), self.api_key))
                delay = self.backoff_seconds[min(backoff_index, len(self.backoff_seconds) - 1)]
                backoff_index += 1
                self.stats.reconnects += 1
                await asyncio.sleep(delay)
        self.connection_state = STREAM_DISCONNECTED

    async def _rule_refresh_loop(self) -> None:
        while not self._stopping:
            try:
                await self.refresh_rule()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stats.provider_errors += 1
                logger.warning("TwitterAPI.io social rule refresh failed: %s", _mask_secret(str(exc), self.api_key))
            try:
                await asyncio.sleep(self.rule_refresh_seconds)
            except asyncio.CancelledError:
                raise

    async def _connect_once(self) -> None:
        self.connection_state = STREAM_CONNECTING
        await asyncio.to_thread(self._blocking_websocket_loop)

    def _blocking_websocket_loop(self) -> None:
        try:
            import websocket
        except ImportError as exc:
            raise TwitterApiIoError("websocket-client dependency is required for social stream") from exc
        ws = websocket.create_connection(
            self.ws_url,
            header=[f"x-api-key: {self.api_key}"],
            timeout=5,
        )
        self._ws = ws
        try:
            self._record_connected(utc_now())
            while not self._stopping:
                try:
                    message = ws.recv()
                except (TimeoutError, websocket.WebSocketTimeoutException):
                    continue
                if not message:
                    continue
                loop = self._loop
                if loop is None:
                    continue
                future = asyncio.run_coroutine_threadsafe(
                    self.handle_message(message, received_at=utc_now()),
                    loop,
                )
                future.result(timeout=30)
        finally:
            self._ws = None
            try:
                ws.close()
            except Exception:
                pass

    def _remember_tweet_id(
        self,
        tweet_id: str,
        *,
        tweet: NormalizedTweet | None = None,
        rule_id: str | None = None,
        rule_tag: str | None = None,
        received_at: datetime | None = None,
        batch_id: int | None = None,
    ) -> bool:
        if tweet_id in self._seen_tweet_ids:
            self._seen_tweet_ids.move_to_end(tweet_id)
            return False
        self._seen_tweet_ids[tweet_id] = None
        if tweet is not None and received_at is not None:
            self._tweet_delivery_index[tweet_id] = {
                "author_id": tweet.author_id,
                "rule_id": rule_id,
                "rule_tag": rule_tag,
                "received_at": received_at,
                "batch_id": batch_id,
                "connection_id": self._current_connection_id,
            }
        while len(self._seen_tweet_ids) > self.max_runtime_dedupe_ids:
            dropped_id, _ = self._seen_tweet_ids.popitem(last=False)
            self._tweet_delivery_index.pop(dropped_id, None)
        return True

    def _record_duplicate_tweet(
        self,
        tweet: NormalizedTweet,
        *,
        rule_id: str | None,
        rule_tag: str | None,
        received_at: datetime,
        batch_id: int | None,
    ) -> None:
        first = self._tweet_delivery_index.get(tweet.tweet_id) or {}
        self._duplicate_samples.append(
            DuplicateTweetDiagnostic(
                tweet_id=tweet.tweet_id,
                author_id=tweet.author_id,
                received_at=received_at,
                rule_id=rule_id,
                rule_tag=rule_tag,
                first_rule_id=first.get("rule_id") if isinstance(first.get("rule_id"), str) else None,
                first_rule_tag=first.get("rule_tag") if isinstance(first.get("rule_tag"), str) else None,
                same_connection=first.get("connection_id") == self._current_connection_id,
                same_payload_batch=first.get("batch_id") == batch_id,
                seconds_since_rule_refresh=_seconds_between(received_at, self.last_rule_refresh_at),
                seconds_since_rule_update=_seconds_between(received_at, self.last_rule_update_at),
            )
        )

    def _record_connected(self, connected_at: datetime) -> None:
        connected = _aware_utc(connected_at)
        if self._disconnect_gap_open and self.last_disconnected_at:
            gap = (connected - _aware_utc(self.last_disconnected_at)).total_seconds()
            if gap > 0:
                self.total_disconnect_seconds += gap
        self._disconnect_gap_open = False
        self.connection_state = STREAM_CONNECTED
        self.connected_at = connected
        self.last_connected_at = connected
        self._connection_sequence += 1
        self._current_connection_id = self._connection_sequence

    def _record_disconnected(self, disconnected_at: datetime) -> None:
        disconnected = _aware_utc(disconnected_at)
        self.last_disconnected_at = disconnected
        self._disconnect_gap_open = True

    def stats_snapshot(self) -> dict[str, object]:
        return {
            "connection_state": self.connection_state,
            "connected_at": _iso_or_none(self.connected_at),
            "last_connected_at": _iso_or_none(self.last_connected_at),
            "last_disconnected_at": _iso_or_none(self.last_disconnected_at),
            "last_message_at": _iso_or_none(self.last_message_at),
            "last_tweet_at": _iso_or_none(self.last_tweet_at),
            "messages_received": self.stats.messages_received,
            "tweets_received": self.stats.tweets_received,
            "unique_tweets_seen": self.stats.unique_tweets_seen,
            "warmup_ignored": self.stats.warmup_ignored,
            "retweets_ignored": self.stats.retweets_ignored,
            "unmatched_tweets": self.stats.unmatched_tweets,
            "social_events_created": self.stats.social_events_created,
            "kol_events": self.stats.kol_events,
            "project_events": self.stats.project_events,
            "dev_x_events": self.stats.dev_x_events,
            "duplicates": self.stats.duplicates,
            "provider_errors": self.stats.provider_errors,
            "disconnects": self.stats.disconnects,
            "reconnects": self.stats.reconnects,
            "rule_updates": self.stats.rule_updates,
            "social_x_rule_updates": self.stats.rule_updates,
            "social_x_rule_reconcile_runs": self.stats.rule_reconcile_runs,
            "social_x_rule_creates": self.stats.rule_creates,
            "social_x_rule_deactivations": self.stats.rule_deactivations,
            "social_x_rule_unchanged": self.stats.rule_unchanged,
            "filter_api_calls": self.stats.filter_api_calls,
            "rule_refresh_checks": self.stats.rule_refresh_checks,
            "registry_changes": self.stats.registry_changes,
            "social_x_registry_changes": self.stats.registry_changes,
            "social_x_shards_changed": self.stats.shards_changed,
            "rule_updates_per_registry_change": _ratio(self.stats.rule_updates, self.stats.registry_changes),
            "social_x_rule_updates_per_registry_change": _ratio(self.stats.rule_updates, self.stats.registry_changes),
            "total_disconnect_seconds": self.total_disconnect_seconds,
            "runtime_dedupe_size": len(self._seen_tweet_ids),
            "stream_duplicate_ratio": _ratio(self.stats.duplicates, self.stats.tweets_received),
            "stream_unique_ratio": _ratio(self.stats.unique_tweets_seen, self.stats.tweets_received),
            "duplicate_samples": [sample.to_dict() for sample in self._duplicate_samples],
            "rule_reconcile_events": self.rule_manager.recent_reconcile_events()
            if hasattr(self.rule_manager, "recent_reconcile_events")
            else [],
            "author_cashtag_shadow": self.author_cashtag_shadow.stats_snapshot()
            if self.author_cashtag_shadow is not None
            else None,
        }


def parse_stream_message(message: str | bytes, *, received_at: datetime | None = None) -> TwitterApiIoStreamMessage:
    payload = _parse_json_message(message)
    if not isinstance(payload, dict):
        raise TwitterApiIoError("malformed websocket message")
    event_type = _str_or_none(_first(payload, "event_type", "eventType", "type"))
    tweets_payload = _extract_stream_tweets(payload)
    if not event_type and tweets_payload:
        event_type = "tweet"
    event_type = event_type or "unknown"
    tweets = []
    if event_type == "tweet":
        detected = received_at or utc_now()
        tweets = normalize_tweets_from_response({"tweets": tweets_payload}, detected_at=detected)
    return TwitterApiIoStreamMessage(
        event_type=event_type,
        rule_id=_str_or_none(_first(payload, "rule_id", "ruleId")),
        rule_tag=_str_or_none(_first(payload, "rule_tag", "ruleTag", "tag")),
        tweets=tweets,
        snow_delay_ms=_float_or_none(_first(payload, "snow_delay_ms", "snowDelayMs")),
    )


def build_rule_shards(
    accounts: list[SocialWatchAccount],
    *,
    tag_prefix: str = SOCIAL_RULE_TAG_PREFIX,
    max_value_chars: int = DEFAULT_RULE_MAX_VALUE_CHARS,
) -> list[tuple[str, str]]:
    usernames = sorted({account.username for account in accounts if account.username}, key=str.lower)
    shards: list[str] = []
    current = ""
    limit = max(1, min(max_value_chars, 254))
    for username in usernames:
        atom = f"from:{username}"
        if len(atom) > limit:
            raise TwitterApiIoError(f"TwitterAPI.io rule atom too long username={username}")
        candidate = atom if not current else f"{current} OR {atom}"
        if len(candidate) > limit:
            shards.append(current)
            current = atom
        else:
            current = candidate
    if current:
        shards.append(current)
    return [(f"{tag_prefix}{index:03d}", value) for index, value in enumerate(shards, start=1)]


def build_stable_rule_shards(
    accounts: list[SocialWatchAccount],
    *,
    store: RuleShardStateStore,
    tag_prefix: str = SOCIAL_RULE_TAG_PREFIX,
    max_value_chars: int = DEFAULT_RULE_MAX_VALUE_CHARS,
) -> tuple[list[tuple[str, str]], bool]:
    desired_usernames = _unique_account_usernames(accounts)
    desired_keys = set(desired_usernames)
    state = store.load()
    state = {tag: list(usernames) for tag, usernames in sorted(state.items(), key=lambda item: _shard_sort_key(item[0]))}
    original_state = {tag: list(usernames) for tag, usernames in state.items()}
    assigned_keys: set[str] = set()
    for tag in list(state):
        kept: list[str] = []
        for username in state[tag]:
            key = username.lower()
            if key not in desired_keys or key in assigned_keys:
                continue
            kept.append(desired_usernames[key])
            assigned_keys.add(key)
        state[tag] = kept
    new_keys = sorted(desired_keys - assigned_keys)
    limit = max(1, min(max_value_chars, 254))
    for key in new_keys:
        username = desired_usernames[key]
        placed = False
        for tag in sorted(state, key=_shard_sort_key):
            candidate = [*state[tag], username]
            if _rule_value_len(candidate) <= limit:
                state[tag] = candidate
                placed = True
                break
        if placed:
            continue
        tag = _next_shard_tag(state, tag_prefix)
        if _rule_value_len([username]) > limit:
            raise TwitterApiIoError(f"TwitterAPI.io rule atom too long username={username}")
        state[tag] = [username]
    if not state and desired_usernames:
        state[f"{tag_prefix}001"] = []
    state_changed = state != original_state
    if state_changed:
        store.save(state)
    desired = [(tag, _rule_value(usernames)) for tag, usernames in sorted(state.items(), key=lambda item: _shard_sort_key(item[0])) if usernames]
    return desired, state_changed


def _find_rule(data: dict[str, Any], tag: str) -> TwitterApiIoRule | None:
    for row in _extract_rules(data):
        if _str_or_none(row.get("tag")) != tag:
            continue
        rule_id = _rule_id(row)
        if not rule_id:
            continue
        return TwitterApiIoRule(
            rule_id=rule_id,
            tag=tag,
            value=_str_or_none(row.get("value")) or "",
            interval_seconds=float(row.get("interval_seconds") or row.get("intervalSeconds") or 60),
            is_effect=_bool_effect(row.get("is_effect")),
        )
    return None


def _managed_rules(data: dict[str, Any], tag_prefix: str, legacy_tag: str) -> dict[str, TwitterApiIoRule]:
    rules: dict[str, TwitterApiIoRule] = {}
    for row in _extract_rules(data):
        tag = _str_or_none(row.get("tag"))
        if tag != legacy_tag and not (tag and tag.startswith(tag_prefix)):
            continue
        rule_id = _rule_id(row)
        if not rule_id or not tag:
            continue
        rules[tag] = TwitterApiIoRule(
            rule_id=rule_id,
            tag=tag,
            value=_str_or_none(row.get("value")) or "",
            interval_seconds=float(row.get("interval_seconds") or row.get("intervalSeconds") or 60),
            is_effect=_bool_effect(row.get("is_effect")),
        )
    return rules


def _extract_rules(data: dict[str, Any]) -> list[dict[str, Any]]:
    rules = data.get("rules")
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("rules"), list):
        return [rule for rule in nested["rules"] if isinstance(rule, dict)]
    return []


def _rule_id(data: dict[str, Any]) -> str | None:
    return _str_or_none(_first(data, "rule_id", "ruleId", "id"))


def _bool_effect(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _same_interval(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) < 0.001


def _unique_account_usernames(accounts: list[SocialWatchAccount]) -> dict[str, str]:
    usernames: dict[str, str] = {}
    for account in accounts:
        username = (account.username or "").strip()
        if not username:
            continue
        key = username.lower()
        usernames.setdefault(key, username)
    return usernames


def _rule_value(usernames: list[str]) -> str:
    return " OR ".join(f"from:{username}" for username in usernames)


def _usernames_from_rule_value(value: str) -> list[str]:
    usernames: list[str] = []
    for atom in value.split(" OR "):
        if atom.startswith("from:"):
            usernames.append(atom.removeprefix("from:"))
    return usernames


def _rule_value_len(usernames: list[str]) -> int:
    return len(_rule_value(usernames))


def _rule_username_count(value: str) -> int:
    if not value:
        return 0
    return len(_usernames_from_rule_value(value))


def _shard_sort_key(tag: str) -> tuple[int, str]:
    suffix = tag.rsplit("-", 1)[-1]
    try:
        return int(suffix), tag
    except ValueError:
        return 0, tag


def _next_shard_tag(state: dict[str, list[str]], tag_prefix: str) -> str:
    next_index = 1
    if state:
        next_index = max(_shard_sort_key(tag)[0] for tag in state) + 1
    return f"{tag_prefix}{next_index:03d}"


def _rule_update_reason(rule: TwitterApiIoRule, desired_value: str, interval_seconds: float) -> str | None:
    reasons = []
    if rule.value != desired_value:
        reasons.append("value_changed")
    if not rule.is_effect:
        reasons.append("inactive")
    if not _same_interval(rule.interval_seconds, interval_seconds):
        reasons.append("interval_changed")
    return "+".join(reasons) if reasons else None


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


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _mask_secret(text: str, secret: str) -> str:
    return text.replace(secret, "***") if secret else text


def _seconds_between(later: datetime, earlier: datetime | None) -> float | None:
    if earlier is None:
        return None
    return round((_aware_utc(later) - _aware_utc(earlier)).total_seconds(), 3)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)
