from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialKOLProfile, TokenWatchState
from app.services.social_kol_service import MANUAL_EXCLUDE, STATUS_QUALIFIED
from app.services.social_token_match_policy import (
    MATCH_STATUS_AMBIGUOUS_SYMBOL,
    MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR,
    MATCH_STATUS_UNMATCHED,
    MATCH_TYPE_CASHTAG,
    MATCH_TYPE_EXACT_CA,
    MATCH_TYPE_EXACT_CA_AND_CASHTAG,
    WatchedTokenIdentity,
    kol_distinct_author_key,
    match_social_tokens,
)
from app.services.twitterapi_io_client import NormalizedTweet, TwitterApiIoClient
from app.services.twitterapi_io_social_ingestion import (
    DEFAULT_FILTER_INTERVAL_SECONDS,
    DEFAULT_RULE_MAX_VALUE_CHARS,
    TWITTERAPI_IO_WS_URL,
    TwitterApiIoError,
    TwitterApiIoRule,
    parse_stream_message,
)
from app.utils.address import is_valid_evm_address, normalize_evm_address
from app.utils.time import utc_now

TOKEN_SHADOW_RULE_TAG_PREFIX = "wallet-agent-token-shadow-v01-"
TOKEN_SHADOW_CA_RULE_TAG_PREFIX = f"{TOKEN_SHADOW_RULE_TAG_PREFIX}ca-"
TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX = f"{TOKEN_SHADOW_RULE_TAG_PREFIX}cashtag-"
DEFAULT_TOKEN_SHADOW_SHARD_STATE_PATH = Path("data/twitter_token_shadow_rule_shards.json")
DEFAULT_TOKEN_SHADOW_PAUSE_STATE_PATH = Path("data/twitter_token_shadow_paused_rules.json")
QUOTE_ASSET_SYMBOLS = {"ETH", "WETH", "USDC", "USDT", "USDG"}


@dataclass(frozen=True)
class TokenShadowDesiredRules:
    watched_tokens: list[WatchedTokenIdentity]
    ca_terms: list[str]
    cashtag_terms: list[str]


@dataclass(frozen=True)
class TokenShadowRuleReconcileResult:
    rules: list[TwitterApiIoRule]
    provider_changed: bool
    provider_changed_at: datetime | None
    created_count: int
    updated_count: int
    deactivated_count: int
    unchanged_count: int
    ca_shard_count: int
    cashtag_shard_count: int


@dataclass
class TokenShadowStats:
    provider_messages: int = 0
    tweet_deliveries: int = 0
    unique_tweets: int = 0
    duplicates: int = 0
    startup_warmup_deliveries: int = 0
    steady_deliveries: int = 0
    steady_unique: int = 0
    steady_duplicates: int = 0
    retweets_received: int = 0
    retweets_ignored: int = 0
    exact_ca_matches: int = 0
    exact_ca_and_cashtag_matches: int = 0
    cashtag_matches: int = 0
    ambiguous_symbol_hits: int = 0
    unqualified_author_hits: int = 0
    unmatched_hits: int = 0
    matched_tweet_hits: int = 0
    steady_matched_tweet_hits: int = 0
    steady_qualified_kol_tweet_hits: int = 0
    qualified_kol_tweet_hits: int = 0
    token_match_results: int = 0
    rule_reconcile_runs: int = 0
    rule_creates: int = 0
    rule_updates: int = 0
    rule_deactivations: int = 0
    rule_unchanged: int = 0
    watched_token_changes: int = 0
    ca_shards_changed: int = 0
    cashtag_shards_changed: int = 0
    provider_errors: int = 0
    _seen_tweets: set[str] = field(default_factory=set)
    _steady_seen_tweets: set[str] = field(default_factory=set)
    _qualified_author_keys: set[str] = field(default_factory=set)
    _token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _tokens_with_kol_hits: set[str] = field(default_factory=set)

    def snapshot(self) -> dict[str, Any]:
        return {
            "provider_messages": self.provider_messages,
            "tweet_deliveries": self.tweet_deliveries,
            "unique_tweets": self.unique_tweets,
            "duplicates": self.duplicates,
            "duplicate_ratio": _ratio(self.duplicates, self.tweet_deliveries),
            "startup_warmup_deliveries": self.startup_warmup_deliveries,
            "steady_deliveries": self.steady_deliveries,
            "steady_unique": self.steady_unique,
            "steady_duplicates": self.steady_duplicates,
            "steady_duplicate_ratio": _ratio(self.steady_duplicates, self.steady_deliveries),
            "retweets_received": self.retweets_received,
            "retweets_ignored": self.retweets_ignored,
            "exact_ca_matches": self.exact_ca_matches,
            "exact_ca_and_cashtag_matches": self.exact_ca_and_cashtag_matches,
            "cashtag_matches": self.cashtag_matches,
            "ambiguous_symbol_hits": self.ambiguous_symbol_hits,
            "unqualified_author_hits": self.unqualified_author_hits,
            "unmatched_hits": self.unmatched_hits,
            "matched_tweet_hits": self.matched_tweet_hits,
            "steady_matched_tweet_hits": self.steady_matched_tweet_hits,
            "steady_qualified_kol_tweet_hits": self.steady_qualified_kol_tweet_hits,
            "qualified_kol_tweet_hits": self.qualified_kol_tweet_hits,
            "token_match_results": self.token_match_results,
            "distinct_qualified_kol_authors": len(self._qualified_author_keys),
            "token_kol_pairs": len(self._token_kol_pairs),
            "tokens_with_kol_hits": len(self._tokens_with_kol_hits),
            "qualified_kol_match_rate": _ratio(self.qualified_kol_tweet_hits, self.unique_tweets),
            "token_match_rate": _ratio(self.matched_tweet_hits, self.unique_tweets),
            "steady_token_match_rate": _ratio(self.steady_matched_tweet_hits, self.steady_unique),
            "steady_qualified_kol_match_rate": _ratio(self.steady_qualified_kol_tweet_hits, self.steady_unique),
            "rule_reconcile_runs": self.rule_reconcile_runs,
            "rule_creates": self.rule_creates,
            "rule_updates": self.rule_updates,
            "rule_deactivations": self.rule_deactivations,
            "rule_unchanged": self.rule_unchanged,
            "watched_token_changes": self.watched_token_changes,
            "ca_shards_changed": self.ca_shards_changed,
            "cashtag_shards_changed": self.cashtag_shards_changed,
            "provider_errors": self.provider_errors,
        }


class TokenShadowShardStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self._memory_state: dict[str, dict[str, list[str]]] = {"ca": {}, "cashtag": {}}

    def load(self) -> dict[str, dict[str, list[str]]]:
        if self.path is None:
            return {group: {tag: list(terms) for tag, terms in shards.items()} for group, shards in self._memory_state.items()}
        if not self.path.exists():
            return {"ca": {}, "cashtag": {}}
        try:
            data = json.loads(self.path.read_text())
        except json.JSONDecodeError as exc:
            raise TwitterApiIoError(f"Twitter token shadow shard state is malformed path={self.path}") from exc
        if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("groups"), dict):
            raise TwitterApiIoError(f"Twitter token shadow shard state is malformed path={self.path}")
        state = {"ca": {}, "cashtag": {}}
        for group, prefix in (("ca", TOKEN_SHADOW_CA_RULE_TAG_PREFIX), ("cashtag", TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX)):
            shards = data["groups"].get(group, {})
            if not isinstance(shards, dict):
                raise TwitterApiIoError(f"Twitter token shadow shard state is malformed path={self.path}")
            for tag, terms in shards.items():
                if not isinstance(tag, str) or not tag.startswith(prefix) or not isinstance(terms, list):
                    raise TwitterApiIoError(f"Twitter token shadow shard state is malformed path={self.path}")
                rows = []
                seen = set()
                for term in terms:
                    if not isinstance(term, str) or not term.strip():
                        raise TwitterApiIoError(f"Twitter token shadow shard state is malformed path={self.path}")
                    clean = _normalize_term(term.strip(), group)
                    key = clean.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(clean)
                state[group][tag] = rows
        return state

    def save(self, state: dict[str, dict[str, list[str]]]) -> None:
        normalized = {
            group: {tag: list(terms) for tag, terms in sorted(shards.items())}
            for group, shards in sorted(state.items())
        }
        if self.path is None:
            self._memory_state = normalized
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"version": 1, "groups": normalized}, indent=2, sort_keys=True))


class TwitterTokenShadowRuleManager:
    def __init__(
        self,
        client: TwitterApiIoClient,
        *,
        interval_seconds: float = DEFAULT_FILTER_INTERVAL_SECONDS,
        max_value_chars: int = DEFAULT_RULE_MAX_VALUE_CHARS,
        shard_state_path: Path | str | None = None,
    ) -> None:
        self.client = client
        self.interval_seconds = max(60, interval_seconds)
        self.max_value_chars = max(1, min(max_value_chars, 254))
        self.shard_state_store = TokenShadowShardStateStore(
            Path(shard_state_path) if shard_state_path is not None else None
        )
        self._last_fingerprint: str | None = None

    async def ensure_rules(
        self,
        desired: TokenShadowDesiredRules,
        *,
        stats: TokenShadowStats | None = None,
    ) -> list[TwitterApiIoRule]:
        return (await self.reconcile_rules(desired, stats=stats)).rules

    async def reconcile_rules(
        self,
        desired: TokenShadowDesiredRules,
        *,
        stats: TokenShadowStats | None = None,
    ) -> TokenShadowRuleReconcileResult:
        fingerprint = self._fingerprint(desired)
        if self._last_fingerprint and self._last_fingerprint != fingerprint and stats:
            stats.watched_token_changes += 1
        ca_shards, ca_changed = build_stable_term_shards(
            desired.ca_terms,
            group="ca",
            tag_prefix=TOKEN_SHADOW_CA_RULE_TAG_PREFIX,
            store=self.shard_state_store,
            max_value_chars=self.max_value_chars,
        )
        cashtag_shards, cashtag_changed = build_stable_term_shards(
            desired.cashtag_terms,
            group="cashtag",
            tag_prefix=TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX,
            store=self.shard_state_store,
            max_value_chars=self.max_value_chars,
        )
        if stats:
            stats.rule_reconcile_runs += 1
            stats.ca_shards_changed += int(ca_changed)
            stats.cashtag_shards_changed += int(cashtag_changed)
        rules_data = await asyncio.to_thread(self.client.get_filter_rules)
        existing = _managed_token_shadow_rules(rules_data)
        managed = []
        desired_shards = [*ca_shards, *cashtag_shards]
        desired_tags = {tag for tag, _ in desired_shards}
        created_count = 0
        updated_count = 0
        deactivated_count = 0
        unchanged_count = 0
        provider_changed_at: datetime | None = None
        for tag, value in desired_shards:
            rule = existing.get(tag)
            if rule is None:
                created = await asyncio.to_thread(
                    self.client.add_filter_rule,
                    tag=tag,
                    value=value,
                    interval_seconds=self.interval_seconds,
                )
                if stats:
                    stats.rule_creates += 1
                created_count += 1
                provider_changed_at = provider_changed_at or utc_now()
                rule_id = _rule_id(created)
                if not rule_id:
                    raise TwitterApiIoError("Twitter token shadow add_rule did not return rule_id")
                rule = TwitterApiIoRule(rule_id, tag, value, self.interval_seconds, False)
            if _rule_update_reason(rule, value, self.interval_seconds):
                rule = await self._update_rule(rule, value, True, stats)
                updated_count += 1
                provider_changed_at = provider_changed_at or utc_now()
            else:
                if stats:
                    stats.rule_unchanged += 1
                unchanged_count += 1
            managed.append(rule)
        for tag, rule in existing.items():
            if tag in desired_tags:
                continue
            if rule.is_effect:
                await self._update_rule(rule, rule.value, False, stats)
                if stats:
                    stats.rule_deactivations += 1
                deactivated_count += 1
                provider_changed_at = provider_changed_at or utc_now()
            else:
                if stats:
                    stats.rule_unchanged += 1
                unchanged_count += 1
        self._last_fingerprint = fingerprint
        return TokenShadowRuleReconcileResult(
            rules=managed,
            provider_changed=bool(created_count or updated_count or deactivated_count),
            provider_changed_at=provider_changed_at,
            created_count=created_count,
            updated_count=updated_count,
            deactivated_count=deactivated_count,
            unchanged_count=unchanged_count,
            ca_shard_count=len(ca_shards),
            cashtag_shard_count=len(cashtag_shards),
        )

    async def _update_rule(
        self,
        rule: TwitterApiIoRule,
        value: str,
        is_effect: bool,
        stats: TokenShadowStats | None,
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
            stats.rule_updates += 1
        return TwitterApiIoRule(rule.rule_id, rule.tag, value, self.interval_seconds, is_effect)

    def _fingerprint(self, desired: TokenShadowDesiredRules) -> str:
        ca = ",".join(sorted(term.lower() for term in desired.ca_terms))
        cashtags = ",".join(sorted(term.upper() for term in desired.cashtag_terms))
        return f"ca={ca}|cashtag={cashtags}|interval={int(self.interval_seconds)}"


class TokenFirstRealtimeShadow:
    def __init__(
        self,
        *,
        session_factory: sessionmaker,
        warmup_seconds: int = 120,
        max_runtime_dedupe_ids: int = 10000,
    ) -> None:
        self.session_factory = session_factory
        self.warmup_seconds = max(0, warmup_seconds)
        self.max_runtime_dedupe_ids = max(1, max_runtime_dedupe_ids)
        self.stats = TokenShadowStats()
        self._seen_tweet_ids: OrderedDict[str, None] = OrderedDict()
        self.last_rule_changed_at: datetime | None = None

    def desired_rules(self) -> TokenShadowDesiredRules:
        watched_tokens = load_current_watched_token_identities(self.session_factory)
        ca_terms = sorted({token.normalized_contract_address for token in watched_tokens})
        cashtag_terms = sorted({f"${token.normalized_symbol}" for token in watched_tokens})
        return TokenShadowDesiredRules(watched_tokens, ca_terms, cashtag_terms)

    def handle_message(self, message: str | bytes, *, received_at: datetime | None = None) -> None:
        received = _aware_utc(received_at or utc_now())
        parsed = parse_stream_message(message, received_at=received)
        if parsed.event_type != "tweet":
            return
        self.stats.provider_messages += 1
        watched_tokens = self.desired_rules().watched_tokens
        for tweet in parsed.tweets:
            self._handle_tweet(tweet, watched_tokens, received_at=received)

    def _handle_tweet(
        self,
        tweet: NormalizedTweet,
        watched_tokens: list[WatchedTokenIdentity],
        *,
        received_at: datetime,
    ) -> None:
        self.stats.tweet_deliveries += 1
        warmup = self._is_warmup(received_at)
        if warmup:
            self.stats.startup_warmup_deliveries += 1
        else:
            self.stats.steady_deliveries += 1
        unique = self._remember_tweet_id(tweet.tweet_id)
        if unique:
            self.stats.unique_tweets += 1
            if not warmup:
                self.stats.steady_unique += 1
        else:
            self.stats.duplicates += 1
            if not warmup:
                self.stats.steady_duplicates += 1
            return
        if tweet.post_type == "retweet":
            self.stats.retweets_received += 1
            self.stats.retweets_ignored += 1
            return
        author_qualified = self._author_is_qualified_kol(tweet.author_id, tweet.author_username)
        results = match_social_tokens(tweet, watched_tokens, author_qualified_kol=author_qualified)
        any_matched = False
        any_qualified_kol_match = False
        for result in results:
            if result.match_status == MATCH_STATUS_AMBIGUOUS_SYMBOL:
                self.stats.ambiguous_symbol_hits += 1
                continue
            if result.match_status == MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR:
                self.stats.unqualified_author_hits += 1
                continue
            if result.match_status == MATCH_STATUS_UNMATCHED:
                self.stats.unmatched_hits += 1
                continue
            if not result.matched:
                continue
            any_matched = True
            self.stats.token_match_results += 1
            if result.match_type == MATCH_TYPE_EXACT_CA:
                self.stats.exact_ca_matches += 1
            elif result.match_type == MATCH_TYPE_EXACT_CA_AND_CASHTAG:
                self.stats.exact_ca_and_cashtag_matches += 1
            elif result.match_type == MATCH_TYPE_CASHTAG:
                self.stats.cashtag_matches += 1
            if author_qualified and result.author_identity_key and result.contract_address:
                any_qualified_kol_match = True
                self.stats._qualified_author_keys.add(result.author_identity_key)
                self.stats._token_kol_pairs.add((result.contract_address, result.author_identity_key))
                self.stats._tokens_with_kol_hits.add(result.contract_address)
        if any_matched:
            self.stats.matched_tweet_hits += 1
            if not warmup:
                self.stats.steady_matched_tweet_hits += 1
        if not any_matched and all(result.match_status != MATCH_STATUS_UNMATCHED for result in results):
            return
        if any_qualified_kol_match:
            self.stats.qualified_kol_tweet_hits += 1
            if not warmup:
                self.stats.steady_qualified_kol_tweet_hits += 1

    def _remember_tweet_id(self, tweet_id: str) -> bool:
        if tweet_id in self._seen_tweet_ids:
            self._seen_tweet_ids.move_to_end(tweet_id)
            return False
        self._seen_tweet_ids[tweet_id] = None
        while len(self._seen_tweet_ids) > self.max_runtime_dedupe_ids:
            self._seen_tweet_ids.popitem(last=False)
        return True

    def _is_warmup(self, received_at: datetime) -> bool:
        if not self.last_rule_changed_at:
            return False
        return received_at <= _aware_utc(self.last_rule_changed_at) + timedelta(seconds=self.warmup_seconds)

    def _author_is_qualified_kol(self, author_id: str | None, author_username: str | None) -> bool:
        username_key = kol_distinct_author_key(None, author_username)
        with session_scope(self.session_factory) as session:
            query = select(SocialKOLProfile).where(
                SocialKOLProfile.status == STATUS_QUALIFIED,
                SocialKOLProfile.is_active.is_(True),
                SocialKOLProfile.manual_override != MANUAL_EXCLUDE,
            )
            identity_filters = []
            if author_id:
                identity_filters.append(SocialKOLProfile.x_author_id == str(author_id))
            if username_key:
                identity_filters.append(SocialKOLProfile.normalized_username == username_key)
            if not identity_filters:
                return False
            return session.scalar(query.where(or_(*identity_filters))) is not None

    def stats_snapshot(self) -> dict[str, Any]:
        return self.stats.snapshot()


def load_current_watched_token_identities(session_factory: sessionmaker) -> list[WatchedTokenIdentity]:
    with session_scope(session_factory) as session:
        rows = list(
            session.scalars(
                select(TokenWatchState)
                .where(TokenWatchState.active.is_(True))
                .order_by(TokenWatchState.id.asc())
            )
        )
    tokens = []
    seen = set()
    for row in rows:
        symbol = (row.symbol or "").strip()
        token_address = (row.token_address or "").strip()
        if not symbol or symbol.upper() in QUOTE_ASSET_SYMBOLS or not is_valid_evm_address(token_address):
            continue
        key = (row.chain, normalize_evm_address(token_address))
        if key in seen:
            continue
        seen.add(key)
        tokens.append(WatchedTokenIdentity(row.chain, token_address, symbol))
    return tokens


def build_stable_term_shards(
    terms: list[str],
    *,
    group: str,
    tag_prefix: str,
    store: TokenShadowShardStateStore,
    max_value_chars: int = DEFAULT_RULE_MAX_VALUE_CHARS,
) -> tuple[list[tuple[str, str]], bool]:
    desired_terms = {_normalize_term(term, group).lower(): _normalize_term(term, group) for term in terms if term}
    state = store.load()
    group_state = {tag: list(values) for tag, values in sorted(state.get(group, {}).items(), key=lambda item: _shard_sort_key(item[0]))}
    original_group_state = {tag: list(values) for tag, values in group_state.items()}
    assigned = set()
    for tag in list(group_state):
        kept = []
        for term in group_state[tag]:
            key = _normalize_term(term, group).lower()
            if key not in desired_terms or key in assigned:
                continue
            kept.append(desired_terms[key])
            assigned.add(key)
        group_state[tag] = kept
    limit = max(1, min(max_value_chars, 254))
    for key in sorted(set(desired_terms) - assigned):
        term = desired_terms[key]
        placed = False
        for tag in sorted(group_state, key=_shard_sort_key):
            candidate = [*group_state[tag], term]
            if _rule_value_len(candidate) <= limit:
                group_state[tag] = candidate
                placed = True
                break
        if placed:
            continue
        if _rule_value_len([term]) > limit:
            raise TwitterApiIoError(f"Twitter token shadow rule atom too long term={term}")
        group_state[_next_shard_tag(group_state, tag_prefix)] = [term]
    state[group] = group_state
    changed = group_state != original_group_state
    if changed:
        store.save(state)
    return [
        (tag, _rule_value(values))
        for tag, values in sorted(group_state.items(), key=lambda item: _shard_sort_key(item[0]))
        if values
    ], changed


def _managed_token_shadow_rules(data: dict[str, Any]) -> dict[str, TwitterApiIoRule]:
    rules = {}
    for row in _extract_rules(data):
        tag = str(row.get("tag") or "")
        if not tag.startswith(TOKEN_SHADOW_RULE_TAG_PREFIX):
            continue
        rule_id = _rule_id(row)
        if not rule_id:
            continue
        rules[tag] = TwitterApiIoRule(
            rule_id,
            tag,
            str(row.get("value") or ""),
            _interval_seconds(row),
            _is_effect(row),
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
    for key in ("rule_id", "ruleId", "id"):
        value = data.get(key)
        if value:
            return str(value)
    return None


def _interval_seconds(rule: dict[str, Any]) -> float:
    value = rule.get("interval_seconds", rule.get("intervalSeconds", 60))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 60.0


def _is_effect(rule: dict[str, Any]) -> bool:
    value = rule.get("is_effect")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _same_interval(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) < 0.001


def _rule_update_reason(rule: TwitterApiIoRule, desired_value: str, interval_seconds: float) -> str | None:
    if rule.value != desired_value or not rule.is_effect or not _same_interval(rule.interval_seconds, interval_seconds):
        return "changed"
    return None


def _normalize_term(term: str, group: str) -> str:
    clean = term.strip()
    if group == "ca":
        return clean.lower()
    if group == "cashtag":
        return f"${clean.removeprefix('$').upper()}"
    return clean


def _rule_value(terms: list[str]) -> str:
    return " OR ".join(terms)


def _rule_value_len(terms: list[str]) -> int:
    return len(_rule_value(terms))


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


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)
