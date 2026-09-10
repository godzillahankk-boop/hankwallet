from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialKOLProfile
from app.services.social_identity_service import normalize_username
from app.services.social_kol_service import MANUAL_EXCLUDE, STATUS_QUALIFIED
from app.services.social_token_match_policy import (
    MATCH_STATUS_AMBIGUOUS_SYMBOL,
    MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR,
    MATCH_STATUS_UNMATCHED,
    MATCH_TYPE_CASHTAG,
    MATCH_TYPE_EXACT_CA,
    MATCH_TYPE_EXACT_CA_AND_CASHTAG,
    SocialTokenMatchResult,
    WatchedTokenIdentity,
    match_social_tokens,
)
from app.services.twitter_token_shadow import load_current_watched_token_identities
from app.services.twitterapi_io_client import NormalizedTweet
from app.services.twitterapi_io_social_ingestion import parse_stream_message
from app.utils.time import utc_now


@dataclass
class AuthorFirstCashtagShadowStats:
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
    matched_tweet_hits: int = 0
    token_match_results: int = 0
    exact_ca_matches: int = 0
    exact_ca_and_cashtag_matches: int = 0
    cashtag_matches: int = 0
    ambiguous_symbol_hits: int = 0
    unqualified_author_hits: int = 0
    unmatched_hits: int = 0
    exact_ca_matched_tweet_hits: int = 0
    cashtag_only_matched_tweet_hits: int = 0
    qualified_kol_tweet_hits: int = 0
    steady_matched_tweet_hits: int = 0
    steady_exact_ca_matched_tweet_hits: int = 0
    steady_cashtag_only_matched_tweet_hits: int = 0
    steady_qualified_kol_tweet_hits: int = 0
    _qualified_author_keys: set[str] = field(default_factory=set)
    _token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _tokens_with_kol_hits: set[str] = field(default_factory=set)
    _exact_ca_token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _cashtag_only_token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _steady_token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _steady_exact_ca_token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)
    _steady_cashtag_only_token_kol_pairs: set[tuple[str, str]] = field(default_factory=set)

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
            "matched_tweet_hits": self.matched_tweet_hits,
            "token_match_results": self.token_match_results,
            "exact_ca_matches": self.exact_ca_matches,
            "exact_ca_and_cashtag_matches": self.exact_ca_and_cashtag_matches,
            "cashtag_matches": self.cashtag_matches,
            "ambiguous_symbol_hits": self.ambiguous_symbol_hits,
            "unqualified_author_hits": self.unqualified_author_hits,
            "unmatched_hits": self.unmatched_hits,
            "exact_ca_matched_tweet_hits": self.exact_ca_matched_tweet_hits,
            "cashtag_only_matched_tweet_hits": self.cashtag_only_matched_tweet_hits,
            "qualified_kol_tweet_hits": self.qualified_kol_tweet_hits,
            "steady_matched_tweet_hits": self.steady_matched_tweet_hits,
            "steady_exact_ca_matched_tweet_hits": self.steady_exact_ca_matched_tweet_hits,
            "steady_cashtag_only_matched_tweet_hits": self.steady_cashtag_only_matched_tweet_hits,
            "steady_qualified_kol_tweet_hits": self.steady_qualified_kol_tweet_hits,
            "distinct_qualified_kol_authors": len(self._qualified_author_keys),
            "token_kol_pairs": len(self._token_kol_pairs),
            "tokens_with_kol_hits": len(self._tokens_with_kol_hits),
            "exact_ca_token_kol_pairs": len(self._exact_ca_token_kol_pairs),
            "cashtag_only_token_kol_pairs": len(self._cashtag_only_token_kol_pairs),
            "steady_token_kol_pairs": len(self._steady_token_kol_pairs),
            "steady_exact_ca_token_kol_pairs": len(self._steady_exact_ca_token_kol_pairs),
            "steady_cashtag_only_token_kol_pairs": len(self._steady_cashtag_only_token_kol_pairs),
            "token_match_rate": _ratio(self.matched_tweet_hits, self.unique_tweets),
            "qualified_kol_match_rate": _ratio(self.qualified_kol_tweet_hits, self.unique_tweets),
            "steady_token_match_rate": _ratio(self.steady_matched_tweet_hits, self.steady_unique),
            "steady_qualified_kol_match_rate": _ratio(
                self.steady_qualified_kol_tweet_hits,
                self.steady_unique,
            ),
        }


class AuthorFirstCashtagShadow:
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
        self.stats = AuthorFirstCashtagShadowStats()
        self._seen_tweet_ids: OrderedDict[str, None] = OrderedDict()
        self.last_rule_changed_at: datetime | None = None

    def handle_message(self, message: str | bytes, *, received_at: datetime | None = None) -> None:
        received = _aware_utc(received_at or utc_now())
        parsed = parse_stream_message(message, received_at=received)
        if parsed.event_type != "tweet":
            return
        self.observe_stream_tweets(parsed.tweets, received_at=received)

    def observe_stream_tweets(self, tweets: list[NormalizedTweet], *, received_at: datetime | None = None) -> None:
        self.stats.provider_messages += 1
        watched_tokens = load_current_watched_token_identities(self.session_factory)
        received = _aware_utc(received_at or utc_now())
        for tweet in tweets:
            self.observe_tweet(tweet, watched_tokens=watched_tokens, received_at=received)

    def observe_tweet(
        self,
        tweet: NormalizedTweet,
        *,
        watched_tokens: list[WatchedTokenIdentity] | None = None,
        received_at: datetime | None = None,
    ) -> None:
        received = _aware_utc(received_at or utc_now())
        self.stats.tweet_deliveries += 1
        warmup = self._is_warmup(received)
        if warmup:
            self.stats.startup_warmup_deliveries += 1
        else:
            self.stats.steady_deliveries += 1
        if not self._remember_tweet_id(tweet.tweet_id):
            self.stats.duplicates += 1
            if not warmup:
                self.stats.steady_duplicates += 1
            return
        self.stats.unique_tweets += 1
        if not warmup:
            self.stats.steady_unique += 1
        if tweet.post_type == "retweet":
            self.stats.retweets_received += 1
            self.stats.retweets_ignored += 1
            return
        tokens = watched_tokens if watched_tokens is not None else load_current_watched_token_identities(self.session_factory)
        author_qualified = self._author_is_qualified_kol(tweet.author_id, tweet.author_username)
        results = match_social_tokens(tweet, tokens, author_qualified_kol=author_qualified)
        self._record_match_results(results, author_qualified=author_qualified, warmup=warmup)

    def _record_match_results(
        self,
        results: list[SocialTokenMatchResult],
        *,
        author_qualified: bool,
        warmup: bool,
    ) -> None:
        has_match = False
        has_exact_match = False
        has_cashtag_match = False
        has_qualified_kol_match = False
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
            has_match = True
            self.stats.token_match_results += 1
            if result.match_type == MATCH_TYPE_EXACT_CA:
                has_exact_match = True
                self.stats.exact_ca_matches += 1
            elif result.match_type == MATCH_TYPE_EXACT_CA_AND_CASHTAG:
                has_exact_match = True
                self.stats.exact_ca_and_cashtag_matches += 1
            elif result.match_type == MATCH_TYPE_CASHTAG:
                has_cashtag_match = True
                self.stats.cashtag_matches += 1
            if author_qualified and result.author_identity_key and result.contract_address:
                pair = (result.contract_address, result.author_identity_key)
                has_qualified_kol_match = True
                self.stats._qualified_author_keys.add(result.author_identity_key)
                self.stats._token_kol_pairs.add(pair)
                self.stats._tokens_with_kol_hits.add(result.contract_address)
                if result.match_type in {MATCH_TYPE_EXACT_CA, MATCH_TYPE_EXACT_CA_AND_CASHTAG}:
                    self.stats._exact_ca_token_kol_pairs.add(pair)
                    self.stats._cashtag_only_token_kol_pairs.discard(pair)
                    if not warmup:
                        self.stats._steady_exact_ca_token_kol_pairs.add(pair)
                        self.stats._steady_cashtag_only_token_kol_pairs.discard(pair)
                elif result.match_type == MATCH_TYPE_CASHTAG:
                    if pair not in self.stats._exact_ca_token_kol_pairs:
                        self.stats._cashtag_only_token_kol_pairs.add(pair)
                    if not warmup and pair not in self.stats._steady_exact_ca_token_kol_pairs:
                        self.stats._steady_cashtag_only_token_kol_pairs.add(pair)
                if not warmup:
                    self.stats._steady_token_kol_pairs.add(pair)
        if has_match:
            self.stats.matched_tweet_hits += 1
            if not warmup:
                self.stats.steady_matched_tweet_hits += 1
        if has_exact_match:
            self.stats.exact_ca_matched_tweet_hits += 1
            if not warmup:
                self.stats.steady_exact_ca_matched_tweet_hits += 1
        if has_cashtag_match and not has_exact_match:
            self.stats.cashtag_only_matched_tweet_hits += 1
            if not warmup:
                self.stats.steady_cashtag_only_matched_tweet_hits += 1
        if has_qualified_kol_match:
            self.stats.qualified_kol_tweet_hits += 1
            if not warmup:
                self.stats.steady_qualified_kol_tweet_hits += 1

    def _is_warmup(self, received_at: datetime) -> bool:
        if not self.last_rule_changed_at:
            return False
        return received_at <= _aware_utc(self.last_rule_changed_at) + timedelta(seconds=self.warmup_seconds)

    def _remember_tweet_id(self, tweet_id: str) -> bool:
        if tweet_id in self._seen_tweet_ids:
            self._seen_tweet_ids.move_to_end(tweet_id)
            return False
        self._seen_tweet_ids[tweet_id] = None
        while len(self._seen_tweet_ids) > self.max_runtime_dedupe_ids:
            self._seen_tweet_ids.popitem(last=False)
        return True

    def _author_is_qualified_kol(self, author_id: str | None, author_username: str | None) -> bool:
        normalized = normalize_username(author_username)
        with session_scope(self.session_factory) as session:
            base = select(SocialKOLProfile).where(
                SocialKOLProfile.status == STATUS_QUALIFIED,
                SocialKOLProfile.is_active.is_(True),
                SocialKOLProfile.manual_override != MANUAL_EXCLUDE,
            )
            if author_id:
                by_author_id = session.scalar(base.where(SocialKOLProfile.x_author_id == str(author_id)))
                if by_author_id is not None:
                    return True
                if not normalized:
                    return False
                username_profile = session.scalar(base.where(SocialKOLProfile.normalized_username == normalized))
                if username_profile is None:
                    return False
                return username_profile.x_author_id in {None, str(author_id)}
            if not normalized:
                return False
            return session.scalar(base.where(SocialKOLProfile.normalized_username == normalized)) is not None

    def stats_snapshot(self) -> dict[str, Any]:
        return self.stats.snapshot()


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
