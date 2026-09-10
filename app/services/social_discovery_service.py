from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import PriceSnapshot, SocialDiscoveryCursor, SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.price_quality import PRICE_QUALITY_VALID
from app.services.social_event_service import INGESTION_DISCOVERY, SocialEventService
from app.services.social_identity_service import DEV_X, PROJECT_X, normalize_username
from app.services.social_kol_service import (
    MIN_KOL_FOLLOWERS,
    OUTCOME_CACHED,
    OUTCOME_FAILED,
    OUTCOME_IGNORED_FOLLOWERS,
    OUTCOME_QUALIFIED,
    OUTCOME_REJECTED,
    OUTCOME_UNCERTAIN,
    SOURCE_SOCIAL_DISCOVERY,
    STATUS_QUALIFIED,
    KOLVerificationInput,
    KOLVerificationResult,
    SocialKOLClassifier,
    SocialKOLService,
    SocialPostSample,
    SocialProfileProvider,
    SocialProfileSnapshot,
)
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet
from app.utils.address import normalize_evm_address
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

DISCOVERY_QUERY_CA = "ca"
DISCOVERY_QUERY_CASHTAG = "cashtag"
SOCIAL_DISCOVERY_SOURCE_EVIDENCE = {"method": "token_ca_mention"}
DEFAULT_DISCOVERY_DUE_SECONDS = 900
DEFAULT_DISCOVERY_MAX_DUE_SECONDS = 14400
DEFAULT_DISCOVERY_BOOTSTRAP_LOOKBACK_SECONDS = 900
DISCOVERY_NO_NEW_BACKOFF_STEPS = (
    (1, 1800),
    (3, 3600),
    (6, 7200),
)


class SocialSearchClient(Protocol):
    def search_tweets(self, query: str, *, query_type: str = "Latest", cursor: str = "") -> list[NormalizedTweet]:
        ...


@dataclass(frozen=True)
class SocialDiscoveryQuery:
    kind: str
    query: str


@dataclass
class SocialDiscoveryStats:
    search_runs: int = 0
    search_api_calls: int = 0
    tokens_scanned: int = 0
    tweets_received: int = 0
    ca_matches: int = 0
    retweets_skipped: int = 0
    project_dev_skipped: int = 0
    low_follower_skipped: int = 0
    known_kol_hits: int = 0
    known_profile_cache_hits: int = 0
    candidates_created: int = 0
    candidates_existing: int = 0
    verification_queued: int = 0
    verification_attempted: int = 0
    verification_qualified: int = 0
    verification_rejected: int = 0
    verification_uncertain: int = 0
    verification_ignored_followers: int = 0
    verification_failed: int = 0
    kol_events_created: int = 0
    duplicates: int = 0
    provider_errors: int = 0
    profile_api_calls: int = 0
    recent_posts_api_calls: int = 0
    deepseek_calls: int = 0
    searches_returning_zero_new_authors: int = 0

    def snapshot(self) -> dict[str, int | float]:
        data: dict[str, int | float] = {
            "search_runs": self.search_runs,
            "search_api_calls": self.search_api_calls,
            "tokens_scanned": self.tokens_scanned,
            "tweets_received": self.tweets_received,
            "ca_matches": self.ca_matches,
            "retweets_skipped": self.retweets_skipped,
            "project_dev_skipped": self.project_dev_skipped,
            "low_follower_skipped": self.low_follower_skipped,
            "known_kol_hits": self.known_kol_hits,
            "known_profile_cache_hits": self.known_profile_cache_hits,
            "candidates_created": self.candidates_created,
            "candidates_existing": self.candidates_existing,
            "verification_queued": self.verification_queued,
            "verification_attempted": self.verification_attempted,
            "verification_qualified": self.verification_qualified,
            "verification_rejected": self.verification_rejected,
            "verification_uncertain": self.verification_uncertain,
            "verification_ignored_followers": self.verification_ignored_followers,
            "verification_failed": self.verification_failed,
            "kol_events_created": self.kol_events_created,
            "duplicates": self.duplicates,
            "provider_errors": self.provider_errors,
            "profile_api_calls": self.profile_api_calls,
            "recent_posts_api_calls": self.recent_posts_api_calls,
            "deepseek_calls": self.deepseek_calls,
            "searches_returning_zero_new_authors": self.searches_returning_zero_new_authors,
        }
        new_candidates = self.candidates_created
        data["discovery_tweets_per_new_candidate"] = _ratio(self.tweets_received, new_candidates)
        data["discovery_searches_per_new_candidate"] = _ratio(self.search_api_calls, new_candidates)
        data["discovery_searches_returning_zero_new_authors"] = self.searches_returning_zero_new_authors
        data["new_kol_per_deepseek_call"] = _ratio(self.verification_qualified, self.deepseek_calls)
        return data


@dataclass(frozen=True)
class WatchedTokenDiscoveryTarget:
    watch_state_id: int
    wallet_id: int
    chain: str
    token_address: str
    symbol: str | None
    watch_started_at: datetime
    usd_value: Decimal


@dataclass(frozen=True)
class DiscoveryProcessResult:
    accepted_count: int = 0
    new_discovery_author_hits: int = 0
    known_kol_event_hits: int = 0


class SocialDiscoveryService:
    def __init__(
        self,
        *,
        session_factory: sessionmaker,
        search_client: SocialSearchClient,
        kol_service: SocialKOLService,
        event_service: SocialEventService,
        due_seconds: int = DEFAULT_DISCOVERY_DUE_SECONDS,
        max_due_seconds: int = DEFAULT_DISCOVERY_MAX_DUE_SECONDS,
        bootstrap_lookback_seconds: int = DEFAULT_DISCOVERY_BOOTSTRAP_LOOKBACK_SECONDS,
        batch_size: int = 3,
        max_pages: int = 1,
        overlap_seconds: int = 120,
        min_usd_value: Decimal = Decimal("5"),
        cashtag_enabled: bool = False,
        profile_provider: SocialProfileProvider | None = None,
        classifier: SocialKOLClassifier | None = None,
        auto_verify_enabled: bool = False,
        verify_batch_size: int = 20,
        verify_max_concurrency: int = 3,
    ) -> None:
        self.session_factory = session_factory
        self.search_client = search_client
        self.kol_service = kol_service
        self.event_service = event_service
        self.due_seconds = max(1, due_seconds)
        self.max_due_seconds = max(self.due_seconds, max_due_seconds)
        self.bootstrap_lookback_seconds = max(1, bootstrap_lookback_seconds)
        self.batch_size = max(1, batch_size)
        if max_pages != 1:
            logger.warning("Social discovery max_pages=%s is not supported in V2B V1; clamped to 1", max_pages)
        self.max_pages = 1
        self.overlap_seconds = max(0, overlap_seconds)
        self.min_usd_value = min_usd_value
        self.cashtag_enabled = cashtag_enabled
        self.profile_provider = profile_provider
        self.classifier = classifier
        self.auto_verify_enabled = auto_verify_enabled
        self.verify_batch_size = max(1, verify_batch_size)
        self.verify_max_concurrency = max(1, verify_max_concurrency)
        self.stats = SocialDiscoveryStats()

    async def scan_due_tokens(self, *, now: datetime | None = None) -> SocialDiscoveryStats:
        run_now = _naive_utc(now or utc_now())
        self.stats.search_runs += 1
        targets = self.due_targets(now=run_now, limit=self.batch_size)
        pending_tweets: dict[int, list[NormalizedTweet]] = {}
        for target in targets:
            try:
                await self._scan_target(target, run_now, pending_tweets)
            except Exception as exc:
                self.stats.provider_errors += 1
                logger.warning(
                    "Social discovery token scan failed wallet_id=%s token=%s: %s",
                    target.wallet_id,
                    target.token_address,
                    exc,
                )
        await self.verify_due_candidates(pending_tweets)
        return self.stats

    def due_targets(self, *, now: datetime | None = None, limit: int | None = None) -> list[WatchedTokenDiscoveryTarget]:
        run_now = _naive_utc(now or utc_now())
        with session_scope(self.session_factory) as session:
            watches = list(
                session.scalars(
                    select(TokenWatchState)
                    .where(TokenWatchState.active.is_(True))
                    .order_by(TokenWatchState.id.asc())
                )
            )
            watch_ids = [watch.id for watch in watches]
            cursor_rows = []
            if watch_ids:
                cursor_rows = list(
                    session.scalars(
                        select(SocialDiscoveryCursor).where(
                            SocialDiscoveryCursor.watch_state_id.in_(watch_ids),
                            SocialDiscoveryCursor.query_kind == DISCOVERY_QUERY_CA,
                        )
                    )
                )
            cursors = {cursor.watch_state_id: cursor for cursor in cursor_rows}
            candidates: list[tuple[tuple[object, ...], WatchedTokenDiscoveryTarget]] = []
            for watch in watches:
                latest_price = _latest_valid_price_snapshot(session, watch)
                if not latest_price or latest_price.usd_value is None:
                    continue
                usd_value = Decimal(str(latest_price.usd_value))
                if usd_value < self.min_usd_value:
                    continue
                cursor = cursors.get(watch.id)
                if cursor and cursor.next_due_at > run_now:
                    continue
                if cursor is None:
                    scan_priority = 0
                elif cursor.last_success_at is None:
                    scan_priority = 1
                else:
                    scan_priority = 2
                target = WatchedTokenDiscoveryTarget(
                    watch_state_id=watch.id,
                    wallet_id=watch.wallet_id,
                    chain=watch.chain,
                    token_address=watch.token_address,
                    symbol=watch.symbol,
                    watch_started_at=watch.started_at,
                    usd_value=usd_value,
                )
                candidates.append(
                    (
                        (
                            scan_priority,
                            cursor.next_due_at if cursor else run_now,
                            cursor.last_success_at if cursor and cursor.last_success_at else datetime.min,
                            watch.id,
                        ),
                        target,
                    )
                )
            candidates.sort(key=lambda row: row[0])
            return [target for _, target in candidates[: (limit or self.batch_size)]]

    def stats_snapshot(self) -> dict[str, int | float]:
        return self.stats.snapshot()

    async def verify_due_candidates(
        self,
        pending_tweets: dict[int, list[NormalizedTweet]] | None = None,
    ) -> list[SocialKOLProfile | None]:
        if not self.auto_verify_enabled or not self.profile_provider or not self.classifier:
            return []
        due_ids = self.kol_service.due_profile_ids(limit=self.verify_batch_size)
        if not due_ids:
            return []
        self.stats.verification_queued += len(due_ids)
        self.stats.verification_attempted += len(due_ids)
        provider = _CountingProfileProvider(self.profile_provider, self.stats)
        classifier = _CountingClassifier(self.classifier, self.stats)
        outcomes = await self.kol_service.verify_candidates_with_outcome(
            due_ids,
            provider=provider,
            classifier=classifier,
            auto_verify_enabled=True,
            max_concurrency=self.verify_max_concurrency,
        )
        for outcome in outcomes:
            if outcome.outcome == OUTCOME_QUALIFIED:
                self.stats.verification_qualified += 1
                await self._promote_pending_tweets(
                    outcome.profile,
                    pending_tweets or {},
                    requested_profile_id=outcome.requested_profile_id,
                )
            elif outcome.outcome == OUTCOME_REJECTED:
                self.stats.verification_rejected += 1
            elif outcome.outcome == OUTCOME_UNCERTAIN:
                self.stats.verification_uncertain += 1
            elif outcome.outcome == OUTCOME_IGNORED_FOLLOWERS:
                self.stats.verification_ignored_followers += 1
            elif outcome.outcome == OUTCOME_FAILED:
                self.stats.verification_failed += 1
            elif outcome.outcome == OUTCOME_CACHED:
                self.stats.known_profile_cache_hits += 1
        return [outcome.profile for outcome in outcomes]

    async def _scan_target(
        self,
        target: WatchedTokenDiscoveryTarget,
        run_now: datetime,
        pending_tweets: dict[int, list[NormalizedTweet]],
    ) -> None:
        self.stats.tokens_scanned += 1
        queries = build_token_discovery_queries(
            token_address=target.token_address,
            symbol=target.symbol,
            cashtag_enabled=self.cashtag_enabled,
        )
        for query in queries:
            cursor = self._mark_attempt(target, query.kind, run_now)
            since = self._since_time(target, cursor, run_now)
            provider_query = build_incremental_discovery_query(query.query, since=since, until=run_now)
            try:
                self.stats.search_api_calls += 1
                tweets = await asyncio.to_thread(self.search_client.search_tweets, provider_query, query_type="Latest")
                self.stats.tweets_received += len(tweets)
                result = await self._process_tweets(target, tweets, since, pending_tweets)
                self._mark_success(target, query.kind, run_now, tweets, result)
            except Exception as exc:
                self._mark_failure(target, query.kind, run_now, exc)
                self.stats.provider_errors += 1
                logger.warning(
                    "Social discovery search failed wallet_id=%s token=%s query_kind=%s: %s",
                    target.wallet_id,
                    target.token_address,
                    query.kind,
                    exc,
                )

    async def _process_tweets(
        self,
        target: WatchedTokenDiscoveryTarget,
        tweets: list[NormalizedTweet],
        since: datetime,
        pending_tweets: dict[int, list[NormalizedTweet]],
    ) -> DiscoveryProcessResult:
        accepted = 0
        new_discovery_author_hits = 0
        known_kol_event_hits = 0
        identities = self._identity_usernames(target)
        for tweet in tweets:
            if not tweet.created_at:
                continue
            posted_at = _naive_utc(tweet.created_at)
            if posted_at < since:
                continue
            if tweet.post_type == "retweet":
                self.stats.retweets_skipped += 1
                continue
            if not _has_ca_match(tweet.token_matches, target.token_address):
                continue
            self.stats.ca_matches += 1
            accepted += 1
            author_key = (normalize_username(tweet.author_username) or "").lower()
            if author_key in identities:
                self.stats.project_dev_skipped += 1
                continue
            lookup = self.kol_service.profile_lookup_for_author(
                author_id=tweet.author_id,
                username=tweet.author_username,
            )
            if lookup.identity_conflict:
                logger.warning(
                    "Social discovery author identity conflict tweet_id=%s username=%s author_id=%s",
                    tweet.tweet_id,
                    tweet.author_username,
                    tweet.author_id,
                )
                continue
            profile = lookup.profile
            if profile and profile.status == STATUS_QUALIFIED and profile.is_active:
                self.stats.known_kol_hits += 1
                if self._create_kol_event(tweet):
                    known_kol_event_hits += 1
                continue
            if profile and not self.kol_service.needs_verification(profile):
                self.stats.known_profile_cache_hits += 1
                continue
            if not profile and tweet.author_followers is not None and tweet.author_followers < MIN_KOL_FOLLOWERS:
                self.stats.low_follower_skipped += 1
                continue
            if not tweet.author_username:
                continue
            was_existing = profile is not None
            profile = self.kol_service.observe_candidate(
                author_id=tweet.author_id,
                username=tweet.author_username,
                followers=tweet.author_followers,
                source=SOURCE_SOCIAL_DISCOVERY,
                source_evidence=SOCIAL_DISCOVERY_SOURCE_EVIDENCE,
            )
            if was_existing:
                self.stats.candidates_existing += 1
            else:
                self.stats.candidates_created += 1
                new_discovery_author_hits += 1
            if self.kol_service.needs_verification(profile):
                pending_tweets.setdefault(profile.id, []).append(tweet)
        return DiscoveryProcessResult(
            accepted_count=accepted,
            new_discovery_author_hits=new_discovery_author_hits,
            known_kol_event_hits=known_kol_event_hits,
        )

    async def _promote_pending_tweets(
        self,
        profile: SocialKOLProfile | None,
        pending_tweets: dict[int, list[NormalizedTweet]],
        *,
        requested_profile_id: int | None = None,
    ) -> None:
        if profile is None:
            return
        tweets = list(pending_tweets.get(profile.id, []))
        if requested_profile_id and requested_profile_id != profile.id:
            tweets.extend(pending_tweets.get(requested_profile_id, []))
        seen: set[str] = set()
        for tweet in tweets:
            if tweet.tweet_id in seen:
                continue
            seen.add(tweet.tweet_id)
            self._create_kol_event(tweet)

    def _create_kol_event(self, tweet: NormalizedTweet) -> bool:
        created = self.event_service.create_event_if_relevant(
            tweet,
            ingestion_type=INGESTION_DISCOVERY,
            known_kol_usernames={(normalize_username(tweet.author_username) or "").lower()},
            known_kol_author_ids={tweet.author_id} if tweet.author_id else set(),
        )
        if created:
            self.stats.kol_events_created += len(created)
            return True
        self.stats.duplicates += 1
        return False

    def _identity_usernames(self, target: WatchedTokenDiscoveryTarget) -> set[str]:
        with session_scope(self.session_factory) as session:
            rows = session.scalars(
                select(SocialIdentity).where(
                    SocialIdentity.chain == target.chain,
                    SocialIdentity.token_address == target.token_address,
                    SocialIdentity.identity_type.in_([PROJECT_X, DEV_X]),
                    SocialIdentity.is_active.is_(True),
                )
            )
            return {
                (normalize_username(identity.normalized_value) or identity.normalized_value).lower()
                for identity in rows
            }

    def _get_or_create_cursor(
        self,
        session,
        watch: TokenWatchState,
        query_kind: str,
        now: datetime,
    ) -> SocialDiscoveryCursor:
        cursor = session.scalar(
            select(SocialDiscoveryCursor).where(
                SocialDiscoveryCursor.watch_state_id == watch.id,
                SocialDiscoveryCursor.query_kind == query_kind,
            )
        )
        if cursor:
            return cursor
        cursor = SocialDiscoveryCursor(
            watch_state_id=watch.id,
            wallet_id=watch.wallet_id,
            chain=watch.chain,
            token_address=watch.token_address,
            query_kind=query_kind,
            next_due_at=now,
        )
        session.add(cursor)
        try:
            session.flush()
        except IntegrityError:
            session.rollback()
            cursor = session.scalar(
                select(SocialDiscoveryCursor).where(
                    SocialDiscoveryCursor.watch_state_id == watch.id,
                    SocialDiscoveryCursor.query_kind == query_kind,
                )
            )
            if cursor is None:
                raise
        return cursor

    def _mark_attempt(
        self,
        target: WatchedTokenDiscoveryTarget,
        query_kind: str,
        now: datetime,
    ) -> SocialDiscoveryCursor:
        with session_scope(self.session_factory) as session:
            watch = session.get(TokenWatchState, target.watch_state_id)
            if watch is None:
                raise RuntimeError("watch_state_missing")
            cursor = self._get_or_create_cursor(session, watch, query_kind, now)
            cursor.last_attempt_at = now
            cursor.updated_at = now
            session.flush()
            session.expunge(cursor)
            return cursor

    def _mark_success(
        self,
        target: WatchedTokenDiscoveryTarget,
        query_kind: str,
        now: datetime,
        tweets: list[NormalizedTweet],
        result: DiscoveryProcessResult,
    ) -> None:
        latest_seen = max((_naive_utc(tweet.created_at) for tweet in tweets if tweet.created_at), default=None)
        with session_scope(self.session_factory) as session:
            cursor = session.scalar(
                select(SocialDiscoveryCursor).where(
                    SocialDiscoveryCursor.watch_state_id == target.watch_state_id,
                    SocialDiscoveryCursor.query_kind == query_kind,
                )
            )
            if not cursor:
                return
            cursor.last_success_at = now
            cursor.latest_seen_posted_at = latest_seen or cursor.latest_seen_posted_at
            if result.new_discovery_author_hits > 0:
                cursor.consecutive_no_new_author_runs = 0
            else:
                cursor.consecutive_no_new_author_runs += 1
                self.stats.searches_returning_zero_new_authors += 1
            cursor.next_due_at = now + timedelta(
                seconds=self._next_due_seconds(cursor.consecutive_no_new_author_runs)
            )
            cursor.api_calls += 1
            cursor.last_error = None
            cursor.updated_at = now

    def _mark_failure(
        self,
        target: WatchedTokenDiscoveryTarget,
        query_kind: str,
        now: datetime,
        exc: Exception,
    ) -> None:
        with session_scope(self.session_factory) as session:
            cursor = session.scalar(
                select(SocialDiscoveryCursor).where(
                    SocialDiscoveryCursor.watch_state_id == target.watch_state_id,
                    SocialDiscoveryCursor.query_kind == query_kind,
                )
            )
            if not cursor:
                return
            cursor.next_due_at = now + timedelta(seconds=self.due_seconds)
            cursor.api_calls += 1
            cursor.error_count += 1
            cursor.last_error = str(exc)[:500]
            cursor.updated_at = now

    def _next_due_seconds(self, consecutive_no_new_author_runs: int) -> int:
        due_seconds = self.due_seconds
        for minimum_runs, candidate_due_seconds in DISCOVERY_NO_NEW_BACKOFF_STEPS:
            if consecutive_no_new_author_runs >= minimum_runs:
                due_seconds = max(self.due_seconds, candidate_due_seconds)
        return min(due_seconds, self.max_due_seconds)

    def _since_time(
        self,
        target: WatchedTokenDiscoveryTarget,
        cursor: SocialDiscoveryCursor,
        run_now: datetime,
    ) -> datetime:
        if cursor.last_success_at:
            return max(
                target.watch_started_at,
                cursor.last_success_at - timedelta(seconds=self.overlap_seconds),
            )
        return max(
            target.watch_started_at,
            run_now - timedelta(seconds=self.bootstrap_lookback_seconds),
        )


def build_token_discovery_queries(
    *,
    token_address: str,
    symbol: str | None,
    cashtag_enabled: bool = False,
) -> list[SocialDiscoveryQuery]:
    queries = [SocialDiscoveryQuery(DISCOVERY_QUERY_CA, normalize_evm_address(token_address))]
    if cashtag_enabled and symbol:
        clean = symbol.strip().upper()
        if clean:
            queries.append(SocialDiscoveryQuery(DISCOVERY_QUERY_CASHTAG, f"${clean}"))
    return queries


def build_incremental_discovery_query(base_query: str, *, since: datetime, until: datetime) -> str:
    since_epoch = int(_aware_utc(since).timestamp())
    until_epoch = int(_aware_utc(until).timestamp())
    if until_epoch <= since_epoch:
        until_epoch = since_epoch + 1
    return f"{base_query} since_time:{since_epoch} until_time:{until_epoch}"


def _latest_valid_price_snapshot(session, watch: TokenWatchState) -> PriceSnapshot | None:
    return session.scalar(
        select(PriceSnapshot)
        .where(
            PriceSnapshot.wallet_id == watch.wallet_id,
            PriceSnapshot.chain == watch.chain,
            PriceSnapshot.token_address == watch.token_address,
            PriceSnapshot.observed_at >= watch.started_at,
            or_(PriceSnapshot.quality_status.is_(None), PriceSnapshot.quality_status == PRICE_QUALITY_VALID),
        )
        .order_by(PriceSnapshot.observed_at.desc(), PriceSnapshot.id.desc())
    )


def _has_ca_match(matches: list[TokenMatch], token_address: str) -> bool:
    token = normalize_evm_address(token_address)
    return any(match.match_type in {"direct_ca", "quote_ca"} and match.value.lower() == token for match in matches)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


class _CountingProfileProvider:
    def __init__(self, provider: SocialProfileProvider, stats: SocialDiscoveryStats) -> None:
        self.provider = provider
        self.stats = stats

    def get_user_profile(self, username: str) -> SocialProfileSnapshot:
        self.stats.profile_api_calls += 1
        return self.provider.get_user_profile(username)

    def get_recent_posts(self, username: str, limit: int = 5) -> list[SocialPostSample]:
        self.stats.recent_posts_api_calls += 1
        return self.provider.get_recent_posts(username, limit=limit)


class _CountingClassifier:
    def __init__(self, classifier: SocialKOLClassifier, stats: SocialDiscoveryStats) -> None:
        self.classifier = classifier
        self.stats = stats

    def classify(self, payload: KOLVerificationInput) -> KOLVerificationResult:
        self.stats.deepseek_calls += 1
        return self.classifier.classify(payload)
