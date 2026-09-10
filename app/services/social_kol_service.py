from __future__ import annotations

import json
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialKOLProfile
from app.services.social_identity_service import HIGH, MEDIUM, normalize_username
from app.services.social_watch_registry import load_fixed_kol_usernames
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

STATUS_CANDIDATE = "candidate"
STATUS_QUALIFIED = "qualified"
STATUS_REJECTED = "rejected"
STATUS_IGNORED = "ignored"
STATUS_UNCERTAIN = "uncertain"

OUTCOME_QUALIFIED = "qualified"
OUTCOME_REJECTED = "rejected"
OUTCOME_UNCERTAIN = "uncertain"
OUTCOME_IGNORED_FOLLOWERS = "ignored_followers"
OUTCOME_CACHED = "cached"
OUTCOME_FAILED = "failed"

FAILURE_STAGE_PROFILE = "profile"
FAILURE_STAGE_RECENT_POSTS = "recent_posts"
FAILURE_STAGE_CLASSIFIER = "classifier"
FAILURE_STAGE_IDENTITY_CONFLICT = "identity_conflict"
FAILURE_STAGE_INVALID_CLASSIFIER_OUTPUT = "invalid_classifier_output"
FAILURE_STAGE_DB = "db"

SOURCE_BOOTSTRAP_SEED = "bootstrap_seed"
SOURCE_TRUSTED_EXTERNAL_SEED = "trusted_external_seed"
SOURCE_SOCIAL_DISCOVERY = "social_discovery"
SOURCE_MANUAL = "manual"

METHOD_BOOTSTRAP = "bootstrap"
METHOD_TRUSTED_SEED = "trusted_seed"
METHOD_DEEPSEEK_PROFILE_CHECK = "deepseek_profile_check"
METHOD_MANUAL = "manual"

MANUAL_NONE = "none"
MANUAL_INCLUDE = "include"
MANUAL_EXCLUDE = "exclude"

MIN_KOL_FOLLOWERS = 1000
TRUSTED_SEED_DIRECT_QUALIFY_FOLLOWERS = 3000
LOW = "LOW"
VERIFICATION_RETRY_DAYS = 1

ALLOWED_CONFIDENCE = {HIGH, MEDIUM, LOW}
ALLOWED_CATEGORIES = {
    "trader",
    "researcher",
    "analyst",
    "news",
    "founder",
    "influencer",
    "community",
    "other",
}

TTL_DAYS = {
    STATUS_QUALIFIED: 60,
    STATUS_REJECTED: 30,
    STATUS_UNCERTAIN: 14,
    STATUS_IGNORED: 30,
}


@dataclass(frozen=True)
class KOLSeedCandidate:
    author_id: str | None
    username: str
    followers: int | None
    source: str
    source_confidence: str | None = None
    evidence: dict[str, Any] | None = None


@dataclass(frozen=True)
class SocialProfileSnapshot:
    author_id: str | None
    username: str
    bio: str | None
    followers: int | None


@dataclass(frozen=True)
class SocialPostSample:
    post_id: str
    text: str
    post_type: str = "original"


@dataclass(frozen=True)
class KOLVerificationInput:
    username: str
    bio: str | None
    recent_posts: list[str]


@dataclass(frozen=True)
class KOLVerificationResult:
    crypto_relevant: bool | None
    confidence: str | None
    category: str | None
    reason: str


@dataclass(frozen=True)
class ProfileLookupResult:
    profile: SocialKOLProfile | None
    identity_conflict: bool = False


@dataclass(frozen=True)
class KOLVerificationOutcome:
    profile: SocialKOLProfile | None
    requested_profile_id: int
    canonical_profile_id: int | None
    outcome: str
    failure_stage: str | None = None


@dataclass
class DeepSeekUsageStats:
    classifier_calls: int = 0
    classifier_successes: int = 0
    classifier_failures: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    prompt_cache_hit_tokens: int = 0
    prompt_cache_miss_tokens: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "classifier_calls": self.classifier_calls,
            "classifier_successes": self.classifier_successes,
            "classifier_failures": self.classifier_failures,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "prompt_cache_hit_tokens": self.prompt_cache_hit_tokens,
            "prompt_cache_miss_tokens": self.prompt_cache_miss_tokens,
        }


class SocialProfileProvider(Protocol):
    def get_user_profile(self, username: str) -> SocialProfileSnapshot:
        ...

    def get_recent_posts(self, username: str, limit: int = 5) -> list[SocialPostSample]:
        ...


class SocialKOLClassifier(Protocol):
    def classify(self, payload: KOLVerificationInput) -> KOLVerificationResult:
        ...


class DeepSeekSocialKOLClassifier:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
        timeout_seconds: float = 20,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self._usage_stats = DeepSeekUsageStats()

    def stats_snapshot(self) -> dict[str, int]:
        return self._usage_stats.snapshot()

    def classify(self, payload: KOLVerificationInput) -> KOLVerificationResult:
        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Return strict JSON only. Determine whether this X account is mainly a Crypto/Web3 account "
                        "worth treating as a crypto KOL distribution node for Wallet Agent. crypto_relevant=true "
                        "fits crypto traders, onchain traders, DeFi or crypto researchers, token analysts, "
                        "blockchain/Web3 builders, crypto news accounts, meme coin traders/KOLs, crypto founders, "
                        "or accounts whose long-running focus is crypto markets. crypto_relevant=false fits normal "
                        "personal accounts, entertainment, sports, general tech with little crypto, accounts that "
                        "only casually mentioned one token, and obvious spam/giveaway/automated promotion accounts. "
                        "Even if they are fully crypto-related, classify mechanical feeds as crypto_relevant=false "
                        "when their main behavior is automatic new listing alerts, automatic price alerts, automatic "
                        "token launch feeds, CA broadcasting, batch mechanical shilling, giveaway spam, templated "
                        "promotion, or bot-like reposting without opinion, analysis, selection, or editorial value. "
                        "Do not reject real crypto news, research, analyst, or editorial media accounts when they "
                        "show meaningful curation, interpretation, reporting, or analysis. "
                        "Use null only when the bio and recent posts are insufficient. Do not include chain-of-thought."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "username": payload.username,
                            "bio": payload.bio,
                            "recent_posts": payload.recent_posts[:5],
                            "required_schema": {
                                "crypto_relevant": "boolean",
                                "confidence": "HIGH|MEDIUM|LOW",
                                "category": "trader|researcher|analyst|news|founder|influencer|community|other",
                                "reason": "short conclusion only",
                            },
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        self._usage_stats.classifier_calls += 1
        try:
            with httpx.Client(timeout=self.timeout_seconds, transport=self.transport) as client:
                response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=body)
        except Exception:
            self._usage_stats.classifier_failures += 1
            raise
        try:
            data = response.json()
        except json.JSONDecodeError:
            self._usage_stats.classifier_failures += 1
            raise
        _record_deepseek_usage(self._usage_stats, data)
        try:
            response.raise_for_status()
            content = data["choices"][0]["message"]["content"]
            result = json.loads(content)
            verification = KOLVerificationResult(
                crypto_relevant=result.get("crypto_relevant"),
                confidence=result.get("confidence"),
                category=result.get("category"),
                reason=str(result.get("reason") or "")[:500],
            )
            _validate_verification_result(verification)
        except Exception:
            self._usage_stats.classifier_failures += 1
            raise
        self._usage_stats.classifier_successes += 1
        return verification


class SocialKOLService:
    def __init__(self, session_factory: sessionmaker) -> None:
        self.session_factory = session_factory

    def observe_candidate(
        self,
        *,
        author_id: str | None,
        username: str,
        followers: int | None,
        source: str,
        source_evidence: dict[str, Any] | None = None,
    ) -> SocialKOLProfile:
        now = utc_now()
        username_value = normalize_username(username)
        if not username_value:
            raise ValueError("username is required")
        normalized = username_value.lower()
        with session_scope(self.session_factory) as session:
            conflict = self._identity_conflict_profile(session, author_id, normalized)
            if conflict:
                logger.warning(
                    "Social KOL identity conflict username=%s incoming_author_id=%s",
                    normalized,
                    author_id,
                )
                session.expunge(conflict)
                return conflict
            profile = self._find_or_merge_profile(session, author_id, username_value, normalized, now)
            created = False
            if not profile:
                profile = SocialKOLProfile(
                    x_author_id=author_id,
                    username=username_value,
                    normalized_username=normalized,
                    status=STATUS_CANDIDATE,
                    follower_count=followers,
                    crypto_relevant=None,
                    confidence=None,
                    category=None,
                    source=source,
                    sources_json=json.dumps({"sources": [_source_record(source, source_evidence)]}, ensure_ascii=False),
                    verification_method=None,
                    verification_reason=None,
                    first_seen_at=now,
                    last_seen_at=now,
                    is_active=True,
                    manual_override=MANUAL_NONE,
                )
                session.add(profile)
                session.flush()
                created = True
            if source == SOURCE_BOOTSTRAP_SEED and not created:
                self._merge_bootstrap_source(profile, source_evidence, now)
            else:
                self._merge_observation(profile, author_id, username_value, normalized, followers, source, source_evidence, now)
                self._apply_source_and_follower_rules(profile, now, source)
            session.flush()
            session.expunge(profile)
            return profile

    def observe_seed_candidates(self, candidates: list[KOLSeedCandidate]) -> list[SocialKOLProfile]:
        return [
            self.observe_candidate(
                author_id=candidate.author_id,
                username=candidate.username,
                followers=candidate.followers,
                source=candidate.source,
                source_evidence=candidate.evidence
                or {"source_confidence": candidate.source_confidence},
            )
            for candidate in candidates
        ]

    def bootstrap_fixed_kols(self, config_path) -> int:
        count = 0
        for username in load_fixed_kol_usernames(config_path):
            self.observe_candidate(
                author_id=None,
                username=username,
                followers=None,
                source=SOURCE_BOOTSTRAP_SEED,
                source_evidence={"config": str(config_path)},
            )
            count += 1
        return count

    def active_kols(self) -> list[SocialKOLProfile]:
        with session_scope(self.session_factory) as session:
            rows = list(
                session.scalars(
                    select(SocialKOLProfile)
                    .where(
                        SocialKOLProfile.status == STATUS_QUALIFIED,
                        SocialKOLProfile.is_active.is_(True),
                        SocialKOLProfile.manual_override != MANUAL_EXCLUDE,
                    )
                    .order_by(SocialKOLProfile.normalized_username.asc())
                )
            )
            for row in rows:
                session.expunge(row)
            return rows

    def profile_lookup_for_author(self, *, author_id: str | None, username: str | None) -> ProfileLookupResult:
        normalized = (normalize_username(username) or "").lower()
        with session_scope(self.session_factory) as session:
            author_profile = None
            if author_id:
                author_profile = session.scalar(
                    select(SocialKOLProfile).where(SocialKOLProfile.x_author_id == str(author_id))
                )
                if author_profile:
                    session.expunge(author_profile)
                    return ProfileLookupResult(author_profile, False)
            username_profile = None
            if normalized:
                username_profile = session.scalar(
                    select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == normalized)
                )
            if username_profile:
                if (
                    author_id
                    and username_profile.x_author_id
                    and username_profile.x_author_id != str(author_id)
                ):
                    session.expunge(username_profile)
                    return ProfileLookupResult(None, True)
                session.expunge(username_profile)
                return ProfileLookupResult(username_profile, False)
            return ProfileLookupResult(None, False)

    def profile_for_author(self, *, author_id: str | None, username: str | None) -> SocialKOLProfile | None:
        lookup = self.profile_lookup_for_author(author_id=author_id, username=username)
        return None if lookup.identity_conflict else lookup.profile

    def due_profile_ids(self, *, limit: int = 20, now: datetime | None = None) -> list[int]:
        now = now or utc_now()
        with session_scope(self.session_factory) as session:
            rows = list(
                session.scalars(
                    select(SocialKOLProfile)
                    .where(
                        SocialKOLProfile.is_active.is_(True),
                        SocialKOLProfile.manual_override == MANUAL_NONE,
                    )
                    .order_by(SocialKOLProfile.recheck_after.asc().nullsfirst(), SocialKOLProfile.id.asc())
                )
            )
            due = [row.id for row in rows if self.needs_verification(row, now)]
            return due[:limit]

    def needs_verification(self, profile: SocialKOLProfile, now: datetime | None = None) -> bool:
        now = now or utc_now()
        if profile.manual_override in {MANUAL_INCLUDE, MANUAL_EXCLUDE}:
            return False
        if (
            profile.status != STATUS_IGNORED
            and profile.follower_count is not None
            and profile.follower_count < MIN_KOL_FOLLOWERS
        ):
            return False
        if profile.verified_at is None:
            return profile.status in {STATUS_CANDIDATE, STATUS_UNCERTAIN}
        return bool(profile.recheck_after and profile.recheck_after <= now)

    def verify_candidate(
        self,
        profile_id: int,
        *,
        provider: SocialProfileProvider,
        classifier: SocialKOLClassifier,
        auto_verify_enabled: bool = True,
    ) -> SocialKOLProfile | None:
        outcome = self.verify_candidate_with_outcome(
            profile_id,
            provider=provider,
            classifier=classifier,
            auto_verify_enabled=auto_verify_enabled,
        )
        if outcome.outcome == OUTCOME_CACHED:
            return None
        return outcome.profile

    def verify_candidate_with_outcome(
        self,
        profile_id: int,
        *,
        provider: SocialProfileProvider,
        classifier: SocialKOLClassifier,
        auto_verify_enabled: bool = True,
    ) -> KOLVerificationOutcome:
        if not auto_verify_enabled:
            return KOLVerificationOutcome(None, profile_id, None, OUTCOME_CACHED)
        with session_scope(self.session_factory) as session:
            profile = session.get(SocialKOLProfile, profile_id)
            if not profile:
                return KOLVerificationOutcome(None, profile_id, None, OUTCOME_FAILED, FAILURE_STAGE_DB)
            if not self.needs_verification(profile):
                session.expunge(profile)
                return KOLVerificationOutcome(profile, profile_id, profile.id, OUTCOME_CACHED)
            username = profile.username
        try:
            profile_snapshot = provider.get_user_profile(username)
        except Exception as exc:
            profile = self._mark_verification_retry(profile_id, exc)
            return KOLVerificationOutcome(profile, profile_id, profile.id if profile else None, OUTCOME_FAILED, FAILURE_STAGE_PROFILE)
        snapshot_username = normalize_username(profile_snapshot.username) or username
        normalized = snapshot_username.lower()
        if self._profile_update_has_identity_conflict(profile_id, profile_snapshot.author_id, normalized):
            profile = self._mark_verification_retry(profile_id, RuntimeError("identity conflict"))
            return KOLVerificationOutcome(
                profile,
                profile_id,
                profile.id if profile else None,
                OUTCOME_FAILED,
                FAILURE_STAGE_IDENTITY_CONFLICT,
            )
        if profile_snapshot.followers is not None and profile_snapshot.followers < MIN_KOL_FOLLOWERS:
            try:
                profile = self._apply_follower_gate_update(
                    profile_id,
                    SocialProfileSnapshot(
                        profile_snapshot.author_id,
                        snapshot_username,
                        profile_snapshot.bio,
                        profile_snapshot.followers,
                    ),
                )
            except Exception as exc:
                profile = self._mark_verification_retry(profile_id, exc)
                return KOLVerificationOutcome(profile, profile_id, profile.id if profile else None, OUTCOME_FAILED, FAILURE_STAGE_DB)
            return KOLVerificationOutcome(
                profile,
                profile_id,
                profile.id if profile else None,
                OUTCOME_IGNORED_FOLLOWERS,
            )
        try:
            posts = provider.get_recent_posts(snapshot_username, limit=5)
            relevant_posts = [post for post in posts if post.post_type != "retweet"][:5]
        except Exception as exc:
            profile = self._mark_verification_retry(profile_id, exc)
            return KOLVerificationOutcome(profile, profile_id, profile.id if profile else None, OUTCOME_FAILED, FAILURE_STAGE_RECENT_POSTS)
        try:
            result = classifier.classify(
                KOLVerificationInput(
                    username=snapshot_username,
                    bio=profile_snapshot.bio,
                    recent_posts=[post.text for post in relevant_posts],
                )
            )
        except Exception as exc:
            profile = self._mark_verification_retry(profile_id, exc)
            return KOLVerificationOutcome(profile, profile_id, profile.id if profile else None, OUTCOME_FAILED, FAILURE_STAGE_CLASSIFIER)
        try:
            _validate_verification_result(result)
        except Exception as exc:
            profile = self._mark_verification_retry(profile_id, exc)
            return KOLVerificationOutcome(
                profile,
                profile_id,
                profile.id if profile else None,
                OUTCOME_FAILED,
                FAILURE_STAGE_INVALID_CLASSIFIER_OUTPUT,
            )
        try:
            profile = self._apply_successful_profile_update(
                profile_id,
                SocialProfileSnapshot(
                    profile_snapshot.author_id,
                    snapshot_username,
                    profile_snapshot.bio,
                    profile_snapshot.followers,
                ),
                result,
                [post.post_id for post in relevant_posts],
            )
        except Exception as exc:
            profile = self._mark_verification_retry(profile_id, exc)
            return KOLVerificationOutcome(profile, profile_id, profile.id if profile else None, OUTCOME_FAILED, FAILURE_STAGE_DB)
        return KOLVerificationOutcome(
            profile,
            profile_id,
            profile.id if profile else None,
            _outcome_for_result(result),
        )

    def _apply_successful_profile_update(
        self,
        profile_id: int,
        profile_snapshot: SocialProfileSnapshot,
        result: KOLVerificationResult,
        sample_tweet_ids: list[str],
    ) -> SocialKOLProfile | None:
        now = utc_now()
        username = normalize_username(profile_snapshot.username) or profile_snapshot.username
        normalized = username.lower()
        with session_scope(self.session_factory) as session:
            profile = session.get(SocialKOLProfile, profile_id)
            if not profile:
                return None
            conflict = self._identity_conflict_profile(session, profile_snapshot.author_id, normalized)
            if conflict and conflict.id != profile.id:
                logger.warning(
                    "Social KOL verification identity conflict profile_id=%s username=%s incoming_author_id=%s",
                    profile_id,
                    normalized,
                    profile_snapshot.author_id,
                )
                profile.recheck_after = now + timedelta(days=VERIFICATION_RETRY_DAYS)
                profile.updated_at = now
                session.flush()
                session.expunge(profile)
                return profile
            profile = self._find_or_merge_profile(session, profile_snapshot.author_id, username, normalized, now) or profile
            self._merge_observation(
                profile,
                profile_snapshot.author_id,
                username,
                normalized,
                profile_snapshot.followers,
                profile.source,
                {"profile_refresh": True},
                now,
            )
            self.apply_verification_result(profile, result, now, sample_tweet_ids)
            session.flush()
            session.expunge(profile)
            return profile

    def _apply_follower_gate_update(
        self,
        profile_id: int,
        profile_snapshot: SocialProfileSnapshot,
    ) -> SocialKOLProfile | None:
        now = utc_now()
        username = normalize_username(profile_snapshot.username) or profile_snapshot.username
        normalized = username.lower()
        with session_scope(self.session_factory) as session:
            profile = session.get(SocialKOLProfile, profile_id)
            if not profile:
                return None
            conflict = self._identity_conflict_profile(session, profile_snapshot.author_id, normalized)
            if conflict and conflict.id != profile.id:
                logger.warning(
                    "Social KOL follower update identity conflict profile_id=%s username=%s incoming_author_id=%s",
                    profile_id,
                    normalized,
                    profile_snapshot.author_id,
                )
                profile.recheck_after = now + timedelta(days=VERIFICATION_RETRY_DAYS)
                profile.updated_at = now
                session.flush()
                session.expunge(profile)
                return profile
            profile = self._find_or_merge_profile(session, profile_snapshot.author_id, username, normalized, now) or profile
            self._merge_observation(
                profile,
                profile_snapshot.author_id,
                username,
                normalized,
                profile_snapshot.followers,
                profile.source,
                {"profile_refresh": True},
                now,
            )
            self._set_ignored(profile, "followers below 1000", now)
            session.flush()
            session.expunge(profile)
            return profile

    def _mark_verification_retry(self, profile_id: int, exc: Exception) -> SocialKOLProfile | None:
        now = utc_now()
        with session_scope(self.session_factory) as session:
            profile = session.get(SocialKOLProfile, profile_id)
            if not profile:
                return None
            logger.warning("Social KOL verification failed username=%s: %s", profile.normalized_username, exc)
            profile.recheck_after = now + timedelta(days=VERIFICATION_RETRY_DAYS)
            profile.updated_at = now
            session.flush()
            session.expunge(profile)
            return profile

    def _profile_update_has_identity_conflict(
        self,
        profile_id: int,
        author_id: str | None,
        normalized_username: str,
    ) -> bool:
        with session_scope(self.session_factory) as session:
            profile = session.get(SocialKOLProfile, profile_id)
            if not profile:
                return False
            if author_id and profile.x_author_id and profile.x_author_id != str(author_id):
                return True
            conflict = self._identity_conflict_profile(session, author_id, normalized_username)
            return bool(conflict and conflict.id != profile.id)

    async def verify_candidates(
        self,
        profile_ids: list[int],
        *,
        provider: SocialProfileProvider,
        classifier: SocialKOLClassifier,
        auto_verify_enabled: bool = True,
        max_concurrency: int = 3,
    ) -> list[SocialKOLProfile | None]:
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def _verify(profile_id: int) -> SocialKOLProfile | None:
            async with semaphore:
                return await asyncio.to_thread(
                    self.verify_candidate,
                    profile_id,
                    provider=provider,
                    classifier=classifier,
                    auto_verify_enabled=auto_verify_enabled,
                )

        return await asyncio.gather(*[_verify(profile_id) for profile_id in profile_ids])

    async def verify_candidates_with_outcome(
        self,
        profile_ids: list[int],
        *,
        provider: SocialProfileProvider,
        classifier: SocialKOLClassifier,
        auto_verify_enabled: bool = True,
        max_concurrency: int = 3,
    ) -> list[KOLVerificationOutcome]:
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def _verify(profile_id: int) -> KOLVerificationOutcome:
            async with semaphore:
                return await asyncio.to_thread(
                    self.verify_candidate_with_outcome,
                    profile_id,
                    provider=provider,
                    classifier=classifier,
                    auto_verify_enabled=auto_verify_enabled,
                )

        return await asyncio.gather(*[_verify(profile_id) for profile_id in profile_ids])

    def apply_verification_result(
        self,
        profile: SocialKOLProfile,
        result: KOLVerificationResult,
        now: datetime,
        sample_tweet_ids: list[str] | None = None,
    ) -> None:
        profile.crypto_relevant = result.crypto_relevant
        profile.confidence = result.confidence
        profile.category = result.category
        profile.verification_method = METHOD_DEEPSEEK_PROFILE_CHECK
        profile.verification_reason = result.reason[:500]
        profile.sample_tweet_ids_json = json.dumps(sample_tweet_ids or [], ensure_ascii=False)
        profile.verified_at = now
        if result.crypto_relevant is True and result.confidence in {HIGH, MEDIUM}:
            profile.status = STATUS_QUALIFIED
        elif result.crypto_relevant is False:
            profile.status = STATUS_REJECTED
        else:
            profile.status = STATUS_UNCERTAIN
        profile.recheck_after = _recheck_after(profile.status, now)
        profile.updated_at = now

    def set_manual_override(self, username: str, override: str) -> SocialKOLProfile:
        now = utc_now()
        normalized = (normalize_username(username) or username).lower()
        with session_scope(self.session_factory) as session:
            profile = session.scalar(
                select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == normalized)
            )
            if not profile:
                profile = SocialKOLProfile(
                    username=normalize_username(username) or username,
                    normalized_username=normalized,
                    status=STATUS_CANDIDATE,
                    source=SOURCE_MANUAL,
                    sources_json=json.dumps({"sources": [_source_record(SOURCE_MANUAL, {"override": override})]}, ensure_ascii=False),
                    first_seen_at=now,
                    last_seen_at=now,
                    is_active=True,
                    manual_override=override,
                )
                session.add(profile)
                session.flush()
            profile.manual_override = override
            if override == MANUAL_INCLUDE:
                self._set_qualified(profile, METHOD_MANUAL, "manual include", now)
            elif override == MANUAL_EXCLUDE:
                profile.status = STATUS_IGNORED
                profile.verification_method = METHOD_MANUAL
                profile.verification_reason = "manual exclude"
                profile.recheck_after = None
                profile.verified_at = now
            profile.updated_at = now
            session.flush()
            session.expunge(profile)
            return profile

    def _find_or_merge_profile(
        self,
        session,
        author_id: str | None,
        username: str,
        normalized_username: str,
        now: datetime,
    ) -> SocialKOLProfile | None:
        author_profile = None
        if author_id:
            author_profile = session.scalar(select(SocialKOLProfile).where(SocialKOLProfile.x_author_id == author_id))
        username_profile = session.scalar(
            select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == normalized_username)
        )
        if author_profile and username_profile and author_profile.id != username_profile.id:
            return self._merge_duplicate_profile(
                session,
                author_profile,
                username_profile,
                author_id,
                username,
                normalized_username,
                now,
            )
        return author_profile or username_profile

    def _identity_conflict_profile(
        self,
        session,
        author_id: str | None,
        normalized_username: str,
    ) -> SocialKOLProfile | None:
        if not author_id:
            return None
        author_profile = session.scalar(select(SocialKOLProfile).where(SocialKOLProfile.x_author_id == author_id))
        username_profile = session.scalar(
            select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == normalized_username)
        )
        if username_profile and username_profile.x_author_id and username_profile.x_author_id != author_id:
            return author_profile or username_profile
        return None

    def _merge_duplicate_profile(
        self,
        session,
        canonical: SocialKOLProfile,
        duplicate: SocialKOLProfile,
        incoming_author_id: str,
        incoming_username: str,
        incoming_normalized_username: str,
        now: datetime,
    ) -> SocialKOLProfile:
        canonical_last_seen = canonical.last_seen_at
        duplicate_last_seen = duplicate.last_seen_at
        canonical.first_seen_at = min(canonical.first_seen_at, duplicate.first_seen_at)
        canonical.last_seen_at = max(canonical_last_seen, duplicate_last_seen, now)
        canonical.sources_json = json.dumps(
            {"sources": _merge_sources(canonical.sources_json, duplicate.sources_json)},
            ensure_ascii=False,
        )
        if duplicate_last_seen > canonical_last_seen and duplicate.follower_count is not None:
            canonical.follower_count = duplicate.follower_count

        manual = _merged_manual_override(canonical.manual_override, duplicate.manual_override)
        if manual == MANUAL_EXCLUDE:
            if canonical.manual_override == MANUAL_INCLUDE or duplicate.manual_override == MANUAL_INCLUDE:
                logger.warning("Social KOL profile merge resolved manual override conflict with exclude")
            canonical.manual_override = MANUAL_EXCLUDE
            canonical.status = STATUS_IGNORED
            canonical.verification_method = METHOD_MANUAL
            canonical.verification_reason = "manual exclude"
            canonical.verified_at = _latest_datetime(canonical.verified_at, duplicate.verified_at) or now
            canonical.recheck_after = None
        elif manual == MANUAL_INCLUDE:
            include_source = duplicate if duplicate.manual_override == MANUAL_INCLUDE else canonical
            canonical.manual_override = MANUAL_INCLUDE
            _copy_verification_state(canonical, include_source)
            canonical.status = STATUS_QUALIFIED
            canonical.crypto_relevant = True
            canonical.confidence = HIGH
            canonical.verification_method = METHOD_MANUAL
            canonical.verification_reason = canonical.verification_reason or "manual include"
            canonical.recheck_after = None
        else:
            source = _latest_verified_profile(canonical, duplicate)
            if source is duplicate:
                _copy_verification_state(canonical, duplicate)
            canonical.manual_override = MANUAL_NONE

        session.delete(duplicate)
        session.flush()
        canonical.x_author_id = incoming_author_id
        canonical.username = incoming_username
        canonical.normalized_username = incoming_normalized_username
        canonical.updated_at = now
        return canonical

    def _merge_observation(
        self,
        profile: SocialKOLProfile,
        author_id: str | None,
        username: str,
        normalized_username: str,
        followers: int | None,
        source: str,
        source_evidence: dict[str, Any] | None,
        now: datetime,
    ) -> None:
        if author_id and profile.x_author_id and profile.x_author_id != author_id:
            logger.warning(
                "Social KOL identity conflict profile_id=%s username=%s existing_author_id=%s incoming_author_id=%s",
                profile.id,
                profile.normalized_username,
                profile.x_author_id,
                author_id,
            )
            author_id = None
        if author_id and profile.x_author_id != author_id:
            profile.x_author_id = author_id
        if normalized_username and profile.normalized_username != normalized_username:
            profile.username = username
            profile.normalized_username = normalized_username
        if followers is not None:
            profile.follower_count = followers
        profile.last_seen_at = now
        profile.source = source
        sources = _sources(profile.sources_json)
        record = _source_record(source, source_evidence)
        if record not in sources:
            sources.append(record)
            profile.sources_json = json.dumps({"sources": sources}, ensure_ascii=False)
        profile.updated_at = now

    def _merge_bootstrap_source(
        self,
        profile: SocialKOLProfile,
        source_evidence: dict[str, Any] | None,
        now: datetime,
    ) -> None:
        profile.last_seen_at = now
        sources = _sources(profile.sources_json)
        record = _source_record(SOURCE_BOOTSTRAP_SEED, source_evidence)
        if record not in sources:
            sources.append(record)
            profile.sources_json = json.dumps({"sources": sources}, ensure_ascii=False)
            profile.updated_at = now

    def _apply_source_and_follower_rules(self, profile: SocialKOLProfile, now: datetime, source: str) -> None:
        if profile.manual_override == MANUAL_INCLUDE:
            if profile.status != STATUS_QUALIFIED or profile.verification_method != METHOD_MANUAL:
                self._set_qualified(profile, METHOD_MANUAL, "manual include", now)
            return
        if profile.manual_override == MANUAL_EXCLUDE:
            if profile.status != STATUS_IGNORED or profile.verification_method != METHOD_MANUAL:
                profile.status = STATUS_IGNORED
                profile.verification_method = METHOD_MANUAL
                profile.verification_reason = "manual exclude"
                profile.recheck_after = None
            return
        if source == SOURCE_BOOTSTRAP_SEED:
            self._set_qualified(profile, METHOD_BOOTSTRAP, "bootstrap fixed KOL seed", now)
            return
        if (
            source == SOURCE_TRUSTED_EXTERNAL_SEED
            and profile.follower_count is not None
            and profile.follower_count >= TRUSTED_SEED_DIRECT_QUALIFY_FOLLOWERS
        ):
            self._set_qualified(profile, METHOD_TRUSTED_SEED, "trusted seed with followers >=3000", now)
            return
        if profile.follower_count is not None and profile.follower_count < MIN_KOL_FOLLOWERS:
            self._set_ignored(profile, "followers below 1000", now)
            return
        if profile.status == STATUS_IGNORED and profile.follower_count is not None and profile.follower_count >= MIN_KOL_FOLLOWERS:
            profile.status = STATUS_CANDIDATE
            profile.crypto_relevant = None
            profile.confidence = None
            profile.category = None
            profile.verification_method = None
            profile.verification_reason = None
            profile.verified_at = None
            profile.recheck_after = None
        elif profile.status not in {STATUS_QUALIFIED, STATUS_REJECTED, STATUS_UNCERTAIN}:
            profile.status = STATUS_CANDIDATE

    def _set_qualified(self, profile: SocialKOLProfile, method: str, reason: str, now: datetime) -> None:
        profile.status = STATUS_QUALIFIED
        profile.crypto_relevant = True
        profile.confidence = HIGH
        profile.verification_method = method
        profile.verification_reason = reason
        profile.verified_at = now
        profile.recheck_after = None if profile.manual_override in {MANUAL_INCLUDE, MANUAL_EXCLUDE} else _recheck_after(STATUS_QUALIFIED, now)

    def _set_ignored(self, profile: SocialKOLProfile, reason: str, now: datetime) -> None:
        profile.status = STATUS_IGNORED
        profile.crypto_relevant = False
        profile.confidence = LOW
        profile.verification_method = None
        profile.verification_reason = reason
        profile.verified_at = now
        profile.recheck_after = _recheck_after(STATUS_IGNORED, now)


def _source_record(source: str, evidence: dict[str, Any] | None) -> dict[str, Any]:
    return {"source": source, "evidence": evidence or {}}


def _sources(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    sources = data.get("sources")
    return [item for item in sources if isinstance(item, dict)] if isinstance(sources, list) else []


def _merge_sources(left: str | None, right: str | None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for record in [*_sources(left), *_sources(right)]:
        if record not in merged:
            merged.append(record)
    return merged


def _latest_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left and right:
        return max(left, right)
    return left or right


def _latest_verified_profile(left: SocialKOLProfile, right: SocialKOLProfile) -> SocialKOLProfile:
    left_at = left.verified_at or datetime.min
    right_at = right.verified_at or datetime.min
    return right if right_at > left_at else left


def _copy_verification_state(target: SocialKOLProfile, source: SocialKOLProfile) -> None:
    target.status = source.status
    target.crypto_relevant = source.crypto_relevant
    target.confidence = source.confidence
    target.category = source.category
    target.verification_method = source.verification_method
    target.verification_reason = source.verification_reason
    target.sample_tweet_ids_json = source.sample_tweet_ids_json
    target.verified_at = source.verified_at
    target.recheck_after = source.recheck_after


def _merged_manual_override(left: str | None, right: str | None) -> str:
    values = {left or MANUAL_NONE, right or MANUAL_NONE}
    if MANUAL_EXCLUDE in values:
        return MANUAL_EXCLUDE
    if MANUAL_INCLUDE in values:
        return MANUAL_INCLUDE
    return MANUAL_NONE


def _validate_verification_result(result: KOLVerificationResult) -> None:
    if result.crypto_relevant not in {True, False, None}:
        raise ValueError("invalid crypto_relevant")
    if result.confidence not in ALLOWED_CONFIDENCE:
        raise ValueError("invalid confidence")
    if result.category not in ALLOWED_CATEGORIES:
        raise ValueError("invalid category")
    if not isinstance(result.reason, str):
        raise ValueError("invalid reason")


def _outcome_for_result(result: KOLVerificationResult) -> str:
    if result.crypto_relevant is True and result.confidence in {HIGH, MEDIUM}:
        return OUTCOME_QUALIFIED
    if result.crypto_relevant is False:
        return OUTCOME_REJECTED
    return OUTCOME_UNCERTAIN


def _record_deepseek_usage(stats: DeepSeekUsageStats, data: dict[str, Any]) -> None:
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict):
        return
    stats.prompt_tokens += _int_usage(usage.get("prompt_tokens"))
    stats.completion_tokens += _int_usage(usage.get("completion_tokens"))
    stats.total_tokens += _int_usage(usage.get("total_tokens"))
    stats.prompt_cache_hit_tokens += _int_usage(
        usage.get("prompt_cache_hit_tokens", usage.get("prompt_cache_hit_tokens"))
    )
    stats.prompt_cache_miss_tokens += _int_usage(
        usage.get("prompt_cache_miss_tokens", usage.get("prompt_cache_miss_tokens"))
    )


def _int_usage(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _recheck_after(status: str, now: datetime) -> datetime | None:
    days = TTL_DAYS.get(status)
    return now + timedelta(days=days) if days else None
