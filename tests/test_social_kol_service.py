from __future__ import annotations

import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_kol_service import (
    MANUAL_EXCLUDE,
    MANUAL_INCLUDE,
    LOW,
    OUTCOME_FAILED,
    OUTCOME_IGNORED_FOLLOWERS,
    OUTCOME_QUALIFIED,
    OUTCOME_REJECTED,
    OUTCOME_UNCERTAIN,
    FAILURE_STAGE_CLASSIFIER,
    FAILURE_STAGE_IDENTITY_CONFLICT,
    FAILURE_STAGE_INVALID_CLASSIFIER_OUTPUT,
    FAILURE_STAGE_PROFILE,
    METHOD_BOOTSTRAP,
    METHOD_DEEPSEEK_PROFILE_CHECK,
    METHOD_TRUSTED_SEED,
    SOURCE_BOOTSTRAP_SEED,
    SOURCE_SOCIAL_DISCOVERY,
    SOURCE_TRUSTED_EXTERNAL_SEED,
    STATUS_CANDIDATE,
    STATUS_IGNORED,
    STATUS_QUALIFIED,
    STATUS_REJECTED,
    STATUS_UNCERTAIN,
    KOLSeedCandidate,
    KOLVerificationInput,
    KOLVerificationResult,
    DeepSeekSocialKOLClassifier,
    SocialKOLService,
    SocialPostSample,
    SocialProfileSnapshot,
)
from app.services.social_watch_registry import WATCH_KOL, WATCH_PROJECT_X, SocialWatchRegistry
from app.utils.time import utc_now

TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/kol.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(
        json.dumps(
            {
                "kols": [
                    {"username": "lookonchain", "enabled": True},
                    {"username": "whale_alert", "enabled": True},
                    {"username": "WuBlockchain", "enabled": True},
                    {"username": "DegenerateNews", "enabled": True},
                    {"username": "tier10k", "enabled": True},
                ]
            }
        )
    )
    return session_factory, config_path


class FakeProvider:
    def __init__(self, *, followers=1500, bio="crypto trader", posts=None, author_id=None) -> None:
        self.followers = followers
        self.bio = bio
        self.posts = posts or [
            SocialPostSample("1", "Onchain token research"),
            SocialPostSample("2", "Market structure"),
            SocialPostSample("3", "A retweet", "retweet"),
            SocialPostSample("4", "Web3 analysis"),
        ]
        self.author_id = author_id
        self.profile_calls = 0
        self.posts_calls = 0

    def get_user_profile(self, username: str) -> SocialProfileSnapshot:
        self.profile_calls += 1
        return SocialProfileSnapshot(self.author_id, username, self.bio, self.followers)

    def get_recent_posts(self, username: str, limit: int = 5) -> list[SocialPostSample]:
        self.posts_calls += 1
        return self.posts[:limit]


class FakeClassifier:
    def __init__(self, result: KOLVerificationResult | Exception) -> None:
        self.result = result
        self.calls = 0
        self.inputs: list[KOLVerificationInput] = []

    def classify(self, payload: KOLVerificationInput) -> KOLVerificationResult:
        self.calls += 1
        self.inputs.append(payload)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def profiles(session_factory) -> list[SocialKOLProfile]:
    with session_scope(session_factory) as session:
        rows = list(session.scalars(select(SocialKOLProfile).order_by(SocialKOLProfile.id.asc())))
        for row in rows:
            session.expunge(row)
        return rows


def profile_by_username(session_factory, username: str) -> SocialKOLProfile:
    normalized = username.lower()
    row = next(profile for profile in profiles(session_factory) if profile.normalized_username == normalized)
    return row


def force_recheck_expired(session_factory, profile_id: int) -> None:
    with session_scope(session_factory) as session:
        session.get(SocialKOLProfile, profile_id).recheck_after = utc_now() - timedelta(seconds=1)


def add_watch_and_identity(session_factory, *, username="TinyProject", followers_irrelevant=True) -> None:
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=1,
                chain="robinhood",
                token_address=TOKEN,
                symbol="PONS",
                active=True,
                started_at=now - timedelta(minutes=5),
                last_seen_at=now,
            )
        )
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=TOKEN,
                symbol="PONS",
                identity_type=PROJECT_X if followers_irrelevant else DEV_X,
                value=username,
                normalized_value=username.lower(),
                source="test",
                source_field="test",
                confidence=HIGH,
                is_active=True,
                first_seen_at=now,
                last_verified_at=now,
                valid_from=now,
            )
        )


def test_candidate_identity_author_id_priority_username_rename_and_fallback(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    first = service.observe_candidate(author_id="a1", username="oldname", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    renamed = service.observe_candidate(author_id="a1", username="newname", followers=1300, source=SOURCE_SOCIAL_DISCOVERY)
    service.observe_candidate(author_id=None, username="fallback", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.observe_candidate(author_id=None, username="Fallback", followers=1250, source=SOURCE_SOCIAL_DISCOVERY)

    rows = profiles(session_factory)
    assert first.id == renamed.id
    assert len(rows) == 2
    assert rows[0].normalized_username == "newname"
    assert rows[0].follower_count == 1300
    assert rows[1].normalized_username == "fallback"


def test_username_only_profile_later_binds_author_id(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    first = service.observe_candidate(author_id=None, username="abc", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    bound = service.observe_candidate(author_id="123", username="abc", followers=1300, source=SOURCE_SOCIAL_DISCOVERY)

    rows = profiles(session_factory)
    assert first.id == bound.id
    assert len(rows) == 1
    assert rows[0].x_author_id == "123"
    assert rows[0].normalized_username == "abc"
    assert rows[0].follower_count == 1300


def test_author_id_profile_and_username_profile_conflict_merge_uses_latest_verification(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    older = utc_now() - timedelta(days=2)
    newer = utc_now() - timedelta(days=1)

    author_profile = service.observe_candidate(author_id="123", username="oldname", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    username_profile = service.observe_candidate(author_id=None, username="newname", followers=1800, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    with session_scope(session_factory) as session:
        author_row = session.get(SocialKOLProfile, author_profile.id)
        username_row = session.get(SocialKOLProfile, username_profile.id)
        service.apply_verification_result(author_row, KOLVerificationResult(True, HIGH, "trader", "old qualified"), older, ["old"])
        service.apply_verification_result(username_row, KOLVerificationResult(False, HIGH, "other", "new rejected"), newer, ["new"])
        username_row.last_seen_at = newer

    merged = service.observe_candidate(author_id="123", username="newname", followers=1900, source=SOURCE_SOCIAL_DISCOVERY)
    rows = profiles(session_factory)
    evidence = json.loads(rows[0].sources_json)

    assert len(rows) == 1
    assert merged.id == author_profile.id
    assert rows[0].x_author_id == "123"
    assert rows[0].normalized_username == "newname"
    assert rows[0].follower_count == 1900
    assert rows[0].status == STATUS_REJECTED
    assert rows[0].verification_reason == "new rejected"
    assert rows[0].first_seen_at <= author_profile.first_seen_at
    assert rows[0].last_seen_at >= newer
    assert {item["source"] for item in evidence["sources"]} == {SOURCE_SOCIAL_DISCOVERY, SOURCE_TRUSTED_EXTERNAL_SEED}


def test_manual_include_and_exclude_survive_profile_merge_with_exclude_priority(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="123", username="oldname", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    service.set_manual_override("oldname", MANUAL_INCLUDE)
    service.set_manual_override("newname", MANUAL_EXCLUDE)

    merged = service.observe_candidate(author_id="123", username="newname", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    rows = profiles(session_factory)

    assert len(rows) == 1
    assert merged.normalized_username == "newname"
    assert rows[0].manual_override == MANUAL_EXCLUDE
    assert rows[0].status == STATUS_IGNORED
    assert rows[0].verification_method == "manual"


def test_manual_include_survives_profile_merge(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="123", username="oldname", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    service.set_manual_override("newname", MANUAL_INCLUDE)

    service.observe_candidate(author_id="123", username="newname", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    rows = profiles(session_factory)

    assert len(rows) == 1
    assert rows[0].manual_override == MANUAL_INCLUDE
    assert rows[0].status == STATUS_QUALIFIED


def test_unproven_username_rename_does_not_merge(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    service.observe_candidate(author_id=None, username="abc", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.observe_candidate(author_id="123", username="xyz", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    rows = profiles(session_factory)
    assert len(rows) == 2
    assert {row.normalized_username for row in rows} == {"abc", "xyz"}


def test_nonempty_author_id_conflict_does_not_overwrite_existing_author(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    existing = service.observe_candidate(author_id="456", username="abc", followers=2000, source=SOURCE_SOCIAL_DISCOVERY)
    incoming = service.observe_candidate(author_id="123", username="abc", followers=2500, source=SOURCE_SOCIAL_DISCOVERY)

    rows = profiles(session_factory)
    assert incoming.id == existing.id
    assert len(rows) == 1
    assert rows[0].x_author_id == "456"
    assert rows[0].normalized_username == "abc"


def test_profile_for_author_does_not_return_same_username_different_author_id(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    service.observe_candidate(author_id="456", username="abc", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)

    lookup = service.profile_lookup_for_author(author_id="123", username="abc")

    assert lookup.identity_conflict is True
    assert lookup.profile is None
    assert service.profile_for_author(author_id="123", username="abc") is None


def test_profile_for_author_allows_bootstrap_username_when_author_id_missing_on_profile(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    service.observe_candidate(author_id=None, username="abc", followers=None, source=SOURCE_BOOTSTRAP_SEED)

    lookup = service.profile_lookup_for_author(author_id="123", username="abc")

    assert lookup.identity_conflict is False
    assert lookup.profile is not None
    assert lookup.profile.x_author_id is None


def test_followers_rules_and_trusted_seed(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    ignored = service.observe_candidate(author_id="low", username="low", followers=999, source=SOURCE_SOCIAL_DISCOVERY)
    candidate = service.observe_candidate(author_id="candidate", username="candidate", followers=1000, source=SOURCE_SOCIAL_DISCOVERY)
    trusted = service.observe_candidate(author_id="trusted", username="trusted", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    mid_trusted = service.observe_candidate(author_id="mid", username="mid", followers=2000, source=SOURCE_TRUSTED_EXTERNAL_SEED)

    assert ignored.status == STATUS_IGNORED
    assert candidate.status == STATUS_CANDIDATE
    assert trusted.status == STATUS_QUALIFIED
    assert trusted.verification_method == METHOD_TRUSTED_SEED
    assert mid_trusted.status == STATUS_CANDIDATE


def test_below_1000_does_not_call_verifier(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="low", username="low", followers=500, source=SOURCE_SOCIAL_DISCOVERY)
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto"))

    result = service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    assert result is None
    assert provider.profile_calls == 0
    assert classifier.calls == 0


@pytest.mark.parametrize(
    ("verification_result", "expected_status"),
    [
        (KOLVerificationResult(True, HIGH, "trader", "主要发布链上交易及代币研究内容"), STATUS_QUALIFIED),
        (KOLVerificationResult(False, HIGH, "other", "主要不是Crypto内容"), STATUS_REJECTED),
        (KOLVerificationResult(None, "LOW", "other", "证据不足"), STATUS_UNCERTAIN),
    ],
)
def test_deepseek_verification_result_status_and_no_full_samples_saved(ctx, verification_result, expected_status) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    provider = FakeProvider()
    classifier = FakeClassifier(verification_result)

    verified = service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    assert verified.status == expected_status
    assert verified.verification_method == METHOD_DEEPSEEK_PROFILE_CHECK
    assert "Onchain token research" not in (verified.sample_tweet_ids_json or "")
    assert json.loads(verified.sample_tweet_ids_json) == ["1", "2", "4"]
    assert classifier.inputs[0].recent_posts == ["Onchain token research", "Market structure", "Web3 analysis"]


def test_verifier_failure_keeps_candidate(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    result = service.verify_candidate(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(RuntimeError("deepseek down")),
    )

    assert result.status == STATUS_CANDIDATE
    assert result.recheck_after is not None


@pytest.mark.parametrize(
    ("verification_result", "expected_outcome"),
    [
        (KOLVerificationResult(True, HIGH, "trader", "crypto"), OUTCOME_QUALIFIED),
        (KOLVerificationResult(False, HIGH, "other", "not crypto"), OUTCOME_REJECTED),
        (KOLVerificationResult(None, LOW, "other", "uncertain"), OUTCOME_UNCERTAIN),
    ],
)
def test_verification_outcome_distinguishes_success_types(ctx, verification_result, expected_outcome) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(verification_result),
    )

    assert outcome.outcome == expected_outcome
    assert outcome.requested_profile_id == profile.id
    assert outcome.canonical_profile_id == profile.id
    assert outcome.profile is not None


def test_verification_outcome_ignored_followers_is_not_failure(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=None, source=SOURCE_SOCIAL_DISCOVERY)
    classifier = FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto"))

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FakeProvider(followers=700),
        classifier=classifier,
    )

    assert outcome.outcome == OUTCOME_IGNORED_FOLLOWERS
    assert outcome.failure_stage is None
    assert outcome.profile.status == STATUS_IGNORED
    assert classifier.calls == 0


def test_qualified_recheck_failure_returns_failed_outcome_but_preserves_status(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.verify_candidate(profile.id, provider=FakeProvider(), classifier=FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "old qualified")))
    force_recheck_expired(session_factory, profile.id)

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(RuntimeError("deepseek down")),
    )

    assert outcome.outcome == OUTCOME_FAILED
    assert outcome.failure_stage == FAILURE_STAGE_CLASSIFIER
    assert outcome.profile.status == STATUS_QUALIFIED
    assert outcome.profile.verification_reason == "old qualified"


def test_profile_failure_returns_failed_outcome_without_downgrading_rejected(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.verify_candidate(profile.id, provider=FakeProvider(), classifier=FakeClassifier(KOLVerificationResult(False, HIGH, "other", "old rejected")))
    force_recheck_expired(session_factory, profile.id)

    class FailingProvider(FakeProvider):
        def get_user_profile(self, username: str) -> SocialProfileSnapshot:
            self.profile_calls += 1
            raise RuntimeError("profile down")

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FailingProvider(),
        classifier=FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto")),
    )

    assert outcome.outcome == OUTCOME_FAILED
    assert outcome.failure_stage == FAILURE_STAGE_PROFILE
    assert outcome.profile.status == STATUS_REJECTED
    assert outcome.profile.verification_reason == "old rejected"


def test_invalid_classifier_output_returns_failed_outcome(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(KOLVerificationResult(True, "BAD", "trader", "bad")),
    )

    assert outcome.outcome == OUTCOME_FAILED
    assert outcome.failure_stage == FAILURE_STAGE_INVALID_CLASSIFIER_OUTPUT
    assert outcome.profile.status == STATUS_CANDIDATE


def test_identity_conflict_returns_failed_outcome(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="456", username="abc", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    profile = service.observe_candidate(author_id=None, username="candidate", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    class ConflictingProvider(FakeProvider):
        def get_user_profile(self, username: str) -> SocialProfileSnapshot:
            self.profile_calls += 1
            return SocialProfileSnapshot("123", "abc", "crypto", 1500)

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=ConflictingProvider(),
        classifier=FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto")),
    )

    assert outcome.outcome == OUTCOME_FAILED
    assert outcome.failure_stage == FAILURE_STAGE_IDENTITY_CONFLICT


def test_ttl_cache_and_recheck_after_expiry(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto"))

    verified = service.verify_candidate(profile.id, provider=provider, classifier=classifier)
    second = service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    assert verified.status == STATUS_QUALIFIED
    assert verified.recheck_after.date() == (verified.verified_at + timedelta(days=60)).date()
    assert second is None
    assert provider.profile_calls == 1
    assert provider.posts_calls == 1
    assert classifier.calls == 1

    with session_scope(session_factory) as session:
        row = session.get(SocialKOLProfile, profile.id)
        row.recheck_after = utc_now() - timedelta(seconds=1)
    service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    assert provider.profile_calls == 2
    assert classifier.calls == 2


def test_rejected_uncertain_and_ignored_ttl_lengths(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    now = utc_now()

    rejected = service.observe_candidate(author_id="r", username="r", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    uncertain = service.observe_candidate(author_id="u", username="u", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    ignored = service.observe_candidate(author_id="i", username="i", followers=900, source=SOURCE_SOCIAL_DISCOVERY)
    with session_scope(session_factory) as session:
        rejected_row = session.get(SocialKOLProfile, rejected.id)
        uncertain_row = session.get(SocialKOLProfile, uncertain.id)
        service.apply_verification_result(rejected_row, KOLVerificationResult(False, HIGH, "other", "not crypto"), now, [])
        service.apply_verification_result(uncertain_row, KOLVerificationResult(None, "LOW", "other", "uncertain"), now, [])

    rows = {row.normalized_username: row for row in profiles(session_factory)}
    assert rows["r"].recheck_after.date() == (now + timedelta(days=30)).date()
    assert rows["u"].recheck_after.date() == (now + timedelta(days=14)).date()
    assert rows["i"].recheck_after.date() == (rows["i"].verified_at + timedelta(days=30)).date()


def test_followers_change_qualified_to_ignored_and_back_to_candidate(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.verify_candidate(
        profile.id,
        provider=FakeProvider(followers=1200),
        classifier=FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto")),
    )
    with session_scope(session_factory) as session:
        row = session.get(SocialKOLProfile, profile.id)
        row.recheck_after = utc_now() - timedelta(seconds=1)

    ignored = service.verify_candidate(
        profile.id,
        provider=FakeProvider(followers=999),
        classifier=FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto")),
    )
    candidate = service.observe_candidate(author_id="a1", username="kol", followers=1000, source=SOURCE_SOCIAL_DISCOVERY)

    assert ignored.status == STATUS_IGNORED
    assert candidate.status == STATUS_CANDIDATE


def test_manual_include_and_exclude_do_not_expire(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    included = service.set_manual_override("manual_in", MANUAL_INCLUDE)
    excluded = service.set_manual_override("manual_out", MANUAL_EXCLUDE)

    assert included.status == STATUS_QUALIFIED
    assert included.recheck_after is None
    assert excluded.status == STATUS_IGNORED
    assert excluded.recheck_after is None
    assert [row.normalized_username for row in service.active_kols()] == ["manual_in"]


def test_bootstrap_fixed_kols_is_idempotent(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)

    assert service.bootstrap_fixed_kols(config_path) == 5
    assert service.bootstrap_fixed_kols(config_path) == 5

    rows = profiles(session_factory)
    assert len(rows) == 5
    assert {row.status for row in rows} == {STATUS_QUALIFIED}
    assert {row.source for row in rows} == {SOURCE_BOOTSTRAP_SEED}
    assert {row.verification_method for row in rows} == {METHOD_BOOTSTRAP}


def test_repeated_bootstrap_does_not_refresh_ttl_or_duplicate_sources(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)

    service.bootstrap_fixed_kols(config_path)
    first = profile_by_username(session_factory, "lookonchain")
    for _ in range(100):
        service.bootstrap_fixed_kols(config_path)
    second = profile_by_username(session_factory, "lookonchain")
    sources = json.loads(second.sources_json)["sources"]

    assert len(profiles(session_factory)) == 5
    assert second.verified_at == first.verified_at
    assert second.recheck_after == first.recheck_after
    assert second.status == STATUS_QUALIFIED
    assert len([item for item in sources if item["source"] == SOURCE_BOOTSTRAP_SEED]) == 1


def test_bootstrap_does_not_overwrite_rejected_ignored_or_deepseek_qualified(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)
    rejected = service.observe_candidate(author_id=None, username="lookonchain", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    ignored = service.observe_candidate(author_id=None, username="whale_alert", followers=900, source=SOURCE_SOCIAL_DISCOVERY)
    qualified = service.observe_candidate(author_id=None, username="WuBlockchain", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    with session_scope(session_factory) as session:
        service.apply_verification_result(
            session.get(SocialKOLProfile, rejected.id),
            KOLVerificationResult(False, HIGH, "other", "not crypto"),
            utc_now(),
            [],
        )
        service.apply_verification_result(
            session.get(SocialKOLProfile, qualified.id),
            KOLVerificationResult(True, HIGH, "analyst", "deepseek qualified"),
            utc_now(),
            [],
        )

    service.bootstrap_fixed_kols(config_path)
    rows = {row.normalized_username: row for row in profiles(session_factory)}

    assert rows["lookonchain"].status == STATUS_REJECTED
    assert rows["whale_alert"].status == STATUS_IGNORED
    assert rows["wublockchain"].status == STATUS_QUALIFIED
    assert rows["wublockchain"].verification_method == METHOD_DEEPSEEK_PROFILE_CHECK


def test_observe_seed_candidates_contract(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)

    rows = service.observe_seed_candidates(
        [KOLSeedCandidate("a1", "trusted", 3200, SOURCE_TRUSTED_EXTERNAL_SEED, HIGH)]
    )

    assert len(rows) == 1
    assert rows[0].status == STATUS_QUALIFIED


def test_registry_reads_qualified_kol_pool_and_filters_rejected_ignored_manual_exclude(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="q", username="qualified", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    rejected = service.observe_candidate(author_id="r", username="rejected", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    ignored = service.observe_candidate(author_id="i", username="ignored", followers=900, source=SOURCE_SOCIAL_DISCOVERY)
    excluded = service.observe_candidate(author_id="e", username="excluded", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    service.set_manual_override("excluded", MANUAL_EXCLUDE)
    with session_scope(session_factory) as session:
        session.get(SocialKOLProfile, rejected.id).status = STATUS_REJECTED
        session.get(SocialKOLProfile, ignored.id).status = STATUS_IGNORED
        session.get(SocialKOLProfile, excluded.id).status = STATUS_QUALIFIED

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()

    assert [(account.normalized_username, account.watch_type) for account in accounts] == [("qualified", WATCH_KOL)]


def test_project_dev_still_not_followers_gated_and_can_overlap_kol(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="kol", username="TinyProject", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    add_watch_and_identity(session_factory, username="TinyProject")

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()
    account = next(row for row in accounts if row.normalized_username == "tinyproject")

    assert account.watch_types == frozenset({WATCH_KOL, WATCH_PROJECT_X})
    assert SocialWatchRegistry.rule_value(accounts).count("from:TinyProject") == 1


def test_ttl_repeated_discovery_100_times_does_not_call_provider_or_deepseek(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto"))
    service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    for _ in range(100):
        service.observe_candidate(author_id="a1", username="kol", followers=1300, source=SOURCE_SOCIAL_DISCOVERY)
        service.verify_candidate(profile.id, provider=provider, classifier=classifier)

    assert provider.profile_calls == 1
    assert provider.posts_calls == 1
    assert classifier.calls == 1


@pytest.mark.parametrize(
    ("initial_result", "expected_status"),
    [
        (KOLVerificationResult(True, HIGH, "trader", "old qualified"), STATUS_QUALIFIED),
        (KOLVerificationResult(False, HIGH, "other", "old rejected"), STATUS_REJECTED),
        (KOLVerificationResult(None, LOW, "other", "old uncertain"), STATUS_UNCERTAIN),
    ],
)
def test_recheck_failure_preserves_existing_verification_state(ctx, initial_result, expected_status) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    service.verify_candidate(profile.id, provider=FakeProvider(), classifier=FakeClassifier(initial_result))
    before = profile_by_username(session_factory, "kol")
    force_recheck_expired(session_factory, before.id)

    after = service.verify_candidate(
        before.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(RuntimeError("classifier down")),
    )

    assert after.status == expected_status
    assert after.crypto_relevant == before.crypto_relevant
    assert after.verification_reason == before.verification_reason
    assert after.recheck_after > utc_now()
    assert after.recheck_after <= utc_now() + timedelta(days=1, seconds=5)


def test_ignored_recheck_failure_preserves_ignored_status(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="low", username="low", followers=900, source=SOURCE_SOCIAL_DISCOVERY)
    force_recheck_expired(session_factory, profile.id)

    after = service.verify_candidate(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(RuntimeError("classifier down")),
    )

    assert after.status == STATUS_IGNORED
    assert after.crypto_relevant is False
    assert after.confidence == LOW
    assert after.verification_reason == "followers below 1000"
    assert after.recheck_after > utc_now()


@pytest.mark.parametrize(
    "bad_result",
    [
        KOLVerificationResult(True, "BAD", "trader", "bad confidence"),
        KOLVerificationResult(True, HIGH, "bad_category", "bad category"),
        KOLVerificationResult("yes", HIGH, "trader", "bad bool"),
    ],
)
def test_invalid_classifier_output_is_failure_and_does_not_write_db(ctx, bad_result) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    after = service.verify_candidate(profile.id, provider=FakeProvider(), classifier=FakeClassifier(bad_result))

    assert after.status == STATUS_CANDIDATE
    assert after.crypto_relevant is None
    assert after.confidence is None
    assert after.category is None
    assert after.verification_method is None
    assert after.verification_reason is None
    assert after.recheck_after > utc_now()


def test_malformed_classifier_failure_preserves_candidate(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)

    after = service.verify_candidate(profile.id, provider=FakeProvider(), classifier=FakeClassifier(ValueError("malformed json")))

    assert after.status == STATUS_CANDIDATE
    assert after.verification_method is None
    assert after.recheck_after > utc_now()


def test_registry_uses_new_username_after_author_id_rename(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="123", username="oldname", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    service.observe_candidate(author_id="123", username="newname", followers=3000, source=SOURCE_SOCIAL_DISCOVERY)

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()
    rule = SocialWatchRegistry.rule_value(accounts)

    assert "from:newname" in rule
    assert "from:oldname" not in rule
    assert len([account for account in accounts if account.normalized_username == "newname"]) == 1


def test_registry_keeps_bootstrapped_seeds_when_other_qualified_kols_exist(ctx) -> None:
    session_factory, config_path = ctx
    service = SocialKOLService(session_factory)
    service.observe_candidate(author_id="other", username="otherkol", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)

    service.bootstrap_fixed_kols(config_path)
    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()
    usernames = {account.normalized_username for account in accounts}

    assert "otherkol" in usernames
    assert {"lookonchain", "whale_alert", "wublockchain", "degeneratenews", "tier10k"}.issubset(usernames)


def test_feature_disabled_and_key_missing_path_is_safe(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="a1", username="kol", followers=1200, source=SOURCE_SOCIAL_DISCOVERY)
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, HIGH, "trader", "crypto"))

    assert service.verify_candidate(profile.id, provider=provider, classifier=classifier, auto_verify_enabled=False) is None
    assert provider.profile_calls == 0
    assert classifier.calls == 0


def test_deepseek_classifier_parses_strict_json_and_uses_temperature_zero() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen["temperature"] = body.get("temperature")
        seen["recent_posts"] = json.loads(body["messages"][1]["content"])["recent_posts"]
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "crypto_relevant": True,
                                    "confidence": "MEDIUM",
                                    "category": "researcher",
                                    "reason": "主要发布Crypto/Web3研究内容",
                                }
                            )
                        }
                    }
                ]
            },
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    result = classifier.classify(
        KOLVerificationInput(
            username="kol",
            bio="crypto",
            recent_posts=["1", "2", "3", "4", "5", "6"],
        )
    )

    assert result.crypto_relevant is True
    assert result.confidence == "MEDIUM"
    assert result.category == "researcher"
    assert seen["temperature"] == 0
    assert seen["recent_posts"] == ["1", "2", "3", "4", "5"]


def test_deepseek_usage_stats_accumulate_real_response_tokens() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "usage": {
                    "prompt_tokens": 812,
                    "completion_tokens": 53,
                    "total_tokens": 865,
                    "prompt_cache_hit_tokens": 100,
                    "prompt_cache_miss_tokens": 712,
                },
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "crypto_relevant": True,
                                    "confidence": "HIGH",
                                    "category": "trader",
                                    "reason": "crypto trader",
                                }
                            )
                        }
                    }
                ],
            },
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    classifier.classify(KOLVerificationInput(username="kol", bio="crypto", recent_posts=["post"]))
    snapshot = classifier.stats_snapshot()

    assert snapshot["classifier_calls"] == 1
    assert snapshot["classifier_successes"] == 1
    assert snapshot["classifier_failures"] == 0
    assert snapshot["prompt_tokens"] == 812
    assert snapshot["completion_tokens"] == 53
    assert snapshot["total_tokens"] == 865
    assert snapshot["prompt_cache_hit_tokens"] == 100
    assert snapshot["prompt_cache_miss_tokens"] == 712
    assert "deepseek-test-key" not in str(snapshot)
    assert "post" not in str(snapshot)


def test_deepseek_failure_records_call_failure_and_usage_when_available() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
                "choices": [{"message": {"content": "{bad json"}}],
            },
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(json.JSONDecodeError):
        classifier.classify(KOLVerificationInput(username="kol", bio=None, recent_posts=[]))

    snapshot = classifier.stats_snapshot()
    assert snapshot["classifier_calls"] == 1
    assert snapshot["classifier_successes"] == 0
    assert snapshot["classifier_failures"] == 1
    assert snapshot["total_tokens"] == 12


def test_deepseek_invalid_schema_counts_as_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "crypto_relevant": True,
                                    "confidence": "BAD",
                                    "category": "trader",
                                    "reason": "bad",
                                }
                            )
                        }
                    }
                ]
            },
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(ValueError, match="invalid confidence"):
        classifier.classify(KOLVerificationInput(username="kol", bio=None, recent_posts=[]))

    assert classifier.stats_snapshot()["classifier_failures"] == 1


def test_deepseek_prompt_filters_mechanical_feed_but_allows_editorial_news() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen["system"] = body["messages"][0]["content"]
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "crypto_relevant": False,
                                    "confidence": "HIGH",
                                    "category": "other",
                                    "reason": "mechanical feed",
                                }
                            )
                        }
                    }
                ]
            },
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    classifier.classify(KOLVerificationInput(username="listing", bio="new listings", recent_posts=[]))
    system = str(seen["system"])

    assert "automatic new listing alerts" in system
    assert "automatic price alerts" in system
    assert "CA broadcasting" in system
    assert "editorial media" in system


def test_deepseek_classifier_non_crypto_and_spam_results_can_be_rejected(ctx) -> None:
    session_factory, _ = ctx
    service = SocialKOLService(session_factory)
    profile = service.observe_candidate(author_id="spam", username="spam", followers=3000, source=SOURCE_SOCIAL_DISCOVERY)

    outcome = service.verify_candidate_with_outcome(
        profile.id,
        provider=FakeProvider(),
        classifier=FakeClassifier(KOLVerificationResult(False, HIGH, "other", "明显自动推广或低质量Spam账号")),
    )

    assert outcome.outcome == OUTCOME_REJECTED
    assert outcome.profile.status == STATUS_REJECTED
    assert outcome.profile.verification_reason == "明显自动推广或低质量Spam账号"


def test_deepseek_classifier_malformed_json_raises_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "{bad json"}}]},
            request=request,
        )

    classifier = DeepSeekSocialKOLClassifier(
        api_key="deepseek-test-key",
        base_url="https://deepseek.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(json.JSONDecodeError):
        classifier.classify(KOLVerificationInput(username="kol", bio=None, recent_posts=[]))
