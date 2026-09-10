from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialEvent, SocialIdentity, SocialMemory, TokenWatchState
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_PROJECT_X, SocialEventService
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_memory_service import (
    CATEGORY_LOW_INFORMATION,
    CATEGORY_SECURITY,
    CATEGORY_TOKENOMICS,
    CONFIDENCE_HIGH,
    DECISION_KEEP_MEMORY,
    DECISION_NO_MEMORY,
    JsonSocialSemanticTriageAdapter,
    SCOPE_PROJECT,
    SCOPE_SECURITY,
    SCOPE_TOKEN,
    SIGNIFICANCE_HIGH,
    SIGNIFICANCE_MEDIUM,
    SocialMemoryService,
    SocialTriageResult,
    validate_triage_result,
)
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet, PROVIDER
from app.services.wallet_service import WalletService

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


class FakeTriageAdapter:
    def __init__(self, result=None) -> None:  # noqa: ANN001
        self.result = result
        self.calls = 0

    def triage(self, payload):  # noqa: ANN001
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class SequenceTriageAdapter:
    def __init__(self, results) -> None:  # noqa: ANN001
        self.results = list(results)
        self.calls = 0

    def triage(self, payload):  # noqa: ANN001
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/social_memory.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 9, 1, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        session.add(
            TokenWatchState(
                wallet_id=wallet.id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                active=True,
                started_at=now - timedelta(hours=1),
                last_seen_at=now,
            )
        )
        session.add_all(
            [
                identity(PROJECT_X, "ProjectUser", "projectuser", now),
                identity(DEV_X, "DevUser", "devuser", now),
            ]
        )
    return session_factory


def test_project_gm_is_deterministic_no_memory_without_llm(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event_service = SocialEventService(ctx, memory_processor=memory)

    event_service.create_event_if_relevant(tweet(author_username="ProjectUser", text="GM", matches=[]))

    assert adapter.calls == 0
    assert memory.stats_snapshot()["deterministic_no_memory"] == 1
    assert all_memories(ctx) == []


@pytest.mark.parametrize("text", ["GM $ROBBIE 🚀", "GN fam", "Thanks community ❤️"])
def test_generic_greeting_with_cashtag_or_suffix_is_deterministic_no_memory(ctx, text: str) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text=text, matches=[])
    )

    assert adapter.calls == 0
    assert memory.stats_snapshot()["deterministic_no_memory"] == 1
    assert all_memories(ctx) == []


@pytest.mark.parametrize(
    "text",
    [
        "GM. Mainnet launches Sep 20.",
        "Target mainnet date is Sep 20.",
        "Price oracle v2 is now live.",
        "Floor fee has been reduced to 0.1%.",
        "New charting API is available for developers.",
    ],
)
def test_project_fact_words_enter_semantic_triage(ctx, text: str) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Project announced a factual update."))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text=text, matches=[])
    )

    assert adapter.calls == 1
    assert memory.stats_snapshot()["deterministic_no_memory"] == 0
    assert len(all_memories(ctx)) == 1


def test_dev_cooking_is_no_memory(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="DevUser", text="we are cooking", matches=[])
    )

    assert adapter.calls == 0
    assert memory.stats_snapshot()["deterministic_no_memory"] == 1


def test_project_mainnet_candidate_keep_memory_persists_summary_and_url(ctx) -> None:
    adapter = FakeTriageAdapter(
        keep_result(
            category="development",
            summary="Project announced mainnet launch for Sep 20.",
            significance=SIGNIFICANCE_HIGH,
            information_scope=SCOPE_PROJECT,
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(tweet_id="mainnet", author_username="ProjectUser", text="Mainnet launches Sep 20", matches=[])
    )

    rows = all_memories(ctx)
    assert adapter.calls == 1
    assert len(rows) == 1
    assert rows[0].category == "development"
    assert rows[0].summary == "Project announced mainnet launch for Sep 20."
    assert rows[0].significance == SIGNIFICANCE_HIGH
    assert rows[0].confidence == CONFIDENCE_HIGH
    assert rows[0].information_scope == SCOPE_PROJECT
    assert rows[0].tweet_url == "https://x.com/ProjectUser/status/mainnet"
    assert rows[0].raw_reference_hash is None


def test_security_update_keep_memory(ctx) -> None:
    adapter = FakeTriageAdapter(
        keep_result(
            category=CATEGORY_SECURITY,
            summary="Project published an audit update.",
            significance=SIGNIFICANCE_MEDIUM,
            information_scope=SCOPE_SECURITY,
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(tweet_id="audit", author_username="ProjectUser", text="Audit update is live.", matches=[])
    )

    rows = all_memories(ctx)
    assert rows[0].category == CATEGORY_SECURITY
    assert memory.stats_snapshot()["memory_category_security"] == 1


def test_token_migration_keep_memory(ctx) -> None:
    adapter = FakeTriageAdapter(
        keep_result(
            category=CATEGORY_TOKENOMICS,
            summary="Project announced a token migration.",
            significance=SIGNIFICANCE_HIGH,
            information_scope=SCOPE_TOKEN,
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(tweet_id="migration", author_username="ProjectUser", text="Token migration begins tomorrow.", matches=[])
    )

    assert all_memories(ctx)[0].category == CATEGORY_TOKENOMICS


def test_generic_price_shill_is_no_memory_without_llm(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="ROBBIE to the moon buy now", matches=[])
    )

    assert adapter.calls == 0
    assert all_memories(ctx) == []


def test_explicit_cashtag_shill_is_no_memory_without_llm(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="$ROBBIE to the moon", matches=[])
    )

    assert adapter.calls == 0
    assert memory.stats_snapshot()["deterministic_no_memory"] == 1
    assert all_memories(ctx) == []


def test_image_only_empty_text_is_no_memory(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="", matches=[])
    )

    assert adapter.calls == 0
    assert all_memories(ctx) == []


def test_same_tweet_replay_does_not_repeat_triage_or_memory(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Project announced mainnet launch for Sep 20."))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event_service = SocialEventService(ctx, memory_processor=memory)
    post = tweet(tweet_id="same", author_username="ProjectUser", text="Mainnet launches Sep 20", matches=[])

    event_service.create_event_if_relevant(post)
    event_service.create_event_if_relevant(post)

    assert adapter.calls == 1
    assert len(all_memories(ctx)) == 1


def test_same_tweet_address_different_chain_can_save_two_memories(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Project announced mainnet launch for Sep 20."))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    memory.process_event(memory_event(chain="robinhood", tweet_id="same-cross-chain"))
    memory.process_event(memory_event(chain="base", tweet_id="same-cross-chain"))

    rows = all_memories(ctx)
    assert adapter.calls == 2
    assert len(rows) == 2
    assert {row.chain for row in rows} == {"robinhood", "base"}


def test_same_tweet_address_same_chain_still_only_one_memory(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Project announced mainnet launch for Sep 20."))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event = memory_event(chain="robinhood", tweet_id="same-chain")

    memory.process_event(event)
    memory.process_event(event)

    assert adapter.calls == 1
    assert len(all_memories(ctx)) == 1


def test_processed_keys_are_chain_aware(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Project announced mainnet launch for Sep 20."))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    memory.process_event(memory_event(chain="robinhood", tweet_id="chain-aware"))
    memory.process_event(memory_event(chain="base", tweet_id="chain-aware"))

    assert adapter.calls == 2


def test_llm_timeout_fails_closed(ctx) -> None:
    adapter = FakeTriageAdapter(TimeoutError("llm timeout"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="Mainnet launches Sep 20", matches=[])
    )

    assert memory.stats_snapshot()["triage_failures"] == 1
    assert all_memories(ctx) == []


def test_triage_failure_does_not_terminally_mark_processed_and_allows_retry(ctx) -> None:
    adapter = SequenceTriageAdapter(
        [
            TimeoutError("llm timeout"),
            keep_result(summary="Project announced mainnet launch for Sep 20."),
        ]
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event = memory_event(tweet_id="retry-after-timeout")

    memory.process_event(event)
    memory.process_event(event)

    rows = all_memories(ctx)
    assert adapter.calls == 2
    assert memory.stats_snapshot()["triage_failures"] == 1
    assert len(rows) == 1
    assert rows[0].tweet_id == "retry-after-timeout"


def test_invalid_schema_fails_closed(ctx) -> None:
    adapter = FakeTriageAdapter(
        {
            "decision": DECISION_KEEP_MEMORY,
            "category": "made_up",
            "significance": SIGNIFICANCE_HIGH,
            "summary": "Bad schema",
            "reason": "bad",
            "confidence": CONFIDENCE_HIGH,
            "information_scope": SCOPE_PROJECT,
        }
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="Mainnet launches Sep 20", matches=[])
    )

    assert memory.stats_snapshot()["triage_failures"] == 1
    assert all_memories(ctx) == []


def test_strict_json_missing_field_is_invalid() -> None:
    payload = {
        "decision": DECISION_KEEP_MEMORY,
        "category": "development",
        "significance": SIGNIFICANCE_MEDIUM,
        "summary": "Project announced mainnet launch for Sep 20.",
        "reason": "Contains a durable project fact.",
        "confidence": CONFIDENCE_HIGH,
    }

    with pytest.raises(ValueError, match="invalid triage schema keys"):
        validate_triage_result(payload)


def test_strict_json_extra_field_is_invalid() -> None:
    payload = {
        "decision": DECISION_KEEP_MEMORY,
        "category": "development",
        "significance": SIGNIFICANCE_MEDIUM,
        "summary": "Project announced mainnet launch for Sep 20.",
        "reason": "Contains a durable project fact.",
        "confidence": CONFIDENCE_HIGH,
        "information_scope": SCOPE_PROJECT,
        "extra": "not allowed",
    }

    with pytest.raises(ValueError, match="invalid triage schema keys"):
        validate_triage_result(payload)


def test_strict_json_exact_required_fields_is_valid() -> None:
    result = validate_triage_result(
        {
            "decision": DECISION_KEEP_MEMORY,
            "category": "development",
            "significance": SIGNIFICANCE_MEDIUM,
            "summary": "Project announced mainnet launch for Sep 20.",
            "reason": "Contains a durable project fact.",
            "confidence": CONFIDENCE_HIGH,
            "information_scope": SCOPE_PROJECT,
        }
    )

    assert result.decision == DECISION_KEEP_MEMORY
    assert result.category == "development"


def test_keep_memory_low_information_category_fails_closed(ctx) -> None:
    adapter = FakeTriageAdapter(
        {
            "decision": DECISION_KEEP_MEMORY,
            "category": CATEGORY_LOW_INFORMATION,
            "significance": SIGNIFICANCE_MEDIUM,
            "summary": "Project announced mainnet launch for Sep 20.",
            "reason": "Contains a durable project fact.",
            "confidence": CONFIDENCE_HIGH,
            "information_scope": SCOPE_PROJECT,
        }
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="Mainnet launches Sep 20", matches=[])
    )

    assert memory.stats_snapshot()["triage_failures"] == 1
    assert all_memories(ctx) == []


def test_triage_no_memory_does_not_persist(ctx) -> None:
    adapter = FakeTriageAdapter(
        SocialTriageResult(
            decision=DECISION_NO_MEMORY,
            category=CATEGORY_LOW_INFORMATION,
            significance="low",
            summary="",
            reason="Generic hype.",
            confidence="high",
            information_scope="unknown",
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="Big announcement soon for the community.", matches=[])
    )

    assert memory.stats_snapshot()["triage_no_memory"] == 1
    assert all_memories(ctx) == []


def test_valid_no_memory_is_terminal_processed(ctx) -> None:
    adapter = FakeTriageAdapter(
        SocialTriageResult(
            decision=DECISION_NO_MEMORY,
            category=CATEGORY_LOW_INFORMATION,
            significance="low",
            summary="",
            reason="Generic hype.",
            confidence="high",
            information_scope="unknown",
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event = memory_event(tweet_id="no-memory-terminal", text="Big announcement soon for the community.")

    memory.process_event(event)
    memory.process_event(event)

    assert adapter.calls == 1
    assert memory.stats_snapshot()["triage_no_memory"] == 1
    assert memory.stats_snapshot()["duplicate_tweets_skipped"] == 1


def test_deterministic_no_memory_is_terminal_processed(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    memory = SocialMemoryService(ctx, triage_adapter=adapter)
    event = memory_event(tweet_id="deterministic-terminal", text="GM $ROBBIE 🚀")

    memory.process_event(event)
    memory.process_event(event)

    assert adapter.calls == 0
    assert memory.stats_snapshot()["deterministic_no_memory"] == 1
    assert memory.stats_snapshot()["duplicate_tweets_skipped"] == 1


def test_memory_already_exists_skips_llm_and_marks_processed(ctx) -> None:
    first = SocialMemoryService(ctx, triage_adapter=FakeTriageAdapter(keep_result()))
    event = memory_event(tweet_id="already-exists")
    first.process_event(event)
    adapter = FakeTriageAdapter(keep_result(summary="Should not be used"))
    second = SocialMemoryService(ctx, triage_adapter=adapter)

    second.process_event(event)
    second.process_event(event)

    assert adapter.calls == 0
    assert second.stats_snapshot()["duplicate_tweets_skipped"] == 2
    assert len(all_memories(ctx)) == 1


def test_integrity_error_race_is_not_fatal_and_terminal_processed(ctx) -> None:
    first = SocialMemoryService(ctx, triage_adapter=FakeTriageAdapter(keep_result()))
    event = memory_event(tweet_id="race")
    first.process_event(event)
    adapter = FakeTriageAdapter(keep_result(summary="Should only be triaged once in racing worker"))
    racing_worker = SocialMemoryService(ctx, triage_adapter=adapter)
    racing_worker._memory_exists = lambda session, event: False  # noqa: ARG005, SLF001

    racing_worker.process_event(event)
    racing_worker.process_event(event)

    assert adapter.calls == 1
    assert racing_worker.stats_snapshot()["duplicate_tweets_skipped"] == 2
    assert len(all_memories(ctx)) == 1


def test_no_memory_low_information_category_is_valid(ctx) -> None:
    adapter = FakeTriageAdapter(
        SocialTriageResult(
            decision=DECISION_NO_MEMORY,
            category=CATEGORY_LOW_INFORMATION,
            significance="low",
            summary="",
            reason="Generic hype.",
            confidence="high",
            information_scope="unknown",
        )
    )
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="ProjectUser", text="Big announcement soon for the community.", matches=[])
    )

    assert memory.stats_snapshot()["triage_no_memory"] == 1
    assert memory.stats_snapshot()["triage_failures"] == 0
    assert all_memories(ctx) == []


def test_semantic_triage_prompt_declares_required_schema_fields() -> None:
    prompt = JsonSocialSemanticTriageAdapter.system_prompt()

    for field in (
        "decision",
        "category",
        "significance",
        "summary",
        "reason",
        "confidence",
        "information_scope",
    ):
        assert field in prompt
    assert "KEEP_MEMORY" in prompt
    assert "NO_MEMORY" in prompt
    assert "low_information" in prompt
    assert "must not use investment language" in prompt


def test_kol_author_type_does_not_enter_memory_triage(ctx) -> None:
    adapter = FakeTriageAdapter(keep_result())
    memory = SocialMemoryService(ctx, triage_adapter=adapter)

    SocialEventService(ctx, memory_processor=memory).create_event_if_relevant(
        tweet(author_username="kol_one"),
        known_kol_usernames={"kol_one"},
    )

    assert adapter.calls == 0
    assert memory.stats_snapshot()["project_dev_candidates"] == 0
    assert all_memories(ctx) == []


def test_memory_query_token_time_category_limit(ctx) -> None:
    memory = SocialMemoryService(ctx, triage_adapter=FakeTriageAdapter(keep_result(category="development")))
    event_service = SocialEventService(ctx, memory_processor=memory)
    event_service.create_event_if_relevant(
        tweet(tweet_id="old", author_username="ProjectUser", text="Mainnet launches Sep 20", created_at=datetime(2026, 9, 9, 1, 0, tzinfo=UTC), matches=[])
    )
    memory.triage_adapter = FakeTriageAdapter(keep_result(category=CATEGORY_SECURITY, summary="Project published an audit update."))
    event_service.create_event_if_relevant(
        tweet(tweet_id="new", author_username="ProjectUser", text="Audit update is live.", created_at=datetime(2026, 9, 9, 2, 0, tzinfo=UTC), matches=[])
    )

    rows = memory.get_recent_memories(
        chain="robinhood",
        token_address=TOKEN,
        since=datetime(2026, 9, 9, 0, 0),
        categories={CATEGORY_SECURITY},
        limit=1,
    )

    assert len(rows) == 1
    assert rows[0].tweet_id == "new"
    assert rows[0].category == CATEGORY_SECURITY


def test_social_memory_has_no_retention_delete_job(ctx) -> None:
    assert not hasattr(SocialMemoryService, "cleanup_old_memories")


def all_memories(session_factory) -> list[SocialMemory]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        rows = list(session.scalars(select(SocialMemory).order_by(SocialMemory.id.asc())))
        for row in rows:
            session.expunge(row)
        return rows


def memory_event(
    *,
    chain: str = "robinhood",
    token_address: str = TOKEN,
    tweet_id: str = "memory-event",
    text: str = "Mainnet launches Sep 20",
    author_type: str = AUTHOR_PROJECT_X,
) -> SocialEvent:
    posted = datetime(2026, 9, 9, 1, 0)
    return SocialEvent(
        wallet_id=1,
        watch_state_id=1,
        chain=chain,
        token_address=token_address.lower(),
        symbol="ROBBIE",
        provider=PROVIDER,
        provider_event_id=tweet_id,
        author_id="author-1",
        author_username="ProjectUser",
        author_type=author_type,
        posted_at=posted,
        received_at=posted + timedelta(seconds=5),
        ingestion_type="stream",
        post_type="original",
        text=text,
        match_type="direct_ca",
        matched_value=token_address.lower(),
        tweet_url=f"https://x.com/ProjectUser/status/{tweet_id}",
    )


def identity(identity_type: str, value: str, normalized_value: str, now: datetime) -> SocialIdentity:
    return SocialIdentity(
        chain="robinhood",
        token_address=TOKEN,
        symbol="ROBBIE",
        identity_type=identity_type,
        value=value,
        normalized_value=normalized_value,
        source="test",
        source_field="test",
        confidence=HIGH,
        evidence_json=None,
        is_active=True,
        first_seen_at=now,
        last_verified_at=now,
        valid_from=now,
    )


def keep_result(
    *,
    category: str = "development",
    summary: str = "Project announced mainnet launch for Sep 20.",
    significance: str = SIGNIFICANCE_MEDIUM,
    information_scope: str = SCOPE_PROJECT,
) -> SocialTriageResult:
    return SocialTriageResult(
        decision=DECISION_KEEP_MEMORY,
        category=category,
        significance=significance,
        summary=summary,
        reason="Contains a durable project fact.",
        confidence=CONFIDENCE_HIGH,
        information_scope=information_scope,
    )


def tweet(
    *,
    tweet_id: str = "tweet-1",
    author_id: str | None = "author-1",
    author_username: str | None = "ProjectUser",
    text: str = f"Watching {TOKEN}",
    matches: list[TokenMatch] | None = None,
    created_at=None,
) -> NormalizedTweet:
    created = created_at or datetime(2026, 9, 9, 1, 0, tzinfo=UTC)
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=tweet_id,
        author_id=author_id,
        author_username=author_username,
        author_name=author_username,
        author_followers=1000,
        text=text,
        created_at=created,
        detected_at=created + timedelta(seconds=5),
        is_reply=False,
        in_reply_to_id=None,
        in_reply_to_username=None,
        conversation_id=tweet_id,
        is_quote=False,
        quoted_tweet_id=None,
        quoted_tweet=None,
        is_retweet=False,
        retweeted_tweet_id=None,
        retweeted_tweet=None,
        like_count=10,
        retweet_count=2,
        reply_count=1,
        quote_count=0,
        view_count=100,
        token_matches=matches if matches is not None else [TokenMatch("direct_ca", TOKEN, TOKEN)],
        raw={"not_saved": True},
    )
