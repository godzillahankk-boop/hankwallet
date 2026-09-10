from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialEvent, SocialIdentity, SocialMemory, TokenWatchState
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_memory_service import DECISION_KEEP_MEMORY, SocialMemoryService, SocialTriageResult
from app.services.wallet_service import WalletService
from app.utils.time import utc_now
import scripts.social_memory_live_shadow as live_shadow

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


class FakeRuleClient:
    def __init__(self, rules=None) -> None:  # noqa: ANN001
        self.rules = rules or []
        self.get_calls = 0
        self.add_calls = []
        self.update_calls = []

    def get_filter_rules(self):
        self.get_calls += 1
        return {"rules": self.rules}

    def add_filter_rule(self, *, tag: str, value: str, interval_seconds: float):
        self.add_calls.append((tag, value, interval_seconds))
        rule_id = f"created-{len(self.add_calls)}"
        self.rules.append(
            {
                "rule_id": rule_id,
                "tag": tag,
                "value": value,
                "interval_seconds": interval_seconds,
                "is_effect": 0,
            }
        )
        return {"rule_id": rule_id}

    def update_filter_rule(self, *, rule_id: str, tag: str, value: str, interval_seconds: float, is_effect: bool):
        self.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
        for rule in self.rules:
            if rule["rule_id"] == rule_id:
                rule.update(
                    {
                        "tag": tag,
                        "value": value,
                        "interval_seconds": interval_seconds,
                        "is_effect": 1 if is_effect else 0,
                    }
                )
        return {"status": "success"}


class FakeTriageAdapter:
    def __init__(self, results=None) -> None:  # noqa: ANN001
        self.results = results or {}
        self.calls: list[str | None] = []

    def triage(self, payload):  # noqa: ANN001
        self.calls.append(payload.text)
        result = self.results.get(payload.text)
        if isinstance(result, Exception):
            raise result
        return result or triage_result()


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/live_shadow.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 10, 11, 0, 0)
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
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(json.dumps({"kols": [{"username": "kol_one", "enabled": True}]}))
    return session_factory, config_path, tmp_path


@pytest.mark.asyncio
async def test_live_shadow_preflight_token_shadow_active_aborts(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("shadow", "wallet-agent-token-shadow-v01-ca-001", TOKEN, active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path)

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_TOKEN_SHADOW_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


@pytest.mark.asyncio
async def test_live_shadow_preflight_identity_probe_active_aborts(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("probe", "wallet-agent-token-identity-probe-ca-v01", TOKEN, active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path)

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_TOKEN_IDENTITY_PROBE_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


@pytest.mark.asyncio
async def test_live_shadow_preflight_unknown_active_rule_aborts(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("foreign", "other-user-rule", "from:someone", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path)

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_FOREIGN_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


@pytest.mark.asyncio
async def test_live_shadow_formal_rules_are_manageable(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert summary["runtime"]["aborted"] is False
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True
    assert any(call[4] is False for call in client.update_calls)


@pytest.mark.asyncio
async def test_live_shadow_does_not_instantiate_social_discovery(ctx, monkeypatch) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())
    monkeypatch.setattr(live_shadow, "SocialDiscoveryService", fail_if_called, raising=False)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert summary["runtime"]["aborted"] is False


@pytest.mark.asyncio
async def test_project_social_event_enters_real_memory_processor_path(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    adapter = FakeTriageAdapter({"Mainnet launches Sep 20.": triage_result(summary="Project announced mainnet launch for Sep 20.")})
    memory = SocialMemoryService(session_factory, triage_adapter=adapter)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()
        ingestor.connected_at = datetime(2026, 9, 10, 11, 59, 0, tzinfo=UTC)
        await ingestor.handle_message(stream_message(tweet_payload("project", "Mainnet launches Sep 20.", "ProjectUser")))

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert adapter.calls == ["Mainnet launches Sep 20."]
    assert summary["social_events"]["new_project_x_events"] == 1
    assert summary["social_memory_triage"]["triage_keep_memory"] == 1
    assert summary["social_memory_triage"]["social_memory_created"] == 1
    assert summary["new_social_memory_count"] == 1
    assert summary["new_social_memories"][0]["original_text"] == "Mainnet launches Sep 20."
    assert summary["new_social_memories"][0]["review_status"] == "UNREVIEWED"


@pytest.mark.asyncio
async def test_kol_social_event_does_not_enter_memory_triage(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    adapter = FakeTriageAdapter()
    memory = SocialMemoryService(session_factory, triage_adapter=adapter)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()
        ingestor.connected_at = datetime(2026, 9, 10, 11, 59, 0, tzinfo=UTC)
        await ingestor.handle_message(stream_message(tweet_payload("kol", TOKEN, "kol_one")))

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert adapter.calls == []
    assert summary["social_events"]["new_kol_events"] == 1
    assert summary["social_memory_triage"]["project_dev_candidates"] == 0
    assert summary["new_social_memory_count"] == 0


@pytest.mark.asyncio
async def test_new_memory_counts_only_current_run(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    existing = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())
    existing.process_event(memory_event("old-memory"))
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter({"Audit published.": triage_result(category="security")}))

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()
        ingestor.connected_at = datetime(2026, 9, 10, 11, 59, 0, tzinfo=UTC)
        await ingestor.handle_message(stream_message(tweet_payload("new-memory", "Audit published.", "ProjectUser")))

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert summary["new_social_memory_count"] == 1
    assert [row["tweet_id"] for row in summary["new_social_memories"]] == ["new-memory"]
    with session_scope(session_factory) as session:
        assert session.scalar(select(SocialMemory).where(SocialMemory.tweet_id == "old-memory")) is not None


@pytest.mark.asyncio
async def test_deepseek_single_failure_does_not_stop_runner(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    adapter = FakeTriageAdapter(
        {
            "Model failure sample.": RuntimeError("model down"),
            "Audit published.": triage_result(category="security"),
        }
    )
    memory = SocialMemoryService(session_factory, triage_adapter=adapter)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()
        ingestor.connected_at = datetime(2026, 9, 10, 11, 59, 0, tzinfo=UTC)
        await ingestor.handle_message(stream_message(tweet_payload("fail", "Model failure sample.", "ProjectUser")))
        await ingestor.handle_message(stream_message(tweet_payload("success", "Audit published.", "ProjectUser")))

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert adapter.calls == ["Model failure sample.", "Audit published."]
    assert summary["social_memory_triage"]["triage_failures"] == 1
    assert summary["social_memory_triage"]["social_memory_created"] == 1
    assert summary["provider_stream"]["provider_errors"] == 0


@pytest.mark.asyncio
async def test_normal_exit_stops_ingestor_before_pause(ctx, monkeypatch) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())
    order = []
    original_stop = live_shadow.TwitterApiIoSocialIngestor.stop

    async def tracked_stop(self):  # noqa: ANN001
        order.append("stop")
        await original_stop(self)

    def tracked_pause(client_arg, *, state_path, api_key):  # noqa: ANN001
        order.append("pause")
        return {"cleanup_failed": False, "cleanup_verified": True, "active_formal_rules_after_cleanup": [], "safe_to_stop_monitoring": True}

    monkeypatch.setattr(live_shadow.TwitterApiIoSocialIngestor, "stop", tracked_stop)
    monkeypatch.setattr(live_shadow, "pause_and_verify_formal_rules", tracked_pause)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()

    await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert order == ["stop", "pause"]


@pytest.mark.asyncio
async def test_exception_exit_still_cleans_up(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()
        raise RuntimeError("stream exploded")

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert summary["runtime"]["runner_error"] == "stream exploded"
    assert summary["cleanup"]["cleanup_verified"] is True
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True


@pytest.mark.asyncio
async def test_cleanup_after_formal_rules_all_inactive_is_safe(ctx) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert all(not row.get("is_effect") for row in client.rules if row["tag"].startswith("wallet-agent-social-v1"))
    assert summary["cleanup"]["active_formal_rules_after_cleanup"] == []
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True


@pytest.mark.asyncio
async def test_safe_to_stop_monitoring_false_when_cleanup_verification_finds_active_rule(ctx, monkeypatch) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())

    def unsafe_pause(client_arg, *, state_path, api_key):  # noqa: ANN001
        return {
            "cleanup_failed": False,
            "cleanup_verified": False,
            "active_formal_rules_after_cleanup": [{"tag": "wallet-agent-social-v1-001"}],
            "safe_to_stop_monitoring": False,
        }

    monkeypatch.setattr(live_shadow, "pause_and_verify_formal_rules", unsafe_pause)

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=lambda ingestor, duration: noop())

    assert summary["cleanup"]["safe_to_stop_monitoring"] is False


@pytest.mark.asyncio
async def test_does_not_call_attention_or_telegram(ctx, monkeypatch) -> None:
    session_factory, config_path, tmp_path = ctx
    client = FakeRuleClient([rule("formal", "wallet-agent-social-v1-001", "from:kol_one", active=True)])
    memory = SocialMemoryService(session_factory, triage_adapter=FakeTriageAdapter())
    monkeypatch.setattr(live_shadow, "AttentionEngineService", fail_if_called, raising=False)
    monkeypatch.setattr(live_shadow, "build_application", fail_if_called, raising=False)

    async def runner(ingestor, duration):  # noqa: ANN001
        await ingestor.refresh_rule()

    summary = await run_runner(client, session_factory, memory, config_path, tmp_path, ingestor_runner=runner)

    assert summary["runtime"]["aborted"] is False


async def run_runner(client, session_factory, memory, config_path, tmp_path, *, ingestor_runner=None):  # noqa: ANN001
    return await live_shadow.run_social_memory_live_shadow(
        client,
        api_key="secret-key",
        session_factory=session_factory,
        memory_service=memory,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=config_path,
        ingestor_runner=ingestor_runner or (lambda ingestor, duration: noop()),
    )


async def noop() -> None:
    return None


def fail_if_called(*args, **kwargs):  # noqa: ANN002, ANN003
    raise AssertionError("should not be called")


def rule(rule_id: str, tag: str, value: str, *, active: bool) -> dict:
    return {
        "rule_id": rule_id,
        "tag": tag,
        "value": value,
        "interval_seconds": 300,
        "is_effect": 1 if active else 0,
    }


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


def tweet_payload(tweet_id: str, text: str, author: str) -> dict:
    return {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-10T12:00:00Z",
        "author": {"id": author, "userName": author, "name": author, "followers": 1000},
        "likeCount": 1,
        "retweetCount": 2,
        "replyCount": 3,
        "quoteCount": 4,
        "viewCount": 5,
    }


def stream_message(tweet: dict) -> str:
    return json.dumps({"event_type": "tweet", "tweet": tweet, "rule_tag": "wallet-agent-social-v1-001"})


def triage_result(
    *,
    category: str = "development",
    summary: str = "Project published a factual update.",
) -> SocialTriageResult:
    return SocialTriageResult(
        decision=DECISION_KEEP_MEMORY,
        category=category,
        significance="medium",
        summary=summary,
        reason="test",
        confidence="high",
        information_scope="project",
    )


def memory_event(tweet_id: str) -> SocialEvent:
    posted = datetime(2026, 9, 10, 10, 0)
    return SocialEvent(
        wallet_id=1,
        watch_state_id=1,
        chain="robinhood",
        token_address=TOKEN,
        symbol="ROBBIE",
        provider="twitterapi.io",
        provider_event_id=tweet_id,
        author_id="author-project",
        author_username="ProjectUser",
        author_type=AUTHOR_PROJECT_X,
        posted_at=posted,
        received_at=posted + timedelta(seconds=5),
        ingestion_type="stream",
        post_type="original",
        text="Mainnet launches Sep 20.",
        match_type="direct_ca",
        matched_value=TOKEN,
        tweet_url=f"https://x.com/ProjectUser/status/{tweet_id}",
    )
