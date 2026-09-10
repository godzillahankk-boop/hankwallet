from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import TokenWatchState
from app.services.social_kol_service import SOURCE_TRUSTED_EXTERNAL_SEED, SocialKOLService
from app.services.twitter_token_shadow import (
    TOKEN_SHADOW_CA_RULE_TAG_PREFIX,
    TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX,
    TOKEN_SHADOW_RULE_TAG_PREFIX,
    TokenFirstRealtimeShadow,
    TokenShadowDesiredRules,
    TokenShadowShardStateStore,
    TokenShadowStats,
    TwitterTokenShadowRuleManager,
    WatchedTokenIdentity,
    build_stable_term_shards,
)
from app.services.wallet_service import WalletService
from scripts.twitter_token_shadow_rule_control import (
    TokenShadowRuleControlError,
    pause_token_shadow_rules,
    resume_token_shadow_rules,
    status_token_shadow_rules,
)
from scripts.twitter_token_shadow_run import run_token_shadow

WALLET_ADDRESS = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN_WALLET = "0x1111111111111111111111111111111111111111"
TOKEN_ROBBIE = "0x2222222222222222222222222222222222222222"
TOKEN_AI_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_AI_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


class FakeTokenShadowClient:
    def __init__(self, rules=None) -> None:  # noqa: ANN001
        self.rules = rules or []
        self.get_calls = 0
        self.add_calls = []
        self.update_calls = []
        self.fail_update_rule_ids: set[str] = set()

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
        if rule_id in self.fail_update_rule_ids:
            raise RuntimeError(f"update failed for {rule_id}")
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


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/token_shadow.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 9, 1, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET_ADDRESS, "robinhood")
        session.add_all(
            [
                watch(wallet.id, TOKEN_WALLET, "WALLET", now=now),
                watch(wallet.id, TOKEN_ROBBIE, "ROBBIE", now=now),
            ]
        )
    return session_factory


def test_watched_tokens_build_ca_and_unique_cashtag_desired_rules(ctx) -> None:
    session_factory = ctx
    now = datetime(2026, 9, 9, 1, 0, 0)
    with session_scope(session_factory) as session:
        wallet_id = session.query(TokenWatchState.wallet_id).first()[0]
        session.add(watch(wallet_id, TOKEN_AI_A, "WALLET", chain="base", now=now))

    desired = TokenFirstRealtimeShadow(session_factory=session_factory).desired_rules()

    assert desired.ca_terms == sorted([TOKEN_AI_A, TOKEN_ROBBIE, TOKEN_WALLET])
    assert desired.cashtag_terms == ["$ROBBIE", "$WALLET"]


@pytest.mark.asyncio
async def test_restart_same_watched_set_does_not_rewrite_provider_rules(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    desired = desired_rules(cashtag_terms=["$WALLET"])
    client = FakeTokenShadowClient()
    first = TwitterTokenShadowRuleManager(client, shard_state_path=state_path)
    await first.ensure_rules(desired, stats=TokenShadowStats())
    client.update_calls.clear()

    second = TwitterTokenShadowRuleManager(client, shard_state_path=state_path)
    await second.ensure_rules(desired, stats=TokenShadowStats())

    assert client.add_calls == [
        (f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001", TOKEN_WALLET, 300),
        (f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}001", "$WALLET", 300),
    ]
    assert client.update_calls == []


@pytest.mark.asyncio
async def test_adding_token_does_not_cascade_rewrite_unrelated_shards(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    stats = TokenShadowStats()
    client = FakeTokenShadowClient()
    manager = TwitterTokenShadowRuleManager(client, shard_state_path=state_path, max_value_chars=90)
    await manager.ensure_rules(desired_rules(ca_terms=[TOKEN_WALLET, TOKEN_ROBBIE, TOKEN_AI_A], cashtag_terms=[]), stats=stats)
    client.update_calls.clear()

    await manager.ensure_rules(
        desired_rules(ca_terms=[TOKEN_WALLET, TOKEN_ROBBIE, TOKEN_AI_A, TOKEN_AI_B], cashtag_terms=[]),
        stats=stats,
    )

    value_updates = [call for call in client.update_calls if call[4] is True]
    assert len(value_updates) == 1
    assert value_updates[0][1] == f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}002"


@pytest.mark.asyncio
async def test_deleting_token_only_affects_own_shard(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    client = FakeTokenShadowClient()
    manager = TwitterTokenShadowRuleManager(client, shard_state_path=state_path, max_value_chars=90)
    await manager.ensure_rules(desired_rules(ca_terms=[TOKEN_WALLET, TOKEN_ROBBIE, TOKEN_AI_A], cashtag_terms=[]))
    client.update_calls.clear()

    await manager.ensure_rules(desired_rules(ca_terms=[TOKEN_WALLET, TOKEN_AI_A], cashtag_terms=[]))

    assert len(client.update_calls) == 1
    assert client.update_calls[0][1] == f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001"


def test_stable_term_shards_do_not_compact_empty_shard(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "groups": {
                    "ca": {
                        f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001": [TOKEN_WALLET],
                        f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}002": [TOKEN_ROBBIE],
                    },
                    "cashtag": {},
                },
            }
        )
    )
    store = TokenShadowShardStateStore(state_path)

    shards, changed = build_stable_term_shards(
        [TOKEN_ROBBIE],
        group="ca",
        tag_prefix=TOKEN_SHADOW_CA_RULE_TAG_PREFIX,
        store=store,
        max_value_chars=90,
    )

    assert changed is True
    assert shards == [(f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}002", TOKEN_ROBBIE)]
    saved = json.loads(state_path.read_text())
    assert saved["groups"]["ca"][f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001"] == []


def test_malformed_shard_state_fails_safe(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{bad")
    store = TokenShadowShardStateStore(path)

    with pytest.raises(Exception, match="malformed"):
        build_stable_term_shards(
            [TOKEN_WALLET],
            group="ca",
            tag_prefix=TOKEN_SHADOW_CA_RULE_TAG_PREFIX,
            store=store,
        )


def test_shadow_message_multi_token_match_uses_distinct_kol_author(ctx) -> None:
    session_factory = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="nancy",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    shadow = TokenFirstRealtimeShadow(session_factory=session_factory)

    shadow.handle_message(stream_message(tweet("t1", "$WALLET $ROBBIE", author_id="nancy", username="NancyCrypto")))

    snap = shadow.stats_snapshot()
    assert snap["cashtag_matches"] == 2
    assert snap["matched_tweet_hits"] == 1
    assert snap["token_match_results"] == 2
    assert snap["token_match_rate"] == 1.0
    assert snap["qualified_kol_tweet_hits"] == 1
    assert snap["distinct_qualified_kol_authors"] == 1
    assert snap["token_kol_pairs"] == 2
    assert snap["tokens_with_kol_hits"] == 2


def test_unqualified_cashtag_does_not_count_kol_hit(ctx) -> None:
    shadow = TokenFirstRealtimeShadow(session_factory=ctx)

    shadow.handle_message(stream_message(tweet("t1", "$WALLET", author_id="unknown", username="Unknown")))

    snap = shadow.stats_snapshot()
    assert snap["unqualified_author_hits"] == 1
    assert snap["cashtag_matches"] == 0
    assert snap["token_kol_pairs"] == 0


def test_exact_ca_from_unqualified_author_matches_token_but_not_kol(ctx) -> None:
    shadow = TokenFirstRealtimeShadow(session_factory=ctx)

    shadow.handle_message(stream_message(tweet("t1", TOKEN_WALLET, author_id="unknown", username="Unknown")))

    snap = shadow.stats_snapshot()
    assert snap["exact_ca_matches"] == 1
    assert snap["token_match_rate"] == 1.0
    assert snap["token_kol_pairs"] == 0


def test_same_kol_multiple_tweets_and_ca_cashtag_count_once_per_token(ctx) -> None:
    session_factory = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="nancy",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    shadow = TokenFirstRealtimeShadow(session_factory=session_factory)

    shadow.handle_message(stream_message(tweet("t1", "$WALLET", author_id="nancy", username="NancyCrypto")))
    shadow.handle_message(stream_message(tweet("t2", "$WALLET", author_id="nancy", username="NancyCrypto")))
    shadow.handle_message(stream_message(tweet("t3", TOKEN_WALLET, author_id="nancy", username="NancyCrypto")))

    snap = shadow.stats_snapshot()
    assert snap["qualified_kol_tweet_hits"] == 3
    assert snap["distinct_qualified_kol_authors"] == 1
    assert snap["token_kol_pairs"] == 1


def test_symbol_collision_is_ambiguous_and_not_kol_pair(ctx) -> None:
    session_factory = ctx
    with session_scope(session_factory) as session:
        wallet_id = session.query(TokenWatchState.wallet_id).first()[0]
        session.add(watch(wallet_id, TOKEN_AI_A, "AI", chain="robinhood"))
        session.add(watch(wallet_id, TOKEN_AI_B, "AI", chain="base"))
    SocialKOLService(session_factory).observe_candidate(
        author_id="nancy",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    shadow = TokenFirstRealtimeShadow(session_factory=session_factory)

    shadow.handle_message(stream_message(tweet("t1", "$AI", author_id="nancy", username="NancyCrypto")))

    snap = shadow.stats_snapshot()
    assert snap["ambiguous_symbol_hits"] == 1
    assert snap["token_kol_pairs"] == 0


def test_warmup_and_steady_duplicate_metrics_are_separate(ctx) -> None:
    shadow = TokenFirstRealtimeShadow(session_factory=ctx, warmup_seconds=120)
    shadow.last_rule_changed_at = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)

    shadow.handle_message(
        stream_message(tweet("warm", TOKEN_WALLET)),
        received_at=datetime(2026, 9, 9, 1, 0, 30, tzinfo=UTC),
    )
    shadow.handle_message(
        stream_message(tweet("steady", TOKEN_WALLET)),
        received_at=datetime(2026, 9, 9, 1, 5, 0, tzinfo=UTC),
    )
    shadow.handle_message(
        stream_message(tweet("steady", TOKEN_WALLET)),
        received_at=datetime(2026, 9, 9, 1, 5, 10, tzinfo=UTC),
    )

    snap = shadow.stats_snapshot()
    assert snap["startup_warmup_deliveries"] == 1
    assert snap["steady_deliveries"] == 2
    assert snap["steady_unique"] == 1
    assert snap["steady_duplicates"] == 1
    assert snap["duplicates"] == 1


def test_retweets_are_ignored_and_counted(ctx) -> None:
    shadow = TokenFirstRealtimeShadow(session_factory=ctx)

    shadow.handle_message(stream_message(tweet("rt", TOKEN_WALLET, retweet=True)))

    snap = shadow.stats_snapshot()
    assert snap["retweets_received"] == 1
    assert snap["retweets_ignored"] == 1
    assert snap["exact_ca_matches"] == 0


def test_token_shadow_rule_control_pause_resume_only_token_shadow_rules(tmp_path) -> None:
    client = FakeTokenShadowClient(
        [
            rule("shadow-ca", f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001", TOKEN_WALLET, active=True),
            rule("shadow-old", f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}002", "$OLD", active=False),
            rule("formal", "wallet-agent-social-v1-001", "from:kol", active=True),
            rule("probe", "wallet-agent-token-identity-probe-ca-v01", TOKEN_WALLET, active=True),
        ]
    )
    state_path = tmp_path / "paused.json"

    status = status_token_shadow_rules(client)
    paused = pause_token_shadow_rules(client, state_path=state_path)
    client.update_calls.clear()
    resumed = resume_token_shadow_rules(client, state_path=state_path)

    assert [row["tag"] for row in status["rules"]] == [
        f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001",
        f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}002",
    ]
    assert paused["updated"][0]["tag"] == f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001"
    assert client.rules[2]["is_effect"] == 1
    assert client.rules[3]["is_effect"] == 1
    assert client.update_calls == [("shadow-ca", f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001", TOKEN_WALLET, 300, True)]
    assert resumed["updated"][0]["rule_id"] == "shadow-ca"
    assert client.rules[1]["is_effect"] == 0


def test_token_shadow_rule_control_resume_without_state_refuses(tmp_path) -> None:
    client = FakeTokenShadowClient()

    with pytest.raises(TokenShadowRuleControlError, match="No paused token shadow rule state"):
        resume_token_shadow_rules(client, state_path=tmp_path / "missing.json")


def test_runner_preflight_formal_social_active_aborts_without_shadow_write(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient([rule("formal", "wallet-agent-social-v1-001", "from:kol", active=True)])

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_FORMAL_SOCIAL_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_runner_preflight_unknown_active_rule_aborts(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient([rule("other", "other-rule", "from:other", active=True)])

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_FOREIGN_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_runner_preflight_identity_probe_active_aborts(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient([rule("probe", "wallet-agent-token-identity-probe-ca-v01", TOKEN_WALLET, active=True)])

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_TOKEN_IDENTITY_PROBE_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_runner_allows_inactive_foreign_and_sets_warmup_on_provider_change(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient([rule("formal", "wallet-agent-social-v1-001", "from:kol", active=False)])

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        assert shadow.last_rule_changed_at is not None
        shadow.handle_message(
            stream_message(tweet("warm", TOKEN_WALLET)),
            received_at=shadow.last_rule_changed_at + timedelta(seconds=1),
        )

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["runtime"]["aborted"] is False
    assert summary["rules"]["provider_changed"] is True
    assert summary["warmup"]["startup_warmup_deliveries"] == 1
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True


def test_runner_restart_without_provider_write_does_not_open_warmup(ctx, tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "groups": {
                    "ca": {f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001": [TOKEN_WALLET, TOKEN_ROBBIE]},
                    "cashtag": {f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}001": ["$WALLET", "$ROBBIE"]},
                },
            }
        )
    )
    client = FakeTokenShadowClient(
        [
            rule("ca", f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001", f"{TOKEN_WALLET} OR {TOKEN_ROBBIE}", active=True),
            rule("tag", f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}001", "$WALLET OR $ROBBIE", active=True),
        ]
    )

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        assert shadow.last_rule_changed_at is None
        shadow.handle_message(stream_message(tweet("steady", TOKEN_WALLET)))

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=state_path,
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["rules"]["provider_changed"] is False
    assert summary["warmup"]["startup_warmup_deliveries"] == 0
    assert summary["steady"]["steady_unique"] == 1


def test_runner_tweet_level_match_rate_never_exceeds_one_for_multi_match(ctx, tmp_path) -> None:
    session_factory = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="nancy",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    client = FakeTokenShadowClient()

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        shadow.last_rule_changed_at = None
        shadow.handle_message(stream_message(tweet("multi", "$WALLET $ROBBIE", author_id="nancy", username="NancyCrypto")))

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=session_factory,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["matching"]["matched_tweet_hits"] == 1
    assert summary["matching"]["token_match_results"] == 2
    assert summary["matching"]["token_match_rate"] == 1.0
    assert summary["qualified_kol"]["qualified_kol_tweet_hits"] == 1
    assert summary["qualified_kol"]["token_kol_pairs"] == 2


def test_runner_warmup_match_does_not_enter_steady_efficiency(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient()

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        shadow.handle_message(
            stream_message(tweet("warm", TOKEN_WALLET)),
            received_at=shadow.last_rule_changed_at + timedelta(seconds=1),
        )

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["matching"]["matched_tweet_hits"] == 1
    assert summary["steady_efficiency"]["steady_matched_tweet_hits"] == 0
    assert summary["steady_efficiency"]["steady_token_match_rate"] == 0.0


def test_runner_warmup_tweet_replayed_in_steady_is_duplicate_not_new_signal(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient()

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        changed_at = shadow.last_rule_changed_at
        shadow.handle_message(
            stream_message(tweet("same", TOKEN_WALLET)),
            received_at=changed_at + timedelta(seconds=1),
        )
        shadow.handle_message(
            stream_message(tweet("same", TOKEN_WALLET)),
            received_at=changed_at + timedelta(minutes=5),
        )

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["warmup"]["startup_warmup_deliveries"] == 1
    assert summary["steady"]["steady_duplicates"] == 1
    assert summary["matching"]["matched_tweet_hits"] == 1
    assert summary["steady_efficiency"]["steady_matched_tweet_hits"] == 0


def test_runner_exception_still_pauses_token_shadow_rules(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient()

    def runner(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("ws failed")

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=runner,
    )

    assert summary["runtime"]["provider_errors"] == 1
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True
    assert all(not rule["is_effect"] for rule in client.rules if rule["tag"].startswith(TOKEN_SHADOW_RULE_TAG_PREFIX))


def test_runner_cleanup_verification_detects_remaining_active_shadow_rule(ctx, tmp_path) -> None:
    client = FakeTokenShadowClient()

    def keep_first_active(*, rule_id: str, tag: str, value: str, interval_seconds: float, is_effect: bool):
        client.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
        if is_effect is False and tag.startswith(TOKEN_SHADOW_CA_RULE_TAG_PREFIX):
            return {"status": "success"}
        for rule_row in client.rules:
            if rule_row["rule_id"] == rule_id:
                rule_row.update({"tag": tag, "value": value, "interval_seconds": interval_seconds, "is_effect": 1 if is_effect else 0})
        return {"status": "success"}

    client.update_filter_rule = keep_first_active

    summary = run_token_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx,
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["cleanup"]["cleanup_verified"] is False
    assert summary["cleanup"]["safe_to_stop_monitoring"] is False
    assert summary["cleanup"]["active_token_shadow_rules_after_cleanup"]


def test_token_shadow_pause_partial_failure_persists_successful_state_and_continues(tmp_path) -> None:
    client = FakeTokenShadowClient(
        [
            rule("ca", f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001", TOKEN_WALLET, active=True),
            rule("tag", f"{TOKEN_SHADOW_CASHTAG_RULE_TAG_PREFIX}001", "$WALLET", active=True),
            rule("formal", "wallet-agent-social-v1-001", "from:kol", active=True),
        ]
    )
    client.fail_update_rule_ids = {"tag"}
    state_path = tmp_path / "paused.json"

    result = pause_token_shadow_rules(client, state_path=state_path)

    assert result["cleanup_failed"] is True
    assert {call[0] for call in client.update_calls} == {"ca", "tag"}
    state = json.loads(state_path.read_text())
    assert state["paused_rules"] == [
        {
            "rule_id": "ca",
            "tag": f"{TOKEN_SHADOW_CA_RULE_TAG_PREFIX}001",
            "paused_at": state["paused_rules"][0]["paused_at"],
        }
    ]
    assert client.rules[2]["is_effect"] == 1


def watch(
    wallet_id: int,
    token: str,
    symbol: str,
    *,
    chain: str = "robinhood",
    now: datetime | None = None,
    active: bool = True,
) -> TokenWatchState:
    ts = now or datetime(2026, 9, 9, 1, 0, 0)
    return TokenWatchState(
        wallet_id=wallet_id,
        chain=chain,
        token_address=token,
        symbol=symbol,
        active=active,
        started_at=ts - timedelta(hours=1),
        last_seen_at=ts,
    )


def desired_rules(
    *,
    ca_terms: list[str] | None = None,
    cashtag_terms: list[str] | None = None,
) -> TokenShadowDesiredRules:
    watched_tokens = [
        WatchedTokenIdentity("robinhood", term, "TKN")
        for term in (ca_terms or [TOKEN_WALLET])
    ]
    return TokenShadowDesiredRules(
        watched_tokens=watched_tokens,
        ca_terms=ca_terms or [TOKEN_WALLET],
        cashtag_terms=cashtag_terms or [],
    )


def stream_message(tweet_payload: dict) -> str:
    return json.dumps({"event_type": "tweet", "rule_id": "rule-1", "rule_tag": "tag-1", "tweet": tweet_payload})


def tweet(
    tweet_id: str,
    text: str,
    *,
    author_id: str = "author",
    username: str = "Author",
    retweet: bool = False,
) -> dict:
    payload = {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-09T01:00:00Z",
        "author": {"id": author_id, "userName": username, "followers": 5000},
    }
    if retweet:
        payload["retweeted_tweet"] = {"id": f"rt-{tweet_id}", "text": text}
    return payload


def rule(rule_id: str, tag: str, value: str, *, active: bool) -> dict:
    return {
        "rule_id": rule_id,
        "tag": tag,
        "value": value,
        "interval_seconds": 300,
        "is_effect": 1 if active else 0,
    }
