from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import TokenWatchState
from app.services.social_kol_service import SOURCE_TRUSTED_EXTERNAL_SEED, SocialKOLService
from app.services.twitterapi_io_social_ingestion import SOCIAL_RULE_TAG_PREFIX
from app.services.wallet_service import WalletService
import scripts.twitter_author_cashtag_shadow_run as runner_module
from scripts.twitter_author_cashtag_shadow_run import run_author_cashtag_shadow

WALLET_ADDRESS = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN_WALLET = "0x1111111111111111111111111111111111111111"
TOKEN_ROBBIE = "0x2222222222222222222222222222222222222222"


class FakeAuthorShadowClient:
    def __init__(self, rules=None) -> None:  # noqa: ANN001
        self.rules = rules or []
        self.get_calls = 0
        self.add_calls = []
        self.update_calls = []
        self.noop_deactivate_tags: set[str] = set()

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
        if is_effect is False and tag in self.noop_deactivate_tags:
            return {"status": "success"}
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


def test_author_shadow_runner_main_passes_settings_kol_config_path(monkeypatch, tmp_path, capsys) -> None:
    captured = {}
    config_path = tmp_path / "custom_social_kols.json"
    config_path.write_text(json.dumps({"kols": []}))
    settings = SimpleNamespace(
        twitterapi_io_api_key="secret-key",
        database_url=f"sqlite:///{tmp_path}/prod_wiring.db",
        social_x_rule_shard_state_path=str(tmp_path / "shards.json"),
        social_x_kol_config_path=str(config_path),
        social_x_websocket_url="wss://example.test/ws",
    )

    monkeypatch.setattr(runner_module, "load_settings", lambda: settings)
    monkeypatch.setattr(runner_module, "make_engine", lambda database_url: ("engine", database_url))
    monkeypatch.setattr(runner_module, "init_db", lambda engine: None)
    monkeypatch.setattr(runner_module, "make_session_factory", lambda engine: "session_factory")
    monkeypatch.setattr(runner_module, "TwitterApiIoClient", lambda api_key: ("client", api_key))
    def fake_run_author_cashtag_shadow(client, **kwargs):  # noqa: ANN001
        captured["client"] = client
        captured["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(runner_module, "run_author_cashtag_shadow", fake_run_author_cashtag_shadow)
    monkeypatch.setattr(
        runner_module.sys,
        "argv",
        [
            "twitter_author_cashtag_shadow_run.py",
            "--duration",
            "1",
            "--interval",
            "300",
            "--output-root",
            str(tmp_path / "outputs"),
        ],
    )

    runner_module.main()

    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert captured["kwargs"]["kol_config_path"] == str(config_path)
    assert captured["kwargs"]["shard_state_path"] == str(tmp_path / "shards.json")


def test_author_shadow_runner_requires_kol_config_path(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient()

    with pytest.raises(ValueError, match="kol_config_path is required"):
        run_author_cashtag_shadow(
            client,
            api_key="secret-key",
            session_factory=ctx[0],
            duration_seconds=1,
            interval_seconds=300,
            output_root=None,
            websocket_runner=fail_if_called,
            shard_state_path=tmp_path / "shards.json",
            pause_state_path=tmp_path / "paused.json",
            kol_config_path=None,
        )


def test_author_shadow_runner_preflight_token_shadow_active_aborts_without_formal_write(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("shadow", "wallet-agent-token-shadow-v01-ca-001", TOKEN_WALLET, active=True)])

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=fail_if_called,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_TOKEN_SHADOW_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_author_shadow_runner_preflight_identity_probe_active_aborts(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("probe", "wallet-agent-token-identity-probe-ca-v01", TOKEN_WALLET, active=True)])

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=fail_if_called,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_TOKEN_IDENTITY_PROBE_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_author_shadow_runner_preflight_unknown_active_rule_aborts(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("other", "other-user-rule", "from:other", active=True)])

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=fail_if_called,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["runtime"]["aborted"] is True
    assert summary["runtime"]["abort_reason"] == "ACTIVE_FOREIGN_RULES_DETECTED"
    assert client.add_calls == []
    assert client.update_calls == []


def test_author_shadow_runner_allows_and_manages_formal_rules(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=lambda *args, **kwargs: None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["runtime"]["aborted"] is False
    assert summary["rules"]["provider_changed"] is False
    assert client.update_calls == [("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", 300, False)]
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True


def test_author_shadow_runner_provider_create_triggers_warmup(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient()

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        assert shadow.last_rule_changed_at is not None
        shadow.handle_message(
            stream_message(tweet_payload("warm", "$WALLET")),
            received_at=shadow.last_rule_changed_at + timedelta(seconds=1),
        )

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["rules"]["provider_changed"] is True
    assert summary["warmup"]["startup_warmup_deliveries"] == 1
    assert summary["matching"]["matched_tweet_hits"] == 1
    assert summary["steady_incremental"]["steady_matched_tweet_hits"] == 0


def test_author_shadow_runner_provider_unchanged_does_not_open_warmup(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        assert shadow.last_rule_changed_at is None
        shadow.handle_message(stream_message(tweet_payload("steady", TOKEN_WALLET)))

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["rules"]["provider_changed"] is False
    assert summary["warmup"]["startup_warmup_deliveries"] == 0
    assert summary["steady"]["steady_unique"] == 1
    assert summary["steady_incremental"]["steady_exact_ca_token_kol_pairs"] == 1


def test_author_shadow_runner_warmup_replay_is_steady_duplicate_not_signal(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient()

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        changed_at = shadow.last_rule_changed_at
        shadow.handle_message(stream_message(tweet_payload("same", "$WALLET")), received_at=changed_at + timedelta(seconds=1))
        shadow.handle_message(stream_message(tweet_payload("same", "$WALLET")), received_at=changed_at + timedelta(minutes=5))

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["warmup"]["startup_warmup_deliveries"] == 1
    assert summary["steady"]["steady_duplicates"] == 1
    assert summary["steady_incremental"]["steady_token_kol_pairs"] == 0
    assert summary["matching"]["cashtag_only_matched_tweet_hits"] == 1


def test_author_shadow_runner_steady_cashtag_and_exact_pair_metrics(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        shadow.handle_message(stream_message(tweet_payload("tag", "$WALLET")))
        shadow.handle_message(stream_message(tweet_payload("ca", TOKEN_ROBBIE)))

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["steady_incremental"]["steady_cashtag_only_token_kol_pairs"] == 1
    assert summary["steady_incremental"]["steady_exact_ca_token_kol_pairs"] == 1
    assert summary["steady_incremental"]["steady_token_kol_pairs"] == 2


def test_author_shadow_runner_same_kol_ca_and_cashtag_still_one_pair(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    def runner(_ws_url, _api_key, *, shadow, duration_seconds):  # noqa: ANN001
        shadow.handle_message(stream_message(tweet_payload("tag", "$WALLET")))
        shadow.handle_message(stream_message(tweet_payload("ca", TOKEN_WALLET)))

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["steady_incremental"]["steady_token_kol_pairs"] == 1
    assert summary["steady_incremental"]["steady_exact_ca_token_kol_pairs"] == 1
    assert summary["steady_incremental"]["steady_cashtag_only_token_kol_pairs"] == 0


def test_author_shadow_runner_exception_exit_still_pauses_formal_rules(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    def runner(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("stream down")

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=runner,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["runtime"]["provider_errors"] == 1
    assert summary["cleanup"]["safe_to_stop_monitoring"] is True
    assert client.rules[0]["is_effect"] == 0


def test_author_shadow_runner_cleanup_verifies_active_formal_rule(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient([rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True)])
    client.noop_deactivate_tags.add(f"{SOCIAL_RULE_TAG_PREFIX}001")
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    summary = run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=lambda *args, **kwargs: None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert summary["cleanup"]["cleanup_verified"] is False
    assert summary["cleanup"]["safe_to_stop_monitoring"] is False
    assert summary["cleanup"]["active_formal_rules_after_cleanup"][0]["tag"] == f"{SOCIAL_RULE_TAG_PREFIX}001"


def test_author_shadow_runner_does_not_modify_token_shadow_or_probe_rules(ctx, tmp_path) -> None:
    client = FakeAuthorShadowClient(
        [
            rule("formal", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:NancyCrypto", active=True),
            rule("token-shadow", "wallet-agent-token-shadow-v01-ca-001", TOKEN_WALLET, active=False),
            rule("identity-probe", "wallet-agent-token-identity-probe-ca-v01", TOKEN_WALLET, active=False),
        ]
    )
    write_social_shard_state(tmp_path / "shards.json", ["NancyCrypto"])

    run_author_cashtag_shadow(
        client,
        api_key="secret-key",
        session_factory=ctx[0],
        duration_seconds=1,
        interval_seconds=300,
        output_root=None,
        websocket_runner=lambda *args, **kwargs: None,
        shard_state_path=tmp_path / "shards.json",
        pause_state_path=tmp_path / "paused.json",
        kol_config_path=ctx[1],
    )

    assert all(call[1].startswith("wallet-agent-social-v1") for call in client.update_calls)
    assert client.rules[1]["is_effect"] == 0
    assert client.rules[2]["is_effect"] == 0


def fail_if_called(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
    raise AssertionError("websocket runner should not be called")


def write_social_shard_state(path, usernames: list[str]) -> None:  # noqa: ANN001
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "shards": {
                    f"{SOCIAL_RULE_TAG_PREFIX}001": usernames,
                },
            }
        )
    )


def stream_message(payload: dict[str, object]) -> str:
    return json.dumps({"event_type": "tweet", "tweet": payload})


def tweet_payload(tweet_id: str, text: str) -> dict[str, object]:
    return {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-09T01:00:00Z",
        "author": {"id": "kol-id", "userName": "NancyCrypto", "followers": 5000},
    }


def rule(rule_id: str, tag: str, value: str, *, active: bool) -> dict[str, object]:
    return {
        "rule_id": rule_id,
        "tag": tag,
        "value": value,
        "interval_seconds": 300,
        "is_effect": 1 if active else 0,
    }


def watch(wallet_id: int, token_address: str, symbol: str, now: datetime) -> TokenWatchState:
    return TokenWatchState(
        wallet_id=wallet_id,
        chain="robinhood",
        token_address=token_address,
        symbol=symbol,
        active=True,
        started_at=now,
        last_seen_at=now,
    )


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/author_cashtag_shadow_run.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 9, 1, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET_ADDRESS, "robinhood")
        session.add_all(
            [
                watch(wallet.id, TOKEN_WALLET, "WALLET", now),
                watch(wallet.id, TOKEN_ROBBIE, "ROBBIE", now),
            ]
        )
    SocialKOLService(session_factory).observe_candidate(
        author_id="kol-id",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(json.dumps({"kols": []}))
    return session_factory, config_path
