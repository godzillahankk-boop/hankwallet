from __future__ import annotations

import json
import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialEvent, SocialIdentity, TokenWatchState
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_kol_service import SOURCE_TRUSTED_EXTERNAL_SEED, SocialKOLService
from app.services.social_watch_registry import SocialWatchAccount, SocialWatchRegistry, WATCH_FIXED_KOL
from app.services.twitterapi_io_social_ingestion import (
    DEFAULT_RULE_MAX_VALUE_CHARS,
    SOCIAL_RULE_TAG,
    SOCIAL_RULE_TAG_PREFIX,
    STREAM_BACKOFF,
    STREAM_CONNECTED,
    SocialIngestionStats,
    RuleShardStateStore,
    TwitterApiIoRuleManager,
    TwitterApiIoSocialIngestor,
    TwitterApiIoError,
    build_rule_shards,
    build_stable_rule_shards,
    parse_stream_message,
)
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
TOKEN_2 = "0x39dbed3a2bd333467115de45665cc57f813c4571"


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/social_stream.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 2, 11, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        watch = TokenWatchState(
            wallet_id=wallet.id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="PONS",
            active=True,
            started_at=now - timedelta(hours=1),
            last_seen_at=now,
        )
        session.add(watch)
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(json.dumps({"kols": [{"username": "kol_one", "enabled": True}]}))
    return session_factory, config_path


def tweet_payload(
    tweet_id: str,
    *,
    text: str = f"Watching {TOKEN}",
    author: str = "kol_one",
    created_at: str = "2026-09-02T12:00:00Z",
    retweet: bool = False,
) -> dict:
    payload = {
        "id": tweet_id,
        "text": text,
        "createdAt": created_at,
        "author": {"id": author, "userName": author, "name": author, "followers": 1000},
        "likeCount": 1,
        "retweetCount": 2,
        "replyCount": 3,
        "quoteCount": 4,
        "viewCount": 5,
    }
    if retweet:
        payload["retweeted_tweet"] = {"id": f"rt-{tweet_id}", "text": text}
    return payload


class FakeRuleClient:
    def __init__(self, rules=None) -> None:
        self.rules = rules or []
        self.get_calls = 0
        self.add_calls = []
        self.update_calls = []

    def get_filter_rules(self):
        self.get_calls += 1
        return {"rules": self.rules}

    def add_filter_rule(self, *, tag: str, value: str, interval_seconds: float):
        self.add_calls.append((tag, value, interval_seconds))
        rule_id = f"created-rule-{len(self.add_calls)}"
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
                rule["tag"] = tag
                rule["value"] = value
                rule["interval_seconds"] = interval_seconds
                rule["is_effect"] = 1 if is_effect else 0
        return {"status": "success"}


class DummyRuleManager:
    def __init__(self) -> None:
        self.calls = 0

    async def ensure_rule(self, accounts, *, stats=None):  # noqa: ANN001
        self.calls += 1
        return None

    async def ensure_rules(self, accounts, *, stats=None):  # noqa: ANN001
        self.calls += 1
        return []


class FailingRuleManager:
    async def ensure_rule(self, accounts, *, stats=None):  # noqa: ANN001
        raise RuntimeError("rule down")

    async def ensure_rules(self, accounts, *, stats=None):  # noqa: ANN001
        raise RuntimeError("rule down")


class FailingRegistry:
    def accounts(self):
        raise RuntimeError("registry down")


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


class HealthyTestIngestor(TwitterApiIoSocialIngestor):
    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self.connect_entered = asyncio.Event()
        self.release_connect = asyncio.Event()

    async def _connect_once(self) -> None:
        self.connection_state = STREAM_CONNECTED
        self.connected_at = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
        self.connect_entered.set()
        await self.release_connect.wait()


class DisconnectingTestIngestor(TwitterApiIoSocialIngestor):
    async def _connect_once(self) -> None:
        raise RuntimeError("stream down")


def make_ingestor(session_factory, config_path, *, event_service=None) -> TwitterApiIoSocialIngestor:
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    ingestor = TwitterApiIoSocialIngestor(
        api_key="secret-key",
        registry=registry,
        event_service=event_service or SocialEventService(session_factory),
        rule_manager=DummyRuleManager(),
        rule_refresh_seconds=300,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )
    ingestor._current_accounts = [SocialWatchAccount("kol_one", "kol_one", WATCH_FIXED_KOL)]
    ingestor.connected_at = datetime(2026, 9, 2, 11, 59, 0, tzinfo=UTC)
    return ingestor


def watch_accounts(*usernames: str) -> list[SocialWatchAccount]:
    return [
        SocialWatchAccount(username, username.lower(), WATCH_FIXED_KOL)
        for username in usernames
    ]


def write_shard_state(path, shards: dict[str, list[str]]) -> None:  # noqa: ANN001
    path.write_text(json.dumps({"version": 1, "shards": shards}, indent=2, sort_keys=True))


@pytest.mark.asyncio
async def test_start_preloads_registry_before_first_rule_refresh_and_tweet_processing(ctx) -> None:
    session_factory, config_path = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="abc",
        username="abc",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    manager = DummyRuleManager()
    ingestor = HealthyTestIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=manager,
        rule_refresh_seconds=60,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )

    await ingestor.start()

    assert {account.normalized_username for account in ingestor._current_accounts} == {"abc"}
    assert manager.calls == 0
    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("abc-call", author="abc")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )
    ingestor.release_connect.set()
    await ingestor.stop()

    events = all_events(session_factory)
    assert len(events) == 1
    assert events[0].author_type == AUTHOR_KOL


@pytest.mark.asyncio
async def test_start_registry_preload_does_not_call_twitter_provider(ctx) -> None:
    session_factory, config_path = ctx
    client = FakeRuleClient()
    ingestor = HealthyTestIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=TwitterApiIoRuleManager(client),
        rule_refresh_seconds=60,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )

    await ingestor.start()

    assert client.get_calls == 0
    assert client.add_calls == []
    assert client.update_calls == []
    ingestor.release_connect.set()
    await ingestor.stop()


@pytest.mark.asyncio
async def test_start_registry_preload_failure_does_not_block_tasks(caplog) -> None:
    manager = DummyRuleManager()
    ingestor = HealthyTestIngestor(
        api_key="secret-key",
        registry=FailingRegistry(),
        event_service=object(),
        rule_manager=manager,
        rule_refresh_seconds=0.05,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )

    with caplog.at_level("WARNING"):
        await ingestor.start()
        await ingestor.connect_entered.wait()
        await asyncio.sleep(0.06)

    assert ingestor._stream_task.done() is False
    assert ingestor._rule_refresh_task.done() is False
    assert "registry preload failed" in caplog.text
    assert ingestor.stats.provider_errors >= 1
    ingestor.release_connect.set()
    await ingestor.stop()


@pytest.mark.asyncio
async def test_rule_missing_creates_and_activates(ctx) -> None:
    _, config_path = ctx
    registry = SocialWatchRegistry(ctx[0], kol_config_path=config_path)
    client = FakeRuleClient()
    stats = SocialIngestionStats()
    manager = TwitterApiIoRuleManager(client)

    rule = await manager.ensure_rule(registry.accounts(), stats=stats)

    assert rule is not None
    assert rule.rule_id == "created-rule-1"
    assert rule.tag == f"{SOCIAL_RULE_TAG_PREFIX}001"
    assert rule.is_effect is True
    assert len(client.add_calls) == 1
    assert len(client.update_calls) == 1
    assert client.add_calls[0][2] == 300
    assert client.update_calls[0][3] == 300
    assert stats.filter_api_calls == 3
    assert stats.rule_updates == 1


@pytest.mark.asyncio
async def test_rule_existing_reused_and_unchanged_fingerprint_skips_provider_calls(ctx) -> None:
    session_factory, config_path = ctx
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    value = "from:kol_one"
    client = FakeRuleClient(
        [
            {"rule_id": "probe-rule", "tag": "wallet-agent-probe-v01", "value": "from:probe", "is_effect": 1},
            {
                "rule_id": "social-rule",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": value,
                "interval_seconds": 300,
                "is_effect": 1,
            },
        ]
    )
    stats = SocialIngestionStats()
    manager = TwitterApiIoRuleManager(client, min_update_interval_seconds=0)

    first = await manager.ensure_rule(registry.accounts(), stats=stats)
    second = await manager.ensure_rule(registry.accounts(), stats=stats)

    assert first.rule_id == "social-rule"
    assert second.rule_id == "social-rule"
    assert client.get_calls == 1
    assert client.add_calls == []
    assert client.update_calls == []
    assert stats.rule_updates == 0
    assert stats.registry_changes == 1
    assert client.rules[0]["tag"] == "wallet-agent-probe-v01"


@pytest.mark.asyncio
async def test_rule_username_change_updates_same_rule(ctx) -> None:
    session_factory, config_path = ctx
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    client = FakeRuleClient([{"rule_id": "social-rule", "tag": f"{SOCIAL_RULE_TAG_PREFIX}001", "value": "from:old", "is_effect": 1}])
    manager = TwitterApiIoRuleManager(client, min_update_interval_seconds=0)

    rule = await manager.ensure_rule(registry.accounts(), stats=SocialIngestionStats())

    assert rule.rule_id == "social-rule"
    assert client.update_calls[0][0] == "social-rule"
    assert client.update_calls[0][2] == "from:kol_one"


@pytest.mark.asyncio
async def test_new_active_project_identity_updates_existing_social_rule(ctx) -> None:
    session_factory, config_path = ctx
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    client = FakeRuleClient(
        [
            {
                "rule_id": "social-rule",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": "from:kol_one",
                "interval_seconds": 300,
                "is_effect": 1,
            }
        ]
    )
    manager = TwitterApiIoRuleManager(client, min_update_interval_seconds=0)
    stats = SocialIngestionStats()

    await manager.ensure_rule(registry.accounts(), stats=stats)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=TOKEN,
                symbol="PONS",
                identity_type=PROJECT_X,
                value="ProjectUser",
                normalized_value="projectuser",
                source="test",
                source_field="test",
                confidence=HIGH,
                is_active=True,
                first_seen_at=now,
                last_verified_at=now,
                valid_from=now,
            )
        )
    updated = await manager.ensure_rule(registry.accounts(), stats=stats)

    assert updated.rule_id == "social-rule"
    assert client.update_calls[-1][0] == "social-rule"
    assert client.update_calls[-1][2] == "from:kol_one OR from:ProjectUser"


@pytest.mark.asyncio
async def test_empty_registry_deactivates_existing_social_rule_and_reactivates_same_rule() -> None:
    client = FakeRuleClient(
        [
            {"rule_id": "probe-rule", "tag": "wallet-agent-probe-v01", "value": "from:probe", "is_effect": 1},
            {"rule_id": "social-rule", "tag": SOCIAL_RULE_TAG, "value": "from:old", "is_effect": 1},
            {"rule_id": "social-shard", "tag": f"{SOCIAL_RULE_TAG_PREFIX}001", "value": "from:older", "is_effect": 1},
        ]
    )
    manager = TwitterApiIoRuleManager(client)
    stats = SocialIngestionStats()

    empty = await manager.ensure_rules([], stats=stats)
    account = SocialWatchAccount("kol_one", "kol_one", WATCH_FIXED_KOL)
    active = await manager.ensure_rule([account], stats=stats)

    assert {rule.rule_id for rule in empty} == {"social-rule", "social-shard"}
    assert all(rule.is_effect is False for rule in empty)
    assert active.rule_id == "social-shard"
    assert active.is_effect is True
    assert client.update_calls[0] == ("social-rule", SOCIAL_RULE_TAG, "from:old", 300, False)
    assert client.update_calls[1] == ("social-shard", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:older", 300, False)
    assert client.update_calls[2] == ("social-shard", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:kol_one", 300, True)
    assert client.rules[0]["tag"] == "wallet-agent-probe-v01"
    assert client.rules[0]["is_effect"] == 1


@pytest.mark.asyncio
async def test_empty_registry_without_cached_rule_gets_rules_once() -> None:
    client = FakeRuleClient([{"rule_id": "social-rule", "tag": SOCIAL_RULE_TAG, "value": "from:old", "is_effect": 1}])
    manager = TwitterApiIoRuleManager(client)

    rules = await manager.ensure_rules([], stats=SocialIngestionStats())

    assert rules[0].is_effect is False
    assert client.get_calls == 1
    assert len(client.update_calls) == 1


def test_rule_sharding_short_accounts_single_shard_and_deterministic() -> None:
    accounts = watch_accounts("beta", "Alpha", "gamma")

    first = build_rule_shards(accounts, max_value_chars=240)
    second = build_rule_shards(list(reversed(accounts)), max_value_chars=240)

    assert first == second
    assert first == [(f"{SOCIAL_RULE_TAG_PREFIX}001", "from:Alpha OR from:beta OR from:gamma")]


def test_rule_sharding_many_accounts_multi_shard_under_limit_and_once_each() -> None:
    accounts = watch_accounts(*[f"longkolname{i:02d}" for i in range(1, 19)])

    shards = build_rule_shards(accounts, max_value_chars=120)
    values = [value for _, value in shards]
    joined = " OR ".join(values)

    assert len(shards) > 1
    assert all(len(value) < 255 for value in values)
    for index in range(1, 19):
        assert joined.count(f"from:longkolname{index:02d}") == 1
    assert [tag for tag, _ in shards] == [
        f"{SOCIAL_RULE_TAG_PREFIX}{index:03d}" for index in range(1, len(shards) + 1)
    ]


def test_stable_rule_sharding_adds_username_to_existing_capacity_without_moving_prior_assignments(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(
        state_path,
        {
            f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice", "Bob"],
            f"{SOCIAL_RULE_TAG_PREFIX}002": ["C", "D"],
        },
    )

    shards, changed = build_stable_rule_shards(
        watch_accounts("Alice", "Bob", "C", "D", "E"),
        store=RuleShardStateStore(state_path),
        max_value_chars=30,
    )

    assert changed is True
    assert shards == [
        (f"{SOCIAL_RULE_TAG_PREFIX}001", "from:Alice OR from:Bob"),
        (f"{SOCIAL_RULE_TAG_PREFIX}002", "from:C OR from:D OR from:E"),
    ]


def test_stable_rule_sharding_middle_letter_add_does_not_cascade_reorder(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(
        state_path,
        {
            f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice", "Bob"],
            f"{SOCIAL_RULE_TAG_PREFIX}002": ["Mike", "Zoe"],
        },
    )

    shards, _ = build_stable_rule_shards(
        watch_accounts("Alice", "Bob", "Carol", "Mike", "Zoe"),
        store=RuleShardStateStore(state_path),
        max_value_chars=40,
    )

    assert shards == [
        (f"{SOCIAL_RULE_TAG_PREFIX}001", "from:Alice OR from:Bob OR from:Carol"),
        (f"{SOCIAL_RULE_TAG_PREFIX}002", "from:Mike OR from:Zoe"),
    ]


@pytest.mark.asyncio
async def test_stable_rule_manager_add_updates_only_affected_shard(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(
        state_path,
        {
            f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice", "Bob"],
            f"{SOCIAL_RULE_TAG_PREFIX}002": ["C", "D"],
        },
    )
    client = FakeRuleClient(
        [
            {
                "rule_id": "rule-1",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": "from:Alice OR from:Bob",
                "interval_seconds": 300,
                "is_effect": 1,
            },
            {
                "rule_id": "rule-2",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}002",
                "value": "from:C OR from:D",
                "interval_seconds": 300,
                "is_effect": 1,
            },
        ]
    )
    manager = TwitterApiIoRuleManager(client, max_value_chars=30, min_update_interval_seconds=0, shard_state_path=state_path)

    await manager.ensure_rules(watch_accounts("Alice", "Bob", "C", "D", "E"))

    assert client.update_calls == [("rule-2", f"{SOCIAL_RULE_TAG_PREFIX}002", "from:C OR from:D OR from:E", 300, True)]


@pytest.mark.asyncio
async def test_stable_rule_manager_delete_updates_only_original_shard(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(
        state_path,
        {
            f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice", "Bob"],
            f"{SOCIAL_RULE_TAG_PREFIX}002": ["Mike", "Zoe"],
        },
    )
    client = FakeRuleClient(
        [
            {
                "rule_id": "rule-1",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": "from:Alice OR from:Bob",
                "interval_seconds": 300,
                "is_effect": 1,
            },
            {
                "rule_id": "rule-2",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}002",
                "value": "from:Mike OR from:Zoe",
                "interval_seconds": 300,
                "is_effect": 1,
            },
        ]
    )
    manager = TwitterApiIoRuleManager(client, max_value_chars=80, min_update_interval_seconds=0, shard_state_path=state_path)

    await manager.ensure_rules(watch_accounts("Alice", "Mike", "Zoe"))

    assert client.update_calls == [("rule-1", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:Alice", 300, True)]


@pytest.mark.asyncio
async def test_stable_rule_manager_empty_shard_deactivates_without_compacting_following_shards(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(
        state_path,
        {
            f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice"],
            f"{SOCIAL_RULE_TAG_PREFIX}002": ["Bob"],
        },
    )
    client = FakeRuleClient(
        [
            {
                "rule_id": "rule-1",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": "from:Alice",
                "interval_seconds": 300,
                "is_effect": 1,
            },
            {
                "rule_id": "rule-2",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}002",
                "value": "from:Bob",
                "interval_seconds": 300,
                "is_effect": 1,
            },
        ]
    )
    manager = TwitterApiIoRuleManager(client, max_value_chars=80, min_update_interval_seconds=0, shard_state_path=state_path)

    await manager.ensure_rules(watch_accounts("Bob"))

    assert client.update_calls == [("rule-1", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:Alice", 300, False)]
    state = json.loads(state_path.read_text())["shards"]
    assert state[f"{SOCIAL_RULE_TAG_PREFIX}001"] == []
    assert state[f"{SOCIAL_RULE_TAG_PREFIX}002"] == ["Bob"]


@pytest.mark.asyncio
async def test_stable_rule_manager_identical_provider_rules_have_zero_updates(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    write_shard_state(state_path, {f"{SOCIAL_RULE_TAG_PREFIX}001": ["Alice", "Bob"]})
    client = FakeRuleClient(
        [
            {
                "rule_id": "rule-1",
                "tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "value": "from:Alice OR from:Bob",
                "interval_seconds": 300,
                "is_effect": 1,
            }
        ]
    )
    manager = TwitterApiIoRuleManager(client, min_update_interval_seconds=0, shard_state_path=state_path)

    await manager.ensure_rules(watch_accounts("Alice", "Bob"))

    assert client.update_calls == []


@pytest.mark.asyncio
async def test_stable_rule_manager_restart_reads_state_without_rewrite(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    client = FakeRuleClient()
    first = TwitterApiIoRuleManager(client, max_value_chars=30, min_update_interval_seconds=0, shard_state_path=state_path)
    await first.ensure_rules(watch_accounts("Alice", "Bob", "C", "D"))
    provider_rules = [dict(rule) for rule in client.rules]
    client = FakeRuleClient(provider_rules)
    second = TwitterApiIoRuleManager(client, max_value_chars=30, min_update_interval_seconds=0, shard_state_path=state_path)

    await second.ensure_rules(watch_accounts("Alice", "Bob", "C", "D"))

    assert client.update_calls == []


@pytest.mark.asyncio
async def test_stable_rule_manager_malformed_state_fails_before_provider_calls(tmp_path) -> None:
    state_path = tmp_path / "shards.json"
    state_path.write_text("{bad json")
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, min_update_interval_seconds=0, shard_state_path=state_path)

    with pytest.raises(TwitterApiIoError, match="shard state is malformed"):
        await manager.ensure_rules(watch_accounts("Alice"))

    assert client.get_calls == 0
    assert client.add_calls == []
    assert client.update_calls == []


@pytest.mark.asyncio
async def test_multi_rule_manager_creates_multiple_shards_and_deactivates_legacy() -> None:
    client = FakeRuleClient(
        [
            {"rule_id": "legacy", "tag": SOCIAL_RULE_TAG, "value": "from:legacy", "is_effect": 1},
            {"rule_id": "probe", "tag": "wallet-agent-probe-v01", "value": "from:probe", "is_effect": 1},
        ]
    )
    manager = TwitterApiIoRuleManager(client, max_value_chars=80, min_update_interval_seconds=0)

    rules = await manager.ensure_rules(watch_accounts(*[f"account{i:02d}" for i in range(1, 10)]))

    assert len(rules) > 1
    assert all(rule.tag.startswith(SOCIAL_RULE_TAG_PREFIX) for rule in rules)
    assert all(len(rule.value) <= 80 for rule in rules)
    assert any(call == ("legacy", SOCIAL_RULE_TAG, "from:legacy", 300, False) for call in client.update_calls)
    assert client.rules[1]["tag"] == "wallet-agent-probe-v01"
    assert client.rules[1]["is_effect"] == 1


@pytest.mark.asyncio
async def test_multi_rule_manager_dynamic_add_only_updates_needed_shards() -> None:
    accounts = watch_accounts("aa", "bb", "cc", "dd")
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, max_value_chars=31, min_update_interval_seconds=0)

    initial = await manager.ensure_rules(accounts)
    initial_update_count = len(client.update_calls)
    await manager.ensure_rules([*accounts, *watch_accounts("zz")])

    updated_rule_ids = [call[0] for call in client.update_calls[initial_update_count:]]
    assert len(initial) == 2
    assert updated_rule_ids == [initial[-1].rule_id]


@pytest.mark.asyncio
async def test_multi_rule_manager_batches_multiple_additions_in_one_refresh() -> None:
    accounts = watch_accounts("aa", "bb", "cc", "dd")
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, max_value_chars=31, min_update_interval_seconds=0)
    stats = SocialIngestionStats()

    await manager.ensure_rules(accounts, stats=stats)
    initial_updates = len(client.update_calls)
    await manager.ensure_rules([*accounts, *watch_accounts("ee", "ff", "gg")], stats=stats)

    assert stats.registry_changes == 2
    assert len(client.update_calls) - initial_updates <= 2


@pytest.mark.asyncio
async def test_multi_rule_manager_removal_deactivates_extra_shard() -> None:
    accounts = watch_accounts("aa", "bb", "cc", "dd")
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, max_value_chars=31, min_update_interval_seconds=0)

    initial = await manager.ensure_rules(accounts)
    await manager.ensure_rules(watch_accounts("aa", "bb"))

    assert len(initial) == 2
    assert any(call[0] == initial[-1].rule_id and call[-1] is False for call in client.update_calls)


@pytest.mark.asyncio
async def test_multi_rule_manager_same_fingerprint_skips_provider_calls() -> None:
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, max_value_chars=80, min_update_interval_seconds=0)
    accounts = watch_accounts("aa", "bb", "cc")

    await manager.ensure_rules(accounts)
    get_calls = client.get_calls
    update_calls = len(client.update_calls)
    await manager.ensure_rules(list(reversed(accounts)))

    assert client.get_calls == get_calls
    assert len(client.update_calls) == update_calls


@pytest.mark.asyncio
async def test_rule_manager_uses_configured_interval_and_clamps_minimum() -> None:
    low_client = FakeRuleClient()
    low_manager = TwitterApiIoRuleManager(low_client, interval_seconds=10)

    await low_manager.ensure_rules(watch_accounts("aa"))

    assert low_client.add_calls[0][2] == 60
    assert low_client.update_calls[0][3] == 60

    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, interval_seconds=420)

    await manager.ensure_rules(watch_accounts("aa"))

    assert client.add_calls[0][2] == 420
    assert client.update_calls[0][3] == 420


@pytest.mark.asyncio
async def test_rule_manager_same_accounts_and_same_interval_does_not_update() -> None:
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(client, interval_seconds=300)
    accounts = watch_accounts("aa")

    await manager.ensure_rules(accounts)
    updates = len(client.update_calls)
    await manager.ensure_rules(accounts)

    assert len(client.update_calls) == updates


@pytest.mark.asyncio
async def test_rule_manager_interval_change_updates_existing_rule_once() -> None:
    rules = [{"rule_id": "social-rule", "tag": f"{SOCIAL_RULE_TAG_PREFIX}001", "value": "from:aa", "interval_seconds": 60, "is_effect": 1}]
    client = FakeRuleClient(rules)
    manager = TwitterApiIoRuleManager(client, interval_seconds=300)

    await manager.ensure_rules(watch_accounts("aa"))

    assert client.add_calls == []
    assert client.update_calls == [("social-rule", f"{SOCIAL_RULE_TAG_PREFIX}001", "from:aa", 300, True)]


@pytest.mark.asyncio
async def test_rule_manager_min_update_interval_defers_and_batches_registry_change() -> None:
    clock = FakeClock(datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC))
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(
        client,
        interval_seconds=300,
        min_update_interval_seconds=1800,
        clock=clock,
    )

    await manager.ensure_rules(watch_accounts("aa"))
    initial_updates = len(client.update_calls)
    deferred = await manager.ensure_rules(watch_accounts("aa", "bb"))

    assert len(client.update_calls) == initial_updates
    assert [rule.value for rule in deferred] == ["from:aa"]

    clock.advance(1800)
    applied = await manager.ensure_rules(watch_accounts("aa", "bb", "cc"))

    assert len(client.update_calls) == initial_updates + 1
    assert [rule.value for rule in applied] == ["from:aa OR from:bb OR from:cc"]


@pytest.mark.asyncio
async def test_rule_manager_first_start_without_formal_rule_creates_immediately() -> None:
    clock = FakeClock(datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC))
    client = FakeRuleClient()
    manager = TwitterApiIoRuleManager(
        client,
        interval_seconds=300,
        min_update_interval_seconds=1800,
        clock=clock,
    )

    await manager.ensure_rules(watch_accounts("aa"))

    assert len(client.add_calls) == 1
    assert len(client.update_calls) == 1


def test_rule_stats_snapshot_reports_update_efficiency(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    ingestor.stats.rule_updates = 3
    ingestor.stats.registry_changes = 2

    snapshot = ingestor.stats_snapshot()

    assert snapshot["rule_updates_per_registry_change"] == 1.5


def test_parse_stream_batch_rule_metadata() -> None:
    parsed = parse_stream_message(
        json.dumps(
            {
                "event_type": "tweet",
                "rule_id": "rule-1",
                "rule_tag": SOCIAL_RULE_TAG,
                "tweets": [tweet_payload("1"), tweet_payload("2", text="$PONS")],
            }
        ),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert parsed.event_type == "tweet"
    assert parsed.rule_id == "rule-1"
    assert parsed.rule_tag == SOCIAL_RULE_TAG
    assert [tweet.tweet_id for tweet in parsed.tweets] == ["1", "2"]


@pytest.mark.asyncio
async def test_stream_connected_ping_unknown_and_malformed_are_safe(ctx, caplog) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)

    await ingestor.handle_message(json.dumps({"event_type": "connected"}), received_at=now)
    await ingestor.handle_message(json.dumps({"event_type": "ping"}), received_at=now)
    await ingestor.handle_message(json.dumps({"event_type": "heartbeat"}), received_at=now)
    await ingestor.handle_message("{bad json", received_at=now)

    assert ingestor.connection_state == STREAM_CONNECTED
    assert ingestor.connected_at == now
    assert ingestor.stats.messages_received == 4
    assert ingestor.stats.provider_errors == 1
    assert "secret-key" not in caplog.text


@pytest.mark.asyncio
async def test_stream_stats_snapshot_connection_and_gap_metrics(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    connected = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    disconnected = connected + timedelta(seconds=30)
    reconnected = connected + timedelta(seconds=120)

    await ingestor.handle_message(json.dumps({"event_type": "connected"}), received_at=connected)
    ingestor._record_disconnected(disconnected)
    ingestor.stats.disconnects = 1
    await ingestor.handle_message(json.dumps({"event_type": "connected"}), received_at=reconnected)
    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("gap")}),
        received_at=reconnected + timedelta(seconds=1),
    )
    snapshot = ingestor.stats_snapshot()

    assert snapshot["connection_state"] == STREAM_CONNECTED
    assert snapshot["last_connected_at"] == reconnected.isoformat()
    assert snapshot["last_disconnected_at"] == disconnected.isoformat()
    assert snapshot["total_disconnect_seconds"] == 90
    assert snapshot["messages_received"] == 3
    assert snapshot["runtime_dedupe_size"] == 1
    assert snapshot["stream_duplicate_ratio"] == 0.0
    assert snapshot["stream_unique_ratio"] == 1.0
    assert "secret-key" not in str(snapshot)


@pytest.mark.asyncio
async def test_realtime_tweet_batch_creates_social_event_and_dedupes(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    message = json.dumps(
        {
            "event_type": "tweet",
            "rule_id": "rule-1",
            "rule_tag": SOCIAL_RULE_TAG,
            "tweets": [tweet_payload("1"), tweet_payload("1")],
        }
    )

    await ingestor.handle_message(message, received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC))

    with session_scope(session_factory) as session:
        events = list(session.scalars(select(SocialEvent)))
    assert len(events) == 1
    assert events[0].author_type == AUTHOR_KOL
    assert events[0].rule_id == "rule-1"
    assert events[0].rule_tag == SOCIAL_RULE_TAG
    assert ingestor.stats.tweets_received == 2
    assert ingestor.stats.unique_tweets_seen == 1
    assert ingestor.stats.duplicates == 1
    assert ingestor.stats.social_events_created == 1


@pytest.mark.asyncio
async def test_duplicate_diagnostic_records_rule_batch_and_connection(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    ingestor._record_connected(datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC))
    ingestor.last_rule_refresh_at = datetime(2026, 9, 2, 12, 0, 1, tzinfo=UTC)
    ingestor.last_rule_update_at = datetime(2026, 9, 2, 12, 0, 2, tzinfo=UTC)

    await ingestor.handle_message(
        json.dumps(
            {
                "event_type": "tweet",
                "rule_id": "rule-1",
                "rule_tag": f"{SOCIAL_RULE_TAG_PREFIX}001",
                "tweet": tweet_payload("dup"),
            }
        ),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )
    await ingestor.handle_message(
        json.dumps(
            {
                "event_type": "tweet",
                "rule_id": "rule-2",
                "rule_tag": f"{SOCIAL_RULE_TAG_PREFIX}002",
                "tweet": tweet_payload("dup"),
            }
        ),
        received_at=datetime(2026, 9, 2, 12, 0, 8, tzinfo=UTC),
    )

    sample = ingestor.stats_snapshot()["duplicate_samples"][0]
    assert ingestor.stats.duplicates == 1
    assert sample["tweet_id"] == "dup"
    assert sample["rule_tag"] == f"{SOCIAL_RULE_TAG_PREFIX}002"
    assert sample["first_rule_tag"] == f"{SOCIAL_RULE_TAG_PREFIX}001"
    assert sample["different_rule"] is True
    assert sample["same_connection"] is True
    assert sample["same_payload_batch"] is False
    assert sample["seconds_since_rule_update"] == 6


@pytest.mark.asyncio
async def test_duplicate_diagnostic_is_bounded(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    ingestor._duplicate_samples = ingestor._duplicate_samples.__class__(maxlen=2)

    for index in range(3):
        message = json.dumps({"event_type": "tweet", "tweet": tweet_payload(f"dup-{index}")})
        await ingestor.handle_message(message, received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC))
        await ingestor.handle_message(message, received_at=datetime(2026, 9, 2, 12, 0, 6, tzinfo=UTC))

    samples = ingestor.stats_snapshot()["duplicate_samples"]
    assert len(samples) == 2
    assert [sample["tweet_id"] for sample in samples] == ["dup-1", "dup-2"]


@pytest.mark.asyncio
async def test_warmup_and_retweet_tweets_are_ignored(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    ingestor.connected_at = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("warmup", created_at="2026-09-02T11:57:00Z")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )
    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("retweet", retweet=True)}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert ingestor.stats.warmup_ignored == 1
    assert ingestor.stats.retweets_ignored == 1
    assert ingestor.stats.social_events_created == 0
    assert not all_events(session_factory)


@pytest.mark.asyncio
async def test_kol_unrelated_tweet_is_not_saved(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("btc", text="BTC market looks strong")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert ingestor.stats.unmatched_tweets == 1
    assert not all_events(session_factory)


@pytest.mark.asyncio
async def test_project_and_dev_identity_account_tweets_create_events(ctx) -> None:
    session_factory, config_path = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        for identity_type, value in ((PROJECT_X, "ProjectUser"), (DEV_X, "DevUser")):
            session.add(
                SocialIdentity(
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="PONS",
                    identity_type=identity_type,
                    value=value,
                    normalized_value=value.lower(),
                    source="test",
                    source_field="test",
                    confidence=HIGH,
                    is_active=True,
                    first_seen_at=now,
                    last_verified_at=now,
                    valid_from=now,
                )
            )
    ingestor = make_ingestor(session_factory, config_path)

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("project", text="Update", author="ProjectUser")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )
    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("dev", text="Update", author="DevUser")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    events = all_events(session_factory)
    assert [event.author_type for event in events] == [AUTHOR_PROJECT_X, AUTHOR_DEV_X]
    assert [event.match_type for event in events] == ["identity_account", "identity_account"]
    assert ingestor.stats.project_events == 1
    assert ingestor.stats.dev_x_events == 1


@pytest.mark.asyncio
async def test_event_service_failure_is_isolated(ctx) -> None:
    session_factory, config_path = ctx

    class FailingEventService:
        def create_event_if_relevant(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("db down")

    ingestor = make_ingestor(session_factory, config_path, event_service=FailingEventService())

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("1")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert ingestor.stats.provider_errors == 1


@pytest.mark.asyncio
async def test_healthy_stream_keeps_rule_refresh_loop_running(ctx) -> None:
    session_factory, config_path = ctx
    manager = DummyRuleManager()
    ingestor = HealthyTestIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=manager,
        rule_refresh_seconds=0.05,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )

    await ingestor.start()
    await ingestor.connect_entered.wait()
    await asyncio.sleep(0.13)
    ingestor.release_connect.set()
    await ingestor.stop()

    assert ingestor.stats.rule_refresh_checks >= 3
    assert manager.calls >= 3
    assert ingestor._stream_task.done()
    assert ingestor._rule_refresh_task.done()


@pytest.mark.asyncio
async def test_rule_refresh_failure_does_not_stop_stream(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = HealthyTestIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=FailingRuleManager(),
        rule_refresh_seconds=0.05,
        warmup_grace_seconds=120,
        backoff_seconds=(0,),
    )

    await ingestor.start()
    await ingestor.connect_entered.wait()
    await asyncio.sleep(0.06)

    assert ingestor._stream_task.done() is False
    assert ingestor.stats.provider_errors >= 1
    ingestor.release_connect.set()
    await ingestor.stop()


@pytest.mark.asyncio
async def test_stream_backoff_does_not_stop_rule_refresh(ctx) -> None:
    session_factory, config_path = ctx
    manager = DummyRuleManager()
    ingestor = DisconnectingTestIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=manager,
        rule_refresh_seconds=0.05,
        warmup_grace_seconds=120,
        backoff_seconds=(1,),
    )

    await ingestor.start()
    for _ in range(20):
        if ingestor.stats.rule_refresh_checks >= 2:
            break
        await asyncio.sleep(0.02)
    await ingestor.stop()

    assert ingestor.stats.rule_refresh_checks >= 2
    assert manager.calls >= 2


def test_runtime_dedupe_is_bounded_and_evicts_oldest(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = TwitterApiIoSocialIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=SocialEventService(session_factory),
        rule_manager=DummyRuleManager(),
        max_runtime_dedupe_ids=3,
    )

    assert ingestor._remember_tweet_id("1") is True
    assert ingestor._remember_tweet_id("2") is True
    assert ingestor._remember_tweet_id("3") is True
    assert ingestor._remember_tweet_id("4") is True
    assert list(ingestor._seen_tweet_ids) == ["2", "3", "4"]
    assert ingestor._remember_tweet_id("1") is True
    assert ingestor._remember_tweet_id("4") is False
    assert len(ingestor._seen_tweet_ids) == 3


@pytest.mark.asyncio
async def test_overlapping_fixed_kol_project_identity_preserves_both_author_paths(tmp_path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path}/overlap.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(json.dumps({"kols": [{"username": "lookonchain", "enabled": True}]}))
    now = datetime(2026, 9, 2, 11, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        session.add_all(
            [
                TokenWatchState(
                    wallet_id=wallet.id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="PONS",
                    active=True,
                    started_at=now - timedelta(hours=1),
                    last_seen_at=now,
                ),
                TokenWatchState(
                    wallet_id=wallet.id,
                    chain="robinhood",
                    token_address=TOKEN_2,
                    symbol="OTHER",
                    active=True,
                    started_at=now - timedelta(hours=1),
                    last_seen_at=now,
                ),
                SocialIdentity(
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="PONS",
                    identity_type=PROJECT_X,
                    value="lookonchain",
                    normalized_value="lookonchain",
                    source="test",
                    source_field="test",
                    confidence=HIGH,
                    is_active=True,
                    first_seen_at=now,
                    last_verified_at=now,
                    valid_from=now,
                ),
            ]
        )
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    ingestor = make_ingestor(session_factory, config_path)
    ingestor._current_accounts = registry.accounts()

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("own-project", text="Project update", author="lookonchain")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )
    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("other-token", text=TOKEN_2, author="lookonchain")}),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    events = all_events(session_factory)
    assert (TOKEN, "own-project", AUTHOR_PROJECT_X, "identity_account") in [
        (event.token_address, event.provider_event_id, event.author_type, event.match_type)
        for event in events
    ]
    assert (TOKEN_2, "other-token", AUTHOR_KOL, "direct_ca") in [
        (event.token_address, event.provider_event_id, event.author_type, event.match_type)
        for event in events
    ]


@pytest.mark.asyncio
async def test_disconnect_enters_backoff_and_shutdown_does_not_reconnect(ctx) -> None:
    session_factory, config_path = ctx
    ingestor = make_ingestor(session_factory, config_path)
    ingestor.connection_state = STREAM_BACKOFF
    ingestor.stats.disconnects = 1

    await ingestor.stop()

    assert ingestor.connection_state != STREAM_BACKOFF


def all_events(session_factory) -> list[SocialEvent]:
    with session_scope(session_factory) as session:
        rows = list(session.scalars(select(SocialEvent).order_by(SocialEvent.id.asc())))
        for row in rows:
            session.expunge(row)
        return rows
