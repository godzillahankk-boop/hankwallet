from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.twitterapi_io_client import normalize_tweet
from scripts.twitter_filter_cost_probe import (
    PROBE_RULE_TAG,
    FilterCostProbeStats,
    deactivate_probe_rule,
    ensure_probe_rule,
    record_probe_stream_message,
    run_filter_cost_probe,
)


class FakeProbeRuleClient:
    def __init__(self) -> None:
        self.rules = [
            {"rule_id": "formal", "tag": "wallet-agent-social-v1-001", "value": "from:formal", "interval_seconds": 300, "is_effect": 1},
            {"rule_id": "probe", "tag": PROBE_RULE_TAG, "value": "from:old", "interval_seconds": 60, "is_effect": 0},
        ]
        self.get_calls = 0
        self.add_calls = []
        self.update_calls = []

    def get_filter_rules(self):
        self.get_calls += 1
        return {"rules": self.rules}

    def add_filter_rule(self, *, tag: str, value: str, interval_seconds: int):
        self.add_calls.append((tag, value, interval_seconds))
        rule_id = "created-probe"
        self.rules.append({"rule_id": rule_id, "tag": tag, "value": value, "interval_seconds": interval_seconds, "is_effect": 0})
        return {"rule_id": rule_id}

    def update_filter_rule(self, *, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
        self.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
        for rule in self.rules:
            if rule["rule_id"] == rule_id:
                rule.update({"tag": tag, "value": value, "interval_seconds": interval_seconds, "is_effect": 1 if is_effect else 0})
        return {"status": "success"}


def test_probe_uses_isolated_tag_and_does_not_modify_formal_rules() -> None:
    client = FakeProbeRuleClient()

    rule = ensure_probe_rule(client, tag=PROBE_RULE_TAG, value="from:lookonchain", interval_seconds=60)
    deactivate_probe_rule(client, rule=rule, value="from:lookonchain", interval_seconds=60)

    assert rule == {"rule_id": "probe", "tag": PROBE_RULE_TAG}
    assert client.add_calls == []
    assert client.update_calls == [
        ("probe", PROBE_RULE_TAG, "from:lookonchain", 60, True),
        ("probe", PROBE_RULE_TAG, "from:lookonchain", 60, False),
    ]
    assert client.rules[0] == {
        "rule_id": "formal",
        "tag": "wallet-agent-social-v1-001",
        "value": "from:formal",
        "interval_seconds": 300,
        "is_effect": 1,
    }


def test_probe_adds_missing_rule_and_cleanup_deactivates_it() -> None:
    client = FakeProbeRuleClient()
    client.rules = [client.rules[0]]

    rule = ensure_probe_rule(client, tag=PROBE_RULE_TAG, value="from:lookonchain", interval_seconds=60)
    deactivate_probe_rule(client, rule=rule, value="from:lookonchain", interval_seconds=60)

    assert client.add_calls == [(PROBE_RULE_TAG, "from:lookonchain", 60)]
    assert client.update_calls[-1] == ("created-probe", PROBE_RULE_TAG, "from:lookonchain", 60, False)


def test_filter_cost_probe_duplicate_and_periodic_stats() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)
    tweet = normalize_tweet({"id": "abc", "text": "hello", "author": {"id": "author"}})

    for offset in (0, 60, 120, 180):
        stats.record_payload(
            rule_id="rule",
            rule_tag=PROBE_RULE_TAG,
            tweets=[tweet],
            received_at=started + timedelta(seconds=offset),
        )

    summary = stats.summary(duration_seconds=180, interval_seconds=60)
    assert summary["provider_messages"] == 4
    assert summary["tweet_deliveries"] == 4
    assert summary["unique_tweet_ids"] == 1
    assert summary["duplicate_deliveries"] == 3
    assert summary["duplicate_ratio"] == 0.75
    assert summary["repeat_interval_seconds"] == 60
    assert summary["suspected_periodic_replay"] is True
    assert summary["conclusion"] == "FILTER_PERIODIC_REPLAY"
    assert summary["tweets"][0]["delivery_count"] == 4
    assert "text" not in str(summary)


def test_filter_cost_probe_distinct_tweets_are_not_duplicates() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)

    stats.record_payload(
        rule_id="rule",
        rule_tag=PROBE_RULE_TAG,
        tweets=[normalize_tweet({"id": "a"}), normalize_tweet({"id": "b"})],
        received_at=started,
    )

    summary = stats.summary(duration_seconds=1, interval_seconds=60)
    assert summary["unique_tweet_ids"] == 2
    assert summary["duplicate_deliveries"] == 0
    assert summary["suspected_periodic_replay"] is False


def test_filter_cost_probe_tweet_memory_is_bounded() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started, max_tracked_tweets=2)

    for tweet_id in ("a", "b", "c"):
        stats.record_tweet(normalize_tweet({"id": tweet_id}), received_at=started)

    summary = stats.summary(duration_seconds=1, interval_seconds=60)
    assert summary["unique_tweet_ids"] == 2
    assert [row["tweet_id"] for row in summary["tweets"]] == ["b", "c"]


def test_filter_cost_probe_zero_tweets_is_low_traffic() -> None:
    stats = FilterCostProbeStats(started_at=datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC))

    summary = stats.summary(duration_seconds=60, interval_seconds=60)

    assert summary["tweet_deliveries"] == 0
    assert summary["conclusion"] == "INCONCLUSIVE_LOW_TRAFFIC"


def test_filter_cost_probe_one_tweet_is_low_traffic() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)
    stats.record_tweet(normalize_tweet({"id": "one"}), received_at=started)

    summary = stats.summary(duration_seconds=60, interval_seconds=60)

    assert summary["tweet_deliveries"] == 1
    assert summary["conclusion"] == "INCONCLUSIVE_LOW_TRAFFIC"


def test_filter_cost_probe_enough_samples_without_periodic_replay_is_not_over_inferred() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)
    for index in range(4):
        stats.record_tweet(normalize_tweet({"id": f"tweet-{index}"}), received_at=started + timedelta(seconds=index))

    summary = stats.summary(duration_seconds=4, interval_seconds=60)

    assert summary["tweet_deliveries"] == 4
    assert summary["duplicate_deliveries"] == 0
    assert summary["suspected_periodic_replay"] is False
    assert summary["conclusion"] == "NO_PERIODIC_REPLAY_OBSERVED"


def test_filter_cost_probe_aborts_when_formal_rules_are_active() -> None:
    client = FakeProbeRuleClient()

    summary = run_filter_cost_probe(
        client,
        api_key="secret",
        rule_value="from:lookonchain",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is True
    assert summary["abort_reason"] == "ACTIVE_FORMAL_RULES_DETECTED"
    assert summary["formal_rules_active_at_start"] is True
    assert summary["active_formal_rules"] == [
        {
            "tag": "wallet-agent-social-v1-001",
            "rule_id": "formal",
            "active": True,
            "interval_seconds": 300,
            "value_length": 11,
        }
    ]
    assert client.add_calls == []
    assert client.update_calls == []


def test_filter_cost_probe_allows_inactive_formal_rules_and_cleans_probe() -> None:
    client = FakeProbeRuleClient()
    client.rules[0]["is_effect"] = 0

    summary = run_filter_cost_probe(
        client,
        api_key="secret",
        rule_value="from:lookonchain",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is False
    assert summary["formal_rules_active_at_start"] is False
    assert client.update_calls == [
        ("probe", PROBE_RULE_TAG, "from:lookonchain", 60, True),
        ("probe", PROBE_RULE_TAG, "from:lookonchain", 60, False),
    ]


def test_filter_cost_probe_only_records_expected_probe_tag_and_tracks_foreign_rules() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)

    record_probe_stream_message(
        _stream_message("probe-tweet", PROBE_RULE_TAG),
        stats=stats,
        received_at=started,
        expected_rule_tag=PROBE_RULE_TAG,
    )
    record_probe_stream_message(
        _stream_message("foreign-tweet", "wallet-agent-social-v1-001"),
        stats=stats,
        received_at=started + timedelta(seconds=1),
        expected_rule_tag=PROBE_RULE_TAG,
    )

    summary = stats.summary(duration_seconds=1, interval_seconds=60)
    assert summary["provider_messages"] == 1
    assert summary["tweet_deliveries"] == 1
    assert summary["unique_tweet_ids"] == 1
    assert summary["tweets"][0]["tweet_id"] == "probe-tweet"
    assert summary["foreign_rule_messages"] == 1
    assert summary["foreign_rule_tweet_deliveries"] == 1
    assert summary["conclusion"] == "INCONCLUSIVE_NOT_ISOLATED"


def test_filter_cost_probe_foreign_rule_tweets_do_not_affect_duplicate_stats() -> None:
    started = datetime(2026, 9, 8, 1, 0, 0, tzinfo=UTC)
    stats = FilterCostProbeStats(started_at=started)

    for offset in (0, 60):
        record_probe_stream_message(
            _stream_message("foreign-dup", "wallet-agent-social-v1-001"),
            stats=stats,
            received_at=started + timedelta(seconds=offset),
            expected_rule_tag=PROBE_RULE_TAG,
        )

    summary = stats.summary(duration_seconds=60, interval_seconds=60)
    assert summary["tweet_deliveries"] == 0
    assert summary["unique_tweet_ids"] == 0
    assert summary["duplicate_deliveries"] == 0
    assert summary["foreign_rule_messages"] == 2
    assert summary["foreign_rule_tweet_deliveries"] == 2
    assert summary["conclusion"] == "INCONCLUSIVE_NOT_ISOLATED"


def test_filter_cost_probe_finally_deactivates_probe_rule_when_runner_fails() -> None:
    client = FakeProbeRuleClient()
    client.rules[0]["is_effect"] = 0

    def fail_runner(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("websocket down")

    try:
        run_filter_cost_probe(
            client,
            api_key="secret",
            rule_value="from:lookonchain",
            websocket_runner=fail_runner,
        )
    except RuntimeError:
        pass

    assert client.update_calls[-1] == ("probe", PROBE_RULE_TAG, "from:lookonchain", 60, False)


def _stream_message(tweet_id: str, rule_tag: str) -> str:
    return (
        '{"event_type":"tweet",'
        '"rule_id":"rule",'
        f'"rule_tag":"{rule_tag}",'
        f'"tweet":{{"id":"{tweet_id}","author":{{"id":"author"}}}}'
        "}"
    )
