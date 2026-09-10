from __future__ import annotations

import json

import pytest

from scripts.twitter_social_rule_control import RuleControlError, pause_formal_rules, resume_formal_rules, status_formal_rules


class FakeRuleControlClient:
    def __init__(self) -> None:
        self.rules = [
            {
                "rule_id": "legacy",
                "tag": "wallet-agent-social-v1",
                "value": "from:legacy",
                "interval_seconds": 300,
                "is_effect": 1,
            },
            {
                "rule_id": "shard",
                "tag": "wallet-agent-social-v1-001",
                "value": "from:kol",
                "interval_seconds": 420,
                "is_effect": 1,
            },
            {
                "rule_id": "inactive",
                "tag": "wallet-agent-social-v1-002",
                "value": "from:old",
                "interval_seconds": 300,
                "is_effect": 0,
            },
            {
                "rule_id": "probe",
                "tag": "wallet-agent-cost-probe-v01",
                "value": "from:probe",
                "interval_seconds": 60,
                "is_effect": 1,
            },
            {
                "rule_id": "other",
                "tag": "other-user-rule",
                "value": "from:other",
                "interval_seconds": 60,
                "is_effect": 1,
            },
        ]
        self.get_calls = 0
        self.update_calls = []
        self.fail_update_rule_ids: set[str] = set()

    def get_filter_rules(self):
        self.get_calls += 1
        return {"rules": self.rules}

    def update_filter_rule(self, *, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
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


def test_rule_control_status_only_reads_formal_rules() -> None:
    client = FakeRuleControlClient()

    result = status_formal_rules(client)

    assert client.get_calls == 1
    assert client.update_calls == []
    assert [rule["tag"] for rule in result["rules"]] == [
        "wallet-agent-social-v1",
        "wallet-agent-social-v1-001",
        "wallet-agent-social-v1-002",
    ]


def test_rule_control_pause_only_stops_active_formal_social_rules(tmp_path) -> None:
    client = FakeRuleControlClient()
    state_path = tmp_path / "paused.json"

    result = pause_formal_rules(client, state_path=state_path)

    assert client.update_calls == [
        ("legacy", "wallet-agent-social-v1", "from:legacy", 300, False),
        ("shard", "wallet-agent-social-v1-001", "from:kol", 420, False),
    ]
    assert {row["tag"] for row in result["updated"]} == {"wallet-agent-social-v1", "wallet-agent-social-v1-001"}
    assert client.rules[3]["tag"] == "wallet-agent-cost-probe-v01"
    assert client.rules[3]["is_effect"] == 1
    assert client.rules[4]["tag"] == "other-user-rule"
    assert client.rules[4]["is_effect"] == 1
    state = json.loads(state_path.read_text())
    assert [row["rule_id"] for row in state["paused_rules"]] == ["legacy", "shard"]
    assert all("value" not in row for row in state["paused_rules"])
    assert "inactive" not in {row["rule_id"] for row in state["paused_rules"]}


def test_rule_control_pause_is_idempotent(tmp_path) -> None:
    client = FakeRuleControlClient()
    state_path = tmp_path / "paused.json"

    pause_formal_rules(client, state_path=state_path)
    pause_formal_rules(client, state_path=state_path)

    assert client.update_calls == [
        ("legacy", "wallet-agent-social-v1", "from:legacy", 300, False),
        ("shard", "wallet-agent-social-v1-001", "from:kol", 420, False),
    ]
    state = json.loads(state_path.read_text())
    assert [row["rule_id"] for row in state["paused_rules"]] == ["legacy", "shard"]


def test_rule_control_partial_pause_persists_successful_state_and_continues(tmp_path) -> None:
    client = FakeRuleControlClient()
    client.fail_update_rule_ids.add("shard")
    state_path = tmp_path / "paused.json"

    result = pause_formal_rules(client, state_path=state_path)

    assert result["cleanup_failed"] is True
    assert result["errors"][0]["rule_id"] == "shard"
    assert client.update_calls == [
        ("legacy", "wallet-agent-social-v1", "from:legacy", 300, False),
        ("shard", "wallet-agent-social-v1-001", "from:kol", 420, False),
    ]
    state = json.loads(state_path.read_text())
    assert [row["rule_id"] for row in state["paused_rules"]] == ["legacy"]


def test_rule_control_resume_preserves_value_and_interval_and_only_sets_active(tmp_path) -> None:
    client = FakeRuleControlClient()
    state_path = tmp_path / "paused.json"
    pause_formal_rules(client, state_path=state_path)
    client.update_calls.clear()

    result = resume_formal_rules(client, state_path=state_path)

    assert client.update_calls == [
        ("legacy", "wallet-agent-social-v1", "from:legacy", 300, True),
        ("shard", "wallet-agent-social-v1-001", "from:kol", 420, True),
    ]
    assert {row["active"] for row in result["updated"]} == {True}
    assert client.rules[1]["value"] == "from:kol"
    assert client.rules[1]["interval_seconds"] == 420
    assert client.rules[2]["rule_id"] == "inactive"
    assert client.rules[2]["is_effect"] == 0
    assert client.rules[3]["is_effect"] == 1
    assert client.rules[4]["is_effect"] == 1
    assert state_path.exists() is False


def test_rule_control_resume_without_state_refuses(tmp_path) -> None:
    client = FakeRuleControlClient()

    with pytest.raises(RuleControlError, match="No paused formal rule state"):
        resume_formal_rules(client, state_path=tmp_path / "missing.json")

    assert client.update_calls == []


def test_rule_control_resume_only_restores_state_rules(tmp_path) -> None:
    client = FakeRuleControlClient()
    state_path = tmp_path / "paused.json"
    state_path.write_text(
        json.dumps(
            {
                "paused_rules": [
                    {"rule_id": "shard", "tag": "wallet-agent-social-v1-001", "paused_at": "2026-09-08T00:00:00+00:00"}
                ]
            }
        )
    )
    client.rules[1]["is_effect"] = 0

    result = resume_formal_rules(client, state_path=state_path)

    assert client.update_calls == [("shard", "wallet-agent-social-v1-001", "from:kol", 420, True)]
    assert result["updated"][0]["rule_id"] == "shard"
    assert client.rules[0]["rule_id"] == "legacy"
    assert client.rules[0]["is_effect"] == 1
    assert client.rules[2]["rule_id"] == "inactive"
    assert client.rules[2]["is_effect"] == 0


def test_rule_control_resume_safely_skips_deleted_rule(tmp_path) -> None:
    client = FakeRuleControlClient()
    state_path = tmp_path / "paused.json"
    pause_formal_rules(client, state_path=state_path)
    client.rules = [rule for rule in client.rules if rule["rule_id"] != "shard"]
    client.update_calls.clear()

    result = resume_formal_rules(client, state_path=state_path)

    assert client.update_calls == [("legacy", "wallet-agent-social-v1", "from:legacy", 300, True)]
    assert result["skipped"] == [{"rule_id": "shard", "tag": "wallet-agent-social-v1-001", "reason": "missing_provider_rule"}]
    assert state_path.exists() is False
