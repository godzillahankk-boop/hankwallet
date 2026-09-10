from __future__ import annotations

import json
from datetime import UTC, datetime

from scripts.twitter_token_identity_probe import (
    CA_PROBE_RULE_TAG,
    CASHTAG_PROBE_RULE_TAG,
    TokenIdentityProbeStats,
    record_stream_message,
    run_token_identity_probe,
)

TOKEN_CA = "0x39dbed3a2bd333467115de45665cc57f813c4571"


class FakeTokenIdentityClient:
    def __init__(self) -> None:
        self.rules = [
            {
                "rule_id": "formal",
                "tag": "wallet-agent-social-v1-001",
                "value": "from:formal",
                "interval_seconds": 300,
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
        self.add_calls = []
        self.update_calls = []
        self.fail_get_on_call = None
        self.get_error_message = "get rules failed"

    def get_filter_rules(self):
        self.get_calls += 1
        if self.fail_get_on_call is not None and self.get_calls >= self.fail_get_on_call:
            raise RuntimeError(self.get_error_message)
        return {"rules": self.rules}

    def add_filter_rule(self, *, tag: str, value: str, interval_seconds: int):
        self.add_calls.append((tag, value, interval_seconds))
        rule_id = f"created-{tag}"
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

    def update_filter_rule(self, *, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
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


def test_token_identity_probe_aborts_when_formal_rule_active_without_creating_rules() -> None:
    client = FakeTokenIdentityClient()

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is True
    assert summary["abort_reason"] == "ACTIVE_FORMAL_RULES_DETECTED"
    assert summary["formal_rules_active_at_start"] is True
    assert summary["cost_isolated"] is False
    assert summary["probe_conclusion"] == "ABORTED_ACTIVE_FORMAL_RULES"
    assert client.add_calls == []
    assert client.update_calls == []


def test_token_identity_probe_aborts_when_foreign_rule_active_without_creating_rules() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is True
    assert summary["abort_reason"] == "ACTIVE_FOREIGN_RULES_DETECTED"
    assert summary["cost_isolated"] is False
    assert summary["probe_conclusion"] == "ABORTED_ACTIVE_FOREIGN_RULES"
    assert summary["active_foreign_rules"] == [
        {
            "tag": "other-user-rule",
            "rule_id": "other",
            "active": True,
            "interval_seconds": 60,
            "value_length": len("from:other"),
        }
    ]
    assert client.add_calls == []
    assert client.update_calls == []


def test_token_identity_probe_inactive_foreign_rule_does_not_abort() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is False
    assert summary["cost_isolated"] is True
    assert summary["cleanup_verified"] is True
    assert summary["active_probe_rules_after_cleanup"] == []
    assert summary["safe_to_stop_monitoring"] is True
    assert summary["probe_conclusion"] == "ISOLATED"
    assert client.add_calls == [
        (CA_PROBE_RULE_TAG, TOKEN_CA, 300),
        (CASHTAG_PROBE_RULE_TAG, "$WALLET", 300),
    ]


def test_token_identity_probe_existing_active_probe_rules_are_not_foreign() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    client.rules.extend(
        [
            {
                "rule_id": "existing-ca",
                "tag": CA_PROBE_RULE_TAG,
                "value": TOKEN_CA,
                "interval_seconds": 300,
                "is_effect": 1,
            },
            {
                "rule_id": "existing-cashtag",
                "tag": CASHTAG_PROBE_RULE_TAG,
                "value": "$WALLET",
                "interval_seconds": 300,
                "is_effect": 1,
            },
        ]
    )

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["aborted"] is False
    assert summary["active_foreign_rules"] == []
    assert client.add_calls == []
    assert client.update_calls[-2:] == [
        ("existing-ca", CA_PROBE_RULE_TAG, TOKEN_CA, 300, False),
        ("existing-cashtag", CASHTAG_PROBE_RULE_TAG, "$WALLET", 300, False),
    ]


def test_token_identity_probe_deactivates_ca_and_cashtag_rules_in_finally() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0

    def fail_runner(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("ws down")

    try:
        run_token_identity_probe(
            client,
            api_key="secret-key",
            ca=TOKEN_CA,
            symbol="WALLET",
            interval_seconds=300,
            websocket_runner=fail_runner,
        )
    except RuntimeError:
        pass

    assert client.add_calls == [
        (CA_PROBE_RULE_TAG, TOKEN_CA, 300),
        (CASHTAG_PROBE_RULE_TAG, "$WALLET", 300),
    ]
    assert client.update_calls[-2:] == [
        (f"created-{CA_PROBE_RULE_TAG}", CA_PROBE_RULE_TAG, TOKEN_CA, 300, False),
        (f"created-{CASHTAG_PROBE_RULE_TAG}", CASHTAG_PROBE_RULE_TAG, "$WALLET", 300, False),
    ]
    assert client.rules[0]["tag"] == "wallet-agent-social-v1-001"
    assert client.rules[0]["is_effect"] == 0
    assert client.rules[1]["tag"] == "other-user-rule"
    assert client.rules[1]["is_effect"] == 0


def test_token_identity_probe_ca_cleanup_failure_does_not_skip_cashtag_cleanup() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    original_update = client.update_filter_rule

    def fail_ca_update(*, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
        if tag == CA_PROBE_RULE_TAG and is_effect is False:
            client.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
            raise RuntimeError("cleanup failed with secret-key")
        return original_update(rule_id=rule_id, tag=tag, value=value, interval_seconds=interval_seconds, is_effect=is_effect)

    client.update_filter_rule = fail_ca_update

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert (f"created-{CA_PROBE_RULE_TAG}", CA_PROBE_RULE_TAG, TOKEN_CA, 300, False) in client.update_calls
    assert (f"created-{CASHTAG_PROBE_RULE_TAG}", CASHTAG_PROBE_RULE_TAG, "$WALLET", 300, False) in client.update_calls
    assert summary["cleanup_failed"] is True
    assert summary["cleanup_errors"][0]["tag"] == CA_PROBE_RULE_TAG
    assert summary["cleanup_verified"] is False
    assert summary["safe_to_stop_monitoring"] is False
    assert summary["cost_isolated"] is True
    assert summary["probe_conclusion"] == "ISOLATED_BUT_CLEANUP_FAILED"
    assert "secret-key" not in json.dumps(summary["cleanup_errors"])


def test_token_identity_probe_cashtag_cleanup_failure_after_ca_attempt_is_reported() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    original_update = client.update_filter_rule

    def fail_cashtag_update(*, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
        if tag == CASHTAG_PROBE_RULE_TAG and is_effect is False:
            client.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
            raise RuntimeError("cashtag cleanup failed")
        return original_update(rule_id=rule_id, tag=tag, value=value, interval_seconds=interval_seconds, is_effect=is_effect)

    client.update_filter_rule = fail_cashtag_update

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert (f"created-{CA_PROBE_RULE_TAG}", CA_PROBE_RULE_TAG, TOKEN_CA, 300, False) in client.update_calls
    assert (f"created-{CASHTAG_PROBE_RULE_TAG}", CASHTAG_PROBE_RULE_TAG, "$WALLET", 300, False) in client.update_calls
    assert summary["cleanup_failed"] is True
    assert summary["cleanup_errors"][0]["tag"] == CASHTAG_PROBE_RULE_TAG
    assert summary["safe_to_stop_monitoring"] is False
    assert summary["probe_conclusion"] == "ISOLATED_BUT_CLEANUP_FAILED"


def test_token_identity_probe_post_cleanup_verifies_both_probe_rules_inactive() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["cleanup_failed"] is False
    assert summary["cleanup_verified"] is True
    assert summary["active_probe_rules_after_cleanup"] == []
    assert summary["safe_to_stop_monitoring"] is True
    assert summary["probe_conclusion"] == "ISOLATED"


def test_token_identity_probe_ca_still_active_after_cleanup_is_not_safe() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    original_update = client.update_filter_rule

    def keep_ca_active(*, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
        if tag == CA_PROBE_RULE_TAG and is_effect is False:
            client.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
            return {"status": "success"}
        return original_update(rule_id=rule_id, tag=tag, value=value, interval_seconds=interval_seconds, is_effect=is_effect)

    client.update_filter_rule = keep_ca_active

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["cleanup_failed"] is False
    assert summary["cleanup_verified"] is False
    assert summary["safe_to_stop_monitoring"] is False
    assert summary["probe_conclusion"] == "ISOLATED_BUT_CLEANUP_UNVERIFIED"
    assert summary["active_probe_rules_after_cleanup"] == [
        {
            "tag": CA_PROBE_RULE_TAG,
            "rule_id": f"created-{CA_PROBE_RULE_TAG}",
            "active": True,
            "interval_seconds": 300,
            "value_length": len(TOKEN_CA),
        }
    ]


def test_token_identity_probe_cashtag_still_active_after_cleanup_is_not_safe() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    original_update = client.update_filter_rule

    def keep_cashtag_active(*, rule_id: str, tag: str, value: str, interval_seconds: int, is_effect: bool):
        if tag == CASHTAG_PROBE_RULE_TAG and is_effect is False:
            client.update_calls.append((rule_id, tag, value, interval_seconds, is_effect))
            return {"status": "success"}
        return original_update(rule_id=rule_id, tag=tag, value=value, interval_seconds=interval_seconds, is_effect=is_effect)

    client.update_filter_rule = keep_cashtag_active

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["cleanup_failed"] is False
    assert summary["cleanup_verified"] is False
    assert summary["safe_to_stop_monitoring"] is False
    assert summary["probe_conclusion"] == "ISOLATED_BUT_CLEANUP_UNVERIFIED"
    assert summary["active_probe_rules_after_cleanup"] == [
        {
            "tag": CASHTAG_PROBE_RULE_TAG,
            "rule_id": f"created-{CASHTAG_PROBE_RULE_TAG}",
            "active": True,
            "interval_seconds": 300,
            "value_length": len("$WALLET"),
        }
    ]


def test_token_identity_probe_post_cleanup_get_failure_returns_sanitized_summary() -> None:
    client = FakeTokenIdentityClient()
    client.rules[0]["is_effect"] = 0
    client.rules[1]["is_effect"] = 0
    client.fail_get_on_call = 4
    client.get_error_message = "get rules failed with secret-key"

    summary = run_token_identity_probe(
        client,
        api_key="secret-key",
        ca=TOKEN_CA,
        symbol="WALLET",
        interval_seconds=300,
        websocket_runner=lambda *args, **kwargs: None,
    )

    assert summary["cleanup_failed"] is False
    assert summary["cleanup_verified"] is False
    assert summary["active_probe_rules_after_cleanup"] == []
    assert summary["cleanup_verification_error"] == "get rules failed with ***"
    assert summary["safe_to_stop_monitoring"] is False
    assert summary["probe_conclusion"] == "ISOLATED_BUT_CLEANUP_UNVERIFIED"
    assert "secret-key" not in json.dumps(summary)


def test_token_identity_probe_classifies_ca_only_cashtag_only_and_both() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(stream_message("ca", CA_PROBE_RULE_TAG, f"New token {TOKEN_CA}"), stats=stats, received_at=started)
    record_stream_message(stream_message("tag", CASHTAG_PROBE_RULE_TAG, "Watching $WALLET"), stats=stats, received_at=started)
    both = stream_message("both", CA_PROBE_RULE_TAG, f"$WALLET {TOKEN_CA}")
    record_stream_message(both, stats=stats, received_at=started)
    record_stream_message(stream_message("both", CASHTAG_PROBE_RULE_TAG, f"$WALLET {TOKEN_CA}"), stats=stats, received_at=started)

    summary = stats.summary(duration_seconds=1)

    assert summary["ca_only_count"] == 1
    assert summary["cashtag_only_count"] == 1
    assert summary["both_count"] == 1
    statuses = {sample["tweet_id"]: sample["identity_status"] for sample in summary["sample_tweets"]}
    assert statuses == {
        "ca": "exact_ca",
        "tag": "ambiguous_symbol",
        "both": "exact_ca_and_cashtag",
    }


def test_token_identity_probe_aggregate_counts_exceed_sample_limit() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started, max_sample_tweets=20)

    for index in range(30):
        record_stream_message(
            stream_message(f"tweet-{index}", CA_PROBE_RULE_TAG, f"New token {TOKEN_CA}"),
            stats=stats,
            received_at=started,
        )

    summary = stats.summary(duration_seconds=1)
    assert summary["unique_tweet_ids"] == 30
    assert summary["ca_only_count"] == 30
    assert len(summary["sample_tweets"]) == 20


def test_token_identity_probe_both_classification_survives_sample_limit() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started, max_sample_tweets=20)

    for index in range(30):
        text = f"$WALLET {TOKEN_CA}" if index == 24 else f"New token {TOKEN_CA}"
        record_stream_message(
            stream_message(f"tweet-{index}", CA_PROBE_RULE_TAG, text),
            stats=stats,
            received_at=started,
        )
    record_stream_message(
        stream_message("tweet-24", CASHTAG_PROBE_RULE_TAG, f"$WALLET {TOKEN_CA}"),
        stats=stats,
        received_at=started,
    )

    summary = stats.summary(duration_seconds=1)
    assert summary["unique_tweet_ids"] == 30
    assert summary["both_count"] == 1
    assert summary["ca_only_count"] == 29
    assert len(summary["sample_tweets"]) == 20
    assert "tweet-24" not in {sample["tweet_id"] for sample in summary["sample_tweets"]}


def test_token_identity_probe_does_not_confuse_plain_wallet_with_cashtag() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(
        stream_message("plain", CASHTAG_PROBE_RULE_TAG, "wallet is a normal word"),
        stats=stats,
        received_at=started,
    )

    sample = stats.summary(duration_seconds=1)["sample_tweets"][0]
    assert sample["classification"] == "cashtag_only"
    assert sample["contains_exact_cashtag"] is False
    assert sample["identity_status"] == "foreign_or_unknown"
    assert sample["identity_evidence"]["cashtag"] is None


def test_token_identity_probe_cashtag_only_does_not_resolve_to_probe_ca() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(stream_message("tag", CASHTAG_PROBE_RULE_TAG, "$WALLET"), stats=stats, received_at=started)

    sample = stats.summary(duration_seconds=1)["sample_tweets"][0]
    assert sample["identity_status"] == "ambiguous_symbol"
    assert "resolved_token" not in sample


def test_token_identity_probe_card_metadata_absent_is_false() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(stream_message("tag", CASHTAG_PROBE_RULE_TAG, "$WALLET"), stats=stats, received_at=started)

    evidence = stats.summary(duration_seconds=1)["sample_tweets"][0]["identity_evidence"]
    assert evidence["token_card_metadata_found"] is False
    assert evidence["token_card_fields"] == []


def test_token_identity_probe_card_metadata_and_unknown_payload_fields_are_safe() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)
    payload = tweet_payload(
        "card",
        "$WALLET on Base",
        card={"binding_values": {"token": {"chain": "base", "contract_address": TOKEN_CA}}},
        entities={
            "urls": [{"expanded_url": "https://x.com/i/cashtags/WALLET"}],
            "user_mentions": [{"screen_name": "SomeProject"}],
        },
        unknown_extra={"nested": {"ok": True}},
    )

    record_stream_message(message(CASHTAG_PROBE_RULE_TAG, payload), stats=stats, received_at=started)

    evidence = stats.summary(duration_seconds=1)["sample_tweets"][0]["identity_evidence"]
    assert evidence["candidate_token_metadata_found"] is True
    assert evidence["token_card_metadata_found"] is True
    assert any(field["path"] == "card" for field in evidence["token_card_fields"])
    assert evidence["chain_mentions"] == ["Base"]
    assert evidence["urls"] == ["https://x.com/i/cashtags/WALLET"]
    assert evidence["mentioned_accounts"] == ["SomeProject"]


def test_token_identity_probe_generic_token_metadata_is_candidate_not_confirmed_card() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)
    payload = tweet_payload(
        "generic",
        "$WALLET",
        token={"symbol": "WALLET"},
        chain="base",
        contract_address=TOKEN_CA,
    )

    record_stream_message(message(CASHTAG_PROBE_RULE_TAG, payload), stats=stats, received_at=started)

    evidence = stats.summary(duration_seconds=1)["sample_tweets"][0]["identity_evidence"]
    assert evidence["candidate_token_metadata_found"] is True
    assert evidence["candidate_token_metadata_fields"]
    assert evidence["token_card_metadata_found"] is False
    assert evidence["token_card_fields"] == []


def test_token_identity_probe_foreign_rule_does_not_pollute_probe_stats() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(stream_message("foreign", "wallet-agent-social-v1-001", "$WALLET"), stats=stats, received_at=started)

    summary = stats.summary(duration_seconds=1)
    assert summary["foreign_rule_messages"] == 1
    assert summary["foreign_rule_tweet_deliveries"] == 1
    assert summary["ca_tweet_deliveries"] == 0
    assert summary["cashtag_tweet_deliveries"] == 0
    assert summary["unique_tweet_ids"] == 0
    assert summary["cost_isolated"] is False
    assert summary["probe_conclusion"] == "INCONCLUSIVE_FOREIGN_RULE_ACTIVITY"


def test_token_identity_probe_cost_isolated_when_no_foreign_rule_activity() -> None:
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)

    record_stream_message(stream_message("tag", CASHTAG_PROBE_RULE_TAG, "$WALLET"), stats=stats, received_at=started)

    summary = stats.summary(duration_seconds=1)
    assert summary["cost_isolated"] is True
    assert summary["probe_conclusion"] == "ISOLATED"


def test_token_identity_probe_summary_does_not_include_api_key_or_authorization(monkeypatch) -> None:
    monkeypatch.setenv("TWITTERAPI_IO_API_KEY", "secret-key")
    started = datetime(2026, 9, 9, 1, 0, 0, tzinfo=UTC)
    stats = TokenIdentityProbeStats(ca=TOKEN_CA, symbol="WALLET", started_at=started)
    payload = tweet_payload("secret", "$WALLET", Authorization="secret-key", **{"x-api-key": "secret-key"})

    record_stream_message(message(CASHTAG_PROBE_RULE_TAG, payload), stats=stats, received_at=started)

    output = json.dumps(stats.summary(duration_seconds=1))
    assert "secret-key" not in output
    assert "Authorization" not in output
    assert "x-api-key" not in output


def stream_message(tweet_id: str, rule_tag: str, text: str) -> str:
    return message(rule_tag, tweet_payload(tweet_id, text))


def message(rule_tag: str, tweet: dict) -> str:
    return json.dumps(
        {
            "event_type": "tweet",
            "rule_id": f"rule-{rule_tag}",
            "rule_tag": rule_tag,
            "tweet": tweet,
        }
    )


def tweet_payload(tweet_id: str, text: str, **extra) -> dict:
    return {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-09T01:00:00Z",
        "author": {"id": f"author-{tweet_id}", "userName": f"Author{tweet_id}", "followers": 1234},
        **extra,
    }
