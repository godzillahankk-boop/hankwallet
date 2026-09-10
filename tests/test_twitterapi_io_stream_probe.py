from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

from scripts.twitterapi_io_probe import (
    ProbeStats,
    _run_stream,
    handle_stream_message,
    parse_stream_message,
)


def tweet_payload(tweet_id: str, text: str = "$PONS") -> dict:
    return {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-02T12:00:00Z",
        "author": {"id": "1", "userName": "kol", "name": "KOL", "followers": 1000},
        "likeCount": 1,
        "retweetCount": 2,
        "replyCount": 3,
        "quoteCount": 4,
        "viewCount": 5,
    }


def test_parse_stream_single_tweet_event() -> None:
    parsed = parse_stream_message(
        json.dumps(
            {
                "event_type": "tweet",
                "rule_id": "rule-1",
                "rule_tag": "wallet-agent-probe-v01",
                "tweet": tweet_payload("1"),
                "snow_delay_ms": 123,
            }
        ),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert parsed is not None
    assert parsed.event_type == "tweet"
    assert parsed.rule_id == "rule-1"
    assert parsed.rule_tag == "wallet-agent-probe-v01"
    assert parsed.snow_delay_ms == 123
    assert len(parsed.tweets) == 1
    assert parsed.tweets[0].tweet_id == "1"
    assert parsed.tweets[0].author_username == "kol"
    assert parsed.tweets[0].delay_seconds() == 5


def test_parse_stream_multi_tweet_batch() -> None:
    parsed = parse_stream_message(
        json.dumps(
            {
                "event_type": "tweet",
                "rule_id": "rule-1",
                "rule_tag": "wallet-agent-probe-v01",
                "tweets": [tweet_payload("1"), tweet_payload("2", "$ABC")],
            }
        ),
        received_at=datetime(2026, 9, 2, 12, 0, 5, tzinfo=UTC),
    )

    assert parsed is not None
    assert [tweet.tweet_id for tweet in parsed.tweets] == ["1", "2"]
    assert parsed.rule_id == "rule-1"
    assert parsed.rule_tag == "wallet-agent-probe-v01"


def test_handle_stream_message_reuses_normalization_and_dedupes(capsys) -> None:
    stats = ProbeStats()
    message = json.dumps(
        {
            "event_type": "tweet",
            "rule_id": "rule-1",
            "rule_tag": "wallet-agent-probe-v01",
            "tweets": [tweet_payload("1"), tweet_payload("1")],
        }
    )

    handle_stream_message(message, stats=stats)

    captured = capsys.readouterr()
    assert "Tweet ID: 1" in captured.out
    assert stats.tweets_received == 2
    assert stats.unique_tweets == 1
    assert stats.duplicate_tweets == 1
    assert stats.event_types == {"tweet": 1}
    assert stats.rule_ids == {"rule-1"}
    assert stats.rule_tags == {"wallet-agent-probe-v01"}


def test_unknown_event_type_is_safely_ignored(capsys) -> None:
    stats = ProbeStats()

    handle_stream_message(
        json.dumps({"event_type": "heartbeat", "rule_id": "rule-1", "rule_tag": "probe"}),
        stats=stats,
    )

    captured = capsys.readouterr()
    assert "Ignored event_type=heartbeat" in captured.out
    assert stats.unique_tweets == 0
    assert stats.event_types == {"heartbeat": 1}


def test_malformed_stream_message_fails_safely() -> None:
    stats = ProbeStats()

    handle_stream_message("{bad json", stats=stats)

    assert stats.provider_errors == ["malformed websocket message"]


def test_connection_close_is_recorded_and_api_key_masked(monkeypatch) -> None:
    api_key = "secret-twitterapi-key"

    class FakeWebSocketTimeoutException(Exception):
        pass

    def create_connection(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError(f"closed with {api_key}")

    fake_websocket = SimpleNamespace(
        create_connection=create_connection,
        WebSocketTimeoutException=FakeWebSocketTimeoutException,
    )
    monkeypatch.setitem(sys.modules, "websocket", fake_websocket)
    stats = ProbeStats()

    _run_stream(
        api_key=api_key,
        ws_url="wss://ws.twitterapi.test/twitter/tweet/websocket",
        duration_minutes=0.01,
        max_reconnects=0,
        reconnect_cooldown_seconds=0,
        debug_raw=False,
        stats=stats,
    )

    assert len(stats.disconnects) == 1
    assert api_key not in stats.disconnects[0]
    assert "***" in stats.disconnects[0]
