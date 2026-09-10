from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from app.services.twitterapi_io_client import (
    TwitterApiIoClient,
    TwitterApiIoError,
    normalize_tweet,
    normalize_tweets_from_response,
)
from app.services.twitterapi_io_profile_provider import (
    TwitterApiIoSocialProfileProvider,
    normalize_recent_posts_response,
    normalize_user_profile_response,
)
from scripts.twitterapi_io_probe import ProbeStats


TOKEN_CA = "0xb6ec6e6e58354d956ef1a2ed6693d5c7056298d2"


def test_tweet_normalization_extracts_core_fields() -> None:
    tweet = normalize_tweet(
        {
            "id": "123",
            "text": f"Watching $PONS {TOKEN_CA}",
            "createdAt": "Wed Sep 02 12:01:20 +0000 2026",
            "conversationId": "100",
            "likeCount": 20,
            "retweetCount": 4,
            "replyCount": 3,
            "quoteCount": 1,
            "viewCount": 1020,
            "author": {
                "id": "42",
                "userName": "kol",
                "name": "KOL",
                "followers": 12345,
            },
        },
        detected_at=datetime(2026, 9, 2, 12, 1, 28, tzinfo=UTC),
    )

    assert tweet.provider == "twitterapi.io"
    assert tweet.tweet_id == "123"
    assert tweet.author_id == "42"
    assert tweet.author_username == "kol"
    assert tweet.author_name == "KOL"
    assert tweet.author_followers == 12345
    assert tweet.created_at == datetime(2026, 9, 2, 12, 1, 20, tzinfo=UTC)
    assert tweet.delay_seconds() == 8
    assert tweet.post_type == "original"
    assert tweet.like_count == 20
    assert tweet.retweet_count == 4
    assert tweet.reply_count == 3
    assert tweet.quote_count == 1
    assert tweet.view_count == 1020
    assert [(match.match_type, match.value) for match in tweet.token_matches] == [
        ("direct_ca", TOKEN_CA),
        ("direct_cashtag", "PONS"),
    ]


def test_quote_and_reply_metadata_are_preserved() -> None:
    tweet = normalize_tweet(
        {
            "id": "124",
            "text": "replying",
            "createdAt": "2026-09-02T12:02:00Z",
            "isReply": True,
            "inReplyToId": "120",
            "inReplyToUsername": "parent",
            "conversationId": "100",
            "quoted_tweet": {
                "id": "121",
                "text": f"Original call $ABC {TOKEN_CA.upper().replace('0X', '0x')}",
                "author": {"userName": "quoted"},
            },
            "author": {"userName": "kol"},
        },
        detected_at=datetime(2026, 9, 2, 12, 2, 10, tzinfo=UTC),
    )

    assert tweet.is_reply is True
    assert tweet.in_reply_to_id == "120"
    assert tweet.in_reply_to_username == "parent"
    assert tweet.conversation_id == "100"
    assert tweet.is_quote is True
    assert tweet.quoted_tweet_id == "121"
    assert tweet.quoted_tweet == {
        "tweet_id": "121",
        "author_username": "quoted",
        "text": f"Original call $ABC {TOKEN_CA.upper().replace('0X', '0x')}",
    }
    assert tweet.post_type == "quote"
    assert [(match.match_type, match.value) for match in tweet.token_matches] == [
        ("quote_ca", TOKEN_CA),
        ("quote_cashtag", "ABC"),
    ]


def test_retweet_metadata_is_preserved() -> None:
    tweet = normalize_tweet(
        {
            "id": "125",
            "text": "RT",
            "retweeted_tweet": {
                "id": "122",
                "text": "$RT",
                "author": {"userName": "source"},
            },
        },
        detected_at=datetime(2026, 9, 2, 12, 2, 10, tzinfo=UTC),
    )

    assert tweet.is_retweet is True
    assert tweet.retweeted_tweet_id == "122"
    assert tweet.post_type == "retweet"


def test_missing_engagement_fields_are_safe() -> None:
    tweet = normalize_tweet({"id": "126", "text": "$ABC"})

    assert tweet.like_count is None
    assert tweet.retweet_count is None
    assert tweet.reply_count is None
    assert tweet.quote_count is None
    assert tweet.view_count is None


def test_malformed_payload_fails_clearly() -> None:
    with pytest.raises(TwitterApiIoError, match="missing id"):
        normalize_tweet({"text": "$ABC"})

    with pytest.raises(TwitterApiIoError, match="malformed tweet payload"):
        normalize_tweet("not a dict")  # type: ignore[arg-type]


def test_normalize_tweets_from_response_supports_documented_tweets_key() -> None:
    tweets = normalize_tweets_from_response(
        {
            "tweets": [
                {"id": "1", "text": "$ONE"},
                {"id": "2", "text": "$TWO"},
                "bad row",
            ]
        },
        detected_at=datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC),
    )

    assert [tweet.tweet_id for tweet in tweets] == ["1", "2"]


def test_probe_stats_dedupes_by_tweet_id() -> None:
    stats = ProbeStats()
    tweet = normalize_tweet({"id": "127", "text": "$ABC"})

    assert stats.add_tweet(tweet) is True
    assert stats.add_tweet(tweet) is False
    assert stats.tweets_received == 2
    assert stats.unique_tweets == 1
    assert stats.tweets_with_cashtag == 1


def test_client_masks_secret_in_non_200_errors() -> None:
    api_key = "secret-twitterapi-key"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=f"provider failed {api_key}", request=request)

    client = TwitterApiIoClient(
        api_key,
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(TwitterApiIoError) as exc_info:
        client.advanced_search("from:test")

    assert api_key not in str(exc_info.value)
    assert "***" in str(exc_info.value)


def test_client_uses_x_api_key_header_and_query_params() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["api_key"] = request.headers.get("X-API-Key", "")
        seen["query"] = request.url.params.get("query", "")
        seen["query_type"] = request.url.params.get("queryType", "")
        return httpx.Response(200, json={"status": "success", "tweets": []}, request=request)

    client = TwitterApiIoClient(
        "test-key",
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
    )

    data = client.advanced_search("from:lookonchain", query_type="Latest")

    assert data["status"] == "success"
    assert seen == {
        "api_key": "test-key",
        "query": "from:lookonchain",
        "query_type": "Latest",
    }
    assert client.stats_snapshot()["advanced_search_http_attempts"] == 1
    assert client.stats_snapshot()["http_requests_total"] == 1


def test_client_http_attempt_metrics_count_transport_retry() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("temporary network failure", request=request)
        return httpx.Response(200, json={"status": "success", "tweets": []}, request=request)

    client = TwitterApiIoClient(
        "test-key",
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
        max_retries=1,
    )

    client.advanced_search("0xtoken")

    snapshot = client.stats_snapshot()
    assert snapshot["http_requests_total"] == 2
    assert snapshot["advanced_search_http_attempts"] == 2


def test_client_profile_and_recent_http_attempt_metrics() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "success", "data": {}}, request=request)

    client = TwitterApiIoClient(
        "secret-key",
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
    )

    client.get_user_info("lookonchain")
    client.get_user_last_tweets("lookonchain")
    snapshot = client.stats_snapshot()

    assert snapshot == {
        "http_requests_total": 2,
        "advanced_search_http_attempts": 0,
        "profile_http_attempts": 1,
        "recent_posts_http_attempts": 1,
        "filter_http_attempts": 0,
    }
    assert "secret-key" not in str(snapshot)
    assert "lookonchain" not in str(snapshot)


def test_client_user_profile_and_last_tweets_endpoints() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.url.params.get("userName", "")))
        return httpx.Response(200, json={"status": "success", "data": {}}, request=request)

    client = TwitterApiIoClient(
        "test-key",
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
    )

    client.get_user_info("lookonchain")
    client.get_user_last_tweets("lookonchain")

    assert seen == [
        ("/twitter/user/info", "lookonchain"),
        ("/twitter/user/last_tweets", "lookonchain"),
    ]


def test_profile_response_shape_maps_required_fields_and_does_not_keep_raw() -> None:
    profile = normalize_user_profile_response(
        {
            "status": "success",
            "data": {
                "id": "44196397",
                "userName": "lookonchain",
                "followers": 650000,
                "description": "Onchain analytics",
                "raw_extra": {"not": "stored"},
            },
        },
        fallback_username="fallback",
    )

    assert profile.author_id == "44196397"
    assert profile.username == "lookonchain"
    assert profile.followers == 650000
    assert profile.bio == "Onchain analytics"
    assert not hasattr(profile, "raw")


def test_recent_tweets_response_shape_filters_retweets_and_limits_to_five() -> None:
    data = {
        "status": "success",
        "data": {
            "tweets": [
                {"id": "rt", "text": "retweet", "retweeted_tweet": {"id": "source"}},
                {"id": "1", "text": "one"},
                {"id": "2", "text": "two", "quoted_tweet": {"id": "q", "text": "$Q"}},
                {"id": "3", "text": "three", "isReply": True},
                {"id": "4", "text": "four"},
                {"id": "5", "text": "five"},
                {"id": "6", "text": "six"},
            ]
        },
    }

    posts = normalize_recent_posts_response(data, limit=5)

    assert [post.post_id for post in posts] == ["1", "2", "3", "4", "5"]
    assert [post.post_type for post in posts] == ["original", "quote", "reply", "original", "original"]


def test_twitterapi_io_social_profile_provider_reuses_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/twitter/user/info":
            return httpx.Response(
                200,
                json={"status": "success", "user": {"id": "42", "userName": "kol", "followers": 1000, "description": "web3"}},
                request=request,
            )
        return httpx.Response(
            200,
            json={"status": "success", "tweets": [{"id": "1", "text": "crypto"}, {"id": "2", "text": "rt", "retweeted_tweet": {"id": "r"}}]},
            request=request,
        )

    client = TwitterApiIoClient(
        "test-key",
        base_url="https://twitterapi.test",
        transport=httpx.MockTransport(handler),
    )
    provider = TwitterApiIoSocialProfileProvider(client)

    profile = provider.get_user_profile("kol")
    posts = provider.get_recent_posts("kol", limit=5)

    assert profile.author_id == "42"
    assert profile.bio == "web3"
    assert [post.post_id for post in posts] == ["1"]
