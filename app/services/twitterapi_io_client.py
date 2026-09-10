from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from app.services.social_token_matcher import TokenMatch, extract_token_matches
from app.utils.time import utc_now

PROVIDER = "twitterapi.io"
DEFAULT_BASE_URL = "https://api.twitterapi.io"


class TwitterApiIoError(RuntimeError):
    pass


class TwitterApiIoAuthError(TwitterApiIoError):
    pass


@dataclass
class TwitterApiIoHttpStats:
    http_requests_total: int = 0
    advanced_search_http_attempts: int = 0
    profile_http_attempts: int = 0
    recent_posts_http_attempts: int = 0
    filter_http_attempts: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "http_requests_total": self.http_requests_total,
            "advanced_search_http_attempts": self.advanced_search_http_attempts,
            "profile_http_attempts": self.profile_http_attempts,
            "recent_posts_http_attempts": self.recent_posts_http_attempts,
            "filter_http_attempts": self.filter_http_attempts,
        }


@dataclass(frozen=True)
class NormalizedTweet:
    provider: str
    tweet_id: str
    author_id: str | None
    author_username: str | None
    author_name: str | None
    author_followers: int | None
    text: str | None
    created_at: datetime | None
    detected_at: datetime
    is_reply: bool
    in_reply_to_id: str | None
    in_reply_to_username: str | None
    conversation_id: str | None
    is_quote: bool
    quoted_tweet_id: str | None
    quoted_tweet: dict[str, Any] | None
    is_retweet: bool
    retweeted_tweet_id: str | None
    retweeted_tweet: dict[str, Any] | None
    like_count: int | None
    retweet_count: int | None
    reply_count: int | None
    quote_count: int | None
    view_count: int | None
    token_matches: list[TokenMatch]
    raw: dict[str, Any]

    @property
    def post_type(self) -> str:
        if self.is_retweet:
            return "retweet"
        if self.is_quote:
            return "quote"
        if self.is_reply:
            return "reply"
        return "original"

    def delay_seconds(self) -> float | None:
        if not self.created_at:
            return None
        created = _as_aware_utc(self.created_at)
        detected = _as_aware_utc(self.detected_at)
        return (detected - created).total_seconds()

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "tweet_id": self.tweet_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "author_username": self.author_username,
            "text": self.text,
            "is_reply": self.is_reply,
            "in_reply_to_id": self.in_reply_to_id,
            "conversation_id": self.conversation_id,
            "is_quote": self.is_quote,
            "quoted_tweet_id": self.quoted_tweet_id,
            "is_retweet": self.is_retweet,
            "retweeted_tweet_id": self.retweeted_tweet_id,
            "like_count": self.like_count,
            "retweet_count": self.retweet_count,
            "reply_count": self.reply_count,
            "quote_count": self.quote_count,
            "view_count": self.view_count,
            "matches": [match.to_dict() for match in self.token_matches],
        }


class TwitterApiIoClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 20,
        max_retries: int = 1,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise TwitterApiIoAuthError("TWITTERAPI_IO_API_KEY is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(0, max_retries)
        self.transport = transport
        self._http_stats = TwitterApiIoHttpStats()

    def stats_snapshot(self) -> dict[str, int]:
        return self._http_stats.snapshot()

    def advanced_search(self, query: str, *, query_type: str = "Latest", cursor: str = "") -> dict[str, Any]:
        return self._request(
            "GET",
            "/twitter/tweet/advanced_search",
            params={"query": query, "queryType": query_type, "cursor": cursor},
        )

    def search_tweets(self, query: str, *, query_type: str = "Latest", cursor: str = "") -> list[NormalizedTweet]:
        data = self.advanced_search(query, query_type=query_type, cursor=cursor)
        return normalize_tweets_from_response(data, detected_at=utc_now())

    def get_user_info(self, username: str) -> dict[str, Any]:
        return self._request("GET", "/twitter/user/info", params={"userName": username})

    def get_user_last_tweets(self, username: str) -> dict[str, Any]:
        return self._request("GET", "/twitter/user/last_tweets", params={"userName": username})

    def get_filter_rules(self) -> dict[str, Any]:
        return self._request("GET", "/oapi/tweet_filter/get_rules")

    def add_filter_rule(self, *, tag: str, value: str, interval_seconds: float = 60) -> dict[str, Any]:
        return self._request(
            "POST",
            "/oapi/tweet_filter/add_rule",
            json_body={"tag": tag, "value": value, "interval_seconds": interval_seconds},
        )

    def update_filter_rule(
        self,
        *,
        rule_id: str,
        tag: str,
        value: str,
        interval_seconds: float,
        is_effect: bool,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/oapi/tweet_filter/update_rule",
            json_body={
                "rule_id": rule_id,
                "tag": tag,
                "value": value,
                "interval_seconds": interval_seconds,
                "is_effect": 1 if is_effect else 0,
            },
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        headers = {"X-API-Key": self.api_key}
        for _ in range(self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout_seconds, transport=self.transport) as client:
                    self._record_http_attempt(path)
                    response = client.request(
                        method,
                        f"{self.base_url}{path}",
                        headers=headers,
                        params=params,
                        json=json_body,
                    )
                if response.status_code in {401, 403}:
                    raise TwitterApiIoAuthError(f"TwitterAPI.io auth failed status={response.status_code}")
                if response.status_code != 200:
                    raise TwitterApiIoError(
                        "TwitterAPI.io request failed "
                        f"status={response.status_code} body={_safe_body(response.text, self.api_key)}"
                    )
                try:
                    data = response.json()
                except json.JSONDecodeError as exc:
                    raise TwitterApiIoError("TwitterAPI.io returned malformed json") from exc
                _raise_provider_error(data)
                return data
            except (TwitterApiIoAuthError, TwitterApiIoError):
                raise
            except Exception as exc:
                last_error = exc
        raise TwitterApiIoError(f"TwitterAPI.io request failed: {_mask_secret(str(last_error), self.api_key)}")

    def _record_http_attempt(self, path: str) -> None:
        self._http_stats.http_requests_total += 1
        if path == "/twitter/tweet/advanced_search":
            self._http_stats.advanced_search_http_attempts += 1
        elif path == "/twitter/user/info":
            self._http_stats.profile_http_attempts += 1
        elif path == "/twitter/user/last_tweets":
            self._http_stats.recent_posts_http_attempts += 1
        elif path.startswith("/oapi/tweet_filter/"):
            self._http_stats.filter_http_attempts += 1


def normalize_tweet(tweet: dict[str, Any], *, detected_at: datetime | None = None) -> NormalizedTweet:
    if not isinstance(tweet, dict):
        raise TwitterApiIoError("malformed tweet payload")
    tweet_id = _str_or_none(_first(tweet, "id", "tweet_id", "tweetId"))
    if not tweet_id:
        raise TwitterApiIoError("malformed tweet payload: missing id")
    detected = detected_at or utc_now()
    author = _dict_or_none(tweet.get("author")) or {}
    quoted = _dict_or_none(_first(tweet, "quoted_tweet", "quotedTweet"))
    retweeted = _dict_or_none(_first(tweet, "retweeted_tweet", "retweetedTweet"))
    direct_matches = extract_token_matches(_str_or_none(tweet.get("text")), prefix="direct")
    quote_matches = extract_token_matches(_str_or_none(quoted.get("text")) if quoted else None, prefix="quote")
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=tweet_id,
        author_id=_str_or_none(_first(author, "id", "userId", "user_id")),
        author_username=_str_or_none(_first(author, "userName", "username", "screen_name")),
        author_name=_str_or_none(author.get("name")),
        author_followers=_int_or_none(_first(author, "followers", "followers_count")),
        text=_str_or_none(tweet.get("text")),
        created_at=_parse_datetime(_first(tweet, "createdAt", "created_at")),
        detected_at=detected,
        is_reply=bool(_first(tweet, "isReply", "is_reply") or _first(tweet, "inReplyToId", "in_reply_to_id")),
        in_reply_to_id=_str_or_none(_first(tweet, "inReplyToId", "in_reply_to_id")),
        in_reply_to_username=_str_or_none(_first(tweet, "inReplyToUsername", "in_reply_to_username")),
        conversation_id=_str_or_none(_first(tweet, "conversationId", "conversation_id")),
        is_quote=quoted is not None,
        quoted_tweet_id=_str_or_none(_first(quoted or {}, "id", "tweet_id", "tweetId")),
        quoted_tweet=_tweet_excerpt(quoted),
        is_retweet=retweeted is not None,
        retweeted_tweet_id=_str_or_none(_first(retweeted or {}, "id", "tweet_id", "tweetId")),
        retweeted_tweet=_tweet_excerpt(retweeted),
        like_count=_int_or_none(_first(tweet, "likeCount", "like_count")),
        retweet_count=_int_or_none(_first(tweet, "retweetCount", "retweet_count")),
        reply_count=_int_or_none(_first(tweet, "replyCount", "reply_count")),
        quote_count=_int_or_none(_first(tweet, "quoteCount", "quote_count")),
        view_count=_int_or_none(_first(tweet, "viewCount", "view_count")),
        token_matches=[*direct_matches, *quote_matches],
        raw=tweet,
    )


def normalize_tweets_from_response(
    data: dict[str, Any],
    *,
    detected_at: datetime | None = None,
) -> list[NormalizedTweet]:
    detected = detected_at or utc_now()
    return [normalize_tweet(tweet, detected_at=detected) for tweet in _extract_tweet_list(data)]


def _extract_tweet_list(data: dict[str, Any]) -> list[dict[str, Any]]:
    tweets = data.get("tweets")
    if isinstance(tweets, list):
        return [tweet for tweet in tweets if isinstance(tweet, dict)]
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("tweets"), list):
        return [tweet for tweet in nested["tweets"] if isinstance(tweet, dict)]
    if isinstance(nested, list):
        return [tweet for tweet in nested if isinstance(tweet, dict)]
    return []


def _raise_provider_error(data: dict[str, Any]) -> None:
    if not isinstance(data, dict):
        raise TwitterApiIoError("TwitterAPI.io returned malformed json")
    status = data.get("status")
    if isinstance(status, str) and status.lower() == "error":
        message = data.get("msg") or data.get("message") or "provider error"
        raise TwitterApiIoError(f"TwitterAPI.io provider error: {message}")
    if "error" in data and "tweets" not in data and "rules" not in data:
        message = data.get("message") or data.get("error")
        raise TwitterApiIoError(f"TwitterAPI.io provider error: {message}")


def _tweet_excerpt(tweet: dict[str, Any] | None) -> dict[str, Any] | None:
    if not tweet:
        return None
    author = _dict_or_none(tweet.get("author")) or {}
    return {
        "tweet_id": _str_or_none(_first(tweet, "id", "tweet_id", "tweetId")),
        "author_username": _str_or_none(_first(author, "userName", "username", "screen_name")),
        "text": _str_or_none(tweet.get("text")),
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _as_aware_utc(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value)
    try:
        return _as_aware_utc(parsedate_to_datetime(text))
    except (TypeError, ValueError):
        pass
    try:
        return _as_aware_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_body(value: str, secret: str = "") -> str:
    return _mask_secret(value[:300], secret)


def _mask_secret(value: str, secret: str) -> str:
    if not secret:
        return value
    return value.replace(secret, "***")
