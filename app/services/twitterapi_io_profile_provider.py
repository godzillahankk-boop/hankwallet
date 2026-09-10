from __future__ import annotations

from typing import Any

from app.services.social_kol_service import SocialPostSample, SocialProfileSnapshot
from app.services.twitterapi_io_client import TwitterApiIoClient, normalize_tweets_from_response


class TwitterApiIoSocialProfileProvider:
    def __init__(self, client: TwitterApiIoClient) -> None:
        self.client = client

    def get_user_profile(self, username: str) -> SocialProfileSnapshot:
        data = self.client.get_user_info(username)
        return normalize_user_profile_response(data, fallback_username=username)

    def get_recent_posts(self, username: str, limit: int = 5) -> list[SocialPostSample]:
        data = self.client.get_user_last_tweets(username)
        return normalize_recent_posts_response(data, limit=limit)


def normalize_user_profile_response(data: dict[str, Any], *, fallback_username: str) -> SocialProfileSnapshot:
    user = _extract_user_object(data)
    username = _str_or_none(_first(user, "userName", "username", "screen_name")) or fallback_username
    return SocialProfileSnapshot(
        author_id=_str_or_none(_first(user, "id", "userId", "user_id", "rest_id")),
        username=username,
        bio=_str_or_none(_first(user, "description", "bio", "biography")),
        followers=_int_or_none(_first(user, "followers", "followers_count", "followersCount")),
    )


def normalize_recent_posts_response(data: dict[str, Any], *, limit: int = 5) -> list[SocialPostSample]:
    posts: list[SocialPostSample] = []
    for tweet in normalize_tweets_from_response(data):
        if tweet.post_type == "retweet":
            continue
        if not tweet.text:
            continue
        posts.append(SocialPostSample(tweet.tweet_id, tweet.text, tweet.post_type))
        if len(posts) >= limit:
            break
    return posts


def _extract_user_object(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    for key in ("user", "data"):
        nested = data.get(key)
        if isinstance(nested, dict):
            if isinstance(nested.get("user"), dict):
                return nested["user"]
            return nested
    return data


def _first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


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
