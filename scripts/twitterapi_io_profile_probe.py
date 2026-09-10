from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import inspect, or_, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import PriceSnapshot, SocialKOLProfile, TokenWatchState  # noqa: E402
from app.services.price_quality import PRICE_QUALITY_VALID  # noqa: E402
from app.services.twitterapi_io_client import (  # noqa: E402
    DEFAULT_BASE_URL,
    NormalizedTweet,
    TwitterApiIoClient,
    normalize_tweets_from_response,
)
from app.services.twitterapi_io_profile_provider import (  # noqa: E402
    normalize_recent_posts_response,
    normalize_user_profile_response,
)
from app.utils.address import normalize_evm_address  # noqa: E402
from app.utils.time import utc_now  # noqa: E402


@dataclass
class ProbeCounters:
    advanced_search_calls: int = 0
    profile_calls: int = 0
    recent_posts_calls: int = 0


@dataclass(frozen=True)
class DiscoveryTarget:
    watch_state_id: int
    wallet_id: int
    chain: str
    token_address: str
    symbol: str | None
    usd_value: Decimal


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="TwitterAPI.io profile/recent-tweets controlled probe")
    parser.add_argument("--username", default="lookonchain")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--skip-user-probe", action="store_true")
    parser.add_argument("--skip-token-discovery", action="store_true")
    parser.add_argument("--max-candidate-profiles", type=int, default=3)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    client = TwitterApiIoClient(api_key, base_url=args.base_url, timeout_seconds=args.timeout, max_retries=1)
    counters = ProbeCounters()

    if not args.skip_user_probe:
        print("=== TwitterAPI.io Profile Probe ===")
        profile_data = _call_user_info(client, args.username, counters)
        profile = normalize_user_profile_response(profile_data, fallback_username=args.username)
        print("Profile endpoint: /twitter/user/info")
        print("HTTP status: 200")
        print(f"Response shape: {_shape(profile_data)}")
        print(f"username: {profile.username}")
        print(f"author_id: {profile.author_id or 'MISSING'}")
        print(f"followers: {profile.followers if profile.followers is not None else 'MISSING'}")
        print(f"bio_present: {bool(profile.bio)}")
        print(f"bio_chars: {len(profile.bio or '')}")
        if args.debug and profile.bio:
            print(f"bio: {profile.bio}")

        print("\n=== TwitterAPI.io Recent Tweets Probe ===")
        recent_data = _call_user_last_tweets(client, args.username, counters)
        tweets = normalize_tweets_from_response(recent_data, detected_at=utc_now())
        samples = normalize_recent_posts_response(recent_data, limit=5)
        print("Recent endpoint: /twitter/user/last_tweets")
        print("HTTP status: 200")
        print(f"Response shape: {_shape(recent_data)}")
        print(f"Tweet container: {_tweet_container_path(recent_data)}")
        print(f"tweets_total: {len(tweets)}")
        print(f"usable_recent_posts: {len(samples)}")
        for tweet in tweets[:5]:
            print(
                "tweet "
                f"id={tweet.tweet_id} type={tweet.post_type} "
                f"created_at={tweet.created_at.isoformat() if tweet.created_at else 'MISSING'} "
                f"text_chars={len(tweet.text or '')}"
            )

    if not args.skip_token_discovery:
        _run_token_discovery_probe(client, counters, max_candidate_profiles=max(0, args.max_candidate_profiles))

    print("\n=== API Call Counters ===")
    print(f"advanced_search_calls={counters.advanced_search_calls}")
    print(f"profile_calls={counters.profile_calls}")
    print(f"recent_posts_calls={counters.recent_posts_calls}")
    print("DeepSeek calls=0")
    print("DB writes=0")


def _run_token_discovery_probe(
    client: TwitterApiIoClient,
    counters: ProbeCounters,
    *,
    max_candidate_profiles: int,
) -> None:
    settings = load_settings()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    target = _first_discovery_target(session_factory, min_usd_value=Decimal(str(settings.price_monitor_min_usd_value)))
    print("\n=== Controlled CA Discovery Probe ===")
    if target is None:
        print("No active watched token with latest VALID usd_value >= min threshold; skipped.")
        return
    print(f"Token: {target.symbol or 'UNKNOWN'}")
    print(f"CA: {target.token_address}")
    data = _call_advanced_search(client, target.token_address, counters)
    tweets = normalize_tweets_from_response(data, detected_at=utc_now())
    ca_matches = [
        tweet
        for tweet in tweets
        if any(
            match.match_type in {"direct_ca", "quote_ca"}
            and normalize_evm_address(match.value) == target.token_address
            for match in tweet.token_matches
        )
    ]
    authors: dict[tuple[str | None, str | None], NormalizedTweet] = {}
    followers_missing = 0
    followers_low = 0
    followers_high = 0
    for tweet in ca_matches:
        key = (tweet.author_id, (tweet.author_username or "").lower() or None)
        authors.setdefault(key, tweet)
        if tweet.author_followers is None:
            followers_missing += 1
        elif tweet.author_followers < 1000:
            followers_low += 1
        else:
            followers_high += 1
    print(f"returned_tweets={len(tweets)}")
    print(f"exact_ca_matches={len(ca_matches)}")
    print(f"unique_authors={len(authors)}")
    print(f"followers_missing={followers_missing}")
    print(f"followers_lt_1000={followers_low}")
    print(f"followers_gte_1000={followers_high}")

    sampled = 0
    for tweet in ca_matches:
        if sampled >= max_candidate_profiles:
            break
        if tweet.author_followers is None or tweet.author_followers < 1000 or not tweet.author_username:
            continue
        if _profile_exists(session_factory, tweet.author_id, tweet.author_username):
            continue
        profile_data = _call_user_info(client, tweet.author_username, counters)
        profile = normalize_user_profile_response(profile_data, fallback_username=tweet.author_username)
        recent_data = _call_user_last_tweets(client, tweet.author_username, counters)
        recent = normalize_recent_posts_response(recent_data, limit=5)
        counts = _post_type_counts(recent)
        print(
            "candidate_sample "
            f"@{profile.username} followers={profile.followers if profile.followers is not None else 'MISSING'} "
            f"bio_chars={len(profile.bio or '')} recent_posts={len(recent)} "
            f"original={counts.get('original', 0)} quote={counts.get('quote', 0)} reply={counts.get('reply', 0)}"
        )
        sampled += 1
    print(f"sampled_profile_authors={sampled}")


def _first_discovery_target(session_factory, *, min_usd_value: Decimal) -> DiscoveryTarget | None:
    with session_scope(session_factory) as session:
        watches = list(
            session.scalars(
                select(TokenWatchState)
                .where(TokenWatchState.active.is_(True))
                .order_by(TokenWatchState.id.asc())
            )
        )
        for watch in watches:
            latest_price = session.scalar(
                select(PriceSnapshot)
                .where(
                    PriceSnapshot.wallet_id == watch.wallet_id,
                    PriceSnapshot.chain == watch.chain,
                    PriceSnapshot.token_address == watch.token_address,
                    PriceSnapshot.observed_at >= watch.started_at,
                    or_(PriceSnapshot.quality_status.is_(None), PriceSnapshot.quality_status == PRICE_QUALITY_VALID),
                )
                .order_by(PriceSnapshot.observed_at.desc(), PriceSnapshot.id.desc())
            )
            if not latest_price or latest_price.usd_value is None:
                continue
            usd_value = Decimal(str(latest_price.usd_value))
            if usd_value < min_usd_value:
                continue
            return DiscoveryTarget(
                watch_state_id=watch.id,
                wallet_id=watch.wallet_id,
                chain=watch.chain,
                token_address=normalize_evm_address(watch.token_address),
                symbol=watch.symbol,
                usd_value=usd_value,
            )
    return None


def _profile_exists(session_factory, author_id: str | None, username: str | None) -> bool:
    normalized = (username or "").lstrip("@").lower()
    inspector = inspect(session_factory.kw["bind"])
    if "social_kol_profiles" not in set(inspector.get_table_names()):
        return False
    with session_scope(session_factory) as session:
        if author_id and session.scalar(select(SocialKOLProfile).where(SocialKOLProfile.x_author_id == str(author_id))):
            return True
        if normalized and session.scalar(select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == normalized)):
            return True
    return False


def _call_user_info(client: TwitterApiIoClient, username: str, counters: ProbeCounters) -> dict[str, Any]:
    counters.profile_calls += 1
    return client.get_user_info(username)


def _call_user_last_tweets(client: TwitterApiIoClient, username: str, counters: ProbeCounters) -> dict[str, Any]:
    counters.recent_posts_calls += 1
    return client.get_user_last_tweets(username)


def _call_advanced_search(client: TwitterApiIoClient, query: str, counters: ProbeCounters) -> dict[str, Any]:
    counters.advanced_search_calls += 1
    return client.advanced_search(query, query_type="Latest")


def _shape(value: Any) -> str:
    if isinstance(value, dict):
        parts = []
        for key, child in value.items():
            if isinstance(child, dict):
                parts.append(f"{key}{{{','.join(child.keys())}}}")
            elif isinstance(child, list):
                parts.append(f"{key}[{len(child)}]")
            else:
                parts.append(key)
        return "root{" + ", ".join(parts) + "}"
    return type(value).__name__


def _tweet_container_path(data: dict[str, Any]) -> str:
    if isinstance(data.get("tweets"), list):
        return "tweets"
    nested = data.get("data")
    if isinstance(nested, dict) and isinstance(nested.get("tweets"), list):
        return "data.tweets"
    if isinstance(nested, list):
        return "data"
    return "UNKNOWN"


def _post_type_counts(posts: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for post in posts:
        counts[post.post_type] = counts.get(post.post_type, 0) + 1
    return counts


if __name__ == "__main__":
    main()
