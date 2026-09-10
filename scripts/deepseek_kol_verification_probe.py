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
from sqlalchemy import inspect, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import SocialIdentity, SocialKOLProfile  # noqa: E402
from app.services.social_identity_service import DEV_X, PROJECT_X, normalize_username  # noqa: E402
from app.services.social_kol_service import (  # noqa: E402
    DeepSeekSocialKOLClassifier,
    KOLVerificationInput,
    MIN_KOL_FOLLOWERS,
)
from app.services.twitterapi_io_client import (  # noqa: E402
    DEFAULT_BASE_URL,
    NormalizedTweet,
    TwitterApiIoClient,
    normalize_tweets_from_response,
)
from app.services.twitterapi_io_profile_provider import TwitterApiIoSocialProfileProvider  # noqa: E402
from app.utils.address import normalize_evm_address  # noqa: E402
from app.utils.time import utc_now  # noqa: E402

PONS_CA = "0x39dbed3a2bd333467115de45665cc57f813c4571"


@dataclass
class ProbeCounters:
    advanced_search_logical_calls: int = 0
    profile_logical_calls: int = 0
    recent_posts_logical_calls: int = 0
    classifier_calls: int = 0


@dataclass(frozen=True)
class Candidate:
    author_id: str | None
    username: str
    followers: int | None


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Controlled DeepSeek KOL verification probe")
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--token-ca", default=PONS_CA)
    parser.add_argument("--max-candidates", type=int, default=3)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    twitter_key = os.getenv("TWITTERAPI_IO_API_KEY", "")
    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not deepseek_key:
        print("DEEPSEEK_API_KEY missing; controlled DeepSeek probe skipped.")
        return

    twitter_client = TwitterApiIoClient(twitter_key, base_url=args.base_url, timeout_seconds=args.timeout, max_retries=1)
    provider = TwitterApiIoSocialProfileProvider(twitter_client)
    classifier = DeepSeekSocialKOLClassifier(
        api_key=deepseek_key,
        base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        model=os.getenv("SOCIAL_KOL_VERIFY_MODEL", "deepseek-chat"),
    )
    counters = ProbeCounters()

    candidates = _candidate_args(args.candidate)
    if not candidates:
        candidates = _discover_candidates(twitter_client, counters, args.token_ca, args.max_candidates)
    candidates = candidates[: max(0, args.max_candidates)]

    print("=== Controlled DeepSeek KOL Verification Probe ===")
    print(f"candidates_selected={len(candidates)}")
    qualified = rejected = uncertain = failed = 0
    for candidate in candidates:
        try:
            counters.profile_logical_calls += 1
            profile = provider.get_user_profile(candidate.username)
            followers = profile.followers if profile.followers is not None else candidate.followers
            if followers is not None and followers < MIN_KOL_FOLLOWERS:
                print(f"@{profile.username} followers={followers} skipped=followers_lt_1000")
                continue
            counters.recent_posts_logical_calls += 1
            posts = provider.get_recent_posts(profile.username, limit=5)
            recent_texts = [post.text for post in posts[:5]]
            counters.classifier_calls += 1
            result = classifier.classify(
                KOLVerificationInput(
                    username=profile.username,
                    bio=profile.bio,
                    recent_posts=recent_texts,
                )
            )
            if result.crypto_relevant is True and result.confidence in {"HIGH", "MEDIUM"}:
                qualified += 1
            elif result.crypto_relevant is False:
                rejected += 1
            else:
                uncertain += 1
            print(
                f"@{profile.username} followers={followers if followers is not None else 'MISSING'} "
                f"bio_chars={len(profile.bio or '')} recent_posts={len(recent_texts)} "
                f"crypto_relevant={result.crypto_relevant} confidence={result.confidence} "
                f"category={result.category} reason={result.reason[:500]}"
            )
            if args.debug:
                print(f"sample_tweet_ids={[post.post_id for post in posts[:5]]}")
        except Exception as exc:
            failed += 1
            print(f"@{candidate.username} failed={type(exc).__name__}: {str(exc)[:200]}")
            break

    print("\n=== Probe Counters ===")
    print(f"advanced_search_logical_calls={counters.advanced_search_logical_calls}")
    print(f"profile_logical_calls={counters.profile_logical_calls}")
    print(f"recent_posts_logical_calls={counters.recent_posts_logical_calls}")
    print(f"classifier_calls={counters.classifier_calls}")
    for key, value in twitter_client.stats_snapshot().items():
        print(f"{key}={value}")
    print(f"qualified={qualified}")
    print(f"rejected={rejected}")
    print(f"uncertain={uncertain}")
    print(f"failed={failed}")
    print("DB writes=0")


def _candidate_args(values: list[str]) -> list[Candidate]:
    candidates: list[Candidate] = []
    for value in values:
        username = normalize_username(value)
        if username:
            candidates.append(Candidate(None, username, None))
    return candidates


def _discover_candidates(
    client: TwitterApiIoClient,
    counters: ProbeCounters,
    token_ca: str,
    max_candidates: int,
) -> list[Candidate]:
    settings = load_settings()
    session_factory = make_session_factory(make_engine(settings.database_url))
    project_dev_usernames = _project_dev_usernames(session_factory, token_ca)
    known_kols = _known_kols(session_factory)
    counters.advanced_search_logical_calls += 1
    data = client.advanced_search(normalize_evm_address(token_ca), query_type="Latest")
    tweets = normalize_tweets_from_response(data, detected_at=utc_now())
    candidates: list[Candidate] = []
    seen: set[str] = set()
    for tweet in tweets:
        if len(candidates) >= max_candidates:
            break
        if tweet.post_type == "retweet" or not _has_ca_match(tweet, token_ca):
            continue
        username = normalize_username(tweet.author_username)
        if not username:
            continue
        key = username.lower()
        if key in seen or key in project_dev_usernames or key in known_kols:
            continue
        if tweet.author_followers is None or tweet.author_followers < MIN_KOL_FOLLOWERS:
            continue
        seen.add(key)
        candidates.append(Candidate(tweet.author_id, username, tweet.author_followers))
    return candidates


def _project_dev_usernames(session_factory, token_ca: str) -> set[str]:
    token = normalize_evm_address(token_ca)
    if not _table_exists(session_factory, "social_identities"):
        return set()
    with session_scope(session_factory) as session:
        rows = session.scalars(
            select(SocialIdentity).where(
                SocialIdentity.token_address == token,
                SocialIdentity.identity_type.in_([PROJECT_X, DEV_X]),
                SocialIdentity.is_active.is_(True),
            )
        )
        return {(normalize_username(row.normalized_value) or row.normalized_value).lower() for row in rows}


def _known_kols(session_factory) -> set[str]:
    if not _table_exists(session_factory, "social_kol_profiles"):
        return set()
    with session_scope(session_factory) as session:
        rows = session.scalars(select(SocialKOLProfile).where(SocialKOLProfile.status == "qualified"))
        return {row.normalized_username for row in rows}


def _has_ca_match(tweet: NormalizedTweet, token_ca: str) -> bool:
    token = normalize_evm_address(token_ca)
    return any(match.match_type in {"direct_ca", "quote_ca"} and normalize_evm_address(match.value) == token for match in tweet.token_matches)


def _table_exists(session_factory, table_name: str) -> bool:
    return table_name in set(inspect(session_factory.kw["bind"]).get_table_names())


if __name__ == "__main__":
    main()
