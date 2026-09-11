from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from collections.abc import Callable
from typing import Any, Protocol

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import KOLTokenFirstMention, SocialEvent, SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.social_identity_service import DEV_X, PROJECT_X, normalize_username
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet
from app.utils.address import normalize_evm_address
from app.utils.time import utc_now

logger = logging.getLogger(__name__)

AUTHOR_KOL = "kol"
AUTHOR_PROJECT_X = "project_x"
AUTHOR_DEV_X = "dev_x"

INGESTION_STREAM = "stream"
INGESTION_BACKFILL = "backfill"
INGESTION_DISCOVERY = "discovery"

OBSERVATION_SCOPE = "wallet_agent_observed"
SOCIAL_EVENT_RETENTION_DAYS = 30

MATCH_PRIORITY = {
    "direct_ca": 0,
    "quote_ca": 1,
    "direct_cashtag": 2,
    "quote_cashtag": 3,
    "identity_account": 4,
}


@dataclass(frozen=True)
class SocialFacts:
    window_minutes: int
    unique_kols: int
    kol_posts: int
    dev_posts: int
    project_posts: int
    dev_x_posts: int
    new_kols: int
    latest_dev_tweet_url: str | None
    latest_event_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_minutes": self.window_minutes,
            "unique_kols": self.unique_kols,
            "kol_posts": self.kol_posts,
            "dev_posts": self.dev_posts,
            "project_posts": self.project_posts,
            "dev_x_posts": self.dev_x_posts,
            "new_kols": self.new_kols,
            "latest_dev_tweet_url": self.latest_dev_tweet_url,
            "latest_event_at": self.latest_event_at.isoformat() if self.latest_event_at else None,
        }


class SocialMemoryProcessor(Protocol):
    def process_event(self, event: SocialEvent) -> object | None:
        ...


class SocialEventService:
    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        memory_processor: SocialMemoryProcessor | None = None,
        social_update_callback: Callable[[SocialEvent, object | None], None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.memory_processor = memory_processor
        self.social_update_callback = social_update_callback

    def create_event_if_relevant(
        self,
        tweet: NormalizedTweet,
        *,
        ingestion_type: str = INGESTION_STREAM,
        rule_id: str | None = None,
        rule_tag: str | None = None,
        known_kol_usernames: set[str] | None = None,
        known_kol_author_ids: set[str] | None = None,
    ) -> list[SocialEvent]:
        if not tweet.tweet_id or not tweet.created_at:
            return []
        posted_at = _naive_utc(tweet.created_at)
        received_at = _naive_utc(tweet.detected_at)
        known_kol_usernames = {_normalize_username_key(value) for value in (known_kol_usernames or set())}
        known_kol_author_ids = {str(value) for value in (known_kol_author_ids or set())}
        created: list[SocialEvent] = []
        with session_scope(self.session_factory) as session:
            watches = list(
                session.scalars(
                    select(TokenWatchState).where(TokenWatchState.active.is_(True))
                )
            )
            if not watches:
                return []
            identities = self._active_x_identities(session)
            symbol_token_counts = _active_symbol_token_counts(watches)
            for watch in watches:
                if ingestion_type in {INGESTION_STREAM, INGESTION_DISCOVERY} and posted_at < watch.started_at:
                    continue
                match = _best_match_for_watch(tweet.token_matches, watch, symbol_token_counts)
                identity_author_type = _identity_author_type(tweet.author_username, identities, watch)
                if not match and identity_author_type:
                    match = TokenMatch("identity_account", _normalize_username_key(tweet.author_username), tweet.author_username or "")
                if not match:
                    continue
                author_type = identity_author_type or self._kol_author_type(
                    session,
                    tweet,
                    known_kol_usernames,
                    known_kol_author_ids,
                )
                if not author_type:
                    continue
                existing = session.scalar(
                    select(SocialEvent).where(
                        SocialEvent.provider == tweet.provider,
                        SocialEvent.provider_event_id == tweet.tweet_id,
                        SocialEvent.token_address == watch.token_address,
                        SocialEvent.watch_state_id == watch.id,
                    )
                )
                if existing:
                    continue
                event = SocialEvent(
                    wallet_id=watch.wallet_id,
                    watch_state_id=watch.id,
                    chain=watch.chain,
                    token_address=watch.token_address,
                    symbol=watch.symbol,
                    provider=tweet.provider,
                    provider_event_id=tweet.tweet_id,
                    author_id=tweet.author_id,
                    author_username=normalize_username(tweet.author_username) if tweet.author_username else None,
                    author_type=author_type,
                    posted_at=posted_at,
                    received_at=received_at,
                    ingestion_type=ingestion_type,
                    post_type=tweet.post_type,
                    text=tweet.text,
                    match_type=match.match_type,
                    matched_value=match.value,
                    tweet_url=_tweet_url(tweet.author_username, tweet.tweet_id),
                    like_count=tweet.like_count,
                    retweet_count=tweet.retweet_count,
                    reply_count=tweet.reply_count,
                    quote_count=tweet.quote_count,
                    view_count=tweet.view_count,
                    rule_id=rule_id,
                    rule_tag=rule_tag,
                )
                try:
                    with session.begin_nested():
                        session.add(event)
                        session.flush()
                except IntegrityError:
                    continue
                if author_type == AUTHOR_KOL:
                    self.upsert_first_mention_if_earlier(session, event)
                session.expunge(event)
                created.append(event)
        self._process_social_updates(created)
        return created

    def _process_social_updates(self, events: list[SocialEvent]) -> None:
        for event in events:
            if event.author_type == AUTHOR_KOL:
                self._notify_social_update(event, None)
                continue
            if event.author_type not in {AUTHOR_PROJECT_X, AUTHOR_DEV_X} or self.memory_processor is None:
                continue
            memory = None
            try:
                memory = self.memory_processor.process_event(event)
            except Exception as exc:  # noqa: BLE001 - memory triage must not break event ingestion.
                logger.warning("Social memory processing failed event_id=%s: %s", event.id, str(exc)[:200])
                continue
            if memory is not None:
                self._notify_social_update(event, memory)

    def _notify_social_update(self, event: SocialEvent, memory: object | None) -> None:
        if self.social_update_callback is None:
            return
        try:
            self.social_update_callback(event, memory)
        except Exception as exc:  # noqa: BLE001 - social attention trigger must not break ingestion.
            logger.warning("Social update callback failed event_id=%s: %s", event.id, str(exc)[:200])

    def upsert_first_mention_if_earlier(self, session, event: SocialEvent) -> None:
        if event.author_type != AUTHOR_KOL:
            return
        author_key = _author_key(event.author_id, event.author_username)
        existing = session.scalar(
            select(KOLTokenFirstMention).where(
                KOLTokenFirstMention.chain == event.chain,
                KOLTokenFirstMention.token_address == event.token_address,
                KOLTokenFirstMention.author_key == author_key,
            )
        )
        if not existing:
            session.add(
                KOLTokenFirstMention(
                    chain=event.chain,
                    token_address=event.token_address,
                    author_id=event.author_id,
                    author_username=event.author_username,
                    author_key=author_key,
                    first_mention_at=event.posted_at,
                    first_tweet_id=event.provider_event_id,
                    first_match_type=event.match_type,
                    observation_scope=OBSERVATION_SCOPE,
                )
            )
            return
        if event.posted_at < existing.first_mention_at:
            existing.first_mention_at = event.posted_at
            existing.first_tweet_id = event.provider_event_id
            existing.first_match_type = event.match_type
            existing.author_id = event.author_id
            existing.author_username = event.author_username
            existing.updated_at = utc_now()

    def recent_events(self, wallet_id: int, token_address: str, minutes: int) -> list[SocialEvent]:
        cutoff = utc_now() - timedelta(minutes=minutes)
        token = normalize_evm_address(token_address)
        with session_scope(self.session_factory) as session:
            active_watch = session.scalar(
                select(TokenWatchState).where(
                    TokenWatchState.wallet_id == wallet_id,
                    TokenWatchState.token_address == token,
                    TokenWatchState.active.is_(True),
                )
            )
            if not active_watch:
                return []
            lower_bound = max(cutoff, active_watch.started_at)
            events = list(
                session.scalars(
                    select(SocialEvent)
                    .where(
                        SocialEvent.wallet_id == wallet_id,
                        SocialEvent.token_address == token,
                        SocialEvent.watch_state_id == active_watch.id,
                        SocialEvent.posted_at >= lower_bound,
                    )
                    .order_by(SocialEvent.posted_at.desc(), SocialEvent.id.desc())
                )
            )
            for event in events:
                session.expunge(event)
            return events

    def build_social_facts(self, wallet_id: int, token_address: str, window_minutes: int = 15) -> SocialFacts:
        token = normalize_evm_address(token_address)
        cutoff = utc_now() - timedelta(minutes=window_minutes)
        with session_scope(self.session_factory) as session:
            active_watch = session.scalar(
                select(TokenWatchState).where(
                    TokenWatchState.wallet_id == wallet_id,
                    TokenWatchState.token_address == token,
                    TokenWatchState.active.is_(True),
                )
            )
            if not active_watch:
                return SocialFacts(window_minutes, 0, 0, 0, 0, 0, 0, None, None)
            lower_bound = max(cutoff, active_watch.started_at)
            events = list(
                session.scalars(
                    select(SocialEvent)
                    .where(
                        SocialEvent.wallet_id == wallet_id,
                        SocialEvent.token_address == token,
                        SocialEvent.watch_state_id == active_watch.id,
                        SocialEvent.posted_at >= lower_bound,
                    )
                    .order_by(SocialEvent.posted_at.asc(), SocialEvent.id.asc())
                )
            )
            kol_author_keys = {
                _author_key(event.author_id, event.author_username)
                for event in events
                if event.author_type == AUTHOR_KOL
            }
            new_kols = 0
            for author_key in kol_author_keys:
                first = session.scalar(
                    select(KOLTokenFirstMention).where(
                        KOLTokenFirstMention.chain == active_watch.chain,
                        KOLTokenFirstMention.token_address == token,
                        KOLTokenFirstMention.author_key == author_key,
                    )
                )
                if first and first.first_mention_at >= lower_bound:
                    new_kols += 1
            project_events = [event for event in events if event.author_type == AUTHOR_PROJECT_X]
            dev_x_events = [event for event in events if event.author_type == AUTHOR_DEV_X]
            dev_events = [*project_events, *dev_x_events]
            latest_dev = max(dev_events, key=lambda event: event.posted_at, default=None)
            latest_event = max(events, key=lambda event: event.posted_at, default=None)
            return SocialFacts(
                window_minutes=window_minutes,
                unique_kols=len(kol_author_keys),
                kol_posts=sum(1 for event in events if event.author_type == AUTHOR_KOL),
                dev_posts=len(dev_events),
                project_posts=len(project_events),
                dev_x_posts=len(dev_x_events),
                new_kols=new_kols,
                latest_dev_tweet_url=latest_dev.tweet_url if latest_dev else None,
                latest_event_at=latest_event.posted_at if latest_event else None,
            )

    def cleanup_old_events(self, now: datetime | None = None) -> int:
        cutoff = (now or utc_now()) - timedelta(days=SOCIAL_EVENT_RETENTION_DAYS)
        with session_scope(self.session_factory) as session:
            result = session.execute(delete(SocialEvent).where(SocialEvent.posted_at < cutoff))
            return int(result.rowcount or 0)

    def _active_x_identities(self, session) -> dict[tuple[str, str, str], str]:
        identities: dict[tuple[str, str, str], str] = {}
        rows = session.scalars(
            select(SocialIdentity).where(
                SocialIdentity.identity_type.in_([PROJECT_X, DEV_X]),
                SocialIdentity.is_active.is_(True),
            )
        )
        for identity in rows:
            author_type = AUTHOR_PROJECT_X if identity.identity_type == PROJECT_X else AUTHOR_DEV_X
            identities[
                (
                    identity.chain,
                    identity.token_address,
                    _normalize_username_key(identity.normalized_value),
                )
            ] = author_type
        return identities

    def _kol_author_type(
        self,
        session,
        tweet: NormalizedTweet,
        known_kol_usernames: set[str],
        known_kol_author_ids: set[str],
    ) -> str | None:
        username_key = _normalize_username_key(tweet.author_username)
        matched = bool(tweet.author_id and tweet.author_id in known_kol_author_ids) or bool(
            username_key and username_key in known_kol_usernames
        )
        if not matched:
            return None
        profile, identity_conflict = _kol_profile_for_author(session, tweet.author_id, username_key)
        if identity_conflict:
            return None
        if tweet.author_followers is not None and profile:
            profile.follower_count = tweet.author_followers
            profile.last_seen_at = utc_now()
        if (
            tweet.author_followers is not None
            and tweet.author_followers < 1000
            and (not profile or profile.manual_override != "include")
        ):
            return None
        return AUTHOR_KOL


def _best_match_for_watch(
    matches: list[TokenMatch],
    watch: TokenWatchState,
    symbol_token_counts: dict[tuple[str, str], int],
) -> TokenMatch | None:
    token = normalize_evm_address(watch.token_address)
    symbol = (watch.symbol or "").upper()
    candidates: list[TokenMatch] = []
    for match in matches:
        if match.match_type.endswith("_ca") and match.value.lower() == token:
            candidates.append(match)
        elif (
            match.match_type.endswith("_cashtag")
            and symbol
            and match.value.upper() == symbol
            and symbol_token_counts.get((watch.chain, symbol), 0) == 1
        ):
            candidates.append(match)
    if not candidates:
        return None
    return min(candidates, key=lambda match: MATCH_PRIORITY.get(match.match_type, 99))


def _active_symbol_token_counts(watches: list[TokenWatchState]) -> dict[tuple[str, str], int]:
    tokens_by_symbol: dict[tuple[str, str], set[str]] = {}
    for watch in watches:
        symbol = (watch.symbol or "").upper()
        if not symbol:
            continue
        key = (watch.chain, symbol)
        tokens_by_symbol.setdefault(key, set()).add(normalize_evm_address(watch.token_address))
    return {key: len(tokens) for key, tokens in tokens_by_symbol.items()}


def _identity_author_type(
    author_username: str | None,
    identities: dict[tuple[str, str, str], str],
    watch: TokenWatchState,
) -> str | None:
    username = _normalize_username_key(author_username)
    if not username:
        return None
    return identities.get((watch.chain, watch.token_address, username))


def _kol_profile_for_author(
    session,
    author_id: str | None,
    username_key: str,
) -> tuple[SocialKOLProfile | None, bool]:
    if author_id:
        profile = session.scalar(select(SocialKOLProfile).where(SocialKOLProfile.x_author_id == str(author_id)))
        if profile:
            return profile, False
    if username_key:
        profile = session.scalar(
            select(SocialKOLProfile).where(SocialKOLProfile.normalized_username == username_key)
        )
        if profile and author_id and profile.x_author_id and profile.x_author_id != str(author_id):
            return None, True
        return profile, False
    return None, False


def _author_key(author_id: str | None, author_username: str | None) -> str:
    if author_id:
        return str(author_id)
    username = _normalize_username_key(author_username)
    if username:
        return username
    return "unknown"


def _normalize_username_key(value: str | None) -> str:
    normalized = normalize_username(value)
    return normalized.lower() if normalized else ""


def _tweet_url(author_username: str | None, tweet_id: str) -> str | None:
    username = normalize_username(author_username)
    if not username:
        return None
    return f"https://x.com/{username}/status/{tweet_id}"


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
