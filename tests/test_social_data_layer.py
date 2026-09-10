from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import KOLTokenFirstMention, SocialEvent, SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.gmgn_client import GmgnTokenOverview
from app.services.social_event_service import (
    AUTHOR_DEV_X,
    AUTHOR_KOL,
    AUTHOR_PROJECT_X,
    INGESTION_BACKFILL,
    INGESTION_DISCOVERY,
    SocialEventService,
)
from app.services.social_identity_service import (
    DEV_WALLET,
    DEV_X,
    HIGH,
    MEDIUM,
    PROJECT_X,
    TELEGRAM,
    WEBSITE,
    IdentityInput,
    SocialIdentityService,
    normalize_username,
)
from app.services.social_kol_service import SOURCE_BOOTSTRAP_SEED, SOURCE_TRUSTED_EXTERNAL_SEED, MANUAL_INCLUDE, SocialKOLService
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet, PROVIDER
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
TOKEN_2 = "0x39dbed3a2bd333467115de45665cc57f813c4571"
DEV = "0xbfcc000000000000000000000000000000000001"


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/social.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = utc_now()
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        watch = TokenWatchState(
            wallet_id=wallet.id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="ROBBIE",
            active=True,
            started_at=now - timedelta(hours=1),
            last_seen_at=now,
        )
        session.add(watch)
        session.flush()
        wallet_id = wallet.id
        watch_id = watch.id
    return session_factory, wallet_id, watch_id


def overview(
    *,
    token: str = TOKEN,
    symbol: str = "ROBBIE",
    twitter: str | None = "@RobbieOnRH",
    creator: str | None = DEV,
    website: str | None = "https://robbie.example",
    telegram: str | None = "https://t.me/robbie",
) -> GmgnTokenOverview:
    return GmgnTokenOverview(
        chain="robinhood",
        token_address=token,
        symbol=symbol,
        name=symbol,
        price_usd=Decimal("1"),
        market_cap_usd=Decimal("1000000"),
        fdv_usd=None,
        liquidity_usd=Decimal("100000"),
        holder_count=500,
        top10_holder_rate=None,
        smart_money_count=None,
        kol_count=None,
        creator_address=creator,
        created_at=None,
        twitter=twitter,
        telegram=telegram,
        website=website,
        raw={},
    )


def watched_token() -> object:
    return type(
        "WatchedTokenLike",
        (),
        {"chain": "robinhood", "token_address": TOKEN, "symbol": "ROBBIE"},
    )()


def tweet(
    *,
    tweet_id: str = "tweet-1",
    author_id: str | None = "author-1",
    author_username: str | None = "kol_one",
    text: str = f"Watching {TOKEN}",
    matches: list[TokenMatch] | None = None,
    created_at=None,
    author_followers: int | None = 1000,
) -> NormalizedTweet:
    created = created_at or datetime.now(UTC)
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=tweet_id,
        author_id=author_id,
        author_username=author_username,
        author_name=author_username,
        author_followers=author_followers,
        text=text,
        created_at=created,
        detected_at=created + timedelta(seconds=5),
        is_reply=False,
        in_reply_to_id=None,
        in_reply_to_username=None,
        conversation_id=tweet_id,
        is_quote=False,
        quoted_tweet_id=None,
        quoted_tweet=None,
        is_retweet=False,
        retweeted_tweet_id=None,
        retweeted_tweet=None,
        like_count=10,
        retweet_count=2,
        reply_count=1,
        quote_count=0,
        view_count=100,
        token_matches=matches if matches is not None else [TokenMatch("direct_ca", TOKEN, TOKEN)],
        raw={"not_saved": True},
    )


def identity_input(
    *,
    identity_type: str = DEV_X,
    value: str = "DevUser",
    normalized_value: str = "devuser",
    confidence: str = MEDIUM,
    source: str = "launchpad",
) -> IdentityInput:
    return IdentityInput(
        chain="robinhood",
        token_address=TOKEN,
        symbol="ROBBIE",
        identity_type=identity_type,
        value=value,
        normalized_value=normalized_value,
        source=source,
        source_field="creator.twitter_username",
        confidence=confidence,
        evidence={"source": source},
    )


def all_identities(session_factory) -> list[SocialIdentity]:
    with session_scope(session_factory) as session:
        rows = list(session.scalars(select(SocialIdentity).order_by(SocialIdentity.id.asc())))
        for row in rows:
            session.expunge(row)
        return rows


def all_events(session_factory) -> list[SocialEvent]:
    with session_scope(session_factory) as session:
        rows = list(session.scalars(select(SocialEvent).order_by(SocialEvent.id.asc())))
        for row in rows:
            session.expunge(row)
        return rows


def test_social_identity_sync_from_token_overview_upserts_project_dev_wallet_links(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    result = service.sync_from_token_overview(watched_token(), overview(twitter="https://x.com/RobbieOnRH/"))

    identities = all_identities(session_factory)
    assert result.created == 4
    assert {(row.identity_type, row.normalized_value) for row in identities} == {
        (PROJECT_X, "robbieonrh"),
        (DEV_WALLET, DEV),
        (WEBSITE, "https://robbie.example"),
        (TELEGRAM, "https://t.me/robbie"),
    }
    project = next(row for row in identities if row.identity_type == PROJECT_X)
    assert project.value == "RobbieOnRH"
    assert project.source == "gmgn_token_info"
    assert project.source_field == "link.twitter_username"
    assert project.confidence == HIGH


def test_project_x_does_not_become_dev_x_and_missing_identity_is_safe(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    service.sync_from_token_overview(watched_token(), overview(twitter="@project", creator=None, website=None, telegram=None))
    service.sync_from_token_overview(watched_token(), overview(twitter=None, creator=None, website=None, telegram=None))

    identities = all_identities(session_factory)
    assert [row.identity_type for row in identities] == [PROJECT_X]
    assert not [row for row in identities if row.identity_type == DEV_X]


def test_duplicate_identity_merges_evidence_and_higher_confidence(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    service.upsert_identity(identity_input(confidence=MEDIUM, source="launchpad"))
    service.upsert_identity(identity_input(confidence=HIGH, source="gmgn_wallet_mapping"))

    identities = all_identities(session_factory)
    assert len(identities) == 1
    assert identities[0].confidence == HIGH
    evidence = json.loads(identities[0].evidence_json)
    assert [item["source"] for item in evidence["sources"]] == ["launchpad", "gmgn_wallet_mapping"]


def test_unchanged_identity_does_not_report_updated(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    created = service.sync_from_token_overview(watched_token(), overview(twitter="@same", creator=None, website=None, telegram=None))
    unchanged = service.sync_from_token_overview(watched_token(), overview(twitter="@same", creator=None, website=None, telegram=None))

    assert created.created == 1
    assert unchanged.updated == 0
    assert len(all_identities(session_factory)) == 1


def test_missing_identity_does_not_deactivate_existing_project_x(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    service.sync_from_token_overview(watched_token(), overview(twitter="@project", creator=None, website=None, telegram=None))
    result = service.sync_from_token_overview(watched_token(), overview(twitter=None, creator=None, website=None, telegram=None))

    identities = all_identities(session_factory)
    assert result.deactivated == 0
    assert identities[0].is_active is True
    assert identities[0].normalized_value == "project"


def test_identity_change_marks_old_inactive_and_keeps_history(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialIdentityService(session_factory)

    service.sync_from_token_overview(watched_token(), overview(twitter="@oldname", creator=None, website=None, telegram=None))
    service.sync_from_token_overview(watched_token(), overview(twitter="@newname", creator=None, website=None, telegram=None))

    identities = all_identities(session_factory)
    old = next(row for row in identities if row.normalized_value == "oldname")
    new = next(row for row in identities if row.normalized_value == "newname")
    assert old.is_active is False
    assert old.valid_to is not None
    assert new.is_active is True
    assert new.valid_to is None


def test_username_normalization() -> None:
    assert normalize_username("@RobbieOnRH") == "RobbieOnRH"
    assert normalize_username("https://twitter.com/RobbieOnRH?x=1") == "RobbieOnRH"
    assert normalize_username("https://x.com/RobbieOnRH/status/1") == "RobbieOnRH"


def test_social_event_relevant_current_watch_token_creates_event_and_first_mention(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(tweet(), known_kol_usernames={"kol_one"})

    assert len(created) == 1
    event = all_events(session_factory)[0]
    assert event.wallet_id == wallet_id
    assert event.watch_state_id == watch_id
    assert event.author_type == AUTHOR_KOL
    assert event.match_type == "direct_ca"
    assert event.tweet_url == "https://x.com/kol_one/status/tweet-1"
    assert not hasattr(event, "raw")
    with session_scope(session_factory) as session:
        first = session.scalar(select(KOLTokenFirstMention))
    assert first is not None
    assert first.first_tweet_id == "tweet-1"


def test_unrelated_tweet_inactive_watch_and_event_before_watch_do_not_create_realtime_event(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)

    unmatched = service.create_event_if_relevant(
        tweet(tweet_id="unmatched", matches=[TokenMatch("direct_cashtag", "NOPE", "$NOPE")]),
        known_kol_usernames={"kol_one"},
    )
    old = service.create_event_if_relevant(
        tweet(tweet_id="old", created_at=datetime.now(UTC) - timedelta(hours=2)),
        known_kol_usernames={"kol_one"},
    )
    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState))
        watch.active = False
    inactive = service.create_event_if_relevant(tweet(tweet_id="inactive"), known_kol_usernames={"kol_one"})

    assert unmatched == []
    assert old == []
    assert inactive == []
    assert all_events(session_factory) == []


def test_same_tweet_multi_match_and_provider_id_dedupe(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)
    matches = [
        TokenMatch("direct_cashtag", "ROBBIE", "$ROBBIE"),
        TokenMatch("direct_ca", TOKEN, TOKEN),
        TokenMatch("quote_ca", TOKEN, TOKEN),
    ]
    same_tweet = tweet(tweet_id="multi", matches=matches)

    first = service.create_event_if_relevant(same_tweet, known_kol_usernames={"kol_one"})
    second = service.create_event_if_relevant(same_tweet, known_kol_usernames={"kol_one"})

    assert len(first) == 1
    assert second == []
    events = all_events(session_factory)
    assert len(events) == 1
    assert events[0].match_type == "direct_ca"


def test_cashtag_and_quote_matches_create_relevant_events(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)

    direct_cashtag = service.create_event_if_relevant(
        tweet(tweet_id="cashtag", matches=[TokenMatch("direct_cashtag", "ROBBIE", "$ROBBIE")]),
        known_kol_usernames={"kol_one"},
    )
    quote_ca = service.create_event_if_relevant(
        tweet(tweet_id="quote-ca", matches=[TokenMatch("quote_ca", TOKEN, TOKEN)]),
        known_kol_usernames={"kol_one"},
    )
    quote_cashtag = service.create_event_if_relevant(
        tweet(tweet_id="quote-cashtag", matches=[TokenMatch("quote_cashtag", "ROBBIE", "$ROBBIE")]),
        known_kol_usernames={"kol_one"},
    )

    assert len(direct_cashtag) == 1
    assert len(quote_ca) == 1
    assert len(quote_cashtag) == 1
    assert [event.match_type for event in all_events(session_factory)] == [
        "direct_cashtag",
        "quote_ca",
        "quote_cashtag",
    ]


def test_duplicate_active_symbol_makes_cashtag_ambiguous_but_ca_wins(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    service = SocialEventService(session_factory)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN_2,
                symbol="ROBBIE",
                active=True,
                started_at=now - timedelta(minutes=30),
                last_seen_at=now,
            )
        )

    ambiguous = service.create_event_if_relevant(
        tweet(tweet_id="ambiguous", matches=[TokenMatch("direct_cashtag", "ROBBIE", "$ROBBIE")]),
        known_kol_usernames={"kol_one"},
    )
    exact = service.create_event_if_relevant(
        tweet(
            tweet_id="ca-wins",
            matches=[
                TokenMatch("direct_cashtag", "ROBBIE", "$ROBBIE"),
                TokenMatch("direct_ca", TOKEN, TOKEN),
            ],
        ),
        known_kol_usernames={"kol_one"},
    )

    assert ambiguous == []
    assert len(exact) == 1
    events = all_events(session_factory)
    assert len(events) == 1
    assert events[0].token_address == TOKEN
    assert events[0].match_type == "direct_ca"


def test_identity_account_project_and_dev_author_types(ctx) -> None:
    session_factory, _, _ = ctx
    identities = SocialIdentityService(session_factory)
    events = SocialEventService(session_factory)
    identities.upsert_identity(identity_input(identity_type=PROJECT_X, value="ProjectUser", normalized_value="projectuser", confidence=HIGH))
    identities.upsert_identity(identity_input(identity_type=DEV_X, value="DevUser", normalized_value="devuser", confidence=HIGH))

    project_created = events.create_event_if_relevant(
        tweet(tweet_id="project", author_username="ProjectUser", matches=[]),
    )
    dev_created = events.create_event_if_relevant(
        tweet(tweet_id="dev", author_username="devuser", matches=[]),
    )

    assert len(project_created) == 1
    assert len(dev_created) == 1
    stored = all_events(session_factory)
    assert [event.author_type for event in stored] == [AUTHOR_PROJECT_X, AUTHOR_DEV_X]
    assert [event.match_type for event in stored] == ["identity_account", "identity_account"]


def test_kol_author_id_and_username_fallback(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)

    by_id = service.create_event_if_relevant(tweet(tweet_id="by-id", author_id="42", author_username="unknown"), known_kol_author_ids={"42"})
    by_username = service.create_event_if_relevant(tweet(tweet_id="by-name", author_id=None, author_username="Kol_Two"), known_kol_usernames={"kol_two"})

    assert len(by_id) == 1
    assert len(by_username) == 1
    with session_scope(session_factory) as session:
        first_mentions = list(session.scalars(select(KOLTokenFirstMention).order_by(KOLTokenFirstMention.id.asc())))
    assert [row.author_key for row in first_mentions] == ["42", "kol_two"]


def test_same_username_different_author_id_is_not_kol(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="456",
        username="abc",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="conflict", author_id="123", author_username="abc", author_followers=3000),
        known_kol_usernames={"abc"},
    )

    assert created == []
    assert all_events(session_factory) == []


def test_bootstrap_username_only_profile_can_still_match_kol(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id=None,
        username="boot",
        followers=None,
        source=SOURCE_BOOTSTRAP_SEED,
    )
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="bootstrap", author_id="123", author_username="boot", author_followers=3000),
        known_kol_usernames={"boot"},
    )

    assert len(created) == 1
    assert all_events(session_factory)[0].author_type == AUTHOR_KOL


def test_discovery_event_is_limited_to_current_watch_session(ctx) -> None:
    session_factory, _, _ = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(2, 200, "0x1111111111111111111111111111111111111111", "robinhood")
        session.add(
            TokenWatchState(
                wallet_id=wallet.id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                active=True,
                started_at=now - timedelta(minutes=10),
                last_seen_at=now,
            )
        )
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="between-sessions", created_at=now - timedelta(minutes=30)),
        ingestion_type=INGESTION_DISCOVERY,
        known_kol_usernames={"kol_one"},
    )

    assert len(created) == 1
    assert created[0].ingestion_type == INGESTION_DISCOVERY
    assert created[0].posted_at < now - timedelta(minutes=10)


def test_backfill_can_write_event_before_watch_started(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="backfill-old", created_at=datetime.now(UTC) - timedelta(hours=2)),
        ingestion_type=INGESTION_BACKFILL,
        known_kol_usernames={"kol_one"},
    )

    assert len(created) == 1
    assert all_events(session_factory)[0].ingestion_type == INGESTION_BACKFILL


def test_low_follower_known_kol_tweet_does_not_create_kol_event(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="low-followers", author_username="small_kol", author_followers=999),
        known_kol_usernames={"small_kol"},
    )

    assert created == []
    assert all_events(session_factory) == []


def test_manual_include_allows_low_follower_kol_event(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).set_manual_override("small_kol", MANUAL_INCLUDE)
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="manual-low", author_username="small_kol", author_followers=100),
        known_kol_usernames={"small_kol"},
    )

    assert len(created) == 1
    assert all_events(session_factory)[0].author_type == AUTHOR_KOL


def test_project_identity_account_is_not_follower_gated(ctx) -> None:
    session_factory, _, _ = ctx
    SocialIdentityService(session_factory).upsert_identity(
        identity_input(identity_type=PROJECT_X, value="TinyProject", normalized_value="tinyproject", confidence=HIGH)
    )
    service = SocialEventService(session_factory)

    created = service.create_event_if_relevant(
        tweet(tweet_id="tiny-project", author_username="TinyProject", author_followers=10, matches=[]),
        known_kol_usernames={"tinyproject"},
    )

    assert len(created) == 1
    assert all_events(session_factory)[0].author_type == AUTHOR_PROJECT_X


def test_first_mention_repeated_does_not_overwrite_and_earlier_backfill_updates(ctx) -> None:
    session_factory, _, _ = ctx
    service = SocialEventService(session_factory)
    now = datetime.now(UTC)

    service.create_event_if_relevant(tweet(tweet_id="first", created_at=now, author_id="a1"), known_kol_usernames={"kol_one"})
    service.create_event_if_relevant(tweet(tweet_id="later", created_at=now + timedelta(minutes=1), author_id="a1"), known_kol_usernames={"kol_one"})
    service.create_event_if_relevant(
        tweet(tweet_id="earlier", created_at=now - timedelta(minutes=5), author_id="a1"),
        ingestion_type=INGESTION_BACKFILL,
        known_kol_usernames={"kol_one"},
    )

    with session_scope(session_factory) as session:
        first = session.scalar(select(KOLTokenFirstMention).where(KOLTokenFirstMention.author_key == "a1"))
    assert first.first_tweet_id == "earlier"


def test_project_and_dev_events_do_not_write_first_mention(ctx) -> None:
    session_factory, _, _ = ctx
    identities = SocialIdentityService(session_factory)
    service = SocialEventService(session_factory)
    identities.upsert_identity(identity_input(identity_type=PROJECT_X, value="ProjectUser", normalized_value="projectuser", confidence=HIGH))
    identities.upsert_identity(identity_input(identity_type=DEV_X, value="DevUser", normalized_value="devuser", confidence=HIGH))

    service.create_event_if_relevant(tweet(tweet_id="project", author_username="ProjectUser", matches=[]))
    service.create_event_if_relevant(tweet(tweet_id="dev", author_username="DevUser", matches=[]))

    with session_scope(session_factory) as session:
        assert session.scalar(select(KOLTokenFirstMention)) is None


def test_social_facts_counts_unique_kols_dev_posts_window_and_latest_dev_url(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    identities = SocialIdentityService(session_factory)
    service = SocialEventService(session_factory)
    identities.upsert_identity(identity_input(identity_type=PROJECT_X, value="ProjectUser", normalized_value="projectuser", confidence=HIGH))
    identities.upsert_identity(identity_input(identity_type=DEV_X, value="DevUser", normalized_value="devuser", confidence=HIGH))
    now = datetime.now(UTC)

    service.create_event_if_relevant(tweet(tweet_id="k1-a", author_id="k1", author_username="kol_one", created_at=now), known_kol_usernames={"kol_one"})
    service.create_event_if_relevant(tweet(tweet_id="k1-b", author_id="k1", author_username="kol_one", created_at=now + timedelta(seconds=10)), known_kol_usernames={"kol_one"})
    service.create_event_if_relevant(tweet(tweet_id="k2", author_id="k2", author_username="kol_two", created_at=now + timedelta(seconds=20)), known_kol_usernames={"kol_two"})
    service.create_event_if_relevant(tweet(tweet_id="project", author_username="ProjectUser", matches=[], created_at=now + timedelta(seconds=30)))
    service.create_event_if_relevant(tweet(tweet_id="dev", author_username="DevUser", matches=[], created_at=now + timedelta(seconds=40)))
    service.create_event_if_relevant(
        tweet(tweet_id="outside", author_id="k3", author_username="kol_three", created_at=now - timedelta(minutes=20)),
        known_kol_usernames={"kol_three"},
    )

    facts = service.build_social_facts(wallet_id, TOKEN, 15).to_dict()

    assert facts["unique_kols"] == 2
    assert facts["kol_posts"] == 3
    assert facts["new_kols"] == 2
    assert facts["project_posts"] == 1
    assert facts["dev_x_posts"] == 1
    assert facts["dev_posts"] == 2
    assert facts["latest_dev_tweet_url"] == "https://x.com/DevUser/status/dev"
    assert facts["latest_event_at"] is not None


def test_social_facts_watch_session_isolation(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    service = SocialEventService(session_factory)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialEvent(
                wallet_id=wallet_id,
                watch_state_id=watch_id + 100,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                provider=PROVIDER,
                provider_event_id="old-session",
                author_id="k-old",
                author_username="old",
                author_type=AUTHOR_KOL,
                posted_at=now,
                received_at=now,
                ingestion_type="stream",
                post_type="original",
                text="old",
                match_type="direct_ca",
                matched_value=TOKEN,
            )
        )

    facts = service.build_social_facts(wallet_id, TOKEN, 15)

    assert facts.unique_kols == 0
    assert facts.kol_posts == 0


def test_recent_events_uses_current_active_watch_session(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    service = SocialEventService(session_factory)
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialEvent(
                wallet_id=wallet_id,
                watch_state_id=watch_id + 100,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                provider=PROVIDER,
                provider_event_id="old-session",
                author_id="k-old",
                author_username="old",
                author_type=AUTHOR_KOL,
                posted_at=now,
                received_at=now,
                ingestion_type="stream",
                post_type="original",
                text="old",
                match_type="direct_ca",
                matched_value=TOKEN,
            )
        )

    assert service.recent_events(wallet_id, TOKEN, 15) == []

    service.create_event_if_relevant(tweet(tweet_id="current", created_at=now), known_kol_usernames={"kol_one"})
    events = service.recent_events(wallet_id, TOKEN, 15)

    assert [event.provider_event_id for event in events] == ["current"]


def test_social_event_retention_deletes_only_old_events(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                identity_type=PROJECT_X,
                value="ProjectUser",
                normalized_value="projectuser",
                source="test",
                source_field="test",
                confidence=HIGH,
                is_active=True,
                first_seen_at=now,
                last_verified_at=now,
                valid_from=now,
            )
        )
        session.add(
            KOLTokenFirstMention(
                chain="robinhood",
                token_address=TOKEN,
                author_id="k1",
                author_username="kol_one",
                author_key="k1",
                first_mention_at=now - timedelta(days=40),
                first_tweet_id="old",
                first_match_type="direct_ca",
                observation_scope="wallet_agent_observed",
            )
        )
        for tweet_id, posted_at in (("old", now - timedelta(days=31)), ("new", now - timedelta(days=1))):
            session.add(
                SocialEvent(
                    wallet_id=wallet_id,
                    watch_state_id=watch_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    symbol="ROBBIE",
                    provider=PROVIDER,
                    provider_event_id=tweet_id,
                    author_id="k1",
                    author_username="kol_one",
                    author_type=AUTHOR_KOL,
                    posted_at=posted_at,
                    received_at=posted_at,
                    ingestion_type="stream",
                    post_type="original",
                    text=tweet_id,
                    match_type="direct_ca",
                    matched_value=TOKEN,
                )
            )
    service = SocialEventService(session_factory)

    deleted = service.cleanup_old_events(now)

    assert deleted == 1
    with session_scope(session_factory) as session:
        assert len(list(session.scalars(select(SocialEvent)))) == 1
        assert len(list(session.scalars(select(SocialIdentity)))) == 1
        assert len(list(session.scalars(select(KOLTokenFirstMention)))) == 1


def test_social_tables_exist_after_init_db(ctx) -> None:
    session_factory, _, _ = ctx
    with session_scope(session_factory) as session:
        assert session.execute(select(SocialIdentity)).all() == []
        assert session.execute(select(SocialEvent)).all() == []
        assert session.execute(select(KOLTokenFirstMention)).all() == []
        assert session.execute(select(SocialKOLProfile)).all() == []


def test_identity_sync_reuses_overview_without_gmgn_client_calls(ctx) -> None:
    class GmgnShouldNotBeCalled:
        calls = 0

        def get_token_overview(self):
            self.calls += 1
            raise AssertionError("extra_gmgn_call")

    session_factory, _, _ = ctx
    gmgn = GmgnShouldNotBeCalled()

    SocialIdentityService(session_factory).sync_from_token_overview(watched_token(), overview())

    assert gmgn.calls == 0
