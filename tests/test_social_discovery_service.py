from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import KOLTokenFirstMention, PriceSnapshot, SocialDiscoveryCursor, SocialEvent, SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.social_discovery_service import (
    DISCOVERY_QUERY_CA,
    SocialDiscoveryService,
    build_incremental_discovery_query,
    build_token_discovery_queries,
)
from app.services.social_event_service import AUTHOR_KOL, INGESTION_DISCOVERY, INGESTION_STREAM, SocialEventService
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_kol_service import (
    HIGH as KOL_HIGH,
    SOURCE_SOCIAL_DISCOVERY,
    SOURCE_TRUSTED_EXTERNAL_SEED,
    STATUS_CANDIDATE,
    STATUS_IGNORED,
    STATUS_QUALIFIED,
    STATUS_REJECTED,
    KOLVerificationInput,
    KOLVerificationResult,
    SocialKOLService,
    SocialPostSample,
    SocialProfileSnapshot,
)
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet, PROVIDER
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
TOKEN_2 = "0x39dbed3a2bd333467115de45665cc57f813c4571"


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/discovery.db")
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
            started_at=now - timedelta(minutes=20),
            last_seen_at=now,
        )
        session.add(watch)
        session.flush()
        session.add(
            PriceSnapshot(
                wallet_id=wallet.id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal("10"),
                observed_at=now,
                quality_status="VALID",
            )
        )
        wallet_id = wallet.id
        watch_id = watch.id
    return session_factory, wallet_id, watch_id


class FakeSearchClient:
    def __init__(self, tweets_by_query=None, errors_by_query=None) -> None:
        self.tweets_by_query = tweets_by_query or {}
        self.errors_by_query = errors_by_query or {}
        self.calls: list[str] = []
        self.provider_queries: list[str] = []

    def search_tweets(self, query: str, *, query_type: str = "Latest", cursor: str = "") -> list[NormalizedTweet]:
        self.provider_queries.append(query)
        base_query = query.split(" since_time:", 1)[0]
        self.calls.append(base_query)
        error = self.errors_by_query.get(query) or self.errors_by_query.get(base_query)
        if error:
            raise error
        return list(self.tweets_by_query.get(query, self.tweets_by_query.get(base_query, [])))


class FakeProvider:
    def __init__(self, *, followers=1500, author_id=None, fail_profile=False, fail_posts=False) -> None:
        self.followers = followers
        self.author_id = author_id
        self.fail_profile = fail_profile
        self.fail_posts = fail_posts
        self.profile_calls = 0
        self.posts_calls = 0

    def get_user_profile(self, username: str) -> SocialProfileSnapshot:
        self.profile_calls += 1
        if self.fail_profile:
            raise RuntimeError("profile down")
        return SocialProfileSnapshot(self.author_id, username, "crypto trader", self.followers)

    def get_recent_posts(self, username: str, limit: int = 5) -> list[SocialPostSample]:
        self.posts_calls += 1
        if self.fail_posts:
            raise RuntimeError("timeline down")
        return [SocialPostSample("p1", "crypto markets"), SocialPostSample("p2", "web3 research")]


class FakeClassifier:
    def __init__(self, result: KOLVerificationResult | Exception) -> None:
        self.result = result
        self.calls = 0
        self.inputs: list[KOLVerificationInput] = []

    def classify(self, payload: KOLVerificationInput) -> KOLVerificationResult:
        self.calls += 1
        self.inputs.append(payload)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def tweet(
    tweet_id: str,
    *,
    token: str = TOKEN,
    author_id: str | None = "author-1",
    username: str = "newkol",
    followers: int | None = 1500,
    created_at: datetime | None = None,
    matches: list[TokenMatch] | None = None,
    is_retweet: bool = False,
) -> NormalizedTweet:
    created = created_at or datetime.now(UTC)
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=tweet_id,
        author_id=author_id,
        author_username=username,
        author_name=username,
        author_followers=followers,
        text=f"Watching {token}",
        created_at=created,
        detected_at=created + timedelta(seconds=3),
        is_reply=False,
        in_reply_to_id=None,
        in_reply_to_username=None,
        conversation_id=tweet_id,
        is_quote=False,
        quoted_tweet_id=None,
        quoted_tweet=None,
        is_retweet=is_retweet,
        retweeted_tweet_id="rt" if is_retweet else None,
        retweeted_tweet={"id": "rt"} if is_retweet else None,
        like_count=1,
        retweet_count=0,
        reply_count=0,
        quote_count=0,
        view_count=100,
        token_matches=matches if matches is not None else [TokenMatch("direct_ca", token.lower(), token)],
        raw={},
    )


def make_service(
    session_factory,
    search_client,
    *,
    auto_verify=False,
    provider=None,
    classifier=None,
    batch_size=3,
    due_seconds=300,
    max_due_seconds=14400,
    bootstrap_lookback_seconds=900,
    overlap_seconds=120,
    max_pages=1,
) -> SocialDiscoveryService:
    return SocialDiscoveryService(
        session_factory=session_factory,
        search_client=search_client,
        kol_service=SocialKOLService(session_factory),
        event_service=SocialEventService(session_factory),
        due_seconds=due_seconds,
        max_due_seconds=max_due_seconds,
        bootstrap_lookback_seconds=bootstrap_lookback_seconds,
        batch_size=batch_size,
        max_pages=max_pages,
        overlap_seconds=overlap_seconds,
        min_usd_value=Decimal("5"),
        auto_verify_enabled=auto_verify,
        profile_provider=provider,
        classifier=classifier,
    )


def rows(session_factory, model):
    with session_scope(session_factory) as session:
        data = list(session.scalars(select(model).order_by(model.id.asc())))
        for row in data:
            session.expunge(row)
        return data


def set_latest_usd_value(session_factory, watch_id: int, value: str) -> None:
    with session_scope(session_factory) as session:
        watch = session.get(TokenWatchState, watch_id)
        session.add(
            PriceSnapshot(
                wallet_id=watch.wallet_id,
                chain=watch.chain,
                token_address=watch.token_address,
                symbol=watch.symbol,
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal(value),
                observed_at=utc_now(),
                quality_status="VALID",
            )
        )


def set_watch_started_at(session_factory, watch_id: int, started_at: datetime) -> None:
    with session_scope(session_factory) as session:
        watch = session.get(TokenWatchState, watch_id)
        watch.started_at = started_at


def add_active_watch(session_factory, wallet_id: int, token_address: str, symbol: str, usd_value: str = "10") -> int:
    now = utc_now()
    with session_scope(session_factory) as session:
        watch = TokenWatchState(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=token_address,
            symbol=symbol,
            active=True,
            started_at=now - timedelta(minutes=20),
            last_seen_at=now,
        )
        session.add(watch)
        session.flush()
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=token_address,
                symbol=symbol,
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal(usd_value),
                observed_at=now,
                quality_status="VALID",
            )
        )
        return watch.id


def token_number(index: int) -> str:
    return f"0x{index:040x}"


def add_identity(session_factory, identity_type: str, username: str) -> None:
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                identity_type=identity_type,
                value=username,
                normalized_value=username.lower(),
                source="test",
                source_field="test",
                confidence=HIGH,
                is_active=True,
                first_seen_at=now,
                last_verified_at=now,
                valid_from=now,
            )
        )


def test_build_token_discovery_queries_defaults_to_ca_only() -> None:
    queries = build_token_discovery_queries(token_address=TOKEN, symbol="ROBBIE")

    assert [(query.kind, query.query) for query in queries] == [(DISCOVERY_QUERY_CA, TOKEN)]


def test_build_token_discovery_queries_cashtag_requires_flag() -> None:
    disabled = build_token_discovery_queries(token_address=TOKEN, symbol="ROBBIE", cashtag_enabled=False)
    enabled = build_token_discovery_queries(token_address=TOKEN, symbol="ROBBIE", cashtag_enabled=True)

    assert [query.query for query in disabled] == [TOKEN]
    assert [query.query for query in enabled] == [TOKEN, "$ROBBIE"]


def test_build_incremental_discovery_query_adds_since_and_until_time() -> None:
    since = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    until = datetime(2026, 9, 2, 12, 5, 0, tzinfo=UTC)

    query = build_incremental_discovery_query(TOKEN, since=since, until=until)

    assert query == f"{TOKEN} since_time:1788350400 until_time:1788350700"


def test_discovery_max_pages_is_explicitly_clamped_to_one(ctx, caplog) -> None:
    session_factory, _, _ = ctx

    service = make_service(session_factory, FakeSearchClient(), max_pages=3)

    assert service.max_pages == 1
    assert "clamped to 1" in caplog.text


def test_active_watch_with_valid_usd_value_is_due_below_five_and_inactive_are_skipped(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    search = FakeSearchClient()
    service = make_service(session_factory, search)
    now = utc_now()
    with session_scope(session_factory) as session:
        inactive = TokenWatchState(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN_2,
            symbol="LOW",
            active=False,
            started_at=now - timedelta(minutes=10),
            last_seen_at=now,
        )
        session.add(inactive)

    assert [target.watch_state_id for target in service.due_targets(now=now)] == [watch_id]
    set_latest_usd_value(session_factory, watch_id, "4.99")

    assert service.due_targets(now=utc_now()) == []


@pytest.mark.asyncio
async def test_fair_scheduling_10_tokens_batch1_no_starvation(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    for index in range(2, 11):
        add_active_watch(session_factory, wallet_id, token_number(index), f"T{index}")
    search = FakeSearchClient()
    service = make_service(session_factory, search, batch_size=1, due_seconds=300)
    start = datetime(2026, 9, 2, 12, 0, 0)

    for round_index in range(10):
        await service.scan_due_tokens(now=start + timedelta(seconds=round_index * 60))

    assert len(search.calls[:10]) == 10
    assert len(set(search.calls[:10])) == 10


@pytest.mark.asyncio
async def test_failed_first_search_respects_next_due_before_retry(ctx) -> None:
    session_factory, _, _ = ctx
    now = datetime(2026, 9, 2, 12, 0, 0)
    search = FakeSearchClient(errors_by_query={TOKEN: RuntimeError("search down")})
    service = make_service(session_factory, search, batch_size=1, due_seconds=300)

    await service.scan_due_tokens(now=now)

    for minute in range(1, 5):
        assert service.due_targets(now=now + timedelta(minutes=minute)) == []
    assert service.due_targets(now=now + timedelta(minutes=5))[0].token_address == TOKEN


@pytest.mark.asyncio
async def test_failed_token_does_not_starve_other_never_scanned_tokens(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    for index in range(2, 11):
        add_active_watch(session_factory, wallet_id, token_number(index), f"T{index}")
    now = datetime(2026, 9, 2, 12, 0, 0)
    search = FakeSearchClient(errors_by_query={TOKEN: RuntimeError("search down")})
    service = make_service(session_factory, search, batch_size=1, due_seconds=300)

    await service.scan_due_tokens(now=now)
    for round_index in range(1, 10):
        await service.scan_due_tokens(now=now + timedelta(seconds=round_index * 60))

    assert search.calls[0] == TOKEN
    assert TOKEN not in search.calls[1:10]
    assert set(search.calls[1:10]) == {token_number(index) for index in range(2, 11)}
    assert service.due_targets(now=now + timedelta(minutes=10), limit=1)[0].token_address == TOKEN


@pytest.mark.asyncio
async def test_fair_scheduling_20_tokens_batch3_eventually_scans_all(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    for index in range(2, 21):
        add_active_watch(session_factory, wallet_id, token_number(index), f"T{index}")
    search = FakeSearchClient()
    service = make_service(session_factory, search, batch_size=3, due_seconds=300)
    start = datetime(2026, 9, 2, 12, 0, 0)

    for round_index in range(7):
        await service.scan_due_tokens(now=start + timedelta(seconds=round_index * 60))

    assert len(set(search.calls)) >= 20


def test_fair_scheduling_never_scanned_and_oldest_due_priority(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    second_id = add_active_watch(session_factory, wallet_id, token_number(2), "T2")
    third_id = add_active_watch(session_factory, wallet_id, token_number(3), "T3")
    now = datetime(2026, 9, 2, 12, 0, 0)
    with session_scope(session_factory) as session:
        session.add_all(
            [
                SocialDiscoveryCursor(
                    watch_state_id=watch_id,
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=TOKEN,
                    query_kind=DISCOVERY_QUERY_CA,
                    last_success_at=now - timedelta(minutes=30),
                    next_due_at=now - timedelta(minutes=1),
                ),
                SocialDiscoveryCursor(
                    watch_state_id=second_id,
                    wallet_id=wallet_id,
                    chain="robinhood",
                    token_address=token_number(2),
                    query_kind=DISCOVERY_QUERY_CA,
                    last_success_at=now - timedelta(minutes=40),
                    next_due_at=now - timedelta(minutes=5),
                ),
            ]
        )
    service = make_service(session_factory, FakeSearchClient(), batch_size=1)

    assert service.due_targets(now=now, limit=1)[0].watch_state_id == third_id

    with session_scope(session_factory) as session:
        session.get(TokenWatchState, third_id).active = False

    assert service.due_targets(now=now, limit=1)[0].watch_state_id == second_id


def test_discovery_pauses_below_five_and_recovers_into_due_queue(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0)
    with session_scope(session_factory) as session:
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                last_success_at=now - timedelta(hours=1),
                next_due_at=now - timedelta(minutes=30),
            )
        )
    service = make_service(session_factory, FakeSearchClient(), batch_size=1)
    set_latest_usd_value(session_factory, watch_id, "4.99")

    assert service.due_targets(now=now) == []

    set_latest_usd_value(session_factory, watch_id, "5.01")

    assert service.due_targets(now=now)[0].watch_state_id == watch_id


def test_due_targets_does_not_create_cursors_for_unselected_watches(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    for index in range(2, 5):
        add_active_watch(session_factory, wallet_id, token_number(index), f"T{index}")
    service = make_service(session_factory, FakeSearchClient(), batch_size=1)

    assert len(service.due_targets(now=utc_now(), limit=1)) == 1
    assert rows(session_factory, SocialDiscoveryCursor) == []


@pytest.mark.asyncio
async def test_cursor_watermark_success_failure_overlap_and_duplicate_tweet(ctx) -> None:
    session_factory, _, watch_id = ctx
    now = datetime.now(UTC)
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("old", created_at=now - timedelta(seconds=181)), tweet("new", created_at=now)]}),
        due_seconds=300,
        overlap_seconds=120,
    )

    await service.scan_due_tokens(now=now)
    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    assert cursor.watch_state_id == watch_id
    assert cursor.last_success_at.replace(tzinfo=UTC) == now
    assert cursor.next_due_at.replace(tzinfo=UTC) == now + timedelta(seconds=300)

    with session_scope(session_factory) as session:
        row = session.get(SocialDiscoveryCursor, cursor.id)
        row.last_success_at = now - timedelta(seconds=60)
        row.next_due_at = now
    failing = make_service(session_factory, FakeSearchClient(errors_by_query={TOKEN: RuntimeError("search down")}))
    await failing.scan_due_tokens(now=now)
    failed = rows(session_factory, SocialDiscoveryCursor)[0]

    assert failed.last_success_at.replace(tzinfo=UTC) == now - timedelta(seconds=60)
    assert failed.error_count == 1


@pytest.mark.asyncio
async def test_first_discovery_bootstrap_uses_recent_window_for_old_watch(ctx) -> None:
    session_factory, _, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    set_watch_started_at(session_factory, watch_id, now - timedelta(minutes=20))
    service = make_service(session_factory, FakeSearchClient(), due_seconds=900, bootstrap_lookback_seconds=900)

    await service.scan_due_tokens(now=now)

    provider_query = service.search_client.provider_queries[0]
    assert f"since_time:{int((now - timedelta(minutes=15)).timestamp())}" in provider_query
    assert f"until_time:{int(now.timestamp())}" in provider_query


@pytest.mark.asyncio
async def test_first_discovery_bootstrap_does_not_start_before_watch_started(ctx) -> None:
    session_factory, _, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    started = now - timedelta(minutes=5)
    set_watch_started_at(session_factory, watch_id, started)
    service = make_service(session_factory, FakeSearchClient(), due_seconds=900, bootstrap_lookback_seconds=900)

    await service.scan_due_tokens(now=now)

    assert f"since_time:{int(started.timestamp())}" in service.search_client.provider_queries[0]


@pytest.mark.asyncio
async def test_second_discovery_uses_incremental_last_success_with_overlap(ctx) -> None:
    session_factory, _, watch_id = ctx
    first = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    second = first + timedelta(minutes=30)
    set_watch_started_at(session_factory, watch_id, first - timedelta(minutes=20))
    search = FakeSearchClient()
    service = make_service(session_factory, search, due_seconds=900, overlap_seconds=120)

    await service.scan_due_tokens(now=first)
    with session_scope(session_factory) as session:
        cursor = session.scalar(select(SocialDiscoveryCursor))
        cursor.next_due_at = second.replace(tzinfo=None)
    await service.scan_due_tokens(now=second)

    assert len(search.provider_queries) == 2
    second_query = search.provider_queries[1]
    assert f"since_time:{int((first - timedelta(seconds=120)).timestamp())}" in second_query
    assert f"until_time:{int(second.timestamp())}" in second_query
    assert search.provider_queries[0] != search.provider_queries[1]


@pytest.mark.asyncio
async def test_zero_result_success_advances_cursor_and_backs_off(ctx) -> None:
    session_factory, _, _ = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    service = make_service(session_factory, FakeSearchClient(), due_seconds=900)

    await service.scan_due_tokens(now=now)

    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    assert cursor.last_success_at.replace(tzinfo=UTC) == now
    assert cursor.consecutive_no_new_author_runs == 1
    assert cursor.next_due_at.replace(tzinfo=UTC) == now + timedelta(minutes=30)
    assert service.stats.searches_returning_zero_new_authors == 1


@pytest.mark.asyncio
async def test_discovery_backoff_resets_after_new_candidate(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    set_watch_started_at(session_factory, watch_id, now - timedelta(minutes=20))
    search = FakeSearchClient({TOKEN: [tweet("candidate", created_at=now + timedelta(seconds=1))]})
    service = make_service(session_factory, search, due_seconds=900)
    with session_scope(session_factory) as session:
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                last_success_at=now - timedelta(hours=1),
                next_due_at=now,
                consecutive_no_new_author_runs=5,
            )
        )

    await service.scan_due_tokens(now=now)

    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    assert cursor.consecutive_no_new_author_runs == 0
    assert cursor.next_due_at.replace(tzinfo=UTC) == now + timedelta(minutes=15)


@pytest.mark.asyncio
async def test_known_qualified_kol_event_does_not_reset_discovery_backoff(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    set_watch_started_at(session_factory, watch_id, now - timedelta(minutes=20))
    SocialKOLService(session_factory).observe_candidate(
        author_id="known",
        username="knownkol",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    with session_scope(session_factory) as session:
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                last_success_at=now - timedelta(hours=1),
                next_due_at=now,
                consecutive_no_new_author_runs=2,
            )
        )
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("known-backoff", author_id="known", username="knownkol", followers=3000)]}),
        due_seconds=900,
    )

    await service.scan_due_tokens(now=now)

    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    events = rows(session_factory, SocialEvent)
    assert cursor.consecutive_no_new_author_runs == 3
    assert cursor.next_due_at.replace(tzinfo=UTC) == now + timedelta(minutes=60)
    assert len(events) == 1
    assert events[0].author_type == AUTHOR_KOL
    assert service.stats.known_kol_hits == 1
    assert service.stats.kol_events_created == 1


@pytest.mark.asyncio
async def test_existing_candidate_again_does_not_reset_discovery_backoff(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    set_watch_started_at(session_factory, watch_id, now - timedelta(minutes=20))
    SocialKOLService(session_factory).observe_candidate(
        author_id="candidate",
        username="candidate",
        followers=1500,
        source=SOURCE_SOCIAL_DISCOVERY,
    )
    with session_scope(session_factory) as session:
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                last_success_at=now - timedelta(hours=1),
                next_due_at=now,
                consecutive_no_new_author_runs=2,
            )
        )
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("candidate-again", author_id="candidate", username="candidate", followers=1500)]}),
        due_seconds=900,
    )

    await service.scan_due_tokens(now=now)

    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    assert cursor.consecutive_no_new_author_runs == 3
    assert service.stats.candidates_existing == 1


@pytest.mark.asyncio
async def test_cached_rejected_again_does_not_reset_discovery_backoff(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)
    set_watch_started_at(session_factory, watch_id, now - timedelta(minutes=20))
    profile = SocialKOLService(session_factory).observe_candidate(
        author_id="rejected",
        username="rejected",
        followers=1500,
        source=SOURCE_SOCIAL_DISCOVERY,
    )
    with session_scope(session_factory) as session:
        row = session.get(SocialKOLProfile, profile.id)
        row.status = STATUS_REJECTED
        row.recheck_after = now + timedelta(days=10)
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                last_success_at=now - timedelta(hours=1),
                next_due_at=now,
                consecutive_no_new_author_runs=2,
            )
        )
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("rejected-again", author_id="rejected", username="rejected", followers=1500)]}),
        due_seconds=900,
    )

    await service.scan_due_tokens(now=now)

    cursor = rows(session_factory, SocialDiscoveryCursor)[0]
    assert cursor.consecutive_no_new_author_runs == 3
    assert service.stats.known_profile_cache_hits == 1


def test_new_watch_session_gets_independent_cursor(ctx) -> None:
    session_factory, wallet_id, watch_id = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialDiscoveryCursor(
                watch_state_id=watch_id,
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                query_kind=DISCOVERY_QUERY_CA,
                next_due_at=now + timedelta(hours=1),
            )
        )
        old_watch = session.get(TokenWatchState, watch_id)
        old_watch.active = False
        old_watch.ended_at = now
        new_watch = TokenWatchState(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="ROBBIE",
            active=True,
            started_at=now,
            last_seen_at=now,
        )
        session.add(new_watch)
        session.flush()
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal("10"),
                observed_at=now,
                quality_status="VALID",
            )
        )
        new_watch_id = new_watch.id

    service = make_service(session_factory, FakeSearchClient())

    assert [target.watch_state_id for target in service.due_targets(now=now)] == [new_watch_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("identity_type", [PROJECT_X, DEV_X])
async def test_project_or_dev_author_is_skipped_as_kol_candidate(ctx, identity_type) -> None:
    session_factory, _, _ = ctx
    add_identity(session_factory, identity_type, "ProjectUser")
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("project", username="ProjectUser", followers=10)]}),
    )

    await service.scan_due_tokens()

    assert rows(session_factory, SocialKOLProfile) == []
    assert rows(session_factory, SocialEvent) == []
    assert service.stats.project_dev_skipped == 1


@pytest.mark.asyncio
async def test_new_author_below_1000_skips_profile_creation(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("small", username="small", followers=999)]}),
    )

    await service.scan_due_tokens()

    assert rows(session_factory, SocialKOLProfile) == []
    assert service.stats.low_follower_skipped == 1


@pytest.mark.asyncio
async def test_new_author_1000_or_missing_followers_creates_candidate(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient(
                {
                    TOKEN: [
                        tweet("candidate", author_id="candidate-id", username="candidate", followers=1000),
                        tweet("unknown-followers", author_id="unknown-id", username="unknown_followers", followers=None),
                    ]
                }
            ),
    )

    await service.scan_due_tokens()
    profiles = rows(session_factory, SocialKOLProfile)

    assert {profile.normalized_username for profile in profiles} == {"candidate", "unknown_followers"}
    assert {profile.status for profile in profiles} == {STATUS_CANDIDATE}


@pytest.mark.asyncio
async def test_known_qualified_kol_creates_discovery_event_without_verification(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="known",
        username="knownkol",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("known", author_id="known", username="knownkol", followers=3000)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
    )

    await service.scan_due_tokens()
    events = rows(session_factory, SocialEvent)

    assert len(events) == 1
    assert events[0].author_type == AUTHOR_KOL
    assert events[0].ingestion_type == INGESTION_DISCOVERY
    assert provider.profile_calls == 0
    assert classifier.calls == 0
    assert service.stats.known_kol_hits == 1


@pytest.mark.asyncio
async def test_author_id_conflict_does_not_count_as_known_kol_hit(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="456",
        username="same_name",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("conflict", author_id="123", username="same_name", followers=3000)]}),
    )

    await service.scan_due_tokens()

    assert rows(session_factory, SocialEvent) == []
    assert service.stats.known_kol_hits == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [STATUS_REJECTED, STATUS_IGNORED, STATUS_QUALIFIED])
async def test_known_profile_ttl_cache_skips_verification(ctx, status) -> None:
    session_factory, _, _ = ctx
    kol_service = SocialKOLService(session_factory)
    profile = kol_service.observe_candidate(author_id="cached", username="cached", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    with session_scope(session_factory) as session:
        row = session.get(SocialKOLProfile, profile.id)
        row.status = status
        row.recheck_after = utc_now() + timedelta(days=10)
    provider = FakeProvider()
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("cached", author_id="cached", username="cached", followers=3000)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
    )

    await service.scan_due_tokens()

    assert provider.profile_calls == 0
    assert classifier.calls == 0
    assert service.stats.known_profile_cache_hits == (0 if status == STATUS_QUALIFIED else 1)


@pytest.mark.asyncio
async def test_direct_and_quote_ca_match_retweet_and_unrelated_skip(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient(
            {
                TOKEN: [
                    tweet("direct", matches=[TokenMatch("direct_ca", TOKEN, TOKEN)]),
                    tweet("quote", matches=[TokenMatch("quote_ca", TOKEN, TOKEN)]),
                    tweet("cashtag-only", matches=[TokenMatch("direct_cashtag", "ROBBIE", "$ROBBIE")]),
                    tweet("retweet", is_retweet=True),
                    tweet("wrong-ca", matches=[TokenMatch("direct_ca", TOKEN_2, TOKEN_2)]),
                ]
            }
        ),
    )

    await service.scan_due_tokens()

    assert service.stats.ca_matches == 2
    assert service.stats.retweets_skipped == 1
    assert len(rows(session_factory, SocialKOLProfile)) == 1


@pytest.mark.asyncio
async def test_candidate_qualified_in_same_run_promotes_original_tweet_to_event_and_first_mention(ctx) -> None:
    session_factory, _, _ = ctx
    provider = FakeProvider(followers=1500, author_id="new")
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("new-call", author_id="new", username="newkol", followers=1500)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
    )

    await service.scan_due_tokens()
    events = rows(session_factory, SocialEvent)
    first_mentions = rows(session_factory, KOLTokenFirstMention)

    assert len(events) == 1
    assert events[0].provider_event_id == "new-call"
    assert first_mentions[0].first_tweet_id == "new-call"
    assert service.stats.verification_qualified == 1


@pytest.mark.asyncio
async def test_candidate_merge_to_canonical_profile_still_promotes_pending_tweet(ctx) -> None:
    session_factory, _, _ = ctx
    kol_service = SocialKOLService(session_factory)
    kol_service.observe_candidate(
        author_id="canonical",
        username="oldname",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    provider = FakeProvider(followers=1500, author_id="canonical")
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("merge-call", author_id=None, username="newname", followers=1500)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
    )

    await service.scan_due_tokens()
    events = rows(session_factory, SocialEvent)
    profiles = rows(session_factory, SocialKOLProfile)

    assert len(profiles) == 1
    assert profiles[0].x_author_id == "canonical"
    assert profiles[0].normalized_username == "newname"
    assert len(events) == 1
    assert events[0].provider_event_id == "merge-call"


@pytest.mark.asyncio
async def test_candidate_rejected_or_verification_failed_does_not_create_kol_event(ctx) -> None:
    session_factory, _, _ = ctx
    rejected = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("reject", author_id="reject", username="reject", followers=1500)]}),
        auto_verify=True,
        provider=FakeProvider(followers=1500, author_id="reject"),
        classifier=FakeClassifier(KOLVerificationResult(False, KOL_HIGH, "other", "not crypto")),
    )
    await rejected.scan_due_tokens()

    assert rows(session_factory, SocialEvent) == []
    assert rejected.stats.verification_rejected == 1


@pytest.mark.asyncio
async def test_discovery_and_stream_same_tweet_dedupe(ctx) -> None:
    session_factory, _, _ = ctx
    SocialKOLService(session_factory).observe_candidate(
        author_id="known",
        username="knownkol",
        followers=3000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    existing_tweet = tweet("same", author_id="known", username="knownkol", followers=3000)
    SocialEventService(session_factory).create_event_if_relevant(
        existing_tweet,
        ingestion_type=INGESTION_STREAM,
        known_kol_usernames={"knownkol"},
        known_kol_author_ids={"known"},
    )
    service = make_service(session_factory, FakeSearchClient({TOKEN: [existing_tweet]}))

    await service.scan_due_tokens()

    events = rows(session_factory, SocialEvent)
    assert len(events) == 1
    assert events[0].ingestion_type == INGESTION_STREAM
    assert service.stats.duplicates == 1


@pytest.mark.asyncio
async def test_one_token_search_failure_does_not_block_other_token(ctx) -> None:
    session_factory, wallet_id, _ = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        watch = TokenWatchState(
            wallet_id=wallet_id,
            chain="robinhood",
            token_address=TOKEN_2,
            symbol="TWO",
            active=True,
            started_at=now - timedelta(minutes=10),
            last_seen_at=now,
        )
        session.add(watch)
        session.flush()
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain="robinhood",
                token_address=TOKEN_2,
                symbol="TWO",
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal("10"),
                observed_at=now,
                quality_status="VALID",
            )
        )
    service = make_service(
        session_factory,
        FakeSearchClient(
            {TOKEN_2: [tweet("two", token=TOKEN_2, matches=[TokenMatch("direct_ca", TOKEN_2, TOKEN_2)])]},
            errors_by_query={TOKEN: RuntimeError("search down")},
        ),
        batch_size=3,
    )

    await service.scan_due_tokens(now=now)

    assert service.stats.provider_errors == 1
    assert service.stats.search_api_calls == 2
    assert service.stats.tokens_scanned == 2
    assert len(rows(session_factory, SocialKOLProfile)) == 1


@pytest.mark.asyncio
async def test_deepseek_or_profile_failure_does_not_stop_search(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("fail", author_id="fail", username="fail", followers=1500)]}),
        auto_verify=True,
        provider=FakeProvider(fail_profile=True),
        classifier=FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto")),
    )

    await service.scan_due_tokens()

    assert service.stats.verification_failed == 1
    assert service.stats.search_api_calls == 1
    assert service.stats.profile_api_calls == 1
    assert service.stats.recent_posts_api_calls == 0
    assert service.stats.deepseek_calls == 0
    assert rows(session_factory, SocialEvent) == []


@pytest.mark.asyncio
async def test_runtime_metrics_count_actual_profile_recent_and_deepseek_calls(ctx) -> None:
    session_factory, _, _ = ctx
    provider = FakeProvider(followers=1500, author_id="metrics")
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("metrics", author_id="metrics", username="metrics", followers=1500)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
    )

    await service.scan_due_tokens()

    assert service.stats.search_api_calls == 1
    assert service.stats.profile_api_calls == 1
    assert service.stats.recent_posts_api_calls == 1
    assert service.stats.deepseek_calls == 1


@pytest.mark.asyncio
async def test_runtime_metrics_skip_recent_and_deepseek_when_profile_followers_low(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("missing-followers", author_id="low", username="low", followers=None)]}),
        auto_verify=True,
        provider=FakeProvider(followers=999, author_id="low"),
        classifier=FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto")),
    )

    await service.scan_due_tokens()

    assert service.stats.profile_api_calls == 1
    assert service.stats.recent_posts_api_calls == 0
    assert service.stats.deepseek_calls == 0
    assert service.stats.verification_ignored_followers == 1
    assert service.stats.verification_failed == 0


@pytest.mark.asyncio
async def test_runtime_metrics_recent_posts_failure_stops_before_deepseek(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("posts-fail", author_id="posts", username="posts", followers=1500)]}),
        auto_verify=True,
        provider=FakeProvider(followers=1500, author_id="posts", fail_posts=True),
        classifier=FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto")),
    )

    await service.scan_due_tokens()

    assert service.stats.profile_api_calls == 1
    assert service.stats.recent_posts_api_calls == 1
    assert service.stats.deepseek_calls == 0
    assert service.stats.verification_failed == 1


@pytest.mark.asyncio
async def test_runtime_metrics_deepseek_failure_counts_classifier_call(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("deepseek-fail", author_id="deep", username="deep", followers=1500)]}),
        auto_verify=True,
        provider=FakeProvider(followers=1500, author_id="deep"),
        classifier=FakeClassifier(RuntimeError("deepseek down")),
    )

    await service.scan_due_tokens()

    assert service.stats.profile_api_calls == 1
    assert service.stats.recent_posts_api_calls == 1
    assert service.stats.deepseek_calls == 1
    assert service.stats.verification_failed == 1


def test_discovery_stats_snapshot_contains_core_metrics_without_tweet_text(ctx) -> None:
    service = SocialDiscoveryService(
        session_factory=None,
        search_client=FakeSearchClient(),
        kol_service=None,
        event_service=None,
    )
    service.stats.search_runs = 1
    service.stats.tweets_received = 2
    service.stats.ca_matches = 1
    service.stats.deepseek_calls = 3

    snapshot = service.stats_snapshot()

    assert snapshot["search_runs"] == 1
    assert snapshot["tweets_received"] == 2
    assert snapshot["ca_matches"] == 1
    assert snapshot["deepseek_calls"] == 3
    assert "text" not in snapshot
    assert "Watching" not in str(snapshot)


@pytest.mark.asyncio
async def test_only_qualified_outcome_promotes_pending_tweet(ctx) -> None:
    session_factory, _, _ = ctx
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("not-qualified", author_id="nq", username="nq", followers=1500)]}),
        auto_verify=True,
        provider=FakeProvider(followers=1500, author_id="nq"),
        classifier=FakeClassifier(KOLVerificationResult(None, "LOW", "other", "uncertain")),
    )

    await service.scan_due_tokens()

    assert service.stats.verification_uncertain == 1
    assert rows(session_factory, SocialEvent) == []


@pytest.mark.asyncio
async def test_repeated_cached_discovery_does_not_increase_verification_calls(ctx) -> None:
    session_factory, _, _ = ctx
    kol_service = SocialKOLService(session_factory)
    profile = kol_service.observe_candidate(author_id="cached", username="cached", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    kol_service.verify_candidate(
        profile.id,
        provider=FakeProvider(followers=1500, author_id="cached"),
        classifier=FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto")),
    )
    provider = FakeProvider(followers=1500, author_id="cached")
    classifier = FakeClassifier(KOLVerificationResult(True, KOL_HIGH, "trader", "crypto"))
    service = make_service(
        session_factory,
        FakeSearchClient({TOKEN: [tweet("cached", author_id="cached", username="cached", followers=1500)]}),
        auto_verify=True,
        provider=provider,
        classifier=classifier,
        due_seconds=1,
    )

    for _ in range(3):
        await service.scan_due_tokens(now=utc_now() + timedelta(seconds=5))
        with session_scope(session_factory) as session:
            cursor = session.scalar(select(SocialDiscoveryCursor))
            cursor.next_due_at = utc_now()

    assert provider.profile_calls == 0
    assert provider.posts_calls == 0
    assert classifier.calls == 0
    assert service.stats.profile_api_calls == 0
    assert service.stats.recent_posts_api_calls == 0
    assert service.stats.deepseek_calls == 0
