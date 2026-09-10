from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import TokenWatchState
from app.services.social_event_service import SocialEventService
from app.services.social_kol_service import SOURCE_TRUSTED_EXTERNAL_SEED, SocialKOLService
from app.services.social_watch_registry import SocialWatchAccount, SocialWatchRegistry, WATCH_FIXED_KOL
from app.services.twitter_author_cashtag_shadow import AuthorFirstCashtagShadow
from app.services.twitter_token_shadow import load_current_watched_token_identities
from app.services.twitterapi_io_client import PROVIDER, NormalizedTweet, normalize_tweet
from app.services.twitterapi_io_social_ingestion import TwitterApiIoSocialIngestor
from app.services.wallet_service import WalletService

WALLET_ADDRESS = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN_WALLET = "0x1111111111111111111111111111111111111111"
TOKEN_ROBBIE = "0x2222222222222222222222222222222222222222"
TOKEN_AI_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_AI_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


class NoopEventService:
    def create_event_if_relevant(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


class DummyRuleManager:
    def __init__(self) -> None:
        self.calls = 0

    async def ensure_rules(self, accounts, *, stats=None):  # noqa: ANN001
        self.calls += 1
        return []


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/author_cashtag_shadow.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 9, 1, 0, 0)
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET_ADDRESS, "robinhood")
        session.add_all(
            [
                watch(wallet.id, TOKEN_WALLET, "WALLET", now=now),
                watch(wallet.id, TOKEN_ROBBIE, "ROBBIE", now=now),
            ]
        )
    SocialKOLService(session_factory).observe_candidate(
        author_id="kol-id",
        username="NancyCrypto",
        followers=5000,
        source=SOURCE_TRUSTED_EXTERNAL_SEED,
    )
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(json.dumps({"kols": [{"username": "NancyCrypto", "enabled": True}]}))
    return session_factory, config_path


def test_author_first_qualified_kol_exact_ca_match(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])

    shadow.observe_tweet(tweet(TOKEN_WALLET), watched_tokens=load_current_watched_token_identities(ctx[0]))

    snap = shadow.stats_snapshot()
    assert snap["exact_ca_matches"] == 1
    assert snap["exact_ca_matched_tweet_hits"] == 1
    assert snap["token_kol_pairs"] == 1
    assert snap["exact_ca_token_kol_pairs"] == 1


def test_author_first_qualified_kol_unique_cashtag_match(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])

    shadow.observe_tweet(tweet("$WALLET"), watched_tokens=load_current_watched_token_identities(ctx[0]))

    snap = shadow.stats_snapshot()
    assert snap["cashtag_matches"] == 1
    assert snap["cashtag_only_matched_tweet_hits"] == 1
    assert snap["cashtag_only_token_kol_pairs"] == 1
    assert snap["token_kol_pairs"] == 1


def test_author_first_symbol_collision_is_ambiguous_and_not_kol_pair(ctx) -> None:
    session_factory, _ = ctx
    add_ai_collision(session_factory)
    shadow = AuthorFirstCashtagShadow(session_factory=session_factory)

    shadow.observe_tweet(tweet("$AI"), watched_tokens=load_current_watched_token_identities(session_factory))

    snap = shadow.stats_snapshot()
    assert snap["ambiguous_symbol_hits"] == 1
    assert snap["token_kol_pairs"] == 0
    assert snap["matched_tweet_hits"] == 0


def test_author_first_exact_ca_wins_over_collision(ctx) -> None:
    session_factory, _ = ctx
    add_ai_collision(session_factory)
    shadow = AuthorFirstCashtagShadow(session_factory=session_factory)

    shadow.observe_tweet(tweet(f"$AI {TOKEN_AI_A}"), watched_tokens=load_current_watched_token_identities(session_factory))

    snap = shadow.stats_snapshot()
    assert snap["exact_ca_and_cashtag_matches"] == 1
    assert snap["ambiguous_symbol_hits"] == 0
    assert snap["token_kol_pairs"] == 1


def test_author_first_multi_token_tweet_counts_multiple_results(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])

    shadow.observe_tweet(tweet("$WALLET $ROBBIE"), watched_tokens=load_current_watched_token_identities(ctx[0]))

    snap = shadow.stats_snapshot()
    assert snap["matched_tweet_hits"] == 1
    assert snap["token_match_results"] == 2
    assert snap["cashtag_matches"] == 2
    assert snap["token_kol_pairs"] == 2
    assert snap["tokens_with_kol_hits"] == 2
    assert snap["distinct_qualified_kol_authors"] == 1


def test_author_first_same_kol_multiple_tweets_count_once_per_token(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])
    watched_tokens = load_current_watched_token_identities(ctx[0])

    shadow.observe_tweet(tweet("$WALLET", tweet_id="a"), watched_tokens=watched_tokens)
    shadow.observe_tweet(tweet("$WALLET", tweet_id="b"), watched_tokens=watched_tokens)
    shadow.observe_tweet(tweet(TOKEN_WALLET, tweet_id="c"), watched_tokens=watched_tokens)

    snap = shadow.stats_snapshot()
    assert snap["qualified_kol_tweet_hits"] == 3
    assert snap["distinct_qualified_kol_authors"] == 1
    assert snap["token_kol_pairs"] == 1


def test_author_first_cashtag_only_metric_excludes_exact_ca_tweets(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])
    watched_tokens = load_current_watched_token_identities(ctx[0])

    shadow.observe_tweet(tweet(TOKEN_WALLET, tweet_id="ca"), watched_tokens=watched_tokens)
    shadow.observe_tweet(tweet(f"$WALLET {TOKEN_WALLET}", tweet_id="both"), watched_tokens=watched_tokens)

    snap = shadow.stats_snapshot()
    assert snap["exact_ca_matched_tweet_hits"] == 2
    assert snap["cashtag_only_matched_tweet_hits"] == 0
    assert snap["cashtag_only_token_kol_pairs"] == 0


def test_author_first_unqualified_author_does_not_count_kol_pair(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])

    shadow.observe_tweet(
        tweet("$WALLET", author_id="unknown", username="Unknown"),
        watched_tokens=load_current_watched_token_identities(ctx[0]),
    )

    snap = shadow.stats_snapshot()
    assert snap["unqualified_author_hits"] == 1
    assert snap["cashtag_matches"] == 0
    assert snap["token_kol_pairs"] == 0


def test_author_first_same_username_different_author_id_is_not_qualified_kol(ctx) -> None:
    shadow = AuthorFirstCashtagShadow(session_factory=ctx[0])

    shadow.observe_tweet(
        tweet("$WALLET", author_id="new-author-id", username="NancyCrypto"),
        watched_tokens=load_current_watched_token_identities(ctx[0]),
    )

    snap = shadow.stats_snapshot()
    assert snap["unqualified_author_hits"] == 1
    assert snap["cashtag_matches"] == 0
    assert snap["qualified_kol_tweet_hits"] == 0
    assert snap["token_kol_pairs"] == 0


@pytest.mark.asyncio
async def test_author_first_ingestor_observes_shadow_without_provider_rule_change(ctx) -> None:
    session_factory, config_path = ctx
    manager = DummyRuleManager()
    shadow = AuthorFirstCashtagShadow(session_factory=session_factory)
    ingestor = TwitterApiIoSocialIngestor(
        api_key="secret-key",
        registry=SocialWatchRegistry(session_factory, kol_config_path=config_path),
        event_service=NoopEventService(),
        rule_manager=manager,
        author_cashtag_shadow=shadow,
    )
    ingestor._current_accounts = [SocialWatchAccount("NancyCrypto", "nancycrypto", WATCH_FIXED_KOL)]
    ingestor.connected_at = datetime(2026, 9, 9, 0, 59, 0, tzinfo=UTC)

    await ingestor.handle_message(
        json.dumps({"event_type": "tweet", "tweet": tweet_payload("stream-1", "$WALLET")}),
        received_at=datetime(2026, 9, 9, 1, 0, 5, tzinfo=UTC),
    )

    snap = ingestor.stats_snapshot()["author_cashtag_shadow"]
    assert manager.calls == 0
    assert snap["provider_messages"] == 1
    assert snap["cashtag_matches"] == 1
    assert snap["cashtag_only_matched_tweet_hits"] == 1


def add_ai_collision(session_factory) -> None:  # noqa: ANN001
    with session_scope(session_factory) as session:
        wallet_id = session.query(TokenWatchState.wallet_id).first()[0]
        session.add(watch(wallet_id, TOKEN_AI_A, "AI", chain="robinhood"))
        session.add(watch(wallet_id, TOKEN_AI_B, "AI", chain="base"))


def watch(
    wallet_id: int,
    token_address: str,
    symbol: str,
    *,
    chain: str = "robinhood",
    now: datetime | None = None,
) -> TokenWatchState:
    current = now or datetime(2026, 9, 9, 1, 0, 0)
    return TokenWatchState(
        wallet_id=wallet_id,
        chain=chain,
        token_address=token_address,
        symbol=symbol,
        active=True,
        started_at=current,
        last_seen_at=current,
    )


def tweet(
    text: str,
    *,
    tweet_id: str = "tweet-1",
    author_id: str | None = "kol-id",
    username: str | None = "NancyCrypto",
    retweet: bool = False,
) -> NormalizedTweet:
    normalized = normalize_tweet(
        tweet_payload(tweet_id, text, author_id=author_id, username=username, retweet=retweet),
        detected_at=datetime(2026, 9, 9, 1, 0, 5, tzinfo=UTC),
    )
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=normalized.tweet_id,
        author_id=normalized.author_id,
        author_username=normalized.author_username,
        author_name=normalized.author_name,
        author_followers=normalized.author_followers,
        text=normalized.text,
        created_at=normalized.created_at,
        detected_at=normalized.detected_at,
        is_reply=normalized.is_reply,
        in_reply_to_id=normalized.in_reply_to_id,
        in_reply_to_username=normalized.in_reply_to_username,
        conversation_id=normalized.conversation_id,
        is_quote=normalized.is_quote,
        quoted_tweet_id=normalized.quoted_tweet_id,
        quoted_tweet=normalized.quoted_tweet,
        is_retweet=normalized.is_retweet,
        retweeted_tweet_id=normalized.retweeted_tweet_id,
        retweeted_tweet=normalized.retweeted_tweet,
        like_count=normalized.like_count,
        retweet_count=normalized.retweet_count,
        reply_count=normalized.reply_count,
        quote_count=normalized.quote_count,
        view_count=normalized.view_count,
        token_matches=normalized.token_matches,
        raw=normalized.raw,
    )


def tweet_payload(
    tweet_id: str,
    text: str,
    *,
    author_id: str | None = "kol-id",
    username: str | None = "NancyCrypto",
    retweet: bool = False,
) -> dict[str, object]:
    payload = {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-09T01:00:00Z",
        "author": {"id": author_id, "userName": username, "followers": 5000},
    }
    if retweet:
        payload["retweeted_tweet"] = {"id": f"rt-{tweet_id}", "text": text}
    return payload
