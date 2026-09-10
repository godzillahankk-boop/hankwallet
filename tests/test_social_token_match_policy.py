from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.social_token_match_policy import (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    IDENTITY_SCOPE_CONTRACT,
    IDENTITY_SCOPE_SYMBOL_OR_PROJECT,
    MATCH_STATUS_AMBIGUOUS_SYMBOL,
    MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR,
    MATCH_STATUS_MATCHED,
    MATCH_STATUS_UNMATCHED,
    MATCH_TYPE_CASHTAG,
    MATCH_TYPE_EXACT_CA,
    MATCH_TYPE_EXACT_CA_AND_CASHTAG,
    WatchedTokenIdentity,
    kol_distinct_author_key,
    match_social_token,
    match_social_tokens,
)
from app.services.twitterapi_io_client import PROVIDER, NormalizedTweet, normalize_tweet

TOKEN_WALLET = "0x1111111111111111111111111111111111111111"
TOKEN_ROBBIE = "0x2222222222222222222222222222222222222222"
TOKEN_AI_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_AI_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def test_unique_watched_symbol_qualified_kol_cashtag_matches_medium_confidence() -> None:
    result = match_social_token(
        tweet("$WALLET", author_id="nancy"),
        [watched(TOKEN_WALLET, "WALLET")],
        author_qualified_kol=True,
    )

    assert result.matched is True
    assert result.match_status == MATCH_STATUS_MATCHED
    assert result.match_type == MATCH_TYPE_CASHTAG
    assert result.confidence == CONFIDENCE_MEDIUM
    assert result.identity_scope == IDENTITY_SCOPE_SYMBOL_OR_PROJECT
    assert result.contract_address == TOKEN_WALLET


def test_unique_symbol_unqualified_author_cashtag_is_ignored() -> None:
    result = match_social_token(
        tweet("$WALLET", author_id="random"),
        [watched(TOKEN_WALLET, "WALLET")],
        author_qualified_kol=False,
    )

    assert result.matched is False
    assert result.match_status == MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR
    assert result.symbol == "WALLET"


def test_duplicate_watched_symbol_cashtag_is_ambiguous_for_qualified_kol() -> None:
    result = match_social_token(
        tweet("$AI", author_id="kol"),
        [
            watched(TOKEN_AI_A, "AI", chain="robinhood"),
            watched(TOKEN_AI_B, "AI", chain="base"),
        ],
        author_qualified_kol=True,
    )

    assert result.matched is False
    assert result.match_status == MATCH_STATUS_AMBIGUOUS_SYMBOL
    assert result.symbol == "AI"
    assert result.candidate_count == 2


def test_exact_ca_wins_over_symbol_collision() -> None:
    result = match_social_token(
        tweet(f"$AI {TOKEN_AI_A}", author_id="kol"),
        [
            watched(TOKEN_AI_A, "AI", chain="robinhood"),
            watched(TOKEN_AI_B, "AI", chain="base"),
        ],
        author_qualified_kol=True,
    )

    assert result.matched is True
    assert result.match_status == MATCH_STATUS_MATCHED
    assert result.match_type == MATCH_TYPE_EXACT_CA_AND_CASHTAG
    assert result.confidence == CONFIDENCE_HIGH
    assert result.identity_scope == IDENTITY_SCOPE_CONTRACT
    assert result.contract_address == TOKEN_AI_A


def test_exact_ca_without_cashtag_matches_contract_scope() -> None:
    result = match_social_token(
        tweet(TOKEN_AI_A, author_id="kol"),
        [watched(TOKEN_AI_A, "AI")],
        author_qualified_kol=False,
    )

    assert result.matched is True
    assert result.match_type == MATCH_TYPE_EXACT_CA
    assert result.confidence == CONFIDENCE_HIGH
    assert result.identity_scope == IDENTITY_SCOPE_CONTRACT


def test_exact_ca_and_cashtag_produce_one_high_confidence_match() -> None:
    result = match_social_token(
        tweet(f"Watching $AI {TOKEN_AI_A}", author_id="kol"),
        [watched(TOKEN_AI_A, "AI")],
        author_qualified_kol=True,
    )

    assert result.matched is True
    assert result.match_type == MATCH_TYPE_EXACT_CA_AND_CASHTAG
    assert result.contract_address == TOKEN_AI_A


def test_symbol_case_is_normalized_for_cashtag_matching() -> None:
    result = match_social_token(
        tweet("$wallet", author_id="kol"),
        [watched(TOKEN_WALLET, "WALLET")],
        author_qualified_kol=True,
    )

    assert result.matched is True
    assert result.match_type == MATCH_TYPE_CASHTAG
    assert result.symbol == "WALLET"


def test_evm_contract_case_is_normalized_for_exact_ca_matching() -> None:
    mixed_case = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"

    result = match_social_token(
        tweet(mixed_case, author_id="kol"),
        [watched(TOKEN_AI_A, "AI")],
        author_qualified_kol=True,
    )

    assert result.matched is True
    assert result.match_type == MATCH_TYPE_EXACT_CA
    assert result.contract_address == TOKEN_AI_A


def test_unrelated_tweet_is_unmatched() -> None:
    result = match_social_token(
        tweet("$NOPE", author_id="kol"),
        [watched(TOKEN_WALLET, "WALLET")],
        author_qualified_kol=True,
    )

    assert result.matched is False
    assert result.match_status == MATCH_STATUS_UNMATCHED


def test_distinct_kol_author_key_supports_unique_author_count_not_tweet_count() -> None:
    watched_tokens = [watched(TOKEN_WALLET, "WALLET")]
    results = [
        match_social_token(tweet("$WALLET", tweet_id="a", author_id="nancy"), watched_tokens, author_qualified_kol=True),
        match_social_token(tweet("$WALLET", tweet_id="b", author_id="nancy"), watched_tokens, author_qualified_kol=True),
        match_social_token(tweet("$WALLET", tweet_id="c", author_id="look"), watched_tokens, author_qualified_kol=True),
    ]

    unique_authors = {result.author_identity_key for result in results if result.matched}

    assert unique_authors == {"nancy", "look"}
    assert len(unique_authors) == 2


def test_same_kol_ca_and_cashtag_for_same_token_still_one_author_key() -> None:
    watched_tokens = [watched(TOKEN_WALLET, "WALLET")]
    results = [
        match_social_token(tweet(TOKEN_WALLET, tweet_id="ca", author_id="nancy"), watched_tokens, author_qualified_kol=True),
        match_social_token(tweet("$WALLET", tweet_id="tag", author_id="nancy"), watched_tokens, author_qualified_kol=True),
    ]

    unique_authors = {result.author_identity_key for result in results if result.matched}

    assert unique_authors == {"nancy"}
    assert len(unique_authors) == 1


def test_kol_distinct_author_key_falls_back_to_normalized_username() -> None:
    assert kol_distinct_author_key(None, "@NancyCrypto") == "nancycrypto"


def test_multi_match_qualified_kol_multiple_cashtags_return_multiple_tokens() -> None:
    results = match_social_tokens(
        tweet("$WALLET $ROBBIE", author_id="nancy"),
        [
            watched(TOKEN_WALLET, "WALLET"),
            watched(TOKEN_ROBBIE, "ROBBIE"),
        ],
        author_qualified_kol=True,
    )

    matched = [result for result in results if result.matched]
    assert [(result.symbol, result.match_type, result.confidence) for result in matched] == [
        ("WALLET", MATCH_TYPE_CASHTAG, CONFIDENCE_MEDIUM),
        ("ROBBIE", MATCH_TYPE_CASHTAG, CONFIDENCE_MEDIUM),
    ]


def test_multi_match_multiple_exact_contracts_return_multiple_tokens() -> None:
    results = match_social_tokens(
        tweet(f"{TOKEN_WALLET} {TOKEN_ROBBIE}", author_id="nancy"),
        [
            watched(TOKEN_WALLET, "WALLET"),
            watched(TOKEN_ROBBIE, "ROBBIE"),
        ],
        author_qualified_kol=True,
    )

    assert [(result.contract_address, result.match_type, result.confidence) for result in results] == [
        (TOKEN_WALLET, MATCH_TYPE_EXACT_CA, CONFIDENCE_HIGH),
        (TOKEN_ROBBIE, MATCH_TYPE_EXACT_CA, CONFIDENCE_HIGH),
    ]


def test_multi_match_exact_ca_and_matching_cashtag_dedupes_to_one_result() -> None:
    results = match_social_tokens(
        tweet(f"$AI {TOKEN_AI_A}", author_id="nancy"),
        [watched(TOKEN_AI_A, "AI")],
        author_qualified_kol=True,
    )

    assert len(results) == 1
    assert results[0].matched is True
    assert results[0].contract_address == TOKEN_AI_A
    assert results[0].match_type == MATCH_TYPE_EXACT_CA_AND_CASHTAG


def test_multi_match_symbol_collision_does_not_block_other_token_match() -> None:
    results = match_social_tokens(
        tweet("$WALLET $AI", author_id="nancy"),
        [
            watched(TOKEN_WALLET, "WALLET"),
            watched(TOKEN_AI_A, "AI", chain="robinhood"),
            watched(TOKEN_AI_B, "AI", chain="base"),
        ],
        author_qualified_kol=True,
    )

    by_symbol = {result.symbol: result for result in results}
    assert by_symbol["WALLET"].matched is True
    assert by_symbol["WALLET"].match_type == MATCH_TYPE_CASHTAG
    assert by_symbol["AI"].matched is False
    assert by_symbol["AI"].match_status == MATCH_STATUS_AMBIGUOUS_SYMBOL
    assert by_symbol["AI"].candidate_count == 2


def test_multi_match_unqualified_author_multiple_cashtags_are_ignored_per_symbol() -> None:
    results = match_social_tokens(
        tweet("$WALLET $ROBBIE", author_id="random"),
        [
            watched(TOKEN_WALLET, "WALLET"),
            watched(TOKEN_ROBBIE, "ROBBIE"),
        ],
        author_qualified_kol=False,
    )

    assert [result.matched for result in results] == [False, False]
    assert {result.symbol for result in results} == {"WALLET", "ROBBIE"}
    assert {result.match_status for result in results} == {MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR}
    assert not any(result.author_qualified_kol for result in results)


def test_multi_match_same_author_mentions_two_tokens_with_same_author_key() -> None:
    results = match_social_tokens(
        tweet("$WALLET $ROBBIE", author_id="nancy"),
        [
            watched(TOKEN_WALLET, "WALLET"),
            watched(TOKEN_ROBBIE, "ROBBIE"),
        ],
        author_qualified_kol=True,
    )

    assert {result.contract_address for result in results if result.matched} == {
        TOKEN_WALLET,
        TOKEN_ROBBIE,
    }
    assert {result.author_identity_key for result in results if result.matched} == {"nancy"}


def watched(token: str, symbol: str, *, chain: str = "robinhood") -> WatchedTokenIdentity:
    return WatchedTokenIdentity(chain=chain, contract_address=token, symbol=symbol)


def tweet(
    text: str,
    *,
    tweet_id: str = "tweet-1",
    author_id: str | None = "author-1",
    author_username: str | None = "NancyCrypto",
) -> NormalizedTweet:
    payload = {
        "id": tweet_id,
        "text": text,
        "createdAt": "2026-09-09T01:00:00Z",
        "author": {"id": author_id, "userName": author_username, "followers": 5000},
    }
    normalized = normalize_tweet(payload, detected_at=datetime(2026, 9, 9, 1, 0, 5, tzinfo=UTC))
    return NormalizedTweet(
        provider=PROVIDER,
        tweet_id=normalized.tweet_id,
        author_id=normalized.author_id,
        author_username=normalized.author_username,
        author_name=normalized.author_name,
        author_followers=normalized.author_followers,
        text=normalized.text,
        created_at=normalized.created_at,
        detected_at=normalized.detected_at + timedelta(seconds=0),
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
