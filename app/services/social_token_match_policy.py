from __future__ import annotations

from dataclasses import dataclass

from app.services.social_identity_service import normalize_username
from app.services.social_token_matcher import TokenMatch
from app.services.twitterapi_io_client import NormalizedTweet
from app.utils.address import normalize_evm_address

MATCH_STATUS_MATCHED = "matched"
MATCH_STATUS_AMBIGUOUS_SYMBOL = "ambiguous_symbol"
MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR = "ignored_unqualified_author"
MATCH_STATUS_UNMATCHED = "unmatched"

MATCH_TYPE_EXACT_CA = "exact_ca"
MATCH_TYPE_EXACT_CA_AND_CASHTAG = "exact_ca_and_cashtag"
MATCH_TYPE_CASHTAG = "cashtag"

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"

IDENTITY_SCOPE_CONTRACT = "contract"
IDENTITY_SCOPE_SYMBOL_OR_PROJECT = "symbol_or_project"


@dataclass(frozen=True)
class WatchedTokenIdentity:
    chain: str
    contract_address: str
    symbol: str

    @property
    def normalized_contract_address(self) -> str:
        return normalize_evm_address(self.contract_address)

    @property
    def normalized_symbol(self) -> str:
        return (self.symbol or "").upper()


@dataclass(frozen=True)
class SocialTokenMatchResult:
    matched: bool
    match_status: str
    match_type: str | None = None
    confidence: str | None = None
    identity_scope: str | None = None
    chain: str | None = None
    contract_address: str | None = None
    symbol: str | None = None
    author_username: str | None = None
    author_qualified_kol: bool = False
    author_identity_key: str | None = None
    candidate_count: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "matched": self.matched,
            "match_status": self.match_status,
            "match_type": self.match_type,
            "confidence": self.confidence,
            "identity_scope": self.identity_scope,
            "chain": self.chain,
            "contract_address": self.contract_address,
            "symbol": self.symbol,
            "author_username": self.author_username,
            "author_qualified_kol": self.author_qualified_kol,
            "author_identity_key": self.author_identity_key,
            "candidate_count": self.candidate_count,
        }


def match_social_token(
    tweet: NormalizedTweet,
    watched_tokens: list[WatchedTokenIdentity],
    *,
    author_qualified_kol: bool,
) -> SocialTokenMatchResult:
    return match_social_tokens(
        tweet,
        watched_tokens,
        author_qualified_kol=author_qualified_kol,
    )[0]


def match_social_tokens(
    tweet: NormalizedTweet,
    watched_tokens: list[WatchedTokenIdentity],
    *,
    author_qualified_kol: bool,
) -> list[SocialTokenMatchResult]:
    tokens = [token for token in watched_tokens if token.normalized_contract_address and token.normalized_symbol]
    author_key = kol_distinct_author_key(tweet.author_id, tweet.author_username)
    results: list[SocialTokenMatchResult] = []
    exact_tokens = _exact_ca_matches(tweet.token_matches, tokens)
    exact_contracts = {token.normalized_contract_address for token in exact_tokens}
    exact_symbols = {token.normalized_symbol for token in exact_tokens}
    for exact_match in exact_tokens:
        match_type = (
            MATCH_TYPE_EXACT_CA_AND_CASHTAG
            if _has_symbol_cashtag(tweet.token_matches, exact_match.normalized_symbol)
            else MATCH_TYPE_EXACT_CA
        )
        results.append(
            _matched_result(
                exact_match,
                match_type=match_type,
                confidence=CONFIDENCE_HIGH,
                identity_scope=IDENTITY_SCOPE_CONTRACT,
                tweet=tweet,
                author_qualified_kol=author_qualified_kol,
                author_key=author_key,
            )
        )

    symbol_candidates = _tokens_by_symbol(tokens)
    seen_cashtag_symbols: set[str] = set()
    for match in tweet.token_matches:
        if not match.match_type.endswith("_cashtag"):
            continue
        symbol = match.value.upper()
        if symbol in seen_cashtag_symbols or symbol in exact_symbols:
            continue
        seen_cashtag_symbols.add(symbol)
        candidates = symbol_candidates.get(symbol, [])
        if not candidates:
            continue
        candidates_without_exact = [
            token for token in candidates if token.normalized_contract_address not in exact_contracts
        ]
        if not candidates_without_exact:
            continue
        if not author_qualified_kol:
            results.append(
                SocialTokenMatchResult(
                    matched=False,
                    match_status=MATCH_STATUS_IGNORED_UNQUALIFIED_AUTHOR,
                    symbol=symbol,
                    author_username=tweet.author_username,
                    author_qualified_kol=False,
                    author_identity_key=author_key,
                    candidate_count=len(candidates_without_exact),
                )
            )
            continue
        if len(candidates_without_exact) > 1:
            results.append(
                SocialTokenMatchResult(
                    matched=False,
                    match_status=MATCH_STATUS_AMBIGUOUS_SYMBOL,
                    symbol=symbol,
                    author_username=tweet.author_username,
                    author_qualified_kol=True,
                    author_identity_key=author_key,
                    candidate_count=len(candidates_without_exact),
                )
            )
            continue
        results.append(
            _matched_result(
                candidates_without_exact[0],
                match_type=MATCH_TYPE_CASHTAG,
                confidence=CONFIDENCE_MEDIUM,
                identity_scope=IDENTITY_SCOPE_SYMBOL_OR_PROJECT,
                tweet=tweet,
                author_qualified_kol=True,
                author_key=author_key,
            )
        )

    if results:
        return results
    return [
        SocialTokenMatchResult(
            matched=False,
            match_status=MATCH_STATUS_UNMATCHED,
            author_username=tweet.author_username,
            author_qualified_kol=author_qualified_kol,
            author_identity_key=author_key,
        )
    ]


def kol_distinct_author_key(author_id: str | None, author_username: str | None) -> str | None:
    if author_id:
        return str(author_id)
    username = normalize_username(author_username)
    return username.lower() if username else None


def _exact_ca_matches(
    matches: list[TokenMatch],
    watched_tokens: list[WatchedTokenIdentity],
) -> list[WatchedTokenIdentity]:
    by_contract = {
        token.normalized_contract_address: token
        for token in watched_tokens
    }
    results: list[WatchedTokenIdentity] = []
    seen_contracts: set[str] = set()
    for match in matches:
        if not match.match_type.endswith("_ca"):
            continue
        contract = normalize_evm_address(match.value)
        if contract in seen_contracts:
            continue
        token = by_contract.get(contract)
        if token:
            seen_contracts.add(contract)
            results.append(token)
    return results


def _has_symbol_cashtag(matches: list[TokenMatch], symbol: str) -> bool:
    return any(match.match_type.endswith("_cashtag") and match.value.upper() == symbol for match in matches)


def _tokens_by_symbol(tokens: list[WatchedTokenIdentity]) -> dict[str, list[WatchedTokenIdentity]]:
    by_symbol: dict[str, dict[tuple[str, str], WatchedTokenIdentity]] = {}
    for token in tokens:
        by_symbol.setdefault(token.normalized_symbol, {})[
            (token.chain, token.normalized_contract_address)
        ] = token
    return {symbol: list(tokens_by_identity.values()) for symbol, tokens_by_identity in by_symbol.items()}


def _matched_result(
    token: WatchedTokenIdentity,
    *,
    match_type: str,
    confidence: str,
    identity_scope: str,
    tweet: NormalizedTweet,
    author_qualified_kol: bool,
    author_key: str | None,
) -> SocialTokenMatchResult:
    return SocialTokenMatchResult(
        matched=True,
        match_status=MATCH_STATUS_MATCHED,
        match_type=match_type,
        confidence=confidence,
        identity_scope=identity_scope,
        chain=token.chain,
        contract_address=token.normalized_contract_address,
        symbol=token.normalized_symbol,
        author_username=tweet.author_username,
        author_qualified_kol=author_qualified_kol,
        author_identity_key=author_key,
    )
