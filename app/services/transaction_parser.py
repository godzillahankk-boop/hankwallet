from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from app.services.chain_client import TokenTransfer
from app.utils.address import normalize_evm_address


QUOTE_TOKEN_SYMBOLS = {
    "ETH",
    "WETH",
    "USDC",
    "USDT",
    "USDG",
    "DAI",
    "WBTC",
}

SPAM_NAME_MARKERS = {
    "airdrop",
    "claim",
    "reward",
    "voucher",
    "visit",
    "http",
    "www.",
    ".com",
}


@dataclass(frozen=True)
class ParsedTokenEvent:
    tx_hash: str
    tx_type: str
    token_address: str
    token_symbol: str | None
    token_name: str | None
    token_decimals: int
    token_amount: Decimal
    quote_token: str | None
    quote_amount: Decimal | None
    block_number: int | None
    tx_timestamp: object | None


def parse_wallet_token_events(
    wallet_address: str, transfers: list[TokenTransfer]
) -> list[ParsedTokenEvent]:
    wallet = normalize_evm_address(wallet_address)
    by_tx: dict[str, list[TokenTransfer]] = defaultdict(list)
    for transfer in transfers:
        by_tx[transfer.tx_hash.lower()].append(transfer)

    events: list[ParsedTokenEvent] = []
    for tx_hash, tx_transfers in by_tx.items():
        incoming = [
            t for t in tx_transfers if t.to_address and normalize_evm_address(t.to_address) == wallet
        ]
        outgoing = [
            t
            for t in tx_transfers
            if t.from_address and normalize_evm_address(t.from_address) == wallet
        ]
        outgoing_quote = [t for t in outgoing if is_quote_token(t.token_symbol)]
        incoming_quote = [t for t in incoming if is_quote_token(t.token_symbol)]

        for transfer in incoming:
            if is_quote_token(transfer.token_symbol):
                continue
            if outgoing_quote:
                quote = outgoing_quote[0]
                tx_type = "BUY"
                quote_token = quote.token_symbol
                quote_amount = quote.amount
            else:
                tx_type = "TRANSFER_IN"
                quote_token = None
                quote_amount = None
            events.append(_event(transfer, tx_hash, tx_type, transfer.amount, quote_token, quote_amount))

        for transfer in outgoing:
            if is_quote_token(transfer.token_symbol):
                continue
            if incoming_quote:
                quote = incoming_quote[0]
                tx_type = "SELL"
                quote_token = quote.token_symbol
                quote_amount = quote.amount
            else:
                tx_type = "TRANSFER_OUT"
                quote_token = None
                quote_amount = None
            events.append(_event(transfer, tx_hash, tx_type, transfer.amount, quote_token, quote_amount))

    return events


def is_quote_token(symbol: str | None) -> bool:
    return bool(symbol and symbol.upper() in QUOTE_TOKEN_SYMBOLS)


def looks_like_spam_token(symbol: str | None, name: str | None) -> bool:
    text = f"{symbol or ''} {name or ''}".lower()
    return any(marker in text for marker in SPAM_NAME_MARKERS)


def _event(
    transfer: TokenTransfer,
    tx_hash: str,
    tx_type: str,
    token_amount: Decimal,
    quote_token: str | None,
    quote_amount: Decimal | None,
) -> ParsedTokenEvent:
    return ParsedTokenEvent(
        tx_hash=tx_hash,
        tx_type=tx_type,
        token_address=transfer.token_address,
        token_symbol=transfer.token_symbol,
        token_name=transfer.token_name,
        token_decimals=transfer.token_decimals,
        token_amount=token_amount,
        quote_token=quote_token,
        quote_amount=quote_amount,
        block_number=transfer.block_number,
        tx_timestamp=transfer.timestamp,
    )
