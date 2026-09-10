from decimal import Decimal

from app.services.chain_client import TokenTransfer
from app.services.transaction_parser import parse_wallet_token_events


WALLET = "0x1111111111111111111111111111111111111111"
PAIR = "0x2222222222222222222222222222222222222222"
TOKEN = "0x3333333333333333333333333333333333333333"
USDG = "0x4444444444444444444444444444444444444444"
WETH = "0x4200000000000000000000000000000000000006"


def transfer(tx_hash, token, symbol, from_address, to_address, amount):
    return TokenTransfer(
        chain="robinhood",
        tx_hash=tx_hash,
        token_address=token,
        token_symbol=symbol,
        token_name=symbol,
        token_decimals=18,
        from_address=from_address,
        to_address=to_address,
        amount=Decimal(amount),
        block_number=1,
        timestamp=None,
    )


def test_usdg_out_and_token_in_is_buy() -> None:
    events = parse_wallet_token_events(
        WALLET,
        [
            transfer("0xbuy", USDG, "USDG", WALLET, PAIR, "10"),
            transfer("0xbuy", TOKEN, "BRODIE", PAIR, WALLET, "5000"),
        ],
    )

    assert len(events) == 1
    assert events[0].tx_type == "BUY"
    assert events[0].token_symbol == "BRODIE"
    assert events[0].quote_token == "USDG"


def test_token_out_and_usdg_in_is_sell() -> None:
    events = parse_wallet_token_events(
        WALLET,
        [
            transfer("0xsell", TOKEN, "POOLS", WALLET, PAIR, "1000"),
            transfer("0xsell", USDG, "USDG", PAIR, WALLET, "20"),
        ],
    )

    assert len(events) == 1
    assert events[0].tx_type == "SELL"
    assert events[0].token_symbol == "POOLS"
    assert events[0].quote_token == "USDG"


def test_eth_out_and_token_in_is_buy() -> None:
    events = parse_wallet_token_events(
        WALLET,
        [
            transfer("0xbuyeth", WETH, "ETH", WALLET, PAIR, "0.05"),
            transfer("0xbuyeth", TOKEN, "BRODIE", PAIR, WALLET, "5000"),
        ],
    )

    assert len(events) == 1
    assert events[0].tx_type == "BUY"
    assert events[0].token_symbol == "BRODIE"
    assert events[0].quote_token == "ETH"


def test_token_out_and_eth_in_is_sell() -> None:
    events = parse_wallet_token_events(
        WALLET,
        [
            transfer("0xselleth", TOKEN, "BRODIE", WALLET, PAIR, "5000"),
            transfer("0xselleth", WETH, "ETH", PAIR, WALLET, "0.05"),
        ],
    )

    assert len(events) == 1
    assert events[0].tx_type == "SELL"
    assert events[0].token_symbol == "BRODIE"
    assert events[0].quote_token == "ETH"
