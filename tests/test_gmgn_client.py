from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest

from app.services import gmgn_client as gmgn_module
from app.services.gmgn_client import (
    GmgnAuthError,
    GmgnApiError,
    GmgnClient,
    GmgnRateLimitError,
    _load_private_key_pem,
    parse_activity,
    parse_holder,
    parse_holding,
    parse_liquidity,
    parse_market_signal,
    parse_stats,
    parse_token_overview,
    parse_token_security,
    parse_track_trade,
)

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
WETH = "0x4200000000000000000000000000000000000006"
ZERO_ETH = "0x0000000000000000000000000000000000000000"


def make_client(responses: list[dict], status_codes: list[int] | None = None) -> GmgnClient:
    status_codes = status_codes or [200] * len(responses)
    seen = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        index = seen["count"]
        seen["count"] += 1
        return httpx.Response(
            status_codes[index],
            json=responses[index],
            request=request,
        )

    client = GmgnClient(
        api_key="test-api-key",
        private_key_pem="test-private-key",
        base_url="https://gmgn.test",
    )
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_holdings_response_parsing(monkeypatch) -> None:
    monkeypatch.setattr(gmgn_module, "_sign_message", lambda message, key: "signature")
    client = make_client(
        [
            {
                "code": 0,
                "data": {
                    "items": [
                        {
                            "token": {
                                "address": TOKEN,
                                "symbol": "WINK",
                                "name": "WINK",
                            },
                            "balance": "195245.079808140984102976",
                            "price_usd": "0.00019016",
                            "usd_value": "37.12",
                            "avg_cost": "0.00018",
                            "history_bought_cost": "50",
                            "history_sold_income": "10",
                            "realized_profit": "3",
                            "unrealized_profit": "-2",
                            "total_profit": "1",
                            "buy_tx_count": 2,
                            "sell_tx_count": 1,
                            "last_active_timestamp": 1770000000,
                        }
                    ]
                },
            }
        ]
    )

    holdings = await client.get_wallet_holdings("robinhood", WALLET)

    assert len(holdings) == 1
    assert holdings[0].symbol == "WINK"
    assert holdings[0].contract_address == TOKEN
    assert holdings[0].decimals is None
    assert holdings[0].balance == Decimal("195245.079808140984102976")
    assert holdings[0].avg_cost_usd == Decimal("0.00018")
    assert holdings[0].historical_bought_cost_usd == Decimal("50")
    assert holdings[0].historical_sold_income_usd == Decimal("10")
    await client.aclose()


@pytest.mark.asyncio
async def test_activity_buy_sell_transfer_parsing() -> None:
    client = make_client(
        [
            {
                "code": 0,
                "data": {
                    "items": [
                        eth_buy_fixture(),
                        token_sell_eth_fixture(),
                        usdg_buy_fixture(),
                        usdg_sell_fixture(),
                        weth_buy_fixture(),
                        transfer_in_fixture(),
                    ]
                },
            }
        ]
    )

    activities = await client.get_wallet_activity("robinhood", WALLET)

    assert [activity.activity_type for activity in activities] == [
        "buy",
        "sell",
        "buy",
        "sell",
        "buy",
        "transferIn",
    ]
    assert activities[0].quote_token == "ETH"
    assert activities[1].quote_token == "ETH"
    assert activities[2].quote_token == USDG
    assert activities[3].quote_token == USDG
    assert activities[4].quote_token == WETH
    assert activities[5].from_address == "0x2222222222222222222222222222222222222222"
    await client.aclose()


def test_empty_fields_parse_as_none() -> None:
    holding = parse_holding("robinhood", {"token": {"symbol": "EMPTY"}})
    activity = parse_activity("robinhood", {"type": "transferOut"})

    assert holding.balance is None
    assert holding.cost_usd is None
    assert activity.token_amount is None
    assert activity.tx_hash is None


def test_holding_price_source_field_tracks_actual_price_field() -> None:
    token_price = parse_holding("robinhood", {"token": {"price": "0.12", "address": TOKEN}})
    item_price = parse_holding("robinhood", {"price": "0.34", "token": {"price": "0.12", "address": TOKEN}})
    item_price_usd = parse_holding("robinhood", {"price_usd": "0.56", "token": {"address": TOKEN}})
    item_current_price = parse_holding("robinhood", {"current_price_usd": "0.78", "token": {"address": TOKEN}})

    assert token_price.current_price_usd == Decimal("0.12")
    assert token_price.price_source_field == "token.price"
    assert item_price.current_price_usd == Decimal("0.34")
    assert item_price.price_source_field == "item.price"
    assert item_price_usd.current_price_usd == Decimal("0.56")
    assert item_price_usd.price_source_field == "item.price_usd"
    assert item_current_price.current_price_usd == Decimal("0.78")
    assert item_current_price.price_source_field == "item.current_price_usd"


def test_token_overview_parser() -> None:
    overview = parse_token_overview(
        "robinhood",
        {
            "address": TOKEN,
            "symbol": "WINK",
            "name": "WinkCat",
            "circulating_supply": "1000000",
            "price": {"price": "0.0002"},
            "liquidity": "12345.6",
            "holder_count": 321,
            "stat": {"top_10_holder_rate": "0.12"},
            "wallet_tags_stat": {"smart_degen_wallets": 3, "renowned_wallets": 2},
            "dev": {"creator_address": "0x3333333333333333333333333333333333333333"},
            "creation_timestamp": 1770000000,
            "link": {"twitter_username": "wink", "telegram": "https://t.me/wink", "website": "https://wink.example"},
        },
    )

    assert overview.token_address == TOKEN
    assert overview.price_usd == Decimal("0.0002")
    assert overview.market_cap_usd == Decimal("200.0000")
    assert overview.top10_holder_rate == Decimal("0.12")
    assert overview.smart_money_count == 3
    assert overview.kol_count == 2
    assert overview.twitter == "wink"


def test_token_security_parser() -> None:
    security = parse_token_security(
        "robinhood",
        {
            "address": TOKEN,
            "is_honeypot": "no",
            "open_source": "yes",
            "owner": "0x4444444444444444444444444444444444444444",
            "owner_renounced": "unknown",
            "buy_tax": "0.01",
            "sell_tax": "0.02",
            "top_10_holder_rate": "0.12",
            "rug_ratio": "0.05",
            "is_wash_trading": False,
            "sniper_count": 7,
        },
    )

    assert security.is_honeypot == "no"
    assert security.buy_tax == Decimal("0.01")
    assert security.rug_ratio == Decimal("0.05")
    assert security.risk_flags["is_wash_trading"] is False
    assert security.risk_flags["sniper_count"] == 7


def test_liquidity_parser() -> None:
    liquidity = parse_liquidity(
        "robinhood",
        {
            "address": "0x5555555555555555555555555555555555555555",
            "base_address": TOKEN,
            "quote_address": USDG,
            "quote_symbol": "USDG",
            "exchange": "rhex",
            "liquidity": "9999.5",
            "base_reserve": "100",
            "quote_reserve": "50",
            "price": "0.5",
            "volume_24h": "1234",
        },
    )

    assert liquidity.token_address == TOKEN
    assert liquidity.dex == "rhex"
    assert liquidity.liquidity_usd == Decimal("9999.5")
    assert liquidity.volume_24h_usd == Decimal("1234")


def test_holder_smart_money_and_kol_parser() -> None:
    holder = parse_holder(
        "robinhood",
        {
            "address": WALLET,
            "balance": "100",
            "amount_percentage": "0.05",
            "usd_value": "25",
            "avg_cost": "0.2",
            "realized_profit": "3",
            "unrealized_profit": "-1",
            "buy_tx_count_cur": 2,
            "sell_tx_count_cur": 1,
            "start_holding_at": 1770000000,
            "tags": ["smart_degen", "renowned"],
            "maker_token_tags": ["top_holder"],
            "twitter_username": "holder_x",
        },
    )

    assert holder.wallet_address == WALLET
    assert holder.hold_percentage == Decimal("0.05")
    assert holder.tags == ["smart_degen", "renowned"]
    assert holder.maker_token_tags == ["top_holder"]
    assert holder.twitter_username == "holder_x"


def test_track_feed_parser() -> None:
    trade = parse_track_trade(track_feed_fixture())

    assert trade.wallet_address == WALLET
    assert trade.token_address == TOKEN
    assert trade.symbol == "WINK"
    assert trade.usd_value == Decimal("12.5")
    assert trade.wallet_tags == ["smart_degen"]


def test_kol_feed_parser() -> None:
    trade = parse_track_trade(
        {
            "chain": "robinhood",
            "transaction_hash": "0xkol",
            "maker": WALLET,
            "side": "sell",
            "base_address": TOKEN,
            "token_amount": "250",
            "amount_usd": "8.75",
            "price_usd": "0.035",
            "timestamp": 1770000010,
            "is_open_or_close": 1,
            "base_token": {"symbol": "WINK"},
            "maker_info": {
                "tags": ["renowned"],
                "twitter_username": "kol_x",
                "twitter_name": "KOL X",
            },
        },
    )

    assert trade.side == "sell"
    assert trade.open_or_close == 1
    assert trade.twitter_username == "kol_x"
    assert trade.twitter_name == "KOL X"


def test_market_signal_parser() -> None:
    signal = parse_market_signal(
        "robinhood",
        {
            "id": "signal-1",
            "token_address": TOKEN,
            "signal_type": 12,
            "trigger_at": 1770000000,
            "trigger_mc": "100000",
            "market_cap": "120000",
            "cur_data": {"liquidity": "20000", "holder_count": 500},
        },
    )

    assert signal.event_id == "signal-1"
    assert signal.token_address == TOKEN
    assert signal.signal_type == 12
    assert signal.trigger_market_cap_usd == Decimal("100000")
    assert signal.liquidity_usd == Decimal("20000")


@pytest.mark.asyncio
async def test_intelligence_methods_and_empty_results() -> None:
    client = make_client(
        [
            {"code": 0, "data": {"list": []}},
            {"code": 0, "data": {"list": []}},
            {"code": 0, "data": []},
        ]
    )

    holders = await client.get_token_holders("robinhood", TOKEN, tag="smart_degen")
    feed = await client.get_track_feed("smartmoney", chain="robinhood")
    signals = await client.get_market_signals("robinhood", groups=[{"signal_type": [12]}])

    assert holders == []
    assert feed == []
    assert signals == []
    await client.aclose()


@pytest.mark.asyncio
async def test_market_signal_body_and_api_error() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = request.content.decode()
        return httpx.Response(400, json={"code": 400, "error": "bad_group"}, request=request)

    client = GmgnClient(api_key="test-api-key", base_url="https://gmgn.test")
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(GmgnApiError):
        await client.get_market_signals("robinhood", groups=[{"signal_type": [14]}])

    assert seen["path"] == "/v1/market/token_signal"
    assert json.loads(seen["body"]) == {"chain": "robinhood", "groups": [{"signal_type": [14]}]}
    await client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_retry_after_is_obeyed(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(gmgn_module.asyncio, "sleep", fake_sleep)

    seen = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["count"] += 1
        if seen["count"] == 1:
            return httpx.Response(
                429,
                json={"code": 429, "error": "too many requests"},
                headers={
                    "Retry-After": "7",
                    "X-RateLimit-Limit": "20",
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": "1780000000",
                },
                request=request,
            )
        return httpx.Response(
            200,
            json={"code": 0, "data": {"list": [track_feed_fixture()]}},
            request=request,
        )

    client = GmgnClient(
        api_key="test-api-key",
        base_url="https://gmgn.test",
        rate_limit_max_retries=1,
    )
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    trades = await client.get_track_feed("smartmoney", chain="robinhood")

    assert sleeps == [7.0]
    assert len(trades) == 1
    assert trades[0].tx_hash == "0xabc"
    await client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_without_retry_after_uses_backoff(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(gmgn_module.asyncio, "sleep", fake_sleep)

    seen = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["count"] += 1
        if seen["count"] == 1:
            return httpx.Response(429, json={"code": 429}, request=request)
        return httpx.Response(200, json={"code": 0, "data": {"list": []}}, request=request)

    client = GmgnClient(
        api_key="test-api-key",
        base_url="https://gmgn.test",
        rate_limit_max_retries=1,
        rate_limit_backoff_seconds=(0.25,),
    )
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    assert await client.get_track_feed("smartmoney", chain="robinhood") == []
    assert sleeps == [0.25]
    await client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_max_retries_raises_and_is_not_empty(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(gmgn_module.asyncio, "sleep", fake_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"code": 429, "error": "rate limit"},
            headers={"X-RateLimit-Remaining": "0"},
            request=request,
        )

    client = GmgnClient(
        api_key="test-api-key",
        base_url="https://gmgn.test",
        rate_limit_max_retries=1,
        rate_limit_backoff_seconds=(0.1,),
    )
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(GmgnRateLimitError) as exc_info:
        await client.get_track_feed("smartmoney", chain="robinhood")

    assert "HTTP 429" in str(exc_info.value)
    assert exc_info.value.remaining == "0"
    assert sleeps == [0.1]
    await client.aclose()


def test_robinhood_holdings_raw_fields_parse_without_inventing_cost() -> None:
    holding = parse_holding(
        "robinhood",
        {
            "accu_amount": "249045.742548186937738568",
            "accu_cost": "67.75069058109",
            "balance": "249045.742548186937738568",
            "history_bought_cost": "94.00797159534",
            "history_sold_income": "34.06223542770979209609",
            "history_total_buys": 5,
            "history_total_sells": 2,
            "last_active_timestamp": 1787734069,
            "realized_profit": "7.12593811942703811134",
            "total_profit": "-3.34699798992900288866",
            "unrealized_profit": "-10.472936109356041",
            "usd_value": "58.63848254464579266780022019088",
            "token": {
                "decimals": 18,
                "name": "WinkCat",
                "price": "0.00023545266",
                "symbol": "WINK",
                "token_address": TOKEN,
            },
        },
    )

    assert holding.decimals == 18
    assert holding.balance == Decimal("249045.742548186937738568")
    assert holding.current_price_usd == Decimal("0.00023545266")
    assert holding.usd_value == Decimal("58.63848254464579266780022019088")
    assert holding.cost_usd is None
    assert holding.avg_cost_usd is None
    assert holding.historical_bought_cost_usd == Decimal("94.00797159534")
    assert holding.historical_sold_income_usd == Decimal("34.06223542770979209609")
    assert holding.buy_tx_count == 5
    assert holding.sell_tx_count == 2
    assert holding.realized_profit_usd == Decimal("7.12593811942703811134")
    assert holding.unrealized_profit_usd == Decimal("-10.472936109356041")
    assert holding.total_profit_usd == Decimal("-3.34699798992900288866")


def test_holding_cost_and_avg_cost_parse_only_when_returned() -> None:
    holding = parse_holding(
        "robinhood",
        {
            "token": {"address": TOKEN, "symbol": "WINK"},
            "balance": "10",
            "cost": "7.5",
            "avg_cost": "0.75",
        },
    )

    assert holding.cost_usd == Decimal("7.5")
    assert holding.avg_cost_usd == Decimal("0.75")


def test_stats_nested_and_robinhood_top_level_fields_parse() -> None:
    stats = parse_stats(
        "robinhood",
        WALLET,
        {
            "buy": 55,
            "sell": 75,
            "realized_profit": "102.97894799543606842526",
            "pnl_stat": {"winrate": 0.5483870967741935},
        },
    )

    assert stats.realized_profit_usd == Decimal("102.97894799543606842526")
    assert stats.unrealized_profit_usd is None
    assert stats.total_profit_usd is None
    assert stats.win_rate == Decimal("0.5483870967741935")
    assert stats.buy_tx_count == 55
    assert stats.sell_tx_count == 75


def test_activity_cost_is_transaction_value_usd() -> None:
    activity = parse_activity(
        "robinhood",
        {
            "type": "sell",
            "token": {"address": TOKEN, "symbol": "WINK"},
            "amount": "400",
            "cost": "4.8",
            "quote_address": ZERO_ETH,
            "quote_amount": "0.0019",
        },
    )

    assert activity.transaction_value_usd == Decimal("4.8")
    assert activity.cost_usd == Decimal("4.8")


@pytest.mark.asyncio
async def test_api_auth_error() -> None:
    client = make_client(
        [{"code": 401, "error": "unauthorized", "message": "bad key"}],
        status_codes=[403],
    )

    with pytest.raises(GmgnAuthError):
        await client.get_wallet_activity("robinhood", WALLET)
    await client.aclose()


@pytest.mark.asyncio
async def test_pagination(monkeypatch) -> None:
    monkeypatch.setattr(gmgn_module, "_sign_message", lambda message, key: "signature")
    client = make_client(
        [
            {
                "code": 0,
                "data": {
                    "items": [{"token": {"address": TOKEN, "symbol": "WINK"}, "balance": "1"}],
                    "next_cursor": "cursor-2",
                },
            },
            {
                "code": 0,
                "data": {
                    "items": [{"token": {"address": USDG, "symbol": "USDG"}, "balance": "2"}]
                },
            },
        ]
    )

    holdings = await client.get_wallet_holdings("robinhood", WALLET, max_pages=2)

    assert [holding.symbol for holding in holdings] == ["WINK", "USDG"]
    await client.aclose()


def test_partial_sell_and_closed_holding_fixtures() -> None:
    partial = parse_holding(
        "robinhood",
        {
            "token": {"address": TOKEN, "symbol": "WINK"},
            "balance": "600",
            "history_bought_cost": "100",
            "history_sold_income": "40",
            "realized_profit": "5",
            "unrealized_profit": "7",
        },
    )
    closed = parse_holding(
        "robinhood",
        {
            "token": {"address": TOKEN, "symbol": "WINK"},
            "balance": "0",
            "history_bought_cost": "100",
            "history_sold_income": "110",
            "realized_profit": "10",
        },
    )

    assert partial.balance == Decimal("600")
    assert partial.realized_profit_usd == Decimal("5")
    assert partial.unrealized_profit_usd == Decimal("7")
    assert closed.balance == Decimal("0")
    assert closed.realized_profit_usd == Decimal("10")


def test_load_private_key_path_extracts_private_pem(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("GMGN_PRIVATE_KEY", raising=False)
    key_file = tmp_path / "keypair.pem"
    key_file.write_text(
        "\n".join(
            [
                "# Private Key",
                "-----BEGIN PRIVATE KEY-----",
                "abc",
                "-----END PRIVATE KEY-----",
                "# Public Key",
                "-----BEGIN PUBLIC KEY-----",
                "def",
                "-----END PUBLIC KEY-----",
            ]
        ),
        encoding="utf-8",
    )

    assert _load_private_key_pem(str(key_file)) == "\n".join(
        ["-----BEGIN PRIVATE KEY-----", "abc", "-----END PRIVATE KEY-----"]
    )


def eth_buy_fixture() -> dict:
    return {
        "type": "buy",
        "token": {"address": TOKEN, "symbol": "WINK", "name": "WINK"},
        "amount": "1000",
        "price_usd": "0.01",
        "cost_usd": "10",
        "quote_symbol": "ETH",
        "quote_amount": "0.004",
        "tx_hash": "0xethbuy",
        "timestamp": 1770000001,
    }


def token_sell_eth_fixture() -> dict:
    return {
        "type": "sell",
        "token": {"address": TOKEN, "symbol": "WINK"},
        "amount": "400",
        "price_usd": "0.012",
        "cost_usd": "4.8",
        "quote_symbol": "ETH",
        "quote_amount": "0.0019",
        "tx_hash": "0xethsell",
        "timestamp": 1770000002,
    }


def usdg_buy_fixture() -> dict:
    return {
        "type": "buy",
        "token": {"address": TOKEN, "symbol": "WINK"},
        "amount": "500",
        "cost_usd": "5",
        "quote": {"address": USDG, "symbol": "USDG", "amount": "5"},
        "tx_hash": "0xusdgbuy",
    }


def usdg_sell_fixture() -> dict:
    return {
        "type": "sell",
        "token": {"address": TOKEN, "symbol": "WINK"},
        "amount": "200",
        "cost_usd": "3",
        "quote": {"address": USDG, "symbol": "USDG", "amount": "3"},
        "tx_hash": "0xusdgsell",
    }


def weth_buy_fixture() -> dict:
    return {
        "type": "buy",
        "token": {"address": TOKEN, "symbol": "WINK"},
        "amount": "100",
        "quote": {"address": WETH, "symbol": "WETH", "amount": "0.001"},
        "tx_hash": "0xwethbuy",
    }


def transfer_in_fixture() -> dict:
    return {
        "type": "transferIn",
        "token": {"address": TOKEN, "symbol": "WINK"},
        "amount": "1",
        "from": "0x2222222222222222222222222222222222222222",
        "to": WALLET,
        "tx_hash": "0xtransferin",
    }


def track_feed_fixture() -> dict:
    return {
        "chain": "robinhood",
        "transaction_hash": "0xabc",
        "maker": WALLET,
        "side": "buy",
        "base_address": TOKEN,
        "token_amount": "1000",
        "amount_usd": "12.5",
        "price_usd": "0.0125",
        "timestamp": 1770000000,
        "is_open_or_close": 0,
        "base_token": {"symbol": "WINK"},
        "maker_info": {"tags": ["smart_degen"], "twitter_username": "smart_x"},
    }
