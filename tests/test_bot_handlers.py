from __future__ import annotations

import pytest

from app.bot.handlers import fetch_gmgn_position_holdings
from app.services.gmgn_client import parse_holding


class FakePriceGuardianService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def fetch_all_holdings(self, chain: str, address: str):
        self.calls.append((chain, address))
        return [
            parse_holding(
                chain,
                {
                    "balance": "1",
                    "history_total_buys": 1,
                    "token": {
                        "symbol": "BUY",
                        "token_address": "0x1111111111111111111111111111111111111111",
                    },
                },
            )
        ]


@pytest.mark.asyncio
async def test_fetch_gmgn_position_holdings_uses_price_guardian_not_blockscout() -> None:
    service = FakePriceGuardianService()

    holdings = await fetch_gmgn_position_holdings(
        [(1, "robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67")],
        service,
    )

    assert service.calls == [("robinhood", "0x0e712f06daeab2e866b1477923764af2fc1a9f67")]
    assert holdings[0].symbol == "BUY"
