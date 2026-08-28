import pytest

from scripts.intelligence_capability_probe import (
    capability_endpoints,
    needs_token,
    parse_capability_selection,
)


def test_capability_selector_keeps_only_selected_endpoint() -> None:
    capabilities = parse_capability_selection(["smartmoney-feed"], "")

    assert capabilities == ["smartmoney-feed"]
    assert capability_endpoints(capabilities) == ["/v1/user/smartmoney"]


def test_capability_selector_supports_csv_and_dedupes() -> None:
    capabilities = parse_capability_selection(
        ["smartmoney-feed"],
        "kol-feed,market-signal,smartmoney-feed",
    )

    assert capabilities == ["smartmoney-feed", "kol-feed", "market-signal"]
    assert capability_endpoints(capabilities) == [
        "/v1/user/smartmoney",
        "/v1/user/kol",
        "/v1/market/token_signal",
    ]


def test_capability_selector_requires_explicit_selection() -> None:
    with pytest.raises(ValueError):
        parse_capability_selection([], "")


def test_holder_capabilities_require_token() -> None:
    assert needs_token(["smartmoney-holder"])
    assert needs_token(["kol-holder"])
    assert not needs_token(["smartmoney-feed", "market-signal"])
