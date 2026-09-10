from __future__ import annotations

from app.services.social_token_matcher import extract_token_matches


TOKEN_CA = "0xb6ec6e6e58354d956ef1a2ed6693d5c7056298d2"


def test_extracts_evm_ca_lowercase_for_matching() -> None:
    mixed_case = "0xB6eC6e6E58354D956eF1A2ED6693D5c7056298D2"

    matches = extract_token_matches(f"CA {mixed_case}")

    assert matches[0].match_type == "direct_ca"
    assert matches[0].value == TOKEN_CA
    assert matches[0].raw_value == mixed_case


def test_extracts_cashtag_and_normalizes_symbol() -> None:
    matches = extract_token_matches("Watching $Pons and $robbie")

    assert [(match.match_type, match.value, match.raw_value) for match in matches] == [
        ("direct_cashtag", "PONS", "$Pons"),
        ("direct_cashtag", "ROBBIE", "$robbie"),
    ]


def test_does_not_treat_dollar_amount_as_ticker() -> None:
    matches = extract_token_matches("Bought $10 and later $100, not a ticker.")

    assert matches == []


def test_cashtag_requires_dollar_prefix() -> None:
    matches = extract_token_matches("PONS without a cashtag should not match.")

    assert matches == []


def test_dedupes_matches_per_type_and_value() -> None:
    upper_body = "0x" + TOKEN_CA[2:].upper()
    matches = extract_token_matches(f"$PONS $pons {TOKEN_CA} {upper_body}")

    assert [(match.match_type, match.value) for match in matches] == [
        ("direct_ca", TOKEN_CA),
        ("direct_cashtag", "PONS"),
    ]
