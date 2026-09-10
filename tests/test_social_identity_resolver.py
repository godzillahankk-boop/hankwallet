from __future__ import annotations

from scripts.social_identity_resolver_probe import _project_x_equals_dev_x

from app.services.social_identity_resolver import resolve_social_identities


TOKEN = "0xe0eba1b76b73be7bfa7716b6ca96f724930e2263"
CREATOR = "0x1111111111111111111111111111111111111111"


def test_project_x_explicitly_identified_high_confidence() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"link": {"twitter_username": "RobbieOnChain"}},
    )

    project = result.by_type("project_x")[0]
    assert project.value == "RobbieOnChain"
    assert project.source == "gmgn_token_info"
    assert project.source_field == "link.twitter_username"
    assert project.confidence == "HIGH"


def test_project_x_is_not_automatically_dev_x() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"link": {"twitter_username": "RobbieOnChain"}},
    )

    assert result.by_type("project_x")
    assert result.by_type("dev_x") == []


def test_creator_wallet_identified_high_confidence() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"dev": {"creator_address": CREATOR}},
    )

    dev_wallet = result.by_type("dev_wallet")[0]
    assert dev_wallet.value == CREATOR
    assert dev_wallet.source_field == "dev.creator_address"
    assert dev_wallet.confidence == "HIGH"


def test_wallet_twitter_mapping_becomes_high_confidence_dev_x() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"dev": {"creator_address": CREATOR}},
        dev_holder_items=[
            {
                "address": CREATOR,
                "twitter_username": "creator_x",
                "twitter_name": "Creator X",
                "tags": ["dev"],
            }
        ],
    )

    dev_x = result.by_type("dev_x")[0]
    assert dev_x.value == "creator_x"
    assert dev_x.source == "gmgn_token_holders"
    assert dev_x.source_field == "holder.twitter_username"
    assert dev_x.confidence == "HIGH"
    assert dev_x.evidence["wallet_address"] == CREATOR


def test_holder_twitter_for_non_creator_is_not_dev_x() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"dev": {"creator_address": CREATOR}},
        dev_holder_items=[
            {
                "address": "0x2222222222222222222222222222222222222222",
                "twitter_username": "not_creator",
            }
        ],
    )

    assert result.by_type("dev_x") == []


def test_launchpad_creator_twitter_is_medium_confidence_dev_x() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={
            "dev": {"creator_address": CREATOR},
            "fee_distribution": {
                "launchpad": "bankr",
                "platform_data": {
                    "list": [
                        {
                            "wallet": CREATOR,
                            "is_creator": True,
                            "username": "Creator",
                            "twitter_username": "creator_x",
                        }
                    ]
                },
            },
        },
    )

    dev_x = result.by_type("dev_x")[0]
    assert dev_x.value == "creator_x"
    assert dev_x.confidence == "MEDIUM"
    assert dev_x.source_field == "fee_distribution.platform_data.list[].twitter_username"


def test_no_dev_x_without_explicit_wallet_or_launchpad_mapping() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={"dev": {"creator_address": CREATOR}, "link": {"twitter_username": "project_x"}},
    )

    assert result.by_type("dev_x") == []


def test_missing_fields_are_safe() -> None:
    result = resolve_social_identities(token_address=TOKEN, symbol=None, token_info=None)

    assert result.candidates == []
    assert result.raw_fields == {
        "probe_error": None,
        "link": {},
        "dev": {},
        "stat": {},
        "fee_distribution": {},
    }


def test_identity_dedupe() -> None:
    result = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={
            "fee_distribution": {
                "launchpad": "bankr",
                "platform_data": {
                    "list": [
                        {
                            "wallet": CREATOR,
                            "is_creator": True,
                            "twitter_username": "creator_x",
                        },
                        {
                            "wallet": CREATOR,
                            "is_creator": True,
                            "twitter_username": "creator_x",
                        },
                    ]
                },
            }
        },
    )

    assert len(result.by_type("dev_x")) == 1
    assert len(result.by_type("dev_wallet")) == 1


def test_project_x_equals_dev_x_requires_matching_username() -> None:
    different = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={
            "link": {"twitter_username": "project_x"},
            "dev": {"creator_address": CREATOR},
            "fee_distribution": {
                "launchpad": "bankr",
                "platform_data": {
                    "list": [
                        {
                            "wallet": CREATOR,
                            "is_creator": True,
                            "twitter_username": "creator_x",
                        }
                    ]
                },
            },
        },
    )
    same = resolve_social_identities(
        token_address=TOKEN,
        symbol="ROBBIE",
        token_info={
            "link": {"twitter_username": "Creator_X"},
            "dev": {"creator_address": CREATOR},
            "fee_distribution": {
                "launchpad": "bankr",
                "platform_data": {
                    "list": [
                        {
                            "wallet": CREATOR,
                            "is_creator": True,
                            "twitter_username": "creator_x",
                        }
                    ]
                },
            },
        },
    )

    assert _project_x_equals_dev_x(different) is False
    assert _project_x_equals_dev_x(same) is True
