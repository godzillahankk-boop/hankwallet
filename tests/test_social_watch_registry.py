from __future__ import annotations

import json
from datetime import timedelta

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialIdentity, TokenWatchState
from app.services.social_identity_service import DEV_X, HIGH, PROJECT_X
from app.services.social_watch_registry import (
    WATCH_DEV_X,
    WATCH_FIXED_KOL,
    WATCH_PROJECT_X,
    SocialWatchAccount,
    SocialWatchRegistry,
    load_fixed_kol_usernames,
)
from app.utils.time import utc_now

TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/registry.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    config_path = tmp_path / "social_kols.json"
    config_path.write_text(
        json.dumps(
            {
                "description": "Wallet Agent Fixed KOL Pool V1",
                "kols": [
                    {"username": "lookonchain", "enabled": True},
                    {"username": "@whale_alert", "enabled": True},
                    {"username": "DisabledKOL", "enabled": False},
                ],
            }
        )
    )
    return session_factory, config_path


def add_identity(session_factory, *, identity_type: str, value: str, active: bool = True, token: str = TOKEN) -> None:
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=token,
                symbol="ROBBIE",
                identity_type=identity_type,
                value=value,
                normalized_value=value.lstrip("@").lower(),
                source="test",
                source_field="test",
                confidence=HIGH,
                evidence_json="{}",
                is_active=active,
                first_seen_at=now - timedelta(minutes=5),
                last_verified_at=now,
                valid_from=now - timedelta(minutes=5),
            )
        )


def add_watch(session_factory, *, token: str = TOKEN, symbol: str = "ROBBIE", active: bool = True) -> int:
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=1,
                chain="robinhood",
                token_address=token,
                symbol=symbol,
                active=active,
                started_at=now - timedelta(minutes=10),
                last_seen_at=now,
                ended_at=None if active else now,
            )
        )
    return 1


def test_fixed_kol_config_loads_enabled_usernames(ctx) -> None:
    _, config_path = ctx

    assert load_fixed_kol_usernames(config_path) == ["lookonchain", "whale_alert"]


def test_watch_registry_includes_fixed_project_and_dev_x_but_not_inactive(ctx) -> None:
    session_factory, config_path = ctx
    add_watch(session_factory)
    add_identity(session_factory, identity_type=PROJECT_X, value="ProjectUser")
    add_identity(session_factory, identity_type=DEV_X, value="DevUser")
    add_identity(session_factory, identity_type=PROJECT_X, value="InactiveUser", active=False)

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()

    assert [(account.username, account.watch_type) for account in accounts] == [
        ("DevUser", WATCH_DEV_X),
        ("lookonchain", WATCH_FIXED_KOL),
        ("ProjectUser", WATCH_PROJECT_X),
        ("whale_alert", WATCH_FIXED_KOL),
    ]


def test_watch_registry_dedupes_usernames_case_insensitive_and_fingerprint_stable(ctx) -> None:
    session_factory, config_path = ctx
    add_watch(session_factory)
    add_identity(session_factory, identity_type=PROJECT_X, value="LookOnChain")

    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)
    first = registry.accounts()
    second = list(reversed(first))

    assert [account.normalized_username for account in first].count("lookonchain") == 1
    assert registry.fingerprint(first) == registry.fingerprint(second)
    assert registry.rule_value(first).startswith("from:")


def test_registry_ignores_non_x_identity_types(ctx) -> None:
    session_factory, config_path = ctx
    now = utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialIdentity(
                chain="robinhood",
                token_address=TOKEN,
                symbol="ROBBIE",
                identity_type="dev_wallet",
                value="0xbfcc000000000000000000000000000000000001",
                normalized_value="0xbfcc000000000000000000000000000000000001",
                source="test",
                source_field="test",
                confidence=HIGH,
                is_active=True,
                first_seen_at=now,
                last_verified_at=now,
                valid_from=now,
            )
        )

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()

    assert {account.watch_type for account in accounts} == {WATCH_FIXED_KOL}
    with session_scope(session_factory) as session:
        assert session.scalar(select(SocialIdentity).where(SocialIdentity.identity_type == "dev_wallet")) is not None


def test_dynamic_identity_requires_active_watch_and_identity_stays_active_after_watch_ends(ctx) -> None:
    session_factory, config_path = ctx
    add_watch(session_factory)
    add_identity(session_factory, identity_type=PROJECT_X, value="RobbieOnRH")
    registry = SocialWatchRegistry(session_factory, kol_config_path=config_path)

    assert "robbieonrh" in {account.normalized_username for account in registry.accounts()}

    with session_scope(session_factory) as session:
        watch = session.scalar(select(TokenWatchState).where(TokenWatchState.token_address == TOKEN))
        watch.active = False
        watch.ended_at = utc_now()

    assert "robbieonrh" not in {account.normalized_username for account in registry.accounts()}
    with session_scope(session_factory) as session:
        identity = session.scalar(select(SocialIdentity).where(SocialIdentity.normalized_value == "robbieonrh"))
        assert identity.is_active is True

    add_watch(session_factory)

    assert "robbieonrh" in {account.normalized_username for account in registry.accounts()}


def test_dev_x_also_requires_active_watch(ctx) -> None:
    session_factory, config_path = ctx
    add_identity(session_factory, identity_type=DEV_X, value="DevUser")

    assert "devuser" not in {account.normalized_username for account in SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()}

    add_watch(session_factory)

    assert "devuser" in {account.normalized_username for account in SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()}


def test_fixed_kol_does_not_need_token_watch_and_overlap_keeps_all_roles(ctx) -> None:
    session_factory, config_path = ctx
    add_watch(session_factory)
    add_identity(session_factory, identity_type=PROJECT_X, value="LookOnChain")

    accounts = SocialWatchRegistry(session_factory, kol_config_path=config_path).accounts()
    lookonchain = next(account for account in accounts if account.normalized_username == "lookonchain")

    assert lookonchain.watch_types == frozenset({WATCH_FIXED_KOL, WATCH_PROJECT_X})
    assert SocialWatchRegistry.fixed_kol_usernames(accounts) == {"lookonchain", "whale_alert"}
    assert SocialWatchRegistry.rule_value(accounts).count("from:LookOnChain") + SocialWatchRegistry.rule_value(accounts).count("from:lookonchain") == 1


def test_rule_fingerprint_uses_provider_username_set_not_roles(ctx) -> None:
    fixed = [SocialWatchAccount("abc", "abc", frozenset({WATCH_FIXED_KOL}))]
    overlap = [SocialWatchAccount("abc", "abc", frozenset({WATCH_FIXED_KOL, WATCH_PROJECT_X}), TOKEN)]

    assert SocialWatchRegistry.fingerprint(fixed) == SocialWatchRegistry.fingerprint(overlap)
