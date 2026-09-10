from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialIdentity, SocialKOLProfile, TokenWatchState
from app.services.social_identity_service import DEV_X, PROJECT_X, normalize_username

WATCH_FIXED_KOL = "fixed_kol"
WATCH_KOL = "kol"
WATCH_PROJECT_X = "project_x"
WATCH_DEV_X = "dev_x"


@dataclass(frozen=True)
class SocialWatchAccount:
    username: str
    normalized_username: str
    watch_types: frozenset[str] | str
    token_address: str | None = None
    token_bindings: frozenset[tuple[str, str, str]] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if isinstance(self.watch_types, str):
            object.__setattr__(self, "watch_types", frozenset({self.watch_types}))

    @property
    def watch_type(self) -> str:
        if WATCH_DEV_X in self.watch_types:
            return WATCH_DEV_X
        if WATCH_PROJECT_X in self.watch_types:
            return WATCH_PROJECT_X
        if WATCH_KOL in self.watch_types:
            return WATCH_KOL
        if WATCH_FIXED_KOL in self.watch_types:
            return WATCH_FIXED_KOL
        return ""


class SocialWatchRegistry:
    def __init__(self, session_factory: sessionmaker, *, kol_config_path: str | Path) -> None:
        self.session_factory = session_factory
        self.kol_config_path = Path(kol_config_path)

    def accounts(self) -> list[SocialWatchAccount]:
        by_username: dict[str, SocialWatchAccount] = {}
        with session_scope(self.session_factory) as session:
            kol_profiles = list(
                session.scalars(
                    select(SocialKOLProfile).where(
                        SocialKOLProfile.status == "qualified",
                        SocialKOLProfile.is_active.is_(True),
                        SocialKOLProfile.manual_override != "exclude",
                    )
                )
            )
            if kol_profiles:
                for profile in kol_profiles:
                    username = normalize_username(profile.username)
                    if not username:
                        continue
                    _put_account(
                        by_username,
                        SocialWatchAccount(
                            username=username,
                            normalized_username=username.lower(),
                            watch_types=frozenset({WATCH_KOL}),
                            token_address=None,
                        ),
                    )
            else:
                for username in load_fixed_kol_usernames(self.kol_config_path):
                    _put_account(
                        by_username,
                        SocialWatchAccount(
                            username=username,
                            normalized_username=username.lower(),
                            watch_types=frozenset({WATCH_FIXED_KOL}),
                            token_address=None,
                        ),
                    )
            identities = session.execute(
                select(SocialIdentity, TokenWatchState).join(
                    TokenWatchState,
                    (TokenWatchState.chain == SocialIdentity.chain)
                    & (TokenWatchState.token_address == SocialIdentity.token_address)
                    & (TokenWatchState.active.is_(True)),
                ).where(
                    SocialIdentity.identity_type.in_([PROJECT_X, DEV_X]),
                    SocialIdentity.is_active.is_(True),
                )
            )
            for identity, watch in identities:
                username = normalize_username(identity.value)
                if not username:
                    continue
                watch_type = WATCH_PROJECT_X if identity.identity_type == PROJECT_X else WATCH_DEV_X
                _put_account(
                    by_username,
                    SocialWatchAccount(
                        username=username,
                        normalized_username=username.lower(),
                        watch_types=frozenset({watch_type}),
                        token_address=identity.token_address,
                        token_bindings=frozenset({(watch_type, watch.chain, watch.token_address)}),
                    ),
                )
        return sorted(by_username.values(), key=lambda item: item.normalized_username)

    @staticmethod
    def fingerprint(accounts: list[SocialWatchAccount]) -> str:
        usernames = sorted({account.normalized_username for account in accounts if account.normalized_username})
        digest = hashlib.sha256("\n".join(usernames).encode("utf-8")).hexdigest()
        return digest

    @staticmethod
    def rule_value(accounts: list[SocialWatchAccount]) -> str:
        usernames = sorted({account.username for account in accounts if account.username}, key=str.lower)
        return " OR ".join(f"from:{username}" for username in usernames)

    @staticmethod
    def fixed_kol_usernames(accounts: list[SocialWatchAccount]) -> set[str]:
        return {
            account.normalized_username
            for account in accounts
            if (WATCH_FIXED_KOL in account.watch_types or WATCH_KOL in account.watch_types)
            and account.normalized_username
        }


def load_fixed_kol_usernames(path: str | Path) -> list[str]:
    config_path = Path(path)
    data = json.loads(config_path.read_text())
    if isinstance(data, list):
        rows = [{"username": item, "enabled": True} for item in data]
    elif isinstance(data, dict) and isinstance(data.get("kols"), list):
        rows = data["kols"]
    else:
        raise ValueError("social KOL config must contain a kols array")
    usernames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or row.get("enabled", True) is False:
            continue
        username = normalize_username(str(row.get("username") or ""))
        if not username:
            continue
        key = username.lower()
        if key in seen:
            continue
        seen.add(key)
        usernames.append(username)
    return usernames


def _put_account(existing: dict[str, SocialWatchAccount], account: SocialWatchAccount) -> None:
    current = existing.get(account.normalized_username)
    if current is None:
        existing[account.normalized_username] = account
        return
    watch_types = current.watch_types | account.watch_types
    token_bindings = current.token_bindings | account.token_bindings
    token_address = current.token_address or account.token_address
    username = current.username if _watch_type_rank(current.watch_type) >= _watch_type_rank(account.watch_type) else account.username
    existing[account.normalized_username] = SocialWatchAccount(
        username=username,
        normalized_username=current.normalized_username,
        watch_types=watch_types,
        token_address=token_address,
        token_bindings=token_bindings,
    )


def _watch_type_rank(watch_type: str) -> int:
    return {WATCH_DEV_X: 4, WATCH_PROJECT_X: 3, WATCH_KOL: 2, WATCH_FIXED_KOL: 1}.get(watch_type, 0)
