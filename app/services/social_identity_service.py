from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialIdentity
from app.services.gmgn_client import GmgnTokenOverview
from app.utils.address import normalize_evm_address
from app.utils.time import utc_now

PROJECT_X = "project_x"
DEV_WALLET = "dev_wallet"
DEV_X = "dev_x"
WEBSITE = "website"
TELEGRAM = "telegram"

HIGH = "HIGH"
MEDIUM = "MEDIUM"

GMGN_TOKEN_INFO = "gmgn_token_info"


@dataclass(frozen=True)
class IdentityInput:
    chain: str
    token_address: str
    symbol: str | None
    identity_type: str
    value: str
    normalized_value: str
    source: str
    source_field: str
    confidence: str
    evidence: dict[str, Any]


@dataclass(frozen=True)
class IdentitySyncResult:
    created: int = 0
    updated: int = 0
    deactivated: int = 0


class SocialIdentityService:
    def __init__(self, session_factory: sessionmaker) -> None:
        self.session_factory = session_factory

    def upsert_identity(self, identity: IdentityInput) -> IdentitySyncResult:
        now = utc_now()
        with session_scope(self.session_factory) as session:
            for old in self._active_different_values(session, identity):
                old.is_active = False
                old.valid_to = now
                old.updated_at = now
            existing = session.scalar(
                select(SocialIdentity).where(
                    SocialIdentity.chain == identity.chain,
                    SocialIdentity.token_address == identity.token_address,
                    SocialIdentity.identity_type == identity.identity_type,
                    SocialIdentity.normalized_value == identity.normalized_value,
                )
            )
            if existing:
                changed = _merge_identity(existing, identity, now)
                return IdentitySyncResult(updated=1 if changed else 0)
            session.add(
                SocialIdentity(
                    chain=identity.chain,
                    token_address=identity.token_address,
                    symbol=identity.symbol,
                    identity_type=identity.identity_type,
                    value=identity.value,
                    normalized_value=identity.normalized_value,
                    source=identity.source,
                    source_field=identity.source_field,
                    confidence=identity.confidence,
                    evidence_json=json.dumps({"sources": [identity.evidence]}, ensure_ascii=False),
                    is_active=True,
                    first_seen_at=now,
                    last_verified_at=now,
                    valid_from=now,
                )
            )
            return IdentitySyncResult(created=1)

    def sync_from_token_overview(self, watched_token, overview: GmgnTokenOverview) -> IdentitySyncResult:
        now = utc_now()
        identities = identity_inputs_from_token_overview(watched_token, overview)
        created = 0
        updated = 0
        deactivated = 0
        with session_scope(self.session_factory) as session:
            for identity in identities:
                for old in self._active_different_values(session, identity):
                    old.is_active = False
                    old.valid_to = now
                    old.updated_at = now
                    deactivated += 1
                existing = session.scalar(
                    select(SocialIdentity).where(
                        SocialIdentity.chain == identity.chain,
                        SocialIdentity.token_address == identity.token_address,
                        SocialIdentity.identity_type == identity.identity_type,
                        SocialIdentity.normalized_value == identity.normalized_value,
                    )
                )
                if existing:
                    changed = _merge_identity(existing, identity, now)
                    updated += 1 if changed else 0
                    continue
                session.add(
                    SocialIdentity(
                        chain=identity.chain,
                        token_address=identity.token_address,
                        symbol=identity.symbol,
                        identity_type=identity.identity_type,
                        value=identity.value,
                        normalized_value=identity.normalized_value,
                        source=identity.source,
                        source_field=identity.source_field,
                        confidence=identity.confidence,
                        evidence_json=json.dumps({"sources": [identity.evidence]}, ensure_ascii=False),
                        is_active=True,
                        first_seen_at=now,
                        last_verified_at=now,
                        valid_from=now,
                    )
                )
                created += 1
        return IdentitySyncResult(created=created, updated=updated, deactivated=deactivated)

    def _active_different_values(self, session, identity: IdentityInput) -> list[SocialIdentity]:
        return list(
            session.scalars(
                select(SocialIdentity).where(
                    SocialIdentity.chain == identity.chain,
                    SocialIdentity.token_address == identity.token_address,
                    SocialIdentity.identity_type == identity.identity_type,
                    SocialIdentity.normalized_value != identity.normalized_value,
                    SocialIdentity.is_active.is_(True),
                )
            )
        )


def identity_inputs_from_token_overview(watched_token, overview: GmgnTokenOverview) -> list[IdentityInput]:
    chain = getattr(watched_token, "chain", None) or overview.chain
    token_address = normalize_evm_address(getattr(watched_token, "token_address", None) or overview.token_address)
    symbol = overview.symbol or getattr(watched_token, "symbol", None)
    identities: list[IdentityInput] = []
    if overview.twitter:
        username = normalize_username(overview.twitter)
        if username:
            identities.append(
                IdentityInput(
                    chain=chain,
                    token_address=token_address,
                    symbol=symbol,
                    identity_type=PROJECT_X,
                    value=username,
                    normalized_value=username.lower(),
                    source=GMGN_TOKEN_INFO,
                    source_field="link.twitter_username",
                    confidence=HIGH,
                    evidence={"source": GMGN_TOKEN_INFO, "source_field": "link.twitter_username", "raw_value": overview.twitter},
                )
            )
    if overview.creator_address:
        creator = normalize_evm_address(overview.creator_address)
        identities.append(
            IdentityInput(
                chain=chain,
                token_address=token_address,
                symbol=symbol,
                identity_type=DEV_WALLET,
                value=creator,
                normalized_value=creator,
                source=GMGN_TOKEN_INFO,
                source_field="dev.creator_address",
                confidence=HIGH,
                evidence={"source": GMGN_TOKEN_INFO, "source_field": "dev.creator_address"},
            )
        )
    if overview.website:
        value = overview.website.strip()
        if value:
            identities.append(
                IdentityInput(
                    chain=chain,
                    token_address=token_address,
                    symbol=symbol,
                    identity_type=WEBSITE,
                    value=value,
                    normalized_value=value.lower(),
                    source=GMGN_TOKEN_INFO,
                    source_field="link.website",
                    confidence=HIGH,
                    evidence={"source": GMGN_TOKEN_INFO, "source_field": "link.website"},
                )
            )
    if overview.telegram:
        value = overview.telegram.strip()
        if value:
            identities.append(
                IdentityInput(
                    chain=chain,
                    token_address=token_address,
                    symbol=symbol,
                    identity_type=TELEGRAM,
                    value=value,
                    normalized_value=value.lower(),
                    source=GMGN_TOKEN_INFO,
                    source_field="link.telegram",
                    confidence=HIGH,
                    evidence={"source": GMGN_TOKEN_INFO, "source_field": "link.telegram"},
                )
            )
    return identities


def normalize_username(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    for prefix in ("https://x.com/", "https://twitter.com/", "http://x.com/", "http://twitter.com/"):
        if text.lower().startswith(prefix):
            text = text[len(prefix) :]
            break
    text = text.strip().lstrip("@").split("/", 1)[0].split("?", 1)[0]
    return text or None


def _merge_identity(existing: SocialIdentity, identity: IdentityInput, now: datetime) -> bool:
    changed = False
    if _confidence_rank(identity.confidence) > _confidence_rank(existing.confidence):
        existing.confidence = identity.confidence
        changed = True
    if existing.value != identity.value:
        existing.value = identity.value
        changed = True
    if existing.symbol != identity.symbol and identity.symbol:
        existing.symbol = identity.symbol
        changed = True
    if existing.source != identity.source:
        existing.source = identity.source
        changed = True
    if existing.source_field != identity.source_field:
        existing.source_field = identity.source_field
        changed = True
    existing.last_verified_at = now
    if not existing.is_active:
        existing.is_active = True
        changed = True
    if existing.valid_to is not None:
        existing.valid_to = None
        changed = True
    sources = _evidence_sources(existing.evidence_json)
    if identity.evidence not in sources:
        sources.append(identity.evidence)
        existing.evidence_json = json.dumps({"sources": sources}, ensure_ascii=False)
        changed = True
    if changed:
        existing.updated_at = now
    return changed


def _evidence_sources(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    sources = data.get("sources")
    return [item for item in sources if isinstance(item, dict)] if isinstance(sources, list) else []


def _confidence_rank(confidence: str) -> int:
    return {MEDIUM: 1, HIGH: 2}.get(confidence, 0)
