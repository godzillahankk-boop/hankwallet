from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.utils.address import normalize_evm_address

HIGH = "HIGH"
MEDIUM = "MEDIUM"


@dataclass(frozen=True)
class SocialIdentityCandidate:
    token_address: str
    symbol: str | None
    identity_type: str
    value: str
    source: str
    source_field: str
    confidence: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def dedupe_key(self) -> tuple[str, str, str]:
        return (self.identity_type, self.value.lower(), self.source_field)

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_address": self.token_address,
            "symbol": self.symbol,
            "identity_type": self.identity_type,
            "value": self.value,
            "source": self.source,
            "source_field": self.source_field,
            "confidence": self.confidence,
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class SocialIdentityResolution:
    token_address: str
    symbol: str | None
    candidates: list[SocialIdentityCandidate]
    raw_fields: dict[str, Any]

    def by_type(self, identity_type: str) -> list[SocialIdentityCandidate]:
        return [candidate for candidate in self.candidates if candidate.identity_type == identity_type]


def resolve_social_identities(
    *,
    token_address: str,
    symbol: str | None,
    token_info: dict[str, Any] | None,
    dev_holder_items: list[dict[str, Any]] | None = None,
) -> SocialIdentityResolution:
    info = token_info if isinstance(token_info, dict) else {}
    normalized_token = normalize_evm_address(token_address)
    candidates: list[SocialIdentityCandidate] = []
    link = _dict(info.get("link"))
    dev = _dict(info.get("dev"))
    stat = _dict(info.get("stat"))
    fee_distribution = _dict(info.get("fee_distribution"))

    project_x = _str(_first(link, info, "twitter_username", "twitter"))
    if project_x:
        candidates.append(
            _candidate(
                normalized_token,
                symbol,
                "project_x",
                _normalize_username(project_x),
                "gmgn_token_info",
                "link.twitter_username",
                HIGH,
                {"raw_value": project_x},
            )
        )
    for identity_type, field_name in (("website", "website"), ("telegram", "telegram"), ("discord", "discord")):
        value = _str(_first(link, info, field_name))
        if value:
            candidates.append(
                _candidate(
                    normalized_token,
                    symbol,
                    identity_type,
                    value,
                    "gmgn_token_info",
                    f"link.{field_name}",
                    HIGH,
                    {},
                )
            )

    creator = _normalize_address_or_none(_first(dev, info, "creator_address", "creator", "deployer"))
    if creator:
        candidates.append(
            _candidate(
                normalized_token,
                symbol,
                "dev_wallet",
                creator,
                "gmgn_token_info",
                _source_field_for_creator(dev, info),
                HIGH,
                {
                    "creator_token_status": _str(_first(dev, stat, "creator_token_status")),
                    "creator_hold_rate": _str(_first(stat, dev, "creator_hold_rate", "creator_balance_rate")),
                    "dev_team_hold_rate": _str(stat.get("dev_team_hold_rate")),
                    "fund_from": _str(dev.get("fund_from")),
                },
            )
        )

    for candidate in _dev_x_from_holder_items(normalized_token, symbol, creator, dev_holder_items or []):
        candidates.append(candidate)
    for candidate in _launchpad_candidates(normalized_token, symbol, fee_distribution, creator):
        candidates.append(candidate)

    launchpad = _str(_first(fee_distribution, info, "launchpad", "launchpad_platform", "launchpad_status"))
    if launchpad:
        candidates.append(
            _candidate(
                normalized_token,
                symbol,
                "launchpad",
                launchpad,
                "gmgn_token_info",
                _launchpad_source_field(fee_distribution, info),
                HIGH,
                {
                    "launchpad_platform": _str(info.get("launchpad_platform")),
                    "launchpad_status": _str(info.get("launchpad_status")),
                },
            )
        )

    return SocialIdentityResolution(
        token_address=normalized_token,
        symbol=symbol,
        candidates=_dedupe(candidates),
        raw_fields={
            "probe_error": info.get("_probe_error"),
            "link": {key: link.get(key) for key in sorted(link)},
            "dev": {key: dev.get(key) for key in sorted(dev)},
            "stat": {key: stat.get(key) for key in sorted(stat)},
            "fee_distribution": fee_distribution,
        },
    )


def _dev_x_from_holder_items(
    token_address: str,
    symbol: str | None,
    creator: str | None,
    items: list[dict[str, Any]],
) -> list[SocialIdentityCandidate]:
    candidates: list[SocialIdentityCandidate] = []
    for item in items:
        holder_address = _normalize_address_or_none(_first(item, {}, "address", "wallet_address", "maker"))
        username = _str(_first(item, {}, "twitter_username"))
        if not holder_address or not username:
            continue
        if creator and holder_address != creator:
            continue
        candidates.append(
            _candidate(
                token_address,
                symbol,
                "dev_x",
                _normalize_username(username),
                "gmgn_token_holders",
                "holder.twitter_username",
                HIGH,
                {
                    "wallet_address": holder_address,
                    "twitter_name": _str(_first(item, {}, "twitter_name", "name")),
                    "tags": item.get("tags"),
                    "maker_token_tags": item.get("maker_token_tags"),
                },
            )
        )
    return candidates


def _launchpad_candidates(
    token_address: str,
    symbol: str | None,
    fee_distribution: dict[str, Any],
    creator: str | None,
) -> list[SocialIdentityCandidate]:
    platform_data = _dict(fee_distribution.get("platform_data"))
    launchpad = _str(fee_distribution.get("launchpad"))
    candidates: list[SocialIdentityCandidate] = []
    for item in _list(platform_data.get("list")):
        if not isinstance(item, dict) or not item.get("is_creator"):
            continue
        wallet = _normalize_address_or_none(item.get("wallet"))
        username = _str(item.get("twitter_username"))
        if wallet and not creator:
            candidates.append(
                _candidate(
                    token_address,
                    symbol,
                    "dev_wallet",
                    wallet,
                    "gmgn_token_info",
                    "fee_distribution.platform_data.list[].wallet",
                    MEDIUM,
                    {"launchpad": launchpad, "is_creator": True},
                )
            )
        if username and (not creator or not wallet or wallet == creator):
            candidates.append(
                _candidate(
                    token_address,
                    symbol,
                    "dev_x",
                    _normalize_username(username),
                    "gmgn_token_info",
                    "fee_distribution.platform_data.list[].twitter_username",
                    MEDIUM,
                    {
                        "launchpad": launchpad,
                        "wallet": wallet,
                        "username": item.get("username"),
                        "is_creator": True,
                    },
                )
            )
    return candidates


def _candidate(
    token_address: str,
    symbol: str | None,
    identity_type: str,
    value: str,
    source: str,
    source_field: str,
    confidence: str,
    evidence: dict[str, Any],
) -> SocialIdentityCandidate:
    return SocialIdentityCandidate(
        token_address=token_address,
        symbol=symbol,
        identity_type=identity_type,
        value=value,
        source=source,
        source_field=source_field,
        confidence=confidence,
        evidence={key: value for key, value in evidence.items() if value not in (None, "", [])},
    )


def _dedupe(candidates: list[SocialIdentityCandidate]) -> list[SocialIdentityCandidate]:
    seen: set[tuple[str, str, str]] = set()
    output: list[SocialIdentityCandidate] = []
    for candidate in candidates:
        key = candidate.dedupe_key()
        if key in seen:
            continue
        seen.add(key)
        output.append(candidate)
    return output


def _source_field_for_creator(dev: dict[str, Any], info: dict[str, Any]) -> str:
    for field in ("creator_address", "creator", "deployer"):
        if dev.get(field):
            return f"dev.{field}"
        if info.get(field):
            return field
    return "dev.creator_address"


def _launchpad_source_field(fee_distribution: dict[str, Any], info: dict[str, Any]) -> str:
    if fee_distribution.get("launchpad"):
        return "fee_distribution.launchpad"
    if info.get("launchpad_platform"):
        return "launchpad_platform"
    if info.get("launchpad_status"):
        return "launchpad_status"
    return "launchpad"


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@")


def _normalize_address_or_none(value: Any) -> str | None:
    if not value:
        return None
    try:
        return normalize_evm_address(str(value))
    except ValueError:
        return None


def _first(primary: dict[str, Any], secondary: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in primary:
            return primary[key]
        if key in secondary:
            return secondary[key]
    return None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
