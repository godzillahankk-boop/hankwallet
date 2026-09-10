from __future__ import annotations

import re
from dataclasses import dataclass

EVM_CA_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9_]{0,14})\b")


@dataclass(frozen=True)
class TokenMatch:
    match_type: str
    value: str
    raw_value: str

    def to_dict(self) -> dict[str, str]:
        return {
            "type": self.match_type,
            "value": self.value,
            "raw_value": self.raw_value,
        }


def extract_token_matches(text: str | None, *, prefix: str = "direct") -> list[TokenMatch]:
    if not text:
        return []
    matches: list[TokenMatch] = []
    seen: set[tuple[str, str]] = set()
    for match in EVM_CA_RE.finditer(text):
        raw = match.group(0)
        key = (f"{prefix}_ca", raw.lower())
        if key in seen:
            continue
        seen.add(key)
        matches.append(TokenMatch(match_type=key[0], value=key[1], raw_value=raw))
    for match in CASHTAG_RE.finditer(text):
        raw = f"${match.group(1)}"
        symbol = match.group(1).upper()
        key = (f"{prefix}_cashtag", symbol)
        if key in seen:
            continue
        seen.add(key)
        matches.append(TokenMatch(match_type=key[0], value=symbol, raw_value=raw))
    return matches
