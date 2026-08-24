from __future__ import annotations

import re


EVM_ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")


def is_valid_evm_address(address: str | None) -> bool:
    if not address:
        return False
    return bool(EVM_ADDRESS_PATTERN.fullmatch(address.strip()))


def normalize_evm_address(address: str) -> str:
    return address.strip().lower()


def short_address(address: str) -> str:
    normalized = normalize_evm_address(address)
    if len(normalized) <= 14:
        return normalized
    return f"{normalized[:6]}...{normalized[-4:]}"

