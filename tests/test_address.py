from app.utils.address import is_valid_evm_address, normalize_evm_address


def test_valid_evm_address() -> None:
    address = "0x1234567890abcdef1234567890ABCDEF12345678"
    assert is_valid_evm_address(address)
    assert normalize_evm_address(address) == "0x1234567890abcdef1234567890abcdef12345678"


def test_invalid_evm_address() -> None:
    assert not is_valid_evm_address("")
    assert not is_valid_evm_address("0x123")
    assert not is_valid_evm_address("1234567890abcdef1234567890abcdef12345678")
    assert not is_valid_evm_address("0xZZZ4567890abcdef1234567890abcdef12345678")

