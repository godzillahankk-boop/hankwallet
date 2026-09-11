from __future__ import annotations

import logging
from concurrent.futures import Future

from app.main import _log_social_attention_future_result


def test_log_social_attention_future_result_warns_on_failure(caplog) -> None:
    future: Future[None] = Future()
    future.set_exception(RuntimeError("assessment failed"))
    logger = logging.getLogger("wallet-agent-test")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _log_social_attention_future_result(
            future,
            wallet_id=42,
            token_address="0x8ad5a580c4215086dec828d8626b95a06d7d00cc",
            logger=logger,
        )

    assert "Social Attention update failed wallet_id=42 token=0x8ad5a580c4215086dec828d8626b95a06d7d00cc" in caplog.text
    assert "assessment failed" in caplog.text


def test_log_social_attention_future_result_success_is_quiet(caplog) -> None:
    future: Future[None] = Future()
    future.set_result(None)
    logger = logging.getLogger("wallet-agent-test")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _log_social_attention_future_result(
            future,
            wallet_id=42,
            token_address="0x8ad5a580c4215086dec828d8626b95a06d7d00cc",
            logger=logger,
        )

    assert not caplog.records
