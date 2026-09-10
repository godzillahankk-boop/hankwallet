from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal

from app.db.database import init_db, make_engine, make_session_factory as build_session_factory, session_scope
from app.db.models import PriceSnapshot, SocialKOLProfile, TokenWatchState
from app.services.social_kol_service import SOURCE_SOCIAL_DISCOVERY, SOURCE_TRUSTED_EXTERNAL_SEED, SocialKOLService
from app.services.social_shadow_reporter import SocialShadowReporter
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


class DummyStatsSource:
    def __init__(self, values):
        self.values = values

    def stats_snapshot(self):
        return dict(self.values)


def make_test_session_factory(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/shadow.db")
    init_db(engine)
    return build_session_factory(engine)


def test_shadow_reporter_snapshot_counts_and_prefixed_metrics(tmp_path) -> None:
    session_factory = make_test_session_factory(tmp_path)
    now = utc_now()
    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET, "robinhood")
        watch = TokenWatchState(
            wallet_id=wallet.id,
            chain="robinhood",
            token_address=TOKEN,
            symbol="PONS",
            active=True,
            started_at=now - timedelta(minutes=30),
            last_seen_at=now,
        )
        session.add(watch)
        session.flush()
        session.add(
            PriceSnapshot(
                wallet_id=wallet.id,
                chain="robinhood",
                token_address=TOKEN,
                symbol="PONS",
                price_usd=Decimal("1"),
                balance=Decimal("10"),
                usd_value=Decimal("10"),
                observed_at=now,
                quality_status="VALID",
            )
        )
    kol_service = SocialKOLService(session_factory)
    kol_service.observe_candidate(author_id="qualified", username="qualified", followers=3000, source=SOURCE_TRUSTED_EXTERNAL_SEED)
    kol_service.observe_candidate(author_id="candidate", username="candidate", followers=1500, source=SOURCE_SOCIAL_DISCOVERY)
    reporter = SocialShadowReporter(
        session_factory=session_factory,
        min_usd_value=Decimal("5"),
        stream_ingestor=DummyStatsSource({"connection_state": "CONNECTED", "tweets_received": 2}),
        discovery_service=DummyStatsSource({"search_api_calls": 1, "tweets_received": 3}),
        twitter_client=DummyStatsSource({"http_requests_total": 4}),
        deepseek_classifier=DummyStatsSource({"classifier_calls": 5, "total_tokens": 600}),
    )

    snapshot = reporter.stats_snapshot()

    assert snapshot["active_watches"] == 1
    assert snapshot["eligible_discovery_watches"] == 1
    assert snapshot["kol_pool_qualified"] == 1
    assert snapshot["kol_candidates"] == 1
    assert snapshot["stream_connection_state"] == "CONNECTED"
    assert snapshot["discovery_search_api_calls"] == 1
    assert snapshot["twitter_http_requests_total"] == 4
    assert snapshot["deepseek_total_tokens"] == 600
    assert "tweet_text" not in snapshot


def test_shadow_reporter_logs_periodic_and_final_snapshots(tmp_path, caplog) -> None:
    reporter = SocialShadowReporter(
        session_factory=make_test_session_factory(tmp_path),
        min_usd_value=Decimal("5"),
    )

    with caplog.at_level(logging.INFO):
        reporter.report()
        reporter.report_final()

    assert "SOCIAL_SHADOW " in caplog.text
    assert "SOCIAL_SHADOW_FINAL " in caplog.text
