from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import Settings
from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import PriceSnapshot, SocialEvent, SocialMemory, TokenIntelligenceSnapshot, TokenWatchState
from app.services import attention_scoring as scoring
from app.services.attention_engine_service import (
    AttentionEngineService,
    build_attention_copy_markup,
    format_attention_alert,
)
from app.services.social_event_service import AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService
from app.services.social_memory_service import SocialMemoryService
from app.services.wallet_service import WalletService
from app.utils.time import utc_now

WALLET_ADDRESS = "0x0e712f06daeab2e866b1477923764af2fc1a9f67"
TOKEN_ADDRESS = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"
CHAIN = "robinhood"
SYMBOL = "TESTSOCIAL"
DEV_URL = "https://x.com/testdev/status/123456"


@dataclass
class AcceptanceResult:
    scenario: str
    passed: bool
    family_scores: dict[str, int]
    primary_family: str | None
    primary_signal: str | None
    direction: str
    dev_modifier: int
    final_att: int
    social_evidence: dict[str, Any]
    telegram_text: str
    dev_button_url: str | None = None


class NoopGmgn:
    async def get_track_feed(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []

    async def get_market_signals(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


def acceptance_settings(db_path: Path) -> Settings:
    return Settings(
        telegram_bot_token="test-token",
        database_url=f"sqlite:///{db_path}",
        wallet_scan_interval_seconds=60,
        legacy_wallet_scan_enabled=False,
        manual_scan_cooldown_seconds=30,
        dust_threshold=Decimal("0.000001"),
        dust_clear_confirmation_scans=2,
        default_chain=CHAIN,
        chain_api_base_url=None,
        chain_api_key=None,
        chain_rpc_url=None,
        chain_token_search_symbols=(),
        chain_request_timeout_seconds=20,
        token_transfer_lookback_limit=100,
        log_level="INFO",
        api_host="127.0.0.1",
        api_port=8000,
        gmgn_enabled=True,
        gmgn_api_base_url="https://openapi.gmgn.ai",
        gmgn_api_key="test-key",
        gmgn_private_key_path=None,
        gmgn_request_timeout_seconds=20,
        price_guardian_enabled=True,
        price_scan_interval_seconds=60,
        price_monitor_min_usd_value=Decimal("5"),
        price_excluded_symbols=("USDG", "USDC", "USDT", "ETH", "WETH"),
        price_history_retention_hours=24,
        price_alert_5m_percent=Decimal("10"),
        price_alert_15m_percent=Decimal("20"),
        price_alert_60m_percent=Decimal("30"),
        price_alert_escalation_step_percent=Decimal("10"),
        price_alert_reset_ratio=Decimal("0.5"),
        price_holdings_max_pages=10,
        price_wallet_concurrency=3,
        attention_engine_enabled=True,
        attention_smart_money_interval_seconds=60,
        attention_kol_interval_seconds=60,
        attention_market_signal_interval_seconds=120,
        attention_token_snapshot_interval_seconds=600,
        attention_top_holder_interval_seconds=900,
        attention_token_snapshot_batch_size=3,
        attention_top_holder_batch_size=1,
        attention_feed_window_minutes=15,
        attention_event_aggregation_minutes=5,
        attention_warning_cooldown_minutes=30,
        attention_critical_cooldown_minutes=60,
    )


def build_env(tmp_dir: Path):
    settings = acceptance_settings(tmp_dir / f"acceptance-{utc_now().timestamp()}.db")
    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    sent: list[tuple[int, str]] = []

    async def notify(chat_id: int, text: str, reply_markup=None) -> None:  # noqa: ANN001
        sent.append((chat_id, text))

    with session_scope(session_factory) as session:
        wallet = WalletService(session).add_wallet(1, 100, WALLET_ADDRESS, CHAIN)
        wallet_id = wallet.id
    return settings, session_factory, notify, wallet_id


def start_watch(session_factory, wallet_id: int, *, started_at=None) -> None:
    started_at = started_at or utc_now() - timedelta(minutes=10)
    with session_scope(session_factory) as session:
        session.add(
            TokenWatchState(
                wallet_id=wallet_id,
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                active=True,
                started_at=started_at,
                last_seen_at=utc_now(),
            )
        )
        session.add(
            PriceSnapshot(
                wallet_id=wallet_id,
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                price_usd=Decimal("0.01"),
                balance=Decimal("5000"),
                usd_value=Decimal("50"),
                observed_at=utc_now(),
                quality_status="VALID",
            )
        )
        session.add(
            TokenIntelligenceSnapshot(
                wallet_id=wallet_id,
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                market_cap_usd=Decimal("1000000"),
                liquidity_usd=Decimal("100000"),
                holder_count=500,
                observed_at=utc_now(),
            )
        )


def end_watch(session_factory, wallet_id: int, *, ended_at=None) -> None:
    ended_at = ended_at or utc_now()
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN_ADDRESS,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        watch.active = False
        watch.ended_at = ended_at


def add_kol_event(session_factory, wallet_id: int, *, tweet_id: str, author_id: str, posted_at=None) -> None:
    posted_at = posted_at or utc_now()
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN_ADDRESS,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        session.add(
            SocialEvent(
                wallet_id=wallet_id,
                watch_state_id=watch.id,
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                provider="twitterapi_io",
                provider_event_id=tweet_id,
                author_id=author_id,
                author_username=author_id,
                author_type=AUTHOR_KOL,
                posted_at=posted_at,
                received_at=posted_at + timedelta(seconds=1),
                ingestion_type="stream",
                post_type="original",
                text=f"${SYMBOL}",
                match_type="direct_cashtag",
                matched_value=SYMBOL,
                tweet_url=f"https://x.com/{author_id}/status/{tweet_id}",
            )
        )


def add_project_event_without_memory(session_factory, wallet_id: int, *, tweet_id: str) -> None:
    posted_at = utc_now()
    with session_scope(session_factory) as session:
        watch = session.scalar(
            select(TokenWatchState).where(
                TokenWatchState.wallet_id == wallet_id,
                TokenWatchState.token_address == TOKEN_ADDRESS,
                TokenWatchState.active.is_(True),
            )
        )
        assert watch is not None
        session.add(
            SocialEvent(
                wallet_id=wallet_id,
                watch_state_id=watch.id,
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                provider="twitterapi_io",
                provider_event_id=tweet_id,
                author_id="project-author",
                author_username="testdev",
                author_type=AUTHOR_PROJECT_X,
                posted_at=posted_at,
                received_at=posted_at + timedelta(seconds=1),
                ingestion_type="stream",
                post_type="original",
                text="GM",
                match_type="identity_account",
                matched_value="@testdev",
                tweet_url=f"https://x.com/testdev/status/{tweet_id}",
            )
        )


def add_memory(session_factory, *, tweet_id: str, significance: str = "high", event_time=None, url: str | None = None) -> None:
    event_time = event_time or utc_now()
    with session_scope(session_factory) as session:
        session.add(
            SocialMemory(
                chain=CHAIN,
                token_address=TOKEN_ADDRESS,
                symbol=SYMBOL,
                project_identity="testdev",
                project_key=f"{CHAIN}:{TOKEN_ADDRESS}",
                source_author_id="project-author",
                source_username="testdev",
                source_author_type=AUTHOR_PROJECT_X,
                provider="twitterapi_io",
                tweet_id=tweet_id,
                tweet_url=url or f"https://x.com/testdev/status/{tweet_id}",
                event_time=event_time,
                category="development",
                summary="Project announced a meaningful DEV update.",
                significance=significance,
                confidence="high",
                information_scope="project",
                triage_version="acceptance",
            )
        )


def service_for(settings: Settings, session_factory, notify) -> AttentionEngineService:  # noqa: ANN001
    return AttentionEngineService(
        session_factory,
        NoopGmgn(),
        settings,
        notify,
        social_event_service=SocialEventService(session_factory),
        social_memory_service=SocialMemoryService(session_factory),
    )


async def assess(settings: Settings, session_factory, notify, wallet_id: int):  # noqa: ANN001
    assessment = await service_for(settings, session_factory, notify).assess_token(wallet_id, TOKEN_ADDRESS)
    assert assessment is not None
    evidence = json.loads(assessment.evidence_json or "{}")
    return assessment, evidence


def button_url(assessment) -> str | None:  # noqa: ANN001
    markup = build_attention_copy_markup(assessment)
    for row in markup.inline_keyboard:
        for button in row:
            if button.text == "🔗 查看DEV更新":
                return button.url
    return None


def result_from_assessment(scenario: str, assessment, evidence: dict[str, Any]) -> AcceptanceResult:  # noqa: ANN001
    return AcceptanceResult(
        scenario=scenario,
        passed=True,
        family_scores=evidence["family_scores"],
        primary_family=assessment.primary_family,
        primary_signal=evidence.get("primary_signal"),
        direction=assessment.direction,
        dev_modifier=assessment.dev_modifier,
        final_att=assessment.final_attention_score,
        social_evidence=evidence["social"],
        telegram_text=format_attention_alert(assessment),
        dev_button_url=button_url(assessment),
    )


async def scenario_a(tmp_dir: Path) -> AcceptanceResult:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    start_watch(session_factory, wallet_id)
    for idx in range(3):
        add_kol_event(session_factory, wallet_id, tweet_id=f"kol-a-{idx}", author_id=f"kol-a-{idx}")

    assessment, evidence = await assess(settings, session_factory, notify, wallet_id)

    social = evidence["social"]
    assert social["unique_kols"] == 3
    assert social["kol_posts"] >= 3
    assert social["x_kol_heat_score"] == 28
    assert social["meaningful_dev_updates"] == 0
    assert social["social_score"] == 28
    assert social["primary_social_signal"] == "social_kol_heat"
    assert assessment.dev_modifier == 0
    assert assessment.final_attention_score == 38
    text = format_attention_alert(assessment)
    assert text.startswith("⚪️ TESTSOCIAL")
    assert "• 社媒｜15m KOL+3" in text
    return result_from_assessment("A_X_KOL_HEAT", assessment, evidence)


async def scenario_b(tmp_dir: Path) -> AcceptanceResult:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    start_watch(session_factory, wallet_id)
    add_memory(session_factory, tweet_id="123456", significance="high", url=DEV_URL)

    assessment, evidence = await assess(settings, session_factory, notify, wallet_id)

    social = evidence["social"]
    assert social["meaningful_dev_updates"] == 1
    assert social["highest_dev_significance"] == "high"
    assert social["dev_update_score"] == 40
    assert social["social_score"] == 40
    assert social["primary_social_signal"] == "dev_project_update"
    assert assessment.dev_modifier == 5
    assert assessment.final_attention_score == 55
    assert assessment.attention_level == scoring.WARNING
    assert assessment.primary_family == scoring.SOCIAL
    assert assessment.direction == scoring.NEUTRAL
    text = format_attention_alert(assessment)
    assert text.startswith("⚪️ TESTSOCIAL")
    assert "DEV推特更新｜Neutral｜ATT" in text
    assert "• 社媒｜15m DEV+1" in text
    assert button_url(assessment) == DEV_URL
    return result_from_assessment("B_MEANINGFUL_DEV_UPDATE", assessment, evidence)


async def scenario_c(tmp_dir: Path) -> AcceptanceResult:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    start_watch(session_factory, wallet_id)
    for idx in range(3):
        add_kol_event(session_factory, wallet_id, tweet_id=f"kol-c-{idx}", author_id=f"kol-c-{idx}")
    add_memory(session_factory, tweet_id="dev-c", significance="high", url=DEV_URL)

    assessment, evidence = await assess(settings, session_factory, notify, wallet_id)

    social = evidence["social"]
    assert social["x_kol_heat_score"] == 28
    assert social["dev_update_score"] == 40
    assert social["social_score"] == 40
    assert social["primary_social_signal"] == "dev_project_update"
    assert assessment.dev_modifier == 5
    assert assessment.final_attention_score == 55
    text = format_attention_alert(assessment)
    assert text.startswith("⚪️ TESTSOCIAL")
    assert "DEV推特更新" in text
    assert "• 社媒｜15m KOL+3 DEV+1" in text
    return result_from_assessment("C_KOL_AND_DEV_COMBINED", assessment, evidence)


async def scenario_d(tmp_dir: Path) -> AcceptanceResult:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    start_watch(session_factory, wallet_id)
    add_project_event_without_memory(session_factory, wallet_id, tweet_id="project-no-memory")

    assessment, evidence = await assess(settings, session_factory, notify, wallet_id)

    social = evidence["social"]
    assert social["meaningful_dev_updates"] == 0
    assert social["dev_update_score"] == 0
    assert social["social_score"] == 0
    assert assessment.dev_modifier == 0
    assert assessment.final_attention_score == 10
    text = format_attention_alert(assessment)
    assert text.startswith("⚪️ TESTSOCIAL")
    assert "DEV推特更新" not in text
    assert "查看DEV更新" not in text
    assert button_url(assessment) is None
    return result_from_assessment("D_NO_MEMORY_NO_DEV_ALERT", assessment, evidence)


async def scenario_e(tmp_dir: Path) -> tuple[AcceptanceResult, AcceptanceResult]:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    now = utc_now()
    start_watch(session_factory, wallet_id, started_at=now - timedelta(minutes=20))
    add_memory(session_factory, tweet_id="old-dev", significance="high", event_time=now - timedelta(minutes=5))
    for idx in range(3):
        add_kol_event(
            session_factory,
            wallet_id,
            tweet_id=f"old-kol-{idx}",
            author_id=f"old-kol-{idx}",
            posted_at=now - timedelta(minutes=5),
        )
    end_watch(session_factory, wallet_id, ended_at=now - timedelta(minutes=2))
    start_watch(session_factory, wallet_id, started_at=now - timedelta(minutes=1))

    empty_assessment, empty_evidence = await assess(settings, session_factory, notify, wallet_id)

    empty_social = empty_evidence["social"]
    assert empty_social["unique_kols"] == 0
    assert empty_social["meaningful_dev_updates"] == 0
    assert empty_social["social_score"] == 0
    assert empty_assessment.dev_modifier == 0
    with session_scope(session_factory) as session:
        assert session.scalar(select(func.count()).select_from(SocialMemory)) == 1

    add_memory(session_factory, tweet_id="new-dev", significance="high", event_time=now)
    dev_assessment, dev_evidence = await assess(settings, session_factory, notify, wallet_id)

    dev_social = dev_evidence["social"]
    assert dev_social["meaningful_dev_updates"] == 1
    assert dev_social["dev_update_score"] == 40
    assert dev_social["social_score"] == 40
    assert dev_assessment.dev_modifier == 5
    assert dev_assessment.final_attention_score == 55
    return (
        result_from_assessment("E_REBUY_SESSION_ISOLATION_EMPTY", empty_assessment, empty_evidence),
        result_from_assessment("E_REBUY_SESSION_ISOLATION_NEW_DEV", dev_assessment, dev_evidence),
    )


async def frozen_evidence_check(tmp_dir: Path) -> dict[str, Any]:
    settings, session_factory, notify, wallet_id = build_env(tmp_dir)
    start_watch(session_factory, wallet_id)
    add_memory(session_factory, tweet_id="frozen-dev", significance="high", url=DEV_URL)
    assessment, evidence = await assess(settings, session_factory, notify, wallet_id)
    before_text = format_attention_alert(assessment)
    before_url = button_url(assessment)

    with session_scope(session_factory) as session:
        session.execute(delete(SocialEvent))
        session.execute(delete(SocialMemory))

    after_text = format_attention_alert(assessment)
    after_url = button_url(assessment)
    with session_scope(session_factory) as session:
        memory_count = session.scalar(select(func.count()).select_from(SocialMemory))

    assert memory_count == 0
    assert before_text == after_text
    assert before_url == after_url == DEV_URL
    assert evidence["social"]["latest_dev_tweet_url"] == DEV_URL
    return {
        "passed": True,
        "text_stable_after_db_delete": before_text == after_text,
        "button_url_stable_after_db_delete": before_url == after_url,
        "frozen_url": before_url,
    }


async def main() -> None:
    with tempfile.TemporaryDirectory(prefix="social-v2d-acceptance-") as tmp:
        tmp_dir = Path(tmp)
        results: list[AcceptanceResult] = [
            await scenario_a(tmp_dir),
            await scenario_b(tmp_dir),
            await scenario_c(tmp_dir),
            await scenario_d(tmp_dir),
        ]
        results.extend(await scenario_e(tmp_dir))
        frozen = await frozen_evidence_check(tmp_dir)

    summary = {
        "all_scenarios_passed": all(result.passed for result in results),
        "scenario_count": len(results),
        "scenarios": [asdict(result) for result in results],
        "frozen_evidence": frozen,
        "external_calls": {
            "twitterapi_io": False,
            "deepseek": False,
            "telegram_sent": False,
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
