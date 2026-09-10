from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.db.database import init_db, make_engine, make_session_factory, session_scope
from app.db.models import SocialEvent, SocialMemory
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X
from app.services.social_memory_service import DECISION_KEEP_MEMORY, DECISION_NO_MEMORY, SocialTriageResult
import scripts.social_memory_historical_audit as audit


TOKEN = "0x8ad5a580c4215086dec828d8626b95a06d7d00cc"


class FakeAdapter:
    def __init__(self, results: dict[str, SocialTriageResult | Exception]) -> None:
        self.results = results
        self.calls: list[str | None] = []

    def triage(self, payload):  # noqa: ANN001
        self.calls.append(payload.text)
        result = self.results[payload.text]
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def ctx(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/historical_audit.db")
    init_db(engine)
    session_factory = make_session_factory(engine)
    now = datetime(2026, 9, 10, 12, 0, 0)
    with session_scope(session_factory) as session:
        session.add_all(
            [
                social_event(1, AUTHOR_KOL, "KolUser", "KOL tweet", now),
                social_event(2, AUTHOR_PROJECT_X, "ProjectUser", "GM $ROBBIE 🚀", now - timedelta(minutes=1)),
                social_event(3, AUTHOR_PROJECT_X, "ProjectUser", "Mainnet launches Sep 20.", now - timedelta(minutes=2)),
                social_event(4, AUTHOR_DEV_X, "DevUser", "Audit published.", now - timedelta(minutes=3)),
                social_event(5, AUTHOR_DEV_X, "DevUser", "Model failure sample.", now - timedelta(minutes=4)),
            ]
        )
    return session_factory


def test_historical_audit_only_reads_project_dev_and_skips_kol(ctx, tmp_path) -> None:
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(decision=DECISION_KEEP_MEMORY, category="development"),
            "Audit published.": triage(decision=DECISION_KEEP_MEMORY, category="security"),
            "Model failure sample.": RuntimeError("model down"),
        }
    )

    payload = audit.run_audit(session_factory=ctx, adapter=adapter, limit=50, output_root=tmp_path)

    event_ids = [row["event_id"] for row in payload["events"]]
    assert event_ids == [2, 3, 4, 5]
    assert "KOL tweet" not in adapter.calls
    assert payload["summary"]["total_events"] == 4
    assert payload["summary"]["project_x_events"] == 2
    assert payload["summary"]["dev_x_events"] == 2


def test_historical_audit_deterministic_no_memory_does_not_call_adapter(ctx, tmp_path) -> None:
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(decision=DECISION_KEEP_MEMORY, category="development"),
            "Audit published.": triage(decision=DECISION_KEEP_MEMORY, category="security"),
            "Model failure sample.": RuntimeError("model down"),
        }
    )

    payload = audit.run_audit(session_factory=ctx, adapter=adapter, limit=50, output_root=tmp_path)
    deterministic_row = next(row for row in payload["events"] if row["text"] == "GM $ROBBIE 🚀")

    assert deterministic_row["stage"] == "deterministic"
    assert deterministic_row["decision"] == DECISION_NO_MEMORY
    assert "GM $ROBBIE 🚀" not in adapter.calls
    assert payload["summary"]["deterministic_no_memory"] == 1


def test_historical_audit_calls_adapter_for_semantic_candidates_and_continues_on_failure(ctx, tmp_path) -> None:
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(decision=DECISION_KEEP_MEMORY, category="development"),
            "Audit published.": triage(decision=DECISION_KEEP_MEMORY, category="security"),
            "Model failure sample.": RuntimeError("model down"),
        }
    )

    payload = audit.run_audit(session_factory=ctx, adapter=adapter, limit=50, output_root=tmp_path)
    failed = next(row for row in payload["events"] if row["text"] == "Model failure sample.")

    assert adapter.calls == ["Mainnet launches Sep 20.", "Audit published.", "Model failure sample."]
    assert failed["stage"] == "deepseek"
    assert failed["triage_failure"].startswith("RuntimeError")
    assert payload["summary"]["deepseek_calls"] == 3
    assert payload["summary"]["triage_failures"] == 1


def test_historical_audit_does_not_write_social_memory(ctx, tmp_path) -> None:
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(decision=DECISION_KEEP_MEMORY, category="development"),
            "Audit published.": triage(decision=DECISION_KEEP_MEMORY, category="security"),
            "Model failure sample.": RuntimeError("model down"),
        }
    )

    audit.run_audit(session_factory=ctx, adapter=adapter, limit=50, output_root=tmp_path)

    with session_scope(ctx) as session:
        assert session.scalar(select(SocialMemory.id)) is None


def test_historical_audit_summary_counts_and_output_file(ctx, tmp_path) -> None:
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(decision=DECISION_KEEP_MEMORY, category="development"),
            "Audit published.": triage(decision=DECISION_KEEP_MEMORY, category="security"),
            "Model failure sample.": RuntimeError("model down"),
        }
    )

    payload = audit.run_audit(session_factory=ctx, adapter=adapter, limit=50, output_root=tmp_path)
    summary = payload["summary"]

    assert summary["keep_memory"] == 2
    assert summary["no_memory"] == 1
    assert summary["keep_rate"] == 0.5
    assert summary["category_counts"] == {"development": 1, "security": 1}
    assert summary["keep_memory_event_ids"] == [3, 4]
    assert (tmp_path / summary["output_path"].split(str(tmp_path), 1)[1].lstrip("/")).exists()
    assert all(row["review_status"] == "UNREVIEWED" for row in payload["events"])


def test_historical_audit_main_fails_fast_without_deepseek_key(monkeypatch) -> None:
    settings = SimpleNamespace(deepseek_api_key=None)
    monkeypatch.setattr(audit, "load_settings", lambda: settings)
    monkeypatch.setattr(audit.sys, "argv", ["social_memory_historical_audit.py"])

    with pytest.raises(SystemExit, match="DEEPSEEK_API_KEY missing"):
        audit.main()


def social_event(
    event_id: int,
    author_type: str,
    username: str,
    text: str,
    posted_at: datetime,
) -> SocialEvent:
    return SocialEvent(
        id=event_id,
        wallet_id=1,
        watch_state_id=1,
        chain="robinhood",
        token_address=TOKEN,
        symbol="ROBBIE",
        provider="twitterapi_io",
        provider_event_id=f"tweet-{event_id}",
        author_id=f"author-{event_id}",
        author_username=username,
        author_type=author_type,
        posted_at=posted_at,
        received_at=posted_at + timedelta(seconds=5),
        ingestion_type="stream",
        post_type="original",
        text=text,
        match_type="direct_ca",
        matched_value=TOKEN,
        tweet_url=f"https://x.com/{username}/status/tweet-{event_id}",
    )


def triage(*, decision: str, category: str) -> SocialTriageResult:
    return SocialTriageResult(
        decision=decision,
        category=category,
        significance="medium",
        summary="Project published a factual update." if decision == DECISION_KEEP_MEMORY else "",
        reason="test",
        confidence="high",
        information_scope="project",
    )
