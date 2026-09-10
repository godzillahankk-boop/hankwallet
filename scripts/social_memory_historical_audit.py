from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import make_engine, make_session_factory  # noqa: E402
from app.db.models import SocialEvent  # noqa: E402
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_PROJECT_X  # noqa: E402
from app.services.social_memory_service import (  # noqa: E402
    CATEGORY_LOW_INFORMATION,
    DECISION_NO_MEMORY,
    DeepSeekSocialSemanticTriageAdapter,
    SocialSemanticTriageAdapter,
    SocialSemanticTriageInput,
    SocialTriageResult,
    deterministic_no_memory_reason,
    validate_triage_result,
)

OUTPUT_ROOT = ROOT / "outputs" / "social_memory_historical_audit"
PROJECT_DEV_AUTHOR_TYPES = (AUTHOR_PROJECT_X, AUTHOR_DEV_X)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit historical Project/DEV SocialEvents with Social Memory triage")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT))
    args = parser.parse_args()

    settings = load_settings()
    if not settings.deepseek_api_key:
        raise SystemExit("DEEPSEEK_API_KEY missing; Social Memory historical audit cannot run.")

    session_factory = make_session_factory(make_engine(settings.database_url))
    adapter = DeepSeekSocialSemanticTriageAdapter(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.social_memory_triage_model,
    )
    payload = run_audit(
        session_factory=session_factory,
        adapter=adapter,
        limit=args.limit,
        output_root=Path(args.output_root),
    )
    print_summary(payload)


def run_audit(
    *,
    session_factory: sessionmaker,
    adapter: SocialSemanticTriageAdapter,
    limit: int = 50,
    output_root: Path = OUTPUT_ROOT,
) -> dict[str, Any]:
    events = load_project_dev_events(session_factory, limit=max(1, limit))
    rows = [evaluate_event(event, adapter) for event in events]
    summary = build_summary(rows)
    payload = {"events": rows, "summary": summary}
    output_path = write_summary(payload, output_root=output_root)
    payload["summary"]["output_path"] = str(output_path)
    return payload


def load_project_dev_events(session_factory: sessionmaker, *, limit: int) -> list[SocialEvent]:
    session = session_factory()
    try:
        events = list(
            session.scalars(
                select(SocialEvent)
                .where(SocialEvent.author_type.in_(PROJECT_DEV_AUTHOR_TYPES))
                .order_by(SocialEvent.posted_at.desc(), SocialEvent.id.desc())
                .limit(limit)
            )
        )
        for event in events:
            session.expunge(event)
        return events
    finally:
        session.close()


def evaluate_event(event: SocialEvent, adapter: SocialSemanticTriageAdapter) -> dict[str, Any]:
    deterministic_reason = deterministic_no_memory_reason(event.text)
    if deterministic_reason:
        result = SocialTriageResult(
            decision=DECISION_NO_MEMORY,
            category=CATEGORY_LOW_INFORMATION,
            significance="low",
            summary="",
            reason=deterministic_reason,
            confidence="high",
            information_scope="unknown",
        )
        return _event_record(event, stage="deterministic", result=result, failure=None)
    try:
        result = validate_triage_result(
            adapter.triage(
                SocialSemanticTriageInput(
                    author_type=event.author_type,
                    author_username=event.author_username,
                    token_symbol=event.symbol,
                    text=event.text,
                    tweet_url=event.tweet_url,
                    event_time=event.posted_at,
                )
            )
        )
        return _event_record(event, stage="deepseek", result=result, failure=None)
    except Exception as exc:  # noqa: BLE001 - audit must continue through individual failures.
        return _event_record(event, stage="deepseek", result=None, failure=f"{type(exc).__name__}: {str(exc)[:200]}")


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    category_counts: dict[str, int] = {}
    keep_ids: list[int] = []
    keep_memory = no_memory = failures = deterministic = deepseek = 0
    project_events = dev_events = 0
    for row in rows:
        if row["author_type"] == AUTHOR_PROJECT_X:
            project_events += 1
        if row["author_type"] == AUTHOR_DEV_X:
            dev_events += 1
        if row["stage"] == "deterministic":
            deterministic += 1
        if row["stage"] == "deepseek":
            deepseek += 1
        if row["triage_failure"]:
            failures += 1
            continue
        if row["decision"] == "KEEP_MEMORY":
            keep_memory += 1
            keep_ids.append(row["event_id"])
            category = row["category"] or "unknown"
            category_counts[category] = category_counts.get(category, 0) + 1
        elif row["decision"] == DECISION_NO_MEMORY:
            no_memory += 1
    total = len(rows)
    return {
        "total_events": total,
        "project_x_events": project_events,
        "dev_x_events": dev_events,
        "deterministic_no_memory": deterministic,
        "deepseek_calls": deepseek,
        "keep_memory": keep_memory,
        "no_memory": no_memory,
        "triage_failures": failures,
        "keep_rate": keep_memory / total if total else 0,
        "category_counts": dict(sorted(category_counts.items())),
        "keep_memory_event_ids": keep_ids,
    }


def write_summary(payload: dict[str, Any], *, output_root: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def print_summary(payload: dict[str, Any]) -> None:
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))


def _event_record(
    event: SocialEvent,
    *,
    stage: str,
    result: SocialTriageResult | None,
    failure: str | None,
) -> dict[str, Any]:
    return {
        "event_id": event.id,
        "posted_at": event.posted_at.isoformat() if event.posted_at else None,
        "author_type": event.author_type,
        "author_username": event.author_username,
        "symbol": event.symbol,
        "token_address": event.token_address,
        "text": event.text,
        "stage": stage,
        "decision": result.decision if result else None,
        "category": result.category if result else None,
        "significance": result.significance if result else None,
        "confidence": result.confidence if result else None,
        "information_scope": result.information_scope if result else None,
        "summary": result.summary if result else "",
        "reason": result.reason if result else "",
        "triage_failure": failure,
        "review_status": "UNREVIEWED",
    }


if __name__ == "__main__":
    main()
