from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

from dotenv import load_dotenv
from sqlalchemy import func, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.db.database import init_db, make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import SocialEvent, SocialMemory  # noqa: E402
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService  # noqa: E402
from app.services.social_memory_service import DeepSeekSocialSemanticTriageAdapter, SocialMemoryService  # noqa: E402
from app.services.social_watch_registry import SocialWatchRegistry  # noqa: E402
from app.services.twitter_token_shadow import TOKEN_SHADOW_RULE_TAG_PREFIX  # noqa: E402
from app.services.twitterapi_io_client import TwitterApiIoClient  # noqa: E402
from app.services.twitterapi_io_social_ingestion import (  # noqa: E402
    SOCIAL_RULE_TAG,
    SOCIAL_RULE_TAG_PREFIX,
    TWITTERAPI_IO_WS_URL,
    TwitterApiIoRuleManager,
    TwitterApiIoSocialIngestor,
)
from scripts.twitter_author_cashtag_shadow_run import (  # noqa: E402
    TOKEN_IDENTITY_PROBE_PREFIX,
    _abort_reason,
    _cleanup_summary,
    _extract_rules,
    _is_effect,
    _is_formal_rule,
    _public_rule_summary,
    active_blocking_rule_summaries,
    pause_and_verify_formal_rules,
)
from scripts.twitter_social_rule_control import DEFAULT_PAUSE_STATE_PATH  # noqa: E402

DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "social_memory_live_shadow"


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Run live Author-first Social Memory shadow.")
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()

    settings = load_settings()
    api_key = settings.twitterapi_io_api_key or os.getenv("TWITTERAPI_IO_API_KEY", "")
    if not api_key:
        raise SystemExit("TWITTERAPI_IO_API_KEY missing")
    if not settings.deepseek_api_key:
        raise SystemExit("DEEPSEEK_API_KEY missing")

    engine = make_engine(settings.database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    client = TwitterApiIoClient(api_key)
    memory_service = SocialMemoryService(
        session_factory,
        triage_adapter=DeepSeekSocialSemanticTriageAdapter(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            model=settings.social_memory_triage_model,
        ),
    )
    summary = asyncio.run(
        run_social_memory_live_shadow(
            client,
            api_key=api_key,
            session_factory=session_factory,
            memory_service=memory_service,
            duration_seconds=max(1, args.duration),
            interval_seconds=max(60, args.interval),
            ws_url=args.ws_url,
            output_root=Path(args.output_root),
            shard_state_path=settings.social_x_rule_shard_state_path,
            kol_config_path=settings.social_x_kol_config_path,
            rule_max_value_chars=settings.social_x_rule_max_value_chars,
            warmup_grace_seconds=settings.social_x_warmup_grace_seconds,
            pause_state_path=DEFAULT_PAUSE_STATE_PATH,
        )
    )
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))


async def run_social_memory_live_shadow(
    client: TwitterApiIoClient,
    *,
    api_key: str,
    session_factory,
    memory_service: SocialMemoryService,
    duration_seconds: int,
    interval_seconds: int,
    ws_url: str = TWITTERAPI_IO_WS_URL,
    output_root: Path | None = DEFAULT_OUTPUT_ROOT,
    shard_state_path: Path | str | None = ROOT / "data" / "twitter_social_rule_shards.json",
    pause_state_path: Path | str = DEFAULT_PAUSE_STATE_PATH,
    kol_config_path: Path | str | None = None,
    rule_max_value_chars: int = 240,
    warmup_grace_seconds: int = 120,
    ingestor_runner: Callable[[TwitterApiIoSocialIngestor, int], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    run_started_at = datetime.now(UTC)
    runtime: dict[str, Any] = {
        "runtime_seconds": 0.0,
        "run_started_at": run_started_at.isoformat(),
        "aborted": False,
        "abort_reason": None,
        "runner_error": None,
    }
    preflight = active_blocking_rule_summaries(client)
    abort_reason = _abort_reason(preflight)
    if abort_reason:
        summary = _summary(
            runtime=runtime | {"aborted": True, "abort_reason": abort_reason},
            stream_snapshot={},
            memory_snapshot={},
            before_counts=_empty_counts(),
            after_counts=_empty_counts(),
            new_memories=[],
            cleanup=_cleanup_summary(cleanup_verified=False),
            started=started,
        )
        return _write_summary(summary, output_root)
    if kol_config_path is None:
        raise ValueError("kol_config_path is required for Social Memory live shadow runner")

    before_counts = _db_counts(session_factory)
    registry = SocialWatchRegistry(session_factory, kol_config_path=kol_config_path)
    rule_manager = TwitterApiIoRuleManager(
        client,
        interval_seconds=interval_seconds,
        max_value_chars=rule_max_value_chars,
        min_update_interval_seconds=0,
        shard_state_path=shard_state_path,
    )
    event_service = SocialEventService(session_factory, memory_processor=memory_service)
    ingestor = TwitterApiIoSocialIngestor(
        api_key=api_key,
        registry=registry,
        event_service=event_service,
        rule_manager=rule_manager,
        ws_url=ws_url,
        rule_refresh_seconds=interval_seconds,
        warmup_grace_seconds=warmup_grace_seconds,
    )
    cleanup: dict[str, Any] = {}
    runner_error: Exception | None = None
    stopped = False
    try:
        runner = ingestor_runner or _default_ingestor_runner
        await runner(ingestor, duration_seconds)
    except BaseException as exc:  # noqa: BLE001 - cleanup must run on cancellation/keyboard interrupt too.
        runner_error = exc if isinstance(exc, Exception) else RuntimeError(type(exc).__name__)
        runtime["runner_error"] = _safe_error(exc, api_key)
        if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            runtime["cancelled"] = True
    finally:
        try:
            await ingestor.stop()
            stopped = True
        finally:
            cleanup = pause_and_verify_formal_rules(client, state_path=Path(pause_state_path), api_key=api_key)
    after_counts = _db_counts(session_factory)
    new_memories = _new_memory_rows(session_factory, min_id=before_counts["social_memory_max_id"])
    runtime["runtime_seconds"] = round(time.monotonic() - started, 3)
    runtime["ingestor_stopped_before_pause"] = stopped
    summary = _summary(
        runtime=runtime,
        stream_snapshot=ingestor.stats_snapshot(),
        memory_snapshot=memory_service.stats_snapshot(),
        before_counts=before_counts,
        after_counts=after_counts,
        new_memories=new_memories,
        cleanup=cleanup,
        started=started,
    )
    if runner_error and isinstance(runner_error, (KeyboardInterrupt, asyncio.CancelledError)):
        pass
    return _write_summary(summary, output_root)


async def _default_ingestor_runner(ingestor: TwitterApiIoSocialIngestor, duration_seconds: int) -> None:
    await ingestor.start()
    await asyncio.sleep(duration_seconds)


def _summary(
    *,
    runtime: dict[str, Any],
    stream_snapshot: dict[str, Any],
    memory_snapshot: dict[str, Any],
    before_counts: dict[str, int],
    after_counts: dict[str, int],
    new_memories: list[dict[str, Any]],
    cleanup: dict[str, Any],
    started: float,
) -> dict[str, Any]:
    runtime = dict(runtime)
    runtime["runtime_seconds"] = runtime.get("runtime_seconds") or round(time.monotonic() - started, 3)
    tweets_received = int(stream_snapshot.get("tweets_received", 0) or 0)
    duplicates = int(stream_snapshot.get("duplicates", 0) or 0)
    return {
        "runtime": runtime,
        "provider_stream": {
            "provider_messages": stream_snapshot.get("messages_received", 0),
            "tweets_received": tweets_received,
            "unique_tweets": stream_snapshot.get("unique_tweets_seen", 0),
            "duplicates": duplicates,
            "duplicate_ratio": duplicates / tweets_received if tweets_received else 0,
            "provider_errors": stream_snapshot.get("provider_errors", 0),
        },
        "social_events": {
            "new_social_events": after_counts["social_event_count"] - before_counts["social_event_count"],
            "new_kol_events": _new_social_events_by_author_type(after_counts, before_counts, AUTHOR_KOL),
            "new_project_x_events": _new_social_events_by_author_type(after_counts, before_counts, AUTHOR_PROJECT_X),
            "new_dev_x_events": _new_social_events_by_author_type(after_counts, before_counts, AUTHOR_DEV_X),
            "before_max_id": before_counts["social_event_max_id"],
            "after_max_id": after_counts["social_event_max_id"],
        },
        "social_memory_triage": {
            key: memory_snapshot.get(key, 0)
            for key in (
                "project_dev_candidates",
                "deterministic_no_memory",
                "triage_calls",
                "triage_keep_memory",
                "triage_no_memory",
                "triage_failures",
                "social_memory_created",
                "duplicate_tweets_skipped",
            )
        },
        "category_counts": {
            key.removeprefix("memory_category_"): value
            for key, value in sorted(memory_snapshot.items())
            if key.startswith("memory_category_") and value
        },
        "new_social_memory_count": after_counts["social_memory_count"] - before_counts["social_memory_count"],
        "new_social_memories": new_memories,
        "cleanup": cleanup,
    }


def _db_counts(session_factory) -> dict[str, int]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        event_count = session.scalar(select(func.count()).select_from(SocialEvent)) or 0
        memory_count = session.scalar(select(func.count()).select_from(SocialMemory)) or 0
        event_max = session.scalar(select(func.max(SocialEvent.id))) or 0
        memory_max = session.scalar(select(func.max(SocialMemory.id))) or 0
        counts = {
            "social_event_count": int(event_count),
            "social_memory_count": int(memory_count),
            "social_event_max_id": int(event_max),
            "social_memory_max_id": int(memory_max),
        }
        for author_type in (AUTHOR_KOL, AUTHOR_PROJECT_X, AUTHOR_DEV_X):
            counts[f"social_event_count_{author_type}"] = int(
                session.scalar(select(func.count()).select_from(SocialEvent).where(SocialEvent.author_type == author_type))
                or 0
            )
        return counts


def _empty_counts() -> dict[str, int]:
    counts = {
        "social_event_count": 0,
        "social_memory_count": 0,
        "social_event_max_id": 0,
        "social_memory_max_id": 0,
    }
    for author_type in (AUTHOR_KOL, AUTHOR_PROJECT_X, AUTHOR_DEV_X):
        counts[f"social_event_count_{author_type}"] = 0
    return counts


def _new_social_events_by_author_type(after_counts: dict[str, int], before_counts: dict[str, int], author_type: str) -> int:
    return after_counts.get(f"social_event_count_{author_type}", 0) - before_counts.get(f"social_event_count_{author_type}", 0)


def _new_memory_rows(session_factory, *, min_id: int) -> list[dict[str, Any]]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        rows = list(
            session.scalars(
                select(SocialMemory).where(SocialMemory.id > min_id).order_by(SocialMemory.id.asc())
            )
        )
        event_rows = {
            (event.provider, event.provider_event_id, event.chain, event.token_address): event
            for event in session.scalars(
                select(SocialEvent).where(SocialEvent.provider_event_id.in_([row.tweet_id for row in rows] or [""]))
            )
        }
        result = []
        for row in rows:
            event = event_rows.get((row.provider, row.tweet_id, row.chain, row.token_address))
            result.append(
                {
                    "memory_id": row.id,
                    "chain": row.chain,
                    "token_address": row.token_address,
                    "symbol": row.symbol,
                    "source_author_type": row.source_author_type,
                    "source_username": row.source_username,
                    "tweet_id": row.tweet_id,
                    "tweet_url": row.tweet_url,
                    "event_time": row.event_time.isoformat() if row.event_time else None,
                    "category": row.category,
                    "significance": row.significance,
                    "confidence": row.confidence,
                    "information_scope": row.information_scope,
                    "summary": row.summary,
                    "original_text": event.text if event else None,
                    "review_status": "UNREVIEWED",
                }
            )
        return result


def blocking_rule_summaries_for_memory_shadow(client: TwitterApiIoClient) -> dict[str, list[dict[str, Any]]]:
    token_shadow = []
    identity_probe = []
    foreign = []
    for rule in _extract_rules(client.get_filter_rules()):
        if not _is_effect(rule):
            continue
        tag = str(rule.get("tag") or "")
        if tag == SOCIAL_RULE_TAG or tag.startswith(SOCIAL_RULE_TAG_PREFIX):
            continue
        if tag.startswith(TOKEN_SHADOW_RULE_TAG_PREFIX):
            token_shadow.append(_public_rule_summary(rule))
        elif tag.startswith(TOKEN_IDENTITY_PROBE_PREFIX):
            identity_probe.append(_public_rule_summary(rule))
        else:
            foreign.append(_public_rule_summary(rule))
    return {
        "active_token_shadow_rules": token_shadow,
        "active_token_identity_probe_rules": identity_probe,
        "active_foreign_rules": foreign,
    }


def _write_summary(summary: dict[str, Any], output_root: Path | None) -> dict[str, Any]:
    if output_root is None:
        return summary
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "summary.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    summary["output_path"] = str(path)
    return summary


def _safe_error(exc: BaseException, api_key: str) -> str:
    message = str(exc)
    if api_key:
        message = message.replace(api_key, "***")
    return message[:300]


if __name__ == "__main__":
    main()
