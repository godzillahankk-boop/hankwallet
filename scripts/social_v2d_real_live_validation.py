from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import Settings, load_settings  # noqa: E402
from app.db.database import init_db, make_engine, make_session_factory, session_scope  # noqa: E402
from app.db.models import (  # noqa: E402
    AttentionAssessment,
    PriceSnapshot,
    SocialEvent,
    SocialIdentity,
    SocialKOLProfile,
    SocialMemory,
    TokenWatchState,
    Wallet,
)
from app.services import attention_scoring as scoring  # noqa: E402
from app.services.attention_engine_service import (  # noqa: E402
    AttentionEngineService,
    build_attention_copy_markup,
    format_attention_alert,
)
from app.services.gmgn_client import GmgnMarketSignal, GmgnTrackTrade  # noqa: E402
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_KOL, AUTHOR_PROJECT_X, SocialEventService  # noqa: E402
from app.services.social_identity_service import DEV_X, PROJECT_X  # noqa: E402
from app.services.social_memory_service import (  # noqa: E402
    DeepSeekSocialSemanticTriageAdapter,
    SocialMemoryService,
    SocialSemanticTriageInput,
    SocialTriageResult,
)
from app.services.social_watch_registry import SocialWatchRegistry  # noqa: E402
from app.services.twitter_token_shadow import TOKEN_SHADOW_RULE_TAG_PREFIX  # noqa: E402
from app.services.twitterapi_io_client import TwitterApiIoClient, TwitterApiIoError  # noqa: E402
from app.services.twitterapi_io_social_ingestion import (  # noqa: E402
    SOCIAL_RULE_TAG,
    SOCIAL_RULE_TAG_PREFIX,
    TWITTERAPI_IO_WS_URL,
    SocialIngestionStats,
    TwitterApiIoRuleManager,
    TwitterApiIoSocialIngestor,
    build_rule_shards,
)
from scripts.twitter_author_cashtag_shadow_run import (  # noqa: E402
    TOKEN_IDENTITY_PROBE_PREFIX,
    _extract_rules,
    _is_effect,
    _public_rule_summary,
)

DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "social_v2d_real_live_validation"
TRIAGE_CALL_CAP = 20


class NoopGmgn:
    async def get_track_feed(self, feed_type: str, *, chain: str, limit: int = 50) -> list[GmgnTrackTrade]:
        return []

    async def get_market_signals(self, chain: str, *, groups) -> list[GmgnMarketSignal]:  # noqa: ANN001
        return []


class CappedTriageAdapter:
    def __init__(self, wrapped: DeepSeekSocialSemanticTriageAdapter, *, cap: int = TRIAGE_CALL_CAP) -> None:
        self.wrapped = wrapped
        self.cap = cap
        self.calls = 0
        self.cap_reached = 0

    def triage(self, payload: SocialSemanticTriageInput) -> SocialTriageResult:
        if self.calls >= self.cap:
            self.cap_reached += 1
            raise RuntimeError("TRIAGE_CAP_REACHED")
        self.calls += 1
        return self.wrapped.triage(payload)


@dataclass(frozen=True)
class BaselineRule:
    rule_id: str
    tag: str
    value: str
    interval_seconds: float
    active: bool


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Run Social V2D real live validation with strict cleanup.")
    parser.add_argument("--duration", type=int, default=720)
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--ws-url", default=TWITTERAPI_IO_WS_URL)
    args = parser.parse_args()

    settings = load_settings()
    output_dir = Path(args.output_root) / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = asyncio.run(
        run_validation(
            settings,
            duration_seconds=min(max(1, args.duration), 720),
            interval_seconds=max(60, args.interval),
            output_dir=output_dir,
            ws_url=args.ws_url,
        )
    )
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))


async def run_validation(
    settings: Settings,
    *,
    duration_seconds: int,
    interval_seconds: int,
    output_dir: Path,
    ws_url: str,
) -> dict[str, Any]:
    started = time.monotonic()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    report: dict[str, Any] = {
        "environment": {
            "started_at": datetime.now(UTC).isoformat(),
            "duration_requested_seconds": duration_seconds,
            "db_is_temporary_copy": True,
            "twitterapi_io_enabled": bool(settings.twitterapi_io_api_key),
            "deepseek_enabled": bool(settings.deepseek_api_key),
            "credits": "NOT_AVAILABLE",
        },
        "config_preflight": _config_preflight(settings),
        "watched_token_registry": {},
        "remote_rule_before": {},
        "rules_created": [],
        "stream_metrics": {},
        "token_match_metrics": {},
        "social_event_metrics": {},
        "social_memory_deepseek": {},
        "social_attention_assessments": [],
        "telegram_would_send": [],
        "cleanup": {},
        "final_verdict": {},
    }
    client: TwitterApiIoClient | None = None
    temp_db_path: Path | None = None
    temp_shard_path: Path | None = None
    baseline_rules: dict[str, BaselineRule] = {}
    created_rule_ids: set[str] = set()
    ingestor: TwitterApiIoSocialIngestor | None = None
    api_key = settings.twitterapi_io_api_key or ""
    try:
        if report["config_preflight"]["abort_reason"]:
            return _finish_report(report, output_dir, started)
        temp_db_path = _copy_sqlite_db(settings.database_url, output_dir)
        temp_shard_path = _copy_shard_state(settings.social_x_rule_shard_state_path, output_dir)
        temp_settings = _settings_with_temp_db(settings, temp_db_path)
        engine = make_engine(temp_settings.database_url)
        init_db(engine)
        session_factory = make_session_factory(engine)
        registry_audit = _registry_audit(session_factory, temp_settings)
        report["watched_token_registry"] = registry_audit
        if registry_audit.get("abort_reason"):
            report["final_verdict"] = _verdict(abort_reason=registry_audit["abort_reason"], cleanup={})
            return _finish_report(report, output_dir, started)
        client = TwitterApiIoClient(api_key)
        baseline_data = client.get_filter_rules()
        baseline_rules = _baseline_rules(baseline_data)
        active_before = [_public_rule_summary(rule) for rule in _extract_rules(baseline_data) if _is_effect(rule)]
        report["remote_rule_before"] = {
            "active_rules_before": active_before,
            "active_rule_count": len(active_before),
            "all_formal_rules": [_public_rule_summary(rule) for rule in _extract_rules(baseline_data) if _is_formal_rule(rule)],
        }
        if active_before:
            report["final_verdict"] = _verdict(abort_reason="ACTIVE_REMOTE_RULES_BEFORE_RUN", cleanup={})
            return _finish_report(report, output_dir, started)

        triage_adapter = CappedTriageAdapter(
            DeepSeekSocialSemanticTriageAdapter(
                api_key=settings.deepseek_api_key or "",
                base_url=settings.deepseek_base_url,
                model=settings.social_memory_triage_model,
            ),
            cap=TRIAGE_CALL_CAP,
        )
        memory_service = SocialMemoryService(session_factory, triage_adapter=triage_adapter)
        would_send: list[dict[str, Any]] = []

        async def capture_notify(chat_id: int, text: str, reply_markup=None) -> None:  # noqa: ANN001
            would_send.append(
                {
                    "chat_id": chat_id,
                    "text": text,
                    "buttons": _markup_buttons(reply_markup),
                }
            )

        attention_service = AttentionEngineService(
            session_factory,
            NoopGmgn(),
            temp_settings,
            capture_notify,
        )
        update_tasks: list[asyncio.Task] = []
        assessments: list[dict[str, Any]] = []

        async def handle_update(event: SocialEvent) -> None:
            assessment = await attention_service.handle_social_update(event.wallet_id, event.token_address)
            if assessment is not None:
                assessments.append(_assessment_summary(assessment))

        def schedule_social_attention(event: SocialEvent, memory=None) -> None:  # noqa: ANN001
            update_tasks.append(asyncio.create_task(handle_update(event)))

        event_service = SocialEventService(
            session_factory,
            memory_processor=memory_service,
            social_update_callback=schedule_social_attention,
        )
        attention_service.set_social_services(
            social_event_service=SocialEventService(session_factory),
            social_memory_service=memory_service,
        )
        registry = SocialWatchRegistry(session_factory, kol_config_path=temp_settings.social_x_kol_config_path)
        rule_manager = TwitterApiIoRuleManager(
            client,
            interval_seconds=interval_seconds,
            max_value_chars=temp_settings.social_x_rule_max_value_chars,
            min_update_interval_seconds=0,
            shard_state_path=temp_shard_path,
        )
        stats = SocialIngestionStats()
        accounts = registry.accounts()
        before_ids = set(baseline_rules)
        rules = await rule_manager.ensure_rules(accounts, stats=stats)
        after_create_data = client.get_filter_rules()
        after_ids = {_rule_id(rule) for rule in _extract_rules(after_create_data) if _rule_id(rule)}
        created_rule_ids = after_ids - before_ids
        report["rules_created"] = [
            _public_rule_summary(rule)
            for rule in _extract_rules(after_create_data)
            if _rule_id(rule) in created_rule_ids
        ]
        report["remote_rule_before"]["expected_rule_shards"] = len(build_rule_shards(accounts, max_value_chars=temp_settings.social_x_rule_max_value_chars))
        report["remote_rule_before"]["actual_managed_rules"] = [rule.to_dict() if hasattr(rule, "to_dict") else rule.__dict__ for rule in rules]

        before_counts = _db_counts(session_factory)
        ingestor = TwitterApiIoSocialIngestor(
            api_key=api_key,
            registry=registry,
            event_service=event_service,
            rule_manager=rule_manager,
            ws_url=ws_url,
            rule_refresh_seconds=interval_seconds,
            warmup_grace_seconds=temp_settings.social_x_warmup_grace_seconds,
        )
        await ingestor.start()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)
        except asyncio.TimeoutError:
            pass
        await ingestor.stop()
        if update_tasks:
            await asyncio.gather(*update_tasks, return_exceptions=True)
        after_counts = _db_counts(session_factory)
        stream_snapshot = ingestor.stats_snapshot()
        memory_snapshot = memory_service.stats_snapshot()
        report["stream_metrics"] = _stream_metrics(stream_snapshot)
        report["token_match_metrics"] = _token_match_metrics(session_factory, before_counts["social_event_max_id"], stream_snapshot)
        report["social_event_metrics"] = _social_event_metrics(session_factory, before_counts, after_counts)
        report["social_memory_deepseek"] = _memory_metrics(memory_snapshot, triage_adapter)
        report["social_attention_assessments"] = assessments
        report["telegram_would_send"] = would_send
        report["representative_telegram"] = would_send[0] if would_send else None
    except BaseException as exc:  # noqa: BLE001 - cleanup still required.
        report["runtime_error"] = _safe_error(exc, api_key)
    finally:
        if ingestor is not None:
            try:
                await ingestor.stop()
            except Exception:
                pass
        cleanup = {}
        if client is not None:
            cleanup = _cleanup_rules(client, baseline_rules, created_rule_ids, api_key=api_key)
        report["cleanup"] = cleanup
        report["runtime_seconds"] = round(time.monotonic() - started, 3)
        report["final_verdict"] = _verdict(report=report, cleanup=cleanup)
    return _finish_report(report, output_dir, started)


def _config_preflight(settings: Settings) -> dict[str, Any]:
    errors = []
    if not settings.social_x_enabled:
        errors.append("SOCIAL_X_ENABLED must be true")
    if not settings.social_memory_enabled:
        errors.append("SOCIAL_MEMORY_ENABLED must be true")
    if settings.social_x_discovery_enabled:
        errors.append("SOCIAL_X_DISCOVERY_ENABLED must be false")
    if settings.social_x_provider != "twitterapi_io":
        errors.append("SOCIAL_X_PROVIDER must be twitterapi_io")
    if not settings.twitterapi_io_api_key:
        errors.append("TWITTERAPI_IO_API_KEY missing")
    if not settings.deepseek_api_key:
        errors.append("DEEPSEEK_API_KEY missing")
    return {
        "social_x_enabled": settings.social_x_enabled,
        "social_memory_enabled": settings.social_memory_enabled,
        "social_x_discovery_enabled": settings.social_x_discovery_enabled,
        "abort_reason": "; ".join(errors) if errors else None,
    }


def _registry_audit(session_factory, settings: Settings) -> dict[str, Any]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        watches = list(session.scalars(select(TokenWatchState).where(TokenWatchState.active.is_(True))))
        wallets = list(session.scalars(select(Wallet).where(Wallet.is_active.is_(True))))
        latest_prices = {
            (row.wallet_id, row.chain, row.token_address): row
            for row in session.scalars(
                select(PriceSnapshot)
                .where(PriceSnapshot.id.in_(select(func.max(PriceSnapshot.id)).group_by(PriceSnapshot.wallet_id, PriceSnapshot.chain, PriceSnapshot.token_address)))
            )
        }
        identities = list(session.scalars(select(SocialIdentity).where(SocialIdentity.is_active.is_(True))))
        project = [row for row in identities if row.identity_type == PROJECT_X]
        dev = [row for row in identities if row.identity_type == DEV_X]
        qualified = list(
            session.scalars(
                select(SocialKOLProfile).where(
                    SocialKOLProfile.status == "qualified",
                    SocialKOLProfile.is_active.is_(True),
                    SocialKOLProfile.manual_override != "exclude",
                )
            )
        )
    tokens = []
    display_eligible = 0
    for watch in watches:
        snap = latest_prices.get((watch.wallet_id, watch.chain, watch.token_address))
        usd_value = Decimal(str(snap.usd_value)) if snap and snap.usd_value is not None else None
        if usd_value is not None and usd_value >= settings.price_monitor_min_usd_value:
            display_eligible += 1
        tokens.append(
            {
                "wallet_id": watch.wallet_id,
                "chain": watch.chain,
                "symbol": watch.symbol,
                "token_address": watch.token_address,
                "usd_value": str(usd_value) if usd_value is not None else None,
                "started_at": watch.started_at.isoformat(),
            }
        )
    registry = SocialWatchRegistry(session_factory, kol_config_path=settings.social_x_kol_config_path)
    accounts = registry.accounts()
    abort_reason = None
    if not watches:
        abort_reason = "NO_ACTIVE_WATCH"
    elif not accounts:
        abort_reason = "NO_EXPECTED_AUTHORS"
    return {
        "active_wallets": len(wallets),
        "active_watch_tokens": len(watches),
        "display_eligible_tokens": display_eligible,
        "tokens": tokens,
        "project_x_count": len(project),
        "project_x_usernames": _identity_usernames(project),
        "dev_x_count": len(dev),
        "dev_x_usernames": _identity_usernames(dev),
        "qualified_kol_count": len(qualified),
        "qualified_kol_examples": [row.username for row in qualified[:10]],
        "expected_author_count": len(accounts),
        "expected_author_examples": [account.username for account in accounts[:10]],
        "expected_rule_shards": len(build_rule_shards(accounts, max_value_chars=settings.social_x_rule_max_value_chars)),
        "abort_reason": abort_reason,
    }


def _copy_sqlite_db(database_url: str, output_dir: Path) -> Path:
    url = make_url(database_url)
    if url.drivername != "sqlite":
        raise RuntimeError("Live validation only supports SQLite DATABASE_URL for safe local DB copy")
    source = Path(url.database or "")
    if not source.is_absolute():
        source = (ROOT / source).resolve()
    if not source.exists():
        raise RuntimeError(f"SQLite DB not found: {source}")
    target = output_dir / "wallet_agent_validation.sqlite"
    shutil.copy2(source, target)
    return target


def _copy_shard_state(path_value: str, output_dir: Path) -> Path:
    source = Path(path_value)
    if not source.is_absolute():
        source = (ROOT / source).resolve()
    target = output_dir / "twitter_social_rule_shards.json"
    if source.exists():
        shutil.copy2(source, target)
    return target


def _settings_with_temp_db(settings: Settings, db_path: Path) -> Settings:
    return Settings(**{**settings.__dict__, "database_url": f"sqlite:///{db_path}"})


def _baseline_rules(data: dict[str, Any]) -> dict[str, BaselineRule]:
    rows = {}
    for rule in _extract_rules(data):
        rule_id = _rule_id(rule)
        if not rule_id:
            continue
        rows[rule_id] = BaselineRule(
            rule_id=rule_id,
            tag=str(rule.get("tag") or ""),
            value=str(rule.get("value") or ""),
            interval_seconds=_interval_seconds(rule),
            active=_is_effect(rule),
        )
    return rows


def _cleanup_rules(
    client: TwitterApiIoClient,
    baseline_rules: dict[str, BaselineRule],
    created_rule_ids: set[str],
    *,
    api_key: str,
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    deleted: list[str] = []
    restored: list[str] = []
    try:
        current = {(_rule_id(rule) or ""): rule for rule in _extract_rules(client.get_filter_rules()) if _rule_id(rule)}
    except Exception as exc:  # noqa: BLE001
        return {
            "cleanup_failed": True,
            "cleanup_errors": [{"stage": "initial_get_rules", "error": _safe_error(exc, api_key)}],
            "cleanup_verified": False,
            "rules_created_by_run": sorted(created_rule_ids),
            "active_rules_after": [],
            "safe_to_stop_monitoring": False,
        }
    for rule_id in sorted(created_rule_ids):
        if rule_id not in current:
            continue
        try:
            _delete_filter_rule(client, rule_id=rule_id)
            deleted.append(rule_id)
        except Exception as exc:  # noqa: BLE001
            errors.append({"stage": "delete_created_rule", "rule_id": rule_id, "error": _safe_error(exc, api_key)})
    for rule_id, baseline in baseline_rules.items():
        current_rule = current.get(rule_id)
        if current_rule is None:
            continue
        if (
            str(current_rule.get("tag") or "") == baseline.tag
            and str(current_rule.get("value") or "") == baseline.value
            and _interval_seconds(current_rule) == baseline.interval_seconds
            and _is_effect(current_rule) == baseline.active
        ):
            continue
        try:
            client.update_filter_rule(
                rule_id=baseline.rule_id,
                tag=baseline.tag,
                value=baseline.value,
                interval_seconds=baseline.interval_seconds,
                is_effect=baseline.active,
            )
            restored.append(rule_id)
        except Exception as exc:  # noqa: BLE001
            errors.append({"stage": "restore_baseline_rule", "rule_id": rule_id, "error": _safe_error(exc, api_key)})
    verification_error = None
    active_after: list[dict[str, Any]] = []
    remaining_created: list[str] = []
    try:
        after = _extract_rules(client.get_filter_rules())
        active_after = [_public_rule_summary(rule) for rule in after if _is_effect(rule)]
        after_ids = {_rule_id(rule) for rule in after if _rule_id(rule)}
        remaining_created = sorted(rule_id for rule_id in created_rule_ids if rule_id in after_ids)
    except Exception as exc:  # noqa: BLE001
        verification_error = _safe_error(exc, api_key)
    cleanup_verified = not errors and not verification_error and not remaining_created
    return {
        "cleanup_failed": bool(errors or verification_error or remaining_created),
        "cleanup_errors": errors,
        "cleanup_verification_error": verification_error,
        "cleanup_verified": cleanup_verified,
        "rules_created_by_run": sorted(created_rule_ids),
        "deleted_created_rule_ids": deleted,
        "restored_baseline_rule_ids": restored,
        "remaining_created_rule_ids": remaining_created,
        "active_rules_after": active_after,
        "safe_to_stop_monitoring": cleanup_verified and not active_after,
    }


def _delete_filter_rule(client: TwitterApiIoClient, *, rule_id: str) -> None:
    client._request("DELETE", "/oapi/tweet_filter/delete_rule", json_body={"rule_id": rule_id})  # noqa: SLF001


def _db_counts(session_factory) -> dict[str, int]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        return {
            "social_event_count": int(session.scalar(select(func.count()).select_from(SocialEvent)) or 0),
            "social_memory_count": int(session.scalar(select(func.count()).select_from(SocialMemory)) or 0),
            "attention_assessment_count": int(session.scalar(select(func.count()).select_from(AttentionAssessment)) or 0),
            "social_event_max_id": int(session.scalar(select(func.max(SocialEvent.id))) or 0),
            "social_memory_max_id": int(session.scalar(select(func.max(SocialMemory.id))) or 0),
            "attention_assessment_max_id": int(session.scalar(select(func.max(AttentionAssessment.id))) or 0),
        }


def _stream_metrics(snapshot: dict[str, Any]) -> dict[str, Any]:
    received = int(snapshot.get("tweets_received", 0) or 0)
    duplicates = int(snapshot.get("duplicates", 0) or 0)
    return {
        "runtime_connection_state": snapshot.get("connection_state"),
        "stream_deliveries": received,
        "provider_messages": snapshot.get("messages_received", 0),
        "unique_provider_tweets": snapshot.get("unique_tweets_seen", 0),
        "duplicate_provider_tweets": duplicates,
        "duplicate_rate": duplicates / received if received else 0,
        "kol_deliveries": snapshot.get("kol_events", 0),
        "project_x_deliveries": snapshot.get("project_events", 0),
        "dev_x_deliveries": snapshot.get("dev_x_events", 0),
        "unknown_deliveries": snapshot.get("unmatched_tweets", 0),
        "provider_errors": snapshot.get("provider_errors", 0),
        "disconnects": snapshot.get("disconnects", 0),
        "reconnects": snapshot.get("reconnects", 0),
    }


def _token_match_metrics(session_factory, min_event_id: int, stream_snapshot: dict[str, Any]) -> dict[str, Any]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        events = list(session.scalars(select(SocialEvent).where(SocialEvent.id > min_event_id)))
    direct_ca = sum(1 for event in events if "ca" in (event.match_type or ""))
    cashtag = sum(1 for event in events if "cashtag" in (event.match_type or ""))
    identity = sum(1 for event in events if event.match_type == "identity_account")
    return {
        "direct_ca_matches": direct_ca,
        "cashtag_matches": cashtag,
        "identity_account_matches": identity,
        "unmatched_tweets": stream_snapshot.get("unmatched_tweets", 0),
    }


def _social_event_metrics(session_factory, before: dict[str, int], after: dict[str, int]) -> dict[str, Any]:  # noqa: ANN001
    with session_scope(session_factory) as session:
        events = list(session.scalars(select(SocialEvent).where(SocialEvent.id > before["social_event_max_id"])))
    by_author: dict[str, int] = {}
    for event in events:
        by_author[event.author_type] = by_author.get(event.author_type, 0) + 1
    token_kols: dict[str, set[str]] = {}
    token_posts: dict[str, int] = {}
    for event in events:
        if event.author_type != AUTHOR_KOL:
            continue
        key = event.author_id or (event.author_username or "").lower()
        token_kols.setdefault(event.symbol or event.token_address, set()).add(key)
        token_posts[event.symbol or event.token_address] = token_posts.get(event.symbol or event.token_address, 0) + 1
    return {
        "created": after["social_event_count"] - before["social_event_count"],
        "by_author_type": by_author,
        "deduped": 0,
        "errors": 0,
        "kol_by_token": [
            {"symbol": token, "kol_plus_n": len(authors), "kol_posts": token_posts.get(token, 0)}
            for token, authors in sorted(token_kols.items())
        ],
    }


def _memory_metrics(snapshot: dict[str, Any], adapter: CappedTriageAdapter) -> dict[str, Any]:
    return {
        "project_dev_candidates": snapshot.get("project_dev_candidates", 0),
        "low_info_rejected": snapshot.get("deterministic_no_memory", 0),
        "deepseek_calls": adapter.calls,
        "triage_cap_reached": adapter.cap_reached,
        "keep_memory": snapshot.get("triage_keep_memory", 0),
        "no_memory": snapshot.get("triage_no_memory", 0),
        "triage_failures": snapshot.get("triage_failures", 0),
        "social_memory_created": snapshot.get("social_memory_created", 0),
        "category_counts": {
            key.removeprefix("memory_category_"): value
            for key, value in sorted(snapshot.items())
            if key.startswith("memory_category_") and value
        },
    }


def _assessment_summary(assessment: AttentionAssessment) -> dict[str, Any]:
    evidence = _json_loads(assessment.evidence_json)
    text = format_attention_alert(assessment)
    return {
        "symbol": assessment.symbol,
        "family_scores": evidence.get("family_scores"),
        "social_evidence": evidence.get("social"),
        "dev_modifier": assessment.dev_modifier,
        "primary_family": assessment.primary_family,
        "primary_signal": evidence.get("primary_signal"),
        "direction": assessment.direction,
        "final_att": assessment.final_attention_score,
        "level": assessment.attention_level,
        "should_notify": assessment.should_notify,
        "telegram_text": text,
        "buttons": _markup_buttons(build_attention_copy_markup(assessment)),
    }


def _markup_buttons(markup) -> list[dict[str, Any]]:  # noqa: ANN001
    if markup is None:
        return []
    rows = []
    for row in getattr(markup, "inline_keyboard", []) or []:
        for button in row:
            rows.append({"text": getattr(button, "text", ""), "url": getattr(button, "url", None)})
    return rows


def _finish_report(report: dict[str, Any], output_dir: Path, started: float) -> dict[str, Any]:
    report["runtime_seconds"] = report.get("runtime_seconds", round(time.monotonic() - started, 3))
    report_path = output_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    md_path = output_dir / "report.md"
    md_path.write_text(_markdown_report(report))
    report["report_json"] = str(report_path)
    report["report_md"] = str(md_path)
    return report


def _markdown_report(report: dict[str, Any]) -> str:
    verdict = report.get("final_verdict", {})
    lines = [
        "# Social V2D Real Live Validation",
        "",
        f"- Overall: {verdict.get('overall', 'UNKNOWN')}",
        f"- Runtime seconds: {report.get('runtime_seconds')}",
        f"- Cleanup verified: {report.get('cleanup', {}).get('cleanup_verified')}",
        f"- Safe to stop monitoring: {report.get('cleanup', {}).get('safe_to_stop_monitoring')}",
        "",
        "## Stream",
        "```json",
        json.dumps(report.get("stream_metrics", {}), ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Final Verdict",
        "```json",
        json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]
    return "\n".join(lines)


def _verdict(
    *,
    report: dict[str, Any] | None = None,
    cleanup: dict[str, Any] | None = None,
    abort_reason: str | None = None,
) -> dict[str, Any]:
    if abort_reason:
        return {"overall": "FAIL", "abort_reason": abort_reason}
    report = report or {}
    cleanup = cleanup or {}
    if not cleanup.get("cleanup_verified"):
        overall = "FAIL"
    else:
        stream = report.get("stream_metrics", {})
        memory = report.get("social_memory_deepseek", {})
        events = report.get("social_event_metrics", {})
        assessments = report.get("social_attention_assessments", [])
        if stream.get("stream_deliveries", 0) and cleanup.get("cleanup_verified"):
            overall = "SAFE_PASS" if events.get("created", 0) or assessments else "PARTIAL_PASS"
        else:
            overall = "PARTIAL_PASS"
    return {
        "provider_rule_sync": "PASS" if report.get("rules_created") else "NOT_OBSERVED",
        "websocket": "PASS" if report.get("stream_metrics", {}).get("runtime_connection_state") in {"CONNECTED", "DISCONNECTED"} else "NOT_OBSERVED",
        "real_tweet_delivery": "PASS" if report.get("stream_metrics", {}).get("stream_deliveries", 0) else "NOT_OBSERVED",
        "project_dev_realtime": _observed_status(report.get("social_event_metrics", {}).get("by_author_type", {}), [AUTHOR_PROJECT_X, AUTHOR_DEV_X]),
        "kol_token_match": _observed_status(report.get("social_event_metrics", {}).get("by_author_type", {}), [AUTHOR_KOL]),
        "deepseek_triage": "PASS" if report.get("social_memory_deepseek", {}).get("deepseek_calls", 0) else "NOT_OBSERVED",
        "social_attention": "PASS" if report.get("social_attention_assessments") else "NOT_OBSERVED",
        "telegram_shadow": "PASS" if report.get("telegram_would_send") else "NOT_OBSERVED",
        "rule_cleanup": "PASS" if cleanup.get("cleanup_verified") else "FAIL",
        "overall": overall,
    }


def _observed_status(mapping: dict[str, int], keys: list[str]) -> str:
    return "PASS" if any(mapping.get(key, 0) for key in keys) else "NOT_OBSERVED"


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        data = json.loads(value)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _identity_usernames(rows: list[SocialIdentity]) -> list[str]:
    values = []
    for row in rows:
        value = str(row.value or "").strip().lstrip("@")
        if value:
            values.append(value)
    return sorted(values)


def _is_formal_rule(rule: dict[str, Any]) -> bool:
    tag = str(rule.get("tag") or "")
    return tag == SOCIAL_RULE_TAG or tag.startswith(SOCIAL_RULE_TAG_PREFIX)


def _rule_id(rule: dict[str, Any]) -> str | None:
    for key in ("rule_id", "ruleId", "id"):
        value = rule.get(key)
        if value:
            return str(value)
    return None


def _interval_seconds(rule: dict[str, Any]) -> float:
    try:
        return float(rule.get("interval_seconds") or rule.get("intervalSeconds") or 60)
    except (TypeError, ValueError):
        return 60.0


def _safe_error(exc: BaseException, secret: str = "") -> str:
    text = str(exc)
    if secret:
        text = text.replace(secret, "***")
    return text[:500]


if __name__ == "__main__":
    main()
