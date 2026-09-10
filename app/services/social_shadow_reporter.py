from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialKOLProfile, TokenWatchState
from app.services.social_discovery_service import SocialDiscoveryService, _latest_valid_price_snapshot
from app.services.social_kol_service import MANUAL_EXCLUDE, STATUS_CANDIDATE, STATUS_QUALIFIED

logger = logging.getLogger(__name__)


class SocialShadowReporter:
    def __init__(
        self,
        *,
        session_factory: sessionmaker,
        min_usd_value: Decimal,
        stream_ingestor: Any | None = None,
        discovery_service: SocialDiscoveryService | None = None,
        twitter_client: Any | None = None,
        deepseek_classifier: Any | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.min_usd_value = min_usd_value
        self.stream_ingestor = stream_ingestor
        self.discovery_service = discovery_service
        self.twitter_client = twitter_client
        self.deepseek_classifier = deepseek_classifier

    def stats_snapshot(self) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        snapshot.update(self._db_counts())
        snapshot.update(_prefixed("stream", _safe_stats_snapshot(self.stream_ingestor)))
        snapshot.update(_prefixed("discovery", _safe_stats_snapshot(self.discovery_service)))
        snapshot.update(_prefixed("twitter", _safe_stats_snapshot(self.twitter_client)))
        snapshot.update(_prefixed("deepseek", _safe_stats_snapshot(self.deepseek_classifier)))
        return snapshot

    def report(self) -> None:
        self._log("SOCIAL_SHADOW")

    def report_final(self) -> None:
        self._log("SOCIAL_SHADOW_FINAL")

    def _log(self, marker: str) -> None:
        try:
            logger.info("%s %s", marker, json.dumps(self.stats_snapshot(), sort_keys=True, default=str))
        except Exception as exc:
            logger.warning("Social shadow reporter failed: %s", exc)

    def _db_counts(self) -> dict[str, int]:
        with session_scope(self.session_factory) as session:
            active_watches = session.scalar(
                select(func.count()).select_from(TokenWatchState).where(TokenWatchState.active.is_(True))
            ) or 0
            qualified = session.scalar(
                select(func.count())
                .select_from(SocialKOLProfile)
                .where(
                    SocialKOLProfile.status == STATUS_QUALIFIED,
                    SocialKOLProfile.is_active.is_(True),
                    SocialKOLProfile.manual_override != MANUAL_EXCLUDE,
                )
            ) or 0
            candidates = session.scalar(
                select(func.count())
                .select_from(SocialKOLProfile)
                .where(
                    SocialKOLProfile.status == STATUS_CANDIDATE,
                    SocialKOLProfile.is_active.is_(True),
                )
            ) or 0
            watches = list(
                session.scalars(
                    select(TokenWatchState)
                    .where(TokenWatchState.active.is_(True))
                    .order_by(TokenWatchState.id.asc())
                )
            )
            eligible = 0
            for watch in watches:
                latest = _latest_valid_price_snapshot(session, watch)
                if latest and latest.usd_value is not None and Decimal(str(latest.usd_value)) >= self.min_usd_value:
                    eligible += 1
            return {
                "active_watches": int(active_watches),
                "eligible_discovery_watches": eligible,
                "kol_pool_qualified": int(qualified),
                "kol_candidates": int(candidates),
            }


def _safe_stats_snapshot(source: Any | None) -> dict[str, Any]:
    if source is None or not hasattr(source, "stats_snapshot"):
        return {}
    try:
        snapshot = source.stats_snapshot()
    except Exception as exc:
        logger.warning("Social shadow stats snapshot failed source=%s: %s", source.__class__.__name__, exc)
        return {}
    return snapshot if isinstance(snapshot, dict) else {}


def _prefixed(prefix: str, values: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in values.items()}
