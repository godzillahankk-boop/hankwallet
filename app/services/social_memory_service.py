from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db.database import session_scope
from app.db.models import SocialEvent, SocialMemory
from app.services.social_event_service import AUTHOR_DEV_X, AUTHOR_PROJECT_X
from app.services.social_identity_service import normalize_username

logger = logging.getLogger(__name__)

DECISION_KEEP_MEMORY = "KEEP_MEMORY"
DECISION_NO_MEMORY = "NO_MEMORY"

CATEGORY_DEVELOPMENT = "development"
CATEGORY_ROADMAP = "roadmap"
CATEGORY_SECURITY = "security"
CATEGORY_TOKENOMICS = "tokenomics"
CATEGORY_LISTING = "listing"
CATEGORY_INTEGRATION = "integration"
CATEGORY_PARTNERSHIP = "partnership"
CATEGORY_FUNDING = "funding"
CATEGORY_TEAM = "team"
CATEGORY_PROJECT_DIRECTION = "project_direction"
CATEGORY_INCIDENT_RESPONSE = "incident_response"
CATEGORY_OTHER_MEANINGFUL = "other_meaningful"
CATEGORY_LOW_INFORMATION = "low_information"

SIGNIFICANCE_LOW = "low"
SIGNIFICANCE_MEDIUM = "medium"
SIGNIFICANCE_HIGH = "high"

CONFIDENCE_LOW = "low"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_HIGH = "high"

SCOPE_PROJECT = "project"
SCOPE_TOKEN = "token"
SCOPE_DEV = "dev"
SCOPE_SECURITY = "security"
SCOPE_ECOSYSTEM = "ecosystem"
SCOPE_UNKNOWN = "unknown"

TRIAGE_VERSION = "social_v2c_project_dev_memory_v1"

ALLOWED_DECISIONS = {DECISION_KEEP_MEMORY, DECISION_NO_MEMORY}
ALLOWED_CATEGORIES = {
    CATEGORY_DEVELOPMENT,
    CATEGORY_ROADMAP,
    CATEGORY_SECURITY,
    CATEGORY_TOKENOMICS,
    CATEGORY_LISTING,
    CATEGORY_INTEGRATION,
    CATEGORY_PARTNERSHIP,
    CATEGORY_FUNDING,
    CATEGORY_TEAM,
    CATEGORY_PROJECT_DIRECTION,
    CATEGORY_INCIDENT_RESPONSE,
    CATEGORY_OTHER_MEANINGFUL,
    CATEGORY_LOW_INFORMATION,
}
ALLOWED_SIGNIFICANCE = {SIGNIFICANCE_LOW, SIGNIFICANCE_MEDIUM, SIGNIFICANCE_HIGH}
ALLOWED_CONFIDENCE = {CONFIDENCE_LOW, CONFIDENCE_MEDIUM, CONFIDENCE_HIGH}
ALLOWED_SCOPES = {SCOPE_PROJECT, SCOPE_TOKEN, SCOPE_DEV, SCOPE_SECURITY, SCOPE_ECOSYSTEM, SCOPE_UNKNOWN}
REQUIRED_TRIAGE_KEYS = {
    "decision",
    "category",
    "significance",
    "summary",
    "reason",
    "confidence",
    "information_scope",
}

PROJECT_DEV_AUTHOR_TYPES = {AUTHOR_PROJECT_X, AUTHOR_DEV_X}

GENERIC_LOW_INFO_PHRASES = {
    "gm",
    "gn",
    "lfg",
    "we are cooking",
    "cooking",
    "big things coming",
    "stay tuned",
    "thank you",
    "thanks",
}

GREETING_WORDS = {"gm", "gn", "lfg"}
THANKS_WORDS = {"thanks"}
LOW_INFO_SUFFIX_WORDS = {"fam", "everyone", "community", "frens", "holders"}

OBVIOUS_SHILL_PATTERNS = (
    re.compile(r"\b(to the moon|moon soon|send it|ape in|buy now|100x|pump it)\b", re.IGNORECASE),
)


@dataclass(frozen=True)
class SocialSemanticTriageInput:
    author_type: str
    author_username: str | None
    token_symbol: str | None
    text: str | None
    tweet_url: str | None
    event_time: datetime


@dataclass(frozen=True)
class SocialTriageResult:
    decision: str
    category: str
    significance: str
    summary: str
    reason: str
    confidence: str
    information_scope: str


class SocialSemanticTriageAdapter(Protocol):
    def triage(self, payload: SocialSemanticTriageInput) -> SocialTriageResult:
        ...


@dataclass
class SocialMemoryStats:
    project_dev_candidates: int = 0
    deterministic_no_memory: int = 0
    triage_calls: int = 0
    triage_keep_memory: int = 0
    triage_no_memory: int = 0
    triage_failures: int = 0
    social_memory_created: int = 0
    duplicate_tweets_skipped: int = 0
    category_counts: dict[str, int] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "project_dev_candidates": self.project_dev_candidates,
            "deterministic_no_memory": self.deterministic_no_memory,
            "triage_calls": self.triage_calls,
            "triage_keep_memory": self.triage_keep_memory,
            "triage_no_memory": self.triage_no_memory,
            "triage_failures": self.triage_failures,
            "social_memory_created": self.social_memory_created,
            "duplicate_tweets_skipped": self.duplicate_tweets_skipped,
        }
        for category in sorted(ALLOWED_CATEGORIES):
            data[f"memory_category_{category}"] = self.category_counts.get(category, 0)
        return data


class SocialMemoryService:
    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        triage_adapter: SocialSemanticTriageAdapter | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.triage_adapter = triage_adapter
        self.stats = SocialMemoryStats()
        self._processed_keys: set[tuple[str, str, str, str]] = set()

    def process_event(self, event: SocialEvent) -> SocialMemory | None:
        if event.author_type not in PROJECT_DEV_AUTHOR_TYPES:
            return None
        self.stats.project_dev_candidates += 1
        key = _processed_key(event)
        if key in self._processed_keys:
            self.stats.duplicate_tweets_skipped += 1
            return None
        with session_scope(self.session_factory) as session:
            if self._memory_exists(session, event):
                self._processed_keys.add(key)
                self.stats.duplicate_tweets_skipped += 1
                return None
        deterministic = deterministic_no_memory_reason(event.text)
        if deterministic:
            self._processed_keys.add(key)
            self.stats.deterministic_no_memory += 1
            return None
        if self.triage_adapter is None:
            self.stats.triage_failures += 1
            logger.warning("Social memory triage adapter missing event_id=%s", event.id)
            return None
        try:
            self.stats.triage_calls += 1
            result = self.triage_adapter.triage(_triage_input(event))
            result = validate_triage_result(result)
        except Exception as exc:  # noqa: BLE001 - fail closed; do not write durable memory.
            self.stats.triage_failures += 1
            logger.warning("Social memory triage failed event_id=%s: %s", event.id, str(exc)[:200])
            return None
        if result.decision == DECISION_NO_MEMORY:
            self._processed_keys.add(key)
            self.stats.triage_no_memory += 1
            return None
        self.stats.triage_keep_memory += 1
        memory = self._create_memory(event, result)
        if memory is not None:
            self._processed_keys.add(key)
        return memory

    def get_recent_memories(
        self,
        *,
        chain: str,
        token_address: str,
        since: datetime | None = None,
        categories: set[str] | None = None,
        limit: int = 20,
    ) -> list[SocialMemory]:
        with session_scope(self.session_factory) as session:
            query = select(SocialMemory).where(
                SocialMemory.chain == chain,
                SocialMemory.token_address == token_address.lower(),
            )
            if since is not None:
                query = query.where(SocialMemory.event_time >= since)
            if categories:
                query = query.where(SocialMemory.category.in_(sorted(categories)))
            rows = list(
                session.scalars(
                    query.order_by(SocialMemory.event_time.desc(), SocialMemory.id.desc()).limit(max(1, limit))
                )
            )
            for row in rows:
                session.expunge(row)
            return rows

    def stats_snapshot(self) -> dict[str, Any]:
        return self.stats.snapshot()

    def _create_memory(self, event: SocialEvent, result: SocialTriageResult) -> SocialMemory | None:
        memory = SocialMemory(
            chain=event.chain,
            token_address=event.token_address.lower(),
            symbol=event.symbol,
            project_identity=event.author_username,
            project_key=_project_key(event),
            source_author_id=event.author_id,
            source_username=event.author_username,
            source_author_type=event.author_type,
            provider=event.provider,
            tweet_id=event.provider_event_id,
            tweet_url=event.tweet_url or tweet_url(event.author_username, event.provider_event_id),
            event_time=event.posted_at,
            category=result.category,
            summary=result.summary,
            significance=result.significance,
            confidence=result.confidence,
            information_scope=result.information_scope,
            raw_reference_hash=None,
            triage_version=TRIAGE_VERSION,
        )
        with session_scope(self.session_factory) as session:
            try:
                with session.begin_nested():
                    session.add(memory)
                    session.flush()
            except IntegrityError:
                self._processed_keys.add(_processed_key(event))
                self.stats.duplicate_tweets_skipped += 1
                return None
            self.stats.social_memory_created += 1
            self.stats.category_counts[result.category] = self.stats.category_counts.get(result.category, 0) + 1
            session.expunge(memory)
            return memory

    def _memory_exists(self, session, event: SocialEvent) -> bool:  # noqa: ANN001
        return (
            session.scalar(
                select(SocialMemory.id).where(
                    SocialMemory.provider == event.provider,
                    SocialMemory.tweet_id == event.provider_event_id,
                    SocialMemory.chain == event.chain,
                    SocialMemory.token_address == event.token_address,
                )
            )
            is not None
        )


def deterministic_no_memory_reason(text: str | None) -> str | None:
    clean = _clean_text(text)
    if not clean:
        return "empty_or_image_only"
    if _is_generic_low_information(clean):
        return "generic_low_information"
    if _emoji_or_symbol_only(clean):
        return "emoji_or_symbol_only"
    if len(clean) <= 100 and any(pattern.search(clean) for pattern in OBVIOUS_SHILL_PATTERNS):
        return "obvious_price_shill"
    return None


def validate_triage_result(result: SocialTriageResult | dict[str, Any]) -> SocialTriageResult:
    if isinstance(result, dict):
        keys = set(result.keys())
        if keys != REQUIRED_TRIAGE_KEYS:
            missing = sorted(REQUIRED_TRIAGE_KEYS - keys)
            extra = sorted(keys - REQUIRED_TRIAGE_KEYS)
            raise ValueError(f"invalid triage schema keys missing={missing} extra={extra}")
        result = SocialTriageResult(
            decision=str(result.get("decision") or ""),
            category=str(result.get("category") or ""),
            significance=str(result.get("significance") or ""),
            summary=str(result.get("summary") or ""),
            reason=str(result.get("reason") or ""),
            confidence=str(result.get("confidence") or ""),
            information_scope=str(result.get("information_scope") or ""),
        )
    if result.decision not in ALLOWED_DECISIONS:
        raise ValueError("invalid triage decision")
    if result.category not in ALLOWED_CATEGORIES:
        raise ValueError("invalid triage category")
    if result.significance not in ALLOWED_SIGNIFICANCE:
        raise ValueError("invalid triage significance")
    if result.confidence not in ALLOWED_CONFIDENCE:
        raise ValueError("invalid triage confidence")
    if result.information_scope not in ALLOWED_SCOPES:
        raise ValueError("invalid triage information_scope")
    if result.decision == DECISION_KEEP_MEMORY and not result.summary.strip():
        raise ValueError("KEEP_MEMORY requires summary")
    if result.decision == DECISION_KEEP_MEMORY and result.category == CATEGORY_LOW_INFORMATION:
        raise ValueError("KEEP_MEMORY cannot use low_information category")
    return SocialTriageResult(
        decision=result.decision,
        category=result.category,
        significance=result.significance,
        summary=result.summary.strip()[:500],
        reason=result.reason.strip()[:500],
        confidence=result.confidence,
        information_scope=result.information_scope,
    )


class JsonSocialSemanticTriageAdapter:
    def parse_response(self, text: str) -> SocialTriageResult:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("malformed triage json") from exc
        if not isinstance(data, dict):
            raise ValueError("triage response must be a json object")
        return validate_triage_result(data)

    @staticmethod
    def system_prompt() -> str:
        return (
            "Return strict JSON only with exactly these required fields: decision, category, significance, "
            "summary, reason, confidence, information_scope. The schema is: "
            '{"decision":"KEEP_MEMORY | NO_MEMORY","category":"development | roadmap | security | tokenomics | '
            'listing | integration | partnership | funding | team | project_direction | incident_response | '
            'other_meaningful | low_information","significance":"low | medium | high","summary":"...",'
            '"reason":"...","confidence":"low | medium | high","information_scope":"project | token | dev | '
            'security | ecosystem | unknown"}. Decide whether this Project/DEV X post contains a durable project fact '
            "worth saving into Social Memory for future project research. Do not judge bullish or bearish. "
            "Generic hype, marketing language alone, price predictions, engagement bait, giveaways, GM/GN, "
            "meme-only, image-only without text, reposts without new information, and casual token shills are "
            "NO_MEMORY. Meaningful project changes such as product launches, development updates, roadmap changes, "
            "security incidents/audits/fixes, tokenomics changes, listings, integrations, partnerships, funding, "
            "team/control changes, strategic repositioning, and material incident responses are KEEP_MEMORY. "
            "For KEEP_MEMORY, category must not be low_information. Do not invent missing context. "
            "The summary must state what happened as a concise fact, not a trading view, and must not use investment "
            "language such as bullish, bearish, good investment, buy opportunity, likely to pump, or positive for price."
        )


class DeepSeekSocialSemanticTriageAdapter(JsonSocialSemanticTriageAdapter):
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
        timeout_seconds: float = 20,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.transport = transport

    def triage(self, payload: SocialSemanticTriageInput) -> SocialTriageResult:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt()},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "author_type": payload.author_type,
                            "author_username": payload.author_username,
                            "token_symbol": payload.token_symbol,
                            "tweet_text": payload.text,
                            "tweet_url": payload.tweet_url,
                            "event_time": payload.event_time.isoformat(),
                            "allowed_decisions": sorted(ALLOWED_DECISIONS),
                            "allowed_categories": sorted(ALLOWED_CATEGORIES),
                            "allowed_significance": sorted(ALLOWED_SIGNIFICANCE),
                            "allowed_confidence": sorted(ALLOWED_CONFIDENCE),
                            "allowed_information_scope": sorted(ALLOWED_SCOPES),
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0,
        }
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        with httpx.Client(timeout=self.timeout_seconds, transport=self.transport) as client:
            response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=body)
        if response.status_code != 200:
            raise RuntimeError(f"DeepSeek social memory triage failed status={response.status_code}")
        data = response.json()
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("DeepSeek social memory triage missing choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str):
            raise ValueError("DeepSeek social memory triage missing content")
        return self.parse_response(content)


def tweet_url(author_username: str | None, tweet_id: str) -> str | None:
    username = normalize_username(author_username)
    if not username:
        return None
    return f"https://x.com/{username}/status/{tweet_id}"


def _triage_input(event: SocialEvent) -> SocialSemanticTriageInput:
    return SocialSemanticTriageInput(
        author_type=event.author_type,
        author_username=event.author_username,
        token_symbol=event.symbol,
        text=event.text,
        tweet_url=event.tweet_url or tweet_url(event.author_username, event.provider_event_id),
        event_time=event.posted_at,
    )


def _project_key(event: SocialEvent) -> str:
    return f"{event.chain}:{event.token_address.lower()}"


def _processed_key(event: SocialEvent) -> tuple[str, str, str, str]:
    return (event.provider, event.provider_event_id, event.chain, event.token_address.lower())


def _clean_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _emoji_or_symbol_only(text: str) -> bool:
    semantic_chars = [char for char in text if char.isalnum()]
    return not semantic_chars


def _is_generic_low_information(text: str) -> bool:
    tokens = _low_info_tokens(text)
    if not tokens:
        return True
    normalized = " ".join(tokens)
    if normalized in GENERIC_LOW_INFO_PHRASES:
        return True
    if tokens[0] in GREETING_WORDS and all(token in LOW_INFO_SUFFIX_WORDS for token in tokens[1:]):
        return True
    if tokens[0] in THANKS_WORDS and all(token in LOW_INFO_SUFFIX_WORDS for token in tokens[1:]):
        return True
    if tokens[:2] == ["thank", "you"] and all(token in LOW_INFO_SUFFIX_WORDS for token in tokens[2:]):
        return True
    return False


def _low_info_tokens(text: str) -> list[str]:
    without_urls = re.sub(r"https?://\S+", " ", text)
    without_cashtags = re.sub(r"(?<!\w)\$[A-Za-z][A-Za-z0-9_]*\b", " ", without_urls)
    return re.findall(r"[A-Za-z0-9]+", without_cashtags.lower())
