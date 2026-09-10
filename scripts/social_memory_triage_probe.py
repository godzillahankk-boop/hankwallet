from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config import load_settings  # noqa: E402
from app.services.social_event_service import AUTHOR_PROJECT_X  # noqa: E402
from app.services.social_memory_service import (  # noqa: E402
    CATEGORY_LOW_INFORMATION,
    DECISION_KEEP_MEMORY,
    DECISION_NO_MEMORY,
    DeepSeekSocialSemanticTriageAdapter,
    SocialSemanticTriageAdapter,
    SocialSemanticTriageInput,
    SocialTriageResult,
    deterministic_no_memory_reason,
    validate_triage_result,
)

OUTPUT_ROOT = ROOT / "outputs" / "social_memory_triage_probe"
INVESTMENT_LANGUAGE = (
    "bullish",
    "bearish",
    "good investment",
    "buy opportunity",
    "likely to pump",
    "positive for price",
    "should buy",
    "buy now",
)


@dataclass(frozen=True)
class ProbeSample:
    sample_id: str
    text: str
    expected_decision: str
    expected_stage: str | None = None
    acceptable_categories: tuple[str, ...] = ()


SAMPLES: tuple[ProbeSample, ...] = (
    ProbeSample("01", "GM $ROBBIE 🚀", DECISION_NO_MEMORY, "deterministic"),
    ProbeSample("02", "GN fam", DECISION_NO_MEMORY, "deterministic"),
    ProbeSample("03", "Thanks community ❤️", DECISION_NO_MEMORY, "deterministic"),
    ProbeSample("04", "We are cooking", DECISION_NO_MEMORY, "deterministic"),
    ProbeSample("05", "$ROBBIE to the moon", DECISION_NO_MEMORY, "deterministic"),
    ProbeSample("06", "Mainnet launches Sep 20.", DECISION_KEEP_MEMORY, acceptable_categories=("development", "roadmap")),
    ProbeSample(
        "07",
        "The security audit by XYZ has been completed and the report is now published.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("security",),
    ),
    ProbeSample(
        "08",
        "Token migration begins tomorrow. Holders will migrate from V1 to V2 at a 1:1 ratio.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("tokenomics",),
    ),
    ProbeSample("09", "Our Base bridge integration is now live.", DECISION_KEEP_MEMORY, acceptable_categories=("integration",)),
    ProbeSample(
        "10",
        "Our lead developer is stepping down from the project effective today.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("team",),
    ),
    ProbeSample(
        "11",
        "We closed a $2 million seed funding round led by ABC Ventures.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("funding",),
    ),
    ProbeSample(
        "12",
        "We updated the Q4 roadmap and moved the mainnet launch to October.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("roadmap",),
    ),
    ProbeSample(
        "13",
        "We identified an exploit affecting withdrawals and have temporarily paused the protocol while a fix is deployed.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("security", "incident_response"),
    ),
    ProbeSample("14", "Big announcement soon for the community.", DECISION_NO_MEMORY),
    ProbeSample("15", "Huge partnership coming soon. Stay tuned.", DECISION_NO_MEMORY),
    ProbeSample(
        "16",
        "We have entered a formal partnership with ABC Protocol to integrate its liquidity layer.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("partnership", "integration"),
    ),
    ProbeSample("17", "Price oracle v2 is now live.", DECISION_KEEP_MEMORY, acceptable_categories=("development",)),
    ProbeSample(
        "18",
        "Target mainnet date is Sep 20.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("roadmap", "development"),
    ),
    ProbeSample("19", "GM. Mainnet launches Sep 20.", DECISION_KEEP_MEMORY, acceptable_categories=("development", "roadmap")),
    ProbeSample("20", "ROBBIE is up 40% today. The chart looks amazing.", DECISION_NO_MEMORY),
    ProbeSample("21", "This changes everything.", DECISION_NO_MEMORY),
    ProbeSample(
        "22",
        "We burned 5% of the circulating token supply today.",
        DECISION_KEEP_MEMORY,
        acceptable_categories=("tokenomics",),
    ),
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Controlled DeepSeek Social Memory semantic triage probe")
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT))
    args = parser.parse_args()

    settings = load_settings()
    if not settings.deepseek_api_key:
        raise SystemExit("DEEPSEEK_API_KEY missing; Social Memory triage probe cannot run.")

    adapter = DeepSeekSocialSemanticTriageAdapter(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.social_memory_triage_model,
    )
    summary = run_probe(adapter=adapter, output_root=Path(args.output_root))
    print_summary(summary)


def run_probe(
    *,
    adapter: SocialSemanticTriageAdapter,
    samples: tuple[ProbeSample, ...] = SAMPLES,
    output_root: Path = OUTPUT_ROOT,
    event_time: datetime | None = None,
) -> dict[str, Any]:
    event_time = event_time or datetime.now(UTC)
    sample_results = [evaluate_sample(sample, adapter, event_time=event_time) for sample in samples]
    summary = build_summary(sample_results)
    payload = {"samples": sample_results, "summary": summary}
    output_path = write_summary(payload, output_root=output_root)
    payload["summary"]["output_path"] = str(output_path)
    return payload


def evaluate_sample(
    sample: ProbeSample,
    adapter: SocialSemanticTriageAdapter,
    *,
    event_time: datetime,
) -> dict[str, Any]:
    deterministic_reason = deterministic_no_memory_reason(sample.text)
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
        stage = "deterministic"
        failure = None
    else:
        stage = "deepseek"
        failure = None
        try:
            result = validate_triage_result(
                adapter.triage(
                    SocialSemanticTriageInput(
                        author_type=AUTHOR_PROJECT_X,
                        author_username="ProjectUser",
                        token_symbol="ROBBIE",
                        text=sample.text,
                        tweet_url=None,
                        event_time=event_time,
                    )
                )
            )
        except Exception as exc:  # noqa: BLE001 - probe records per-sample failure and continues.
            result = None
            failure = f"{type(exc).__name__}: {str(exc)[:200]}"
    if result is None:
        return _sample_record(sample, stage=stage, failure=failure)
    decision_pass = result.decision == sample.expected_decision
    category_pass = _category_pass(sample, result)
    summary_language_pass = _summary_language_pass(result.summary)
    return _sample_record(
        sample,
        stage=stage,
        result=result,
        decision_pass=decision_pass,
        category_pass=category_pass,
        summary_language_pass=summary_language_pass,
        failure=failure,
    )


def build_summary(sample_results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(sample_results)
    expected_keep = sum(1 for row in sample_results if row["expected_decision"] == DECISION_KEEP_MEMORY)
    expected_no_memory = sum(1 for row in sample_results if row["expected_decision"] == DECISION_NO_MEMORY)
    decision_correct = sum(1 for row in sample_results if row["decision_pass"])
    keep_correct = sum(
        1
        for row in sample_results
        if row["expected_decision"] == DECISION_KEEP_MEMORY and row["actual_decision"] == DECISION_KEEP_MEMORY
    )
    no_memory_correct = sum(
        1
        for row in sample_results
        if row["expected_decision"] == DECISION_NO_MEMORY and row["actual_decision"] == DECISION_NO_MEMORY
    )
    false_positive = [
        row["id"]
        for row in sample_results
        if row["expected_decision"] == DECISION_NO_MEMORY and row["actual_decision"] == DECISION_KEEP_MEMORY
    ]
    false_negative = [
        row["id"]
        for row in sample_results
        if row["expected_decision"] == DECISION_KEEP_MEMORY and row["actual_decision"] == DECISION_NO_MEMORY
    ]
    failures = [row["id"] for row in sample_results if not row["overall_pass"]]
    return {
        "total_samples": total,
        "deterministic_skips": sum(1 for row in sample_results if row["stage"] == "deterministic"),
        "deepseek_calls": sum(1 for row in sample_results if row["stage"] == "deepseek"),
        "expected_keep": expected_keep,
        "expected_no_memory": expected_no_memory,
        "decision_correct": decision_correct,
        "decision_accuracy": decision_correct / total if total else 0,
        "keep_correct": keep_correct,
        "no_memory_correct": no_memory_correct,
        "category_correct": sum(1 for row in sample_results if row["category_pass"]),
        "summary_language_violations": sum(1 for row in sample_results if not row["summary_language_pass"]),
        "triage_failures": sum(1 for row in sample_results if row["triage_failure"]),
        "false_positive_memory": len(false_positive),
        "false_negative_memory": len(false_negative),
        "failed_sample_ids": failures,
        "false_positive_sample_ids": false_positive,
        "false_negative_sample_ids": false_negative,
    }


def write_summary(payload: dict[str, Any], *, output_root: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def print_summary(payload: dict[str, Any]) -> None:
    for row in payload["samples"]:
        print(json.dumps(row, ensure_ascii=False))
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))


def _sample_record(
    sample: ProbeSample,
    *,
    stage: str,
    result: SocialTriageResult | None = None,
    decision_pass: bool = False,
    category_pass: bool = False,
    summary_language_pass: bool = True,
    failure: str | None = None,
) -> dict[str, Any]:
    actual_decision = result.decision if result else "TRIAGE_FAILED"
    overall_pass = decision_pass and category_pass and summary_language_pass and failure is None
    return {
        "id": sample.sample_id,
        "text": sample.text,
        "stage": stage,
        "expected_decision": sample.expected_decision,
        "actual_decision": actual_decision,
        "category": result.category if result else None,
        "significance": result.significance if result else None,
        "confidence": result.confidence if result else None,
        "information_scope": result.information_scope if result else None,
        "summary": result.summary if result else "",
        "reason": result.reason if result else failure,
        "decision_pass": decision_pass,
        "category_pass": category_pass,
        "summary_language_pass": summary_language_pass,
        "overall_pass": overall_pass,
        "triage_failure": failure,
    }


def _category_pass(sample: ProbeSample, result: SocialTriageResult) -> bool:
    if sample.expected_decision != DECISION_KEEP_MEMORY:
        return True
    if result.decision != DECISION_KEEP_MEMORY:
        return False
    if not sample.acceptable_categories:
        return True
    return result.category in sample.acceptable_categories


def _summary_language_pass(summary: str) -> bool:
    lower = summary.lower()
    return not any(phrase in lower for phrase in INVESTMENT_LANGUAGE)


if __name__ == "__main__":
    main()
