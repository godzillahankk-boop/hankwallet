from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.services.social_memory_service import (
    DECISION_KEEP_MEMORY,
    DECISION_NO_MEMORY,
    SocialTriageResult,
)
import scripts.social_memory_triage_probe as probe


class FakeAdapter:
    def __init__(self, results: dict[str, SocialTriageResult]) -> None:
        self.results = results
        self.calls: list[str | None] = []

    def triage(self, payload):  # noqa: ANN001
        self.calls.append(payload.text)
        result = self.results[payload.text]
        if isinstance(result, Exception):
            raise result
        return result


def test_probe_skips_deterministic_samples_and_calls_deepseek_per_candidate(tmp_path) -> None:
    samples = (
        probe.ProbeSample("01", "GM $ROBBIE 🚀", DECISION_NO_MEMORY, "deterministic"),
        probe.ProbeSample("06", "Mainnet launches Sep 20.", DECISION_KEEP_MEMORY, acceptable_categories=("development",)),
        probe.ProbeSample("14", "Big announcement soon for the community.", DECISION_NO_MEMORY),
    )
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": triage(
                decision=DECISION_KEEP_MEMORY,
                category="development",
                summary="Project announced mainnet launch for Sep 20.",
            ),
            "Big announcement soon for the community.": triage(
                decision=DECISION_NO_MEMORY,
                category="low_information",
                summary="",
            ),
        }
    )

    payload = probe.run_probe(
        adapter=adapter,
        samples=samples,
        output_root=tmp_path,
        event_time=datetime(2026, 9, 10, tzinfo=UTC),
    )

    assert adapter.calls == ["Mainnet launches Sep 20.", "Big announcement soon for the community."]
    assert payload["samples"][0]["stage"] == "deterministic"
    assert payload["samples"][0]["actual_decision"] == DECISION_NO_MEMORY
    assert payload["summary"]["total_samples"] == 3
    assert payload["summary"]["deterministic_skips"] == 1
    assert payload["summary"]["deepseek_calls"] == 2
    assert payload["summary"]["decision_correct"] == 3
    assert payload["summary"]["category_correct"] == 3
    assert payload["summary"]["triage_failures"] == 0
    assert (tmp_path / payload["summary"]["output_path"].split(str(tmp_path), 1)[1].lstrip("/")).exists()


def test_probe_tracks_false_positive_false_negative_and_failed_ids(tmp_path) -> None:
    samples = (
        probe.ProbeSample("14", "Big announcement soon for the community.", DECISION_NO_MEMORY),
        probe.ProbeSample("06", "Mainnet launches Sep 20.", DECISION_KEEP_MEMORY, acceptable_categories=("development",)),
    )
    adapter = FakeAdapter(
        {
            "Big announcement soon for the community.": triage(
                decision=DECISION_KEEP_MEMORY,
                category="development",
                summary="Project announced something bullish.",
            ),
            "Mainnet launches Sep 20.": triage(
                decision=DECISION_NO_MEMORY,
                category="low_information",
                summary="",
            ),
        }
    )

    payload = probe.run_probe(
        adapter=adapter,
        samples=samples,
        output_root=tmp_path,
        event_time=datetime(2026, 9, 10, tzinfo=UTC),
    )

    assert payload["summary"]["false_positive_memory"] == 1
    assert payload["summary"]["false_negative_memory"] == 1
    assert payload["summary"]["summary_language_violations"] == 1
    assert payload["summary"]["failed_sample_ids"] == ["14", "06"]
    assert payload["summary"]["false_positive_sample_ids"] == ["14"]
    assert payload["summary"]["false_negative_sample_ids"] == ["06"]


def test_probe_records_triage_failure_and_continues(tmp_path) -> None:
    samples = (
        probe.ProbeSample("06", "Mainnet launches Sep 20.", DECISION_KEEP_MEMORY, acceptable_categories=("development",)),
        probe.ProbeSample("07", "Audit published.", DECISION_KEEP_MEMORY, acceptable_categories=("security",)),
    )
    adapter = FakeAdapter(
        {
            "Mainnet launches Sep 20.": RuntimeError("model down"),
            "Audit published.": triage(
                decision=DECISION_KEEP_MEMORY,
                category="security",
                summary="Project published an audit.",
            ),
        }
    )

    payload = probe.run_probe(
        adapter=adapter,
        samples=samples,
        output_root=tmp_path,
        event_time=datetime(2026, 9, 10, tzinfo=UTC),
    )

    assert adapter.calls == ["Mainnet launches Sep 20.", "Audit published."]
    assert payload["samples"][0]["actual_decision"] == "TRIAGE_FAILED"
    assert payload["summary"]["triage_failures"] == 1
    assert payload["summary"]["decision_correct"] == 1


def test_main_fails_fast_without_deepseek_key(monkeypatch) -> None:
    settings = SimpleNamespace(deepseek_api_key=None)
    monkeypatch.setattr(probe, "load_settings", lambda: settings)
    monkeypatch.setattr(probe.sys, "argv", ["social_memory_triage_probe.py"])

    with pytest.raises(SystemExit, match="DEEPSEEK_API_KEY missing"):
        probe.main()


def test_main_wires_settings_without_printing_key(monkeypatch, tmp_path, capsys) -> None:
    captured = {}
    settings = SimpleNamespace(
        deepseek_api_key="secret-key",
        deepseek_base_url="https://deepseek.test",
        social_memory_triage_model="deepseek-chat",
    )

    class FakeDeepSeekAdapter:
        def __init__(self, **kwargs):  # noqa: ANN001
            captured["kwargs"] = kwargs

    def fake_run_probe(*, adapter, output_root):  # noqa: ANN001
        captured["adapter"] = adapter
        captured["output_root"] = output_root
        return {"samples": [], "summary": {"total_samples": 0}}

    monkeypatch.setattr(probe, "load_settings", lambda: settings)
    monkeypatch.setattr(probe, "DeepSeekSocialSemanticTriageAdapter", FakeDeepSeekAdapter)
    monkeypatch.setattr(probe, "run_probe", fake_run_probe)
    monkeypatch.setattr(
        probe.sys,
        "argv",
        ["social_memory_triage_probe.py", "--output-root", str(tmp_path)],
    )

    probe.main()

    output = capsys.readouterr().out
    assert "secret-key" not in output
    assert captured["kwargs"] == {
        "api_key": "secret-key",
        "base_url": "https://deepseek.test",
        "model": "deepseek-chat",
    }
    assert captured["output_root"] == tmp_path


def triage(
    *,
    decision: str,
    category: str,
    summary: str,
    significance: str = "medium",
    confidence: str = "high",
    information_scope: str = "project",
) -> SocialTriageResult:
    return SocialTriageResult(
        decision=decision,
        category=category,
        significance=significance,
        summary=summary,
        reason="test",
        confidence=confidence,
        information_scope=information_scope,
    )
