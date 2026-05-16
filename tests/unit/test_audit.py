"""Regression tests for the kate audit ready-to-ship CLI.

The audit module is itself a load-bearing artifact under the audit
protocol — these tests guard its check logic, result aggregation, and
output rendering. If the audit breaks silently, every other artifact's
review gate breaks with it.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_bot.audit import (
    AuditReport,
    Check,
    CheckResult,
    CheckStatus,
    CodeImportsCheck,
    ConfigSecretsCheck,
    IntegrationSmokeCheck,
    JsonValidityCheck,
    ProtocolDriftCheck,
    ReviewInboxCheck,
    ReviewRecordsCheck,
    render_human,
    render_json,
    run_audit,
)


# ── CheckResult + AuditReport ─────────────────────────────────────────────


def _result(name: str, status: CheckStatus, message: str = "test") -> CheckResult:
    return CheckResult(check_name=name, status=status, message=message)


def test_audit_report_overall_status_pass():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.PASS),
        ),
    )
    assert report.overall_status == "PASS"
    assert report.exit_code == 0


def test_audit_report_overall_status_pass_with_warn():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.WARN),
        ),
    )
    assert report.overall_status == "PASS-WITH-WARN"
    assert report.exit_code == 0  # WARN does NOT block deploy


def test_audit_report_overall_status_fail():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.FAIL),
            _result("c", CheckStatus.WARN),  # mix of WARN + FAIL = still FAIL
        ),
    )
    assert report.overall_status == "FAIL"
    assert report.exit_code == 2  # FAIL blocks deploy


def test_audit_report_skip_does_not_affect_overall():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.SKIP),
        ),
    )
    assert report.overall_status == "PASS"


# ── Check ABC behaviour ──────────────────────────────────────────────────


class _PassingCheck(Check):
    name = "passing_test_check"

    def run(self) -> CheckResult:
        return self._timed(lambda: CheckResult(
            check_name="passing_test_check",
            status=CheckStatus.PASS,
            message="ok",
        ))


class _RaisingCheck(Check):
    name = "raising_test_check"

    def run(self) -> CheckResult:
        return self._timed(self._explode)

    def _explode(self):
        raise RuntimeError("simulated failure")


def test_check_timed_records_duration():
    result = _PassingCheck().run()
    assert result.status == CheckStatus.PASS
    assert result.duration_ms >= 0.0


def test_check_timed_catches_unexpected_exceptions():
    result = _RaisingCheck().run()
    assert result.status == CheckStatus.FAIL
    assert "RuntimeError" in result.message
    assert "simulated failure" in result.message
    # Check did not propagate the exception — caller can keep auditing
    # other checks even when one explodes.


# ── JsonValidityCheck — using tmp directories ────────────────────────────


def test_json_validity_passes_on_valid_json(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod

    valid = tmp_path / "test.json"
    valid.write_text('{"key": "value"}')
    monkeypatch.setattr(audit_mod, "TEE_INPUTS_PATH", valid)
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", valid)
    monkeypatch.setattr(audit_mod, "OMNI_ROOT", tmp_path)

    # Need to create the .mcp-brain/config subdir for the glob
    config_dir = tmp_path / ".mcp-brain" / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "extra.json").write_text('{"a": 1}')

    result = JsonValidityCheck().run()
    assert result.status == CheckStatus.PASS


def test_json_validity_fails_on_malformed_json(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod

    bad = tmp_path / "bad.json"
    bad.write_text('{this is not json')
    config_dir = tmp_path / ".mcp-brain" / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "bad.json").write_text('{not valid')

    monkeypatch.setattr(audit_mod, "OMNI_ROOT", tmp_path)
    monkeypatch.setattr(audit_mod, "TEE_INPUTS_PATH", config_dir / "bad.json")
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", bad)

    result = JsonValidityCheck().run()
    assert result.status == CheckStatus.FAIL
    assert "broken_files" in result.details
    assert len(result.details["broken_files"]) >= 1


# ── ConfigSecretsCheck ───────────────────────────────────────────────────


def test_config_secrets_fails_when_secrets_file_missing(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", tmp_path / "missing.json")
    result = ConfigSecretsCheck().run()
    assert result.status == CheckStatus.FAIL
    assert "not found" in result.message


def test_config_secrets_fails_when_required_keys_missing(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    secrets = tmp_path / "secrets.json"
    secrets.write_text('{"telegram": {"bot_token": ""}}')  # missing chat_id
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", secrets)

    result = ConfigSecretsCheck().run()
    assert result.status == CheckStatus.FAIL
    assert "telegram.chat_id" in result.message or "telegram.bot_token" in result.message


def test_config_secrets_warns_when_optional_keys_missing(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    secrets = tmp_path / "secrets.json"
    secrets.write_text(json.dumps({
        "telegram": {"bot_token": "abc", "chat_id": "123"},
        # ninja_bridge / mt5_ic_markets deliberately absent
    }))
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", secrets)

    result = ConfigSecretsCheck().run()
    assert result.status == CheckStatus.WARN
    assert "missing_optional" in result.details


def test_config_secrets_passes_when_all_present(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    secrets = tmp_path / "secrets.json"
    secrets.write_text(json.dumps({
        "telegram": {"bot_token": "abc", "chat_id": "123"},
        "ninja_bridge": {"hmac_secret": "x", "host": "127.0.0.1", "port": 9876},
        "mt5_ic_markets": {"primary_demo": 52880143},
    }))
    monkeypatch.setattr(audit_mod, "SECRETS_PATH", secrets)

    result = ConfigSecretsCheck().run()
    assert result.status == CheckStatus.PASS


# ── CodeImportsCheck ─────────────────────────────────────────────────────


def test_code_imports_passes_for_known_good_modules():
    # Real check against the actual Kate codebase — this guards against
    # the audit module knowing about a stale module list.
    result = CodeImportsCheck().run()
    assert result.status == CheckStatus.PASS, (
        f"Kate module imports broken: {result.details}"
    )


# ── ProtocolDriftCheck ───────────────────────────────────────────────────


def test_protocol_drift_skips_when_bridge_md_missing(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    monkeypatch.setattr(audit_mod, "NT_BRIDGE_DIR", tmp_path / "missing")
    result = ProtocolDriftCheck().run()
    assert result.status == CheckStatus.SKIP


def test_protocol_drift_fails_on_wrong_port(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    monkeypatch.setattr(audit_mod, "NT_BRIDGE_DIR", tmp_path)
    (tmp_path / "bridge_protocol.md").write_text(
        "Transport: TCP on 127.0.0.1:8765.\n"
        "HMAC-SHA256 signs canonical_json(payload).\n"
        '"msg_type" "payload" "sequence" "signature"\n'
        "signal fill heartbeat reconcile_req reconcile_resp ack\n"
    )
    result = ProtocolDriftCheck().run()
    assert result.status == CheckStatus.FAIL
    assert any("port" in issue.lower() for issue in result.details["issues"])


def test_protocol_drift_fails_on_missing_msg_type(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    monkeypatch.setattr(audit_mod, "NT_BRIDGE_DIR", tmp_path)
    (tmp_path / "bridge_protocol.md").write_text(
        "Transport: TCP on 127.0.0.1:9876.\n"
        "HMAC-SHA256 signs canonical_json(payload).\n"
        '"msg_type" "payload" "sequence" "signature"\n'
        "signal fill heartbeat\n"  # missing reconcile_*, ack
    )
    result = ProtocolDriftCheck().run()
    assert result.status == CheckStatus.FAIL


def test_protocol_drift_passes_on_aligned_spec(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    monkeypatch.setattr(audit_mod, "NT_BRIDGE_DIR", tmp_path)
    (tmp_path / "bridge_protocol.md").write_text(
        "Transport: TCP on 127.0.0.1:9876.\n"
        "HMAC-SHA256 signs canonical_json(payload).\n"
        '"msg_type" "payload" "sequence" "signature"\n'
        "signal fill heartbeat reconcile_req reconcile_resp ack\n"
    )
    result = ProtocolDriftCheck().run()
    assert result.status == CheckStatus.PASS


# ── IntegrationSmokeCheck — real localhost TCP ───────────────────────────


def test_integration_smoke_real_round_trip():
    result = IntegrationSmokeCheck().run()
    assert result.status == CheckStatus.PASS, result.message
    assert "round-trip OK" in result.message


# ── ReviewRecordsCheck — empty handoffs ──────────────────────────────────


def test_review_records_warns_when_triggered_artifacts_lack_reviews(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    fake_omni = tmp_path / "omni"
    fake_kate = tmp_path / "kate"
    fake_omni.mkdir()
    fake_kate.mkdir()
    handoffs = fake_omni / "handoffs"
    handoffs.mkdir()

    # Create a triggered artifact with no review record
    (fake_kate / "trading_bot" / "core" / "execution").mkdir(parents=True)
    (fake_kate / "trading_bot" / "core" / "execution" / "test_broker_adapter.py").write_text("# triggered")

    monkeypatch.setattr(audit_mod, "OMNI_ROOT", fake_omni)
    monkeypatch.setattr(audit_mod, "KATE_ROOT", fake_kate)
    monkeypatch.setattr(audit_mod, "HANDOFFS_DIR", handoffs)

    result = ReviewRecordsCheck().run()
    assert result.status == CheckStatus.WARN


def test_review_records_passes_when_artifact_has_approved_review(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    fake_omni = tmp_path / "omni"
    fake_kate = tmp_path / "kate"
    fake_omni.mkdir()
    fake_kate.mkdir()
    handoffs = fake_omni / "handoffs"
    handoffs.mkdir()

    (fake_kate / "trading_bot" / "core" / "execution").mkdir(parents=True)
    artifact = fake_kate / "trading_bot" / "core" / "execution" / "test_broker_adapter.py"
    artifact.write_text("# triggered")

    review = handoffs / "2026-05-16-codex-to-claude-REVIEW-RESPONSE-test-broker.md"
    review.write_text(
        "---\n"
        "review_outcome: APPROVED\n"
        "artifact_paths:\n"
        "  - trading_bot/core/execution/test_broker_adapter.py\n"
        "---\n"
        "approved.\n"
    )

    monkeypatch.setattr(audit_mod, "OMNI_ROOT", fake_omni)
    monkeypatch.setattr(audit_mod, "KATE_ROOT", fake_kate)
    monkeypatch.setattr(audit_mod, "HANDOFFS_DIR", handoffs)

    result = ReviewRecordsCheck().run()
    assert result.status == CheckStatus.PASS, result.details


# ── ReviewInboxCheck ─────────────────────────────────────────────────────


def test_review_inbox_passes_when_no_outstanding_requests(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    handoffs = tmp_path / "handoffs"
    handoffs.mkdir()
    monkeypatch.setattr(audit_mod, "HANDOFFS_DIR", handoffs)

    result = ReviewInboxCheck(agent="claude").run()
    assert result.status == CheckStatus.PASS


def test_review_inbox_warns_on_unanswered_request(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    handoffs = tmp_path / "handoffs"
    handoffs.mkdir()
    (handoffs / "2026-05-16-codex-to-claude-REVIEW-REQUEST-test-thing.md").write_text(
        "---\nreview_due: 2026-05-16T13:00:00+01:00\n---\nplease review.\n"
    )
    monkeypatch.setattr(audit_mod, "HANDOFFS_DIR", handoffs)

    result = ReviewInboxCheck(agent="claude").run()
    assert result.status == CheckStatus.WARN
    assert len(result.details["unanswered"]) == 1


def test_review_inbox_passes_when_request_has_matching_response(tmp_path, monkeypatch):
    from trading_bot import audit as audit_mod
    handoffs = tmp_path / "handoffs"
    handoffs.mkdir()
    (handoffs / "2026-05-16-codex-to-claude-REVIEW-REQUEST-test-thing.md").write_text(
        "---\nreview_due: 2026-05-16T13:00:00+01:00\n---\nplease review.\n"
    )
    (handoffs / "2026-05-16-claude-to-codex-REVIEW-RESPONSE-test-thing.md").write_text(
        "---\nreview_outcome: APPROVED\n---\napproved.\n"
    )
    monkeypatch.setattr(audit_mod, "HANDOFFS_DIR", handoffs)

    result = ReviewInboxCheck(agent="claude").run()
    assert result.status == CheckStatus.PASS


# ── Orchestrator + CLI rendering ─────────────────────────────────────────


def test_run_audit_only_filter():
    report = run_audit(only=["protocol_drift"])
    assert len(report.results) == 1
    assert report.results[0].check_name == "protocol_drift"


def test_run_audit_skip_filter():
    report = run_audit(skip=["test_suite", "integration_smoke"])
    skipped = [r for r in report.results if r.status == CheckStatus.SKIP]
    assert len(skipped) >= 2
    skipped_names = {r.check_name for r in skipped}
    assert "test_suite" in skipped_names
    assert "integration_smoke" in skipped_names


def test_render_human_does_not_crash_on_any_status_combination():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.WARN),
            _result("c", CheckStatus.FAIL),
            _result("d", CheckStatus.SKIP),
        ),
    )
    output = render_human(report)
    assert "[PASS]" in output
    assert "[WARN]" in output
    assert "[FAIL]" in output
    assert "[SKIP]" in output
    assert "FAILED" in output  # overall status banner


def test_render_json_is_machine_parseable():
    report = AuditReport(
        timestamp_utc="2026-05-16T11:00:00+00:00",
        results=(
            _result("a", CheckStatus.PASS),
            _result("b", CheckStatus.FAIL, message="boom"),
        ),
    )
    output = render_json(report)
    parsed = json.loads(output)  # must parse cleanly
    assert parsed["overall_status"] == "FAIL"
    assert parsed["exit_code"] == 2
    assert len(parsed["results"]) == 2
    assert parsed["results"][1]["message"] == "boom"
