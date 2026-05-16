"""Regression tests for the kate audit live runtime sibling.

Each check has its own fixture-driven test so we can simulate the failure
modes (Sierra stall, malformed JSON, tampered chain, etc) without needing
the actual Kate Host filesystem.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from pathlib import Path

import pytest

from trading_bot.audit_live import (
    ActivityChainIntegrityCheck,
    AggregateDDEarlyWarningCheck,
    LiveCheck,
    LiveCheckResult,
    LiveCheckStatus,
    ScidFreshnessCheck,
    SecretsFileFreshnessCheck,
    TeeInputsIntegrityCheck,
    _format_alert,
    _format_recovery,
    _human_duration,
    run_loop,
)
from trading_bot import audit_live as audit_live_mod


# ── _human_duration ──────────────────────────────────────────────────────


def test_human_duration_buckets():
    assert _human_duration(5) == "5s"
    assert _human_duration(90) == "1m"
    assert _human_duration(3700) == "1.0h"
    assert _human_duration(90_000) == "1.0d"


# ── LiveCheck.timed catches exceptions ───────────────────────────────────


class _ExplodingCheck(LiveCheck):
    name = "exploding_test"

    def run(self) -> LiveCheckResult:
        return self.timed(self._explode)

    def _explode(self):
        raise RuntimeError("boom")


def test_timed_catches_exceptions_returns_fail():
    result = _ExplodingCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "RuntimeError" in result.message
    assert "boom" in result.message
    assert result.duration_ms >= 0.0


# ── ScidFreshnessCheck ───────────────────────────────────────────────────


def test_scid_freshness_skips_when_dir_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path / "nope")
    result = ScidFreshnessCheck().run()
    assert result.status == LiveCheckStatus.SKIP
    assert "not on Kate Host" in result.message


def test_scid_freshness_skips_when_outside_rth(tmp_path, monkeypatch):
    # Force the time check to a known non-RTH moment (Sunday 6am UK)
    sunday_6am = dt.datetime(2026, 5, 17, 6, 0)
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path)
    (tmp_path / "MESM26.scid").write_text("fake")

    class _FakeDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return sunday_6am

    monkeypatch.setattr(audit_live_mod.dt, "datetime", _FakeDT)
    result = ScidFreshnessCheck().run()
    assert result.status == LiveCheckStatus.SKIP
    assert "no data flow expected" in result.message


def test_scid_freshness_fails_when_files_stale_during_rth(tmp_path, monkeypatch):
    # Use a real time during RTH: Tue 2026-05-19 15:00 UK
    tue_3pm = dt.datetime(2026, 5, 19, 15, 0)
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path)

    # Create a stale .scid file (mtime 1 hour ago — exceeds 600s threshold)
    stale = tmp_path / "MESM26_FUT_CME.scid"
    stale.write_text("data")
    old_ts = time.time() - 3700
    import os
    os.utime(stale, (old_ts, old_ts))

    class _FakeDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return tue_3pm

    monkeypatch.setattr(audit_live_mod.dt, "datetime", _FakeDT)
    result = ScidFreshnessCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "stale" in result.message.lower()
    assert "MESM26_FUT_CME.scid" in str(result.details.get("stale_files", []))


def test_scid_freshness_fails_when_no_files_during_rth(tmp_path, monkeypatch):
    tue_3pm = dt.datetime(2026, 5, 19, 15, 0)
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path)
    # Empty dir — no .scid files

    class _FakeDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return tue_3pm

    monkeypatch.setattr(audit_live_mod.dt, "datetime", _FakeDT)
    result = ScidFreshnessCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "no .scid files" in result.message


def test_scid_freshness_passes_when_files_fresh_during_rth(tmp_path, monkeypatch):
    tue_3pm = dt.datetime(2026, 5, 19, 15, 0)
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path)

    fresh = tmp_path / "MESM26_FUT_CME.scid"
    fresh.write_text("data")  # mtime is now, fresh

    class _FakeDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return tue_3pm

    monkeypatch.setattr(audit_live_mod.dt, "datetime", _FakeDT)
    result = ScidFreshnessCheck().run()
    assert result.status == LiveCheckStatus.PASS


# ── TeeInputsIntegrityCheck ──────────────────────────────────────────────


def test_tee_inputs_fails_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tmp_path / "missing.json")
    result = TeeInputsIntegrityCheck().run()
    assert result.status == LiveCheckStatus.FAIL


def test_tee_inputs_fails_on_malformed_json(tmp_path, monkeypatch):
    # Simulate Gemini's 2026-05-15 missing-brace incident
    bad = tmp_path / "tee.json"
    bad.write_text('  "aggregate_drawdown_cap_gbp": 500.00,\n}')  # missing opening {
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", bad)
    result = TeeInputsIntegrityCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "parse failed" in result.message


def test_tee_inputs_fails_on_missing_required_keys(tmp_path, monkeypatch):
    incomplete = tmp_path / "tee.json"
    incomplete.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        # missing aggregate_dd_cap_breach_action, fronts, monthly_costs_gbp
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", incomplete)
    result = TeeInputsIntegrityCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "missing required top-level keys" in result.message


def test_tee_inputs_fails_on_invalid_cap_value(tmp_path, monkeypatch):
    bad_cap = tmp_path / "tee.json"
    bad_cap.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": "not-a-number",
        "aggregate_dd_cap_breach_action": "halt",
        "fronts": [],
        "monthly_costs_gbp": {},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", bad_cap)
    result = TeeInputsIntegrityCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "invalid" in result.message.lower()


def test_tee_inputs_passes_on_valid_canonical_file(tmp_path, monkeypatch):
    good = tmp_path / "tee.json"
    good.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        "aggregate_dd_cap_breach_action": "mandatory-halt-and-reeval",
        "fronts": [{"id": "front_1", "real_capital_gbp": 1000.0}],
        "monthly_costs_gbp": {"sc": 50.0},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", good)
    result = TeeInputsIntegrityCheck().run()
    assert result.status == LiveCheckStatus.PASS


# ── AggregateDDEarlyWarningCheck ─────────────────────────────────────────


def test_aggregate_dd_warn_at_50pct(tmp_path, monkeypatch):
    tee = tmp_path / "tee.json"
    tee.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        "aggregate_dd_cap_breach_action": "halt",
        "fronts": [
            {"id": "front_1", "live_drawdown_gbp": 250.0},  # exactly 50%
            {"id": "front_2", "live_drawdown_gbp": 0.0},
        ],
        "monthly_costs_gbp": {},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tee)
    result = AggregateDDEarlyWarningCheck().run()
    assert result.status == LiveCheckStatus.WARN
    assert "de-risk" in result.message


def test_aggregate_dd_fails_at_80pct(tmp_path, monkeypatch):
    tee = tmp_path / "tee.json"
    tee.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        "aggregate_dd_cap_breach_action": "halt",
        "fronts": [{"id": "front_1", "live_drawdown_gbp": 420.0}],
        "monthly_costs_gbp": {},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tee)
    result = AggregateDDEarlyWarningCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "approaching mandatory halt" in result.message


def test_aggregate_dd_passes_when_no_dd(tmp_path, monkeypatch):
    tee = tmp_path / "tee.json"
    tee.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        "aggregate_dd_cap_breach_action": "halt",
        "fronts": [],
        "monthly_costs_gbp": {},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tee)
    result = AggregateDDEarlyWarningCheck().run()
    assert result.status == LiveCheckStatus.PASS


# ── ActivityChainIntegrityCheck ──────────────────────────────────────────


def test_activity_chain_skips_when_no_log_today(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_live_mod, "ACTIVITY_LOG_DIR", tmp_path / "nope")
    result = ActivityChainIntegrityCheck().run()
    assert result.status == LiveCheckStatus.SKIP


# ── SecretsFileFreshnessCheck ────────────────────────────────────────────


def test_secrets_present_fails_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", tmp_path / "nope.json")
    result = SecretsFileFreshnessCheck().run()
    assert result.status == LiveCheckStatus.FAIL


def test_secrets_present_fails_when_no_telegram_section(tmp_path, monkeypatch):
    s = tmp_path / "s.json"
    s.write_text(json.dumps({"other": "stuff"}))
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", s)
    result = SecretsFileFreshnessCheck().run()
    assert result.status == LiveCheckStatus.FAIL
    assert "telegram" in result.message


def test_secrets_present_passes_when_well_formed(tmp_path, monkeypatch):
    s = tmp_path / "s.json"
    s.write_text(json.dumps({"telegram": {"bot_token": "x", "chat_id": "y"}}))
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", s)
    result = SecretsFileFreshnessCheck().run()
    assert result.status == LiveCheckStatus.PASS


# ── Alert formatting ─────────────────────────────────────────────────────


def test_format_alert_includes_check_name_and_message():
    r = LiveCheckResult(
        name="test_check",
        status=LiveCheckStatus.FAIL,
        message="something broke",
    )
    text = _format_alert(r)
    assert "test_check" in text
    assert "something broke" in text
    assert "FAIL" in text


def test_format_recovery_mentions_prior_status():
    r = LiveCheckResult(
        name="test_check",
        status=LiveCheckStatus.PASS,
        message="back online",
    )
    text = _format_recovery(r, LiveCheckStatus.FAIL)
    assert "RECOVERED" in text
    assert "FAIL" in text
    assert "back online" in text


# ── run_loop one-shot integration ────────────────────────────────────────


def test_run_loop_one_shot_returns_zero_when_all_pass(tmp_path, monkeypatch):
    # Force all checks to skip or pass
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path / "no-sc")
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tmp_path / "no-tee.json")
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", tmp_path / "s.json")
    monkeypatch.setattr(audit_live_mod, "ACTIVITY_LOG_DIR", tmp_path / "no-log")
    (tmp_path / "s.json").write_text(json.dumps({"telegram": {"bot_token": "x"}}))

    # Disable alerts so we don't touch Telegram
    rc = asyncio.run(run_loop(
        interval_seconds=60,
        alerts_enabled=False,
        one_shot=True,
    ))
    # One of the checks (tee_inputs missing) will FAIL — expected exit 2
    assert rc == 2


def test_run_loop_one_shot_returns_zero_when_only_skips(tmp_path, monkeypatch):
    # Configure so all checks SKIP cleanly
    monkeypatch.setattr(audit_live_mod, "SC_DATA_DIR", tmp_path / "no-sc")
    # tee_inputs valid so that check passes
    tee = tmp_path / "tee.json"
    tee.write_text(json.dumps({
        "aggregate_drawdown_cap_gbp": 500.0,
        "aggregate_dd_cap_breach_action": "halt",
        "fronts": [],
        "monthly_costs_gbp": {},
    }))
    monkeypatch.setattr(audit_live_mod, "TEE_INPUTS_PATH", tee)
    # secrets present
    s = tmp_path / "s.json"
    s.write_text(json.dumps({"telegram": {"bot_token": "x", "chat_id": "y"}}))
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", s)
    # no activity log → SKIP
    monkeypatch.setattr(audit_live_mod, "ACTIVITY_LOG_DIR", tmp_path / "no-log")

    rc = asyncio.run(run_loop(
        interval_seconds=60,
        alerts_enabled=False,
        one_shot=True,
    ))
    assert rc == 0


def test_run_loop_one_shot_filters_by_only(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_live_mod, "SECRETS_PATH", tmp_path / "missing.json")
    rc = asyncio.run(run_loop(
        interval_seconds=60,
        only=["secrets_file_present"],
        alerts_enabled=False,
        one_shot=True,
    ))
    # Only the secrets check runs, and it FAILs (file missing) → exit 2
    assert rc == 2
