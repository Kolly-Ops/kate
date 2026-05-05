"""Tests for the supervisor's Sierra TradeActivityLog suffix validator.

The validator is a Gate #11/14 enforcement at supervisor startup time —
refuses to launch Kate if Sierra's TradeActivityLog filename suffix
doesn't match the required account+mode. Prevents the cycle 2/3 silent-
drop pattern where Kate submits orders into a wrong-mode Sierra for hours.

Empirical justification + design notes in:
    omni/protocol/kate-pre-live-flip-gate.md (Gate #11, Gate #14)
    trading_bot/supervisor/main.py (_validate_sierra_trade_activity_suffix)
"""
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import pytest

from trading_bot.supervisor.main import _validate_sierra_trade_activity_suffix


def _today_utc_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def test_empty_suffix_disables_check(tmp_path: Path, caplog) -> None:
    """Empty required_suffix is the documented escape hatch — log warning and pass."""
    _validate_sierra_trade_activity_suffix(
        logs_dir=tmp_path, required_suffix="", allow_missing=False,
    )
    # No exception = pass


def test_dir_missing_with_required_suffix_exits_99(tmp_path: Path) -> None:
    """If the logs dir doesn't exist and a suffix is required, exit 99."""
    missing = tmp_path / "does-not-exist"
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=missing, required_suffix="Sim1.simulated", allow_missing=False,
        )
    assert exc_info.value.code == 99


def test_no_file_today_strict_exits_99(tmp_path: Path) -> None:
    """No file for today + allow_missing=False → exit 99 (Sierra session not initialised)."""
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=tmp_path,
            required_suffix="Sim1.simulated",
            allow_missing=False,
        )
    assert exc_info.value.code == 99


def test_no_file_today_permissive_passes(tmp_path: Path) -> None:
    """No file for today + allow_missing=True → warn but proceed.

    Use case: first launch immediately after a Sierra restart, before
    Sierra has logged anything.
    """
    _validate_sierra_trade_activity_suffix(
        logs_dir=tmp_path,
        required_suffix="Sim1.simulated",
        allow_missing=True,
    )


def test_matching_suffix_passes(tmp_path: Path) -> None:
    """Sim1 + sim mode file present → check passes."""
    today = _today_utc_str()
    (tmp_path / f"TradeActivityLog_{today}_UTC.Sim1.simulated.data").write_text("")
    _validate_sierra_trade_activity_suffix(
        logs_dir=tmp_path,
        required_suffix="Sim1.simulated",
        allow_missing=False,
    )


def test_wrong_suffix_e8933_live_exits_99(tmp_path: Path) -> None:
    """E8933 (live cash account) + LIVE mode (no '.simulated.') → exit 99.

    This is the cycle 3 failure mode: Sierra came up under E8933 + live
    after the 22:00 UTC Globex reopen on 2026-05-04. Validator must
    refuse to launch Kate into this configuration.
    """
    today = _today_utc_str()
    (tmp_path / f"TradeActivityLog_{today}_UTC.E8933.data").write_text("")
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=tmp_path,
            required_suffix="Sim1.simulated",
            allow_missing=False,
        )
    assert exc_info.value.code == 99


def test_wrong_suffix_none_exits_99(tmp_path: Path) -> None:
    """Empty TradeAccount + LIVE mode → exit 99 (cycle 2 failure mode)."""
    today = _today_utc_str()
    (tmp_path / f"TradeActivityLog_{today}_UTC.None.data").write_text("")
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=tmp_path,
            required_suffix="Sim1.simulated",
            allow_missing=False,
        )
    assert exc_info.value.code == 99


def test_multiple_files_latest_correct_passes(tmp_path: Path) -> None:
    """Multiple files for same day → check the most recently modified.

    Real scenario: Sierra started in wrong mode at 22:00 UTC, operator
    fixed it via GUI mid-day. Old wrong-mode file remains on disk;
    new correct-mode file is the current state.
    """
    today = _today_utc_str()
    bad = tmp_path / f"TradeActivityLog_{today}_UTC.E8933.data"
    bad.write_text("")
    time.sleep(0.05)  # ensure mtime ordering
    good = tmp_path / f"TradeActivityLog_{today}_UTC.Sim1.simulated.data"
    good.write_text("")
    _validate_sierra_trade_activity_suffix(
        logs_dir=tmp_path,
        required_suffix="Sim1.simulated",
        allow_missing=False,
    )


def test_multiple_files_latest_wrong_exits_99(tmp_path: Path) -> None:
    """Multiple files, latest is wrong-mode → exit 99.

    Real scenario: operator started in correct mode, Sierra reverted
    later in the day. Most recent activity is wrong-mode → must refuse.
    """
    today = _today_utc_str()
    good = tmp_path / f"TradeActivityLog_{today}_UTC.Sim1.simulated.data"
    good.write_text("")
    time.sleep(0.05)
    bad = tmp_path / f"TradeActivityLog_{today}_UTC.E8933.data"
    bad.write_text("")
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=tmp_path,
            required_suffix="Sim1.simulated",
            allow_missing=False,
        )
    assert exc_info.value.code == 99


def test_live_mode_suffix_passes_when_required(tmp_path: Path) -> None:
    """Post-live-flip configuration: required suffix is just '<account>'
    (no '.simulated.'). Validator must accept this as the live-mode signal.
    """
    today = _today_utc_str()
    (tmp_path / f"TradeActivityLog_{today}_UTC.E8933.data").write_text("")
    _validate_sierra_trade_activity_suffix(
        logs_dir=tmp_path,
        required_suffix="E8933",
        allow_missing=False,
    )


def test_only_other_day_file_strict_exits_99(tmp_path: Path) -> None:
    """File exists but for a different day → still treated as no-file-today.

    Edge case: Sierra hasn't written anything today, yesterday's file is
    still on disk. Must exit 99 because today's session-init is what we
    care about.
    """
    yesterday = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    (tmp_path / f"TradeActivityLog_{yesterday}_UTC.Sim1.simulated.data").write_text("")
    with pytest.raises(SystemExit) as exc_info:
        _validate_sierra_trade_activity_suffix(
            logs_dir=tmp_path,
            required_suffix="Sim1.simulated",
            allow_missing=False,
        )
    assert exc_info.value.code == 99
