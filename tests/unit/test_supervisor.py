"""
Unit tests for trading_bot.supervisor — CLI parsing, instrument building,
risk-policy loading, and dry-run composition.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from trading_bot.supervisor.main import (
    TRADE_MODE_LOOKUP,
    _build_instruments,
    _load_risk_policy,
    _parse_args,
    _run,
)
from trading_bot.supervisor.runtime import KNOWN_INSTRUMENTS, InstrumentRuntime


# ── runtime.py — instrument registry ──────────────────────────────────────
def test_known_instruments_have_three_distinct_identifier_types() -> None:
    """Every known instrument's three name fields should be non-empty
    and meaningful — catches accidental defaults."""
    for key, rt in KNOWN_INSTRUMENTS.items():
        assert rt.strategy_symbol, key
        assert rt.dtc_symbol, key
        assert rt.scid_basename, key
        assert rt.exchange, key
        assert rt.tick_size > 0, key
        assert rt.tick_value > 0, key


def test_mesm26_known_instrument_identifiers() -> None:
    rt = KNOWN_INSTRUMENTS["MESM26"]
    assert rt.strategy_symbol == "MESM26"
    assert rt.dtc_symbol == "MESM26-CME"
    assert rt.scid_basename == "MESM26_FUT_CME"
    assert rt.exchange == "CME"
    assert rt.tick_size == 0.25
    assert rt.tick_value == 1.25


# ── CLI parsing ───────────────────────────────────────────────────────────
def test_parse_args_defaults() -> None:
    args = _parse_args([])
    assert args.symbols == ["MESM26"]
    assert args.dtc_host == "127.0.0.1"
    assert args.dtc_port == 11099
    assert args.trade_account == ""  # empty default — Sierra sim mode rejects live accounts
    assert args.trade_mode == "demo"
    assert args.timeframe_minutes == 1
    assert args.dry_run is False


def test_parse_args_overrides_full() -> None:
    args = _parse_args([
        "--symbols", "MESM26", "MGCM26",
        "--dtc-host", "10.0.0.5",
        "--dtc-port", "12345",
        "--trade-account", "ACCT-X",
        "--trade-mode", "live",
        "--timeframe-minutes", "5",
        "--breakout-lookback", "10",
        "--atr-stop-mult", "1.5",
        "--dry-run",
    ])
    assert args.symbols == ["MESM26", "MGCM26"]
    assert args.dtc_host == "10.0.0.5"
    assert args.dtc_port == 12345
    assert args.trade_account == "ACCT-X"
    assert args.trade_mode == "live"
    assert args.timeframe_minutes == 5
    assert args.breakout_lookback == 10
    assert args.atr_stop_mult == 1.5
    assert args.dry_run is True


def test_parse_args_rejects_invalid_trade_mode() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--trade-mode", "bogus"])


def test_parse_args_rejects_invalid_log_level() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--log-level", "TRACE"])


# ── Instrument builder ────────────────────────────────────────────────────
def test_build_instruments_known() -> None:
    instruments = _build_instruments(["MESM26"])
    assert "MESM26" in instruments
    meta = instruments["MESM26"]
    assert meta.symbol == "MESM26"
    assert meta.scid_filename == "MESM26_FUT_CME"
    assert meta.dtc_symbol == "MESM26-CME"
    assert meta.tick_size == 0.25
    assert meta.tick_value == 1.25
    assert meta.per_contract_margin == 100.0


def test_build_instruments_multiple() -> None:
    instruments = _build_instruments(["MESM26", "MGCM26"])
    assert set(instruments) == {"MESM26", "MGCM26"}


def test_build_instruments_unknown_raises_clearly() -> None:
    with pytest.raises(SystemExit, match="unknown instrument"):
        _build_instruments(["FAKEXYZ"])


# ── Risk policy loader ────────────────────────────────────────────────────
def test_load_risk_policy_from_json(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "risk.json").write_text(json.dumps({
        "starting_nlv": 5000.0,
        "nlv_floor": 1000.0,
        "kill_switch_drawdown_pct": 0.20,
        "max_risk_per_trade_pct_nlv": 0.01,
        "max_margin_utilization_pct": 0.50,
        "max_open_positions": 5,
        "require_stop_loss": True,
    }))
    policy = _load_risk_policy(cfg_dir)
    assert policy.starting_nlv == 5000.0
    assert policy.kill_switch_drawdown_pct == 0.20
    assert policy.max_open_positions == 5


def test_load_risk_policy_falls_back_to_defaults_when_missing(tmp_path: Path) -> None:
    """When config/risk.json doesn't exist, supervisor uses RiskPolicy()
    defaults rather than raising — useful for first-run / dev scenarios.
    A WARNING is logged."""
    policy = _load_risk_policy(tmp_path)   # tmp_path has no risk.json
    assert policy.starting_nlv == 1080.0   # CEO-policy default
    assert policy.nlv_floor == 300.0


# ── Dry-run composition ───────────────────────────────────────────────────
def test_dry_run_composes_components_and_exits_clean(tmp_path: Path) -> None:
    """Dry-run builds everything (state store, candle manager, strategy,
    risk, reconciler, DTC client, engine) and returns 0 without
    connecting. Verifies all the wiring at the supervisor level —
    catches signature mismatches between modules in CI."""
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    # Real risk.json so we exercise the loader path
    (cfg_dir / "risk.json").write_text(json.dumps({
        "starting_nlv": 1080.0, "nlv_floor": 300.0,
        "kill_switch_drawdown_pct": 0.30,
        "max_risk_per_trade_pct_nlv": 0.015,
        "max_margin_utilization_pct": 0.40,
        "max_open_positions": 3, "require_stop_loss": True,
    }))

    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(cfg_dir),
        "--scid-dir", str(tmp_path),
        "--symbols", "MESM26",
        "--dry-run",
        "--log-level", "WARNING",   # quiet during test
    ])
    rc = asyncio.run(_run(args))
    assert rc == 0
    # State DB was created (parent dir auto-created too)
    assert (tmp_path / "data" / "state.db").exists()


def test_dry_run_with_unknown_symbol_fails_fast(tmp_path: Path) -> None:
    args = _parse_args([
        "--db-path", str(tmp_path / "state.db"),
        "--config-dir", str(tmp_path),
        "--symbols", "BOGUSSYM",
        "--dry-run",
    ])
    with pytest.raises(SystemExit, match="unknown instrument"):
        asyncio.run(_run(args))


# ── Trade-mode mapping ────────────────────────────────────────────────────
def test_trade_mode_lookup_matches_dtc_protocol_constants() -> None:
    from trading_bot.core.execution import dtc_protocol as proto
    assert TRADE_MODE_LOOKUP["demo"] == proto.TRADE_MODE_DEMO
    assert TRADE_MODE_LOOKUP["simulated"] == proto.TRADE_MODE_SIMULATED
    assert TRADE_MODE_LOOKUP["live"] == proto.TRADE_MODE_LIVE


# ── FX London Breakout — trade-mode wires fail_on_unknown_symbol ─────────
def _write_risk_json(cfg_dir: Path) -> None:
    """Minimal risk.json honouring the post-2026-05-21 schema."""
    cfg_dir.mkdir(exist_ok=True)
    (cfg_dir / "risk.json").write_text(json.dumps({
        "starting_nlv": 4998.0, "nlv_floor": 1500.0,
        "kill_switch_drawdown_pct": 0.30,
        "max_risk_per_trade_pct_nlv": 0.01,
        "max_margin_utilization_pct": 0.40,
        "max_open_positions": 1, "require_stop_loss": True,
    }))


def test_fx_london_breakout_live_mode_wires_fail_on_unknown_symbol(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per Codex 2026-05-22 A-prime cross-check: when --trade-mode live,
    supervisor must construct FXLondonBreakoutStrategy with
    fail_on_unknown_symbol=True. Guessing a min-stop floor on an
    unknown symbol is acceptable for demo only."""
    _write_risk_json(tmp_path / "config")

    captured: dict[str, object] = {}
    from trading_bot.supervisor import main as supervisor_main

    real_cls = supervisor_main.FXLondonBreakoutStrategy

    def capturing_ctor(**kwargs):
        captured.update(kwargs)
        return real_cls(**kwargs)

    monkeypatch.setattr(supervisor_main, "FXLondonBreakoutStrategy", capturing_ctor)

    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(tmp_path / "config"),
        "--scid-dir", str(tmp_path),
        "--symbols", "GBPUSD",
        "--broker", "mt5",
        "--strategy", "fx-london-breakout",
        "--trade-mode", "live",
        "--dry-run",
        "--log-level", "WARNING",
    ])
    rc = asyncio.run(_run(args))
    assert rc == 0
    assert captured.get("fail_on_unknown_symbol") is True, (
        f"live mode must wire fail_on_unknown_symbol=True; got {captured}"
    )


def test_fx_london_breakout_demo_mode_does_not_fail_loud(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Demo mode keeps fallback + warning behaviour — fail_on_unknown_symbol=False."""
    _write_risk_json(tmp_path / "config")

    captured: dict[str, object] = {}
    from trading_bot.supervisor import main as supervisor_main

    real_cls = supervisor_main.FXLondonBreakoutStrategy

    def capturing_ctor(**kwargs):
        captured.update(kwargs)
        return real_cls(**kwargs)

    monkeypatch.setattr(supervisor_main, "FXLondonBreakoutStrategy", capturing_ctor)

    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(tmp_path / "config"),
        "--scid-dir", str(tmp_path),
        "--symbols", "GBPUSD",
        "--broker", "mt5",
        "--strategy", "fx-london-breakout",
        "--trade-mode", "demo",
        "--dry-run",
        "--log-level", "WARNING",
    ])
    rc = asyncio.run(_run(args))
    assert rc == 0
    assert captured.get("fail_on_unknown_symbol") is False, (
        f"demo mode must keep fallback behaviour; got {captured}"
    )


# ── --broker ig wiring (Front 7 UK spread-bet) ───────────────────────────
def _write_ig_secrets(secrets_path: Path) -> None:
    """Minimal IG secrets stub for dry-run tests — never hits the wire."""
    secrets_path.parent.mkdir(parents=True, exist_ok=True)
    secrets_path.write_text(json.dumps({
        "ig": {
            "api_key": "test-api-key",
            "username": "test-user",
            "password": "test-password",
            "active_account_id": "Z6BHQ1",
        }
    }))


def test_broker_ig_dry_run_constructs_adapter_with_verified_symbol_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--broker ig builds an IGBrokerAdapter with the 4 verified FX-mini
    epics (CS.D.*.MINI.IP) loaded from supervisor's hard-coded ig_specs.
    Per Codex 2026-05-21 review pre-condition: no guessed epics ever
    reach the wire; supervisor rejects unverified symbols loudly.
    """
    _write_risk_json(tmp_path / "config")
    _write_ig_secrets(tmp_path / "secrets" / "secrets.json")
    monkeypatch.setenv("KATE_SECRETS_PATH", str(tmp_path / "secrets" / "secrets.json"))

    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(tmp_path / "config"),
        "--scid-dir", str(tmp_path),
        "--symbols", "GBPUSD", "EURUSD", "AUDUSD", "EURGBP",
        "--broker", "ig",
        "--strategy", "fx-london-breakout",
        "--trade-mode", "demo",
        "--dry-run",
        "--log-level", "WARNING",
    ])
    rc = asyncio.run(_run(args))
    assert rc == 0


def test_broker_ig_enables_native_brackets_and_broker_market_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 2026-05-21 review HARD requirement:

       Adapter is REST-only with native /positions/otc brackets,
       so the engine MUST receive use_native_brackets=True and
       use_broker_market_data=True. Otherwise the engine would
       try to submit stop/target as separate legs (uncovered or
       malformed exposure).

    Verifies the supervisor passes both flags = True when --broker ig.
    """
    _write_risk_json(tmp_path / "config")
    _write_ig_secrets(tmp_path / "secrets" / "secrets.json")
    monkeypatch.setenv("KATE_SECRETS_PATH", str(tmp_path / "secrets" / "secrets.json"))

    from trading_bot.supervisor import main as supervisor_main
    from trading_bot.engines.managed_futures_engine import ManagedFuturesEngine

    captured: dict[str, object] = {}
    real_engine = ManagedFuturesEngine

    def capturing_engine(*args, **kwargs):
        captured.update(kwargs)
        return real_engine(*args, **kwargs)

    monkeypatch.setattr(supervisor_main, "ManagedFuturesEngine", capturing_engine)

    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(tmp_path / "config"),
        "--scid-dir", str(tmp_path),
        "--symbols", "GBPUSD",
        "--broker", "ig",
        "--strategy", "fx-london-breakout",
        "--trade-mode", "demo",
        "--dry-run",
        "--log-level", "WARNING",
    ])
    rc = asyncio.run(_run(args))
    assert rc == 0
    assert captured.get("use_native_brackets") is True, (
        f"--broker ig MUST set use_native_brackets=True; got {captured}"
    )
    assert captured.get("use_broker_market_data") is True, (
        f"--broker ig MUST set use_broker_market_data=True; got {captured}"
    )


def test_broker_ig_rejects_unverified_symbol_loudly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per Codex: 'must be verified with real IG demo search/markets calls
    before smoke. Do not deploy with guessed epics.' Supervisor must
    SystemExit when a symbol has no verified IGSymbolSpec rather than
    fabricating one."""
    _write_risk_json(tmp_path / "config")
    _write_ig_secrets(tmp_path / "secrets" / "secrets.json")
    monkeypatch.setenv("KATE_SECRETS_PATH", str(tmp_path / "secrets" / "secrets.json"))

    # MESM26 is in KNOWN_INSTRUMENTS but NOT in the supervisor's verified
    # ig_specs — exactly the failure class Codex blocked.
    args = _parse_args([
        "--db-path", str(tmp_path / "data" / "state.db"),
        "--config-dir", str(tmp_path / "config"),
        "--scid-dir", str(tmp_path),
        "--symbols", "MESM26",
        "--broker", "ig",
        "--strategy", "fx-london-breakout",
        "--trade-mode", "demo",
        "--dry-run",
        "--log-level", "WARNING",
    ])
    with pytest.raises(SystemExit, match="no verified IGSymbolSpec"):
        asyncio.run(_run(args))
