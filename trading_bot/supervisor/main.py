"""
Supervisor — top-level entry point for the trading bot.

Composes every Phase A module (data + execution + risk + state +
reconciler + strategy + engine) into a runnable process. Loads risk
policy from disk; reads instrument metadata from a static registry;
takes runtime params via CLI flags. Handles graceful shutdown on
SIGINT/SIGTERM (POSIX) or Ctrl+C / KeyboardInterrupt (Windows).

Usage:
    python -m trading_bot.supervisor.main \\
        --symbols MESM26 \\
        --scid-dir "C:/SierraChart/Data" \\
        --dtc-host 127.0.0.1 \\
        --dtc-port 11099 \\
        --trade-mode demo \\
        --log-level INFO \\
        --log-file logs/supervisor.log

Add --dry-run to build all components and exit without connecting —
useful for config / wiring validation before a real run.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import signal
import sys
from pathlib import Path

from trading_bot.core.data import CandleManager
from trading_bot.core.execution import (
    IGBrokerAdapter,
    IGConfig,
    IGSymbolSpec,
    MT5BrokerAdapter,
    MT5Config,
    NinjaBrokerAdapter,
    NinjaConfig,
    dtc_protocol as proto,
)
from trading_bot.core.execution.broker_adapter import BrokerError, BrokerSymbolSpec
from trading_bot.core.execution.dtc_broker_adapter import DTCBrokerAdapter
from trading_bot.core.risk import RiskManager, RiskPolicy
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import (
    AtrBreakoutStrategy,
    FXLondonBreakoutStrategy,
    ORBStrategy,
    SessionWindow,
)
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine
from trading_bot.supervisor.runtime import KNOWN_INSTRUMENTS

LOGGER = logging.getLogger("trading_bot.supervisor")


TRADE_MODE_LOOKUP = {
    "demo": proto.TRADE_MODE_DEMO,
    "simulated": proto.TRADE_MODE_SIMULATED,
    "live": proto.TRADE_MODE_LIVE,
}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="trading_bot.supervisor",
        description="Top-level runner for the deterministic trading bot",
    )
    p.add_argument("--symbols", nargs="+", default=["MESM26"],
                   help="logical symbols to trade (must be in KNOWN_INSTRUMENTS)")
    p.add_argument("--scid-dir", default=r"C:\SierraChart\Data",
                   help="Sierra Chart .scid file directory")
    p.add_argument("--db-path", default="data/state.db",
                   help="SQLite state-store path")
    p.add_argument("--config-dir", default="config",
                   help="directory holding risk.json + instruments.json")
    # Broker selector — DTC (Sierra Chart) today; rithmic / ibkr will plug
    # into the BrokerAdapter ABC as their concrete adapters complete. The
    # engine is broker-agnostic; this flag picks which adapter the
    # supervisor constructs.
    p.add_argument(
        "--broker", choices=["dtc", "mt5", "ninja", "ig"], default="dtc",
        help="Broker adapter selector. NOTE: 'ninja' is order-routing-only "
             "scaffold as of 2026-05-18 — engine.start() will fail at "
             "market-data subscribe + account-state seed until the Option A "
             "data path lands. 'ig' is Front 7 UK spread-bet (CGT-free), "
             "REST-only adapter — uses native bracket orders and broker "
             "market data, not DTC. See handoffs/2026-05-18-claude-to-team-"
             "NT-data-architecture-Option-A-brainstorm.md and "
             "handoffs/2026-05-21-claude-to-codex-REVIEW-REQUEST-ig-broker-adapter-front7.md",
    )
    p.add_argument("--ninja-host", default="127.0.0.1",
                   help="NinjaTrader bridge listen host (Python is the server).")
    p.add_argument("--ninja-port", type=int, default=9876,
                   help="NinjaTrader bridge listen port.")
    p.add_argument("--ninja-secrets-path", default=None,
                   help="optional secrets.json path containing nt_bridge.hmac_secret")
    p.add_argument("--ninja-account-label", default="Sim101",
                   help="NT account label for audit logs. Does not bind the "
                        "ATM template — NT decides target account via template config.")
    p.add_argument("--mt5-login", type=int, default=None,
                   help="MT5 account login. Defaults to MT5_LOGIN env or secrets file.")
    p.add_argument("--mt5-password", default=None,
                   help="MT5 password. Defaults to MT5_PASSWORD env or secrets file.")
    p.add_argument("--mt5-server", default=None,
                   help="MT5 server. Defaults to MT5_SERVER env or secrets file.")
    p.add_argument("--mt5-path", default=None,
                   help="MT5 terminal64.exe path. Defaults to MT5_PATH env or secrets file.")
    p.add_argument("--mt5-secrets-path", default=None,
                   help="optional secrets.json path containing mt5_ic_markets.demos[0]")
    p.add_argument("--dtc-host", default="127.0.0.1")
    p.add_argument("--dtc-port", type=int, default=11099)
    # Default empty: Sierra Trade Simulation Mode rejects orders that
    # specify a non-sim trade account (e.g. live "E8933"). Empty lets
    # Sierra route the order to its internal sim. Same pattern as the
    # seed requests. Override with --trade-account E8933 only when
    # Sierra's sim mode is OFF and we're cleared for live trading.
    p.add_argument("--trade-account", default="")
    # Sierra DTC has split routing: seed requests (msgs 305/300/601) work
    # with empty TradeAccount; SUBMIT (msg 208) does NOT — Sierra rejects
    # "Trade Account is empty" in Trade Simulation Mode (verified via
    # TradeActivityLog 2026-04-29). When set, this string is used for
    # SUBMIT_NEW_SINGLE_ORDER while seeds stay empty. When omitted,
    # submit reuses --trade-account (legacy behaviour).
    p.add_argument("--submit-trade-account", default=None,
                   help="trade_account string for SUBMIT_NEW_SINGLE_ORDER. "
                        "If unset, falls back to --trade-account. Set this "
                        "to Sierra's sim-account name (e.g. 'Sim1') in "
                        "Trade Simulation Mode.")
    # Volatility-blackout windows in UTC. Strategy is NOT invoked when
    # the candle timestamp falls inside any window. Format: comma-
    # separated HH:MM-HH:MM ranges, e.g. "13:30-14:30" (one window) or
    # "13:30-14:30,02:00-04:00" (multiple). Empty = always trade.
    # Decision doc: omni/decisions/2026-04-30-paper-validation-operational-additions.md
    # Default covers US cash-open vol expansion (13:30-14:30 UTC =
    # 14:30-15:30 UK BST = 09:30-10:30 ET — first hour of NYSE).
    p.add_argument("--no-trade-windows-utc", default="13:30-14:30",
                   help="comma-separated UTC blackout windows HH:MM-HH:MM. "
                        "Default '13:30-14:30' covers US cash-open vol spike. "
                        "Pass '' to disable.")
    # ── Sierra TradeActivityLog suffix validator (Gate #11/14 enforcement) ──
    # Sierra writes TradeActivityLog_<YYYY-MM-DD>_UTC.<account>.<mode>.data
    # files when its trade service initialises. The suffix encodes account
    # name + mode. Pure binary DTC + Sim1 + sim mode → expected suffix is
    # ".Sim1.simulated.data". Cycles 2-3 on 2026-05-04 saw Sierra come up
    # with ".E8933.data" or ".None.data" (live mode) and silently drop
    # submissions for hours. This validator fails Kate fast at startup if
    # the suffix doesn't match — operator sees the wrong-mode error
    # immediately instead of after orders accumulate. See
    # protocol/kate-pre-live-flip-gate.md Gates #11 + #14 for context.
    p.add_argument(
        "--trade-activity-log-dir",
        dest="trade_activity_logs_dir",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--require-trade-activity-suffix", default="Sim1.simulated",
        help="Required TradeActivityLog filename suffix (between the date and "
             "'.data'). Default 'Sim1.simulated' for paper trading. Set to "
             "'<account>' (no '.simulated.') for live trading after live-flip "
             "sign-off. Set to '' to disable the check (NOT recommended).",
    )
    p.add_argument(
        "--trade-activity-logs-dir",
        default=r"C:\SierraChart\TradeActivityLogs",
        help="Directory where Sierra writes TradeActivityLog_*.data files",
    )
    p.add_argument(
        "--allow-no-trade-activity-log", action="store_true",
        help="If set, supervisor proceeds even when no TradeActivityLog file "
             "exists for today yet. Default: fail fast (Sierra session not "
             "initialised). Useful for first-launch-after-Sierra-restart "
             "windows where the file may not have been created yet.",
    )
    p.add_argument("--client-name", default="OMNI_TRADING_BOT")
    p.add_argument("--trade-mode", choices=list(TRADE_MODE_LOOKUP), default="demo",
                   help="DTC trade-mode (demo=Sierra sim, live=real broker routing)")
    p.add_argument("--timeframe-minutes", type=int, default=1)
    p.add_argument("--tick-interval", type=float, default=1.0)
    p.add_argument("--reconciliation-interval", type=float, default=30.0)
    p.add_argument("--seed-timeout", type=float, default=2.0)
    p.add_argument("--strategy", choices=["orb", "breakout", "fx-london-breakout"], default="orb",
                   help="signal strategy: orb=multi-session Opening Range Breakout "
                        "(validated 2026-05-09, default); breakout=legacy ATR breakout "
                        "(retained for rollback / regression testing only); "
                        "fx-london-breakout=GBPUSD MT5 Front 4 strategy.")
    p.add_argument("--breakout-lookback", type=int, default=20,
                   help="legacy AtrBreakoutStrategy: prior-N-bar high lookback")
    p.add_argument("--ma-period", type=int, default=50,
                   help="legacy AtrBreakoutStrategy: SMA filter period")
    p.add_argument("--atr-period", type=int, default=14,
                   help="ATR period (used by both strategies)")
    # Default kept in sync with AtrBreakoutStrategy ctor default in
    # breakout.py — argparse here ALWAYS overrides the class default
    # because we pass args.atr_stop_mult explicitly to the ctor below,
    # so this is the load-bearing one. Lesson learned 2026-04-30:
    # editing only the class default is a no-op at runtime.
    p.add_argument("--atr-stop-mult", type=float, default=1.1,
                   help="ATR stop multiplier (used by both strategies)")
    p.add_argument("--atr-target-mult", type=float, default=3.0,
                   help="legacy AtrBreakoutStrategy: ATR target multiplier (R:R = mult/stop_mult)")
    # ORB-specific defaults match the validated configuration from
    # decisions/2026-05-09-kate-12-month-strategy-master-plan-v2.md.
    p.add_argument("--orb-reward-risk", type=float, default=2.5,
                   help="ORB: target = stop_distance × reward_risk (R:R)")
    p.add_argument("--orb-ema-period", type=int, default=200,
                   help="ORB: trend filter EMA period")
    p.add_argument("--orb-direction", choices=["both", "long", "short"], default="both",
                   help="ORB: which direction to trade")
    p.add_argument("--orb-min-range-points", type=float, default=1.0,
                   help="ORB: minimum opening-range width (filters chop)")
    p.add_argument("--orb-max-range-points", type=float, default=25.0,
                   help="ORB: maximum opening-range width (filters vol blowouts)")
    p.add_argument("--fx-quantity", type=float, default=0.01,
                   help="FX London breakout order size in lots")
    p.add_argument("--fx-reward-risk", type=float, default=2.0,
                   help="FX London breakout reward:risk")
    p.add_argument("--fx-min-range-pips", type=float, default=5.0,
                   help="FX London breakout minimum Asian range")
    p.add_argument("--fx-max-range-pips", type=float, default=120.0,
                   help="FX London breakout maximum Asian range")
    p.add_argument("--fx-min-breakout-pips", type=float, default=0.0,
                   help="FX London breakout minimum depth beyond range "
                        "boundary before firing. Default 0.0 = off. Set to "
                        "2.0 to reject shallow false-breakouts (documented "
                        "AUDUSD 2026-05-29 failure mode).")
    p.add_argument("--log-file", default=None,
                   help="optional log file path (in addition to stderr)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--dry-run", action="store_true",
                   help="build components, validate config, then exit "
                        "without connecting to DTC")
    return p.parse_args(argv)


def _setup_logging(level: str, log_file: str | None) -> None:
    fmt = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)


def _resolve_scid_basename(scid_dir: Path, rt) -> str:
    """Sierra installs vary in their .scid naming convention. VPS uses
    e.g. 'MESM26_FUT_CME.scid'; Wiltshire's local lab uses 'MESM26-CME.scid'.
    Try the configured basename first, then known alternates, return the
    first that actually exists on disk. Raise SystemExit with a clear
    message if none exist — a silent miss here means the engine polls a
    non-existent file and the strategy never fires (caused 26 hours of
    silent heartbeats on Wiltshire 2026-05-06/07 before this guard was
    added).
    """
    candidates: list[str] = []
    seen: set[str] = set()
    for cand in (rt.scid_basename, rt.dtc_symbol, f"{rt.strategy_symbol}-{rt.exchange}", rt.strategy_symbol):
        if cand and cand not in seen:
            candidates.append(cand)
            seen.add(cand)
    for cand in candidates:
        if (scid_dir / f"{cand}.scid").exists():
            if cand != rt.scid_basename:
                logging.getLogger(__name__).info(
                    "supervisor: scid for %s resolved to %s.scid (configured was %s.scid — Sierra install uses different convention on this rig)",
                    rt.strategy_symbol, cand, rt.scid_basename,
                )
            return cand
    raise SystemExit(
        f"no .scid file found for {rt.strategy_symbol} in {scid_dir} — tried: {candidates}. "
        f"Verify Sierra is recording bars for this symbol and check the filename convention."
    )


def _build_broker_adapter(*, args, instruments):
    """Construct the BrokerAdapter for the selected `--broker` value.

    Currently only `dtc` is wired. As Rithmic / IBKR adapters graduate
    from unit-green to runtime-green, add branches here. The engine
    code doesn't change; this is the seam.
    """
    if args.broker == "dtc":
        # Sierra DTC seed-request quirk: empty TradeAccount on seeds,
        # populated on submits. The DTCBrokerAdapter encapsulates this:
        # caller passes `submit_trade_account` for SUBMIT_NEW_SINGLE_ORDER
        # and the adapter sends "" on seed requests regardless of any
        # trade_account kwarg passed in by the engine.
        submit_account = (
            args.submit_trade_account
            if args.submit_trade_account is not None
            else args.trade_account
        )
        symbol_map = {
            inst.symbol: BrokerSymbolSpec(
                logical_symbol=inst.symbol,
                broker_symbol=inst.dtc_symbol,
                exchange=inst.exchange,
                tick_size=inst.tick_size,
            )
            for inst in instruments.values()
        }
        return DTCBrokerAdapter(
            host=args.dtc_host,
            port=args.dtc_port,
            client_name=args.client_name,
            trade_mode=TRADE_MODE_LOOKUP[args.trade_mode],
            symbol_map=symbol_map,
            submit_trade_account=submit_account,
            seed_timeout=args.seed_timeout,
        )
    if args.broker == "mt5":
        symbol_map = {
            inst.symbol: BrokerSymbolSpec(
                logical_symbol=inst.symbol,
                broker_symbol=inst.dtc_symbol,
                exchange=inst.exchange,
                tick_size=inst.tick_size,
            )
            for inst in instruments.values()
        }
        return MT5BrokerAdapter(
            config=_build_mt5_config(args),
            symbol_map=symbol_map,
        )
    if args.broker == "ninja":
        # Ninja branch wants the NT display form (e.g. "MES 06-26"), not
        # the DTC wire form ("MESM26-CME"). KateBridgeStrategy.cs maps
        # NT-display → MasterInstrument internally; passing DTC form would
        # fail to resolve on the NT side.
        symbol_map: dict[str, BrokerSymbolSpec] = {}
        for inst_key, inst in instruments.items():
            rt = KNOWN_INSTRUMENTS[inst_key]
            if not rt.nt_symbol:
                raise SystemExit(
                    f"--broker ninja requires nt_symbol on InstrumentRuntime "
                    f"for {inst_key!r}; add it to KNOWN_INSTRUMENTS in "
                    f"trading_bot/supervisor/runtime.py"
                )
            symbol_map[inst.symbol] = BrokerSymbolSpec(
                logical_symbol=inst.symbol,
                broker_symbol=rt.nt_symbol,
                exchange=inst.exchange,
                tick_size=inst.tick_size,
            )
        return NinjaBrokerAdapter(
            config=_build_ninja_config(args),
            symbol_map=symbol_map,
        )
    if args.broker == "ig":
        # Verified 2026-05-22 via diag against demo-api.ig.com /markets/{epic}.
        # All four are spread-bet FX mini contracts; lotSize=1.0 per IG API,
        # quantity_per_lot=10.0 is the standard FX-mini conversion (Kate's
        # 1.0 lot -> 10 GBP/point IG size). Per Codex 2026-05-21 review,
        # the 10.0 default is only safe inside verified FX MINI epics —
        # do NOT extend this map to other instruments without re-running
        # the /markets/{epic} verification.
        ig_specs = {
            "GBPUSD": IGSymbolSpec(
                logical_symbol="GBPUSD",
                epic="CS.D.GBPUSD.MINI.IP",
                quantity_per_lot=10.0,
                pip_decimal_position=4,
            ),
            "EURUSD": IGSymbolSpec(
                logical_symbol="EURUSD",
                epic="CS.D.EURUSD.MINI.IP",
                quantity_per_lot=10.0,
                pip_decimal_position=4,
            ),
            "AUDUSD": IGSymbolSpec(
                logical_symbol="AUDUSD",
                epic="CS.D.AUDUSD.MINI.IP",
                quantity_per_lot=10.0,
                pip_decimal_position=4,
            ),
            "EURGBP": IGSymbolSpec(
                logical_symbol="EURGBP",
                epic="CS.D.EURGBP.MINI.IP",
                quantity_per_lot=10.0,
                pip_decimal_position=4,
            ),
        }
        # Filter to symbols actually requested at runtime. Reject unknowns
        # loudly — guessing an epic is the exact failure class Codex blocked.
        symbol_map: dict[str, IGSymbolSpec] = {}
        for inst_key in instruments:
            if inst_key not in ig_specs:
                raise SystemExit(
                    f"--broker ig: no verified IGSymbolSpec for {inst_key!r}. "
                    f"Verified epics: {list(ig_specs)}. Add via "
                    f"/markets/{{epic}} verification + supervisor patch."
                )
            symbol_map[inst_key] = ig_specs[inst_key]
        ig_config = IGConfig.from_secrets(
            environment="live" if args.trade_mode == "live" else "demo",
        )
        return IGBrokerAdapter(
            config=ig_config,
            symbol_map=symbol_map,
        )
    raise SystemExit(f"unsupported --broker value: {args.broker!r}")


def _build_ninja_config(args) -> NinjaConfig:  # type: ignore[no-untyped-def]
    secret = _load_ninja_hmac_secret(args.ninja_secrets_path)
    return NinjaConfig(
        hmac_secret=secret,
        host=args.ninja_host,
        port=args.ninja_port,
        nt_account_label=args.ninja_account_label,
    )


def _load_ninja_hmac_secret(secrets_path: str | None) -> bytes:
    candidates: list[Path] = []
    if secrets_path:
        candidates.append(Path(secrets_path))
    env_path = os.getenv("NINJA_SECRETS_PATH")
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(r"C:\models\omni\.mcp-brain\config\secrets.json"))

    for path in candidates:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            nt = data.get("nt_bridge", {})
            secret_str = nt.get("hmac_secret")
            if secret_str:
                return secret_str.encode("utf-8")
        except Exception:
            LOGGER.warning("supervisor: could not read NT bridge secret from %s", path)
    env_secret = os.getenv("NINJA_HMAC_SECRET")
    if env_secret:
        return env_secret.encode("utf-8")
    # Match the placeholder used by auto_trigger.py + KateBridgeStrategy.cs
    # dev defaults so the smoke harness keeps working when no secrets file
    # is present. Production deploys MUST provide a real secret.
    LOGGER.warning(
        "supervisor: no NT bridge secret found in secrets.json or "
        "NINJA_HMAC_SECRET env — falling back to dev placeholder. DO NOT "
        "run --broker ninja in production with this secret."
    )
    return b"change-me-local-only"


def _build_mt5_config(args) -> MT5Config:  # type: ignore[no-untyped-def]
    base = MT5Config.from_env()
    secret_values = _load_mt5_secret_values(args.mt5_secrets_path)
    default_path = r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe"
    return MT5Config(
        login=args.mt5_login if args.mt5_login is not None else int(
            secret_values.get("login") or base.login
        ),
        password=args.mt5_password if args.mt5_password is not None else str(
            secret_values.get("password") or base.password
        ),
        server=args.mt5_server if args.mt5_server is not None else str(
            secret_values.get("server") or base.server
        ),
        path=args.mt5_path if args.mt5_path is not None else str(
            secret_values.get("path") or base.path or default_path
        ),
        timeout_ms=base.timeout_ms,
        portable=base.portable,
        magic=base.magic,
        deviation=base.deviation,
        comment=base.comment,
        poll_interval_seconds=base.poll_interval_seconds,
    )


def _load_mt5_secret_values(secrets_path: str | None) -> dict[str, object]:
    candidates: list[Path] = []
    if secrets_path:
        candidates.append(Path(secrets_path))
    env_path = os.getenv("MT5_SECRETS_PATH")
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(r"C:\models\omni\.mcp-brain\config\secrets.json"))

    for path in candidates:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            mt5 = data.get("mt5_ic_markets", {})
            demo = mt5.get("demos", [{}])[0]
            return {
                "login": demo.get("login") or demo.get("account"),
                "password": demo.get("password"),
                "server": demo.get("server") or mt5.get("server_demo"),
                "path": demo.get("path") or mt5.get("path"),
            }
        except Exception:
            LOGGER.warning("supervisor: could not read MT5 secrets from %s", path)
    return {}


def _build_instruments(
    symbols: list[str],
    scid_dir: Path | None = None,
    *,
    broker: str = "dtc",
    require_scid: bool = True,
) -> dict[str, InstrumentMeta]:
    out: dict[str, InstrumentMeta] = {}
    for s in symbols:
        if s not in KNOWN_INSTRUMENTS:
            raise SystemExit(
                f"unknown instrument: {s!r} "
                f"(known: {sorted(KNOWN_INSTRUMENTS)})"
            )
        rt = KNOWN_INSTRUMENTS[s]
        scid_filename = (
            _resolve_scid_basename(scid_dir, rt)
            if broker == "dtc" and require_scid and scid_dir is not None
            else rt.scid_basename
        )
        out[s] = InstrumentMeta(
            symbol=rt.strategy_symbol,
            exchange=rt.exchange,
            scid_filename=scid_filename,
            dtc_symbol=rt.dtc_symbol,
            tick_size=rt.tick_size,
            tick_value=rt.tick_value,
            per_contract_margin=rt.per_contract_margin,
            round_trip_commission=rt.round_trip_commission,
        )
    return out


def _parse_no_trade_windows(spec: str) -> list[tuple[dt.time, dt.time]]:
    """Parse the --no-trade-windows-utc CLI value into a list of UTC
    (start, end) time-of-day tuples. Format: comma-separated HH:MM-HH:MM
    ranges. Empty / whitespace input returns an empty list (no blackout).

    Wrap-around windows are valid: '23:30-00:30' means "everything from
    23:30 inclusive to 00:30 exclusive, crossing midnight". The engine's
    _is_in_no_trade_window handles the half-open interval semantics."""
    if not spec or not spec.strip():
        return []
    out: list[tuple[dt.time, dt.time]] = []
    for piece in spec.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" not in piece:
            raise SystemExit(f"--no-trade-windows-utc: malformed range {piece!r}")
        start_s, end_s = piece.split("-", 1)
        try:
            start = dt.time.fromisoformat(start_s.strip())
            end = dt.time.fromisoformat(end_s.strip())
        except ValueError as e:
            raise SystemExit(
                f"--no-trade-windows-utc: bad HH:MM in {piece!r} — {e}"
            ) from e
        out.append((start, end))
    return out


def _load_risk_policy(config_dir: Path) -> RiskPolicy:
    risk_path = config_dir / "risk.json"
    if not risk_path.exists():
        LOGGER.warning(
            "supervisor: no risk.json at %s — using built-in defaults "
            "(starting_nlv=1080, nlv_floor=300, ...)",
            risk_path,
        )
        return RiskPolicy()
    return RiskPolicy.from_json(risk_path)


def _on_drift(report) -> None:  # type: ignore[no-untyped-def]
    LOGGER.warning(
        "drift report: %d position drifts, %d order drifts — "
        "no auto-correct (CEO approval gate)",
        len(report.position_drifts), len(report.order_drifts),
    )


def _on_intent_rejected(intent, verdict) -> None:  # type: ignore[no-untyped-def]
    LOGGER.info(
        "risk REJECTED %s: %s",
        intent.intent_id, " | ".join(verdict.reasons),
    )


def _validate_sierra_trade_activity_suffix(
    *,
    logs_dir: Path,
    required_suffix: str,
    allow_missing: bool,
) -> None:
    """Pre-flight check that Sierra's trade service is in the expected
    account+mode before Kate connects.

    Sierra writes a TradeActivityLog_<YYYY-MM-DD>_UTC.<account>.<mode>.data
    file when its trade service initialises (typically at the 22:00 UTC
    Globex daily reopen). The suffix between the UTC date and '.data'
    encodes the active TradeAccount and (when sim mode is on) the
    '.simulated.' segment. Examples:

      .Sim1.simulated.data   — Sim1 + sim mode (paper validation)
      .E8933.simulated.data  — E8933 + sim mode (Sierra rejects this combo)
      .E8933.data            — E8933 + LIVE mode (real cash routing)
      .None.data             — empty TradeAccount + LIVE mode

    Cycles 2 + 3 on 2026-05-04 saw Sierra come up under '.E8933.data' or
    '.None.data' after a session boundary and silently drop submissions
    for hours. This check refuses to launch Kate into that state.

    Empty `required_suffix` disables the check (escape hatch — log a
    warning but proceed).

    Raises SystemExit(99) on validation failure so the watchdog .bat can
    distinguish "Sierra mode wrong" from other failure classes.
    """
    if not required_suffix:
        LOGGER.warning(
            "supervisor: TradeActivityLog suffix check disabled "
            "(--require-trade-activity-suffix is empty). "
            "Kate will launch into whatever Sierra mode is active."
        )
        return

    if not logs_dir.exists():
        msg = (
            f"supervisor: TradeActivityLog directory not found: {logs_dir}. "
            f"Is Sierra installed at the expected path? "
            f"Override with --trade-activity-logs-dir."
        )
        LOGGER.error(msg)
        raise SystemExit(99)

    today_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    pattern = f"TradeActivityLog_{today_utc}_*.data"
    matches = sorted(logs_dir.glob(pattern))

    if not matches:
        if allow_missing:
            LOGGER.warning(
                "supervisor: no TradeActivityLog for today (%s) in %s. "
                "--allow-no-trade-activity-log is set; proceeding anyway. "
                "Sierra may not have initialised its trade service yet.",
                today_utc, logs_dir,
            )
            return
        msg = (
            f"supervisor: NO TradeActivityLog file for today ({today_utc}) "
            f"in {logs_dir}. Sierra's trade service has not initialised "
            f"for the current trading day. Either Sierra is down, the "
            f"22:00 UTC Globex session-init failed, or sim/account mode "
            f"is misconfigured. RDP to the VPS, verify Sierra is running "
            f"in Sim1 + Trade Simulation Mode, save the chartbook, and "
            f"reconnect the trade service before relaunching Kate. "
            f"(Override with --allow-no-trade-activity-log to bypass.)"
        )
        LOGGER.error(msg)
        raise SystemExit(99)

    # Multiple files for the same day = mode flipped during the day.
    # Take the most recently modified one (= current Sierra state).
    latest = max(matches, key=lambda p: p.stat().st_mtime)
    name = latest.name  # e.g. "TradeActivityLog_2026-05-05_UTC.Sim1.simulated.data"
    # Strip prefix "TradeActivityLog_<YYYY-MM-DD>_" (UTC marker present
    # in older versions; absent in some — match either) and the trailing
    # ".data" to leave the suffix segment.
    prefix = f"TradeActivityLog_{today_utc}_"
    if not name.startswith(prefix):
        # Fallback: split on first underscore-date and trailing .data
        suffix_segment = name
    else:
        suffix_segment = name[len(prefix):]
    if suffix_segment.endswith(".data"):
        suffix_segment = suffix_segment[:-len(".data")]
    # suffix_segment is now e.g. "UTC.Sim1.simulated" or "UTC.E8933"
    # Sierra prepends "UTC." so strip it for clean comparison
    if suffix_segment.startswith("UTC."):
        suffix_segment = suffix_segment[len("UTC."):]

    if suffix_segment == required_suffix:
        LOGGER.info(
            "supervisor: TradeActivityLog suffix check PASS — %s matches "
            "required '%s'", latest.name, required_suffix,
        )
        return

    # Mismatch — refuse to launch
    other_files = ", ".join(p.name for p in matches if p != latest)
    msg = (
        f"supervisor: TradeActivityLog suffix check FAIL — found "
        f"'{suffix_segment}', required '{required_suffix}'. "
        f"Latest file: {latest.name}. "
        + (f"Other files today: {other_files}. " if other_files else "")
        + f"Sierra is NOT in the expected account+mode for this Kate "
        f"invocation. RDP to VPS, set Trade Account = {required_suffix.split('.')[0]}, "
        f"verify {'Trade Simulation Mode ON' if 'simulated' in required_suffix else 'live mode'}, "
        f"save chartbook, reconnect trade service. Refusing to launch — "
        f"submissions would silently drop. "
        f"(Override with --require-trade-activity-suffix '' to bypass — "
        f"NOT recommended unless you know what you are doing.)"
    )
    LOGGER.error(msg)
    raise SystemExit(99)


async def _verify_dtc_reachable(host: str, port: int, *, timeout: float = 3.0) -> None:
    """Quick TCP probe before the supervisor commits to a full DTC handshake.
    Fails with a clear error message if the host:port isn't accepting
    connections — saves debugging time when (e.g.) a firewall rule has
    silently lapsed or Sierra is down. Does NOT validate that the listener
    is actually a DTC server — just that something is accepting TCP."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
    except (asyncio.TimeoutError, OSError) as e:
        raise SystemExit(
            f"supervisor: cannot reach DTC server at {host}:{port} — {e}\n"
            f"  is Sierra running? is the firewall open? is the IP correct?"
        ) from e
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


async def _run(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir)
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    scid_dir = Path(args.scid_dir)

    state = StateStore(db_path).open()
    try:
        instruments = _build_instruments(
            args.symbols,
            scid_dir,
            broker=args.broker,
            require_scid=not args.dry_run,
        )
        candle_mgr = CandleManager(scid_dir=scid_dir, timeframe_minutes=args.timeframe_minutes)
        if args.strategy == "fx-london-breakout":
            # fail_on_unknown_symbol=True under --trade-mode live per Codex
            # 2026-05-22 A-prime cross-check: a guessed min-stop floor on
            # an unknown symbol is acceptable for demo/paper but not for
            # live capital. Demo/simulated route to the fallback + warning.
            strategy = FXLondonBreakoutStrategy(
                quantity=args.fx_quantity,
                reward_risk=args.fx_reward_risk,
                atr_period=args.atr_period,
                atr_stop_multiplier=args.atr_stop_mult,
                min_range_pips=args.fx_min_range_pips,
                max_range_pips=args.fx_max_range_pips,
                min_breakout_pips=args.fx_min_breakout_pips,
                fail_on_unknown_symbol=(args.trade_mode == "live"),
            )
        elif args.strategy == "orb":
            # Multi-session Opening Range Breakout. Sessions are hardcoded
            # to the validated Asian + US configuration from the 2026-05-09
            # master plan. Future tuning (e.g. session-time overrides)
            # routed via config/orb.json when needed; not exposed via CLI
            # to keep the runtime profile auditable.
            strategy = ORBStrategy(
                sessions=[
                    SessionWindow(
                        name="asian",
                        range_start=dt.time(0, 0),
                        range_end=dt.time(0, 30),
                        trade_end=dt.time(6, 0),
                    ),
                    SessionWindow(
                        name="us",
                        range_start=dt.time(14, 30),
                        range_end=dt.time(15, 0),
                        trade_end=dt.time(20, 45),
                    ),
                ],
                ema_period=args.orb_ema_period,
                atr_period=args.atr_period,
                atr_stop_mult=args.atr_stop_mult,
                reward_risk=args.orb_reward_risk,
                min_range_points=args.orb_min_range_points,
                max_range_points=args.orb_max_range_points,
                direction=args.orb_direction,
            )
        else:  # "breakout" — legacy ATR breakout, retained for rollback
            strategy = AtrBreakoutStrategy(
                breakout_lookback=args.breakout_lookback,
                ma_period=args.ma_period,
                atr_period=args.atr_period,
                atr_stop_mult=args.atr_stop_mult,
                atr_target_mult=args.atr_target_mult,
            )
        risk = RiskManager(_load_risk_policy(config_dir))
        reconciler = Reconciler()
        no_trade_windows = _parse_no_trade_windows(args.no_trade_windows_utc)

        # Build the broker adapter from the configured selector. Engine
        # depends on the BrokerAdapter ABC; this is the only place the
        # concrete adapter is chosen.
        broker = _build_broker_adapter(
            args=args, instruments=instruments,
        )

        engine = ManagedFuturesEngine(
            symbols=args.symbols,
            instruments=instruments,
            candle_manager=candle_mgr,
            strategy=strategy,
            risk=risk,
            state=state,
            reconciler=reconciler,
            broker=broker,
            trade_account=args.trade_account,
            no_trade_windows_utc=no_trade_windows,
            client_name=args.client_name,
            trade_mode=TRADE_MODE_LOOKUP[args.trade_mode],
            # ig is REST-only with native /positions/otc brackets, identical
            # market-data + native-bracket semantics to mt5. See Codex
            # REVIEW-RESPONSE 2026-05-21 on IGBrokerAdapter Front 7 v0.
            use_broker_market_data=args.broker in ("mt5", "ninja", "ig"),
            use_native_brackets=args.broker in ("mt5", "ninja", "ig"),
            tick_interval_seconds=args.tick_interval,
            reconciliation_interval_seconds=args.reconciliation_interval,
            on_drift=_on_drift,
            on_intent_rejected=_on_intent_rejected,
        )

        LOGGER.info(
            "supervisor: composed engine — symbols=%s dtc=%s:%d mode=%s "
            "db=%s scid_dir=%s policy=$%.0f NLV / %.1f%% per-trade strategy=%s",
            args.symbols, args.dtc_host, args.dtc_port, args.trade_mode,
            db_path, scid_dir,
            risk.policy.starting_nlv, risk.policy.max_risk_per_trade_pct_nlv * 100,
            strategy.name,
        )

        if args.dry_run:
            LOGGER.info("supervisor: dry-run successful — exiting without connecting")
            return 0

        # Pre-flight Sierra mode check (Gate #11/14 enforcement). Fails fast
        # with a clear error if Sierra's TradeActivityLog filename suffix
        # doesn't match the required account+mode — prevents Kate from
        # silently dropping submissions for hours like cycles 2-3 did.
        if args.broker == "dtc":
            LOGGER.info(
            "supervisor: validating Sierra TradeActivityLog suffix "
            "(required '%s', dir %s)",
            args.require_trade_activity_suffix, args.trade_activity_logs_dir,
            )
            _validate_sierra_trade_activity_suffix(
            logs_dir=Path(args.trade_activity_logs_dir),
            required_suffix=args.require_trade_activity_suffix,
            allow_missing=args.allow_no_trade_activity_log,
            )

        # Pre-flight TCP probe before the full DTC handshake. Fails fast
        # with a clear error if the host:port isn't reachable, instead of
        # the asyncio traceback wall the engine produces on a bad connect.
        if args.broker == "dtc":
            LOGGER.info("supervisor: probing DTC reachability at %s:%d", args.dtc_host, args.dtc_port)
            await _verify_dtc_reachable(args.dtc_host, args.dtc_port)
            LOGGER.info("supervisor: DTC port reachable — proceeding with logon")
        elif args.broker == "ninja":
            LOGGER.warning(
                "supervisor: --broker ninja is order-routing scaffold only "
                "(2026-05-18). subscribe_market_data + request_account_state "
                "will fail. Use --broker dtc until Option A data path lands."
            )
        elif args.broker == "ig":
            LOGGER.info(
                "supervisor: IG selected (Front 7 UK spread-bet); "
                "skipping Sierra DTC preflights. account_id=%s env=%s",
                broker.config.active_account_id, broker.config.environment,
            )
        else:
            LOGGER.info("supervisor: MT5 selected; skipping Sierra DTC preflights")

        stop_event = asyncio.Event()

        def _request_stop(sig_name: str) -> None:
            LOGGER.info("supervisor: received %s, requesting graceful stop", sig_name)
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, lambda n=sig_name: _request_stop(n))
            except NotImplementedError:
                # Windows asyncio: signal handlers via add_signal_handler
                # are unsupported. Ctrl+C raises KeyboardInterrupt at the
                # outer asyncio.run level — handled in main().
                pass

        try:
            await engine.start()
        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            # The most common cause is Sierra-side: the DTC port is open
            # but the server closes the connection before LOGON_RESPONSE
            # (allow-list, encoding mismatch, stale client holding the slot,
            # "Allow Trading from Network DTC Clients" disabled). Surface
            # a clear actionable message instead of an asyncio traceback.
            LOGGER.error(
                "supervisor: DTC handshake failed — %s. Check Sierra's "
                "Trade Service Log on the VPS for the reason. Common "
                "causes: 'Allow Trading from Network DTC Clients' off; "
                "stale DTC client holding the slot; encoding mismatch; "
                "IP not in Sierra's allow-list.", e,
            )
            return 2
        except BrokerError as e:
            # 2026-05-31 (Sprint 3 P0 fix per Codex REVIEW-RESPONSE):
            # broker position/open-orders seed failures are now fatal so
            # the State Hygiene Preflight never runs against an empty
            # snapshot that was empty-because-failed. Without this branch
            # the BrokerError would propagate to asyncio.run and produce
            # an opaque traceback; here we log a clear operator message
            # and return a dedicated exit code.
            LOGGER.error(
                "supervisor: broker state seed failed — %s. Cannot start "
                "without an authoritative broker position/orders snapshot "
                "(State Hygiene Preflight depends on it). Restart will "
                "keep failing until broker connectivity is restored.", e,
            )
            # Codex REVIEW-RESPONSE 2 (P1): best-effort cleanup. If
            # broker.connect() succeeded before request_positions/orders
            # raised, sockets + adapter pump tasks may still be live.
            # Process exit closes them implicitly but explicit stop()
            # reduces stale-client risk on the broker side (e.g., Sierra
            # holding our DTC slot through next restart).
            with contextlib.suppress(Exception):
                await engine.stop()
            return 4

        # State Hygiene Preflight (Sprint 3, 2026-05-31) — runs AFTER
        # engine.start() seeds broker positions/orders, BEFORE strategy
        # dispatch begins. Auto-clears stale local rows; trips startup
        # on any broker-only exposure that requires human review.
        # Permanent fix for the DB-rotation pattern (Codex HANDOFF
        # 2026-05-30).
        preflight = engine.run_state_hygiene_preflight()
        if preflight.block_trading:
            # Per Codex P1: if any safe repairs ran BEFORE the blocking
            # drift was detected, surface them in the same operator log
            # so the post-restart DB state isn't a surprise.
            if preflight.cleared_positions or preflight.marked_stale_orders:
                LOGGER.error(
                    "supervisor: state-hygiene preflight blocked startup — %s. "
                    "NOTE: %d local position(s) and %d local order(s) WERE "
                    "already repaired before the block; cleared_positions=%s, "
                    "marked_stale_orders=%s. Human review required. Restart "
                    "will keep failing until the broker-side drift is "
                    "reconciled.",
                    preflight.block_reason,
                    len(preflight.cleared_positions),
                    len(preflight.marked_stale_orders),
                    list(preflight.cleared_positions),
                    list(preflight.marked_stale_orders),
                )
            else:
                LOGGER.error(
                    "supervisor: state-hygiene preflight blocked startup — %s. "
                    "Human review required. Restart will keep failing until "
                    "the broker-side drift is reconciled.",
                    preflight.block_reason,
                )
            await engine.stop()
            return 3

        run_task = asyncio.create_task(engine.run(), name="engine.run")
        stop_task = asyncio.create_task(stop_event.wait(), name="stop_signal")

        try:
            done, pending = await asyncio.wait(
                {run_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            await engine.stop()
            for t in (run_task, stop_task):
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

        LOGGER.info("supervisor: clean shutdown")
        return 0
    finally:
        state.close()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    _setup_logging(args.log_level, args.log_file)
    LOGGER.info("supervisor: starting")
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        LOGGER.info("supervisor: KeyboardInterrupt — exiting")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
