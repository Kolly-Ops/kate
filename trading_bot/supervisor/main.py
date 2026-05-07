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
import datetime as dt
import logging
import signal
import sys
from pathlib import Path

from trading_bot.core.data import CandleManager
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.dtc_client import DTCClient
from trading_bot.core.risk import RiskManager, RiskPolicy
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import AtrBreakoutStrategy
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
    p.add_argument("--breakout-lookback", type=int, default=20)
    p.add_argument("--ma-period", type=int, default=50)
    p.add_argument("--atr-period", type=int, default=14)
    # Default kept in sync with AtrBreakoutStrategy ctor default in
    # breakout.py — argparse here ALWAYS overrides the class default
    # because we pass args.atr_stop_mult explicitly to the ctor below,
    # so this is the load-bearing one. Lesson learned 2026-04-30:
    # editing only the class default is a no-op at runtime.
    p.add_argument("--atr-stop-mult", type=float, default=1.1)
    p.add_argument("--atr-target-mult", type=float, default=3.0)
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


def _build_instruments(symbols: list[str], scid_dir: Path) -> dict[str, InstrumentMeta]:
    out: dict[str, InstrumentMeta] = {}
    for s in symbols:
        if s not in KNOWN_INSTRUMENTS:
            raise SystemExit(
                f"unknown instrument: {s!r} "
                f"(known: {sorted(KNOWN_INSTRUMENTS)})"
            )
        rt = KNOWN_INSTRUMENTS[s]
        out[s] = InstrumentMeta(
            symbol=rt.strategy_symbol,
            exchange=rt.exchange,
            scid_filename=_resolve_scid_basename(scid_dir, rt),
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
        instruments = _build_instruments(args.symbols, scid_dir)
        candle_mgr = CandleManager(scid_dir=scid_dir, timeframe_minutes=args.timeframe_minutes)
        strategy = AtrBreakoutStrategy(
            breakout_lookback=args.breakout_lookback,
            ma_period=args.ma_period,
            atr_period=args.atr_period,
            atr_stop_mult=args.atr_stop_mult,
            atr_target_mult=args.atr_target_mult,
        )
        risk = RiskManager(_load_risk_policy(config_dir))
        reconciler = Reconciler()
        dtc_client = DTCClient(host=args.dtc_host, port=args.dtc_port)
        no_trade_windows = _parse_no_trade_windows(args.no_trade_windows_utc)

        engine = ManagedFuturesEngine(
            symbols=args.symbols,
            instruments=instruments,
            candle_manager=candle_mgr,
            strategy=strategy,
            risk=risk,
            state=state,
            reconciler=reconciler,
            dtc_client=dtc_client,
            trade_account=args.trade_account,
            submit_trade_account=args.submit_trade_account,
            no_trade_windows_utc=no_trade_windows,
            client_name=args.client_name,
            trade_mode=TRADE_MODE_LOOKUP[args.trade_mode],
            tick_interval_seconds=args.tick_interval,
            reconciliation_interval_seconds=args.reconciliation_interval,
            seed_timeout_seconds=args.seed_timeout,
            on_drift=_on_drift,
            on_intent_rejected=_on_intent_rejected,
        )

        LOGGER.info(
            "supervisor: composed engine — symbols=%s dtc=%s:%d mode=%s "
            "db=%s scid_dir=%s policy=$%.0f NLV / %.1f%% per-trade",
            args.symbols, args.dtc_host, args.dtc_port, args.trade_mode,
            db_path, scid_dir,
            risk.policy.starting_nlv, risk.policy.max_risk_per_trade_pct_nlv * 100,
        )

        if args.dry_run:
            LOGGER.info("supervisor: dry-run successful — exiting without connecting")
            return 0

        # Pre-flight Sierra mode check (Gate #11/14 enforcement). Fails fast
        # with a clear error if Sierra's TradeActivityLog filename suffix
        # doesn't match the required account+mode — prevents Kate from
        # silently dropping submissions for hours like cycles 2-3 did.
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
        LOGGER.info("supervisor: probing DTC reachability at %s:%d", args.dtc_host, args.dtc_port)
        await _verify_dtc_reachable(args.dtc_host, args.dtc_port)
        LOGGER.info("supervisor: DTC port reachable — proceeding with logon")

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
