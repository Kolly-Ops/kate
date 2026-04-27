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
    p.add_argument("--trade-account", default="E8933")
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
    p.add_argument("--atr-stop-mult", type=float, default=2.0)
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


def _build_instruments(symbols: list[str]) -> dict[str, InstrumentMeta]:
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
            scid_filename=rt.scid_basename,
            dtc_symbol=rt.dtc_symbol,
            tick_size=rt.tick_size,
            tick_value=rt.tick_value,
            per_contract_margin=rt.per_contract_margin,
            round_trip_commission=rt.round_trip_commission,
        )
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


async def _run(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir)
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    scid_dir = Path(args.scid_dir)

    state = StateStore(db_path).open()
    try:
        instruments = _build_instruments(args.symbols)
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

        await engine.start()
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
