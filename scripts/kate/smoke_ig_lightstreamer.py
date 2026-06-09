"""Smoke IG Lightstreamer price flow.

Runs the real IGBrokerAdapter against configured IG demo/live secrets,
subscribes to one or more symbols, and waits for closed MARKET_DATA_BAR
events with changing OHLC.

Example:
    python scripts/kate/smoke_ig_lightstreamer.py --symbols GBPUSD EURUSD --timeout 300
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time

from trading_bot.core.execution.broker_adapter import BrokerEventKind
from trading_bot.core.execution.ig_broker_adapter import (
    IGBrokerAdapter,
    IGConfig,
    IGSymbolSpec,
)


DEFAULT_SYMBOLS = {
    "GBPUSD": IGSymbolSpec("GBPUSD", "CS.D.GBPUSD.MINI.IP"),
    "EURUSD": IGSymbolSpec("EURUSD", "CS.D.EURUSD.MINI.IP"),
    "AUDUSD": IGSymbolSpec("AUDUSD", "CS.D.AUDUSD.MINI.IP"),
}


async def _run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )
    symbol_map = {
        symbol: DEFAULT_SYMBOLS[symbol]
        for symbol in args.symbols
    }
    adapter = IGBrokerAdapter(
        config=IGConfig.from_secrets(
            environment=args.environment,
            active_account_id=args.account,
        ),
        symbol_map=symbol_map,
        emit_stream_ticks=args.emit_ticks,
        require_streaming=True,
    )
    bars_seen = 0
    changing_bars = 0
    started = time.monotonic()
    try:
        await adapter.connect()
        for symbol in args.symbols:
            await adapter.subscribe_market_data(symbol=symbol, exchange="IG")
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            event = await asyncio.wait_for(adapter._events_q.get(), timeout=remaining)
            if event.kind is BrokerEventKind.MARKET_DATA_BAR and event.bar is not None:
                bar = event.bar
                bars_seen += 1
                changed = (
                    bar.open != bar.close
                    or bar.high != bar.low
                    or bar.high != bar.open
                    or bar.low != bar.open
                )
                if changed:
                    changing_bars += 1
                print(
                    "BAR "
                    f"symbol={bar.symbol} ts={bar.timestamp.isoformat()} "
                    f"o={bar.open:.5f} h={bar.high:.5f} "
                    f"l={bar.low:.5f} c={bar.close:.5f} "
                    f"v={bar.volume} changed={changed}"
                )
                if bars_seen >= args.min_bars and changing_bars >= args.min_changing:
                    elapsed = time.monotonic() - started
                    print(
                        "PASS "
                        f"bars_seen={bars_seen} changing_bars={changing_bars} "
                        f"elapsed={elapsed:.1f}s"
                    )
                    return 0
        print(
            "FAIL "
            f"bars_seen={bars_seen} changing_bars={changing_bars} "
            f"timeout={args.timeout}s"
        )
        return 2
    finally:
        await adapter.disconnect()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["GBPUSD"],
        choices=sorted(DEFAULT_SYMBOLS),
    )
    parser.add_argument("--environment", default="demo", choices=["demo", "live"])
    parser.add_argument(
        "--account",
        default=None,
        help="Override IG active account id, e.g. Z6BHQ0 for CFD demo.",
    )
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--min-bars", type=int, default=1)
    parser.add_argument("--min-changing", type=int, default=1)
    parser.add_argument("--emit-ticks", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run(_parse_args())))
