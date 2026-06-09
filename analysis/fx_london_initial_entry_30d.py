"""30-day initial-entry analysis for Kate FX London breakout.

Pulls M5 candles from MT5 (preferred) or CSV fallback, replays the current
FXLondonBreakoutStrategy, and analyzes only the first entry per symbol/session.
This is an analysis artifact: it does not place or modify orders.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import FXLondonBreakoutStrategy, StrategyContext
from trading_bot.core.strategy.stop_management import StepRatchetStopPolicy


SYMBOLS = ("GBPUSD", "EURUSD", "AUDUSD", "EURGBP")
UK = ZoneInfo("Europe/London")


@dataclass
class TradeRecord:
    symbol: str
    session_date: str
    entry_time_uk: str
    side: str
    entry: float
    stop: float
    target: float
    risk_pips: float
    range_pips: float
    breakout_pips: float
    atr_stop_pips: float
    floor_binding: bool
    exit_time_uk: str
    exit_reason: str
    r_result: float
    minutes_to_exit: float
    max_favorable_r: float
    max_adverse_r: float
    reversed_within_5m: bool
    stop_hit_within_30m: bool
    v1_be_r: float
    v2_step_r: float
    v3_continuous_r: float
    v4_aggressive_r: float


def _field(row, name: str, default=0.0):
    try:
        return row[name]
    except Exception:
        return default


def _utc_from_epoch(epoch: float, offset_seconds: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(epoch - offset_seconds, tz=dt.timezone.utc)


def _detect_mt5_offset(mt5, symbol: str) -> float:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return 0.0
    tick_time = float(getattr(tick, "time", 0.0) or 0.0)
    now = dt.datetime.now(dt.timezone.utc).timestamp()
    diff = tick_time - now
    rounded_hours = round(diff / 3600.0)
    if abs(rounded_hours) <= 6 and abs(diff - (rounded_hours * 3600.0)) < 900:
        return float(rounded_hours * 3600)
    return 0.0


def load_mt5_bars(symbols: Iterable[str], days: int, path: str) -> tuple[dict[str, list[Candle]], float]:
    import MetaTrader5 as mt5  # type: ignore

    if not mt5.initialize(path=path, timeout=60000):
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    try:
        for symbol in symbols:
            if not mt5.symbol_select(symbol, True):
                raise RuntimeError(f"MT5 symbol_select failed for {symbol}: {mt5.last_error()}")

        offset_seconds = _detect_mt5_offset(mt5, next(iter(symbols)))
        end = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        start = end - dt.timedelta(days=days + 10)
        out: dict[str, list[Candle]] = {}
        for symbol in symbols:
            rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M5, start, end)
            if rates is None or len(rates) == 0:
                raise RuntimeError(f"MT5 returned no M5 bars for {symbol}: {mt5.last_error()}")
            candles = [
                Candle(
                    timestamp=_utc_from_epoch(float(_field(r, "time")), offset_seconds),
                    open=float(_field(r, "open")),
                    high=float(_field(r, "high")),
                    low=float(_field(r, "low")),
                    close=float(_field(r, "close")),
                    volume=int(_field(r, "tick_volume", 0) or 0),
                )
                for r in rates
            ]
            candles.sort(key=lambda c: c.timestamp)
            out[symbol] = candles
        return out, offset_seconds
    finally:
        mt5.shutdown()


def load_csv_bars(path: Path) -> dict[str, list[Candle]]:
    out: dict[str, list[Candle]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            symbol = row["symbol"].strip().upper()
            ts = dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=dt.timezone.utc)
            out.setdefault(symbol, []).append(
                Candle(
                    timestamp=ts.astimezone(dt.timezone.utc),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(float(row.get("volume") or 0)),
                )
            )
    for candles in out.values():
        candles.sort(key=lambda c: c.timestamp)
    return out


def strategy_ctx(symbol: str, history: list[Candle], has_open_position: bool = False) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        exchange="ICMarketsSC-Demo",
        candle=history[-1],
        history=tuple(history),
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=has_open_position,
    )


def _side_name(side: int) -> str:
    return "BUY" if side == proto.BUY else "SELL"


def _hit_prices(side: int, bar: Candle, stop: float, target: float) -> tuple[bool, bool]:
    if side == proto.BUY:
        return bar.low <= stop, bar.high >= target
    return bar.high >= stop, bar.low <= target


def _profit_r(side: int, entry: float, price: float, risk: float) -> float:
    if risk <= 0:
        return 0.0
    return ((price - entry) if side == proto.BUY else (entry - price)) / risk


def _bar_best_worst_r(side: int, entry: float, risk: float, bar: Candle) -> tuple[float, float]:
    if side == proto.BUY:
        return (bar.high - entry) / risk, (bar.low - entry) / risk
    return (entry - bar.low) / risk, (entry - bar.high) / risk


def simulate_stop_variant(
    bars_after_entry: list[Candle],
    *,
    side: int,
    entry: float,
    stop: float,
    target: float,
    mode: str,
) -> float:
    """Conservative independent stop-management simulation.

    Stop/target hits are evaluated before same-bar close-based stage updates,
    so a wick cannot advance the stop. If stop and target are both touched in
    one bar, count the stop first.
    """
    risk = abs(entry - stop)
    if risk <= 0 or not bars_after_entry:
        return 0.0
    managed_stop = stop
    managed_target = target
    stage = 0
    peak_r = 0.0
    v2_policy = StepRatchetStopPolicy(buffer_pips=1.0)
    v2_state = v2_policy.initial_state(initial_stop=stop)

    for bar in bars_after_entry:
        best_r, _worst_r = _bar_best_worst_r(side, entry, risk, bar)
        peak_r = max(peak_r, best_r)
        stop_hit, target_hit = _hit_prices(side, bar, managed_stop, managed_target)
        if stop_hit:
            return _profit_r(side, entry, managed_stop, risk)
        if target_hit:
            return _profit_r(side, entry, managed_target, risk)

        close_r = _profit_r(side, entry, bar.close, risk)
        if mode == "v1":
            if stage == 0 and close_r >= 1.0:
                stage = 1
                managed_stop = entry + (0.0001 if side == proto.BUY else -0.0001)
        elif mode == "v2":
            decision = v2_policy.evaluate_bar_close(
                state=v2_state,
                side=side,
                entry_price=entry,
                initial_stop=stop,
                bar_close=bar.close,
                pip_size=0.0001,
            )
            v2_state = decision.state
            managed_stop = v2_state.stop_price
        elif mode == "v3":
            if peak_r >= 1.0:
                trail = entry + (
                    ((peak_r - 0.5) * risk)
                    if side == proto.BUY
                    else -((peak_r - 0.5) * risk)
                )
                managed_stop = max(managed_stop, trail) if side == proto.BUY else min(managed_stop, trail)
        elif mode == "v4":
            if stage == 0 and close_r >= 1.0:
                stage = 1
                managed_stop = entry + (0.0001 if side == proto.BUY else -0.0001)
            if stage == 1 and close_r >= 1.5:
                stage = 2
                managed_stop = entry + ((0.5 * risk) if side == proto.BUY else -(0.5 * risk))
            if stage == 2 and close_r >= 2.0:
                stage = 3
                managed_target = entry + ((3.0 * risk) if side == proto.BUY else -(3.0 * risk))
        elif mode != "v0":
            raise ValueError(f"unknown variant mode: {mode}")

    return _profit_r(side, entry, bars_after_entry[-1].close, risk)


def simulate_exit(
    bars_after_entry: list[Candle],
    *,
    side: int,
    entry: float,
    stop: float,
    target: float,
    entry_time: dt.datetime,
) -> dict[str, float | str | bool | dt.datetime]:
    risk = abs(entry - stop)
    if risk <= 0:
        raise ValueError("risk must be positive")

    max_fav_r = 0.0
    max_adv_r = 0.0
    reversed_within_5m = False
    stop_hit_within_30m = False

    v1_stop = stop
    v2_stop = stop
    v2_stage = 0
    v3_stop = stop
    v4_stop = stop
    v4_stage = 0
    v4_target = target
    v1_done = v2_done = v3_done = v4_done = False
    v1_r = v2_r = v3_r = v4_r = math.nan

    exit_reason = "flat_window"
    exit_time = bars_after_entry[-1].timestamp if bars_after_entry else entry_time
    r_result = 0.0

    for idx, bar in enumerate(bars_after_entry, start=1):
        best_r, worst_r = _bar_best_worst_r(side, entry, risk, bar)
        max_fav_r = max(max_fav_r, best_r)
        max_adv_r = min(max_adv_r, worst_r)
        if idx == 1 and best_r > 0.25 and worst_r < 0:
            reversed_within_5m = True

        stop_hit, target_hit = _hit_prices(side, bar, stop, target)
        if stop_hit and (bar.timestamp - entry_time).total_seconds() <= 30 * 60:
            stop_hit_within_30m = True
        if stop_hit or target_hit:
            if stop_hit and target_hit:
                exit_reason = "ambiguous_stop_first"
                r_result = -1.0
            elif stop_hit:
                exit_reason = "stop"
                r_result = -1.0
            else:
                exit_reason = "target"
                r_result = 2.0
            exit_time = bar.timestamp
            break

        # Variant stop checks use conservative stop-first ordering.
        for label in ("v1", "v2", "v3", "v4"):
            if label == "v1" and not v1_done and _hit_prices(side, bar, v1_stop, target)[0]:
                v1_done, v1_r = True, _profit_r(side, entry, v1_stop, risk)
            if label == "v2" and not v2_done and _hit_prices(side, bar, v2_stop, target)[0]:
                v2_done, v2_r = True, _profit_r(side, entry, v2_stop, risk)
            if label == "v3" and not v3_done and _hit_prices(side, bar, v3_stop, target)[0]:
                v3_done, v3_r = True, _profit_r(side, entry, v3_stop, risk)
            if label == "v4" and not v4_done and _hit_prices(side, bar, v4_stop, v4_target)[0]:
                v4_done, v4_r = True, _profit_r(side, entry, v4_stop, risk)

        close_r = _profit_r(side, entry, bar.close, risk)
        if not v1_done and close_r >= 1.0:
            v1_stop = entry + (0.0001 if side == proto.BUY else -0.0001)
        if not v2_done:
            if v2_stage == 0 and close_r >= 1.0:
                v2_stage = 1
                v2_stop = entry + (0.0001 if side == proto.BUY else -0.0001)
            if v2_stage == 1 and close_r >= 1.5:
                v2_stage = 2
                v2_stop = entry + ((0.5 * risk) if side == proto.BUY else -(0.5 * risk))
        if not v3_done and max_fav_r >= 1.0:
            trail = entry + ((max_fav_r - 0.5) * risk if side == proto.BUY else -((max_fav_r - 0.5) * risk))
            v3_stop = max(v3_stop, trail) if side == proto.BUY else min(v3_stop, trail)
        if not v4_done:
            if v4_stage == 0 and close_r >= 1.0:
                v4_stage = 1
                v4_stop = entry + (0.0001 if side == proto.BUY else -0.0001)
            if v4_stage == 1 and close_r >= 1.5:
                v4_stage = 2
                v4_stop = entry + ((0.5 * risk) if side == proto.BUY else -(0.5 * risk))
            if v4_stage == 2 and close_r >= 2.0:
                v4_stage = 3
                v4_target = entry + ((3.0 * risk) if side == proto.BUY else -(3.0 * risk))

    if not v1_done:
        v1_r = r_result if exit_reason != "flat_window" else _profit_r(side, entry, bars_after_entry[-1].close, risk)
    if not v2_done:
        v2_r = r_result if exit_reason != "flat_window" else _profit_r(side, entry, bars_after_entry[-1].close, risk)
    if not v3_done:
        v3_r = r_result if exit_reason != "flat_window" else _profit_r(side, entry, bars_after_entry[-1].close, risk)
    if not v4_done:
        v4_r = r_result if exit_reason != "flat_window" else _profit_r(side, entry, bars_after_entry[-1].close, risk)

    return {
        "exit_reason": exit_reason,
        "exit_time": exit_time,
        "r_result": r_result,
        "minutes_to_exit": (exit_time - entry_time).total_seconds() / 60.0,
        "max_favorable_r": max_fav_r,
        "max_adverse_r": max_adv_r,
        "reversed_within_5m": reversed_within_5m,
        "stop_hit_within_30m": stop_hit_within_30m,
        "v1_be_r": simulate_stop_variant(
            bars_after_entry, side=side, entry=entry, stop=stop, target=target, mode="v1"
        ),
        "v2_step_r": simulate_stop_variant(
            bars_after_entry, side=side, entry=entry, stop=stop, target=target, mode="v2"
        ),
        "v3_continuous_r": simulate_stop_variant(
            bars_after_entry, side=side, entry=entry, stop=stop, target=target, mode="v3"
        ),
        "v4_aggressive_r": simulate_stop_variant(
            bars_after_entry, side=side, entry=entry, stop=stop, target=target, mode="v4"
        ),
    }


def replay_symbol(symbol: str, candles: list[Candle], days: int) -> list[TradeRecord]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    candles = [c for c in candles if c.timestamp >= cutoff]
    strategy = FXLondonBreakoutStrategy(intent_cooldown_minutes=0)
    trades: list[TradeRecord] = []
    traded_sessions: set[tuple[str, dt.date]] = set()

    for i in range(strategy.history_window, len(candles) - 1):
        history = candles[: i + 1]
        ts_uk = history[-1].timestamp.astimezone(UK)
        session_key = (symbol, ts_uk.date())
        if session_key in traded_sessions:
            continue
        intent = strategy.on_candle_close(strategy_ctx(symbol, history))
        if intent is None:
            continue
        traded_sessions.add(session_key)
        strategy.mark_session_traded(symbol, intent.signal_timestamp_utc)
        entry = float(intent.price or history[-1].close)
        stop = float(intent.stop_loss or 0.0)
        target = float(intent.take_profit or 0.0)
        risk = abs(entry - stop)
        bars_after = candles[i + 1 :]
        # Stop the simulation at that session's flat time or at next day.
        clipped: list[Candle] = []
        for b in bars_after:
            b_uk = b.timestamp.astimezone(UK)
            if b_uk.date() != ts_uk.date() or b_uk.time() >= dt.time(10, 0):
                if clipped:
                    break
            clipped.append(b)
            if b_uk.time() >= dt.time(10, 0):
                break
        if not clipped:
            continue
        exit_data = simulate_exit(
            clipped,
            side=intent.side,
            entry=entry,
            stop=stop,
            target=target,
            entry_time=history[-1].timestamp,
        )
        trades.append(
            TradeRecord(
                symbol=symbol,
                session_date=str(ts_uk.date()),
                entry_time_uk=ts_uk.isoformat(),
                side=_side_name(intent.side),
                entry=entry,
                stop=stop,
                target=target,
                risk_pips=risk / 0.0001,
                range_pips=float(intent.metadata.get("asian_range_pips", 0.0)),
                breakout_pips=float(intent.metadata.get("breakout_pips", 0.0)),
                atr_stop_pips=float(intent.metadata.get("atr_stop_pips", 0.0)),
                floor_binding=intent.metadata.get("floor_binding") == "true",
                exit_time_uk=exit_data["exit_time"].astimezone(UK).isoformat(),  # type: ignore[union-attr]
                exit_reason=str(exit_data["exit_reason"]),
                r_result=float(exit_data["r_result"]),
                minutes_to_exit=float(exit_data["minutes_to_exit"]),
                max_favorable_r=float(exit_data["max_favorable_r"]),
                max_adverse_r=float(exit_data["max_adverse_r"]),
                reversed_within_5m=bool(exit_data["reversed_within_5m"]),
                stop_hit_within_30m=bool(exit_data["stop_hit_within_30m"]),
                v1_be_r=float(exit_data["v1_be_r"]),
                v2_step_r=float(exit_data["v2_step_r"]),
                v3_continuous_r=float(exit_data["v3_continuous_r"]),
                v4_aggressive_r=float(exit_data["v4_aggressive_r"]),
            )
        )
    return trades


def summarize(trades: list[TradeRecord], offset_seconds: float) -> dict:
    losers = [t for t in trades if t.r_result < 0]
    winners = [t for t in trades if t.r_result > 0]
    by_symbol = {}
    for symbol in sorted({t.symbol for t in trades}):
        subset = [t for t in trades if t.symbol == symbol]
        by_symbol[symbol] = {
            "trades": len(subset),
            "win_rate": sum(t.r_result > 0 for t in subset) / len(subset) if subset else 0.0,
            "total_r": sum(t.r_result for t in subset),
            "avg_range_pips": sum(t.range_pips for t in subset) / len(subset) if subset else 0.0,
        }
    by_side = {}
    for side in ("BUY", "SELL"):
        subset = [t for t in trades if t.side == side]
        by_side[side] = {
            "trades": len(subset),
            "win_rate": sum(t.r_result > 0 for t in subset) / len(subset) if subset else 0.0,
            "total_r": sum(t.r_result for t in subset),
        }
    variants = {}
    for field in ("r_result", "v1_be_r", "v2_step_r", "v3_continuous_r", "v4_aggressive_r"):
        vals = [getattr(t, field) for t in trades]
        variants[field] = {
            "total_r": sum(vals),
            "avg_r": sum(vals) / len(vals) if vals else 0.0,
        }
    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mt5_detected_offset_hours": offset_seconds / 3600.0,
        "trades": len(trades),
        "wins": len(winners),
        "losses": len(losers),
        "win_rate": len(winners) / len(trades) if trades else 0.0,
        "total_r": sum(t.r_result for t in trades),
        "losses_stopped_within_30m_pct": (
            sum(t.stop_hit_within_30m for t in losers) / len(losers) if losers else 0.0
        ),
        "losses_with_positive_peak_pct": (
            sum(t.max_favorable_r > 0.25 for t in losers) / len(losers) if losers else 0.0
        ),
        "losses_reversed_within_5m_pct": (
            sum(t.reversed_within_5m for t in losers) / len(losers) if losers else 0.0
        ),
        "avg_range_winners": sum(t.range_pips for t in winners) / len(winners) if winners else 0.0,
        "avg_range_losers": sum(t.range_pips for t in losers) / len(losers) if losers else 0.0,
        "by_symbol": by_symbol,
        "by_side": by_side,
        "variants": variants,
    }


def write_outputs(trades: list[TradeRecord], summary: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "fx_london_initial_entry_30d_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    with (out_dir / "fx_london_initial_entry_30d_trades.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(trades[0]).keys()) if trades else ["symbol"])
        writer.writeheader()
        for trade in trades:
            writer.writerow(asdict(trade))
    lines = [
        "# FX London Initial-Entry 30-Day Analysis",
        "",
        f"Generated UTC: {summary['generated_at_utc']}",
        f"Trades: {summary['trades']} | Win rate: {summary['win_rate']:.1%} | Total R: {summary['total_r']:.2f}",
        "",
        "## Failure Pattern",
        "",
        f"- Losing trades stopped within 30 min: {summary['losses_stopped_within_30m_pct']:.1%}",
        f"- Losing trades that first reached >0.25R profit: {summary['losses_with_positive_peak_pct']:.1%}",
        f"- Losing trades with first-bar profit-then-reverse signature: {summary['losses_reversed_within_5m_pct']:.1%}",
        f"- Average range winners vs losers: {summary['avg_range_winners']:.1f} vs {summary['avg_range_losers']:.1f} pips",
        "",
        "## Stop Variant Matrix",
        "",
    ]
    for name, label in [
        ("r_result", "V0 current"),
        ("v1_be_r", "V1 BE at 1R"),
        ("v2_step_r", "V2 step-ratchet"),
        ("v3_continuous_r", "V3 continuous trail"),
        ("v4_aggressive_r", "V4 step + 3R extension"),
    ]:
        v = summary["variants"][name]
        lines.append(f"- {label}: total R {v['total_r']:.2f}, avg R {v['avg_r']:.2f}")
    lines.extend(["", "## By Symbol", ""])
    for symbol, stats in summary["by_symbol"].items():
        lines.append(
            f"- {symbol}: trades {stats['trades']}, win rate {stats['win_rate']:.1%}, total R {stats['total_r']:.2f}, avg range {stats['avg_range_pips']:.1f}"
        )
    lines.extend(["", "## By Side", ""])
    for side, stats in summary["by_side"].items():
        lines.append(f"- {side}: trades {stats['trades']}, win rate {stats['win_rate']:.1%}, total R {stats['total_r']:.2f}")
    (out_dir / "fx_london_initial_entry_30d_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--symbols", default=",".join(SYMBOLS))
    parser.add_argument("--mt5-path", default=r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe")
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=Path("analysis/fx_london_initial_entry_30d"))
    args = parser.parse_args()

    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    if args.csv:
        bars = load_csv_bars(args.csv)
        offset_seconds = 0.0
    else:
        bars, offset_seconds = load_mt5_bars(symbols, args.days, args.mt5_path)
    trades: list[TradeRecord] = []
    for symbol in symbols:
        trades.extend(replay_symbol(symbol, bars.get(symbol, []), args.days))
    trades.sort(key=lambda t: (t.session_date, t.symbol, t.entry_time_uk))
    summary = summarize(trades, offset_seconds)
    write_outputs(trades, summary, args.out_dir)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
