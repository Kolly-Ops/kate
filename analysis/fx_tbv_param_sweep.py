"""TBV multi-timeframe/risk-parameter sweep for Kate strategy proving."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Sequence

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto

from fx_candidate_net_cost_backtest import (
    CostModel,
    Signal,
    Trade,
    atr_values,
    execute_signals,
    linear_regression_value,
    load_csv_bars,
    pip_size_for,
    resample,
    summarize,
)


@dataclass(frozen=True)
class TBVParams:
    timeframe: str
    lookback: int
    volume_multiple: float
    atr_stop_mult: float
    reward_risk: float
    quantity: float
    min_stop_pips: float


@dataclass
class SweepRow:
    timeframe: str
    lookback: int
    volume_multiple: float
    atr_stop_mult: float
    reward_risk: float
    quantity: float
    min_stop_pips: float
    trades: int
    net_pnl: float
    profit_factor_net: float | None
    expectancy_cash: float
    win_rate: float
    max_drawdown_pct: float
    avg_cost_cash: float
    passed_gate: bool


def timeframe_minutes(value: str) -> int:
    value = value.upper()
    if value == "H1":
        return 60
    if value == "H4":
        return 240
    raise ValueError(f"unsupported TBV timeframe: {value}")


def tbv_signals(symbol: str, candles_m5: Sequence[Candle], params: TBVParams) -> list[Signal]:
    bars = resample(candles_m5, timeframe_minutes(params.timeframe))
    atr_period = 14
    atrs = atr_values(bars, atr_period)
    signals: list[Signal] = []
    start = max(params.lookback + 5, atr_period + 5)
    for i in range(start, len(bars) - 1):
        atr = atrs[i]
        if atr is None:
            continue
        closes = [b.close for b in bars[i - params.lookback:i]]
        volumes = [b.volume for b in bars[i - params.lookback:i]]
        if not volumes or bars[i].volume <= params.volume_multiple * mean(volumes):
            continue
        line, slope = linear_regression_value(closes)
        prev_line, _prev_slope = linear_regression_value(closes[:-1])
        prev_close = bars[i - 1].close
        entry = bars[i + 1].open
        pip_size = pip_size_for(symbol)
        stop_distance = max(atr * params.atr_stop_mult, params.min_stop_pips * pip_size)
        max_exit = min(i + 31, len(bars) - 1)

        if prev_close >= prev_line and bars[i].close < line and slope < 0:
            signals.append(
                Signal(
                    "TBV",
                    symbol,
                    proto.SELL,
                    i + 1,
                    entry,
                    entry + stop_distance,
                    entry - params.reward_risk * stop_distance,
                    max_exit,
                    f"tf={params.timeframe};tick_volume",
                )
            )
        elif prev_close <= prev_line and bars[i].close > line and slope > 0:
            signals.append(
                Signal(
                    "TBV",
                    symbol,
                    proto.BUY,
                    i + 1,
                    entry,
                    entry - stop_distance,
                    entry + params.reward_risk * stop_distance,
                    max_exit,
                    f"tf={params.timeframe};tick_volume",
                )
            )
    return remap_signals_to_m5(signals, bars, candles_m5)


def remap_signals_to_m5(signals: Sequence[Signal], source_bars: Sequence[Candle], candles_m5: Sequence[Candle]) -> list[Signal]:
    out: list[Signal] = []
    cursor = 0
    for sig in signals:
        entry_ts = source_bars[sig.entry_index].timestamp
        max_ts = source_bars[sig.max_exit_index].timestamp
        while cursor < len(candles_m5) and candles_m5[cursor].timestamp < entry_ts:
            cursor += 1
        entry_i = cursor
        max_i = entry_i
        while max_i < len(candles_m5) and candles_m5[max_i].timestamp < max_ts:
            max_i += 1
        if 0 <= entry_i < max_i < len(candles_m5):
            out.append(Signal(sig.strategy, sig.symbol, sig.side, entry_i, sig.entry_price, sig.stop_price, sig.target_price, max_i, sig.notes))
    return out


def run_combo(bars_by_symbol: dict[str, list[Candle]], symbols: Sequence[str], params: TBVParams, args: argparse.Namespace) -> tuple[SweepRow, list[Trade]]:
    trades: list[Trade] = []
    cost = CostModel(args.spread_pips, args.commission_cash, args.swap_cash, args.pip_value_per_lot, params.quantity)
    for symbol in symbols:
        candles = bars_by_symbol.get(symbol, [])
        if not candles:
            continue
        trades.extend(execute_signals(candles, tbv_signals(symbol, candles, params), cost))
    trades.sort(key=lambda t: (t.entry_time_uk, t.symbol))
    stats = summarize(trades, args.starting_balance)
    pf = stats["profit_factor_net"]
    passed = bool(
        stats["trades"] >= args.min_trades
        and pf is not None
        and pf >= 1.3
        and stats["expectancy_cash"] > 0
        and stats["max_drawdown_pct"] < 0.15
    )
    row = SweepRow(
        timeframe=params.timeframe,
        lookback=params.lookback,
        volume_multiple=params.volume_multiple,
        atr_stop_mult=params.atr_stop_mult,
        reward_risk=params.reward_risk,
        quantity=params.quantity,
        min_stop_pips=params.min_stop_pips,
        trades=int(stats["trades"]),
        net_pnl=float(stats["net_pnl"]),
        profit_factor_net=None if pf is None else float(pf),
        expectancy_cash=float(stats["expectancy_cash"]),
        win_rate=float(stats["win_rate"]),
        max_drawdown_pct=float(stats["max_drawdown_pct"]),
        avg_cost_cash=float(stats["avg_cost_cash"]),
        passed_gate=passed,
    )
    return row, trades


def parse_float_list(raw: str) -> list[float]:
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


def parse_int_list(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path(r"C:\models\omni\analysis\fx_candidate_8mo_bars.csv"))
    parser.add_argument("--out-dir", type=Path, default=Path(r"C:\models\omni\analysis\fx_tbv_sweep_8mo"))
    parser.add_argument("--symbols", default="GBPUSD,EURUSD,AUDUSD,EURGBP,USDCAD")
    parser.add_argument("--timeframes", default="H1,H4")
    parser.add_argument("--lookbacks", default="12,20,32")
    parser.add_argument("--volume-multiples", default="1.2,1.5,2.0")
    parser.add_argument("--atr-stop-mults", default="0.75,1.0,1.5,2.0")
    parser.add_argument("--reward-risks", default="2.0,3.0,4.0")
    parser.add_argument("--quantities", default="0.28,0.56,1.12")
    parser.add_argument("--min-stop-pips", default="0,5,10,20")
    parser.add_argument("--starting-balance", type=float, default=5000.0)
    parser.add_argument("--spread-pips", type=float, default=0.8)
    parser.add_argument("--commission-cash", type=float, default=4.5)
    parser.add_argument("--swap-cash", type=float, default=0.0)
    parser.add_argument("--pip-value-per-lot", type=float, default=7.8)
    parser.add_argument("--min-trades", type=int, default=30)
    args = parser.parse_args()

    bars_by_symbol = load_csv_bars(args.csv)
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    rows: list[SweepRow] = []
    best_trades: list[Trade] = []
    best_row: SweepRow | None = None

    for timeframe in [x.strip().upper() for x in args.timeframes.split(",") if x.strip()]:
        for lookback in parse_int_list(args.lookbacks):
            for volume_multiple in parse_float_list(args.volume_multiples):
                for atr_stop_mult in parse_float_list(args.atr_stop_mults):
                    for reward_risk in parse_float_list(args.reward_risks):
                        for quantity in parse_float_list(args.quantities):
                            for min_stop in parse_float_list(args.min_stop_pips):
                                params = TBVParams(timeframe, lookback, volume_multiple, atr_stop_mult, reward_risk, quantity, min_stop)
                                row, trades = run_combo(bars_by_symbol, symbols, params, args)
                                rows.append(row)
                                if row.trades >= args.min_trades:
                                    if best_row is None or (
                                        (row.profit_factor_net or 0.0),
                                        -row.max_drawdown_pct,
                                        row.expectancy_cash,
                                    ) > (
                                        (best_row.profit_factor_net or 0.0),
                                        -best_row.max_drawdown_pct,
                                        best_row.expectancy_cash,
                                    ):
                                        best_row = row
                                        best_trades = trades

    args.out_dir.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: ((r.profit_factor_net or 0.0), -r.max_drawdown_pct, r.expectancy_cash), reverse=True)
    with (args.out_dir / "tbv_sweep_results.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(SweepRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    with (args.out_dir / "tbv_best_trades.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(Trade.__dataclass_fields__.keys()))
        writer.writeheader()
        for trade in best_trades:
            writer.writerow(asdict(trade))
    summary = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "symbols": symbols,
        "cost_model": {
            "spread_pips": args.spread_pips,
            "commission_cash": args.commission_cash,
            "swap_cash": args.swap_cash,
            "pip_value_per_lot": args.pip_value_per_lot,
        },
        "grid_rows": len(rows),
        "pass_count": sum(r.passed_gate for r in rows),
        "best": None if best_row is None else asdict(best_row),
        "top_20": [asdict(r) for r in rows[:20]],
        "tick_volume_caveat": "FX volume is MT5 tick_volume, not centralized real volume.",
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
