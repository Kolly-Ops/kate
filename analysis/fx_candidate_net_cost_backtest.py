"""Net-of-cost first-pass backtests for DeepSeek strategy candidates.

This script is intentionally conservative and audit-oriented. It implements
the price/volume-only candidates from `kate-strategy-ideas-deepseek-2026-06-12`
and records data-dependent candidates as blocked instead of fabricating inputs.

Implemented:
- ATR_VOL_BREAKOUT
- SHLR
- VWMR
- TBV
- REC

Blocked for missing inputs:
- CTMF: needs rate-differential series and GBPJPY/MGC bars.
- MEM: needs macro-event calendar and consensus-surprise data.
- SPR: needs 10y seasonal sample; local MT5 depth is about 8 months.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean, pstdev
from typing import Callable, Iterable, Sequence
from zoneinfo import ZoneInfo

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto

from fx_session_net_cost_backtest import load_csv_bars, load_mt5_bars


UK = ZoneInfo("Europe/London")
DEFAULT_SYMBOLS = ("GBPUSD", "EURUSD", "AUDUSD", "EURGBP", "USDCAD")


@dataclass(frozen=True)
class CostModel:
    spread_pips: float
    commission_cash: float
    swap_cash: float
    pip_value_per_lot: float
    quantity_lots: float

    @property
    def roundtrip_cash(self) -> float:
        return (
            self.spread_pips * self.pip_value_per_lot * self.quantity_lots
            + self.commission_cash
            + self.swap_cash
        )


@dataclass(frozen=True)
class Signal:
    strategy: str
    symbol: str
    side: int
    entry_index: int
    entry_price: float
    stop_price: float
    target_price: float
    max_exit_index: int
    notes: str = ""


@dataclass
class Trade:
    strategy: str
    symbol: str
    entry_time_uk: str
    exit_time_uk: str
    side: str
    entry: float
    stop: float
    target: float
    exit_price: float
    exit_reason: str
    gross_pips: float
    gross_pnl_cash: float
    cost_cash: float
    net_pnl_cash: float
    net_r: float
    notes: str


def pip_size_for(symbol: str) -> float:
    return 0.01 if symbol.endswith("JPY") else 0.0001


def side_name(side: int) -> str:
    return "BUY" if side == proto.BUY else "SELL"


def profit_pips(side: int, entry: float, price: float, pip_size: float) -> float:
    return ((price - entry) if side == proto.BUY else (entry - price)) / pip_size


def true_range(prev_close: float, bar: Candle) -> float:
    return max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))


def atr_values(candles: Sequence[Candle], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(candles)
    trs: list[float] = []
    for i in range(1, len(candles)):
        trs.append(true_range(candles[i - 1].close, candles[i]))
        if len(trs) >= period:
            out[i] = sum(trs[-period:]) / period
    return out


def rsi_values(candles: Sequence[Candle], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(candles)
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(candles)):
        change = candles[i].close - candles[i - 1].close
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
        if len(gains) >= period:
            avg_gain = sum(gains[-period:]) / period
            avg_loss = sum(losses[-period:]) / period
            out[i] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return out


def resample(candles: Sequence[Candle], minutes: int) -> list[Candle]:
    buckets: dict[dt.datetime, list[Candle]] = {}
    for bar in candles:
        ts = bar.timestamp.astimezone(dt.timezone.utc)
        minute = (ts.minute // minutes) * minutes if minutes < 60 else 0
        hour = ts.hour if minutes < 60 else (ts.hour // (minutes // 60)) * (minutes // 60)
        key = ts.replace(hour=hour, minute=minute, second=0, microsecond=0)
        buckets.setdefault(key, []).append(bar)
    out: list[Candle] = []
    for key in sorted(buckets):
        group = buckets[key]
        out.append(
            Candle(
                timestamp=key,
                open=group[0].open,
                high=max(b.high for b in group),
                low=min(b.low for b in group),
                close=group[-1].close,
                volume=sum(b.volume for b in group),
            )
        )
    return out


def resample_daily(candles: Sequence[Candle]) -> list[Candle]:
    buckets: dict[dt.date, list[Candle]] = {}
    for bar in candles:
        buckets.setdefault(bar.timestamp.astimezone(UK).date(), []).append(bar)
    out: list[Candle] = []
    for day in sorted(buckets):
        group = buckets[day]
        ts = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK).astimezone(dt.timezone.utc)
        out.append(
            Candle(ts, group[0].open, max(b.high for b in group), min(b.low for b in group), group[-1].close, sum(b.volume for b in group))
        )
    return out


def linear_regression_value(values: Sequence[float]) -> tuple[float, float]:
    n = len(values)
    xs = list(range(n))
    xbar = mean(xs)
    ybar = mean(values)
    denom = sum((x - xbar) ** 2 for x in xs) or 1.0
    slope = sum((x - xbar) * (y - ybar) for x, y in zip(xs, values)) / denom
    return ybar + slope * (n - 1 - xbar), slope


def signals_atr_vol_breakout(symbol: str, candles_m5: Sequence[Candle]) -> list[Signal]:
    bars = resample(candles_m5, 60)
    atrs = atr_values(bars, 14)
    signals: list[Signal] = []
    for i in range(35, len(bars) - 1):
        atr = atrs[i]
        if atr is None or atr / bars[i].close < 0.005:
            continue
        prev = bars[i - 20:i]
        high = max(b.high for b in prev)
        low = min(b.low for b in prev)
        if bars[i].close > high:
            signals.append(Signal("ATR_VOL_BREAKOUT", symbol, proto.BUY, i + 1, bars[i + 1].open, bars[i + 1].open - atr, bars[i + 1].open + 3 * atr, min(i + 49, len(bars) - 1)))
        elif bars[i].close < low:
            signals.append(Signal("ATR_VOL_BREAKOUT", symbol, proto.SELL, i + 1, bars[i + 1].open, bars[i + 1].open + atr, bars[i + 1].open - 3 * atr, min(i + 49, len(bars) - 1)))
    return remap_signals_to_m5(signals, bars, candles_m5)


def signals_shlr(symbol: str, candles_m5: Sequence[Candle]) -> list[Signal]:
    signals: list[Signal] = []
    by_day: dict[dt.date, list[tuple[int, Candle]]] = {}
    for i, bar in enumerate(candles_m5):
        by_day.setdefault(bar.timestamp.astimezone(UK).date(), []).append((i, bar))
    for _day, rows in by_day.items():
        session = [(i, b) for i, b in rows if dt.time(8, 0) <= b.timestamp.astimezone(UK).time() < dt.time(12, 0)]
        after = [(i, b) for i, b in rows if dt.time(12, 0) <= b.timestamp.astimezone(UK).time() < dt.time(14, 0)]
        if not session or not after:
            continue
        high = max(b.high for _, b in session)
        low = min(b.low for _, b in session)
        broke_high = False
        broke_low = False
        for i, b in after:
            if b.high >= high * 1.002:
                broke_high = True
            if b.low <= low * 0.998:
                broke_low = True
            if broke_high and b.close < high:
                entry = b.close
                signals.append(Signal("SHLR", symbol, proto.SELL, i, entry, entry * 1.005, entry * 0.985, min(i + 144, len(candles_m5) - 1)))
                break
            if broke_low and b.close > low:
                entry = b.close
                signals.append(Signal("SHLR", symbol, proto.BUY, i, entry, entry * 0.995, entry * 1.015, min(i + 144, len(candles_m5) - 1)))
                break
    return signals


def signals_vwmr(symbol: str, candles_m5: Sequence[Candle]) -> list[Signal]:
    h1 = resample(candles_m5, 60)
    d1 = resample_daily(candles_m5)
    d_atr10 = atr_values(d1, 10)
    d_atr20 = atr_values(d1, 20)
    d_atr50 = atr_values(d1, 50)
    daily_by_date = {d1[i].timestamp.astimezone(UK).date(): i for i in range(len(d1))}
    signals: list[Signal] = []
    for i in range(240, len(h1) - 1):
        day_idx = daily_by_date.get(h1[i].timestamp.astimezone(UK).date())
        if day_idx is None or day_idx < 50:
            continue
        atr10 = d_atr10[day_idx]
        atr20 = d_atr20[day_idx]
        atr50 = d_atr50[day_idx]
        if atr10 is None or atr20 is None or atr50 is None or atr20 >= atr50:
            continue
        closes = [b.close for b in h1[i - 240:i]]
        ma = mean(closes)
        sd = pstdev(closes)
        entry = h1[i + 1].open
        if h1[i].close > ma + 3 * sd:
            signals.append(Signal("VWMR", symbol, proto.SELL, i + 1, entry, entry + 1.5 * atr10, ma, min(i + 49, len(h1) - 1)))
        elif h1[i].close < ma - 3 * sd:
            signals.append(Signal("VWMR", symbol, proto.BUY, i + 1, entry, entry - 1.5 * atr10, ma, min(i + 49, len(h1) - 1)))
    return remap_signals_to_m5(signals, h1, candles_m5)


def signals_tbv(symbol: str, candles_m5: Sequence[Candle]) -> list[Signal]:
    h4 = resample(candles_m5, 240)
    atrs = atr_values(h4, 14)
    signals: list[Signal] = []
    for i in range(25, len(h4) - 1):
        atr = atrs[i]
        if atr is None:
            continue
        closes = [b.close for b in h4[i - 20:i]]
        volumes = [b.volume for b in h4[i - 20:i]]
        line, slope = linear_regression_value(closes)
        prev_line, _ = linear_regression_value(closes[:-1])
        prev_close = h4[i - 1].close
        if h4[i].volume <= 1.5 * mean(volumes):
            continue
        entry = h4[i + 1].open
        if prev_close >= prev_line and h4[i].close < line and slope < 0:
            signals.append(Signal("TBV", symbol, proto.SELL, i + 1, entry, entry + atr, entry - 3 * atr, min(i + 31, len(h4) - 1)))
        elif prev_close <= prev_line and h4[i].close > line and slope > 0:
            signals.append(Signal("TBV", symbol, proto.BUY, i + 1, entry, entry - atr, entry + 3 * atr, min(i + 31, len(h4) - 1)))
    return remap_signals_to_m5(signals, h4, candles_m5)


def signals_rec(symbol: str, candles_m5: Sequence[Candle]) -> list[Signal]:
    h1 = resample(candles_m5, 60)
    atrs = atr_values(h1, 20)
    signals: list[Signal] = []
    for i in range(30, len(h1) - 1):
        uk_time = h1[i].timestamp.astimezone(UK).time()
        if not (dt.time(13, 0) <= uk_time <= dt.time(16, 0)):
            continue
        atr = atrs[i]
        if atr is None:
            continue
        closes = [b.close for b in h1[i - 20:i]]
        ma = mean(closes)
        sd = pstdev(closes)
        bandwidth = (4 * sd) / ma if ma else math.inf
        prev_bandwidths = []
        for j in range(i - 5, i):
            prev = [b.close for b in h1[j - 20:j]]
            prev_ma = mean(prev)
            prev_bandwidths.append((4 * pstdev(prev)) / prev_ma if prev_ma else math.inf)
        if min(prev_bandwidths) >= 0.001:
            continue
        upper = ma + 2 * sd
        lower = ma - 2 * sd
        entry = h1[i + 1].open
        if h1[i].close > upper:
            signals.append(Signal("REC", symbol, proto.BUY, i + 1, entry, entry - atr, entry + 3 * atr, min(i + 49, len(h1) - 1)))
        elif h1[i].close < lower:
            signals.append(Signal("REC", symbol, proto.SELL, i + 1, entry, entry + atr, entry - 3 * atr, min(i + 49, len(h1) - 1)))
    return remap_signals_to_m5(signals, h1, candles_m5)


def remap_signals_to_m5(signals: Sequence[Signal], source_bars: Sequence[Candle], candles_m5: Sequence[Candle]) -> list[Signal]:
    index_by_ts = {bar.timestamp: i for i, bar in enumerate(candles_m5)}
    out: list[Signal] = []
    for sig in signals:
        entry_ts = source_bars[sig.entry_index].timestamp
        max_ts = source_bars[sig.max_exit_index].timestamp
        entry_i = next((i for ts, i in index_by_ts.items() if ts >= entry_ts), None)
        max_i = next((i for ts, i in index_by_ts.items() if ts >= max_ts), None)
        if entry_i is None or max_i is None or max_i <= entry_i:
            continue
        out.append(Signal(sig.strategy, sig.symbol, sig.side, entry_i, sig.entry_price, sig.stop_price, sig.target_price, max_i, sig.notes))
    return out


def execute_signals(candles: Sequence[Candle], signals: Sequence[Signal], cost: CostModel) -> list[Trade]:
    trades: list[Trade] = []
    next_allowed = 0
    pip_size = pip_size_for(signals[0].symbol) if signals else 0.0001
    for sig in sorted(signals, key=lambda s: s.entry_index):
        if sig.entry_index < next_allowed:
            continue
        exit_price = candles[sig.max_exit_index].close
        exit_idx = sig.max_exit_index
        reason = "time_exit"
        for i in range(sig.entry_index + 1, sig.max_exit_index + 1):
            bar = candles[i]
            if sig.side == proto.BUY:
                stop_hit = bar.low <= sig.stop_price
                target_hit = bar.high >= sig.target_price
            else:
                stop_hit = bar.high >= sig.stop_price
                target_hit = bar.low <= sig.target_price
            if stop_hit:
                exit_price, exit_idx, reason = sig.stop_price, i, "stop"
                break
            if target_hit:
                exit_price, exit_idx, reason = sig.target_price, i, "target"
                break
        gross_pips = profit_pips(sig.side, sig.entry_price, exit_price, pip_size)
        gross_cash = gross_pips * cost.pip_value_per_lot * cost.quantity_lots
        net_cash = gross_cash - cost.roundtrip_cash
        risk_pips = abs(sig.entry_price - sig.stop_price) / pip_size
        risk_cash = risk_pips * cost.pip_value_per_lot * cost.quantity_lots
        trades.append(
            Trade(
                strategy=sig.strategy,
                symbol=sig.symbol,
                entry_time_uk=candles[sig.entry_index].timestamp.astimezone(UK).isoformat(),
                exit_time_uk=candles[exit_idx].timestamp.astimezone(UK).isoformat(),
                side=side_name(sig.side),
                entry=sig.entry_price,
                stop=sig.stop_price,
                target=sig.target_price,
                exit_price=exit_price,
                exit_reason=reason,
                gross_pips=gross_pips,
                gross_pnl_cash=gross_cash,
                cost_cash=cost.roundtrip_cash,
                net_pnl_cash=net_cash,
                net_r=net_cash / risk_cash if risk_cash else 0.0,
                notes=sig.notes,
            )
        )
        next_allowed = exit_idx + 1
    return trades


def summarize(trades: Sequence[Trade], starting_balance: float) -> dict:
    wins = [t for t in trades if t.net_pnl_cash > 0]
    losses = [t for t in trades if t.net_pnl_cash < 0]
    gp = sum(t.net_pnl_cash for t in wins)
    gl = abs(sum(t.net_pnl_cash for t in losses))
    equity = [starting_balance]
    for trade in trades:
        equity.append(equity[-1] + trade.net_pnl_cash)
    peak = starting_balance
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
    return {
        "trades": len(trades),
        "net_pnl": sum(t.net_pnl_cash for t in trades),
        "profit_factor_net": gp / gl if gl else None,
        "expectancy_cash": sum(t.net_pnl_cash for t in trades) / len(trades) if trades else 0.0,
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "max_drawdown_cash": max_dd,
        "max_drawdown_pct": abs(max_dd) / starting_balance if starting_balance else 0.0,
        "avg_cost_cash": sum(t.cost_cash for t in trades) / len(trades) if trades else 0.0,
    }


def write_bars_cache(path: Path, bars: dict[str, list[Candle]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "timestamp", "open", "high", "low", "close", "volume"])
        for symbol, candles in bars.items():
            for bar in candles:
                writer.writerow([symbol, bar.timestamp.isoformat(), bar.open, bar.high, bar.low, bar.close, bar.volume])


def run(args: argparse.Namespace) -> dict:
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    if args.csv and args.csv.exists():
        bars = load_csv_bars(args.csv)
        offset = 0.0
    else:
        bars, offset = load_mt5_bars(symbols, years=args.years, path=args.mt5_path)
        if args.csv:
            write_bars_cache(args.csv, bars)

    strategy_fns: dict[str, Callable[[str, Sequence[Candle]], list[Signal]]] = {
        "ATR_VOL_BREAKOUT": signals_atr_vol_breakout,
        "SHLR": signals_shlr,
        "VWMR": signals_vwmr,
        "TBV": signals_tbv,
        "REC": signals_rec,
    }
    cost = CostModel(args.spread_pips, args.commission_cash, args.swap_cash, args.pip_value_per_lot, args.quantity)
    all_trades: list[Trade] = []
    summary: dict[str, object] = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mt5_detected_offset_hours": offset / 3600.0,
        "years_requested": args.years,
        "cost_model": asdict(cost),
        "blocked": {
            "CTMF": "missing rate-differential series plus GBPJPY/MGC bars",
            "MEM": "missing macro-event calendar and consensus-surprise data",
            "SPR": "requires 10y seasonality; local MT5 depth is about 8 months",
        },
        "strategies": {},
    }
    for name, fn in strategy_fns.items():
        trades: list[Trade] = []
        for symbol in symbols:
            symbol_bars = bars.get(symbol, [])
            if not symbol_bars:
                continue
            trades.extend(execute_signals(symbol_bars, fn(symbol, symbol_bars), cost))
        trades.sort(key=lambda t: (t.entry_time_uk, t.symbol))
        all_trades.extend(trades)
        summary["strategies"][name] = summarize(trades, args.starting_balance)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (args.out_dir / "trades.csv").open("w", encoding="utf-8", newline="") as fh:
        fields = list(Trade.__dataclass_fields__.keys())
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for trade in all_trades:
            writer.writerow(asdict(trade))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=float, default=0.6)
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--quantity", type=float, default=0.56)
    parser.add_argument("--starting-balance", type=float, default=5000.0)
    parser.add_argument("--spread-pips", type=float, default=0.8)
    parser.add_argument("--commission-cash", type=float, default=4.5)
    parser.add_argument("--swap-cash", type=float, default=0.0)
    parser.add_argument("--pip-value-per-lot", type=float, default=7.8)
    parser.add_argument("--mt5-path", default=r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe")
    parser.add_argument("--csv", type=Path, default=Path("analysis/fx_candidate_8mo_bars.csv"))
    parser.add_argument("--out-dir", type=Path, default=Path("analysis/fx_candidates_8mo"))
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
