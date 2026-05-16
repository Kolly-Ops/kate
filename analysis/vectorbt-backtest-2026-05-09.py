"""Track B: Kate ATR-breakout backtest validation.

This script is intentionally standalone: it reads Sierra Chart .scid files,
recreates Kate's current long-only ATR breakout signal, applies one-open-trade
bracket execution, and writes metrics + an equity-curve PNG for the 2026-05-11
gate review.

Data caveat: local Sierra data found on 2026-05-07 spans roughly 2025-06 to
2026-05 across MESH26/MESM26, not the requested 2-3 years of continuous MES.
The report generated from this script must be read with that limitation.
"""
from __future__ import annotations

import json
import math
import os
import sys
import argparse
from dataclasses import asdict, dataclass
from pathlib import Path

os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import vectorbt as vbt  # noqa: F401  # Presence check; simulation is custom bracket logic.
except Exception as exc:  # pragma: no cover - surfaced in report metadata.
    vbt = None
    VECTORBT_IMPORT_ERROR = repr(exc)
else:
    VECTORBT_IMPORT_ERROR = None


REPO_ROOT = Path(__file__).resolve().parents[1]
OMNI_ROOT = Path(r"C:\models\omni")
SIERRA_DATA = Path(r"C:\SierraChart\Data")
OUTPUT_DIR = REPO_ROOT / "analysis"
EQUITY_PNG = OUTPUT_DIR / "vectorbt-backtest-2026-05-09-equity.png"
METRICS_JSON = OUTPUT_DIR / "vectorbt-backtest-2026-05-09-metrics.json"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trading_bot.core.data.scid_parser import parse_scid_aggregated


@dataclass(frozen=True)
class Params:
    breakout_lookback: int = 20
    ma_period: int = 50
    atr_period: int = 14
    atr_stop_mult: float = 1.1
    atr_target_mult: float = 3.0
    initial_nlv: float = 1080.0
    risk_per_trade: float = 0.025
    nlv_floor: float = 300.0
    kill_switch_drawdown_pct: float = 0.30
    tick_size: float = 0.25
    tick_value_gbp: float = 1.0
    contracts: int = 1


@dataclass
class Trade:
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    pnl_gbp: float
    exit_reason: str
    bars_held: int


def load_scid_bars(*, timeframe_minutes: int, files: list[str], max_gb: float) -> pd.DataFrame:
    """Load and stitch available local MES contracts into candles."""
    files = [
        SIERRA_DATA / name for name in files
    ]
    frames: list[pd.DataFrame] = []
    for path in files:
        if not path.exists():
            continue
        rows = parse_scid_aggregated(str(path), timeframe_min=timeframe_minutes, max_gb=max_gb)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["source_file"] = path.name
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No local MES .scid data found in C:\\SierraChart\\Data")

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["timestamp", "source_file"])
    # Prefer the later/current contract when overlapping timestamps exist.
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    df = df.set_index("timestamp").sort_index()
    return df[["open", "high", "low", "close", "volume", "source_file"]]


def parse_time_window(value: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start, end = value.split("-", 1)
    return pd.Timestamp(start).time(), pd.Timestamp(end).time()


def in_time_windows(index: pd.DatetimeIndex, windows: list[str]) -> pd.Series:
    mask = pd.Series(False, index=index)
    for window in windows:
        start, end = parse_time_window(window)
        times = pd.Series(index.time, index=index)
        if start <= end:
            mask |= (times >= start) & (times < end)
        else:
            mask |= (times >= start) | (times < end)
    return mask


def add_indicators(df: pd.DataFrame, p: Params, blackout_windows_utc: list[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    prev_close = out["close"].shift(1)
    tr = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr"] = tr.rolling(p.atr_period).mean()
    out["sma"] = out["close"].rolling(p.ma_period).mean()
    out["prior_high"] = out["high"].shift(1).rolling(p.breakout_lookback).max()
    out["entry_signal"] = (out["close"] > out["prior_high"]) & (out["close"] > out["sma"])
    out["blackout_window"] = False
    if blackout_windows_utc:
        blackout_mask = in_time_windows(out.index, blackout_windows_utc)
        out["blackout_window"] = blackout_mask
        out.loc[blackout_mask, "entry_signal"] = False
    return out


def simulate(df: pd.DataFrame, p: Params) -> tuple[pd.Series, list[Trade]]:
    """Custom one-position bracket simulation matching Kate's execution model."""
    equity = p.initial_nlv
    equity_curve: list[tuple[pd.Timestamp, float]] = []
    trades: list[Trade] = []
    in_position = False
    entry_idx = -1
    entry_time = None
    entry_price = stop_loss = take_profit = math.nan

    for i, (ts, row) in enumerate(df.iterrows()):
        if pd.isna(row["atr"]) or pd.isna(row["sma"]) or pd.isna(row["prior_high"]):
            equity_curve.append((ts, equity))
            continue

        if in_position:
            low = float(row["low"])
            high = float(row["high"])
            exit_price = None
            exit_reason = ""

            # Conservative same-bar ambiguity: if stop and target are both touched,
            # count the stop first.
            if low <= stop_loss:
                exit_price = stop_loss
                exit_reason = "stop"
            elif high >= take_profit:
                exit_price = take_profit
                exit_reason = "target"

            if exit_price is not None:
                pnl = ((exit_price - entry_price) / p.tick_size) * p.tick_value_gbp * p.contracts
                equity += pnl
                trades.append(
                    Trade(
                        entry_time=entry_time.isoformat(),
                        exit_time=ts.isoformat(),
                        entry_price=entry_price,
                        exit_price=exit_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        pnl_gbp=pnl,
                        exit_reason=exit_reason,
                        bars_held=i - entry_idx,
                    )
                )
                in_position = False
                entry_time = None

        drawdown_from_start = (p.initial_nlv - equity) / p.initial_nlv if equity < p.initial_nlv else 0.0
        risk_gate_open = equity > p.nlv_floor and drawdown_from_start < p.kill_switch_drawdown_pct
        if not in_position and bool(row["entry_signal"]) and risk_gate_open:
            risk_budget = equity * p.risk_per_trade
            atr_stop_distance = float(row["atr"]) * p.atr_stop_mult
            risk_per_contract = (atr_stop_distance / p.tick_size) * p.tick_value_gbp
            if risk_per_contract <= risk_budget:
                entry_price = float(row["close"])
                stop_loss = entry_price - atr_stop_distance
                take_profit = entry_price + float(row["atr"]) * p.atr_target_mult
                entry_time = ts
                entry_idx = i
                in_position = True

        equity_curve.append((ts, equity))

    return pd.Series([v for _, v in equity_curve], index=[t for t, _ in equity_curve]), trades


def max_losing_streak(trades: list[Trade]) -> int:
    longest = current = 0
    for trade in trades:
        if trade.pnl_gbp < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def summarize(df: pd.DataFrame, equity: pd.Series, trades: list[Trade], p: Params) -> dict:
    trade_pnls = np.array([t.pnl_gbp for t in trades], dtype=float)
    returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    hourly_periods = max(len(returns), 1)
    annualization = math.sqrt(24 * 252)
    sharpe = float((returns.mean() / returns.std()) * annualization) if returns.std() > 0 else 0.0
    rolling_peak = equity.cummax()
    drawdown = equity - rolling_peak
    drawdown_pct = (equity / rolling_peak - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0)
    wins = trade_pnls[trade_pnls > 0]
    losses = trade_pnls[trade_pnls < 0]
    total_days = max((df.index.max() - df.index.min()).days, 1)

    metrics = {
        "data_start": df.index.min().isoformat(),
        "data_end": df.index.max().isoformat(),
        "bars": int(len(df)),
        "source_files": sorted(df["source_file"].unique().tolist()),
        "vectorbt_version": getattr(vbt, "__version__", None) if vbt is not None else None,
        "vectorbt_import_error": VECTORBT_IMPORT_ERROR,
        "params": asdict(p),
        "initial_nlv": p.initial_nlv,
        "ending_equity": float(equity.iloc[-1]),
        "total_return_pct": float((equity.iloc[-1] / p.initial_nlv - 1.0) * 100.0),
        "annualized_return_pct": float(((equity.iloc[-1] / p.initial_nlv) ** (365 / total_days) - 1.0) * 100.0),
        "sharpe_ratio": sharpe,
        "max_drawdown_gbp": float(drawdown.min()),
        "max_drawdown_pct": float(drawdown_pct.min() * 100.0),
        "trade_count": int(len(trades)),
        "win_rate_pct": float((len(wins) / len(trades) * 100.0) if trades else 0.0),
        "avg_win_gbp": float(wins.mean()) if len(wins) else 0.0,
        "avg_loss_gbp": float(losses.mean()) if len(losses) else 0.0,
        "expectancy_gbp": float(trade_pnls.mean()) if len(trade_pnls) else 0.0,
        "trades_per_day": float(len(trades) / total_days),
        "trades_per_month": float(len(trades) / total_days * 30.4375),
        "largest_losing_streak": int(max_losing_streak(trades)),
        "target_exits": int(sum(t.exit_reason == "target" for t in trades)),
        "stop_exits": int(sum(t.exit_reason == "stop" for t in trades)),
    }
    return metrics


def write_plot(equity: pd.Series, output_path: Path) -> None:
    plt.figure(figsize=(11, 5))
    equity.plot()
    plt.title("Kate ATR Breakout Backtest Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity (GBP)")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe-minutes", type=int, default=60)
    parser.add_argument(
        "--files",
        nargs="+",
        default=["MESH26-CME.scid", "MESM26-CME.scid"],
        help="Sierra .scid filenames under C:\\SierraChart\\Data",
    )
    parser.add_argument("--max-gb", type=float, default=2.0)
    parser.add_argument("--start", type=str, default=None, help="Optional inclusive start timestamp filter.")
    parser.add_argument("--end", type=str, default=None, help="Optional inclusive end timestamp filter.")
    parser.add_argument("--metrics-json", type=Path, default=METRICS_JSON)
    parser.add_argument("--equity-png", type=Path, default=EQUITY_PNG)
    parser.add_argument(
        "--blackout-windows-utc",
        nargs="*",
        default=[],
        help="UTC no-new-entry windows such as 13:30-14:30. Existing positions remain managed.",
    )
    parser.add_argument("--atr-stop-mult", type=float, default=Params.atr_stop_mult)
    parser.add_argument("--atr-target-mult", type=float, default=Params.atr_target_mult)
    parser.add_argument("--tick-value-gbp", type=float, default=Params.tick_value_gbp)
    parser.add_argument("--initial-nlv", type=float, default=Params.initial_nlv)
    parser.add_argument("--risk-per-trade", type=float, default=Params.risk_per_trade)
    parser.add_argument("--nlv-floor", type=float, default=Params.nlv_floor)
    parser.add_argument("--kill-switch-drawdown-pct", type=float, default=Params.kill_switch_drawdown_pct)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    params = Params(
        atr_stop_mult=args.atr_stop_mult,
        atr_target_mult=args.atr_target_mult,
        tick_value_gbp=args.tick_value_gbp,
        initial_nlv=args.initial_nlv,
        risk_per_trade=args.risk_per_trade,
        nlv_floor=args.nlv_floor,
        kill_switch_drawdown_pct=args.kill_switch_drawdown_pct,
    )
    raw = load_scid_bars(
        timeframe_minutes=args.timeframe_minutes,
        files=args.files,
        max_gb=args.max_gb,
    )
    if args.start:
        raw = raw[raw.index >= pd.Timestamp(args.start)]
    if args.end:
        raw = raw[raw.index <= pd.Timestamp(args.end)]
    if raw.empty:
        raise ValueError("No bars remain after applying start/end filters")
    data = add_indicators(raw, params, blackout_windows_utc=args.blackout_windows_utc)
    equity, trades = simulate(data, params)
    metrics = summarize(data, equity, trades, params)
    metrics["timeframe_minutes"] = args.timeframe_minutes
    metrics["blackout_windows_utc"] = args.blackout_windows_utc
    metrics["blackout_bars"] = int(data["blackout_window"].sum())
    metrics["trades"] = [asdict(t) for t in trades]

    args.equity_png.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
    write_plot(equity, args.equity_png)
    args.metrics_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in metrics.items() if k != "trades"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
