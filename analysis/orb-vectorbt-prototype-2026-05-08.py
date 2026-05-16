"""Path 2 prototype: session-scoped ORB on MES 1-minute data.

This intentionally mirrors the Kate validation harness constraints:
same local Sierra .scid data, same 2.5% risk cap, same GBP 1,080 NLV,
same GBP 300 floor, and same 30% kill-switch gate. The strategy logic is
different: build an opening range after the volatility blackout and allow
at most one breakout trade per UTC day.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SIERRA_DATA = Path(r"C:\SierraChart\Data")
OUTPUT_DIR = REPO_ROOT / "analysis"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trading_bot.core.data.scid_parser import parse_scid_aggregated


@dataclass(frozen=True)
class OrbParams:
    initial_nlv: float = 1080.0
    risk_per_trade: float = 0.025
    nlv_floor: float = 300.0
    kill_switch_drawdown_pct: float = 0.30
    tick_size: float = 0.25
    tick_value_gbp: float = 1.25
    contracts: int = 1
    range_start_utc: str = "14:30"
    range_end_utc: str = "15:00"
    trade_end_utc: str = "20:45"
    reward_risk: float = 2.0
    ema_period: int = 200
    atr_period: int = 14
    atr_stop_mult: float = 1.1
    min_range_points: float = 1.0
    max_range_points: float = 25.0
    direction: str = "both"


@dataclass
class OrbTrade:
    session: str
    side: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    range_high: float
    range_low: float
    stop_loss: float
    take_profit: float
    pnl_gbp: float
    exit_reason: str
    bars_held: int


def hhmm(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).time()


def load_scid_bars(*, timeframe_minutes: int, file: str, max_gb: float) -> pd.DataFrame:
    path = SIERRA_DATA / file
    if not path.exists():
        raise FileNotFoundError(path)
    rows = parse_scid_aggregated(str(path), timeframe_min=timeframe_minutes, max_gb=max_gb)
    if not rows:
        raise ValueError(f"no candles parsed from {path}")
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    df["source_file"] = path.name
    return df[["open", "high", "low", "close", "volume", "source_file"]]


def load_yfinance_bars(*, symbol: str, period: str, interval: str) -> pd.DataFrame:
    import yfinance as yf

    df = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=False)
    if df.empty:
        raise ValueError(f"no yfinance bars returned for {symbol} period={period} interval={interval}")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower().replace(" ", "_") for col in df.columns]
    else:
        df.columns = [str(col).lower().replace(" ", "_") for col in df.columns]
    df = df.rename(columns={"adj_close": "adj_close"})
    required = ["open", "high", "low", "close"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"yfinance data missing columns: {missing}")
    if "volume" not in df.columns:
        df["volume"] = 0
    df.index = pd.to_datetime(df.index, utc=True)
    df["source_file"] = f"yfinance:{symbol}"
    return df[["open", "high", "low", "close", "volume", "source_file"]].dropna()


def add_indicators(df: pd.DataFrame, p: OrbParams) -> pd.DataFrame:
    out = df.copy()
    out["ema"] = out["close"].ewm(span=p.ema_period, adjust=False).mean()
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
    out["session"] = out.index.date
    return out


def simulate(df: pd.DataFrame, p: OrbParams) -> tuple[pd.Series, list[OrbTrade], dict]:
    equity = p.initial_nlv
    equity_curve: list[tuple[pd.Timestamp, float]] = []
    trades: list[OrbTrade] = []

    range_start = hhmm(p.range_start_utc)
    range_end = hhmm(p.range_end_utc)
    trade_end = hhmm(p.trade_end_utc)

    in_position = False
    current_session = None
    traded_today = False
    range_high = range_low = math.nan
    entry_idx = -1
    entry_time = None
    entry_price = stop_loss = take_profit = math.nan
    side = ""

    skipped_risk = 0
    skipped_range = 0
    sessions_seen = set()

    for i, (ts, row) in enumerate(df.iterrows()):
        session = ts.date()
        sessions_seen.add(session.isoformat())
        t = ts.time()

        if session != current_session:
            current_session = session
            traded_today = False
            range_high = math.nan
            range_low = math.nan

        if range_start <= t < range_end:
            high = float(row["high"])
            low = float(row["low"])
            range_high = high if math.isnan(range_high) else max(range_high, high)
            range_low = low if math.isnan(range_low) else min(range_low, low)

        if in_position:
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
            exit_price = None
            exit_reason = ""

            if side == "long":
                if low <= stop_loss:
                    exit_price = stop_loss
                    exit_reason = "stop"
                elif high >= take_profit:
                    exit_price = take_profit
                    exit_reason = "target"
                elif t >= trade_end:
                    exit_price = close
                    exit_reason = "time"
                if exit_price is not None:
                    pnl = ((exit_price - entry_price) / p.tick_size) * p.tick_value_gbp * p.contracts
            else:
                if high >= stop_loss:
                    exit_price = stop_loss
                    exit_reason = "stop"
                elif low <= take_profit:
                    exit_price = take_profit
                    exit_reason = "target"
                elif t >= trade_end:
                    exit_price = close
                    exit_reason = "time"
                if exit_price is not None:
                    pnl = ((entry_price - exit_price) / p.tick_size) * p.tick_value_gbp * p.contracts

            if exit_price is not None:
                equity += pnl
                trades.append(
                    OrbTrade(
                        session=session.isoformat(),
                        side=side,
                        entry_time=entry_time.isoformat(),
                        exit_time=ts.isoformat(),
                        entry_price=entry_price,
                        exit_price=exit_price,
                        range_high=range_high,
                        range_low=range_low,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        pnl_gbp=pnl,
                        exit_reason=exit_reason,
                        bars_held=i - entry_idx,
                    )
                )
                in_position = False
                entry_time = None

        range_ready = not math.isnan(range_high) and not math.isnan(range_low)
        trade_window = range_end <= t < trade_end
        drawdown_from_start = (p.initial_nlv - equity) / p.initial_nlv if equity < p.initial_nlv else 0.0
        risk_gate_open = equity > p.nlv_floor and drawdown_from_start < p.kill_switch_drawdown_pct

        if not in_position and not traded_today and range_ready and trade_window:
            width = range_high - range_low
            if width < p.min_range_points or width > p.max_range_points:
                skipped_range += 1
            elif not risk_gate_open:
                skipped_risk += 1
            elif pd.isna(row["atr"]):
                pass
            else:
                close = float(row["close"])
                ema = float(row["ema"])
                stop_distance = float(row["atr"]) * p.atr_stop_mult
                candidate_side = None
                if p.direction in ("both", "long") and close > range_high and close > ema:
                    candidate_side = "long"
                    entry_price = close
                    risk_points = stop_distance
                    stop_loss = entry_price - risk_points
                    take_profit = entry_price + p.reward_risk * risk_points
                elif p.direction in ("both", "short") and close < range_low and close < ema:
                    candidate_side = "short"
                    entry_price = close
                    risk_points = stop_distance
                    stop_loss = entry_price + risk_points
                    take_profit = entry_price - p.reward_risk * risk_points

                if candidate_side and risk_points > 0:
                    risk_amount = (risk_points / p.tick_size) * p.tick_value_gbp * p.contracts
                    if risk_amount <= equity * p.risk_per_trade:
                        side = candidate_side
                        entry_time = ts
                        entry_idx = i
                        in_position = True
                        traded_today = True
                    else:
                        skipped_risk += 1

        equity_curve.append((ts, equity))

    meta = {
        "sessions": len(sessions_seen),
        "skipped_risk": skipped_risk,
        "skipped_range": skipped_range,
    }
    return pd.Series([v for _, v in equity_curve], index=[t for t, _ in equity_curve]), trades, meta


def max_losing_streak(trades: list[OrbTrade]) -> int:
    longest = current = 0
    for trade in trades:
        if trade.pnl_gbp < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def summarize(df: pd.DataFrame, equity: pd.Series, trades: list[OrbTrade], p: OrbParams, meta: dict) -> dict:
    pnls = np.array([t.pnl_gbp for t in trades], dtype=float)
    returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    sharpe = float((returns.mean() / returns.std()) * math.sqrt(24 * 252)) if returns.std() > 0 else 0.0
    peak = equity.cummax()
    drawdown = equity - peak
    drawdown_pct = (equity / peak - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0)
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    total_days = max((df.index.max() - df.index.min()).days, 1)

    return {
        "data_start": df.index.min().isoformat(),
        "data_end": df.index.max().isoformat(),
        "bars": int(len(df)),
        "source_files": sorted(df["source_file"].unique().tolist()),
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
        "expectancy_gbp": float(pnls.mean()) if len(pnls) else 0.0,
        "trades_per_day": float(len(trades) / total_days),
        "trades_per_month": float(len(trades) / total_days * 30.4375),
        "largest_losing_streak": int(max_losing_streak(trades)),
        "target_exits": int(sum(t.exit_reason == "target" for t in trades)),
        "stop_exits": int(sum(t.exit_reason == "stop" for t in trades)),
        "time_exits": int(sum(t.exit_reason == "time" for t in trades)),
        "long_trades": int(sum(t.side == "long" for t in trades)),
        "short_trades": int(sum(t.side == "short" for t in trades)),
        **meta,
        "trades": [asdict(t) for t in trades],
    }


def write_plot(equity: pd.Series, output_path: Path) -> None:
    plt.figure(figsize=(11, 5))
    equity.plot()
    plt.title("MES ORB Prototype Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity (GBP)")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["scid", "yfinance"], default="scid")
    parser.add_argument("--file", default="MESM26-CME.scid")
    parser.add_argument("--yf-symbol", default=None)
    parser.add_argument("--yf-period", default="60d")
    parser.add_argument("--yf-interval", default="5m")
    parser.add_argument("--max-gb", type=float, default=1.1)
    parser.add_argument("--start", default="2026-03-23T13:55:00")
    parser.add_argument("--end", default="2026-05-07T22:33:00")
    parser.add_argument("--range-start-utc", default=OrbParams.range_start_utc)
    parser.add_argument("--range-end-utc", default=OrbParams.range_end_utc)
    parser.add_argument("--trade-end-utc", default=OrbParams.trade_end_utc)
    parser.add_argument("--reward-risk", type=float, default=OrbParams.reward_risk)
    parser.add_argument("--ema-period", type=int, default=OrbParams.ema_period)
    parser.add_argument("--atr-stop-mult", type=float, default=OrbParams.atr_stop_mult)
    parser.add_argument("--tick-size", type=float, default=OrbParams.tick_size)
    parser.add_argument("--tick-value-gbp", type=float, default=OrbParams.tick_value_gbp)
    parser.add_argument("--min-range-points", type=float, default=OrbParams.min_range_points)
    parser.add_argument("--max-range-points", type=float, default=OrbParams.max_range_points)
    parser.add_argument("--direction", choices=["long", "short", "both"], default=OrbParams.direction)
    parser.add_argument("--metrics-json", type=Path, default=OUTPUT_DIR / "orb-vectorbt-prototype-2026-05-08-metrics.json")
    parser.add_argument("--equity-png", type=Path, default=OUTPUT_DIR / "orb-vectorbt-prototype-2026-05-08-equity.png")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    p = OrbParams(
        range_start_utc=args.range_start_utc,
        range_end_utc=args.range_end_utc,
        trade_end_utc=args.trade_end_utc,
        reward_risk=args.reward_risk,
        ema_period=args.ema_period,
        atr_stop_mult=args.atr_stop_mult,
        tick_size=args.tick_size,
        tick_value_gbp=args.tick_value_gbp,
        min_range_points=args.min_range_points,
        max_range_points=args.max_range_points,
        direction=args.direction,
    )
    if args.source == "yfinance":
        if not args.yf_symbol:
            raise ValueError("--yf-symbol is required when --source yfinance")
        raw = load_yfinance_bars(symbol=args.yf_symbol, period=args.yf_period, interval=args.yf_interval)
    else:
        raw = load_scid_bars(timeframe_minutes=1, file=args.file, max_gb=args.max_gb)
    raw = raw[(raw.index >= pd.Timestamp(args.start)) & (raw.index <= pd.Timestamp(args.end))]
    if raw.empty:
        raise ValueError("no bars remain after start/end filters")
    data = add_indicators(raw, p)
    equity, trades, meta = simulate(data, p)
    metrics = summarize(data, equity, trades, p, meta)
    args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    write_plot(equity, args.equity_png)
    print(json.dumps({k: v for k, v in metrics.items() if k != "trades"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
