"""Shared-account MES multi-session ORB portfolio prototype."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


BASE_SCRIPT = Path(__file__).with_name("orb-vectorbt-prototype-2026-05-08.py")
spec = importlib.util.spec_from_file_location("orb_base", BASE_SCRIPT)
orb_base = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules["orb_base"] = orb_base
spec.loader.exec_module(orb_base)


@dataclass(frozen=True)
class SessionDef:
    name: str
    range_start_utc: str
    range_end_utc: str
    trade_end_utc: str


@dataclass
class PortfolioTrade:
    session_name: str
    session_date: str
    side: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    pnl_gbp: float
    exit_reason: str
    bars_held: int


SESSIONS = {
    "asian": SessionDef("asian", "00:00", "00:30", "06:00"),
    "europe": SessionDef("europe", "08:00", "08:30", "14:00"),
    "us": SessionDef("us", "14:30", "15:00", "20:45"),
}


def hhmm(value: str):
    return pd.Timestamp(value).time()


def max_losing_streak(trades: list[PortfolioTrade]) -> int:
    longest = current = 0
    for trade in trades:
        if trade.pnl_gbp < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def simulate_portfolio(df: pd.DataFrame, sessions: list[SessionDef], p) -> tuple[pd.Series, list[PortfolioTrade], dict]:
    equity = p.initial_nlv
    equity_curve: list[tuple[pd.Timestamp, float]] = []
    trades: list[PortfolioTrade] = []
    states = {}
    skipped_risk = 0
    skipped_range = 0

    for s in sessions:
        states[s.name] = {
            "def": s,
            "current_session": None,
            "traded_today": False,
            "range_high": math.nan,
            "range_low": math.nan,
            "in_position": False,
            "entry_idx": -1,
            "entry_time": None,
            "entry_price": math.nan,
            "stop_loss": math.nan,
            "take_profit": math.nan,
            "side": "",
        }

    for i, (ts, row) in enumerate(df.iterrows()):
        session_date = ts.date()
        t = ts.time()

        for name, st in states.items():
            sd = st["def"]
            range_start = hhmm(sd.range_start_utc)
            range_end = hhmm(sd.range_end_utc)
            trade_end = hhmm(sd.trade_end_utc)

            if session_date != st["current_session"]:
                st["current_session"] = session_date
                st["traded_today"] = False
                st["range_high"] = math.nan
                st["range_low"] = math.nan

            if range_start <= t < range_end:
                high = float(row["high"])
                low = float(row["low"])
                st["range_high"] = high if math.isnan(st["range_high"]) else max(st["range_high"], high)
                st["range_low"] = low if math.isnan(st["range_low"]) else min(st["range_low"], low)

            if st["in_position"]:
                high = float(row["high"])
                low = float(row["low"])
                close = float(row["close"])
                exit_price = None
                exit_reason = ""
                if st["side"] == "long":
                    if low <= st["stop_loss"]:
                        exit_price = st["stop_loss"]
                        exit_reason = "stop"
                    elif high >= st["take_profit"]:
                        exit_price = st["take_profit"]
                        exit_reason = "target"
                    elif t >= trade_end:
                        exit_price = close
                        exit_reason = "time"
                    if exit_price is not None:
                        pnl = ((exit_price - st["entry_price"]) / p.tick_size) * p.tick_value_gbp * p.contracts
                else:
                    if high >= st["stop_loss"]:
                        exit_price = st["stop_loss"]
                        exit_reason = "stop"
                    elif low <= st["take_profit"]:
                        exit_price = st["take_profit"]
                        exit_reason = "target"
                    elif t >= trade_end:
                        exit_price = close
                        exit_reason = "time"
                    if exit_price is not None:
                        pnl = ((st["entry_price"] - exit_price) / p.tick_size) * p.tick_value_gbp * p.contracts

                if exit_price is not None:
                    equity += pnl
                    trades.append(
                        PortfolioTrade(
                            session_name=name,
                            session_date=session_date.isoformat(),
                            side=st["side"],
                            entry_time=st["entry_time"].isoformat(),
                            exit_time=ts.isoformat(),
                            entry_price=st["entry_price"],
                            exit_price=exit_price,
                            stop_loss=st["stop_loss"],
                            take_profit=st["take_profit"],
                            pnl_gbp=pnl,
                            exit_reason=exit_reason,
                            bars_held=i - st["entry_idx"],
                        )
                    )
                    st["in_position"] = False
                    st["entry_time"] = None

            range_ready = not math.isnan(st["range_high"]) and not math.isnan(st["range_low"])
            trade_window = range_end <= t < trade_end
            drawdown_from_start = (p.initial_nlv - equity) / p.initial_nlv if equity < p.initial_nlv else 0.0
            risk_gate_open = equity > p.nlv_floor and drawdown_from_start < p.kill_switch_drawdown_pct

            if not st["in_position"] and not st["traded_today"] and range_ready and trade_window:
                width = st["range_high"] - st["range_low"]
                if width < p.min_range_points or width > p.max_range_points:
                    skipped_range += 1
                    continue
                if not risk_gate_open:
                    skipped_risk += 1
                    continue
                if pd.isna(row["atr"]):
                    continue

                close = float(row["close"])
                ema = float(row["ema"])
                stop_distance = float(row["atr"]) * p.atr_stop_mult
                candidate_side = None
                if p.direction in ("both", "long") and close > st["range_high"] and close > ema:
                    candidate_side = "long"
                    risk_points = stop_distance
                    entry_price = close
                    stop_loss = entry_price - risk_points
                    take_profit = entry_price + p.reward_risk * risk_points
                elif p.direction in ("both", "short") and close < st["range_low"] and close < ema:
                    candidate_side = "short"
                    risk_points = stop_distance
                    entry_price = close
                    stop_loss = entry_price + risk_points
                    take_profit = entry_price - p.reward_risk * risk_points

                if candidate_side and risk_points > 0:
                    risk_amount = (risk_points / p.tick_size) * p.tick_value_gbp * p.contracts
                    if risk_amount <= equity * p.risk_per_trade:
                        st["side"] = candidate_side
                        st["entry_time"] = ts
                        st["entry_idx"] = i
                        st["entry_price"] = entry_price
                        st["stop_loss"] = stop_loss
                        st["take_profit"] = take_profit
                        st["in_position"] = True
                        st["traded_today"] = True
                    else:
                        skipped_risk += 1

        equity_curve.append((ts, equity))

    return pd.Series([v for _, v in equity_curve], index=[t for t, _ in equity_curve]), trades, {
        "skipped_risk": skipped_risk,
        "skipped_range": skipped_range,
    }


def summarize(df: pd.DataFrame, equity: pd.Series, trades: list[PortfolioTrade], p, sessions: list[SessionDef], meta: dict) -> dict:
    pnls = np.array([t.pnl_gbp for t in trades], dtype=float)
    returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    sharpe = float((returns.mean() / returns.std()) * math.sqrt(24 * 252)) if returns.std() > 0 else 0.0
    peak = equity.cummax()
    drawdown = equity - peak
    drawdown_pct = (equity / peak - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0)
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    total_days = max((df.index.max() - df.index.min()).days, 1)
    by_session = {}
    for s in sessions:
        ts = [t for t in trades if t.session_name == s.name]
        spnls = np.array([t.pnl_gbp for t in ts], dtype=float)
        by_session[s.name] = {
            "trade_count": len(ts),
            "pnl_gbp": float(spnls.sum()) if len(spnls) else 0.0,
            "expectancy_gbp": float(spnls.mean()) if len(spnls) else 0.0,
            "target_exits": int(sum(t.exit_reason == "target" for t in ts)),
            "stop_exits": int(sum(t.exit_reason == "stop" for t in ts)),
            "time_exits": int(sum(t.exit_reason == "time" for t in ts)),
        }

    return {
        "data_start": df.index.min().isoformat(),
        "data_end": df.index.max().isoformat(),
        "bars": int(len(df)),
        "sessions_enabled": [s.name for s in sessions],
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
        "trades_per_week": float(len(trades) / total_days * 7),
        "trades_per_month": float(len(trades) / total_days * 30.4375),
        "largest_losing_streak": int(max_losing_streak(trades)),
        "target_exits": int(sum(t.exit_reason == "target" for t in trades)),
        "stop_exits": int(sum(t.exit_reason == "stop" for t in trades)),
        "time_exits": int(sum(t.exit_reason == "time" for t in trades)),
        "long_trades": int(sum(t.side == "long" for t in trades)),
        "short_trades": int(sum(t.side == "short" for t in trades)),
        "by_session": by_session,
        **meta,
        "trades": [asdict(t) for t in trades],
    }


def write_plot(equity: pd.Series, output_path: Path) -> None:
    plt.figure(figsize=(11, 5))
    equity.plot()
    plt.title("MES Multi-Session ORB Portfolio Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity (GBP)")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", nargs="+", choices=sorted(SESSIONS), default=["asian", "europe", "us"])
    parser.add_argument("--direction", choices=["long", "both"], default="both")
    parser.add_argument("--reward-risk", type=float, default=2.5)
    parser.add_argument("--metrics-json", type=Path, required=True)
    parser.add_argument("--equity-png", type=Path, required=True)
    args = parser.parse_args()

    p = orb_base.OrbParams(direction=args.direction, reward_risk=args.reward_risk)
    raw = orb_base.load_scid_bars(timeframe_minutes=1, file="MESM26-CME.scid", max_gb=1.1)
    raw = raw[(raw.index >= pd.Timestamp("2026-03-23T13:55:00")) & (raw.index <= pd.Timestamp("2026-05-07T22:33:00"))]
    data = orb_base.add_indicators(raw, p)
    sessions = [SESSIONS[name] for name in args.sessions]
    equity, trades, meta = simulate_portfolio(data, sessions, p)
    metrics = summarize(data, equity, trades, p, sessions, meta)
    args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_json.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    write_plot(equity, args.equity_png)
    print(json.dumps({k: v for k, v in metrics.items() if k != "trades"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
