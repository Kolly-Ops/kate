"""Net-of-cost FX session breakout backtest for Kate strategy proving.

This is the shared audit harness for the 2026-06 strategy-proving sprint.
It replays the production FX London/NY session breakout strategy on M5 bars
and reports broker-style net P&L after spread, commission, and swap costs.

The harness is deliberately data-source light:

* MT5 mode pulls bars from the local terminal with ``copy_rates_range``.
* CSV mode accepts exported bars with columns:
  ``symbol,timestamp,open,high,low,close,volume``.

It does not place orders.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import (
    FXLondonBreakoutStrategy,
    FXNYBreakoutStrategy,
    StrategyContext,
)
from trading_bot.core.strategy.stop_management import StepRatchetStopPolicy


UK = ZoneInfo("Europe/London")
DEFAULT_SYMBOLS = ("GBPUSD", "EURUSD", "AUDUSD", "EURGBP", "USDCAD")


@dataclass(frozen=True)
class CostModel:
    spread_pips: float = 0.8
    commission_roundtrip_cash: float = 0.0
    swap_cash: float = 0.0
    pip_value_per_lot_cash: float = 7.8

    def roundtrip_cost(self, quantity_lots: float) -> float:
        return (
            self.spread_pips * self.pip_value_per_lot_cash * quantity_lots
            + self.commission_roundtrip_cash
            + self.swap_cash
        )


@dataclass
class NetTrade:
    symbol: str
    session: str
    session_date: str
    entry_time_uk: str
    exit_time_uk: str
    side: str
    entry: float
    stop: float
    target: float
    exit_price: float
    exit_reason: str
    quantity_lots: float
    risk_pips: float
    gross_pips: float
    spread_pips: float
    gross_pnl_cash: float
    cost_cash: float
    net_pnl_cash: float
    gross_r: float
    net_r: float
    atr_stop_pips: float
    min_stop_pips: float
    effective_stop_pips: float
    floor_binding: bool


def _field(row, name: str, default=0.0):
    try:
        return row[name]
    except Exception:
        return default


def _parse_symbol_values(raw: str, default: float) -> dict[str, float]:
    values: dict[str, float] = {}
    if not raw:
        return values
    for part in raw.split(","):
        if not part.strip():
            continue
        if "=" not in part:
            values["*"] = float(part)
            continue
        symbol, value = part.split("=", 1)
        values[symbol.strip().upper()] = float(value.strip())
    values.setdefault("*", default)
    return values


def _symbol_value(values: dict[str, float], symbol: str, default: float) -> float:
    return values.get(symbol.upper(), values.get("*", default))


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


def load_mt5_bars(
    symbols: Iterable[str],
    *,
    years: float,
    path: str,
    timeframe: str = "M5",
) -> tuple[dict[str, list[Candle]], float]:
    import MetaTrader5 as mt5  # type: ignore

    tf_map = {
        "M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15, "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1, "H4": mt5.TIMEFRAME_H4, "D1": mt5.TIMEFRAME_D1,
    }
    tf = tf_map.get(timeframe.upper())
    if tf is None:
        raise RuntimeError(f"unsupported timeframe {timeframe!r}; use one of {sorted(tf_map)}")

    if not mt5.initialize(path=path, timeout=60000):
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    try:
        symbols = tuple(symbols)
        for symbol in symbols:
            if not mt5.symbol_select(symbol, True):
                raise RuntimeError(f"MT5 symbol_select failed for {symbol}: {mt5.last_error()}")

        offset_seconds = _detect_mt5_offset(mt5, symbols[0])
        end = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        start = end - dt.timedelta(days=int(math.ceil(years * 365.25)) + 10)
        out: dict[str, list[Candle]] = {}
        for symbol in symbols:
            rates = mt5.copy_rates_range(symbol, tf, start, end)
            if rates is None or len(rates) == 0:
                raise RuntimeError(f"MT5 returned no {timeframe} bars for {symbol}: {mt5.last_error()}")
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


def strategy_context(symbol: str, history: Sequence[Candle]) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        exchange="ICMarketsSC-Demo",
        candle=history[-1],
        history=tuple(history),
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=False,
    )


def _side_name(side: int) -> str:
    return "BUY" if side == proto.BUY else "SELL"


def _hit_prices(side: int, bar: Candle, stop: float, target: float) -> tuple[bool, bool]:
    if side == proto.BUY:
        return bar.low <= stop, bar.high >= target
    return bar.high >= stop, bar.low <= target


def _profit_pips(side: int, entry: float, price: float, pip_size: float) -> float:
    raw = price - entry if side == proto.BUY else entry - price
    return raw / pip_size


def _profit_r(side: int, entry: float, price: float, risk: float) -> float:
    if risk <= 0:
        return 0.0
    raw = price - entry if side == proto.BUY else entry - price
    return raw / risk


def _bar_close_r(side: int, entry: float, risk: float, bar: Candle) -> float:
    return _profit_r(side, entry, bar.close, risk)


def simulate_exit(
    bars_after_entry: Sequence[Candle],
    *,
    side: int,
    entry: float,
    stop: float,
    target: float,
    stop_mode: str,
    pip_size: float,
) -> tuple[float, dt.datetime, str]:
    if not bars_after_entry:
        return entry, dt.datetime.now(dt.timezone.utc), "no_bars"
    risk = abs(entry - stop)
    managed_stop = stop
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=stop)

    for bar in bars_after_entry:
        stop_hit, target_hit = _hit_prices(side, bar, managed_stop, target)
        if stop_hit and target_hit:
            return managed_stop, bar.timestamp, "ambiguous_stop_first"
        if stop_hit:
            return managed_stop, bar.timestamp, "stop"
        if target_hit:
            return target, bar.timestamp, "target"

        if stop_mode == "v2_step":
            decision = policy.evaluate_bar_close(
                state=state,
                side=side,
                entry_price=entry,
                initial_stop=stop,
                bar_close=bar.close,
                pip_size=pip_size,
            )
            state = decision.state
            managed_stop = state.stop_price
        elif stop_mode != "v0":
            raise ValueError(f"unknown stop_mode: {stop_mode}")

    return bars_after_entry[-1].close, bars_after_entry[-1].timestamp, "flat_window"


def replay_session(
    *,
    session_name: str,
    symbol: str,
    candles: Sequence[Candle],
    years: float,
    quantity: float,
    cost: CostModel,
    stop_mode: str,
) -> list[NetTrade]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=int(math.ceil(years * 365.25)))
    candles = [c for c in candles if c.timestamp >= cutoff]
    strategy_cls = FXLondonBreakoutStrategy if session_name == "london" else FXNYBreakoutStrategy
    strategy = strategy_cls(quantity=quantity, intent_cooldown_minutes=0)
    trades: list[NetTrade] = []
    traded_sessions: set[tuple[str, dt.date]] = set()
    pip_size = strategy.pip_size

    for i in range(strategy.history_window, len(candles) - 1):
        history = candles[: i + 1]
        ts_uk = history[-1].timestamp.astimezone(UK)
        session_key = (symbol, ts_uk.date())
        if session_key in traded_sessions:
            continue
        intent = strategy.on_candle_close(strategy_context(symbol, history))
        if intent is None:
            continue
        traded_sessions.add(session_key)
        strategy.mark_session_traded(symbol, intent.signal_timestamp_utc)

        entry = float(intent.price or history[-1].close)
        stop = float(intent.stop_loss or 0.0)
        target = float(intent.take_profit or 0.0)
        risk = abs(entry - stop)
        if risk <= 0:
            continue

        clipped: list[Candle] = []
        for bar in candles[i + 1 :]:
            bar_uk = bar.timestamp.astimezone(UK)
            if bar_uk.date() != ts_uk.date():
                break
            clipped.append(bar)
            if bar_uk.time() >= strategy.session.force_flat:
                break
        if not clipped:
            continue

        exit_price, exit_time, exit_reason = simulate_exit(
            clipped,
            side=intent.side,
            entry=entry,
            stop=stop,
            target=target,
            stop_mode=stop_mode,
            pip_size=pip_size,
        )
        gross_pips = _profit_pips(intent.side, entry, exit_price, pip_size)
        risk_pips = risk / pip_size
        gross_pnl = gross_pips * cost.pip_value_per_lot_cash * quantity
        total_cost = cost.roundtrip_cost(quantity)
        net_pnl = gross_pnl - total_cost
        risk_cash = risk_pips * cost.pip_value_per_lot_cash * quantity
        trades.append(
            NetTrade(
                symbol=symbol,
                session=session_name,
                session_date=str(ts_uk.date()),
                entry_time_uk=ts_uk.isoformat(),
                exit_time_uk=exit_time.astimezone(UK).isoformat(),
                side=_side_name(intent.side),
                entry=entry,
                stop=stop,
                target=target,
                exit_price=exit_price,
                exit_reason=exit_reason,
                quantity_lots=quantity,
                risk_pips=risk_pips,
                gross_pips=gross_pips,
                spread_pips=cost.spread_pips,
                gross_pnl_cash=gross_pnl,
                cost_cash=total_cost,
                net_pnl_cash=net_pnl,
                gross_r=_profit_r(intent.side, entry, exit_price, risk),
                net_r=net_pnl / risk_cash if risk_cash else 0.0,
                atr_stop_pips=float(intent.metadata.get("atr_stop_pips", 0.0)),
                min_stop_pips=float(intent.metadata.get("min_stop_pips", 0.0)),
                effective_stop_pips=float(intent.metadata.get("effective_stop_pips", 0.0)),
                floor_binding=intent.metadata.get("floor_binding") == "true",
            )
        )
    return trades


def max_drawdown(equity: Sequence[float]) -> float:
    peak = -math.inf
    worst = 0.0
    for value in equity:
        peak = max(peak, value)
        worst = min(worst, value - peak)
    return worst


def summarize(trades: Sequence[NetTrade], *, starting_balance: float, offset_seconds: float) -> dict:
    equity = [starting_balance]
    for trade in trades:
        equity.append(equity[-1] + trade.net_pnl_cash)
    wins = [t for t in trades if t.net_pnl_cash > 0]
    losses = [t for t in trades if t.net_pnl_cash < 0]
    gross_profit = sum(t.net_pnl_cash for t in wins)
    gross_loss = abs(sum(t.net_pnl_cash for t in losses))
    by_symbol: dict[str, dict[str, float | int]] = {}
    for symbol in sorted({t.symbol for t in trades}):
        subset = [t for t in trades if t.symbol == symbol]
        by_symbol[symbol] = _subset_metrics(subset)
    by_session: dict[str, dict[str, float | int]] = {}
    for session in sorted({t.session for t in trades}):
        subset = [t for t in trades if t.session == session]
        by_session[session] = _subset_metrics(subset)
    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mt5_detected_offset_hours": offset_seconds / 3600.0,
        "starting_balance": starting_balance,
        "ending_balance": equity[-1],
        "net_pnl": equity[-1] - starting_balance,
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "profit_factor_net": gross_profit / gross_loss if gross_loss else None,
        "expectancy_cash": sum(t.net_pnl_cash for t in trades) / len(trades) if trades else 0.0,
        "avg_win_cash": sum(t.net_pnl_cash for t in wins) / len(wins) if wins else 0.0,
        "avg_loss_cash": sum(t.net_pnl_cash for t in losses) / len(losses) if losses else 0.0,
        "max_drawdown_cash": max_drawdown(equity),
        "max_drawdown_pct": abs(max_drawdown(equity)) / starting_balance if starting_balance else 0.0,
        "floor_binding_rate": sum(t.floor_binding for t in trades) / len(trades) if trades else 0.0,
        "avg_cost_cash": sum(t.cost_cash for t in trades) / len(trades) if trades else 0.0,
        "avg_net_r": sum(t.net_r for t in trades) / len(trades) if trades else 0.0,
        "by_symbol": by_symbol,
        "by_session": by_session,
    }


def _subset_metrics(trades: Sequence[NetTrade]) -> dict[str, float | int]:
    wins = [t for t in trades if t.net_pnl_cash > 0]
    losses = [t for t in trades if t.net_pnl_cash < 0]
    gross_profit = sum(t.net_pnl_cash for t in wins)
    gross_loss = abs(sum(t.net_pnl_cash for t in losses))
    return {
        "trades": len(trades),
        "net_pnl": sum(t.net_pnl_cash for t in trades),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "profit_factor_net": gross_profit / gross_loss if gross_loss else None,
        "expectancy_cash": sum(t.net_pnl_cash for t in trades) / len(trades) if trades else 0.0,
        "floor_binding_rate": sum(t.floor_binding for t in trades) / len(trades) if trades else 0.0,
    }


def write_outputs(trades: Sequence[NetTrade], summary: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (out_dir / "trades.csv").open("w", encoding="utf-8", newline="") as fh:
        fieldnames = list(asdict(trades[0]).keys()) if trades else list(NetTrade.__dataclass_fields__.keys())
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(asdict(trade))
    lines = [
        "# Kate FX Session Net-of-Cost Backtest",
        "",
        f"Generated UTC: {summary['generated_at_utc']}",
        f"Trades: {summary['trades']} | Win rate: {summary['win_rate']:.1%}",
        f"Net P&L: {summary['net_pnl']:.2f} | Ending balance: {summary['ending_balance']:.2f}",
        f"Net PF: {_fmt_optional(summary['profit_factor_net'])} | Expectancy: {summary['expectancy_cash']:.2f}",
        f"Max DD: {summary['max_drawdown_cash']:.2f} ({summary['max_drawdown_pct']:.1%})",
        f"Floor binding rate: {summary['floor_binding_rate']:.1%} | Avg cost/trade: {summary['avg_cost_cash']:.2f}",
        "",
        "## By Session",
        "",
    ]
    for session, stats in summary["by_session"].items():
        lines.append(
            f"- {session}: trades {stats['trades']}, net {stats['net_pnl']:.2f}, "
            f"PF {_fmt_optional(stats['profit_factor_net'])}, expectancy {stats['expectancy_cash']:.2f}"
        )
    lines.extend(["", "## By Symbol", ""])
    for symbol, stats in summary["by_symbol"].items():
        lines.append(
            f"- {symbol}: trades {stats['trades']}, net {stats['net_pnl']:.2f}, "
            f"PF {_fmt_optional(stats['profit_factor_net'])}, floor {stats['floor_binding_rate']:.1%}"
        )
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt_optional(value: object) -> str:
    return "inf" if value is None else f"{float(value):.2f}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=float, default=2.0)
    parser.add_argument("--timeframe", default="M5", help="M5|M15|M30|H1|H4|D1 (MT5 mode)")
    parser.add_argument("--sessions", default="london,ny")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--quantity", type=float, default=0.56)
    parser.add_argument("--starting-balance", type=float, default=5000.0)
    parser.add_argument("--stop-mode", choices=["v0", "v2_step"], default="v2_step")
    parser.add_argument("--spread-pips", default="*=0.8")
    parser.add_argument("--commission-roundtrip-cash", default="*=0.0")
    parser.add_argument("--swap-cash", default="*=0.0")
    parser.add_argument("--pip-value-per-lot-cash", default="*=7.8")
    parser.add_argument("--mt5-path", default=r"C:\Program Files\MetaTrader 5 IC Markets Global\terminal64.exe")
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=Path("analysis/fx_session_net_cost_backtest"))
    args = parser.parse_args()

    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    sessions = tuple(s.strip().lower() for s in args.sessions.split(",") if s.strip())
    spread_pips = _parse_symbol_values(args.spread_pips, 0.8)
    commissions = _parse_symbol_values(args.commission_roundtrip_cash, 0.0)
    swaps = _parse_symbol_values(args.swap_cash, 0.0)
    pip_values = _parse_symbol_values(args.pip_value_per_lot_cash, 7.8)

    if args.csv:
        bars = load_csv_bars(args.csv)
        offset_seconds = 0.0
    else:
        bars, offset_seconds = load_mt5_bars(symbols, years=args.years, path=args.mt5_path, timeframe=args.timeframe)

    trades: list[NetTrade] = []
    for session in sessions:
        if session not in {"london", "ny"}:
            raise ValueError(f"unsupported session: {session}")
        for symbol in symbols:
            cost = CostModel(
                spread_pips=_symbol_value(spread_pips, symbol, 0.8),
                commission_roundtrip_cash=_symbol_value(commissions, symbol, 0.0),
                swap_cash=_symbol_value(swaps, symbol, 0.0),
                pip_value_per_lot_cash=_symbol_value(pip_values, symbol, 7.8),
            )
            trades.extend(
                replay_session(
                    session_name=session,
                    symbol=symbol,
                    candles=bars.get(symbol, []),
                    years=args.years,
                    quantity=args.quantity,
                    cost=cost,
                    stop_mode=args.stop_mode,
                )
            )

    trades.sort(key=lambda t: (t.session_date, t.entry_time_uk, t.symbol, t.session))
    summary = summarize(trades, starting_balance=args.starting_balance, offset_seconds=offset_seconds)
    write_outputs(trades, summary, args.out_dir)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
