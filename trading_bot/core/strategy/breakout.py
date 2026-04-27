"""
ATR breakout — first managed-futures strategy for Direction A.

Pattern (deterministic, rule-based, audit-trail friendly):

  ENTRY (long):
    * the just-closed candle's CLOSE > highest high of the previous
      `breakout_lookback` bars (excluding the just-closed bar itself), AND
    * close > simple moving average of the last `ma_period` closes
  STOP:   close - `atr_stop_mult` × ATR(`atr_period`)
  TARGET: close + `atr_target_mult` × ATR(`atr_period`)

Pyramiding is disabled — the strategy returns None when a position is
already open (risk engine would also reject second entries via
max_open_positions, but explicit beats implicit). Symmetric short-side
entries are NOT in this v1 — managed-futures trend-following is typically
asymmetric (long bias), and short rules require their own backtest/audit
trail. Add a `BreakoutShortStrategy` later if/when the trade thesis warrants.

Why this pattern:
  - Common managed-futures template (Winton/Man AHL/AQR family) — proven
    at scale, rule-based, regulator-friendly per CEO policy
  - Has a built-in stop on every entry — required by risk engine
  - Three tunables (lookback / MA / ATR) → small parameter space, low
    overfitting risk on the small-sample data we have
"""
from __future__ import annotations

from typing import Optional

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import TradeIntent

from .base import Strategy, StrategyContext
from .indicators import atr, highest_high, sma


class AtrBreakoutStrategy(Strategy):
    def __init__(
        self,
        *,
        breakout_lookback: int = 20,
        ma_period: int = 50,
        atr_period: int = 14,
        atr_stop_mult: float = 2.0,
        atr_target_mult: float = 3.0,
        quantity: float = 1.0,
    ) -> None:
        if breakout_lookback < 2:
            raise ValueError("breakout_lookback must be >= 2")
        if ma_period < 2:
            raise ValueError("ma_period must be >= 2")
        if atr_period < 2:
            raise ValueError("atr_period must be >= 2")
        if atr_stop_mult <= 0 or atr_target_mult <= 0:
            raise ValueError("atr multipliers must be > 0")
        if quantity <= 0:
            raise ValueError("quantity must be > 0")

        self.breakout_lookback = breakout_lookback
        self.ma_period = ma_period
        self.atr_period = atr_period
        self.atr_stop_mult = atr_stop_mult
        self.atr_target_mult = atr_target_mult
        self.quantity = quantity

    @property
    def name(self) -> str:
        return (
            f"atr_breakout(lb={self.breakout_lookback},"
            f"ma={self.ma_period},atr={self.atr_period},"
            f"sl={self.atr_stop_mult},tp={self.atr_target_mult})"
        )

    @property
    def history_window(self) -> int:
        # +1 because the breakout uses the previous N bars (excluding the
        # just-closed bar), and ATR needs N+1 bars for N true-range values.
        return max(self.breakout_lookback, self.ma_period, self.atr_period) + 1

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        if ctx.has_open_position:
            return None  # no pyramiding in v1

        history = ctx.history
        if len(history) < self.history_window:
            return None

        # ATR over the most recent atr_period bars (uses atr_period + 1 candles)
        atr_value = atr(history, self.atr_period)
        if atr_value <= 0:
            return None

        # SMA over the most recent ma_period closes
        ma_value = sma(history, self.ma_period)

        # Highest high of the previous `breakout_lookback` bars,
        # EXCLUDING the just-closed bar (which is history[-1]).
        prior_window = history[: -1]                  # all bars before the close
        breakout_high = highest_high(prior_window, self.breakout_lookback)

        c = ctx.candle  # the just-closed candle
        if c.close > breakout_high and c.close > ma_value:
            stop = c.close - self.atr_stop_mult * atr_value
            target = c.close + self.atr_target_mult * atr_value
            # intent_id must fit in DTC's ClientOrderID[32] field on the
            # wire. Use a compact deterministic format: strategy tag +
            # symbol + bar timestamp. The full strategy name (with
            # parameters) is preserved in `strategy_name` for audit.
            intent_id = (
                f"atrbo-{ctx.symbol[:10]}-"
                f"{c.timestamp.strftime('%y%m%d%H%M%S')}"
            )
            return TradeIntent(
                intent_id=intent_id,
                strategy_name=self.name,
                symbol=ctx.symbol,
                exchange=ctx.exchange,
                side=proto.BUY,
                quantity=self.quantity,
                order_type=proto.ORDER_TYPE_MARKET,
                tick_size=ctx.tick_size,
                tick_value=ctx.tick_value,
                price=c.close,
                stop_loss=stop,
                take_profit=target,
                per_contract_margin=ctx.per_contract_margin,
                reason=(
                    f"breakout: close {c.close:.2f} > prior-{self.breakout_lookback}-high "
                    f"{breakout_high:.2f} AND > SMA{self.ma_period} {ma_value:.2f}; "
                    f"ATR{self.atr_period}={atr_value:.2f}"
                ),
            )
        return None
