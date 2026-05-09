"""
Session ORB strategy for Kate Phase 1.

This module keeps the historical AtrBreakoutStrategy public name so the
supervisor and engine composition stay stable, but the signal logic is now
the validated Asian+US opening-range breakout candidate:

  * Asian session: 00:00-00:30 UTC range, entries until 06:00 UTC
  * US session:    14:30-15:00 UTC range, entries until 20:45 UTC
  * both directions
  * EMA200 trend filter
  * ATR14 x 1.1 stop distance
  * 2.5R target

The strategy emits market-entry intents only. The risk engine remains
authoritative for account drawdown, per-trade risk, margin, and open-position
limits.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import time
from typing import Optional

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import TradeIntent

from .base import Strategy, StrategyContext
from .indicators import atr


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionWindow:
    name: str
    range_start: time
    range_end: time
    trade_end: time

    def includes_range(self, t: time) -> bool:
        return self.range_start <= t < self.range_end

    def includes_trade_window(self, t: time) -> bool:
        return self.range_end <= t < self.trade_end


DEFAULT_ORB_SESSIONS: tuple[SessionWindow, ...] = (
    SessionWindow(
        name="asian",
        range_start=time(0, 0),
        range_end=time(0, 30),
        trade_end=time(6, 0),
    ),
    SessionWindow(
        name="us",
        range_start=time(14, 30),
        range_end=time(15, 0),
        trade_end=time(20, 45),
    ),
)


class ORBStrategy(Strategy):
    """Validated multi-session opening-range breakout strategy."""

    def __init__(
        self,
        *,
        sessions: tuple[SessionWindow, ...] | list[SessionWindow] = DEFAULT_ORB_SESSIONS,
        ema_period: int = 200,
        atr_period: int = 14,
        atr_stop_mult: float = 1.1,
        reward_risk: float = 2.5,
        direction: str = "both",
        quantity: float = 1.0,
        min_range_points: float = 1.0,
        max_range_points: float = 25.0,
    ) -> None:
        if ema_period < 2:
            raise ValueError("ema_period must be >= 2")
        if atr_period < 2:
            raise ValueError("atr_period must be >= 2")
        if atr_stop_mult <= 0 or reward_risk <= 0:
            raise ValueError("atr_stop_mult and reward_risk must be > 0")
        if direction not in {"both", "long", "short"}:
            raise ValueError("direction must be one of: both, long, short")
        if quantity <= 0:
            raise ValueError("quantity must be > 0")
        if min_range_points <= 0:
            raise ValueError("min_range_points must be > 0")
        if max_range_points <= min_range_points:
            raise ValueError("max_range_points must be > min_range_points")
        if not sessions:
            raise ValueError("at least one ORB session is required")

        self.ema_period = ema_period
        self.atr_period = atr_period
        self.atr_stop_mult = atr_stop_mult
        self.reward_risk = reward_risk
        self.direction = direction
        self.quantity = quantity
        self.min_range_points = min_range_points
        self.max_range_points = max_range_points
        self.sessions = tuple(sessions)

        self._traded_session_keys: set[str] = set()
        self._logged_eval_keys: set[str] = set()

    @property
    def name(self) -> str:
        session_names = "+".join(s.name for s in self.sessions)
        return (
            f"orb_session({session_names},rr={self.reward_risk},"
            f"ema={self.ema_period},atr={self.atr_period}x{self.atr_stop_mult})"
        )

    @property
    def history_window(self) -> int:
        return max(self.ema_period, self.atr_period + 1)

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        if ctx.has_open_position:
            return None

        history = ctx.history
        if len(history) < self.history_window:
            return None

        c = ctx.candle
        session = self._active_session(c)
        if session is None:
            return None

        session_key = f"{session.name}:{c.timestamp.date().isoformat()}"
        if session_key in self._traded_session_keys:
            return None

        range_candles = self._range_candles(history, session, c)
        if not range_candles:
            return None

        range_high = max(bar.high for bar in range_candles)
        range_low = min(bar.low for bar in range_candles)
        range_width = range_high - range_low
        if range_width < self.min_range_points or range_width > self.max_range_points:
            self._log_first_eval(
                session_key, c, range_high, range_low,
                f"range_width {range_width:.2f} outside "
                f"{self.min_range_points:.2f}-{self.max_range_points:.2f}",
            )
            return None

        atr_value = atr(history, self.atr_period)
        ema_value = self._ema(history, self.ema_period)
        if atr_value <= 0 or ema_value <= 0:
            return None

        side_name: str | None = None
        side: int | None = None
        stop_loss: float
        take_profit: float
        stop_distance = atr_value * self.atr_stop_mult

        if self.direction in {"both", "long"} and c.close > range_high and c.close > ema_value:
            side_name = "long"
            side = proto.BUY
            stop_loss = c.close - stop_distance
            take_profit = c.close + (self.reward_risk * stop_distance)
        elif self.direction in {"both", "short"} and c.close < range_low and c.close < ema_value:
            side_name = "short"
            side = proto.SELL
            stop_loss = c.close + stop_distance
            take_profit = c.close - (self.reward_risk * stop_distance)
        else:
            self._log_first_eval(
                session_key, c, range_high, range_low,
                f"no breakout/filter pass; close={c.close:.2f} ema{self.ema_period}={ema_value:.2f}",
            )
            return None

        self._traded_session_keys.add(session_key)
        intent_id = f"orb-{session.name[:2]}-{ctx.symbol[:8]}-{c.timestamp.strftime('%y%m%d%H%M')}"
        reason = (
            f"ORB {session.name} {side_name}: close {c.close:.2f}, "
            f"range {range_low:.2f}-{range_high:.2f}, "
            f"EMA{self.ema_period}={ema_value:.2f}, "
            f"ATR{self.atr_period}={atr_value:.2f}, "
            f"stop={stop_loss:.2f}, target={take_profit:.2f}"
        )
        LOGGER.info("strategy eval: %s -> intent %s", reason, intent_id)

        return TradeIntent(
            intent_id=intent_id[:32],
            strategy_name=self.name,
            symbol=ctx.symbol,
            exchange=ctx.exchange,
            side=side,
            quantity=self.quantity,
            order_type=proto.ORDER_TYPE_MARKET,
            tick_size=ctx.tick_size,
            tick_value=ctx.tick_value,
            price=c.close,
            stop_loss=stop_loss,
            take_profit=take_profit,
            per_contract_margin=ctx.per_contract_margin,
            round_trip_commission=ctx.round_trip_commission,
            reason=reason,
            metadata={
                "strategy": "orb_session",
                "session": session.name,
                "range_high": f"{range_high:.8f}",
                "range_low": f"{range_low:.8f}",
                "ema": f"{ema_value:.8f}",
                "atr": f"{atr_value:.8f}",
                "reward_risk": f"{self.reward_risk:.4f}",
            },
        )

    def _active_session(self, candle: Candle) -> SessionWindow | None:
        t = candle.timestamp.time()
        for session in self.sessions:
            if session.includes_trade_window(t):
                return session
        return None

    def _range_candles(
        self,
        history: tuple[Candle, ...],
        session: SessionWindow,
        candle: Candle,
    ) -> list[Candle]:
        session_date = candle.timestamp.date()
        return [
            bar
            for bar in history
            if bar.timestamp.date() == session_date
            and session.includes_range(bar.timestamp.time())
        ]

    @staticmethod
    def _ema(history: tuple[Candle, ...], period: int) -> float:
        if len(history) < period:
            return 0.0
        alpha = 2.0 / (period + 1.0)
        value = history[-period].close
        for bar in history[-period + 1:]:
            value = (bar.close * alpha) + (value * (1.0 - alpha))
        return value

    def _log_first_eval(
        self,
        session_key: str,
        candle: Candle,
        range_high: float,
        range_low: float,
        outcome: str,
    ) -> None:
        if session_key in self._logged_eval_keys:
            return
        self._logged_eval_keys.add(session_key)
        LOGGER.info(
            "strategy eval: %s at %s range %.2f-%.2f -> %s",
            session_key,
            candle.timestamp.isoformat(sep=" "),
            range_low,
            range_high,
            outcome,
        )


class AtrBreakoutStrategy(ORBStrategy):
    """Backward-compatible name kept for existing imports and rollback flags.

    The runtime default now uses ORBStrategy directly. If an older caller still
    instantiates AtrBreakoutStrategy, map the old parameters onto the ORB
    implementation rather than reviving the retired ATR breakout logic.
    """

    def __init__(
        self,
        *,
        breakout_lookback: int = 20,
        ma_period: int = 200,
        atr_period: int = 14,
        atr_stop_mult: float = 1.1,
        atr_target_mult: float = 2.5,
        quantity: float = 1.0,
        min_range_points: float = 1.0,
        max_range_points: float = 25.0,
        sessions: tuple[SessionWindow, ...] | list[SessionWindow] = DEFAULT_ORB_SESSIONS,
    ) -> None:
        if breakout_lookback < 2:
            raise ValueError("breakout_lookback must be >= 2")
        super().__init__(
            sessions=sessions,
            ema_period=ma_period,
            atr_period=atr_period,
            atr_stop_mult=atr_stop_mult,
            reward_risk=atr_target_mult,
            direction="both",
            quantity=quantity,
            min_range_points=min_range_points,
            max_range_points=max_range_points,
        )
        self.breakout_lookback = breakout_lookback
