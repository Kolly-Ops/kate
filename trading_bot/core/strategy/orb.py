"""
ORB (Opening Range Breakout) — multi-session strategy for Direction A.

Pattern (deterministic, rule-based, audit-trail friendly):

  OPENING RANGE: high/low of bars whose UTC time falls within
    [range_start_utc, range_end_utc) for each configured session.
  ENTRY (long):
    * just-closed candle's CLOSE > opening-range high, AND
    * close > EMA(ema_period), AND
    * we are inside [range_end_utc, trade_end_utc) for this session,
    * we have NOT yet entered a trade in this session today,
    * the range width is within [min_range_points, max_range_points].
  ENTRY (short, mirror): close < range_low AND close < ema.
  STOP:   ATR(atr_period) × atr_stop_mult, distance below/above entry.
  TARGET: stop_distance × reward_risk (R:R), i.e. take_profit at
    entry ± reward_risk × stop_distance.

Multiple sessions may be configured (e.g. Asian + US). Each session has
independent state (range, traded-today flag). Sessions don't overlap in
time, so at most one session is active for any given bar.

Why this pattern:
  - Validated by Codex's VectorBT prototype (2026-05-08): MES 1-min,
    Asian + US, both-direction 2.5R returned +19.59% with -4.80% max
    drawdown over 6.5 weeks under production risk policy. See
    decisions/2026-05-09-kate-12-month-strategy-master-plan-v2.md.
  - ORB is one of the most-documented retail-futures strategies; clear
    breakout/no-trade rules; naturally low-frequency; intraday only
    (no overnight risk) which fits prop-firm rule sets.

Stateless-by-policy compromise:
  Per Kate's strategy contract (`base.py`), strategies should be
  stateless across calls. Pure ORB needs a per-session "traded today"
  flag and an in-progress opening range. We compromise by computing the
  range FROM HISTORY each call (fully deterministic) and tracking the
  "traded today" flag as instance state (small per-session-per-day
  bool — recomputable from order history on restart, but we accept the
  one-extra-trade-on-restart edge case rather than introducing a
  StateStore dependency in the strategy layer).
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from typing import Optional

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import TradeIntent

from .base import Strategy, StrategyContext
from .indicators import atr, ema


@dataclass(frozen=True)
class SessionWindow:
    """One opening-range trading session in UTC.

    Asian session example: range_start=00:00, range_end=00:30,
    trade_end=06:00. US session: 14:30 / 15:00 / 20:45.
    """
    name: str                 # e.g. "asian", "us"
    range_start: dt.time      # opening-range start (inclusive)
    range_end: dt.time        # opening-range end (exclusive); trade window starts here
    trade_end: dt.time        # trade window end (exclusive); after this, no entries

    def __post_init__(self) -> None:
        if not (self.range_start < self.range_end < self.trade_end):
            raise ValueError(
                f"SessionWindow {self.name!r}: must satisfy "
                f"range_start ({self.range_start}) < range_end ({self.range_end}) "
                f"< trade_end ({self.trade_end})"
            )


@dataclass
class _SessionState:
    """Mutable state we maintain per session."""
    last_seen_date: Optional[dt.date] = None
    traded_in_current: bool = False


class ORBStrategy(Strategy):
    """Multi-session Opening Range Breakout. See module docstring."""

    def __init__(
        self,
        *,
        sessions: list[SessionWindow],
        ema_period: int = 200,
        atr_period: int = 14,
        atr_stop_mult: float = 1.1,
        reward_risk: float = 2.5,
        min_range_points: float = 1.0,
        max_range_points: float = 25.0,
        direction: str = "both",
        quantity: float = 1.0,
    ) -> None:
        if not sessions:
            raise ValueError("at least one SessionWindow required")
        if ema_period < 2:
            raise ValueError("ema_period must be >= 2")
        if atr_period < 2:
            raise ValueError("atr_period must be >= 2")
        if atr_stop_mult <= 0 or reward_risk <= 0:
            raise ValueError("atr_stop_mult and reward_risk must be > 0")
        if min_range_points < 0 or max_range_points <= min_range_points:
            raise ValueError(
                "must satisfy 0 <= min_range_points < max_range_points"
            )
        if direction not in ("long", "short", "both"):
            raise ValueError(f"direction must be long|short|both (got {direction!r})")
        if quantity <= 0:
            raise ValueError("quantity must be > 0")

        self.sessions = list(sessions)
        self.ema_period = ema_period
        self.atr_period = atr_period
        self.atr_stop_mult = atr_stop_mult
        self.reward_risk = reward_risk
        self.min_range_points = min_range_points
        self.max_range_points = max_range_points
        self.direction = direction
        self.quantity = quantity

        # Per-session state. See module docstring for the stateless-by-policy
        # compromise note.
        self._state: dict[str, _SessionState] = {
            s.name: _SessionState() for s in sessions
        }

    @property
    def name(self) -> str:
        session_tags = ",".join(
            f"{s.name}({s.range_start.strftime('%H%M')}-{s.range_end.strftime('%H%M')})"
            for s in self.sessions
        )
        return (
            f"orb({session_tags},dir={self.direction},"
            f"R:R={self.reward_risk},ema={self.ema_period},"
            f"atr={self.atr_period},sl={self.atr_stop_mult})"
        )

    @property
    def history_window(self) -> int:
        # EMA needs `ema_period` bars to seed; ATR needs `atr_period + 1`
        # bars. Plus enough room to compute today's opening range from
        # history (max 30 bars at 1-min for a 30-min range). Safety
        # margin keeps the engine's history deque well-sized.
        return max(self.ema_period, self.atr_period) + 30

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        if ctx.has_open_position:
            return None  # no pyramiding

        history = ctx.history
        if len(history) < self.history_window:
            return None

        c = ctx.candle
        bar_time = c.timestamp.time()
        bar_date = c.timestamp.date()

        # Identify which session (if any) this bar belongs to. Sessions
        # are non-overlapping by construction (validated at __post_init__),
        # so at most one matches.
        active_session = self._session_for_time(bar_time)
        if active_session is None:
            return None

        state = self._state[active_session.name]

        # New trading day for this session — reset.
        if state.last_seen_date != bar_date:
            state.last_seen_date = bar_date
            state.traded_in_current = False

        # If we're still inside the opening-range window, don't trade —
        # range is still being built. Engine will keep calling us.
        if bar_time < active_session.range_end:
            return None

        # If we're past the trade window, session is done for the day.
        if bar_time >= active_session.trade_end:
            return None

        # If we've already taken our one trade for this session today,
        # nothing further fires.
        if state.traded_in_current:
            return None

        # Compute opening range from history (deterministic — no state).
        range_high, range_low = self._opening_range_from_history(
            history, bar_date, active_session
        )
        if math.isnan(range_high) or math.isnan(range_low):
            return None  # range bars not yet in history (engine warmup edge case)

        range_width = range_high - range_low
        if range_width < self.min_range_points or range_width > self.max_range_points:
            return None  # range too narrow (chop) or too wide (vol blowout)

        # Indicators on the just-closed bar.
        atr_value = atr(history, self.atr_period)
        if atr_value <= 0:
            return None
        ema_value = ema(history, self.ema_period)
        if ema_value <= 0:
            return None

        stop_distance = atr_value * self.atr_stop_mult
        if stop_distance <= 0:
            return None

        # Entry conditions.
        side: Optional[int] = None
        entry_reason = ""
        if (
            self.direction in ("both", "long")
            and c.close > range_high
            and c.close > ema_value
        ):
            side = proto.BUY
            stop = c.close - stop_distance
            target = c.close + self.reward_risk * stop_distance
            entry_reason = (
                f"orb-long {active_session.name}: close {c.close:.2f} > range_high "
                f"{range_high:.2f} AND > EMA{self.ema_period} {ema_value:.2f}; "
                f"ATR{self.atr_period}={atr_value:.2f}, R:R={self.reward_risk}"
            )
        elif (
            self.direction in ("both", "short")
            and c.close < range_low
            and c.close < ema_value
        ):
            side = proto.SELL
            stop = c.close + stop_distance
            target = c.close - self.reward_risk * stop_distance
            entry_reason = (
                f"orb-short {active_session.name}: close {c.close:.2f} < range_low "
                f"{range_low:.2f} AND < EMA{self.ema_period} {ema_value:.2f}; "
                f"ATR{self.atr_period}={atr_value:.2f}, R:R={self.reward_risk}"
            )
        else:
            return None

        # Mark session as having traded — single-trade-per-session rule.
        # Note: if the entry is rejected by the risk engine, this still
        # marks the session as "traded". That's intentional — we don't
        # retry within a session; one chance per ORB.
        state.traded_in_current = True

        # Compact deterministic intent_id (must fit DTC's ClientOrderID[32]).
        intent_id = (
            f"orb-{ctx.symbol[:8]}-{active_session.name[:3]}-"
            f"{c.timestamp.strftime('%y%m%d%H%M%S')}"
        )

        return TradeIntent(
            intent_id=intent_id,
            strategy_name=self.name,
            symbol=ctx.symbol,
            exchange=ctx.exchange,
            side=side,
            quantity=self.quantity,
            order_type=proto.ORDER_TYPE_MARKET,
            tick_size=ctx.tick_size,
            tick_value=ctx.tick_value,
            price=c.close,
            stop_loss=stop,
            take_profit=target,
            per_contract_margin=ctx.per_contract_margin,
            round_trip_commission=ctx.round_trip_commission,
            reason=entry_reason,
        )

    # ── Internals ─────────────────────────────────────────────────────────

    def _session_for_time(self, bar_time: dt.time) -> Optional[SessionWindow]:
        """Which session (if any) does this UTC time of day belong to?
        Returns the session whose [range_start, trade_end) interval
        contains bar_time. Sessions are non-overlapping (validated at
        construction), so at most one matches.
        """
        for s in self.sessions:
            if s.range_start <= bar_time < s.trade_end:
                return s
        return None

    def _opening_range_from_history(
        self,
        history: tuple,
        bar_date: dt.date,
        session: SessionWindow,
    ) -> tuple[float, float]:
        """Compute high/low of bars whose UTC date == `bar_date` and whose
        UTC time falls within [session.range_start, session.range_end).

        Returns (NaN, NaN) if no in-range bars are found.
        """
        range_high = math.nan
        range_low = math.nan
        # Iterate from newest to oldest — once we cross to an earlier date
        # we can stop, since history is oldest-first we walk in reverse.
        for candle in reversed(history):
            ts = candle.timestamp
            cd = ts.date()
            if cd > bar_date:
                continue  # shouldn't happen; just in case
            if cd < bar_date:
                break  # entered a previous day, no more relevant bars
            ct = ts.time()
            if ct < session.range_start or ct >= session.range_end:
                continue
            range_high = candle.high if math.isnan(range_high) else max(range_high, candle.high)
            range_low = candle.low if math.isnan(range_low) else min(range_low, candle.low)
        return range_high, range_low
