"""FX London breakout strategy.

Captures the UK Asian session range, then trades one London open breakout.
Designed for the MT5 demo path first, with GBPUSD as the primary symbol.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import TradeIntent

from .base import Strategy, StrategyContext
from .indicators import atr


# Per-symbol minimum stop distance in pips. Guards against the failure mode
# observed on AUDUSD 2026-05-22: ATR(14) can collapse below tradable
# microstructure width during quiet sessions, producing stops inside normal
# noise. See decisions/2026-05-22-... (codex-approved A-prime).
DEFAULT_MIN_STOP_PIPS_BY_SYMBOL: dict[str, float] = {
    "GBPUSD": 6.0,
    "EURUSD": 5.0,
    "AUDUSD": 5.0,
    "EURGBP": 4.0,
}
DEFAULT_MIN_STOP_PIPS_FALLBACK: float = 5.0


@dataclass(frozen=True)
class NewsEvent:
    """High-impact event timestamp that blocks entries around release time."""

    timestamp: dt.datetime
    region: str = ""
    label: str = ""


class FXLondonBreakoutStrategy(Strategy):
    """Trade breaks of the 00:00-07:00 UK Asian range during 07:00-10:00 UK."""

    def __init__(
        self,
        *,
        quantity: float = 0.01,
        reward_risk: float = 2.0,
        atr_period: int = 14,
        atr_stop_multiplier: float = 1.0,
        pip_size: float = 0.0001,
        min_range_pips: float = 5.0,
        max_range_pips: float = 120.0,
        min_breakout_pips: float = 0.0,
        min_stop_pips_by_symbol: Optional[dict[str, float]] = None,
        min_stop_pips_fallback: float = DEFAULT_MIN_STOP_PIPS_FALLBACK,
        fail_on_unknown_symbol: bool = False,
        timezone: str = "Europe/London",
        news_events: Sequence[NewsEvent | dt.datetime] = (),
        news_buffer_minutes: int = 2,
        intent_cooldown_minutes: int = 60,
    ) -> None:
        if quantity <= 0:
            raise ValueError("quantity must be > 0")
        if reward_risk <= 0:
            raise ValueError("reward_risk must be > 0")
        if atr_period <= 0:
            raise ValueError("atr_period must be > 0")
        if atr_stop_multiplier <= 0:
            raise ValueError("atr_stop_multiplier must be > 0")
        if pip_size <= 0:
            raise ValueError("pip_size must be > 0")
        if min_range_pips < 0:
            raise ValueError("min_range_pips must be >= 0")
        if max_range_pips <= 0:
            raise ValueError("max_range_pips must be > 0")
        if min_range_pips > max_range_pips:
            raise ValueError("min_range_pips must be <= max_range_pips")
        if min_breakout_pips < 0:
            raise ValueError("min_breakout_pips must be >= 0")
        if min_stop_pips_fallback < 0:
            raise ValueError("min_stop_pips_fallback must be >= 0")
        if intent_cooldown_minutes < 0:
            raise ValueError("intent_cooldown_minutes must be >= 0")

        self.quantity = quantity
        self.reward_risk = reward_risk
        self.atr_period = atr_period
        self.atr_stop_multiplier = atr_stop_multiplier
        self.pip_size = pip_size
        self.min_range_pips = min_range_pips
        self.max_range_pips = max_range_pips
        self.min_breakout_pips = min_breakout_pips
        self.min_stop_pips_by_symbol = (
            dict(min_stop_pips_by_symbol)
            if min_stop_pips_by_symbol is not None
            else dict(DEFAULT_MIN_STOP_PIPS_BY_SYMBOL)
        )
        self.min_stop_pips_fallback = min_stop_pips_fallback
        self.fail_on_unknown_symbol = fail_on_unknown_symbol
        self.timezone = ZoneInfo(timezone)
        self.news_events = tuple(news_events)
        self.news_buffer = dt.timedelta(minutes=news_buffer_minutes)
        self._traded_sessions: set[tuple[str, dt.date]] = set()
        # 2026-06-02 INTENT COOLDOWN (CEO directive): per-symbol last-intent
        # timestamp. Belt-and-braces gate that doesn't rely on FILL event
        # propagation to mark_session_traded — the canonical session marker
        # is failing silently (see retrospective 2026-06-02).
        self.intent_cooldown_minutes = int(intent_cooldown_minutes)
        self._last_intent_at_utc: dict[str, dt.datetime] = {}

    @property
    def name(self) -> str:
        return (
            "fx_london_breakout("
            f"qty={self.quantity:g},rr={self.reward_risk:g},atr={self.atr_period},"
            f"atr_mult={self.atr_stop_multiplier:g})"
        )

    @property
    def history_window(self) -> int:
        return max(480, self.atr_period + 1)

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        if ctx.has_open_position:
            logger.debug("fxlon %s: skip — open position", ctx.symbol)
            return None
        # 2026-06-02 INTENT COOLDOWN: skip if we emitted an intent on this
        # symbol within the configured cooldown window. Uses candle.timestamp
        # (UTC) so backtests and live runs share a clock.
        if self.intent_cooldown_minutes > 0:
            last_at = self._last_intent_at_utc.get(ctx.symbol)
            if last_at is not None:
                candle_ts = ctx.candle.timestamp
                if candle_ts.tzinfo is None:
                    candle_ts = candle_ts.replace(tzinfo=dt.timezone.utc)
                if last_at.tzinfo is None:
                    last_at = last_at.replace(tzinfo=dt.timezone.utc)
                elapsed_min = (candle_ts - last_at).total_seconds() / 60.0
                if elapsed_min < self.intent_cooldown_minutes:
                    logger.info(
                        "fxlon %s: skip - cooldown (%.1f min elapsed < %d min window)",
                        ctx.symbol, elapsed_min, self.intent_cooldown_minutes,
                    )
                    return None

        ts_uk = self._to_local(ctx.candle.timestamp)
        if not self._in_trade_window(ts_uk):
            logger.debug(
                "fxlon %s @ %s UK: outside trade window 07:00-10:00",
                ctx.symbol, ts_uk.strftime("%Y-%m-%d %H:%M"),
            )
            return None

        # Inside the trade window — promote to INFO so we can see the
        # strategy is actually being evaluated each minute.
        logger.info(
            "fxlon %s @ %s UK: IN trade window, evaluating (history=%d candles)",
            ctx.symbol, ts_uk.strftime("%H:%M"), len(ctx.history),
        )

        session_key = (ctx.symbol, ts_uk.date())
        if session_key in self._traded_sessions:
            logger.info("fxlon %s: skip — already traded this session", ctx.symbol)
            return None

        if self._in_news_blackout(ts_uk):
            logger.info("fxlon %s: skip — inside news blackout buffer", ctx.symbol)
            return None

        range_candles = self._asian_range_candles(ctx.history, ts_uk.date())
        if not range_candles:
            logger.info(
                "fxlon %s: skip — no Asian-range candles found for session %s",
                ctx.symbol, ts_uk.date(),
            )
            return None
        if len(ctx.history) < self.atr_period + 1:
            logger.info(
                "fxlon %s: skip — insufficient history (%d < %d for ATR%d)",
                ctx.symbol, len(ctx.history), self.atr_period + 1, self.atr_period,
            )
            return None

        range_high = max(c.high for c in range_candles)
        range_low = min(c.low for c in range_candles)
        range_pips = (range_high - range_low) / self.pip_size
        logger.info(
            "fxlon %s Asian range: high=%.5f low=%.5f pips=%.1f close=%.5f (n_bars=%d)",
            ctx.symbol, range_high, range_low, range_pips, ctx.candle.close, len(range_candles),
        )
        if range_pips < self.min_range_pips or range_pips > self.max_range_pips:
            logger.info(
                "fxlon %s: skip — range %.1f pips outside filter [%.0f, %.0f]",
                ctx.symbol, range_pips, self.min_range_pips, self.max_range_pips,
            )
            return None

        # Sprint 2 (2026-05-30): min_breakout_pips guard — runs BEFORE
        # ATR/min-stop-floor calculation per Codex REVIEW-RESPONSE
        # 2026-05-30 (item 1 concern #3). Reasons:
        #   - skipped trades don't need stop sizing computed
        #   - avoids fail_on_unknown_symbol floor-lookup for trades that
        #     would be skipped anyway
        #   - shallow-breakout log isn't preceded by an irrelevant
        #     min-stop-floor line
        # Live evidence Fri 2026-05-29: AUDUSD fired on 0.5-pip break of
        # a 13.8-pip range, stopped out in 49 seconds when price retraced
        # to range-mid. False-breakout / liquidity-hunt pattern is the
        # documented failure mode; widening the floor doesn't help
        # because the stop still falls inside the prior range. See
        # proposals/2026-05-29-claude-audusd-breakout-confirmation-not-floor-tuning.md
        if ctx.candle.close > range_high:
            breakout_pips = (ctx.candle.close - range_high) / self.pip_size
        elif ctx.candle.close < range_low:
            breakout_pips = (range_low - ctx.candle.close) / self.pip_size
        else:
            logger.info(
                "fxlon %s: skip — no breakout (close %.5f inside range %.5f-%.5f)",
                ctx.symbol, ctx.candle.close, range_low, range_high,
            )
            return None
        if breakout_pips < self.min_breakout_pips:
            logger.info(
                "fxlon %s: skip — shallow breakout (%.2f pips < %.2f min)",
                ctx.symbol, breakout_pips, self.min_breakout_pips,
            )
            return None

        current_atr = atr(ctx.history, self.atr_period)
        if current_atr <= 0:
            logger.info("fxlon %s: skip — ATR%d <= 0", ctx.symbol, self.atr_period)
            return None

        atr_stop_distance = current_atr * self.atr_stop_multiplier
        atr_stop_pips = atr_stop_distance / self.pip_size
        min_stop_pips = self._min_stop_pips_for(ctx.symbol)
        min_stop_distance = min_stop_pips * self.pip_size
        stop_distance = max(atr_stop_distance, min_stop_distance)
        floor_binding = min_stop_distance > atr_stop_distance
        if floor_binding:
            logger.info(
                "fxlon %s: min-stop floor binding — atr_stop=%.2f pips < min=%.2f pips; "
                "stop_distance=%.2f pips",
                ctx.symbol, atr_stop_pips, min_stop_pips, stop_distance / self.pip_size,
            )

        side: int
        stop_loss: float
        if ctx.candle.close > range_high:
            side = proto.BUY
            stop_loss = max(range_low, ctx.candle.close - stop_distance)
            risk = ctx.candle.close - stop_loss
            take_profit = ctx.candle.close + (risk * self.reward_risk)
            logger.info(
                "fxlon %s: BREAKOUT HIGH — BUY @ %.5f stop=%.5f tp=%.5f (range %.5f-%.5f, breakout=%.2f pips)",
                ctx.symbol, ctx.candle.close, stop_loss, take_profit, range_low, range_high, breakout_pips,
            )
        else:
            side = proto.SELL
            stop_loss = min(range_high, ctx.candle.close + stop_distance)
            risk = stop_loss - ctx.candle.close
            take_profit = ctx.candle.close - (risk * self.reward_risk)
            logger.info(
                "fxlon %s: BREAKOUT LOW — SELL @ %.5f stop=%.5f tp=%.5f (range %.5f-%.5f, breakout=%.2f pips)",
                ctx.symbol, ctx.candle.close, stop_loss, take_profit, range_low, range_high, breakout_pips,
            )

        if risk <= 0:
            logger.info("fxlon %s: skip — risk computed <= 0", ctx.symbol)
            return None

        # Sprint 2 #44 (2026-05-30): session-marking moved from intent-
        # emission to engine callback via mark_session_traded(). The
        # engine populates _entry_intents on broker submission and calls
        # mark_session_traded() on FILL/PARTIAL_FILL. This fixes the
        # risk-reject-burns-the-slot issue while keeping fast-fill-fast-
        # close protection (the FILL event arrives before the next
        # strategy evaluation, marking the session before re-fire is
        # possible).
        # NOTE: do NOT add self._traded_sessions.add(session_key) here —
        # that's the pre-#44 intent-emission marking. Engine owns it now.
        ymdhm = ts_uk.strftime("%y%m%d%H%M")
        effective_stop_pips = abs(ctx.candle.close - stop_loss) / self.pip_size
        # Sprint 2 #44 (2026-05-30): carry the signal candle's UTC timestamp
        # on the intent so the engine can mark the correct session even if
        # the fill event is delayed/replayed/processed across a UK boundary.
        # ctx.candle.timestamp is normally tz-aware; convert to UTC.
        signal_ts = ctx.candle.timestamp
        if signal_ts.tzinfo is None:
            signal_ts_utc = signal_ts.replace(tzinfo=dt.timezone.utc)
        else:
            signal_ts_utc = signal_ts.astimezone(dt.timezone.utc)
        # 2026-06-02 INTENT COOLDOWN: stamp the symbol BEFORE returning so the
        # next candle close in the cooldown window is gated. Even if the
        # intent is risk-rejected, the cooldown still applies (deliberate
        # - don't churn on a symbol that risk just rejected).
        self._last_intent_at_utc[ctx.symbol] = (
            ctx.candle.timestamp
            if ctx.candle.timestamp.tzinfo is not None
            else ctx.candle.timestamp.replace(tzinfo=dt.timezone.utc)
        )
        return TradeIntent(
            intent_id=f"fxlon-{ctx.symbol}-{ymdhm}"[:32],
            strategy_name=self.name,
            symbol=ctx.symbol,
            exchange=ctx.exchange,
            side=side,
            quantity=self.quantity,
            order_type=proto.ORDER_TYPE_MARKET,
            tick_size=ctx.tick_size,
            tick_value=ctx.tick_value,
            price=round(ctx.candle.close, 5),
            stop_loss=round(stop_loss, 5),
            take_profit=round(take_profit, 5),
            signal_timestamp_utc=signal_ts_utc,
            per_contract_margin=ctx.per_contract_margin,
            round_trip_commission=ctx.round_trip_commission,
            reason=(
                "London breakout "
                f"{'high' if side == proto.BUY else 'low'}; "
                f"Asian range {range_low:.5f}-{range_high:.5f}"
            ),
            metadata={
                "asian_range_low": f"{range_low:.5f}",
                "asian_range_high": f"{range_high:.5f}",
                "asian_range_pips": f"{range_pips:.1f}",
                "breakout_pips": f"{breakout_pips:.2f}",
                "min_breakout_pips": f"{self.min_breakout_pips:.2f}",
                "atr": f"{current_atr:.5f}",
                "atr_stop_pips": f"{atr_stop_pips:.2f}",
                "min_stop_pips": f"{min_stop_pips:.2f}",
                "effective_stop_pips": f"{effective_stop_pips:.2f}",
                "floor_binding": "true" if floor_binding else "false",
                "timezone": str(self.timezone),
            },
        )

    def mark_session_traded(self, symbol: str, timestamp_utc: dt.datetime) -> None:
        """Sprint 2 #44 (2026-05-30): engine-driven session marking.

        Called by the engine when an entry order for this strategy is
        accepted at the broker (ORDER_FILLED or ORDER_PARTIAL_FILL).
        Converts the UTC timestamp to UK local date (matching the
        on_candle_close session-key convention at line 137).

        Idempotent — set membership prevents double-marking. Risk-rejected
        intents never reach this callback (they're filtered before broker
        submission), so the session stays un-marked and the strategy can
        re-attempt on the next candle within the trade window.
        """
        # Ensure timestamp is UTC-aware before converting; defensive only
        if timestamp_utc.tzinfo is None:
            timestamp_utc = timestamp_utc.replace(tzinfo=dt.timezone.utc)
        session_date = self._to_local(timestamp_utc).date()
        self._traded_sessions.add((symbol, session_date))

    def _min_stop_pips_for(self, symbol: str) -> float:
        if symbol in self.min_stop_pips_by_symbol:
            return self.min_stop_pips_by_symbol[symbol]
        if self.fail_on_unknown_symbol:
            raise ValueError(
                f"fx_london_breakout: no min_stop_pips configured for {symbol!r}; "
                f"add it to min_stop_pips_by_symbol or set "
                f"fail_on_unknown_symbol=False to use the fallback "
                f"({self.min_stop_pips_fallback} pips)"
            )
        logger.warning(
            "fxlon %s: no min_stop_pips configured — using fallback %.1f pips. "
            "Configure min_stop_pips_by_symbol for production.",
            symbol, self.min_stop_pips_fallback,
        )
        return self.min_stop_pips_fallback

    def _to_local(self, timestamp: dt.datetime) -> dt.datetime:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        return timestamp.astimezone(self.timezone)

    def _asian_range_candles(self, history: Sequence[Candle], session_date: dt.date) -> tuple[Candle, ...]:
        return tuple(
            candle
            for candle in history
            if self._is_asian_range_bar(self._to_local(candle.timestamp), session_date)
        )

    @staticmethod
    def _is_asian_range_bar(ts_uk: dt.datetime, session_date: dt.date) -> bool:
        return ts_uk.date() == session_date and dt.time(0, 0) <= ts_uk.time() < dt.time(7, 0)

    @staticmethod
    def _in_trade_window(ts_uk: dt.datetime) -> bool:
        return dt.time(7, 0) <= ts_uk.time() < dt.time(10, 0)

    def _in_news_blackout(self, ts_uk: dt.datetime) -> bool:
        for event in self.news_events:
            event_ts = event.timestamp if isinstance(event, NewsEvent) else event
            event_uk = self._to_local(event_ts)
            if abs(ts_uk - event_uk) <= self.news_buffer:
                return True
        return False
