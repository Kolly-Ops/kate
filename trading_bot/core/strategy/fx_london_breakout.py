"""FX session breakout strategies.

The London strategy captures the 00:00-07:00 UK Asian range, then trades one
London-open breakout. The NY strategy uses the same mechanics on a separate
12:00-15:30 UK range and 15:30-18:00 UK trade window.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import TradeIntent

from .base import Strategy, StrategyContext
from .indicators import atr

logger = logging.getLogger(__name__)


# Per-symbol minimum stop distance in pips. Guards against the failure mode
# observed on AUDUSD 2026-05-22: ATR(14) can collapse below tradable
# microstructure width during quiet sessions, producing stops inside normal
# noise. See decisions/2026-05-22-... (codex-approved A-prime).
DEFAULT_MIN_STOP_PIPS_BY_SYMBOL: dict[str, float] = {
    "GBPUSD": 6.0,
    "EURUSD": 5.0,
    "AUDUSD": 5.0,
    "EURGBP": 4.0,
    "USDCAD": 5.0,
}
DEFAULT_MIN_STOP_PIPS_FALLBACK: float = 5.0


@dataclass(frozen=True)
class NewsEvent:
    """High-impact event timestamp that blocks entries around release time."""

    timestamp: dt.datetime
    region: str = ""
    label: str = ""


@dataclass(frozen=True)
class FXSessionConfig:
    """Local-time breakout session windows."""

    name: str
    intent_prefix: str
    range_label: str
    range_start: dt.time
    range_end: dt.time
    trade_start: dt.time
    trade_end: dt.time
    force_flat: dt.time

    def __post_init__(self) -> None:
        if not (self.range_start < self.range_end <= self.trade_start < self.trade_end):
            raise ValueError(
                "session windows must satisfy "
                "range_start < range_end <= trade_start < trade_end"
            )
        if self.force_flat != self.trade_end:
            raise ValueError("force_flat must match trade_end for current bracket-only wiring")


LONDON_SESSION = FXSessionConfig(
    name="london",
    intent_prefix="fxlon",
    range_label="Asian range",
    range_start=dt.time(0, 0),
    range_end=dt.time(7, 0),
    trade_start=dt.time(7, 0),
    trade_end=dt.time(10, 0),
    force_flat=dt.time(10, 0),
)

NY_SESSION = FXSessionConfig(
    name="ny",
    intent_prefix="fxny",
    range_label="NY reference range",
    range_start=dt.time(12, 0),
    range_end=dt.time(15, 30),
    trade_start=dt.time(15, 30),
    trade_end=dt.time(18, 0),
    force_flat=dt.time(18, 0),
)


class FXSessionBreakoutStrategy(Strategy):
    """Trade breaks of a configured FX range during its trade window."""

    def __init__(
        self,
        *,
        session: FXSessionConfig = LONDON_SESSION,
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
        intent_cooldown_minutes: int = 120,
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

        self.session = session
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
        self.intent_cooldown_minutes = int(intent_cooldown_minutes)
        self._traded_sessions: set[tuple[str, dt.date]] = set()
        self._last_exit_at_utc: dict[str, dt.datetime] = {}

    @property
    def name(self) -> str:
        return (
            f"fx_{self.session.name}_breakout("
            f"qty={self.quantity:g},rr={self.reward_risk:g},atr={self.atr_period},"
            f"atr_mult={self.atr_stop_multiplier:g})"
        )

    @property
    def history_window(self) -> int:
        return max(480, self.atr_period + 1)

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        if ctx.has_open_position:
            logger.debug("%s %s: skip - open position", self.session.intent_prefix, ctx.symbol)
            return None

        if self.intent_cooldown_minutes > 0:
            last_at = self._last_exit_at_utc.get(ctx.symbol)
            if last_at is not None:
                candle_ts = self._as_utc(ctx.candle.timestamp)
                last_at = self._as_utc(last_at)
                elapsed_min = (candle_ts - last_at).total_seconds() / 60.0
                if elapsed_min < self.intent_cooldown_minutes:
                    logger.info(
                        "%s %s: skip - post-exit cooldown "
                        "(last_exit=%s, %.1f min elapsed < %d min window)",
                        self.session.intent_prefix,
                        ctx.symbol,
                        last_at.isoformat(),
                        elapsed_min,
                        self.intent_cooldown_minutes,
                    )
                    return None

        ts_local = self._to_local(ctx.candle.timestamp)
        if not self._in_trade_window(ts_local):
            logger.debug(
                "%s %s @ %s UK: outside trade window %s-%s",
                self.session.intent_prefix,
                ctx.symbol,
                ts_local.strftime("%Y-%m-%d %H:%M"),
                self.session.trade_start.strftime("%H:%M"),
                self.session.trade_end.strftime("%H:%M"),
            )
            return None

        logger.info(
            "%s %s @ %s UK: IN trade window, evaluating (history=%d candles)",
            self.session.intent_prefix,
            ctx.symbol,
            ts_local.strftime("%H:%M"),
            len(ctx.history),
        )

        session_key = (ctx.symbol, ts_local.date())
        if session_key in self._traded_sessions:
            logger.info(
                "%s %s: skip - already traded this session",
                self.session.intent_prefix,
                ctx.symbol,
            )
            return None

        if self._in_news_blackout(ts_local):
            logger.info(
                "%s %s: skip - inside news blackout buffer",
                self.session.intent_prefix,
                ctx.symbol,
            )
            return None

        range_candles = self._session_range_candles(ctx.history, ts_local.date())
        if not range_candles:
            logger.info(
                "%s %s: skip - no %s candles found for session %s",
                self.session.intent_prefix,
                ctx.symbol,
                self.session.range_label,
                ts_local.date(),
            )
            return None
        if len(ctx.history) < self.atr_period + 1:
            logger.info(
                "%s %s: skip - insufficient history (%d < %d for ATR%d)",
                self.session.intent_prefix,
                ctx.symbol,
                len(ctx.history),
                self.atr_period + 1,
                self.atr_period,
            )
            return None

        range_high = max(c.high for c in range_candles)
        range_low = min(c.low for c in range_candles)
        range_pips = (range_high - range_low) / self.pip_size
        logger.info(
            "%s %s %s: high=%.5f low=%.5f pips=%.1f close=%.5f (n_bars=%d)",
            self.session.intent_prefix,
            ctx.symbol,
            self.session.range_label,
            range_high,
            range_low,
            range_pips,
            ctx.candle.close,
            len(range_candles),
        )
        if range_pips < self.min_range_pips or range_pips > self.max_range_pips:
            logger.info(
                "%s %s: skip - range %.1f pips outside filter [%.0f, %.0f]",
                self.session.intent_prefix,
                ctx.symbol,
                range_pips,
                self.min_range_pips,
                self.max_range_pips,
            )
            return None

        if ctx.candle.close > range_high:
            breakout_pips = (ctx.candle.close - range_high) / self.pip_size
        elif ctx.candle.close < range_low:
            breakout_pips = (range_low - ctx.candle.close) / self.pip_size
        else:
            logger.info(
                "%s %s: skip - no breakout (close %.5f inside range %.5f-%.5f)",
                self.session.intent_prefix,
                ctx.symbol,
                ctx.candle.close,
                range_low,
                range_high,
            )
            return None
        if breakout_pips < self.min_breakout_pips:
            logger.info(
                "%s %s: skip - shallow breakout (%.2f pips < %.2f min)",
                self.session.intent_prefix,
                ctx.symbol,
                breakout_pips,
                self.min_breakout_pips,
            )
            return None

        current_atr = atr(ctx.history, self.atr_period)
        if current_atr <= 0:
            logger.info(
                "%s %s: skip - ATR%d <= 0",
                self.session.intent_prefix,
                ctx.symbol,
                self.atr_period,
            )
            return None

        atr_stop_distance = current_atr * self.atr_stop_multiplier
        atr_stop_pips = atr_stop_distance / self.pip_size
        min_stop_pips = self._min_stop_pips_for(ctx.symbol)
        min_stop_distance = min_stop_pips * self.pip_size
        stop_distance = max(atr_stop_distance, min_stop_distance)
        floor_binding = min_stop_distance > atr_stop_distance
        if floor_binding:
            logger.info(
                "%s %s: min-stop floor binding - atr_stop=%.2f pips < "
                "min=%.2f pips; stop_distance=%.2f pips",
                self.session.intent_prefix,
                ctx.symbol,
                atr_stop_pips,
                min_stop_pips,
                stop_distance / self.pip_size,
            )

        if ctx.candle.close > range_high:
            side = proto.BUY
            stop_loss = max(range_low, ctx.candle.close - stop_distance)
            risk = ctx.candle.close - stop_loss
            take_profit = ctx.candle.close + (risk * self.reward_risk)
            logger.info(
                "%s %s: BREAKOUT HIGH - BUY @ %.5f stop=%.5f tp=%.5f "
                "(range %.5f-%.5f, breakout=%.2f pips)",
                self.session.intent_prefix,
                ctx.symbol,
                ctx.candle.close,
                stop_loss,
                take_profit,
                range_low,
                range_high,
                breakout_pips,
            )
        else:
            side = proto.SELL
            stop_loss = min(range_high, ctx.candle.close + stop_distance)
            risk = stop_loss - ctx.candle.close
            take_profit = ctx.candle.close - (risk * self.reward_risk)
            logger.info(
                "%s %s: BREAKOUT LOW - SELL @ %.5f stop=%.5f tp=%.5f "
                "(range %.5f-%.5f, breakout=%.2f pips)",
                self.session.intent_prefix,
                ctx.symbol,
                ctx.candle.close,
                stop_loss,
                take_profit,
                range_low,
                range_high,
                breakout_pips,
            )

        if risk <= 0:
            logger.info(
                "%s %s: skip - risk computed <= 0",
                self.session.intent_prefix,
                ctx.symbol,
            )
            return None

        ymdhm = ts_local.strftime("%y%m%d%H%M")
        effective_stop_pips = abs(ctx.candle.close - stop_loss) / self.pip_size
        signal_ts_utc = self._as_utc(ctx.candle.timestamp)
        return TradeIntent(
            intent_id=f"{self.session.intent_prefix}-{ctx.symbol}-{ymdhm}"[:32],
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
                f"{self.session.name.upper()} breakout "
                f"{'high' if side == proto.BUY else 'low'}; "
                f"{self.session.range_label} {range_low:.5f}-{range_high:.5f}"
            ),
            metadata={
                "asian_range_low": f"{range_low:.5f}",
                "asian_range_high": f"{range_high:.5f}",
                "asian_range_pips": f"{range_pips:.1f}",
                "session": self.session.name,
                "range_label": self.session.range_label,
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
        """Engine-driven session marking after broker accepts entry."""
        session_date = self._to_local(self._as_utc(timestamp_utc)).date()
        self._traded_sessions.add((symbol, session_date))

    def on_position_closed(self, symbol: str, timestamp_utc: dt.datetime) -> None:
        """Start cooldown from broker-observed exit time, not entry time."""
        timestamp_utc = self._as_utc(timestamp_utc)
        self._last_exit_at_utc[symbol] = timestamp_utc
        logger.info(
            "%s %s: post-exit cooldown stamped at %s for %d minutes",
            self.session.intent_prefix,
            symbol,
            timestamp_utc.isoformat(),
            self.intent_cooldown_minutes,
        )

    def _min_stop_pips_for(self, symbol: str) -> float:
        if symbol in self.min_stop_pips_by_symbol:
            return self.min_stop_pips_by_symbol[symbol]
        if self.fail_on_unknown_symbol:
            raise ValueError(
                f"fx_{self.session.name}_breakout: no min_stop_pips configured for {symbol!r}; "
                f"add it to min_stop_pips_by_symbol or set "
                f"fail_on_unknown_symbol=False to use the fallback "
                f"({self.min_stop_pips_fallback} pips)"
            )
        logger.warning(
            "%s %s: no min_stop_pips configured - using fallback %.1f pips. "
            "Configure min_stop_pips_by_symbol for production.",
            self.session.intent_prefix,
            symbol,
            self.min_stop_pips_fallback,
        )
        return self.min_stop_pips_fallback

    def _to_local(self, timestamp: dt.datetime) -> dt.datetime:
        return self._as_utc(timestamp).astimezone(self.timezone)

    @staticmethod
    def _as_utc(timestamp: dt.datetime) -> dt.datetime:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=dt.timezone.utc)
        return timestamp.astimezone(dt.timezone.utc)

    def _session_range_candles(
        self,
        history: Sequence[Candle],
        session_date: dt.date,
    ) -> tuple[Candle, ...]:
        return tuple(
            candle
            for candle in history
            if self._is_session_range_bar(self._to_local(candle.timestamp), session_date)
        )

    def _asian_range_candles(
        self,
        history: Sequence[Candle],
        session_date: dt.date,
    ) -> tuple[Candle, ...]:
        """Backward-compatible alias used by older London analysis code."""
        return self._session_range_candles(history, session_date)

    def _is_session_range_bar(self, ts_local: dt.datetime, session_date: dt.date) -> bool:
        return (
            ts_local.date() == session_date
            and self.session.range_start <= ts_local.time() < self.session.range_end
        )

    def _in_trade_window(self, ts_local: dt.datetime) -> bool:
        return self.session.trade_start <= ts_local.time() < self.session.trade_end

    def _in_news_blackout(self, ts_local: dt.datetime) -> bool:
        for event in self.news_events:
            event_ts = event.timestamp if isinstance(event, NewsEvent) else event
            event_local = self._to_local(event_ts)
            if abs(ts_local - event_local) <= self.news_buffer:
                return True
        return False


class FXLondonBreakoutStrategy(FXSessionBreakoutStrategy):
    """London-open FX breakout using the 00:00-07:00 UK Asian range."""

    def __init__(self, **kwargs) -> None:
        super().__init__(session=LONDON_SESSION, **kwargs)


class FXNYBreakoutStrategy(FXSessionBreakoutStrategy):
    """NY-session FX breakout using the 12:00-15:30 UK reference range."""

    def __init__(self, **kwargs) -> None:
        super().__init__(session=NY_SESSION, **kwargs)
