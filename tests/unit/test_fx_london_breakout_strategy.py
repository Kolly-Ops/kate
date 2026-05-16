import datetime as dt

import pytest

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import FXLondonBreakoutStrategy, NewsEvent, StrategyContext


UK = dt.timezone(dt.timedelta(hours=1))


def candle(ts: dt.datetime, close: float, high: float | None = None, low: float | None = None) -> Candle:
    high = close + 0.0003 if high is None else high
    low = close - 0.0003 if low is None else low
    return Candle(timestamp=ts, open=close, high=high, low=low, close=close, volume=100)


def ctx(candles: tuple[Candle, ...], *, has_open_position: bool = False) -> StrategyContext:
    return StrategyContext(
        symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        candle=candles[-1],
        history=candles,
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=has_open_position,
    )


def asian_range(day: dt.date) -> list[Candle]:
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK)
    for minute in range(7 * 60):
        ts = start + dt.timedelta(minutes=minute)
        close = 1.2500 + ((minute % 20) * 0.00001)
        candles.append(candle(ts, close, high=1.2520, low=1.2480))
    return candles


def warmup_before(day: dt.date) -> list[Candle]:
    start = dt.datetime.combine(day, dt.time(23, 30), tzinfo=UK) - dt.timedelta(days=1)
    return [candle(start + dt.timedelta(minutes=i), 1.2500) for i in range(30)]


def test_long_breakout_uses_tighter_atr_stop_and_fractional_lot() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530, high=1.2532, low=1.2527)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(quantity=0.01, reward_risk=2.0, atr_period=14)

    intent = strategy.on_candle_close(ctx(history))

    assert intent is not None
    assert intent.side == proto.BUY
    assert intent.quantity == pytest.approx(0.01)
    assert intent.order_type == proto.ORDER_TYPE_MARKET
    assert intent.stop_loss == pytest.approx(1.24907)
    assert intent.take_profit == pytest.approx(1.26086)
    assert intent.metadata["asian_range_high"] == "1.25200"


def test_short_breakout_uses_tighter_atr_stop() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 15, tzinfo=UK), 1.2475, high=1.2478, low=1.2473)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(quantity=0.01, reward_risk=1.5, atr_period=14)

    intent = strategy.on_candle_close(ctx(history))

    assert intent is not None
    assert intent.side == proto.SELL
    assert intent.stop_loss == pytest.approx(1.25142)
    assert intent.take_profit == pytest.approx(1.24162)


def test_no_entry_outside_london_trade_window() -> None:
    day = dt.date(2026, 5, 13)
    late_breakout = candle(dt.datetime(2026, 5, 13, 10, 0, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + asian_range(day) + [late_breakout])

    assert FXLondonBreakoutStrategy().on_candle_close(ctx(history)) is None


def test_news_event_blocks_two_minutes_each_side() -> None:
    day = dt.date(2026, 5, 13)
    event = NewsEvent(dt.datetime(2026, 5, 13, 7, 6, tzinfo=UK), region="UK", label="CPI")
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])

    assert FXLondonBreakoutStrategy(news_events=(event,)).on_candle_close(ctx(history)) is None


def test_one_trade_per_symbol_session_day() -> None:
    day = dt.date(2026, 5, 13)
    first = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    second = candle(dt.datetime(2026, 5, 13, 7, 10, tzinfo=UK), 1.2535)
    strategy = FXLondonBreakoutStrategy()

    assert strategy.on_candle_close(ctx(tuple(warmup_before(day) + asian_range(day) + [first]))) is not None
    assert strategy.on_candle_close(ctx(tuple(warmup_before(day) + asian_range(day) + [first, second]))) is None


def test_requires_asian_range_history() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + [breakout])

    assert FXLondonBreakoutStrategy().on_candle_close(ctx(history)) is None
