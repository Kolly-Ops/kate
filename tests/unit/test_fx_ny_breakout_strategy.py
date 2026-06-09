import datetime as dt

import pytest

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import FXLondonBreakoutStrategy, FXNYBreakoutStrategy, StrategyContext


UK = dt.timezone(dt.timedelta(hours=1))


def candle(
    ts: dt.datetime,
    close: float,
    high: float | None = None,
    low: float | None = None,
) -> Candle:
    high = close + 0.0003 if high is None else high
    low = close - 0.0003 if low is None else low
    return Candle(timestamp=ts, open=close, high=high, low=low, close=close, volume=100)


def ctx(
    candles: tuple[Candle, ...],
    *,
    symbol: str = "USDCAD",
    has_open_position: bool = False,
) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        exchange="ICMarketsSC-Demo",
        candle=candles[-1],
        history=candles,
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=has_open_position,
    )


def warmup_before(day: dt.date) -> list[Candle]:
    start = dt.datetime.combine(day, dt.time(11, 30), tzinfo=UK)
    return [candle(start + dt.timedelta(minutes=i), 1.3500) for i in range(30)]


def ny_range(day: dt.date) -> list[Candle]:
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(12, 0), tzinfo=UK)
    for minute in range(210):
        ts = start + dt.timedelta(minutes=minute)
        close = 1.3500 + ((minute % 20) * 0.00001)
        candles.append(candle(ts, close, high=1.3520, low=1.3480))
    return candles


def london_range(day: dt.date) -> list[Candle]:
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK)
    for minute in range(7 * 60):
        ts = start + dt.timedelta(minutes=minute)
        close = 1.2500 + ((minute % 20) * 0.00001)
        candles.append(candle(ts, close, high=1.2520, low=1.2480))
    return candles


def test_ny_breakout_fires_only_inside_ny_trade_window() -> None:
    day = dt.date(2026, 6, 5)
    before_window = candle(dt.datetime(2026, 6, 5, 15, 29, tzinfo=UK), 1.3530)
    first_trade_bar = candle(dt.datetime(2026, 6, 5, 15, 30, tzinfo=UK), 1.3530)
    history = tuple(warmup_before(day) + ny_range(day))
    strategy = FXNYBreakoutStrategy(quantity=0.56, reward_risk=2.0, atr_period=14)

    assert strategy.on_candle_close(ctx(history + (before_window,))) is None

    intent = strategy.on_candle_close(ctx(history + (first_trade_bar,)))

    assert intent is not None
    assert intent.intent_id.startswith("fxny-USDCAD-")
    assert intent.strategy_name.startswith("fx_ny_breakout(")
    assert intent.side == proto.BUY
    assert intent.quantity == pytest.approx(0.56)
    assert intent.metadata["session"] == "ny"
    assert intent.metadata["range_label"] == "NY reference range"


def test_ny_breakout_does_not_enter_at_or_after_force_flat_time() -> None:
    day = dt.date(2026, 6, 5)
    cutoff_bar = candle(dt.datetime(2026, 6, 5, 18, 0, tzinfo=UK), 1.3530)
    history = tuple(warmup_before(day) + ny_range(day) + [cutoff_bar])

    assert FXNYBreakoutStrategy().on_candle_close(ctx(history)) is None


def test_london_and_ny_cooldowns_are_isolated_by_strategy_instance() -> None:
    day = dt.date(2026, 6, 5)
    london = FXLondonBreakoutStrategy(intent_cooldown_minutes=120)
    ny = FXNYBreakoutStrategy(intent_cooldown_minutes=120)
    london_exit = dt.datetime(2026, 6, 5, 9, 15, tzinfo=UK).astimezone(dt.timezone.utc)
    london.on_position_closed("EURUSD", london_exit)

    ny_breakout = candle(dt.datetime(2026, 6, 5, 15, 30, tzinfo=UK), 1.3530)
    ny_history = tuple(warmup_before(day) + ny_range(day) + [ny_breakout])

    assert ny.on_candle_close(ctx(ny_history, symbol="EURUSD")) is not None


def test_london_and_ny_can_trade_same_pair_same_day_independently() -> None:
    day = dt.date(2026, 6, 5)
    london = FXLondonBreakoutStrategy(intent_cooldown_minutes=0)
    ny = FXNYBreakoutStrategy(intent_cooldown_minutes=0)

    london_breakout = candle(dt.datetime(2026, 6, 5, 7, 5, tzinfo=UK), 1.2530)
    london_history = tuple(london_range(day) + [london_breakout])
    london_intent = london.on_candle_close(ctx(london_history, symbol="EURUSD"))

    ny_breakout = candle(dt.datetime(2026, 6, 5, 15, 30, tzinfo=UK), 1.3530)
    ny_history = tuple(warmup_before(day) + ny_range(day) + [ny_breakout])
    ny_intent = ny.on_candle_close(ctx(ny_history, symbol="EURUSD"))

    assert london_intent is not None
    assert ny_intent is not None
    assert london_intent.intent_id.startswith("fxlon-EURUSD-")
    assert ny_intent.intent_id.startswith("fxny-EURUSD-")
