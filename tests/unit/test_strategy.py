"""
Unit tests for strategy layer, indicators, and the ORB-backed
AtrBreakoutStrategy compatibility wrapper.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import (
    AtrBreakoutStrategy,
    StrategyContext,
    atr,
    highest_high,
    lowest_low,
    sma,
    true_range,
)


def _candle(t: datetime, *, o: float, h: float, l: float, c: float, v: int = 1) -> Candle:
    return Candle(timestamp=t, open=o, high=h, low=l, close=c, volume=v)


def _series(*ohlcs: tuple[float, float, float, float]) -> tuple[Candle, ...]:
    """Build a candle series at minute-resolution from a flat list of OHLC tuples."""
    base = datetime(2026, 4, 27, 12, 0)
    return tuple(
        _candle(base + timedelta(minutes=i), o=o, h=h, l=l, c=c)
        for i, (o, h, l, c) in enumerate(ohlcs)
    )


def _series_at(
    base: datetime,
    *ohlcs: tuple[float, float, float, float],
) -> tuple[Candle, ...]:
    """Build a minute-resolution candle series from a specified base time."""
    return tuple(
        _candle(base + timedelta(minutes=i), o=o, h=h, l=l, c=c)
        for i, (o, h, l, c) in enumerate(ohlcs)
    )


def _orb_us_long_history() -> tuple[Candle, ...]:
    return (
        _candle(datetime(2026, 4, 27, 14, 30), o=100, h=102, l=99, c=100),
        _candle(datetime(2026, 4, 27, 14, 45), o=100, h=102, l=99, c=101),
        _candle(datetime(2026, 4, 27, 15, 30), o=101, h=111, l=100, c=110),
    )


def _orb_asian_long_history() -> tuple[Candle, ...]:
    return (
        _candle(datetime(2026, 4, 27, 0, 0), o=100, h=102, l=99, c=100),
        _candle(datetime(2026, 4, 27, 0, 15), o=100, h=102, l=99, c=101),
        _candle(datetime(2026, 4, 27, 1, 0), o=101, h=111, l=100, c=110),
    )


# Indicators
def test_sma_basic() -> None:
    candles = _series((1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4))
    assert sma(candles, 4) == 2.5
    assert sma(candles, 2) == 3.5


def test_sma_returns_zero_when_insufficient() -> None:
    candles = _series((1, 1, 1, 1))
    assert sma(candles, 5) == 0.0
    assert sma(candles, 0) == 0.0


def test_true_range() -> None:
    prev = _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101)
    cur = _candle(datetime(2026, 4, 27, 12, 1), o=101, h=104, l=100, c=103)
    assert true_range(prev, cur) == 4


def test_true_range_with_gap() -> None:
    prev = _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101)
    cur = _candle(datetime(2026, 4, 27, 12, 1), o=110, h=112, l=108, c=111)
    assert true_range(prev, cur) == 11


def test_atr_basic() -> None:
    candles = (
        _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101),
        _candle(datetime(2026, 4, 27, 12, 1), o=101, h=104, l=100, c=103),
        _candle(datetime(2026, 4, 27, 12, 2), o=103, h=105, l=102, c=104),
    )
    assert atr(candles, 2) == pytest.approx(3.5)


def test_atr_zero_when_insufficient() -> None:
    candles = _series((1, 1, 1, 1))
    assert atr(candles, 14) == 0.0


def test_highest_high_and_lowest_low() -> None:
    candles = _series(
        (10, 12, 9, 11),
        (11, 15, 10, 14),
        (14, 14, 12, 13),
    )
    assert highest_high(candles, 3) == 15
    assert lowest_low(candles, 3) == 9
    assert highest_high(candles, 1) == 14
    assert lowest_low(candles, 0) == 0.0


# Strategy: validation
def test_strategy_rejects_bad_params() -> None:
    with pytest.raises(ValueError):
        AtrBreakoutStrategy(breakout_lookback=1)
    with pytest.raises(ValueError):
        AtrBreakoutStrategy(ma_period=1)
    with pytest.raises(ValueError):
        AtrBreakoutStrategy(atr_period=1)
    with pytest.raises(ValueError):
        AtrBreakoutStrategy(atr_stop_mult=0)
    with pytest.raises(ValueError):
        AtrBreakoutStrategy(quantity=0)


def test_orb_history_window_is_max_ema_or_atr_plus_one() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=20, ma_period=50, atr_period=14)
    assert s.history_window == 50

    s = AtrBreakoutStrategy(breakout_lookback=20, ma_period=10, atr_period=14)
    assert s.history_window == 15


# Strategy: behavior
def _ctx(history: tuple[Candle, ...], *, has_open: bool = False) -> StrategyContext:
    return StrategyContext(
        symbol="MESM26",
        exchange="CME",
        candle=history[-1],
        history=history,
        tick_size=0.25,
        tick_value=1.25,
        per_contract_margin=100.0,
        has_open_position=has_open,
    )


def test_no_signal_when_history_too_short() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=5, ma_period=5, atr_period=3)
    short = _series((100, 101, 99, 100), (100, 101, 99, 100))
    assert s.on_candle_close(_ctx(short)) is None


def test_no_signal_when_position_open() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    assert s.on_candle_close(_ctx(_orb_us_long_history(), has_open=True)) is None


def test_orb_long_intent_in_us_session() -> None:
    s = AtrBreakoutStrategy(
        breakout_lookback=3,
        ma_period=3,
        atr_period=2,
        atr_stop_mult=1.0,
        atr_target_mult=2.5,
    )
    history = _orb_us_long_history()
    intent = s.on_candle_close(_ctx(history))
    assert intent is not None
    assert intent.side == proto.BUY
    assert intent.symbol == "MESM26"
    assert intent.exchange == "CME"
    assert intent.quantity == 1.0
    assert intent.order_type == proto.ORDER_TYPE_MARKET
    assert intent.price == 110
    assert intent.stop_loss == pytest.approx(103.0)
    assert intent.take_profit == pytest.approx(127.5)
    assert intent.per_contract_margin == 100.0
    assert "ORB us long" in intent.reason
    assert intent.metadata["session"] == "us"
    assert len(intent.intent_id) <= 32
    assert intent.intent_id.startswith("orb-us-")
    assert "MESM26" in intent.intent_id


def test_orb_long_intent_in_asian_session() -> None:
    s = AtrBreakoutStrategy(
        breakout_lookback=3,
        ma_period=3,
        atr_period=2,
        atr_stop_mult=1.0,
        atr_target_mult=2.5,
    )
    history = _orb_asian_long_history()
    intent = s.on_candle_close(_ctx(history))
    assert intent is not None
    assert intent.side == proto.BUY
    assert intent.metadata["session"] == "asian"
    assert intent.intent_id.startswith("orb-as-")


def test_orb_no_signal_outside_session_window() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    history = _series_at(
        datetime(2026, 4, 27, 12, 0),
        (100, 102, 99, 100),
        (100, 102, 99, 101),
        (101, 111, 100, 110),
    )
    assert s.on_candle_close(_ctx(history)) is None


def test_no_signal_when_close_below_breakout_high() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    history = _series(
        (100, 105, 99, 104),
        (104, 106, 103, 105),
        (105, 107, 104, 105),
        (105, 106, 104, 105),
    )
    assert s.on_candle_close(_ctx(history)) is None


def test_no_signal_when_close_below_sma() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    history = _series(
        (110, 115, 109, 114),
        (114, 116, 113, 115),
        (115, 117, 114, 116),
        (116, 117, 100, 101),
    )
    assert s.on_candle_close(_ctx(history)) is None


def test_intent_includes_stop_loss_so_risk_engine_will_evaluate() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    intent = s.on_candle_close(_ctx(_orb_us_long_history()))
    assert intent is not None
    assert intent.stop_loss is not None
    assert intent.stop_loss < intent.price
