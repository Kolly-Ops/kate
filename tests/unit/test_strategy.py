"""
Unit tests for strategy layer — indicators + AtrBreakoutStrategy.
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


# ── Indicators ────────────────────────────────────────────────────────────
def test_sma_basic() -> None:
    candles = _series((1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4))
    assert sma(candles, 4) == 2.5
    assert sma(candles, 2) == 3.5    # last 2 closes


def test_sma_returns_zero_when_insufficient() -> None:
    candles = _series((1, 1, 1, 1))
    assert sma(candles, 5) == 0.0
    assert sma(candles, 0) == 0.0


def test_true_range() -> None:
    prev = _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101)
    cur = _candle(datetime(2026, 4, 27, 12, 1), o=101, h=104, l=100, c=103)
    # H-L = 4, |H - prev_c| = 3, |L - prev_c| = 1 → max = 4
    assert true_range(prev, cur) == 4


def test_true_range_with_gap() -> None:
    prev = _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101)
    cur = _candle(datetime(2026, 4, 27, 12, 1), o=110, h=112, l=108, c=111)
    # H-L = 4, |H - prev_c| = 11, |L - prev_c| = 7 → max = 11 (gap up)
    assert true_range(prev, cur) == 11


def test_atr_basic() -> None:
    # 3 candles → 2 true ranges
    candles = (
        _candle(datetime(2026, 4, 27, 12, 0), o=100, h=102, l=99, c=101),
        _candle(datetime(2026, 4, 27, 12, 1), o=101, h=104, l=100, c=103),  # TR = 4
        _candle(datetime(2026, 4, 27, 12, 2), o=103, h=105, l=102, c=104),  # TR = max(3, 2, 1) = 3
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
    assert highest_high(candles, 1) == 14   # last candle only
    assert lowest_low(candles, 0) == 0.0    # invalid period


# ── Strategy: validation ──────────────────────────────────────────────────
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


def test_strategy_history_window_is_max_param_plus_one() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=20, ma_period=50, atr_period=14)
    assert s.history_window == 51   # max(20, 50, 14) + 1


# ── Strategy: behavior ────────────────────────────────────────────────────
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
    # Build a clear breakout, but mark position open — should still return None
    history = _series(
        *[(100, 101, 99, 100)] * 5,            # flat history
        (100, 110, 100, 109),                  # explosive breakout candle
    )
    assert s.on_candle_close(_ctx(history, has_open=True)) is None


def test_breakout_long_intent_is_emitted() -> None:
    s = AtrBreakoutStrategy(
        breakout_lookback=3,
        ma_period=3,
        atr_period=2,
        atr_stop_mult=2.0,
        atr_target_mult=3.0,
    )
    # 5 flat candles around 100 + a strong breakout at 110
    history = _series(
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 110, 100, 109),
    )
    intent = s.on_candle_close(_ctx(history))
    assert intent is not None
    assert intent.side == proto.BUY
    assert intent.symbol == "MESM26"
    assert intent.exchange == "CME"
    assert intent.quantity == 1.0
    assert intent.order_type == proto.ORDER_TYPE_MARKET
    assert intent.price == 109
    # ATR(period=2) uses the last 3 candles (period + 1) for 2 true ranges:
    #   bar4 (flat 100/101/99/100) → bar5 (flat 100/101/99/100):
    #     TR = max(101-99, |101-100|, |99-100|) = max(2, 1, 1) = 2
    #   bar5 (flat) → breakout (100/110/100/109):
    #     TR = max(110-100, |110-100|, |100-100|) = 10
    #   ATR(2) = (2 + 10) / 2 = 6.0
    # Stop  = 109 - 2*6.0 = 97.0
    # Target = 109 + 3*6.0 = 127.0
    assert intent.stop_loss == pytest.approx(97.0)
    assert intent.take_profit == pytest.approx(127.0)
    assert intent.per_contract_margin == 100.0
    assert "breakout" in intent.reason
    assert intent.strategy_name in intent.intent_id


def test_no_signal_when_close_below_breakout_high() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    # Last bar barely above SMA but BELOW the prior 3-bar high
    history = _series(
        (100, 105, 99, 104),    # high = 105
        (104, 106, 103, 105),   # high = 106 — this will be the breakout reference
        (105, 107, 104, 105),   # high = 107
        (105, 106, 104, 105),   # close 105 < highest_high(prior 3) = 107
    )
    assert s.on_candle_close(_ctx(history)) is None


def test_no_signal_when_close_below_sma() -> None:
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    # Build history where close exceeds prior high but is below SMA
    # (rising prices then a small breakout from below the average)
    history = _series(
        (110, 115, 109, 114),
        (114, 116, 113, 115),
        (115, 117, 114, 116),
        (116, 117, 100, 101),   # close below SMA but above prior high? No — this clarifies why we test SMA
    )
    intent = s.on_candle_close(_ctx(history))
    # close 101 < prior 3-bar high (117) AND < SMA — both filters fail
    assert intent is None


def test_intent_includes_stop_loss_so_risk_engine_will_evaluate() -> None:
    """Risk engine requires stop_loss for per-trade-risk evaluation. Confirm
    the strategy always sets one on entries."""
    s = AtrBreakoutStrategy(breakout_lookback=3, ma_period=3, atr_period=2)
    history = _series(
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 101, 99, 100),
        (100, 110, 100, 109),
    )
    intent = s.on_candle_close(_ctx(history))
    assert intent is not None
    assert intent.stop_loss is not None
    assert intent.stop_loss < intent.price   # long stop is below entry
