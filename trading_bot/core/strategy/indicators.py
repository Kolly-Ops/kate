"""
Indicator primitives used by deterministic strategies.

Pure functions over Candle sequences. No state, no caching — the engine
re-evaluates on each closed candle, and these compute fast enough at
Phase A volumes (1m candles, history windows ≤ 100) that caching adds
complexity without benefit.

All indicators return floats. Empty / insufficient input returns 0.0
rather than raising — strategies guard with their own length checks.
"""
from __future__ import annotations

from typing import Sequence

from trading_bot.core.data import Candle


def sma(candles: Sequence[Candle], period: int) -> float:
    """Simple moving average of close prices over the last `period` candles."""
    if period <= 0 or len(candles) < period:
        return 0.0
    window = candles[-period:]
    return sum(c.close for c in window) / period


def true_range(prev: Candle, current: Candle) -> float:
    """One-bar true range: max(H-L, |H - prev_close|, |L - prev_close|)."""
    return max(
        current.high - current.low,
        abs(current.high - prev.close),
        abs(current.low - prev.close),
    )


def atr(candles: Sequence[Candle], period: int) -> float:
    """Average True Range over the last `period` candles. Simple mean of
    true ranges (Wilder's smoothing is a Phase A v2 enhancement; the simple
    mean is sufficient for a deterministic baseline)."""
    if period <= 0 or len(candles) < period + 1:
        return 0.0
    window = candles[-(period + 1):]
    trs = [true_range(window[i - 1], window[i]) for i in range(1, len(window))]
    return sum(trs) / len(trs)


def highest_high(candles: Sequence[Candle], period: int) -> float:
    """Highest high over the last `period` candles."""
    if period <= 0 or len(candles) < period:
        return 0.0
    return max(c.high for c in candles[-period:])


def lowest_low(candles: Sequence[Candle], period: int) -> float:
    """Lowest low over the last `period` candles."""
    if period <= 0 or len(candles) < period:
        return 0.0
    return min(c.low for c in candles[-period:])


def ema(candles: Sequence[Candle], period: int) -> float:
    """Exponential moving average of close prices over the last `period`
    candles. Uses pandas-equivalent smoothing factor α = 2/(period+1) and
    seeds the recursion with the SMA of the first `period` bars (matches
    pandas' `ewm(span=period, adjust=False).mean()` against an SMA seed).

    Returns 0.0 if insufficient history.
    """
    if period <= 1 or len(candles) < period:
        return 0.0
    alpha = 2.0 / (period + 1)
    seed = sum(c.close for c in candles[:period]) / period
    value = seed
    for c in candles[period:]:
        value = alpha * c.close + (1.0 - alpha) * value
    return value
