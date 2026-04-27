"""
Candle — canonical OHLCV record used throughout the bot.

The Sierra .scid parser yields dicts; CandleManager normalizes them into this
dataclass so downstream consumers (strategies, backtests, persistence layer)
work against a single typed shape.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Candle:
    """One OHLCV bar.

    `timestamp` is the candle's START time, naive (Sierra Chart writes
    timezone-naive timestamps in microseconds since 1899-12-30).
    """

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open
