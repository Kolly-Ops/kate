"""Tick-to-candle aggregation for broker-native live feeds.

Rithmic-direct replaces Sierra's `.scid` file tail for live market data.
This small aggregator keeps the downstream engine contract unchanged by
turning last-trade ticks into closed `Candle` objects.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from .candle import Candle


@dataclass
class _OpenCandle:
    timestamp: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class TickCandleAggregator:
    """Aggregate trade ticks into fixed-minute candles.

    Semantics intentionally match Kate's file-tail path: a candle is emitted
    only when a tick for a later bucket arrives. There is no timer-based forced
    close, so quiet markets do not produce synthetic candles.
    """

    def __init__(self, timeframe_minutes: int = 1) -> None:
        if timeframe_minutes <= 0:
            raise ValueError("timeframe_minutes must be positive")
        self.timeframe_minutes = timeframe_minutes
        self._open: dict[str, _OpenCandle] = {}

    def ingest_tick(
        self,
        *,
        symbol: str,
        timestamp: dt.datetime,
        price: float,
        size: float = 0.0,
    ) -> list[Candle]:
        if price <= 0:
            return []

        bucket = self._bucket_start(timestamp)
        volume = max(0, int(size or 0))
        cur = self._open.get(symbol)
        if cur is None:
            self._open[symbol] = _OpenCandle(
                timestamp=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
            )
            return []

        if bucket == cur.timestamp:
            cur.high = max(cur.high, price)
            cur.low = min(cur.low, price)
            cur.close = price
            cur.volume += volume
            return []

        closed = Candle(
            timestamp=cur.timestamp,
            open=cur.open,
            high=cur.high,
            low=cur.low,
            close=cur.close,
            volume=cur.volume,
        )
        self._open[symbol] = _OpenCandle(
            timestamp=bucket,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=volume,
        )
        return [closed]

    def _bucket_start(self, timestamp: dt.datetime) -> dt.datetime:
        minute = timestamp.minute - (timestamp.minute % self.timeframe_minutes)
        return timestamp.replace(minute=minute, second=0, microsecond=0)


__all__ = ["TickCandleAggregator"]
