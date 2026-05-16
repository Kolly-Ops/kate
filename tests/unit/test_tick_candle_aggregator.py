from __future__ import annotations

import datetime as dt

import pytest

from trading_bot.core.data.tick_candle_aggregator import TickCandleAggregator


def test_aggregator_emits_only_when_next_bucket_arrives():
    agg = TickCandleAggregator(timeframe_minutes=1)

    assert agg.ingest_tick(
        symbol="MESM26",
        timestamp=dt.datetime(2026, 5, 11, 12, 0, 1),
        price=5000.0,
        size=2,
    ) == []
    assert agg.ingest_tick(
        symbol="MESM26",
        timestamp=dt.datetime(2026, 5, 11, 12, 0, 30),
        price=5001.0,
        size=1,
    ) == []

    closed = agg.ingest_tick(
        symbol="MESM26",
        timestamp=dt.datetime(2026, 5, 11, 12, 1, 0),
        price=4999.5,
        size=3,
    )

    assert len(closed) == 1
    candle = closed[0]
    assert candle.timestamp == dt.datetime(2026, 5, 11, 12, 0)
    assert candle.open == 5000.0
    assert candle.high == 5001.0
    assert candle.low == 5000.0
    assert candle.close == 5001.0
    assert candle.volume == 3


def test_aggregator_rejects_non_positive_timeframe():
    with pytest.raises(ValueError):
        TickCandleAggregator(timeframe_minutes=0)
