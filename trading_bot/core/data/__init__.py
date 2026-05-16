"""Data normalization — Sierra .scid → typed OHLCV candles."""
from .candle import Candle
from .candle_manager import CandleManager
from .tick_candle_aggregator import TickCandleAggregator

__all__ = ["Candle", "CandleManager", "TickCandleAggregator"]
