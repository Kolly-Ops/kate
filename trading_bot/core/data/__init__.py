"""Data normalization — Sierra .scid → typed OHLCV candles."""
from .candle import Candle
from .candle_manager import CandleManager

__all__ = ["Candle", "CandleManager"]
