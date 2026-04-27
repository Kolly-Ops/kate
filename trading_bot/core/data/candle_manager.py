"""
CandleManager — backfill + live tail of OHLCV candles from Sierra .scid files.

Backfill uses the existing `parse_scid_aggregated` (chunked read, MES price
scaling). Live tail is a stateful poller: it tracks the file position + the
in-progress candle, and returns ONLY closed candles per poll. The current
in-progress candle is held in state until a new candle begins.

Semantics:
- First `poll(symbol)` baselines `last_position` at the file's current end —
  NO historical replay. Use `backfill(symbol)` for history.
- Subsequent `poll()`s read new bytes since baseline, aggregate ticks into
  candles, return any candles that closed (i.e. a tick from the next bar
  arrived).
- A candle whose boundary has elapsed in wallclock but received no new ticks
  is NOT emitted until the next tick arrives. Time-based forced-close is a
  Phase A v2 enhancement; not needed for deterministic-strategy semantics
  where signal evaluation runs on close-of-bar events.
"""
from __future__ import annotations

import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .candle import Candle
from .scid_parser import (
    SCID_RECORD_FORMAT,
    SCID_RECORD_SIZE,
    parse_scid_aggregated,
)


_BASE_DATE = datetime(1899, 12, 30)


def _scale_price(p_raw: float, fallback: Optional[float] = None) -> float:
    """Sierra MES-style scaled prices (>20000) get /100; others natural.
    Returns `fallback` when raw is non-positive (open=0 sentinel for the
    open of a new candle is replaced by close)."""
    if p_raw <= 0:
        return fallback if fallback is not None else 0.0
    return p_raw / 100.0 if p_raw > 20000 else p_raw


class _TailState:
    """Per-instrument tail-poller state.

    Holds: file path, candle bucket size in microseconds, last byte read,
    and the in-progress candle. Calls to `poll()` are stateful — each call
    advances last_position and may emit zero or more closed candles."""

    __slots__ = ("filepath", "timeframe_us", "last_position", "current_id", "current")

    def __init__(self, filepath: Path, timeframe_minutes: int) -> None:
        self.filepath = filepath
        self.timeframe_us = timeframe_minutes * 60 * 1_000_000
        self.last_position: Optional[int] = None
        self.current_id: Optional[int] = None
        self.current: Optional[Candle] = None

    def poll(self) -> list[Candle]:
        if not self.filepath.exists():
            return []

        file_size = self.filepath.stat().st_size

        # First poll: baseline at end of file. No historical replay.
        if self.last_position is None:
            self.last_position = file_size
            return []

        if file_size <= self.last_position:
            return []

        with open(self.filepath, "rb") as f:
            f.seek(self.last_position)
            new_data = f.read(file_size - self.last_position)
        self.last_position = file_size

        # Defensive: Sierra may write partial records mid-flush; truncate.
        usable_len = len(new_data) - (len(new_data) % SCID_RECORD_SIZE)
        if usable_len == 0:
            return []

        closed: list[Candle] = []
        for row in struct.iter_unpack(SCID_RECORD_FORMAT, new_data[:usable_len]):
            dt_raw, o_raw, h_raw, l_raw, c_raw, v, *_ = row
            if c_raw <= 0:
                continue

            close_p = _scale_price(c_raw)
            open_p = _scale_price(o_raw, fallback=close_p)
            high_p = _scale_price(h_raw, fallback=close_p)
            low_p = _scale_price(l_raw, fallback=close_p)

            candle_id = dt_raw // self.timeframe_us

            if candle_id != self.current_id:
                # Boundary crossed — close the previous candle, open new one.
                if self.current is not None:
                    closed.append(self.current)
                ts = _BASE_DATE + timedelta(
                    microseconds=candle_id * self.timeframe_us
                )
                self.current = Candle(
                    timestamp=ts,
                    open=open_p,
                    high=high_p,
                    low=low_p,
                    close=close_p,
                    volume=int(v),
                )
                self.current_id = candle_id
            else:
                # Same bucket — update OHLCV in-place (Candle is frozen,
                # so build a new instance).
                c = self.current
                assert c is not None  # mypy: current_id matched, so current set
                self.current = Candle(
                    timestamp=c.timestamp,
                    open=c.open,
                    high=max(c.high, high_p),
                    low=min(c.low, low_p),
                    close=close_p,
                    volume=c.volume + int(v),
                )

        return closed


class CandleManager:
    """Top-level data normalization layer.

    One CandleManager handles a single timeframe across multiple symbols.
    For multi-timeframe strategies (e.g. 1m execution + 1h trend filter),
    instantiate one manager per timeframe.
    """

    def __init__(
        self,
        scid_dir: Path | str,
        *,
        timeframe_minutes: int = 1,
    ) -> None:
        self.scid_dir = Path(scid_dir)
        self.timeframe_minutes = timeframe_minutes
        self._tails: dict[str, _TailState] = {}

    def scid_path(self, symbol: str) -> Path:
        """e.g. 'MESM26_FUT_CME' → {scid_dir}/MESM26_FUT_CME.scid"""
        return self.scid_dir / f"{symbol}.scid"

    def backfill(
        self,
        symbol: str,
        *,
        max_candles: Optional[int] = 1000,
        max_gb: float = 1.0,
    ) -> list[Candle]:
        """Load up to `max_candles` most recent closed candles from the
        .scid file. Returns [] if the file does not exist."""
        path = self.scid_path(symbol)
        if not path.exists():
            return []

        raw = parse_scid_aggregated(
            str(path),
            timeframe_min=self.timeframe_minutes,
            max_gb=max_gb,
        )
        candles = [
            Candle(
                timestamp=d["timestamp"],
                open=d["open"],
                high=d["high"],
                low=d["low"],
                close=d["close"],
                volume=int(d["volume"]),
            )
            for d in raw
            if d.get("close", 0) > 0
        ]
        if max_candles is not None and len(candles) > max_candles:
            candles = candles[-max_candles:]
        return candles

    def poll(self, symbol: str) -> list[Candle]:
        """Read new ticks since last poll, return any candles that closed.

        First call for a given symbol baselines at the file's current end
        (no replay). Returns [] when there are no new ticks or the file
        does not exist."""
        tail = self._tails.get(symbol)
        if tail is None:
            tail = _TailState(self.scid_path(symbol), self.timeframe_minutes)
            self._tails[symbol] = tail
        return tail.poll()

    def current_candle(self, symbol: str) -> Optional[Candle]:
        """The in-progress (not yet closed) candle. Useful for intra-bar
        stop-loss evaluation. Returns None if poll() has never run for the
        symbol or no ticks have arrived since baseline."""
        tail = self._tails.get(symbol)
        return tail.current if tail else None

    def reset(self, symbol: str) -> None:
        """Discard tail state for a symbol. Next poll() re-baselines."""
        self._tails.pop(symbol, None)
