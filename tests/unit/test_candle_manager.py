"""
Unit tests for CandleManager — uses synthetic .scid files so we don't depend
on Sierra Chart running.
"""
from __future__ import annotations

import struct
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from trading_bot.core.data import Candle, CandleManager
from trading_bot.core.data.scid_parser import (
    SCID_HEADER_SIZE,
    SCID_RECORD_FORMAT,
    SCID_RECORD_SIZE,
)


# Sierra base date: 1899-12-30 — every tick timestamp is microseconds since.
_BASE = datetime(1899, 12, 30)


def _us_since_base(when: datetime) -> int:
    delta = when - _BASE
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _pack_record(
    when: datetime,
    *,
    o: float,
    h: float,
    l: float,
    c: float,
    vol: int = 1,
    nt: int = 1,
    bv: int = 0,
    av: int = 0,
) -> bytes:
    return struct.pack(
        SCID_RECORD_FORMAT,
        _us_since_base(when),
        o,
        h,
        l,
        c,
        vol,
        nt,
        bv,
        av,
    )


def _write_scid(path: Path, ticks: list[bytes]) -> None:
    """Write a synthetic .scid file with a 56-byte zero header + the given ticks."""
    with open(path, "wb") as f:
        f.write(b"\x00" * SCID_HEADER_SIZE)
        for t in ticks:
            f.write(t)


def _append_ticks(path: Path, ticks: list[bytes]) -> None:
    with open(path, "ab") as f:
        for t in ticks:
            f.write(t)


# ── Scaffolding sanity ────────────────────────────────────────────────────
def test_pack_record_size_matches_scid_format() -> None:
    rec = _pack_record(datetime(2026, 4, 27, 12, 0, 0), o=5000, h=5001, l=4999, c=5000.5)
    assert len(rec) == SCID_RECORD_SIZE


# ── Backfill ──────────────────────────────────────────────────────────────
def test_backfill_returns_typed_candles_aggregated_to_minute(tmp_path: Path) -> None:
    scid = tmp_path / "MESM26_FUT_CME.scid"
    # Three ticks in minute 12:00, two in 12:01, one in 12:02
    ticks = [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),  o=5000, h=5001, l=4999, c=5000, vol=2),
        _pack_record(datetime(2026, 4, 27, 12, 0, 30), o=5000, h=5003, l=5000, c=5002, vol=3),
        _pack_record(datetime(2026, 4, 27, 12, 0, 55), o=5002, h=5004, l=5001, c=5003, vol=1),
        _pack_record(datetime(2026, 4, 27, 12, 1, 5),  o=5003, h=5005, l=5002, c=5005, vol=2),
        _pack_record(datetime(2026, 4, 27, 12, 1, 50), o=5005, h=5006, l=5004, c=5004, vol=1),
        _pack_record(datetime(2026, 4, 27, 12, 2, 10), o=5004, h=5004, l=5003, c=5003, vol=1),
    ]
    _write_scid(scid, ticks)

    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    candles = cm.backfill("MESM26_FUT_CME")

    assert len(candles) == 3
    assert all(isinstance(c, Candle) for c in candles)

    c0, c1, c2 = candles
    # Minute 12:00 — 3 ticks, OHLC: 5000/5004/4999/5003, volume 2+3+1=6
    assert c0.timestamp == datetime(2026, 4, 27, 12, 0)
    assert c0.open == 5000
    assert c0.high == 5004
    assert c0.low == 4999
    assert c0.close == 5003
    assert c0.volume == 6
    # Minute 12:01 — 2 ticks
    assert c1.timestamp == datetime(2026, 4, 27, 12, 1)
    assert c1.high == 5006
    assert c1.low == 5002
    assert c1.close == 5004
    assert c1.volume == 3
    # Minute 12:02 — 1 tick
    assert c2.timestamp == datetime(2026, 4, 27, 12, 2)
    assert c2.volume == 1


def test_backfill_returns_empty_when_file_missing(tmp_path: Path) -> None:
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    assert cm.backfill("NONEXISTENT_FUT_CME") == []


def test_backfill_max_candles_truncates_to_most_recent(tmp_path: Path) -> None:
    scid = tmp_path / "MES.scid"
    # 5 separate minutes, one tick each
    ticks = [
        _pack_record(datetime(2026, 4, 27, 12, m, 0), o=5000, h=5001, l=4999, c=5000 + m)
        for m in range(5)
    ]
    _write_scid(scid, ticks)

    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    truncated = cm.backfill("MES", max_candles=2)
    assert len(truncated) == 2
    assert truncated[0].close == 5003
    assert truncated[1].close == 5004


def test_backfill_handles_mes_scaled_prices(tmp_path: Path) -> None:
    """Sierra writes MES prices as integers ×100 in some configurations
    (e.g. 500050 instead of 5000.50). The parser /100s anything > 20000."""
    scid = tmp_path / "MES.scid"
    ticks = [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),
                     o=500050, h=500200, l=499900, c=500100, vol=1),
    ]
    _write_scid(scid, ticks)

    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    candles = cm.backfill("MES")
    assert len(candles) == 1
    c = candles[0]
    assert c.open == pytest.approx(5000.50)
    assert c.high == pytest.approx(5002.00)
    assert c.low == pytest.approx(4999.00)
    assert c.close == pytest.approx(5001.00)


# ── Live tail / poll() ────────────────────────────────────────────────────
def test_first_poll_baselines_at_file_end_no_replay(tmp_path: Path) -> None:
    scid = tmp_path / "MES.scid"
    ticks = [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),  o=5000, h=5001, l=4999, c=5000),
        _pack_record(datetime(2026, 4, 27, 12, 0, 10), o=5000, h=5001, l=4999, c=5001),
    ]
    _write_scid(scid, ticks)

    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    # First poll: even though ticks already exist, return [] (baseline only).
    assert cm.poll("MES") == []
    # No in-progress candle yet either.
    assert cm.current_candle("MES") is None


def test_poll_returns_no_closed_candle_when_only_one_minute_active(
    tmp_path: Path,
) -> None:
    scid = tmp_path / "MES.scid"
    _write_scid(scid, [])  # empty file
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")  # baseline

    # Append 3 ticks all in minute 12:00 — no boundary crossed
    _append_ticks(scid, [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),  o=5000, h=5001, l=4999, c=5000, vol=2),
        _pack_record(datetime(2026, 4, 27, 12, 0, 25), o=5000, h=5005, l=5000, c=5004, vol=3),
        _pack_record(datetime(2026, 4, 27, 12, 0, 55), o=5004, h=5006, l=5003, c=5005, vol=1),
    ])
    closed = cm.poll("MES")
    assert closed == []   # no boundary crossed
    cur = cm.current_candle("MES")
    assert cur is not None
    assert cur.timestamp == datetime(2026, 4, 27, 12, 0)
    assert cur.open == 5000
    assert cur.high == 5006
    assert cur.low == 4999
    assert cur.close == 5005
    assert cur.volume == 6


def test_poll_emits_closed_candle_on_boundary_cross(tmp_path: Path) -> None:
    scid = tmp_path / "MES.scid"
    _write_scid(scid, [])
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")  # baseline

    # Two ticks in 12:00, then one in 12:01 — should close 12:00
    _append_ticks(scid, [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),  o=5000, h=5001, l=4999, c=5000, vol=2),
        _pack_record(datetime(2026, 4, 27, 12, 0, 55), o=5000, h=5004, l=5000, c=5003, vol=3),
        _pack_record(datetime(2026, 4, 27, 12, 1, 5),  o=5003, h=5005, l=5002, c=5004, vol=1),
    ])
    closed = cm.poll("MES")
    assert len(closed) == 1
    c = closed[0]
    assert c.timestamp == datetime(2026, 4, 27, 12, 0)
    assert c.high == 5004
    assert c.low == 4999
    assert c.close == 5003
    assert c.volume == 5

    # In-progress is now the 12:01 bar with the single tick
    cur = cm.current_candle("MES")
    assert cur is not None
    assert cur.timestamp == datetime(2026, 4, 27, 12, 1)
    assert cur.close == 5004
    assert cur.volume == 1


def test_poll_emits_multiple_closed_when_ticks_span_boundaries(tmp_path: Path) -> None:
    scid = tmp_path / "MES.scid"
    _write_scid(scid, [])
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")

    _append_ticks(scid, [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5),  o=5000, h=5001, l=4999, c=5000),
        _pack_record(datetime(2026, 4, 27, 12, 1, 5),  o=5000, h=5002, l=4998, c=5001),
        _pack_record(datetime(2026, 4, 27, 12, 2, 5),  o=5001, h=5003, l=5000, c=5002),
        _pack_record(datetime(2026, 4, 27, 12, 3, 5),  o=5002, h=5004, l=5001, c=5003),
    ])
    closed = cm.poll("MES")
    assert len(closed) == 3   # 12:00, 12:01, 12:02 all closed; 12:03 in progress
    assert [c.timestamp.minute for c in closed] == [0, 1, 2]


def test_poll_handles_partial_record_at_eof(tmp_path: Path) -> None:
    """Sierra writes records as they form. We must not blow up on a
    partially-written final record (defensive truncation)."""
    scid = tmp_path / "MES.scid"
    _write_scid(scid, [])
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")

    # Append one full record + 7 garbage bytes (partial record)
    full = _pack_record(datetime(2026, 4, 27, 12, 0, 5), o=5000, h=5001, l=4999, c=5000)
    with open(scid, "ab") as f:
        f.write(full)
        f.write(b"\x00" * 7)   # partial — should be ignored

    closed = cm.poll("MES")
    assert closed == []
    cur = cm.current_candle("MES")
    assert cur is not None
    assert cur.close == 5000


def test_reset_drops_state_so_next_poll_rebaselines(tmp_path: Path) -> None:
    scid = tmp_path / "MES.scid"
    _write_scid(scid, [])
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")
    _append_ticks(scid, [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5), o=5000, h=5001, l=4999, c=5000),
    ])
    cm.poll("MES")
    assert cm.current_candle("MES") is not None
    cm.reset("MES")
    assert cm.current_candle("MES") is None


def test_multi_instrument_isolation(tmp_path: Path) -> None:
    """Two symbols share a CandleManager but their tail states are independent."""
    mes = tmp_path / "MES.scid"
    mgc = tmp_path / "MGC.scid"
    _write_scid(mes, [])
    _write_scid(mgc, [])
    cm = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    cm.poll("MES")
    cm.poll("MGC")

    _append_ticks(mes, [
        _pack_record(datetime(2026, 4, 27, 12, 0, 5), o=5000, h=5001, l=4999, c=5000),
    ])
    cm.poll("MES")
    cm.poll("MGC")

    assert cm.current_candle("MES") is not None
    assert cm.current_candle("MGC") is None
