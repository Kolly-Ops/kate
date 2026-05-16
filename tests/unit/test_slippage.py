"""Regression tests for slippage telemetry.

Slippage telemetry is the audit-recommendation product from the FX
backtest self-audit 2026-05-15. These tests guard the contract that
informs Front 4 validation gates.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from trading_bot.core.telemetry.slippage import (
    SlippageRecord,
    SlippageRecorder,
    SlippageSummary,
    _pip_units,
)


# ── Pip computation ──────────────────────────────────────────────────────


def test_pip_units_buy_positive_slippage_when_fill_higher():
    # BUY at 1.32675, filled at 1.32683 = 0.8 pip BAD
    delta_price, delta_pips = _pip_units("BUY", 1.32675, 1.32683, 0.0001)
    assert delta_price == pytest.approx(0.00008, abs=1e-9)
    assert delta_pips == pytest.approx(0.8, abs=1e-5)


def test_pip_units_buy_negative_slippage_when_fill_lower():
    # BUY at 1.32675, filled at 1.32670 = -0.5 pip GOOD (got better fill)
    delta_price, delta_pips = _pip_units("BUY", 1.32675, 1.32670, 0.0001)
    assert delta_pips == pytest.approx(-0.5, abs=1e-5)


def test_pip_units_sell_positive_slippage_when_fill_lower():
    # SELL at 1.32675, filled at 1.32670 = 0.5 pip BAD (received less)
    delta_price, delta_pips = _pip_units("SELL", 1.32675, 1.32670, 0.0001)
    assert delta_pips == pytest.approx(0.5, abs=1e-5)


def test_pip_units_sell_negative_slippage_when_fill_higher():
    # SELL at 1.32675, filled at 1.32680 = -0.5 pip GOOD (received more)
    delta_price, delta_pips = _pip_units("SELL", 1.32675, 1.32680, 0.0001)
    assert delta_pips == pytest.approx(-0.5, abs=1e-5)


def test_pip_units_rejects_unknown_side():
    with pytest.raises(ValueError, match="unknown side"):
        _pip_units("LONG", 1.0, 1.0, 0.0001)


def test_pip_units_handles_case_insensitive_side():
    _, pips_lower = _pip_units("buy", 1.0, 1.0001, 0.0001)
    _, pips_upper = _pip_units("BUY", 1.0, 1.0001, 0.0001)
    assert pips_lower == pips_upper


# ── SlippageRecorder basic flow ──────────────────────────────────────────


def test_recorder_records_intent_and_pairs_with_fill(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    rec.record_intent(
        intent_id="fxlon-1",
        signal_price=1.32675,
        side="BUY",
        symbol="GBPUSD",
        signal_timestamp_utc="2026-05-19T06:55:00+00:00",
    )
    assert rec.pending_count == 1

    record = rec.record_fill(
        intent_id="fxlon-1",
        fill_price=1.32683,
        fill_timestamp_utc="2026-05-19T06:55:01+00:00",
    )
    assert record is not None
    assert record.intent_id == "fxlon-1"
    assert record.signal_price == 1.32675
    assert record.fill_price == 1.32683
    assert record.slippage_pips == pytest.approx(0.8, abs=1e-5)
    assert rec.pending_count == 0
    assert len(rec.records) == 1


def test_recorder_writes_jsonl_on_fill(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    rec.record_intent(
        intent_id="fxlon-1", signal_price=1.32675, side="BUY",
        symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:55:00+00:00",
    )
    rec.record_fill(
        intent_id="fxlon-1", fill_price=1.32683,
        fill_timestamp_utc="2026-05-19T06:55:01+00:00",
    )

    jsonl = (tmp_path / "front_4_slippage.jsonl").read_text()
    assert jsonl.count("\n") == 1
    parsed = json.loads(jsonl.strip())
    assert parsed["intent_id"] == "fxlon-1"
    assert parsed["front_id"] == "front_4"
    assert parsed["slippage_pips"] == pytest.approx(0.8, abs=1e-5)


def test_recorder_fill_for_unknown_intent_returns_none(tmp_path, caplog):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    result = rec.record_fill(
        intent_id="never-seen",
        fill_price=1.0,
        fill_timestamp_utc="2026-05-19T06:55:01+00:00",
    )
    assert result is None
    assert not (tmp_path / "front_4_slippage.jsonl").exists()


def test_recorder_rejects_invalid_intent(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    with pytest.raises(ValueError):
        rec.record_intent(
            intent_id="", signal_price=1.0, side="BUY",
            symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:55:00+00:00",
        )
    with pytest.raises(ValueError):
        rec.record_intent(
            intent_id="x", signal_price=-1.0, side="BUY",
            symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:55:00+00:00",
        )
    with pytest.raises(ValueError):
        rec.record_intent(
            intent_id="x", signal_price=1.0, side="LONG",
            symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:55:00+00:00",
        )


def test_recorder_rejects_invalid_fill(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    with pytest.raises(ValueError):
        rec.record_fill(intent_id="", fill_price=1.0, fill_timestamp_utc="t")
    with pytest.raises(ValueError):
        rec.record_fill(intent_id="x", fill_price=0.0, fill_timestamp_utc="t")


# ── Pending-intent eviction (memory protection) ───────────────────────────


def test_recorder_evicts_oldest_pending_at_cap(tmp_path):
    rec = SlippageRecorder(
        front_id="front_4", log_root=tmp_path, max_pending=3,
    )
    for i in range(5):
        rec.record_intent(
            intent_id=f"intent-{i}", signal_price=1.0 + i * 0.0001,
            side="BUY", symbol="GBPUSD",
            signal_timestamp_utc=f"2026-05-19T06:5{i}:00+00:00",
        )
    # Should have evicted the 2 oldest (intent-0, intent-1) — only 3 stay
    assert rec.pending_count == 3
    # Latest 3 should still be matchable
    record = rec.record_fill(
        intent_id="intent-4", fill_price=1.0005,
        fill_timestamp_utc="2026-05-19T06:54:01+00:00",
    )
    assert record is not None


# ── Duplicate intent_id handling ─────────────────────────────────────────


def test_recorder_warns_and_overwrites_on_duplicate_intent_id(tmp_path, caplog):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    rec.record_intent(
        intent_id="dup", signal_price=1.0, side="BUY",
        symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:55:00+00:00",
    )
    # Re-recording overwrites — by design (strategy might retry on rejection)
    rec.record_intent(
        intent_id="dup", signal_price=1.5, side="SELL",
        symbol="GBPUSD", signal_timestamp_utc="2026-05-19T06:56:00+00:00",
    )
    record = rec.record_fill(
        intent_id="dup", fill_price=1.5,
        fill_timestamp_utc="2026-05-19T06:56:01+00:00",
    )
    assert record is not None
    assert record.signal_price == 1.5  # overwrite took effect
    assert record.side == "SELL"


# ── Summary statistics ───────────────────────────────────────────────────


def test_summary_zero_records(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    s = rec.summary()
    assert s.n_pairs == 0
    assert s.mean_pips == 0.0
    assert s.std_pips == 0.0


def test_summary_multiple_records(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    # 3 BUY trades with known slippages: +1.0, +0.5, -0.5 pips
    for i, (signal, fill) in enumerate([
        (1.32675, 1.32685),  # +1.0 pip
        (1.32700, 1.32705),  # +0.5 pip
        (1.32750, 1.32745),  # -0.5 pip
    ]):
        rec.record_intent(
            intent_id=f"i{i}", signal_price=signal, side="BUY",
            symbol="GBPUSD", signal_timestamp_utc=f"t{i}",
        )
        rec.record_fill(intent_id=f"i{i}", fill_price=fill, fill_timestamp_utc=f"t{i}+1")

    s = rec.summary()
    assert s.n_pairs == 3
    assert s.mean_pips == pytest.approx((1.0 + 0.5 - 0.5) / 3, abs=1e-5)
    assert s.median_pips == pytest.approx(0.5, abs=1e-5)
    assert s.min_pips == pytest.approx(-0.5, abs=1e-5)
    assert s.max_pips == pytest.approx(1.0, abs=1e-5)


# ── Persistence + reload ─────────────────────────────────────────────────


def test_load_persisted_rehydrates_records(tmp_path):
    rec1 = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    rec1.record_intent(
        intent_id="p1", signal_price=1.0, side="BUY", symbol="GBPUSD",
        signal_timestamp_utc="t1",
    )
    rec1.record_fill(intent_id="p1", fill_price=1.001, fill_timestamp_utc="t2")

    # Fresh recorder loads from disk
    rec2 = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    loaded = rec2.load_persisted()
    assert loaded == 1
    assert len(rec2.records) == 1
    assert rec2.records[0].intent_id == "p1"


def test_load_persisted_skips_malformed_lines(tmp_path):
    jsonl = tmp_path / "front_4_slippage.jsonl"
    jsonl.write_text(
        json.dumps({
            "intent_id": "good",
            "front_id": "front_4",
            "symbol": "GBPUSD",
            "side": "BUY",
            "signal_price": 1.0,
            "signal_timestamp_utc": "t",
            "fill_price": 1.001,
            "fill_timestamp_utc": "t",
            "slippage_price": 0.001,
            "slippage_pips": 1.0,
            "fill_latency_seconds": 0.5,
        }) + "\n"
        + "this is not json\n"
        + "{\"missing\": \"fields\"}\n"
    )
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path)
    loaded = rec.load_persisted()
    assert loaded == 1
    assert rec.records[0].intent_id == "good"


# ── Thread safety ────────────────────────────────────────────────────────


def test_concurrent_intent_recording_thread_safe(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path, max_pending=10_000)

    def worker(start: int):
        for i in range(start, start + 50):
            rec.record_intent(
                intent_id=f"i{i}", signal_price=1.0 + i * 0.00001,
                side="BUY", symbol="GBPUSD",
                signal_timestamp_utc=f"t{i}",
            )

    threads = [threading.Thread(target=worker, args=(i * 50,)) for i in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert rec.pending_count == 200


def test_concurrent_intent_and_fill_thread_safe(tmp_path):
    rec = SlippageRecorder(front_id="front_4", log_root=tmp_path, max_pending=10_000)

    # Pre-populate intents
    for i in range(100):
        rec.record_intent(
            intent_id=f"i{i}", signal_price=1.0, side="BUY",
            symbol="GBPUSD", signal_timestamp_utc=f"t{i}",
        )

    def fill_worker(start: int, end: int):
        for i in range(start, end):
            rec.record_fill(
                intent_id=f"i{i}", fill_price=1.001,
                fill_timestamp_utc=f"t{i}+1",
            )

    threads = [
        threading.Thread(target=fill_worker, args=(0, 50)),
        threading.Thread(target=fill_worker, args=(50, 100)),
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    assert rec.pending_count == 0
    assert len(rec.records) == 100
