"""Slippage telemetry — measure realised entry vs signal-time close.

Default log location: `.mcp-brain/logs/slippage/` (per Gemini CFO/Ops
review 2026-05-16 — durable audit trail, survives local-app log rotation).

Motivation (FX backtest self-audit 2026-05-15):

The biggest methodological gap in the FX London Breakout backtest was
entry-at-close lookahead bias — the harness assumed fill at `bar.close`
of the signal bar, but in live trading the earliest fill is the NEXT
bar's open. On London-open breakouts the next-bar-open is typically
WORSE than the signal-bar-close (continuation momentum), so the
backtest systematically overstates edge. Estimate: 0.3-0.5 pip per
trade on GBPUSD; over 24-trade sample erodes 7-12 pips of headline
edge — meaningful relative to the +22.8 pip backtest result.

This module records per-trade telemetry so that within 30 live trades
we know whether the backtest edge survives real execution.

API shape (thread-safe):

  recorder = SlippageRecorder(front_id="front_4", log_root=Path("logs/slippage"))

  # When strategy fires the intent (signal time):
  recorder.record_intent(
      intent_id="fxlon-GBPUSD-260519-0755",
      signal_price=1.32675,
      side="BUY",
      symbol="GBPUSD",
      signal_timestamp_utc="2026-05-19T06:55:00+00:00",
  )

  # When broker confirms the fill (some seconds later):
  recorder.record_fill(
      intent_id="fxlon-GBPUSD-260519-0755",
      fill_price=1.32683,
      fill_timestamp_utc="2026-05-19T06:55:01+00:00",
  )
  # Above call triggers slippage computation + JSONL write.

  # Periodic summary (used by audit_live or runbook checks):
  print(recorder.summary())

Storage: one JSONL file per front at `<log_root>/<front_id>_slippage.jsonl`.
Each completed pair (intent matched with fill) writes one line. Intents
without fills stay in memory; the caller's responsibility to record fills.

Integration points (wired by supervisor / broker adapter, NOT here):
- Strategy intent path → call `record_intent` when intent is created
- Broker adapter fill path → call `record_fill` when ORDER_ACK with
  fill_price arrives
- Both calls correlate by `intent_id`
"""
from __future__ import annotations

import json
import logging
import math
import re
import statistics
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SlippageRecord:
    """One completed intent-to-fill pair."""
    intent_id: str
    front_id: str
    symbol: str
    side: str                       # "BUY" | "SELL"
    signal_price: float             # bar.close at on_candle_close time
    signal_timestamp_utc: str       # ISO 8601 UTC
    fill_price: float               # broker-reported realised entry
    fill_timestamp_utc: str         # ISO 8601 UTC
    slippage_price: float           # signed: positive = bad for trader
    slippage_pips: float            # in pip-units per the front's pip_size
    fill_latency_seconds: float     # signal → fill wall-clock

    def to_jsonl(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":")) + "\n"


@dataclass
class SlippageSummary:
    """Aggregate metrics over N completed records."""
    front_id: str
    n_pairs: int
    n_pending_intents: int
    mean_pips: float
    median_pips: float
    std_pips: float
    min_pips: float
    max_pips: float
    mean_latency_seconds: float

    def __repr__(self) -> str:
        return (
            f"SlippageSummary({self.front_id}: n={self.n_pairs}, "
            f"mean={self.mean_pips:.2f} pip, median={self.median_pips:.2f} pip, "
            f"std={self.std_pips:.2f}, latency_avg={self.mean_latency_seconds:.2f}s, "
            f"pending={self.n_pending_intents})"
        )


def _pip_units(side: str, signal_price: float, fill_price: float, pip_size: float) -> tuple[float, float]:
    """Return (slippage_price_signed, slippage_pips_signed).

    Convention: POSITIVE slippage = bad for the trader.
    - BUY: paid more than signal → positive slippage
    - SELL: received less than signal → positive slippage

    Defense-in-depth: rejects invalid pip_size up front rather than
    silently returning 0.0 (Codex AWC P1, 2026-05-16).
    """
    if pip_size <= 0:
        raise ValueError(f"pip_size must be > 0; got {pip_size}")
    if side.upper() == "BUY":
        delta = fill_price - signal_price
    elif side.upper() == "SELL":
        delta = signal_price - fill_price
    else:
        raise ValueError(f"unknown side: {side!r} (expected BUY or SELL)")
    return delta, delta / pip_size


class SlippageRecorder:
    """Thread-safe per-front slippage recorder.

    Holds pending intents in memory; writes one JSONL line per completed
    (intent + fill) pair. Designed for the supervisor + broker adapter
    to call from different threads.
    """

    def __init__(
        self,
        *,
        front_id: str,
        log_root: Path,
        pip_size: float = 0.0001,
        max_pending: int = 100,
    ) -> None:
        # Per Codex APPROVED-WITH-CONCERNS 2026-05-16:
        # P1 - invalid pip_size silently returning 0.0 = false confidence
        # P2 - max_pending <= 0 breaks eviction logic
        # P2 - front_id used in filename; sanitize against path escape
        if not front_id:
            raise ValueError("front_id is required")
        if not re.match(r"^[A-Za-z0-9_.-]+$", front_id):
            raise ValueError(
                f"front_id must match [A-Za-z0-9_.-]+; got {front_id!r}"
            )
        if pip_size <= 0:
            raise ValueError(f"pip_size must be > 0; got {pip_size}")
        if max_pending < 1:
            raise ValueError(f"max_pending must be >= 1; got {max_pending}")
        self.front_id = front_id
        self.log_root = Path(log_root)
        self.pip_size = pip_size
        self.max_pending = max_pending

        self._lock = threading.Lock()
        # intent_id -> (signal_price, side, symbol, signal_timestamp, signal_recv_time)
        self._pending: dict[str, tuple[float, str, str, str, float]] = {}
        # All completed records this session (also persisted to JSONL)
        self._records: list[SlippageRecord] = []

        self.log_root.mkdir(parents=True, exist_ok=True)
        self._jsonl_path = self.log_root / f"{front_id}_slippage.jsonl"

    @property
    def jsonl_path(self) -> Path:
        return self._jsonl_path

    @property
    def records(self) -> tuple[SlippageRecord, ...]:
        """Snapshot of completed records this session (read-only)."""
        with self._lock:
            return tuple(self._records)

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def record_intent(
        self,
        *,
        intent_id: str,
        signal_price: float,
        side: str,
        symbol: str,
        signal_timestamp_utc: str,
    ) -> None:
        """Called by the supervisor when the strategy returns a TradeIntent.

        Stores the intent in memory; pairs with a fill via record_fill().
        """
        if not intent_id:
            raise ValueError("intent_id is required")
        if signal_price <= 0:
            raise ValueError(f"signal_price must be > 0, got {signal_price}")
        if side.upper() not in {"BUY", "SELL"}:
            raise ValueError(f"side must be BUY or SELL, got {side!r}")

        with self._lock:
            if intent_id in self._pending:
                logger.warning(
                    "slippage: intent %s already pending — overwriting", intent_id
                )
            if len(self._pending) >= self.max_pending:
                # Evict oldest pending — protect against memory leak from
                # intents that never get a fill (rejected, cancelled)
                oldest = min(self._pending, key=lambda k: self._pending[k][4])
                self._pending.pop(oldest)
                logger.warning(
                    "slippage: pending intents at cap (%d) — evicted %s",
                    self.max_pending, oldest,
                )
            self._pending[intent_id] = (
                float(signal_price),
                side.upper(),
                symbol,
                signal_timestamp_utc,
                time.time(),
            )

    def record_fill(
        self,
        *,
        intent_id: str,
        fill_price: float,
        fill_timestamp_utc: str,
    ) -> Optional[SlippageRecord]:
        """Called by the broker adapter when the fill confirms.

        If a matching pending intent exists: computes slippage, writes a
        JSONL line, returns the SlippageRecord. If no match: logs a
        warning and returns None (fill without recorded intent — happens
        if intent recording was skipped for this trade).
        """
        if not intent_id:
            raise ValueError("intent_id is required")
        if fill_price <= 0:
            raise ValueError(f"fill_price must be > 0, got {fill_price}")

        with self._lock:
            pending = self._pending.pop(intent_id, None)

        if pending is None:
            logger.warning(
                "slippage: fill arrived for unknown intent %s (front %s) — skipping",
                intent_id, self.front_id,
            )
            return None

        signal_price, side, symbol, signal_ts, signal_recv = pending
        slippage_price, slippage_pips = _pip_units(side, signal_price, fill_price, self.pip_size)
        fill_recv = time.time()
        latency = max(0.0, fill_recv - signal_recv)

        record = SlippageRecord(
            intent_id=intent_id,
            front_id=self.front_id,
            symbol=symbol,
            side=side,
            signal_price=signal_price,
            signal_timestamp_utc=signal_ts,
            fill_price=float(fill_price),
            fill_timestamp_utc=fill_timestamp_utc,
            slippage_price=slippage_price,
            slippage_pips=slippage_pips,
            fill_latency_seconds=latency,
        )

        with self._lock:
            self._records.append(record)
            try:
                with self._jsonl_path.open("a", encoding="utf-8") as f:
                    f.write(record.to_jsonl())
            except OSError as exc:
                logger.error("slippage: failed to persist record %s: %s", intent_id, exc)

        return record

    def summary(self) -> SlippageSummary:
        """Aggregate metrics over all completed records this session."""
        with self._lock:
            pairs = list(self._records)
            pending = len(self._pending)

        if not pairs:
            return SlippageSummary(
                front_id=self.front_id,
                n_pairs=0,
                n_pending_intents=pending,
                mean_pips=0.0,
                median_pips=0.0,
                std_pips=0.0,
                min_pips=0.0,
                max_pips=0.0,
                mean_latency_seconds=0.0,
            )

        pips = [r.slippage_pips for r in pairs]
        latencies = [r.fill_latency_seconds for r in pairs]
        return SlippageSummary(
            front_id=self.front_id,
            n_pairs=len(pairs),
            n_pending_intents=pending,
            mean_pips=statistics.fmean(pips),
            median_pips=statistics.median(pips),
            std_pips=statistics.stdev(pips) if len(pips) > 1 else 0.0,
            min_pips=min(pips),
            max_pips=max(pips),
            mean_latency_seconds=statistics.fmean(latencies),
        )

    def load_persisted(self) -> int:
        """Rehydrate records from the JSONL file (e.g. on supervisor restart).

        Returns the number of records loaded. Does NOT re-emit them; just
        populates `self._records` so subsequent summaries reflect history.
        """
        if not self._jsonl_path.exists():
            return 0
        loaded = 0
        with self._lock:
            self._records.clear()
            for line in self._jsonl_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    self._records.append(SlippageRecord(**obj))
                    loaded += 1
                except (json.JSONDecodeError, TypeError) as exc:
                    logger.warning("slippage: skipping malformed line: %s", exc)
        return loaded


__all__ = ["SlippageRecorder", "SlippageRecord", "SlippageSummary"]
