"""Sprint 2 #44 (2026-05-30) — engine.mark_session_traded callback.

Tests the engine-driven session-marking path via the _entry_intents
registry. Engine populates on entry submission, consumes on
FILL/PARTIAL_FILL (calls strategy.mark_session_traded), removes on
REJECTED/CANCELED (without calling).

Design: proposals/2026-05-30-claude-sprint2-44-engine-mark-session-traded-callback-design.md (v2)
Audit: Codex REVIEW-RESPONSE 2026-05-30 (HARD-OBJECTION on v1 _pending_brackets discriminator)
"""
from __future__ import annotations

import asyncio
import datetime as dt
import pathlib
import time
from typing import Any, Optional

import pytest

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BrokerEvent,
    BrokerEventKind,
    OrderEvent,
)
from trading_bot.core.risk import RiskManager, RiskPolicy
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy.base import Strategy, StrategyContext
from trading_bot.core.risk import TradeIntent
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine
from trading_bot.engines.managed_futures_engine import _EntryMarker


class _RecordingStrategy(Strategy):
    """Test double — records every mark_session_traded call without
    implementing real session logic. Lets us assert exactly when (and
    how often) the engine fires the callback."""

    def __init__(self, name: str = "test_strategy") -> None:
        self._name = name
        self.calls: list[tuple[str, dt.datetime]] = []
        self.raise_on_call = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def history_window(self) -> int:
        return 1

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        return None

    def mark_session_traded(self, symbol: str, timestamp_utc: dt.datetime) -> None:
        if self.raise_on_call:
            raise RuntimeError("simulated strategy failure")
        self.calls.append((symbol, timestamp_utc))


def _meta() -> InstrumentMeta:
    return InstrumentMeta(
        symbol="MESM26", exchange="CME",
        scid_filename="MESM26_FUT_CME", dtc_symbol="MESM26-CME",
        tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
    )


def _build_engine(tmp_path: pathlib.Path) -> tuple[
    ManagedFuturesEngine, FakeBrokerAdapter, StateStore, _RecordingStrategy,
]:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    strategy = _RecordingStrategy()
    engine = ManagedFuturesEngine(
        symbols=["MESM26"],
        instruments={"MESM26": _meta()},
        candle_manager=None,
        strategy=strategy,
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="",
    )
    return engine, fake, state, strategy


@pytest.fixture
def engine_setup(tmp_path: pathlib.Path):
    engine, fake, state, strategy = _build_engine(tmp_path)
    try:
        yield engine, fake, state, strategy
    finally:
        state.close()


def _seed_entry_marker(engine: ManagedFuturesEngine, coid: str = "test-entry-1") -> None:
    """Mimics what _submit_order does after successful broker submit:
    records the entry order in StateStore and adds an _EntryMarker."""
    engine.state.record_order(
        client_order_id=coid,
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0,
        order_type=proto.ORDER_TYPE_MARKET,
    )
    engine._entry_intents[coid] = _EntryMarker(
        symbol="MESM26",
        exchange="CME",
        strategy_name="test_strategy",
    )


def _make_event(
    kind: BrokerEventKind,
    *,
    client_order_id: str = "test-entry-1",
    fill_price: float = 6125.0,
    fill_quantity: float = 1.0,
    rejected_reason: Optional[str] = None,
) -> BrokerEvent:
    return BrokerEvent(
        kind=kind,
        received_at=time.time(),
        order=OrderEvent(
            client_order_id=client_order_id,
            symbol="MESM26",
            side=proto.BUY,
            quantity=fill_quantity,
            fill_price=fill_price if kind in (
                BrokerEventKind.ORDER_FILLED, BrokerEventKind.ORDER_PARTIAL_FILL,
            ) else None,
            fill_quantity=fill_quantity if kind in (
                BrokerEventKind.ORDER_FILLED, BrokerEventKind.ORDER_PARTIAL_FILL,
            ) else None,
            rejected_reason=rejected_reason,
        ),
    )


def _drive(engine: ManagedFuturesEngine, event: BrokerEvent) -> None:
    asyncio.get_event_loop().run_until_complete(engine._handle_broker_event(event))


# ── Tests ──────────────────────────────────────────────────────────────


def test_order_filled_consumes_marker_and_calls_strategy(engine_setup) -> None:
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)
    assert "test-entry-1" in engine._entry_intents

    _drive(engine, _make_event(BrokerEventKind.ORDER_FILLED))

    assert "test-entry-1" not in engine._entry_intents, "marker must be popped on FILL"
    assert len(strategy.calls) == 1, "strategy.mark_session_traded must be called exactly once"
    symbol, ts = strategy.calls[0]
    assert symbol == "MESM26"
    assert ts.tzinfo is not None, "engine must pass tz-aware UTC timestamp"
    assert ts.tzinfo == dt.timezone.utc


def test_order_partial_fill_consumes_marker_and_calls_strategy(engine_setup) -> None:
    """Per Codex 2026-05-30: partial fills represent real broker-accepted
    position exposure and consume the session same as full fills."""
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)

    _drive(engine, _make_event(BrokerEventKind.ORDER_PARTIAL_FILL, fill_quantity=0.5))

    assert "test-entry-1" not in engine._entry_intents
    assert len(strategy.calls) == 1


def test_order_ack_does_not_consume_marker(engine_setup) -> None:
    """ACK = alive at broker but not filled. Could still cancel out.
    Session must NOT be marked yet."""
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)

    _drive(engine, _make_event(BrokerEventKind.ORDER_ACK))

    assert "test-entry-1" in engine._entry_intents, "ACK must NOT pop the marker"
    assert len(strategy.calls) == 0, "ACK must NOT call mark_session_traded"


def test_order_rejected_removes_marker_without_calling(engine_setup) -> None:
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)

    _drive(engine, _make_event(
        BrokerEventKind.ORDER_REJECTED,
        rejected_reason="market closed",
    ))

    assert "test-entry-1" not in engine._entry_intents, (
        "REJECTED must remove the marker (otherwise it leaks)"
    )
    assert len(strategy.calls) == 0, (
        "REJECTED must NOT mark session — strategy should be able to re-attempt"
    )


def test_order_canceled_removes_marker_without_calling(engine_setup) -> None:
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)

    _drive(engine, _make_event(BrokerEventKind.ORDER_CANCELED))

    assert "test-entry-1" not in engine._entry_intents
    assert len(strategy.calls) == 0


def test_duplicate_fill_events_only_call_strategy_once(engine_setup) -> None:
    """Idempotence: if the broker emits duplicate FILL events for the same
    coid (rare but possible), the engine's pop semantics mean the second
    event finds no marker and skips the callback."""
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine)

    _drive(engine, _make_event(BrokerEventKind.ORDER_FILLED))
    _drive(engine, _make_event(BrokerEventKind.ORDER_FILLED))

    assert len(strategy.calls) == 1, "duplicate FILL must NOT double-call the strategy"


def test_exit_leg_fill_does_not_mark_session(engine_setup) -> None:
    """Bracket exit legs use derived coids (entry-S, entry-T). They have
    no entry marker, so their FILL events must not call mark_session_traded.

    This is the codex HARD-OBJECTION scenario inverted — we must not
    accidentally mark sessions from exit fills."""
    engine, _fake, _state, strategy = engine_setup
    _seed_entry_marker(engine, coid="test-entry-1")
    # Exit leg has its own coid — never registered in _entry_intents
    exit_event = _make_event(
        BrokerEventKind.ORDER_FILLED,
        client_order_id="test-entry-1-S",  # stop leg
    )

    _drive(engine, exit_event)

    # Entry marker untouched
    assert "test-entry-1" in engine._entry_intents
    # Strategy NOT called from exit
    assert len(strategy.calls) == 0


def test_strategy_exception_does_not_break_engine(engine_setup) -> None:
    """Engine must keep running even if strategy.mark_session_traded
    raises. The exception is logged and the marker is still popped."""
    engine, _fake, _state, strategy = engine_setup
    strategy.raise_on_call = True
    _seed_entry_marker(engine)

    # Should NOT propagate the exception
    _drive(engine, _make_event(BrokerEventKind.ORDER_FILLED))

    # Marker was already popped before the strategy call raised, so
    # _entry_intents is clean either way
    assert "test-entry-1" not in engine._entry_intents


def test_fill_for_unknown_coid_is_safe(engine_setup) -> None:
    """If a FILL event arrives for a coid never registered (e.g. recovered
    from a stale broker state), the pop returns None and nothing happens."""
    engine, _fake, _state, strategy = engine_setup
    # No _seed_entry_marker — registry is empty

    _drive(engine, _make_event(BrokerEventKind.ORDER_FILLED, client_order_id="unknown-coid"))

    assert len(strategy.calls) == 0, "no marker = no callback"
