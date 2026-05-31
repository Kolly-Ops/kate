"""Sprint 3 (2026-05-31) — State Hygiene Preflight tests.

Per Codex HANDOFF 2026-05-30 (permanent fix for the DB-rotation pattern):
test the four-quadrant policy implemented in
ManagedFuturesEngine.run_state_hygiene_preflight().

  1. Local-only position + broker flat → AUTO-CLEAR
  2. Broker-only position + local missing → BLOCK STARTUP
  3. Local-pending order + broker absent → MARK STALE
  4. Broker-working order + local missing → BLOCK STARTUP

Plus:
  - No drift → proceed (healthy=True)
  - Mixed safe+unsafe → block (presence of any unsafe drift trumps repair)
  - WAL-backed DB opens cleanly and repair persists across reopen
"""
from __future__ import annotations

import asyncio
import pathlib
from typing import Optional

import pytest

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import BrokerError
from trading_bot.core.risk import RiskManager, RiskPolicy
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.state.reconciliation import RemoteOrder, RemotePosition
from trading_bot.core.strategy import AtrBreakoutStrategy
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


def _meta(symbol: str = "MESM26") -> InstrumentMeta:
    return InstrumentMeta(
        symbol=symbol, exchange="CME",
        scid_filename=f"{symbol}_FUT_CME", dtc_symbol=f"{symbol}-CME",
        tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
    )


def _build_engine(tmp_path: pathlib.Path, *, symbols: Optional[list[str]] = None) -> tuple[
    ManagedFuturesEngine, StateStore,
]:
    state = StateStore(tmp_path / "state.db").open()
    symbols = symbols or ["MESM26"]
    instruments = {s: _meta(s) for s in symbols}
    engine = ManagedFuturesEngine(
        symbols=symbols,
        instruments=instruments,
        candle_manager=None,
        strategy=AtrBreakoutStrategy(),
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=FakeBrokerAdapter(),
        trade_account="",
    )
    return engine, state


@pytest.fixture
def engine_setup(tmp_path: pathlib.Path):
    engine, state = _build_engine(tmp_path)
    try:
        yield engine, state
    finally:
        state.close()


# ── Quadrant 1: local-only position, broker flat → auto-clear ─────────


def test_local_only_position_broker_flat_is_auto_cleared(engine_setup) -> None:
    """Phantom-state pattern from Wed/Thu/Fri 2026-05-27/28/29. Local DB
    has positions, broker reports flat → preflight clears the locals."""
    engine, state = engine_setup
    state.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=6125.0,
    )
    assert len(state.get_open_positions()) == 1
    # broker is flat — engine._broker_positions is empty by default

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is False
    assert report.healthy is False  # we DID repair something
    assert report.cleared_positions == (("MESM26", "CME"),)
    assert report.marked_stale_orders == ()
    # Local position row is GONE
    assert state.get_open_positions() == []


def test_multiple_local_only_positions_all_cleared(engine_setup) -> None:
    """Friday's 4-phantom scenario in miniature."""
    engine, state = engine_setup
    for symbol in ("MESM26", "MNQM26"):
        state.upsert_position(
            symbol=symbol, exchange="CME",
            side=proto.BUY, quantity=1.0, avg_price=6125.0,
        )
    assert len(state.get_open_positions()) == 2

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is False
    assert set(report.cleared_positions) == {("MESM26", "CME"), ("MNQM26", "CME")}
    assert state.get_open_positions() == []


# ── Quadrant 2: broker-only position → block startup ───────────────────


def test_broker_only_position_blocks_startup(engine_setup) -> None:
    """Real broker exposure with no local record. Auto-correcting either
    direction is unsafe — block and force human review."""
    engine, state = engine_setup
    # Inject broker-side position; local is empty
    engine._broker_positions[("MESM26", "CME")] = RemotePosition(
        symbol="MESM26", exchange="CME", quantity=1.0,
    )

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is True
    assert "MESM26" in report.block_reason
    assert "no local record" in report.block_reason
    assert report.healthy is False
    # Critically: the broker position is NOT cleared (we don't touch broker state)
    assert ("MESM26", "CME") in engine._broker_positions


# ── Quadrant 3: local-only active order → mark stale ──────────────────


def test_local_pending_order_broker_absent_is_marked_stale(engine_setup) -> None:
    engine, state = engine_setup
    state.record_order(
        client_order_id="orphan-1",
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, order_type=proto.ORDER_TYPE_MARKET,
    )
    # Default status is PENDING; appears in get_active_orders
    assert len(state.get_active_orders()) == 1

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is False
    assert report.marked_stale_orders == ("orphan-1",)
    # Order is now CANCELLED — not in get_active_orders
    assert state.get_active_orders() == []
    # And the row itself reflects the stale reason
    order = state.get_order(client_order_id="orphan-1")
    assert order is not None
    assert order.status == "CANCELLED"
    assert order.rejected_reason is not None and "stale" in order.rejected_reason


# ── Quadrant 4: broker-working order, no local record → block ──────────


def test_broker_working_order_local_missing_blocks_startup(engine_setup) -> None:
    engine, state = engine_setup
    engine._broker_orders["broker-orphan-1"] = RemoteOrder(
        client_order_id="broker-orphan-1", status="WORKING",
    )

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is True
    assert "broker-orphan-1" in report.block_reason
    assert "working at broker" in report.block_reason.lower() or "no local record" in report.block_reason


# ── No-drift baseline ─────────────────────────────────────────────────


def test_no_drift_proceeds_healthy(engine_setup) -> None:
    """Clean state on both sides → healthy=True, block_trading=False,
    nothing repaired."""
    engine, _state = engine_setup
    # Both local and broker are empty by default

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is False
    assert report.healthy is True
    assert report.cleared_positions == ()
    assert report.marked_stale_orders == ()


# ── Mixed scenarios ───────────────────────────────────────────────────


def test_safe_repair_alongside_unsafe_drift_still_blocks(engine_setup) -> None:
    """Even if there's a hygiene-safe drift we CAN repair, the presence of
    ANY unsafe drift (broker-only position) must trip startup. Human review
    must come first; safe repairs can also wait until then."""
    engine, state = engine_setup
    # Safe drift: stale local position
    state.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=6125.0,
    )
    # Unsafe drift: broker-only position
    engine._broker_positions[("MNQM26", "CME")] = RemotePosition(
        symbol="MNQM26", exchange="CME", quantity=2.0,
    )

    report = engine.run_state_hygiene_preflight()

    assert report.block_trading is True
    assert "MNQM26" in report.block_reason
    # The safe-repair side STILL ran (idempotent + audit signal)
    # That's a deliberate policy choice — clean what we can, then halt.
    assert ("MESM26", "CME") in report.cleared_positions


# ── Broker-seed-failure guard (Codex P0, 2026-05-31) ──────────────────


class _FailingPositionsBroker(FakeBrokerAdapter):
    """Test double: raises BrokerError on request_positions(), succeeds
    everywhere else. Lets us prove engine.start() refuses to proceed
    when broker truth for positions is unavailable, so preflight never
    sees a falsely-empty broker_positions snapshot."""

    async def request_positions(self, *, trade_account: str):  # type: ignore[override]
        raise BrokerError("simulated broker outage: request_positions")


class _FailingOpenOrdersBroker(FakeBrokerAdapter):
    """Same shape, but fails on request_open_orders() instead."""

    async def request_open_orders(self, *, trade_account: str):  # type: ignore[override]
        raise BrokerError("simulated broker outage: request_open_orders")


def _build_engine_with(
    tmp_path: pathlib.Path, broker: FakeBrokerAdapter,
) -> tuple[ManagedFuturesEngine, StateStore]:
    state = StateStore(tmp_path / "state.db").open()
    engine = ManagedFuturesEngine(
        symbols=["MESM26"],
        instruments={"MESM26": _meta()},
        candle_manager=None,
        strategy=AtrBreakoutStrategy(),
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=broker,
        trade_account="",
    )
    return engine, state


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    """Python 3.13 deprecates implicit event-loop creation. Match the
    pattern in test_engine_session_marking.py / test_mt5_broker_adapter.py
    so this suite plays nicely when run alongside others."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
        return loop
    except (RuntimeError, DeprecationWarning):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def test_seed_positions_failure_raises_and_leaves_local_state_untouched(
    tmp_path: pathlib.Path,
) -> None:
    """Codex P0 (2026-05-31): if request_positions() fails, engine.start()
    MUST raise BrokerError. The State Hygiene Preflight never gets a
    chance to interpret the empty snapshot as 'broker flat' and clear
    real local positions."""
    engine, state = _build_engine_with(tmp_path, _FailingPositionsBroker())
    try:
        state.upsert_position(
            symbol="MESM26", exchange="CME",
            side=proto.BUY, quantity=1.0, avg_price=6125.0,
        )
        assert len(state.get_open_positions()) == 1

        with pytest.raises(BrokerError):
            _get_or_create_loop().run_until_complete(engine._seed_broker_state())

        # Critical assertion: the local position survives the failed start.
        # Auto-clearing here is what we're guarding against.
        assert len(state.get_open_positions()) == 1
    finally:
        state.close()


def test_seed_open_orders_failure_raises_and_leaves_local_state_untouched(
    tmp_path: pathlib.Path,
) -> None:
    """Codex P0 (2026-05-31): same guard for request_open_orders()."""
    engine, state = _build_engine_with(tmp_path, _FailingOpenOrdersBroker())
    try:
        state.record_order(
            client_order_id="real-working-order-1",
            symbol="MESM26", exchange="CME",
            side=proto.BUY, quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
        )
        assert len(state.get_active_orders()) == 1

        with pytest.raises(BrokerError):
            _get_or_create_loop().run_until_complete(engine._seed_broker_state())

        # The local active order MUST NOT have been auto-cancelled.
        assert len(state.get_active_orders()) == 1
        order = state.get_order(client_order_id="real-working-order-1")
        assert order is not None
        # status unchanged from the original PENDING
        assert order.status != "CANCELLED"
    finally:
        state.close()


def test_successful_empty_broker_snapshot_still_auto_clears_stale_local_position(
    tmp_path: pathlib.Path,
) -> None:
    """Codex P0 regression-guard: confirm the seed-failure guard didn't
    over-correct. The normal phantom-state path (broker SUCCESSFULLY
    reports flat + local has stale position) MUST still auto-clear."""
    fake = FakeBrokerAdapter()
    # No positions set → broker SUCCESSFULLY returns empty tuple from
    # request_positions(). This is the legitimate 'broker is flat' case.
    engine, state = _build_engine_with(tmp_path, fake)
    try:
        state.upsert_position(
            symbol="MESM26", exchange="CME",
            side=proto.BUY, quantity=1.0, avg_price=6125.0,
        )
        _get_or_create_loop().run_until_complete(engine._seed_broker_state())

        report = engine.run_state_hygiene_preflight()

        assert report.block_trading is False
        assert report.cleared_positions == (("MESM26", "CME"),)
        assert state.get_open_positions() == []
    finally:
        state.close()


def test_successful_empty_broker_snapshot_still_marks_stale_local_order(
    tmp_path: pathlib.Path,
) -> None:
    """Codex P0 regression-guard: same as above but for the orders path."""
    fake = FakeBrokerAdapter()
    engine, state = _build_engine_with(tmp_path, fake)
    try:
        state.record_order(
            client_order_id="orphan-after-restart",
            symbol="MESM26", exchange="CME",
            side=proto.BUY, quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
        )
        _get_or_create_loop().run_until_complete(engine._seed_broker_state())

        report = engine.run_state_hygiene_preflight()

        assert report.block_trading is False
        assert report.marked_stale_orders == ("orphan-after-restart",)
        assert state.get_active_orders() == []
    finally:
        state.close()


def test_position_clears_persist_across_db_reopen(tmp_path: pathlib.Path) -> None:
    """Verify the repair actually durables to disk: SQLite WAL/main file
    write-back must commit so a fresh StateStore opens to the cleaned
    state, not the pre-repair stale rows."""
    # Phase 1: open engine, seed local position, run preflight, close
    engine1, state1 = _build_engine(tmp_path)
    state1.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=6125.0,
    )
    assert len(state1.get_open_positions()) == 1
    report = engine1.run_state_hygiene_preflight()
    assert report.cleared_positions == (("MESM26", "CME"),)
    state1.close()

    # Phase 2: re-open the SAME db file in a fresh StateStore
    state2 = StateStore(tmp_path / "state.db").open()
    try:
        # Repair must have persisted
        assert state2.get_open_positions() == [], (
            "preflight repair did not durable: position row still present "
            "after StateStore.close() + reopen"
        )
    finally:
        state2.close()
