"""
Unit tests for trading_bot.core.state.state_store + reconciliation.

Uses an in-memory SQLite DB per test (file:memdb_<n>?mode=memory&cache=shared
isn't necessary — tmp_path gives each test its own file fixture for full
isolation including WAL mode).
"""
from __future__ import annotations

import pathlib

import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.state import (
    KILL_SWITCH_ACTIVE,
    KILL_SWITCH_TRIPPED,
    ORDER_STATUS_FILLED,
    ORDER_STATUS_PENDING,
    ORDER_STATUS_REJECTED,
    ORDER_STATUS_WORKING,
    Reconciler,
    RemoteOrder,
    RemotePosition,
    StateStore,
    compare_orders,
    compare_positions,
)
from trading_bot.core.state.state_store import _row_to_position  # noqa: F401  # ensure module imports


# ── Fixtures ──────────────────────────────────────────────────────────────
@pytest.fixture
def store(tmp_path: pathlib.Path) -> StateStore:
    db = tmp_path / "state.db"
    s = StateStore(db).open()
    yield s
    s.close()


# ── Schema init ───────────────────────────────────────────────────────────
def test_schema_init_creates_kill_switch_singleton(store: StateStore) -> None:
    ks = store.get_kill_switch()
    assert ks.state == KILL_SWITCH_ACTIVE
    assert ks.is_tripped is False


def test_schema_init_idempotent(tmp_path: pathlib.Path) -> None:
    db = tmp_path / "state.db"
    s1 = StateStore(db).open()
    s1.trip_kill_switch(reason="test")
    s1.close()
    # Re-open: schema init should NOT clobber the tripped state
    s2 = StateStore(db).open()
    assert s2.get_kill_switch().is_tripped
    s2.close()


# ── Positions ─────────────────────────────────────────────────────────────
def test_upsert_new_position(store: StateStore) -> None:
    p = store.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=5000.0,
    )
    assert p.symbol == "MESM26"
    assert p.opened_at == p.updated_at  # first insert: opened == updated

    fetched = store.get_position(symbol="MESM26", exchange="CME")
    assert fetched is not None
    assert fetched.quantity == 1.0


def test_upsert_existing_position_preserves_opened_at(store: StateStore) -> None:
    p1 = store.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=5000.0,
    )
    p2 = store.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=2.0, avg_price=5005.0,
    )
    assert p2.opened_at == p1.opened_at        # preserved
    assert p2.updated_at >= p1.updated_at      # advanced
    assert p2.quantity == 2.0


def test_close_position_returns_true_when_present(store: StateStore) -> None:
    store.upsert_position(
        symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, avg_price=5000.0,
    )
    assert store.close_position(symbol="MESM26", exchange="CME") is True
    assert store.get_position(symbol="MESM26", exchange="CME") is None


def test_close_position_returns_false_when_absent(store: StateStore) -> None:
    assert store.close_position(symbol="MESM26", exchange="CME") is False


def test_get_open_positions_returns_all(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=1, quantity=1, avg_price=5000)
    store.upsert_position(symbol="MGCM26", exchange="CME", side=2, quantity=1, avg_price=2400)
    rows = store.get_open_positions()
    assert {r.symbol for r in rows} == {"MESM26", "MGCM26"}


# ── Orders ────────────────────────────────────────────────────────────────
def test_record_order(store: StateStore) -> None:
    o = store.record_order(
        client_order_id="T-001", symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0, order_type=proto.ORDER_TYPE_MARKET,
    )
    assert o.status == ORDER_STATUS_PENDING
    fetched = store.get_order(client_order_id="T-001")
    assert fetched is not None
    assert fetched.client_order_id == "T-001"


def test_update_order_status_to_filled(store: StateStore) -> None:
    store.record_order(
        client_order_id="T-001", symbol="MESM26", exchange="CME",
        side=1, quantity=1.0, order_type=1,
    )
    updated = store.update_order_status(
        client_order_id="T-001", status=ORDER_STATUS_FILLED,
        fill_price=5001.5, fill_quantity=1.0,
    )
    assert updated is not None
    assert updated.status == ORDER_STATUS_FILLED
    assert updated.fill_price == 5001.5
    assert updated.filled_at is not None


def test_update_order_status_records_exit_telemetry(store: StateStore) -> None:
    store.record_order(
        client_order_id="T-EXIT", symbol="MESU26", exchange="CME",
        side=1, quantity=1.0, order_type=1,
    )
    store.update_order_status(
        client_order_id="T-EXIT", status=ORDER_STATUS_FILLED,
        fill_price=7492.50, fill_quantity=1.0,
    )
    updated = store.update_order_status(
        client_order_id="T-EXIT", status=ORDER_STATUS_FILLED,
        exit_price=7494.95,
        exit_quantity=1.0,
        exit_reason="TARGET_HIT",
        realized_pnl=12.25,
    )
    assert updated is not None
    assert updated.fill_price == 7492.50
    assert updated.exit_price == 7494.95
    assert updated.exit_quantity == 1.0
    assert updated.exit_reason == "TARGET_HIT"
    assert updated.realized_pnl == 12.25
    assert updated.exited_at is not None


def test_update_order_status_to_rejected_records_reason(store: StateStore) -> None:
    store.record_order(
        client_order_id="T-002", symbol="MESM26", exchange="CME",
        side=1, quantity=1.0, order_type=1,
    )
    updated = store.update_order_status(
        client_order_id="T-002", status=ORDER_STATUS_REJECTED,
        rejected_reason="margin insufficient",
    )
    assert updated is not None
    assert updated.status == ORDER_STATUS_REJECTED
    assert updated.rejected_reason == "margin insufficient"
    assert updated.fill_price is None


def test_update_order_status_returns_none_if_unknown(store: StateStore) -> None:
    assert store.update_order_status(
        client_order_id="missing", status=ORDER_STATUS_FILLED
    ) is None


def test_update_order_status_rejects_invalid_status(store: StateStore) -> None:
    store.record_order(
        client_order_id="T-003", symbol="MESM26", exchange="CME",
        side=1, quantity=1.0, order_type=1,
    )
    with pytest.raises(ValueError, match="invalid order status"):
        store.update_order_status(client_order_id="T-003", status="GIBBERISH")


def test_get_active_orders_filters_to_pending_and_working(store: StateStore) -> None:
    store.record_order(client_order_id="A", symbol="MES", exchange="CME", side=1, quantity=1, order_type=1)
    store.record_order(client_order_id="B", symbol="MES", exchange="CME", side=1, quantity=1, order_type=1)
    store.record_order(client_order_id="C", symbol="MES", exchange="CME", side=1, quantity=1, order_type=1)
    store.update_order_status(client_order_id="A", status=ORDER_STATUS_WORKING)
    store.update_order_status(client_order_id="B", status=ORDER_STATUS_FILLED, fill_price=5000, fill_quantity=1)
    # C remains PENDING

    active = store.get_active_orders()
    ids = {o.client_order_id for o in active}
    assert ids == {"A", "C"}


# ── Kill switch ───────────────────────────────────────────────────────────
def test_trip_and_reset_kill_switch(store: StateStore) -> None:
    initial = store.get_kill_switch()
    assert initial.state == KILL_SWITCH_ACTIVE

    tripped = store.trip_kill_switch(reason="-30% drawdown")
    assert tripped.state == KILL_SWITCH_TRIPPED
    assert tripped.reason == "-30% drawdown"
    assert tripped.is_tripped

    reset = store.reset_kill_switch()
    assert reset.state == KILL_SWITCH_ACTIVE
    assert reset.reason is None


def test_kill_switch_since_advances_only_on_state_change(store: StateStore) -> None:
    s1 = store.trip_kill_switch(reason="first")
    s2 = store.trip_kill_switch(reason="second")
    # Same state (TRIPPED → TRIPPED) — `since` should be preserved
    assert s2.since == s1.since
    assert s2.reason == "second"  # but reason updates
    s3 = store.reset_kill_switch()
    # State changed → since advances
    assert s3.since >= s1.since


# ── Account snapshots ─────────────────────────────────────────────────────
def test_record_and_fetch_latest_snapshot(store: StateStore) -> None:
    assert store.get_latest_account_snapshot() is None
    store.record_account_snapshot(nlv=1080.0, drawdown_pct=0.0)
    store.record_account_snapshot(nlv=1075.0, drawdown_pct=0.0046)
    latest = store.get_latest_account_snapshot()
    assert latest is not None
    assert latest.nlv == 1075.0


# ── Reconciliation ────────────────────────────────────────────────────────
def test_compare_positions_clean(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.BUY, quantity=1.0, avg_price=5000)
    locals_ = store.get_open_positions()
    remotes = [RemotePosition(symbol="MESM26", exchange="CME", quantity=1.0)]
    drifts = compare_positions(locals_, remotes)
    assert drifts == ()


def test_compare_positions_remote_only(store: StateStore) -> None:
    locals_ = []
    remotes = [RemotePosition(symbol="MESM26", exchange="CME", quantity=1.0)]
    drifts = compare_positions(locals_, remotes)
    assert len(drifts) == 1
    assert drifts[0].kind == "remote_only"
    assert drifts[0].delta == 1.0


def test_compare_positions_local_only(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.SELL, quantity=2.0, avg_price=5000)
    locals_ = store.get_open_positions()
    remotes: list[RemotePosition] = []
    drifts = compare_positions(locals_, remotes)
    assert len(drifts) == 1
    assert drifts[0].kind == "local_only"
    assert drifts[0].local_qty == -2.0   # SELL → negative signed qty


def test_compare_positions_size_mismatch(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.BUY, quantity=1.0, avg_price=5000)
    locals_ = store.get_open_positions()
    remotes = [RemotePosition(symbol="MESM26", exchange="CME", quantity=2.0)]
    drifts = compare_positions(locals_, remotes)
    assert len(drifts) == 1
    assert drifts[0].kind == "size_mismatch"
    assert drifts[0].delta == pytest.approx(1.0)


def test_compare_positions_tolerance(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.BUY, quantity=1.0, avg_price=5000)
    locals_ = store.get_open_positions()
    remotes = [RemotePosition(symbol="MESM26", exchange="CME", quantity=1.000001)]
    drifts = compare_positions(locals_, remotes, tolerance=1e-3)
    assert drifts == ()


def test_compare_orders_status_mismatch() -> None:
    locals_ = [
        # simulating Order objects via dataclass-compatible namespace
    ]
    # Build minimal Orders via StateStore for realism
    # (kept simple: use direct status compare via mock objects)

    class MiniOrder:
        def __init__(self, oid, status):
            self.client_order_id = oid
            self.status = status

    locals_ = [MiniOrder("A", ORDER_STATUS_WORKING), MiniOrder("B", ORDER_STATUS_FILLED)]
    remotes = [RemoteOrder(client_order_id="A", status=ORDER_STATUS_FILLED),
               RemoteOrder(client_order_id="B", status=ORDER_STATUS_FILLED)]
    drifts = compare_orders(locals_, remotes)
    assert len(drifts) == 1
    assert drifts[0].client_order_id == "A"
    assert drifts[0].kind == "status_mismatch"


def test_reconciler_full_clean(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.BUY, quantity=1.0, avg_price=5000)
    store.record_order(client_order_id="A", symbol="MESM26", exchange="CME", side=1, quantity=1, order_type=1)
    store.update_order_status(client_order_id="A", status=ORDER_STATUS_FILLED, fill_price=5000, fill_quantity=1)

    r = Reconciler()
    report = r.reconcile(
        local_positions=store.get_open_positions(),
        remote_positions=[RemotePosition(symbol="MESM26", exchange="CME", quantity=1.0)],
        local_orders=[store.get_order(client_order_id="A")],
        remote_orders=[RemoteOrder(client_order_id="A", status=ORDER_STATUS_FILLED)],
    )
    assert report.has_drift is False
    assert report.drift_count == 0


def test_reconciler_full_with_drift(store: StateStore) -> None:
    store.upsert_position(symbol="MESM26", exchange="CME", side=proto.BUY, quantity=1.0, avg_price=5000)
    store.record_order(client_order_id="A", symbol="MESM26", exchange="CME", side=1, quantity=1, order_type=1)

    r = Reconciler()
    report = r.reconcile(
        local_positions=store.get_open_positions(),
        # broker says 2 contracts, we have 1 — size mismatch
        remote_positions=[RemotePosition(symbol="MESM26", exchange="CME", quantity=2.0)],
        local_orders=[store.get_order(client_order_id="A")],
        # broker says order A is FILLED, we say PENDING — status mismatch
        remote_orders=[RemoteOrder(client_order_id="A", status=ORDER_STATUS_FILLED)],
    )
    assert report.has_drift is True
    assert report.drift_count == 2
    assert report.position_drifts[0].kind == "size_mismatch"
    assert report.order_drifts[0].kind == "status_mismatch"
