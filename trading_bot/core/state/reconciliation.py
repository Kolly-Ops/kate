"""
Reconciliation — compare local StateStore snapshot vs broker ground truth.

The bot's StateStore is local-of-record but the broker (EdgeClear via DTC)
is the actual source of truth. Drift between the two is a critical signal:
either we missed an order update, or an order was filled differently than
we recorded, or — worst case — a position exists that we don't know about.

Per CLAUDE.md approval-gate matrix, position correction is NOT autonomous:
the reconciler reports drift, surfaces alerts, and stops new trades — but
it does NOT auto-correct positions. That requires CEO approval.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Iterable, Optional

from .state_store import Order, Position


# ── Drift records ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class PositionDrift:
    symbol: str
    exchange: str
    local_qty: float
    remote_qty: float

    @property
    def delta(self) -> float:
        return self.remote_qty - self.local_qty

    @property
    def kind(self) -> str:
        if self.local_qty == 0 and self.remote_qty != 0:
            return "remote_only"
        if self.remote_qty == 0 and self.local_qty != 0:
            return "local_only"
        return "size_mismatch"


@dataclass(frozen=True)
class OrderDrift:
    client_order_id: str
    local_status: Optional[str]
    remote_status: Optional[str]

    @property
    def kind(self) -> str:
        if self.local_status is None:
            return "remote_only"
        if self.remote_status is None:
            return "local_only"
        return "status_mismatch"


@dataclass(frozen=True)
class ReconciliationReport:
    timestamp: str
    position_drifts: tuple[PositionDrift, ...] = ()
    order_drifts: tuple[OrderDrift, ...] = ()

    @property
    def has_drift(self) -> bool:
        return bool(self.position_drifts) or bool(self.order_drifts)

    @property
    def drift_count(self) -> int:
        return len(self.position_drifts) + len(self.order_drifts)


# ── Remote (broker-side) snapshot shape ───────────────────────────────────
@dataclass(frozen=True)
class RemotePosition:
    """Broker-side position snapshot. The DTC adapter is responsible for
    populating these from POSITION_UPDATE messages."""

    symbol: str
    exchange: str
    quantity: float        # net signed quantity (positive = long, negative = short)


@dataclass(frozen=True)
class RemoteOrder:
    """Broker-side order snapshot."""

    client_order_id: str
    status: str    # using the same string codes as StateStore for clean compare


# ── Comparison primitives ─────────────────────────────────────────────────
def _signed_qty(position: Position) -> float:
    # side: 1 = BUY (long → positive), 2 = SELL (short → negative)
    sign = 1 if position.side == 1 else -1
    return sign * position.quantity


def compare_positions(
    local: Iterable[Position],
    remote: Iterable[RemotePosition],
    *,
    tolerance: float = 1e-6,
) -> tuple[PositionDrift, ...]:
    """Compare two position snapshots. Returns drift records for any
    (symbol, exchange) that differs by more than `tolerance`."""

    local_by_key = {(p.symbol, p.exchange): _signed_qty(p) for p in local}
    remote_by_key = {(r.symbol, r.exchange): r.quantity for r in remote}

    drifts: list[PositionDrift] = []
    for key in local_by_key.keys() | remote_by_key.keys():
        symbol, exchange = key
        lq = local_by_key.get(key, 0.0)
        rq = remote_by_key.get(key, 0.0)
        if abs(lq - rq) > tolerance:
            drifts.append(PositionDrift(
                symbol=symbol, exchange=exchange,
                local_qty=lq, remote_qty=rq,
            ))
    return tuple(sorted(drifts, key=lambda d: (d.symbol, d.exchange)))


def compare_orders(
    local: Iterable[Order],
    remote: Iterable[RemoteOrder],
) -> tuple[OrderDrift, ...]:
    """Compare two order snapshots. Returns drift for any client_order_id
    where local and remote disagree on status (or one side has no record).
    """
    local_by_id = {o.client_order_id: o.status for o in local}
    remote_by_id = {o.client_order_id: o.status for o in remote}

    drifts: list[OrderDrift] = []
    for oid in local_by_id.keys() | remote_by_id.keys():
        ls = local_by_id.get(oid)
        rs = remote_by_id.get(oid)
        if ls != rs:
            drifts.append(OrderDrift(
                client_order_id=oid, local_status=ls, remote_status=rs,
            ))
    return tuple(sorted(drifts, key=lambda d: d.client_order_id))


# ── Reconciler ────────────────────────────────────────────────────────────
class Reconciler:
    """Pure compare-and-report. Detects drift; does NOT auto-correct.

    Call sites are responsible for fetching `remote_positions` and
    `remote_orders` from the DTC client (POSITION_UPDATE / ORDER_UPDATE
    snapshots) and the local snapshots from the StateStore.
    """

    def reconcile(
        self,
        *,
        local_positions: Iterable[Position],
        remote_positions: Iterable[RemotePosition],
        local_orders: Iterable[Order],
        remote_orders: Iterable[RemoteOrder],
        position_tolerance: float = 1e-6,
    ) -> ReconciliationReport:
        position_drifts = compare_positions(
            local_positions, remote_positions, tolerance=position_tolerance,
        )
        order_drifts = compare_orders(local_orders, remote_orders)
        return ReconciliationReport(
            timestamp=dt.datetime.now(dt.timezone.utc).isoformat(),
            position_drifts=position_drifts,
            order_drifts=order_drifts,
        )
