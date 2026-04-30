"""
Unit tests for the bracket-order machinery in ManagedFuturesEngine.

After an entry market order fills, the engine should submit a STOP order
at intent.stop_loss and a LIMIT (target) order at intent.take_profit. When
either exit leg fills, the engine should cancel the sibling so the position
closes via exactly one of the two exit paths.

Tests use a recording-only fake DTCClient that captures submit/cancel calls
without speaking to a real Sierra. The synthetic ORDER_UPDATE bytes are
packed via the shared helper from test_dtc_protocol so we exercise the
real unpack path on the way in.
"""
from __future__ import annotations

import asyncio
import pathlib
from datetime import datetime, timezone
from typing import Optional

import pytest

from tests.unit.test_dtc_protocol import _pack_order_update
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.dtc_client import DTCMessage
from trading_bot.core.risk import RiskManager, RiskPolicy
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import AtrBreakoutStrategy
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


class FakeDTCClient:
    """Records submit_order / cancel_order calls. No network."""

    def __init__(self) -> None:
        self.submitted: list[dict] = []
        self.cancelled: list[dict] = []

    async def submit_order(self, **kwargs) -> str:
        self.submitted.append(kwargs)
        return kwargs["client_order_id"]

    async def cancel_order(self, **kwargs) -> None:
        self.cancelled.append(kwargs)


def _meta() -> InstrumentMeta:
    return InstrumentMeta(
        symbol="MESM26", exchange="CME",
        scid_filename="MESM26_FUT_CME", dtc_symbol="MESM26-CME",
        tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
    )


def _build_engine(tmp_path: pathlib.Path) -> tuple[ManagedFuturesEngine, FakeDTCClient, StateStore]:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeDTCClient()
    engine = ManagedFuturesEngine(
        symbols=["MESM26"],
        instruments={"MESM26": _meta()},
        candle_manager=None,                       # not used in these tests
        strategy=AtrBreakoutStrategy(),
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        dtc_client=fake,                           # type: ignore[arg-type]
        trade_account="",
        submit_trade_account="Sim1",
    )
    return engine, fake, state


@pytest.fixture
def engine_setup(tmp_path: pathlib.Path):
    engine, fake, state = _build_engine(tmp_path)
    try:
        yield engine, fake, state
    finally:
        state.close()


def _seed_entry_with_bracket(
    engine: ManagedFuturesEngine,
    *,
    entry_coid: str = "atrbo-MESM26-260430080000",
    entry_side: int = proto.BUY,
    stop_loss: float = 6100.0,
    take_profit: Optional[float] = 6150.0,
    quantity: float = 1.0,
) -> None:
    """Mimics what _submit_order does: records the entry in StateStore and
    caches a bracket spec keyed by the entry coid. Skips the actual DTC
    submit so the test can drive ORDER_UPDATE traffic directly."""
    engine.state.record_order(
        client_order_id=entry_coid,
        symbol="MESM26", exchange="CME",
        side=entry_side, quantity=quantity,
        order_type=proto.ORDER_TYPE_MARKET,
    )
    from trading_bot.engines.managed_futures_engine import _BracketSpec
    engine._pending_brackets[entry_coid] = _BracketSpec(
        entry_coid=entry_coid,
        dtc_symbol="MESM26-CME",
        exchange="CME",
        entry_side=entry_side,
        quantity=quantity,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )


# ── Entry-fill triggers bracket submission ──────────────────────────────
def test_entry_fill_submits_stop_and_target(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430080000"
    _seed_entry_with_bracket(
        engine, entry_coid=entry_coid,
        entry_side=proto.BUY, stop_loss=6100.0, take_profit=6150.0,
    )

    # Sierra reports the entry as FILLED
    payload = _pack_order_update(
        client_order_id=entry_coid,
        order_status=proto.ORDER_STATUS_FILLED,
        side=proto.BUY, filled_qty=1.0, remaining_qty=0.0,
        avg_fill_price=6125.0,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(
        engine._handle_dtc_message(msg)
    )

    # Two submits — STOP then TARGET, both opposite-side, correct prices
    assert len(fake.submitted) == 2

    stop = fake.submitted[0]
    assert stop["client_order_id"] == f"{entry_coid}-S"
    assert stop["order_type"] == proto.ORDER_TYPE_STOP
    assert stop["side"] == proto.SELL                  # close-side flips
    assert stop["price1"] == 6100.0
    assert stop["trade_account"] == "Sim1"
    assert stop["symbol"] == "MESM26-CME"

    target = fake.submitted[1]
    assert target["client_order_id"] == f"{entry_coid}-T"
    assert target["order_type"] == proto.ORDER_TYPE_LIMIT
    assert target["side"] == proto.SELL
    assert target["price1"] == 6150.0


def test_entry_fill_submits_only_stop_when_no_target(engine_setup) -> None:
    """take_profit is optional on TradeIntent. With it None, only the stop
    leg is submitted — no spurious LIMIT order at price 0."""
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430081000"
    _seed_entry_with_bracket(
        engine, entry_coid=entry_coid,
        stop_loss=6100.0, take_profit=None,
    )

    payload = _pack_order_update(
        client_order_id=entry_coid,
        order_status=proto.ORDER_STATUS_FILLED,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(engine._handle_dtc_message(msg))

    assert len(fake.submitted) == 1
    assert fake.submitted[0]["client_order_id"] == f"{entry_coid}-S"
    assert fake.submitted[0]["order_type"] == proto.ORDER_TYPE_STOP


def test_entry_fill_with_short_entry_flips_close_side_to_buy(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430082000"
    _seed_entry_with_bracket(
        engine, entry_coid=entry_coid,
        entry_side=proto.SELL, stop_loss=6160.0, take_profit=6100.0,
    )

    payload = _pack_order_update(
        client_order_id=entry_coid,
        order_status=proto.ORDER_STATUS_FILLED,
        side=proto.SELL,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(engine._handle_dtc_message(msg))

    assert all(s["side"] == proto.BUY for s in fake.submitted)


# ── Exit-leg fill cancels sibling ───────────────────────────────────────
def test_stop_fill_cancels_target(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430083000"
    stop_coid = f"{entry_coid}-S"
    target_coid = f"{entry_coid}-T"

    # Pre-wire: pretend brackets were already submitted and Sierra
    # acknowledged them. Engine state has the sibling map populated and
    # both child orders in StateStore.
    for coid, otype in ((stop_coid, proto.ORDER_TYPE_STOP),
                        (target_coid, proto.ORDER_TYPE_LIMIT)):
        engine.state.record_order(
            client_order_id=coid, symbol="MESM26", exchange="CME",
            side=proto.SELL, quantity=1.0, order_type=otype,
        )
    engine._sibling_orders[stop_coid] = target_coid
    engine._sibling_orders[target_coid] = stop_coid

    # Stop fills
    payload = _pack_order_update(
        client_order_id=stop_coid,
        order_status=proto.ORDER_STATUS_FILLED,
        side=proto.SELL, avg_fill_price=6098.5,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(engine._handle_dtc_message(msg))

    assert len(fake.cancelled) == 1
    assert fake.cancelled[0]["client_order_id"] == target_coid
    assert fake.cancelled[0]["trade_account"] == "Sim1"

    # Sibling map cleared in both directions — second fill won't double-cancel.
    assert stop_coid not in engine._sibling_orders
    assert target_coid not in engine._sibling_orders


def test_target_fill_cancels_stop(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430084000"
    stop_coid = f"{entry_coid}-S"
    target_coid = f"{entry_coid}-T"

    for coid, otype in ((stop_coid, proto.ORDER_TYPE_STOP),
                        (target_coid, proto.ORDER_TYPE_LIMIT)):
        engine.state.record_order(
            client_order_id=coid, symbol="MESM26", exchange="CME",
            side=proto.SELL, quantity=1.0, order_type=otype,
        )
    engine._sibling_orders[stop_coid] = target_coid
    engine._sibling_orders[target_coid] = stop_coid

    payload = _pack_order_update(
        client_order_id=target_coid,
        order_status=proto.ORDER_STATUS_FILLED,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(engine._handle_dtc_message(msg))

    assert len(fake.cancelled) == 1
    assert fake.cancelled[0]["client_order_id"] == stop_coid


def test_entry_without_stop_loss_does_not_seed_bracket(engine_setup) -> None:
    """If the strategy ever returns an intent without stop_loss (and the
    risk gate happens to let it through, e.g. require_stop_loss=False),
    the bracket machinery silently no-ops rather than crashing on a
    None stop price."""
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430085000"
    engine.state.record_order(
        client_order_id=entry_coid, symbol="MESM26", exchange="CME",
        side=proto.BUY, quantity=1.0,
        order_type=proto.ORDER_TYPE_MARKET,
    )
    # No bracket cached.

    payload = _pack_order_update(
        client_order_id=entry_coid,
        order_status=proto.ORDER_STATUS_FILLED,
    )
    msg = DTCMessage(msg_type=proto.ORDER_UPDATE, body=payload, received_at=0.0)
    asyncio.get_event_loop().run_until_complete(engine._handle_dtc_message(msg))

    assert fake.submitted == []
    assert fake.cancelled == []
