"""
Unit tests for the bracket-order machinery in ManagedFuturesEngine.

After an entry market order fills, the engine should submit a STOP order
at intent.stop_loss and a LIMIT (target) order at intent.take_profit. When
either exit leg fills, the engine should cancel the sibling so the position
closes via exactly one of the two exit paths.

Post-refactor: engine depends on BrokerAdapter ABC, not DTCClient. Tests
use FakeBrokerAdapter (records submit/cancel calls, satisfies the ABC)
and drive engine state by calling `engine._handle_broker_event(event)`
with synthetic BrokerEvents. No raw DTC bytes — the BrokerAdapter ABC
already normalized those out.
"""
from __future__ import annotations

import asyncio
import pathlib
import time
from typing import Optional

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
from trading_bot.core.strategy import AtrBreakoutStrategy
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


def _meta() -> InstrumentMeta:
    return InstrumentMeta(
        symbol="MESM26", exchange="CME",
        scid_filename="MESM26_FUT_CME", dtc_symbol="MESM26-CME",
        tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
    )


def _build_engine(tmp_path: pathlib.Path) -> tuple[ManagedFuturesEngine, FakeBrokerAdapter, StateStore]:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    engine = ManagedFuturesEngine(
        symbols=["MESM26"],
        instruments={"MESM26": _meta()},
        candle_manager=None,                       # not used in these tests
        strategy=AtrBreakoutStrategy(),
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="",
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
    caches a bracket spec keyed by the entry coid. Skips the actual broker
    submit so the test can drive the fill event directly."""
    engine.state.record_order(
        client_order_id=entry_coid,
        symbol="MESM26", exchange="CME",
        side=entry_side, quantity=quantity,
        order_type=proto.ORDER_TYPE_MARKET,
    )
    from trading_bot.engines.managed_futures_engine import _BracketSpec
    engine._pending_brackets[entry_coid] = _BracketSpec(
        entry_coid=entry_coid,
        symbol="MESM26",                       # logical — adapter translates
        exchange="CME",
        entry_side=entry_side,
        quantity=quantity,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )


def _fill_event(
    *,
    client_order_id: str,
    side: int = proto.BUY,
    fill_price: float = 6125.0,
    fill_quantity: float = 1.0,
) -> BrokerEvent:
    """Construct a BrokerEvent of kind ORDER_FILLED with the given payload."""
    return BrokerEvent(
        kind=BrokerEventKind.ORDER_FILLED,
        received_at=time.time(),
        order=OrderEvent(
            client_order_id=client_order_id,
            symbol="MESM26",
            side=side,
            quantity=fill_quantity,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
        ),
    )


# ── Entry-fill triggers bracket submission ──────────────────────────────
def test_native_bracket_exit_updates_existing_order_telemetry(engine_setup) -> None:
    engine, _fake, state = engine_setup
    coid = "orb-MESU26-asi-260609035500"
    state.record_order(
        client_order_id=coid,
        symbol="MESU26",
        exchange="CME",
        side=proto.BUY,
        quantity=1.0,
        order_type=proto.ORDER_TYPE_MARKET,
    )
    state.update_order_status(
        client_order_id=coid,
        status="FILLED",
        fill_price=7492.50,
        fill_quantity=1.0,
    )

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(BrokerEvent(
            kind=BrokerEventKind.ORDER_FILLED,
            received_at=time.time(),
            order=OrderEvent(
                client_order_id=coid,
                symbol="MESU26",
                side=proto.BUY,
                quantity=1.0,
                fill_price=7494.95,
                fill_quantity=1.0,
                event_type="TARGET_HIT",
                exit_reason="TARGET_HIT",
                realized_pnl=12.25,
            ),
        ))
    )

    updated = state.get_order(client_order_id=coid)
    assert updated is not None
    assert updated.fill_price == 7492.50
    assert updated.exit_price == 7494.95
    assert updated.exit_reason == "TARGET_HIT"
    assert updated.realized_pnl == 12.25
    assert updated.exited_at is not None


def test_entry_fill_submits_stop_and_target(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430080000"
    _seed_entry_with_bracket(
        engine, entry_coid=entry_coid,
        entry_side=proto.BUY, stop_loss=6100.0, take_profit=6150.0,
    )

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(_fill_event(client_order_id=entry_coid))
    )

    # Two submits — STOP then TARGET, both opposite-side, correct prices
    assert len(fake.submitted) == 2

    stop = fake.submitted[0]
    assert stop["client_order_id"] == f"{entry_coid}-S"
    assert stop["order_type"] == proto.ORDER_TYPE_STOP
    assert stop["side"] == proto.SELL                  # close-side flips
    assert stop["price"] == 6100.0
    # Engine speaks logical symbols to the adapter; adapter does the
    # logical → broker translation internally.
    assert stop["symbol"] == "MESM26"

    target = fake.submitted[1]
    assert target["client_order_id"] == f"{entry_coid}-T"
    assert target["order_type"] == proto.ORDER_TYPE_LIMIT
    assert target["side"] == proto.SELL
    assert target["price"] == 6150.0


def test_entry_fill_submits_only_stop_when_no_target(engine_setup) -> None:
    """take_profit is optional on TradeIntent. With it None, only the stop
    leg is submitted — no spurious LIMIT order at price 0."""
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430081000"
    _seed_entry_with_bracket(
        engine, entry_coid=entry_coid,
        stop_loss=6100.0, take_profit=None,
    )

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(_fill_event(client_order_id=entry_coid))
    )

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

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(
            _fill_event(client_order_id=entry_coid, side=proto.SELL)
        )
    )

    assert all(s["side"] == proto.BUY for s in fake.submitted)


# ── Exit-leg fill cancels sibling ───────────────────────────────────────
def test_stop_fill_cancels_target(engine_setup) -> None:
    engine, fake, _state = engine_setup
    entry_coid = "atrbo-MESM26-260430083000"
    stop_coid = f"{entry_coid}-S"
    target_coid = f"{entry_coid}-T"

    # Pre-wire: pretend brackets were already submitted and the broker
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
    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(_fill_event(
            client_order_id=stop_coid, side=proto.SELL, fill_price=6098.5,
        ))
    )

    assert len(fake.cancelled) == 1
    assert fake.cancelled[0]["client_order_id"] == target_coid

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

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(_fill_event(client_order_id=target_coid))
    )

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

    asyncio.get_event_loop().run_until_complete(
        engine._handle_broker_event(_fill_event(client_order_id=entry_coid))
    )

    assert fake.submitted == []
    assert fake.cancelled == []
