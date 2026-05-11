"""
Unit tests for DTCBrokerAdapter — the first concrete BrokerAdapter,
wrapping DTCClient against Sierra Chart binary DTC v8.

Uses BinaryMockDTCServer (the same mock the existing DTC integration
tests rely on) to exercise the adapter end-to-end without a real Sierra.

Async pattern matches the rest of this project's unit tests: sync test
functions, async work driven via asyncio.run. Lets the tests run under
plain pytest without requiring pytest-asyncio.
"""
from __future__ import annotations

import asyncio
import struct
from typing import Awaitable, Callable, TypeVar

import pytest

from tests.mocks.mock_dtc_server import (
    BinaryMockDTCServer,
    OrderFixture,
    PositionFixture,
)
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BrokerAdapter,
    BrokerError,
    BrokerEventKind,
    BrokerSymbolSpec,
)
from trading_bot.core.execution.dtc_broker_adapter import DTCBrokerAdapter


SYMBOL_MAP = {
    "MESM26": BrokerSymbolSpec(
        logical_symbol="MESM26",
        broker_symbol="MESM26-CME",
        exchange="CME",
        tick_size=0.25,
    ),
}


# ── Async test harness ────────────────────────────────────────────────────

T = TypeVar("T")


def _run(coro: Callable[[BinaryMockDTCServer, DTCBrokerAdapter], Awaitable[T]]) -> T:
    """Spin up mock server + connected adapter, run the test coroutine,
    tear everything down. Uses run_until_complete on the current loop —
    matches the existing sync-test convention in this project (see
    test_engine_brackets.py). Avoids asyncio.run() which would close
    the loop and break subsequent tests' asyncio.get_event_loop() calls."""

    async def _impl() -> T:
        server = BinaryMockDTCServer(host="127.0.0.1", port=0)
        await server.start()
        try:
            adapter = DTCBrokerAdapter(
                host="127.0.0.1",
                port=server.actual_port,
                client_name="TEST_ADAPTER",
                trade_mode=proto.TRADE_MODE_DEMO,
                symbol_map=SYMBOL_MAP,
                submit_trade_account="Sim1",
                seed_timeout=2.0,
            )
            await adapter.connect()
            await adapter.logon(client_name="TEST_ADAPTER", trade_account="")
            try:
                return await coro(server, adapter)
            finally:
                await adapter.disconnect()
        finally:
            await server.stop()

    return _get_or_create_loop().run_until_complete(_impl())


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    """Return the current event loop, creating a fresh one if none
    exists or the previous one was closed. Insulates this test module
    from event-loop state left behind by other test modules."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
        return loop
    except (RuntimeError, DeprecationWarning):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


# ── Sanity: adapter satisfies the ABC and core lifecycle ──────────────────

def test_adapter_is_a_broker_adapter():
    a = DTCBrokerAdapter(
        host="127.0.0.1",
        port=11099,
        symbol_map=SYMBOL_MAP,
    )
    assert isinstance(a, BrokerAdapter)


def test_connect_failure_raises_broker_error():
    async def _impl() -> None:
        a = DTCBrokerAdapter(
            host="127.0.0.1",
            port=1,                          # closed port
            connect_timeout=0.5,
            symbol_map=SYMBOL_MAP,
        )
        with pytest.raises(BrokerError):
            await a.connect()

    _get_or_create_loop().run_until_complete(_impl())


def test_connect_and_logon_records_one_logon():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        # Both already happened in _run setup
        assert len(server.received_logons) == 1

    _run(_coro)


# ── Seed primitives ───────────────────────────────────────────────────────

def test_request_account_state_returns_typed_balance():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        server.set_account_balance(
            cash_balance=1080.0,
            margin_requirement=100.0,
            open_pnl=12.50,
            currency="USD",
        )
        balance = await adapter.request_account_state(trade_account="Sim1")
        assert balance.cash == 1080.0
        # NLV is cash + open_pnl (futures MTM equity per protocol docstring)
        assert balance.nlv == pytest.approx(1080.0 + 12.50)
        assert balance.margin_requirement == 100.0
        assert balance.currency == "USD"

    _run(_coro)


def test_request_account_state_fires_request_on_wire():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        # Sierra protocol quirk: seed requests must go with empty TradeAccount.
        # Adapter accepts the kwarg for ABC compliance but discards it on wire.
        await adapter.request_account_state(trade_account="E8933-LIVE-ACCOUNT")
        assert len(server.received_account_balance_requests) == 1

    _run(_coro)


def test_request_positions_flat_returns_empty_tuple():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        server.set_positions([])
        positions = await adapter.request_positions(trade_account="Sim1")
        assert positions == ()

    _run(_coro)


def test_request_positions_returns_typed_positions():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        server.set_positions([
            PositionFixture(
                symbol="MESM26-CME", exchange="CME", quantity=2.0,
                average_price=4998.25,
            ),
        ])
        positions = await adapter.request_positions(trade_account="Sim1")
        assert len(positions) == 1
        pos = positions[0]
        # Adapter MUST translate broker_symbol → logical_symbol on inbound
        assert pos.symbol == "MESM26"
        assert pos.quantity == 2.0
        assert pos.avg_price == pytest.approx(4998.25)

    _run(_coro)


def test_request_open_orders_flat_returns_empty_tuple():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        server.set_open_orders([])
        orders = await adapter.request_open_orders(trade_account="Sim1")
        assert orders == ()

    _run(_coro)


def test_request_open_orders_returns_typed_orders():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        server.set_open_orders([
            OrderFixture(
                client_order_id="abc-123",
                symbol="MESM26-CME",
                side=proto.BUY,
                order_status=proto.ORDER_STATUS_OPEN,
                filled_quantity=0.0,
                remaining_quantity=1.0,
            ),
        ])
        orders = await adapter.request_open_orders(trade_account="Sim1")
        assert len(orders) == 1
        assert orders[0].client_order_id == "abc-123"
        assert orders[0].symbol == "MESM26"        # logical, not dtc

    _run(_coro)


# ── Submit / cancel ───────────────────────────────────────────────────────

def test_submit_order_translates_logical_to_broker_symbol():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        coid = await adapter.submit_order(
            client_order_id="o-001",
            symbol="MESM26",
            exchange="CME",
            side=proto.BUY,
            quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
            free_form_text="orb-test",
        )
        assert coid == "o-001"
        # Poll briefly: TCP write completes before the mock server has
        # finished reading. Wait up to 1s for the server to record the
        # submission.
        for _ in range(50):
            if server.received_orders:
                break
            await asyncio.sleep(0.02)
        assert len(server.received_orders) == 1
        raw = server.received_orders[0]
        sub = struct.unpack(proto.ORDER_FMT, raw[:proto.ORDER_SIZE])
        symbol_on_wire = sub[2].split(b"\x00", 1)[0].decode()
        trade_account_on_wire = sub[4].split(b"\x00", 1)[0].decode()
        # Sierra DTC needs broker-form symbol on wire
        assert symbol_on_wire == "MESM26-CME"
        # submit_trade_account from constructor flows through (Sim1, not empty)
        assert trade_account_on_wire == "Sim1"

    _run(_coro)


def test_submit_order_unknown_symbol_raises():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        with pytest.raises(BrokerError, match="symbol_map"):
            await adapter.submit_order(
                client_order_id="o-001",
                symbol="ZZZZ99",                # not in symbol_map
                exchange="CME",
                side=proto.BUY,
                quantity=1.0,
                order_type=proto.ORDER_TYPE_MARKET,
            )

    _run(_coro)


# ── Event stream ──────────────────────────────────────────────────────────

def test_events_yields_filled_event_after_submit():
    """Mock fills the submission immediately. After submit_order, the
    pump should produce an ORDER_FILLED BrokerEvent on the events()
    stream."""

    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        stream = adapter.events()
        # CONNECTED + LOGON_OK already enqueued from _run setup
        pre_kinds: set[BrokerEventKind] = set()
        for _ in range(2):
            event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
            pre_kinds.add(event.kind)
        assert BrokerEventKind.CONNECTED in pre_kinds
        assert BrokerEventKind.LOGON_OK in pre_kinds

        await adapter.submit_order(
            client_order_id="o-fill",
            symbol="MESM26",
            exchange="CME",
            side=proto.BUY,
            quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
        )

        event = await asyncio.wait_for(stream.__anext__(), timeout=2.0)
        assert event.kind == BrokerEventKind.ORDER_FILLED
        assert event.order is not None
        assert event.order.client_order_id == "o-fill"
        assert event.order.symbol == "MESM26"       # logical, not dtc
        assert event.order.fill_quantity == 1.0

    _run(_coro)


# ── Market data: explicitly unsupported on DTC adapter ────────────────────

def test_subscribe_market_data_raises_not_implemented():
    async def _coro(server: BinaryMockDTCServer, adapter: DTCBrokerAdapter) -> None:
        with pytest.raises(NotImplementedError, match=r"\.scid"):
            await adapter.subscribe_market_data(symbol="MESM26", exchange="CME")

    _run(_coro)
