from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass

import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BrokerAdapter,
    BrokerError,
    BrokerEventKind,
    BrokerSymbolSpec,
)
from trading_bot.core.execution.mt5_broker_adapter import MT5BrokerAdapter, MT5Config


@dataclass
class _AccountInfo:
    balance: float = 1000.0
    equity: float = 1012.5
    profit: float = 12.5
    margin: float = 25.0
    currency: str = "USD"


@dataclass
class _Position:
    symbol: str
    volume: float
    type: int
    price_open: float


@dataclass
class _Order:
    ticket: int
    symbol: str
    type: int
    volume_current: float
    volume_initial: float
    price_open: float
    comment: str


@dataclass
class _Tick:
    time: int = 1_715_601_600
    time_msc: int = 1_715_601_600_123
    bid: float = 1.2500
    ask: float = 1.2502
    last: float = 1.2501
    volume: float = 10.0
    volume_real: float = 10.0


@dataclass
class _Result:
    retcode: int
    order: int = 0
    deal: int = 0
    price: float = 0.0
    volume: float = 0.0
    comment: str = ""


class _FakeMT5:
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_PENDING = 5
    TRADE_ACTION_REMOVE = 8

    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TYPE_BUY_LIMIT = 2
    ORDER_TYPE_SELL_LIMIT = 3
    ORDER_TYPE_BUY_STOP = 4
    ORDER_TYPE_SELL_STOP = 5
    ORDER_TYPE_BUY_STOP_LIMIT = 6
    ORDER_TYPE_SELL_STOP_LIMIT = 7

    ORDER_TIME_GTC = 0
    ORDER_FILLING_RETURN = 2
    ORDER_FILLING_IOC = 1

    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1

    TRADE_RETCODE_PLACED = 10008
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_REJECT = 10006

    def __init__(self) -> None:
        self.initialized = False
        self.shutdown_called = False
        self.login_calls = []
        self.selected_symbols = []
        self.sent_orders = []
        self.next_order_result = _Result(
            retcode=self.TRADE_RETCODE_DONE,
            order=123,
            deal=456,
            price=1.2502,
            volume=1.0,
        )
        self.account = _AccountInfo()
        self.positions = [_Position("GBPUSD", 1.0, self.POSITION_TYPE_BUY, 1.2400)]
        self.orders = [_Order(999, "GBPUSD", self.ORDER_TYPE_BUY_LIMIT, 1.0, 1.0, 1.2300, "pending-1")]
        self.tick = _Tick()
        self.error = (0, "ok")

    def initialize(self, **kwargs):
        self.initialized = True
        self.initialize_kwargs = kwargs
        return True

    def shutdown(self):
        self.shutdown_called = True
        return True

    def login(self, login, **kwargs):
        self.login_calls.append((login, kwargs))
        return True

    def symbol_select(self, symbol, selected):
        self.selected_symbols.append((symbol, selected))
        return True

    def symbol_info_tick(self, symbol):
        return self.tick

    def order_send(self, request):
        self.sent_orders.append(request)
        return self.next_order_result

    def account_info(self):
        return self.account

    def positions_get(self):
        return self.positions

    def orders_get(self):
        return self.orders

    def last_error(self):
        return self.error


SYMBOL_MAP = {
    "GBPUSD": BrokerSymbolSpec(
        logical_symbol="GBPUSD",
        broker_symbol="GBPUSD",
        exchange="FX",
        tick_size=0.0001,
    ),
}


def _adapter(runtime: _FakeMT5) -> MT5BrokerAdapter:
    return MT5BrokerAdapter(
        config=MT5Config(
            login=123456,
            password="pw",
            server="Demo-Server",
            magic=42,
            deviation=3,
            poll_interval_seconds=60.0,
        ),
        symbol_map=SYMBOL_MAP,
        runtime=runtime,
    )


def _run(coro):
    return _get_or_create_loop().run_until_complete(coro)


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
        return loop
    except (RuntimeError, DeprecationWarning):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def test_adapter_is_broker_adapter():
    assert isinstance(_adapter(_FakeMT5()), BrokerAdapter)


def test_connect_initializes_and_selects_symbols():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        assert runtime.initialized is True
        assert runtime.initialize_kwargs["login"] == 123456
        assert runtime.initialize_kwargs["server"] == "Demo-Server"
        assert runtime.selected_symbols == [("GBPUSD", True)]

    _run(_impl())


def test_logon_calls_mt5_login_when_account_present():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        await adapter.logon(client_name="kate", trade_account="", username="654321", password="secret")
        assert runtime.login_calls == [(654321, {
            "password": "secret",
            "server": "Demo-Server",
            "timeout": 60000,
        })]

    _run(_impl())


def test_request_account_state_positions_and_orders():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        balance = await adapter.request_account_state(trade_account="")
        positions = await adapter.request_positions(trade_account="")
        orders = await adapter.request_open_orders(trade_account="")

        assert balance.nlv == 1012.5
        assert balance.margin_requirement == 25.0
        assert positions[0].symbol == "GBPUSD"
        assert positions[0].quantity == 1.0
        assert positions[0].avg_price == 1.24
        assert orders[0].client_order_id == "pending-1"
        assert orders[0].server_order_id == "999"

    _run(_impl())


def test_submit_market_buy_maps_to_mt5_deal_with_bracket_prices():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        await adapter.submit_order(
            client_order_id="orb-1",
            symbol="GBPUSD",
            exchange="FX",
            side=proto.BUY,
            quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
            stop_price=1.2450,
            target_price=1.2600,
        )
        request = runtime.sent_orders[0]
        assert request["action"] == runtime.TRADE_ACTION_DEAL
        assert request["type"] == runtime.ORDER_TYPE_BUY
        assert request["symbol"] == "GBPUSD"
        assert request["price"] == pytest.approx(1.2502)
        assert request["sl"] == pytest.approx(1.2450)
        assert request["tp"] == pytest.approx(1.2600)
        assert request["magic"] == 42
        assert request["deviation"] == 3
        assert request["type_filling"] == runtime.ORDER_FILLING_IOC

    _run(_impl())


def test_submit_pending_sell_stop_maps_to_mt5_pending():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        await adapter.submit_order(
            client_order_id="stop-1",
            symbol="GBPUSD",
            exchange="FX",
            side=proto.SELL,
            quantity=1.0,
            order_type=proto.ORDER_TYPE_STOP,
            price=1.2400,
        )
        request = runtime.sent_orders[0]
        assert request["action"] == runtime.TRADE_ACTION_PENDING
        assert request["type"] == runtime.ORDER_TYPE_SELL_STOP
        assert request["price"] == pytest.approx(1.2400)

    _run(_impl())


def test_submit_rejection_enqueues_rejected_event_and_raises():
    async def _impl():
        runtime = _FakeMT5()
        runtime.next_order_result = _Result(
            retcode=runtime.TRADE_RETCODE_REJECT,
            comment="market closed",
        )
        adapter = _adapter(runtime)
        await adapter.connect()
        with pytest.raises(BrokerError, match="market closed"):
            await adapter.submit_order(
                client_order_id="bad",
                symbol="GBPUSD",
                exchange="FX",
                side=proto.BUY,
                quantity=1.0,
                order_type=proto.ORDER_TYPE_MARKET,
            )

        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        event = await stream.__anext__()
        assert event.kind == BrokerEventKind.ORDER_REJECTED
        assert event.order is not None
        assert event.order.rejected_reason == "market closed"

    _run(_impl())


def test_subscribe_market_data_enqueues_normalized_tick():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="GBPUSD")
        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        event = await stream.__anext__()
        assert event.kind == BrokerEventKind.MARKET_DATA_TICK
        assert event.tick is not None
        assert event.tick.symbol == "GBPUSD"
        assert event.tick.last_price == pytest.approx(1.2501)
        assert event.tick.timestamp == dt.datetime.utcfromtimestamp(1_715_601_600.123)

    _run(_impl())


def test_cancel_order_requires_server_order_id():
    async def _impl():
        runtime = _FakeMT5()
        adapter = _adapter(runtime)
        await adapter.connect()
        with pytest.raises(BrokerError, match="server_order_id"):
            await adapter.cancel_order(client_order_id="x")

    _run(_impl())


def test_poll_loop_emits_position_delta():
    async def _impl():
        runtime = _FakeMT5()
        adapter = MT5BrokerAdapter(
            config=MT5Config(
                login=123456,
                password="pw",
                server="Demo-Server",
                poll_interval_seconds=0.01,
            ),
            symbol_map=SYMBOL_MAP,
            runtime=runtime,
        )
        await adapter.connect()
        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED

        runtime.positions = [_Position("GBPUSD", 2.0, runtime.POSITION_TYPE_BUY, 1.2410)]
        event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        assert event.kind == BrokerEventKind.POSITION_UPDATE
        assert event.position is not None
        assert event.position.quantity == 2.0

        await adapter.disconnect()

    _run(_impl())
