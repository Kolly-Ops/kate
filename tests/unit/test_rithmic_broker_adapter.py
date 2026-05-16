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
from trading_bot.core.execution.rithmic_broker_adapter import (
    RithmicBrokerAdapter,
    RithmicConfig,
    _RithmicRuntime,
)


class _Event:
    def __init__(self) -> None:
        self.callbacks = []

    def __iadd__(self, callback):
        self.callbacks.append(callback)
        return self

    async def fire(self, data):
        for callback in self.callbacks:
            await callback(data)


class _Enum:
    AUTO = "AUTO"
    ORDER_PLANT = "ORDER_PLANT"
    PNL_PLANT = "PNL_PLANT"
    TICKER_PLANT = "TICKER_PLANT"
    LAST_TRADE = 1
    BBO = 2
    BUY = "BUY"
    SELL = "SELL"
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"


@dataclass
class _Account:
    account_id: str


class _FakeRithmicClient:
    last_instance = None

    def __init__(self, **kwargs) -> None:
        type(self).last_instance = self
        self.kwargs = kwargs
        self.accounts = [_Account("E8933")]
        self.connected_plants = None
        self.submitted_orders = []
        self.cancel_requests = []
        self.market_subscriptions = []
        self.on_tick = _Event()
        self.on_account_pnl_update = _Event()
        self.on_instrument_pnl_update = _Event()
        self.on_rithmic_order_notification = _Event()
        self.on_exchange_order_notification = _Event()
        self.on_bracket_update = _Event()
        self.on_disconnected = _Event()

    async def connect(self, *, plants):
        self.connected_plants = plants

    async def disconnect(self):
        return None

    async def get_front_month_contract(self, symbol, exchange):
        assert (symbol, exchange) == ("MES", "CME")
        return "MESM6"

    async def submit_order(self, **kwargs):
        self.submitted_orders.append(kwargs)
        return [{"ok": True}]

    async def cancel_order(self, **kwargs):
        self.cancel_requests.append(kwargs)

    async def subscribe_to_market_data(self, symbol, exchange, data_type):
        self.market_subscriptions.append((symbol, exchange, data_type))

    async def list_account_summary(self, *, account_id):
        assert account_id == "E8933"
        return [{
            "cash_balance": 1000.0,
            "net_liquidation_value": 1012.5,
            "open_position_pnl": 12.5,
            "margin_requirement": 100.0,
            "currency": "USD",
        }]

    async def list_positions(self, *, account_id):
        return [{
            "symbol": "MESM6",
            "quantity": 1,
            "average_price": 5000.25,
        }]

    async def list_orders(self, *, account_id):
        return [{
            "user_tag": "order-1",
            "symbol": "MESM6",
            "transaction_type": "BUY",
            "quantity": 1,
            "basket_id": "b-1",
        }]


SYMBOL_MAP = {
    "MESM26": BrokerSymbolSpec(
        logical_symbol="MESM26",
        broker_symbol="MES",
        exchange="CME",
        tick_size=0.25,
    ),
}


def _runtime() -> _RithmicRuntime:
    return _RithmicRuntime(
        RithmicClient=_FakeRithmicClient,
        SysInfraType=_Enum,
        DataType=_Enum,
        OrderPlacement=_Enum,
        OrderType=_Enum,
        TransactionType=_Enum,
    )


def _adapter() -> RithmicBrokerAdapter:
    return RithmicBrokerAdapter(
        config=RithmicConfig(
            user="user",
            password="pw",
            system_name="Rithmic Test",
            account_id="E8933",
        ),
        symbol_map=SYMBOL_MAP,
        runtime=_runtime(),
    )


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_adapter_is_broker_adapter():
    assert isinstance(_adapter(), BrokerAdapter)


def test_connect_uses_order_pnl_ticker_plants_and_account():
    async def _impl():
        adapter = _adapter()
        await adapter.connect()
        client = _FakeRithmicClient.last_instance
        assert client.connected_plants == ["ORDER_PLANT", "PNL_PLANT", "TICKER_PLANT"]
        assert client.kwargs["manual_or_auto"] == "AUTO"

    _run(_impl())


def test_submit_market_order_maps_symbol_side_and_native_bracket_ticks():
    async def _impl():
        adapter = _adapter()
        await adapter.connect()
        await adapter.submit_order(
            client_order_id="orb-1",
            symbol="MESM26",
            exchange="CME",
            side=proto.BUY,
            quantity=1.0,
            order_type=proto.ORDER_TYPE_MARKET,
            price=5000.0,
            stop_price=4997.5,
            target_price=5005.0,
        )
        order = _FakeRithmicClient.last_instance.submitted_orders[0]
        assert order["order_id"] == "orb-1"
        assert order["symbol"] == "MESM6"
        assert order["exchange"] == "CME"
        assert order["qty"] == 1
        assert order["transaction_type"] == "BUY"
        assert order["order_type"] == "MARKET"
        assert order["stop_ticks"] == 10
        assert order["target_ticks"] == 20
        assert order["account_id"] == "E8933"

    _run(_impl())


def test_submit_rejects_fractional_futures_quantity():
    async def _impl():
        adapter = _adapter()
        await adapter.connect()
        with pytest.raises(BrokerError, match="integer quantity"):
            await adapter.submit_order(
                client_order_id="bad",
                symbol="MESM26",
                exchange="CME",
                side=proto.BUY,
                quantity=0.5,
                order_type=proto.ORDER_TYPE_MARKET,
            )

    _run(_impl())


def test_seed_methods_return_typed_snapshots_and_enqueue_events():
    async def _impl():
        adapter = _adapter()
        await adapter.connect()
        balance = await adapter.request_account_state(trade_account="E8933")
        positions = await adapter.request_positions(trade_account="E8933")
        orders = await adapter.request_open_orders(trade_account="E8933")

        assert balance.nlv == 1012.5
        assert positions[0].symbol == "MESM26"
        assert positions[0].avg_price == 5000.25
        assert orders[0].client_order_id == "order-1"
        assert orders[0].symbol == "MESM26"

        stream = adapter.events()
        kinds = [await stream.__anext__() for _ in range(4)]
        assert [event.kind for event in kinds] == [
            BrokerEventKind.CONNECTED,
            BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
            BrokerEventKind.POSITION_UPDATE,
            BrokerEventKind.ORDER_ACK,
        ]

    _run(_impl())


def test_market_data_callback_normalizes_tick_event():
    async def _impl():
        adapter = _adapter()
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="MESM26", exchange="CME")
        client = _FakeRithmicClient.last_instance
        assert client.market_subscriptions == [("MESM6", "CME", 3)]

        await client.on_tick.fire({
            "symbol": "MESM6",
            "datetime": dt.datetime(2026, 5, 11, 12, 0, 1),
            "trade_price": 5000.25,
            "trade_size": 2,
            "bid_price": 5000.0,
            "ask_price": 5000.25,
        })

        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        event = await stream.__anext__()
        assert event.kind == BrokerEventKind.MARKET_DATA_TICK
        assert event.tick is not None
        assert event.tick.symbol == "MESM26"
        assert event.tick.last_price == 5000.25
        assert event.tick.last_size == 2

    _run(_impl())
