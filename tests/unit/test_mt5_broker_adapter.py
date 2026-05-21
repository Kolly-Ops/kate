from __future__ import annotations

import asyncio
import datetime as dt
import time
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


@pytest.fixture(autouse=True)
def _block_real_telegram_alerts(monkeypatch):
    """Replace push_telegram_alert with a no-op for every adapter test.

    Without this guard, the unit tests trigger real Telegram alerts on
    any workstation that has a valid `secrets.json` (the adapter's
    submit_order path calls push_telegram_alert on success). Live
    evidence 2026-05-21 14:12-14:13 BST: a single pytest run pushed
    `🟢 Kate ORDER FILLED — GBPUSD BUY qty=1.0 fill=1.25020 coid=orb-1
    ticket=123` and the matching SELL/stop-1 ticket=123 to the CEO's
    Telegram. Fixture data leaked to production alert channel because
    push_telegram_alert wasn't mocked.

    Autouse so every test in this module is protected.
    """
    monkeypatch.setattr(
        "trading_bot.core.execution.mt5_broker_adapter.push_telegram_alert",
        lambda *args, **kwargs: True,
    )


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

    TIMEFRAME_M1 = 1
    TIMEFRAME_M5 = 5
    TIMEFRAME_M15 = 15
    TIMEFRAME_M30 = 30
    TIMEFRAME_H1 = 16385

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
        # Bars returned by copy_rates_from_pos — tests override this to
        # exercise the history backfill path. Each entry must be a dict
        # supporting `b["time"|"open"|"high"|"low"|"close"|"tick_volume"]`
        # so the adapter's iteration works on real numpy records OR
        # dicts in test fixtures.
        self.rates: list[dict] = []
        self.copy_rates_calls = []

    def copy_rates_from_pos(self, symbol, timeframe, start_pos, count):
        self.copy_rates_calls.append((symbol, timeframe, start_pos, count))
        if not self.rates:
            return None
        return self.rates[start_pos: start_pos + count]

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
        expected_ts = dt.datetime.utcfromtimestamp(1_715_601_600.123)
        expected_ts -= dt.timedelta(seconds=adapter._mt5_server_offset_seconds)
        assert event.tick.timestamp == expected_ts

    _run(_impl())


def test_poll_loop_emits_tick_delta_after_subscription():
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
        await adapter.subscribe_market_data(symbol="GBPUSD")
        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        assert (await stream.__anext__()).kind == BrokerEventKind.MARKET_DATA_TICK

        runtime.tick = _Tick(
            time=1_715_601_601,
            time_msc=1_715_601_601_123,
            bid=1.2503,
            ask=1.2505,
            last=1.2504,
            volume=11.0,
            volume_real=11.0,
        )
        event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        assert event.kind == BrokerEventKind.MARKET_DATA_TICK
        assert event.tick is not None
        assert event.tick.last_price == pytest.approx(1.2504)

        await adapter.disconnect()

    _run(_impl())


def test_poll_loop_emits_error_when_subscribed_ticks_return_none():
    async def _impl():
        runtime = _FakeMT5()
        adapter = MT5BrokerAdapter(
            config=MT5Config(
                login=123456,
                password="pw",
                server="Demo-Server",
                poll_interval_seconds=0.01,
                market_data_stale_seconds=0.01,
            ),
            symbol_map=SYMBOL_MAP,
            runtime=runtime,
        )
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="GBPUSD")
        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        assert (await stream.__anext__()).kind == BrokerEventKind.MARKET_DATA_TICK

        runtime.tick = None
        event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        assert event.kind == BrokerEventKind.ERROR
        assert event.error_message is not None
        assert "no tick returned" in event.error_message

        await adapter.disconnect()

    _run(_impl())


def test_poll_loop_emits_error_when_subscribed_tick_is_stale():
    async def _impl():
        runtime = _FakeMT5()
        adapter = MT5BrokerAdapter(
            config=MT5Config(
                login=123456,
                password="pw",
                server="Demo-Server",
                poll_interval_seconds=0.01,
                market_data_stale_seconds=0.01,
            ),
            symbol_map=SYMBOL_MAP,
            runtime=runtime,
        )
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="GBPUSD")
        stream = adapter.events()
        assert (await stream.__anext__()).kind == BrokerEventKind.CONNECTED
        assert (await stream.__anext__()).kind == BrokerEventKind.MARKET_DATA_TICK

        event = None
        for _ in range(5):
            event = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
            if event.kind == BrokerEventKind.ERROR:
                break
        assert event is not None
        assert event.kind == BrokerEventKind.ERROR
        assert event.error_message is not None
        assert "tick unchanged" in event.error_message

        await adapter.disconnect()

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


def test_get_recent_candles_returns_bars_with_tz_corrected_timestamps():
    """Backfill: copy_rates_from_pos result -> chronological Candle tuple
    with timestamps normalized via the detected MT5 server offset.

    Regression guard for 2026-05-21 incident: 11 missed London sessions
    because the engine's strategy.history_window=480 was never met after
    restart. This method is the engine's startup seed path."""
    async def _impl() -> None:
        runtime = _FakeMT5()
        # Place the fake "current" tick at server time = real_utc + 3h.
        # The adapter's _detect_server_offset will round to +3h.
        now = int(time.time())
        runtime.tick = _Tick(
            bid=1.34000, ask=1.34002, last=0.0,
            volume=0, volume_real=0,
            time=now + 3 * 3600,
            time_msc=(now + 3 * 3600) * 1000,
        )
        # MT5 position 0 is the active, still-forming bar. Backfill must
        # start at position 1 so the strategy history receives only
        # completed candles. The completed bars are intentionally newest
        # first to assert the adapter returns chronological history.
        current_bar = {
            "time": now + 3 * 3600,
            "open": 9.9900,
            "high": 9.9900,
            "low": 9.9900,
            "close": 9.9900,
            "tick_volume": 999,
        }
        runtime.rates = [
            {
                "time": (now - i * 60) + 3 * 3600,
                "open": 1.3400 + i * 0.0001,
                "high": 1.3410 + i * 0.0001,
                "low": 1.3390 + i * 0.0001,
                "close": 1.3405 + i * 0.0001,
                "tick_volume": 100 + i,
            }
            for i in range(0, 5)  # newest completed first
        ]
        completed_bars_oldest_first = sorted(runtime.rates, key=lambda row: row["time"])
        runtime.rates = [current_bar] + runtime.rates

        adapter = _adapter(runtime)
        await adapter.connect()

        candles = await adapter.get_recent_candles(
            symbol="GBPUSD", count=5, timeframe_minutes=1,
        )

        assert len(candles) == 5, "should return all 5 backfilled bars"
        assert runtime.copy_rates_calls[-1] == ("GBPUSD", runtime.TIMEFRAME_M1, 1, 5)
        # Each candle's timestamp must be 3h behind the raw server epoch
        # (the offset detector rounds to +3h, so we subtract 3h exactly).
        for c, raw in zip(candles, completed_bars_oldest_first):
            expected_utc = dt.datetime.utcfromtimestamp(raw["time"] - 3 * 3600)
            assert c.timestamp == expected_utc, (
                f"timestamp mismatch: got {c.timestamp}, expected {expected_utc} "
                f"(raw_epoch={raw['time']})"
            )
            assert c.open == raw["open"]
            assert c.high == raw["high"]
            assert c.low == raw["low"]
            assert c.close == raw["close"]
            assert c.volume == raw["tick_volume"]
        assert all(c.open != current_bar["open"] for c in candles)

        await adapter.disconnect()

    _run(_impl())


def test_get_recent_candles_returns_empty_when_broker_has_no_history():
    """If copy_rates_from_pos returns None or empty, the method returns
    an empty tuple (NOT an error) so the engine falls back to live
    aggregation cleanly."""
    async def _impl() -> None:
        runtime = _FakeMT5()
        runtime.rates = []  # no history available

        adapter = _adapter(runtime)
        await adapter.connect()

        candles = await adapter.get_recent_candles(
            symbol="GBPUSD", count=480, timeframe_minutes=1,
        )

        assert candles == ()
        await adapter.disconnect()

    _run(_impl())


def test_get_recent_candles_rejects_unsupported_timeframe():
    """Defensive: timeframes not in the M1/M5/M15/M30/H1 map raise
    BrokerError instead of silently returning empty (which the engine
    would treat as 'no backfill available' and run blind for 8h)."""
    async def _impl() -> None:
        runtime = _FakeMT5()

        adapter = _adapter(runtime)
        await adapter.connect()

        try:
            await adapter.get_recent_candles(
                symbol="GBPUSD", count=10, timeframe_minutes=7,
            )
        except BrokerError as e:
            assert "timeframe_minutes=7" in str(e)
        else:
            raise AssertionError("expected BrokerError for timeframe_minutes=7")

        await adapter.disconnect()

    _run(_impl())
