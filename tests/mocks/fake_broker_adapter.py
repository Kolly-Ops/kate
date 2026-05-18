"""
FakeBrokerAdapter — in-memory test double for the BrokerAdapter ABC.

Records submit/cancel calls (mirrors the existing FakeDTCClient pattern
in test_engine_brackets.py). Seed methods return configurable results.
The events() async iterator pulls from an asyncio.Queue that tests can
push to via inject_event(...) to simulate broker callbacks.

Usage:
    fake = FakeBrokerAdapter()
    fake.set_account_balance(AccountBalanceEvent(cash=1080, nlv=1080, pnl=0))
    fake.set_positions((PositionEvent(symbol="MESM26", quantity=1, avg_price=5000),))

    engine = ManagedFuturesEngine(..., broker=fake, ...)
    await engine.start()

    # Simulate Sierra sending an ORDER_FILLED for our submit
    fake.inject_event(BrokerEvent(
        kind=BrokerEventKind.ORDER_FILLED,
        received_at=time.time(),
        order=OrderEvent(
            client_order_id="o-1",
            symbol="MESM26",
            side=proto.BUY,
            quantity=1,
            fill_price=5000.0,
            fill_quantity=1,
        ),
    ))
    await asyncio.sleep(0.05)   # let the pump task forward it

    assert len(fake.submitted) == 3    # entry + bracket stop + bracket target
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterator, Optional

from trading_bot.core.execution.broker_adapter import (
    AccountBalanceEvent,
    BrokerAdapter,
    BrokerEvent,
    OrderEvent,
    PositionEvent,
)


class FakeBrokerAdapter(BrokerAdapter):
    """In-memory BrokerAdapter for unit tests.

    Records what the engine asks the broker to do (submit/cancel).
    Returns canned seed results that tests can configure. Lets tests
    push BrokerEvents onto the public events() stream to simulate
    asynchronous broker callbacks (fills, position updates, etc).
    """

    def __init__(
        self,
        *,
        account_balance: Optional[AccountBalanceEvent] = None,
        positions: tuple[PositionEvent, ...] = (),
        open_orders: tuple[OrderEvent, ...] = (),
    ) -> None:
        self._account_balance = account_balance or AccountBalanceEvent(
            cash=1080.0, nlv=1080.0, pnl=0.0,
        )
        self._positions = positions
        self._open_orders = open_orders

        # Submit/cancel call recordings — tests assert against these
        self.submitted: list[dict] = []
        self.cancelled: list[dict] = []
        self.subscriptions: list[dict] = []

        # Lifecycle flags
        self.connected: bool = False
        self.logged_on: bool = False
        self.logon_kwargs: dict = {}

        # Event stream
        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._closed = asyncio.Event()

    # ── Test-side configuration ──────────────────────────────────────────

    def set_account_balance(self, balance: AccountBalanceEvent) -> None:
        self._account_balance = balance

    def set_positions(self, positions: tuple[PositionEvent, ...]) -> None:
        self._positions = positions

    def set_open_orders(self, open_orders: tuple[OrderEvent, ...]) -> None:
        self._open_orders = open_orders

    def inject_event(self, event: BrokerEvent) -> None:
        """Push an event onto the public events() stream. Tests use this
        to simulate broker callbacks arriving asynchronously."""
        self._events_q.put_nowait(event)

    # ── BrokerAdapter contract ───────────────────────────────────────────

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False
        self._closed.set()

    async def logon(
        self,
        *,
        client_name: str,
        trade_account: str,
        username: str = "",
        password: str = "",
        demo: bool = True,
    ) -> None:
        self.logged_on = True
        self.logon_kwargs = dict(
            client_name=client_name, trade_account=trade_account,
            username=username, password=password, demo=demo,
        )

    async def submit_order(
        self,
        *,
        client_order_id: str,
        symbol: str,
        exchange: str,
        side: int,
        quantity: float,
        order_type: int,
        price: float = 0.0,
        stop_price: Optional[float] = None,
        target_price: Optional[float] = None,
        signal_close_price: Optional[float] = None,
        free_form_text: str = "",
    ) -> str:
        self.submitted.append({
            "client_order_id": client_order_id,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "quantity": quantity,
            "order_type": order_type,
            "price": price,
            "stop_price": stop_price,
            "target_price": target_price,
            "signal_close_price": signal_close_price,
            "free_form_text": free_form_text,
        })
        return client_order_id

    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        self.cancelled.append({
            "client_order_id": client_order_id,
            "server_order_id": server_order_id,
        })

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        self.subscriptions.append({
            "symbol": symbol,
            "exchange": exchange,
        })

    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        return self._account_balance

    async def request_positions(
        self,
        *,
        trade_account: str,
    ) -> tuple[PositionEvent, ...]:
        return self._positions

    async def request_open_orders(
        self,
        *,
        trade_account: str,
    ) -> tuple[OrderEvent, ...]:
        return self._open_orders

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while not self._closed.is_set():
            try:
                event = await self._events_q.get()
            except asyncio.CancelledError:
                return
            yield event


__all__ = ["FakeBrokerAdapter"]
