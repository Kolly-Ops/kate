"""
DTCBrokerAdapter — first concrete BrokerAdapter, wraps the existing
DTCClient (Sierra Chart binary DTC v8).

Why this exists
---------------
The platform pivot to Rithmic-direct (2026-05-09) gave the engine a new
broker target. To swap brokers cleanly the engine needs a
protocol-agnostic seat (the BrokerAdapter ABC) that any
implementation — Sierra DTC today, Rithmic tomorrow — can fill. This
file is the Sierra-side implementation. It does NOT change anything
about the running Sierra path; it wraps the same DTCClient the engine
already uses and exposes it through the ABC.

When the engine is refactored to depend on BrokerAdapter instead of
DTCClient directly (Step 3 of the migration plan in
broker_adapter.py's docstring), THIS adapter is the regression-safe
DTC seat. The engine refactor swaps `dtc_client=DTCClient(...)` for
`broker=DTCBrokerAdapter(...)` and nothing else about Sierra behaviour
should change.

What the adapter does
---------------------
1. Owns a DTCClient instance internally (host/port/connect lifecycle).
2. Translates DTC binary messages into normalized BrokerEvent stream:
     ORDER_UPDATE          → ORDER_ACK / ORDER_FILLED / ORDER_REJECTED /
                              ORDER_PARTIAL_FILL / ORDER_CANCELED
     POSITION_UPDATE       → POSITION_UPDATE
     ACCOUNT_BALANCE_UPDATE → ACCOUNT_BALANCE_UPDATE
     HEARTBEAT             → silently consumed (DTCClient handles itself)
3. Provides synchronous-style seed methods (`request_account_state`,
   `request_positions`, `request_open_orders`) that fire the request,
   wait for the matching response (or sentinel), and return a typed
   result. Side-effect: the same events also flow to the public
   events() stream so any post-seed listener stays in sync.
4. Handles Sierra's symbol-form quirks:
     - dtc_symbol (e.g. "MESM26-CME") goes on the wire
     - logical symbol (e.g. "MESM26") is what the engine speaks
     - reverse map translates inbound POSITION_UPDATE.symbol
5. Encapsulates Sierra's seed-request trade_account quirk: callers can
   pass the live account name to `request_account_state` etc., but the
   adapter always sends empty TradeAccount on the seed wire (per COO
   Gemini's 2026-04-27 capture — populating the field caused silent
   drops). SUBMIT goes out with the configured submit_trade_account.

What the adapter does NOT do
----------------------------
- Market data subscription via DTC. Kate sources MES ticks from the
  Sierra `.scid` file path (CandleManager), not from DTC. This adapter
  raises NotImplementedError on subscribe_market_data, matching the
  ABC's documented optional-override semantics. The Rithmic adapter
  WILL implement subscribe_market_data because it has no .scid fallback.
- Native bracket attachment. DTC has no native bracket protocol; the
  engine's existing `_pending_brackets` flow submits child stop/target
  orders separately after the entry fills. The adapter's submit_order
  ignores `stop_price`/`target_price` kwargs (with a debug log) — they
  exist on the ABC for Rithmic's stop_ticks/target_ticks. Caller submits
  the child legs as additional ORDER_TYPE_STOP / ORDER_TYPE_LIMIT calls.
- Auto-reconnect. DTCClient.connect() is idempotent but doesn't retry on
  failure. Higher-level supervisor logic handles disconnect→stop→restart.

Lifecycle
---------
1. construct: holds DTCClient (not yet connected), symbol_map, account
   config, but no network state
2. connect(): TCP connect to Sierra DTC port, spawn event-pump task
3. logon(): explicit DTC LOGON_REQUEST handshake (overrides ABC's
   no-op default — DTC needs this whereas Rithmic doesn't)
4. request_account_state / request_positions / request_open_orders:
   per-call seed primitives, each returns a typed result
5. submit_order / cancel_order: pass through to DTCClient with
   logical→broker symbol translation
6. events(): async iterator yielding BrokerEvent
7. disconnect(): cancel pump, close TCP socket
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import AsyncIterator, Optional

from . import dtc_protocol as proto
from .broker_adapter import (
    AccountBalanceEvent,
    BrokerAdapter,
    BrokerError,
    BrokerEvent,
    BrokerEventKind,
    BrokerSymbolSpec,
    MarketDataTick,
    OrderEvent,
    PositionEvent,
)
from .dtc_client import DTCClient, DTCError, DTCMessage

logger = logging.getLogger(__name__)


# ── DTC OrderStatusEnum → BrokerEventKind ─────────────────────────────────
#
# The engine cares about a small set of order outcomes: ACK (broker has it
# but no fill yet), FILLED, PARTIAL_FILL, REJECTED, CANCELED. Sierra has a
# wider enum (PENDING_OPEN, PENDING_CHILD, OPEN, etc.). Anything that
# means "broker accepted, not yet resolved" collapses to ORDER_ACK so the
# engine has one explicit event for "submission successful, waiting for
# fill". The unmapped middle states (cancel-replace pending etc.) also
# collapse to ORDER_ACK — they signal lifecycle activity without changing
# the order's fundamental fill state.
_STATUS_TO_KIND: dict[int, BrokerEventKind] = {
    proto.ORDER_STATUS_FILLED: BrokerEventKind.ORDER_FILLED,
    proto.ORDER_STATUS_PARTIALLY_FILLED: BrokerEventKind.ORDER_PARTIAL_FILL,
    proto.ORDER_STATUS_REJECTED: BrokerEventKind.ORDER_REJECTED,
    proto.ORDER_STATUS_CANCELED: BrokerEventKind.ORDER_CANCELED,
    proto.ORDER_STATUS_OPEN: BrokerEventKind.ORDER_ACK,
    proto.ORDER_STATUS_ORDER_SENT: BrokerEventKind.ORDER_ACK,
    proto.ORDER_STATUS_PENDING_OPEN: BrokerEventKind.ORDER_ACK,
    proto.ORDER_STATUS_PENDING_CHILD: BrokerEventKind.ORDER_ACK,
    proto.ORDER_STATUS_PENDING_CANCEL: BrokerEventKind.ORDER_ACK,
    proto.ORDER_STATUS_PENDING_CANCEL_REPLACE: BrokerEventKind.ORDER_ACK,
}


def _dtc_status_to_event_kind(status: int) -> BrokerEventKind:
    """Map Sierra OrderStatusEnum to the engine's normalized
    BrokerEventKind. Unknown values fall through to ORDER_ACK — same
    conservative posture as dtc_order_status_to_state_store: keep the
    order in the engine's view, let the reconciler surface any drift."""
    return _STATUS_TO_KIND.get(status, BrokerEventKind.ORDER_ACK)


class DTCBrokerAdapter(BrokerAdapter):
    """BrokerAdapter wrapping DTCClient for Sierra Chart.

    Construction:
        adapter = DTCBrokerAdapter(
            host="127.0.0.1",
            port=11099,
            client_name="OMNI_TRADING_BOT",
            trade_mode=proto.TRADE_MODE_DEMO,
            symbol_map={
                "MESM26": BrokerSymbolSpec(
                    logical_symbol="MESM26",
                    broker_symbol="MESM26-CME",
                    exchange="CME",
                    tick_size=0.25,
                ),
            },
            submit_trade_account="Sim1",
        )
        await adapter.connect()
        await adapter.logon(client_name="OMNI_TRADING_BOT", trade_account="")
        balance = await adapter.request_account_state(trade_account="")
        positions = await adapter.request_positions(trade_account="")
        orders = await adapter.request_open_orders(trade_account="")
        async for event in adapter.events():
            ...

    `submit_trade_account` is the account string sent on
    SUBMIT_NEW_SINGLE_ORDER. Sierra requires this to be non-empty in
    Trade Simulation Mode (verified 2026-04-29) — set it to "Sim1" for
    paper, the live account string (e.g. "E8933") for live mode.
    Seed-request wire frames go out with empty TradeAccount regardless;
    Sierra filters server-side by logon context.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int = 11099,
        client_name: str = "TRADING_BOT",
        trade_mode: int = proto.TRADE_MODE_DEMO,
        symbol_map: dict[str, BrokerSymbolSpec],
        submit_trade_account: str = "",
        connect_timeout: float = 10.0,
        seed_timeout: float = 5.0,
    ) -> None:
        self._client = DTCClient(host=host, port=port, connect_timeout=connect_timeout)
        self._client_name = client_name
        self._trade_mode = trade_mode
        self._submit_trade_account = submit_trade_account
        self._seed_timeout = seed_timeout

        self.symbol_map: dict[str, BrokerSymbolSpec] = dict(symbol_map)
        # Reverse map: broker_symbol → logical_symbol. Used to translate
        # inbound POSITION_UPDATE.symbol (which Sierra reports in
        # dtc_symbol form like "MESM26-CME") back to the logical symbol
        # the engine speaks.
        self._broker_to_logical: dict[str, str] = {
            spec.broker_symbol: spec.logical_symbol
            for spec in self.symbol_map.values()
        }

        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._pump_task: Optional[asyncio.Task[None]] = None
        self._closed = asyncio.Event()

        # Seed-response futures. Each seed method sets its future
        # immediately before sending the request; the pump task
        # completes the future when the matching response arrives.
        # `_positions_buf` / `_orders_buf` accumulate per-record events
        # between request and sentinel; they're cleared by the sentinel.
        self._seed_account_future: Optional[asyncio.Future[AccountBalanceEvent]] = None
        self._seed_positions_future: Optional[asyncio.Future[tuple[PositionEvent, ...]]] = None
        self._seed_positions_buf: list[PositionEvent] = []
        self._seed_orders_future: Optional[asyncio.Future[tuple[OrderEvent, ...]]] = None
        self._seed_orders_buf: list[OrderEvent] = []

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        try:
            await self._client.connect()
        except (asyncio.TimeoutError, OSError) as e:
            raise BrokerError(f"DTC connect failed: {e}") from e
        self._closed.clear()
        self._pump_task = asyncio.create_task(self._pump_loop(), name="dtc-adapter-pump")
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.CONNECTED,
            received_at=time.time(),
        ))

    async def disconnect(self) -> None:
        self._closed.set()
        if self._pump_task is not None:
            self._pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump_task
            self._pump_task = None
        await self._client.disconnect()
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.DISCONNECTED,
            received_at=time.time(),
        ))

    async def logon(
        self,
        *,
        client_name: str,
        trade_account: str,
        username: str = "",
        password: str = "",
        demo: bool = True,
    ) -> None:
        """Override the ABC's no-op logon — DTC needs an explicit
        LOGON_REQUEST handshake after connect().

        `trade_account` is accepted for ABC-signature compliance but
        the field is not sent on LOGON_REQUEST (Sierra infers context
        from `client_name` + the logon's `general_text`). It's still
        useful as documentation of which account this adapter
        instance is configured for.
        """
        try:
            await self._client.logon(
                client_name=client_name or self._client_name,
                trade_mode=self._trade_mode,
                username=username,
                password=password,
            )
        except DTCError as e:
            raise BrokerError(f"DTC logon failed: {e}") from e
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.LOGON_OK,
            received_at=time.time(),
        ))

    # ── Order submission & cancellation ──────────────────────────────────

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
        signal_close_price: Optional[float] = None,  # accepted; DTC slippage via .scid path, not adapter
        free_form_text: str = "",
    ) -> str:
        """Submit a single order via DTC SUBMIT_NEW_SINGLE_ORDER.

        `stop_price` and `target_price` are accepted for ABC-signature
        compliance but NOT acted on here — DTC has no native bracket
        attachment, so caller is responsible for submitting child
        stop/target legs as separate ORDER_TYPE_STOP / ORDER_TYPE_LIMIT
        calls after the entry fills. The engine's existing
        `_pending_brackets` flow does exactly this.
        """
        if stop_price is not None or target_price is not None:
            logger.debug(
                "DTCBrokerAdapter: submit_order received stop_price=%r "
                "target_price=%r — DTC has no native bracket; caller "
                "should submit child legs separately",
                stop_price, target_price,
            )

        spec = self.symbol_map.get(symbol)
        if spec is None:
            raise BrokerError(
                f"DTCBrokerAdapter: no symbol_map entry for logical "
                f"symbol {symbol!r}; cannot translate to DTC wire form"
            )

        try:
            return await self._client.submit_order(
                symbol=spec.broker_symbol,
                exchange=exchange or spec.exchange,
                trade_account=self._submit_trade_account,
                client_order_id=client_order_id,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price1=price,
                free_form_text=free_form_text,
            )
        except DTCError as e:
            raise BrokerError(f"DTC submit_order failed: {e}") from e

    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        try:
            await self._client.cancel_order(
                client_order_id=client_order_id,
                trade_account=self._submit_trade_account,
                server_order_id=server_order_id,
            )
        except DTCError as e:
            raise BrokerError(f"DTC cancel_order failed: {e}") from e

    # ── Market data: not provided by this adapter ────────────────────────

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        # Kate sources ticks from the Sierra .scid file path, not DTC.
        # See class docstring; default ABC behaviour suffices.
        raise NotImplementedError(
            "DTCBrokerAdapter does not provide market data — engine "
            "reads ticks from Sierra's .scid file directly via "
            "CandleManager. Use the Rithmic adapter when ticks must "
            "come over the broker connection."
        )

    # ── Seed primitives ──────────────────────────────────────────────────

    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        """Fire ACCOUNT_BALANCE_REQUEST and wait for the first
        ACCOUNT_BALANCE_UPDATE response.

        `trade_account` is accepted for ABC compliance but NOT sent on
        the wire — Sierra requires empty TradeAccount on seed
        requests (verified COO Gemini 2026-04-27; populating caused
        silent drops). The configured account context is sufficient
        for Sierra to route.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[AccountBalanceEvent] = loop.create_future()
        self._seed_account_future = fut
        try:
            await self._client.request_account_balance(request_id=1, trade_account="")
            return await asyncio.wait_for(fut, timeout=self._seed_timeout)
        except asyncio.TimeoutError as e:
            raise BrokerError(
                f"DTC request_account_state timed out after "
                f"{self._seed_timeout}s — Sierra did not respond with "
                f"ACCOUNT_BALANCE_UPDATE. Check Sierra's Trade Service "
                f"Log; common causes: trade service not initialised, "
                f"chartbook not loaded, account not selected."
            ) from e
        finally:
            self._seed_account_future = None

    async def request_positions(
        self,
        *,
        trade_account: str,
    ) -> tuple[PositionEvent, ...]:
        """Fire CURRENT_POSITIONS_REQUEST and return the position
        snapshot.

        Sierra's reply shape on real connections:
          - Flat account: a single POSITION_UPDATE with no_positions=1.
          - Non-flat: one POSITION_UPDATE per position, NO terminator
            sentinel afterward.

        Completion strategy uses both signals:
          - If the no_positions sentinel arrives, complete immediately
            (covers flat-account fast path).
          - Otherwise, settle on quiescence: once records have stopped
            arriving for `_seed_quiet_window` seconds, return what we
            have. Hard-bounded by `seed_timeout`.

        `trade_account` accepted for ABC compliance but not sent —
        Sierra seed convention requires empty.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[PositionEvent, ...]] = loop.create_future()
        self._seed_positions_future = fut
        self._seed_positions_buf.clear()
        try:
            await self._client.request_current_positions(request_id=2, trade_account="")
            return await self._await_seed_settle(
                fut=fut,
                buf=self._seed_positions_buf,
                what="POSITION_UPDATE",
            )
        finally:
            self._seed_positions_future = None
            self._seed_positions_buf.clear()

    async def request_open_orders(
        self,
        *,
        trade_account: str,
    ) -> tuple[OrderEvent, ...]:
        """Fire OPEN_ORDERS_REQUEST and return the open-orders snapshot.

        Same dual-completion strategy as `request_positions`: complete
        on `no_orders` sentinel if flat, else settle on quiescence.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[OrderEvent, ...]] = loop.create_future()
        self._seed_orders_future = fut
        self._seed_orders_buf.clear()
        try:
            await self._client.request_open_orders(request_id=3, trade_account="")
            return await self._await_seed_settle(
                fut=fut,
                buf=self._seed_orders_buf,
                what="ORDER_UPDATE",
            )
        finally:
            self._seed_orders_future = None
            self._seed_orders_buf.clear()

    async def _await_seed_settle(
        self,
        *,
        fut: "asyncio.Future[tuple]",
        buf: list,
        what: str,
        quiet_window: float = 0.15,
        poll_interval: float = 0.02,
    ) -> tuple:
        """Wait for a multi-record seed response to complete.

        Returns whichever happens first:
          - `fut` resolves (sentinel arrived → broker says "that's all")
          - Records arrive then stop for at least `quiet_window` seconds
          - Hard timeout (`seed_timeout`) elapses — returns whatever's
            in the buffer (possibly empty); no exception. Better to
            return a possibly-incomplete snapshot than fail the seed
            and prevent engine startup.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._seed_timeout
        last_count = -1
        last_change_at = loop.time()
        while True:
            if fut.done():
                return fut.result()
            now = loop.time()
            if now >= deadline:
                logger.warning(
                    "DTCBrokerAdapter: %s seed hit hard %.1fs timeout; "
                    "returning %d records collected so far",
                    what, self._seed_timeout, len(buf),
                )
                return tuple(buf)
            cur = len(buf)
            if cur != last_count:
                last_count = cur
                last_change_at = now
            elif cur > 0 and (now - last_change_at) >= quiet_window:
                # Records stopped arriving — Sierra is done sending.
                return tuple(buf)
            await asyncio.sleep(poll_interval)

    # ── Event stream ─────────────────────────────────────────────────────

    async def events(self) -> AsyncIterator[BrokerEvent]:
        """Yield normalized BrokerEvents until disconnect.

        Termination: when disconnect() is called, _closed is set AND a
        terminal DISCONNECTED event is enqueued. The loop yields that
        last event then exits.
        """
        while True:
            event = await self._events_q.get()
            yield event
            if event.kind == BrokerEventKind.DISCONNECTED:
                return

    # ── Internal: event pump ─────────────────────────────────────────────

    async def _pump_loop(self) -> None:
        """Pull DTCMessages from the underlying DTCClient, translate
        each into a BrokerEvent, dispatch to seed futures + public
        events queue.

        Runs from connect() until disconnect() cancels it. Exceptions
        inside the translator are logged but do not kill the loop —
        a single malformed message shouldn't break the engine."""
        try:
            while not self._closed.is_set():
                try:
                    msg = await self._client.recv_event()
                except asyncio.CancelledError:
                    raise
                try:
                    await self._handle_dtc_message(msg)
                except Exception:
                    logger.exception(
                        "DTCBrokerAdapter: pump dispatch failed for "
                        "msg_type=%d (continuing)",
                        msg.msg_type,
                    )
        except asyncio.CancelledError:
            return

    async def _handle_dtc_message(self, msg: DTCMessage) -> None:
        if msg.msg_type == proto.ORDER_UPDATE:
            await self._on_order_update(msg)
        elif msg.msg_type == proto.POSITION_UPDATE:
            await self._on_position_update(msg)
        elif msg.msg_type == proto.ACCOUNT_BALANCE_UPDATE:
            await self._on_account_balance_update(msg)
        elif msg.msg_type == proto.HEARTBEAT:
            pass  # DTCClient handles heartbeats internally
        else:
            logger.debug(
                "DTCBrokerAdapter: unhandled DTC msg_type=%d", msg.msg_type
            )

    async def _on_order_update(self, msg: DTCMessage) -> None:
        update = proto.unpack_order_update(msg.body)
        # Sentinel "no open orders" — terminates a request_open_orders
        # wait. Does NOT go on the public events stream (it's a seed
        # protocol artifact, not a real order event).
        if update.no_orders:
            if (
                self._seed_orders_future is not None
                and not self._seed_orders_future.done()
            ):
                self._seed_orders_future.set_result(
                    tuple(self._seed_orders_buf)
                )
                self._seed_orders_buf.clear()
            return

        kind = _dtc_status_to_event_kind(update.order_status)
        logical_symbol = self._broker_to_logical.get(update.symbol, update.symbol)
        order = OrderEvent(
            client_order_id=update.client_order_id,
            symbol=logical_symbol,
            side=update.side,
            quantity=update.filled_quantity + update.remaining_quantity,
            fill_price=update.average_fill_price if kind in (
                BrokerEventKind.ORDER_FILLED,
                BrokerEventKind.ORDER_PARTIAL_FILL,
            ) else None,
            fill_quantity=update.filled_quantity if kind in (
                BrokerEventKind.ORDER_FILLED,
                BrokerEventKind.ORDER_PARTIAL_FILL,
            ) else None,
            rejected_reason=update.info_text if kind == BrokerEventKind.ORDER_REJECTED else None,
            server_order_id=update.server_order_id or None,
        )

        # Accumulate into the seed buffer if a request_open_orders is
        # in-flight. The future is completed by the no_orders sentinel
        # branch above. Note: in practice this branch usually fires for
        # the unsolicited per-fill stream, not the seed reply — Sierra
        # sends ORDER_UPDATEs for active orders during seed AND for
        # real-time activity afterwards. We buffer regardless; if the
        # caller registered a future, the eventual sentinel completes it.
        if (
            self._seed_orders_future is not None
            and not self._seed_orders_future.done()
        ):
            self._seed_orders_buf.append(order)

        await self._events_q.put(BrokerEvent(
            kind=kind, received_at=msg.received_at, order=order,
        ))

    async def _on_position_update(self, msg: DTCMessage) -> None:
        update = proto.unpack_position_update(msg.body)
        if update.no_positions:
            if (
                self._seed_positions_future is not None
                and not self._seed_positions_future.done()
            ):
                self._seed_positions_future.set_result(
                    tuple(self._seed_positions_buf)
                )
                self._seed_positions_buf.clear()
            return

        logical_symbol = self._broker_to_logical.get(update.symbol, update.symbol)
        position = PositionEvent(
            symbol=logical_symbol,
            quantity=update.quantity,
            avg_price=update.average_price,
        )
        if (
            self._seed_positions_future is not None
            and not self._seed_positions_future.done()
        ):
            self._seed_positions_buf.append(position)
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.POSITION_UPDATE,
            received_at=msg.received_at,
            position=position,
        ))

    async def _on_account_balance_update(self, msg: DTCMessage) -> None:
        update = proto.unpack_account_balance_update(msg.body)
        balance = AccountBalanceEvent(
            cash=update.cash_balance,
            nlv=update.net_liquidation_value,
            pnl=update.open_positions_profit_loss,
            margin_requirement=update.margin_requirement,
            currency=update.account_currency or "USD",
        )
        if (
            self._seed_account_future is not None
            and not self._seed_account_future.done()
        ):
            self._seed_account_future.set_result(balance)
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
            received_at=msg.received_at,
            balance=balance,
        ))


__all__ = ["DTCBrokerAdapter"]
