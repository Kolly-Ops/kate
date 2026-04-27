"""
Binary-mode mock DTC server for trading_bot.core.execution.dtc_client and
the ManagedFuturesEngine integration tests.

Behavior:
- LOGON_REQUEST → LOGON_RESPONSE (success). Response is intentionally PADDED
  beyond the legacy 238-byte struct so the client's variable-size unpack
  is exercised — regression path for COO Gemini's "unpack requires 238b"
  finding.
- HEARTBEAT → echo HEARTBEAT.
- SUBMIT_NEW_SINGLE_ORDER → properly-shaped ORDER_UPDATE filled
  (uses the real ORDER_UPDATE_FMT so the engine's unpacker is exercised).
- OPEN_ORDERS_REQUEST → ORDER_UPDATE with NoOrders=1 by default, OR a
  configurable list of fixture ORDER_UPDATEs.
- CURRENT_POSITIONS_REQUEST → POSITION_UPDATE with NoPositions=1 by
  default, OR a configurable list of fixture POSITION_UPDATEs.
- ACCOUNT_BALANCE_REQUEST → ACCOUNT_BALANCE_UPDATE with a configurable
  fixture (defaults to $1,080 NLV per CEO policy).
- MARKET_DATA_REQUEST → MARKET_DATA_SNAPSHOT (legacy support).

Test fixtures:
  set_account_balance(...) / set_positions([...]) / set_open_orders([...])
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
import time
from dataclasses import dataclass

from trading_bot.core.execution import dtc_protocol as proto

logger = logging.getLogger(__name__)


# ── Fixture shapes for tests to configure mock responses ──────────────────
@dataclass
class _AccountBalanceFixture:
    cash_balance: float = 1080.0
    balance_available: float = 980.0
    securities_value: float = 0.0
    margin_requirement: float = 100.0
    margin_full: float = 100.0
    daily_pnl: float = 0.0
    open_pnl: float = 0.0
    currency: str = "USD"
    trade_account: str = "E8933"
    daily_loss_limit_reached: bool = False
    is_under_required_margin: bool = False
    trading_disabled: bool = False
    is_under_required_account_value: bool = False


@dataclass
class _PositionFixture:
    symbol: str
    exchange: str = "CME"
    quantity: float = 0.0
    average_price: float = 0.0
    margin_requirement: float = 0.0
    open_pnl: float = 0.0
    trade_account: str = "E8933"


@dataclass
class _OrderFixture:
    client_order_id: str
    symbol: str = "MESM26"
    exchange: str = "CME"
    server_order_id: str = ""
    order_status: int = proto.ORDER_STATUS_FILLED
    order_update_reason: int = 0
    side: int = proto.BUY
    filled_quantity: float = 1.0
    remaining_quantity: float = 0.0
    average_fill_price: float = 5000.0
    info_text: str = ""
    trade_account: str = "E8933"


# ── The mock ──────────────────────────────────────────────────────────────
class BinaryMockDTCServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.port = port
        self._server: asyncio.base_events.Server | None = None

        # Captured inbound traffic for test assertions
        self.received_orders: list[bytes] = []
        self.received_logons: list[bytes] = []
        self.received_open_orders_requests: list[bytes] = []
        self.received_positions_requests: list[bytes] = []
        self.received_account_balance_requests: list[bytes] = []

        # Configurable response fixtures (test sets these)
        self._account_balance = _AccountBalanceFixture()
        self._positions: list[_PositionFixture] = []
        self._open_orders: list[_OrderFixture] = []

        # Pending order responses keyed by client_order_id — when the engine
        # submits an order matching a configured fixture, the mock replies
        # with that fixture's ORDER_UPDATE; otherwise sends a default fill.
        self._pending_order_responses: dict[str, _OrderFixture] = {}

    @property
    def actual_port(self) -> int:
        if not self._server:
            raise RuntimeError("server not running")
        return self._server.sockets[0].getsockname()[1]

    # ── Test-side fixture configuration ───────────────────────────────────
    def set_account_balance(self, **fields) -> None:
        self._account_balance = _AccountBalanceFixture(**fields)

    def set_positions(self, positions: list[_PositionFixture]) -> None:
        self._positions = list(positions)

    def set_open_orders(self, orders: list[_OrderFixture]) -> None:
        self._open_orders = list(orders)

    def configure_order_response(self, fixture: _OrderFixture) -> None:
        """When the engine submits an order with this client_order_id, the
        mock will reply with the configured ORDER_UPDATE shape."""
        self._pending_order_responses[fixture.client_order_id] = fixture

    # ── Server lifecycle ──────────────────────────────────────────────────
    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.debug("mock DTC client connected: %s", peer)
        try:
            while True:
                try:
                    header = await reader.readexactly(proto.HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    return
                size, msg_type = struct.unpack(proto.HEADER_FMT, header)
                body_len = max(0, size - proto.HEADER_SIZE)
                body = b""
                if body_len > 0:
                    try:
                        body = await reader.readexactly(body_len)
                    except asyncio.IncompleteReadError:
                        return
                await self._on_message(msg_type, header + body, writer)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _on_message(
        self, msg_type: int, full: bytes, writer: asyncio.StreamWriter
    ) -> None:
        if msg_type == proto.LOGON_REQUEST:
            self.received_logons.append(full)
            await self._send_logon_response(writer)
        elif msg_type == proto.HEARTBEAT:
            await self._send_heartbeat(writer)
        elif msg_type == proto.SUBMIT_NEW_SINGLE_ORDER:
            self.received_orders.append(full)
            await self._send_order_response_for_submit(writer, full)
        elif msg_type == proto.OPEN_ORDERS_REQUEST:
            self.received_open_orders_requests.append(full)
            await self._send_open_orders_snapshot(writer)
        elif msg_type == proto.CURRENT_POSITIONS_REQUEST:
            self.received_positions_requests.append(full)
            await self._send_positions_snapshot(writer)
        elif msg_type == proto.ACCOUNT_BALANCE_REQUEST:
            self.received_account_balance_requests.append(full)
            await self._send_account_balance_snapshot(writer)
        elif msg_type == proto.MARKET_DATA_REQUEST:
            await self._send_market_data_snapshot(writer, full)
        else:
            logger.debug("mock DTC: unhandled msg_type %d", msg_type)

    # ── Response builders ─────────────────────────────────────────────────
    async def _send_logon_response(self, writer: asyncio.StreamWriter) -> None:
        legacy = struct.pack(
            proto.LOGON_RESPONSE_LEGACY_FMT,
            0,                                # Size — overwritten below
            proto.LOGON_RESPONSE,
            8,                                # ProtocolVersion
            proto.LOGON_SUCCESS,              # Result
            b"OK",                            # ResultText[96]
            b"",                              # ReconnectAddress[64]
            0,                                # Integer_1
            b"BinaryMockDTC",                 # ServerName[60]
            1,                                # MarketDataIsSupported
            1,                                # TradingIsSupported
        )
        extra = b"\x00" * 14
        payload = legacy + extra
        size = len(payload)
        payload = struct.pack("<H", size) + payload[2:]
        writer.write(payload)
        await writer.drain()

    async def _send_heartbeat(self, writer: asyncio.StreamWriter) -> None:
        writer.write(proto.pack_heartbeat(timestamp=time.time()))
        await writer.drain()

    async def _send_order_response_for_submit(
        self, writer: asyncio.StreamWriter, submit_msg: bytes
    ) -> None:
        """Reply to SUBMIT_NEW_SINGLE_ORDER with a properly-shaped
        ORDER_UPDATE. Uses the configured fixture if the client_order_id
        matches; otherwise fills the order at the submitted Price1 (or 5000.0
        if market) by default."""
        # Extract client_order_id + side + qty from the submitted order
        try:
            sub = struct.unpack(
                proto.ORDER_FMT, submit_msg[:proto.ORDER_SIZE]
            )
            symbol_b, exchange_b, _ta_b, client_oid_b = sub[2], sub[3], sub[4], sub[5]
            order_type, side, price1, _price2, qty = sub[6], sub[7], sub[8], sub[9], sub[10]
            client_oid = client_oid_b.split(b"\x00", 1)[0].decode()
            symbol = symbol_b.split(b"\x00", 1)[0].decode()
            exchange = exchange_b.split(b"\x00", 1)[0].decode()
        except (struct.error, UnicodeDecodeError):
            client_oid = ""
            symbol = "UNKNOWN"
            exchange = "CME"
            side = proto.BUY
            qty = 1.0
            price1 = 0.0

        fixture = self._pending_order_responses.get(client_oid)
        if fixture is None:
            fill_price = price1 if price1 > 0 else 5000.0
            fixture = _OrderFixture(
                client_order_id=client_oid,
                symbol=symbol,
                exchange=exchange,
                side=side,
                filled_quantity=float(qty),
                remaining_quantity=0.0,
                average_fill_price=fill_price,
            )
        await self._send_order_update(writer, fixture)

    async def _send_open_orders_snapshot(
        self, writer: asyncio.StreamWriter
    ) -> None:
        """Respond to OPEN_ORDERS_REQUEST. Sends one ORDER_UPDATE per
        configured fixture, OR a single NoOrders=1 sentinel if none."""
        if not self._open_orders:
            sentinel = _OrderFixture(
                client_order_id="",
                order_status=proto.ORDER_STATUS_UNSPECIFIED,
            )
            await self._send_order_update(writer, sentinel, no_orders=True)
            return
        for fx in self._open_orders:
            await self._send_order_update(writer, fx)

    async def _send_order_update(
        self,
        writer: asyncio.StreamWriter,
        fx: _OrderFixture,
        *,
        no_orders: bool = False,
    ) -> None:
        payload = struct.pack(
            proto.ORDER_UPDATE_FMT,
            proto.ORDER_UPDATE_SIZE, proto.ORDER_UPDATE,
            0, 1, 1,                                              # RequestID, Total, MsgNum
            fx.symbol.encode(), fx.exchange.encode(),
            b"", fx.server_order_id.encode(),
            fx.client_order_id.encode(), b"",
            fx.order_status, fx.order_update_reason,
            proto.ORDER_TYPE_MARKET, fx.side,
            0.0, 0.0,                                             # Price1, Price2
            proto.TIME_IN_FORCE_DAY,
            0.0,                                                  # GoodTillDateTime
            fx.filled_quantity + fx.remaining_quantity,           # OrderQuantity
            fx.filled_quantity,
            fx.remaining_quantity,
            fx.average_fill_price,
            fx.average_fill_price,                                # LastFillPrice
            0,                                                    # LastFillDateTime
            fx.filled_quantity,                                   # LastFillQuantity
            b"",                                                  # LastFillExecutionID
            fx.trade_account.encode(),
            fx.info_text.encode(),
            1 if no_orders else 0,
            b"", b"",                                             # Parent, OCO
            0,                                                    # OpenOrClose
            b"",                                                  # PreviousClientOrderID
            b"",                                                  # FreeFormText
            0.0, 0.0,                                             # ReceivedDT, LatestTxnDT
            b"",                                                  # Username
        )
        writer.write(payload)
        await writer.drain()

    async def _send_positions_snapshot(
        self, writer: asyncio.StreamWriter
    ) -> None:
        if not self._positions:
            await self._send_position_update(
                writer,
                _PositionFixture(symbol="", exchange="", quantity=0.0),
                no_positions=True,
            )
            return
        for pos in self._positions:
            await self._send_position_update(writer, pos)

    async def _send_position_update(
        self,
        writer: asyncio.StreamWriter,
        fx: _PositionFixture,
        *,
        no_positions: bool = False,
    ) -> None:
        payload = struct.pack(
            proto.POSITION_UPDATE_FMT,
            proto.POSITION_UPDATE_SIZE, proto.POSITION_UPDATE,
            0, 1, 1,
            fx.symbol.encode(), fx.exchange.encode(),
            fx.quantity, fx.average_price,
            b"", fx.trade_account.encode(),
            1 if no_positions else 0, 0,
            fx.margin_requirement,
            0,                                                # EntryDateTime
            fx.open_pnl, 0.0, 0.0, 0.0, 0.0,
        )
        writer.write(payload)
        await writer.drain()

    async def _send_account_balance_snapshot(
        self, writer: asyncio.StreamWriter
    ) -> None:
        await self._send_account_balance_update(writer, self._account_balance)

    async def _send_account_balance_update(
        self,
        writer: asyncio.StreamWriter,
        fx: _AccountBalanceFixture,
    ) -> None:
        payload = struct.pack(
            proto.ACCOUNT_BALANCE_UPDATE_FMT,
            proto.ACCOUNT_BALANCE_UPDATE_SIZE, proto.ACCOUNT_BALANCE_UPDATE,
            0,                                                  # RequestID
            fx.cash_balance, fx.balance_available,
            fx.currency.encode(),
            fx.trade_account.encode(),
            fx.securities_value, fx.margin_requirement,
            1, 1,                                               # Total, MsgNum
            0, 0,                                               # NoBalances, Unsolicited
            fx.open_pnl, fx.daily_pnl,
            b"",                                                # InfoText
            0,                                                  # TransactionIdentifier
            0.0, 0.0,                                           # DailyNetLossLimit, Trailing
            int(fx.daily_loss_limit_reached),
            int(fx.is_under_required_margin),
            0,                                                  # ClosePositionsAtEndOfDay
            int(fx.trading_disabled),
            b"",                                                # Description
            int(fx.is_under_required_account_value),
            0,                                                  # TransactionDateTime
            fx.margin_full, 0.0, 0.0,
            b"",                                                # IntroducingBroker
        )
        writer.write(payload)
        await writer.drain()

    async def _send_market_data_snapshot(
        self, writer: asyncio.StreamWriter, request: bytes
    ) -> None:
        try:
            req = struct.unpack(
                proto.MD_REQUEST_FMT,
                request[: struct.calcsize(proto.MD_REQUEST_FMT)],
            )
            symbol_id = req[3]
        except struct.error:
            symbol_id = 0
        size = struct.calcsize(proto.MD_SNAPSHOT_FMT)
        payload = struct.pack(
            proto.MD_SNAPSHOT_FMT,
            size, proto.MARKET_DATA_SNAPSHOT,
            symbol_id,
            5000.0, 5010.0, 5050.0, 4990.0, 100000.0,
            500, 1000,
            10.0, 12.0,
            5005.0, 5004.5,
            5005.0, 1.0,
            0.0,
        )
        writer.write(payload)
        await writer.drain()


# Re-export fixture classes for tests
PositionFixture = _PositionFixture
OrderFixture = _OrderFixture
AccountBalanceFixture = _AccountBalanceFixture
