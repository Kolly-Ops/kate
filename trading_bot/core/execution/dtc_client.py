"""
DTCClient — asyncio client for Sierra Chart DTC binary protocol.

Usage:
    async with DTCClient(host="127.0.0.1", port=11099) as client:
        resp = await client.logon(client_name="TRADING_BOT")
        await client.submit_order(symbol="MESM26", exchange="CME", ...)
        msg = await client.recv_event(timeout=10)
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from . import dtc_protocol as proto

logger = logging.getLogger(__name__)


class DTCError(Exception):
    """Raised when DTC protocol operations fail."""


@dataclass(frozen=True)
class DTCMessage:
    msg_type: int
    body: bytes
    received_at: float


class DTCClient:
    def __init__(
        self,
        host: str,
        port: int = 11099,
        *,
        connect_timeout: float = 10.0,
    ) -> None:
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._events: asyncio.Queue[DTCMessage] = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_interval = 10
        self._closed = asyncio.Event()

    async def __aenter__(self) -> "DTCClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.connect_timeout,
        )
        self._reader_task = asyncio.create_task(self._read_loop(), name="dtc-read-loop")
        logger.info("DTC connected to %s:%d", self.host, self.port)

    async def disconnect(self) -> None:
        self._closed.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._writer:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._writer = None
        self._reader = None
        logger.info("DTC disconnected")

    async def logon(
        self,
        *,
        client_name: str,
        trade_mode: int = proto.TRADE_MODE_DEMO,
        heartbeat_interval: int = 10,
        general_text: str = "",
        username: str = "",
        password: str = "",
        timeout: float = 10.0,
    ) -> proto.LogonResponse:
        if not self._writer:
            raise DTCError("not connected")
        self._heartbeat_interval = heartbeat_interval

        payload = proto.pack_logon_request(
            client_name=client_name,
            trade_mode=trade_mode,
            heartbeat_interval=heartbeat_interval,
            general_text=general_text,
            username=username,
            password=password,
        )
        self._writer.write(payload)
        await self._writer.drain()

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        deferred: list[DTCMessage] = []
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise DTCError("logon response timeout")
                msg = await asyncio.wait_for(self._events.get(), timeout=remaining)
                if msg.msg_type == proto.LOGON_RESPONSE:
                    resp = proto.unpack_logon_response(msg.body)
                    if resp.result_code != proto.LOGON_SUCCESS:
                        raise DTCError(
                            f"logon rejected: code={resp.result_code} "
                            f"text={resp.result_text!r}"
                        )
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(), name="dtc-heartbeat"
                    )
                    logger.info(
                        "DTC logon OK — server=%s mode=%d raw_size=%d",
                        resp.server_name, trade_mode, resp.raw_size,
                    )
                    return resp
                deferred.append(msg)
        finally:
            for m in deferred:
                self._events.put_nowait(m)

    async def submit_order(
        self,
        *,
        symbol: str,
        exchange: str,
        trade_account: str,
        client_order_id: str,
        side: int,
        quantity: float,
        order_type: int = proto.ORDER_TYPE_MARKET,
        price1: float = 0.0,
        price2: float = 0.0,
        time_in_force: int = proto.TIME_IN_FORCE_DAY,
        free_form_text: str = "",
    ) -> str:
        if not self._writer:
            raise DTCError("not connected")
        payload = proto.pack_submit_order(
            symbol=symbol,
            exchange=exchange,
            trade_account=trade_account,
            client_order_id=client_order_id,
            order_type=order_type,
            side=side,
            quantity=quantity,
            price1=price1,
            price2=price2,
            time_in_force=time_in_force,
            free_form_text=free_form_text,
        )
        self._writer.write(payload)
        await self._writer.drain()
        return client_order_id

    async def subscribe_market_data(
        self, symbol_id: int, symbol: str, *, exchange: str = ""
    ) -> None:
        if not self._writer:
            raise DTCError("not connected")
        payload = proto.pack_market_data_request(
            symbol_id=symbol_id,
            symbol=symbol,
            exchange=exchange,
            action=proto.REQUEST_ACTION_SUBSCRIBE,
        )
        self._writer.write(payload)
        await self._writer.drain()

    async def heartbeat(self) -> None:
        if not self._writer:
            return
        self._writer.write(proto.pack_heartbeat(timestamp=time.time()))
        await self._writer.drain()

    async def recv_event(self, *, timeout: Optional[float] = None) -> DTCMessage:
        if timeout is None:
            return await self._events.get()
        return await asyncio.wait_for(self._events.get(), timeout=timeout)

    async def events(self) -> AsyncIterator[DTCMessage]:
        while not self._closed.is_set():
            try:
                yield await self._events.get()
            except asyncio.CancelledError:
                break

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while not self._closed.is_set():
                try:
                    header = await self._reader.readexactly(proto.HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    logger.info("DTC server closed connection")
                    return
                size, msg_type = struct.unpack(proto.HEADER_FMT, header)
                body_len = max(0, size - proto.HEADER_SIZE)
                body = b""
                if body_len > 0:
                    try:
                        body = await self._reader.readexactly(body_len)
                    except asyncio.IncompleteReadError as e:
                        logger.warning("DTC incomplete body read: %s", e)
                        return
                full = header + body
                await self._events.put(
                    DTCMessage(msg_type=msg_type, body=full, received_at=time.time())
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("DTC read loop error")

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._closed.is_set():
                await asyncio.sleep(self._heartbeat_interval)
                if not self._closed.is_set():
                    with contextlib.suppress(Exception):
                        await self.heartbeat()
        except asyncio.CancelledError:
            raise
