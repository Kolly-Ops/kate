"""TCP transport for the NT bridge — localhost NDJSON, HMAC-protected.

Per Codex's 2026-05-15 architecture scope. Python runs the TCP listener;
NinjaScript C# connects as the client. One client at a time — if NT
reconnects (process restart, mid-trade), the new connection replaces the
old; Python keeps running across NT-side outages.

Recovery semantics:
  - Python publisher dying = bridge dies. On reconnect, Python sends
    RECONCILE_REQ first thing to snapshot current ATM state.
  - NT shim dying = client connection drops. Python's send() raises
    NotConnectedError until NT reconnects. ATM brackets keep running
    on NT's side regardless (NT manages them internally).

Threading model: pure asyncio. Inbound messages are buffered to an
asyncio.Queue; consumer calls await server.receive().
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from .ninja_messages import (
    MsgType,
    WireEnvelope,
    build_envelope,
    decode_envelope,
    encode_envelope,
)

logger = logging.getLogger(__name__)


class NotConnectedError(RuntimeError):
    """Raised when send() is called with no client connected."""


class NinjaBridgeServer:
    """TCP server that accepts the NinjaScript bridge client.

    Single-client by design. If a new client connects while another is
    active, the old connection is dropped (last-write-wins). This matches
    expected NT lifecycle: NT process restart → new connection.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 9876,
        secret: bytes,
        sequence_start: int = 0,
    ) -> None:
        if not isinstance(secret, (bytes, bytearray)):
            raise TypeError("HMAC secret must be bytes")
        self._host = host
        self._port = port
        self._secret = bytes(secret)
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_writer: Optional[asyncio.StreamWriter] = None
        self._client_writer_lock = asyncio.Lock()
        self._inbound_q: asyncio.Queue[WireEnvelope] = asyncio.Queue()
        self._sequence = sequence_start
        self._client_connected = asyncio.Event()

    @property
    def is_listening(self) -> bool:
        return self._server is not None and self._server.is_serving()

    @property
    def is_client_connected(self) -> bool:
        return self._client_writer is not None and not self._client_writer.is_closing()

    @property
    def port(self) -> int:
        """Resolved port — useful when constructed with port=0 (auto-pick)."""
        if self._server is None:
            return self._port
        # Pull from underlying sockets — return the first bound port we find
        for sock in self._server.sockets or ():
            return sock.getsockname()[1]
        return self._port

    async def start(self) -> None:
        """Begin listening. Returns once the socket is bound.

        For loopback, bind BOTH IPv4 (127.0.0.1) and IPv6 (::1) so the
        NinjaScript client connects whether it dials "127.0.0.1" or
        "localhost" — on Windows "localhost" often resolves to ::1 first, and
        an IPv4-only bind would then silently refuse the connection (the client
        logs "connect failed" every retry while the server sees no client).
        """
        bind_host: Any = self._host
        if self._host in ("127.0.0.1", "localhost", "::1", "::"):
            bind_host = ["127.0.0.1", "::1"]
        self._server = await asyncio.start_server(
            self._handle_client, bind_host, self._port
        )
        logger.info("ninja-bridge listening on %s:%d", bind_host, self.port)

    async def stop(self) -> None:
        """Close any active client + stop listening."""
        async with self._client_writer_lock:
            if self._client_writer is not None:
                self._client_writer.close()
                try:
                    await self._client_writer.wait_closed()
                except Exception:
                    pass
                self._client_writer = None
        self._client_connected.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("ninja-bridge stopped")

    async def wait_for_client(self, timeout: Optional[float] = None) -> None:
        """Block until a client connects. Raises asyncio.TimeoutError if timeout."""
        await asyncio.wait_for(self._client_connected.wait(), timeout=timeout)

    async def send(self, msg_type: MsgType, payload: Any) -> int:
        """Send a message to the connected client. Returns the seq assigned.

        Raises NotConnectedError if no client is currently connected.
        Caller decides whether to buffer/retry — we do not silently drop.
        """
        async with self._client_writer_lock:
            if self._client_writer is None or self._client_writer.is_closing():
                raise NotConnectedError(
                    f"no NinjaScript client connected on {self._host}:{self.port}"
                )
            self._sequence += 1
            envelope = build_envelope(
                msg_type=msg_type,
                sequence=self._sequence,
                payload=payload,
                secret=self._secret,
            )
            wire = encode_envelope(envelope)
            self._client_writer.write(wire)
            await self._client_writer.drain()
            return self._sequence

    async def receive(self) -> WireEnvelope:
        """Yield the next received envelope. Blocks until one arrives."""
        return await self._inbound_q.get()

    def receive_nowait(self) -> Optional[WireEnvelope]:
        """Non-blocking receive. Returns None if queue is empty."""
        try:
            return self._inbound_q.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("ninja-bridge client connected: %s", peer)

        # Replace any existing client (last-write-wins on NT restart)
        async with self._client_writer_lock:
            if self._client_writer is not None and not self._client_writer.is_closing():
                logger.warning(
                    "ninja-bridge client replaced — closing previous connection"
                )
                self._client_writer.close()
            self._client_writer = writer
        self._client_connected.set()

        try:
            while True:
                try:
                    line = await reader.readline()
                except (ConnectionError, asyncio.IncompleteReadError):
                    break
                if not line:
                    break  # peer closed cleanly
                stripped = line.rstrip(b"\n")
                if not stripped:
                    continue
                try:
                    envelope = decode_envelope(stripped, secret=self._secret)
                except ValueError as exc:
                    logger.warning(
                        "ninja-bridge dropped malformed message from %s: %s", peer, exc
                    )
                    continue
                except Exception as exc:
                    logger.warning(
                        "ninja-bridge dropped unparseable message from %s: %s",
                        peer,
                        exc,
                    )
                    continue
                await self._inbound_q.put(envelope)
        finally:
            logger.info("ninja-bridge client disconnected: %s", peer)
            async with self._client_writer_lock:
                if self._client_writer is writer:
                    self._client_writer = None
                    self._client_connected.clear()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


__all__ = ["NinjaBridgeServer", "NotConnectedError"]
