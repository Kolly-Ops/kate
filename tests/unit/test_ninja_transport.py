"""Regression tests for the NT bridge TCP transport.

Spins up a real localhost server on an ephemeral port and connects a
test-side TCP client that speaks the wire protocol. This isn't a pure
unit test — it exercises asyncio + sockets — but the transport is fully
about real socket behaviour, so mocking would just hide bugs.

All tests use port=0 (OS picks free port) to allow parallel runs.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from trading_bot.core.execution.ninja_messages import (
    FillEventType,
    FillPayload,
    HeartbeatPayload,
    MsgType,
    SignalPayload,
    build_envelope,
    decode_envelope,
    encode_envelope,
)
from trading_bot.core.execution.ninja_transport import (
    NinjaBridgeServer,
    NotConnectedError,
)


SECRET = b"transport-test-shared-secret-for-hmac"


# ── Test helpers ─────────────────────────────────────────────────────────


@asynccontextmanager
async def _running_server():
    server = NinjaBridgeServer(host="127.0.0.1", port=0, secret=SECRET)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


async def _connect_test_client(port: int):
    """Open a TCP connection to the test server. Returns (reader, writer)."""
    return await asyncio.open_connection("127.0.0.1", port)


async def _send_envelope_from_client(
    writer: asyncio.StreamWriter, *, msg_type: MsgType, sequence: int, payload
):
    envelope = build_envelope(
        msg_type=msg_type, sequence=sequence, payload=payload, secret=SECRET
    )
    writer.write(encode_envelope(envelope))
    await writer.drain()


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── Lifecycle ────────────────────────────────────────────────────────────


def test_server_starts_and_stops_cleanly():
    async def _impl():
        async with _running_server() as server:
            assert server.is_listening is True
            assert server.is_client_connected is False
            assert server.port > 0  # OS picked a port
        # After context exit, server.stop() ran — listening should be False
        assert server.is_listening is False

    _run(_impl())


def test_server_requires_bytes_secret():
    with pytest.raises(TypeError):
        NinjaBridgeServer(host="127.0.0.1", port=0, secret="not-bytes")  # type: ignore[arg-type]


# ── Client connection ────────────────────────────────────────────────────


def test_client_connect_sets_is_client_connected():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            try:
                await asyncio.wait_for(server.wait_for_client(), timeout=1.0)
                assert server.is_client_connected is True
            finally:
                writer.close()
                await writer.wait_closed()

    _run(_impl())


def test_client_disconnect_clears_state():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)
            assert server.is_client_connected is True

            writer.close()
            await writer.wait_closed()

            # Give server a tick to process the disconnect
            for _ in range(20):
                if not server.is_client_connected:
                    break
                await asyncio.sleep(0.05)

            assert server.is_client_connected is False

    _run(_impl())


def test_second_client_replaces_first():
    # NT lifecycle: process restart → new connection. We want last-write-wins.
    async def _impl():
        async with _running_server() as server:
            _, writer1 = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            _, writer2 = await _connect_test_client(server.port)
            # Give server a tick to process the replacement
            await asyncio.sleep(0.1)

            assert server.is_client_connected is True
            # writer1 should be closing (replaced); writer2 should be active
            # We can't easily inspect that from writer1 alone, but a send
            # from writer2 should arrive and writer1's connection should be
            # closed by the server.
            await _send_envelope_from_client(
                writer2,
                msg_type=MsgType.HEARTBEAT,
                sequence=1,
                payload=HeartbeatPayload(
                    timestamp="2026-05-15T22:00:00+00:00", from_party="nt"
                ),
            )
            received = await asyncio.wait_for(server.receive(), timeout=1.0)
            assert received.payload["from_party"] == "nt"
            assert received.sequence == 1

            writer2.close()
            try:
                await writer2.wait_closed()
            except Exception:
                pass
            writer1.close()
            try:
                await writer1.wait_closed()
            except Exception:
                pass

    _run(_impl())


# ── Send / Receive roundtrip ─────────────────────────────────────────────


def test_send_with_no_client_raises_not_connected():
    async def _impl():
        async with _running_server() as server:
            with pytest.raises(NotConnectedError):
                await server.send(
                    MsgType.HEARTBEAT,
                    HeartbeatPayload(
                        timestamp="2026-05-15T22:00:00+00:00", from_party="python"
                    ),
                )

    _run(_impl())


def test_server_to_client_send_roundtrip():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            payload = SignalPayload(
                intent_id="test-1",
                timestamp="2026-05-15T22:00:00+00:00",
                symbol="MESU26",
                nt_symbol="MES 09-26",
                side="BUY",
                quantity=1,
                atm_template="KATE_MES_ORB_BASE",
                stop_price=5234.50,
                target_price=5240.00,
                signal_close_price=5236.25,
            )
            seq = await server.send(MsgType.SIGNAL, payload)
            assert seq == 1  # first message → sequence 1

            # Client reads one line
            line = await asyncio.wait_for(reader.readline(), timeout=1.0)
            assert line.endswith(b"\n")
            envelope = decode_envelope(line.rstrip(b"\n"), secret=SECRET)
            assert envelope.msg_type == MsgType.SIGNAL.value
            assert envelope.sequence == 1
            assert envelope.payload["intent_id"] == "test-1"

            writer.close()
            await writer.wait_closed()

    _run(_impl())


def test_client_to_server_send_roundtrip():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            payload = FillPayload(
                intent_id="test-1",
                timestamp="2026-05-15T22:00:05+00:00",
                event_type=FillEventType.ENTRY.value,
                fill_price=5236.50,
                fill_quantity=1,
                nt_order_id="NT-12345",
            )
            await _send_envelope_from_client(
                writer, msg_type=MsgType.FILL, sequence=42, payload=payload
            )

            received = await asyncio.wait_for(server.receive(), timeout=1.0)
            assert received.msg_type == MsgType.FILL.value
            assert received.sequence == 42
            assert received.payload["nt_order_id"] == "NT-12345"

            writer.close()
            await writer.wait_closed()

    _run(_impl())


def test_send_increments_sequence():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            payload = HeartbeatPayload(
                timestamp="2026-05-15T22:00:00+00:00", from_party="python"
            )
            s1 = await server.send(MsgType.HEARTBEAT, payload)
            s2 = await server.send(MsgType.HEARTBEAT, payload)
            s3 = await server.send(MsgType.HEARTBEAT, payload)
            assert (s1, s2, s3) == (1, 2, 3)

            writer.close()
            await writer.wait_closed()

    _run(_impl())


# ── Malformed messages get dropped, not crash the server ──────────────────


def test_malformed_message_dropped_without_killing_server():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            # Send a bad line first
            writer.write(b"this-is-not-json\n")
            await writer.drain()

            # Then a valid one
            payload = HeartbeatPayload(
                timestamp="2026-05-15T22:00:00+00:00", from_party="nt"
            )
            await _send_envelope_from_client(
                writer, msg_type=MsgType.HEARTBEAT, sequence=1, payload=payload
            )

            # Server should have dropped the bad line and delivered the good one
            received = await asyncio.wait_for(server.receive(), timeout=1.0)
            assert received.msg_type == MsgType.HEARTBEAT.value
            assert received.payload["from_party"] == "nt"

            # Server still alive
            assert server.is_listening is True
            assert server.is_client_connected is True

            writer.close()
            await writer.wait_closed()

    _run(_impl())


def test_hmac_mismatch_dropped_without_killing_server():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            # Build an envelope with the WRONG secret and send it
            from trading_bot.core.execution.ninja_messages import (
                build_envelope as _build,
                encode_envelope as _enc,
            )

            bad = _build(
                msg_type=MsgType.HEARTBEAT,
                sequence=1,
                payload=HeartbeatPayload(
                    timestamp="2026-05-15T22:00:00+00:00", from_party="attacker"
                ),
                secret=b"the-wrong-secret",
            )
            writer.write(_enc(bad))
            await writer.drain()

            # Then a properly signed one
            payload = HeartbeatPayload(
                timestamp="2026-05-15T22:00:01+00:00", from_party="nt"
            )
            await _send_envelope_from_client(
                writer, msg_type=MsgType.HEARTBEAT, sequence=2, payload=payload
            )

            received = await asyncio.wait_for(server.receive(), timeout=1.0)
            # The attacker's message must have been dropped — we get the valid one
            assert received.payload["from_party"] == "nt"
            assert received.sequence == 2

            writer.close()
            await writer.wait_closed()

    _run(_impl())


# ── receive_nowait ───────────────────────────────────────────────────────


def test_receive_nowait_returns_none_when_empty():
    async def _impl():
        async with _running_server() as server:
            assert server.receive_nowait() is None

    _run(_impl())


def test_receive_nowait_returns_envelope_when_available():
    async def _impl():
        async with _running_server() as server:
            reader, writer = await _connect_test_client(server.port)
            await asyncio.wait_for(server.wait_for_client(), timeout=1.0)

            payload = HeartbeatPayload(
                timestamp="2026-05-15T22:00:00+00:00", from_party="nt"
            )
            await _send_envelope_from_client(
                writer, msg_type=MsgType.HEARTBEAT, sequence=1, payload=payload
            )

            # Give the server a tick to enqueue
            await asyncio.sleep(0.1)

            received = server.receive_nowait()
            assert received is not None
            assert received.payload["from_party"] == "nt"

            writer.close()
            await writer.wait_closed()

    _run(_impl())
