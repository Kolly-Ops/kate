"""
Integration tests for trading_bot.core.execution.dtc_client against the
binary-mode mock DTC server.

Covers the regression path for COO Gemini's 2026-04-27 sim-test finding:
the LOGON_RESPONSE on the wire was LARGER than the legacy 238-byte struct,
which broke a fixed-size struct.unpack. The mock pads its response beyond
238 bytes to exercise the new variable-size unpack path.
"""
from __future__ import annotations

import asyncio

import pytest

from tests.mocks.mock_dtc_server import BinaryMockDTCServer
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.dtc_client import DTCClient


@pytest.mark.asyncio
async def test_logon_handshake_handles_oversized_response() -> None:
    server = BinaryMockDTCServer(host="127.0.0.1", port=0)
    await server.start()
    try:
        async with DTCClient(host="127.0.0.1", port=server.actual_port) as client:
            resp = await client.logon(
                client_name="TEST_BOT", trade_mode=proto.TRADE_MODE_DEMO
            )
        assert resp.result_code == proto.LOGON_SUCCESS
        assert resp.server_name == "BinaryMockDTC"
        assert resp.market_data_supported is True
        assert resp.trading_supported is True
        # Mock pads response beyond legacy 238-byte struct — verifies the
        # client tolerated the size mismatch that broke the original
        # fixed-size struct.unpack.
        assert resp.raw_size > proto.LOGON_RESPONSE_LEGACY_SIZE
        assert len(server.received_logons) == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_submit_order_returns_order_update() -> None:
    server = BinaryMockDTCServer(host="127.0.0.1", port=0)
    await server.start()
    try:
        async with DTCClient(host="127.0.0.1", port=server.actual_port) as client:
            await client.logon(client_name="TEST_BOT", trade_mode=proto.TRADE_MODE_DEMO)
            order_id = await client.submit_order(
                symbol="MESM26",
                exchange="CME",
                trade_account="",
                client_order_id="TEST_001",
                side=proto.BUY,
                quantity=1.0,
            )
            assert order_id == "TEST_001"
            msg = await client.recv_event(timeout=5.0)
            assert msg.msg_type == proto.ORDER_UPDATE
        assert len(server.received_orders) == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_heartbeat_roundtrip() -> None:
    server = BinaryMockDTCServer(host="127.0.0.1", port=0)
    await server.start()
    try:
        async with DTCClient(host="127.0.0.1", port=server.actual_port) as client:
            await client.logon(client_name="TEST_BOT", trade_mode=proto.TRADE_MODE_DEMO)
            await client.heartbeat()
            msg = await client.recv_event(timeout=5.0)
            assert msg.msg_type == proto.HEARTBEAT
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_logon_rejection_raises() -> None:
    """If the mock returns a non-success result code, logon should raise."""

    class RejectingMock(BinaryMockDTCServer):
        async def _send_logon_response(self, writer):
            import struct
            legacy = struct.pack(
                proto.LOGON_RESPONSE_LEGACY_FMT,
                0, proto.LOGON_RESPONSE, 8,
                proto.LOGON_ERROR,                    # rejection
                b"Bad credentials", b"", 0,
                b"BinaryMockDTC", 0, 0,
            )
            extra = b"\x00" * 14
            payload = legacy + extra
            size = len(payload)
            payload = struct.pack("<H", size) + payload[2:]
            writer.write(payload)
            await writer.drain()

    server = RejectingMock(host="127.0.0.1", port=0)
    await server.start()
    try:
        from trading_bot.core.execution.dtc_client import DTCError

        async with DTCClient(host="127.0.0.1", port=server.actual_port) as client:
            with pytest.raises(DTCError, match="logon rejected"):
                await client.logon(client_name="TEST_BOT")
    finally:
        await server.stop()
