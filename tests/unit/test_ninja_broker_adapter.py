"""Tests for the NinjaBrokerAdapter skeleton.

Exercises real localhost TCP (port=0, OS picks) like test_ninja_transport.py
— the adapter is mostly bridge integration so mocking would hide bugs.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BrokerError,
    BrokerEventKind,
    BrokerSymbolSpec,
)
from trading_bot.core.execution.ninja_broker_adapter import (
    NinjaBrokerAdapter,
    NinjaConfig,
    _STUB_NLV_SENTINEL,
)
from trading_bot.core.execution.ninja_messages import (
    BarPayload,
    FillEventType,
    FillPayload,
    HeartbeatPayload,
    MsgType,
    build_envelope,
    decode_envelope,
    encode_envelope,
)
from trading_bot.core.execution.ninja_transport import NinjaBridgeServer

SECRET = b"adapter-test-shared-secret"

SYMBOL_MAP = {
    "MESM26": BrokerSymbolSpec(
        logical_symbol="MESM26",
        broker_symbol="MES 06-26",
        exchange="CME",
        tick_size=0.25,
    ),
}


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@asynccontextmanager
async def _connected_adapter():
    """Build adapter + run NT-side test client; yield (adapter, reader, writer)."""
    bridge = NinjaBridgeServer(host="127.0.0.1", port=0, secret=SECRET)
    adapter = NinjaBrokerAdapter(
        config=NinjaConfig(
            hmac_secret=SECRET,
            host="127.0.0.1",
            port=0,
            client_connect_timeout_seconds=2.0,
        ),
        symbol_map=SYMBOL_MAP,
        bridge=bridge,
    )
    # Start the server first so we know the bound port.
    await bridge.start()

    async def _client_task():
        return await asyncio.open_connection("127.0.0.1", bridge.port)

    client_fut = asyncio.create_task(_client_task())
    # connect() will wait for the client; allow the client to attach.
    await adapter.connect()
    reader, writer = await client_fut
    # Drain the CONNECTED event that connect() emits internally.
    async for ev in adapter.events():
        assert ev.kind is BrokerEventKind.CONNECTED
        break
    try:
        yield adapter, reader, writer
    finally:
        writer.close()
        with pytest.MonkeyPatch.context() as m:
            # Silence wait_closed exceptions on already-closed writers.
            pass
        try:
            await writer.wait_closed()
        except Exception:
            pass
        await adapter.disconnect()


# ── Config + construction ─────────────────────────────────────────────────

def test_empty_secret_rejected():
    with pytest.raises(BrokerError):
        NinjaBrokerAdapter(
            config=NinjaConfig(hmac_secret=b""),
            symbol_map=SYMBOL_MAP,
        )


# ── Lifecycle ─────────────────────────────────────────────────────────────

def test_connect_times_out_when_no_client():
    async def _impl():
        bridge = NinjaBridgeServer(host="127.0.0.1", port=0, secret=SECRET)
        adapter = NinjaBrokerAdapter(
            config=NinjaConfig(
                hmac_secret=SECRET,
                client_connect_timeout_seconds=0.2,
            ),
            symbol_map=SYMBOL_MAP,
            bridge=bridge,
        )
        with pytest.raises(BrokerError, match="did not connect"):
            await adapter.connect()
    _run(_impl())


# ── submit_order ──────────────────────────────────────────────────────────

def test_submit_order_sends_signal_envelope():
    async def _impl():
        async with _connected_adapter() as (adapter, reader, _writer):
            await adapter.submit_order(
                client_order_id="intent-001",
                symbol="MESM26",
                exchange="CME",
                side=proto.BUY,
                quantity=1,
                order_type=1,
                price=5000.0,
                stop_price=4990.0,
                target_price=5025.0,
            )
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            obj = json.loads(line.decode("utf-8"))
            assert obj["msg_type"] == MsgType.SIGNAL.value
            payload = obj["payload"]
            assert payload["intent_id"] == "intent-001"
            assert payload["symbol"] == "MESM26"
            assert payload["nt_symbol"] == "MES 06-26"
            assert payload["side"] == "BUY"
            assert payload["quantity"] == 1
            assert payload["stop_price"] == 4990.0
            assert payload["target_price"] == 5025.0
            assert payload["atm_template"] == "KATE_MES_ORB_BASE"
            # Legacy path: no signal_close_price kwarg → falls back to `price`
            assert payload["signal_close_price"] == 5000.0
    _run(_impl())


def test_submit_order_wires_explicit_signal_close_price():
    """Codex review §3: when caller passes signal_close_price, the adapter
    uses it for SignalPayload.signal_close_price (not the `price` arg)."""
    async def _impl():
        async with _connected_adapter() as (adapter, reader, _writer):
            await adapter.submit_order(
                client_order_id="intent-002",
                symbol="MESM26",
                exchange="CME",
                side=proto.BUY,
                quantity=1,
                order_type=1,
                price=0.0,                  # market order — no price
                stop_price=4990.0,
                target_price=5025.0,
                signal_close_price=5012.75,  # explicit bar close at decision
            )
            line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            payload = json.loads(line.decode("utf-8"))["payload"]
            assert payload["signal_close_price"] == 5012.75
    _run(_impl())


def test_submit_order_rejects_unknown_symbol():
    async def _impl():
        async with _connected_adapter() as (adapter, _reader, _writer):
            with pytest.raises(BrokerError, match="no symbol_map entry"):
                await adapter.submit_order(
                    client_order_id="x",
                    symbol="UNKNOWN26",
                    exchange="CME",
                    side=proto.BUY,
                    quantity=1,
                    order_type=1,
                    stop_price=1.0,
                    target_price=2.0,
                )
    _run(_impl())


def test_submit_order_requires_stop_and_target():
    async def _impl():
        async with _connected_adapter() as (adapter, _reader, _writer):
            with pytest.raises(BrokerError, match="stop_price and target_price"):
                await adapter.submit_order(
                    client_order_id="x",
                    symbol="MESM26",
                    exchange="CME",
                    side=proto.BUY,
                    quantity=1,
                    order_type=1,
                    stop_price=None,
                    target_price=None,
                )
    _run(_impl())


# ── events() — translation pump ───────────────────────────────────────────

def test_fill_envelope_translates_to_order_filled_event():
    async def _impl():
        async with _connected_adapter() as (adapter, _reader, writer):
            fill = FillPayload(
                intent_id="intent-001",
                timestamp="2026-05-18T10:00:00+00:00",
                event_type=FillEventType.ENTRY.value,
                fill_price=5001.25,
                fill_quantity=1,
                nt_order_id="nt-abc-123",
            )
            envelope = build_envelope(
                msg_type=MsgType.FILL,
                sequence=1,
                payload=fill,
                secret=SECRET,
            )
            writer.write(encode_envelope(envelope))
            await writer.drain()
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.ORDER_FILLED
                assert ev.order is not None
                assert ev.order.client_order_id == "intent-001"
                assert ev.order.fill_price == 5001.25
                assert ev.order.server_order_id == "nt-abc-123"
                break
    _run(_impl())


def test_heartbeat_envelope_translates_to_heartbeat_event():
    async def _impl():
        async with _connected_adapter() as (adapter, _reader, writer):
            hb = HeartbeatPayload(
                timestamp="2026-05-18T10:00:00+00:00",
                from_party="nt",
            )
            envelope = build_envelope(
                msg_type=MsgType.HEARTBEAT,
                sequence=1,
                payload=hb,
                secret=SECRET,
            )
            writer.write(encode_envelope(envelope))
            await writer.drain()
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.HEARTBEAT
                break
    _run(_impl())


# ── Skeleton stubs ────────────────────────────────────────────────────────

def test_subscribe_market_data_acknowledges_known_symbol():
    """NT bar publication is autonomous — subscribe is a config check, not
    a network subscription. Known symbols return cleanly; unknown raise."""
    async def _impl():
        async with _connected_adapter() as (adapter, _r, _w):
            # Known symbol — no-op
            await adapter.subscribe_market_data(symbol="MESM26", exchange="CME")
            # Unknown symbol — clear BrokerError
            with pytest.raises(BrokerError, match="unknown logical symbol"):
                await adapter.subscribe_market_data(symbol="UNKNOWN26", exchange="CME")
    _run(_impl())


def test_cancel_order_not_implemented():
    async def _impl():
        async with _connected_adapter() as (adapter, _r, _w):
            with pytest.raises(NotImplementedError, match="CANCEL envelope"):
                await adapter.cancel_order(client_order_id="x")
    _run(_impl())


def test_request_account_state_returns_sentinel():
    async def _impl():
        async with _connected_adapter() as (adapter, _r, _w):
            state = await adapter.request_account_state(trade_account="Sim101")
            assert state.nlv == _STUB_NLV_SENTINEL
            assert state.currency == "USD"
    _run(_impl())


# ── BAR translation + dedup (Codex's contract 2026-05-18) ────────────────


def _bar_payload(
    *,
    bar_index: int,
    timestamp: str = "2026-05-18T14:31:00+00:00",
    symbol: str = "MESM26",
    open: float = 5000.0,
    high: float = 5002.5,
    low: float = 4999.5,
    close: float = 5001.25,
    volume: int = 42,
) -> BarPayload:
    return BarPayload(
        timestamp=timestamp,
        symbol=symbol,
        nt_symbol="MES 06-26",
        timeframe_minutes=1,
        bar_index=bar_index,
        open=open, high=high, low=low, close=close, volume=volume,
    )


async def _send_bar(writer, *, seq: int, bar: BarPayload):
    envelope = build_envelope(
        msg_type=MsgType.BAR, sequence=seq, payload=bar, secret=SECRET,
    )
    writer.write(encode_envelope(envelope))
    await writer.drain()


def test_bar_envelope_translates_to_market_data_bar_event():
    async def _impl():
        async with _connected_adapter() as (adapter, _r, writer):
            await _send_bar(writer, seq=1, bar=_bar_payload(bar_index=12345))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.MARKET_DATA_BAR
                assert ev.bar is not None
                assert ev.bar.symbol == "MESM26"
                assert ev.bar.open == 5000.0
                assert ev.bar.close == 5001.25
                assert ev.bar.timeframe_minutes == 1
                assert ev.bar.timestamp.tzinfo is not None
                break
    _run(_impl())


def test_bar_retransmit_with_identical_ohlcv_is_dropped_silently():
    async def _impl():
        async with _connected_adapter() as (adapter, _r, writer):
            bar = _bar_payload(bar_index=100)
            await _send_bar(writer, seq=1, bar=bar)
            # First emit
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.MARKET_DATA_BAR
                break
            # Retransmit — should be dropped silently, no event
            await _send_bar(writer, seq=2, bar=bar)
            # Send a heartbeat after — that should be the next event we see
            hb = HeartbeatPayload(timestamp="x", from_party="nt")
            envelope = build_envelope(
                msg_type=MsgType.HEARTBEAT, sequence=3, payload=hb, secret=SECRET,
            )
            writer.write(encode_envelope(envelope))
            await writer.drain()
            async for ev in adapter.events():
                # Retransmit was dropped; we see the heartbeat next
                assert ev.kind is BrokerEventKind.HEARTBEAT
                break
    _run(_impl())


def test_bar_revision_emits_error_event():
    """Same (symbol, bar_index, timestamp) with different OHLCV must
    surface as an ERROR event so audit layer can fail validation day."""
    async def _impl():
        async with _connected_adapter() as (adapter, _r, writer):
            await _send_bar(writer, seq=1, bar=_bar_payload(bar_index=100, close=5001.25))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.MARKET_DATA_BAR
                break
            # Same key, different close — revision
            await _send_bar(writer, seq=2, bar=_bar_payload(bar_index=100, close=5005.00))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.ERROR
                assert ev.error_message is not None
                assert "BAR_REVISION" in ev.error_message
                break
    _run(_impl())


def test_bar_out_of_order_emits_error_event():
    """bar_index must be monotonically non-decreasing per symbol."""
    async def _impl():
        async with _connected_adapter() as (adapter, _r, writer):
            await _send_bar(writer, seq=1, bar=_bar_payload(bar_index=200))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.MARKET_DATA_BAR
                break
            # bar_index regression
            await _send_bar(writer, seq=2, bar=_bar_payload(
                bar_index=100, timestamp="2026-05-18T14:25:00+00:00",
            ))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.ERROR
                assert "out-of-order" in (ev.error_message or "")
                break
    _run(_impl())


def test_bar_naive_timestamp_emits_error():
    """Reject naive datetimes — NinjaScript must send tz-aware UTC."""
    async def _impl():
        async with _connected_adapter() as (adapter, _r, writer):
            await _send_bar(writer, seq=1, bar=_bar_payload(
                bar_index=1, timestamp="2026-05-18T14:31:00",  # no offset
            ))
            async for ev in adapter.events():
                assert ev.kind is BrokerEventKind.ERROR
                assert "timezone" in (ev.error_message or "")
                break
    _run(_impl())
