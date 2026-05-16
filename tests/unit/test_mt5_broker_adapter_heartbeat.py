"""Regression tests for the MT5BrokerAdapter heartbeat alert.

Per Gemini's resilience directive 2026-05-15: silent disconnects must not
quietly consume a London-session window again. These tests guard the
heartbeat detection + Telegram alert path. Silent breakage = silent
catastrophic risk; keep them green.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from trading_bot.core.execution.broker_adapter import BrokerSymbolSpec
from trading_bot.core.execution import mt5_broker_adapter as mt5_adapter_module
from trading_bot.core.execution.mt5_broker_adapter import MT5BrokerAdapter, MT5Config


# ── Test fixtures ────────────────────────────────────────────────────────


@dataclass
class _TerminalInfo:
    connected: bool = True


@dataclass
class _AccountInfo:
    balance: float = 1000.0
    equity: float = 1000.0
    profit: float = 0.0
    margin: float = 0.0
    currency: str = "USD"


class _FakeMT5Heartbeat:
    """Minimal MT5 fake — just enough surface for the heartbeat code path."""

    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_PLACED = 10008

    def __init__(self) -> None:
        self.terminal = _TerminalInfo(connected=True)

    def initialize(self, **kwargs):
        return True

    def shutdown(self):
        return True

    def symbol_select(self, symbol, selected):
        return True

    def symbol_info_tick(self, symbol):
        return None

    def account_info(self):
        return _AccountInfo()

    def positions_get(self):
        return []

    def orders_get(self):
        return []

    def terminal_info(self):
        return self.terminal

    def last_error(self):
        return (0, "ok")


SYMBOL_MAP = {
    "GBPUSD": BrokerSymbolSpec(
        logical_symbol="GBPUSD",
        broker_symbol="GBPUSD",
        exchange="FX",
        tick_size=0.0001,
    ),
}


def _adapter(runtime: _FakeMT5Heartbeat, *, threshold: float = 300.0) -> MT5BrokerAdapter:
    return MT5BrokerAdapter(
        config=MT5Config(
            login=123456,
            password="pw",
            server="Demo-Server",
            heartbeat_disconnect_alert_seconds=threshold,
            poll_interval_seconds=60.0,  # poll loop won't actually fire in tests
        ),
        symbol_map=SYMBOL_MAP,
        runtime=runtime,
    )


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture
def captured_alerts(monkeypatch):
    """Replace push_telegram_alert with a capture list. Returns the list."""
    captured: list[str] = []

    def _capture(text: str, **kwargs) -> bool:
        captured.append(text)
        return True

    monkeypatch.setattr(mt5_adapter_module, "push_telegram_alert", _capture)
    return captured


# ── Tests ────────────────────────────────────────────────────────────────


def test_default_path_is_hard_coded_per_gemini_directive():
    # Gemini's resilience directive: hard-code MT5 path so initialize() can't
    # silently miss a path env var. Verify the default reflects the
    # IC Markets MT5 install location on Kate Host.
    cfg = MT5Config()
    assert cfg.path == r"C:\Program Files\MetaTrader 5"


def test_default_heartbeat_threshold_is_300_seconds():
    # Per Gemini's spec: 5-minute disconnect window before paging operator.
    cfg = MT5Config()
    assert cfg.heartbeat_disconnect_alert_seconds == 300.0


def test_connect_seeds_heartbeat_clock():
    # Without seeding _last_connected_at on connect, the very first poll
    # tick would (now - 0.0) > 300s and fire a false-positive alert.
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime)
        assert adapter._last_connected_at == 0.0
        await adapter.connect()
        assert adapter._last_connected_at > 0.0
        assert adapter._heartbeat_alerted is False

    _run(_impl())


def test_connected_terminal_keeps_clock_fresh_no_alert(captured_alerts):
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        runtime.terminal.connected = True
        adapter = _adapter(runtime)
        await adapter.connect()

        # Simulate clock passage AND a connected reply — clock should refresh
        original_clock = adapter._last_connected_at
        adapter._last_connected_at = original_clock - 100.0  # pretend 100s ago
        await adapter._check_heartbeat_and_alert()

        assert adapter._last_connected_at > original_clock - 100.0
        assert adapter._heartbeat_alerted is False
        assert captured_alerts == []

    _run(_impl())


def test_disconnect_under_threshold_no_alert(captured_alerts):
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()

        # Simulate 100s of disconnection — under the 300s threshold
        runtime.terminal.connected = False
        adapter._last_connected_at -= 100.0
        await adapter._check_heartbeat_and_alert()

        assert adapter._heartbeat_alerted is False
        assert captured_alerts == []

    _run(_impl())


def test_disconnect_over_threshold_fires_one_alert(captured_alerts):
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()

        # Simulate 400s of disconnection — past 300s threshold
        runtime.terminal.connected = False
        adapter._last_connected_at -= 400.0
        await adapter._check_heartbeat_and_alert()

        assert adapter._heartbeat_alerted is True
        assert len(captured_alerts) == 1
        assert "DISCONNECTED" in captured_alerts[0]
        assert "Action required" in captured_alerts[0]

    _run(_impl())


def test_repeated_disconnect_polls_only_alert_once(captured_alerts):
    # Idempotent alerting — no spam at every poll tick during a long outage.
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()
        runtime.terminal.connected = False
        adapter._last_connected_at -= 400.0

        # Five consecutive poll-tick checks during the same disconnect cycle
        for _ in range(5):
            await adapter._check_heartbeat_and_alert()

        assert len(captured_alerts) == 1  # exactly one alert fired

    _run(_impl())


def test_reconnect_after_alert_sends_recovery_message_and_clears_flag(captured_alerts):
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()

        # Trigger an alert
        runtime.terminal.connected = False
        adapter._last_connected_at -= 400.0
        await adapter._check_heartbeat_and_alert()
        assert adapter._heartbeat_alerted is True
        assert len(captured_alerts) == 1

        # Reconnect — should send recovery message + clear the flag
        runtime.terminal.connected = True
        await adapter._check_heartbeat_and_alert()

        assert adapter._heartbeat_alerted is False
        assert len(captured_alerts) == 2
        assert "RECONNECTED" in captured_alerts[1]

    _run(_impl())


def test_terminal_info_none_treated_as_disconnect(captured_alerts):
    # If mt5.terminal_info() returns None (API itself dead), treat as
    # disconnected — don't crash the adapter on a NoneType.connected access.
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()

        # Patch terminal_info to return None
        runtime.terminal_info = lambda: None
        adapter._last_connected_at -= 400.0
        await adapter._check_heartbeat_and_alert()

        assert adapter._heartbeat_alerted is True
        assert len(captured_alerts) == 1

    _run(_impl())


def test_terminal_info_raises_treated_as_disconnect(captured_alerts):
    # If mt5.terminal_info() raises (e.g. broken pipe), treat as disconnect
    # — the adapter must not propagate the exception into the poll loop.
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()

        def _raise():
            raise RuntimeError("MT5 API pipe broken")

        runtime.terminal_info = _raise
        adapter._last_connected_at -= 400.0
        await adapter._check_heartbeat_and_alert()

        assert adapter._heartbeat_alerted is True
        assert len(captured_alerts) == 1

    _run(_impl())


def test_telegram_failure_does_not_crash_adapter(monkeypatch):
    # push_telegram_alert returning False (network down, missing creds, etc)
    # must NOT raise inside the adapter — heartbeat check is best-effort.
    captured = []

    def _failing_push(text: str, **kwargs) -> bool:
        captured.append(text)
        return False  # simulated failure

    monkeypatch.setattr(mt5_adapter_module, "push_telegram_alert", _failing_push)

    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()
        runtime.terminal.connected = False
        adapter._last_connected_at -= 400.0
        # Should not raise
        await adapter._check_heartbeat_and_alert()
        # Flag still set even though push failed — alert was attempted
        assert adapter._heartbeat_alerted is True
        assert len(captured) == 1

    _run(_impl())


def test_recovery_alert_only_fires_once_per_disconnect_cycle(captured_alerts):
    # After recovery, _heartbeat_alerted=False. Subsequent connected polls
    # should NOT re-send the recovery message.
    async def _impl():
        runtime = _FakeMT5Heartbeat()
        adapter = _adapter(runtime, threshold=300.0)
        await adapter.connect()
        runtime.terminal.connected = False
        adapter._last_connected_at -= 400.0
        await adapter._check_heartbeat_and_alert()  # alert fires
        runtime.terminal.connected = True
        await adapter._check_heartbeat_and_alert()  # recovery fires

        # Three more connected polls — should NOT add more recovery messages
        for _ in range(3):
            await adapter._check_heartbeat_and_alert()

        assert len(captured_alerts) == 2  # disconnect + recovery, that's it

    _run(_impl())
