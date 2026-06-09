"""Unit tests for IGBrokerAdapter.

The adapter wraps IG Markets REST API. Tests inject a fake
http_session whose `get/post/put/delete` return pre-canned response
objects, so no network calls leave the test process. Pattern matches
the MT5 adapter test fixture approach.

Autouse fixture blocks real Telegram alerts — a 2026-05-21 incident
proved the MT5 adapter tests leaked real alerts to the CEO's phone
when push_telegram_alert wasn't mocked. Same risk class here.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BrokerAdapter,
    BrokerError,
    BrokerEventKind,
)
from trading_bot.core.execution.ig_broker_adapter import (
    IGBrokerAdapter,
    IGConfig,
    IGSymbolSpec,
    _lightstreamer_endpoint_candidates,
)


@pytest.fixture(autouse=True)
def _block_real_telegram_alerts(monkeypatch):
    """Mock push_telegram_alert across both call sites in the adapter.

    The adapter does `from ..alerts import push_telegram_alert` inside
    submit_order and _alert_position_closed (lazy import), so we patch
    the source module's symbol so both lazy lookups resolve to the
    no-op. Mirrors the lesson from MT5 adapter tests.
    """
    monkeypatch.setattr(
        "trading_bot.core.alerts.push_telegram_alert",
        lambda *a, **kw: True,
    )


@dataclass
class _FakeResponse:
    status_code: int = 200
    _json_body: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    content: bytes = b"{}"
    text: str = "{}"

    def json(self) -> dict[str, Any]:
        return self._json_body


@dataclass
class _Call:
    method: str
    url: str
    headers: dict[str, str]
    json_body: Optional[dict[str, Any]]


class _FakeHttpSession:
    """Records every call and returns scripted responses.

    Test sets `scripted` as a list of (method, path_substring, response)
    tuples; on each call we find the first matching entry. Anything
    unmatched returns a 500 to surface test gaps.
    """

    def __init__(self) -> None:
        self.scripted: list[tuple[str, str, _FakeResponse]] = []
        self.calls: list[_Call] = []

    def _dispatch(
        self, method: str, url: str, headers=None, json=None, timeout=None,
    ) -> _FakeResponse:
        self.calls.append(_Call(
            method=method, url=url,
            headers=dict(headers or {}),
            json_body=json,
        ))
        for m, path_substr, resp in self.scripted:
            if m == method and path_substr in url:
                return resp
        return _FakeResponse(
            status_code=500,
            _json_body={"error": f"unscripted: {method} {url}"},
            content=b'{"error":"unscripted"}',
            text='{"error":"unscripted"}',
        )

    def get(self, url, **kw):     return self._dispatch("GET", url, **kw)
    def post(self, url, **kw):    return self._dispatch("POST", url, **kw)
    def put(self, url, **kw):     return self._dispatch("PUT", url, **kw)
    def delete(self, url, **kw):  return self._dispatch("DELETE", url, **kw)


class _FakeStreamingClient:
    def __init__(self) -> None:
        self.connected: Optional[dict[str, Any]] = None
        self.subscriptions: list[dict[str, Any]] = []
        self.disconnected = False

    async def connect(
        self, *, endpoint: str, account_id: str, cst: str, security_token: str,
    ) -> None:
        self.connected = {
            "endpoint": endpoint,
            "account_id": account_id,
            "cst": cst,
            "security_token": security_token,
        }

    async def subscribe_price(
        self, *, spec: IGSymbolSpec, account_id: str, callback,
    ) -> None:
        self.subscriptions.append({
            "spec": spec,
            "account_id": account_id,
            "callback": callback,
        })

    async def disconnect(self) -> None:
        self.disconnected = True

    def emit(self, spec: IGSymbolSpec, fields: dict[str, Any]) -> None:
        callback = next(
            sub["callback"] for sub in self.subscriptions
            if sub["spec"] == spec
        )
        callback(spec, fields)


def _config() -> IGConfig:
    return IGConfig(
        api_key="test-key",
        username="test-user",
        password="test-pass",
        active_account_id="Z6BHQ1",
        environment="demo",
        poll_interval_seconds=99999.0,  # disable background poll in tests
        request_timeout_seconds=5.0,
    )


_SYMBOL_MAP = {
    "GBPUSD": IGSymbolSpec(
        logical_symbol="GBPUSD",
        epic="CS.D.GBPUSD.MINI.IP",
        quantity_per_lot=10.0,
        pip_decimal_position=4,
    ),
}


def _adapter(http: _FakeHttpSession) -> IGBrokerAdapter:
    return IGBrokerAdapter(
        config=_config(),
        symbol_map=_SYMBOL_MAP,
        http_session=http,
    )


def _adapter_with_stream(
    http: _FakeHttpSession, stream: _FakeStreamingClient,
    *, require_streaming: bool = False,
) -> IGBrokerAdapter:
    return IGBrokerAdapter(
        config=_config(),
        symbol_map=_SYMBOL_MAP,
        http_session=http,
        streaming_client_factory=lambda: stream,
        require_streaming=require_streaming,
    )


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ── from_secrets tests ────────────────────────────────────────────────────

def test_from_secrets_loads_all_fields(tmp_path):
    secrets = tmp_path / "secrets.json"
    secrets.write_text(json.dumps({
        "ig": {
            "api_key": "k",
            "username": "u",
            "password": "p",
            "active_account_id": "Z6BHQ1",
            "environment": "demo",
        }
    }))
    cfg = IGConfig.from_secrets(secrets_path=secrets)
    assert cfg.api_key == "k"
    assert cfg.username == "u"
    assert cfg.password == "p"
    assert cfg.active_account_id == "Z6BHQ1"
    assert cfg.environment == "demo"
    assert cfg.base_url == "https://demo-api.ig.com/gateway/deal"


def test_from_secrets_raises_on_missing_field(tmp_path):
    secrets = tmp_path / "secrets.json"
    secrets.write_text(json.dumps({
        "ig": {"api_key": "k", "username": "u"}  # password + account missing
    }))
    with pytest.raises(BrokerError) as exc:
        IGConfig.from_secrets(secrets_path=secrets)
    assert "password" in str(exc.value)
    assert "active_account_id" in str(exc.value)


def test_from_secrets_raises_when_file_missing(tmp_path):
    with pytest.raises(BrokerError) as exc:
        IGConfig.from_secrets(secrets_path=tmp_path / "nope.json")
    assert "not found" in str(exc.value)


# ── lifecycle tests ──────────────────────────────────────────────────────

def test_adapter_is_broker_adapter():
    assert isinstance(_adapter(_FakeHttpSession()), BrokerAdapter)


def test_lightstreamer_endpoint_candidates_keep_https_first_and_demo_http_fallback():
    candidates = _lightstreamer_endpoint_candidates(
        "https://demo-apd.marketdatasystems.com"
    )

    assert candidates[0] == "https://demo-apd.marketdatasystems.com"
    assert "https://demo-apd.marketdatasystems.com/" in candidates
    assert candidates[-1] == "http://demo-apd.marketdatasystems.com/"


def test_connect_authenticates_and_switches_account():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ0",  # default to CFD
                    "accounts": [
                        {"accountId": "Z6BHQ0", "accountName": "CFD"},
                        {"accountId": "Z6BHQ1", "accountName": "Spread bet"},
                    ],
                    "clientId": "104701005",
                },
                headers={
                    "CST": "cst-token-here",
                    "X-SECURITY-TOKEN": "sec-token-here",
                },
                content=b'{"x":1}',
            )),
            ("PUT", "/session", _FakeResponse(
                status_code=200, _json_body={}, content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        # Auth POST happened
        assert any(c.method == "POST" and "/session" in c.url for c in http.calls)
        # PUT happened to switch to spread-bet (CFD was default)
        put_call = next(c for c in http.calls if c.method == "PUT" and "/session" in c.url)
        assert put_call.json_body == {
            "accountId": "Z6BHQ1",
            "defaultAccountId": "Z6BHQ1",
        }
        # Subsequent calls now carry the session headers
        assert adapter._cst == "cst-token-here"
        assert adapter._security_token == "sec-token-here"
        # CONNECTED + LOGON_OK events queued
        events = []
        for _ in range(2):
            events.append(await asyncio.wait_for(adapter._events_q.get(), 0.5))
        assert events[0].kind == BrokerEventKind.CONNECTED
        assert events[1].kind == BrokerEventKind.LOGON_OK
        # Cancel the poll task so the test can exit cleanly
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_connect_skips_account_switch_when_already_active():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ1",  # already spread-bet
                    "accounts": [
                        {"accountId": "Z6BHQ1", "accountName": "Spread bet"},
                    ],
                    "clientId": "104701005",
                },
                headers={
                    "CST": "cst-token-here",
                    "X-SECURITY-TOKEN": "sec-token-here",
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        # No PUT call since current account is already the target
        assert not any(c.method == "PUT" for c in http.calls)
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_connect_starts_lightstreamer_when_endpoint_available():
    async def _impl():
        http = _FakeHttpSession()
        stream = _FakeStreamingClient()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ1",
                    "accounts": [{"accountId": "Z6BHQ1"}],
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter_with_stream(http, stream)
        await adapter.connect()

        assert stream.connected == {
            "endpoint": "https://demo-apd.marketdatasystems.com",
            "account_id": "Z6BHQ1",
            "cst": "cst",
            "security_token": "sec",
        }

        await adapter.disconnect()
        assert stream.disconnected is True

    _run(_impl())


def test_connect_raises_when_streaming_required_and_endpoint_missing():
    async def _impl():
        http = _FakeHttpSession()
        stream = _FakeStreamingClient()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ1",
                    "accounts": [{"accountId": "Z6BHQ1"}],
                },
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter_with_stream(http, stream, require_streaming=True)

        with pytest.raises(BrokerError) as exc:
            await adapter.connect()

        assert "lightstreamerEndpoint" in str(exc.value)
        assert stream.connected is None
        await adapter.disconnect()

    _run(_impl())


def test_connect_raises_when_session_headers_missing():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                # NO CST or X-SECURITY-TOKEN headers
                headers={},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        with pytest.raises(BrokerError) as exc:
            await adapter.connect()
        assert "CST or X-SECURITY-TOKEN" in str(exc.value)

    _run(_impl())


# ── state-query tests ────────────────────────────────────────────────────

def test_request_account_state_returns_balance_event():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": [{"accountId": "Z6BHQ1"}]},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/accounts", _FakeResponse(
                status_code=200,
                _json_body={
                    "accounts": [
                        {
                            "accountId": "Z6BHQ1",
                            "currency": "GBP",
                            "balance": {
                                "balance": 10000.0,
                                "available": 9500.0,
                                "deposit": 0.0,
                                "profitLoss": 25.50,
                            },
                        },
                    ],
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        evt = await adapter.request_account_state(trade_account="Z6BHQ1")
        assert evt.cash == 10000.0
        assert evt.nlv == 10025.50
        assert evt.pnl == 25.50
        assert evt.margin_requirement == 500.0  # balance - available
        assert evt.currency == "GBP"
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_request_positions_translates_direction_to_signed_qty():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/positions", _FakeResponse(
                status_code=200,
                _json_body={
                    "positions": [
                        {
                            "position": {
                                "dealId": "DI001",
                                "direction": "SELL",
                                "size": 5.0,
                                "level": 1.34000,
                            },
                            "market": {"epic": "CS.D.GBPUSD.MINI.IP"},
                        },
                    ],
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        positions = await adapter.request_positions(trade_account="Z6BHQ1")
        assert len(positions) == 1
        assert positions[0].symbol == "GBPUSD"
        assert positions[0].quantity == -0.5  # SELL → negative; IG size normalized to lots
        assert positions[0].avg_price == 1.34000
        assert positions[0].side == proto.SELL
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_request_positions_skips_unknown_epics():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/positions", _FakeResponse(
                status_code=200,
                _json_body={
                    "positions": [
                        {
                            "position": {"dealId": "X", "direction": "BUY", "size": 1.0, "level": 1.0},
                            "market": {"epic": "CS.D.UNMAPPED.MINI.IP"},
                        },
                    ],
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        positions = await adapter.request_positions(trade_account="Z6BHQ1")
        assert positions == ()  # epic not in symbol_map → skipped
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


# ── submit_order tests ──────────────────────────────────────────────────

def test_submit_market_buy_sends_correct_payload_and_emits_fill():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("POST", "/positions/otc", _FakeResponse(
                status_code=200,
                _json_body={"dealReference": "kate-ref-1"},
                content=b'{}',
            )),
            ("GET", "/confirms/", _FakeResponse(
                status_code=200,
                _json_body={
                    "dealStatus": "ACCEPTED",
                    "reason": "SUCCESS",
                    "dealId": "DEAL-001",
                    "level": 1.34050,
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        coid = await adapter.submit_order(
            client_order_id="fxlon-GBPUSD-2605211400",
            symbol="GBPUSD",
            exchange="IG",
            side=proto.BUY,
            quantity=0.5,
            order_type=proto.ORDER_TYPE_MARKET,
            price=1.34050,
            stop_price=1.33950,
            target_price=1.34250,
        )
        assert coid == "fxlon-GBPUSD-2605211400"
        # /positions/otc payload checks
        otc_call = next(c for c in http.calls if c.method == "POST" and "/positions/otc" in c.url)
        body = otc_call.json_body
        assert body["epic"] == "CS.D.GBPUSD.MINI.IP"
        assert body["direction"] == "BUY"
        assert body["size"] == 5.0  # 0.5 lot * quantity_per_lot=10
        assert body["orderType"] == "MARKET"
        assert body["stopLevel"] == 1.33950
        assert body["limitLevel"] == 1.34250
        assert body["forceOpen"] is True
        assert body["currencyCode"] == "GBP"
        # dealReference sanitization: hyphens preserved, length <= 30
        assert "fxlon" in body["dealReference"]
        assert len(body["dealReference"]) <= 30
        # ORDER_FILLED emitted in Kate's normalized units, while the
        # broker payload carries IG's GBP/point size.
        events = []
        while not adapter._events_q.empty():
            events.append(await adapter._events_q.get())
        kinds = [e.kind for e in events]
        assert BrokerEventKind.ORDER_FILLED in kinds
        fill = next(e for e in events if e.kind == BrokerEventKind.ORDER_FILLED)
        assert fill.order.fill_price == 1.34050
        assert fill.order.fill_quantity == 0.5
        assert fill.order.server_order_id == "DEAL-001"
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_submit_order_rejection_emits_event_and_raises():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("POST", "/positions/otc", _FakeResponse(
                status_code=200,
                _json_body={"dealReference": "kate-ref-rejected"},
                content=b'{}',
            )),
            ("GET", "/confirms/", _FakeResponse(
                status_code=200,
                _json_body={
                    "dealStatus": "REJECTED",
                    "reason": "INSUFFICIENT_FUNDS",
                    "dealId": "",
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        with pytest.raises(BrokerError) as exc:
            await adapter.submit_order(
                client_order_id="fxlon-GBPUSD-1",
                symbol="GBPUSD",
                exchange="IG",
                side=proto.BUY,
                quantity=99999.0,
                order_type=proto.ORDER_TYPE_MARKET,
                stop_price=1.33950,
            )
        assert "REJECTED" in str(exc.value)
        assert "INSUFFICIENT_FUNDS" in str(exc.value)
        # ORDER_REJECTED was queued before the raise
        events = []
        while not adapter._events_q.empty():
            events.append(await adapter._events_q.get())
        kinds = [e.kind for e in events]
        assert BrokerEventKind.ORDER_REJECTED in kinds
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_submit_order_sanitizes_dealreference_invalid_chars():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("POST", "/positions/otc", _FakeResponse(
                status_code=200,
                _json_body={"dealReference": "ok"},
                content=b'{}',
            )),
            ("GET", "/confirms/", _FakeResponse(
                status_code=200,
                _json_body={"dealStatus": "ACCEPTED", "dealId": "D1", "level": 1.0, "reason": ""},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        await adapter.submit_order(
            client_order_id="strategy/with#weird&chars*here-and$more",
            symbol="GBPUSD",
            exchange="IG",
            side=proto.SELL,
            quantity=0.1,
            order_type=proto.ORDER_TYPE_MARKET,
            stop_price=1.34100,
        )
        otc_call = next(c for c in http.calls if c.method == "POST" and "/positions/otc" in c.url)
        deal_ref = otc_call.json_body["dealReference"]
        # All weird chars stripped to underscore
        for bad in ("/", "#", "&", "*", "$"):
            assert bad not in deal_ref
        # Length cap respected
        assert len(deal_ref) <= 30
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_submit_order_fails_closed_without_native_stop():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        with pytest.raises(BrokerError, match="requires stop_price"):
            await adapter.submit_order(
                client_order_id="fxlon-GBPUSD-no-stop",
                symbol="GBPUSD",
                exchange="IG",
                side=proto.BUY,
                quantity=0.5,
                order_type=proto.ORDER_TYPE_MARKET,
            )
        assert not any("/positions/otc" in c.url for c in http.calls)
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_submit_order_rejects_separate_bracket_legs():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        with pytest.raises(BrokerError, match="only MARKET entries"):
            await adapter.submit_order(
                client_order_id="fxlon-GBPUSD-stop",
                symbol="GBPUSD",
                exchange="IG",
                side=proto.SELL,
                quantity=0.5,
                order_type=proto.ORDER_TYPE_STOP,
                price=1.33950,
                stop_price=1.33950,
            )
        assert not any("/positions/otc" in c.url for c in http.calls)
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


# ── backfill tests ──────────────────────────────────────────────────────

def test_subscribe_market_data_uses_lightstreamer_price_stream_and_emits_closed_bar():
    async def _impl():
        http = _FakeHttpSession()
        stream = _FakeStreamingClient()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ1",
                    "accounts": [{"accountId": "Z6BHQ1"}],
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/markets/CS.D.GBPUSD.MINI.IP", _FakeResponse(
                status_code=200,
                _json_body={
                    "snapshot": {
                        "bid": 1.3400,
                        "offer": 1.3402,
                        "updateTime": "12:00:00",
                    },
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter_with_stream(http, stream)
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="GBPUSD", exchange="IG")

        assert len(stream.subscriptions) == 1
        assert stream.subscriptions[0]["account_id"] == "Z6BHQ1"
        spec = stream.subscriptions[0]["spec"]
        assert spec.epic == "CS.D.GBPUSD.MINI.IP"

        stream.emit(spec, {
            "BIDPRICE1": "1.3410",
            "ASKPRICE1": "1.3412",
            "TIMESTAMP": str(int(dt.datetime(
                2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
        })
        stream.emit(spec, {
            "BIDPRICE1": "1.3420",
            "ASKPRICE1": "1.3424",
            "TIMESTAMP": str(int(dt.datetime(
                2026, 6, 2, 12, 1, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
        })
        await asyncio.sleep(0.05)

        events = []
        while not adapter._events_q.empty():
            events.append(await adapter._events_q.get())
        bars = [e.bar for e in events if e.kind == BrokerEventKind.MARKET_DATA_BAR]
        assert len(bars) == 1
        assert bars[0].symbol == "GBPUSD"
        assert bars[0].timestamp == dt.datetime(
            2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
        )
        assert bars[0].open == pytest.approx(1.3411)
        assert bars[0].high == pytest.approx(1.3411)
        assert bars[0].low == pytest.approx(1.3411)
        assert bars[0].close == pytest.approx(1.3411)

        await adapter.disconnect()

    _run(_impl())


def test_chart_tick_stream_fields_emit_closed_bar():
    async def _impl():
        adapter = _adapter(_FakeHttpSession())
        spec = _SYMBOL_MAP["GBPUSD"]

        await adapter._handle_stream_price_update(spec, {
            "BID": "1.3510",
            "OFR": "1.3512",
            "UTM": str(int(dt.datetime(
                2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
        })
        await adapter._handle_stream_price_update(spec, {
            "BID": "1.3520",
            "OFR": "1.3524",
            "UTM": str(int(dt.datetime(
                2026, 6, 2, 12, 1, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
        })

        events = []
        while not adapter._events_q.empty():
            events.append(await adapter._events_q.get())
        bars = [e.bar for e in events if e.kind == BrokerEventKind.MARKET_DATA_BAR]
        assert len(bars) == 1
        assert bars[0].symbol == "GBPUSD"
        assert bars[0].timestamp == dt.datetime(
            2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
        )
        assert bars[0].open == pytest.approx(1.3511)

    _run(_impl())


def test_chart_1minute_stream_fields_emit_native_closed_bar():
    async def _impl():
        adapter = _adapter(_FakeHttpSession())
        spec = _SYMBOL_MAP["GBPUSD"]

        await adapter._handle_stream_price_update(spec, {
            "BID_OPEN": "1.3500",
            "BID_HIGH": "1.3520",
            "BID_LOW": "1.3490",
            "BID_CLOSE": "1.3510",
            "OFR_OPEN": "1.3502",
            "OFR_HIGH": "1.3524",
            "OFR_LOW": "1.3494",
            "OFR_CLOSE": "1.3514",
            "UTM": str(int(dt.datetime(
                2026, 6, 2, 12, 0, 31, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
            "TTV": "17",
            "CONS_END": "1",
        })

        event = await adapter._events_q.get()
        assert event.kind == BrokerEventKind.MARKET_DATA_BAR
        assert event.bar is not None
        assert event.bar.symbol == "GBPUSD"
        assert event.bar.timestamp == dt.datetime(
            2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
        )
        assert event.bar.open == pytest.approx(1.3501)
        assert event.bar.high == pytest.approx(1.3522)
        assert event.bar.low == pytest.approx(1.3492)
        assert event.bar.close == pytest.approx(1.3512)
        assert event.bar.volume == 17

    _run(_impl())


def test_chart_1minute_stream_ignores_provisional_bar():
    async def _impl():
        adapter = _adapter(_FakeHttpSession())
        spec = _SYMBOL_MAP["GBPUSD"]

        await adapter._handle_stream_price_update(spec, {
            "BID_OPEN": "1.3500",
            "OFR_OPEN": "1.3502",
            "UTM": str(int(dt.datetime(
                2026, 6, 2, 12, 0, tzinfo=dt.timezone.utc,
            ).timestamp() * 1000)),
            "CONS_END": "0",
        })

        assert adapter._events_q.empty()

    _run(_impl())


def test_poll_once_skips_rest_market_ticks_when_streaming_connected():
    async def _impl():
        http = _FakeHttpSession()
        stream = _FakeStreamingClient()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={
                    "currentAccountId": "Z6BHQ1",
                    "accounts": [{"accountId": "Z6BHQ1"}],
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/markets/CS.D.GBPUSD.MINI.IP", _FakeResponse(
                status_code=200,
                _json_body={
                    "snapshot": {
                        "bid": 1.3400,
                        "offer": 1.3402,
                        "updateTime": "12:00:00",
                    },
                },
                content=b'{}',
            )),
            ("GET", "/accounts", _FakeResponse(
                status_code=200,
                _json_body={"accounts": []},
                content=b'{}',
            )),
            ("GET", "/positions", _FakeResponse(
                status_code=200,
                _json_body={"positions": []},
                content=b'{}',
            )),
        ]
        adapter = _adapter_with_stream(http, stream)
        await adapter.connect()
        await adapter.subscribe_market_data(symbol="GBPUSD", exchange="IG")

        market_calls_before = len([
            c for c in http.calls
            if c.method == "GET" and "/markets/CS.D.GBPUSD.MINI.IP" in c.url
        ])
        await adapter._poll_once()
        market_calls_after = len([
            c for c in http.calls
            if c.method == "GET" and "/markets/CS.D.GBPUSD.MINI.IP" in c.url
        ])

        assert market_calls_after == market_calls_before
        await adapter.disconnect()

    _run(_impl())


def test_get_recent_candles_parses_price_blocks_to_mid_candles():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/prices/", _FakeResponse(
                status_code=200,
                _json_body={
                    "prices": [
                        {
                            "snapshotTime": "2026/05/21 14:00:00",
                            "openPrice": {"bid": 1.34000, "ask": 1.34002},
                            "highPrice": {"bid": 1.34020, "ask": 1.34022},
                            "lowPrice": {"bid": 1.33990, "ask": 1.33992},
                            "closePrice": {"bid": 1.34010, "ask": 1.34012},
                            "lastTradedVolume": 150,
                        },
                        {
                            "snapshotTime": "2026/05/21 14:01:00",
                            "openPrice": {"bid": 1.34010, "ask": 1.34012},
                            "highPrice": {"bid": 1.34030, "ask": 1.34032},
                            "lowPrice": {"bid": 1.34005, "ask": 1.34007},
                            "closePrice": {"bid": 1.34025, "ask": 1.34027},
                            "lastTradedVolume": 200,
                        },
                    ],
                },
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        candles = await adapter.get_recent_candles(
            symbol="GBPUSD", count=2, timeframe_minutes=1,
        )
        price_call = next(c for c in http.calls if c.method == "GET" and "/prices/" in c.url)
        assert price_call.headers["VERSION"] == "2"
        assert "/prices/CS.D.GBPUSD.MINI.IP/MINUTE/2" in price_call.url
        assert len(candles) == 2
        # Mid prices: average of bid + ask per O/H/L/C
        assert candles[0].open == pytest.approx((1.34000 + 1.34002) / 2)
        assert candles[0].close == pytest.approx((1.34010 + 1.34012) / 2)
        assert candles[0].volume == 150
        # Chronological ordering enforced
        assert candles[0].timestamp < candles[1].timestamp
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_get_recent_candles_empty_response_returns_empty_tuple():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("GET", "/prices/", _FakeResponse(
                status_code=200,
                _json_body={"prices": []},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        candles = await adapter.get_recent_candles(
            symbol="GBPUSD", count=480, timeframe_minutes=1,
        )
        assert candles == ()
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_get_recent_candles_rejects_unsupported_timeframe():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        with pytest.raises(BrokerError) as exc:
            await adapter.get_recent_candles(
                symbol="GBPUSD", count=10, timeframe_minutes=7,
            )
        assert "timeframe_minutes=7" in str(exc.value)
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


# ── cancel + disconnect tests ───────────────────────────────────────────

def test_cancel_order_requires_server_order_id():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        with pytest.raises(BrokerError) as exc:
            await adapter.cancel_order(client_order_id="abc", server_order_id="")
        assert "server_order_id" in str(exc.value)
        if adapter._poll_task is not None:
            adapter._poll_task.cancel()
            try:
                await adapter._poll_task
            except asyncio.CancelledError:
                pass

    _run(_impl())


def test_disconnect_idempotent():
    async def _impl():
        http = _FakeHttpSession()
        http.scripted = [
            ("POST", "/session", _FakeResponse(
                status_code=200,
                _json_body={"currentAccountId": "Z6BHQ1", "accounts": []},
                headers={"CST": "cst", "X-SECURITY-TOKEN": "sec"},
                content=b'{}',
            )),
            ("DELETE", "/session", _FakeResponse(
                status_code=200, _json_body={}, content=b'{}',
            )),
        ]
        adapter = _adapter(http)
        await adapter.connect()
        await adapter.disconnect()
        # Second disconnect must be no-op (idempotent contract)
        await adapter.disconnect()
        # CST cleared
        assert adapter._cst is None
        assert adapter._connected is False

    _run(_impl())
