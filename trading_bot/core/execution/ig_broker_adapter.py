"""IGBrokerAdapter — Kate's Front 7 lane against IG Markets REST API.

This wraps IG's REST API behind the `BrokerAdapter` ABC for the UK
spread-bet path. Spread-betting profits are exempt from UK Capital
Gains Tax for UK residents, which is the strategic reason this lane
exists alongside the CFD-based MT5 Front 4 lane.

Design choices
--------------
1. **Lightstreamer-first prices.** IG's REST `/markets/{epic}`
   endpoint is metadata/session sanity only, not the live tick source.
   Live prices arrive through Lightstreamer PRICE subscriptions and
   are aggregated locally into closed minute bars. If the optional
   `lightstreamer-client-lib` package is unavailable, the adapter
   falls back to low-cadence REST polling for continuity.

2. **Auth model.** POST `/session` returns CST + X-SECURITY-TOKEN
   headers. Every subsequent request includes those plus the API key.
   IG defaults the active account to whatever's marked `preferred`
   in the user's My IG settings — we explicitly PUT `/session` after
   auth to switch to the spread-bet account (Z6BHQ1) so Kate trades
   the CGT-free product even if the CFD account is web-default.

3. **Secrets.** Loaded from `.mcp-brain/config/secrets.json` under
   the `"ig"` key. NEVER committed to git. Same pattern as the
   Telegram alerts already use.

4. **Symbol mapping.** IG uses "epics" like
   `CS.D.GBPUSD.MINI.IP` for spread-bet GBP/USD mini. Each pair has
   a different epic for CFD vs spread-bet vs the standard size.
   `IGSymbolSpec` carries the epic plus the
   `quantity_per_lot` multiplier (spread-bet size is in £/point,
   so 1 MT5 lot = 10 IG spread-bet units for the standard mini
   pair — verify per epic before live trading).

5. **Backfill.** GET `/prices/{epic}/{resolution}/{numPoints}` is
   the IG equivalent of MT5's `copy_rates_from_pos` — fixes the
   480-candle history-window blocker the same way.

Runtime prerequisite: secrets.json contains valid `ig.api_key`,
`ig.username`, `ig.password`, `ig.active_account_id`. The first
connect() will fail loudly if any of those are missing.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Optional, Protocol

import requests

from . import dtc_protocol as proto
from ..data import Candle
from .broker_adapter import (
    AccountBalanceEvent,
    BarEvent,
    BrokerAdapter,
    BrokerError,
    BrokerEvent,
    BrokerEventKind,
    BrokerSymbolSpec,
    MarketDataTick,
    OrderEvent,
    PositionEvent,
)

logger = logging.getLogger(__name__)

# Default secrets location on Kate Host VPS and workstation. Override
# via KATE_SECRETS_PATH env var.
_DEFAULT_SECRETS_PATH = Path(r"C:\models\omni\.mcp-brain\config\secrets.json")

_DEMO_BASE = "https://demo-api.ig.com/gateway/deal"
_LIVE_BASE = "https://api.ig.com/gateway/deal"


@dataclass(frozen=True)
class IGSymbolSpec:
    """Per-instrument mapping for IG epics.

    `epic` is the IG identifier (different for CFD vs spread-bet vs
    standard size). `quantity_per_lot` converts the strategy's
    "1 lot" output to IG's spread-bet size (£/point). For most FX
    mini pairs, 1 MT5 lot ≈ 10 IG spread-bet units, but VERIFY
    against IG's market details endpoint before live trading.
    `pip_decimal_position` is the digit count to the right of the
    decimal that constitutes one pip (4 for most FX, 2 for JPY pairs).
    """
    logical_symbol: str               # e.g. "GBPUSD"
    epic: str                         # e.g. "CS.D.GBPUSD.MINI.IP"
    quantity_per_lot: float = 10.0    # spread-bet £/point per 1 lot
    pip_decimal_position: int = 4     # 4 for most FX, 2 for JPY


@dataclass(frozen=True)
class IGConfig:
    """Runtime config for IGBrokerAdapter.

    Resolved from secrets.json + env vars by `from_secrets()`.
    Never logged at INFO. Adapter __repr__ deliberately masks
    api_key and password.
    """
    api_key: str
    username: str
    password: str
    active_account_id: str            # Z6BHQ0 (CFD) or Z6BHQ1 (spread-bet)
    environment: str = "demo"         # "demo" or "live"
    poll_interval_seconds: float = 60.0  # one tick per minute is enough for v0
    request_timeout_seconds: float = 15.0

    @property
    def base_url(self) -> str:
        return _DEMO_BASE if self.environment == "demo" else _LIVE_BASE

    @classmethod
    def from_secrets(
        cls,
        *,
        secrets_path: Optional[Path] = None,
        environment: Optional[str] = None,
        active_account_id: Optional[str] = None,
    ) -> "IGConfig":
        """Load creds from secrets.json. Optional overrides for tests.

        Raises BrokerError if any required field is missing — better to
        fail loudly than auth silently against an empty key.
        """
        path = secrets_path or Path(
            os.getenv("KATE_SECRETS_PATH", str(_DEFAULT_SECRETS_PATH))
        )
        if not path.exists():
            raise BrokerError(
                f"IG secrets file not found at {path}. Add an 'ig' block "
                f"with api_key, username, password, active_account_id."
            )
        try:
            secrets = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise BrokerError(f"IG secrets load failed: {exc}") from exc
        ig = secrets.get("ig") or {}
        api_key = ig.get("api_key")
        username = ig.get("username")
        password = ig.get("password")
        env = environment or ig.get("environment") or "demo"
        account_id = active_account_id or ig.get("active_account_id")
        missing = [k for k, v in (
            ("api_key", api_key),
            ("username", username),
            ("password", password),
            ("active_account_id", account_id),
        ) if not v]
        if missing:
            raise BrokerError(
                f"IG secrets missing required field(s): {missing}. "
                f"Edit {path} under the 'ig' block."
            )
        return cls(
            api_key=api_key,
            username=username,
            password=password,
            active_account_id=account_id,
            environment=env,
        )


IGStreamUpdateCallback = Callable[[IGSymbolSpec, dict[str, Any]], None]


class IGStreamingClient(Protocol):
    async def connect(
        self, *, endpoint: str, account_id: str, cst: str, security_token: str,
    ) -> None:
        ...

    async def subscribe_price(
        self, *, spec: IGSymbolSpec, account_id: str,
        callback: IGStreamUpdateCallback,
    ) -> None:
        ...

    async def disconnect(self) -> None:
        ...


@dataclass
class _MinuteBarBuilder:
    minute_start: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 1

    @classmethod
    def from_tick(cls, tick: MarketDataTick) -> "_MinuteBarBuilder":
        minute = _floor_utc_minute(tick.timestamp)
        price = float(tick.last_price)
        return cls(
            minute_start=minute,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=max(1, int(tick.last_size or 0)),
        )

    def update(self, tick: MarketDataTick) -> None:
        price = float(tick.last_price)
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += max(1, int(tick.last_size or 0))

    def to_bar_event(self, symbol: str) -> BarEvent:
        return BarEvent(
            symbol=symbol,
            timestamp=self.minute_start,
            timeframe_minutes=1,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
        )


@dataclass(frozen=True)
class _IGStreamRoute:
    name: str
    mode: str
    item: str
    fields: list[str]
    data_adapter: Optional[str] = None
    snapshot: Optional[str] = None


class _LightstreamerSDKPriceClient:
    """Thin wrapper around lightstreamer-client-lib.

    IG's current price stream is PRICE:{account}:{epic} through the
    Pricing adapter. Some demo accounts reject that item with Lightstreamer
    code 21, so the wrapper can fall back to the documented chart-tick
    stream. MARKET:{epic} is deprecated/decommissioned, so keep that older
    item shape out of the production path.
    """

    _PRICE_FIELDS = ["BIDPRICE1", "ASKPRICE1", "TIMESTAMP", "DELAY"]
    _CHART_TICK_FIELDS = ["BID", "OFR", "LTP", "LTV", "TTV", "UTM"]
    _CHART_1MINUTE_FIELDS = [
        "BID_OPEN",
        "BID_HIGH",
        "BID_LOW",
        "BID_CLOSE",
        "OFR_OPEN",
        "OFR_HIGH",
        "OFR_LOW",
        "OFR_CLOSE",
        "LTP_OPEN",
        "LTP_HIGH",
        "LTP_LOW",
        "LTP_CLOSE",
        "LTV",
        "TTV",
        "UTM",
        "CONS_END",
    ]

    def __init__(self) -> None:
        self._client: Any = None
        self._subscriptions: list[Any] = []
        self._last_status: Optional[str] = None
        self._last_server_error: Optional[str] = None

    async def connect(
        self, *, endpoint: str, account_id: str, cst: str, security_token: str,
    ) -> None:
        try:
            from lightstreamer.client import ClientListener, LightstreamerClient
        except ImportError as exc:
            raise BrokerError(
                "IG Lightstreamer support requires package "
                "'lightstreamer-client-lib'. Install it before enabling "
                "Front 7 streaming."
            ) from exc

        loop = asyncio.get_running_loop()

        endpoint_candidates = _lightstreamer_endpoint_candidates(endpoint)
        last_status = ""
        for candidate in endpoint_candidates:
            for transport in (None, "HTTP-STREAMING", "HTTP-POLLING"):
                status_queue: asyncio.Queue[str] = asyncio.Queue()

                class _ClientListener(ClientListener):
                    def onStatusChange(_self, status: str) -> None:
                        self._last_status = status
                        logger.info(
                            "IG Lightstreamer status=%s endpoint=%s transport=%s",
                            status,
                            candidate,
                            transport or "auto",
                        )
                        asyncio.run_coroutine_threadsafe(
                            status_queue.put(status), loop,
                        )

                    def onServerError(_self, code: Any, message: Any) -> None:
                        self._last_server_error = f"{code}: {message}"
                        logger.error(
                            "IG Lightstreamer server error code=%s message=%s",
                            code, message,
                        )
                        asyncio.run_coroutine_threadsafe(
                            status_queue.put(f"SERVER_ERROR:{code}:{message}"),
                            loop,
                        )

                def _connect() -> Any:
                    client = LightstreamerClient(candidate, "DEFAULT")
                    client.addListener(_ClientListener())
                    client.connectionDetails.setUser(account_id)
                    client.connectionDetails.setPassword(
                        f"CST-{cst}|XST-{security_token}"
                    )
                    if transport is not None:
                        client.connectionOptions.setForcedTransport(transport)
                    client.connect()
                    return client

                client = await asyncio.to_thread(_connect)
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline:
                    try:
                        status = await asyncio.wait_for(
                            status_queue.get(),
                            timeout=max(0.1, deadline - time.monotonic()),
                        )
                    except asyncio.TimeoutError:
                        break
                    last_status = status
                    if status.startswith("CONNECTED:"):
                        self._client = client
                        logger.info(
                            "IG Lightstreamer connected endpoint=%s "
                            "transport=%s status=%s",
                            candidate,
                            transport or "auto",
                            status,
                        )
                        return
                    if status.startswith("SERVER_ERROR:"):
                        break
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(client.disconnect)
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(client.closeConnection)
                logger.warning(
                    "IG Lightstreamer candidate failed endpoint=%s "
                    "transport=%s last_status=%s server_error=%s",
                    candidate,
                    transport or "auto",
                    last_status or self._last_status,
                    self._last_server_error,
                )

        raise BrokerError(
            "IG Lightstreamer failed to reach CONNECTED status. "
            f"last_status={last_status or self._last_status!r} "
            f"server_error={self._last_server_error!r} "
            f"endpoint_candidates={endpoint_candidates!r}"
        )

    async def subscribe_price(
        self, *, spec: IGSymbolSpec, account_id: str,
        callback: IGStreamUpdateCallback,
    ) -> None:
        if self._client is None:
            raise BrokerError("IG Lightstreamer client is not connected")
        try:
            from lightstreamer.client import Subscription, SubscriptionListener
        except ImportError as exc:
            raise BrokerError(
                "IG Lightstreamer support requires package "
                "'lightstreamer-client-lib'."
            ) from exc

        routes = [
            _IGStreamRoute(
                name="PRICE",
                mode="MERGE",
                item=f"PRICE:{account_id}:{spec.epic}",
                fields=list(self._PRICE_FIELDS),
                data_adapter="Pricing",
                snapshot="yes",
            ),
            _IGStreamRoute(
                name="CHART_1MINUTE",
                mode="MERGE",
                item=f"CHART:{spec.epic}:1MINUTE",
                fields=list(self._CHART_1MINUTE_FIELDS),
            ),
            _IGStreamRoute(
                name="CHART_TICK",
                mode="DISTINCT",
                item=f"CHART:{spec.epic}:TICK",
                fields=list(self._CHART_TICK_FIELDS),
            ),
        ]

        for route in routes:
            accepted = await self._try_subscribe_route(
                Subscription=Subscription,
                SubscriptionListener=SubscriptionListener,
                route=route,
                spec=spec,
                callback=callback,
            )
            if accepted:
                return

        raise BrokerError(
            "IG Lightstreamer could not subscribe to any documented price "
            f"route for {spec.logical_symbol}/{spec.epic}"
        )

    async def _try_subscribe_route(
        self,
        *,
        Subscription: Any,
        SubscriptionListener: Any,
        route: _IGStreamRoute,
        spec: IGSymbolSpec,
        callback: IGStreamUpdateCallback,
    ) -> bool:
        loop = asyncio.get_running_loop()
        status_queue: asyncio.Queue[tuple[str, Optional[Any], Optional[Any]]] = (
            asyncio.Queue()
        )

        class _Listener(SubscriptionListener):
            def onSubscription(self) -> None:
                logger.info(
                    "IG Lightstreamer subscription accepted route=%s item=%s",
                    route.name,
                    route.item,
                )
                asyncio.run_coroutine_threadsafe(
                    status_queue.put(("accepted", None, None)), loop,
                )

            def onItemUpdate(self, update: Any) -> None:
                callback(
                    spec,
                    {field: update.getValue(field) for field in route.fields},
                )

            def onSubscriptionError(self, code: Any, message: Any) -> None:
                logger.error(
                    "IG Lightstreamer subscription error route=%s item=%s "
                    "code=%s message=%s",
                    route.name,
                    route.item,
                    code,
                    message,
                )
                asyncio.run_coroutine_threadsafe(
                    status_queue.put(("error", code, message)), loop,
                )

        def _subscribe() -> None:
            sub = Subscription(route.mode, [route.item], route.fields)
            if route.data_adapter:
                sub.setDataAdapter(route.data_adapter)
            if route.snapshot:
                sub.setRequestedSnapshot(route.snapshot)
            sub.addListener(_Listener())
            self._client.subscribe(sub)
            self._subscriptions.append(sub)

        await asyncio.to_thread(_subscribe)
        try:
            status, code, message = await asyncio.wait_for(
                status_queue.get(),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "IG Lightstreamer subscription route timed out route=%s item=%s",
                route.name,
                route.item,
            )
            return False
        if status == "accepted":
            return True
        logger.warning(
            "IG Lightstreamer route rejected route=%s item=%s code=%s message=%s",
            route.name,
            route.item,
            code,
            message,
        )
        return False

    async def disconnect(self) -> None:
        client = self._client
        if client is None:
            return

        def _disconnect() -> None:
            for sub in list(self._subscriptions):
                with contextlib.suppress(Exception):
                    client.unsubscribe(sub)
            self._subscriptions.clear()
            with contextlib.suppress(Exception):
                client.disconnect()
            with contextlib.suppress(Exception):
                client.closeConnection()
            self._client = None

        await asyncio.to_thread(_disconnect)


class IGBrokerAdapter(BrokerAdapter):
    """BrokerAdapter implementation for IG Markets REST API.

    `symbol_map` maps Kate logical symbols (e.g. "GBPUSD") to
    `IGSymbolSpec` carrying the IG epic + sizing config.

    `http_session` is dependency-injected to keep tests offline: real
    deploys pass `requests.Session()`; tests pass a fake with
    pre-canned responses. None defaults to a fresh `requests.Session`.
    """

    def __init__(
        self,
        *,
        config: IGConfig,
        symbol_map: dict[str, IGSymbolSpec],
        http_session: Optional[Any] = None,
        streaming_client_factory: Optional[Callable[[], IGStreamingClient]] = None,
        emit_stream_ticks: bool = False,
        require_streaming: bool = False,
    ) -> None:
        self.config = config
        self.symbol_map = dict(symbol_map)
        self._http = http_session if http_session is not None else requests.Session()
        self._streaming_client_factory = (
            streaming_client_factory or _LightstreamerSDKPriceClient
        )
        self._stream_client: Optional[IGStreamingClient] = None
        self._stream_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stream_subscribed_symbols: set[str] = set()
        self._emit_stream_ticks = emit_stream_ticks
        self._require_streaming = require_streaming
        self._connected = False
        self._cst: Optional[str] = None
        self._security_token: Optional[str] = None
        self._client_id: Optional[str] = None
        self._lightstreamer_endpoint: Optional[str] = None
        self._subscribed_symbols: set[str] = set()
        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._poll_task: Optional[asyncio.Task[None]] = None
        self._bar_builders: dict[str, _MinuteBarBuilder] = {}
        self._last_account_hash: Optional[tuple[Any, ...]] = None
        self._last_positions_hash: Optional[tuple[Any, ...]] = None
        self._last_orders_hash: Optional[tuple[Any, ...]] = None
        self._last_tick_hashes: dict[str, tuple[Any, ...]] = {}
        # Per-ticket position snapshots — used to detect closures (broker
        # filled SL/TP brackets) the same way the MT5 adapter does.
        self._known_position_dealrefs: dict[str, dict[str, Any]] = {}

    def __repr__(self) -> str:
        return (
            f"IGBrokerAdapter(env={self.config.environment}, "
            f"account={self.config.active_account_id}, "
            f"symbols={list(self.symbol_map)}, "
            f"connected={self._connected})"
        )

    # ── HTTP layer ────────────────────────────────────────────────────────

    def _headers(self, *, version: int = 1, authenticated: bool = True) -> dict[str, str]:
        h: dict[str, str] = {
            "X-IG-API-KEY": self.config.api_key,
            "Accept": "application/json; charset=UTF-8",
            "Content-Type": "application/json; charset=UTF-8",
            "VERSION": str(version),
        }
        if authenticated:
            if not self._cst or not self._security_token:
                raise BrokerError(
                    "IG adapter not authenticated — call connect() first"
                )
            h["CST"] = self._cst
            h["X-SECURITY-TOKEN"] = self._security_token
        return h

    async def _request(
        self,
        method: str,
        path: str,
        *,
        version: int = 1,
        authenticated: bool = True,
        json_body: Optional[dict[str, Any]] = None,
        return_headers: bool = False,
    ) -> Any:
        """Wrap requests.{get,post,put,delete} in to_thread.

        Returns parsed JSON body. If return_headers=True, returns
        (body, headers) tuple so the caller can extract CST etc on
        the auth path.
        """
        url = f"{self.config.base_url}{path}"
        headers = self._headers(version=version, authenticated=authenticated)
        call = getattr(self._http, method.lower())
        try:
            r = await asyncio.to_thread(
                call,
                url,
                headers=headers,
                json=json_body,
                timeout=self.config.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise BrokerError(f"IG {method} {path} transport failure: {exc}") from exc
        if r.status_code >= 400:
            try:
                body = r.json()
            except Exception:
                body = r.text
            raise BrokerError(
                f"IG {method} {path} returned {r.status_code}: {body}"
            )
        try:
            body = r.json() if r.content else {}
        except Exception as exc:
            raise BrokerError(
                f"IG {method} {path} returned non-JSON body: {exc}"
            ) from exc
        if return_headers:
            return body, dict(r.headers)
        return body

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self) -> None:
        if self._connected:
            return
        # POST /session — get CST + X-SECURITY-TOKEN
        body, resp_headers = await self._request(
            "POST",
            "/session",
            version=2,
            authenticated=False,
            json_body={
                "identifier": self.config.username,
                "password": self.config.password,
            },
            return_headers=True,
        )
        self._cst = resp_headers.get("CST")
        self._security_token = resp_headers.get("X-SECURITY-TOKEN")
        if not self._cst or not self._security_token:
            raise BrokerError(
                "IG /session response missing CST or X-SECURITY-TOKEN "
                "headers — auth probably succeeded but session is unusable"
            )
        self._client_id = body.get("clientId")
        self._lightstreamer_endpoint = body.get("lightstreamerEndpoint")
        current = body.get("currentAccountId")
        logger.info(
            "IG /session OK: clientId=%s currentAccount=%s targetAccount=%s "
            "accounts=%s",
            self._client_id, current, self.config.active_account_id,
            [a.get("accountId") for a in (body.get("accounts") or [])],
        )

        # PUT /session — switch to the configured active account (typically
        # Z6BHQ1 spread-bet) regardless of the user's preferred-account
        # setting in My IG. This is load-bearing for the CGT-free path.
        if current != self.config.active_account_id:
            _, put_headers = await self._request(
                "PUT",
                "/session",
                version=1,
                authenticated=True,
                json_body={
                    "accountId": self.config.active_account_id,
                    "defaultAccountId": self.config.active_account_id,
                },
                return_headers=True,
            )
            if put_headers.get("CST"):
                self._cst = put_headers.get("CST")
            if put_headers.get("X-SECURITY-TOKEN"):
                self._security_token = put_headers.get("X-SECURITY-TOKEN")
            logger.info(
                "IG account switched to %s (was %s)",
                self.config.active_account_id, current,
            )

        self._connected = True
        self._stream_loop = asyncio.get_running_loop()
        await self._start_streaming_client()
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.CONNECTED,
            received_at=time.time(),
        ))
        # Emit LOGON_OK immediately after — IG doesn't have a separate
        # logon step after the REST session opens.
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.LOGON_OK,
            received_at=time.time(),
        ))
        # Background poll loop drives positions/orders/account/tick events.
        self._poll_task = asyncio.create_task(self._poll_loop(), name="ig-adapter-poll")

    async def disconnect(self) -> None:
        if self._stream_client is not None:
            with contextlib.suppress(Exception):
                await self._stream_client.disconnect()
            self._stream_client = None
        self._stream_subscribed_symbols.clear()
        self._stream_loop = None
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        if self._connected and self._cst:
            # DELETE /session — clean logout, ignore errors (best-effort)
            try:
                await self._request("DELETE", "/session", version=1)
            except BrokerError:
                logger.warning("IG /session DELETE failed during disconnect; continuing")
        self._connected = False
        self._cst = None
        self._security_token = None
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.DISCONNECTED,
            received_at=time.time(),
        ))

    async def logon(self, **kwargs: Any) -> None:
        """No-op for IG. The REST session opened in connect() IS the logon.
        This stub exists because the supervisor's engine wiring calls
        broker.logon() after connect() unconditionally."""
        return None

    # ── State queries ────────────────────────────────────────────────────

    async def request_account_state(
        self, *, trade_account: str,
    ) -> AccountBalanceEvent:
        self._require_connected()
        body = await self._request("GET", "/accounts", version=1)
        accounts = body.get("accounts") or []
        target = self.config.active_account_id
        match = next((a for a in accounts if a.get("accountId") == target), None)
        if match is None:
            raise BrokerError(
                f"IG /accounts response did not include active account {target!r}"
            )
        balance_block = match.get("balance") or {}
        balance = float(balance_block.get("balance") or 0.0)
        # IG's "available" already nets out open-position margin
        available = float(balance_block.get("available") or balance)
        deposit = float(balance_block.get("deposit") or 0.0)
        profit_loss = float(balance_block.get("profitLoss") or 0.0)
        currency = str(match.get("currency") or "GBP")
        return AccountBalanceEvent(
            cash=balance,
            nlv=balance + profit_loss,
            pnl=profit_loss,
            margin_requirement=max(0.0, balance - available),
            currency=currency,
        )

    async def request_positions(
        self, *, trade_account: str,
    ) -> tuple[PositionEvent, ...]:
        self._require_connected()
        body = await self._request("GET", "/positions", version=2)
        out: list[PositionEvent] = []
        for row in body.get("positions") or []:
            pos = row.get("position") or {}
            market = row.get("market") or {}
            epic = market.get("epic", "")
            logical = self._epic_to_logical(epic)
            if not logical:
                # Position on an instrument we're not subscribed to — skip
                continue
            direction = (pos.get("direction") or "").upper()
            spec = self._spec(logical)
            qty = float(pos.get("size") or 0.0) / spec.quantity_per_lot
            if direction == "SELL":
                qty = -qty
            out.append(PositionEvent(
                symbol=logical,
                quantity=qty,
                avg_price=float(pos.get("level") or 0.0),
                side=proto.SELL if direction == "SELL" else proto.BUY,
            ))
        return tuple(out)

    async def request_open_orders(
        self, *, trade_account: str,
    ) -> tuple[OrderEvent, ...]:
        self._require_connected()
        body = await self._request("GET", "/workingorders", version=2)
        out: list[OrderEvent] = []
        for row in body.get("workingOrders") or []:
            order = row.get("workingOrderData") or {}
            market = row.get("marketData") or {}
            epic = market.get("epic", "")
            logical = self._epic_to_logical(epic)
            if not logical:
                continue
            deal_id = order.get("dealId", "")
            ref = order.get("dealReference") or deal_id
            direction = (order.get("direction") or "").upper()
            side = proto.BUY if direction == "BUY" else proto.SELL
            spec = self._spec(logical)
            out.append(OrderEvent(
                client_order_id=str(ref),
                symbol=logical,
                side=side,
                quantity=float(order.get("orderSize") or 0.0) / spec.quantity_per_lot,
                server_order_id=str(deal_id),
            ))
        return tuple(out)

    # ── Market data ──────────────────────────────────────────────────────

    async def subscribe_market_data(
        self, *, symbol: str, exchange: str = "",
    ) -> None:
        self._require_connected()
        spec = self._spec(symbol)
        # Seed one current tick so the engine's first candle aggregation
        # has a non-empty open price. Subsequent ticks arrive via the
        # background poll loop.
        tick = await self._fetch_market_tick(spec)
        self._subscribed_symbols.add(spec.logical_symbol)
        logger.info(
            "IG subscribe_market_data: logical=%s epic=%s seed_bid=%s "
            "seed_ask=%s subscribed_count=%d",
            spec.logical_symbol, spec.epic, tick.bid, tick.ask,
            len(self._subscribed_symbols),
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_TICK,
            received_at=time.time(),
            tick=tick,
        ))
        await self._subscribe_streaming_price(spec)

    async def get_recent_candles(
        self, *, symbol: str, count: int, timeframe_minutes: int = 1,
    ) -> tuple[Candle, ...]:
        """Return last N closed bars from IG's price-history endpoint.

        Resolution map: IG uses string codes (MINUTE, MINUTE_5, etc.)
        rather than minute counts. We map our timeframe_minutes int to
        the IG code. Unsupported values raise BrokerError so the engine's
        backfill block falls back to live aggregation rather than
        silently seeding with the wrong resolution.
        """
        self._require_connected()
        if count <= 0:
            return ()
        resolution_map = {
            1: "MINUTE", 5: "MINUTE_5", 10: "MINUTE_10",
            15: "MINUTE_15", 30: "MINUTE_30", 60: "HOUR",
        }
        resolution = resolution_map.get(timeframe_minutes)
        if resolution is None:
            raise BrokerError(
                f"IG get_recent_candles: unsupported timeframe_minutes="
                f"{timeframe_minutes}; supported={list(resolution_map)}"
            )
        spec = self._spec(symbol)
        # IG's count-style /prices URL is exposed on v1/v2. v3 is the
        # bare /prices/{epic} shape and returns 404 for
        # /prices/{epic}/{resolution}/{numPoints} on demo.
        path = f"/prices/{spec.epic}/{resolution}/{count}"
        body = await self._request("GET", path, version=2)
        prices = body.get("prices") or []
        if not prices:
            logger.warning(
                "IG /prices returned 0 bars for %s (epic=%s count=%d "
                "resolution=%s) — backfill empty",
                spec.logical_symbol, spec.epic, count, resolution,
            )
            return ()
        candles: list[Candle] = []
        for p in prices:
            # IG snapshotTime format: "2026/05/21 13:45:00" (UTC, naive).
            # Each bar block has openPrice/highPrice/lowPrice/closePrice
            # objects with bid + ask fields. We use mid prices (bid+ask)/2
            # to match the engine's tick-aggregation behaviour.
            ts_str = p.get("snapshotTime", "")
            try:
                ts = dt.datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
            except ValueError:
                # Some IG responses use "snapshotTimeUTC" with ISO format
                ts_str = p.get("snapshotTimeUTC", "")
                try:
                    ts = dt.datetime.fromisoformat(ts_str.replace("Z", ""))
                except ValueError:
                    logger.warning("IG bar with unparseable timestamp %r — skipping", ts_str)
                    continue
            candles.append(Candle(
                timestamp=ts,
                open=_mid_price(p.get("openPrice")),
                high=_mid_price(p.get("highPrice")),
                low=_mid_price(p.get("lowPrice")),
                close=_mid_price(p.get("closePrice")),
                volume=int(p.get("lastTradedVolume") or 0),
            ))
        candles.sort(key=lambda c: c.timestamp)
        logger.info(
            "IG get_recent_candles: returned %d bars for %s "
            "(timeframe=%dm, first_ts=%s, last_ts=%s)",
            len(candles), spec.logical_symbol, timeframe_minutes,
            candles[0].timestamp.isoformat() if candles else "n/a",
            candles[-1].timestamp.isoformat() if candles else "n/a",
        )
        return tuple(candles)

    async def _fetch_market_tick(self, spec: IGSymbolSpec) -> MarketDataTick:
        body = await self._request("GET", f"/markets/{spec.epic}", version=3)
        snapshot = body.get("snapshot") or {}
        bid = float(snapshot.get("bid") or 0.0)
        offer = float(snapshot.get("offer") or 0.0)
        # IG snapshot times are server-local UTC strings like
        # "2026/05/21 13:45:00". Parse defensively.
        ts_str = snapshot.get("updateTime") or snapshot.get("updateTimeUTC") or ""
        ts: Optional[dt.datetime] = None
        for fmt in ("%Y/%m/%d %H:%M:%S", "%H:%M:%S"):
            try:
                if ":" in ts_str and "/" not in ts_str:
                    today = dt.datetime.utcnow().date()
                    ts = dt.datetime.combine(
                        today, dt.datetime.strptime(ts_str, "%H:%M:%S").time(),
                    )
                else:
                    ts = dt.datetime.strptime(ts_str, fmt)
                break
            except (ValueError, TypeError):
                continue
        if ts is None:
            ts = dt.datetime.utcnow()
        last_price = (bid + offer) / 2.0 if (bid > 0 and offer > 0) else (bid or offer)
        return MarketDataTick(
            symbol=spec.logical_symbol,
            timestamp=ts,
            last_price=last_price,
            last_size=0.0,
            bid=bid or None,
            ask=offer or None,
        )

    # ── Order management ─────────────────────────────────────────────────

    async def _start_streaming_client(self) -> None:
        if not self._lightstreamer_endpoint:
            msg = (
                "IG /session did not return lightstreamerEndpoint; "
                "falling back to REST market polling"
            )
            if self._require_streaming:
                raise BrokerError(msg)
            logger.warning(msg)
            return
        if not self._cst or not self._security_token:
            msg = "IG Lightstreamer not started because session tokens are missing"
            if self._require_streaming:
                raise BrokerError(msg)
            logger.warning(msg)
            return
        try:
            client = self._streaming_client_factory()
            await client.connect(
                endpoint=self._lightstreamer_endpoint,
                account_id=self.config.active_account_id,
                cst=self._cst,
                security_token=self._security_token,
            )
        except Exception as exc:
            if self._require_streaming:
                raise BrokerError(
                    "IG Lightstreamer start failed while streaming is required"
                ) from exc
            logger.warning(
                "IG Lightstreamer start failed; falling back to REST polling: %s",
                exc,
            )
            self._stream_client = None
            return
        self._stream_client = client
        logger.info(
            "IG Lightstreamer connected endpoint=%s account=%s",
            self._lightstreamer_endpoint,
            self.config.active_account_id,
        )

    async def _subscribe_streaming_price(self, spec: IGSymbolSpec) -> None:
        if self._stream_client is None:
            return
        if spec.logical_symbol in self._stream_subscribed_symbols:
            return
        await self._stream_client.subscribe_price(
            spec=spec,
            account_id=self.config.active_account_id,
            callback=self._on_stream_price_update,
        )
        self._stream_subscribed_symbols.add(spec.logical_symbol)
        logger.info(
            "IG Lightstreamer subscribed symbol=%s account=%s epic=%s",
            spec.logical_symbol,
            self.config.active_account_id,
            spec.epic,
        )

    def _on_stream_price_update(
        self, spec: IGSymbolSpec, fields: dict[str, Any],
    ) -> None:
        loop = self._stream_loop
        if loop is None or loop.is_closed():
            logger.warning(
                "IG Lightstreamer update dropped for %s; event loop unavailable",
                spec.logical_symbol,
            )
            return
        asyncio.run_coroutine_threadsafe(
            self._handle_stream_price_update(spec, fields),
            loop,
        )

    async def _handle_stream_price_update(
        self, spec: IGSymbolSpec, fields: dict[str, Any],
    ) -> None:
        if "BID_OPEN" in fields or "OFR_OPEN" in fields or "LTP_OPEN" in fields:
            await self._handle_stream_chart_bar_update(spec, fields)
            return
        bid_raw = fields.get("BIDPRICE1") or fields.get("BID")
        ask_raw = fields.get("ASKPRICE1") or fields.get("OFFER") or fields.get("OFR")
        try:
            bid = float(bid_raw) if bid_raw not in (None, "") else None
            ask = float(ask_raw) if ask_raw not in (None, "") else None
        except (TypeError, ValueError):
            logger.warning(
                "IG Lightstreamer bad price update for %s: %s",
                spec.logical_symbol,
                fields,
            )
            return
        if bid is None and ask is None:
            return
        last_price = (
            (bid + ask) / 2.0 if bid is not None and ask is not None
            else float(bid if bid is not None else ask)
        )
        tick = MarketDataTick(
            symbol=spec.logical_symbol,
            timestamp=_parse_ig_stream_timestamp(
                fields.get("UPDATE_TIME")
                or fields.get("TIMESTAMP")
                or fields.get("UTM")
            ),
            last_price=last_price,
            last_size=1.0,
            bid=bid,
            ask=ask,
        )
        if self._emit_stream_ticks:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.MARKET_DATA_TICK,
                received_at=time.time(),
                tick=tick,
            ))
        await self._update_bar_builder_and_maybe_emit(tick)

    async def _handle_stream_chart_bar_update(
        self, spec: IGSymbolSpec, fields: dict[str, Any],
    ) -> None:
        if not _ig_cons_end_is_true(fields.get("CONS_END")):
            logger.debug(
                "IG provisional chart bar ignored for %s: %s",
                spec.logical_symbol,
                fields,
            )
            return

        try:
            open_price = _ig_mid_or_last(
                bid=fields.get("BID_OPEN"),
                ask=fields.get("OFR_OPEN"),
                last=fields.get("LTP_OPEN"),
            )
            high_price = _ig_mid_or_last(
                bid=fields.get("BID_HIGH"),
                ask=fields.get("OFR_HIGH"),
                last=fields.get("LTP_HIGH"),
            )
            low_price = _ig_mid_or_last(
                bid=fields.get("BID_LOW"),
                ask=fields.get("OFR_LOW"),
                last=fields.get("LTP_LOW"),
            )
            close_price = _ig_mid_or_last(
                bid=fields.get("BID_CLOSE"),
                ask=fields.get("OFR_CLOSE"),
                last=fields.get("LTP_CLOSE"),
            )
        except (TypeError, ValueError) as exc:
            logger.warning(
                "IG Lightstreamer bad chart bar update for %s: %s",
                spec.logical_symbol,
                fields,
            )
            return

        timestamp = _parse_ig_stream_timestamp(fields.get("UTM"))
        volume = _parse_ig_int(fields.get("TTV") or fields.get("LTV"), default=0)
        bar = BarEvent(
            symbol=spec.logical_symbol,
            timestamp=_floor_utc_minute(timestamp),
            timeframe_minutes=1,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
        )
        logger.info(
            "IG CLOSED BAR %s route=CHART_1MINUTE ts=%s "
            "o=%.5f h=%.5f l=%.5f c=%.5f v=%d",
            bar.symbol,
            bar.timestamp.isoformat(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_BAR,
            received_at=time.time(),
            bar=bar,
        ))

    async def _update_bar_builder_and_maybe_emit(
        self, tick: MarketDataTick,
    ) -> None:
        minute = _floor_utc_minute(tick.timestamp)
        builder = self._bar_builders.get(tick.symbol)
        if builder is None:
            self._bar_builders[tick.symbol] = _MinuteBarBuilder.from_tick(tick)
            return
        if minute == builder.minute_start:
            builder.update(tick)
            return
        closed = builder.to_bar_event(tick.symbol)
        self._bar_builders[tick.symbol] = _MinuteBarBuilder.from_tick(tick)
        logger.info(
            "IG CLOSED BAR %s ts=%s o=%.5f h=%.5f l=%.5f c=%.5f v=%d",
            closed.symbol,
            closed.timestamp.isoformat(),
            closed.open,
            closed.high,
            closed.low,
            closed.close,
            closed.volume,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_BAR,
            received_at=time.time(),
            bar=closed,
        ))

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
        signal_close_price: Optional[float] = None,
        target_price: Optional[float] = None,
        free_form_text: str = "",
    ) -> str:
        """Place a position via IG's /positions/otc endpoint.

        IG combines entry + bracket SL/TP in a single request via
        stopLevel/limitLevel absolute price fields. For spread-bet,
        the `size` parameter is in £/point; we convert from MT5
        lots using `spec.quantity_per_lot`.

        IG rejects per-deal `dealReference` strings containing certain
        characters (similar to MT5's comment-validation quirk). We
        sanitize to [a-zA-Z0-9_-] and truncate to 30 chars.
        """
        self._require_connected()
        spec = self._spec(symbol)
        direction = "BUY" if side == proto.BUY else "SELL"
        if order_type != proto.ORDER_TYPE_MARKET:
            raise BrokerError(
                "IG adapter supports only MARKET entries with native "
                "stopLevel/limitLevel brackets; separate stop/limit exit legs "
                "must not be routed through /positions/otc"
            )
        if stop_price is None or stop_price <= 0:
            raise BrokerError(
                "IG adapter requires stop_price on every MARKET entry so the "
                "broker receives a native protective stop in the same request"
            )
        # Convert MT5-style lot to IG spread-bet size
        size = float(quantity) * spec.quantity_per_lot
        # Sanitize dealReference: IG docs say [A-Za-z0-9_-]{1,30}
        import re as _re
        deal_ref = _re.sub(r"[^A-Za-z0-9_\-]", "_", client_order_id or "")[:30]
        if not deal_ref:
            deal_ref = f"kate-{int(time.time())}"

        order_body: dict[str, Any] = {
            "epic": spec.epic,
            "expiry": "-",            # FX spread-bet: no expiry
            "direction": direction,
            "size": size,
            "orderType": "MARKET" if order_type == proto.ORDER_TYPE_MARKET else "LIMIT",
            "guaranteedStop": False,
            "forceOpen": True,        # always open new position vs net out
            "currencyCode": "GBP",
            "dealReference": deal_ref,
        }
        if order_type != proto.ORDER_TYPE_MARKET and price > 0:
            order_body["level"] = float(price)
        if stop_price is not None and stop_price > 0:
            order_body["stopLevel"] = float(stop_price)
        if target_price is not None and target_price > 0:
            order_body["limitLevel"] = float(target_price)

        confirm = await self._request(
            "POST", "/positions/otc", version=2, json_body=order_body,
        )
        deal_reference = confirm.get("dealReference") or deal_ref

        # The confirm response from POST /positions/otc is just the
        # dealReference. To get the dealId + fill status, query
        # /confirms/{dealReference} immediately after.
        details = await self._request(
            "GET", f"/confirms/{deal_reference}", version=1,
        )
        deal_status = (details.get("dealStatus") or "").upper()
        reason = (details.get("reason") or "").upper()
        deal_id = str(details.get("dealId") or "")
        level = float(details.get("level") or 0.0)

        if deal_status != "ACCEPTED":
            rejected = OrderEvent(
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                rejected_reason=f"{deal_status}: {reason}",
                server_order_id=deal_id or None,
            )
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.ORDER_REJECTED,
                received_at=time.time(),
                order=rejected,
            ))
            raise BrokerError(
                f"IG /positions/otc rejected: {deal_status} reason={reason} "
                f"dealReference={deal_reference}"
            )

        order_event = OrderEvent(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            fill_price=level or None,
            fill_quantity=quantity,
            server_order_id=deal_id or None,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ORDER_FILLED,
            received_at=time.time(),
            order=order_event,
        ))
        # Telegram alert mirrors the MT5 adapter shape so CEO gets
        # consistent notifications regardless of broker lane.
        from ..alerts import push_telegram_alert
        sl_str = f" SL={stop_price:.5f}" if stop_price else ""
        tp_str = f" TP={target_price:.5f}" if target_price else ""
        push_telegram_alert(
            f"🟢 *Kate ORDER FILLED (IG)* — {symbol} {direction}\n"
            f"  size={size:.2f}£/pt fill={level:.5f}{sl_str}{tp_str}\n"
            f"  coid={client_order_id}\n"
            f"  dealId={deal_id}",
        )
        return client_order_id

    async def cancel_order(
        self, *, client_order_id: str, server_order_id: str = "",
    ) -> None:
        """Cancel a pending working order by dealId."""
        self._require_connected()
        if not server_order_id:
            raise BrokerError(
                "IG cancel_order requires server_order_id (dealId) — "
                "client_order_id alone isn't a broker-side handle"
            )
        await self._request(
            "DELETE", f"/workingorders/otc/{server_order_id}", version=2,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ORDER_CANCELED,
            received_at=time.time(),
            order=OrderEvent(
                client_order_id=client_order_id,
                symbol="",
                side=0,
                quantity=0.0,
                server_order_id=server_order_id,
            ),
        ))

    # ── Background poll loop ─────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while self._connected:
            await asyncio.sleep(self.config.poll_interval_seconds)
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("IGBrokerAdapter poll failed; continuing")

    async def _poll_once(self) -> None:
        # Account
        try:
            account_event = await self.request_account_state(
                trade_account=self.config.active_account_id,
            )
            acct_hash = (account_event.cash, account_event.nlv, account_event.pnl)
            if acct_hash != self._last_account_hash:
                self._last_account_hash = acct_hash
                await self._events_q.put(BrokerEvent(
                    kind=BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
                    received_at=time.time(),
                    balance=account_event,
                ))
        except BrokerError as exc:
            logger.warning("IG poll account_state failed: %s", exc)

        # Positions — emit deltas + detect closures via dealref tracking
        try:
            positions = await self._fetch_raw_positions()
            current_refs: dict[str, dict[str, Any]] = {}
            for row in positions:
                pos = row.get("position") or {}
                market = row.get("market") or {}
                deal_id = str(pos.get("dealId") or "")
                deal_ref = str(pos.get("dealReference") or deal_id)
                if not deal_ref:
                    continue
                current_refs[deal_ref] = {
                    "symbol": self._epic_to_logical(market.get("epic", "")),
                    "level": float(pos.get("level") or 0.0),
                    "size": float(pos.get("size") or 0.0),
                    "direction": pos.get("direction"),
                }
            pos_hash = tuple(sorted(current_refs.keys()))
            if pos_hash != self._last_positions_hash:
                self._last_positions_hash = pos_hash
                for row in positions:
                    pos = row.get("position") or {}
                    market = row.get("market") or {}
                    epic = market.get("epic", "")
                    logical = self._epic_to_logical(epic)
                    if not logical:
                        continue
                    direction = (pos.get("direction") or "").upper()
                    spec = self._spec(logical)
                    qty = float(pos.get("size") or 0.0) / spec.quantity_per_lot
                    if direction == "SELL":
                        qty = -qty
                    await self._events_q.put(BrokerEvent(
                        kind=BrokerEventKind.POSITION_UPDATE,
                        received_at=time.time(),
                        position=PositionEvent(
                            symbol=logical,
                            quantity=qty,
                            avg_price=float(pos.get("level") or 0.0),
                            side=proto.SELL if direction == "SELL" else proto.BUY,
                        ),
                    ))
                closed_refs = set(self._known_position_dealrefs) - set(current_refs)
                for closed_ref in closed_refs:
                    prev = self._known_position_dealrefs[closed_ref]
                    await self._alert_position_closed(closed_ref, prev)
                self._known_position_dealrefs = current_refs
        except BrokerError as exc:
            logger.warning("IG poll positions failed: %s", exc)

        # Ticks - REST fallback only. When Lightstreamer is connected it
        # is the live price source; /markets is not fresh enough for bars.
        if self._stream_client is None:
            for logical in tuple(self._subscribed_symbols):
                try:
                    spec = self._spec(logical)
                    tick = await self._fetch_market_tick(spec)
                    tick_hash = (tick.bid, tick.ask, tick.timestamp.isoformat())
                    if tick_hash == self._last_tick_hashes.get(logical):
                        continue
                    self._last_tick_hashes[logical] = tick_hash
                    await self._events_q.put(BrokerEvent(
                        kind=BrokerEventKind.MARKET_DATA_TICK,
                        received_at=time.time(),
                        tick=tick,
                    ))
                except BrokerError as exc:
                    logger.warning("IG poll tick for %s failed: %s", logical, exc)

    async def _fetch_raw_positions(self) -> list[dict[str, Any]]:
        body = await self._request("GET", "/positions", version=2)
        return list(body.get("positions") or [])

    async def _alert_position_closed(
        self, deal_ref: str, prev_snapshot: dict[str, Any],
    ) -> None:
        """Push Telegram alert when a tracked dealReference disappears.

        Best-effort — failures don't break the poll loop. P&L recovery
        from IG requires querying activity history; deferred to v1.
        """
        try:
            from ..alerts import push_telegram_alert
            sym = prev_snapshot.get("symbol") or "?"
            level = float(prev_snapshot.get("level") or 0.0)
            push_telegram_alert(
                f"ℹ️ *Kate POSITION CLOSED (IG)* — {sym}\n"
                f"  entry_level={level:.5f}\n"
                f"  dealRef={deal_ref}\n"
                f"  (realized P&L: query /history/activity for full breakdown)",
            )
            logger.info(
                "IG position closed: dealRef=%s symbol=%s entry=%.5f",
                deal_ref, sym, level,
            )
        except Exception:
            logger.exception(
                "IG _alert_position_closed failed for dealRef=%s — continuing",
                deal_ref,
            )

    # ── Event stream ─────────────────────────────────────────────────────

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            event = await self._events_q.get()
            yield event

    # ── Helpers ──────────────────────────────────────────────────────────

    def _require_connected(self) -> None:
        if not self._connected:
            raise BrokerError("IG adapter not connected — call connect() first")

    def _spec(self, logical_symbol: str) -> IGSymbolSpec:
        spec = self.symbol_map.get(logical_symbol)
        if spec is None:
            raise BrokerError(
                f"IG adapter: no symbol_map entry for {logical_symbol!r}; "
                f"known={list(self.symbol_map)}"
            )
        return spec

    def _epic_to_logical(self, epic: str) -> Optional[str]:
        for spec in self.symbol_map.values():
            if spec.epic == epic:
                return spec.logical_symbol
        return None


def _mid_price(price_block: Any) -> float:
    """IG bars give bid/ask separately per O/H/L/C. Engine wants single
    price per bar (mid). Returns 0.0 if neither side is present."""
    if not isinstance(price_block, dict):
        return float(price_block or 0.0)
    bid = price_block.get("bid")
    ask = price_block.get("ask")
    if bid is not None and ask is not None:
        return (float(bid) + float(ask)) / 2.0
    return float(bid or ask or 0.0)


def _floor_utc_minute(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    else:
        value = value.astimezone(dt.timezone.utc)
    return value.replace(second=0, microsecond=0)


def _parse_ig_stream_timestamp(value: Any) -> dt.datetime:
    if value not in (None, ""):
        with contextlib.suppress(TypeError, ValueError):
            millis = int(float(value))
            if millis > 0:
                return dt.datetime.fromtimestamp(
                    millis / 1000.0,
                    tz=dt.timezone.utc,
                )
    return dt.datetime.now(dt.timezone.utc)


def _ig_mid_or_last(*, bid: Any, ask: Any, last: Any) -> float:
    bid_value = _parse_ig_float(bid)
    ask_value = _parse_ig_float(ask)
    if bid_value is not None and ask_value is not None:
        return (bid_value + ask_value) / 2.0
    if bid_value is not None:
        return bid_value
    if ask_value is not None:
        return ask_value
    last_value = _parse_ig_float(last)
    if last_value is not None:
        return last_value
    raise ValueError("IG chart bar update did not include bid/offer/last price")


def _parse_ig_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _parse_ig_int(value: Any, *, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _ig_cons_end_is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, 1.0):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _lightstreamer_endpoint_candidates(endpoint: str) -> list[str]:
    raw = (endpoint or "").strip()
    if not raw:
        return []
    candidates = []
    for value in (raw, raw.rstrip("/") + "/"):
        if value not in candidates:
            candidates.append(value)
    # IG returns https endpoints, but some Lightstreamer SDKs negotiate
    # more reliably against the http form in demo. Keep https first.
    if raw.startswith("https://demo-"):
        http_value = "http://" + raw[len("https://"):].rstrip("/") + "/"
        if http_value not in candidates:
            candidates.append(http_value)
    return candidates


__all__ = [
    "IGBrokerAdapter",
    "IGConfig",
    "IGSymbolSpec",
]
