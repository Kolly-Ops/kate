"""MT5BrokerAdapter for Kate's broker-neutral execution layer.

This wraps the MetaTrader5 Python package behind the BrokerAdapter ABC.
The adapter is import-safe: the real package is loaded lazily unless a fake
runtime is injected by tests.

Runtime prerequisite: the MT5 desktop terminal must be installed and running,
and "Algo Trading" must be toggled on/green before connecting. For the IC
Markets demo lane, the expected terminal path is usually:

    C:\\Program Files\\MetaTrader 5 IC Markets Global\\terminal64.exe
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

from . import dtc_protocol as proto
from ..alerts import push_telegram_alert
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

logger = logging.getLogger(__name__)

# Default install path for IC Markets MT5 on Kate Host. Hard-coded per
# Gemini's resilience directive 2026-05-15 — too easy to forget setting
# MT5_PATH env var, leaving initialize() to guess. Override via the
# `path` field on MT5Config or the MT5_PATH env var if Kate Host's
# install ever moves.
_DEFAULT_MT5_PATH = r"C:\Program Files\MetaTrader 5"


@dataclass(frozen=True)
class MT5Config:
    login: int = 0
    password: str = ""
    server: str = ""
    path: str = _DEFAULT_MT5_PATH
    timeout_ms: int = 60000
    portable: bool = False
    magic: int = 1001
    deviation: int = 10
    comment: str = "kate"
    poll_interval_seconds: float = 0.25
    # Per Gemini's resilience directive 2026-05-15: if MT5
    # terminal_info().connected has been False for longer than this
    # threshold, push a P0 Telegram alert so silent disconnects can't
    # quietly consume a London-session window again. 300s ≈ 5 minutes.
    heartbeat_disconnect_alert_seconds: float = 300.0

    @classmethod
    def from_env(cls) -> "MT5Config":
        login_raw = os.getenv("MT5_LOGIN", "")
        try:
            login = int(login_raw) if login_raw else 0
        except ValueError as exc:
            raise BrokerError("MT5_LOGIN must be an integer") from exc
        return cls(
            login=login,
            password=os.getenv("MT5_PASSWORD", ""),
            server=os.getenv("MT5_SERVER", ""),
            path=os.getenv("MT5_PATH", _DEFAULT_MT5_PATH),
            timeout_ms=int(os.getenv("MT5_TIMEOUT_MS", "60000")),
            portable=os.getenv("MT5_PORTABLE", "").lower() in {"1", "true", "yes"},
            magic=int(os.getenv("MT5_MAGIC", "260513")),
            deviation=int(os.getenv("MT5_DEVIATION", "10")),
            comment=os.getenv("MT5_COMMENT", "kate"),
            poll_interval_seconds=float(os.getenv("MT5_POLL_INTERVAL_SECONDS", "0.25")),
            heartbeat_disconnect_alert_seconds=float(
                os.getenv("MT5_HEARTBEAT_DISCONNECT_ALERT_SECONDS", "300.0")
            ),
        )


def _load_runtime() -> Any:
    try:
        import MetaTrader5 as mt5  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on local install
        raise BrokerError(
            "MetaTrader5 is not importable. Install the MetaTrader5 package "
            f"and ensure the MT5 terminal is available before using MT5BrokerAdapter: {exc}"
        ) from exc
    return mt5


class MT5BrokerAdapter(BrokerAdapter):
    """BrokerAdapter implementation for MetaTrader 5.

    `symbol_map` maps Kate logical symbols to MT5 symbols. For example a
    future Front 4 FX lane might map ``GBPUSD`` -> ``GBPUSD`` or a broker
    suffixed form such as ``GBPUSD.a``.
    """

    def __init__(
        self,
        *,
        config: MT5Config,
        symbol_map: dict[str, BrokerSymbolSpec],
        runtime: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.symbol_map = dict(symbol_map)
        self._runtime = runtime
        self._connected = False
        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._poll_task: Optional[asyncio.Task[None]] = None
        self._subscribed_symbols: set[str] = set()
        self._last_account_hash: Optional[tuple[Any, ...]] = None
        self._last_positions_hash: Optional[tuple[Any, ...]] = None
        self._last_orders_hash: Optional[tuple[Any, ...]] = None
        self._last_tick_hashes: dict[str, tuple[Any, ...]] = {}
        self._broker_to_logical: dict[str, str] = {
            spec.broker_symbol: spec.logical_symbol
            for spec in self.symbol_map.values()
        }
        # Heartbeat state — last time we observed terminal_info().connected
        # as True, and whether we've already paged the operator about the
        # current disconnect cycle (avoids alert spam every poll tick).
        self._last_connected_at: float = 0.0
        self._heartbeat_alerted: bool = False

    async def connect(self) -> None:
        if self._connected:
            return
        mt5 = self._ensure_runtime()
        kwargs: dict[str, Any] = {
            "timeout": self.config.timeout_ms,
            "portable": self.config.portable,
        }
        if self.config.path:
            kwargs["path"] = self.config.path
        if self.config.login:
            kwargs["login"] = self.config.login
        if self.config.password:
            kwargs["password"] = self.config.password
        if self.config.server:
            kwargs["server"] = self.config.server

        ok = await asyncio.to_thread(mt5.initialize, **kwargs)
        if not ok:
            raise BrokerError(f"MT5 initialize failed: {self._last_error()}")

        for spec in self.symbol_map.values():
            selected = await asyncio.to_thread(mt5.symbol_select, spec.broker_symbol, True)
            if not selected:
                await asyncio.to_thread(mt5.shutdown)
                raise BrokerError(
                    f"MT5 symbol_select failed for {spec.broker_symbol!r}: {self._last_error()}"
                )

        self._connected = True
        # Seed heartbeat clock — successful initialize counts as a confirmed
        # connection moment. Without this seed, disconnect detection would
        # always fire (now - 0.0 > threshold) on the very first poll tick.
        self._last_connected_at = time.time()
        self._heartbeat_alerted = False
        await self._prime_poll_hashes()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="mt5-adapter-poll")
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.CONNECTED,
            received_at=time.time(),
        ))

    async def disconnect(self) -> None:
        mt5 = self._ensure_runtime()
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        if self._connected:
            await asyncio.to_thread(mt5.shutdown)
        self._connected = False
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
        """Optional explicit MT5 login after initialize().

        MT5 can authenticate during initialize(); this override exists so the
        supervisor can re-login without tearing down the terminal session.
        """
        mt5 = self._ensure_runtime()
        login = int(username or trade_account or self.config.login or 0)
        if not login:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.LOGON_OK,
                received_at=time.time(),
            ))
            return
        ok = await asyncio.to_thread(
            mt5.login,
            login,
            password=password or self.config.password,
            server=self.config.server or None,
            timeout=self.config.timeout_ms,
        )
        if not ok:
            raise BrokerError(f"MT5 login failed: {self._last_error()}")
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.LOGON_OK,
            received_at=time.time(),
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
        target_price: Optional[float] = None,
        free_form_text: str = "",
    ) -> str:
        self._require_connected()
        mt5 = self._ensure_runtime()
        spec = self._spec(symbol)
        request = self._order_request(
            spec=spec,
            side=side,
            quantity=quantity,
            order_type=order_type,
            price=price,
            stop_price=stop_price,
            target_price=target_price,
            comment=free_form_text or client_order_id,
        )
        result = await asyncio.to_thread(mt5.order_send, request)
        retcode = int(_field(result, "retcode", 0) or 0)
        if retcode not in self._success_retcodes(mt5):
            reason = str(_field(result, "comment", "") or self._last_error())
            order = OrderEvent(
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                rejected_reason=reason,
                server_order_id=str(_field(result, "order", "") or "") or None,
            )
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.ORDER_REJECTED,
                received_at=time.time(),
                order=order,
            ))
            raise BrokerError(f"MT5 order_send rejected: retcode={retcode} {reason}")

        server_order_id = str(_field(result, "order", "") or _field(result, "deal", "") or "")
        order_event = OrderEvent(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            fill_price=float(_field(result, "price", 0.0) or 0.0) or None,
            fill_quantity=float(_field(result, "volume", quantity) or quantity),
            server_order_id=server_order_id or None,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ORDER_ACK,
            received_at=time.time(),
            order=order_event,
        ))
        return client_order_id

    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        self._require_connected()
        if not server_order_id:
            raise BrokerError("MT5 cancel_order requires server_order_id")
        mt5 = self._ensure_runtime()
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": int(server_order_id),
            "magic": self.config.magic,
            "comment": client_order_id,
        }
        result = await asyncio.to_thread(mt5.order_send, request)
        retcode = int(_field(result, "retcode", 0) or 0)
        if retcode not in self._success_retcodes(mt5):
            raise BrokerError(f"MT5 cancel_order failed: retcode={retcode} {self._last_error()}")
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

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        """Seed one current tick into the event stream.

        The MT5 Python API is polling-based. Continuous polling belongs in the
        supervisor/front runner; the adapter exposes normalized tick payloads.
        """
        self._require_connected()
        mt5 = self._ensure_runtime()
        spec = self._spec(symbol)
        selected = await asyncio.to_thread(mt5.symbol_select, spec.broker_symbol, True)
        if not selected:
            raise BrokerError(f"MT5 symbol_select failed for {spec.broker_symbol!r}: {self._last_error()}")
        self._subscribed_symbols.add(spec.logical_symbol)
        tick = await asyncio.to_thread(mt5.symbol_info_tick, spec.broker_symbol)
        if tick is None:
            raise BrokerError(f"MT5 symbol_info_tick returned no data for {spec.broker_symbol}")
        event_tick = self._tick_to_event(spec.logical_symbol, tick)
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_TICK,
            received_at=time.time(),
            tick=event_tick,
        ))

    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        self._require_connected()
        info = await asyncio.to_thread(self._ensure_runtime().account_info)
        if info is None:
            raise BrokerError(f"MT5 account_info returned no data: {self._last_error()}")
        balance = AccountBalanceEvent(
            cash=float(_field(info, "balance", 0.0) or 0.0),
            nlv=float(_field(info, "equity", 0.0) or 0.0),
            pnl=float(_field(info, "profit", 0.0) or 0.0),
            margin_requirement=float(_field(info, "margin", 0.0) or 0.0),
            currency=str(_field(info, "currency", "USD") or "USD"),
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
            received_at=time.time(),
            balance=balance,
        ))
        return balance

    async def request_positions(
        self,
        *,
        trade_account: str,
    ) -> tuple[PositionEvent, ...]:
        self._require_connected()
        rows = await asyncio.to_thread(self._ensure_runtime().positions_get)
        positions = tuple(
            self._position_to_event(row)
            for row in (rows or ())
            if row is not None
        )
        for position in positions:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.POSITION_UPDATE,
                received_at=time.time(),
                position=position,
            ))
        return positions

    async def request_open_orders(
        self,
        *,
        trade_account: str,
    ) -> tuple[OrderEvent, ...]:
        self._require_connected()
        rows = await asyncio.to_thread(self._ensure_runtime().orders_get)
        orders = tuple(
            self._open_order_to_event(row)
            for row in (rows or ())
            if row is not None
        )
        for order in orders:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.ORDER_ACK,
                received_at=time.time(),
                order=order,
            ))
        return orders

    async def events(self) -> AsyncIterator[BrokerEvent]:
        while True:
            event = await self._events_q.get()
            yield event
            if event.kind == BrokerEventKind.DISCONNECTED:
                return

    def _order_request(
        self,
        *,
        spec: BrokerSymbolSpec,
        side: int,
        quantity: float,
        order_type: int,
        price: float,
        stop_price: Optional[float],
        target_price: Optional[float],
        comment: str,
    ) -> dict[str, Any]:
        mt5 = self._ensure_runtime()
        symbol = spec.broker_symbol
        mapped_type = self._map_order_type(side=side, order_type=order_type)
        request: dict[str, Any] = {
            "symbol": symbol,
            "volume": float(quantity),
            "type": mapped_type,
            "magic": self.config.magic,
            "comment": comment[:31],
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": getattr(mt5, "ORDER_FILLING_IOC", getattr(mt5, "ORDER_FILLING_RETURN", 0)),
            "deviation": self.config.deviation,
        }
        if stop_price is not None:
            request["sl"] = float(stop_price)
        if target_price is not None:
            request["tp"] = float(target_price)
        if order_type == proto.ORDER_TYPE_MARKET:
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                raise BrokerError(f"MT5 has no tick for market order symbol {symbol!r}")
            request["action"] = mt5.TRADE_ACTION_DEAL
            request["price"] = float(_field(tick, "ask", 0.0) if side == proto.BUY else _field(tick, "bid", 0.0))
        else:
            if price <= 0:
                raise BrokerError("MT5 pending orders require positive price")
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["price"] = float(price)
        return request

    async def _prime_poll_hashes(self) -> None:
        mt5 = self._ensure_runtime()
        account = await asyncio.to_thread(mt5.account_info)
        positions = await asyncio.to_thread(mt5.positions_get)
        orders = await asyncio.to_thread(mt5.orders_get)
        self._last_account_hash = self._account_hash(account)
        self._last_positions_hash = self._positions_hash(positions or ())
        self._last_orders_hash = self._orders_hash(orders or ())

    async def _poll_loop(self) -> None:
        while self._connected:
            await asyncio.sleep(self.config.poll_interval_seconds)
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("MT5BrokerAdapter poll failed; continuing")

    async def _poll_once(self) -> None:
        mt5 = self._ensure_runtime()
        account = await asyncio.to_thread(mt5.account_info)
        account_hash = self._account_hash(account)
        if account is not None and account_hash != self._last_account_hash:
            self._last_account_hash = account_hash
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
                received_at=time.time(),
                balance=AccountBalanceEvent(
                    cash=float(_field(account, "balance", 0.0) or 0.0),
                    nlv=float(_field(account, "equity", 0.0) or 0.0),
                    pnl=float(_field(account, "profit", 0.0) or 0.0),
                    margin_requirement=float(_field(account, "margin", 0.0) or 0.0),
                    currency=str(_field(account, "currency", "USD") or "USD"),
                ),
            ))

        positions = await asyncio.to_thread(mt5.positions_get)
        positions_hash = self._positions_hash(positions or ())
        if positions_hash != self._last_positions_hash:
            self._last_positions_hash = positions_hash
            for row in positions or ():
                await self._events_q.put(BrokerEvent(
                    kind=BrokerEventKind.POSITION_UPDATE,
                    received_at=time.time(),
                    position=self._position_to_event(row),
                ))

        orders = await asyncio.to_thread(mt5.orders_get)
        orders_hash = self._orders_hash(orders or ())
        if orders_hash != self._last_orders_hash:
            self._last_orders_hash = orders_hash
            for row in orders or ():
                await self._events_q.put(BrokerEvent(
                    kind=BrokerEventKind.ORDER_ACK,
                    received_at=time.time(),
                    order=self._open_order_to_event(row),
                ))

        for logical_symbol in tuple(self._subscribed_symbols):
            spec = self._spec(logical_symbol)
            tick = await asyncio.to_thread(mt5.symbol_info_tick, spec.broker_symbol)
            if tick is None:
                continue
            tick_hash = self._tick_hash(tick)
            if tick_hash == self._last_tick_hashes.get(logical_symbol):
                continue
            self._last_tick_hashes[logical_symbol] = tick_hash
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.MARKET_DATA_TICK,
                received_at=time.time(),
                tick=self._tick_to_event(logical_symbol, tick),
            ))

        await self._check_heartbeat_and_alert()

    async def _check_heartbeat_and_alert(self) -> None:
        """Track terminal connection state, page operator on prolonged outage.

        Per Gemini's resilience directive 2026-05-15. Today's incident
        (broker disconnect 07:11 UK, manual RDP reset hours later) cost a
        full London-session window because no one was watching. This check
        runs every poll tick, but only alerts once per disconnect cycle and
        once on recovery — no alert spam.
        """
        mt5 = self._ensure_runtime()
        try:
            info = await asyncio.to_thread(mt5.terminal_info)
        except Exception:
            info = None

        is_connected = info is not None and bool(_field(info, "connected", False))
        now = time.time()

        if is_connected:
            self._last_connected_at = now
            if self._heartbeat_alerted:
                push_telegram_alert(
                    "✅ *Kate Resilience — Front 4 (MT5) RECONNECTED*\n\n"
                    f"Terminal back online at "
                    f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}.\n"
                    "Resuming normal operation."
                )
                self._heartbeat_alerted = False
            return

        seconds_disconnected = now - self._last_connected_at
        if (
            seconds_disconnected > self.config.heartbeat_disconnect_alert_seconds
            and not self._heartbeat_alerted
        ):
            last_seen = dt.datetime.fromtimestamp(
                self._last_connected_at, tz=dt.timezone.utc
            ).isoformat(timespec="seconds")
            push_telegram_alert(
                "🚨 *Kate Resilience Alert — Front 4 (MT5) DISCONNECTED*\n\n"
                f"Terminal disconnected for >"
                f"{int(self.config.heartbeat_disconnect_alert_seconds)}s "
                f"({int(seconds_disconnected)}s and counting).\n"
                f"Last seen connected: `{last_seen}` UTC\n\n"
                "*Action required:*\n"
                "1. RDP to Kate Host\n"
                "2. Verify MT5 terminal is alive\n"
                "3. Re-login to IC Markets demo if needed\n\n"
                "_Front 4 will not fire trades until reconnected._"
            )
            self._heartbeat_alerted = True

    def _account_hash(self, account: Any) -> Optional[tuple[Any, ...]]:
        if account is None:
            return None
        return (
            _field(account, "balance", None),
            _field(account, "equity", None),
            _field(account, "profit", None),
            _field(account, "margin", None),
            _field(account, "currency", None),
        )

    def _positions_hash(self, rows: Any) -> tuple[Any, ...]:
        return tuple(
            (
                _field(row, "ticket", None),
                _field(row, "symbol", None),
                _field(row, "volume", None),
                _field(row, "type", None),
                _field(row, "price_open", None),
                _field(row, "profit", None),
            )
            for row in rows
        )

    def _orders_hash(self, rows: Any) -> tuple[Any, ...]:
        return tuple(
            (
                _field(row, "ticket", None),
                _field(row, "symbol", None),
                _field(row, "type", None),
                _field(row, "volume_current", None),
                _field(row, "price_open", None),
                _field(row, "comment", None),
            )
            for row in rows
        )

    def _tick_hash(self, tick: Any) -> tuple[Any, ...]:
        return (
            _field(tick, "time_msc", None),
            _field(tick, "time", None),
            _field(tick, "bid", None),
            _field(tick, "ask", None),
            _field(tick, "last", None),
            _field(tick, "volume", None),
            _field(tick, "volume_real", None),
        )

    def _map_order_type(self, *, side: int, order_type: int) -> int:
        mt5 = self._ensure_runtime()
        if order_type == proto.ORDER_TYPE_MARKET:
            if side == proto.BUY:
                return mt5.ORDER_TYPE_BUY
            if side == proto.SELL:
                return mt5.ORDER_TYPE_SELL
        if order_type == proto.ORDER_TYPE_LIMIT:
            if side == proto.BUY:
                return mt5.ORDER_TYPE_BUY_LIMIT
            if side == proto.SELL:
                return mt5.ORDER_TYPE_SELL_LIMIT
        if order_type == proto.ORDER_TYPE_STOP:
            if side == proto.BUY:
                return mt5.ORDER_TYPE_BUY_STOP
            if side == proto.SELL:
                return mt5.ORDER_TYPE_SELL_STOP
        raise BrokerError(f"unsupported MT5 side/order_type pair: side={side!r} order_type={order_type!r}")

    def _position_to_event(self, row: Any) -> PositionEvent:
        symbol = str(_field(row, "symbol", ""))
        quantity = float(_field(row, "volume", 0.0) or 0.0)
        position_type = int(_field(row, "type", 0) or 0)
        mt5 = self._ensure_runtime()
        side = proto.SELL if position_type == getattr(mt5, "POSITION_TYPE_SELL", 1) else proto.BUY
        signed_qty = -quantity if side == proto.SELL else quantity
        return PositionEvent(
            symbol=self._broker_to_logical.get(symbol, symbol),
            quantity=signed_qty,
            avg_price=float(_field(row, "price_open", 0.0) or 0.0),
            side=side,
        )

    def _open_order_to_event(self, row: Any) -> OrderEvent:
        symbol = str(_field(row, "symbol", ""))
        order_type = int(_field(row, "type", 0) or 0)
        side = self._mt5_order_type_to_side(order_type)
        return OrderEvent(
            client_order_id=str(_field(row, "comment", "") or _field(row, "ticket", "")),
            symbol=self._broker_to_logical.get(symbol, symbol),
            side=side,
            quantity=float(_field(row, "volume_current", None) or _field(row, "volume_initial", 0.0) or 0.0),
            fill_price=float(_field(row, "price_open", 0.0) or 0.0),
            server_order_id=str(_field(row, "ticket", "") or "") or None,
        )

    def _mt5_order_type_to_side(self, order_type: int) -> int:
        mt5 = self._ensure_runtime()
        buy_types = {
            mt5.ORDER_TYPE_BUY,
            mt5.ORDER_TYPE_BUY_LIMIT,
            mt5.ORDER_TYPE_BUY_STOP,
            getattr(mt5, "ORDER_TYPE_BUY_STOP_LIMIT", -1),
        }
        return proto.BUY if order_type in buy_types else proto.SELL

    def _tick_to_event(self, logical_symbol: str, tick: Any) -> MarketDataTick:
        timestamp = _coerce_time(_field(tick, "time_msc", None), millis=True)
        if timestamp is None:
            timestamp = _coerce_time(_field(tick, "time", None), millis=False)
        return MarketDataTick(
            symbol=logical_symbol,
            timestamp=timestamp or dt.datetime.utcnow(),
            last_price=float(_field(tick, "last", None) or _field(tick, "bid", 0.0) or 0.0),
            last_size=float(_field(tick, "volume_real", None) or _field(tick, "volume", 0.0) or 0.0),
            bid=_maybe_float(_field(tick, "bid", None)),
            ask=_maybe_float(_field(tick, "ask", None)),
        )

    def _success_retcodes(self, mt5: Any) -> set[int]:
        return {
            int(getattr(mt5, "TRADE_RETCODE_DONE", 10009)),
            int(getattr(mt5, "TRADE_RETCODE_PLACED", 10008)),
        }

    def _spec(self, logical_symbol: str) -> BrokerSymbolSpec:
        spec = self.symbol_map.get(logical_symbol)
        if spec is None:
            raise BrokerError(f"no symbol_map entry for {logical_symbol!r}")
        return spec

    def _ensure_runtime(self) -> Any:
        if self._runtime is None:
            self._runtime = _load_runtime()
        return self._runtime

    def _require_connected(self) -> None:
        if not self._connected:
            raise BrokerError("MT5BrokerAdapter is not connected")

    def _last_error(self) -> str:
        mt5 = self._ensure_runtime()
        try:
            return str(mt5.last_error())
        except Exception:
            return "unknown MT5 error"


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _maybe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_time(value: Any, *, millis: bool) -> Optional[dt.datetime]:
    if value in (None, ""):
        return None
    try:
        seconds = float(value) / 1000.0 if millis else float(value)
    except (TypeError, ValueError):
        return None
    return dt.datetime.utcfromtimestamp(seconds)


__all__ = ["MT5BrokerAdapter", "MT5Config"]
