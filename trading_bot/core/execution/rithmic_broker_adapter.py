"""RithmicBrokerAdapter for Kate's broker-neutral execution layer.

This wraps `async_rithmic.RithmicClient` behind the `BrokerAdapter` ABC.
The adapter is deliberately import-safe when async_rithmic or its runtime
dependencies are absent: only `connect()` with the default client factory
requires the real package. Unit tests can inject a fake client.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Optional

from . import dtc_protocol as proto
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

DEFAULT_RITHMIC_TEST_URL = "rituz00100.rithmic.com:443"


@dataclass(frozen=True)
class RithmicConfig:
    user: str
    password: str
    system_name: str = "Rithmic Test"
    url: str = DEFAULT_RITHMIC_TEST_URL
    app_name: str = "kate"
    app_version: str = "0.1"
    account_id: str = ""

    @classmethod
    def from_env(cls) -> "RithmicConfig":
        user = os.getenv("RITHMIC_USER") or os.getenv("EDGECLEAR_RITHMIC_USER") or ""
        password = (
            os.getenv("RITHMIC_PASSWORD")
            or os.getenv("EDGECLEAR_RITHMIC_PASSWORD")
            or ""
        )
        missing = []
        if not user:
            missing.append("RITHMIC_USER")
        if not password:
            missing.append("RITHMIC_PASSWORD")
        if missing:
            raise BrokerError(
                "missing required Rithmic credential env vars: "
                + ", ".join(missing)
            )
        return cls(
            user=user,
            password=password,
            system_name=os.getenv("RITHMIC_SYSTEM_NAME", "Rithmic Test"),
            url=os.getenv("RITHMIC_URL", DEFAULT_RITHMIC_TEST_URL),
            app_name=os.getenv("RITHMIC_APP_NAME", "kate"),
            app_version=os.getenv("RITHMIC_APP_VERSION", "0.1"),
            account_id=os.getenv("RITHMIC_ACCOUNT_ID", ""),
        )


@dataclass(frozen=True)
class _RithmicRuntime:
    RithmicClient: Any
    SysInfraType: Any
    DataType: Any
    OrderPlacement: Any
    OrderType: Any
    TransactionType: Any


def _load_runtime() -> _RithmicRuntime:
    try:
        from async_rithmic import (  # type: ignore
            DataType,
            OrderPlacement,
            OrderType,
            RithmicClient,
            SysInfraType,
            TransactionType,
        )
    except Exception as exc:  # pragma: no cover - depends on local install
        raise BrokerError(
            "async_rithmic is not importable. Install async-rithmic and its "
            f"runtime dependencies before using RithmicBrokerAdapter: {exc}"
        ) from exc
    return _RithmicRuntime(
        RithmicClient=RithmicClient,
        SysInfraType=SysInfraType,
        DataType=DataType,
        OrderPlacement=OrderPlacement,
        OrderType=OrderType,
        TransactionType=TransactionType,
    )


class RithmicBrokerAdapter(BrokerAdapter):
    """BrokerAdapter implementation for Edgeclear/Rithmic direct.

    `symbol_map` uses Kate logical symbols as keys. For Rithmic, each
    `BrokerSymbolSpec.broker_symbol` should be the product root (for example
    `MES`), and the adapter resolves the current front-month trading symbol at
    startup/subscription time.
    """

    def __init__(
        self,
        *,
        config: RithmicConfig,
        symbol_map: dict[str, BrokerSymbolSpec],
        client_factory: Optional[Callable[..., Any]] = None,
        runtime: Optional[_RithmicRuntime] = None,
        seed_timeout: float = 10.0,
    ) -> None:
        self.config = config
        self.symbol_map = dict(symbol_map)
        self._client_factory = client_factory
        self._runtime = runtime
        self._seed_timeout = seed_timeout

        self._client: Any = None
        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._closed = asyncio.Event()
        self._connected = False
        self._account_id = config.account_id
        self._resolved_contracts: dict[str, str] = {}
        self._broker_to_logical: dict[str, str] = {}

    async def connect(self) -> None:
        if self._connected:
            return
        runtime = self._ensure_runtime()
        if self._client is None:
            factory = self._client_factory or runtime.RithmicClient
            self._client = factory(
                user=self.config.user,
                password=self.config.password,
                system_name=self.config.system_name,
                app_name=self.config.app_name,
                app_version=self.config.app_version,
                url=self.config.url,
                manual_or_auto=runtime.OrderPlacement.AUTO,
            )
            self._wire_callbacks(self._client)
        try:
            await self._client.connect(
                plants=[
                    runtime.SysInfraType.ORDER_PLANT,
                    runtime.SysInfraType.PNL_PLANT,
                    runtime.SysInfraType.TICKER_PLANT,
                ]
            )
            self._account_id = self._select_account_id()
            for logical_symbol in self.symbol_map:
                await self._resolve_symbol(logical_symbol)
            self._closed.clear()
            self._connected = True
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.CONNECTED,
                received_at=time.time(),
            ))
        except Exception as exc:
            raise BrokerError(f"Rithmic connect failed: {exc}") from exc

    async def disconnect(self) -> None:
        self._closed.set()
        if self._client is not None and self._connected:
            try:
                await self._client.disconnect()
            except Exception as exc:
                logger.warning("Rithmic disconnect failed: %s", exc)
        self._connected = False
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.DISCONNECTED,
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
        spec = await self._resolve_symbol(symbol)
        qty = self._validate_quantity(quantity)
        kwargs: dict[str, Any] = {"account_id": self._account_id}
        rithmic_order_type = self._map_order_type(order_type)
        if order_type == proto.ORDER_TYPE_LIMIT:
            kwargs["price"] = price
        elif order_type == proto.ORDER_TYPE_STOP:
            kwargs["trigger_price"] = price

        if stop_price is not None:
            kwargs["stop_ticks"] = self._price_offset_ticks(
                entry_price=price,
                exit_price=stop_price,
                tick_size=spec.tick_size,
                field_name="stop_price",
            )
        if target_price is not None:
            kwargs["target_ticks"] = self._price_offset_ticks(
                entry_price=price,
                exit_price=target_price,
                tick_size=spec.tick_size,
                field_name="target_price",
            )

        try:
            await self._client.submit_order(
                order_id=client_order_id,
                symbol=self._resolved_contracts[symbol],
                exchange=exchange or spec.exchange,
                qty=qty,
                transaction_type=self._map_side(side),
                order_type=rithmic_order_type,
                **kwargs,
            )
        except Exception as exc:
            raise BrokerError(f"Rithmic submit_order failed: {exc}") from exc
        return client_order_id

    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        self._require_connected()
        kwargs: dict[str, Any] = {"account_id": self._account_id}
        if server_order_id:
            kwargs["basket_id"] = server_order_id
        else:
            kwargs["order_id"] = client_order_id
        try:
            await self._client.cancel_order(**kwargs)
        except Exception as exc:
            raise BrokerError(f"Rithmic cancel_order failed: {exc}") from exc

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        self._require_connected()
        runtime = self._ensure_runtime()
        spec = await self._resolve_symbol(symbol)
        data_type = int(runtime.DataType.LAST_TRADE) | int(runtime.DataType.BBO)
        try:
            await self._client.subscribe_to_market_data(
                self._resolved_contracts[symbol],
                exchange or spec.exchange,
                data_type,
            )
        except Exception as exc:
            raise BrokerError(f"Rithmic subscribe_market_data failed: {exc}") from exc

    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        self._require_connected()
        account_id = trade_account or self._account_id
        try:
            rows = await asyncio.wait_for(
                self._client.list_account_summary(account_id=account_id),
                timeout=self._seed_timeout,
            )
        except Exception as exc:
            raise BrokerError(f"Rithmic account summary failed: {exc}") from exc
        if not rows:
            raise BrokerError(f"Rithmic returned no account summary for {account_id!r}")
        balance = self._account_summary_to_balance(rows[0])
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
        account_id = trade_account or self._account_id
        try:
            rows = await asyncio.wait_for(
                self._client.list_positions(account_id=account_id),
                timeout=self._seed_timeout,
            )
        except Exception as exc:
            raise BrokerError(f"Rithmic positions request failed: {exc}") from exc
        positions = tuple(
            p for p in (self._position_to_event(row) for row in rows)
            if p is not None
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
        account_id = trade_account or self._account_id
        try:
            rows = await asyncio.wait_for(
                self._client.list_orders(account_id=account_id),
                timeout=self._seed_timeout,
            )
        except Exception as exc:
            raise BrokerError(f"Rithmic open-orders request failed: {exc}") from exc
        orders = tuple(o for o in (self._order_to_event(row) for row in rows) if o is not None)
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

    async def _resolve_symbol(self, logical_symbol: str) -> BrokerSymbolSpec:
        spec = self.symbol_map.get(logical_symbol)
        if spec is None:
            raise BrokerError(f"no symbol_map entry for {logical_symbol!r}")
        if logical_symbol not in self._resolved_contracts:
            contract = await self._client.get_front_month_contract(
                spec.broker_symbol,
                spec.exchange,
            )
            if not contract:
                raise BrokerError(
                    f"Rithmic could not resolve front month for "
                    f"{spec.broker_symbol}/{spec.exchange}"
                )
            self._resolved_contracts[logical_symbol] = str(contract)
            self._broker_to_logical[str(contract)] = logical_symbol
            logger.info(
                "Rithmic symbol resolved: %s -> %s/%s",
                logical_symbol, contract, spec.exchange,
            )
        return spec

    def _wire_callbacks(self, client: Any) -> None:
        self._add_callback(client, "on_tick", self._on_tick)
        self._add_callback(client, "on_account_pnl_update", self._on_account_pnl_update)
        self._add_callback(client, "on_instrument_pnl_update", self._on_instrument_pnl_update)
        self._add_callback(client, "on_rithmic_order_notification", self._on_order_notification)
        self._add_callback(client, "on_exchange_order_notification", self._on_order_notification)
        self._add_callback(client, "on_bracket_update", self._on_order_notification)
        self._add_callback(client, "on_disconnected", self._on_disconnected)

    def _add_callback(self, client: Any, attr: str, callback: Callable[..., Any]) -> None:
        event = getattr(client, attr, None)
        if event is None:
            return
        try:
            event += callback
        except TypeError:
            if hasattr(event, "append"):
                event.append(callback)

    async def _on_tick(self, data: Any) -> None:
        tick = self._tick_to_event(data)
        if tick is None:
            return
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_TICK,
            received_at=time.time(),
            tick=tick,
        ))

    async def _on_account_pnl_update(self, data: Any) -> None:
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ACCOUNT_BALANCE_UPDATE,
            received_at=time.time(),
            balance=self._account_summary_to_balance(data),
        ))

    async def _on_instrument_pnl_update(self, data: Any) -> None:
        position = self._position_to_event(data)
        if position is None:
            return
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.POSITION_UPDATE,
            received_at=time.time(),
            position=position,
        ))

    async def _on_order_notification(self, data: Any) -> None:
        order = self._order_to_event(data)
        if order is None:
            return
        await self._events_q.put(BrokerEvent(
            kind=self._order_kind(data),
            received_at=time.time(),
            order=order,
        ))

    async def _on_disconnected(self, *_args: Any) -> None:
        self._connected = False
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.DISCONNECTED,
            received_at=time.time(),
        ))

    def _tick_to_event(self, data: Any) -> Optional[MarketDataTick]:
        d = _plain(data)
        price = _first_float(d, "trade_price", "last_price", "price")
        bid = _first_float(d, "bid_price", "bid")
        ask = _first_float(d, "ask_price", "ask")
        if price is None:
            # BBO-only update: surface midpoint if possible, otherwise ignore.
            if bid is None or ask is None:
                return None
            price = (bid + ask) / 2.0
        broker_symbol = str(d.get("symbol") or d.get("trading_symbol") or "")
        logical = self._broker_to_logical.get(broker_symbol, broker_symbol)
        timestamp = _coerce_datetime(d.get("datetime")) or dt.datetime.utcnow()
        return MarketDataTick(
            symbol=logical,
            timestamp=timestamp,
            last_price=price,
            last_size=_first_float(d, "trade_size", "last_size", "size") or 0.0,
            bid=bid,
            ask=ask,
        )

    def _account_summary_to_balance(self, data: Any) -> AccountBalanceEvent:
        d = _plain(data)
        cash = _first_float(d, "cash_balance", "cash", "cash_value") or 0.0
        nlv = _first_float(
            d,
            "net_liquidation_value",
            "nlv",
            "account_balance",
            "liquidating_value",
            "total_account_value",
        )
        pnl = _first_float(
            d,
            "pnl",
            "open_position_pnl",
            "open_positions_profit_loss",
            "total_pnl",
            "realized_pnl",
        ) or 0.0
        if nlv is None:
            nlv = cash + pnl
        return AccountBalanceEvent(
            cash=cash,
            nlv=nlv,
            pnl=pnl,
            margin_requirement=_first_float(
                d, "margin_requirement", "initial_margin", "maintenance_margin"
            ) or 0.0,
            currency=str(d.get("currency") or d.get("account_currency") or "USD"),
        )

    def _position_to_event(self, data: Any) -> Optional[PositionEvent]:
        d = _plain(data)
        quantity = _first_float(d, "quantity", "position", "net_quantity", "open_quantity")
        if quantity is None:
            return None
        broker_symbol = str(d.get("symbol") or d.get("trading_symbol") or "")
        logical = self._broker_to_logical.get(broker_symbol, broker_symbol)
        return PositionEvent(
            symbol=logical,
            quantity=quantity,
            avg_price=_first_float(d, "avg_price", "average_price", "avg_fill_price") or 0.0,
            side=proto.BUY if quantity > 0 else proto.SELL if quantity < 0 else None,
        )

    def _order_to_event(self, data: Any) -> Optional[OrderEvent]:
        d = _plain(data)
        client_order_id = str(
            d.get("user_tag")
            or d.get("order_id")
            or d.get("client_order_id")
            or ""
        )
        if not client_order_id:
            return None
        broker_symbol = str(d.get("symbol") or d.get("trading_symbol") or "")
        logical = self._broker_to_logical.get(broker_symbol, broker_symbol)
        return OrderEvent(
            client_order_id=client_order_id,
            symbol=logical,
            side=self._rithmic_side_to_proto(d.get("transaction_type")),
            quantity=_first_float(d, "quantity", "qty", "order_quantity") or 0.0,
            fill_price=_first_float(d, "fill_price", "avg_fill_price", "price"),
            fill_quantity=_first_float(d, "fill_quantity", "filled_quantity"),
            rejected_reason=str(d.get("reject_reason") or d.get("reason") or "") or None,
            server_order_id=str(d.get("basket_id") or d.get("server_order_id") or "") or None,
        )

    def _order_kind(self, data: Any) -> BrokerEventKind:
        d = _plain(data)
        text = " ".join(
            str(d.get(k, ""))
            for k in ("status", "notify_type", "order_status", "completion_reason")
        ).lower()
        if "reject" in text:
            return BrokerEventKind.ORDER_REJECTED
        if "cancel" in text:
            return BrokerEventKind.ORDER_CANCELED
        if "partial" in text:
            return BrokerEventKind.ORDER_PARTIAL_FILL
        if "fill" in text or "complete" in text:
            return BrokerEventKind.ORDER_FILLED
        return BrokerEventKind.ORDER_ACK

    def _select_account_id(self) -> str:
        if self._account_id:
            return self._account_id
        accounts = getattr(self._client, "accounts", None) or []
        if not accounts:
            raise BrokerError("Rithmic login returned no accounts")
        return str(getattr(accounts[0], "account_id", None) or _plain(accounts[0]).get("account_id"))

    def _map_side(self, side: int) -> Any:
        enum = self._ensure_runtime().TransactionType
        if side == proto.BUY:
            return enum.BUY
        if side == proto.SELL:
            return enum.SELL
        raise BrokerError(f"unsupported side {side!r}")

    def _map_order_type(self, order_type: int) -> Any:
        enum = self._ensure_runtime().OrderType
        if order_type == proto.ORDER_TYPE_MARKET:
            return enum.MARKET
        if order_type == proto.ORDER_TYPE_LIMIT:
            return enum.LIMIT
        if order_type == proto.ORDER_TYPE_STOP:
            return enum.STOP_MARKET
        raise BrokerError(f"unsupported order_type {order_type!r}")

    def _rithmic_side_to_proto(self, side: Any) -> int:
        text = str(side).upper()
        if text.endswith("BUY") or text == "1":
            return proto.BUY
        if text.endswith("SELL") or text == "2":
            return proto.SELL
        return 0

    def _price_offset_ticks(
        self,
        *,
        entry_price: float,
        exit_price: float,
        tick_size: float,
        field_name: str,
    ) -> int:
        if entry_price <= 0:
            raise BrokerError(f"{field_name} requires a positive entry price")
        ticks = round(abs(entry_price - exit_price) / tick_size)
        if ticks < 1:
            raise BrokerError(f"{field_name} is less than one tick from entry")
        return ticks

    def _validate_quantity(self, quantity: float) -> int:
        qty = int(quantity)
        if qty <= 0 or qty != quantity:
            raise BrokerError("Rithmic futures orders require positive integer quantity")
        return qty

    def _ensure_runtime(self) -> _RithmicRuntime:
        if self._runtime is None:
            self._runtime = _load_runtime()
        return self._runtime

    def _require_connected(self) -> None:
        if not self._connected or self._client is None:
            raise BrokerError("RithmicBrokerAdapter is not connected")


def _plain(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        from google.protobuf.json_format import MessageToDict  # type: ignore
        if hasattr(value, "DESCRIPTOR"):
            return MessageToDict(value, preserving_proto_field_name=True)
    except Exception:
        pass
    if hasattr(value, "__dict__"):
        return {
            k: v for k, v in vars(value).items()
            if not k.startswith("_") and not inspect.ismethod(v)
        }
    return {}


def _first_float(d: dict[str, Any], *names: str) -> Optional[float]:
    for name in names:
        if name in d and d[name] not in (None, ""):
            try:
                return float(d[name])
            except (TypeError, ValueError):
                continue
    return None


def _coerce_datetime(value: Any) -> Optional[dt.datetime]:
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, str):
        try:
            return dt.datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


__all__ = ["RithmicBrokerAdapter", "RithmicConfig", "DEFAULT_RITHMIC_TEST_URL"]
