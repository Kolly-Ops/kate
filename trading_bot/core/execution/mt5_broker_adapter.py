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
from ..data import Candle
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

# Server timezone offset detection guards (task #37 — Codex 2026-05-25
# PRECONDITION). Real retail brokers operate in the ±6h timezone band
# (Cyprus EEST +3, US EDT -4, NZ NZST +12 is the outer edge but rare
# for FX retail). Detections outside this band are silent broker
# misconfigurations or stale data — loud-fail rather than apply.
ACCEPTABLE_OFFSET_HOURS: frozenset[int] = frozenset(range(-6, 7))

# Maximum tolerated gap between sampled tick.time and the rounded
# hourly offset value. A fresh live tick has staleness < 5s typically;
# the Friday 2026-05-22 23:42 incident sampled a tick with staleness
# ~14.6 minutes (877s) from a post-market-close stale source.
# 60s gives plenty of headroom for network jitter while catching
# staleness measured in minutes.
MAX_TICK_STALENESS_SECONDS: float = 60.0


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
    market_data_stale_seconds: float = 15.0
    market_data_metrics_seconds: float = 30.0
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
            market_data_stale_seconds=float(os.getenv("MT5_MARKET_DATA_STALE_SECONDS", "15.0")),
            market_data_metrics_seconds=float(os.getenv("MT5_MARKET_DATA_METRICS_SECONDS", "30.0")),
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
        # MT5 server timezone offset in seconds. Many brokers (e.g.
        # ICMarketsSC-Demo runs GMT+3) serve epoch values that represent
        # server-local wall clock TREATED as if it were UTC. Without
        # correction the engine sees timestamps 3h ahead of real UTC,
        # which mis-aligned the FX London Breakout trade window for 10
        # silent days. Detected on first successful connect() by sampling
        # a tick and comparing to real wall clock — see
        # `_detect_server_offset`.
        self._mt5_server_offset_seconds: float = 0.0
        # TEMP diagnostic counters — bridge measure pending Codex's full
        # observability hardening per his 2026-05-20 HARD-OBJECTION
        # (5-item patch: stale-tick events, ERROR emission, metrics,
        # unit tests). Until that lands, count per-symbol poll outcomes
        # so we can SEE which silent-failure mode is active when the
        # engine starves of ticks despite a spinning poll loop.
        self._market_data_diag: dict[str, dict[str, Any]] = {}
        self._last_market_data_metrics_at: float = 0.0
        # Per-ticket snapshot of currently-open broker positions. Used by
        # _poll_once to detect closures (SL/TP fills) so we can Telegram
        # alert with P&L from history_deals_get. Without this, an empty
        # positions_get response silently no-op'd the position-change
        # branch and the engine never learned a trade resolved.
        self._known_position_tickets: dict[int, dict[str, Any]] = {}

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
        # Detect broker's server timezone offset BEFORE polling starts so
        # the very first emitted tick is normalized to real UTC.
        self._mt5_server_offset_seconds = await self._detect_server_offset()
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
        signal_close_price: Optional[float] = None,  # accepted; MT5 telemetry deferred
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
        fill_price = float(_field(result, "price", 0.0) or 0.0) or None
        fill_quantity = float(_field(result, "volume", quantity) or quantity)
        order_event = OrderEvent(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            server_order_id=server_order_id or None,
        )
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ORDER_ACK,
            received_at=time.time(),
            order=order_event,
        ))
        # Telegram alert on successful order entry. CEO trust depends on
        # being able to see fills land without scraping logs. Per the
        # 2026-05-21 EURGBP TP outcome where the CEO had to manually
        # check MT5 to confirm the trade closed — that gap closes here.
        side_label = "BUY" if side == proto.BUY else "SELL" if side == proto.SELL else f"side={side}"
        sl_str = f" SL={stop_price:.5f}" if stop_price else ""
        tp_str = f" TP={target_price:.5f}" if target_price else ""
        push_telegram_alert(
            f"🟢 *Kate ORDER FILLED* — {symbol} {side_label}\n"
            f"  qty={quantity} fill={fill_price or 0:.5f}{sl_str}{tp_str}\n"
            f"  coid={client_order_id}\n"
            f"  ticket={server_order_id}",
        )
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
        import re as _re
        clean_comment = _re.sub(r"[^a-zA-Z0-9_]", "_", client_order_id or "")[:31]
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": int(server_order_id),
            "magic": self.config.magic,
            "comment": clean_comment,
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
        # TEMP — log subscription evidence at INFO so we can confirm seed
        # tick + per-pair subscription succeeded. Codex's patch will
        # replace with structured event emission.
        logger.info(
            "MT5 subscribe_market_data: logical=%s broker=%s seed_bid=%s seed_ask=%s "
            "seed_time_msc=%s subscribed_count=%d",
            spec.logical_symbol, spec.broker_symbol,
            _field(tick, "bid", None), _field(tick, "ask", None),
            _field(tick, "time_msc", None),
            len(self._subscribed_symbols),
        )
        event_tick = self._tick_to_event(spec.logical_symbol, tick)
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_TICK,
            received_at=time.time(),
            tick=event_tick,
        ))

    async def get_recent_candles(
        self,
        *,
        symbol: str,
        count: int,
        timeframe_minutes: int = 1,
    ) -> tuple[Candle, ...]:
        """Return the most recent N completed M1/M5/M15 bars from MT5.

        Uses `copy_rates_from_pos` to seed the engine's strategy history
        on startup so strategies with large `history_window` (e.g. FX
        London Breakout: 480 bars / 8h) can evaluate from minute 1 after
        a supervisor restart instead of waiting 8h for live aggregation.

        Bar timestamps are returned in the broker's server timezone
        (same offset as ticks). We apply the same
        `_mt5_server_offset_seconds` correction so the engine receives
        real-UTC timestamps consistent with live tick aggregation.
        """
        self._require_connected()
        if count <= 0:
            return ()
        mt5 = self._ensure_runtime()
        spec = self._spec(symbol)
        tf_map = {
            1: getattr(mt5, "TIMEFRAME_M1", 1),
            5: getattr(mt5, "TIMEFRAME_M5", 5),
            15: getattr(mt5, "TIMEFRAME_M15", 15),
            30: getattr(mt5, "TIMEFRAME_M30", 30),
            60: getattr(mt5, "TIMEFRAME_H1", 16385),
        }
        tf = tf_map.get(timeframe_minutes)
        if tf is None:
            raise BrokerError(
                f"MT5 get_recent_candles: unsupported timeframe_minutes={timeframe_minutes}"
            )
        bars = await asyncio.to_thread(
            mt5.copy_rates_from_pos, spec.broker_symbol, tf, 1, count,
        )
        if bars is None or len(bars) == 0:
            logger.warning(
                "MT5 get_recent_candles: copy_rates_from_pos returned empty for %s (broker=%s, count=%d, tf=%dm) — %s",
                spec.logical_symbol, spec.broker_symbol, count, timeframe_minutes,
                self._last_error(),
            )
            return ()
        candles: list[Candle] = []
        for b in bars:
            # MT5 position 0 is the active, still-forming bar. We start
            # from position 1 above so strategy history contains only
            # completed candles.
            # b is a numpy structured record from MT5; fields:
            # time (epoch seconds in server tz), open, high, low, close,
            # tick_volume, spread, real_volume. Apply the same TZ offset
            # we detect for the tick path so candle timestamps are real UTC.
            raw_epoch = float(_field(b, "time", 0.0) or 0.0)
            corrected_epoch = raw_epoch - self._mt5_server_offset_seconds
            ts = dt.datetime.utcfromtimestamp(corrected_epoch)
            candles.append(Candle(
                timestamp=ts,
                open=float(_field(b, "open", 0.0) or 0.0),
                high=float(_field(b, "high", 0.0) or 0.0),
                low=float(_field(b, "low", 0.0) or 0.0),
                close=float(_field(b, "close", 0.0) or 0.0),
                volume=int(_field(b, "tick_volume", 0) or 0),
            ))
        candles.sort(key=lambda candle: candle.timestamp)
        logger.info(
            "MT5 get_recent_candles: returned %d bars for %s (timeframe=%dm, "
            "first_ts=%s, last_ts=%s)",
            len(candles), spec.logical_symbol, timeframe_minutes,
            candles[0].timestamp.isoformat() if candles else "n/a",
            candles[-1].timestamp.isoformat() if candles else "n/a",
        )
        return tuple(candles)

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
        # MT5 IC Markets rejects dynamic per-order comments (even ASCII
        # alphanumeric+underscore) with retcode (-2, 'Invalid "comment"
        # argument'). Observed live 2026-05-21 08:07 + 08:11 BST on two
        # consecutive strategy fires after the backfill stack landed.
        # The Python API does some pre-validation we don't fully
        # understand — but the static `config.comment` (set via
        # MT5_COMMENT env, default "kate") is consistently accepted.
        # The MT5 `magic` number is the broker-side identifier we use
        # to claim orders as ours; coid lives in the engine's StateStore
        # for our own bookkeeping, so we don't actually need it in MT5's
        # comment field.
        request: dict[str, Any] = {
            "symbol": symbol,
            "volume": float(quantity),
            "type": mapped_type,
            "magic": self.config.magic,
            "comment": (self.config.comment or "kate")[:31],
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
            # Track per-ticket position state so we can detect closures
            # (broker filled SL or TP and the position is now gone).
            # Previously the empty-positions branch silently no-op'd,
            # leaving the engine + Telegram alerts with no signal that a
            # trade had resolved. Live evidence 2026-05-21 EURGBP TP fill
            # produced no engine-side log; only the broker UI knew.
            current_tickets: dict[int, dict[str, Any]] = {}
            for row in positions or ():
                ticket = int(_field(row, "ticket", 0) or 0)
                current_tickets[ticket] = {
                    "symbol": _field(row, "symbol", ""),
                    "volume": float(_field(row, "volume", 0.0) or 0.0),
                    "type": int(_field(row, "type", 0) or 0),
                    "price_open": float(_field(row, "price_open", 0.0) or 0.0),
                    "profit": float(_field(row, "profit", 0.0) or 0.0),
                }
                await self._events_q.put(BrokerEvent(
                    kind=BrokerEventKind.POSITION_UPDATE,
                    received_at=time.time(),
                    position=self._position_to_event(row),
                ))
            closed_tickets = set(self._known_position_tickets.keys()) - set(current_tickets.keys())
            for closed_ticket in closed_tickets:
                prev = self._known_position_tickets[closed_ticket]
                await self._alert_position_closed(closed_ticket, prev)
            self._known_position_tickets = current_tickets

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
            diag = self._ensure_market_data_diag(logical_symbol)
            diag["last_poll_at"] = time.time()
            diag["poll_count"] += 1
            tick = await asyncio.to_thread(mt5.symbol_info_tick, spec.broker_symbol)
            if tick is None:
                diag["none_count"] += 1
                await self._check_market_data_stale(logical_symbol, "no tick returned")
                continue
            tick_hash = self._tick_hash(tick)
            if tick_hash == self._last_tick_hashes.get(logical_symbol):
                diag["unchanged_count"] += 1
                await self._check_market_data_stale(logical_symbol, "tick unchanged")
                continue
            self._last_tick_hashes[logical_symbol] = tick_hash
            self._record_market_data_emit(logical_symbol, tick)
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.MARKET_DATA_TICK,
                received_at=time.time(),
                tick=self._tick_to_event(logical_symbol, tick),
            ))

        self._log_market_data_metrics()

        await self._check_heartbeat_and_alert()

    def _ensure_market_data_diag(self, logical_symbol: str) -> dict[str, Any]:
        now = time.time()
        return self._market_data_diag.setdefault(
            logical_symbol,
            {
                "last_poll_at": 0.0,
                "last_non_none_tick_at": 0.0,
                "last_emitted_at": 0.0,
                "last_tick_hash": None,
                "poll_count": 0,
                "none_count": 0,
                "unchanged_count": 0,
                "emitted_count": 0,
                "stale_alerted": False,
                "subscribed_at": now,
            },
        )

    def _record_market_data_emit(self, logical_symbol: str, tick: Any) -> None:
        diag = self._ensure_market_data_diag(logical_symbol)
        now = time.time()
        diag["last_non_none_tick_at"] = now
        diag["last_emitted_at"] = now
        diag["last_tick_hash"] = self._tick_hash(tick)
        self._last_tick_hashes[logical_symbol] = diag["last_tick_hash"]
        diag["emitted_count"] += 1
        diag["stale_alerted"] = False

    async def _check_market_data_stale(self, logical_symbol: str, reason: str) -> None:
        diag = self._ensure_market_data_diag(logical_symbol)
        now = time.time()
        last_emitted_at = float(diag.get("last_emitted_at") or diag.get("subscribed_at") or now)
        stale_for = now - last_emitted_at
        if stale_for < self.config.market_data_stale_seconds or diag.get("stale_alerted"):
            return

        message = (
            f"MT5 market data stale for {logical_symbol}: no emitted tick for "
            f"{stale_for:.1f}s ({reason}); polls={diag['poll_count']} "
            f"none={diag['none_count']} unchanged={diag['unchanged_count']} "
            f"emitted={diag['emitted_count']}"
        )
        logger.error(message)
        diag["stale_alerted"] = True
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ERROR,
            received_at=now,
            error_message=message,
        ))

    def _log_market_data_metrics(self) -> None:
        now = time.time()
        if (
            not self._market_data_diag
            or now - self._last_market_data_metrics_at < self.config.market_data_metrics_seconds
        ):
            return
        self._last_market_data_metrics_at = now
        for sym, c in self._market_data_diag.items():
            logger.info(
                "MT5 market data metrics[%s]: last_poll_age=%.1fs "
                "last_non_none_age=%.1fs last_emit_age=%.1fs polls=%d "
                "none=%d unchanged=%d emitted=%d subscribed=%d",
                sym,
                now - float(c.get("last_poll_at") or now),
                now - float(c.get("last_non_none_tick_at") or now),
                now - float(c.get("last_emitted_at") or now),
                c["poll_count"],
                c["none_count"],
                c["unchanged_count"],
                c["emitted_count"],
                len(self._subscribed_symbols),
            )

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

    async def _alert_position_closed(
        self,
        ticket: int,
        prev_snapshot: dict[str, Any],
    ) -> None:
        """Push a Telegram alert announcing a broker-side position close.

        Called from _poll_once when a previously-tracked ticket
        disappears from positions_get — the broker has filled either
        the SL or the TP bracket. We query history_deals_get to pull
        the closing deal's profit so the alert carries P&L.

        Failures here must not break the poll loop — this is
        observability, not a trade-correctness path. Telegram alert
        failures already log a warning inside push_telegram_alert.
        """
        try:
            mt5 = self._ensure_runtime()
            deals = await asyncio.to_thread(mt5.history_deals_get, position=ticket)
            close_price = 0.0
            profit = float(prev_snapshot.get("profit", 0.0) or 0.0)
            close_reason = "CLOSED"
            if deals:
                # The closing deal (entry=DEAL_ENTRY_OUT or last deal in
                # the position lifecycle) carries the realized profit.
                # Different MT5 brokers differ on field availability so
                # we accumulate profit defensively across deals tagged
                # to this position.
                total_profit = 0.0
                latest_price = 0.0
                latest_time = 0
                for d in deals:
                    d_profit = float(_field(d, "profit", 0.0) or 0.0)
                    d_swap = float(_field(d, "swap", 0.0) or 0.0)
                    d_commission = float(_field(d, "commission", 0.0) or 0.0)
                    total_profit += d_profit + d_swap + d_commission
                    d_time = int(_field(d, "time", 0) or 0)
                    if d_time > latest_time:
                        latest_time = d_time
                        latest_price = float(_field(d, "price", 0.0) or 0.0)
                if total_profit != 0.0:
                    profit = total_profit
                if latest_price > 0:
                    close_price = latest_price
            # Infer SL vs TP vs other from sign + which bracket was nearer
            sym = prev_snapshot.get("symbol", "")
            entry = float(prev_snapshot.get("price_open", 0.0) or 0.0)
            if profit > 0:
                close_reason = "TP HIT"
                emoji = "✅"
            elif profit < 0:
                close_reason = "SL HIT"
                emoji = "⛔"
            else:
                close_reason = "CLOSED (flat)"
                emoji = "ℹ️"
            push_telegram_alert(
                f"{emoji} *Kate POSITION CLOSED* — {sym} {close_reason}\n"
                f"  entry={entry:.5f} close={close_price:.5f}\n"
                f"  realized P&L = £{profit:.2f}\n"
                f"  ticket={ticket}",
            )
            logger.info(
                "MT5 position closed: ticket=%d symbol=%s entry=%.5f close=%.5f profit=%.2f reason=%s",
                ticket, sym, entry, close_price, profit, close_reason,
            )
        except Exception:
            logger.exception(
                "MT5 _alert_position_closed failed for ticket=%d — continuing", ticket,
            )

    async def _detect_server_offset(self) -> float:
        """Detect MT5 server timezone offset from real UTC, in seconds.

        MT5's `tick.time` is a Unix epoch, but many brokers' servers run
        in a non-UTC zone (ICMarketsSC-Demo = GMT+3 / EEST). The
        epoch values returned represent that server-local wall clock
        treated as if it were UTC, so `datetime.utcfromtimestamp(tick.time)`
        is the SERVER WALL CLOCK, not real UTC.

        Without correction the engine misreads timestamps and any
        UTC-based filter (trade window, blackout, news buffer) fires at
        the wrong real-world hour. This silently mis-aligned the FX
        London Breakout strategy's 07:00-10:00 UK window for 10 days
        before detection.

        Per task #37 hardening (Codex 2026-05-25 PRECONDITION; RCA at
        handoffs/2026-05-25-claude-to-team-tz-bug-rca-and-restart-fix.md):
        Friday 2026-05-22 23:42 supervisor restart sampled a stale
        post-market-close tick whose `time` field was from earlier in
        the day. The raw delta of 4477s rounded to +1h (wrong) instead
        of the broker's true +3h, causing ~60h of mis-timestamped
        market data. Three guards now prevent silent recurrence:

        1. **Stale-tick guard**: the pre-rounding delta must be within
           MAX_TICK_STALENESS_SECONDS of the rounded hourly value. A
           fresh broker tick has staleness < 60s; a stale post-close
           tick has staleness in minutes-to-hours.
        2. **Sanity bound**: the rounded hours_off must be within
           ACCEPTABLE_OFFSET_HOURS ({0, ±1..±6}). Real retail brokers
           live in this range; anything else is detection failure.
        3. **Loud failure**: on guard violation, emits CRITICAL log
           and raises BrokerError. Supervisor refuses to start with
           an unverified offset rather than silently mis-trade.

        Returns offset in seconds (positive = server is ahead of real
        UTC). Raises BrokerError on any guard failure.
        """
        mt5 = self._ensure_runtime()
        sample_seconds: Optional[float] = None
        sample_symbol: Optional[str] = None
        for spec in self.symbol_map.values():
            tick = await asyncio.to_thread(mt5.symbol_info_tick, spec.broker_symbol)
            if tick is None:
                continue
            raw = _field(tick, "time", None)
            if raw is None:
                continue
            try:
                candidate = float(raw)
            except (TypeError, ValueError):
                continue
            if candidate > 0:
                sample_seconds = candidate
                sample_symbol = spec.broker_symbol
                break

        if sample_seconds is None:
            msg = (
                "MT5 server timezone offset detection FAILED: no tick sample "
                f"available across {len(self.symbol_map)} symbols. Restart "
                "during market hours (Sun 22:00 UTC -> Fri 22:00 UTC) to "
                "re-attempt detection."
            )
            logger.critical(msg)
            raise BrokerError(msg)

        real_utc = time.time()
        delta = sample_seconds - real_utc
        hours_off = round(delta / 3600.0)

        if hours_off not in ACCEPTABLE_OFFSET_HOURS:
            msg = (
                f"MT5 server timezone offset detection FAILED: detected "
                f"{hours_off:+d}h is outside acceptable range "
                f"{sorted(ACCEPTABLE_OFFSET_HOURS)}. "
                f"raw_delta={delta:.1f}s, server_epoch={sample_seconds:.0f}, "
                f"real_utc={real_utc:.0f}, sample_symbol={sample_symbol}. "
                "Supervisor refusing to start with an absurd offset."
            )
            logger.critical(msg)
            raise BrokerError(msg)

        expected_hour_delta = hours_off * 3600.0
        staleness_seconds = abs(delta - expected_hour_delta)
        if staleness_seconds > MAX_TICK_STALENESS_SECONDS:
            msg = (
                f"MT5 server timezone offset detection FAILED: tick staleness "
                f"{staleness_seconds:.1f}s exceeds threshold "
                f"{MAX_TICK_STALENESS_SECONDS}s. raw_delta={delta:.1f}s rounded "
                f"to {hours_off:+d}h, expected_hourly={expected_hour_delta:.0f}s, "
                f"sample_symbol={sample_symbol}. Last tick predates the current "
                "moment beyond live-broker network jitter -- market may be "
                "closed or feed has dropped. Restart during active market "
                "hours (Sun 22:00 UTC -> Fri 22:00 UTC) to re-attempt."
            )
            logger.critical(msg)
            raise BrokerError(msg)

        offset_seconds = hours_off * 3600.0
        logger.info(
            "MT5 server timezone offset detected: %+d hour(s) "
            "(raw delta %.1fs, server epoch %.0f, real UTC %.0f, "
            "sample symbol %s, tick staleness %.1fs). "
            "All emitted tick timestamps will be normalized to real UTC.",
            hours_off, delta, sample_seconds, real_utc,
            sample_symbol, staleness_seconds,
        )
        return offset_seconds

    def _tick_to_event(self, logical_symbol: str, tick: Any) -> MarketDataTick:
        timestamp = _coerce_time(_field(tick, "time_msc", None), millis=True)
        if timestamp is None:
            timestamp = _coerce_time(_field(tick, "time", None), millis=False)
        # Normalize MT5 server clock to real UTC. See connect() +
        # `_detect_server_offset` for the rationale (10 silent days of
        # mis-aligned FX London Breakout window proved this is load-bearing).
        if timestamp is not None and self._mt5_server_offset_seconds:
            timestamp = timestamp - dt.timedelta(seconds=self._mt5_server_offset_seconds)
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
    # MT5's copy_rates_from_pos returns numpy structured arrays whose
    # elements are `numpy.void` records — these support string-key
    # subscript but NOT attribute access (getattr returns the default
    # silently). Try subscript first, fall through to attribute for
    # MT5's tick/account/position objects which expose attributes.
    try:
        return value[name]
    except (TypeError, KeyError, ValueError, IndexError):
        pass
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
