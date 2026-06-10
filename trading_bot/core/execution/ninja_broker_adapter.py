"""NinjaBrokerAdapter — Kate's BrokerAdapter implementation for the
NinjaTrader bridge.

Status (2026-05-18): **SKELETON — order routing only.** Market-data path
not yet wired; account-state/positions/open-orders return placeholder
seed values that will fail the engine's pre-flight check in production
mode. This file is the seam that lets `--broker ninja` exist; the bodies
of the data + state methods fill in during the Option A sprint.

References:
  - decisions/2026-04-24-ceo-ratified-l2-expansion-and-phase-0.md (L2)
  - handoffs/2026-05-18-claude-to-team-NT-data-architecture-Option-A-brainstorm.md
  - handoffs/2026-05-18-codex-to-claude-RESPONSE-NT-data-architecture-Option-A.md
  - handoffs/2026-05-18-gemini-to-team-RESPONSE-option-A-approved-with-concerns.md

What works today
----------------
- connect() / disconnect() — bridge server lifecycle
- submit_order() — builds SignalPayload + sends via NinjaBridgeServer
- events() — translates incoming bridge envelopes into BrokerEvents
  (FILL → ORDER_FILLED / STOP_HIT / TARGET_HIT, HEARTBEAT → HEARTBEAT,
  ACK → ORDER_ACK, RECONCILE_RESP → position snapshot, BAR → bar event
  for the future market-data path)

What does NOT work yet (raises with a pointer to the sprint task)
-----------------------------------------------------------------
- subscribe_market_data() — requires C# bar publisher in
  KateBridgeStrategy.cs (sprint item #2 per Codex's sequence)
- cancel_order() — bridge protocol has no CANCEL message yet (sprint
  item — extend MsgType + KateBridgeStrategy.cs)

What returns a documented stub
------------------------------
- request_account_state() — placeholder NLV; engine pre-flight will
  refuse to seed against this. Real implementation requires extending
  ReconcileResponsePayload with account balance fields.
- request_positions() / request_open_orders() — send RECONCILE_REQ and
  parse the response (partial: positions/brackets are already in the
  protocol via ReconcileResponsePayload).

Wire format note
----------------
The bridge does not require credentials — it's a localhost TCP server
that NT connects to via HMAC. The "logon" step is a no-op (default ABC
behaviour). Symbol mapping is held in `symbol_map` like other adapters.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from . import dtc_protocol as proto
from .broker_adapter import (
    AccountBalanceEvent,
    BarEvent,
    BrokerAdapter,
    BrokerError,
    BrokerEvent,
    BrokerEventKind,
    BrokerSymbolSpec,
    OrderEvent,
    PositionEvent,
)
from .ninja_messages import (
    BracketUpdatePayload,
    FillEventType,
    HeartbeatPayload,
    MsgType,
    ReconcileRequestPayload,
    Side,
    SignalPayload,
    WireEnvelope,
)
from .ninja_transport import NinjaBridgeServer, NotConnectedError

logger = logging.getLogger(__name__)


# Retained for import compatibility with older tests/handoffs. Phase 5
# replaces the sentinel path with NT reconcile-backed account telemetry.
_STUB_NLV_SENTINEL: float = -1.0


@dataclass(frozen=True)
class NinjaConfig:
    """Construction config for the NinjaBrokerAdapter.

    Mirrors the MT5Config / RithmicConfig dataclass pattern. The HMAC
    secret + bridge host/port are the load-bearing values; the rest is
    operational tuning.
    """
    hmac_secret: bytes
    host: str = "127.0.0.1"
    port: int = 9876
    # Time to wait for NT to connect after server.start(). NinjaScript
    # client retries every ~5s on its own loop; allow generous window.
    client_connect_timeout_seconds: float = 30.0
    # Account binding — surfaces in audit logs. Not load-bearing for the
    # bridge itself (NT decides which account the ATM template targets).
    nt_account_label: str = "Sim101"
    # Default ATM template name used when SignalPayload doesn't override.
    default_atm_template: str = "KATE_MES_ORB_BASE"


class NinjaBrokerAdapter(BrokerAdapter):
    """BrokerAdapter for NinjaTrader via the Kate ↔ NT TCP bridge.

    Single-instance design: one adapter wraps one `NinjaBridgeServer`. The
    server is bound on construction (or via `connect()`) and waits for the
    NinjaScript `KateBridgeStrategy.cs` client to connect.

    `symbol_map` maps Kate logical symbols (e.g. "MESU26") to the
    `BrokerSymbolSpec` carrying NT's display form (e.g. "MES 09-26"). The
    NinjaScript side resolves to the actual NT instrument at signal time;
    the adapter just passes the broker form through.
    """

    def __init__(
        self,
        *,
        config: NinjaConfig,
        symbol_map: dict[str, BrokerSymbolSpec],
        bridge: Optional[NinjaBridgeServer] = None,
    ) -> None:
        if not config.hmac_secret:
            raise BrokerError("NinjaConfig.hmac_secret must not be empty")
        self.config = config
        self.symbol_map = dict(symbol_map)
        self._bridge = bridge or NinjaBridgeServer(
            host=config.host,
            port=config.port,
            secret=config.hmac_secret,
        )
        self._connected = False
        self._events_q: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._pump_task: Optional[asyncio.Task[None]] = None
        # Cache the latest reconcile snapshot so request_positions /
        # request_open_orders can return synchronously without round-tripping
        # the bridge each call (matches the seed-call semantics in the ABC).
        self._last_account_state: Optional[AccountBalanceEvent] = None
        self._last_reconcile_positions: tuple[PositionEvent, ...] = ()
        self._last_reconcile_orders: tuple[OrderEvent, ...] = ()
        self._reconcile_event: asyncio.Event = asyncio.Event()
        self._subscribed_symbols: set[str] = set()
        # Bar-dedup state (Codex's design 2026-05-18): keyed by (symbol,
        # bar_index, timestamp). Idempotent retransmits with identical
        # OHLCV are dropped silently; revisions with different OHLCV are
        # surfaced as an ERROR event so the audit layer can fail-day if
        # configured.
        self._last_bar_key: dict[str, tuple[int, str]] = {}
        self._last_bar_ohlcv: dict[tuple[str, int, str], tuple[float, float, float, float, int]] = {}

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Start the bridge listener + wait for NinjaScript to connect.

        Idempotent: re-entry is a no-op when already connected. The
        NinjaScript client retries on its own loop, so we allow generous
        wall time before giving up.
        """
        if self._connected:
            return
        await self._bridge.start()
        logger.info(
            "ninja-adapter: bridge listening on %s:%d — waiting for "
            "KateBridgeStrategy client",
            self.config.host, self._bridge.port,
        )
        try:
            await self._bridge.wait_for_client(
                timeout=self.config.client_connect_timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            await self._bridge.stop()
            raise BrokerError(
                f"ninja-adapter: KateBridgeStrategy client did not connect "
                f"within {self.config.client_connect_timeout_seconds}s. "
                f"On Kate Host VPS: open NinjaTrader, enable the "
                f"KateBridgeStrategy on the MES chart, confirm "
                f"'Enable Auto Trading' is on, retry."
            ) from exc
        self._pump_task = asyncio.create_task(
            self._pump_inbound(), name="ninja-adapter-inbound-pump"
        )
        self._connected = True
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.CONNECTED,
            received_at=_now_epoch(),
        ))

    async def disconnect(self) -> None:
        if not self._connected and self._pump_task is None:
            return
        if self._pump_task is not None:
            self._pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump_task
            self._pump_task = None
        await self._bridge.stop()
        self._connected = False
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.DISCONNECTED,
            received_at=_now_epoch(),
        ))

    # ── Order submission ─────────────────────────────────────────────────

    async def submit_order(
        self,
        *,
        client_order_id: str,
        symbol: str,
        exchange: str,
        side: int,                       # 1=BUY, 2=SELL
        quantity: float,
        order_type: int,
        price: float = 0.0,
        stop_price: Optional[float] = None,
        target_price: Optional[float] = None,
        signal_close_price: Optional[float] = None,
        free_form_text: str = "",
    ) -> str:
        """Translate Kate's normalized order → NinjaScript SIGNAL envelope.

        Kate submits absolute stop/target prices; NinjaScript attaches them
        via the named ATM template. The adapter does not synthesise stop/
        target legs — that's the ATM bracket's job on the NT side.
        """
        if not self._connected:
            raise BrokerError("ninja-adapter: submit_order called while disconnected")
        spec = self.symbol_map.get(symbol)
        if spec is None:
            raise BrokerError(
                f"ninja-adapter: no symbol_map entry for logical {symbol!r} "
                f"— known: {sorted(self.symbol_map)}"
            )
        if side not in (proto.BUY, proto.SELL):
            raise BrokerError(f"ninja-adapter: unsupported side {side!r} (expected 1 or 2)")
        if stop_price is None or target_price is None:
            raise BrokerError(
                "ninja-adapter: SIGNAL requires both stop_price and target_price "
                "— NinjaScript needs absolute prices to ChangeStopTarget on the "
                "ATM bracket. None placeholders are not supported."
            )

        nt_side = Side.BUY.value if side == proto.BUY else Side.SELL.value
        now_utc = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        # Slippage telemetry: prefer the explicit signal_close_price kwarg
        # (engine passes intent.signal_close_price when ORBStrategy or any
        # bar-aware strategy sets it). Fall back to `price` for legacy
        # callers (e.g. unit-test harness that doesn't populate the new
        # field). Final fallback 0.0 is meaningful for market-mode tests
        # only — slippage telemetry against 0.0 is nonsense and should
        # be filtered downstream.
        sig_close = (
            float(signal_close_price)
            if signal_close_price is not None
            else (float(price) if price else 0.0)
        )
        payload = SignalPayload(
            intent_id=client_order_id,
            timestamp=now_utc,
            symbol=symbol,
            nt_symbol=spec.broker_symbol,
            side=nt_side,
            quantity=int(quantity),
            atm_template=self.config.default_atm_template,
            stop_price=float(stop_price),
            target_price=float(target_price),
            signal_close_price=sig_close,
        )
        try:
            seq = await self._bridge.send(MsgType.SIGNAL, payload)
        except NotConnectedError as exc:
            raise BrokerError(
                "ninja-adapter: bridge client disconnected mid-send — "
                "NinjaTrader process may have crashed or strategy was "
                "disabled. Reconnect required."
            ) from exc
        logger.info(
            "ninja-adapter: SIGNAL sent seq=%d intent_id=%s %s %d %s @ATM=%s "
            "stop=%.4f target=%.4f",
            seq, client_order_id, nt_side, int(quantity), spec.broker_symbol,
            self.config.default_atm_template, stop_price, target_price,
        )
        return client_order_id

    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        """SKELETON — bridge protocol has no CANCEL message yet.

        ATM brackets on the NT side are managed by NinjaTrader; an
        entry-leg cancel before fill is a real use case (e.g. risk gate
        late-rejects a signal). Implementing this requires extending
        MsgType + the C# strategy + a round-trip ACK on cancel. Tracked
        as sprint follow-up.
        """
        raise NotImplementedError(
            "ninja-adapter: cancel_order not implemented — bridge protocol "
            "has no CANCEL envelope yet. Sprint follow-up after market-data "
            "path lands. See handoffs/2026-05-18-claude-to-team-"
            "NT-data-architecture-Option-A-brainstorm.md"
        )

    # ── Market data ──────────────────────────────────────────────────────

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        """No-op subscription — NT publishes bars autonomously.

        Unlike MT5/Rithmic where the adapter explicitly subscribes to a
        symbol's tick feed, NinjaTrader's `KateBridgeStrategy` is bound
        to its chart's instrument at load time and publishes BAR
        envelopes for the bar series it's running on. There's no
        subscription protocol from Python's side — the strategy decides
        what it publishes. Python's job is to consume.

        We accept any symbol present in `symbol_map` (a sanity check
        that the engine and the NinjaScript-side instrument config
        agree) and log a notice. The actual bar stream begins whenever
        the NinjaScript strategy is enabled on its chart.

        For symbols not yet in `symbol_map`, raises BrokerError so the
        engine can fail fast on configuration drift.
        """
        if symbol not in self.symbol_map:
            raise BrokerError(
                f"ninja-adapter: subscribe_market_data called with "
                f"unknown logical symbol {symbol!r}. Add to "
                f"KNOWN_INSTRUMENTS + supervisor symbol_map. Known: "
                f"{sorted(self.symbol_map)}"
            )
        self._subscribed_symbols.add(symbol)
        logger.info(
            "ninja-adapter: subscribe_market_data(%s) acknowledged — "
            "NT-side bar publication is autonomous; ensure "
            "KateBridgeStrategy is loaded on the %s chart on Kate Host VPS",
            symbol, self.symbol_map[symbol].broker_symbol,
        )

    # ── State queries ────────────────────────────────────────────────────

    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        await self._send_reconcile_request()
        try:
            await asyncio.wait_for(self._reconcile_event.wait(), timeout=5.0)
        except asyncio.TimeoutError as exc:
            raise BrokerError(
                "ninja-adapter: reconcile response not received within 5s "
                "while requesting account state"
            ) from exc
        if self._last_account_state is None:
            raise BrokerError(
                "ninja-adapter: reconcile response did not include account "
                "balance fields; update KateBridgeStrategy.cs and re-run smoke"
            )
        return self._last_account_state

    async def request_positions(
        self,
        *,
        trade_account: str,
    ) -> tuple[PositionEvent, ...]:
        """Send RECONCILE_REQ and return positions from the response.

        Partial implementation: ReconcileResponsePayload already carries
        OpenPositionSnapshot entries. Returns empty tuple if the bridge
        has not yet received a reconcile response (e.g. at first call
        before pump translates the response). Best-effort.
        """
        await self._send_reconcile_request()
        # Wait briefly for the inbound pump to populate the cache.
        try:
            await asyncio.wait_for(self._reconcile_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(
                "ninja-adapter: reconcile response not received within 5s — "
                "returning empty positions tuple"
            )
            return ()
        return self._last_reconcile_positions

    async def request_open_orders(
        self,
        *,
        trade_account: str,
    ) -> tuple[OrderEvent, ...]:
        """Send RECONCILE_REQ and return pending brackets as OrderEvents.

        Partial: ReconcileResponsePayload carries PendingBracketSnapshot
        entries — these map to OrderEvent with `client_order_id =
        intent_id` and the symbol pulled from the corresponding position
        snapshot (or empty if position-less bracket). Real implementation
        will need either a richer PendingBracketSnapshot or an explicit
        open-orders message type.
        """
        await self._send_reconcile_request()
        try:
            await asyncio.wait_for(self._reconcile_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return ()
        return self._last_reconcile_orders

    # ── Event stream ─────────────────────────────────────────────────────

    def events(self) -> AsyncIterator[BrokerEvent]:
        return _AdapterEventIter(self._events_q)

    # ── Internals ────────────────────────────────────────────────────────

    async def _send_reconcile_request(self) -> None:
        now_utc = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        self._reconcile_event.clear()
        try:
            await self._bridge.send(
                MsgType.RECONCILE_REQ,
                ReconcileRequestPayload(timestamp=now_utc),
            )
        except NotConnectedError as exc:
            raise BrokerError(
                "ninja-adapter: reconcile request failed — bridge client not connected"
            ) from exc

    async def _pump_inbound(self) -> None:
        """Background task: translate incoming bridge envelopes into
        BrokerEvents on `self._events_q`.

        Handles FILL, HEARTBEAT, ACK, RECONCILE_RESP. BAR envelopes are
        accepted but currently not translated into a BrokerEvent (the
        engine seam for bar-close events lands when market-data path is
        wired). BRACKET_UPDATE is logged as smoke/audit evidence.
        Unknown msg_types are logged + dropped.
        """
        while True:
            try:
                envelope = await self._bridge.receive()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("ninja-adapter: pump receive error: %s", exc)
                continue
            try:
                await self._translate_envelope(envelope)
            except Exception as exc:
                logger.exception(
                    "ninja-adapter: failed to translate envelope seq=%d "
                    "msg_type=%s: %s",
                    envelope.sequence, envelope.msg_type, exc,
                )

    async def _translate_envelope(self, envelope: WireEnvelope) -> None:
        msg_type = envelope.msg_type
        payload = envelope.payload
        now = _now_epoch()

        if msg_type == MsgType.FILL.value:
            event_type = payload.get("event_type")
            kind = _FILL_EVENT_KIND_MAP.get(
                event_type, BrokerEventKind.ORDER_FILLED
            )
            await self._events_q.put(BrokerEvent(
                kind=kind,
                received_at=now,
                order=OrderEvent(
                    client_order_id=str(payload.get("intent_id", "")),
                    symbol=str(payload.get("symbol", "")) or _UNKNOWN_SYMBOL,
                    side=1,  # bridge FILL doesn't carry side; engine
                             # tracks via intent_id → entry-side cache
                    quantity=float(payload.get("fill_quantity", 0)),
                    fill_price=float(payload.get("fill_price", 0.0)) or None,
                    fill_quantity=float(payload.get("fill_quantity", 0)) or None,
                    rejected_reason=str(payload.get("reason", "")) or None,
                    server_order_id=str(payload.get("nt_order_id", "")) or None,
                    event_type=str(event_type) if event_type else None,
                    exit_reason=str(payload.get("exit_reason", "")) or None,
                    realized_pnl=float(payload.get("realized_pnl", 0.0)),
                ),
            ))
            if event_type in (
                FillEventType.STOP_HIT.value,
                FillEventType.TARGET_HIT.value,
                FillEventType.MANUAL_FLAT.value,
                FillEventType.OTHER.value,
            ):
                logger.info(
                    "ninja-adapter: EXIT_FILL intent_id=%s event_type=%s "
                    "price=%.4f qty=%s realized_pnl=%.2f",
                    payload.get("intent_id", ""),
                    event_type,
                    float(payload.get("fill_price", 0.0)),
                    payload.get("fill_quantity", 0),
                    float(payload.get("realized_pnl", 0.0)),
                )
            return

        if msg_type == MsgType.HEARTBEAT.value:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.HEARTBEAT,
                received_at=now,
            ))
            return

        if msg_type == MsgType.ACK.value:
            await self._events_q.put(BrokerEvent(
                kind=BrokerEventKind.ORDER_ACK,
                received_at=now,
            ))
            return

        if msg_type == MsgType.BRACKET_UPDATE.value:
            update = BracketUpdatePayload(**payload)
            logger.info(
                "ninja-adapter: BRACKET_UPDATE intent_id=%s symbol=%s "
                "nt_symbol=%s atm_strategy_id=%s %s=%.4f %s=%.4f",
                update.intent_id,
                update.symbol,
                update.nt_symbol,
                update.atm_strategy_id,
                update.stop_name,
                update.stop_price,
                update.target_name,
                update.target_price,
            )
            return

        if msg_type == MsgType.RECONCILE_RESP.value:
            account_name = str(payload.get("account_name", ""))
            cash = float(payload.get("cash_balance", 0.0))
            equity = float(payload.get("equity", 0.0))
            unrealized_pnl = float(payload.get("unrealized_pnl", 0.0))
            realized_pnl = float(payload.get("realized_pnl", 0.0))
            margin_used = float(payload.get("margin_used", 0.0))
            nlv = equity if equity > 0.0 else cash
            self._last_account_state = AccountBalanceEvent(
                cash=cash,
                nlv=nlv,
                pnl=realized_pnl + unrealized_pnl,
                margin_requirement=margin_used,
                currency=str(payload.get("currency", "USD")) or "USD",
            )
            if account_name and account_name != self.config.nt_account_label:
                logger.warning(
                    "ninja-adapter: reconcile account_name=%s differs from "
                    "configured nt_account_label=%s",
                    account_name, self.config.nt_account_label,
                )
            self._last_reconcile_positions = tuple(
                PositionEvent(
                    symbol=str(p.get("symbol", "")),
                    quantity=float(p.get("quantity", 0)),
                    avg_price=float(p.get("avg_price", 0.0)),
                    side=1 if str(p.get("side", "")).upper() == "BUY" else 2,
                    server_position_id=str(p.get("server_position_id", "")) or None,
                )
                for p in payload.get("open_positions", [])
            )
            self._last_reconcile_orders = tuple(
                OrderEvent(
                    client_order_id=str(b.get("intent_id", "")),
                    symbol=str(b.get("symbol", "")) or _UNKNOWN_SYMBOL,
                    side=1 if str(b.get("side", "")).upper() == "BUY" else 2,
                    quantity=float(b.get("quantity", 0)),
                    server_order_id=str(b.get("atm_strategy_id", "")) or None,
                )
                for b in payload.get("pending_brackets", [])
            )
            self._reconcile_event.set()
            return

        if msg_type == MsgType.BAR.value:
            await self._translate_bar(envelope)
            return

        logger.debug(
            "ninja-adapter: unhandled msg_type=%r seq=%d",
            msg_type, envelope.sequence,
        )


    async def _translate_bar(self, envelope: WireEnvelope) -> None:
        """Translate MsgType.BAR → MARKET_DATA_BAR event with dedup.

        Per Codex's design 2026-05-18:
          - First-seen (symbol, bar_index, timestamp) → emit MARKET_DATA_BAR
          - Same key with identical OHLCV → idempotent retransmit, drop silently
          - Same key with different OHLCV → BAR_REVISION; emit ERROR event
            (audit layer / engine decides whether to fail-day)
          - bar_index regression (lower than last seen for symbol) →
            ERROR event (out-of-order; suggests NinjaScript restart or bug)
        """
        payload = envelope.payload
        try:
            symbol = str(payload["symbol"])
            bar_index = int(payload["bar_index"])
            ts_str = str(payload["timestamp"])
            timeframe_minutes = int(payload.get("timeframe_minutes", 1))
            ohlcv = (
                float(payload["open"]),
                float(payload["high"]),
                float(payload["low"]),
                float(payload["close"]),
                int(payload["volume"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            await self._emit_bar_error(
                f"malformed BAR payload seq={envelope.sequence}: {exc} "
                f"(payload={payload!r})"
            )
            return

        if symbol not in self._subscribed_symbols:
            logger.debug(
                "ninja-adapter: BAR for unsubscribed symbol %s seq=%d dropped",
                symbol, envelope.sequence,
            )
            return

        # Out-of-order detection: bar_index must be monotonically
        # non-decreasing per symbol within a single NT session.
        last = self._last_bar_key.get(symbol)
        if last is not None and bar_index < last[0]:
            await self._emit_bar_error(
                f"BAR out-of-order on {symbol}: incoming bar_index={bar_index} "
                f"< last seen={last[0]}. NinjaScript restart? Strategy "
                f"reloaded? Verify bar continuity on the Kate Host chart."
            )
            return

        key = (symbol, bar_index, ts_str)
        prior = self._last_bar_ohlcv.get(key)
        if prior is not None:
            if prior == ohlcv:
                # Idempotent retransmit (e.g. NT reconnected + replayed)
                logger.debug(
                    "ninja-adapter: BAR retransmit (%s bar_index=%d) — drop",
                    symbol, bar_index,
                )
                return
            await self._emit_bar_error(
                f"BAR_REVISION on {symbol} bar_index={bar_index} ts={ts_str}: "
                f"prior OHLCV={prior} new={ohlcv}. Fail validation day per "
                f"audit protocol; investigate NT data revision behaviour."
            )
            return

        try:
            ts = dt.datetime.fromisoformat(ts_str)
        except ValueError as exc:
            await self._emit_bar_error(
                f"BAR timestamp not ISO 8601 on {symbol} bar_index={bar_index}: "
                f"{ts_str!r} — {exc}"
            )
            return
        if ts.tzinfo is None:
            # NinjaScript canonical UTC ISO 8601 includes offset; refuse
            # naive timestamps to prevent silent local-time leaks.
            await self._emit_bar_error(
                f"BAR timestamp lacks timezone on {symbol} bar_index={bar_index}: "
                f"{ts_str!r}. NinjaScript must emit tz-aware UTC."
            )
            return

        self._last_bar_key[symbol] = (bar_index, ts_str)
        self._last_bar_ohlcv[key] = ohlcv

        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_BAR,
            received_at=_now_epoch(),
            bar=BarEvent(
                symbol=symbol,
                timestamp=ts,
                timeframe_minutes=timeframe_minutes,
                open=ohlcv[0],
                high=ohlcv[1],
                low=ohlcv[2],
                close=ohlcv[3],
                volume=ohlcv[4],
            ),
        ))

    async def _emit_bar_error(self, message: str) -> None:
        logger.error("ninja-adapter: %s", message)
        await self._events_q.put(BrokerEvent(
            kind=BrokerEventKind.ERROR,
            received_at=_now_epoch(),
            error_message=message,
        ))


_FILL_EVENT_KIND_MAP: dict[str, BrokerEventKind] = {
    FillEventType.ENTRY.value: BrokerEventKind.ORDER_FILLED,
    FillEventType.STOP_HIT.value: BrokerEventKind.ORDER_FILLED,
    FillEventType.TARGET_HIT.value: BrokerEventKind.ORDER_FILLED,
    FillEventType.MANUAL_FLAT.value: BrokerEventKind.ORDER_FILLED,
    FillEventType.OTHER.value: BrokerEventKind.ORDER_FILLED,
    FillEventType.CANCELLED.value: BrokerEventKind.ORDER_CANCELED,
    FillEventType.REJECTED.value: BrokerEventKind.ORDER_REJECTED,
}


_UNKNOWN_SYMBOL = "__unknown__"


def _now_epoch() -> float:
    return dt.datetime.now(dt.timezone.utc).timestamp()


class _AdapterEventIter:
    """Async iterator wrapper over the adapter's event queue.

    Matches the BrokerAdapter ABC's `events() -> AsyncIterator[BrokerEvent]`
    contract. Yields DISCONNECTED then stops cleanly.
    """

    def __init__(self, q: asyncio.Queue[BrokerEvent]) -> None:
        self._q = q
        self._stopped = False

    def __aiter__(self) -> "_AdapterEventIter":
        return self

    async def __anext__(self) -> BrokerEvent:
        if self._stopped:
            raise StopAsyncIteration
        event = await self._q.get()
        if event.kind is BrokerEventKind.DISCONNECTED:
            self._stopped = True
        return event


__all__ = ["NinjaBrokerAdapter", "NinjaConfig"]
