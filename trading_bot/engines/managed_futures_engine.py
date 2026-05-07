"""
ManagedFuturesEngine — Direction A.

Drives the spine: candles → strategy → intent → risk → execution → state,
with broker-event handling and periodic reconciliation.

Lifecycle:
    engine = ManagedFuturesEngine(...)
    await engine.start()        # connect DTC, logon, seed state + history
    await engine.run()           # main loop until stop()
    await engine.stop()

Per CLAUDE.md and architecture doc, the engine is single-threaded asyncio.
There is one engine per direction (A here, C later). It loops at
`tick_interval_seconds`, draining DTC events and polling candles.
Strategy evaluation runs on candle close. Reconciliation runs every
`reconciliation_interval_seconds`.

Drift handling: when the Reconciler reports drift between local StateStore
and broker POSITION_UPDATE snapshots, the engine emits a callback (or logs)
but does NOT auto-correct. Position correction crosses the CEO approval
gate per CLAUDE.md.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from trading_bot.core.data import Candle, CandleManager
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.dtc_client import DTCClient, DTCMessage
from trading_bot.core.risk import (
    AccountState,
    RiskManager,
    RiskVerdict,
    TradeIntent,
)
from trading_bot.core.state import (
    Reconciler,
    ReconciliationReport,
    RemoteOrder,
    RemotePosition,
    StateStore,
)
from trading_bot.core.strategy import Strategy, StrategyContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _BracketSpec:
    """Stop/target parameters cached when an entry order is submitted, used
    when the entry's FILLED ORDER_UPDATE arrives to spawn the exit legs.

    `entry_side` is the entry-order side; close orders use the opposite.
    `dtc_symbol` and `exchange` are needed because by the time the exit
    legs are submitted we no longer have the original TradeIntent in scope.
    """
    entry_coid: str
    dtc_symbol: str
    exchange: str
    entry_side: int          # 1=BUY, 2=SELL — exits flip this
    quantity: float
    stop_loss: float
    take_profit: Optional[float]


@dataclass(frozen=True)
class InstrumentMeta:
    """Per-instrument calibration. Sourced from config/instruments.json.

    The three name fields capture distinct identifiers we encounter in the
    real Sierra+DTC stack — they often differ:

      symbol         — logical/display name. Used for strategy ctx,
                       TradeIntent, StateStore, log lines. Stable, short.
                       Example: "MESM26".
      scid_filename  — name of the .scid file Sierra writes ticks to,
                       WITHOUT the .scid extension. Used by CandleManager
                       to find the file. Example: "MESM26_FUT_CME".
      dtc_symbol     — what Sierra accepts on the DTC wire as Symbol[64]
                       in SUBMIT_NEW_SINGLE_ORDER and what it returns in
                       POSITION_UPDATE. Example: "MESM26-CME".

    Phase A integration tests passed because the test fixtures used the
    same string for all three. In production they MUST be set
    independently — the engine maps logical → scid for CandleManager
    calls and logical → dtc for DTCClient calls.
    """
    symbol: str
    exchange: str
    scid_filename: str
    dtc_symbol: str
    tick_size: float
    tick_value: float
    per_contract_margin: float
    # Per-contract round-trip commission. For sim mode, set to 0.0 to match
    # Sierra Trade Sim's default zero-commission fills (verified by COO
    # Gemini 2026-04-27 — `findstr /s /i commission C:\SierraChart\*` empty
    # across xml/cht/ini). For live mode, set to the broker's actual rate;
    # EdgeClear MES is $1.38/RT ($0.69/side) verified April 2026. Setting
    # this AND Sierra's matching commission config in lockstep keeps the
    # local NLV vs broker NLV reconciliation clean.
    round_trip_commission: float = 0.0


class ManagedFuturesEngine:
    def __init__(
        self,
        *,
        symbols: list[str],
        instruments: dict[str, InstrumentMeta],
        candle_manager: CandleManager,
        strategy: Strategy,
        risk: RiskManager,
        state: StateStore,
        reconciler: Reconciler,
        dtc_client: DTCClient,
        trade_account: str,
        submit_trade_account: Optional[str] = None,
        no_trade_windows_utc: Optional[list[tuple[dt.time, dt.time]]] = None,
        client_name: str = "TRADING_BOT",
        trade_mode: int = proto.TRADE_MODE_DEMO,
        history_size: Optional[int] = None,
        tick_interval_seconds: float = 1.0,
        reconciliation_interval_seconds: float = 30.0,
        seed_timeout_seconds: float = 1.0,
        on_drift: Optional[Callable[[ReconciliationReport], None]] = None,
        on_intent_rejected: Optional[Callable[[TradeIntent, RiskVerdict], None]] = None,
    ) -> None:
        for s in symbols:
            if s not in instruments:
                raise ValueError(f"no InstrumentMeta provided for symbol {s!r}")
        self.symbols = symbols
        self.instruments = instruments
        self.candle_manager = candle_manager
        self.strategy = strategy
        self.risk = risk
        self.state = state
        self.reconciler = reconciler
        self.dtc = dtc_client
        self.trade_account = trade_account
        # Sierra DTC has split routing: SEED requests (msgs 305/300/601)
        # work with empty TradeAccount because Sierra infers from the
        # logon's account context. SUBMIT (msg 208) does NOT — Sierra
        # validates TradeAccount up front and rejects with "Trade Account
        # is empty" in Trade Simulation Mode (verified via Sierra's
        # TradeActivityLog 2026-04-29). Default to `trade_account` so
        # behaviour is unchanged when callers haven't opted in; pass an
        # explicit sim-account string here once we know what Sierra is
        # configured to accept.
        self.submit_trade_account = (
            submit_trade_account if submit_trade_account is not None
            else trade_account
        )
        # Volatility-blackout windows in UTC. The strategy is NOT invoked
        # for candle-closes whose timestamp falls in any window — no
        # signal generated, no risk-rejection log noise. Windows protect
        # the entry path only; existing positions keep their bracket
        # lifecycle untouched (force-closing into thin liquidity is
        # worse than letting the bracket fire at predefined levels).
        # See decisions/2026-04-30-paper-validation-operational-additions.md
        # for the data-backed motivation.
        self.no_trade_windows_utc: tuple[tuple[dt.time, dt.time], ...] = tuple(
            no_trade_windows_utc or ()
        )
        self.client_name = client_name
        self.trade_mode = trade_mode
        self.tick_interval_seconds = tick_interval_seconds
        self.reconciliation_interval_seconds = reconciliation_interval_seconds
        self.seed_timeout_seconds = seed_timeout_seconds
        self.on_drift = on_drift
        self.on_intent_rejected = on_intent_rejected

        self._history_size = history_size or max(2 * strategy.history_window, 200)
        self._history: dict[str, deque[Candle]] = {
            s: deque(maxlen=self._history_size) for s in symbols
        }
        # Reverse lookup: dtc_symbol → logical symbol. Sierra reports
        # POSITION_UPDATE in dtc_symbol form; we key broker_positions
        # by logical symbol for clean reconciler compare against the
        # local StateStore (which uses logical symbols).
        self._dtc_to_logical: dict[str, str] = {
            meta.dtc_symbol: logical for logical, meta in instruments.items()
        }
        # Latest broker-side position snapshot per (LOGICAL symbol, exchange).
        self._broker_positions: dict[tuple[str, str], RemotePosition] = {}
        # Latest broker-side order status per client_order_id
        self._broker_orders: dict[str, RemoteOrder] = {}
        # Latest known account state (fed from ACCOUNT_BALANCE_UPDATE)
        self._account_state: Optional[AccountState] = None
        self._starting_nlv: Optional[float] = None
        self._running = False

        # ── Bracket-order tracking (synthetic OCO) ────────────────────
        # `_pending_brackets[entry_coid]` holds the stop/target params we'll
        # submit once Sierra sends back ORDER_UPDATE status=FILLED for the
        # entry. Submitting brackets only after entry-fill avoids the brief
        # window where Sierra has the stop/target sitting in the book
        # against a position that doesn't yet exist (some Sierra builds
        # reject this; doing it sequentially is safer than relying on
        # native bracket support whose wire format we haven't validated).
        self._pending_brackets: dict[str, "_BracketSpec"] = {}
        # `_sibling_orders` maps each exit-leg coid to its sibling's coid.
        # When stop fills, cancel target. When target fills, cancel stop.
        # Both directions populated when brackets are submitted; entries
        # cleared once a sibling cancel has been issued.
        self._sibling_orders: dict[str, str] = {}

        # Missing-data canary. If no candles arrive for a symbol within
        # 60s of the engine running, log a single WARNING. Wiltshire 2026
        # -05-06/07 ate 26 hours of silent heartbeats because the .scid
        # filename convention differed between rigs and CandleManager
        # silently polled a file that didn't exist. The supervisor now
        # resolves the filename at startup, but this canary catches any
        # other path where Sierra stops writing bars (chart closed,
        # subscription expired, file permissions, etc.).
        self._first_run_time: Optional[float] = None
        self._missing_data_warned: dict[str, bool] = {s: False for s in symbols}

    # ── Public lifecycle ──────────────────────────────────────────────────
    async def start(self) -> None:
        """Warm candle history, then connect DTC + seed initial broker state.

        Order matters: the scid backfill is synchronous file I/O that can
        block the asyncio event loop for tens of seconds on multi-GB
        history files (the KATE-derived scid_parser reads the last 1 GB
        of ticks on startup — ~50 s observed on Contabo Win VPS for
        MESM26 on 2026-04-28). If DTC is connected before that load
        finishes, Sierra disconnects us for unacknowledged heartbeats
        and the seed phase never runs.

        Warming candles BEFORE dtc.connect() means Sierra only sees us
        once the loop is responsive. TODO: replace this load-everything
        approach with a bounded warmup (only need ATR + breakout
        lookback worth of candles, not 43k) — see
        `omni/proposals/2026-04-28-claude-kate-scid-warmup-bounded.md`.
        """
        for symbol in self.symbols:
            meta = self.instruments[symbol]
            backfill = self.candle_manager.backfill(
                meta.scid_filename, max_candles=self._history_size
            )
            self._history[symbol].extend(backfill)
            # Initialize tail baseline (first poll returns [] but baselines
            # the file position so subsequent polls see only new ticks).
            self.candle_manager.poll(meta.scid_filename)

        await self.dtc.connect()
        await self.dtc.logon(
            client_name=self.client_name,
            trade_mode=self.trade_mode,
        )

        await self._seed_broker_state()

        self._running = True

    async def run(self) -> None:
        """Main loop. Returns when stop() is called."""
        if not self._running:
            raise RuntimeError("engine.start() must be called before run()")

        loop = asyncio.get_running_loop()
        last_reconciliation = loop.time()

        while self._running:
            await self._drain_dtc_events(timeout=0.001)

            for symbol in self.symbols:
                await self._process_symbol(symbol)

            now = loop.time()
            if now - last_reconciliation >= self.reconciliation_interval_seconds:
                self._run_reconciliation()
                last_reconciliation = now

            await asyncio.sleep(self.tick_interval_seconds)

    async def stop(self) -> None:
        self._running = False
        await self.dtc.disconnect()

    # ── State accessors (read-only for tests/observability) ───────────────
    @property
    def account_state(self) -> Optional[AccountState]:
        return self._account_state

    @property
    def broker_positions(self) -> dict[tuple[str, str], RemotePosition]:
        return dict(self._broker_positions)

    @property
    def broker_orders(self) -> dict[str, RemoteOrder]:
        return dict(self._broker_orders)

    def history(self, symbol: str) -> tuple[Candle, ...]:
        return tuple(self._history[symbol])

    # ── Initial state seeding ─────────────────────────────────────────────
    async def _seed_broker_state(self) -> None:
        """Send the three snapshot requests and consume responses.

        Does NOT block forever — uses a bounded drain. If the broker
        doesn't reply within the timeout, we proceed with whatever we
        have (the reconciler will catch the gap on its next pass)."""
        # Per COO Gemini's 2026-04-27 wire-capture diff: Sierra wants the
        # TradeAccount field EMPTY in seed requests (it filters server-side
        # based on the logon's account context). Sending "E8933" caused the
        # initial connect to receive zero balance/positions responses — the
        # account-name mismatch was a silent drop. Use empty here; the
        # configured self.trade_account still flows through SUBMIT orders.
        self.dtc._writer.write(   # noqa: SLF001 — direct send is fine here
            proto.pack_account_balance_request(request_id=1, trade_account="")
        )
        self.dtc._writer.write(   # noqa: SLF001
            proto.pack_current_positions_request(request_id=2, trade_account="")
        )
        self.dtc._writer.write(   # noqa: SLF001
            proto.pack_open_orders_request(request_id=3, trade_account="")
        )
        await self.dtc._writer.drain()   # noqa: SLF001

        # Drain responses up to the configured seed timeout. If the broker
        # doesn't reply within the window we proceed anyway — the
        # reconciler will catch any state gaps on its next pass.
        await self._drain_dtc_events(timeout=self.seed_timeout_seconds)

    # ── Event handling ────────────────────────────────────────────────────
    async def _drain_dtc_events(self, *, timeout: float) -> int:
        """Pull and dispatch all messages currently queued. Returns count."""
        count = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                # Always at least try a non-blocking peek
                remaining = 0.0001
            try:
                msg = await asyncio.wait_for(
                    self.dtc.recv_event(), timeout=remaining
                )
            except asyncio.TimeoutError:
                return count
            await self._handle_dtc_message(msg)
            count += 1

    async def _handle_dtc_message(self, msg: DTCMessage) -> None:
        # Always log inbound traffic at INFO so paper-live logs are
        # diagnosable without flipping log levels mid-session.
        logger.info(
            "DTC inbound: msg_type=%d size=%d",
            msg.msg_type, len(msg.body),
        )
        try:
            if msg.msg_type == proto.ORDER_UPDATE:
                await self._handle_order_update(proto.unpack_order_update(msg.body))
            elif msg.msg_type == proto.POSITION_UPDATE:
                self._handle_position_update(proto.unpack_position_update(msg.body))
            elif msg.msg_type == proto.ACCOUNT_BALANCE_UPDATE:
                self._handle_account_balance_update(
                    proto.unpack_account_balance_update(msg.body)
                )
            elif msg.msg_type == proto.HEARTBEAT:
                pass   # background heartbeat task handles outbound
            else:
                logger.warning("engine: unhandled msg_type %d", msg.msg_type)
        except Exception:
            logger.exception("engine: failed to handle msg_type %d", msg.msg_type)

    async def _handle_order_update(self, msg: proto.OrderUpdate) -> None:
        # Sierra sends a sentinel ORDER_UPDATE with NoOrders=1 when
        # responding to OPEN_ORDERS_REQUEST on a flat account. This is
        # NOT a real order update — skip it so the empty client_order_id
        # doesn't poison broker_orders and create false reconciliation
        # drift against the empty local state.
        if msg.no_orders:
            logger.info(
                "engine: ORDER_UPDATE no-orders sentinel (broker reports "
                "no open orders) — broker_orders cleared"
            )
            self._broker_orders.clear()
            return
        store_status = proto.dtc_order_status_to_state_store(msg.order_status)
        # Visibility: log every real ORDER_UPDATE so we can diagnose the
        # submit→fill round-trip from the Kate log alone. Without this,
        # silent fills/rejects/cancels look identical to "Sierra never
        # responded" — see 2026-04-29 paper-validation drift incident.
        logger.info(
            "engine: ORDER_UPDATE coid=%s status=%d→%s reason=%d "
            "filled=%g remaining=%g avg_fill=%g info=%r",
            msg.client_order_id, msg.order_status, store_status,
            msg.order_update_reason,
            msg.filled_quantity, msg.remaining_quantity,
            msg.average_fill_price, msg.info_text,
        )
        # Always update our remote-orders snapshot
        self._broker_orders[msg.client_order_id] = RemoteOrder(
            client_order_id=msg.client_order_id, status=store_status,
        )
        # Update StateStore — but only if this client_order_id is one we
        # submitted (record_order ran before submit). Updates to unknown
        # IDs are dropped silently — those are echoes from concurrent
        # external order activity.
        existing = self.state.get_order(client_order_id=msg.client_order_id)
        if existing is None:
            logger.info(
                "engine: ORDER_UPDATE for unknown coid=%s — not in local "
                "StateStore, broker_orders updated only",
                msg.client_order_id,
            )
            return
        rejected_reason = msg.info_text if store_status == "REJECTED" else None
        fill_price = msg.average_fill_price if store_status == "FILLED" else None
        fill_qty = msg.filled_quantity if store_status == "FILLED" else None
        self.state.update_order_status(
            client_order_id=msg.client_order_id,
            status=store_status,
            fill_price=fill_price,
            fill_quantity=fill_qty,
            rejected_reason=rejected_reason,
        )

        # ── Bracket-order side effects on FILL ────────────────────────
        if store_status == "FILLED":
            # 1. If this is an ENTRY for which we cached a bracket spec,
            #    submit the stop + target legs now.
            bracket = self._pending_brackets.pop(msg.client_order_id, None)
            if bracket is not None:
                await self._submit_exit_brackets(bracket)
            # 2. If this is an EXIT leg, cancel its sibling so the
            #    position closes via exactly one of the two legs.
            sibling = self._sibling_orders.pop(msg.client_order_id, None)
            if sibling is not None:
                # Symmetric removal: the sibling no longer needs to know
                # about us either.
                self._sibling_orders.pop(sibling, None)
                await self._cancel_sibling_after_fill(
                    filled_coid=msg.client_order_id, sibling_coid=sibling,
                )

    def _handle_position_update(self, msg: proto.PositionUpdate) -> None:
        if msg.no_positions:
            self._broker_positions.clear()
            return
        # Sierra reports POSITION_UPDATE.symbol in dtc_symbol form
        # (e.g. "MESM26-CME"). Map to logical for reconciler key consistency.
        logical = self._dtc_to_logical.get(msg.symbol)
        if logical is None:
            logger.debug(
                "engine: POSITION_UPDATE for unmanaged dtc_symbol %r — ignored",
                msg.symbol,
            )
            return
        key = (logical, msg.exchange)
        if msg.quantity == 0:
            self._broker_positions.pop(key, None)
        else:
            self._broker_positions[key] = RemotePosition(
                symbol=logical, exchange=msg.exchange, quantity=msg.quantity,
            )

    def _handle_account_balance_update(
        self, msg: proto.AccountBalanceUpdate
    ) -> None:
        nlv = msg.net_liquidation_value
        if self._starting_nlv is None:
            self._starting_nlv = nlv
        self._account_state = AccountState(
            nlv=nlv,
            starting_nlv=self._starting_nlv,
            open_positions_margin=msg.margin_requirement,
            open_position_count=len(self._broker_positions),
        )
        self.state.record_account_snapshot(
            nlv=nlv, drawdown_pct=self._account_state.drawdown_pct,
        )

    # ── Per-tick processing ───────────────────────────────────────────────
    async def _process_symbol(self, symbol: str) -> None:
        meta = self.instruments[symbol]
        closed = self.candle_manager.poll(meta.scid_filename)
        for candle in closed:
            self._history[symbol].append(candle)
            await self._on_candle_close(symbol, candle)

        # Missing-data canary: warn ONCE per symbol if no candles arrive
        # within 60s of engine start. See note in __init__.
        if not self._missing_data_warned[symbol] and not self._history[symbol]:
            now = asyncio.get_running_loop().time()
            if self._first_run_time is None:
                self._first_run_time = now
            elif now - self._first_run_time > 60:
                logger.warning(
                    "engine: NO CANDLES received for %s after 60s — check that "
                    "Sierra is recording bars for %s.scid in the configured "
                    "scid_dir; strategy will not fire without bar data",
                    symbol, meta.scid_filename,
                )
                self._missing_data_warned[symbol] = True

    async def _on_candle_close(self, symbol: str, candle: Candle) -> None:
        if self._account_state is None:
            logger.debug("engine: no account state yet, skipping strategy on %s", symbol)
            return

        # Volatility-blackout: skip strategy invocation entirely if the
        # bar's UTC timestamp falls inside any configured window. Existing
        # positions are unaffected — their stop/target brackets remain in
        # the broker's book and exit normally if hit. This only blocks
        # NEW entries during the window.
        if self._is_in_no_trade_window(candle.timestamp):
            logger.debug(
                "engine: blackout window active at %s — skipping strategy on %s",
                candle.timestamp.time().strftime("%H:%M:%S"), symbol,
            )
            return

        meta = self.instruments[symbol]
        history = self.history(symbol)
        if len(history) < self.strategy.history_window:
            return

        ctx = StrategyContext(
            symbol=symbol,
            exchange=meta.exchange,
            candle=candle,
            history=history,
            tick_size=meta.tick_size,
            tick_value=meta.tick_value,
            per_contract_margin=meta.per_contract_margin,
            round_trip_commission=meta.round_trip_commission,
            has_open_position=self._has_open_position(symbol, meta.exchange),
        )

        intent = self.strategy.on_candle_close(ctx)
        if intent is None:
            return

        await self._evaluate_intent(intent)

    async def _evaluate_intent(self, intent: TradeIntent) -> None:
        assert self._account_state is not None  # checked in _on_candle_close
        verdict = self.risk.evaluate(intent, self._account_state)
        if not verdict.approved:
            logger.info(
                "intent rejected by risk: %s | reasons=%s",
                intent.intent_id, verdict.reasons,
            )
            if self.on_intent_rejected is not None:
                with _suppress_callback_errors():
                    self.on_intent_rejected(intent, verdict)
            return

        await self._submit_order(intent)

    async def _submit_order(self, intent: TradeIntent) -> None:
        # Record locally as PENDING BEFORE wire submit — if submit fails
        # mid-flight the reconciler can detect the orphan record.
        self.state.record_order(
            client_order_id=intent.intent_id,
            symbol=intent.symbol,           # logical — for state
            exchange=intent.exchange,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
        )
        meta = self.instruments[intent.symbol]
        try:
            await self.dtc.submit_order(
                symbol=meta.dtc_symbol,     # dtc_symbol on the wire
                exchange=intent.exchange,
                trade_account=self.submit_trade_account,
                client_order_id=intent.intent_id,
                side=intent.side,
                quantity=intent.quantity,
                order_type=intent.order_type,
                price1=intent.price,
                free_form_text=intent.strategy_name[:48],
            )
            logger.info(
                "submitted order %s for %s (dtc_symbol=%s)",
                intent.intent_id, intent.symbol, meta.dtc_symbol,
            )
        except Exception:
            # Mark the order REJECTED locally so the active-orders set is
            # accurate. The reconciler will further validate on next pass.
            self.state.update_order_status(
                client_order_id=intent.intent_id,
                status="REJECTED",
                rejected_reason="submit failed (transport error)",
            )
            raise

        # Cache bracket params keyed by entry coid. Submitted only after
        # the entry's FILLED ORDER_UPDATE arrives (see _handle_order_update).
        # Risk policy already gates this — entries without stop_loss are
        # rejected before reaching here when require_stop_loss=True.
        if intent.stop_loss is not None:
            self._pending_brackets[intent.intent_id] = _BracketSpec(
                entry_coid=intent.intent_id,
                dtc_symbol=meta.dtc_symbol,
                exchange=intent.exchange,
                entry_side=intent.side,
                quantity=intent.quantity,
                stop_loss=intent.stop_loss,
                take_profit=intent.take_profit,
            )

    async def _submit_exit_brackets(self, bracket: _BracketSpec) -> None:
        """Submit stop and (optional) take-profit close orders after the
        entry fills. The two legs are mutually-cancelling: when one fills
        we cancel the other via _handle_order_update's sibling logic.

        ID convention: <entry_coid>-S for stop, <entry_coid>-T for target.
        Both fit DTC's 32-byte ClientOrderID since entries are ≤30 chars."""
        close_side = proto.SELL if bracket.entry_side == proto.BUY else proto.BUY
        stop_coid = f"{bracket.entry_coid}-S"
        target_coid = (
            f"{bracket.entry_coid}-T" if bracket.take_profit is not None else None
        )

        # Track sibling links BEFORE submit so even if a fill races back
        # the cancel-on-fill logic has the linkage to act on.
        if target_coid is not None:
            self._sibling_orders[stop_coid] = target_coid
            self._sibling_orders[target_coid] = stop_coid

        # 1) STOP leg — close at adverse price
        # Logical symbol for StateStore; engine doesn't care about
        # bracket-vs-entry distinction at the SQLite level.
        # We look up logical from dtc_symbol via the reverse map.
        logical = self._dtc_to_logical.get(bracket.dtc_symbol, bracket.dtc_symbol)
        self.state.record_order(
            client_order_id=stop_coid,
            symbol=logical, exchange=bracket.exchange,
            side=close_side, quantity=bracket.quantity,
            order_type=proto.ORDER_TYPE_STOP,
        )
        try:
            await self.dtc.submit_order(
                symbol=bracket.dtc_symbol,
                exchange=bracket.exchange,
                trade_account=self.submit_trade_account,
                client_order_id=stop_coid,
                side=close_side,
                quantity=bracket.quantity,
                order_type=proto.ORDER_TYPE_STOP,
                price1=bracket.stop_loss,
                free_form_text=f"stop:{bracket.entry_coid[:40]}"[:48],
            )
            logger.info(
                "submitted bracket STOP %s @ %.2f (entry=%s)",
                stop_coid, bracket.stop_loss, bracket.entry_coid,
            )
        except Exception:
            self.state.update_order_status(
                client_order_id=stop_coid, status="REJECTED",
                rejected_reason="bracket-stop submit failed (transport error)",
            )
            # Don't propagate — keep going to attempt target submit. A
            # missing stop is a serious situation but better to have one
            # exit leg in book than zero. Reconciler will surface this.
            logger.exception("engine: bracket STOP submit failed")

        # 2) TARGET leg — close at favorable price (optional)
        if target_coid is not None:
            self.state.record_order(
                client_order_id=target_coid,
                symbol=logical, exchange=bracket.exchange,
                side=close_side, quantity=bracket.quantity,
                order_type=proto.ORDER_TYPE_LIMIT,
            )
            try:
                await self.dtc.submit_order(
                    symbol=bracket.dtc_symbol,
                    exchange=bracket.exchange,
                    trade_account=self.submit_trade_account,
                    client_order_id=target_coid,
                    side=close_side,
                    quantity=bracket.quantity,
                    order_type=proto.ORDER_TYPE_LIMIT,
                    price1=bracket.take_profit,
                    free_form_text=f"target:{bracket.entry_coid[:38]}"[:48],
                )
                logger.info(
                    "submitted bracket TARGET %s @ %.2f (entry=%s)",
                    target_coid, bracket.take_profit, bracket.entry_coid,
                )
            except Exception:
                self.state.update_order_status(
                    client_order_id=target_coid, status="REJECTED",
                    rejected_reason="bracket-target submit failed (transport error)",
                )
                logger.exception("engine: bracket TARGET submit failed")

    async def _cancel_sibling_after_fill(
        self, *, filled_coid: str, sibling_coid: str,
    ) -> None:
        """Issue a CANCEL_ORDER for the sibling exit leg after one side
        fills. Sierra responds with an ORDER_UPDATE status=CANCELED for
        the sibling, which `_handle_order_update` writes through to
        StateStore via the normal flow."""
        try:
            await self.dtc.cancel_order(
                client_order_id=sibling_coid,
                trade_account=self.submit_trade_account,
            )
            logger.info(
                "engine: cancelled sibling %s after %s filled",
                sibling_coid, filled_coid,
            )
        except Exception:
            # Cancel failed — the sibling may still be in book. Reconciler
            # will detect the discrepancy on next pass; not auto-corrected
            # here per CEO approval-gate policy.
            logger.exception(
                "engine: failed to cancel sibling %s after %s filled",
                sibling_coid, filled_coid,
            )

    def _has_open_position(self, symbol: str, exchange: str) -> bool:
        pos = self._broker_positions.get((symbol, exchange))
        return pos is not None and pos.quantity != 0

    def _is_in_no_trade_window(self, timestamp: dt.datetime) -> bool:
        """True if the candle's UTC time-of-day falls in any configured
        blackout window. Handles wrap-around windows (e.g. 23:30→00:30)
        for future overnight-session blackouts; all-numeric, no DST math."""
        if not self.no_trade_windows_utc:
            return False
        # Strategy and engine treat candle timestamps as UTC. Sierra's
        # SCID base-date math in scid_parser.py produces naive UTC
        # datetimes; we compare against naive time-of-day here. If we
        # ever flip to tz-aware datetimes upstream, normalize via
        # `.astimezone(dt.timezone.utc).time()` before comparing.
        t = timestamp.time()
        for start, end in self.no_trade_windows_utc:
            if start <= end:
                if start <= t < end:
                    return True
            else:
                # Wrap-around window (start > end means crosses midnight)
                if t >= start or t < end:
                    return True
        return False

    # ── Reconciliation ────────────────────────────────────────────────────
    def _run_reconciliation(self) -> ReconciliationReport:
        local_positions = self.state.get_open_positions()
        local_orders = self.state.get_active_orders()
        report = self.reconciler.reconcile(
            local_positions=local_positions,
            remote_positions=tuple(self._broker_positions.values()),
            local_orders=local_orders,
            remote_orders=tuple(self._broker_orders.values()),
        )
        if report.has_drift:
            logger.warning(
                "reconciliation drift detected: %d positions, %d orders",
                len(report.position_drifts), len(report.order_drifts),
            )
            if self.on_drift is not None:
                with _suppress_callback_errors():
                    self.on_drift(report)
        return report


import contextlib

@contextlib.contextmanager
def _suppress_callback_errors():
    """Callbacks are user-supplied — never let one of them break the engine
    loop. Log and continue."""
    try:
        yield
    except Exception:
        logger.exception("engine: callback raised; engine continues")
