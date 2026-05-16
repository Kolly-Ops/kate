"""
ManagedFuturesEngine — Direction A.

Drives the spine: candles → strategy → intent → risk → execution → state,
with broker-event handling and periodic reconciliation.

Lifecycle:
    engine = ManagedFuturesEngine(...)
    await engine.start()        # connect adapter, logon, seed state + history
    await engine.run()           # main loop until stop()
    await engine.stop()

Per CLAUDE.md and architecture doc, the engine is single-threaded asyncio.
There is one engine per direction (A here, C later). It loops at
`tick_interval_seconds`, draining BrokerEvents and polling candles.
Strategy evaluation runs on candle close. Reconciliation runs every
`reconciliation_interval_seconds`.

Broker abstraction
------------------
Engine depends on the BrokerAdapter ABC, not any concrete broker. The
adapter pumps normalized BrokerEvents into a local asyncio.Queue via a
background task; the main loop drains the queue with a tight timeout
(replaces the prior raw DTC `recv_event()` poll). This means swapping
broker (DTC → Rithmic → IBKR → etc.) is a constructor change, not an
engine rewrite.

Drift handling: when the Reconciler reports drift between local StateStore
and broker POSITION_UPDATE snapshots, the engine emits a callback (or logs)
but does NOT auto-correct. Position correction crosses the CEO approval
gate per CLAUDE.md.
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from trading_bot.core.data import Candle, CandleManager
from trading_bot.core.data.tick_candle_aggregator import TickCandleAggregator
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    AccountBalanceEvent,
    BrokerAdapter,
    BrokerError,
    BrokerEvent,
    BrokerEventKind,
    MarketDataTick,
    OrderEvent,
    PositionEvent,
)
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
    when the entry's FILLED event arrives to spawn the exit legs.

    `entry_side` is the entry-order side; close orders use the opposite.
    `symbol` is the logical symbol — the adapter translates to broker form.
    """
    entry_coid: str
    symbol: str              # logical (e.g. "MESM26"); adapter translates
    exchange: str
    entry_side: int          # 1=BUY, 2=SELL — exits flip this
    quantity: float
    stop_loss: float
    take_profit: Optional[float]


@dataclass(frozen=True)
class InstrumentMeta:
    """Per-instrument calibration. Sourced from config/instruments.json.

    `symbol`, `scid_filename`, and `dtc_symbol` capture distinct identifiers
    that may differ in real Sierra+DTC installs:

      symbol         — logical/display name. Used for strategy ctx,
                       TradeIntent, StateStore, log lines. Stable, short.
                       Example: "MESM26".
      scid_filename  — name of the .scid file Sierra writes ticks to,
                       WITHOUT the .scid extension. Used by CandleManager
                       to find the file. Example: "MESM26_FUT_CME".
      dtc_symbol     — Sierra's wire form. Used by the supervisor when
                       constructing a DTCBrokerAdapter's symbol_map.
                       Engine no longer touches this directly — the
                       broker adapter holds the logical→broker map.
                       Example: "MESM26-CME".

    Phase A integration tests passed because the test fixtures used the
    same string for all three. In production they MUST be set
    independently — supervisor maps logical → scid for CandleManager
    and logical → broker_symbol for the adapter via BrokerSymbolSpec.
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
        broker: BrokerAdapter,
        trade_account: str,
        no_trade_windows_utc: Optional[list[tuple[dt.time, dt.time]]] = None,
        client_name: str = "TRADING_BOT",
        trade_mode: int = proto.TRADE_MODE_DEMO,
        use_broker_market_data: bool = False,
        use_native_brackets: bool = False,
        history_size: Optional[int] = None,
        tick_interval_seconds: float = 1.0,
        reconciliation_interval_seconds: float = 30.0,
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
        self.broker = broker
        self.trade_account = trade_account
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
        self.use_broker_market_data = use_broker_market_data
        self.use_native_brackets = use_native_brackets
        self.tick_interval_seconds = tick_interval_seconds
        self.reconciliation_interval_seconds = reconciliation_interval_seconds
        self.on_drift = on_drift
        self.on_intent_rejected = on_intent_rejected

        self._history_size = history_size or max(2 * strategy.history_window, 200)
        self._history: dict[str, deque[Candle]] = {
            s: deque(maxlen=self._history_size) for s in symbols
        }
        timeframe_minutes = getattr(candle_manager, "timeframe_minutes", 1)
        self._tick_aggregator = TickCandleAggregator(
            timeframe_minutes=timeframe_minutes
        )
        # Latest broker-side position snapshot per (logical symbol, exchange).
        self._broker_positions: dict[tuple[str, str], RemotePosition] = {}
        # Latest broker-side order status per client_order_id
        self._broker_orders: dict[str, RemoteOrder] = {}
        # Latest known account state (fed from ACCOUNT_BALANCE_UPDATE)
        self._account_state: Optional[AccountState] = None
        self._starting_nlv: Optional[float] = None
        self._running = False

        # ── Broker event plumbing ─────────────────────────────────────
        # Background pump task consumes `broker.events()` and forwards
        # each BrokerEvent onto this queue. Main loop drains the queue
        # with a tight timeout (same shape as the prior raw-DTC drain).
        self._event_queue: asyncio.Queue[BrokerEvent] = asyncio.Queue()
        self._event_pump_task: Optional[asyncio.Task[None]] = None

        # ── Bracket-order tracking (synthetic OCO) ────────────────────
        # `_pending_brackets[entry_coid]` holds the stop/target params we'll
        # submit once we see ORDER_FILLED for the entry. Submitting brackets
        # only after entry-fill avoids the brief window where the broker has
        # the stop/target sitting against a position that doesn't yet exist
        # (some brokers reject this; doing it sequentially is safer than
        # relying on native bracket support whose wire format varies).
        # NOTE: brokers that DO support native brackets (Rithmic, IBKR)
        # can short-circuit this by attaching stop_price/target_price on the
        # entry submit. The DTC adapter ignores those and we use this flow.
        self._pending_brackets: dict[str, "_BracketSpec"] = {}
        # `_sibling_orders` maps each exit-leg coid to its sibling's coid.
        # When stop fills, cancel target. When target fills, cancel stop.
        # Both directions populated when brackets are submitted; entries
        # cleared once a sibling cancel has been issued.
        self._sibling_orders: dict[str, str] = {}

        # Missing-data canary. If no candles arrive for a symbol within
        # 60s of the engine running, log a single WARNING. Wiltshire
        # 2026-05-06/07 ate 26 hours of silent heartbeats because the .scid
        # filename convention differed between rigs and CandleManager
        # silently polled a file that didn't exist. The supervisor now
        # resolves the filename at startup, but this canary catches any
        # other path where Sierra stops writing bars (chart closed,
        # subscription expired, file permissions, etc.).
        self._first_run_time: Optional[float] = None
        self._missing_data_warned: dict[str, bool] = {s: False for s in symbols}

        # Sim-mode NLV fallback: log the fallback once per engine run.
        # See _handle_account_balance_event for rationale.
        self._sim_nlv_fallback_logged: bool = False

    # ── Public lifecycle ──────────────────────────────────────────────────
    async def start(self) -> None:
        """Warm candle history, then connect broker + seed initial state.

        Order matters: the scid backfill is synchronous file I/O that can
        block the asyncio event loop for tens of seconds on multi-GB
        history files (the KATE-derived scid_parser reads the last 1 GB
        of ticks on startup — ~50 s observed on Contabo Win VPS for
        MESM26 on 2026-04-28). If the broker is connected before that
        load finishes, Sierra disconnects us for unacknowledged
        heartbeats and the seed phase never runs.

        Warming candles BEFORE broker.connect() means the broker only
        sees us once the loop is responsive. TODO: replace this
        load-everything approach with a bounded warmup (only need ATR
        + breakout lookback worth of candles, not 43k) — see
        `omni/proposals/2026-04-28-claude-kate-scid-warmup-bounded.md`.
        """
        if not self.use_broker_market_data:
            for symbol in self.symbols:
                meta = self.instruments[symbol]
                backfill = self.candle_manager.backfill(
                    meta.scid_filename, max_candles=self._history_size
                )
                self._history[symbol].extend(backfill)
                # Initialize tail baseline (first poll returns [] but baselines
                # the file position so subsequent polls see only new ticks).
                self.candle_manager.poll(meta.scid_filename)

        await self.broker.connect()
        if self.use_broker_market_data:
            for symbol in self.symbols:
                meta = self.instruments[symbol]
                await self.broker.subscribe_market_data(
                    symbol=symbol, exchange=meta.exchange,
                )
        # Spawn the background pump BEFORE logon: some adapters emit
        # LOGON_OK on the event stream, and we want it captured.
        self._event_pump_task = asyncio.create_task(
            self._event_pump(), name="engine-event-pump"
        )
        await self.broker.logon(
            client_name=self.client_name,
            trade_account=self.trade_account,
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
            await self._drain_broker_events(timeout=0.001)

            for symbol in self.symbols:
                await self._process_symbol(symbol)

            now = loop.time()
            if now - last_reconciliation >= self.reconciliation_interval_seconds:
                self._run_reconciliation()
                last_reconciliation = now

            await asyncio.sleep(self.tick_interval_seconds)

    async def stop(self) -> None:
        self._running = False
        if self._event_pump_task is not None:
            self._event_pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._event_pump_task
            self._event_pump_task = None
        await self.broker.disconnect()

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
        """Call the broker adapter's three seed methods and hydrate
        engine state from the typed results. Each method returns
        synchronously; failures raise BrokerError which propagates up
        and prevents the engine from going active against unknown
        broker state.
        """
        # Account state — REQUIRED. The adapter's contract is that this
        # method either returns a valid AccountBalanceEvent or raises.
        # We then apply the same sim-mode NLV fallback logic the engine
        # has always had (see _hydrate_account_balance).
        try:
            balance = await self.broker.request_account_state(
                trade_account=self.trade_account,
            )
            self._hydrate_account_balance(balance)
        except BrokerError:
            logger.exception(
                "engine: broker.request_account_state failed; engine cannot "
                "start without account state seed"
            )
            raise

        # Positions — seed broker_positions snapshot from whatever the
        # broker reports. Empty tuple means flat.
        try:
            positions = await self.broker.request_positions(
                trade_account=self.trade_account,
            )
        except BrokerError:
            logger.exception(
                "engine: broker.request_positions failed; proceeding with "
                "empty broker_positions (reconciler will catch any gap)"
            )
            positions = ()
        for pos in positions:
            meta = self.instruments.get(pos.symbol)
            exchange = meta.exchange if meta is not None else ""
            self._broker_positions[(pos.symbol, exchange)] = RemotePosition(
                symbol=pos.symbol, exchange=exchange, quantity=pos.quantity,
            )

        # Open orders — seed broker_orders snapshot. Returned OrderEvents
        # are by definition WORKING (otherwise they wouldn't be in the
        # broker's open-order list).
        try:
            open_orders = await self.broker.request_open_orders(
                trade_account=self.trade_account,
            )
        except BrokerError:
            logger.exception(
                "engine: broker.request_open_orders failed; proceeding with "
                "empty broker_orders (reconciler will catch any gap)"
            )
            open_orders = ()
        for order in open_orders:
            self._broker_orders[order.client_order_id] = RemoteOrder(
                client_order_id=order.client_order_id, status="WORKING",
            )

    # ── Broker-event consumption ──────────────────────────────────────────
    async def _event_pump(self) -> None:
        """Background task: consume broker.events() onto the local queue.

        Runs from start() until stop() cancels it. The adapter's events()
        iterator terminates on disconnect; if it raises, we log and exit
        — main loop's _drain will just see no events thereafter (and
        the reconciler will surface any state staleness).
        """
        try:
            async for event in self.broker.events():
                await self._event_queue.put(event)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception(
                "engine: broker event-pump exited unexpectedly; engine will "
                "stop receiving broker events until restart"
            )

    async def _drain_broker_events(self, *, timeout: float) -> int:
        """Pull and dispatch all queued BrokerEvents. Returns count."""
        count = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                remaining = 0.0001
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(), timeout=remaining
                )
            except asyncio.TimeoutError:
                return count
            await self._handle_broker_event(event)
            count += 1

    async def _handle_broker_event(self, event: BrokerEvent) -> None:
        """Dispatch a normalized broker event to the right handler."""
        try:
            if event.kind in (
                BrokerEventKind.ORDER_FILLED,
                BrokerEventKind.ORDER_PARTIAL_FILL,
                BrokerEventKind.ORDER_REJECTED,
                BrokerEventKind.ORDER_CANCELED,
                BrokerEventKind.ORDER_ACK,
            ):
                if event.order is not None:
                    await self._handle_order_event(event.kind, event.order)
            elif event.kind == BrokerEventKind.POSITION_UPDATE:
                if event.position is not None:
                    self._handle_position_event(event.position)
            elif event.kind == BrokerEventKind.ACCOUNT_BALANCE_UPDATE:
                if event.balance is not None:
                    self._handle_account_balance_event(event.balance)
            elif event.kind == BrokerEventKind.MARKET_DATA_TICK:
                if event.tick is not None:
                    await self._handle_market_data_tick(event.tick)
            elif event.kind in (
                BrokerEventKind.CONNECTED,
                BrokerEventKind.LOGON_OK,
                BrokerEventKind.HEARTBEAT,
                BrokerEventKind.DISCONNECTED,
                BrokerEventKind.ERROR,
            ):
                # Lifecycle/info events — logged at adapter level, not
                # acted on by engine logic. (DISCONNECTED could trigger
                # supervisor logic in future; for now main loop will
                # naturally observe missing events.)
                logger.info(
                    "engine: broker event %s (no engine action)", event.kind.value,
                )
            else:
                logger.warning("engine: unhandled broker event kind %r", event.kind)
        except Exception:
            logger.exception("engine: failed to handle broker event %r", event.kind)

    async def _handle_order_event(
        self, kind: BrokerEventKind, order: OrderEvent,
    ) -> None:
        # Map BrokerEventKind back to StateStore status string.
        store_status = _broker_event_kind_to_state_store_status(kind)
        logger.info(
            "engine: order event coid=%s kind=%s store_status=%s "
            "fill_price=%s fill_qty=%s rejected=%s",
            order.client_order_id, kind.value, store_status,
            order.fill_price, order.fill_quantity, order.rejected_reason,
        )
        # Update remote-orders snapshot
        self._broker_orders[order.client_order_id] = RemoteOrder(
            client_order_id=order.client_order_id, status=store_status,
        )
        # Update StateStore — only if this coid is one we submitted.
        existing = self.state.get_order(client_order_id=order.client_order_id)
        if existing is None:
            logger.info(
                "engine: order event for unknown coid=%s — not in local "
                "StateStore, broker_orders updated only",
                order.client_order_id,
            )
            return
        rejected_reason = order.rejected_reason if store_status == "REJECTED" else None
        fill_price = order.fill_price if store_status == "FILLED" else None
        fill_qty = order.fill_quantity if store_status == "FILLED" else None
        self.state.update_order_status(
            client_order_id=order.client_order_id,
            status=store_status,
            fill_price=fill_price,
            fill_quantity=fill_qty,
            rejected_reason=rejected_reason,
        )

        # ── Bracket-order side effects on FILL ────────────────────────
        if store_status == "FILLED":
            # 1. ENTRY for which we cached a bracket spec → submit exits
            bracket = self._pending_brackets.pop(order.client_order_id, None)
            if bracket is not None:
                await self._submit_exit_brackets(bracket)
            # 2. EXIT leg → cancel sibling
            sibling = self._sibling_orders.pop(order.client_order_id, None)
            if sibling is not None:
                self._sibling_orders.pop(sibling, None)
                await self._cancel_sibling_after_fill(
                    filled_coid=order.client_order_id, sibling_coid=sibling,
                )

    def _handle_position_event(self, position: PositionEvent) -> None:
        meta = self.instruments.get(position.symbol)
        if meta is None:
            logger.debug(
                "engine: POSITION_UPDATE for unmanaged symbol %r — ignored",
                position.symbol,
            )
            return
        key = (position.symbol, meta.exchange)
        if position.quantity == 0:
            self._broker_positions.pop(key, None)
        else:
            self._broker_positions[key] = RemotePosition(
                symbol=position.symbol, exchange=meta.exchange,
                quantity=position.quantity,
            )

    def _handle_account_balance_event(self, balance: AccountBalanceEvent) -> None:
        """Apply sim-mode NLV fallback then update local AccountState.

        Sierra Chart sim mode does NOT report synthetic NLV via DTC
        (confirmed by SC support 2026-05-09: "Simulation accounts do
        not show in the Trade Account Manager. That window only shows
        information for live accounts."). When sim mode reports
        NLV<=0, we fall back to the risk policy's configured
        starting_nlv so the risk gates have a meaningful denominator.
        In live mode we trust the broker's NLV unconditionally — a
        zero there is a real signal, not an artefact of sim mode.

        Note: the adapter ABC carries no sim/live flag on the event —
        we infer from `self.trade_mode`. Other adapters (Rithmic, IBKR)
        don't have the Sim1 NLV-blindness issue but the fallback
        gracefully degrades there too (they wouldn't report NLV<=0
        unless the account is actually broken).
        """
        self._hydrate_account_balance(balance)

    def _hydrate_account_balance(self, balance: AccountBalanceEvent) -> None:
        raw_nlv = balance.nlv
        if self.trade_mode == proto.TRADE_MODE_DEMO and raw_nlv <= 0:
            nlv = self.risk.policy.starting_nlv
            if not self._sim_nlv_fallback_logged:
                logger.warning(
                    "engine: sim mode reports NLV=$%.2f via broker (Sierra "
                    "Chart by-design — sim accounts have no DTC-exposed "
                    "balance); falling back to risk_policy.starting_nlv="
                    "$%.2f for risk gates. Drawdown tracking is approximate "
                    "in sim mode.",
                    raw_nlv, self.risk.policy.starting_nlv,
                )
                self._sim_nlv_fallback_logged = True
        else:
            nlv = raw_nlv
        if self._starting_nlv is None:
            self._starting_nlv = nlv
        self._account_state = AccountState(
            nlv=nlv,
            starting_nlv=self._starting_nlv,
            open_positions_margin=balance.margin_requirement,
            open_position_count=len(self._broker_positions),
        )
        self.state.record_account_snapshot(
            nlv=nlv, drawdown_pct=self._account_state.drawdown_pct,
        )

    # ── Per-tick processing ───────────────────────────────────────────────
    async def _handle_market_data_tick(self, tick: MarketDataTick) -> None:
        if tick.symbol not in self._history:
            logger.debug(
                "engine: MARKET_DATA_TICK for unmanaged symbol %r ignored",
                tick.symbol,
            )
            return
        closed = self._tick_aggregator.ingest_tick(
            symbol=tick.symbol,
            timestamp=tick.timestamp,
            price=tick.last_price,
            size=tick.last_size,
        )
        for candle in closed:
            self._history[tick.symbol].append(candle)
            await self._on_candle_close(tick.symbol, candle)

    async def _process_symbol(self, symbol: str) -> None:
        if self.use_broker_market_data:
            return
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
            symbol=intent.symbol,
            exchange=intent.exchange,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
        )
        try:
            await self.broker.submit_order(
                client_order_id=intent.intent_id,
                symbol=intent.symbol,        # logical — adapter translates
                exchange=intent.exchange,
                side=intent.side,
                quantity=intent.quantity,
                order_type=intent.order_type,
                price=intent.price,
                stop_price=intent.stop_loss if self.use_native_brackets else None,
                target_price=intent.take_profit if self.use_native_brackets else None,
                free_form_text=intent.strategy_name[:48],
            )
            logger.info(
                "submitted order %s for %s",
                intent.intent_id, intent.symbol,
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
        # the entry's FILLED event arrives (see _handle_order_event).
        # Risk policy already gates this — entries without stop_loss are
        # rejected before reaching here when require_stop_loss=True.
        if intent.stop_loss is not None and not self.use_native_brackets:
            self._pending_brackets[intent.intent_id] = _BracketSpec(
                entry_coid=intent.intent_id,
                symbol=intent.symbol,
                exchange=intent.exchange,
                entry_side=intent.side,
                quantity=intent.quantity,
                stop_loss=intent.stop_loss,
                take_profit=intent.take_profit,
            )

    async def _submit_exit_brackets(self, bracket: _BracketSpec) -> None:
        """Submit stop and (optional) take-profit close orders after the
        entry fills. The two legs are mutually-cancelling: when one fills
        we cancel the other via _handle_order_event's sibling logic.

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
        self.state.record_order(
            client_order_id=stop_coid,
            symbol=bracket.symbol, exchange=bracket.exchange,
            side=close_side, quantity=bracket.quantity,
            order_type=proto.ORDER_TYPE_STOP,
        )
        try:
            await self.broker.submit_order(
                client_order_id=stop_coid,
                symbol=bracket.symbol,
                exchange=bracket.exchange,
                side=close_side,
                quantity=bracket.quantity,
                order_type=proto.ORDER_TYPE_STOP,
                price=bracket.stop_loss,
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
                symbol=bracket.symbol, exchange=bracket.exchange,
                side=close_side, quantity=bracket.quantity,
                order_type=proto.ORDER_TYPE_LIMIT,
            )
            try:
                await self.broker.submit_order(
                    client_order_id=target_coid,
                    symbol=bracket.symbol,
                    exchange=bracket.exchange,
                    side=close_side,
                    quantity=bracket.quantity,
                    order_type=proto.ORDER_TYPE_LIMIT,
                    price=bracket.take_profit,
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
        """Issue a cancel for the sibling exit leg after one side fills.
        Broker responds with an ORDER_CANCELED event for the sibling,
        which `_handle_order_event` writes through to StateStore via the
        normal flow."""
        try:
            await self.broker.cancel_order(client_order_id=sibling_coid)
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


# ── Helpers ───────────────────────────────────────────────────────────────

def _broker_event_kind_to_state_store_status(kind: BrokerEventKind) -> str:
    """Map normalized BrokerEventKind back to the StateStore status string
    convention. ORDER_ACK collapses to WORKING (the order is live on the
    broker side); fills/rejects/cancels map directly."""
    if kind == BrokerEventKind.ORDER_FILLED:
        return "FILLED"
    if kind == BrokerEventKind.ORDER_PARTIAL_FILL:
        return "WORKING"
    if kind == BrokerEventKind.ORDER_REJECTED:
        return "REJECTED"
    if kind == BrokerEventKind.ORDER_CANCELED:
        return "CANCELLED"
    # ORDER_ACK or anything else
    return "WORKING"


@contextlib.contextmanager
def _suppress_callback_errors():
    """Callbacks are user-supplied — never let one of them break the engine
    loop. Log and continue."""
    try:
        yield
    except Exception:
        logger.exception("engine: callback raised; engine continues")
