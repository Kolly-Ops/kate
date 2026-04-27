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

    # ── Public lifecycle ──────────────────────────────────────────────────
    async def start(self) -> None:
        """Connect DTC, log on, seed candle history + initial broker state."""
        await self.dtc.connect()
        await self.dtc.logon(
            client_name=self.client_name,
            trade_mode=self.trade_mode,
        )

        for symbol in self.symbols:
            meta = self.instruments[symbol]
            backfill = self.candle_manager.backfill(
                meta.scid_filename, max_candles=self._history_size
            )
            self._history[symbol].extend(backfill)
            # Initialize tail baseline (first poll returns [] but baselines
            # the file position so subsequent polls see only new ticks).
            self.candle_manager.poll(meta.scid_filename)

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
        self.dtc._writer.write(   # noqa: SLF001 — direct send is fine here
            proto.pack_account_balance_request(trade_account=self.trade_account)
        )
        self.dtc._writer.write(   # noqa: SLF001
            proto.pack_current_positions_request(trade_account=self.trade_account)
        )
        self.dtc._writer.write(   # noqa: SLF001
            proto.pack_open_orders_request(trade_account=self.trade_account)
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
            self._handle_dtc_message(msg)
            count += 1

    def _handle_dtc_message(self, msg: DTCMessage) -> None:
        try:
            if msg.msg_type == proto.ORDER_UPDATE:
                self._handle_order_update(proto.unpack_order_update(msg.body))
            elif msg.msg_type == proto.POSITION_UPDATE:
                self._handle_position_update(proto.unpack_position_update(msg.body))
            elif msg.msg_type == proto.ACCOUNT_BALANCE_UPDATE:
                self._handle_account_balance_update(
                    proto.unpack_account_balance_update(msg.body)
                )
            elif msg.msg_type == proto.HEARTBEAT:
                pass   # background heartbeat task handles outbound
            else:
                logger.debug("engine: unhandled msg_type %d", msg.msg_type)
        except Exception:
            logger.exception("engine: failed to handle msg_type %d", msg.msg_type)

    def _handle_order_update(self, msg: proto.OrderUpdate) -> None:
        store_status = proto.dtc_order_status_to_state_store(msg.order_status)
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

    async def _on_candle_close(self, symbol: str, candle: Candle) -> None:
        if self._account_state is None:
            logger.debug("engine: no account state yet, skipping strategy on %s", symbol)
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
                trade_account=self.trade_account,
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

    def _has_open_position(self, symbol: str, exchange: str) -> bool:
        pos = self._broker_positions.get((symbol, exchange))
        return pos is not None and pos.quantity != 0

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
