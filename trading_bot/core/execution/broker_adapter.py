"""
BrokerAdapter — protocol-agnostic interface between Kate's engine and a
broker/data vendor (Sierra Chart DTC, Rithmic, eventually NinjaTrader,
IBKR, etc.).

Why this exists
---------------
Kate Phase 1 hardcoded `DTCClient` into `ManagedFuturesEngine`. That
worked while we only spoke to Sierra Chart, but it locked us into one
vendor's protocol and one vendor's quirks. The 2026-05-09 platform
pivot to Rithmic-direct (decisions/2026-05-09-kate-12-month-strategy-
master-plan-v2.md) requires Kate to talk a different protocol entirely
without rewriting the engine.

This module defines the abstraction that lets us swap brokers/data
vendors as cleanly as swapping a strategy. Concrete implementations:

  * DTCBrokerAdapter   — wraps the existing DTCClient (Sierra Chart)
  * RithmicBrokerAdapter — wraps async_rithmic.RithmicClient (Phase 2+)

(Wrappers live in their own modules. This file only declares the
contract.)

Design principles
-----------------
1. **Normalized events.** The engine works with `BrokerEvent` instances
   (FILLED, POSITION_UPDATE, ACCOUNT_BALANCE_UPDATE) instead of raw
   DTC msg_type integers or Rithmic Protocol Buffer types. Each
   implementation translates from its native protocol into these.
2. **Idempotent lifecycle.** `connect()` and `disconnect()` may be
   called multiple times; implementations must tolerate reconnection.
3. **Explicit account state.** Balance / position state is requested
   via `request_account_state()` rather than scraped from event
   stream. Async-rithmic exposes this naturally; DTC requires a
   light wrapper.
4. **Stateless ABC.** Adapter holds connection state internally;
   engine never reads or mutates internal adapter state directly.
5. **Stop / target ticks vs absolute prices.** The current Kate
   engine submits absolute stop/target prices (computed from ATR).
   Rithmic prefers tick offsets. Adapters convert; the engine
   continues to think in absolute prices.

Migration path
--------------
This ABC ships before any concrete adapter. The existing
ManagedFuturesEngine can continue using DTCClient directly while
Rithmic adapter is built. Once both adapters exist:

  Step 1: write DTCBrokerAdapter wrapping DTCClient (1-day task,
          mechanical translation).
  Step 2: write RithmicBrokerAdapter wrapping async_rithmic
          (Codex's lane, ~3 days once credentials arrive).
  Step 3: refactor engine to depend on BrokerAdapter ABC instead
          of DTCClient. Add CLI flag `--broker dtc|rithmic` to
          select implementation.
  Step 4: paper window runs on RithmicBrokerAdapter. DTC stays
          as fallback / regression-test path.

This module = step 0. Lays the foundation for steps 1-4 without
breaking the running Sierra-Chart engine.
"""
from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator, Optional


# ── Normalized event types (vendor-agnostic) ──────────────────────────────

class BrokerEventKind(Enum):
    """The set of broker-side events Kate's engine reacts to.

    Each adapter translates its native protocol's event types into
    one of these. Unknown events are filtered out by the adapter,
    not surfaced.
    """
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    LOGON_OK = "logon_ok"
    HEARTBEAT = "heartbeat"
    ORDER_ACK = "order_ack"             # broker received the submit
    ORDER_FILLED = "order_filled"       # full fill on a submitted order
    ORDER_PARTIAL_FILL = "order_partial_fill"
    ORDER_REJECTED = "order_rejected"
    ORDER_CANCELED = "order_canceled"
    POSITION_UPDATE = "position_update"
    ACCOUNT_BALANCE_UPDATE = "account_balance_update"
    MARKET_DATA_TICK = "market_data_tick"   # if adapter does ticks
    ERROR = "error"


@dataclass(frozen=True)
class OrderEvent:
    """Per-order payload for ORDER_* event kinds."""
    client_order_id: str
    symbol: str                          # logical symbol (e.g. MESM26)
    side: int                            # 1=BUY, 2=SELL — same convention as proto
    quantity: float                      # contracts
    fill_price: Optional[float] = None
    fill_quantity: Optional[float] = None
    rejected_reason: Optional[str] = None
    server_order_id: Optional[str] = None


@dataclass(frozen=True)
class PositionEvent:
    """Per-symbol payload for POSITION_UPDATE."""
    symbol: str
    quantity: float
    avg_price: float
    side: Optional[int] = None           # may be None when flat


@dataclass(frozen=True)
class AccountBalanceEvent:
    """Account-level state for ACCOUNT_BALANCE_UPDATE.

    Kate's risk engine consumes nlv as the canonical equity figure.
    Margin requirements are passed through for utilisation checks.
    """
    cash: float
    nlv: float                           # net liquidation value
    pnl: float                           # session/realized
    margin_requirement: float = 0.0      # current open-position margin held
    currency: str = "USD"


@dataclass(frozen=True)
class MarketDataTick:
    """Per-symbol tick for MARKET_DATA_TICK (used by Rithmic adapter
    to feed Kate's CandleManager when .scid files aren't available)."""
    symbol: str
    timestamp: dt.datetime
    last_price: float
    last_size: float
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass(frozen=True)
class BrokerEvent:
    """Top-level event the engine receives.

    `kind` selects which payload field is populated. Unused payload
    fields are None. This shape lets the engine pattern-match cleanly
    without adapter-specific type imports.
    """
    kind: BrokerEventKind
    received_at: float                   # epoch seconds; adapter clock
    order: Optional[OrderEvent] = None
    position: Optional[PositionEvent] = None
    balance: Optional[AccountBalanceEvent] = None
    tick: Optional[MarketDataTick] = None
    error_message: Optional[str] = None


# ── The contract ──────────────────────────────────────────────────────────

class BrokerAdapter(ABC):
    """Protocol-agnostic broker / data adapter.

    Implementations: DTCBrokerAdapter (Sierra Chart), RithmicBrokerAdapter
    (Edgeclear via async_rithmic), future TradovateBrokerAdapter,
    NinjaTraderBrokerAdapter, IBKRBrokerAdapter, etc.
    """

    # ── Lifecycle ────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Establish transport connection to the broker.

        Idempotent: callers may call multiple times; implementations
        either return immediately if already connected or re-establish.
        Raises BrokerError on unrecoverable failure.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Tear down connection cleanly. Idempotent."""

    @abstractmethod
    async def logon(
        self,
        *,
        client_name: str,
        trade_account: str,
        username: str = "",
        password: str = "",
        demo: bool = True,
    ) -> None:
        """Authenticate against the broker.

        For DTC this is the LOGON_REQUEST handshake; for Rithmic it's
        the per-plant login flow. Either way, on success the adapter
        is ready to submit_order / subscribe_market_data.
        Raises BrokerError on auth failure.
        """

    # ── Order submission & cancellation ──────────────────────────────────

    @abstractmethod
    async def submit_order(
        self,
        *,
        client_order_id: str,
        symbol: str,
        exchange: str,
        side: int,                       # 1=BUY, 2=SELL (same as proto)
        quantity: float,
        order_type: int,                 # 1=MARKET, 2=LIMIT (same as proto)
        price: float = 0.0,              # absolute price for LIMIT/STOP
        stop_price: Optional[float] = None,
        target_price: Optional[float] = None,
        free_form_text: str = "",
    ) -> str:
        """Submit a single order. Bracket orders (stop+target attached)
        are surfaced through stop_price/target_price; the adapter is
        responsible for translating to its native bracket model
        (Rithmic stop_ticks/target_ticks, DTC submit-then-attach, etc).

        Returns the broker's accepted ClientOrderID (usually equal to
        the input client_order_id; adapters that mangle IDs return
        what they actually used so the caller can track).
        """

    @abstractmethod
    async def cancel_order(
        self,
        *,
        client_order_id: str,
        server_order_id: str = "",
    ) -> None:
        """Cancel a pending order. Acknowledgment arrives as an
        ORDER_CANCELED event on the event stream."""

    # ── Market data (optional) ───────────────────────────────────────────

    async def subscribe_market_data(
        self,
        *,
        symbol: str,
        exchange: str = "",
    ) -> None:
        """Subscribe to live ticks for a symbol. Default implementation
        raises NotImplementedError; adapters that source ticks from
        elsewhere (e.g. the Sierra .scid file path) override only if
        they actually expose the network feed."""
        raise NotImplementedError(
            f"{type(self).__name__} does not provide market data via the "
            f"adapter; the engine should source ticks elsewhere "
            f"(.scid files, separate feed, etc.)"
        )

    # ── State queries ────────────────────────────────────────────────────

    @abstractmethod
    async def request_account_state(
        self,
        *,
        trade_account: str,
    ) -> AccountBalanceEvent:
        """Pull the current account snapshot synchronously. The result
        is also emitted on the event stream as ACCOUNT_BALANCE_UPDATE
        for engine state-sync, but having a synchronous request method
        avoids race conditions on engine startup / reconnect."""

    # ── Event stream ─────────────────────────────────────────────────────

    @abstractmethod
    def events(self) -> AsyncIterator[BrokerEvent]:
        """Async iterator over normalized BrokerEvents. Engine consumes
        this to drive its state machine. The iterator terminates when
        disconnect() is called."""


# ── Errors ───────────────────────────────────────────────────────────────

class BrokerError(Exception):
    """Base class for all adapter-level errors. Wraps protocol-specific
    exceptions (DTCError, RithmicError, etc.) so the engine catches one
    type regardless of which adapter is in play."""
