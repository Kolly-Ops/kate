"""
Strategy base — the contract every concrete strategy implements.

Strategies are deterministic by policy (CEO ratification 2026-04-25). They
take a StrategyContext (latest closed candle + recent history + instrument
metadata) and return either None (no action) or a TradeIntent (proposal).

The risk engine decides whether the intent reaches the executor — strategies
do NOT make execution decisions. This separation is the structural fix for
KATE's "strategy directly opens positions" pattern.

Stateless-by-contract (with one narrow exception, see below): strategies
should not store mutable state across calls. Anything persistent lives in
StateStore. This makes restart-safe behavior trivial — re-load history,
re-evaluate, the bot is in the same place it was before the crash.

Session-aware exception (Sprint 2 #44, 2026-05-30): strategies that
implement once-per-(symbol, session-date) semantics MAY hold an in-memory
set of traded sessions, but ONLY mutated via the engine-driven
mark_session_traded() callback. This preserves the "one source of truth"
discipline — sessions are marked when a broker accepts (fills) an entry,
NOT when the strategy emits an intent. Risk-rejected and fast-fill-fast-
close paths both behave correctly under this rule.
"""
from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from trading_bot.core.data import Candle
from trading_bot.core.risk import TradeIntent


@dataclass(frozen=True)
class StrategyContext:
    """Read-only snapshot passed to a strategy on each candle-close event."""

    symbol: str
    exchange: str
    candle: Candle              # the just-closed candle
    history: tuple[Candle, ...] # most recent N candles, oldest-first; INCLUDES `candle`
    tick_size: float
    tick_value: float
    per_contract_margin: float
    has_open_position: bool
    round_trip_commission: float = 0.0   # per-contract round-trip; baked into risk math (default 0 = Sierra Sim)


class Strategy(ABC):
    """Abstract base class for deterministic trading strategies."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable identifier including parameters. Used in
        intent_id, log lines, audit trail."""

    @property
    @abstractmethod
    def history_window(self) -> int:
        """Minimum number of historical candles this strategy needs to
        evaluate. The engine ensures `len(ctx.history) >= history_window`
        before invoking on_candle_close."""

    @abstractmethod
    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        """Called once per closed candle. Returns a TradeIntent or None.

        Strategies SHOULD set stop_loss on every entry intent — without it,
        the risk engine will reject (per CEO policy require_stop_loss=True).
        """

    def mark_session_traded(self, symbol: str, timestamp_utc: dt.datetime) -> None:
        """Engine callback fired when an entry order for this strategy is
        accepted by the broker (ORDER_FILLED or ORDER_PARTIAL_FILL).

        Sprint 2 #44 (2026-05-30). Default implementation is a no-op so
        non-session-aware strategies aren't forced to implement. Strategies
        that track once-per-(symbol, session-date) semantics override to
        record the consumed session.

        The engine passes UTC timestamp; the strategy converts to its own
        local timezone if needed (e.g. FXLondonBreakoutStrategy converts
        to UK time for session-date derivation).

        Idempotence: implementations should be safe under duplicate calls
        (the engine pops the entry marker after first emission, but
        belt-and-braces is cheap).

        Risk-rejected intents NEVER reach this callback — risk-gate
        rejections happen before broker submission. Risk-rejected paths
        thus do not consume the session, allowing the strategy to
        re-attempt on the next candle.
        """
        # Default: no-op. Stateful strategies override.
