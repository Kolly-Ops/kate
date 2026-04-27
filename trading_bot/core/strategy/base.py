"""
Strategy base — the contract every concrete strategy implements.

Strategies are deterministic by policy (CEO ratification 2026-04-25). They
take a StrategyContext (latest closed candle + recent history + instrument
metadata) and return either None (no action) or a TradeIntent (proposal).

The risk engine decides whether the intent reaches the executor — strategies
do NOT make execution decisions. This separation is the structural fix for
KATE's "strategy directly opens positions" pattern.

Stateless-by-contract: strategies should not store mutable state across
calls. Anything persistent lives in StateStore. This makes restart-safe
behavior trivial — re-load history, re-evaluate, the bot is in the same
place it was before the crash.
"""
from __future__ import annotations

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
