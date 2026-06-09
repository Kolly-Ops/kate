"""Protective stop-management policies for strategy backtests/live wiring.

These helpers are deliberately broker-agnostic. They decide whether a stop
should advance; the engine/adapter layer owns the actual broker modification.
"""
from __future__ import annotations

from dataclasses import dataclass

from trading_bot.core.execution import dtc_protocol as proto


@dataclass(frozen=True)
class StepRatchetState:
    """Current state of the V2 step-ratchet stop policy.

    `stage` meanings:
      0 = original strategy stop
      1 = breakeven-plus-buffer active
      2 = entry-plus-0.5R active
    """

    stage: int
    stop_price: float


@dataclass(frozen=True)
class StepRatchetDecision:
    """Result of evaluating one completed bar close."""

    state: StepRatchetState
    advanced: bool
    reason: str = ""


class StepRatchetStopPolicy:
    """CEO-approved V2 protective stop policy.

    Stop advances only on completed-bar close milestones:
      - +1R close: move stop to entry +/- one pip
      - +1.5R close: move stop to entry +/- 0.5R

    Wicks do not advance the stop. Stops only move in the protective
    direction and never loosen.
    """

    def __init__(self, *, buffer_pips: float = 1.0) -> None:
        if buffer_pips < 0:
            raise ValueError("buffer_pips must be >= 0")
        self.buffer_pips = float(buffer_pips)

    def initial_state(self, *, initial_stop: float) -> StepRatchetState:
        return StepRatchetState(stage=0, stop_price=float(initial_stop))

    def evaluate_bar_close(
        self,
        *,
        state: StepRatchetState,
        side: int,
        entry_price: float,
        initial_stop: float,
        bar_close: float,
        pip_size: float,
    ) -> StepRatchetDecision:
        if side not in (proto.BUY, proto.SELL):
            raise ValueError("side must be proto.BUY or proto.SELL")
        if pip_size <= 0:
            raise ValueError("pip_size must be > 0")
        risk = abs(float(entry_price) - float(initial_stop))
        if risk <= 0:
            raise ValueError("entry_price and initial_stop must define positive risk")

        close_r = self._profit_r(side=side, entry=entry_price, price=bar_close, risk=risk)
        next_stage = state.stage
        next_stop = state.stop_price
        reason = ""

        if state.stage < 1 and close_r >= 1.0:
            next_stage = 1
            next_stop = self._breakeven_stop(
                side=side,
                entry=entry_price,
                pip_size=pip_size,
            )
            reason = "close>=1R"

        if next_stage == 1 and close_r >= 1.5:
            next_stage = 2
            next_stop = self._half_r_stop(
                side=side,
                entry=entry_price,
                risk=risk,
            )
            reason = "close>=1.5R"

        next_stop = self._protective_only(
            side=side,
            current_stop=state.stop_price,
            proposed_stop=next_stop,
        )
        advanced = next_stage != state.stage or next_stop != state.stop_price
        return StepRatchetDecision(
            state=StepRatchetState(stage=next_stage, stop_price=next_stop),
            advanced=advanced,
            reason=reason if advanced else "",
        )

    @staticmethod
    def _profit_r(*, side: int, entry: float, price: float, risk: float) -> float:
        if side == proto.BUY:
            return (price - entry) / risk
        return (entry - price) / risk

    def _breakeven_stop(self, *, side: int, entry: float, pip_size: float) -> float:
        buffer = self.buffer_pips * pip_size
        if side == proto.BUY:
            return entry + buffer
        return entry - buffer

    @staticmethod
    def _half_r_stop(*, side: int, entry: float, risk: float) -> float:
        if side == proto.BUY:
            return entry + (0.5 * risk)
        return entry - (0.5 * risk)

    @staticmethod
    def _protective_only(*, side: int, current_stop: float, proposed_stop: float) -> float:
        if side == proto.BUY:
            return max(current_stop, proposed_stop)
        return min(current_stop, proposed_stop)
