"""
RiskManager — authoritative over strategy.

Enforces CEO-ratified policies (decisions/2026-04-25-trading-policies-ml-and-capital.md):
  - $1,080 capital baseline across paper / sim / live-disabled / live
  - Deterministic-only signal path (no ML influencing risk decisions)

And the risk gates from the technical architecture doc:
  - NLV floor at $300 (above EdgeClear's $200 micro-product auto-liquidation)
  - Kill switch at -30% account drawdown
  - Max 1.5% per-trade risk on NLV
  - Max 40% margin utilization on NLV
  - Max open position count

Strategy generates TradeIntents. evaluate() returns a RiskVerdict. Only
verdicts with `approved=True` should reach the executor.
"""
from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field, replace
from typing import Optional

from .intent import TradeIntent


# ── Policy ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RiskPolicy:
    """CEO-ratified policy thresholds. Source of truth: config/risk.json."""

    starting_nlv: float = 1080.0
    nlv_floor: float = 300.0
    kill_switch_drawdown_pct: float = 0.30
    max_risk_per_trade_pct_nlv: float = 0.015
    max_margin_utilization_pct: float = 0.40
    max_open_positions: int = 3
    require_stop_loss: bool = True

    @classmethod
    def from_json(cls, path: str | pathlib.Path) -> "RiskPolicy":
        data = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        # Ignore unknown keys so config can carry extra info without breaking
        # construction. Known keys override dataclass defaults.
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in known})


# ── Account state ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class AccountState:
    """Current account snapshot. Reconciliation worker keeps this fresh
    against EdgeClear/Dorman ground truth."""

    nlv: float                       # current net liquidation value
    starting_nlv: float              # baseline for drawdown calc
    open_positions_margin: float     # $ of margin used across open positions
    open_position_count: int

    @property
    def drawdown_pct(self) -> float:
        if self.starting_nlv <= 0:
            return 0.0
        if self.nlv >= self.starting_nlv:
            return 0.0
        return (self.starting_nlv - self.nlv) / self.starting_nlv


# ── Verdict ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RiskVerdict:
    """Outcome of a risk evaluation. Approved intents reach the executor;
    rejected intents are logged and dropped."""

    approved: bool
    reasons: tuple[str, ...] = ()
    risk_amount: Optional[float] = None      # $ at risk on this trade
    risk_pct_nlv: Optional[float] = None     # risk_amount / nlv
    margin_required: Optional[float] = None  # $ margin this trade would consume

    @classmethod
    def approve(
        cls,
        *,
        risk_amount: float,
        risk_pct_nlv: float,
        margin_required: float,
    ) -> "RiskVerdict":
        return cls(
            approved=True,
            risk_amount=risk_amount,
            risk_pct_nlv=risk_pct_nlv,
            margin_required=margin_required,
        )

    @classmethod
    def reject(cls, *reasons: str) -> "RiskVerdict":
        return cls(approved=False, reasons=tuple(reasons))


# ── Manager ────────────────────────────────────────────────────────────────
class RiskManager:
    """Evaluate a TradeIntent against current account state + policy.

    All gates evaluate independently and accumulate rejection reasons; the
    verdict reports every gate that fired, not just the first. This makes
    rejected intents diagnosable in one log line."""

    def __init__(self, policy: Optional[RiskPolicy] = None) -> None:
        self.policy = policy or RiskPolicy()

    def evaluate(self, intent: TradeIntent, account: AccountState) -> RiskVerdict:
        reasons: list[str] = []

        # ── Gate 1: Kill switch (account-wide drawdown) ──────────────────
        if account.drawdown_pct >= self.policy.kill_switch_drawdown_pct:
            reasons.append(
                f"kill_switch: drawdown {account.drawdown_pct:.1%} "
                f">= limit {self.policy.kill_switch_drawdown_pct:.1%}"
            )

        # ── Gate 2: NLV floor ────────────────────────────────────────────
        if account.nlv < self.policy.nlv_floor:
            reasons.append(
                f"nlv_floor: NLV ${account.nlv:.2f} "
                f"< floor ${self.policy.nlv_floor:.2f}"
            )

        # ── Gate 3: Open position count ──────────────────────────────────
        if account.open_position_count >= self.policy.max_open_positions:
            reasons.append(
                f"max_positions: {account.open_position_count} open "
                f">= limit {self.policy.max_open_positions}"
            )

        # ── Gate 4: Margin utilization ───────────────────────────────────
        margin_required = intent.per_contract_margin * intent.quantity
        margin_after = account.open_positions_margin + margin_required
        margin_pct_after = margin_after / account.nlv if account.nlv > 0 else float("inf")
        if margin_pct_after > self.policy.max_margin_utilization_pct:
            reasons.append(
                f"margin: post-trade utilization {margin_pct_after:.1%} "
                f"> limit {self.policy.max_margin_utilization_pct:.1%}"
            )

        # ── Gate 5: Per-trade risk (requires stop loss) ──────────────────
        # Total per-trade risk = gross price-move risk + round-trip commission.
        # The gate evaluates TOTAL cost vs the policy cap, not just slippage —
        # otherwise the bot systematically over-approves on small-stop trades
        # where commissions are a meaningful fraction of risk. Per CEO+Gemini
        # decision (2026-04-27): commission stays at 0 for sim mode (matching
        # Sierra Trade Sim's zero-commission fills) and switches to real
        # EdgeClear rate ($1.38/RT for MES) at live transition, keeping local
        # vs broker NLV reconciliation clean.
        risk_amount: Optional[float] = None
        risk_pct: Optional[float] = None
        entry_price = intent.price if intent.price > 0 else None
        commission_total = intent.round_trip_commission * intent.quantity

        if intent.stop_loss is None:
            if self.policy.require_stop_loss:
                reasons.append(
                    "per_trade_risk: stop_loss is required by policy "
                    "but intent has none"
                )
        elif entry_price is None:
            reasons.append(
                "per_trade_risk: cannot compute risk without entry price "
                "(market orders need a reference price in intent.price)"
            )
        else:
            ticks_to_stop = abs(entry_price - intent.stop_loss) / intent.tick_size
            gross_risk = ticks_to_stop * intent.tick_value * intent.quantity
            risk_amount = gross_risk + commission_total
            risk_pct = risk_amount / account.nlv if account.nlv > 0 else float("inf")
            if risk_pct > self.policy.max_risk_per_trade_pct_nlv:
                reasons.append(
                    f"per_trade_risk: ${risk_amount:.2f} "
                    f"(${gross_risk:.2f} slippage + ${commission_total:.2f} fees) "
                    f"= {risk_pct:.2%} > limit "
                    f"{self.policy.max_risk_per_trade_pct_nlv:.2%}"
                )

        if reasons:
            return RiskVerdict.reject(*reasons)
        return RiskVerdict.approve(
            risk_amount=risk_amount or 0.0,
            risk_pct_nlv=risk_pct or 0.0,
            margin_required=margin_required,
        )

    def with_policy(self, **overrides) -> "RiskManager":
        """Return a new RiskManager with policy fields overridden. Useful
        for tests and what-if analysis."""
        return RiskManager(replace(self.policy, **overrides))
