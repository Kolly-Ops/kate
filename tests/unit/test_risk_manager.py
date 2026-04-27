"""
Unit tests for trading_bot.core.risk — gate-by-gate coverage of the
authoritative risk engine.

Anchored to CEO-ratified policy (decisions/2026-04-25-trading-policies-ml-and-capital.md):
  - $1,080 starting NLV
  - $300 NLV floor
  - -30% kill switch
  - 1.5% per-trade risk cap
  - 40% margin utilization cap
"""
from __future__ import annotations

import pathlib

import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.risk import (
    AccountState,
    RiskManager,
    RiskPolicy,
    RiskVerdict,
    TradeIntent,
)


# ── Fixtures ───────────────────────────────────────────────────────────────
@pytest.fixture
def policy() -> RiskPolicy:
    return RiskPolicy()


@pytest.fixture
def manager(policy: RiskPolicy) -> RiskManager:
    return RiskManager(policy)


@pytest.fixture
def healthy_account() -> AccountState:
    return AccountState(
        nlv=1080.0,
        starting_nlv=1080.0,
        open_positions_margin=0.0,
        open_position_count=0,
    )


def make_intent(
    *,
    quantity: float = 1.0,
    price: float = 5000.0,
    stop_loss: float | None = 4998.0,   # 2pt = 8 ticks = $10 = 0.93% of $1080
    side: int = proto.BUY,
    per_contract_margin: float = 100.0,
    round_trip_commission: float = 0.0,
) -> TradeIntent:
    return TradeIntent(
        intent_id="T-001",
        strategy_name="test_strategy",
        symbol="MESM26",
        exchange="CME",
        side=side,
        quantity=quantity,
        order_type=proto.ORDER_TYPE_LIMIT,
        tick_size=0.25,
        tick_value=1.25,
        price=price,
        stop_loss=stop_loss,
        per_contract_margin=per_contract_margin,
        round_trip_commission=round_trip_commission,
    )


# ── Approval path ──────────────────────────────────────────────────────────
def test_healthy_intent_approves(manager: RiskManager, healthy_account: AccountState) -> None:
    # Default make_intent: 2pt stop = 8 ticks = $10 risk = 0.93% NLV (under 1.5% cap)
    intent = make_intent()
    verdict = manager.evaluate(intent, healthy_account)
    assert verdict.approved, verdict.reasons
    assert verdict.risk_amount == pytest.approx(10.0)
    assert verdict.risk_pct_nlv == pytest.approx(10 / 1080)
    assert verdict.margin_required == pytest.approx(100.0)


# ── Gate 1: Kill switch ────────────────────────────────────────────────────
def test_kill_switch_blocks_at_30pct_drawdown(manager: RiskManager) -> None:
    blown_account = AccountState(
        nlv=756.0,                # exactly -30% from 1080
        starting_nlv=1080.0,
        open_positions_margin=0.0,
        open_position_count=0,
    )
    intent = make_intent(stop_loss=4998.0)
    verdict = manager.evaluate(intent, blown_account)
    assert not verdict.approved
    assert any("kill_switch" in r for r in verdict.reasons)


def test_kill_switch_does_not_fire_at_29pct(manager: RiskManager) -> None:
    near_blown = AccountState(
        nlv=767.0,                # -28.98% from 1080
        starting_nlv=1080.0,
        open_positions_margin=0.0,
        open_position_count=0,
    )
    intent = make_intent(stop_loss=4998.0)
    verdict = manager.evaluate(intent, near_blown)
    assert verdict.approved


# ── Gate 2: NLV floor ──────────────────────────────────────────────────────
def test_nlv_floor_blocks_below_300(manager: RiskManager) -> None:
    below_floor = AccountState(
        nlv=299.99,
        starting_nlv=1080.0,
        open_positions_margin=0.0,
        open_position_count=0,
    )
    intent = make_intent(stop_loss=4999.5)   # 2 ticks = $2.50 risk
    verdict = manager.evaluate(intent, below_floor)
    assert not verdict.approved
    assert any("nlv_floor" in r for r in verdict.reasons)


# ── Gate 3: Max open positions ────────────────────────────────────────────
def test_max_positions_blocks_at_limit(manager: RiskManager) -> None:
    saturated = AccountState(
        nlv=1080.0,
        starting_nlv=1080.0,
        open_positions_margin=0.0,
        open_position_count=3,    # at default policy limit
    )
    intent = make_intent(stop_loss=4998.0)
    verdict = manager.evaluate(intent, saturated)
    assert not verdict.approved
    assert any("max_positions" in r for r in verdict.reasons)


# ── Gate 4: Margin utilization ────────────────────────────────────────────
def test_margin_blocks_when_post_trade_exceeds_40pct(manager: RiskManager) -> None:
    nearly_full = AccountState(
        nlv=1080.0,
        starting_nlv=1080.0,
        open_positions_margin=400.0,   # 37%, leaves 3% headroom
        open_position_count=1,
    )
    intent = make_intent(stop_loss=4998.0, per_contract_margin=100.0)  # +$100 → 46%
    verdict = manager.evaluate(intent, nearly_full)
    assert not verdict.approved
    assert any("margin" in r for r in verdict.reasons)


def test_margin_approves_at_exactly_40pct(manager: RiskManager) -> None:
    intent = make_intent(stop_loss=4998.0, per_contract_margin=432.0)  # 40% of 1080
    fresh = AccountState(
        nlv=1080.0, starting_nlv=1080.0,
        open_positions_margin=0.0, open_position_count=0,
    )
    verdict = manager.evaluate(intent, fresh)
    assert verdict.approved
    assert verdict.margin_required == pytest.approx(432.0)


# ── Gate 5: Per-trade risk ─────────────────────────────────────────────────
def test_per_trade_risk_blocks_above_1_5_pct(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    # Stop 7 points = 28 ticks = $35 risk = 3.24% of $1080 — well over 1.5%
    intent = make_intent(price=5000.0, stop_loss=4993.0)
    verdict = manager.evaluate(intent, healthy_account)
    assert not verdict.approved
    assert any("per_trade_risk" in r for r in verdict.reasons)


def test_per_trade_risk_requires_stop_loss(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    intent = make_intent(stop_loss=None)
    verdict = manager.evaluate(intent, healthy_account)
    assert not verdict.approved
    assert any("stop_loss" in r for r in verdict.reasons)


def test_per_trade_risk_can_be_disabled_via_policy(healthy_account: AccountState) -> None:
    relaxed = RiskManager(RiskPolicy(require_stop_loss=False))
    intent = make_intent(stop_loss=None)
    verdict = relaxed.evaluate(intent, healthy_account)
    assert verdict.approved


# ── Compound rejections ────────────────────────────────────────────────────
def test_multiple_gates_fire_simultaneously(manager: RiskManager) -> None:
    bad = AccountState(
        nlv=200.0,                # below floor
        starting_nlv=1080.0,      # 81% drawdown — kill switch
        open_positions_margin=100.0,  # 50% margin — over cap
        open_position_count=4,    # over max
    )
    intent = make_intent(stop_loss=None)   # missing stop
    verdict = manager.evaluate(intent, bad)
    assert not verdict.approved
    assert len(verdict.reasons) >= 4
    keywords = " | ".join(verdict.reasons)
    assert "kill_switch" in keywords
    assert "nlv_floor" in keywords
    assert "max_positions" in keywords
    assert "stop_loss" in keywords


# ── Policy I/O ─────────────────────────────────────────────────────────────
def test_policy_loads_from_json() -> None:
    config_path = (
        pathlib.Path(__file__).resolve().parents[2] / "config" / "risk.json"
    )
    assert config_path.is_file(), config_path
    policy = RiskPolicy.from_json(config_path)
    assert policy.starting_nlv == 1080.0
    assert policy.nlv_floor == 300.0
    assert policy.kill_switch_drawdown_pct == 0.30
    assert policy.max_risk_per_trade_pct_nlv == 0.015
    assert policy.max_margin_utilization_pct == 0.40


def test_policy_ignores_unknown_keys(tmp_path: pathlib.Path) -> None:
    f = tmp_path / "risk.json"
    f.write_text(
        '{"_comment": "ignore me", "starting_nlv": 5000.0, "future_field": "x"}',
        encoding="utf-8",
    )
    policy = RiskPolicy.from_json(f)
    assert policy.starting_nlv == 5000.0
    assert policy.nlv_floor == 300.0   # default preserved


def test_with_policy_returns_new_manager_with_overrides(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    relaxed = manager.with_policy(max_risk_per_trade_pct_nlv=0.05)
    intent = make_intent(price=5000.0, stop_loss=4990.0)   # ~3.7% risk
    assert not manager.evaluate(intent, healthy_account).approved
    assert relaxed.evaluate(intent, healthy_account).approved
    # Original manager unchanged
    assert manager.policy.max_risk_per_trade_pct_nlv == 0.015


# ── Commission integration into per-trade-risk ────────────────────────────
# Per CEO+Gemini decision (2026-04-27): for sim mode commission stays at 0
# (matching Sierra Trade Sim's zero-commission fills); for live mode set to
# the broker's real rate (EdgeClear MES = $1.38/RT). The risk math must add
# commission to gross slippage so the per-trade cap evaluates TOTAL cost.

def test_default_commission_zero_preserves_phase_a_behavior(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    """Default round_trip_commission = 0.0 (sim mode). Risk amount equals
    pure price-move risk — same as Phase A pre-commission integration."""
    intent = make_intent(stop_loss=4998.0)   # 2pt = $10 gross risk
    verdict = manager.evaluate(intent, healthy_account)
    assert verdict.approved
    assert verdict.risk_amount == pytest.approx(10.0)


def test_non_zero_commission_adds_to_risk_amount(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    """Live mode: round_trip_commission > 0 lifts risk_amount accordingly."""
    intent = make_intent(stop_loss=4998.0, round_trip_commission=1.38)  # EdgeClear MES live rate
    verdict = manager.evaluate(intent, healthy_account)
    assert verdict.approved
    # gross $10 + $1.38 commission = $11.38
    assert verdict.risk_amount == pytest.approx(11.38)


def test_commission_scales_with_quantity(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    """Round-trip commission is per-contract — 2 contracts pays 2× fees."""
    intent = make_intent(
        quantity=2.0,
        stop_loss=4999.0,                  # 1pt × 2 contracts = $10 gross
        round_trip_commission=1.38,        # × 2 contracts = $2.76 fees
        per_contract_margin=50.0,          # × 2 = $100, fits margin cap
    )
    verdict = manager.evaluate(intent, healthy_account)
    assert verdict.approved, verdict.reasons
    # $10 gross + $2.76 commission = $12.76 (1.18% of $1080, under 1.5% cap)
    assert verdict.risk_amount == pytest.approx(12.76)


def test_commission_can_push_borderline_trade_from_approve_to_reject(
    manager: RiskManager, healthy_account: AccountState
) -> None:
    """A trade whose pure slippage risk fits under the 1.5% cap can be
    rejected once commissions are added — exactly the systematic-over-
    approval bug the integration prevents."""
    # $1080 × 1.5% = $16.20 cap. Pick a stop where gross risk is $15.50
    # (under cap) and commission pushes it to $16.88 (over cap).
    # Stop 12.4 ticks = 3.1 pts: $15.50 gross at $1.25/tick.
    # 3.1 pts → entry-stop = 5000 - 3.1 = 4996.9
    intent = make_intent(stop_loss=4996.9, round_trip_commission=1.38)
    # Without commission this would approve; with it, it should reject.
    base = manager.evaluate(make_intent(stop_loss=4996.9), healthy_account)
    assert base.approved, base.reasons       # baseline: pure slippage fits
    fees = manager.evaluate(intent, healthy_account)
    assert not fees.approved, "commission should have pushed over the 1.5% cap"
    assert any("per_trade_risk" in r for r in fees.reasons)


# ── AccountState helpers ───────────────────────────────────────────────────
def test_drawdown_pct_clamps_to_zero_when_account_is_up() -> None:
    up = AccountState(
        nlv=1500.0, starting_nlv=1080.0,
        open_positions_margin=0.0, open_position_count=0,
    )
    assert up.drawdown_pct == 0.0
