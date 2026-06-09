import pytest

from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy.stop_management import StepRatchetStopPolicy


def test_step_ratchet_rejects_wick_until_close_confirms_long() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=1.2490)

    wick_only = policy.evaluate_bar_close(
        state=state,
        side=proto.BUY,
        entry_price=1.2500,
        initial_stop=1.2490,
        bar_close=1.2508,  # high could have pierced +1R; close did not
        pip_size=0.0001,
    )

    assert wick_only.state.stage == 0
    assert wick_only.state.stop_price == pytest.approx(1.2490)
    assert wick_only.advanced is False

    confirmed = policy.evaluate_bar_close(
        state=state,
        side=proto.BUY,
        entry_price=1.2500,
        initial_stop=1.2490,
        bar_close=1.2510,
        pip_size=0.0001,
    )

    assert confirmed.state.stage == 1
    assert confirmed.state.stop_price == pytest.approx(1.2501)
    assert confirmed.advanced is True
    assert confirmed.reason == "close>=1R"


def test_step_ratchet_advances_to_half_r_on_confirmed_1p5r_long() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=1.2490)

    decision = policy.evaluate_bar_close(
        state=state,
        side=proto.BUY,
        entry_price=1.2500,
        initial_stop=1.2490,
        bar_close=1.2515,
        pip_size=0.0001,
    )

    assert decision.state.stage == 2
    assert decision.state.stop_price == pytest.approx(1.2505)
    assert decision.reason == "close>=1.5R"


def test_step_ratchet_skip_ahead_to_stage_2_on_first_close() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=1.2490)

    decision = policy.evaluate_bar_close(
        state=state,
        side=proto.BUY,
        entry_price=1.2500,
        initial_stop=1.2490,
        bar_close=1.2516,
        pip_size=0.0001,
    )

    assert decision.state.stage == 2
    assert decision.state.stop_price == pytest.approx(1.2505)


def test_step_ratchet_never_loosens_long_stop() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)

    decision = policy.evaluate_bar_close(
        state=policy.initial_state(initial_stop=1.2506),
        side=proto.BUY,
        entry_price=1.2500,
        initial_stop=1.2490,
        bar_close=1.2515,
        pip_size=0.0001,
    )

    assert decision.state.stage == 2
    assert decision.state.stop_price == pytest.approx(1.2506)


def test_step_ratchet_short_side_uses_inverse_prices() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=1.2510)

    stage1 = policy.evaluate_bar_close(
        state=state,
        side=proto.SELL,
        entry_price=1.2500,
        initial_stop=1.2510,
        bar_close=1.2490,
        pip_size=0.0001,
    )

    assert stage1.state.stage == 1
    assert stage1.state.stop_price == pytest.approx(1.2499)

    stage2 = policy.evaluate_bar_close(
        state=stage1.state,
        side=proto.SELL,
        entry_price=1.2500,
        initial_stop=1.2510,
        bar_close=1.2485,
        pip_size=0.0001,
    )

    assert stage2.state.stage == 2
    assert stage2.state.stop_price == pytest.approx(1.2495)


def test_step_ratchet_rejects_wick_until_close_confirms_short() -> None:
    policy = StepRatchetStopPolicy(buffer_pips=1.0)
    state = policy.initial_state(initial_stop=1.2510)

    wick_only = policy.evaluate_bar_close(
        state=state,
        side=proto.SELL,
        entry_price=1.2500,
        initial_stop=1.2510,
        bar_close=1.2492,  # low could have pierced +1R; close did not
        pip_size=0.0001,
    )
    assert wick_only.state.stage == 0

    confirmed = policy.evaluate_bar_close(
        state=state,
        side=proto.SELL,
        entry_price=1.2500,
        initial_stop=1.2510,
        bar_close=1.2490,
        pip_size=0.0001,
    )
    assert confirmed.state.stage == 1
    assert confirmed.state.stop_price == pytest.approx(1.2499)


def test_step_ratchet_validates_inputs() -> None:
    policy = StepRatchetStopPolicy()
    state = policy.initial_state(initial_stop=1.2490)

    with pytest.raises(ValueError, match="pip_size"):
        policy.evaluate_bar_close(
            state=state,
            side=proto.BUY,
            entry_price=1.2500,
            initial_stop=1.2490,
            bar_close=1.2510,
            pip_size=0.0,
        )

    with pytest.raises(ValueError, match="positive risk"):
        policy.evaluate_bar_close(
            state=state,
            side=proto.BUY,
            entry_price=1.2500,
            initial_stop=1.2500,
            bar_close=1.2510,
            pip_size=0.0001,
        )
