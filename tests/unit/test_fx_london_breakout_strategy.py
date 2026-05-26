import datetime as dt

import pytest

from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.strategy import FXLondonBreakoutStrategy, NewsEvent, StrategyContext


UK = dt.timezone(dt.timedelta(hours=1))


def candle(ts: dt.datetime, close: float, high: float | None = None, low: float | None = None) -> Candle:
    high = close + 0.0003 if high is None else high
    low = close - 0.0003 if low is None else low
    return Candle(timestamp=ts, open=close, high=high, low=low, close=close, volume=100)


def ctx(candles: tuple[Candle, ...], *, has_open_position: bool = False) -> StrategyContext:
    return StrategyContext(
        symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        candle=candles[-1],
        history=candles,
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=has_open_position,
    )


def asian_range(day: dt.date) -> list[Candle]:
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK)
    for minute in range(7 * 60):
        ts = start + dt.timedelta(minutes=minute)
        close = 1.2500 + ((minute % 20) * 0.00001)
        candles.append(candle(ts, close, high=1.2520, low=1.2480))
    return candles


def warmup_before(day: dt.date) -> list[Candle]:
    start = dt.datetime.combine(day, dt.time(23, 30), tzinfo=UK) - dt.timedelta(days=1)
    return [candle(start + dt.timedelta(minutes=i), 1.2500) for i in range(30)]


def test_long_breakout_uses_tighter_atr_stop_and_fractional_lot() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530, high=1.2532, low=1.2527)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(quantity=0.01, reward_risk=2.0, atr_period=14)

    intent = strategy.on_candle_close(ctx(history))

    assert intent is not None
    assert intent.side == proto.BUY
    assert intent.quantity == pytest.approx(0.01)
    assert intent.order_type == proto.ORDER_TYPE_MARKET
    assert intent.stop_loss == pytest.approx(1.24907)
    assert intent.take_profit == pytest.approx(1.26086)
    assert intent.metadata["asian_range_high"] == "1.25200"


def test_short_breakout_uses_tighter_atr_stop() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 15, tzinfo=UK), 1.2475, high=1.2478, low=1.2473)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(quantity=0.01, reward_risk=1.5, atr_period=14)

    intent = strategy.on_candle_close(ctx(history))

    assert intent is not None
    assert intent.side == proto.SELL
    assert intent.stop_loss == pytest.approx(1.25142)
    assert intent.take_profit == pytest.approx(1.24162)


def test_no_entry_outside_london_trade_window() -> None:
    day = dt.date(2026, 5, 13)
    late_breakout = candle(dt.datetime(2026, 5, 13, 10, 0, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + asian_range(day) + [late_breakout])

    assert FXLondonBreakoutStrategy().on_candle_close(ctx(history)) is None


def test_news_event_blocks_two_minutes_each_side() -> None:
    day = dt.date(2026, 5, 13)
    event = NewsEvent(dt.datetime(2026, 5, 13, 7, 6, tzinfo=UK), region="UK", label="CPI")
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])

    assert FXLondonBreakoutStrategy(news_events=(event,)).on_candle_close(ctx(history)) is None


def test_one_trade_per_symbol_session_day() -> None:
    """Once a position has been observed open for a symbol+date, the
    strategy refuses to re-enter for the rest of that session.

    Note: this test exercises the 2026-05-26 observation-driven session
    cache. Pre-2026-05-26 the cache was populated on intent generation;
    now it's populated only when has_open_position=True is observed,
    which models the actual broker fill. The second call here mirrors
    the engine's post-fill state by passing has_open_position=True.
    """
    day = dt.date(2026, 5, 13)
    first = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    second = candle(dt.datetime(2026, 5, 13, 7, 10, tzinfo=UK), 1.2535)
    third = candle(dt.datetime(2026, 5, 13, 7, 15, tzinfo=UK), 1.2540)
    strategy = FXLondonBreakoutStrategy()

    # First candle: fresh signal, strategy emits intent.
    assert strategy.on_candle_close(
        ctx(tuple(warmup_before(day) + asian_range(day) + [first]))
    ) is not None

    # Second candle: position now observed open (mirrors engine state
    # after broker fill). Strategy skips with "open position" and
    # marks the session as traded.
    assert strategy.on_candle_close(
        ctx(tuple(warmup_before(day) + asian_range(day) + [first, second]),
            has_open_position=True)
    ) is None

    # Third candle: position has since closed (TP/SL fired), but the
    # session is now in the strategy's cache because we observed an
    # open position earlier. Strategy must NOT re-enter same-day.
    assert strategy.on_candle_close(
        ctx(tuple(warmup_before(day) + asian_range(day) + [first, second, third]),
            has_open_position=False)
    ) is None


def test_rejected_intent_does_not_consume_session_slot() -> None:
    """2026-05-26 CEO directive: session slots should only be consumed
    by trades that ACTUALLY filled. A strategy signal that gets rejected
    downstream by the risk manager (e.g. max_open_positions cap) must
    leave the session slot available for the same symbol to re-attempt
    later in the trade window.

    This test models the pattern observed today: EURGBP filled at 07:02,
    GBPUSD/EURUSD/AUDUSD signals fired but were rejected by max_positions
    cap. Under the old (pre-2026-05-26) cache semantics, those rejected
    symbols would never re-fire that day. Under the new semantics, they
    can attempt again on subsequent candles.
    """
    day = dt.date(2026, 5, 13)
    first = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    second = candle(dt.datetime(2026, 5, 13, 7, 10, tzinfo=UK), 1.2535)
    strategy = FXLondonBreakoutStrategy()

    # First candle: strategy emits intent. We're modelling the case
    # where downstream risk REJECTS this intent (no fill).
    intent1 = strategy.on_candle_close(
        ctx(tuple(warmup_before(day) + asian_range(day) + [first]))
    )
    assert intent1 is not None

    # Second candle: NO position was ever observed open
    # (because the prior intent was rejected by risk, not filled).
    # Strategy must be willing to emit ANOTHER intent for the same
    # symbol+date — the session is NOT yet consumed.
    intent2 = strategy.on_candle_close(
        ctx(tuple(warmup_before(day) + asian_range(day) + [first, second]),
            has_open_position=False)
    )
    assert intent2 is not None, (
        "rejected intent must not consume the session slot; strategy "
        "should be willing to re-attempt on the next candle close"
    )


def test_requires_asian_range_history() -> None:
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530)
    history = tuple(warmup_before(day) + [breakout])

    assert FXLondonBreakoutStrategy().on_candle_close(ctx(history)) is None


# Reproduces 2026-05-22 AUDUSD failure shape: Asian range with two specific
# extrema bars but tight per-bar TR for the rest of the session, so ATR(14)
# at signal time is sub-2-pip and the min-stop floor must bind.
def tight_audusd_asian_range(day: dt.date) -> list[Candle]:
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK)
    # Inject range extrema in the FIRST 2 bars so they fall outside the
    # last-15 ATR window when the breakout fires.
    candles.append(Candle(
        timestamp=start, open=0.71430, high=0.71520, low=0.71430,
        close=0.71430, volume=100,
    ))
    candles.append(Candle(
        timestamp=start + dt.timedelta(minutes=1), open=0.71430,
        high=0.71430, low=0.71344, close=0.71400, volume=100,
    ))
    # Remaining 418 Asian bars: constant close 0.71400 with 1-pip TR each.
    for minute in range(2, 7 * 60):
        ts = start + dt.timedelta(minutes=minute)
        candles.append(Candle(
            timestamp=ts, open=0.71400, high=0.71405, low=0.71395,
            close=0.71400, volume=100,
        ))
    return candles


def tight_warmup_before(day: dt.date) -> list[Candle]:
    start = dt.datetime.combine(day, dt.time(23, 30), tzinfo=UK) - dt.timedelta(days=1)
    return [
        Candle(
            timestamp=start + dt.timedelta(minutes=i),
            open=0.71400, high=0.71405, low=0.71395,
            close=0.71400, volume=100,
        )
        for i in range(30)
    ]


def audusd_ctx(candles: tuple[Candle, ...], *, symbol: str = "AUDUSD") -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        exchange="ICMarketsSC-Demo",
        candle=candles[-1],
        history=candles,
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=False,
    )


def test_audusd_quiet_atr_short_breakout_floor_binds() -> None:
    """Reproduces 2026-05-22 failure: ATR(14) ≈ 1.4 pips, floor must bind at 5 pips.

    Before A-prime: stop would be 1.5 pips wide → stop-out inside broker noise.
    After A-prime: floor binds, stop is 5 pips wide.
    """
    day = dt.date(2026, 5, 22)
    breakout_ts = dt.datetime(2026, 5, 22, 7, 18, tzinfo=UK)
    breakout = Candle(
        timestamp=breakout_ts, open=0.71400, high=0.71400, low=0.71336,
        close=0.71336, volume=100,
    )
    history = tuple(tight_warmup_before(day) + tight_audusd_asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(
        quantity=0.56, reward_risk=2.0, atr_period=14, atr_stop_multiplier=1.1,
    )

    intent = strategy.on_candle_close(audusd_ctx(history))

    assert intent is not None
    assert intent.side == proto.SELL
    # Floor binds: stop_distance = max(atr*1.1, 5_pips) = 5_pips = 0.0005
    # stop = min(range_high=0.71520, close+0.0005) = min(0.71520, 0.71386) = 0.71386
    assert intent.stop_loss == pytest.approx(0.71386, abs=1e-5)
    # take_profit at rr=2: close - 2*risk where risk=stop-close=0.0005
    # tp = 0.71336 - 0.0010 = 0.71236
    assert intent.take_profit == pytest.approx(0.71236, abs=1e-5)
    assert intent.metadata["floor_binding"] == "true"
    assert intent.metadata["min_stop_pips"] == "5.00"
    assert intent.metadata["effective_stop_pips"] == "5.00"
    # ATR-derived stop should be well below 5 pips on this tight-ATR setup
    assert float(intent.metadata["atr_stop_pips"]) < 5.0


def test_gbpusd_normal_vol_breakout_floor_does_not_bind() -> None:
    """Existing GBPUSD test fixture has 40-pip per-bar TR → ATR-derived stop
    wildly exceeds the 6-pip GBPUSD floor. Floor must NOT bind; existing
    pre-A-prime behaviour preserved."""
    day = dt.date(2026, 5, 13)
    breakout = candle(dt.datetime(2026, 5, 13, 7, 5, tzinfo=UK), 1.2530, high=1.2532, low=1.2527)
    history = tuple(warmup_before(day) + asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(quantity=0.01, reward_risk=2.0, atr_period=14)

    intent = strategy.on_candle_close(ctx(history))

    assert intent is not None
    assert intent.side == proto.BUY
    # Existing assertion preserved — stop_loss unchanged from pre-A-prime
    assert intent.stop_loss == pytest.approx(1.24907)
    assert intent.metadata["floor_binding"] == "false"
    assert intent.metadata["min_stop_pips"] == "6.00"  # GBPUSD default
    # Effective stop in pips should match ATR-derived stop (floor not binding)
    assert intent.metadata["effective_stop_pips"] == intent.metadata["atr_stop_pips"]


def test_unknown_symbol_uses_fallback_with_warning(caplog) -> None:
    """Demo-safe default: unknown symbol uses fallback floor and warns."""
    day = dt.date(2026, 5, 22)
    breakout_ts = dt.datetime(2026, 5, 22, 7, 18, tzinfo=UK)
    breakout = Candle(
        timestamp=breakout_ts, open=0.71400, high=0.71400, low=0.71336,
        close=0.71336, volume=100,
    )
    history = tuple(tight_warmup_before(day) + tight_audusd_asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(
        quantity=0.56, reward_risk=2.0, atr_period=14, atr_stop_multiplier=1.1,
        min_stop_pips_fallback=5.0,
        fail_on_unknown_symbol=False,
    )

    with caplog.at_level("WARNING"):
        intent = strategy.on_candle_close(audusd_ctx(history, symbol="ZARJPY"))

    assert intent is not None
    assert intent.metadata["min_stop_pips"] == "5.00"  # the fallback
    assert "no min_stop_pips configured" in caplog.text


def test_unknown_symbol_with_fail_loud_raises() -> None:
    """Production-safe: fail_on_unknown_symbol=True raises rather than guess."""
    day = dt.date(2026, 5, 22)
    breakout_ts = dt.datetime(2026, 5, 22, 7, 18, tzinfo=UK)
    breakout = Candle(
        timestamp=breakout_ts, open=0.71400, high=0.71400, low=0.71336,
        close=0.71336, volume=100,
    )
    history = tuple(tight_warmup_before(day) + tight_audusd_asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(
        quantity=0.56, reward_risk=2.0, atr_period=14, atr_stop_multiplier=1.1,
        fail_on_unknown_symbol=True,
    )

    with pytest.raises(ValueError, match="no min_stop_pips configured"):
        strategy.on_candle_close(audusd_ctx(history, symbol="ZARJPY"))


# Codex 2026-05-22 A-prime cross-check follow-up: range clamp can reduce
# the effective stop below the configured floor when the breakout candle
# closes near a range boundary. Documenting the current behaviour
# (intact clamp, no skip) per Codex's "leave behaviour intact and watch
# the metadata" directive. If we later decide the floor should be
# absolute, swap this test's assertions for a `assert intent is None`
# behaviour and add the skip guard in the strategy.
def narrow_gbpusd_asian_range(day: dt.date) -> list[Candle]:
    """Asian range of 5.5 pips at GBPUSD-level prices (safely > 5.0-pip filter
    despite floating-point precision). 1 GBPUSD pip = 0.0001."""
    candles: list[Candle] = []
    start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=UK)
    # Bar 0: high marker at 1.30055
    candles.append(Candle(
        timestamp=start, open=1.30025, high=1.30055, low=1.30025,
        close=1.30030, volume=100,
    ))
    # Bar 1: low marker at 1.30000
    candles.append(Candle(
        timestamp=start + dt.timedelta(minutes=1), open=1.30025,
        high=1.30030, low=1.30000, close=1.30025, volume=100,
    ))
    # Remaining bars: 1-pip TR centered at 1.30025 to keep ATR collapsed.
    for minute in range(2, 7 * 60):
        ts = start + dt.timedelta(minutes=minute)
        candles.append(Candle(
            timestamp=ts, open=1.30025, high=1.30030, low=1.30020,
            close=1.30025, volume=100,
        ))
    return candles


def gbpusd_tight_warmup_before(day: dt.date) -> list[Candle]:
    start = dt.datetime.combine(day, dt.time(23, 30), tzinfo=UK) - dt.timedelta(days=1)
    return [
        Candle(
            timestamp=start + dt.timedelta(minutes=i),
            open=1.30025, high=1.30030, low=1.30020,
            close=1.30025, volume=100,
        )
        for i in range(30)
    ]


def test_gbpusd_range_clamp_pulls_effective_stop_strictly_below_floor() -> None:
    """The edge case Codex flagged on 2026-05-22 A-prime cross-check:
    GBPUSD floor=6 pips, Asian range=5.5 pips. Breakout closes 0.4 pips
    below range_low, so range_high is only 5.9 pips above close.
    Pre-clamp stop wants to be 6 pips above close (the floor), but
    range_high clamps at 5.9 pips. Effective stop ends up strictly
    below the configured floor.

    Current behaviour: clamp wins, trade fires with effective_stop_pips
    less than min_stop_pips. This is the documented intact-clamp
    behaviour per Codex's "leave behaviour intact and watch the
    metadata" directive. If we later decide the floor must be absolute,
    swap `assert intent is not None` for `assert intent is None` and
    add the skip guard in fx_london_breakout.on_candle_close.
    """
    day = dt.date(2026, 5, 22)
    breakout_ts = dt.datetime(2026, 5, 22, 7, 18, tzinfo=UK)
    # Close 0.4 pips below range_low (1.30000) -> close = 1.29996
    breakout = Candle(
        timestamp=breakout_ts, open=1.30000, high=1.30000, low=1.29996,
        close=1.29996, volume=100,
    )
    history = tuple(gbpusd_tight_warmup_before(day) + narrow_gbpusd_asian_range(day) + [breakout])
    strategy = FXLondonBreakoutStrategy(
        quantity=0.01, reward_risk=2.0, atr_period=14, atr_stop_multiplier=1.1,
    )

    intent = strategy.on_candle_close(StrategyContext(
        symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        candle=history[-1],
        history=history,
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        has_open_position=False,
    ))

    assert intent is not None
    assert intent.side == proto.SELL
    # range_high = 1.30055. Pre-clamp stop = 1.29996 + 0.0006 = 1.30056.
    # min(1.30055, 1.30056) = 1.30055. effective stop = 1.30055 - 1.29996
    # = 0.00059 = 5.9 pips.
    assert intent.stop_loss == pytest.approx(1.30055, abs=1e-5)
    assert intent.metadata["min_stop_pips"] == "6.00"
    assert intent.metadata["floor_binding"] == "true"
    # The KEY assertion: effective_stop_pips is STRICTLY LESS than the configured floor.
    assert float(intent.metadata["effective_stop_pips"]) < 6.0
    assert float(intent.metadata["effective_stop_pips"]) == pytest.approx(5.9, abs=0.1)
