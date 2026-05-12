"""
Unit tests for the volatility-blackout window in ManagedFuturesEngine.

The engine should skip strategy invocation entirely on candle closes whose
UTC timestamp falls inside any configured `no_trade_windows_utc` range.
Existing positions and bracket orders are unaffected by the blackout —
only NEW signal generation is suppressed.

Tests use a recording-only fake strategy so we can assert exactly when
on_candle_close is invoked vs skipped, plus a date-tunable Candle
constructor so we can drive timestamps deterministically across the
window boundaries.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import pathlib
from typing import Optional

import pytest

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.data import Candle
from trading_bot.core.risk import RiskManager, RiskPolicy, TradeIntent
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import Strategy, StrategyContext
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


class RecordingStrategy(Strategy):
    """Captures every on_candle_close invocation. Returns None so no
    intent flows through risk + execution."""

    def __init__(self) -> None:
        self.calls: list[Candle] = []

    @property
    def name(self) -> str:
        return "recording"

    @property
    def history_window(self) -> int:
        return 1

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        self.calls.append(ctx.candle)
        return None


def _meta() -> InstrumentMeta:
    return InstrumentMeta(
        symbol="MESM26", exchange="CME",
        scid_filename="MESM26_FUT_CME", dtc_symbol="MESM26-CME",
        tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
    )


def _candle(ts: dt.datetime) -> Candle:
    return Candle(
        timestamp=ts,
        open=6125.0, high=6126.0, low=6124.5, close=6125.5, volume=10,
    )


def _build_engine(
    tmp_path: pathlib.Path,
    *,
    no_trade_windows_utc: Optional[list[tuple[dt.time, dt.time]]] = None,
) -> tuple[ManagedFuturesEngine, RecordingStrategy, StateStore]:
    state = StateStore(tmp_path / "state.db").open()
    strat = RecordingStrategy()
    engine = ManagedFuturesEngine(
        symbols=["MESM26"],
        instruments={"MESM26": _meta()},
        candle_manager=None,
        strategy=strat,
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=FakeBrokerAdapter(),
        trade_account="",
        no_trade_windows_utc=no_trade_windows_utc,
    )
    # Engine needs an account_state to invoke the strategy at all.
    from trading_bot.core.risk import AccountState
    engine._account_state = AccountState(
        nlv=1080.0, starting_nlv=1080.0,
        open_positions_margin=0.0, open_position_count=0,
    )
    # And history needs to meet strategy.history_window
    engine._history["MESM26"].append(_candle(dt.datetime(2026, 4, 30, 0, 0)))
    return engine, strat, state


@pytest.fixture
def engine_setup(tmp_path: pathlib.Path):
    engine, strat, state = _build_engine(
        tmp_path,
        no_trade_windows_utc=[(dt.time(13, 30), dt.time(14, 30))],
    )
    try:
        yield engine, strat, state
    finally:
        state.close()


# ── Inside-window: strategy must NOT be invoked ───────────────────────
def test_blackout_skips_strategy_at_window_start(engine_setup) -> None:
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 13, 30, 0))   # exactly at start
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert strat.calls == []


def test_blackout_skips_strategy_mid_window(engine_setup) -> None:
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 14, 0, 0))   # mid-window
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert strat.calls == []


def test_blackout_window_end_is_exclusive(engine_setup) -> None:
    """At exactly 14:30:00 the window has ended — strategy fires again."""
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 14, 30, 0))
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert len(strat.calls) == 1


# ── Outside-window: strategy IS invoked ────────────────────────────────
def test_strategy_fires_before_window(engine_setup) -> None:
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 13, 29, 59))
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert len(strat.calls) == 1


def test_strategy_fires_well_after_window(engine_setup) -> None:
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 16, 0, 0))
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert len(strat.calls) == 1


def test_strategy_fires_overnight_outside_window(engine_setup) -> None:
    engine, strat, _ = engine_setup
    candle = _candle(dt.datetime(2026, 4, 30, 3, 15, 0))    # nowhere near window
    asyncio.get_event_loop().run_until_complete(
        engine._on_candle_close("MESM26", candle)
    )
    assert len(strat.calls) == 1


# ── No window configured: behaves as before ────────────────────────────
def test_no_window_configured_strategy_always_fires(tmp_path: pathlib.Path) -> None:
    engine, strat, state = _build_engine(tmp_path, no_trade_windows_utc=None)
    try:
        for hour in (0, 6, 13, 14, 21):
            candle = _candle(dt.datetime(2026, 4, 30, hour, 0, 0))
            asyncio.get_event_loop().run_until_complete(
                engine._on_candle_close("MESM26", candle)
            )
        assert len(strat.calls) == 5
    finally:
        state.close()


# ── Multi-window support ───────────────────────────────────────────────
def test_multiple_windows_both_block(tmp_path: pathlib.Path) -> None:
    engine, strat, state = _build_engine(
        tmp_path,
        no_trade_windows_utc=[
            (dt.time(13, 30), dt.time(14, 30)),
            (dt.time(2, 0),  dt.time(4, 0)),
        ],
    )
    try:
        # In first window
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 4, 30, 14, 0, 0)))
        )
        # In second window
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 4, 30, 3, 0, 0)))
        )
        # Outside both
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 4, 30, 10, 0, 0)))
        )
        assert len(strat.calls) == 1   # only the third call fires
    finally:
        state.close()


# ── Wrap-around (overnight) window ─────────────────────────────────────
def test_wrap_around_window_blocks_across_midnight(tmp_path: pathlib.Path) -> None:
    """A window like 23:30-00:30 should block 23:45 AND 00:15."""
    engine, strat, state = _build_engine(
        tmp_path,
        no_trade_windows_utc=[(dt.time(23, 30), dt.time(0, 30))],
    )
    try:
        # 23:45 — inside the start half
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 4, 30, 23, 45, 0)))
        )
        # 00:15 next day — inside the end half
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 5, 1, 0, 15, 0)))
        )
        # 12:00 — outside
        asyncio.get_event_loop().run_until_complete(
            engine._on_candle_close("MESM26", _candle(dt.datetime(2026, 4, 30, 12, 0, 0)))
        )
        assert len(strat.calls) == 1
    finally:
        state.close()
