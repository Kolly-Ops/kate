from __future__ import annotations

import datetime as dt
import asyncio
import pathlib
from types import SimpleNamespace

import pytest

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.data import Candle
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import PositionEvent
from trading_bot.core.risk import AccountState, RiskManager, RiskPolicy, TradeIntent
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy.base import Strategy, StrategyContext
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


class _QuietStrategy(Strategy):
    @property
    def name(self) -> str:
        return "quiet"

    @property
    def history_window(self) -> int:
        return 1

    def on_candle_close(self, ctx: StrategyContext):
        return None


class _MT5LikeBroker(FakeBrokerAdapter):
    def __init__(self, *, modify_success: bool = True) -> None:
        super().__init__()
        self.modify_success = modify_success
        self.modified: list[dict] = []

    async def mt5_modify_position_stop(
        self,
        *,
        ticket: int,
        symbol: str,
        new_stop_price: float,
        keep_take_profit: bool = True,
    ):
        self.modified.append({
            "ticket": ticket,
            "symbol": symbol,
            "new_stop_price": new_stop_price,
            "keep_take_profit": keep_take_profit,
        })
        if not self.modify_success:
            return SimpleNamespace(success=False, reason="invalid stops")
        return SimpleNamespace(success=True, reason="")


def _meta() -> InstrumentMeta:
    return InstrumentMeta(
        symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        scid_filename="GBPUSD",
        dtc_symbol="GBPUSD",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
    )


def _engine(
    tmp_path: pathlib.Path,
    *,
    broker: FakeBrokerAdapter,
    enabled: bool,
) -> ManagedFuturesEngine:
    state = StateStore(tmp_path / "state.db").open()
    engine = ManagedFuturesEngine(
        symbols=["GBPUSD"],
        instruments={"GBPUSD": _meta()},
        candle_manager=None,
        strategy=_QuietStrategy(),
        risk=RiskManager(RiskPolicy()),
        state=state,
        reconciler=Reconciler(),
        broker=broker,
        trade_account="",
        use_native_brackets=True,
        enable_step_ratchet_stops=enabled,
    )
    engine._account_state = AccountState(
        nlv=1000,
        starting_nlv=1000,
        open_positions_margin=0.0,
        open_position_count=0,
    )
    return engine


def _intent() -> TradeIntent:
    ts = dt.datetime(2026, 6, 5, 6, 5, tzinfo=dt.timezone.utc)
    return TradeIntent(
        intent_id="fxlon-GBPUSD-2606050705",
        strategy_name="fx_london_breakout",
        symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        side=proto.BUY,
        quantity=0.56,
        order_type=proto.ORDER_TYPE_MARKET,
        tick_size=0.00001,
        tick_value=1.0,
        price=1.2500,
        stop_loss=1.2490,
        take_profit=1.2520,
        signal_timestamp_utc=ts,
    )


def _position(ticket: str = "101") -> PositionEvent:
    return PositionEvent(
        symbol="GBPUSD",
        quantity=0.56,
        avg_price=1.2500,
        side=proto.BUY,
        server_position_id=ticket,
    )


def _bar(close: float) -> Candle:
    return Candle(
        timestamp=dt.datetime(2026, 6, 5, 7, 10, tzinfo=dt.timezone.utc),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=100,
    )


def test_feature_flag_off_never_instantiates_tracker(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker()
        engine = _engine(tmp_path, broker=broker, enabled=False)

        await engine._submit_order(_intent())
        engine._handle_position_event(_position())

        assert engine._ratchet_pending_by_symbol_side == {}
        assert engine._ratchet_tracked == {}

    asyncio.run(_impl())


def test_feature_flag_on_new_position_creates_tracker(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker()
        engine = _engine(tmp_path, broker=broker, enabled=True)

        await engine._submit_order(_intent())
        engine._handle_position_event(_position("202"))

        assert 202 in engine._ratchet_tracked
        assert engine._ratchet_tracked[202].state.stage == 0

    asyncio.run(_impl())


def test_existing_startup_position_is_not_tracked(tmp_path: pathlib.Path) -> None:
    broker = _MT5LikeBroker()
    engine = _engine(tmp_path, broker=broker, enabled=True)

    engine._handle_position_event(_position("303"))

    assert engine._ratchet_tracked == {}


def test_bar_close_at_1r_advances_and_modifies_stop(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker()
        engine = _engine(tmp_path, broker=broker, enabled=True)
        await engine._submit_order(_intent())
        engine._handle_position_event(_position("404"))

        candle = _bar(1.2510)
        engine._history["GBPUSD"].append(candle)
        await engine._on_candle_close("GBPUSD", candle)

        assert broker.modified[-1]["ticket"] == 404
        assert broker.modified[-1]["new_stop_price"] == pytest.approx(1.2501)
        assert engine._ratchet_tracked[404].state.stage == 1

    asyncio.run(_impl())


def test_bar_close_at_1p5r_advances_to_stage_2(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker()
        engine = _engine(tmp_path, broker=broker, enabled=True)
        await engine._submit_order(_intent())
        engine._handle_position_event(_position("505"))

        candle = _bar(1.2515)
        engine._history["GBPUSD"].append(candle)
        await engine._on_candle_close("GBPUSD", candle)

        assert broker.modified[-1]["new_stop_price"] == pytest.approx(1.2505)
        assert engine._ratchet_tracked[505].state.stage == 2

    asyncio.run(_impl())


def test_modify_failure_leaves_tracker_at_previous_stage(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker(modify_success=False)
        engine = _engine(tmp_path, broker=broker, enabled=True)
        await engine._submit_order(_intent())
        engine._handle_position_event(_position("606"))

        candle = _bar(1.2510)
        engine._history["GBPUSD"].append(candle)
        await engine._on_candle_close("GBPUSD", candle)

        assert broker.modified
        assert engine._ratchet_tracked[606].state.stage == 0

    asyncio.run(_impl())


def test_position_close_removes_tracker(tmp_path: pathlib.Path) -> None:
    async def _impl() -> None:
        broker = _MT5LikeBroker()
        engine = _engine(tmp_path, broker=broker, enabled=True)
        await engine._submit_order(_intent())
        engine._handle_position_event(_position("707"))

        engine._handle_position_event(PositionEvent(
            symbol="GBPUSD",
            quantity=0.0,
            avg_price=1.2520,
            side=None,
            server_position_id="707",
        ))

        assert 707 not in engine._ratchet_tracked

    asyncio.run(_impl())
