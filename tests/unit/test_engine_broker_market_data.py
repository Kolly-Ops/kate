from __future__ import annotations

import datetime as dt
import asyncio
import pathlib
from typing import Optional

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.data import CandleManager
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
    BarEvent,
    BrokerEvent,
    BrokerEventKind,
    MarketDataTick,
)
from trading_bot.core.risk import RiskManager, RiskPolicy, TradeIntent
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import Strategy, StrategyContext
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


class CapturingStrategy(Strategy):
    @property
    def name(self) -> str:
        return "capture"

    @property
    def history_window(self) -> int:
        return 1

    def on_candle_close(self, ctx: StrategyContext) -> Optional[TradeIntent]:
        return TradeIntent(
            intent_id="cap-GBPUSD-1",
            strategy_name=self.name,
            symbol=ctx.symbol,
            exchange=ctx.exchange,
            side=proto.BUY,
            quantity=0.01,
            order_type=proto.ORDER_TYPE_MARKET,
            tick_size=ctx.tick_size,
            tick_value=ctx.tick_value,
            price=ctx.candle.close,
            stop_loss=ctx.candle.close - 0.0010,
            take_profit=ctx.candle.close + 0.0020,
            per_contract_margin=ctx.per_contract_margin,
        )


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


def _tick(ts: dt.datetime, price: float) -> BrokerEvent:
    return BrokerEvent(
        kind=BrokerEventKind.MARKET_DATA_TICK,
        received_at=ts.timestamp(),
        tick=MarketDataTick(
            symbol="GBPUSD",
            timestamp=ts,
            last_price=price,
            last_size=1.0,
        ),
    )


def test_broker_market_data_ticks_close_candles_and_submit_native_bracket(tmp_path: pathlib.Path) -> None:
    asyncio.run(_run_broker_market_data_case(tmp_path))


def _bar(ts: dt.datetime, *, open_: float, high: float, low: float, close: float) -> BrokerEvent:
    """Build a MARKET_DATA_BAR event mirroring the NinjaBrokerAdapter's output shape."""
    return BrokerEvent(
        kind=BrokerEventKind.MARKET_DATA_BAR,
        received_at=ts.timestamp(),
        bar=BarEvent(
            symbol="GBPUSD",
            timestamp=ts,
            timeframe_minutes=1,
            open=open_, high=high, low=low, close=close, volume=10,
        ),
    )


def test_broker_market_data_bar_feeds_candle_directly_and_fires_strategy(tmp_path: pathlib.Path) -> None:
    """Option A path: pre-aggregated bars from NinjaTrader bypass the
    TickCandleAggregator and feed the engine's history + strategy
    callback in one step."""
    asyncio.run(_run_broker_market_data_bar_case(tmp_path))


def test_engine_passes_signal_close_price_to_adapter(tmp_path: pathlib.Path) -> None:
    """Codex review §3 wiring: engine pulls signal_close_price from the
    intent (or falls back to intent.price) and passes it to submit_order
    so slippage telemetry is computed against the actual decision-time
    close, not guessed downstream."""
    asyncio.run(_run_signal_close_price_wired_case(tmp_path))


async def _run_signal_close_price_wired_case(tmp_path: pathlib.Path) -> None:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    engine = ManagedFuturesEngine(
        symbols=["GBPUSD"],
        instruments={"GBPUSD": _meta()},
        candle_manager=CandleManager(scid_dir=tmp_path, timeframe_minutes=1),
        strategy=CapturingStrategy(),  # uses intent.price = ctx.candle.close
        risk=RiskManager(RiskPolicy(max_risk_per_trade_pct_nlv=1.0)),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="52880143",
        use_broker_market_data=True,
        use_native_brackets=True,
    )
    try:
        await engine.start()
        # CapturingStrategy doesn't set intent.signal_close_price, so the
        # engine falls back to intent.price (== ctx.candle.close == 1.2510
        # in this case).
        await engine._handle_broker_event(_bar(
            dt.datetime(2026, 5, 13, 6, 59, 0),
            open_=1.2500, high=1.2515, low=1.2495, close=1.2510,
        ))
        assert len(fake.submitted) == 1
        # The fallback path: intent.price == 1.2510 propagated as signal_close_price
        assert fake.submitted[0]["signal_close_price"] == 1.251
    finally:
        await engine.stop()
        state.close()


async def _run_broker_market_data_bar_case(tmp_path: pathlib.Path) -> None:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    engine = ManagedFuturesEngine(
        symbols=["GBPUSD"],
        instruments={"GBPUSD": _meta()},
        candle_manager=CandleManager(scid_dir=tmp_path, timeframe_minutes=1),
        strategy=CapturingStrategy(),
        risk=RiskManager(RiskPolicy(max_risk_per_trade_pct_nlv=1.0)),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="52880143",
        use_broker_market_data=True,
        use_native_brackets=True,
    )
    try:
        await engine.start()
        # One pre-aggregated bar = one candle = one strategy invocation.
        # No second tick needed to "close" it — bar arrives already closed.
        await engine._handle_broker_event(_bar(
            dt.datetime(2026, 5, 13, 6, 59, 0),
            open_=1.2500, high=1.2515, low=1.2495, close=1.2510,
        ))
        assert len(engine.history("GBPUSD")) == 1
        assert len(fake.submitted) == 1
        submitted = fake.submitted[0]
        assert submitted["client_order_id"] == "cap-GBPUSD-1"
        # Strategy reads ctx.candle.close = 1.2510; SL/TP arithmetic from
        # CapturingStrategy: close-0.0010=1.2500, close+0.0020=1.2530
        assert submitted["stop_price"] == 1.25
        assert submitted["target_price"] == 1.253
    finally:
        await engine.stop()
        state.close()


def test_broker_market_data_bar_timeframe_mismatch_dropped(tmp_path: pathlib.Path) -> None:
    """Bars whose timeframe disagrees with the engine config are dropped
    with a WARNING rather than silently consumed (would corrupt history)."""
    asyncio.run(_run_broker_market_data_bar_timeframe_case(tmp_path))


async def _run_broker_market_data_bar_timeframe_case(tmp_path: pathlib.Path) -> None:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    engine = ManagedFuturesEngine(
        symbols=["GBPUSD"],
        instruments={"GBPUSD": _meta()},
        candle_manager=CandleManager(scid_dir=tmp_path, timeframe_minutes=1),
        strategy=CapturingStrategy(),
        risk=RiskManager(RiskPolicy(max_risk_per_trade_pct_nlv=1.0)),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="52880143",
        use_broker_market_data=True,
        use_native_brackets=True,
    )
    try:
        await engine.start()
        mismatched = BrokerEvent(
            kind=BrokerEventKind.MARKET_DATA_BAR,
            received_at=0.0,
            bar=BarEvent(
                symbol="GBPUSD",
                timestamp=dt.datetime(2026, 5, 13, 7, 0, 0),
                timeframe_minutes=5,           # engine configured for 1m
                open=1.25, high=1.26, low=1.24, close=1.255, volume=1,
            ),
        )
        await engine._handle_broker_event(mismatched)
        assert len(engine.history("GBPUSD")) == 0
        assert fake.submitted == []
    finally:
        await engine.stop()
        state.close()


async def _run_broker_market_data_case(tmp_path: pathlib.Path) -> None:
    state = StateStore(tmp_path / "state.db").open()
    fake = FakeBrokerAdapter()
    engine = ManagedFuturesEngine(
        symbols=["GBPUSD"],
        instruments={"GBPUSD": _meta()},
        candle_manager=CandleManager(scid_dir=tmp_path, timeframe_minutes=1),
        strategy=CapturingStrategy(),
        risk=RiskManager(RiskPolicy(max_risk_per_trade_pct_nlv=1.0)),
        state=state,
        reconciler=Reconciler(),
        broker=fake,
        trade_account="52880143",
        use_broker_market_data=True,
        use_native_brackets=True,
    )
    try:
        await engine.start()
        assert fake.subscriptions == [{"symbol": "GBPUSD", "exchange": "ICMarketsSC-Demo"}]

        await engine._handle_broker_event(_tick(dt.datetime(2026, 5, 13, 6, 59, 10), 1.2500))
        await engine._handle_broker_event(_tick(dt.datetime(2026, 5, 13, 7, 0, 1), 1.2510))

        assert len(engine.history("GBPUSD")) == 1
        assert len(fake.submitted) == 1
        submitted = fake.submitted[0]
        assert submitted["client_order_id"] == "cap-GBPUSD-1"
        assert submitted["stop_price"] == 1.249
        assert submitted["target_price"] == 1.252
        assert engine._pending_brackets == {}
    finally:
        await engine.stop()
        state.close()
