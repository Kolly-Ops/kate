from __future__ import annotations

import datetime as dt
import asyncio
import pathlib
from typing import Optional

from tests.mocks.fake_broker_adapter import FakeBrokerAdapter
from trading_bot.core.data import CandleManager
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.broker_adapter import (
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
