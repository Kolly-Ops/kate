"""
End-to-end integration test for ManagedFuturesEngine.

Wires the full Phase A spine — synthetic .scid file + binary mock DTC server
+ all five core layers (data / strategy / risk / state / execution) — and
verifies a breakout pattern flows from candle-close through to a filled
ORDER_UPDATE recorded in the local StateStore.

These tests use asyncio directly (no pytest-asyncio plugin in omni's venv).
A standalone smoke harness at the bottom of the file makes them runnable
via `python -m tests.integration.test_managed_futures_engine`.
"""
from __future__ import annotations

import asyncio
import struct
from datetime import datetime, timedelta
from pathlib import Path

from tests.mocks.mock_dtc_server import (
    AccountBalanceFixture,
    BinaryMockDTCServer,
    PositionFixture,
)
from trading_bot.core.data import CandleManager
from trading_bot.core.data.scid_parser import (
    SCID_HEADER_SIZE,
    SCID_RECORD_FORMAT,
    SCID_RECORD_SIZE,
)
from trading_bot.core.execution import dtc_protocol as proto
from trading_bot.core.execution.dtc_client import DTCClient
from trading_bot.core.risk import RiskManager
from trading_bot.core.state import Reconciler, StateStore
from trading_bot.core.strategy import AtrBreakoutStrategy
from trading_bot.engines import InstrumentMeta, ManagedFuturesEngine


# ── Synthetic SCID helpers ────────────────────────────────────────────────
_BASE = datetime(1899, 12, 30)


def _us(when: datetime) -> int:
    delta = when - _BASE
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000


def _pack_record(when: datetime, *, o: float, h: float, l: float, c: float, vol: int = 1) -> bytes:
    return struct.pack(SCID_RECORD_FORMAT, _us(when), o, h, l, c, vol, 1, 0, 0)


def _write_scid(path: Path, ticks: list[bytes]) -> None:
    with open(path, "wb") as f:
        f.write(b"\x00" * SCID_HEADER_SIZE)
        for t in ticks:
            f.write(t)


def _append_ticks(path: Path, ticks: list[bytes]) -> None:
    with open(path, "ab") as f:
        for t in ticks:
            f.write(t)


# ── Engine factory for tests ──────────────────────────────────────────────
async def _make_engine(
    *,
    tmp_path: Path,
    scid_filename: str,
    mock_port: int,
    breakout_lookback: int = 3,
    ma_period: int = 3,
    atr_period: int = 2,
) -> tuple[ManagedFuturesEngine, StateStore]:
    state = StateStore(tmp_path / "state.db").open()
    candle_mgr = CandleManager(scid_dir=tmp_path, timeframe_minutes=1)
    strategy = AtrBreakoutStrategy(
        breakout_lookback=breakout_lookback,
        ma_period=ma_period,
        atr_period=atr_period,
    )
    risk = RiskManager()  # default policy ($1080, 1.5% per trade, etc.)
    reconciler = Reconciler()
    dtc = DTCClient(host="127.0.0.1", port=mock_port)
    symbol = scid_filename.removesuffix(".scid")

    engine = ManagedFuturesEngine(
        symbols=[symbol],
        instruments={
            symbol: InstrumentMeta(
                symbol=symbol, exchange="CME",
                # In integration tests we use the same string for all
                # three identifiers — the synthetic .scid file is
                # named {symbol}.scid and the mock accepts any
                # symbol the engine sends.
                scid_filename=symbol,
                dtc_symbol=symbol,
                tick_size=0.25, tick_value=1.25, per_contract_margin=100.0,
            ),
        },
        candle_manager=candle_mgr,
        strategy=strategy,
        risk=risk,
        state=state,
        reconciler=reconciler,
        dtc_client=dtc,
        trade_account="E8933",
        client_name="TEST_BOT",
        trade_mode=proto.TRADE_MODE_DEMO,
        tick_interval_seconds=0.02,
        reconciliation_interval_seconds=60.0,
        seed_timeout_seconds=0.3,
    )
    return engine, state


# ── Test 1: Engine boot seeds account state from broker ──────────────────
async def smoke_engine_seeds_account_state_on_start() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        # Need a scid file just for backfill — empty file is fine
        _write_scid(tmp_path / "MESM26.scid", [])

        mock = BinaryMockDTCServer(host="127.0.0.1", port=0)
        await mock.start()
        try:
            mock.set_account_balance(
                cash_balance=1080.0,
                balance_available=980.0,
                securities_value=0.0,
                margin_requirement=100.0,
            )
            engine, state = await _make_engine(
                tmp_path=tmp_path, scid_filename="MESM26.scid",
                mock_port=mock.actual_port,
            )

            await engine.start()
            try:
                # Seeded account state from ACCOUNT_BALANCE_UPDATE
                acct = engine.account_state
                assert acct is not None, "engine should have account state after start"
                assert acct.nlv == 1080.0
                assert acct.starting_nlv == 1080.0
                assert acct.open_positions_margin == 100.0
                assert acct.open_position_count == 0   # no positions seeded

                # Mock saw all three seed requests
                assert len(mock.received_account_balance_requests) == 1
                assert len(mock.received_positions_requests) == 1
                assert len(mock.received_open_orders_requests) == 1
                print("  smoke #1: engine seeds account state OK")
            finally:
                await engine.stop()
                state.close()
        finally:
            await mock.stop()


# ── Test 2: Strategy fires on breakout, order is submitted + filled ──────
async def smoke_breakout_fills_through_full_pipeline() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        scid_path = tmp_path / "MESM26.scid"

        # 6 flat minutes of history with TIGHT ranges so the resulting ATR
        # produces a stop that fits under the 1.5% NLV cap on $1080. Flat
        # bars: range = 0.2 (TR ≈ 0.2). Breakout bar: TR ≈ 0.5.
        # ATR(2) ≈ 0.35; stop = close - 2*0.35 = 0.7 pts ≈ 2.8 ticks ≈ $3.50
        # → 0.32% of $1080 NLV (well under 1.5% cap).
        flat_ticks = [
            _pack_record(
                datetime(2026, 4, 27, 12, m, 0),
                o=100.0, h=100.1, l=99.9, c=100.0, vol=1,
            )
            for m in range(6)
        ]
        _write_scid(scid_path, flat_ticks)

        mock = BinaryMockDTCServer(host="127.0.0.1", port=0)
        await mock.start()
        try:
            engine, state = await _make_engine(
                tmp_path=tmp_path, scid_filename="MESM26.scid",
                mock_port=mock.actual_port,
            )
            await engine.start()
            run_task = asyncio.create_task(engine.run())
            try:
                # Append: a tight breakout candle in minute 6, then a
                # boundary-crossing tick in minute 7. The boundary tick
                # closes minute 6 → engine sees the breakout candle close
                # and fires the strategy.
                _append_ticks(scid_path, [
                    _pack_record(
                        datetime(2026, 4, 27, 12, 6, 5),
                        o=100.0, h=100.5, l=100.0, c=100.4, vol=5,
                    ),
                    _pack_record(
                        datetime(2026, 4, 27, 12, 7, 5),
                        o=100.4, h=100.5, l=100.3, c=100.4, vol=1,
                    ),
                ])

                # Wait for the engine loop to detect the new candle, fire
                # the strategy, and submit the order. Up to 1s.
                deadline = asyncio.get_running_loop().time() + 1.0
                while asyncio.get_running_loop().time() < deadline:
                    if mock.received_orders:
                        break
                    await asyncio.sleep(0.02)

                assert len(mock.received_orders) == 1, (
                    f"expected 1 order, got {len(mock.received_orders)}"
                )

                # Wait for the ORDER_UPDATE round-trip to update local state.
                deadline = asyncio.get_running_loop().time() + 1.0
                while asyncio.get_running_loop().time() < deadline:
                    active = state.get_active_orders()
                    if not active:
                        break
                    await asyncio.sleep(0.02)

                # Find the order by listing all orders (active + filled)
                all_active = state.get_active_orders()
                assert len(all_active) == 0, f"order should have filled, still active: {all_active}"

                # Inspect the recorded order (by querying state directly via SQL)
                rows = state.conn.execute(
                    "SELECT client_order_id, status, fill_price, fill_quantity "
                    "FROM orders ORDER BY submitted_at DESC LIMIT 1"
                ).fetchone()
                assert rows is not None
                assert rows["status"] == "FILLED"
                assert rows["fill_price"] is not None and rows["fill_price"] > 0
                assert rows["fill_quantity"] == 1.0

                print(
                    f"  smoke #2: breakout pipeline OK — order "
                    f"{rows['client_order_id'][:50]}... filled at "
                    f"{rows['fill_price']}"
                )
            finally:
                await engine.stop()
                run_task.cancel()
                try:
                    await run_task
                except asyncio.CancelledError:
                    pass
                state.close()
        finally:
            await mock.stop()


# ── Test 3: Risk-rejected intent does NOT submit an order ────────────────
async def smoke_risk_rejection_blocks_submission() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        scid_path = tmp_path / "MESM26.scid"
        flat_ticks = [
            _pack_record(
                datetime(2026, 4, 27, 12, m, 0),
                o=100, h=101, l=99, c=100, vol=1,
            )
            for m in range(6)
        ]
        _write_scid(scid_path, flat_ticks)

        mock = BinaryMockDTCServer(host="127.0.0.1", port=0)
        await mock.start()
        try:
            # Set account NLV BELOW the floor — risk engine rejects every entry
            mock.set_account_balance(
                cash_balance=200.0,        # below $300 nlv_floor
                balance_available=100.0,
                margin_requirement=100.0,
            )
            engine, state = await _make_engine(
                tmp_path=tmp_path, scid_filename="MESM26.scid",
                mock_port=mock.actual_port,
            )

            rejected: list[tuple] = []
            engine.on_intent_rejected = lambda i, v: rejected.append((i, v))

            await engine.start()
            run_task = asyncio.create_task(engine.run())
            try:
                _append_ticks(scid_path, [
                    _pack_record(
                        datetime(2026, 4, 27, 12, 6, 5),
                        o=100, h=110, l=100, c=109, vol=5,
                    ),
                    _pack_record(
                        datetime(2026, 4, 27, 12, 7, 5),
                        o=109, h=110, l=108, c=109, vol=1,
                    ),
                ])

                # Wait long enough for the breakout candle to close + the
                # rejection callback to fire.
                deadline = asyncio.get_running_loop().time() + 1.0
                while asyncio.get_running_loop().time() < deadline:
                    if rejected:
                        break
                    await asyncio.sleep(0.02)

                assert len(rejected) >= 1, "risk should have rejected the breakout intent"
                _intent, verdict = rejected[0]
                assert verdict.approved is False
                assert any("nlv_floor" in r for r in verdict.reasons)
                # No order should have hit the wire
                assert len(mock.received_orders) == 0, (
                    f"risk rejected, but {len(mock.received_orders)} orders reached the broker"
                )
                print("  smoke #3: risk rejection blocks submission OK")
            finally:
                await engine.stop()
                run_task.cancel()
                try:
                    await run_task
                except asyncio.CancelledError:
                    pass
                state.close()
        finally:
            await mock.stop()


# ── Test 4: Reconciliation detects drift ──────────────────────────────────
async def smoke_reconciliation_reports_position_drift() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        _write_scid(tmp_path / "MESM26.scid", [])

        mock = BinaryMockDTCServer(host="127.0.0.1", port=0)
        await mock.start()
        try:
            # Broker says we hold 2 contracts long
            mock.set_positions([
                PositionFixture(
                    symbol="MESM26", exchange="CME",
                    quantity=2.0, average_price=5000.0,
                    margin_requirement=200.0,
                ),
            ])
            engine, state = await _make_engine(
                tmp_path=tmp_path, scid_filename="MESM26.scid",
                mock_port=mock.actual_port,
            )

            drifts: list = []
            engine.on_drift = lambda r: drifts.append(r)
            engine.reconciliation_interval_seconds = 0.05    # fast

            await engine.start()
            # Local state: NO positions recorded — broker says 2.
            # Reconciler should report a remote_only drift.

            # Manually drive reconciliation (don't need full run loop)
            report = engine._run_reconciliation()
            assert report.has_drift is True
            assert len(report.position_drifts) == 1
            d = report.position_drifts[0]
            assert d.symbol == "MESM26"
            assert d.kind == "remote_only"
            assert d.remote_qty == 2.0
            assert d.local_qty == 0.0
            print("  smoke #4: reconciliation drift detection OK")
            await engine.stop()
            state.close()
        finally:
            await mock.stop()


# ── Smoke harness ─────────────────────────────────────────────────────────
async def _all() -> None:
    print("--- engine integration smokes ---")
    await smoke_engine_seeds_account_state_on_start()
    await smoke_breakout_fills_through_full_pipeline()
    await smoke_risk_rejection_blocks_submission()
    await smoke_reconciliation_reports_position_drift()
    print("all 4 engine smokes PASS")


if __name__ == "__main__":
    asyncio.run(_all())
