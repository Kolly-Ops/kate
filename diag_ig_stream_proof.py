"""Proof: live streaming ticks via the CS.D.GBPUSD.TODAY.IP spread-bet epic.

The previous smokes failed at SUBSCRIPTION even with the Lightstreamer
connection up, because the epic wasn't streamable on Z6BHQ1. This subscribes
with the confirmed .TODAY.IP epic and collects live ticks for a bounded
window, then exits. Read-only, no orders. Run on VPS during FX hours:

    $env:KATE_SECRETS_PATH="C:\\models\\TradingBot\\.mcp-brain\\config\\secrets.json"
    python diag_ig_stream_proof.py
"""
import asyncio

from trading_bot.core.execution.ig_broker_adapter import (
    IGBrokerAdapter,
    IGConfig,
    IGSymbolSpec,
)
from trading_bot.core.execution.broker_adapter import BrokerEventKind

WINDOW_SECONDS = 25
EPIC = "CS.D.GBPUSD.TODAY.IP"


async def main() -> None:
    config = IGConfig.from_secrets()
    symbol_map = {
        "GBPUSD": IGSymbolSpec(
            logical_symbol="GBPUSD",
            epic=EPIC,
            quantity_per_lot=10.0,
            pip_decimal_position=4,
        )
    }
    adapter = IGBrokerAdapter(config=config, symbol_map=symbol_map, emit_stream_ticks=True)
    await adapter.connect()
    await adapter.subscribe_market_data(symbol="GBPUSD")
    print(f"\n=== subscribed GBPUSD via {EPIC} — collecting ticks for {WINDOW_SECONDS}s ===\n")

    ticks = 0
    samples: list[str] = []

    async def _collect() -> None:
        nonlocal ticks
        async for event in adapter.events():
            if event.kind == BrokerEventKind.MARKET_DATA_TICK and event.tick is not None:
                ticks += 1
                if len(samples) < 6:
                    t = event.tick
                    samples.append(f"bid={t.bid} ask={t.ask}")

    try:
        await asyncio.wait_for(_collect(), timeout=WINDOW_SECONDS)
    except asyncio.TimeoutError:
        pass

    print(f"RESULT: {ticks} market-data tick(s) in {WINDOW_SECONDS}s")
    for s in samples:
        print(f"    {s}")
    print("\nVERDICT:", "STREAMING WORKS" if ticks > 0 else "NO TICKS - still blocked")
    await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
