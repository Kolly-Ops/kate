"""
bar_listener.py — minimal bridge listener for first NT → Python BAR test.

Filed 2026-05-18 alongside Option A architecture sprint. Diagnostic-only;
not part of the supervisor or audit_live production path. Use to confirm
KateBridgeStrategy.cs is publishing BAR envelopes correctly after Codex's
bar publisher additions land on the VPS.

Usage on Kate Host VPS:
    cd C:\\models\\TradingBot
    python tools\\bar_listener.py

Then in NinjaTrader: enable KateBridgeStrategy on the MES chart with
PublishBars=True, LogicalSymbol=MESU26. Watch this terminal for:
    "client connected"        — NT side connected to Python bridge
    "[hb seq=N]"              — heartbeats every 5s (connection healthy)
    "*** BAR #N ***"          — actual closed-bar envelopes from NT

Ctrl+C to stop cleanly.
"""
import asyncio
import logging

from trading_bot.core.execution.ninja_transport import NinjaBridgeServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    secret = b"change-me-local-only"  # matches KateBridgeStrategy SharedSecret default
    server = NinjaBridgeServer(host="127.0.0.1", port=9876, secret=secret)
    await server.start()
    print("\n" + "=" * 60)
    print("BRIDGE LISTENING on 127.0.0.1:9876")
    print("Waiting for KateBridgeStrategy to connect from NinjaTrader...")
    print("Ctrl+C to stop")
    print("=" * 60 + "\n")

    bar_count = 0
    while True:
        env = await server.receive()
        if env.msg_type == "bar":
            bar_count += 1
            print(f"\n*** BAR #{bar_count} *** seq={env.sequence}")
            for k in sorted(env.payload.keys()):
                print(f"    {k} = {env.payload[k]}")
        elif env.msg_type == "heartbeat":
            print(f"[hb seq={env.sequence}]", end=" ", flush=True)
        else:
            print(f"\n[{env.msg_type}] seq={env.sequence} payload={env.payload}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped cleanly.")
