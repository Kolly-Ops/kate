import asyncio
import datetime as dt
import sys
from pathlib import Path

KATE_ROOT = Path(r"C:\models\TradingBot")
if str(KATE_ROOT) not in sys.path:
    sys.path.insert(0, str(KATE_ROOT))

from trading_bot.core.execution.ninja_messages import MsgType, SignalPayload
from trading_bot.core.execution.ninja_transport import NinjaBridgeServer

async def wait_for_open():
    print("Auto-trigger is active. Waiting for CME open at 22:00 UK time...", flush=True)
    while True:
        now = dt.datetime.now()
        # 22:00 local time
        if now.hour == 22 and now.minute >= 0:
            break
        await asyncio.sleep(5)
    
    print("\n[TIME] 22:00 reached! Waiting 15 seconds for initial market spreads to settle...", flush=True)
    await asyncio.sleep(15)

async def main():
    # Start the bridge server so NT can stay connected
    server = NinjaBridgeServer(host="127.0.0.1", port=9876, secret=b"change-me-local-only")
    await server.start()
    print("[LISTEN] Auto-Trigger server listening on 127.0.0.1:9876...")
    print("Make sure NinjaTrader Strategy is ENABLED so it connects to us!", flush=True)
    
    # Run the wait loop
    await wait_for_open()
    
    # Build payload
    payload = SignalPayload(
        intent_id=f"auto-smoke-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')}",
        timestamp=dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        symbol="MESM26",
        nt_symbol="MES 06-26",
        side="BUY",
        quantity=1,
        atm_template="KATE_MES_ORB_BASE",
        stop_price=4000.0,
        target_price=6000.0,
        signal_close_price=5000.0,
    )
    
    try:
        seq = await server.send(MsgType.SIGNAL, payload)
        print(f"[SUCCESS] Fired auto BUY signal. Seq: {seq}")
        print("Check your NinjaTrader Chart to see if the trade filled and the ATM bracket attached!", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to send signal: {e}")
        
    print("Keeping server alive for 60 seconds to catch any responses...", flush=True)
    for _ in range(60):
        try:
            envelope = await asyncio.wait_for(server.receive(), timeout=1.0)
            print(f"[RECV] seq={envelope.sequence} type={envelope.msg_type} payload={envelope.payload}", flush=True)
        except asyncio.TimeoutError:
            pass
            
    await server.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAuto-trigger cancelled.")
