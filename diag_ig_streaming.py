import asyncio
import logging
from trading_bot.core.execution.ig_broker_adapter import IGBrokerAdapter, IGConfig, IGSymbolSpec

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(name)s | %(message)s')

async def main():
    config = IGConfig.from_secrets()
    symbol_map = {
        'GBPUSD': IGSymbolSpec(logical_symbol='GBPUSD', epic='CS.D.GBPUSD.MINI.IP')
    }
    adapter = IGBrokerAdapter(config=config, symbol_map=symbol_map, emit_stream_ticks=True)
    
    await adapter.connect()
    await adapter.subscribe_market_data(symbol='GBPUSD')
    
    try:
        async for event in adapter.events():
            print(event)
    except asyncio.CancelledError:
        pass
    finally:
        await adapter.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
