"""Lean IG .TODAY.IP epic confirmation (throttled — dodges API rate limit).

Confirms the 4 FX spread-bet DFB epics are streamable AND captures the
dealing rules needed to verify sizing parity vs the current .MINI.IP specs
before swapping. 4 calls, 2.5s apart. Read-only. Run on VPS where creds live:

    $env:KATE_SECRETS_PATH="C:\\models\\TradingBot\\.mcp-brain\\config\\secrets.json"
    python diag_ig_today_epics.py
"""
import asyncio

from trading_bot.core.execution.ig_broker_adapter import IGBrokerAdapter, IGConfig

EPICS = [
    "CS.D.GBPUSD.TODAY.IP",
    "CS.D.EURUSD.TODAY.IP",
    "CS.D.AUDUSD.TODAY.IP",
    "CS.D.EURGBP.TODAY.IP",
]


async def main() -> None:
    config = IGConfig.from_secrets()
    adapter = IGBrokerAdapter(config=config, symbol_map={}, emit_stream_ticks=False)
    await adapter.connect()
    print(f"\n=== {config.base_url}  account={config.active_account_id} ===\n")
    for i, epic in enumerate(EPICS):
        if i:
            await asyncio.sleep(2.5)
        try:
            body = await adapter._request("GET", f"/markets/{epic}", version=3)
        except Exception as exc:  # noqa: BLE001
            print(f"{epic}: FAILED {exc}")
            continue
        inst = body.get("instrument") or {}
        snap = body.get("snapshot") or {}
        rules = body.get("dealingRules") or {}

        def _rule(name: str) -> str:
            r = rules.get(name) or {}
            return f"{r.get('value')}{r.get('unit', '')}"

        pip_value = inst.get("valueOfOnePip")
        one_pip = inst.get("onePipMeans")
        lot = inst.get("lotSize")
        ccy = inst.get("currencies") or []
        ccy_codes = ",".join(c.get("code", "") for c in ccy)
        print(f"{epic}")
        print(f"    streaming={inst.get('streamingPricesAvailable')}  "
              f"status={snap.get('marketStatus')}  expiry={inst.get('expiry')}  type={inst.get('type')}")
        print(f"    lotSize={lot}  onePipMeans={one_pip}  valueOfOnePip={pip_value}  currencies={ccy_codes}")
        print(f"    minDealSize={_rule('minDealSize')}  "
              f"minNormalStop={_rule('minNormalStopOrLimitDistance')}  "
              f"minCtrlRiskStop={_rule('minControlledRiskStopDistance')}")
        print(f"    marginFactor={inst.get('marginFactor')}{inst.get('marginFactorUnit', '')}\n")
    await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
