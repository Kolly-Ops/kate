"""IG epic + streaming discovery diagnostic.

Purpose (2026-06-10): IG support (Jeremy) confirmed there is NO separate
streaming entitlement — an API key grants both REST and STREAMING. The
PRICE:{accountId}:{epic} + dataAdapter="Pricing" shape we already use is
correct. The remaining cause of our streaming rejections is the EPIC:

    "you will need to use the correct epic that belongs to account type.
     The epics between CFD and Spread betting differ, and the epics may
     also differ between PROD and DEMO."

Our hardcoded CS.D.<pair>.MINI.IP epics pass REST /markets/{epic} but are
not necessarily streamable on the spread-bet account (Z6BHQ1). IG exposes
`instrument.streamingPricesAvailable` on /markets/{epic} — the definitive
signal. This script:

  1. Authenticates and switches to the configured active account.
  2. Prints every account + its accountType (CFD vs SPREADBET).
  3. For each FX pair, searches /markets and, for each currency-pair epic
     returned, fetches /markets/{epic} and reports:
        epic | instrumentType | expiry | marketStatus | streamingPricesAvailable
  4. Flags the epic(s) with streamingPricesAvailable == true — those are
     the ones to put in supervisor/main.py for this account.

READ-ONLY. No orders. Demo by default (IGConfig base_url). Run where
secrets.json with valid ig creds exists (VPS, or drop secrets.json locally):

    python diag_ig_epics.py
"""
import asyncio
import logging

from trading_bot.core.execution.ig_broker_adapter import IGBrokerAdapter, IGConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)

PAIRS = {
    "GBPUSD": ["GBP/USD", "GBPUSD"],
    "EURUSD": ["EUR/USD", "EURUSD"],
    "AUDUSD": ["AUD/USD", "AUDUSD"],
    "EURGBP": ["EUR/GBP", "EURGBP"],
}


async def _search_epics(adapter: IGBrokerAdapter, terms: list[str]) -> list[dict]:
    seen: dict[str, dict] = {}
    for term in terms:
        try:
            body = await adapter._request("GET", f"/markets?searchTerm={term}", version=1)
        except Exception as exc:  # noqa: BLE001 — diagnostic, surface everything
            print(f"    search '{term}' failed: {exc}")
            continue
        for m in body.get("markets") or []:
            epic = m.get("epic", "")
            if epic and epic not in seen:
                seen[epic] = m
    return list(seen.values())


async def _epic_detail(adapter: IGBrokerAdapter, epic: str) -> dict:
    body = await adapter._request("GET", f"/markets/{epic}", version=3)
    instrument = body.get("instrument") or {}
    snapshot = body.get("snapshot") or {}
    return {
        "epic": epic,
        "type": instrument.get("type") or instrument.get("instrumentType") or "?",
        "expiry": instrument.get("expiry") or "-",
        "marketStatus": snapshot.get("marketStatus") or "?",
        "streaming": instrument.get("streamingPricesAvailable"),
        "name": instrument.get("name") or instrument.get("marketName") or "",
    }


async def main() -> None:
    config = IGConfig.from_secrets()
    print(f"\n=== IG endpoint: {config.base_url}  active_account_id: {config.active_account_id} ===")
    adapter = IGBrokerAdapter(config=config, symbol_map={}, emit_stream_ticks=False)
    await adapter.connect()

    # 1. accounts + types
    try:
        accounts = await adapter._request("GET", "/accounts", version=1)
        print("\n--- accounts ---")
        for a in accounts.get("accounts") or []:
            print(
                f"  {a.get('accountId')}  type={a.get('accountType')}  "
                f"name={a.get('accountName')}  preferred={a.get('preferred')}  "
                f"status={a.get('status')}"
            )
    except Exception as exc:  # noqa: BLE001
        print(f"  /accounts failed: {exc}")

    # 2. per-pair epic discovery with streamingPricesAvailable
    winners: dict[str, list[str]] = {}
    for logical, terms in PAIRS.items():
        print(f"\n--- {logical} (search: {', '.join(terms)}) ---")
        markets = await _search_epics(adapter, terms)
        # only currency-pair markets
        fx = [m for m in markets if "CURRENC" in (m.get("instrumentType") or "").upper()]
        candidates = fx or markets
        print(f"  {len(markets)} markets, {len(fx)} currency-pair")
        for m in candidates:
            epic = m.get("epic", "")
            try:
                d = await _epic_detail(adapter, epic)
            except Exception as exc:  # noqa: BLE001
                print(f"    {epic}: detail failed: {exc}")
                continue
            flag = "  <== STREAMABLE" if d["streaming"] is True else ""
            print(
                f"    {d['epic']:<28} type={d['type']:<12} expiry={d['expiry']:<8} "
                f"status={d['marketStatus']:<12} streaming={d['streaming']}{flag}"
            )
            if d["streaming"] is True and "CURRENC" in str(d["type"]).upper():
                winners.setdefault(logical, []).append(d["epic"])

    print("\n=== STREAMABLE FX epics for this account (use these in supervisor/main.py) ===")
    if winners:
        for logical, epics in winners.items():
            print(f"  {logical}: {epics}")
    else:
        print("  NONE found with streamingPricesAvailable=true — escalate to IG with this output.")

    await adapter.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
