"""Rithmic connectivity spike for the Kate platform pivot.

Reads credentials from environment variables and performs the smallest useful
probe:

1. Connect to Rithmic via async_rithmic.
2. List available accounts.
3. Pull an account PNL summary snapshot.
4. Resolve MES front-month contract.
5. Stream MES market data for a short window and report tick count.

No credentials are printed. No orders are submitted.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

from google.protobuf.json_format import MessageToDict

from async_rithmic import (
    BestBidOfferPresenceBits,
    DataType,
    LastTradePresenceBits,
    RithmicClient,
    SysInfraType,
)


DEFAULT_TEST_URL = "rituz00100.rithmic.com:443"


@dataclass(frozen=True)
class ProbeConfig:
    user: str
    password: str
    system_name: str
    url: str
    app_name: str
    app_version: str
    account_id: str | None
    symbol: str
    exchange: str
    seconds: int

    @classmethod
    def from_env(cls, args: argparse.Namespace) -> "ProbeConfig":
        user = args.user or os.getenv("RITHMIC_USER") or os.getenv("EDGECLEAR_RITHMIC_USER")
        password = (
            args.password
            or os.getenv("RITHMIC_PASSWORD")
            or os.getenv("EDGECLEAR_RITHMIC_PASSWORD")
        )
        missing = []
        if not user:
            missing.append("RITHMIC_USER")
        if not password:
            missing.append("RITHMIC_PASSWORD")
        if missing:
            raise SystemExit(
                "missing required Rithmic credential env vars: "
                + ", ".join(missing)
                + ". Optional aliases: EDGECLEAR_RITHMIC_USER / EDGECLEAR_RITHMIC_PASSWORD."
            )

        return cls(
            user=user,
            password=password,
            system_name=args.system_name or os.getenv("RITHMIC_SYSTEM_NAME", "Rithmic Test"),
            url=args.url or os.getenv("RITHMIC_URL", DEFAULT_TEST_URL),
            app_name=args.app_name or os.getenv("RITHMIC_APP_NAME", "kate_rithmic_probe"),
            app_version=args.app_version or os.getenv("RITHMIC_APP_VERSION", "0.1"),
            account_id=args.account_id or os.getenv("RITHMIC_ACCOUNT_ID"),
            symbol=args.symbol,
            exchange=args.exchange,
            seconds=args.seconds,
        )

    def safe_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["user"] = _mask(self.user)
        out["password"] = "***"
        return out


def _mask(value: str) -> str:
    if len(value) <= 4:
        return "***"
    return value[:2] + "***" + value[-2:]


def _to_plain(value: Any) -> Any:
    if hasattr(value, "DESCRIPTOR"):
        return MessageToDict(value, preserving_proto_field_name=True)
    if hasattr(value, "__dict__"):
        return {
            k: _to_plain(v)
            for k, v in vars(value).items()
            if not k.startswith("_")
        }
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    return value


async def run_probe(config: ProbeConfig) -> dict[str, Any]:
    ticks: list[dict[str, Any]] = []
    first_tick_at = None

    async def on_tick(data: dict[str, Any]) -> None:
        nonlocal first_tick_at
        if first_tick_at is None:
            first_tick_at = time.time()
        if data.get("data_type") == DataType.LAST_TRADE:
            if data.get("presence_bits", 0) & LastTradePresenceBits.LAST_TRADE:
                ticks.append(_tick_digest(data))
        elif data.get("data_type") == DataType.BBO:
            bits = data.get("presence_bits", 0)
            if bits & (BestBidOfferPresenceBits.BID | BestBidOfferPresenceBits.ASK):
                ticks.append(_tick_digest(data))

    client = RithmicClient(
        user=config.user,
        password=config.password,
        system_name=config.system_name,
        app_name=config.app_name,
        app_version=config.app_version,
        url=config.url,
    )
    client.on_tick += on_tick

    connected = False
    try:
        await client.connect(
            plants=[
                SysInfraType.ORDER_PLANT,
                SysInfraType.PNL_PLANT,
                SysInfraType.TICKER_PLANT,
            ]
        )
        connected = True

        accounts = [_to_plain(a) for a in client.accounts]
        account_id = config.account_id
        if not account_id and client.accounts:
            account_id = client.accounts[0].account_id

        account_summary = []
        if account_id:
            account_summary = [_to_plain(x) for x in await client.list_account_summary(account_id=account_id)]

        contract = await client.get_front_month_contract(config.symbol, config.exchange)
        data_type = int(DataType.LAST_TRADE) | int(DataType.BBO)
        await client.subscribe_to_market_data(contract, config.exchange, data_type)
        await asyncio.sleep(config.seconds)
        await client.unsubscribe_from_market_data(contract, config.exchange, data_type)

        return {
            "ok": True,
            "connected": connected,
            "config": config.safe_dict(),
            "accounts": accounts,
            "selected_account_id": account_id,
            "account_summary": account_summary,
            "front_month_contract": contract,
            "tick_count": len(ticks),
            "first_tick_latency_s": None if first_tick_at is None else round(first_tick_at - time.time() + config.seconds, 3),
            "sample_ticks": ticks[:5],
        }
    finally:
        if connected:
            await client.disconnect()


def _tick_digest(data: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "datetime",
        "data_type",
        "symbol",
        "exchange",
        "trade_price",
        "trade_size",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    ]
    return {
        k: (str(v) if k == "datetime" else v)
        for k, v in data.items()
        if k in fields
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--user", default=None)
    p.add_argument("--password", default=None)
    p.add_argument("--system-name", default=None)
    p.add_argument("--url", default=None)
    p.add_argument("--app-name", default=None)
    p.add_argument("--app-version", default=None)
    p.add_argument("--account-id", default=None)
    p.add_argument("--symbol", default="MES")
    p.add_argument("--exchange", default="CME")
    p.add_argument("--seconds", type=int, default=30)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    config = ProbeConfig.from_env(args)
    try:
        result = asyncio.run(run_probe(config))
    except Exception as exc:
        result = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "config": config.safe_dict(),
        }
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
