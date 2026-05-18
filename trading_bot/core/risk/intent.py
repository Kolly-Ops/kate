"""
TradeIntent — the contract between strategy and risk.

Strategies generate TradeIntents. The risk engine evaluates each against
account state + policy and returns a RiskVerdict. Only approved intents
reach the executor. This separation is non-negotiable per the architecture
doc — it's the structural fix for KATE's "strategy directly opens
positions" pattern.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from trading_bot.core.execution import dtc_protocol as proto


@dataclass(frozen=True)
class TradeIntent:
    """One proposed trade. Strategy → risk → execution.

    All prices are absolute (not in ticks). Stop loss is REQUIRED for entries
    that the risk engine can size — without it, per-trade-risk cannot be
    computed and the intent is rejected.
    """

    intent_id: str
    strategy_name: str
    symbol: str
    exchange: str
    side: int              # proto.BUY | proto.SELL
    quantity: float        # contracts (float for fractional sizing in scaled-out micros)
    order_type: int        # proto.ORDER_TYPE_MARKET | LIMIT | STOP | STOP_LIMIT

    # Per-instrument calibration — sourced from config/instruments.json
    tick_size: float       # e.g. 0.25 for MES
    tick_value: float      # $ per tick per contract — e.g. 1.25 for MES

    price: float = 0.0           # absolute price (limit/stop orders); 0.0 for market
    stop_loss: Optional[float] = None   # absolute price; required for entries
    take_profit: Optional[float] = None # absolute price; optional

    # Bar-close price at the moment the strategy fired this intent. Used
    # by adapters that publish slippage telemetry — fill slippage =
    # fill_price - signal_close_price (sign-flipped for SELLs). For
    # market-order strategies, `price` is 0.0 and this carries the bar
    # close instead. For limit/stop strategies, `price` is the order
    # price and this should be set separately to the actual bar close.
    # Defaults to None — adapters that care must fall back to `price`
    # when None. ORBStrategy populates this explicitly from
    # `StrategyContext.candle.close` post the Codex 2026-05-18 wiring
    # ask. See handoffs/2026-05-18-codex-to-team-REVIEW-RESPONSE-
    # claude-ninja-skeleton-and-bar-publisher.md §3.
    signal_close_price: Optional[float] = None

    # Margin the broker will hold for this position (per-contract). Caller
    # supplies it because broker margin tables are external to this module.
    per_contract_margin: float = 0.0

    # Per-contract round-trip commission. RiskManager adds this to gross
    # price-move risk so the per-trade-risk gate evaluates TOTAL cost, not
    # just slippage. Strategies populate from StrategyContext (which gets
    # it from InstrumentMeta). Defaults to 0.0 to match Sierra Trade Sim's
    # zero-commission fills; for live mode set to the broker's actual rate
    # (EdgeClear MES = $1.38/RT verified April 2026).
    round_trip_commission: float = 0.0

    reason: str = ""             # free-text strategy rationale
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.side not in (proto.BUY, proto.SELL):
            raise ValueError(f"invalid side: {self.side}")
        if self.quantity <= 0:
            raise ValueError(f"quantity must be > 0, got {self.quantity}")
        if self.tick_size <= 0:
            raise ValueError(f"tick_size must be > 0, got {self.tick_size}")
        if self.tick_value <= 0:
            raise ValueError(f"tick_value must be > 0, got {self.tick_value}")
