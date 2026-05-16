"""
Runtime configuration for the supervisor — composes instrument metadata
with strategy-side and DTC-side identifiers.

Sierra Chart's three identifiers for the same contract:
  - strategy_symbol  e.g. "MESM26"          (used by strategies + state store)
  - dtc_symbol       e.g. "MESM26-CME"      (used in DTC SUBMIT_NEW_SINGLE_ORDER)
  - scid_basename    e.g. "MESM26_FUT_CME"  (used for the .scid filename)

These three are NOT always derivable from one another — Sierra installs
vary in their naming convention. The supervisor takes them explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentRuntime:
    """All identifiers + calibration the engine needs for one instrument.

    The engine's `InstrumentMeta` holds the strategy-side `symbol` (used as
    dict key + StrategyContext field) and `exchange`. The CandleManager
    needs the `.scid` filename. The DTC client needs the wire `symbol` +
    `exchange`. This dataclass holds all three so the supervisor can wire
    them in one place."""

    strategy_symbol: str    # e.g. "MESM26"
    dtc_symbol: str         # e.g. "MESM26-CME"
    exchange: str           # e.g. "CME"
    scid_basename: str      # filename without .scid extension
    tick_size: float
    tick_value: float
    per_contract_margin: float
    # Per-contract round-trip commission. SIM mode: 0.0 (matches Sierra
    # Trade Sim's zero-commission default — verified by COO Gemini
    # 2026-04-27, no commission setting found in any Sierra config file).
    # LIVE mode: set to broker rate. EdgeClear MES verified April 2026:
    #   $0.37 commission + $0.22 exchange/NFA + $0.10 Rithmic = $0.69/side
    #   = $1.38/RT. Same for all CME micros (MNQ, MYM, M2K).
    # When transitioning to live, BOTH this value AND Sierra's commission
    # config get set to the matching rate — keeps NLV reconciliation clean.
    round_trip_commission: float = 0.0


# Known instruments — extend as we onboard more contracts. Per-month
# (e.g. M26 = June 2026, U26 = September 2026) identifiers will rotate
# at front-month roll dates; until that automation lands, edit this dict
# manually or pass overrides via the supervisor CLI.
KNOWN_INSTRUMENTS: dict[str, InstrumentRuntime] = {
    "MESM26": InstrumentRuntime(
        strategy_symbol="MESM26",
        dtc_symbol="MESM26-CME",
        exchange="CME",
        scid_basename="MESM26_FUT_CME",
        tick_size=0.25,
        tick_value=1.25,
        per_contract_margin=100.0,    # placeholder — verify against EdgeClear
        round_trip_commission=0.0,    # sim default; live = 1.38 per Gemini
    ),
    "MGCM26": InstrumentRuntime(
        strategy_symbol="MGCM26",
        dtc_symbol="MGCM26-CME",
        exchange="CME",
        scid_basename="MGCM26_FUT_CME",
        tick_size=0.10,
        tick_value=1.00,
        per_contract_margin=100.0,    # placeholder
        round_trip_commission=0.0,    # sim default; live rate TBD
    ),
    "GBPUSD": InstrumentRuntime(
        strategy_symbol="GBPUSD",
        dtc_symbol="GBPUSD",
        exchange="ICMarketsSC-Demo",
        scid_basename="GBPUSD",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        round_trip_commission=0.0,
    ),
}
