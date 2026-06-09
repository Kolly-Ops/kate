"""
Runtime configuration for the supervisor — composes instrument metadata
with strategy-side and DTC-side identifiers.

Sierra Chart's three identifiers for the same contract:
  - strategy_symbol  e.g. "MESU26"          (used by strategies + state store)
  - dtc_symbol       e.g. "MESU26-CME"      (used in DTC SUBMIT_NEW_SINGLE_ORDER)
  - scid_basename    e.g. "MESU26_FUT_CME"  (used for the .scid filename)

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
    `exchange`. The NinjaTrader bridge needs the NT display form (e.g.
    "MES 09-26"). This dataclass holds them all so the supervisor wires
    them in one place per broker."""

    strategy_symbol: str    # e.g. "MESU26"
    dtc_symbol: str         # e.g. "MESU26-CME"
    exchange: str           # e.g. "CME"
    scid_basename: str      # filename without .scid extension
    tick_size: float
    tick_value: float
    per_contract_margin: float
    # NT display form — used by NinjaBrokerAdapter when constructing the
    # BrokerSymbolSpec for --broker ninja. Examples: "MES 09-26",
    # "M2K 06-26". NinjaTrader maps this to its internal MasterInstrument
    # at signal time. Verified live on Kate Host VPS 2026-05-18 (Gemini's
    # M2K live-data evidence handoff confirms both forms work on Tradovate
    # sim feed). Default empty so legacy DTC/MT5-only instruments don't
    # break — supervisor errors when --broker ninja meets an empty value.
    nt_symbol: str = ""
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
    "MESU26": InstrumentRuntime(
        strategy_symbol="MESU26",
        dtc_symbol="MESU26-CME",
        nt_symbol="MES 09-26",
        exchange="CME",
        scid_basename="MESU26_FUT_CME",
        tick_size=0.25,
        tick_value=1.25,
        per_contract_margin=100.0,    # placeholder — verify against EdgeClear
        round_trip_commission=0.0,    # sim default; live = 1.38 per Gemini
    ),
    "MGCM26": InstrumentRuntime(
        strategy_symbol="MGCM26",
        dtc_symbol="MGCM26-CME",
        nt_symbol="MGC 06-26",
        exchange="CME",
        scid_basename="MGCM26_FUT_CME",
        tick_size=0.10,
        tick_value=1.00,
        per_contract_margin=100.0,    # placeholder
        round_trip_commission=0.0,    # sim default; live rate TBD
    ),
    # M2KM26 — Micro E-mini Russell 2000 Futures, June 2026.
    # Graduated from Phase 2 → Phase 1 per CEO directive 2026-05-18 after
    # Gemini's live-data evidence handoff confirmed M2K JUN26 actively
    # ticking on the Kate Host VPS Tradovate sim feed (NinjaTrader
    # MasterInstrument ID 699839150753736; tick 0.10; point value $5).
    # Phase 1 trading awaits Codex's NinjaScript multi-series expansion
    # (current bar publisher is single-chart). Registry entry lands now
    # so symbol mapping is ready when the C# side ships.
    "M2KM26": InstrumentRuntime(
        strategy_symbol="M2KM26",
        dtc_symbol="M2KM26-CME",
        nt_symbol="M2K 06-26",
        exchange="CME",
        scid_basename="M2KM26_FUT_CME",
        tick_size=0.10,
        # tick_value = tick_size × point_value = 0.10 × $5.0 = $0.50/tick.
        tick_value=0.50,
        per_contract_margin=100.0,    # placeholder — verify against EdgeClear / MFFU
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
    # EURUSD — most liquid FX pair globally; deepest book, tightest spread.
    # Added 2026-05-20 per CEO directive to broaden Front 4 setup count.
    # Same tick/pip structure as GBPUSD (4-decimal pair). Compatible with
    # FXLondonBreakoutStrategy default pip_size=0.0001 — no code change needed.
    "EURUSD": InstrumentRuntime(
        strategy_symbol="EURUSD",
        dtc_symbol="EURUSD",
        exchange="ICMarketsSC-Demo",
        scid_basename="EURUSD",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        round_trip_commission=0.0,
    ),
    # AUDUSD — Asian-session-driven (Australian data + commodities).
    # Often produces clean Asian-range setups breaking at London open.
    # Added 2026-05-20 per CEO directive. 4-decimal pip-compatible.
    "AUDUSD": InstrumentRuntime(
        strategy_symbol="AUDUSD",
        dtc_symbol="AUDUSD",
        exchange="ICMarketsSC-Demo",
        scid_basename="AUDUSD",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        round_trip_commission=0.0,
    ),
    # EURGBP — pure European cross. Classic London-session breakout pair;
    # often produces narrow Asian ranges (5-20 pips) that break cleanly
    # on London opening flow. Added 2026-05-20 per CEO directive.
    "EURGBP": InstrumentRuntime(
        strategy_symbol="EURGBP",
        dtc_symbol="EURGBP",
        exchange="ICMarketsSC-Demo",
        scid_basename="EURGBP",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        round_trip_commission=0.0,
    ),
    # USDCAD — 4-decimal pip-compatible USD pair for NY-session testing.
    # Added 2026-06-04 for CEO-directed demo-only NY breakout basket.
    "USDCAD": InstrumentRuntime(
        strategy_symbol="USDCAD",
        dtc_symbol="USDCAD",
        exchange="ICMarketsSC-Demo",
        scid_basename="USDCAD",
        tick_size=0.00001,
        tick_value=1.0,
        per_contract_margin=0.0,
        round_trip_commission=0.0,
    ),
}
