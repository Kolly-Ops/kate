# Trading Bot

Standalone deterministic trading bot built on Sierra Chart + DTC binary protocol + Teton CME routing into EdgeClear account E8933.

**Status:** Phase A build, started 2026-04-27. DTC sim path verified PASS 2026-04-27 00:13 UK on MESM26.

## Constraints (CEO-ratified 2026-04-25)

- **No ML in live signal path.** Deterministic, rule-based strategies only. ML is permitted in review/analysis layer only.
- **Capital baseline $1,080.** Paper, sim, live-disabled, and live all run at the same starting NLV the bot will use in production.

See `decisions/2026-04-25-trading-policies-ml-and-capital.md` in the Omni repo.

## Layout

```
trading_bot/
├── core/
│   ├── execution/
│   │   └── dtc_protocol.py      # Reference, drop-in from KATE
│   ├── data/
│   │   ├── scid_parser.py       # Sierra .scid binary parser
│   │   └── scid_tailer.py       # Real-time .scid tail reader
│   ├── state/                   # SQLite-backed state store (TBD)
│   └── risk/                    # Authoritative risk engine (TBD)
config/
└── instruments.json             # MES/ES/MGC/MNQ/M6E/MCL config
tests/
├── mocks/                       # Mock DTC + Sierra servers
└── integration/
    └── dtc_sim_order_test.py    # Canonical Phase A handshake test (PASS)
docs/legacy/
└── kate_shared_state.py         # KATE shared-state — reference only, NOT used
data/                            # Trade history CSVs — gitignored, local only
```

## Provenance

KATE reference files imported from Sierra Windows VPS by COO Gemini, Omni commit `338f42b` (2026-04-25). See Omni handoff `2026-04-25-gemini-to-team-kate-code-staged.md`.

## Coordination

This repo is its own line of work. Coordination, governance, and decision records live in the Omni repo (`c:\models\omni\`). Cross-team work moves through Omni `handoffs/` and `proposals/`.
