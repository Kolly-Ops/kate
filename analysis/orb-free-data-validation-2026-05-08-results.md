# ORB Free-Data Validation - NQ and Russell Proxies

Date run: 2026-05-08
Owner: Codex
Source handoff: `handoffs/2026-05-08-claude-to-codex-orb-free-data-validation.md`

## Scope

Purpose: validate the ORB multi-instrument thesis using free intraday proxy data before spending on Sierra MNQ/M2K data.

Data source:

- `yfinance`
- Interval: 5-minute bars
- Period returned: 2026-02-27 05:05 UTC to 2026-05-08 20:55 UTC

Symbols:

- NASDAQ proxy: `NQ=F`
- Russell proxy: `RTY=F`

Configs tested:

- Long-only 2.0R
- Both-directions 2.5R

Risk policy:

- Initial NLV: GBP 1,080
- Per-trade cap: 2.5%
- NLV floor: GBP 300
- Kill-switch gate: 30% drawdown blocks new entries

## Results

| Proxy | Config | Return | Max DD | Trades | Win rate | Expectancy | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| NQ=F | long-only 2.0R | -2.50% | -2.50% | 1 | 0.00% | GBP -26.95 | Mostly risk-gated |
| NQ=F | both 2.5R | -2.50% | -2.50% | 1 | 0.00% | GBP -26.95 | Mostly risk-gated |
| RTY=F | long-only 2.0R | -6.49% | -14.75% | 22 | 27.27% | GBP -3.19 | Fails |
| RTY=F | both 2.5R | +29.22% | -10.15% | 36 | 41.67% | GBP +8.77 | Passes strongly |

## Readout

Free-data validation does not support a broad "NQ + Russell both work" thesis.

It does support a narrower thesis:

- Russell 2000 / M2K-style ORB is worth validating with real Sierra M2K data.
- NASDAQ / MNQ-style ORB is not viable under the current GBP 1,080 / 2.5% account risk geometry unless the stop/risk design changes. Most NQ setups were rejected because one micro-sized contract's ATR stop exceeded the GBP 27 risk budget.

NQ detail:

- Initial MES-style `max_range_points=25` filtered all NQ sessions, so I reran with `max_range_points=250`.
- Even then, only one setup passed the risk gate.
- Skipped by risk: 1,162 long-only / 1,862 both-direction.

RTY detail:

- Long-only fails.
- Both-direction 2.5R passes with positive expectancy and drawdown well below the 30% kill-switch.
- This is encouraging enough to justify pulling proper Sierra M2K data before making a build decision.

## Recommendation

Do not spend on broad multi-symbol live data yet.

Next best move:

1. Pull Sierra 1-minute data for M2K M26 first.
2. Validate the `RTY=F` signal against actual M2K futures data.
3. Keep MES + M2K as the likely reduced portfolio candidate if M2K confirms.
4. Defer MNQ unless we redesign risk geometry for larger ATR products or increase account size.

This outcome maps to Claude's Scenario 2: one works, one does not. It supports a reduced 2-instrument portfolio thesis, not the full 4-symbol thesis.

## Data Quality Caveat

This validates strategy thesis only, not live execution:

- Yahoo data is not broker-grade.
- `NQ=F` and `RTY=F` are continuous proxy symbols, not exact MNQ/M2K contracts.
- 5-minute data has fewer intraday decision points than Sierra 1-minute bars.
- Slippage/spread/order lifecycle are not modeled.

## Artifacts

- ORB harness: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08.py`
- NQ long-only: `C:\models\Trading Bot\analysis\orb-free-data-nq-5m-long-max250-metrics.json`
- NQ both 2.5R: `C:\models\Trading Bot\analysis\orb-free-data-nq-5m-both-rr25-max250-metrics.json`
- RTY long-only: `C:\models\Trading Bot\analysis\orb-free-data-rty-5m-long-metrics.json`
- RTY both 2.5R: `C:\models\Trading Bot\analysis\orb-free-data-rty-5m-both-rr25-metrics.json`
