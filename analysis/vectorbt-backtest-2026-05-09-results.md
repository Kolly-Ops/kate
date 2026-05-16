# Kate VectorBT Backtest Validation — Track B

**Run date:** 2026-05-07  
**Owner:** Codex  
**Scope:** First-pass validation of Kate's current ATR-breakout signal using local Sierra Chart MES `.scid` data.

## Verdict

❌ **Strategy edge is not supported by this first-pass backtest.**

On the available local MES data, Kate's current long-only ATR breakout produced negative return, negative Sharpe, negative expectancy, and an 8-trade losing streak. This is not enough evidence to proceed confidently into the paper window without either a longer historical re-test, a corrected data export, or a strategy pivot review at the Monday gate.

## Data

| Item | Value |
|---|---:|
| Source files | `MESH26-CME.scid`, `MESM26-CME.scid` |
| Date span | 2025-06-16 13:00 → 2026-05-07 22:00 |
| Hourly bars | 4,369 |
| Requested span | 2-3 years |
| Actual span caveat | Less than 1 year; current-contract stitching only |

This does **not** satisfy Claude's requested 2-3 year continuous MES window. Treat this report as a first-pass red flag, not final statistical proof.

## Strategy Recreated

Kate's current strategy logic from `trading_bot/core/strategy/breakout.py`:

- Long-only entry.
- Enter when the just-closed candle closes above the prior 20-bar high.
- Require close above SMA50.
- ATR14 stop at `1.1 * ATR`.
- ATR14 target at `3.0 * ATR`.
- One open position max.
- No pyramiding.
- Conservative same-bar handling: if stop and target are both touched in one bar, stop is counted first.

Risk envelope applied:

- Initial NLV: £1,080.
- Per-trade risk cap: 2.5%.
- NLV floor: £300.
- One MES contract.
- Mandatory stop-loss.

## Metrics

| Metric | Result |
|---|---:|
| Starting equity | £1,080.00 |
| Ending equity | £958.89 |
| Total return | -11.21% |
| Annualised return | -12.50% |
| Sharpe ratio | -1.79 |
| Max drawdown | -£174.04 |
| Max drawdown | -15.36% |
| Trades | 9 |
| Win rate | 11.11% |
| Average win | £52.93 |
| Average loss | -£21.75 |
| Expectancy | -£13.46/trade |
| Trades per day | 0.028 |
| Trades per month | 0.84 |
| Largest losing streak | 8 |
| Target exits | 1 |
| Stop exits | 8 |

## Equity Curve

![Kate ATR Breakout Backtest Equity Curve](C:/models/Trading Bot/analysis/vectorbt-backtest-2026-05-09-equity.png)

## Limitations

- The available data is less than 1 year, not the requested 2-3 years.
- The contract series is stitched from local MESH26 and MESM26 files, not a professionally back-adjusted continuous MES series.
- This is hourly-bar testing; live Kate currently evaluates against Sierra candle closes and may use a different runtime timeframe depending on supervisor flags.
- Bar data cannot faithfully model tick order inside a candle. This report uses conservative stop-first logic when stop and target are both touched in the same bar.
- Slippage, partial fills, DTC latency, exchange fees, and Sierra simulation quirks are not modeled.
- VectorBT is installed and imported (`vectorbt==1.0.0`) with `NUMBA_DISABLE_JIT=1`; bracket sequencing is simulated in custom Python because Kate's synthetic bracket logic is more specific than a plain vectorized entries/exits model.

## Recommendation

For the Monday 2026-05-11 gate:

1. Do **not** treat the current strategy as edge-confirmed.
2. Ask for/export a longer continuous MES history from Sierra if the team wants one more confirmation pass before pivoting.
3. If Track A live-fire also looks weak, fire **Path 2** and start the QuantConnect Lean strategy-candidate review.

Generated artifacts:

- `analysis/vectorbt-backtest-2026-05-09.py`
- `analysis/vectorbt-backtest-2026-05-09-metrics.json`
- `analysis/vectorbt-backtest-2026-05-09-equity.png`
