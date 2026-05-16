# VectorBT Backtest Rerun - 1-Minute Six-Week Window

Date run: 2026-05-07
Owner: Codex
Request: rerun the same backtest infrastructure on the six-week 1-minute window.

## Scope

- Strategy: Kate current ATR breakout logic mirrored from `trading_bot/core/strategy/breakout.py`
- Data source: `MESM26-CME.scid`
- Timeframe: 1-minute candles
- Data span: 2026-03-23 13:55 to 2026-05-07 22:33
- Bars: 45,534
- Initial NLV: GBP 1,080.00
- Contract assumption: 1 MES contract, GBP 1.00 per tick, 0.25 tick size
- Same-bar assumption: if stop and target were both touched in the same candle, stop was counted first

## Result

Verdict: FAIL / not edge-confirmed.

The 1-minute rerun is worse than the earlier 60-minute pass. It produces far more trades, but the expectancy remains negative and drawdown becomes materially larger.

| Metric | 1-minute rerun |
| --- | ---: |
| Ending equity | GBP 852.06 |
| Total return | -21.11% |
| Annualized return | -85.38% |
| Sharpe ratio | -0.032 |
| Max drawdown | GBP -513.70 |
| Max drawdown | -45.88% |
| Trade count | 957 |
| Win rate | 26.12% |
| Average win | GBP 25.78 |
| Average loss | GBP -9.44 |
| Expectancy | GBP -0.24/trade |
| Trades/day | 21.27 |
| Trades/month | 647.30 |
| Largest losing streak | 21 |
| Target exits | 250 |
| Stop exits | 707 |

## Comparison To 60-Minute First Pass

| Metric | 60-minute first pass | 1-minute rerun |
| --- | ---: | ---: |
| Data span | 2025-06-16 to 2026-05-07 | 2026-03-23 to 2026-05-07 |
| Bars | 4,369 | 45,534 |
| Trade count | 9 | 957 |
| Win rate | 11.11% | 26.12% |
| Total return | -11.21% | -21.11% |
| Max drawdown | -15.36% | -45.88% |
| Expectancy | GBP -13.46/trade | GBP -0.24/trade |
| Largest losing streak | 8 | 21 |

## Readout

The 1-minute data confirms the same direction as the first pass: this strategy is not edge-confirmed on the available Sierra Chart history. At 1-minute granularity it appears to overtrade: 957 trades across roughly six and a half weeks, with 707 stop exits versus 250 target exits.

This strengthens the case that Track A needs to rescue the live behavior with a concrete implementation or data issue. If Track A does not identify a clear mismatch, the current Kate breakout logic should not be treated as production-ready for live broker routing.

## Artifacts

- Metrics JSON: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-metrics.json`
- Equity plot: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-equity.png`
- Backtest script: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09.py`
