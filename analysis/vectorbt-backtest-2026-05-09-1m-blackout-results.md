# VectorBT Backtest Rerun - 1-Minute With Volatility Blackout

Date run: 2026-05-08
Owner: Codex
Request: rerun the 6-week 1-minute production-timeframe test with Kate's 13:30-14:30 UTC volatility blackout filter applied.

## Scope

- Strategy: Kate current ATR breakout logic mirrored from `trading_bot/core/strategy/breakout.py`
- Data source: `MESM26-CME.scid`
- Timeframe: 1-minute candles
- Data span: 2026-03-23 13:55 to 2026-05-07 22:33
- Bars: 45,534
- Blackout: 13:30-14:30 UTC, new entries blocked only
- Blackout bars: 1,955
- Initial NLV: GBP 1,080.00
- Contract assumption: 1 MES contract, GBP 1.00 per tick, 0.25 tick size
- Same-bar assumption: if stop and target were both touched in the same candle, stop was counted first

## Result

Verdict: FAIL / not edge-confirmed.

The blackout filter helps, reducing the total loss from -21.11% to -11.94%, but it does not rescue the strategy. Expectancy remains negative and max drawdown remains severe at -47.06%.

| Metric | 1-minute blackout rerun |
| --- | ---: |
| Ending equity | GBP 951.06 |
| Total return | -11.94% |
| Annualized return | -64.34% |
| Sharpe ratio | 0.027 |
| Max drawdown | GBP -526.99 |
| Max drawdown | -47.06% |
| Trade count | 915 |
| Win rate | 26.34% |
| Average win | GBP 25.10 |
| Average loss | GBP -9.17 |
| Expectancy | GBP -0.14/trade |
| Trades/day | 20.33 |
| Trades/month | 618.90 |
| Largest losing streak | 20 |
| Target exits | 241 |
| Stop exits | 674 |

## Comparison

| Metric | 1-minute base | 1-minute blackout |
| --- | ---: | ---: |
| Bars | 45,534 | 45,534 |
| Trades | 957 | 915 |
| Win rate | 26.12% | 26.34% |
| Total return | -21.11% | -11.94% |
| Max drawdown | -45.88% | -47.06% |
| Expectancy | GBP -0.24/trade | GBP -0.14/trade |
| Largest losing streak | 21 | 20 |
| Target exits | 250 | 241 |
| Stop exits | 707 | 674 |

## Readout

The production volatility blackout removes 42 trades and improves headline return by roughly 9 percentage points, so the filter is useful. But it does not change the Monday gate decision materially: the strategy still has negative expectancy, a high trade rate, and a drawdown profile that would have breached the operating risk tolerance.

My recommendation remains: current Kate ATR breakout logic should stay sim-only. If Track A does not surface a concrete implementation mismatch, Path 2 should fire Monday.

## Artifacts

- Metrics JSON: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-blackout-metrics.json`
- Equity plot: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-blackout-equity.png`
- Backtest script: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09.py`
- Blackout wrapper: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-blackout.py`
