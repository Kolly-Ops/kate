# ORB VectorBT Prototype Results

Date run: 2026-05-08
Owner: Codex
Purpose: Path 2 first prototype, tested against the same MESM26 1-minute Sierra data and the same 2.5% paper/sim risk baseline used for Kate.

## Prototype Logic

- Data: `MESM26-CME.scid`
- Window: 2026-03-23 13:55 to 2026-05-07 22:33
- Bars: 45,534
- Sessions observed: 40
- Opening range: 14:30-15:00 UTC
- Trade window: 15:00-20:45 UTC
- Max trades: 1 per UTC day
- Direction: both long and short unless noted
- Filter: EMA200 direction filter
- Stop: ATR14 * 1.1
- Target: reward/risk variant
- Risk: 2.5% per trade, GBP 1,080 initial NLV, GBP 300 NLV floor
- Kill-switch: block new entries once drawdown reaches 30%

## Headline Result

The ORB prototype is the first candidate that beats the Kate control case cleanly on the same data and policy constraints.

Base ORB, both directions, 2.0R:

- Return: +7.90%
- Max drawdown: -8.88%
- Trades: 22
- Win rate: 45.45%
- Expectancy: GBP +3.88/trade
- Largest losing streak: 4
- Targets/stops: 10 / 12
- Long/short split: 13 / 9

## Sensitivity Sweep

| Variant | Return | Max DD | Trades | Win rate | Expectancy | Long | Short | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Both, 1.5R | +1.02% | -8.88% | 22 | 45.45% | GBP +0.50 | 13 | 9 | Positive but weak |
| Both, 2.0R | +7.90% | -8.88% | 22 | 45.45% | GBP +3.88 | 13 | 9 | Strong base |
| Both, 2.5R | +11.92% | -8.88% | 22 | 40.91% | GBP +5.85 | 13 | 9 | Best return |
| Long-only, 2.0R | +5.66% | -4.77% | 15 | 46.67% | GBP +4.07 | 15 | 0 | Cleanest drawdown |
| Short-only, 2.0R | +3.86% | -10.86% | 13 | 38.46% | GBP +3.20 | 0 | 13 | Positive but weaker |

## Comparison To Kate Control

| Strategy | Return | Max DD | Trades | Expectancy |
| --- | ---: | ---: | ---: | ---: |
| Current Kate, production risk | -30.81% | -37.24% | 86 | GBP -3.87 |
| Conservative Kate, production risk | -30.44% | -33.44% | 75 | GBP -4.38 |
| Aggressive Kate, production risk | -31.21% | -31.21% | 77 | GBP -4.38 |
| ORB both, 2.0R | +7.90% | -8.88% | 22 | GBP +3.88 |
| ORB long-only, 2.0R | +5.66% | -4.77% | 15 | GBP +4.07 |
| ORB both, 2.5R | +11.92% | -8.88% | 22 | GBP +5.85 |

## Interpretation

This is not yet live-ready proof. The sample is still only about 6.5 weeks, and 22 trades is a small sample.

But it is a strong Path 2 signal because it fixes the main failure modes seen in Kate:

- Far fewer trades.
- No account-level kill-switch breach.
- Positive expectancy under the same 2.5% policy.
- Drawdown stays materially below 30%.
- Both long and short sides are positive in isolation.

My read:

- ORB should advance to the next Path 2 validation step.
- The leading variants are `both directions, 2.5R` for return and `long-only, 2.0R` for drawdown quality.
- The next step should test ORB robustness across a wider history before porting fully to Lean or production.

## Caveats

- Uses OHLC touch simulation, not exact tick-path replay.
- Uses ATR stop rather than opposite-side opening range stop because the full range stop was too wide for the 2.5% risk budget and produced zero trades.
- Needs longer historical data or Sierra export before confidence-building.
- Needs implementation details for short-side broker handling if both-direction ORB is selected.

## Artifacts

- Script: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08.py`
- Base metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-metrics.json`
- Base equity: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-equity.png`
- RR 1.5 metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-rr15-metrics.json`
- RR 2.5 metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-rr25-metrics.json`
- Long-only metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-long-metrics.json`
- Short-only metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-short-metrics.json`
