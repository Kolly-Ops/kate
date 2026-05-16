# Kate Strategy Rework Investigation

Date: 2026-05-08
Owner: Codex
Source handoff: `handoffs/2026-05-08-claude-to-codex-strategy-rework-investigation.md`

## Summary

Claude was right that the prior backtest had `tick_value_gbp=1.0` while production uses `1.25`. That mismatch matters in two ways:

1. It changes P&L per tick.
2. More importantly, it changes which trades pass the per-trade risk gate.

However, there was a second production-risk mismatch in the backtest: production blocks new entries once account drawdown reaches 30%, but the earlier backtest continued trading through deeper drawdown and allowed later recovery trades.

After aligning both:

- `tick_value_gbp=1.25`
- `kill_switch_drawdown_pct=0.30`
- 13:30-14:30 UTC blackout enabled
- 2.5% per-trade risk cap
- NLV floor GBP 300

All three risk-regime variants fail.

## Production-Risk Results

Same 1-minute MESM26 window: 2026-03-23 13:55 to 2026-05-07 22:33.

| Variant | Stop | Target | Trades | Win rate | Return | Max DD | Expectancy | Targets | Stops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Conservative | 1.5 ATR | 5.0 ATR | 75 | 18.67% | -30.44% | -33.44% | GBP -4.38 | 14 | 61 |
| Current | 1.1 ATR | 3.0 ATR | 86 | 23.26% | -30.81% | -37.24% | GBP -3.87 | 20 | 66 |
| Aggressive | 0.8 ATR | 2.0 ATR | 77 | 20.78% | -31.21% | -31.21% | GBP -4.38 | 16 | 61 |

## Why The Earlier Tick-Only Result Looked Better

With `tick_value=1.25` but without the production kill-switch gate, the current strategy appeared to recover:

- Ending equity: GBP 1,195.80
- Return: +10.72%
- Max drawdown: -40.58%
- Trades: 905
- Expectancy: GBP +0.13/trade

That is not production-realistic. Production would stop approving new entries once drawdown crossed 30%. The apparent recovery depends on trades that production risk policy should not allow.

## 2026-04-30 Day-Specific Test

Ground-truth expectation from handoff: 6 TP / 1 SL.

Backtest result for 2026-04-30, no blackout, production tick/risk:

- Trades: 39
- Targets: 13
- Stops: 26
- Return: +13.65%
- Max drawdown: -5.49%
- Expectancy: GBP +3.78/trade

This does not match the stated 6 TP / 1 SL. It is directionally consistent with 2026-04-30 being a strong/lucky day, but it is not an exact replay of live runtime.

Likely divergence sources to inspect next:

- Production may have had fewer approved entries because DTC/account state, open-position state, or bracket lifecycle stayed open longer than the candle-level simulation.
- Backtest assumes stop/target fills from OHLC touch logic; production fills depend on Sierra order lifecycle and exact tick path.
- Production paper-trade count may be filtered by order rejection/state-store behavior not represented in the custom simulator.

## Recommendation

Do not treat the brief tick-only positive result as a rescue. Once production risk is aligned, current/conservative/aggressive variants all breach the 30% kill-switch zone and finish negative.

Path 2 remains justified, but the investigation should be framed precisely:

- The signal may contain some favorable regimes, especially 2026-04-30.
- The current risk/entry/execution system is not production-viable as configured.
- A replacement or rework should prioritize lower drawdown, fewer trades, and tighter runtime/backtest parity before live cutover.

## Lean Candidate Review - Initial Shortlist

I started the formal Path 2 review using QuantConnect/Lean public material.

Initial lanes worth deeper review:

1. Opening Range Breakout with trend filter and protective/trailing stops.
   - Source: QuantConnect forum strategy example, simple mechanical structure.
   - Fit: strong candidate for MES because it naturally limits entries to a session window and avoids all-day overtrading.

2. Futures momentum / trend-following template.
   - Source: QuantConnect Lean `FuturesMomentumAlgorithm.cs`.
   - Fit: closer to managed-futures heritage than the current 20-minute breakout. Needs adaptation from basket/continuous-futures style to single MES micro execution.

3. Moving-average crossover / trend filter baseline.
   - Source: QuantConnect Lean `MovingAverageCrossAlgorithm.py`.
   - Fit: not futures-specific, but useful as a simple mechanical baseline and sanity comparator.

4. Reworked Kate breakout, but session-scoped and kill-switch-aware.
   - Source: current Kate strategy.
   - Fit: only worth keeping if we materially reduce firing frequency and drawdown, likely by adding session constraints, stronger regime filter, and a cooldown after stop-outs.

Next candidate-review step: pull 3-5 concrete Lean algorithms/templates into a comparison note with implementation effort, expected data requirements, and risk controls.

## Artifacts

- Current production-risk metrics: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-current-prod-risk-metrics.json`
- Conservative production-risk metrics: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-conservative-prod-risk-metrics.json`
- Aggressive production-risk metrics: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-1m-aggressive-prod-risk-metrics.json`
- 2026-04-30 metrics: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09-20260430-no-blackout-prod-risk-metrics.json`
- Updated script: `C:\models\Trading Bot\analysis\vectorbt-backtest-2026-05-09.py`
