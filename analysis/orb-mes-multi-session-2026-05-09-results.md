# MES Multi-Session ORB Results

Date run: 2026-05-09
Owner: Codex
Source handoff: `handoffs/2026-05-09-claude-to-codex-orb-multi-session-mes.md`

## Scope

Data:

- `MESM26-CME.scid`
- 1-minute bars
- Window: 2026-03-23 13:55 to 2026-05-07 22:33

Shared risk policy:

- Initial NLV: GBP 1,080
- Per-trade risk cap: 2.5%
- NLV floor: GBP 300
- Kill-switch: 30% drawdown blocks new entries
- Tick size: 0.25
- Tick value: GBP 1.25
- ATR14 * 1.1 stop
- EMA200 trend filter

Sessions tested:

| Session | Opening range UTC | Trade window UTC |
| --- | --- | --- |
| Asian | 00:00-00:30 | 00:30-06:00 |
| European | 08:00-08:30 | 08:30-14:00 |
| US | 14:30-15:00 | 15:00-20:45 |

## Pass 1 - Individual Session Results

| Session | Config | Return | Max DD | Trades | Win rate | Expectancy | Targets | Stops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Asian | long-only 2.0R | +6.69% | -4.55% | 23 | 39.13% | GBP +3.14 | 9 | 14 |
| Asian | both 2.5R | +7.67% | -5.37% | 29 | 31.03% | GBP +2.85 | 9 | 20 |
| European | long-only 2.0R | -3.46% | -6.50% | 25 | 28.00% | GBP -1.50 | 7 | 18 |
| European | both 2.5R | -9.85% | -12.25% | 30 | 20.00% | GBP -3.55 | 6 | 24 |
| US | long-only 2.0R | +5.66% | -4.77% | 15 | 46.67% | GBP +4.07 | 7 | 8 |
| US | both 2.5R | +11.92% | -8.88% | 22 | 40.91% | GBP +5.85 | 9 | 13 |

Readout:

- Asian passes.
- US passes and reproduces the prior control exactly.
- European fails and should not be included in v1.

## Pass 2 - Shared-Account Portfolio Results

| Portfolio | Config | Return | Max DD | Trades | Trades/week | Win rate | Expectancy | Largest losing streak |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All sessions | long-only 2.0R | +8.88% | -9.39% | 63 | 9.80 | 36.51% | GBP +1.52 | 14 |
| All sessions | both 2.5R | +9.74% | -12.02% | 81 | 12.60 | 29.63% | GBP +1.30 | 8 |
| Asian + US | long-only 2.0R | +12.35% | -5.33% | 38 | 5.91 | 42.11% | GBP +3.51 | 8 |
| Asian + US | both 2.5R | +19.59% | -4.80% | 51 | 7.93 | 35.29% | GBP +4.15 | 5 |

## Recommendation

Advance **Asian + US MES ORB, both-direction 2.5R** as the leading no-spend candidate.

Why:

- Highest return among the no-spend MES-only tests: +19.59%.
- Max drawdown remains low: -4.80%.
- Trade volume improves from US-only 22 trades to 51 trades over the same window.
- Expected cadence improves from roughly 3.4 trades/week to 7.9 trades/week.
- Largest losing streak improves versus the all-session long-only case.
- Excluding Europe removes the clearly negative session.

Fallback candidate:

- Asian + US long-only 2.0R: +12.35%, -5.33% max DD, 38 trades.
- This is operationally simpler but gives up return and volume.

## Caveats

- Sample remains only ~6.5 weeks.
- Asian session has thinner liquidity than US. Live slippage/spread risk may be materially higher than the backtest suggests.
- Asian trade window crosses potential CME/Sierra maintenance-hour risk; live engine should avoid stale bars and confirm no data gaps.
- European session overlaps the 13:30-14:00 pre-blackout interval but failed even before adding any extra restriction.
- This is still OHLC-touch simulation, not full tick-path or broker-order replay.

## Artifacts

- Shared portfolio script: `C:\models\Trading Bot\analysis\orb-mes-multi-session-portfolio-2026-05-09.py`
- Asian long metrics: `C:\models\Trading Bot\analysis\orb-mes-asian-long-rr20-metrics.json`
- Asian both metrics: `C:\models\Trading Bot\analysis\orb-mes-asian-both-rr25-metrics.json`
- European long metrics: `C:\models\Trading Bot\analysis\orb-mes-europe-long-rr20-metrics.json`
- European both metrics: `C:\models\Trading Bot\analysis\orb-mes-europe-both-rr25-metrics.json`
- US long metrics: `C:\models\Trading Bot\analysis\orb-mes-us-long-rr20-control-metrics.json`
- US both metrics: `C:\models\Trading Bot\analysis\orb-mes-us-both-rr25-control-metrics.json`
- Asian + US both portfolio metrics: `C:\models\Trading Bot\analysis\orb-mes-portfolio-asian-us-both-rr25-metrics.json`
- Asian + US long portfolio metrics: `C:\models\Trading Bot\analysis\orb-mes-portfolio-asian-us-long-rr20-metrics.json`
- All-session both portfolio metrics: `C:\models\Trading Bot\analysis\orb-mes-portfolio-all-both-rr25-metrics.json`
- All-session long portfolio metrics: `C:\models\Trading Bot\analysis\orb-mes-portfolio-all-long-rr20-metrics.json`
