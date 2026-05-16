# ORB Multi-Symbol Extension Results

Date run: 2026-05-08
Owner: Codex
Source handoff: `handoffs/2026-05-08-claude-to-codex-orb-multi-symbol-extension.md`

## Data Availability

Local Sierra Chart data in `C:\SierraChart\Data`:

| Requested | Local file found | Status |
| --- | --- | --- |
| MES | `MESM26-CME.scid` | Available, already tested |
| MNQ | none found | Missing |
| MYM/Micro Dow | `YMM26-CBOT.scid` | Available as Dow micro/mini proxy; tested with requested micro tick settings |
| M2K | none found | Missing |

Additional filename sweep found only:

- `YMH26-CBOT.scid`
- `YMM26-CBOT.scid`
- `YMU25-CBOT.scid`
- no `MNQ`, `M2K`, or `RTY` contract files

## Individual Instrument Results

Same ORB baseline:

- Opening range: 14:30-15:00 UTC
- Trade window: 15:00-20:45 UTC
- EMA200 direction filter
- ATR14 * 1.1 stop
- 2.5% per-trade risk cap
- GBP 1,080 NLV
- GBP 300 floor
- 30% kill-switch gate
- Max one trade per day per symbol

### MES Control

| Config | Return | Max DD | Trades | Win rate | Expectancy | Targets | Stops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Long-only 2.0R | +5.66% | -4.77% | 15 | 46.67% | GBP +4.07 | 7 | 8 |
| Both 2.5R | +11.92% | -8.88% | 22 | 40.91% | GBP +5.85 | 9 | 13 |

### YMM26-CBOT

Tested with Claude's requested micro Dow settings:

- Tick size: 1.0
- Tick value: GBP 0.50

| Config | Return | Max DD | Trades | Win rate | Expectancy | Targets | Stops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Long-only 2.0R | -0.15% | -0.15% | 17 | 17.65% | GBP -0.09 | 3 | 14 |
| Both 2.5R | -0.04% | -0.10% | 26 | 26.92% | GBP -0.02 | 7 | 19 |

## Portfolio Pass

Not run.

Reason: Pass 2 required at least two instruments with edge. We currently have only MES passing. YMM/MYM proxy is slightly negative, and MNQ/M2K are missing from local Sierra data.

## Interpretation

ORB remains a strong MES candidate, but the local data does not yet prove multi-symbol scalability.

Current state:

- MES passes cleanly.
- YMM/MYM proxy does not pass.
- MNQ/M2K cannot be tested until Sierra data is available.

This means we should not claim the four-symbol scaling thesis yet. The next useful move is to have Sierra download/export MNQ and M2K 1-minute data, then rerun the same two configs.

## Recommendation

Proceed with ORB as the best candidate so far, but do not build the multi-symbol portfolio layer until at least MNQ and M2K have been tested.

Immediate next steps:

1. Ask CEO/operator to pull 1-minute Sierra data for MNQ M26 and M2K M26.
2. Confirm whether `YMM26-CBOT.scid` is the intended micro Dow contract or an E-mini Dow file. If it is not MYM, fetch MYM separately.
3. Rerun Pass 1 on the missing symbols.
4. Only run Pass 2 portfolio if at least two instruments show positive expectancy.

## Artifacts

- ORB script: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08.py`
- MES result note: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-results.md`
- YMM long-only metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-ym-long-microtick-metrics.json`
- YMM both 2.5R metrics: `C:\models\Trading Bot\analysis\orb-vectorbt-prototype-2026-05-08-ym-both-rr25-microtick-metrics.json`
