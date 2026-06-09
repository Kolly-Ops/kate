# IG ProOrder London Open Breakout v0 Runbook

Status: experimental demo lane. MT5 remains the trusted primary lane until this has compiled, backtested, and produced clean demo evidence.

## Files

- `C:\models\Trading Bot\proorder\ig_london_open_breakout_v0.probuilder` - GBPUSD, 6-pip stop floor
- `C:\models\Trading Bot\proorder\ig_london_open_breakout_eurusd_v0.probuilder` - EURUSD, 5-pip stop floor
- `C:\models\Trading Bot\proorder\ig_london_open_breakout_audusd_v0.probuilder` - AUDUSD, 5-pip stop floor
- `C:\models\Trading Bot\proorder\ig_london_open_breakout_eurgbp_v0.probuilder` - EURGBP, 4-pip stop floor

ProOrder systems are chart/instrument scoped. Load one file per 5-minute chart/instrument.

## Required chart setup

1. Open ProRealTime demo through IG.
2. Open the target spread-bet instrument: GBPUSD, EURUSD, AUDUSD, or EURGBP.
3. Use a 5-minute chart.
4. Ensure the chart/trading-system timezone is UK / Europe-London.
5. Confirm the account is demo before launching ProOrder.

## Strategy settings

Default v0 parameters:

- Position size: `1`
- Asian range: `00:00-07:00` UK
- Trade window: `07:00-10:00` UK
- Reward/risk: `2.0R`
- ATR stop: `ATR14`
- Minimum stop floor: `6 pips`
- Minimum range: `5 pips`
- Maximum range: `120 pips`
- One trade per day
- 60-minute cooldown concept: `12` five-minute bars
- Forced flat near 10:00 UK

Per-pair minimum stop floors copied from Kate:

| Pair | Min stop floor |
|---|---:|
| GBPUSD | 6 pips |
| EURUSD | 5 pips |
| AUDUSD | 5 pips |
| EURGBP | 4 pips |

## Compile/backtest sequence

1. Paste the matching pair code into a new ProBacktest / ProOrder trading system for that instrument.
2. Backtest on at least 60 recent trading days.
3. Check these diagnostics:
   - `AsianHigh` and `AsianLow` update during the 00:00-07:00 UK window.
   - `RangePips` is sane, usually above 5 and below 120.
   - At most one trade fires per day.
   - Every trade has stop and target attached.
   - Any open trade is closed around 10:00 UK if stop/target did not fire.
4. If compile fails, capture the exact ProRealTime error line and message.

## Demo activation guardrails

Before pressing ProOrder live-demo start:

- Demo account only.
- Maximum position size in ProOrder should be set to `1`.
- Confirm no other automated system is running on the same instrument.
- If running all four pairs, confirm there are four separate systems and each one is attached to the correct chart.
- Confirm no manual open position on the instrument.
- Confirm stop and target are visible in ProBacktest examples.
- CEO knows where to stop the ProOrder system manually.

## Known limitations versus Kate

This is not full Kate parity:

- No Kate StateStore.
- No cross-venue MT5 exposure check.
- No Python risk gate / NLV floor.
- No external kill switch beyond ProOrder/manual stop.
- No Kate WAL trade journal.
- No news-event blackout in v0.

The purpose is to get a working IG-native experimental lane while the Python/Lightstreamer API entitlement remains blocked by IG.
