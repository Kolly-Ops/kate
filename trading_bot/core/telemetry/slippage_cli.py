"""Slippage telemetry CLI — read JSONL log + emit human / JSON report.

Usage:
  python -m trading_bot.core.telemetry.slippage_cli --front front_4
  python -m trading_bot.core.telemetry.slippage_cli --front front_4 --json
  python -m trading_bot.core.telemetry.slippage_cli --front front_4 --log-root C:/path/to/logs
  python -m trading_bot.core.telemetry.slippage_cli --front front_4 --last 30
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .slippage import SlippageRecorder


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--front", required=True, help="front_id, e.g. front_4")
    p.add_argument(
        "--log-root",
        type=Path,
        default=Path(r"C:\models\omni\.mcp-brain\logs\slippage"),
        help="dir containing <front>_slippage.jsonl "
        "(default .mcp-brain/logs/slippage — Gemini CFO/Ops 2026-05-16: "
        "durable audit trail)",
    )
    p.add_argument("--pip-size", type=float, default=0.0001)
    p.add_argument("--json", action="store_true", help="emit JSON instead of human")
    p.add_argument(
        "--last",
        type=int,
        default=0,
        help="limit summary to last N records (0 = all)",
    )
    args = p.parse_args(argv)

    recorder = SlippageRecorder(
        front_id=args.front, log_root=args.log_root, pip_size=args.pip_size
    )
    loaded = recorder.load_persisted()
    if loaded == 0:
        print(
            f"No slippage records yet for {args.front} at {recorder.jsonl_path}",
            file=sys.stderr,
        )
        return 0

    records = list(recorder.records)
    if args.last > 0:
        records = records[-args.last:]
        # Build an ad-hoc summary over only the last N
        import statistics
        pips = [r.slippage_pips for r in records]
        latencies = [r.fill_latency_seconds for r in records]
        sub_summary = {
            "front_id": args.front,
            "n_pairs": len(records),
            "mean_pips": round(statistics.fmean(pips), 3) if pips else 0.0,
            "median_pips": round(statistics.median(pips), 3) if pips else 0.0,
            "std_pips": round(statistics.stdev(pips), 3) if len(pips) > 1 else 0.0,
            "min_pips": round(min(pips), 3) if pips else 0.0,
            "max_pips": round(max(pips), 3) if pips else 0.0,
            "mean_latency_seconds": round(statistics.fmean(latencies), 3) if latencies else 0.0,
        }
        if args.json:
            print(json.dumps(sub_summary, indent=2))
        else:
            _render_human(sub_summary, recent=records[-5:])
        return 0

    summary = recorder.summary()
    if args.json:
        print(json.dumps({
            "front_id": summary.front_id,
            "n_pairs": summary.n_pairs,
            "n_pending_intents": summary.n_pending_intents,
            "mean_pips": round(summary.mean_pips, 3),
            "median_pips": round(summary.median_pips, 3),
            "std_pips": round(summary.std_pips, 3),
            "min_pips": round(summary.min_pips, 3),
            "max_pips": round(summary.max_pips, 3),
            "mean_latency_seconds": round(summary.mean_latency_seconds, 3),
        }, indent=2))
        return 0

    summary_dict = {
        "front_id": summary.front_id,
        "n_pairs": summary.n_pairs,
        "mean_pips": round(summary.mean_pips, 3),
        "median_pips": round(summary.median_pips, 3),
        "std_pips": round(summary.std_pips, 3),
        "min_pips": round(summary.min_pips, 3),
        "max_pips": round(summary.max_pips, 3),
        "mean_latency_seconds": round(summary.mean_latency_seconds, 3),
    }
    _render_human(summary_dict, recent=records[-5:])
    return 0


def _render_human(summary: dict, *, recent: list) -> None:
    print("=" * 72)
    print(f"  SLIPPAGE TELEMETRY  -  {summary['front_id']}")
    print("=" * 72)
    print(f"  n trades            : {summary['n_pairs']}")
    print(f"  mean slippage       : {summary['mean_pips']:+.3f} pip")
    print(f"  median slippage     : {summary['median_pips']:+.3f} pip")
    print(f"  std dev             : {summary['std_pips']:.3f} pip")
    print(f"  min  / max          : {summary['min_pips']:+.3f}  /  {summary['max_pips']:+.3f} pip")
    print(f"  avg recorder latency: {summary['mean_latency_seconds']:.2f} s")
    print()
    print("  Convention: positive = bad for trader (paid more on BUY, received less on SELL)")
    print("  Latency is local recorder-observed time, not broker/exchange event latency.")
    print()
    if recent:
        print("  Last 5 trades:")
        print(f"  {'intent_id':<28} {'side':<5} {'signal':<10} {'fill':<10} {'slip(pip)':<10}")
        for r in recent:
            print(
                f"  {r.intent_id:<28} {r.side:<5} "
                f"{r.signal_price:<10.5f} {r.fill_price:<10.5f} {r.slippage_pips:+.3f}"
            )
    print("=" * 72)


if __name__ == "__main__":
    sys.exit(main())
