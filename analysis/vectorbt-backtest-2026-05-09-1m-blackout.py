"""Run the Kate 1-minute backtest with the production volatility blackout."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


SCRIPT = Path(__file__).with_name("vectorbt-backtest-2026-05-09.py")

sys.argv = [
    str(SCRIPT),
    "--timeframe-minutes",
    "1",
    "--files",
    "MESM26-CME.scid",
    "--max-gb",
    "1.1",
    "--start",
    "2026-03-23T13:55:00",
    "--end",
    "2026-05-07T22:33:00",
    "--blackout-windows-utc",
    "13:30-14:30",
    "--metrics-json",
    str(Path(__file__).with_name("vectorbt-backtest-2026-05-09-1m-blackout-metrics.json")),
    "--equity-png",
    str(Path(__file__).with_name("vectorbt-backtest-2026-05-09-1m-blackout-equity.png")),
]

runpy.run_path(str(SCRIPT), run_name="__main__")
