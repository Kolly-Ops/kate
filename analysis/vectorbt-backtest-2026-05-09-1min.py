"""Run the Kate VectorBT backtest on the production 1-minute MESM26 window."""

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
    "1.0",
    "--metrics-json",
    str(Path(__file__).with_name("vectorbt-backtest-2026-05-09-1min-metrics.json")),
    "--equity-png",
    str(Path(__file__).with_name("vectorbt-backtest-2026-05-09-1min-equity.png")),
]

runpy.run_path(str(SCRIPT), run_name="__main__")
