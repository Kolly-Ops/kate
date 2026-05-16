"""Telemetry — per-trade observability that informs validation decisions.

Slippage telemetry is the first member. Catches the FX backtest's #1
methodology gap (entry-at-close lookahead bias) by measuring actual fill
prices vs signal-time close prices.
"""
from .slippage import SlippageRecorder, SlippageRecord, SlippageSummary

__all__ = ["SlippageRecorder", "SlippageRecord", "SlippageSummary"]
