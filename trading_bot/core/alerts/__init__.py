"""Out-of-band alerting (Telegram).

Used by adapters/strategies to surface operational events (disconnects,
kill-switches, drift warnings) outside the main BrokerEvent stream.

Per Gemini's resilience directive 2026-05-15: silent disconnects are not
acceptable. Operational events that block trading must page the operator
within minutes, not be discovered hours later in an audit.
"""
from .telegram import push_telegram_alert

__all__ = ["push_telegram_alert"]
