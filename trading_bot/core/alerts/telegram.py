"""Telegram push for Kate operational alerts.

Mirrors omni_cli/signal_publisher.py's telegram_send so Kate-internal code
doesn't depend on the omni package being importable on Kate Host. Reads from
the same secrets.json layout (key path: secrets["telegram"]["bot_token"] +
secrets["telegram"]["chat_id"]).

Best-effort by design: failures must NOT propagate to the trading loop.
Adapters/strategies should swallow the False return and continue.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Default path matches a co-located Omni + Kate install on Kate Host
# (Contabo Windows VPS today; IONOS Windows after migration).
# Override via KATE_SECRETS_PATH env var for tests or alternate layouts.
_DEFAULT_SECRETS_PATH = Path(r"C:\models\omni\.mcp-brain\config\secrets.json")


def _load_telegram_credentials() -> tuple[str, str] | None:
    secrets_path = Path(os.getenv("KATE_SECRETS_PATH", str(_DEFAULT_SECRETS_PATH)))
    if not secrets_path.exists():
        logger.warning("Telegram alert skipped: secrets file not found at %s", secrets_path)
        return None
    try:
        secrets = json.loads(secrets_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Telegram alert skipped: secrets load failed: %s", exc)
        return None
    tg = secrets.get("telegram") or {}
    token = tg.get("bot_token")
    chat_id = tg.get("chat_id")
    if not token or not chat_id:
        logger.warning("Telegram alert skipped: bot_token or chat_id missing in secrets")
        return None
    return token, str(chat_id)


def push_telegram_alert(
    text: str,
    *,
    parse_mode: str = "Markdown",
    timeout_seconds: float = 10.0,
) -> bool:
    """Best-effort Telegram push. Returns True on success, False otherwise.

    Designed for operational alerts from inside Kate adapters. Failure to
    push does not raise — caller logs the False and carries on.
    """
    creds = _load_telegram_credentials()
    if creds is None:
        return False
    token, chat_id = creds
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
            timeout=timeout_seconds,
        )
        r.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.warning("Telegram alert push failed: %s", exc)
        return False
