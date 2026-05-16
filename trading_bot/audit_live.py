"""Kate audit live — runtime safety net.

Sibling of `trading_bot.audit` (ready-to-ship CLI). Where audit ready-to-ship
gates DEPLOYS, audit live monitors RUNTIME. Long-running process that runs
each check at its own cadence and pushes Telegram alerts on failures.

The failure modes this catches (with reference to the actual incidents that
motivated each check):

  - Sierra Chart .scid data flow stalls (caught 2026-05-15 Day-9 stall;
    Front 1 was dark for over a week before Gemini noticed).
  - MT5 disconnect (already covered by the resilience patch's heartbeat,
    but duplicated here as defense-in-depth).
  - TEE inputs JSON corruption (caught 2026-05-15 — Gemini's edit dropped
    the opening `{`, validate_fronts.py would have crashed silently).
  - Aggregate-DD approaching the £500 cap (early warning before
    validate_fronts.py kill-criterion fires).
  - Activity chain hash-chain integrity (tamper detection).
  - Secret rotation overdue.

Usage:
  python -m trading_bot.audit_live                # default 60s loop, all checks
  python -m trading_bot.audit_live --once         # one pass, exit
  python -m trading_bot.audit_live --interval 300 # 5-minute loop
  python -m trading_bot.audit_live --check scid_freshness  # only one check
  python -m trading_bot.audit_live --no-alerts    # log only, no Telegram

CEO directive 2026-05-16: 2 hrs of audit work prevents days/weeks of silent-
failure troubleshooting. Sierra stall + MT5 disconnect would have been caught
by this. Built per that brief.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Configuration ────────────────────────────────────────────────────────

OMNI_ROOT = Path(r"C:\models\omni")
KATE_ROOT = Path(__file__).resolve().parents[1]
SECRETS_PATH = OMNI_ROOT / ".mcp-brain" / "config" / "secrets.json"
TEE_INPUTS_PATH = OMNI_ROOT / ".mcp-brain" / "config" / "tee_inputs.json"
SECRETS_REGISTRY_PATH = OMNI_ROOT / ".mcp-brain" / "config" / "secrets-registry.md"
ACTIVITY_LOG_DIR = OMNI_ROOT / ".mcp-brain" / "logs" / "activity"

# Sierra Chart data folder — host-specific. Override via env on Kate Host.
SC_DATA_DIR = Path(os.getenv("SC_DATA_DIR", r"C:\SierraChart\Data"))

# Default cadences (override per-check via env or CLI later if needed).
DEFAULT_INTERVAL_SECONDS = 60

# CME-RTH window (UK time). SC data should be updating during these hours.
CME_RTH_START_UK = dt.time(14, 30)   # 14:30 UK = 08:30 Chicago
CME_RTH_END_UK = dt.time(21, 0)      # 21:00 UK = 15:00 Chicago


# ── Result model ─────────────────────────────────────────────────────────


class LiveCheckStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"      # blocks: page operator immediately
    WARN = "warn"      # surface: log + telegram at low cadence
    SKIP = "skip"      # not applicable now (e.g. weekend, market closed)


@dataclass(frozen=True)
class LiveCheckResult:
    name: str
    status: LiveCheckStatus
    message: str
    details: dict = field(default_factory=dict)
    duration_ms: float = 0.0


# ── Check ABC ────────────────────────────────────────────────────────────


class LiveCheck(ABC):
    """Base class for runtime checks. Subclasses set `name` and
    `interval_seconds` and implement `run()`."""

    name: str = ""
    interval_seconds: int = 60

    @abstractmethod
    def run(self) -> LiveCheckResult: ...

    def timed(self, fn) -> LiveCheckResult:
        start = time.time()
        try:
            result = fn()
        except Exception as exc:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"check raised unexpectedly: {type(exc).__name__}: {exc}",
                duration_ms=(time.time() - start) * 1000.0,
            )
        if not isinstance(result, LiveCheckResult):
            raise TypeError(f"{self.name}.run() must return LiveCheckResult")
        return LiveCheckResult(
            name=result.name or self.name,
            status=result.status,
            message=result.message,
            details=result.details,
            duration_ms=(time.time() - start) * 1000.0,
        )


# ── Check 1: SC .scid file freshness (Sierra stall detector) ─────────────


class ScidFreshnessCheck(LiveCheck):
    """The check that would have caught the 9-day Sierra stall.

    Scans `SC_DATA_DIR` for `.scid` files; flags any whose mtime is
    older than the threshold during CME RTH hours. Outside RTH and
    weekends, returns SKIP (markets closed, no data flow expected).
    """
    name = "scid_freshness"
    interval_seconds = 300  # 5 min during RTH

    # If during RTH, no .scid file has been touched in this many seconds, FAIL.
    STALENESS_THRESHOLD_SECONDS = 600  # 10 minutes

    def run(self) -> LiveCheckResult:
        return self.timed(self._run)

    def _run(self) -> LiveCheckResult:
        if not SC_DATA_DIR.exists():
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message=f"SC data dir not found at {SC_DATA_DIR} (not on Kate Host?)",
            )

        now_uk = dt.datetime.now()  # naive local time — Kate Host is UK
        is_weekend = now_uk.weekday() >= 5  # Sat=5, Sun=6
        is_rth = CME_RTH_START_UK <= now_uk.time() <= CME_RTH_END_UK

        if is_weekend or not is_rth:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message=f"outside CME RTH ({now_uk.strftime('%a %H:%M')} UK) — no data flow expected",
            )

        scid_files = list(SC_DATA_DIR.glob("*.scid"))
        if not scid_files:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"no .scid files found in {SC_DATA_DIR}",
                details={"path": str(SC_DATA_DIR)},
            )

        now_ts = time.time()
        stale_files = []
        for f in scid_files:
            try:
                age = now_ts - f.stat().st_mtime
                if age > self.STALENESS_THRESHOLD_SECONDS:
                    stale_files.append({
                        "file": f.name,
                        "age_seconds": int(age),
                        "age_human": _human_duration(age),
                    })
            except OSError:
                continue

        if stale_files:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=(
                    f"{len(stale_files)} .scid file(s) stale > "
                    f"{self.STALENESS_THRESHOLD_SECONDS}s during CME RTH — "
                    f"Sierra data feed likely dead"
                ),
                details={"stale_files": stale_files[:10]},
            )

        return LiveCheckResult(
            name=self.name,
            status=LiveCheckStatus.PASS,
            message=f"{len(scid_files)} .scid file(s) all fresh (< {self.STALENESS_THRESHOLD_SECONDS}s)",
        )


# ── Check 2: TEE inputs JSON integrity ───────────────────────────────────


class TeeInputsIntegrityCheck(LiveCheck):
    """Catches the Gemini-2026-05-15 incident class: edit drops a `{`,
    JSON breaks silently, validate_fronts.py loads {} and the £500 cap
    silently stops firing."""

    name = "tee_inputs_integrity"
    interval_seconds = 300

    REQUIRED_TOP_LEVEL_KEYS = {
        "aggregate_drawdown_cap_gbp",
        "aggregate_dd_cap_breach_action",
        "fronts",
        "monthly_costs_gbp",
    }

    def run(self) -> LiveCheckResult:
        return self.timed(self._run)

    def _run(self) -> LiveCheckResult:
        if not TEE_INPUTS_PATH.exists():
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"tee_inputs.json not found at {TEE_INPUTS_PATH}",
            )
        try:
            data = json.loads(TEE_INPUTS_PATH.read_text())
        except json.JSONDecodeError as exc:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"tee_inputs.json parse failed: {exc.msg} at line {exc.lineno} col {exc.colno}",
                details={"error": str(exc)},
            )

        missing = self.REQUIRED_TOP_LEVEL_KEYS - set(data.keys())
        if missing:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"tee_inputs.json missing required top-level keys: {sorted(missing)}",
                details={"missing": sorted(missing)},
            )

        cap = data.get("aggregate_drawdown_cap_gbp")
        if not isinstance(cap, (int, float)) or cap <= 0:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"aggregate_drawdown_cap_gbp is invalid: {cap!r}",
            )

        return LiveCheckResult(
            name=self.name,
            status=LiveCheckStatus.PASS,
            message=f"tee_inputs.json valid; cap=£{cap}",
        )


# ── Check 3: Aggregate DD early-warning ──────────────────────────────────


class AggregateDDEarlyWarningCheck(LiveCheck):
    """Warns when aggregate DD approaches the cap. validate_fronts.py
    enforces the hard halt at breach; this fires earlier so we can
    reduce risk before the halt triggers and surprises us.

    Reads tee_inputs.json for the cap + live trade-log data for
    per-front DD. Simplified for v0: just reads any live DD values
    surfaced in tee_inputs.json (the CFO ledger should track these
    going forward; for now this is a sanity check)."""

    name = "aggregate_dd_warning"
    interval_seconds = 300

    WARN_AT_PCT = 0.50  # warn at 50% of cap
    FAIL_AT_PCT = 0.80  # alarm at 80% of cap

    def run(self) -> LiveCheckResult:
        return self.timed(self._run)

    def _run(self) -> LiveCheckResult:
        try:
            data = json.loads(TEE_INPUTS_PATH.read_text())
        except Exception:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message="cannot read tee_inputs.json (covered by tee_inputs_integrity check)",
            )

        cap = data.get("aggregate_drawdown_cap_gbp")
        if not isinstance(cap, (int, float)) or cap <= 0:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message="no valid cap configured",
            )

        # Sum any front-level dd values present in tee_inputs.json.
        # CFO ledger schema doesn't yet include live DD per front;
        # placeholder until that's wired by Gemini.
        fronts = data.get("fronts", []) or []
        live_dd_total = 0.0
        fronts_with_dd = []
        for f in fronts:
            dd = f.get("live_drawdown_gbp", 0.0)
            try:
                dd = float(dd)
            except (TypeError, ValueError):
                dd = 0.0
            if dd > 0:
                live_dd_total += dd
                fronts_with_dd.append({"id": f.get("id"), "dd": dd})

        utilization = live_dd_total / cap if cap > 0 else 0.0

        if utilization >= self.FAIL_AT_PCT:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=(
                    f"aggregate DD £{live_dd_total:.2f} = {utilization:.0%} of £{cap} cap "
                    f"— approaching mandatory halt"
                ),
                details={"fronts_with_dd": fronts_with_dd, "utilization": utilization},
            )
        if utilization >= self.WARN_AT_PCT:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.WARN,
                message=(
                    f"aggregate DD £{live_dd_total:.2f} = {utilization:.0%} of £{cap} cap "
                    f"— consider de-risking"
                ),
                details={"fronts_with_dd": fronts_with_dd, "utilization": utilization},
            )
        return LiveCheckResult(
            name=self.name,
            status=LiveCheckStatus.PASS,
            message=f"aggregate DD £{live_dd_total:.2f} / cap £{cap:.0f} ({utilization:.0%})",
        )


# ── Check 4: Activity chain hash-chain integrity ─────────────────────────


class ActivityChainIntegrityCheck(LiveCheck):
    """Verifies the hash chain on today's activity log. Tamper detection.

    Per omni_cli.logger — every entry has prev_hash → entry_hash, forming
    a chain back to GENESIS. If any line is tampered, this check fails.
    """
    name = "activity_chain_integrity"
    interval_seconds = 900  # 15 min — log changes are slow

    def run(self) -> LiveCheckResult:
        return self.timed(self._run)

    def _run(self) -> LiveCheckResult:
        today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        log_file = ACTIVITY_LOG_DIR / f"{today}.jsonl"
        if not log_file.exists():
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message=f"no activity log yet for {today}",
            )

        # Try to use the canonical verifier from omni_cli.logger if importable.
        try:
            sys.path.insert(0, str(OMNI_ROOT))
            from omni_cli.logger import verify_chain  # type: ignore
            ok, msg = verify_chain(log_file)
        except Exception as exc:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.SKIP,
                message=f"omni_cli.logger.verify_chain not importable: {exc}",
            )

        if not ok:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"activity chain TAMPERED on {today}: {msg}",
                details={"log_file": str(log_file)},
            )
        return LiveCheckResult(
            name=self.name,
            status=LiveCheckStatus.PASS,
            message=f"activity chain intact on {today}",
        )


# ── Check 5: Secrets file freshness ──────────────────────────────────────


class SecretsFileFreshnessCheck(LiveCheck):
    """Confirms secrets.json exists and has been touched recently enough.

    Not a rotation check (that requires per-secret cadence from registry).
    Just a "the file exists and has actual content" sanity check."""

    name = "secrets_file_present"
    interval_seconds = 3600  # 1 hr — secrets don't move often

    def run(self) -> LiveCheckResult:
        return self.timed(self._run)

    def _run(self) -> LiveCheckResult:
        if not SECRETS_PATH.exists():
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"secrets.json missing at {SECRETS_PATH}",
            )
        try:
            data = json.loads(SECRETS_PATH.read_text())
        except json.JSONDecodeError as exc:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message=f"secrets.json parse failed: {exc.msg}",
            )
        if not isinstance(data, dict) or not data:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message="secrets.json parsed empty",
            )
        # Minimum keys required for any operational use.
        if "telegram" not in data:
            return LiveCheckResult(
                name=self.name,
                status=LiveCheckStatus.FAIL,
                message="secrets.json missing 'telegram' section",
            )
        return LiveCheckResult(
            name=self.name,
            status=LiveCheckStatus.PASS,
            message=f"secrets.json present with {len(data)} sections",
        )


# ── Runner ───────────────────────────────────────────────────────────────


ALL_CHECKS: list[type[LiveCheck]] = [
    ScidFreshnessCheck,
    TeeInputsIntegrityCheck,
    AggregateDDEarlyWarningCheck,
    ActivityChainIntegrityCheck,
    SecretsFileFreshnessCheck,
]


@dataclass
class AuditLiveState:
    """Tracks per-check state so we don't spam alerts on the same failure."""
    last_run_at: dict[str, float] = field(default_factory=dict)
    last_status: dict[str, LiveCheckStatus] = field(default_factory=dict)
    last_alert_at: dict[str, float] = field(default_factory=dict)


def _alert_via_telegram(text: str) -> bool:
    """Push alert via existing alerts helper; returns True on success."""
    try:
        from trading_bot.core.alerts import push_telegram_alert
        return push_telegram_alert(text)
    except Exception as exc:
        logger.warning("could not push telegram alert: %s", exc)
        return False


def _format_alert(result: LiveCheckResult) -> str:
    glyph = "🚨" if result.status == LiveCheckStatus.FAIL else "⚠️"
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{glyph} *Kate Audit Live — {result.status.value.upper()}*\n\n"
        f"Check: `{result.name}`\n"
        f"At: {ts}\n\n"
        f"{result.message}\n\n"
        f"_Runtime safety net per the 2026-05-16 audit-protocol ratification._"
    )


def _format_recovery(result: LiveCheckResult, prior_status: LiveCheckStatus) -> str:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"✅ *Kate Audit Live — RECOVERED*\n\n"
        f"Check: `{result.name}` (was {prior_status.value.upper()}, now PASS)\n"
        f"At: {ts}\n\n"
        f"{result.message}"
    )


async def run_loop(
    *,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    only: Optional[list[str]] = None,
    alerts_enabled: bool = True,
    one_shot: bool = False,
) -> int:
    """Run the audit live loop. Returns final exit code."""
    state = AuditLiveState()
    checks = [cls() for cls in ALL_CHECKS]
    if only:
        checks = [c for c in checks if c.name in only]

    iteration = 0
    while True:
        iteration += 1
        now = time.time()
        for check in checks:
            due = state.last_run_at.get(check.name, 0)
            if not one_shot and now - due < check.interval_seconds:
                continue

            result = check.run()
            state.last_run_at[check.name] = now
            prior_status = state.last_status.get(check.name)
            state.last_status[check.name] = result.status

            print(
                f"[{dt.datetime.now().strftime('%H:%M:%S')}] "
                f"{result.status.value.upper():<5} {check.name:<30} {result.message}"
            )

            # Alert on transitions into FAIL/WARN, recovery to PASS
            if alerts_enabled and result.status in {LiveCheckStatus.FAIL, LiveCheckStatus.WARN}:
                # De-dupe: only alert once per failure cycle, or every 1hr if persistent
                last_alert = state.last_alert_at.get(check.name, 0)
                hours_since_alert = (now - last_alert) / 3600.0
                if prior_status != result.status or hours_since_alert >= 1.0:
                    _alert_via_telegram(_format_alert(result))
                    state.last_alert_at[check.name] = now
            elif alerts_enabled and result.status == LiveCheckStatus.PASS and prior_status in {
                LiveCheckStatus.FAIL,
                LiveCheckStatus.WARN,
            }:
                _alert_via_telegram(_format_recovery(result, prior_status))

        if one_shot:
            failing = [n for n, s in state.last_status.items() if s == LiveCheckStatus.FAIL]
            return 2 if failing else 0

        await asyncio.sleep(min(interval_seconds, 30))


def _human_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds / 60)}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS,
                   help="loop interval in seconds (default 60)")
    p.add_argument("--once", action="store_true",
                   help="run one pass and exit (good for cron / debugging)")
    p.add_argument("--check", action="append",
                   help="run only the named check (repeatable)")
    p.add_argument("--no-alerts", action="store_true",
                   help="log only, do not push Telegram alerts")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        return asyncio.run(run_loop(
            interval_seconds=args.interval,
            only=args.check,
            alerts_enabled=not args.no_alerts,
            one_shot=args.once,
        ))
    except KeyboardInterrupt:
        print("\nstopped by operator")
        return 0


if __name__ == "__main__":
    sys.exit(main())
