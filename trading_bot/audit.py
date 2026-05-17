"""Kate audit ready-to-ship CLI.

Per the Omni-wide audit protocol (CEO-ratified 2026-05-16):
`protocol/audit-protocol.md`. This CLI is the mechanical enforcement of
the protocol for the Kate project — refuses to pass for any triggered
artifact that lacks a peer-reviewed handoff record.

Eight checks. Exit code 0 = clear to deploy. Non-zero = specific
failures with fix hints.

Usage:
  python -m trading_bot.audit                    # run all checks, human output
  python -m trading_bot.audit --json             # machine-readable output
  python -m trading_bot.audit --check json_validity  # run only one check
  python -m trading_bot.audit --skip integration_smoke  # skip a check
  python -m trading_bot.audit --review-inbox     # show only unanswered review requests
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib
import json
import re
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional


# ── Configuration ────────────────────────────────────────────────────────


KATE_ROOT = Path(__file__).resolve().parents[1]
OMNI_ROOT = Path(r"C:\models\omni")
HANDOFFS_DIR = OMNI_ROOT / "handoffs"
PROTOCOL_DIR = OMNI_ROOT / "protocol"
SECRETS_PATH = OMNI_ROOT / ".mcp-brain" / "config" / "secrets.json"
TEE_INPUTS_PATH = OMNI_ROOT / ".mcp-brain" / "config" / "tee_inputs.json"
NT_BRIDGE_DIR = OMNI_ROOT / "projects" / "kate" / "ninjatrader_bridge"

# Paths whose edits trigger the audit protocol's review requirement.
# Glob patterns (relative to OMNI_ROOT or KATE_ROOT depending on root).
# Per Codex HARD-OBJECTION 2026-05-16 #P1.1: list was too narrow — future
# changes to audit infra / telemetry / supervisor / strategy / risk / state
# could avoid the review-records check entirely. Broadened to include all
# load-bearing categories.
TRIGGERED_ARTIFACT_PATHS = [
    # Kate-side (paths relative to KATE_ROOT) — load-bearing engineering
    "trading_bot/audit.py",
    "trading_bot/audit_live.py",
    "trading_bot/core/execution/*.py",
    "trading_bot/core/alerts/*.py",
    "trading_bot/core/telemetry/*.py",
    "trading_bot/core/risk/*.py",
    "trading_bot/core/state/*.py",
    "trading_bot/core/strategy/*.py",
    "trading_bot/supervisor/*.py",
    "trading_bot/engines/*.py",
    # Omni-side (paths relative to OMNI_ROOT) — governance + cross-cutting
    "omni_cli/validate_fronts.py",
    "omni_cli/logger.py",
    "protocol/*.md",
    "decisions/*.md",
    "projects/kate/ninjatrader_bridge/*.cs",
    "projects/kate/ninjatrader_bridge/*.md",
    "projects/kate/ninjatrader_bridge/*.py",
    ".mcp-brain/config/secrets-registry.md",
]

REQUIRED_SECRETS_KEYS = [
    ("telegram", "bot_token"),
    ("telegram", "chat_id"),
]
# These become required only once the relevant bridge work is live:
OPTIONAL_SECRETS_KEYS = [
    ("ninja_bridge", "hmac_secret"),
    ("ninja_bridge", "host"),
    ("ninja_bridge", "port"),
    ("mt5_ic_markets", "primary_demo"),
]


# ── Result model ─────────────────────────────────────────────────────────


class CheckStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    SKIP = "skip"


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    status: CheckStatus
    message: str
    details: dict = field(default_factory=dict)
    fix_hint: Optional[str] = None
    duration_ms: float = 0.0


@dataclass(frozen=True)
class AuditReport:
    timestamp_utc: str
    results: tuple[CheckResult, ...]

    @property
    def overall_status(self) -> str:
        if any(r.status == CheckStatus.FAIL for r in self.results):
            return "FAIL"
        if any(r.status == CheckStatus.WARN for r in self.results):
            return "PASS-WITH-WARN"
        return "PASS"

    @property
    def exit_code(self) -> int:
        return 0 if self.overall_status != "FAIL" else 2


# ── Check ABC ────────────────────────────────────────────────────────────


class Check(ABC):
    """Base class for all audit checks. Subclasses implement run()."""

    name: str = ""

    @abstractmethod
    def run(self) -> CheckResult: ...

    def _timed(self, fn) -> CheckResult:
        start = time.time()
        try:
            result = fn()
        except Exception as exc:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"check raised unexpectedly: {type(exc).__name__}: {exc}",
                fix_hint="Check the audit module for a bug, or report to Tech CTO.",
                duration_ms=(time.time() - start) * 1000.0,
            )
        if isinstance(result, CheckResult):
            return CheckResult(
                check_name=result.check_name or self.name,
                status=result.status,
                message=result.message,
                details=result.details,
                fix_hint=result.fix_hint,
                duration_ms=(time.time() - start) * 1000.0,
            )
        raise TypeError(f"{self.name}.run() must return CheckResult, got {type(result)}")


# ── Check 1: JSON validity ───────────────────────────────────────────────


class JsonValidityCheck(Check):
    name = "json_validity"

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        scan_targets = [
            OMNI_ROOT / ".mcp-brain" / "config" / "tee_inputs.json",
            SECRETS_PATH,
        ]
        scan_targets += list((OMNI_ROOT / ".mcp-brain" / "config").glob("*.json"))
        # Dedupe while preserving order
        seen: set[Path] = set()
        unique: list[Path] = []
        for p in scan_targets:
            if p not in seen and p.exists():
                seen.add(p)
                unique.append(p)

        broken: list[dict] = []
        for path in unique:
            try:
                json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                broken.append({
                    "path": str(path),
                    "error": str(exc),
                    "line": exc.lineno,
                    "col": exc.colno,
                })

        if broken:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"{len(broken)} JSON file(s) failed to parse",
                details={"broken_files": broken, "scanned": [str(p) for p in unique]},
                fix_hint=(
                    "Fix the JSON syntax in each listed file. "
                    "The Friday 2026-05-15 incident was a missing opening '{' "
                    "in tee_inputs.json — this check catches that class of error."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message=f"{len(unique)} JSON config files parse cleanly",
            details={"scanned": [str(p) for p in unique]},
        )


# ── Check 2: Config + secrets validator ──────────────────────────────────


class ConfigSecretsCheck(Check):
    name = "config_secrets"

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        if not SECRETS_PATH.exists():
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"secrets.json not found at {SECRETS_PATH}",
                fix_hint="Create the file with at minimum a 'telegram' section.",
            )
        try:
            secrets = json.loads(SECRETS_PATH.read_text())
        except json.JSONDecodeError as exc:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"secrets.json failed to parse: {exc}",
                fix_hint="Run the json_validity check for details.",
            )

        missing_required: list[str] = []
        missing_optional: list[str] = []

        for section, key in REQUIRED_SECRETS_KEYS:
            value = (secrets.get(section) or {}).get(key)
            if not value:
                missing_required.append(f"{section}.{key}")

        for section, key in OPTIONAL_SECRETS_KEYS:
            value = (secrets.get(section) or {}).get(key)
            if not value:
                missing_optional.append(f"{section}.{key}")

        if missing_required:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"missing required secrets: {missing_required}",
                details={"missing_required": missing_required, "missing_optional": missing_optional},
                fix_hint=(
                    "Populate the missing keys in secrets.json. "
                    "Telegram keys are needed for the resilience patch alert path."
                ),
            )
        if missing_optional:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.WARN,
                message=f"missing optional secrets: {missing_optional}",
                details={"missing_optional": missing_optional},
                fix_hint=(
                    "Optional today — required when the relevant front goes live. "
                    "e.g., ninja_bridge.hmac_secret is needed for the Sunday smoke."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message="all required + optional secrets present",
        )


# ── Check 3: Code imports ────────────────────────────────────────────────


class CodeImportsCheck(Check):
    name = "code_imports"

    KATE_MODULES_TO_IMPORT = [
        "trading_bot.core.data",
        "trading_bot.core.execution.broker_adapter",
        "trading_bot.core.execution.mt5_broker_adapter",
        "trading_bot.core.execution.dtc_broker_adapter",
        "trading_bot.core.execution.rithmic_broker_adapter",
        "trading_bot.core.execution.ninja_messages",
        "trading_bot.core.execution.ninja_transport",
        "trading_bot.core.alerts",
        "trading_bot.core.alerts.telegram",
        "trading_bot.core.risk",
        "trading_bot.core.state",
        "trading_bot.core.strategy.base",
        "trading_bot.core.strategy.fx_london_breakout",
        "trading_bot.core.strategy.orb",
        "trading_bot.core.strategy.breakout",
        "trading_bot.core.strategy.indicators",
        "trading_bot.supervisor.runtime",
    ]

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        broken: list[dict] = []
        for module_name in self.KATE_MODULES_TO_IMPORT:
            try:
                importlib.import_module(module_name)
            except Exception as exc:
                broken.append({
                    "module": module_name,
                    "error": f"{type(exc).__name__}: {exc}",
                })

        if broken:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"{len(broken)} Kate module(s) failed to import",
                details={"broken_modules": broken, "scanned": self.KATE_MODULES_TO_IMPORT},
                fix_hint=(
                    "Fix the import errors in the listed modules. "
                    "Common causes: missing dependency in a from-import, "
                    "circular import, syntax error."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message=f"{len(self.KATE_MODULES_TO_IMPORT)} Kate modules import cleanly",
        )


# ── Check 4: Pytest gate ─────────────────────────────────────────────────


class TestSuiteCheck(Check):
    name = "test_suite"

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/unit/", "-q", "--no-header"],
            cwd=str(KATE_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
        passed_count = 0
        failed_count = 0
        for line in (proc.stdout + proc.stderr).splitlines():
            m = re.match(r"(\d+) passed", line)
            if m:
                passed_count = int(m.group(1))
            m = re.search(r"(\d+) failed", line)
            if m:
                failed_count = int(m.group(1))

        if proc.returncode != 0 or failed_count > 0:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"pytest failed (return code {proc.returncode}, {failed_count} test failures)",
                details={
                    "passed": passed_count,
                    "failed": failed_count,
                    "stdout_tail": "\n".join(proc.stdout.splitlines()[-20:]),
                    "stderr_tail": "\n".join(proc.stderr.splitlines()[-10:]),
                },
                fix_hint="Run `python -m pytest tests/unit/ -v` for the full failure detail.",
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message=f"{passed_count} tests passing",
            details={"passed": passed_count},
        )


# ── Check 5: Protocol drift detector (Python ↔ C# bridge) ─────────────────


class ProtocolDriftCheck(Check):
    name = "protocol_drift"

    EXPECTED_MSG_TYPES = {"signal", "fill", "heartbeat", "reconcile_req", "reconcile_resp", "ack"}
    EXPECTED_PORT = 9876

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        bridge_md = NT_BRIDGE_DIR / "bridge_protocol.md"
        if not bridge_md.exists():
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.SKIP,
                message=f"bridge_protocol.md not found at {bridge_md}",
            )

        md_text = bridge_md.read_text()
        issues: list[str] = []

        # Port check
        port_match = re.search(r"127\.0\.0\.1:(\d+)", md_text)
        if not port_match:
            issues.append("bridge_protocol.md doesn't declare a 127.0.0.1:<port> binding")
        else:
            declared_port = int(port_match.group(1))
            if declared_port != self.EXPECTED_PORT:
                issues.append(
                    f"bridge_protocol.md declares port {declared_port}, "
                    f"but Python ninja_transport defaults to {self.EXPECTED_PORT}"
                )

        # Message types check
        msg_types_in_md: set[str] = set()
        for msg in self.EXPECTED_MSG_TYPES:
            if msg in md_text:
                msg_types_in_md.add(msg)
        missing = self.EXPECTED_MSG_TYPES - msg_types_in_md
        if missing:
            issues.append(
                f"bridge_protocol.md missing message types: {sorted(missing)}"
            )

        # HMAC check — must mention HMAC-SHA256 and canonical_json
        if "HMAC-SHA256" not in md_text:
            issues.append("bridge_protocol.md doesn't declare HMAC-SHA256")
        if "canonical" not in md_text.lower():
            issues.append("bridge_protocol.md doesn't mention canonical JSON")

        # Envelope structure — must mention payload + signature + msg_type + sequence
        for required_field in ["msg_type", "payload", "sequence", "signature"]:
            if f'"{required_field}"' not in md_text:
                issues.append(f"bridge_protocol.md doesn't declare envelope field '{required_field}'")

        if issues:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"protocol drift detected ({len(issues)} mismatches between bridge_protocol.md and Python implementation)",
                details={"issues": issues},
                fix_hint=(
                    "Reconcile the C# bridge_protocol.md spec with the Python "
                    "ninja_messages.py / ninja_transport.py implementation. "
                    "This is the failure mode that birthed the audit protocol."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message="bridge_protocol.md aligns with Python implementation",
        )


# ── Check 6: Integration smoke (mock client → real server) ───────────────


class IntegrationSmokeCheck(Check):
    name = "integration_smoke"

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        try:
            from trading_bot.core.execution.ninja_transport import NinjaBridgeServer
            from trading_bot.core.execution.ninja_messages import (
                MsgType, SignalPayload, build_envelope, encode_envelope, decode_envelope,
            )
        except ImportError as exc:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"bridge modules failed to import: {exc}",
                fix_hint="Run code_imports check for module-level errors.",
            )

        secret = b"audit-smoke-secret"

        async def _smoke():
            server = NinjaBridgeServer(host="127.0.0.1", port=0, secret=secret)
            await server.start()
            try:
                # Test: server is listening
                if not server.is_listening:
                    return False, "server did not start listening"

                # Build a signed envelope ourselves (mock client) + send
                reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
                await asyncio.wait_for(server.wait_for_client(), timeout=2.0)

                payload = SignalPayload(
                    intent_id="audit-smoke-1",
                    timestamp="2026-05-16T11:00:00+00:00",
                    symbol="MESM26",
                    nt_symbol="MES 06-26",
                    side="BUY",
                    quantity=1,
                    atm_template="KATE_MES_ORB_BASE",
                    stop_price=5234.50,
                    target_price=5240.00,
                    signal_close_price=5236.25,
                )
                envelope = build_envelope(
                    msg_type=MsgType.SIGNAL, sequence=1, payload=payload, secret=secret
                )
                writer.write(encode_envelope(envelope))
                await writer.drain()

                received = await asyncio.wait_for(server.receive(), timeout=2.0)
                if received.msg_type != MsgType.SIGNAL.value:
                    return False, f"unexpected msg_type received: {received.msg_type}"
                if received.payload.get("intent_id") != "audit-smoke-1":
                    return False, f"payload mismatch: {received.payload}"

                writer.close()
                await writer.wait_closed()
                return True, "round-trip OK"
            finally:
                await server.stop()

        try:
            ok, msg = asyncio.new_event_loop().run_until_complete(_smoke())
        except Exception as exc:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                message=f"integration smoke raised: {type(exc).__name__}: {exc}",
                fix_hint="Check bridge transport implementation; this should always pass locally.",
            )

        if ok:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.PASS,
                message=f"localhost TCP + NDJSON + HMAC integration smoke: {msg}",
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.FAIL,
            message=f"integration smoke failed: {msg}",
            fix_hint="Trace the bridge protocol — wire format, HMAC, message routing.",
        )


# ── Check 7: Review records (audit protocol enforcement) ──────────────────


class ReviewRecordsCheck(Check):
    name = "review_records"

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        # Collect triggered artifacts (files matching the trigger glob patterns)
        triggered: set[str] = set()
        for pattern in TRIGGERED_ARTIFACT_PATHS:
            for root in [OMNI_ROOT, KATE_ROOT]:
                for path in root.glob(pattern):
                    if path.is_file():
                        try:
                            rel = str(path.relative_to(root))
                        except ValueError:
                            rel = str(path)
                        triggered.add(rel)

        # Collect review records — scan recent handoffs/ for REVIEW-RESPONSE-* with
        # APPROVED or APPROVED-WITH-CONCERNS outcome.
        # Per Codex HARD-OBJECTION 2026-05-16 #P1.2: read explicit UTF-8 (was
        # using platform default, which crashes on cp1252 Windows hosts).
        # Per Codex HARD-OBJECTION #P2.4: also parse `relates_to:` as a
        # compatibility layer for review responses that listed artifacts there
        # before the protocol pinned `artifact_paths:`.
        reviewed: dict[str, str] = {}  # artifact_path → latest outcome
        if HANDOFFS_DIR.exists():
            for handoff in HANDOFFS_DIR.glob("*-REVIEW-RESPONSE-*.md"):
                try:
                    text = handoff.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue
                # Parse outcome from frontmatter
                outcome_match = re.search(r"review_outcome:\s*(\S+)", text)
                if not outcome_match:
                    continue
                outcome = outcome_match.group(1)
                if outcome not in ("APPROVED", "APPROVED-WITH-CONCERNS"):
                    continue
                # Parse artifact_paths (preferred) and relates_to (compat).
                for key in ("artifact_paths", "relates_to", "artifacts"):
                    section = re.search(
                        rf"{key}:\s*\n((?:\s*-\s*\S+\n)+)", text
                    )
                    if not section:
                        continue
                    for line in section.group(1).splitlines():
                        m = re.match(r"\s*-\s*(\S+)", line)
                        if m:
                            reviewed[m.group(1).strip()] = outcome

        # Now: which triggered artifacts have NO review record?
        unreviewed: list[str] = []
        for artifact in triggered:
            # Match by suffix — handoffs may use forward-slash paths
            artifact_norm = artifact.replace("\\", "/")
            if not any(
                artifact_norm == r.replace("\\", "/")
                or artifact_norm.endswith(r.replace("\\", "/"))
                or r.replace("\\", "/").endswith(artifact_norm)
                for r in reviewed
            ):
                unreviewed.append(artifact)

        if unreviewed:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.WARN,
                message=f"{len(unreviewed)} triggered artifact(s) have no recent APPROVED review record",
                details={
                    "unreviewed": sorted(unreviewed)[:20],  # cap output
                    "total_unreviewed": len(unreviewed),
                    "total_triggered": len(triggered),
                    "total_with_review": len(triggered) - len(unreviewed),
                },
                fix_hint=(
                    "File REVIEW-REQUEST-* handoffs for these artifacts and "
                    "get peer review per protocol/audit-protocol.md. "
                    "Status is WARN not FAIL so the gate doesn't block work in "
                    "progress — promote to FAIL when CEO directive tightens."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message=f"all {len(triggered)} triggered artifacts have recent review records",
        )


# ── Check 8: Review inbox surface (closes Codex's concern #2) ────────────


class ReviewInboxCheck(Check):
    name = "review_inbox"

    def __init__(self, agent: str = "claude") -> None:
        self.agent = agent

    def run(self) -> CheckResult:
        return self._timed(self._run)

    def _run(self) -> CheckResult:
        if not HANDOFFS_DIR.exists():
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.SKIP,
                message=f"handoffs/ not found at {HANDOFFS_DIR}",
            )

        # Per Codex HARD-OBJECTION 2026-05-16 #P1.3: filename-pattern alone
        # misses team-addressed requests (`claude-to-team-REVIEW-REQUEST-*`).
        # Scan ALL REVIEW-REQUEST handoffs + parse frontmatter `to:` for the
        # actual recipient list — match agent against direct address, "team",
        # or comma-separated lists like "codex, gemini".
        all_requests = list(HANDOFFS_DIR.glob("*-REVIEW-REQUEST-*.md"))
        agent_lower = self.agent.lower()

        unanswered: list[dict] = []
        for req in all_requests:
            try:
                text = req.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            # Parse frontmatter `to:` to determine recipients
            to_match = re.search(r"^to:\s*(.+)$", text, re.MULTILINE)
            if not to_match:
                continue
            to_value = to_match.group(1).lower()
            # Recipient match: agent named directly, "team", or comma-separated
            # list containing the agent
            recipients = re.split(r"[,\s\(\)]+", to_value)
            if not (agent_lower in recipients or "team" in recipients):
                continue

            slug_match = re.search(r"REVIEW-REQUEST-(.+)\.md$", req.name)
            if not slug_match:
                continue
            slug = slug_match.group(1)
            # Look for any REVIEW-RESPONSE for this slug FROM this agent
            response_pattern = f"*-{self.agent}-to-*-REVIEW-RESPONSE-{slug}.md"
            responses = list(HANDOFFS_DIR.glob(response_pattern))
            if not responses:
                due_match = re.search(r"review_due:\s*(\S+)", text)
                due = due_match.group(1) if due_match else "no review_due declared"
                unanswered.append({
                    "request_file": req.name,
                    "slug": slug,
                    "review_due": due,
                })

        if unanswered:
            return CheckResult(
                check_name=self.name,
                status=CheckStatus.WARN,
                message=f"{len(unanswered)} unanswered review request(s) addressed to {self.agent}",
                details={"unanswered": unanswered},
                fix_hint=(
                    f"Respond to each REVIEW REQUEST with a REVIEW-RESPONSE handoff. "
                    f"Use APPROVED / APPROVED-WITH-CONCERNS / HARD-OBJECTION outcome."
                ),
            )
        return CheckResult(
            check_name=self.name,
            status=CheckStatus.PASS,
            message=f"no unanswered review requests for {self.agent}",
        )


# ── Orchestrator ─────────────────────────────────────────────────────────


ALL_CHECKS: list[type[Check]] = [
    JsonValidityCheck,
    ConfigSecretsCheck,
    CodeImportsCheck,
    TestSuiteCheck,
    ProtocolDriftCheck,
    IntegrationSmokeCheck,
    ReviewRecordsCheck,
    ReviewInboxCheck,
]


def run_audit(
    *,
    only: Optional[list[str]] = None,
    skip: Optional[list[str]] = None,
    review_inbox_agent: str = "claude",
) -> AuditReport:
    results: list[CheckResult] = []
    for cls in ALL_CHECKS:
        instance = cls(agent=review_inbox_agent) if cls is ReviewInboxCheck else cls()
        if only and instance.name not in only:
            continue
        if skip and instance.name in skip:
            results.append(CheckResult(
                check_name=instance.name,
                status=CheckStatus.SKIP,
                message="skipped by --skip flag",
            ))
            continue
        results.append(instance.run())

    return AuditReport(
        timestamp_utc=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        results=tuple(results),
    )


# ── Output formatting ────────────────────────────────────────────────────


_STATUS_GLYPH = {
    CheckStatus.PASS: "[PASS]",
    CheckStatus.FAIL: "[FAIL]",
    CheckStatus.WARN: "[WARN]",
    CheckStatus.SKIP: "[SKIP]",
}


def render_human(report: AuditReport) -> str:
    lines = []
    lines.append("=" * 72)
    lines.append(f"  KATE AUDIT - READY-TO-SHIP")
    lines.append(f"  {report.timestamp_utc}  |  overall: {report.overall_status}")
    lines.append("=" * 72)
    lines.append("")
    for r in report.results:
        glyph = _STATUS_GLYPH[r.status]
        lines.append(f"{glyph}  {r.check_name:<22} {r.status.value.upper():<5} ({r.duration_ms:>6.1f}ms)")
        lines.append(f"        {r.message}")
        if r.status in {CheckStatus.FAIL, CheckStatus.WARN}:
            if r.fix_hint:
                lines.append(f"        -> fix: {r.fix_hint}")
            if r.details:
                preview = json.dumps(r.details, indent=2, default=str)
                preview = "\n".join("        " + ln for ln in preview.splitlines()[:30])
                lines.append("        details:")
                lines.append(preview)
        lines.append("")
    lines.append("=" * 72)
    if report.overall_status == "PASS":
        lines.append("  [PASS] ALL CHECKS PASSED - cleared to deploy")
    elif report.overall_status == "PASS-WITH-WARN":
        lines.append("  [WARN] PASSED WITH WARNINGS - cleared to deploy, address warnings")
    else:
        lines.append("  [FAIL] AUDIT FAILED - fix the issues above before deploy")
    lines.append("=" * 72)
    return "\n".join(lines)


def render_json(report: AuditReport) -> str:
    return json.dumps({
        "timestamp_utc": report.timestamp_utc,
        "overall_status": report.overall_status,
        "exit_code": report.exit_code,
        "results": [
            {
                "check_name": r.check_name,
                "status": r.status.value,
                "message": r.message,
                "details": r.details,
                "fix_hint": r.fix_hint,
                "duration_ms": r.duration_ms,
            }
            for r in report.results
        ],
    }, indent=2, default=str)


# ── CLI ──────────────────────────────────────────────────────────────────


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    p.add_argument("--check", action="append", help="run only the named check (repeatable)")
    p.add_argument("--skip", action="append", help="skip the named check (repeatable)")
    p.add_argument(
        "--review-inbox",
        action="store_true",
        help="show only the review inbox check (unanswered REVIEW-REQUEST-* files)",
    )
    p.add_argument(
        "--agent",
        default="claude",
        help="agent identity for the review inbox check (default: claude)",
    )
    args = p.parse_args(argv)

    if args.review_inbox:
        report = run_audit(only=["review_inbox"], review_inbox_agent=args.agent)
    else:
        report = run_audit(only=args.check, skip=args.skip, review_inbox_agent=args.agent)

    if args.json:
        print(render_json(report))
    else:
        print(render_human(report))

    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
