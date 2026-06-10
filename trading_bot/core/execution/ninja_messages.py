"""Wire-protocol message schemas for the Python ↔ NinjaScript bridge.

Per Codex's 2026-05-15 architecture scope: localhost raw TCP with newline-
delimited JSON (NDJSON), HMAC-protected. This module owns the typed
schemas, canonical JSON serialization, and HMAC envelope/verify logic.

Both sides (Python publisher + NinjaScript KateBridgeStrategy.cs) MUST
produce byte-identical canonical JSON for the HMAC to verify. We pin:
  - sort_keys=True
  - separators=(",", ":")  — no whitespace
  - ensure_ascii=False     — Unicode passes through
  - UTF-8 encoding

The C# side must mirror these choices.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


# ── Message types ─────────────────────────────────────────────────────────


class MsgType(str, Enum):
    SIGNAL = "signal"               # Python → NT: strategy fired an entry
    FILL = "fill"                   # NT → Python: order/bracket state change
    BRACKET_UPDATE = "bracket_update"  # NT -> Python: ATM Stop1/Target1 update evidence
    HEARTBEAT = "heartbeat"         # bidirectional liveness ping
    RECONCILE_REQ = "reconcile_req"  # Python → NT on reconnect
    RECONCILE_RESP = "reconcile_resp"  # NT → Python with current state
    ACK = "ack"                     # generic acknowledgement (by seq)
    BAR = "bar"                     # NT → Python: closed OHLCV bar (Option A data path)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class FillEventType(str, Enum):
    ENTRY = "ENTRY"
    STOP_HIT = "STOP_HIT"
    TARGET_HIT = "TARGET_HIT"
    MANUAL_FLAT = "MANUAL_FLAT"
    OTHER = "OTHER"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


# ── Payload dataclasses ──────────────────────────────────────────────────


@dataclass(frozen=True)
class SignalPayload:
    """Python → NT: strategy fired an entry signal.

    `signal_close_price` doubles as the slippage-telemetry hook — once NT
    fills the entry, the slippage = realized_fill_price - signal_close_price
    (accounting for side).
    """
    intent_id: str
    timestamp: str       # ISO 8601 UTC, e.g. "2026-05-15T22:30:00+00:00"
    symbol: str          # logical symbol, e.g. "MESU26"
    nt_symbol: str       # NT broker symbol, e.g. "MES 09-26"
    side: str            # "BUY" | "SELL"  (use Side enum values)
    quantity: int        # contracts
    atm_template: str    # base template name, e.g. "KATE_MES_ORB_BASE"
    stop_price: float    # absolute SL price (NT side ChangeStopTarget to this)
    target_price: float  # absolute TP price (NT side ChangeStopTarget to this)
    signal_close_price: float  # bar close at signal moment (slippage hook)


@dataclass(frozen=True)
class FillPayload:
    """NT → Python: order/bracket lifecycle event."""
    intent_id: str
    timestamp: str
    event_type: str      # FillEventType.value
    fill_price: float
    fill_quantity: int
    nt_order_id: str
    reason: str = ""     # populated on REJECTED
    symbol: str = ""
    nt_symbol: str = ""
    exit_reason: str = ""
    realized_pnl: float = 0.0


@dataclass(frozen=True)
class BracketUpdatePayload:
    """NT -> Python: smoke/audit evidence for dynamic ATM bracket prices."""
    intent_id: str
    timestamp: str
    symbol: str
    nt_symbol: str
    atm_strategy_id: str
    stop_name: str
    stop_price: float
    target_name: str
    target_price: float


@dataclass(frozen=True)
class HeartbeatPayload:
    timestamp: str
    from_party: str      # "python" | "nt"


@dataclass(frozen=True)
class ReconcileRequestPayload:
    timestamp: str


@dataclass(frozen=True)
class OpenPositionSnapshot:
    symbol: str
    nt_symbol: str
    quantity: int
    side: str
    avg_price: float
    server_position_id: str = ""


@dataclass(frozen=True)
class PendingBracketSnapshot:
    intent_id: str
    atm_strategy_id: str
    status: str
    symbol: str = ""
    side: str = ""
    quantity: int = 0


@dataclass(frozen=True)
class ReconcileResponsePayload:
    timestamp: str
    cash_balance: float = 0.0
    equity: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    margin_used: float = 0.0
    buying_power: float = 0.0
    currency: str = "USD"
    account_name: str = ""
    open_positions: list[OpenPositionSnapshot] = field(default_factory=list)
    pending_brackets: list[PendingBracketSnapshot] = field(default_factory=list)


@dataclass(frozen=True)
class AckPayload:
    """Acknowledge receipt of a message by its sequence number."""
    ack_seq: int
    timestamp: str


@dataclass(frozen=True)
class BarPayload:
    """NT → Python: a closed OHLCV bar from NinjaTrader's own data feed.

    Emitted by `KateBridgeStrategy.cs` on `OnBarUpdate()` when the bar
    finalises (`Calculate.OnBarClose`, `State == Realtime`). One bar per
    instrument per timeframe boundary. Python consumes these via
    NinjaBrokerAdapter and feeds them to the engine as closed candles —
    bypassing TickCandleAggregator, which is the tick-path.

    Dedup contract (Codex's design 2026-05-18):
      - `bar_index` is NT's monotonically increasing CurrentBar value
      - Python rejects duplicate (bar_index, bar_timestamp) pairs that
        carry identical OHLCV (idempotent retransmit)
      - Python flags BAR_REVISION + fails validation day if a duplicate
        (bar_index, bar_timestamp) pair arrives with *different* OHLCV

    Timestamp convention: ISO 8601 UTC, e.g. "2026-05-18T14:31:00+00:00".
    NT emits in its bar-start convention (the timestamp of the bar that
    just closed, not the wallclock at close). Python aligns to this.

    Volume is an integer count of contracts traded inside the bar
    (NinjaTrader's `BarsArray[0].Volumes[CurrentBar]`).
    """
    timestamp: str       # bar-start UTC ISO 8601
    symbol: str          # logical symbol, e.g. "MESU26"
    nt_symbol: str       # NT instrument FullName, e.g. "MES 09-26"
    timeframe_minutes: int  # 1, 5, etc.
    bar_index: int       # NT's CurrentBar at close
    open: float
    high: float
    low: float
    close: float
    volume: int


# ── Wire envelope ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WireEnvelope:
    """Outer wrapper for every NDJSON-on-TCP message.

    The signature is HMAC-SHA256(secret, canonical_json(payload)) as hex.
    Receiver recomputes and compares; mismatch = drop the message + log.
    """
    msg_type: str       # MsgType value
    sequence: int       # monotonic sender-side counter
    payload: dict[str, Any]
    signature: str


# ── Canonical serialization ──────────────────────────────────────────────


def canonical_json(payload: Any) -> bytes:
    """Produce the canonical JSON bytes used as HMAC input.

    Identical output is required on both sides for HMAC to verify. The
    C# NinjaScript side must mirror: sorted keys, no whitespace, UTF-8.
    """
    if hasattr(payload, "__dataclass_fields__"):
        payload = asdict(payload)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sign_payload(secret: bytes, payload: Any) -> str:
    """Return hex-encoded HMAC-SHA256 of canonical(payload)."""
    if not isinstance(secret, (bytes, bytearray)):
        raise TypeError("HMAC secret must be bytes")
    return hmac.new(secret, canonical_json(payload), hashlib.sha256).hexdigest()


def verify_signature(secret: bytes, payload: Any, signature: str) -> bool:
    """Constant-time HMAC verification. False = drop the message."""
    if not isinstance(secret, (bytes, bytearray)):
        raise TypeError("HMAC secret must be bytes")
    expected = sign_payload(secret, payload)
    return hmac.compare_digest(expected, signature)


# ── Envelope build / parse ───────────────────────────────────────────────


def build_envelope(
    *,
    msg_type: MsgType,
    sequence: int,
    payload: Any,
    secret: bytes,
) -> WireEnvelope:
    payload_dict = asdict(payload) if hasattr(payload, "__dataclass_fields__") else dict(payload)
    return WireEnvelope(
        msg_type=msg_type.value,
        sequence=sequence,
        payload=payload_dict,
        signature=sign_payload(secret, payload_dict),
    )


def encode_envelope(envelope: WireEnvelope) -> bytes:
    """Serialize the envelope as one NDJSON line, UTF-8, terminated with \\n."""
    line = json.dumps(
        {
            "msg_type": envelope.msg_type,
            "sequence": envelope.sequence,
            "payload": envelope.payload,
            "signature": envelope.signature,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return (line + "\n").encode("utf-8")


def decode_envelope(line: bytes, *, secret: bytes) -> WireEnvelope:
    """Parse one NDJSON line into an envelope. Raises on HMAC mismatch.

    Caller is responsible for line-splitting upstream — pass one complete
    JSON-terminated line at a time (without the trailing newline).
    """
    obj = json.loads(line.decode("utf-8"))
    expected_keys = {"msg_type", "sequence", "payload", "signature"}
    if not expected_keys.issubset(obj.keys()):
        missing = expected_keys - obj.keys()
        raise ValueError(f"envelope missing keys: {sorted(missing)}")
    if not verify_signature(secret, obj["payload"], obj["signature"]):
        raise ValueError(
            f"HMAC mismatch on msg_type={obj['msg_type']!r} "
            f"sequence={obj['sequence']!r} — message dropped"
        )
    return WireEnvelope(
        msg_type=obj["msg_type"],
        sequence=int(obj["sequence"]),
        payload=obj["payload"],
        signature=obj["signature"],
    )


__all__ = [
    "MsgType",
    "Side",
    "FillEventType",
    "SignalPayload",
    "FillPayload",
    "HeartbeatPayload",
    "ReconcileRequestPayload",
    "OpenPositionSnapshot",
    "PendingBracketSnapshot",
    "ReconcileResponsePayload",
    "AckPayload",
    "BarPayload",
    "WireEnvelope",
    "canonical_json",
    "sign_payload",
    "verify_signature",
    "build_envelope",
    "encode_envelope",
    "decode_envelope",
]
