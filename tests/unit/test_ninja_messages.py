"""Regression tests for the NT bridge wire-protocol schemas + HMAC envelope.

Critical: byte-for-byte canonical JSON must match the NinjaScript C# side
or HMAC verify fails everywhere. These tests pin our canonical format so
any change here forces a coordinated update on the C# side.
"""
from __future__ import annotations

import pytest

from trading_bot.core.execution.ninja_messages import (
    AckPayload,
    FillEventType,
    FillPayload,
    HeartbeatPayload,
    MsgType,
    OpenPositionSnapshot,
    PendingBracketSnapshot,
    ReconcileRequestPayload,
    ReconcileResponsePayload,
    Side,
    SignalPayload,
    WireEnvelope,
    build_envelope,
    canonical_json,
    decode_envelope,
    encode_envelope,
    sign_payload,
    verify_signature,
)


SECRET = b"test-shared-secret-bytes-for-hmac"


# ── Canonical JSON pinning ────────────────────────────────────────────────


def test_canonical_json_sorts_keys():
    a = canonical_json({"b": 2, "a": 1})
    b = canonical_json({"a": 1, "b": 2})
    assert a == b == b'{"a":1,"b":2}'


def test_canonical_json_no_whitespace():
    out = canonical_json({"a": 1, "b": [1, 2, 3]})
    assert b" " not in out
    assert b"\n" not in out
    assert b"\t" not in out


def test_canonical_json_handles_dataclass():
    p = HeartbeatPayload(timestamp="2026-05-15T22:00:00+00:00", from_party="python")
    out = canonical_json(p)
    # asdict-ed and serialized; sorted keys means from_party < timestamp
    assert out == b'{"from_party":"python","timestamp":"2026-05-15T22:00:00+00:00"}'


def test_canonical_json_preserves_unicode():
    # ensure_ascii=False means Unicode passes through literally
    out = canonical_json({"label": "MES 06-26 ✓"})
    assert "✓".encode("utf-8") in out


# ── HMAC sign + verify ────────────────────────────────────────────────────


def test_sign_and_verify_roundtrip():
    payload = {"intent_id": "abc", "qty": 1}
    sig = sign_payload(SECRET, payload)
    assert verify_signature(SECRET, payload, sig) is True


def test_verify_rejects_tampered_payload():
    payload = {"intent_id": "abc", "qty": 1}
    sig = sign_payload(SECRET, payload)
    payload["qty"] = 99  # tamper
    assert verify_signature(SECRET, payload, sig) is False


def test_verify_rejects_wrong_secret():
    payload = {"intent_id": "abc"}
    sig = sign_payload(SECRET, payload)
    assert verify_signature(b"different-secret", payload, sig) is False


def test_verify_rejects_corrupted_signature():
    payload = {"intent_id": "abc"}
    sig = sign_payload(SECRET, payload)
    bad_sig = "0" * len(sig)
    assert verify_signature(SECRET, payload, bad_sig) is False


def test_sign_requires_bytes_secret():
    with pytest.raises(TypeError):
        sign_payload("not-bytes", {"a": 1})  # type: ignore[arg-type]


def test_verify_requires_bytes_secret():
    with pytest.raises(TypeError):
        verify_signature("not-bytes", {"a": 1}, "0" * 64)  # type: ignore[arg-type]


# ── Envelope build / encode / decode roundtrip ───────────────────────────


def test_envelope_roundtrip_signal_payload():
    payload = SignalPayload(
        intent_id="fxlon-GBPUSD-260515",
        timestamp="2026-05-15T07:55:00+00:00",
        symbol="MESM26",
        nt_symbol="MES 06-26",
        side=Side.BUY.value,
        quantity=1,
        atm_template="KATE_MES_ORB_BASE",
        stop_price=5234.50,
        target_price=5240.00,
        signal_close_price=5236.25,
    )
    envelope = build_envelope(
        msg_type=MsgType.SIGNAL, sequence=42, payload=payload, secret=SECRET
    )
    wire = encode_envelope(envelope)
    assert wire.endswith(b"\n")

    decoded = decode_envelope(wire.rstrip(b"\n"), secret=SECRET)
    assert decoded.msg_type == MsgType.SIGNAL.value
    assert decoded.sequence == 42
    assert decoded.payload["intent_id"] == "fxlon-GBPUSD-260515"
    assert decoded.payload["atm_template"] == "KATE_MES_ORB_BASE"
    assert decoded.signature == envelope.signature


def test_envelope_roundtrip_fill_payload():
    payload = FillPayload(
        intent_id="fxlon-GBPUSD-260515",
        timestamp="2026-05-15T07:56:00+00:00",
        event_type=FillEventType.ENTRY.value,
        fill_price=5236.50,
        fill_quantity=1,
        nt_order_id="NT-12345",
    )
    envelope = build_envelope(
        msg_type=MsgType.FILL, sequence=1, payload=payload, secret=SECRET
    )
    wire = encode_envelope(envelope)
    decoded = decode_envelope(wire.rstrip(b"\n"), secret=SECRET)
    assert decoded.msg_type == MsgType.FILL.value
    assert decoded.payload["event_type"] == "ENTRY"
    assert decoded.payload["fill_price"] == 5236.50


def test_envelope_roundtrip_heartbeat():
    payload = HeartbeatPayload(timestamp="2026-05-15T22:00:00+00:00", from_party="nt")
    envelope = build_envelope(
        msg_type=MsgType.HEARTBEAT, sequence=999, payload=payload, secret=SECRET
    )
    wire = encode_envelope(envelope)
    decoded = decode_envelope(wire.rstrip(b"\n"), secret=SECRET)
    assert decoded.msg_type == "heartbeat"
    assert decoded.payload["from_party"] == "nt"


def test_envelope_roundtrip_reconcile_request_and_response():
    req = ReconcileRequestPayload(timestamp="2026-05-15T22:00:00+00:00")
    req_env = build_envelope(
        msg_type=MsgType.RECONCILE_REQ, sequence=1, payload=req, secret=SECRET
    )
    decoded_req = decode_envelope(encode_envelope(req_env).rstrip(b"\n"), secret=SECRET)
    assert decoded_req.msg_type == "reconcile_req"

    resp = ReconcileResponsePayload(
        timestamp="2026-05-15T22:00:01+00:00",
        open_positions=[
            OpenPositionSnapshot(
                symbol="MESM26",
                nt_symbol="MES 06-26",
                quantity=1,
                side="BUY",
                avg_price=5236.25,
            ),
        ],
        pending_brackets=[
            PendingBracketSnapshot(
                intent_id="fxlon-GBPUSD-260515",
                atm_strategy_id="ATM-7",
                status="ACTIVE",
            ),
        ],
    )
    resp_env = build_envelope(
        msg_type=MsgType.RECONCILE_RESP, sequence=2, payload=resp, secret=SECRET
    )
    decoded_resp = decode_envelope(encode_envelope(resp_env).rstrip(b"\n"), secret=SECRET)
    assert decoded_resp.payload["open_positions"][0]["symbol"] == "MESM26"
    assert decoded_resp.payload["pending_brackets"][0]["status"] == "ACTIVE"


def test_envelope_roundtrip_ack():
    ack = AckPayload(ack_seq=42, timestamp="2026-05-15T22:00:00+00:00")
    env = build_envelope(msg_type=MsgType.ACK, sequence=100, payload=ack, secret=SECRET)
    decoded = decode_envelope(encode_envelope(env).rstrip(b"\n"), secret=SECRET)
    assert decoded.payload["ack_seq"] == 42


# ── Decode error handling ────────────────────────────────────────────────


def test_decode_rejects_hmac_mismatch():
    payload = HeartbeatPayload(timestamp="2026-05-15T22:00:00+00:00", from_party="python")
    env = build_envelope(
        msg_type=MsgType.HEARTBEAT, sequence=1, payload=payload, secret=SECRET
    )
    wire = encode_envelope(env)

    # Decode with a different secret — must reject
    with pytest.raises(ValueError, match="HMAC mismatch"):
        decode_envelope(wire.rstrip(b"\n"), secret=b"wrong-secret")


def test_decode_rejects_missing_keys():
    bad_wire = b'{"msg_type":"signal","sequence":1}'  # missing payload + signature
    with pytest.raises(ValueError, match="missing keys"):
        decode_envelope(bad_wire, secret=SECRET)


def test_decode_rejects_invalid_json():
    bad_wire = b"this is not json at all"
    with pytest.raises(Exception):  # json.JSONDecodeError
        decode_envelope(bad_wire, secret=SECRET)


def test_decode_rejects_tampered_payload_via_hmac():
    payload = SignalPayload(
        intent_id="abc",
        timestamp="2026-05-15T22:00:00+00:00",
        symbol="MESM26",
        nt_symbol="MES 06-26",
        side="BUY",
        quantity=1,
        atm_template="KATE_MES_ORB_BASE",
        stop_price=5234.50,
        target_price=5240.00,
        signal_close_price=5236.25,
    )
    env = build_envelope(msg_type=MsgType.SIGNAL, sequence=1, payload=payload, secret=SECRET)
    wire = encode_envelope(env)

    # Tamper with quantity inside the wire bytes (replace "quantity":1 with "quantity":99)
    tampered = wire.replace(b'"quantity":1', b'"quantity":99')
    assert tampered != wire  # confirm the replace landed

    with pytest.raises(ValueError, match="HMAC mismatch"):
        decode_envelope(tampered.rstrip(b"\n"), secret=SECRET)
