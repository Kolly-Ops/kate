"""
Wire-format validation tests — parse REAL Sierra DTC v8 byte captures
through our unpackers, independent of the binary mock.

Source: COO Gemini's 2026-04-27 18:39 UK byte capture from live Sierra
Chart on the Contabo Windows VPS (build 56302), MESM26-CME sim mode.
See `omni/handoffs/2026-04-27-gemini-to-claude-wire-capture-complete.md`.

The hex is loaded from `tests/_fixtures/sierra_v8_wire_capture_2026-04-27.json`
(extracted verbatim from Gemini's handoff at fixture-build time).

Why this matters: Phase A integration tests pass against a binary mock
the engine and I both built from the same DTC header extract. The mock
is not an independent ground truth. These tests close that gap by
parsing actual on-wire bytes from real Sierra.
"""
from __future__ import annotations

import json
import pathlib
import struct

import pytest

from trading_bot.core.execution import dtc_protocol as proto


_FIXTURE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "_fixtures"
    / "sierra_v8_wire_capture_2026-04-27.json"
)


@pytest.fixture(scope="module")
def captures() -> dict[str, dict]:
    return json.loads(_FIXTURE_PATH.read_text())


def _capture_bytes(captures: dict[str, dict], name: str) -> bytes:
    return bytes.fromhex(captures[name]["hex"])


# ── Constants ─────────────────────────────────────────────────────────────
def test_message_type_constants_match_real_sierra(captures: dict) -> None:
    """Lock in the corrected v8 IDs. Gemini's 2026-04-27 capture proved
    POSITION_UPDATE = 306 and ACCOUNT_BALANCE_UPDATE = 600 (not 311/402
    as in earlier guesses from public docs)."""
    assert proto.ORDER_UPDATE == captures["ORDER_UPDATE"]["msg_type"] == 301
    assert proto.POSITION_UPDATE == captures["POSITION_UPDATE"]["msg_type"] == 306
    assert (
        proto.ACCOUNT_BALANCE_UPDATE
        == captures["ACCOUNT_BALANCE_UPDATE"]["msg_type"]
        == 600
    )


# ── Header sanity ─────────────────────────────────────────────────────────
def test_real_order_update_header(captures: dict) -> None:
    data = _capture_bytes(captures, "ORDER_UPDATE")
    size, msg_type = proto.unpack_header(data)
    assert size == 720
    assert msg_type == proto.ORDER_UPDATE
    # Wire is larger than my legacy struct (713) — oversized-tolerance path.
    assert size > proto.ORDER_UPDATE_SIZE


def test_real_position_update_header(captures: dict) -> None:
    data = _capture_bytes(captures, "POSITION_UPDATE")
    size, msg_type = proto.unpack_header(data)
    assert size == 240
    assert msg_type == proto.POSITION_UPDATE
    assert size > proto.POSITION_UPDATE_SIZE


def test_real_account_balance_update_header(captures: dict) -> None:
    data = _capture_bytes(captures, "ACCOUNT_BALANCE_UPDATE")
    size, msg_type = proto.unpack_header(data)
    assert size == 416
    assert msg_type == proto.ACCOUNT_BALANCE_UPDATE
    assert size > proto.ACCOUNT_BALANCE_UPDATE_SIZE


# ── Full unpack: ACCOUNT_BALANCE_UPDATE ───────────────────────────────────
def test_real_account_balance_update_unpacks_with_correct_fields(
    captures: dict,
) -> None:
    """Spot-check fields against Gemini's manual decode. The capture is
    from a $1,080 sim account with TradeAccount=E8933, USD currency."""
    data = _capture_bytes(captures, "ACCOUNT_BALANCE_UPDATE")
    msg = proto.unpack_account_balance_update(data)
    assert msg.cash_balance == pytest.approx(1080.0)
    assert msg.balance_available == pytest.approx(1080.0)
    assert msg.account_currency == "USD"
    assert msg.trade_account.startswith("E8933")
    assert msg.net_liquidation_value == pytest.approx(1080.0)
    # Healthy sim account — no circuit breakers tripped
    assert msg.daily_net_loss_limit_reached is False
    assert msg.is_under_required_margin is False
    assert msg.trading_is_disabled is False
    assert msg.raw_size == 416


# ── Full unpack: POSITION_UPDATE ──────────────────────────────────────────
def test_real_position_update_unpacks_no_positions_snapshot(
    captures: dict,
) -> None:
    """Sierra sends an empty snapshot (no_positions=1) when the account
    has no open positions. Gemini's capture was on a flat sim account."""
    data = _capture_bytes(captures, "POSITION_UPDATE")
    msg = proto.unpack_position_update(data)
    assert msg.symbol == ""
    assert msg.exchange == ""
    assert msg.quantity == 0.0
    assert msg.no_positions is True
    assert msg.raw_size == 240


# ── Full unpack: ORDER_UPDATE ─────────────────────────────────────────────
def test_real_order_update_unpacks_open_orders_request_response(
    captures: dict,
) -> None:
    """Gemini's capture was the response to OPEN_ORDERS_REQUEST on a
    flat sim account — Sierra returns a sentinel ORDER_UPDATE with no
    real order data."""
    data = _capture_bytes(captures, "ORDER_UPDATE")
    msg = proto.unpack_order_update(data)
    assert msg.symbol == ""
    assert msg.client_order_id == ""
    assert msg.raw_size == 720


# ── Direct byte-offset spot-checks (paranoia / drift catcher) ─────────────
def test_account_balance_cash_at_byte_offset_8(captures: dict) -> None:
    """CashBalance double sits at byte offset 8 (4-byte header +
    4-byte RequestID). Direct decoding bypasses the unpacker so a
    failure here distinguishes "format string off" from "field name off"."""
    data = _capture_bytes(captures, "ACCOUNT_BALANCE_UPDATE")
    cash = struct.unpack("<d", data[8:16])[0]
    assert cash == pytest.approx(1080.0)


def test_account_balance_currency_at_byte_offset_24(captures: dict) -> None:
    data = _capture_bytes(captures, "ACCOUNT_BALANCE_UPDATE")
    currency = data[24:32].split(b"\x00", 1)[0].decode("utf-8")
    assert currency == "USD"


def test_account_balance_trade_account_at_byte_offset_32(captures: dict) -> None:
    data = _capture_bytes(captures, "ACCOUNT_BALANCE_UPDATE")
    ta = data[32:64].split(b"\x00", 1)[0].decode("utf-8")
    assert ta == "E8933"
