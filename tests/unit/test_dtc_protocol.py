"""
Unit tests for trading_bot.core.execution.dtc_protocol — pack/unpack
roundtrip for all inbound message types parsed by the engine.

Sources the wire layouts from COO Gemini's 2026-04-27 extraction of
Sierra DTCProtocol.h (header file from her Sierra install — Sierra DTC v8).
"""
from __future__ import annotations

import struct

import pytest

from trading_bot.core.execution import dtc_protocol as proto


# ── Pack helpers (test-only — these are NOT in the production module
#    because the bot only PARSES these messages, never sends them) ─────────
def _pack_order_update(
    *,
    client_order_id: str = "TEST_001",
    server_order_id: str = "SRV_001",
    symbol: str = "MESM26",
    exchange: str = "CME",
    order_status: int = proto.ORDER_STATUS_FILLED,
    order_update_reason: int = 0,
    order_type: int = proto.ORDER_TYPE_MARKET,
    side: int = proto.BUY,
    price1: float = 0.0,
    price2: float = 0.0,
    time_in_force: int = proto.TIME_IN_FORCE_DAY,
    good_till_dt: float = 0.0,
    order_qty: float = 1.0,
    filled_qty: float = 1.0,
    remaining_qty: float = 0.0,
    avg_fill_price: float = 5001.5,
    last_fill_price: float = 5001.5,
    last_fill_dt: int = 0,
    last_fill_qty: float = 1.0,
    trade_account: str = "E8933",
    info_text: str = "",
    free_form: str = "",
    open_close: int = 0,
) -> bytes:
    return struct.pack(
        proto.ORDER_UPDATE_FMT,
        proto.ORDER_UPDATE_SIZE, proto.ORDER_UPDATE,
        0, 0, 0,                                                 # RequestID, TotalNumMessages, MessageNumber
        symbol.encode(), exchange.encode(),
        b"", server_order_id.encode(), client_order_id.encode(), b"",
        order_status, order_update_reason, order_type, side,
        price1, price2,
        time_in_force,
        good_till_dt,
        order_qty, filled_qty, remaining_qty, avg_fill_price,
        last_fill_price,
        last_fill_dt,
        last_fill_qty,
        b"",                                                      # LastFillExecutionID
        trade_account.encode(),
        info_text.encode(),
        0,                                                        # NoOrders
        b"", b"",                                                 # ParentServerOrderID, OCO
        open_close,
        b"",                                                      # PreviousClientOrderID
        free_form.encode(),
        0.0, 0.0,                                                 # OrderReceivedDateTime, LatestTransactionDateTime
        b"",                                                      # Username
    )


def _pack_position_update(
    *,
    symbol: str = "MESM26",
    exchange: str = "CME",
    trade_account: str = "E8933",
    quantity: float = 1.0,
    avg_price: float = 5000.0,
    margin_req: float = 100.0,
    no_positions: int = 0,
    open_pnl: float = 0.0,
) -> bytes:
    return struct.pack(
        proto.POSITION_UPDATE_FMT,
        proto.POSITION_UPDATE_SIZE, proto.POSITION_UPDATE,
        0, 0, 0,                                              # RequestID, TotalNumberMessages, MessageNumber
        symbol.encode(), exchange.encode(),
        quantity, avg_price,
        b"", trade_account.encode(),                          # PositionIdentifier, TradeAccount
        no_positions, 0,                                      # NoPositions, Unsolicited
        margin_req,
        0,                                                    # EntryDateTime (int32)
        open_pnl, 0.0, 0.0, 0.0, 0.0,                         # OpenPnL, High, Low, QtyLimit, MaxPotential
    )


def _pack_account_balance_update(
    *,
    cash_balance: float = 1080.0,
    balance_available: float = 980.0,
    securities_value: float = 0.0,
    margin_req: float = 100.0,
    margin_full: float = 100.0,
    open_pnl: float = 0.0,
    daily_pnl: float = 0.0,
    currency: str = "USD",
    trade_account: str = "E8933",
    daily_loss_reached: int = 0,
    under_margin: int = 0,
    trading_disabled: int = 0,
    under_account_value: int = 0,
) -> bytes:
    return struct.pack(
        proto.ACCOUNT_BALANCE_UPDATE_FMT,
        proto.ACCOUNT_BALANCE_UPDATE_SIZE, proto.ACCOUNT_BALANCE_UPDATE,
        0,                                                     # RequestID
        cash_balance, balance_available,
        currency.encode(),
        trade_account.encode(),
        securities_value, margin_req,
        0, 0,                                                  # TotalNumberMessages, MessageNumber
        0, 0,                                                  # NoAccountBalances, Unsolicited
        open_pnl, daily_pnl,
        b"",                                                   # InfoText
        0,                                                     # TransactionIdentifier (uint64)
        0.0, 0.0,                                              # DailyNetLossLimit, TrailingAccountValueToLimitPositions
        daily_loss_reached, under_margin, 0, trading_disabled,
        b"",                                                   # Description
        under_account_value,
        0,                                                     # TransactionDateTime (int64)
        margin_full, 0.0, 0.0,                                 # MarginRequirementFull, FullPos, Peak
        b"",                                                   # IntroducingBroker
    )


# ── Sizes ────────────────────────────────────────────────────────────────
def test_order_update_size_matches_known_layout() -> None:
    # Documented total: header (4) + body fields summed per Gemini's
    # extraction. Aligned = 720 bytes.
    assert proto.ORDER_UPDATE_SIZE == 720


def test_position_update_size_matches_known_layout() -> None:
    assert proto.POSITION_UPDATE_SIZE == 240


def test_account_balance_update_size_matches_known_layout() -> None:
    assert proto.ACCOUNT_BALANCE_UPDATE_SIZE == 416


# ── Order update ──────────────────────────────────────────────────────────
def test_order_update_roundtrip_filled() -> None:
    payload = _pack_order_update(
        client_order_id="ORD-A",
        server_order_id="SRV-A",
        order_status=proto.ORDER_STATUS_FILLED,
        side=proto.BUY,
        filled_qty=2.0,
        remaining_qty=0.0,
        avg_fill_price=5001.25,
    )
    msg = proto.unpack_order_update(payload)
    assert msg.client_order_id == "ORD-A"
    assert msg.server_order_id == "SRV-A"
    assert msg.symbol == "MESM26"
    assert msg.exchange == "CME"
    assert msg.order_status == proto.ORDER_STATUS_FILLED
    assert msg.side == proto.BUY
    assert msg.filled_quantity == 2.0
    assert msg.average_fill_price == pytest.approx(5001.25)
    assert msg.trade_account == "E8933"
    assert msg.raw_size == proto.ORDER_UPDATE_SIZE


def test_order_update_carries_reject_reason_in_info_text() -> None:
    payload = _pack_order_update(
        order_status=proto.ORDER_STATUS_REJECTED,
        info_text="margin insufficient",
    )
    msg = proto.unpack_order_update(payload)
    assert msg.order_status == proto.ORDER_STATUS_REJECTED
    assert msg.info_text == "margin insufficient"


def test_order_update_tolerates_oversized_buffer() -> None:
    """Sierra may append fields in future versions; the parser must
    tolerate trailing bytes (same pattern as LOGON_RESPONSE)."""
    payload = _pack_order_update() + b"\x00" * 32
    msg = proto.unpack_order_update(payload)
    assert msg.client_order_id == "TEST_001"
    assert msg.raw_size == proto.ORDER_UPDATE_SIZE + 32


def test_order_update_rejects_short_buffer() -> None:
    with pytest.raises(ValueError, match="ORDER_UPDATE too short"):
        proto.unpack_order_update(b"\x00" * 100)


# ── Position update ───────────────────────────────────────────────────────
def test_position_update_long_position() -> None:
    payload = _pack_position_update(
        symbol="MESM26", exchange="CME",
        quantity=2.0, avg_price=5000.5,
        margin_req=200.0, open_pnl=15.0,
    )
    msg = proto.unpack_position_update(payload)
    assert msg.symbol == "MESM26"
    assert msg.quantity == 2.0
    assert msg.average_price == pytest.approx(5000.5)
    assert msg.margin_requirement == 200.0
    assert msg.open_profit_loss == 15.0
    assert msg.no_positions is False


def test_position_update_short_position_uses_negative_quantity() -> None:
    """Reconciler uses signed-quantity convention. Sierra's quantity field
    can be negative for short positions."""
    payload = _pack_position_update(quantity=-1.5)
    msg = proto.unpack_position_update(payload)
    assert msg.quantity == -1.5


def test_position_update_no_positions_flag() -> None:
    """Sierra sends a snapshot with no_positions=1 when account has no
    open positions — distinct from quantity=0."""
    payload = _pack_position_update(no_positions=1, quantity=0.0)
    msg = proto.unpack_position_update(payload)
    assert msg.no_positions is True
    assert msg.quantity == 0.0


def test_position_update_tolerates_oversized_buffer() -> None:
    payload = _pack_position_update() + b"\x00" * 16
    msg = proto.unpack_position_update(payload)
    assert msg.symbol == "MESM26"


# ── Account balance update ────────────────────────────────────────────────
def test_account_balance_update_basic() -> None:
    payload = _pack_account_balance_update(
        cash_balance=1080.0,
        balance_available=980.0,
        securities_value=0.0,
        margin_req=100.0,
    )
    msg = proto.unpack_account_balance_update(payload)
    assert msg.cash_balance == 1080.0
    assert msg.balance_available == 980.0
    assert msg.securities_value == 0.0
    assert msg.margin_requirement == 100.0
    assert msg.account_currency == "USD"
    assert msg.trade_account == "E8933"
    # NLV = cash + securities
    assert msg.net_liquidation_value == 1080.0


def test_account_balance_update_with_open_position_unrealized_profit() -> None:
    """For futures, NLV = cash_balance + open_positions_profit_loss.
    Unrealized profit on open positions pushes NLV above cash; an
    unrealized loss pushes it below."""
    profit = _pack_account_balance_update(
        cash_balance=1080.0, open_pnl=50.0, margin_req=100.0,
    )
    assert proto.unpack_account_balance_update(profit).net_liquidation_value == 1130.0

    loss = _pack_account_balance_update(
        cash_balance=1080.0, open_pnl=-30.0, margin_req=100.0,
    )
    assert proto.unpack_account_balance_update(loss).net_liquidation_value == 1050.0


def test_account_balance_update_securities_value_does_not_inflate_nlv() -> None:
    """Sierra Sim reports SecuritiesValue ≈ CashBalance as redundant
    bookkeeping. Our NLV formula must NOT add them — that would double-
    count the account equity. Verified against COO Gemini's 2026-04-27
    real-Sierra wire capture (cash=1080, securities=1080, expected NLV=1080)."""
    payload = _pack_account_balance_update(
        cash_balance=1080.0, securities_value=1080.0, open_pnl=0.0,
    )
    msg = proto.unpack_account_balance_update(payload)
    assert msg.net_liquidation_value == 1080.0    # NOT 2160
    assert msg.securities_value == 1080.0          # field still preserved


def test_account_balance_update_carries_circuit_breaker_flags() -> None:
    payload = _pack_account_balance_update(
        cash_balance=200.0, balance_available=0.0,
        margin_req=200.0,
        daily_loss_reached=1, under_margin=1, trading_disabled=1,
        under_account_value=1,
    )
    msg = proto.unpack_account_balance_update(payload)
    assert msg.daily_net_loss_limit_reached is True
    assert msg.is_under_required_margin is True
    assert msg.trading_is_disabled is True
    assert msg.is_under_required_account_value is True


def test_account_balance_update_tolerates_oversized_buffer() -> None:
    payload = _pack_account_balance_update() + b"\x00" * 24
    msg = proto.unpack_account_balance_update(payload)
    assert msg.cash_balance == 1080.0


# ── Status mapping ────────────────────────────────────────────────────────
def test_status_mapping_filled() -> None:
    assert proto.dtc_order_status_to_state_store(proto.ORDER_STATUS_FILLED) == "FILLED"


def test_status_mapping_rejected() -> None:
    assert proto.dtc_order_status_to_state_store(proto.ORDER_STATUS_REJECTED) == "REJECTED"


def test_status_mapping_cancelled() -> None:
    assert proto.dtc_order_status_to_state_store(proto.ORDER_STATUS_CANCELED) == "CANCELLED"


def test_status_mapping_open_is_working() -> None:
    assert proto.dtc_order_status_to_state_store(proto.ORDER_STATUS_OPEN) == "WORKING"


def test_status_mapping_partial_fill_is_working() -> None:
    assert proto.dtc_order_status_to_state_store(
        proto.ORDER_STATUS_PARTIALLY_FILLED
    ) == "WORKING"


def test_status_mapping_pending_variants_are_pending() -> None:
    for s in (
        proto.ORDER_STATUS_UNSPECIFIED,
        proto.ORDER_STATUS_ORDER_SENT,
        proto.ORDER_STATUS_PENDING_OPEN,
        proto.ORDER_STATUS_PENDING_CHILD,
    ):
        assert proto.dtc_order_status_to_state_store(s) == "PENDING"


def test_status_mapping_unknown_falls_back_to_pending() -> None:
    assert proto.dtc_order_status_to_state_store(999) == "PENDING"


# ── Integration: status string is valid for StateStore ───────────────────
def test_mapped_statuses_are_valid_state_store_statuses() -> None:
    """Every value the mapping produces must be a valid StateStore status.
    This catches drift between the two modules."""
    from trading_bot.core.state.state_store import VALID_ORDER_STATUSES
    for status_int in (
        proto.ORDER_STATUS_UNSPECIFIED,
        proto.ORDER_STATUS_ORDER_SENT,
        proto.ORDER_STATUS_PENDING_OPEN,
        proto.ORDER_STATUS_PENDING_CHILD,
        proto.ORDER_STATUS_OPEN,
        proto.ORDER_STATUS_PENDING_CANCEL_REPLACE,
        proto.ORDER_STATUS_PENDING_CANCEL,
        proto.ORDER_STATUS_FILLED,
        proto.ORDER_STATUS_CANCELED,
        proto.ORDER_STATUS_REJECTED,
        proto.ORDER_STATUS_PARTIALLY_FILLED,
    ):
        mapped = proto.dtc_order_status_to_state_store(status_int)
        assert mapped in VALID_ORDER_STATUSES, (status_int, mapped)
