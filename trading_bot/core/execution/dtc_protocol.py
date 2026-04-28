"""
DTC binary protocol primitives — pack/unpack for Sierra Chart DTC server.

Reference: DTCProtocol.h (Sierra Chart binary mode v8+).
Confirmed working over the wire 2026-04-27 00:13 UK on MESM26-CME — see
tests/integration/dtc_sim_order_test.py for the canonical handshake test.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass


# ── Message types ─────────────────────────────────────────────────────────
LOGON_REQUEST                    = 1
LOGON_RESPONSE                   = 2
HEARTBEAT                        = 3
LOGOFF                           = 5
ENCODING_REQUEST                 = 6
ENCODING_RESPONSE                = 7

MARKET_DATA_REQUEST              = 101
MARKET_DATA_REJECT               = 103
MARKET_DATA_SNAPSHOT             = 104
MARKET_DATA_UPDATE_TRADE         = 107
MARKET_DATA_UPDATE_BID_ASK       = 108

CANCEL_ORDER                     = 203
CANCEL_REPLACE_ORDER             = 204
SUBMIT_NEW_SINGLE_ORDER          = 208
OPEN_ORDERS_REQUEST              = 300
ORDER_UPDATE                     = 301
# Sierra DTC v8 msg IDs corrected per COO Gemini's 2026-04-27 wire captures:
# - 16:55 UK capture: POSITION_UPDATE = 306 (was 311), ACCOUNT_BALANCE_UPDATE = 600 (was 402)
# - 20:20 UK capture: ACCOUNT_BALANCE_REQUEST = 601 (was 400),
#   CURRENT_POSITIONS_REQUEST = 305 (was 310). The first set fixed inbound
#   parsing; the second set fixed outbound seed requests — without them
#   Sierra silently dropped my balance + positions requests on the first
#   real-Sierra connect (logs showed only ORDER_UPDATE coming back).
# See tests/unit/test_dtc_protocol_wire.py — parses Gemini's actual Sierra
# hex through these constants + the unpackers below.
CURRENT_POSITIONS_REQUEST        = 305
POSITION_UPDATE                  = 306

ACCOUNT_BALANCE_REQUEST          = 601
ACCOUNT_BALANCE_UPDATE           = 600

# ── Trade mode (LogonRequest field) ───────────────────────────────────────
TRADE_MODE_DEMO       = 1   # confirmed working in sim mode 2026-04-27
TRADE_MODE_SIMULATED  = 2
TRADE_MODE_LIVE       = 3

# ── Logon result codes ────────────────────────────────────────────────────
LOGON_SUCCESS              = 1
LOGON_ERROR                = 2
LOGON_ERROR_NO_RECONNECT   = 3
LOGON_RECONNECT_NEW_ADDR   = 4

# ── Order side / type / TIF ───────────────────────────────────────────────
BUY    = 1
SELL   = 2

ORDER_TYPE_MARKET       = 1
ORDER_TYPE_LIMIT        = 2
ORDER_TYPE_STOP         = 3
ORDER_TYPE_STOP_LIMIT   = 4

TIME_IN_FORCE_DAY       = 1
TIME_IN_FORCE_GTC       = 2
TIME_IN_FORCE_IOC       = 3
TIME_IN_FORCE_FOK       = 4

REQUEST_ACTION_SUBSCRIBE   = 1
REQUEST_ACTION_UNSUBSCRIBE = 2
REQUEST_ACTION_SNAPSHOT    = 3


# ── Struct formats (little-endian) ─────────────────────────────────────────
HEADER_FMT  = "<HH"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# LogonRequest — fixed 184 bytes:
#   Size, Type, ProtocolVersion, Username[32], Password[32], GeneralText[64],
#   Integer_1, Integer_2, HeartbeatIntervalInSeconds, TradeMode, ClientName[32]
LOGON_REQUEST_FMT  = "<HH i 32s 32s 64s i i i i 32s"
LOGON_REQUEST_SIZE = struct.calcsize(LOGON_REQUEST_FMT)

# LogonResponse — legacy v7 layout is 238 bytes. Newer Sierra builds send
# LARGER responses with additional capability flags appended. We unpack only
# the stable prefix and ignore trailing bytes — see unpack_logon_response().
LOGON_RESPONSE_LEGACY_FMT  = "<HH i i 96s 64s i 60s B B"
LOGON_RESPONSE_LEGACY_SIZE = struct.calcsize(LOGON_RESPONSE_LEGACY_FMT)

HEARTBEAT_FMT = "<HH I d"

MD_REQUEST_FMT  = "<HH i I 64s 16s"
MD_SNAPSHOT_FMT = "<HH I d d d d d I I d d d d d d d"
MD_TRADE_FMT    = "<HH I H d d d"

# SubmitNewSingleOrder — Size, Type, Symbol[64], Exchange[16], TradeAccount[32],
#   ClientOrderID[32], OrderType, BuySell, Price1, Price2, Quantity, TIF,
#   GoodTillDateTime, IsAutomated, IsParent, FreeFormText[48], OpenClose
ORDER_FMT  = "<HH 64s 16s 32s 32s i i d d d i d B B 48s i"
ORDER_SIZE = struct.calcsize(ORDER_FMT)


# ── Pack helpers ───────────────────────────────────────────────────────────
def pack_logon_request(
    client_name: str,
    *,
    trade_mode: int = TRADE_MODE_DEMO,
    heartbeat_interval: int = 10,
    general_text: str = "",
    username: str = "",
    password: str = "",
    protocol_version: int = 8,
) -> bytes:
    return struct.pack(
        LOGON_REQUEST_FMT,
        LOGON_REQUEST_SIZE, LOGON_REQUEST,
        protocol_version,
        username.encode("utf-8"),
        password.encode("utf-8"),
        general_text.encode("utf-8"),
        0, 0,
        heartbeat_interval,
        trade_mode,
        client_name.encode("utf-8"),
    )


def pack_heartbeat(num_dropped: int = 0, timestamp: float = 0.0) -> bytes:
    size = struct.calcsize(HEARTBEAT_FMT)
    return struct.pack(HEARTBEAT_FMT, size, HEARTBEAT, num_dropped, timestamp)


def pack_market_data_request(
    symbol_id: int,
    symbol: str,
    *,
    exchange: str = "",
    action: int = REQUEST_ACTION_SUBSCRIBE,
) -> bytes:
    size = struct.calcsize(MD_REQUEST_FMT)
    return struct.pack(
        MD_REQUEST_FMT,
        size, MARKET_DATA_REQUEST,
        action, symbol_id,
        symbol.encode("utf-8"),
        exchange.encode("utf-8"),
    )


def pack_open_orders_request(
    *,
    request_id: int = 0,
    request_all_orders: int = 0,
    server_order_id: str = "",
    trade_account: str = "",
) -> bytes:
    """OPEN_ORDERS_REQUEST (msg 300). Sierra DTC v8 layout per COO Gemini's
    2026-04-27 20:20 wire capture — 76 bytes total, with a `RequestAllOrders`
    int32 between RequestID and ServerOrderID. Earlier 72-byte version
    omitted RequestAllOrders; Sierra was tolerant enough to still respond,
    which is how we initially missed the bug."""
    fmt = "<HH i i 32s 32s"
    size = struct.calcsize(fmt)
    return struct.pack(
        fmt, size, OPEN_ORDERS_REQUEST,
        request_id, request_all_orders,
        server_order_id.encode("utf-8"), trade_account.encode("utf-8"),
    )


def pack_current_positions_request(
    *, request_id: int = 0, trade_account: str = ""
) -> bytes:
    """CURRENT_POSITIONS_REQUEST (msg 305). Empty `trade_account` requests
    all accounts."""
    fmt = "<HH i 32s"
    size = struct.calcsize(fmt)
    return struct.pack(
        fmt, size, CURRENT_POSITIONS_REQUEST, request_id,
        trade_account.encode("utf-8"),
    )


def pack_account_balance_request(
    *, request_id: int = 0, trade_account: str = ""
) -> bytes:
    """ACCOUNT_BALANCE_REQUEST (msg 601). Empty `trade_account` requests
    all accounts."""
    fmt = "<HH i 32s"
    size = struct.calcsize(fmt)
    return struct.pack(
        fmt, size, ACCOUNT_BALANCE_REQUEST, request_id,
        trade_account.encode("utf-8"),
    )


def pack_submit_order(
    *,
    symbol: str,
    exchange: str,
    trade_account: str,
    client_order_id: str,
    order_type: int,
    side: int,
    quantity: float,
    price1: float = 0.0,
    price2: float = 0.0,
    time_in_force: int = TIME_IN_FORCE_DAY,
    good_till_dt: float = 0.0,
    is_automated: bool = True,
    is_parent: bool = False,
    free_form_text: str = "",
    open_close: int = 0,
) -> bytes:
    return struct.pack(
        ORDER_FMT,
        ORDER_SIZE, SUBMIT_NEW_SINGLE_ORDER,
        symbol.encode("utf-8"),
        exchange.encode("utf-8"),
        trade_account.encode("utf-8"),
        client_order_id.encode("utf-8"),
        order_type,
        side,
        price1, price2,
        float(quantity),
        time_in_force,
        good_till_dt,
        1 if is_automated else 0,
        1 if is_parent else 0,
        free_form_text.encode("utf-8"),
        open_close,
    )


# ── Unpack helpers ─────────────────────────────────────────────────────────
def unpack_header(data: bytes) -> tuple[int | None, int | None]:
    if len(data) < HEADER_SIZE:
        return None, None
    size, msg_type = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
    return size, msg_type


def _decode_cstr(buf: bytes) -> str:
    return buf.split(b"\x00", 1)[0].decode("utf-8", errors="replace")


@dataclass(frozen=True)
class LogonResponse:
    protocol_version: int
    result_code: int
    result_text: str
    reconnect_address: str
    server_name: str
    market_data_supported: bool
    trading_supported: bool
    raw_size: int


def unpack_logon_response(data: bytes) -> LogonResponse:
    """
    Parse a LOGON_RESPONSE payload, tolerating Sierra's larger-than-legacy
    response sizes. Sierra v8+ appends additional capability flags after the
    legacy 238-byte struct — fixed-size struct.unpack on the full buffer
    raised "unpack requires a buffer of 238 bytes" in COO Gemini's
    2026-04-27 sim test. We unpack only the stable prefix and ignore trailing
    bytes.
    """
    raw_size = len(data)
    if raw_size < LOGON_RESPONSE_LEGACY_SIZE:
        raise ValueError(
            f"LOGON_RESPONSE too short: {raw_size}b "
            f"(need at least {LOGON_RESPONSE_LEGACY_SIZE}b)"
        )
    fields = struct.unpack(
        LOGON_RESPONSE_LEGACY_FMT, data[:LOGON_RESPONSE_LEGACY_SIZE]
    )
    (
        _size, _type, proto_ver, result_code,
        result_text_b, reconnect_addr_b,
        _int1, server_name_b,
        md_support, trading_support,
    ) = fields
    return LogonResponse(
        protocol_version=proto_ver,
        result_code=result_code,
        result_text=_decode_cstr(result_text_b),
        reconnect_address=_decode_cstr(reconnect_addr_b),
        server_name=_decode_cstr(server_name_b),
        market_data_supported=bool(md_support),
        trading_supported=bool(trading_support),
        raw_size=raw_size,
    )


@dataclass(frozen=True)
class HeartbeatMessage:
    num_dropped: int
    timestamp: float


def unpack_heartbeat(data: bytes) -> HeartbeatMessage:
    size = struct.calcsize(HEARTBEAT_FMT)
    if len(data) < size:
        raise ValueError(f"HEARTBEAT too short: {len(data)}b (need {size}b)")
    _size, _type, num_dropped, timestamp = struct.unpack(HEARTBEAT_FMT, data[:size])
    return HeartbeatMessage(num_dropped=num_dropped, timestamp=timestamp)


# ── Sierra OrderStatusEnum (msg 301 OrderStatus field) ────────────────────
# Per Sierra DTC v8 OrderStatusEnum. Engine maps these to StateStore's
# string statuses (PENDING / WORKING / FILLED / CANCELLED / REJECTED).
ORDER_STATUS_UNSPECIFIED              = 0
ORDER_STATUS_ORDER_SENT               = 1
ORDER_STATUS_PENDING_OPEN             = 2
ORDER_STATUS_PENDING_CHILD            = 3
ORDER_STATUS_OPEN                     = 4
ORDER_STATUS_PENDING_CANCEL_REPLACE   = 5
ORDER_STATUS_PENDING_CANCEL           = 6
ORDER_STATUS_FILLED                   = 7
ORDER_STATUS_CANCELED                 = 8
ORDER_STATUS_REJECTED                 = 9
ORDER_STATUS_PARTIALLY_FILLED         = 10


# ── Inbound message struct formats (Sierra DTC v8) ────────────────────────
# Source: COO Gemini's extraction from Sierra's DTCProtocol.h, 2026-04-27.
# All formats are byte-packed (`<` little-endian, no alignment) — matches
# the wire format Sierra accepted for outbound LOGON_REQUEST and
# SubmitNewSingleOrder. As with LOGON_RESPONSE, we unpack only the stable
# prefix and ignore trailing bytes; this insulates us against Sierra
# appending capability flags in future versions.
ORDER_UPDATE_FMT = (
    "<HH"            # Size, Type
    "iii"            # RequestID, TotalNumMessages, MessageNumber
    "64s 16s"        # Symbol, Exchange
    "32s 32s 32s 32s"# PreviousServerOrderID, ServerOrderID, ClientOrderID, ExchangeOrderID
    "iiii"           # OrderStatus, OrderUpdateReason, OrderType, BuySell
    "dd"             # Price1, Price2
    "i"              # TimeInForce
    "d"              # GoodTillDateTime
    "dddd"           # OrderQuantity, FilledQuantity, RemainingQuantity, AverageFillPrice
    "d"              # LastFillPrice
    "q"              # LastFillDateTime (t_DateTimeWithMillisecondsInt — int64)
    "d"              # LastFillQuantity
    "64s"            # LastFillExecutionID
    "32s"            # TradeAccount
    "96s"            # InfoText
    "B"              # NoOrders
    "32s 32s"        # ParentServerOrderID, OCOLinkedOrderServerOrderID
    "i"              # OpenOrClose
    "32s"            # PreviousClientOrderID
    "48s"            # FreeFormText
    "d d"            # OrderReceivedDateTime, LatestTransactionDateTime
    "32s"            # Username
)
ORDER_UPDATE_SIZE = struct.calcsize(ORDER_UPDATE_FMT)

POSITION_UPDATE_FMT = (
    "<HH"            # Size, Type
    "iii"            # RequestID, TotalNumberMessages, MessageNumber
    "64s 16s"        # Symbol, Exchange
    "dd"             # Quantity (signed), AveragePrice
    "32s 32s"        # PositionIdentifier, TradeAccount
    "BB"             # NoPositions, Unsolicited
    "d"              # MarginRequirement
    "i"              # EntryDateTime (t_DateTime4Byte — int32)
    "ddddd"          # OpenProfitLoss, HighPriceDuringPosition, LowPriceDuringPosition,
                     # QuantityLimit, MaxPotentialPostionQuantity
)
POSITION_UPDATE_SIZE = struct.calcsize(POSITION_UPDATE_FMT)

ACCOUNT_BALANCE_UPDATE_FMT = (
    "<HH"            # Size, Type
    "i"              # RequestID
    "dd"             # CashBalance, BalanceAvailableForNewPositions
    "8s"             # AccountCurrency
    "32s"            # TradeAccount
    "dd"             # SecuritiesValue, MarginRequirement
    "ii"             # TotalNumberMessages, MessageNumber
    "BB"             # NoAccountBalances, Unsolicited
    "dd"             # OpenPositionsProfitLoss, DailyProfitLoss
    "96s"            # InfoText
    "Q"              # TransactionIdentifier (uint64)
    "dd"             # DailyNetLossLimit, TrailingAccountValueToLimitPositions
    "BBBB"           # DailyNetLossLimitReached, IsUnderRequiredMargin,
                     # ClosePositionsAtEndOfDay, TradingIsDisabled
    "96s"            # Description
    "B"              # IsUnderRequiredAccountValue
    "q"              # TransactionDateTime (t_DateTimeWithMicrosecondsInt — int64)
    "ddd"            # MarginRequirementFull, MarginRequirementFullPositionsOnly,
                     # PeakMarginRequirement
    "32s"            # IntroducingBroker
)
ACCOUNT_BALANCE_UPDATE_SIZE = struct.calcsize(ACCOUNT_BALANCE_UPDATE_FMT)


# ── Inbound message dataclasses ───────────────────────────────────────────
@dataclass(frozen=True)
class OrderUpdate:
    """Subset of s_OrderUpdate (msg 301) the engine cares about.

    Engine maps `order_status` → StateStore status string via
    `dtc_order_status_to_state_store()`. `info_text` becomes the
    `rejected_reason` when status indicates rejection.

    `no_orders` is True when Sierra responds to OPEN_ORDERS_REQUEST on a
    flat account — it's a sentinel "no orders to report" message, NOT a
    real order update. Engine must skip these rather than register them
    in broker_orders (otherwise they show up as false reconciliation drift
    against the empty local state).
    """
    client_order_id: str
    server_order_id: str
    symbol: str
    exchange: str
    order_status: int        # raw enum; use dtc_order_status_to_state_store
    order_update_reason: int
    side: int                # BUY / SELL
    filled_quantity: float
    remaining_quantity: float
    average_fill_price: float
    last_fill_price: float
    last_fill_quantity: float
    trade_account: str
    info_text: str
    no_orders: bool
    raw_size: int


@dataclass(frozen=True)
class PositionUpdate:
    """Subset of s_PositionUpdate (msg 311) the engine cares about.

    `quantity` is signed: positive = long, negative = short — matches the
    Reconciler's RemotePosition convention.
    """
    symbol: str
    exchange: str
    trade_account: str
    quantity: float
    average_price: float
    margin_requirement: float
    open_profit_loss: float
    no_positions: bool       # True when broker reports "no positions" snapshot
    raw_size: int


@dataclass(frozen=True)
class AccountBalanceUpdate:
    """Subset of s_AccountBalanceUpdate (msg 402) the engine cares about.

    NLV computed as cash + securities. `margin_requirement` feeds the risk
    engine's open_positions_margin field. Risk-management flag bytes
    (daily_net_loss_limit_reached, trading_is_disabled, etc.) surface
    broker-side circuit breakers.
    """
    cash_balance: float
    balance_available: float
    securities_value: float
    margin_requirement: float
    margin_requirement_full: float
    open_positions_profit_loss: float
    daily_profit_loss: float
    account_currency: str
    trade_account: str
    daily_net_loss_limit_reached: bool
    is_under_required_margin: bool
    trading_is_disabled: bool
    is_under_required_account_value: bool
    raw_size: int

    @property
    def net_liquidation_value(self) -> float:
        """Mark-to-market account value for FUTURES accounts:
            NLV = CashBalance + OpenPositionsProfitLoss

        Sierra's `SecuritiesValue` field reports total account equity for
        sim accounts (i.e. ≈ CashBalance) and is NOT additive — using
        cash + securities would double-count. For futures, MTM equity is
        cash + unrealized P&L of open positions; that's the figure the
        risk engine compares against the NLV floor + drawdown thresholds.
        """
        return self.cash_balance + self.open_positions_profit_loss


# ── Unpack helpers ────────────────────────────────────────────────────────
def unpack_order_update(data: bytes) -> OrderUpdate:
    """Parse ORDER_UPDATE (msg 301). Tolerates oversized buffers (Sierra
    may append fields in future versions); reads only the documented
    prefix."""
    raw_size = len(data)
    if raw_size < ORDER_UPDATE_SIZE:
        raise ValueError(
            f"ORDER_UPDATE too short: {raw_size}b "
            f"(need at least {ORDER_UPDATE_SIZE}b)"
        )
    fields = struct.unpack(ORDER_UPDATE_FMT, data[:ORDER_UPDATE_SIZE])
    (
        _size, _type, _request_id, _total_msgs, _msg_num,
        symbol_b, exchange_b,
        _prev_server_oid_b, server_oid_b, client_oid_b, _exch_oid_b,
        order_status, order_update_reason, _order_type, side,
        _price1, _price2,
        _tif,
        _good_till_dt,
        _order_qty, filled_qty, remaining_qty, avg_fill_price,
        last_fill_price,
        _last_fill_dt,
        last_fill_qty,
        _last_fill_exec_id_b,
        trade_account_b,
        info_text_b,
        no_orders,
        _parent_oid_b, _oco_oid_b,
        _open_close,
        _prev_client_oid_b,
        _free_form_b,
        _order_received_dt, _latest_txn_dt,
        _username_b,
    ) = fields
    client_order_id = _decode_cstr(client_oid_b)
    server_order_id = _decode_cstr(server_oid_b)
    symbol = _decode_cstr(symbol_b)
    # Sierra v8 build 56302 (observed 2026-04-28 06:51 UK) sends the
    # OPEN_ORDERS_REQUEST sentinel response with the explicit NoOrders
    # byte CLEARED — so we can't trust that field alone. Treat empty
    # client_order_id + server_order_id + symbol + status=0 as the
    # sentinel even when the byte is 0. Keep the explicit-byte check
    # too in case a future Sierra build does set it.
    is_no_orders_sentinel = bool(no_orders) or (
        client_order_id == ""
        and server_order_id == ""
        and symbol == ""
        and order_status == 0
    )
    return OrderUpdate(
        client_order_id=client_order_id,
        server_order_id=server_order_id,
        symbol=symbol,
        exchange=_decode_cstr(exchange_b),
        order_status=order_status,
        order_update_reason=order_update_reason,
        side=side,
        filled_quantity=filled_qty,
        remaining_quantity=remaining_qty,
        average_fill_price=avg_fill_price,
        last_fill_price=last_fill_price,
        last_fill_quantity=last_fill_qty,
        trade_account=_decode_cstr(trade_account_b),
        info_text=_decode_cstr(info_text_b),
        no_orders=is_no_orders_sentinel,
        raw_size=raw_size,
    )


def unpack_position_update(data: bytes) -> PositionUpdate:
    """Parse POSITION_UPDATE (msg 311)."""
    raw_size = len(data)
    if raw_size < POSITION_UPDATE_SIZE:
        raise ValueError(
            f"POSITION_UPDATE too short: {raw_size}b "
            f"(need at least {POSITION_UPDATE_SIZE}b)"
        )
    fields = struct.unpack(POSITION_UPDATE_FMT, data[:POSITION_UPDATE_SIZE])
    (
        _size, _type, _request_id, _total_msgs, _msg_num,
        symbol_b, exchange_b,
        quantity, avg_price,
        _position_id_b, trade_account_b,
        no_positions, _unsolicited,
        margin_req,
        _entry_dt,
        open_pnl, _high_price, _low_price,
        _qty_limit, _max_potential_qty,
    ) = fields
    return PositionUpdate(
        symbol=_decode_cstr(symbol_b),
        exchange=_decode_cstr(exchange_b),
        trade_account=_decode_cstr(trade_account_b),
        quantity=quantity,
        average_price=avg_price,
        margin_requirement=margin_req,
        open_profit_loss=open_pnl,
        no_positions=bool(no_positions),
        raw_size=raw_size,
    )


def unpack_account_balance_update(data: bytes) -> AccountBalanceUpdate:
    """Parse ACCOUNT_BALANCE_UPDATE (msg 402)."""
    raw_size = len(data)
    if raw_size < ACCOUNT_BALANCE_UPDATE_SIZE:
        raise ValueError(
            f"ACCOUNT_BALANCE_UPDATE too short: {raw_size}b "
            f"(need at least {ACCOUNT_BALANCE_UPDATE_SIZE}b)"
        )
    fields = struct.unpack(
        ACCOUNT_BALANCE_UPDATE_FMT,
        data[:ACCOUNT_BALANCE_UPDATE_SIZE],
    )
    (
        _size, _type, _request_id,
        cash_balance, balance_available,
        currency_b,
        trade_account_b,
        securities_value, margin_req,
        _total_msgs, _msg_num,
        _no_balances, _unsolicited,
        open_pnl, daily_pnl,
        _info_text_b,
        _txn_id,
        _daily_loss_limit, _trailing_value_limit,
        daily_loss_reached, under_margin, _close_at_eod, trading_disabled,
        _description_b,
        under_account_value,
        _txn_dt,
        margin_full, _margin_full_pos, _peak_margin,
        _introducing_broker_b,
    ) = fields
    return AccountBalanceUpdate(
        cash_balance=cash_balance,
        balance_available=balance_available,
        securities_value=securities_value,
        margin_requirement=margin_req,
        margin_requirement_full=margin_full,
        open_positions_profit_loss=open_pnl,
        daily_profit_loss=daily_pnl,
        account_currency=_decode_cstr(currency_b),
        trade_account=_decode_cstr(trade_account_b),
        daily_net_loss_limit_reached=bool(daily_loss_reached),
        is_under_required_margin=bool(under_margin),
        trading_is_disabled=bool(trading_disabled),
        is_under_required_account_value=bool(under_account_value),
        raw_size=raw_size,
    )


# ── Status mapping ────────────────────────────────────────────────────────
# Maps Sierra OrderStatusEnum → StateStore status string. Used by the
# engine when it receives an ORDER_UPDATE and needs to call
# StateStore.update_order_status.
_DTC_TO_STATE_STORE_STATUS: dict[int, str] = {
    ORDER_STATUS_UNSPECIFIED:            "PENDING",
    ORDER_STATUS_ORDER_SENT:             "PENDING",
    ORDER_STATUS_PENDING_OPEN:           "PENDING",
    ORDER_STATUS_PENDING_CHILD:          "PENDING",
    ORDER_STATUS_OPEN:                   "WORKING",
    ORDER_STATUS_PENDING_CANCEL_REPLACE: "WORKING",
    ORDER_STATUS_PENDING_CANCEL:         "WORKING",
    ORDER_STATUS_PARTIALLY_FILLED:       "WORKING",
    ORDER_STATUS_FILLED:                 "FILLED",
    ORDER_STATUS_CANCELED:               "CANCELLED",
    ORDER_STATUS_REJECTED:               "REJECTED",
}


def dtc_order_status_to_state_store(status: int) -> str:
    """Translate Sierra OrderStatusEnum → StateStore status string.

    Unknown values map to PENDING (conservative — keeps the order in the
    active set so the reconciler will pick up any drift)."""
    return _DTC_TO_STATE_STORE_STATUS.get(status, "PENDING")
