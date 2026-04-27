"""
DTC Sim Order Test — Phase A validation
Connects to Sierra Chart DTC server at 127.0.0.1:11099
Sends a 1-contract MES BUY sim order, waits for fill, then flattens.

Usage:
    python dtc_sim_order_test.py

Requirements:
    - Sierra Chart running on same machine
    - Sierra Chart DTC server enabled (default port 11099)
    - Sierra Chart in Teton CME Routing (Sim mode ON for test)

Author: COO Gemini — 2026-04-26
"""

import socket
import struct
import time
import datetime

# ── DTC Message Types ──────────────────────────────────────────────────────
LOGON_REQUEST                     = 1
LOGON_RESPONSE                    = 2
HEARTBEAT                         = 3
SUBMIT_NEW_SINGLE_ORDER           = 208
ORDER_UPDATE                      = 301
OPEN_ORDERS_REQUEST               = 300
CANCEL_ORDER                      = 315

# ── Struct formats (little-endian) ─────────────────────────────────────────
HEADER_FMT       = "<HH"
HEADER_SIZE      = struct.calcsize(HEADER_FMT)

LOGON_REQ_FMT    = "<HH i 32s 32s 64s i i i i 32s"
LOGON_RESP_FMT   = "<HH i i 96s 64s i 60s B B"
HEARTBEAT_FMT    = "<HH I d"

# Submit New Single Order (simplified — key fields only)
# Size(2), Type(2), Symbol(64), Exchange(16), TradeAccount(32),
# ClientOrderID(32), OrderType(4), BuySell(4), Price1(8), Price2(8),
# Qty(8), TimeInForce(4), GoodTillDT(8), IsAutomated(1), IsParentOrder(1),
# FreeFormText(48), OpenClose(4)
ORDER_FMT = "<HH 64s 16s 32s 32s i i d d d i d B B 48s i"

# ── Constants ──────────────────────────────────────────────────────────────
DTC_HOST          = "127.0.0.1"
DTC_PORT          = 11099
CLIENT_NAME       = b"OMNI_SIM_TEST"
SYMBOL            = b"MESM26-CME"      # MES June 2026 front month
EXCHANGE          = b"CME"
TRADE_ACCOUNT     = b""               # Sierra will use default sim account
ORDER_ID          = b"OMNI_TEST_001"
BUY               = 1
SELL              = 2
MARKET_ORDER      = 1
DAY               = 1


def log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}")


def pack_logon():
    size = struct.calcsize(LOGON_REQ_FMT)
    return struct.pack(
        LOGON_REQ_FMT,
        size, LOGON_REQUEST,
        8,                  # ProtocolVersion
        b"",                # Username
        b"",                # Password
        b"OMNI sim test",   # GeneralText
        0, 0,
        10,                 # HeartbeatIntervalInSeconds
        1,                  # TradeMode (1 = sim)
        CLIENT_NAME,
    )


def pack_heartbeat():
    size = struct.calcsize(HEARTBEAT_FMT)
    return struct.pack(HEARTBEAT_FMT, size, HEARTBEAT, 0, 0.0)


def pack_order(side, qty=1):
    size = struct.calcsize(ORDER_FMT)
    order_id = ORDER_ID if side == BUY else b"OMNI_TEST_002"
    return struct.pack(
        ORDER_FMT,
        size, SUBMIT_NEW_SINGLE_ORDER,
        SYMBOL,
        EXCHANGE,
        TRADE_ACCOUNT,
        order_id,
        MARKET_ORDER,       # OrderType
        side,               # BuySell
        0.0,                # Price1 (market = 0)
        0.0,                # Price2
        float(qty),         # Quantity
        DAY,                # TimeInForce
        0.0,                # GoodTillDateTime
        1,                  # IsAutomatedOrder
        0,                  # IsParentOrder
        b"OMNI Phase A sim test",
        0,                  # OpenOrClose
    )


def recv_msg(sock):
    """Read one complete DTC message from socket."""
    header = b""
    while len(header) < HEADER_SIZE:
        chunk = sock.recv(HEADER_SIZE - len(header))
        if not chunk:
            return None, None
        header += chunk
    size, msg_type = struct.unpack(HEADER_FMT, header)
    body = b""
    remaining = size - HEADER_SIZE
    while len(body) < remaining:
        chunk = sock.recv(remaining - len(body))
        if not chunk:
            return msg_type, None
        body += chunk
    return msg_type, header + body


def main():
    log("=" * 60)
    log("OMNI DTC Sim Order Test — Phase A")
    log(f"Target: {DTC_HOST}:{DTC_PORT}")
    log(f"Symbol: {SYMBOL.decode().rstrip(chr(0))}")
    log("=" * 60)

    with socket.create_connection((DTC_HOST, DTC_PORT), timeout=10) as sock:
        sock.settimeout(15)

        # ── 1. Logon ───────────────────────────────────────────────────────
        log("Sending LOGON_REQUEST...")
        sock.sendall(pack_logon())

        msg_type, data = recv_msg(sock)
        if msg_type != LOGON_RESPONSE:
            log(f"ERROR: Expected LOGON_RESPONSE (2), got {msg_type}")
            return
        log(f"LOGON_RESPONSE received (type={msg_type}) ✅")

        # Try to unpack result text
        try:
            resp = struct.unpack(LOGON_RESP_FMT, data)
            result_code = resp[3]
            result_text = resp[4].decode("utf-8", errors="ignore").rstrip("\x00")
            server_name = resp[7].decode("utf-8", errors="ignore").rstrip("\x00")
            log(f"  Server: {server_name}")
            log(f"  Result code: {result_code}  Text: {result_text}")
        except Exception as e:
            log(f"  (Could not fully unpack logon response: {e})")

        # ── 2. Heartbeat ───────────────────────────────────────────────────
        log("Sending HEARTBEAT...")
        sock.sendall(pack_heartbeat())
        time.sleep(0.5)

        # ── 3. Submit BUY order ────────────────────────────────────────────
        log("Submitting SIM BUY order — 1 contract MES...")
        sock.sendall(pack_order(BUY, qty=1))

        # Wait for ORDER_UPDATE
        deadline = time.time() + 30
        buy_filled = False
        while time.time() < deadline:
            try:
                msg_type, data = recv_msg(sock)
            except socket.timeout:
                log("  Waiting for order update...")
                sock.sendall(pack_heartbeat())
                continue

            if msg_type is None:
                log("Connection closed by server.")
                break

            if msg_type == ORDER_UPDATE:
                log(f"ORDER_UPDATE received ✅ (len={len(data)})")
                buy_filled = True
                break
            elif msg_type == HEARTBEAT:
                log("  Heartbeat received, still waiting...")
            else:
                log(f"  Message type {msg_type} received (ignoring)")

        if not buy_filled:
            log("ERROR: Did not receive order fill within 30s. Check Sierra is in Teton/Sim mode.")
            return

        # ── 4. Wait 2 seconds then flatten ────────────────────────────────
        log("Waiting 2 seconds before flattening...")
        time.sleep(2)

        log("Submitting SIM SELL to flatten position...")
        sock.sendall(pack_order(SELL, qty=1))

        deadline = time.time() + 30
        sell_filled = False
        while time.time() < deadline:
            try:
                msg_type, data = recv_msg(sock)
            except socket.timeout:
                log("  Waiting for flatten confirmation...")
                sock.sendall(pack_heartbeat())
                continue

            if msg_type is None:
                break
            if msg_type == ORDER_UPDATE:
                log(f"FLATTEN ORDER_UPDATE received ✅")
                sell_filled = True
                break
            elif msg_type == HEARTBEAT:
                log("  Heartbeat received...")

        # ── 5. Result ──────────────────────────────────────────────────────
        log("=" * 60)
        if buy_filled and sell_filled:
            log("✅ SIM ORDER TEST PASSED — BUY + FLATTEN both filled")
            log("   DTC path is confirmed working. Phase A is GO.")
        elif buy_filled:
            log("⚠️  BUY filled but FLATTEN did not confirm within timeout.")
            log("   Manually flatten in Sierra Chart. DTC path is partial.")
        else:
            log("❌ SIM ORDER TEST FAILED — No order fills received.")
            log("   Check: Sierra Chart in Teton CME Routing + Sim mode ON")
        log("=" * 60)


if __name__ == "__main__":
    main()
