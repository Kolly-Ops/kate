"""
StateStore — SQLite-backed local state of record for the trading bot.

Tracks open positions, order lifecycle, the kill switch, and a rolling
account-NLV snapshot history (for drawdown calc + audit). This is the
LOCAL source of truth; the broker (EdgeClear via Sierra DTC) is the
remote source of truth, and the Reconciler compares the two.

Design notes:
- Sync SQLite. Operations are sub-ms; async wrappers add a dep without
  measurable benefit at Phase A volumes (~10 writes/sec peak).
- Single-writer assumption: the supervisor coordinates one writer.
- All timestamps stored as ISO-8601 UTC strings.
"""
from __future__ import annotations

import datetime as dt
import pathlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional


# ── Constants ──────────────────────────────────────────────────────────────
ORDER_STATUS_PENDING   = "PENDING"
ORDER_STATUS_WORKING   = "WORKING"
ORDER_STATUS_FILLED    = "FILLED"
ORDER_STATUS_CANCELLED = "CANCELLED"
ORDER_STATUS_REJECTED  = "REJECTED"

VALID_ORDER_STATUSES = frozenset({
    ORDER_STATUS_PENDING,
    ORDER_STATUS_WORKING,
    ORDER_STATUS_FILLED,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_REJECTED,
})

KILL_SWITCH_ACTIVE  = "ACTIVE"
KILL_SWITCH_TRIPPED = "TRIPPED"


# ── Records ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Position:
    symbol: str
    exchange: str
    side: int
    quantity: float
    avg_price: float
    opened_at: str
    updated_at: str


@dataclass(frozen=True)
class Order:
    client_order_id: str
    symbol: str
    exchange: str
    side: int
    quantity: float
    order_type: int
    status: str
    submitted_at: str
    updated_at: str
    fill_price: Optional[float] = None
    fill_quantity: Optional[float] = None
    filled_at: Optional[str] = None
    exit_price: Optional[float] = None
    exit_quantity: Optional[float] = None
    exited_at: Optional[str] = None
    exit_reason: Optional[str] = None
    realized_pnl: Optional[float] = None
    rejected_reason: Optional[str] = None


@dataclass(frozen=True)
class KillSwitch:
    state: str
    reason: Optional[str]
    since: str
    updated_at: str

    @property
    def is_tripped(self) -> bool:
        return self.state == KILL_SWITCH_TRIPPED


@dataclass(frozen=True)
class AccountSnapshot:
    nlv: float
    drawdown_pct: float
    recorded_at: str


# ── Schema ─────────────────────────────────────────────────────────────────
SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS positions (
    symbol      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    side        INTEGER NOT NULL,
    quantity    REAL NOT NULL,
    avg_price   REAL NOT NULL,
    opened_at   TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (symbol, exchange)
);

CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    symbol          TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    side            INTEGER NOT NULL,
    quantity        REAL NOT NULL,
    order_type      INTEGER NOT NULL,
    status          TEXT NOT NULL,
    submitted_at    TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    fill_price      REAL,
    fill_quantity   REAL,
    filled_at       TEXT,
    exit_price      REAL,
    exit_quantity   REAL,
    exited_at       TEXT,
    exit_reason     TEXT,
    realized_pnl    REAL,
    rejected_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

CREATE TABLE IF NOT EXISTS kill_switch (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    state       TEXT NOT NULL,
    reason      TEXT,
    since       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    nlv           REAL NOT NULL,
    drawdown_pct  REAL NOT NULL,
    recorded_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_time
    ON account_snapshots(recorded_at);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


# ── StateStore ─────────────────────────────────────────────────────────────
class StateStore:
    def __init__(self, db_path: str | pathlib.Path) -> None:
        self.db_path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────
    def open(self) -> "StateStore":
        if self._conn is None:
            # Add timeout and check_same_thread=False to handle multi-process
            # (engine + health-check script) access gracefully.
            self._conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,
                timeout=30.0,
                check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._init_schema()
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "StateStore":
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("StateStore not opened")
        return self._conn

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        c = self.conn
        c.execute("BEGIN")
        try:
            yield c
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise

    def _init_schema(self) -> None:
        c = self.conn
        c.executescript(SCHEMA_V1)
        # Seed the singleton kill_switch row if missing
        existing = c.execute(
            "SELECT 1 FROM kill_switch WHERE id = 1"
        ).fetchone()
        if existing is None:
            now = _utc_now_iso()
            c.execute(
                "INSERT INTO kill_switch (id, state, reason, since, updated_at) "
                "VALUES (1, ?, NULL, ?, ?)",
                (KILL_SWITCH_ACTIVE, now, now),
            )
        # Record schema version (idempotent)
        self._ensure_order_telemetry_columns(c)
        c.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)", (1,)
        )

    def _ensure_order_telemetry_columns(self, c: sqlite3.Connection) -> None:
        """Add post-v1 order outcome columns to existing SQLite files.

        `CREATE TABLE IF NOT EXISTS` does not alter live DBs, so keep this
        migration idempotent and column-scoped.
        """
        existing = {
            row["name"]
            for row in c.execute("PRAGMA table_info(orders)").fetchall()
        }
        additions = {
            "exit_price": "REAL",
            "exit_quantity": "REAL",
            "exited_at": "TEXT",
            "exit_reason": "TEXT",
            "realized_pnl": "REAL",
        }
        for name, ddl_type in additions.items():
            if name not in existing:
                c.execute(f"ALTER TABLE orders ADD COLUMN {name} {ddl_type}")

    # ── Positions ─────────────────────────────────────────────────────────
    def upsert_position(
        self,
        *,
        symbol: str,
        exchange: str,
        side: int,
        quantity: float,
        avg_price: float,
    ) -> Position:
        now = _utc_now_iso()
        with self._txn() as c:
            existing = c.execute(
                "SELECT opened_at FROM positions WHERE symbol = ? AND exchange = ?",
                (symbol, exchange),
            ).fetchone()
            opened_at = existing["opened_at"] if existing else now
            c.execute(
                """
                INSERT INTO positions
                    (symbol, exchange, side, quantity, avg_price, opened_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, exchange) DO UPDATE SET
                    side       = excluded.side,
                    quantity   = excluded.quantity,
                    avg_price  = excluded.avg_price,
                    updated_at = excluded.updated_at
                """,
                (symbol, exchange, side, quantity, avg_price, opened_at, now),
            )
        return Position(
            symbol=symbol, exchange=exchange, side=side,
            quantity=quantity, avg_price=avg_price,
            opened_at=opened_at, updated_at=now,
        )

    def close_position(self, *, symbol: str, exchange: str) -> bool:
        with self._txn() as c:
            cur = c.execute(
                "DELETE FROM positions WHERE symbol = ? AND exchange = ?",
                (symbol, exchange),
            )
            return cur.rowcount > 0

    def close_stale_position(self, *, symbol: str, exchange: str, reason: str) -> bool:
        """Sprint 3 (2026-05-31) state-hygiene helper. Delete a local position
        row that the broker no longer reports.

        Behavior is identical to close_position(); the distinct entry point
        exists so callers can signal "this is hygiene cleanup, not normal
        close" — auditors / log readers can grep for close_stale_position
        calls when reconstructing why a position disappeared from local
        state without a corresponding ORDER_FILLED exit.

        Returns True iff a row was actually deleted (i.e. there was a stale
        row to clean). Caller is responsible for activity-chain logging.
        Per Codex HANDOFF 2026-05-30: this MUST only be called for positions
        where the broker reports flat — never use this on positions the
        broker still confirms (that would mask real exposure).
        """
        return self.close_position(symbol=symbol, exchange=exchange)

    def get_position(self, *, symbol: str, exchange: str) -> Optional[Position]:
        row = self.conn.execute(
            "SELECT * FROM positions WHERE symbol = ? AND exchange = ?",
            (symbol, exchange),
        ).fetchone()
        return _row_to_position(row) if row else None

    def get_open_positions(self) -> list[Position]:
        rows = self.conn.execute("SELECT * FROM positions ORDER BY symbol").fetchall()
        return [_row_to_position(r) for r in rows]

    # ── Orders ────────────────────────────────────────────────────────────
    def record_order(
        self,
        *,
        client_order_id: str,
        symbol: str,
        exchange: str,
        side: int,
        quantity: float,
        order_type: int,
        status: str = ORDER_STATUS_PENDING,
    ) -> Order:
        if status not in VALID_ORDER_STATUSES:
            raise ValueError(f"invalid order status: {status}")
        now = _utc_now_iso()
        with self._txn() as c:
            c.execute(
                """
                INSERT INTO orders
                    (client_order_id, symbol, exchange, side, quantity,
                     order_type, status, submitted_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (client_order_id, symbol, exchange, side, quantity,
                 order_type, status, now, now),
            )
        return Order(
            client_order_id=client_order_id, symbol=symbol, exchange=exchange,
            side=side, quantity=quantity, order_type=order_type, status=status,
            submitted_at=now, updated_at=now,
        )

    def update_order_status(
        self,
        *,
        client_order_id: str,
        status: str,
        fill_price: Optional[float] = None,
        fill_quantity: Optional[float] = None,
        exit_price: Optional[float] = None,
        exit_quantity: Optional[float] = None,
        exit_reason: Optional[str] = None,
        realized_pnl: Optional[float] = None,
        rejected_reason: Optional[str] = None,
    ) -> Optional[Order]:
        if status not in VALID_ORDER_STATUSES:
            raise ValueError(f"invalid order status: {status}")
        now = _utc_now_iso()
        filled_at = now if status == ORDER_STATUS_FILLED else None
        exited_at = now if exit_price is not None or exit_reason is not None else None
        with self._txn() as c:
            cur = c.execute(
                """
                UPDATE orders SET
                    status          = ?,
                    fill_price      = COALESCE(?, fill_price),
                    fill_quantity   = COALESCE(?, fill_quantity),
                    filled_at       = COALESCE(?, filled_at),
                    exit_price      = COALESCE(?, exit_price),
                    exit_quantity   = COALESCE(?, exit_quantity),
                    exited_at       = COALESCE(?, exited_at),
                    exit_reason     = COALESCE(?, exit_reason),
                    realized_pnl    = COALESCE(?, realized_pnl),
                    rejected_reason = COALESCE(?, rejected_reason),
                    updated_at      = ?
                WHERE client_order_id = ?
                """,
                (status, fill_price, fill_quantity, filled_at,
                 exit_price, exit_quantity, exited_at, exit_reason,
                 realized_pnl, rejected_reason, now, client_order_id),
            )
            if cur.rowcount == 0:
                return None
        return self.get_order(client_order_id=client_order_id)

    def get_order(self, *, client_order_id: str) -> Optional[Order]:
        row = self.conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?",
            (client_order_id,),
        ).fetchone()
        return _row_to_order(row) if row else None

    def get_active_orders(self) -> list[Order]:
        rows = self.conn.execute(
            "SELECT * FROM orders WHERE status IN (?, ?) ORDER BY submitted_at",
            (ORDER_STATUS_PENDING, ORDER_STATUS_WORKING),
        ).fetchall()
        return [_row_to_order(r) for r in rows]

    def mark_stale_order(self, *, client_order_id: str, reason: str) -> Optional[Order]:
        """Sprint 3 (2026-05-31) state-hygiene helper. Mark a locally-PENDING
        or WORKING order as CANCELLED with a stale-state reason, when the
        broker has no record of it.

        Reason is stored in rejected_reason (the existing column) with a
        'stale: ' prefix so audits can tell hygiene-cancels from broker-side
        rejects. Per Codex HANDOFF 2026-05-30: only call when the broker
        confirms the order is absent — never use this for orders still
        WORKING at the broker (that would mask a real fill we missed).

        Returns the updated Order, or None if the client_order_id doesn't
        exist locally.

        NOT independently idempotent: calling this directly on an already-
        CANCELLED row will overwrite its rejected_reason. The preflight
        path is safe because get_active_orders() excludes terminal-status
        rows, so a second preflight run does not re-call this helper on
        the same coid. Callers outside preflight should status-check
        first if they need idempotency.
        """
        return self.update_order_status(
            client_order_id=client_order_id,
            status=ORDER_STATUS_CANCELLED,
            rejected_reason=f"stale: {reason}",
        )

    # ── Kill switch ───────────────────────────────────────────────────────
    def trip_kill_switch(self, *, reason: str) -> KillSwitch:
        return self._set_kill_switch(KILL_SWITCH_TRIPPED, reason)

    def reset_kill_switch(self) -> KillSwitch:
        return self._set_kill_switch(KILL_SWITCH_ACTIVE, None)

    def _set_kill_switch(self, state: str, reason: Optional[str]) -> KillSwitch:
        now = _utc_now_iso()
        with self._txn() as c:
            current = c.execute(
                "SELECT state, since FROM kill_switch WHERE id = 1"
            ).fetchone()
            since = now if current is None or current["state"] != state else current["since"]
            c.execute(
                "UPDATE kill_switch SET state = ?, reason = ?, since = ?, updated_at = ? "
                "WHERE id = 1",
                (state, reason, since, now),
            )
        return KillSwitch(state=state, reason=reason, since=since, updated_at=now)

    def get_kill_switch(self) -> KillSwitch:
        row = self.conn.execute(
            "SELECT state, reason, since, updated_at FROM kill_switch WHERE id = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("kill_switch row missing — schema not initialized")
        return KillSwitch(
            state=row["state"], reason=row["reason"],
            since=row["since"], updated_at=row["updated_at"],
        )

    # ── Account snapshots ─────────────────────────────────────────────────
    def record_account_snapshot(
        self, *, nlv: float, drawdown_pct: float
    ) -> AccountSnapshot:
        now = _utc_now_iso()
        with self._txn() as c:
            c.execute(
                "INSERT INTO account_snapshots (nlv, drawdown_pct, recorded_at) "
                "VALUES (?, ?, ?)",
                (nlv, drawdown_pct, now),
            )
        return AccountSnapshot(nlv=nlv, drawdown_pct=drawdown_pct, recorded_at=now)

    def get_latest_account_snapshot(self) -> Optional[AccountSnapshot]:
        row = self.conn.execute(
            "SELECT nlv, drawdown_pct, recorded_at FROM account_snapshots "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return AccountSnapshot(
            nlv=row["nlv"], drawdown_pct=row["drawdown_pct"],
            recorded_at=row["recorded_at"],
        )


# ── Row mappers ────────────────────────────────────────────────────────────
def _row_to_position(row: sqlite3.Row) -> Position:
    return Position(
        symbol=row["symbol"], exchange=row["exchange"],
        side=row["side"], quantity=row["quantity"], avg_price=row["avg_price"],
        opened_at=row["opened_at"], updated_at=row["updated_at"],
    )


def _row_to_order(row: sqlite3.Row) -> Order:
    return Order(
        client_order_id=row["client_order_id"],
        symbol=row["symbol"], exchange=row["exchange"],
        side=row["side"], quantity=row["quantity"],
        order_type=row["order_type"], status=row["status"],
        submitted_at=row["submitted_at"], updated_at=row["updated_at"],
        fill_price=row["fill_price"], fill_quantity=row["fill_quantity"],
        filled_at=row["filled_at"],
        exit_price=row["exit_price"], exit_quantity=row["exit_quantity"],
        exited_at=row["exited_at"], exit_reason=row["exit_reason"],
        realized_pnl=row["realized_pnl"],
        rejected_reason=row["rejected_reason"],
    )
