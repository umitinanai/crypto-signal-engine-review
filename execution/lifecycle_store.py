"""
Durable SQLite persistence for the autonomous Testnet trading lifecycle
(Phase 3/11/15). This EXTENDS the existing Testnet execution database
(same file as `reconciliation_store.py::ExecutionStateStore`) with
ADDITIVE, migration-safe new tables — `execution_record`'s schema and
`SCHEMA_VERSION` are never touched, so all of Phase 11's existing
concurrency/reconciliation guarantees and tests are untouched.

Three new tables, all `CREATE TABLE IF NOT EXISTS` (never a destructive
migration):
- `bridge_position`: one row per symbol, the CURRENT lifecycle state
  (Phase 3's durable fields). Upserted, never deleted.
- `bridge_fee_ledger`: append-only, one row per observed commission
  (Phase 5's fee ledger — grouped by `trade_group_id`, which is the
  position's entry `client_order_id`, so a completed trade's fees can be
  looked up precisely even across many re-entries of the same symbol).
- `bridge_completed_trade`: append-only, one row per fully closed round
  trip (Phase 14/15's trade history).
- `bridge_daily_risk`: one row per UTC trading day, the Phase 11 daily
  conservative-risk-P&L accumulator (never reset by a restart).

HARD SAFETY INVARIANT: no table here has any credential/secret column."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    DailyRiskAccumulator,
    FeeLedgerEntry,
    PositionLifecycleState,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bridge_position (
    symbol TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    gross_entry_vwap REAL,
    net_owned_base_quantity REAL NOT NULL,
    entry_order_client_order_id TEXT,
    initial_protective_stop REAL,
    high_water REAL,
    effective_stop REAL,
    trailing_active INTEGER NOT NULL,
    last_stop_mechanism TEXT NOT NULL,
    take_profit REAL,
    last_evaluated_candle_close TEXT,
    exit_pending_client_order_id TEXT,
    last_exit_reason TEXT,
    entry_timestamp TEXT,
    entry_signal_context_id TEXT,
    entry_client_order_id TEXT,
    cumulative_realized_gross_pnl REAL NOT NULL,
    cooldown_until TEXT,
    migrated_existing_position INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bridge_fee_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_group_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side TEXT NOT NULL,
    amount REAL NOT NULL,
    asset TEXT NOT NULL,
    usdt_equivalent REAL,
    trade_id INTEGER,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bridge_fee_ledger_group ON bridge_fee_ledger(symbol, trade_group_id);

CREATE TABLE IF NOT EXISTS bridge_completed_trade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_group_id TEXT NOT NULL,
    entry_client_order_id TEXT NOT NULL,
    exit_client_order_id TEXT NOT NULL,
    entry_timestamp TEXT NOT NULL,
    exit_timestamp TEXT NOT NULL,
    quantity_closed REAL NOT NULL,
    gross_entry_vwap REAL NOT NULL,
    exit_gross_vwap REAL NOT NULL,
    gross_realized_pnl REAL NOT NULL,
    net_realized_pnl REAL,
    exit_reason TEXT NOT NULL,
    entry_signal_context_id TEXT,
    exit_signal_context_id TEXT,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bridge_completed_trade_symbol ON bridge_completed_trade(symbol, recorded_at);

CREATE TABLE IF NOT EXISTS bridge_daily_risk (
    trading_day TEXT PRIMARY KEY,
    conservative_risk_pnl REAL NOT NULL,
    trades_counted INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
"""

# Adaptive Intelligence v1 — ADDITIVE-only columns (no destructive
# migration, no column ever dropped/renamed). `CREATE TABLE IF NOT
# EXISTS` above never adds columns to an already-existing table, so a
# real Testnet database created before this change needs these applied
# via `ALTER TABLE ... ADD COLUMN` on open (idempotently — SQLite has no
# `ADD COLUMN IF NOT EXISTS`, so a "duplicate column" error is expected
# and swallowed on every run after the first).
_BRIDGE_POSITION_NEW_COLUMNS = (
    ("exit_policy_stop_atr_multiple", "REAL"),
    ("exit_policy_take_profit_atr_multiple", "REAL"),
    ("exit_policy_trailing_activation_atr_multiple", "REAL"),
    ("exit_policy_trailing_distance_atr_multiple", "REAL"),
    ("exit_policy_max_hold_hours", "REAL"),
    ("policy_version_id", "TEXT"),
)
_BRIDGE_COMPLETED_TRADE_NEW_COLUMNS = (
    ("policy_version_id", "TEXT"),
)


def _add_column_if_missing(connection: sqlite3.Connection, table: str, column: str, sql_type: str) -> None:
    try:
        with connection:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class LifecycleStore:
    """Opens its OWN connection to the SAME execution-db file used by
    `ExecutionStateStore` — SQLite's WAL mode makes this safe for
    concurrent readers/writers across connections (same discipline the
    codebase already relies on for `paper_state.db` vs. `testnet_execution.db`
    being separate files; here it is the same file, safe because every
    write here is independently transacted and touches disjoint tables)."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
            for column, sql_type in _BRIDGE_POSITION_NEW_COLUMNS:
                _add_column_if_missing(self._connection, "bridge_position", column, sql_type)
            for column, sql_type in _BRIDGE_COMPLETED_TRADE_NEW_COLUMNS:
                _add_column_if_missing(self._connection, "bridge_completed_trade", column, sql_type)
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle schema oluşturma başarısız: {exc}") from exc

    def close(self) -> None:
        self._connection.close()

    # -- Transaction discipline (CRITICAL FIX M1 — mainnet-readiness review) --
    #
    # This connection is opened with `isolation_level=None` (autocommit) —
    # EXACTLY like `reconciliation_store.py`'s own connection, and for the
    # SAME reason (explicit `BEGIN IMMEDIATE` control, see that module's
    # `__init__` comment). Unlike `reconciliation_store.py`, every write
    # method here used to wrap its statement(s) in `with self._connection:`
    # instead — under `isolation_level=None` that context manager is a
    # SILENT NO-OP (Python's sqlite3 module only auto-manages BEGIN/COMMIT
    # when `isolation_level` is NOT `None`), so it provided the ILLUSION of
    # a transaction while actually running every statement in true SQLite
    # autocommit mode. Two concrete gaps this caused:
    #   1. `append_fee_entries()`'s `executemany()` INSERT of several rows
    #      had NO atomicity guarantee across those rows — a crash/OS error
    #      mid-loop could leave a PARTIAL fee ledger for one trade group.
    #   2. `_finalize_exit()` (lifecycle_manager.py) makes FOUR separate
    #      store calls to close one trade (`append_fee_entries()` ->
    #      `record_completed_trade()` -> `save_daily_risk()` ->
    #      `save_position()`) — each was independently "transacted" (i.e.
    #      not really), so a crash between any two of them could leave
    #      inconsistent cross-table state: e.g. a `bridge_completed_trade`
    #      row recorded but `bridge_position` never actually flipped out of
    #      LONG, or a position flipped FLAT with the trade never recorded.
    #
    # Fix: every write method below now uses the SAME explicit `BEGIN
    # IMMEDIATE` / `commit()` / `rollback()` discipline `reconciliation_
    # store.py::save()` already established, via the `_begin`/`_commit`/
    # `_rollback` helpers and `_locked` inner methods (assume a transaction
    # is already open — never call these directly). `save_position_with_
    # fee_entries()` and `finalize_exit()` are NEW composite methods that
    # run their entire multi-statement write in ONE transaction — the two
    # cross-table call sites above (`on_entry_filled()`/`_finalize_exit()`
    # in lifecycle_manager.py) now use these instead of separate calls.

    def _begin(self) -> None:
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle store: transaction başlatılamadı: {exc}") from exc

    def _commit(self) -> None:
        try:
            self._connection.commit()
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle store: transaction commit başarısız: {exc}") from exc

    def _rollback(self) -> None:
        try:
            self._connection.rollback()
        except sqlite3.Error:
            pass  # best-effort — the ORIGINAL error is what propagates to the caller

    # -- bridge_position ----------------------------------------------------

    def save_position(self, record: BridgePositionRecord) -> None:
        self._begin()
        try:
            self._save_position_locked(record)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_position kaydı başarısız (symbol={record.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _save_position_locked(self, record: BridgePositionRecord) -> None:
        """Assumes a transaction is ALREADY open (`_begin()` already
        called by the caller) — never call directly."""
        self._connection.execute(
            """
            INSERT INTO bridge_position (
                        symbol, state, gross_entry_vwap, net_owned_base_quantity,
                        entry_order_client_order_id, initial_protective_stop, high_water, effective_stop,
                        trailing_active, last_stop_mechanism, take_profit, last_evaluated_candle_close,
                        exit_pending_client_order_id, last_exit_reason, entry_timestamp,
                        entry_signal_context_id, entry_client_order_id, cumulative_realized_gross_pnl,
                        cooldown_until, migrated_existing_position, updated_at,
                        exit_policy_stop_atr_multiple, exit_policy_take_profit_atr_multiple,
                        exit_policy_trailing_activation_atr_multiple, exit_policy_trailing_distance_atr_multiple,
                        exit_policy_max_hold_hours, policy_version_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        state=excluded.state, gross_entry_vwap=excluded.gross_entry_vwap,
                        net_owned_base_quantity=excluded.net_owned_base_quantity,
                        entry_order_client_order_id=excluded.entry_order_client_order_id,
                        initial_protective_stop=excluded.initial_protective_stop,
                        high_water=excluded.high_water, effective_stop=excluded.effective_stop,
                        trailing_active=excluded.trailing_active, last_stop_mechanism=excluded.last_stop_mechanism,
                        take_profit=excluded.take_profit,
                        last_evaluated_candle_close=excluded.last_evaluated_candle_close,
                        exit_pending_client_order_id=excluded.exit_pending_client_order_id,
                        last_exit_reason=excluded.last_exit_reason, entry_timestamp=excluded.entry_timestamp,
                        entry_signal_context_id=excluded.entry_signal_context_id,
                        entry_client_order_id=excluded.entry_client_order_id,
                        cumulative_realized_gross_pnl=excluded.cumulative_realized_gross_pnl,
                        cooldown_until=excluded.cooldown_until,
                        migrated_existing_position=excluded.migrated_existing_position,
                        updated_at=excluded.updated_at,
                        exit_policy_stop_atr_multiple=excluded.exit_policy_stop_atr_multiple,
                        exit_policy_take_profit_atr_multiple=excluded.exit_policy_take_profit_atr_multiple,
                        exit_policy_trailing_activation_atr_multiple=excluded.exit_policy_trailing_activation_atr_multiple,
                        exit_policy_trailing_distance_atr_multiple=excluded.exit_policy_trailing_distance_atr_multiple,
                        exit_policy_max_hold_hours=excluded.exit_policy_max_hold_hours,
                        policy_version_id=excluded.policy_version_id
                    """,
                    (
                        record.symbol, record.state.value, record.gross_entry_vwap,
                        record.net_owned_base_quantity, record.entry_order_client_order_id,
                        record.initial_protective_stop, record.high_water, record.effective_stop,
                        int(record.trailing_active), record.last_stop_mechanism, record.take_profit,
                        _iso(record.last_evaluated_candle_close), record.exit_pending_client_order_id,
                        record.last_exit_reason, _iso(record.entry_timestamp),
                        record.entry_signal_context_id, record.entry_client_order_id,
                        record.cumulative_realized_gross_pnl, _iso(record.cooldown_until),
                        int(record.migrated_existing_position), _iso(record.updated_at),
                        record.exit_policy_stop_atr_multiple, record.exit_policy_take_profit_atr_multiple,
                        record.exit_policy_trailing_activation_atr_multiple,
                        record.exit_policy_trailing_distance_atr_multiple,
                record.exit_policy_max_hold_hours, record.policy_version_id,
            ),
        )

    def load_position(self, symbol: str) -> BridgePositionRecord | None:
        normalized = normalize_symbol(symbol)
        row = self._connection.execute(
            "SELECT * FROM bridge_position WHERE symbol = ?", (normalized,)
        ).fetchone()
        if row is None:
            return None
        try:
            return BridgePositionRecord(
                symbol=row["symbol"], state=PositionLifecycleState(row["state"]),
                gross_entry_vwap=row["gross_entry_vwap"], net_owned_base_quantity=row["net_owned_base_quantity"],
                entry_order_client_order_id=row["entry_order_client_order_id"],
                initial_protective_stop=row["initial_protective_stop"], high_water=row["high_water"],
                effective_stop=row["effective_stop"], trailing_active=bool(row["trailing_active"]),
                last_stop_mechanism=row["last_stop_mechanism"], take_profit=row["take_profit"],
                last_evaluated_candle_close=_parse_dt(row["last_evaluated_candle_close"]),
                exit_pending_client_order_id=row["exit_pending_client_order_id"],
                last_exit_reason=row["last_exit_reason"], entry_timestamp=_parse_dt(row["entry_timestamp"]),
                entry_signal_context_id=row["entry_signal_context_id"],
                entry_client_order_id=row["entry_client_order_id"],
                cumulative_realized_gross_pnl=row["cumulative_realized_gross_pnl"],
                cooldown_until=_parse_dt(row["cooldown_until"]),
                migrated_existing_position=bool(row["migrated_existing_position"]),
                updated_at=_parse_dt(row["updated_at"]),
                # Adaptive Intelligence v1 additive columns — `row[...]`
                # returns SQL NULL as `None` for a legacy row written
                # before these columns existed (they are added via
                # `ALTER TABLE ... ADD COLUMN` with no DEFAULT), which is
                # exactly `BridgePositionRecord`'s own default for these
                # fields — `resolved_exit_policy()` then correctly falls
                # back to the caller's current `ExitPolicyConfig`.
                exit_policy_stop_atr_multiple=row["exit_policy_stop_atr_multiple"],
                exit_policy_take_profit_atr_multiple=row["exit_policy_take_profit_atr_multiple"],
                exit_policy_trailing_activation_atr_multiple=row["exit_policy_trailing_activation_atr_multiple"],
                exit_policy_trailing_distance_atr_multiple=row["exit_policy_trailing_distance_atr_multiple"],
                exit_policy_max_hold_hours=row["exit_policy_max_hold_hours"],
                policy_version_id=row["policy_version_id"],
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise CorruptRecordError(f"bozuk bridge_position satırı ({symbol!r}): {exc}") from exc

    def list_positions(self) -> tuple[BridgePositionRecord, ...]:
        rows = self._connection.execute("SELECT symbol FROM bridge_position ORDER BY symbol ASC").fetchall()
        return tuple(self.load_position(row["symbol"]) for row in rows)  # type: ignore[misc]

    # -- bridge_fee_ledger ----------------------------------------------------

    def append_fee_entries(self, symbol: str, trade_group_id: str, entries: tuple[FeeLedgerEntry, ...]) -> None:
        if not entries:
            return
        self._begin()
        try:
            self._append_fee_entries_locked(symbol, trade_group_id, entries)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(
                f"bridge_fee_ledger yazımı başarısız (symbol={symbol}, trade_group_id={trade_group_id}): {exc}"
            ) from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _append_fee_entries_locked(self, symbol: str, trade_group_id: str, entries: tuple[FeeLedgerEntry, ...]) -> None:
        """Assumes a transaction is ALREADY open — never call directly.
        CRITICAL FIX (M1 — mainnet-readiness review): this `executemany()`
        previously ran under `with self._connection:`, a no-op under
        `isolation_level=None` — a crash mid-loop could leave a PARTIAL
        fee ledger for one trade group (some fills recorded, others not).
        Now runs inside the caller's single explicit transaction, so
        either ALL of these rows land or NONE do."""
        if not entries:
            return
        normalized = normalize_symbol(symbol)
        self._connection.executemany(
            """
            INSERT INTO bridge_fee_ledger (
                symbol, trade_group_id, client_order_id, side, amount, asset,
                usdt_equivalent, trade_id, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    normalized, trade_group_id, e.client_order_id, e.side, e.amount, e.asset,
                    e.usdt_equivalent, e.trade_id, e.recorded_at.isoformat(),
                )
                for e in entries
            ],
        )

    def fee_ledger_for_trade_group(self, symbol: str, trade_group_id: str) -> tuple[FeeLedgerEntry, ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM bridge_fee_ledger WHERE symbol = ? AND trade_group_id = ? ORDER BY id ASC",
            (normalized, trade_group_id),
        ).fetchall()
        return tuple(
            FeeLedgerEntry(
                amount=row["amount"], asset=row["asset"], usdt_equivalent=row["usdt_equivalent"],
                client_order_id=row["client_order_id"], side=row["side"], trade_id=row["trade_id"],
                recorded_at=_parse_dt(row["recorded_at"]),  # type: ignore[arg-type]
            )
            for row in rows
        )

    # -- bridge_completed_trade ----------------------------------------------

    def record_completed_trade(
        self,
        *,
        symbol: str,
        trade_group_id: str,
        entry_client_order_id: str,
        exit_client_order_id: str,
        entry_timestamp: datetime,
        exit_timestamp: datetime,
        quantity_closed: float,
        gross_entry_vwap: float,
        exit_gross_vwap: float,
        gross_realized_pnl: float,
        net_realized_pnl: float | None,
        exit_reason: str,
        entry_signal_context_id: str | None,
        exit_signal_context_id: str | None,
        now: datetime,
        policy_version_id: str | None = None,
    ) -> None:
        self._begin()
        try:
            self._record_completed_trade_locked(
                symbol=symbol, trade_group_id=trade_group_id, entry_client_order_id=entry_client_order_id,
                exit_client_order_id=exit_client_order_id, entry_timestamp=entry_timestamp,
                exit_timestamp=exit_timestamp, quantity_closed=quantity_closed, gross_entry_vwap=gross_entry_vwap,
                exit_gross_vwap=exit_gross_vwap, gross_realized_pnl=gross_realized_pnl,
                net_realized_pnl=net_realized_pnl, exit_reason=exit_reason,
                entry_signal_context_id=entry_signal_context_id, exit_signal_context_id=exit_signal_context_id,
                now=now, policy_version_id=policy_version_id,
            )
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_completed_trade yazımı başarısız (symbol={symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _record_completed_trade_locked(
        self,
        *,
        symbol: str,
        trade_group_id: str,
        entry_client_order_id: str,
        exit_client_order_id: str,
        entry_timestamp: datetime,
        exit_timestamp: datetime,
        quantity_closed: float,
        gross_entry_vwap: float,
        exit_gross_vwap: float,
        gross_realized_pnl: float,
        net_realized_pnl: float | None,
        exit_reason: str,
        entry_signal_context_id: str | None,
        exit_signal_context_id: str | None,
        now: datetime,
        policy_version_id: str | None = None,
    ) -> None:
        """Assumes a transaction is ALREADY open — never call directly."""
        normalized = normalize_symbol(symbol)
        self._connection.execute(
            """
            INSERT INTO bridge_completed_trade (
                symbol, trade_group_id, entry_client_order_id, exit_client_order_id,
                entry_timestamp, exit_timestamp, quantity_closed, gross_entry_vwap, exit_gross_vwap,
                gross_realized_pnl, net_realized_pnl, exit_reason, entry_signal_context_id,
                exit_signal_context_id, recorded_at, policy_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized, trade_group_id, entry_client_order_id, exit_client_order_id,
                entry_timestamp.isoformat(), exit_timestamp.isoformat(), quantity_closed,
                gross_entry_vwap, exit_gross_vwap, gross_realized_pnl, net_realized_pnl, exit_reason,
                entry_signal_context_id, exit_signal_context_id, now.isoformat(), policy_version_id,
            ),
        )

    def recent_completed_trades(self, limit: int = 50) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade ORDER BY recorded_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def completed_trades_for_symbol(self, symbol: str) -> tuple[dict[str, object], ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade WHERE symbol = ? ORDER BY recorded_at ASC", (normalized,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def total_realized_pnl(self) -> tuple[float, int]:
        """Portfolio/Accounting v1, step 2 — TRUE lifetime realized P&L:
        `SUM(net_realized_pnl)`/`COUNT(*)` over EVERY completed trade
        EVER, via one SQL aggregate query — no `LIMIT`, no in-Python
        summation of a bounded `recent_completed_trades()` fetch (the
        previous dashboard code's blind spot once more than 50 trades
        exist). `SUM()` already skips `NULL` `net_realized_pnl` rows per
        standard SQL (a trade whose fee USDT-equivalent could not be
        resolved, see `net_realized_pnl()` in `lifecycle.py`) — the
        returned count is `COUNT(*)`, every completed trade regardless of
        whether its net P&L happens to be known, since a trade with
        unknown net P&L is still a completed TRADE. Returns `(0.0, 0)`
        for an empty table (`COALESCE`, never a SQL `NULL` sum)."""
        row = self._connection.execute(
            "SELECT COALESCE(SUM(net_realized_pnl), 0.0) AS total, COUNT(*) AS cnt FROM bridge_completed_trade"
        ).fetchone()
        return float(row["total"]), int(row["cnt"])

    def realized_pnl_for_day(self, trading_day: str) -> tuple[float, int]:
        """Portfolio/Accounting v1, step 2 — same SQL-aggregate discipline
        as `total_realized_pnl()`, filtered to one UTC calendar day.
        `trading_day` MUST be `trading_day_key()`'s exact `"YYYY-MM-DD"`
        format — this reuses that SAME convention rather than inventing a
        second day-boundary definition, by matching the first 10
        characters of the `recorded_at` column (an ISO-8601 string whose
        datetime is always UTC-aware by construction, so its own date
        component is exactly what `trading_day_key()` would compute for
        that same instant).

        WHY `recorded_at`, NOT `exit_timestamp`: `_finalize_exit()`
        passes the EXACT SAME `now` to both `record_completed_trade(...,
        now=now)` (this column) AND `trading_day_key(now)` when updating
        `bridge_daily_risk` — using `recorded_at` here means this
        function's day bucket is anchored to the IDENTICAL moment
        `DailyRiskAccumulator` uses, not the (usually equal, but not
        structurally guaranteed identical) `exit_timestamp` sourced from
        the exchange's own `updated_at`.

        RECONCILING THIS AGAINST `DailyRiskAccumulator.conservative_
        risk_pnl` FOR THE SAME DAY (they are allowed to differ, and here
        is why, so a future reader never has to rediscover this): this
        function's sum is NET (fees subtracted in full, via `lifecycle.
        net_realized_pnl()` at trade-completion time -- possibly `NULL`/
        excluded if any fee's USDT value was unknown) applied to CLOSED
        trades only. `conservative_risk_pnl` is a DELIBERATELY PESSIMISTIC
        risk-breaker accumulator (`compute_trade_risk_contribution()`):
        it subtracts every KNOWN fee AND an additional conservative
        RESERVE for every fee leg whose USDT value could NOT be resolved
        (never simply omits it) -- so `conservative_risk_pnl` is expected
        to run equal to or WORSE (more negative / less positive) than this
        function's true net sum whenever any trade that day had an
        unresolved fee leg, and identical when every fee that day was
        fully known. This is intentional: the risk breaker is deliberately
        pessimistic (fails closed), while this function reports the true,
        undistorted net P&L for observability."""
        row = self._connection.execute(
            "SELECT COALESCE(SUM(net_realized_pnl), 0.0) AS total, COUNT(*) AS cnt "
            "FROM bridge_completed_trade WHERE substr(recorded_at, 1, 10) = ?",
            (trading_day,),
        ).fetchone()
        return float(row["total"]), int(row["cnt"])

    def completed_trades_for_policy_version(self, policy_version_id: str) -> tuple[dict[str, object], ...]:
        """Adaptive Intelligence v1, step 3 — trade/policy attribution: a
        finalized round trip's `policy_version_id` (`None` for every
        trade closed before this milestone, or for a position opened
        without `exit_policy_provider` wired in) survives from the open
        `bridge_position` row through to `bridge_completed_trade`, so a
        champion's REAL Testnet/PAPER evidence can be queried here by
        version — never mixed with discovery/confirmation/shadow
        evidence (those live in `adaptive/`'s own store)."""
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade WHERE policy_version_id = ? ORDER BY recorded_at ASC",
            (policy_version_id,),
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- bridge_daily_risk ----------------------------------------------------

    def load_daily_risk(self, trading_day: str) -> DailyRiskAccumulator:
        row = self._connection.execute(
            "SELECT * FROM bridge_daily_risk WHERE trading_day = ?", (trading_day,)
        ).fetchone()
        if row is None:
            return DailyRiskAccumulator(trading_day=trading_day)
        return DailyRiskAccumulator(
            trading_day=row["trading_day"], conservative_risk_pnl=row["conservative_risk_pnl"],
            trades_counted=row["trades_counted"],
        )

    def save_daily_risk(self, accumulator: DailyRiskAccumulator, *, now: datetime) -> None:
        self._begin()
        try:
            self._save_daily_risk_locked(accumulator, now=now)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_daily_risk yazımı başarısız (trading_day={accumulator.trading_day}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _save_daily_risk_locked(self, accumulator: DailyRiskAccumulator, *, now: datetime) -> None:
        """Assumes a transaction is ALREADY open — never call directly."""
        self._connection.execute(
            """
            INSERT INTO bridge_daily_risk (trading_day, conservative_risk_pnl, trades_counted, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(trading_day) DO UPDATE SET
                conservative_risk_pnl=excluded.conservative_risk_pnl,
                trades_counted=excluded.trades_counted, updated_at=excluded.updated_at
            """,
            (accumulator.trading_day, accumulator.conservative_risk_pnl, accumulator.trades_counted, now.isoformat()),
        )

    # -- Atomic composite writes (CRITICAL FIX M1) ---------------------------

    def save_position_with_fee_entries(
        self, record: BridgePositionRecord, *, trade_group_id: str, fee_entries: tuple[FeeLedgerEntry, ...]
    ) -> None:
        """Atomically persists a `bridge_position` row together with its
        fee-ledger entries, in ONE transaction — used by `on_entry_filled()`
        (lifecycle_manager.py) so a crash between the two writes can never
        leave fee-ledger rows for a position that was never actually saved,
        or vice versa."""
        self._begin()
        try:
            self._save_position_locked(record)
            self._append_fee_entries_locked(record.symbol, trade_group_id, fee_entries)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_position+fee_ledger atomik kaydı başarısız (symbol={record.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def finalize_exit(
        self,
        *,
        new_position: BridgePositionRecord,
        trade_group_id: str,
        fee_entries: tuple[FeeLedgerEntry, ...],
        completed_trade: dict[str, object],
        daily_risk: DailyRiskAccumulator,
        daily_risk_now: datetime,
    ) -> None:
        """CRITICAL FIX (M1 — mainnet-readiness review): atomically performs
        ALL FOUR writes a closed trade requires — fee-ledger append,
        completed-trade record, daily-risk accumulator update, and the
        final `bridge_position` save — in ONE transaction. `_finalize_exit()`
        (lifecycle_manager.py) previously made these as four SEPARATE store
        calls; under the (broken) `with self._connection:` no-op discipline
        described above, a crash between any two of them could leave
        cross-table state inconsistent (e.g. a trade recorded as completed
        while `bridge_position` never actually left LONG). `completed_trade`
        holds exactly `record_completed_trade()`'s own keyword arguments
        (minus `self`) — passed through unchanged, never a second,
        divergent schema."""
        self._begin()
        try:
            self._append_fee_entries_locked(new_position.symbol, trade_group_id, fee_entries)
            self._record_completed_trade_locked(**completed_trade)
            self._save_daily_risk_locked(daily_risk, now=daily_risk_now)
            self._save_position_locked(new_position)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"finalize_exit atomik kaydı başarısız (symbol={new_position.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()
