"""
Faz 11 — durable `ExecutionRecord` persistence (SQLite).

Mimari kural ("persistence bir SINIRDIR, domain nesnelerini database-aware
YAPMAZ" — Faz 7 ile AYNI ilke): bu modül `ExecutionRecord`'u DEĞİŞTİRMEZ,
onu SARAR. Explicit `SCHEMA_VERSION` + sessiz migration YOK (Faz 7 ile
AYNI disiplin — bkz. `persistence/paper_state_store.py`).

HARD SAFETY INVARIANT: bu şemada API key/secret/signature alanı YOKTUR ve
OLAMAZ — yalnızca order kimliği/miktarları/durumu/zaman damgaları."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.errors import ReconciliationContradictionError
from crypto_signal_engine.execution.models import OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import (
    NEEDS_RECONCILIATION_STATES,
    ExecutionRecord,
    assert_monotonic,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS execution_record (
    context_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity REAL,
    quote_quantity REAL,
    price REAL,
    lifecycle_state TEXT NOT NULL,
    exchange_order_id INTEGER,
    executed_quantity REAL NOT NULL,
    cumulative_quote_quantity REAL NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_reconciled_at TEXT
);
"""


def _parse_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _row_to_record(row: sqlite3.Row) -> ExecutionRecord:
    try:
        return ExecutionRecord(
            context_id=row["context_id"],
            symbol=row["symbol"],
            client_order_id=row["client_order_id"],
            side=OrderSide(row["side"]),
            order_type=OrderType(row["order_type"]),
            quantity=row["quantity"],
            quote_quantity=row["quote_quantity"],
            price=row["price"],
            lifecycle_state=row["lifecycle_state"],
            exchange_order_id=row["exchange_order_id"],
            executed_quantity=row["executed_quantity"],
            cumulative_quote_quantity=row["cumulative_quote_quantity"],
            detail=row["detail"],
            created_at=_parse_utc(row["created_at"]),
            updated_at=_parse_utc(row["updated_at"]),
            last_reconciled_at=_parse_utc(row["last_reconciled_at"]),
        )
    except (ValueError, TypeError, KeyError) as exc:
        raise CorruptRecordError(f"bozuk execution_record satırı ({row['context_id']!r}): {exc}") from exc


class ExecutionStateStore:
    """Faz 11 execution-lifecycle durumu için durable, injectable SQLite
    store. `db_path` HER ZAMAN çağıran tarafından enjekte edilir."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            # `isolation_level=None` (autocommit) — `save()` artık `BEGIN
            # IMMEDIATE`/`commit()`/`rollback()`'ı ELLE yönetir (bkz. HIGH-5
            # fix notu, aşağı); Python'ın KENDİ implicit transaction
            # yönetimiyle (varsayılan isolation_level) ÇAKIŞMAMASI için
            # KASITLI OLARAK devre dışı bırakılır.
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        # BLOCKER FİX (HIGH-5): `save()` artık `BEGIN IMMEDIATE` kullanır
        # (bkz. aşağı) — bu, AYNI context_id için EŞZAMANLI yazan İKİNCİ bir
        # process'i/connection'ı KISA SÜRELİĞİNE bloklar (ilk yazar commit
        # edene kadar). `busy_timeout` OLMADAN SQLite bu durumda ANINDA
        # `database is locked` hatası fırlatırdı — birkaç saniyelik bir
        # bekleme payı, normal (kötü niyetli olmayan) eşzamanlı submit/
        # reconcile çakışmalarının SESSİZCE (retry'siz, engine seviyesinde)
        # çözülmesini sağlar.
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
                    return
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

        existing = self._connection.execute("SELECT version FROM schema_version").fetchone()[0]
        if existing != SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                f"Durable execution store schema_version={existing}, bu kod SCHEMA_VERSION={SCHEMA_VERSION} "
                f"bekliyor — sessiz migration YAPILMAZ ({self._db_path})"
            )

    # -- Yazma ------------------------------------------------------------------

    def save(self, record: ExecutionRecord) -> None:
        """`context_id` PRIMARY KEY'i üzerinden GÜVENLİ (monotonik) bir
        upsert — TEK bir atomik transaction (kısmi bir yazma asla görünmez).

        BLOCKER FİX (HIGH-5 — "concurrent submit aynı deterministic
        clientOrderId ile local durable truth'u corrupt edebilir, örn.
        FILLED -> REJECTED"): eski implementasyon KÖRÜ KÖRÜNE bir
        `INSERT ... ON CONFLICT DO UPDATE` yapıyordu — çağıranın elindeki
        `record` DİSKTEKİ güncel satırdan daha ESKİ/BAYAT olsa BİLE
        (iki eşzamanlı `submit()`/`reconcile()` çağrısı, HER BİRİ kendi
        in-memory kopyasından "geçerli" bir geçiş üretebilir) SESSİZCE
        üzerine yazardı. ARTIK: `BEGIN IMMEDIATE` ile bu context_id için
        yazma kilidini ÖNCE alır (AYNI context_id'e eşzamanlı yazmaya
        çalışan başka bir process/connection, bu transaction bitene kadar
        BLOKLANIR — bkz. `busy_timeout`, `__init__`), SONRA disk üzerindeki
        GÜNCEL satırı okur, `assert_monotonic()` ile geçişin İMKANSIZ bir
        regresyon OLMADIĞINI doğrular, YALNIZCA O ZAMAN yazar. Bir
        regresyon tespit edilirse `ReconciliationContradictionError`
        fırlatılır (transaction rollback edilir, DURABLE TRUTH
        DOKUNULMADAN kalır) — çağıran (`ExecutionReconciliationService.
        _save_or_defer_to_winner`) bunu yakalayıp OTORİTER (kazanan)
        gerçeği yeniden okur."""
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise PersistenceError(
                f"execution_record checkpoint kilidi alınamadı (context_id={record.context_id}): {exc}"
            ) from exc

        try:
            row = self._connection.execute(
                "SELECT * FROM execution_record WHERE context_id = ?", (record.context_id,)
            ).fetchone()
            if row is not None:
                assert_monotonic(_row_to_record(row), record)
            self._connection.execute(
                """
                INSERT INTO execution_record (
                    context_id, symbol, client_order_id, side, order_type, quantity, quote_quantity,
                    price, lifecycle_state, exchange_order_id, executed_quantity, cumulative_quote_quantity,
                    detail, created_at, updated_at, last_reconciled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(context_id) DO UPDATE SET
                    symbol = excluded.symbol,
                    client_order_id = excluded.client_order_id,
                    side = excluded.side,
                    order_type = excluded.order_type,
                    quantity = excluded.quantity,
                    quote_quantity = excluded.quote_quantity,
                    price = excluded.price,
                    lifecycle_state = excluded.lifecycle_state,
                    exchange_order_id = excluded.exchange_order_id,
                    executed_quantity = excluded.executed_quantity,
                    cumulative_quote_quantity = excluded.cumulative_quote_quantity,
                    detail = excluded.detail,
                    updated_at = excluded.updated_at,
                    last_reconciled_at = excluded.last_reconciled_at
                """,
                (
                    record.context_id, record.symbol, record.client_order_id, record.side.value,
                    record.order_type.value, record.quantity, record.quote_quantity, record.price,
                    record.lifecycle_state, record.exchange_order_id, record.executed_quantity,
                    record.cumulative_quote_quantity, record.detail, record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                    record.last_reconciled_at.isoformat() if record.last_reconciled_at is not None else None,
                ),
            )
        except (ReconciliationContradictionError, CorruptRecordError):
            self._connection.rollback()
            raise
        except sqlite3.Error as exc:
            self._connection.rollback()
            raise PersistenceError(
                f"execution_record checkpoint başarısız (context_id={record.context_id}): {exc}"
            ) from exc
        else:
            try:
                self._connection.commit()
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"execution_record checkpoint commit başarısız (context_id={record.context_id}): {exc}"
                ) from exc

    # -- Okuma --------------------------------------------------------------------

    def load_by_context_id(self, context_id: str) -> ExecutionRecord | None:
        row = self._connection.execute(
            "SELECT * FROM execution_record WHERE context_id = ?", (context_id,)
        ).fetchone()
        return _row_to_record(row) if row is not None else None

    def load_by_client_order_id(self, client_order_id: str) -> ExecutionRecord | None:
        row = self._connection.execute(
            "SELECT * FROM execution_record WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        return _row_to_record(row) if row is not None else None

    def list_for_symbol(self, symbol: str) -> tuple[ExecutionRecord, ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM execution_record WHERE symbol = ? ORDER BY created_at ASC", (normalized,)
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def list_needing_reconciliation(self) -> tuple[ExecutionRecord, ...]:
        placeholders = ", ".join("?" for _ in NEEDS_RECONCILIATION_STATES)
        rows = self._connection.execute(
            f"SELECT * FROM execution_record WHERE lifecycle_state IN ({placeholders}) ORDER BY created_at ASC",
            tuple(NEEDS_RECONCILIATION_STATES),
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def list_all_symbols(self) -> tuple[str, ...]:
        """Every distinct symbol with at least one execution record
        (bridge- or `lab-`-namespaced alike) — used by startup symbol-set
        computation (Phase 4/16) to find real bridge-owned inventory that
        must never be silently excluded from the runtime universe."""
        rows = self._connection.execute("SELECT DISTINCT symbol FROM execution_record ORDER BY symbol ASC").fetchall()
        return tuple(row["symbol"] for row in rows)

    def close(self) -> None:
        self._connection.close()
