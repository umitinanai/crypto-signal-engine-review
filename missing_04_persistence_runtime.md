<!-- missing_04_persistence_runtime.md — 14 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/persistence/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/persistence/errors.py (1569 bytes) -->
<!--   - crypto_signal_engine/persistence/paper_state_store.py (19592 bytes) -->
<!--   - crypto_signal_engine/persistence/recovery.py (28204 bytes) -->
<!--   - crypto_signal_engine/persistence/serialization.py (6022 bytes) -->
<!--   - crypto_signal_engine/persistence/sqlite_store.py (10683 bytes) -->
<!--   - crypto_signal_engine/runtime/__init__.py (642 bytes) -->
<!--   - crypto_signal_engine/runtime/bootstrap.py (2709 bytes) -->
<!--   - crypto_signal_engine/runtime/candle_window.py (4246 bytes) -->
<!--   - crypto_signal_engine/runtime/coordinator.py (41906 bytes) -->
<!--   - crypto_signal_engine/runtime/errors.py (2248 bytes) -->
<!--   - crypto_signal_engine/runtime/health.py (9599 bytes) -->
<!--   - crypto_signal_engine/runtime/models.py (5905 bytes) -->
<!--   - crypto_signal_engine/runtime/reselection_scheduler.py (8846 bytes) -->

=== FILE: crypto_signal_engine/persistence/__init__.py ===


=== FILE: crypto_signal_engine/persistence/errors.py ===
"""
Faz 7 (Persistence & Recovery) exception taxonomy.

Kural: sığ ve yararlı. Mevcut `crypto_signal_engine.errors.PersistenceError`
(Faz 2'de tanımlı — "SQLite/persistence katmanında bir hata") TEKRAR
TANIMLANMAZ; bu iki yeni tip, Faz 7'nin GERÇEKTEN farklı davranış
gerektiren iki durumunu ayırt eder: sürüm uyuşmazlığı (migration YOK,
sessiz geçiş YOK) ve bozuk/okunamaz kayıt (sessizce atlamak idempotency/
PnL koruma garantisini bozar — bkz. modül docstring'leri).
"""

from __future__ import annotations

from crypto_signal_engine.errors import PersistenceError


class SchemaVersionMismatchError(PersistenceError):
    """Durable store'un `schema_version`'ı, bu kod sürümünün beklediği
    `SCHEMA_VERSION` ile UYUŞMUYOR.

    Sessiz bir migration veya "en iyi çaba" okuma YAPILMAZ — bu, "eski
    formatı yanlış yorumlayıp PnL/idempotency state'ini bozma" riskini
    fiziksel olarak imkansız kılar (bkz. PHASE7_PERSISTENCE_RECOVERY.md
    — "corruption/failure behavior")."""


class CorruptRecordError(PersistenceError):
    """Durable store'daki bir kayıt (JSON blob, enum değeri, sayısal alan)
    beklenen şemaya UYMUYOR veya deserialize EDİLEMİYOR.

    Bu kayıt SESSİZCE ATLANMAZ — bir `processed_context`/`paper_position`
    kaydını sessizce yok saymak, idempotency/PnL koruma garantisini
    doğrudan ihlal eder (aynı context_id'nin "hiç işlenmemiş" gibi tekrar
    kabul edilmesine yol açabilir). Recovery, bu hatayı AÇIKÇA yukarı
    fırlatır; runtime NOT-READY/DEGRADED kalır."""


=== FILE: crypto_signal_engine/persistence/paper_state_store.py ===
"""
Faz 7 — `PaperStateStore`: Faz 5 (paper trading) + Faz 6 (runtime candle
checkpoint) durable state'i için SQLite-backed store.

Kural (Bölüm 12 — Faz 2 `sqlite_store.py` ile AYNI disiplin):
- parameterized SQL (hiçbir yerde string interpolation ile SQL kurulmaz)
- explicit schema creation + deterministic `SCHEMA_VERSION`
- transaction boundaries (`with self._connection:`) — sembol başına
  pozisyon + processed_context + fill + order yazımı TEK bir transaction'da
- no global hidden connection (her instance kendi connection'ını sahiplenir)
- sürüm uyuşmazlığı/bozuk kayıt SESSİZCE yutulmaz (`SchemaVersionMismatchError`/
  `CorruptRecordError`, bkz. errors.py)

Bu dosya `crypto_signal_engine/persistence/sqlite_store.py`'nin (Faz 2,
candle canonical state) YANINDA, AYRI bir concern için AYRI bir SQLite
veritabanıdır — Faz 2'nin candle store'unu DEĞİŞTİRMEZ/paylaşmaz.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.models import (
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError
from crypto_signal_engine.persistence.serialization import (
    dict_to_fill,
    dict_to_order,
    dict_to_position,
    dict_to_signal,
    fill_to_dict,
    order_to_dict,
    position_to_dict,
    signal_to_dict,
)

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_position (
    symbol TEXT PRIMARY KEY,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    average_entry_price REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL,
    entry_fee REAL NOT NULL,
    last_signal_timestamp TEXT
);

CREATE TABLE IF NOT EXISTS processed_context (
    symbol TEXT NOT NULL,
    context_id TEXT NOT NULL,
    signal_json TEXT NOT NULL,
    position_json TEXT NOT NULL,
    PRIMARY KEY (symbol, context_id)
);

CREATE TABLE IF NOT EXISTS paper_fill (
    fill_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    fill_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_order (
    order_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    order_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candle_checkpoint (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    last_open_time TEXT NOT NULL,
    PRIMARY KEY (symbol, timeframe)
);
"""


@dataclass(frozen=True)
class PaperStateSnapshot:
    """Bir sembol için durable store'dan geri okunmuş TAM Faz 5 state'i —
    `recovery.py::restore_paper_engine`'in `PaperTradingEngine._states`'i
    yeniden inşa etmek için ihtiyaç duyduğu her şey."""

    position: PaperPosition
    entry_fee: float
    last_signal_timestamp: datetime | None
    processed_context_ids: dict[str, tuple[Signal, PaperTradingResult]]
    fills: tuple[PaperFill, ...]
    orders: tuple[PaperOrder, ...]


class PaperStateStore:
    """Faz 5 paper-trading state'i ve Faz 6 candle checkpoint'leri için
    durable, injectable SQLite store. `db_path` HER ZAMAN çağıran tarafından
    enjekte edilir — hiçbir geliştirici-makinesi path'i hardcode EDİLMEZ."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
                    return
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

        existing = self._connection.execute("SELECT version FROM schema_version").fetchone()[0]
        if existing != SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                f"Durable store schema_version={existing}, bu kod SCHEMA_VERSION={SCHEMA_VERSION} "
                f"bekliyor — sessiz migration YAPILMAZ ({self._db_path})"
            )

    # -- Yazma (checkpoint) ---------------------------------------------------

    def _write_candle_checkpoint(self, symbol: str, timeframe: Timeframe, open_time: datetime) -> None:
        """AÇIK bir SQLite transaction İÇİNDE (çağıranın `with self._connection:`
        bloğu) tek bir candle_checkpoint upsert'i yazar — KENDİ transaction'ını
        AÇMAZ/KAPATMAZ (bkz. `checkpoint_candle`/`checkpoint_candle_transition`,
        bu ikisi TEK gerçek çağrı noktasıdır, aynı SQL'i AYRI/kombine
        transaction'larda yeniden kullanmak için)."""
        self._connection.execute(
            "INSERT INTO candle_checkpoint (symbol, timeframe, last_open_time) VALUES (?, ?, ?) "
            "ON CONFLICT(symbol, timeframe) DO UPDATE SET last_open_time = excluded.last_open_time",
            (symbol, timeframe.value, open_time.isoformat()),
        )

    def _write_paper_transition(
        self,
        symbol: str,
        position: PaperPosition,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """AÇIK bir SQLite transaction İÇİNDE (bkz. `_write_candle_checkpoint`
        yorumu — AYNI desen) bir Paper transition'ın TÜM durable
        sonuçlarını (pozisyon upsert + yeni processed_context + yeni
        fill/order kayıtları) yazar."""
        self._connection.execute(
            """
            INSERT INTO paper_position
                (symbol, side, quantity, average_entry_price, realized_pnl,
                 updated_at, entry_fee, last_signal_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                side = excluded.side,
                quantity = excluded.quantity,
                average_entry_price = excluded.average_entry_price,
                realized_pnl = excluded.realized_pnl,
                updated_at = excluded.updated_at,
                entry_fee = excluded.entry_fee,
                last_signal_timestamp = excluded.last_signal_timestamp
            """,
            (
                symbol, position.side.value, position.quantity, position.average_entry_price,
                position.realized_pnl, position.updated_at.isoformat(), entry_fee,
                last_signal_timestamp.isoformat() if last_signal_timestamp is not None else None,
            ),
        )
        for context_id, signal, ctx_position in new_context_entries:
            self._connection.execute(
                "INSERT OR REPLACE INTO processed_context (symbol, context_id, signal_json, position_json) "
                "VALUES (?, ?, ?, ?)",
                (
                    symbol, context_id,
                    json.dumps(signal_to_dict(signal)), json.dumps(position_to_dict(ctx_position)),
                ),
            )
        for fill in new_fills:
            self._connection.execute(
                "INSERT OR REPLACE INTO paper_fill (fill_id, symbol, fill_json) VALUES (?, ?, ?)",
                (fill.fill_id, symbol, json.dumps(fill_to_dict(fill))),
            )
        for order in new_orders:
            self._connection.execute(
                "INSERT OR REPLACE INTO paper_order (order_id, symbol, order_json) VALUES (?, ?, ?)",
                (order.order_id, symbol, json.dumps(order_to_dict(order))),
            )

    def checkpoint_paper_state(
        self,
        *,
        symbol: str,
        position: PaperPosition,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """Bir sembolün TAM güncel Faz 5 durumunu TEK bir transaction'da
        durable hâle getirir: pozisyon (upsert) + yeni processed_context
        kayıtları + yeni fill/order kayıtları. Ya HEPSİ ya HİÇBİRİ —
        kısmi bir checkpoint asla başarılı görünmez (Bölüm — atomicity).

        Faz 7 pre-audit remediation NOTU: `PersistedRuntime` bu metodu
        ARTIK canlı ingestion'dan DOĞRUDAN çağırmaz (bkz.
        `checkpoint_candle_transition` — candle checkpoint'iyle AYNI
        transaction'da, HIGH-1 fix). Bu metod, Faz 5 seviyesinde izole
        test/entegrasyon senaryoları (bkz. `tests/test_persistence_recovery.py::
        _checkpoint_from_result`) ve doğrudan store kullanımı için PUBLIC
        API olarak KORUNUR — davranışı DEĞİŞMEDİ."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_paper_transition(
                    normalized, position, entry_fee, last_signal_timestamp,
                    new_context_entries, new_fills, new_orders,
                )
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper state checkpoint başarısız ({normalized}): {exc}") from exc

    def checkpoint_candle(self, symbol: str, timeframe: Timeframe, open_time: datetime) -> None:
        """Bir (symbol, timeframe) için en son KABUL EDİLMİŞ candle'ın
        `open_time`'ını durable hâle getirir. Yalnızca `RuntimeCoordinator.
        ingest_candle()` ZATEN `ACCEPTED` (kesin olarak monoton artan)
        döndürdüğünde çağrılır — bu store KENDİSİ hiçbir sıralama/gap
        kontrolü YAPMAZ (bu, Faz 6'nın `CandleWindow`'unun sorumluluğudur;
        burada TEKRARLANMAZ). `PersistedRuntime` bu metodu YALNIZCA o
        candle'ın hiçbir durable Paper transition ÜRETMEDİĞİ (NEUTRAL/
        idempotent replay/aynı-yön no-churn) durumda kullanır — bir
        transition VARSA `checkpoint_candle_transition` kullanılır."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_candle_checkpoint(normalized, timeframe, open_time)
        except sqlite3.Error as exc:
            raise PersistenceError(f"candle checkpoint başarısız ({normalized}/{timeframe.value}): {exc}") from exc

    def checkpoint_candle_transition(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        candle_open_time: datetime,
        position: PaperPosition | None,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """Faz 7 pre-audit remediation (HIGH-1 fix): bir kabul edilmiş
        candle'ın checkpoint'ini VE (varsa) o candle'ın ÜRETTİĞİ Paper
        transition'ı TEK bir SQLite transaction'da durable hâle getirir —
        ya HEPSİ ya HİÇBİRİ. Önceki tasarımda bu ikisi AYRI transaction'lar
        (`checkpoint_candle()` + `checkpoint_paper_state()`) İDİ — bir
        crash/write-failure ikisinin ARASINA düşerse, candle checkpoint'i
        durable olarak İLERLEYEBİLİYORDU ama ilişkili Paper transition'ı
        HİÇ yazılmamış olabiliyordu (bkz. DECISIONS.md, HIGH-1). ARTIK bu
        durum YAPISAL OLARAK İMKANSIZDIR: her ikisi de AYNI `with
        self._connection:` bloğu içinde yazılır.

        `position` `None` ise (candle kabul edildi ama hiçbir durable Paper
        transition ÜRETMEDİ) YALNIZCA candle checkpoint'i yazılır —
        `PersistedRuntime` bu durumda zaten bu metodu ÇAĞIRMAZ (bkz.
        `checkpoint_candle`), ama bu metod da tek başına çağrılırsa AYNI
        güvenli davranışı sağlar (gereksiz boş paper-state yazımı YAPMAZ)."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_candle_checkpoint(normalized, timeframe, candle_open_time)
                if position is not None:
                    self._write_paper_transition(
                        normalized, position, entry_fee, last_signal_timestamp,
                        new_context_entries, new_fills, new_orders,
                    )
        except sqlite3.Error as exc:
            raise PersistenceError(
                f"candle+paper transition checkpoint başarısız ({normalized}/{timeframe.value}): {exc}"
            ) from exc

    # -- Okuma (recovery) ---------------------------------------------------

    def load_paper_state(self, symbol: str) -> PaperStateSnapshot | None:
        """Bir sembol için durable store'da HİÇ kayıt yoksa `None` döner
        (bu sembol için bootstrap'tan başlanmalı — Faz 7 tarafından SAHTE
        bir state ÜRETİLMEZ). Bozuk bir kayıt bulunursa `CorruptRecordError`
        AÇIKÇA fırlatılır — SESSİZCE atlanmaz."""
        normalized = normalize_symbol(symbol)
        try:
            position_row = self._connection.execute(
                "SELECT side, quantity, average_entry_price, realized_pnl, updated_at, "
                "entry_fee, last_signal_timestamp FROM paper_position WHERE symbol = ?",
                (normalized,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_position okunamadı ({normalized}): {exc}") from exc

        if position_row is None:
            return None

        try:
            side_str, quantity, avg_entry, realized_pnl, updated_at_str, entry_fee, last_signal_str = position_row
            position = PaperPosition(
                symbol=normalized, side=PositionSide(side_str), quantity=quantity,
                average_entry_price=avg_entry, realized_pnl=realized_pnl,
                updated_at=datetime.fromisoformat(updated_at_str),
            )
            last_signal_timestamp = datetime.fromisoformat(last_signal_str) if last_signal_str else None
        except (ValueError, TypeError, KeyError) as exc:
            raise CorruptRecordError(f"paper_position kaydı bozuk ({normalized}): {exc}") from exc

        processed_context_ids: dict[str, tuple[Signal, PaperTradingResult]] = {}
        try:
            context_rows = self._connection.execute(
                "SELECT context_id, signal_json, position_json FROM processed_context WHERE symbol = ?",
                (normalized,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"processed_context okunamadı ({normalized}): {exc}") from exc

        for context_id, signal_json, position_json in context_rows:
            try:
                signal = dict_to_signal(json.loads(signal_json))
                ctx_position = dict_to_position(json.loads(position_json))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(
                    f"processed_context kaydı bozuk ({normalized}/{context_id}): {exc}"
                ) from exc
            result = PaperTradingResult(
                symbol=normalized, position=ctx_position, orders=(), fills=(), idempotent_replay=False,
            )
            processed_context_ids[context_id] = (signal, result)

        fills: list[PaperFill] = []
        try:
            fill_rows = self._connection.execute(
                "SELECT fill_json FROM paper_fill WHERE symbol = ?", (normalized,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_fill okunamadı ({normalized}): {exc}") from exc
        for (fill_json,) in fill_rows:
            try:
                fills.append(dict_to_fill(json.loads(fill_json)))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(f"paper_fill kaydı bozuk ({normalized}): {exc}") from exc
        fills.sort(key=lambda f: f.filled_at)

        orders: list[PaperOrder] = []
        try:
            order_rows = self._connection.execute(
                "SELECT order_json FROM paper_order WHERE symbol = ?", (normalized,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_order okunamadı ({normalized}): {exc}") from exc
        for (order_json,) in order_rows:
            try:
                orders.append(dict_to_order(json.loads(order_json)))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(f"paper_order kaydı bozuk ({normalized}): {exc}") from exc
        orders.sort(key=lambda o: o.created_at)

        return PaperStateSnapshot(
            position=position, entry_fee=entry_fee, last_signal_timestamp=last_signal_timestamp,
            processed_context_ids=processed_context_ids, fills=tuple(fills), orders=tuple(orders),
        )

    def list_open_position_symbols(self) -> tuple[str, ...]:
        """Durable `paper_position` tablosunda `side != 'FLAT'` olan (yani
        GERÇEKTEN açık) tüm sembolleri, deterministik (alfabetik) sırayla
        döner. Otomatik sembol seçiminin "açık pozisyonlu bir sembol ASLA
        izlemenin dışına düşmemeli" güvenlik kuralı (bkz. `app.py::
        Application.__init__` — pinning) İÇİN eklenmiştir; salt-okunur,
        hiçbir state MUTATE ETMEZ."""
        try:
            rows = self._connection.execute(
                "SELECT symbol FROM paper_position WHERE side != ? ORDER BY symbol", (PositionSide.FLAT.value,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_position (open positions) okunamadı: {exc}") from exc
        return tuple(row[0] for row in rows)

    def load_candle_checkpoint(self, symbol: str, timeframe: Timeframe) -> datetime | None:
        normalized = normalize_symbol(symbol)
        try:
            row = self._connection.execute(
                "SELECT last_open_time FROM candle_checkpoint WHERE symbol = ? AND timeframe = ?",
                (normalized, timeframe.value),
            ).fetchone()
        except sqlite3.Error as exc:
            raise PersistenceError(f"candle_checkpoint okunamadı ({normalized}/{timeframe.value}): {exc}") from exc
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row[0])
        except (ValueError, TypeError) as exc:
            raise CorruptRecordError(
                f"candle_checkpoint kaydı bozuk ({normalized}/{timeframe.value}): {exc}"
            ) from exc

    def close(self) -> None:
        self._connection.close()


=== FILE: crypto_signal_engine/persistence/recovery.py ===
"""
Faz 7 — recovery.py: process-restart sonrası Faz 5 (paper trading) ve
Faz 6 (runtime candle continuity) state'ini durable store'dan geri yükleyen
AÇIK bir recovery boundary, ve normal çalışma sırasında (her kabul edilmiş
mutation'dan HEMEN sonra) checkpoint yazan `PersistedRuntime` wrapper'ı.

MİMARİ KURAL ("persistence bir SINIRDIR, domain nesnelerini
database-aware YAPMAZ"): Bu modül `PaperTradingEngine`/`RuntimeCoordinator`
sınıflarını DEĞİŞTİRMEZ — onları SARAR (wrap). `PaperTradingEngine`'in
KASITLI OLARAK sunmadığı tek şey (bir "restore/seed state" API'si —
Faz 5'in kendi kabul edilmiş sözleşmesi, TÜM mutation'ların YALNIZCA
`process_signal()` üzerinden geçmesini zorunlu kılar) için, bu dosyada
TEK ve AÇIKÇA dokümante edilmiş bir istisna yapılır: `restore_paper_engine()`
doğrudan `PaperTradingEngine`'in private `_states` sözlüğüne yazar. Bunun
DIŞINDA hiçbir yerde Faz 5/6'nın private state'ine YAZILMAZ.

NEDEN `process_signal()` ÜZERİNDEN "REPLAY" EDİLEMEZ: Restart sonrası
geçmiş sinyalleri `process_signal()`'a yeniden vermek, onları "yeni" gibi
işlemeye ÇALIŞMAK anlamına gelir — ki bu TAM OLARAK önlenmesi gereken
şeydir (duplicate paper action/PnL). Bu yüzden restore, mutation
YOLUNDAN GEÇMEDEN, doğrudan state seed eder.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine, _SymbolState
from crypto_signal_engine.paper_trading.models import PaperPosition
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome, ProcessedMarketEvent, RuntimeCycleResult

# `coordinator.py::_TIMEFRAME_DURATIONS`'ın KASITLI, KÜÇÜK bir kopyası —
# bu sabit, iş mantığı DEĞİLDİR (yalnızca Timeframe -> timedelta lookup),
# bu yüzden coordinator'ın private isim alanına erişmek yerine burada
# bağımsız olarak tanımlanması tercih edilmiştir (bkz. modül docstring'i —
# "persistence bir sınırdır", coordinator'ın iç mimarisine sıkı sıkıya
# bağlanmaz).
_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}


def restore_paper_engine(
    store: PaperStateStore, engine: PaperTradingEngine, symbols: Iterable[str]
) -> dict[str, bool]:
    """Her sembol için durable store'da state VARSA, `PaperTradingEngine`'in
    private `_states` sözlüğüne DOĞRUDAN yazarak geri yükler (bkz. modül
    docstring'i — bu, dokümante edilmiş TEK istisnadır). State YOKSA
    (sembol daha önce hiç işlem görmemiş), o sembol için HİÇBİR ŞEY
    YAPILMAZ — sahte bir başlangıç state'i ÜRETİLMEZ.

    Döner: `{symbol: bool}` — her sembol için state geri yüklenip
    yüklenmediği (testler ve raporlama için)."""
    restored: dict[str, bool] = {}
    for symbol in symbols:
        normalized = normalize_symbol(symbol)
        snapshot = store.load_paper_state(normalized)
        if snapshot is None:
            restored[normalized] = False
            continue
        engine._states[normalized] = _SymbolState(
            position=snapshot.position,
            entry_fee=snapshot.entry_fee,
            orders=snapshot.orders,
            fills=snapshot.fills,
            processed_context_ids=dict(snapshot.processed_context_ids),
            last_signal_timestamp=snapshot.last_signal_timestamp,
        )
        restored[normalized] = True
    return restored


class PersistedRuntime:
    """`RuntimeCoordinator`'ı (Faz 6, DEĞİŞTİRİLMEDEN) SARAN, kabul edilmiş
    her state-değiştirici mutation'dan HEMEN SONRA durable checkpoint yazan
    bir Faz 7 wrapper'ı.

    Checkpoint yazma politikası (Bölüm — "persistence write policy"):
    - Yeni bir candle `ACCEPTED` olduğunda -> `checkpoint_candle()`.
    - Bir sinyal değerlendirmesi GERÇEK bir mutation ürettiğinde
      (`evaluated=True`, idempotent replay DEĞİL, NEUTRAL DEĞİL) ->
      `checkpoint_paper_state()`.
    - NEUTRAL (NO_ACTION), idempotent replay, ve aynı-yön tekrarının
      ürettiği "hiçbir yeni fill/order yok" durumları İÇİN GEREKSİZ YAZMA
      YAPILMAZ (Faz 5'in `same-direction no churn`/`NEUTRAL NO_ACTION`
      davranışı BOZULMAZ) — bkz. `_is_durable_transition`.

    Checkpoint yazımı BAŞARISIZ olursa (`PersistenceError`): in-memory
    Faz 5/6 state'i GERİ ALINMAZ (Faz 5 yeniden tasarlanmadan bu mümkün
    değildir) — ilgili sembol `RuntimeCoordinator.mark_persistence_fault()`
    ile AÇIKÇA DEGRADED işaretlenir (fail-closed, "pretend atomicity"
    YOKTUR)."""

    def __init__(self, coordinator: RuntimeCoordinator, store: PaperStateStore) -> None:
        self._coordinator = coordinator
        self._store = store
        # SINGLE FINAL ACCEPTANCE BLOCKER fix (bkz. DECISIONS.md): bir
        # (symbol, timeframe) için BAŞARISIZ olan `checkpoint_candle_transition`
        # yazımı, o payload'ı BURADA (in-memory, FIFO) bir "pending-unsynced"
        # backlog'a EKLER — atılmaz, GÖRMEZDEN GELİNMEZ. Aynı (symbol,
        # timeframe) için HER sonraki checkpoint denemesi (yeni bir candle
        # kabul edildiğinde), ÖNCE bu backlog'u BAŞTAN (en eski/başarısız
        # olan ÖNCE) SIRAYLA flush etmeye ÇALIŞIR — kendi (daha yeni)
        # payload'ını backlog'un SONUNA ekleyerek. Bir öğe flush BAŞARISIZ
        # olursa, o öğeden SONRAKİ HİÇBİR öğe (yeni gelen dahil) yazılmaz;
        # persistence-fault AÇIK kalır. Backlog TAMAMEN boşaldığında
        # (TÜM bekleyen transition'lar durable hâle geldiğinde) fault
        # temizlenir. Böylece daha SONRAKİ bir candle'ın checkpoint'i, HÂLÂ
        # eksik olan DAHA ESKİ bir failed transition'ı asla "es geçip"
        # sessizce durable görünüme SAHİP OLAMAZ (bkz. `_flush_pending`).
        #
        # Bu backlog KASITLI OLARAK SADECE in-memory'dir (durable hâle
        # GETİRİLMEZ) — bir process crash'i sırasında hâlâ boşaltılmamış bir
        # backlog kaybolur (Faz 7'nin ZATEN kabul edilmiş fail-closed
        # tasarımıyla AYNI sınır: in-memory state hiçbir zaman durable
        # store'a geri YAZILMADAN "rollback" edilmez). Bu, GENEL bir 2PC
        # DEĞİLDİR: yalnızca ZATEN hesaplanmış (Signal YENİDEN
        # DEĞERLENDİRİLMEDEN) bir SQL yazımının retry'ıdır.
        self._pending: dict[tuple[str, Timeframe], list[dict[str, object]]] = {}

        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol task
        # ownership, additive to the flat task list `run()` extends into
        # `coordinator._tasks` (full-shutdown cancellation via
        # `coordinator.stop()` is completely unchanged: it still just
        # iterates `coordinator._tasks`). This dict exists so `add_symbol`/
        # `remove_symbol` (below) can create/cancel EXACTLY one symbol's
        # OWN checkpoint-writing consumer tasks, mirroring the identical
        # `self._tasks`/`self._tasks_by_symbol` pattern already accepted in
        # `RuntimeCoordinator` — this is ONLY ever the actual task owner
        # when `PersistedRuntime` is itself the top-level composed runtime
        # (bridge disabled); when wrapped by `BridgeRuntime`, that class
        # owns task spawning instead (see its own module docstring) and
        # this dict simply stays empty.
        self._tasks_by_symbol: dict[str, list[asyncio.Task]] = {}

    # -- Recovery (AÇIK, tek seferlik restart boundary) ------------------------

    async def recover(self) -> tuple[BootstrapReport, ...]:
        """Restart sonrası recovery sırası:

        1. Faz 5 paper state'ini (`restore_paper_engine`) geri yükler.
        2. Her (symbol, timeframe) için, durable bir candle checkpoint'i
           VARSA veya YOKSA FARK ETMEKSİZİN, AYNI warmup-lookback penceresi
           (`_TIMEFRAME_DURATIONS[timeframe] * warmup_candles * 2` — Faz
           6'nın kendi `bootstrap()`'ıyla AYNI, mevcut/gerçek runtime
           gereksinimi, YENİ bir sabit İCAT EDİLMEDİ) kullanılır:
           - checkpoint YOKSA: `[now - lookback, now)`.
           - checkpoint VARSA: `[checkpoint - lookback, now)` — checkpoint
             candle'ının KENDİSİ bu aralığa HER ZAMAN dahildir (`checkpoint
             - lookback < checkpoint < now`), böylece hem `CandleWindow`
             doğru şekilde yeniden "çapalanır" hem de FULL warmup GERÇEKTEN
             yeniden inşa edilir (BLOCKER FİX — bkz. DECISIONS.md Karar 68:
             eski dar `[checkpoint, now)` penceresi, checkpoint "now"a
             yakınken warmup'ı yeniden inşa etmeye YETMİYORDU, restart'ı
             gereksiz yere BOOTSTRAPPING'de bırakıyordu).
        3. Fetch edilen candle'lar `coordinator.bootstrap_candles()`'a
           (Faz 6, DEĞİŞTİRİLMEDEN) uygulanır — dedup/kronolojik
           sıralama/gap tespiti ZATEN orada yapılır, burada TEKRARLANMAZ.
           PUBLIC REST'in GERÇEKTEN dönebildiğinden FAZLASI asla
           FABRİKE EDİLMEZ — yetersiz geçmiş, dürüstçe `ready=False`
           (BOOTSTRAPPING) ile sonuçlanır.
        4. READY, Faz 6'nın KENDİ `_maybe_mark_symbol_ready()` kuralı
           (TÜM candle timeframe'leri + en az bir order-book feature
           snapshot'ı) tekrar GERÇEKTEN sağlandığında, doğal olarak
           `bootstrap_candles()` üzerinden oluşur — burada AYRICA/ERKEN
           bir READY işaretlemesi YAPILMAZ.

        Bu fazda GEÇMİŞTE (offline iken) kapanmış hiçbir M5 candle için
        YENİDEN bir sinyal değerlendirmesi/paper trade TETİKLENMEZ —
        yalnızca state (candle window + feature history) yeniden inşa
        edilir (`bootstrap_candles()` ASLA `SignalEngine.evaluate()`
        çağırmaz); restart sonrası İLK canlı M5 kapanışı normal Faz 6
        sınırı üzerinden bir sonraki değerlendirmeyi tetikler (bkz.
        PHASE7_PERSISTENCE_RECOVERY.md — "restart re-evaluation policy")."""
        restore_paper_engine(self._store, self._coordinator._paper_engine, self._coordinator._symbols)

        reports: list[BootstrapReport] = []
        now = self._coordinator._clock.now()
        for symbol in self._coordinator._symbols:
            for timeframe in self._coordinator._candle_timeframes:
                checkpoint = self._store.load_candle_checkpoint(symbol, timeframe)
                lookback = _TIMEFRAME_DURATIONS[timeframe] * self._coordinator._warmup_candles * 2
                anchor = now if checkpoint is None else checkpoint
                start = anchor - lookback
                candles = await self._coordinator._provider.fetch_historical_candles(symbol, timeframe, start, now)
                reports.append(self._coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=now))
        return tuple(reports)

    # -- Canlı ingestion (checkpoint'li) ---------------------------------------

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> ProcessedMarketEvent:
        event = self._coordinator.ingest_candle(symbol, timeframe, candle)
        if event.outcome is IngestOutcome.ACCEPTED:
            durable = event.cycle_result if event.cycle_result is not None and self._is_durable_transition(
                event.cycle_result
            ) else None
            self._checkpoint_candle_transition(symbol, timeframe, candle, durable)
        return event

    async def resolve_gap(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> ProcessedMarketEvent:
        """BLOCKER FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        eski davranış, `_consume_candles`'ın bir gap tespit ettiğinde
        DOĞRUDAN `self._coordinator.resolve_gap(...)`'i (bare coordinator)
        çağırmasıydı — bu, backfill'in SONUNDA bare coordinator'ın KENDİ
        (checkpoint YAZMAYAN) `ingest_candle()`'ını çağırıyordu. Sonuç:
        gap resolution'ın ürettiği bir Paper transition in-memory state'i
        mutate EDEBİLİYORDU ama HİÇBİR ZAMAN durable hâle GELMİYORDU (yalnızca
        candle checkpoint'i ilerliyordu) — restart sonrası bu trade KALICI
        olarak kayboluyordu (bootstrap tarihi sinyalleri asla yeniden
        DEĞERLENDİRMEZ, bkz. `recover()` docstring'i).

        Fix: `RuntimeCoordinator.resolve_gap_backfill()`'i (backfill adımı,
        HİÇBİR gap-tespit/REST-fetch mantığı burada TEKRARLANMADAN) yeniden
        kullanır, ardından `pending_candle`'ı bare coordinator yerine BU
        wrapper'ın KENDİ `ingest_candle()`'ı (candle+paper transition'ı TEK
        atomik transaction'da checkpoint'leyen, bkz. HIGH-1 fix) üzerinden
        ingest eder — gap resolution'ın ürettiği bir Paper transition,
        normal kabul edilmiş bir M5 transition ile TAMAMEN AYNI durable
        checkpoint garantisini alır."""
        await self._coordinator.resolve_gap_backfill(symbol, timeframe, pending_candle)
        normalized = normalize_symbol(symbol)
        return self.ingest_candle(normalized, timeframe, pending_candle)

    def ingest_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> ProcessedMarketEvent:
        # Order-book event'leri hiçbir durable checkpoint TETİKLEMEZ — M1
        # order-book state'i, Faz 6'nın kendi tasarımıyla zaten "en son
        # snapshot" bazlıdır ve restart sonrası yeniden canlı akıştan
        # DOĞAL olarak yeniden elde edilir (bkz. PHASE7 "rebuildable state").
        return self._coordinator.ingest_order_book(symbol, snapshot)

    def bootstrap_candles(self, symbol: str, timeframe: Timeframe, candles: list[Candle], as_of) -> BootstrapReport:
        return self._coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    def status(self):
        return self._coordinator.status()

    def _is_durable_transition(self, cycle_result: RuntimeCycleResult) -> bool:
        if not cycle_result.evaluated:
            return False
        if cycle_result.paper_result.idempotent_replay:
            return False
        if cycle_result.signal.direction is SignalDirection.NEUTRAL:
            return False
        return True

    def _checkpoint_candle_transition(
        self, symbol: str, timeframe: Timeframe, candle: Candle, cycle_result: RuntimeCycleResult | None
    ) -> None:
        """HIGH-1 FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        candle checkpoint'i VE (varsa, `cycle_result is not None`) o
        candle'ın ürettiği Paper transition'ı `PaperStateStore.
        checkpoint_candle_transition()` üzerinden TEK bir atomik SQLite
        transaction'da yazar — eski tasarımda bu ikisi (`checkpoint_candle`
        + `checkpoint_paper_state`) İKİ AYRI transaction'dı; bir crash/write
        failure aralarına düşerse candle checkpoint'i durable ilerlerken
        ilişkili Paper transition'ı HİÇ yazılmamış olabiliyordu. ARTIK bu
        yapısal olarak İMKANSIZDIR: ya İKİSİ DE durable olur ya da HİÇBİRİ.

        Health fault semantiği de bu atomicity'yi DOĞRU yansıtır: yazım
        BAŞARISIZ olursa, `cycle_result` mevcutsa HEM "candle_checkpoint"
        HEM "paper_state_checkpoint" reason'ı DEGRADED işaretlenir (ikisi
        de GERÇEKTEN aynı anda başarısız oldu — kısmi başarı YOKTUR);
        `cycle_result` yoksa yalnızca "candle_checkpoint" işaretlenir
        (yazılacak hiçbir Paper transition zaten YOKTU). Aynı simetri
        başarı/clear yolunda da geçerlidir.

        SINGLE FINAL ACCEPTANCE BLOCKER fix: bu payload doğrudan store'a
        YAZILMAZ — `self._pending` backlog'una (bkz. `__init__` yorumu)
        EKLENİR, ardından `_flush_pending()` çağrılır. Bu, bir ÖNCEKİ
        (aynı symbol/timeframe için) BAŞARISIZ kalmış transition'ın, bu
        DAHA YENİ candle'ın checkpoint'i tarafından ASLA "es geçilip"
        (overtake) sessizce durable görünüme sahip OLAMAYACAĞINI yapısal
        olarak garanti eder — flush her zaman backlog'un EN BAŞINDAN,
        sırayla dener (bkz. `_flush_pending`)."""
        normalized = normalize_symbol(symbol)
        payload = self._build_transition_payload(normalized, candle, cycle_result)
        key = (normalized, timeframe)
        self._pending.setdefault(key, []).append(payload)
        self._flush_pending(normalized, timeframe)

    def _build_transition_payload(
        self, symbol: str, candle: Candle, cycle_result: RuntimeCycleResult | None
    ) -> dict[str, object]:
        """Bir candle+ (varsa) Paper transition'ının, `PaperStateStore.
        checkpoint_candle_transition()`'a AYNEN geçirilebilecek durable
        yazım payload'ını inşa eder — HİÇBİR SQL çağrısı yapmaz (bkz.
        `_checkpoint_candle_transition`/`_flush_pending`, gerçek çağrı
        buradan AYRIŞTIRILMIŞTIR ki AYNI payload, ilk denemede VE
        sonraki bir retry'da BİREBİR AYNI şekilde yeniden kullanılabilsin
        — Signal YENİDEN DEĞERLENDİRİLMEZ, yalnızca ZATEN hesaplanmış
        sonuç yeniden yazılmaya ÇALIŞILIR)."""
        position: PaperPosition | None = None
        entry_fee = 0.0
        last_signal_timestamp = None
        new_context_entries: list[tuple[str, object, PaperPosition]] = []
        new_fills: tuple = ()
        new_orders: tuple = ()

        if cycle_result is not None:
            # `entry_fee`/`last_signal_timestamp`, Faz 5'in PUBLIC yüzeyinde
            # YOKTUR (Faz 5'e yeni bir public API eklemek "redesign" olurdu)
            # — bu iki skaler alan için, dokümante edilmiş minimal bir
            # salt-okunur private erişim yapılır (bkz. modül docstring'i).
            state = self._coordinator._paper_engine._states.get(symbol)
            entry_fee = state.entry_fee if state is not None else 0.0
            last_signal_timestamp = state.last_signal_timestamp if state is not None else None
            position = cycle_result.paper_result.position
            new_context_entries = [
                (cycle_result.signal.context_id, cycle_result.signal, cycle_result.paper_result.position)
            ]
            new_fills = cycle_result.paper_result.fills
            new_orders = cycle_result.paper_result.orders

        return {
            "candle_open_time": candle.open_time,
            "position": position,
            "entry_fee": entry_fee,
            "last_signal_timestamp": last_signal_timestamp,
            "new_context_entries": new_context_entries,
            "new_fills": new_fills,
            "new_orders": new_orders,
        }

    def _flush_pending(self, symbol: str, timeframe: Timeframe) -> None:
        """SINGLE FINAL ACCEPTANCE BLOCKER fix — backlog'u (bkz. `__init__`
        yorumu) baştan (en eski/ilk başarısız olan payload ÖNCE) sırayla
        flush etmeye çalışır:

        - Bir payload BAŞARIYLA yazılırsa backlog'dan ÇIKARILIR, sıradaki
          (varsa) denenir.
        - Bir payload BAŞARISIZ olursa, DÖNGÜ ORADA DURUR — o payload VE
          ondan SONRAKİ (backlog'da hâlâ bekleyen, bu ÇAĞRIYI TETİKLEYEN
          en yeni candle'ınki DAHİL) HİÇBİRİ yazılmaz; persistence-fault
          AÇIK bırakılır/YENİDEN işaretlenir.
        - Backlog TAMAMEN boşalırsa (bu (symbol, timeframe) için TÜM
          bekleyen transition'lar artık durable), fault temizlenir.

        Bu, gerekli invaryantı DOĞRUDAN uygular: health ANCAK backlog boş
        olduğunda READY'ye dönebilir — yani ANCAK eksik durable
        sonuçların HEPSİ başarıyla yazıldığında. Daha yeni bir candle'ın
        checkpoint'i, backlog'da bekleyen daha eski bir öğe flush
        edilmeden ASLA store'a ULAŞMAZ (FIFO sıra korunur)."""
        key = (symbol, timeframe)
        backlog = self._pending.get(key)
        if not backlog:
            return
        while backlog:
            payload = backlog[0]
            try:
                self._store.checkpoint_candle_transition(symbol=symbol, timeframe=timeframe, **payload)
            except PersistenceError:
                self._coordinator.mark_persistence_fault(symbol, reason="candle_checkpoint")
                if payload["position"] is not None:
                    self._coordinator.mark_persistence_fault(symbol, reason="paper_state_checkpoint")
                return
            backlog.pop(0)
        del self._pending[key]
        self._coordinator.clear_persistence_fault(symbol, reason="candle_checkpoint")
        self._coordinator.clear_persistence_fault(symbol, reason="paper_state_checkpoint")

    # -- Async run/stop (checkpoint'li run döngüsü) ------------------------

    async def run(self) -> None:
        """`RuntimeCoordinator.run()`'ın (Faz 6) KENDİSİ, iç döngüsünde
        `self.ingest_candle` (COORDINATOR'ın kendi metodu) çağırır —
        checkpoint yazan WRAPPER metodunu DEĞİL. Bu yüzden sürekli/canlı
        çalıştırma için `PersistedRuntime` KENDİ tüketim döngüsünü sağlar
        (aynı akış şekli, ama `self.ingest_candle`/`ingest_order_book`
        ÜZERİNDEN — bu ikisi checkpoint yazar). Üretilen task'lar
        `coordinator._tasks`'a EKLENİR, böylece `coordinator.stop()`
        (DEĞİŞTİRİLMEDEN) bunları da doğru şekilde iptal eder."""
        if self._coordinator._stopped:
            raise ValueError("stopped bir PersistedRuntime tekrar run() ile başlatılamaz")

        tasks: list[asyncio.Task] = []
        for symbol in self._coordinator._symbols:
            tasks.extend(self._spawn_symbol_tasks(symbol))

        await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn_symbol_tasks(self, symbol: str) -> list[asyncio.Task]:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same checkpoint-aware
        task-creation code path — never a second implementation that could
        drift. Tracks the created tasks BOTH in the flat `coordinator.
        _tasks` (so `coordinator.stop()`'s existing full-shutdown
        cancellation is completely unchanged) AND in `self._tasks_by_symbol`
        (so `remove_symbol` can cancel just this symbol's own tasks)."""
        created: list[asyncio.Task] = []
        for timeframe in self._coordinator._candle_timeframes:
            created.append(asyncio.create_task(self._consume_candles(symbol, timeframe)))
        created.append(asyncio.create_task(self._consume_order_book(symbol)))
        self._coordinator._tasks.extend(created)
        self._tasks_by_symbol[symbol] = created
        return created

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: hot-adds a symbol's
        OWN checkpoint-writing consumer tasks — ONLY meaningful when this
        `PersistedRuntime` is itself the top-level composed runtime (bridge
        disabled, `Application._runtime is persisted_runtime`). Idempotent:
        a symbol that already has active task ownership here is a no-op
        (never a duplicate subscription).

        Bootstrap/state establishment is delegated ONCE to
        `RuntimeCoordinator.add_symbol()` — that method spawns tasks itself
        only when `coordinator._running` is `True`, which is NEVER the case
        in production (production never calls `RuntimeCoordinator.run()`
        directly, see `coordinator.py`/`app.py`), so this call is
        structurally guaranteed to be state+bootstrap ONLY here — never a
        second, competing set of bare coordinator tasks."""
        normalized = normalize_symbol(symbol)
        if normalized in self._tasks_by_symbol:
            return
        await self._coordinator.add_symbol(normalized)
        if self._coordinator._stopped:
            return
        self._spawn_symbol_tasks(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cleanly cancels JUST
        this symbol's own checkpoint-writing tasks, then delegates state
        cleanup to `RuntimeCoordinator.remove_symbol()`. Idempotent:
        removing a symbol with no active task ownership here is a no-op."""
        normalized = normalize_symbol(symbol)
        tasks = self._tasks_by_symbol.pop(normalized, [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._coordinator._tasks = [t for t in self._coordinator._tasks if t not in tasks]
        await self._coordinator.remove_symbol(normalized)

    async def _consume_candles(self, symbol: str, timeframe: Timeframe) -> None:
        """Bölüm (Faz 9 soak testinde bulunan BLOCKER'ın düzeltmesi):
        `RuntimeCoordinator._consume_candles`'ın (Faz 6, DEĞİŞTİRİLMEDEN)
        `except Exception: mark_disconnected(symbol); raise` disiplini
        BURADA DA uygulanır — aksi halde GERÇEK üretim giriş noktasında
        (Faz 8 `Application` -> bu metod) bir stream kopması `gather(...,
        return_exceptions=True)` tarafından SESSİZCE yutulur ve health,
        staleness eşiği dolana KADAR yanlışlıkla READY görünmeye devam
        eder (bkz. DECISIONS.md Karar 67).

        BLOCKER-1 FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        bir gap tespit edildiğinde, eski davranış bare `self._coordinator.
        resolve_gap(...)`'i çağırıp SONRA yalnızca candle checkpoint'ini
        elle yazıyordu — gap resolution'ın ürettiği bir Paper transition
        (bare coordinator'ın ingest'i mutation ÜRETEBİLİR) hiçbir zaman
        durable hâle GELMİYORDU. ARTIK `self.resolve_gap(...)` (BU
        wrapper'ın KENDİ, checkpoint yazan metodu) çağrılır — backfill
        AYNI kalır (`RuntimeCoordinator.resolve_gap_backfill()` üzerinden,
        hiçbir mantık TEKRARLANMADAN), ama son (`pending_candle`) ingestion'ı
        `self.ingest_candle()` üzerinden geçer, dolayısıyla candle checkpoint'i
        VE (varsa) ürettiği Paper transition AYNI atomik transaction'da
        (bkz. HIGH-1 fix) checkpoint'lenir — elle ayrı bir `_checkpoint_candle`
        çağrısına ARTIK gerek YOKTUR (`self.resolve_gap` içindeki
        `self.ingest_candle` bunu ZATEN yapar)."""
        try:
            async for candle in self._coordinator._provider.stream_candles(symbol, timeframe):
                if self._coordinator._stopped:
                    break
                event = self.ingest_candle(symbol, timeframe, candle)
                if event.outcome is IngestOutcome.GAP_DETECTED:
                    await self.resolve_gap(symbol, timeframe, candle)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._coordinator.mark_disconnected(symbol)
            raise

    async def _consume_order_book(self, symbol: str) -> None:
        """Bkz. `_consume_candles` docstring'i — aynı disiplin."""
        try:
            async for snapshot in self._coordinator._provider.stream_order_book(
                symbol, self._coordinator._order_book_depth
            ):
                if self._coordinator._stopped:
                    break
                self.ingest_order_book(symbol, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._coordinator.mark_disconnected(symbol)
            raise

    async def stop(self) -> None:
        await self._coordinator.stop()
        self._tasks_by_symbol.clear()
        self._store.close()


=== FILE: crypto_signal_engine/persistence/serialization.py ===
"""
Faz 7 — durable serialization: kabul edilmiş Faz 4/5 domain nesnelerini
(Signal, AgentEvidence, PaperPosition, PaperFill, PaperOrder) JSON-safe
dict'lere ve GERİYE çevirir.

Kural (Bölüm — "Persistence is a boundary around accepted runtime/domain
state"): Bu modül, Faz 1/4/5 domain modellerini DEĞİŞTİRMEZ (hiçbir
`to_dict()`/`from_dict()` metodu domain sınıflarına EKLENMEZ) — dönüşüm
tamamen bu modülün DIŞARIDAN, saf fonksiyonlarıyla yapılır.

Deterministic: aynı domain nesnesi HER ZAMAN aynı dict'i üretir (sabit
alan sırası, ISO-8601 UTC zaman damgaları, enum `.value` string'leri).
Deserialize edilen nesneler, domain sınıflarının KENDİ `__post_init__`
validasyonundan geçer — bozuk/geçersiz veri (yanlış enum değeri, naive
datetime, sonsuz/NaN sayı, vb.) domain katmanının KENDİ hata tipleriyle
(ValueError/TypeError) reddedilir; bu modül bunları YUTMAZ, çağıran
(`paper_state_store.py`) bunları `CorruptRecordError`'a çevirir.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.paper_trading.models import (
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PositionSide,
)


def _dt_to_str(value: datetime) -> str:
    return value.isoformat()


def _str_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


# -- AgentEvidence -----------------------------------------------------------

def agent_evidence_to_dict(evidence: AgentEvidence) -> dict:
    return {
        "agent": evidence.agent.value,
        "score": evidence.score,
        "rationale": evidence.rationale,
        "primary_timeframe": evidence.primary_timeframe.value,
        "symbol": evidence.symbol,
        "as_of": _dt_to_str(evidence.as_of),
        "context_id": evidence.context_id,
        "supporting_metrics": dict(evidence.supporting_metrics),
    }


def dict_to_agent_evidence(data: dict) -> AgentEvidence:
    return AgentEvidence(
        agent=AgentName(data["agent"]),
        score=data["score"],
        rationale=data["rationale"],
        primary_timeframe=Timeframe(data["primary_timeframe"]),
        symbol=data["symbol"],
        as_of=_str_to_dt(data["as_of"]),
        context_id=data["context_id"],
        supporting_metrics=dict(data["supporting_metrics"]),
    )


# -- Signal --------------------------------------------------------------

def signal_to_dict(signal: Signal) -> dict:
    return {
        "symbol": signal.symbol,
        "timestamp": _dt_to_str(signal.timestamp),
        "context_id": signal.context_id,
        "score": signal.score,
        "confidence": signal.confidence,
        "risk_level": signal.risk_level.value,
        "primary_timeframe": signal.primary_timeframe.value,
        "supporting_factors": [agent_evidence_to_dict(e) for e in signal.supporting_factors],
        "contradicting_factors": [agent_evidence_to_dict(e) for e in signal.contradicting_factors],
        "invalidation": signal.invalidation,
        "model_version": signal.model_version,
    }


def dict_to_signal(data: dict) -> Signal:
    return Signal(
        symbol=data["symbol"],
        timestamp=_str_to_dt(data["timestamp"]),
        context_id=data["context_id"],
        score=data["score"],
        confidence=data["confidence"],
        risk_level=RiskLevel(data["risk_level"]),
        primary_timeframe=Timeframe(data["primary_timeframe"]),
        supporting_factors=tuple(dict_to_agent_evidence(e) for e in data["supporting_factors"]),
        contradicting_factors=tuple(dict_to_agent_evidence(e) for e in data["contradicting_factors"]),
        invalidation=data["invalidation"],
        model_version=data["model_version"],
    )


# -- PaperPosition -------------------------------------------------------

def position_to_dict(position: PaperPosition) -> dict:
    return {
        "symbol": position.symbol,
        "side": position.side.value,
        "quantity": position.quantity,
        "average_entry_price": position.average_entry_price,
        "realized_pnl": position.realized_pnl,
        "updated_at": _dt_to_str(position.updated_at),
    }


def dict_to_position(data: dict) -> PaperPosition:
    return PaperPosition(
        symbol=data["symbol"],
        side=PositionSide(data["side"]),
        quantity=data["quantity"],
        average_entry_price=data["average_entry_price"],
        realized_pnl=data["realized_pnl"],
        updated_at=_str_to_dt(data["updated_at"]),
    )


# -- PaperFill / PaperOrder ------------------------------------------------

def fill_to_dict(fill: PaperFill) -> dict:
    return {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "symbol": fill.symbol,
        "side": fill.side.value,
        "quantity": fill.quantity,
        "price": fill.price,
        "fee": fill.fee,
        "filled_at": _dt_to_str(fill.filled_at),
    }


def dict_to_fill(data: dict) -> PaperFill:
    return PaperFill(
        fill_id=data["fill_id"],
        order_id=data["order_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        quantity=data["quantity"],
        price=data["price"],
        fee=data["fee"],
        filled_at=_str_to_dt(data["filled_at"]),
    )


def order_to_dict(order: PaperOrder) -> dict:
    return {
        "order_id": order.order_id,
        "symbol": order.symbol,
        "side": order.side.value,
        "quantity": order.quantity,
        "signal_context_id": order.signal_context_id,
        "created_at": _dt_to_str(order.created_at),
    }


def dict_to_order(data: dict) -> PaperOrder:
    return PaperOrder(
        order_id=data["order_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        quantity=data["quantity"],
        signal_context_id=data["signal_context_id"],
        created_at=_str_to_dt(data["created_at"]),
    )


=== FILE: crypto_signal_engine/persistence/sqlite_store.py ===
"""
SQLite tabanlı `CandleStateStore` implementasyonu.

Kural (Bölüm 12):
- parameterized SQL (hiçbir yerde string interpolation ile SQL kurulmaz)
- explicit schema creation + deterministic schema_version
- transaction boundaries (`with self._connection:`)
- proper close()
- no global hidden connection (her instance kendi connection'ını sahiplenir)
- thread/async ownership AÇIK: bu sınıf SENKRONDUR; async bağlamda
  kullanılacaksa çağıran taraf `asyncio.to_thread(...)` ile sarmalamalıdır
  (Bölüm 13 — domain/persistence senkron kalır, yalnızca network async'tir)
- duplicate handling explicit: `INSERT ... ON CONFLICT DO UPDATE`, ama bu
  yalnızca CandleSequencer'ın ZATEN kabul ettiği (duplicate/out-of-order
  reddedilmiş) update'ler için çağrılır — gerçek "duplicate" asla persist
  edilmez, yalnızca AYNI identity'nin ardışık kabul edilen update'leri
  upsert edilir.
- primary key: (symbol, timeframe, open_time_ms) — `close_time` KESİNLİKLE
  uniqueness key olarak kullanılmaz (Phase 1 Karar: candle identity
  close_time içermez).
- Rejected quality data buraya HİÇ ULAŞMAZ (bkz. commit() — quality
  reddi sequencer'a bile gitmeden erken döner).
"""

from __future__ import annotations

import copy
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain.candle_sequencing import CandleSequencer, CandleUpdate
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, CandleIdentity
from crypto_signal_engine.domain.state_contract import CandleStateStore, CommitOutcome, CommitResult
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.quality.base import DataQualityResult

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time_ms INTEGER NOT NULL,
    close_time_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    is_closed INTEGER NOT NULL,
    trade_count INTEGER,
    update_seq INTEGER NOT NULL,
    PRIMARY KEY (symbol, timeframe, open_time_ms)
);
"""


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


class SqliteCandleStateStore(CandleStateStore):
    """`CandleStateStore` contract'ının SQLite-backed implementasyonu.

    Sequencing kararı (duplicate/out-of-order/after-final) her zaman
    in-memory `CandleSequencer` tarafından verilir — SQLite yalnızca kabul
    edilmiş sonucun DAYANIKLI (durable) kopyasını tutar. Bu ayrım, aynı
    saf sequencing mantığının hem `InMemoryCandleStateStore` hem
    `SqliteCandleStateStore` arasında TUTARLI kalmasını sağlar.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._sequencer = CandleSequencer()
        self._init_schema()
        self._restore_sequencer_from_disk()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

    def _restore_sequencer_from_disk(self) -> None:
        """Restart sonrası diskteki mevcut candle'ları in-memory sequencer'a
        yükler ki canonical state süreklilik göstersin.

        Not: burada quality-gate TEKRAR çalıştırılmaz — bu satırlar daha
        önce zaten quality-gate'ten geçip commit edilmiş verilerdir.
        """
        try:
            cursor = self._connection.execute(
                "SELECT symbol, timeframe, open_time_ms, close_time_ms, open, high, low, "
                "close, volume, is_closed, trade_count, update_seq FROM candles "
                "ORDER BY symbol, timeframe, open_time_ms ASC"
            )
            rows = cursor.fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"Mevcut candle'lar okunamadı: {exc}") from exc

        for row in rows:
            (symbol, timeframe_str, open_ms, close_ms, open_, high, low, close, volume,
             is_closed, trade_count, update_seq) = row
            candle = Candle(
                symbol=symbol,
                timeframe=Timeframe(timeframe_str),
                open_time=_from_epoch_ms(open_ms),
                close_time=_from_epoch_ms(close_ms),
                open=open_, high=high, low=low, close=close, volume=volume,
                is_closed=bool(is_closed), trade_count=trade_count,
            )
            update = CandleUpdate(
                candle=candle, update_seq=update_seq,
                event_time=candle.close_time, received_at=candle.close_time,
            )
            # Diskten okunan satırlar zaten sırayla ve geçerli; taze bir
            # sequencer'a uygulanması her zaman kabul edilir.
            self._sequencer.apply(update)

    def peek_previous(self, identity: CandleIdentity) -> Candle | None:
        return self._sequencer.peek(identity)

    def commit(self, quality_result: DataQualityResult, update: CandleUpdate) -> CommitResult:
        """Quality-gate'ten geçen bir update'i ATOMIK olarak commit etmeyi dener.

        ATOMICITY INVARIANT: SQLite persist BAŞARISIZ olursa, canonical
        in-memory `self._sequencer` durumu DEĞİŞMEMİŞ olmalıdır — ne
        `_latest` ne `_finalized` ilerlemiş olmalı. Bunu sağlamak için:

        1. Sequencing kararı önce bir SCRATCH (deep-copy) sequencer üzerinde
           "dry-run" edilir — bu, GERÇEK `self._sequencer`'ı HİÇ MUTATE
           ETMEZ.
        2. Dry-run sonucu REJECTED_SEQUENCE ise, gerçek sequencer'a hiç
           dokunulmadan doğrudan döner.
        3. Dry-run sonucu ACCEPTED ise, ÖNCE SQLite'a persist edilir.
           Persist başarısız olursa `PersistenceError` YUKARI FIRLATILIR
           ve `self._sequencer` HÂLÂ ESKİ HÂLİNDEDİR (hiç çağrılmadı).
        4. Persist başarılı olursa, AYNI update GERÇEK `self._sequencer`'a
           uygulanır (dry-run'da doğrulanmış olduğundan bu adım her zaman
           deterministik olarak aynı ACCEPTED sonucu üretir).

        Bu, `CandleSequencer`'ın (Phase 1 domain modülü) kaynak kodunu
        HİÇ DEĞİŞTİRMEDEN, yalnızca standart kütüphane `copy.deepcopy` ile
        Phase 2 tarafında sağlanan bir atomicity garantisidir.
        """
        if not quality_result.passed:
            # KRİTİK INVARIANT: reddedilen veri ne sequencer'a ne de
            # SQLite'a ulaşır.
            return CommitResult(
                outcome=CommitOutcome.REJECTED_QUALITY,
                canonical_candle=self._sequencer.peek(update.identity),
                detail=f"data quality reddetti: {quality_result.status} — {quality_result.reason}",
            )

        # 1-2. Dry-run: gerçek sequencer'ı mutate ETMEDEN sonucu öğren.
        scratch_sequencer = copy.deepcopy(self._sequencer)
        dry_run_result = scratch_sequencer.apply(update)
        if not dry_run_result.accepted:
            return CommitResult(
                outcome=CommitOutcome.REJECTED_SEQUENCE,
                canonical_candle=dry_run_result.canonical_candle,
                detail=f"sequence reddetti: {dry_run_result.outcome}",
            )

        # 3. Persist ÖNCE dener; başarısız olursa gerçek sequencer'a hiç
        #    dokunulmamış olarak (aşağıdaki satıra hiç ulaşılmadan)
        #    PersistenceError yukarı fırlatılır.
        self._persist(update)

        # 4. Persist başarılı: şimdi GERÇEK sequencer'a aynı update'i
        #    uygula — dry-run'da doğrulandığı için deterministik olarak
        #    aynı ACCEPTED sonucu üretecektir.
        real_result = self._sequencer.apply(update)
        return CommitResult(
            outcome=CommitOutcome.COMMITTED,
            canonical_candle=real_result.canonical_candle,
            detail=f"commit edildi (SQLite): {real_result.outcome}",
        )

    def _persist(self, update: CandleUpdate) -> None:
        c = update.candle
        try:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO candles
                        (symbol, timeframe, open_time_ms, close_time_ms, open, high, low,
                         close, volume, is_closed, trade_count, update_seq)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, timeframe, open_time_ms) DO UPDATE SET
                        close_time_ms = excluded.close_time_ms,
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        is_closed = excluded.is_closed,
                        trade_count = excluded.trade_count,
                        update_seq = excluded.update_seq
                    """,
                    (
                        c.symbol, c.timeframe.value, _to_epoch_ms(c.open_time), _to_epoch_ms(c.close_time),
                        c.open, c.high, c.low, c.close, c.volume, int(c.is_closed), c.trade_count,
                        update.update_seq,
                    ),
                )
        except sqlite3.Error as exc:
            raise PersistenceError(f"Candle persist edilemedi ({c.symbol}/{c.timeframe}): {exc}") from exc

    def close(self) -> None:
        """Bağlantıyı kapatır. Idempotent değildir; iki kez çağrılırsa
        sqlite3 kendi hatasını fırlatır (bu, açık ve beklenen bir davranıştır)."""
        self._connection.close()


=== FILE: crypto_signal_engine/runtime/__init__.py ===
"""
Faz 6 — Real-Time Market Data & Runtime.

Bu paket, ZATEN kabul edilmiş Faz 2 (Binance PUBLIC market data), Faz 3
(Feature Engine), Faz 4 (agents/consensus/risk/SignalEngine) ve Faz 5
(PaperTradingEngine) bileşenlerini KOORDİNE eder — hiçbirinin mantığını
yeniden uygulamaz veya kopyalamaz (bkz. coordinator.py).

Hiçbir private/signed Binance endpoint'i, API key/secret, order
placement/cancellation, veya Testnet/Mainnet execution İÇERMEZ.
`ALLOW_LIVE_TRADING = False` korunur. Faz 6 SADECE paper trading (Faz 5)
ile PUBLIC market data'yı sürekli/gerçek-zamanlı olarak besler.
"""

from __future__ import annotations


=== FILE: crypto_signal_engine/runtime/bootstrap.py ===
"""
Faz 6 — bootstrap: canlı işlemeye başlamadan önce Faz 3/4 hesaplamalarının
geçerli olması için yeterli public geçmiş veriyi elde eder.

Kural (Bölüm 9 — talimat): "Bootstrap must be deterministic for the same
input data. If insufficient data exists, expose an explicit NOT_READY
state rather than fabricating features/signals." Bu modül YALNIZCA
zaten-fetch-edilmiş bir `Candle` listesini deterministik olarak uygular;
REST çağrısının KENDİSİ (async, Faz 2'nin `fetch_historical_candles()`'ı)
`coordinator.py::RuntimeCoordinator.bootstrap()`'ta yapılır — bu ayrım,
bu fonksiyonun (ve dolayısıyla bootstrap mantığının) ağ olmadan,
senkron ve doğrudan test edilebilir kalmasını sağlar (Bölüm 22).

Aynı REST çağrısı iki kez yapılırsa veya bootstrap ile canlı akış overlap
ederse, uygulama `CandleWindow.offer()` üzerinden geçer — bu yüzden
duplicate/out-of-order candle'lar burada da (Bölüm 12 ile aynı garantiyle)
güvenle SESSİZCE atlanır.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.runtime.candle_window import CandleWindow
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome


def apply_bootstrap_candles(
    *,
    window: CandleWindow,
    feature_engine: FeatureEngine,
    symbol: str,
    timeframe: Timeframe,
    candles: list[Candle],
    as_of: datetime,
    min_candles: int,
) -> BootstrapReport:
    """Fetch edilmiş (REST veya canlı gap-fill) geçmiş candle'ları
    kronolojik sırayla pencereye uygular; yeterliyse bir `FeatureSnapshot`
    commit eder.

    `candles` GİRDİ SIRASINA GÜVENİLMEZ — açıkça `open_time`'a göre
    sıralanır (Bölüm 9 — "bootstrap chronological ordering"), ardından
    `window.offer()` ile TEK TEK uygulanır (Bölüm 9 — "duplicate bootstrap
    data handling").
    """
    applied = 0
    for candle in sorted(candles, key=lambda c: c.open_time):
        outcome = window.offer(candle)
        if outcome is IngestOutcome.ACCEPTED:
            applied += 1

    history = window.history()
    ready = len(history) >= min_candles

    generated_at = history[-1].close_time if history else as_of
    if ready:
        snapshot = feature_engine.compute_candle_features(list(history), symbol, timeframe, as_of=generated_at)
        feature_engine.commit_snapshot(snapshot)

    return BootstrapReport(
        symbol=symbol, timeframe=timeframe, candles_applied=applied, ready=ready, generated_at=generated_at
    )


=== FILE: crypto_signal_engine/runtime/candle_window.py ===
"""
Faz 6 — per-(symbol, timeframe) canlı candle penceresi.

NEDEN GEREKLİ (bkz. PHASE6_REALTIME_RUNTIME.md — "Neden ayrı bir candle
penceresi"): `BinanceMarketDataProvider`'ın kendi `StateManager`'ı (Faz 2)
TAMAMEN İÇ (private) bir bileşendir — dışarıdan erişilemez. Faz 3
`FeatureEngine.compute_candle_features()` ise STATELESS'tir: her çağrıda
TAM candle listesini parametre olarak bekler, kendi geçmişini tutmaz.
Runtime'ın feature hesaplaması için kullanacağı candle geçmişini BİR
YERDE tutması gerekir — bu sınıf o yerdir.

KRİTİK AYRIM: Bu sınıf OHLC/kalite doğrulaması YAPMAZ (`DataQualityGate`,
`CandleSequencer` zaten Faz 2'de bunu tamamlamıştır — provider'dan çıkan
her `Candle` zaten kalite-onaylı, kanonik bir candle'dır). Bu sınıf
YALNIZCA runtime'ın KENDİ rolling penceresi için identity-bazlı
(`open_time`) dedup + monoton sıralama garantisi sağlar — bir reconnect
sonrası aynı kapalı candle'ın tekrar teslim edilmesi veya bootstrap/canlı
akış overlap'i gibi runtime-seviyesi senaryolara karşı (Bölüm 12).
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.runtime.models import IngestOutcome


class CandleWindow:
    """Tek bir (symbol, timeframe) için bounded, deterministic candle penceresi."""

    def __init__(self, maxlen: int = 500, *, duration: timedelta | None = None) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen pozitif olmalı")
        self._candles: deque[Candle] = deque(maxlen=maxlen)
        self._last_open_time: datetime | None = None
        # `duration` verilirse (bkz. coordinator.py — HER ZAMAN verilir),
        # gap detection AKTİFTİR: `open_time`'da tam olarak bir interval'lik
        # ilerlemeden FAZLASI bir "atlanmış aralık" (gap) olarak SAYILIR ve
        # ASLA sessizce kabul EDİLMEZ (Bölüm 14 — "never hide a gap by
        # jumping directly to the latest event"). `None` ise (yalnızca
        # geriye-uyumluluk/genel amaçlı kullanım için) gap detection KAPALI.
        self._duration = duration

    def offer(self, candle: Candle) -> IngestOutcome:
        """Bir candle'ı pencereye eklemeyi dener.

        - `is_closed=False` (hâlâ oluşmakta olan candle) HİÇBİR ZAMAN
          pencereye girmez (Bölüm 6 — "never treat a still-forming candle
          as completed") -> `UNCLOSED_SKIPPED`.
        - Aynı `open_time` ikinci kez gelirse (reconnect sonrası yeniden
          teslimat, bootstrap/canlı overlap) -> `DUPLICATE` (no-op; state
          SESSİZCE ezilmez — "conflicting duplicate data must not silently
          overwrite trusted state").
        - Daha eski bir `open_time` gelirse -> `OUT_OF_ORDER` (no-op; state
          ASLA geriye alınmaz).
        - Bir sonraki BEKLENEN `open_time`'dan (son + `duration`) DAHA
          İLERİ bir `open_time` gelirse -> `GAP_DETECTED` (no-op; candle
          HENÜZ pencereye KABUL EDİLMEZ — çağıran taraf önce eksik aralığı
          doldurmalıdır, bkz. `coordinator.py::resolve_gap`).
        - Aksi halde (kesin olarak bir sonraki beklenen `open_time`)
          -> `ACCEPTED`.
        """
        if not candle.is_closed:
            return IngestOutcome.UNCLOSED_SKIPPED

        if self._last_open_time is not None:
            if candle.open_time == self._last_open_time:
                return IngestOutcome.DUPLICATE
            if candle.open_time < self._last_open_time:
                return IngestOutcome.OUT_OF_ORDER
            if self._duration is not None and candle.open_time > self._last_open_time + self._duration:
                return IngestOutcome.GAP_DETECTED

        self._candles.append(candle)
        self._last_open_time = candle.open_time
        return IngestOutcome.ACCEPTED

    def history(self) -> tuple[Candle, ...]:
        """Kabul edilmiş TÜM (bounded) candle'lar, ascending open_time sırayla."""
        return tuple(self._candles)

    def __len__(self) -> int:
        return len(self._candles)

    @property
    def latest_open_time(self) -> datetime | None:
        return self._last_open_time


=== FILE: crypto_signal_engine/runtime/coordinator.py ===
"""
Faz 6 — RuntimeCoordinator: mevcut Faz 2 (public Binance market data),
Faz 3 (`FeatureEngine`), Faz 4 (`SignalEngine`) ve Faz 5
(`PaperTradingEngine`) bileşenlerini KOORDİNE EDEN, instance-scoped bir
runtime katmanı.

Kural (Bölüm 4 — talimat): runtime, mevcut modülleri koordine eder, onların
mantığını YENİDEN UYGULAMAZ:
- candle/order-book kalite/sıralama/reconnect/gap-recovery: Faz 2
  (`BinanceMarketDataProvider` İÇİNDE, ZATEN tam otomatik — bkz. aşağı).
- feature formülleri: Faz 3 (`FeatureEngine`, DEĞİŞTİRİLMEDEN kullanılır).
- agent/consensus/risk/context-id mantığı: Faz 4 (`SignalEngine`, TEK bir
  yerden çağrılır — bkz. `_maybe_evaluate_signal`).
- paper accounting/fee/slippage/idempotency: Faz 5 (`PaperTradingEngine`,
  DEĞİŞTİRİLMEDEN kullanılır).
- `ConsensusResult.regime` sözleşme sapması (Faz 4, Karar 46/47) bu
  modülde HİÇ görünmez — Faz 6, Faz 4'ün `Signal` çıktısını tüketir,
  `ConsensusResult`/`RegimeContext`'e hiçbir bağımlılığı yoktur.

NEDEN AYRI BİR `CandleWindow` GEREKLİ: `BinanceMarketDataProvider`'ın kendi
`StateManager`'ı (Faz 2) private'tır, dışarıdan erişilemez; `FeatureEngine`
stateless'tir (her çağrıda TAM candle listesi bekler). Runtime, feature
hesaplaması için KULLANACAĞI candle geçmişini kendi (bounded, dedup'lı)
penceresinde tutmak ZORUNDADIR (bkz. candle_window.py).

RECONNECT/GAP-RECOVERY NEREDE: `BinanceMarketDataProvider.stream_candles()`/
`stream_order_book()`, kendi İÇİNDE (`while not self._closed:`) sınırlı/
backoff'lu reconnect döngüsü VE Phase 2'nin KENDİ kanonik state'i (private
`StateManager`) için otomatik REST gap-recovery/resync çalıştırır (Faz 2,
zaten test edilmiş) — Phase 6 BU İÇ mekanizmayı YENİDEN İMPLEMENTE ETMEZ;
yalnızca provider'ın tek bir async iterator'ını tüketir ve ondan çıkan
CANDLE/ORDER-BOOK nesnelerini olduğu gibi kabul eder.

Runtime'ın KENDİ eklediği şey — Phase 2'nin private state'inin AYNASI
DEĞİL, ONA TAMAMLAYICI, farklı bir katmanda çalışan üç şey:
(a) bu iterator'dan gelen her candle için identity-bazlı dedup/sıralama/
GAP TESPİTİ (`CandleWindow`, Bölüm 12/14 — runtime'ın KENDİ rolling
penceresi provider'ın internal state'inden bağımsız olduğu için gereken
bir savunma katmanı: `resolve_gap()`, tespit edilen bir aralığı YİNE Faz
2'nin `fetch_historical_candles()` REST mekanizmasıyla, yalnızca eksik
kısmı çekerek doldurur — REST çağrısının kendisi ödünç alınır, gap-tespit
mantığı runtime'a özeldir), (b) staleness-bazlı sağlık izleme
(`HealthMonitor`), (c) "M5 kapanışı -> SignalEngine.evaluate() ->
PaperTradingEngine.process_signal()" akışının TEK, deterministic
orkestrasyonu.

Instance isolation (Bölüm 8): TÜM mutable state `__init__` içinde, HER
ZAMAN taze instance attribute'leri olarak oluşturulur — hiçbir
modül-seviyesi mutable state YOKTUR; iki ayrı `RuntimeCoordinator` örneği
hiçbir şey PAYLAŞMAZ (bkz. `tests/test_runtime_coordinator.py::
TestInstanceIsolation`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.errors import AgentInputError
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot
from crypto_signal_engine.providers.base import LiveDataProvider
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock
from crypto_signal_engine.runtime.bootstrap import apply_bootstrap_candles
from crypto_signal_engine.runtime.candle_window import CandleWindow
from crypto_signal_engine.runtime.errors import BootstrapNotReadyError, IdentityMismatchError
from crypto_signal_engine.runtime.health import HealthMonitor
from crypto_signal_engine.runtime.models import (
    BootstrapReport,
    IngestOutcome,
    MarketEventKind,
    ProcessedMarketEvent,
    RuntimeCycleResult,
    RuntimeHealth,
    RuntimeStatus,
    SymbolHealth,
)
from crypto_signal_engine.signal_engine import SignalEngine

# Faz 4'ün primary signal timeframe'i (bkz. PHASE4_QUANT_AGENT_ENGINE.md —
# QuantAgent M5) — sinyal üretimi YALNIZCA bu timeframe'in kapanışında
# tetiklenir (Bölüm 10 — "meaningful completed-data boundary", her ham
# paket için DEĞİL).
PRIMARY_SIGNAL_TIMEFRAME = Timeframe.M5

DEFAULT_CANDLE_TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M5, Timeframe.M15, Timeframe.H1)

# Faz 3/4'ün gerektirdiği minimum kapalı candle sayısı (RSI_14/ROC_10/
# VWAP_DEVIATION_20/RELATIVE_VOLUME_20/BOLLINGER_BANDWIDTH_20_2/
# DIST_FROM_HIGH_20/DIST_FROM_LOW_20 arasındaki EN BÜYÜK min_history = 20)
# ARTI güvenlik payı — bkz. PHASE6_REALTIME_RUNTIME.md "Bootstrap".
MIN_REQUIRED_WARMUP_CANDLES = 20
DEFAULT_WARMUP_CANDLES = 25

_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}

_HEALTH_SEVERITY: dict[RuntimeHealth, int] = {
    RuntimeHealth.READY: 0,
    RuntimeHealth.BOOTSTRAPPING: 1,
    RuntimeHealth.DEGRADED: 2,
    RuntimeHealth.STOPPED: 3,
}


def _worst_health(healths: Iterable[RuntimeHealth]) -> RuntimeHealth:
    """Verilen sağlık durumlarının EN KÖTÜSÜ (fail-safe — bkz. RuntimeStatus
    docstring'i)."""
    healths = list(healths)
    if not healths:
        return RuntimeHealth.BOOTSTRAPPING
    return max(healths, key=lambda h: _HEALTH_SEVERITY[h])


def _lookback_window(timeframe: Timeframe, warmup_candles: int) -> timedelta:
    """Bootstrap REST fetch'i için, `warmup_candles` kadar kapalı candle'ı
    KESİNLİKLE kapsayacak deterministik bir geçmişe-dönük pencere (güvenlik
    payı ile — hafta sonu/tatil boşlukları, vb. için 2x)."""
    return _TIMEFRAME_DURATIONS[timeframe] * warmup_candles * 2


class RuntimeCoordinator:
    """Bir sembol kümesi için Faz 2->5 pipeline'ını koordine eden runtime.

    HİÇBİR gerçek Binance API çağrısı bu sınıfın İÇİNDE yapılmaz — tüm
    network erişimi enjekte edilen `provider: LiveDataProvider` üzerinden
    (Faz 2'nin ZATEN kabul edilmiş, salt public-market-data sözleşmesiyle)
    gerçekleşir. Bu sınıf hiçbir API key/secret ALMAZ, hiçbir emir
    GÖNDERMEZ.
    """

    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        provider: LiveDataProvider,
        candle_timeframes: tuple[Timeframe, ...] = DEFAULT_CANDLE_TIMEFRAMES,
        warmup_candles: int = DEFAULT_WARMUP_CANDLES,
        candle_window_size: int = 500,
        order_book_depth: int = 20,
        stale_feed_threshold_seconds: float = 30.0,
        history_store: FeatureHistoryStore | None = None,
        feature_engine: FeatureEngine | None = None,
        paper_engine: PaperTradingEngine | None = None,
        model_version: str | None = None,
        clock: Clock | None = None,
        order_book_observer: Callable[[str, FeatureSnapshot], None] | None = None,
        candle_observer: Callable[[str, Timeframe, Candle], None] | None = None,
    ) -> None:
        if not symbols:
            raise ValueError("symbols boş olamaz")
        if not candle_timeframes:
            raise ValueError("candle_timeframes boş olamaz")
        if warmup_candles < MIN_REQUIRED_WARMUP_CANDLES:
            raise ValueError(
                f"warmup_candles en az {MIN_REQUIRED_WARMUP_CANDLES} olmalı "
                f"(Faz 3/4 feature min_history gereksinimi)"
            )

        self._symbols: tuple[str, ...] = tuple(dict.fromkeys(normalize_symbol(s) for s in symbols))
        self._provider = provider
        self._candle_timeframes = candle_timeframes
        self._warmup_candles = warmup_candles
        self._candle_window_size = candle_window_size
        self._order_book_depth = order_book_depth
        self._clock: Clock = clock or SystemClock()

        self._history_store = history_store or FeatureHistoryStore()
        self._feature_engine = feature_engine or FeatureEngine(history_store=self._history_store)
        self._paper_engine = paper_engine or PaperTradingEngine()
        self._signal_engine = (
            SignalEngine(self._history_store, model_version=model_version)
            if model_version is not None
            else SignalEngine(self._history_store)
        )
        # PRE-AUDIT ENHANCEMENT PASS — additive, optional (bkz.
        # PRE_AUDIT_ENHANCEMENTS.md, `research/orderbook_capture.py`).
        # `None` (varsayılan) iken bu satırın etkisi SIFIRDIR: aşağıdaki
        # tek çağrı sitesi (`ingest_order_book`) hiçbir şey yapmaz, hiçbir
        # mevcut davranış DEĞİŞMEZ. Verildiğinde, kabul edilmiş Faz 3
        # order-book `FeatureSnapshot`'ının (M1 "raporlama çerçevesi")
        # commit edildiği HER an, salt-gözlemsel olarak çağrılır —
        # çağrı `ingest_order_book` içinde try/except ile SARILIR (bkz.
        # aşağı): bir observer hatası Signal/PaperTradingResult üretimini
        # HİÇBİR ŞEKİLDE etkileyemez.
        self._order_book_observer = order_book_observer
        # Adaptive Intelligence v1, step 8 — additive, optional, exact
        # same discipline as `order_book_observer` above (Karar 87):
        # `None` (default) has ZERO effect on any existing behaviour. When
        # supplied, called ONLY for an already-ACCEPTED candle (see
        # `ingest_candle`, after ALL state mutation for this event has
        # already completed successfully), purely observationally, so
        # `adaptive/shadow.py` can evolve its own independent hypothetical
        # `PositionLifecycleState` per shadowed challenger without this
        # coordinator ever knowing shadow evaluation exists. KNOWN,
        # DOCUMENTED LIMITATION: `ingest_candle` only ever receives M5/
        # M15/H1 candles — the live M1 stream is consumed directly by
        # `LifecycleRuntime._consume_m1`, entirely bypassing this
        # coordinator, so shadow evaluation observes candles at M5
        # granularity, never true M1 fidelity. This is a real, accepted
        # deviation from live-M1 fidelity, not a defect — see the
        # Adaptive Intelligence v1 final report.
        self._candle_observer = candle_observer

        self._health = HealthMonitor(clock=self._clock, stale_feed_threshold_seconds=stale_feed_threshold_seconds)
        self._candle_windows: dict[tuple[str, Timeframe], CandleWindow] = {
            (symbol, timeframe): CandleWindow(maxlen=candle_window_size, duration=_TIMEFRAME_DURATIONS[timeframe])
            for symbol in self._symbols
            for timeframe in self._candle_timeframes
        }
        self._ready: dict[tuple[str, Timeframe], bool] = dict.fromkeys(self._candle_windows, False)
        # Faz 4'ün SignalEngine'i M1 order-book evidence'ı ZORUNLU kılar
        # (bkz. `agents/context.py::build_agent_context` — 4 timeframe'in
        # HEPSİ gerekli). Bu yüzden READY, M5/M15/H1 candle warmup'ına EK
        # OLARAK, sembol başına en az bir kabul edilmiş order-book feature
        # snapshot'ı da gerektirir (bkz. `_maybe_mark_symbol_ready`).
        self._order_book_ready: dict[str, bool] = dict.fromkeys(self._symbols, False)
        self._latest_order_book: dict[str, OrderBookSnapshot] = {}

        self._tasks: list[asyncio.Task] = []
        # Autonomous Testnet trading lifecycle Phase 16 — per-symbol task
        # tracking, additive to `self._tasks` (never a second source of
        # truth for full-shutdown: `stop()` still cancels via `self._tasks`
        # alone; this dict only enables SELECTIVE per-symbol cancellation
        # for `remove_symbol`, see below).
        self._tasks_by_symbol: dict[str, list[asyncio.Task]] = {}
        self._running = False
        self._stopped = False

        # Faz 12: read-only gözlemlenebilirlik için (dashboard) — sembol
        # başına EN SON tamamlanmış `RuntimeCycleResult`'ı tutar. Sınırlı
        # bellek: sembol başına TEK bir giriş (üzerine yazılır, asla
        # büyümez) — bkz. Bölüm 7 "bounded in-memory history".
        self._last_cycle_results: dict[str, RuntimeCycleResult] = {}

        for symbol in self._symbols:
            self._health.mark_bootstrapping(symbol)

    @property
    def paper_engine(self) -> PaperTradingEngine:
        """Faz 12: read-only gözlemlenebilirlik (dashboard) için — bu
        property İŞ MANTIĞI eklemez, yalnızca ZATEN kurulmuş olan Faz 5
        motorunu DIŞARIYA açar (pozisyon/PnL SORGULAMAK için, ASLA
        mutasyon için kullanılmamalıdır)."""
        return self._paper_engine

    def last_cycle_result(self, symbol: str) -> RuntimeCycleResult | None:
        """Faz 12: sembol için EN SON tamamlanmış sinyal-değerlendirme
        döngüsü (varsa) — dashboard'un "last completed signal" alanı
        için. Henüz hiç değerlendirme YAPILMAMIŞSA `None`."""
        return self._last_cycle_results.get(normalize_symbol(symbol))

    # -- Bootstrap ---------------------------------------------------------

    async def bootstrap(self) -> tuple[BootstrapReport, ...]:
        """Her (symbol, timeframe) için REST üzerinden (Faz 2'nin
        `provider.fetch_historical_candles()`'ı ile) geçmiş candle'ları
        çeker ve `bootstrap_candles()`'a (senkron, deterministik) uygular.
        Sağlık aggregation'ı (TÜM timeframe'ler hazır -> READY) TEK bir
        yerde, `bootstrap_candles()`'ın KENDİSİNDE yapılır (bkz. aşağı) —
        bu yüzden `bootstrap()` YALNIZCA REST orkestrasyonu ekler; canlı
        gap-fill de dahil `bootstrap_candles()`'a giden HER yol aynı
        aggregation'dan geçer."""
        reports: list[BootstrapReport] = []
        for symbol in self._symbols:
            reports.extend(await self._bootstrap_symbol(symbol))
        return tuple(reports)

    async def _bootstrap_symbol(self, symbol: str) -> tuple[BootstrapReport, ...]:
        """Per-symbol REST bootstrap across every configured candle
        timeframe — factored out of `bootstrap()` so `add_symbol` (Phase
        16) reuses the EXACT same warmup code path for a hot-added symbol,
        never a second implementation."""
        now = self._clock.now()
        reports: list[BootstrapReport] = []
        for timeframe in self._candle_timeframes:
            start = now - _lookback_window(timeframe, self._warmup_candles)
            candles = await self._provider.fetch_historical_candles(symbol, timeframe, start, now)
            reports.append(self.bootstrap_candles(symbol, timeframe, candles, as_of=now))
        return tuple(reports)

    def bootstrap_candles(
        self, symbol: str, timeframe: Timeframe, candles: list[Candle], as_of: datetime
    ) -> BootstrapReport:
        """Senkron, doğrudan test edilebilir (ağ GEREKTİRMEZ): fetch edilmiş
        bir candle listesini ilgili pencereye uygular. Canlı gap-fill de bu
        metodu yeniden kullanır (aynı dedup/sıralama garantisi). Bir
        sembolün TÜM candle timeframe'leri hazır OLMASI YETMEZ — READY için
        ayrıca en az bir order-book feature snapshot'ı da gerekir (bkz.
        `_maybe_mark_symbol_ready`); aksi halde BOOTSTRAPPING'de kalır
        (Bölüm 9 — sahte hazırlık ASLA üretilmez).

        Girdideki HER candle'ın identity'si (`symbol`, `timeframe`),
        HERHANGİ bir state mutate edilmeden ÖNCE, beyan edilen
        parametrelerle doğrulanır — bir tanesi bile UYUŞMAZSA, HİÇBİRİ
        pencereye uygulanmaz (atomik reddediş, Bölüm 12/28 — "identity
        mismatch")."""
        normalized = normalize_symbol(symbol)
        key = (normalized, timeframe)
        if key not in self._candle_windows:
            raise ValueError(f"runtime bu (symbol, timeframe) için konfigüre edilmedi: {key}")

        for candle in candles:
            if candle.symbol != normalized or candle.timeframe != timeframe:
                raise IdentityMismatchError(
                    f"bootstrap_candles: candle identity ({candle.symbol}/{candle.timeframe.value}) "
                    f"!= beyan edilen ({normalized}/{timeframe.value})"
                )

        report = apply_bootstrap_candles(
            window=self._candle_windows[key], feature_engine=self._feature_engine, symbol=normalized,
            timeframe=timeframe, candles=candles, as_of=as_of, min_candles=self._warmup_candles,
        )
        self._ready[key] = report.ready
        self._maybe_mark_symbol_ready(normalized)
        return report

    def _maybe_mark_symbol_ready(self, symbol: str) -> None:
        """Bir sembolü YALNIZCA hem TÜM candle timeframe'leri (M5/M15/H1)
        hazır HEM DE en az bir order-book feature snapshot'ı kabul
        edilmişse READY işaretler (Faz 4 `SignalEngine`'in 4 timeframe'in
        HEPSİNİ gerektirmesiyle BİREBİR uyumlu — bkz. `agents/context.py::
        build_agent_context`). Koşullardan biri bile eksikse hiçbir şey
        YAPILMAZ; sembol BOOTSTRAPPING'de kalır."""
        if not all(self._ready[(symbol, tf)] for tf in self._candle_timeframes):
            return
        if not self._order_book_ready.get(symbol, False):
            return
        self._health.mark_ready(symbol)

    # -- Hot symbol add/remove (Phase 16 — periodic automatic reselection) --

    async def add_symbol(self, symbol: str) -> None:
        """Adds a new symbol's full state + (if `run()` has already
        started) live consumer tasks. Idempotent: adding an already-tracked
        symbol is a no-op (never a duplicate task/subscription, Phase 16
        invariant). Bootstraps this symbol EXACTLY like every symbol
        present at construction time (`_bootstrap_symbol`, the same code
        `bootstrap()` uses) — a hot-added symbol gets the SAME warmup
        discipline, then a fresh execution-activation boundary applies to
        it downstream exactly as for any other symbol (Phase 12, unrelated
        to this method — that boundary lives in the bridge/lifecycle
        layer, not here)."""
        normalized = normalize_symbol(symbol)
        if normalized in self._symbols:
            return
        self._symbols = (*self._symbols, normalized)
        for timeframe in self._candle_timeframes:
            key = (normalized, timeframe)
            self._candle_windows[key] = CandleWindow(
                maxlen=self._candle_window_size, duration=_TIMEFRAME_DURATIONS[timeframe],
            )
            self._ready[key] = False
        self._order_book_ready[normalized] = False
        self._health.mark_bootstrapping(normalized)

        await self._bootstrap_symbol(normalized)

        if self._running and not self._stopped:
            self._spawn_symbol_tasks(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Cleanly cancels JUST this symbol's tasks (candle consumers for
        every timeframe + its order-book consumer) and removes its state.
        Idempotent: removing an untracked symbol is a no-op. Callers
        (the reselection scheduler) are responsible for NEVER calling this
        for a safety-owned symbol (open bridge position, DUST,
        ENTRY_PENDING/EXIT_PENDING/AMBIGUOUS, or PAPER-pinned) — this is a
        low-level primitive with no opinion on trading safety policy."""
        normalized = normalize_symbol(symbol)
        if normalized not in self._symbols:
            return

        tasks = self._tasks_by_symbol.pop(normalized, [])
        for task in tasks:
            task.cancel()
        # Same idiom as `stop()`: `return_exceptions=True` collects both
        # `CancelledError` (expected — we just cancelled these) and any
        # real error (already reflected via `mark_disconnected` inside
        # `_consume_candles`/`_consume_order_book` themselves) as return
        # values rather than raising — removal must never fail because one
        # stream's own already-reported error surfaces here again.
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks = [t for t in self._tasks if t not in tasks]

        self._symbols = tuple(s for s in self._symbols if s != normalized)
        for timeframe in self._candle_timeframes:
            self._candle_windows.pop((normalized, timeframe), None)
            self._ready.pop((normalized, timeframe), None)
        self._order_book_ready.pop(normalized, None)
        self._latest_order_book.pop(normalized, None)
        self._last_cycle_results.pop(normalized, None)

    # -- Live ingestion (senkron, doğrudan test edilebilir) ---------------------

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> ProcessedMarketEvent:
        """Tek bir canlı candle event'ini işler. `outcome != ACCEPTED` ise
        HİÇBİR state mutate edilmez (Bölüm 12/7).

        `candle`'ın GERÇEK identity'si (`candle.symbol`, `candle.timeframe`),
        HERHANGİ bir state mutate edilmeden ÖNCE, çağının beyan ettiği
        (`symbol`, `timeframe`) parametreleriyle doğrulanır — çağıranın
        parametrelerine KÖRÜ KÖRÜNE güvenilmez (Bölüm 12/28 — "identity
        mismatch"; örn. `ingest_candle("BTCUSDT", M5, <ETHUSDT M5 candle>)`
        artık `IdentityMismatchError` ile reddedilir, ASLA BTCUSDT
        penceresine uygulanmaz)."""
        normalized = normalize_symbol(symbol)
        if candle.symbol != normalized:
            raise IdentityMismatchError(
                f"ingest_candle: candle.symbol ({candle.symbol}) != beyan edilen symbol ({normalized})"
            )
        if candle.timeframe != timeframe:
            raise IdentityMismatchError(
                f"ingest_candle: candle.timeframe ({candle.timeframe.value}) != "
                f"beyan edilen timeframe ({timeframe.value})"
            )

        key = (normalized, timeframe)
        if key not in self._candle_windows:
            raise ValueError(f"runtime bu (symbol, timeframe) için konfigüre edilmedi: {key}")

        outcome = self._candle_windows[key].offer(candle)
        if outcome is IngestOutcome.GAP_DETECTED:
            # Bölüm 14/28 — bir gap tespit edildiği anda (recovery henüz
            # denenmemiş olsa bile) sembol AÇIKÇA DEGRADED işaretlenir;
            # "sessizce hâlâ healthy" ARA DURUMU hiçbir zaman gözlemlenemez.
            self._health.mark_gap_fault(normalized, timeframe)
        if outcome is not IngestOutcome.ACCEPTED:
            return ProcessedMarketEvent(
                symbol=normalized, kind=MarketEventKind.CANDLE, outcome=outcome,
                event_time=candle.close_time, timeframe=timeframe,
            )

        # Bu (symbol, timeframe) için bir candle ANCAK tam olarak bir
        # sonraki beklenen open_time olduğunda ACCEPTED olabilir (bkz.
        # CandleWindow.offer) — yani continuity GERÇEKTEN sağlanmıştır;
        # önceki HERHANGİ bir gap fault'u burada güvenle temizlenir.
        self._health.clear_gap_fault(normalized, timeframe)

        self._health.mark_event(normalized, candle.close_time)
        history = self._candle_windows[key].history()
        if len(history) >= self._warmup_candles:
            snapshot = self._feature_engine.compute_candle_features(
                list(history), normalized, timeframe, as_of=candle.close_time
            )
            self._feature_engine.commit_snapshot(snapshot)
            if not self._ready[key]:
                self._ready[key] = True
                self._maybe_mark_symbol_ready(normalized)

        cycle_result: RuntimeCycleResult | None = None
        if timeframe is PRIMARY_SIGNAL_TIMEFRAME and self._ready[key]:
            cycle_result = self._maybe_evaluate_signal(normalized, candle.close_time)

        if self._candle_observer is not None:
            # Adaptive Intelligence v1, step 8 — bkz. __init__ yorumu (aynı
            # desen, `order_book_observer` ile). Bu candle için TÜM state
            # mutasyonu ZATEN BAŞARIYLA tamamlandıktan SONRA, salt-
            # gözlemsel olarak çağrılır; geniş try/except KASITLI (bkz.
            # order_book_observer'daki aynı gerekçe) — TEK sözleşme "bir
            # observer hatası runtime'ı ASLA etkilemez"dir.
            try:
                self._candle_observer(normalized, timeframe, candle)
            except Exception:
                self._health.mark_persistence_fault(normalized, reason="candle_observer")

        return ProcessedMarketEvent(
            symbol=normalized, kind=MarketEventKind.CANDLE, outcome=outcome,
            event_time=candle.close_time, timeframe=timeframe, cycle_result=cycle_result,
        )

    def ingest_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> ProcessedMarketEvent:
        """Tek bir canlı order-book snapshot'ını işler (M1 "reporting frame"
        — bkz. `FeatureEngine.compute_order_book_features`). Bu event ASLA
        kendi başına bir sinyal değerlendirmesi TETİKLEMEZ (Bölüm 10).

        `snapshot`'ın GERÇEK `symbol`'ü, HERHANGİ bir state (`
        _latest_order_book`, health, feature history) mutate edilmeden
        ÖNCE, çağının beyan ettiği `symbol` parametresiyle doğrulanır
        (Bölüm 12/28 — "identity mismatch")."""
        normalized = normalize_symbol(symbol)
        if snapshot.symbol != normalized:
            raise IdentityMismatchError(
                f"ingest_order_book: snapshot.symbol ({snapshot.symbol}) != beyan edilen symbol ({normalized})"
            )
        previous = self._latest_order_book.get(normalized)
        if previous is None:
            outcome = IngestOutcome.ACCEPTED
        elif snapshot.timestamp == previous.timestamp:
            outcome = IngestOutcome.DUPLICATE
        elif snapshot.timestamp < previous.timestamp:
            outcome = IngestOutcome.OUT_OF_ORDER
        else:
            outcome = IngestOutcome.ACCEPTED

        if outcome is not IngestOutcome.ACCEPTED:
            return ProcessedMarketEvent(
                symbol=normalized, kind=MarketEventKind.ORDER_BOOK, outcome=outcome, event_time=snapshot.timestamp,
            )

        self._latest_order_book[normalized] = snapshot
        self._health.mark_event(normalized, snapshot.timestamp)
        feature_snapshot = self._feature_engine.compute_order_book_features(snapshot)
        self._feature_engine.commit_snapshot(feature_snapshot)

        if self._order_book_observer is not None:
            # PRE-AUDIT ENHANCEMENT PASS — bkz. __init__ yorumu. Commit
            # ZATEN BAŞARIYLA tamamlandıktan SONRA, salt-gözlemsel olarak
            # çağrılır; try/except burada KASITLI OLARAK geniştir (belirli
            # bir exception tipiyle sınırlı DEĞİLDİR) çünkü çağıranın
            # (research/orderbook_capture.py dahil, ama bununla sınırlı
            # olmayan) observer implementasyonu hakkında hiçbir varsayımda
            # bulunulamaz — TEK sözleşme "bir observer hatası runtime'ı
            # ASLA etkilemez"dir. Bu, repo'nun `except Exception: pass`
            # yasağını ihlal ETMEZ: hata sessizce yutulmuyor, DEĞERLENDİ-
            # RİLİYOR (health'e AÇIKÇA bir persistence-fault-benzeri sinyal
            # olarak yansıtılıyor — bkz. `mark_persistence_fault`, AYNI
            # "reason-bazlı, sessizce yutmayan" desen).
            try:
                self._order_book_observer(normalized, feature_snapshot)
            except Exception:
                self._health.mark_persistence_fault(normalized, reason="order_book_observer")

        if not self._order_book_ready.get(normalized, False):
            self._order_book_ready[normalized] = True
            self._maybe_mark_symbol_ready(normalized)

        return ProcessedMarketEvent(
            symbol=normalized, kind=MarketEventKind.ORDER_BOOK, outcome=outcome, event_time=snapshot.timestamp,
        )

    # -- Signal generation (TEK sınır — Bölüm 16) --------------------------

    def _maybe_evaluate_signal(self, symbol: str, as_of: datetime) -> RuntimeCycleResult:
        """Faz 4 `SignalEngine`'in TEK çağrıldığı yer (bu runtime içinde
        başka HİÇBİR yerden `SignalEngine`/tek tek agent'lar çağrılmaz).

        Eksik veri (`AgentInputError`) SESSİZCE yutulmaz: `evaluated=False`
        ile AÇIKÇA raporlanır; exception yukarı SIZDIRILMAZ (warm-up
        sırasında normal, beklenen bir durumdur)."""
        generated_at = self._clock.now()
        try:
            signal = self._signal_engine.evaluate(symbol, as_of)
        except AgentInputError:
            return RuntimeCycleResult(
                symbol=symbol, evaluated=False, signal=None, paper_result=None, generated_at=generated_at
            )

        price = self._reference_price(symbol, not_after=signal.timestamp)
        paper_result = self._paper_engine.process_signal(signal, price)
        result = RuntimeCycleResult(
            symbol=symbol, evaluated=True, signal=signal, paper_result=paper_result, generated_at=generated_at
        )
        self._last_cycle_results[symbol] = result
        return result

    def _reference_price(self, symbol: str, not_after: datetime) -> MarketPriceSnapshot:
        """Faz 5'in no-look-ahead sözleşmesini (`price.as_of <=
        signal.timestamp`) YAPISAL OLARAK sağlayan referans fiyat: sinyalin
        BİZZAT ÜRETİLDİĞİ M5 candle kapanışı (bkz. `ingest_candle` —
        `_maybe_evaluate_signal`'ın `as_of`'u HER ZAMAN o candle'ın
        `close_time`'ıdır, dolayısıyla `signal.timestamp` de öyledir).
        Bu eşitlik tesadüfi DEĞİLDİR — Faz 5 validasyonu asla ZAYIFLATILMAZ,
        bunun yerine değerlendirme/referans-fiyat sınırı buna göre
        TASARLANMIŞTIR (Bölüm 17)."""
        history = self._candle_windows[(symbol, PRIMARY_SIGNAL_TIMEFRAME)].history()
        candidates = [c for c in history if c.close_time <= not_after]
        if not candidates:
            raise BootstrapNotReadyError(f"{symbol}: referans fiyat için kapalı M5 candle yok")
        reference = candidates[-1]
        return MarketPriceSnapshot(symbol=symbol, price=reference.close, as_of=reference.close_time)

    # -- Health / status -----------------------------------------------------

    def mark_disconnected(self, symbol: str) -> None:
        """Provider-seviyesi bir bağlantı kaybı gözlemlendiğinde çağrılır
        (bkz. `_consume_candles`/`_consume_order_book`). Staleness-bazlı
        tespit BAĞIMSIZ olarak zaten çalışır (bkz. health.py docstring'i);
        bu, anlık/açık bir sinyal EKLER."""
        self._health.mark_disconnected(symbol)

    def mark_persistence_fault(self, symbol: str, reason: str = "checkpoint") -> None:
        """Faz 7 entegrasyon noktası (bkz. `persistence/recovery.py::
        PersistedRuntime`): bir sembol için durable checkpoint yazımı
        BAŞARISIZ olduğunda DIŞARIDAN çağrılır. Bu sınıfın (Faz 6) KENDİSİ
        hiçbir persistence çağrısı YAPMAZ — yalnızca bu AÇIK, PUBLIC
        sinyal noktasını sağlar (`mark_disconnected` ile AYNI desen).
        `reason`, HANGİ checkpoint türünün (örn. candle vs paper-state)
        başarısız olduğunu ayırt eder — ilgisiz bir türün BAŞARISI, BU
        reason'ı MASKELEMEZ (bkz. health.py)."""
        self._health.mark_persistence_fault(symbol, reason)

    def clear_persistence_fault(self, symbol: str, reason: str = "checkpoint") -> None:
        """Bkz. `mark_persistence_fault` — YALNIZCA AYNI (symbol, reason)
        için BAŞARILI bir sonraki durable checkpoint yazımından sonra
        çağrılmalıdır."""
        self._health.clear_persistence_fault(symbol, reason)

    def status(self) -> RuntimeStatus:
        now = self._clock.now()
        if self._stopped:
            stopped = tuple(
                SymbolHealth(
                    symbol=s, health=RuntimeHealth.STOPPED, last_event_at=None,
                    reconnect_count=0, detail="runtime stopped",
                )
                for s in self._symbols
            )
            return RuntimeStatus(overall_health=RuntimeHealth.STOPPED, symbols=stopped, generated_at=now)

        symbol_healths = tuple(self._health.status_for(s) for s in self._symbols)
        overall = _worst_health(h.health for h in symbol_healths)
        return RuntimeStatus(overall_health=overall, symbols=symbol_healths, generated_at=now)

    # -- Async run/stop (Bölüm 20 — graceful shutdown) -----------------------

    async def run(self) -> None:
        """Her (symbol, timeframe) için `provider.stream_candles()`'ı, her
        sembol için `provider.stream_order_book()`'u TEK BİR KEZ tüketmeye
        başlar (Faz 2 kendi içinde sınırsız reconnect döngüsü çalıştırır —
        bu metod asla kendi başına yeniden subscribe OLMAZ, dolayısıyla
        reconnect başına duplicate subscription YARATAMAZ, bkz. modül
        docstring'i)."""
        if self._stopped:
            raise ValueError("stopped bir RuntimeCoordinator tekrar run() ile başlatılamaz")

        for symbol in self._symbols:
            self._spawn_symbol_tasks(symbol)
        self._running = True

        await asyncio.gather(*self._tasks, return_exceptions=True)

    def _spawn_symbol_tasks(self, symbol: str) -> None:
        """Creates this symbol's candle+order-book consumer tasks and
        tracks them BOTH in the flat `self._tasks` (so `stop()`'s existing
        full-shutdown cancellation is completely unchanged) AND in
        `self._tasks_by_symbol` (so `remove_symbol` can cancel just this
        symbol's tasks). Factored out of `run()` so `add_symbol` reuses the
        EXACT same task-creation code path — never a second implementation
        that could drift (Phase 16)."""
        created: list[asyncio.Task] = []
        for timeframe in self._candle_timeframes:
            created.append(asyncio.create_task(self._consume_candles(symbol, timeframe)))
        created.append(asyncio.create_task(self._consume_order_book(symbol)))
        self._tasks.extend(created)
        self._tasks_by_symbol[symbol] = created

    async def _consume_candles(self, symbol: str, timeframe: Timeframe) -> None:
        """Beklenmeyen bir hata (Faz 2'nin KENDİ reconnect döngüsünün
        yakalayamadığı, gerçekten olağandışı bir durum) SESSİZCE
        YUTULMAZ: sembol AÇIKÇA `DEGRADED` işaretlenir (bkz.
        `mark_disconnected` — bu, `status()` üzerinden gözlemlenebilir tek
        hata-raporlama mekanizmasıdır, repo genelinde henüz bir logging
        altyapısı yoktur) VE exception yukarı fırlatılır (gather'ın
        `return_exceptions=True` ile YAKALAYIP TUTMASI, health-state
        geçişinin ZATEN gerçekleşmiş olması sayesinde "sessiz" değildir)."""
        try:
            async for candle in self._provider.stream_candles(symbol, timeframe):
                if self._stopped:
                    break
                event = self.ingest_candle(symbol, timeframe, candle)
                if event.outcome is IngestOutcome.GAP_DETECTED:
                    await self.resolve_gap(symbol, timeframe, candle)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._health.mark_disconnected(symbol)
            raise

    async def resolve_gap(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> ProcessedMarketEvent:
        """Bölüm 14 — canlı akışta tespit edilmiş bir gap'i (atlanmış
        interval) Faz 2'nin PUBLIC REST mekanizmasıyla (`provider.
        fetch_historical_candles`) doldurur, ardından bekleyen candle'ı
        TEKRAR dener.

        1. eksik aralığı tespit et: `[son kabul edilen open_time + duration,
           pending_candle.open_time)`.
        2. YALNIZCA bu eksik aralığı REST'ten çek.
        3. `bootstrap_candles()` ile (Faz 1 domain modelleri kullanılarak
           ZATEN normalize edilmiş) kronolojik sırayla, dedup-güvenli uygula.
        4. `pending_candle`'ı TEKRAR dene.

        Recovery başarısız olursa (REST yetersiz/hatalı veri döndürürse),
        `pending_candle` HÂLÂ `GAP_DETECTED` olarak reddedilir — state
        SAHTE bir devamlılıkla İLERİ SÜRÜLMEZ. Bu durumda `ingest_candle()`
        (aşağıdaki `return` çağrısı üzerinden), bu (symbol, timeframe) için
        `HealthMonitor.mark_gap_fault()`'u AÇIKÇA tetikler — sembol
        staleness eşiğinin dolmasını BEKLEMEDEN, ANINDA DEGRADED olur
        (Bölüm 14/28 — "must immediately be observably unhealthy"). Bu
        fault, İLGİSİZ bir order-book/başka-timeframe event'i ile ASLA
        temizlenmez; YALNIZCA bu TAM (symbol, timeframe) için sonraki bir
        `ACCEPTED` candle (continuity'nin GERÇEKTEN sağlandığının kanıtı)
        temizler (bkz. `ingest_candle`, `HealthMonitor.clear_gap_fault`).

        Adım 1-3 (`resolve_gap_backfill`'e ayrıştırılmıştır — bkz. o
        metodun docstring'i, Faz 7 BLOCKER FİX) burada AYNEN yeniden
        kullanılır; adım 4 bu sınıfın KENDİ (bare) `ingest_candle()`'ını
        çağırır (bu metodun HER ZAMAN yaptığı şey — davranış DEĞİŞMEDİ)."""
        await self.resolve_gap_backfill(symbol, timeframe, pending_candle)
        normalized = normalize_symbol(symbol)
        return self.ingest_candle(normalized, timeframe, pending_candle)

    async def resolve_gap_backfill(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> None:
        """`resolve_gap`'in SADECE backfill adımı (1-3, yukarı bkz.) —
        `pending_candle`'ın KENDİSİNİ ingest ETMEZ (adım 4 hariç).

        PUBLIC olarak ayrıştırılmıştır (Faz 7 BLOCKER FİX — bkz.
        DECISIONS.md, `persistence/recovery.py::PersistedRuntime.
        resolve_gap`): Faz 7'nin gap-resolution'ın ürettiği bir Paper
        transition'ı da normal kabul edilmiş bir M5 transition ile AYNI
        durable checkpoint garantisi altına alabilmesi için, `PersistedRuntime`
        AYNI backfill hesaplamasını (gap-start tespiti + REST fetch +
        `bootstrap_candles` uygulaması) HİÇBİR mantığı TEKRARLAMADAN yeniden
        kullanır, ardından `pending_candle`'ı bare coordinator yerine KENDİ
        (checkpoint yazan) `ingest_candle()`'ı üzerinden ingest eder. Bu
        metodun kendisi hiçbir persistence/checkpoint kavramından HABERDAR
        DEĞİLDİR (Faz 6, DEĞİŞTİRİLMEDEN kalır) — yalnızca ZATEN var olan
        backfill mantığını, çağıranın (bare coordinator VEYA `PersistedRuntime`)
        ingest adımını kendi seçmesine izin verecek şekilde AÇIĞA çıkarır."""
        normalized = normalize_symbol(symbol)
        window = self._candle_windows[(normalized, timeframe)]
        last_open = window.latest_open_time
        gap_start = (last_open + _TIMEFRAME_DURATIONS[timeframe]) if last_open is not None else pending_candle.open_time

        recovered = await self._provider.fetch_historical_candles(
            normalized, timeframe, gap_start, pending_candle.open_time
        )
        self.bootstrap_candles(normalized, timeframe, recovered, as_of=pending_candle.close_time)

    async def _consume_order_book(self, symbol: str) -> None:
        """Bkz. `_consume_candles` docstring'i — aynı "sessizce yutma"
        önleme disiplini burada da uygulanır."""
        try:
            async for snapshot in self._provider.stream_order_book(symbol, self._order_book_depth):
                if self._stopped:
                    break
                self.ingest_order_book(symbol, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._health.mark_disconnected(symbol)
            raise

    async def stop(self) -> None:
        """Kontrollü, idempotent shutdown (Bölüm 20). Yeni processing kabul
        etmeyi durdurur, tüm consumer task'larını iptal eder (Faz 2
        provider'ın kendi async generator'ları `finally` ile TEMİZ drain
        olur, bkz. modül docstring'i), ardından `provider.close()` çağırır.
        Zaten durdurulmuş bir runtime'da tekrar çağrılması NO-OP'tur —
        in-memory state (pozisyonlar, feature history) KORUNUR."""
        if self._stopped:
            return
        self._stopped = True

        for task in self._tasks:
            task.cancel()
        # `return_exceptions=True`: bir task zaten kendi hata-raporlama
        # disiplinini uyguladı (bkz. `_consume_candles`/`_consume_order_book`
        # — `mark_disconnected()` ile health'e YANSITILDI, sonra re-raise
        # edildi). `stop()`'un işi TÜM task'ları temiz sonlandırmaktır —
        # daha önce zaten gözlemlenebilir hâle getirilmiş bir hatayı burada
        # TEKRAR fırlatmak shutdown'ı gereksiz yere BAŞARISIZ kılar (Bölüm
        # 20 — shutdown'ın KENDİSİ HER ZAMAN temiz tamamlanmalıdır); bu
        # sonuçları görmezden gelmek "sessizce yutmak" DEĞİLDİR çünkü hata
        # zaten `status()` üzerinden gözlemlenebilir hâle getirilmiştir.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        await self._provider.close()


=== FILE: crypto_signal_engine/runtime/errors.py ===
"""
Faz 6 (Real-Time Market Data & Runtime) exception taxonomy.

Kural (Bölüm 19 — talimat): sığ ve yararlı. Yeni bir tip yalnızca gerçek
bir davranış farkı taşıyorsa eklenir. Mevcut hiyerarşiden yeniden
kullanılabilenler (`StaleFeedError`, `AgentInputError`,
`NoLookAheadViolationError`, `IdempotencyConflictError`) burada
TEKRARLANMAZ — çağıran kod doğrudan `crypto_signal_engine.errors`'tan
import eder.
"""

from __future__ import annotations

from crypto_signal_engine.errors import CryptoSignalEngineError


class RuntimeErrorBase(CryptoSignalEngineError):
    """Faz 6 (runtime/orchestration) kaynaklı tüm hataların ortak atası."""


class BootstrapNotReadyError(RuntimeErrorBase):
    """Bootstrap, mevcut Faz 3/4 hesaplamalarının geçerli olması için
    yeterli geçmiş/public veri elde edemedi.

    Bu, sahte/nötr bir Signal üretmek yerine AÇIKÇA raporlanan bir
    "henüz hazır değil" durumudur (Bölüm 9 — "expose an explicit
    NOT_READY state rather than fabricating features/signals")."""


class OutOfOrderMarketEventError(RuntimeErrorBase):
    """Bir piyasa event'i, kendi (symbol, timeframe) sıralama garantisini
    ihlal etti (örn. runtime'ın kendi candle window'una, zaten kabul
    edilmiş bir open_time'dan daha eski bir open_time ile ulaştı).

    Normal akışta bu durum `IngestOutcome.OUT_OF_ORDER` ile SESSİZCE (state
    mutate edilmeden) ele alınır; bu exception yalnızca "bu asla
    olmamalıydı" seviyesindeki programlama-sözleşmesi ihlallerini
    işaretlemek için ayrılmıştır (defensive guard)."""


class IdentityMismatchError(RuntimeErrorBase):
    """Bir domain nesnesinin (Candle/OrderBookSnapshot) GERÇEK identity'si
    (symbol ve/veya timeframe), çağrının BEYAN ETTİĞİ (symbol, timeframe)
    parametreleriyle UYUŞMUYOR (örn. `ingest_candle("BTCUSDT", M5,
    <ETHUSDT M5 candle>)`).

    Bu, runtime sınırlarının çağıranın parametrelerine KÖRÜ KÖRÜNE
    GÜVENMEK yerine, gerçek domain nesnesinin kendi identity'sini
    doğrulamasının bir sonucudur — HER ZAMAN, herhangi bir state mutate
    edilmeden ÖNCE fırlatılır (bkz. `coordinator.py::ingest_candle`,
    `bootstrap_candles`, `ingest_order_book`)."""


=== FILE: crypto_signal_engine/runtime/health.py ===
"""
Faz 6 — runtime sağlık/staleness izleme.

Kural (Bölüm 15 — talimat): "Do not use wall-clock calls scattered
throughout business logic. Inject or centralize time access." Bu modül,
Faz 2'nin ZATEN VAR OLAN `providers.binance.clock.Clock` protokolünü
YENİDEN KULLANIR — yeni bir zaman soyutlaması İCAT EDİLMEZ. Testler
`FixedClock` ile deterministik kalır (gerçek `sleep()`/wall-clock
GEREKMEZ).

Staleness politikası kasıtlı olarak basittir ve tek bir sinyale dayanır:
"bu sembol için en son KABUL EDİLMİŞ (ACCEPTED) piyasa event'inden bu yana
geçen süre, `stale_feed_threshold_seconds`'ı aştı mı?" Bu, hem gerçek bir
WebSocket kopmasını (event akışı durur) hem de sessiz bir "veri geliyor
ama işlenmiyor" durumunu aynı şekilde ve tek bir yerden yakalar — ayrı bir
connection-state polling mekanizması GEREKTİRMEZ.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_finite
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.providers.binance.clock import Clock
from crypto_signal_engine.runtime.models import RuntimeHealth, SymbolHealth


class HealthMonitor:
    """Sembol başına en son event zamanını ve reconnect sayacını izler;
    `status_for()` ile deterministik bir `SymbolHealth` üretir.

    PRIVATE state tamamen instance-scoped'dur (Bölüm 8 — instance
    isolation); iki ayrı `HealthMonitor` hiçbir şey paylaşmaz."""

    def __init__(self, *, clock: Clock, stale_feed_threshold_seconds: float) -> None:
        require_finite(stale_feed_threshold_seconds, "stale_feed_threshold_seconds")
        if stale_feed_threshold_seconds < 0:
            raise ValueError("stale_feed_threshold_seconds negatif olamaz")
        self._clock = clock
        self._threshold = stale_feed_threshold_seconds
        self._last_event_at: dict[str, datetime] = {}
        self._reconnect_count: dict[str, int] = {}
        self._disconnected: set[str] = set()
        self._not_ready: set[str] = set()
        # (symbol) -> {timeframe, ...} için ÇÖZÜLMEMİŞ runtime gap fault'ları.
        # Bu, `_disconnected`'DAN KASITLI OLARAK AYRIDIR: `mark_event()`
        # (örn. ilgisiz bir order-book event'i) BUNU ASLA temizlemez —
        # yalnızca AYNI (symbol, timeframe) için başarılı bir ACCEPTED
        # candle (gap'in bizzat gerçekten çözüldüğünün kanıtı) temizler
        # (bkz. `clear_gap_fault`, `coordinator.py::ingest_candle`).
        self._gap_faults: dict[str, set[Timeframe]] = {}
        # Faz 7 (persistence/recovery) entegrasyonu: bir sembolün durable
        # checkpoint yazımı BAŞARISIZ olduğunda dışarıdan (bkz.
        # `coordinator.py::mark_persistence_fault`) işaretlenir.
        # `(symbol) -> {reason, ...}` — REASON-BAZLI tutulur (örn.
        # "candle_checkpoint" vs "paper_state_checkpoint") çünkü AYNI
        # `ingest_candle()` çağrısı içinde biri BAŞARISIZ olup diğeri
        # BAŞARILI olabilir; tek bir düz `set[str]` olsaydı, ilgisiz bir
        # checkpoint türünün başarısı, DİĞER (hâlâ çözülmemiş) checkpoint
        # türünün hatasını YANLIŞLIKLA MASKELERDİ. `_gap_faults` ile AYNI
        # izolasyon kuralı: yalnızca AYNI (symbol, reason) için BAŞARILI
        # bir sonraki checkpoint yazımı bunu temizler — ilgisiz bir piyasa
        # event'i (`mark_event`) ASLA temizlemez.
        self._persistence_faults: dict[str, set[str]] = {}

    def mark_bootstrapping(self, symbol: str) -> None:
        self._not_ready.add(normalize_symbol(symbol))

    def mark_ready(self, symbol: str) -> None:
        normalized = normalize_symbol(symbol)
        self._not_ready.discard(normalized)
        self._last_event_at[normalized] = self._clock.now()

    def mark_event(self, symbol: str, event_time: datetime) -> None:
        """Kabul edilmiş (ACCEPTED) bir event sonrası çağrılır.

        `event_time` (piyasa event'inin KENDİ zaman damgası) DEĞİL,
        `self._clock.now()` (bu event'in runtime tarafından NE ZAMAN
        işlendiği) kaydedilir — staleness, "veri akışı duruyor mu?"
        sorusuna cevap verir; bu, piyasa event zaman damgasının kendisiyle
        DEĞİL, işleme anıyla ölçülmelidir."""
        normalized = normalize_symbol(symbol)
        self._last_event_at[normalized] = self._clock.now()
        self._disconnected.discard(normalized)

    def mark_disconnected(self, symbol: str) -> None:
        normalized = normalize_symbol(symbol)
        self._disconnected.add(normalized)
        self._reconnect_count[normalized] = self._reconnect_count.get(normalized, 0) + 1

    def mark_gap_fault(self, symbol: str, timeframe: Timeframe) -> None:
        """Bir (symbol, timeframe) için runtime-seviyesi bir gap ÇÖZÜLEMEDİ
        (bkz. `coordinator.py::ingest_candle`/`resolve_gap`) — bu sembol,
        AYNI (symbol, timeframe) için başarılı bir `ACCEPTED` candle
        gelene kadar (bkz. `clear_gap_fault`) DEGRADED kalır."""
        normalized = normalize_symbol(symbol)
        self._gap_faults.setdefault(normalized, set()).add(timeframe)

    def clear_gap_fault(self, symbol: str, timeframe: Timeframe) -> None:
        """Tam olarak o (symbol, timeframe) için continuity YENİDEN
        SAĞLANDIĞINDA (bir `ACCEPTED` candle) çağrılır — başka HİÇBİR
        event türü (order-book dahil) bunu temizleyemez."""
        normalized = normalize_symbol(symbol)
        faults = self._gap_faults.get(normalized)
        if not faults:
            return
        faults.discard(timeframe)
        if not faults:
            del self._gap_faults[normalized]

    def mark_persistence_fault(self, symbol: str, reason: str) -> None:
        """Bir sembol için durable checkpoint yazımı BAŞARISIZ oldu (bkz.
        `persistence/recovery.py::PersistedRuntime`). Runtime, in-memory
        Faz 5 state'ini GERİ ALMAZ (Faz 5 yeniden tasarlanmadan bu mümkün
        değildir) — bunun yerine bu sembolü AÇIKÇA DEGRADED işaretleyerek
        "durable state, in-memory state'in gerisinde kalmış olabilir"
        durumunu fail-closed olarak gözlemlenebilir kılar.

        `reason`, HANGİ checkpoint türünün başarısız olduğunu ayırt eder
        (örn. "candle_checkpoint", "paper_state_checkpoint") — bkz.
        `_persistence_faults` alan yorumu (maskeleme önleme)."""
        normalized = normalize_symbol(symbol)
        self._persistence_faults.setdefault(normalized, set()).add(reason)

    def clear_persistence_fault(self, symbol: str, reason: str) -> None:
        """YALNIZCA AYNI (symbol, reason) için BAŞARILI bir sonraki
        checkpoint yazımından sonra çağrılır — farklı bir reason'ın
        başarısı bunu TEMİZLEMEZ."""
        normalized = normalize_symbol(symbol)
        faults = self._persistence_faults.get(normalized)
        if not faults:
            return
        faults.discard(reason)
        if not faults:
            del self._persistence_faults[normalized]

    def status_for(self, symbol: str) -> SymbolHealth:
        normalized = normalize_symbol(symbol)
        now = self._clock.now()
        last_event = self._last_event_at.get(normalized)
        reconnects = self._reconnect_count.get(normalized, 0)

        if normalized in self._not_ready:
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.BOOTSTRAPPING,
                last_event_at=last_event, reconnect_count=reconnects, detail="bootstrap in progress",
            )
        persistence_faults = self._persistence_faults.get(normalized)
        if persistence_faults:
            reason_list = ", ".join(sorted(persistence_faults))
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.DEGRADED,
                last_event_at=last_event, reconnect_count=reconnects,
                detail=f"durable checkpoint write failed ({reason_list}) — "
                       f"in-memory state may be ahead of durable state",
            )
        gap_faults = self._gap_faults.get(normalized)
        if gap_faults:
            timeframe_list = ", ".join(sorted(tf.value for tf in gap_faults))
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.DEGRADED,
                last_event_at=last_event, reconnect_count=reconnects,
                detail=f"unresolved gap: {timeframe_list}",
            )
        if normalized in self._disconnected:
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.DEGRADED,
                last_event_at=last_event, reconnect_count=reconnects, detail="stream disconnected",
            )
        if last_event is None:
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.DEGRADED,
                last_event_at=None, reconnect_count=reconnects, detail="no event observed yet",
            )

        age_seconds = (now - last_event).total_seconds()
        if age_seconds > self._threshold:
            return SymbolHealth(
                symbol=normalized, health=RuntimeHealth.DEGRADED, last_event_at=last_event,
                reconnect_count=reconnects,
                detail=f"stale: {age_seconds:.1f}s since last event (threshold {self._threshold}s)",
            )
        return SymbolHealth(
            symbol=normalized, health=RuntimeHealth.READY,
            last_event_at=last_event, reconnect_count=reconnects, detail="healthy",
        )


=== FILE: crypto_signal_engine/runtime/models.py ===
"""
Faz 6 (Real-Time Market Data & Runtime) domain modelleri.

Kural: Bu modül Binance'e/execution'a bağımlı DEĞİLDİR. Yalnızca runtime'ın
kendi durum/olay modellerini tanımlar; hiçbir iş mantığı içermez (bkz.
coordinator.py). Tüm dataclass'lar Faz 1'in ortak validation
yardımcılarını (`domain._validation`) yeniden kullanarak kendi
invariant'larını `__post_init__` içinde uygular — bu reponun genelindeki
kuralla (her yeni dataclass kendi kontrolünü ayrı ayrı YAZMAZ, merkezi
yardımcıları kullanır) tutarlıdır.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.models import PaperTradingResult


class RuntimeHealth(str, Enum):
    """Runtime'ın (veya bir sembolün) anlık sağlık durumu.

    Dört durum, talimatın Bölüm 15'te istediği en az dört kategoriyi
    karşılar: BOOTSTRAPPING (STARTING/BOOTSTRAPPING), READY (READY/
    HEALTHY), DEGRADED (DEGRADED/STALE), STOPPED (STOPPED)."""

    BOOTSTRAPPING = "BOOTSTRAPPING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"


class IngestOutcome(str, Enum):
    """Bir piyasa event'inin runtime'ın kendi (symbol, timeframe) penceresine
    kabul edilip edilmediğinin deterministik sonucu (bkz. candle_window.py)."""

    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    UNCLOSED_SKIPPED = "UNCLOSED_SKIPPED"
    GAP_DETECTED = "GAP_DETECTED"


class MarketEventKind(str, Enum):
    CANDLE = "CANDLE"
    ORDER_BOOK = "ORDER_BOOK"


@dataclass(frozen=True)
class RuntimeCycleResult:
    """Bir sembol için tek bir sinyal-değerlendirme döngüsünün sonucu.

    `evaluated=False` — bu döngüde `SignalEngine.evaluate()` veri
    eksikliği (`AgentInputError`, örn. warm-up devam ediyor) nedeniyle
    tamamlanamadı; bu SESSİZCE yutulmaz, açıkça bu alanla raporlanır.
    `evaluated=True` iken `signal`/`paper_result` HER ZAMAN doludur (NEUTRAL
    dahil — NEUTRAL de PaperTradingEngine'e gönderilir, yalnızca NO_ACTION
    sonucu üretir, bkz. Faz 5)."""

    symbol: str
    evaluated: bool
    signal: Signal | None
    paper_result: PaperTradingResult | None
    generated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.generated_at, "generated_at")
        if self.evaluated and (self.signal is None or self.paper_result is None):
            raise ValueError("evaluated=True iken signal ve paper_result dolu olmalı")
        if not self.evaluated and (self.signal is not None or self.paper_result is not None):
            raise ValueError("evaluated=False iken signal ve paper_result None olmalı")


@dataclass(frozen=True)
class ProcessedMarketEvent:
    """`RuntimeCoordinator.ingest_candle()`/`ingest_order_book()`'un tam
    sonucu — gözlemlenebilirlik ve testler için (bkz. Bölüm 18, 23)."""

    symbol: str
    kind: MarketEventKind
    outcome: IngestOutcome
    event_time: datetime
    timeframe: Timeframe | None = None
    cycle_result: RuntimeCycleResult | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.kind, MarketEventKind, "kind")
        require_enum(self.outcome, IngestOutcome, "outcome")
        require_utc_aware(self.event_time, "event_time")
        if self.timeframe is not None:
            require_enum(self.timeframe, Timeframe, "timeframe")


@dataclass(frozen=True)
class BootstrapReport:
    """Tek bir (symbol, timeframe) için bootstrap/gap-fill uygulamasının
    sonucu. `ready=False` asla exception yerine kullanılmaz — çağıran
    (`RuntimeCoordinator.bootstrap()`) bunu health'e yansıtır."""

    symbol: str
    timeframe: Timeframe
    candles_applied: int
    ready: bool
    generated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")
        require_utc_aware(self.generated_at, "generated_at")
        if self.candles_applied < 0:
            raise ValueError("candles_applied negatif olamaz")


@dataclass(frozen=True)
class SymbolHealth:
    """Tek bir sembol için anlık sağlık görünümü."""

    symbol: str
    health: RuntimeHealth
    last_event_at: datetime | None
    reconnect_count: int
    detail: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.health, RuntimeHealth, "health")
        if self.last_event_at is not None:
            require_utc_aware(self.last_event_at, "last_event_at")
        if self.reconnect_count < 0:
            raise ValueError("reconnect_count negatif olamaz")


@dataclass(frozen=True)
class RuntimeStatus:
    """Runtime'ın TÜM sembollerini kapsayan anlık sağlık görünümü.

    `overall_health`, `symbols` içindeki EN KÖTÜ (en ciddi) duruma eşittir
    (bkz. coordinator.py::_worst_health) — tek bir sembolün DEGRADED olması
    tüm runtime'ı DEGRADED olarak raporlar (fail-safe: iyimser bir genel
    durum asla üretilmez)."""

    overall_health: RuntimeHealth
    symbols: tuple[SymbolHealth, ...]
    generated_at: datetime

    def __post_init__(self) -> None:
        require_enum(self.overall_health, RuntimeHealth, "overall_health")
        require_utc_aware(self.generated_at, "generated_at")
        if not isinstance(self.symbols, tuple):
            object.__setattr__(self, "symbols", tuple(self.symbols))


=== FILE: crypto_signal_engine/runtime/reselection_scheduler.py ===
"""
Autonomous Testnet trading lifecycle Phase 16 — the live periodic
reselection scheduler. This is the "hot-reload wiring" the
`selection/reevaluation.py` module docstring says was deliberately left
unbuilt (its own decision logic — `safe_reselect`/`apply_hysteresis` — is
reused here EXACTLY as-is, no reimplementation).

Safety pins (Phase 16's "safety-owned symbols" — never removed by this
scheduler) are computed FRESH on every rescan by the injected
`safety_pinned_provider`, and passed as `apply_hysteresis`'s own
`open_position_symbols` parameter — meaning a pinned symbol is guaranteed
kept by the SAME pure decision function every other symbol goes through,
not a second, separately-trusted mechanism. `remove_symbol` additionally
double-checks (defense in depth) that nothing in `to_remove` is pinned.

A failed rescan (`safe_reselect` already swallows `SymbolSelectionError`
internally and returns the unchanged symbol set) never kills the
scheduler loop — one bad rescan just repeats next interval."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from crypto_signal_engine.providers.binance.clock import Clock, SystemClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig
from crypto_signal_engine.selection.reevaluation import safe_reselect
from crypto_signal_engine.selection.selector import AutomaticSymbolSelector

_LOGGER = logging.getLogger("crypto_signal_engine.runtime.reselection_scheduler")


@dataclass(frozen=True)
class ReselectionStatus:
    armed: bool
    rescan_interval_seconds: float
    last_rescan_at: datetime | None
    next_rescan_at: datetime | None
    last_added: tuple[str, ...]
    last_removed: tuple[str, ...]
    last_error: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "armed": self.armed,
            "rescan_interval_seconds": self.rescan_interval_seconds,
            "last_rescan_at": self.last_rescan_at.isoformat() if self.last_rescan_at is not None else None,
            "next_rescan_at": self.next_rescan_at.isoformat() if self.next_rescan_at is not None else None,
            "last_added": list(self.last_added),
            "last_removed": list(self.last_removed),
            "last_error": self.last_error,
        }


class ReselectionScheduler:
    """Owns ONE background `asyncio.Task` that periodically calls
    `safe_reselect()` and applies the add/remove diff via
    `add_symbol()`/`remove_symbol()` on whatever object was injected as
    `coordinator` (Phase 16). Uses the existing accepted
    `rescan_interval_seconds` default (no artificial shortening) —
    `run_once()` is exposed separately so tests can drive it
    deterministically without waiting on a real sleep.

    PRODUCTION HOT-RESELECTION FIX: `coordinator` need NOT be a bare
    `RuntimeCoordinator` — it may be ANY object exposing async
    `add_symbol(symbol)`/`remove_symbol(symbol)` with the SAME contract.
    In production this is `Application._runtime` — the ACTUAL composed,
    running `BridgeRuntime`/`LifecycleRuntime` (whichever is the real
    top-level runtime for the current configuration) — never a raw,
    disconnected `RuntimeCoordinator` that has no live consumer tasks
    wired to it at all (a raw coordinator's own `add_symbol()` only spawns
    tasks when `coordinator._running` is `True`, which production never
    sets, since production drives the wrapping runtime's `run()` instead —
    see `bridge_runtime.py`/`lifecycle_runtime.py` module docstrings for
    why hot-add/hot-remove must go through the SAME wrapping layer that
    owns the actual live tasks). A bare `RuntimeCoordinator` remains a
    fully valid, supported value here too — for the case where NO
    wrapping layer exists at all (standalone coordinator usage, e.g. in
    tests) — `_current_symbols()` below transparently unwraps whichever
    was given."""

    def __init__(
        self,
        *,
        coordinator: RuntimeCoordinator,
        selector: AutomaticSymbolSelector,
        config: AutoSymbolSelectionConfig,
        safety_pinned_provider: Callable[[], frozenset[str]],
        clock: Clock | None = None,
    ) -> None:
        self._coordinator = coordinator
        self._selector = selector
        self._config = config
        self._safety_pinned_provider = safety_pinned_provider
        self._clock = clock or SystemClock()
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._last_rescan_at: datetime | None = None
        self._next_rescan_at: datetime | None = None
        self._last_added: tuple[str, ...] = ()
        self._last_removed: tuple[str, ...] = ()
        self._last_error: str | None = None

    def start(self) -> None:
        if self._task is not None:
            return
        self._next_rescan_at = self._clock.now() + timedelta(seconds=self._config.rescan_interval_seconds)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self._config.rescan_interval_seconds)
            if self._stopped:
                return
            await self.run_once()

    def _current_symbols(self) -> tuple[str, ...]:
        """Reads the LIVE symbol universe regardless of whether `self.
        _coordinator` is a bare `RuntimeCoordinator` (exposes `_symbols`
        directly) or a wrapping runtime (`BridgeRuntime`/`LifecycleRuntime`
        — both expose a `_coordinator` property to the SAME underlying,
        actually-running `RuntimeCoordinator`, the same private-accessor
        precedent `bridge_runtime.py`/`app.py` already use)."""
        coordinator = getattr(self._coordinator, "_coordinator", self._coordinator)
        return coordinator._symbols  # noqa: SLF001

    async def run_once(self) -> None:
        """One rescan cycle — public so tests (and, if ever needed,
        operator tooling) can trigger it deterministically without a real
        `asyncio.sleep`."""
        current = self._current_symbols()
        safety_pinned = frozenset(self._safety_pinned_provider())
        try:
            new_symbols = await safe_reselect(
                self._selector, current_symbols=current, open_position_symbols=safety_pinned,
                target_count=self._config.target_count,
            )
        except Exception as exc:  # noqa: BLE001 - a rescan failure must NEVER kill the scheduler loop or the runtime
            self._last_error = str(exc)
            _LOGGER.error("reselection: rescan FAILED (universe unchanged, will retry next interval)", exc_info=True)
            self._advance_schedule()
            return

        to_add = tuple(s for s in new_symbols if s not in current)
        to_remove = tuple(s for s in current if s not in new_symbols and s not in safety_pinned)

        for symbol in to_add:
            try:
                await self._coordinator.add_symbol(symbol)
            except Exception:  # noqa: BLE001 - one symbol's add failure must never block others or the loop
                _LOGGER.error("reselection: add_symbol(%s) FAILED (isolated)", symbol, exc_info=True)
        for symbol in to_remove:
            try:
                await self._coordinator.remove_symbol(symbol)
            except Exception:  # noqa: BLE001 - one symbol's remove failure must never block others or the loop
                _LOGGER.error("reselection: remove_symbol(%s) FAILED (isolated)", symbol, exc_info=True)

        self._last_added = to_add
        self._last_removed = to_remove
        self._last_error = None
        if to_add or to_remove:
            _LOGGER.info("reselection: added=%s removed=%s", list(to_add), list(to_remove))
        self._advance_schedule()

    def _advance_schedule(self) -> None:
        now = self._clock.now()
        self._last_rescan_at = now
        self._next_rescan_at = now + timedelta(seconds=self._config.rescan_interval_seconds)

    def status(self) -> ReselectionStatus:
        return ReselectionStatus(
            armed=self._task is not None and not self._stopped,
            rescan_interval_seconds=self._config.rescan_interval_seconds,
            last_rescan_at=self._last_rescan_at, next_rescan_at=self._next_rescan_at,
            last_added=self._last_added, last_removed=self._last_removed, last_error=self._last_error,
        )


