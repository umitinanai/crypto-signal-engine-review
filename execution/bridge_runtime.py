"""
Faz 13 — `BridgeRuntime`: `PersistedRuntime`'ı (Faz 7, DEĞİŞTİRİLMEDEN)
SARAN, kabul edilmiş her CANLI sinyal-değerlendirme döngüsünden HEMEN
SONRA `SignalTestnetBridge.on_cycle_result()`'ı çağıran bir Faz 13
wrapper'ı — `PersistedRuntime`'ın Faz 6 `RuntimeCoordinator`'ı SARDIĞI
AYNI desenle (bkz. persistence/recovery.py modül docstring'i).

NEDEN AYRI BİR SARMALAYICI (Faz 6/7 dosyaları DEĞİŞTİRİLMEZ): talimat
KASITLI OLARAK "Do not directly POST orders from RuntimeCoordinator" ve
"Do NOT reopen Phases 1-12" der — bu yüzden bridge çağrısı, KOORDİNATÖRÜN
(Faz 6) veya `PersistedRuntime`'ın (Faz 7) İÇİNE GÖMÜLMEZ, onun YERİNE bir
ÜÇÜNCÜ katman eklenir. Bu, repo'nun ZATEN kabul ettiği "wrap, don't
modify" mimari disiplinin (bkz. `PersistedRuntime`'ın KENDİSİ) doğrudan bir
devamıdır.

TARİHSEL REPLAY KORUMASI (Bölüm "NO HISTORICAL REPLAY ORDERS"): `recover()`
BURADA `self._persisted.recover()`'a SAF olarak delege edilir — `Persisted
Runtime.recover()` (Faz 7) ASLA `SignalEngine.evaluate()` çağırmadığı için
(bootstrap yalnızca candle/feature state'i yeniden inşa eder, YENİ bir
sinyal/paper-trade ÜRETMEZ), `recover()` sırasında bridge'e TEK bir
`on_cycle_result()` çağrısı bile ULAŞMAZ — bu YAPISAL bir garanti, ayrı bir
"eğer startup ise atla" bayrağı GEREKMEZ. Bridge, YALNIZCA bu sınıfın KENDİ
`_consume_candles`/`resolve_gap` (canlı akış) yollarından beslenir."""

from __future__ import annotations

import asyncio

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome, ProcessedMarketEvent


class BridgeRuntime:
    """`PersistedRuntime`'ı (Faz 7) saran, isteğe bağlı bir `SignalTestnetBridge`
    (Faz 13) enjekte edilebilen runtime katmanı. `bridge is None` iken bu
    sınıf `PersistedRuntime`'a SAF bir pass-through'dur — hiçbir davranış
    DEĞİŞMEZ (Application, bridge devre dışıyken bu sınıfı HİÇ kullanmaz,
    bkz. `app.py`, ama pass-through modu yine de test edilir/korunur)."""

    def __init__(self, persisted: PersistedRuntime, bridge: SignalTestnetBridge | None) -> None:
        self._persisted = persisted
        self._bridge = bridge
        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol task
        # ownership for THIS layer's bridge-aware consumer tasks (never the
        # bare `RuntimeCoordinator._consume_candles`/`_consume_order_book`
        # — that pair never calls `_maybe_bridge`, see module docstring).
        # Same `self._tasks_by_symbol` pattern already accepted in
        # `RuntimeCoordinator`/`PersistedRuntime`.
        self._tasks_by_symbol: dict[str, list[asyncio.Task]] = {}

    @property
    def _coordinator(self):  # noqa: ANN202 - `Application`'ın mevcut `self.runtime._coordinator` erişimiyle AYNI sözleşme
        return self._persisted._coordinator

    async def recover(self) -> tuple[BootstrapReport, ...]:
        return await self._persisted.recover()

    def status(self):  # noqa: ANN201
        return self._persisted.status()

    # -- Canlı ingestion (bridge'e beslenen TEK yol) ---------------------------

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> ProcessedMarketEvent:
        return self._persisted.ingest_candle(symbol, timeframe, candle)

    def ingest_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> ProcessedMarketEvent:
        # Order-book event'leri hiçbir sinyal değerlendirmesi TETİKLEMEZ
        # (Faz 6, DEĞİŞMEDİ) — bridge için hiçbir şey YAPILMAZ.
        return self._persisted.ingest_order_book(symbol, snapshot)

    async def _maybe_bridge(self, event: ProcessedMarketEvent) -> None:
        if self._bridge is None:
            return
        if event.outcome is not IngestOutcome.ACCEPTED or event.cycle_result is None:
            return
        await self._bridge.on_cycle_result(event.cycle_result)

    async def resolve_gap(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> ProcessedMarketEvent:
        event = await self._persisted.resolve_gap(symbol, timeframe, pending_candle)
        await self._maybe_bridge(event)
        return event

    def bootstrap_candles(self, symbol: str, timeframe: Timeframe, candles: list[Candle], as_of) -> BootstrapReport:  # noqa: ANN001
        return self._persisted.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    # -- Async run/stop (bridge'in beslendiği KENDİ tüketim döngüsü) -----------

    async def run(self) -> None:
        """`PersistedRuntime.run()`'ı (Faz 7) DOĞRUDAN kullanmaz — o kendi
        `self.ingest_candle`'ını (bridge'i ASLA çağırmayan) kullanır. Bu
        yüzden `BridgeRuntime`, AYNI akış şeklini, ama `self.ingest_candle`
        SONRASINDA `await self._maybe_bridge(event)` ÇAĞIRACAK şekilde
        yeniden sağlar (`PersistedRuntime`'ın Faz 6 `RuntimeCoordinator.run()`'ı
        SARDIĞI AYNI desen, bkz. modül docstring'i)."""
        coordinator = self._persisted._coordinator
        if coordinator._stopped:
            raise ValueError("stopped bir BridgeRuntime tekrar run() ile başlatılamaz")

        tasks: list[asyncio.Task] = []
        for symbol in coordinator._symbols:
            tasks.extend(self._spawn_symbol_tasks(symbol))

        await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn_symbol_tasks(self, symbol: str) -> list[asyncio.Task]:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same bridge-aware
        task-creation code path — never a second implementation that could
        drift, and NEVER the bare `RuntimeCoordinator._spawn_symbol_tasks`
        (which calls the coordinator's own `_consume_candles`/
        `_consume_order_book` — those never reach `_maybe_bridge`, see
        module docstring). Tracks the created tasks BOTH in the flat
        `coordinator._tasks` (so `coordinator.stop()`'s existing
        full-shutdown cancellation is completely unchanged) AND in
        `self._tasks_by_symbol` (so `remove_symbol` can cancel just this
        symbol's own tasks)."""
        coordinator = self._persisted._coordinator
        created: list[asyncio.Task] = []
        for timeframe in coordinator._candle_timeframes:
            created.append(asyncio.create_task(self._consume_candles(symbol, timeframe)))
        created.append(asyncio.create_task(self._consume_order_book(symbol)))
        coordinator._tasks.extend(created)
        self._tasks_by_symbol[symbol] = created
        return created

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: hot-adds a symbol's OWN
        bridge-aware candle/order-book consumer tasks — this is the ONE
        correct production surface for the CONFIRMED BLOCKER's part (a)
        (a hot-added symbol previously got NO live candle/order-book
        consumption at all when `BridgeRuntime` is the actual production
        composition, since `RuntimeCoordinator.add_symbol()`'s own
        task-spawn only fires when `coordinator._running` is `True`, which
        production never sets). Idempotent: a symbol that already has
        active task ownership here is a no-op (never a duplicate
        subscription).

        Bootstrap/state establishment is delegated ONCE, DIRECTLY to
        `RuntimeCoordinator.add_symbol()` (bypassing `PersistedRuntime.
        add_symbol()` entirely) — `coordinator.add_symbol()` only spawns
        ITS OWN bare tasks when `coordinator._running` is `True`, which is
        structurally never the case in production, so this call is
        guaranteed to be state+bootstrap ONLY: never a second, competing
        set of bare (non-bridge-aware) consumer tasks."""
        coordinator = self._persisted._coordinator
        normalized = normalize_symbol(symbol)
        if normalized in self._tasks_by_symbol:
            return
        await coordinator.add_symbol(normalized)
        if coordinator._stopped:
            return
        self._spawn_symbol_tasks(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cleanly cancels JUST
        this symbol's own bridge-aware tasks, then delegates state cleanup
        DIRECTLY to `RuntimeCoordinator.remove_symbol()` (same bypass of
        `PersistedRuntime.remove_symbol()` as `add_symbol()`, for the same
        reason — this layer owns the actual production tasks, not
        `PersistedRuntime`). Idempotent: removing a symbol with no active
        task ownership here is a no-op."""
        coordinator = self._persisted._coordinator
        normalized = normalize_symbol(symbol)
        tasks = self._tasks_by_symbol.pop(normalized, [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        coordinator._tasks = [t for t in coordinator._tasks if t not in tasks]
        await coordinator.remove_symbol(normalized)

    async def _consume_candles(self, symbol: str, timeframe: Timeframe) -> None:
        coordinator = self._persisted._coordinator
        try:
            async for candle in coordinator._provider.stream_candles(symbol, timeframe):
                if coordinator._stopped:
                    break
                event = self.ingest_candle(symbol, timeframe, candle)
                if event.outcome is IngestOutcome.GAP_DETECTED:
                    await self.resolve_gap(symbol, timeframe, candle)
                else:
                    await self._maybe_bridge(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            coordinator.mark_disconnected(symbol)
            raise

    async def _consume_order_book(self, symbol: str) -> None:
        coordinator = self._persisted._coordinator
        try:
            async for snapshot in coordinator._provider.stream_order_book(symbol, coordinator._order_book_depth):
                if coordinator._stopped:
                    break
                self.ingest_order_book(symbol, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            coordinator.mark_disconnected(symbol)
            raise

    async def stop(self) -> None:
        await self._persisted.stop()
        self._tasks_by_symbol.clear()
