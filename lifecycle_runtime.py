"""
`LifecycleRuntime` — the M1-candle-driven lifecycle evaluation loop
(Phase 6/8/9/12). Wraps `BridgeRuntime` the SAME way `BridgeRuntime` wraps
`PersistedRuntime` (an additional layer, never editing the wrapped class —
see `bridge_runtime.py`'s own module docstring for the precedent).

CRITICAL ISOLATION (Phase 9/12): this subscribes to the market-data
provider's M1 candle stream DIRECTLY — it NEVER calls
`RuntimeCoordinator.ingest_candle()`. That means M1 candles never touch
`FeatureEngine`/`SignalEngine`/`PaperTradingEngine` at all: the existing
accepted PAPER simulation and M5/M15/H1 signal cadence are byte-for-byte
unaffected by this module's existence. This is what lets Phase 6's "no
per-second REST polling, evaluate on M1 candle close" requirement be met
without touching the already-accepted signal pipeline in any way.

Per-symbol execution-activation firewall (Phase 12): a freshly (re)started
runtime, or a freshly hot-added symbol, must not fire on stale
already-closed M1 candles from before this process/symbol was live — this
is enforced structurally by only ever consuming the LIVE websocket stream
here (never a backfill/bootstrap replay), exactly like `BridgeRuntime`
already does for the signal path."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle as DomainCandle
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager

_LOGGER = logging.getLogger("crypto_signal_engine.execution.lifecycle_runtime")

_ATR_FEATURE_NAME = "ATR_14"
# The M5 ATR reference is considered stale (Phase 9 "fail closed for that
# trigger only") once it is older than this multiple of the M5 interval —
# two missed M5 closes is a generous, explainable staleness bound that
# tolerates one transient gap without needlessly disabling trailing.
_ATR_STALENESS_M5_MULTIPLES = 2
_M5_INTERVAL = timedelta(minutes=5)


def _to_lifecycle_candle(candle: DomainCandle) -> LifecycleCandle:
    return LifecycleCandle(
        open=candle.open, high=candle.high, low=candle.low, close=candle.close, close_time=candle.close_time,
    )


def latest_m5_atr(coordinator, symbol: str, *, now) -> float | None:  # noqa: ANN001
    """Shared by `LifecycleRuntime` (per-M1-candle trailing recompute) and
    `app.py`'s entry-time `atr_provider` — a single staleness/lookup policy
    for "the latest valid M5 ATR" (Phase 9), reached via the SAME private
    `coordinator._feature_engine` accessor `bridge_runtime.py` already
    uses for `coordinator._symbols`/`coordinator._provider` (existing
    precedent, not a new anti-pattern)."""
    try:
        snapshot = coordinator._feature_engine.latest_snapshot(symbol, Timeframe.M5)
    except Exception:  # noqa: BLE001 - ATR yoksa fail-closed (skip), sinyal/paper etkilenmez
        return None
    if snapshot is None:
        return None
    age = now - snapshot.as_of
    if age > _M5_INTERVAL * _ATR_STALENESS_M5_MULTIPLES:
        _LOGGER.warning(
            "lifecycle: %s M5 ATR STALE (age=%s, snapshot.as_of=%s) — treated as unavailable", symbol, age, snapshot.as_of,
        )
        return None
    value = snapshot.values.get(_ATR_FEATURE_NAME)
    if value is None or value <= 0:
        return None
    return value


class LifecycleRuntime:
    """Wraps a `BridgeRuntime` and adds one independent M1 candle consumer
    task per symbol, feeding `LifecycleManager.evaluate_m1_candle()`.
    `run()`/`stop()` compose with the wrapped runtime's own task
    lifecycle — the M1 tasks are tracked separately here and always
    cancelled in `stop()`, so a `LifecycleRuntime.stop()` leaves no
    dangling subscriptions."""

    def __init__(
        self, bridge_runtime: BridgeRuntime, lifecycle_manager: LifecycleManager | None,
        *, m1_candle_observer: Callable[[str, LifecycleCandle], None] | None = None,
    ) -> None:
        self._bridge_runtime = bridge_runtime
        self._lifecycle_manager = lifecycle_manager
        # Adaptive Intelligence v1 — M1-fidelity shadow-evaluation fix
        # (independent-verification round). Additive, optional, exact
        # same defensive-wrap discipline as `RuntimeCoordinator.
        # candle_observer`/`order_book_observer` (Karar 87): `None`
        # (default) has ZERO effect on any existing behaviour. When
        # supplied, called ONCE PER REAL, ALREADY-CLOSED M1 candle — the
        # SAME frequency `LifecycleManager.evaluate_m1_candle()` itself
        # uses for the real position — purely observationally, AFTER the
        # real evaluation attempt (whether it succeeded or was isolated
        # by the `except Exception` below), never altering this loop's
        # own control flow or timing. This exists because
        # `RuntimeCoordinator.ingest_candle`'s `candle_observer` ONLY
        # ever sees M5/M15/H1 — live M1 candles bypass `RuntimeCoordinator`
        # entirely via `_consume_m1` below, so a shadow evaluator wired
        # to that M5-level hook alone would run at 5-minute-aggregated
        # granularity, not the true 1-minute granularity the real
        # position is evaluated at — see `adaptive.shadow.ShadowMonitor.
        # on_m1_candle` for the consumer.
        self._m1_candle_observer = m1_candle_observer
        self._m1_tasks: list[asyncio.Task] = []
        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol M1 task
        # ownership, additive to `self._m1_tasks` (existing tests assert
        # that flat list directly; `stop()`'s full-shutdown cancellation
        # stays unchanged). This is the CONFIRMED BLOCKER's part (b): a
        # hot-added symbol previously never got an M1 stop/target/trailing
        # evaluator task AT ALL, since `run()` only ever iterated
        # `coordinator._symbols` ONCE, at call time.
        self._m1_tasks_by_symbol: dict[str, asyncio.Task] = {}

    @property
    def _coordinator(self):  # noqa: ANN202 - aynı erişim sözleşmesi (bkz. BridgeRuntime)
        return self._bridge_runtime._coordinator

    async def recover(self):  # noqa: ANN201
        return await self._bridge_runtime.recover()

    def status(self):  # noqa: ANN201
        return self._bridge_runtime.status()

    def ingest_candle(self, symbol, timeframe, candle):  # noqa: ANN001, ANN201
        return self._bridge_runtime.ingest_candle(symbol, timeframe, candle)

    def ingest_order_book(self, symbol, snapshot):  # noqa: ANN001, ANN201
        return self._bridge_runtime.ingest_order_book(symbol, snapshot)

    async def resolve_gap(self, symbol, timeframe, pending_candle):  # noqa: ANN001, ANN201
        return await self._bridge_runtime.resolve_gap(symbol, timeframe, pending_candle)

    def bootstrap_candles(self, symbol, timeframe, candles, as_of):  # noqa: ANN001, ANN201
        return self._bridge_runtime.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    def _latest_m5_atr(self, symbol: str, *, now) -> float | None:  # noqa: ANN001
        if self._lifecycle_manager is None:
            return None
        return latest_m5_atr(self._coordinator, symbol, now=now)

    async def _consume_m1(self, symbol: str) -> None:
        coordinator = self._coordinator
        try:
            async for candle in coordinator._provider.stream_candles(symbol, Timeframe.M1):
                if coordinator._stopped:
                    break
                if not candle.is_closed:
                    continue
                if self._lifecycle_manager is None:
                    continue
                lifecycle_candle = _to_lifecycle_candle(candle)
                atr = self._latest_m5_atr(symbol, now=candle.close_time)
                try:
                    await self._lifecycle_manager.evaluate_m1_candle(
                        symbol, lifecycle_candle, atr_for_trailing=atr,
                    )
                except Exception:  # noqa: BLE001 - bir sembolün lifecycle hatası DİĞER sembolleri/runtime'ı ASLA etkilemez
                    _LOGGER.error("lifecycle: M1 evaluation FAILED for %s (isolated)", symbol, exc_info=True)
                if self._m1_candle_observer is not None:
                    # Bkz. __init__ yorumu — TÜM gerçek M1 değerlendirmesi
                    # ZATEN tamamlandıktan SONRA (başarılı ya da izole
                    # edilmiş), salt-gözlemsel olarak çağrılır; geniş
                    # try/except KASITLI (aynı gerekçe: bir observer hatası
                    # bu döngüyü ASLA etkileyemez).
                    try:
                        self._m1_candle_observer(symbol, lifecycle_candle)
                    except Exception:  # noqa: BLE001 - observer hatası M1 tüketim döngüsünü ASLA etkilemez
                        _LOGGER.error("lifecycle: M1 candle observer FAILED for %s (isolated)", symbol, exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error("lifecycle: M1 stream FAILED for %s", symbol, exc_info=True)

    def _spawn_m1_task(self, symbol: str) -> None:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same M1-task-creation
        code path — never a second implementation that could drift. A
        no-op when no `LifecycleManager` is wired (mirrors `run()`'s own
        existing guard — PAPER/bridge-only behavior is unaffected)."""
        if self._lifecycle_manager is None:
            return
        task = asyncio.create_task(self._consume_m1(symbol))
        self._m1_tasks.append(task)
        self._m1_tasks_by_symbol[symbol] = task

    async def run(self) -> None:
        if self._lifecycle_manager is not None:
            coordinator = self._coordinator
            for symbol in coordinator._symbols:
                self._spawn_m1_task(symbol)
        await self._bridge_runtime.run()

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: this is the ONE correct
        production surface for the CONFIRMED BLOCKER's part (b) — a
        hot-added symbol previously NEVER got an M1 stop/target/trailing
        evaluator task, meaning a position it later opened would sit
        completely unprotected. Delegates bootstrap/state establishment
        AND bridge-aware candle/order-book task creation to
        `self._bridge_runtime.add_symbol()` (already idempotent), then
        additionally spawns this symbol's OWN M1 task — symmetric to how
        `run()` composes with `self._bridge_runtime.run()`. Idempotent:
        a symbol that already has an active M1 task is never given a
        second one."""
        normalized = normalize_symbol(symbol)
        await self._bridge_runtime.add_symbol(normalized)
        if normalized not in self._m1_tasks_by_symbol:
            self._spawn_m1_task(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cancels JUST this
        symbol's M1 task first (symmetric to `add_symbol` spawning it
        last), then delegates to `self._bridge_runtime.remove_symbol()`
        for the candle/order-book layer + state cleanup. Idempotent:
        removing a symbol with no active M1 task here is a no-op for this
        layer (the delegated call below still proceeds)."""
        normalized = normalize_symbol(symbol)
        task = self._m1_tasks_by_symbol.pop(normalized, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._m1_tasks = [t for t in self._m1_tasks if t is not task]
        await self._bridge_runtime.remove_symbol(normalized)

    async def stop(self) -> None:
        for task in self._m1_tasks:
            task.cancel()
        for task in self._m1_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._m1_tasks.clear()
        self._m1_tasks_by_symbol.clear()
        await self._bridge_runtime.stop()
