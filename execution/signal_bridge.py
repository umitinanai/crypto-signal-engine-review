"""
Faz 13 — Signal -> Binance Spot TESTNET execution bridge.

Bu modül YENİ bir trading stratejisi DEĞİLDİR: Quant/MarketStructure/
OrderBook/Regime/Consensus/Risk mantığına HİÇ dokunmaz, `SignalDirection`in
KENDİ anlamını DEĞİŞTİRMEZ. Tek sorumluluğu: kabul edilmiş sinyal/runtime
pipeline'ının ZATEN ürettiği bir `RuntimeCycleResult`'ı (Faz 6), Binance
SPOT-only bir yürütme POLİTİKASI üzerinden, Faz 10/11'in ZATEN kabul
edilmiş bileşenlerine (`TestnetExecutionAdapter`, `ExecutionReconciliationService`,
deterministik `client_order_id`, HIGH-5 monotonik durable store)
DELEGE ETMEKTİR — yeni bir order-submission kod yolu İCAT EDİLMEZ, yalnızca
mevcut `ExecutionReconciliationService.submit()` çağrılır.

SPOT-ONLY POLİTİKA (Bölüm "SPOT-ONLY EXECUTION POLICY"):
- LONG-family sinyal + bridge FLAT  -> BUY (küçük sabit notional).
- LONG-family sinyal + bridge LONG  -> NO_ACTION (churn yok).
- SHORT-family sinyal + bridge LONG -> SELL, TAM OLARAK bridge-owned
  miktar (asla daha fazlası, asla hesabın ilgisiz/manuel bakiyesi).
- SHORT-family sinyal + bridge FLAT -> NO_ACTION (sentetik short YOK).
- NEUTRAL -> NO_ACTION.

BRIDGE-OWNED ENVANTER (Bölüm "BRIDGE-OWNED INVENTORY — CRITICAL"): ayrı bir
mutable ledger İNŞA EDİLMEZ — bridge'in sahip olduğu miktar, HER ZAMAN
`ExecutionStateStore`'daki (Faz 11, ZATEN durable/HIGH-5-monotonik)
KENDİ (bridge-namespaced `context_id` — bkz. `bridge_context_id()`) execution
kayıtlarından, TERMİNAL `FILLED` durumdaki `executed_quantity`
toplamından (`BUY` +, `SELL` -) YENİDEN İNŞA edilir (bkz.
`compute_bridge_position`). Bu, restart/reconciliation'dan OTOMATİK olarak
hayatta kalır (Faz 11'in ZATEN kanıtlanmış durable garantisiyle AYNI) ve
manuel `scripts/binance_testnet_lab.py` kaydlarını/hesabın mevcut genel
bakiyesini ASLA karıştırmaz (context_id namespace'i ile YAPISAL olarak
izole, bkz. `bridge_context_id`).

TARİHSEL REPLAY KORUMASI: bu modülün TEK giriş noktası `on_cycle_result()`,
YALNIZCA `bridge_runtime.py::BridgeRuntime`'ın CANLI candle-ingestion
yolundan çağrılır — `PersistedRuntime.recover()` (Faz 7, DEĞİŞTİRİLMEDEN)
ASLA `SignalEngine.evaluate()` çağırmadığı için (bkz. persistence/
recovery.py docstring'i), restart sonrası geri yüklenen HİÇBİR eski Paper
pozisyonu/sinyali bu fonksiyona ASLA ULAŞMAZ — yapısal olarak İMKANSIZDIR,
bu modülde AYRICA bir "startup guard" YAZILMASINA gerek YOKTUR."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from datetime import datetime
from typing import TYPE_CHECKING

from crypto_signal_engine.domain.enums import SignalDirection
from crypto_signal_engine.errors import ExecutionError
from crypto_signal_engine.execution.adapter import TestnetExecutionAdapter
from crypto_signal_engine.execution.errors import ExecutionPersistenceError
from crypto_signal_engine.execution.lifecycle import resolve_lot_size_filter
from crypto_signal_engine.execution.models import ExecutionResult, OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, ExecutionRecord
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock
from crypto_signal_engine.runtime.models import RuntimeCycleResult

if TYPE_CHECKING:
    # Import-cycle avoidance: `lifecycle_manager.py` imports `bridge_context_id`/
    # `_ACTION_CLOSE` FROM this module, so this module cannot import it at
    # runtime — the dependency only exists for type annotations, which
    # `from __future__ import annotations` already makes lazy/string-only.
    from crypto_signal_engine.execution.lifecycle import ExitReason
    from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager

_LOGGER = logging.getLogger("crypto_signal_engine.execution.signal_bridge")

_LONG_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_LONG, SignalDirection.LONG, SignalDirection.WEAK_LONG}
)
_SHORT_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_SHORT, SignalDirection.SHORT, SignalDirection.WEAK_SHORT}
)

_BRIDGE_NAMESPACE = "bridge"
_ACTION_OPEN = "OPEN"
_ACTION_CLOSE = "CLOSE"

SPOT_LONG_ONLY_POLICY = "SPOT_LONG_ONLY"


def bridge_context_id(symbol: str, action: str, signal_context_id: str) -> str:
    """Deterministik, bridge'e-özel bir `context_id` üretir — AYNI sinyal
    context'i (`signal_context_id`) HER ZAMAN AYNI bridge context_id'yi
    üretir (idempotency, Faz 5/11'in KENDİ `context_id` ilkesiyle AYNI),
    ve `{_BRIDGE_NAMESPACE}:{symbol}:` prefix'i, bu kaydın manuel
    `scripts/binance_testnet_lab.py` çağrılarından VEYA hesabın diğer
    faaliyetlerinden YAPISAL olarak AYRIŞTIRILMASINI sağlar (bkz.
    `compute_bridge_position` — yalnızca bu prefix'e sahip kayıtlar
    bridge-owned envantere DAHİL edilir)."""
    return f"{_BRIDGE_NAMESPACE}:{symbol}:{action}:{signal_context_id}"


def _is_bridge_record(record: ExecutionRecord, symbol: str) -> bool:
    return record.context_id.startswith(f"{_BRIDGE_NAMESPACE}:{symbol}:")


@dataclass(frozen=True)
class BridgePosition:
    """Bir sembol için, YALNIZCA bridge'in KENDİ execution kayıtlarından
    yeniden inşa edilmiş envanter görünümü. `ambiguous=True` iken
    `owned_quantity` GÜVENİLİR DEĞİLDİR (çağıran hiçbir yeni ekonomik
    karar ÜRETMEMELİDİR — bkz. `SignalTestnetBridge._decide_and_act`)."""

    symbol: str
    owned_quantity: float
    ambiguous: bool
    unresolved_context_ids: tuple[str, ...] = ()


def compute_bridge_position(store: ExecutionStateStore, symbol: str) -> BridgePosition:
    """`bridge_owned_quantity >= 0` invariant'ını HER ZAMAN sağlar (negatif
    bir toplam asla döndürülmez — politika zaten sahip olunandan fazlasını
    SATMAZ, ama defense-in-depth olarak burada da clamp edilir).

    Herhangi bir bridge-namespaced kayıt TERMİNAL DEĞİLSE (`SUBMISSION_
    ATTEMPTED`/`AMBIGUOUS`/`ACKNOWLEDGED`/`PARTIALLY_FILLED`/
    `UNKNOWN_NOT_FOUND`), bu sembol için `ambiguous=True` döner — bridge
    bu sembol için gerçek envanterini KANITLAYAMAZ, dolayısıyla HİÇBİR YENİ
    ekonomik karar (BUY/SELL) VEREMEZ (fail-closed)."""
    records = [r for r in store.list_for_symbol(symbol) if _is_bridge_record(r, symbol)]
    unresolved = tuple(r.context_id for r in records if not r.is_terminal())
    if unresolved:
        return BridgePosition(symbol=symbol, owned_quantity=0.0, ambiguous=True, unresolved_context_ids=unresolved)

    owned = 0.0
    for record in records:
        if record.lifecycle_state != ExecutionLifecycleState.FILLED:
            continue
        if record.side is OrderSide.BUY:
            owned += record.executed_quantity
        else:
            owned -= record.executed_quantity
    return BridgePosition(symbol=symbol, owned_quantity=max(owned, 0.0), ambiguous=False)


def _floor_to_step(value: float, step: float) -> float:
    """`value`'yu `step`'in katlarına AŞAĞI yuvarlar (asla YUKARI — bir
    SELL miktarının bridge-owned miktarı AŞMASI YAPISAL olarak İMKANSIZ
    olmalıdır). `Decimal` kullanılır (ham `float` bölmesi/çarpımı, step
    hizalamasında yanlış-negatif/yanlış-pozitif kayan-nokta hatalarına yol
    açabilir)."""
    if step <= 0:
        return value
    step_dec = Decimal(str(step))
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step_dec)


@dataclass(frozen=True)
class BridgeSymbolStatus:
    """Faz 13 gözlemlenebilirlik — bkz. `SignalTestnetBridge.status_for()`.
    Hiçbir alan credential/secret TAŞIMAZ."""

    symbol: str
    policy: str
    owned_quantity: float | None
    last_signal_context_id: str | None
    last_signal_direction: str | None
    last_action: str
    last_action_detail: str
    last_client_order_id: str | None
    last_lifecycle_state: str | None
    ambiguous: bool
    updated_at: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "policy": self.policy,
            "owned_quantity": self.owned_quantity,
            "last_signal_context_id": self.last_signal_context_id,
            "last_signal_direction": self.last_signal_direction,
            "last_action": self.last_action,
            "last_action_detail": self.last_action_detail,
            "last_client_order_id": self.last_client_order_id,
            "last_lifecycle_state": self.last_lifecycle_state,
            "ambiguous": self.ambiguous,
            "updated_at": self.updated_at,
        }


class SignalTestnetBridge:
    """Faz 13'ün TEK yüksek-seviye giriş noktası: `on_cycle_result()`.

    `mark_operational(True, ...)` YALNIZCA `app.py::Application`'ın
    startup execution reconciliation'ı BAŞARIYLA tamamlandıktan SONRA
    çağrılır — bu çağrıya kadar (VEYA reconciliation BAŞARISIZ olursa)
    `_operational=False` KALIR ve `on_cycle_result()` HİÇBİR order
    göndermez (fail-closed, bkz. modül docstring'i)."""

    POLICY = SPOT_LONG_ONLY_POLICY

    def __init__(
        self,
        *,
        execution_service: ExecutionReconciliationService,
        execution_store: ExecutionStateStore,
        notional_usdt: float,
        clock: Clock | None = None,
        lifecycle_manager: LifecycleManager | None = None,
        atr_provider: Callable[[str], float | None] | None = None,
        all_symbols_provider: Callable[[], tuple[str, ...]] | None = None,
        entries_paused_provider: Callable[[], bool] | None = None,
    ) -> None:
        """`lifecycle_manager`/`atr_provider`/`all_symbols_provider` are all
        `None` by default — with `lifecycle_manager=None`, every code path
        below is byte-for-byte the pre-autonomous-lifecycle behavior (the
        existing accepted opposite-signal-only bridge). When
        `lifecycle_manager` is provided (Application wires it whenever the
        bridge is enabled — see `app.py`), entries additionally pass
        `lifecycle_manager.entry_gate()` and initialize a durable LONG
        position (`on_entry_filled`), and opposite-signal exits route
        through `lifecycle_manager.attempt_exit()` instead of this class's
        own `_submit_sell` — the SAME shared per-symbol lock
        (`lifecycle_manager.lock_for`) is then used for `_lock_for()` too,
        so the M1 stop/target/trailing evaluator and this signal-driven
        exit path can never race each other (Phase 7)."""
        if not (notional_usdt > 0):
            raise ValueError(f"notional_usdt pozitif olmalı, alınan: {notional_usdt}")
        self._service = execution_service
        self._store = execution_store
        self._adapter = TestnetExecutionAdapter(execution_service.client)
        self._notional = notional_usdt
        self._clock = clock or SystemClock()
        self._operational = False
        self._operational_detail = "bridge not yet marked operational (startup reconciliation pending)"
        self._symbol_locks: dict[str, asyncio.Lock] = {}
        self._status: dict[str, BridgeSymbolStatus] = {}
        self._lifecycle_manager = lifecycle_manager
        self._atr_provider = atr_provider
        self._all_symbols_provider = all_symbols_provider
        # 24/7 Ops v1, Step 3 — Control Center pause gate. `None` (every
        # existing caller) means "never paused", bit-for-bit identical to
        # before this parameter existed. When provided (only by
        # `app.py::Application` once `dashboard_admin_token` is set), it
        # reads a `threading.Event.is_set()` (see `ops/admin.py`) — an
        # INDEPENDENT gate from `LifecycleManager.entry_gate()`'s own
        # three risk gates, checked here, BEFORE `entry_gate()` is even
        # called, and never applied to an already-open position's
        # management (that code path never reaches this check at all).
        self._entries_paused_provider = entries_paused_provider

    # -- Operational gate (Application startup wires this) -----------------

    def mark_operational(self, ok: bool, detail: str) -> None:
        self._operational = ok
        self._operational_detail = detail

    @property
    def operational(self) -> bool:
        return self._operational

    @property
    def operational_detail(self) -> str:
        return self._operational_detail

    # -- Observability -------------------------------------------------------

    def status_for(self, symbol: str) -> dict[str, object]:
        """Bu process'te bridge HENÜZ bu sembol için bir sinyal İŞLEMEDİYSE
        (örn. restart HEMEN sonrası, ilk sinyalden ÖNCE) `last_action`
        alanları hâlâ "NONE"'dır (in-memory karar geçmişi, restart'ta
        DOĞAL olarak boşalır) — ama `owned_quantity`, YİNE DE durable
        store'dan (bkz. `compute_bridge_position`) TAZE olarak okunur, bu
        yüzden restart sonrası envanter GÖZLEMLENEBİLİRLİĞİ bir sonraki
        sinyali BEKLEMEK ZORUNDA DEĞİLDİR (bkz. Bölüm "OBSERVABILITY" —
        "bridge-owned quantity" HER ZAMAN görülebilir olmalıdır)."""
        status = self._status.get(symbol)
        if status is None:
            owned_quantity: float | None = None
            try:
                owned_quantity = compute_bridge_position(self._store, symbol).owned_quantity
            except Exception as exc:  # noqa: BLE001 - yalnızca gözlemlenebilirlik amaçlı best-effort okuma
                _LOGGER.warning("signal-testnet-bridge: status_for(%s) envanter okunamadı: %s", symbol, exc)
            return BridgeSymbolStatus(
                symbol=symbol, policy=self.POLICY, owned_quantity=owned_quantity,
                last_signal_context_id=None, last_signal_direction=None,
                last_action="NONE", last_action_detail="no signal observed by the bridge yet",
                last_client_order_id=None, last_lifecycle_state=None,
                ambiguous=False, updated_at=None,
            ).as_dict()
        return status.as_dict()

    # -- Main entry point ------------------------------------------------------

    def _lock_for(self, symbol: str) -> asyncio.Lock:
        if self._lifecycle_manager is not None:
            # Shares the EXACT SAME lock object the M1 stop/target/trailing
            # evaluator uses (Phase 7) — a signal-driven opposite-signal
            # exit decision and a candle-driven stop/target decision for
            # the same symbol can then never run concurrently.
            return self._lifecycle_manager.lock_for(symbol)
        lock = self._symbol_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._symbol_locks[symbol] = lock
        return lock

    async def on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        """Bu metod ASLA exception fırlatmaz (bkz. modül docstring'i —
        `BridgeRuntime`'ın canlı candle-consume döngüsü içinden çağrılır;
        bir bridge hatası market-data/paper-trading akışını ASLA
        etkilememelidir, tıpkı `RuntimeCoordinator`'ın `order_book_observer`
        hata izolasyonu ile AYNI disiplin)."""
        try:
            await self._on_cycle_result(cycle_result)
        except Exception:  # noqa: BLE001 - bkz. docstring: bridge hatası runtime'ı ASLA etkilemez
            _LOGGER.error("signal-testnet-bridge: beklenmeyen hata (izole edildi)", exc_info=True)

    async def _on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        if not cycle_result.evaluated or cycle_result.signal is None:
            return
        signal = cycle_result.signal
        symbol = signal.symbol

        if cycle_result.paper_result is not None and cycle_result.paper_result.idempotent_replay:
            # Aynı (symbol, context_id) daha önce zaten değerlendirildi
            # (Faz 5 idempotency) — bu KESİNLİKLE YENİ bir ekonomik karar
            # DEĞİLDİR, bkz. modül docstring'i "at most ONE economic
            # Testnet order intent per signal context".
            self._record(
                symbol, signal, action="NO_ACTION",
                detail="idempotent paper replay — no new economic decision",
            )
            return

        async with self._lock_for(symbol):
            await self._decide_and_act(symbol, signal)

    async def _decide_and_act(self, symbol: str, signal) -> None:  # noqa: ANN001 - Signal, döngüsel import'u önlemek için
        if not self._operational:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"bridge not operational: {self._operational_detail}",
            )
            return

        try:
            position = compute_bridge_position(self._store, symbol)
        except Exception as exc:  # noqa: BLE001 - store okunamazsa fail-closed, order YOK
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"bridge-owned inventory unavailable (fail-closed, no order): {exc}",
                ambiguous=True,
            )
            return

        if position.ambiguous:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=(
                    f"unresolved execution ambiguity for {symbol} "
                    f"({', '.join(position.unresolved_context_ids)}) — economic transitions blocked"
                ),
                ambiguous=True,
            )
            return

        direction = signal.direction

        if direction is SignalDirection.NEUTRAL:
            self._record(
                symbol, signal, action="NO_ACTION", detail="NEUTRAL — no execution",
                owned_quantity=position.owned_quantity,
            )
            return

        # When a `LifecycleManager` is wired (autonomous lifecycle enabled),
        # LONG/FLAT is decided from its fee-aware `net_owned_base_quantity`
        # (Phase 3/5) rather than the raw execution-record sum above — the
        # two can differ by base-asset commission amounts. `position` above
        # (raw sum) remains authoritative ONLY for ambiguity detection,
        # which is unaffected by fee accounting.
        if self._lifecycle_manager is not None:
            lifecycle_position = self._lifecycle_manager.position(symbol)
            if lifecycle_position.state.value == "FLAT" and position.owned_quantity > 0:
                # Raw execution-record truth shows REAL owned inventory but
                # the lifecycle layer has no record for it at all (e.g. a
                # legacy position migration that could not trustworthily
                # reconstruct it, Phase 4's "fail-closed, keep monitored,
                # never blindly sell/never treat as FLAT"). Never treat
                # this as FLAT — that would risk a duplicate BUY on top of
                # real inventory (pyramiding). No economic action either
                # way until this is resolved (operator/migration retry).
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail=(
                        f"bridge-owned inventory exists ({position.owned_quantity}) but has no lifecycle "
                        f"record (fail-closed, no order — needs migration/operator attention)"
                    ),
                    owned_quantity=position.owned_quantity, ambiguous=True,
                )
                return
            currently_long = lifecycle_position.state.value == "LONG"
            # Self-review fix: a new BUY must never be allowed while the
            # symbol owns ANY real bridge inventory in ANY non-FLAT
            # lifecycle state — not just LONG. This matters for every
            # transient state (DUST/ENTRY_PENDING/EXIT_PENDING/AMBIGUOUS)
            # and is REQUIRED for Phase 19's two-stage legacy migration:
            # a RECOVERED position (ownership authoritatively established
            # pre-bootstrap, market-dependent fields not yet initialized)
            # must never be pyramided into with a duplicate BUY before
            # stage 2 completes.
            blocks_new_long_entry = lifecycle_position.state.value != "FLAT"
        else:
            currently_long = position.owned_quantity > 0
            blocks_new_long_entry = currently_long

        if direction in _LONG_DIRECTIONS:
            if blocks_new_long_entry:
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail=(
                        "already bridge-owned LONG — same-direction, no churn" if currently_long
                        else f"bridge already owns inventory (state={lifecycle_position.state.value}) — no duplicate entry"
                    ),
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._entries_paused_provider is not None and self._entries_paused_provider():
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail="new entries PAUSED via Control Center admin action — already-open positions unaffected",
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._lifecycle_manager is not None:
                all_symbols = self._all_symbols_provider() if self._all_symbols_provider is not None else (symbol,)
                # CRITICAL FIX (C1 — mainnet-readiness review): pass this
                # BUY's own prospective notional so the exposure gate checks
                # what exposure would become AFTER this order, not just
                # exposure across every OTHER symbol today. Previously the
                # gate silently ignored the size of the very order it was
                # meant to be gating (see `entry_gate()`'s docstring).
                gate = self._lifecycle_manager.entry_gate(
                    symbol, all_symbols=all_symbols, additional_notional_usdt=self._notional,
                )
                if not gate.allowed:
                    self._record(
                        symbol, signal, action="NO_ACTION", detail=f"entry risk gate blocked: {gate.detail}",
                        owned_quantity=position.owned_quantity,
                    )
                    return
            await self._submit_buy(symbol, signal)
            return

        if direction in _SHORT_DIRECTIONS:
            if not currently_long:
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail="bridge is FLAT — no synthetic Spot short, no unrelated wallet inventory touched",
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._lifecycle_manager is not None:
                await self._submit_opposite_signal_exit(symbol, signal)
                return
            await self._submit_sell(symbol, signal, position.owned_quantity)
            return

        raise AssertionError(f"unhandled SignalDirection: {direction!r}")  # pragma: no cover - fail-closed guard

    async def _submit_opposite_signal_exit(self, symbol: str, signal) -> None:  # noqa: ANN001
        from crypto_signal_engine.execution.lifecycle import ExitReason

        outcome = await self._lifecycle_manager.attempt_exit(  # type: ignore[union-attr]
            symbol, reason=ExitReason.OPPOSITE_SIGNAL, exit_signal_context_id=signal.context_id,
        )
        action = "SELL" if outcome.action in ("SOLD", "PARTIAL") else "NO_ACTION"
        refreshed_owned_quantity = self._lifecycle_manager.position(symbol).net_owned_base_quantity  # type: ignore[union-attr]
        self._record(
            symbol, signal, action=action, detail=f"lifecycle exit: {outcome.detail}",
            owned_quantity=refreshed_owned_quantity,
        )

    # -- Order construction / submission -----------------------------------

    async def _submit_buy(self, symbol: str, signal) -> None:  # noqa: ANN001
        context_id = bridge_context_id(symbol, _ACTION_OPEN, signal.context_id)
        try:
            intent = OrderIntent(
                symbol=symbol, side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id=context_id, timestamp=self._now(), quote_quantity=self._notional,
            )
        except ValueError as exc:
            self._record(symbol, signal, action="NO_ACTION", detail=f"BUY intent invalid (fail-closed): {exc}")
            return
        await self._submit(symbol, signal, intent, action="BUY")

    async def _submit_sell(self, symbol: str, signal, owned_quantity: float) -> None:  # noqa: ANN001
        try:
            filters = await self._adapter.validate_symbol(symbol)
        except Exception as exc:  # noqa: BLE001 - filtre lookup başarısızsa fail-closed
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"SELL filter lookup failed (fail-closed, no order): {exc}",
                owned_quantity=owned_quantity,
            )
            return

        step, floor_qty = resolve_lot_size_filter(
            market_step_size=filters.market_step_size, market_min_qty=filters.market_min_qty,
            step_size=filters.step_size, min_qty=filters.min_qty,
        )
        quantity = _floor_to_step(owned_quantity, step)
        if quantity <= 0 or quantity < floor_qty or quantity > owned_quantity:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=(
                    f"bridge-owned quantity {owned_quantity} cannot be sold as a valid Binance lot "
                    f"after step-size alignment (aligned={quantity}, minQty={floor_qty}) — no order"
                ),
                owned_quantity=owned_quantity,
            )
            return

        context_id = bridge_context_id(symbol, _ACTION_CLOSE, signal.context_id)
        try:
            intent = OrderIntent(
                symbol=symbol, side=OrderSide.SELL, order_type=OrderType.MARKET,
                context_id=context_id, timestamp=self._now(), quantity=quantity,
            )
        except ValueError as exc:
            self._record(
                symbol, signal, action="NO_ACTION", detail=f"SELL intent invalid (fail-closed): {exc}",
                owned_quantity=owned_quantity,
            )
            return
        await self._submit(symbol, signal, intent, action="SELL")

    async def _submit(self, symbol: str, signal, intent: OrderIntent, *, action: str) -> None:  # noqa: ANN001
        try:
            record = await self._service.submit(intent)
        except ExecutionPersistenceError as exc:
            if exc.exchange_may_have_accepted_order and action == "BUY" and self._lifecycle_manager is not None:
                # CRITICAL FIX (H3 — mainnet-readiness review, "phantom
                # BUY"): the exchange may have already accepted/filled a
                # REAL BUY that we have zero local record of (place_order()
                # succeeded, persisting that fact failed). The old
                # behaviour fell through to the generic fail-closed record
                # below, which left this symbol looking FLAT to every
                # future signal cycle — `entry_gate()`/`blocks_new_long_
                # entry` would happily allow ANOTHER BUY on top of real,
                # invisible inventory (doubled exposure, zero stop-loss
                # ever set for the phantom half). Pin the symbol AMBIGUOUS
                # instead — this already blocks new entries for it
                # (`blocks_new_long_entry = state != FLAT`) until an
                # operator reconciles `intent.client_order_id` and
                # corrects this record.
                self._lifecycle_manager.mark_ambiguous(
                    symbol,
                    detail=(
                        f"BUY may have reached the exchange (client_order_id={intent.client_order_id}) but local "
                        f"persistence failed — pinned fail-safe pending manual reconcile: {exc}"
                    ),
                    client_order_id=intent.client_order_id,
                )
            self._record(
                symbol, signal, action=action,
                detail=f"submission did not produce a confirmed order (fail-closed): {exc}",
                client_order_id=intent.client_order_id,
            )
            return
        except ExecutionError as exc:
            self._record(
                symbol, signal, action=action,
                detail=f"submission did not produce a confirmed order (fail-closed): {exc}",
                client_order_id=intent.client_order_id,
            )
            return
        except Exception as exc:  # noqa: BLE001 - bilinmeyen hata, ASLA gizli retry, ASLA runtime'ı etkileme
            self._record(
                symbol, signal, action=action,
                detail=f"unexpected submission error (fail-closed, no retry): {exc}",
                client_order_id=intent.client_order_id,
            )
            return

        # Gözlemlenebilirlik doğruluğu: `_record()` çağrılmadan ÖNCE, submit
        # SONRASI GÜNCEL bridge-owned miktarı TEKRAR hesaplanır — aksi halde
        # (bu çağrı `_decide_and_act`'te submit ÖNCESİ hesaplanan `position`
        # üzerinden GEÇMEDİĞİ için) `status_for()` bir BAŞARILI BUY/SELL'DEN
        # SONRA BİLE eski (`None`/stale) `owned_quantity`'yi göstermeye devam
        # ederdi. Bu YALNIZCA gözlemlenebilirlik içindir — GERÇEK envanter
        # (`compute_bridge_position()`, bir SONRAKİ sinyal kararında YENİDEN
        # okunur) HER ZAMAN durable store'dan doğru şekilde türetilir; bu
        # tazeleme BAŞARISIZ olsa bile (ağ/store geçici hatası) ekonomik
        # karar mantığı ETKİLENMEZ.
        if (
            self._lifecycle_manager is not None and action == "BUY"
            and record.lifecycle_state == ExecutionLifecycleState.FILLED
        ):
            # Phase 5/6 — a filled BUY becomes a durable LONG lifecycle
            # position (fee-aware quantity, ATR-based stop/target) here,
            # exactly once per entry (idempotent replay is already excluded
            # upstream in `_on_cycle_result`, and `submit()` itself never
            # re-confirms an already-terminal record as freshly FILLED).
            synthetic_result = ExecutionResult(
                symbol=symbol, client_order_id=record.client_order_id, exchange_order_id=record.exchange_order_id,
                side=OrderSide.BUY, status=record.lifecycle_state, executed_quantity=record.executed_quantity,
                cumulative_quote_quantity=record.cumulative_quote_quantity, transaction_time=record.updated_at,
                context_id=record.context_id,
            )
            atr = self._atr_provider(symbol) if self._atr_provider is not None else None
            try:
                await self._lifecycle_manager.on_entry_filled(
                    symbol, result=synthetic_result, signal_context_id=signal.context_id, atr=atr,
                )
            except Exception:  # noqa: BLE001 - a lifecycle bookkeeping failure must never look like a failed BUY
                _LOGGER.error(
                    "signal-testnet-bridge: on_entry_filled FAILED for %s after a real BUY fill "
                    "(money already moved — this needs operator attention)", symbol, exc_info=True,
                )

        refreshed_owned_quantity: float | None = None
        try:
            refreshed_owned_quantity = compute_bridge_position(self._store, symbol).owned_quantity
        except Exception as exc:  # noqa: BLE001 - yalnızca status alanı için best-effort tazeleme
            _LOGGER.warning(
                "signal-testnet-bridge: submit sonrası owned_quantity tazelenemedi (%s): %s", symbol, exc
            )

        self._record(
            symbol, signal, action=action,
            detail=f"submit result: {record.lifecycle_state}"
            + ("" if record.is_terminal() else " (unresolved — reconciliation-eligible)"),
            client_order_id=record.client_order_id, lifecycle_state=record.lifecycle_state,
            ambiguous=not record.is_terminal(), owned_quantity=refreshed_owned_quantity,
        )

    def _now(self) -> datetime:
        return self._clock.now()

    # -- Status recording --------------------------------------------------

    def _record(
        self, symbol: str, signal, *, action: str, detail: str,  # noqa: ANN001
        owned_quantity: float | None = None, client_order_id: str | None = None,
        lifecycle_state: str | None = None, ambiguous: bool = False,
    ) -> None:
        prior = self._status.get(symbol)
        resolved_owned = owned_quantity if owned_quantity is not None else (
            prior.owned_quantity if prior is not None else None
        )
        self._status[symbol] = BridgeSymbolStatus(
            symbol=symbol, policy=self.POLICY, owned_quantity=resolved_owned,
            last_signal_context_id=signal.context_id, last_signal_direction=signal.direction.value,
            last_action=action, last_action_detail=detail,
            last_client_order_id=client_order_id if client_order_id is not None else (
                prior.last_client_order_id if prior is not None else None
            ),
            last_lifecycle_state=lifecycle_state if lifecycle_state is not None else (
                prior.last_lifecycle_state if prior is not None else None
            ),
            ambiguous=ambiguous, updated_at=self._now().isoformat(),
        )
        _LOGGER.info(
            "signal-testnet-bridge symbol=%s context_id=%s direction=%s action=%s detail=%s",
            symbol, signal.context_id, signal.direction.value, action, detail,
        )
