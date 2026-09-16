"""
`LifecycleManager` — the single orchestration point for the autonomous
Testnet trading lifecycle's entry/exit/risk decisions (Phases 4-15, 17-19).

This is deliberately the ONLY place that:
- turns a filled BUY into a durable LONG position (fee-aware, ATR-based
  stop/target, Phase 5/6),
- turns a triggered exit (stop/target/max-hold/opposite-signal) into a
  SELL submission and a finalized completed trade (Phase 6/7/13/14/15),
- enforces the entry risk gates (cooldown/max-positions/max-exposure/
  daily-loss-breaker, Phase 10/11),
- owns the per-symbol lock BOTH the M1 candle evaluator
  (`lifecycle_runtime.py`) and the signal-driven opposite-signal path
  (`signal_bridge.py`) serialize through — this is what makes "at most one
  economic exit intent per market event" (Phase 7) a real guarantee rather
  than a hope: whichever caller acquires the lock first re-reads FRESH
  persisted state, so a loser always observes the winner's result and
  no-ops.

It delegates ALL actual math to the pure functions in `lifecycle.py` and
ALL actual order submission to the already-accepted
`ExecutionReconciliationService`/`TestnetExecutionAdapter` — no new
order-submission code path is invented here."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.execution.adapter import TestnetExecutionAdapter
from crypto_signal_engine.execution.errors import ExecutionError, ExecutionPersistenceError
from crypto_signal_engine.execution.lifecycle import (
    FALLBACK_STOP_PCT,
    FALLBACK_TARGET_PCT,
    BridgePositionRecord,
    Candle,
    DailyRiskAccumulator,
    ExitPolicyConfig,
    ExitReason,
    PositionLifecycleState,
    RiskPolicyConfig,
    apply_entry_fills,
    apply_exit_fills,
    apply_trade_to_daily_accumulator,
    compute_cooldown_until,
    compute_initial_stop_and_target,
    compute_sell_quantity,
    compute_total_exposure,
    resolve_lot_size_filter,
    compute_trade_risk_contribution,
    cooldown_clear,
    daily_loss_breaker_tripped,
    evaluate_candle,
    flat_record,
    gross_realized_pnl,
    max_exposure_gate_open,
    max_positions_gate_open,
    net_realized_pnl,
    trading_day_key,
)
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import ExecutionResult, Fill, OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.signal_bridge import _ACTION_CLOSE, bridge_context_id
from crypto_signal_engine.ops.event_log import record_event
from crypto_signal_engine.ops.notifier import Notifier, safe_notify
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock

_LOGGER = logging.getLogger("crypto_signal_engine.execution.lifecycle_manager")


def derive_base_asset(symbol: str) -> str:
    """All symbols this bridge trades are USDT-quoted (enforced by
    `AutomaticSymbolSelector`, see selector.py:90 `quote_asset != "USDT"`
    rejection) — a manual `CSE_SYMBOLS` override could theoretically supply
    a non-USDT pair, so this fails closed (no guessing) rather than
    silently stripping the wrong suffix."""
    if not symbol.endswith("USDT"):
        raise ValueError(f"derive_base_asset: yalnızca USDT-quote semboller destekleniyor, alınan: {symbol!r}")
    return symbol[: -len("USDT")]


@dataclass(frozen=True)
class EntryGateResult:
    allowed: bool
    detail: str


@dataclass(frozen=True)
class ExitOutcome:
    action: str  # "SOLD" | "PARTIAL" | "DUST" | "EXIT_PENDING" | "NO_ACTION"
    detail: str
    exit_reason: ExitReason | None = None


@dataclass(frozen=True)
class ReconciliationOutcome:
    """CRITICAL FIX (C4 — mainnet-readiness review): the result of
    `LifecycleManager.reconcile_stuck_position()`. `action` is one of:
    "NOT_APPLICABLE" (position wasn't AMBIGUOUS/EXIT_PENDING),
    "STILL_AMBIGUOUS" / "STILL_EXIT_PENDING" (exchange truth not yet
    resolvable — pin deliberately left in place, safe to retry later),
    "RELEASED_TO_FLAT" (a pinned BUY never actually took effect),
    "PROMOTED_TO_LONG" (a pinned BUY DID fill — position now fully
    initialized exactly as a normal fill would be), "REVERTED_TO_LONG"
    (a pinned SELL never actually took effect), or "FINALIZED_<action>"
    (a pinned SELL DID fill — wraps the underlying `ExitOutcome.action`,
    e.g. "FINALIZED_SOLD"/"FINALIZED_PARTIAL")."""

    action: str
    detail: str


def _recover_exit_reason(last_exit_reason: str | None) -> ExitReason | None:
    """`BridgePositionRecord.last_exit_reason` for an EXIT_PENDING pin is
    always either the bare `ExitReason.value` (line ~593 above) or that
    value plus a fixed suffix (e.g. `f"{reason.value}_PERSISTENCE_
    AMBIGUOUS"`, the H3 fix) — never anything else. Recovering it lets
    reconciliation finalize a completed trade with its ORIGINAL exit
    reason rather than a fabricated one. Returns `None` (never raises)
    for a record with no recognizable prefix, e.g. one pinned before
    this field existed."""
    if last_exit_reason is None:
        return None
    for candidate in ExitReason:
        if last_exit_reason == candidate.value or last_exit_reason.startswith(f"{candidate.value}_"):
            return candidate
    return None


class LifecycleManager:
    def __init__(
        self,
        *,
        store: LifecycleStore,
        execution_service: ExecutionReconciliationService,
        exit_policy: ExitPolicyConfig | None = None,
        risk_policy: RiskPolicyConfig | None = None,
        clock: Clock | None = None,
        exit_policy_provider: Callable[[], tuple[ExitPolicyConfig, str | None]] | None = None,
        notifier: Notifier | None = None,
        event_log_path: Path | None = None,
    ) -> None:
        self._store = store
        self._service = execution_service
        self._adapter = TestnetExecutionAdapter(execution_service.client)
        self._client = execution_service.client
        self._exit_policy = exit_policy or ExitPolicyConfig()
        self._risk_policy = risk_policy or RiskPolicyConfig()
        self._clock = clock or SystemClock()
        self._locks: dict[str, asyncio.Lock] = {}
        # Adaptive Intelligence v1 (additive, optional — see step 14):
        # when supplied, called ONCE per NEW position entry (never for an
        # already-open one — see `evaluate_m1_candle`'s use of
        # `position.resolved_exit_policy`) to resolve which
        # `ExitPolicyConfig` + opaque `policy_version_id` a brand-new
        # position should be opened under, instead of the static
        # `self._exit_policy`. `crypto_signal_engine/` never imports
        # `adaptive/`; the actual champion-reading callable is
        # constructed by `scripts/run_with_adaptive_policy.py`. When
        # `None` (the default, e.g. `app.py`'s existing static-config
        # path), behaviour is bit-for-bit identical to before this field
        # existed.
        self._exit_policy_provider = exit_policy_provider
        # 24/7 Ops v1, Step 4 — outbound-only alerting. `None` by default
        # (every existing caller) — bit-for-bit unaffected. Notified
        # ONCE per UTC trading day, on the TRANSITION into a tripped
        # breaker (never on every subsequent blocked entry_gate() call
        # that same day, which would spam an operator's phone on a busy
        # trading day). Defensively wrapped via `safe_notify()` — a
        # raising/hanging notifier can never affect this gate's decision
        # (see `tests/test_execution_lifecycle_manager.py`'s required
        # test proving this).
        self._notifier = notifier
        self._daily_loss_breaker_notified_day: str | None = None
        # UI Polish v1, Step A9 — event log (additive, optional, `None`
        # by default). Written ALONGSIDE (never instead of) the
        # `safe_notify()` call above, at the SAME transition point —
        # `record_event()` itself never raises (see `ops/event_log.py`).
        self._event_log_path = event_log_path

    def _resolve_entry_policy(self) -> tuple[ExitPolicyConfig, str | None]:
        if self._exit_policy_provider is None:
            return self._exit_policy, None
        return self._exit_policy_provider()

    def lock_for(self, symbol: str) -> asyncio.Lock:
        lock = self._locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[symbol] = lock
        return lock

    def _now(self) -> datetime:
        return self._clock.now()

    def position(self, symbol: str) -> BridgePositionRecord:
        record = self._store.load_position(symbol)
        return record if record is not None else flat_record(symbol, now=self._now())

    def mark_ambiguous(self, symbol: str, *, detail: str, client_order_id: str | None = None) -> BridgePositionRecord:
        """CRITICAL FIX (H3 — mainnet-readiness review, BUY side): pins a
        symbol into `AMBIGUOUS` when an order MAY have reached the
        exchange but local persistence could not confirm it (`signal_
        bridge.py::_submit()`'s `ExecutionPersistenceError.exchange_may_
        have_accepted_order` case). `AMBIGUOUS` already blocks a new
        entry for this symbol (`signal_bridge.py`'s `blocks_new_long_
        entry = lifecycle_position.state.value != "FLAT"`) and already
        occupies an `entry_gate()` open-slot (this module's own `open_
        slots` count) — so this prevents a duplicate BUY from stacking on
        top of a possibly-real, currently-invisible exchange position
        until an operator resolves it via `reconcile_stuck_position()`
        (CRITICAL FIX C4, below) and this record is corrected. Only ever
        downgrades FLAT (or overwrites an already-ambiguous marker with a
        fresher detail) — never overwrites a real, already-established
        LONG/RECOVERED/DUST/*_PENDING record, since this is called ONLY
        from the `on_entry_filled` path this exact symbol/attempt would
        otherwise have reached, and the entry gate already guarantees the
        symbol was FLAT immediately before this attempt was submitted.

        CRITICAL FIX (C4 — mainnet-readiness review): `client_order_id`
        is stored in `entry_order_client_order_id` — WITHOUT this, the
        pin was previously recoverable ONLY by grepping the free-text
        `detail`/`last_exit_reason` string, giving `reconcile_stuck_
        position()` nothing structured to query the exchange with. This
        field already existed in the schema (documented "set while
        ENTRY_PENDING") and was otherwise never populated anywhere in
        this codebase — reused here for its natural purpose."""
        current = self.position(symbol)
        if current.state is not PositionLifecycleState.FLAT and current.state is not PositionLifecycleState.AMBIGUOUS:
            # Should be unreachable (entry_gate/blocks_new_long_entry
            # already require FLAT before a BUY is ever attempted) — fail
            # closed by leaving the existing, presumably-more-authoritative
            # record untouched rather than clobbering it.
            _LOGGER.error(
                "lifecycle: mark_ambiguous(%s) called but position is already state=%s (NOT overwriting): %s",
                symbol, current.state.value, detail,
            )
            return current
        now = self._now()
        record = BridgePositionRecord(
            symbol=symbol, state=PositionLifecycleState.AMBIGUOUS, last_exit_reason=detail,
            entry_order_client_order_id=client_order_id, updated_at=now,
        )
        self._store.save_position(record)
        _LOGGER.error("lifecycle: %s pinned AMBIGUOUS (fail-safe, blocks new entries until reconciled): %s", symbol, detail)
        return record

    # -- Reconciliation back into bridge_position (Phase 20 / C4) ------------

    async def reconcile_stuck_position(self, symbol: str) -> ReconciliationOutcome:
        """CRITICAL FIX (C4 — mainnet-readiness review): closes the "no
        reconciliation path back into bridge_position" gap identified
        against `lifecycle_migration.py`. Once a position is pinned
        AMBIGUOUS (H3, BUY side) or EXIT_PENDING (Phase 17 / H3, SELL
        side), NOTHING in this codebase previously ever revisited it —
        `entry_gate()`/`blocks_new_long_entry` correctly keep it frozen
        (occupying a slot, blocking new entries) forever, and
        `reconstruct_legacy_ownership()`'s own first guard (`if
        lifecycle_store.load_position(symbol) is not None: return None`)
        means the one-time legacy-migration path can NEVER touch a
        symbol that already has ANY `bridge_position` row. So a stuck
        AMBIGUOUS/EXIT_PENDING record was PERMANENT — not merely
        "restart-recoverable" as first assumed — until this method
        existed.

        This re-queries the exchange for the pinned `client_order_id`
        via the SAME `ExecutionReconciliationService.reconcile()` the
        execution layer already exposes for manual/CLI use, then applies
        the now-authoritative outcome to the `bridge_position` record —
        promoting to LONG (via the SAME `on_entry_filled()` construction
        path a normal fill uses) or finalizing the exit (via the SAME
        `_finalize_exit()` a normal exit uses) if the order truly filled,
        reverting to FLAT/LONG if it never took effect, or leaving the
        pin in place (and saying so) if the exchange truth is still not
        resolvable. Intended for an operator-facing entry point (CLI or
        a periodic reconciliation sweep) — deliberately NEVER called
        automatically from the hot M1/signal path, since correcting a
        pinned position is a deliberate, audited action."""
        position = self.position(symbol)
        if position.state is PositionLifecycleState.AMBIGUOUS:
            return await self._reconcile_ambiguous_entry(symbol, position)
        if position.state is PositionLifecycleState.EXIT_PENDING:
            return await self._reconcile_pending_exit(symbol, position)
        return ReconciliationOutcome(
            action="NOT_APPLICABLE",
            detail=(
                f"{symbol} is state={position.state.value} — reconcile_stuck_position() only applies to "
                f"AMBIGUOUS/EXIT_PENDING, nothing to do"
            ),
        )

    async def _reconcile_ambiguous_entry(self, symbol: str, position: BridgePositionRecord) -> ReconciliationOutcome:
        client_order_id = position.entry_order_client_order_id
        if client_order_id is None:
            return ReconciliationOutcome(
                action="STILL_AMBIGUOUS",
                detail=(
                    f"{symbol} is AMBIGUOUS but has no recorded client_order_id (pinned before the C4 fix, or "
                    f"pinned manually) — cannot auto-reconcile, needs direct operator inspection"
                ),
            )
        try:
            exec_record = await self._service.reconcile(client_order_id=client_order_id)
        except Exception as exc:  # noqa: BLE001 - a reconciliation query failure must NEVER crash the caller or clear the pin
            return ReconciliationOutcome(
                action="STILL_AMBIGUOUS",
                detail=f"{symbol}: exchange reconciliation query failed (pin left in place, safe to retry later): {exc}",
            )
        if not exec_record.is_terminal():
            return ReconciliationOutcome(
                action="STILL_AMBIGUOUS",
                detail=f"{symbol}: exchange state still not terminal ({exec_record.lifecycle_state}) — pin left in place",
            )
        if exec_record.executed_quantity <= 0:
            # The BUY never actually took effect — safe to release back to FLAT.
            now = self._now()
            self._store.save_position(flat_record(symbol, now=now))
            return ReconciliationOutcome(
                action="RELEASED_TO_FLAT",
                detail=(
                    f"{symbol}: exchange confirms {exec_record.lifecycle_state} with zero fill — BUY never took "
                    f"effect, released back to FLAT"
                ),
            )
        # The BUY WAS filled — promote to a real, fully-initialized LONG
        # position via the SAME construction path a normal fill uses
        # (`on_entry_filled`), never a second, divergent code path.
        # `atr=None` deliberately uses the SAME already-established
        # fallback fixed-percentage stop/target as any other fill where
        # ATR is unavailable (no live candle exists at reconciliation
        # time to derive one from).
        result = ExecutionResult(
            symbol=symbol, client_order_id=exec_record.client_order_id,
            exchange_order_id=exec_record.exchange_order_id, side=OrderSide.BUY,
            status=exec_record.lifecycle_state, executed_quantity=exec_record.executed_quantity,
            cumulative_quote_quantity=exec_record.cumulative_quote_quantity,
            transaction_time=exec_record.updated_at, context_id=exec_record.context_id,
        )
        record = await self.on_entry_filled(
            symbol, result=result, signal_context_id=f"RECONCILED:{exec_record.context_id}", atr=None,
        )
        return ReconciliationOutcome(
            action="PROMOTED_TO_LONG",
            detail=(
                f"{symbol}: exchange confirms {exec_record.lifecycle_state} (qty={exec_record.executed_quantity}) "
                f"— promoted to LONG (entry_client_order_id={record.entry_client_order_id})"
            ),
        )

    async def _reconcile_pending_exit(self, symbol: str, position: BridgePositionRecord) -> ReconciliationOutcome:
        client_order_id = position.exit_pending_client_order_id
        if client_order_id is None:
            return ReconciliationOutcome(
                action="STILL_EXIT_PENDING",
                detail=(
                    f"{symbol} is EXIT_PENDING but has no recorded client_order_id — cannot auto-reconcile, "
                    f"needs direct operator inspection"
                ),
            )
        try:
            exec_record = await self._service.reconcile(client_order_id=client_order_id)
        except Exception as exc:  # noqa: BLE001 - a reconciliation query failure must NEVER crash the caller or clear the pin
            return ReconciliationOutcome(
                action="STILL_EXIT_PENDING",
                detail=f"{symbol}: exchange reconciliation query failed (pin left in place, safe to retry later): {exc}",
            )
        if not exec_record.is_terminal():
            return ReconciliationOutcome(
                action="STILL_EXIT_PENDING",
                detail=f"{symbol}: exchange state still not terminal ({exec_record.lifecycle_state}) — pin left in place",
            )
        reason = _recover_exit_reason(position.last_exit_reason)
        if exec_record.executed_quantity <= 0:
            # The SELL never actually took effect — revert to LONG,
            # unchanged, so normal monitoring/exit resumes on the next
            # candle (mirrors attempt_exit()'s own "zero economic effect"
            # branch for a normal, non-pinned terminal-failed SELL).
            now = self._now()
            reverted = _replace_state(
                position, state=PositionLifecycleState.LONG, exit_pending_client_order_id=None,
                last_exit_reason=(
                    f"{(reason.value if reason is not None else position.last_exit_reason)}_RECONCILED_NO_FILL"
                ),
                now=now,
            )
            self._store.save_position(reverted)
            return ReconciliationOutcome(
                action="REVERTED_TO_LONG",
                detail=(
                    f"{symbol}: exchange confirms {exec_record.lifecycle_state} with zero fill — SELL never took "
                    f"effect, reverted to LONG"
                ),
            )
        # The SELL WAS filled — finalize it via the SAME `_finalize_exit()`
        # a normal (non-pinned) exit uses, never a second, divergent code
        # path.
        try:
            filters = await self._adapter.validate_symbol(symbol)
        except Exception as exc:  # noqa: BLE001 - filter lookup failure must fail closed, pin left in place
            return ReconciliationOutcome(
                action="STILL_EXIT_PENDING",
                detail=(
                    f"{symbol}: exchange confirms a real fill (qty={exec_record.executed_quantity}) but filter "
                    f"lookup failed, cannot finalize yet (pin left in place, safe to retry): {exc}"
                ),
            )
        _, min_qty = resolve_lot_size_filter(
            market_step_size=filters.market_step_size, market_min_qty=filters.market_min_qty,
            step_size=filters.step_size, min_qty=filters.min_qty,
        )
        outcome = await self._finalize_exit(
            symbol, position=position, exec_record=exec_record,
            reason=reason if reason is not None else ExitReason.OPPOSITE_SIGNAL,
            exit_signal_context_id=None, min_qty=min_qty,
        )
        return ReconciliationOutcome(
            action=f"FINALIZED_{outcome.action}",
            detail=(
                f"{symbol}: exchange confirms {exec_record.lifecycle_state} (qty={exec_record.executed_quantity}) "
                f"— exit finalized: {outcome.detail}"
            ),
        )

    # -- Entry risk gates (Phase 10/11) --------------------------------------

    def entry_gate(
        self, symbol: str, *, all_symbols: tuple[str, ...], additional_notional_usdt: float = 0.0
    ) -> EntryGateResult:
        """Checks cooldown, max-open-positions, max-exposure, and the daily
        loss breaker. `all_symbols` is the full monitored universe — needed
        to compute the current slot count/exposure across every symbol,
        not just this one. Ambiguity/FLAT/already-LONG checks remain
        `signal_bridge`'s responsibility (unchanged, Phase 5).

        CRITICAL FIX (C1 — mainnet-readiness review): `additional_notional_
        usdt` MUST be the prospective new order's own notional (the amount
        this entry itself would add to exposure) — it previously reached
        `max_exposure_gate_open()` hardcoded as `0.0`, which meant the gate
        only ever checked EXISTING exposure across other symbols and never
        the size of the very order it was supposed to be gating. That made
        `max_total_exposure_usdt` bypassable by construction: current
        exposure could sit at e.g. 99/100 USDT and a fresh 500 USDT BUY
        would still pass (99 + 0 <= 100), landing exposure at 599 USDT. The
        default of `0.0` is kept only for callers doing a hypothetical/
        informational check with no concrete order size yet (e.g. dashboard
        previews, existing tests) — the real order-submission path
        (`signal_bridge.py`) MUST pass its actual prospective notional."""
        now = self._now()
        position = self.position(symbol)
        if not cooldown_clear(cooldown_until=position.cooldown_until, now=now):
            return EntryGateResult(False, f"cooldown active until {position.cooldown_until.isoformat()}")

        other_positions = [self.position(other_symbol) for other_symbol in all_symbols]
        open_slots = sum(
            1 for other in other_positions
            if other.state in (
                PositionLifecycleState.LONG, PositionLifecycleState.ENTRY_PENDING,
                PositionLifecycleState.EXIT_PENDING, PositionLifecycleState.AMBIGUOUS,
                PositionLifecycleState.RECOVERED,
            )
        )
        # Portfolio/Accounting v1 — single source of truth for "total
        # exposure" (see `compute_total_exposure`'s own docstring); this
        # was previously computed inline here, duplicated independently
        # in `app.py::_bridge_lifecycle_snapshot()`.
        exposure = compute_total_exposure(other_positions)

        if not max_positions_gate_open(open_slot_count=open_slots, config=self._risk_policy):
            return EntryGateResult(False, f"max open positions reached ({open_slots}/{self._risk_policy.max_open_positions})")

        if not max_exposure_gate_open(
            current_exposure_usdt=exposure, additional_notional_usdt=additional_notional_usdt, config=self._risk_policy
        ):
            # Detail string kept byte-for-bit unchanged for the additional_
            # notional_usdt=0.0 case (existing regression test asserts the
            # exact string); only extended when a real prospective order
            # size was actually supplied.
            if additional_notional_usdt:
                detail = (
                    f"max exposure reached ({exposure:.2f} + {additional_notional_usdt:.2f} prospective "
                    f"> {self._risk_policy.max_total_exposure_usdt} USDT)"
                )
            else:
                detail = f"max exposure reached ({exposure:.2f}/{self._risk_policy.max_total_exposure_usdt} USDT)"
            return EntryGateResult(False, detail)

        day_key = trading_day_key(now)
        accumulator = self._store.load_daily_risk(day_key)
        if daily_loss_breaker_tripped(accumulator, self._risk_policy):
            detail = (
                f"daily loss circuit breaker tripped (conservative_risk_pnl={accumulator.conservative_risk_pnl:.2f} "
                f"USDT, limit={-self._risk_policy.daily_loss_limit_usdt})"
            )
            # 24/7 Ops v1, Step 4 — notify only on the TRANSITION into
            # tripped for THIS trading day (in-memory; a process restart
            # mid-day may notify once more, harmless for an alert).
            if self._daily_loss_breaker_notified_day != day_key:
                self._daily_loss_breaker_notified_day = day_key
                safe_notify(self._notifier, f"[crypto-signal-engine] {symbol}: {detail}")
                # UI Polish v1, Step A9 — SAME transition point as the
                # notifier call above; `record_event()` never raises
                # (see ops/event_log.py), a `None` path is a no-op.
                if self._event_log_path is not None:
                    record_event(
                        self._event_log_path, event_type="daily_loss_breaker_tripped",
                        detail=f"{symbol}: {detail}", occurred_at=now,
                    )
            return EntryGateResult(False, detail)
        return EntryGateResult(True, "entry gates open")

    # -- Entry fill -> durable LONG position (Phase 5/6) ---------------------

    async def _resolve_fills(self, symbol: str, result: ExecutionResult) -> tuple[Fill, ...]:
        if result.fills:
            return result.fills
        # Reconciliation-recovered fill (query_order never carries fills[])
        # — backfill via myTrades (Phase 0-C).
        try:
            return await self._client.my_trades(symbol, order_id=result.exchange_order_id)
        except ExecutionError as exc:
            _LOGGER.error(
                "lifecycle: fills backfill FAILED for %s order_id=%s — fee ledger will be incomplete "
                "for this fill (quantity/VWAP still accurate from executedQty/cumulativeQuoteQty): %s",
                symbol, result.exchange_order_id, exc,
            )
            return ()

    async def on_entry_filled(
        self,
        symbol: str,
        *,
        result: ExecutionResult,
        signal_context_id: str,
        atr: float | None,
    ) -> BridgePositionRecord:
        """`result.fills` is a fast-path only — in the real wiring the
        caller gets back an `ExecutionRecord` (from
        `ExecutionReconciliationService.submit()`, which does NOT persist
        `fills[]`) and must construct a synthetic `ExecutionResult` with
        `fills=()`, in which case this ALWAYS backfills via `my_trades()`
        (see `_resolve_fills`) — this is by design (Phase 0-C), not a
        fallback path expected to be rare."""
        now = self._now()
        base_asset = derive_base_asset(symbol)
        fills = await self._resolve_fills(symbol, result)
        if fills:
            agg = apply_entry_fills(fills, base_asset=base_asset, client_order_id=result.client_order_id, now=now)
            gross_vwap = agg.gross_entry_vwap
            net_quantity = agg.net_owned_base_quantity
            fee_entries = agg.fee_ledger_entries
        else:
            # No fills recovered at all (backfill also failed) — fall back
            # to the order's own authoritative executedQty/cumulativeQuoteQty
            # (still real exchange truth, just not fee-attributed per fill).
            gross_vwap = result.cumulative_quote_quantity / result.executed_quantity
            net_quantity = result.executed_quantity
            fee_entries = ()

        # Adaptive Intelligence v1 — resolved exactly ONCE, here, for this
        # brand-new position only (never for an already-open one — see
        # `evaluate_m1_candle`). Its five fields are embedded inline below
        # so this position keeps using THIS policy for its entire life,
        # regardless of any later champion swap.
        entry_policy, entry_policy_version_id = self._resolve_entry_policy()

        if atr is not None and atr > 0:
            stop, target = compute_initial_stop_and_target(entry_price=gross_vwap, atr=atr, config=entry_policy)
        else:
            _LOGGER.warning(
                "lifecycle: ATR unavailable at entry for %s — using fallback fixed-percentage stop/target "
                "(%.1f%%/%.1f%%)", symbol, FALLBACK_STOP_PCT * 100, FALLBACK_TARGET_PCT * 100,
            )
            stop = gross_vwap * (1 - FALLBACK_STOP_PCT)
            target = gross_vwap * (1 + FALLBACK_TARGET_PCT)

        previous = self.position(symbol)
        record = BridgePositionRecord(
            symbol=symbol, state=PositionLifecycleState.LONG, gross_entry_vwap=gross_vwap,
            net_owned_base_quantity=net_quantity, initial_protective_stop=stop, high_water=gross_vwap,
            effective_stop=stop, trailing_active=False, last_stop_mechanism="STOP_LOSS", take_profit=target,
            entry_timestamp=result.transaction_time, entry_signal_context_id=signal_context_id,
            entry_client_order_id=result.client_order_id,
            cumulative_realized_gross_pnl=previous.cumulative_realized_gross_pnl,
            cooldown_until=None, migrated_existing_position=False, updated_at=now,
            exit_policy_stop_atr_multiple=entry_policy.stop_atr_multiple,
            exit_policy_take_profit_atr_multiple=entry_policy.take_profit_atr_multiple,
            exit_policy_trailing_activation_atr_multiple=entry_policy.trailing_activation_atr_multiple,
            exit_policy_trailing_distance_atr_multiple=entry_policy.trailing_distance_atr_multiple,
            exit_policy_max_hold_hours=entry_policy.max_hold_hours,
            policy_version_id=entry_policy_version_id,
        )
        # CRITICAL FIX (M1 — mainnet-readiness review): these two writes
        # now happen in ONE transaction (`LifecycleStore.save_position_
        # with_fee_entries()`) — previously separate calls, each only
        # apparently transacted (see that method's own docstring / the
        # module-level comment in lifecycle_store.py), so a crash between
        # them could leave fee-ledger rows for a position that was never
        # actually saved as LONG, or vice versa.
        self._store.save_position_with_fee_entries(record, trade_group_id=result.client_order_id, fee_entries=fee_entries)
        return record

    # -- Shared exit attempt (Phase 6/7/13/14/15/18) -------------------------

    async def attempt_exit(
        self, symbol: str, *, reason: ExitReason, exit_signal_context_id: str | None,
        current_price: float | None = None,
    ) -> ExitOutcome:
        """The ONE method both the M1 lifecycle evaluator and
        `signal_bridge`'s opposite-signal path call — both MUST acquire
        `self.lock_for(symbol)` before calling this (this method itself
        does not lock, to let callers batch a read-then-decide sequence
        under the same lock they already hold; see `lifecycle_runtime.py`
        and `signal_bridge.py` for the exact call sites).

        `current_price` (MIN_NOTIONAL pre-flight fix, see below): the
        caller's freshest already-available price, if it has one.
        `evaluate_m1_candle()` passes the triggering M1 candle's own close
        (zero extra network cost — that price is already in hand for
        every single call). `signal_bridge.py`'s opposite-signal path has
        no candle in scope, so it leaves this `None` and this method falls
        back to one `symbol_price()` lookup ONLY for that (rare — only on
        an actual signal reversal, not every M1 candle) call path. This is
        design choice (a) from the task: thread an optional price through,
        fetch fresh only where no price is already at hand — never a
        second, redundant network round-trip on the hot (every-candle) M1
        path."""
        position = self.position(symbol)
        if position.state is not PositionLifecycleState.LONG:
            return ExitOutcome(action="NO_ACTION", detail=f"not LONG (state={position.state.value})")

        try:
            filters = await self._adapter.validate_symbol(symbol)
        except Exception as exc:  # noqa: BLE001 - filter lookup failure must fail closed, no order
            return ExitOutcome(action="NO_ACTION", detail=f"SELL filter lookup failed (fail-closed): {exc}")

        step, min_qty = resolve_lot_size_filter(
            market_step_size=filters.market_step_size, market_min_qty=filters.market_min_qty,
            step_size=filters.step_size, min_qty=filters.min_qty,
        )
        sizing = compute_sell_quantity(
            net_owned_base_quantity=position.net_owned_base_quantity, step_size=step, min_qty=min_qty,
            sell_commission_headroom_bps=self._risk_policy.sell_commission_headroom_bps,
        )
        if sizing.quantity <= 0:
            now = self._now()
            dust_record = _replace_state(position, state=PositionLifecycleState.DUST, last_exit_reason=reason.value, now=now)
            self._store.save_position(dust_record)
            return ExitOutcome(action="DUST", detail=f"residual {position.net_owned_base_quantity} unsellable (below min lot)", exit_reason=reason)

        # MIN_NOTIONAL pre-flight (bug fix): unlike LOT_SIZE dust above,
        # `notional = price * quantity` can become sellable again on its
        # own (price can move back up) — this residual is NOT permanently
        # unsellable, so `position.state` is DELIBERATELY left LONG here
        # (never DUST, never any new state). `evaluate_m1_candle()`'s own
        # `state is not LONG: return None` gate means DUST is a one-way,
        # terminal state (see lifecycle.py) — transitioning here would
        # permanently strand a position that a later price move could
        # still close for real money. Only the doomed exchange SUBMISSION
        # is skipped; the position keeps being evaluated every candle,
        # exactly as before this fix, and the normal SELL path fires the
        # instant notional clears the filter again.
        if filters.min_notional is not None and filters.min_notional_applies_to_market:
            if current_price is not None:
                price = current_price
            else:
                try:
                    price = await self._client.symbol_price(symbol)
                except Exception as exc:  # noqa: BLE001 - fail-closed, same convention as filter lookup above: no order
                    return ExitOutcome(
                        action="NO_ACTION",
                        detail=f"MIN_NOTIONAL pre-check price lookup failed (fail-closed): {exc}",
                    )
            prospective_notional = price * sizing.quantity
            if prospective_notional < filters.min_notional:
                return ExitOutcome(
                    action="NO_ACTION",
                    detail=(
                        f"blocked by MIN_NOTIONAL pre-check: prospective notional {prospective_notional:.8f} "
                        f"USDT (price {price} x quantity {sizing.quantity}) below exchange minimum "
                        f"{filters.min_notional} USDT — no order submitted, position stays LONG and will be "
                        f"re-evaluated on the next candle"
                    ),
                    exit_reason=reason,
                )

        # Live-validation bug fix: the context_id must be deterministic PER
        # TRIGGERING EVENT (Phase 7: "derived from ... the deterministic
        # triggering market context"), never a single static value for the
        # position's whole life — `entry_client_order_id` alone would give
        # every retry attempt (across every future candle) the EXACT SAME
        # context_id as a first, since-REJECTED attempt, and
        # `ExecutionReconciliationService.submit()` correctly refuses to
        # re-POST for an already-terminal context_id — permanently
        # blocking any real retry after a genuine rejection. The signal's
        # own context_id (opposite-signal path) or the specific M1
        # candle's close_time (`last_evaluated_candle_close`, just saved by
        # `evaluate_m1_candle` before calling this) uniquely identifies
        # THIS triggering event, while staying idempotent if the SAME
        # event is ever processed twice.
        #
        # NOTE (C3/H3 mainnet-readiness review — verified this rotation is
        # SAFE): this discriminator only ever advances between calls when
        # the PREVIOUS attempt for this position reached a CONFIRMED
        # terminal, zero-exchange-effect outcome (see the `exec_record.
        # is_terminal() and executed_quantity == 0` revert-to-LONG branch
        # below) — a fresh discriminator there is correct and desirable
        # (it un-sticks the position from a dead terminal context_id so a
        # real retry can happen). The genuinely dangerous case — a submit
        # whose exchange-side outcome is UNKNOWN (e.g. `ExecutionPersistenceError`
        # raised AFTER `place_order()` already succeeded) — is handled
        # separately below by pinning the position into EXIT_PENDING
        # BEFORE this method can ever be re-entered for it, so that case
        # never reaches a second discriminator rotation at all.
        event_discriminator = exit_signal_context_id or (
            position.last_evaluated_candle_close.isoformat()
            if position.last_evaluated_candle_close is not None
            else "unknown"
        )
        exit_context_id = bridge_context_id(
            symbol, _ACTION_CLOSE,
            f"lifecycle:{reason.value}:{position.entry_client_order_id}:{event_discriminator}",
        )
        intent = OrderIntent(
            symbol=symbol, side=OrderSide.SELL, order_type=OrderType.MARKET,
            context_id=exit_context_id, timestamp=self._now(), quantity=sizing.quantity,
        )
        try:
            exec_record = await self._service.submit(intent)
        except ExecutionPersistenceError as exc:
            if exc.exchange_may_have_accepted_order:
                # CRITICAL FIX (H3 — mainnet-readiness review): the
                # exchange may already hold a real, possibly-filled SELL
                # order that we have ZERO local record of (place_order()
                # succeeded, but persisting that fact failed). Silently
                # returning NO_ACTION here (the old behaviour) left the
                # position LONG and unchanged, so the NEXT M1 candle would
                # re-evaluate, re-trigger, and submit ANOTHER real SELL —
                # a genuine duplicate-order risk. Instead we durably pin
                # this position into EXIT_PENDING (blocking any further
                # automated exit attempt — `attempt_exit`/`evaluate_m1_
                # candle` both refuse to touch anything that isn't exactly
                # LONG) carrying the attempted client_order_id, so a human
                # or an explicit `reconcile --client-order-id ...` must
                # resolve the ambiguity before this position is touched
                # again.
                now = self._now()
                pending_record = _replace_state(
                    position, state=PositionLifecycleState.EXIT_PENDING,
                    exit_pending_client_order_id=exc.client_order_id,
                    last_exit_reason=f"{reason.value}_PERSISTENCE_AMBIGUOUS", now=now,
                )
                self._store.save_position(pending_record)
                return ExitOutcome(
                    action="EXIT_PENDING",
                    detail=(
                        f"SELL may have reached the exchange but local persistence failed — position pinned "
                        f"fail-safe, requires manual `reconcile --client-order-id {exc.client_order_id}`: {exc}"
                    ),
                    exit_reason=reason,
                )
            # SAFE: nothing was ever sent to the exchange (persistence
            # failed BEFORE place_order was attempted) — the position is
            # unchanged, retrying freely on the next candle is fine.
            return ExitOutcome(action="NO_ACTION", detail=f"SELL submission did not confirm (fail-closed, safe to retry): {exc}")
        except ExecutionError as exc:
            return ExitOutcome(action="NO_ACTION", detail=f"SELL submission did not confirm (fail-closed): {exc}")

        if exec_record.lifecycle_state == ExecutionLifecycleState.FILLED:
            return await self._finalize_exit(
                symbol, position=position, exec_record=exec_record, reason=reason,
                exit_signal_context_id=exit_signal_context_id, min_qty=min_qty,
            )

        if exec_record.is_terminal():
            # Live-validation bug fix: REJECTED/CANCELED/EXPIRED are
            # TERMINAL — they will NEVER become FILLED via reconciliation
            # (unlike ACKNOWLEDGED/PARTIALLY_FILLED/AMBIGUOUS/
            # UNKNOWN_NOT_FOUND below, which genuinely might). Treating a
            # terminal-failed order the same as a genuinely pending one
            # (the original bug) permanently stranded the position in
            # EXIT_PENDING, since `evaluate_m1_candle` only re-evaluates
            # `state == LONG` positions — it would never be retried again.
            if exec_record.executed_quantity > 0:
                # Partially executed before failing/being cancelled — real
                # inventory WAS sold; finalize exactly what was filled
                # (Phase 13's "account for executed quantity, not
                # requested quantity").
                return await self._finalize_exit(
                    symbol, position=position, exec_record=exec_record, reason=reason,
                    exit_signal_context_id=exit_signal_context_id, min_qty=min_qty,
                )
            # Zero economic effect — the position is UNCHANGED from before
            # this attempt. Revert to LONG (never EXIT_PENDING) so it stays
            # monitored and the NEXT M1 candle can retry the exit.
            now = self._now()
            reverted = _replace(
                position, last_exit_reason=f"{reason.value}_SUBMIT_FAILED_{exec_record.lifecycle_state}", updated_at=now,
            )
            self._store.save_position(reverted)
            return ExitOutcome(
                action="NO_ACTION",
                detail=(
                    f"SELL failed with zero execution ({exec_record.lifecycle_state}: {exec_record.detail}) — "
                    f"position remains LONG for retry on the next candle"
                ),
                exit_reason=reason,
            )

        # Genuinely non-terminal (ACKNOWLEDGED / PARTIALLY_FILLED /
        # AMBIGUOUS / UNKNOWN_NOT_FOUND / SUBMISSION_ATTEMPTED) — may still
        # resolve to FILLED via reconciliation; EXIT_PENDING is correct
        # here (Phase 17).
        now = self._now()
        pending_record = _replace_state(
            position, state=PositionLifecycleState.EXIT_PENDING,
            exit_pending_client_order_id=exec_record.client_order_id, last_exit_reason=reason.value, now=now,
        )
        self._store.save_position(pending_record)
        return ExitOutcome(action="EXIT_PENDING", detail=f"submit result: {exec_record.lifecycle_state}", exit_reason=reason)

    async def _finalize_exit(
        self, symbol: str, *, position: BridgePositionRecord, exec_record, reason: ExitReason,
        exit_signal_context_id: str | None, min_qty: float,
    ) -> ExitOutcome:
        now = self._now()
        base_asset = derive_base_asset(symbol)
        result = ExecutionResult(
            symbol=symbol, client_order_id=exec_record.client_order_id,
            exchange_order_id=exec_record.exchange_order_id, side=OrderSide.SELL,
            status=exec_record.lifecycle_state, executed_quantity=exec_record.executed_quantity,
            cumulative_quote_quantity=exec_record.cumulative_quote_quantity,
            transaction_time=exec_record.updated_at, context_id=exec_record.context_id,
        )
        fills = await self._resolve_fills(symbol, result)
        if fills:
            exit_agg = apply_exit_fills(fills, base_asset=base_asset, client_order_id=exec_record.client_order_id, now=now)
            exit_vwap = exit_agg.exit_gross_vwap
            quantity_closed = exit_agg.gross_sold_quantity
            fee_entries = exit_agg.fee_ledger_entries
            base_commission = exit_agg.base_asset_commission_total
        else:
            exit_vwap = exec_record.cumulative_quote_quantity / exec_record.executed_quantity
            quantity_closed = exec_record.executed_quantity
            fee_entries = ()
            base_commission = 0.0

        trade_group_id = position.entry_client_order_id or exec_record.client_order_id
        # CRITICAL FIX (M1 — mainnet-readiness review): read the ALREADY-
        # DURABLE fees for this trade group (entry fees, and any earlier
        # partial-exit fees) BEFORE appending this exit's own fee_entries,
        # then combine them IN MEMORY (`net_realized_pnl()`/`compute_
        # trade_risk_contribution()` are both plain order-independent
        # sums — see their own docstrings) rather than appending first and
        # re-querying afterward. This lets the append below happen inside
        # the SAME single atomic transaction as the completed-trade
        # record, daily-risk update, and position save (`LifecycleStore.
        # finalize_exit()`) instead of being its own separate, earlier
        # transaction — closing the LAST gap between the four writes a
        # closed trade requires. Safe under this codebase's concurrency
        # model because `_finalize_exit()` only ever runs while the
        # caller already holds `self.lock_for(symbol)`, so no concurrent
        # writer can append to this same trade_group_id between the read
        # and the write below.
        previously_committed_fees = self._store.fee_ledger_for_trade_group(symbol, trade_group_id)
        all_fees = previously_committed_fees + fee_entries

        gross_pnl = gross_realized_pnl(
            gross_entry_vwap=position.gross_entry_vwap, exit_gross_vwap=exit_vwap, quantity_closed=quantity_closed,
        )
        net_pnl = net_realized_pnl(gross_pnl=gross_pnl, fee_ledger_entries=all_fees)

        contribution = compute_trade_risk_contribution(gross_pnl=gross_pnl, fee_ledger_entries=all_fees, config=self._risk_policy)
        day_key = trading_day_key(now)
        accumulator = self._store.load_daily_risk(day_key)
        accumulator = apply_trade_to_daily_accumulator(accumulator, contribution)

        residual = position.net_owned_base_quantity - quantity_closed - base_commission
        cooldown_until = compute_cooldown_until(exit_time=now, config=self._risk_policy)
        cumulative_gross = position.cumulative_realized_gross_pnl + gross_pnl

        residual = max(residual, 0.0)
        if residual <= 1e-12:
            new_position = flat_record(symbol, now=now)
            new_position = _replace(
                new_position, cooldown_until=cooldown_until, cumulative_realized_gross_pnl=cumulative_gross,
                last_exit_reason=reason.value,
            )
            action = "SOLD"
        elif residual < min_qty:
            # Genuinely unsellable (Phase 13: legal Binance rounding, or
            # SELL-side commission headroom, left a residual below the
            # exchange's own minimum lot) — never compensated from
            # unrelated wallet balance, and never retried as a normal exit.
            new_position = _replace(
                position, state=PositionLifecycleState.DUST, net_owned_base_quantity=residual,
                cooldown_until=cooldown_until, cumulative_realized_gross_pnl=cumulative_gross,
                last_exit_reason=reason.value, updated_at=now,
            )
            action = "PARTIAL"
        else:
            # A LEGALLY SELLABLE amount remains (e.g. an order that was
            # cancelled mid-fill before completing) — this is NOT dust; the
            # position stays LONG (fee-aware quantity reduced to what
            # actually remains) so it keeps being monitored and a future
            # exit can be retried for the rest (self-review fix: the
            # original code marked ANY nonzero residual DUST regardless of
            # size, which would have stopped managing a substantial
            # remaining position).
            new_position = _replace(
                position, state=PositionLifecycleState.LONG, net_owned_base_quantity=residual,
                cooldown_until=cooldown_until, cumulative_realized_gross_pnl=cumulative_gross,
                last_exit_reason=reason.value, updated_at=now,
            )
            action = "PARTIAL"

        # CRITICAL FIX (M1 — mainnet-readiness review): all four writes a
        # closed trade requires (fee-ledger append, completed-trade
        # record, daily-risk update, final position save) now happen in
        # ONE atomic transaction — see `LifecycleStore.finalize_exit()`'s
        # own docstring for exactly what this closes.
        self._store.finalize_exit(
            new_position=new_position, trade_group_id=trade_group_id, fee_entries=fee_entries,
            completed_trade=dict(
                symbol=symbol, trade_group_id=trade_group_id,
                entry_client_order_id=position.entry_client_order_id or "", exit_client_order_id=exec_record.client_order_id,
                entry_timestamp=position.entry_timestamp, exit_timestamp=exec_record.updated_at,
                quantity_closed=quantity_closed, gross_entry_vwap=position.gross_entry_vwap, exit_gross_vwap=exit_vwap,
                gross_realized_pnl=gross_pnl, net_realized_pnl=net_pnl, exit_reason=reason.value,
                entry_signal_context_id=position.entry_signal_context_id, exit_signal_context_id=exit_signal_context_id,
                now=now, policy_version_id=position.policy_version_id,
            ),
            daily_risk=accumulator, daily_risk_now=now,
        )
        return ExitOutcome(action=action, detail=f"exit filled, gross_pnl={gross_pnl:.6f}", exit_reason=reason)

    # -- M1 candle-driven lifecycle evaluation (Phase 6/8/9) -----------------

    async def evaluate_m1_candle(self, symbol: str, candle: Candle, *, atr_for_trailing: float | None) -> ExitOutcome | None:
        async with self.lock_for(symbol):
            position = self.position(symbol)
            if position.state is not PositionLifecycleState.LONG:
                return None
            if candle.close_time <= position.entry_timestamp:
                # No-lookahead guard (Phase 8): never evaluate a candle at
                # or before the authoritative entry timestamp.
                return None
            price_state = position.to_price_state()
            # Adaptive Intelligence v1 — position-pinning invariant: a
            # currently OPEN position must keep using whichever
            # `ExitPolicyConfig` it was actually opened under, for its
            # entire remaining life, even if `self._exit_policy`/the
            # active champion has since changed. Never read
            # `self._exit_policy` directly here.
            resolved_policy = position.resolved_exit_policy(default=self._exit_policy)
            result = evaluate_candle(price_state, candle, resolved_policy, atr_for_trailing=atr_for_trailing)
            now = self._now()
            # C2 fix: candle_close_time is the candle's OWN authoritative
            # close time, never wall-clock `now` — see BridgePositionRecord.
            # with_price_state()'s docstring.
            updated_position = position.with_price_state(result.updated_state, now=now, candle_close_time=candle.close_time)
            self._store.save_position(updated_position)
            if result.exit_reason is None:
                return None
            return await self.attempt_exit(
                symbol, reason=result.exit_reason, exit_signal_context_id=None, current_price=candle.close,
            )


def _replace(record: BridgePositionRecord, **kwargs: object) -> BridgePositionRecord:
    from dataclasses import replace as _dc_replace

    return _dc_replace(record, **kwargs)  # type: ignore[arg-type]


def _replace_state(record: BridgePositionRecord, *, state: PositionLifecycleState, now: datetime, **kwargs: object) -> BridgePositionRecord:
    return _replace(record, state=state, updated_at=now, **kwargs)
