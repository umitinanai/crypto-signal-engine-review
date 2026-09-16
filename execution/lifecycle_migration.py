"""
Legacy bridge position migration (Phase 4/19) — split into two durable
stages, matching the canonical Phase 19 startup order EXACTLY:

    reconciliation -> authoritative recovery -> LEGACY OWNERSHIP
    RECONSTRUCTION (this module, stage 1) -> market bootstrap ->
    FIRST-FRESH MARKET-DEPENDENT INITIALIZATION (this module, stage 2)
    -> activation boundary -> economic actions

STAGE 1 — `reconstruct_legacy_ownership()` / `reconstruct_all_legacy_ownership()`
runs BEFORE market bootstrap. It reconstructs ONLY what authoritative
execution truth already proves — `gross_entry_vwap`, fee-aware
`net_owned_base_quantity`, `fee_ledger`, authoritative `entry_timestamp`,
entry execution identity — from bridge-owned FILLED fills (via `myTrades`,
backfilled — never from PAPER/wallet balance/manual `lab-*` records). It
persists a `PositionLifecycleState.RECOVERED` record (real, safety-pinned,
economically owned — never FLAT, never evaluated for exit, never a target
for a duplicate BUY, see `lifecycle.py`'s enum docstring) and creates ZERO
exchange orders. No market data of any kind is read or required.

STAGE 2 — `finalize_legacy_lifecycle_init()` / `finalize_all_legacy_lifecycle_init()`
runs AFTER market bootstrap. It is initialization of an ALREADY-
authoritatively-migrated position, NOT delayed ownership migration: it is
a no-op for anything not currently in exactly `RECOVERED` state (an
already-LONG position on a normal restart is untouched — Phase 19's
"not incorrectly remigrated/reinitialized"). It reads the first genuinely
fresh post-bootstrap M5 candle close (real data the bootstrap fetch just
produced) as `first_fresh_post_recovery_live_market_price`, and the first
post-bootstrap M5 ATR feature snapshot (same `FeatureEngine` bootstrap
already computed) as the volatility reference — NEVER fabricating either.
`high_water = max(gross_entry_vwap, first_fresh_post_recovery_live_market_price)`
per Phase 4's exact rule; `initial_protective_stop`/`take_profit` are
derived from `gross_entry_vwap` + that ATR, exactly like a fresh entry.
Only once these are set does the position become `LONG` — no automated
lifecycle exit and no new economic order can occur before that (Phase 19),
since `evaluate_m1_candle`/`signal_bridge.py`'s entry gate both refuse to
touch anything that is not exactly `LONG`/`FLAT` respectively."""

from __future__ import annotations

import logging
from dataclasses import replace

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.errors import ExecutionError
from crypto_signal_engine.execution.lifecycle import (
    FALLBACK_STOP_PCT,
    FALLBACK_TARGET_PCT,
    BridgePositionRecord,
    ExitPolicyConfig,
    PositionLifecycleState,
    apply_entry_fills,
    compute_initial_stop_and_target,
)
from crypto_signal_engine.execution.lifecycle_manager import derive_base_asset
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import OrderSide
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import bridge_context_id, compute_bridge_position
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient
from crypto_signal_engine.providers.binance.clock import Clock

_LOGGER = logging.getLogger("crypto_signal_engine.execution.lifecycle_migration")
_ATR_FEATURE_NAME = "ATR_14"
_BRIDGE_OPEN_PREFIX_TEMPLATE = "bridge:{symbol}:OPEN:"


# ---------------------------------------------------------------------------
# Stage 1 (pre-bootstrap): authoritative ownership reconstruction only.
# ---------------------------------------------------------------------------


async def reconstruct_legacy_ownership(
    symbol: str,
    *,
    execution_store: ExecutionStateStore,
    lifecycle_store: LifecycleStore,
    client: BinanceTestnetClient,
    clock: Clock,
) -> BridgePositionRecord | None:
    """Idempotent: a symbol already carrying ANY `bridge_position` row
    (fresh entry, already-`RECOVERED`, or already fully `LONG`/other) is
    untouched — this never re-runs ownership reconstruction. Returns the
    new `RECOVERED` record, or `None` if nothing needed reconstructing or
    nothing trustworthy could be established. Reads NO market data and
    creates ZERO exchange orders."""
    if lifecycle_store.load_position(symbol) is not None:
        return None

    position = compute_bridge_position(execution_store, symbol)
    if position.ambiguous or position.owned_quantity <= 0:
        return None

    open_prefix = _BRIDGE_OPEN_PREFIX_TEMPLATE.format(symbol=symbol)
    entry_records = [
        r for r in execution_store.list_for_symbol(symbol)
        if r.context_id.startswith(open_prefix) and r.side is OrderSide.BUY
        and r.lifecycle_state == ExecutionLifecycleState.FILLED
    ]
    if not entry_records:
        _LOGGER.error(
            "lifecycle-migration: %s has owned inventory (%.10f) but no FILLED bridge OPEN record — "
            "cannot trustworthily reconstruct entry, leaving fail-closed/unmigrated (Phase 4)",
            symbol, position.owned_quantity,
        )
        return None
    entry_record = entry_records[-1]  # latest OPEN — no-pyramiding guarantees at most one open lot

    now = clock.now()
    base_asset = derive_base_asset(symbol)
    try:
        fills = await client.my_trades(symbol, order_id=entry_record.exchange_order_id)
    except ExecutionError as exc:
        _LOGGER.warning("lifecycle-migration: myTrades backfill failed for %s: %s", symbol, exc)
        fills = ()

    if fills:
        agg = apply_entry_fills(fills, base_asset=base_asset, client_order_id=entry_record.client_order_id, now=now)
        gross_vwap, net_quantity, fee_entries = agg.gross_entry_vwap, agg.net_owned_base_quantity, agg.fee_ledger_entries
    else:
        if entry_record.executed_quantity <= 0 or entry_record.cumulative_quote_quantity <= 0:
            _LOGGER.error(
                "lifecycle-migration: %s entry record has no usable price/quantity truth — leaving unmigrated", symbol,
            )
            return None
        gross_vwap = entry_record.cumulative_quote_quantity / entry_record.executed_quantity
        net_quantity = entry_record.executed_quantity
        fee_entries = ()

    record = BridgePositionRecord(
        symbol=symbol, state=PositionLifecycleState.RECOVERED, gross_entry_vwap=gross_vwap,
        net_owned_base_quantity=net_quantity, initial_protective_stop=None, high_water=None,
        effective_stop=None, take_profit=None, trailing_active=False, last_stop_mechanism="STOP_LOSS",
        entry_timestamp=entry_record.updated_at, entry_signal_context_id=None,
        entry_client_order_id=entry_record.client_order_id, cumulative_realized_gross_pnl=0.0,
        cooldown_until=None, migrated_existing_position=True, updated_at=now,
    )
    lifecycle_store.append_fee_entries(symbol, entry_record.client_order_id, fee_entries)
    lifecycle_store.save_position(record)
    _LOGGER.info(
        "lifecycle-migration stage 1: reconstructed ownership for %s (gross_entry_vwap=%.8f, "
        "net_owned_base_quantity=%.10f) — state=RECOVERED, awaiting post-bootstrap initialization",
        symbol, gross_vwap, net_quantity,
    )
    return record


async def reconstruct_all_legacy_ownership(
    symbols: tuple[str, ...],
    *,
    execution_store: ExecutionStateStore,
    lifecycle_store: LifecycleStore,
    client: BinanceTestnetClient,
    clock: Clock,
) -> tuple[str, ...]:
    """Reconstructs every symbol independently — one symbol's failure never
    blocks another's (Phase 4/19 isolation)."""
    reconstructed: list[str] = []
    for symbol in symbols:
        try:
            record = await reconstruct_legacy_ownership(
                symbol, execution_store=execution_store, lifecycle_store=lifecycle_store,
                client=client, clock=clock,
            )
        except Exception:  # noqa: BLE001 - one symbol's failure must never block startup or other symbols
            _LOGGER.error("lifecycle-migration stage 1: unexpected failure for %s (isolated)", symbol, exc_info=True)
            continue
        if record is not None:
            reconstructed.append(symbol)
    return tuple(reconstructed)


# ---------------------------------------------------------------------------
# Stage 2 (post-bootstrap): market-dependent lifecycle initialization of an
# ALREADY-authoritatively-migrated position. Never re-runs ownership
# reconstruction.
# ---------------------------------------------------------------------------


def _first_fresh_post_recovery_price(coordinator, symbol: str) -> float | None:
    """The latest M5 candle close bootstrap just fetched — real market
    data, never fabricated. `None` if bootstrap did not produce one (e.g.
    a symbol that failed to bootstrap) — the caller then falls back to
    `gross_entry_vwap` alone (still real, still not fabricated; simply
    "no fresher price is known yet")."""
    try:
        window = coordinator._candle_windows.get((symbol, Timeframe.M5))  # noqa: SLF001 - same private-accessor precedent as bridge_runtime.py
    except Exception:  # noqa: BLE001
        return None
    if window is None:
        return None
    history = window.history()
    if not history:
        return None
    return history[-1].close


def _first_fresh_post_recovery_atr(coordinator, symbol: str) -> float | None:
    """The first post-bootstrap M5 ATR_14 feature snapshot — computed by
    the SAME `FeatureEngine` bootstrap already ran
    (`runtime/bootstrap.py::apply_bootstrap_candles` commits feature
    snapshots exactly like live `ingest_candle` does). Never a separate
    computation, never fabricated."""
    try:
        snapshot = coordinator._feature_engine.latest_snapshot(symbol, Timeframe.M5)  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return None
    if snapshot is None:
        return None
    value = snapshot.values.get(_ATR_FEATURE_NAME)
    if value is None or value <= 0:
        return None
    return value


async def finalize_legacy_lifecycle_init(
    symbol: str,
    *,
    lifecycle_store: LifecycleStore,
    coordinator,
    clock: Clock,
    exit_policy: ExitPolicyConfig | None = None,
    policy_version_id: str | None = None,
) -> BridgePositionRecord | None:
    """A no-op for any position not currently in EXACTLY `RECOVERED` state
    — this is initialization, never remigration (Phase 19: "not
    incorrectly remigrated/reinitialized"). Transitions `RECOVERED` ->
    `LONG` using only real, first-fresh post-bootstrap market data.

    Adaptive Intelligence v1: this transition IS this position's entry
    moment for policy-pinning purposes (it is the first time stop/target/
    trailing become active) — `policy`'s five fields and the caller-
    supplied `policy_version_id` are embedded inline into the finalized
    record here, exactly like a fresh `on_entry_filled()` entry, so this
    position also obeys the position-pinning invariant from then on."""
    record = lifecycle_store.load_position(symbol)
    if record is None or record.state is not PositionLifecycleState.RECOVERED:
        return None

    now = clock.now()
    policy = exit_policy or ExitPolicyConfig()

    first_fresh_price = _first_fresh_post_recovery_price(coordinator, symbol)
    high_water = (
        max(record.gross_entry_vwap, first_fresh_price) if first_fresh_price is not None else record.gross_entry_vwap
    )

    atr_value = _first_fresh_post_recovery_atr(coordinator, symbol)
    if atr_value is not None:
        stop, target = compute_initial_stop_and_target(entry_price=record.gross_entry_vwap, atr=atr_value, config=policy)
        atr_source = "M5_ATR"
    else:
        _LOGGER.warning(
            "lifecycle-migration stage 2: no fresh post-bootstrap ATR available for %s — using fallback "
            "fixed-percentage stop/target", symbol,
        )
        stop = record.gross_entry_vwap * (1 - FALLBACK_STOP_PCT)
        target = record.gross_entry_vwap * (1 + FALLBACK_TARGET_PCT)
        atr_source = "FALLBACK_FIXED_PCT"

    finalized = replace(
        record, state=PositionLifecycleState.LONG, initial_protective_stop=stop, high_water=high_water,
        effective_stop=stop, take_profit=target, updated_at=now,
        exit_policy_stop_atr_multiple=policy.stop_atr_multiple,
        exit_policy_take_profit_atr_multiple=policy.take_profit_atr_multiple,
        exit_policy_trailing_activation_atr_multiple=policy.trailing_activation_atr_multiple,
        exit_policy_trailing_distance_atr_multiple=policy.trailing_distance_atr_multiple,
        exit_policy_max_hold_hours=policy.max_hold_hours,
        policy_version_id=policy_version_id,
    )
    lifecycle_store.save_position(finalized)
    _LOGGER.info(
        "lifecycle-migration stage 2: finalized %s (high_water=%.8f, initial_protective_stop=%.8f, "
        "take_profit=%.8f, atr_source=%s) — state=LONG, now monitored",
        symbol, high_water, stop, target, atr_source,
    )
    return finalized


async def finalize_all_legacy_lifecycle_init(
    symbols: tuple[str, ...],
    *,
    lifecycle_store: LifecycleStore,
    coordinator,
    clock: Clock,
    exit_policy: ExitPolicyConfig | None = None,
    policy_version_id: str | None = None,
) -> tuple[str, ...]:
    """Finalizes every symbol independently — one symbol's failure never
    blocks another's (Phase 4/19 isolation). A symbol left un-finalized
    (e.g. an unexpected exception) simply stays `RECOVERED` — still
    safety-pinned, still never tradeable, and will be retried on the next
    restart (idempotent: `finalize_legacy_lifecycle_init` only acts on
    exactly `RECOVERED`)."""
    finalized: list[str] = []
    for symbol in symbols:
        try:
            record = await finalize_legacy_lifecycle_init(
                symbol, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=clock, exit_policy=exit_policy,
                policy_version_id=policy_version_id,
            )
        except Exception:  # noqa: BLE001 - one symbol's failure must never block startup or other symbols
            _LOGGER.error("lifecycle-migration stage 2: unexpected failure for %s (isolated)", symbol, exc_info=True)
            continue
        if record is not None:
            finalized.append(symbol)
    return tuple(finalized)
