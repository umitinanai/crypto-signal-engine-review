"""
Bridge position lifecycle — durable-state math for the autonomous
Testnet trading lifecycle (entry -> stop/target/trailing -> exit -> P&L).

This module is intentionally the ONLY place that computes:
- gross entry/exit VWAP and net owned base quantity from real fills,
  keeping fees strictly separate (see `apply_entry_fills`/`apply_exit_fills`),
- the single monotonic `effective_stop` and static `take_profit`,
- the shared candle-ordering evaluation used by BOTH live execution
  (`lifecycle_runtime.py`) and historical replay (`research/replay.py`) —
  `evaluate_candle()` is pure and takes no wall-clock/IO dependency, so the
  two call sites are provably running the same code, not two
  implementations that happen to agree.

Nothing here performs I/O, submits an order, or reads a clock — every
function is a pure transformation of (state, inputs) -> new state. This is
what lets replay reuse it byte-for-byte and lets every invariant be unit
tested without a network or a database.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from enum import Enum

from crypto_signal_engine.domain._validation import (
    normalize_symbol,
    require_finite,
    require_utc_aware,
)
from crypto_signal_engine.execution.models import Fill


class PositionLifecycleState(str, Enum):
    """A bridge-owned position's lifecycle state — orthogonal to
    `ExecutionLifecycleState` (which tracks a single order). This tracks
    the *economic* state of the symbol's bridge inventory."""

    FLAT = "FLAT"
    LONG = "LONG"
    ENTRY_PENDING = "ENTRY_PENDING"
    EXIT_PENDING = "EXIT_PENDING"
    AMBIGUOUS = "AMBIGUOUS"
    DUST = "DUST"
    # Phase 19 two-stage legacy migration: ownership (gross_entry_vwap,
    # net_owned_base_quantity, fee_ledger, entry identity) has been
    # AUTHORITATIVELY reconstructed from execution truth PRE-bootstrap, but
    # market-dependent fields (initial_protective_stop/high_water/
    # effective_stop/take_profit) are not yet known — those require the
    # first fresh post-recovery market data, which only exists after
    # market bootstrap. A RECOVERED position is real, safety-pinned,
    # economically owned inventory — it is NEVER FLAT (no duplicate entry
    # allowed) and NEVER evaluated for exit (no stop/target exist yet) —
    # `finalize_legacy_lifecycle_init()` is the ONLY path that transitions
    # it onward, to LONG, once market data is available.
    RECOVERED = "RECOVERED"


class ExitReason(str, Enum):
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    MAX_HOLD = "MAX_HOLD"
    OPPOSITE_SIGNAL = "OPPOSITE_SIGNAL"


# Deterministic exit-reason priority (Phase 7). Lower index wins when more
# than one condition is true for the same evaluation.
EXIT_REASON_PRIORITY: tuple[ExitReason, ...] = (
    ExitReason.STOP_LOSS,
    ExitReason.TRAILING_STOP,
    ExitReason.TAKE_PROFIT,
    ExitReason.MAX_HOLD,
    ExitReason.OPPOSITE_SIGNAL,
)


@dataclass(frozen=True)
class FeeLedgerEntry:
    """A single append-only fee observation. Recorded here and ONLY here —
    a base-asset commission that already reduced `net_owned_base_quantity`
    must still get exactly one entry here (for P&L subtraction), never a
    second adjustment anywhere else (Phase 5/14)."""

    amount: float
    asset: str
    usdt_equivalent: float | None  # None = not reliably convertible
    client_order_id: str
    side: str  # "ENTRY" | "EXIT"
    trade_id: int | None
    recorded_at: datetime

    def __post_init__(self) -> None:
        require_finite(self.amount, "amount")
        require_utc_aware(self.recorded_at, "recorded_at")
        if self.usdt_equivalent is not None:
            require_finite(self.usdt_equivalent, "usdt_equivalent")
        if self.side not in ("ENTRY", "EXIT"):
            raise ValueError(f"side ENTRY veya EXIT olmalı, alınan: {self.side!r}")


_QUOTE_ASSET = "USDT"  # this bridge only ever trades USDT-quote symbols (see lifecycle_manager.derive_base_asset)


def _known_usdt_equivalent(*, amount: float, asset: str) -> float | None:
    """A fee already denominated in the quote asset (USDT) is trivially
    known in USDT — no price lookup needed. Anything else (base asset,
    BNB, ...) is genuinely unknown here (a pure function with no price
    feed) and stays `None` (UNKNOWN) unless a caller resolves it
    separately (Phase 14 — never silently treated as zero)."""
    return amount if asset == _QUOTE_ASSET else None


@dataclass(frozen=True)
class EntryFillAggregate:
    """Result of aggregating one or more BUY fills — see `apply_entry_fills`.
    `gross_entry_vwap` and `net_owned_base_quantity` are kept strictly
    separate for the life of the position (Phase 5)."""

    gross_entry_vwap: float
    gross_filled_quantity: float
    net_owned_base_quantity: float
    fee_ledger_entries: tuple[FeeLedgerEntry, ...]


def apply_entry_fills(
    fills: tuple[Fill, ...], *, base_asset: str, client_order_id: str, now: datetime
) -> EntryFillAggregate:
    """Aggregates BUY fills into a fee-free gross VWAP, a fee-aware owned
    quantity, and an append-only fee ledger — the three are never blended
    (Phase 5). `base_asset` must be the symbol's base asset (e.g. "BTC" for
    BTCUSDT) so a base-asset commission can be told apart from a quote/BNB
    one per fill."""
    if not fills:
        raise ValueError("apply_entry_fills: en az bir fill gerekli")
    gross_quantity = sum(f.quantity for f in fills)
    gross_notional = sum(f.price * f.quantity for f in fills)
    gross_vwap = gross_notional / gross_quantity

    base_asset_commission_total = 0.0
    ledger: list[FeeLedgerEntry] = []
    for fill in fills:
        if fill.commission <= 0:
            continue
        if fill.commission_asset == base_asset:
            base_asset_commission_total += fill.commission
        ledger.append(
            FeeLedgerEntry(
                amount=fill.commission, asset=fill.commission_asset,
                usdt_equivalent=_known_usdt_equivalent(amount=fill.commission, asset=fill.commission_asset),
                client_order_id=client_order_id, side="ENTRY", trade_id=fill.trade_id, recorded_at=now,
            )
        )

    net_owned = gross_quantity - base_asset_commission_total
    return EntryFillAggregate(
        gross_entry_vwap=gross_vwap,
        gross_filled_quantity=gross_quantity,
        net_owned_base_quantity=max(net_owned, 0.0),
        fee_ledger_entries=tuple(ledger),
    )


@dataclass(frozen=True)
class ExitFillAggregate:
    """Result of aggregating one or more SELL fills — see `apply_exit_fills`."""

    exit_gross_vwap: float
    gross_sold_quantity: float
    base_asset_commission_total: float
    fee_ledger_entries: tuple[FeeLedgerEntry, ...]


def apply_exit_fills(
    fills: tuple[Fill, ...], *, base_asset: str, client_order_id: str, now: datetime
) -> ExitFillAggregate:
    """Mirror of `apply_entry_fills` for the SELL side. `base_asset_commission_total`
    is returned separately (not pre-subtracted) so the caller can reduce
    `net_owned_base_quantity` by EXACTLY this amount, once (Phase 6/13)."""
    if not fills:
        raise ValueError("apply_exit_fills: en az bir fill gerekli")
    gross_quantity = sum(f.quantity for f in fills)
    gross_notional = sum(f.price * f.quantity for f in fills)
    gross_vwap = gross_notional / gross_quantity

    base_asset_commission_total = 0.0
    ledger: list[FeeLedgerEntry] = []
    for fill in fills:
        if fill.commission <= 0:
            continue
        if fill.commission_asset == base_asset:
            base_asset_commission_total += fill.commission
        ledger.append(
            FeeLedgerEntry(
                amount=fill.commission, asset=fill.commission_asset,
                usdt_equivalent=_known_usdt_equivalent(amount=fill.commission, asset=fill.commission_asset),
                client_order_id=client_order_id, side="EXIT", trade_id=fill.trade_id, recorded_at=now,
            )
        )
    return ExitFillAggregate(
        exit_gross_vwap=gross_vwap,
        gross_sold_quantity=gross_quantity,
        base_asset_commission_total=base_asset_commission_total,
        fee_ledger_entries=tuple(ledger),
    )


def gross_realized_pnl(*, gross_entry_vwap: float, exit_gross_vwap: float, quantity_closed: float) -> float:
    """Phase 14 — the ONLY realized-P&L formula. Uses pure, fee-free VWAPs
    only; fees are never blended in here (see `net_realized_pnl`)."""
    return (exit_gross_vwap - gross_entry_vwap) * quantity_closed


def net_realized_pnl(
    *, gross_pnl: float, fee_ledger_entries: tuple[FeeLedgerEntry, ...]
) -> float | None:
    """Subtracts every relevant fee EXACTLY once, converted to USDT. Returns
    `None` (UNKNOWN) the moment any relevant fee's USDT value is not
    reliably known — never silently treated as zero (Phase 14)."""
    total_fees_usdt = 0.0
    for entry in fee_ledger_entries:
        if entry.usdt_equivalent is None:
            return None
        total_fees_usdt += entry.usdt_equivalent
    return gross_pnl - total_fees_usdt


def unrealized_gross_pnl(
    *, gross_entry_vwap: float, net_owned_base_quantity: float, current_price: float
) -> float:
    """Dashboard localization (Turkish operator UI) — mark-to-market P&L
    for a STILL-OPEN position. Structurally identical to
    `gross_realized_pnl`, with `current_price` (a live, read-only mark —
    e.g. the latest closed M5 candle's close) standing in for
    `exit_gross_vwap` (no exit has actually happened yet, fee-free/gross
    only, exactly like realized P&L is fee-free/gross before
    `net_realized_pnl`). This is NOT a second accounting system — it is
    the SAME formula as `gross_realized_pnl`, applied to an as-of-now mark
    instead of an actual fill price. Callers are responsible for treating
    the result as "Hesaplanamıyor" (unknown) whenever no authoritative
    read-only current price exists — this function itself never fabricates
    a price."""
    return (current_price - gross_entry_vwap) * net_owned_base_quantity


@dataclass(frozen=True)
class ExitPolicyConfig:
    """Deterministic, explainable, small-surface ATR-based exit policy
    (Phase 6). Defaults chosen for a conservative, explainable 1:2
    risk:reward with trailing activated once 1R of favorable movement is
    banked (see final report for rationale) — NOT curve-fit, NOT
    reoptimized per symbol."""

    stop_atr_multiple: float = 2.0
    take_profit_atr_multiple: float = 4.0
    trailing_activation_atr_multiple: float = 2.0
    trailing_distance_atr_multiple: float = 2.0
    max_hold_hours: float = 48.0

    def __post_init__(self) -> None:
        for value, name in (
            (self.stop_atr_multiple, "stop_atr_multiple"),
            (self.take_profit_atr_multiple, "take_profit_atr_multiple"),
            (self.trailing_activation_atr_multiple, "trailing_activation_atr_multiple"),
            (self.trailing_distance_atr_multiple, "trailing_distance_atr_multiple"),
            (self.max_hold_hours, "max_hold_hours"),
        ):
            if not (value > 0):
                raise ValueError(f"ExitPolicyConfig.{name} pozitif olmalı, alınan: {value}")


# Used only when the entry-time ATR reference is genuinely unavailable
# (should not happen in normal live operation — warmup already guarantees
# ATR_14's min_history by the time any M5 signal can fire — but a
# stop-loss safety net with SOME conservative protective stop is far safer
# than leaving a real, money-committed position with none). Shared by
# `lifecycle_manager.py` (fresh entries) and `lifecycle_migration.py`
# (legacy positions migrated before market bootstrap has produced a fresh
# ATR snapshot). Documented as a known limitation in the final report.
FALLBACK_STOP_PCT = 0.02
FALLBACK_TARGET_PCT = 0.04


def compute_initial_stop_and_target(
    *, entry_price: float, atr: float, config: ExitPolicyConfig
) -> tuple[float, float]:
    """Entry-time-only computation (Phase 5/6) — `initial_protective_stop`
    and `take_profit` are fixed here and never recalculated afterward."""
    require_finite(entry_price, "entry_price")
    require_finite(atr, "atr")
    if entry_price <= 0:
        raise ValueError(f"entry_price pozitif olmalı, alınan: {entry_price}")
    if atr <= 0:
        raise ValueError(f"atr pozitif olmalı, alınan: {atr}")
    initial_protective_stop = entry_price - config.stop_atr_multiple * atr
    take_profit = entry_price + config.take_profit_atr_multiple * atr
    return max(initial_protective_stop, 0.0), take_profit


@dataclass(frozen=True)
class Candle:
    """Minimal OHLC input the evaluator needs — deliberately independent
    of `domain.models.Candle` so this module has zero import coupling to
    the signal/feature pipeline (kept pure, replay-friendly)."""

    open: float
    high: float
    low: float
    close: float
    close_time: datetime

    def __post_init__(self) -> None:
        for value, name in (
            (self.open, "open"), (self.high, "high"), (self.low, "low"), (self.close, "close"),
        ):
            require_finite(value, name)
            if value <= 0:
                raise ValueError(f"Candle.{name} pozitif olmalı, alınan: {value}")
        require_utc_aware(self.close_time, "close_time")
        if self.low > self.high:
            raise ValueError(f"Candle.low ({self.low}) > Candle.high ({self.high})")


@dataclass(frozen=True)
class LifecyclePriceState:
    """The subset of a bridge position's durable state that
    `evaluate_candle` reads/updates. Kept separate from the full durable
    record (`lifecycle_store.py`) so this module has no persistence
    coupling.

    `entry_price` (== `gross_entry_vwap`, fixed at entry) is carried here
    purely so the trailing-activation threshold can be computed relative
    to actual favorable movement from entry — it is never mutated by this
    module."""

    entry_price: float
    entry_timestamp: datetime
    initial_protective_stop: float
    take_profit: float
    high_water: float
    effective_stop: float
    trailing_active: bool
    last_stop_mechanism: str  # "STOP_LOSS" | "TRAILING_STOP" — which mechanism last raised effective_stop

    def __post_init__(self) -> None:
        require_utc_aware(self.entry_timestamp, "entry_timestamp")
        for value, name in (
            (self.entry_price, "entry_price"),
            (self.initial_protective_stop, "initial_protective_stop"),
            (self.take_profit, "take_profit"),
            (self.high_water, "high_water"),
            (self.effective_stop, "effective_stop"),
        ):
            require_finite(value, name)
        if self.entry_price <= 0:
            raise ValueError(f"entry_price pozitif olmalı, alınan: {self.entry_price}")
        if self.last_stop_mechanism not in ("STOP_LOSS", "TRAILING_STOP"):
            raise ValueError(f"last_stop_mechanism geçersiz: {self.last_stop_mechanism!r}")


@dataclass(frozen=True)
class CandleEvaluationResult:
    """Output of one `evaluate_candle()` call — Phase 6/7/8/9's single
    shared decision point, used identically by live and replay."""

    updated_state: LifecyclePriceState
    exit_reason: ExitReason | None
    atr_used_for_trailing: float | None  # None if ATR was stale/unavailable this candle


def _monotonic_effective_stop(
    *, previous_effective_stop: float, initial_protective_stop: float, trailing_candidate: float | None
) -> float:
    """Phase 6's non-negotiable update rule — `effective_stop` may only
    increase, from any input path (ATR change, trailing recompute,
    restart, migration)."""
    candidates = [previous_effective_stop, initial_protective_stop]
    if trailing_candidate is not None:
        candidates.append(trailing_candidate)
    return max(candidates)


def evaluate_candle(
    state: LifecyclePriceState,
    candle: Candle,
    config: ExitPolicyConfig,
    *,
    atr_for_trailing: float | None,
) -> CandleEvaluationResult:
    """The ONE shared candle-ordering implementation (Phase 6/8/20) — must
    be called identically from live M1 ingestion and from replay, on every
    completed candle for an open LONG position, in this exact order:

    1. Check `candle.low` against the effective_stop that was ALREADY
       active before this candle (`state.effective_stop`) — never a value
       this same candle is about to produce (no lookahead, Phase 8).
    2. Check `candle.high` against the static `take_profit`.
    3. If both fire and true intrabar ordering is unknown, the stop side
       wins conservatively (Phase 6 point 3 / Phase 8).
    4. Only if neither stop nor target fired: check max-hold using the
       candle's own `close_time` (never wall-clock — replay-safe) against
       the authoritative `entry_timestamp` (never reset by a restart, since
       `entry_timestamp` is durable, Phase 6's "Other exit triggers").
    5. Only THEN: update `high_water` from this candle's `high` if it's a
       new favorable extreme, evaluate trailing activation/candidate from
       the freshly updated `high_water`, and raise `effective_stop`
       monotonically. This new value takes effect starting the NEXT candle
       only — it is never used to retroactively re-test this candle's low
       (Phase 6 point 5, Phase 8).

    `atr_for_trailing=None` means the latest M5 ATR reference is stale or
    unavailable this candle (Phase 9) — the trailing recompute step is
    skipped (fails closed for THAT step only); stop/target/max-hold checks
    against ALREADY-set values are unaffected."""
    pre_candle_stop = state.effective_stop
    stop_triggered = candle.low <= pre_candle_stop
    take_profit_triggered = candle.high >= state.take_profit

    exit_reason: ExitReason | None = None
    if stop_triggered:
        exit_reason = (
            ExitReason.TRAILING_STOP if state.last_stop_mechanism == "TRAILING_STOP" else ExitReason.STOP_LOSS
        )
    elif take_profit_triggered:
        exit_reason = ExitReason.TAKE_PROFIT
    elif candle.close_time - state.entry_timestamp >= timedelta(hours=config.max_hold_hours):
        exit_reason = ExitReason.MAX_HOLD

    new_high_water = max(state.high_water, candle.high)

    # Trailing activates once favorable movement from entry reaches
    # `trailing_activation_atr_multiple` ATRs (using the latest valid M5
    # ATR reference, Phase 9) — a one-way latch, never deactivated once set
    # (Phase 6: "Trailing activates only after sufficient favorable price
    # movement"; nothing in the spec un-arms it).
    trailing_active = state.trailing_active
    if not trailing_active and atr_for_trailing is not None and atr_for_trailing > 0:
        activation_distance = config.trailing_activation_atr_multiple * atr_for_trailing
        if new_high_water - state.entry_price >= activation_distance:
            trailing_active = True

    trailing_candidate: float | None = None
    if trailing_active and atr_for_trailing is not None and atr_for_trailing > 0:
        trailing_candidate = new_high_water - config.trailing_distance_atr_multiple * atr_for_trailing

    new_effective_stop = _monotonic_effective_stop(
        previous_effective_stop=state.effective_stop,
        initial_protective_stop=state.initial_protective_stop,
        trailing_candidate=trailing_candidate,
    )
    new_mechanism = (
        "TRAILING_STOP"
        if trailing_candidate is not None and new_effective_stop == trailing_candidate and new_effective_stop > state.effective_stop
        else state.last_stop_mechanism
    )

    updated_state = replace(
        state,
        high_water=new_high_water,
        effective_stop=new_effective_stop,
        trailing_active=trailing_active,
        last_stop_mechanism=new_mechanism,
    )
    return CandleEvaluationResult(
        updated_state=updated_state, exit_reason=exit_reason,
        atr_used_for_trailing=atr_for_trailing if trailing_active else None,
    )


def floor_to_step(value: float, step: float) -> float:
    """Rounds `value` DOWN to a multiple of `step` using `Decimal` (never
    raw float math, which can misalign step boundaries) — shared by SELL
    sizing here and in `signal_bridge.py`."""
    if step <= 0:
        return value
    step_dec = Decimal(str(step))
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step_dec)


def resolve_lot_size_filter(
    *, market_step_size: float | None, market_min_qty: float | None, step_size: float, min_qty: float
) -> tuple[float, float]:
    """Live-validation bug fix: Binance's `exchangeInfo` parser
    (`adapter.py::parse_symbol_filters`) returns `0.0` (NOT `None`) for
    `market_step_size`/`market_min_qty` when a symbol has no SEPARATE
    `MARKET_LOT_SIZE` filter (the common case) — a bare `is not None`
    check then wrongly treats that `0.0` as "the real MARKET_LOT_SIZE step
    is zero", so `floor_to_step(value, step=0.0)` returns `value`
    COMPLETELY UNFLOORED (`floor_to_step`'s own `step <= 0` passthrough).
    This was latent and never triggered before this lifecycle's SELL-sizing
    headroom (Phase 6) — the existing accepted opposite-signal SELL path
    always sells the FULL owned quantity, which is already lot-aligned
    (it came directly from a valid BUY fill), so an unfloored no-op passed
    by coincidence. A HEADROOM-reduced quantity is deliberately NOT
    lot-aligned and must actually be floored, which is what exposed this.
    Treats any non-positive `market_*` value as "unset" and falls back to
    the base `LOT_SIZE` filter, exactly like the `is not None` check
    intended."""
    step = market_step_size if market_step_size is not None and market_step_size > 0 else step_size
    min_qty_effective = market_min_qty if market_min_qty is not None and market_min_qty > 0 else min_qty
    return step, min_qty_effective


@dataclass(frozen=True)
class SellSizingResult:
    quantity: float
    headroom_reserved: float


def compute_sell_quantity(
    *,
    net_owned_base_quantity: float,
    step_size: float,
    min_qty: float,
    sell_commission_headroom_bps: float,
) -> SellSizingResult:
    """Phase 6's SELL-sizing rule. Since this adapter's SELL commission
    could not be reliably confirmed as always quote/BNB-only (Phase 0-C),
    this ALWAYS reserves a conservative worst-case base-asset commission
    headroom before sizing (the headroom-reserved branch, not full-quantity
    sizing) — `sell_commission_headroom_bps` is the exact, documented
    formula surface (default 15bps, 50% above Binance's standard 10bps
    taker fee, applied to `net_owned_base_quantity`)."""
    headroom = net_owned_base_quantity * (sell_commission_headroom_bps / 10_000.0)
    raw_sellable = max(net_owned_base_quantity - headroom, 0.0)
    quantity = floor_to_step(raw_sellable, step_size)
    if quantity < min_qty or quantity <= 0 or quantity > net_owned_base_quantity:
        return SellSizingResult(quantity=0.0, headroom_reserved=headroom)
    return SellSizingResult(quantity=quantity, headroom_reserved=headroom)


# ---------------------------------------------------------------------------
# Durable per-symbol bridge position record (Phase 3) — the full persisted
# shape. `lifecycle_store.py` is the only place that serializes/deserializes
# this to SQLite; this module stays free of any persistence/IO coupling.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BridgePositionRecord:
    """Durable per-symbol bridge position lifecycle state (Phase 3). Every
    field here is either fixed-at-entry, monotonic, or append-only —
    nothing is ever silently overwritten in a way that could lose economic
    history (see field-level docstrings below and the mission's Phase 3
    field list)."""

    symbol: str
    state: PositionLifecycleState
    gross_entry_vwap: float | None = None
    net_owned_base_quantity: float = 0.0
    entry_order_client_order_id: str | None = None  # set while ENTRY_PENDING
    initial_protective_stop: float | None = None
    high_water: float | None = None
    effective_stop: float | None = None
    trailing_active: bool = False
    last_stop_mechanism: str = "STOP_LOSS"
    take_profit: float | None = None
    last_evaluated_candle_close: datetime | None = None
    exit_pending_client_order_id: str | None = None
    last_exit_reason: str | None = None
    entry_timestamp: datetime | None = None
    entry_signal_context_id: str | None = None
    entry_client_order_id: str | None = None
    cumulative_realized_gross_pnl: float = 0.0
    fee_ledger: tuple[FeeLedgerEntry, ...] = ()
    cooldown_until: datetime | None = None
    migrated_existing_position: bool = False
    updated_at: datetime | None = None
    # Adaptive Intelligence v1 — ADDITIVE, backward-compatible fields.
    # These five mirror `ExitPolicyConfig`'s own fields EXACTLY and are
    # embedded INLINE (never a reference requiring an external lookup) so
    # a position's exit behaviour is fully determined by its OWN durable
    # record, frozen at the moment it was opened (or, for a migrated
    # legacy position, at the moment stage-2 finalization ran) — see
    # `resolved_exit_policy()`. `policy_version_id` is an OPAQUE tag this
    # module stores and passes through but never interprets/validates —
    # the `adaptive/` package (which this module never imports) owns its
    # meaning. A position persisted BEFORE this change has all six fields
    # `None` — `resolved_exit_policy()` treats that as "opened under
    # today's actual default policy", never crashing, never silently
    # changing that position's already-observed behaviour.
    exit_policy_stop_atr_multiple: float | None = None
    exit_policy_take_profit_atr_multiple: float | None = None
    exit_policy_trailing_activation_atr_multiple: float | None = None
    exit_policy_trailing_distance_atr_multiple: float | None = None
    exit_policy_max_hold_hours: float | None = None
    policy_version_id: str | None = None

    def resolved_exit_policy(self, *, default: "ExitPolicyConfig") -> "ExitPolicyConfig":
        """The ONE authoritative way to obtain the `ExitPolicyConfig` this
        SPECIFIC position must keep using for its entire remaining life
        (Adaptive Intelligence v1 — the position-pinning invariant: a
        champion/policy swap must NEVER retroactively change an
        already-open position's trailing/max-hold behaviour). Returns the
        embedded values when ALL FIVE are present (a position opened
        under this change); falls back to `default` (the caller's
        current static `ExitPolicyConfig`) when even ONE is missing — the
        only way that happens is a position persisted before this change,
        or the permanently-fixed LOT_SIZE dust path, which never reaches
        this method."""
        fields = (
            self.exit_policy_stop_atr_multiple, self.exit_policy_take_profit_atr_multiple,
            self.exit_policy_trailing_activation_atr_multiple, self.exit_policy_trailing_distance_atr_multiple,
            self.exit_policy_max_hold_hours,
        )
        if any(f is None for f in fields):
            return default
        return ExitPolicyConfig(
            stop_atr_multiple=self.exit_policy_stop_atr_multiple,  # type: ignore[arg-type]
            take_profit_atr_multiple=self.exit_policy_take_profit_atr_multiple,  # type: ignore[arg-type]
            trailing_activation_atr_multiple=self.exit_policy_trailing_activation_atr_multiple,  # type: ignore[arg-type]
            trailing_distance_atr_multiple=self.exit_policy_trailing_distance_atr_multiple,  # type: ignore[arg-type]
            max_hold_hours=self.exit_policy_max_hold_hours,  # type: ignore[arg-type]
        )

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if self.updated_at is None:
            raise ValueError("BridgePositionRecord.updated_at zorunlu (None olamaz)")
        require_utc_aware(self.updated_at, "updated_at")
        if self.net_owned_base_quantity < 0:
            raise ValueError("net_owned_base_quantity negatif olamaz")
        if self.state is PositionLifecycleState.LONG:
            missing = [
                name
                for name, value in (
                    ("gross_entry_vwap", self.gross_entry_vwap),
                    ("initial_protective_stop", self.initial_protective_stop),
                    ("high_water", self.high_water),
                    ("effective_stop", self.effective_stop),
                    ("take_profit", self.take_profit),
                    ("entry_timestamp", self.entry_timestamp),
                )
                if value is None
            ]
            if missing:
                raise ValueError(f"state=LONG için zorunlu alanlar eksik: {missing}")
            if self.net_owned_base_quantity <= 0:
                raise ValueError("state=LONG iken net_owned_base_quantity > 0 olmalı")
        if self.state is PositionLifecycleState.RECOVERED:
            # Phase 19 stage 1: ownership only — market-dependent fields
            # are deliberately NOT required (and must not be fabricated)
            # until stage 2 (`finalize_legacy_lifecycle_init`) runs.
            missing = [
                name
                for name, value in (
                    ("gross_entry_vwap", self.gross_entry_vwap),
                    ("entry_timestamp", self.entry_timestamp),
                    ("entry_client_order_id", self.entry_client_order_id),
                )
                if value is None
            ]
            if missing:
                raise ValueError(f"state=RECOVERED için zorunlu alanlar eksik: {missing}")
            if self.net_owned_base_quantity <= 0:
                raise ValueError("state=RECOVERED iken net_owned_base_quantity > 0 olmalı")
            if not self.migrated_existing_position:
                raise ValueError("state=RECOVERED yalnızca migrated_existing_position=True ile geçerlidir")

    def to_price_state(self) -> LifecyclePriceState:
        """Extracts the pure-evaluator subset — only valid for `state ==
        LONG` (enforced by `__post_init__` requiring these fields then)."""
        if self.state is not PositionLifecycleState.LONG:
            raise ValueError(f"to_price_state() yalnızca state=LONG için geçerlidir, alınan: {self.state}")
        return LifecyclePriceState(
            entry_price=self.gross_entry_vwap,  # type: ignore[arg-type]
            entry_timestamp=self.entry_timestamp,  # type: ignore[arg-type]
            initial_protective_stop=self.initial_protective_stop,  # type: ignore[arg-type]
            take_profit=self.take_profit,  # type: ignore[arg-type]
            high_water=self.high_water,  # type: ignore[arg-type]
            effective_stop=self.effective_stop,  # type: ignore[arg-type]
            trailing_active=self.trailing_active,
            last_stop_mechanism=self.last_stop_mechanism,
        )

    def with_price_state(
        self, price_state: LifecyclePriceState, *, now: datetime, candle_close_time: datetime | None = None
    ) -> "BridgePositionRecord":
        """Merges an updated `LifecyclePriceState` (from `evaluate_candle`)
        back into the full durable record, leaving every other field
        (fee_ledger, entry identity, realized P&L, cooldown, ...)
        untouched.

        CRITICAL FIX (C2 — mainnet-readiness review): `last_evaluated_
        candle_close` MUST be the triggering M1 candle's own authoritative
        `close_time`, never wall-clock `now`. Storing wall-clock here was
        the root cause of a naming/correctness bug where a bookkeeping
        field promising "the candle this position was last evaluated
        against" actually held "whenever this process happened to run" —
        harmless on its own (nothing else in this codebase reads this
        field), but a footgun for any future code that reasonably assumes
        it means what it says. `candle_close_time` defaults to `now` only
        for backward-compatible call sites that don't have a candle in
        scope; the real M1 path (`evaluate_m1_candle`) always passes the
        real one explicitly."""
        return replace(
            self,
            high_water=price_state.high_water,
            effective_stop=price_state.effective_stop,
            trailing_active=price_state.trailing_active,
            last_stop_mechanism=price_state.last_stop_mechanism,
            last_evaluated_candle_close=candle_close_time if candle_close_time is not None else now,
            updated_at=now,
        )


def flat_record(symbol: str, *, now: datetime) -> BridgePositionRecord:
    return BridgePositionRecord(symbol=symbol, state=PositionLifecycleState.FLAT, updated_at=now)


def compute_total_exposure(positions: Sequence[BridgePositionRecord]) -> float:
    """Portfolio/Accounting v1 — the ONE authoritative "total exposure"
    formula: cost-basis (`gross_entry_vwap * net_owned_base_quantity`,
    NOT mark-to-market) summed over every NON-FLAT position, INCLUDING
    `DUST` (still-owned, still-unsold inventory that is still economically
    real and must still count toward `max_total_exposure_usdt` — see
    `max_exposure_gate_open`'s own docstring). Extracted from what was
    previously two independent, duplicated implementations
    (`LifecycleManager.entry_gate()` and `app.py::_bridge_lifecycle_
    snapshot()`) — both now call this SAME function, so the risk gate's
    notion of exposure and the dashboard's displayed exposure can never
    silently drift apart again."""
    total = 0.0
    for position in positions:
        if position.state is not PositionLifecycleState.FLAT and position.gross_entry_vwap is not None:
            total += position.gross_entry_vwap * position.net_owned_base_quantity
    return total


# ---------------------------------------------------------------------------
# Risk gates (Phase 10/11) — pure decision functions. Callers own reading
# current state (open positions, today's completed trades) and pass
# summarized numbers in; nothing here touches a clock, a store, or a
# network.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RiskPolicyConfig:
    max_open_positions: int = 8
    max_total_exposure_usdt: float = 100.0
    cooldown_minutes: float = 30.0
    daily_loss_limit_usdt: float = 50.0
    unknown_fee_conservative_reserve_usdt: float = 0.05
    sell_commission_headroom_bps: float = 15.0

    def __post_init__(self) -> None:
        if self.max_open_positions < 1:
            raise ValueError("max_open_positions >= 1 olmalı")
        for value, name in (
            (self.max_total_exposure_usdt, "max_total_exposure_usdt"),
            (self.cooldown_minutes, "cooldown_minutes"),
            (self.daily_loss_limit_usdt, "daily_loss_limit_usdt"),
            (self.unknown_fee_conservative_reserve_usdt, "unknown_fee_conservative_reserve_usdt"),
            (self.sell_commission_headroom_bps, "sell_commission_headroom_bps"),
        ):
            if not (value > 0):
                raise ValueError(f"RiskPolicyConfig.{name} pozitif olmalı, alınan: {value}")


def max_positions_gate_open(*, open_slot_count: int, config: RiskPolicyConfig) -> bool:
    """Phase 10 — DUST positions never occupy a slot (callers must exclude
    them from `open_slot_count`); ENTRY_PENDING/EXIT_PENDING/AMBIGUOUS DO
    reserve a slot (callers must include them)."""
    return open_slot_count < config.max_open_positions


def max_exposure_gate_open(
    *, current_exposure_usdt: float, additional_notional_usdt: float, config: RiskPolicyConfig
) -> bool:
    """Phase 10 — `current_exposure_usdt` MUST include DUST (still owned
    inventory, still marked-to-market)."""
    return (current_exposure_usdt + additional_notional_usdt) <= config.max_total_exposure_usdt


def cooldown_clear(*, cooldown_until: datetime | None, now: datetime) -> bool:
    return cooldown_until is None or now >= cooldown_until


def compute_cooldown_until(*, exit_time: datetime, config: RiskPolicyConfig) -> datetime:
    return exit_time + timedelta(minutes=config.cooldown_minutes)


@dataclass(frozen=True)
class DailyRiskAccumulator:
    """Phase 11's `daily_conservative_risk_pnl` accumulator for one UTC
    trading day. Deliberately pessimistic; never surfaced as authoritative
    net P&L anywhere (see dashboard wiring)."""

    trading_day: str  # "YYYY-MM-DD", UTC calendar day
    conservative_risk_pnl: float = 0.0
    trades_counted: int = 0

    def __post_init__(self) -> None:
        require_finite(self.conservative_risk_pnl, "conservative_risk_pnl")
        if self.trades_counted < 0:
            raise ValueError("trades_counted negatif olamaz")


def trading_day_key(moment: datetime) -> str:
    require_utc_aware(moment, "moment")
    return moment.date().isoformat()


@dataclass(frozen=True)
class TradeRiskContribution:
    """One completed trade's contribution to `daily_conservative_risk_pnl`
    (Phase 11) — computed once, at trade-completion time, from that
    trade's own gross P&L and fee ledger."""

    conservative_pnl: float
    fee_basis: str  # "KNOWN" | "RESERVED" | "UNRESOLVED"


def compute_trade_risk_contribution(
    *, gross_pnl: float, fee_ledger_entries: tuple[FeeLedgerEntry, ...], config: RiskPolicyConfig
) -> TradeRiskContribution:
    """Phase 11's per-trade contribution: subtract every RELIABLY known fee
    (USDT-converted) — never treat a known fee as zero — and, for any fee
    leg that could not be converted, subtract the configured conservative
    reserve instead of ignoring it. If a fee leg exists but neither a known
    value nor a usable reserve applies, `fee_basis="UNRESOLVED"` signals the
    caller to fail closed for new entries on this symbol (never silently
    treated as zero)."""
    known_fees_usdt = 0.0
    unresolved_legs = 0
    for entry in fee_ledger_entries:
        if entry.usdt_equivalent is not None:
            known_fees_usdt += entry.usdt_equivalent
        else:
            unresolved_legs += 1

    if unresolved_legs == 0:
        return TradeRiskContribution(conservative_pnl=gross_pnl - known_fees_usdt, fee_basis="KNOWN")

    reserved = unresolved_legs * config.unknown_fee_conservative_reserve_usdt
    return TradeRiskContribution(
        conservative_pnl=gross_pnl - known_fees_usdt - reserved, fee_basis="RESERVED"
    )


def apply_trade_to_daily_accumulator(
    accumulator: DailyRiskAccumulator, contribution: TradeRiskContribution
) -> DailyRiskAccumulator:
    return replace(
        accumulator,
        conservative_risk_pnl=accumulator.conservative_risk_pnl + contribution.conservative_pnl,
        trades_counted=accumulator.trades_counted + 1,
    )


def daily_loss_breaker_tripped(accumulator: DailyRiskAccumulator, config: RiskPolicyConfig) -> bool:
    """Blocks new entries only — callers must never call this for exits."""
    return accumulator.conservative_risk_pnl <= -config.daily_loss_limit_usdt
