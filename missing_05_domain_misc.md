<!-- missing_05_domain_misc.md — 37 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/portfolio/__init__.py (615 bytes) -->
<!--   - crypto_signal_engine/portfolio/accounting.py (10216 bytes) -->
<!--   - adaptive/__init__.py (3304 bytes) -->
<!--   - adaptive/challenger.py (5115 bytes) -->
<!--   - adaptive/cycle.py (15944 bytes) -->
<!--   - adaptive/decision.py (9736 bytes) -->
<!--   - adaptive/drift_signal.py (2447 bytes) -->
<!--   - adaptive/evaluation.py (6234 bytes) -->
<!--   - adaptive/policy.py (4758 bytes) -->
<!--   - adaptive/rollback.py (12768 bytes) -->
<!--   - adaptive/scheduler.py (9081 bytes) -->
<!--   - adaptive/symbol_score.py (5917 bytes) -->
<!--   - adaptive/windows.py (4828 bytes) -->
<!--   - crypto_signal_engine/errors.py (6974 bytes) -->
<!--   - crypto_signal_engine/__init__.py (573 bytes) -->
<!--   - crypto_signal_engine/signal_engine.py (5074 bytes) -->
<!--   - research/attribution.py (10561 bytes) -->
<!--   - research/data_quality.py (13474 bytes) -->
<!--   - research/drift.py (3982 bytes) -->
<!--   - research/errors.py (1864 bytes) -->
<!--   - research/guardrails.py (6267 bytes) -->
<!--   - research/monte_carlo.py (4632 bytes) -->
<!--   - research/oos_stability.py (4923 bytes) -->
<!--   - research/orderbook_capture.py (7775 bytes) -->
<!--   - research/sizing.py (12891 bytes) -->
<!--   - scripts/adaptive_evaluation_cycle.py (5990 bytes) -->
<!--   - scripts/backup_sqlite.py (8711 bytes) -->
<!--   - scripts/binance_testnet_lab.py (14321 bytes) -->
<!--   - scripts/historical_replay.py (7686 bytes) -->
<!--   - scripts/lifecycle_replay_sanity.py (6551 bytes) -->
<!--   - scripts/live_public_smoke_test.py (3665 bytes) -->
<!--   - scripts/local_readiness_check.py (4541 bytes) -->
<!--   - scripts/monte_carlo_robustness.py (4196 bytes) -->
<!--   - scripts/oos_stability.py (4057 bytes) -->
<!--   - scripts/public_soak_test.py (7156 bytes) -->
<!--   - scripts/run_with_adaptive_policy.py (14404 bytes) -->
<!--   - scripts/verify_phase1.py (5447 bytes) -->

=== FILE: crypto_signal_engine/portfolio/__init__.py ===
"""
Portfolio/Accounting v1 — the authoritative, reusable accounting layer
for the autonomous Testnet trading lifecycle bridge.

Kapsam sınırı (KASITLI): bu paket YALNIZCA zaten var olan durable state
(`BridgePositionRecord`, `bridge_completed_trade` toplamları, exchange
bakiye sorgusu) üzerinden salt-okunur, saf muhasebe/gözlemlenebilirlik
hesaplar — hiçbir order göndermez, hiçbir mevcut risk kapısını
(`RiskPolicyConfig`/`entry_gate()`) değiştirmez veya BLOKE ETMEZ. Bu bir
düzeltme + gözlemlenebilirlik milestone'udur, YENİ bir kontrol DEĞİLDİR."""

from __future__ import annotations


=== FILE: crypto_signal_engine/portfolio/accounting.py ===
"""
Portfolio/Accounting v1, step 3 — the ONE authoritative, reusable,
PURE accounting snapshot for the autonomous Testnet trading lifecycle
bridge, replacing the ad-hoc, duplicated/inconsistent math previously
scattered across `LifecycleManager.entry_gate()` (exposure) and
`app.py::_bridge_lifecycle_snapshot()` (exposure AGAIN, plus a
bounded-limit-50/GROSS "realized P&L" and "lifetime P&L" that were
silently different numbers from the real, authoritative,
`bridge_daily_risk`-persisted, NET risk-breaker accumulator).

PURE, READ-ONLY, NO I/O OF ITS OWN — same discipline as `adaptive.
decision.evidence_from_pnls`/`research.attribution._compute_metrics`:
this module takes already-fetched data as plain arguments and performs
zero database/network access itself. It deliberately does NOT import
`crypto_signal_engine.execution.lifecycle_store.LifecycleStore` (the
data-access layer) — only pure types/functions from `crypto_signal_
engine.execution.lifecycle` (`BridgePositionRecord`, `PositionLifecycleState`,
`compute_total_exposure`, `unrealized_gross_pnl`) — mirroring the
`adaptive/symbol_score.py` precedent of keeping a pure computation
module decoupled from the store that happens to feed it. The caller
(`app.py`) owns fetching `LifecycleStore.list_positions()`/`total_
realized_pnl()`/`realized_pnl_for_day()` and wiring the results in here.

Nothing in this module can submit an order, block an entry, or alter any
existing `RiskPolicyConfig` gate — it is read-only observability +
correctness, never a new control.

Step 5 — `AssetBalance`/`parse_account_balances()`: minimal, typed
parsing for Binance's `GET /api/v3/account` payload (`testnet_client.
py::account_info()` — already implemented, already working, previously
called from EXACTLY ZERO places). Deliberately kept PURE here too (no
network call — this module only parses an already-fetched raw `dict`);
the actual `account_info()` call, and its defensive "never raise, never
block trading, degrade to `None` on any failure" wrapping, lives in
`ExecutionReconciliationService` (see `execution/reconciliation_service.
py::check_usdt_balance` and this milestone's DECISIONS.md entry for why
it lives there rather than here — it already owns the exchange client
and the "compare internal state against exchange truth" pattern this
extends, and this module stays free of any exchange-client dependency)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from crypto_signal_engine.domain._validation import require_finite, require_utc_aware
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    PositionLifecycleState,
    compute_total_exposure,
    unrealized_gross_pnl,
)


@dataclass(frozen=True)
class AccountingSnapshot:
    """One point-in-time, fully-computed portfolio accounting snapshot.

    `unrealized_pnl_usdt` is `None` whenever ANY open position's live
    price lookup failed or returned nothing — NEVER a partial/fabricated
    sum over only the positions that happened to resolve (same
    discipline `app.py`'s existing `unrealized_gross_pnl_total` already
    followed; this module reuses that discipline rather than inventing a
    new one). `lifetime_realized_pnl_net_usdt`/`lifetime_trade_count` and
    `today_realized_pnl_net_usdt`/`today_trade_count` are expected to be
    computed via `LifecycleStore.total_realized_pnl()`/`realized_pnl_
    for_day()` (step 2's SQL-aggregate functions) — TRUE lifetime/daily
    totals, NET of fees, never a bounded/gross approximation."""

    generated_at: datetime
    open_position_count: int
    total_exposure_usdt: float
    unrealized_pnl_usdt: float | None
    lifetime_realized_pnl_net_usdt: float
    lifetime_trade_count: int
    today_realized_pnl_net_usdt: float
    today_trade_count: int

    def __post_init__(self) -> None:
        require_utc_aware(self.generated_at, "generated_at")
        for value, name in (
            (self.total_exposure_usdt, "total_exposure_usdt"),
            (self.lifetime_realized_pnl_net_usdt, "lifetime_realized_pnl_net_usdt"),
            (self.today_realized_pnl_net_usdt, "today_realized_pnl_net_usdt"),
        ):
            require_finite(value, name)
        if self.unrealized_pnl_usdt is not None:
            require_finite(self.unrealized_pnl_usdt, "unrealized_pnl_usdt")
        if self.open_position_count < 0:
            raise ValueError("open_position_count negatif olamaz")
        if self.lifetime_trade_count < 0:
            raise ValueError("lifetime_trade_count negatif olamaz")
        if self.today_trade_count < 0:
            raise ValueError("today_trade_count negatif olamaz")


def _has_sellable_quantity(position: BridgePositionRecord) -> bool:
    """Mirrors `app.py`'s existing `has_quantity` condition exactly (the
    same test that already gated per-position `latest_price`/`unrealized_
    gross_pnl` lookups there) — never a second, divergent definition of
    "this position needs a live price"."""
    return (
        position.state is not PositionLifecycleState.FLAT
        and position.gross_entry_vwap is not None
        and position.net_owned_base_quantity > 0
    )


def build_accounting_snapshot(
    *,
    positions: Sequence[BridgePositionRecord],
    price_lookup: Callable[[str], float | None],
    lifetime_realized_pnl_net_usdt: float,
    lifetime_trade_count: int,
    today_realized_pnl_net_usdt: float,
    today_trade_count: int,
    now: datetime,
) -> AccountingSnapshot:
    """The ONE function that computes a full `AccountingSnapshot`.

    `positions`: every currently-tracked `BridgePositionRecord` (e.g.
    `LifecycleStore.list_positions()`'s result) — this function itself
    never fetches them.

    `price_lookup(symbol)`: caller-supplied, e.g. `Application.
    _latest_price` — called ONCE per open (non-FLAT, quantified) position
    to obtain a live mark for unrealized P&L. Defensively wrapped here:
    if it RAISES for any relevant symbol, that is treated exactly like a
    `None` return (unresolvable price) — `unrealized_pnl_usdt` becomes
    `None` for the WHOLE snapshot, never a partial sum silently excluding
    the failed symbol.

    `lifetime_realized_pnl_net_usdt`/`lifetime_trade_count`/`today_
    realized_pnl_net_usdt`/`today_trade_count`: already-computed
    aggregates (see `LifecycleStore.total_realized_pnl()`/`realized_pnl_
    for_day()`) — this function performs no trade-history aggregation of
    its own, only the exposure/unrealized computation over `positions`.

    Deterministic for identical inputs (no wall-clock read beyond the
    caller-supplied `now`, no hidden state)."""
    require_utc_aware(now, "now")

    open_position_count = sum(1 for position in positions if position.state is not PositionLifecycleState.FLAT)
    total_exposure_usdt = compute_total_exposure(positions)

    unrealized_total = 0.0
    unrealized_known = True
    for position in positions:
        if not _has_sellable_quantity(position):
            continue
        try:
            price = price_lookup(position.symbol)
        except Exception:  # noqa: BLE001 - a price-lookup failure must NEVER crash this snapshot, only mark it unknown
            price = None
        if price is None:
            unrealized_known = False
            continue
        unrealized_total += unrealized_gross_pnl(
            gross_entry_vwap=position.gross_entry_vwap,  # type: ignore[arg-type]
            net_owned_base_quantity=position.net_owned_base_quantity,
            current_price=price,
        )

    return AccountingSnapshot(
        generated_at=now,
        open_position_count=open_position_count,
        total_exposure_usdt=total_exposure_usdt,
        unrealized_pnl_usdt=unrealized_total if unrealized_known else None,
        lifetime_realized_pnl_net_usdt=lifetime_realized_pnl_net_usdt,
        lifetime_trade_count=lifetime_trade_count,
        today_realized_pnl_net_usdt=today_realized_pnl_net_usdt,
        today_trade_count=today_trade_count,
    )


@dataclass(frozen=True)
class AssetBalance:
    """One asset's free/locked balance, parsed from Binance's `GET
    /api/v3/account` response. Deliberately minimal — the raw payload
    (which also carries account-level permission flags, commission
    rates, and update timestamps this bridge has no use for) is NEVER
    leaked further than `parse_account_balances()` below, mirroring
    `ExecutionResult`'s own "raw_response never sızdırılır" rule."""

    asset: str
    free: float
    locked: float

    def __post_init__(self) -> None:
        if not self.asset:
            raise ValueError("AssetBalance.asset boş olamaz")
        require_finite(self.free, "free")
        require_finite(self.locked, "locked")
        if self.free < 0:
            raise ValueError(f"AssetBalance.free negatif olamaz, alınan: {self.free}")
        if self.locked < 0:
            raise ValueError(f"AssetBalance.locked negatif olamaz, alınan: {self.locked}")


def parse_account_balances(raw: dict) -> tuple[AssetBalance, ...]:
    """Parses `testnet_client.py::account_info()`'s raw JSON payload's
    `"balances"` array into typed `AssetBalance` records. Every OTHER
    top-level field (`accountType`, `permissions`, `commissionRates`,
    `updateTime`, ...) is deliberately ignored — this function's only job
    is the balances array, and the raw `dict` itself is never returned or
    otherwise propagated by this function's caller (see `Execution
    ReconciliationService.check_usdt_balance`)."""
    balances_raw = raw.get("balances", [])
    if not isinstance(balances_raw, list):
        raise ValueError("account_info payload'ında 'balances' bir liste değil")
    balances = []
    for entry in balances_raw:
        asset = entry.get("asset")
        free = entry.get("free")
        locked = entry.get("locked")
        if asset is None or free is None or locked is None:
            raise ValueError(f"eksik balance alanı (asset/free/locked): {entry!r}")
        balances.append(AssetBalance(asset=asset, free=float(free), locked=float(locked)))
    return tuple(balances)


=== FILE: adaptive/__init__.py ===
"""
Adaptive Intelligence v1 — automated `ExitPolicyConfig` tuning layer.

This package is ADDITIVE and lives OUTSIDE the `crypto_signal_engine/`
package boundary, sibling to `research/` (whose evaluation building
blocks it reuses unmodified: `research.replay`, `research.oos_stability`,
`research.monte_carlo`, `research.attribution`, `research.drift`). It
selects VALUES for `crypto_signal_engine.execution.lifecycle.
ExitPolicyConfig`'s five existing fields — it never writes, generates, or
modifies Python source code, and it never touches `RiskPolicyConfig`
(portfolio-wide human-set safety ceiling — never adaptive, ever) or
anything in `agents/`/`consensus/`/RiskOverlay.

Hard rules that apply to every module in this package:

- `crypto_signal_engine/` (app.py included) NEVER imports anything from
  `adaptive/` — only the reverse. The one place production code opts
  into adaptive policy is a small, ADDITIVE, optional
  `exit_policy_provider` callable parameter that `crypto_signal_engine/`
  already owns the *type* of (see `execution/lifecycle_manager.py`); the
  actual champion-reading provider is constructed in the separate
  top-level entrypoint `scripts/run_with_adaptive_policy.py`, which is
  the only file importing both packages.
- No second implementation of the trading strategy, signal generator, or
  consensus/backtest engine. Discovery/confirmation/shadow evaluation
  reuse `research.replay.HistoricalReplayDriver`,
  `crypto_signal_engine.execution.lifecycle_replay_sanity.
  simulate_lifecycle_exits`, and `crypto_signal_engine.execution.
  lifecycle.evaluate_candle`/`compute_initial_stop_and_target` exactly as
  they exist — this package only selects `ExitPolicyConfig` values to
  feed them and evaluates the results.
- Reading pure TYPE/FUNCTION definitions from
  `crypto_signal_engine.execution.lifecycle` (`ExitPolicyConfig`,
  `compute_initial_stop_and_target`, `evaluate_candle`,
  `LifecyclePriceState`, `Candle`) and from `crypto_signal_engine.
  execution.lifecycle_replay_sanity` (`simulate_lifecycle_exits`,
  `LifecycleSanityReport`) is fine — these are pure functions/dataclasses
  with no order-submission side effect. Importing any ORDER-SUBMISSION
  module (`lifecycle_manager`, `signal_bridge`, `reconciliation_service`,
  `reconciliation_store`, `testnet_client`, `adapter`, `factory`) is
  forbidden — see `tests/test_repository_safety_scan_adaptive.py`.
- `ALLOW_LIVE_TRADING` is never imported, read, or mutated by this
  package.
- No uncontrolled Binance REST fan-out: shadow evaluation (step 8) makes
  ZERO new network calls of its own — it only observes candles the
  already-running live runtime already fetched, via a defensive,
  optional hook. The scheduler/CLI reuse the existing rate-limited
  historical-candle client `research/` already uses.
- No unseeded randomness: challenger generation and any Monte Carlo
  robustness screening take an explicit, caller-provided seed via a
  LOCAL `random.Random(seed)` and never touch global `random` state
  (same discipline as `research.monte_carlo`).
- Pure/deterministic wherever possible: for a given (champion, seed,
  historical data), challenger generation and discovery evaluation must
  produce identical output on every run.
"""

from __future__ import annotations


=== FILE: adaptive/challenger.py ===
"""
Adaptive Intelligence v1, step 5 — challenger generation.

Given a champion `PolicySnapshot` and a caller-supplied seed, generate a
bounded set of challengers by perturbing each `ExitPolicyConfig` field
within an explicit, documented percentage band. Pure and deterministic
for a given (champion, seed, count, perturbation_pct): calling this twice
with the same arguments produces byte-identical challengers.

PERTURBATION BAND — ±15% (`DEFAULT_PERTURBATION_PCT = 0.15`):
Chosen because it is large enough to produce a materially different
trailing/stop/target/max-hold behavior worth spending discovery-window
compute on (the five fields interact multiplicatively with ATR, so even
a 10-15% multiplier change measurably shifts stop distance and holding
time), while small enough that no challenger strays into a qualitatively
different risk posture than the current champion in one hop -- the
champion can only drift gradually, round over round, never jump straight
from e.g. a 2.0 ATR stop to a 4.0 ATR stop in a single promotion. This
mirrors the discipline of `research.monte_carlo`'s LOCAL, explicitly
seeded `random.Random` -- global `random` state is never touched, so
challenger generation never interacts with any other seeded process in
the same run."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone

from adaptive.policy import PolicySnapshot

DEFAULT_PERTURBATION_PCT = 0.15

# `ExitPolicyConfig.__post_init__` only requires each field to be
# strictly positive. That alone is not a sane bound for an AUTOMATED
# tuner (an unbounded stop_atr_multiple could make positions functionally
# unstoppable; an unbounded max_hold_hours could hold a position for
# months) -- these are additional, adaptive/-owned sanity clamps applied
# AFTER perturbation, independent of and stricter than
# ExitPolicyConfig's own bare-positivity rule.
_FIELD_BOUNDS: dict[str, tuple[float, float]] = {
    "stop_atr_multiple": (0.5, 6.0),
    "take_profit_atr_multiple": (0.5, 10.0),
    "trailing_activation_atr_multiple": (0.5, 6.0),
    "trailing_distance_atr_multiple": (0.5, 6.0),
    "max_hold_hours": (1.0, 168.0),  # 1 hour .. 7 days
}

_TUNABLE_FIELDS = tuple(_FIELD_BOUNDS.keys())


@dataclass(frozen=True)
class ChallengerGenerationConfig:
    perturbation_pct: float = DEFAULT_PERTURBATION_PCT
    count: int = 8

    def __post_init__(self) -> None:
        if not (0.0 < self.perturbation_pct < 1.0):
            raise ValueError(f"perturbation_pct (0, 1) aralığında olmalı, alınan: {self.perturbation_pct}")
        if self.count < 1:
            raise ValueError(f"count en az 1 olmalı, alınan: {self.count}")


def _clamp(value: float, bounds: tuple[float, float]) -> float:
    low, high = bounds
    return min(max(value, low), high)


def _perturb_one(
    champion: PolicySnapshot, *, rng: random.Random, perturbation_pct: float, index: int, created_at: datetime,
) -> PolicySnapshot:
    values: dict[str, float] = {}
    for field in _TUNABLE_FIELDS:
        base = getattr(champion, field)
        factor = 1.0 + rng.uniform(-perturbation_pct, perturbation_pct)
        values[field] = _clamp(base * factor, _FIELD_BOUNDS[field])

    return PolicySnapshot(
        version_id=f"challenger-of-{champion.version_id}-{index}",
        stop_atr_multiple=values["stop_atr_multiple"],
        take_profit_atr_multiple=values["take_profit_atr_multiple"],
        trailing_activation_atr_multiple=values["trailing_activation_atr_multiple"],
        trailing_distance_atr_multiple=values["trailing_distance_atr_multiple"],
        max_hold_hours=values["max_hold_hours"],
        created_at=created_at,
        provenance=(
            f"challenger of {champion.version_id}, seed-derived, "
            f"perturbation=±{perturbation_pct * 100:.0f}%, index={index}"
        ),
    )


def generate_challengers(
    champion: PolicySnapshot, *, seed: int, config: ChallengerGenerationConfig | None = None,
    created_at: datetime | None = None,
) -> tuple[PolicySnapshot, ...]:
    """Pure, deterministic for a given (champion, seed, config,
    created_at): a LOCAL `random.Random(seed)` drives every perturbation
    -- global `random` state is never read or mutated. Each of the five
    `ExitPolicyConfig` fields is perturbed independently (not just one
    field at a time) so a single challenger represents one coherent,
    internally-consistent candidate policy, then clamped to
    `_FIELD_BOUNDS`. `created_at` defaults to wall-clock "now" (the real
    caller has no reason to backdate a challenger it just generated) but
    is an explicit parameter so tests/replay callers can pin it for exact
    reproducibility, per this package's "no wall-clock dependency where
    determinism matters" discipline."""
    cfg = config or ChallengerGenerationConfig()
    stamp = created_at or datetime.now(timezone.utc)
    rng = random.Random(seed)
    return tuple(
        _perturb_one(champion, rng=rng, perturbation_pct=cfg.perturbation_pct, index=i, created_at=stamp)
        for i in range(cfg.count)
    )


=== FILE: adaptive/cycle.py ===
"""
Adaptive Intelligence v1, step 13 — the shared core evaluation-cycle
function. BOTH `adaptive/scheduler.py` (the live periodic background
task) and `scripts/adaptive_evaluation_cycle.py` (the manual CLI) call
this EXACT SAME `run_adaptive_cycle()` function — no duplicated logic
between them, mirroring `crypto_signal_engine.runtime.
reselection_scheduler.ReselectionScheduler`'s own `run_once()`/CLI-
callable split discipline.

Each firing runs ONE full cycle:

 1. Resolve the current champion from `store` (bootstrap the default,
    bit-for-bit-equivalent-to-today's-static-config champion if this is
    the very first cycle ever run against this store).
 2. Generate a bounded set of challengers (step 5) from the champion.
 3. Compute this cycle's discovery+confirmation windows (step 6), using
    the store's own persisted `last_confirmation_end` cursor for
    restart-durability. If the next confirmation window has not elapsed
    yet, discovery/confirmation/promotion evaluation is skipped
    entirely for this cycle — there is genuinely nothing new to
    evaluate, and the shadow evidence that WOULD feed a promotion
    decision only grows via live candles between cycles anyway, not
    inside this function.
 4. Evaluate every challenger AND the champion itself (as the
    comparison baseline) against the discovery window (step 7), reusing
    ONE shared replay per window; a challenger that beats the champion
    on discovery survives to confirmation — no reason to spend a
    scarce, once-only confirmation window on one that already loses on
    freely-reusable discovery data.
 5. Evaluate every discovery-surviving challenger (and the champion)
    against the confirmation window (SAME `evaluate_challenger`
    function, just a different window, step 9) — this consumes the
    confirmation window exactly once; the cursor is then advanced so no
    future cycle can reuse it. A challenger that ALSO beats the champion
    on confirmation is marked shadow-eligible (persisted, step 8/12).
    Actually STARTING a shadow happens separately, at the moment a live
    position opens (`adaptive.shadow.detect_new_long_entries`/
    `start_shadow`), since this pure cycle function has no live candle
    access of its own.
 6. For EVERY shadow-eligible challenger (this cycle's new ones AND
    every one marked by an earlier cycle), look up its accumulated
    shadow evidence (persisted across restarts, step 12) and the
    champion's REAL live evidence (via `champion_live_evidence_
    provider`) and run the promotion decision (steps 9-10), logging it
    regardless of outcome and applying PROMOTE by writing a new champion
    to the store. Discovery/confirmation evidence for a challenger from
    an earlier cycle (no longer in this cycle's freshly-generated set)
    is re-evaluated fresh against THIS cycle's windows — never a stale,
    persisted value — the same "confirmation used once, never cached"
    discipline windows.py itself follows. Each decision logged this
    cycle carries this cycle's drift summary (step 11, see below) as an
    auxiliary, read-only note — computed ONCE per cycle whenever fresh
    windows exist, never fed into `decide()` itself.

`champion_live_evidence_provider`: an OPTIONAL callable,
`Callable[[str], EvidenceSummary]`, given the current champion's
`version_id`, returning its REAL Testnet/PAPER evidence (attributed via
`policy_version_id`, step 3). This is deliberately NOT computed inside
`adaptive/` — reading `LifecycleStore.completed_trades_for_policy_
version()` requires importing `crypto_signal_engine.execution.
lifecycle_store`, which is outside `adaptive/`'s permitted narrow
exception (`lifecycle`/`lifecycle_replay_sanity` only). The real
provider is constructed in `scripts/run_with_adaptive_policy.py` (the
only file importing both packages). When omitted, the champion's shadow
evidence defaults to `adaptive.decision.EMPTY_EVIDENCE`, which can only
ever make promotion HARDER (an empty baseline is never "beaten" by a
comparison requiring `challenger > champion`), never falsely easier.

`champion_live_pnls_provider`: an OPTIONAL callable,
`Callable[[str], Sequence[float]]`, given a `version_id`, returning the
RAW per-trade net P&L values attributed to it (same underlying data
source as `champion_live_evidence_provider`, just not pre-aggregated) —
used ONLY by `adaptive.rollback.apply_rollback_if_needed`, which needs
`research.attribution.PerformanceMetrics`-shaped statistics
(`profit_factor`, `win_rate`) that an aggregate `EvidenceSummary` cannot
reconstruct. Same dependency-direction discipline: constructed in
`scripts/run_with_adaptive_policy.py`. When omitted, rollback is never
evaluated (no live evidence to check) — the champion simply stays as-is
until a provider is wired, never a silent, ungrounded reversion.

`live_cycle_results_provider`: an OPTIONAL callable,
`Callable[[], Sequence[RuntimeCycleResult]]`, returning the live
system's own recent PAPER/Testnet-bridge signal-evaluation cycle
results (`ProvenanceSource.PAPER_LIVE`). Step 11's drift summary
compares these against this cycle's DISCOVERY replay's own
`cycle_results` (`ProvenanceSource.REPLAY`) via `adaptive.drift_signal.
summarize_drift` — reused, UNCHANGED. Same dependency-direction
discipline as `champion_live_evidence_provider`: `adaptive/` never reads
live state itself; when omitted, drift comparison runs against an empty
PAPER-side sequence (a harmless, honestly-labelled "nothing to compare"
note, never a fabricated finding)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from adaptive.challenger import ChallengerGenerationConfig, generate_challengers
from adaptive.decision import (
    EMPTY_EVIDENCE,
    Decision,
    DecisionResult,
    EvidenceSummary,
    decide,
    discovery_evidence_from_evaluation,
    evidence_improved,
    shadow_evidence_from_outcomes,
)
from adaptive.drift_signal import DriftSummary, summarize_drift
from adaptive.evaluation import evaluate_challenger, run_shared_replay
from adaptive.policy import PolicySnapshot, default_champion_snapshot
from adaptive.rollback import apply_rollback_if_needed
from adaptive.store import AdaptiveStore
from adaptive.windows import DiscoveryConfirmationWindows, WindowConfig, compute_cycle_windows
from crypto_signal_engine.ops.notifier import Notifier
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.replay import HistoricalCandleSource, ReplayConfig

_CONFIRMATION_CURSOR_NAME = "last_confirmation_end"


@dataclass(frozen=True)
class CycleReport:
    """What one `run_adaptive_cycle()` call actually did — returned so
    both the scheduler's status endpoint and the CLI's stdout report show
    identical, real information, never duplicated/divergent logic."""

    champion_version_id: str
    windows: DiscoveryConfirmationWindows | None
    challengers_generated: tuple[str, ...] = ()
    discovery_survivors: tuple[str, ...] = ()
    confirmation_survivors: tuple[str, ...] = ()
    newly_shadow_eligible: tuple[str, ...] = ()
    decisions: tuple[DecisionResult, ...] = field(default_factory=tuple)
    promoted_to: str | None = None
    drift_summary: DriftSummary | None = None


async def run_adaptive_cycle(
    *, store: AdaptiveStore, candle_source: HistoricalCandleSource, symbols: tuple[str, ...],
    window_config: WindowConfig, anchor: datetime, seed: int, now: datetime,
    challenger_config: ChallengerGenerationConfig | None = None,
    replay_config: ReplayConfig | None = None,
    champion_live_evidence_provider: Callable[[str], EvidenceSummary] | None = None,
    champion_live_pnls_provider: Callable[[str], Sequence[float]] | None = None,
    live_cycle_results_provider: Callable[[], Sequence[RuntimeCycleResult]] | None = None,
    notifier: Notifier | None = None,
    event_log_path: Path | None = None,
) -> CycleReport:
    champion = store.current_champion()
    if champion is None:
        champion = default_champion_snapshot(created_at=now)
        store.promote_champion(
            champion, reason="bootstrap: ilk champion, statik ExitPolicyConfig() varsayılanı", now=now,
        )
    elif champion_live_pnls_provider is not None:
        # Step 14 — ROLLBACK: checked EVERY cycle, before generating new
        # challengers against what might be a degraded champion. A
        # rollback itself is just a normal, logged promotion back to an
        # older PolicySnapshot (see adaptive.rollback) — never a special
        # mutation, always visible in champion_history(). Uses RAW pnls
        # (not `EvidenceSummary`) because the degradation criterion needs
        # `research.attribution.PerformanceMetrics`-shaped statistics
        # (profit_factor, win_rate) that an aggregate `EvidenceSummary`
        # cannot reconstruct.
        rolled_back_to = apply_rollback_if_needed(
            store, champion=champion, live_pnls_provider=champion_live_pnls_provider, now=now,
            notifier=notifier, event_log_path=event_log_path,
        )
        if rolled_back_to is not None:
            champion = rolled_back_to

    challengers = generate_challengers(champion, seed=seed, config=challenger_config, created_at=now)
    for challenger in challengers:
        store.record_policy_version(challenger)
    challengers_by_id = {c.version_id: c for c in challengers}

    last_confirmation_end_raw = store.get_cursor(_CONFIRMATION_CURSOR_NAME)
    last_confirmation_end = datetime.fromisoformat(last_confirmation_end_raw) if last_confirmation_end_raw else None
    windows = compute_cycle_windows(
        last_confirmation_end=last_confirmation_end, anchor=anchor, config=window_config, now=now,
    )

    discovery_survivors: tuple[str, ...] = ()
    confirmation_survivors: tuple[str, ...] = ()
    newly_shadow_eligible: list[str] = []
    decisions: list[DecisionResult] = []
    promoted_to: str | None = None
    drift_summary: DriftSummary | None = None

    if windows is not None:
        discovery_replay = await run_shared_replay(
            candle_source=candle_source, symbols=symbols, window=windows.discovery, replay_config=replay_config,
        )
        champion_discovery = discovery_evidence_from_evaluation(
            await evaluate_challenger(
                champion, replay_result=discovery_replay, candle_source=candle_source, monte_carlo_seed=seed,
            )
        )
        challenger_discovery_evidence: dict[str, EvidenceSummary] = {}
        for challenger_snapshot in challengers:
            evaluation = await evaluate_challenger(
                challenger_snapshot, replay_result=discovery_replay, candle_source=candle_source,
                monte_carlo_seed=seed,
            )
            evidence = discovery_evidence_from_evaluation(evaluation)
            challenger_discovery_evidence[challenger_snapshot.version_id] = evidence
            if evidence_improved(evidence, champion_discovery):
                discovery_survivors = discovery_survivors + (challenger_snapshot.version_id,)

        confirmation_replay = await run_shared_replay(
            candle_source=candle_source, symbols=symbols, window=windows.confirmation, replay_config=replay_config,
        )
        champion_confirmation = discovery_evidence_from_evaluation(
            await evaluate_challenger(
                champion, replay_result=confirmation_replay, candle_source=candle_source, monte_carlo_seed=seed,
            )
        )
        challenger_confirmation_evidence: dict[str, EvidenceSummary] = {}
        for version_id in discovery_survivors:
            evaluation = await evaluate_challenger(
                challengers_by_id[version_id], replay_result=confirmation_replay, candle_source=candle_source,
                monte_carlo_seed=seed,
            )
            evidence = discovery_evidence_from_evaluation(evaluation)
            challenger_confirmation_evidence[version_id] = evidence
            if evidence_improved(evidence, champion_confirmation):
                confirmation_survivors = confirmation_survivors + (version_id,)
                store.mark_shadow_eligible(version_id, now=now)
                newly_shadow_eligible.append(version_id)

        # Confirmation window consumed exactly once, regardless of how
        # many (if any) challengers survived it.
        store.set_cursor(_CONFIRMATION_CURSOR_NAME, windows.confirmation.end.isoformat(), now=now)

        # Step 11 — drift awareness: computed ONCE per cycle, purely
        # auxiliary/read-only, attached to every decision logged THIS
        # cycle below. Never fed into `decide()`.
        live_cycle_results = live_cycle_results_provider() if live_cycle_results_provider is not None else ()
        drift_summary = summarize_drift(discovery_replay.cycle_results, live_cycle_results)

        # Step 7 — promotion check for EVERY shadow-eligible challenger,
        # this cycle's new ones AND every one marked by an earlier cycle.
        champion_shadow_evidence = (
            champion_live_evidence_provider(champion.version_id) if champion_live_evidence_provider is not None
            else EMPTY_EVIDENCE
        )
        for version_id in store.list_shadow_eligible():
            if version_id == champion.version_id:
                continue  # the champion is never "promoted" against its own evidence
            challenger_snapshot = store.load_policy_version(version_id)
            if challenger_snapshot is None:
                continue  # defensive: should never happen (recorded together), never crash the cycle

            if version_id in challenger_discovery_evidence:
                discovery_evidence = challenger_discovery_evidence[version_id]
            else:
                discovery_evidence = discovery_evidence_from_evaluation(
                    await evaluate_challenger(
                        challenger_snapshot, replay_result=discovery_replay, candle_source=candle_source,
                        monte_carlo_seed=seed,
                    )
                )
            if version_id in challenger_confirmation_evidence:
                confirmation_evidence = challenger_confirmation_evidence[version_id]
            else:
                confirmation_evidence = discovery_evidence_from_evaluation(
                    await evaluate_challenger(
                        challenger_snapshot, replay_result=confirmation_replay, candle_source=candle_source,
                        monte_carlo_seed=seed,
                    )
                )
            shadow_evidence = shadow_evidence_from_outcomes(store.shadow_outcomes_for(version_id))

            result = decide(
                version_id,
                discovery_challenger=discovery_evidence, discovery_champion=champion_discovery,
                confirmation_challenger=confirmation_evidence, confirmation_champion=champion_confirmation,
                shadow_challenger=shadow_evidence, shadow_champion=champion_shadow_evidence,
            )
            store.record_decision(result, now=now, drift_note=drift_summary.as_log_note())
            decisions.append(result)
            if result.decision is Decision.PROMOTE:
                store.promote_champion(challenger_snapshot, reason=result.reason, now=now)
                promoted_to = challenger_snapshot.version_id

    final_champion = store.current_champion() or champion
    return CycleReport(
        champion_version_id=final_champion.version_id, windows=windows,
        challengers_generated=tuple(c.version_id for c in challengers),
        discovery_survivors=discovery_survivors, confirmation_survivors=confirmation_survivors,
        newly_shadow_eligible=tuple(newly_shadow_eligible), decisions=tuple(decisions), promoted_to=promoted_to,
        drift_summary=drift_summary,
    )


=== FILE: adaptive/decision.py ===
"""
Adaptive Intelligence v1, steps 9-10 — the promotion gate and decision
function.

THREE SEPARATE evidence classes are tracked — discovery, confirmation,
and shadow — NEVER mixed or averaged into one score. Each is summarized
into an `EvidenceSummary` (sample count, win rate, total P&L, max
drawdown), computed identically regardless of source via
`discovery_evidence_from_evaluation`/`confirmation_evidence_from_
evaluation` (both reuse `adaptive.evaluation.ChallengerEvaluation` --
confirmation is evaluated with the exact same function, just called on
the confirmation window instead, per step 6/7) and `shadow_evidence_
from_outcomes` (from `adaptive.shadow.ShadowOutcome`s).

MINIMUM SAMPLE-SIZE THRESHOLDS (both documented, both required before
PROMOTE is even considered):

- `MIN_DISCOVERY_TRADES = 30`: the discovery window is cheap to
  re-evaluate and freely reusable, so demanding a reasonably large
  sample before a candidate is even worth confirming is low-cost. 30 is
  a common rule-of-thumb minimum for a win-rate/P&L estimate to start
  being more signal than noise -- a heuristic screening threshold, NOT a
  rigorous statistical guarantee (same "screen, not proof" framing as
  `research.monte_carlo.ASSUMPTIONS_NOTE`).
- `MIN_CONFIRMATION_TRADES = 10`: the confirmation window is consumed
  ONLY ONCE and is typically much shorter than the discovery window
  (see `adaptive.windows`), so demanding 30 trades here would make
  almost no challenger ever survive confirmation. 10 is a deliberate
  compromise: still double digits of genuinely untouched, held-out
  trades, never a single lucky/unlucky handful.
- `MIN_SHADOW_TRADES = 5`: shadow evidence accumulates only as fast as
  the LIVE symbols actually open and close real positions (bounded by
  real market activity, never accelerated) -- demanding 30 would mean
  many months could pass before ANY promotion is ever possible. 5 is the
  practical minimum that still rules out a single lucky/unlucky shadow
  trade deciding a promotion.

DECISION FUNCTION: `PROMOTE` requires improvement across discovery AND
confirmation AND shadow -- never one cherry-picked metric.
`INSUFFICIENT_EVIDENCE` is the default, common outcome whenever ANY of
the three sample-size thresholds is not met, checked BEFORE any
improvement comparison -- so a challenger with excellent discovery and
confirmation numbers but too few shadow trades is REJECTED exactly like
one with no evidence at all (see the required test in
`tests/test_adaptive_decision.py::TestInsufficientEvidenceIsDefault`)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from adaptive.evaluation import ChallengerEvaluation
from adaptive.shadow import ShadowOutcome

MIN_DISCOVERY_TRADES = 30
MIN_CONFIRMATION_TRADES = 10
MIN_SHADOW_TRADES = 5

# A challenger's max drawdown may be at most this much WORSE (as a
# multiplier) than the champion's while still counting as "improved" --
# prevents promoting a candidate that merely got lucky on raw total P&L
# while quietly carrying a much deeper drawdown profile. Documented,
# not tuned against any specific dataset.
MAX_DRAWDOWN_TOLERANCE_MULTIPLIER = 1.10


class Decision(str, Enum):
    PROMOTE = "PROMOTE"
    KEEP_CHAMPION = "KEEP_CHAMPION"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class EvidenceSummary:
    """One evidence class's summary for ONE candidate (challenger or
    champion) over ONE window/period. Never averaged across evidence
    classes -- `DecisionResult` below carries all three separately."""

    sample_count: int
    win_rate: float | None
    total_gross_pnl_per_unit: float
    max_drawdown_per_unit: float | None

    def __post_init__(self) -> None:
        if self.sample_count < 0:
            raise ValueError("sample_count negatif olamaz")


EMPTY_EVIDENCE = EvidenceSummary(sample_count=0, win_rate=None, total_gross_pnl_per_unit=0.0, max_drawdown_per_unit=None)


def _evidence_from_sanity_report(evaluation: ChallengerEvaluation) -> EvidenceSummary:
    report = evaluation.sanity_report
    return EvidenceSummary(
        sample_count=report.trade_count, win_rate=report.win_rate,
        total_gross_pnl_per_unit=report.total_gross_pnl_per_unit, max_drawdown_per_unit=report.max_drawdown_per_unit,
    )


# Same computation, two names -- discovery and confirmation evidence are
# computed IDENTICALLY (both are `ChallengerEvaluation`s from
# `adaptive.evaluation.evaluate_challengers_against_window`, just called
# against a different window per step 6/7's discipline); separate names
# only so a caller/decision-log reader can tell which evidence class a
# given `EvidenceSummary` came from without inspecting the window itself.
discovery_evidence_from_evaluation = _evidence_from_sanity_report
confirmation_evidence_from_evaluation = _evidence_from_sanity_report


def evidence_from_pnls(pnls: "list[float] | tuple[float, ...]") -> EvidenceSummary:
    """Pure aggregation shared by every evidence source: a plain sequence
    of per-trade P&L values -> one `EvidenceSummary`. Used by `shadow_
    evidence_from_outcomes` below AND by `scripts/run_with_adaptive_
    policy.py`'s champion-live-evidence provider (built from real
    `bridge_completed_trade.net_realized_pnl` rows, attributed via
    `policy_version_id`, step 3) — one aggregation implementation, never
    two divergent ones."""
    if not pnls:
        return EMPTY_EVIDENCE
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        running += p
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    return EvidenceSummary(
        sample_count=len(pnls), win_rate=wins / len(pnls), total_gross_pnl_per_unit=total,
        max_drawdown_per_unit=max_dd,
    )


def shadow_evidence_from_outcomes(outcomes: tuple[ShadowOutcome, ...]) -> EvidenceSummary:
    return evidence_from_pnls([o.gross_pnl_per_unit for o in outcomes])


@dataclass(frozen=True)
class DecisionResult:
    decision: Decision
    challenger_version_id: str
    reason: str
    discovery_challenger: EvidenceSummary
    discovery_champion: EvidenceSummary
    confirmation_challenger: EvidenceSummary
    confirmation_champion: EvidenceSummary
    shadow_challenger: EvidenceSummary
    shadow_champion: EvidenceSummary


def evidence_improved(challenger: EvidenceSummary, champion: EvidenceSummary) -> bool:
    if challenger.total_gross_pnl_per_unit <= champion.total_gross_pnl_per_unit:
        return False
    if challenger.max_drawdown_per_unit is not None and champion.max_drawdown_per_unit is not None:
        if challenger.max_drawdown_per_unit > champion.max_drawdown_per_unit * MAX_DRAWDOWN_TOLERANCE_MULTIPLIER:
            return False
    return True


def decide(
    challenger_version_id: str, *,
    discovery_challenger: EvidenceSummary, discovery_champion: EvidenceSummary,
    confirmation_challenger: EvidenceSummary, confirmation_champion: EvidenceSummary,
    shadow_challenger: EvidenceSummary, shadow_champion: EvidenceSummary,
) -> DecisionResult:
    """`INSUFFICIENT_EVIDENCE` is checked FIRST, unconditionally, before
    ANY improvement comparison -- so excellent discovery/confirmation
    numbers can never compensate for too few shadow trades (or vice
    versa). Only once all three thresholds are met does the function
    compare improvement, and PROMOTE requires all three evidence classes
    to independently show improvement -- never one cherry-picked metric."""
    common_kwargs = dict(
        challenger_version_id=challenger_version_id,
        discovery_challenger=discovery_challenger, discovery_champion=discovery_champion,
        confirmation_challenger=confirmation_challenger, confirmation_champion=confirmation_champion,
        shadow_challenger=shadow_challenger, shadow_champion=shadow_champion,
    )
    if discovery_challenger.sample_count < MIN_DISCOVERY_TRADES:
        return DecisionResult(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            reason=(
                f"discovery örneklem yetersiz: {discovery_challenger.sample_count} < {MIN_DISCOVERY_TRADES}"
            ),
            **common_kwargs,
        )
    if confirmation_challenger.sample_count < MIN_CONFIRMATION_TRADES:
        return DecisionResult(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            reason=(
                f"confirmation örneklem yetersiz: {confirmation_challenger.sample_count} < {MIN_CONFIRMATION_TRADES}"
            ),
            **common_kwargs,
        )
    if shadow_challenger.sample_count < MIN_SHADOW_TRADES:
        return DecisionResult(
            decision=Decision.INSUFFICIENT_EVIDENCE,
            reason=f"shadow örneklem yetersiz: {shadow_challenger.sample_count} < {MIN_SHADOW_TRADES}",
            **common_kwargs,
        )

    discovery_ok = evidence_improved(discovery_challenger, discovery_champion)
    confirmation_ok = evidence_improved(confirmation_challenger, confirmation_champion)
    shadow_ok = evidence_improved(shadow_challenger, shadow_champion)

    if discovery_ok and confirmation_ok and shadow_ok:
        return DecisionResult(
            decision=Decision.PROMOTE,
            reason="discovery + confirmation + shadow kanıtlarının ÜÇÜ de champion'a göre iyileşme gösterdi",
            **common_kwargs,
        )
    return DecisionResult(
        decision=Decision.KEEP_CHAMPION,
        reason=(
            f"iyileşme yetersiz (discovery={discovery_ok}, confirmation={confirmation_ok}, shadow={shadow_ok}) "
            f"-- PROMOTE üçünün de iyileşme göstermesini gerektirir"
        ),
        **common_kwargs,
    )


=== FILE: adaptive/drift_signal.py ===
"""
Adaptive Intelligence v1, step 11 — drift awareness (auxiliary,
READ-ONLY).

Reuses `research.drift`'s existing `SignalProvenanceRecord`/
`compare_by_context_id` scaffolding UNCHANGED as an auxiliary, read-only
signal attached to the decision log -- no new statistical drift model is
built here, and the existing scaffolding is not omitted either, per the
mission's explicit instruction. This NEVER influences `adaptive.decision.
decide()`'s PROMOTE/KEEP_CHAMPION/INSUFFICIENT_EVIDENCE outcome -- a
`DriftSummary` is logged ALONGSIDE a `DecisionResult` purely for human
audit, never fed back into the decision itself."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.drift import ProvenanceSource, compare_by_context_id, provenance_index


@dataclass(frozen=True)
class DriftSummary:
    """A read-only, auxiliary summary of `research.drift`'s finding
    distribution. `finding_counts` keys are `DriftFinding.value` strings
    (never the enum itself, for trivial JSON/decision-log serialization
    by `adaptive/store.py`)."""

    finding_counts: dict[str, int]
    total_compared: int

    def as_log_note(self) -> str:
        if self.total_compared == 0:
            return "drift: karşılaştırılacak context_id yok (auxiliary, karar etkilenmedi)"
        parts = ", ".join(f"{k}={v}" for k, v in sorted(self.finding_counts.items()))
        return f"drift (auxiliary, salt-gözlemsel, karar ETKİLENMEDİ): {parts} (n={self.total_compared})"


def summarize_drift(
    replay_cycle_results: Sequence[RuntimeCycleResult], paper_cycle_results: Sequence[RuntimeCycleResult],
) -> DriftSummary:
    """The ONE entry point `adaptive/cycle.py` calls to attach a drift
    note to a cycle's decision-log entry. Pure pass-through to
    `research.drift`'s existing functions -- no new comparison logic,
    no new statistical model."""
    replay_index = provenance_index(replay_cycle_results, source=ProvenanceSource.REPLAY)
    paper_index = provenance_index(paper_cycle_results, source=ProvenanceSource.PAPER_LIVE)
    findings = compare_by_context_id(replay_index, paper_index)
    counts: dict[str, int] = {}
    for finding in findings.values():
        counts[finding.value] = counts.get(finding.value, 0) + 1
    return DriftSummary(finding_counts=counts, total_compared=len(findings))


=== FILE: adaptive/evaluation.py ===
"""
Adaptive Intelligence v1, step 7 — discovery (and, reused unmodified for
step 9, confirmation) evaluation.

Runs each challenger against a historical window by reusing, UNMODIFIED:
- `research.replay.HistoricalReplayDriver` produces ONE `ReplayResult`
  per window, shared across ALL challengers in a round -- entries are
  policy-independent (signal-driven, via the existing accepted
  Quant/Consensus/PAPER pipeline), only EXITS vary per
  `ExitPolicyConfig`, so there is no reason to re-run the (expensive)
  replay once per challenger.
- `crypto_signal_engine.execution.lifecycle_replay_sanity.
  simulate_lifecycle_exits` -- the exact `ExitPolicyConfig`-aware,
  real-M1-driven backtest engine already accepted for Phase 20 sanity
  checking. This IS the bridge between `research/`'s signal-only replay
  and `lifecycle.py`'s ATR-based exits; this module builds no second
  implementation of either.
- `research.monte_carlo.run_monte_carlo_robustness_screen` -- a
  drawdown-sensitivity screen over each challenger's completed-trade
  P&Ls, using a caller-supplied seed via a local `random.Random`
  (`run_monte_carlo_robustness_screen` itself already enforces this).

FEE/SLIPPAGE ASSUMPTIONS (must be explicit and non-zero --
`research.replay.ReplayConfig`'s own default of 0.0/0.0 is unrealistic
for evaluating a candidate LIVE trading policy):
- `DISCOVERY_FEE_BPS = 10.0`: Binance's published standard spot taker
  fee is 0.10% (10 bps) for a non-VIP, non-BNB-fee-discount account --
  the conservative assumption (no assumed VIP tier, no assumed BNB
  discount).
- `DISCOVERY_SLIPPAGE_BPS = 5.0`: no official Binance-published
  slippage figure exists for market orders on these liquid USDT pairs.
  5 bps (roughly half the taker fee) is a conservative, EXPLICITLY
  DOCUMENTED ESTIMATE, not a measured value -- flagged the same way
  `research.monte_carlo.ASSUMPTIONS_NOTE` flags its own simplifications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from crypto_signal_engine.execution.lifecycle_replay_sanity import (
    LifecycleSanityReport,
    simulate_lifecycle_exits,
)
from research.monte_carlo import MonteCarloReport, run_monte_carlo_robustness_screen
from research.oos_stability import OOSWindow
from research.replay import HistoricalCandleSource, HistoricalReplayDriver, ReplayConfig, ReplayResult

from adaptive.policy import PolicySnapshot

DISCOVERY_FEE_BPS = 10.0
DISCOVERY_SLIPPAGE_BPS = 5.0
DEFAULT_EXIT_WINDOW = timedelta(hours=72)
DEFAULT_MONTE_CARLO_ITERATIONS = 200


def default_discovery_replay_config() -> ReplayConfig:
    """The one place `DISCOVERY_FEE_BPS`/`DISCOVERY_SLIPPAGE_BPS` become a
    real `ReplayConfig` -- never `ReplayConfig()`'s unrealistic zero
    default."""
    return ReplayConfig(fee_bps=DISCOVERY_FEE_BPS, slippage_bps=DISCOVERY_SLIPPAGE_BPS)


@dataclass(frozen=True)
class ChallengerEvaluation:
    policy_version_id: str
    sanity_report: LifecycleSanityReport
    monte_carlo_report: MonteCarloReport | None  # None when zero completed trades in this window


async def run_shared_replay(
    *, candle_source: HistoricalCandleSource, symbols: tuple[str, ...], window: OOSWindow,
    replay_config: ReplayConfig | None = None,
) -> ReplayResult:
    """Runs the SHARED, policy-independent replay ONCE per window. Both
    discovery evaluation (this module) and confirmation evaluation (step
    9, which calls this SAME function against the confirmation window)
    use it identically -- there is no separate "confirmation replay"
    implementation."""
    config = replay_config or default_discovery_replay_config()
    driver = HistoricalReplayDriver(candle_source=candle_source, config=config)
    return await driver.run(symbols=symbols, start=window.start, end=window.end)


async def evaluate_challenger(
    challenger: PolicySnapshot, *, replay_result: ReplayResult, candle_source: HistoricalCandleSource,
    monte_carlo_seed: int, exit_window: timedelta = DEFAULT_EXIT_WINDOW,
    monte_carlo_iterations: int = DEFAULT_MONTE_CARLO_ITERATIONS,
) -> ChallengerEvaluation:
    """Evaluates ONE challenger against an ALREADY-COMPUTED shared
    `replay_result` (see `run_shared_replay`) -- zero new replay work per
    challenger; only the (pure, cheap) lifecycle-exit simulation and
    Monte Carlo screen vary per candidate."""
    sanity_report = await simulate_lifecycle_exits(
        replay_result, candle_source=candle_source, exit_policy=challenger.to_exit_policy_config(),
        exit_window=exit_window,
    )
    pnls = [s.gross_pnl_per_unit for s in sanity_report.simulations if s.gross_pnl_per_unit is not None]
    monte_carlo_report = (
        run_monte_carlo_robustness_screen(pnls, seed=monte_carlo_seed, iterations=monte_carlo_iterations)
        if pnls else None
    )
    return ChallengerEvaluation(
        policy_version_id=challenger.version_id, sanity_report=sanity_report, monte_carlo_report=monte_carlo_report,
    )


async def evaluate_challengers_against_window(
    challengers: tuple[PolicySnapshot, ...], *, candle_source: HistoricalCandleSource, symbols: tuple[str, ...],
    window: OOSWindow, monte_carlo_seed: int, replay_config: ReplayConfig | None = None,
    exit_window: timedelta = DEFAULT_EXIT_WINDOW, monte_carlo_iterations: int = DEFAULT_MONTE_CARLO_ITERATIONS,
) -> tuple[ChallengerEvaluation, ...]:
    """The ONE entry point `adaptive/cycle.py` calls for a discovery OR a
    confirmation round (same function, different window, different
    call-site discipline: confirmation is called at most once per
    candidate — see step 6/9): runs the shared replay ONCE, then
    evaluates every challenger against it."""
    replay_result = await run_shared_replay(
        candle_source=candle_source, symbols=symbols, window=window, replay_config=replay_config,
    )
    results = []
    for challenger in challengers:
        results.append(
            await evaluate_challenger(
                challenger, replay_result=replay_result, candle_source=candle_source, exit_window=exit_window,
                monte_carlo_seed=monte_carlo_seed, monte_carlo_iterations=monte_carlo_iterations,
            )
        )
    return tuple(results)


=== FILE: adaptive/policy.py ===
"""
Adaptive Intelligence v1, step 1 — `PolicySnapshot`: an immutable,
versioned wrapper around ONLY `crypto_signal_engine.execution.lifecycle.
ExitPolicyConfig`'s five fields.

`RiskPolicyConfig` (portfolio-wide, human-set safety ceiling) is NEVER
part of this schema, in any version, ever — see `adaptive/__init__.py`'s
hard rules and `ConsensusEngine`/RiskOverlay, which also stay fixed by
design and are out of scope for this milestone.

A `PolicySnapshot` is a pure data holder: it carries no order-submission
capability and this module makes zero network calls."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import datetime, timezone

from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig

_VERSION_COUNTER = itertools.count(1)


def _next_version_id() -> str:
    """Monotonic within a single process. Callers that need cross-restart
    monotonicity (the real `adaptive/store.py` champion history) derive
    the next id from the persisted store's own max version instead of
    calling this — this is only the convenience default for ad hoc/test
    construction, mirroring the style of `research`'s pure helpers."""
    return f"policy-{next(_VERSION_COUNTER)}"


@dataclass(frozen=True)
class PolicySnapshot:
    """One immutable, fully-resolved candidate/champion exit policy.

    `version_id` is the SAME opaque string
    `BridgePositionRecord.policy_version_id` stores and
    `crypto_signal_engine/` passes through without interpreting — this is
    the one place its meaning is actually defined. `provenance` is a
    free-text explanation of where this snapshot came from (e.g.
    "champion default", "challenger of policy-3, seed=42, perturbation
    stop_atr_multiple +12%", "promoted from shadow evidence at
    2026-09-05T00:00:00Z") — never parsed programmatically, purely for
    the decision log / human audit trail."""

    version_id: str
    stop_atr_multiple: float
    take_profit_atr_multiple: float
    trailing_activation_atr_multiple: float
    trailing_distance_atr_multiple: float
    max_hold_hours: float
    created_at: datetime
    provenance: str

    def __post_init__(self) -> None:
        if not self.version_id:
            raise ValueError("PolicySnapshot.version_id boş olamaz")
        if self.created_at.tzinfo is None:
            raise ValueError("PolicySnapshot.created_at tz-aware olmalı")
        # Reuse ExitPolicyConfig's own validation (positive-value rule) —
        # never a second, divergent bounds-check implementation.
        self.to_exit_policy_config()

    def to_exit_policy_config(self) -> ExitPolicyConfig:
        """The ONE way this snapshot becomes something
        `crypto_signal_engine/` can actually use — never a reference,
        always a fresh, fully-resolved `ExitPolicyConfig`."""
        return ExitPolicyConfig(
            stop_atr_multiple=self.stop_atr_multiple,
            take_profit_atr_multiple=self.take_profit_atr_multiple,
            trailing_activation_atr_multiple=self.trailing_activation_atr_multiple,
            trailing_distance_atr_multiple=self.trailing_distance_atr_multiple,
            max_hold_hours=self.max_hold_hours,
        )


def snapshot_from_exit_policy_config(
    config: ExitPolicyConfig, *, version_id: str | None = None, provenance: str, created_at: datetime | None = None,
) -> PolicySnapshot:
    """Wrap an existing `ExitPolicyConfig` (e.g. `crypto_signal_engine`'s
    own static default, read purely as a value — never imported as a
    reference into any order-submission path) as a `PolicySnapshot`."""
    return PolicySnapshot(
        version_id=version_id or _next_version_id(),
        stop_atr_multiple=config.stop_atr_multiple,
        take_profit_atr_multiple=config.take_profit_atr_multiple,
        trailing_activation_atr_multiple=config.trailing_activation_atr_multiple,
        trailing_distance_atr_multiple=config.trailing_distance_atr_multiple,
        max_hold_hours=config.max_hold_hours,
        created_at=created_at or datetime.now(timezone.utc),
        provenance=provenance,
    )


def default_champion_snapshot(*, created_at: datetime | None = None) -> PolicySnapshot:
    """The bootstrap champion for a brand-new `adaptive/` deployment —
    exactly today's production default, so a system that has never
    promoted anything behaves bit-for-bit identically to the existing
    static-config path (see step 14's equivalence requirement)."""
    return snapshot_from_exit_policy_config(
        ExitPolicyConfig(), version_id="policy-champion-default",
        provenance="bootstrap champion: crypto_signal_engine.execution.lifecycle.ExitPolicyConfig() defaults",
        created_at=created_at,
    )


=== FILE: adaptive/rollback.py ===
"""
Adaptive Intelligence v1, step 14 — ROLLBACK.

Given a live champion whose REAL Testnet/PAPER evidence (attributed via
`policy_version_id`, step 3) degrades past an explicit, documented,
NON-TRIVIAL magnitude relative to a documented BASELINE, revert to the
immediately-prior champion and log the rollback in the decision log (via
`AdaptiveStore.promote_champion`, itself append-only — a rollback is just
a normal, auditable promotion back to an older `PolicySnapshot`, never a
special-cased mutation).

CRITERION REDESIGNED (independent-verification fix — the original rule
was "sample_count >= 10 AND net P&L < 0", which fires on ANY negative
P&L however small, e.g. a single -0.01 USDT trade among nine break-even
ones. That is exactly the "a few losses -> change the policy" pattern
this project explicitly rejects everywhere else — INSUFFICIENT_EVIDENCE-
by-default, no reaction to small samples, no curve-fitting to noise).

The new criterion is evidence-based and magnitude-aware, reusing
`research.attribution.PerformanceMetrics`'s SHAPE (`win_rate`,
`profit_factor`) — `research.attribution._compute_metrics` itself is
private and shaped around Signal `TradeRecord`s, not a plain P&L float
sequence, so `performance_metrics_from_pnls()` below mirrors its EXACT
formulas (gross_profit/gross_loss split, win_rate, profit_factor) against
a plain float list instead of duplicating a second, divergent statistic.

BASELINE (documented, in preference order):
1. The champion's OWN pre-promotion SHADOW-stage evidence
   (`AdaptiveStore.shadow_outcomes_for(champion.version_id)`) — the most
   directly comparable number available: it is literally what this exact
   policy was expected to do, measured on the SAME champion, just before
   promotion. Requires no new provider — already persisted in `store`.
2. When no shadow baseline exists (e.g. the bootstrap default champion,
   which is never shadowed before becoming champion) or it has too few
   trades, fall back to the immediately-PRIOR champion's own REAL live
   performance (via `champion_live_pnls_provider`, over whatever sample
   it has accumulated) — a genuinely comparable, real-world number.
3. If neither baseline has enough evidence, rollback is NOT evaluated
   this cycle (same "insufficient evidence -> no action" discipline as
   the promotion gate) — never falls back to a bare sign check.

DEGRADATION CRITERION (both documented, both required):
- `ROLLBACK_MIN_LIVE_SAMPLE_COUNT = 10`: at least 10 real completed
  trades under the current champion before its live evidence is trusted
  at all — same rationale as the original rule's floor.
- `ROLLBACK_MIN_BASELINE_SAMPLE_COUNT = 5`: the baseline itself must have
  at least 5 trades, or it is itself too noisy to compare against.
- The champion's live P&L must be net NEGATIVE overall (necessary, not
  sufficient — this alone no longer triggers anything).
- AND at least one of two INDEPENDENT, magnitude-aware signals must show
  a NON-TRIVIAL degradation versus the baseline:
    - `ROLLBACK_PROFIT_FACTOR_DEGRADATION_RATIO = 0.5`: live profit_factor
      has collapsed to under HALF the baseline's profit_factor (a >=50%
      relative collapse in "gross profit per unit of gross loss" is a
      real, structural change, not sampling noise).
    - `ROLLBACK_WIN_RATE_DROP = 0.15`: live win_rate has dropped by more
      than 15 percentage points versus the baseline's win_rate.
  Requiring TWO independent lenses (magnitude via profit_factor, AND/OR
  frequency via win_rate) to at least one show a real drop — never a bare
  sign check — mirrors the promotion gate's own "all evidence classes
  must independently show improvement, never one cherry-picked metric"
  philosophy, inverted for degradation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from adaptive.policy import PolicySnapshot
from adaptive.store import AdaptiveStore
from crypto_signal_engine.ops.event_log import record_event
from crypto_signal_engine.ops.notifier import Notifier, safe_notify
from research.attribution import PerformanceMetrics

ROLLBACK_MIN_LIVE_SAMPLE_COUNT = 10
ROLLBACK_MIN_BASELINE_SAMPLE_COUNT = 5
ROLLBACK_PROFIT_FACTOR_DEGRADATION_RATIO = 0.5
ROLLBACK_WIN_RATE_DROP = 0.15


def performance_metrics_from_pnls(pnls: Sequence[float]) -> PerformanceMetrics:
    """Mirrors `research.attribution._compute_metrics`'s EXACT formulas
    (that function is private and shaped around Signal `TradeRecord`s,
    not a plain P&L float sequence — this is not a second, divergent
    statistic, it is the same computation applied to a different input
    shape). `total_fees=0.0` always: `bridge_completed_trade.net_realized_
    pnl` (this function's real caller) is ALREADY fee-net, so there is no
    separate fee figure to report here. `max_drawdown` intentionally
    omitted (`None`) — that requires chronological ordering this plain
    float sequence does not carry; step 9's promotion gate already
    computes drawdown correctly, from ordered data, via `adaptive.
    decision.evidence_from_pnls`."""
    trade_count = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    breakeven = trade_count - len(wins) - len(losses)
    gross_profit = sum(wins)
    gross_loss = sum(-p for p in losses)
    net_pnl = sum(pnls)

    win_rate = (len(wins) / trade_count) if trade_count > 0 else None
    average_win = (gross_profit / len(wins)) if wins else None
    average_loss = (gross_loss / len(losses)) if losses else None
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None

    return PerformanceMetrics(
        trade_count=trade_count, wins=len(wins), losses=len(losses), breakeven=breakeven,
        win_rate=win_rate, gross_profit=gross_profit, gross_loss=gross_loss, net_pnl=net_pnl,
        total_fees=0.0, average_win=average_win, average_loss=average_loss,
        profit_factor=profit_factor, max_drawdown=None,
    )


@dataclass(frozen=True)
class RollbackCheck:
    should_rollback: bool
    reason: str


def check_rollback(*, live: PerformanceMetrics, baseline: PerformanceMetrics, baseline_label: str) -> RollbackCheck:
    if live.trade_count < ROLLBACK_MIN_LIVE_SAMPLE_COUNT:
        return RollbackCheck(
            should_rollback=False,
            reason=f"canlı örneklem yetersiz ({live.trade_count} < {ROLLBACK_MIN_LIVE_SAMPLE_COUNT}) -- rollback değerlendirilmedi",
        )
    if baseline.trade_count < ROLLBACK_MIN_BASELINE_SAMPLE_COUNT:
        return RollbackCheck(
            should_rollback=False,
            reason=(
                f"baseline ({baseline_label}) örneklemi yetersiz "
                f"({baseline.trade_count} < {ROLLBACK_MIN_BASELINE_SAMPLE_COUNT}) -- rollback değerlendirilmedi"
            ),
        )
    if live.net_pnl >= 0:
        return RollbackCheck(should_rollback=False, reason="canlı net PnL negatif değil -- rollback gerekmiyor")

    profit_factor_degraded = (
        baseline.profit_factor is not None and baseline.profit_factor > 0
        and (live.profit_factor or 0.0) < baseline.profit_factor * ROLLBACK_PROFIT_FACTOR_DEGRADATION_RATIO
    )
    win_rate_degraded = (
        baseline.win_rate is not None and live.win_rate is not None
        and (baseline.win_rate - live.win_rate) > ROLLBACK_WIN_RATE_DROP
    )

    if not (profit_factor_degraded or win_rate_degraded):
        return RollbackCheck(
            should_rollback=False,
            reason=(
                f"net PnL negatif ama düşüş {baseline_label} baseline'ına göre anlamlı büyüklükte DEĞİL "
                f"(profit_factor_degraded={profit_factor_degraded}, win_rate_degraded={win_rate_degraded}) "
                f"-- tek bir kayıp işlem rollback'i TETİKLEMEZ"
            ),
        )

    return RollbackCheck(
        should_rollback=True,
        reason=(
            f"champion GERÇEK üretimde {baseline_label} baseline'ına göre anlamlı ölçüde kötüleşti "
            f"(live: net_pnl={live.net_pnl:.4f}, profit_factor={live.profit_factor}, win_rate={live.win_rate}; "
            f"baseline: profit_factor={baseline.profit_factor}, win_rate={baseline.win_rate}; "
            f"profit_factor_degraded={profit_factor_degraded}, win_rate_degraded={win_rate_degraded}) "
            f"-- önceki champion'a dönülüyor"
        ),
    )


def _resolve_baseline(
    store: AdaptiveStore, *, champion: PolicySnapshot, live_pnls_provider: Callable[[str], Sequence[float]],
) -> tuple[PerformanceMetrics, str] | None:
    """Baseline resolution order — see module docstring. Returns
    `(metrics, label)` for logging, or `None` if no baseline has enough
    evidence to compare against at all."""
    shadow_outcomes = store.shadow_outcomes_for(champion.version_id)
    shadow_pnls = [o.gross_pnl_per_unit for o in shadow_outcomes]
    shadow_metrics = performance_metrics_from_pnls(shadow_pnls)
    if shadow_metrics.trade_count >= ROLLBACK_MIN_BASELINE_SAMPLE_COUNT:
        return shadow_metrics, "kendi shadow-aşaması kanıtı"

    history = store.champion_history()
    prior_candidates = [h for h in history if h["version_id"] != champion.version_id]
    if prior_candidates:
        prior_version_id = prior_candidates[-1]["version_id"]
        prior_pnls = live_pnls_provider(prior_version_id)
        prior_metrics = performance_metrics_from_pnls(prior_pnls)
        if prior_metrics.trade_count >= ROLLBACK_MIN_BASELINE_SAMPLE_COUNT:
            return prior_metrics, f"önceki champion'ın ({prior_version_id}) gerçek performansı"

    return None


def apply_rollback_if_needed(
    store: AdaptiveStore, *, champion: PolicySnapshot,
    live_pnls_provider: Callable[[str], Sequence[float]], now: datetime,
    notifier: Notifier | None = None,
    event_log_path: Path | None = None,
) -> PolicySnapshot | None:
    """Checks the CURRENT champion's real live P&L against a documented
    baseline (see module docstring) and, if it has degraded by a
    non-trivial, documented magnitude, reverts to the immediately-prior
    DIFFERENT champion in history — logged via `promote_champion`
    (append-only, so this rollback itself becomes a normal, auditable
    entry in `champion_history()`, never a silent mutation). Returns the
    `PolicySnapshot` rolled back to, or `None` if no rollback was applied
    (the check did not fire, no baseline had enough evidence, or there is
    no distinct prior champion to revert to)."""
    live_pnls = live_pnls_provider(champion.version_id)
    live_metrics = performance_metrics_from_pnls(live_pnls)

    baseline_result = _resolve_baseline(store, champion=champion, live_pnls_provider=live_pnls_provider)
    if baseline_result is None:
        return None
    baseline_metrics, baseline_label = baseline_result

    check = check_rollback(live=live_metrics, baseline=baseline_metrics, baseline_label=baseline_label)
    if not check.should_rollback:
        return None

    history = store.champion_history()
    prior_candidates = [h for h in history if h["version_id"] != champion.version_id]
    if not prior_candidates:
        return None  # nothing to roll back TO -- documented limitation, not a silent no-op bug
    prior_version_id = prior_candidates[-1]["version_id"]
    prior_snapshot = store.load_policy_version(prior_version_id)
    if prior_snapshot is None:
        return None  # defensive: should never happen, never crash the cycle over it

    store.promote_champion(prior_snapshot, reason=f"ROLLBACK: {check.reason}", now=now)
    # 24/7 Ops v1, Step 4 — outbound-only alerting, wired at THIS
    # already-logged decision point (the rollback itself is already
    # durable/auditable via `promote_champion` above; this is purely an
    # additive notification on top). `notifier=None` (every existing
    # caller before this milestone) is bit-for-bit unaffected. Called
    # via `safe_notify()` so a raising/hanging notifier can never affect
    # whether the rollback itself was applied — it already was, above.
    safe_notify(
        notifier,
        f"[adaptive] ROLLBACK: {champion.version_id} -> {prior_snapshot.version_id} — {check.reason}",
    )
    # UI Polish v1, Step A9 — event log, SAME transition point as the
    # notifier call above; `record_event()` never raises (see
    # `crypto_signal_engine/ops/event_log.py`), a `None` path is a no-op.
    if event_log_path is not None:
        record_event(
            event_log_path, event_type="adaptive_rollback",
            detail=f"{champion.version_id} -> {prior_snapshot.version_id}: {check.reason}", occurred_at=now,
        )
    return prior_snapshot


=== FILE: adaptive/scheduler.py ===
"""
Adaptive Intelligence v1, step 13 — the automatic periodic trigger,
built on the EXACT SAME pattern as `crypto_signal_engine.runtime.
reselection_scheduler.ReselectionScheduler` (Phase 16): one background
`asyncio.Task`, `run_once()` exposed separately so tests (and, if ever
needed, operator tooling) can drive it deterministically without a real
`asyncio.sleep`, and a failed cycle NEVER kills the loop — one bad cycle
just repeats next interval, mirroring `ReselectionScheduler.run_once()`'s
own "a rescan failure must never kill the scheduler loop" discipline.

DEFAULT INTERVAL — `DEFAULT_CYCLE_INTERVAL_SECONDS = 86400.0` (24h),
configurable, mirroring `AutoSymbolSelectionConfig.rescan_interval_
seconds`'s own config style, with a documented HARD CEILING at the same
value (an evaluation cycle may run AT MOST once per 24h — see the
mission's own "default no more than once per 24h"). Rationale: a cycle
does real (bounded, historical, already-rate-limited) replay work, unlike
a symbol rescan's lightweight REST calls — running it more than once a
day would mostly re-evaluate the same discovery data for no new signal,
and the confirmation window itself only ever advances on `adaptive.
windows`'s own (typically much longer) schedule regardless of how often
this scheduler fires, so a shorter interval buys nothing.

This scheduler NEVER touches the live async runtime's own event loop
beyond `asyncio.create_task` (the same primitive `ReselectionScheduler`
and every other accepted background task in this codebase use) — it
never blocks candle-ingestion/signal-evaluation, since it is its own
independent task."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from adaptive.challenger import ChallengerGenerationConfig
from adaptive.cycle import CycleReport, run_adaptive_cycle
from adaptive.decision import EvidenceSummary
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.ops.notifier import Notifier
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.replay import HistoricalCandleSource, ReplayConfig

_LOGGER = logging.getLogger("adaptive.scheduler")

DEFAULT_CYCLE_INTERVAL_SECONDS = 86400.0  # 24h — documented hard ceiling, see module docstring


@dataclass(frozen=True)
class AdaptiveSchedulerConfig:
    symbols: tuple[str, ...]
    anchor: datetime
    window_config: WindowConfig
    cycle_interval_seconds: float = DEFAULT_CYCLE_INTERVAL_SECONDS
    seed: int = 0
    challenger_config: ChallengerGenerationConfig | None = None
    replay_config: ReplayConfig | None = None

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("symbols boş olamaz")
        if self.cycle_interval_seconds <= 0:
            raise ValueError("cycle_interval_seconds pozitif olmalı")
        if self.cycle_interval_seconds > DEFAULT_CYCLE_INTERVAL_SECONDS:
            raise ValueError(
                f"cycle_interval_seconds {DEFAULT_CYCLE_INTERVAL_SECONDS}s (24 saat) üst sınırını aşamaz "
                f"-- bkz. adaptive/scheduler.py modül docstring'i"
            )


@dataclass(frozen=True)
class AdaptiveSchedulerStatus:
    armed: bool
    cycle_interval_seconds: float
    last_run_at: datetime | None
    next_run_at: datetime | None
    last_champion_version_id: str | None
    last_promoted_to: str | None
    last_error: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "armed": self.armed,
            "cycle_interval_seconds": self.cycle_interval_seconds,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at is not None else None,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at is not None else None,
            "last_champion_version_id": self.last_champion_version_id,
            "last_promoted_to": self.last_promoted_to,
            "last_error": self.last_error,
        }


class AdaptiveScheduler:
    """Owns ONE background `asyncio.Task` that periodically calls the
    SAME `adaptive.cycle.run_adaptive_cycle()` function `scripts/
    adaptive_evaluation_cycle.py` (the manual CLI) calls directly — no
    duplicated evaluation logic between the two."""

    def __init__(
        self, *, store: AdaptiveStore, candle_source: HistoricalCandleSource, config: AdaptiveSchedulerConfig,
        champion_live_evidence_provider: Callable[[str], EvidenceSummary] | None = None,
        champion_live_pnls_provider: Callable[[str], Sequence[float]] | None = None,
        live_cycle_results_provider: Callable[[], Sequence[RuntimeCycleResult]] | None = None,
        clock: Clock | None = None,
        notifier: Notifier | None = None,
        event_log_path: Path | None = None,
    ) -> None:
        self._store = store
        self._candle_source = candle_source
        self._config = config
        self._champion_live_evidence_provider = champion_live_evidence_provider
        self._champion_live_pnls_provider = champion_live_pnls_provider
        self._live_cycle_results_provider = live_cycle_results_provider
        self._clock = clock or SystemClock()
        # 24/7 Ops v1, Step 4 — passed straight through to
        # `run_adaptive_cycle()` -> `apply_rollback_if_needed()`. `None`
        # by default (every existing caller), bit-for-bit unaffected.
        self._notifier = notifier
        # UI Polish v1, Step A9 — same pass-through discipline as notifier.
        self._event_log_path = event_log_path
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._last_run_at: datetime | None = None
        self._next_run_at: datetime | None = None
        self._last_report: CycleReport | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self._task is not None:
            return
        self._next_run_at = self._clock.now() + timedelta(seconds=self._config.cycle_interval_seconds)
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
            await asyncio.sleep(self._config.cycle_interval_seconds)
            if self._stopped:
                return
            await self.run_once()

    async def run_once(self) -> CycleReport | None:
        """One evaluation cycle — public so tests (and operator tooling)
        can trigger it deterministically without a real `asyncio.sleep`.
        Returns `None` (never raises) if the cycle itself failed — the
        loop survives, exactly like `ReselectionScheduler.run_once()`."""
        now = self._clock.now()
        try:
            report = await run_adaptive_cycle(
                store=self._store, candle_source=self._candle_source, symbols=self._config.symbols,
                window_config=self._config.window_config, anchor=self._config.anchor, seed=self._config.seed,
                now=now, challenger_config=self._config.challenger_config, replay_config=self._config.replay_config,
                champion_live_evidence_provider=self._champion_live_evidence_provider,
                champion_live_pnls_provider=self._champion_live_pnls_provider,
                live_cycle_results_provider=self._live_cycle_results_provider,
                notifier=self._notifier, event_log_path=self._event_log_path,
            )
        except Exception as exc:  # noqa: BLE001 - a failed cycle must NEVER kill the scheduler loop or the live runtime
            self._last_error = str(exc)
            _LOGGER.error("adaptive: evaluation cycle FAILED (will retry next interval)", exc_info=True)
            self._advance_schedule(now)
            return None

        self._last_report = report
        self._last_error = None
        self._advance_schedule(now)
        return report

    def _advance_schedule(self, now: datetime) -> None:
        self._last_run_at = now
        self._next_run_at = now + timedelta(seconds=self._config.cycle_interval_seconds)

    def status(self) -> AdaptiveSchedulerStatus:
        return AdaptiveSchedulerStatus(
            armed=self._task is not None and not self._stopped,
            cycle_interval_seconds=self._config.cycle_interval_seconds,
            last_run_at=self._last_run_at, next_run_at=self._next_run_at,
            last_champion_version_id=self._last_report.champion_version_id if self._last_report is not None else None,
            last_promoted_to=self._last_report.promoted_to if self._last_report is not None else None,
            last_error=self._last_error,
        )


=== FILE: adaptive/symbol_score.py ===
"""
Adaptive Symbol Intelligence v1 — a 4th, OPTIONAL scoring term for
`crypto_signal_engine.selection.selector.AutomaticSymbolSelector`,
derived directly from each symbol's own REAL completed-trade history.

This is a deliberately lower-risk mechanism than the exit-policy
`adaptive/` system (`windows.py`/`challenger.py`/`evaluation.py`/
`shadow.py`/`store.py`/`scheduler.py`/`rollback.py`/`drift_signal.py`/
`cycle.py` — NONE of which are reused or modified here): it only nudges
the RANKING among symbols that are ALREADY eligible under the existing
absolute floors (`min_atr_pct`/`min_current_move_pct`/
`min_quote_volume_24h`). It can never submit an order, bypass a safety
gate, or change what "eligible" means — see `DECISIONS.md`'s Karar entry
for this milestone for the full "why no promotion/shadow/rollback gate"
argument.

NO BACKTESTING, NO REPLAY: this module is pure, deterministic, in-process
arithmetic over an already-fetched plain float P&L sequence. It never
reads `crypto_signal_engine.execution` (not even `lifecycle_store` —
see `tests/test_repository_safety_scan_adaptive.py`'s allowlist, which
this module deliberately does NOT need to be added to) and never touches
`crypto_signal_engine.selection` either (the dependency direction is
one-way: `crypto_signal_engine/selection/` may call INTO this module only
via the injected `learned_factor_provider` callable it owns the type of
— see `selector.py` — never the reverse).

MIN_SYMBOL_TRADES = 10 (documented reasoning, NOT copied from
`adaptive.decision.MIN_CONFIRMATION_TRADES` without thought): a symbol's
learned factor is consulted on EVERY periodic re-ranking of the ENTIRE
active universe (potentially every rescan, indefinitely, for as long as
the symbol keeps being considered) — this is a smaller per-decision
blast radius than a full policy PROMOTE (which is scoped to one exit
policy champion), but a LARGER-frequency exposure (every symbol, every
rescan, forever) than a one-time promotion decision. `MIN_SHADOW_TRADES
= 5` (the loosest bar in the exit-policy system) is too thin here — a
lucky/unlucky handful of trades would keep nudging the SAME symbol's
rank every single rescan cycle, not just influence one decision.
`MIN_DISCOVERY_TRADES = 30` is unnecessarily strict — unlike a discovery
replay window (cheap, freely re-runnable, no real-world constraint), a
symbol's completed-trade count is bounded by how often the live system
ACTUALLY opens and closes real positions in that symbol, a genuine
real-world constraint. `10` mirrors `MIN_CONFIRMATION_TRADES`'s own
"double digits of genuine trades, never a single lucky/unlucky handful"
bar — chosen for a DIFFERENT reason (continuous re-ranking exposure,
not one-time confirmation), landing on the same number because both
represent "the smallest sample this project is willing to trust for a
recurring, not one-off, decision."

FORMULA (documented, no unexplained magic numbers): given `pnls` with
`len(pnls) >= MIN_SYMBOL_TRADES`, aggregate via `adaptive.decision.
evidence_from_pnls` (reused, not reimplemented) into an `EvidenceSummary`,
then blend two independent, already-bounded signals with EQUAL (0.5/0.5)
weight — deliberately symmetric, since neither alone tells the whole
story (a high win-rate of tiny wins wiped out by one huge loss, or a low
win-rate rescued by a few large wins, are both partial pictures):

    learned_factor = 0.5 * win_rate + 0.5 * profitability_component

- `win_rate` — `EvidenceSummary.win_rate`, already in `[0, 1]` by
  construction (fraction of trades with positive P&L) — reused directly,
  no rescaling needed.
- `profitability_component` — a simple, deterministic SIGN read of the
  symbol's own net realized P&L history: `1.0` if
  `total_gross_pnl_per_unit > 0`, `0.0` if `< 0`, `0.5` if exactly
  breakeven. `EvidenceSummary` does not split gross profit from gross
  loss (that split lives in `adaptive.rollback.PerformanceMetrics`,
  which this module deliberately does NOT import — the mission scopes
  this to `evidence_from_pnls`'s `EvidenceSummary` only), so a magnitude-
  weighted ratio (e.g. a profit-factor-style term) is not reconstructable
  here without inventing an extra, unexplained normalization constant.
  A plain sign read needs none: it is already exactly in `{0.0, 0.5,
  1.0} ⊂ [0, 1]`.

Both components are already bounded in `[0, 1]`, so the blend is always
in `[0, 1]` by construction — no additional clamping is needed (and none
is applied, so a bug in either component would surface as an
out-of-range value rather than being silently masked)."""

from __future__ import annotations

from collections.abc import Sequence

from adaptive.decision import evidence_from_pnls

MIN_SYMBOL_TRADES = 10

_NEUTRAL_SCORE = 0.5


def learned_factor(symbol: str, pnls: Sequence[float]) -> float:
    """Returns a value in `[0.0, 1.0]`. Returns EXACTLY `0.5` (neutral —
    not `0.0`, which would look like a manufactured penalty, mirroring
    `selector.py::_normalize`'s own "insufficient/no discriminating
    information -> 0.5" convention) when `len(pnls) < MIN_SYMBOL_TRADES`.
    `symbol` is accepted (and unused beyond this docstring's intent) so
    a caller building a `Callable[[str], float]` provider never needs an
    awkward wrapper lambda purely to satisfy the signature `selector.py`
    expects — see module docstring for the full formula above the
    threshold."""
    if len(pnls) < MIN_SYMBOL_TRADES:
        return _NEUTRAL_SCORE

    evidence = evidence_from_pnls(pnls)
    win_rate = evidence.win_rate if evidence.win_rate is not None else _NEUTRAL_SCORE
    if evidence.total_gross_pnl_per_unit > 0:
        profitability_component = 1.0
    elif evidence.total_gross_pnl_per_unit < 0:
        profitability_component = 0.0
    else:
        profitability_component = _NEUTRAL_SCORE

    return 0.5 * win_rate + 0.5 * profitability_component


=== FILE: adaptive/windows.py ===
"""
Adaptive Intelligence v1, step 6 — discovery vs. final-validation window
split (anti-overfitting, required).

Historical data is partitioned into two kinds of window:

- DISCOVERY: reused freely across many challenger-generation/evaluation
  rounds. Always the trailing `discovery_window_size` period ending
  exactly where the earliest not-yet-consumed CONFIRMATION window
  begins -- by construction this makes discovery data strictly and
  permanently OLDER than any confirmation window, so no ordering mistake
  can make challenger-generation or discovery-comparison code read
  confirmation data (see `TestDiscoveryConfirmationIsolation` in the
  test module for the enforced proof).
- CONFIRMATION: used ONLY ONCE per candidate, right before a promotion
  decision, then never revisited. Rolls forward over calendar time, tied
  to the step-13 scheduler's own firing interval -- `next_confirmation_
  window()` returns at most one new window per call, and a caller
  (`adaptive/cycle.py`) advances `last_confirmation_end` (persisted by
  `adaptive/store.py`, surviving restarts) only after that window has
  actually been consumed by a promotion decision.

This module reuses `research.oos_stability.OOSWindow` for the window
shape itself (never a second, divergent one) but does NOT reuse
`build_sequential_windows()` directly, because that function eagerly
materializes every window across a whole `[start, end)` span -- the
wrong shape for "the one next not-yet-consumed window, advanced
incrementally, restart-durable". Both this module's window construction
and `build_sequential_windows()` share the identical non-overlap
invariant (`step_size`/window boundaries never overlap) and the same
`OOSWindow` validation (UTC-aware, `start < end`)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from research.oos_stability import OOSWindow


@dataclass(frozen=True)
class WindowConfig:
    discovery_window_size: timedelta
    confirmation_window_size: timedelta

    def __post_init__(self) -> None:
        if self.discovery_window_size <= timedelta(0):
            raise ValueError("discovery_window_size pozitif olmalı")
        if self.confirmation_window_size <= timedelta(0):
            raise ValueError("confirmation_window_size pozitif olmalı")


@dataclass(frozen=True)
class DiscoveryConfirmationWindows:
    discovery: OOSWindow
    confirmation: OOSWindow

    def __post_init__(self) -> None:
        # The isolation invariant, enforced structurally: discovery data
        # is always strictly non-overlapping with, and entirely before,
        # the confirmation window paired with it.
        if self.discovery.end > self.confirmation.start:
            raise ValueError(
                "discovery penceresi confirmation penceresiyle çakışıyor -- "
                "bu asla olmamalı (izolasyon ihlali)"
            )


def next_confirmation_window(
    *, last_confirmation_end: datetime | None, anchor: datetime, config: WindowConfig, now: datetime,
) -> OOSWindow | None:
    """The next not-yet-consumed confirmation window, or `None` if it has
    not fully elapsed yet (never returns a window extending past `now` --
    no lookahead, no fabricated future data). `last_confirmation_end` is
    `None` for a brand-new `adaptive/` deployment (or one that has never
    completed a confirmation cycle) -- the first window then starts at
    `anchor` (e.g. the earliest historical data this deployment considers
    trustworthy, or the moment `adaptive/` was first enabled)."""
    window_start = last_confirmation_end or anchor
    window_end = window_start + config.confirmation_window_size
    if window_end > now:
        return None
    return OOSWindow(index=0, start=window_start, end=window_end)


def compute_cycle_windows(
    *, last_confirmation_end: datetime | None, anchor: datetime, config: WindowConfig, now: datetime,
) -> DiscoveryConfirmationWindows | None:
    """The ONE entry point `adaptive/cycle.py` calls each scheduled cycle
    to obtain this cycle's paired discovery+confirmation windows. Returns
    `None` when the next confirmation window has not elapsed yet -- the
    caller then runs discovery-only evaluation for this cycle (still
    useful for challenger screening) but skips confirmation/promotion
    entirely, exactly as if evidence were insufficient."""
    confirmation = next_confirmation_window(
        last_confirmation_end=last_confirmation_end, anchor=anchor, config=config, now=now,
    )
    if confirmation is None:
        return None
    discovery_end = confirmation.start
    discovery = OOSWindow(index=0, start=discovery_end - config.discovery_window_size, end=discovery_end)
    return DiscoveryConfirmationWindows(discovery=discovery, confirmation=confirmation)


=== FILE: crypto_signal_engine/errors.py ===
"""
Faz 2 exception taxonomy.

Kural (Bölüm 17): `except Exception: pass` yasaktır. Broad exception
yalnızca lifecycle boundary'de (log + state transition + cleanup +
retry/rethrow gibi açık davranışla) kullanılabilir. Bu modül, farklı
hata sınıflarının farklı şekillerde ele alınabilmesi (örn. retry-safe
transport hatası ile retry-safe OLMAYAN bir parse hatasının ayrılması)
için minimal ama yeterli bir hiyerarşi tanımlar.

Hiyerarşi kasıtlı olarak sığ tutulmuştur — her yeni exception türü gerçek
bir davranış farkı (farklı retry/log/state-transition politikası) taşımak
zorundadır.
"""

from __future__ import annotations


class CryptoSignalEngineError(Exception):
    """Faz 2 kaynaklı tüm hataların ortak atası."""


class TransportError(CryptoSignalEngineError):
    """Ağ/transport seviyesinde bir hata (bağlantı koptu, timeout, vb.).

    Genellikle retry-safe kabul edilir (bkz. reconnect.py / retry policy).
    """


class StaleFeedError(TransportError):
    """Bir WebSocket bağlantısından, konfigüre edilmiş
    `stale_feed_threshold_seconds` süresi boyunca hiç mesaj gelmedi.

    `TransportError`'ın bir alt sınıfıdır (mevcut genel `except
    TransportError:` reconnect blokları bunu da otomatik yakalar) ama
    ayrı bir tip olarak tanımlanmıştır ki çağıran kod "gerçek bir
    transport hatası" ile "veri akışı durdu ama socket hâlâ açık"
    durumunu health/log seviyesinde AYIRT edebilsin (Bölüm: stale-feed
    detection, DEGRADED ayrı bir state).
    """


class RateLimitError(TransportError):
    """Binance rate-limit/ban yanıtı (HTTP 429/418).

    `retry_after_seconds` doluysa, retry politikası bunu dikkate almalıdır.
    """

    def __init__(self, message: str, *, status_code: int, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class BinanceProtocolError(CryptoSignalEngineError):
    """Binance'in döndürdüğü yanıt, beklenen protokol şeklini taşımıyor
    (örn. hata kodu içeren bir JSON, beklenmeyen şema)."""


class ParseError(CryptoSignalEngineError):
    """Ham Binance payload'ı domain modeline dönüştürülemedi
    (eksik alan, yanlış tip, malformed sayı/timestamp, bilinmeyen event türü).

    Bu hata retry-safe DEĞİLDİR — aynı malformed veri tekrar denense de
    yine parse edilemez. Retry loop'una sokulmamalıdır (Bölüm 20).
    """


class SequenceGapError(CryptoSignalEngineError):
    """Bir akışta (candle update, order book diff) beklenmeyen bir
    sequence/ordering boşluğu tespit edildi; ilgili akış güvenilmez kabul
    edilmeli ve resync/backfill tetiklenmelidir."""


class OrderBookSyncError(SequenceGapError):
    """Order book snapshot+diff senkronizasyonu bozuldu; local book
    güvenilmez kabul edilip resync gerekir."""


class PersistenceError(CryptoSignalEngineError):
    """SQLite/persistence katmanında bir hata (yazma/okuma/şema)."""


class ConfigurationError(CryptoSignalEngineError):
    """Geçersiz/eksik Faz 2 konfigürasyonu — fail-fast için kullanılır."""


class FeatureError(CryptoSignalEngineError):
    """Faz 3 (Feature Engine) kaynaklı tüm hataların ortak atası."""


class FeatureValidationError(FeatureError):
    """Feature domain modeli (identity/snapshot) geçersiz bir state temsil etmeye çalışıyor."""


class InsufficientHistoryError(FeatureError):
    """Bir hesaplama için gereken minimum geçmiş veri mevcut değil.

    NaN/sıfır ile SESSİZCE doldurulmaz — bu, açık ve tipik bir "henüz
    hesaplanamaz" durumudur.
    """

    def __init__(self, feature_name: str, required: int, available: int) -> None:
        super().__init__(
            f"'{feature_name}' için yetersiz geçmiş: {required} gerekli, {available} mevcut"
        )
        self.feature_name = feature_name
        self.required = required
        self.available = available


class FeatureCalculationError(FeatureError):
    """Hesaplama sırasında matematiksel olarak tanımsız bir durum oluştu
    (sıfıra bölme, geçersiz logaritma domain'i, vb.)."""


class FeatureStateError(FeatureError):
    """Feature history/state store'da geçersiz bir işlem denendi
    (örn. eski bir snapshot'ın daha yeni canonical bir snapshot'ı ezmeye
    çalışması)."""


class AgentError(CryptoSignalEngineError):
    """Faz 4 (Agent/Consensus/Risk Engine) kaynaklı tüm hataların ortak atası."""


class AgentInputError(AgentError):
    """Bir agent'ın gereksinim duyduğu minimum feature seti mevcut değil.

    NaN/sıfır ile SESSİZCE doldurulmaz; eksik durum açıkça raporlanır
    (agent adı, timeframe, eksik feature adları).
    """

    def __init__(self, agent_name: str, timeframe: str, missing_features: list[str]) -> None:
        super().__init__(
            f"{agent_name} ({timeframe}): eksik zorunlu feature'lar: {', '.join(missing_features)}"
        )
        self.agent_name = agent_name
        self.timeframe = timeframe
        self.missing_features = missing_features


class ConsensusError(AgentError):
    """ConsensusEngine.combine() çağrısı geçersiz/tutarsız girdi ile yapıldı."""


class PaperTradingError(CryptoSignalEngineError):
    """Faz 5 (Paper Trading / Simulation Engine) kaynaklı tüm hataların ortak atası."""


class NoLookAheadViolationError(PaperTradingError):
    """Bir fiyat/sinyal zaman sıralaması no-look-ahead invariant'ını ihlal etti:
    gelecekten fiyat (`price.as_of > signal.timestamp`), sembol uyuşmazlığı,
    veya bir sembol için kronolojik olmayan sinyal işleme sırası."""


class IdempotencyConflictError(PaperTradingError):
    """Aynı `Signal.context_id`, daha önce işlenenden FARKLI bir Signal
    içeriğiyle tekrar işlenmeye çalışıldı — idempotency key'in stabil
    olduğu varsayımı ihlal edildi."""


class SymbolSelectionError(CryptoSignalEngineError):
    """Otomatik sembol evreni/fırsat seçimi (`crypto_signal_engine.selection`)
    güvenli şekilde tamamlanamadı (discovery/ranking hatası, VEYA hiçbir
    sembol eligibility/likidite/geçmiş-veri eşiklerini geçemedi).

    Fail-safe kural: bu hata YAKALANIP bir sembole (örn. BTCUSDT) sessizce
    düşülmez — `app.py::main()` bunu açık bir startup hatası olarak ele
    alır, mevcut durable PAPER state'e HİÇ dokunulmadan process
    başlatılmaz (bkz. selection/selector.py modül docstring'i)."""


class ExecutionError(CryptoSignalEngineError):
    """Faz 10 (Binance Spot TESTNET Execution Lab) kaynaklı tüm hataların
    ortak atası. Bu hiyerarşi YALNIZCA `crypto_signal_engine/execution/`
    paketi tarafından fırlatılır — hiçbir Mainnet/private-execution kod
    yolu bu hatayı BAŞKA hiçbir modülden fırlatmamalıdır (bkz.
    `execution/errors.py` alt sınıfları, SAFETY_INVARIANTS.md)."""


=== FILE: crypto_signal_engine/__init__.py ===
"""Binance Crypto Intelligence / Signal Engine — Phase 1 domain layer.

Bu paket, gerçek Binance bağlantısı, CCXT entegrasyonu veya execution kodu
İÇERMEZ. Yalnızca domain modelleri, enum'lar, provider/quality contract'ları
ve safety modelleri Faz 1 kapsamındadır.

ALLOW_LIVE_TRADING = False  # hard safety invariant, bkz. SAFETY_INVARIANTS.md
"""

__version__ = "0.1.0-phase1"

# Hard safety invariant. Bu satır hiçbir koşulda True yapılamaz ve hiçbir
# execution kodu bu paket içinde bulunmaz (Faz 1 = domain contract only).
ALLOW_LIVE_TRADING = False


=== FILE: crypto_signal_engine/signal_engine.py ===
"""
SignalEngine — Faz 4 orkestratörü.

Zorunlu sıra (Bölüm 31):
    1. AgentContext inşa et (no-look-ahead garantili)
    2. dört bağımsız agent'ı çalıştır
    3. ConsensusEngine.combine(evidence=[dört AgentEvidence]) çağır
    4. RiskOverlay.assess(consensus, regime) çağır — regime, AYNI
       RegimeAgent.evaluate() çağrısından gelen RegimeContext'tir
       (provenance correspondence burada, orkestratör seviyesinde
       garanti edilir — bkz. Bölüm 6)
    5. nihai Signal'ı birleştir

`SignalEngine`, `RegimeAgentOutput`'un kendisini ASLA `RiskOverlay`'e
geçirmez; ham evidence'ı da ayrıca geçirmez.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.agents.base import clamp
from crypto_signal_engine.agents.context import build_agent_context
from crypto_signal_engine.agents.market_structure import MarketStructureAgent
from crypto_signal_engine.agents.order_book import OrderBookAgent
from crypto_signal_engine.agents.quant import QuantAgent
from crypto_signal_engine.agents.regime import RegimeAgent
from crypto_signal_engine.consensus.engine import ConsensusEngine
from crypto_signal_engine.consensus.risk import RiskOverlay
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.features.state import FeatureHistoryStore

DEFAULT_MODEL_VERSION = "phase4-v1"


def _split_evidence(raw_score: float, evidence: tuple[AgentEvidence, ...]) -> tuple[list[AgentEvidence], list[AgentEvidence]]:
    """Bölüm 36 dokümante edilmiş politika: pozitif consensus için pozitif
    evidence supporting/negatif contradicting; negatif consensus için
    tersi; tam nötr (raw_score==0.0) consensus için sıfır-olmayan TÜM
    evidence "contradicting" sayılır (ortak bir yön kazanamadı). Sıfır
    skorlu evidence her durumda her iki koleksiyondan da OMİT edilir.
    """
    supporting: list[AgentEvidence] = []
    contradicting: list[AgentEvidence] = []
    if raw_score > 0.0:
        for e in evidence:
            if e.score > 0.0:
                supporting.append(e)
            elif e.score < 0.0:
                contradicting.append(e)
    elif raw_score < 0.0:
        for e in evidence:
            if e.score < 0.0:
                supporting.append(e)
            elif e.score > 0.0:
                contradicting.append(e)
    else:
        for e in evidence:
            if e.score != 0.0:
                contradicting.append(e)
    return supporting, contradicting


class SignalEngine:
    """Faz 4 orkestratörü: `FeatureHistoryStore` -> nihai `Signal`."""

    def __init__(self, history: FeatureHistoryStore, model_version: str = DEFAULT_MODEL_VERSION) -> None:
        self._history = history
        self._model_version = model_version
        self._quant_agent = QuantAgent()
        self._structure_agent = MarketStructureAgent()
        self._order_book_agent = OrderBookAgent()
        self._regime_agent = RegimeAgent()
        self._consensus_engine = ConsensusEngine()
        self._risk_overlay = RiskOverlay()

    def evaluate(self, symbol: str, as_of: datetime) -> Signal:
        context = build_agent_context(self._history, symbol, as_of)

        quant_evidence = self._quant_agent.evaluate(context)
        structure_evidence = self._structure_agent.evaluate(context)
        order_book_evidence = self._order_book_agent.evaluate(context)
        regime_output = self._regime_agent.evaluate(context)

        all_evidence = (quant_evidence, structure_evidence, order_book_evidence, regime_output.evidence)

        consensus = self._consensus_engine.combine(all_evidence, regime=regime_output.regime)

        # KRİTİK provenance garantisi: RiskOverlay'e geçirilen RegimeContext,
        # AYNI `regime_output` nesnesinden gelir (ConsensusEngine'e
        # geçirilenle TAM OLARAK aynı obje) — orkestratör bu ikisinin AYNI
        # değerlendirmeye ait olduğunu burada, tek bir yerde garanti eder.
        risk_assessment = self._risk_overlay.assess(consensus, regime_output.regime)

        confidence = clamp(
            abs(consensus.raw_score) * consensus.agreement * risk_assessment.confidence_multiplier, 0.0, 1.0
        )

        supporting, contradicting = _split_evidence(consensus.raw_score, consensus.contributing_evidence)

        return Signal(
            symbol=context.symbol,
            timestamp=context.as_of,
            context_id=context.context_id,
            score=consensus.raw_score,
            confidence=confidence,
            risk_level=risk_assessment.risk_level,
            primary_timeframe=Timeframe.M5,
            supporting_factors=tuple(supporting),
            contradicting_factors=tuple(contradicting),
            invalidation=(
                "Sinyal, çoklu-zaman-dilimi yönlü uyum maddi şekilde bozulursa "
                "(agent'lar arası agreement önemli ölçüde düşerse) geçersiz olur."
            ),
            model_version=self._model_version,
        )


=== FILE: research/attribution.py ===
"""
Part G — Performance Attribution.

Read-only, pure aggregation over `replay.ReplayResult.cycle_results`.
Never parses `AgentEvidence.rationale` (a free-text field) to manufacture
attribution — every dimension below is derived exclusively from typed,
structured fields already on the accepted `Signal`/`AgentEvidence`
contracts (`Signal.symbol/direction/risk_level/model_version/confidence/
score`, `AgentEvidence.agent`/`score`). Regime attribution is reported as
explicitly NOT AVAILABLE (see `_REGIME_NOTE` below) because
`RegimeContext` is not part of the accepted `Signal` contract — Phase 5
never touches `ConsensusResult`/`RegimeContext` (see `paper_trading/
engine.py` module docstring), so no durable output carries it forward to
attribute against. This is stated honestly rather than fabricated.

A "trade" here means one completed round-trip (a signal-driven direction
change that produced BOTH a closing fill and a re-opening fill in the
same `PaperTradingEngine` cycle — see `extract_trades()`). A position
still open at the end of a replay window is excluded from trade
statistics (its PnL is unrealized, not yet a completed trade) — this is
stated as a report limitation, not silently dropped.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, SignalDirection
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.runtime.models import RuntimeCycleResult

_REGIME_NOTE = (
    "NOT AVAILABLE FROM CURRENT DURABLE OUTPUT: RegimeContext is not part of "
    "the accepted Signal contract — Phase 5's PaperTradingEngine never touches "
    "ConsensusResult/RegimeContext (see paper_trading/engine.py module "
    "docstring), so no durable replay output carries regime forward to "
    "attribute trades against. This is a structural limitation, not an "
    "oversight, and this module does not fabricate a substitute."
)

_CONFIDENCE_BUCKET_EDGES = (0.25, 0.5, 0.75, 1.0)


def _bucket_label(value: float, edges: Sequence[float]) -> str:
    lo = 0.0
    for edge in edges:
        if value < edge or edge == edges[-1]:
            return f"[{lo:.2f}, {edge:.2f}]" if edge == edges[-1] else f"[{lo:.2f}, {edge:.2f})"
        lo = edge
    return f"[{edges[-1]:.2f}, 1.00]"  # pragma: no cover - defensive


@dataclass(frozen=True)
class TradeRecord:
    """One completed round-trip trade, attributed back to the Signal
    that opened it."""

    symbol: str
    open_context_id: str
    open_timestamp: datetime
    close_timestamp: datetime
    direction: SignalDirection
    risk_level: RiskLevel
    model_version: str
    confidence: float
    score: float
    net_pnl: float
    fees: float
    supporting_agents: tuple[AgentName, ...]
    contradicting_agents: tuple[AgentName, ...]


def extract_trades(cycle_results: Sequence[RuntimeCycleResult]) -> tuple[TradeRecord, ...]:
    """Walks evaluated cycle results in the order they were produced
    (already chronological per `replay.py`'s ordering policy) and
    reconstructs completed round-trip trades from `PaperTradingEngine`'s
    own accepted accounting semantics: a cycle whose `paper_result`
    carries exactly two fills is a close-then-reopen round trip (the
    only way `PaperTradingEngine.process_signal` ever produces two fills
    in one call — see its module docstring); a cycle with fewer than two
    fills never completed a round trip in this window (either NO_ACTION,
    an unchanged-direction repeat, an idempotent replay, or a fresh
    FLAT->open with nothing yet to close) and contributes no trade.

    A completed trade is attributed to the SIGNAL THAT OPENED the
    position being closed — the entry decision being evaluated — NOT the
    signal that happens to close it (a reversal's closing signal is a
    *different* decision, already the entry for the *next* trade). This
    requires tracking, per symbol, which earlier signal most recently
    opened the position that is currently held — done here via
    `open_signal_by_symbol`, updated on every fresh open (one fill) and
    every reversal's re-open leg (two fills)."""
    trades: list[TradeRecord] = []
    last_realized_pnl: dict[str, float] = {}
    open_signal_by_symbol: dict[str, Signal] = {}

    for cycle in cycle_results:
        if not cycle.evaluated or cycle.signal is None or cycle.paper_result is None:
            continue
        result = cycle.paper_result
        if result.idempotent_replay or len(result.fills) == 0:
            continue
        signal = cycle.signal

        if len(result.fills) == 2:
            opening_signal = open_signal_by_symbol.get(result.symbol)
            if opening_signal is not None:
                previous = last_realized_pnl.get(result.symbol, 0.0)
                net_pnl = result.position.realized_pnl - previous
                fees = sum(f.fee for f in result.fills)
                close_fill, _open_fill = result.fills
                trades.append(
                    TradeRecord(
                        symbol=result.symbol,
                        open_context_id=opening_signal.context_id,
                        open_timestamp=opening_signal.timestamp,
                        close_timestamp=close_fill.filled_at,
                        direction=opening_signal.direction,
                        risk_level=opening_signal.risk_level,
                        model_version=opening_signal.model_version,
                        confidence=opening_signal.confidence,
                        score=opening_signal.score,
                        net_pnl=net_pnl,
                        fees=fees,
                        supporting_agents=tuple(e.agent for e in opening_signal.supporting_factors),
                        contradicting_agents=tuple(e.agent for e in opening_signal.contradicting_factors),
                    )
                )
            last_realized_pnl[result.symbol] = result.position.realized_pnl
            # `signal` just opened the NEW (post-reversal) leg — it becomes
            # the opener to attribute the NEXT trade to.
            open_signal_by_symbol[result.symbol] = signal
        else:
            # Exactly one fill: a fresh FLAT -> open with nothing yet to
            # close. Nothing to realize yet; remember it as this
            # symbol's opener for whenever it eventually closes.
            open_signal_by_symbol[result.symbol] = signal
    return tuple(trades)


@dataclass(frozen=True)
class PerformanceMetrics:
    """Core research metrics for one trade group. Every ratio that can
    be undefined (division by zero) is `None`, never a fabricated 0.0 or
    infinity (Section G: "avoid meaningless precision", "handle
    zero-trade/zero-loss divisions explicitly")."""

    trade_count: int
    wins: int
    losses: int
    breakeven: int
    win_rate: float | None
    gross_profit: float
    gross_loss: float
    net_pnl: float
    total_fees: float
    average_win: float | None
    average_loss: float | None
    profit_factor: float | None
    max_drawdown: float | None


def _compute_metrics(trades: Sequence[TradeRecord], *, with_drawdown: bool) -> PerformanceMetrics:
    trade_count = len(trades)
    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl < 0]
    breakeven = trade_count - len(wins) - len(losses)
    gross_profit = sum(t.net_pnl for t in wins)
    gross_loss = sum(-t.net_pnl for t in losses)
    net_pnl = sum(t.net_pnl for t in trades)
    total_fees = sum(t.fees for t in trades)

    win_rate = (len(wins) / trade_count) if trade_count > 0 else None
    average_win = (gross_profit / len(wins)) if wins else None
    average_loss = (gross_loss / len(losses)) if losses else None
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None

    max_drawdown: float | None = None
    if with_drawdown and trades:
        ordered = sorted(trades, key=lambda t: t.close_timestamp)
        equity = 0.0
        peak = 0.0
        worst = 0.0
        for t in ordered:
            equity += t.net_pnl
            peak = max(peak, equity)
            worst = max(worst, peak - equity)
        max_drawdown = worst

    return PerformanceMetrics(
        trade_count=trade_count, wins=len(wins), losses=len(losses), breakeven=breakeven,
        win_rate=win_rate, gross_profit=gross_profit, gross_loss=gross_loss, net_pnl=net_pnl,
        total_fees=total_fees, average_win=average_win, average_loss=average_loss,
        profit_factor=profit_factor, max_drawdown=max_drawdown,
    )


@dataclass(frozen=True)
class AttributionReport:
    overall: PerformanceMetrics
    by_symbol: Mapping[str, PerformanceMetrics]
    by_direction: Mapping[str, PerformanceMetrics]
    by_risk_level: Mapping[str, PerformanceMetrics]
    by_model_version: Mapping[str, PerformanceMetrics]
    by_confidence_bucket: Mapping[str, PerformanceMetrics]
    by_supporting_agent: Mapping[str, PerformanceMetrics]
    regime_attribution_note: str = field(default=_REGIME_NOTE)
    open_position_count_excluded: int = 0


def _group_by(trades: Sequence[TradeRecord], key_fn) -> Mapping[str, PerformanceMetrics]:
    buckets: dict[str, list[TradeRecord]] = {}
    for t in trades:
        buckets.setdefault(key_fn(t), []).append(t)
    return {label: _compute_metrics(group, with_drawdown=False) for label, group in sorted(buckets.items())}


def compute_report(cycle_results: Sequence[RuntimeCycleResult], *, open_position_count: int = 0) -> AttributionReport:
    trades = extract_trades(cycle_results)

    by_agent: dict[str, list[TradeRecord]] = {}
    for t in trades:
        for agent in set(t.supporting_agents):
            by_agent.setdefault(agent.value, []).append(t)

    return AttributionReport(
        overall=_compute_metrics(trades, with_drawdown=True),
        by_symbol=_group_by(trades, lambda t: t.symbol),
        by_direction=_group_by(trades, lambda t: t.direction.value),
        by_risk_level=_group_by(trades, lambda t: t.risk_level.value),
        by_model_version=_group_by(trades, lambda t: t.model_version),
        by_confidence_bucket=_group_by(
            trades, lambda t: _bucket_label(t.confidence, _CONFIDENCE_BUCKET_EDGES)
        ),
        by_supporting_agent={
            label: _compute_metrics(group, with_drawdown=False) for label, group in sorted(by_agent.items())
        },
        open_position_count_excluded=open_position_count,
    )


=== FILE: research/data_quality.py ===
"""
Part A — Historical Data Quality Validation.

An additive, read-only validator for historical candle datasets, run
BEFORE any dataset is handed to `replay.HistoricalReplayDriver`. It
never mutates its input and never fills gaps by default — validation
fails explicitly on any anomaly instead of silently normalizing it.

Design choice — reuse, don't duplicate: per-row numeric/timestamp/OHLC
validity is NOT reimplemented here. `crypto_signal_engine.domain.models.
Candle.__post_init__` (Phase 1, accepted, unmodified) already enforces
UTC-aware timestamps, finite OHLCV numerics, and impossible-OHLC
rejection. This validator either (a) accepts already-constructed
`Candle` instances (the normal path — `fetch_historical_candles` already
returns these) and checks only DATASET-level properties that a single
`Candle` cannot express (identity consistency, ordering, spacing,
duplicates, gaps, overlap), or (b) accepts raw row mappings (for
fixtures/tests that need to exercise malformed input) and attempts to
construct a `Candle` per row, capturing any `ValueError`/`TypeError` from
that ACCEPTED constructor as a structured, reported violation instead of
letting it propagate as an uncaught exception from an unrelated call
site.

Minimum checks performed (Section A of the enhancement-pass spec):
symbol identity, timeframe identity, UTC-aware timestamps (via Candle),
finite OHLCV numerics (via Candle), closed-candle semantics, unique
(symbol, timeframe, open_time), chronological consistency, expected
timeframe spacing, duplicate candles, missing bars/gaps, open_time/
close_time consistency, no impossible overlap/regression.

Gap policy: by default (`allow_gaps=False`), any gap in the sequence is
a hard, dataset-failing violation (`GAP_DETECTED`) — the validator NEVER
fills gaps itself. `allow_gaps=True` is a separate, explicitly-named
mode: gaps are then reported as structured `GapRecord`s on the report
(`report.gaps`) instead of failing validation, but the report is
unambiguously marked `gap_tolerant=True` so a caller can never confuse
"validated as gap-free" with "validated while explicitly tolerating
gaps" (see `DataQualityReport.gap_tolerant`).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from research.errors import DataQualityError

_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}

# Binance's REST kline `closeTime` field (index 6) is preserved verbatim by
# `providers/binance/parser.py::parse_kline_rest_row` — it is the exchange's
# own convention of `open_time + duration - 1ms`, NOT an exact boundary. A
# dataset produced by the accepted `fetch_historical_candles()` therefore
# legitimately has `close_time - open_time == duration - 1ms` for every row,
# and two contiguous candles legitimately have
# `next.open_time - previous.close_time == 1ms` (the next candle's exact
# open boundary is 1ms after the previous one's Binance-reported close).
# This tolerance makes duration/contiguity checks correct for genuine
# Binance data instead of flagging its normal convention as a defect.
_CLOSE_TIME_CONVENTION_TOLERANCE = timedelta(milliseconds=1)


@dataclass(frozen=True)
class DataQualityViolation:
    """A single, structured dataset-quality finding.

    `code` is a stable, machine-readable identifier (never renamed
    across versions of this module) so callers can filter/aggregate
    programmatically instead of parsing `detail` strings.
    """

    code: str
    index: int | None
    detail: str

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        where = f"[row {self.index}] " if self.index is not None else ""
        return f"{where}{self.code}: {self.detail}"


@dataclass(frozen=True)
class GapRecord:
    """A detected missing-bar gap between two consecutive accepted candles.

    Only ever populated when the validator was explicitly constructed
    with `allow_gaps=True` — under default (strict) validation, a gap is
    a `DataQualityViolation` instead and the dataset fails.
    """

    after_open_time: datetime
    before_open_time: datetime
    missing_bar_count: int


@dataclass(frozen=True)
class DataQualityReport:
    """Full result of validating one (symbol, timeframe) candle dataset."""

    symbol: str
    timeframe: Timeframe
    candle_count: int
    violations: tuple[DataQualityViolation, ...]
    gaps: tuple[GapRecord, ...] = field(default_factory=tuple)
    gap_tolerant: bool = False

    @property
    def passed(self) -> bool:
        return not self.violations


class HistoricalDatasetValidator:
    """Validates a historical candle dataset for one (symbol, timeframe)
    before it is trusted as replay input. Read-only — never mutates the
    input sequence, never fills gaps."""

    def __init__(self, *, allow_gaps: bool = False) -> None:
        self._allow_gaps = allow_gaps

    def validate(
        self,
        rows: Sequence[Candle | Mapping[str, object]],
        *,
        symbol: str,
        timeframe: Timeframe,
    ) -> DataQualityReport:
        canonical_symbol = normalize_symbol(symbol)
        if timeframe not in _TIMEFRAME_DURATIONS:
            raise ValueError(f"unsupported timeframe: {timeframe!r}")
        expected_duration = _TIMEFRAME_DURATIONS[timeframe]

        violations: list[DataQualityViolation] = []
        candles: list[Candle | None] = []

        for index, row in enumerate(rows):
            if isinstance(row, Candle):
                candles.append(row)
                continue
            try:
                candle = Candle(
                    symbol=str(row["symbol"]),
                    timeframe=timeframe,
                    open_time=row["open_time"],  # type: ignore[arg-type]
                    close_time=row["close_time"],  # type: ignore[arg-type]
                    open=row["open"],  # type: ignore[arg-type]
                    high=row["high"],  # type: ignore[arg-type]
                    low=row["low"],  # type: ignore[arg-type]
                    close=row["close"],  # type: ignore[arg-type]
                    volume=row["volume"],  # type: ignore[arg-type]
                    is_closed=bool(row.get("is_closed", True)),
                    trade_count=row.get("trade_count"),  # type: ignore[arg-type]
                )
            except (ValueError, TypeError, KeyError) as exc:
                violations.append(
                    DataQualityViolation(code="MALFORMED_CANDLE", index=index, detail=str(exc))
                )
                candles.append(None)
                continue
            candles.append(candle)

        # Row-level identity/closed-semantics checks (only for rows that
        # successfully constructed a Candle).
        for index, candle in enumerate(candles):
            if candle is None:
                continue
            if candle.symbol != canonical_symbol:
                violations.append(
                    DataQualityViolation(
                        code="SYMBOL_MISMATCH", index=index,
                        detail=f"expected {canonical_symbol}, got {candle.symbol}",
                    )
                )
            if candle.timeframe != timeframe:
                violations.append(
                    DataQualityViolation(
                        code="TIMEFRAME_MISMATCH", index=index,
                        detail=f"expected {timeframe.value}, got {candle.timeframe.value}",
                    )
                )
            if not candle.is_closed:
                violations.append(
                    DataQualityViolation(
                        code="UNCLOSED_CANDLE", index=index,
                        detail="historical dataset candle must represent a CLOSED bar",
                    )
                )
            actual_duration = candle.close_time - candle.open_time
            duration_delta = expected_duration - actual_duration
            if not (timedelta(0) <= duration_delta <= _CLOSE_TIME_CONVENTION_TOLERANCE):
                violations.append(
                    DataQualityViolation(
                        code="WRONG_DURATION", index=index,
                        detail=(
                            f"close_time - open_time = {actual_duration}, expected "
                            f"{expected_duration} (tolerance {_CLOSE_TIME_CONVENTION_TOLERANCE} "
                            f"for Binance's close_time-1ms convention) for {timeframe.value}"
                        ),
                    )
                )

        # Dataset-level: duplicate/ordering/overlap/spacing/gaps.
        seen_open_times: dict[datetime, int] = {}
        gaps: list[GapRecord] = []
        previous: Candle | None = None
        previous_index: int | None = None
        for index, candle in enumerate(candles):
            if candle is None:
                continue
            if candle.open_time in seen_open_times:
                violations.append(
                    DataQualityViolation(
                        code="DUPLICATE_OPEN_TIME", index=index,
                        detail=(
                            f"open_time {candle.open_time} duplicates row "
                            f"{seen_open_times[candle.open_time]}"
                        ),
                    )
                )
                continue  # do not use a duplicate as the ordering anchor
            seen_open_times[candle.open_time] = index

            if previous is not None:
                if candle.open_time < previous.open_time:
                    violations.append(
                        DataQualityViolation(
                            code="CHRONOLOGICAL_REGRESSION", index=index,
                            detail=(
                                f"open_time {candle.open_time} < previous "
                                f"open_time {previous.open_time} (row {previous_index})"
                            ),
                        )
                    )
                elif candle.open_time < previous.close_time - _CLOSE_TIME_CONVENTION_TOLERANCE:
                    violations.append(
                        DataQualityViolation(
                            code="IMPOSSIBLE_OVERLAP", index=index,
                            detail=(
                                f"open_time {candle.open_time} precedes previous "
                                f"close_time {previous.close_time} (row {previous_index}, "
                                f"beyond the {_CLOSE_TIME_CONVENTION_TOLERANCE} close_time convention tolerance)"
                            ),
                        )
                    )
                elif candle.open_time > previous.close_time + _CLOSE_TIME_CONVENTION_TOLERANCE:
                    missing = round(
                        (candle.open_time - previous.close_time - _CLOSE_TIME_CONVENTION_TOLERANCE)
                        / expected_duration
                    )
                    if missing > 0:
                        if self._allow_gaps:
                            gaps.append(
                                GapRecord(
                                    after_open_time=previous.open_time,
                                    before_open_time=candle.open_time,
                                    missing_bar_count=missing,
                                )
                            )
                        else:
                            violations.append(
                                DataQualityViolation(
                                    code="GAP_DETECTED", index=index,
                                    detail=(
                                        f"{missing} missing bar(s) between row {previous_index} "
                                        f"(close {previous.close_time}) and row {index} "
                                        f"(open {candle.open_time})"
                                    ),
                                )
                            )
                # Within [previous.close_time - tolerance, + tolerance]: contiguous, no finding
                # (covers both an exact boundary match and Binance's close_time-1ms convention).
            previous = candle
            previous_index = index

        return DataQualityReport(
            symbol=canonical_symbol,
            timeframe=timeframe,
            candle_count=len(rows),
            violations=tuple(violations),
            gaps=tuple(gaps),
            gap_tolerant=self._allow_gaps,
        )

    def validate_or_raise(
        self,
        rows: Sequence[Candle | Mapping[str, object]],
        *,
        symbol: str,
        timeframe: Timeframe,
    ) -> DataQualityReport:
        """Same as `validate()` but fails explicitly (raises
        `DataQualityError`) instead of returning a failing report — the
        preferred entry point for replay, which must never proceed on an
        invalid dataset."""
        report = self.validate(rows, symbol=symbol, timeframe=timeframe)
        if not report.passed:
            raise DataQualityError(report.violations)
        return report


=== FILE: research/drift.py ===
"""
Part M — Backtest <-> Paper Drift (provenance structures only).

Deliberately minimal per the enhancement-pass spec ("Do not build a large
drift system now. Actual backtest-vs-paper drift analysis should wait
until the upcoming multi-week PAPER run has data."). This module ONLY
prepares a small, compatible provenance record — built identically from
either a `replay.ReplayResult`'s cycle results or a live/paper
`RuntimeCoordinator`'s own `RuntimeCycleResult` stream — keyed by
`context_id`, `model_version`, `symbol`, and signal timestamp, plus a
basic, honest comparison function. No dashboard, no persistence, no
scheduled job. This is scaffolding for a FUTURE comparison, not a
present-tense drift report — there is no live paper history yet to
compare against.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from crypto_signal_engine.runtime.models import RuntimeCycleResult


class ProvenanceSource(str, Enum):
    REPLAY = "REPLAY"
    PAPER_LIVE = "PAPER_LIVE"


@dataclass(frozen=True)
class SignalProvenanceRecord:
    """The minimal, compatible fields needed to later match a replayed
    signal against a live/paper one for the same underlying market
    context. Built identically regardless of source."""

    context_id: str
    symbol: str
    model_version: str
    signal_timestamp: datetime
    score: float
    confidence: float
    risk_level: str
    source: ProvenanceSource


def provenance_from_cycle_result(cycle: RuntimeCycleResult, *, source: ProvenanceSource) -> SignalProvenanceRecord | None:
    """Returns `None` for an unevaluated cycle (nothing to compare)."""
    if not cycle.evaluated or cycle.signal is None:
        return None
    signal = cycle.signal
    return SignalProvenanceRecord(
        context_id=signal.context_id, symbol=signal.symbol, model_version=signal.model_version,
        signal_timestamp=signal.timestamp, score=signal.score, confidence=signal.confidence,
        risk_level=signal.risk_level.value, source=source,
    )


def provenance_index(
    cycle_results: Sequence[RuntimeCycleResult], *, source: ProvenanceSource
) -> Mapping[str, SignalProvenanceRecord]:
    """Indexes by `context_id` — the same stable, deterministic key
    already used everywhere else in the accepted system for idempotency
    and provenance (Phase 4/5)."""
    index: dict[str, SignalProvenanceRecord] = {}
    for cycle in cycle_results:
        record = provenance_from_cycle_result(cycle, source=source)
        if record is not None:
            index[record.context_id] = record
    return index


class DriftFinding(str, Enum):
    MATCHED = "MATCHED"
    SCORE_MISMATCH = "SCORE_MISMATCH"
    MISSING_IN_REPLAY = "MISSING_IN_REPLAY"
    MISSING_IN_PAPER = "MISSING_IN_PAPER"


_SCORE_TOLERANCE = 1e-9


def compare_by_context_id(
    replay_index: Mapping[str, SignalProvenanceRecord],
    paper_index: Mapping[str, SignalProvenanceRecord],
) -> Mapping[str, DriftFinding]:
    """Basic, honest comparison — NOT a statistical drift model, just a
    per-context_id equality check. Real drift analysis (distributions,
    trend detection, alerting) is explicitly deferred (Section M) until
    there is real multi-week paper history to analyze."""
    findings: dict[str, DriftFinding] = {}
    for context_id in set(replay_index) | set(paper_index):
        replay_record = replay_index.get(context_id)
        paper_record = paper_index.get(context_id)
        if replay_record is None:
            findings[context_id] = DriftFinding.MISSING_IN_REPLAY
        elif paper_record is None:
            findings[context_id] = DriftFinding.MISSING_IN_PAPER
        elif abs(replay_record.score - paper_record.score) > _SCORE_TOLERANCE:
            findings[context_id] = DriftFinding.SCORE_MISMATCH
        else:
            findings[context_id] = DriftFinding.MATCHED
    return findings


=== FILE: research/errors.py ===
"""
Exception taxonomy for the research/validation layer.

Kept deliberately shallow (same discipline as `crypto_signal_engine.
errors`): a new exception type is only introduced when it carries a real
behavioral difference for the caller, not for cosmetic categorization.
"""

from __future__ import annotations


class ResearchError(Exception):
    """Common ancestor for every error raised by the `research/` package."""


class DataQualityError(ResearchError):
    """A historical dataset failed `data_quality.HistoricalDatasetValidator`.

    Carries the full, structured list of violations found so a caller can
    report all of them at once rather than fail on the first one.
    """

    def __init__(self, violations: "tuple[object, ...]") -> None:
        summary = "; ".join(str(v) for v in violations[:10])
        more = f" (+{len(violations) - 10} more)" if len(violations) > 10 else ""
        super().__init__(f"historical dataset failed quality validation: {summary}{more}")
        self.violations = violations


class ReplayConfigurationError(ResearchError):
    """`HistoricalReplayDriver` was constructed or invoked with an invalid,
    ambiguous, or unsafe configuration (e.g. empty symbol set, non-UTC
    bounds, unsupported timeframe combination)."""


class ReplayIntegrityError(ResearchError):
    """A replay run's own internal invariant was violated — this is a
    defensive, should-never-happen guard (e.g. the deterministic
    close-time grouping policy produced a candle application order that
    would look ahead). Raised instead of silently continuing, because a
    violated invariant here means the replay result cannot be trusted."""


class SizingPolicyError(ResearchError):
    """A `sizing.PortfolioRiskSizingPolicy` was given invalid configuration
    or invalid runtime inputs (fails closed rather than guessing)."""


=== FILE: research/guardrails.py ===
"""
Part K — Portfolio Guardrails / Circuit Breaker.

Conservative, deterministic, PAPER-only portfolio-wide caps. Deliberately
NOT Markowitz/risk-parity/covariance optimization — see module docstring
in `sizing.py` for the composition. This module answers exactly one
question, purely: "given the current portfolio snapshot, how much MORE
notional (if any) is this symbol allowed to take on right now?" It never
touches `PaperTradingEngine` state and never decides to close/flatten an
existing position — the explicit, documented first behaviour requested
by the enhancement-pass spec is HALT OR CAP new/increased risk, never
automatic panic-flatten.

`evaluate_guardrails()` is a pure function of a `PortfolioSnapshot` and a
`RiskBudgetConfig` — no I/O, no randomness, no wall-clock, fully
deterministic and independently testable from `sizing.py`'s per-signal
confidence/risk_level scaling.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum

from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.paper_trading.models import PaperPosition, PositionSide


class GuardrailBreach(str, Enum):
    """A specific, named portfolio-wide guardrail condition. Kept as an
    explicit, closed enum (not a free-text reason only) so callers can
    branch on breach type programmatically."""

    MAX_CONCURRENT_POSITIONS = "MAX_CONCURRENT_POSITIONS"
    MAX_GROSS_NOTIONAL_EXHAUSTED = "MAX_GROSS_NOTIONAL_EXHAUSTED"
    CUMULATIVE_REALIZED_LOSS = "CUMULATIVE_REALIZED_LOSS"


@dataclass(frozen=True)
class RiskBudgetConfig:
    """Explicit, immutable, fully-documented configuration for the
    guardrail + sizing layer. Every multiplier/cap here is operator
    config — nothing is inferred or silently defaulted from strategy
    internals, and none of it reinterprets `Signal.score` (see
    `sizing.py`)."""

    reference_capital: float
    max_per_symbol_notional: float
    max_gross_notional: float
    max_concurrent_positions: int
    max_cumulative_realized_loss: float  # positive magnitude; breached when aggregate_realized_pnl <= -this
    confidence_multiplier_floor: float = 0.25
    risk_level_multipliers: Mapping[str, float] = field(
        default_factory=lambda: {"LOW": 1.0, "MEDIUM": 0.75, "HIGH": 0.5, "EXTREME": 0.25}
    )

    def __post_init__(self) -> None:
        for name in ("reference_capital", "max_per_symbol_notional", "max_gross_notional", "max_cumulative_realized_loss"):
            value = getattr(self, name)
            require_finite(value, name)
            if value <= 0:
                raise ValueError(f"{name} pozitif olmalı")
        if self.max_concurrent_positions <= 0:
            raise ValueError("max_concurrent_positions pozitif olmalı")
        require_finite(self.confidence_multiplier_floor, "confidence_multiplier_floor")
        if not (0.0 <= self.confidence_multiplier_floor <= 1.0):
            raise ValueError("confidence_multiplier_floor [0.0, 1.0] aralığında olmalı")
        if self.max_per_symbol_notional > self.max_gross_notional:
            raise ValueError("max_per_symbol_notional, max_gross_notional'ı aşamaz")


@dataclass(frozen=True)
class PortfolioSnapshot:
    """A read-only view of portfolio state at decision time. Built by
    the caller (see `sizing.SizedPaperTradingEngine`) from
    `PaperTradingEngine`'s existing PUBLIC accessors only — never from
    private state."""

    positions: Mapping[str, PaperPosition]
    aggregate_realized_pnl: float

    def notional_of(self, symbol: str) -> float:
        position = self.positions.get(symbol)
        if position is None or position.side is PositionSide.FLAT:
            return 0.0
        return position.quantity * position.average_entry_price

    def gross_notional_excluding(self, symbol: str) -> float:
        return sum(
            self.notional_of(sym) for sym in self.positions if sym != symbol
        )

    def concurrent_positions_excluding(self, symbol: str) -> int:
        return sum(
            1 for sym, pos in self.positions.items()
            if sym != symbol and pos.side is not PositionSide.FLAT
        )


@dataclass(frozen=True)
class GuardrailStatus:
    """Immutable, small decision model (Section J: "prefer a small
    immutable decision model")."""

    breaches: tuple[GuardrailBreach, ...]
    available_symbol_notional: float  # 0.0 if any breach applies
    concurrent_position_count: int
    aggregate_realized_pnl: float

    @property
    def halted(self) -> bool:
        return len(self.breaches) > 0


def evaluate_guardrails(
    *, symbol: str, is_new_symbol_exposure: bool, snapshot: PortfolioSnapshot, config: RiskBudgetConfig
) -> GuardrailStatus:
    """Pure function: no mutation, no I/O. `is_new_symbol_exposure` is
    True when the caller's current position for `symbol` is FLAT/absent
    (i.e. this decision would open a brand-new exposure rather than
    resize an existing one in the same direction) — the concurrent-
    positions cap only applies to genuinely NEW exposures, matching
    "HALT new/increased risk" rather than penalizing a symbol that is
    already within budget."""
    breaches: list[GuardrailBreach] = []

    if snapshot.aggregate_realized_pnl <= -config.max_cumulative_realized_loss:
        breaches.append(GuardrailBreach.CUMULATIVE_REALIZED_LOSS)

    concurrent_other = snapshot.concurrent_positions_excluding(symbol)
    if is_new_symbol_exposure and (concurrent_other + 1) > config.max_concurrent_positions:
        breaches.append(GuardrailBreach.MAX_CONCURRENT_POSITIONS)

    gross_other = snapshot.gross_notional_excluding(symbol)
    remaining_gross = config.max_gross_notional - gross_other
    if remaining_gross <= 0:
        breaches.append(GuardrailBreach.MAX_GROSS_NOTIONAL_EXHAUSTED)
        remaining_gross = 0.0

    if breaches:
        available = 0.0
    else:
        available = min(config.max_per_symbol_notional, remaining_gross)

    return GuardrailStatus(
        breaches=tuple(breaches),
        available_symbol_notional=max(available, 0.0),
        concurrent_position_count=concurrent_other,
        aggregate_realized_pnl=snapshot.aggregate_realized_pnl,
    )


=== FILE: research/monte_carlo.py ===
"""
Part H — Monte Carlo Robustness Screen.

Purpose: estimate PATH/DRAWDOWN SENSITIVITY of the already-realized trade
outcome sequence from a replay. NOT a proof of statistical edge — see
`ASSUMPTIONS_NOTE` below, which every `MonteCarloReport` carries
verbatim. Pure permutation of a fixed multiset of trade P&Ls leaves the
TERMINAL P&L invariant by construction (sum is order-independent); this
module reports that fact honestly (Section H: "If pure permutation
leaves terminal PnL invariant, report that fact honestly") rather than
implying the screen tests profitability. What DOES vary across
permutations is the PATH — in particular maximum drawdown — which is
genuinely path-order-dependent and is what this screen actually
measures.

Determinism: every permutation is driven by a single, caller-supplied
seed through a LOCAL `random.Random(seed)` instance — the module-global
`random` state is never touched, so this is safe to call from anywhere
without cross-contaminating unrelated randomness elsewhere in a process.
Same seed + same input trade sequence -> byte-identical output, always.
The input trade P&L sequence itself is never mutated (every shuffle
operates on a fresh copy).
"""

from __future__ import annotations

import random
import statistics
from dataclasses import dataclass
from collections.abc import Sequence

from research.errors import ResearchError

ASSUMPTIONS_NOTE = (
    "This is a PATH/DRAWDOWN SENSITIVITY screen, not a test of statistical "
    "edge. Permuting the realized trade sequence assumes exchangeability "
    "(that trade order carries no information) — real crypto price series "
    "are autocorrelated and violate this assumption, so a 'typical' "
    "permuted drawdown does NOT prove the strategy has genuine edge, and an "
    "atypical one does not disprove it. Terminal PnL is mathematically "
    "invariant under permutation (the sum of a fixed multiset does not "
    "depend on order) — this screen never uses it as a pass/fail signal; "
    "only the drawdown distribution is informative here."
)


class MonteCarloConfigurationError(ResearchError):
    pass


def _max_drawdown(pnls: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


@dataclass(frozen=True)
class MonteCarloReport:
    seed: int
    iterations: int
    trade_count: int
    original_terminal_pnl: float
    original_max_drawdown: float
    terminal_pnl_invariant: bool
    permuted_max_drawdowns: tuple[float, ...]
    drawdown_min: float
    drawdown_max: float
    drawdown_mean: float
    drawdown_median: float
    assumptions_note: str = ASSUMPTIONS_NOTE


def run_monte_carlo_robustness_screen(
    trade_pnls: Sequence[float], *, seed: int, iterations: int
) -> MonteCarloReport:
    """Deterministic for a given `(trade_pnls, seed, iterations)` triple.
    Raises `MonteCarloConfigurationError` on invalid config (fails
    closed rather than silently clamping)."""
    if iterations <= 0:
        raise MonteCarloConfigurationError("iterations pozitif olmalı")
    if not isinstance(seed, int):
        raise MonteCarloConfigurationError("seed bir int olmalı (deterministic-ness için)")

    original = list(trade_pnls)
    original_terminal = sum(original)
    original_drawdown = _max_drawdown(original)

    rng = random.Random(seed)  # local instance — module-global random state untouched
    permuted_drawdowns: list[float] = []
    terminal_invariant = True
    for _ in range(iterations):
        shuffled = list(original)  # fresh copy every iteration — input never mutated
        rng.shuffle(shuffled)
        if abs(sum(shuffled) - original_terminal) > 1e-9:
            terminal_invariant = False  # pragma: no cover - defensive, mathematically unreachable
        permuted_drawdowns.append(_max_drawdown(shuffled))

    if not permuted_drawdowns:  # pragma: no cover - iterations>0 already guaranteed
        permuted_drawdowns = [original_drawdown]

    return MonteCarloReport(
        seed=seed,
        iterations=iterations,
        trade_count=len(original),
        original_terminal_pnl=original_terminal,
        original_max_drawdown=original_drawdown,
        terminal_pnl_invariant=terminal_invariant,
        permuted_max_drawdowns=tuple(permuted_drawdowns),
        drawdown_min=min(permuted_drawdowns),
        drawdown_max=max(permuted_drawdowns),
        drawdown_mean=statistics.fmean(permuted_drawdowns),
        drawdown_median=statistics.median(permuted_drawdowns),
    )


=== FILE: research/oos_stability.py ===
"""
Part F — Rolling Out-of-Sample Stability.

TERMINOLOGY (Section F — do not overclaim): ConsensusEngine weights and
RiskOverlay thresholds are fixed by design in the accepted Phase 1-12
baseline (nothing is fit to any window). This module therefore measures
whether the EXISTING, UNCHANGED strategy behaves consistently across
different historical periods — it is Rolling OOS Stability /
Multi-Window OOS Evaluation, NOT walk-forward optimization (which would
imply fitting parameters per window and testing on the next). No
parameter is ever tuned here. A future genuine walk-forward optimizer
MAY reuse this window-splitting machinery, but only once a deliberately
approved tunable parameter surface is introduced elsewhere — not in this
pass (Section F: "No hyperopt now").

Each window gets a completely FRESH `RuntimeCoordinator`/
`PaperTradingEngine`/`FeatureHistoryStore` (via a fresh
`replay.HistoricalReplayDriver.run()` call per window — see that
module's class docstring) — no state, position, or feature history ever
leaks from one window into the next.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from crypto_signal_engine.domain._validation import require_utc_aware
from research.attribution import PerformanceMetrics, compute_report
from research.errors import ReplayConfigurationError
from research.replay import HistoricalReplayDriver, ReplayResult

ROLLING_OOS_LABEL = "Rolling OOS Stability (measurement of the existing fixed strategy — NOT walk-forward optimization)"


@dataclass(frozen=True)
class OOSWindow:
    index: int
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        require_utc_aware(self.start, "start")
        require_utc_aware(self.end, "end")
        if self.start >= self.end:
            raise ValueError("window start end'den küçük olmalı")
        if self.index < 0:
            raise ValueError("index negatif olamaz")


def build_sequential_windows(
    *, start: datetime, end: datetime, window_size: timedelta, step_size: timedelta
) -> tuple[OOSWindow, ...]:
    """Sequential, time-safe, NON-OVERLAPPING windows only — `step_size`
    must be `>= window_size` (enforced, not just documented), which
    guarantees windows are time-disjoint by construction rather than by
    convention. No hard-coded 70/30 (or any other) split ratio — both
    sizes are caller-configured."""
    require_utc_aware(start, "start")
    require_utc_aware(end, "end")
    if window_size <= timedelta(0):
        raise ReplayConfigurationError("window_size pozitif olmalı")
    if step_size <= timedelta(0):
        raise ReplayConfigurationError("step_size pozitif olmalı")
    if step_size < window_size:
        raise ReplayConfigurationError(
            "step_size, window_size'dan küçük olamaz (overlapping/non-disjoint pencereler "
            "bu fonksiyon tarafından KASITLI OLARAK reddedilir — bkz. modül docstring'i)"
        )
    if start >= end:
        raise ReplayConfigurationError("start end'den küçük olmalı")

    windows: list[OOSWindow] = []
    cursor = start
    index = 0
    while cursor + window_size <= end:
        windows.append(OOSWindow(index=index, start=cursor, end=cursor + window_size))
        cursor = cursor + step_size
        index += 1
    if not windows:
        raise ReplayConfigurationError(
            f"[{start}, {end}) aralığı tek bir {window_size} penceresi bile içermiyor"
        )
    return tuple(windows)


@dataclass(frozen=True)
class OOSWindowResult:
    window: OOSWindow
    replay_result: ReplayResult
    metrics: PerformanceMetrics


@dataclass(frozen=True)
class RollingOOSStabilityReport:
    windows: tuple[OOSWindowResult, ...]
    window_size: timedelta
    step_size: timedelta
    label: str = ROLLING_OOS_LABEL


class RollingOOSStabilityRunner:
    """Drives `replay.HistoricalReplayDriver` once per window. Every
    window is fully independent — see module docstring."""

    def __init__(self, *, driver: HistoricalReplayDriver) -> None:
        self._driver = driver

    async def run(
        self,
        *,
        symbols: tuple[str, ...],
        start: datetime,
        end: datetime,
        window_size: timedelta,
        step_size: timedelta,
    ) -> RollingOOSStabilityReport:
        windows = build_sequential_windows(start=start, end=end, window_size=window_size, step_size=step_size)
        results: list[OOSWindowResult] = []
        for window in windows:
            replay_result = await self._driver.run(symbols=symbols, start=window.start, end=window.end)
            report = compute_report(replay_result.cycle_results)
            results.append(OOSWindowResult(window=window, replay_result=replay_result, metrics=report.overall))
        return RollingOOSStabilityReport(windows=tuple(results), window_size=window_size, step_size=step_size)


=== FILE: research/orderbook_capture.py ===
"""
Part E — Future Real Order-Book Research Capture.

A SMALL, OPTIONAL, observational mechanism so the upcoming multi-week
24/7 PAPER run can accumulate REAL PUBLIC order-book evidence for a
future, higher-fidelity replay (closing the gap `replay.py` documents:
today's replay only has SYNTHETIC order-book evidence, see that
module's Section D). This module does NOT build a market-data platform:
it stores exactly the normalized, already-accepted Phase 3 order-book
FEATURE SNAPSHOT that Phase 4's `OrderBookAgent` actually consumes at
evaluation-relevant times — never raw depth/every packet.

PUBLIC data only; no credentials of any kind pass through or near this
module (it never touches `providers/binance/rest.py`'s or `execution/`'s
signed-request machinery — it only ever receives an already-computed,
already-accepted `crypto_signal_engine.features.domain.FeatureSnapshot`,
built entirely from Phase 2 PUBLIC market data).

FAILURE ISOLATION (hard requirement): `record()` NEVER raises. Any
storage failure (disk full, permissions, corrupt file) is caught,
classified, and returned as a `RecordOutcome` — it can never propagate
into, or alter, a `Signal`/`PaperTradingResult`. This is enforced twice:
once here (broad-but-classified exception handling, never a silent
`except Exception: pass` — every failure is captured as data and
returned, per this repository's own anti-silent-exception discipline),
and again structurally by the optional `RuntimeCoordinator.
order_book_observer` hook (see `runtime/coordinator.py`) which itself
wraps any observer call in a defensive try/except before it can ever
reach the coordinator's own control flow.

STORAGE GROWTH (documented, bounded): one row per (symbol, as_of,
feature_name) triple, written once per accepted order-book feature
commit. At a live cadence of roughly one order-book snapshot per minute
per symbol (matching `RuntimeCoordinator`'s existing order-book ingest
cadence), a single symbol over a multi-week run produces on the order of
a few thousand rows per feature per week — small, fixed-width floats,
no unbounded firehose. Operators should periodically archive/rotate this
SQLite file exactly as they already do for the accepted Phase 7
`PaperStateStore` checkpoint database (`scripts/backup_sqlite.py`'s
Online Backup API pattern applies equally here — raw `cp` against a live
SQLite file is unsafe for the same WAL-consistency reason documented
there).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol, require_utc_aware
from crypto_signal_engine.features.domain import FeatureSnapshot

_SCHEMA = """
CREATE TABLE IF NOT EXISTS order_book_evidence (
    symbol TEXT NOT NULL,
    as_of TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value REAL NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (symbol, as_of, feature_name)
);
"""


class RecordOutcome(str, Enum):
    RECORDED = "RECORDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class OrderBookEvidenceRecord:
    symbol: str
    as_of: datetime
    feature_values: Mapping[str, float]


class OrderBookEvidenceRecorder:
    """Durable-local (SQLite), append-mostly store for the exact Phase 3
    order-book-derived `FeatureSnapshot` values Phase 4 consumed at
    evaluation-relevant times. See module docstring for the failure-
    isolation and storage-growth design."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        try:
            with closing(sqlite3.connect(self._db_path)) as conn:
                conn.execute(_SCHEMA)
                conn.commit()
        except (sqlite3.Error, OSError) as exc:
            # Construction-time failure IS surfaced (there is no trading
            # state to protect yet — this mirrors the accepted
            # PaperStateStore's own fail-fast-at-construction posture).
            raise RuntimeError(f"OrderBookEvidenceRecorder schema init failed: {exc}") from exc

    def record(self, symbol: str, feature_snapshot: FeatureSnapshot) -> RecordOutcome:
        """NEVER raises — see module docstring "FAILURE ISOLATION"."""
        try:
            normalized = normalize_symbol(symbol)
            require_utc_aware(feature_snapshot.as_of, "feature_snapshot.as_of")
            recorded_at = datetime.now(timezone.utc).isoformat()
            with closing(sqlite3.connect(self._db_path)) as conn:
                with conn:
                    for name, value in feature_snapshot.values.items():
                        conn.execute(
                            "INSERT OR REPLACE INTO order_book_evidence "
                            "(symbol, as_of, feature_name, feature_value, recorded_at) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (normalized, feature_snapshot.as_of.isoformat(), name, float(value), recorded_at),
                        )
            return RecordOutcome.RECORDED
        except Exception:
            # Deliberately broad — this is exactly the "lifecycle
            # boundary with explicit behaviour" case `crypto_signal_
            # engine/errors.py`'s own module docstring carves out as the
            # one legitimate use of a broad except (never a bare/blind
            # `except Exception: pass` — this branch returns a classified
            # `RecordOutcome`, it does not silently swallow). An earlier
            # revision enumerated specific exception types instead
            # (`sqlite3.Error`/`OSError`/`ValueError`/`TypeError`) and
            # missed `AttributeError` from `require_utc_aware` touching
            # `.tzinfo` on a non-datetime `as_of` — proving that trying to
            # enumerate every way a malformed, externally-supplied
            # `feature_snapshot` can fail is inherently incomplete. This
            # method's ONE contractual promise (module docstring,
            # "FAILURE ISOLATION") is "never raises, for any input" — a
            # promise only a genuinely broad catch can keep.
            return RecordOutcome.FAILED

    def read_all(self, symbol: str) -> tuple[OrderBookEvidenceRecord, ...]:
        """Read-only; groups rows back into per-`as_of` snapshots. Used by
        a future higher-fidelity replay (Section D/E composition) —
        NOT used by this pass's `replay.py`, which remains SYNTHETIC-only
        today (see that module's Section D)."""
        normalized = normalize_symbol(symbol)
        grouped: dict[datetime, dict[str, float]] = {}
        with closing(sqlite3.connect(self._db_path)) as conn:
            rows = conn.execute(
                "SELECT as_of, feature_name, feature_value FROM order_book_evidence "
                "WHERE symbol = ? ORDER BY as_of ASC",
                (normalized,),
            ).fetchall()
        for as_of_text, feature_name, feature_value in rows:
            as_of = datetime.fromisoformat(as_of_text)
            grouped.setdefault(as_of, {})[feature_name] = feature_value
        return tuple(
            OrderBookEvidenceRecord(symbol=normalized, as_of=as_of, feature_values=values)
            for as_of, values in sorted(grouped.items())
        )

    def row_count(self) -> int:
        """For storage-growth monitoring — a cheap, explicit way to
        observe the bounded-growth claim above rather than trusting it
        blindly."""
        with closing(sqlite3.connect(self._db_path)) as conn:
            (count,) = conn.execute("SELECT COUNT(*) FROM order_book_evidence").fetchone()
        return int(count)


=== FILE: research/sizing.py ===
"""
Part I/J — Portfolio / Risk-Based PAPER Sizing.

Data flow (Section I, exactly as specified):

    completed Signal
         |
         v
    Portfolio/Risk Sizing Policy   (this module — pure, deterministic)
         |
         v
    PaperTradingEngine             (Phase 5, accepted, additively extended
                                     with `notional_override` — see
                                     `paper_trading/engine.py`)

NOT: agents -> sizing, consensus -> sizing mutation, RiskOverlay ->
capital allocation mutation. This module NEVER imports `agents/`,
`consensus/`, or mutates any `ConsensusResult`/`RiskAssessment`/`Signal`
field. It reads exactly two already-finalized `Signal` fields —
`confidence` and `risk_level` — both of which are frozen by the time
`SignalEngine.evaluate()` returns (see `signal_engine.py`); it never
reinterprets `Signal.score` as a sizing input (Section J: "Do NOT
secretly reinterpret Signal.score").

`PortfolioRiskSizingPolicy.decide()` is a pure function: given a
`Signal`, a `guardrails.PortfolioSnapshot`, and a `base_notional`, it
returns an immutable `SizingDecision` — ALLOWED / CAPPED / DENIED, with
an explicit machine-readable reason. It never calls `PaperTradingEngine`
itself.

`SizedPaperTradingEngine` is the thin, ADDITIVE composition wrapper that
makes the above usable end-to-end through the accepted, UNMODIFIED
`RuntimeCoordinator` (which already accepts any object exposing
`process_signal(signal, price) -> PaperTradingResult` as its
`paper_engine` — a duck-typed, existing, optional constructor
parameter; see `runtime/coordinator.py`, no coordinator changes were
needed or made). See the class docstring below for the full idempotency
and denial-handling design, and PRE_AUDIT_ENHANCEMENTS.md for the
documented limitation this wrapper deliberately does NOT solve
(denial cannot suppress a directional trade at the RuntimeCoordinator
level without a deeper Phase 5 "skip this Signal" contract that does
not exist today — see that document's "Known limitations" section).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.enums import SignalDirection
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import (
    MarketPriceSnapshot,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)
from research.errors import SizingPolicyError
from research.guardrails import GuardrailStatus, PortfolioSnapshot, RiskBudgetConfig, evaluate_guardrails


class SizingOutcome(str, Enum):
    ALLOWED = "ALLOWED"
    CAPPED = "CAPPED"
    DENIED = "DENIED"


@dataclass(frozen=True)
class SizingDecision:
    """Small, immutable decision model (Section J)."""

    symbol: str
    context_id: str
    outcome: SizingOutcome
    requested_notional: float
    notional: float | None  # None iff outcome is DENIED
    reason: str

    def __post_init__(self) -> None:
        if self.outcome is SizingOutcome.DENIED and self.notional is not None:
            raise ValueError("DENIED kararında notional None olmalı")
        if self.outcome is not SizingOutcome.DENIED and self.notional is None:
            raise ValueError(f"{self.outcome} kararında notional None OLAMAZ")
        if self.notional is not None and self.notional <= 0:
            raise ValueError("notional pozitif olmalı")


class PortfolioRiskSizingPolicy:
    """Pure per-signal notional decision. Composes `guardrails.
    evaluate_guardrails()` (portfolio-wide caps/circuit-breaker) with a
    per-signal confidence/risk_level scaling of `base_notional`."""

    def __init__(self, config: RiskBudgetConfig) -> None:
        self._config = config

    def decide(self, *, signal: Signal, snapshot: PortfolioSnapshot, base_notional: float) -> SizingDecision:
        if signal.direction is SignalDirection.NEUTRAL:
            raise SizingPolicyError(
                "sizing policy must not be invoked for a NEUTRAL signal — NEUTRAL is "
                "NO_ACTION at the Phase 5 layer and requests no position change"
            )
        require_finite(base_notional, "base_notional")
        if base_notional <= 0:
            raise SizingPolicyError("base_notional pozitif olmalı (fail-closed)")

        confidence_multiplier = max(signal.confidence, self._config.confidence_multiplier_floor)
        risk_multiplier = self._config.risk_level_multipliers[signal.risk_level.value]
        requested = base_notional * confidence_multiplier * risk_multiplier

        current_position = snapshot.positions.get(signal.symbol)
        is_new_symbol_exposure = current_position is None or current_position.side is PositionSide.FLAT

        status: GuardrailStatus = evaluate_guardrails(
            symbol=signal.symbol,
            is_new_symbol_exposure=is_new_symbol_exposure,
            snapshot=snapshot,
            config=self._config,
        )

        if status.halted:
            reason = "guardrail breach: " + ", ".join(b.value for b in status.breaches)
            return SizingDecision(
                symbol=signal.symbol, context_id=signal.context_id, outcome=SizingOutcome.DENIED,
                requested_notional=requested, notional=None, reason=reason,
            )

        capped = min(requested, status.available_symbol_notional)
        if capped <= 0:
            return SizingDecision(
                symbol=signal.symbol, context_id=signal.context_id, outcome=SizingOutcome.DENIED,
                requested_notional=requested, notional=None,
                reason="no remaining risk budget for this symbol (per-symbol/gross cap exhausted)",
            )

        if capped < requested:
            reason = (
                f"capped from requested {requested:.2f} to {capped:.2f} by the "
                f"available per-symbol/gross portfolio risk budget "
                f"(max_per_symbol_notional={self._config.max_per_symbol_notional:.2f})"
            )
            return SizingDecision(
                symbol=signal.symbol, context_id=signal.context_id, outcome=SizingOutcome.CAPPED,
                requested_notional=requested, notional=capped, reason=reason,
            )

        return SizingDecision(
            symbol=signal.symbol, context_id=signal.context_id, outcome=SizingOutcome.ALLOWED,
            requested_notional=requested, notional=capped,
            reason=(
                f"allowed at requested notional (confidence={signal.confidence:.3f} x "
                f"{confidence_multiplier:.3f} floor-applied, risk_level={signal.risk_level.value} x "
                f"{risk_multiplier:.3f})"
            ),
        )


class SizedPaperTradingEngine:
    """Additive composition wrapper: `PortfolioRiskSizingPolicy` +
    `PaperTradingEngine`, exposing the exact `process_signal(signal,
    price) -> PaperTradingResult` shape `RuntimeCoordinator` already
    calls — a plain, duck-typed drop-in for its `paper_engine`
    constructor parameter (no coordinator changes needed; Python does
    not isinstance-check that parameter).

    Idempotency (Section P, test 36 — "idempotent replay cannot allocate
    twice"): for an ALLOWED/CAPPED decision, this wrapper delegates to
    `inner.process_signal(signal, price, notional_override=...)`. The
    inner, ACCEPTED engine's own idempotency check runs FIRST, before
    `notional_override` is ever consulted (see `paper_trading/
    engine.py::process_signal`) — so a genuine replay of an
    already-processed `(symbol, context_id)` returns the inner engine's
    cached result UNCHANGED, regardless of what this call's fresh
    sizing computation produced. This wrapper additionally short-
    circuits via `inner.has_processed()` BEFORE running sizing at all,
    so a known replay never perturbs the sizing policy's own state
    (e.g. a would-be different guardrail reading from current portfolio
    state) purely for a no-op call.

    DENIAL (documented, honest limitation — see PRE_AUDIT_ENHANCEMENTS.md):
    a DENIED decision is realized as a synthesized NO_ACTION-shaped
    `PaperTradingResult` (no orders, no fills, position unchanged) —
    the exact same shape `PaperTradingEngine._no_action_result` already
    produces for NEUTRAL signals. The inner engine's state is NEVER
    touched for a denial: no order/fill is created, and the context_id
    is NOT recorded in the inner engine's idempotency ledger. This means
    a denied signal is NOT idempotency-protected against being
    re-evaluated later with a different guardrail outcome — this is
    intentional and safe (denial never allocates, so re-evaluating it
    cannot double-allocate) but means a DENIED decision is a
    point-in-time portfolio-capacity judgement, not a permanent fact
    recorded against the signal. What this wrapper deliberately does
    NOT do: force-close/flatten an existing position on denial (no
    panic-flatten, per Section K), and it does NOT attempt to suppress
    a directional trade via any RuntimeCoordinator-level mechanism
    (none exists without a deeper Phase 5/6 contract change — see the
    blocker note in PRE_AUDIT_ENHANCEMENTS.md).
    """

    def __init__(
        self,
        *,
        inner: PaperTradingEngine,
        policy: PortfolioRiskSizingPolicy,
        base_notional: float,
        symbols: tuple[str, ...],
    ) -> None:
        require_finite(base_notional, "base_notional")
        if base_notional <= 0:
            raise SizingPolicyError("base_notional pozitif olmalı")
        self._inner = inner
        self._policy = policy
        self._base_notional = base_notional
        self._symbols = tuple(symbols)
        self._decisions: list[SizingDecision] = []

    @property
    def decisions(self) -> tuple[SizingDecision, ...]:
        """Full, append-only audit trail of every sizing decision made so
        far — for attribution/reporting (Section G)."""
        return tuple(self._decisions)

    def _current_snapshot(self) -> PortfolioSnapshot:
        positions: dict[str, PaperPosition] = {}
        total_realized = 0.0
        for symbol in self._symbols:
            position = self._inner.position(symbol)
            if position is not None:
                positions[symbol] = position
                total_realized += position.realized_pnl
        return PortfolioSnapshot(positions=positions, aggregate_realized_pnl=total_realized)

    def _denied_no_action_result(self, signal: Signal) -> PaperTradingResult:
        current = self._inner.position(signal.symbol)
        position = current if current is not None else PaperPosition(
            symbol=signal.symbol, side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=0.0, updated_at=signal.timestamp,
        )
        return PaperTradingResult(symbol=signal.symbol, position=position, orders=(), fills=(), idempotent_replay=False)

    def process_signal(self, signal: Signal, price: MarketPriceSnapshot) -> PaperTradingResult:
        if signal.direction is SignalDirection.NEUTRAL:
            return self._inner.process_signal(signal, price)

        if self._inner.has_processed(signal.symbol, signal.context_id):
            # Known replay (or a signal this exact wrapper already denied
            # and the caller is re-presenting): delegate straight through.
            # If the inner engine already recorded a real trade, its own
            # idempotency returns that cached result untouched, with no
            # sizing recomputation. If it was previously DENIED (never
            # recorded by the inner engine), `has_processed` is False and
            # we fall through to re-run sizing below — see class
            # docstring "DENIAL".
            return self._inner.process_signal(signal, price)

        snapshot = self._current_snapshot()
        decision = self._policy.decide(signal=signal, snapshot=snapshot, base_notional=self._base_notional)
        self._decisions.append(decision)

        if decision.outcome is SizingOutcome.DENIED:
            return self._denied_no_action_result(signal)

        assert decision.notional is not None
        return self._inner.process_signal(signal, price, notional_override=decision.notional)

    # -- Read-through accessors (duck-typed parity with PaperTradingEngine) --

    def position(self, symbol: str) -> PaperPosition | None:
        return self._inner.position(symbol)

    def orders(self, symbol: str):
        return self._inner.orders(symbol)

    def fills(self, symbol: str):
        return self._inner.fills(symbol)

    def unrealized_pnl(self, symbol: str, mark_price: MarketPriceSnapshot) -> float:
        return self._inner.unrealized_pnl(symbol, mark_price)


=== FILE: scripts/adaptive_evaluation_cycle.py ===
#!/usr/bin/env python3
"""
Adaptive Intelligence v1, step 13 — manual, on-demand evaluation-cycle
CLI. Follows `scripts/historical_replay.py`'s exact pattern (public-only
Binance REST via the accepted, unmodified `BinanceRestClient`, no
credentials of any kind, prints a plain stdout report, never claims
"ready for live trading").

Calls the EXACT SAME `adaptive.cycle.run_adaptive_cycle()` function
`adaptive.scheduler.AdaptiveScheduler`'s background task calls — no
duplicated evaluation logic between the automatic and manual paths.

This script imports ONLY `adaptive/` + already-accepted, generic
`crypto_signal_engine.providers.binance.*` infrastructure — it does NOT
wire a real champion into the LIVE trading system (that is `scripts/
run_with_adaptive_policy.py`'s job, step 14). This script is for
on-demand/testing runs against `adaptive/`'s own dedicated database only.

Usage:
    python3 scripts/adaptive_evaluation_cycle.py --symbol BTCUSDT \\
        --db var/adaptive.db --anchor 2026-01-01T00:00:00+00:00 \\
        --now 2026-02-01T00:00:00+00:00

Requires network access to https://api.binance.com (PUBLIC endpoints
only). No credentials of any kind are read, requested, or required."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

from adaptive.cycle import run_adaptive_cycle
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from research.errors import ResearchError


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı (örn. 2026-01-01T00:00:00+00:00)")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    config = BinanceConfig(supported_symbols=tuple(args.symbol))
    rest_client = BinanceRestClient(
        config=config, http_client=UrllibHttpClient(), clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    store = AdaptiveStore(args.db)
    now = args.now or datetime.now(tz=args.anchor.tzinfo)
    window_config = WindowConfig(
        discovery_window_size=timedelta(hours=args.discovery_hours),
        confirmation_window_size=timedelta(hours=args.confirmation_hours),
    )

    print("=" * 72)
    print("ADAPTIVE EVALUATION CYCLE — Adaptive Intelligence v1 (manual CLI)")
    print("=" * 72)
    print(f"SYMBOLS:               {', '.join(args.symbol)}")
    print(f"DB:                    {args.db}")
    print(f"ANCHOR:                {args.anchor.isoformat()}")
    print(f"NOW:                   {now.isoformat()}")
    print(f"DISCOVERY WINDOW:      {args.discovery_hours}h")
    print(f"CONFIRMATION WINDOW:   {args.confirmation_hours}h")

    try:
        report = await run_adaptive_cycle(
            store=store, candle_source=rest_client, symbols=tuple(args.symbol), window_config=window_config,
            anchor=args.anchor, seed=args.seed, now=now,
        )
    except ResearchError as exc:
        print(f"CYCLE ERROR:           {exc}")
        store.close()
        return 3

    print("-" * 72)
    print(f"CHAMPION (after cycle): {report.champion_version_id}")
    print(f"CHALLENGERS GENERATED:  {len(report.challengers_generated)}")
    if report.windows is None:
        print("WINDOWS:                confirmation window not yet elapsed -- discovery/confirmation skipped this cycle")
    else:
        print(f"DISCOVERY WINDOW:       {report.windows.discovery.start.isoformat()} -> {report.windows.discovery.end.isoformat()}")
        print(f"CONFIRMATION WINDOW:    {report.windows.confirmation.start.isoformat()} -> {report.windows.confirmation.end.isoformat()}")
    print(f"DISCOVERY SURVIVORS:    {len(report.discovery_survivors)}")
    print(f"CONFIRMATION SURVIVORS: {len(report.confirmation_survivors)}")
    print(f"NEWLY SHADOW-ELIGIBLE:  {list(report.newly_shadow_eligible)}")
    print(f"DECISIONS THIS CYCLE:   {len(report.decisions)}")
    for decision in report.decisions:
        print(f"    - {decision.challenger_version_id}: {decision.decision.value} -- {decision.reason}")
    print(f"PROMOTED TO:            {report.promoted_to or '(no promotion this cycle)'}")

    print("=" * 72)
    print("This cycle ran against adaptive/'s OWN dedicated database — it did")
    print("NOT change any live Testnet/PAPER position or any crypto_signal_engine")
    print("persisted state. Wiring a champion into the live system is done by")
    print("scripts/run_with_adaptive_policy.py, never by this script.")
    print("=" * 72)

    store.close()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", required=True, help="repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT")
    parser.add_argument("--db", required=True, help="path to adaptive/'s own dedicated SQLite database file")
    parser.add_argument("--anchor", type=_parse_utc, required=True, help="the very first confirmation window's start")
    parser.add_argument("--now", type=_parse_utc, default=None, help="defaults to real wall-clock now")
    parser.add_argument("--discovery-hours", type=float, default=720.0, help="discovery window size in hours (default 30 days)")
    parser.add_argument("--confirmation-hours", type=float, default=168.0, help="confirmation window size in hours (default 7 days)")
    parser.add_argument("--seed", type=int, default=0, help="challenger-generation seed (deterministic for a given seed)")
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/backup_sqlite.py ===
#!/usr/bin/env python3
"""
Faz 8 — SQLite durable state için GÜVENLİ online backup yardımcı script'i.

Neden ham dosya kopyalama (`cp`) KULLANILMAZ: aktif olarak yazılan bir
SQLite dosyasını `cp` ile kopyalamak, WAL/journal dosyalarıyla tutarsız,
bozuk bir anlık görüntü üretebilir. Bunun yerine SQLite'ın kendi resmi
"Online Backup API"si (`sqlite3.Connection.backup()`, CPython stdlib'de
mevcut) kullanılır — bu, kaynak veritabanı AÇIK ve YAZILIYOR olsa bile
tutarlı bir kopya garanti eder.

Kullanım:
    python3 scripts/backup_sqlite.py --source /var/lib/crypto-signal-engine/paper_state.db \\
        --destination /var/backups/crypto-signal-engine/paper_state-2026-01-01.db

24/7 Ops v1 — otomatik zamanlama + retention (additive, opsiyonel):
`deploy/systemd/crypto-signal-engine-backup.timer`/`.service`, bu script'i
DEĞİŞTİRMEDEN, günlük olarak çalıştırır (bkz. PHASE8_UBUNTU_OPERATIONS.md
Bölüm 9). `--keep-last N` verilirse (varsayılan `None` = eski davranış,
DEĞİŞMEDEN), backup BAŞARILI olduktan SONRA `--destination`'ın bulunduğu
dizinde AYNI kaynağa ait ("{source.stem}*{destination.suffix}" glob'una
uyan) dosyalar mtime'a göre sıralanır ve en yeni N tanesi DIŞINDAKİLER
silinir — böylece yedekler sınırsız BÜYÜMEZ. Silinecek dosyaları seçen
`select_backups_to_delete()` SAF bir fonksiyondur (gerçek dosya sistemi
I/O'su YAPMAZ) — bkz. tests/test_backup_sqlite.py.

UI Polish v1 — backup status görünürlüğü (additive): HER başarılı backup
sonrası, `write_backup_status()` `source.parent/backup_status.json`'a
(`completed_at`/`destination`/`size_bytes`) ATOMİK olarak yazar (aynı
temp-file + `os.replace` deseni, bkz. `ops/health_snapshot.py`). Bu
dosyanın YAZILAMAMASI backup'ın KENDİSİNİ asla başarısız hâle GETİRMEZ —
yalnızca bir uyarı yazdırılır (exit code hâlâ `0`). `crypto_signal_
engine/ops/dashboard.py`, bu dosyayı `health.json` ile AYNI salt-okunur
desende okur (bkz. `app.py::Application._backup_snapshot`).

Restore prosedürü (bkz. PHASE8_UBUNTU_OPERATIONS.md — "Restore"):
    1. `systemctl stop crypto-signal-engine` (servisi DURDUR — SQLite dosyası
       çalışırken DEĞİŞTİRİLMEMELİ).
    2. Mevcut (potansiyel olarak bozuk) dosyayı YOK ETMEDEN kenara taşı:
       `mv paper_state.db paper_state.db.before-restore`.
    3. Yedeği hedef konuma kopyala: `cp backup.db paper_state.db`.
    4. `systemctl start crypto-signal-engine` ile başlat, ardından
       `python -m crypto_signal_engine.app status` VE
       `journalctl -u crypto-signal-engine -n 100` ile recovery'nin
       BAŞARILI olduğunu doğrula.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path


def backup_sqlite_database(source: Path, destination: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"kaynak veritabanı bulunamadı: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(str(source))
    try:
        destination_conn = sqlite3.connect(str(destination))
        try:
            source_conn.backup(destination_conn)
        finally:
            destination_conn.close()
    finally:
        source_conn.close()


def select_backups_to_delete(backups: Sequence[tuple[str, float]], *, keep_last: int) -> list[str]:
    """SAF retention-seçim fonksiyonu — gerçek dosya sistemine ASLA
    dokunmaz (girdi: `(dosya_adı, mtime)` çiftleri; çıktı: silinecek dosya
    adları). En YENİ `keep_last` dosya TUTULUR, geri kalanı silinmek üzere
    döner (mtime'a göre en yeniden en eskiye sıralanır — eşit mtime'larda
    girdi sırası korunur, `sorted()`'ın kararlılığı sayesinde).

    `keep_last <= 0` KASITLI OLARAK "hiçbir şey silme" anlamına gelir
    (fail-safe varsayılan — bu codebase'deki her opsiyonel bayrağın AYNI
    disiplini: bir yanlış konfigürasyon SESSİZCE TÜM yedekleri silmemeli,
    bir backup aracı için mümkün olan EN KÖTÜ başarısızlık modu budur)."""
    if keep_last <= 0:
        return []
    ordered = sorted(backups, key=lambda item: item[1], reverse=True)
    return [name for name, _mtime in ordered[keep_last:]]


def write_backup_status(*, source: Path, destination: Path, completed_at: datetime) -> None:
    """UI Polish v1, Step A7 — additive to this script's core logic
    (never touches the actual `sqlite3.Connection.backup()` call above).
    Written next to the SOURCE db (`source.parent / "backup_status.
    json"`) — the SAME directory `AppConfig.db_path.parent` already
    resolves to, so `app.py::Application._backup_snapshot()` can find it
    with no new config surface. Atomic (temp file + `os.replace`), the
    EXACT SAME pattern as `ops/health_snapshot.py::write_snapshot()` —
    a reader never sees a partially-written file."""
    status_path = source.parent / "backup_status.json"
    payload = {
        "completed_at": completed_at.isoformat(),
        "destination": str(destination),
        "size_bytes": destination.stat().st_size,
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(status_path.parent), prefix=".backup-status-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_name, status_path)
    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


def apply_retention(destination: Path, *, source: Path, keep_last: int) -> list[str]:
    """`select_backups_to_delete()`'in gerçek dosya sistemi sarmalayıcısı
    — AYNI kaynağa ait ("{source.stem}*{destination.suffix}" glob'una
    uyan, `destination.parent` içindeki) dosyaları listeler, saf
    fonksiyona verir, ve döneni SİLER. Bir dosya silinemezse (örn. izin
    hatası) o dosya ATLANIR ve diğerleri denenmeye devam eder — tek bir
    silme hatası TÜM retention adımını BAŞARISIZ kılmaz (backup'ın
    KENDİSİ zaten başarıyla tamamlandı, bu yalnızca temizlik)."""
    family_glob = f"{source.stem}*{destination.suffix}"
    candidates = [(p.name, p.stat().st_mtime) for p in destination.parent.glob(family_glob) if p.is_file()]
    to_delete = select_backups_to_delete(candidates, keep_last=keep_last)
    deleted: list[str] = []
    for name in to_delete:
        target = destination.parent / name
        try:
            target.unlink()
        except OSError as exc:
            print(f"RETENTION WARNING: {target} silinemedi: {exc}", file=sys.stderr)
            continue
        deleted.append(name)
    return deleted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, type=Path, help="Aktif durable SQLite dosyasının yolu")
    parser.add_argument("--destination", required=True, type=Path, help="Yedek dosyasının yazılacağı yol")
    parser.add_argument(
        "--keep-last", type=int, default=None,
        help=(
            "Verilirse, backup başarılı olduktan SONRA aynı kaynağa ait en yeni N yedek "
            "DIŞINDAKİLER silinir. Varsayılan None = eski davranış, DEĞİŞMEDEN (hiçbir "
            "dosya silinmez)."
        ),
    )
    args = parser.parse_args(argv)

    try:
        backup_sqlite_database(args.source, args.destination)
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"BACKUP FAILED: {exc}", file=sys.stderr)
        return 1

    print(f"OK: {args.source} -> {args.destination}")

    try:
        write_backup_status(source=args.source, destination=args.destination, completed_at=datetime.now(timezone.utc))
    except OSError as exc:
        # The backup ITSELF already succeeded above — a failure to write
        # this purely-additive status file must never turn a successful
        # backup into a reported failure (same "observability must never
        # break the underlying operation" discipline as everywhere else
        # in this codebase).
        print(f"BACKUP STATUS WARNING: backup_status.json yazılamadı: {exc}", file=sys.stderr)

    if args.keep_last is not None:
        deleted = apply_retention(args.destination, source=args.source, keep_last=args.keep_last)
        print(f"RETENTION: keep-last={args.keep_last}, silinen={len(deleted)}: {deleted}")

    return 0


if __name__ == "__main__":
    sys.exit(main())


=== FILE: scripts/binance_testnet_lab.py ===
#!/usr/bin/env python3
"""
Faz 10/11 — Binance Spot TESTNET Execution Lab CLI.

GÜVENLİK (bkz. crypto_signal_engine/execution/ — SAFETY_INVARIANTS.md):
- Bu script YALNIZCA `https://testnet.binance.vision`'a bağlanır. Mainnet
  HİÇBİR ZAMAN bir seçenek DEĞİLDİR — host allowlist kod seviyesinde
  sabit-kodlanmıştır (`BinanceTestnetConfig`/`validate_testnet_host`), bu
  script'te DEĞİŞTİRİLEBİLİR bir `--base-url`/host bayrağı YOKTUR.
- Kimlik bilgileri YALNIZCA ortam değişkenlerinden okunur:
      BINANCE_TESTNET_API_KEY
      BINANCE_TESTNET_API_SECRET
  Bu script kimlik bilgisi İSTEMEZ/YARATMAZ; eksikse, yalnızca kimlik
  gerektiren komutlar (account-check, --confirm-testnet-order ile
  gönderim, reconcile*) temiz bir hata ile başarısız olur.
- Secret/signature HİÇBİR ZAMAN yazdırılmaz/persist EDİLMEZ.
- Order gönderimi, KASITLI olarak `--confirm-testnet-order` bayrağı
  OLMADAN gerçekleşmez — bayraksız çağrı yalnızca doğrulama/dry-run yapar.
- `ALLOW_LIVE_TRADING` bu script tarafından OKUNMAZ/DEĞİŞTİRİLMEZ; `False`
  kalır (bu script zaten Mainnet'e hiç dokunamaz).
- (Faz 11) `--confirm-testnet-order` ile gerçek gönderim,
  `ExecutionReconciliationService` üzerinden geçer — aynı `context_id`'nin
  TEKRAR gönderilmesi asla ikinci bir order ÜRETMEZ (bkz. modülün
  `reconciliation_service.py` docstring'i).

Bu script normal pytest suite'inin PARÇASI DEĞİLDİR (yalnızca manuel
kullanım için) — ama `run_cli_async()` fonksiyonu, `tests/test_execution_cli.py`
tarafından sahte bir HTTP client + geçici SQLite enjekte edilerek TAMAMEN
offline test edilir.

Kullanım:
    python3 scripts/binance_testnet_lab.py account-check
    python3 scripts/binance_testnet_lab.py validate-symbol BTCUSDT
    python3 scripts/binance_testnet_lab.py place-market --symbol BTCUSDT --side BUY --quantity 0.001
    python3 scripts/binance_testnet_lab.py place-market --symbol BTCUSDT --side BUY --quantity 0.001 --confirm-testnet-order
    python3 scripts/binance_testnet_lab.py place-limit --symbol BTCUSDT --side BUY --quantity 0.001 --price 50000
    python3 scripts/binance_testnet_lab.py order-status --context-id lab-abc123
    python3 scripts/binance_testnet_lab.py reconcile --client-order-id csl-abc123
    python3 scripts/binance_testnet_lab.py reconcile-pending
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.execution.adapter import TestnetExecutionAdapter
from crypto_signal_engine.execution.errors import ExecutionError
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import (
    BinanceTestnetClient,
    BinanceTestnetConfig,
    TestnetHttpClient,
    UrllibTestnetHttpClient,
)
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock

EXIT_OK = 0
EXIT_ERROR = 1

_DEFAULT_EXECUTION_DB_PATH = "var/lib/crypto-signal-engine/testnet_execution.db"


def _build_config() -> BinanceTestnetConfig:
    return BinanceTestnetConfig(
        api_key=os.environ.get("BINANCE_TESTNET_API_KEY") or None,
        api_secret=os.environ.get("BINANCE_TESTNET_API_SECRET") or None,
    )


def _execution_db_path() -> Path:
    return Path(os.environ.get("BINANCE_TESTNET_EXECUTION_DB_PATH") or _DEFAULT_EXECUTION_DB_PATH)


def _format_record(record) -> str:
    return (
        f"context_id={record.context_id} client_order_id={record.client_order_id} "
        f"symbol={record.symbol} side={record.side.value} type={record.order_type.value} "
        f"state={record.lifecycle_state} exchange_order_id={record.exchange_order_id} "
        f"executed_quantity={record.executed_quantity} "
        f"cumulative_quote_quantity={record.cumulative_quote_quantity} "
        f"updated_at={record.updated_at.isoformat()}"
    )


async def _cmd_account_check(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    account = await client.account_info()
    print(f"accountType={account.get('accountType')} canTrade={account.get('canTrade')}")
    balances = [
        b for b in account.get("balances", [])
        if float(b.get("free", 0)) > 0 or float(b.get("locked", 0)) > 0
    ]
    for balance in balances[:20]:
        print(f"  {balance['asset']:<8} free={balance['free']} locked={balance['locked']}")
    if not balances:
        print("  (sıfır olmayan bakiye yok)")
    return EXIT_OK


async def _cmd_validate_symbol(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    adapter = TestnetExecutionAdapter(client)
    filters = await adapter.validate_symbol(args.symbol)
    print(f"symbol={filters.symbol} status={filters.status}")
    print(f"  LOT_SIZE: min={filters.min_qty} max={filters.max_qty} step={filters.step_size}")
    if filters.market_step_size is not None:
        print(f"  MARKET_LOT_SIZE: min={filters.market_min_qty} max={filters.market_max_qty} step={filters.market_step_size}")
    if filters.tick_size is not None:
        print(f"  PRICE_FILTER: min={filters.min_price} max={filters.max_price} tick={filters.tick_size}")
    if filters.min_notional is not None:
        print(f"  MIN/NOTIONAL: min={filters.min_notional} applyMinToMarket={filters.min_notional_applies_to_market}")
    if filters.max_notional is not None:
        print(f"  NOTIONAL: max={filters.max_notional} applyMaxToMarket={filters.max_notional_applies_to_market}")
    return EXIT_OK


def _build_intent(args: argparse.Namespace, order_type: OrderType, clock: Clock) -> OrderIntent:
    context_id = args.context_id or f"lab-{uuid.uuid4().hex[:16]}"
    kwargs: dict[str, object] = dict(
        symbol=args.symbol, side=OrderSide(args.side), order_type=order_type,
        context_id=context_id, timestamp=clock.now(),
    )
    if args.quantity is not None:
        kwargs["quantity"] = args.quantity
    if getattr(args, "quote_quantity", None) is not None:
        kwargs["quote_quantity"] = args.quote_quantity
    if order_type is OrderType.LIMIT:
        kwargs["price"] = args.price
    return OrderIntent(**kwargs)  # type: ignore[arg-type]


async def _place_order(
    config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace, order_type: OrderType, clock: Clock
) -> int:
    intent = _build_intent(args, order_type, clock)
    adapter = TestnetExecutionAdapter(client)

    # `validate_intent()`, GEREKİYORSA (bkz. `determine_market_price_requirement`)
    # GÜNCEL bir TESTNET PUBLIC fiyatı da çeker — bu yüzden dry-run çıktısı
    # bile MARKET order'ların notional doğrulamasını DOĞRU yansıtır.
    _filters, market_price = await adapter.validate_intent(intent)
    price_note = f" (market_price={market_price} kullanılarak doğrulandı)" if market_price is not None else ""

    print(
        f"Validated intent: symbol={intent.symbol} side={intent.side.value} type={intent.order_type.value} "
        f"quantity={intent.quantity} quote_quantity={intent.quote_quantity} price={intent.price} "
        f"client_order_id={intent.client_order_id}{price_note}"
    )

    if not args.confirm_testnet_order:
        print("DRY-RUN ONLY — pass --confirm-testnet-order to actually submit to TESTNET. No order was sent.")
        return EXIT_OK

    # Faz 11: GERÇEK gönderim HER ZAMAN ExecutionReconciliationService
    # üzerinden geçer — stabil client_order_id + durable state + ambiguous-
    # timeout/duplicate-suppression garantisi (bkz. modül docstring'i).
    db_path = _execution_db_path()
    store = ExecutionStateStore(db_path)
    try:
        service = ExecutionReconciliationService(client, store, clock=clock)
        record = await service.submit(intent)
    finally:
        store.close()

    print(f"RESULT: {_format_record(record)}")
    return EXIT_OK


async def _cmd_place_market(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    return await _place_order(config, client, args, OrderType.MARKET, SystemClock())


async def _cmd_place_limit(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    return await _place_order(config, client, args, OrderType.LIMIT, SystemClock())


def _load_local_record(args: argparse.Namespace):
    store = ExecutionStateStore(_execution_db_path())
    try:
        if args.context_id is not None:
            return store.load_by_context_id(args.context_id)
        return store.load_by_client_order_id(args.client_order_id)
    finally:
        store.close()


async def _cmd_order_status(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    """YALNIZCA yerel durable kaydı okur — HİÇBİR ağ çağrısı YAPMAZ (güncel
    exchange truth'u için `reconcile`/`reconcile-pending` kullanın)."""
    record = _load_local_record(args)
    if record is None:
        print("NO LOCAL RECORD FOUND for the given identity.")
        return EXIT_ERROR
    print(_format_record(record))
    return EXIT_OK


async def _cmd_reconcile(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    store = ExecutionStateStore(_execution_db_path())
    try:
        service = ExecutionReconciliationService(client, store, clock=SystemClock())
        record = await service.reconcile(client_order_id=args.client_order_id, context_id=args.context_id)
    finally:
        store.close()
    print(_format_record(record))
    return EXIT_OK


async def _cmd_reconcile_pending(config: BinanceTestnetConfig, client: BinanceTestnetClient, args: argparse.Namespace) -> int:
    store = ExecutionStateStore(_execution_db_path())
    try:
        service = ExecutionReconciliationService(client, store, clock=SystemClock())
        records = await service.reconcile_pending()
    finally:
        store.close()
    if not records:
        print("No pending execution records required reconciliation.")
        return EXIT_OK
    for record in records:
        print(_format_record(record))
    return EXIT_OK


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="binance_testnet_lab.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("account-check", help="TESTNET hesap bilgisini gösterir (kimlik bilgisi GEREKTİRİR)")

    p_validate = subparsers.add_parser(
        "validate-symbol", help="Bir sembolün TESTNET exchange filtrelerini gösterir (kimlik bilgisi GEREKMEZ)"
    )
    p_validate.add_argument("symbol")

    p_market = subparsers.add_parser("place-market", help="TESTNET'e bir MARKET order doğrular/gönderir")
    p_market.add_argument("--symbol", required=True)
    p_market.add_argument("--side", required=True, choices=["BUY", "SELL"])
    p_market.add_argument("--quantity", type=float, default=None)
    p_market.add_argument("--quote-quantity", type=float, default=None, dest="quote_quantity")
    p_market.add_argument("--context-id", default=None)
    p_market.add_argument("--confirm-testnet-order", action="store_true")

    p_limit = subparsers.add_parser("place-limit", help="TESTNET'e bir LIMIT GTC order doğrular/gönderir")
    p_limit.add_argument("--symbol", required=True)
    p_limit.add_argument("--side", required=True, choices=["BUY", "SELL"])
    p_limit.add_argument("--quantity", type=float, required=True)
    p_limit.add_argument("--price", type=float, required=True)
    p_limit.add_argument("--context-id", default=None)
    p_limit.add_argument("--confirm-testnet-order", action="store_true")

    p_status = subparsers.add_parser(
        "order-status", help="Yerel durable execution kaydını gösterir (ağ çağrısı YAPMAZ)"
    )
    status_group = p_status.add_mutually_exclusive_group(required=True)
    status_group.add_argument("--context-id", default=None)
    status_group.add_argument("--client-order-id", default=None)

    p_reconcile = subparsers.add_parser(
        "reconcile", help="Tek bir kaydı TESTNET'e karşı yeniden sorgulayıp günceller (kimlik bilgisi GEREKTİRİR)"
    )
    reconcile_group = p_reconcile.add_mutually_exclusive_group(required=True)
    reconcile_group.add_argument("--context-id", default=None)
    reconcile_group.add_argument("--client-order-id", default=None)

    subparsers.add_parser(
        "reconcile-pending",
        help="Reconciliation GEREKTİREN TÜM yerel kayıtları TESTNET'e karşı yeniden sorgular "
        "(restart-sonrası kurtarma; kimlik bilgisi GEREKTİRİR)",
    )

    return parser


_HANDLERS = {
    "account-check": _cmd_account_check,
    "validate-symbol": _cmd_validate_symbol,
    "place-market": _cmd_place_market,
    "place-limit": _cmd_place_limit,
    "order-status": _cmd_order_status,
    "reconcile": _cmd_reconcile,
    "reconcile-pending": _cmd_reconcile_pending,
}


async def run_cli_async(argv: list[str] | None, *, http_client: TestnetHttpClient | None = None) -> int:
    """`main()`'in test edilebilir çekirdeği — `http_client` enjekte
    edilebilir (offline testler için `FakeTestnetHttpClient`; gerçek
    kullanım için `None` -> `UrllibTestnetHttpClient()`)."""
    args = _build_parser().parse_args(argv)

    try:
        config = _build_config()
    except ExecutionError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return EXIT_ERROR

    print(f"Target host: {config.base_url}")

    client = BinanceTestnetClient(config, http_client or UrllibTestnetHttpClient(), clock=SystemClock())
    handler = _HANDLERS[args.command]

    try:
        return await handler(config, client, args)
    except ExecutionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_ERROR


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run_cli_async(argv))


if __name__ == "__main__":
    sys.exit(main())


=== FILE: scripts/historical_replay.py ===
#!/usr/bin/env python3
"""
Pre-Audit Enhancement Pass — Deterministic Historical Replay CLI.

Fetches REAL historical candles from Binance's PUBLIC REST endpoint
(`/api/v3/klines`, via the accepted, unmodified `providers/binance/
rest.py::BinanceRestClient.fetch_historical_candles` — no private/signed
endpoint, no API key/secret, no Testnet order of any kind) and replays
them through `research.replay.HistoricalReplayDriver`, which reuses the
accepted `RuntimeCoordinator`/`FeatureEngine`/`SignalEngine`/
`PaperTradingEngine` pipeline unmodified (see `research/replay.py`'s
module docstring for the full reuse architecture and no-look-ahead
ordering policy).

This is a RESEARCH tool. It never claims live-equivalent results (order-
book evidence is always SYNTHETIC today — see `research/replay.py`,
Section D) and never prints a "profitable = ready for live trading"
message — a profitable replay is evidence to weigh, not a verdict.

Usage:
    python3 scripts/historical_replay.py --symbol BTCUSDT \\
        --start 2026-01-01T00:00:00+00:00 --end 2026-02-01T00:00:00+00:00

    python3 scripts/historical_replay.py --symbol BTCUSDT --symbol ETHUSDT \\
        --start 2026-01-01T00:00:00+00:00 --end 2026-01-15T00:00:00+00:00 \\
        --attribution --fee-bps 10 --slippage-bps 5

Requires network access to https://api.binance.com (PUBLIC endpoints
only). No credentials of any kind are read, requested, or required.

OPERATOR NOTE — pick `--end` safely in the past: if `--end` lands on or
after the CURRENTLY-FORMING candle's open time, Binance's REST API
returns that in-progress candle too (with `is_closed=False`, per the
accepted `providers/binance/parser.py`), and `research.data_quality`
correctly rejects it (`UNCLOSED_CANDLE`) rather than silently treating
partial live data as historical fact — the replay then aborts with
`DATA QUALITY: FAILED`. This is intentional, correct, fail-closed
behavior, not a bug: pick `--end` at least one full candle duration (of
your largest configured timeframe, H1 by default) before "now".
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime

from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from research.attribution import compute_report
from research.errors import DataQualityError, ResearchError
from research.replay import HistoricalReplayDriver, ReplayConfig


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı (örn. 2026-01-01T00:00:00+00:00)")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    config = BinanceConfig(supported_symbols=tuple(args.symbol))
    rest_client = BinanceRestClient(
        config=config, http_client=UrllibHttpClient(), clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    driver = HistoricalReplayDriver(
        candle_source=rest_client,
        config=ReplayConfig(
            notional_per_position=args.notional, fee_bps=args.fee_bps, slippage_bps=args.slippage_bps,
        ),
    )

    print("=" * 72)
    print("HISTORICAL REPLAY — Pre-Audit Enhancement Pass research tool")
    print("=" * 72)
    print(f"SYMBOLS:              {', '.join(args.symbol)}")
    print(f"DATE RANGE:           {args.start.isoformat()}  ->  {args.end.isoformat()}")

    try:
        result = await driver.run(symbols=tuple(args.symbol), start=args.start, end=args.end)
    except DataQualityError as exc:
        print("DATA QUALITY:         FAILED — replay ABORTED, no trades were simulated")
        for violation in exc.violations[:20]:
            print(f"    - {violation}")
        return 2
    except ResearchError as exc:
        print(f"REPLAY ERROR:          {exc}")
        return 3

    print("DATA QUALITY:          PASSED "
          f"({sum(r.candle_count for r in result.data_quality_reports)} candles across "
          f"{len(result.data_quality_reports)} (symbol, timeframe) series validated)")
    print(f"REPLAY MODE:           deterministic historical replay (RuntimeCoordinator, unmodified)")
    print(f"ORDER BOOK PROVENANCE: {result.order_book_provenance.value}")

    report = compute_report(result.cycle_results, open_position_count=len(result.final_positions))
    m = report.overall
    print(f"TRADE COUNT:           {m.trade_count}  (wins={m.wins}, losses={m.losses}, breakeven={m.breakeven})")
    print(f"WIN RATE:              {'n/a (no trades)' if m.win_rate is None else f'{m.win_rate:.1%}'}")
    print(f"NET PNL:               {m.net_pnl:.2f}")
    print(f"TOTAL FEES:            {m.total_fees:.2f}")
    print(f"PROFIT FACTOR:         {'n/a (no losing trades)' if m.profit_factor is None else f'{m.profit_factor:.2f}'}")
    print(f"MAX DRAWDOWN:          {'n/a (no trades)' if m.max_drawdown is None else f'{m.max_drawdown:.2f}'}")
    print(f"OPEN AT END (excl.):   {report.open_position_count_excluded} position(s) still open — unrealized, not counted above")

    print("-" * 72)
    print("LIMITATIONS:")
    for limitation in result.limitations:
        print(f"    [{limitation.code}] {limitation.detail}")
    print(f"    [{report.regime_attribution_note.split(':')[0]}] {report.regime_attribution_note}")

    if args.attribution:
        print("-" * 72)
        print("ATTRIBUTION BY SYMBOL:")
        for label, metrics in report.by_symbol.items():
            print(f"    {label}: trades={metrics.trade_count} net_pnl={metrics.net_pnl:.2f}")
        print("ATTRIBUTION BY DIRECTION:")
        for label, metrics in report.by_direction.items():
            print(f"    {label}: trades={metrics.trade_count} net_pnl={metrics.net_pnl:.2f}")
        print("ATTRIBUTION BY RISK LEVEL:")
        for label, metrics in report.by_risk_level.items():
            print(f"    {label}: trades={metrics.trade_count} net_pnl={metrics.net_pnl:.2f}")
        print("ATTRIBUTION BY CONFIDENCE BUCKET:")
        for label, metrics in report.by_confidence_bucket.items():
            print(f"    {label}: trades={metrics.trade_count} net_pnl={metrics.net_pnl:.2f}")
        print("ATTRIBUTION BY SUPPORTING AGENT:")
        for label, metrics in report.by_supporting_agent.items():
            print(f"    {label}: trades={metrics.trade_count} net_pnl={metrics.net_pnl:.2f}")

    print("=" * 72)
    print("This is a RESEARCH result over SYNTHETIC order-book evidence — it is")
    print("NOT proof of live-equivalent historical edge, and profitability here")
    print("is NOT a signal that the strategy is ready for live trading.")
    print("=" * 72)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", required=True, help="repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT")
    parser.add_argument("--start", type=_parse_utc, required=True)
    parser.add_argument("--end", type=_parse_utc, required=True)
    parser.add_argument("--notional", type=float, default=1000.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--attribution", action="store_true", help="print the full per-dimension attribution breakdown")
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/lifecycle_replay_sanity.py ===
#!/usr/bin/env python3
"""
Autonomous Testnet trading lifecycle Phase 20 — bounded, deterministic
replay sanity check for the ATR-based stop/target/trailing exit policy.

Fetches REAL historical candles from Binance's PUBLIC REST endpoint (no
API key/secret, no Testnet order of any kind — identical network boundary
to `scripts/historical_replay.py`), runs the accepted
`research.replay.HistoricalReplayDriver` unmodified to get REAL
Quant/Consensus/Risk-generated entry signals, then simulates exits for
every FLAT->LONG entry using
`crypto_signal_engine.execution.lifecycle_replay_sanity.simulate_lifecycle_exits`
— the EXACT SAME `evaluate_candle()` function the live M1 lifecycle
evaluator uses.

This is NOT profit optimization and does not search/tune any parameter —
it exists only to catch pathological policy behavior (absurdly tight
stops, unreachable targets, one exit reason dominating everything,
max-hold swallowing every trade). A positive or negative P&L result here
proves nothing about live profitability.

Usage:
    python3 scripts/lifecycle_replay_sanity.py --symbol BTCUSDT \\
        --start 2026-08-01T00:00:00+00:00 --end 2026-09-01T00:00:00+00:00

Requires network access to https://api.binance.com (PUBLIC endpoints
only). No credentials of any kind are read, requested, or required.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig
from crypto_signal_engine.execution.lifecycle_replay_sanity import simulate_lifecycle_exits
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from research.errors import DataQualityError, ResearchError
from research.replay import HistoricalReplayDriver, ReplayConfig


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı (örn. 2026-01-01T00:00:00+00:00)")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    config = BinanceConfig(supported_symbols=tuple(args.symbol))
    rest_client = BinanceRestClient(
        config=config, http_client=UrllibHttpClient(), clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    driver = HistoricalReplayDriver(candle_source=rest_client, config=ReplayConfig(notional_per_position=1000.0))
    exit_policy = ExitPolicyConfig(
        stop_atr_multiple=args.stop_atr_multiple, take_profit_atr_multiple=args.take_profit_atr_multiple,
        trailing_activation_atr_multiple=args.trailing_activation_atr_multiple,
        trailing_distance_atr_multiple=args.trailing_distance_atr_multiple, max_hold_hours=args.max_hold_hours,
    )

    print("=" * 72)
    print("LIFECYCLE REPLAY SANITY — autonomous Testnet trading, Phase 20")
    print("=" * 72)
    print(f"SYMBOLS:               {', '.join(args.symbol)}")
    print(f"DATE RANGE:            {args.start.isoformat()}  ->  {args.end.isoformat()}")
    print(
        f"EXIT POLICY:           stop={exit_policy.stop_atr_multiple}x ATR, "
        f"target={exit_policy.take_profit_atr_multiple}x ATR, "
        f"trailing_activation={exit_policy.trailing_activation_atr_multiple}x ATR, "
        f"trailing_distance={exit_policy.trailing_distance_atr_multiple}x ATR, "
        f"max_hold={exit_policy.max_hold_hours}h"
    )

    try:
        replay_result = await driver.run(symbols=tuple(args.symbol), start=args.start, end=args.end)
    except DataQualityError as exc:
        print("DATA QUALITY:          FAILED — sanity check ABORTED")
        for violation in exc.violations[:20]:
            print(f"    - {violation}")
        return 2
    except ResearchError as exc:
        print(f"REPLAY ERROR:          {exc}")
        return 3

    print(
        "DATA QUALITY:          PASSED "
        f"({sum(r.candle_count for r in replay_result.data_quality_reports)} candles across "
        f"{len(replay_result.data_quality_reports)} (symbol, timeframe) series validated)"
    )

    report = await simulate_lifecycle_exits(
        replay_result, candle_source=rest_client, exit_policy=exit_policy,
        exit_window=timedelta(hours=args.max_hold_hours * 2),
    )

    print("-" * 72)
    print(f"CANDIDATE ENTRIES:     {report.trade_count + report.open_at_end_count}")
    print(f"COMPLETED TRADES:      {report.trade_count}")
    print(f"STILL OPEN AT END:     {report.open_at_end_count} (excluded from stats below)")
    print(f"EXIT REASON COUNTS:    {dict(report.exit_reason_distribution)}")
    print(f"TOTAL GROSS PNL/UNIT:  {report.total_gross_pnl_per_unit:.4f}")
    print(f"WIN/LOSS:              {report.win_count}/{report.loss_count}")
    print(f"WIN RATE:              {'n/a (no trades)' if report.win_rate is None else f'{report.win_rate:.1%}'}")
    print(
        "AVG HOLD:              "
        f"{'n/a' if report.average_holding_seconds is None else f'{report.average_holding_seconds / 3600:.2f}h'}"
    )
    print(f"MAX DRAWDOWN/UNIT:     {'n/a' if report.max_drawdown_per_unit is None else f'{report.max_drawdown_per_unit:.4f}'}")

    print("=" * 72)
    print("This checks POLICY BEHAVIOR (sane stop/target distances, no single exit")
    print("reason dominating, trailing behaves sensibly) — it is NOT profit")
    print("optimization and proves nothing about live profitability.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", required=True, help="repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT")
    parser.add_argument("--start", type=_parse_utc, required=True)
    parser.add_argument("--end", type=_parse_utc, required=True)
    parser.add_argument("--stop-atr-multiple", type=float, default=2.0)
    parser.add_argument("--take-profit-atr-multiple", type=float, default=4.0)
    parser.add_argument("--trailing-activation-atr-multiple", type=float, default=2.0)
    parser.add_argument("--trailing-distance-atr-multiple", type=float, default=2.0)
    parser.add_argument("--max-hold-hours", type=float, default=48.0)
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/live_public_smoke_test.py ===
#!/usr/bin/env python3
"""
Faz 6 — canlı PUBLIC market-data smoke test (OPSİYONEL, MANUEL).

Bu script:
- Gerçek Binance PUBLIC WebSocket/REST'e bağlanır (`wss://stream.binance.com`,
  `https://api.binance.com`) — YALNIZCA public market data.
- API key/secret/credential ALMAZ ve KULLANMAZ.
- HİÇBİR emir göndermez/iptal etmez — yalnızca `stream_candles()`/
  `stream_order_book()` üzerinden birkaç public event alır ve normalize
  edildiğini doğrular.
- SINIRLI bir zaman aşımı (`--timeout-seconds`, varsayılan 15s) sonunda
  HER DURUMDA temiz bir şekilde kapanır (`provider.close()`).

Bu script:
- ZORUNLU offline pytest suite'inin PARÇASI DEĞİLDİR (bkz. tests/ dizini —
  hiçbir test dosyası bunu import/çağırmaz).
- İnternet erişimi olmayan bir ortamda ÇALIŞMAZ/timeout ile başarısız
  olabilir — bu BEKLENEN bir durumdur, Faz 6 acceptance'ı bu script'in
  başarısına BAĞLI DEĞİLDİR.

Kullanım:
    python3 scripts/live_public_smoke_test.py --symbol BTCUSDT --timeout-seconds 15
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.provider import BinanceMarketDataProvider
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient


async def _consume_a_few_candles(provider: BinanceMarketDataProvider, symbol: str, limit: int) -> int:
    received = 0
    async for candle in provider.stream_candles(symbol, Timeframe.M1):
        print(f"[candle] {candle.symbol} {candle.timeframe.value} open_time={candle.open_time} "
              f"close={candle.close} is_closed={candle.is_closed}")
        received += 1
        if received >= limit:
            break
    return received


async def main(symbol: str, timeout_seconds: float, event_limit: int) -> int:
    config = BinanceConfig(supported_symbols=(symbol,))
    provider = BinanceMarketDataProvider(
        config=config,
        http_client=UrllibHttpClient(),
        ws_factory=_real_ws_factory(),
        clock=SystemClock(),
        sleeper=AsyncioSleeper(),
    )

    try:
        received = await asyncio.wait_for(
            _consume_a_few_candles(provider, symbol, event_limit), timeout=timeout_seconds
        )
        print(f"OK: {received} public candle event(s) received and normalized for {symbol}.")
        return 0
    except TimeoutError:
        print(f"TIMEOUT after {timeout_seconds}s — no internet access, or Binance unreachable. "
              f"This is expected in offline/sandboxed environments and does NOT affect Phase 6 acceptance.")
        return 1
    finally:
        await provider.close()
        print("Provider closed cleanly.")


def _real_ws_factory():
    # Lazy import — bkz. real_websocket.py docstring'i (offline testleri
    # ASLA etkilemez; yalnızca bu script gerçekten çalıştırıldığında
    # import edilir).
    from crypto_signal_engine.providers.binance.real_websocket import RealWebSocketConnectionFactory

    return RealWebSocketConnectionFactory()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--event-limit", type=int, default=3)
    args = parser.parse_args()

    exit_code = asyncio.run(main(args.symbol, args.timeout_seconds, args.event_limit))
    sys.exit(exit_code)


=== FILE: scripts/local_readiness_check.py ===
#!/usr/bin/env python3
"""
Faz 12 — yerel 24/7 çalıştırma öncesi tek-komutluk hazırlık kontrolü.

Bu script TAMAMEN GÜVENLİDİR:
- Hiçbir order göndermez/iptal etmez.
- Hiçbir zorunlu ağ çağrısı YAPMAZ (`crypto_signal_engine.app doctor`'ın
  KENDİSİ TAMAMEN offline'dır — bkz. `app.py::_run_doctor`).
- Hiçbir credential DEĞERİNİ yazdırmaz (yalnızca SET/UNSET).
- Kullanıcıdan ASLA bir credential yapıştırmasını İSTEMEZ.

Yaptıkları (sırayla):
1. `python -m crypto_signal_engine.app doctor`'ı (mevcut ortam
   değişkenleriyle) ÇALIŞTIRIR — config/dosya-sistemi/kilit/şema/
   execution-mode-gating/Mainnet-imkansızlığı/dashboard-bind kontrolleri.
2. `python -m compileall crypto_signal_engine scripts`'i ÇALIŞTIRIR.
3. `websockets` paketinin kurulu olup OLMADIĞINI raporlar (GERÇEK PUBLIC
   runtime için gerekli — `pip install .[runtime]`; kurulu DEĞİLSE bu bir
   FAIL DEĞİLDİR, yalnızca bir bilgilendirmedir — offline geliştirme/test
   bundan etkilenmez).
4. Durable state dizininde YETERLİ boş disk alanı olup OLMADIĞINI kontrol
   eder (aşırı düşükse WARN).

PUBLIC Binance bağlantısı veya gerçek TESTNET sorgusu bu script'in
PARÇASI DEĞİLDİR — bunlar ayrı, AÇIKÇA isimlendirilmiş, opsiyonel
script'lerdir:
    python3 scripts/live_public_smoke_test.py --symbol BTCUSDT
    python3 scripts/binance_testnet_lab.py account-check   (kimlik bilgisi
        ZATEN yerelde varsa)

Kullanım:
    python3 scripts/local_readiness_check.py
"""

from __future__ import annotations

import compileall
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_MIN_FREE_DISK_MB = 200


def _print_header(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def _run_doctor() -> bool:
    _print_header("1. crypto-signal-engine doctor (offline preflight)")
    result = subprocess.run(
        [sys.executable, "-m", "crypto_signal_engine.app", "doctor"], cwd=str(REPO_ROOT)
    )
    return result.returncode == 0


def _run_compileall() -> bool:
    _print_header("2. compileall (crypto_signal_engine, scripts)")
    ok_pkg = compileall.compile_dir(str(REPO_ROOT / "crypto_signal_engine"), quiet=1)
    ok_scripts = compileall.compile_dir(str(REPO_ROOT / "scripts"), quiet=1)
    ok = bool(ok_pkg and ok_scripts)
    print("PASS: compileall" if ok else "FAIL: compileall")
    return ok


def _check_websockets_installed() -> None:
    _print_header("3. `websockets` package (needed for real PUBLIC runtime)")
    if importlib.util.find_spec("websockets") is not None:
        print("INFO: websockets is installed — real `crypto-signal-engine run` can reach live Binance PUBLIC data")
    else:
        print(
            "INFO: websockets is NOT installed — this is fine for offline development/tests, "
            "but the real `crypto-signal-engine run` command needs it: pip install .[runtime]"
        )


def _check_disk_space() -> bool:
    _print_header("4. disk space for the durable state directory")
    db_path_raw = os.environ.get("CSE_DB_PATH", "var/lib/crypto-signal-engine/paper_state.db")
    state_dir = Path(db_path_raw).parent
    state_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(state_dir)
    free_mb = usage.free / (1024 * 1024)
    if free_mb < _MIN_FREE_DISK_MB:
        print(f"WARN: only {free_mb:.0f}MB free at {state_dir} (recommended minimum: {_MIN_FREE_DISK_MB}MB)")
        return False
    print(f"PASS: {free_mb:.0f}MB free at {state_dir}")
    return True


def main() -> int:
    doctor_ok = _run_doctor()
    compileall_ok = _run_compileall()
    _check_websockets_installed()
    disk_ok = _check_disk_space()

    _print_header("RESULT")
    overall_ok = doctor_ok and compileall_ok and disk_ok
    print(f"doctor:      {'PASS' if doctor_ok else 'FAIL'}")
    print(f"compileall:  {'PASS' if compileall_ok else 'FAIL'}")
    print(f"disk space:  {'PASS' if disk_ok else 'WARN'}")
    print("\nOptional (not run automatically, no credentials required):")
    print("  python3 scripts/live_public_smoke_test.py --symbol BTCUSDT --timeout-seconds 15")
    print("Optional (only if BINANCE_TESTNET_API_KEY/SECRET already exist locally):")
    print("  python3 scripts/binance_testnet_lab.py account-check")
    print(f"\nLOCAL READINESS: {'PASS' if overall_ok else 'FAIL'}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())


=== FILE: scripts/monte_carlo_robustness.py ===
#!/usr/bin/env python3
"""
Pre-Audit Enhancement Pass — Monte Carlo Robustness Screen CLI.

Replays real PUBLIC Binance historical candles (same reuse architecture
as `scripts/historical_replay.py`), extracts the completed trade
sequence (`research.attribution.extract_trades`), and runs
`research.monte_carlo.run_monte_carlo_robustness_screen` over it with an
explicit, reproducible seed. This is a PATH/DRAWDOWN SENSITIVITY screen,
NOT a test of statistical edge — see `research/monte_carlo.py`'s
`ASSUMPTIONS_NOTE`, printed verbatim below.

Usage:
    python3 scripts/monte_carlo_robustness.py --symbol BTCUSDT \\
        --start 2026-01-01T00:00:00+00:00 --end 2026-02-01T00:00:00+00:00 \\
        --seed 42 --iterations 2000
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime

from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from research.attribution import extract_trades
from research.errors import DataQualityError, ResearchError
from research.monte_carlo import run_monte_carlo_robustness_screen
from research.replay import HistoricalReplayDriver, ReplayConfig


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    config = BinanceConfig(supported_symbols=tuple(args.symbol))
    rest_client = BinanceRestClient(
        config=config, http_client=UrllibHttpClient(), clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    driver = HistoricalReplayDriver(candle_source=rest_client, config=ReplayConfig())

    print("=" * 72)
    print("MONTE CARLO ROBUSTNESS SCREEN — path/drawdown sensitivity, NOT edge proof")
    print("=" * 72)
    print(f"SYMBOLS:     {', '.join(args.symbol)}")
    print(f"DATE RANGE:  {args.start.isoformat()} -> {args.end.isoformat()}")
    print(f"SEED:        {args.seed}   ITERATIONS: {args.iterations}")

    try:
        replay_result = await driver.run(symbols=tuple(args.symbol), start=args.start, end=args.end)
    except DataQualityError as exc:
        print(f"DATA QUALITY: FAILED — {len(exc.violations)} violation(s), aborted")
        return 2
    except ResearchError as exc:
        print(f"ERROR: {exc}")
        return 3

    trades = extract_trades(replay_result.cycle_results)
    if not trades:
        print("No completed trades in this window — nothing to screen.")
        return 0

    report = run_monte_carlo_robustness_screen(
        [t.net_pnl for t in trades], seed=args.seed, iterations=args.iterations
    )

    print("-" * 72)
    print(f"TRADE COUNT:              {report.trade_count}")
    print(f"ORIGINAL TERMINAL PNL:    {report.original_terminal_pnl:.2f}")
    print(f"ORIGINAL MAX DRAWDOWN:    {report.original_max_drawdown:.2f}")
    print(f"TERMINAL PNL INVARIANT:   {report.terminal_pnl_invariant} (expected: True — sum is order-independent)")
    print(f"PERMUTED DRAWDOWN  min:   {report.drawdown_min:.2f}")
    print(f"PERMUTED DRAWDOWN  mean:  {report.drawdown_mean:.2f}")
    print(f"PERMUTED DRAWDOWN  median:{report.drawdown_median:.2f}")
    print(f"PERMUTED DRAWDOWN  max:   {report.drawdown_max:.2f}")
    print("-" * 72)
    print("ASSUMPTIONS / LIMITATIONS:")
    print(f"    {report.assumptions_note}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--start", type=_parse_utc, required=True)
    parser.add_argument("--end", type=_parse_utc, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--iterations", type=int, default=1000)
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/oos_stability.py ===
#!/usr/bin/env python3
"""
Pre-Audit Enhancement Pass — Rolling Out-of-Sample Stability CLI.

Runs `research.oos_stability.RollingOOSStabilityRunner` over real PUBLIC
Binance historical candles (same data source and reuse architecture as
`scripts/historical_replay.py` — see that script's and `research/
replay.py`'s docstrings). Each window gets a fully fresh strategy state;
see `research/oos_stability.py`'s module docstring for why this is a
STABILITY measurement of the existing, unchanged strategy, NOT walk-
forward optimization — no parameter is fit or searched here.

Usage:
    python3 scripts/oos_stability.py --symbol BTCUSDT \\
        --start 2026-01-01T00:00:00+00:00 --end 2026-03-01T00:00:00+00:00 \\
        --window-days 14 --step-days 14
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from research.errors import DataQualityError, ResearchError
from research.oos_stability import ROLLING_OOS_LABEL, RollingOOSStabilityRunner
from research.replay import HistoricalReplayDriver, ReplayConfig


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı")
    return parsed


async def _run(args: argparse.Namespace) -> int:
    config = BinanceConfig(supported_symbols=tuple(args.symbol))
    rest_client = BinanceRestClient(
        config=config, http_client=UrllibHttpClient(), clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    driver = HistoricalReplayDriver(candle_source=rest_client, config=ReplayConfig())
    runner = RollingOOSStabilityRunner(driver=driver)

    print("=" * 72)
    print(ROLLING_OOS_LABEL)
    print("=" * 72)
    print(f"SYMBOLS:     {', '.join(args.symbol)}")
    print(f"DATE RANGE:  {args.start.isoformat()} -> {args.end.isoformat()}")
    print(f"WINDOW SIZE: {args.window_days} day(s)   STEP SIZE: {args.step_days} day(s)")

    try:
        report = await runner.run(
            symbols=tuple(args.symbol), start=args.start, end=args.end,
            window_size=timedelta(days=args.window_days), step_size=timedelta(days=args.step_days),
        )
    except DataQualityError as exc:
        print(f"DATA QUALITY: FAILED — {len(exc.violations)} violation(s), aborted")
        return 2
    except ResearchError as exc:
        print(f"ERROR: {exc}")
        return 3

    print("-" * 72)
    for window_result in report.windows:
        w = window_result.window
        m = window_result.metrics
        ob = window_result.replay_result.order_book_provenance.value
        print(
            f"WINDOW {w.index:>3}  [{w.start.date()} -> {w.end.date()}]  "
            f"OB={ob}  trades={m.trade_count:>4}  "
            f"net_pnl={m.net_pnl:>10.2f}  "
            f"max_dd={'n/a' if m.max_drawdown is None else f'{m.max_drawdown:.2f}':>10}  "
            f"win_rate={'n/a' if m.win_rate is None else f'{m.win_rate:.1%}':>7}"
        )
    print("=" * 72)
    print("NOTE: no parameter was fit or searched in any window — this measures")
    print("consistency of the existing fixed strategy across periods only.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--start", type=_parse_utc, required=True)
    parser.add_argument("--end", type=_parse_utc, required=True)
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--step-days", type=int, default=14)
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/public_soak_test.py ===
#!/usr/bin/env python3
"""
Faz 9 — OPSİYONEL, MANUEL gerçek Binance PUBLIC market-data soak testi.

Bu script:
- Gerçek Binance PUBLIC WebSocket/REST'e bağlanır — YALNIZCA public market
  data (`scripts/live_public_smoke_test.py` ile AYNI güvenlik sınırı).
- API key/secret/credential ALMAZ ve KULLANMAZ.
- HİÇBİR emir göndermez/iptal etmez — yalnızca Faz 5 `PaperTradingEngine`
  (paper trading) + Faz 7 `PaperStateStore` (SQLite persistence) kullanır.
  `ALLOW_LIVE_TRADING = False` değişmez.
- Açıkça belirtilen bir süre (`--duration-seconds`) boyunca çalışır,
  SIGINT/SIGTERM'de graceful/idempotent olarak durur (Faz 8'in
  `PersistedRuntime.stop()` sözleşmesiyle AYNI).
- Periyodik (varsayılan 30s) bir sağlık/kaynak özeti yazdırır.
- Sonunda hem insan-okur hem makine-okur (`--report-path`) bir soak raporu
  üretir (bkz. PHASE9_LONG_RUN_STABILITY.md — "Soak Report").
- Recovery/runtime FATAL hata durumunda non-zero exit code döner.

Bu script:
- ZORUNLU offline pytest suite'inin PARÇASI DEĞİLDİR — hiçbir test dosyası
  bunu import/çağırmaz.
- İnternet erişimi olmayan bir ortamda ÇALIŞMAZ/timeout ile başarısız
  olabilir — bu BEKLENEN bir durumdur, Faz 9 acceptance'ı bu script'in
  başarısına BAĞLI DEĞİLDİR.

Kullanım:
    python3 scripts/public_soak_test.py --symbols BTCUSDT,ETHUSDT \\
        --duration-seconds 3600 --db-path /tmp/soak.db --report-path /tmp/soak-report.json
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.provider import BinanceMarketDataProvider
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator


def _real_ws_factory():
    from crypto_signal_engine.providers.binance.real_websocket import RealWebSocketConnectionFactory

    return RealWebSocketConnectionFactory()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summarize(runtime: PersistedRuntime) -> dict[str, object]:
    status = runtime.status()
    return {
        "generated_at": status.generated_at.isoformat(),
        "overall_health": status.overall_health.value,
        "symbols": [
            {
                "symbol": s.symbol, "health": s.health.value,
                "last_event_at": s.last_event_at.isoformat() if s.last_event_at else None,
                "reconnect_count": s.reconnect_count, "detail": s.detail,
            }
            for s in status.symbols
        ],
    }


async def run_soak(symbols: tuple[str, ...], duration_seconds: float, db_path: Path, print_interval_seconds: float) -> tuple[int, dict[str, object]]:
    config = BinanceConfig(supported_symbols=symbols)
    provider = BinanceMarketDataProvider(
        config=config, http_client=UrllibHttpClient(), ws_factory=_real_ws_factory(),
        clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    paper_engine = PaperTradingEngine()
    coordinator = RuntimeCoordinator(symbols=symbols, provider=provider, paper_engine=paper_engine)
    store = PaperStateStore(db_path)
    runtime = PersistedRuntime(coordinator, store)

    report: dict[str, object] = {
        "start": _utc_now(), "symbols": list(symbols), "duration_requested_seconds": duration_seconds,
        "health_samples": [], "fatal_errors": [],
    }

    try:
        recovery_reports = await runtime.recover()
        report["recovery"] = {
            "ok": True,
            "bootstrap": [
                {"symbol": r.symbol, "timeframe": r.timeframe.value, "candles_applied": r.candles_applied, "ready": r.ready}
                for r in recovery_reports
            ],
        }
    except Exception as exc:
        report["recovery"] = {"ok": False, "detail": str(exc)}
        report["fatal_errors"].append(f"recovery: {exc}")
        report["end"] = _utc_now()
        store.close()
        return 1, report

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    run_task = asyncio.create_task(runtime.run())
    start = time.monotonic()
    exit_code = 0
    try:
        while True:
            elapsed = time.monotonic() - start
            remaining = duration_seconds - elapsed
            if remaining <= 0 or stop_event.is_set():
                break
            wait_for = min(print_interval_seconds, remaining)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait_for)
            except TimeoutError:
                pass
            summary = _summarize(runtime)
            report["health_samples"].append(summary)
            print(f"[{summary['generated_at']}] overall={summary['overall_health']} "
                  f"({elapsed:.0f}s / {duration_seconds:.0f}s elapsed)")
            for s in summary["symbols"]:
                print(f"    {s['symbol']:<12} {s['health']:<12} reconnects={s['reconnect_count']} detail={s['detail']!r}")
            if run_task.done() and run_task.exception() is not None:
                report["fatal_errors"].append(str(run_task.exception()))
                exit_code = 1
                break
    finally:
        await runtime.stop()
        with contextlib.suppress(Exception):
            await run_task

    report["end"] = _utc_now()
    report["final_health"] = _summarize(runtime) if not run_task.cancelled() else None
    return exit_code, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", default="BTCUSDT", help="Virgülle ayrılmış sembol listesi")
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--db-path", type=Path, default=Path("/tmp/crypto-signal-engine-public-soak.db"))
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--print-interval-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)

    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    exit_code, report = asyncio.run(
        run_soak(symbols, args.duration_seconds, args.db_path, args.print_interval_seconds)
    )

    print(json.dumps(report, indent=2))
    if args.report_path is not None:
        args.report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Report written to {args.report_path}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())


=== FILE: scripts/run_with_adaptive_policy.py ===
#!/usr/bin/env python3
"""
Adaptive Intelligence v1, step 14 — PRODUCTION WIRING. This is the ONLY
file in the whole milestone that imports BOTH `crypto_signal_engine/`
AND `adaptive/` — it resolves the dependency direction the hard rule
requires (`crypto_signal_engine/` must never import `adaptive/`) by
constructing, HERE, the concrete callables `Application` accepts as
plain, already-owned types:

- `exit_policy_provider` (`Callable[[], tuple[ExitPolicyConfig, str |
  None]]`): reads the current champion from `adaptive.store.AdaptiveStore`
  and returns its resolved `ExitPolicyConfig` + `version_id`. Called ONCE
  per NEW position entry by `LifecycleManager` (step 2/14) — never
  retroactively for an already-open position.
- `m1_candle_observer` (`Callable[[str, LifecycleCandle], None]`): an
  `adaptive.shadow.ShadowMonitor.on_m1_candle` bound method, wired into
  the SAME live M1 stream `LifecycleManager.evaluate_m1_candle()` itself
  consumes (`LifecycleRuntime._consume_m1`, step 8's M1-fidelity fix) —
  NOT `RuntimeCoordinator.candle_observer` (M5/M15/H1 only), which would
  evaluate shadow challengers at 5-minute-aggregated granularity instead
  of the true 1-minute granularity the real position uses.
- `champion_live_evidence_provider` (`Callable[[str], EvidenceSummary]`):
  reads REAL completed-trade P&L, attributed by `policy_version_id`, from
  `LifecycleStore.completed_trades_for_policy_version()` — this is why
  this callable must be built HERE rather than inside `adaptive/cycle.py`
  itself (`adaptive/` is not permitted to import `lifecycle_store`).

This script also starts `adaptive.scheduler.AdaptiveScheduler` as an
independent background `asyncio.Task` alongside the live `Application`
(step 13) — the automatic evaluation-cycle loop and the live trading
runtime share one event loop but never block each other.

The existing static-config `python -m crypto_signal_engine.app run` path
is COMPLETELY UNCHANGED and still works standalone — this script is
strictly additive, equivalent to running `Application` with
`exit_policy_provider=None`/`m1_candle_observer=None` when adaptive
policy is simply not wired in.

IMPORTANT — mode requirement: `exit_policy_provider`/`m1_candle_observer`
only have any observable effect when the autonomous Testnet trading
lifecycle bridge is enabled (`CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET
CSE_ENABLE_TESTNET_EXECUTION=true CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=true`)
— `ExitPolicyConfig`/`policy_version_id` are concepts that exist ONLY on
`BridgePositionRecord` (the Testnet bridge's own position record, see
step 3's finding); a pure PAPER-mode run has no such record at all, so
this script is a genuine no-op with respect to adaptive policy in PAPER
mode (still fully safe to run — it simply demonstrates nothing new).

Usage (PAPER, safe, demonstrates nothing new re: adaptive policy):
    python3 scripts/run_with_adaptive_policy.py \\
        --adaptive-db var/adaptive.db --anchor 2026-01-01T00:00:00+00:00

Usage (Testnet bridge enabled, the mode that actually exercises this
wiring — requires the SAME env vars as `python -m crypto_signal_engine.app
run` in Testnet-bridge mode, PLUS the two flags above):
    CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET CSE_ENABLE_TESTNET_EXECUTION=true \\
    CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=true \\
    python3 scripts/run_with_adaptive_policy.py \\
        --adaptive-db var/adaptive.db --anchor 2026-01-01T00:00:00+00:00
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

from adaptive.decision import EMPTY_EVIDENCE, EvidenceSummary, evidence_from_pnls
from adaptive.policy import PolicySnapshot
from adaptive.scheduler import (
    DEFAULT_CYCLE_INTERVAL_SECONDS,
    AdaptiveScheduler,
    AdaptiveSchedulerConfig,
)
from adaptive.shadow import ShadowMonitor
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.app import (
    EXIT_CONFIG_ERROR,
    EXIT_FATAL,
    EXIT_SYMBOL_SELECTION_FAILURE,
    Application,
    build_real_provider,
    configure_logging,
    resolve_symbols,
)
from crypto_signal_engine.errors import SymbolSelectionError
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig
from crypto_signal_engine.ops.config import AppConfig, load_config
from crypto_signal_engine.ops.notifier import build_telegram_notifier
from crypto_signal_engine.ops.errors import AppConfigurationError
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient

_LOGGER = logging.getLogger("scripts.run_with_adaptive_policy")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} UTC-aware olmalı (örn. 2026-01-01T00:00:00+00:00)")
    return parsed


def _build_exit_policy_provider(store: AdaptiveStore):
    def provider() -> tuple[ExitPolicyConfig, str | None]:
        champion: PolicySnapshot | None = store.current_champion()
        if champion is None:
            return ExitPolicyConfig(), None
        return champion.to_exit_policy_config(), champion.version_id

    return provider


def _completed_trade_pnls(app: Application, version_id: str) -> list[float]:
    """Shared by both providers below: real net-of-fees P&L per completed
    trade attributed to `version_id`, via `LifecycleStore.completed_
    trades_for_policy_version()` (step 3). Empty in PAPER mode / when the
    Testnet bridge is disabled (`app._lifecycle_store is None`) -- no
    real bridge trades exist to attribute."""
    if app._lifecycle_store is None:  # noqa: SLF001
        return []
    rows = app._lifecycle_store.completed_trades_for_policy_version(version_id)  # noqa: SLF001
    return [row["net_realized_pnl"] for row in rows if row["net_realized_pnl"] is not None]


def _build_champion_live_evidence_provider(app: Application):
    def provider(version_id: str) -> EvidenceSummary:
        pnls = _completed_trade_pnls(app, version_id)
        return evidence_from_pnls(pnls) if pnls else EMPTY_EVIDENCE

    return provider


def _build_champion_live_pnls_provider(app: Application):
    """Rollback-criterion fix (independent-verification round):
    `adaptive.rollback.apply_rollback_if_needed` needs RAW per-trade P&L
    values (to compute `research.attribution.PerformanceMetrics`-shaped
    `profit_factor`/`win_rate`), not the pre-aggregated `EvidenceSummary`
    `_build_champion_live_evidence_provider` above returns -- same
    underlying data, different shape."""

    def provider(version_id: str) -> list[float]:
        return _completed_trade_pnls(app, version_id)

    return provider


def _build_adaptive_status_provider(store: AdaptiveStore):
    """UI Polish v1, Step A3 — the concrete callable `app.py::
    Application`'s `adaptive_status_provider` hook accepts. Reads ONLY
    already-existing `AdaptiveStore` queries (Karar 93's `current_
    champion()`/`champion_history()`) — no new persistence. Called from
    `Application._adaptive_snapshot()`, which ALREADY wraps this in its
    own try/except (defense in depth on top of this function's own
    straightforward, exception-free read path)."""

    def provider() -> dict[str, object]:
        champion: PolicySnapshot | None = store.current_champion()
        history = store.champion_history()
        last_event = history[-1] if history else None
        return {
            "active": True,
            "champion": None if champion is None else {
                "version_id": champion.version_id,
                "stop_atr_multiple": champion.stop_atr_multiple,
                "take_profit_atr_multiple": champion.take_profit_atr_multiple,
                "trailing_activation_atr_multiple": champion.trailing_activation_atr_multiple,
                "trailing_distance_atr_multiple": champion.trailing_distance_atr_multiple,
                "max_hold_hours": champion.max_hold_hours,
                "provenance": champion.provenance,
                "created_at": champion.created_at.isoformat(),
            },
            "last_event": None if last_event is None else {
                "version_id": last_event["version_id"],
                "promoted_at": last_event["promoted_at"],
                "reason": last_event["reason"],
                "is_rollback": "ROLLBACK" in str(last_event["reason"]),
            },
        }

    return provider


def _build_live_cycle_results_provider(app: Application, symbols: tuple[str, ...]):
    """Adaptive Intelligence v1, drift-signal wiring fix — the real,
    already-accepted `RuntimeCoordinator.last_cycle_result(symbol)`
    accessor (no new coordinator surface needed) supplies each
    configured symbol's most recent live PAPER/Testnet-bridge signal
    evaluation, feeding `adaptive.cycle.run_adaptive_cycle`'s per-cycle
    drift comparison against the discovery replay's own signal stream."""
    coordinator = getattr(app.runtime, "_coordinator", app.runtime)

    def provider():
        results = (coordinator.last_cycle_result(symbol) for symbol in symbols)
        return tuple(r for r in results if r is not None)

    return provider


async def _run(args: argparse.Namespace) -> int:
    try:
        config: AppConfig = load_config(os.environ)
    except AppConfigurationError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    configure_logging(config.log_level)

    selection_result = None
    pinned_symbols: tuple[str, ...] = ()
    if config.auto_select_symbols:
        try:
            config, selection_result, pinned_symbols = await resolve_symbols(config)
        except SymbolSelectionError as exc:
            print(f"AUTOMATIC SYMBOL SELECTION FAILED: {exc}", file=sys.stderr)
            return EXIT_SYMBOL_SELECTION_FAILURE

    adaptive_store = AdaptiveStore(args.adaptive_db)
    shadow_monitor = ShadowMonitor()
    exit_policy_provider = _build_exit_policy_provider(adaptive_store)
    provider = build_real_provider(config)

    app = Application(
        config, provider, selection_result=selection_result, pinned_symbols=pinned_symbols,
        exit_policy_provider=exit_policy_provider, m1_candle_observer=shadow_monitor.on_m1_candle,
        # UI Polish v1, Step A3 — the dashboard's Adaptive Intelligence
        # section reads this via the health snapshot (see app.py
        # ::Application._adaptive_snapshot). A plain `python -m
        # crypto_signal_engine.app run` never passes this, so the
        # dashboard correctly shows "not active in this run" there.
        adaptive_status_provider=_build_adaptive_status_provider(adaptive_store),
    )

    scheduler_rest_client = BinanceRestClient(
        config=BinanceConfig(supported_symbols=config.symbols), http_client=UrllibHttpClient(),
        clock=SystemClock(), sleeper=AsyncioSleeper(),
    )
    scheduler = AdaptiveScheduler(
        store=adaptive_store, candle_source=scheduler_rest_client,
        config=AdaptiveSchedulerConfig(
            symbols=config.symbols, anchor=args.anchor,
            window_config=WindowConfig(
                discovery_window_size=timedelta(hours=args.discovery_hours),
                confirmation_window_size=timedelta(hours=args.confirmation_hours),
            ),
            cycle_interval_seconds=args.scheduler_interval_seconds,
        ),
        champion_live_evidence_provider=_build_champion_live_evidence_provider(app),
        champion_live_pnls_provider=_build_champion_live_pnls_provider(app),
        live_cycle_results_provider=_build_live_cycle_results_provider(app, config.symbols),
        # 24/7 Ops v1, Step 4 — outbound-only alerting for a rollback
        # event. `None` unless CSE_TELEGRAM_BOT_TOKEN/CSE_TELEGRAM_CHAT_ID
        # are both set (see ops/notifier.py); a genuine no-op otherwise.
        notifier=build_telegram_notifier(),
        # UI Polish v1, Step A9 — the SAME dedicated event-log file
        # `Application` itself writes to/reads from (see app.py
        # ::Application.__init__, `# noqa: SLF001` — same private-
        # accessor precedent as `app._lifecycle_store`/`app.runtime.
        # _coordinator` elsewhere in this script) — never a second,
        # divergent path.
        event_log_path=app._event_log_path,  # noqa: SLF001
    )
    scheduler.start()

    print("=" * 72)
    print("RUN WITH ADAPTIVE POLICY — Adaptive Intelligence v1, step 14")
    print("=" * 72)
    print(f"ADAPTIVE DB:           {args.adaptive_db}")
    champion = adaptive_store.current_champion()
    print(f"CURRENT CHAMPION:      {champion.version_id if champion is not None else '(none yet -- static default)'}")
    print(f"SCHEDULER INTERVAL:    {args.scheduler_interval_seconds}s")
    print(f"BRIDGE ENABLED:        {app._lifecycle_manager is not None}")  # noqa: SLF001
    print("=" * 72)

    try:
        exit_code = await app.start()
    except Exception:
        _LOGGER.error("fatal uncaught exception during startup", exc_info=True)
        exit_code = EXIT_FATAL
    finally:
        await scheduler.stop()
        adaptive_store.close()
    return exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--adaptive-db", required=True, help="path to adaptive/'s own dedicated SQLite database file")
    parser.add_argument("--anchor", type=_parse_utc, required=True, help="the very first confirmation window's start")
    parser.add_argument("--discovery-hours", type=float, default=720.0, help="discovery window size in hours (default 30 days)")
    parser.add_argument("--confirmation-hours", type=float, default=168.0, help="confirmation window size in hours (default 7 days)")
    parser.add_argument(
        "--scheduler-interval-seconds", type=float, default=DEFAULT_CYCLE_INTERVAL_SECONDS,
        help=f"evaluation-cycle interval, max {DEFAULT_CYCLE_INTERVAL_SECONDS}s (24h)",
    )
    parsed_args = parser.parse_args()
    sys.exit(asyncio.run(_run(parsed_args)))


=== FILE: scripts/verify_phase1.py ===
#!/usr/bin/env python3
"""
Faz 1 resmi acceptance script'i (Quality Gate 29).

Kullanım (repo kökünden):

    python3 scripts/verify_phase1.py

Bu script şunları SIRAYLA çalıştırır ve herhangi biri başarısız olursa
non-zero exit code ile durur:

  1. Repo ile birlikte teslim edilen önceden-inşa edilmiş wheel'i
     (`dist/crypto_signal_engine-*-py3-none-any.whl`) bulur; yoksa
     DETERMİNİSTİK olarak FAIL verir (yeniden inşa etmeye ÇALIŞMAZ).
  2. Bu wheel'i tamamen izole bir venv'e `pip install --no-index --no-deps`
     ile OFFLINE kurar (build backend çağrılmaz, ağ erişimi gerekmez).
  3. Bu izole venv'in python'ıyla, repo'nun ebeveyninden (dış dizin) gerçek
     bir import smoke test.
  4. Tam pytest suite'i (mevcut Python ortamında, kurulum GEREKTİRMEZ —
     repo source ağacından doğrudan çalışır; `tests/test_package_import.py`
     kendi izole/offline wheel testini ayrıca ve bağımsız olarak yapar).
  5. `python -m compileall`
  6. Repository safety scan (ALLOW_LIVE_TRADING, BinanceLiveExecutionAdapter,
     silent exception pattern'leri).

HARDENING NOTU (v3 — reproducibility düzeltmesi): önceki revizyon ilk
adımda `pip install -e .` çalıştırıyordu. `pyproject.toml` içindeki
`setuptools>=68.0` build-dependency'si, pip'in bunu build isolation ile
PyPI'den indirmeye çalışmasına yol açıyordu — ağsız/temiz bir ortamda bu
adım FAIL veriyordu. Bu, script'in kendi Gate 29 ("offline, reproducible
acceptance") hedefiyle doğrudan çelişiyordu. `pip install -e .` adımı
TAMAMEN KALDIRILDI; script artık HİÇBİR ADIMDA build backend'i devreye
sokmaz, ağ erişimi gerektirmez, ve --break-system-packages GEREKTİRMEZ.
"""

from __future__ import annotations

import glob
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"

IMPORT_SCRIPT = (
    "import crypto_signal_engine; "
    "import crypto_signal_engine.domain.models; "
    "import crypto_signal_engine.domain.events; "
    "import crypto_signal_engine.domain.consensus; "
    "import crypto_signal_engine.domain.candle_sequencing; "
    "import crypto_signal_engine.domain.state_contract; "
    "import crypto_signal_engine.providers.base; "
    "import crypto_signal_engine.quality.base; "
    "import crypto_signal_engine.safety.models; "
    "assert crypto_signal_engine.ALLOW_LIVE_TRADING is False; "
    "print('import smoke test OK')"
)


def run_step(title: str, cmd: list[str], cwd: Path | None = None) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
    result = subprocess.run(cmd, cwd=str(cwd or REPO_ROOT))
    if result.returncode != 0:
        print(f"\nFAIL: {title} (exit code {result.returncode})")
        sys.exit(result.returncode)
    print(f"PASS: {title}")


def find_prebuilt_wheel() -> Path:
    """Repo ile teslim edilen önceden-inşa edilmiş wheel'i bulur.

    Bulunamazsa DETERMİNİSTİK olarak fail eder (build backend çağırarak
    yeniden inşa etmeye ÇALIŞMAZ — bu, tam olarak ağ/setuptools
    bağımlılığını yeniden getirir).
    """
    print(f"\n{'=' * 70}\n1. Prebuilt wheel kontrolü\n{'=' * 70}")
    candidates = sorted(glob.glob(str(DIST_DIR / "crypto_signal_engine-*-py3-none-any.whl")))
    if not candidates:
        print(
            f"\nFAIL: Önceden inşa edilmiş wheel bulunamadı: "
            f"{DIST_DIR}/crypto_signal_engine-*-py3-none-any.whl\n"
            f"Bu dosya repo teslimatının bir parçası olmalıdır."
        )
        sys.exit(1)
    wheel_path = Path(candidates[-1])
    print(f"PASS: wheel bulundu: {wheel_path.name}")
    return wheel_path


def install_wheel_into_isolated_venv(wheel_path: Path) -> str:
    """Wheel'i tamamen izole (system_site_packages=False) bir venv'e
    `--no-index --no-deps` ile OFFLINE kurar. Build backend çağrılmaz,
    ağ erişimi gerekmez, ana ortamın setuptools durumu sonucu etkilemez.
    """
    print(f"\n{'=' * 70}\n2. Wheel'i izole venv'e offline kurulum\n{'=' * 70}")
    tmp_dir = tempfile.mkdtemp(prefix="phase1_verify_venv_")
    venv.create(tmp_dir, with_pip=True, system_site_packages=False)
    venv_python = str(Path(tmp_dir) / "bin" / "python")

    result = subprocess.run(
        [venv_python, "-m", "pip", "install", "--no-index", "--no-deps", "--quiet", str(wheel_path)],
    )
    if result.returncode != 0:
        print(f"\nFAIL: offline wheel kurulumu (exit code {result.returncode})")
        sys.exit(result.returncode)
    print("PASS: wheel izole venv'e offline kuruldu")
    return venv_python


def main() -> None:
    wheel_path = find_prebuilt_wheel()
    venv_python = install_wheel_into_isolated_venv(wheel_path)

    run_step(
        "3. External-directory import smoke test (izole venv python'ı ile)",
        [venv_python, "-c", IMPORT_SCRIPT],
        cwd=REPO_ROOT.parent,
    )

    run_step(
        "4. Full pytest suite",
        [sys.executable, "-m", "pytest", "tests/", "-q"],
    )

    run_step(
        "5. compileall",
        [sys.executable, "-m", "compileall", "crypto_signal_engine", "tests", "-q"],
    )

    run_step(
        "6. Repository safety scan",
        [sys.executable, "-m", "pytest", "tests/test_repository_safety_scan.py", "-q"],
    )

    print(f"\n{'=' * 70}\nTÜM ADIMLAR BAŞARILI — PHASE 1 VERIFICATION: PASS\n{'=' * 70}")


if __name__ == "__main__":
    main()


