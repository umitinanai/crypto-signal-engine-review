<!-- adaptive_wiring.md — 9 files -->
<!-- Contents of this part: -->
<!--   - scripts/run_with_adaptive_policy.py (14404 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_replay_sanity.py (9330 bytes) -->
<!--   - adaptive/windows.py (4828 bytes) -->
<!--   - adaptive/store.py (13638 bytes) -->
<!--   - adaptive/symbol_score.py (5917 bytes) -->
<!--   - research/replay.py (21810 bytes) -->
<!--   - research/monte_carlo.py (4632 bytes) -->
<!--   - research/attribution.py (10561 bytes) -->
<!--   - research/drift.py (3982 bytes) -->

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


=== FILE: crypto_signal_engine/execution/lifecycle_replay_sanity.py ===
"""
Autonomous Testnet trading lifecycle Phase 20 — replay sanity check for
the ATR-based stop/target/trailing exit policy.

ARCHITECTURAL PLACEMENT (why this lives in `execution/`, not `research/`):
`research/` has a hard, repo-wide, automated boundary (`tests/
test_repository_safety_scan_research.py::
test_research_package_never_imports_execution_package`) that it must
NEVER import anything from `execution/` — this module needs
`execution.lifecycle`'s pure `evaluate_candle`/`ExitPolicyConfig`
machinery, so it lives here instead. The dependency is one-directional
and safe: this module reads `research.replay`'s OUTPUT (a completed,
already-accepted `ReplayResult`), it does not modify or wrap
`research/`'s own logic, and `research/` still has zero awareness this
module exists.

ARCHITECTURE (no second backtest engine, no second signal implementation):
this module does NOT reimplement replay/signal/paper-trading logic — it is
a thin analysis layer over an ALREADY-COMPLETED `research.replay.
HistoricalReplayDriver` run:
- Candidate bridge entries are read directly from that replay's own
  `ReplayResult.cycle_results` (every genuine FLAT->LONG PAPER transition
  the accepted Quant/Consensus/Risk pipeline produced) — no signal is
  invented here.
- Exit simulation for each candidate entry calls
  `crypto_signal_engine.execution.lifecycle.evaluate_candle()` — the
  EXACT SAME pure function `lifecycle_runtime.py` calls for every live M1
  candle — over REAL subsequent M1 candles fetched via the same
  `HistoricalCandleSource` contract the replay driver itself uses.
- The entry-time ATR reference is computed with the same pure
  `features.candle_calculators.atr` calculator FeatureEngine uses,
  applied to a real M5 window ending at the entry.

SIMPLIFICATION (documented, not hidden): this tool holds the entry ATR
fixed for the life of each simulated trade (rather than re-fetching a
rolling "latest M5 ATR" on every M1 candle, which would require far more
REST calls for a sanity pass) — trailing distance is therefore computed
from a slightly-stale ATR reference in this tool only; live/production
`lifecycle_runtime.py` always uses the latest available M5 snapshot. This
does not affect the stop/target/lookahead correctness being sanity-checked
here, only the precision of the trailing distance.

`gross_pnl` here is a PER-UNIT price difference (exit_price - entry_price),
not scaled by any notional/quantity — this tool checks POLICY BEHAVIOR
(do exits happen at sane distances, is one exit reason dominating, does a
single extreme candle break things), not dollar P&L; Phase 20 explicitly
does not require profitability, only non-pathological behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
from crypto_signal_engine.execution.lifecycle import (
    ExitPolicyConfig,
    LifecyclePriceState,
    compute_initial_stop_and_target,
    evaluate_candle,
)
from crypto_signal_engine.features.candle_calculators import atr as compute_atr
from crypto_signal_engine.paper_trading.models import PositionSide
from research.replay import HistoricalCandleSource, ReplayResult

_ATR_PERIOD = 14


@dataclass(frozen=True)
class LifecycleTradeSimulation:
    symbol: str
    entry_time: datetime
    entry_price: float
    initial_protective_stop: float
    take_profit: float
    exit_time: datetime | None
    exit_reason: str | None  # None -> still open at the end of the bounded exit_window
    exit_price: float | None
    gross_pnl_per_unit: float | None
    holding_seconds: float | None


@dataclass(frozen=True)
class LifecycleSanityReport:
    trade_count: int  # completed (exited) simulations only
    open_at_end_count: int
    exit_reason_distribution: dict[str, int]
    total_gross_pnl_per_unit: float
    win_count: int
    loss_count: int
    win_rate: float | None
    average_holding_seconds: float | None
    max_drawdown_per_unit: float | None
    simulations: tuple[LifecycleTradeSimulation, ...]


def _to_lifecycle_candle(candle) -> LifecycleCandle:  # noqa: ANN001
    return LifecycleCandle(open=candle.open, high=candle.high, low=candle.low, close=candle.close, close_time=candle.close_time)


async def _entry_atr(candle_source: HistoricalCandleSource, symbol: str, *, entry_time: datetime) -> float | None:
    lookback = timedelta(minutes=5) * (_ATR_PERIOD + 6)
    m5_candles = await candle_source.fetch_historical_candles(symbol, Timeframe.M5, entry_time - lookback, entry_time)
    closed = [c for c in m5_candles if c.is_closed]
    if len(closed) < _ATR_PERIOD + 1:
        return None
    return compute_atr(closed, period=_ATR_PERIOD)


async def simulate_lifecycle_exits(
    replay_result: ReplayResult,
    *,
    candle_source: HistoricalCandleSource,
    exit_policy: ExitPolicyConfig,
    exit_window: timedelta,
) -> LifecycleSanityReport:
    """Runs the shared `evaluate_candle` exit policy over real M1 data for
    every genuine FLAT->LONG entry the replay's accepted pipeline
    produced. `exit_window` bounds how far forward each simulated trade is
    allowed to run before being counted as still-open (never an unbounded
    fetch)."""
    simulations: list[LifecycleTradeSimulation] = []
    previous_side: dict[str, PositionSide] = {}

    for cycle_result in replay_result.cycle_results:
        if not cycle_result.evaluated:
            continue
        symbol = cycle_result.symbol
        side = cycle_result.paper_result.position.side
        prior = previous_side.get(symbol, PositionSide.FLAT)
        previous_side[symbol] = side
        if not (prior is PositionSide.FLAT and side is PositionSide.LONG):
            continue

        entry_time = cycle_result.signal.timestamp
        entry_price = cycle_result.paper_result.position.average_entry_price

        atr_value = await _entry_atr(candle_source, symbol, entry_time=entry_time)
        if atr_value is None:
            continue  # cannot establish a trustworthy entry-time volatility snapshot -- skip, not a hard failure

        stop, target = compute_initial_stop_and_target(entry_price=entry_price, atr=atr_value, config=exit_policy)
        state = LifecyclePriceState(
            entry_price=entry_price, entry_timestamp=entry_time, initial_protective_stop=stop,
            take_profit=target, high_water=entry_price, effective_stop=stop, trailing_active=False,
            last_stop_mechanism="STOP_LOSS",
        )

        m1_candles = await candle_source.fetch_historical_candles(
            symbol, Timeframe.M1, entry_time, entry_time + exit_window
        )
        exit_reason: str | None = None
        exit_time: datetime | None = None
        exit_price: float | None = None
        for candle in m1_candles:
            if not candle.is_closed or candle.close_time <= entry_time:
                continue  # no-lookahead guard (Phase 8) -- identical discipline to live/lifecycle_manager
            result = evaluate_candle(state, _to_lifecycle_candle(candle), exit_policy, atr_for_trailing=atr_value)
            state = result.updated_state
            if result.exit_reason is not None:
                exit_reason = result.exit_reason.value
                exit_time = candle.close_time
                exit_price = candle.close
                break

        gross_pnl = (exit_price - entry_price) if exit_price is not None else None
        holding = (exit_time - entry_time).total_seconds() if exit_time is not None else None
        simulations.append(
            LifecycleTradeSimulation(
                symbol=symbol, entry_time=entry_time, entry_price=entry_price,
                initial_protective_stop=stop, take_profit=target, exit_time=exit_time,
                exit_reason=exit_reason, exit_price=exit_price, gross_pnl_per_unit=gross_pnl,
                holding_seconds=holding,
            )
        )

    completed = [s for s in simulations if s.exit_reason is not None]
    distribution: dict[str, int] = {}
    for s in completed:
        distribution[s.exit_reason] = distribution.get(s.exit_reason, 0) + 1  # type: ignore[index]
    wins = [s for s in completed if s.gross_pnl_per_unit is not None and s.gross_pnl_per_unit > 0]
    losses = [s for s in completed if s.gross_pnl_per_unit is not None and s.gross_pnl_per_unit < 0]
    total = sum(s.gross_pnl_per_unit for s in completed if s.gross_pnl_per_unit is not None)

    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for s in completed:
        running += s.gross_pnl_per_unit or 0.0
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)

    return LifecycleSanityReport(
        trade_count=len(completed),
        open_at_end_count=len(simulations) - len(completed),
        exit_reason_distribution=distribution,
        total_gross_pnl_per_unit=total,
        win_count=len(wins), loss_count=len(losses),
        win_rate=(len(wins) / len(completed)) if completed else None,
        average_holding_seconds=(sum(s.holding_seconds for s in completed if s.holding_seconds) / len(completed)) if completed else None,
        max_drawdown_per_unit=max_dd if completed else None,
        simulations=tuple(simulations),
    )


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


=== FILE: adaptive/store.py ===
"""
Adaptive Intelligence v1, step 12 — append-only, restart-durable
persistence for the full policy version history, current champion, and
the full decision log across all three evidence classes.

Mirrors `crypto_signal_engine.execution.lifecycle_store.LifecycleStore`'s
existing SQLite discipline (own dedicated connection, WAL mode,
`CREATE TABLE IF NOT EXISTS`, explicit column-by-column mapping — never
`SELECT *` reconstructed straight into a dataclass) rather than importing
it, keeping `adaptive/` a standalone, additive package with its own
dedicated database file (never sharing a file with, or writing a single
byte into, any table `crypto_signal_engine/` owns — see step 15's state-
protection requirement).

Four tables, all append-only (rows are never UPDATEd or DELETEd, matching
`bridge_fee_ledger`/`bridge_completed_trade`'s own append-only
discipline):

- `adaptive_policy_version`: every `PolicySnapshot` ever created
  (challenger or champion), keyed by its own `version_id`.
- `adaptive_champion_history`: one row per promotion (including the
  initial bootstrap champion) — "the current champion" is simply the
  most recent row here, never a separately-tracked mutable pointer.
- `adaptive_decision_log`: one row per `adaptive.decision.decide()` call
  ever made, regardless of outcome (PROMOTE/KEEP_CHAMPION/
  INSUFFICIENT_EVIDENCE all logged identically) — the three evidence
  classes are stored as three SEPARATE JSON blobs, never merged into one.
- `adaptive_cursor`: a tiny generic key-value table for restart-durable
  scalars this package needs to remember across restarts (currently just
  `adaptive.windows`'s `last_confirmation_end`)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from adaptive.decision import DecisionResult, EvidenceSummary
from adaptive.policy import PolicySnapshot
from adaptive.shadow import ShadowOutcome
from crypto_signal_engine.execution.lifecycle import ExitReason

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS adaptive_policy_version (
    version_id TEXT PRIMARY KEY,
    stop_atr_multiple REAL NOT NULL,
    take_profit_atr_multiple REAL NOT NULL,
    trailing_activation_atr_multiple REAL NOT NULL,
    trailing_distance_atr_multiple REAL NOT NULL,
    max_hold_hours REAL NOT NULL,
    created_at TEXT NOT NULL,
    provenance TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_champion_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL,
    promoted_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adaptive_champion_history_time ON adaptive_champion_history(promoted_at);

CREATE TABLE IF NOT EXISTS adaptive_decision_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    challenger_version_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    discovery_challenger_json TEXT NOT NULL,
    discovery_champion_json TEXT NOT NULL,
    confirmation_challenger_json TEXT NOT NULL,
    confirmation_champion_json TEXT NOT NULL,
    shadow_challenger_json TEXT NOT NULL,
    shadow_champion_json TEXT NOT NULL,
    drift_note TEXT
);
CREATE INDEX IF NOT EXISTS idx_adaptive_decision_log_time ON adaptive_decision_log(recorded_at);

CREATE TABLE IF NOT EXISTS adaptive_cursor (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_shadow_eligible (
    version_id TEXT PRIMARY KEY,
    marked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_shadow_outcome (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    challenger_version_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exit_reason TEXT NOT NULL,
    exit_price REAL NOT NULL,
    exit_time TEXT NOT NULL,
    gross_pnl_per_unit REAL NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adaptive_shadow_outcome_version ON adaptive_shadow_outcome(challenger_version_id);
"""


def _iso(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _evidence_to_json(evidence: EvidenceSummary) -> str:
    return json.dumps(asdict(evidence))


def _evidence_from_json(text: str) -> EvidenceSummary:
    return EvidenceSummary(**json.loads(text))


class AdaptiveStore:
    """Opens its OWN dedicated connection to its OWN dedicated database
    file — never the same file as `LifecycleStore`/`ExecutionStateStore`/
    `PaperStateStore` (see step 15: this package must not alter the
    schema or content of any existing persisted state)."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        with self._connection:
            self._connection.executescript(_CREATE_SCHEMA_SQL)

    def close(self) -> None:
        self._connection.close()

    # -- adaptive_policy_version ---------------------------------------------

    def record_policy_version(self, snapshot: PolicySnapshot) -> None:
        """Append-only: `INSERT OR IGNORE` makes re-recording an
        already-known `version_id` a safe no-op (idempotent under
        crash-and-retry), never an overwrite of a differently-valued row
        under the same id."""
        with self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO adaptive_policy_version (
                    version_id, stop_atr_multiple, take_profit_atr_multiple,
                    trailing_activation_atr_multiple, trailing_distance_atr_multiple, max_hold_hours,
                    created_at, provenance
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.version_id, snapshot.stop_atr_multiple, snapshot.take_profit_atr_multiple,
                    snapshot.trailing_activation_atr_multiple, snapshot.trailing_distance_atr_multiple,
                    snapshot.max_hold_hours, _iso(snapshot.created_at), snapshot.provenance,
                ),
            )

    def load_policy_version(self, version_id: str) -> PolicySnapshot | None:
        row = self._connection.execute(
            "SELECT * FROM adaptive_policy_version WHERE version_id = ?", (version_id,)
        ).fetchone()
        if row is None:
            return None
        return PolicySnapshot(
            version_id=row["version_id"], stop_atr_multiple=row["stop_atr_multiple"],
            take_profit_atr_multiple=row["take_profit_atr_multiple"],
            trailing_activation_atr_multiple=row["trailing_activation_atr_multiple"],
            trailing_distance_atr_multiple=row["trailing_distance_atr_multiple"],
            max_hold_hours=row["max_hold_hours"], created_at=_parse_dt(row["created_at"]),
            provenance=row["provenance"],
        )

    def list_policy_versions(self) -> tuple[PolicySnapshot, ...]:
        rows = self._connection.execute("SELECT version_id FROM adaptive_policy_version ORDER BY created_at ASC").fetchall()
        return tuple(self.load_policy_version(row["version_id"]) for row in rows)  # type: ignore[misc]

    # -- adaptive_champion_history --------------------------------------------

    def promote_champion(self, snapshot: PolicySnapshot, *, reason: str, now: datetime) -> None:
        """Records the policy version (idempotent) AND appends a new
        champion-history row — "the current champion" is always just the
        latest row here, never a separately-tracked mutable pointer that
        could drift out of sync with the version history."""
        self.record_policy_version(snapshot)
        with self._connection:
            self._connection.execute(
                "INSERT INTO adaptive_champion_history (version_id, promoted_at, reason) VALUES (?, ?, ?)",
                (snapshot.version_id, _iso(now), reason),
            )

    def current_champion(self) -> PolicySnapshot | None:
        row = self._connection.execute(
            "SELECT version_id FROM adaptive_champion_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return self.load_policy_version(row["version_id"])

    def champion_history(self) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT version_id, promoted_at, reason FROM adaptive_champion_history ORDER BY id ASC"
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- adaptive_decision_log -------------------------------------------------

    def record_decision(self, result: DecisionResult, *, now: datetime, drift_note: str | None = None) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_decision_log (
                    recorded_at, challenger_version_id, decision, reason,
                    discovery_challenger_json, discovery_champion_json,
                    confirmation_challenger_json, confirmation_champion_json,
                    shadow_challenger_json, shadow_champion_json, drift_note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _iso(now), result.challenger_version_id, result.decision.value, result.reason,
                    _evidence_to_json(result.discovery_challenger), _evidence_to_json(result.discovery_champion),
                    _evidence_to_json(result.confirmation_challenger), _evidence_to_json(result.confirmation_champion),
                    _evidence_to_json(result.shadow_challenger), _evidence_to_json(result.shadow_champion),
                    drift_note,
                ),
            )

    def recent_decisions(self, limit: int = 50) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT * FROM adaptive_decision_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- adaptive_cursor (generic key-value, e.g. windows.py's last_confirmation_end) ---

    def get_cursor(self, name: str) -> str | None:
        row = self._connection.execute("SELECT value FROM adaptive_cursor WHERE name = ?", (name,)).fetchone()
        return row["value"] if row is not None else None

    def set_cursor(self, name: str, value: str, *, now: datetime) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_cursor (name, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (name, value, _iso(now)),
            )

    # -- adaptive_shadow_eligible ----------------------------------------------

    def mark_shadow_eligible(self, version_id: str, *, now: datetime) -> None:
        """A challenger that cleared discovery + confirmation (step 9) is
        marked here — restart-durable, so a live runtime that restarts
        mid-shadow-observation does not lose track of which challengers
        it was supposed to be shadowing."""
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO adaptive_shadow_eligible (version_id, marked_at) VALUES (?, ?)",
                (version_id, _iso(now)),
            )

    def list_shadow_eligible(self) -> tuple[str, ...]:
        rows = self._connection.execute(
            "SELECT version_id FROM adaptive_shadow_eligible ORDER BY marked_at ASC"
        ).fetchall()
        return tuple(row["version_id"] for row in rows)

    # -- adaptive_shadow_outcome -------------------------------------------------

    def record_shadow_outcome(self, challenger_version_id: str, *, symbol: str, outcome: ShadowOutcome, now: datetime) -> None:
        """Append-only: EVERY completed shadow trade for a challenger is
        kept, across restarts — `adaptive.decision.shadow_evidence_from_
        outcomes` accumulates evidence across however many of these a
        challenger has collected so far, never just the most recent one."""
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_shadow_outcome (
                    challenger_version_id, symbol, exit_reason, exit_price, exit_time,
                    gross_pnl_per_unit, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    challenger_version_id, symbol, outcome.exit_reason.value, outcome.exit_price,
                    _iso(outcome.exit_time), outcome.gross_pnl_per_unit, _iso(now),
                ),
            )

    def shadow_outcomes_for(self, challenger_version_id: str) -> tuple[ShadowOutcome, ...]:
        rows = self._connection.execute(
            "SELECT * FROM adaptive_shadow_outcome WHERE challenger_version_id = ? ORDER BY id ASC",
            (challenger_version_id,),
        ).fetchall()
        return tuple(
            ShadowOutcome(
                exit_reason=ExitReason(row["exit_reason"]), exit_price=row["exit_price"],
                exit_time=_parse_dt(row["exit_time"]), gross_pnl_per_unit=row["gross_pnl_per_unit"],
            )
            for row in rows
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


=== FILE: research/replay.py ===
"""
Part B/C/D — Deterministic Historical Replay.

ARCHITECTURE (no second strategy implementation): this module NEVER
reimplements FeatureEngine/SignalEngine/agent/consensus/risk/paper-
trading logic. It fetches historical candles (reusing the accepted
`fetch_historical_candles` contract), validates them (`research.
data_quality`), and feeds them through a freshly-constructed, unmodified
`crypto_signal_engine.runtime.coordinator.RuntimeCoordinator` using
exactly its PUBLIC, synchronous, already-accepted methods:
`bootstrap_candles()` for warmup and `ingest_candle()` /
`ingest_order_book()` for the scored region — the SAME methods the live
async `run()` loop calls per event (see `coordinator.py`'s own
docstring), and the same pattern `crypto_signal_engine.stability.
harness.SoakHarness` already uses (with synthetic data) as the accepted
Phase 9 precedent for driving the coordinator synchronously outside its
`run()` loop.

MULTI-TIMEFRAME EVENT ORDERING POLICY (Section C — critical):
Replay availability is governed by CLOSE time (when a candle's
information genuinely became available), not open time. Because Binance
kline boundaries are epoch-aligned, every M15 close coincides exactly
with an M5 close, and every H1 close coincides exactly with both an M15
and an M5 close (60 % 15 == 0, 15 % 5 == 0) — so grouping events by
identical `close_time` is well-defined and produces one group per M5
close, each optionally also carrying an M15 and/or H1 candle.

Within one close-time group, events are applied in a FIXED, documented
order, independent of how the underlying per-timeframe lists were
concatenated:

    H1 candle (if present in this group)
    -> M15 candle (if present in this group)
    -> synthetic order-book snapshot (Section D; always present in a
       group that has an M5 candle, since OrderBookAgent evidence is
       required for every evaluation)
    -> M5 candle (triggers `RuntimeCoordinator.ingest_candle`'s signal
       evaluation, per accepted Phase 6 behaviour, timeframe==M5 only)

Rationale: at a shared close instant T, an H1/M15 candle that closes at
T is genuine, already-realised information as of T (it did not come
from the future) — it is not look-ahead to apply it before the same-
instant M5 evaluation. Leaving the tie-break to accidental input-list
order would instead make the replay result depend on an arbitrary
artifact (whichever timeframe's list happened to be iterated first),
which this module treats as a defect to prevent, not tolerate. Groups
themselves are always applied in strictly ascending `close_time` order,
so no event from group T+1 is ever applied before group T is fully
drained — this is the structural no-look-ahead guarantee, independent
of and in addition to the no-look-ahead assertions already enforced
inside `FeatureEngine`/`PaperTradingEngine`.

ORDER-BOOK LIMITATION (Section D — do not fake parity): Binance public
REST has no historical depth/order-book endpoint. `OrderBookAgent`
materially contributes to Phase 4 consensus, and `RuntimeCoordinator`
requires at least one accepted order-book feature snapshot before a
symbol is marked READY. This module therefore injects a DETERMINISTIC,
CLEARLY-LABELLED SYNTHETIC order-book snapshot once per close-time group
that contains an M5 candle, derived purely from that candle's own
OHLC (same spirit as the accepted Phase 9 `SoakHarness.
make_deterministic_order_book`, but seeded from real historical price
data rather than a sine wave). Every `ReplayResult` carries an explicit
`order_book_provenance` field and a `limitations` tuple stating this
plainly — a profitable synthetic-order-book replay MUST NOT be read as
proof of historical live-equivalent edge. `OrderBookProvenance.
REAL_RECORDED` exists in the enum for forward compatibility with Section
E's future real capture, but this module never produces it today.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol

from crypto_signal_engine.domain._validation import normalize_symbol, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import PaperPosition
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import (
    DEFAULT_WARMUP_CANDLES,
    MIN_REQUIRED_WARMUP_CANDLES,
    RuntimeCoordinator,
)
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.data_quality import DataQualityReport, HistoricalDatasetValidator
from research.errors import ReplayConfigurationError, ReplayIntegrityError

_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}

# Fixed application-order rank within one close-time group — lower runs
# first. This is the ONE documented deterministic tie-break rule
# (Section C). The order-book synthetic event is always ranked
# immediately before the M5 candle so OrderBookAgent evidence is fresh
# at the instant of evaluation.
_TIER_RANK: dict[str, int] = {"H1": 0, "M15": 1, "ORDER_BOOK": 2, "M5": 3}

REPLAY_TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


class HistoricalCandleSource(Protocol):
    """The only capability replay needs from a data source — a strict
    subset of `crypto_signal_engine.providers.base.LiveDataProvider`
    (which also demands streaming methods replay never uses). Both a
    real `providers.binance.rest.BinanceRestClient` and a deterministic
    test fake satisfy this Protocol structurally."""

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]: ...


class OrderBookProvenance(str, Enum):
    """Where a replay's order-book evidence actually came from. See
    module docstring, Section D — never claim REAL_RECORDED without a
    genuine historical capture behind it (Section E)."""

    REAL_RECORDED = "REAL_RECORDED"
    SYNTHETIC = "SYNTHETIC"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class ReplayLimitation:
    code: str
    detail: str


_SYNTHETIC_OB_LIMITATION = ReplayLimitation(
    code="SYNTHETIC_ORDER_BOOK",
    detail=(
        "OrderBookAgent evidence in this replay is SYNTHETIC — deterministically "
        "derived from each M5 candle's own OHLC spread, NOT real recorded Binance "
        "depth (no historical depth endpoint exists). This replay is PARTIAL "
        "research, not proof of live-equivalent historical edge."
    ),
)

_FIXED_STRATEGY_LIMITATION = ReplayLimitation(
    code="FIXED_STRATEGY_WEIGHTS",
    detail=(
        "ConsensusEngine weights and RiskOverlay thresholds are fixed by design "
        "in this pass; this replay measures the existing strategy, it does not "
        "search or optimize any parameter."
    ),
)


@dataclass(frozen=True)
class ReplayResult:
    """Full, immutable result of one deterministic historical replay run."""

    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    order_book_provenance: OrderBookProvenance
    data_quality_reports: tuple[DataQualityReport, ...]
    cycle_results: tuple[RuntimeCycleResult, ...]
    skipped_evaluation_count: int
    final_positions: tuple[PaperPosition, ...]
    limitations: tuple[ReplayLimitation, ...]

    # Plain class constant (deliberately NOT a dataclass field — no type
    # annotation, so it is excluded from __init__/repr/eq entirely):
    # replay is NEVER live-equivalent (Section D). This exists so a
    # caller/report can assert `ReplayResult.LIVE_EQUIVALENT is False`
    # instead of relying on prose alone.
    LIVE_EQUIVALENT = False

    def __post_init__(self) -> None:
        require_utc_aware(self.start, "start")
        require_utc_aware(self.end, "end")
        if self.skipped_evaluation_count < 0:
            raise ValueError("skipped_evaluation_count negatif olamaz")

    @property
    def evaluated_cycle_results(self) -> tuple[RuntimeCycleResult, ...]:
        return tuple(r for r in self.cycle_results if r.evaluated)


def synthetic_order_book_from_candle(
    candle: Candle, *, spread_fraction: float = 0.0015, level_quantity: float = 5.0
) -> OrderBookSnapshot:
    """Deterministic, pure function of ONE closed candle — no randomness,
    no external state. Explicitly SYNTHETIC (see module docstring,
    Section D): a fixed nominal spread derived from the candle's own
    high-low range around its close price. Two price levels per side,
    matching the shape of the accepted Phase 9
    `SoakHarness.make_deterministic_order_book` fixture."""
    mid = candle.close
    band = max(candle.high - candle.low, mid * 1e-6)
    spread = max(band * spread_fraction, mid * 1e-6)
    return OrderBookSnapshot(
        symbol=candle.symbol,
        timestamp=candle.close_time,
        bids=(
            OrderBookLevel(price=mid - spread, quantity=level_quantity),
            OrderBookLevel(price=mid - spread * 2, quantity=level_quantity),
        ),
        asks=(
            OrderBookLevel(price=mid + spread, quantity=level_quantity),
            OrderBookLevel(price=mid + spread * 2, quantity=level_quantity),
        ),
        last_update_id=int(candle.close_time.timestamp()),
    )


@dataclass(frozen=True)
class _PlannedEvent:
    """One internal, ordered unit of replay application — never exposed
    outside this module."""

    close_time: datetime
    tier: str
    symbol: str
    timeframe: Timeframe | None
    candle: Candle | None
    order_book: OrderBookSnapshot | None


def build_deterministic_event_plan(
    symbol: str, candles_by_timeframe: dict[Timeframe, Sequence[Candle]]
) -> tuple[_PlannedEvent, ...]:
    """Pure function: groups candles by `close_time` and orders each
    group per the fixed tie-break policy (module docstring, Section C).
    Deterministic regardless of the relative order the per-timeframe
    sequences were supplied in — callers may pass the three sequences in
    any dict-key order, or even reversed internally; the returned plan
    is identical because it is derived purely from `(close_time, tier)`
    sort keys, never from input position."""
    normalized = normalize_symbol(symbol)
    events: list[_PlannedEvent] = []
    for timeframe, candles in candles_by_timeframe.items():
        tier = {Timeframe.H1: "H1", Timeframe.M15: "M15", Timeframe.M5: "M5"}[timeframe]
        for candle in candles:
            events.append(
                _PlannedEvent(
                    close_time=candle.close_time, tier=tier, symbol=normalized,
                    timeframe=timeframe, candle=candle, order_book=None,
                )
            )

    # One synthetic order-book event per group that carries an M5 candle
    # (module docstring, Section D) — M5 is the finest configured
    # granularity, so (given a gap-free, validated M5 series) every
    # close-time group has exactly one M5 candle.
    for event in list(events):
        if event.tier == "M5" and event.candle is not None:
            events.append(
                _PlannedEvent(
                    close_time=event.close_time, tier="ORDER_BOOK", symbol=normalized,
                    timeframe=None, candle=None,
                    order_book=synthetic_order_book_from_candle(event.candle),
                )
            )

    events.sort(key=lambda e: (e.close_time, _TIER_RANK[e.tier]))

    # Defensive, explicit no-look-ahead proof (Section C — "regression
    # tests ... prove no candle is visible before it closed"): the sort
    # above is trusted, but this re-walks the result and asserts it is
    # monotonically non-decreasing by close_time. A violation here would
    # mean this function's own logic is broken, not the input data —
    # raised as an integrity error rather than silently continuing.
    previous_close: datetime | None = None
    for event in events:
        if previous_close is not None and event.close_time < previous_close:
            raise ReplayIntegrityError(
                f"event plan not monotonic in close_time: {event.close_time} < {previous_close}"
            )
        previous_close = event.close_time

    return tuple(events)


_LOOKBACK_SAFETY_FACTOR = 2  # mirrors RuntimeCoordinator's own private `_lookback_window` factor


def _lookback_window(timeframe: Timeframe, warmup_candles: int) -> timedelta:
    """Deliberately duplicated, tiny (one-line) constant relationship —
    NOT a reimplementation of runtime logic. `RuntimeCoordinator.
    _lookback_window` is module-private (leading underscore) and cannot
    be imported; replay must reproduce the same safety-margin arithmetic
    to fetch enough pre-`start` history for each timeframe to reach
    `warmup_candles`, exactly as the accepted `bootstrap()` does."""
    return _TIMEFRAME_DURATIONS[timeframe] * warmup_candles * _LOOKBACK_SAFETY_FACTOR


@dataclass(frozen=True)
class ReplayConfig:
    warmup_candles: int = DEFAULT_WARMUP_CANDLES
    candle_window_size: int = 500
    notional_per_position: float = 1000.0
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    model_version: str | None = None
    data_quality_validator: HistoricalDatasetValidator = field(
        default_factory=HistoricalDatasetValidator
    )

    def __post_init__(self) -> None:
        if self.warmup_candles < MIN_REQUIRED_WARMUP_CANDLES:
            raise ReplayConfigurationError(
                f"warmup_candles must be >= {MIN_REQUIRED_WARMUP_CANDLES} "
                f"(RuntimeCoordinator's own Phase 3/4 minimum-history requirement)"
            )


class HistoricalReplayDriver:
    """Deterministic historical replay/backtest harness.

    Reuses, unmodified: `providers.binance.rest.BinanceRestClient.
    fetch_historical_candles` (or a structurally-compatible fake),
    `providers.binance.clock.FixedClock`, `runtime.coordinator.
    RuntimeCoordinator.bootstrap_candles()`/`ingest_candle()`/
    `ingest_order_book()`, `paper_trading.engine.PaperTradingEngine`.
    Never reimplements FeatureEngine/SignalEngine/agent/consensus/risk
    logic — see module docstring.

    A fresh `RuntimeCoordinator` + `PaperTradingEngine` +
    `FeatureHistoryStore` is constructed on every `run()` call — no state
    is ever shared or carried over between replay runs (Section F's
    rolling OOS windows depend on this for leak-free, independent
    per-window state).
    """

    def __init__(self, *, candle_source: HistoricalCandleSource, config: ReplayConfig | None = None) -> None:
        self._candle_source = candle_source
        self._config = config or ReplayConfig()

    async def run(
        self,
        *,
        symbols: tuple[str, ...],
        start: datetime,
        end: datetime,
        paper_engine: PaperTradingEngine | None = None,
    ) -> ReplayResult:
        """Replays `[start, end)` for `symbols`. `paper_engine`, if
        given, MUST be a freshly-constructed engine (or a structurally
        compatible wrapper — see `research.sizing.SizedPaperTradingEngine`)
        with no prior state; passing one lets a caller opt into
        portfolio/risk-based sizing (Section I/J) without this module
        needing to know anything about that policy. When omitted, a
        fresh, default (fixed-notional) `PaperTradingEngine` is built
        from `self._config`, matching Phase 5's accepted default
        behaviour exactly."""
        if not symbols:
            raise ReplayConfigurationError("symbols boş olamaz")
        require_utc_aware(start, "start")
        require_utc_aware(end, "end")
        if start >= end:
            raise ReplayConfigurationError(f"start ({start}) end'den ({end}) küçük olmalı")

        normalized_symbols = tuple(dict.fromkeys(normalize_symbol(s) for s in symbols))
        clock = FixedClock(end)
        history_store = FeatureHistoryStore()
        feature_engine = FeatureEngine(history_store=history_store)
        engine = paper_engine or PaperTradingEngine(
            notional_per_position=self._config.notional_per_position,
            fee_bps=self._config.fee_bps,
            slippage_bps=self._config.slippage_bps,
        )
        coordinator = RuntimeCoordinator(
            symbols=normalized_symbols,
            provider=_NullProvider(),
            candle_timeframes=REPLAY_TIMEFRAMES,
            warmup_candles=self._config.warmup_candles,
            candle_window_size=self._config.candle_window_size,
            history_store=history_store,
            feature_engine=feature_engine,
            paper_engine=engine,
            model_version=self._config.model_version,
            clock=clock,
        )

        data_quality_reports: list[DataQualityReport] = []
        all_planned_events: list[_PlannedEvent] = []

        for symbol in normalized_symbols:
            candles_by_timeframe: dict[Timeframe, list[Candle]] = {}
            for timeframe in REPLAY_TIMEFRAMES:
                fetch_start = start - _lookback_window(timeframe, self._config.warmup_candles)
                fetched = await self._candle_source.fetch_historical_candles(
                    symbol, timeframe, fetch_start, end
                )
                report = self._config.data_quality_validator.validate_or_raise(
                    fetched, symbol=symbol, timeframe=timeframe
                )
                data_quality_reports.append(report)
                candles_by_timeframe[timeframe] = fetched

            for timeframe in REPLAY_TIMEFRAMES:
                warmup_slice = [c for c in candles_by_timeframe[timeframe] if c.open_time < start]
                coordinator.bootstrap_candles(symbol, timeframe, warmup_slice, as_of=start)

            scored_by_timeframe = {
                timeframe: [c for c in candles if c.open_time >= start]
                for timeframe, candles in candles_by_timeframe.items()
            }
            all_planned_events.extend(build_deterministic_event_plan(symbol, scored_by_timeframe))

        # Cross-symbol merge: stable-sorted purely by (close_time, tier) —
        # relative ordering between DIFFERENT symbols never affects
        # correctness (RuntimeCoordinator isolates state per symbol), so
        # this is a safe, simple global ordering for a single
        # chronological event log.
        all_planned_events.sort(key=lambda e: (e.close_time, _TIER_RANK[e.tier]))

        cycle_results: list[RuntimeCycleResult] = []
        skipped = 0
        for event in all_planned_events:
            if event.tier == "ORDER_BOOK":
                assert event.order_book is not None
                coordinator.ingest_order_book(event.symbol, event.order_book)
                continue
            assert event.candle is not None and event.timeframe is not None
            processed = coordinator.ingest_candle(event.symbol, event.timeframe, event.candle)
            if processed.cycle_result is not None:
                if processed.cycle_result.evaluated:
                    cycle_results.append(processed.cycle_result)
                else:
                    skipped += 1

        final_positions = tuple(
            pos for pos in (engine.position(symbol) for symbol in normalized_symbols) if pos is not None
        )

        return ReplayResult(
            symbols=normalized_symbols,
            start=start,
            end=end,
            order_book_provenance=OrderBookProvenance.SYNTHETIC,
            data_quality_reports=tuple(data_quality_reports),
            cycle_results=tuple(cycle_results),
            skipped_evaluation_count=skipped,
            final_positions=final_positions,
            limitations=(_SYNTHETIC_OB_LIMITATION, _FIXED_STRATEGY_LIMITATION),
        )


class _NullProvider:
    """`RuntimeCoordinator` requires a `LiveDataProvider` at construction
    time even though replay never calls its streaming methods (only
    `bootstrap_candles()`/`ingest_candle()`/`ingest_order_book()` are
    ever invoked directly — see `run()` above). This stub exists purely
    to satisfy that constructor signature; calling any of its methods
    is a programming error in this module and fails loudly rather than
    silently returning fake data."""

    async def stream_candles(self, symbol: str, timeframe: Timeframe):  # pragma: no cover
        raise AssertionError("HistoricalReplayDriver must never stream — bootstrap/ingest only")
        yield  # type: ignore[unreachable]

    async def stream_trades(self, symbol: str):  # pragma: no cover
        raise AssertionError("HistoricalReplayDriver must never stream — bootstrap/ingest only")
        yield  # type: ignore[unreachable]

    async def stream_order_book(self, symbol: str, depth: int):  # pragma: no cover
        raise AssertionError("HistoricalReplayDriver must never stream — bootstrap/ingest only")
        yield  # type: ignore[unreachable]

    async def fetch_historical_candles(self, symbol, timeframe, start, end):  # pragma: no cover
        raise AssertionError("HistoricalReplayDriver fetches via its own candle_source, not the provider stub")

    def health(self):  # pragma: no cover
        raise AssertionError("_NullProvider.health() must never be called by replay")

    async def close(self) -> None:  # pragma: no cover
        return None


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


