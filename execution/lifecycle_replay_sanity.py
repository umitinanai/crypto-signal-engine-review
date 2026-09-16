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
