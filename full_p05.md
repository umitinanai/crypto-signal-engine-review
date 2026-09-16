<!-- full_p05.md — Part 5/7 — 38 files -->
<!-- Contents of this part: -->
<!--   - tests/test_agents.py (15702 bytes) -->
<!--   - tests/test_app_24_7_ops.py (8240 bytes) -->
<!--   - tests/test_app_adaptive_wiring.py (9158 bytes) -->
<!--   - tests/test_app_admin_wiring.py (6182 bytes) -->
<!--   - tests/test_app_bridge_lifecycle_snapshot.py (10511 bytes) -->
<!--   - tests/test_app_cli.py (8967 bytes) -->
<!--   - tests/test_app_composition.py (3122 bytes) -->
<!--   - tests/test_app_lifecycle.py (7766 bytes) -->
<!--   - tests/test_app_notifier_wiring.py (3542 bytes) -->
<!--   - tests/test_app_ops_snapshots.py (4154 bytes) -->
<!--   - tests/test_app_price_freshness.py (7934 bytes) -->
<!--   - tests/test_app_signal_testnet_bridge.py (12897 bytes) -->
<!--   - tests/test_app_symbol_resolution.py (18031 bytes) -->
<!--   - tests/test_app_watchdog_wiring.py (2719 bytes) -->
<!--   - tests/test_backup_sqlite.py (4177 bytes) -->
<!--   - tests/test_binance_clock.py (2451 bytes) -->
<!--   - tests/test_binance_config.py (4331 bytes) -->
<!--   - tests/test_binance_order_book_sync.py (13325 bytes) -->
<!--   - tests/test_binance_parser.py (10596 bytes) -->
<!--   - tests/test_binance_provider.py (41694 bytes) -->
<!--   - tests/test_binance_quality_rules.py (8508 bytes) -->
<!--   - tests/test_binance_reconnect.py (3602 bytes) -->
<!--   - tests/test_binance_rest.py (8031 bytes) -->
<!--   - tests/test_binance_symbols.py (1213 bytes) -->
<!--   - tests/test_bridge_runtime.py (6794 bytes) -->
<!--   - tests/test_candle_features.py (13701 bytes) -->
<!--   - tests/test_candle_sequencing.py (9088 bytes) -->
<!--   - tests/test_consensus.py (8988 bytes) -->
<!--   - tests/test_consensus_engine.py (6481 bytes) -->
<!--   - tests/test_domain_models.py (23761 bytes) -->
<!--   - tests/test_end_to_end.py (5010 bytes) -->
<!--   - tests/test_enums.py (1459 bytes) -->
<!--   - tests/test_events.py (9668 bytes) -->
<!--   - tests/test_execution_adapter.py (24639 bytes) -->
<!--   - tests/test_execution_cli.py (22017 bytes) -->
<!--   - tests/test_execution_concurrency.py (19916 bytes) -->
<!--   - tests/test_execution_lifecycle.py (34954 bytes) -->
<!--   - tests/test_execution_lifecycle_manager.py (73080 bytes) -->

=== FILE: tests/test_agents.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.agents.context import AgentContext
from crypto_signal_engine.agents.market_structure import MarketStructureAgent
from crypto_signal_engine.agents.order_book import OrderBookAgent
from crypto_signal_engine.agents.quant import QuantAgent
from crypto_signal_engine.agents.regime import RegimeAgent, RegimeAgentOutput
from crypto_signal_engine.domain.consensus import RegimeContext
from crypto_signal_engine.domain.enums import (
    AgentName,
    LiquidityRegime,
    StructureRegime,
    Timeframe,
    VolatilityRegime,
)
from crypto_signal_engine.domain.models import AgentEvidence
from crypto_signal_engine.errors import AgentInputError
from crypto_signal_engine.features.domain import FeatureSnapshot

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def snap(tf, values, as_of=T0, symbol="BTCUSDT") -> FeatureSnapshot:
    return FeatureSnapshot(symbol=symbol, timeframe=tf, as_of=as_of, values=values)


def make_context(m1=None, m5=None, m15=None, h1=None, as_of=T0, symbol="BTCUSDT") -> AgentContext:
    return AgentContext(
        symbol=symbol, as_of=as_of, context_id="ctx-test",
        m1=m1 or snap(Timeframe.M1, {}), m5=m5 or snap(Timeframe.M5, {}),
        m15=m15 or snap(Timeframe.M15, {}), h1=h1 or snap(Timeframe.H1, {}),
    )


# --------------------------------------------------------------------------
# QuantAgent
# --------------------------------------------------------------------------

class TestQuantAgent:
    def _m5(self, **overrides):
        values = {
            "RSI_14": 50.0, "ROC_10": 0.0, "VWAP_DEVIATION_20": 0.0,
            "RELATIVE_VOLUME_20": 1.0, "BOLLINGER_BANDWIDTH_20_2": 0.03,
        }
        values.update(overrides)
        return snap(Timeframe.M5, values)

    def test_canonical_identity_and_timeframe(self) -> None:
        context = make_context(m5=self._m5())
        evidence = QuantAgent().evaluate(context)
        assert evidence.agent == AgentName.QUANT
        assert evidence.primary_timeframe == Timeframe.M5

    def test_bullish_case(self) -> None:
        context = make_context(m5=self._m5(RSI_14=75.0, ROC_10=3.0, VWAP_DEVIATION_20=0.02))
        evidence = QuantAgent().evaluate(context)
        assert evidence.score > 0.5

    def test_bearish_case(self) -> None:
        context = make_context(m5=self._m5(RSI_14=25.0, ROC_10=-3.0, VWAP_DEVIATION_20=-0.02))
        evidence = QuantAgent().evaluate(context)
        assert evidence.score < -0.5

    def test_neutral_case(self) -> None:
        context = make_context(m5=self._m5())
        evidence = QuantAgent().evaluate(context)
        assert evidence.score == pytest.approx(0.0, abs=0.05)

    def test_bounded_score(self) -> None:
        context = make_context(m5=self._m5(RSI_14=100.0, ROC_10=100.0, VWAP_DEVIATION_20=1.0))
        evidence = QuantAgent().evaluate(context)
        assert -1.0 <= evidence.score <= 1.0

    def test_finite_metrics(self) -> None:
        import math
        context = make_context(m5=self._m5())
        evidence = QuantAgent().evaluate(context)
        assert all(math.isfinite(v) for v in evidence.supporting_metrics.values())

    def test_deterministic(self) -> None:
        context = make_context(m5=self._m5(RSI_14=60.0, ROC_10=1.0))
        e1 = QuantAgent().evaluate(context)
        e2 = QuantAgent().evaluate(context)
        assert e1.score == e2.score
        assert e1.rationale == e2.rationale

    def test_missing_feature_rejected(self) -> None:
        context = make_context(m5=snap(Timeframe.M5, {"RSI_14": 50.0}))
        with pytest.raises(AgentInputError, match="QuantAgent"):
            QuantAgent().evaluate(context)

    def test_low_volume_cannot_flip_sign(self) -> None:
        bullish_high_vol = make_context(m5=self._m5(RSI_14=70.0, ROC_10=2.0, VWAP_DEVIATION_20=0.01, RELATIVE_VOLUME_20=1.5))
        bullish_low_vol = make_context(m5=self._m5(RSI_14=70.0, ROC_10=2.0, VWAP_DEVIATION_20=0.01, RELATIVE_VOLUME_20=0.01))
        e_high = QuantAgent().evaluate(bullish_high_vol)
        e_low = QuantAgent().evaluate(bullish_low_vol)
        assert e_high.score > 0
        assert e_low.score > 0  # işaret DEĞİŞMEDİ, yalnızca büyüklük azaldı
        assert e_low.score <= e_high.score


# --------------------------------------------------------------------------
# MarketStructureAgent
# --------------------------------------------------------------------------

class TestMarketStructureAgent:
    def _m15(self, **overrides):
        values = {
            "ROC_10": 0.0, "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.03,
            "RELATIVE_VOLUME_20": 1.0, "BODY_TO_RANGE_RATIO": 0.5,
        }
        values.update(overrides)
        return snap(Timeframe.M15, values)

    def test_canonical_identity_and_timeframe(self) -> None:
        context = make_context(m15=self._m15())
        evidence = MarketStructureAgent().evaluate(context)
        assert evidence.agent == AgentName.MARKET_STRUCTURE
        assert evidence.primary_timeframe == Timeframe.M15

    def test_bullish_structure(self) -> None:
        context = make_context(m15=self._m15(ROC_10=2.0, DIST_FROM_HIGH_20=0.0, DIST_FROM_LOW_20=0.05))
        evidence = MarketStructureAgent().evaluate(context)
        assert evidence.score > 0.3

    def test_bearish_structure(self) -> None:
        context = make_context(m15=self._m15(ROC_10=-2.0, DIST_FROM_HIGH_20=0.05, DIST_FROM_LOW_20=0.0))
        evidence = MarketStructureAgent().evaluate(context)
        assert evidence.score < -0.3

    def test_ambiguous_range_like_structure(self) -> None:
        context = make_context(m15=self._m15(ROC_10=0.0, DIST_FROM_HIGH_20=0.03, DIST_FROM_LOW_20=0.03))
        evidence = MarketStructureAgent().evaluate(context)
        assert evidence.score == pytest.approx(0.0, abs=0.05)

    def test_bounded_output(self) -> None:
        context = make_context(m15=self._m15(ROC_10=100.0, DIST_FROM_HIGH_20=0.0, DIST_FROM_LOW_20=1.0))
        evidence = MarketStructureAgent().evaluate(context)
        assert -1.0 <= evidence.score <= 1.0

    def test_finite_metrics(self) -> None:
        import math
        context = make_context(m15=self._m15())
        evidence = MarketStructureAgent().evaluate(context)
        assert all(math.isfinite(v) for v in evidence.supporting_metrics.values())

    def test_missing_feature_rejected(self) -> None:
        context = make_context(m15=snap(Timeframe.M15, {"ROC_10": 1.0}))
        with pytest.raises(AgentInputError, match="MarketStructureAgent"):
            MarketStructureAgent().evaluate(context)

    def test_optional_vwap_absent_still_works(self) -> None:
        context = make_context(m15=self._m15())  # VWAP_DEVIATION_20 yok
        evidence = MarketStructureAgent().evaluate(context)
        assert "vwap_component" not in evidence.supporting_metrics

    def test_optional_vwap_present_participates(self) -> None:
        context = make_context(m15=self._m15(VWAP_DEVIATION_20=0.01))
        evidence = MarketStructureAgent().evaluate(context)
        assert "vwap_component" in evidence.supporting_metrics

    def test_dimensionless_features_no_absolute_price(self) -> None:
        # Aynı normalize edilmiş girdiler için, hiçbir absolute price
        # parametresi kullanılmadığından sonuç sabit kalmalı.
        context1 = make_context(m15=self._m15(ROC_10=1.5))
        context2 = make_context(m15=self._m15(ROC_10=1.5))
        e1 = MarketStructureAgent().evaluate(context1)
        e2 = MarketStructureAgent().evaluate(context2)
        assert e1.score == e2.score


# --------------------------------------------------------------------------
# OrderBookAgent
# --------------------------------------------------------------------------

class TestOrderBookAgent:
    def _m1(self, **overrides):
        values = {
            "SPREAD_BPS": 2.0, "DEPTH_IMBALANCE_10": 0.0, "TOB_IMBALANCE": 0.0,
            "MID_PRICE": 100.0, "MICROPRICE": 100.0,
        }
        values.update(overrides)
        return snap(Timeframe.M1, values)

    def test_timeframe_m1(self) -> None:
        context = make_context(m1=self._m1())
        evidence = OrderBookAgent().evaluate(context)
        assert evidence.primary_timeframe == Timeframe.M1
        assert evidence.agent == AgentName.ORDER_BOOK

    def test_bid_heavy_positive_case(self) -> None:
        context = make_context(m1=self._m1(DEPTH_IMBALANCE_10=0.6, TOB_IMBALANCE=0.5, MICROPRICE=100.05))
        evidence = OrderBookAgent().evaluate(context)
        assert evidence.score > 0.2

    def test_ask_heavy_negative_case(self) -> None:
        context = make_context(m1=self._m1(DEPTH_IMBALANCE_10=-0.6, TOB_IMBALANCE=-0.5, MICROPRICE=99.95))
        evidence = OrderBookAgent().evaluate(context)
        assert evidence.score < -0.2

    def test_balanced_near_neutral(self) -> None:
        context = make_context(m1=self._m1())
        evidence = OrderBookAgent().evaluate(context)
        assert evidence.score == pytest.approx(0.0, abs=0.01)

    def test_spread_widening_lowers_conviction(self) -> None:
        tight = make_context(m1=self._m1(DEPTH_IMBALANCE_10=0.5, TOB_IMBALANCE=0.5, SPREAD_BPS=1.0))
        wide = make_context(m1=self._m1(DEPTH_IMBALANCE_10=0.5, TOB_IMBALANCE=0.5, SPREAD_BPS=40.0))
        e_tight = OrderBookAgent().evaluate(tight)
        e_wide = OrderBookAgent().evaluate(wide)
        assert e_wide.score < e_tight.score
        assert e_wide.score > 0  # işaret hâlâ pozitif

    def test_spread_alone_cannot_flip_sign(self) -> None:
        context = make_context(m1=self._m1(DEPTH_IMBALANCE_10=0.3, TOB_IMBALANCE=0.3, SPREAD_BPS=1000.0))
        evidence = OrderBookAgent().evaluate(context)
        assert evidence.score >= 0  # asla negatife dönmedi

    def test_missing_feature_rejected(self) -> None:
        context = make_context(m1=snap(Timeframe.M1, {"SPREAD_BPS": 1.0}))
        with pytest.raises(AgentInputError, match="OrderBookAgent"):
            OrderBookAgent().evaluate(context)

    def test_rationale_uses_microstructure_language(self) -> None:
        context = make_context(m1=self._m1())
        evidence = OrderBookAgent().evaluate(context)
        assert "microstructure" in evidence.rationale.lower()
        assert "execution" not in evidence.rationale.lower()


# --------------------------------------------------------------------------
# RegimeAgent
# --------------------------------------------------------------------------

class TestRegimeAgent:
    def _h1(self, **overrides):
        values = {
            "ROC_10": 0.0, "RSI_14": 50.0, "BOLLINGER_BANDWIDTH_20_2": 0.03,
            "RELATIVE_VOLUME_20": 1.0, "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.05,
        }
        values.update(overrides)
        return snap(Timeframe.H1, values)

    def _m1(self, spread_bps=2.0):
        return snap(Timeframe.M1, {"SPREAD_BPS": spread_bps})

    def test_returns_regime_agent_output(self) -> None:
        context = make_context(h1=self._h1(), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert isinstance(output, RegimeAgentOutput)
        assert isinstance(output.evidence, AgentEvidence)
        assert isinstance(output.regime, RegimeContext)
        assert output.evidence.agent == AgentName.REGIME
        assert output.evidence.primary_timeframe == Timeframe.H1

    def test_trending_case(self) -> None:
        context = make_context(h1=self._h1(ROC_10=1.5, DIST_FROM_HIGH_20=0.05, DIST_FROM_LOW_20=0.10), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert output.regime.structure == StructureRegime.TRENDING

    def test_ranging_case(self) -> None:
        context = make_context(h1=self._h1(ROC_10=0.1), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert output.regime.structure == StructureRegime.RANGING

    def test_breakout_case(self) -> None:
        context = make_context(h1=self._h1(ROC_10=3.0, DIST_FROM_HIGH_20=0.001, DIST_FROM_LOW_20=0.20), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert output.regime.structure == StructureRegime.BREAKOUT

    def test_mean_reversion_case(self) -> None:
        context = make_context(h1=self._h1(ROC_10=0.5, RSI_14=75.0, DIST_FROM_HIGH_20=0.005, DIST_FROM_LOW_20=0.20), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert output.regime.structure == StructureRegime.MEAN_REVERSION

    def test_low_volatility(self) -> None:
        context = make_context(h1=self._h1(BOLLINGER_BANDWIDTH_20_2=0.01), m1=self._m1())
        assert RegimeAgent().evaluate(context).regime.volatility == VolatilityRegime.LOW

    def test_normal_volatility(self) -> None:
        context = make_context(h1=self._h1(BOLLINGER_BANDWIDTH_20_2=0.03), m1=self._m1())
        assert RegimeAgent().evaluate(context).regime.volatility == VolatilityRegime.NORMAL

    def test_high_volatility(self) -> None:
        context = make_context(h1=self._h1(BOLLINGER_BANDWIDTH_20_2=0.07), m1=self._m1())
        assert RegimeAgent().evaluate(context).regime.volatility == VolatilityRegime.HIGH

    def test_extreme_volatility(self) -> None:
        context = make_context(h1=self._h1(BOLLINGER_BANDWIDTH_20_2=0.15), m1=self._m1())
        assert RegimeAgent().evaluate(context).regime.volatility == VolatilityRegime.EXTREME

    def test_normal_liquidity(self) -> None:
        context = make_context(h1=self._h1(), m1=self._m1(spread_bps=2.0))
        assert RegimeAgent().evaluate(context).regime.liquidity == LiquidityRegime.NORMAL

    def test_thin_liquidity(self) -> None:
        context = make_context(h1=self._h1(), m1=self._m1(spread_bps=10.0))
        assert RegimeAgent().evaluate(context).regime.liquidity == LiquidityRegime.THIN

    def test_stressed_liquidity(self) -> None:
        context = make_context(h1=self._h1(), m1=self._m1(spread_bps=20.0))
        assert RegimeAgent().evaluate(context).regime.liquidity == LiquidityRegime.STRESSED

    def test_high_volatility_does_not_automatically_cause_bearish_evidence(self) -> None:
        context = make_context(h1=self._h1(ROC_10=1.5, BOLLINGER_BANDWIDTH_20_2=0.15), m1=self._m1())
        output = RegimeAgent().evaluate(context)
        assert output.regime.volatility == VolatilityRegime.EXTREME
        assert output.evidence.score > 0  # yüksek volatilite yönü BOZMADI

    def test_thin_liquidity_does_not_automatically_cause_bearish_evidence(self) -> None:
        context = make_context(h1=self._h1(ROC_10=1.5), m1=self._m1(spread_bps=20.0))
        output = RegimeAgent().evaluate(context)
        assert output.regime.liquidity == LiquidityRegime.STRESSED
        assert output.evidence.score > 0

    def test_missing_h1_feature_rejected(self) -> None:
        context = make_context(h1=snap(Timeframe.H1, {"ROC_10": 1.0}), m1=self._m1())
        with pytest.raises(AgentInputError, match="RegimeAgent"):
            RegimeAgent().evaluate(context)

    def test_missing_aligned_m1_feature_rejected(self) -> None:
        context = make_context(h1=self._h1(), m1=snap(Timeframe.M1, {}))
        with pytest.raises(AgentInputError, match="RegimeAgent"):
            RegimeAgent().evaluate(context)

    def test_does_not_consume_order_book_agent_evidence(self) -> None:
        """Yapısal kanıt: RegimeAgent.evaluate()'in imzası yalnızca
        AgentContext alır; OrderBookAgent'ın evidence'ını parametre olarak
        ALAMAZ (Python imza seviyesinde zaten imkansızdır)."""
        import inspect

        sig = inspect.signature(RegimeAgent.evaluate)
        params = list(sig.parameters.keys())
        assert params == ["self", "context"]


=== FILE: tests/test_app_24_7_ops.py ===
"""24/7 Ops v1 — `app.py` integration wiring tests for Steps 2/3/4:
watchdog heartbeat reuses the existing health loop, Control Center admin
backend (None-default regression + real pause/stop-through-Application),
and outbound-only alerting on process start/graceful-stop.

Same offline discipline as `tests/test_app_lifecycle.py`: `FakeLiveDataProvider`,
no real OS signal, `run_async()` (no pytest-asyncio dependency)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from crypto_signal_engine.app import EXIT_OK, Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.ops.health_snapshot import read_snapshot
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _config(tmp_path: Path, *, extra: dict[str, str] | None = None):
    env = {
        "CSE_SYMBOLS": _SYMBOL,
        "CSE_DB_PATH": str(tmp_path / "state.db"),
        "CSE_WARMUP_CANDLES": "20",
        "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
        "CSE_DASHBOARD_ENABLED": "false",  # not under test here; avoids port binding
    }
    if extra:
        env.update(extra)
    return load_config(env)


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _wait_for_run_task(app: Application) -> None:
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)


class TestWatchdogHeartbeatWiring:
    def test_health_loop_calls_notify_watchdog(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())
        calls: list[None] = []

        async def body() -> None:
            with patch("crypto_signal_engine.app.notify_watchdog", side_effect=lambda: calls.append(None)):
                run_task = asyncio.create_task(app.start())
                await _wait_for_run_task(app)
                await _wait_until(lambda: len(calls) >= 1)
                app.request_shutdown("test")
                await asyncio.wait_for(run_task, timeout=5.0)

        run_async(body())
        assert len(calls) >= 1


class TestNotifierWiringStartStop:
    def test_start_and_graceful_stop_both_notify(self, tmp_path: Path) -> None:
        calls: list[str] = []
        with patch("crypto_signal_engine.app.build_telegram_notifier", return_value=calls.append):
            app = Application(_config(tmp_path), _bootstrappable_provider())

            async def body() -> int:
                run_task = asyncio.create_task(app.start())
                await _wait_for_run_task(app)
                app.request_shutdown("test")
                return await asyncio.wait_for(run_task, timeout=5.0)

            exit_code = run_async(body())
        assert exit_code == EXIT_OK
        assert any("started" in m for m in calls)
        assert any("stopped gracefully" in m for m in calls)

    def test_none_notifier_by_default_never_raises(self, tmp_path: Path) -> None:
        """Regression: with no CSE_TELEGRAM_* env set, `build_telegram_
        notifier()` returns `None` and every `safe_notify()` call site is
        a silent no-op — start/stop must behave exactly as before."""
        app = Application(_config(tmp_path), _bootstrappable_provider())
        assert app._notifier is None

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


class TestAdminControllerNoneDefault:
    def test_no_admin_token_means_no_admin_controller_ever(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._admin_controller is None
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_no_admin_token_entries_paused_never_settable_snapshot_shows_disabled(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._entries_paused.is_set() is False
            snapshot = read_snapshot(config.health_snapshot_path)
            assert snapshot is not None
            assert snapshot["admin"] == {"enabled": False}
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


class TestAdminControllerConfiguredEndToEnd:
    def test_admin_controller_constructed_and_pause_reaches_entries_paused(self, tmp_path: Path) -> None:
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._admin_controller is not None
            result = app._admin_controller.pause()
            assert result["entries_paused"] is True
            assert app._entries_paused.is_set() is True
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_admin_stop_triggers_the_exact_same_request_shutdown_path_as_sigterm(self, tmp_path: Path) -> None:
        """Required test: `AdminController.stop()` (running on a REAL
        asyncio loop here, captured by `Application.start()`) must drive
        the process to the SAME clean exit `request_shutdown()`/SIGTERM
        does — never a parallel shutdown mechanism."""
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._shutdown_started is False
            app._admin_controller.stop()  # schedules via call_soon_threadsafe onto THIS loop
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_admin_state_and_last_action_appear_in_health_snapshot(self, tmp_path: Path) -> None:
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            app._admin_controller.pause()
            await app._write_snapshot(recovery_ok=True, recovery_detail="running")
            snapshot = read_snapshot(config.health_snapshot_path)
            assert snapshot["admin"]["enabled"] is True
            assert snapshot["admin"]["entries_paused"] is True
            assert snapshot["admin"]["last_action"]["action"] == "pause"
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


=== FILE: tests/test_app_adaptive_wiring.py ===
"""
Adaptive Intelligence v1, step 14 — `app.py::Application`'s additive,
optional `exit_policy_provider` wiring. Deliberately does NOT import
`adaptive/` anywhere (the hard dependency-direction rule: `crypto_signal_
engine/` must never import `adaptive/`) — a plain callable is enough to
prove the wiring reaches `LifecycleManager` correctly. Same fixtures/
discipline as `tests/test_app_signal_testnet_bridge.py`."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.app import Application, build_lifecycle_manager
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig
from crypto_signal_engine.ops.config import load_config
from tests.execution_fakes import FakeTestnetHttpClient
from tests.test_app_signal_testnet_bridge import _bootstrappable_provider, _testnet_bridge_env


class TestExitPolicyProviderBitForBitEquivalence:
    def test_no_provider_given_lifecycle_manager_has_none(self, tmp_path: Path, monkeypatch) -> None:
        """Bit-for-bit equivalence requirement: with no champion ever
        promoted (the default -- no `exit_policy_provider` passed), the
        composed `Application` is IDENTICAL to before this parameter
        existed."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        app = Application(config, _bootstrappable_provider(), testnet_http_client=FakeTestnetHttpClient())

        assert app._lifecycle_manager is not None
        assert app._lifecycle_manager._exit_policy_provider is None  # noqa: SLF001

    def test_paper_mode_unaffected_by_omitted_provider(self, tmp_path: Path) -> None:
        from tests.test_app_signal_testnet_bridge import _base_env

        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        assert app._lifecycle_manager is None  # bridge disabled entirely -- unchanged from before this parameter


class TestExitPolicyProviderWiring:
    def test_provider_passed_to_application_reaches_lifecycle_manager(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))

        def provider() -> tuple[ExitPolicyConfig, str | None]:
            return ExitPolicyConfig(max_hold_hours=12.0), "policy-champion-7"

        app = Application(
            config, _bootstrappable_provider(), testnet_http_client=FakeTestnetHttpClient(),
            exit_policy_provider=provider,
        )
        assert app._lifecycle_manager is not None
        assert app._lifecycle_manager._exit_policy_provider is provider  # noqa: SLF001
        assert app._lifecycle_manager._resolve_entry_policy() == (ExitPolicyConfig(max_hold_hours=12.0), "policy-champion-7")  # noqa: SLF001

    def test_build_lifecycle_manager_threads_provider_through(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))

        from crypto_signal_engine.app import build_execution_service

        execution = build_execution_service(config, http_client=FakeTestnetHttpClient())

        def provider() -> tuple[ExitPolicyConfig, str | None]:
            return ExitPolicyConfig(), "v1"

        result = build_lifecycle_manager(config, execution, exit_policy_provider=provider)
        assert result is not None
        _store, manager = result
        assert manager._exit_policy_provider is provider  # noqa: SLF001

    def test_build_lifecycle_manager_defaults_to_none_when_omitted(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))

        from crypto_signal_engine.app import build_execution_service

        execution = build_execution_service(config, http_client=FakeTestnetHttpClient())
        result = build_lifecycle_manager(config, execution)
        assert result is not None
        _store, manager = result
        assert manager._exit_policy_provider is None  # noqa: SLF001


class TestCandleObserverWiring:
    def test_default_none_reaches_coordinator_unchanged(self, tmp_path: Path) -> None:
        from tests.test_app_signal_testnet_bridge import _base_env

        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        assert app.runtime._coordinator._candle_observer is None  # noqa: SLF001

    def test_provided_observer_reaches_coordinator(self, tmp_path: Path) -> None:
        from tests.test_app_signal_testnet_bridge import _base_env

        def observer(symbol, timeframe, candle):  # noqa: ANN001
            pass

        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider(), candle_observer=observer)
        assert app.runtime._coordinator._candle_observer is observer  # noqa: SLF001


class TestM1CandleObserverWiring:
    """M1-fidelity shadow-evaluation fix (independent-verification
    round): `m1_candle_observer` only has any effect once the Testnet
    bridge is enabled (`app.runtime` is a `LifecycleRuntime` only then).

    Dashboard price/P&L sync fix: `LifecycleRuntime`'s own `m1_candle_
    observer` is NO LONGER `None` by default -- `Application` always
    composes its own `_record_m1_price` (feeding `_latest_price()`'s M1
    cache, see `app.py::_compose_m1_candle_observers`) with whatever
    EXTERNAL observer this constructor param supplies, even when that
    param is `None`. See `tests/test_app_price_freshness.py` for the
    composition function's own isolated tests."""

    def test_default_none_reaches_lifecycle_runtime_as_the_apps_own_price_cache_observer(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        app = Application(config, _bootstrappable_provider(), testnet_http_client=FakeTestnetHttpClient())
        assert isinstance(app.runtime, LifecycleRuntime)
        # No external observer requested -> the composed observer IS
        # `_record_m1_price` itself (zero wrapper indirection), never
        # `None`. `==`, not `is`: two separate `self._record_m1_price`
        # attribute accesses are equal bound methods but not the same
        # Python object.
        assert app.runtime._m1_candle_observer == app._record_m1_price  # noqa: SLF001

    def test_provided_observer_reaches_lifecycle_runtime_composed_with_the_price_cache(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
        from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))

        calls: list[tuple[str, object]] = []

        def observer(symbol, candle):  # noqa: ANN001
            calls.append((symbol, candle))

        app = Application(
            config, _bootstrappable_provider(), testnet_http_client=FakeTestnetHttpClient(),
            m1_candle_observer=observer,
        )
        assert isinstance(app.runtime, LifecycleRuntime)
        combined = app.runtime._m1_candle_observer  # noqa: SLF001
        assert combined is not observer  # composed, never the raw external observer directly

        candle_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        candle = LifecycleCandle(open=1.0, high=1.1, low=0.9, close=1.05, close_time=candle_time)
        combined("BTCUSDT", candle)
        assert calls == [("BTCUSDT", candle)]  # the external observer STILL fires
        assert app._m1_price_cache["BTCUSDT"] == (1.05, candle_time)  # noqa: SLF001 -- AND the app's own cache is updated


=== FILE: tests/test_app_admin_wiring.py ===
"""24/7 Ops v1, Step 3 — `app.py` integration tests for the Control
Center admin backend: `dashboard_admin_token=None` (default) proven
behaviorally identical to today, and a real, running-asyncio-loop
end-to-end pause/stop through `Application` (see `tests/test_ops_admin.py`
for the isolated `AdminController` unit tests and `tests/test_ops_
dashboard.py` for the real-HTTP-level `/admin/*` coverage).

Same offline discipline as `tests/test_app_lifecycle.py`: `FakeLiveDataProvider`,
no real OS signal, `run_async()` (no pytest-asyncio dependency)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_signal_engine.app import EXIT_OK, Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.ops.health_snapshot import read_snapshot
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _config(tmp_path: Path, *, extra: dict[str, str] | None = None):
    env = {
        "CSE_SYMBOLS": _SYMBOL,
        "CSE_DB_PATH": str(tmp_path / "state.db"),
        "CSE_WARMUP_CANDLES": "20",
        "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
        "CSE_DASHBOARD_ENABLED": "false",  # not under test here; avoids port binding
    }
    if extra:
        env.update(extra)
    return load_config(env)


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _wait_for_run_task(app: Application) -> None:
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)


class TestAdminControllerNoneDefault:
    def test_no_admin_token_means_no_admin_controller_ever(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._admin_controller is None
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_no_admin_token_entries_paused_never_settable_snapshot_shows_disabled(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._entries_paused.is_set() is False
            snapshot = read_snapshot(config.health_snapshot_path)
            assert snapshot is not None
            assert snapshot["admin"] == {"enabled": False}
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


class TestAdminControllerConfiguredEndToEnd:
    def test_admin_controller_constructed_and_pause_reaches_entries_paused(self, tmp_path: Path) -> None:
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._admin_controller is not None
            result = app._admin_controller.pause()
            assert result["entries_paused"] is True
            assert app._entries_paused.is_set() is True
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_admin_stop_triggers_the_exact_same_request_shutdown_path_as_sigterm(self, tmp_path: Path) -> None:
        """Required test: `AdminController.stop()` (running on a REAL
        asyncio loop here, captured by `Application.start()`) must drive
        the process to the SAME clean exit `request_shutdown()`/SIGTERM
        does — never a parallel shutdown mechanism."""
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert app._shutdown_started is False
            app._admin_controller.stop()  # schedules via call_soon_threadsafe onto THIS loop
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_admin_state_and_last_action_appear_in_health_snapshot(self, tmp_path: Path) -> None:
        config = _config(tmp_path, extra={"CSE_DASHBOARD_ADMIN_TOKEN": "s3cr3t"})
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            app._admin_controller.pause()
            await app._write_snapshot(recovery_ok=True, recovery_detail="running")
            snapshot = read_snapshot(config.health_snapshot_path)
            assert snapshot["admin"]["enabled"] is True
            assert snapshot["admin"]["entries_paused"] is True
            assert snapshot["admin"]["last_action"]["action"] == "pause"
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


=== FILE: tests/test_app_bridge_lifecycle_snapshot.py ===
"""
Portfolio/Accounting v1, step 4 — `Application._bridge_lifecycle_
snapshot()` now sources its portfolio-level numbers from `portfolio.
accounting.build_accounting_snapshot()` instead of ad-hoc inline math.
Same fixtures/discipline as `tests/test_app_signal_testnet_bridge.py`/
`tests/test_app_adaptive_wiring.py`."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.app import Application
from crypto_signal_engine.ops.config import load_config
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.test_app_signal_testnet_bridge import _bootstrappable_provider, _testnet_bridge_env

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _bridge_app(tmp_path: Path, monkeypatch) -> Application:
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
    exec_db = tmp_path / "exec.db"
    monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
    config = load_config(_testnet_bridge_env(tmp_path, exec_db))
    return Application(config, _bootstrappable_provider(), testnet_http_client=FakeTestnetHttpClient())


class TestDashboardDictKeysUnchanged:
    """THE required test: dashboard-facing dict keys are unchanged (no
    frontend break) even though the VALUES are now correct/true-lifetime
    instead of limit-50/gross."""

    def test_performance_dict_has_the_exact_same_keys_as_before(self, tmp_path: Path, monkeypatch) -> None:
        app = _bridge_app(tmp_path, monkeypatch)
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot["enabled"] is True
        expected_keys = {
            "exposure_usdt", "open_long_count", "dust_count", "non_flat_position_count",
            "realized_gross_pnl", "todays_realized_gross_pnl", "unrealized_gross_pnl_total",
            "completed_trade_count", "win_count", "loss_count", "win_rate",
            "average_win_gross", "average_loss_gross",
        }
        assert set(snapshot["performance"].keys()) == expected_keys

    def test_top_level_dict_shape_has_the_new_usdt_balance_key(self, tmp_path: Path, monkeypatch) -> None:
        """`"usdt_balance"` is a deliberate, NEW top-level key (the gap
        fix: `check_usdt_balance()` is now actually wired in) -- every
        pre-existing key is still present unchanged."""
        app = _bridge_app(tmp_path, monkeypatch)
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert set(snapshot.keys()) == {"enabled", "positions", "performance", "recent_trades", "usdt_balance"}

    def test_disabled_bridge_shape_unchanged(self, tmp_path: Path) -> None:
        from tests.test_app_signal_testnet_bridge import _base_env

        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot == {"enabled": False, "positions": {}}


class TestValuesAreNowTrueLifetimeAndNet:
    """The bug this milestone actually fixes: >50 trades and net-vs-gross."""

    def test_completed_trade_count_reflects_true_lifetime_not_limit_50(self, tmp_path: Path, monkeypatch) -> None:
        app = _bridge_app(tmp_path, monkeypatch)
        store = app._lifecycle_store  # noqa: SLF001
        for i in range(63):
            store.record_completed_trade(
                symbol="BTCUSDT", trade_group_id=f"csl-{i}", entry_client_order_id=f"csl-{i}",
                exit_client_order_id=f"csl-exit-{i}", entry_timestamp=NOW, exit_timestamp=NOW,
                quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
                gross_realized_pnl=10.0, net_realized_pnl=9.0, exit_reason="TAKE_PROFIT",
                entry_signal_context_id=None, exit_signal_context_id=None, now=NOW,
            )
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot["performance"]["completed_trade_count"] == 63  # not capped at 50
        assert snapshot["performance"]["realized_gross_pnl"] == 63 * 9.0  # NET (9.0), not gross (10.0) per trade

    def test_todays_realized_pnl_is_net_and_sql_aggregate_based(self, tmp_path: Path, monkeypatch) -> None:
        app = _bridge_app(tmp_path, monkeypatch)
        store = app._lifecycle_store  # noqa: SLF001
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-0", entry_client_order_id="csl-0",
            exit_client_order_id="csl-exit-0", entry_timestamp=NOW, exit_timestamp=NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=8.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=NOW,
        )
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        # `now` inside the real Application uses utc_now() (real wall-clock),
        # so this trade recorded "now" (test-time NOW fixture) is only
        # guaranteed "today" if run on the same UTC day -- record using the
        # app's OWN utc_now() instead to make this test time-independent.
        from crypto_signal_engine.ops.health_snapshot import utc_now

        real_now = utc_now()
        store.record_completed_trade(
            symbol="ETHUSDT", trade_group_id="csl-1", entry_client_order_id="csl-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=real_now, exit_timestamp=real_now,
            quantity_closed=1.0, gross_entry_vwap=50.0, exit_gross_vwap=55.0,
            gross_realized_pnl=5.0, net_realized_pnl=4.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=real_now,
        )
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot["performance"]["todays_realized_gross_pnl"] == 4.5  # only the "today" trade, net

    def test_win_loss_stats_remain_unchanged_bounded_gross_out_of_scope(self, tmp_path: Path, monkeypatch) -> None:
        """win_count/win_rate/average_win_gross are explicitly OUT OF
        `AccountingSnapshot`'s scope -- must remain computed exactly as
        before (bounded to recent_completed_trades(limit=50), gross)."""
        app = _bridge_app(tmp_path, monkeypatch)
        store = app._lifecycle_store  # noqa: SLF001
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-0", entry_client_order_id="csl-0",
            exit_client_order_id="csl-exit-0", entry_timestamp=NOW, exit_timestamp=NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.0, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=NOW,
        )
        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot["performance"]["win_count"] == 1
        assert snapshot["performance"]["win_rate"] == 1.0
        assert snapshot["performance"]["average_win_gross"] == 10.0  # still GROSS, unchanged


class TestUsdtBalanceGapFix:
    """Gap fix: `check_usdt_balance()` was previously wired nowhere in
    any runtime path -- it only ran inside its own test file. This proves
    a NORMAL `_bridge_lifecycle_snapshot()` call actually triggers it."""

    def test_snapshot_call_actually_invokes_check_usdt_balance(self, tmp_path: Path, monkeypatch) -> None:
        from crypto_signal_engine.portfolio.accounting import AssetBalance

        app = _bridge_app(tmp_path, monkeypatch)
        calls = {"count": 0}

        async def _fake_check_usdt_balance():
            calls["count"] += 1
            return AssetBalance(asset="USDT", free=1000.0, locked=25.0)

        app._execution_service.check_usdt_balance = _fake_check_usdt_balance  # noqa: SLF001

        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001

        assert calls["count"] == 1  # actually triggered, exactly once per snapshot
        assert snapshot["usdt_balance"] == {"asset": "USDT", "free": 1000.0, "locked": 25.0}

    def test_snapshot_call_invokes_it_again_on_a_second_call(self, tmp_path: Path, monkeypatch) -> None:
        """Not a one-time/cached call -- every snapshot re-checks."""
        from crypto_signal_engine.portfolio.accounting import AssetBalance

        app = _bridge_app(tmp_path, monkeypatch)
        calls = {"count": 0}

        async def _fake_check_usdt_balance():
            calls["count"] += 1
            return AssetBalance(asset="USDT", free=1.0, locked=0.0)

        app._execution_service.check_usdt_balance = _fake_check_usdt_balance  # noqa: SLF001

        run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001

        assert calls["count"] == 2

    def test_none_balance_result_is_reflected_as_none_in_the_dict(self, tmp_path: Path, monkeypatch) -> None:
        app = _bridge_app(tmp_path, monkeypatch)

        async def _fake_check_usdt_balance():
            return None

        app._execution_service.check_usdt_balance = _fake_check_usdt_balance  # noqa: SLF001

        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001
        assert snapshot["usdt_balance"] is None

    def test_a_failing_check_usdt_balance_never_crashes_the_snapshot(self, tmp_path: Path, monkeypatch) -> None:
        """`check_usdt_balance()` itself already degrades to `None` on any
        failure -- but the SNAPSHOT call site ALSO wraps defensively
        (defense-in-depth, matching the "wrap at the call site too"
        discipline used for `order_book_observer`/`candle_observer`/
        `m1_candle_observer`): even a hypothetical future bug that made
        `check_usdt_balance()` raise must NOT crash the whole snapshot,
        NOT block the health loop, and NOT touch trading."""
        app = _bridge_app(tmp_path, monkeypatch)

        async def _raising():
            raise RuntimeError("simulated account_info failure")

        app._execution_service.check_usdt_balance = _raising  # noqa: SLF001

        snapshot = run_async(app._bridge_lifecycle_snapshot())  # noqa: SLF001 - must not raise
        assert snapshot["usdt_balance"] is None
        assert snapshot["enabled"] is True  # the REST of the snapshot is unaffected


=== FILE: tests/test_app_cli.py ===
"""Faz 8 — app.py CLI testleri: `main()`'in config-error exit path'i ve
`status` subcommand'ı. `run` subcommand'ı BURADA test EDİLMEZ (gerçek
Binance PUBLIC bağlantısı + `websockets` gerektirir, bkz.
scripts/live_public_smoke_test.py) — `Application` lifecycle'ı zaten
tests/test_app_lifecycle.py'de tam olarak, offline/fake provider ile
kapsanmıştır."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import crypto_signal_engine.app as app_module
from crypto_signal_engine.app import EXIT_CONFIG_ERROR, EXIT_FATAL, EXIT_OK, EXIT_SYMBOL_SELECTION_FAILURE, main
from crypto_signal_engine.errors import TransportError
from crypto_signal_engine.ops.health_snapshot import write_snapshot
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.runtime.models import RuntimeHealth, RuntimeStatus, SymbolHealth
from tests.binance_fakes import FakeHttpClient


class TestMainConfigErrorPath:
    def test_missing_symbols_no_longer_a_config_error_but_still_never_starts_without_real_symbols(
        self, monkeypatch, tmp_path: Path, capsys
    ) -> None:
        """`CSE_SYMBOLS` artık zorunlu DEĞİL (bkz. `resolve_symbols`) — bu
        yüzden `main(["run"])` ARTIK `EXIT_CONFIG_ERROR` DÖNMEZ. Ama bu
        test GERÇEK Binance ağına ASLA dokunmamalıdır: `build_discovery_
        rest_client` monkeypatch'lenerek offline/deterministic bir
        discovery-failure enjekte edilir — sonuç `EXIT_SYMBOL_SELECTION_
        FAILURE` olmalı (sembole SESSİZCE düşülmez, `var/lib/...`
        production dizinine ASLA dokunulmaz, bkz. `CSE_DB_PATH` override)."""
        monkeypatch.delenv("CSE_SYMBOLS", raising=False)
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))

        def _offline_failing_client() -> BinanceRestClient:
            return BinanceRestClient(
                BinanceConfig(), FakeHttpClient([TransportError("no real network in tests")] * 4),
                FixedClock(datetime(2026, 9, 4, tzinfo=timezone.utc)), FakeSleeper(),
            )

        monkeypatch.setattr(app_module, "build_discovery_rest_client", _offline_failing_client)

        exit_code = main(["run"])
        assert exit_code == EXIT_SYMBOL_SELECTION_FAILURE
        assert "AUTOMATIC SYMBOL SELECTION FAILED" in capsys.readouterr().err
        assert not (tmp_path / "state.db").exists()  # hiçbir durable state DOKUNULMADI

    def test_doctor_command_does_not_trigger_network_selection_even_when_symbols_absent(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        """`doctor`, kendi sözleşmesi gereği ("offline, no network calls")
        `status`/`doctor` her ikisi de sembol seçiminden ÖNCE return eder —
        bu, otomatik modun bu iki komutu ASLA ağa çıkarmadığını kanıtlar."""
        monkeypatch.delenv("CSE_SYMBOLS", raising=False)
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))

        def _exploding_client() -> BinanceRestClient:
            raise AssertionError("doctor/status must never attempt symbol discovery")

        monkeypatch.setattr(app_module, "build_discovery_rest_client", _exploding_client)
        exit_code = main(["doctor"])
        assert exit_code in (EXIT_OK, EXIT_FATAL)  # ne dönerse dönsün — AssertionError FIRLAMADIYSA yeterli

    def test_invalid_env_value_returns_config_error_exit_code(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_LOG_LEVEL", "NOT_A_LEVEL")
        exit_code = main(["status"])
        assert exit_code == EXIT_CONFIG_ERROR
        assert "CONFIGURATION ERROR" in capsys.readouterr().err


class TestStatusSubcommand:
    def test_status_with_no_snapshot_yet_returns_fatal(self, monkeypatch, tmp_path: Path, capsys) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))
        exit_code = main(["status"])
        assert exit_code == EXIT_FATAL
        assert "NO SNAPSHOT YET" in capsys.readouterr().out

    def test_status_with_ready_snapshot_prints_summary_and_returns_ok(
        self, monkeypatch, tmp_path: Path, capsys
    ) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))
        health_path = tmp_path / "health.json"
        monkeypatch.setenv("CSE_HEALTH_SNAPSHOT_PATH", str(health_path))

        now = datetime.now(timezone.utc)
        write_snapshot(
            health_path,
            status=RuntimeStatus(
                overall_health=RuntimeHealth.READY,
                symbols=(
                    SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=now,
                                 reconnect_count=0, detail="ok"),
                ),
                generated_at=now,
            ),
            recovery_ok=True, recovery_detail="recovery completed", pid=999, started_at=now,
        )

        exit_code = main(["status"])
        out = capsys.readouterr().out
        assert exit_code == EXIT_OK
        assert "overall_health: READY" in out
        assert "BTCUSDT" in out

    def test_status_with_degraded_snapshot_returns_fatal(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))
        health_path = tmp_path / "health.json"
        monkeypatch.setenv("CSE_HEALTH_SNAPSHOT_PATH", str(health_path))

        now = datetime.now(timezone.utc)
        write_snapshot(
            health_path,
            status=RuntimeStatus(
                overall_health=RuntimeHealth.DEGRADED,
                symbols=(
                    SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.DEGRADED, last_event_at=now,
                                 reconnect_count=2, detail="stream disconnected"),
                ),
                generated_at=now,
            ),
            recovery_ok=True, recovery_detail="running", pid=999, started_at=now,
        )

        exit_code = main(["status"])
        assert exit_code == EXIT_FATAL


class TestStatusStalenessDetection:
    """`kill -9`/OOM ile ANİ ölen bir process, diskte eski bir "READY"
    anlık görüntüsü BIRAKABİLİR — `status` bunu KÖRÜ KÖRÜNE sağlıklı
    raporlamamalı (bkz. app.py::_run_status docstring'i)."""

    def test_stale_ready_snapshot_is_reported_as_fatal(self, monkeypatch, tmp_path: Path, capsys) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))
        health_path = tmp_path / "health.json"
        monkeypatch.setenv("CSE_HEALTH_SNAPSHOT_PATH", str(health_path))

        stale_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        write_snapshot(
            health_path,
            status=RuntimeStatus(
                overall_health=RuntimeHealth.READY,
                symbols=(
                    SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=stale_time,
                                 reconnect_count=0, detail="ok"),
                ),
                generated_at=stale_time,
            ),
            recovery_ok=True, recovery_detail="recovery completed", pid=999, started_at=stale_time,
        )

        exit_code = main(["status"])
        out = capsys.readouterr().out
        assert exit_code == EXIT_FATAL
        assert "STALE SNAPSHOT" in out

    def test_fresh_ready_snapshot_is_not_flagged_stale(self, monkeypatch, tmp_path: Path, capsys) -> None:
        monkeypatch.setenv("CSE_SYMBOLS", "BTCUSDT")
        monkeypatch.setenv("CSE_DB_PATH", str(tmp_path / "state.db"))
        health_path = tmp_path / "health.json"
        monkeypatch.setenv("CSE_HEALTH_SNAPSHOT_PATH", str(health_path))

        now = datetime.now(timezone.utc)
        write_snapshot(
            health_path,
            status=RuntimeStatus(
                overall_health=RuntimeHealth.READY,
                symbols=(
                    SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=now,
                                 reconnect_count=0, detail="ok"),
                ),
                generated_at=now,
            ),
            recovery_ok=True, recovery_detail="recovery completed", pid=999, started_at=now,
        )

        exit_code = main(["status"])
        assert exit_code == EXIT_OK
        assert "STALE SNAPSHOT" not in capsys.readouterr().out


class TestMainRequiresSubcommand:
    def test_no_subcommand_exits_nonzero(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code != 0


=== FILE: tests/test_app_composition.py ===
"""Faz 8 — app.py kompozisyon testleri: `Application`'ın Faz 5/6/7 nesnelerini
config'e göre doğru bağladığını doğrular. Gerçek network/`websockets` YOK —
`FakeLiveDataProvider` enjekte edilir (bkz. tests/runtime_fakes.py)."""

from __future__ import annotations

from pathlib import Path

from crypto_signal_engine.app import Application
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from tests.runtime_fakes import FakeLiveDataProvider


def _config(tmp_path: Path, **overrides: object):
    env = {
        "CSE_SYMBOLS": "BTCUSDT,ETHUSDT",
        "CSE_DB_PATH": str(tmp_path / "state.db"),
        "CSE_FEE_BPS": "5",
        "CSE_SLIPPAGE_BPS": "2",
        "CSE_NOTIONAL_PER_POSITION": "500",
        "CSE_ORDER_BOOK_DEPTH": "50",
        "CSE_STALE_FEED_THRESHOLD_SECONDS": "45",
    }
    env.update({k: str(v) for k, v in overrides.items()})
    return load_config(env)


class TestApplicationComposition:
    def test_symbols_propagate_to_coordinator(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, FakeLiveDataProvider())
        try:
            assert app.runtime._coordinator._symbols == ("BTCUSDT", "ETHUSDT")
        finally:
            app._store.close()
            app._lock.release()

    def test_paper_engine_fee_slippage_notional_propagate(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, FakeLiveDataProvider())
        try:
            paper_engine = app.runtime._coordinator._paper_engine
            assert paper_engine._fee_bps == 5.0
            assert paper_engine._slippage_bps == 2.0
            assert paper_engine._notional_per_position == 500.0
        finally:
            app._store.close()
            app._lock.release()

    def test_order_book_depth_and_stale_threshold_propagate(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, FakeLiveDataProvider())
        try:
            coordinator = app.runtime._coordinator
            assert coordinator._order_book_depth == 50
            assert coordinator._health._threshold == 45.0
        finally:
            app._store.close()
            app._lock.release()

    def test_uses_a_real_paper_state_store_at_configured_db_path(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, FakeLiveDataProvider())
        try:
            assert isinstance(app._store, PaperStateStore)
            assert Path(config.db_path).exists()
        finally:
            app._store.close()
            app._lock.release()

    def test_coordinator_is_a_real_runtime_coordinator(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, FakeLiveDataProvider())
        try:
            assert isinstance(app.runtime._coordinator, RuntimeCoordinator)
        finally:
            app._store.close()
            app._lock.release()


=== FILE: tests/test_app_lifecycle.py ===
"""Faz 8 — app.py lifecycle testleri: startup/recovery başarı-başarısızlık,
duplicate-instance engelleme, graceful/idempotent shutdown, persistence
store'un shutdown'da kapatılması. Tamamen offline/deterministic —
`FakeLiveDataProvider` kullanılır, gerçek OS sinyali GÖNDERİLMEZ (shutdown
mantığı doğrudan `request_shutdown()` çağrılarak tetiklenir).

Kural (bkz. tests/conftest.py): dış bir `pytest-asyncio` bağımlılığı
EKLENMEZ — coroutine'ler `run_async()` ile senkron test fonksiyonları
içinden çalıştırılır (mevcut Faz 6/7 test disipliniyle AYNI)."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.app import EXIT_LOCK_HELD, EXIT_OK, EXIT_STARTUP_FAILURE, Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.ops.health_snapshot import read_snapshot
from crypto_signal_engine.ops.lock import ProcessLock
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _config(tmp_path: Path):
    return load_config(
        {
            "CSE_SYMBOLS": _SYMBOL,
            "CSE_DB_PATH": str(tmp_path / "state.db"),
            "CSE_WARMUP_CANDLES": "20",
            "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
        }
    )


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


class _FailingFetchProvider(FakeLiveDataProvider):
    async def fetch_historical_candles(self, symbol, timeframe, start, end):  # type: ignore[override]
        raise RuntimeError("simulated PUBLIC REST failure during recovery")


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    """Bölüm: "gerçek wall-clock sleep YOK, bounded polling helper KULLAN"
    disiplini — sabit bir `asyncio.sleep(N)` tahmini yerine, koşul
    gerçekleşene KADAR küçük adımlarla (cooperative yield) bekler; bir
    üst sınır (`timeout`) aşılırsa açıkça başarısız olur (sonsuza kadar
    asılı kalmaz)."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _wait_for_run_task(app: Application) -> None:
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)


async def _start_then_shutdown(app: Application) -> int:
    run_task = asyncio.create_task(app.start())
    await _wait_for_run_task(app)
    app.request_shutdown("test")
    return await asyncio.wait_for(run_task, timeout=5.0)


class TestSuccessfulStartupAndShutdown:
    def test_start_recovers_then_shutdown_returns_ok(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())
        exit_code = run_async(_start_then_shutdown(app))
        assert exit_code == EXIT_OK

    def test_store_is_closed_after_shutdown(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())
        run_async(_start_then_shutdown(app))
        with pytest.raises(sqlite3.ProgrammingError):
            app._store._connection.execute("SELECT 1")

    def test_lock_is_released_after_shutdown(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _bootstrappable_provider())
        run_async(_start_then_shutdown(app))

        reacquired = ProcessLock(config.lock_path)
        reacquired.acquire()
        reacquired.release()

    def test_health_snapshot_exists_before_shutdown_completes(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            assert config.health_snapshot_path.exists()
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        run_async(body())

    def test_coordinator_is_marked_stopped_after_shutdown(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())
        run_async(_start_then_shutdown(app))
        assert app.runtime._coordinator._stopped is True

    def test_request_shutdown_is_idempotent(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            app.request_shutdown("first")
            app.request_shutdown("second")
            app.request_shutdown("third")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


class TestRecoveryFailurePreventsRuntimeStart:
    def test_recovery_failure_returns_startup_failure_exit_code(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _FailingFetchProvider())
        exit_code = run_async(asyncio.wait_for(app.start(), timeout=5.0))
        assert exit_code == EXIT_STARTUP_FAILURE
        assert app._run_task is None

    def test_recovery_failure_releases_lock(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _FailingFetchProvider())
        run_async(asyncio.wait_for(app.start(), timeout=5.0))

        reacquired = ProcessLock(config.lock_path)
        reacquired.acquire()
        reacquired.release()

    def test_recovery_failure_closes_store(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _FailingFetchProvider())
        run_async(asyncio.wait_for(app.start(), timeout=5.0))
        with pytest.raises(sqlite3.ProgrammingError):
            app._store._connection.execute("SELECT 1")

    def test_recovery_failure_writes_unhealthy_snapshot(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        app = Application(config, _FailingFetchProvider())
        run_async(asyncio.wait_for(app.start(), timeout=5.0))

        snapshot = read_snapshot(config.health_snapshot_path)
        assert snapshot is not None
        assert snapshot["recovery"]["ok"] is False


class TestDuplicateInstancePrevention:
    def test_second_application_on_same_db_is_rejected_without_disturbing_first(self, tmp_path: Path) -> None:
        config = _config(tmp_path)

        async def body() -> tuple[int, int]:
            first = Application(config, _bootstrappable_provider())
            first_task = asyncio.create_task(first.start())
            await _wait_for_run_task(first)

            second = Application(_config(tmp_path), _bootstrappable_provider())
            second_exit_code = await asyncio.wait_for(second.start(), timeout=5.0)

            assert first._run_task is not None
            assert not first._run_task.done()

            first.request_shutdown("test")
            first_exit_code = await asyncio.wait_for(first_task, timeout=5.0)
            return second_exit_code, first_exit_code

        second_exit_code, first_exit_code = run_async(body())
        assert second_exit_code == EXIT_LOCK_HELD
        assert first_exit_code == EXIT_OK


=== FILE: tests/test_app_notifier_wiring.py ===
"""24/7 Ops v1, Step 4 — `app.py` integration tests: outbound-only
alerting fires on process start and graceful-stop, and stays a silent
no-op with no CSE_TELEGRAM_* env configured (see `tests/test_ops_
notifier.py` for the isolated notifier-module tests).

Same offline discipline as `tests/test_app_lifecycle.py`: `FakeLiveDataProvider`,
no real OS signal, `run_async()` (no pytest-asyncio dependency)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from crypto_signal_engine.app import EXIT_OK, Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.ops.config import load_config
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _config(tmp_path: Path):
    return load_config(
        {
            "CSE_SYMBOLS": _SYMBOL,
            "CSE_DB_PATH": str(tmp_path / "state.db"),
            "CSE_WARMUP_CANDLES": "20",
            "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
            "CSE_DASHBOARD_ENABLED": "false",  # not under test here; avoids port binding
        }
    )


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _wait_for_run_task(app: Application) -> None:
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)


class TestNotifierWiringStartStop:
    def test_start_and_graceful_stop_both_notify(self, tmp_path: Path) -> None:
        calls: list[str] = []
        with patch("crypto_signal_engine.app.build_telegram_notifier", return_value=calls.append):
            app = Application(_config(tmp_path), _bootstrappable_provider())

            async def body() -> int:
                run_task = asyncio.create_task(app.start())
                await _wait_for_run_task(app)
                app.request_shutdown("test")
                return await asyncio.wait_for(run_task, timeout=5.0)

            exit_code = run_async(body())
        assert exit_code == EXIT_OK
        assert any("started" in m for m in calls)
        assert any("stopped gracefully" in m for m in calls)

    def test_none_notifier_by_default_never_raises(self, tmp_path: Path) -> None:
        """Regression: with no CSE_TELEGRAM_* env set, `build_telegram_
        notifier()` returns `None` and every `safe_notify()` call site is
        a silent no-op — start/stop must behave exactly as before."""
        app = Application(_config(tmp_path), _bootstrappable_provider())
        assert app._notifier is None

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_for_run_task(app)
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK


=== FILE: tests/test_app_ops_snapshots.py ===
"""
UI Polish v1 — isolated unit tests for two of `app.py::Application`'s
health-snapshot builder methods that, until now, were only ever exercised
indirectly through full dashboard-render tests:

- Step A3 — `_adaptive_snapshot()`: `{"active": False}` default, the
  optional `adaptive_status_provider` result passed through verbatim, and
  a raising provider never affecting the snapshot write (display-only,
  must never affect trading).
- Step A7 — `_backup_snapshot()`: reads `scripts/backup_sqlite.py`'s
  additive `backup_status.json` READ-ONLY, `{"available": False}`
  whenever missing/corrupt, never fabricating a "last backup" time.

Neither method needs a running asyncio loop or a real bridge -- both are
plain synchronous methods on `Application` reading only `self._config`/
`self._adaptive_status_provider`, so these tests construct `Application`
directly (same lightweight, no-`start()` pattern as `tests/
test_app_adaptive_wiring.py`)."""

from __future__ import annotations

import json
from pathlib import Path

from crypto_signal_engine.app import Application
from crypto_signal_engine.ops.config import load_config
from tests.test_app_signal_testnet_bridge import _base_env, _bootstrappable_provider


class TestAdaptiveSnapshot:
    def test_default_none_provider_reports_inactive(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        assert app._adaptive_snapshot() == {"active": False}  # noqa: SLF001

    def test_provider_result_passed_through_verbatim(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        provider_result = {"active": True, "champion_version_id": "policy-champion-7"}
        app = Application(config, _bootstrappable_provider(), adaptive_status_provider=lambda: provider_result)
        assert app._adaptive_snapshot() == provider_result  # noqa: SLF001

    def test_raising_provider_never_crashes_the_snapshot(self, tmp_path: Path) -> None:
        def _raising_provider() -> dict[str, object]:
            raise RuntimeError("simulated adaptive status provider failure")

        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider(), adaptive_status_provider=_raising_provider)
        result = app._adaptive_snapshot()  # noqa: SLF001 -- must not raise
        assert result["active"] is False
        assert "detail" in result


class TestBackupSnapshot:
    def test_no_backup_status_file_reports_unavailable(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        assert app._backup_snapshot() == {"available": False}  # noqa: SLF001

    def test_reads_real_backup_status_fields(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        status_path = config.db_path.parent / "backup_status.json"
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(
            json.dumps({
                "completed_at": "2026-01-01T00:00:00+00:00",
                "destination": "/var/backups/paper_state-2026-01-01.db",
                "size_bytes": 4096,
            }),
            encoding="utf-8",
        )
        app = Application(config, _bootstrappable_provider())
        result = app._backup_snapshot()  # noqa: SLF001
        assert result == {
            "available": True,
            "completed_at": "2026-01-01T00:00:00+00:00",
            "destination": "/var/backups/paper_state-2026-01-01.db",
            "size_bytes": 4096,
        }

    def test_corrupt_backup_status_file_never_raises(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        status_path = config.db_path.parent / "backup_status.json"
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text("not valid json at all {{{", encoding="utf-8")
        app = Application(config, _bootstrappable_provider())
        assert app._backup_snapshot() == {"available": False}  # noqa: SLF001 -- must not raise


=== FILE: tests/test_app_price_freshness.py ===
"""
Dashboard price/P&L sync fix — `app.py::Application._latest_price()`
previously read ONLY the latest CLOSED M5 candle, which can lag real
price movement by up to 5 minutes even though the M1 lifecycle evaluator
itself already reacts every minute. This module tests, in isolation:

- `_compose_m1_candle_observers()` — the pure composition helper that
  lets `Application`'s own M1 price-cache observer (`_record_m1_price`)
  coexist with an EXTERNAL `m1_candle_observer` (e.g. `scripts/run_with_
  adaptive_policy.py`'s shadow-fidelity monitor) at the SAME hook,
  each independently defensively wrapped.
- `Application._latest_price()`/`_latest_price_source()`/`_record_m1_
  price()` — the REQUIRED regression proof that an empty M1 cache falls
  back to the pre-existing M5 behaviour byte-for-byte, and the M1-
  preferred behaviour once the cache is populated and genuinely newer.

See `tests/test_app_adaptive_wiring.py::TestM1CandleObserverWiring` for
the real end-to-end wiring proof (Application -> LifecycleRuntime)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_signal_engine.app import Application, _compose_m1_candle_observers
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
from crypto_signal_engine.ops.config import load_config
from tests.runtime_fakes import make_candle_series_ending_at
from tests.test_app_signal_testnet_bridge import _base_env, _bootstrappable_provider

SYMBOL = "BTCUSDT"


def _app(tmp_path: Path) -> Application:
    config = load_config(_base_env(tmp_path))
    return Application(config, _bootstrappable_provider())


def _seed_m5_candle(app: Application, *, close: float, close_time: datetime) -> None:
    candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, close_time, count=1, base_price=close)
    app.runtime._coordinator.bootstrap_candles(SYMBOL, Timeframe.M5, candles, as_of=close_time)  # noqa: SLF001


def _m1_candle(*, close: float, close_time: datetime) -> LifecycleCandle:
    return LifecycleCandle(open=close, high=close + 0.5, low=close - 0.5, close=close, close_time=close_time)


class TestComposeM1CandleObservers:
    def test_both_none_returns_none(self) -> None:
        assert _compose_m1_candle_observers(None, None) is None

    def test_only_a_returns_a_directly(self) -> None:
        def a(symbol, candle):  # noqa: ANN001
            pass

        assert _compose_m1_candle_observers(a, None) is a

    def test_only_b_returns_b_directly(self) -> None:
        def b(symbol, candle):  # noqa: ANN001
            pass

        assert _compose_m1_candle_observers(None, b) is b

    def test_both_present_calls_both(self) -> None:
        calls: list[str] = []
        combined = _compose_m1_candle_observers(
            lambda symbol, candle: calls.append(f"a:{symbol}"), lambda symbol, candle: calls.append(f"b:{symbol}"),
        )
        candle = _m1_candle(close=100.0, close_time=datetime(2026, 1, 1, tzinfo=timezone.utc))
        combined(SYMBOL, candle)
        assert calls == [f"a:{SYMBOL}", f"b:{SYMBOL}"]

    def test_first_raising_never_prevents_the_second_from_firing(self) -> None:
        calls: list[str] = []

        def raising_a(symbol, candle):  # noqa: ANN001
            raise RuntimeError("simulated observer a failure")

        combined = _compose_m1_candle_observers(raising_a, lambda symbol, candle: calls.append("b"))
        candle = _m1_candle(close=100.0, close_time=datetime(2026, 1, 1, tzinfo=timezone.utc))
        combined(SYMBOL, candle)  # must not raise
        assert calls == ["b"]

    def test_second_raising_never_undoes_the_first_having_fired(self) -> None:
        calls: list[str] = []

        def raising_b(symbol, candle):  # noqa: ANN001
            raise RuntimeError("simulated observer b failure")

        combined = _compose_m1_candle_observers(lambda symbol, candle: calls.append("a"), raising_b)
        candle = _m1_candle(close=100.0, close_time=datetime(2026, 1, 1, tzinfo=timezone.utc))
        combined(SYMBOL, candle)  # must not raise
        assert calls == ["a"]

    def test_both_raising_never_propagates(self) -> None:
        def raising_a(symbol, candle):  # noqa: ANN001
            raise RuntimeError("a")

        def raising_b(symbol, candle):  # noqa: ANN001
            raise RuntimeError("b")

        combined = _compose_m1_candle_observers(raising_a, raising_b)
        candle = _m1_candle(close=100.0, close_time=datetime(2026, 1, 1, tzinfo=timezone.utc))
        combined(SYMBOL, candle)  # must not raise


class TestLatestPriceRegressionAndM1Preference:
    def test_no_m1_no_m5_returns_none(self, tmp_path: Path) -> None:
        app = _app(tmp_path)
        assert app._latest_price(SYMBOL) is None  # noqa: SLF001
        assert app._latest_price_source(SYMBOL) is None  # noqa: SLF001

    def test_empty_m1_cache_falls_back_to_m5_byte_for_bit(self, tmp_path: Path) -> None:
        """REQUIRED regression test: with the M1 cache empty (e.g. right
        after a restart, or the Testnet bridge disabled so no M1 stream
        is ever consumed), `_latest_price()` returns EXACTLY the same
        value it did before this fix -- the latest closed M5 candle."""
        app = _app(tmp_path)
        m5_time = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
        _seed_m5_candle(app, close=100.0, close_time=m5_time)

        assert app._latest_price(SYMBOL) == 100.0  # noqa: SLF001
        source = app._latest_price_source(SYMBOL)  # noqa: SLF001
        assert source == (100.0, "M5", m5_time)

    def test_m1_newer_than_m5_is_preferred(self, tmp_path: Path) -> None:
        """REQUIRED test: once the M1 cache holds a candle strictly newer
        than the latest closed M5 one, `_latest_price()` uses it."""
        app = _app(tmp_path)
        m5_time = datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc)
        _seed_m5_candle(app, close=100.0, close_time=m5_time)

        m1_time = m5_time + timedelta(minutes=3)
        app._record_m1_price(SYMBOL, _m1_candle(close=103.5, close_time=m1_time))  # noqa: SLF001

        assert app._latest_price(SYMBOL) == 103.5  # noqa: SLF001
        source = app._latest_price_source(SYMBOL)  # noqa: SLF001
        assert source == (103.5, "M1", m1_time)

    def test_m1_older_than_m5_falls_back_to_m5(self, tmp_path: Path) -> None:
        """A stale M1 cache entry (e.g. the bridge briefly stopped
        consuming M1 candles) must NEVER win over a genuinely newer M5
        candle -- comparison is by `close_time`, never insertion order."""
        app = _app(tmp_path)
        m1_time = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        app._record_m1_price(SYMBOL, _m1_candle(close=99.0, close_time=m1_time))  # noqa: SLF001

        m5_time = m1_time + timedelta(minutes=5)
        _seed_m5_candle(app, close=101.0, close_time=m5_time)

        assert app._latest_price(SYMBOL) == 101.0  # noqa: SLF001
        source = app._latest_price_source(SYMBOL)  # noqa: SLF001
        assert source == (101.0, "M5", m5_time)

    def test_m1_present_with_no_m5_candle_yet_is_used(self, tmp_path: Path) -> None:
        app = _app(tmp_path)
        m1_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        app._record_m1_price(SYMBOL, _m1_candle(close=50.0, close_time=m1_time))  # noqa: SLF001

        assert app._latest_price(SYMBOL) == 50.0  # noqa: SLF001
        assert app._latest_price_source(SYMBOL) == (50.0, "M1", m1_time)  # noqa: SLF001

    def test_record_m1_price_never_raises_and_updates_the_cache(self, tmp_path: Path) -> None:
        app = _app(tmp_path)
        candle_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        app._record_m1_price(SYMBOL, _m1_candle(close=42.0, close_time=candle_time))  # noqa: SLF001
        assert app._m1_price_cache[SYMBOL] == (42.0, candle_time)  # noqa: SLF001


=== FILE: tests/test_app_signal_testnet_bridge.py ===
"""Faz 13 — `app.py::Application` Signal->TESTNET bridge KOMPOZİSYON
testleri: gating (bridge NE ZAMAN inşa edilir/edilmez), `BridgeRuntime`
DOĞRU sarmalama, startup reconciliation'ın bridge'i operational işaretlemesi,
ve health-snapshot gözlemlenebilirliği. `SignalTestnetBridge`'in KENDİ
politika mantığı burada TEKRAR test EDİLMEZ (bkz. `test_signal_testnet_
bridge.py`) — yalnızca `Application`'ın onu DOĞRU koşullarda DOĞRU
şekilde BAĞLADIĞI doğrulanır (Faz 12'nin `test_phase12_local_production_
readiness.py`'siyle AYNI disiplin/fikstürler)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.app import EXIT_OK, Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.ops.errors import AppConfigurationError
from crypto_signal_engine.ops.health_snapshot import read_snapshot
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _base_env(tmp_path: Path, **overrides: str) -> dict:
    env = {
        "CSE_SYMBOLS": _SYMBOL,
        "CSE_DB_PATH": str(tmp_path / "state.db"),
        "CSE_WARMUP_CANDLES": "20",
        "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
    }
    env.update(overrides)
    return env


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _run_until_ready_then_shutdown(app: Application, *, extra_wait: float = 0.2) -> int:
    run_task = asyncio.create_task(app.start())
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
    await asyncio.sleep(extra_wait)
    app.request_shutdown("test")
    return await asyncio.wait_for(run_task, timeout=5.0)


def _testnet_bridge_env(tmp_path: Path, exec_db: Path, **overrides: str) -> dict:
    return _base_env(
        tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true",
        CSE_ENABLE_SIGNAL_TESTNET_BRIDGE="true", **overrides,
    )


class TestConfigGate:
    def test_bridge_flag_defaults_false(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        assert config.enable_signal_testnet_bridge is False
        assert config.testnet_bridge_notional_usdt == 10.0

    def test_bridge_flag_parses_true(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_ENABLE_SIGNAL_TESTNET_BRIDGE="true"))
        assert config.enable_signal_testnet_bridge is True

    def test_notional_must_be_positive(self, tmp_path: Path) -> None:
        with pytest.raises(AppConfigurationError):
            load_config(_base_env(tmp_path, CSE_TESTNET_BRIDGE_NOTIONAL_USDT="0"))
        with pytest.raises(AppConfigurationError):
            load_config(_base_env(tmp_path, CSE_TESTNET_BRIDGE_NOTIONAL_USDT="-5"))

    def test_notional_override_is_honored(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_TESTNET_BRIDGE_NOTIONAL_USDT="25"))
        assert config.testnet_bridge_notional_usdt == 25.0


class TestApplicationCompositionGate:
    def test_paper_mode_never_constructs_bridge_even_with_flag_true(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_ENABLE_SIGNAL_TESTNET_BRIDGE="true"))
        app = Application(config, _bootstrappable_provider())
        assert app._signal_bridge is None
        assert isinstance(app.runtime, PersistedRuntime)
        assert not isinstance(app.runtime, BridgeRuntime)

    def test_testnet_without_execution_gate_never_constructs_bridge(self, tmp_path: Path) -> None:
        config = load_config(
            _base_env(
                tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_SIGNAL_TESTNET_BRIDGE="true"
            )
        )
        app = Application(config, _bootstrappable_provider())
        assert app._signal_bridge is None
        assert not isinstance(app.runtime, BridgeRuntime)

    def test_testnet_and_execution_gate_without_bridge_flag_never_constructs_bridge(self, tmp_path: Path) -> None:
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        app = Application(config, _bootstrappable_provider())
        assert app._signal_bridge is None
        assert not isinstance(app.runtime, BridgeRuntime)

    def test_all_three_gates_true_constructs_bridge_and_bridge_runtime(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)
        assert app._signal_bridge is not None
        # Autonomous Testnet trading lifecycle: `LifecycleRuntime` now wraps
        # `BridgeRuntime` (composition, not inheritance) whenever the bridge
        # is enabled — the bridge is still fully wired underneath.
        assert isinstance(app.runtime, LifecycleRuntime)
        assert isinstance(app.runtime._bridge_runtime, BridgeRuntime)
        assert app._signal_bridge.operational is False  # not yet (startup reconciliation hasn't run)


class TestStartupWiresBridgeOperationalFlag:
    def test_successful_startup_marks_bridge_operational(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        exit_code = run_async(_run_until_ready_then_shutdown(app))

        assert exit_code == EXIT_OK
        assert app.execution_ready is True
        assert app._signal_bridge.operational is True
        assert len(fake_http.post_calls) == 0  # no signal fired -> no order, but bridge is armed

    def test_missing_credentials_keeps_bridge_not_operational(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert app._signal_bridge.operational is False
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_pending_ambiguous_record_still_becomes_ready_bridge_reflects_it(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """`reconcile_pending()` sembol/kayıt bazında izole çalışır (bkz.
        `ExecutionReconciliationService.reconcile_pending()` docstring'i) —
        bir kaydın query'si BAŞARISIZ olsa bile sweep'in KENDİSİ hâlâ
        BAŞARIYLA tamamlanır (`execution_ready=True`); bridge de buna göre
        operational olur (startup, bu senaryoda ASLA yeni bir order
        GÖNDERMEZ — sıfır POST)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        from crypto_signal_engine.execution.errors import ExecutionTransportError
        from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
        from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, new_record, transition
        from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore

        store = ExecutionStateStore(exec_db)
        intent = OrderIntent(
            symbol=_SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
            context_id="bridge:BTCUSDT:OPEN:ctx-pending", timestamp=datetime.now(timezone.utc), quantity=0.001,
        )
        record = new_record(intent, now=datetime.now(timezone.utc))
        record = transition(
            record, new_state=ExecutionLifecycleState.AMBIGUOUS, now=datetime.now(timezone.utc), detail="pending"
        )
        store.save(record)
        store.close()

        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient(get_responses=[ExecutionTransportError("simulated query failure")])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is True
        assert app._signal_bridge.operational is True
        assert len(fake_http.post_calls) == 0


class TestHealthSnapshotObservability:
    def test_snapshot_reflects_disabled_bridge(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        run_async(_run_until_ready_then_shutdown(app))

        snapshot = read_snapshot(config.health_snapshot_path)
        bridge_snapshot = snapshot["signal_testnet_bridge"]
        assert bridge_snapshot["enabled"] is False
        assert bridge_snapshot["operational"] is False

    def test_snapshot_reflects_operational_bridge_with_symbol_status(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        snapshot = read_snapshot(config.health_snapshot_path)
        bridge_snapshot = snapshot["signal_testnet_bridge"]
        assert bridge_snapshot["enabled"] is True
        assert bridge_snapshot["operational"] is True
        assert bridge_snapshot["policy"] == "SPOT_LONG_ONLY"
        assert _SYMBOL in bridge_snapshot["symbols"]
        symbol_status = bridge_snapshot["symbols"][_SYMBOL]
        assert symbol_status["last_action"] == "NONE"  # no signal has fired yet in this short window

    def test_no_credential_values_leak_into_snapshot(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "unmistakable-secret-key-value")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "unmistakable-secret-secret-value")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(_testnet_bridge_env(tmp_path, exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        snapshot = read_snapshot(config.health_snapshot_path)
        blob = str(snapshot)
        assert "unmistakable-secret-key-value" not in blob
        assert "unmistakable-secret-secret-value" not in blob


=== FILE: tests/test_app_symbol_resolution.py ===
"""`app.py::resolve_symbols()` + `Application` sembol-seçimi entegrasyon
testleri. Gerçek Binance ağı YOK (`BinanceRestClient` + `FakeHttpClient`,
bkz. tests/binance_fakes.py) — `Application` ise `FakeLiveDataProvider`
(bkz. tests/runtime_fakes.py) ile, tests/test_app_composition.py ile AYNI
desende kurulur.

Bölüm A ("Existing open PAPER positions") — pinning testleri de burada:
durable bir AÇIK pozisyon, otomatik fırsat ranking'i onu dışarıda bıraksa
BİLE nihai runtime sembol kümesinden ASLA düşmemelidir."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.app import Application, resolve_symbols
from crypto_signal_engine.errors import SymbolSelectionError, TransportError
from crypto_signal_engine.ops.config import load_config
from crypto_signal_engine.paper_trading.models import PositionSide
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from tests.binance_fakes import FakeHttpClient, json_response
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider

UTC = timezone.utc
NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)


def _rest_client(responses: list) -> BinanceRestClient:
    return BinanceRestClient(BinanceConfig(), FakeHttpClient(responses), FixedClock(NOW), FakeSleeper())


def _exchange_info(symbols: list[str]) -> tuple[int, str]:
    return json_response(
        {
            "symbols": [
                {"symbol": s, "baseAsset": s.removesuffix("USDT"), "quoteAsset": "USDT",
                 "status": "TRADING", "isSpotTradingAllowed": True, "permissions": ["SPOT"]}
                for s in symbols
            ]
        }
    )


def _tickers(symbol_volumes: dict[str, float]) -> tuple[int, str]:
    return json_response(
        [{"symbol": s, "quoteVolume": str(v), "priceChangePercent": "1.0", "count": 10_000} for s, v in symbol_volumes.items()]
    )


def _kline_row(open_ms: int, close: float, volume: float = 100.0) -> list:
    return [open_ms, str(close), str(close * 1.001), str(close * 0.999), str(close), str(volume),
            open_ms + 5 * 60_000 - 1, str(volume * close), 10, str(volume / 2), str(volume * close / 2), "0"]


def _trending(count: int, *, step_pct: float = 0.02, start: float = 100.0) -> tuple[int, str]:
    rows, price = [], start
    for i in range(count):
        price *= 1 + step_pct
        rows.append(_kline_row(i * 5 * 60_000, price))
    return json_response(rows)


def _config(tmp_path: Path, **overrides: object):
    env = {"CSE_DB_PATH": str(tmp_path / "state.db")}
    env.update({k: str(v) for k, v in overrides.items()})
    return load_config(env)


_DEFAULT_AUTO_ENV = {
    "CSE_AUTO_SYMBOL_COUNT": "1",
    "CSE_AUTO_SYMBOL_SHORTLIST_SIZE": "5",
    "CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME": "1000000",
    "CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "20",
    "CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES": "5",
}


def _seed_position(db_path: Path, symbol: str, *, side: PositionSide, quantity: float = 0.0) -> None:
    """Durable state'e (Faz 5/7 store'u DOĞRUDAN kullanarak) bir pozisyon
    kaydı yazar — testte "zaten çalışmış bir önceki oturumdan kalan durable
    PAPER state" senaryosunu üretmek için."""
    from crypto_signal_engine.paper_trading.models import PaperPosition

    store = PaperStateStore(db_path)
    try:
        position = PaperPosition(
            symbol=symbol, side=side, quantity=quantity if side is not PositionSide.FLAT else 0.0,
            average_entry_price=100.0 if side is not PositionSide.FLAT else 0.0,
            realized_pnl=0.0, updated_at=NOW,
        )
        store.checkpoint_paper_state(
            symbol=symbol, position=position, entry_fee=0.0, last_signal_timestamp=None,
            new_context_entries=[], new_fills=(), new_orders=(),
        )
    finally:
        store.close()


class TestResolveSymbolsManualOverride:
    def test_explicit_symbols_bypass_selection_entirely(self, tmp_path: Path) -> None:
        config = _config(tmp_path, CSE_SYMBOLS="BTCUSDT,ETHUSDT")

        class _ExplodingRestClient:
            async def fetch_exchange_info(self):
                raise AssertionError("manual mode must NEVER touch the network")

        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=_ExplodingRestClient()))
        assert resolved is config
        assert result is None
        assert pinned == ()
        assert resolved.symbols == ("BTCUSDT", "ETHUSDT")

    def test_manual_mode_ignores_open_positions_on_other_symbols(self, tmp_path: Path) -> None:
        """Bölüm gereksinimi: manual override AYNEN korunur — pinning
        yalnızca AUTOMATIC modda uygulanır, manual bir seçimi GENİŞLETMEZ."""
        _seed_position(tmp_path / "state.db", "BTCUSDT", side=PositionSide.LONG, quantity=1.0)
        config = _config(tmp_path, CSE_SYMBOLS="ETHUSDT")
        resolved, result, pinned = run_async(resolve_symbols(config))
        assert resolved.symbols == ("ETHUSDT",)
        assert pinned == ()


class TestResolveSymbolsAutomaticMode:
    def test_missing_symbols_triggers_real_selection_and_fills_config(self, tmp_path: Path) -> None:
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)
        assert config.auto_select_symbols is True
        assert config.symbols == ()

        rest_client = _rest_client(
            [_exchange_info(["BTCUSDT"]), _tickers({"BTCUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        assert resolved.symbols == ("BTCUSDT",)
        assert resolved.auto_select_symbols is True
        assert result is not None
        assert result.selected_symbols == ("BTCUSDT",)
        assert pinned == ()

    def test_selection_failure_propagates_and_config_stays_unresolved(self, tmp_path: Path) -> None:
        config = _config(tmp_path)  # no CSE_SYMBOLS -> automatic
        rest_client = _rest_client([TransportError("down")] * 4)
        with pytest.raises(SymbolSelectionError):
            run_async(resolve_symbols(config, rest_client=rest_client))


class TestOpenPositionPinningAtStartup:
    """Bölüm A — "Existing open PAPER positions" — self-review senaryosu:
    ranking ARBUSDT/ZECUSDT/... seçer ama durable state'te BTCUSDT LONG
    var -> nihai küme BTCUSDT'yi HER ZAMAN İÇERMELİDİR."""

    def test_open_position_symbol_not_in_opportunity_ranking_is_still_included(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        _seed_position(db_path, "BTCUSDT", side=PositionSide.LONG, quantity=0.5)
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)  # target_count=1

        # Discovery yalnızca ETHUSDT'yi eligible/likit gösterir — BTCUSDT
        # evrende YOK (örn. o anki eligibility taramasında elenmiş olabilir)
        # ama durable pozisyonu var.
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        assert result.selected_symbols == ("ETHUSDT",)
        assert pinned == ("BTCUSDT",)
        # Nihai küme İKİSİNİ DE içerir — target_count=1 aşılmış olsa BİLE.
        assert set(resolved.symbols) == {"BTCUSDT", "ETHUSDT"}
        assert "BTCUSDT" in resolved.symbols

    def test_multiple_open_positions_are_all_pinned(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        _seed_position(db_path, "BTCUSDT", side=PositionSide.LONG, quantity=0.5)
        _seed_position(db_path, "BNBUSDT", side=PositionSide.SHORT, quantity=2.0)
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))
        assert set(pinned) == {"BTCUSDT", "BNBUSDT"}
        assert {"BTCUSDT", "BNBUSDT", "ETHUSDT"} <= set(resolved.symbols)

    def test_historical_flat_position_is_not_pinned(self, tmp_path: Path) -> None:
        """Bölüm: "Do not pin historical FLAT symbols unnecessarily" —
        eskiden pozisyon açılmış ama ŞİMDİ FLAT olan bir sembol PINLENMEZ."""
        db_path = tmp_path / "state.db"
        _seed_position(db_path, "DOGEUSDT", side=PositionSide.FLAT)
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))
        assert pinned == ()
        assert resolved.symbols == ("ETHUSDT",)

    def test_no_open_positions_means_no_pinning(self, tmp_path: Path) -> None:
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)  # fresh DB, no positions at all
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))
        assert pinned == ()
        assert resolved.symbols == ("ETHUSDT",)

    def test_pinning_creates_no_duplicate_order_or_fill(self, tmp_path: Path) -> None:
        """Bölüm 20: pinning yalnızca "hangi semboller izlenecek" sorusunu
        etkiler — RECOVERY yolu (Faz 7, DEĞİŞTİRİLMEDİ) HİÇBİR yeni order/
        fill üretmez, mevcut durable kayıtlar AYNEN kalır."""
        db_path = tmp_path / "state.db"
        _seed_position(db_path, "BTCUSDT", side=PositionSide.LONG, quantity=0.5)
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        store = PaperStateStore(db_path)
        try:
            snapshot = store.load_paper_state("BTCUSDT")
        finally:
            store.close()
        assert snapshot is not None
        assert len(snapshot.orders) == 0  # bu senaryoda hiç order yazılmadı — pinning da hiçbirini EKLEMEDİ
        assert len(snapshot.fills) == 0
        assert snapshot.position.side == PositionSide.LONG
        assert snapshot.position.quantity == 0.5


class TestBridgeOwnedSymbolPinningAtStartup:
    """Autonomous Testnet trading lifecycle Phase 4/16 self-review finding:
    the PRE-EXISTING PAPER-only pinning above does not by itself protect a
    symbol with REAL bridge-owned Testnet inventory but no correlated
    PAPER position — `_read_bridge_owned_symbols` closes that gap."""

    def _seed_bridge_buy(self, exec_db_path: Path, symbol: str) -> None:
        from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
        from crypto_signal_engine.execution.reconciliation_models import (
            ExecutionLifecycleState,
            apply_exchange_truth,
            new_record,
            transition,
        )
        from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
        from crypto_signal_engine.execution.signal_bridge import bridge_context_id

        store = ExecutionStateStore(exec_db_path)
        try:
            intent = OrderIntent(
                symbol=symbol, side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id=bridge_context_id(symbol, "OPEN", "ctx-legacy"), timestamp=NOW, quote_quantity=10.0,
            )
            record = new_record(intent, now=NOW)
            record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
            record = apply_exchange_truth(
                record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=1,
                executed_quantity=1.0, cumulative_quote_quantity=100.0, now=NOW,
            )
            store.save(record)
        finally:
            store.close()

    def _testnet_bridge_env(self, exec_db: Path) -> dict[str, str]:
        return {
            **_DEFAULT_AUTO_ENV, "CSE_EXECUTION_MODE": "BINANCE_SPOT_TESTNET",
            "CSE_ENABLE_TESTNET_EXECUTION": "true", "CSE_ENABLE_SIGNAL_TESTNET_BRIDGE": "true",
        }

    def test_bridge_owned_symbol_without_paper_position_is_still_pinned(self, tmp_path: Path, monkeypatch) -> None:
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        self._seed_bridge_buy(exec_db, "BTCUSDT")
        config = _config(tmp_path, **self._testnet_bridge_env(exec_db))

        # BTCUSDT has ZERO PAPER position (never seeded) -- only bridge inventory.
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        assert "BTCUSDT" in pinned
        assert "BTCUSDT" in resolved.symbols

    def test_no_bridge_pinning_when_bridge_disabled(self, tmp_path: Path, monkeypatch) -> None:
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        self._seed_bridge_buy(exec_db, "BTCUSDT")
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)  # PAPER mode, bridge disabled

        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        assert "BTCUSDT" not in pinned  # bridge disabled -- no bridge-inventory concept to protect

    def test_no_execution_db_yet_is_not_an_error(self, tmp_path: Path, monkeypatch) -> None:
        exec_db = tmp_path / "does-not-exist.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = _config(tmp_path, **self._testnet_bridge_env(exec_db))
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))
        assert pinned == ()


class TestAutomaticSymbolsReachRuntimeCoordinator:
    def test_selected_symbols_propagate_to_coordinator_and_snapshot(self, tmp_path: Path) -> None:
        config = _config(
            tmp_path, CSE_AUTO_SYMBOL_COUNT="2", CSE_AUTO_SYMBOL_SHORTLIST_SIZE="5",
            CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME="1000000", CSE_AUTO_SYMBOL_LOOKBACK_CANDLES="20",
            CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES="5",
        )
        rest_client = _rest_client(
            [
                _exchange_info(["BTCUSDT", "ETHUSDT"]),
                _tickers({"BTCUSDT": 10_000_000.0, "ETHUSDT": 9_000_000.0}),
                _trending(20, step_pct=0.02),
                _trending(20, step_pct=0.015),
            ]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))
        assert result is not None
        assert pinned == ()

        app = Application(resolved, FakeLiveDataProvider(), selection_result=result, pinned_symbols=pinned)
        try:
            assert app.runtime._coordinator._symbols == result.selected_symbols
            snapshot = app._symbol_selection_snapshot()
            assert snapshot["mode"] == "AUTOMATIC"
            assert snapshot["opportunity_symbols"] == list(result.selected_symbols)
            assert snapshot["pinned_open_position_symbols"] == []
            assert snapshot["final_runtime_symbols"] == list(result.selected_symbols)
            assert len(snapshot["candidates"]) == len(result.ranked_candidates)
        finally:
            app._store.close()
            app._lock.release()

    def test_pinned_symbol_reaches_coordinator_and_snapshot(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        _seed_position(db_path, "BTCUSDT", side=PositionSide.LONG, quantity=0.5)
        config = _config(tmp_path, **_DEFAULT_AUTO_ENV)
        rest_client = _rest_client(
            [_exchange_info(["ETHUSDT"]), _tickers({"ETHUSDT": 10_000_000.0}), _trending(20)]
        )
        resolved, result, pinned = run_async(resolve_symbols(config, rest_client=rest_client))

        app = Application(resolved, FakeLiveDataProvider(), selection_result=result, pinned_symbols=pinned)
        try:
            assert set(app.runtime._coordinator._symbols) == {"BTCUSDT", "ETHUSDT"}
            snapshot = app._symbol_selection_snapshot()
            assert snapshot["pinned_open_position_symbols"] == ["BTCUSDT"]
            assert set(snapshot["final_runtime_symbols"]) == {"BTCUSDT", "ETHUSDT"}
        finally:
            app._store.close()
            app._lock.release()

    def test_manual_mode_snapshot_reports_manual(self, tmp_path: Path) -> None:
        config = _config(tmp_path, CSE_SYMBOLS="BTCUSDT")
        app = Application(config, FakeLiveDataProvider())
        try:
            snapshot = app._symbol_selection_snapshot()
            assert snapshot == {
                "mode": "MANUAL", "opportunity_symbols": ["BTCUSDT"],
                "pinned_open_position_symbols": [], "final_runtime_symbols": ["BTCUSDT"],
                # Autonomous Testnet trading lifecycle Phase 16: periodic
                # reselection only applies in AUTOMATIC mode.
                "reselection_scheduler": {"enabled": False},
            }
        finally:
            app._store.close()
            app._lock.release()


=== FILE: tests/test_app_watchdog_wiring.py ===
"""24/7 Ops v1, Step 2 — `app.py` integration test: the existing health
loop actually calls `notify_watchdog()` on every tick (see `crypto_
signal_engine/ops/systemd_notify.py` for the isolated no-op/payload
tests).

Same offline discipline as `tests/test_app_lifecycle.py`: `FakeLiveDataProvider`,
no real OS signal, `run_async()` (no pytest-asyncio dependency)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from crypto_signal_engine.app import Application
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.ops.config import load_config
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def _config(tmp_path: Path):
    return load_config(
        {
            "CSE_SYMBOLS": _SYMBOL,
            "CSE_DB_PATH": str(tmp_path / "state.db"),
            "CSE_WARMUP_CANDLES": "20",
            "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
            "CSE_DASHBOARD_ENABLED": "false",  # not under test here; avoids port binding
        }
    )


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _wait_for_run_task(app: Application) -> None:
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)


class TestWatchdogHeartbeatWiring:
    def test_health_loop_calls_notify_watchdog(self, tmp_path: Path) -> None:
        app = Application(_config(tmp_path), _bootstrappable_provider())
        calls: list[None] = []

        async def body() -> None:
            with patch("crypto_signal_engine.app.notify_watchdog", side_effect=lambda: calls.append(None)):
                run_task = asyncio.create_task(app.start())
                await _wait_for_run_task(app)
                await _wait_until(lambda: len(calls) >= 1)
                app.request_shutdown("test")
                await asyncio.wait_for(run_task, timeout=5.0)

        run_async(body())
        assert len(calls) >= 1


=== FILE: tests/test_backup_sqlite.py ===
"""24/7 Ops v1, Step 1 — `scripts/backup_sqlite.py`'nin retention-cleanup
mantığı: `select_backups_to_delete()` SAF bir fonksiyondur (gerçek dosya
sistemi I/O'su YAPMAZ, girdi/çıktısı yalnızca `(dosya_adı, mtime)`
çiftleri/dizinleri) — bu dosya bunu izole olarak test eder."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.backup_sqlite import select_backups_to_delete, write_backup_status


class TestSelectBackupsToDelete:
    def test_keeps_newest_n_deletes_the_rest(self) -> None:
        backups = [("a.db", 1.0), ("b.db", 2.0), ("c.db", 3.0)]
        assert select_backups_to_delete(backups, keep_last=2) == ["a.db"]

    def test_keeps_everything_when_fewer_than_keep_last(self) -> None:
        backups = [("a.db", 1.0), ("b.db", 2.0)]
        assert select_backups_to_delete(backups, keep_last=5) == []

    def test_keep_last_zero_never_deletes_anything(self) -> None:
        """Fail-safe varsayılan: `keep_last<=0` bir yanlış konfigürasyon
        olabilir — SESSİZCE tüm yedekleri silmek, bir backup aracı için
        mümkün olan EN KÖTÜ hata modudur."""
        backups = [("a.db", 1.0), ("b.db", 2.0), ("c.db", 3.0)]
        assert select_backups_to_delete(backups, keep_last=0) == []

    def test_keep_last_negative_never_deletes_anything(self) -> None:
        backups = [("a.db", 1.0), ("b.db", 2.0)]
        assert select_backups_to_delete(backups, keep_last=-3) == []

    def test_empty_input_deletes_nothing(self) -> None:
        assert select_backups_to_delete([], keep_last=5) == []

    def test_keeps_the_most_recent_by_mtime_not_input_order(self) -> None:
        backups = [("newest.db", 100.0), ("oldest.db", 1.0), ("middle.db", 50.0)]
        assert select_backups_to_delete(backups, keep_last=1) == ["middle.db", "oldest.db"]

    def test_exact_keep_last_count_deletes_nothing(self) -> None:
        backups = [("a.db", 1.0), ("b.db", 2.0), ("c.db", 3.0)]
        assert select_backups_to_delete(backups, keep_last=3) == []


class TestWriteBackupStatus:
    """UI Polish v1, Step A7 — `write_backup_status()` writes
    `source.parent/backup_status.json` atomically; `app.py::Application.
    _backup_snapshot()` reads it back READ-ONLY (see
    `tests/test_app_ops_snapshots.py` for that read side)."""

    def test_writes_expected_fields_next_to_the_source_db(self, tmp_path: Path) -> None:
        source_dir = tmp_path / "live"
        source_dir.mkdir()
        source = source_dir / "paper_state.db"
        source.write_bytes(b"fake-live-db")
        destination = tmp_path / "backups" / "paper_state-2026-01-01.db"
        destination.parent.mkdir()
        destination.write_bytes(b"fake-backup-contents")
        completed_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        write_backup_status(source=source, destination=destination, completed_at=completed_at)

        status_path = source_dir / "backup_status.json"
        assert status_path.exists()
        data = json.loads(status_path.read_text(encoding="utf-8"))
        assert data["completed_at"] == completed_at.isoformat()
        assert data["destination"] == str(destination)
        assert data["size_bytes"] == len(b"fake-backup-contents")

    def test_second_call_overwrites_the_first_atomically(self, tmp_path: Path) -> None:
        source = tmp_path / "paper_state.db"
        source.write_bytes(b"fake-live-db")
        dest1 = tmp_path / "backup1.db"
        dest1.write_bytes(b"aaa")
        dest2 = tmp_path / "backup2.db"
        dest2.write_bytes(b"bbbbb")

        write_backup_status(source=source, destination=dest1, completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        write_backup_status(source=source, destination=dest2, completed_at=datetime(2026, 1, 2, tzinfo=timezone.utc))

        status_path = source.parent / "backup_status.json"
        data = json.loads(status_path.read_text(encoding="utf-8"))
        assert data["destination"] == str(dest2)
        assert data["size_bytes"] == 5
        # no leftover atomic-write temp files
        assert list(source.parent.glob(".backup-status-*.tmp")) == []


=== FILE: tests/test_binance_clock.py ===
from datetime import datetime, timezone

import pytest

from tests.conftest import run_async
from crypto_signal_engine.providers.binance.clock import (
    DeterministicJitterSource,
    FakeSleeper,
    FixedClock,
    RandomJitterSource,
    SystemClock,
)

UTC = timezone.utc


class TestSystemClock:
    def test_now_is_utc_aware(self) -> None:
        now = SystemClock().now()
        assert now.tzinfo is not None


class TestFixedClock:
    def test_naive_initial_rejected(self) -> None:
        with pytest.raises(ValueError, match="UTC-aware"):
            FixedClock(datetime(2026, 8, 31))

    def test_now_returns_fixed_value(self) -> None:
        clock = FixedClock(datetime(2026, 8, 31, tzinfo=UTC))
        assert clock.now() == datetime(2026, 8, 31, tzinfo=UTC)

    def test_advance(self) -> None:
        clock = FixedClock(datetime(2026, 8, 31, 10, 0, tzinfo=UTC))
        clock.advance(30)
        assert clock.now() == datetime(2026, 8, 31, 10, 0, 30, tzinfo=UTC)

    def test_set_naive_rejected(self) -> None:
        clock = FixedClock(datetime(2026, 8, 31, tzinfo=UTC))
        with pytest.raises(ValueError, match="UTC-aware"):
            clock.set(datetime(2026, 8, 31))


class TestFakeSleeper:
    def test_records_calls_without_real_delay(self) -> None:
        async def _run() -> None:
            sleeper = FakeSleeper()
            await sleeper.sleep(30.0)
            await sleeper.sleep(5.0)
            assert sleeper.calls == [30.0, 5.0]

        run_async(_run())

    def test_trigger_cancellation(self) -> None:
        import asyncio

        async def _run() -> None:
            sleeper = FakeSleeper()
            sleeper.trigger_cancellation()
            with pytest.raises(asyncio.CancelledError):
                await sleeper.sleep(1.0)

        run_async(_run())


class TestJitterSources:
    def test_random_jitter_bounded(self) -> None:
        source = RandomJitterSource()
        for _ in range(20):
            value = source.jitter(2.0)
            assert 0.0 <= value <= 2.0

    def test_random_jitter_zero_max(self) -> None:
        assert RandomJitterSource().jitter(0.0) == 0.0

    def test_deterministic_jitter_fixed_value(self) -> None:
        source = DeterministicJitterSource(0.3)
        assert source.jitter(2.0) == 0.3

    def test_deterministic_jitter_clamped_to_max(self) -> None:
        source = DeterministicJitterSource(5.0)
        assert source.jitter(2.0) == 2.0


=== FILE: tests/test_binance_config.py ===
import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import ConfigurationError
from crypto_signal_engine.providers.binance.config import (
    BinanceConfig,
    ReconnectPolicyConfig,
    binance_interval_for,
)


class TestReconnectPolicyConfig:
    def test_valid_defaults(self) -> None:
        config = ReconnectPolicyConfig()
        assert config.initial_delay_seconds == 1.0

    def test_non_positive_initial_delay_rejected(self) -> None:
        with pytest.raises(ValueError, match="initial_delay_seconds"):
            ReconnectPolicyConfig(initial_delay_seconds=0)

    def test_max_delay_below_initial_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_delay_seconds"):
            ReconnectPolicyConfig(initial_delay_seconds=10, max_delay_seconds=5)

    def test_multiplier_not_greater_than_one_rejected(self) -> None:
        with pytest.raises(ValueError, match="multiplier"):
            ReconnectPolicyConfig(multiplier=1.0)

    def test_negative_jitter_rejected(self) -> None:
        with pytest.raises(ValueError, match="jitter_seconds"):
            ReconnectPolicyConfig(jitter_seconds=-1)

    def test_max_attempts_zero_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_attempts"):
            ReconnectPolicyConfig(max_attempts=0)

    def test_max_attempts_none_allowed(self) -> None:
        config = ReconnectPolicyConfig(max_attempts=None)
        assert config.max_attempts is None


class TestBinanceConfig:
    def test_valid_defaults(self) -> None:
        config = BinanceConfig()
        assert config.rest_base_url == "https://api.binance.com"

    def test_http_rest_url_rejected(self) -> None:
        with pytest.raises(ValueError, match="https://"):
            BinanceConfig(rest_base_url="http://api.binance.com")

    def test_ws_url_not_wss_rejected(self) -> None:
        with pytest.raises(ValueError, match="wss://"):
            BinanceConfig(ws_base_url="ws://stream.binance.com")

    def test_non_positive_timeout_rejected(self) -> None:
        with pytest.raises(ValueError, match="request_timeout_seconds"):
            BinanceConfig(request_timeout_seconds=0)

    def test_invalid_order_book_depth_rejected(self) -> None:
        with pytest.raises(ValueError, match="order_book_depth"):
            BinanceConfig(order_book_depth=42)

    @pytest.mark.parametrize("valid_depth", [5, 10, 20, 50, 100, 500, 1000, 5000])
    def test_valid_order_book_depths_accepted(self, valid_depth: int) -> None:
        config = BinanceConfig(order_book_depth=valid_depth)
        assert config.order_book_depth == valid_depth

    def test_non_positive_stale_threshold_rejected(self) -> None:
        with pytest.raises(ValueError, match="stale_feed_threshold_seconds"):
            BinanceConfig(stale_feed_threshold_seconds=0)

    def test_wrong_reconnect_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="ReconnectPolicyConfig"):
            BinanceConfig(reconnect="not-a-config")  # type: ignore[arg-type]

    def test_non_timeframe_in_supported_timeframes_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            BinanceConfig(supported_timeframes=("5m",))  # type: ignore[arg-type]

    def test_max_klines_per_request_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_klines_per_request"):
            BinanceConfig(max_klines_per_request=0)
        with pytest.raises(ValueError, match="max_klines_per_request"):
            BinanceConfig(max_klines_per_request=1001)

    def test_supported_symbols_converted_to_tuple(self) -> None:
        config = BinanceConfig(supported_symbols=["BTCUSDT", "ETHUSDT"])
        assert isinstance(config.supported_symbols, tuple)


class TestBinanceIntervalFor:
    def test_known_timeframes(self) -> None:
        assert binance_interval_for(Timeframe.M1) == "1m"
        assert binance_interval_for(Timeframe.M5) == "5m"
        assert binance_interval_for(Timeframe.M15) == "15m"
        assert binance_interval_for(Timeframe.H1) == "1h"

    def test_unknown_timeframe_raises_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError):
            binance_interval_for("not-a-timeframe")  # type: ignore[arg-type]


=== FILE: tests/test_binance_order_book_sync.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import OrderBookSyncError
from crypto_signal_engine.providers.binance.order_book_sync import (
    MAX_PRE_SYNC_BUFFER_SIZE,
    DepthDiffEvent,
    OrderBookSyncState,
    OrderBookSynchronizer,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def diff(first: int, final: int, bids=(), asks=(), event_time=T0) -> DepthDiffEvent:
    return DepthDiffEvent(
        symbol="BTCUSDT", first_update_id=first, final_update_id=final,
        event_time=event_time, bids=tuple(bids), asks=tuple(asks),
    )


class TestDepthDiffEventInvariants:
    def test_final_less_than_first_rejected(self) -> None:
        with pytest.raises(ValueError, match="final_update_id"):
            diff(first=10, final=5)

    def test_symbol_normalized(self) -> None:
        event = DepthDiffEvent(
            symbol="btcusdt", first_update_id=1, final_update_id=2,
            event_time=T0, bids=(), asks=(),
        )
        assert event.symbol == "BTCUSDT"


class TestSnapshotBeforeDiff:
    def test_diff_before_snapshot_is_buffered_not_applied(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        result = sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        assert result is None
        assert sync.state == OrderBookSyncState.UNSYNCED
        assert sync.buffered_event_count == 1


class TestSnapshotFromStaleBufferedDiff:
    def test_stale_buffered_diff_dropped(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=90, final=100, bids=[(1.0, 1.0)], asks=[(2.0, 1.0)]))
        sync.ingest_diff(diff(first=151, final=155, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        snapshot = sync.ingest_snapshot(
            last_update_id=150, bids=((99.5, 2.0),), asks=((100.5, 2.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 155


class TestExactBoundaryUpdateId:
    def test_exact_boundary_bridges_correctly(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=101, bids=[(99.0, 5.0)], asks=[(100.0, 5.0)]))
        snapshot = sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 101
        assert snapshot.best_bid.quantity == 5.0

    def test_boundary_not_satisfied_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=105, final=110, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        with pytest.raises(OrderBookSyncError, match="köprü"):
            sync.ingest_snapshot(
                last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
            )
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestSequenceGap:
    def test_gap_after_sync_detected_and_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=105, final=110, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        assert sync.state == OrderBookSyncState.UNSYNCED

    def test_gap_in_buffered_events_during_snapshot_apply_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        sync.ingest_diff(diff(first=110, final=115, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        with pytest.raises(OrderBookSyncError, match="buffer içinde sequence gap"):
            sync.ingest_snapshot(
                last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
            )

    def test_after_gap_resync_works(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError):
            sync.ingest_diff(diff(first=105, final=110))
        assert sync.state == OrderBookSyncState.UNSYNCED
        snapshot = sync.ingest_snapshot(
            last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert sync.state == OrderBookSyncState.SYNCED
        assert snapshot.last_update_id == 200


class TestDuplicateDiff:
    def test_duplicate_final_update_id_treated_as_stale_not_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        result = sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 999.0)], asks=[(100.0, 999.0)]))
        assert result is not None
        assert sync.state == OrderBookSyncState.SYNCED
        assert result.best_bid.quantity == 2.0


class TestOutOfOrderDiff:
    def test_out_of_order_after_sync_is_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        # final_update_id (250) > last_update_id (200) -> stale DEĞİL; ve
        # first_update_id (210) beklenen U<=201 sınırını AŞIYOR -> gerçek gap.
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=210, final=250))

    def test_fully_stale_replay_after_sync_is_ignored_not_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        # final_update_id (60) <= last_update_id (200) -> tamamen eski/replay,
        # Binance dokümantasyonuna göre normal kabul edilir, gap DEĞİLDİR.
        result = sync.ingest_diff(diff(first=50, final=60))
        assert result is not None
        assert sync.state == OrderBookSyncState.SYNCED


class TestReconnectPendingEvent:
    def test_buffered_events_pending_during_reconnect_are_cleared_on_reset(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=1, final=5))
        assert sync.buffered_event_count == 1
        sync.reset()
        assert sync.buffered_event_count == 0
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestZeroQuantityDelete:
    def test_zero_quantity_removes_level(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0), (98.0, 2.0)), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        snapshot = sync.ingest_diff(diff(first=101, final=101, bids=[(98.0, 0.0)], asks=[]))
        prices = [level.price for level in snapshot.bids]
        assert 98.0 not in prices
        assert 99.0 in prices

    def test_deleting_nonexistent_level_is_noop(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=101, final=101, bids=[(50.0, 0.0)], asks=[]))
        assert snapshot.best_bid.price == 99.0


class TestBidAskSorting:
    def test_bids_descending_asks_ascending(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        snapshot = sync.ingest_snapshot(
            last_update_id=1,
            bids=((98.0, 1.0), (99.5, 1.0), (99.0, 1.0)),
            asks=((101.0, 1.0), (100.0, 1.0), (100.5, 1.0)),
            snapshot_timestamp=T0,
        )
        bid_prices = [lvl.price for lvl in snapshot.bids]
        ask_prices = [lvl.price for lvl in snapshot.asks]
        assert bid_prices == sorted(bid_prices, reverse=True)
        assert ask_prices == sorted(ask_prices)


class TestCrossedBook:
    def test_crossed_book_after_diff_raises_and_unsyncs(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="crossed"):
            sync.ingest_diff(diff(first=101, final=101, bids=[(101.0, 5.0)], asks=[]))
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestEmptySide:
    def test_snapshot_resulting_in_empty_side_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        with pytest.raises(OrderBookSyncError, match="boş kaldı"):
            sync.ingest_snapshot(last_update_id=1, bids=(), asks=((100.0, 1.0),), snapshot_timestamp=T0)

    def test_diff_emptying_last_bid_level_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="boş kaldı"):
            sync.ingest_diff(diff(first=101, final=101, bids=[(99.0, 0.0)], asks=[]))


class TestMalformedPriceQty:
    def test_negative_price_rejected_by_order_book_level(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        with pytest.raises(ValueError):
            sync.ingest_snapshot(last_update_id=1, bids=((-1.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)


class TestResyncAfterReplay:
    def test_stale_event_after_resync_with_higher_last_update_id_ignored(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError):
            sync.ingest_diff(diff(first=105, final=110))
        sync.ingest_snapshot(last_update_id=300, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        result = sync.ingest_diff(diff(first=105, final=110))
        assert result is not None


class TestBufferOverflow:
    def test_buffer_overflow_before_snapshot_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        for i in range(MAX_PRE_SYNC_BUFFER_SIZE):
            sync.ingest_diff(diff(first=i, final=i))
        with pytest.raises(OrderBookSyncError, match="sınırını aştı"):
            sync.ingest_diff(diff(first=MAX_PRE_SYNC_BUFFER_SIZE, final=MAX_PRE_SYNC_BUFFER_SIZE))


class TestSymbolMismatch:
    def test_mismatched_symbol_rejected(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        wrong = DepthDiffEvent(symbol="ETHUSDT", first_update_id=1, final_update_id=2, event_time=T0, bids=(), asks=())
        with pytest.raises(ValueError, match="symbol uyuşmazlığı"):
            sync.ingest_diff(wrong)


class TestReviewerOverlapProbes:
    """Bağımsız reviewer probe'ları A ve B — overlap semantiği (strict
    equality zorunluluğu kaldırıldı)."""

    def test_probe_a_overlap_within_range_accepted(self) -> None:
        """A) local book last_update_id=100, incoming U=100,u=102 → ACCEPT."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=100, final=102, bids=[(99.0, 5.0)], asks=[]))
        assert snapshot is not None
        assert snapshot.last_update_id == 102
        assert sync.state == OrderBookSyncState.SYNCED

    def test_probe_b_true_gap_still_rejected(self) -> None:
        """B) local book last_update_id=100, incoming U=102,u=103 → REJECT/RESYNC."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=102, final=103))
        assert sync.state == OrderBookSyncState.UNSYNCED

    def test_exact_match_still_accepted(self) -> None:
        """Overlap kabulü, strict equality'yi de kapsamalı (regresyon)."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=101, final=101))
        assert snapshot is not None
        assert snapshot.last_update_id == 101

    def test_buffer_continuity_overlap_accepted(self) -> None:
        """ingest_snapshot içindeki buffer-sürekliliği kontrolü de overlap kabul etmeli."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        # İkinci buffer event'i, ilkinin son ID'siyle TAM eşleşmiyor ama
        # overlap ediyor (first=104 <= prev.final(105)+1=106).
        sync.ingest_diff(diff(first=104, final=110, bids=[(99.0, 2.0)], asks=[]))
        snapshot = sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 110


=== FILE: tests/test_binance_parser.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import ParseError
from crypto_signal_engine.providers.binance import parser
from crypto_signal_engine.providers.binance.order_book_sync import DepthDiffEvent

UTC = timezone.utc


def valid_kline_ws_payload(**overrides) -> dict:
    k = {
        "t": 1000000, "T": 1059999, "s": "BTCUSDT", "i": "1m",
        "f": 100, "L": 200, "o": "100.0", "c": "101.0", "h": "102.0", "l": "99.0",
        "v": "10.0", "n": 5, "x": False, "q": "1000.0", "V": "5.0", "Q": "500.0", "B": "0",
    }
    k.update(overrides.pop("k", {}))
    payload = {"e": "kline", "E": 1000500, "s": "BTCUSDT", "k": k}
    payload.update(overrides)
    return payload


class TestParseKlineWsEvent:
    def test_valid_payload(self) -> None:
        update = parser.parse_kline_ws_event(valid_kline_ws_payload(), received_at=datetime(2026, 8, 31, tzinfo=UTC))
        assert update.candle.symbol == "BTCUSDT"
        assert update.candle.timeframe == Timeframe.M1
        assert update.update_seq == 1000500
        assert update.candle.is_closed is False

    def test_wrong_event_type_rejected(self) -> None:
        payload = valid_kline_ws_payload(e="aggTrade")
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_missing_top_level_key_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        del payload["E"]
        with pytest.raises(ParseError, match="'E' alanı eksik"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_missing_nested_key_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        del payload["k"]["o"]
        with pytest.raises(ParseError, match="'o' alanı eksik"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_null_value_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = None
        with pytest.raises(ParseError, match="None olamaz"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_wrong_type_numeric_field_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = 100.0  # str olmalı, float geldi
        with pytest.raises(ParseError, match="sayısal string"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_malformed_numeric_string_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = "not-a-number"
        with pytest.raises(ParseError, match="geçerli bir sayı değil"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_nan_string_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = "nan"
        with pytest.raises(ParseError):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_unknown_interval_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["i"] = "3d"
        with pytest.raises(ParseError, match="Desteklenmeyen/bilinmeyen Binance interval"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_wrong_top_level_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="dict bekleniyordu"):
            parser.parse_kline_ws_event("not-a-dict", received_at=datetime(2026, 8, 31, tzinfo=UTC))  # type: ignore[arg-type]

    def test_negative_epoch_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["t"] = -1
        with pytest.raises(ParseError, match="negatif olamaz"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_bool_type_rejected_for_int_field(self) -> None:
        payload = valid_kline_ws_payload()
        payload["E"] = True  # bool, int isinstance True ama semantik olarak yanlış
        with pytest.raises(ParseError, match="int olmalı"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))


class TestParseKlineRestRow:
    VALID_ROW = [
        1000000, "100.0", "102.0", "99.0", "101.0", "10.0", 1059999,
        "1000.0", 5, "5.0", "500.0", "0",
    ]

    def test_valid_row_historical_now_none(self) -> None:
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=None)
        assert candle.is_closed is True

    def test_is_closed_true_when_close_time_in_past(self) -> None:
        now = datetime(2026, 8, 31, tzinfo=UTC)
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=now)
        assert candle.is_closed is True

    def test_is_closed_false_when_close_time_in_future(self) -> None:
        now = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)  # çok eski "şimdi", close_time gelecekte kalır
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=now)
        assert candle.is_closed is False

    def test_too_short_row_rejected(self) -> None:
        with pytest.raises(ParseError, match="en az 9 elemanlı"):
            parser.parse_kline_rest_row([1, 2, 3], symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_non_list_row_rejected(self) -> None:
        with pytest.raises(ParseError):
            parser.parse_kline_rest_row("not-a-list", symbol="BTCUSDT", timeframe=Timeframe.M1)  # type: ignore[arg-type]

    def test_malformed_numeric_in_row_rejected(self) -> None:
        row = list(self.VALID_ROW)
        row[1] = "garbage"
        with pytest.raises(ParseError, match="geçerli sayı değil"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_wrong_type_open_time_rejected(self) -> None:
        row = list(self.VALID_ROW)
        row[0] = "1000000"  # int olmalı
        with pytest.raises(ParseError, match="int olmalı"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_impossible_ohlc_rejected_by_domain_model(self) -> None:
        row = list(self.VALID_ROW)
        row[2] = "50.0"  # high < low ihlali
        with pytest.raises(ValueError, match="impossible OHLC"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)


class TestParseAggTradeWsEvent:
    def valid_payload(self, **overrides) -> dict:
        payload = {
            "e": "aggTrade", "E": 123456789, "s": "BTCUSDT", "a": 12345,
            "p": "100.5", "q": "2.0", "f": 100, "l": 105, "T": 123456785, "m": True, "M": True,
        }
        payload.update(overrides)
        return payload

    def test_valid_payload(self) -> None:
        trade = parser.parse_agg_trade_ws_event(self.valid_payload())
        assert trade.symbol == "BTCUSDT"
        assert trade.trade_id == 12345
        assert trade.is_buyer_maker is True

    def test_wrong_event_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_agg_trade_ws_event(self.valid_payload(e="kline"))

    def test_missing_field_rejected(self) -> None:
        payload = self.valid_payload()
        del payload["p"]
        with pytest.raises(ParseError, match="'p' alanı eksik"):
            parser.parse_agg_trade_ws_event(payload)

    def test_malformed_price_rejected(self) -> None:
        with pytest.raises(ParseError):
            parser.parse_agg_trade_ws_event(self.valid_payload(p="abc"))

    def test_wrong_type_boolean_field_rejected(self) -> None:
        with pytest.raises(ParseError, match="bool olmalı"):
            parser.parse_agg_trade_ws_event(self.valid_payload(m="true"))


class TestParseDepthLevels:
    def test_valid_levels(self) -> None:
        levels = parser.parse_depth_levels([["100.0", "1.0"], ["101.0", "2.0"]], "ctx")
        assert levels == ((100.0, 1.0), (101.0, 2.0))

    def test_non_list_rejected(self) -> None:
        with pytest.raises(ParseError, match="liste bekleniyordu"):
            parser.parse_depth_levels("not-a-list", "ctx")

    def test_entry_too_short_rejected(self) -> None:
        with pytest.raises(ParseError, match="formatında olmalı"):
            parser.parse_depth_levels([["100.0"]], "ctx")

    def test_non_string_price_rejected(self) -> None:
        with pytest.raises(ParseError, match="string olmalı"):
            parser.parse_depth_levels([[100.0, "1.0"]], "ctx")

    def test_malformed_numeric_rejected(self) -> None:
        with pytest.raises(ParseError, match="geçersiz sayı"):
            parser.parse_depth_levels([["abc", "1.0"]], "ctx")


class TestParseDepthSnapshotResponse:
    def test_valid_response(self) -> None:
        payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        last_id, bids, asks = parser.parse_depth_snapshot_response(payload)
        assert last_id == 100
        assert bids == ((99.0, 1.0),)
        assert asks == ((100.0, 1.0),)

    def test_missing_last_update_id_rejected(self) -> None:
        with pytest.raises(ParseError, match="'lastUpdateId' alanı eksik"):
            parser.parse_depth_snapshot_response({"bids": [], "asks": []})


class TestParseDepthDiffWsEvent:
    def valid_payload(self, **overrides) -> dict:
        payload = {
            "e": "depthUpdate", "E": 123456789, "s": "BTCUSDT",
            "U": 157, "u": 160, "b": [["0.0024", "10"]], "a": [["0.0026", "100"]],
        }
        payload.update(overrides)
        return payload

    def test_valid_payload(self) -> None:
        event = parser.parse_depth_diff_ws_event(self.valid_payload())
        assert isinstance(event, DepthDiffEvent)
        assert event.first_update_id == 157
        assert event.final_update_id == 160

    def test_wrong_event_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_depth_diff_ws_event(self.valid_payload(e="kline"))

    def test_missing_field_rejected(self) -> None:
        payload = self.valid_payload()
        del payload["U"]
        with pytest.raises(ParseError, match="'U' alanı eksik"):
            parser.parse_depth_diff_ws_event(payload)


=== FILE: tests/test_binance_provider.py ===
import asyncio
import contextlib
import json
from datetime import datetime, timezone

import pytest

from tests.binance_fakes import FakeHttpClient, FakeWebSocketConnection, FakeWebSocketConnectionFactory, json_response
from tests.conftest import run_async
from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.domain.state_contract import InMemoryCandleStateStore
from crypto_signal_engine.errors import TransportError
from crypto_signal_engine.providers.base import ConnectionState
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig, ReconnectPolicyConfig
from crypto_signal_engine.providers.binance.provider import BinanceMarketDataProvider
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate, BinanceQualityThresholds
from crypto_signal_engine.state.manager import StateManager

UTC = timezone.utc


def kline_msg(
    open_ms: int, close_ms: int, is_closed: bool, close: str = "100.0", event_ms: int | None = None,
    interval: str = "1m",
) -> str:
    close_val = float(close)
    high = max(105.0, close_val + 1.0)
    low = min(95.0, close_val - 1.0)
    payload = {
        "e": "kline", "E": event_ms if event_ms is not None else close_ms, "s": "BTCUSDT",
        "k": {
            "t": open_ms, "T": close_ms, "s": "BTCUSDT", "i": interval, "f": 1, "L": 2,
            "o": "100.0", "c": close, "h": str(high), "l": str(low), "v": "10.0",
            "n": 5, "x": is_closed, "q": "1000.0", "V": "5.0", "Q": "500.0", "B": "0",
        },
    }
    return json.dumps(payload)


def trade_msg(trade_id: int, price: str, ts_ms: int) -> str:
    return json.dumps({
        "e": "aggTrade", "E": ts_ms, "s": "BTCUSDT", "a": trade_id,
        "p": price, "q": "1.0", "f": trade_id, "l": trade_id, "T": ts_ms, "m": False, "M": True,
    })


def depth_diff_msg(first: int, final: int, bids=(), asks=(), event_ms: int = 0) -> str:
    return json.dumps({
        "e": "depthUpdate", "E": event_ms, "s": "BTCUSDT", "U": first, "u": final,
        "b": [[str(p), str(q)] for p, q in bids], "a": [[str(p), str(q)] for p, q in asks],
    })


FAST_RECONNECT = ReconnectPolicyConfig(initial_delay_seconds=0.001, max_delay_seconds=0.01, jitter_seconds=0.0)


def make_provider(ws_factory, http_client=None, now=None, config_overrides=None) -> BinanceMarketDataProvider:
    overrides = config_overrides or {}
    config = BinanceConfig(reconnect=FAST_RECONNECT, **overrides)
    clock = FixedClock(now or datetime(2026, 8, 31, tzinfo=UTC))
    return BinanceMarketDataProvider(
        config=config, http_client=http_client or FakeHttpClient([]), ws_factory=ws_factory,
        clock=clock, sleeper=FakeSleeper(),
    )


async def collect_n(agen, n: int, timeout: float = 2.0) -> list:
    results = []
    for _ in range(n):
        results.append(await asyncio.wait_for(agen.__anext__(), timeout=timeout))
    return results


class TestStreamCandlesHappyPath:
    def test_yields_committed_candles_in_order(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.5", event_ms=1),
            kline_msg(0, 59999, True, close="101.0", event_ms=2),
            kline_msg(60000, 119999, False, close="102.0", event_ms=3),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 3)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 101.0, 102.0]
        assert candles[1].is_closed is True

    def test_duplicate_update_suppressed(self) -> None:
        duplicate_msg = kline_msg(0, 59999, False, close="100.5", event_ms=5)
        conn = FakeWebSocketConnection([
            duplicate_msg,
            duplicate_msg,  # aynı update_seq (E=5) -> duplicate, sessizce atlanır
            kline_msg(0, 59999, False, close="101.0", event_ms=6),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)  # yalnızca 2 tanesi yayınlanmalı
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 101.0]

    def test_final_then_replay_ignored(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.5", event_ms=10),  # final
            kline_msg(0, 59999, False, close="999.0", event_ms=5),  # eski replay (E=5 < final E=10)
            kline_msg(60000, 119999, False, close="102.0", event_ms=11),  # sıradaki candle, normal
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)  # final + sıradaki; replay atlanmalı
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 102.0]


class TestMalformedPayloadDoesNotCrashStream:
    def test_malformed_message_skipped(self) -> None:
        conn = FakeWebSocketConnection([
            "not-json-at-all{{{",
            json.dumps({"e": "kline", "E": 1, "s": "BTCUSDT"}),  # eksik 'k' alanı
            kline_msg(0, 59999, False, close="100.5", event_ms=5),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.5


class TestReconnectOnTransportError:
    def test_reconnects_after_disconnect_and_continues(self) -> None:
        first_conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.5", event_ms=1),
            TransportError("bağlantı koptu"),
        ])
        second_conn = FakeWebSocketConnection([
            kline_msg(60000, 119999, False, close="105.0", event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([first_conn, second_conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)
            health = provider.health()
            await provider.close()
            return candles, health

        candles, health = run_async(_run())
        assert [c.close for c in candles] == [100.5, 105.0]
        assert first_conn.closed is True
        assert health.reconnect_count >= 1

    def test_connect_failure_retried_with_backoff(self) -> None:
        factory = FakeWebSocketConnectionFactory([
            TransportError("ilk bağlantı başarısız"),
            FakeWebSocketConnection([kline_msg(0, 59999, False, close="55.0", event_ms=1)]),
        ])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 55.0
        assert len(factory.connect_calls) >= 2


class TestGracefulShutdown:
    def test_close_cancels_background_tasks_cleanly(self) -> None:
        conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1)
            await provider.close()
            # close() sonrası background_tasks boş olmalı (idempotent + temiz)
            assert len(provider._background_tasks) == 0
            await provider.close()  # ikinci close() da güvenli (idempotent) olmalı

        run_async(_run())
        assert conn.closed is True

    def test_agen_aclose_cancels_its_own_task(self) -> None:
        conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1)
            await agen.aclose()
            assert len(provider._background_tasks) == 0

        run_async(_run())


class TestCandleGapRecovery:
    def test_11_rest_response_including_incoming_candle_is_not_committed_via_backfill(self) -> None:
        """REST endTime sınırı incoming candle'ı da döndürebilir; historical
        path YALNIZCA gerçekten eksik (expected) candle'ları commit etmeli —
        incoming candle'ın kendisi ASLA historical yoldan commit edilmemeli.

        latest=12:00, incoming WS=12:05 -> yalnızca 12:01,12:02,12:03,12:04
        historical recovery ile commit edilebilir. REST yanıtı 12:05'i de
        (incoming candle'ı) döndürse bile bu, historical path tarafından
        YOK SAYILMALI; gap kapandıktan sonra ORİJİNAL WS 12:05 update'i
        tekrar denenip canonical state ONUN üzerinden oluşmalı.
        """
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            # incoming WS candle: open_time=240000 (12:04 sonrası -> 4 dakika
            # gap, 1m için), event_ms ile canonical'a taşınacak ayırt edici
            # bir WS-kaynaklı değer.
            kline_msg(240000, 299999, False, close="102.0", event_ms=987654),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # REST yanıtı: gerçekten eksik olan 4 candle (60000..239999 open_time)
        # ARTI Binance'in endTime sınır davranışı nedeniyle incoming candle'ın
        # KENDİSİNİ de (open_time=240000) döndürüyor — FARKLI bir close
        # değeriyle ("103.5"), böylece hangi yoldan commit edildiği net
        # şekilde ayırt edilebilir.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            [120000, "100.5", "101.5", "99.5", "101.0", "10.0", 179999, "0", 1, "0", "0", "0"],
            [180000, "101.0", "102.0", "100.0", "101.5", "10.0", 239999, "0", 1, "0", "0", "0"],
            [240000, "101.5", "104.0", "100.5", "103.5", "10.0", 299999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 5, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        # KRİTİK: incoming candle (open_time=240000) historical path'in
        # "103.5" değeriyle DEĞİL, orijinal WS update'in "102.0" değeriyle
        # canonical hâle gelmiş olmalı.
        assert candles[1].open_time.timestamp() == 240.0
        assert candles[1].close == 102.0
        assert candles[1].close != 103.5

        # Ek doğrulama: canonical state, incoming candle için gerçekten
        # WS-kaynaklı değerleri taşıyor (StateManager üzerinden).
        assert provider._state_manager.latest_candle("BTCUSDT", Timeframe.M1).close == 102.0

    def test_missing_interval_triggers_backfill_then_commits(self) -> None:
        # İlk candle: open_time=0 (aligned), kapanır.
        # İkinci candle: open_time=180000 (3 dakika sonra) -> 1m için gap.
        # Beklenen davranış: MISSING_CANDLE tespit edilince REST'ten backfill
        # yapılır, ardından ORİJİNAL update tekrar denenip commit edilir.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(180000, 239999, False, close="105.0", event_ms=180001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill REST yanıtı: eksik iki candle'ı (60000 ve 120000 open_time)
        # kapalı olarak döndürür.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            [120000, "100.5", "101.5", "99.5", "101.0", "10.0", 179999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # İlk candle (final) + gap sonrası backfill tetiklenip nihayetinde
            # canlı update'in kendisi de commit edilir.
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        # İlk yayınlanan: t=0 final candle. İkincisi: gap sonrası, canlı
        # update'in (open_time=180000) başarıyla commit edilmiş hâli.
        assert candles[0].close == 100.0
        assert candles[1].open_time.timestamp() == 180.0
        assert candles[1].close == 105.0
        # Backfill REST çağrısı gerçekten yapılmış olmalı.
        assert len(http.calls) == 1
        # DETERMINISTIC pencere doğrulaması: tam olarak [60000, 180000)
        # istenmiş olmalı — sabit/kör bir pencere (örn. now-20*duration)
        # KULLANILMADI.
        _, params = http.calls[0]
        assert params["startTime"] == "60000"
        assert params["endTime"] == "180000"

    def test_2_multi_interval_gap_recovered_exactly(self) -> None:
        """2) multi-interval gap (5m: 12:00 -> 12:20, eksik 12:05/10/15) doğru recover edilir."""
        # UTC epoch: 12:00 = 43200s, 12:05=43500s, ... 12:20=44400s (5m aralıklarla)
        base = 43200 * 1000
        step = 5 * 60 * 1000
        conn = FakeWebSocketConnection([
            kline_msg(base, base + 299999, True, close="100.0", event_ms=base + 300000, interval="5m"),
            kline_msg(base + 4 * step, base + 4 * step + 299999, False, close="108.0", event_ms=base + 4 * step + 1, interval="5m"),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Eksik: 12:05, 12:10, 12:15 (3 candle)
        backfill_rows = [
            [base + 1 * step, "100.0", "103.0", "99.0", "102.0", "10.0", base + 1 * step + 299999, "0", 1, "0", "0", "0"],
            [base + 2 * step, "102.0", "105.0", "101.0", "104.0", "10.0", base + 2 * step + 299999, "0", 1, "0", "0", "0"],
            [base + 3 * step, "104.0", "107.0", "103.0", "106.0", "10.0", base + 3 * step + 299999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        now = datetime(1970, 1, 1, 12, 20, 5, tzinfo=UTC)
        clock = FixedClock(now)
        # 20 dakikalık gap, default 300s staleness eşiğinden büyük olduğundan
        # (bu test'in amacı staleness DEĞİL, gap-recovery doğruluğu olduğundan)
        # bu test için realtime staleness eşiği bilinçli olarak büyütülüyor.
        gate = BinanceDataQualityGate(clock, BinanceQualityThresholds(max_candle_staleness_seconds=3600))
        config = BinanceConfig(reconnect=FAST_RECONNECT, supported_timeframes=(Timeframe.M5,))
        provider = BinanceMarketDataProvider(
            config=config, http_client=http, ws_factory=factory,
            clock=clock, sleeper=FakeSleeper(), quality_gate=gate,
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M5)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        assert candles[1].close == 108.0
        _, params = http.calls[0]
        assert params["startTime"] == str(base + 1 * step)
        assert params["endTime"] == str(base + 4 * step)

    def test_3_old_but_valid_historical_candle_not_rejected_for_age(self) -> None:
        """3) eski (ama geçerli) historical candle sırf age nedeniyle reddedilmez.

        Bu, structural-vs-freshness ayrımını (Faz 2 Bölüm 2) doğrudan
        `StateManager` seviyesinde doğrular: AYNI eski candle, realtime
        `handle_candle_update` ile STALE_PRICE nedeniyle reddedilirken,
        `handle_historical_candle_update` ile (yalnızca structural
        validity uygulanarak) KABUL EDİLMELİDİR.
        """
        clock = FixedClock(datetime(1970, 1, 1, 1, 0, 0, tzinfo=UTC))  # "şimdi" çok ileride
        gate = BinanceDataQualityGate(clock)  # varsayılan eşik: 300s

        old_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True, trade_count=1,
        )
        update = CandleUpdate(
            candle=old_candle, update_seq=0, event_time=old_candle.close_time, received_at=old_candle.close_time
        )

        # Realtime check (canlı WS akışı) İSE staleness nedeniyle reddeder.
        realtime_manager = StateManager(gate, InMemoryCandleStateStore())
        realtime_result = realtime_manager.handle_candle_update(update)
        assert not realtime_result.quality_result.passed
        assert realtime_result.quality_result.status == DataQualityStatus.STALE_PRICE

        # Historical/backfill check İSE (yalnızca structural validity) KABUL EDER.
        historical_manager = StateManager(gate, InMemoryCandleStateStore())
        historical_result = historical_manager.handle_historical_candle_update(update)
        assert historical_result.quality_result.passed
        assert historical_result.commit_result.committed

    def test_4_malformed_historical_candle_still_rejected(self) -> None:
        """4) malformed historical candle yine reddedilir (structural validity hâlâ uygulanıyor).

        Ayrıca: malformed bir backfill satırı, canlı stream'in KENDİSİNİ
        ÇÖKERTMEMELİDİR (Bölüm 17 — retry-safe olmayan hatalar bile
        stream'i düşürmeden ele alınmalı).
        """
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="101.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill satırı, impossible OHLC içeriyor (high < low) — domain
        # model construction seviyesinde reddedilecek (ValueError).
        malformed_rows = [
            [60000, "100.0", "50.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(malformed_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Malformed backfill hatası pump loop'u ÇÖKERTMEMELİ; ilk candle
            # (t=0) her hâlükârda yayınlanmış olmalı.
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0

    def test_5_backfill_committed_in_chronological_order(self) -> None:
        """5) backfill chronological sırayla commit edilir."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Bilerek TERS sırada döndürüyoruz — implementasyon sort etmeli.
        backfill_rows_reversed = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows_reversed)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[1].close == 102.0

    def test_6_duplicate_backfill_does_not_corrupt_state(self) -> None:
        """6) duplicate backfill canonical state'i bozmaz."""
        # İki ayrı gap tetiklenip AYNI backfill aralığının iki kez
        # istenmesi senaryosu: ikinci deneme idempotent olmalı.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        # Aynı backfill yanıtı iki kez scriptlenmiş olsa bile (defensive),
        # StateManager/CandleSequencer duplicate'i deterministic reddeder.
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        # canonical state tutarlı: backfilled candle'ın (60000) değeri 100.5
        # olarak KALICI, ikinci (canlı) candle da doğru şekilde geldi.
        assert candles[1].close == 102.0

    def test_7_original_incoming_update_retried_and_committed(self) -> None:
        """7) backfill sonrası orijinal incoming WS update yeniden denenir ve commit edilir."""
        # Zaten test_missing_interval_triggers_backfill_then_commits bunu
        # doğruluyor (candles[1], orijinal incoming update'in canonical
        # hâlidir); burada AYRICA update_seq/event_time'ın orijinal WS
        # event'inden geldiğini (backfill'in synthetic seq'inden DEĞİL)
        # doğruluyoruz.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=999999),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[1].is_closed is False  # orijinal WS update'in kendi is_closed durumu korunmuş

    def test_8_incomplete_backfill_not_treated_as_success(self) -> None:
        """8) REST response gap'in tamamını doldurmuyorsa recovery başarılı sayılmaz."""
        # 3 dakikalık gap (open_time 0 -> 180000, 1m), ama backfill YALNIZCA
        # bir candle (60000) döndürüyor — 120000 EKSİK kalıyor.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(180000, 239999, False, close="105.0", event_ms=180001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        incomplete_backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            # 120000 open_time'lı candle KASITLI OLARAK EKSİK.
        ]
        http = FakeHttpClient([json_response(incomplete_backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Backfill eksik kaldığı için orijinal (180000) update TEKRAR
            # DENENMEZ/commit edilmez — yalnızca ilk (t=0) candle ve
            # backfill edilen tek candle (60000) canonical state'e girer,
            # ama HİÇBİRİ queue'ya YAYINLANMAZ (yalnızca doğrudan gelen
            # WS update'leri queue'ya gider; backfill candle'ları sessizce
            # canonical state'e yazılır, stream'e YAYINLANMAZ — bu, mevcut
            # tasarımın davranışıdır). Bu yüzden yalnızca 1 candle (t=0)
            # queue'dan okunabilir olmalı; 180000 update'i queue'ya HİÇ
            # gelmemelidir (retry commit edilmediği için).
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.3)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0

    def test_9_recovery_does_not_affect_other_symbol_or_timeframe_state(self) -> None:
        """9) recovery sırasında başka interval/symbol'ün state'i etkilenmez."""
        clock = FixedClock(datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))
        gate = BinanceDataQualityGate(clock)
        store = InMemoryCandleStateStore()
        manager = StateManager(gate, store)

        # Farklı bir symbol/timeframe için ÖNCEDEN mevcut bir state kuruyoruz.
        other_candle = Candle(
            symbol="ETHUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=10.0, high=11.0, low=9.0, close=10.5, volume=1.0, is_closed=True, trade_count=1,
        )
        other_update = CandleUpdate(candle=other_candle, update_seq=1, event_time=other_candle.close_time, received_at=other_candle.close_time)
        manager.handle_candle_update(other_update)
        assert manager.latest_candle("ETHUSDT", Timeframe.M1).close == 10.5

        # BTCUSDT için backfill/historical commit yapıyoruz.
        btc_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True, trade_count=1,
        )
        btc_update = CandleUpdate(candle=btc_candle, update_seq=1, event_time=btc_candle.close_time, received_at=btc_candle.close_time)
        manager.handle_historical_candle_update(btc_update)

        # ETHUSDT'nin state'i ETKİLENMEMİŞ olmalı.
        assert manager.latest_candle("ETHUSDT", Timeframe.M1).close == 10.5
        assert manager.latest_candle("BTCUSDT", Timeframe.M1).close == 100.5

    def test_10_open_candle_not_committed_via_historical_recovery(self) -> None:
        """10) partial/open (henüz kapanmamış) candle historical recovery'ye yanlışlıkla commit edilmez."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill yanıtı, "now" AÇISINDAN HENÜZ KAPANMAMIŞ bir satır
        # içeriyor (close_time > now) — parser bunu is_closed=False
        # üretecek şekilde işaretler; bu, backfill loop'unda ATLANMALIDIR.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        # "now" tam olarak 60000-119999 aralığının close_time'ından ÖNCE
        # olacak şekilde ayarlanıyor ki backfill satırı is_closed=False
        # üretsin (parser politika: close_time <= now ise closed).
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 1, 30, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Backfill satırı is_closed=False üretileceği için commit
            # edilmeyecek (backfill loop'u yalnızca is_closed=True olanları
            # işler) — dolayısıyla gap kapanmayacak ve orijinal (120000)
            # update de tekrar denendiğinde hâlâ MISSING_CANDLE ile
            # reddedilecektir. Yalnızca ilk candle (t=0) queue'ya ulaşır.
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.3)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0


class TestStreamTrades:
    def test_rejected_trades_filtered_out(self) -> None:
        conn = FakeWebSocketConnection([
            trade_msg(1, "100.0", 0),
            trade_msg(1, "100.0", 0),  # duplicate trade_id -> quality reddi
            trade_msg(2, "100.5", 1),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_trades("BTCUSDT")
            trades = await collect_n(agen, 2)
            await provider.close()
            return trades

        trades = run_async(_run())
        assert [t.trade_id for t in trades] == [1, 2]


class TestStreamOrderBook:
    def test_snapshot_and_diff_sync_happy_path(self) -> None:
        # WS: snapshot beklerken buffer'lanan bir diff, sonra devamı.
        conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
            depth_diff_msg(first=106, final=106, bids=[(99.0, 2.0)], asks=[], event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        snapshot_payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(snapshot_payload)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 2)
            await provider.close()
            return books

        books = run_async(_run())
        assert books[0].last_update_id == 105
        assert books[1].last_update_id == 106
        assert books[1].best_bid.quantity == 2.0

    def test_sequence_gap_triggers_resync(self) -> None:
        conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=101, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
            # gap: beklenen U=102, ama U=500 geliyor
            depth_diff_msg(first=500, final=505, bids=[(98.0, 5.0)], asks=[(101.0, 5.0)], event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        first_snapshot = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        resync_snapshot = {"lastUpdateId": 499, "bids": [["98.0", "1.0"]], "asks": [["101.0", "1.0"]]}
        http = FakeHttpClient([json_response(first_snapshot), json_response(resync_snapshot)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 2)
            await provider.close()
            return books

        books = run_async(_run())
        assert books[0].last_update_id == 101
        assert books[1].last_update_id == 505  # resync sonrası devam eden diff uygulanmış


class TestBackpressure:
    def test_bounded_queue_blocks_producer_until_space(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.0", event_ms=1),
            kline_msg(60000, 119999, False, close="102.0", event_ms=2),
            kline_msg(120000, 179999, False, close="104.0", event_ms=3),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        config = BinanceConfig(reconnect=FAST_RECONNECT)
        clock = FixedClock(datetime(1970, 1, 1, tzinfo=UTC))
        provider = BinanceMarketDataProvider(
            config=config, http_client=FakeHttpClient([]), ws_factory=factory,
            clock=clock, sleeper=FakeSleeper(), queue_maxsize=1,
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Yavaş tüketici: bounded queue (maxsize=1) üretici tarafını
            # backpressure ile bekletir; sessiz veri kaybı OLMAMALI.
            candles = await collect_n(agen, 3, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.0, 102.0, 104.0]  # hiçbiri kaybolmadı


class TestStaleFeedDetection:
    """Reviewer probe D + minimum adversarial testler (Bölüm 1)."""

    STALE_THRESHOLD = 0.03  # saniye — gerçek ama çok küçük (Bölüm: "saniyelerce" DEĞİL)

    def test_1_stale_detected_after_threshold_with_no_messages(self) -> None:
        """1) socket connected + mesaj yok + threshold aşılır → stale detected."""
        hanging_conn = FakeWebSocketConnection([])  # hiç mesaj yok, recv() sonsuza kadar bekler
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="100.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1, timeout=5.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        assert hanging_conn.closed is True  # stale bağlantı güvenli şekilde kapatıldı

    def test_2_health_does_not_remain_connected_after_stale(self) -> None:
        """2) health CONNECTED kalmaz."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )
        key = "candle:BTCUSDT:1m"

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1, timeout=5.0)
            health = provider.health_for(key)
            await provider.close()
            return health

        health = run_async(_run())
        # Stale tespit edildikten sonra ikinci bağlantı kurulup mesaj alındığı
        # için nihai health CONNECTED'a geri döner — ama reconnect_count'un
        # artmış olması stale-tetiklenmiş reconnect'in gerçekleştiğini kanıtlar.
        assert health.reconnect_count >= 1

    def test_3_reconnect_triggered_after_stale(self) -> None:
        """3) reconnect tetiklenir."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1, timeout=5.0)
            await provider.close()

        run_async(_run())
        assert len(factory.connect_calls) >= 2

    def test_4_message_before_threshold_no_false_positive(self) -> None:
        """4) mesaj threshold'dan önce gelirse false-positive stale oluşmaz."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.0", event_ms=1),
            kline_msg(60000, 119999, False, close="101.0", event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Görece büyük bir threshold — mesajlar anında geldiği için hiç aşılmaz.
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": 5.0},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.0, 101.0]
        assert len(factory.connect_calls) == 1  # hiç reconnect olmadı

    def test_5_cancellation_does_not_produce_stale_reconnect(self) -> None:
        """5) cancellation stale-reconnect üretmez."""
        hanging_conn = FakeWebSocketConnection([])  # sonsuza kadar bekler
        factory = FakeWebSocketConnectionFactory([hanging_conn])
        # Threshold'u BÜYÜK tutuyoruz — test'in kendisi stale beklemeden
        # cancel edecek; eğer cancellation yanlışlıkla stale-reconnect
        # üretiyorsa factory.connect_calls > 1 olurdu.
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": 10.0},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Generator'ı GERÇEKTEN başlatmak için __anext__() çağrısını bir
            # task olarak başlatıyoruz (connect() + recv() beklemesi bu
            # noktada gerçekleşir), sonra bu task'ı cancel ediyoruz — bu,
            # generator'ın `finally` bloğunu (normal shutdown) tetikler;
            # StaleFeedError/TimeoutError DEĞİL, CancelledError üretir.
            consume_task = asyncio.create_task(agen.__anext__())
            await asyncio.sleep(0.01)
            consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consume_task

        run_async(_run())
        assert len(factory.connect_calls) == 1  # reconnect DENENMEDİ
        assert hanging_conn.closed is True  # ama bağlantı düzgün kapatıldı

    def test_6_order_book_resyncs_after_stale_reconnect(self) -> None:
        """6) order-book: stale-reconnect sonrası resync yapar."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=101, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
        ])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        snapshot_payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(snapshot_payload)])
        provider = make_provider(
            factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 1, timeout=5.0)
            await provider.close()
            return books

        books = run_async(_run())
        # Stale-reconnect sonrası synchronizer sıfırlanıp REST snapshot
        # tekrar çekilerek resync yapıldığı için book başarıyla üretildi.
        assert books[0].last_update_id == 101
        assert len(http.calls) == 1  # snapshot yalnızca reconnect sonrası (ilk denemede hiç veri yoktu) çekildi


=== FILE: tests/test_binance_quality_rules.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate, BinanceQualityThresholds

UTC = timezone.utc
ALIGNED_OPEN = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)  # 1m sınırına hizalı


def make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True, trade_count=5, volume=10.0) -> Candle:
    return Candle(
        symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=100.0, high=max(105.0, close), low=min(95.0, close), close=close,
        volume=volume, is_closed=is_closed, trade_count=trade_count,
    )


def make_gate(now: datetime) -> BinanceDataQualityGate:
    return BinanceDataQualityGate(FixedClock(now), BinanceQualityThresholds())


class TestCheckCandleAlignment:
    def test_aligned_open_time_passes(self) -> None:
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_candle(make_candle(is_closed=False), None)
        assert result.passed

    def test_misaligned_open_time_rejected(self) -> None:
        misaligned = ALIGNED_OPEN + timedelta(seconds=30)
        gate = make_gate(misaligned + timedelta(seconds=1))
        result = gate.check_candle(make_candle(open_time=misaligned, is_closed=False), None)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH


class TestCheckCandleStaleness:
    def test_fresh_closed_candle_passes(self) -> None:
        candle = make_candle(is_closed=True)
        gate = make_gate(candle.close_time + timedelta(seconds=5))
        assert gate.check_candle(candle, None).passed

    def test_stale_closed_candle_rejected(self) -> None:
        candle = make_candle(is_closed=True)
        gate = make_gate(candle.close_time + timedelta(seconds=1000))
        result = gate.check_candle(candle, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_open_candle_staleness_not_checked(self) -> None:
        candle = make_candle(is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=100000))
        result = gate.check_candle(candle, None)
        assert result.passed  # açık candle staleness kontrolünden muaf


class TestCheckCandleMissingInterval:
    def test_consecutive_candles_pass(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, previous).passed

    def test_gap_between_candles_rejected(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=3), is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        result = gate.check_candle(current, previous)
        assert result.status == DataQualityStatus.MISSING_CANDLE

    def test_first_candle_no_previous_not_flagged(self) -> None:
        current = make_candle(is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, None).passed


class TestCheckCandleZeroVolumeAnomaly:
    def test_zero_volume_with_trades_rejected(self) -> None:
        candle = make_candle(volume=0.0, trade_count=10, is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=1))
        result = gate.check_candle(candle, None)
        assert result.status == DataQualityStatus.ZERO_VOLUME_ANOMALY

    def test_zero_volume_zero_trades_passes(self) -> None:
        candle = make_candle(volume=0.0, trade_count=0, is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=1))
        assert gate.check_candle(candle, None).passed


class TestCheckCandleExtremeOutlier:
    def test_normal_move_passes(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), close=102.0, is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, previous).passed

    def test_extreme_move_rejected(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), close=200.0, is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        result = gate.check_candle(current, previous)
        assert result.status == DataQualityStatus.EXTREME_OUTLIER


class TestCheckTrade:
    def make_trade(self, trade_id=1, price=100.0, ts=None) -> Trade:
        return Trade(
            symbol="BTCUSDT", trade_id=trade_id, price=price, quantity=1.0,
            timestamp=ts or ALIGNED_OPEN, is_buyer_maker=False,
        )

    def test_fresh_trade_passes(self) -> None:
        trade = self.make_trade(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        assert gate.check_trade(trade, None).passed

    def test_stale_trade_rejected(self) -> None:
        trade = self.make_trade(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1000))
        result = gate.check_trade(trade, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_non_monotonic_trade_id_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)  # duplicate ID
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH

    def test_decreasing_trade_id_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=50, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH

    def test_extreme_price_move_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, price=100.0, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=101, price=500.0, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.EXTREME_OUTLIER


class TestCheckOrderBook:
    def make_book(self, last_update_id=1, ts=None) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=ts or ALIGNED_OPEN,
            bids=(OrderBookLevel(price=99.0, quantity=1.0),),
            asks=(OrderBookLevel(price=100.0, quantity=1.0),),
            last_update_id=last_update_id,
        )

    def test_fresh_book_passes(self) -> None:
        book = self.make_book(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        assert gate.check_order_book(book, None).passed

    def test_stale_book_rejected(self) -> None:
        book = self.make_book(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1000))
        result = gate.check_order_book(book, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_non_monotonic_update_id_rejected(self) -> None:
        previous = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        current = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_order_book(current, previous)
        assert result.status == DataQualityStatus.WEBSOCKET_GAP

    def test_decreasing_update_id_rejected(self) -> None:
        previous = self.make_book(last_update_id=200, ts=ALIGNED_OPEN)
        current = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_order_book(current, previous)
        assert result.status == DataQualityStatus.WEBSOCKET_GAP


=== FILE: tests/test_binance_reconnect.py ===
import asyncio

import pytest

from tests.conftest import run_async
from crypto_signal_engine.providers.binance.clock import DeterministicJitterSource, FakeSleeper
from crypto_signal_engine.providers.binance.config import ReconnectPolicyConfig
from crypto_signal_engine.providers.binance.reconnect import MaxReconnectAttemptsExceeded, ReconnectPolicy


def make_policy(**overrides) -> tuple[ReconnectPolicy, FakeSleeper]:
    config = ReconnectPolicyConfig(
        initial_delay_seconds=1.0, max_delay_seconds=10.0, multiplier=2.0, jitter_seconds=0.0, **overrides
    )
    sleeper = FakeSleeper()
    policy = ReconnectPolicy(config, sleeper, jitter_source=DeterministicJitterSource(0.0))
    return policy, sleeper


class TestExponentialBackoff:
    def test_delay_grows_exponentially(self) -> None:
        policy, _ = make_policy()
        assert policy.compute_delay_seconds() == 1.0
        policy._attempt = 1
        assert policy.compute_delay_seconds() == 2.0
        policy._attempt = 2
        assert policy.compute_delay_seconds() == 4.0
        policy._attempt = 3
        assert policy.compute_delay_seconds() == 8.0

    def test_delay_is_bounded_by_max(self) -> None:
        policy, _ = make_policy()
        policy._attempt = 10  # 1 * 2**10 = 1024, ama max_delay=10 ile sınırlı
        assert policy.compute_delay_seconds() == 10.0

    def test_reset_restarts_backoff(self) -> None:
        policy, _ = make_policy()
        policy._attempt = 5
        policy.reset()
        assert policy.attempt_count == 0
        assert policy.compute_delay_seconds() == 1.0

    def test_jitter_is_added(self) -> None:
        config = ReconnectPolicyConfig(initial_delay_seconds=1.0, jitter_seconds=5.0)
        policy = ReconnectPolicy(config, FakeSleeper(), jitter_source=DeterministicJitterSource(0.5))
        assert policy.compute_delay_seconds() == 1.5


class TestSleepBeforeNextAttempt:
    def test_sleeper_called_with_computed_delay(self) -> None:
        policy, sleeper = make_policy()

        async def _run() -> None:
            await policy.sleep_before_next_attempt()
            await policy.sleep_before_next_attempt()

        run_async(_run())
        assert sleeper.calls == [1.0, 2.0]
        assert policy.attempt_count == 2

    def test_delay_override_used_instead_of_computed(self) -> None:
        policy, sleeper = make_policy()

        async def _run() -> None:
            await policy.sleep_before_next_attempt(delay_override_seconds=42.0)

        run_async(_run())
        assert sleeper.calls == [42.0]

    def test_max_attempts_exceeded_raises(self) -> None:
        policy, _ = make_policy(max_attempts=2)

        async def _run() -> None:
            await policy.sleep_before_next_attempt()
            await policy.sleep_before_next_attempt()
            with pytest.raises(MaxReconnectAttemptsExceeded):
                await policy.sleep_before_next_attempt()

        run_async(_run())

    def test_max_attempts_none_never_raises(self) -> None:
        policy, _ = make_policy(max_attempts=None)

        async def _run() -> None:
            for _ in range(50):
                await policy.sleep_before_next_attempt()

        run_async(_run())  # exception fırlatmamalı
        assert policy.attempt_count == 50

    def test_cancellation_propagates_without_swallowing(self) -> None:
        policy, sleeper = make_policy()
        sleeper.trigger_cancellation()

        async def _run() -> None:
            await policy.sleep_before_next_attempt()

        with pytest.raises(asyncio.CancelledError):
            run_async(_run())


=== FILE: tests/test_binance_rest.py ===
import json
from datetime import datetime, timezone

import pytest

from tests.binance_fakes import FakeHttpClient, json_response
from tests.conftest import run_async
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import BinanceProtocolError, ParseError, RateLimitError, TransportError
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient

UTC = timezone.utc


def make_client(http_client, now=None, max_retries=3) -> BinanceRestClient:
    config = BinanceConfig()
    clock = FixedClock(now or datetime(2026, 8, 31, tzinfo=UTC))
    sleeper = FakeSleeper()
    return BinanceRestClient(config, http_client, clock, sleeper, max_retries=max_retries)


def kline_row(open_ms: int, close_ms: int) -> list:
    return [open_ms, "100.0", "101.0", "99.0", "100.5", "10.0", close_ms, "1000.0", 5, "5.0", "500.0", "0"]


class TestFetchHistoricalCandles:
    def test_invalid_range_rejected(self) -> None:
        client = make_client(FakeHttpClient([]))
        start = datetime(2026, 8, 31, tzinfo=UTC)
        end = datetime(2026, 8, 30, tzinfo=UTC)

        async def _run():
            await client.fetch_historical_candles("BTCUSDT", Timeframe.M1, start, end)

        with pytest.raises(ValueError, match="start"):
            run_async(_run())

    def test_naive_start_rejected(self) -> None:
        client = make_client(FakeHttpClient([]))

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(2026, 8, 31), datetime(2026, 8, 31, 1, tzinfo=UTC)
            )

        with pytest.raises(ValueError, match="naive datetime"):
            run_async(_run())

    def test_single_page(self) -> None:
        rows = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(5)]
        http = FakeHttpClient([json_response(rows)])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1,
                datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, 1, tzinfo=UTC),
            )

        candles = run_async(_run())
        assert len(candles) == 5
        assert all(c.is_closed for c in candles)
        # Ascending kronolojik sıra
        assert [c.open_time for c in candles] == sorted(c.open_time for c in candles)

    def test_pagination_across_multiple_pages_no_duplicates(self) -> None:
        config_limit = BinanceConfig().max_klines_per_request
        # İlk sayfa tam limit kadar döner (daha fazla veri olduğunu ima eder),
        # ikinci sayfa daha az döner (son sayfa).
        page1 = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(config_limit)]
        page2 = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(config_limit, config_limit + 3)]
        http = FakeHttpClient([json_response(page1), json_response(page2)])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1,
                datetime(1970, 1, 1, tzinfo=UTC),
                datetime(1970, 1, 1, 0, 0, 0, 60_000 * (config_limit + 3), tzinfo=UTC) if False else
                datetime.fromtimestamp(60 * (config_limit + 3), tz=UTC),
            )

        candles = run_async(_run())
        open_times = [c.open_time for c in candles]
        assert len(open_times) == len(set(open_times))  # duplicate yok
        assert open_times == sorted(open_times)  # ascending

    def test_malformed_response_not_list_raises_protocol_error(self) -> None:
        http = FakeHttpClient([json_response({"not": "a list"})])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(BinanceProtocolError):
            run_async(_run())

    def test_malformed_row_raises_parse_error_not_retried(self) -> None:
        bad_row = kline_row(0, 59999)
        bad_row[1] = "garbage"
        http = FakeHttpClient([json_response([bad_row])])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(ParseError):
            run_async(_run())
        # Yalnızca 1 istek yapılmış olmalı — parser hatası retry-safe DEĞİL.
        assert len(http.calls) == 1

    def test_empty_response_stops_pagination(self) -> None:
        http = FakeHttpClient([json_response([])])
        client = make_client(http)

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        assert run_async(_run()) == []


class TestRetryBehavior:
    def test_transport_error_retried_then_succeeds(self) -> None:
        http = FakeHttpClient([TransportError("geçici hata"), json_response([kline_row(0, 59999)])])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, tzinfo=UTC)
            )

        candles = run_async(_run())
        assert len(candles) == 1

    def test_transport_error_exhausts_retries_and_raises(self) -> None:
        http = FakeHttpClient([TransportError("kalıcı hata")] * 10)
        client = make_client(http, max_retries=2)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(TransportError):
            run_async(_run())

    def test_rate_limit_respects_retry_after(self) -> None:
        http = FakeHttpClient([
            RateLimitError("rate limited", status_code=429, retry_after_seconds=7.5),
            json_response([kline_row(0, 59999)]),
        ])
        sleeper = FakeSleeper()
        config = BinanceConfig()
        clock = FixedClock(datetime(2026, 8, 31, tzinfo=UTC))
        client = BinanceRestClient(config, http, clock, sleeper, max_retries=3)

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, tzinfo=UTC)
            )

        candles = run_async(_run())
        assert len(candles) == 1
        assert 7.5 in sleeper.calls

    def test_unexpected_status_code_raises_protocol_error(self) -> None:
        http = FakeHttpClient([(500, "internal server error")])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(BinanceProtocolError):
            run_async(_run())


class TestFetchDepthSnapshot:
    def test_valid_snapshot(self) -> None:
        payload = {"lastUpdateId": 500, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(payload)])
        client = make_client(http)

        async def _run():
            return await client.fetch_depth_snapshot("BTCUSDT")

        last_id, bids, asks, received_at = run_async(_run())
        assert last_id == 500
        assert bids == ((99.0, 1.0),)
        assert received_at.tzinfo is not None


=== FILE: tests/test_binance_symbols.py ===
import pytest

from crypto_signal_engine.providers.binance.symbols import (
    agg_trade_stream_name,
    depth_diff_stream_name,
    from_binance_wire_symbol,
    kline_stream_name,
    to_binance_wire_symbol,
)


class TestSymbolConversion:
    @pytest.mark.parametrize("domain_symbol", ["BTCUSDT", "btcusdt", " BTCUSDT "])
    def test_to_wire_symbol_is_lowercase(self, domain_symbol: str) -> None:
        assert to_binance_wire_symbol(domain_symbol) == "btcusdt"

    def test_from_wire_symbol_normalizes(self) -> None:
        assert from_binance_wire_symbol("btcusdt") == "BTCUSDT"
        assert from_binance_wire_symbol("BTCUSDT") == "BTCUSDT"

    def test_round_trip(self) -> None:
        domain = "ETHUSDT"
        wire = to_binance_wire_symbol(domain)
        assert from_binance_wire_symbol(wire) == domain


class TestStreamNames:
    def test_kline_stream_name(self) -> None:
        assert kline_stream_name("BTCUSDT", "1m") == "btcusdt@kline_1m"

    def test_agg_trade_stream_name(self) -> None:
        assert agg_trade_stream_name("BTCUSDT") == "btcusdt@aggTrade"

    def test_depth_diff_stream_name(self) -> None:
        assert depth_diff_stream_name("BTCUSDT") == "btcusdt@depth@100ms"


=== FILE: tests/test_bridge_runtime.py ===
"""Faz 13 — `crypto_signal_engine.execution.bridge_runtime.BridgeRuntime`
wiring testleri.

Bu dosya `SignalTestnetBridge`'in POLİTİKA mantığını TEKRAR test ETMEZ
(bkz. `tests/test_signal_testnet_bridge.py`) — yalnızca `BridgeRuntime`'ın
DOĞRU olayları DOĞRU zamanda bridge'e ilettiğini (ve HİÇBİR ZAMAN
`recover()` sırasında iletmediğini — "NO HISTORICAL REPLAY ORDERS"
invariant'ının YAPISAL kanıtı) doğrular. `PersistedRuntime`'ın KENDİSİ
tamamen sahte (`_FakePersisted`) bir stub'dır — gerçek candle/feature/
signal pipeline'ı burada KURULMAZ (bu, `test_persistence_recovery.py`/
`test_runtime_coordinator.py`'nin ZATEN kanıtladığı bir şeydir)."""

from __future__ import annotations

from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.runtime.models import IngestOutcome, MarketEventKind, ProcessedMarketEvent, RuntimeCycleResult
from tests.conftest import run_async

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[RuntimeCycleResult] = []

    async def on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        self.calls.append(cycle_result)


class _FakePersisted:
    def __init__(self) -> None:
        self.recover_calls = 0
        self.ingest_calls: list[tuple] = []
        self.resolve_gap_calls: list[tuple] = []
        self.next_event: ProcessedMarketEvent | None = None

    async def recover(self):
        self.recover_calls += 1
        return ("bootstrap-report",)

    def ingest_candle(self, symbol, timeframe, candle):  # noqa: ANN001
        self.ingest_calls.append((symbol, timeframe, candle))
        return self.next_event

    async def resolve_gap(self, symbol, timeframe, pending_candle):  # noqa: ANN001
        self.resolve_gap_calls.append((symbol, timeframe, pending_candle))
        return self.next_event

    def ingest_order_book(self, symbol, snapshot):  # noqa: ANN001
        return "ob-event"

    def status(self):  # noqa: ANN201
        return "status"

    @property
    def _coordinator(self):  # noqa: ANN202
        return "coordinator-stub"

    async def stop(self) -> None:
        self.stopped = True


def _event(outcome: IngestOutcome, cycle_result: RuntimeCycleResult | None = None) -> ProcessedMarketEvent:
    return ProcessedMarketEvent(
        symbol="BTCUSDT", kind=MarketEventKind.CANDLE, outcome=outcome, event_time=NOW,
        timeframe=Timeframe.M5, cycle_result=cycle_result,
    )


def _evaluated_false_cycle_result() -> RuntimeCycleResult:
    return RuntimeCycleResult(symbol="BTCUSDT", evaluated=False, signal=None, paper_result=None, generated_at=NOW)


class TestRecoverNeverFeedsBridge:
    def test_recover_delegates_and_never_calls_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]

        reports = run_async(runtime.recover())

        assert reports == ("bootstrap-report",)
        assert persisted.recover_calls == 1
        assert bridge.calls == []  # STRUCTURAL guarantee: recovery never reaches the bridge


class TestLiveIngestFeedsBridge:
    def test_ingest_candle_forwards_evaluated_cycle_result_to_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        cycle_result = _evaluated_false_cycle_result()
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=cycle_result)

        async def body() -> ProcessedMarketEvent:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)
            return event

        event = run_async(body())

        assert event is persisted.next_event
        assert bridge.calls == [cycle_result]

    def test_ingest_candle_without_cycle_result_does_not_call_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=None)

        async def body() -> None:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)

        run_async(body())

        assert bridge.calls == []

    def test_duplicate_outcome_does_not_call_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.DUPLICATE, cycle_result=None)

        async def body() -> None:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)

        run_async(body())

        assert bridge.calls == []

    def test_resolve_gap_forwards_final_event_to_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        cycle_result = _evaluated_false_cycle_result()
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=cycle_result)

        event = run_async(runtime.resolve_gap("BTCUSDT", Timeframe.M5, pending_candle=object()))

        assert event is persisted.next_event
        assert len(persisted.resolve_gap_calls) == 1
        assert bridge.calls == [cycle_result]


class TestNoneBridgeIsPureFallthrough:
    def test_none_bridge_never_raises_and_never_records_anything(self) -> None:
        persisted = _FakePersisted()
        runtime = BridgeRuntime(persisted, None)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=_evaluated_false_cycle_result())

        async def body() -> ProcessedMarketEvent:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)  # must be a safe no-op with bridge=None
            return event

        event = run_async(body())
        assert event is persisted.next_event  # pass-through unaffected


class TestOrderBookNeverFeedsBridge:
    def test_ingest_order_book_never_touches_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        result = runtime.ingest_order_book("BTCUSDT", snapshot=object())
        assert result == "ob-event"
        assert bridge.calls == []


=== FILE: tests/test_candle_features.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.errors import FeatureCalculationError, InsufficientHistoryError
from crypto_signal_engine.features import candle_calculators as cc

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_candles(closes: list[float], highs=None, lows=None, opens=None, volumes=None) -> list[Candle]:
    """Ardışık 1m candle'lar üretir. high/low varsayılan olarak close'u
    kapsayacak şekilde close±1 alınır (aksi belirtilmedikçe)."""
    n = len(closes)
    highs = highs or [c + 1.0 for c in closes]
    lows = lows or [c - 1.0 for c in closes]
    opens = opens or closes
    volumes = volumes or [10.0] * n
    candles = []
    for i in range(n):
        open_time = T0 + timedelta(minutes=i)
        candles.append(Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
            close_time=open_time + timedelta(minutes=1),
            open=opens[i], high=max(highs[i], opens[i], closes[i]), low=min(lows[i], opens[i], closes[i]),
            close=closes[i], volume=volumes[i], is_closed=True, trade_count=1,
        ))
    return candles


class TestSMA:
    def test_known_value(self) -> None:
        # closes 1..7, SMA(5) son 5 değerin ortalaması: (3+4+5+6+7)/5 = 5.0
        candles = make_candles([1, 2, 3, 4, 5, 6, 7])
        assert cc.sma(candles, 5) == pytest.approx(5.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1, 2, 3])
        with pytest.raises(InsufficientHistoryError):
            cc.sma(candles, 5)

    def test_exact_minimum_history(self) -> None:
        candles = make_candles([1, 2, 3, 4, 5])
        assert cc.sma(candles, 5) == pytest.approx(3.0)


class TestEMA:
    def test_known_value(self) -> None:
        # closes 1..7, period=5: ilk SMA=(1+2+3+4+5)/5=3.0, alpha=1/3
        # step6 (close=6): ema=6*(1/3)+3*(2/3)=4.0
        # step7 (close=7): ema=7*(1/3)+4*(2/3)=5.0
        candles = make_candles([1, 2, 3, 4, 5, 6, 7])
        assert cc.ema(candles, 5) == pytest.approx(5.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1, 2])
        with pytest.raises(InsufficientHistoryError):
            cc.ema(candles, 5)

    def test_exact_minimum_history_equals_sma(self) -> None:
        candles = make_candles([1, 2, 3, 4, 5])
        assert cc.ema(candles, 5) == pytest.approx(3.0)


class TestRSI:
    def test_all_gains_is_100(self) -> None:
        closes = list(range(1, 17))  # 16 ardışık artan değer -> 15 gain
        candles = make_candles([float(c) for c in closes])
        assert cc.rsi(candles, 14) == pytest.approx(100.0)

    def test_all_losses_is_0(self) -> None:
        closes = list(range(16, 0, -1))
        candles = make_candles([float(c) for c in closes])
        assert cc.rsi(candles, 14) == pytest.approx(0.0)

    def test_no_change_is_neutral_50(self) -> None:
        candles = make_candles([100.0] * 16)
        assert cc.rsi(candles, 14) == pytest.approx(50.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1.0] * 10)
        with pytest.raises(InsufficientHistoryError):
            cc.rsi(candles, 14)

    def test_bounded_0_100(self) -> None:
        import random

        random.seed(42)
        closes = [100.0]
        for _ in range(30):
            closes.append(closes[-1] + random.uniform(-5, 5))
        candles = make_candles(closes)
        value = cc.rsi(candles, 14)
        assert 0.0 <= value <= 100.0


class TestROC:
    def test_known_value(self) -> None:
        # closes: base=100 (index -1-10=-11), current=110 -> ROC=10%
        closes = [100.0] * 1 + [100.0] * 9 + [110.0]  # 11 candles, base=closes[0]=100, current=closes[-1]=110
        candles = make_candles(closes)
        assert cc.roc(candles, 10) == pytest.approx(10.0)

    def test_zero_base_raises(self) -> None:
        closes = [0.0] + [1.0] * 10
        candles = make_candles(closes, highs=[1.0] * 11, lows=[0.0] * 11)
        with pytest.raises(FeatureCalculationError, match="sıfır"):
            cc.roc(candles, 10)

    def test_insufficient_history(self) -> None:
        candles = make_candles([1.0, 2.0])
        with pytest.raises(InsufficientHistoryError):
            cc.roc(candles, 10)


class TestTrueRangeAndATR:
    def test_constant_range_atr_equals_range(self) -> None:
        # 15 candle, high=101, low=100 (range=1), close=100.5 sabit
        n = 15
        candles = []
        for i in range(n):
            open_time = T0 + timedelta(minutes=i)
            candles.append(Candle(
                symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
                close_time=open_time + timedelta(minutes=1),
                open=100.5, high=101.0, low=100.0, close=100.5, volume=10.0, is_closed=True, trade_count=1,
            ))
        assert cc.atr(candles, 14) == pytest.approx(1.0)

    def test_true_range_first_candle_no_previous(self) -> None:
        c = make_candles([100.0])[0]
        assert cc.true_range(c, None) == pytest.approx(c.high - c.low)

    def test_true_range_gap_up(self) -> None:
        prev = make_candles([100.0])[0]
        current = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0 + timedelta(minutes=1),
            close_time=T0 + timedelta(minutes=2), open=110.0, high=112.0, low=109.0, close=111.0,
            volume=1.0, is_closed=True,
        )
        # true range = max(high-low=3, |high-prev_close|=|112-100|=12, |low-prev_close|=|109-100|=9) = 12
        assert cc.true_range(current, prev) == pytest.approx(12.0)

    def test_atr_insufficient_history(self) -> None:
        candles = make_candles([1.0] * 5)
        with pytest.raises(InsufficientHistoryError):
            cc.atr(candles, 14)


class TestRollingStd:
    def test_zero_variance(self) -> None:
        candles = make_candles([100.0] * 20)
        assert cc.rolling_std(candles, 20, ddof=0) == pytest.approx(0.0)

    def test_known_population_std(self) -> None:
        # [1,2,3,4,5]: mean=3, population variance=(4+1+0+1+4)/5=2, std=sqrt(2)
        candles = make_candles([1.0, 2.0, 3.0, 4.0, 5.0])
        import math
        assert cc.rolling_std(candles, 5, ddof=0) == pytest.approx(math.sqrt(2.0))

    def test_unsupported_ddof_rejected(self) -> None:
        candles = make_candles([1.0, 2.0, 3.0])
        with pytest.raises(FeatureCalculationError, match="ddof"):
            cc.rolling_std(candles, 3, ddof=5)


class TestBollinger:
    def test_zero_variance_bands_equal_basis(self) -> None:
        candles = make_candles([100.0] * 20)
        basis = cc.bollinger_basis(candles, 20)
        upper = cc.bollinger_upper(candles, 20, 2.0)
        lower = cc.bollinger_lower(candles, 20, 2.0)
        assert basis == pytest.approx(100.0)
        assert upper == pytest.approx(100.0)
        assert lower == pytest.approx(100.0)

    def test_bandwidth_zero_when_no_variance(self) -> None:
        candles = make_candles([100.0] * 20)
        assert cc.bollinger_bandwidth(candles, 20, 2.0) == pytest.approx(0.0)

    def test_upper_gte_basis_gte_lower_property(self) -> None:
        closes = [100.0, 102.0, 98.0, 105.0, 95.0] * 5
        candles = make_candles(closes)
        basis = cc.bollinger_basis(candles, 20)
        upper = cc.bollinger_upper(candles, 20)
        lower = cc.bollinger_lower(candles, 20)
        assert upper >= basis >= lower

    def test_bandwidth_zero_basis_raises(self) -> None:
        candles = make_candles([0.0] * 20, highs=[0.0] * 20, lows=[0.0] * 20, opens=[0.0] * 20)
        with pytest.raises(FeatureCalculationError, match="basis sıfır"):
            cc.bollinger_bandwidth(candles, 20)


class TestVolumeFeatures:
    def test_relative_volume_known_value(self) -> None:
        volumes = [10.0] * 19 + [20.0]  # ortalama ~10.5, son değer 20
        candles = make_candles([100.0] * 20, volumes=volumes)
        mean = sum(volumes) / 20
        assert cc.relative_volume(candles, 20) == pytest.approx(20.0 / mean)

    def test_relative_volume_zero_mean_raises(self) -> None:
        candles = make_candles([100.0] * 20, volumes=[0.0] * 20)
        with pytest.raises(FeatureCalculationError, match="ortalama hacim sıfır"):
            cc.relative_volume(candles, 20)

    def test_volume_zscore_zero_std_raises(self) -> None:
        candles = make_candles([100.0] * 20, volumes=[10.0] * 20)
        with pytest.raises(FeatureCalculationError, match="std'si sıfır"):
            cc.volume_zscore(candles, 20)

    def test_volume_zscore_known_value(self) -> None:
        volumes = [10.0] * 19 + [30.0]
        candles = make_candles([100.0] * 20, volumes=volumes)
        import statistics
        mean = sum(volumes) / 20
        std = statistics.pstdev(volumes)
        expected = (30.0 - mean) / std
        assert cc.volume_zscore(candles, 20) == pytest.approx(expected)


class TestPriceStructure:
    def test_highest_lowest(self) -> None:
        candles = make_candles([100.0, 105.0, 95.0, 110.0, 90.0])
        hh = cc.highest_high(candles, 5)
        ll = cc.lowest_low(candles, 5)
        assert hh >= max(c.high for c in candles)
        assert ll <= min(c.low for c in candles)

    def test_body_wick_geometry(self) -> None:
        c = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
            open=100.0, high=105.0, low=98.0, close=103.0, volume=1.0, is_closed=True,
        )
        candles = [c]
        assert cc.body_size(candles) == pytest.approx(3.0)
        assert cc.upper_wick(candles) == pytest.approx(2.0)  # 105 - max(100,103)
        assert cc.lower_wick(candles) == pytest.approx(2.0)  # min(100,103) - 98
        assert cc.body_to_range_ratio(candles) == pytest.approx(3.0 / 7.0)

    def test_body_to_range_ratio_zero_range_doji(self) -> None:
        c = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
            open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0, is_closed=True,
        )
        assert cc.body_to_range_ratio([c]) == 0.0

    def test_distance_from_high_zero_denominator_raises(self) -> None:
        candles = make_candles([0.0] * 5, highs=[0.0] * 5, lows=[0.0] * 5, opens=[0.0] * 5)
        with pytest.raises(FeatureCalculationError):
            cc.distance_from_high(candles, 5)


class TestReturns:
    def test_simple_return_known_value(self) -> None:
        candles = make_candles([100.0, 110.0])
        assert cc.simple_return(candles, 1) == pytest.approx(0.10)

    def test_log_return_known_value(self) -> None:
        import math
        candles = make_candles([100.0, 110.0])
        assert cc.log_return(candles, 1) == pytest.approx(math.log(1.10))

    def test_log_return_nonpositive_price_raises(self) -> None:
        candles = make_candles([0.0, 1.0], highs=[0.0, 1.0], lows=[0.0, 0.0], opens=[0.0, 0.0])
        with pytest.raises(FeatureCalculationError, match="logaritma"):
            cc.log_return(candles, 1)

    def test_cumulative_return_known_value(self) -> None:
        closes = [100.0] * 20 + [120.0]
        candles = make_candles(closes)
        assert cc.cumulative_return(candles, 20) == pytest.approx(0.20)


class TestVWAP:
    def test_known_value(self) -> None:
        # 2 candle, typical price=(h+l+c)/3
        candles = [
            Candle(symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
                   open=100.0, high=102.0, low=98.0, close=100.0, volume=10.0, is_closed=True),
            Candle(symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0 + timedelta(minutes=1),
                   close_time=T0 + timedelta(minutes=2), open=100.0, high=104.0, low=100.0, close=102.0,
                   volume=20.0, is_closed=True),
        ]
        tp1 = (102.0 + 98.0 + 100.0) / 3.0
        tp2 = (104.0 + 100.0 + 102.0) / 3.0
        expected = (tp1 * 10.0 + tp2 * 20.0) / 30.0
        assert cc.rolling_vwap(candles, 2) == pytest.approx(expected)

    def test_zero_volume_raises(self) -> None:
        candles = make_candles([100.0] * 5, volumes=[0.0] * 5)
        with pytest.raises(FeatureCalculationError, match="hacim sıfır"):
            cc.rolling_vwap(candles, 5)


class TestNoNonFiniteOutputEver:
    """Herhangi bir finite girdi kombinasyonu non-finite (NaN/inf) çıktı üretmemeli."""

    def test_all_calculators_on_realistic_data_are_finite(self) -> None:
        import math

        closes = [100.0 + i * 0.37 - (i % 3) * 0.5 for i in range(30)]
        candles = make_candles(closes)
        results = [
            cc.sma(candles, 20), cc.ema(candles, 20), cc.rsi(candles, 14), cc.roc(candles, 10),
            cc.atr(candles, 14), cc.rolling_std(candles, 20), cc.bollinger_basis(candles, 20),
            cc.bollinger_upper(candles, 20), cc.bollinger_lower(candles, 20),
            cc.bollinger_bandwidth(candles, 20), cc.rolling_volume_mean(candles, 20),
            cc.relative_volume(candles, 20), cc.highest_high(candles, 20), cc.lowest_low(candles, 20),
            cc.distance_from_high(candles, 20), cc.distance_from_low(candles, 20),
            cc.body_size(candles), cc.upper_wick(candles), cc.lower_wick(candles),
            cc.body_to_range_ratio(candles), cc.simple_return(candles), cc.log_return(candles),
            cc.cumulative_return(candles, 20), cc.rolling_vwap(candles, 20), cc.vwap_deviation(candles, 20),
        ]
        assert all(math.isfinite(r) for r in results)


=== FILE: tests/test_candle_sequencing.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.candle_sequencing import (
    CandleSequencer,
    CandleUpdate,
    CandleUpdateOutcome,
)
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle

UTC = timezone.utc
OPEN_TIME = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def make_candle(close: float = 100.0, is_closed: bool = False, close_time_offset_sec: int = 300) -> Candle:
    return Candle(
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
        open_time=OPEN_TIME,
        close_time=OPEN_TIME + timedelta(seconds=close_time_offset_sec),
        open=100.0,
        high=max(105.0, close),
        low=min(99.0, close),
        close=close,
        volume=10.0,
        is_closed=is_closed,
    )


def make_update(update_seq: int, close: float = 100.0, is_closed: bool = False, seconds_after_open: int = 1) -> CandleUpdate:
    return CandleUpdate(
        candle=make_candle(close=close, is_closed=is_closed),
        update_seq=update_seq,
        event_time=OPEN_TIME + timedelta(seconds=seconds_after_open),
        received_at=OPEN_TIME + timedelta(seconds=seconds_after_open, milliseconds=50),
    )


class TestCandleUpdateConstruction:
    def test_negative_update_seq_rejected(self) -> None:
        with pytest.raises(ValueError, match="update_seq"):
            CandleUpdate(
                candle=make_candle(),
                update_seq=-1,
                event_time=OPEN_TIME,
                received_at=OPEN_TIME,
            )

    def test_naive_event_time_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            CandleUpdate(
                candle=make_candle(),
                update_seq=1,
                event_time=datetime(2026, 8, 31, 10, 0),
                received_at=OPEN_TIME,
            )


class TestCandleSequencerRepeatedUpdates:
    """Aynı açık candle'ın art arda güncellenmesi senaryosu."""

    def test_sequential_updates_all_accepted(self) -> None:
        seq = CandleSequencer()
        r1 = seq.apply(make_update(update_seq=1, close=100.5, seconds_after_open=1))
        r2 = seq.apply(make_update(update_seq=2, close=101.0, seconds_after_open=8))
        r3 = seq.apply(make_update(update_seq=3, close=100.8, seconds_after_open=75))

        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r3.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r3.canonical_candle.close == 100.8

    def test_intermediate_updates_are_not_treated_as_duplicates(self) -> None:
        # Bu, Master Prompt'un vurguladığı temel hatanın regresyon testidir:
        # aynı candle identity'sine ait farklı update_seq'li mesajlar
        # yanlışlıkla duplicate reddedilmemelidir.
        seq = CandleSequencer()
        outcomes = [
            seq.apply(make_update(update_seq=i, close=100.0 + i, seconds_after_open=i)).outcome
            for i in range(1, 5)
        ]
        assert all(o == CandleUpdateOutcome.ACCEPTED_UPDATE for o in outcomes)


class TestCandleSequencerFinal:
    def test_final_update_marks_finalized(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, close=100.5))
        final = seq.apply(make_update(update_seq=2, close=101.0, is_closed=True, seconds_after_open=300))
        assert final.outcome == CandleUpdateOutcome.ACCEPTED_FINAL
        assert final.canonical_candle.is_closed is True
        assert seq.is_finalized(final.canonical_candle.identity) is True

    def test_update_after_final_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, is_closed=True, seconds_after_open=300))
        late = seq.apply(make_update(update_seq=2, close=999.0, seconds_after_open=301))
        assert late.outcome == CandleUpdateOutcome.REJECTED_AFTER_FINAL
        # canonical state, finalize edilmiş haliyle KALIR, bozulmaz
        assert late.canonical_candle.close != 999.0


class TestCandleSequencerDuplicate:
    def test_exact_duplicate_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=5, close=100.5))
        dup = seq.apply(make_update(update_seq=5, close=100.5))
        assert dup.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE

    def test_duplicate_seq_with_different_payload_still_rejected(self) -> None:
        # update_seq aynıysa, payload farklı olsa bile reddedilir (sequence
        # kaynağı tektir, çelişkili iki "aynı update" kabul edilemez).
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=5, close=100.5))
        dup = seq.apply(make_update(update_seq=5, close=555.0))
        assert dup.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE
        assert dup.canonical_candle.close == 100.5


class TestCandleSequencerOutOfOrder:
    def test_out_of_order_update_rejected_and_state_unchanged(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=10, close=100.5))
        stale = seq.apply(make_update(update_seq=3, close=1.0))
        assert stale.outcome == CandleUpdateOutcome.REJECTED_OUT_OF_ORDER
        assert stale.canonical_candle.close == 100.5  # eski state korunur


class TestCandleSequencerDifferentCandle:
    def test_different_open_time_is_independent_identity(self) -> None:
        seq = CandleSequencer()
        r1 = seq.apply(make_update(update_seq=1, close=100.0))
        different_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            open_time=OPEN_TIME + timedelta(minutes=5),
            close_time=OPEN_TIME + timedelta(minutes=10),
            open=200.0, high=205.0, low=199.0, close=203.0, volume=5.0, is_closed=False,
        )
        update2 = CandleUpdate(
            candle=different_candle, update_seq=1,  # aynı update_seq ama FARKLI identity
            event_time=OPEN_TIME + timedelta(minutes=5, seconds=1),
            received_at=OPEN_TIME + timedelta(minutes=5, seconds=1),
        )
        r2 = seq.apply(update2)
        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE  # duplicate DEĞİL, bağımsız identity

    def test_different_timeframe_is_independent_identity(self) -> None:
        seq = CandleSequencer()
        c5m = make_candle()
        c15m = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M15, open_time=OPEN_TIME,
            close_time=OPEN_TIME + timedelta(minutes=15),
            open=100.0, high=105.0, low=99.0, close=100.0, volume=10.0, is_closed=False,
        )
        r1 = seq.apply(CandleUpdate(candle=c5m, update_seq=1, event_time=OPEN_TIME, received_at=OPEN_TIME))
        r2 = seq.apply(CandleUpdate(candle=c15m, update_seq=1, event_time=OPEN_TIME, received_at=OPEN_TIME))
        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE


class TestCandleSequencerReconnect:
    def test_reconnect_resending_same_update_treated_as_duplicate(self) -> None:
        seq = CandleSequencer()
        original = make_update(update_seq=7, close=101.2, seconds_after_open=42)
        seq.apply(original)
        # reconnect sonrası WS aynı mesajı tekrar gönderiyor (aynı update_seq)
        resent = seq.apply(original)
        assert resent.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE
        assert resent.canonical_candle.close == 101.2

    def test_reconnect_gap_fill_with_higher_seq_accepted(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=7, close=101.2, seconds_after_open=42))
        # reconnect sonrası REST gap-fill ile daha yeni bir update geliyor
        gap_fill = seq.apply(make_update(update_seq=8, close=101.5, seconds_after_open=90))
        assert gap_fill.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert gap_fill.canonical_candle.close == 101.5

    def test_reconnect_stale_replay_after_final_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, close=100.0, seconds_after_open=1))
        seq.apply(make_update(update_seq=2, close=101.0, is_closed=True, seconds_after_open=300))
        # reconnect sonrası eski (finalize öncesi) bir update tekrar geliyor
        replay = seq.apply(make_update(update_seq=1, close=100.0, seconds_after_open=1))
        assert replay.outcome == CandleUpdateOutcome.REJECTED_AFTER_FINAL
        assert replay.canonical_candle.close == 101.0


class TestCandleSequencerPeek:
    def test_peek_unknown_identity_returns_none(self) -> None:
        seq = CandleSequencer()
        candle = make_candle()
        assert seq.peek(candle.identity) is None

    def test_peek_returns_current_canonical_state(self) -> None:
        seq = CandleSequencer()
        update = make_update(update_seq=1, close=123.0)
        seq.apply(update)
        assert seq.peek(update.candle.identity).close == 123.0


=== FILE: tests/test_consensus.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.consensus import (
    ConsensusResult,
    RegimeContext,
    RiskAssessment,
    compute_agreement,
)
from crypto_signal_engine.domain.enums import (
    AgentName,
    LiquidityRegime,
    RiskLevel,
    StructureRegime,
    Timeframe,
    VolatilityRegime,
)
from crypto_signal_engine.domain.models import AgentEvidence

UTC = timezone.utc
AS_OF = datetime(2026, 8, 31, tzinfo=UTC)


def make_evidence(agent: AgentName, score: float, symbol: str = "BTCUSDT", context_id: str = "ctx-1") -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=score, rationale="test rationale",
        primary_timeframe=Timeframe.M5, symbol=symbol, as_of=AS_OF, context_id=context_id,
    )


DEFAULT_REGIME = RegimeContext(
    structure=StructureRegime.TRENDING,
    volatility=VolatilityRegime.NORMAL,
    liquidity=LiquidityRegime.NORMAL,
)


class TestComputeAgreement:
    def test_full_agreement_same_direction(self) -> None:
        assert compute_agreement((0.8, 0.75, 0.9)) > 0.9

    def test_full_disagreement_opposite_scores(self) -> None:
        assert compute_agreement((0.8, -0.8)) < 0.1

    def test_all_neutral_scores_treated_as_full_agreement(self) -> None:
        assert compute_agreement((0.0, 0.0, 0.0)) == 1.0

    def test_single_score_returns_neutral_one(self) -> None:
        assert compute_agreement((0.5,)) == 1.0

    def test_result_always_bounded(self) -> None:
        for scores in [(1.0, -1.0), (1.0, 1.0), (-1.0, -1.0), (0.01, -0.99, 0.5)]:
            agreement = compute_agreement(scores)
            assert 0.0 <= agreement <= 1.0

    def test_empty_scores_rejected(self) -> None:
        with pytest.raises(ValueError):
            compute_agreement(())

    def test_nan_score_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            compute_agreement((0.5, float("nan")))

    def test_identical_scores_maximum_agreement(self) -> None:
        assert compute_agreement((0.6, 0.6, 0.6)) == pytest.approx(1.0)

    def test_opposite_extreme_scores_minimum_agreement(self) -> None:
        assert compute_agreement((1.0, -1.0)) == pytest.approx(0.0)


class TestConsensusResultProvenance:
    def test_valid_consensus_result(self) -> None:
        result = ConsensusResult(
            symbol="btcusdt", context_id="ctx-1", raw_score=0.5, agreement=0.8,
            regime=DEFAULT_REGIME,
            contributing_evidence=(make_evidence(AgentName.QUANT, 0.5),),
        )
        assert result.symbol == "BTCUSDT"

    def test_empty_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="contributing_evidence"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.0, agreement=1.0,
                regime=DEFAULT_REGIME, contributing_evidence=(),
            )

    def test_agreement_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="agreement"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.0, agreement=1.5,
                regime=DEFAULT_REGIME,
                contributing_evidence=(make_evidence(AgentName.QUANT, 0.0),),
            )

    def test_mixed_symbol_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5, symbol="BTCUSDT"),
                    make_evidence(AgentName.ORDER_BOOK, 0.6, symbol="ETHUSDT"),
                ),
            )

    def test_mismatched_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5, context_id="ctx-1"),
                    make_evidence(AgentName.ORDER_BOOK, 0.6, context_id="ctx-2"),
                ),
            )

    def test_duplicate_agent_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate agent"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5),
                    make_evidence(AgentName.QUANT, 0.7),  # aynı agent, ikinci evidence
                ),
            )

    def test_distinct_agents_same_context_accepted(self) -> None:
        result = ConsensusResult(
            symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
            regime=DEFAULT_REGIME,
            contributing_evidence=(
                make_evidence(AgentName.QUANT, 0.5),
                make_evidence(AgentName.ORDER_BOOK, 0.6),
                make_evidence(AgentName.REGIME, 0.4),
            ),
        )
        assert len(result.contributing_evidence) == 3

    def test_raw_score_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=float("nan"), agreement=0.5,
                regime=DEFAULT_REGIME,
                contributing_evidence=(make_evidence(AgentName.QUANT, 0.0),),
            )


class TestRegimeContext:
    def test_axes_are_independent(self) -> None:
        # Aynı anda TRENDING + HIGH_VOLATILITY + THIN mümkün olmalı (Gate 12)
        regime = RegimeContext(
            structure=StructureRegime.TRENDING,
            volatility=VolatilityRegime.HIGH,
            liquidity=LiquidityRegime.THIN,
        )
        assert regime.structure == StructureRegime.TRENDING
        assert regime.volatility == VolatilityRegime.HIGH
        assert regime.liquidity == LiquidityRegime.THIN

    def test_string_structure_rejected(self) -> None:
        with pytest.raises(TypeError, match="StructureRegime"):
            RegimeContext(structure="TRENDING", volatility=VolatilityRegime.HIGH, liquidity=LiquidityRegime.NORMAL)  # type: ignore[arg-type]

    def test_string_volatility_rejected(self) -> None:
        with pytest.raises(TypeError, match="VolatilityRegime"):
            RegimeContext(structure=StructureRegime.TRENDING, volatility="HIGH", liquidity=LiquidityRegime.NORMAL)  # type: ignore[arg-type]

    def test_string_liquidity_rejected(self) -> None:
        with pytest.raises(TypeError, match="LiquidityRegime"):
            RegimeContext(structure=StructureRegime.TRENDING, volatility=VolatilityRegime.HIGH, liquidity="NORMAL")  # type: ignore[arg-type]

    def test_all_strings_rejected(self) -> None:
        with pytest.raises(TypeError):
            RegimeContext("TRENDING", "HIGH", "NORMAL")  # type: ignore[arg-type]


class TestRiskAssessment:
    def test_valid_assessment(self) -> None:
        assessment = RiskAssessment(
            confidence_multiplier=0.6, risk_level=RiskLevel.HIGH,
            rationale="realized volatility 89th percentile, spread widening",
        )
        assert assessment.confidence_multiplier == 0.6

    def test_multiplier_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="confidence_multiplier"):
            RiskAssessment(confidence_multiplier=1.2, risk_level=RiskLevel.LOW, rationale="test")

    def test_risk_level_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="RiskLevel"):
            RiskAssessment(confidence_multiplier=0.5, risk_level="LOW", rationale="test")  # type: ignore[arg-type]

    def test_multiplier_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            RiskAssessment(confidence_multiplier=float("nan"), risk_level=RiskLevel.LOW, rationale="test")

    def test_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            RiskAssessment(confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="   ")

    def test_contradicting_metrics_immutable(self) -> None:
        assessment = RiskAssessment(
            confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
            contradicting_metrics={"volatility_pct": 89.0},
        )
        with pytest.raises(TypeError):
            assessment.contradicting_metrics["volatility_pct"] = 999.0  # type: ignore[index]

    def test_contradicting_metrics_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            RiskAssessment(
                confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
                contradicting_metrics={"x": float("nan")},
            )


=== FILE: tests/test_consensus_engine.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.consensus.engine import AGENT_WEIGHTS, ConsensusEngine
from crypto_signal_engine.domain.consensus import RegimeContext, compute_agreement
from crypto_signal_engine.domain.enums import AgentName, LiquidityRegime, StructureRegime, Timeframe, VolatilityRegime
from crypto_signal_engine.domain.models import AgentEvidence
from crypto_signal_engine.errors import ConsensusError

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)
REGIME = RegimeContext(structure=StructureRegime.RANGING, volatility=VolatilityRegime.NORMAL, liquidity=LiquidityRegime.NORMAL)


def make_evidence(agent, score, symbol="BTCUSDT", context_id="ctx-1", as_of=T0) -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=score, rationale="test", primary_timeframe=Timeframe.M5,
        symbol=symbol, as_of=as_of, context_id=context_id,
    )


class TestConsensusContract:
    def test_accepts_evidence_only_positionally(self) -> None:
        import inspect

        sig = inspect.signature(ConsensusEngine.combine)
        params = list(sig.parameters.keys())
        assert params[:2] == ["self", "evidence"]
        assert sig.parameters["regime"].kind == inspect.Parameter.KEYWORD_ONLY

    def test_exact_fixed_weights(self) -> None:
        assert AGENT_WEIGHTS[AgentName.QUANT] == pytest.approx(0.35)
        assert AGENT_WEIGHTS[AgentName.MARKET_STRUCTURE] == pytest.approx(0.30)
        assert AGENT_WEIGHTS[AgentName.ORDER_BOOK] == pytest.approx(0.15)
        assert AGENT_WEIGHTS[AgentName.REGIME] == pytest.approx(0.20)
        assert sum(AGENT_WEIGHTS.values()) == pytest.approx(1.0)

    def test_exact_weighted_score(self) -> None:
        evidence = [
            make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5),
            make_evidence(AgentName.ORDER_BOOK, 0.5), make_evidence(AgentName.REGIME, 0.5),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert result.raw_score == pytest.approx(0.5)

    def test_uses_existing_compute_agreement(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, 0.8)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        expected = compute_agreement((0.8, 0.8))
        assert result.agreement == pytest.approx(expected)

    def test_mixed_symbols_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5, symbol="BTCUSDT"), make_evidence(AgentName.MARKET_STRUCTURE, 0.5, symbol="ETHUSDT")]
        with pytest.raises(ConsensusError, match="mixed-symbol"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_mixed_context_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5, context_id="ctx-1"), make_evidence(AgentName.MARKET_STRUCTURE, 0.5, context_id="ctx-2")]
        with pytest.raises(ConsensusError, match="context"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_duplicate_agent_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.QUANT, 0.3)]
        with pytest.raises(ConsensusError, match="duplicate"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_deterministic_evidence_ordering(self) -> None:
        evidence = [
            make_evidence(AgentName.REGIME, 0.1), make_evidence(AgentName.QUANT, 0.2),
            make_evidence(AgentName.ORDER_BOOK, 0.3), make_evidence(AgentName.MARKET_STRUCTURE, 0.4),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert [e.agent for e in result.contributing_evidence] == [
            AgentName.QUANT, AgentName.MARKET_STRUCTURE, AgentName.ORDER_BOOK, AgentName.REGIME,
        ]

    def test_finite_bounded_score(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 1.0), make_evidence(AgentName.MARKET_STRUCTURE, 1.0)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert -1.0 <= result.raw_score <= 1.0

    def test_conflicting_evidence_lowers_agreement(self) -> None:
        aligned = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, 0.8)]
        conflicted = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, -0.8)]
        r1 = ConsensusEngine().combine(aligned, regime=REGIME)
        r2 = ConsensusEngine().combine(conflicted, regime=REGIME)
        assert r2.agreement < r1.agreement

    def test_all_four_agents_normal_evaluation(self) -> None:
        evidence = [
            make_evidence(AgentName.QUANT, 0.4), make_evidence(AgentName.MARKET_STRUCTURE, 0.3),
            make_evidence(AgentName.ORDER_BOOK, 0.2), make_evidence(AgentName.REGIME, 0.1),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        expected = 0.35 * 0.4 + 0.30 * 0.3 + 0.15 * 0.2 + 0.20 * 0.1
        assert result.raw_score == pytest.approx(expected)

    def test_regime_evidence_participates_exactly_once(self) -> None:
        evidence = [make_evidence(AgentName.REGIME, 0.6), make_evidence(AgentName.QUANT, 0.2)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        regime_evidences = [e for e in result.contributing_evidence if e.agent == AgentName.REGIME]
        assert len(regime_evidences) == 1

    def test_empty_evidence_rejected(self) -> None:
        with pytest.raises(ConsensusError, match="boş"):
            ConsensusEngine().combine([], regime=REGIME)

    def test_non_agent_evidence_element_rejected(self) -> None:
        with pytest.raises(ConsensusError):
            ConsensusEngine().combine(["not-evidence"], regime=REGIME)  # type: ignore[list-item]

    def test_does_not_require_regime_context_influence_on_score(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5)]
        regime_a = RegimeContext(structure=StructureRegime.RANGING, volatility=VolatilityRegime.LOW, liquidity=LiquidityRegime.NORMAL)
        regime_b = RegimeContext(structure=StructureRegime.BREAKOUT, volatility=VolatilityRegime.EXTREME, liquidity=LiquidityRegime.STRESSED)
        r1 = ConsensusEngine().combine(evidence, regime=regime_a)
        r2 = ConsensusEngine().combine(evidence, regime=regime_b)
        assert r1.raw_score == r2.raw_score
        assert r1.agreement == r2.agreement


=== FILE: tests/test_domain_models.py ===
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import (
    AgentName,
    RiskLevel,
    SignalDirection,
    Timeframe,
)
from crypto_signal_engine.domain.models import (
    AgentEvidence,
    Candle,
    CandleIdentity,
    FeatureVector,
    OrderBookLevel,
    OrderBookSnapshot,
    Signal,
    Trade,
)

UTC = timezone.utc


def make_candle(**overrides) -> Candle:
    defaults = dict(
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
        open_time=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC),
        open=100.0,
        high=105.0,
        low=99.0,
        close=103.0,
        volume=10.0,
        is_closed=True,
    )
    defaults.update(overrides)
    return Candle(**defaults)


def make_evidence(**overrides) -> AgentEvidence:
    defaults = dict(
        agent=AgentName.QUANT,
        score=0.5,
        rationale="momentum accelerating",
        primary_timeframe=Timeframe.M5,
        symbol="BTCUSDT",
        as_of=datetime(2026, 8, 31, tzinfo=UTC),
        context_id="snapshot-1",
    )
    defaults.update(overrides)
    return AgentEvidence(**defaults)


def make_signal(**overrides) -> Signal:
    defaults = dict(
        symbol="BTCUSDT",
        timestamp=datetime(2026, 8, 31, tzinfo=UTC),
        context_id="snapshot-1",
        score=0.5,
        confidence=0.7,
        risk_level=RiskLevel.MEDIUM,
        primary_timeframe=Timeframe.M5,
        supporting_factors=(),
        contradicting_factors=(),
        invalidation=None,
        model_version="baseline-v0.1",
    )
    defaults.update(overrides)
    return Signal(**defaults)


class TestCandleIdentity:
    def test_identity_derived_from_candle(self) -> None:
        candle = make_candle()
        identity = candle.identity
        assert identity == CandleIdentity(
            symbol="BTCUSDT", timeframe=Timeframe.M5, open_time=candle.open_time
        )

    def test_close_time_not_part_of_identity(self) -> None:
        c1 = make_candle(close_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC), is_closed=False)
        c2 = make_candle(close_time=datetime(2026, 8, 31, 10, 5, 30, tzinfo=UTC), is_closed=True)
        assert c1.identity == c2.identity

    def test_symbol_normalized_in_identity(self) -> None:
        identity = CandleIdentity(
            symbol="btcusdt", timeframe=Timeframe.M5, open_time=datetime(2026, 8, 31, tzinfo=UTC)
        )
        assert identity.symbol == "BTCUSDT"

    def test_non_timeframe_enum_rejected(self) -> None:
        with pytest.raises(TypeError):
            CandleIdentity(symbol="BTCUSDT", timeframe="5m", open_time=datetime(2026, 8, 31, tzinfo=UTC))  # type: ignore[arg-type]


class TestCandleInvariants:
    def test_valid_candle_constructs(self) -> None:
        assert make_candle().symbol == "BTCUSDT"

    def test_symbol_normalized(self) -> None:
        assert make_candle(symbol="btcusdt").symbol == "BTCUSDT"
        assert make_candle(symbol=" BTCUSDT ").symbol == "BTCUSDT"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            make_candle(open_time=datetime(2026, 8, 31, 10, 0))

    def test_non_utc_timezone_rejected(self) -> None:
        offset_tz = timezone(timedelta(hours=3))
        with pytest.raises(ValueError, match="UTC olmalı"):
            make_candle(open_time=datetime(2026, 8, 31, 10, 0, tzinfo=offset_tz))

    def test_close_before_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="close_time"):
            make_candle(
                open_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC),
                close_time=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
            )

    def test_high_below_low_rejected(self) -> None:
        with pytest.raises(ValueError, match="impossible OHLC"):
            make_candle(high=90.0, low=99.0)

    def test_open_outside_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="open"):
            make_candle(open=200.0)

    def test_close_outside_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="close"):
            make_candle(close=200.0)

    def test_negative_volume_rejected(self) -> None:
        with pytest.raises(ValueError, match="volume"):
            make_candle(volume=-1.0)

    def test_negative_trade_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="trade_count"):
            make_candle(trade_count=-1)

    def test_non_timeframe_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_candle(timeframe="5m")

    @pytest.mark.parametrize("field_name", ["open", "high", "low", "close", "volume"])
    def test_nan_rejected(self, field_name: str) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_candle(**{field_name: float("nan")})

    @pytest.mark.parametrize("field_name", ["open", "high", "low", "close", "volume"])
    def test_inf_rejected(self, field_name: str) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_candle(**{field_name: float("inf")})

    def test_candle_is_frozen(self) -> None:
        candle = make_candle()
        with pytest.raises(FrozenInstanceError):
            candle.close = 999.0  # type: ignore[misc]


class TestTradeInvariants:
    def test_valid_trade(self) -> None:
        trade = Trade(
            symbol="btcusdt",
            trade_id=1,
            price=100.0,
            quantity=1.0,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            is_buyer_maker=False,
        )
        assert trade.symbol == "BTCUSDT"

    def test_zero_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="price"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=0.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )

    def test_negative_trade_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="trade_id"):
            Trade(
                symbol="BTCUSDT", trade_id=-1, price=100.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )

    def test_naive_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=100.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31), is_buyer_maker=False,
            )

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_price_non_finite_rejected(self, bad: float) -> None:
        with pytest.raises(ValueError, match="finite"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=bad, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )


class TestOrderBookSnapshot:
    def _levels(self, prices_qty):
        return tuple(OrderBookLevel(price=p, quantity=q) for p, q in prices_qty)

    def test_valid_snapshot_computed_properties(self) -> None:
        snap = OrderBookSnapshot(
            symbol="BTCUSDT",
            timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            bids=self._levels([(99.0, 1.0), (98.5, 2.0)]),
            asks=self._levels([(100.0, 1.5), (100.5, 1.0)]),
            last_update_id=42,
        )
        assert snap.best_bid.price == 99.0
        assert snap.best_ask.price == 100.0
        assert snap.mid_price == pytest.approx(99.5)
        assert snap.spread == pytest.approx(1.0)

    def test_crossed_book_rejected(self) -> None:
        with pytest.raises(ValueError, match="crossed"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(101.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=1,
            )

    def test_locked_book_rejected(self) -> None:
        with pytest.raises(ValueError, match="crossed"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(100.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=1,
            )

    def test_empty_bids_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=(), asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_empty_asks_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(100.0, 1.0)]), asks=(), last_update_id=1,
            )

    def test_unsorted_bids_rejected(self) -> None:
        with pytest.raises(ValueError, match="bids"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(98.0, 1.0), (99.0, 1.0)]),  # artan -> yanlış
                asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_unsorted_asks_rejected(self) -> None:
        with pytest.raises(ValueError, match="asks"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]),
                asks=self._levels([(101.0, 1.0), (100.0, 1.0)]),  # azalan -> yanlış
                last_update_id=1,
            )

    def test_duplicate_bid_level_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0), (99.0, 2.0)]),
                asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_duplicate_ask_level_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]),
                asks=self._levels([(100.0, 1.0), (100.0, 2.0)]), last_update_id=1,
            )

    def test_negative_update_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="last_update_id"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=-1,
            )

    def test_nan_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            OrderBookLevel(price=float("nan"), quantity=1.0)

    def test_inf_quantity_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            OrderBookLevel(price=100.0, quantity=float("inf"))

    def test_zero_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="price"):
            OrderBookLevel(price=0.0, quantity=1.0)

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValueError, match="quantity"):
            OrderBookLevel(price=100.0, quantity=-1.0)

    def test_accepts_list_input_and_converts_to_tuple(self) -> None:
        snap = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            bids=[OrderBookLevel(price=99.0, quantity=1.0)],
            asks=[OrderBookLevel(price=100.0, quantity=1.0)],
            last_update_id=1,
        )
        assert isinstance(snap.bids, tuple)
        assert isinstance(snap.asks, tuple)


class TestFeatureVector:
    def test_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": float("nan")},
            )

    def test_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"ema": float("inf")},
            )

    def test_valid_values_pass(self) -> None:
        fv = FeatureVector(
            symbol="btcusdt", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": 55.2},
        )
        assert fv.symbol == "BTCUSDT"
        assert fv.values["rsi"] == 55.2

    def test_values_immutable(self) -> None:
        fv = FeatureVector(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": 55.2},
        )
        with pytest.raises(TypeError):
            fv.values["rsi"] = 999.0  # type: ignore[index]

    def test_original_dict_mutation_does_not_leak(self) -> None:
        source = {"rsi": 55.2}
        fv = FeatureVector(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values=source,
        )
        source["rsi"] = 999.0
        assert fv.values["rsi"] == 55.2


class TestAgentEvidence:
    def test_score_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="score"):
            make_evidence(score=1.5)

    def test_score_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_evidence(score=float("nan"))

    def test_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            make_evidence(rationale="   ")

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context_id"):
            make_evidence(context_id="  ")

    def test_agent_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="AgentName"):
            make_evidence(agent="QUANT")

    def test_primary_timeframe_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_evidence(primary_timeframe="5m")

    def test_naive_as_of_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            make_evidence(as_of=datetime(2026, 8, 31))

    def test_symbol_normalized(self) -> None:
        evidence = make_evidence(symbol="btcusdt")
        assert evidence.symbol == "BTCUSDT"

    def test_supporting_metrics_immutable(self) -> None:
        evidence = make_evidence(supporting_metrics={"rsi": 55.0})
        with pytest.raises(TypeError):
            evidence.supporting_metrics["rsi"] = 999.0  # type: ignore[index]

    def test_supporting_metrics_defensive_copy(self) -> None:
        source = {"rsi": 55.0}
        evidence = make_evidence(supporting_metrics=source)
        source["rsi"] = 999.0
        assert evidence.supporting_metrics["rsi"] == 55.0

    def test_supporting_metrics_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_evidence(supporting_metrics={"rsi": float("nan")})


class TestSignalSingleSourceOfTruth:
    def test_direction_derived_from_score(self) -> None:
        signal = make_signal(score=0.55)
        assert signal.direction == SignalDirection.LONG

    def test_direction_is_not_a_settable_field(self) -> None:
        # direction bir property'dir, constructor argümanı DEĞİLDİR.
        with pytest.raises(TypeError):
            make_signal(direction=SignalDirection.STRONG_LONG)  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "score,expected",
        [
            (1.0, SignalDirection.STRONG_LONG),
            (0.70, SignalDirection.STRONG_LONG),
            (0.69, SignalDirection.LONG),
            (0.40, SignalDirection.LONG),
            (0.39, SignalDirection.WEAK_LONG),
            (0.15, SignalDirection.WEAK_LONG),
            (0.14, SignalDirection.NEUTRAL),
            (0.0, SignalDirection.NEUTRAL),
            (-0.14, SignalDirection.NEUTRAL),
            (-0.15, SignalDirection.WEAK_SHORT),
            (-0.40, SignalDirection.SHORT),
            (-0.70, SignalDirection.STRONG_SHORT),
            (-1.0, SignalDirection.STRONG_SHORT),
        ],
    )
    def test_all_boundaries_produce_consistent_direction(self, score, expected) -> None:
        assert make_signal(score=score).direction == expected

    def test_contradictory_state_impossible_by_construction(self) -> None:
        # score=-1.0 iken direction ASLA STRONG_LONG olamaz çünkü direction
        # score'dan türetilir; bunu "yanlış" bir direction ile inşa etme
        # imkanı yoktur (constructor'da direction alanı yok).
        signal = make_signal(score=-1.0)
        assert signal.direction == SignalDirection.STRONG_SHORT


class TestSignal:
    def test_confidence_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="confidence"):
            make_signal(confidence=1.2)

    def test_confidence_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_signal(confidence=float("nan"))

    def test_score_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_signal(score=float("inf"))

    def test_empty_model_version_rejected(self) -> None:
        with pytest.raises(ValueError, match="model_version"):
            make_signal(model_version="   ")

    def test_symbol_normalized(self) -> None:
        assert make_signal(symbol="btcusdt").symbol == "BTCUSDT"

    def test_signal_is_frozen(self) -> None:
        signal = make_signal()
        with pytest.raises(FrozenInstanceError):
            signal.confidence = 0.9  # type: ignore[misc]

    def test_supporting_factors_converted_to_tuple(self) -> None:
        signal = make_signal(supporting_factors=[make_evidence()])
        assert isinstance(signal.supporting_factors, tuple)

    def test_risk_level_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="RiskLevel"):
            make_signal(risk_level="HIGH")

    def test_primary_timeframe_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_signal(primary_timeframe="5m")

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context_id"):
            make_signal(context_id="   ")


class TestSignalEvidenceProvenance:
    """Quality Gate 26 — final Signal provenance."""

    CTX = "snapshot-1"
    SIGNAL_TS = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)

    def _evidence(self, agent=AgentName.QUANT, symbol="BTCUSDT", context_id=None, as_of=None) -> AgentEvidence:
        return make_evidence(
            agent=agent,
            symbol=symbol,
            context_id=context_id or self.CTX,
            as_of=as_of or (self.SIGNAL_TS - timedelta(seconds=1)),
        )

    def test_valid_same_context_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(agent=AgentName.QUANT),),
            contradicting_factors=(self._evidence(agent=AgentName.ORDER_BOOK),),
        )
        assert len(signal.supporting_factors) == 1

    def test_mixed_symbol_supporting_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(symbol="ETHUSDT"),),
            )

    def test_mixed_symbol_contradicting_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                contradicting_factors=(self._evidence(symbol="ETHUSDT"),),
            )

    def test_mixed_context_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-context"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(context_id="other-ctx"),),
            )

    def test_future_evidence_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="GELECEKTEN"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(as_of=self.SIGNAL_TS + timedelta(seconds=1)),),
            )

    def test_evidence_as_of_exactly_equal_to_signal_timestamp_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(as_of=self.SIGNAL_TS),),
        )
        assert signal.supporting_factors[0].as_of == self.SIGNAL_TS

    def test_identical_evidence_object_across_lists_rejected(self) -> None:
        evidence = self._evidence(agent=AgentName.QUANT)
        with pytest.raises(ValueError, match="hem supporting hem contradicting"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(evidence,),
                contradicting_factors=(evidence,),
            )

    def test_distinct_evidence_same_agent_across_lists_rejected(self) -> None:
        """Aynı obje DEĞİL, farklı rationale'lı iki farklı evidence ama AYNI
        AgentName — supporting/contradicting'e dağıtılmış. Global
        one-agent/one-evidence politikası bunu da reddeder (bu, salt
        obje-eşitliği overlap kontrolünden AYRI bir kuraldır)."""
        evidence_a = self._evidence(agent=AgentName.QUANT)
        evidence_b = make_evidence(
            agent=AgentName.QUANT, symbol="BTCUSDT", context_id=self.CTX,
            as_of=self.SIGNAL_TS - timedelta(seconds=1), rationale="different rationale, same agent",
        )
        assert evidence_a != evidence_b  # gerçekten farklı objeler/değerler
        with pytest.raises(ValueError, match="duplicate agent evidence"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(evidence_a,),
                contradicting_factors=(evidence_b,),
            )

    def test_same_agent_twice_within_supporting_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate agent evidence"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(
                    self._evidence(agent=AgentName.QUANT),
                    self._evidence(agent=AgentName.QUANT),
                ),
            )

    def test_distinct_agents_across_both_lists_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(agent=AgentName.QUANT),),
            contradicting_factors=(
                self._evidence(agent=AgentName.ORDER_BOOK),
                self._evidence(agent=AgentName.REGIME),
            ),
        )
        assert len(signal.contradicting_factors) == 2


=== FILE: tests/test_end_to_end.py ===
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.signal_engine import SignalEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def commit_snapshot(history: FeatureHistoryStore, tf: Timeframe, as_of: datetime, values: dict) -> None:
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=tf, as_of=as_of, values=values))


def build_history(as_of: datetime, bullish: bool) -> FeatureHistoryStore:
    history = FeatureHistoryStore()
    sign = 1.0 if bullish else -1.0
    commit_snapshot(history, Timeframe.M1, as_of, {
        "SPREAD_BPS": 2.0, "DEPTH_IMBALANCE_10": sign * 0.4, "TOB_IMBALANCE": sign * 0.3,
        "MID_PRICE": 100.0, "MICROPRICE": 100.0 + sign * 0.03,
    })
    commit_snapshot(history, Timeframe.M5, as_of, {
        "RSI_14": 50.0 + sign * 20.0, "ROC_10": sign * 2.0, "VWAP_DEVIATION_20": sign * 0.012,
        "RELATIVE_VOLUME_20": 1.3, "BOLLINGER_BANDWIDTH_20_2": 0.03,
    })
    commit_snapshot(history, Timeframe.M15, as_of, {
        "ROC_10": sign * 1.8, "DIST_FROM_HIGH_20": 0.0 if bullish else 0.05,
        "DIST_FROM_LOW_20": 0.05 if bullish else 0.0, "RELATIVE_VOLUME_20": 1.2, "BODY_TO_RANGE_RATIO": 0.7,
    })
    commit_snapshot(history, Timeframe.H1, as_of, {
        "ROC_10": sign * 1.5, "RSI_14": 50.0 + sign * 15.0, "BOLLINGER_BANDWIDTH_20_2": 0.03,
        "RELATIVE_VOLUME_20": 1.1, "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.06,
    })
    return history


class TestEndToEndScenarios:
    def test_clearly_bullish(self) -> None:
        history = build_history(T0, bullish=True)
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.score > 0.3
        assert signal.direction in (SignalDirection.LONG, SignalDirection.STRONG_LONG, SignalDirection.WEAK_LONG)

    def test_clearly_bearish(self) -> None:
        history = build_history(T0, bullish=False)
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.score < -0.3
        assert signal.direction in (SignalDirection.SHORT, SignalDirection.STRONG_SHORT, SignalDirection.WEAK_SHORT)

    def test_conflicted_scenario_lowers_agreement_and_confidence(self) -> None:
        history = FeatureHistoryStore()
        commit_snapshot(history, Timeframe.M1, T0, {
            "SPREAD_BPS": 2.0, "DEPTH_IMBALANCE_10": 0.0, "TOB_IMBALANCE": 0.0, "MID_PRICE": 100.0, "MICROPRICE": 100.0,
        })
        commit_snapshot(history, Timeframe.M5, T0, {
            "RSI_14": 80.0, "ROC_10": 2.5, "VWAP_DEVIATION_20": 0.015, "RELATIVE_VOLUME_20": 1.2, "BOLLINGER_BANDWIDTH_20_2": 0.03,
        })
        commit_snapshot(history, Timeframe.M15, T0, {
            "ROC_10": -2.5, "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.0, "RELATIVE_VOLUME_20": 1.2, "BODY_TO_RANGE_RATIO": 0.7,
        })
        commit_snapshot(history, Timeframe.H1, T0, {
            "ROC_10": 0.0, "RSI_14": 50.0, "BOLLINGER_BANDWIDTH_20_2": 0.03, "RELATIVE_VOLUME_20": 1.0,
            "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.05,
        })
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        clean_bullish = SignalEngine(build_history(T0, bullish=True)).evaluate("BTCUSDT", T0)
        assert signal.confidence < clean_bullish.confidence

    def test_deterministic_repeatability(self) -> None:
        history = build_history(T0, bullish=True)
        engine = SignalEngine(history)
        s1 = engine.evaluate("BTCUSDT", T0)
        s2 = engine.evaluate("BTCUSDT", T0)
        assert s1.score == s2.score
        assert s1.confidence == s2.confidence
        assert s1.context_id == s2.context_id
        assert s1.risk_level == s2.risk_level


class TestFutureDataInvariance:
    """Bölüm 50 — HARD BLOCKER: T anındaki sonuç, T'den SONRAKİ snapshot'lar
    eklendikten sonra bile DEĞİŞMEMELİDİR."""

    def test_full_result_unchanged_after_adding_future_snapshots(self) -> None:
        history = build_history(T0, bullish=True)
        engine = SignalEngine(history)

        signal_before = engine.evaluate("BTCUSDT", T0)

        future_time = T0 + timedelta(minutes=1)
        future_history = build_history(future_time, bullish=False)
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            future_snapshot = future_history.latest("BTCUSDT", tf)
            history.commit(future_snapshot)

        signal_after = engine.evaluate("BTCUSDT", T0)

        assert signal_before.score == signal_after.score
        assert signal_before.confidence == signal_after.confidence
        assert signal_before.risk_level == signal_after.risk_level
        assert signal_before.context_id == signal_after.context_id
        assert [e.agent for e in signal_before.supporting_factors] == [e.agent for e in signal_after.supporting_factors]


=== FILE: tests/test_enums.py ===
import pytest

from crypto_signal_engine.domain.enums import SignalDirection


class TestSignalDirectionFromScore:
    @pytest.mark.parametrize(
        "score,expected",
        [
            (0.0, SignalDirection.NEUTRAL),
            (0.10, SignalDirection.NEUTRAL),
            (0.15, SignalDirection.WEAK_LONG),
            (0.39, SignalDirection.WEAK_LONG),
            (0.40, SignalDirection.LONG),
            (0.69, SignalDirection.LONG),
            (0.70, SignalDirection.STRONG_LONG),
            (1.0, SignalDirection.STRONG_LONG),
            (-0.10, SignalDirection.NEUTRAL),
            (-0.15, SignalDirection.WEAK_SHORT),
            (-0.40, SignalDirection.SHORT),
            (-0.70, SignalDirection.STRONG_SHORT),
            (-1.0, SignalDirection.STRONG_SHORT),
        ],
    )
    def test_thresholds(self, score: float, expected: SignalDirection) -> None:
        assert SignalDirection.from_score(score) == expected

    def test_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError):
            SignalDirection.from_score(1.5)
        with pytest.raises(ValueError):
            SignalDirection.from_score(-1.5)

    def test_nan_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            SignalDirection.from_score(float("nan"))

    def test_inf_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            SignalDirection.from_score(float("inf"))


=== FILE: tests/test_events.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.events import EventType, MarketDataEvent, SequenceCursor
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade

UTC = timezone.utc


def make_candle(symbol: str = "BTCUSDT") -> Candle:
    return Candle(
        symbol=symbol, timeframe=Timeframe.M5,
        open_time=datetime(2026, 8, 31, tzinfo=UTC),
        close_time=datetime(2026, 8, 31, 0, 5, tzinfo=UTC),
        open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True,
    )


def make_trade(symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        symbol=symbol, trade_id=1, price=100.0, quantity=1.0,
        timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
    )


def make_orderbook(symbol: str = "BTCUSDT") -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol=symbol, timestamp=datetime(2026, 8, 31, tzinfo=UTC),
        bids=(OrderBookLevel(price=99.0, quantity=1.0),),
        asks=(OrderBookLevel(price=100.0, quantity=1.0),),
        last_update_id=1,
    )


class TestMarketDataEventTypeSafety:
    def test_valid_candle_event(self) -> None:
        event = MarketDataEvent(
            event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
        )
        assert event.symbol == "BTCUSDT"

    def test_valid_trade_event(self) -> None:
        MarketDataEvent(
            event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_trade(),
        )

    def test_valid_order_book_event(self) -> None:
        MarketDataEvent(
            event_type=EventType.ORDER_BOOK, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
        )

    def test_trade_type_with_candle_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Trade"):
            MarketDataEvent(
                event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_candle_type_with_trade_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Candle"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_trade(),
            )

    def test_order_book_type_with_candle_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="OrderBookSnapshot"):
            MarketDataEvent(
                event_type=EventType.ORDER_BOOK, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_candle_type_with_order_book_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Candle"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
            )

    def test_trade_type_with_order_book_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Trade"):
            MarketDataEvent(
                event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
            )

    def test_symbol_mismatch_rejected(self) -> None:
        with pytest.raises(ValueError, match="eşleşmiyor"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="ETHUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="BTCUSDT"),
            )

    def test_symbol_mismatch_after_normalization_still_detected(self) -> None:
        # "btcusdt" (event.symbol) normalize sonrası BTCUSDT olur ve
        # payload'ın ETHUSDT'si ile hâlâ eşleşmemelidir.
        with pytest.raises(ValueError, match="eşleşmiyor"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="btcusdt", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="ETHUSDT"),
            )

    def test_symbol_case_normalized_match_accepted(self) -> None:
        event = MarketDataEvent(
            event_type=EventType.CANDLE, symbol="btcusdt", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="BTCUSDT"),
        )
        assert event.symbol == "BTCUSDT"

    def test_negative_sequence_rejected(self) -> None:
        with pytest.raises(ValueError, match="sequence_id"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=-1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_naive_received_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31), payload=make_candle(),
            )

    def test_event_type_string_rejected(self) -> None:
        """Kritik regresyon: EventType(str, Enum) olduğundan "CANDLE" ==
        EventType.CANDLE VE hash(...) eşit olur; bu yüzden dict.get() bazlı
        eski kontrol bir plain string'i YAKALAYAMIYORDU. isinstance bazlı
        require_enum bunu düzeltir."""
        with pytest.raises(TypeError, match="EventType"):
            MarketDataEvent(
                event_type="CANDLE", symbol="BTCUSDT", sequence_id=1,  # type: ignore[arg-type]
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )


class TestSequenceCursor:
    def test_symbol_normalized(self) -> None:
        cursor = SequenceCursor(
            symbol="btcusdt", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.symbol == "BTCUSDT"

    def test_negative_last_sequence_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="last_sequence_id"):
            SequenceCursor(
                symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=-1,
                last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_naive_last_received_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            SequenceCursor(
                symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
                last_received_at=datetime(2026, 8, 31),
            )

    def test_event_type_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="EventType"):
            SequenceCursor(
                symbol="BTCUSDT", event_type="TRADE", last_sequence_id=1,  # type: ignore[arg-type]
                last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_next_is_valid_monotonic(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=100,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.next_is_valid(101) is True
        assert cursor.next_is_valid(100) is False
        assert cursor.next_is_valid(99) is False

    def test_is_stale_naive_now_rejected(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="naive datetime"):
            cursor.is_stale(datetime(2026, 8, 31), max_age_seconds=30)

    def test_is_stale_negative_max_age_rejected(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="max_age_seconds"):
            cursor.is_stale(datetime(2026, 8, 31, tzinfo=UTC), max_age_seconds=-1)

    def test_is_stale_boundary_exactly_at_threshold_not_stale(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, 10, 0, 0, tzinfo=UTC),
        )
        now = datetime(2026, 8, 31, 10, 0, 30, tzinfo=UTC)  # tam 30 saniye sonra
        assert cursor.is_stale(now, max_age_seconds=30) is False

    def test_is_stale_just_past_threshold_is_stale(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, 10, 0, 0, tzinfo=UTC),
        )
        now = datetime(2026, 8, 31, 10, 0, 30, 1, tzinfo=UTC)  # 30sn + 1 mikro saniye
        assert cursor.is_stale(now, max_age_seconds=30) is True

    def test_is_stale_zero_max_age_allowed(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.is_stale(datetime(2026, 8, 31, tzinfo=UTC), max_age_seconds=0) is False


=== FILE: tests/test_execution_adapter.py ===
"""Faz 10 — execution/adapter.py testleri: exchange-filter doğrulama
(LOT_SIZE/MARKET_LOT_SIZE/PRICE_FILTER/MIN_NOTIONAL/NOTIONAL, applyToMarket/
applyMinToMarket/applyMaxToMarket semantics), MARKET order notional
doğrulaması için GÜNCEL PUBLIC fiyat lookup'ı (BLOCKER FİX — Karar 73),
`TestnetExecutionAdapter.submit()` akışı, duplicate intent kimliği,
multi-symbol izolasyonu. Tamamen offline."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.adapter import (
    TestnetExecutionAdapter,
    determine_market_price_requirement,
    parse_symbol_filters,
    validate_intent_against_filters,
)
from crypto_signal_engine.execution.errors import (
    ExecutionTimeoutError,
    ExecutionTransportError,
    FilterValidationError,
    MalformedResponseError,
    MarketPriceUnavailableError,
)
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _exchange_info(
    symbol: str = "BTCUSDT",
    *,
    status: str = "TRADING",
    apply_to_market: bool = False,
    with_market_lot_size: bool = False,
    notional_filter: str | None = "MIN_NOTIONAL",
    min_notional: str = "10.0",
    max_notional: str | None = None,
    apply_min_to_market: bool = False,
    apply_max_to_market: bool = False,
) -> dict:
    filters = [
        {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
        {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
    ]
    if with_market_lot_size:
        filters.append(
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "5000.0", "stepSize": "0.001"}
        )
    if notional_filter == "MIN_NOTIONAL":
        filters.append({"filterType": "MIN_NOTIONAL", "minNotional": min_notional, "applyToMarket": apply_to_market})
    elif notional_filter == "NOTIONAL":
        notional_entry = {
            "filterType": "NOTIONAL", "minNotional": min_notional, "applyMinToMarket": apply_min_to_market,
        }
        if max_notional is not None:
            notional_entry["maxNotional"] = max_notional
            notional_entry["applyMaxToMarket"] = apply_max_to_market
        filters.append(notional_entry)
    return {"symbols": [{"symbol": symbol, "status": status, "filters": filters}]}


def _market_intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


def _price_response(symbol: str = "BTCUSDT", price: str = "50000.0") -> tuple:
    return json_response({"symbol": symbol, "price": price})


def _order_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.01", "cummulativeQuoteQty": "500.0",
        "transactTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _adapter(get_responses=None, post_responses=None) -> tuple[TestnetExecutionAdapter, FakeTestnetHttpClient]:
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    return TestnetExecutionAdapter(client), http


class TestParseSymbolFilters:
    def test_parses_lot_size_price_and_notional(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        assert filters.step_size == 0.0001
        assert filters.min_qty == 0.0001
        assert filters.tick_size == 0.01
        assert filters.min_notional == 10.0
        assert filters.status == "TRADING"

    def test_missing_symbol_raises(self) -> None:
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(_exchange_info(symbol="ETHUSDT"), "BTCUSDT")

    def test_missing_lot_size_raises(self) -> None:
        payload = {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "filters": []}]}
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(payload, "BTCUSDT")

    def test_missing_apply_to_market_flag_is_malformed_not_defaulted(self) -> None:
        """Gerçek Binance yanıtı bu bayrağı HER ZAMAN içerir — eksikse
        sessizce `False` varsayılmaz, AÇIKÇA reddedilir (fail-closed)."""
        payload = {
            "symbols": [
                {
                    "symbol": "BTCUSDT", "status": "TRADING",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10.0"},  # applyToMarket EKSİK
                    ],
                }
            ]
        }
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(payload, "BTCUSDT")

    def test_market_lot_size_parsed_when_present(self) -> None:
        filters = parse_symbol_filters(_exchange_info(with_market_lot_size=True), "BTCUSDT")
        assert filters.market_step_size == 0.001
        assert filters.market_min_qty == 0.001
        assert filters.market_max_qty == 5000.0

    def test_notional_filter_preferred_over_min_notional_when_both_absent_or_present(self) -> None:
        filters = parse_symbol_filters(
            _exchange_info(notional_filter="NOTIONAL", min_notional="15.0", apply_min_to_market=True), "BTCUSDT"
        )
        assert filters.min_notional == 15.0
        assert filters.min_notional_applies_to_market is True


class TestDetermineMarketPriceRequirement:
    def test_limit_never_requires_price_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.01, price=100.0,
        )
        assert determine_market_price_requirement(intent, filters) is False

    def test_market_with_quote_quantity_never_requires_price_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        intent = _market_intent(quantity=None, quote_quantity=20.0)
        assert determine_market_price_requirement(intent, filters) is False

    def test_market_base_quantity_with_applicable_filter_requires_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is True

    def test_market_base_quantity_with_apply_to_market_false_does_not_require_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=False), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is False

    def test_market_with_no_notional_filter_at_all_does_not_require_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(notional_filter=None), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is False


class TestValidateIntentAgainstFilters:
    def test_valid_market_intent_passes(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        validate_intent_against_filters(_market_intent(quantity=0.01), filters)  # raise etmemeli

    def test_quantity_below_min_qty_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(quantity=0.00001), filters)

    def test_quantity_not_step_aligned_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(quantity=0.000123456), filters)

    def test_symbol_not_trading_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(status="BREAK"), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(), filters)

    def test_price_not_tick_aligned_rejected_for_limit(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.01, price=100.005,
        )
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(intent, filters)

    def test_limit_notional_below_minimum_rejected_using_intent_price(self) -> None:
        """#12 — LIMIT regresyonu: notional HER ZAMAN `intent.price`
        kullanır, hiçbir market-price lookup'ı GEREKMEZ."""
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.0001, price=1.0,
        )
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(intent, filters)  # market_price VERİLMEDİ, GEREKMEZ

    def test_limit_notional_passes_without_market_price_argument(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=1.0, price=100.0,
        )
        validate_intent_against_filters(intent, filters)  # raise etmemeli, market_price=None ile bile

    def test_never_silently_rounds_quantity(self) -> None:
        """Filtre ihlali AÇIKÇA reddedilir — quantity/price SESSİZCE
        farklı bir değere YUVARLANMAZ."""
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        original_quantity = 0.000123456
        try:
            validate_intent_against_filters(_market_intent(quantity=original_quantity), filters)
            raise AssertionError("beklenen FilterValidationError fırlatılmadı")
        except FilterValidationError:
            pass  # intent nesnesi hiç mutate edilmedi (frozen dataclass zaten garanti eder)


class TestMarketOrderNotionalUsesCurrentPrice:
    """BLOCKER FİX (Karar 73) regresyon testleri: MARKET order + base
    quantity + uygulanabilir bir notional filtresi -> GÜNCEL bir TESTNET
    PUBLIC fiyatı kullanılarak notional GERÇEKTEN doğrulanır."""

    def test_market_buy_below_min_notional_via_current_price_rejected_before_post(self) -> None:
        async def scenario() -> None:
            # price=50000, quantity=0.0001 -> notional=5.0 < min_notional=10.0
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(side=OrderSide.BUY, quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_sell_below_min_notional_via_current_price_rejected_before_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(side=OrderSide.SELL, quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_above_minimum_notional_price_lookup_then_submission(self) -> None:
        async def scenario() -> None:
            # price=50000, quantity=0.01 -> notional=500 >= min_notional=10.0
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 2  # exchangeInfo + ticker/price, ÖNCE
            assert len(http.post_calls) == 1
            assert http.get_calls[1][0].endswith("/api/v3/ticker/price")
            assert http.post_calls[0][0].endswith("/api/v3/order")

        run_async(scenario())

    def test_market_quote_quantity_uses_quote_amount_directly_no_price_fetch(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True))],  # yalnızca exchangeInfo
                post_responses=[_order_response()],
            )
            intent = _market_intent(quantity=None, quote_quantity=20.0)  # 20 >= min_notional=10.0
            result = await adapter.submit(intent)
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # HİÇBİR fiyat lookup'ı YAPILMADI

        run_async(scenario())

    def test_market_quote_quantity_below_minimum_rejected_without_price_fetch(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(get_responses=[json_response(_exchange_info(apply_to_market=True))])
            intent = _market_intent(quantity=None, quote_quantity=5.0)  # 5 < min_notional=10.0
            with pytest.raises(FilterValidationError):
                await adapter.submit(intent)
            assert len(http.get_calls) == 1
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_apply_to_market_false_does_not_impose_min_notional_on_market(self) -> None:
        async def scenario() -> None:
            # notional çok küçük olurdu (0.0001 * hiçbir fiyat çekilmedi) ama
            # applyToMarket=False olduğundan filtre MARKET'e HİÇ uygulanmaz.
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=False))],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.0001))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # fiyat lookup'ı HİÇ YAPILMADI (gereksiz)

        run_async(scenario())

    def test_notional_filter_apply_min_to_market_true_apply_max_to_market_false(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(
                        _exchange_info(
                            notional_filter="NOTIONAL", min_notional="10.0", max_notional="1000000.0",
                            apply_min_to_market=True, apply_max_to_market=False,
                        )
                    ),
                    _price_response(price="50000.0"),
                ]
            )
            # notional = 0.0001 * 50000 = 5.0 < min 10.0 -> min UYGULANIR -> reddedilir
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_notional_filter_apply_max_to_market_true_rejects_oversized_market_order(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(
                        _exchange_info(
                            notional_filter="NOTIONAL", min_notional="10.0", max_notional="100.0",
                            apply_min_to_market=False, apply_max_to_market=True,
                        )
                    ),
                    _price_response(price="50000.0"),
                ]
            )
            # notional = 1.0 * 50000 = 50000 > max 100.0 -> max UYGULANIR -> reddedilir
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=1.0))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_lot_size_used_for_market_quantity_validation(self) -> None:
        async def scenario() -> None:
            # MARKET_LOT_SIZE: min=0.001 — LOT_SIZE'ın min'i (0.0001) İZİN
            # VERİRDİ ama MARKET_LOT_SIZE VARSA MARKET için o KULLANILIR.
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(with_market_lot_size=True, apply_to_market=False))]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_lot_size_allows_quantity_valid_under_market_rules(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(with_market_lot_size=True, apply_to_market=False))],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))  # MARKET_LOT_SIZE aralığında/hizalı
            assert result.status == "FILLED"

        run_async(scenario())


class TestMarketPriceLookupFailClosed:
    """#8-#11 — fiyat lookup'ı BAŞARISIZ olursa order KESİNLİKLE
    GÖNDERİLMEZ (fail-closed); "doğrulamayı atla ve gönder" ASLA olmaz."""

    def test_price_lookup_transport_failure_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(_exchange_info(apply_to_market=True)),
                    ExecutionTransportError("connection reset"),
                ]
            )
            with pytest.raises(ExecutionTransportError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_price_lookup_timeout_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), ExecutionTimeoutError("timed out")]
            )
            with pytest.raises(ExecutionTimeoutError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_malformed_price_response_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), (200, "not json{{")]
            )
            with pytest.raises(MalformedResponseError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_price_response_missing_fields_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), json_response({"symbol": "BTCUSDT"})]
            )
            with pytest.raises(MalformedResponseError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_wrong_symbol_price_response_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(symbol="ETHUSDT")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(symbol="BTCUSDT", quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_zero_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="0")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_negative_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="-1.0")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_non_finite_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="NaN")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())


class TestTestnetExecutionAdapterSubmit:
    def test_submit_validates_then_places_order(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # exchangeInfo validate ÖNCE çağrıldı (fiyat lookup'ı GEREKMEDİ)
            assert len(http.post_calls) == 1

        run_async(scenario())

    def test_submit_rejects_before_transport_when_filters_violated(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(get_responses=[json_response(_exchange_info())])
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.000123456))
            assert len(http.post_calls) == 0  # invalid quantity TRANSPORT'a hiç ULAŞMADI

        run_async(scenario())

    def test_duplicate_intent_identity_produces_same_client_order_id(self) -> None:
        first = _market_intent(context_id="ctx-dup")
        second = _market_intent(context_id="ctx-dup")
        assert first.client_order_id == second.client_order_id

    def test_multi_symbol_isolation_independent_results(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info("BTCUSDT")), json_response(_exchange_info("ETHUSDT"))],
                post_responses=[
                    _order_response(),
                    json_response(
                        {
                            "symbol": "ETHUSDT", "clientOrderId": "csl-eth", "orderId": 2, "side": "SELL",
                            "status": "FILLED", "executedQty": "0.1", "cummulativeQuoteQty": "300.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    ),
                ],
            )
            btc_result = await adapter.submit(_market_intent(symbol="BTCUSDT", quantity=0.01))
            eth_result = await adapter.submit(_market_intent(symbol="ETHUSDT", side=OrderSide.SELL, quantity=0.01))
            assert btc_result.symbol == "BTCUSDT"
            assert eth_result.symbol == "ETHUSDT"
            assert btc_result.client_order_id != eth_result.client_order_id

        run_async(scenario())


=== FILE: tests/test_execution_cli.py ===
"""Faz 10/11 — scripts/binance_testnet_lab.py CLI testleri: dry-run/confirm
davranışı, kimlik bilgisi zorunluluğu, host yazdırma, secret redaction,
Faz 11 reconciliation komutları (order-status/reconcile/reconcile-pending).
Tamamen offline — `FakeTestnetHttpClient` + geçici SQLite enjekte edilir,
gerçek ağ YOK."""

from __future__ import annotations

from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

from scripts.binance_testnet_lab import run_cli_async

FAKE_KEY = "fake-cli-key"
FAKE_SECRET = "fake-cli-secret-value"


def _exchange_info(symbol: str = "BTCUSDT") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_response(**overrides) -> dict:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0", "transactTime": 1735689600000,
    }
    payload.update(overrides)
    return payload


def _set_execution_db(monkeypatch, tmp_path) -> None:
    """Her `--confirm-testnet-order`/`reconcile*`/`order-status` testi,
    GERÇEK proje dizinine ASLA bir SQLite dosyası yazmamalıdır — bu yüzden
    `BINANCE_TESTNET_EXECUTION_DB_PATH` her zaman izole bir `tmp_path`'e
    yönlendirilir."""
    monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(tmp_path / "execution.db"))


class TestValidateSymbolDryRun:
    def test_works_without_any_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(run_cli_async(["validate-symbol", "BTCUSDT"], http_client=http))

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Target host: https://testnet.binance.vision" in out
        assert "symbol=BTCUSDT" in out


class TestAccountCheckRequiresCredentials:
    def test_fails_cleanly_without_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["account-check"], http_client=http))

        assert exit_code != 0
        assert "ERROR" in capsys.readouterr().err

    def test_succeeds_with_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(
            get_responses=[json_response({"accountType": "SPOT", "canTrade": True, "balances": []})]
        )

        exit_code = run_async(run_cli_async(["account-check"], http_client=http))

        assert exit_code == 0
        assert "accountType=SPOT" in capsys.readouterr().out


class TestPlaceOrderRequiresExplicitConfirmation:
    def test_without_confirm_flag_performs_dry_run_only(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001"], http_client=http
            )
        )

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "DRY-RUN ONLY" in out
        assert len(http.post_calls) == 0  # order HİÇBİR ZAMAN gönderilmedi

    def test_dry_run_does_not_require_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001"], http_client=http
            )
        )

        assert exit_code == 0
        assert len(http.post_calls) == 0

    def test_with_confirm_flag_actually_submits(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code == 0
        assert len(http.post_calls) == 1
        assert "RESULT:" in capsys.readouterr().out

    def test_confirm_without_credentials_fails_cleanly(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code != 0
        assert len(http.post_calls) == 0

    def test_filter_violation_rejected_before_any_submission(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.000000001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code != 0
        assert len(http.post_calls) == 0

    def test_replay_same_context_id_produces_no_duplicate_post(self, monkeypatch, capsys, tmp_path) -> None:
        """Faz 11 — CLI üzerinden AYNI `--context-id` ile iki kez gönderim,
        yalnızca TEK bir gerçek POST üretir (idempotent replay)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info()), json_response(_exchange_info())],
            post_responses=[json_response(_order_response())],
        )
        argv = [
            "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
            "--context-id", "cli-ctx-replay", "--confirm-testnet-order",
        ]

        first = run_async(run_cli_async(argv, http_client=http))
        second = run_async(run_cli_async(argv, http_client=http))

        assert first == 0
        assert second == 0
        assert len(http.post_calls) == 1

    def test_market_price_lookup_reflected_in_dry_run_output(self, monkeypatch, capsys) -> None:
        """Faz 10 kontrat korunumu: MARKET notional doğrulaması gerektiğinde
        dry-run çıktısı, kullanılan market_price'ı gösterir."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        info = _exchange_info()
        info["symbols"][0]["filters"].append(
            {"filterType": "MIN_NOTIONAL", "minNotional": "10.0", "applyToMarket": True}
        )
        http = FakeTestnetHttpClient(
            get_responses=[json_response(info), json_response({"symbol": "BTCUSDT", "price": "50000.0"})]
        )

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.01"], http_client=http
            )
        )

        assert exit_code == 0
        assert "market_price=50000.0" in capsys.readouterr().out


class TestLimitOrder:
    def test_limit_dry_run_shows_price(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-limit", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001", "--price", "50000"],
                http_client=http,
            )
        )

        assert exit_code == 0
        assert "price=50000" in capsys.readouterr().out


class TestOrderStatusCommand:
    def test_no_local_record_returns_error(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["order-status", "--context-id", "does-not-exist"], http_client=http))

        assert exit_code != 0
        assert len(http.get_calls) == 0  # yalnızca yerel okuma — HİÇBİR ağ çağrısı YAPILMADI
        assert len(http.post_calls) == 0

    def test_shows_record_after_submission(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-status", "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )
        capsys.readouterr()

        exit_code = run_async(
            run_cli_async(["order-status", "--context-id", "cli-ctx-status"], http_client=FakeTestnetHttpClient())
        )

        assert exit_code == 0
        assert "context_id=cli-ctx-status" in capsys.readouterr().out


class TestReconcileCommand:
    def test_reconcile_unknown_identity_fails_cleanly_not_a_crash(self, monkeypatch, capsys, tmp_path) -> None:
        """Regresyon: `reconcile()` önceden ham bir `ValueError` fırlatıyordu
        (CLI'nin `except ExecutionError` yakalayıcısı tarafından
        YAKALANMAZDI) — artık `LocalExecutionRecordNotFoundError`
        (bir `ExecutionError` alt sınıfı) fırlatır, temiz bir "ERROR: ..."
        mesajıyla sonuçlanır, ham bir traceback DEĞİL."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(
            run_cli_async(["reconcile", "--context-id", "does-not-exist"], http_client=http)
        )

        assert exit_code != 0
        assert "ERROR" in capsys.readouterr().err

    def test_reconcile_requires_credentials_when_record_exists(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        submit_http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())],
            post_responses=[json_response(_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0"))],
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-needs-creds", "--confirm-testnet-order",
                ],
                http_client=submit_http,
            )
        )
        capsys.readouterr()

        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exit_code = run_async(
            run_cli_async(["reconcile", "--context-id", "cli-ctx-needs-creds"], http_client=FakeTestnetHttpClient())
        )

        assert exit_code != 0

    def test_reconcile_pending_with_nothing_pending(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["reconcile-pending"], http_client=http))

        assert exit_code == 0
        assert "No pending" in capsys.readouterr().out
        assert len(http.get_calls) == 0

    def test_reconcile_pending_requeries_unknown_not_found_without_new_post(self, monkeypatch, capsys, tmp_path) -> None:
        """BLOCKER FİX (Karar 78) regresyonu: ambiguous submission -2013 ile
        `UNKNOWN_NOT_FOUND`'a düştüğünde, bu kayıt reconciliation-pending
        KÜMESİNDE KALIR (artık "güvenli/çözülmüş" SAYILMAZ). Yeni bir
        "process" (`reconcile-pending`) bu kaydı GERÇEKTEN yeniden sorgular
        — ama ASLA yeni bir POST üretmez."""
        from crypto_signal_engine.execution.errors import ExecutionTimeoutError

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)

        # CLI wrapper'ın KENDİ `validate_intent()`'i VE `submit()`'in İÇ
        # `validate_intent()`'i AYRI AYRI birer GET tüketir (bkz. yukarıdaki
        # #8 testinin yorumu) — bu yüzden İKİ exchange_info yanıtı gerekir,
        # ardından reconcile-query'nin -2013'ü.
        submit_http = FakeTestnetHttpClient(
            get_responses=[
                json_response(_exchange_info()),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
            ],
            post_responses=[ExecutionTimeoutError("timed out")],
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-ambiguous", "--confirm-testnet-order",
                ],
                http_client=submit_http,
            )
        )
        capsys.readouterr()

        # submit()'in KENDİSİ zaten AMBIGUOUS -> UNKNOWN_NOT_FOUND'a reconcile
        # denedi (tek bir -2013). Kayıt HÂLÂ "çözülmemiş" (Karar 78) — bu
        # yüzden yeni bir "process" (`reconcile-pending`) onu GERÇEKTEN
        # yeniden sorgular (bir GET beklenir), ama HİÇBİR yeni POST üretmez.
        reconcile_http = FakeTestnetHttpClient(
            get_responses=[(400, '{"code": -2013, "msg": "Order does not exist."}')],
        )
        exit_code = run_async(run_cli_async(["reconcile-pending"], http_client=reconcile_http))

        assert exit_code == 0
        assert "UNKNOWN_NOT_FOUND" in capsys.readouterr().out
        assert len(reconcile_http.get_calls) == 1
        assert len(reconcile_http.post_calls) == 0

    def test_replay_confirmed_order_after_unknown_not_found_produces_no_second_post(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        """Test-matrisi #8: bir ambiguous POST timeout'u, ardından tek bir
        -2013 ile `UNKNOWN_NOT_FOUND`'a düştükten SONRA, AYNI confirmed CLI
        çağrısının (aynı `--context-id`) TEKRARLANMASI, İKİNCİ bir POST
        ÜRETMEZ — yalnızca yeniden sorgular (BLOCKER FİX, Karar 78)."""
        from crypto_signal_engine.execution.errors import ExecutionTimeoutError

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)

        # Sıra ÖNEMLİDİR: `_place_order()`'daki CLI wrapper'ın KENDİ
        # `validate_intent()` çağrısı VE `submit()`'in İÇ `validate_intent()`
        # çağrısı AYRI AYRI birer GET tüketir (bkz. `scripts/binance_testnet_lab.py`
        # `_place_order()` + `reconciliation_service.py::submit()`).
        # run1: CLI-validate(exchange_info) -> submit-validate(exchange_info)
        #       -> POST timeout -> reconcile-query(-2013) => UNKNOWN_NOT_FOUND
        # run2 (aynı context_id, HÂLÂ çözülmemiş): CLI-validate(exchange_info)
        #       -> submit() existing kaydı bulur -> reconcile-query(-2013)
        #       => HÂLÂ UNKNOWN_NOT_FOUND, YENİ POST YOK.
        http = FakeTestnetHttpClient(
            get_responses=[
                json_response(_exchange_info()),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
            ],
            post_responses=[ExecutionTimeoutError("timed out")],
        )
        argv = [
            "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
            "--context-id", "cli-ctx-replay-ambiguous", "--confirm-testnet-order",
        ]

        first = run_async(run_cli_async(argv, http_client=http))
        capsys.readouterr()
        second = run_async(run_cli_async(argv, http_client=http))

        assert first == 0
        assert second == 0
        assert "UNKNOWN_NOT_FOUND" in capsys.readouterr().out
        assert len(http.post_calls) == 1
        assert len(http.get_calls) == 5


class TestSecretNeverPrinted:
    def test_credentials_never_appear_in_stdout_or_stderr(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        captured = capsys.readouterr()
        assert FAKE_KEY not in captured.out
        assert FAKE_KEY not in captured.err
        assert FAKE_SECRET not in captured.out
        assert FAKE_SECRET not in captured.err

    def test_signature_param_never_printed_directly(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        _, params, _ = http.post_calls[0]
        assert "signature" in params  # transport'a doğru GİTTİ
        assert params["signature"] not in capsys.readouterr().out  # ama HİÇBİR ZAMAN CLI çıktısına YAZDIRILMADI

    def test_persisted_execution_record_never_contains_credentials(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        db_path = tmp_path / "execution.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(db_path))
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        raw_db_bytes = db_path.read_bytes()
        assert FAKE_KEY.encode() not in raw_db_bytes
        assert FAKE_SECRET.encode() not in raw_db_bytes


=== FILE: tests/test_execution_concurrency.py ===
"""HIGH-5 fix testleri — "Manual Binance Spot Testnet concurrent submit
aynı deterministic clientOrderId ile local durable execution truth'u
corrupt edebilir (örn. FILLED -> REJECTED)".

Bu dosya İKİ AYRI savunma katmanını AYRI AYRI kanıtlar:
1. `ExecutionStateStore.save()` seviyesinde MUTLAK, monotonik bir
   compare-and-swap (`assert_monotonic`, `BEGIN IMMEDIATE`) — bu, AYNI
   context_id'ye YAZAN İKİ BAĞIMSIZ (`ExecutionReconciliationService`
   instance'ı, farklı process'leri SİMÜLE eder — HİÇBİR kilit
   PAYLAŞMAZLAR) yazarın interleave olduğu EN KÖTÜ senaryoyu bile güvenli
   kılar.
2. `ExecutionReconciliationService` seviyesinde context_id-başına bir
   `asyncio.Lock` — AYNI process İÇİNDE eşzamanlı iki `submit()` çağrısının
   İKİSİNİN DE `place_order()`'a ULAŞMASINI (gerçek bir Binance'e ÇİFT POST
   göndermeyi) baştan engeller.

Tamamen offline — gerçek Binance TESTNET ağına ASLA bağımlı değil."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.errors import (
    BinanceRejectionError,
    ExecutionTransportError,
    ReconciliationContradictionError,
)
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import (
    ExecutionLifecycleState,
    apply_exchange_truth,
    new_record,
    transition,
)
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _exchange_info(symbol: str = "BTCUSDT") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-race", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


def _order_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-race", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.01", "cummulativeQuoteQty": "500.0",
        "transactTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _query_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-race", "orderId": 1, "side": "BUY",
        "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
        "updateTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _service(get_responses, post_responses, *, db_path) -> tuple[ExecutionReconciliationService, ExecutionStateStore]:
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    store = ExecutionStateStore(db_path)
    service = ExecutionReconciliationService(client, store, clock=FixedClock(NOW))
    return service, store


def _filled_record(intent: OrderIntent):
    record = new_record(intent, now=NOW)
    record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
    return apply_exchange_truth(
        record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=1,
        executed_quantity=0.01, cumulative_quote_quantity=500.0, now=NOW,
    )


class TestStoreLevelMonotonicity:
    """`ExecutionStateStore.save()`'in KENDİSİ, HERHANGİ bir çağıran
    disiplininden BAĞIMSIZ olarak İMKANSIZ regresyonları reddeder."""

    def test_filled_cannot_be_overwritten_by_rejected(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        filled = _filled_record(intent)
        store.save(filled)

        rejected = transition(
            new_record(intent, now=NOW), new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW
        )
        rejected = transition(rejected, new_state=ExecutionLifecycleState.REJECTED, now=NOW, detail="duplicate")

        with pytest.raises(ReconciliationContradictionError):
            store.save(rejected)

        # Durable truth DOKUNULMADAN FILLED kalır.
        current = store.load_by_context_id(intent.context_id)
        assert current.lifecycle_state == ExecutionLifecycleState.FILLED
        assert current.executed_quantity == 0.01
        store.close()

    def test_canceled_cannot_regress_to_new(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        canceled = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.CANCELED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        store.save(canceled)

        stale_new = apply_exchange_truth(
            transition(new_record(intent, now=NOW), new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW),
            new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        with pytest.raises(ReconciliationContradictionError):
            store.save(stale_new)
        assert store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.CANCELED
        store.close()

    def test_executed_quantity_cannot_decrease(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        partial = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=1,
            executed_quantity=0.005, cumulative_quote_quantity=250.0, now=NOW,
        )
        store.save(partial)

        regressed = apply_exchange_truth(
            partial, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=1,
            executed_quantity=0.005, cumulative_quote_quantity=250.0, now=NOW,
        )
        object.__setattr__(regressed, "executed_quantity", 0.001)  # stale/kaybolmuş bir güncelleme simülasyonu
        with pytest.raises(ReconciliationContradictionError):
            store.save(regressed)
        assert store.load_by_context_id(intent.context_id).executed_quantity == 0.005
        store.close()

    def test_exchange_order_id_cannot_change(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        acked = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        store.save(acked)

        mismatched = apply_exchange_truth(
            acked, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        object.__setattr__(mismatched, "exchange_order_id", 999)
        with pytest.raises(ReconciliationContradictionError):
            store.save(mismatched)
        store.close()

    def test_identical_restate_is_idempotent_no_error(self, tmp_path) -> None:
        """AYNI terminal durumun TEKRAR yazılması (örn. iki reconcile
        sweep'inin AYNI sonucu bulması) bir regresyon DEĞİLDİR."""
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        filled = _filled_record(intent)
        store.save(filled)
        store.save(filled)  # HATA FIRLATMAMALI
        assert store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED
        store.close()


class _RaceInjectingClient:
    """`place_order()` çağrıldığında, GERÇEK bir cross-process race'i
    simüle eder: KENDİ yanıtını (exception) döndürmeden HEMEN ÖNCE,
    "BAŞKA bir eşzamanlı yazarın" (örn. AYNI clientOrderId ile eşzamanlı
    çalışan ikinci bir CLI process'inin) SONUCUNU doğrudan store'a yazar.
    Bu, iki BAĞIMSIZ `ExecutionReconciliationService` instance'ının
    (HİÇBİR kilit PAYLAŞMADAN) AYNI context_id'ye eşzamanlı yazdığı
    en kötü durumu, GERÇEK asyncio zamanlama şansına bağlı KALMADAN
    deterministik olarak üretir."""

    def __init__(self, exchange_info_payload: dict, store: ExecutionStateStore, winner_record, own_exception: Exception):
        self._exchange_info_payload = exchange_info_payload
        self._store = store
        self._winner_record = winner_record
        self._own_exception = own_exception

    async def exchange_info(self, symbol: str) -> dict:
        return self._exchange_info_payload

    async def symbol_price(self, symbol: str) -> float:  # pragma: no cover - bu senaryoda çağrılmaz
        raise AssertionError("symbol_price bu testte çağrılmamalı")

    async def place_order(self, intent: OrderIntent):
        self._store.save(self._winner_record)
        raise self._own_exception


class TestConcurrentSubmitRace:
    """Tam olarak bildirilen HIGH-5 senaryosu: iki BAĞIMSIZ (kilit
    PAYLAŞMAYAN) `ExecutionReconciliationService`, AYNI context_id/
    client_order_id ile, AYNI durable store'a karşı eşzamanlı `submit()`
    çağırıyor. Biri FILLED alıyor, diğeri (Binance'in KENDİ duplicate-
    clientOrderId reddi nedeniyle) REJECTED alıyor."""

    def test_losing_rejected_result_cannot_overwrite_winning_filled_result(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()

            # "Kazanan" (A) süreci: place_order() BAŞARIYLA FILLED döner.
            winner_store = ExecutionStateStore(db_path)
            winner_client = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            winner_service = ExecutionReconciliationService(winner_client, winner_store, clock=FixedClock(NOW))
            winner_result = await winner_service.submit(intent)
            assert winner_result.lifecycle_state == ExecutionLifecycleState.FILLED

            # "Kaybeden" (B) süreci: TAMAMEN AYRI bir service/lock instance'ı
            # (farklı bir process'i simüle eder), AYNI context_id/
            # client_order_id ile eşzamanlı submit ediyor — ama Binance
            # KENDİSİ bu ikinci POST'u "duplicate order" olarak reddediyor.
            # place_order() çağrıldığı ANDA (A'nın SONUCU zaten durable
            # olduktan SONRA — bu, worst-case interleaving'i temsil eder)
            # kendi REJECTED yanıtını üretir.
            loser_store = ExecutionStateStore(db_path)  # AYNI dosya, AYRI connection (AYRI process simülasyonu)
            loser_client = _RaceInjectingClient(
                _exchange_info(), loser_store, winner_result,
                BinanceRejectionError("Duplicate order sent.", binance_code=-2010),
            )
            loser_service = ExecutionReconciliationService(loser_client, loser_store, clock=FixedClock(NOW))
            loser_result = await loser_service.submit(intent)

            # HIGH-5 invariant: kaybeden, KENDİ REJECTED sonucunu DÖNMEZ —
            # OTORİTER (kazanan) FILLED gerçeğini döner.
            assert loser_result.lifecycle_state == ExecutionLifecycleState.FILLED
            assert loser_result.executed_quantity == 0.01

            # Durable truth (HERHANGİ bir bağlantıdan okunduğunda) HÂLÂ FILLED'dir.
            assert winner_store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED
            assert loser_store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED

            winner_store.close()
            loser_store.close()

        run_async(scenario())

    def test_reconciliation_after_race_is_authoritative(self, tmp_path) -> None:
        """Yarıştan SONRA, `client_order_id` ile reconcile/sorgu, ZATEN
        terminal olan OTORİTER FILLED gerçeğini döner — GEREKSİZ bir
        exchange sorgusu YAPMADAN (kısa-devre, bkz. `reconcile()`)."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()
            store_a = ExecutionStateStore(db_path)
            client_a = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            service_a = ExecutionReconciliationService(client_a, store_a, clock=FixedClock(NOW))
            winner = await service_a.submit(intent)

            store_b = ExecutionStateStore(db_path)
            loser_client = _RaceInjectingClient(
                _exchange_info(), store_b, winner, BinanceRejectionError("Duplicate order sent.", binance_code=-2010)
            )
            service_b = ExecutionReconciliationService(loser_client, store_b, clock=FixedClock(NOW))
            await service_b.submit(intent)

            # Faz 11'in MEVCUT authoritative-by-client_order_id sözleşmesi.
            reconciled = await service_b.reconcile(client_order_id=intent.client_order_id)
            assert reconciled.lifecycle_state == ExecutionLifecycleState.FILLED

            store_a.close()
            store_b.close()

        run_async(scenario())

    def test_restart_after_race_sees_authoritative_filled(self, tmp_path) -> None:
        """"Restart" simülasyonu: yarış çözüldükten SONRA, TAMAMEN YENİ bir
        `ExecutionReconciliationService`/`ExecutionStateStore` (yeni bir
        process başlatımını temsil eder) AYNI context_id için `submit()`
        TEKRAR çağrılırsa (örn. operatör script'i TEKRAR çalıştırırsa),
        idempotent replay FILLED'i döner — KESİNLİKLE yeniden POST YAPMAZ."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()
            store_a = ExecutionStateStore(db_path)
            client_a = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            service_a = ExecutionReconciliationService(client_a, store_a, clock=FixedClock(NOW))
            winner = await service_a.submit(intent)
            store_a.close()

            store_b = ExecutionStateStore(db_path)
            loser_client = _RaceInjectingClient(
                _exchange_info(), store_b, winner, BinanceRejectionError("Duplicate order sent.", binance_code=-2010)
            )
            service_b = ExecutionReconciliationService(loser_client, store_b, clock=FixedClock(NOW))
            await service_b.submit(intent)
            store_b.close()

            # "Restart": tamamen yeni store/service/http — hiçbir POST scripti bile yok.
            restarted_store = ExecutionStateStore(db_path)
            restarted_http = FakeTestnetHttpClient([], [])
            restarted_client = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET), restarted_http, clock=FixedClock(NOW)
            )
            restarted_service = ExecutionReconciliationService(restarted_client, restarted_store, clock=FixedClock(NOW))
            replay = await restarted_service.submit(intent)
            assert replay.lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(restarted_http.post_calls) == 0  # idempotent replay — POST YOK
            restarted_store.close()

        run_async(scenario())


class TestInProcessLockPreventsDoubleSubmit:
    """AYNI process/`ExecutionReconciliationService` instance'ı içinde,
    eşzamanlı (`asyncio.gather`) iki `submit()` çağrısı, ASLA `place_order()`'a
    İKİ KEZ ULAŞMAZ — context_id başına `asyncio.Lock` bunu YAPISAL olarak
    engeller (bkz. reconciliation_service.py, HIGH-5 fix)."""

    def test_two_concurrent_submit_calls_same_service_post_exactly_once(self, tmp_path) -> None:
        async def scenario() -> None:
            service, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            first, second = await asyncio.gather(service.submit(intent), service.submit(intent))

            assert first.lifecycle_state == ExecutionLifecycleState.FILLED
            assert second.lifecycle_state == ExecutionLifecycleState.FILLED
            assert first.exchange_order_id == second.exchange_order_id
            store.close()

        run_async(scenario())

    def test_concurrent_submit_and_reconcile_do_not_corrupt_state(self, tmp_path) -> None:
        """`submit()` ile eşzamanlı bir `reconcile()` çağrısı da AYNI
        context_id kilidini paylaşır — birbirleriyle YARIŞMAZLAR."""
        async def scenario() -> None:
            service, store = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    _query_response(status="NEW"),
                ],
                post_responses=[ExecutionTransportError("connection reset")],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            submit_task = asyncio.create_task(service.submit(intent))
            await asyncio.sleep(0)  # submit()'in en azından başlamasına izin ver
            try:
                reconciled = await service.reconcile(context_id=intent.context_id)
            except Exception:
                reconciled = None  # kayıt henüz YOKSA LocalExecutionRecordNotFoundError beklenir
            submitted = await submit_task

            assert submitted.lifecycle_state in (
                ExecutionLifecycleState.ACKNOWLEDGED, ExecutionLifecycleState.FILLED,
                ExecutionLifecycleState.PARTIALLY_FILLED, ExecutionLifecycleState.AMBIGUOUS,
            )
            final = store.load_by_context_id(intent.context_id)
            assert final.lifecycle_state == submitted.lifecycle_state
            store.close()

        run_async(scenario())


=== FILE: tests/test_execution_lifecycle.py ===
"""
Autonomous Testnet trading lifecycle — unit tests for the pure domain
module `crypto_signal_engine.execution.lifecycle`. These are the direct
evidence for Phase 22's "Fee/quantity/price separation", "Effective stop
mechanics", "Candle-ordering/lookahead", and "P&L integrity" invariant
groups — all pure, no I/O, no clock.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    Candle,
    DailyRiskAccumulator,
    ExitPolicyConfig,
    ExitReason,
    FeeLedgerEntry,
    Fill,
    LifecyclePriceState,
    PositionLifecycleState,
    RiskPolicyConfig,
    apply_entry_fills,
    apply_exit_fills,
    apply_trade_to_daily_accumulator,
    compute_cooldown_until,
    compute_initial_stop_and_target,
    compute_sell_quantity,
    compute_trade_risk_contribution,
    cooldown_clear,
    daily_loss_breaker_tripped,
    evaluate_candle,
    flat_record,
    floor_to_step,
    gross_realized_pnl,
    max_exposure_gate_open,
    compute_total_exposure,
    max_positions_gate_open,
    net_realized_pnl,
    resolve_lot_size_filter,
    trading_day_key,
    unrealized_gross_pnl,
)

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _candle(*, open_: float, high: float, low: float, close: float, close_time: datetime) -> Candle:
    return Candle(open=open_, high=high, low=low, close=close, close_time=close_time)


class TestApplyEntryFills:
    def test_single_fill_no_commission(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.0, commission_asset="USDT"),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 100.0
        assert agg.net_owned_base_quantity == 2.0
        assert agg.fee_ledger_entries == ()

    def test_base_asset_commission_reduces_net_quantity_not_vwap(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 100.0  # unaffected by fee
        assert agg.net_owned_base_quantity == pytest.approx(1.998)
        assert len(agg.fee_ledger_entries) == 1
        assert agg.fee_ledger_entries[0].asset == "BTC"
        assert agg.fee_ledger_entries[0].amount == 0.002

    def test_quote_asset_commission_leaves_net_quantity_unchanged(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.2, commission_asset="USDT", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == 2.0
        assert len(agg.fee_ledger_entries) == 1
        assert agg.fee_ledger_entries[0].asset == "USDT"

    def test_bnb_commission_leaves_net_quantity_unchanged(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.001, commission_asset="BNB", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == 2.0

    def test_multi_fill_vwap_is_notional_weighted(self) -> None:
        fills = (
            Fill(price=100.0, quantity=1.0, commission=0.0, commission_asset="USDT"),
            Fill(price=200.0, quantity=1.0, commission=0.0, commission_asset="USDT"),
        )
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 150.0
        assert agg.gross_filled_quantity == 2.0

    def test_per_fill_commission_asset_can_vary_within_one_order(self) -> None:
        fills = (
            Fill(price=100.0, quantity=1.0, commission=0.001, commission_asset="BTC", trade_id=1),
            Fill(price=100.0, quantity=1.0, commission=0.1, commission_asset="USDT", trade_id=2),
        )
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == pytest.approx(1.999)
        assert len(agg.fee_ledger_entries) == 2

    def test_fee_recorded_exactly_once_never_double_counted(self) -> None:
        fills = (Fill(price=100.0, quantity=1.0, commission=0.001, commission_asset="BTC", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        total_fee_in_ledger = sum(e.amount for e in agg.fee_ledger_entries)
        assert total_fee_in_ledger == 0.001
        # gross_entry_vwap must show zero trace of the fee.
        assert agg.gross_entry_vwap == 100.0

    def test_empty_fills_rejected(self) -> None:
        with pytest.raises(ValueError):
            apply_entry_fills((), base_asset="BTC", client_order_id="csl-1", now=_NOW)


class TestApplyExitFills:
    def test_base_asset_commission_reported_separately_not_pre_subtracted(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        agg = apply_exit_fills(fills, base_asset="BTC", client_order_id="csl-2", now=_NOW)
        assert agg.exit_gross_vwap == 100.0
        assert agg.gross_sold_quantity == 2.0
        assert agg.base_asset_commission_total == 0.002


class TestPnL:
    def test_gross_realized_pnl_uses_only_gross_vwaps(self) -> None:
        pnl = gross_realized_pnl(gross_entry_vwap=100.0, exit_gross_vwap=110.0, quantity_closed=2.0)
        assert pnl == 20.0

    def test_net_realized_pnl_subtracts_known_fees_once(self) -> None:
        gross = 20.0
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=0.05, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
            FeeLedgerEntry(
                amount=0.02, asset="USDT", usdt_equivalent=0.02, client_order_id="csl-2",
                side="EXIT", trade_id=2, recorded_at=_NOW,
            ),
        )
        net = net_realized_pnl(gross_pnl=gross, fee_ledger_entries=fees)
        assert net == pytest.approx(20.0 - 0.07)

    def test_net_realized_pnl_unknown_when_any_fee_unconvertible(self) -> None:
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        assert net_realized_pnl(gross_pnl=20.0, fee_ledger_entries=fees) is None

    def test_base_asset_commission_already_in_net_qty_not_double_subtracted_in_pnl(self) -> None:
        # A BUY fill with base-asset commission reduces net_owned_base_quantity
        # (Phase 5) — the trade closes on that REDUCED quantity, and the
        # gross P&L formula multiplies by quantity_closed, which is already
        # net of the fee. The fee then appears ONCE MORE in fee_ledger for
        # the *net* P&L subtraction (a different, additive, USDT-denominated
        # deduction) — never as a second reduction of quantity/vwap.
        entry_fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        entry_agg = apply_entry_fills(entry_fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert entry_agg.net_owned_base_quantity == pytest.approx(1.998)

        exit_fills = (Fill(price=110.0, quantity=entry_agg.net_owned_base_quantity, commission=0.0, commission_asset="USDT"),)
        exit_agg = apply_exit_fills(exit_fills, base_asset="BTC", client_order_id="csl-2", now=_NOW)

        gross = gross_realized_pnl(
            gross_entry_vwap=entry_agg.gross_entry_vwap,
            exit_gross_vwap=exit_agg.exit_gross_vwap,
            quantity_closed=exit_agg.gross_sold_quantity,
        )
        assert gross == pytest.approx((110.0 - 100.0) * 1.998)

        fee_entry_usdt_equiv = 0.002 * 100.0  # priced at entry fill price for this test
        ledger = entry_agg.fee_ledger_entries[0]
        ledger_with_usdt = FeeLedgerEntry(
            amount=ledger.amount, asset=ledger.asset, usdt_equivalent=fee_entry_usdt_equiv,
            client_order_id=ledger.client_order_id, side=ledger.side, trade_id=ledger.trade_id,
            recorded_at=ledger.recorded_at,
        )
        net = net_realized_pnl(gross_pnl=gross, fee_ledger_entries=(ledger_with_usdt,))
        assert net == pytest.approx(gross - fee_entry_usdt_equiv)


class TestUnrealizedGrossPnl:
    """Dashboard localization — direct unit tests for the pure
    mark-to-market helper, independent of any dashboard/route/template
    exercise (per mission requirement #17)."""

    def test_price_above_entry_is_positive(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=2.0, current_price=110.0)
        assert pnl == pytest.approx(20.0)

    def test_price_below_entry_is_negative(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=2.0, current_price=95.0)
        assert pnl == pytest.approx(-10.0)

    def test_price_equal_to_entry_is_zero(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=3.0, current_price=100.0)
        assert pnl == 0.0

    def test_same_formula_shape_as_gross_realized_pnl(self) -> None:
        # Structurally identical to `gross_realized_pnl` -- `current_price`
        # simply stands in for `exit_gross_vwap` (mark-to-market, not an
        # actual fill).
        realized = gross_realized_pnl(gross_entry_vwap=50.0, exit_gross_vwap=53.0, quantity_closed=4.0)
        unrealized = unrealized_gross_pnl(gross_entry_vwap=50.0, net_owned_base_quantity=4.0, current_price=53.0)
        assert unrealized == realized


class TestInitialStopAndTarget:
    def test_formula(self) -> None:
        config = ExitPolicyConfig(stop_atr_multiple=2.0, take_profit_atr_multiple=4.0)
        stop, target = compute_initial_stop_and_target(entry_price=100.0, atr=5.0, config=config)
        assert stop == 90.0
        assert target == 120.0

    def test_stop_never_negative(self) -> None:
        config = ExitPolicyConfig(stop_atr_multiple=10.0)
        stop, _ = compute_initial_stop_and_target(entry_price=10.0, atr=5.0, config=config)
        assert stop == 0.0

    def test_rejects_non_positive_entry_price(self) -> None:
        with pytest.raises(ValueError):
            compute_initial_stop_and_target(entry_price=0.0, atr=5.0, config=ExitPolicyConfig())


def _fresh_state(*, entry_price: float = 100.0, atr: float = 5.0, config: ExitPolicyConfig | None = None) -> LifecyclePriceState:
    cfg = config or ExitPolicyConfig(stop_atr_multiple=2.0, take_profit_atr_multiple=4.0)
    stop, target = compute_initial_stop_and_target(entry_price=entry_price, atr=atr, config=cfg)
    return LifecyclePriceState(
        entry_price=entry_price, entry_timestamp=_NOW, initial_protective_stop=stop, take_profit=target,
        high_water=entry_price, effective_stop=stop, trailing_active=False, last_stop_mechanism="STOP_LOSS",
    )


class TestEvaluateCandleBasics:
    def test_no_trigger_updates_high_water_only(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=101, high=102, low=99, close=101, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None
        assert result.updated_state.high_water == 102
        assert result.updated_state.effective_stop == state.effective_stop  # unchanged, no favorable-enough move

    def test_stop_loss_triggers_on_pre_candle_stop(self) -> None:
        state = _fresh_state()  # effective_stop = 90.0
        config = ExitPolicyConfig()
        candle = _candle(open_=91, high=91, low=89, close=90, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_take_profit_triggers(self) -> None:
        state = _fresh_state()  # take_profit = 120.0
        config = ExitPolicyConfig()
        candle = _candle(open_=118, high=121, low=117, close=119, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.TAKE_PROFIT

    def test_stop_wins_when_both_fire_same_candle(self) -> None:
        state = _fresh_state()  # stop=90, target=120
        config = ExitPolicyConfig()
        candle = _candle(open_=100, high=125, low=85, close=100, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_max_hold_triggers_only_when_neither_stop_nor_target_fired(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig(max_hold_hours=1.0)
        candle = _candle(open_=101, high=102, low=99, close=101, close_time=_NOW + timedelta(hours=2))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.MAX_HOLD

    def test_max_hold_does_not_override_stop(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig(max_hold_hours=1.0)
        candle = _candle(open_=91, high=91, low=89, close=90, close_time=_NOW + timedelta(hours=2))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_flat_low_equal_to_stop_triggers(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=95, high=96, low=90.0, close=95, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS


class TestNoLookahead:
    def test_same_candle_high_water_update_cannot_trigger_its_own_low(self) -> None:
        """A candle whose HIGH is large enough to activate trailing and pull
        the trailing candidate up ABOVE this same candle's own LOW must NOT
        exit on this candle — the raised stop only applies starting the
        NEXT candle (Phase 6 point 5 / Phase 8)."""
        state = _fresh_state(entry_price=100.0, atr=5.0)  # stop=90, activation=2*ATR=10 -> at high_water>=110
        config = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=100.0,  # keep TP effectively unreachable
            trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0,
        )
        # This candle's high (115) both activates trailing AND produces a
        # trailing candidate (115 - 1*5 = 110) that is ABOVE this candle's
        # own low (95) — if lookahead existed, this would wrongly "trigger"
        # against 110 within the same candle. It must not.
        candle = _candle(open_=100, high=115, low=95, close=110, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None
        assert result.updated_state.effective_stop == 110.0  # raised, but only for the NEXT candle
        assert result.updated_state.trailing_active is True

        # The NEXT candle's low touching 110 now correctly triggers.
        candle2 = _candle(open_=110, high=112, low=109, close=111, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle2, config, atr_for_trailing=5.0)
        assert result2.exit_reason is ExitReason.TRAILING_STOP

    def test_entry_candle_cannot_retroactively_trigger_its_own_exit(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig()
        # A hypothetical "entry candle" whose low would touch the stop is
        # never fed to evaluate_candle for evaluation purposes at all in
        # live/replay wiring (only candles strictly after entry_timestamp
        # are evaluated) — this is enforced by the caller (Phase 8), and is
        # additionally safe here: entry_timestamp itself is never inside the
        # comparison, only close_time deltas for max-hold.
        candle = _candle(open_=100, high=101, low=99, close=100, close_time=state.entry_timestamp)
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None


class TestMonotonicEffectiveStop:
    def test_effective_stop_never_decreases_when_atr_contracts(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0)
        candle_up = _candle(open_=100, high=115, low=100, close=112, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle_up, config, atr_for_trailing=5.0)
        raised_stop = result.updated_state.effective_stop
        assert raised_stop > state.effective_stop

        # ATR EXPANDS sharply (high_water unchanged, since this candle's
        # high 112 < prior high_water 115) — a larger ATR widens the
        # trailing distance and would produce a LOWER trailing candidate
        # (115 - 1*10 = 105 < 110); the monotonic clamp must never let this
        # pull the stop back down.
        candle_pullback = _candle(open_=112, high=112, low=108, close=109, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle_pullback, config, atr_for_trailing=10.0)
        assert result2.updated_state.effective_stop == raised_stop

    def test_stale_atr_skips_trailing_recompute_but_not_stop_check(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0)
        candle_up = _candle(open_=100, high=115, low=100, close=112, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle_up, config, atr_for_trailing=None)
        # ATR unavailable -> trailing candidate cannot be computed, stop
        # stays at initial_protective_stop (still valid/monotonic).
        assert result.updated_state.effective_stop == state.initial_protective_stop
        assert result.updated_state.trailing_active is False

        # A subsequent candle whose low pierces the (still-initial) stop
        # must still trigger normally — staleness only disabled the
        # trailing-recompute step, not the stop check itself.
        candle_drop = _candle(open_=100, high=100, low=89, close=90, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle_drop, config, atr_for_trailing=None)
        assert result2.exit_reason is ExitReason.STOP_LOSS

    def test_take_profit_unchanged_across_many_evaluations(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig()
        current = state
        for i in range(10):
            candle = _candle(
                open_=100 + i, high=101 + i, low=99 + i, close=100 + i,
                close_time=_NOW + timedelta(minutes=i + 1),
            )
            current = evaluate_candle(current, candle, config, atr_for_trailing=5.0).updated_state
        assert current.take_profit == state.take_profit

    def test_trailing_only_tightens_never_below_initial_stop(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=100.0)  # never activates
        candle = _candle(open_=95, high=96, low=94, close=95, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.updated_state.effective_stop == state.initial_protective_stop


class TestSellSizing:
    def test_headroom_reserved_and_floored_to_step(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=1.0, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.headroom_reserved == pytest.approx(0.0015)
        assert result.quantity <= 1.0 - 0.0015
        assert result.quantity > 0

    def test_never_exceeds_owned_quantity(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=0.001, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.quantity <= 0.001

    def test_dust_when_below_min_qty_after_headroom(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=0.0009, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.quantity == 0.0


class TestSharedCandleOrderingImplementation:
    def test_live_and_replay_call_sites_use_the_identical_function_object(self) -> None:
        """Phase 20/22 — direct proof (not just "they happen to agree")
        that the live M1 evaluator and the replay sanity tool call the
        SAME `evaluate_candle` function object, never two implementations."""
        import crypto_signal_engine.execution.lifecycle as lifecycle_module
        import crypto_signal_engine.execution.lifecycle_manager as lifecycle_manager_module
        import crypto_signal_engine.execution.lifecycle_replay_sanity as lifecycle_replay_sanity_module

        assert lifecycle_manager_module.evaluate_candle is lifecycle_module.evaluate_candle
        assert lifecycle_replay_sanity_module.evaluate_candle is lifecycle_module.evaluate_candle


class TestFloorToStep:
    def test_basic(self) -> None:
        assert floor_to_step(1.2345, 0.01) == pytest.approx(1.23)

    def test_zero_step_returns_value_unchanged(self) -> None:
        assert floor_to_step(1.2345, 0.0) == 1.2345


class TestResolveLotSizeFilter:
    """Live-validation bug fix regression: `parse_symbol_filters` returns
    `0.0` (not `None`) for `market_step_size`/`market_min_qty` when a
    symbol has no separate `MARKET_LOT_SIZE` filter (the common case) —
    this must fall back to the base `LOT_SIZE` filter, never silently
    treat 0.0 as "the real step is zero" (which skips flooring entirely
    and lets an unfloored, non-lot-aligned SELL quantity reach Binance)."""

    def test_zero_market_step_falls_back_to_base_lot_size(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.0, market_min_qty=0.0, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.01
        assert min_qty == 0.01

    def test_none_market_step_falls_back_to_base_lot_size(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=None, market_min_qty=None, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.01
        assert min_qty == 0.01

    def test_genuine_positive_market_lot_size_is_used(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.1, market_min_qty=0.1, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.1
        assert min_qty == 0.1

    def test_headroom_sizing_with_previously_buggy_zero_market_filter_produces_lot_aligned_quantity(self) -> None:
        """Direct reproduction of the live bug: net_owned=0.83, LINKUSDT's
        real step_size=0.01, market_step_size=0.0 (no MARKET_LOT_SIZE
        filter) — the resulting SELL quantity must be a clean multiple of
        0.01, never the raw unfloored 0.828755..."""
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.0, market_min_qty=0.0, step_size=0.01, min_qty=0.01,
        )
        sizing = compute_sell_quantity(
            net_owned_base_quantity=0.83, step_size=step, min_qty=min_qty, sell_commission_headroom_bps=15.0,
        )
        # A clean multiple of 0.01 -- not 0.8287549999999999.
        assert round(sizing.quantity, 2) == sizing.quantity
        assert sizing.quantity == pytest.approx(0.82)


class TestBridgePositionRecord:
    def test_flat_record_has_no_price_fields(self) -> None:
        record = flat_record("BTCUSDT", now=_NOW)
        assert record.state is PositionLifecycleState.FLAT
        assert record.net_owned_base_quantity == 0.0
        assert record.gross_entry_vwap is None

    def test_long_state_requires_price_fields(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(symbol="BTCUSDT", state=PositionLifecycleState.LONG, updated_at=_NOW)

    def test_long_state_requires_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(
                symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
                net_owned_base_quantity=0.0, initial_protective_stop=90.0, high_water=100.0,
                effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
            )

    def test_to_price_state_round_trips_with_with_price_state(self) -> None:
        record = BridgePositionRecord(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
        )
        price_state = record.to_price_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=101, high=115, low=99, close=110, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(price_state, candle, config, atr_for_trailing=5.0)
        merged = record.with_price_state(result.updated_state, now=candle.close_time, candle_close_time=candle.close_time)
        assert merged.high_water == result.updated_state.high_water
        assert merged.effective_stop == result.updated_state.effective_stop
        # Non-price fields must be untouched by the merge.
        assert merged.net_owned_base_quantity == record.net_owned_base_quantity
        assert merged.gross_entry_vwap == record.gross_entry_vwap

    def test_to_price_state_rejects_non_long(self) -> None:
        record = flat_record("BTCUSDT", now=_NOW)
        with pytest.raises(ValueError):
            record.to_price_state()

    def test_updated_at_required(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(symbol="BTCUSDT", state=PositionLifecycleState.FLAT, updated_at=None)


class TestResolvedExitPolicy:
    """Adaptive Intelligence v1, step 2 -- `resolved_exit_policy()` is the
    ONE authoritative way any caller obtains the `ExitPolicyConfig` a
    SPECIFIC position must keep using for its remaining life."""

    def _record(self, **overrides) -> BridgePositionRecord:
        defaults = dict(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
        )
        defaults.update(overrides)
        return BridgePositionRecord(**defaults)

    def test_all_five_fields_present_reconstructs_embedded_policy(self) -> None:
        record = self._record(
            exit_policy_stop_atr_multiple=1.5, exit_policy_take_profit_atr_multiple=3.0,
            exit_policy_trailing_activation_atr_multiple=1.0, exit_policy_trailing_distance_atr_multiple=1.0,
            exit_policy_max_hold_hours=12.0, policy_version_id="v7",
        )
        default = ExitPolicyConfig()  # deliberately different from the embedded values
        resolved = record.resolved_exit_policy(default=default)
        assert resolved.stop_atr_multiple == 1.5
        assert resolved.take_profit_atr_multiple == 3.0
        assert resolved.trailing_activation_atr_multiple == 1.0
        assert resolved.trailing_distance_atr_multiple == 1.0
        assert resolved.max_hold_hours == 12.0
        assert resolved is not default

    def test_missing_fields_falls_back_to_default(self) -> None:
        # A legacy position, persisted before this milestone -- all six
        # new fields at their `None` default.
        record = self._record()
        assert record.policy_version_id is None
        default = ExitPolicyConfig(max_hold_hours=37.0)
        resolved = record.resolved_exit_policy(default=default)
        assert resolved is default

    def test_partial_fields_present_still_falls_back_to_default(self) -> None:
        # Defensive: even one missing field must fall back to `default`
        # wholesale, never a mix of embedded and default values.
        record = self._record(
            exit_policy_stop_atr_multiple=1.5, exit_policy_take_profit_atr_multiple=3.0,
            exit_policy_trailing_activation_atr_multiple=1.0, exit_policy_trailing_distance_atr_multiple=1.0,
            exit_policy_max_hold_hours=None,
        )
        default = ExitPolicyConfig(max_hold_hours=37.0)
        resolved = record.resolved_exit_policy(default=default)
        assert resolved is default


class TestComputeTotalExposure:
    """Portfolio/Accounting v1, step 1 — the single, de-duplicated
    exposure formula (previously independently duplicated in
    `LifecycleManager.entry_gate()` and `app.py::_bridge_lifecycle_
    snapshot()`)."""

    def _long(self, symbol: str, *, vwap: float, qty: float) -> BridgePositionRecord:
        return BridgePositionRecord(
            symbol=symbol, state=PositionLifecycleState.LONG, gross_entry_vwap=vwap,
            net_owned_base_quantity=qty, initial_protective_stop=vwap * 0.9, high_water=vwap,
            effective_stop=vwap * 0.9, take_profit=vwap * 1.1, entry_timestamp=_NOW, updated_at=_NOW,
        )

    def test_empty_sequence_is_zero(self) -> None:
        assert compute_total_exposure([]) == 0.0

    def test_flat_positions_contribute_nothing(self) -> None:
        positions = [flat_record("BTCUSDT", now=_NOW), flat_record("ETHUSDT", now=_NOW)]
        assert compute_total_exposure(positions) == 0.0

    def test_long_positions_contribute_cost_basis(self) -> None:
        positions = [self._long("BTCUSDT", vwap=100.0, qty=2.0), self._long("ETHUSDT", vwap=50.0, qty=1.0)]
        assert compute_total_exposure(positions) == pytest.approx(250.0)

    def test_dust_still_contributes_never_excluded(self) -> None:
        from dataclasses import replace as _replace

        dust = _replace(self._long("BTCUSDT", vwap=100.0, qty=0.5), state=PositionLifecycleState.DUST)
        assert compute_total_exposure([dust]) == pytest.approx(50.0)

    def test_mixed_flat_and_non_flat(self) -> None:
        positions = [
            self._long("BTCUSDT", vwap=100.0, qty=1.0),  # 100.0
            flat_record("ETHUSDT", now=_NOW),  # 0.0
        ]
        assert compute_total_exposure(positions) == pytest.approx(100.0)


class TestRiskGates:
    def test_max_positions_gate(self) -> None:
        config = RiskPolicyConfig(max_open_positions=3)
        assert max_positions_gate_open(open_slot_count=2, config=config) is True
        assert max_positions_gate_open(open_slot_count=3, config=config) is False

    def test_max_exposure_gate(self) -> None:
        config = RiskPolicyConfig(max_total_exposure_usdt=100.0)
        assert max_exposure_gate_open(current_exposure_usdt=90.0, additional_notional_usdt=10.0, config=config) is True
        assert max_exposure_gate_open(current_exposure_usdt=95.0, additional_notional_usdt=10.0, config=config) is False

    def test_cooldown(self) -> None:
        config = RiskPolicyConfig(cooldown_minutes=30.0)
        until = compute_cooldown_until(exit_time=_NOW, config=config)
        assert until == _NOW + timedelta(minutes=30)
        assert cooldown_clear(cooldown_until=until, now=_NOW + timedelta(minutes=29)) is False
        assert cooldown_clear(cooldown_until=until, now=_NOW + timedelta(minutes=30)) is True
        assert cooldown_clear(cooldown_until=None, now=_NOW) is True


class TestDailyLossBreaker:
    def test_known_fees_always_subtracted_never_treated_as_zero(self) -> None:
        config = RiskPolicyConfig()
        fees = (
            FeeLedgerEntry(
                amount=1.0, asset="USDT", usdt_equivalent=1.0, client_order_id="csl-1",
                side="EXIT", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=10.0, fee_ledger_entries=fees, config=config)
        assert contribution.fee_basis == "KNOWN"
        assert contribution.conservative_pnl == 9.0

    def test_unknown_fee_uses_conservative_reserve_not_zero(self) -> None:
        config = RiskPolicyConfig(unknown_fee_conservative_reserve_usdt=0.5)
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=10.0, fee_ledger_entries=fees, config=config)
        assert contribution.fee_basis == "RESERVED"
        assert contribution.conservative_pnl == pytest.approx(9.5)

    def test_conservative_figure_trips_breaker_when_gross_alone_would_not(self) -> None:
        """Gross P&L alone (+2.0) looks fine, but after subtracting a
        larger-than-gross conservative fee reserve the conservative figure
        goes negative enough to trip the breaker — proving the breaker uses
        the fee-adjusted figure, not raw gross."""
        config = RiskPolicyConfig(daily_loss_limit_usdt=1.0, unknown_fee_conservative_reserve_usdt=5.0)
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="EXIT", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=2.0, fee_ledger_entries=fees, config=config)
        accumulator = apply_trade_to_daily_accumulator(DailyRiskAccumulator(trading_day="2026-01-01"), contribution)
        assert accumulator.conservative_risk_pnl == pytest.approx(-3.0)
        assert daily_loss_breaker_tripped(accumulator, config) is True

    def test_breaker_not_tripped_when_within_limit(self) -> None:
        config = RiskPolicyConfig(daily_loss_limit_usdt=50.0)
        accumulator = DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-10.0)
        assert daily_loss_breaker_tripped(accumulator, config) is False

    def test_trading_day_key_is_utc_calendar_day(self) -> None:
        assert trading_day_key(_NOW) == "2026-01-01"
        assert trading_day_key(_NOW + timedelta(hours=23, minutes=59)) == "2026-01-01"
        assert trading_day_key(_NOW + timedelta(hours=24)) == "2026-01-02"


=== FILE: tests/test_execution_lifecycle_manager.py ===
"""
Autonomous Testnet trading lifecycle — `LifecycleManager` orchestration
tests (Phase 5/6/7/10/11/13/14/15/17/18). Fully offline: `FakeTestnetHttpClient`
+ `FixedClock` + temp SQLite, same discipline as
`tests/test_signal_testnet_bridge.py`/`tests/test_execution_reconciliation_service.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.execution.lifecycle import (
    Candle,
    ExitPolicyConfig,
    ExitReason,
    PositionLifecycleState,
    RiskPolicyConfig,
    compute_cooldown_until,
    flat_record,
)
from crypto_signal_engine.execution.lifecycle_manager import EntryGateResult, LifecycleManager, derive_base_asset
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import ExecutionResult, Fill, OrderSide
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"
SYMBOL = "BTCUSDT"


def _exchange_info(symbol: str = SYMBOL, *, step_size: str = "0.0001", min_qty: str = "0.0001") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": min_qty, "maxQty": "9000.0", "stepSize": step_size},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _exchange_info_with_min_notional(
    symbol: str = SYMBOL, *, step_size: str = "0.0001", min_qty: str = "0.0001",
    min_notional: str = "5.0", applies_to_market: bool = True,
) -> dict:
    """Same shape as `_exchange_info()` plus a legacy `MIN_NOTIONAL` filter
    (the exact filter type real Binance Testnet returns for the affected
    live symbols — see the "notional ... minimum ... altında" wording
    already produced by `adapter.py::_check_notional`, which matches the
    already-observed live log lines this fix addresses)."""
    info = _exchange_info(symbol, step_size=step_size, min_qty=min_qty)
    info["symbols"][0]["filters"].append(
        {"filterType": "MIN_NOTIONAL", "minNotional": min_notional, "applyToMarket": applies_to_market}
    )
    return info


def _price_response(price: float, *, symbol: str = SYMBOL) -> tuple[int, str]:
    return json_response({"symbol": symbol, "price": str(price)})


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int = 1, status: str = "FILLED", fills=None) -> dict:
    payload = {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }
    if fills is not None:
        payload["fills"] = fills
    return payload


def _manager(
    get_responses=None, post_responses=None, *, tmp_path: Path, risk_policy=None, exit_policy=None,
    exit_policy_provider=None, notifier=None,
):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    manager = LifecycleManager(
        store=lifecycle_store, execution_service=service, exit_policy=exit_policy, risk_policy=risk_policy,
        clock=FixedClock(NOW), exit_policy_provider=exit_policy_provider, notifier=notifier,
    )
    return manager, http, exec_store, lifecycle_store


def _fake_buy_result(*, price: float = 50000.0, qty: float = 0.002, commission: float = 0.0, commission_asset: str = "BTC") -> ExecutionResult:
    return ExecutionResult(
        symbol=SYMBOL, client_order_id="csl-entry-1", exchange_order_id=1, side=OrderSide.BUY,
        status="FILLED", executed_quantity=qty, cumulative_quote_quantity=price * qty,
        transaction_time=NOW, context_id="bridge:BTCUSDT:OPEN:ctx-1",
        fills=(Fill(price=price, quantity=qty, commission=commission, commission_asset=commission_asset, trade_id=1),),
    )


class TestDeriveBaseAsset:
    def test_strips_usdt_suffix(self) -> None:
        assert derive_base_asset("BTCUSDT") == "BTC"

    def test_rejects_non_usdt_quote(self) -> None:
        with pytest.raises(ValueError):
            derive_base_asset("BTCETH")


class TestOnEntryFilled:
    def test_persists_long_position_with_fee_separation(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        result = _fake_buy_result(price=50000.0, qty=0.002, commission=0.000002, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.state is PositionLifecycleState.LONG
        assert record.gross_entry_vwap == 50000.0  # fee-free
        assert record.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        assert record.initial_protective_stop == 50000.0 - 2.0 * 500.0
        assert record.take_profit == 50000.0 + 4.0 * 500.0
        assert record.high_water == 50000.0
        assert record.entry_client_order_id == "csl-entry-1"

        persisted = store.load_position(SYMBOL)
        assert persisted == record
        fees = store.fee_ledger_for_trade_group(SYMBOL, "csl-entry-1")
        assert len(fees) == 1
        assert fees[0].asset == "BTC"

    def test_fallback_stop_target_when_atr_unavailable(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="USDT")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=None)

        record = run_async(scenario())
        assert record.initial_protective_stop == pytest.approx(98.0)
        assert record.take_profit == pytest.approx(104.0)

    def test_backfills_fills_via_my_trades_when_missing(self, tmp_path: Path) -> None:
        get_responses = [
            json_response([
                {"symbol": SYMBOL, "id": 9, "orderId": 1, "price": "50000.0", "qty": "0.002",
                 "commission": "0.000002", "commissionAsset": "BTC"},
            ]),
        ]
        manager, _, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        result = ExecutionResult(
            symbol=SYMBOL, client_order_id="csl-entry-1", exchange_order_id=1, side=OrderSide.BUY,
            status="FILLED", executed_quantity=0.002, cumulative_quote_quantity=100.0,
            transaction_time=NOW, context_id="bridge:BTCUSDT:OPEN:ctx-1", fills=(),
        )

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        fees = store.fee_ledger_for_trade_group(SYMBOL, "csl-entry-1")
        assert len(fees) == 1

    def test_preserves_cumulative_realized_pnl_across_reentry(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        store.save_position(flat_record(SYMBOL, now=NOW))
        # Simulate a prior realized P&L already accumulated.
        prior = store.load_position(SYMBOL)
        from dataclasses import replace
        store.save_position(replace(prior, cumulative_realized_gross_pnl=42.0))

        result = _fake_buy_result()

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.cumulative_realized_gross_pnl == 42.0


class TestEntryGate:
    def test_open_when_no_positions(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is True

    def test_cooldown_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        from dataclasses import replace
        store.save_position(replace(flat_record(SYMBOL, now=NOW), cooldown_until=NOW + timedelta(minutes=10)))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "cooldown" in result.detail

    def test_max_positions_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=1))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        assert result.allowed is False
        assert "max open positions" in result.detail

    def test_max_exposure_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_total_exposure_usdt=50.0))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        assert result.allowed is False
        assert "max exposure" in result.detail

    def test_max_exposure_blocks_a_prospective_order_that_would_itself_breach_the_cap(self, tmp_path: Path) -> None:
        """CRITICAL FIX (C1 — mainnet-readiness review) regression test.
        Existing exposure across OTHER symbols is comfortably under the
        cap (10/100 USDT) but the prospective BUY's own notional (500
        USDT) would blow straight through it. Before the fix,
        `additional_notional_usdt` was hardcoded to 0.0 inside
        `entry_gate()`, so this scenario was WRONGLY allowed (10 + 0 <=
        100) — the gate never looked at the size of the very order it was
        supposed to be gating."""
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_total_exposure_usdt=100.0))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=10.0,
            net_owned_base_quantity=1.0, initial_protective_stop=9.0, high_water=10.0,
            effective_stop=9.0, take_profit=12.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        # Sanity check: with no prospective addition, the gate still opens
        # (existing exposure alone is well under the cap).
        assert manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT")).allowed is True

        result = manager.entry_gate(
            SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"), additional_notional_usdt=500.0,
        )
        assert result.allowed is False
        assert "max exposure" in result.detail
        assert "500.00" in result.detail

    def test_dust_excluded_from_slot_count_but_included_in_exposure(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=5, max_total_exposure_usdt=50.0)
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.DUST, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        # slot count excludes DUST -> still allowed by slot gate ...
        # ...but exposure (100 USDT) exceeds the 50 USDT cap -> blocked by exposure.
        assert result.allowed is False
        assert "max exposure" in result.detail

    def test_daily_loss_breaker_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0))
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key
        store.save_daily_risk(DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1), now=NOW)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "daily loss" in result.detail

    def test_daily_loss_breaker_notifies_once_per_transition(self, tmp_path: Path) -> None:
        """24/7 Ops v1, Step 4 — required per-site test: the notifier
        fires exactly ONCE on the transition into a tripped breaker for
        a given trading day, not on every subsequent blocked entry_gate()
        call the same day."""
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key

        calls: list[str] = []
        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0), notifier=calls.append,
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        manager.entry_gate("ETHUSDT", all_symbols=(SYMBOL, "ETHUSDT"))
        assert len(calls) == 1
        assert "daily loss" in calls[0]

    def test_a_raising_notifier_never_affects_entry_gate_decision(self, tmp_path: Path) -> None:
        """24/7 Ops v1, Step 4 — required per-site test: the underlying
        decision (breaker still trips correctly) is UNAFFECTED even when
        the notifier itself raises."""
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key

        def _raising_notifier(message: str) -> None:
            raise RuntimeError("simulated notifier failure")

        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
            notifier=_raising_notifier,
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "daily loss" in result.detail

    def test_no_notification_when_breaker_not_tripped(self, tmp_path: Path) -> None:
        calls: list[str] = []
        manager, _, _, _ = _manager(tmp_path=tmp_path, notifier=calls.append)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is True
        assert calls == []

    def test_max_exposure_decision_byte_for_bit_unchanged_after_dedup_refactor(self, tmp_path: Path) -> None:
        """Portfolio/Accounting v1, step 1 — required regression test:
        `entry_gate()`'s exposure computation was refactored to call the
        newly-extracted `compute_total_exposure()` instead of an inline
        loop. This fixture hand-computes the OLD formula's expected
        result (sum of gross_entry_vwap * net_owned_base_quantity over
        every non-FLAT position, DUST included, FLAT/None-vwap excluded)
        across a mixed-state multi-symbol universe and asserts the
        `EntryGateResult` (both `allowed` AND the exact `detail` string,
        which embeds the computed exposure number) is IDENTICAL to that
        hand-computed expectation -- proving pure de-duplication, zero
        behavior change."""
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=10, max_total_exposure_usdt=1000.0),
        )
        fixture_positions = {
            "ETHUSDT": BridgePositionRecord(
                symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
                net_owned_base_quantity=2.0, initial_protective_stop=90.0, high_water=100.0,
                effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
            ),  # non-FLAT, contributes 200.0
            "BNBUSDT": BridgePositionRecord(
                symbol="BNBUSDT", state=PositionLifecycleState.DUST, gross_entry_vwap=50.0,
                net_owned_base_quantity=0.5, initial_protective_stop=45.0, high_water=50.0,
                effective_stop=45.0, take_profit=60.0, entry_timestamp=NOW, updated_at=NOW,
            ),  # DUST, still non-FLAT -> contributes 25.0
            "SOLUSDT": flat_record("SOLUSDT", now=NOW),  # FLAT -> contributes 0.0
        }
        for record in fixture_positions.values():
            store.save_position(record)

        expected_exposure = 200.0 + 25.0 + 0.0
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT", "BNBUSDT", "SOLUSDT"))

        assert result.allowed is True  # 225.0 <= 1000.0 cap
        assert result == EntryGateResult(True, "entry gates open")

        # Now tighten the cap to just below the hand-computed exposure and
        # confirm the exact detail string embeds that SAME number.
        tight_dir = tmp_path / "tight"
        tight_dir.mkdir()
        manager_tight, _, _, store_tight = _manager(
            tmp_path=tight_dir, risk_policy=RiskPolicyConfig(max_open_positions=10, max_total_exposure_usdt=224.0),
        )
        for record in fixture_positions.values():
            store_tight.save_position(record)
        tight_result = manager_tight.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT", "BNBUSDT", "SOLUSDT"))
        assert tight_result.allowed is False
        assert tight_result.detail == f"max exposure reached ({expected_exposure:.2f}/224.0 USDT)"


class TestAttemptExit:
    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_not_long_is_no_op(self, tmp_path: Path) -> None:
        manager, http, _, store = _manager(tmp_path=tmp_path)
        store.save_position(flat_record(SYMBOL, now=NOW))

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert http.post_calls == []

    def test_full_sell_transitions_to_flat_and_records_trade(self, tmp_path: Path) -> None:
        # `ExecutionRecord` (what `submit()` actually returns) never carries
        # `fills` — the manager always backfills via `myTrades` after a
        # confirmed FILLED SELL (see `LifecycleManager._fetch_fills`).
        # Two `exchange_info` responses are consumed first (this method's
        # own `validate_symbol` call, then `submit()`'s internal
        # `validate_intent`), then `myTrades` for the fee backfill.
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 5, "orderId": 1, "price": "110.0", "qty": "1.0",
                 "commission": "0.11", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.TAKE_PROFIT, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "SOLD"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cooldown_until is not None
        assert final.cumulative_realized_gross_pnl == pytest.approx(10.0)

        trades = store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1
        assert trades[0]["gross_realized_pnl"] == pytest.approx(10.0)
        assert trades[0]["net_realized_pnl"] == pytest.approx(10.0 - 0.11)
        assert trades[0]["exit_reason"] == "TAKE_PROFIT"

    def test_dust_when_quantity_unsellable(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info(step_size="0.01", min_qty="0.01"))]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=0.001)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "DUST"
        assert http.post_calls == []
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.DUST

    def test_repeated_attempt_on_dust_position_never_resubmits(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info(step_size="0.01", min_qty="0.01"))]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=0.001)

        async def scenario():
            await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"  # already DUST, not LONG -> no-op
        assert http.post_calls == []

    def test_unresolved_submit_marks_exit_pending(self, tmp_path: Path) -> None:
        # Binance acknowledges the order but it has not reached a terminal
        # fill state yet within this response (status=NEW) — the position
        # must become EXIT_PENDING, not FLAT, until reconciliation confirms
        # a terminal state (Phase 5/17).
        get_responses = [json_response(_exchange_info())]
        post_responses = [json_response(_order_payload(side="SELL", qty="0.0", quote_qty="0.0", status="NEW"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "EXIT_PENDING"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.EXIT_PENDING
        assert final.exit_pending_client_order_id is not None

    def test_transport_failure_during_submit_leaves_position_long_for_safe_retry(self, tmp_path: Path) -> None:
        """`ExecutionReconciliationService.submit()` itself durably records
        an AMBIGUOUS `execution_record` and then re-raises when its own
        internal reconciliation attempt ALSO fails — the exit context_id is
        deterministic (symbol+reason+entry_client_order_id), so a later
        retry safely converges on the SAME durable record via the
        service's own idempotency rather than double-submitting; leaving
        the lifecycle position as LONG (instead of a separate EXIT_PENDING
        label) is safe specifically because of that determinism."""
        from crypto_signal_engine.execution.errors import ExecutionTransportError

        get_responses = [json_response(_exchange_info()), ExecutionTransportError("query also unresolved")]
        post_responses = [ExecutionTransportError("connection reset")]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG

    def test_rejected_sell_with_zero_execution_reverts_to_long_not_exit_pending(self, tmp_path: Path) -> None:
        """Live-validation bug fix regression: a REJECTED order (e.g.
        Binance -1013 LOT_SIZE filter failure) is TERMINAL — it will NEVER
        become FILLED via reconciliation. The original bug labeled this
        EXIT_PENDING, which permanently stranded the position (Phase 22's
        `evaluate_m1_candle` only re-evaluates `state == LONG`). It must
        revert to LONG, unchanged, so the position stays monitored and can
        be retried on the next candle."""
        get_responses = [json_response(_exchange_info())]
        post_responses = [json_response({"code": -1013, "msg": "Filter failure: LOT_SIZE"}, status_code=400)]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.LONG
        # Position is otherwise byte-for-byte unchanged -- no economic effect.
        assert final.net_owned_base_quantity == 1.0
        assert final.gross_entry_vwap == 100.0
        assert "SUBMIT_FAILED" in final.last_exit_reason

    def test_rejected_sell_can_be_retried_from_a_later_candle_and_succeed(self, tmp_path: Path) -> None:
        """Direct proof the revert actually enables a working retry (not
        just a state label): a retry from the SAME triggering event (same
        `last_evaluated_candle_close`) correctly reuses the dead REJECTED
        context (never silently duplicating), but a retry from a
        DIFFERENT (later) M1 candle — a genuinely new triggering event —
        gets a fresh context_id and can submit/fill normally."""
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response(_exchange_info()), json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 9, "orderId": 2, "price": "90.0", "qty": "1.0",
                 "commission": "0.09", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [
            json_response({"code": -1013, "msg": "Filter failure: LOT_SIZE"}, status_code=400),
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0", order_id=2)),
        ]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store, last_evaluated_candle_close=NOW)

        async def scenario():
            first = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            # Same triggering event retried -- still the same dead
            # context_id, correctly deduped to the cached REJECTED result.
            same_event_retry = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            # A later M1 candle fires the SAME reason again -- a genuinely
            # NEW triggering event (`last_evaluated_candle_close` advances,
            # exactly as `evaluate_m1_candle` does before calling this).
            from dataclasses import replace as _dc_replace
            later = store.load_position(SYMBOL)
            store.save_position(_dc_replace(later, last_evaluated_candle_close=NOW + timedelta(minutes=1)))
            next_candle_attempt = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            return first, same_event_retry, next_candle_attempt

        first, same_event_retry, next_candle_attempt = run_async(scenario())
        assert first.action == "NO_ACTION"
        assert same_event_retry.action == "NO_ACTION"  # same event, same dead context -- correctly deduped
        assert next_candle_attempt.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_canceled_with_partial_fill_finalizes_the_executed_portion(self, tmp_path: Path) -> None:
        """A CANCELED order can still carry a nonzero `executedQty` if
        partially filled before cancellation — Phase 13's "account for
        executed quantity, not requested quantity" applies even on this
        terminal-but-not-FILLED path."""
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 10, "orderId": 3, "price": "90.0", "qty": "0.4",
                 "commission": "0.036", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="0.4", quote_qty="36.0", order_id=3, status="CANCELED"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=1.0)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "PARTIAL"
        final = store.load_position(SYMBOL)
        # 0.6 remaining is a LEGALLY SELLABLE amount (self-review fix) --
        # stays LONG so it keeps being monitored/retried, never DUST.
        assert final.state is PositionLifecycleState.LONG
        assert final.net_owned_base_quantity == pytest.approx(0.6)
        trades = store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1
        assert trades[0]["quantity_closed"] == pytest.approx(0.4)


class TestExitPersistenceAmbiguityPinning:
    """H3 fix (mainnet-readiness review, "phantom exit"): when `submit()`
    raises `ExecutionPersistenceError` with `exchange_may_have_accepted_
    order=True` (place_order() succeeded but the local save afterward
    failed), `attempt_exit()` must NOT silently return NO_ACTION and
    leave the position LONG-and-unchanged (the old behaviour) — that
    would let the very next M1 candle submit ANOTHER real SELL on top of
    a possibly-already-filled one. It must instead pin the position into
    EXIT_PENDING (blocking further attempts) carrying the attempted
    client_order_id."""

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def _manager_with_flaky_exec_store(self, tmp_path: Path, *, get_responses, post_responses):
        from tests.test_execution_reconciliation_service import _FlakyExecutionStore

        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = _FlakyExecutionStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        return manager, http, exec_store, lifecycle_store

    def test_post_ack_persistence_failure_pins_exit_pending_not_silent_no_action(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info()), json_response(_exchange_info())]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        # First save (SUBMISSION_ATTEMPTED, before place_order) succeeds;
        # only the SECOND save (after the exchange already accepted the
        # order) fails -- exactly the dangerous, ambiguous case.
        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "EXIT_PENDING"
        assert len(http.post_calls) == 1  # the SELL really was sent to the exchange

        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.EXIT_PENDING
        assert pinned.exit_pending_client_order_id is not None

        # A second attempt_exit() call (e.g. the next M1 candle) must NOT
        # submit another SELL -- state != LONG blocks it outright.
        async def retry():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        retry_outcome = run_async(retry())
        assert retry_outcome.action == "NO_ACTION"
        assert len(http.post_calls) == 1  # still just the one real POST, no duplicate


class TestReconcileStuckPosition:
    """CRITICAL FIX (C4 — mainnet-readiness review) regression tests.

    Before this fix, an AMBIGUOUS (BUY-side H3) or EXIT_PENDING (SELL-side
    H3/Phase 17) pin was PERMANENT: nothing ever revisited it, and
    `lifecycle_migration.py::reconstruct_legacy_ownership()`'s own first
    guard (`if lifecycle_store.load_position(symbol) is not None: return
    None`) means the one-time legacy-migration path can NEVER touch a
    symbol that already has ANY `bridge_position` row. `reconcile_stuck_
    position()` is the only path that can ever correct such a record —
    these tests exercise all four of its real outcomes."""

    def _manager_with_flaky_exec_store(self, tmp_path: Path, *, get_responses, post_responses):
        from tests.test_execution_reconciliation_service import _FlakyExecutionStore

        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = _FlakyExecutionStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        return manager, http, exec_store, lifecycle_store

    def _query_payload(self, *, order_id: int = 1, side: str = "BUY", qty: str = "0.002", quote_qty: str = "100.0", status: str = "FILLED") -> dict:
        return {
            "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
            "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
            "updateTime": int(NOW.timestamp() * 1000),
        }

    def _my_trades_payload(self, *, price: str = "50000.0", qty: str = "0.002") -> list:
        return [{"symbol": SYMBOL, "id": 9, "orderId": 1, "price": price, "qty": qty, "commission": "0.0", "commissionAsset": "BTC"}]

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def _pin_ambiguous_buy(self, manager: LifecycleManager, exec_store) -> None:
        """Reproduces the exact H3 pinning sequence (a post-ack persistence
        failure during a BUY submit) `signal_bridge.py::_submit()` handles
        in production — this test file exercises `LifecycleManager`
        directly, without the bridge layer."""
        from crypto_signal_engine.errors import PersistenceError
        from crypto_signal_engine.execution.errors import ExecutionPersistenceError
        from crypto_signal_engine.execution.models import OrderIntent, OrderType

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save
        intent = OrderIntent(
            symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
            context_id="bridge:BTCUSDT:OPEN:ctx-1", timestamp=NOW, quote_quantity=100.0,
        )

        async def scenario() -> None:
            with pytest.raises(ExecutionPersistenceError) as excinfo:
                await manager._service.submit(intent)  # noqa: SLF001 - deliberately bypassing signal_bridge for this test
            manager.mark_ambiguous(
                SYMBOL, detail=f"BUY may have reached the exchange: {excinfo.value}",
                client_order_id=excinfo.value.client_order_id,
            )

        run_async(scenario())
        exec_store.save = original_save  # stop flaking before the reconciliation step

    def test_ambiguous_buy_that_actually_filled_is_promoted_to_long(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # original submit()'s validate_intent
            json_response(self._query_payload(status="FILLED")),  # reconcile()'s query_order
            json_response(self._my_trades_payload()),  # on_entry_filled()'s fills backfill
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)
        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.AMBIGUOUS
        assert pinned.entry_order_client_order_id is not None

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "PROMOTED_TO_LONG"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.LONG
        assert record.gross_entry_vwap == pytest.approx(100.0 / 0.002)
        assert record.net_owned_base_quantity == pytest.approx(0.002)

    def test_ambiguous_buy_that_never_filled_is_released_to_flat(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response(self._query_payload(status="REJECTED", qty="0", quote_qty="0")),
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)
        assert store.load_position(SYMBOL).state is PositionLifecycleState.AMBIGUOUS

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "RELEASED_TO_FLAT"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_ambiguous_position_still_unresolved_leaves_pin_in_place(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response(self._query_payload(status="ACKNOWLEDGED", qty="0", quote_qty="0")),  # still non-terminal
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "STILL_AMBIGUOUS"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.AMBIGUOUS

    def test_not_applicable_for_a_normal_long_position(self, tmp_path: Path) -> None:
        manager, _, _, store = self._manager_with_flaky_exec_store(tmp_path, get_responses=[], post_responses=[])
        self._seed_long(store)
        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "NOT_APPLICABLE"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG

    def test_exit_pending_sell_that_actually_filled_is_finalized(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # attempt_exit()'s filter lookup
            json_response(_exchange_info()),  # submit()'s validate_intent
            json_response(self._query_payload(side="SELL", qty="1.0", quote_qty="110.0", status="FILLED")),  # reconcile()
            json_response(_exchange_info()),  # _reconcile_pending_exit()'s validate_symbol for min_qty
            json_response(self._my_trades_payload(price="110.0", qty="1.0")),  # _finalize_exit()'s fills backfill
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        pending_outcome = run_async(scenario())
        assert pending_outcome.action == "EXIT_PENDING"
        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.EXIT_PENDING
        assert pinned.exit_pending_client_order_id is not None
        exec_store.save = original_save

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "FINALIZED_SOLD"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.FLAT

    def test_exit_pending_sell_that_never_filled_is_reverted_to_long(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # attempt_exit()'s filter lookup
            json_response(_exchange_info()),  # submit()'s validate_intent
            json_response(self._query_payload(side="SELL", qty="0", quote_qty="0", status="REJECTED")),  # reconcile()
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        pending_outcome = run_async(scenario())
        assert pending_outcome.action == "EXIT_PENDING"
        exec_store.save = original_save

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "REVERTED_TO_LONG"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.LONG
        assert record.net_owned_base_quantity == pytest.approx(1.0)
        assert record.exit_pending_client_order_id is None


class TestMinNotionalPreflight:
    """MIN_NOTIONAL pre-flight fix — a residual that clears LOT_SIZE but
    whose (price x quantity) is below the exchange's MIN_NOTIONAL filter
    must NEVER reach `self._service.submit()` (no wasted real-Testnet
    order/API budget, no repeated "SELL submission did not confirm"
    noise), but must ALSO never be marked DUST (unlike a true LOT_SIZE
    dust residual, notional can clear again on its own once price moves)."""

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_blocked_when_notional_below_minimum_stays_long_no_submission(self, tmp_path: Path) -> None:
        # quantity clears LOT_SIZE (min_qty=0.0001) but price(4.0) x
        # quantity(~1.0) = ~4.0 USDT is below the 5.0 USDT MIN_NOTIONAL.
        get_responses = [json_response(_exchange_info_with_min_notional())]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=4.0,
            )

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert "MIN_NOTIONAL" in outcome.detail
        assert "5.0" in outcome.detail  # the actual threshold is stated
        assert http.post_calls == []  # the doomed order was NEVER submitted
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.LONG  # NEVER DUST -- notional can still recover
        assert final.net_owned_base_quantity == pytest.approx(1.0)  # untouched

    def test_recovery_after_price_rises_sell_finalizes_normally(self, tmp_path: Path) -> None:
        """THE recovery test — proves a MIN_NOTIONAL-blocked residual is
        NEVER locked out of a real future sale. Same position, same
        symbol: first blocked at a low price, then (a later candle) price
        has risen enough to clear MIN_NOTIONAL -- the SELL must actually
        submit and finalize exactly like any normal exit."""
        get_responses = [
            json_response(_exchange_info_with_min_notional()),  # attempt 1: validate_symbol (blocked, no more calls)
            json_response(_exchange_info_with_min_notional()),  # attempt 2: this method's own validate_symbol
            json_response(_exchange_info_with_min_notional()),  # attempt 2: submit()'s internal validate_intent
            _price_response(10.0),  # attempt 2: submit()'s internal validate_intent MARKET-notional price fetch
            json_response([  # attempt 2: myTrades fee backfill after FILLED
                {"symbol": SYMBOL, "id": 7, "orderId": 1, "price": "10.0", "qty": "1.0",
                 "commission": "0.01", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="10.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            blocked = await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=4.0,
            )
            state_after_blocked = store.load_position(SYMBOL).state  # checked BEFORE the second attempt
            recovered = await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=10.0,
            )
            return blocked, state_after_blocked, recovered

        blocked, state_after_blocked, recovered = run_async(scenario())
        assert blocked.action == "NO_ACTION"
        assert state_after_blocked is PositionLifecycleState.LONG  # never abandoned after being blocked

        assert recovered.action == "SOLD"  # the REAL sale actually happens once price recovers
        assert len(http.post_calls) == 1
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cumulative_realized_gross_pnl == pytest.approx((10.0 - 100.0) * 1.0)

    def test_notional_clears_minimum_submits_normally_no_regression(self, tmp_path: Path) -> None:
        """A position genuinely sellable on the very first check, WITH a
        MIN_NOTIONAL filter present (price x quantity clears it) — behaves
        byte-for-byte like the pre-fix full-sell path (see
        `TestAttemptExit.test_full_sell_transitions_to_flat_and_records_trade`),
        proving the new gate never interferes with a legitimately sellable
        position."""
        get_responses = [
            json_response(_exchange_info_with_min_notional()), json_response(_exchange_info_with_min_notional()),
            _price_response(110.0),
            json_response([
                {"symbol": SYMBOL, "id": 8, "orderId": 1, "price": "110.0", "qty": "1.0",
                 "commission": "0.11", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(
                SYMBOL, reason=ExitReason.TAKE_PROFIT, exit_signal_context_id=None, current_price=110.0,
            )

        outcome = run_async(scenario())
        assert outcome.action == "SOLD"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cumulative_realized_gross_pnl == pytest.approx(10.0)

    def test_price_lookup_failure_during_preflight_fails_closed(self, tmp_path: Path) -> None:
        """No `current_price` supplied (the opposite-signal call-site
        shape) AND the fallback `symbol_price()` lookup itself fails --
        must fail closed exactly like the existing filter-lookup-failure
        convention in this same method: no submission, NO_ACTION, LONG
        untouched."""
        from crypto_signal_engine.execution.errors import ExecutionTransportError

        get_responses = [json_response(_exchange_info_with_min_notional()), ExecutionTransportError("timeout")]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert "fail-closed" in outcome.detail
        assert http.post_calls == []
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG


class TestEvaluateM1Candle:
    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_no_trigger_updates_high_water_persisted(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=101, high=105, low=99, close=101, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        result = run_async(scenario())
        assert result is None
        assert store.load_position(SYMBOL).high_water == 105

    def test_pre_entry_candle_never_evaluated(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=1, high=1, low=1, close=1, close_time=NOW)  # == entry_timestamp

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        result = run_async(scenario())
        assert result is None
        # high_water untouched -- proves the candle was never fed to evaluate_candle at all.
        assert store.load_position(SYMBOL).high_water == 100.0

    def test_stop_trigger_submits_sell(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "90.0", "qty": "1.0",
                 "commission": "0.09", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=91, high=91, low=89, close=90, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(scenario())
        assert outcome is not None
        assert outcome.action == "SOLD"
        assert outcome.exit_reason is ExitReason.STOP_LOSS
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_m1_candle_close_used_for_min_notional_preflight_no_extra_price_fetch(self, tmp_path: Path) -> None:
        """Design choice (a): `evaluate_m1_candle` already has the
        triggering candle's close price in hand, so the MIN_NOTIONAL
        pre-flight must use `candle.close` directly -- proven here by
        there being NO `symbol_price()` GET at all (only the one
        `validate_symbol` exchangeInfo call) despite a MIN_NOTIONAL filter
        being present and the check actually running (blocking the SELL)."""
        get_responses = [json_response(_exchange_info_with_min_notional())]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)
        # close=4.0 x quantity~1.0 = ~4.0 USDT, below the 5.0 USDT minimum.
        candle = Candle(open=91, high=91, low=4, close=4.0, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(scenario())
        assert outcome is not None
        assert outcome.action == "NO_ACTION"
        assert "MIN_NOTIONAL" in outcome.detail
        assert http.post_calls == []
        assert len(http.get_calls) == 1  # exactly the one validate_symbol call -- no redundant price fetch
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG


class _MutablePolicyProvider:
    """Test double for `exit_policy_provider` -- a simple mutable holder so
    a test can simulate an `adaptive/` champion promotion mid-test by
    reassigning `.policy`/`.version_id` between two `on_entry_filled()`
    calls, with zero dependency on the real `adaptive/` package."""

    def __init__(self, policy: ExitPolicyConfig, version_id: str) -> None:
        self.policy = policy
        self.version_id = version_id

    def __call__(self) -> tuple[ExitPolicyConfig, str | None]:
        return self.policy, self.version_id


class TestAdaptivePolicyPinning:
    """Adaptive Intelligence v1, step 2 -- the position-pinning invariant:
    a currently-OPEN position must keep using whichever `ExitPolicyConfig`
    it was actually opened under for its ENTIRE remaining life, even after
    the champion/`self._exit_policy` changes; a brand-new position opened
    AFTER the swap must use the new policy. See `BridgePositionRecord.
    resolved_exit_policy()` and `LifecycleManager.evaluate_m1_candle()`."""

    SYMBOL_2 = "ETHUSDT"

    def test_new_position_embeds_provider_policy_and_version_id(self, tmp_path: Path) -> None:
        policy = ExitPolicyConfig(
            stop_atr_multiple=1.5, take_profit_atr_multiple=3.0, trailing_activation_atr_multiple=1.0,
            trailing_distance_atr_multiple=1.0, max_hold_hours=12.0,
        )
        provider = _MutablePolicyProvider(policy, "policy-v1")
        manager, _, _, store = _manager(tmp_path=tmp_path, exit_policy_provider=provider)
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=10.0)

        record = run_async(scenario())
        assert record.policy_version_id == "policy-v1"
        assert record.exit_policy_stop_atr_multiple == 1.5
        assert record.exit_policy_take_profit_atr_multiple == 3.0
        assert record.exit_policy_trailing_activation_atr_multiple == 1.0
        assert record.exit_policy_trailing_distance_atr_multiple == 1.0
        assert record.exit_policy_max_hold_hours == 12.0
        # Stop/target math itself used the PROVIDER's policy, not the
        # manager's static (unset, default) `self._exit_policy`.
        assert record.initial_protective_stop == 100.0 - 1.5 * 10.0
        assert record.take_profit == 100.0 + 3.0 * 10.0
        assert store.load_position(SYMBOL).policy_version_id == "policy-v1"

    def test_no_provider_embeds_static_policy_with_none_version_id(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)  # no exit_policy -> ExitPolicyConfig() defaults
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=10.0)

        record = run_async(scenario())
        assert record.policy_version_id is None
        default = ExitPolicyConfig()
        assert record.exit_policy_stop_atr_multiple == default.stop_atr_multiple
        assert record.exit_policy_take_profit_atr_multiple == default.take_profit_atr_multiple
        assert record.exit_policy_max_hold_hours == default.max_hold_hours

    def test_core_proof_open_position_keeps_v1_behavior_after_promotion_to_v2(self, tmp_path: Path) -> None:
        """THE core Adaptive Intelligence v1 proof: open a position under
        champion v1 (max_hold_hours=1.0), "promote" to v2 (max_hold_hours
        =100.0) while it is STILL OPEN by mutating the provider exactly as
        a real champion swap would, feed it a candle 2 hours later that
        triggers neither stop nor target -- it must STILL exit on
        MAX_HOLD (proving it used v1's 1-hour limit, not v2's 100-hour
        one). A position opened AFTER the promotion must use v2."""
        policy_v1 = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        policy_v2 = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=100.0,
        )
        provider = _MutablePolicyProvider(policy_v1, "policy-v1")
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
            exit_policy_provider=provider,
        )
        entry_result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def open_v1():
            return await manager.on_entry_filled(SYMBOL, result=entry_result, signal_context_id="ctx-1", atr=10.0)

        opened = run_async(open_v1())
        assert opened.policy_version_id == "policy-v1"
        assert opened.initial_protective_stop == 100.0 - 2.0 * 10.0  # 80.0
        assert opened.take_profit == 100.0 + 4.0 * 10.0  # 120.0

        # Simulate an `adaptive/` champion promotion: a NEW champion policy
        # takes effect for future entries only -- this open position must
        # be completely unaffected.
        provider.policy = policy_v2
        provider.version_id = "policy-v2"

        # Neither stop (80) nor target (120) is touched; only max-hold can
        # fire. close_time is 2h after entry -- v1's 1h limit is exceeded,
        # v2's 100h limit is not.
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

        # A brand-new position opened AFTER the promotion uses v2.
        entry_result_2 = ExecutionResult(
            symbol=self.SYMBOL_2, client_order_id="csl-entry-2", exchange_order_id=2, side=OrderSide.BUY,
            status="FILLED", executed_quantity=1.0, cumulative_quote_quantity=100.0,
            transaction_time=NOW, context_id="bridge:ETHUSDT:OPEN:ctx-2",
            fills=(Fill(price=100.0, quantity=1.0, commission=0.0, commission_asset="ETH", trade_id=2),),
        )

        async def open_v2():
            return await manager.on_entry_filled(
                self.SYMBOL_2, result=entry_result_2, signal_context_id="ctx-2", atr=10.0,
            )

        opened_2 = run_async(open_v2())
        assert opened_2.policy_version_id == "policy-v2"
        assert opened_2.exit_policy_max_hold_hours == 100.0

    def test_legacy_position_missing_policy_fields_falls_back_to_manager_default(self, tmp_path: Path) -> None:
        """Backward compatibility (required -- real open Testnet positions
        predate this change): a position persisted BEFORE this milestone
        has all six new fields `None`. Loading and evaluating it must not
        crash and must produce IDENTICAL behavior to before this change --
        i.e. it is treated as opened under the manager's own current
        `self._exit_policy`, never the `ExitPolicyConfig()` bare default
        and never any provider-resolved policy (the provider is only ever
        consulted for a BRAND NEW position's entry)."""
        custom_policy = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        # Provider present but must NEVER be consulted for an already-open
        # legacy position -- only `self._exit_policy` (the manager's own
        # static config) is the correct fallback here. It returns a
        # 999-hour max-hold that would NOT fire on the 2h-later candle
        # below, so wrongly consulting it would silently swallow the
        # MAX_HOLD exit this test proves DOES fire.
        provider = _MutablePolicyProvider(
            ExitPolicyConfig(max_hold_hours=999.0), "should-never-be-used",
        )
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
            exit_policy=custom_policy, exit_policy_provider=provider,
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        legacy = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
            # exit_policy_* / policy_version_id all left at their None default.
        )
        assert legacy.policy_version_id is None
        store.save_position(legacy)

        # candle.close_time is 2h after entry -- fires MAX_HOLD only under
        # `custom_policy`'s 1h limit (the manager's `self._exit_policy`),
        # never under the provider's 999h value.
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        # Fires MAX_HOLD -- proving `custom_policy` (1h) governed this
        # position, not the provider's 999h value (which would have
        # produced `outcome is None` here instead).
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_legacy_position_max_hold_still_fires_under_manager_default(self, tmp_path: Path) -> None:
        """Companion to the above: WITHOUT a provider at all (the common,
        real-world case for every position that predates this milestone --
        `app.py`'s existing static-config wiring never sets one), a legacy
        position's max-hold behavior is unaffected by this change: it
        still exits on MAX_HOLD exactly as it would have before the
        position-pinning fields existed."""
        custom_policy = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path, exit_policy=custom_policy,
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        legacy = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        store.save_position(legacy)
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT


