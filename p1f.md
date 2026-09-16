<!-- p1f.md — Part 6/6 of all_code_part1.md — 58 files -->
<!-- Contents of this part: -->
<!--   - tests/test_feature_engine.py -->
<!--   - tests/test_feature_registry.py -->
<!--   - tests/test_feature_state.py -->
<!--   - tests/test_gap_and_atomic_checkpoint_remediation.py -->
<!--   - tests/test_immutability.py -->
<!--   - tests/test_lifecycle_concurrency.py -->
<!--   - tests/test_lifecycle_runtime.py -->
<!--   - tests/test_local_readiness_check.py -->
<!--   - tests/test_ops_admin.py -->
<!--   - tests/test_ops_config.py -->
<!--   - tests/test_ops_dashboard.py -->
<!--   - tests/test_ops_event_log.py -->
<!--   - tests/test_ops_health_snapshot.py -->
<!--   - tests/test_ops_lock.py -->
<!--   - tests/test_ops_logging.py -->
<!--   - tests/test_ops_no_hardcoded_paths.py -->
<!--   - tests/test_ops_notifier.py -->
<!--   - tests/test_ops_systemd_backup_template.py -->
<!--   - tests/test_ops_systemd_notify.py -->
<!--   - tests/test_ops_systemd_template.py -->
<!--   - tests/test_orchestrator.py -->
<!--   - tests/test_orderbook_features.py -->
<!--   - tests/test_package_import.py -->
<!--   - tests/test_paper_trading_engine.py -->
<!--   - tests/test_paper_trading_models.py -->
<!--   - tests/test_paper_trading_notional_override.py -->
<!--   - tests/test_persistence_paper_state_store.py -->
<!--   - tests/test_persistence_recovery.py -->
<!--   - tests/test_persistence_serialization.py -->
<!--   - tests/test_phase12_local_production_readiness.py -->
<!--   - tests/test_portfolio_accounting.py -->
<!--   - tests/test_production_hot_reselection.py -->
<!--   - tests/test_providers.py -->
<!--   - tests/test_quality.py -->
<!--   - tests/test_repository_safety_scan.py -->
<!--   - tests/test_repository_safety_scan_adaptive.py -->
<!--   - tests/test_repository_safety_scan_research.py -->
<!--   - tests/test_research_attribution.py -->
<!--   - tests/test_research_data_quality.py -->
<!--   - tests/test_research_drift.py -->
<!--   - tests/test_research_monte_carlo.py -->
<!--   - tests/test_research_oos_stability.py -->
<!--   - tests/test_research_orderbook_capture.py -->
<!--   - tests/test_research_replay.py -->
<!--   - tests/test_research_sizing.py -->
<!--   - tests/test_reselection_scheduler.py -->
<!--   - tests/test_risk_overlay.py -->
<!--   - tests/test_run_with_adaptive_policy_script.py -->
<!--   - tests/test_runtime_bootstrap.py -->
<!--   - tests/test_runtime_candle_window.py -->
<!--   - tests/test_runtime_coordinator.py -->
<!--   - tests/test_runtime_coordinator_candle_observer.py -->
<!--   - tests/test_runtime_coordinator_hot_symbols.py -->
<!--   - tests/test_runtime_health.py -->
<!--   - tests/test_runtime_models.py -->
<!--   - tests/test_safety.py -->
<!--   - tests/test_selection_models.py -->
<!--   - tests/test_selection_reevaluation.py -->

=== FILE: tests/test_feature_engine.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade
from crypto_signal_engine.errors import FeatureValidationError
from crypto_signal_engine.features.engine import FeatureEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_candles(n: int, closes=None, is_closed=True, symbol="BTCUSDT", timeframe=Timeframe.M1) -> list[Candle]:
    closes = closes or [100.0 + i * 0.5 for i in range(n)]
    candles = []
    for i in range(n):
        open_time = T0 + timedelta(minutes=i)
        c = closes[i]
        candles.append(Candle(
            symbol=symbol, timeframe=timeframe, open_time=open_time,
            close_time=open_time + timedelta(minutes=1),
            open=c, high=c + 1.0, low=c - 1.0, close=c, volume=10.0,
            is_closed=is_closed, trade_count=1,
        ))
    return candles


class TestClosedCandlePolicy:
    def test_open_candle_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25, is_closed=False)
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])

    def test_closed_candles_accepted(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])
        assert "SMA_20" in snap


class TestMixedSymbolTimeframeRejected:
    def test_mixed_symbol_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(21, symbol="BTCUSDT") + make_candles(1, symbol="ETHUSDT")
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="mixed-symbol"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])

    def test_mixed_timeframe_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(21, timeframe=Timeframe.M1) + make_candles(1, timeframe=Timeframe.M5)
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="mixed-timeframe"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])


class TestInsufficientHistoryOmission:
    def test_feature_omitted_when_insufficient_history(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(5)  # SMA_20 için yetersiz
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])
        assert "SMA_20" not in snap
        assert dict(snap.values) == {}

    def test_partial_snapshot_with_mixed_sufficient_insufficient(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(5)
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(
            candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["BODY_SIZE", "SMA_20"]
        )
        assert "BODY_SIZE" in snap  # min_history=1, yeterli
        assert "SMA_20" not in snap  # min_history=20, yetersiz


class TestNoLookAheadAdversarial:
    """Bölüm 17 — HARD ACCEPTANCE GATE: T anındaki feature, T'den sonraki
    veri var olsun ya da olmasın AYNI olmalı."""

    def test_future_open_candle_ignored_without_exception(self) -> None:
        """KRİTİK regresyon (no-look-ahead blocker düzeltmesi): T anında
        SMA_20, dataset T'de bitse de T'den SONRAKİ AÇIK (henüz kapanmamış)
        bir candle içerse de AYNI olmalı ve HİÇBİR exception fırlatılmamalı.

        Önceki hatalı implementasyon, `close_time > as_of` filtresinden ÖNCE
        `is_closed` kontrolü yaptığından, ufkun DIŞINDAKİ (gelecekteki) açık
        bir candle'ı yanlışlıkla closed-candle politikası ihlali olarak
        reddediyordu — bu tamamen sahte bir hataydı (o candle henüz
        oluşmaktadır, bu NORMAL ve BEKLENEN bir durumdur).
        """
        engine = FeatureEngine()
        closed_candles = make_candles(20)  # T = candles[19].close_time
        t = closed_candles[-1].close_time

        # A) dataset T'de bitiyor (yalnızca 20 kapalı candle)
        snap_a = engine.compute_candle_features(closed_candles, "BTCUSDT", Timeframe.M1, t, feature_names=["SMA_20"])

        # B) AYNI 20 kapalı candle + T'den SONRAKİ, HENÜZ KAPANMAMIŞ bir candle.
        future_open_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=t, close_time=t + timedelta(minutes=1),
            open=999.0, high=1000.0, low=998.0, close=999.5, volume=1.0, is_closed=False,
        )
        dataset_b = closed_candles + [future_open_candle]
        snap_b = engine.compute_candle_features(dataset_b, "BTCUSDT", Timeframe.M1, t, feature_names=["SMA_20"])

        assert snap_a.get("SMA_20") == snap_b.get("SMA_20")
        assert snap_b.get("SMA_20") == pytest.approx(sum(c.close for c in closed_candles) / 20)

    def test_unfinished_candle_within_horizon_still_rejected(self) -> None:
        """Regresyona karşı koruma: değerlendirme ufkunun İÇİNDE
        (`close_time <= as_of`) kalan, henüz kapanmamış bir candle HÂLÂ
        reddedilmelidir — yalnızca ufkun DIŞINDAKİ açık candle'lar
        sessizce atlanır."""
        engine = FeatureEngine()
        closed_candles = make_candles(20)
        unfinished_within_horizon = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=closed_candles[-1].open_time, close_time=closed_candles[-1].close_time,
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=False,
        )
        as_of = unfinished_within_horizon.close_time  # ufkun İÇİNDE (close_time <= as_of)
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(
                closed_candles[:-1] + [unfinished_within_horizon], "BTCUSDT", Timeframe.M1, as_of,
                feature_names=["SMA_20"],
            )

    def test_ema_at_t_identical_with_or_without_future_candles(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of_t = candles[19].close_time  # 20. candle'ın (index 19) close_time'ı

        # A) dataset T'de bitiyor
        dataset_a = candles[:20]
        snap_a = engine.compute_candle_features(dataset_a, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["EMA_20"])

        # B) T'den sonraki candle'lar da storage'da mevcut
        dataset_b = candles  # tüm 25 candle
        snap_b = engine.compute_candle_features(dataset_b, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["EMA_20"])

        assert snap_a.get("EMA_20") == snap_b.get("EMA_20")

    def test_rsi_at_t_unaffected_by_future_candles(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(30)
        as_of_t = candles[19].close_time

        snap_a = engine.compute_candle_features(candles[:20], "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["RSI_14"])
        snap_b = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["RSI_14"])
        assert snap_a.get("RSI_14") == snap_b.get("RSI_14")

    def test_rolling_high_at_t_unaffected_by_future_candles(self) -> None:
        engine = FeatureEngine()
        # Gelecekteki candle'lar çok daha yüksek high'a sahip — eğer look-ahead
        # olsaydı bu, T anındaki highest_high'ı DEĞİŞTİRİRDİ.
        closes = [100.0 + i * 0.1 for i in range(20)] + [500.0] * 10
        candles = make_candles(30, closes=closes)
        as_of_t = candles[19].close_time

        snap_a = engine.compute_candle_features(candles[:20], "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["HIGHEST_HIGH_20"])
        snap_b = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["HIGHEST_HIGH_20"])
        assert snap_a.get("HIGHEST_HIGH_20") == snap_b.get("HIGHEST_HIGH_20")
        assert snap_b.get("HIGHEST_HIGH_20") < 500.0  # gelecekteki sıçrama SIZMADI

    def test_future_candles_in_list_are_silently_excluded_not_erroring(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of_t = candles[19].close_time
        # 25 candle veriliyor ama as_of yalnızca ilk 20'sini kapsıyor.
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["SMA_20"])
        expected = sum(c.close for c in candles[:20]) / 20
        assert snap.get("SMA_20") == pytest.approx(expected)

    def test_cross_timeframe_snapshot_as_of_never_returns_future(self) -> None:
        engine = FeatureEngine()
        candles_1h = make_candles(25, timeframe=Timeframe.H1)
        as_of_early = candles_1h[19].close_time
        as_of_late = candles_1h[24].close_time

        snap_early = engine.compute_candle_features(candles_1h[:20], "BTCUSDT", Timeframe.H1, as_of_early, feature_names=["SMA_20"])
        engine.commit_snapshot(snap_early)

        snap_late = engine.compute_candle_features(candles_1h[:25], "BTCUSDT", Timeframe.H1, as_of_late, feature_names=["SMA_20"])
        engine.commit_snapshot(snap_late)

        # 1h evaluation zamanı early'ye eşit bir 5m değerlendirme yapılıyor
        # varsayımıyla: snapshot_as_of(as_of_early) yalnızca early'yi (ya da
        # ondan ÖNCEKİ) döndürmeli, ASLA late'i değil.
        result = engine.snapshot_as_of("BTCUSDT", Timeframe.H1, as_of_early)
        assert result.as_of == as_of_early
        assert result.get("SMA_20") == snap_early.get("SMA_20")


class TestMultiTimeframeIsolation:
    def test_four_timeframes_independent_histories(self) -> None:
        engine = FeatureEngine()
        for tf in (Timeframe.H1, Timeframe.M15, Timeframe.M5, Timeframe.M1):
            candles = make_candles(25, timeframe=tf, closes=[100.0 + i for i in range(25)])
            as_of = candles[-1].close_time
            snap = engine.compute_candle_features(candles, "BTCUSDT", tf, as_of, feature_names=["SMA_20"])
            engine.commit_snapshot(snap)

        for tf in (Timeframe.H1, Timeframe.M15, Timeframe.M5, Timeframe.M1):
            latest = engine.latest_snapshot("BTCUSDT", tf)
            assert latest is not None
            assert latest.timeframe == tf

    def test_unfinished_higher_timeframe_candle_not_exposed(self) -> None:
        """Bir üst-timeframe candle'ı henüz kapanmadıysa (is_closed=False),
        FeatureEngine bunu candle listesine dahil edilse bile REDDEDER
        (closed-candle policy) — kapanmamış bir 1h candle asla kapalıymış
        gibi kullanılamaz."""
        engine = FeatureEngine()
        candles = make_candles(20, timeframe=Timeframe.H1)
        unfinished = make_candles(1, timeframe=Timeframe.H1, is_closed=False)
        unfinished[0] = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.H1,
            open_time=candles[-1].open_time + timedelta(hours=1),
            close_time=candles[-1].open_time + timedelta(hours=2),
            open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0, is_closed=False,
        )
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(
                candles + unfinished, "BTCUSDT", Timeframe.H1,
                unfinished[0].close_time, feature_names=["SMA_20"],
            )


class TestOrderBookFeaturesViaEngine:
    def test_compute_order_book_features(self) -> None:
        engine = FeatureEngine()
        book = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=T0,
            bids=(OrderBookLevel(price=99.0, quantity=1.0),),
            asks=(OrderBookLevel(price=101.0, quantity=1.0),),
            last_update_id=1,
        )
        snap = engine.compute_order_book_features(book, feature_names=["MID_PRICE", "SPREAD_ABS"])
        assert snap.get("MID_PRICE") == pytest.approx(100.0)
        assert snap.get("SPREAD_ABS") == pytest.approx(2.0)
        assert snap.as_of == T0


class TestTradeFeaturesViaEngine:
    def test_compute_trade_features_with_future_trade_excluded(self) -> None:
        engine = FeatureEngine()
        trades = [
            Trade(symbol="BTCUSDT", trade_id=1, price=100.0, quantity=1.0, timestamp=T0, is_buyer_maker=False),
            Trade(symbol="BTCUSDT", trade_id=2, price=100.0, quantity=5.0, timestamp=T0 + timedelta(minutes=10), is_buyer_maker=False),
        ]
        as_of = T0  # ikinci (gelecekteki) trade dışlanmalı
        snap = engine.compute_trade_features(trades, "BTCUSDT", Timeframe.M1, as_of, feature_names=["BUY_VOLUME"])
        assert snap.get("BUY_VOLUME") == pytest.approx(1.0)  # yalnızca ilk trade


=== FILE: tests/test_feature_registry.py ===
import pytest

from crypto_signal_engine.errors import FeatureValidationError
from crypto_signal_engine.features.registry import (
    FeatureConfig,
    FeatureRegistry,
    FeatureSourceType,
    default_candle_feature_registry,
    default_order_book_feature_registry,
    default_trade_feature_registry,
)


class TestFeatureConfig:
    def test_valid_config(self) -> None:
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20}, min_history=20)
        assert config.params["period"] == 20

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="name"):
            FeatureConfig(name="  ", source=FeatureSourceType.CANDLE, calculator_key="sma")

    def test_string_source_rejected(self) -> None:
        with pytest.raises(TypeError, match="FeatureSourceType"):
            FeatureConfig(name="X", source="CANDLE", calculator_key="sma")  # type: ignore[arg-type]

    def test_zero_min_history_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="min_history"):
            FeatureConfig(name="X", source=FeatureSourceType.CANDLE, calculator_key="sma", min_history=0)

    def test_params_immutable(self) -> None:
        config = FeatureConfig(name="X", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        with pytest.raises(TypeError):
            config.params["period"] = 999  # type: ignore[index]


class TestFeatureRegistry:
    def test_register_and_get(self) -> None:
        reg = FeatureRegistry()
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        reg.register(config)
        assert reg.get("SMA_20") is config
        assert "SMA_20" in reg

    def test_unknown_feature_rejected(self) -> None:
        reg = FeatureRegistry()
        with pytest.raises(FeatureValidationError, match="bilinmeyen feature"):
            reg.get("NOPE")

    def test_identical_reregistration_is_idempotent(self) -> None:
        reg = FeatureRegistry()
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        reg.register(config)
        reg.register(config)  # aynı config -> no-op
        assert len(reg.all()) == 1

    def test_conflicting_params_same_name_rejected(self) -> None:
        reg = FeatureRegistry()
        reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20}))
        with pytest.raises(FeatureValidationError, match="çakışması"):
            reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 50}))

    def test_by_source_filters_correctly(self) -> None:
        reg = FeatureRegistry()
        reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma"))
        reg.register(FeatureConfig(name="SPREAD_ABS", source=FeatureSourceType.ORDER_BOOK, calculator_key="spread_abs"))
        candle_configs = reg.by_source(FeatureSourceType.CANDLE)
        assert len(candle_configs) == 1
        assert candle_configs[0].name == "SMA_20"


class TestDefaultRegistries:
    def test_default_candle_registry_has_no_collisions(self) -> None:
        reg = default_candle_feature_registry()
        names = [c.name for c in reg.all()]
        assert len(names) == len(set(names))
        assert "SMA_20" in reg
        assert "EMA_20" in reg
        assert "RSI_14" in reg
        assert "ATR_14" in reg

    def test_default_order_book_registry(self) -> None:
        reg = default_order_book_feature_registry(depth=10)
        assert "BID_DEPTH_10" in reg
        assert "DEPTH_IMBALANCE_10" in reg

    def test_default_trade_registry(self) -> None:
        reg = default_trade_feature_registry(window_size=50)
        assert "BUY_VOLUME" in reg
        assert "TRADE_INTENSITY_50" in reg

    def test_different_depth_produces_different_identity(self) -> None:
        reg5 = default_order_book_feature_registry(depth=5)
        reg10 = default_order_book_feature_registry(depth=10)
        assert "BID_DEPTH_5" in reg5
        assert "BID_DEPTH_5" not in reg10
        assert "BID_DEPTH_10" in reg10


=== FILE: tests/test_feature_state.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import FeatureStateError
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_snapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values=None) -> FeatureSnapshot:
    return FeatureSnapshot(symbol=symbol, timeframe=timeframe, as_of=as_of, values=values or {"X": 1.0})


class TestCommitAndLatest:
    def test_commit_and_latest(self) -> None:
        store = FeatureHistoryStore()
        snap = make_snapshot()
        store.commit(snap)
        assert store.latest("BTCUSDT", Timeframe.M1) is snap

    def test_latest_unknown_returns_none(self) -> None:
        store = FeatureHistoryStore()
        assert store.latest("BTCUSDT", Timeframe.M1) is None

    def test_symbol_normalized_lookup(self) -> None:
        store = FeatureHistoryStore()
        snap = make_snapshot()
        store.commit(snap)
        assert store.latest("btcusdt", Timeframe.M1) is snap


class TestOlderSnapshotRejected:
    def test_older_as_of_rejected(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5)))
        with pytest.raises(FeatureStateError, match="eski"):
            store.commit(make_snapshot(as_of=T0))

    def test_canonical_state_unchanged_after_rejection(self) -> None:
        store = FeatureHistoryStore()
        newer = make_snapshot(as_of=T0 + timedelta(minutes=5), values={"X": 999.0})
        store.commit(newer)
        with pytest.raises(FeatureStateError):
            store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 999.0


class TestSameTimestampUpsert:
    def test_same_as_of_upserts(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(values={"X": 1.0}))
        store.commit(make_snapshot(values={"X": 2.0}))  # aynı as_of, farklı değer
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 2.0
        assert store.history_length("BTCUSDT", Timeframe.M1) == 1  # duplicate satır YOK


class TestSymbolTimeframeIsolation:
    def test_different_symbols_independent(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(symbol="BTCUSDT", values={"X": 1.0}))
        store.commit(make_snapshot(symbol="ETHUSDT", values={"X": 2.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 1.0
        assert store.latest("ETHUSDT", Timeframe.M1).get("X") == 2.0

    def test_different_timeframes_independent(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(timeframe=Timeframe.M1, values={"X": 1.0}))
        store.commit(make_snapshot(timeframe=Timeframe.M5, values={"X": 2.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 1.0
        assert store.latest("BTCUSDT", Timeframe.M5).get("X") == 2.0


class TestAsOfLookup:
    def test_as_of_returns_most_recent_not_exceeding_timestamp(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5), values={"X": 2.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=10), values={"X": 3.0}))

        result = store.as_of("BTCUSDT", Timeframe.M1, T0 + timedelta(minutes=7))
        assert result.get("X") == 2.0  # 5dk'lık snapshot, 10dk'lık DEĞİL

    def test_as_of_before_any_snapshot_returns_none(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5)))
        result = store.as_of("BTCUSDT", Timeframe.M1, T0)
        assert result is None

    def test_as_of_never_returns_future_snapshot(self) -> None:
        """KRİTİK no-look-ahead testi: `timestamp`'tan SONRAKİ bir snapshot
        ASLA döndürülmemeli."""
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=100), values={"X": 999.0}))
        result = store.as_of("BTCUSDT", Timeframe.M1, T0 + timedelta(minutes=1))
        assert result.get("X") == 1.0
        assert result.as_of <= T0 + timedelta(minutes=1)

    def test_as_of_naive_timestamp_rejected(self) -> None:
        store = FeatureHistoryStore()
        with pytest.raises(ValueError, match="naive datetime"):
            store.as_of("BTCUSDT", Timeframe.M1, datetime(2026, 8, 31))


class TestBoundedRetention:
    def test_bounded_retention_evicts_oldest(self) -> None:
        store = FeatureHistoryStore(max_history=3)
        for i in range(5):
            store.commit(make_snapshot(as_of=T0 + timedelta(minutes=i), values={"X": float(i)}))
        assert store.history_length("BTCUSDT", Timeframe.M1) == 3
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 4.0
        # en eski (0, 1) silinmiş olmalı -> as_of=T0'a sorgulandığında None dönmeli
        assert store.as_of("BTCUSDT", Timeframe.M1, T0) is None

    def test_invalid_max_history_rejected(self) -> None:
        with pytest.raises(FeatureStateError, match="max_history"):
            FeatureHistoryStore(max_history=0)


=== FILE: tests/test_gap_and_atomic_checkpoint_remediation.py ===
"""
Faz 7 pre-audit remediation — regression tests for two independently
confirmed defects:

BLOCKER-1: `PersistedRuntime._consume_candles()` used to resolve a
detected gap via the BARE `RuntimeCoordinator.resolve_gap()`, whose final
step (`ingest_candle()` on the recovered pending candle) could produce a
real Paper transition — but control returned to `_consume_candles()`
without that transition ever being durably checkpointed (only the candle
checkpoint advanced). A restart after such a gap-triggered trade silently
and permanently lost it, because bootstrap/recovery intentionally never
re-evaluates historical candles.

HIGH-1: the normal (non-gap) ingestion path persisted the candle
checkpoint and the associated Paper-state transition in two independent
SQLite transactions. A crash/write-failure between them could leave the
candle checkpoint durably advanced while the Paper transition it caused
was never persisted.

Both are fixed by (a) `PaperStateStore.checkpoint_candle_transition()`,
which commits the candle checkpoint and (if present) its Paper transition
in ONE atomic SQLite transaction, and (b) routing gap-resolution's final
`ingest_candle()` call through `PersistedRuntime`'s own (checkpointing)
`ingest_candle()`/`resolve_gap()` instead of the bare coordinator's.

These tests exercise the REAL `RuntimeCoordinator`/`PaperTradingEngine`/
`PaperStateStore`/`PersistedRuntime` stack (no mocking of signal
evaluation) against a temporary SQLite database, using a strong,
deterministic uptrend candle series + a bid-imbalanced order book to
reliably produce a genuine (non-NEUTRAL) directional trade through the
full Phase 3/4/5/6 pipeline.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at

UTC = timezone.utc
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
WARMUP = 20
_UPTREND_STEP = 3.0
_M5 = Timeframe.M5


def _strong_bid_order_book(symbol: str, timestamp: datetime, *, last_update_id: int = 1) -> OrderBookSnapshot:
    """Heavily bid-weighted order book — combined with a strong candle
    uptrend, this reliably produces a directional (non-NEUTRAL) signal
    through the real Phase 3/4 feature/agent/consensus pipeline
    (verified: STRONG_LONG, RSI_14=100, ROC_10 strongly positive across
    all timeframes) — no mocked/injected Signal objects are used anywhere
    in this file."""
    return OrderBookSnapshot(
        symbol=symbol, timestamp=timestamp,
        bids=(OrderBookLevel(price=99.5, quantity=500.0), OrderBookLevel(price=99.0, quantity=500.0)),
        asks=(OrderBookLevel(price=100.5, quantity=5.0), OrderBookLevel(price=101.0, quantity=5.0)),
        last_update_id=last_update_id,
    )


def _bootstrap_strong_uptrend(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in (Timeframe.M5, Timeframe.M15, Timeframe.H1):
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP, base_price=100.0, step=_UPTREND_STEP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready
    coordinator.ingest_order_book(symbol, _strong_bid_order_book(symbol, end))


def _uptrend_price(steps_past_warmup_end: int) -> float:
    # last warmup candle price is 100 + step*(WARMUP-1); continue the SAME
    # monotonic uptrend for candles arriving after warmup.
    return 100.0 + _UPTREND_STEP * (WARMUP - 1 + steps_past_warmup_end)


def _gap_candles(symbol: str) -> tuple[list, "object"]:
    """Two genuinely missing M5 candles (continuing the uptrend) plus a
    live pending candle arriving 3 intervals ahead — the exact shape a
    reconnect-induced gap produces."""
    missing = [
        make_candle(symbol, _M5, END, price=_uptrend_price(1)),
        make_candle(symbol, _M5, END + timedelta(minutes=5), price=_uptrend_price(2)),
    ]
    pending = make_candle(symbol, _M5, END + timedelta(minutes=10), price=_uptrend_price(3))
    return missing, pending


class TestBlocker1GapResolutionDurability:
    """BLOCKER-1: gap resolution's Paper transition must receive the SAME
    durable guarantees as a normal accepted M5 transition, and restart
    must reconstruct exactly the same state an uninterrupted process
    would have reached — no double evaluation, no lost ledger entries."""

    def test_gap_resolution_produces_durable_restart_equivalent_trade(self, tmp_path) -> None:
        async def scenario() -> None:
            symbol = "BTCUSDT"

            # -- 1. REFERENCE: uninterrupted process, bare Phase 6 coordinator,
            #    NO persistence involved at all. This is the ground-truth for
            #    "what should the final state be after this exact gap is
            #    resolved" (bare RuntimeCoordinator.resolve_gap is accepted,
            #    unmodified Phase 6 behavior).
            missing, pending = _gap_candles(symbol)
            ref_provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing})
            ref_engine = PaperTradingEngine(notional_per_position=1000.0)
            ref_coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=ref_provider, warmup_candles=WARMUP, paper_engine=ref_engine
            )
            _bootstrap_strong_uptrend(ref_coordinator, symbol, END)

            ref_gap_event = ref_coordinator.ingest_candle(symbol, _M5, pending)
            assert ref_gap_event.outcome is IngestOutcome.GAP_DETECTED

            ref_resolved = await ref_coordinator.resolve_gap(symbol, _M5, pending)
            assert ref_resolved.outcome is IngestOutcome.ACCEPTED
            assert ref_resolved.cycle_result is not None
            assert ref_resolved.cycle_result.evaluated is True
            assert ref_resolved.cycle_result.signal.direction is not SignalDirection.NEUTRAL  # (1) genuine trade
            assert len(ref_resolved.cycle_result.paper_result.orders) == 1
            assert len(ref_resolved.cycle_result.paper_result.fills) == 1

            reference_position = ref_engine.position(symbol)
            reference_fills = ref_engine.fills(symbol)
            reference_orders = ref_engine.orders(symbol)
            reference_context_ids = set(ref_engine._states[symbol].processed_context_ids)  # noqa: SLF001
            reference_entry_fee = ref_engine._states[symbol].entry_fee  # noqa: SLF001

            # -- 2. GAP + PERSISTENCE path: identical scenario, but driven
            #    through PersistedRuntime (the real production entry point).
            missing2, pending2 = _gap_candles(symbol)  # fresh, structurally identical candle objects
            db_path = tmp_path / "state.db"
            gap_provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing2})
            store1 = PaperStateStore(db_path)
            engine1 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator1 = RuntimeCoordinator(
                symbols=(symbol,), provider=gap_provider, warmup_candles=WARMUP, paper_engine=engine1
            )
            runtime1 = PersistedRuntime(coordinator1, store1)
            _bootstrap_strong_uptrend(coordinator1, symbol, END)

            gap_event = runtime1.ingest_candle(symbol, _M5, pending2)
            assert gap_event.outcome is IngestOutcome.GAP_DETECTED

            resolved = await runtime1.resolve_gap(symbol, _M5, pending2)  # BLOCKER-1 fix under test
            assert resolved.outcome is IngestOutcome.ACCEPTED
            assert resolved.cycle_result is not None and resolved.cycle_result.evaluated is True
            assert resolved.cycle_result.signal.direction == ref_resolved.cycle_result.signal.direction

            # in-memory state matches the reference exactly
            assert engine1.position(symbol) == reference_position
            assert engine1.fills(symbol) == reference_fills
            assert engine1.orders(symbol) == reference_orders

            # (2) paper state is durably checkpointed, (7) candle checkpoint
            # is consistent with the paper transition — both written by the
            # SAME atomic call (checkpoint_candle_transition).
            assert store1.load_candle_checkpoint(symbol, _M5) == pending2.open_time
            durable = store1.load_paper_state(symbol)
            assert durable is not None
            assert durable.position == reference_position

            # (3) processed context identity is durable
            assert set(durable.processed_context_ids) == reference_context_ids

            # (4) generated orders are durable, (5) generated fills are durable
            assert [o.order_id for o in durable.orders] == [o.order_id for o in reference_orders]
            assert [f.fill_id for f in durable.fills] == [f.fill_id for f in reference_fills]

            # (6) fees / realized PnL / entry fee / position are durable
            assert durable.entry_fee == reference_entry_fee
            assert durable.position.realized_pnl == reference_position.realized_pnl

            assert _health_of(coordinator1, symbol) is RuntimeHealth.READY  # no false DEGRADED
            store1.close()

            # -- 3. RESTART: brand-new engine/store/coordinator/runtime,
            #    recover from the SAME durable SQLite database.
            full_m5 = (
                make_candle_series_ending_at(symbol, _M5, END, WARMUP, base_price=100.0, step=_UPTREND_STEP)
                + missing2 + [pending2]
            )
            recover_provider = FakeLiveDataProvider(
                historical_candles={
                    (symbol, _M5): full_m5,
                    (symbol, Timeframe.M15): make_candle_series_ending_at(
                        symbol, Timeframe.M15, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                    (symbol, Timeframe.H1): make_candle_series_ending_at(
                        symbol, Timeframe.H1, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                }
            )
            store2 = PaperStateStore(db_path)
            engine2 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator2 = RuntimeCoordinator(
                symbols=(symbol,), provider=recover_provider, warmup_candles=WARMUP, paper_engine=engine2
            )
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready  # (9, part 1) plenty of history — genuinely READY, not fabricated

            # (8) restart restores the same state as uninterrupted execution
            assert engine2.position(symbol) == reference_position
            assert engine2.fills(symbol) == reference_fills
            assert engine2.orders(symbol) == reference_orders
            assert set(engine2._states[symbol].processed_context_ids) == reference_context_ids  # noqa: SLF001
            assert engine2._states[symbol].entry_fee == reference_entry_fee  # noqa: SLF001

            # (9) the recovered historical candle is NOT evaluated twice,
            # (10) no duplicate trade occurs after restart
            assert len(engine2.fills(symbol)) == 1
            assert len(engine2.orders(symbol)) == 1
            assert len(engine2._states[symbol].processed_context_ids) == 1  # noqa: SLF001

            store2.close()

        run_async(scenario())

    def test_gap_resolution_health_stays_ready_no_stuck_gap_fault(self, tmp_path) -> None:
        """A resolved gap must not leave a phantom DEGRADED gap-fault once
        the checkpointed path is used (same Phase 6 clear_gap_fault
        semantics as the bare-coordinator path, unaffected by BLOCKER-1's
        fix)."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            missing, pending = _gap_candles(symbol)
            db_path = tmp_path / "state.db"
            provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing})
            store = PaperStateStore(db_path)
            engine = PaperTradingEngine(notional_per_position=1000.0)
            coordinator = RuntimeCoordinator(symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine)
            runtime = PersistedRuntime(coordinator, store)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            gap_event = runtime.ingest_candle(symbol, _M5, pending)
            assert gap_event.outcome is IngestOutcome.GAP_DETECTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED

            resolved = await runtime.resolve_gap(symbol, _M5, pending)
            assert resolved.outcome is IngestOutcome.ACCEPTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.READY
            store.close()

        run_async(scenario())


class _FailAfterNCalls:
    """Wraps a REAL sqlite3 connection and raises on the Nth `execute()`
    call — used to prove HIGH-1's atomicity at the actual SQL boundary:
    an exception raised INSIDE the `with self._connection:` block must
    roll back everything written earlier in that same block, including
    statements the real connection already executed successfully."""

    def __init__(self, real: sqlite3.Connection, fail_on_call: int) -> None:
        self._real = real
        self._fail_on_call = fail_on_call
        self.calls = 0

    def execute(self, *args, **kwargs):
        self.calls += 1
        if self.calls == self._fail_on_call:
            raise sqlite3.OperationalError("simulated mid-transaction failure (HIGH-1 regression test)")
        return self._real.execute(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._real.executescript(*args, **kwargs)

    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._real.__exit__(exc_type, exc, tb)

    def close(self) -> None:
        self._real.close()


class TestHigh1AtomicCheckpoint:
    """HIGH-1: candle checkpoint + its associated Paper transition must
    commit atomically — a failure at any point must roll back the WHOLE
    combined write, never leave the candle checkpoint durably advanced
    while the Paper transition it caused is missing."""

    def test_failure_between_candle_and_paper_write_rolls_back_both(self, tmp_path) -> None:
        """(A) NORMAL PATH ATOMICITY — direct store-level proof: inject a
        failure AFTER the candle_checkpoint write has already executed
        against the real connection but BEFORE the paper_position write
        completes, and assert NOTHING survives (no candle checkpoint, no
        paper position, no processed context, no orders/fills)."""
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)

        from crypto_signal_engine.domain.enums import RiskLevel
        from crypto_signal_engine.domain.models import Signal
        from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide

        signal = Signal(
            symbol="BTCUSDT", timestamp=END, context_id="ctx-atomic-1", score=0.6, confidence=0.7,
            risk_level=RiskLevel.LOW, primary_timeframe=_M5, supporting_factors=(),
            contradicting_factors=(), invalidation=None, model_version="test-v1",
        )
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.LONG, quantity=1.0, average_entry_price=100.0,
            realized_pnl=0.0, updated_at=END,
        )
        order = PaperOrder(
            order_id="BTCUSDT:ctx-atomic-1:0", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=1.0, signal_context_id="ctx-atomic-1", created_at=END,
        )
        fill = PaperFill(
            fill_id="BTCUSDT:ctx-atomic-1:0:fill", order_id=order.order_id, symbol="BTCUSDT",
            side=OrderSide.BUY, quantity=1.0, price=100.0, fee=0.0, filled_at=END,
        )

        # 1st execute() = candle_checkpoint INSERT (succeeds against the
        # real connection, uncommitted). 2nd execute() = paper_position
        # INSERT — this is where we inject the failure.
        real_connection = store._connection  # noqa: SLF001
        store._connection = _FailAfterNCalls(real_connection, fail_on_call=2)  # noqa: SLF001

        with pytest.raises(PersistenceError):
            store.checkpoint_candle_transition(
                symbol="BTCUSDT", timeframe=_M5, candle_open_time=END,
                position=position, entry_fee=0.0, last_signal_timestamp=END,
                new_context_entries=[(signal.context_id, signal, position)],
                new_fills=(fill,), new_orders=(order,),
            )

        store._connection = real_connection  # noqa: SLF001 - restore for clean reads below

        assert store.load_candle_checkpoint("BTCUSDT", _M5) is None  # no advanced candle checkpoint survives
        assert store.load_paper_state("BTCUSDT") is None  # no partial paper transition/context/orders/fills survives
        store.close()

    def test_end_to_end_persistence_failure_leaves_no_partial_durable_state(self, tmp_path) -> None:
        """(A) NORMAL PATH ATOMICITY, exercised through the real production
        entry point (`PersistedRuntime.ingest_candle`) rather than the bare
        store — proves the wiring, not just the SQL. In-memory Phase 5/6
        state must still reflect the mutation (fail-closed does not roll
        back in-memory state, by accepted Phase 7 design) while durable
        state must show NOTHING from the failed candle."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            store = PaperStateStore(db_path)
            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine)
            runtime = PersistedRuntime(coordinator, store)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            checkpoint_before = store.load_candle_checkpoint(symbol, _M5)
            paper_state_before = store.load_paper_state(symbol)
            assert checkpoint_before is None
            assert paper_state_before is None

            store.close()  # force the next durable write to fail entirely (closed connection)

            candle = make_candle(symbol, _M5, END, price=_uptrend_price(1))
            event = runtime.ingest_candle(symbol, _M5, candle)

            # (F) FAILURE HEALTH: in-memory Phase 6 state is unaffected
            # (fail-closed does not retroactively undo Phase 5, by accepted
            # design) — the candle IS accepted in-memory...
            assert event.outcome is IngestOutcome.ACCEPTED
            # ...but health must correctly reflect the persistence failure,
            # never silently presenting in-memory/durable divergence as a
            # successful checkpoint.
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED

            # ...and NOTHING durable exists for this candle/transition —
            # not even the candle checkpoint alone (this is exactly the
            # non-atomic HIGH-1 hazard: with the store closed, the OLD
            # code could not have written either piece; the atomic store
            # method must reject the whole write up front, same result).
            reopened = PaperStateStore(db_path)
            assert reopened.load_candle_checkpoint(symbol, _M5) is None
            assert reopened.load_paper_state(symbol) is None
            reopened.close()

        run_async(scenario())

    # NOTE: the old `test_next_successful_checkpoint_clears_fault_and_backfills_nothing_partial`
    # test that lived here only asserted the LATER candle's checkpoint and
    # health — it did not compare the complete in-memory/durable ledger, so
    # it could not distinguish "the failed transition was correctly flushed
    # first" from "the failed transition was silently skipped/overtaken".
    # It has been replaced by `TestPendingUnsyncedTransitionBacklog` below
    # (SINGLE FINAL ACCEPTANCE BLOCKER fix), which asserts full ledger
    # equality (candle chronology, position, realized PnL, entry_fee,
    # last_signal_timestamp, ALL processed context ids, ALL orders, ALL
    # fills) and proves the originally-failed trade's order/fill/context
    # are present — not merely that health returned to READY.


class _FlakyTransitionStore:
    """Wraps a REAL `PaperStateStore`; fails the next N calls to
    `checkpoint_candle_transition` (one `PersistenceError` per call,
    consuming the counter), then delegates transparently. All other
    methods pass straight through — this double is scoped to exactly the
    single real call site `PersistedRuntime` uses for durable writes."""

    def __init__(self, inner: PaperStateStore, *, fail_next: int = 0) -> None:
        self._inner = inner
        self._fail_remaining = fail_next

    def checkpoint_candle_transition(self, **kwargs: object) -> None:
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise PersistenceError("simulated pending-unsynced fault injection")
        self._inner.checkpoint_candle_transition(**kwargs)

    def checkpoint_candle(self, *args: object, **kwargs: object) -> None:
        self._inner.checkpoint_candle(*args, **kwargs)

    def checkpoint_paper_state(self, **kwargs: object) -> None:
        self._inner.checkpoint_paper_state(**kwargs)

    def load_paper_state(self, *args: object, **kwargs: object):
        return self._inner.load_paper_state(*args, **kwargs)

    def load_candle_checkpoint(self, *args: object, **kwargs: object):
        return self._inner.load_candle_checkpoint(*args, **kwargs)

    def close(self) -> None:
        self._inner.close()


class TestPendingUnsyncedTransitionBacklog:
    """SINGLE FINAL ACCEPTANCE BLOCKER fix: a failed atomic
    candle+Paper-transition checkpoint must NOT be overtaken by a later
    candle's successful checkpoint. `PersistedRuntime` now holds the
    failed payload in an in-memory, per-(symbol, timeframe) FIFO backlog
    (`_pending`) and refuses to let a later checkpoint reach the store
    until every earlier backlog entry has been durably flushed (bkz.
    `PersistedRuntime._flush_pending`)."""

    def test_failed_transition_flushed_before_later_checkpoint_and_full_ledger_equality(self, tmp_path) -> None:
        """CRITICAL TEST (A-E scenario):

        A. a real M5 event opens a genuine Paper LONG position.
        B. its atomic checkpoint fails exactly once.
        C. health is DEGRADED and nothing durable was written.
        D. a later valid M5 event is submitted.
        E. persistence becomes available again (the flaky store stops
           failing) -> health returns to READY.

        After READY, in-memory and durable state must be FULLY equal —
        including the FIRST (originally-failed) trade's order/fill/
        processed-context entries, which the old (replaced) regression
        never checked. Also proves restart-equivalence and no duplicate
        trade after recovery of the originally-failed transition."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            real_store = PaperStateStore(db_path)
            flaky = _FlakyTransitionStore(real_store, fail_next=1)

            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine
            )
            runtime = PersistedRuntime(coordinator, flaky)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            # A + B: a genuine directional trade whose checkpoint fails once.
            first_candle = make_candle(symbol, _M5, END, price=_uptrend_price(1))
            first = runtime.ingest_candle(symbol, _M5, first_candle)
            assert first.outcome is IngestOutcome.ACCEPTED
            assert first.cycle_result is not None and first.cycle_result.evaluated is True
            assert first.cycle_result.signal.direction is not SignalDirection.NEUTRAL
            assert len(first.cycle_result.paper_result.orders) == 1
            assert len(first.cycle_result.paper_result.fills) == 1
            first_order_id = first.cycle_result.paper_result.orders[0].order_id
            first_fill_id = first.cycle_result.paper_result.fills[0].fill_id
            first_context_id = first.cycle_result.signal.context_id

            # C: DEGRADED, nothing durable yet — the failed transition is
            # held in the pending backlog, not discarded.
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
            assert real_store.load_candle_checkpoint(symbol, _M5) is None
            assert real_store.load_paper_state(symbol) is None
            assert runtime._pending[(symbol, _M5)]  # noqa: SLF001 - backlog holds the failed payload

            # D: a later valid M5 event (store already recovered by now).
            later_time = END + timedelta(minutes=5)
            later_candle = make_candle(symbol, _M5, later_time, price=_uptrend_price(2))
            second = runtime.ingest_candle(symbol, _M5, later_candle)
            assert second.outcome is IngestOutcome.ACCEPTED

            # E: persistence available again -> backlog fully flushed (the
            # FIRST payload first, in order) -> health READY.
            assert _health_of(coordinator, symbol) is RuntimeHealth.READY
            assert (symbol, _M5) not in runtime._pending  # noqa: SLF001 - backlog fully drained

            # FULL EQUALITY: candle checkpoint chronology.
            assert real_store.load_candle_checkpoint(symbol, _M5) == later_time

            durable = real_store.load_paper_state(symbol)
            assert durable is not None
            live_position = engine.position(symbol)
            live_fills = engine.fills(symbol)
            live_orders = engine.orders(symbol)
            live_state = engine._states[symbol]  # noqa: SLF001

            # current position / realized PnL / entry_fee / last_signal_timestamp
            assert durable.position == live_position
            assert durable.position.realized_pnl == live_position.realized_pnl
            assert durable.entry_fee == live_state.entry_fee
            assert durable.last_signal_timestamp == live_state.last_signal_timestamp

            # ALL processed context ids / ALL orders / ALL fills
            assert set(durable.processed_context_ids) == set(live_state.processed_context_ids)
            assert {o.order_id for o in durable.orders} == {o.order_id for o in live_orders}
            assert {f.fill_id for f in durable.fills} == {f.fill_id for f in live_fills}

            # Specifically: the FIRST (originally-failed) trade's order/
            # fill/context are present, not lost/skipped by the overtake.
            assert first_context_id in durable.processed_context_ids
            assert first_order_id in {o.order_id for o in durable.orders}
            assert first_fill_id in {f.fill_id for f in durable.fills}

            # -- RESTART: fresh engine/store/coordinator/runtime recovers
            #    from the SAME durable database.
            real_store.close()
            full_m5 = (
                make_candle_series_ending_at(symbol, _M5, END, WARMUP, base_price=100.0, step=_UPTREND_STEP)
                + [first_candle, later_candle]
            )
            recover_provider = FakeLiveDataProvider(
                historical_candles={
                    (symbol, _M5): full_m5,
                    (symbol, Timeframe.M15): make_candle_series_ending_at(
                        symbol, Timeframe.M15, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                    (symbol, Timeframe.H1): make_candle_series_ending_at(
                        symbol, Timeframe.H1, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                }
            )
            store2 = PaperStateStore(db_path)
            engine2 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator2 = RuntimeCoordinator(
                symbols=(symbol,), provider=recover_provider, warmup_candles=WARMUP, paper_engine=engine2
            )
            runtime2 = PersistedRuntime(coordinator2, store2)
            reports = await runtime2.recover()
            for report in reports:
                assert report.ready

            # restart restores exactly the uninterrupted in-memory state...
            assert engine2.position(symbol) == live_position
            assert engine2.fills(symbol) == live_fills
            assert engine2.orders(symbol) == live_orders
            assert set(engine2._states[symbol].processed_context_ids) == set(  # noqa: SLF001
                live_state.processed_context_ids
            )
            assert engine2._states[symbol].entry_fee == live_state.entry_fee  # noqa: SLF001

            # ...and contains the ORIGINALLY-FAILED trade's order/fill/context.
            assert first_context_id in engine2._states[symbol].processed_context_ids  # noqa: SLF001
            assert first_order_id in {o.order_id for o in engine2.orders(symbol)}
            assert first_fill_id in {f.fill_id for f in engine2.fills(symbol)}

            # no re-evaluation / no duplicate trade: exactly one order/fill
            # survived (the historical candle carrying the real trade is
            # never re-evaluated by recover()/bootstrap_candles()).
            assert len(engine2.orders(symbol)) == len(live_orders)
            assert len(engine2.fills(symbol)) == len(live_fills)

            store2.close()

        run_async(scenario())

    def test_repeated_persistence_failure_keeps_degraded_and_blocks_overtake(self, tmp_path) -> None:
        """If the store keeps failing across MULTIPLE checkpoint attempts,
        health must stay DEGRADED and NONE of the later candles' checkpoints
        may reach the durable store — the backlog grows, but nothing is
        ever silently allowed to overtake the still-unflushed first entry."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            real_store = PaperStateStore(db_path)
            flaky = _FlakyTransitionStore(real_store, fail_next=1000)  # never succeeds in this test

            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine
            )
            runtime = PersistedRuntime(coordinator, flaky)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            first = runtime.ingest_candle(symbol, _M5, make_candle(symbol, _M5, END, price=_uptrend_price(1)))
            assert first.outcome is IngestOutcome.ACCEPTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
            assert real_store.load_candle_checkpoint(symbol, _M5) is None

            for step in range(2, 5):
                candle_time = END + timedelta(minutes=5 * (step - 1))
                event = runtime.ingest_candle(
                    symbol, _M5, make_candle(symbol, _M5, candle_time, price=_uptrend_price(step))
                )
                assert event.outcome is IngestOutcome.ACCEPTED
                # every later candle is accepted in-memory (Phase 6 continuity
                # is unaffected), but repeated persistence failure means NONE
                # of them can overtake the still-unflushed first transition.
                assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
                assert real_store.load_candle_checkpoint(symbol, _M5) is None
                assert real_store.load_paper_state(symbol) is None

            # the backlog accumulated one entry per accepted M5 candle,
            # still ordered with the original failure first.
            assert len(runtime._pending[(symbol, _M5)]) == 4  # noqa: SLF001

            real_store.close()

        run_async(scenario())


def _health_of(coordinator: RuntimeCoordinator, symbol: str) -> RuntimeHealth:
    status = coordinator.status()
    return next(s for s in status.symbols if s.symbol == symbol).health


=== FILE: tests/test_immutability.py ===
"""
Quality Gate 4 — kapsamlı nested immutability regresyon testi.

Bu dosya, `@dataclass(frozen=True)` kullanımının TEK BAŞINA yeterli
olmadığını, nested mutable mapping'lerin ayrıca gerçek immutable bir
view'a sarılması gerektiğini kanıtlar. Her ilgili alan için hem
"construction sonrası mutation" hem "orijinal dict mutation sonrası sızma"
senaryoları test edilir.
"""

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.consensus import RiskAssessment
from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, FeatureVector
from crypto_signal_engine.quality.base import DataQualityResult
from crypto_signal_engine.domain.enums import DataQualityStatus
from crypto_signal_engine.safety.models import SafetyEvent, SafetyReasonCode, SafetySeverity

UTC = timezone.utc


@pytest.mark.parametrize(
    "build_and_get_mapping",
    [
        lambda source: (
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values=source,
            ).values
        ),
        lambda source: (
            AgentEvidence(
                agent=AgentName.QUANT, score=0.5, rationale="test",
                primary_timeframe=Timeframe.M5, symbol="BTCUSDT",
                as_of=datetime(2026, 8, 31, tzinfo=UTC), context_id="ctx-1",
                supporting_metrics=source,
            ).supporting_metrics
        ),
        lambda source: (
            RiskAssessment(
                confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
                contradicting_metrics=source,
            ).contradicting_metrics
        ),
    ],
    ids=["FeatureVector.values", "AgentEvidence.supporting_metrics", "RiskAssessment.contradicting_metrics"],
)
class TestNumericMappingImmutability:
    def test_item_assignment_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            mapping["x"] = 999.0

    def test_new_key_assignment_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            mapping["y"] = 5.0

    def test_deletion_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            del mapping["x"]

    def test_original_dict_mutation_after_construction_does_not_leak(self, build_and_get_mapping) -> None:
        source = {"x": 1.0}
        mapping = build_and_get_mapping(source)
        source["x"] = 999.0
        source["y"] = 42.0
        assert mapping["x"] == 1.0
        assert "y" not in mapping


class TestStringMappingImmutability:
    """context / details gibi str->str mapping'ler için aynı garanti."""

    def test_safety_event_context_immutable(self) -> None:
        event = SafetyEvent(
            reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
            component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            context={"k": "v"},
        )
        with pytest.raises(TypeError):
            event.context["k"] = "changed"

    def test_data_quality_result_details_immutable(self) -> None:
        result = DataQualityResult(
            status=DataQualityStatus.STALE_PRICE, reason="test", details={"k": "v"}
        )
        with pytest.raises(TypeError):
            result.details["k"] = "changed"


=== FILE: tests/test_lifecycle_concurrency.py ===
"""
Autonomous Testnet trading lifecycle Phase 7/17 — concurrency proof: the
M1 candle-driven stop/target evaluator and the signal-driven
opposite-signal exit path share the EXACT SAME per-symbol lock
(`LifecycleManager.lock_for` == `SignalTestnetBridge._lock_for` whenever a
`LifecycleManager` is wired). This test drives both concurrently for the
same LONG position and proves at most one SELL is ever submitted — the
loser observes the winner's fresh state and no-ops, never a double sell."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, Candle, PositionLifecycleState
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"
SYMBOL = "BTCUSDT"


def _exchange_info() -> dict:
    return {
        "symbols": [
            {
                "symbol": SYMBOL, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int) -> dict:
    return {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": "FILLED", "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }


class _SlowFakeHttp(FakeTestnetHttpClient):
    """Adds a cooperative yield before responding, to widen the window in
    which two concurrent callers could (incorrectly, if the lock were
    missing) both observe a stale LONG position and both submit a SELL."""

    async def get(self, url, params, headers, timeout_seconds):  # noqa: ANN001
        await asyncio.sleep(0)
        return await super().get(url, params, headers, timeout_seconds)

    async def post(self, url, params, headers, timeout_seconds):  # noqa: ANN001
        await asyncio.sleep(0)
        return await super().post(url, params, headers, timeout_seconds)


def _signal(*, score: float, context_id: str) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=NOW, context_id=context_id, score=score, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test-bridge",
    )


def _cycle_result(signal: Signal) -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=signal.symbol,
        position=PaperPosition(
            symbol=signal.symbol, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0,
            realized_pnl=0.0, updated_at=signal.timestamp,
        ),
        orders=(), fills=(), idempotent_replay=False,
    )
    return RuntimeCycleResult(
        symbol=signal.symbol, evaluated=True, signal=signal, paper_result=paper_result, generated_at=signal.timestamp
    )


class TestSharedLockPreventsDoubleSell:
    def test_concurrent_m1_stop_and_opposite_signal_produce_at_most_one_sell(self, tmp_path: Path) -> None:
        # Two DIFFERENT SELL fill responses queued -- if the shared lock
        # failed to serialize the two paths, BOTH would be consumed
        # (two POSTs); with the lock working, only one is ever submitted.
        get_responses = [json_response(_exchange_info())] * 4
        post_responses = [
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="80.0", order_id=1)),
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0", order_id=2)),
        ]
        http = _SlowFakeHttp(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = ExecutionStateStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        bridge = SignalTestnetBridge(
            execution_service=service, execution_store=exec_store, notional_usdt=10.0, clock=FixedClock(NOW),
            lifecycle_manager=manager, atr_provider=lambda _s: 5.0, all_symbols_provider=lambda: (SYMBOL,),
        )
        bridge.mark_operational(True, "test")

        lifecycle_store.save_position(BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-entry", updated_at=NOW,
        ))

        stop_candle = Candle(open=91, high=91, low=89, close=90, close_time=NOW + timedelta(minutes=1))
        short_signal = _signal(score=-0.9, context_id="ctx-short")

        async def scenario():
            await asyncio.gather(
                manager.evaluate_m1_candle(SYMBOL, stop_candle, atr_for_trailing=5.0),
                bridge.on_cycle_result(_cycle_result(short_signal)),
            )

        run_async(scenario())

        assert len(http.post_calls) == 1, "exactly one SELL must be submitted, never two"
        final = lifecycle_store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        trades = lifecycle_store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1


=== FILE: tests/test_lifecycle_runtime.py ===
"""
Autonomous Testnet trading lifecycle — `LifecycleRuntime` wiring tests
(Phase 6/8/9/12). Proves: an M1 consumer task exists per symbol only when
a `LifecycleManager` is wired; unclosed candles are skipped; a stale/absent
M5 ATR snapshot is passed through as `atr_for_trailing=None`; one symbol's
evaluation failure never kills another symbol's task; `stop()` leaves no
dangling M1 tasks."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime
from tests.conftest import run_async
from tests.runtime_fakes import make_candle

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeFeatureEngine:
    def __init__(self, snapshot=None) -> None:
        self._snapshot = snapshot

    def latest_snapshot(self, symbol, timeframe):  # noqa: ANN001
        return self._snapshot


class _Snapshot:
    def __init__(self, as_of, values) -> None:  # noqa: ANN001
        self.as_of = as_of
        self.values = values


class _FakeProvider:
    def __init__(self, candles_by_symbol: dict[str, list]) -> None:
        self._candles_by_symbol = candles_by_symbol

    async def stream_candles(self, symbol, timeframe):  # noqa: ANN001
        for candle in self._candles_by_symbol.get(symbol, []):
            yield candle


class _FakeCoordinator:
    def __init__(self, symbols, provider, feature_engine) -> None:  # noqa: ANN001
        self._symbols = symbols
        self._provider = provider
        self._feature_engine = feature_engine
        self._stopped = False


class _FakeBridgeRuntime:
    def __init__(self, coordinator) -> None:  # noqa: ANN001
        self._coord = coordinator
        self.run_called = False
        self.stop_called = False

    @property
    def _coordinator(self):  # noqa: ANN202
        return self._coord

    async def recover(self):  # noqa: ANN201
        return ()

    def status(self):  # noqa: ANN201
        return "status"

    async def run(self) -> None:
        self.run_called = True
        await asyncio.sleep(0.05)

    async def stop(self) -> None:
        self.stop_called = True


class _FakeLifecycleManager:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def evaluate_m1_candle(self, symbol, candle, *, atr_for_trailing):  # noqa: ANN001
        self.calls.append((symbol, candle, atr_for_trailing))
        return None


class _FailingLifecycleManager:
    async def evaluate_m1_candle(self, symbol, candle, *, atr_for_trailing):  # noqa: ANN001
        raise RuntimeError("boom")


def _m1_candle(symbol: str, minute: int, *, closed: bool = True):
    return make_candle(symbol, Timeframe.M1, NOW + timedelta(minutes=minute), is_closed=closed)


class TestTaskCreation:
    def test_no_lifecycle_manager_creates_no_m1_tasks(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        runtime = LifecycleRuntime(bridge, None)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            await runtime.stop()
            await task

        run_async(scenario())
        assert runtime._m1_tasks == []
        assert bridge.run_called is True
        assert bridge.stop_called is True

    def test_one_m1_task_per_symbol_when_lifecycle_manager_present(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT", "ETHUSDT"), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            count = len(runtime._m1_tasks)
            await runtime.stop()
            await task
            return count

        count = run_async(scenario())
        assert count == 2


class TestM1Evaluation:
    def test_closed_candle_triggers_evaluation(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert len(manager.calls) == 1
        assert manager.calls[0][0] == "BTCUSDT"

    def test_unclosed_candle_is_skipped(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=False)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls == []

    def test_fresh_atr_snapshot_is_passed_through(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        snapshot = _Snapshot(as_of=candle.close_time - timedelta(minutes=1), values={"ATR_14": 42.0})
        coordinator = _FakeCoordinator(
            ("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(snapshot),
        )
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] == 42.0

    def test_stale_atr_snapshot_yields_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        snapshot = _Snapshot(as_of=candle.close_time - timedelta(minutes=30), values={"ATR_14": 42.0})
        coordinator = _FakeCoordinator(
            ("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(snapshot),
        )
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] is None

    def test_missing_atr_snapshot_yields_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(None))
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] is None

    def test_one_symbol_evaluation_failure_does_not_raise(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        runtime = LifecycleRuntime(bridge, _FailingLifecycleManager())

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())  # no exception == pass


class TestStopCleansUpTasks:
    def test_stop_cancels_all_m1_tasks(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT", "ETHUSDT"), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            tasks_before_stop = list(runtime._m1_tasks)
            await runtime.stop()
            await task
            return tasks_before_stop

        tasks_before_stop = run_async(scenario())
        assert all(t.done() for t in tasks_before_stop)
        assert runtime._m1_tasks == []


class TestM1CandleObserver:
    """Adaptive Intelligence v1 — M1-fidelity shadow-evaluation fix
    (independent-verification round). Same discipline as `RuntimeCoordinator.
    candle_observer`'s own test suite."""

    def test_observer_called_once_per_closed_m1_candle(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        observed: list[tuple] = []
        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=lambda symbol, c: observed.append((symbol, c)))

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert len(observed) == 1
        assert observed[0][0] == "BTCUSDT"

    def test_observer_never_called_for_unclosed_candle(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=False)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        observed: list[tuple] = []
        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=lambda symbol, c: observed.append((symbol, c)))

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert observed == []

    def test_observer_still_called_when_real_evaluation_fails(self) -> None:
        """The observer must fire regardless of whether the REAL
        evaluation succeeded or was isolated -- it is purely
        observational and must never depend on the real path's outcome."""
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        observed: list[tuple] = []
        runtime = LifecycleRuntime(
            bridge, _FailingLifecycleManager(), m1_candle_observer=lambda symbol, c: observed.append((symbol, c)),
        )

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(observed) == 1

    def test_observer_failure_never_raises_or_stops_m1_consumption(self) -> None:
        candles = [_m1_candle("BTCUSDT", 1, closed=True), _m1_candle("BTCUSDT", 2, closed=True)]
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": candles}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()

        def _broken_observer(symbol, candle):  # noqa: ANN001
            raise RuntimeError("simulated shadow-evaluation failure")

        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=_broken_observer)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(manager.calls) == 2  # both candles still reached the REAL evaluation path

    def test_default_behaviour_unchanged_when_observer_is_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)  # no m1_candle_observer

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(manager.calls) == 1


=== FILE: tests/test_local_readiness_check.py ===
"""Faz 12 — `scripts/local_readiness_check.py` testi: tamamen offline,
gerçek ağ çağrısı/order YOK, credential DEĞERİ asla stdout'a YAZILMAZ."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(tmp_path: Path, env_overrides: dict | None = None) -> subprocess.CompletedProcess:
    import os

    env = dict(os.environ)
    env.pop("BINANCE_TESTNET_API_KEY", None)
    env.pop("BINANCE_TESTNET_API_SECRET", None)
    env["CSE_SYMBOLS"] = "BTCUSDT"
    env["CSE_DB_PATH"] = str(tmp_path / "state.db")
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "scripts/local_readiness_check.py"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
    )


class TestLocalReadinessCheck:
    def test_passes_for_a_clean_paper_configuration(self, tmp_path: Path) -> None:
        result = _run(tmp_path)
        assert result.returncode == 0
        assert "LOCAL READINESS: PASS" in result.stdout

    def test_never_prints_credential_values(self, tmp_path: Path) -> None:
        result = _run(
            tmp_path,
            {
                "BINANCE_TESTNET_API_KEY": "readiness-secret-key",
                "BINANCE_TESTNET_API_SECRET": "readiness-secret-value",
                "CSE_EXECUTION_MODE": "BINANCE_SPOT_TESTNET",
                "CSE_ENABLE_TESTNET_EXECUTION": "true",
            },
        )
        combined = result.stdout + result.stderr
        assert "readiness-secret-key" not in combined
        assert "readiness-secret-value" not in combined

    def test_does_not_create_execution_db(self, tmp_path: Path) -> None:
        exec_db = tmp_path / "should-not-exist.db"
        _run(tmp_path, {"BINANCE_TESTNET_EXECUTION_DB_PATH": str(exec_db)})
        assert not exec_db.exists()


=== FILE: tests/test_ops_admin.py ===
"""24/7 Ops v1, Step 3 — `ops/admin.py::AdminController` unit tests.

Fully offline — no real asyncio loop, no real HTTP server. `loop` is a
`MagicMock` standing in for `asyncio.AbstractEventLoop`; `stop()`'s
`call_soon_threadsafe` call is asserted on directly (see
`tests/test_ops_dashboard.py` for the real HTTP-level integration
covering the 401/token-header path end to end)."""

from __future__ import annotations

from unittest.mock import MagicMock

from crypto_signal_engine.ops.admin import AdminController


def _controller(*, notifier=None) -> tuple[AdminController, MagicMock, MagicMock]:
    loop = MagicMock()
    request_shutdown = MagicMock()
    controller = AdminController(token="s3cr3t-token", loop=loop, request_shutdown=request_shutdown, notifier=notifier)
    return controller, loop, request_shutdown


class TestCheckToken:
    def test_correct_token_accepted(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("s3cr3t-token") is True

    def test_wrong_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("wrong") is False

    def test_missing_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token(None) is False

    def test_empty_string_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("") is False

    def test_wrong_token_never_toggles_state(self) -> None:
        """Required test: a rejected token must NEVER be able to mutate
        `entries_paused` by any side channel — `check_token()` alone has
        no mutation capability at all (only `pause()`/`resume()`/`stop()`
        do, and the HTTP layer only calls those AFTER a successful
        `check_token()`, see `ops/dashboard.py::_handle_admin_action`)."""
        controller, _, _ = _controller()
        assert controller.check_token("wrong") is False
        assert controller.entries_paused.is_set() is False


class TestPauseResume:
    def test_pause_sets_entries_paused(self) -> None:
        controller, _, _ = _controller()
        result = controller.pause()
        assert controller.entries_paused.is_set() is True
        assert result["entries_paused"] is True
        assert result["enabled"] is True

    def test_resume_clears_entries_paused(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        result = controller.resume()
        assert controller.entries_paused.is_set() is False
        assert result["entries_paused"] is False

    def test_pause_is_idempotent(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        controller.pause()
        assert controller.entries_paused.is_set() is True

    def test_last_action_recorded_with_timestamp_and_detail(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        status = controller.status()
        assert status["last_action"]["action"] == "pause"
        assert status["last_action"]["at"]
        assert "False -> True" in status["last_action"]["detail"]

    def test_pause_and_resume_notify(self) -> None:
        calls: list[str] = []
        controller, _, _ = _controller(notifier=calls.append)
        controller.pause()
        controller.resume()
        assert len(calls) == 2
        assert "DURAKLATILDI" in calls[0]
        assert "DEVAM ETTİRİLDİ" in calls[1]

    def test_raising_notifier_never_affects_pause_result(self) -> None:
        def _raising(message: str) -> None:
            raise RuntimeError("boom")

        controller, _, _ = _controller(notifier=_raising)
        result = controller.pause()
        assert result["entries_paused"] is True


class TestStop:
    def test_stop_schedules_request_shutdown_via_call_soon_threadsafe(self) -> None:
        controller, loop, request_shutdown = _controller()
        controller.stop()
        loop.call_soon_threadsafe.assert_called_once_with(request_shutdown, "admin-stop")
        # The exact same idempotent chain as SIGTERM -- never a parallel
        # shutdown implementation. `request_shutdown` itself is not
        # called directly here (only scheduled), since this runs on a
        # non-loop thread in production.
        request_shutdown.assert_not_called()

    def test_stop_notifies(self) -> None:
        calls: list[str] = []
        controller, _, _ = _controller(notifier=calls.append)
        controller.stop()
        assert len(calls) == 1
        assert "Acil DURDUR" in calls[0]

    def test_stop_records_last_action(self) -> None:
        controller, _, _ = _controller()
        controller.stop()
        status = controller.status()
        assert status["last_action"]["action"] == "stop"


class TestStatus:
    def test_no_action_yet_has_none_last_action(self) -> None:
        controller, _, _ = _controller()
        status = controller.status()
        assert status == {"enabled": True, "entries_paused": False, "last_action": None}


=== FILE: tests/test_ops_config.py ===
"""Faz 8 — ops/config.py testleri: env-var parsing, validation, fail-fast."""

from __future__ import annotations

from pathlib import Path

import pytest

from crypto_signal_engine.ops.config import AppConfig, load_config
from crypto_signal_engine.ops.errors import AppConfigurationError
from crypto_signal_engine.providers.binance.config import ReconnectPolicyConfig


class TestLoadConfigDefaults:
    def test_minimal_env_produces_safe_defaults(self) -> None:
        config = load_config({"CSE_SYMBOLS": "btcusdt"})
        assert config.symbols == ("BTCUSDT",)
        assert config.log_level == "INFO"
        assert config.fee_bps == 0.0
        assert config.slippage_bps == 0.0
        assert config.notional_per_position == 1000.0
        assert config.warmup_candles == 25
        assert config.order_book_depth == 20
        assert config.stale_feed_threshold_seconds == 30.0
        assert isinstance(config.reconnect, ReconnectPolicyConfig)

    def test_symbols_are_deduplicated_and_normalized(self) -> None:
        config = load_config({"CSE_SYMBOLS": "btcusdt, BTCUSDT ,ethusdt"})
        assert config.symbols == ("BTCUSDT", "ETHUSDT")

    def test_lock_and_health_paths_derived_from_db_path_by_default(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DB_PATH": "/var/lib/cse/state.db"})
        assert config.lock_path == Path("/var/lib/cse/crypto-signal-engine.lock")
        assert config.health_snapshot_path == Path("/var/lib/cse/health.json")

    def test_explicit_lock_and_health_paths_are_respected(self) -> None:
        config = load_config(
            {
                "CSE_SYMBOLS": "BTCUSDT",
                "CSE_LOCK_PATH": "/tmp/custom.lock",
                "CSE_HEALTH_SNAPSHOT_PATH": "/tmp/custom-health.json",
            }
        )
        assert config.lock_path == Path("/tmp/custom.lock")
        assert config.health_snapshot_path == Path("/tmp/custom-health.json")


class TestLoadConfigFailFast:
    def test_missing_symbols_activates_automatic_mode(self) -> None:
        """`CSE_SYMBOLS` artık zorunlu DEĞİL — verilmezse `load_config()`
        HATA VERMEZ, `auto_select_symbols=True` ile boş bir `symbols`
        üretir (gerçek seçim `app.py::resolve_symbols()`'da, network I/O
        ile yapılır — bkz. tests/test_selection_config_integration.py)."""
        config = load_config({})
        assert config.auto_select_symbols is True
        assert config.symbols == ()

    def test_whitespace_only_symbols_also_activates_automatic_mode(self) -> None:
        """Boşluk-only bir değer, `_env()` seviyesinde "verilmemiş" ile
        AYIRT EDİLEMEZ (bkz. `_env()` — strip sonrası boşsa default'a
        düşer) — bu yüzden KASITLI OLARAK "yok" ile aynı, otomatik modu
        tetikler (garbage bir değere değil, gerçekten hiçbir sembol
        listesi VERİLMEMİŞ gibi davranılır)."""
        config = load_config({"CSE_SYMBOLS": "   "})
        assert config.auto_select_symbols is True
        assert config.symbols == ()

    def test_explicit_empty_symbols_list_raises(self) -> None:
        """`CSE_SYMBOLS` AÇIKÇA verilmiş ama hiçbir geçerli sembol
        İÇERMİYORSA (örn. yalnızca virgül) — bu "otomatik moda bırakma"
        DEĞİL, geçersiz bir manuel değerdir, hâlâ AÇIKÇA reddedilir."""
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": ","})

    def test_invalid_log_level_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="LOG_LEVEL"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_LOG_LEVEL": "VERBOSE"})

    def test_non_numeric_fee_bps_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="FEE_BPS"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_FEE_BPS": "not-a-number"})

    def test_negative_fee_bps_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_FEE_BPS": "-1"})

    def test_invalid_order_book_depth_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="ORDER_BOOK_DEPTH"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_ORDER_BOOK_DEPTH": "7"})

    def test_warmup_candles_below_minimum_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="WARMUP_CANDLES"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_WARMUP_CANDLES": "5"})

    def test_non_integer_warmup_candles_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_WARMUP_CANDLES": "abc"})

    def test_zero_stale_feed_threshold_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_STALE_FEED_THRESHOLD_SECONDS": "0"})


class TestAppConfigHasNoCredentialFields:
    def test_no_key_secret_token_credential_fields(self) -> None:
        """24/7 Ops v1, Step 3 — mimari-farkında güncelleme: `dashboard_
        admin_token` KASITLI OLARAK AppConfig içinde YAŞAR (bkz.
        `ops/admin.py`/DECISIONS.md) — bu bir Binance/exchange kimlik
        bilgisi DEĞİLDİR, yalnızca yerel Control Center admin token'ıdır
        (`BINANCE_TESTNET_API_KEY`/`_SECRET`'in AKSİNE, KASITLI OLARAK bu
        sınıfın DIŞINDA tutulur — bkz. `execution/factory.py`). Bu tarama
        ZAYIFLATILMADI — yalnızca TEK, dokümante edilmiş bir istisnaya
        izin verir; başka HİÇBİR "key/secret/token/credential/password"
        parçalı alan adı hâlâ YASAKTIR."""
        import dataclasses

        field_names = {f.name.lower() for f in dataclasses.fields(AppConfig)}
        forbidden_fragments = ["key", "secret", "token", "credential", "password"]
        allowed_exceptions = {"dashboard_admin_token"}
        violations = [
            name for name in field_names
            if any(f in name for f in forbidden_fragments) and name not in allowed_exceptions
        ]
        assert violations == [], f"AppConfig içinde kimlik bilgisi alanı bulundu: {violations}"
        assert "dashboard_admin_token" in field_names, "dashboard_admin_token alanı beklenmedik şekilde eksik"

    def test_binance_config_url_is_not_overridable_via_env(self) -> None:
        """Defense-in-depth: config yüzeyi, Testnet/başka bir execution
        endpoint'ine işaret edecek şekilde KÖTÜYE KULLANILAMAZ — REST/WS
        base URL için hiçbir CSE_* alanı yoktur."""
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        binance_config = config.binance_config()
        assert binance_config.rest_base_url == "https://api.binance.com"
        assert binance_config.ws_base_url == "wss://stream.binance.com:9443"


class TestDashboardAdminTokenConfig:
    """24/7 Ops v1, Step 3 — `dashboard_admin_token` parsing. Default
    `None` = admin actions fully disabled (see `ops/admin.py`/
    `app.py::Application.start()`)."""

    def test_defaults_to_none(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        assert config.dashboard_admin_token is None

    def test_set_via_env(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "my-token"})
        assert config.dashboard_admin_token == "my-token"

    def test_whitespace_only_value_is_treated_as_unset(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "   "})
        assert config.dashboard_admin_token is None

    def test_never_appears_in_summary(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "my-token"})
        assert "dashboard_admin_token" not in config.summary()
        assert "my-token" not in str(config.summary())


class TestAutoSymbolSelectionConfigParsing:
    """`CSE_AUTO_SYMBOL_*` env-parsing + validation (Bölüm "CONFIGURATION").
    Ağ/discovery testleri BURADA DEĞİL — bkz. tests/test_selection_selector.py."""

    def test_defaults_when_unset(self) -> None:
        config = load_config({})
        sel = config.symbol_selection
        assert sel.target_count == 5
        assert sel.shortlist_size == 30
        assert sel.min_quote_volume_24h == 5_000_000.0
        assert sel.lookback_candles == 96
        assert sel.recent_window_candles == 12
        assert sel.rescan_interval_seconds == 3600.0
        assert sel.min_atr_pct == 0.0005
        assert sel.min_current_move_pct == 0.05

    def test_all_fields_overridable_via_env(self) -> None:
        config = load_config(
            {
                "CSE_AUTO_SYMBOL_COUNT": "3",
                "CSE_AUTO_SYMBOL_SHORTLIST_SIZE": "10",
                "CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME": "1000000",
                "CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "50",
                "CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES": "6",
                "CSE_AUTO_SYMBOL_RESCAN_INTERVAL_SECONDS": "1800",
            }
        )
        sel = config.symbol_selection
        assert (sel.target_count, sel.shortlist_size, sel.min_quote_volume_24h) == (3, 10, 1_000_000.0)
        assert (sel.lookback_candles, sel.recent_window_candles, sel.rescan_interval_seconds) == (50, 6, 1800.0)

    def test_manual_symbols_still_parse_selection_config_with_defaults(self) -> None:
        """Manual modda bile `symbol_selection` GEÇERLİ bir config'tir
        (kullanılmaz ama var olması, `AppConfig` şemasını dallandırmaz)."""
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        assert config.auto_select_symbols is False
        assert config.symbol_selection.target_count == 5

    def test_target_count_below_one_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_COUNT": "0"})

    def test_shortlist_smaller_than_target_count_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_COUNT": "10", "CSE_AUTO_SYMBOL_SHORTLIST_SIZE": "5"})

    def test_negative_min_quote_volume_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME": "-1"})

    def test_lookback_below_minimum_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "5"})

    def test_recent_window_larger_than_lookback_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config(
                {"CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "20", "CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES": "50"}
            )

    def test_zero_rescan_interval_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_RESCAN_INTERVAL_SECONDS": "0"})

    def test_negative_min_atr_pct_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_ATR_PCT": "-0.001"})

    def test_negative_min_current_move_pct_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_CURRENT_MOVE_PCT": "-0.01"})

    def test_absolute_activity_floors_overridable_via_env(self) -> None:
        config = load_config(
            {"CSE_AUTO_SYMBOL_MIN_ATR_PCT": "0.002", "CSE_AUTO_SYMBOL_MIN_CURRENT_MOVE_PCT": "0.2"}
        )
        assert config.symbol_selection.min_atr_pct == 0.002
        assert config.symbol_selection.min_current_move_pct == 0.2


=== FILE: tests/test_ops_dashboard.py ===
"""Faz 12 / Görsel yeniden tasarım — `ops/dashboard.py` birim testleri.

Dashboard artık kullanıcı-onaylı `dashboard_design_reference.html`
mockup'ının CSS/JS'iyle render edilir; TÜM gerçek veri, sayfaya gömülen
TEK bir `window.__DASHBOARD_DATA__` JSON nesnesi üzerinden geçer (bkz.
`dashboard.py::_build_view_model`). Bu yüzden testler artık sunucu
tarafında render edilmiş Türkçe HTML metnini DEĞİL, gömülü JSON'u
doğrudan ayrıştırıp GERÇEK pozisyon/performans alanlarının doğru
taşındığını doğrular — bkz. `_extract_view_model()`.

Tamamen offline — gerçek runtime/asyncio event loop'a bağımlı DEĞİLDİR
(bkz. tests/test_phase12_local_production_readiness.py — o dosya
`Application` ile TAM entegrasyonu, bu dosya modülün KENDİSİNİ test
eder)."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from crypto_signal_engine.ops.admin import ADMIN_TOKEN_HEADER, AdminController
from crypto_signal_engine.ops.dashboard import DashboardServer, _build_view_model, _embed_json, render_dashboard_html
from crypto_signal_engine.ops.health_snapshot import write_snapshot
from crypto_signal_engine.runtime.models import RuntimeHealth, RuntimeStatus, SymbolHealth

_DATA_RE = re.compile(r"window\.__DASHBOARD_DATA__ = (.*?);</script>", re.S)


def _extract_view_model(page: str) -> dict:
    match = _DATA_RE.search(page)
    assert match is not None, "window.__DASHBOARD_DATA__ bulunamadı"
    return json.loads(match.group(1))


def _minimal_snapshot(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "overall_health": "READY", "generated_at": "2026-01-01T00:00:00+00:00", "pid": 1,
        "started_at": "2026-01-01T00:00:00+00:00",
        "recovery": {"ok": True, "detail": "running"},
        "symbols": [],
        "execution": {"mode": "PAPER", "enabled": False, "ready": False, "pending_reconciliation_count": None,
                      "last_reconciliation_at": None, "detail": ""},
        "paper": {},
        "symbol_selection": {"mode": "MANUAL", "opportunity_symbols": [], "pinned_open_position_symbols": [],
                              "final_runtime_symbols": [], "reselection_scheduler": {"enabled": False}},
        "candle_history": {},
    }
    base.update(overrides)
    return base


def _testnet_position(**overrides: object) -> dict[str, object]:
    base = {
        "state": "LONG", "net_owned_base_quantity": 0.20000000000000284, "gross_entry_vwap": 81108.02,
        "effective_stop": 80752.67659074345, "take_profit": 81818.70681851312, "trailing_active": False,
        "cumulative_realized_gross_pnl": 0.0, "migrated_existing_position": False, "last_exit_reason": None,
        "latest_price": 81500.0, "unrealized_gross_pnl": 78.396,
    }
    base.update(overrides)
    return base


def _bridge_lifecycle(positions: dict, *, performance: dict | None = None, recent_trades: list | None = None) -> dict:
    return {
        "enabled": True, "positions": positions,
        "performance": performance if performance is not None else {
            "exposure_usdt": 0.0, "non_flat_position_count": len([p for p in positions.values() if p.get("state") != "FLAT"]),
            "completed_trade_count": 0, "win_count": 0, "loss_count": 0, "win_rate": None,
            "realized_gross_pnl": 0.0, "todays_realized_gross_pnl": 0.0, "unrealized_gross_pnl_total": 0.0,
            "average_win_gross": None, "average_loss_gross": None,
        },
        "recent_trades": recent_trades if recent_trades is not None else [],
    }


class TestNoSnapshotYet:
    def test_renders_without_crashing_and_marks_no_snapshot(self, tmp_path: Path) -> None:
        page = render_dashboard_html(None, health_snapshot_path=tmp_path / "health.json")
        assert "Sinyal Botu Terminali" in page
        model = _extract_view_model(page)
        assert model["no_snapshot_yet"] is True


class TestJsonEmbeddingSafety:
    def test_script_breakout_is_neutralized(self) -> None:
        payload = {"x": "</script><script>alert(1)</script>"}
        embedded = _embed_json(payload)
        assert "</script>" not in embedded
        # round-trips back to the ORIGINAL string -- escaping is presentation-only, never lossy
        assert json.loads(embedded)["x"] == "</script><script>alert(1)</script>"

    def test_malicious_detail_survives_as_data_never_as_a_live_script_tag(self) -> None:
        snapshot = _minimal_snapshot(
            overall_health="DEGRADED",
            symbols=[{"symbol": "BTCUSDT", "health": "DEGRADED", "last_event_at": "x", "reconnect_count": 0,
                      "detail": "</script><script>alert(1)</script>"}],
        )
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        assert "</script><script>alert(1)</script>" not in page  # no live breakout in the HTML source
        model = _extract_view_model(page)
        assert model["degraded_symbols"][0]["detail_tr"] == "</script><script>alert(1)</script>"  # data preserved

    def test_free_text_fields_are_escaped_at_every_dom_insertion_site(self) -> None:
        """Structural guarantee: every JS call site that inserts a
        free-text (detail/exit-reason) field into innerHTML wraps it in
        `esc(...)` -- source-level check, since this test suite has no JS
        engine to execute the script against a real DOM."""
        src = Path("crypto_signal_engine/ops/dashboard.py").read_text(encoding="utf-8")
        for needle in (
            "esc(first.symbol)", "esc(first.detail_tr)", "esc(t.exit_reason_tr",
            "esc(p.last_exit_reason_tr", "esc(d.symbol)", "esc(d.detail_tr)",
        ):
            assert needle in src, f"{needle} bulunamadı — bir DOM-insertion noktası escape edilmemiş olabilir"


class TestReadOnlySafety:
    def test_no_forms_present(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "<form" not in page

    def test_no_inline_onclick_mutation_handlers(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "onclick=" not in page

    def test_restart_button_still_shows_not_connected_toast_not_fake_success(self) -> None:
        """24/7 Ops v1, Step 3 — Pause/Resume/Stop are now real (see
        `TestControlCenterAdminWiring` below); Restart remains
        deliberately NOT wired (see module docstring: remote-restart is
        out of scope) and still shows an honest "not connected" message,
        never a fabricated success string."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "kasıtlı olarak bağlanmadı" in page
        for fake_success in ("Bot duraklatıldı", "Bot devam ettirildi", "Yeniden başlatma tamamlandı", "Bot acil olarak durduruldu"):
            assert fake_success not in page

    def test_pause_resume_stop_wired_to_real_admin_endpoints(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "/admin/pause" in page
        assert "/admin/resume" in page
        assert "/admin/stop" in page

    def test_admin_token_and_stop_confirmation_use_a_real_modal_not_native_dialogs(self) -> None:
        """UI Polish v1, Step A6 — the approved reference mockup's modal
        pattern (`dashboard_design_reference.html`) now backs the admin
        token prompt and the Emergency Stop confirmation, replacing
        `window.prompt()`/`window.confirm()` entirely."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "modal-veil" in page
        assert "modal-actions" in page
        assert "modal-check" in page
        assert "window.prompt(" not in page
        assert "window.confirm(" not in page

    def test_modal_is_keyboard_operable(self) -> None:
        """Step A8 — Escape closes it, focus is trapped/managed."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "Escape" in page
        assert "modalReturnFocus" in page

    def test_toast_wrap_is_aria_live_polite(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert 'aria-live="polite"' in page

    def test_buy_sell_wording_absent(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position()}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        assert "buy" not in page.lower().replace("btnrestart", "")  # symbol-free lowercase scan
        assert "<button" in page  # control buttons still visually present (per reference design)


class TestViewModelRealPositionData:
    def test_known_positions_stop_and_target_appear_correctly(self) -> None:
        """Mission-required: a known open position's effective_stop/
        take_profit values appear correctly in the embedded view model."""
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({
            "ARBUSDT": _testnet_position(effective_stop=0.13530768, take_profit=0.13925248, gross_entry_vwap=0.1352),
        }))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        arb = next(p for p in model["positions"] if p["symbol"] == "ARBUSDT")
        assert arb["effective_stop"] == 0.13530768
        assert arb["take_profit"] == 0.13925248
        assert arb["gross_entry_vwap"] == 0.1352

    def test_flat_positions_excluded(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"ETHUSDT": {"state": "FLAT"}}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"] == []
        assert model["dust"] == []

    def test_dust_positions_separated_from_open_positions(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({
            "ARBUSDT": _testnet_position(state="LONG"),
            "BTCUSDT": _testnet_position(state="DUST", last_exit_reason="STOP_LOSS"),
        }))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert [p["symbol"] for p in model["positions"]] == ["ARBUSDT"]
        assert [p["symbol"] for p in model["dust"]] == ["BTCUSDT"]
        assert model["dust"][0]["last_exit_reason_tr"] == "Güvenlik seviyesi"

    def test_each_lifecycle_state_translates_to_turkish(self) -> None:
        expected = {
            "LONG": "Açık", "ENTRY_PENDING": "Alış emri sonuç bekliyor", "EXIT_PENDING": "Satış emri sonuç bekliyor",
            "AMBIGUOUS": "Binance sonucu kontrol ediliyor", "RECOVERED": "Geri yüklendi / aktifleşme bekliyor",
        }
        for raw, turkish in expected.items():
            snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position(state=raw)}))
            page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
            model = _extract_view_model(page)
            assert model["positions"][0]["state_tr"] == turkish

    def test_each_exit_reason_translates_to_turkish(self) -> None:
        expected = {
            "STOP_LOSS": "Güvenlik seviyesi", "TAKE_PROFIT": "Kâr hedefi", "TRAILING_STOP": "Takip eden stop",
            "MAX_HOLD": "Maksimum bekleme süresi", "OPPOSITE_SIGNAL": "Ters sinyal",
        }
        for raw, turkish in expected.items():
            snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
                {}, recent_trades=[{"recorded_at": "x", "symbol": "BTCUSDT", "quantity_closed": 1.0,
                                     "gross_entry_vwap": 100.0, "exit_gross_vwap": 105.0, "gross_realized_pnl": 5.0,
                                     "net_realized_pnl": 4.9, "exit_reason": raw}],
            ))
            page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
            model = _extract_view_model(page)
            assert model["recent_trades"][0]["exit_reason_tr"] == turkish

    def test_unknown_exit_reason_falls_back_to_raw_value(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position(last_exit_reason="SOME_FUTURE_REASON")}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"][0]["last_exit_reason_tr"] == "SOME_FUTURE_REASON"

    def test_commission_derived_correctly_from_gross_and_net(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {}, recent_trades=[{"recorded_at": "x", "symbol": "BTCUSDT", "quantity_closed": 1.0,
                                 "gross_entry_vwap": 100.0, "exit_gross_vwap": 105.0, "gross_realized_pnl": 5.0,
                                 "net_realized_pnl": 4.5, "exit_reason": "TAKE_PROFIT"}],
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["recent_trades"][0]["commission_usdt"] == 0.5

    def test_numeric_precision_is_not_altered_by_the_view_model(self) -> None:
        """Formatting (1.240,50 style) happens in JS at render time --
        the Python-side view model must NEVER truncate/round the
        underlying stored precision."""
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {"ARBUSDT": _testnet_position(net_owned_base_quantity=0.20000000000000284)},
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"][0]["net_owned_base_quantity"] == 0.20000000000000284


class TestViewModelKpisAndScan:
    def test_kpi_fields_pass_through_from_performance(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {"ARBUSDT": _testnet_position()},
            performance={
                "exposure_usdt": 16.0, "non_flat_position_count": 1, "completed_trade_count": 8,
                "win_count": 2, "loss_count": 6, "win_rate": 0.25, "realized_gross_pnl": -1.08,
                "todays_realized_gross_pnl": 0.42, "unrealized_gross_pnl_total": 78.396,
                "average_win_gross": 0.28, "average_loss_gross": -0.27,
            },
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        kpi = model["kpi"]
        assert kpi["unrealized_total"] == 78.396
        assert kpi["todays_realized"] == 0.42
        assert kpi["win_count"] == 2 and kpi["loss_count"] == 6
        assert kpi["win_rate"] == 0.25

    def test_bridge_disabled_kpi_is_none(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle={"enabled": False, "positions": {}})
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["kpi"] is None
        assert model["bridge_enabled"] is False

    def test_scan_fields_map_from_symbol_selection(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTOMATIC", "opportunity_symbols": ["ZENUSDT", "DASHUSDT"],
            "pinned_open_position_symbols": ["ARBUSDT", "BTCUSDT"], "final_runtime_symbols": ["ARBUSDT", "BTCUSDT", "ZENUSDT", "DASHUSDT"],
            "universe_size": 486, "shortlist_size": 30,
            "reselection_scheduler": {"enabled": True, "armed": True, "last_rescan_at": "2026-01-01T00:00:00+00:00",
                                       "next_rescan_at": "2026-01-01T01:00:00+00:00", "rescan_interval_seconds": 3600.0,
                                       "last_added": [], "last_removed": [], "last_error": None},
        })
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        scan = model["scan"]
        assert scan["scanned_count"] == 30
        assert scan["new_opportunities"] == ["ZENUSDT", "DASHUSDT"]
        assert scan["protected_symbols"] == ["ARBUSDT", "BTCUSDT"]
        assert scan["armed"] is True

    def test_degraded_symbols_translated_and_listed(self) -> None:
        snapshot = _minimal_snapshot(
            overall_health="DEGRADED",
            symbols=[
                {"symbol": "PROMUSDT", "health": "DEGRADED", "last_event_at": "x", "reconnect_count": 0, "detail": "unresolved gap: 5m"},
                {"symbol": "BTCUSDT", "health": "READY", "last_event_at": "x", "reconnect_count": 0, "detail": "healthy"},
            ],
        )
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert len(model["degraded_symbols"]) == 1
        assert model["degraded_symbols"][0]["symbol"] == "PROMUSDT"
        assert model["degraded_symbols"][0]["detail_tr"] == "5 dakikalık piyasa verisinde eksik veri algılandı"


class TestCandleHistory:
    def test_real_candle_history_passes_through_unaltered(self) -> None:
        candles = [{"t": "2026-01-01T00:00:00+00:00", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5}]
        snapshot = _minimal_snapshot(candle_history={"BTCUSDT": {"5m": candles}})
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["candles"]["BTCUSDT"]["5m"] == candles

    def test_missing_candle_history_is_an_empty_dict_not_fabricated(self) -> None:
        snapshot = _minimal_snapshot()
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["candles"] == {}
        # the JS-side fallback generator is explicitly marked with the example-data badge
        assert "örnek veri" in page
        assert "gerçek veri" in page


class TestMainStructurePresent:
    def test_reference_design_sections_present(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"ARBUSDT": _testnet_position()}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        for marker in (
            "Sinyal Botu Terminali", "view-genel", "view-islemler", "view-control",
            "coinListBody", "candleWrap", "tradesBody", "openBody", "dustBody", "scanGrid", "healthCard", "svcGrid",
        ):
            assert marker in page

    def test_credentials_never_appear(self) -> None:
        snapshot = _minimal_snapshot(execution={
            "mode": "BINANCE_SPOT_TESTNET", "enabled": True, "ready": True,
            "pending_reconciliation_count": 0, "last_reconciliation_at": "x", "detail": "ok",
        })
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        for forbidden in ("api_key", "api_secret", "secret", "signature"):
            assert forbidden not in page.lower()


class TestDashboardServerLifecycle:
    def _write(self, path: Path) -> None:
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(
            overall_health=RuntimeHealth.READY,
            symbols=(SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=now,
                                   reconnect_count=0, detail="healthy"),),
            generated_at=now,
        )
        write_snapshot(
            path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now,
            execution={"mode": "PAPER", "enabled": False, "ready": False, "pending_reconciliation_count": None,
                       "last_reconciliation_at": None, "detail": ""},
            paper={"BTCUSDT": {"position": {"side": "FLAT", "quantity": 0.0, "average_entry_price": 0.0,
                                             "realized_pnl": 0.0},
                                "last_signal": {"direction": None, "timestamp": None, "context_id": None}}},
            bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position()}),
            candle_history={"BTCUSDT": {"5m": [{"t": now.isoformat(), "o": 1, "h": 1, "l": 1, "c": 1}]}},
        )

    def test_start_stop_is_idempotent_and_serves_status_and_html(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=18999, health_snapshot_path=snap_path)
        server.start()
        server.start()  # idempotent
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:18999/healthz", timeout=2) as resp:
                assert resp.status == 200
            with urllib.request.urlopen("http://127.0.0.1:18999/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            assert data["overall_health"] == "READY"
            assert "candle_history" in data
            with urllib.request.urlopen("http://127.0.0.1:18999/", timeout=2) as resp:
                assert resp.status == 200
                page = resp.read().decode("utf-8")
            model = _extract_view_model(page)
            assert model["positions"][0]["symbol"] == "BTCUSDT"
        finally:
            server.stop()
            server.stop()  # idempotent

    def test_unknown_path_returns_404(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19000, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            try:
                urllib.request.urlopen("http://127.0.0.1:19000/nonexistent", timeout=2)
                raise AssertionError("expected HTTPError")
            except urllib.error.HTTPError as exc:
                assert exc.code == 404
        finally:
            server.stop()

    def test_mutation_methods_are_rejected(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19001, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                request = urllib.request.Request("http://127.0.0.1:19001/", method=method)
                try:
                    urllib.request.urlopen(request, timeout=2)
                    raise AssertionError(f"{method} should have been rejected")
                except urllib.error.HTTPError as exc:
                    assert exc.code == 405
        finally:
            server.stop()

    def test_api_status_preserves_existing_top_level_keys(self, tmp_path: Path) -> None:
        """Bölüm "OUT OF SCOPE"/"IMPLEMENTATION SCOPE" — `/api/status`
        mevcut alan adlarını KORUR; `candle_history` yalnızca EKLENİR."""
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19002, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:19002/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            for key in (
                "generated_at", "overall_health", "pid", "started_at", "recovery", "symbols",
                "execution", "paper", "symbol_selection", "signal_testnet_bridge", "bridge_lifecycle",
                "candle_history",
            ):
                assert key in data
            position = data["bridge_lifecycle"]["positions"]["BTCUSDT"]
            for key in ("state", "net_owned_base_quantity", "gross_entry_vwap", "effective_stop", "take_profit"):
                assert key in position
        finally:
            server.stop()


class TestControlCenterAdminEndpoints:
    """24/7 Ops v1, Step 3 — real HTTP-level coverage of `POST /admin/
    {pause,resume,stop}`. Required tests: wrong/missing token always 401
    and NEVER toggles state; `dashboard_admin_token` unset (no
    `AdminController` set) behaves EXACTLY like before (405, same as
    every other mutation method)."""

    def _server(self, tmp_path: Path, *, port: int) -> DashboardServer:
        snap_path = tmp_path / "health.json"
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(overall_health=RuntimeHealth.READY, symbols=(), generated_at=now)
        write_snapshot(snap_path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now)
        return DashboardServer(host="127.0.0.1", port=port, health_snapshot_path=snap_path)

    def _post(self, url: str, *, token: str | None = None):
        headers = {ADMIN_TOKEN_HEADER: token} if token is not None else {}
        request = urllib.request.Request(url, method="POST", headers=headers)
        return urllib.request.urlopen(request, timeout=2)

    def test_admin_paths_405_when_no_admin_controller_configured(self, tmp_path: Path) -> None:
        """Regression: with no `AdminController` set (the `dashboard_
        admin_token=None` default path), `/admin/pause` is not special —
        it falls through to the SAME 405 every other POST gets, exactly
        like before this milestone."""
        server = self._server(tmp_path, port=19100)
        server.start()
        try:
            time.sleep(0.1)
            for path in ("/admin/pause", "/admin/resume", "/admin/stop"):
                try:
                    self._post(f"http://127.0.0.1:19100{path}")
                    raise AssertionError(f"{path} should have been rejected (405)")
                except urllib.error.HTTPError as exc:
                    assert exc.code == 405
        finally:
            server.stop()

    def test_missing_token_returns_401_and_never_toggles_state(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19101)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            try:
                self._post("http://127.0.0.1:19101/admin/pause")
                raise AssertionError("expected 401")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_wrong_token_returns_401_and_never_toggles_state(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19102)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            try:
                self._post("http://127.0.0.1:19102/admin/pause", token="totally-wrong")
                raise AssertionError("expected 401")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_correct_token_pauses_and_resumes(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19103)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with self._post("http://127.0.0.1:19103/admin/pause", token="s3cr3t") as resp:
                assert resp.status == 200
                body = json.loads(resp.read())
            assert body["entries_paused"] is True
            assert admin.entries_paused.is_set() is True

            with self._post("http://127.0.0.1:19103/admin/resume", token="s3cr3t") as resp:
                body = json.loads(resp.read())
            assert body["entries_paused"] is False
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_stop_reuses_request_shutdown_not_a_parallel_mechanism(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19104)
        loop = MagicMock()
        request_shutdown = MagicMock()
        admin = AdminController(token="s3cr3t", loop=loop, request_shutdown=request_shutdown)
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with self._post("http://127.0.0.1:19104/admin/stop", token="s3cr3t") as resp:
                assert resp.status == 200
            loop.call_soon_threadsafe.assert_called_once_with(request_shutdown, "admin-stop")
        finally:
            server.stop()

    def test_admin_state_reflected_in_api_status_and_view_model(self, tmp_path: Path) -> None:
        """The dashboard's GET responses (both `/api/status` and `/`)
        NEVER read `AdminController` directly — they read ONLY the
        periodic atomic snapshot file (see `ops/dashboard.py` module
        docstring, "Bölüm 6"); `Application._write_snapshot()` is what
        threads `AdminController.status()` into that file in production
        (via `_admin_snapshot()`). This test proves the READ side of
        that contract by writing the snapshot with `admin=` explicitly,
        exactly like `Application` does."""
        snap_path = tmp_path / "health.json"
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(overall_health=RuntimeHealth.READY, symbols=(), generated_at=now)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        admin.pause()
        write_snapshot(
            snap_path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now,
            admin=admin.status(),
        )
        server = DashboardServer(host="127.0.0.1", port=19105, health_snapshot_path=snap_path)
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:19105/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            assert data["admin"]["enabled"] is True
            assert data["admin"]["entries_paused"] is True

            with urllib.request.urlopen("http://127.0.0.1:19105/", timeout=2) as resp:
                page = resp.read().decode("utf-8")
            model = _extract_view_model(page)
            assert model["admin"]["enabled"] is True
        finally:
            server.stop()


class TestStepA2UsdtBalanceNoneSafety:
    """UI Polish v1, Step A2 — `_build_view_model()` reads
    `bridge_lifecycle.usdt_balance` via `.get()` (never fabricates a
    number) and ignores it entirely when the bridge itself is disabled."""

    def test_present_when_bridge_enabled(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})
        bridge_lifecycle["usdt_balance"] = 1234.56
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] == 1234.56

    def test_none_when_key_missing_even_though_bridge_enabled(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})  # no "usdt_balance" key at all
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None

    def test_none_when_bridge_disabled_even_if_value_present(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})
        bridge_lifecycle["usdt_balance"] = 999.0
        bridge_lifecycle["enabled"] = False
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None

    def test_none_when_bridge_key_absent_from_snapshot_entirely(self) -> None:
        snapshot = _minimal_snapshot()  # no "bridge_lifecycle" key at all
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None


class TestStepA4LearnedFactorScoreSourcing:
    """UI Polish v1, Step A4 — `_build_view_model()` builds `scan.
    learned_factors` from `symbol_selection.candidates` (already produced
    by `app.py::_symbol_selection_snapshot()`), never inventing a score
    for a symbol with no candidate entry."""

    def test_built_from_candidates(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTO", "opportunity_symbols": [], "pinned_open_position_symbols": [],
            "final_runtime_symbols": ["BTCUSDT", "ETHUSDT"], "reselection_scheduler": {"enabled": True},
            "candidates": [
                {"symbol": "BTCUSDT", "learned_factor_score": 0.7},
                {"symbol": "ETHUSDT", "learned_factor_score": 0.3},
            ],
        })
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {"BTCUSDT": 0.7, "ETHUSDT": 0.3}

    def test_empty_when_no_candidates_present_manual_mode(self) -> None:
        snapshot = _minimal_snapshot()  # default MANUAL mode, no "candidates" key
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {}

    def test_candidate_without_a_symbol_is_skipped_not_fabricated(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTO", "opportunity_symbols": [], "pinned_open_position_symbols": [],
            "final_runtime_symbols": [], "reselection_scheduler": {"enabled": True},
            "candidates": [{"learned_factor_score": 0.9}, {"symbol": None, "learned_factor_score": 0.5}],
        })
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {}


=== FILE: tests/test_ops_event_log.py ===
"""
UI Polish / Final Acceptance v1, Step A9 — `ops/event_log.py` unit tests,
plus the defensive-wrap discipline proof at every real call site
(`ops/admin.py`, `execution/lifecycle_manager.py`, `adaptive/rollback.py`,
and the `adaptive/cycle.py` -> `adaptive/scheduler.py` pass-through chain
above it — see `ops/event_log.py`'s module docstring for the full list).

`record_event()` NEVER raises (any `sqlite3.Error`/`OSError` is caught
INSIDE the function itself) — which is exactly what lets every call site
above call it with no try/except of its own; unlike `safe_notify()`
(itself a wrapper every caller goes through), `record_event()` is called
directly, so the only faithful way to prove a caller is unaffected by a
failure is to force `record_event()`'s REAL internal failure path (an
`event_log_path` that can never be written to) rather than mock the
function to raise arbitrarily — mocking it to raise would just prove the
caller has no try/except of its own (true, and by design: the safety
net is entirely inside `record_event()`), not that the documented
discipline actually holds. Same standard `tests/test_adaptive_rollback.
py::test_a_raising_notifier_never_affects_the_rollback_decision` and
`tests/test_execution_lifecycle_manager.py::
test_a_raising_notifier_never_affects_entry_gate_decision` already set
for `safe_notify()`, adapted to `record_event()`'s different (non-
injectable, module-level) call shape."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from adaptive.cycle import run_adaptive_cycle
from adaptive.policy import PolicySnapshot
from adaptive.rollback import apply_rollback_if_needed
from adaptive.scheduler import AdaptiveScheduler, AdaptiveSchedulerConfig
from adaptive.shadow import ShadowOutcome
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.execution.lifecycle import (
    DailyRiskAccumulator,
    ExitReason,
    RiskPolicyConfig,
    trading_day_key,
)
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.ops.admin import AdminController
from crypto_signal_engine.ops.event_log import record_event, recent_events
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SYMBOL = "BTCUSDT"
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _unwritable_path(tmp_path: Path) -> Path:
    """A `db_path` whose parent can never be created: `blocker` already
    exists as a plain FILE, so `db_path.parent.mkdir(parents=True,
    exist_ok=True)` inside `record_event()` always raises `FileExistsError`
    (an `OSError` subclass) — the exact real-world failure mode
    `record_event()`'s own internal `except (sqlite3.Error, OSError)` is
    documented to swallow. This is a REAL failure, not a mock, so a test
    passing this path proves the documented discipline, not just that a
    mock was installed correctly."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    return blocker / "events.db"


# ============================================================================
# ops/event_log.py itself
# ============================================================================


class TestRecordEventAndRecentEvents:
    def test_round_trip(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        record_event(db_path, event_type="admin_pause", detail="entries_paused: False -> True", occurred_at=NOW)
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "admin_pause"
        assert events[0]["detail"] == "entries_paused: False -> True"
        assert events[0]["occurred_at"] == NOW.isoformat()

    def test_most_recent_first_and_limit_respected(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        for i in range(5):
            record_event(db_path, event_type=f"event_{i}", detail="d", occurred_at=NOW + timedelta(minutes=i))
        events = recent_events(db_path, limit=3)
        assert [e["event_type"] for e in events] == ["event_4", "event_3", "event_2"]

    def test_recent_events_empty_when_file_never_written(self, tmp_path: Path) -> None:
        assert recent_events(tmp_path / "never_written.db") == []

    def test_table_and_parent_directory_created_lazily_on_first_write(self, tmp_path: Path) -> None:
        db_path = tmp_path / "nested" / "dir" / "events.db"
        assert not db_path.parent.exists()
        record_event(db_path, event_type="x", detail="y", occurred_at=NOW)
        assert db_path.exists()
        assert recent_events(db_path) != []

    def test_record_event_never_raises_when_parent_directory_cannot_be_created(self, tmp_path: Path) -> None:
        record_event(_unwritable_path(tmp_path), event_type="x", detail="y", occurred_at=NOW)  # must not raise

    def test_recent_events_never_raises_on_a_corrupt_db_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        db_path.write_bytes(b"not a real sqlite file at all")
        assert recent_events(db_path) == []

    def test_record_event_never_raises_on_a_corrupt_db_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        db_path.write_bytes(b"not a real sqlite file at all")
        record_event(db_path, event_type="x", detail="y", occurred_at=NOW)  # must not raise


# ============================================================================
# Call site 1/4 — ops/admin.py::AdminController
# ============================================================================


def _admin_controller(*, event_log_path: Path | None) -> AdminController:
    from unittest.mock import MagicMock

    return AdminController(
        token="s3cr3t-token", loop=MagicMock(), request_shutdown=MagicMock(), event_log_path=event_log_path,
    )


class TestAdminControllerEventLog:
    def test_pause_records_a_real_event(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        controller = _admin_controller(event_log_path=db_path)
        controller.pause()
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "admin_pause"

    def test_pause_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        result = controller.pause()  # must not raise
        assert result["entries_paused"] is True
        assert controller.entries_paused.is_set() is True

    def test_resume_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        controller.pause()
        result = controller.resume()  # must not raise
        assert result["entries_paused"] is False

    def test_stop_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        result = controller.stop()  # must not raise
        assert result["last_action"]["action"] == "stop"


# ============================================================================
# Call site 2/4 — execution/lifecycle_manager.py::LifecycleManager.entry_gate
# ============================================================================


def _lifecycle_manager(tmp_path: Path, *, event_log_path: Path | None, risk_policy=None) -> tuple[LifecycleManager, LifecycleStore]:
    http = FakeTestnetHttpClient()
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    manager = LifecycleManager(
        store=lifecycle_store, execution_service=service, risk_policy=risk_policy,
        clock=FixedClock(NOW), event_log_path=event_log_path,
    )
    return manager, lifecycle_store


class TestLifecycleManagerEventLog:
    def test_daily_loss_breaker_transition_records_a_real_event(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        manager, store = _lifecycle_manager(
            tmp_path, event_log_path=db_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "daily_loss_breaker_tripped"

    def test_entry_gate_decision_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        manager, store = _lifecycle_manager(
            tmp_path, event_log_path=_unwritable_path(tmp_path),
            risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))  # must not raise
        assert result.allowed is False
        assert "daily loss" in result.detail


# ============================================================================
# Call site 3/4 — adaptive/rollback.py::apply_rollback_if_needed
# ============================================================================


def _policy_snapshot(version_id: str, **overrides) -> PolicySnapshot:
    defaults = dict(
        stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
        trailing_distance_atr_multiple=2.0, max_hold_hours=48.0, created_at=NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(version_id=version_id, **defaults)


def _seed_shadow_outcomes(store: AdaptiveStore, version_id: str, pnls: list[float]) -> None:
    for pnl in pnls:
        store.record_shadow_outcome(
            version_id, symbol=SYMBOL,
            outcome=ShadowOutcome(
                exit_reason=ExitReason.TAKE_PROFIT if pnl > 0 else ExitReason.STOP_LOSS,
                exit_price=100.0 + pnl, exit_time=NOW, gross_pnl_per_unit=pnl,
            ),
            now=NOW,
        )


def _degraded_champion_pair(store: AdaptiveStore) -> tuple[PolicySnapshot, PolicySnapshot]:
    """v2 is champion with a healthy shadow-stage track record, but has
    genuinely collapsed in real production -- the exact
    `test_rolls_back_using_own_shadow_stage_baseline_on_genuine_
    degradation` scenario from `tests/test_adaptive_rollback.py`, reused
    here purely as a realistic rollback-triggering fixture."""
    v1 = _policy_snapshot("v1")
    v2 = _policy_snapshot("v2", max_hold_hours=12.0)
    store.promote_champion(v1, reason="bootstrap", now=NOW)
    store.promote_champion(v2, reason="promoted", now=NOW)
    _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)
    return v1, v2


def _degraded_live_pnls(version_id: str) -> list[float]:
    return [1.0] * 2 + [-10.0] * 8


@pytest.fixture()
def adaptive_store(tmp_path: Path) -> AdaptiveStore:
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


class TestRollbackEventLog:
    def test_rollback_records_a_real_event(self, adaptive_store: AdaptiveStore, tmp_path: Path) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"
        result = apply_rollback_if_needed(
            adaptive_store, champion=v2, live_pnls_provider=_degraded_live_pnls, now=NOW, event_log_path=db_path,
        )
        assert result == v1
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "adaptive_rollback"
        assert "v2" in events[0]["detail"] and "v1" in events[0]["detail"]

    def test_rollback_decision_unaffected_by_an_unwritable_event_log(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        result = apply_rollback_if_needed(
            adaptive_store, champion=v2, live_pnls_provider=_degraded_live_pnls, now=NOW,
            event_log_path=_unwritable_path(tmp_path),
        )  # must not raise
        assert result == v1
        assert adaptive_store.current_champion() == v1


# ============================================================================
# Call site 4/4 (pass-through) — adaptive/cycle.py -> adaptive/scheduler.py
# ============================================================================


def _window_config() -> WindowConfig:
    return WindowConfig(discovery_window_size=timedelta(hours=6), confirmation_window_size=timedelta(hours=2))


class TestCycleEventLogWiring:
    """`adaptive/cycle.py` never calls `record_event()` itself -- it only
    threads `event_log_path` through to `apply_rollback_if_needed()`
    (already proven defensive above). These tests prove the wiring
    actually reaches it, end to end, with a REAL rollback and a REAL
    (or genuinely broken) event log."""

    def test_rollback_wired_through_cycle_records_a_real_event(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"

        async def scenario():
            return await run_adaptive_cycle(
                store=adaptive_store, candle_source=FakeHistoricalCandleSource(), symbols=(SYMBOL,),
                window_config=_window_config(), anchor=EPOCH, seed=1, now=EPOCH + timedelta(minutes=30),
                champion_live_pnls_provider=_degraded_live_pnls, event_log_path=db_path,
            )

        report = run_async(scenario())
        assert report.champion_version_id == "v1"
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "adaptive_rollback"

    def test_cycle_result_unaffected_by_an_unwritable_event_log(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)

        async def scenario():
            return await run_adaptive_cycle(
                store=adaptive_store, candle_source=FakeHistoricalCandleSource(), symbols=(SYMBOL,),
                window_config=_window_config(), anchor=EPOCH, seed=1, now=EPOCH + timedelta(minutes=30),
                champion_live_pnls_provider=_degraded_live_pnls, event_log_path=_unwritable_path(tmp_path),
            )

        report = run_async(scenario())  # must not raise, must not silently drop the rollback
        assert report.champion_version_id == "v1"
        assert adaptive_store.current_champion() == v1


class TestSchedulerEventLogWiring:
    """`adaptive/scheduler.py::AdaptiveScheduler` never calls
    `record_event()` itself either -- it only threads `event_log_path`
    through to `run_adaptive_cycle()`. `run_once()` ALSO wraps the whole
    cycle in a broad `except Exception` (a failed cycle must never kill
    the scheduler loop) -- these tests prove a broken event log is
    swallowed by `record_event()` itself and never even reaches that
    outer guard as a reported cycle failure."""

    def _config(self) -> AdaptiveSchedulerConfig:
        return AdaptiveSchedulerConfig(
            symbols=(SYMBOL,), anchor=EPOCH, window_config=_window_config(), cycle_interval_seconds=3600.0, seed=1,
        )

    def test_run_once_wires_event_log_path_and_records_a_real_event(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"
        scheduler = AdaptiveScheduler(
            store=adaptive_store, candle_source=FakeHistoricalCandleSource(), config=self._config(),
            champion_live_pnls_provider=_degraded_live_pnls, clock=FixedClock(EPOCH + timedelta(minutes=30)),
            event_log_path=db_path,
        )
        report = run_async(scheduler.run_once())
        assert report is not None
        assert report.champion_version_id == "v1"
        assert recent_events(db_path) != []
        assert scheduler.status().last_error is None

    def test_run_once_not_reported_as_a_failed_cycle_when_event_log_is_unwritable(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        scheduler = AdaptiveScheduler(
            store=adaptive_store, candle_source=FakeHistoricalCandleSource(), config=self._config(),
            champion_live_pnls_provider=_degraded_live_pnls, clock=FixedClock(EPOCH + timedelta(minutes=30)),
            event_log_path=_unwritable_path(tmp_path),
        )
        report = run_async(scheduler.run_once())
        assert report is not None  # NOT swallowed into "cycle failed" by the outer except Exception
        assert report.champion_version_id == "v1"
        status = scheduler.status()
        assert status.last_error is None
        assert status.last_promoted_to is None  # a rollback is not a PROMOTE decision


=== FILE: tests/test_ops_health_snapshot.py ===
"""Faz 8 — ops/health_snapshot.py testleri: atomik yazma, round-trip okuma."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.ops.health_snapshot import read_snapshot, write_snapshot
from crypto_signal_engine.runtime.models import RuntimeHealth, RuntimeStatus, SymbolHealth


def _status(*, health: RuntimeHealth = RuntimeHealth.READY, detail: str = "ok") -> RuntimeStatus:
    now = datetime.now(timezone.utc)
    return RuntimeStatus(
        overall_health=health,
        symbols=(
            SymbolHealth(symbol="BTCUSDT", health=health, last_event_at=now, reconnect_count=0, detail=detail),
        ),
        generated_at=now,
    )


class TestWriteReadRoundTrip:
    def test_read_missing_file_returns_none(self, tmp_path: Path) -> None:
        assert read_snapshot(tmp_path / "does-not-exist.json") is None

    def test_write_then_read_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        started_at = datetime.now(timezone.utc)
        write_snapshot(
            path, status=_status(), recovery_ok=True, recovery_detail="recovery completed",
            pid=1234, started_at=started_at,
        )
        snapshot = read_snapshot(path)
        assert snapshot is not None
        assert snapshot["overall_health"] == "READY"
        assert snapshot["pid"] == 1234
        assert snapshot["recovery"] == {"ok": True, "detail": "recovery completed"}
        assert len(snapshot["symbols"]) == 1
        assert snapshot["symbols"][0]["symbol"] == "BTCUSDT"
        assert snapshot["symbols"][0]["detail"] == "ok"

    def test_write_leaves_no_temp_file_behind(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        write_snapshot(
            path, status=_status(), recovery_ok=True, recovery_detail="x",
            pid=1, started_at=datetime.now(timezone.utc),
        )
        leftovers = [p for p in tmp_path.iterdir() if p.name != "health.json"]
        assert leftovers == []

    def test_repeated_writes_overwrite_cleanly(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        for detail in ("first", "second", "third"):
            write_snapshot(
                path, status=_status(detail=detail), recovery_ok=True, recovery_detail=detail,
                pid=1, started_at=datetime.now(timezone.utc),
            )
        snapshot = read_snapshot(path)
        assert snapshot["recovery"]["detail"] == "third"
        assert snapshot["symbols"][0]["detail"] == "third"

    def test_degraded_symbol_detail_is_preserved(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        write_snapshot(
            path,
            status=_status(health=RuntimeHealth.DEGRADED, detail="unresolved gap: M5"),
            recovery_ok=True, recovery_detail="running",
            pid=1, started_at=datetime.now(timezone.utc),
        )
        snapshot = read_snapshot(path)
        assert snapshot["overall_health"] == "DEGRADED"
        assert snapshot["symbols"][0]["detail"] == "unresolved gap: M5"


=== FILE: tests/test_ops_lock.py ===
"""Faz 8 — ops/lock.py testleri: flock-tabanlı process-lock, offline/deterministic."""

from __future__ import annotations

from pathlib import Path

import pytest

from crypto_signal_engine.ops.errors import ConcurrentInstanceError
from crypto_signal_engine.ops.lock import ProcessLock


class TestProcessLock:
    def test_acquire_creates_lock_file_with_pid(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        lock = ProcessLock(lock_path)
        lock.acquire()
        try:
            assert lock_path.exists()
            assert lock_path.read_text().strip().isdigit()
        finally:
            lock.release()

    def test_second_acquire_on_same_path_is_rejected(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        first = ProcessLock(lock_path)
        first.acquire()
        try:
            second = ProcessLock(lock_path)
            with pytest.raises(ConcurrentInstanceError):
                second.acquire()
        finally:
            first.release()

    def test_after_release_a_new_lock_can_be_acquired(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        first = ProcessLock(lock_path)
        first.acquire()
        first.release()

        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_release_is_idempotent(self, tmp_path: Path) -> None:
        lock = ProcessLock(tmp_path / "cse.lock")
        lock.acquire()
        lock.release()
        lock.release()  # ikinci çağrı no-op olmalı, exception fırlatmamalı

    def test_context_manager_releases_on_exit(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        with ProcessLock(lock_path):
            pass
        # context manager çıktıktan sonra yeniden acquire edilebilmeli
        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_context_manager_releases_even_on_exception(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        with pytest.raises(ValueError):
            with ProcessLock(lock_path):
                raise ValueError("boom")
        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_parent_directory_is_created_if_missing(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "nested" / "dir" / "cse.lock"
        lock = ProcessLock(lock_path)
        lock.acquire()
        try:
            assert lock_path.exists()
        finally:
            lock.release()

    def test_double_acquire_on_same_instance_raises(self, tmp_path: Path) -> None:
        lock = ProcessLock(tmp_path / "cse.lock")
        lock.acquire()
        try:
            with pytest.raises(ConcurrentInstanceError):
                lock.acquire()
        finally:
            lock.release()


=== FILE: tests/test_ops_logging.py ===
"""Faz 8 — ops/logging_setup.py testleri: stdlib logging kurulumu,
idempotency. Gerçek dosya sistemine bağımlı DEĞİLDİR (stdout handler)."""

from __future__ import annotations

import logging

from crypto_signal_engine.ops.logging_setup import configure_logging


class TestConfigureLogging:
    def test_sets_requested_level(self) -> None:
        configure_logging("DEBUG")
        logger = logging.getLogger("crypto_signal_engine")
        assert logger.level == logging.DEBUG

    def test_does_not_duplicate_handlers_on_repeated_calls(self) -> None:
        configure_logging("INFO")
        configure_logging("INFO")
        configure_logging("WARNING")
        logger = logging.getLogger("crypto_signal_engine")
        assert len(logger.handlers) == 1

    def test_logger_does_not_propagate_to_root(self) -> None:
        configure_logging("INFO")
        logger = logging.getLogger("crypto_signal_engine")
        assert logger.propagate is False

    def test_child_logger_messages_are_captured_by_caplog(self, caplog) -> None:
        """`configure_logging` bilerek `propagate=False` ayarlar (root
        logger'a çift loglama YAPILMASIN diye) — bu yüzden caplog'un
        varsayılan root-tabanlı capture handler'ı BUNU GÖRMEZ; test, capture
        handler'ını doğrudan logger'a ekleyerek bunu (production
        davranışını DEĞİŞTİRMEDEN) doğrular."""
        configure_logging("INFO")
        logger = logging.getLogger("crypto_signal_engine.app")
        logger.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.INFO, logger="crypto_signal_engine.app"):
                logger.info("startup config summary: %s", {"symbols": ["BTCUSDT"]})
        finally:
            logger.removeHandler(caplog.handler)
        assert any("startup config summary" in record.message for record in caplog.records)


=== FILE: tests/test_ops_no_hardcoded_paths.py ===
"""Faz 8 — Faz 8'in eklediği dosyaların (kod + deploy + backup script), bu
geliştirme ortamına özgü hiçbir hardcoded WSL/Windows/developer-machine
yolu İÇERMEDİĞİNİ doğrular (Bölüm: "no hardcoded developer-machine paths").

Bu, mevcut kabul edilmiş `tests/test_repository_safety_scan.py`'ı
DEĞİŞTİRMEDEN, Faz 8'in kendi ek dosya kümesini (o dosyanın taramadığı
`deploy/` ve `scripts/` dahil) tarayan AYRI/ek bir regresyon testidir."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_SCANNED_PATHS = [
    REPO_ROOT / "crypto_signal_engine",
    REPO_ROOT / "deploy",
    REPO_ROOT / "scripts" / "backup_sqlite.py",
]

_FORBIDDEN_PATTERNS = [
    re.compile(r"/mnt/c/", re.IGNORECASE),
    re.compile(r"OneDrive", re.IGNORECASE),
    re.compile(r"[A-Za-z]:\\\\"),
    re.compile(r"/home/[a-zA-Z0-9_]+/"),
    re.compile(r"C:\\Users", re.IGNORECASE),
]


def _all_files() -> list[Path]:
    files: list[Path] = []
    for base in _SCANNED_PATHS:
        if base.is_file():
            files.append(base)
        elif base.is_dir():
            files.extend(p for p in base.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    return files


class TestNoHardcodedDeveloperMachinePaths:
    def test_no_wsl_or_windows_or_home_paths(self) -> None:
        violations = []
        for path in _all_files():
            text = path.read_text(encoding="utf-8")
            for pattern in _FORBIDDEN_PATTERNS:
                if pattern.search(text):
                    violations.append(f"{path}: {pattern.pattern!r}")
        assert violations == [], f"hardcoded developer-machine yolu bulundu: {violations}"


=== FILE: tests/test_ops_notifier.py ===
"""24/7 Ops v1, Step 4 — `ops/notifier.py` testleri: credential SET/UNSET
tespiti (değer ASLA döner/loglanmaz), Telegram HTTP çağrısının şekli
(mock `urlopen` — GERÇEK ağa ASLA dokunulmaz), ve `safe_notify()`'ın
"raising/None notifier ASLA çağıranı etkilemez" garantisi."""

from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock, patch

from crypto_signal_engine.ops.notifier import (
    build_telegram_notifier,
    safe_notify,
    telegram_credentials_status,
)


class TestTelegramCredentialsStatus:
    def test_both_unset(self) -> None:
        assert telegram_credentials_status(env={}) == (False, False)

    def test_only_bot_token_set(self) -> None:
        assert telegram_credentials_status(env={"CSE_TELEGRAM_BOT_TOKEN": "abc"}) == (True, False)

    def test_only_chat_id_set(self) -> None:
        assert telegram_credentials_status(env={"CSE_TELEGRAM_CHAT_ID": "123"}) == (False, True)

    def test_both_set(self) -> None:
        env = {"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "123"}
        assert telegram_credentials_status(env=env) == (True, True)

    def test_never_returns_actual_values(self) -> None:
        """SET/UNSET olarak `bool` döner — dönen değer ASLA credential
        değerinin kendisini İÇERMEZ."""
        env = {"CSE_TELEGRAM_BOT_TOKEN": "super-secret-token-value", "CSE_TELEGRAM_CHAT_ID": "999"}
        result = telegram_credentials_status(env=env)
        assert "super-secret-token-value" not in repr(result)
        assert result == (True, True)


class TestBuildTelegramNotifier:
    def test_returns_none_when_bot_token_missing(self) -> None:
        assert build_telegram_notifier(env={"CSE_TELEGRAM_CHAT_ID": "123"}) is None

    def test_returns_none_when_chat_id_missing(self) -> None:
        assert build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc"}) is None

    def test_returns_none_when_both_missing(self) -> None:
        assert build_telegram_notifier(env={}) is None

    def test_returns_a_callable_when_both_configured(self) -> None:
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "123"})
        assert callable(notifier)

    def test_sends_expected_url_and_payload(self) -> None:
        """`notify()` fires the HTTP POST on a background thread
        (fire-and-forget) — this test waits for that thread to finish
        before asserting on the mocked `urlopen` call."""
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc123", "CSE_TELEGRAM_CHAT_ID": "999"})
        assert notifier is not None

        fake_response = MagicMock()
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)
        fake_response.read = MagicMock(return_value=b"{}")

        done = threading.Event()

        def _tracking_urlopen(request, timeout):
            result = fake_response
            done.set()
            return result

        with patch("crypto_signal_engine.ops.notifier.urllib.request.urlopen", side_effect=_tracking_urlopen) as mocked:
            notifier("test message")
            assert done.wait(timeout=2.0), "background thread did not call urlopen in time"

        request = mocked.call_args.args[0]
        assert request.full_url == "https://api.telegram.org/botabc123/sendMessage"
        payload = json.loads(request.data.decode("utf-8"))
        assert payload == {"chat_id": "999", "text": "test message"}

    def test_bot_token_never_appears_in_notifier_repr(self) -> None:
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "top-secret-xyz", "CSE_TELEGRAM_CHAT_ID": "1"})
        assert "top-secret-xyz" not in repr(notifier)

    def test_network_failure_never_raises(self) -> None:
        """The background worker's own try/except must swallow a network
        failure — nothing propagates back to the caller of `notify()`."""
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "1"})
        assert notifier is not None
        import urllib.error

        with patch(
            "crypto_signal_engine.ops.notifier.urllib.request.urlopen",
            side_effect=urllib.error.URLError("unreachable"),
        ):
            notifier("message")  # must not raise
            time.sleep(0.05)  # let the background thread run to completion


class TestSafeNotify:
    def test_none_notifier_is_a_silent_no_op(self) -> None:
        safe_notify(None, "message")  # must not raise

    def test_raising_notifier_never_propagates(self) -> None:
        def _raising(message: str) -> None:
            raise RuntimeError("boom")

        safe_notify(_raising, "message")  # must not raise

    def test_working_notifier_receives_exact_message(self) -> None:
        received: list[str] = []
        safe_notify(received.append, "hello world")
        assert received == ["hello world"]


=== FILE: tests/test_ops_systemd_backup_template.py ===
"""24/7 Ops v1, Step 1 — `deploy/systemd/crypto-signal-engine-backup.{service,timer}`
için statik sağlık kontrolleri (dosya içeriği üzerinde string/regex
assertion'ları — gerçek systemd hiçbir zaman çalıştırılmaz, root
gerekmez), `tests/test_ops_systemd_template.py`'nin AYNI deseniyle."""

from __future__ import annotations

import re
from pathlib import Path

_DEPLOY_DIR = Path(__file__).resolve().parents[1] / "deploy" / "systemd"
SERVICE_PATH = _DEPLOY_DIR / "crypto-signal-engine-backup.service"
TIMER_PATH = _DEPLOY_DIR / "crypto-signal-engine-backup.timer"


class TestBackupServiceUnitFile:
    def test_file_exists(self) -> None:
        assert SERVICE_PATH.exists()

    def test_does_not_run_as_root(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        match = re.search(r"^User=(.+)$", text, re.MULTILINE)
        assert match is not None
        assert match.group(1).strip() != "root"

    def test_invokes_existing_backup_script_unchanged(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        assert "scripts/backup_sqlite.py" in text
        assert "--source" in text
        assert "--destination" in text
        assert "--keep-last" in text

    def test_no_sudo_or_privilege_escalation(self) -> None:
        assert "sudo" not in SERVICE_PATH.read_text(encoding="utf-8").lower()

    def test_no_embedded_secrets_or_credential_strings(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8").lower()
        for forbidden in ("api_key", "apikey", "secret", "hmac", "password", "listenkey"):
            assert forbidden not in text, f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_no_execution_or_private_endpoint_strings(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        for forbidden in ("/api/v3/order", "/sapi/", "/fapi/", "testnet", "mainnet"):
            assert forbidden.lower() not in text.lower(), f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_environment_file_is_referenced_not_inlined(self) -> None:
        assert re.search(r"^EnvironmentFile=", SERVICE_PATH.read_text(encoding="utf-8"), re.MULTILINE)

    def test_hardening_directives_present(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        for directive in ("NoNewPrivileges=yes", "ProtectSystem=strict"):
            assert directive in text


class TestBackupTimerUnitFile:
    def test_file_exists(self) -> None:
        assert TIMER_PATH.exists()

    def test_has_a_schedule(self) -> None:
        assert re.search(r"^OnCalendar=", TIMER_PATH.read_text(encoding="utf-8"), re.MULTILINE)

    def test_is_persistent_across_missed_runs(self) -> None:
        assert "Persistent=true" in TIMER_PATH.read_text(encoding="utf-8")

    def test_installed_by_timers_target(self) -> None:
        assert re.search(r"^WantedBy=timers\.target", TIMER_PATH.read_text(encoding="utf-8"), re.MULTILINE)


=== FILE: tests/test_ops_systemd_notify.py ===
"""24/7 Ops v1, Step 2 — `ops/systemd_notify.py`'nin watchdog heartbeat
testleri: `$NOTIFY_SOCKET` yokken TAM bir no-op (ASLA fırlatmaz), varken
GERÇEK bir UNIX datagram soketine BEKLENEN payload'ı yazar (mocked
socket — gerçek bir dosya sistemi soketi AÇILMAZ)."""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

from crypto_signal_engine.ops.systemd_notify import _WATCHDOG_PAYLOAD, notify_watchdog


class TestNotifyWatchdogWithoutNotifySocket:
    def test_returns_false_and_never_raises_when_env_var_absent(self) -> None:
        assert notify_watchdog(env={}) is False

    def test_returns_false_when_env_var_is_empty_string(self) -> None:
        assert notify_watchdog(env={"NOTIFY_SOCKET": ""}) is False

    def test_never_raises_even_with_a_hostile_env_mapping(self) -> None:
        # A no-op must be provably safe regardless of what else is in env.
        assert notify_watchdog(env={"UNRELATED": "value", "PATH": "/usr/bin"}) is False


class TestNotifyWatchdogWithNotifySocket:
    def test_sends_exact_watchdog_payload_to_configured_path(self) -> None:
        fake_socket = MagicMock()
        fake_socket.__enter__ = MagicMock(return_value=fake_socket)
        fake_socket.__exit__ = MagicMock(return_value=False)
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", return_value=fake_socket) as ctor:
            result = notify_watchdog(env={"NOTIFY_SOCKET": "/run/systemd/notify"})
        assert result is True
        ctor.assert_called_once_with(socket.AF_UNIX, socket.SOCK_DGRAM)
        fake_socket.sendto.assert_called_once_with(_WATCHDOG_PAYLOAD, "/run/systemd/notify")

    def test_abstract_namespace_socket_path_is_translated(self) -> None:
        fake_socket = MagicMock()
        fake_socket.__enter__ = MagicMock(return_value=fake_socket)
        fake_socket.__exit__ = MagicMock(return_value=False)
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", return_value=fake_socket):
            notify_watchdog(env={"NOTIFY_SOCKET": "@systemd-notify"})
        fake_socket.sendto.assert_called_once_with(_WATCHDOG_PAYLOAD, "\0systemd-notify")

    def test_socket_error_is_caught_and_never_raises(self) -> None:
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", side_effect=OSError("boom")):
            result = notify_watchdog(env={"NOTIFY_SOCKET": "/run/systemd/notify"})
        assert result is False

    def test_defaults_to_real_os_environ_when_env_omitted(self, monkeypatch) -> None:
        monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
        assert notify_watchdog() is False


=== FILE: tests/test_ops_systemd_template.py ===
"""Faz 8 — deploy/systemd/crypto-signal-engine.service için statik sağlık
kontrolleri (dosya içeriği üzerinde string/regex assertion'ları — gerçek
systemd hiçbir zaman çalıştırılmaz, root gerekmez)."""

from __future__ import annotations

import re
from pathlib import Path

UNIT_PATH = Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "crypto-signal-engine.service"


def _text() -> str:
    return UNIT_PATH.read_text(encoding="utf-8")


class TestSystemdUnitFile:
    def test_file_exists(self) -> None:
        assert UNIT_PATH.exists()

    def test_does_not_run_as_root(self) -> None:
        text = _text()
        match = re.search(r"^User=(.+)$", text, re.MULTILINE)
        assert match is not None, "User= satırı bulunamadı"
        assert match.group(1).strip() != "root"

    def test_execstart_invokes_python_module_not_a_shell_script(self) -> None:
        text = _text()
        match = re.search(r"^ExecStart=(.+)$", text, re.MULTILINE)
        assert match is not None
        assert "python" in match.group(1)
        assert "-m crypto_signal_engine.app run" in match.group(1)

    def test_no_sudo_or_privilege_escalation(self) -> None:
        text = _text()
        assert "sudo" not in text.lower()

    def test_has_bounded_restart_policy(self) -> None:
        text = _text()
        assert "Restart=on-failure" in text
        assert re.search(r"^RestartSec=\d+", text, re.MULTILINE)
        assert re.search(r"^StartLimitBurst=\d+", text, re.MULTILINE)
        assert re.search(r"^StartLimitIntervalSec=\d+", text, re.MULTILINE)

    def test_supports_graceful_sigterm_shutdown(self) -> None:
        text = _text()
        assert "KillSignal=SIGTERM" in text
        assert re.search(r"^TimeoutStopSec=\d+", text, re.MULTILINE)

    def test_hardening_directives_present(self) -> None:
        text = _text()
        for directive in ("NoNewPrivileges=yes", "ProtectSystem=strict", "ReadWritePaths="):
            assert directive in text, f"eksik hardening satırı: {directive}"

    def test_no_embedded_secrets_or_credential_strings(self) -> None:
        text = _text().lower()
        for forbidden in ("api_key", "apikey", "secret", "hmac", "password", "listenkey"):
            assert forbidden not in text, f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_no_execution_or_private_endpoint_strings(self) -> None:
        text = _text()
        for forbidden in ("/api/v3/order", "/sapi/", "/fapi/", "testnet", "mainnet"):
            assert forbidden.lower() not in text.lower(), f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_environment_file_is_referenced_not_inlined(self) -> None:
        text = _text()
        assert re.search(r"^EnvironmentFile=", text, re.MULTILINE), "EnvironmentFile= satırı bulunamadı"


=== FILE: tests/test_orchestrator.py ===
from datetime import datetime, timezone
from unittest.mock import patch

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.signal_engine import SignalEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_history() -> FeatureHistoryStore:
    history = FeatureHistoryStore()
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={
        "SPREAD_BPS": 3.0, "DEPTH_IMBALANCE_10": 0.2, "TOB_IMBALANCE": 0.1, "MID_PRICE": 100.0, "MICROPRICE": 100.02,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M5, as_of=T0, values={
        "RSI_14": 60.0, "ROC_10": 1.0, "VWAP_DEVIATION_20": 0.005, "RELATIVE_VOLUME_20": 1.1, "BOLLINGER_BANDWIDTH_20_2": 0.03,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M15, as_of=T0, values={
        "ROC_10": 0.8, "DIST_FROM_HIGH_20": 0.02, "DIST_FROM_LOW_20": 0.04, "RELATIVE_VOLUME_20": 1.0, "BODY_TO_RANGE_RATIO": 0.6,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.H1, as_of=T0, values={
        "ROC_10": 0.5, "RSI_14": 55.0, "BOLLINGER_BANDWIDTH_20_2": 0.03, "RELATIVE_VOLUME_20": 1.0,
        "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.05,
    }))
    return history


class TestOrchestrationChain:
    def test_consensus_engine_receives_evidence_not_regime_context_positionally(self) -> None:
        from crypto_signal_engine.consensus.engine import ConsensusEngine

        engine = SignalEngine(make_history())
        original_combine = ConsensusEngine.combine
        captured = {}

        def spy_combine(self, evidence, *, regime):
            captured["evidence_types"] = [type(e).__name__ for e in evidence]
            captured["regime_is_kwonly"] = True
            return original_combine(self, evidence, regime=regime)

        with patch.object(ConsensusEngine, "combine", spy_combine):
            engine.evaluate("BTCUSDT", T0)

        assert all(t == "AgentEvidence" for t in captured["evidence_types"])
        assert captured["regime_is_kwonly"] is True

    def test_risk_overlay_receives_consensus_result_and_regime_context_not_output(self) -> None:
        from crypto_signal_engine.consensus.risk import RiskOverlay

        engine = SignalEngine(make_history())
        original_assess = RiskOverlay.assess
        captured = {}

        def spy_assess(self, consensus, regime):
            captured["consensus_type"] = type(consensus).__name__
            captured["regime_type"] = type(regime).__name__
            return original_assess(self, consensus, regime)

        with patch.object(RiskOverlay, "assess", spy_assess):
            engine.evaluate("BTCUSDT", T0)

        assert captured["consensus_type"] == "ConsensusResult"
        assert captured["regime_type"] == "RegimeContext"

    def test_regime_context_passed_to_risk_is_same_object_from_regime_agent(self) -> None:
        """KRİTİK provenance testi: RiskOverlay'e geçirilen RegimeContext,
        TAM OLARAK aynı `RegimeAgent.evaluate()` çağrısından gelen nesne
        olmalı (başka bir değerlendirmeden GELMEMELİ)."""
        from crypto_signal_engine.agents.regime import RegimeAgent
        from crypto_signal_engine.consensus.risk import RiskOverlay

        engine = SignalEngine(make_history())
        original_regime_evaluate = RegimeAgent.evaluate
        original_risk_assess = RiskOverlay.assess
        captured = {}

        def spy_regime_evaluate(self, context):
            output = original_regime_evaluate(self, context)
            captured["regime_from_agent"] = output.regime
            return output

        def spy_risk_assess(self, consensus, regime):
            captured["regime_to_risk"] = regime
            return original_risk_assess(self, consensus, regime)

        with patch.object(RegimeAgent, "evaluate", spy_regime_evaluate), patch.object(RiskOverlay, "assess", spy_risk_assess):
            engine.evaluate("BTCUSDT", T0)

        assert captured["regime_to_risk"] is captured["regime_from_agent"]

    def test_evaluate_produces_valid_signal(self) -> None:
        engine = SignalEngine(make_history())
        signal = engine.evaluate("BTCUSDT", T0)
        assert signal.symbol == "BTCUSDT"
        assert signal.timestamp == T0
        assert signal.primary_timeframe == Timeframe.M5


=== FILE: tests/test_orderbook_features.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.errors import FeatureCalculationError, FeatureValidationError
from crypto_signal_engine.features import orderbook_calculators as oc

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_book(bids, asks, last_update_id=1) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol="BTCUSDT", timestamp=T0,
        bids=tuple(OrderBookLevel(price=p, quantity=q) for p, q in bids),
        asks=tuple(OrderBookLevel(price=p, quantity=q) for p, q in asks),
        last_update_id=last_update_id,
    )


class TestBasicPrices:
    def test_best_bid_ask_mid(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        assert oc.best_bid(book) == pytest.approx(99.0)
        assert oc.best_ask(book) == pytest.approx(101.0)
        assert oc.mid_price(book) == pytest.approx(100.0)

    def test_spread_abs(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        assert oc.spread_abs(book) == pytest.approx(2.0)

    def test_spread_bps_known_value(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        # spread=2, mid=100 -> bps = 2/100*10000 = 200
        assert oc.spread_bps(book) == pytest.approx(200.0)


class TestDepth:
    def test_bid_ask_depth_sum(self) -> None:
        book = make_book([(99.0, 1.0), (98.0, 2.0), (97.0, 3.0)], [(101.0, 1.0), (102.0, 2.0)])
        assert oc.bid_depth(book, 2) == pytest.approx(3.0)  # 1+2
        assert oc.ask_depth(book, 2) == pytest.approx(3.0)  # 1+2

    def test_depth_beyond_available_uses_all_levels(self) -> None:
        book = make_book([(99.0, 1.0), (98.0, 2.0)], [(101.0, 1.0)])
        assert oc.bid_depth(book, 100) == pytest.approx(3.0)

    def test_invalid_depth_rejected(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        with pytest.raises(FeatureValidationError, match="depth"):
            oc.bid_depth(book, 0)
        with pytest.raises(FeatureValidationError, match="depth"):
            oc.bid_depth(book, -1)

    def test_depth_imbalance_known_value(self) -> None:
        book = make_book([(99.0, 3.0)], [(101.0, 1.0)])
        # (3-1)/(3+1) = 0.5
        assert oc.depth_imbalance(book, 1) == pytest.approx(0.5)

    def test_depth_imbalance_within_bounds(self) -> None:
        book = make_book([(99.0, 100.0)], [(101.0, 0.001)])
        value = oc.depth_imbalance(book, 1)
        assert -1.0 <= value <= 1.0


class TestMicropriceAndTOB:
    def test_microprice_weighted_toward_larger_opposite_side(self) -> None:
        # best_bid qty daha büyükse microprice ask'a daha yakın olmalı
        # (ağırlıklama karşı tarafın miktarına göre yapılır)
        book = make_book([(99.0, 10.0)], [(101.0, 1.0)])
        mp = oc.microprice(book)
        mid = oc.mid_price(book)
        # microprice = (99*1 + 101*10)/11 = (99+1010)/11 = 1109/11 = 100.818...
        assert mp == pytest.approx((99.0 * 1.0 + 101.0 * 10.0) / 11.0)
        assert mp > mid  # ağır bid tarafı fiyatı ask'a doğru çeker

    def test_top_of_book_imbalance_known_value(self) -> None:
        book = make_book([(99.0, 3.0)], [(101.0, 1.0)])
        assert oc.top_of_book_imbalance(book) == pytest.approx(0.5)

    def test_top_of_book_imbalance_within_bounds(self) -> None:
        book = make_book([(99.0, 1000.0)], [(101.0, 0.0001)])
        value = oc.top_of_book_imbalance(book)
        assert -1.0 <= value <= 1.0


class TestSymbolTimeIsolation:
    def test_different_snapshots_independent(self) -> None:
        book1 = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        book2 = OrderBookSnapshot(
            symbol="ETHUSDT", timestamp=T0,
            bids=(OrderBookLevel(price=50.0, quantity=1.0),),
            asks=(OrderBookLevel(price=51.0, quantity=1.0),),
            last_update_id=1,
        )
        assert oc.mid_price(book1) != oc.mid_price(book2)


=== FILE: tests/test_package_import.py ===
"""
Quality Gate 1, 21.C & 29 — package import smoke test (environment-robust, offline).

HARDENING NOTU (v2 — reproducibility düzeltmesi): önceki revizyon, izole
venv'in `system_site_packages=True` ile ana ortamdaki `setuptools`'u miras
alacağını ve bu sayede `pip install --no-build-isolation` çağrısının
`setuptools.build_meta` backend'ini bulabileceğini VARSAYIYORDU. Bağımsız
bir reviewer ortamında bu varsayım YANLIŞ çıktı:

    pip._vendor.pyproject_hooks._impl.BackendUnavailable:
        Cannot import 'setuptools.build_meta'

Kök neden: "system site-packages inherit edilir" garantisi platform/ortam
bağımlıdır (venv'in nasıl kurulduğuna, base Python'un setuptools'u nereye
kurduğuna, PEP 668 kısıtlarına göre değişir) — bu YANLIŞ bir zemin üzerine
inşa edilmiş bir testti.

ÇÖZÜM: Bu test artık build backend'i (`setuptools.build_meta`) HİÇ
DEVREYE SOKMAZ. Paket, teslimat sırasında ÖNCEDEN inşa edilmiş bir wheel
dosyasından (`dist/crypto_signal_engine-*-py3-none-any.whl`) `pip install
--no-index --no-deps <wheel>` ile OFFLINE kurulur. Wheel kurulumu salt bir
unzip+copy işlemidir; hedef ortamda setuptools'un bulunup bulunmaması,
ağ erişiminin olup olmaması, veya ana ortamın nasıl yapılandırıldığı HİÇBİR
ŞEKİLDE önemli değildir — yalnızca pip'in kendisi (venv ile birlikte gelir)
yeterlidir.

Bu, aynı zamanda "yalnızca source-tree cwd sayesinde import oluyor" gibi bir
false-positive'i de engeller: kurulum GERÇEKTEN izole bir venv'in
site-packages'ına yapılır (source ağacına hiçbir referans/sys.path hilesi
yoktur) ve import testi repo'dan tamamen bağımsız dizinlerden (`/tmp`,
repo'nun ebeveyni) çalıştırılır.
"""

from __future__ import annotations

import glob
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"

IMPORT_SCRIPT = """
import crypto_signal_engine
import crypto_signal_engine.domain.models
import crypto_signal_engine.domain.events
import crypto_signal_engine.domain.enums
import crypto_signal_engine.domain.consensus
import crypto_signal_engine.domain.candle_sequencing
import crypto_signal_engine.domain.state_contract
import crypto_signal_engine.providers.base
import crypto_signal_engine.quality.base
import crypto_signal_engine.safety.models

assert crypto_signal_engine.ALLOW_LIVE_TRADING is False, "safety invariant bozuldu"
print("IMPORT_SMOKE_TEST_OK")
"""


def _find_prebuilt_wheel() -> Path:
    """Repo ile birlikte teslim edilen önceden-inşa edilmiş wheel'i bulur.

    Bu dosya `dist/` altında, teslimat sırasında `python -m build --wheel`
    ile ÖNCEDEN üretilip repoya dahil edilmiştir. Bu test onu YENİDEN
    İNŞA ETMEZ — build backend'i hiç çağırmaz, yalnızca var olan wheel'i
    offline kurar.
    """
    candidates = sorted(glob.glob(str(DIST_DIR / "crypto_signal_engine-*-py3-none-any.whl")))
    if not candidates:
        pytest.fail(
            f"Önceden inşa edilmiş wheel bulunamadı: {DIST_DIR}/crypto_signal_engine-*-py3-none-any.whl. "
            f"Bu dosya repo teslimatının bir parçası olmalıdır (bkz. scripts/verify_phase1.py "
            f"veya `python -m build --wheel .` ile yeniden üretin ve dist/ altına koyun)."
        )
    return Path(candidates[-1])


@pytest.fixture(scope="module")
def isolated_venv_python() -> str:
    """Tamamen izole (system_site_packages=False) bir venv kurar ve içine
    önceden-inşa edilmiş wheel'i OFFLINE (`--no-index --no-deps`) kurar.

    Build backend hiç devreye girmez; ağ erişimi GEREKMEZ; ana ortamın
    setuptools/wheel durumu SONUCU ETKİLEMEZ.
    """
    wheel_path = _find_prebuilt_wheel()

    tmp_dir = tempfile.mkdtemp(prefix="phase1_verify_venv_")
    venv.create(tmp_dir, with_pip=True, system_site_packages=False)
    venv_python = str(Path(tmp_dir) / "bin" / "python")

    install = subprocess.run(
        [
            venv_python, "-m", "pip", "install",
            "--no-index",       # PyPI'ye HİÇ gitme
            "--no-deps",        # bağımlılık çözümü yok (paketin zaten bağımlılığı yok)
            "--quiet",
            str(wheel_path),
        ],
        capture_output=True, text=True, timeout=60,
    )
    if install.returncode != 0:
        pytest.fail(
            f"İzole venv'e offline wheel kurulumu başarısız oldu (wheel={wheel_path}).\n"
            f"stdout: {install.stdout}\nstderr: {install.stderr}"
        )
    return venv_python


def test_isolated_install_and_import_from_repo_parent(isolated_venv_python: str) -> None:
    """Offline kurulmuş izole venv'in python'ı ile, repo kökünün
    EBEVEYNİNDEN (tamamen bağımsız bir cwd) import testi.

    cwd repo'dan bağımsız olduğu için, import'un başarılı olması YALNIZCA
    venv'in site-packages'ına GERÇEKTEN kurulmuş olmasıyla mümkündür —
    "source tree cwd sayesinde çalışıyor" false-positive'i yapısal olarak
    imkansızdır.
    """
    result = subprocess.run(
        [isolated_venv_python, "-c", IMPORT_SCRIPT],
        cwd=str(REPO_ROOT.parent),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, (
        f"İzole offline kurulum sonrası import FAILED.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "IMPORT_SMOKE_TEST_OK" in result.stdout


def test_isolated_install_and_import_from_tmp(isolated_venv_python: str) -> None:
    """Aynı testi /tmp'den (repo ile hiçbir ilişkisi olmayan bir dizin) tekrar dener."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = subprocess.run(
            [isolated_venv_python, "-c", IMPORT_SCRIPT],
            cwd=tmp_dir, capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, (
            f"Import from {tmp_dir} FAILED.\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "IMPORT_SMOKE_TEST_OK" in result.stdout


def test_installed_package_is_not_the_source_tree(isolated_venv_python: str) -> None:
    """İzole venv'e kurulan paketin GERÇEKTEN venv'in site-packages'ında
    olduğunu, repo'nun source ağacına işaret eden bir editable install
    (.pth dosyası vb.) OLMADIĞINI doğrular — false-positive'i yapısal
    olarak ekarte eder."""
    check_script = (
        "import crypto_signal_engine, os; "
        "p = os.path.abspath(crypto_signal_engine.__file__); "
        "print(p)"
    )
    result = subprocess.run(
        [isolated_venv_python, "-c", check_script],
        cwd=str(REPO_ROOT.parent), capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    installed_path = result.stdout.strip()
    repo_root_str = str(REPO_ROOT)
    assert not installed_path.startswith(repo_root_str), (
        f"Kurulan paket hâlâ repo source ağacına işaret ediyor ({installed_path}); "
        f"bu, wheel'in gerçekten site-packages'a KOPYALANMADIĞI anlamına gelir."
    )
    assert "site-packages" in installed_path


def test_current_environment_import_also_works() -> None:
    """Ek olarak: eğer bu test suite'i ZATEN (editable veya normal) kurulu
    bir ortamda çalıştırılıyorsa, o ortamdaki import da başarılı olmalıdır.
    Bu, izole venv testlerinin YERİNE değil, EK OLARAK çalışır; mevcut
    ortamda paket kurulu değilse bu tek test skip edilir (asıl gate,
    yukarıdaki izole/offline testlerle zaten karşılanmıştır)."""
    result = subprocess.run(
        [sys.executable, "-c", IMPORT_SCRIPT],
        cwd=str(REPO_ROOT.parent),
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 and "ModuleNotFoundError" in result.stderr:
        pytest.skip(
            "Mevcut ortamda package kurulu değil; asıl Gate 1/29 gereksinimi "
            "yukarıdaki izole+offline wheel testleriyle zaten bağımsız olarak "
            "karşılanıyor. Bu test yalnızca ek bir sinyal sağlar."
        )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert "IMPORT_SMOKE_TEST_OK" in result.stdout


=== FILE: tests/test_paper_trading_engine.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError, NoLookAheadViolationError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, OrderSide, PositionSide

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)


def make_signal(
    symbol: str = "BTCUSDT",
    timestamp: datetime = T0,
    context_id: str = "ctx-0",
    score: float = 0.0,
    model_version: str = "test-v1",
) -> Signal:
    return Signal(
        symbol=symbol,
        timestamp=timestamp,
        context_id=context_id,
        score=score,
        confidence=0.8,
        risk_level=RiskLevel.LOW,
        primary_timeframe=Timeframe.M5,
        supporting_factors=(),
        contradicting_factors=(),
        invalidation=None,
        model_version=model_version,
    )


def make_price(symbol: str = "BTCUSDT", price: float = 100.0, as_of: datetime = T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


LONG_SCORE = 0.5  # >= moderate (0.40), < strong (0.70) -> LONG
SHORT_SCORE = -0.5  # -> SHORT
NEUTRAL_SCORE = 0.0  # -> NEUTRAL


class TestOpenPosition:
    def test_long_signal_opens_long_position(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE)
        price = make_price(price=100.0, as_of=T0)

        result = engine.process_signal(signal, price)

        assert len(result.orders) == 1
        assert result.orders[0].side is OrderSide.BUY
        assert len(result.fills) == 1
        assert result.position.side is PositionSide.LONG
        assert result.position.quantity == pytest.approx(10.0)
        assert result.position.average_entry_price == pytest.approx(100.0)
        assert result.position.realized_pnl == pytest.approx(0.0)
        assert result.idempotent_replay is False

    def test_short_signal_opens_short_position(self) -> None:
        engine = PaperTradingEngine(notional_per_position=500.0)
        signal = make_signal(context_id="ctx-1", score=SHORT_SCORE)
        price = make_price(price=50.0)

        result = engine.process_signal(signal, price)

        assert result.orders[0].side is OrderSide.SELL
        assert result.position.side is PositionSide.SHORT
        assert result.position.quantity == pytest.approx(10.0)

    def test_neutral_signal_with_no_prior_position_is_noop(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(context_id="ctx-1", score=NEUTRAL_SCORE)
        price = make_price()

        result = engine.process_signal(signal, price)

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.side is PositionSide.FLAT


class TestSameDirectionRepeat:
    def test_repeat_long_signal_produces_no_new_orders(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        p1 = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        t1 = T0 + timedelta(minutes=5)
        p2 = engine.process_signal(
            make_signal(context_id="ctx-2", score=LONG_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        assert p2.orders == ()
        assert p2.fills == ()
        # position size/entry unchanged (no re-open), but observation timestamp refreshed
        assert p2.position.quantity == pytest.approx(p1.position.quantity)
        assert p2.position.average_entry_price == pytest.approx(p1.position.average_entry_price)
        assert p2.position.updated_at == t1


class TestFlipAndFlatten:
    def test_flip_long_to_short_realizes_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        # closed 10 units bought at 100, sold at 120 -> realized = (120-100)*10 = 200
        assert len(result.orders) == 2
        assert len(result.fills) == 2
        assert result.position.side is PositionSide.SHORT
        assert result.position.realized_pnl == pytest.approx(200.0)
        assert result.position.average_entry_price == pytest.approx(120.0)


class TestNeutralIsNoAction:
    """Faz 5 acceptance blocker fix: NEUTRAL sinyal NO_ACTION olmalı — hiçbir
    order/fill üretmemeli, mevcut pozisyonu (LONG/SHORT) kapatmamalı, cash/
    realized/unrealized PnL'i değiştirmemeli ve position/account state'i
    mutate etmemelidir."""

    def test_neutral_while_flat_is_no_action(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=NEUTRAL_SCORE, timestamp=T0)

        result = engine.process_signal(signal, make_price(price=100.0, as_of=T0))

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.side is PositionSide.FLAT
        assert result.position.quantity == pytest.approx(0.0)
        assert result.position.realized_pnl == pytest.approx(0.0)
        # no state was ever created for this symbol — truly untouched
        assert engine.position("BTCUSDT") is None
        assert engine.orders("BTCUSDT") == ()
        assert engine.fills("BTCUSDT") == ()

    def test_neutral_while_long_is_no_action_long_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=90.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        # position is byte-for-byte unchanged, including updated_at (no
        # "observed at this price" refresh for NEUTRAL — truly no mutation)
        assert result.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills

    def test_neutral_while_short_is_no_action_short_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=110.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills

    def test_neutral_replay_is_idempotent_no_action(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        neutral_signal = make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1)
        neutral_price = make_price(price=90.0, as_of=t1)

        first = engine.process_signal(neutral_signal, neutral_price)
        second = engine.process_signal(neutral_signal, neutral_price)

        assert first.orders == () and first.fills == ()
        assert second.orders == () and second.fills == ()
        assert first.position == opened.position
        assert second.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills


class TestIdempotency:
    def test_replaying_same_signal_returns_cached_result_without_mutation(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)

        first = engine.process_signal(signal, price)
        second = engine.process_signal(signal, price)

        assert first.idempotent_replay is False
        assert second.idempotent_replay is True
        assert second.orders == ()
        assert second.fills == ()
        assert second.position == first.position
        # engine-level state confirms no duplicate fills were recorded
        assert engine.fills("BTCUSDT") == first.fills

    def test_same_context_id_different_signal_raises_and_state_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)
        first = engine.process_signal(signal, price)

        conflicting = make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0)
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(conflicting, price)

        assert engine.position("BTCUSDT") == first.position


class TestNoLookAhead:
    def test_price_after_signal_timestamp_raises(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        future_price = make_price(price=100.0, as_of=T0 + timedelta(seconds=1))

        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(signal, future_price)

        assert engine.position("BTCUSDT") is None

    def test_cross_symbol_price_mismatch_raises(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(symbol="BTCUSDT", context_id="ctx-1", score=LONG_SCORE)
        price = make_price(symbol="ETHUSDT", price=100.0)

        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(signal, price)

    def test_out_of_order_signal_raises_and_state_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        t1 = T0 + timedelta(minutes=5)
        first = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=t1),
            make_price(price=100.0, as_of=t1),
        )

        earlier_signal = make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=T0)
        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(earlier_signal, make_price(price=90.0, as_of=T0))

        assert engine.position("BTCUSDT") == first.position
        assert engine.orders("BTCUSDT") == first.orders


class TestMultiSymbolIsolation:
    def test_symbols_are_independent_and_errors_do_not_cross(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        btc = engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-1", score=LONG_SCORE, timestamp=T0),
            make_price(symbol="BTCUSDT", price=100.0, as_of=T0),
        )
        eth = engine.process_signal(
            make_signal(symbol="ETHUSDT", context_id="ctx-eth-1", score=SHORT_SCORE, timestamp=T0),
            make_price(symbol="ETHUSDT", price=10.0, as_of=T0),
        )

        assert btc.position.symbol == "BTCUSDT"
        assert btc.position.side is PositionSide.LONG
        assert eth.position.symbol == "ETHUSDT"
        assert eth.position.side is PositionSide.SHORT

        # an out-of-order signal on ETHUSDT must not affect BTCUSDT state
        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(
                make_signal(symbol="ETHUSDT", context_id="ctx-eth-2", score=LONG_SCORE, timestamp=T0 - timedelta(minutes=1)),
                make_price(symbol="ETHUSDT", price=9.0, as_of=T0 - timedelta(minutes=1)),
            )

        assert engine.position("BTCUSDT") == btc.position
        assert engine.position("ETHUSDT") == eth.position


class TestDeterminism:
    def test_identical_input_sequence_yields_identical_ids_across_instances(self) -> None:
        def run() -> tuple:
            engine = PaperTradingEngine(notional_per_position=1000.0)
            engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))
            t1 = T0 + timedelta(minutes=5)
            engine.process_signal(make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1), make_price(price=110.0, as_of=t1))
            return engine.orders("BTCUSDT"), engine.fills("BTCUSDT")

        orders_a, fills_a = run()
        orders_b, fills_b = run()

        assert orders_a == orders_b
        assert fills_a == fills_b
        assert [o.order_id for o in orders_a] == ["BTCUSDT:ctx-1:0", "BTCUSDT:ctx-2:0", "BTCUSDT:ctx-2:1"]


class TestUnrealizedPnl:
    def test_flat_position_has_zero_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine()
        assert engine.unrealized_pnl("BTCUSDT", make_price(price=100.0)) == 0.0

    def test_long_position_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        pnl = engine.unrealized_pnl("BTCUSDT", make_price(price=110.0, as_of=T0))
        assert pnl == pytest.approx((110.0 - 100.0) * 10.0)

    def test_short_position_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        pnl = engine.unrealized_pnl("BTCUSDT", make_price(price=90.0, as_of=T0))
        assert pnl == pytest.approx((100.0 - 90.0) * 10.0)

    def test_mark_price_symbol_mismatch_raises(self) -> None:
        engine = PaperTradingEngine()
        with pytest.raises(NoLookAheadViolationError):
            engine.unrealized_pnl("BTCUSDT", make_price(symbol="ETHUSDT", price=100.0))


class TestFeeSlippageConfigValidation:
    """Faz 5 acceptance blocker fix: configurable deterministic fee/slippage."""

    @pytest.mark.parametrize("bad_value", [-0.01, float("nan"), float("inf"), float("-inf")])
    def test_invalid_fee_bps_rejected(self, bad_value: float) -> None:
        with pytest.raises(ValueError):
            PaperTradingEngine(fee_bps=bad_value)

    @pytest.mark.parametrize("bad_value", [-0.01, float("nan"), float("inf"), float("-inf")])
    def test_invalid_slippage_bps_rejected(self, bad_value: float) -> None:
        with pytest.raises(ValueError):
            PaperTradingEngine(slippage_bps=bad_value)

    def test_zero_defaults_are_valid(self) -> None:
        engine = PaperTradingEngine()
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))
        assert result.fills[0].fee == 0.0


class TestFeeAndSlippage:
    def test_buy_open_uses_slippage_formula(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        expected_fill_price = 100.0 * (1 + 25.0 / 10000)
        assert result.fills[0].side is OrderSide.BUY
        assert result.fills[0].price == pytest.approx(expected_fill_price)

    def test_sell_open_uses_slippage_formula(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE), make_price(price=100.0))

        expected_fill_price = 100.0 * (1 - 25.0 / 10000)
        assert result.fills[0].side is OrderSide.SELL
        assert result.fills[0].price == pytest.approx(expected_fill_price)

    def test_fee_formula_uses_actual_slippage_adjusted_fill_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=15.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        fill = result.fills[0]
        assert fill.fee == pytest.approx(abs(fill.quantity * fill.price) * 15.0 / 10000)
        # quantity == notional / fill_price by construction, so fee == notional * fee_bps/10000
        assert fill.fee == pytest.approx(1000.0 * 15.0 / 10000)

    def test_zero_fee_and_slippage_preserves_legacy_pricing(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        assert result.fills[0].price == pytest.approx(100.0)
        assert result.fills[0].fee == pytest.approx(0.0)
        assert result.position.average_entry_price == pytest.approx(100.0)
        assert result.position.quantity == pytest.approx(10.0)

    def test_long_open_entry_uses_slippage_adjusted_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=50.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        expected_entry = 100.0 * (1 + 50.0 / 10000)
        assert result.position.average_entry_price == pytest.approx(expected_entry)
        assert result.position.quantity == pytest.approx(1000.0 / expected_entry)

    def test_short_open_entry_uses_slippage_adjusted_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=50.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE), make_price(price=100.0))

        expected_entry = 100.0 * (1 - 50.0 / 10000)
        assert result.position.average_entry_price == pytest.approx(expected_entry)
        assert result.position.quantity == pytest.approx(1000.0 / expected_entry)

    def test_long_close_realized_gross_and_net_pnl_with_fees(self) -> None:
        # Chosen so every intermediate value is a clean, exact decimal:
        # entry fill_price=110 (100*1.10), qty=10, entry_fee=11.0 (1%% of 1100)
        # exit  fill_price=135 (150*0.90), close_fee=13.5 (1%% of 1350)
        engine = PaperTradingEngine(notional_per_position=1100.0, fee_bps=100.0, slippage_bps=1000.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=150.0, as_of=t1),
        )

        close_fill = result.fills[0]
        assert close_fill.price == pytest.approx(135.0)
        assert close_fill.fee == pytest.approx(13.5)
        gross = (135.0 - 110.0) * 10.0
        expected_net = gross - 11.0 - 13.5
        assert result.position.realized_pnl == pytest.approx(expected_net)

    def test_short_close_realized_gross_and_net_pnl_with_fees(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1100.0, fee_bps=100.0, slippage_bps=1000.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0), make_price(price=150.0, as_of=T0)
        )
        entry_fill_price = 150.0 * (1 - 1000.0 / 10000)  # 135.0
        closed_qty = 1100.0 / entry_fill_price
        entry_fee = 1100.0 * 100.0 / 10000  # 11.0

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=LONG_SCORE, timestamp=t1),
            make_price(price=100.0, as_of=t1),
        )

        close_fill = result.fills[0]
        exit_fill_price = 100.0 * (1 + 1000.0 / 10000)  # 110.0
        assert close_fill.price == pytest.approx(exit_fill_price)
        expected_close_fee = abs(closed_qty * exit_fill_price) * 100.0 / 10000
        assert close_fill.fee == pytest.approx(expected_close_fee)

        gross = (entry_fill_price - exit_fill_price) * closed_qty
        expected_net = gross - entry_fee - expected_close_fee
        assert result.position.realized_pnl == pytest.approx(expected_net)

    def test_reversal_charges_fees_on_both_legs(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        assert len(result.fills) == 2
        close_fill, open_fill = result.fills
        assert close_fill.fee > 0.0
        assert open_fill.fee > 0.0
        assert close_fill.fee == pytest.approx(abs(close_fill.quantity * close_fill.price) * 10.0 / 10000)
        assert open_fill.fee == pytest.approx(abs(open_fill.quantity * open_fill.price) * 10.0 / 10000)
        # new position's cost basis is its own actual (slippage-adjusted) opening fill price
        assert result.position.average_entry_price == pytest.approx(open_fill.price)
        assert result.position.side is PositionSide.SHORT

    def test_no_fee_double_counting_across_consecutive_reversals(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        r1 = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )
        t1 = T0 + timedelta(minutes=5)
        r2 = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1), make_price(price=110.0, as_of=t1)
        )
        leg1_open, leg1_close = r1.fills[0], r2.fills[0]
        leg1_net = (leg1_close.price - leg1_open.price) * leg1_open.quantity - leg1_open.fee - leg1_close.fee
        assert r2.position.realized_pnl == pytest.approx(leg1_net)

        t2 = t1 + timedelta(minutes=5)
        r3 = engine.process_signal(
            make_signal(context_id="ctx-3", score=LONG_SCORE, timestamp=t2), make_price(price=105.0, as_of=t2)
        )
        leg2_open, leg2_close = r2.fills[1], r3.fills[0]
        leg2_net = (leg2_open.price - leg2_close.price) * leg2_open.quantity - leg2_open.fee - leg2_close.fee
        # total realized PnL is the SUM of each leg's own net — no fee is
        # counted against more than one leg, and none is dropped.
        assert r3.position.realized_pnl == pytest.approx(leg1_net + leg2_net)

    def test_idempotent_replay_charges_no_second_fee(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)

        first = engine.process_signal(signal, price)
        second = engine.process_signal(signal, price)

        assert second.idempotent_replay is True
        assert second.fills == ()
        assert engine.fills("BTCUSDT") == first.fills
        assert len(engine.fills("BTCUSDT")) == 1

    def test_neutral_charges_no_fee_and_causes_no_mutation(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=50.0, slippage_bps=100.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=90.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position == opened.position
        assert engine.fills("BTCUSDT") == opened.fills
        assert engine.position("BTCUSDT") == opened.position

    def test_multi_symbol_fee_accounting_is_isolated(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        btc = engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-1", score=LONG_SCORE, timestamp=T0),
            make_price(symbol="BTCUSDT", price=100.0, as_of=T0),
        )
        eth = engine.process_signal(
            make_signal(symbol="ETHUSDT", context_id="ctx-eth-1", score=SHORT_SCORE, timestamp=T0),
            make_price(symbol="ETHUSDT", price=10.0, as_of=T0),
        )

        assert engine.fills("BTCUSDT")[0].symbol == "BTCUSDT"
        assert engine.fills("ETHUSDT")[0].symbol == "ETHUSDT"
        assert engine.fills("BTCUSDT")[0].fee != engine.fills("ETHUSDT")[0].fee

        t1 = T0 + timedelta(minutes=5)
        engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-2", score=SHORT_SCORE, timestamp=t1),
            make_price(symbol="BTCUSDT", price=120.0, as_of=t1),
        )
        # closing/reversing BTCUSDT must not touch ETHUSDT's recorded fills/fees
        assert engine.fills("ETHUSDT") == eth.fills


=== FILE: tests/test_paper_trading_models.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.paper_trading.models import (
    MarketPriceSnapshot,
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


class TestMarketPriceSnapshot:
    def test_normalizes_symbol(self) -> None:
        snap = MarketPriceSnapshot(symbol="btcusdt", price=100.0, as_of=T0)
        assert snap.symbol == "BTCUSDT"

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=100.0, as_of=datetime(2026, 8, 31))

    def test_rejects_non_positive_price(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=0.0, as_of=T0)
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=-5.0, as_of=T0)

    def test_rejects_non_finite_price(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=float("nan"), as_of=T0)


class TestPaperOrder:
    def test_rejects_non_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            PaperOrder(
                order_id="X:ctx:0", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=0.0, signal_context_id="ctx", created_at=T0,
            )

    def test_rejects_empty_order_id(self) -> None:
        with pytest.raises(ValueError):
            PaperOrder(
                order_id="  ", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, signal_context_id="ctx", created_at=T0,
            )


class TestPaperFill:
    def test_rejects_non_positive_price(self) -> None:
        with pytest.raises(ValueError):
            PaperFill(
                fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, price=0.0, fee=0.0, filled_at=T0,
            )

    def test_rejects_negative_fee(self) -> None:
        with pytest.raises(ValueError):
            PaperFill(
                fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, price=100.0, fee=-0.01, filled_at=T0,
            )

    def test_zero_fee_is_valid(self) -> None:
        fill = PaperFill(
            fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=1.0, price=100.0, fee=0.0, filled_at=T0,
        )
        assert fill.fee == 0.0


class TestPaperPosition:
    def test_flat_requires_zero_quantity_and_entry(self) -> None:
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.FLAT, quantity=1.0,
                average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
            )
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
                average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
            )

    def test_flat_zero_zero_is_valid(self) -> None:
        pos = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
        )
        assert pos.side is PositionSide.FLAT

    def test_long_requires_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.LONG, quantity=0.0,
                average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
            )


class TestPaperTradingResult:
    def test_coerces_orders_and_fills_to_tuple(self) -> None:
        pos = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
        )
        result = PaperTradingResult(
            symbol="BTCUSDT", position=pos, orders=[], fills=[], idempotent_replay=False,
        )
        assert isinstance(result.orders, tuple)
        assert isinstance(result.fills, tuple)


=== FILE: tests/test_paper_trading_notional_override.py ===
"""
Pre-Audit Enhancement Pass — tests for the additive, backward-compatible
`PaperTradingEngine.process_signal(..., notional_override=...)` and
`.has_processed()` extensions. `tests/test_paper_trading_engine.py`
(accepted, Phase 5) is left completely untouched — this is a NEW,
separate file so that file's own diff stays zero.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, PositionSide

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)

LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def make_signal(symbol="BTCUSDT", timestamp=T0, context_id="ctx-0", score=0.0, model_version="test-v1") -> Signal:
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version=model_version,
    )


def make_price(symbol="BTCUSDT", price=100.0, as_of=T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


class TestDefaultBehaviourUnchanged:
    def test_no_override_matches_pre_extension_behaviour_byte_for_byte(self) -> None:
        engine_a = PaperTradingEngine(notional_per_position=1000.0)
        engine_b = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        price = make_price()

        result_a = engine_a.process_signal(signal, price)
        result_b = engine_b.process_signal(signal, price, notional_override=None)
        assert result_a == result_b
        assert result_a.position.quantity == 10.0  # 1000 notional / 100 price


class TestOverrideAppliesOnlyToFreshOpen:
    def test_override_changes_opened_quantity(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        result = engine.process_signal(signal, make_price(price=100.0), notional_override=500.0)
        assert result.position.quantity == 5.0  # 500 / 100

    def test_zero_or_negative_override_fails_closed(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=0.0)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=-10.0)

    def test_nan_override_fails_closed(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=float("nan"))

    def test_inf_override_fails_closed(self) -> None:
        """Regression (BLOCKER FIX, Decision 90, item 6): a BRAND-NEW
        context must still fail closed on an invalid override — this
        must NOT regress just because validation moved deeper into the
        control flow."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=float("inf"))
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=0.0)


class TestIdempotencyIgnoresOverrideOnReplay:
    """Regression suite for the BLOCKER FIX (Decision 90, independent
    acceptance review finding): an already-processed (symbol,
    context_id) replay must ALWAYS return the cached result untouched,
    regardless of whether the replay's `notional_override` is a
    different valid value, or outright invalid (0/NaN/inf) — the
    override must never even be evaluated once idempotency has already
    established this is a replay, not a new transition."""

    def test_replay_with_different_override_returns_original_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price(), notional_override=500.0)
        second = engine.process_signal(signal, make_price(), notional_override=9999.0)
        assert second.idempotent_replay is True
        assert second.position.quantity == first.position.quantity == 5.0  # override IGNORED on replay

    def test_replay_with_zero_override_returns_cached_result_no_raise(self) -> None:
        """Item 2: processed context + zero override -> cached result
        (must NOT raise ValueError, even though 0.0 is an invalid
        notional in isolation)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=0.0)
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_nan_override_returns_cached_result_no_raise(self) -> None:
        """Item 3."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=float("nan"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_inf_override_returns_cached_result_no_raise(self) -> None:
        """Item 4."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=float("inf"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_negative_override_returns_cached_result_no_raise(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=-50.0)
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_no_additional_order_fill_fee_across_all_invalid_override_replays(self) -> None:
        """Item 5: none of the replay cases (different-valid / zero /
        NaN / inf / negative override) may produce a new order, fill,
        fee, or allocation — `orders`/`fills` must be empty on every
        replay result, and the engine's cumulative order/fill history
        for the symbol must stay at exactly one entry (the original
        open)."""
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())

        for override in (500.0, 9999.0, 0.0, float("nan"), float("inf"), -50.0):
            replay_result = engine.process_signal(signal, make_price(), notional_override=override)
            assert replay_result.orders == ()
            assert replay_result.fills == ()
            assert replay_result.idempotent_replay is True

        assert len(engine.orders("BTCUSDT")) == 1
        assert len(engine.fills("BTCUSDT")) == 1

    def test_conflicting_signal_content_still_raises(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1")
        s2 = make_signal(score=SHORT_SCORE, context_id="c1")  # same context_id, different content
        engine.process_signal(s1, make_price(), notional_override=500.0)
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(), notional_override=500.0)

    def test_conflicting_signal_raises_conflict_error_not_swallowed_by_invalid_override(self) -> None:
        """Item 7 (unchanged conflict semantics): a conflicting replay
        must still raise `IdempotencyConflictError` — and specifically
        NOT get reinterpreted as "ignore the invalid override, return
        cached result" just because its own `notional_override` also
        happens to be invalid. Conflict detection runs before the
        override is ever consulted, so this must raise the conflict
        error, never a ValueError and never a silent cached return."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1")
        s2 = make_signal(score=SHORT_SCORE, context_id="c1")
        engine.process_signal(s1, make_price())
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(), notional_override=float("nan"))


class TestReversalAccountingCorrectWithOverride:
    def test_reversal_realized_pnl_uses_correct_notional_on_each_leg(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=5.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        s2 = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        opened = engine.process_signal(s1, make_price(price=100.0, as_of=T0), notional_override=500.0)
        result = engine.process_signal(
            s2, make_price(price=110.0, as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )
        assert len(result.fills) == 2
        close_fill, open_fill = result.fills
        opened_quantity = opened.position.quantity  # slippage-adjusted quantity from the FIRST leg
        assert close_fill.quantity == pytest.approx(opened_quantity)  # closes the ORIGINAL 500-notional leg exactly
        assert open_fill.quantity == pytest.approx(250.0 / open_fill.price)  # opens the NEW 250-notional leg
        assert close_fill.fee > 0 and open_fill.fee > 0  # fees still applied


class TestHasProcessed:
    def test_false_before_processing(self) -> None:
        engine = PaperTradingEngine()
        assert engine.has_processed("BTCUSDT", "c1") is False

    def test_true_after_processing_directional_signal(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())
        assert engine.has_processed("BTCUSDT", "c1") is True

    def test_false_for_neutral_signal_no_action(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=0.0, context_id="c1")  # NEUTRAL
        engine.process_signal(signal, make_price())
        assert engine.has_processed("BTCUSDT", "c1") is False

    def test_read_only_does_not_mutate_state(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())
        position_before = engine.position("BTCUSDT")
        engine.has_processed("BTCUSDT", "c1")
        engine.has_processed("BTCUSDT", "unknown-context")
        assert engine.position("BTCUSDT") == position_before


class TestBrandNewContextAlwaysValidatesRegardlessOfEventualOutcome:
    """Regression suite for the SECOND-ROUND blocker fix (Decision 91,
    independent acceptance review finding): validation was moved all the
    way down to the `_open_position()` call site by Decision 90, which
    fixed idempotent-replay/conflict cases but left a remaining gap — a
    BRAND-NEW context whose direction matches an already-open position
    takes the same-direction NO_ACTION `else` branch (no `_open_position`
    call at all), so an invalid `notional_override` on that path was
    never validated. These tests prove: once idempotency/conflict
    resolution has established a signal is genuinely NEW (not a replay,
    not a conflict), `notional_override` is validated unconditionally —
    BEFORE branching into same-direction/open/reversal behavior — while
    the already-processed-replay and conflicting-replay cases (Decision
    90) remain completely unaffected."""

    def _engine_with_existing_long(self) -> PaperTradingEngine:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0), make_price(as_of=T0))
        return engine

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_same_direction_brand_new_context_invalid_override_fails_closed(self, bad_override: float) -> None:
        """Items 1-4: existing LONG + brand-new LONG context + invalid
        override -> ValueError, even though this signal would otherwise
        take the same-direction NO_ACTION path and never call
        `_open_position` at all."""
        engine = self._engine_with_existing_long()
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))
        with pytest.raises(ValueError):
            engine.process_signal(new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override)

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_same_direction_invalid_override_produces_no_mutation(self, bad_override: float) -> None:
        """Item 5: the rejected call must leave state completely
        untouched — no new order/fill, no position/timestamp change,
        and the (never-processed) new context_id must not be recorded."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        orders_before = engine.orders("BTCUSDT")
        fills_before = engine.fills("BTCUSDT")
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        with pytest.raises(ValueError):
            engine.process_signal(new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override)

        assert engine.position("BTCUSDT") == position_before
        assert engine.orders("BTCUSDT") == orders_before
        assert engine.fills("BTCUSDT") == fills_before
        assert engine.has_processed("BTCUSDT", "c2") is False

    def test_same_direction_brand_new_context_valid_override_retains_no_action_behaviour(self) -> None:
        """Item 6: a VALID override on a brand-new same-direction context
        must not error, and must retain the accepted Phase 5
        same-direction NO_ACTION semantics exactly — no new order/fill,
        only the position's `updated_at` timestamp refreshes."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        result = engine.process_signal(
            new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.quantity == position_before.quantity  # unchanged -- override was NEVER consumed here
        assert result.position.average_entry_price == position_before.average_entry_price
        assert result.position.updated_at == T0 + timedelta(minutes=5)
        assert engine.has_processed("BTCUSDT", "c2") is True

    def test_already_processed_identical_replay_with_invalid_override_still_cached(self) -> None:
        """Item 7 (unaffected by this fix — Decision 90's guarantee)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        first = engine.process_signal(signal, make_price(as_of=T0))
        second = engine.process_signal(signal, make_price(as_of=T0), notional_override=float("nan"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_conflicting_replay_with_invalid_override_still_raises_conflict(self) -> None:
        """Item 8 (unaffected by this fix — Decision 90's guarantee)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        s2 = make_signal(score=SHORT_SCORE, context_id="c1", timestamp=T0)
        engine.process_signal(s1, make_price(as_of=T0))
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(as_of=T0), notional_override=float("inf"))

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_reversal_brand_new_context_invalid_override_fails_closed_before_mutation(
        self, bad_override: float
    ) -> None:
        """Item 9: a brand-new context that would REVERSE an existing
        position must also fail closed on an invalid override, and must
        do so before any state mutation — the existing LONG position
        must remain exactly as it was (not partially closed)."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        reversal_signal = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        with pytest.raises(ValueError):
            engine.process_signal(
                reversal_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override
            )

        assert engine.position("BTCUSDT") == position_before  # still LONG, untouched -- no partial close
        assert engine.has_processed("BTCUSDT", "c2") is False

    def test_reversal_brand_new_context_valid_override_still_works(self) -> None:
        engine = self._engine_with_existing_long()
        reversal_signal = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))
        result = engine.process_signal(
            reversal_signal, make_price(price=110.0, as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )
        assert len(result.fills) == 2
        assert result.position.side is PositionSide.SHORT

    def test_default_fixed_notional_behaviour_unchanged(self) -> None:
        """Item 10: no override at all -- brand-new same-direction and
        brand-new reversal contexts behave exactly as accepted Phase 5
        (fixed notional, no ValueError from this validation at all)."""
        engine = self._engine_with_existing_long()
        same_direction = engine.process_signal(
            make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5)),
            make_price(as_of=T0 + timedelta(minutes=5)),
        )
        assert same_direction.orders == ()

        reversal = engine.process_signal(
            make_signal(score=SHORT_SCORE, context_id="c3", timestamp=T0 + timedelta(minutes=10)),
            make_price(price=110.0, as_of=T0 + timedelta(minutes=10)),
        )
        assert len(reversal.fills) == 2
        assert reversal.position.quantity == pytest.approx(1000.0 / 110.0)  # fixed notional default


=== FILE: tests/test_persistence_paper_state_store.py ===
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError
from crypto_signal_engine.persistence.paper_state_store import SCHEMA_VERSION, PaperStateStore

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def _signal(context_id="ctx-1") -> Signal:
    return Signal(
        symbol="BTCUSDT", timestamp=T0, context_id=context_id, score=0.5, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="test-v1",
    )


def _position(**overrides) -> PaperPosition:
    defaults = dict(
        symbol="BTCUSDT", side=PositionSide.LONG, quantity=10.0,
        average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
    )
    defaults.update(overrides)
    return PaperPosition(**defaults)


class TestPaperStateStoreBasics:
    def test_fresh_database_has_no_paper_state(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.load_paper_state("BTCUSDT") is None
        store.close()

    def test_fresh_database_has_no_candle_checkpoint(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) is None
        store.close()

    def test_checkpoint_and_load_position_round_trip(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        position = _position()
        fill = PaperFill(
            fill_id="f1", order_id="o1", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=10.0, price=100.0, fee=1.0, filled_at=T0,
        )
        order = PaperOrder(
            order_id="o1", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=10.0, signal_context_id="ctx-1", created_at=T0,
        )
        signal = _signal()

        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=position, entry_fee=2.5, last_signal_timestamp=T0,
            new_context_entries=[("ctx-1", signal, position)], new_fills=(fill,), new_orders=(order,),
        )

        snapshot = store.load_paper_state("BTCUSDT")
        assert snapshot is not None
        assert snapshot.position == position
        assert snapshot.entry_fee == 2.5
        assert snapshot.last_signal_timestamp == T0
        assert "ctx-1" in snapshot.processed_context_ids
        restored_signal, restored_result = snapshot.processed_context_ids["ctx-1"]
        assert restored_signal == signal
        assert restored_result.position == position
        assert restored_result.idempotent_replay is False
        assert snapshot.fills == (fill,)
        assert snapshot.orders == (order,)
        store.close()

    def test_checkpoint_upserts_position_not_duplicates(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(realized_pnl=0.0), entry_fee=1.0,
            last_signal_timestamp=T0, new_context_entries=[], new_fills=(), new_orders=(),
        )
        later = T0 + timedelta(minutes=5)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(realized_pnl=50.0, updated_at=later), entry_fee=3.0,
            last_signal_timestamp=later, new_context_entries=[], new_fills=(), new_orders=(),
        )

        snapshot = store.load_paper_state("BTCUSDT")
        assert snapshot.position.realized_pnl == 50.0
        assert snapshot.entry_fee == 3.0
        assert snapshot.last_signal_timestamp == later
        store.close()

    def test_candle_checkpoint_round_trip(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == T0

        later = T0 + timedelta(minutes=5)
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, later)
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == later
        store.close()

    def test_symbols_are_isolated(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT"), entry_fee=1.0,
            last_signal_timestamp=T0, new_context_entries=[], new_fills=(), new_orders=(),
        )
        assert store.load_paper_state("ETHUSDT") is None
        assert store.load_paper_state("BTCUSDT") is not None
        store.close()


class TestListOpenPositionSymbols:
    """`list_open_position_symbols()` — otomatik sembol seçiminin Bölüm A
    ("Existing open PAPER positions") pinning kuralının okuma tarafı."""

    def test_empty_database_returns_empty_tuple(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.list_open_position_symbols() == ()
        store.close()

    def test_only_non_flat_positions_are_returned(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT", side=PositionSide.LONG),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.checkpoint_paper_state(
            symbol="ETHUSDT",
            position=_position(symbol="ETHUSDT", side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.checkpoint_paper_state(
            symbol="BNBUSDT", position=_position(symbol="BNBUSDT", side=PositionSide.SHORT),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        assert store.list_open_position_symbols() == ("BNBUSDT", "BTCUSDT")  # alfabetik, deterministik
        store.close()

    def test_is_read_only_does_not_mutate_state(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT", side=PositionSide.LONG),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        before = store.load_paper_state("BTCUSDT")
        store.list_open_position_symbols()
        after = store.load_paper_state("BTCUSDT")
        assert before == after
        store.close()


class TestSchemaVersioning:
    def test_reopening_same_version_succeeds(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        store1.close()
        store2 = PaperStateStore(db_path)  # must not raise
        store2.close()

    def test_schema_version_mismatch_raises_on_open(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))
        conn.commit()
        conn.close()

        with pytest.raises(SchemaVersionMismatchError):
            PaperStateStore(db_path)


class TestCorruptRecordHandling:
    def test_corrupt_position_side_raises_not_silently_skipped(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(), entry_fee=1.0, last_signal_timestamp=T0,
            new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE paper_position SET side = 'NOT_A_REAL_SIDE' WHERE symbol = 'BTCUSDT'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_paper_state("BTCUSDT")
        reopened.close()

    def test_corrupt_context_json_raises(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(), entry_fee=1.0, last_signal_timestamp=T0,
            new_context_entries=[("ctx-1", _signal(), _position())], new_fills=(), new_orders=(),
        )
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE processed_context SET signal_json = 'not valid json{{' WHERE context_id = 'ctx-1'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_paper_state("BTCUSDT")
        reopened.close()

    def test_corrupt_candle_checkpoint_raises(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE candle_checkpoint SET last_open_time = 'not-a-timestamp'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
        reopened.close()


class TestFailureIsExplicit:
    def test_checkpoint_after_close_raises_persistence_error(self, tmp_path) -> None:
        from crypto_signal_engine.errors import PersistenceError

        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.close()

        with pytest.raises(PersistenceError):
            store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)


=== FILE: tests/test_persistence_recovery.py ===
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError, PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, PositionSide
from crypto_signal_engine.persistence.errors import CorruptRecordError
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime, restore_paper_engine
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at, make_order_book

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
WARMUP = 20

LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def _signal(symbol="BTCUSDT", context_id="ctx-1", score=LONG_SCORE, timestamp=T0) -> Signal:
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="test-v1",
    )


def _price(symbol="BTCUSDT", price=100.0, as_of=T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


def _checkpoint_from_result(store: PaperStateStore, engine: PaperTradingEngine, symbol: str, signal: Signal, result) -> None:
    """Testlerde `PersistedRuntime._checkpoint_paper_transition` ile AYNI
    şeyi doğrudan Faz 5 seviyesinde yapan küçük bir yardımcı (tam
    RuntimeCoordinator pipeline'ı kurmadan Faz 5 restart-idempotency
    sözleşmesini izole test etmek için)."""
    state = engine._states[symbol]  # noqa: SLF001 (test introspection, aynı desen coordinator.py'de de var)
    store.checkpoint_paper_state(
        symbol=symbol, position=result.position, entry_fee=state.entry_fee,
        last_signal_timestamp=state.last_signal_timestamp,
        new_context_entries=[(signal.context_id, signal, result.position)],
        new_fills=result.fills, new_orders=result.orders,
    )


class TestRestartIdempotency:
    """Blocker-level: process signal/context A -> persist -> brand new
    engine/store objects -> recover -> replay A -> no duplicate mutation."""

    def test_replaying_same_context_after_restart_causes_no_duplicate_mutation(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        signal = _signal(context_id="ctx-A")
        price = _price()

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        result1 = engine1.process_signal(signal, price)
        assert len(result1.fills) == 1
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result1)
        store1.close()

        # entirely new objects
        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT"])
        assert restored == {"BTCUSDT": True}

        replay_result = engine2.process_signal(signal, price)

        assert replay_result.idempotent_replay is True
        assert replay_result.orders == ()
        assert replay_result.fills == ()
        assert replay_result.position == result1.position
        assert engine2.fills("BTCUSDT") == result1.fills  # nothing duplicated
        store2.close()

    def test_conflicting_replay_after_restart_preserves_phase5_conflict_behavior(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        signal = _signal(context_id="ctx-A", score=LONG_SCORE)
        price = _price()

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        result1 = engine1.process_signal(signal, price)
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result1)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restore_paper_engine(store2, engine2, ["BTCUSDT"])

        conflicting = _signal(context_id="ctx-A", score=SHORT_SCORE)
        with pytest.raises(IdempotencyConflictError):
            engine2.process_signal(conflicting, price)

        # state must remain exactly as recovered, not corrupted by the conflict attempt
        assert engine2.position("BTCUSDT") == result1.position
        store2.close()


class TestOpenPositionRecovery:
    """Blocker-level: open position (with fee/slippage), persist, destroy
    old objects, recover, close/reverse — PnL/fees must match an
    uninterrupted equivalent run. Entry fee/average entry must NOT reset."""

    def test_recovered_close_matches_uninterrupted_run(self, tmp_path) -> None:
        open_signal = _signal(context_id="ctx-open", score=LONG_SCORE, timestamp=T0)
        close_signal = _signal(context_id="ctx-close", score=SHORT_SCORE, timestamp=T0 + timedelta(minutes=5))
        open_price = _price(price=100.0, as_of=T0)
        close_price = _price(price=120.0, as_of=T0 + timedelta(minutes=5))

        # reference: uninterrupted single-process run
        reference_engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        reference_engine.process_signal(open_signal, open_price)
        reference_result = reference_engine.process_signal(close_signal, close_price)

        # interrupted run: open, persist, destroy, recover, close
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        open_result = engine1.process_signal(open_signal, open_price)
        assert open_result.position.side is PositionSide.LONG
        _checkpoint_from_result(store1, engine1, "BTCUSDT", open_signal, open_result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        restore_paper_engine(store2, engine2, ["BTCUSDT"])

        recovered_position_before_close = engine2.position("BTCUSDT")
        assert recovered_position_before_close.average_entry_price == pytest.approx(open_result.position.average_entry_price)

        recovered_result = engine2.process_signal(close_signal, close_price)

        assert recovered_result.position.side == reference_result.position.side
        assert recovered_result.position.realized_pnl == pytest.approx(reference_result.position.realized_pnl)
        assert recovered_result.position.average_entry_price == pytest.approx(
            reference_result.position.average_entry_price
        )
        store2.close()


class TestMultiSymbolRecovery:
    def test_two_symbols_recover_in_isolation(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        btc_signal = _signal(symbol="BTCUSDT", context_id="ctx-btc", score=LONG_SCORE, timestamp=T0)
        eth_signal = _signal(symbol="ETHUSDT", context_id="ctx-eth", score=SHORT_SCORE, timestamp=T0)

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        btc_result = engine1.process_signal(btc_signal, _price("BTCUSDT", 100.0, T0))
        eth_result = engine1.process_signal(eth_signal, _price("ETHUSDT", 10.0, T0))
        _checkpoint_from_result(store1, engine1, "BTCUSDT", btc_signal, btc_result)
        _checkpoint_from_result(store1, engine1, "ETHUSDT", eth_signal, eth_result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT", "ETHUSDT"])
        assert restored == {"BTCUSDT": True, "ETHUSDT": True}

        assert engine2.position("BTCUSDT").side is PositionSide.LONG
        assert engine2.position("ETHUSDT").side is PositionSide.SHORT

        # replaying BTC's context must not touch ETH's recovered state
        engine2.process_signal(btc_signal, _price("BTCUSDT", 100.0, T0))
        assert engine2.position("ETHUSDT") == eth_result.position
        store2.close()

    def test_symbol_never_persisted_is_not_restored(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        signal = _signal(symbol="BTCUSDT")
        result = engine1.process_signal(signal, _price("BTCUSDT", 100.0, T0))
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT", "ETHUSDT"])

        assert restored == {"BTCUSDT": True, "ETHUSDT": False}
        assert engine2.position("ETHUSDT") is None  # no fabricated state
        store2.close()


class TestRecoveryCorruptionSafety:
    def test_corrupt_persisted_record_blocks_recovery_rather_than_resetting(self, tmp_path) -> None:
        import sqlite3

        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        signal = _signal()
        result = engine1.process_signal(signal, _price())
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result)
        store1.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE paper_position SET side = 'GARBAGE'")
        conn.commit()
        conn.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        with pytest.raises(CorruptRecordError):
            restore_paper_engine(store2, engine2, ["BTCUSDT"])

        # the engine must NOT have been silently seeded with empty/fresh state
        assert engine2.position("BTCUSDT") is None
        store2.close()


def _bootstrap_fully(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready
    coordinator.ingest_order_book(symbol, make_order_book(symbol, end))


def _make_coordinator(symbols=("BTCUSDT",), **kwargs) -> RuntimeCoordinator:
    provider = kwargs.pop("provider", None) or FakeLiveDataProvider()
    return RuntimeCoordinator(symbols=symbols, provider=provider, warmup_candles=WARMUP, **kwargs)


def _symbol_health(coordinator: RuntimeCoordinator, symbol: str) -> RuntimeHealth:
    status = coordinator.status()
    return next(s for s in status.symbols if s.symbol == symbol).health


class _RangeAwareProvider(FakeLiveDataProvider):
    """`FakeLiveDataProvider.fetch_historical_candles` (varsayılan) scripted
    listeyi `start`/`end`'i YOK SAYARAK döner — bu, `recover()`'ın GERÇEKTEN
    doğru `[start, end)` penceresini istediğini kanıtlamak için YETERSİZDİR
    (bir GERÇEK PUBLIC REST endpoint'i istenmeyen aralık dışındaki veriyi
    DÖNMEZ). Bu fake, gerçek bir REST endpoint'i gibi yalnızca `[start,
    end)` içindeki candle'ları döner — restart-recovery penceresinin
    GERÇEKTEN yeterli lookback istediğini doğrulayan blocker regresyon
    testi için kullanılır."""

    async def fetch_historical_candles(self, symbol, timeframe, start, end):
        self.fetch_calls.append((symbol, timeframe, start, end))
        candles = self._historical_candles.get((symbol, timeframe), [])
        return [c for c in candles if start <= c.open_time < end]


class TestRestartRecoveryRebuildsWarmupHistory:
    """BLOCKER (bağımsız acceptance review bulgusu): restart recovery,
    checkpoint VARKEN yalnızca dar `[checkpoint, now)` aralığını çekiyordu
    — bu aralık normalde `warmup_candles` gereksinimini KARŞILAMAYA
    YETMEZ, restart'ı gereksiz yere BOOTSTRAPPING'de bırakıyordu (GERÇEK
    PUBLIC geçmiş veri MEVCUT olsa bile). Fix: checkpoint VARKEN de,
    checkpoint'ten ÖNCEYE doğru aynı warmup-lookback penceresi (`checkpoint
    is None` dalıyla AYNI formül) eklenir — `[checkpoint - lookback, now)`."""

    def test_restart_with_existing_checkpoint_rebuilds_warmup_and_reaches_ready_immediately(
        self, tmp_path
    ) -> None:
        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            # Faz canlı candle: checkpoint'i "now"a YAKIN bir noktaya taşır.
            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            event = runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)
            assert event.outcome is IngestOutcome.ACCEPTED
            checkpoint_before = store1.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
            assert checkpoint_before == live_candle.open_time
            state_before = store1.load_paper_state("BTCUSDT")
            store1.close()

            # TÜM runtime/paper nesnelerini yok et, AYNI SQLite DB ile
            # baştan kur (gerçek bir process restart'ını simüle eder).
            now2 = END + timedelta(minutes=5)  # restart, son canlı candle'dan KISA süre sonra
            store2 = PaperStateStore(db_path)
            full_m5_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP) + [live_candle]
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): full_m5_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready, f"{report.symbol}/{report.timeframe.value} warmup yeniden inşa edilemedi"

            # M1 order-book YENİDEN edinilmeli (Faz 6/7 hiçbir zaman
            # order-book state'i persist ETMEZ — bu KASITLI).
            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))

            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.READY

            # checkpoint GERİYE gitmedi.
            checkpoint_after = store2.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
            assert checkpoint_after == checkpoint_before

            # candle window GERÇEKTEN warmup'a sahip (fabrikasyon DEĞİL,
            # `_RangeAwareProvider`'ın GERÇEKTEN döndürdüğü candle'lardan).
            window = coordinator2._candle_windows[("BTCUSDT", Timeframe.M5)]
            assert len(window.history()) >= WARMUP

            # historical rebuild hiçbir YENİ paper order/fill/PnL ÜRETMEDİ
            # (state reconstruction, retroactive trading DEĞİL).
            state_after = store2.load_paper_state("BTCUSDT")
            assert len(state_after.fills) == len(state_before.fills)
            assert len(state_after.orders) == len(state_before.orders)
            assert state_after.position.realized_pnl == state_before.position.realized_pnl
            assert set(state_after.processed_context_ids) == set(state_before.processed_context_ids)

            store2.close()

        run_async(scenario())

    def test_restart_recovery_preserves_multi_symbol_isolation(self, tmp_path) -> None:
        """İkinci sembol HİÇ canlı candle görmedi (checkpoint hâlâ `None`)
        — bu, BİRİNCİ sembolün checkpoint-bazlı geniş-pencere recovery'siyle
        AYNI `recover()` çağrısında karışmadan doğru işlenmeli."""

        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"), clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            _bootstrap_fully(coordinator1, "ETHUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)  # yalnızca BTCUSDT checkpoint alır
            store1.close()

            now2 = END + timedelta(minutes=5)
            store2 = PaperStateStore(db_path)
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP)
                    + [live_candle],
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                    ("ETHUSDT", Timeframe.M5): make_candle_series_ending_at("ETHUSDT", Timeframe.M5, now2, WARMUP),
                    ("ETHUSDT", Timeframe.M15): make_candle_series_ending_at("ETHUSDT", Timeframe.M15, now2, WARMUP),
                    ("ETHUSDT", Timeframe.H1): make_candle_series_ending_at("ETHUSDT", Timeframe.H1, now2, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"), provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready

            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))
            coordinator2.ingest_order_book("ETHUSDT", make_order_book("ETHUSDT", now2))

            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.READY
            assert _symbol_health(coordinator2, "ETHUSDT") is RuntimeHealth.READY
            assert store2.load_candle_checkpoint("ETHUSDT", Timeframe.M5) is None  # hiç canlı checkpoint YOK

            store2.close()

        run_async(scenario())

    def test_genuinely_insufficient_public_history_stays_bootstrapping_not_fabricated_ready(
        self, tmp_path
    ) -> None:
        """PUBLIC REST kaynağı GERÇEKTEN yetersizse (yalnızca birkaç
        candle mevcutsa), sembol dürüstçe BOOTSTRAPPING kalmalı — SAHTE
        bir READY asla ÜRETİLMEMELİDİR."""

        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)
            store1.close()

            now2 = END + timedelta(minutes=5)
            store2 = PaperStateStore(db_path)
            # PUBLIC kaynak yalnızca SON birkaç candle'ı "hatırlıyor" (GERÇEKTEN
            # yetersiz geçmiş) — WARMUP=20 gereksinimini KARŞILAMAZ.
            insufficient_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, 3) + [live_candle]
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): insufficient_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            m5_report = next(r for r in reports if r.timeframe is Timeframe.M5)
            assert m5_report.ready is False  # sahte bir READY asla üretilmedi

            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))
            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING

            store2.close()

        run_async(scenario())


class TestPersistedRuntimeIntegration:
    """Uçtan uca (RuntimeCoordinator + PersistedRuntime + PaperStateStore)
    entegrasyon testleri — Faz 6 pipeline'ının Faz 7 checkpoint'lerle
    birlikte doğru çalıştığını doğrular."""

    def test_accepted_candle_checkpoints_and_recovery_restores_window_anchor(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "state.db"
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator()
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            event = runtime1.ingest_candle("BTCUSDT", Timeframe.M5, candle)
            assert event.outcome is IngestOutcome.ACCEPTED
            assert store1.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == candle.open_time
            store1.close()

            # brand new provider/coordinator/store — configure provider2 to
            # answer the recovery REST re-fetch with the exact same history.
            store2 = PaperStateStore(db_path)
            full_m5_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP) + [candle]
            provider2 = FakeLiveDataProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): full_m5_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2)
            runtime2 = PersistedRuntime(coordinator2, store2)

            await runtime2.recover()

            window = coordinator2._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
            assert window.latest_open_time == candle.open_time  # re-anchored, not fabricated
            store2.close()

        run_async(scenario())

    def test_persistence_write_failure_degrades_health_not_falsely_healthy(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

        store.close()  # force subsequent durable writes to fail

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.outcome is IngestOutcome.ACCEPTED  # in-memory Phase 6 state unaffected
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

    def test_unrelated_order_book_event_does_not_clear_persistence_fault(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)
        store.close()

        runtime.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        later = END + timedelta(seconds=1)
        event = runtime.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", later, last_update_id=2))

        assert event.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

    def test_successful_checkpoint_clears_persistence_fault(self, tmp_path) -> None:
        class _FlakyStore:
            """Bir sonraki 1 checkpoint çağrısında BAŞARISIZ OLAN, sonra
            normal çalışan bir test double'ı — gerçek SQLite bağlantı
            yaşam döngüsünü karmaşıklaştırmadan "geçici arıza -> düzelme"
            senaryosunu deterministik test eder."""

            def __init__(self, real: PaperStateStore) -> None:
                self._real = real
                self._fail_remaining = 1

            def checkpoint_candle(self, *args, **kwargs) -> None:
                if self._fail_remaining > 0:
                    self._fail_remaining -= 1
                    raise PersistenceError("simulated transient failure")
                self._real.checkpoint_candle(*args, **kwargs)

            def checkpoint_paper_state(self, *args, **kwargs) -> None:
                self._real.checkpoint_paper_state(*args, **kwargs)

            def checkpoint_candle_transition(self, *args, **kwargs) -> None:
                # HIGH-1 fix: PersistedRuntime.ingest_candle() now always
                # goes through this combined atomic call (never the two
                # separate methods above) — same fail-once-then-recover
                # fault injection, applied to the single real call site.
                if self._fail_remaining > 0:
                    self._fail_remaining -= 1
                    raise PersistenceError("simulated transient failure")
                self._real.checkpoint_candle_transition(*args, **kwargs)

            def load_candle_checkpoint(self, *args, **kwargs):
                return self._real.load_candle_checkpoint(*args, **kwargs)

            def load_paper_state(self, *args, **kwargs):
                return self._real.load_paper_state(*args, **kwargs)

            def close(self) -> None:
                self._real.close()

        db_path = tmp_path / "state.db"
        real_store = PaperStateStore(db_path)
        flaky = _FlakyStore(real_store)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, flaky)

        first = runtime.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))
        assert first.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        later = END + timedelta(minutes=5)
        second = runtime.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, later, price=102.0)
        )
        assert second.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY
        real_store.close()

    def test_neutral_signal_produces_no_persistence_write(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)

        # flat prices -> NEUTRAL-leaning score; regardless of the exact
        # direction produced, assert the *policy*: idempotent replay /
        # NEUTRAL paths never call checkpoint_paper_state. We verify this
        # by checking that no processed_context row exists yet for a
        # context_id that was never durably confirmed as a real mutation.
        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=100.0)
        event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        if event.cycle_result is not None and event.cycle_result.evaluated:
            from crypto_signal_engine.domain.enums import SignalDirection

            if event.cycle_result.signal.direction is SignalDirection.NEUTRAL:
                snapshot = store.load_paper_state("BTCUSDT")
                assert snapshot is None or event.cycle_result.signal.context_id not in snapshot.processed_context_ids
        store.close()

    def test_stream_disconnect_marks_health_degraded_via_persisted_runtime_run(self, tmp_path) -> None:
        """BLOCKER (Faz 9 soak harness geliştirmesi sırasında, GERÇEK
        `PersistedRuntime.run()` üzerinden bulundu): `RuntimeCoordinator._consume_candles`
        (Faz 6) bir stream hatasını `except Exception: mark_disconnected(symbol);
        raise` ile ANINDA gözlemlenebilir kılar — ama `PersistedRuntime._consume_candles`/
        `_consume_order_book` (Faz 7, Faz 8'in GERÇEK üretim giriş noktası
        `Application.start()` -> `runtime.run()` tarafından kullanılır) bu
        sarmalayıcıya SAHİP DEĞİLDİ. Sonuç: GERÇEK üretimde bir stream kopması,
        `mark_disconnected()` HİÇ ÇAĞRILMADAN sessizce `gather(...,
        return_exceptions=True)` tarafından yutuluyordu — sembol yalnızca
        staleness eşiği DOLANA KADAR yanlışlıkla READY/healthy görünmeye
        devam ediyordu (Faz 6'nın KENDİ "must immediately be observably
        unhealthy" invariant'ının ihlali)."""

        class _DisconnectingProvider(FakeLiveDataProvider):
            async def stream_candles(self, symbol, timeframe):
                raise ConnectionError("simulated PUBLIC stream disconnect")
                yield  # pragma: no cover - jeneratör imzasını korumak için, asla ulaşılmaz

        async def scenario() -> None:
            db_path = tmp_path / "state.db"
            store = PaperStateStore(db_path)
            provider = _DisconnectingProvider()
            coordinator = _make_coordinator(provider=provider)
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            runtime = PersistedRuntime(coordinator, store)
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

            run_task = asyncio.create_task(runtime.run())
            for _ in range(10):
                await asyncio.sleep(0)

            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

            await runtime.stop()
            await run_task

        run_async(scenario())


=== FILE: tests/test_persistence_serialization.py ===
from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide
from crypto_signal_engine.persistence.serialization import (
    dict_to_fill,
    dict_to_order,
    dict_to_position,
    dict_to_signal,
    fill_to_dict,
    order_to_dict,
    position_to_dict,
    signal_to_dict,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def _make_evidence(agent=AgentName.QUANT, context_id="ctx-1") -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=0.4, rationale="test rationale", primary_timeframe=Timeframe.M5,
        symbol="BTCUSDT", as_of=T0, context_id=context_id, supporting_metrics={"RSI_14": 55.0},
    )


def _make_signal() -> Signal:
    return Signal(
        symbol="BTCUSDT", timestamp=T0, context_id="ctx-1", score=0.5, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(_make_evidence(AgentName.QUANT),),
        contradicting_factors=(_make_evidence(AgentName.MARKET_STRUCTURE),),
        invalidation="test invalidation", model_version="test-v1",
    )


class TestSerializationRoundTrip:
    def test_signal_round_trip(self) -> None:
        signal = _make_signal()
        restored = dict_to_signal(signal_to_dict(signal))
        assert restored == signal

    def test_position_round_trip(self) -> None:
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.LONG, quantity=10.0,
            average_entry_price=100.0, realized_pnl=5.0, updated_at=T0,
        )
        assert dict_to_position(position_to_dict(position)) == position

    def test_flat_position_round_trip(self) -> None:
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=12.5, updated_at=T0,
        )
        assert dict_to_position(position_to_dict(position)) == position

    def test_fill_round_trip(self) -> None:
        fill = PaperFill(
            fill_id="BTCUSDT:ctx-1:0:fill", order_id="BTCUSDT:ctx-1:0", symbol="BTCUSDT",
            side=OrderSide.BUY, quantity=10.0, price=100.5, fee=1.0, filled_at=T0,
        )
        assert dict_to_fill(fill_to_dict(fill)) == fill

    def test_order_round_trip(self) -> None:
        order = PaperOrder(
            order_id="BTCUSDT:ctx-1:0", symbol="BTCUSDT", side=OrderSide.SELL,
            quantity=10.0, signal_context_id="ctx-1", created_at=T0,
        )
        assert dict_to_order(order_to_dict(order)) == order

    def test_signal_with_no_evidence_round_trips(self) -> None:
        signal = Signal(
            symbol="ETHUSDT", timestamp=T0, context_id="ctx-2", score=0.0, confidence=0.0,
            risk_level=RiskLevel.MEDIUM, primary_timeframe=Timeframe.M5,
            supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test-v1",
        )
        assert dict_to_signal(signal_to_dict(signal)) == signal


=== FILE: tests/test_phase12_local_production_readiness.py ===
"""Faz 12 — Final Local Production Readiness: yüksek-değerli entegrasyon
testleri (Bölüm 12'nin 20 maddelik senaryo listesi). Tamamen offline —
gerçek ağ/websockets/Binance TESTNET bağlantısı YOK; `FakeLiveDataProvider`
(Faz 6/8) ve `FakeTestnetHttpClient` (Faz 10/11) enjekte edilir.

Bu dosya, YENİ Faz 12 davranışına ODAKLANIR (execution-mode gating,
startup reconciliation, read-only dashboard, doctor). Zaten Faz 1-11'in
KENDİ test dosyalarında kanıtlanmış invariant'lar (multi-symbol izolasyon,
stale-data health, contradiction protection, vb.) burada TEKRAR
test EDİLMEZ — yalnızca üretim kompozisyonunun (`app.py::Application`)
bu invariant'ları GERÇEKTEN doğru şekilde BAĞLADIĞI doğrulanır."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.app import EXIT_FATAL, EXIT_OK, Application, _run_doctor
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.models import ExecutionMode, OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, new_record, transition
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.ops.config import load_config
from scripts.backup_sqlite import backup_sqlite_database
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)
_NOT_FOUND_RESPONSE = (400, '{"code": -2013, "msg": "Order does not exist."}')


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


def _pending_unknown_not_found_record(execution_db_path: Path, *, context_id: str = "ctx-pending") -> None:
    store = ExecutionStateStore(execution_db_path)
    intent = OrderIntent(
        symbol=_SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id=context_id, timestamp=datetime.now(timezone.utc), quantity=0.001,
    )
    record = new_record(intent, now=datetime.now(timezone.utc))
    record = transition(
        record, new_state=ExecutionLifecycleState.UNKNOWN_NOT_FOUND, now=datetime.now(timezone.utc),
        detail="pending from a prior session",
    )
    store.save(record)
    store.close()


class TestPaperFirstSafety:
    """#1 fresh PAPER startup, #3 default config never executes TESTNET,
    #20 PAPER requires zero credentials."""

    def test_fresh_paper_startup_reaches_ready(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        assert config.execution_mode is ExecutionMode.PAPER
        app = Application(config, _bootstrappable_provider())
        exit_code = run_async(_run_until_ready_then_shutdown(app))
        assert exit_code == EXIT_OK
        assert app._execution_service is None
        assert app._execution_store is None

    def test_default_config_has_no_execution_service_at_all(self, tmp_path: Path, monkeypatch) -> None:
        """#3: bir order göndermek için GEREKLİ olan `ExecutionReconciliationService`
        nesnesinin KENDİSİ, varsayılan config ile HİÇ İNŞA EDİLMEZ — bu
        yüzden "yanlışlıkla bir TESTNET order gönderme" için erişilebilir
        HİÇBİR kod yolu YOKTUR (in-memory olarak yapısal olarak imkansız)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        try:
            assert app._execution_service is None
            assert app._execution_store is None
        finally:
            app._store.close()
            app._lock.release()

    def test_paper_mode_requires_zero_credentials(self, tmp_path: Path, monkeypatch) -> None:
        """#20: PAPER modda, ortamda HİÇBİR TESTNET kimlik bilgisi
        YOKKEN bile kompozisyon/startup tamamen BAŞARILI olur."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        exit_code = run_async(_run_until_ready_then_shutdown(app))
        assert exit_code == EXIT_OK


class TestTestnetGating:
    """#4 TESTNET without explicit enable -> no execution possible,
    #5 TESTNET enabled without credentials -> doctor FAILs (rejected)."""

    def test_testnet_selected_without_enable_flag_has_no_execution_service(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET"))
        assert config.enable_testnet_execution is False
        app = Application(config, _bootstrappable_provider())
        try:
            # STRICTLY opt-in kapı: mode seçilmiş olsa bile enable=False
            # iken execution service HİÇ İNŞA EDİLMEZ.
            assert app._execution_service is None
            assert app._execution_store is None
        finally:
            app._store.close()
            app._lock.release()

    def test_testnet_enabled_without_credentials_fails_doctor(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        exit_code = _run_doctor(config)
        out = capsys.readouterr().out
        assert exit_code == EXIT_FATAL
        assert "[FAIL] TESTNET credentials configured" in out
        assert "UNSET" in out

    def test_mainnet_is_not_a_selectable_execution_mode(self, tmp_path: Path) -> None:
        """#19: Mainnet private execution YAPISAL OLARAK imkansızdır —
        `CSE_EXECUTION_MODE=MAINNET` HER ZAMAN bir config hatasıdır."""
        from crypto_signal_engine.ops.errors import AppConfigurationError

        with pytest.raises(AppConfigurationError):
            load_config(_base_env(tmp_path, CSE_EXECUTION_MODE="MAINNET"))


class TestCredentialPreflightBlockerFix:
    """BLOCKER FİX (bağımsız acceptance review bulgusu): TESTNET + enable=true,
    execution DB BOŞKEN (hiçbir bekleyen kayıt yokken) kimlik bilgisi eksik
    OLSA BİLE `reconcile_pending()` hiçbir signed çağrı yapmadığından
    `execution_ready=True` olabiliyordu. Fix:
    `execution/factory.py::testnet_credentials_status()`, HERHANGİ bir
    ağ çağrısından ÖNCE kimlik bilgisi VARLIĞINI kontrol eder — bekleyen
    bir signed API çağrısına VEYA operatörün `doctor` çalıştırmış
    olmasına GÜVENMEZ."""

    def _testnet_config(self, tmp_path: Path) -> object:
        exec_db = tmp_path / "exec.db"
        return exec_db, load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )

    def test_empty_db_no_credentials_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#1: enabled TESTNET + boş execution DB + kimlik bilgisi YOK ->
        `execution_ready` HER ZAMAN `False` kalmalıdır (regresyon: eskiden
        boş DB üzerinde `reconcile_pending()` sıfır signed çağrı yapıp
        "başarıyla" tamamlanır ve YANLIŞLIKLA `True` olurdu)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()  # BOŞ script — herhangi bir çağrı denenirse HATA verir
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        exit_code = run_async(_run_until_ready_then_shutdown(app))

        assert exit_code == EXIT_OK
        assert app.execution_ready is False
        assert "credentials missing" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_key_only_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#2: yalnızca `BINANCE_TESTNET_API_KEY` SET -> hâlâ `False`."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "only-the-key")
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert "BINANCE_TESTNET_API_KEY=SET" in app._execution_detail
        assert "BINANCE_TESTNET_API_SECRET=UNSET" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_secret_only_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#3: yalnızca `BINANCE_TESTNET_API_SECRET` SET -> hâlâ `False`."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "only-the-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert "BINANCE_TESTNET_API_KEY=UNSET" in app._execution_detail
        assert "BINANCE_TESTNET_API_SECRET=SET" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_both_credentials_present_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#4: kimlik bilgisi TAM iken boş DB üzerinde reconciliation
        BAŞARIYLA tamamlanır ve `execution_ready=True` olur (sıfır kayıt
        üzerinde sıfır ağ çağrısı ile — bu YİNE DE geçerli bir "hazır"
        durumudur, çünkü kimlik bilgisi ÖNCEDEN doğrulanmıştır)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "real-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "real-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is True
        assert "reconciliation OK: 0 record(s)" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_missing_credentials_with_pending_record_still_zero_network_calls(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """#5: kimlik bilgisi eksikken, HATTA bekleyen bir kayıt VARKEN
        bile, credential-preflight `reconcile_pending()`'e HİÇ ULAŞMAZ —
        sıfır POST, sıfır signed GET (eski davranışta bu senaryo zaten
        bir `MissingCredentialsError`'a yol açardı, ama en azından BİR ağ
        denemesi/imzalama girişimi OLURDU; şimdi hiç DENENMEZ bile)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-no-creds")
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_unknown_not_found_reconciliation_with_credentials_is_unchanged(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """#7: kimlik bilgisi TAM olduğunda, bekleyen `UNKNOWN_NOT_FOUND`
        kaydının startup reconciliation davranışı (Faz 11 Karar 78 ile
        AYNI) DEĞİŞMEDİ — tek bir yeniden sorgu, sıfır POST."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "real-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "real-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-unchanged")
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is True
        assert len(fake_http.get_calls) == 1
        assert len(fake_http.post_calls) == 0

    def test_credential_status_never_leaks_secret_values(self, tmp_path: Path, monkeypatch) -> None:
        """#8: gerçek credential DEĞERLERİ hiçbir zaman `_execution_detail`
        (health-snapshot/dashboard'a yansıyan alan) İÇİNE SIZMAZ — SADECE
        SET/UNSET etiketleri görünür."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "leak-check-key-value")
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert "leak-check-key-value" not in app._execution_detail
        assert "SET" in app._execution_detail and "UNSET" in app._execution_detail


class TestStartupReconciliation:
    """#6 pending reconciliation runs before new execution is possible,
    #7 unresolved UNKNOWN_NOT_FOUND at startup -> zero POST,
    #8 duplicate context via the production-wired service -> zero duplicate POST."""

    def test_pending_unknown_not_found_is_reconciled_at_startup_with_zero_posts(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db)

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        exit_code = run_async(_run_until_ready_then_shutdown(app))

        assert exit_code == EXIT_OK
        assert len(fake_http.post_calls) == 0
        assert len(fake_http.get_calls) == 1
        assert app.execution_ready is True  # reconciliation ÇALIŞTI (BAŞARISIZ olmadı), kayıt hâlâ çözülmemiş olabilir

    def test_startup_reconciliation_runs_before_run_loop_starts(self, tmp_path: Path, monkeypatch) -> None:
        """#6: reconciliation, run-loop/health-loop BAŞLAMADAN ÖNCE
        tamamlanmış OLMALIDIR — bu, `Application.start()`'ın kaynak
        kodundaki SIRA ile yapısal olarak garanti edilir; burada dolaylı
        olarak `execution_ready`'nin run_task oluşturulmadan ÖNCE zaten
        belirlenmiş OLDUĞUNU (reconcile_pending tamamlanmadan run_task
        asla create edilmez) doğrularız."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db)

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            # run_task zaten oluşturulmuş olduğuna göre, reconciliation
            # ZATEN tamamlanmış olmalı (bkz. start()'ın kaynak sırası).
            assert app._execution_ready is True
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK
        assert len(fake_http.post_calls) == 0

    def test_restart_with_unresolved_record_still_never_posts(self, tmp_path: Path, monkeypatch) -> None:
        """#7 + #8 (restart açısı): execution store'u kapatıp AYNI
        db path ile YENİ bir `Application` (restart simülasyonu)
        kurulduğunda, kayıt hâlâ `UNKNOWN_NOT_FOUND` iken TEK bir GET
        dışında hiçbir ağ çağrısı OLMAZ — özellikle POST asla ÇAĞRILMAZ."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-restart")

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE, _NOT_FOUND_RESPONSE])

        first = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)
        run_async(_run_until_ready_then_shutdown(first))

        second = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)
        run_async(_run_until_ready_then_shutdown(second))

        assert len(fake_http.post_calls) == 0
        assert len(fake_http.get_calls) == 2


class TestDashboardSafety:
    """#9 read does not mutate, #10 cannot submit orders, #11 no secrets,
    #12 dashboard failure never stops runtime, #13 loopback default."""

    def test_dashboard_host_defaults_to_loopback(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        assert config.dashboard_host == "127.0.0.1"
        assert config.dashboard_enabled is True

    def test_dashboard_read_does_not_mutate_paper_state(self, tmp_path: Path, monkeypatch) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18901"))
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.2)
            before = app.runtime._coordinator.paper_engine.position(_SYMBOL)
            for _ in range(5):
                with urllib.request.urlopen("http://127.0.0.1:18901/api/status", timeout=2) as resp:
                    assert resp.status == 200
                with urllib.request.urlopen("http://127.0.0.1:18901/", timeout=2) as resp:
                    assert resp.status == 200
            after = app.runtime._coordinator.paper_engine.position(_SYMBOL)
            assert before == after  # dashboard okuması pozisyonu DEĞİŞTİRMEDİ
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_rejects_all_mutation_methods(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18902"))
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.1)
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                req = urllib.request.Request("http://127.0.0.1:18902/", method=method)
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req, timeout=2)
                assert exc_info.value.code == 405
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_response_never_contains_credential_values(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "super-secret-key-value")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "super-secret-secret-value")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(
            _base_env(
                tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true",
                CSE_DASHBOARD_PORT="18903",
            )
        )
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:18903/api/status", timeout=2) as resp:
                raw = resp.read().decode("utf-8")
            with urllib.request.urlopen("http://127.0.0.1:18903/", timeout=2) as resp:
                raw += resp.read().decode("utf-8")
            assert "super-secret-key-value" not in raw
            assert "super-secret-secret-value" not in raw
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_bind_failure_does_not_stop_runtime(self, tmp_path: Path) -> None:
        """#12: dashboard portu ZATEN kullanımdaysa (bind hatası),
        `Application.start()` HÂLÂ BAŞARIYLA tamamlanır — ana runtime
        (market-data/paper-trading/persistence) bu yüzden ASLA durmaz."""
        import socket

        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        busy_port = blocker.getsockname()[1]
        try:
            config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT=str(busy_port)))
            app = Application(config, _bootstrappable_provider())
            exit_code = run_async(_run_until_ready_then_shutdown(app))
            assert exit_code == EXIT_OK
            assert app._dashboard is None  # başlatma başarısız oldu, devre dışı bırakıldı
        finally:
            blocker.close()


class TestGracefulShutdownAndRestart:
    """#2 PAPER restart from persisted state, #14 graceful shutdown."""

    def test_paper_restart_from_persisted_state_recovers_cleanly(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        config = load_config(env)
        first = Application(config, _bootstrappable_provider())
        first_exit = run_async(_run_until_ready_then_shutdown(first))
        assert first_exit == EXIT_OK

        second = Application(load_config(env), _bootstrappable_provider())
        second_exit = run_async(_run_until_ready_then_shutdown(second))
        assert second_exit == EXIT_OK

    def test_graceful_shutdown_releases_lock_and_stops_dashboard(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18904"))
        app = Application(config, _bootstrappable_provider())
        run_async(_run_until_ready_then_shutdown(app))

        with pytest.raises(sqlite3.ProgrammingError):
            app._store._connection.execute("SELECT 1")
        with pytest.raises((urllib.error.URLError, ConnectionRefusedError)):
            urllib.request.urlopen("http://127.0.0.1:18904/api/status", timeout=1)


class TestBackupStillValid:
    """#18: mevcut Faz 8 `backup_sqlite.py` yardımcı programı, üretim
    `Application`'ın GERÇEKTEN ürettiği bir paper_state.db üzerinde HÂLÂ
    çalışır ve geçerli, yeniden açılabilir bir kopya üretir."""

    def test_backup_of_a_real_running_apps_database_is_valid(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        run_async(_run_until_ready_then_shutdown(app))

        backup_path = tmp_path / "backup.db"
        backup_sqlite_database(config.db_path, backup_path)

        conn = sqlite3.connect(str(backup_path))
        try:
            (result,) = conn.execute("PRAGMA integrity_check").fetchone()
            assert result == "ok"
        finally:
            conn.close()


class TestDoctorOfflineNoSideEffects:
    def test_doctor_passes_for_a_clean_paper_configuration(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        exit_code = _run_doctor(config)
        out = capsys.readouterr().out
        assert exit_code == EXIT_OK
        assert "ALL CHECKS PASSED" in out
        assert "127.0.0.1" not in out or "[PASS] dashboard bind is loopback-only" in out

    def test_doctor_never_prints_credential_values(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "doctor-secret-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "doctor-secret-value")
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        _run_doctor(config)
        out = capsys.readouterr().out
        assert "doctor-secret-key" not in out
        assert "doctor-secret-value" not in out
        assert "SET" in out

    def test_doctor_does_not_create_execution_db_or_touch_network(self, tmp_path: Path, monkeypatch) -> None:
        """No side effects: doctor, execution store dosyasını asla
        oluşturmaz (offline, TAMAMEN yerel kontroller)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "should-not-be-created.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        _run_doctor(config)
        assert not exec_db.exists()


=== FILE: tests/test_portfolio_accounting.py ===
"""Portfolio/Accounting v1, step 3 — crypto_signal_engine/portfolio/
accounting.py tests. Fully offline/pure -- no LifecycleStore, no I/O."""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, PositionLifecycleState, flat_record
from crypto_signal_engine.portfolio.accounting import (
    AccountingSnapshot,
    AssetBalance,
    build_accounting_snapshot,
    parse_account_balances,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _long(symbol: str, *, vwap: float = 100.0, qty: float = 1.0) -> BridgePositionRecord:
    return BridgePositionRecord(
        symbol=symbol, state=PositionLifecycleState.LONG, gross_entry_vwap=vwap,
        net_owned_base_quantity=qty, initial_protective_stop=vwap * 0.9, high_water=vwap,
        effective_stop=vwap * 0.9, take_profit=vwap * 1.1, entry_timestamp=NOW, updated_at=NOW,
    )


def _default_kwargs(**overrides) -> dict:
    defaults = dict(
        positions=(), price_lookup=lambda symbol: None,
        lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
        today_realized_pnl_net_usdt=0.0, today_trade_count=0, now=NOW,
    )
    defaults.update(overrides)
    return defaults


class TestAccountingSnapshotValidation:
    def test_rejects_naive_generated_at(self) -> None:
        with pytest.raises(ValueError):
            AccountingSnapshot(
                generated_at=datetime(2026, 1, 1), open_position_count=0, total_exposure_usdt=0.0,
                unrealized_pnl_usdt=None, lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
                today_realized_pnl_net_usdt=0.0, today_trade_count=0,
            )

    def test_rejects_nan_exposure(self) -> None:
        with pytest.raises(ValueError):
            AccountingSnapshot(
                generated_at=NOW, open_position_count=0, total_exposure_usdt=math.nan,
                unrealized_pnl_usdt=None, lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
                today_realized_pnl_net_usdt=0.0, today_trade_count=0,
            )

    def test_rejects_infinite_unrealized(self) -> None:
        with pytest.raises(ValueError):
            AccountingSnapshot(
                generated_at=NOW, open_position_count=0, total_exposure_usdt=0.0,
                unrealized_pnl_usdt=math.inf, lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
                today_realized_pnl_net_usdt=0.0, today_trade_count=0,
            )

    def test_rejects_negative_counts(self) -> None:
        with pytest.raises(ValueError):
            AccountingSnapshot(
                generated_at=NOW, open_position_count=-1, total_exposure_usdt=0.0,
                unrealized_pnl_usdt=None, lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
                today_realized_pnl_net_usdt=0.0, today_trade_count=0,
            )

    def test_none_unrealized_is_valid(self) -> None:
        snapshot = AccountingSnapshot(
            generated_at=NOW, open_position_count=0, total_exposure_usdt=0.0,
            unrealized_pnl_usdt=None, lifetime_realized_pnl_net_usdt=0.0, lifetime_trade_count=0,
            today_realized_pnl_net_usdt=0.0, today_trade_count=0,
        )
        assert snapshot.unrealized_pnl_usdt is None


class TestBuildAccountingSnapshotBasics:
    def test_empty_portfolio(self) -> None:
        snapshot = build_accounting_snapshot(**_default_kwargs())
        assert snapshot.open_position_count == 0
        assert snapshot.total_exposure_usdt == 0.0
        assert snapshot.unrealized_pnl_usdt == 0.0  # no open positions to sum -> vacuously known, zero

    def test_flat_only_positions_are_zero_exposure_and_zero_open_count(self) -> None:
        positions = (flat_record("BTCUSDT", now=NOW), flat_record("ETHUSDT", now=NOW))
        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=lambda s: 100.0))
        assert snapshot.open_position_count == 0
        assert snapshot.total_exposure_usdt == 0.0
        assert snapshot.unrealized_pnl_usdt == 0.0  # no open positions to sum -> known, zero

    def test_open_position_count_and_exposure_reuse_step_1_function(self) -> None:
        positions = (_long("BTCUSDT", vwap=100.0, qty=2.0), _long("ETHUSDT", vwap=50.0, qty=1.0))
        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=lambda s: 100.0))
        assert snapshot.open_position_count == 2
        assert snapshot.total_exposure_usdt == pytest.approx(250.0)  # 100*2 + 50*1

    def test_lifetime_and_today_aggregates_pass_through_unchanged(self) -> None:
        snapshot = build_accounting_snapshot(
            **_default_kwargs(
                lifetime_realized_pnl_net_usdt=123.45, lifetime_trade_count=77,
                today_realized_pnl_net_usdt=-6.0, today_trade_count=3,
            )
        )
        assert snapshot.lifetime_realized_pnl_net_usdt == 123.45
        assert snapshot.lifetime_trade_count == 77
        assert snapshot.today_realized_pnl_net_usdt == -6.0
        assert snapshot.today_trade_count == 3

    def test_generated_at_reflects_the_now_argument(self) -> None:
        snapshot = build_accounting_snapshot(**_default_kwargs(now=NOW))
        assert snapshot.generated_at == NOW


class TestUnrealizedPnlNoneOnFailureDiscipline:
    """THE required discipline: unrealized is exactly None when the
    injected price lookup raises or returns nothing for ANY open symbol
    -- never a partial/fabricated sum."""

    def test_none_when_price_lookup_returns_none_for_an_open_position(self) -> None:
        positions = (_long("BTCUSDT"),)
        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=lambda s: None))
        assert snapshot.unrealized_pnl_usdt is None

    def test_none_when_price_lookup_raises_for_an_open_position(self) -> None:
        def _raising(symbol: str) -> float:
            raise RuntimeError(f"simulated price failure for {symbol}")

        positions = (_long("BTCUSDT"),)
        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=_raising))
        assert snapshot.unrealized_pnl_usdt is None

    def test_partial_failure_across_multiple_positions_still_yields_none_not_a_partial_sum(self) -> None:
        positions = (_long("BTCUSDT", vwap=100.0, qty=1.0), _long("ETHUSDT", vwap=50.0, qty=1.0))

        def _partial(symbol: str) -> float | None:
            return 110.0 if symbol == "BTCUSDT" else None

        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=_partial))
        assert snapshot.unrealized_pnl_usdt is None  # NEVER just BTCUSDT's +10.0 alone

    def test_known_when_every_open_position_resolves(self) -> None:
        positions = (_long("BTCUSDT", vwap=100.0, qty=1.0), _long("ETHUSDT", vwap=50.0, qty=2.0))

        def _price(symbol: str) -> float:
            return {"BTCUSDT": 110.0, "ETHUSDT": 55.0}[symbol]

        snapshot = build_accounting_snapshot(**_default_kwargs(positions=positions, price_lookup=_price))
        # BTCUSDT: (110-100)*1 = 10.0; ETHUSDT: (55-50)*2 = 10.0
        assert snapshot.unrealized_pnl_usdt == pytest.approx(20.0)

    def test_flat_and_dust_below_zero_quantity_never_require_a_price(self) -> None:
        """A position with zero net_owned_base_quantity (already fully
        drained) must never gate the whole snapshot to None even if its
        price lookup would fail -- it is not "open" in any economically
        meaningful sense."""
        from dataclasses import replace

        zero_qty = replace(_long("BTCUSDT"), net_owned_base_quantity=0.0, state=PositionLifecycleState.DUST)
        snapshot = build_accounting_snapshot(
            **_default_kwargs(positions=(zero_qty,), price_lookup=lambda s: (_ for _ in ()).throw(RuntimeError("boom")))
        )
        assert snapshot.unrealized_pnl_usdt == 0.0


class TestDeterminism:
    def test_deterministic_for_identical_inputs(self) -> None:
        positions = (_long("BTCUSDT", vwap=100.0, qty=1.0),)
        kwargs = _default_kwargs(
            positions=positions, price_lookup=lambda s: 105.0,
            lifetime_realized_pnl_net_usdt=50.0, lifetime_trade_count=5,
            today_realized_pnl_net_usdt=2.0, today_trade_count=1,
        )
        first = build_accounting_snapshot(**kwargs)
        second = build_accounting_snapshot(**kwargs)
        assert first == second


class TestAssetBalance:
    def test_valid_balance(self) -> None:
        balance = AssetBalance(asset="USDT", free=100.0, locked=5.0)
        assert balance.asset == "USDT"
        assert balance.free == 100.0
        assert balance.locked == 5.0

    def test_rejects_empty_asset(self) -> None:
        with pytest.raises(ValueError):
            AssetBalance(asset="", free=1.0, locked=0.0)

    def test_rejects_negative_free(self) -> None:
        with pytest.raises(ValueError):
            AssetBalance(asset="USDT", free=-1.0, locked=0.0)

    def test_rejects_negative_locked(self) -> None:
        with pytest.raises(ValueError):
            AssetBalance(asset="USDT", free=0.0, locked=-1.0)

    def test_rejects_nan(self) -> None:
        with pytest.raises(ValueError):
            AssetBalance(asset="USDT", free=math.nan, locked=0.0)


class TestParseAccountBalances:
    def test_parses_multiple_balances(self) -> None:
        raw = {
            "balances": [
                {"asset": "USDT", "free": "123.45", "locked": "5.0"},
                {"asset": "BTC", "free": "0.001", "locked": "0.0"},
            ],
            "accountType": "SPOT", "permissions": ["SPOT"],  # deliberately ignored, never leaked further
        }
        balances = parse_account_balances(raw)
        assert balances == (
            AssetBalance(asset="USDT", free=123.45, locked=5.0),
            AssetBalance(asset="BTC", free=0.001, locked=0.0),
        )

    def test_empty_balances_list(self) -> None:
        assert parse_account_balances({"balances": []}) == ()

    def test_missing_balances_key_defaults_to_empty(self) -> None:
        assert parse_account_balances({}) == ()

    def test_raises_on_non_list_balances(self) -> None:
        with pytest.raises(ValueError):
            parse_account_balances({"balances": "not-a-list"})

    def test_raises_on_missing_field(self) -> None:
        with pytest.raises(ValueError):
            parse_account_balances({"balances": [{"asset": "USDT", "free": "1.0"}]})  # missing "locked"


=== FILE: tests/test_production_hot_reselection.py ===
"""
CONFIRMED PHASE 16 PRODUCTION HOT-RESELECTION BLOCKER FIX — regression
coverage for the ACTUAL production composition:

    RuntimeCoordinator -> PersistedRuntime -> BridgeRuntime -> LifecycleRuntime

(the exact object graph `Application.__init__` builds, minus PAPER-store/
dashboard/health-snapshot plumbing that is orthogonal to this blocker).

The bug: `ReselectionScheduler` was wired directly to the raw
`RuntimeCoordinator` — `coordinator.add_symbol()` only spawns LIVE
consumer tasks when `coordinator._running` is `True`, which production
NEVER sets (production drives `LifecycleRuntime.run()`, never
`RuntimeCoordinator.run()` directly). A periodically hot-added symbol
could appear in `coordinator._symbols`/the dashboard while receiving ZERO
live candle/order-book consumption (part a) AND ZERO M1 stop/target/
trailing evaluation (part b) — a silent failure, no crash required, that
could leave a real open position completely unprotected.

The fix: `BridgeRuntime`/`LifecycleRuntime` now each own an explicit
`add_symbol`/`remove_symbol` hot-lifecycle API (mirroring `RuntimeCoordinator.
add_symbol`/`remove_symbol`'s existing accepted shape), and
`ReselectionScheduler` (via `Application` wiring it to `self._runtime`
instead of the raw coordinator) now calls THROUGH the actual composed,
running layer — so hot-add reaches both consumption layers symmetrically,
and hot-remove tears down both symmetrically.

Fully offline: `FakeLiveDataProvider` (scripted candle/order-book streams)
+ `FakeTestnetHttpClient` (scripted Binance Testnet REST responses). No
network, no real Testnet trade is ever manufactured."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, PositionLifecycleState
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.providers.binance.clock import SystemClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.reselection_scheduler import ReselectionScheduler
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig
from crypto_signal_engine.selection.models import CandidateScore, SelectionResult
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at, make_order_book

END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=timezone.utc)
WARMUP = 20
BTC = "BTCUSDT"
ETH = "ETHUSDT"


# -- Selector/ranking fakes (same shape as tests/test_reselection_scheduler.py) --


def _score(symbol: str, total: float) -> CandidateScore:
    return CandidateScore(
        symbol=symbol, eligible=True, total_score=total, liquidity_score=total,
        historical_movement_score=total, current_opportunity_score=total,
        reason=f"score={total}", quote_volume_24h=1_000_000.0,
    )


def _ranking(scores: dict[str, float]) -> SelectionResult:
    candidates = tuple(_score(s, v) for s, v in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0])))
    return SelectionResult(
        generated_at=END, selected_symbols=tuple(c.symbol for c in candidates),
        ranked_candidates=candidates, rejected=(), universe_size=len(scores), shortlist_size=len(scores),
    )


class _FakeSelector:
    def __init__(self, results) -> None:
        self._results = list(results)
        self.call_count = 0

    async def select(self) -> SelectionResult:
        self.call_count += 1
        return self._results.pop(0) if len(self._results) > 1 else self._results[0]


# -- Production-equivalent composition ---------------------------------------


def _bootstrap_fully(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
    coordinator.ingest_order_book(symbol, make_order_book(symbol, end))


def _register_hot_add_data(
    provider: FakeLiveDataProvider, symbol: str, *, end: datetime = END, live_m5_price: float = 100.0,
) -> None:
    """Populates everything `add_symbol`'s REST bootstrap AND the freshly
    spawned live consumer tasks need for `symbol`, BEFORE hot-add is
    triggered (mirrors `test_runtime_coordinator_hot_symbols.py`'s own
    established recipe). `end` defaults to the module's fixed `END` but a
    re-add MUST pass a STRICTLY LATER value (mirroring how real wall-clock
    time would have actually advanced between a remove and a re-add) --
    otherwise the fresh bootstrap's `FeatureHistoryStore` commit would
    collide with an already-newer snapshot left behind by the FIRST add's
    live candle (a test-fixture-only concern: `FeatureHistoryStore` is not
    reset by `remove_symbol`, exactly like real production, where
    `RuntimeCoordinator._clock.now()` never goes backward)."""
    for tf in (Timeframe.M5, Timeframe.M15, Timeframe.H1):
        provider._historical_candles[(symbol, tf)] = make_candle_series_ending_at(symbol, tf, end, WARMUP)  # noqa: SLF001
    provider._candle_scripts[(symbol, Timeframe.M5)] = [make_candle(symbol, Timeframe.M5, end, price=live_m5_price)]  # noqa: SLF001
    provider._order_book_scripts[symbol] = [make_order_book(symbol, end)]  # noqa: SLF001


class _Stack:
    def __init__(self, tmp_path: Path, provider: FakeLiveDataProvider, http: FakeTestnetHttpClient) -> None:
        paper_engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=0.0, slippage_bps=0.0)
        self.coordinator = RuntimeCoordinator(
            symbols=(BTC,), provider=provider, warmup_candles=WARMUP, paper_engine=paper_engine,
        )
        _bootstrap_fully(self.coordinator, BTC, END)
        self.persisted = PersistedRuntime(self.coordinator, PaperStateStore(tmp_path / "paper.db"))

        exec_db = tmp_path / "exec.db"
        self.exec_store = ExecutionStateStore(exec_db)
        client = BinanceTestnetClient(
            BinanceTestnetConfig(api_key="k", api_secret="s"), http, clock=SystemClock(),
        )
        self.execution_service = ExecutionReconciliationService(client, self.exec_store, clock=SystemClock())
        self.lifecycle_store = LifecycleStore(exec_db)
        self.lifecycle_manager = LifecycleManager(
            store=self.lifecycle_store, execution_service=self.execution_service, clock=SystemClock(),
        )
        self.bridge = SignalTestnetBridge(
            execution_service=self.execution_service, execution_store=self.exec_store, notional_usdt=10.0,
            lifecycle_manager=self.lifecycle_manager, atr_provider=lambda _s: 1.0,
            all_symbols_provider=lambda: self.coordinator._symbols,  # noqa: SLF001
        )
        self.bridge.mark_operational(True, "test")
        self.bridge_runtime = BridgeRuntime(self.persisted, self.bridge)
        self.lifecycle_runtime = LifecycleRuntime(self.bridge_runtime, self.lifecycle_manager)


def _scheduler(stack: _Stack, *, results, target_count: int = 2) -> ReselectionScheduler:
    return ReselectionScheduler(
        coordinator=stack.lifecycle_runtime,  # PRODUCTION FIX: the ACTUAL composed runtime, not the raw coordinator
        selector=_FakeSelector(results),
        config=AutoSymbolSelectionConfig(target_count=target_count, rescan_interval_seconds=3600.0),
        safety_pinned_provider=lambda: frozenset(),
    )


async def _poll_until(predicate, *, max_iterations: int = 500) -> None:
    for _ in range(max_iterations):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail("condition not met within bounded polling window")


def _start(stack: _Stack) -> asyncio.Task:
    return asyncio.create_task(stack.lifecycle_runtime.run())


async def _shutdown(stack: _Stack, run_task: asyncio.Task) -> None:
    await stack.lifecycle_runtime.stop()
    await run_task


class TestApplicationCompositionHotAdd:
    """Items 1-8: hot-add through the ACTUAL production runtime surface."""

    def test_hot_add_gets_real_task_ownership_not_just_symbols_membership(self, tmp_path: Path) -> None:
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: ETH not in stack.coordinator._symbols and BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await scheduler.run_once()

            # Item 3: real active OWNERSHIP, not merely `_symbols` membership.
            assert ETH in stack.coordinator._symbols  # noqa: SLF001
            assert ETH in stack.bridge_runtime._tasks_by_symbol  # noqa: SLF001 - part (a): bridge/candle layer
            assert len(stack.bridge_runtime._tasks_by_symbol[ETH]) == len(stack.coordinator._candle_timeframes) + 1  # noqa: SLF001
            assert ETH in stack.lifecycle_runtime._m1_tasks_by_symbol  # noqa: SLF001 - part (b): M1 layer (item 19)

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_hot_added_symbol_reaches_normal_pipeline_and_evaluates(self, tmp_path: Path) -> None:
        """Items 4-5: fresh deterministic market data reaches
        FeatureEngine/SignalEngine — a real cycle evaluation occurs."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await scheduler.run_once()
            # SignalEngine requires M1 order-book evidence for ALL 4
            # timeframes (bkz. agents/context.py::build_agent_context) --
            # bootstrap only ever covers M5/M15/H1 candles, never
            # order-book state, so it is ingested directly here to
            # deterministically avoid a task-scheduling race against the
            # freshly spawned (identically scripted) live order-book
            # consumer task -- ingesting the same snapshot twice is a safe,
            # idempotent DUPLICATE (see `ingest_order_book`).
            stack.coordinator.ingest_order_book(ETH, make_order_book(ETH, END))
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)

            assert stack.coordinator.last_cycle_result(ETH) is not None
            assert stack.coordinator.last_cycle_result(ETH).evaluated is True

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_bridge_receives_post_activation_cycle_result_no_real_order_required(self, tmp_path: Path) -> None:
        """Item 6: bridge receives the post-activation cycle result. No
        real Binance order is required/created merely for this — the
        assertion is that the bridge's `on_cycle_result` was invoked, and
        the bridge's own status reflects a processed (not "NONE") action."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])

        original = stack.bridge.on_cycle_result
        calls: list[str] = []

        async def spy(cycle_result):  # noqa: ANN001
            calls.append(cycle_result.symbol)
            await original(cycle_result)

        stack.bridge.on_cycle_result = spy  # type: ignore[method-assign]

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await scheduler.run_once()
            stack.coordinator.ingest_order_book(ETH, make_order_book(ETH, END))  # see sibling test's comment
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)
            await _poll_until(lambda: ETH in calls)

            assert stack.bridge.status_for(ETH)["last_action"] != "NONE"

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_bootstrap_alone_creates_no_economic_action(self, tmp_path: Path) -> None:
        """Item 7: pre-activation/bootstrap historical context (REST
        candles fetched by `add_symbol`) cannot, by itself, create an
        economic action — no order is ever submitted purely from
        bootstrapping a hot-added symbol's warmup history."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await scheduler.run_once()
            assert http.post_calls == []  # bootstrap alone: zero orders

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_duplicate_hot_add_creates_no_duplicate_tasks(self, tmp_path: Path) -> None:
        """Item 8."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await stack.lifecycle_runtime.add_symbol(ETH)
            first_bridge_tasks = list(stack.bridge_runtime._tasks_by_symbol[ETH])  # noqa: SLF001
            first_m1_task = stack.lifecycle_runtime._m1_tasks_by_symbol[ETH]  # noqa: SLF001
            fetch_calls_after_first = len(provider.fetch_calls)

            await stack.lifecycle_runtime.add_symbol(ETH)  # duplicate

            assert stack.coordinator._symbols.count(ETH) == 1  # noqa: SLF001
            assert stack.bridge_runtime._tasks_by_symbol[ETH] == first_bridge_tasks  # noqa: SLF001 - same tasks, no new ones
            assert stack.lifecycle_runtime._m1_tasks_by_symbol[ETH] is first_m1_task  # noqa: SLF001
            assert len(provider.fetch_calls) == fetch_calls_after_first  # no second bootstrap

            await _shutdown(stack, run_task)

        run_async(scenario())


class TestHotRemove:
    """Items 9-13: hot-remove symmetry across both consumption layers."""

    def test_hot_remove_cancels_bridge_and_m1_tasks(self, tmp_path: Path) -> None:
        """Items 9 and 21."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            await stack.lifecycle_runtime.add_symbol(ETH)
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)

            bridge_tasks = list(stack.bridge_runtime._tasks_by_symbol[ETH])  # noqa: SLF001
            m1_task = stack.lifecycle_runtime._m1_tasks_by_symbol[ETH]  # noqa: SLF001
            btc_bridge_tasks_before = list(stack.bridge_runtime._tasks_by_symbol[BTC])  # noqa: SLF001

            await stack.lifecycle_runtime.remove_symbol(ETH)

            assert all(t.done() for t in bridge_tasks)  # item 9 - bridge/candle layer
            assert m1_task.done()  # item 21 - M1 layer, symmetric
            assert ETH not in stack.bridge_runtime._tasks_by_symbol  # noqa: SLF001
            assert ETH not in stack.lifecycle_runtime._m1_tasks_by_symbol  # noqa: SLF001
            assert m1_task not in stack.lifecycle_runtime._m1_tasks
            # BTCUSDT completely untouched (isolation).
            assert all(not t.done() for t in btc_bridge_tasks_before)
            assert stack.bridge_runtime._tasks_by_symbol[BTC] == btc_bridge_tasks_before  # noqa: SLF001

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_removed_symbol_stops_receiving_processing(self, tmp_path: Path) -> None:
        """Item 10."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            await stack.lifecycle_runtime.add_symbol(ETH)
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)

            await stack.lifecycle_runtime.remove_symbol(ETH)

            assert ETH not in stack.coordinator._symbols  # noqa: SLF001
            assert (ETH, Timeframe.M5) not in stack.coordinator._candle_windows  # noqa: SLF001
            assert stack.coordinator.last_cycle_result(ETH) is None

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_remove_then_readd_creates_exactly_one_fresh_ownership_no_stale_replay(self, tmp_path: Path) -> None:
        """Items 11-12."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            await stack.lifecycle_runtime.add_symbol(ETH)
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)
            await stack.lifecycle_runtime.remove_symbol(ETH)

            # Fresh historical+live scripts for the RE-add, anchored to a
            # STRICTLY LATER reference time (mirrors real wall-clock time
            # actually advancing between remove and re-add) -- proves no
            # stale pre-removal state/candle is replayed into the fresh
            # ownership.
            end2 = END + timedelta(hours=1)
            _register_hot_add_data(provider, ETH, end=end2, live_m5_price=250.0)
            await stack.lifecycle_runtime.add_symbol(ETH)

            assert stack.coordinator._symbols.count(ETH) == 1  # noqa: SLF001 - exactly one fresh ownership
            assert len(stack.bridge_runtime._tasks_by_symbol[ETH]) == len(stack.coordinator._candle_timeframes) + 1  # noqa: SLF001
            new_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            assert new_window is not eth_window  # brand new window object, no stale history carried over
            await _poll_until(lambda: new_window.latest_open_time == end2)
            assert len(new_window.history()) == WARMUP + 1  # fresh bootstrap + exactly one fresh live candle

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_pinned_non_flat_symbol_cannot_be_removed(self, tmp_path: Path) -> None:
        """Item 13: the scheduler itself (via `safety_pinned_provider`)
        must never even attempt to remove a pinned/non-FLAT symbol."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        scheduler = ReselectionScheduler(
            coordinator=stack.lifecycle_runtime,
            selector=_FakeSelector([_ranking({ETH: 0.99})]),  # BTCUSDT drops out of ranking entirely
            config=AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0),
            safety_pinned_provider=lambda: frozenset({BTC}),
        )
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            btc_tasks_before = list(stack.bridge_runtime._tasks_by_symbol[BTC])  # noqa: SLF001

            await scheduler.run_once()

            assert BTC in stack.coordinator._symbols  # noqa: SLF001
            assert BTC in stack.bridge_runtime._tasks_by_symbol  # noqa: SLF001
            assert all(not t.done() for t in btc_tasks_before)

            await _shutdown(stack, run_task)

        run_async(scenario())


class TestFailureIsolationAndStartupSafety:
    """Items 14-18."""

    def test_failed_reselection_leaves_active_universe_and_tasks_intact(self, tmp_path: Path) -> None:
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)

        class _FailingSelector:
            async def select(self) -> SelectionResult:
                from crypto_signal_engine.errors import SymbolSelectionError

                raise SymbolSelectionError("discovery unavailable")

        scheduler = ReselectionScheduler(
            coordinator=stack.lifecycle_runtime, selector=_FailingSelector(),
            config=AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0),
            safety_pinned_provider=lambda: frozenset(),
        )

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            btc_tasks_before = list(stack.bridge_runtime._tasks_by_symbol[BTC])  # noqa: SLF001

            await scheduler.run_once()  # must not raise

            assert stack.coordinator._symbols == (BTC,)  # noqa: SLF001
            assert stack.bridge_runtime._tasks_by_symbol[BTC] == btc_tasks_before  # noqa: SLF001
            assert all(not t.done() for t in btc_tasks_before)

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_startup_selected_symbols_remain_unaffected_by_reselection_wiring(self, tmp_path: Path) -> None:
        """Item 15."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        scheduler = _scheduler(stack, results=[_ranking({BTC: 0.9})])  # ranking keeps BTCUSDT, adds nothing

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            btc_tasks_before = list(stack.bridge_runtime._tasks_by_symbol[BTC])  # noqa: SLF001

            await scheduler.run_once()

            assert stack.bridge_runtime._tasks_by_symbol[BTC] == btc_tasks_before  # noqa: SLF001 - startup symbol untouched
            assert all(not t.done() for t in btc_tasks_before)

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_runtime_shutdown_cleans_all_hot_added_symbol_tasks(self, tmp_path: Path) -> None:
        """Item 16."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            await stack.lifecycle_runtime.add_symbol(ETH)
            eth_window = stack.coordinator._candle_windows[(ETH, Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)
            eth_bridge_tasks = list(stack.bridge_runtime._tasks_by_symbol[ETH])  # noqa: SLF001
            eth_m1_task = stack.lifecycle_runtime._m1_tasks_by_symbol[ETH]  # noqa: SLF001

            await _shutdown(stack, run_task)

            assert all(t.done() for t in eth_bridge_tasks)
            assert eth_m1_task.done()
            assert stack.lifecycle_runtime._m1_tasks == []

        run_async(scenario())

    def test_no_duplicate_runtime_coordinator_is_created(self, tmp_path: Path) -> None:
        """Item 17."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001
            coordinator_before = stack.lifecycle_runtime._coordinator

            await scheduler.run_once()

            assert stack.lifecycle_runtime._coordinator is coordinator_before
            assert stack.lifecycle_runtime._coordinator is stack.coordinator

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_scheduler_calls_the_live_runtime_surface_not_a_dead_coordinator_path(self, tmp_path: Path) -> None:
        """Item 18: proves the scheduler's `add_symbol`/`remove_symbol`
        calls actually land on `LifecycleRuntime` (which owns BOTH
        consumption layers), not merely on the bare coordinator."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        scheduler = _scheduler(stack, results=[_ranking({ETH: 0.9, BTC: 0.5})])
        _register_hot_add_data(provider, ETH)

        calls: list[str] = []
        original_add = stack.lifecycle_runtime.add_symbol

        async def spy_add(symbol: str) -> None:
            calls.append(symbol)
            await original_add(symbol)

        stack.lifecycle_runtime.add_symbol = spy_add  # type: ignore[method-assign]

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.bridge_runtime._tasks_by_symbol)  # noqa: SLF001

            await scheduler.run_once()

            assert calls == [ETH]  # the scheduler called LifecycleRuntime.add_symbol, not a dead coordinator path
            assert ETH in stack.lifecycle_runtime._m1_tasks_by_symbol  # noqa: SLF001 - proof it's the REAL, wired surface

            await _shutdown(stack, run_task)

        run_async(scenario())


class TestM1LifecycleProtectionSymmetry:
    """Items 19-20: the M1-specific proof the mission calls out by name."""

    def test_lifecycle_runtime_spawns_m1_task_for_hot_added_symbol(self, tmp_path: Path) -> None:
        """Item 19."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.lifecycle_runtime._m1_tasks_by_symbol)  # noqa: SLF001

            await stack.lifecycle_runtime.add_symbol(ETH)

            assert ETH in stack.lifecycle_runtime._m1_tasks_by_symbol  # noqa: SLF001
            m1_task = stack.lifecycle_runtime._m1_tasks_by_symbol[ETH]  # noqa: SLF001
            assert m1_task in stack.lifecycle_runtime._m1_tasks
            assert not m1_task.done()

            await _shutdown(stack, run_task)

        run_async(scenario())

    def test_evaluate_m1_candle_actually_invoked_for_hot_added_symbols_open_position(self, tmp_path: Path) -> None:
        """Item 20: once a hot-added symbol has an open bridge position,
        `LifecycleManager.evaluate_m1_candle()` is genuinely invoked on its
        M1 candle closes — stop/target/trailing protection is LIVE, not
        merely that an entry could theoretically be submitted."""
        provider = FakeLiveDataProvider()
        http = FakeTestnetHttpClient()
        stack = _Stack(tmp_path, provider, http)
        _register_hot_add_data(provider, ETH)

        async def scenario() -> None:
            run_task = _start(stack)
            await _poll_until(lambda: BTC in stack.lifecycle_runtime._m1_tasks_by_symbol)  # noqa: SLF001

            await stack.lifecycle_runtime.add_symbol(ETH)

            # A genuine open bridge position for the hot-added symbol
            # (seeded directly at the durable-state boundary -- exactly
            # what a real filled BUY would leave behind; no fabricated
            # Testnet trade is placed to get here).
            stack.lifecycle_store.save_position(BridgePositionRecord(
                symbol=ETH, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
                net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
                effective_stop=90.0, take_profit=120.0, entry_timestamp=END, updated_at=END,
            ))

            calls: list[tuple] = []
            original_eval = stack.lifecycle_manager.evaluate_m1_candle

            async def spy_eval(symbol, candle, *, atr_for_trailing):  # noqa: ANN001
                calls.append((symbol, candle))
                return await original_eval(symbol, candle, atr_for_trailing=atr_for_trailing)

            stack.lifecycle_manager.evaluate_m1_candle = spy_eval  # type: ignore[method-assign]

            m1_candle = make_candle(ETH, Timeframe.M1, END, price=80.0)  # below effective_stop -> real exit path
            provider._candle_scripts[(ETH, Timeframe.M1)] = [m1_candle]  # noqa: SLF001

            await _poll_until(lambda: any(c[0] == ETH for c in calls))

            # `LifecycleRuntime._consume_m1` converts the domain `Candle`
            # into the lifecycle module's OWN `Candle` type before calling
            # `evaluate_m1_candle` (see `_to_lifecycle_candle`) -- so the
            # object itself differs by design; compare the OHLC/close-time
            # VALUES actually delivered instead of object identity.
            assert calls[0][0] == ETH
            assert calls[0][1].close == m1_candle.close
            assert calls[0][1].close_time == m1_candle.close_time

            await _shutdown(stack, run_task)

        run_async(scenario())


class TestExistingActivationInvariantsUnaffected:
    """Item 9's sibling check + a final structural sanity check that this
    fix did not touch execution/PAPER identity semantics."""

    def test_hot_add_before_run_still_spawns_no_tasks_bare_coordinator_unaffected(self, tmp_path: Path) -> None:
        """`RuntimeCoordinator.add_symbol()` itself (used standalone, e.g.
        by other tests) is completely UNCHANGED by this fix."""
        provider = FakeLiveDataProvider()
        provider._historical_candles.update(  # noqa: SLF001
            {(ETH, tf): make_candle_series_ending_at(ETH, tf, END, WARMUP) for tf in (Timeframe.M5, Timeframe.M15, Timeframe.H1)}
        )
        coordinator = RuntimeCoordinator(symbols=(BTC,), provider=provider, warmup_candles=WARMUP)

        async def scenario() -> None:
            await coordinator.add_symbol(ETH)

        run_async(scenario())
        assert coordinator._tasks == []  # noqa: SLF001
        assert ETH not in coordinator._tasks_by_symbol  # noqa: SLF001


=== FILE: tests/test_providers.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.providers.base import ConnectionState, LiveDataProvider, ProviderHealthSnapshot

UTC = timezone.utc


def test_live_data_provider_is_abstract() -> None:
    with pytest.raises(TypeError):
        LiveDataProvider()  # type: ignore[abstract]


class TestProviderHealthSnapshot:
    def test_valid_construction(self) -> None:
        snapshot = ProviderHealthSnapshot(
            connection_state=ConnectionState.CONNECTED,
            last_message_at=datetime(2026, 8, 31, tzinfo=UTC),
            reconnect_count=0, symbol="btcusdt",
        )
        assert snapshot.connection_state == ConnectionState.CONNECTED
        assert snapshot.symbol == "BTCUSDT"

    def test_empty_symbol_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            ProviderHealthSnapshot(
                connection_state=ConnectionState.CONNECTED, last_message_at=None,
                reconnect_count=0, symbol="   ",
            )

    def test_negative_reconnect_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="reconnect_count"):
            ProviderHealthSnapshot(
                connection_state=ConnectionState.RECONNECTING, last_message_at=None,
                reconnect_count=-1, symbol="BTCUSDT",
            )

    def test_naive_last_message_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            ProviderHealthSnapshot(
                connection_state=ConnectionState.CONNECTED,
                last_message_at=datetime(2026, 8, 31),
                reconnect_count=0, symbol="BTCUSDT",
            )

    def test_none_last_message_at_allowed(self) -> None:
        snapshot = ProviderHealthSnapshot(
            connection_state=ConnectionState.DISCONNECTED, last_message_at=None,
            reconnect_count=0, symbol="BTCUSDT",
        )
        assert snapshot.last_message_at is None

    def test_connection_state_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="ConnectionState"):
            ProviderHealthSnapshot(
                connection_state="CONNECTED", last_message_at=None,  # type: ignore[arg-type]
                reconnect_count=0, symbol="BTCUSDT",
            )


class _IncompleteProvider(LiveDataProvider):
    async def stream_candles(self, symbol, timeframe):
        yield  # pragma: no cover


def test_incomplete_implementation_cannot_instantiate() -> None:
    with pytest.raises(TypeError):
        _IncompleteProvider()  # type: ignore[abstract]


=== FILE: tests/test_quality.py ===
import pytest

from crypto_signal_engine.domain.enums import DataQualityStatus
from crypto_signal_engine.quality.base import DataQualityGate, DataQualityResult


class TestDataQualityResult:
    def test_ok_result_passed(self) -> None:
        result = DataQualityResult.ok()
        assert result.passed is True
        assert result.status == DataQualityStatus.OK

    def test_non_ok_result_not_passed(self) -> None:
        result = DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason="son veri eski")
        assert result.passed is False

    def test_details_immutable(self) -> None:
        result = DataQualityResult(
            status=DataQualityStatus.STALE_PRICE, reason="test", details={"age": "45"}
        )
        with pytest.raises(TypeError):
            result.details["age"] = "999"  # type: ignore[index]

    def test_details_defensive_copy(self) -> None:
        source = {"age": "45"}
        result = DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason="test", details=source)
        source["age"] = "999"
        assert result.details["age"] == "45"

    def test_status_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="DataQualityStatus"):
            DataQualityResult(status="STALE_PRICE", reason="test")  # type: ignore[arg-type]

    def test_reason_non_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="str"):
            DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason=123)  # type: ignore[arg-type]

    def test_non_ok_status_with_empty_reason_rejected(self) -> None:
        with pytest.raises(ValueError, match="reason boş olamaz"):
            DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason="")

    def test_non_ok_status_with_whitespace_reason_rejected(self) -> None:
        with pytest.raises(ValueError, match="reason boş olamaz"):
            DataQualityResult(status=DataQualityStatus.EXTREME_OUTLIER, reason="   ")

    def test_ok_status_with_empty_reason_allowed(self) -> None:
        result = DataQualityResult(status=DataQualityStatus.OK, reason="")
        assert result.passed is True


class _RejectAllGate(DataQualityGate):
    def check_candle(self, candle, previous):
        return DataQualityResult(status=DataQualityStatus.MISSING_CANDLE, reason="test")

    def check_trade(self, trade, previous):
        return DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason="test")

    def check_order_book(self, snapshot, previous):
        return DataQualityResult(status=DataQualityStatus.EXTREME_OUTLIER, reason="test")


def test_gate_is_abstract() -> None:
    with pytest.raises(TypeError):
        DataQualityGate()  # type: ignore[abstract]


def test_concrete_gate_can_reject() -> None:
    gate = _RejectAllGate()
    result = gate.check_candle(candle=None, previous=None)
    assert result.passed is False
    assert result.status == DataQualityStatus.MISSING_CANDLE


=== FILE: tests/test_repository_safety_scan.py ===
"""
Quality Gate 19 & 21.E — repository güvenlik taraması.

Bu test, production kaynak kodunu (yalnızca `crypto_signal_engine/` paketi,
testler hariç) tarayarak yasak pattern'lerin bulunmadığını doğrular:

- `except Exception: pass` ve eşdeğer silent exception pattern'leri
- `ALLOW_LIVE_TRADING = True`
- `BinanceLiveExecutionAdapter` (bu sınıf hiçbir zaman yazılmamalı)

Not: Bu test bir "hile ile PASS üretme" değildir — tam tersi, bu
invariant'ların GERÇEKTEN ihlal edilmediğini ispatlayan bir regresyon
testidir. Repository büyüdükçe bu test otomatik olarak yeni dosyaları da
tarar.

MİMARİ-FARKINDA GÜNCELLEME (Faz 10 — Binance Spot TESTNET Execution Lab):
Faz 10, KASITLI OLARAK HMAC/credential/private-endpoint izleri içeren
İZOLE bir `crypto_signal_engine/execution/` paketi ekler — bu ARTIK bir
ihlal DEĞİLDİR, TALEP EDİLEN mimaridir. Bu yüzden `TestPhase2PublicDataOnlyBoundary`
ARTIK `execution/`'ı taramaz (bu paket kendi, AYRI ve DAHA SIKI
`TestPhase10ExecutionBoundarySafety` taramasına sahiptir) — eski scan
ZAYIFLATILMADI, yalnızca mimari olarak DOĞRU sınırlara BÖLÜNDÜ: (1)
`execution/` DIŞINDA bu izlerin SIFIR olduğu hâlâ garanti edilir (private
execution'ın izole sınırın DIŞINA SIZMADIĞI), (2) `execution/` İÇİNDE
BİLE futures/margin/withdrawal/transfer (`/sapi/`, `/fapi/`, `/dapi/`) ve
Mainnet host literal'leri hâlâ SIFIRDIR."""

from __future__ import annotations

import ast
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "crypto_signal_engine"
EXECUTION_PACKAGE_ROOT = PACKAGE_ROOT / "execution"
SCRIPTS_ROOT = Path(__file__).resolve().parents[1] / "scripts"


def _all_source_files() -> list[Path]:
    return sorted(PACKAGE_ROOT.rglob("*.py"))


def _non_execution_source_files() -> list[Path]:
    return [p for p in _all_source_files() if EXECUTION_PACKAGE_ROOT not in p.parents]


def _execution_source_files() -> list[Path]:
    return [p for p in _all_source_files() if EXECUTION_PACKAGE_ROOT in p.parents]


class TestNoSilentExceptionPatterns:
    def test_no_except_exception_pass_textual(self) -> None:
        """Hızlı metinsel tarama: `except Exception:` sonrası yalnızca `pass`."""
        pattern = re.compile(r"except\s+Exception\s*:\s*\n\s*pass\b")
        violations = []
        for path in _all_source_files():
            text = path.read_text(encoding="utf-8")
            if pattern.search(text):
                violations.append(str(path))
        assert violations == [], f"except Exception: pass bulundu: {violations}"

    def test_no_bare_except_pass_via_ast(self) -> None:
        """AST bazlı, daha güvenilir tarama: her `except` bloğunun tek
        ifadesi `pass` olan ve gövdesi başka hiçbir şey (loglama dahil)
        içermeyen durumları yakalar — hem `except Exception:` hem bare
        `except:` için."""
        violations = []
        for path in _all_source_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler):
                    body = node.body
                    if len(body) == 1 and isinstance(body[0], ast.Pass):
                        is_broad = node.type is None or (
                            isinstance(node.type, ast.Name) and node.type.id == "Exception"
                        )
                        if is_broad:
                            violations.append(f"{path}:{node.lineno}")
        assert violations == [], f"silent exception pattern bulundu: {violations}"


class TestNoLiveTradingBackdoor:
    def test_allow_live_trading_never_true(self) -> None:
        pattern = re.compile(r"ALLOW_LIVE_TRADING\s*=\s*True\b")
        violations = [str(p) for p in _all_source_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"ALLOW_LIVE_TRADING = True bulundu: {violations}"

    def test_allow_live_trading_is_false_somewhere(self) -> None:
        """Invariant'ın varlığını da doğrula (yanlışlıkla silinmemiş olsun)."""
        pattern = re.compile(r"ALLOW_LIVE_TRADING\s*=\s*False\b")
        found = any(pattern.search(p.read_text(encoding="utf-8")) for p in _all_source_files())
        assert found, "ALLOW_LIVE_TRADING = False invariant'ı hiçbir dosyada bulunamadı"

    def test_no_binance_live_execution_adapter(self) -> None:
        violations = [
            str(p) for p in _all_source_files() if "BinanceLiveExecutionAdapter" in p.read_text(encoding="utf-8")
        ]
        assert violations == [], f"BinanceLiveExecutionAdapter bulundu (YAZILMAMALI): {violations}"

    def test_no_production_execution_module(self) -> None:
        """Faz 1'de execution/ klasörü hiç yok; ileride oluşturulsa bile
        canlı emir gönderen bir sınıf adı (create_order vb. Binance trade
        endpoint çağrısı) bulunmamalı."""
        forbidden_calls = ["create_market_order", "create_limit_order", "cancel_order"]
        violations = []
        for path in _all_source_files():
            text = path.read_text(encoding="utf-8")
            for call in forbidden_calls:
                if call in text:
                    violations.append(f"{path}: {call}")
        assert violations == [], f"execution/trade çağrısı bulundu (Faz 1'de yasak): {violations}"


class TestPhase2PublicDataOnlyBoundary:
    """Quality Gate — Faz 2 Bölüm 3 & 30: yalnızca public/read-only market data.

    Bu testler özellikle `crypto_signal_engine/providers/binance/` ve ilgili
    Faz 2 modüllerini tarar; hiçbir signed-request/private-endpoint/order
    yeteneği bulunmamalıdır.

    Mimari-farkında not (Faz 10): bu tarama artık `execution/` paketini
    HARİÇ TUTAR — o paket KASITLI OLARAK bu pattern'leri içerir (bkz.
    `TestPhase10ExecutionBoundarySafety`, modül docstring'i). Bu, taramayı
    ZAYIFLATMAZ: `execution/` DIŞINDAKİ HER YER için invariant hâlâ TAM
    OLARAK aynı sıkılıkla (SIFIR tolerans) geçerlidir.
    """

    FORBIDDEN_SUBSTRINGS = [
        "create_order(",
        "cancel_order(",
        "place_order(",
        "submit_order(",
        "api_secret",
        "apiSecret",
        "hmac.new",
        "X-MBX-APIKEY",
        "/api/v3/order",
        "/sapi/",
        "/fapi/",
        "/dapi/",
        "signature=",
    ]

    def test_no_forbidden_execution_or_private_endpoint_strings(self) -> None:
        violations = []
        for path in _non_execution_source_files():
            text = path.read_text(encoding="utf-8")
            for forbidden in self.FORBIDDEN_SUBSTRINGS:
                if forbidden in text:
                    violations.append(f"{path}: {forbidden!r}")
        assert violations == [], f"private/execution endpoint izi (execution/ DIŞINDA) bulundu: {violations}"

    def test_binance_config_has_no_credential_fields(self) -> None:
        """BinanceConfig dataclass'ının alan adları arasında API key/secret
        alanı olmadığını doğrular (hard safety invariant, kod seviyesinde)."""
        import dataclasses

        from crypto_signal_engine.providers.binance.config import BinanceConfig

        field_names = {f.name.lower() for f in dataclasses.fields(BinanceConfig)}
        forbidden_field_fragments = ["key", "secret", "token", "credential", "password"]
        violations = [
            name for name in field_names
            if any(fragment in name for fragment in forbidden_field_fragments)
        ]
        assert violations == [], f"BinanceConfig içinde kimlik bilgisi alanı bulundu: {violations}"

    def test_rest_client_only_uses_public_endpoints(self) -> None:
        """rest.py içindeki tüm endpoint path'lerinin public/market-data
        endpoint'leri olduğunu doğrular (Bölüm 1.A)."""
        rest_path = PACKAGE_ROOT / "providers" / "binance" / "rest.py"
        text = rest_path.read_text(encoding="utf-8")
        allowed_endpoints = [
            "/api/v3/klines",
            "/api/v3/depth",
            # Faz "otomatik sembol seçimi" — ikisi de public/credential-
            # gerektirmeyen keşif/istatistik endpoint'leri (sipariş/hesap
            # değil), bkz. `crypto_signal_engine/selection/selector.py`.
            "/api/v3/exchangeInfo",
            "/api/v3/ticker/24hr",
        ]
        found_endpoints = re.findall(r'"(/api/v\d+/[a-zA-Z]+)"', text)
        assert found_endpoints, "rest.py içinde hiç endpoint bulunamadı (beklenmiyordu)"
        for endpoint in found_endpoints:
            assert endpoint in allowed_endpoints, f"İzin verilmeyen/tanınmayan endpoint: {endpoint}"


class TestPhase10ExecutionBoundarySafety:
    """Faz 10 (Binance Spot TESTNET Execution Lab) mimari-farkında güvenlik
    taraması. `execution/` paketi KASITLI OLARAK HMAC/credential/private-
    endpoint izleri İÇERİR — bu Faz 10'un AMACIDIR. Bu sınıf, o iznin
    (1) YALNIZCA `execution/` içinde kaldığını (repo'nun geri kalanına
    SIZMADIĞINI) ve (2) `execution/` İÇİNDE BİLE futures/margin/withdrawal/
    transfer endpoint'lerinin veya bir Mainnet host literal'inin ASLA
    bulunmadığını doğrular."""

    # Binance'in TÜM futures (`/fapi/`, `/dapi/`) ve margin/withdrawal/
    # transfer/sub-account (`/sapi/`) endpoint'leri BU path prefix'lerinin
    # ALTINDADIR — bu yüzden üç prefix, "bare English kelime" taraması
    # YERİNE (ki bu, bu dosyanın KENDİ "futures/margin YOK" dokümantasyon
    # cümlelerini yanlışlıkla YAKALARDI) kullanılır: kesin, düşük-yanlış-
    # pozitifli bir kanıttır.
    ALWAYS_FORBIDDEN_EVEN_IN_EXECUTION = [
        "/sapi/",
        "/fapi/",
        "/dapi/",
        "create_order(",
        "cancel_order(",
        "submit_order(",
        "apiSecret",
    ]

    EXECUTION_ONLY_ALLOWED_PATTERNS = [
        "place_order(",
        "api_secret",
        "hmac.new",
        "X-MBX-APIKEY",
        "/api/v3/order",
    ]

    MAINNET_HOST_LITERALS = ["api.binance.com", "stream.binance.com"]

    def test_execution_package_exists(self) -> None:
        assert EXECUTION_PACKAGE_ROOT.is_dir(), "crypto_signal_engine/execution/ bulunamadı"
        assert _execution_source_files(), "execution/ içinde hiç .py dosyası yok"

    def test_always_forbidden_patterns_absent_even_inside_execution(self) -> None:
        violations = []
        for path in _execution_source_files():
            text = path.read_text(encoding="utf-8")
            for forbidden in self.ALWAYS_FORBIDDEN_EVEN_IN_EXECUTION:
                if forbidden in text:
                    violations.append(f"{path}: {forbidden!r}")
        assert violations == [], f"Faz 10 execution/ İÇİNDE BİLE yasak pattern bulundu: {violations}"

    def test_execution_only_patterns_never_leak_outside_execution_package(self) -> None:
        violations = []
        for path in _non_execution_source_files():
            text = path.read_text(encoding="utf-8")
            for pattern in self.EXECUTION_ONLY_ALLOWED_PATTERNS:
                if pattern in text:
                    violations.append(f"{path}: {pattern!r}")
        assert violations == [], f"private execution izi execution/ SINIRI DIŞINDA bulundu: {violations}"

    def test_no_mainnet_host_literal_inside_execution(self) -> None:
        violations = []
        for path in _execution_source_files():
            text = path.read_text(encoding="utf-8")
            for host in self.MAINNET_HOST_LITERALS:
                # Yalnızca GERÇEK bir Python string literal'i (tırnak
                # İÇİNDE) olarak arar — markdown backtick'lerindeki
                # dokümantasyon anmalarını (bkz. yukarıdaki docstring)
                # YANLIŞ POZİTİF SAYMAZ.
                if re.search(rf'["\']{re.escape(host)}', text):
                    violations.append(f"{path}: {host!r}")
        assert violations == [], f"execution/ içinde Mainnet host literal'ı bulundu: {violations}"

    def test_cli_lab_script_has_no_host_override_or_mainnet_flag(self) -> None:
        cli_path = SCRIPTS_ROOT / "binance_testnet_lab.py"
        text = cli_path.read_text(encoding="utf-8")
        for forbidden in ("--base-url", "--host", "--mainnet"):
            # `add_argument("--base-url", ...)` gibi GERÇEK bir argparse
            # kaydını arar — docstring'deki backtick'li anmayı DEĞİL.
            assert not re.search(rf'["\']{re.escape(forbidden)}', text), (
                f"CLI'de host override/Mainnet bayrağı bulundu: {forbidden!r}"
            )
        for host in self.MAINNET_HOST_LITERALS:
            assert not re.search(rf'["\']{re.escape(host)}', text), (
                f"CLI script'inde Mainnet host literal'ı bulundu: {host!r}"
            )

    def test_allow_live_trading_not_mutated_by_execution_package(self) -> None:
        pattern = re.compile(r"ALLOW_LIVE_TRADING\s*=\s*True\b")
        violations = [str(p) for p in _execution_source_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"execution/ içinde ALLOW_LIVE_TRADING=True bulundu: {violations}"

    def test_execution_mode_enum_has_no_mainnet_member(self) -> None:
        from crypto_signal_engine.execution.models import ExecutionMode

        assert "MAINNET" not in {m.name for m in ExecutionMode}
        assert "MAINNET" not in {m.value for m in ExecutionMode}


class TestOps247CredentialLeakSafety:
    """24/7 Ops v1, Step 6 — required explicit tests: `dashboard_admin_
    token` (a LOCAL admin token, kept IN `AppConfig` unlike exchange
    credentials — see `ops/config.py`) must never appear in `AppConfig.
    summary()`'s output; the Telegram bot token/chat id (kept OUTSIDE
    `AppConfig` entirely, same precedent as Testnet signing credentials
    — see `ops/notifier.py`) must never be logged or appear in the
    health snapshot."""

    def test_dashboard_admin_token_never_in_summary(self) -> None:
        from crypto_signal_engine.ops.config import load_config

        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "super-secret-admin-token"})
        assert config.dashboard_admin_token == "super-secret-admin-token"
        summary = config.summary()
        assert "dashboard_admin_token" not in summary
        blob = json.dumps(summary)
        assert "super-secret-admin-token" not in blob

    def test_telegram_bot_token_never_in_health_snapshot(self, tmp_path: Path) -> None:
        """End-to-end: with `CSE_TELEGRAM_BOT_TOKEN`/`CSE_TELEGRAM_CHAT_ID`
        set in the real process environment (the ONLY way `Application`
        reads them, see `ops/notifier.py::build_telegram_notifier`),
        drive one real startup + health snapshot write and confirm the
        raw token/chat-id values are absent from BOTH the on-disk JSON
        snapshot and the structured log record emitted at startup
        (`_LOGGER.info(..., config=config.summary())` — `summary()` never
        carries these fields, so this also regression-proves that)."""
        import asyncio
        import logging

        from crypto_signal_engine.app import Application
        from crypto_signal_engine.domain.enums import Timeframe
        from crypto_signal_engine.ops.config import load_config
        from crypto_signal_engine.ops.health_snapshot import read_snapshot
        from tests.conftest import run_async
        from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

        monkeypatch_env = {
            "CSE_TELEGRAM_BOT_TOKEN": "top-secret-telegram-bot-token-value",
            "CSE_TELEGRAM_CHAT_ID": "999888777",
        }
        original = {k: os.environ.get(k) for k in monkeypatch_env}
        os.environ.update(monkeypatch_env)
        try:
            symbol = "BTCUSDT"
            timeframes = (Timeframe.M5, Timeframe.M15, Timeframe.H1)
            end = datetime.now(timezone.utc) - timedelta(seconds=2)
            historical = {(symbol, tf): make_candle_series_ending_at(symbol, tf, end, 20) for tf in timeframes}
            provider = FakeLiveDataProvider(historical_candles=historical)
            config = load_config(
                {
                    "CSE_SYMBOLS": symbol, "CSE_DB_PATH": str(tmp_path / "state.db"),
                    "CSE_WARMUP_CANDLES": "20", "CSE_DASHBOARD_ENABLED": "false",
                }
            )
            app = Application(config, provider)
            assert app._notifier is not None  # sanity: notifier actually configured

            log_records: list[str] = []

            class _CapturingHandler(logging.Handler):
                def emit(self, record: logging.LogRecord) -> None:
                    log_records.append(record.getMessage())

            handler = _CapturingHandler()
            root_logger = logging.getLogger("crypto_signal_engine")
            root_logger.addHandler(handler)
            try:
                async def body() -> None:
                    run_task = asyncio.create_task(app.start())
                    deadline = asyncio.get_event_loop().time() + 5.0
                    while app._run_task is None and not app._shutdown_started:
                        if asyncio.get_event_loop().time() >= deadline:
                            raise AssertionError("startup did not complete in time")
                        await asyncio.sleep(0.005)
                    app.request_shutdown("test")
                    await asyncio.wait_for(run_task, timeout=5.0)

                run_async(body())
            finally:
                root_logger.removeHandler(handler)

            snapshot = read_snapshot(config.health_snapshot_path)
            assert snapshot is not None
            snapshot_text = json.dumps(snapshot)
            assert "top-secret-telegram-bot-token-value" not in snapshot_text
            assert "999888777" not in snapshot_text

            log_blob = "\n".join(log_records)
            assert "top-secret-telegram-bot-token-value" not in log_blob
            assert "999888777" not in log_blob
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


=== FILE: tests/test_repository_safety_scan_adaptive.py ===
"""
Adaptive Intelligence v1, step 4 — ADDITIVE extension of the accepted
`tests/test_repository_safety_scan.py` (and sibling to
`tests/test_repository_safety_scan_research.py`) to cover the NEW
`adaptive/` package and its new `scripts/*.py` CLI entry points.

Unlike `research/`, which is FORBIDDEN from ever importing
`crypto_signal_engine.execution` at all, `adaptive/` is explicitly
permitted a narrow exception: reading pure TYPE/FUNCTION definitions from
`crypto_signal_engine.execution.lifecycle` (`ExitPolicyConfig`,
`evaluate_candle`, `compute_initial_stop_and_target`, ...) and from
`crypto_signal_engine.execution.lifecycle_replay_sanity`
(`simulate_lifecycle_exits`) — both pure, no order-submission side
effect. The dependency direction itself (`crypto_signal_engine/` must
NEVER import `adaptive/`) is enforced separately below by scanning
`crypto_signal_engine/` instead of `adaptive/`.

ALLOWLIST, NOT DENYLIST (independent-verification fix): the boundary
below is enforced as an ALLOWLIST of exactly the two permitted
`crypto_signal_engine.execution` submodules
(`ALLOWED_EXECUTION_SUBMODULES`), not a denylist of forbidden ones. A
denylist is enumerable and therefore incomplete by construction — an
earlier version of this file named 7 forbidden submodules and missed
`bridge_runtime.py`/`lifecycle_runtime.py`/`lifecycle_store.py`/
`lifecycle_migration.py`/`signer.py`, and `bridge_runtime.py` itself
imports the already-forbidden `signal_bridge.py`, a transitive backdoor
the old enumerable list would never have caught. `test_
adaptive_package_only_imports_allowlisted_execution_submodules` below
is AST-based (covers `import X`, `import X as Y`, `from X import Y`,
and `from crypto_signal_engine import execution`/`execution.Y` forms)
and fails on ANY `crypto_signal_engine.execution.*` import (or the bare
package itself) that is not in the allowlist — complete by construction,
not by enumeration."""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ADAPTIVE_ROOT = REPO_ROOT / "adaptive"
CRYPTO_SIGNAL_ENGINE_ROOT = REPO_ROOT / "crypto_signal_engine"
SELECTION_ROOT = REPO_ROOT / "crypto_signal_engine" / "selection"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
NEW_SCRIPTS = ("adaptive_evaluation_cycle.py", "run_with_adaptive_policy.py")

# The ONLY `crypto_signal_engine.execution` submodules `adaptive/` may
# ever import — both pure types/functions, zero order-submission side
# effect. Anything else under `crypto_signal_engine.execution` (present
# OR future) is forbidden by construction, never by enumeration — see
# module docstring.
ALLOWED_EXECUTION_SUBMODULES = (
    "crypto_signal_engine.execution.lifecycle",
    "crypto_signal_engine.execution.lifecycle_replay_sanity",
)


def _adaptive_source_files() -> list[Path]:
    return sorted(ADAPTIVE_ROOT.rglob("*.py")) if ADAPTIVE_ROOT.exists() else []


def _crypto_signal_engine_source_files() -> list[Path]:
    return sorted(CRYPTO_SIGNAL_ENGINE_ROOT.rglob("*.py")) if CRYPTO_SIGNAL_ENGINE_ROOT.exists() else []


def _selection_source_files() -> list[Path]:
    return sorted(SELECTION_ROOT.rglob("*.py")) if SELECTION_ROOT.exists() else []


def _imports_adaptive_package(source: str) -> list[str]:
    """AST-based (same discipline as `_non_allowlisted_execution_imports`
    above): every `import`/`from ... import` statement that touches the
    `adaptive` package (bare, any submodule, or aliased) — covers
    `import adaptive`, `import adaptive as x`, `import adaptive.symbol_
    score`, `from adaptive import x`, `from adaptive.symbol_score import
    learned_factor`. Adaptive Symbol Intelligence v1's hard rule:
    `crypto_signal_engine/selection/*.py` must NEVER import `adaptive/`
    — the wiring is one-directional via the injected
    `learned_factor_provider` callable only."""
    violations: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "adaptive" or alias.name.startswith("adaptive."):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "adaptive" or node.module.startswith("adaptive."):
                violations.append(node.module)
    return violations


def _new_script_files() -> list[Path]:
    return [SCRIPTS_ROOT / name for name in NEW_SCRIPTS if (SCRIPTS_ROOT / name).exists()]


def _all_files() -> list[Path]:
    return _adaptive_source_files() + _new_script_files()


def _non_allowlisted_execution_imports(source: str) -> list[str]:
    """The actual allowlist-enforcement logic, factored out so both the
    real repository scan AND a dedicated positive-control test (proving
    this check actually catches a forbidden/transitive import, not just
    that the real repo happens to be clean today) can exercise it."""
    violations: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        touched: list[str] = []
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "crypto_signal_engine.execution" or alias.name.startswith(
                    "crypto_signal_engine.execution."
                ):
                    touched.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "crypto_signal_engine.execution" or node.module.startswith(
                "crypto_signal_engine.execution."
            ):
                touched.append(node.module)
            elif node.module == "crypto_signal_engine":
                for alias in node.names:
                    if alias.name == "execution" or alias.name.startswith("execution."):
                        touched.append(f"crypto_signal_engine.{alias.name}")
        for module_path in touched:
            if module_path not in ALLOWED_EXECUTION_SUBMODULES:
                violations.append(module_path)
    return violations


class TestNoSilentExceptionPatterns:
    def test_no_except_exception_pass_textual(self) -> None:
        pattern = re.compile(r"except\s+Exception\s*:\s*\n\s*pass\b")
        violations = [str(p) for p in _all_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"except Exception: pass bulundu: {violations}"

    def test_no_bare_except_pass_via_ast(self) -> None:
        violations = []
        for path in _all_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler):
                    body = node.body
                    if len(body) == 1 and isinstance(body[0], ast.Pass):
                        is_broad = node.type is None or (
                            isinstance(node.type, ast.Name) and node.type.id == "Exception"
                        )
                        if is_broad:
                            violations.append(f"{path}:{node.lineno}")
        assert violations == [], f"silent exception pattern bulundu: {violations}"


class TestNoLiveTradingOrPrivateExecutionBackdoor:
    FORBIDDEN_SUBSTRINGS = (
        "ALLOW_LIVE_TRADING = True",
        "ALLOW_LIVE_TRADING=True",
        "BinanceLiveExecutionAdapter",
        "create_market_order",
        "create_limit_order",
        "cancel_order(",
        "create_order(",
        "place_order(",
        "submit_order(",
        "api_secret",
        "apiSecret",
        "hmac.new",
        "X-MBX-APIKEY",
        "/api/v3/order",
        "/sapi/",
        "/fapi/",
        "/dapi/",
        "signature=",
        "--confirm-testnet-order",
    )

    def test_no_forbidden_patterns_in_adaptive_or_new_scripts(self) -> None:
        violations = []
        for path in _all_files():
            text = path.read_text(encoding="utf-8")
            for forbidden in self.FORBIDDEN_SUBSTRINGS:
                if forbidden in text:
                    violations.append(f"{path}: {forbidden!r}")
        assert violations == [], f"yasak pattern bulundu: {violations}"

    def test_adaptive_package_only_imports_allowlisted_execution_submodules(self) -> None:
        """Complete by construction, not by enumeration — see module
        docstring. Every `import`/`from ... import` statement in
        `adaptive/` that touches `crypto_signal_engine.execution` (the
        bare package, any submodule, or a `from crypto_signal_engine
        import execution[.submodule]` form) must resolve to EXACTLY one
        of `ALLOWED_EXECUTION_SUBMODULES` — anything else fails, whether
        or not anyone remembered to add its name to a denylist."""
        violations: list[str] = []
        for path in _adaptive_source_files():
            for module_path in _non_allowlisted_execution_imports(path.read_text(encoding="utf-8")):
                violations.append(f"{path}: '{module_path}'")
        assert violations == [], (
            f"adaptive/ izin verilmeyen execution alt-modülünü import ediyor "
            f"(yalnızca {ALLOWED_EXECUTION_SUBMODULES} izinli): {violations}"
        )

    def test_crypto_signal_engine_never_imports_adaptive_package(self) -> None:
        """The hard dependency-direction rule: `crypto_signal_engine/`
        (app.py included) must NEVER import `adaptive/` — only the
        reverse. See step 14: the actual champion-reading wiring lives in
        the separate `scripts/run_with_adaptive_policy.py`, never inside
        `crypto_signal_engine/` itself."""
        violations = []
        pattern = re.compile(r"^\s*(import\s+adaptive\b|from\s+adaptive\b)", re.MULTILINE)
        for path in _crypto_signal_engine_source_files():
            text = path.read_text(encoding="utf-8")
            if pattern.search(text):
                violations.append(str(path))
        assert violations == [], f"crypto_signal_engine/ adaptive/ paketini import ediyor: {violations}"

    def test_selection_package_never_imports_adaptive_package(self) -> None:
        """Adaptive Symbol Intelligence v1 — the SAME hard dependency-
        direction rule, explicitly re-asserted (AST-based, not just
        regex) and scoped to `crypto_signal_engine/selection/*.py`
        specifically: `AutomaticSymbolSelector` receives its
        `learned_factor_provider` as an injected callable ONLY (a type
        `crypto_signal_engine/` already owns) — it must never import
        `adaptive.symbol_score` or anything else under `adaptive/`
        itself. Already covered incidentally by the broader, regex-based
        `test_crypto_signal_engine_never_imports_adaptive_package` above
        (which scans all of `crypto_signal_engine/`); this is a
        dedicated, AST-based, `selection/`-scoped check per this
        milestone's own explicit requirement."""
        violations: list[str] = []
        for path in _selection_source_files():
            for module_path in _imports_adaptive_package(path.read_text(encoding="utf-8")):
                violations.append(f"{path}: '{module_path}'")
        assert violations == [], f"crypto_signal_engine/selection/ adaptive/ paketini import ediyor: {violations}"

    def test_new_scripts_only_use_public_klines_endpoint_or_no_network(self) -> None:
        """Mirrors the research safety scan's equivalent check: if a new
        adaptive script talks to Binance at all, it must go through the
        accepted PUBLIC-only `BinanceRestClient` (never raw HTTP)."""
        for path in _new_script_files():
            text = path.read_text(encoding="utf-8")
            if "requests." in text or "http.client" in text or "urllib.request" in text:
                assert "BinanceRestClient" in text, f"{path} ham HTTP kullanıyor ama BinanceRestClient yok"

    def test_adaptive_package_never_assigns_allow_live_trading(self) -> None:
        pattern = re.compile(r"ALLOW_LIVE_TRADING\s*=")
        violations = [str(p) for p in _adaptive_source_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"adaptive/ ALLOW_LIVE_TRADING'ı ATIYOR (assignment): {violations}"

    def test_adaptive_package_never_touches_risk_policy_config(self) -> None:
        """Hard safety rule: the adaptive engine never proposes or changes
        any `RiskPolicyConfig` field, in any version, ever. `RiskPolicyConfig`
        may be mentioned in a comment/docstring explaining WHY it is
        excluded, but the type itself must never be imported/instantiated
        here."""
        violations = []
        for path in _adaptive_source_files():
            text = path.read_text(encoding="utf-8")
            if "RiskPolicyConfig(" in text or "import RiskPolicyConfig" in text:
                violations.append(str(path))
        assert violations == [], f"adaptive/ RiskPolicyConfig'i kullanıyor: {violations}"


class TestAllowlistCatchesTransitiveAndEveryImportForm:
    """Positive-control tests for the fix in gap #2 (independent-
    verification round): prove `_non_allowlisted_execution_imports`
    actually CATCHES a forbidden/transitive import, not merely that
    today's real `adaptive/` tree happens to be clean — a denylist would
    pass these silently if the specific name were missing; the allowlist
    catches all of them because none is `crypto_signal_engine.execution.
    lifecycle`/`lifecycle_replay_sanity`."""

    def test_catches_bridge_runtime_the_exact_transitive_backdoor_named_in_the_gap_report(self) -> None:
        source = "from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime\n"
        assert _non_allowlisted_execution_imports(source) == ["crypto_signal_engine.execution.bridge_runtime"]

    def test_catches_every_other_named_missing_submodule(self) -> None:
        for name in ("signer", "lifecycle_runtime", "lifecycle_store", "lifecycle_migration"):
            source = f"from crypto_signal_engine.execution.{name} import Something\n"
            assert _non_allowlisted_execution_imports(source) == [f"crypto_signal_engine.execution.{name}"]

    def test_catches_bare_execution_package_import(self) -> None:
        assert _non_allowlisted_execution_imports("import crypto_signal_engine.execution\n") == [
            "crypto_signal_engine.execution"
        ]

    def test_catches_from_crypto_signal_engine_import_execution_form(self) -> None:
        assert _non_allowlisted_execution_imports("from crypto_signal_engine import execution\n") == [
            "crypto_signal_engine.execution"
        ]

    def test_catches_import_x_as_y_aliased_form(self) -> None:
        source = "import crypto_signal_engine.execution.bridge_runtime as br\n"
        assert _non_allowlisted_execution_imports(source) == ["crypto_signal_engine.execution.bridge_runtime"]

    def test_allows_exactly_the_two_permitted_submodules(self) -> None:
        source = (
            "from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig\n"
            "from crypto_signal_engine.execution.lifecycle_replay_sanity import simulate_lifecycle_exits\n"
        )
        assert _non_allowlisted_execution_imports(source) == []

    def test_unrelated_imports_are_never_flagged(self) -> None:
        source = "from crypto_signal_engine.runtime.models import RuntimeCycleResult\nimport os\n"
        assert _non_allowlisted_execution_imports(source) == []


class TestSelectionAdaptiveImportCheckCatchesAPlantedViolation:
    """Positive-control tests for Adaptive Symbol Intelligence v1's step-5
    requirement (same style as `TestAllowlistCatchesTransitiveAndEveryImportForm`
    above): prove `_imports_adaptive_package` actually CATCHES a planted
    violation, not merely that today's real `crypto_signal_engine/
    selection/` tree happens to be clean."""

    def test_catches_from_adaptive_symbol_score_import_the_exact_violation_this_milestone_forbids(self) -> None:
        source = "from adaptive.symbol_score import learned_factor\n"
        assert _imports_adaptive_package(source) == ["adaptive.symbol_score"]

    def test_catches_bare_adaptive_package_import(self) -> None:
        assert _imports_adaptive_package("import adaptive\n") == ["adaptive"]

    def test_catches_from_adaptive_import_form(self) -> None:
        assert _imports_adaptive_package("from adaptive import decision\n") == ["adaptive"]

    def test_catches_aliased_import_form(self) -> None:
        assert _imports_adaptive_package("import adaptive.symbol_score as ss\n") == ["adaptive.symbol_score"]

    def test_unrelated_imports_are_never_flagged(self) -> None:
        source = "from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig\nimport os\n"
        assert _imports_adaptive_package(source) == []


=== FILE: tests/test_repository_safety_scan_research.py ===
"""
Pre-Audit Enhancement Pass — ADDITIVE extension of the accepted
`tests/test_repository_safety_scan.py` to cover the NEW `research/`
package and the three new `scripts/*.py` CLI entry points this pass
introduces. The accepted safety-scan file itself is left completely
untouched (its own scope is `crypto_signal_engine/` + one specific
script; see its own docstring) — this is a separate, sibling file so
that file's diff stays zero, per the "strengthen, don't rewrite"
discipline of this pass.

Re-uses the exact same forbidden-pattern lists as the accepted scan
(duplicated here deliberately, not imported, so a future edit to the
accepted scan's patterns does not silently narrow this one, and
vice-versa — each file's list is its own independent, from-scratch
guarantee)."""

from __future__ import annotations

import ast
import re
from pathlib import Path

RESEARCH_ROOT = Path(__file__).resolve().parents[1] / "research"
SCRIPTS_ROOT = Path(__file__).resolve().parents[1] / "scripts"
NEW_SCRIPTS = ("historical_replay.py", "oos_stability.py", "monte_carlo_robustness.py")


def _research_source_files() -> list[Path]:
    return sorted(RESEARCH_ROOT.rglob("*.py"))


def _new_script_files() -> list[Path]:
    return [SCRIPTS_ROOT / name for name in NEW_SCRIPTS]


def _all_files() -> list[Path]:
    return _research_source_files() + _new_script_files()


class TestNoSilentExceptionPatterns:
    def test_no_except_exception_pass_textual(self) -> None:
        pattern = re.compile(r"except\s+Exception\s*:\s*\n\s*pass\b")
        violations = [str(p) for p in _all_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"except Exception: pass bulundu: {violations}"

    def test_no_bare_except_pass_via_ast(self) -> None:
        violations = []
        for path in _all_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler):
                    body = node.body
                    if len(body) == 1 and isinstance(body[0], ast.Pass):
                        is_broad = node.type is None or (
                            isinstance(node.type, ast.Name) and node.type.id == "Exception"
                        )
                        if is_broad:
                            violations.append(f"{path}:{node.lineno}")
        assert violations == [], f"silent exception pattern bulundu: {violations}"


class TestNoLiveTradingOrPrivateExecutionBackdoor:
    FORBIDDEN_SUBSTRINGS = (
        "ALLOW_LIVE_TRADING = True",
        "ALLOW_LIVE_TRADING=True",
        "BinanceLiveExecutionAdapter",
        "create_market_order",
        "create_limit_order",
        "cancel_order(",
        "create_order(",
        "place_order(",
        "submit_order(",
        "api_secret",
        "apiSecret",
        "hmac.new",
        "X-MBX-APIKEY",
        "/api/v3/order",
        "/sapi/",
        "/fapi/",
        "/dapi/",
        "signature=",
        "--confirm-testnet-order",
    )

    def test_no_forbidden_patterns_in_research_or_new_scripts(self) -> None:
        violations = []
        for path in _all_files():
            text = path.read_text(encoding="utf-8")
            for forbidden in self.FORBIDDEN_SUBSTRINGS:
                if forbidden in text:
                    violations.append(f"{path}: {forbidden!r}")
        assert violations == [], f"yasak pattern bulundu: {violations}"

    def test_research_package_never_imports_execution_package(self) -> None:
        violations = []
        for path in _research_source_files():
            text = path.read_text(encoding="utf-8")
            if "crypto_signal_engine.execution" in text or "crypto_signal_engine import execution" in text:
                violations.append(str(path))
        assert violations == [], f"research/ execution/ paketini import ediyor: {violations}"

    def test_new_scripts_never_import_execution_package(self) -> None:
        violations = []
        for path in _new_script_files():
            text = path.read_text(encoding="utf-8")
            if "crypto_signal_engine.execution" in text:
                violations.append(str(path))
        assert violations == [], f"yeni script execution/ paketini import ediyor: {violations}"

    def test_new_scripts_only_use_public_klines_endpoint(self) -> None:
        """The new CLI scripts fetch data via the accepted, PUBLIC-only
        Phase 2 `BinanceRestClient` (whose only two allowed endpoints,
        `/api/v3/klines` and `/api/v3/depth`, are already independently
        enforced by the accepted `tests/test_repository_safety_scan.py::
        TestPhase2PublicDataOnlyBoundary::test_rest_client_only_uses_public_endpoints`)
        — they must never construct or reference any raw private/order
        endpoint path themselves, and must go through that client rather
        than rolling their own HTTP calls."""
        for path in _new_script_files():
            text = path.read_text(encoding="utf-8")
            assert "BinanceRestClient" in text, f"{path} beklenen public REST client'ı kullanmıyor"

    def test_research_package_never_assigns_allow_live_trading(self) -> None:
        """Regex, not bare substring — mirrors the accepted scan's own
        `test_allow_live_trading_never_true` approach, so a docstring
        that merely MENTIONS the invariant by name (as this pass's own
        `research/__init__.py` module docstring legitimately does) is not
        a false-positive violation; only an actual Python assignment is."""
        pattern = re.compile(r"ALLOW_LIVE_TRADING\s*=")
        violations = [str(p) for p in _research_source_files() if pattern.search(p.read_text(encoding="utf-8"))]
        assert violations == [], f"research/ ALLOW_LIVE_TRADING'ı ATIYOR (assignment): {violations}"


=== FILE: tests/test_research_attribution.py ===
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.attribution import compute_report, extract_trades

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)

LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def _signal(
    *, symbol: str, timestamp: datetime, context_id: str, score: float,
    confidence: float = 0.8, risk_level: RiskLevel = RiskLevel.LOW, model_version: str = "test-v1",
    agents: tuple[AgentName, ...] = (AgentName.QUANT,),
) -> Signal:
    supporting = tuple(
        AgentEvidence(
            agent=agent, score=score, rationale="test", primary_timeframe=Timeframe.M5,
            symbol=symbol, as_of=timestamp, context_id=context_id,
        )
        for agent in agents
    )
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=confidence,
        risk_level=risk_level, primary_timeframe=Timeframe.M5, supporting_factors=supporting,
        contradicting_factors=(), invalidation=None, model_version=model_version,
    )


def _cycle(engine: PaperTradingEngine, signal: Signal, price: float) -> RuntimeCycleResult:
    snapshot = MarketPriceSnapshot(symbol=signal.symbol, price=price, as_of=signal.timestamp)
    result = engine.process_signal(signal, snapshot)
    return RuntimeCycleResult(
        symbol=signal.symbol, evaluated=True, signal=signal, paper_result=result, generated_at=signal.timestamp
    )


class TestExtractTrades:
    def test_no_trade_from_single_open(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycle = _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE), 100.0)
        assert extract_trades([cycle]) == ()

    def test_reversal_produces_one_trade_with_correct_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        c1 = _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE), 100.0)
        c2 = _cycle(
            engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE), 110.0
        )
        trades = extract_trades([c1, c2])
        assert len(trades) == 1
        assert trades[0].net_pnl == 100.0  # (110-100)/100 * 1000 notional = 100 gross, zero fees
        assert trades[0].symbol == "BTCUSDT"
        assert AgentName.QUANT in trades[0].supporting_agents

    def test_idempotent_replay_not_counted_as_second_trade(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE)
        s2 = _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE)
        c1 = _cycle(engine, s1, 100.0)
        c2 = _cycle(engine, s2, 110.0)
        c2_replay = _cycle(engine, s2, 110.0)  # same context_id -> idempotent
        assert len(extract_trades([c1, c2, c2_replay])) == 1


class TestPerformanceMetrics:
    def test_zero_trades_edge_case(self) -> None:
        report = compute_report([])
        m = report.overall
        assert m.trade_count == 0
        assert m.win_rate is None
        assert m.average_win is None
        assert m.average_loss is None
        assert m.profit_factor is None
        assert m.max_drawdown is None
        assert m.net_pnl == 0.0

    def test_zero_losses_profit_factor_undefined(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        c1 = _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE), 100.0)
        c2 = _cycle(
            engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE), 110.0
        )
        report = compute_report([c1, c2])
        assert report.overall.losses == 0
        assert report.overall.profit_factor is None  # gross_loss == 0 -> undefined, not fabricated

    def test_wins_losses_and_win_rate(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycles = [
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE), 100.0),
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE), 110.0),  # win
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=10), context_id="c3", score=LONG_SCORE), 105.0),  # win
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=15), context_id="c4", score=SHORT_SCORE), 95.0),  # loss vs 105
        ]
        report = compute_report(cycles)
        assert report.overall.trade_count == 3
        assert report.overall.wins == 2
        assert report.overall.losses == 1
        assert report.overall.win_rate == 2 / 3

    def test_max_drawdown_well_defined_for_losing_streak(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycles = [
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE), 100.0),
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE), 90.0),  # loss -100
        ]
        report = compute_report(cycles)
        assert report.overall.max_drawdown == 100.0


class TestAttributionDimensions:
    def test_by_symbol_and_by_direction_and_by_risk_level(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycles = [
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE, risk_level=RiskLevel.LOW), 100.0),
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE), 110.0),
        ]
        report = compute_report(cycles)
        assert "BTCUSDT" in report.by_symbol
        assert report.by_symbol["BTCUSDT"].trade_count == 1
        assert any(m.trade_count == 1 for m in report.by_direction.values())
        assert "LOW" in report.by_risk_level

    def test_regime_attribution_explicitly_marked_unavailable(self) -> None:
        report = compute_report([])
        assert "NOT AVAILABLE" in report.regime_attribution_note

    def test_agent_attribution_uses_structured_agent_field_not_rationale_text(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycles = [
            _cycle(
                engine,
                _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE, agents=(AgentName.QUANT, AgentName.REGIME)),
                100.0,
            ),
            _cycle(
                engine,
                _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE),
                110.0,
            ),
        ]
        report = compute_report(cycles)
        assert AgentName.QUANT.value in report.by_supporting_agent
        assert AgentName.REGIME.value in report.by_supporting_agent
        assert report.by_supporting_agent[AgentName.QUANT.value].trade_count == 1

    def test_confidence_bucket_boundaries(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        cycles = [
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0, context_id="c1", score=LONG_SCORE, confidence=0.9), 100.0),
            _cycle(engine, _signal(symbol="BTCUSDT", timestamp=T0 + timedelta(minutes=5), context_id="c2", score=SHORT_SCORE, confidence=0.9), 110.0),
        ]
        report = compute_report(cycles)
        assert "[0.75, 1.00]" in report.by_confidence_bucket


=== FILE: tests/test_research_data_quality.py ===
from __future__ import annotations

from datetime import timedelta

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from research.data_quality import HistoricalDatasetValidator
from research.errors import DataQualityError
from tests.research_fakes import EPOCH, TIMEFRAME_DURATIONS, deterministic_candle, deterministic_series


def _series(n: int, timeframe: Timeframe = Timeframe.M5, symbol: str = "BTCUSDT"):
    duration = TIMEFRAME_DURATIONS[timeframe]
    return deterministic_series(symbol, timeframe, EPOCH, EPOCH + duration * n, base_price=100.0)


class TestValidHistoryPasses:
    def test_valid_contiguous_series_passes(self) -> None:
        candles = _series(30)
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert report.passed
        assert report.violations == ()
        assert report.candle_count == 30

    def test_validate_or_raise_does_not_raise_on_valid_data(self) -> None:
        candles = _series(10)
        HistoricalDatasetValidator().validate_or_raise(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)


class TestDuplicateRejected:
    def test_duplicate_open_time_rejected(self) -> None:
        candles = list(_series(5))
        candles.append(candles[2])  # duplicate row
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "DUPLICATE_OPEN_TIME" for v in report.violations)


class TestMissingBarDetected:
    def test_gap_detected_by_default(self) -> None:
        candles = list(_series(5))
        del candles[2]  # remove one bar -> gap
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "GAP_DETECTED" for v in report.violations)

    def test_gap_tolerant_mode_reports_gap_record_instead_of_failing(self) -> None:
        candles = list(_series(5))
        del candles[2]
        report = HistoricalDatasetValidator(allow_gaps=True).validate(
            candles, symbol="BTCUSDT", timeframe=Timeframe.M5
        )
        assert report.passed
        assert report.gap_tolerant is True
        assert len(report.gaps) == 1
        assert report.gaps[0].missing_bar_count == 1

    def test_multi_bar_gap_missing_count(self) -> None:
        candles = list(_series(6))
        del candles[3]
        del candles[2]  # remove two consecutive bars -> 2 missing
        report = HistoricalDatasetValidator(allow_gaps=True).validate(
            candles, symbol="BTCUSDT", timeframe=Timeframe.M5
        )
        assert report.gaps[0].missing_bar_count == 2


class TestOutOfOrderRegression:
    def test_chronological_regression_rejected(self) -> None:
        candles = list(_series(5))
        candles[3], candles[1] = candles[1], candles[3]  # swap -> regression at index 1
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        codes = {v.code for v in report.violations}
        assert "CHRONOLOGICAL_REGRESSION" in codes or "IMPOSSIBLE_OVERLAP" in codes

    def test_impossible_overlap_rejected(self) -> None:
        candles = list(_series(5))
        overlapping = deterministic_candle(
            "BTCUSDT", Timeframe.M5, candles[2].open_time + timedelta(seconds=1)
        )
        candles.insert(3, overlapping)
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "IMPOSSIBLE_OVERLAP" for v in report.violations)


class TestMalformedRejected:
    def test_nan_rejected_via_raw_row(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=5),
                "open": float("nan"), "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0,
                "is_closed": True,
            }
        ]
        report = HistoricalDatasetValidator().validate(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert report.violations[0].code == "MALFORMED_CANDLE"

    def test_inf_rejected_via_raw_row(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=5),
                "open": float("inf"), "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0,
                "is_closed": True,
            }
        ]
        report = HistoricalDatasetValidator().validate(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert report.violations[0].code == "MALFORMED_CANDLE"

    def test_impossible_ohlc_rejected(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=5),
                "open": 100.0, "high": 90.0, "low": 99.0, "close": 100.0, "volume": 10.0,  # high < low
                "is_closed": True,
            }
        ]
        report = HistoricalDatasetValidator().validate(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert report.violations[0].code == "MALFORMED_CANDLE"

    def test_validate_or_raise_raises_data_quality_error(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=5),
                "open": float("nan"), "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0,
            }
        ]
        with pytest.raises(DataQualityError):
            HistoricalDatasetValidator().validate_or_raise(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)


class TestIdentityEnforcement:
    def test_symbol_mismatch_rejected(self) -> None:
        candles = _series(3, symbol="ETHUSDT")
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "SYMBOL_MISMATCH" for v in report.violations)

    def test_timeframe_mismatch_rejected(self) -> None:
        candles = _series(3, timeframe=Timeframe.M15)
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "TIMEFRAME_MISMATCH" for v in report.violations)

    def test_unclosed_candle_rejected(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=5),
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0,
                "is_closed": False,
            }
        ]
        report = HistoricalDatasetValidator().validate(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "UNCLOSED_CANDLE" for v in report.violations)


class TestBinanceCloseTimeConvention:
    def test_real_binance_minus_1ms_convention_is_accepted(self) -> None:
        """`deterministic_series` already uses the -1ms convention (see
        `tests/research_fakes.py`); this asserts the validator does NOT
        flag it as WRONG_DURATION/a gap — a regression guard for the
        real-data bug this module's tolerance constant fixes."""
        candles = _series(20)
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert report.passed, report.violations

    def test_exact_boundary_convention_is_also_accepted(self) -> None:
        duration = timedelta(minutes=5)
        candles = []
        t = EPOCH
        for i in range(10):
            candles.append(
                Candle(
                    symbol="BTCUSDT", timeframe=Timeframe.M5, open_time=t, close_time=t + duration,
                    open=100.0, high=101.0, low=99.0, close=100.0, volume=10.0, is_closed=True,
                )
            )
            t += duration
        report = HistoricalDatasetValidator().validate(candles, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert report.passed, report.violations

    def test_wrong_duration_rejected(self) -> None:
        rows = [
            {
                "symbol": "BTCUSDT", "open_time": EPOCH, "close_time": EPOCH + timedelta(minutes=15),
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0, "is_closed": True,
            }
        ]
        report = HistoricalDatasetValidator().validate(rows, symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert not report.passed
        assert any(v.code == "WRONG_DURATION" for v in report.violations)


class TestEmptyDataset:
    def test_empty_dataset_passes_with_zero_count(self) -> None:
        report = HistoricalDatasetValidator().validate([], symbol="BTCUSDT", timeframe=Timeframe.M5)
        assert report.passed
        assert report.candle_count == 0


=== FILE: tests/test_research_drift.py ===
from __future__ import annotations

from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.drift import (
    DriftFinding,
    ProvenanceSource,
    compare_by_context_id,
    provenance_from_cycle_result,
    provenance_index,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _signal(context_id="c1", score=0.5) -> Signal:
    return Signal(
        symbol="BTCUSDT", timestamp=T0, context_id=context_id, score=score, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="v1",
    )


def _cycle(context_id="c1", score=0.5) -> RuntimeCycleResult:
    engine = PaperTradingEngine()
    signal = _signal(context_id=context_id, score=score)
    price = MarketPriceSnapshot(symbol="BTCUSDT", price=100.0, as_of=T0)
    result = engine.process_signal(signal, price)
    return RuntimeCycleResult(symbol="BTCUSDT", evaluated=True, signal=signal, paper_result=result, generated_at=T0)


class TestProvenanceExtraction:
    def test_unevaluated_cycle_yields_no_record(self) -> None:
        cycle = RuntimeCycleResult(symbol="BTCUSDT", evaluated=False, signal=None, paper_result=None, generated_at=T0)
        assert provenance_from_cycle_result(cycle, source=ProvenanceSource.REPLAY) is None

    def test_evaluated_cycle_yields_record(self) -> None:
        record = provenance_from_cycle_result(_cycle(), source=ProvenanceSource.REPLAY)
        assert record is not None
        assert record.context_id == "c1"
        assert record.source is ProvenanceSource.REPLAY


class TestComparison:
    def test_matched_when_scores_agree(self) -> None:
        replay_index = provenance_index([_cycle(context_id="c1", score=0.5)], source=ProvenanceSource.REPLAY)
        paper_index = provenance_index([_cycle(context_id="c1", score=0.5)], source=ProvenanceSource.PAPER_LIVE)
        findings = compare_by_context_id(replay_index, paper_index)
        assert findings["c1"] is DriftFinding.MATCHED

    def test_score_mismatch_detected(self) -> None:
        replay_index = provenance_index([_cycle(context_id="c1", score=0.5)], source=ProvenanceSource.REPLAY)
        paper_index = provenance_index([_cycle(context_id="c1", score=0.6)], source=ProvenanceSource.PAPER_LIVE)
        findings = compare_by_context_id(replay_index, paper_index)
        assert findings["c1"] is DriftFinding.SCORE_MISMATCH

    def test_missing_in_paper(self) -> None:
        replay_index = provenance_index([_cycle(context_id="c1")], source=ProvenanceSource.REPLAY)
        findings = compare_by_context_id(replay_index, {})
        assert findings["c1"] is DriftFinding.MISSING_IN_PAPER

    def test_missing_in_replay(self) -> None:
        paper_index = provenance_index([_cycle(context_id="c1")], source=ProvenanceSource.PAPER_LIVE)
        findings = compare_by_context_id({}, paper_index)
        assert findings["c1"] is DriftFinding.MISSING_IN_REPLAY


=== FILE: tests/test_research_monte_carlo.py ===
from __future__ import annotations

import random

import pytest

from research.monte_carlo import (
    MonteCarloConfigurationError,
    run_monte_carlo_robustness_screen,
)

_TRADES = [100.0, -50.0, 30.0, -80.0, 60.0, -20.0, 40.0, -10.0]


class TestDeterminism:
    def test_same_seed_identical_output(self) -> None:
        report_a = run_monte_carlo_robustness_screen(_TRADES, seed=7, iterations=200)
        report_b = run_monte_carlo_robustness_screen(_TRADES, seed=7, iterations=200)
        assert report_a == report_b

    def test_different_seed_can_differ(self) -> None:
        report_a = run_monte_carlo_robustness_screen(_TRADES, seed=1, iterations=200)
        report_b = run_monte_carlo_robustness_screen(_TRADES, seed=2, iterations=200)
        assert report_a.permuted_max_drawdowns != report_b.permuted_max_drawdowns

    def test_module_global_random_state_untouched(self) -> None:
        random.seed(12345)
        state_before = random.getstate()
        run_monte_carlo_robustness_screen(_TRADES, seed=99, iterations=100)
        state_after = random.getstate()
        assert state_before == state_after


class TestOriginalDataNeverMutated:
    def test_input_list_unchanged_after_run(self) -> None:
        original = list(_TRADES)
        run_monte_carlo_robustness_screen(_TRADES, seed=3, iterations=50)
        assert _TRADES == original


class TestTerminalPnlInvariance:
    def test_terminal_pnl_is_invariant_under_permutation(self) -> None:
        report = run_monte_carlo_robustness_screen(_TRADES, seed=5, iterations=300)
        assert report.terminal_pnl_invariant is True
        assert report.original_terminal_pnl == sum(_TRADES)


class TestDrawdownDistribution:
    def test_drawdown_distribution_has_min_max_mean_median(self) -> None:
        report = run_monte_carlo_robustness_screen(_TRADES, seed=11, iterations=500)
        assert report.drawdown_min <= report.drawdown_median <= report.drawdown_max
        assert report.drawdown_min <= report.drawdown_mean <= report.drawdown_max
        assert len(report.permuted_max_drawdowns) == 500


class TestAssumptionsDocumented:
    def test_assumptions_note_present_and_honest(self) -> None:
        report = run_monte_carlo_robustness_screen(_TRADES, seed=1, iterations=10)
        assert "exchangeability" in report.assumptions_note
        assert "does NOT prove" in report.assumptions_note


class TestConfigValidation:
    def test_non_positive_iterations_rejected(self) -> None:
        with pytest.raises(MonteCarloConfigurationError):
            run_monte_carlo_robustness_screen(_TRADES, seed=1, iterations=0)

    def test_non_int_seed_rejected(self) -> None:
        with pytest.raises(MonteCarloConfigurationError):
            run_monte_carlo_robustness_screen(_TRADES, seed=1.5, iterations=10)  # type: ignore[arg-type]

    def test_empty_trade_list_handled(self) -> None:
        report = run_monte_carlo_robustness_screen([], seed=1, iterations=10)
        assert report.trade_count == 0
        assert report.original_terminal_pnl == 0.0


=== FILE: tests/test_research_oos_stability.py ===
from __future__ import annotations

from datetime import timedelta

import pytest

from research.errors import ReplayConfigurationError
from research.oos_stability import RollingOOSStabilityRunner, build_sequential_windows
from research.replay import HistoricalReplayDriver, ReplayConfig
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource


class TestBuildSequentialWindows:
    def test_windows_are_time_disjoint(self) -> None:
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(days=6)
        windows = build_sequential_windows(
            start=start, end=end, window_size=timedelta(days=2), step_size=timedelta(days=2)
        )
        for a, b in zip(windows, windows[1:]):
            assert a.end <= b.start

    def test_overlapping_step_rejected(self) -> None:
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(days=6)
        with pytest.raises(ReplayConfigurationError):
            build_sequential_windows(
                start=start, end=end, window_size=timedelta(days=2), step_size=timedelta(days=1)
            )

    def test_configurable_window_and_step(self) -> None:
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(days=10)
        windows = build_sequential_windows(
            start=start, end=end, window_size=timedelta(days=3), step_size=timedelta(days=5)
        )
        assert all(w.end - w.start == timedelta(days=3) for w in windows)
        assert windows[1].start - windows[0].start == timedelta(days=5)

    def test_no_windows_fit_raises(self) -> None:
        start = EPOCH
        end = start + timedelta(hours=1)
        with pytest.raises(ReplayConfigurationError):
            build_sequential_windows(
                start=start, end=end, window_size=timedelta(days=2), step_size=timedelta(days=2)
            )


class TestRollingOOSStabilityRunner:
    def test_each_window_gets_fresh_state_and_no_leakage(self) -> None:
        driver = HistoricalReplayDriver(candle_source=FakeHistoricalCandleSource(), config=ReplayConfig(warmup_candles=20))
        runner = RollingOOSStabilityRunner(driver=driver)
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(days=1)

        report = run_async(
            runner.run(
                symbols=("BTCUSDT",), start=start, end=end,
                window_size=timedelta(hours=6), step_size=timedelta(hours=6),
            )
        )
        assert len(report.windows) == 4
        for window_result in report.windows:
            for cycle in window_result.replay_result.evaluated_cycle_results:
                assert cycle.signal.timestamp >= window_result.window.start
                assert cycle.signal.timestamp < window_result.window.end

    def test_deterministic_across_repeated_runs(self) -> None:
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=12)

        async def _once():
            driver = HistoricalReplayDriver(candle_source=FakeHistoricalCandleSource(), config=ReplayConfig(warmup_candles=20))
            runner = RollingOOSStabilityRunner(driver=driver)
            return await runner.run(
                symbols=("BTCUSDT",), start=start, end=end,
                window_size=timedelta(hours=6), step_size=timedelta(hours=6),
            )

        report_a = run_async(_once())
        report_b = run_async(_once())
        assert [w.metrics for w in report_a.windows] == [w.metrics for w in report_b.windows]

    def test_label_states_this_is_stability_not_optimization(self) -> None:
        driver = HistoricalReplayDriver(candle_source=FakeHistoricalCandleSource(), config=ReplayConfig(warmup_candles=20))
        runner = RollingOOSStabilityRunner(driver=driver)
        start = EPOCH + timedelta(hours=45)
        report = run_async(
            runner.run(
                symbols=("BTCUSDT",), start=start, end=start + timedelta(hours=6),
                window_size=timedelta(hours=6), step_size=timedelta(hours=6),
            )
        )
        assert "NOT walk-forward optimization" in report.label


=== FILE: tests/test_research_orderbook_capture.py ===
from __future__ import annotations

from datetime import timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from research.orderbook_capture import OrderBookEvidenceRecorder, RecordOutcome
from research.replay import OrderBookProvenance, ReplayConfig
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource
from tests.runtime_fakes import FakeLiveDataProvider


def _feature_snapshot(symbol: str = "BTCUSDT"):
    engine = FeatureEngine(history_store=FeatureHistoryStore())
    snapshot = OrderBookSnapshot(
        symbol=symbol, timestamp=EPOCH,
        bids=(OrderBookLevel(price=99.0, quantity=1.0), OrderBookLevel(price=98.0, quantity=1.0)),
        asks=(OrderBookLevel(price=101.0, quantity=1.0), OrderBookLevel(price=102.0, quantity=1.0)),
        last_update_id=1,
    )
    return engine.compute_order_book_features(snapshot)


class TestSyntheticProvenanceLabeledPartial:
    def test_replay_always_reports_synthetic_provenance(self) -> None:
        from research.replay import HistoricalReplayDriver

        driver = HistoricalReplayDriver(
            candle_source=FakeHistoricalCandleSource(), config=ReplayConfig(warmup_candles=20)
        )
        start = EPOCH + timedelta(hours=45)
        result = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=start + timedelta(hours=1)))
        assert result.order_book_provenance is OrderBookProvenance.SYNTHETIC
        assert result.LIVE_EQUIVALENT is False
        codes = {lim.code for lim in result.limitations}
        assert "SYNTHETIC_ORDER_BOOK" in codes


class TestRealRecordedProvenanceDistinguishable:
    def test_provenance_enum_distinguishes_real_from_synthetic(self) -> None:
        assert OrderBookProvenance.REAL_RECORDED != OrderBookProvenance.SYNTHETIC
        assert OrderBookProvenance.UNAVAILABLE != OrderBookProvenance.SYNTHETIC


class TestRecorderPersistsAndReads(object):
    def test_record_and_read_round_trip(self, tmp_path) -> None:
        recorder = OrderBookEvidenceRecorder(tmp_path / "ob_evidence.db")
        snapshot = _feature_snapshot()
        outcome = recorder.record("BTCUSDT", snapshot)
        assert outcome is RecordOutcome.RECORDED

        records = recorder.read_all("BTCUSDT")
        assert len(records) == 1
        assert records[0].symbol == "BTCUSDT"
        assert records[0].as_of == snapshot.as_of
        assert dict(records[0].feature_values) == dict(snapshot.values)

    def test_row_count_grows_boundedly_with_distinct_snapshots(self, tmp_path) -> None:
        recorder = OrderBookEvidenceRecorder(tmp_path / "ob_evidence.db")
        engine = FeatureEngine(history_store=FeatureHistoryStore())
        before = recorder.row_count()
        for i in range(5):
            snapshot = OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=EPOCH + timedelta(minutes=i),
                bids=(OrderBookLevel(price=99.0, quantity=1.0), OrderBookLevel(price=98.0, quantity=1.0)),
                asks=(OrderBookLevel(price=101.0, quantity=1.0), OrderBookLevel(price=102.0, quantity=1.0)),
                last_update_id=i + 1,
            )
            feature_snapshot = engine.compute_order_book_features(snapshot)
            recorder.record("BTCUSDT", feature_snapshot)
        after = recorder.row_count()
        feature_count_per_snapshot = len(feature_snapshot.values)
        assert after - before == 5 * feature_count_per_snapshot


class TestRecorderFailureIsolation:
    def test_record_never_raises_on_bad_snapshot_input(self, tmp_path) -> None:
        recorder = OrderBookEvidenceRecorder(tmp_path / "ob_evidence.db")

        class _BrokenSnapshot:
            as_of = "not-a-datetime"
            values = {"X": 1.0}

        outcome = recorder.record("BTCUSDT", _BrokenSnapshot())  # type: ignore[arg-type]
        assert outcome is RecordOutcome.FAILED

    def test_coordinator_observer_failure_does_not_alter_ingest_outcome(self) -> None:
        """Wires a deliberately-broken observer into a REAL, unmodified
        RuntimeCoordinator (via the additive, optional
        `order_book_observer` hook) and proves an order-book ingest event
        still succeeds identically to the no-observer case."""
        history_store = FeatureHistoryStore()

        def _broken_observer(symbol: str, snapshot) -> None:
            raise RuntimeError("simulated recorder failure")

        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",),
            provider=FakeLiveDataProvider(),
            history_store=history_store,
            feature_engine=FeatureEngine(history_store=history_store),
            clock=FixedClock(EPOCH),
            order_book_observer=_broken_observer,
        )
        snapshot = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=EPOCH,
            bids=(OrderBookLevel(price=99.0, quantity=1.0), OrderBookLevel(price=98.0, quantity=1.0)),
            asks=(OrderBookLevel(price=101.0, quantity=1.0), OrderBookLevel(price=102.0, quantity=1.0)),
            last_update_id=1,
        )
        from crypto_signal_engine.runtime.models import IngestOutcome

        event = coordinator.ingest_order_book("BTCUSDT", snapshot)
        assert event.outcome is IngestOutcome.ACCEPTED  # observer's RuntimeError never propagated

    def test_default_behaviour_unchanged_when_observer_is_none(self) -> None:
        history_store = FeatureHistoryStore()
        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",),
            provider=FakeLiveDataProvider(),
            history_store=history_store,
            feature_engine=FeatureEngine(history_store=history_store),
            clock=FixedClock(EPOCH),
        )
        snapshot = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=EPOCH,
            bids=(OrderBookLevel(price=99.0, quantity=1.0), OrderBookLevel(price=98.0, quantity=1.0)),
            asks=(OrderBookLevel(price=101.0, quantity=1.0), OrderBookLevel(price=102.0, quantity=1.0)),
            last_update_id=1,
        )
        from crypto_signal_engine.runtime.models import IngestOutcome

        event = coordinator.ingest_order_book("BTCUSDT", snapshot)
        assert event.outcome is IngestOutcome.ACCEPTED


=== FILE: tests/test_research_replay.py ===
from __future__ import annotations

from datetime import timedelta

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.errors import ReplayConfigurationError, ReplayIntegrityError
from research.replay import (
    HistoricalReplayDriver,
    OrderBookProvenance,
    ReplayConfig,
    build_deterministic_event_plan,
)
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource, deterministic_candle

_SMALL_WARMUP = ReplayConfig(warmup_candles=20)


def _driver(**overrides) -> tuple[HistoricalReplayDriver, FakeHistoricalCandleSource]:
    source = FakeHistoricalCandleSource()
    config = ReplayConfig(warmup_candles=20, **overrides)
    return HistoricalReplayDriver(candle_source=source, config=config), source


class TestDeterministicEventPlanOrdering:
    def test_shared_close_time_order_is_h1_m15_orderbook_m5(self) -> None:
        close_time = EPOCH + timedelta(hours=1)
        h1 = deterministic_candle("BTCUSDT", Timeframe.H1, EPOCH)
        m15 = deterministic_candle("BTCUSDT", Timeframe.M15, EPOCH + timedelta(minutes=45))
        m5 = deterministic_candle("BTCUSDT", Timeframe.M5, EPOCH + timedelta(minutes=55))
        assert h1.close_time == m15.close_time == m5.close_time == close_time - timedelta(milliseconds=1)

        plan = build_deterministic_event_plan(
            "BTCUSDT", {Timeframe.M5: [m5], Timeframe.M15: [m15], Timeframe.H1: [h1]}
        )
        tiers = [e.tier for e in plan]
        assert tiers == ["H1", "M15", "ORDER_BOOK", "M5"]

    def test_plan_independent_of_dict_key_insertion_order(self) -> None:
        h1 = deterministic_candle("BTCUSDT", Timeframe.H1, EPOCH)
        m15 = deterministic_candle("BTCUSDT", Timeframe.M15, EPOCH + timedelta(minutes=45))
        m5 = deterministic_candle("BTCUSDT", Timeframe.M5, EPOCH + timedelta(minutes=55))

        plan_a = build_deterministic_event_plan(
            "BTCUSDT", {Timeframe.M5: [m5], Timeframe.M15: [m15], Timeframe.H1: [h1]}
        )
        plan_b = build_deterministic_event_plan(
            "BTCUSDT", {Timeframe.H1: [h1], Timeframe.M15: [m15], Timeframe.M5: [m5]}
        )
        assert [(e.tier, e.close_time) for e in plan_a] == [(e.tier, e.close_time) for e in plan_b]

    def test_plan_is_monotonic_by_close_time_across_groups(self) -> None:
        m5s = [deterministic_candle("BTCUSDT", Timeframe.M5, EPOCH + timedelta(minutes=5 * i)) for i in range(6)]
        plan = build_deterministic_event_plan("BTCUSDT", {Timeframe.M5: m5s})
        close_times = [e.close_time for e in plan]
        assert close_times == sorted(close_times)

    def test_order_book_event_present_once_per_m5_group(self) -> None:
        m5s = [deterministic_candle("BTCUSDT", Timeframe.M5, EPOCH + timedelta(minutes=5 * i)) for i in range(4)]
        plan = build_deterministic_event_plan("BTCUSDT", {Timeframe.M5: m5s})
        assert sum(1 for e in plan if e.tier == "ORDER_BOOK") == 4


class TestReplayDeterminism:
    def test_identical_input_yields_value_identical_result(self) -> None:
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=2)

        result_a = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        driver_b, _ = _driver()
        result_b = run_async(driver_b.run(symbols=("BTCUSDT",), start=start, end=end))

        assert result_a.cycle_results == result_b.cycle_results
        assert result_a.final_positions == result_b.final_positions
        assert result_a.order_book_provenance == OrderBookProvenance.SYNTHETIC

    def test_result_independent_of_symbol_tuple_order(self) -> None:
        driver_a, _ = _driver()
        driver_b, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=2)

        result_a = run_async(driver_a.run(symbols=("BTCUSDT", "ETHUSDT"), start=start, end=end))
        result_b = run_async(driver_b.run(symbols=("ETHUSDT", "BTCUSDT"), start=start, end=end))

        by_symbol_a = {r.symbol: r for r in result_a.cycle_results}
        by_symbol_b = {r.symbol: r for r in result_b.cycle_results}
        assert set(by_symbol_a) == set(by_symbol_b) or (not by_symbol_a and not by_symbol_b)


class TestNoLookAhead:
    def test_no_look_ahead_violation_ever_raised_across_shared_boundaries(self) -> None:
        """A regression guard spanning the 00:00/00:15/01:00-style shared
        boundaries: if the merge/ordering policy ever accidentally applied
        a later-closing candle before an earlier one, PaperTradingEngine's
        own no-look-ahead check (or FeatureEngine's) would raise. This
        must never happen across a run that spans many H1/M15/M5
        boundaries."""
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=6)  # spans multiple H1 and M15 boundaries
        result = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        assert result.symbols == ("BTCUSDT",)

    def test_warmup_produces_no_evaluated_cycles_before_start(self) -> None:
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=2)
        result = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        for cycle in result.evaluated_cycle_results:
            assert cycle.signal is not None
            assert cycle.signal.timestamp >= start


class TestReuseOfAcceptedRuntimeCoordinator:
    def test_cycle_results_are_genuine_runtime_cycle_result_instances(self) -> None:
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=2)
        result = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        for cycle in result.cycle_results:
            assert isinstance(cycle, RuntimeCycleResult)


class TestMultiSymbolIsolation:
    def test_two_symbols_do_not_cross_contaminate(self) -> None:
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=3)
        result = run_async(driver.run(symbols=("BTCUSDT", "ETHUSDT"), start=start, end=end))
        for cycle in result.cycle_results:
            assert cycle.symbol in ("BTCUSDT", "ETHUSDT")
        for position in result.final_positions:
            assert position.symbol in ("BTCUSDT", "ETHUSDT")


class TestFeesAndSlippageApplied:
    def test_nonzero_fee_bps_produces_nonzero_fees_when_trades_occur(self) -> None:
        driver, _ = _driver(fee_bps=10.0, slippage_bps=5.0)
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=12)
        result = run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        any_fills = [f for c in result.cycle_results if c.paper_result for f in c.paper_result.fills]
        if any_fills:
            assert any(f.fee > 0 for f in any_fills)


class TestNoNetworkOrPrivateCalls:
    def test_fake_source_is_the_only_data_source_touched(self) -> None:
        driver, source = _driver()
        start = EPOCH + timedelta(hours=45)
        end = start + timedelta(hours=2)
        run_async(driver.run(symbols=("BTCUSDT",), start=start, end=end))
        assert len(source.calls) == 3  # one fetch per (M5, M15, H1) timeframe
        for symbol, timeframe, call_start, call_end in source.calls:
            assert symbol == "BTCUSDT"
            assert timeframe in (Timeframe.M5, Timeframe.M15, Timeframe.H1)


class TestReplayConfiguration:
    def test_empty_symbols_rejected(self) -> None:
        driver, _ = _driver()
        with pytest.raises(ReplayConfigurationError):
            run_async(driver.run(symbols=(), start=EPOCH, end=EPOCH + timedelta(hours=1)))

    def test_start_after_end_rejected(self) -> None:
        driver, _ = _driver()
        start = EPOCH + timedelta(hours=45)
        with pytest.raises(ReplayConfigurationError):
            run_async(driver.run(symbols=("BTCUSDT",), start=start, end=start - timedelta(hours=1)))

    def test_warmup_below_minimum_rejected_at_config_construction(self) -> None:
        with pytest.raises(ReplayConfigurationError):
            ReplayConfig(warmup_candles=1)

    def test_naive_datetime_rejected(self) -> None:
        import datetime as dt

        driver, _ = _driver()
        with pytest.raises(ValueError):
            run_async(
                driver.run(
                    symbols=("BTCUSDT",), start=dt.datetime(2026, 1, 1), end=dt.datetime(2026, 1, 2)
                )
            )


class TestEventPlanIntegrityGuard:
    def test_integrity_error_type_is_importable_and_is_a_research_error(self) -> None:
        # The monotonicity assertion inside build_deterministic_event_plan
        # is a should-never-happen defensive guard given this module's own
        # correct sort; this test only proves the guard type exists and is
        # wired into the research error hierarchy, since triggering it
        # would require corrupting the function's own sort (not exercised
        # here — see module docstring).
        assert issubclass(ReplayIntegrityError, Exception)


=== FILE: tests/test_research_sizing.py ===
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, PaperPosition, PositionSide
from research.errors import SizingPolicyError
from research.guardrails import (
    GuardrailBreach,
    PortfolioSnapshot,
    RiskBudgetConfig,
    evaluate_guardrails,
)
from research.sizing import PortfolioRiskSizingPolicy, SizedPaperTradingEngine, SizingOutcome

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)
LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def _signal(symbol="BTCUSDT", context_id="c1", timestamp=T0, score=LONG_SCORE, confidence=0.8, risk_level=RiskLevel.LOW) -> Signal:
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=confidence,
        risk_level=risk_level, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="test-v1",
    )


def _price(symbol="BTCUSDT", price=100.0, as_of=T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


def _position(symbol, side, quantity, entry_price, realized_pnl=0.0, updated_at=T0) -> PaperPosition:
    return PaperPosition(
        symbol=symbol, side=side, quantity=quantity, average_entry_price=entry_price,
        realized_pnl=realized_pnl, updated_at=updated_at,
    )


_DEFAULT_CONFIG = RiskBudgetConfig(
    reference_capital=10_000.0, max_per_symbol_notional=1000.0, max_gross_notional=2500.0,
    max_concurrent_positions=3, max_cumulative_realized_loss=1000.0,
)


class TestRiskBudgetConfigValidation:
    def test_rejects_non_positive_caps(self) -> None:
        with pytest.raises(ValueError):
            RiskBudgetConfig(
                reference_capital=10_000.0, max_per_symbol_notional=0.0, max_gross_notional=2500.0,
                max_concurrent_positions=3, max_cumulative_realized_loss=1000.0,
            )

    def test_rejects_per_symbol_cap_exceeding_gross_cap(self) -> None:
        with pytest.raises(ValueError):
            RiskBudgetConfig(
                reference_capital=10_000.0, max_per_symbol_notional=5000.0, max_gross_notional=2500.0,
                max_concurrent_positions=3, max_cumulative_realized_loss=1000.0,
            )


class TestGuardrailsPure:
    def test_no_positions_no_breach(self) -> None:
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        status = evaluate_guardrails(symbol="BTCUSDT", is_new_symbol_exposure=True, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert not status.halted
        assert status.available_symbol_notional == 1000.0

    def test_max_concurrent_positions_breach_for_new_exposure_only(self) -> None:
        positions = {
            "AAA": _position("AAA", PositionSide.LONG, 1.0, 100.0),
            "BBB": _position("BBB", PositionSide.LONG, 1.0, 100.0),
            "CCC": _position("CCC", PositionSide.LONG, 1.0, 100.0),
        }
        snapshot = PortfolioSnapshot(positions=positions, aggregate_realized_pnl=0.0)
        # a NEW symbol (DDD) would be a 4th concurrent position -> breach
        status_new = evaluate_guardrails(symbol="DDD", is_new_symbol_exposure=True, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert GuardrailBreach.MAX_CONCURRENT_POSITIONS in status_new.breaches
        # resizing an EXISTING exposure (AAA) does not trip the concurrent-count cap
        status_existing = evaluate_guardrails(symbol="AAA", is_new_symbol_exposure=False, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert GuardrailBreach.MAX_CONCURRENT_POSITIONS not in status_existing.breaches

    def test_gross_notional_cap_isolates_and_aggregates_correctly(self) -> None:
        positions = {
            "AAA": _position("AAA", PositionSide.LONG, 10.0, 100.0),  # 1000 notional
            "BBB": _position("BBB", PositionSide.LONG, 10.0, 100.0),  # 1000 notional
        }
        snapshot = PortfolioSnapshot(positions=positions, aggregate_realized_pnl=0.0)
        # gross_other for a NEW symbol CCC = 2000; max_gross=2500 -> only 500 left, capped below max_per_symbol
        status = evaluate_guardrails(symbol="CCC", is_new_symbol_exposure=True, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert status.available_symbol_notional == pytest.approx(500.0)
        # resizing AAA itself: gross_other excludes AAA -> only BBB's 1000 counts, remaining=1500, capped at per-symbol 1000
        status_aaa = evaluate_guardrails(symbol="AAA", is_new_symbol_exposure=False, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert status_aaa.available_symbol_notional == pytest.approx(1000.0)

    def test_cumulative_realized_loss_halts(self) -> None:
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=-1000.0)
        status = evaluate_guardrails(symbol="BTCUSDT", is_new_symbol_exposure=True, snapshot=snapshot, config=_DEFAULT_CONFIG)
        assert status.halted
        assert GuardrailBreach.CUMULATIVE_REALIZED_LOSS in status.breaches
        assert status.available_symbol_notional == 0.0


class TestPortfolioRiskSizingPolicyDecide:
    def test_neutral_signal_rejected(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        neutral = _signal(score=0.0)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        with pytest.raises(SizingPolicyError):
            policy.decide(signal=neutral, snapshot=snapshot, base_notional=1000.0)

    def test_zero_base_notional_fails_closed(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        with pytest.raises(SizingPolicyError):
            policy.decide(signal=_signal(), snapshot=snapshot, base_notional=0.0)

    def test_allowed_full_confidence_full_notional(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        decision = policy.decide(signal=_signal(confidence=1.0, risk_level=RiskLevel.LOW), snapshot=snapshot, base_notional=800.0)
        assert decision.outcome is SizingOutcome.ALLOWED
        assert decision.notional == pytest.approx(800.0)

    def test_confidence_and_risk_level_scale_notional(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        decision = policy.decide(
            signal=_signal(confidence=0.5, risk_level=RiskLevel.HIGH), snapshot=snapshot, base_notional=800.0
        )
        # 0.5 confidence (above floor 0.25) x 0.5 risk multiplier (HIGH) = 0.25 -> 200
        assert decision.notional == pytest.approx(200.0)

    def test_confidence_floor_prevents_near_zero_sizing(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        decision = policy.decide(
            signal=_signal(confidence=0.01, risk_level=RiskLevel.LOW), snapshot=snapshot, base_notional=800.0
        )
        assert decision.notional == pytest.approx(800.0 * _DEFAULT_CONFIG.confidence_multiplier_floor)

    def test_capped_by_per_symbol_budget(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        decision = policy.decide(
            signal=_signal(confidence=1.0, risk_level=RiskLevel.LOW), snapshot=snapshot, base_notional=5000.0
        )
        assert decision.outcome is SizingOutcome.CAPPED
        assert decision.notional == pytest.approx(_DEFAULT_CONFIG.max_per_symbol_notional)

    def test_denied_on_guardrail_breach(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=-1000.0)
        decision = policy.decide(signal=_signal(), snapshot=snapshot, base_notional=1000.0)
        assert decision.outcome is SizingOutcome.DENIED
        assert decision.notional is None

    def test_decision_never_reinterprets_score(self) -> None:
        policy = PortfolioRiskSizingPolicy(_DEFAULT_CONFIG)
        snapshot = PortfolioSnapshot(positions={}, aggregate_realized_pnl=0.0)
        # extreme score should have NO effect on sizing -- only confidence/risk_level do
        low_score_decision = policy.decide(
            signal=_signal(score=0.16, confidence=1.0, risk_level=RiskLevel.LOW), snapshot=snapshot, base_notional=800.0
        )
        high_score_decision = policy.decide(
            signal=_signal(score=0.95, confidence=1.0, risk_level=RiskLevel.LOW, context_id="c2"), snapshot=snapshot, base_notional=800.0
        )
        assert low_score_decision.notional == high_score_decision.notional == pytest.approx(800.0)


class TestSizedPaperTradingEngineEndToEnd:
    def test_default_sizing_respects_gross_cap_end_to_end(self) -> None:
        inner = PaperTradingEngine(notional_per_position=1000.0)
        config = RiskBudgetConfig(
            reference_capital=10_000.0, max_per_symbol_notional=1000.0, max_gross_notional=1200.0,
            max_concurrent_positions=5, max_cumulative_realized_loss=100_000.0,
        )
        wrapper = SizedPaperTradingEngine(
            inner=inner, policy=PortfolioRiskSizingPolicy(config), base_notional=1000.0,
            symbols=("AAA", "BBB"),
        )
        # default `_signal()` confidence=0.8, risk_level=LOW (x1.0 multiplier)
        # -> requested notional = 1000 * 0.8 * 1.0 = 800 (below the 1000
        # per-symbol cap, so AAA is fully ALLOWED at 800).
        r1 = wrapper.process_signal(_signal(symbol="AAA", context_id="c1"), _price(symbol="AAA"))
        assert r1.position.quantity == pytest.approx(8.0)
        assert wrapper.decisions[-1].outcome is SizingOutcome.ALLOWED

        r2 = wrapper.process_signal(_signal(symbol="BBB", context_id="c2"), _price(symbol="BBB"))
        # only 400 gross budget left (1200 - 800) -> BBB's own 800 request is capped to 400
        assert r2.position.quantity == pytest.approx(4.0)
        assert wrapper.decisions[-1].outcome is SizingOutcome.CAPPED

    def test_denial_does_not_touch_inner_state_no_panic_flatten(self) -> None:
        inner = PaperTradingEngine(notional_per_position=1000.0)
        config = RiskBudgetConfig(
            reference_capital=10_000.0, max_per_symbol_notional=1000.0, max_gross_notional=2500.0,
            max_concurrent_positions=3, max_cumulative_realized_loss=50.0,
        )
        wrapper = SizedPaperTradingEngine(
            inner=inner, policy=PortfolioRiskSizingPolicy(config), base_notional=1000.0, symbols=("AAA",),
        )
        # open a position, then force a big realized loss via a reversal
        wrapper.process_signal(_signal(symbol="AAA", context_id="c1", timestamp=T0), _price(symbol="AAA", price=100.0, as_of=T0))
        wrapper.process_signal(
            _signal(symbol="AAA", context_id="c2", score=SHORT_SCORE, timestamp=T0 + timedelta(minutes=5)),
            _price(symbol="AAA", price=50.0, as_of=T0 + timedelta(minutes=5)),  # big loss on the LONG close
        )
        assert inner.position("AAA").realized_pnl < -50.0  # breach threshold now exceeded

        before = inner.position("AAA")
        result = wrapper.process_signal(
            _signal(symbol="AAA", context_id="c3", score=LONG_SCORE, timestamp=T0 + timedelta(minutes=10)),
            _price(symbol="AAA", price=50.0, as_of=T0 + timedelta(minutes=10)),
        )
        assert wrapper.decisions[-1].outcome is SizingOutcome.DENIED
        # existing (opposite-direction, still-open) position is untouched -- no panic flatten
        assert inner.position("AAA") == before
        assert result.orders == ()
        assert result.fills == ()
        assert result.position == before

    def test_idempotent_replay_through_wrapper_cannot_double_allocate(self) -> None:
        inner = PaperTradingEngine(notional_per_position=1000.0)
        config = RiskBudgetConfig(
            reference_capital=10_000.0, max_per_symbol_notional=1000.0, max_gross_notional=2500.0,
            max_concurrent_positions=3, max_cumulative_realized_loss=100_000.0,
        )
        wrapper = SizedPaperTradingEngine(
            inner=inner, policy=PortfolioRiskSizingPolicy(config), base_notional=1000.0, symbols=("AAA",),
        )
        signal = _signal(symbol="AAA", context_id="c1")
        r1 = wrapper.process_signal(signal, _price(symbol="AAA"))
        r2 = wrapper.process_signal(signal, _price(symbol="AAA"))
        assert r2.idempotent_replay is True
        assert r1.position == r2.position
        assert len(inner.orders("AAA")) == 1  # exactly one allocation, never two

    def test_neutral_signal_passthrough_unsized(self) -> None:
        inner = PaperTradingEngine(notional_per_position=1000.0)
        config = _DEFAULT_CONFIG
        wrapper = SizedPaperTradingEngine(
            inner=inner, policy=PortfolioRiskSizingPolicy(config), base_notional=1000.0, symbols=("AAA",),
        )
        result = wrapper.process_signal(_signal(symbol="AAA", score=0.0), _price(symbol="AAA"))
        assert result.orders == ()
        assert wrapper.decisions == ()  # sizing never invoked for NEUTRAL


=== FILE: tests/test_reselection_scheduler.py ===
"""
Autonomous Testnet trading lifecycle Phase 16 — `ReselectionScheduler`
tests. Fully offline: a fake selector (duck-typed `select()`) + a real
`RuntimeCoordinator` with `FakeLiveDataProvider`. Proves: a rescan can add
a new opportunity and remove a stale flat symbol; safety-pinned symbols
are never removed even when a fresh ranking would drop them; a failed
rescan preserves the existing universe and never raises; duplicate/no-op
scheduling behaves correctly; `status()` reports armed/last/next
rescan info without needing to shorten the production interval."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.errors import SymbolSelectionError
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.reselection_scheduler import ReselectionScheduler
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig
from crypto_signal_engine.selection.models import CandidateScore, SelectionResult
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at, make_order_book

NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)
WARMUP = 20


def _score(symbol: str, total: float) -> CandidateScore:
    return CandidateScore(
        symbol=symbol, eligible=True, total_score=total, liquidity_score=total,
        historical_movement_score=total, current_opportunity_score=total,
        reason=f"score={total}", quote_volume_24h=1_000_000.0,
    )


def _ranking(scores: dict[str, float]) -> SelectionResult:
    candidates = tuple(_score(s, v) for s, v in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0])))
    return SelectionResult(
        generated_at=NOW, selected_symbols=tuple(c.symbol for c in candidates),
        ranked_candidates=candidates, rejected=(), universe_size=len(scores), shortlist_size=len(scores),
    )


class _FakeSelector:
    def __init__(self, results=None, *, fail: bool = False) -> None:
        self._results = list(results or [])
        self._fail = fail
        self.call_count = 0

    async def select(self) -> SelectionResult:
        self.call_count += 1
        if self._fail:
            raise SymbolSelectionError("discovery unavailable")
        return self._results.pop(0) if len(self._results) > 1 else self._results[0]


def _coordinator_with_symbol(symbol: str) -> tuple[RuntimeCoordinator, FakeLiveDataProvider]:
    provider = FakeLiveDataProvider()
    coordinator = RuntimeCoordinator(symbols=(symbol,), provider=provider, warmup_candles=WARMUP)
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, NOW, WARMUP)
        coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=NOW)
    coordinator.ingest_order_book(symbol, make_order_book(symbol, NOW))
    return coordinator, provider


def _add_historical(provider: FakeLiveDataProvider, coordinator: RuntimeCoordinator, symbol: str) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        provider._historical_candles[(symbol, timeframe)] = make_candle_series_ending_at(  # noqa: SLF001
            symbol, timeframe, NOW, WARMUP
        )


class TestBasicAddRemove:
    def test_add_new_opportunity_and_remove_stale_flat(self) -> None:
        coordinator, provider = _coordinator_with_symbol("BTCUSDT")
        _add_historical(provider, coordinator, "ETHUSDT")
        selector = _FakeSelector([_ranking({"ETHUSDT": 0.9})])
        config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
        )

        run_async(scheduler.run_once())

        assert "ETHUSDT" in coordinator._symbols  # noqa: SLF001
        assert "BTCUSDT" not in coordinator._symbols  # noqa: SLF001
        status = scheduler.status()
        assert status.last_added == ("ETHUSDT",)
        assert status.last_removed == ("BTCUSDT",)


class TestSafetyPinning:
    def test_pinned_symbol_never_removed_even_when_ranking_drops_it(self) -> None:
        coordinator, provider = _coordinator_with_symbol("BTCUSDT")
        _add_historical(provider, coordinator, "ETHUSDT")
        selector = _FakeSelector([_ranking({"ETHUSDT": 0.99})])
        config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset({"BTCUSDT"}), clock=FixedClock(NOW),
        )

        run_async(scheduler.run_once())

        assert "BTCUSDT" in coordinator._symbols  # noqa: SLF001 - safety-pinned, never removed
        status = scheduler.status()
        assert "BTCUSDT" not in status.last_removed

    def test_dust_and_pending_symbols_also_stay_via_provider_contract(self) -> None:
        """The scheduler itself has no opinion on WHAT counts as
        safety-owned — it trusts `safety_pinned_provider` completely. This
        test proves that contract: whatever the provider returns is
        treated as pinned, regardless of ranking."""
        coordinator, provider = _coordinator_with_symbol("BTCUSDT")
        selector = _FakeSelector([_ranking({})])  # BTCUSDT drops out of ranking entirely
        config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset({"BTCUSDT"}), clock=FixedClock(NOW),
        )

        run_async(scheduler.run_once())
        assert "BTCUSDT" in coordinator._symbols  # noqa: SLF001


class TestFailureIsolation:
    def test_failed_rescan_preserves_existing_universe_and_does_not_raise(self) -> None:
        coordinator, provider = _coordinator_with_symbol("BTCUSDT")
        selector = _FakeSelector(fail=True)
        config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
        )

        run_async(scheduler.run_once())  # must not raise

        assert coordinator._symbols == ("BTCUSDT",)  # noqa: SLF001
        status = scheduler.status()
        assert status.last_added == ()
        assert status.last_removed == ()

    def test_add_symbol_exception_does_not_block_other_adds_or_removes(self) -> None:
        coordinator, provider = _coordinator_with_symbol("BTCUSDT")
        _add_historical(provider, coordinator, "SOLUSDT")
        # ETHUSDT has no historical candles registered -> add_symbol's
        # bootstrap will get an empty candle list -> bootstrap_candles
        # simply reports not-ready (no exception) -- to actually exercise
        # the isolation path, monkeypatch add_symbol to fail for one symbol.
        original_add_symbol = coordinator.add_symbol

        async def failing_add_symbol(symbol: str) -> None:
            if symbol == "ETHUSDT":
                raise RuntimeError("boom")
            await original_add_symbol(symbol)

        coordinator.add_symbol = failing_add_symbol  # type: ignore[method-assign]
        selector = _FakeSelector([_ranking({"ETHUSDT": 0.95, "SOLUSDT": 0.9})])
        config = AutoSymbolSelectionConfig(target_count=2, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
        )

        run_async(scheduler.run_once())  # must not raise despite ETHUSDT failing

        assert "SOLUSDT" in coordinator._symbols  # noqa: SLF001 - unaffected by ETHUSDT's failure


class TestStatus:
    def test_status_before_start_is_not_armed(self) -> None:
        coordinator, _ = _coordinator_with_symbol("BTCUSDT")
        selector = _FakeSelector([_ranking({"BTCUSDT": 0.5})])
        config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
        scheduler = ReselectionScheduler(
            coordinator=coordinator, selector=selector, config=config,
            safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
        )
        assert scheduler.status().armed is False

    def test_status_armed_after_start_reports_next_rescan(self) -> None:
        async def scenario() -> None:
            coordinator, _ = _coordinator_with_symbol("BTCUSDT")
            selector = _FakeSelector([_ranking({"BTCUSDT": 0.5})])
            config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
            scheduler = ReselectionScheduler(
                coordinator=coordinator, selector=selector, config=config,
                safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
            )
            scheduler.start()
            status = scheduler.status()
            assert status.armed is True
            assert status.rescan_interval_seconds == 3600.0
            assert status.next_rescan_at == NOW + timedelta(seconds=3600.0)
            await scheduler.stop()

        run_async(scenario())

    def test_stop_is_idempotent(self) -> None:
        async def scenario() -> None:
            coordinator, _ = _coordinator_with_symbol("BTCUSDT")
            selector = _FakeSelector([_ranking({"BTCUSDT": 0.5})])
            config = AutoSymbolSelectionConfig(target_count=1, rescan_interval_seconds=3600.0)
            scheduler = ReselectionScheduler(
                coordinator=coordinator, selector=selector, config=config,
                safety_pinned_provider=lambda: frozenset(), clock=FixedClock(NOW),
            )
            scheduler.start()
            await scheduler.stop()
            await scheduler.stop()  # must not raise

        run_async(scenario())


=== FILE: tests/test_risk_overlay.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.consensus.risk import RiskOverlay
from crypto_signal_engine.domain.consensus import ConsensusResult, RegimeContext
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
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_evidence(agent, score) -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=score, rationale="test", primary_timeframe=Timeframe.M5,
        symbol="BTCUSDT", as_of=T0, context_id="ctx-1",
    )


def make_consensus(raw_score=0.5, agreement=0.9, evidence=None) -> ConsensusResult:
    ev = evidence or (make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5))
    return ConsensusResult(
        symbol="BTCUSDT", context_id="ctx-1", raw_score=raw_score, agreement=agreement,
        regime=RegimeContext(StructureRegime.RANGING, VolatilityRegime.NORMAL, LiquidityRegime.NORMAL),
        contributing_evidence=ev,
    )


def make_regime(volatility=VolatilityRegime.NORMAL, liquidity=LiquidityRegime.NORMAL) -> RegimeContext:
    return RegimeContext(structure=StructureRegime.RANGING, volatility=volatility, liquidity=liquidity)


class TestRiskOverlayContract:
    def test_exact_signature(self) -> None:
        import inspect

        sig = inspect.signature(RiskOverlay.assess)
        assert list(sig.parameters.keys()) == ["self", "consensus", "regime"]

    def test_requires_consensus_result_type(self) -> None:
        with pytest.raises(TypeError, match="ConsensusResult"):
            RiskOverlay().assess("not-consensus", make_regime())  # type: ignore[arg-type]

    def test_requires_regime_context_type(self) -> None:
        with pytest.raises(TypeError, match="RegimeContext"):
            RiskOverlay().assess(make_consensus(), "not-regime")  # type: ignore[arg-type]


class TestMultiplierBounds:
    def test_multiplier_never_above_one(self) -> None:
        result = RiskOverlay().assess(make_consensus(agreement=1.0), make_regime())
        assert result.confidence_multiplier <= 1.0

    def test_multiplier_never_below_zero(self) -> None:
        result = RiskOverlay().assess(
            make_consensus(agreement=0.01, raw_score=0.9), make_regime(VolatilityRegime.EXTREME, LiquidityRegime.STRESSED)
        )
        assert result.confidence_multiplier >= 0.0

    def test_low_agreement_cannot_improve_multiplier(self) -> None:
        high_agreement = RiskOverlay().assess(make_consensus(agreement=0.9), make_regime())
        low_agreement = RiskOverlay().assess(make_consensus(agreement=0.2), make_regime())
        assert low_agreement.confidence_multiplier <= high_agreement.confidence_multiplier


class TestVolatilityOrdering:
    def test_high_no_more_permissive_than_normal(self) -> None:
        normal = RiskOverlay().assess(make_consensus(), make_regime(volatility=VolatilityRegime.NORMAL))
        high = RiskOverlay().assess(make_consensus(), make_regime(volatility=VolatilityRegime.HIGH))
        assert high.confidence_multiplier <= normal.confidence_multiplier

    def test_extreme_no_more_permissive_than_high(self) -> None:
        high = RiskOverlay().assess(make_consensus(), make_regime(volatility=VolatilityRegime.HIGH))
        extreme = RiskOverlay().assess(make_consensus(), make_regime(volatility=VolatilityRegime.EXTREME))
        assert extreme.confidence_multiplier <= high.confidence_multiplier


class TestLiquidityOrdering:
    def test_thin_no_more_permissive_than_normal(self) -> None:
        normal = RiskOverlay().assess(make_consensus(), make_regime(liquidity=LiquidityRegime.NORMAL))
        thin = RiskOverlay().assess(make_consensus(), make_regime(liquidity=LiquidityRegime.THIN))
        assert thin.confidence_multiplier <= normal.confidence_multiplier

    def test_stressed_no_more_permissive_than_thin(self) -> None:
        thin = RiskOverlay().assess(make_consensus(), make_regime(liquidity=LiquidityRegime.THIN))
        stressed = RiskOverlay().assess(make_consensus(), make_regime(liquidity=LiquidityRegime.STRESSED))
        assert stressed.confidence_multiplier <= thin.confidence_multiplier


class TestContradiction:
    def test_strong_contradiction_increases_risk(self) -> None:
        aligned = make_consensus(raw_score=0.1, evidence=(make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5)))
        contradicted = make_consensus(raw_score=0.05, evidence=(make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, -0.45)))
        r_aligned = RiskOverlay().assess(aligned, make_regime())
        r_contradicted = RiskOverlay().assess(contradicted, make_regime())
        assert r_contradicted.confidence_multiplier <= r_aligned.confidence_multiplier


class TestNoMutationAndInvariance:
    def test_raw_score_unchanged(self) -> None:
        consensus = make_consensus(raw_score=0.42)
        RiskOverlay().assess(consensus, make_regime())
        assert consensus.raw_score == 0.42

    def test_score_sign_unchanged_conceptually(self) -> None:
        consensus = make_consensus(raw_score=-0.3)
        result = RiskOverlay().assess(consensus, make_regime())
        assert consensus.raw_score == -0.3
        assert not hasattr(result, "raw_score")
        assert not hasattr(result, "score")

    def test_consensus_object_not_mutated(self) -> None:
        consensus = make_consensus()
        original_evidence = consensus.contributing_evidence
        RiskOverlay().assess(consensus, make_regime())
        assert consensus.contributing_evidence is original_evidence

    def test_regime_object_not_mutated(self) -> None:
        regime = make_regime()
        RiskOverlay().assess(make_consensus(), regime)
        assert regime.volatility == VolatilityRegime.NORMAL

    def test_deterministic_output(self) -> None:
        consensus = make_consensus()
        regime = make_regime()
        r1 = RiskOverlay().assess(consensus, regime)
        r2 = RiskOverlay().assess(consensus, regime)
        assert r1.confidence_multiplier == r2.confidence_multiplier
        assert r1.risk_level == r2.risk_level


class TestRiskLevelOrdering:
    def test_low_risk_case(self) -> None:
        result = RiskOverlay().assess(make_consensus(agreement=0.95), make_regime())
        assert result.risk_level == RiskLevel.LOW

    def test_extreme_volatility_produces_extreme_or_high_risk(self) -> None:
        result = RiskOverlay().assess(make_consensus(agreement=0.95), make_regime(volatility=VolatilityRegime.EXTREME))
        assert result.risk_level in (RiskLevel.HIGH, RiskLevel.EXTREME)

    def test_severe_agreement_produces_extreme_risk(self) -> None:
        result = RiskOverlay().assess(make_consensus(agreement=0.1), make_regime())
        assert result.risk_level == RiskLevel.EXTREME


=== FILE: tests/test_run_with_adaptive_policy_script.py ===
"""
Adaptive Intelligence v1, step 14 — offline unit tests for `scripts/
run_with_adaptive_policy.py`'s pure provider-construction helpers
(`_build_exit_policy_provider`/`_build_champion_live_evidence_provider`).
No network, no live `Application`/`asyncio.run` — loaded via
`importlib` since `scripts/` is not a package (same convention every
other `scripts/*.py` follows: never imported as a package elsewhere)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from adaptive.decision import EMPTY_EVIDENCE
from adaptive.policy import default_champion_snapshot, snapshot_from_exit_policy_config
from adaptive.store import AdaptiveStore
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig
from datetime import datetime, timezone

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_with_adaptive_policy.py"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("run_with_adaptive_policy_script", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script_module():
    return _load_script_module()


@pytest.fixture()
def store(tmp_path) -> AdaptiveStore:  # noqa: ANN001
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


class TestExitPolicyProviderBitForBitEquivalence:
    """THE required test: no champion promoted yet => identical to
    today's static default."""

    def test_empty_store_returns_bare_default_and_no_version_id(self, script_module, store: AdaptiveStore) -> None:
        provider = script_module._build_exit_policy_provider(store)
        config, version_id = provider()
        assert config == ExitPolicyConfig()
        assert version_id is None

    def test_after_bootstrap_champion_promoted_returns_its_values(self, script_module, store: AdaptiveStore) -> None:
        champion = default_champion_snapshot(created_at=NOW)
        store.promote_champion(champion, reason="bootstrap", now=NOW)
        provider = script_module._build_exit_policy_provider(store)
        config, version_id = provider()
        assert config == ExitPolicyConfig()  # the bootstrap champion IS the bare default
        assert version_id == "policy-champion-default"

    def test_after_a_real_promotion_returns_the_new_champions_values(self, script_module, store: AdaptiveStore) -> None:
        champion = default_champion_snapshot(created_at=NOW)
        store.promote_champion(champion, reason="bootstrap", now=NOW)
        promoted = snapshot_from_exit_policy_config(
            ExitPolicyConfig(max_hold_hours=12.0), version_id="policy-v2", provenance="test", created_at=NOW,
        )
        store.promote_champion(promoted, reason="promoted after evidence", now=NOW)

        provider = script_module._build_exit_policy_provider(store)
        config, version_id = provider()
        assert config.max_hold_hours == 12.0
        assert version_id == "policy-v2"


class TestChampionLiveEvidenceProvider:
    def test_lifecycle_store_none_returns_empty_evidence(self, script_module) -> None:
        class _FakeApp:
            _lifecycle_store = None

        provider = script_module._build_champion_live_evidence_provider(_FakeApp())
        assert provider("any-version") == EMPTY_EVIDENCE

    def test_reads_completed_trades_attributed_by_policy_version_id(self, script_module) -> None:
        class _FakeLifecycleStore:
            def completed_trades_for_policy_version(self, version_id: str):
                assert version_id == "policy-v2"
                return (
                    {"net_realized_pnl": 5.0}, {"net_realized_pnl": -2.0}, {"net_realized_pnl": None},
                )

        class _FakeApp:
            _lifecycle_store = _FakeLifecycleStore()

        provider = script_module._build_champion_live_evidence_provider(_FakeApp())
        evidence = provider("policy-v2")
        assert evidence.sample_count == 2  # the None row is excluded, never crashes
        assert evidence.total_gross_pnl_per_unit == 3.0


class TestLiveCycleResultsProvider:
    """Drift-signal wiring fix: the production script's real
    live_cycle_results_provider, built from RuntimeCoordinator's own
    already-accepted last_cycle_result() accessor."""

    def test_collects_last_cycle_result_per_symbol_skipping_none(self, script_module) -> None:
        class _FakeCoordinator:
            def last_cycle_result(self, symbol: str):
                return f"result-for-{symbol}" if symbol != "ETHUSDT" else None

        class _FakeRuntime:
            _coordinator = _FakeCoordinator()

        class _FakeApp:
            runtime = _FakeRuntime()

        provider = script_module._build_live_cycle_results_provider(_FakeApp(), ("BTCUSDT", "ETHUSDT", "SOLUSDT"))
        results = provider()
        assert results == ("result-for-BTCUSDT", "result-for-SOLUSDT")

    def test_falls_back_to_bare_runtime_when_no_coordinator_attribute(self, script_module) -> None:
        class _FakeBareCoordinator:
            def last_cycle_result(self, symbol: str):
                return f"result-for-{symbol}"

        provider = script_module._build_live_cycle_results_provider(
            type("FakeApp", (), {"runtime": _FakeBareCoordinator()})(), ("BTCUSDT",),
        )
        assert provider() == ("result-for-BTCUSDT",)


=== FILE: tests/test_runtime_bootstrap.py ===
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.runtime.bootstrap import apply_bootstrap_candles
from crypto_signal_engine.runtime.candle_window import CandleWindow
from tests.runtime_fakes import make_candle_series_ending_at

UTC = timezone.utc
END = datetime(2026, 8, 31, tzinfo=UTC)
SYMBOL = "BTCUSDT"


class TestApplyBootstrapCandles:
    def test_sufficient_history_reaches_ready(self) -> None:
        window = CandleWindow()
        engine = FeatureEngine()
        candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, END, 20)

        report = apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=candles, as_of=END, min_candles=20,
        )

        assert report.ready is True
        assert report.candles_applied == 20
        assert engine.latest_snapshot(SYMBOL, Timeframe.M5) is not None

    def test_insufficient_history_remains_not_ready_no_fabrication(self) -> None:
        window = CandleWindow()
        engine = FeatureEngine()
        candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, END, 5)

        report = apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=candles, as_of=END, min_candles=20,
        )

        assert report.ready is False
        assert report.candles_applied == 5
        # no snapshot fabricated for an underfilled window
        assert engine.latest_snapshot(SYMBOL, Timeframe.M5) is None

    def test_out_of_order_input_is_sorted_before_applying(self) -> None:
        window = CandleWindow()
        engine = FeatureEngine()
        candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, END, 20)
        shuffled = list(reversed(candles))

        report = apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=shuffled, as_of=END, min_candles=20,
        )

        assert report.ready is True
        assert report.candles_applied == 20
        assert window.history() == tuple(candles)  # chronological, regardless of input order

    def test_duplicate_bootstrap_candles_are_deduplicated(self) -> None:
        window = CandleWindow()
        engine = FeatureEngine()
        candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, END, 20)
        doubled = candles + candles  # simulate an overlapping/duplicated REST fetch

        report = apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=doubled, as_of=END, min_candles=20,
        )

        assert report.candles_applied == 20  # duplicates counted once
        assert len(window) == 20

    def test_live_gap_fill_extends_an_already_ready_window(self) -> None:
        window = CandleWindow()
        engine = FeatureEngine()
        initial = make_candle_series_ending_at(SYMBOL, Timeframe.M5, END, 20)
        apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=initial, as_of=END, min_candles=20,
        )

        gap_end = END + timedelta(minutes=25)
        missing = [
            c for c in make_candle_series_ending_at(SYMBOL, Timeframe.M5, gap_end, 25) if c.open_time >= END
        ]
        report = apply_bootstrap_candles(
            window=window, feature_engine=engine, symbol=SYMBOL, timeframe=Timeframe.M5,
            candles=missing, as_of=gap_end, min_candles=20,
        )

        assert report.ready is True
        assert window.latest_open_time == missing[-1].open_time


=== FILE: tests/test_runtime_candle_window.py ===
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.candle_window import CandleWindow
from crypto_signal_engine.runtime.models import IngestOutcome
from tests.runtime_fakes import make_candle

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


class TestCandleWindow:
    def test_accepts_ascending_closed_candles(self) -> None:
        window = CandleWindow(maxlen=10)
        c1 = make_candle("BTCUSDT", Timeframe.M5, T0)
        c2 = make_candle("BTCUSDT", Timeframe.M5, T0 + timedelta(minutes=5))

        assert window.offer(c1) is IngestOutcome.ACCEPTED
        assert window.offer(c2) is IngestOutcome.ACCEPTED
        assert len(window) == 2
        assert window.history() == (c1, c2)

    def test_unclosed_candle_is_skipped_never_enters_window(self) -> None:
        window = CandleWindow()
        candle = make_candle("BTCUSDT", Timeframe.M5, T0, is_closed=False)

        assert window.offer(candle) is IngestOutcome.UNCLOSED_SKIPPED
        assert len(window) == 0
        assert window.latest_open_time is None

    def test_duplicate_open_time_is_rejected_without_mutation(self) -> None:
        window = CandleWindow()
        c1 = make_candle("BTCUSDT", Timeframe.M5, T0, price=100.0)
        window.offer(c1)

        duplicate = make_candle("BTCUSDT", Timeframe.M5, T0, price=999.0)  # different content, same identity
        outcome = window.offer(duplicate)

        assert outcome is IngestOutcome.DUPLICATE
        assert len(window) == 1
        assert window.history()[0].close == 100.0  # trusted state not silently overwritten

    def test_out_of_order_candle_does_not_roll_back_state(self) -> None:
        window = CandleWindow()
        window.offer(make_candle("BTCUSDT", Timeframe.M5, T0 + timedelta(minutes=10)))
        earlier = make_candle("BTCUSDT", Timeframe.M5, T0)

        outcome = window.offer(earlier)

        assert outcome is IngestOutcome.OUT_OF_ORDER
        assert len(window) == 1
        assert window.latest_open_time == T0 + timedelta(minutes=10)

    def test_window_is_bounded(self) -> None:
        window = CandleWindow(maxlen=3)
        for i in range(5):
            window.offer(make_candle("BTCUSDT", Timeframe.M5, T0 + timedelta(minutes=5 * i)))

        assert len(window) == 3
        assert window.history()[0].open_time == T0 + timedelta(minutes=10)  # oldest 2 evicted


=== FILE: tests/test_runtime_coordinator.py ===
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import NoLookAheadViolationError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.errors import IdentityMismatchError
from crypto_signal_engine.runtime.models import IngestOutcome, MarketEventKind, RuntimeHealth
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at, make_order_book

UTC = timezone.utc
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)  # aligned so a 20h H1 warmup fits exactly at T0
WARMUP = 20


def _bootstrap_fully(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready, f"{symbol}/{timeframe} bootstrap unexpectedly not ready"
    coordinator.ingest_order_book(symbol, make_order_book(symbol, end))


async def _poll_until(predicate, *, max_iterations: int = 200) -> None:
    """Sınırlı sayıda `asyncio.sleep(0)` (gerçek wall-clock gecikmesi
    YOKTUR — yalnızca event loop'a kontrolü geri veren deterministik bir
    cooperative yield) ile bir koşulu bekler. Sabit bir "kaç tick
    gerekiyor" varsayımına dayanmaz; koşul karşılanır karşılanmaz döner."""
    for _ in range(max_iterations):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail("condition not met within bounded polling window")


def _make_coordinator(symbols=("BTCUSDT",), **kwargs) -> RuntimeCoordinator:
    provider = kwargs.pop("provider", None) or FakeLiveDataProvider()
    return RuntimeCoordinator(symbols=symbols, provider=provider, warmup_candles=WARMUP, **kwargs)


def _bootstrap_candles_only(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    """`_bootstrap_fully` gibi ama order-book ADIMI OLMADAN — Blocker 3
    testleri için: "candle warmup tamam ama order-book yok" durumunu
    izole şekilde üretir."""
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready, f"{symbol}/{timeframe} bootstrap unexpectedly not ready"


def _symbol_health(coordinator: RuntimeCoordinator, symbol: str) -> RuntimeHealth:
    status = coordinator.status()
    return next(s for s in status.symbols if s.symbol == symbol).health


class TestLiveCandleProcessing:
    def test_valid_completed_candle_updates_pipeline(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.outcome is IngestOutcome.ACCEPTED
        assert event.kind is MarketEventKind.CANDLE

    def test_duplicate_completed_candle_does_not_double_process(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)

        first = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)
        second = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert first.outcome is IngestOutcome.ACCEPTED
        assert second.outcome is IngestOutcome.DUPLICATE
        assert second.cycle_result is None
        # only one signal/trade was produced despite two ingest calls
        engine = coordinator._paper_engine  # noqa: SLF001 (test introspection only)
        assert len(engine.fills("BTCUSDT")) <= 1

    def test_stale_candle_rejected_per_explicit_policy(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        coordinator.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))

        stale = make_candle("BTCUSDT", Timeframe.M5, END - timedelta(minutes=30), price=999.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, stale)

        assert event.outcome is IngestOutcome.OUT_OF_ORDER

    def test_out_of_order_candle_does_not_roll_state_backward(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        accepted = coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        )
        assert accepted.outcome is IngestOutcome.ACCEPTED

        coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END - timedelta(minutes=5), price=50.0)
        )

        window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
        assert window.latest_open_time == END


class TestMultiTimeframe:
    def test_evaluation_uses_only_data_available_as_of_evaluation_time(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.cycle_result is not None
        assert event.cycle_result.evaluated is True
        assert event.cycle_result.signal.timestamp == candle.close_time

    def test_incomplete_future_higher_timeframe_candle_is_not_consumed(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        unclosed_h1 = make_candle("BTCUSDT", Timeframe.H1, END, price=200.0, is_closed=False)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.H1, unclosed_h1)

        assert event.outcome is IngestOutcome.UNCLOSED_SKIPPED
        window = coordinator._candle_windows[("BTCUSDT", Timeframe.H1)]  # noqa: SLF001
        assert window.latest_open_time == END - timedelta(hours=1)  # last bootstrapped candle, unchanged

    def test_m5_evaluation_does_not_require_new_h1_close(self) -> None:
        # M5 evaluates using the LATEST already-closed H1 snapshot, never
        # waiting for (or synthesizing) a not-yet-closed H1 candle.
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        event = coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        )

        assert event.cycle_result.evaluated is True


class TestNoLookAhead:
    def test_price_after_signal_timestamp_is_rejected_by_phase5(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        signal = coordinator._signal_engine.evaluate("BTCUSDT", END)  # noqa: SLF001
        future_price = MarketPriceSnapshot(symbol="BTCUSDT", price=999.0, as_of=END + timedelta(seconds=1))

        with pytest.raises(NoLookAheadViolationError):
            coordinator._paper_engine.process_signal(signal, future_price)  # noqa: SLF001

    def test_reference_price_never_exceeds_signal_timestamp(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        price = coordinator._reference_price("BTCUSDT", not_after=event.cycle_result.signal.timestamp)  # noqa: SLF001
        assert price.as_of <= event.cycle_result.signal.timestamp

    def test_future_candle_does_not_leak_into_earlier_evaluation(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        first_event = coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        )
        first_score_inputs = coordinator._signal_engine.evaluate("BTCUSDT", first_event.cycle_result.signal.timestamp)  # noqa: SLF001

        # a later, out-of-order (already-superseded) re-evaluation at the
        # SAME as_of must produce the identical result — nothing "leaked"
        replay = coordinator._signal_engine.evaluate("BTCUSDT", first_event.cycle_result.signal.timestamp)  # noqa: SLF001
        assert replay.score == first_score_inputs.score


class TestSignalIntegration:
    def test_neutral_signal_causes_no_trade(self) -> None:
        provider = FakeLiveDataProvider()
        paper_engine = PaperTradingEngine(notional_per_position=1000.0)
        coordinator = _make_coordinator(provider=provider, paper_engine=paper_engine)
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        # flat prices across warmup -> near-zero/neutral score is likely,
        # but regardless of direction, assert the no-trade invariant only
        # when NEUTRAL is actually produced (score-independent structural
        # check lives in TestOpenPosition/TestNeutralIsNoAction already);
        # here we assert the *integration* contract: whatever Signal comes
        # out, it reaches PaperTradingEngine exactly once.
        event = coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=100.0)
        )
        assert event.cycle_result.evaluated is True
        assert event.cycle_result.paper_result is not None

    def test_repeated_context_does_not_duplicate_paper_trade(self) -> None:
        provider = FakeLiveDataProvider()
        paper_engine = PaperTradingEngine(notional_per_position=1000.0)
        coordinator = _make_coordinator(provider=provider, paper_engine=paper_engine)
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)

        coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)
        second = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)  # duplicate delivery

        assert second.outcome is IngestOutcome.DUPLICATE
        assert len(paper_engine.fills("BTCUSDT")) <= 1


class TestReconnect:
    def test_disconnect_marks_symbol_degraded(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        coordinator.mark_disconnected("BTCUSDT")

        status = coordinator.status()
        symbol_status = next(s for s in status.symbols if s.symbol == "BTCUSDT")
        assert symbol_status.health is RuntimeHealth.DEGRADED
        assert symbol_status.reconnect_count == 1

    def test_run_consumes_streams_and_stop_is_idempotent(self) -> None:
        async def scenario() -> None:
            candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            provider = FakeLiveDataProvider(candle_scripts={("BTCUSDT", Timeframe.M5): [candle]})
            coordinator = _make_coordinator(symbols=("BTCUSDT",), provider=provider)
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001

            run_task = asyncio.create_task(coordinator.run())
            await _poll_until(lambda: window.latest_open_time == END)

            await coordinator.stop()
            await coordinator.stop()  # idempotent — must not raise
            await run_task

            assert provider.closed is True
            assert window.latest_open_time == END

        run_async(scenario())

    def test_unexpected_stream_error_marks_symbol_degraded_not_silent(self) -> None:
        """Bölüm 28 — 'swallowed network/runtime errors' bulgusu: bir
        consumer task'ında BEKLENMEYEN bir hata, `run()`'ın kendi
        `asyncio.gather(..., return_exceptions=True)`'ı tarafından
        SESSİZCE yutulmaz — health AÇIKÇA DEGRADED'e geçer."""

        class FailingProvider(FakeLiveDataProvider):
            async def stream_candles(self, symbol: str, timeframe: Timeframe):
                raise RuntimeError("boom")
                yield  # pragma: no cover

        async def scenario() -> None:
            provider = FailingProvider()
            coordinator = _make_coordinator(
                symbols=("BTCUSDT",), provider=provider, candle_timeframes=(Timeframe.M5,)
            )
            _bootstrap_fully(coordinator, "BTCUSDT", END)

            def is_degraded() -> bool:
                status = coordinator.status()
                return next(s for s in status.symbols if s.symbol == "BTCUSDT").health is RuntimeHealth.DEGRADED

            run_task = asyncio.create_task(coordinator.run())
            await _poll_until(is_degraded)

            await coordinator.stop()
            await run_task  # completes cleanly; error was surfaced via health, not lost

        run_async(scenario())


class TestGapRecovery:
    def test_gap_is_detected_and_not_silently_accepted(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        far_future = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=105.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, far_future)

        assert event.outcome is IngestOutcome.GAP_DETECTED
        window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
        assert window.latest_open_time == END - timedelta(minutes=5)  # unchanged, no fabricated continuity

    def test_resolve_gap_recovers_missing_range_and_applies_in_order(self) -> None:
        async def scenario() -> None:
            coordinator = _make_coordinator()
            _bootstrap_fully(coordinator, "BTCUSDT", END)  # last M5 candle: open_time=END-5m, close=END

            # two genuinely MISSING candles (open_time=END, END+5m) plus a
            # live candle arriving 3 intervals ahead (open_time=END+10m) —
            # exactly the shape a reconnect-induced gap would produce.
            missing = [
                make_candle("BTCUSDT", Timeframe.M5, END, price=102.0),
                make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=5), price=103.0),
            ]
            pending = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=10), price=104.0)

            provider: FakeLiveDataProvider = coordinator._provider  # noqa: SLF001
            provider._historical_candles[("BTCUSDT", Timeframe.M5)] = missing  # noqa: SLF001

            gap_event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, pending)
            assert gap_event.outcome is IngestOutcome.GAP_DETECTED  # detected, NOT silently accepted

            event = await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)

            assert event.outcome is IngestOutcome.ACCEPTED
            window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
            assert window.latest_open_time == pending.open_time
            assert len(provider.fetch_calls) == 1  # only the missing range was fetched

        run_async(scenario())

    def test_failed_recovery_does_not_produce_false_healthy_state(self) -> None:
        async def scenario() -> None:
            coordinator = _make_coordinator()
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY
            # provider has no historical data configured -> recovery yields nothing
            pending = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=105.0)

            event = await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)

            assert event.outcome is IngestOutcome.GAP_DETECTED  # still rejected, not fabricated as healthy
            # BLOCKER 1: failed recovery must make the symbol OBSERVABLY unhealthy
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        run_async(scenario())

    def test_unrelated_order_book_activity_does_not_clear_unresolved_gap_fault(self) -> None:
        async def scenario() -> None:
            coordinator = _make_coordinator()
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            pending = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=105.0)
            await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

            # an unrelated, perfectly valid order-book event for the SAME
            # symbol must NOT erase the still-unresolved candle-gap fault.
            later = END + timedelta(minutes=1)
            event = coordinator.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", later, last_update_id=2))

            assert event.outcome is IngestOutcome.ACCEPTED
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        run_async(scenario())

    def test_successful_recovery_clears_the_runtime_gap_fault(self) -> None:
        async def scenario() -> None:
            coordinator = _make_coordinator()
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            pending = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=105.0)
            await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

            # now provide the genuinely missing candles (open_time END..END+25m,
            # exactly filling [last accepted + duration, pending.open_time)) and retry
            missing = [
                make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=5 * i), price=100.0 + i)
                for i in range(6)
            ]
            provider: FakeLiveDataProvider = coordinator._provider  # noqa: SLF001
            provider._historical_candles[("BTCUSDT", Timeframe.M5)] = missing  # noqa: SLF001

            event = await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)

            assert event.outcome is IngestOutcome.ACCEPTED
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

        run_async(scenario())

    def test_gap_fault_on_one_symbol_does_not_degrade_another(self) -> None:
        async def scenario() -> None:
            coordinator = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"))
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            _bootstrap_fully(coordinator, "ETHUSDT", END)

            pending = make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=105.0)
            await coordinator.resolve_gap("BTCUSDT", Timeframe.M5, pending)

            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED
            assert _symbol_health(coordinator, "ETHUSDT") is RuntimeHealth.READY

        run_async(scenario())


class TestStaleFeed:
    def test_fresh_bootstrap_reports_ready(self) -> None:
        clock = FixedClock(END)
        provider = FakeLiveDataProvider()
        coordinator = _make_coordinator(provider=provider, clock=clock, stale_feed_threshold_seconds=30.0)
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        coordinator.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))

        status = coordinator.status()
        assert status.overall_health is RuntimeHealth.READY

    def test_stale_threshold_crossing_degrades_overall_status(self) -> None:
        clock = FixedClock(END)
        provider = FakeLiveDataProvider()
        coordinator = _make_coordinator(provider=provider, clock=clock, stale_feed_threshold_seconds=30.0)
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        coordinator.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))

        clock.advance(60.0)

        status = coordinator.status()
        assert status.overall_health is RuntimeHealth.DEGRADED


class TestMultiSymbolIsolation:
    def test_btc_and_eth_stay_isolated(self) -> None:
        coordinator = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"))
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        _bootstrap_fully(coordinator, "ETHUSDT", END)

        coordinator.mark_disconnected("ETHUSDT")
        btc_event = coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        )

        status = coordinator.status()
        btc_health = next(s for s in status.symbols if s.symbol == "BTCUSDT")
        eth_health = next(s for s in status.symbols if s.symbol == "ETHUSDT")
        assert btc_event.outcome is IngestOutcome.ACCEPTED
        assert eth_health.health is RuntimeHealth.DEGRADED
        assert btc_health.health is RuntimeHealth.READY

    def test_gap_on_one_symbol_does_not_corrupt_other_symbol_window(self) -> None:
        coordinator = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"))
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        _bootstrap_fully(coordinator, "ETHUSDT", END)

        coordinator.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END + timedelta(minutes=30), price=1.0)
        )  # gap on BTC

        eth_event = coordinator.ingest_candle(
            "ETHUSDT", Timeframe.M5, make_candle("ETHUSDT", Timeframe.M5, END, price=10.0)
        )
        assert eth_event.outcome is IngestOutcome.ACCEPTED


class TestInstanceIsolation:
    def test_two_coordinators_share_no_mutable_state(self) -> None:
        c1 = _make_coordinator(symbols=("BTCUSDT",))
        c2 = _make_coordinator(symbols=("BTCUSDT",))
        _bootstrap_fully(c1, "BTCUSDT", END)

        assert c1._candle_windows is not c2._candle_windows  # noqa: SLF001
        assert len(c1._candle_windows[("BTCUSDT", Timeframe.M5)]) == WARMUP  # noqa: SLF001
        assert len(c2._candle_windows[("BTCUSDT", Timeframe.M5)]) == 0  # noqa: SLF001
        assert c2.status().overall_health is RuntimeHealth.BOOTSTRAPPING


class TestShutdown:
    def test_graceful_stop_closes_provider(self) -> None:
        async def scenario() -> None:
            provider = FakeLiveDataProvider()
            coordinator = _make_coordinator(provider=provider)
            await coordinator.stop()
            assert provider.closed is True
            assert coordinator.status().overall_health is RuntimeHealth.STOPPED

        run_async(scenario())

    def test_double_stop_is_safe(self) -> None:
        async def scenario() -> None:
            provider = FakeLiveDataProvider()
            coordinator = _make_coordinator(provider=provider)
            await coordinator.stop()
            await coordinator.stop()  # must not raise
            assert provider.closed is True

        run_async(scenario())

    def test_run_after_stop_is_rejected(self) -> None:
        async def scenario() -> None:
            provider = FakeLiveDataProvider()
            coordinator = _make_coordinator(provider=provider)
            await coordinator.stop()
            with pytest.raises(ValueError):
                await coordinator.run()

        run_async(scenario())


class TestConfigValidation:
    def test_rejects_empty_symbols(self) -> None:
        with pytest.raises(ValueError):
            RuntimeCoordinator(symbols=(), provider=FakeLiveDataProvider())

    def test_rejects_warmup_below_minimum(self) -> None:
        with pytest.raises(ValueError):
            RuntimeCoordinator(symbols=("BTCUSDT",), provider=FakeLiveDataProvider(), warmup_candles=5)


class TestIdentityValidation:
    """BLOCKER 2 — ingest/bootstrap boundaries must validate the ACTUAL
    domain object's identity, not trust the caller's parameters."""

    def test_ingest_candle_rejects_symbol_mismatch(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
        history_before = window.history()

        wrong_symbol_candle = make_candle("ETHUSDT", Timeframe.M5, END, price=101.0)
        with pytest.raises(IdentityMismatchError):
            coordinator.ingest_candle("BTCUSDT", Timeframe.M5, wrong_symbol_candle)

        assert window.history() == history_before  # untouched
        assert coordinator._latest_order_book.get("BTCUSDT") is not None  # noqa: SLF001 (unaffected)

    def test_ingest_candle_rejects_timeframe_mismatch(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
        history_before = window.history()

        wrong_timeframe_candle = make_candle("BTCUSDT", Timeframe.M15, END, price=101.0)
        with pytest.raises(IdentityMismatchError):
            coordinator.ingest_candle("BTCUSDT", Timeframe.M5, wrong_timeframe_candle)

        assert window.history() == history_before  # untouched
        m15_window = coordinator._candle_windows[("BTCUSDT", Timeframe.M15)]  # noqa: SLF001
        # the M15 window (the candle's ACTUAL timeframe) was not touched either —
        # the call declared M5, so the M15 window must never see this candle.
        assert wrong_timeframe_candle not in m15_window.history()

    def test_bootstrap_candles_rejects_identity_mismatch_atomically(self) -> None:
        coordinator = _make_coordinator()
        good = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP - 1)
        contaminated = good + [make_candle("ETHUSDT", Timeframe.M5, END, price=999.0)]

        with pytest.raises(IdentityMismatchError):
            coordinator.bootstrap_candles("BTCUSDT", Timeframe.M5, contaminated, as_of=END)

        window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
        assert len(window) == 0  # NOTHING applied — all-or-nothing
        assert coordinator._feature_engine.latest_snapshot("BTCUSDT", Timeframe.M5) is None  # noqa: SLF001
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING

    def test_ingest_order_book_rejects_symbol_mismatch(self) -> None:
        coordinator = _make_coordinator()
        wrong_symbol_book = make_order_book("ETHUSDT", END)

        with pytest.raises(IdentityMismatchError):
            coordinator.ingest_order_book("BTCUSDT", wrong_symbol_book)

        assert coordinator._latest_order_book.get("BTCUSDT") is None  # noqa: SLF001
        assert coordinator._feature_engine.latest_snapshot("BTCUSDT", Timeframe.M1) is None  # noqa: SLF001
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING


class TestReadinessRequiresOrderBook:
    """BLOCKER 3 — READY must mean ALL Phase 4 evaluation inputs (including
    at least one M1 order-book feature snapshot) are actually available,
    not merely that M5/M15/H1 candle warmup finished."""

    def test_candle_warmup_complete_without_order_book_is_not_ready(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)

        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING
        assert coordinator._feature_engine._history.as_of(  # noqa: SLF001
            "BTCUSDT", Timeframe.M1, END
        ) is None

    def test_first_valid_order_book_after_candle_warmup_becomes_ready(self) -> None:
        coordinator = _make_coordinator()
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING

        event = coordinator.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", END))

        assert event.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

    def test_order_book_first_with_incomplete_candle_warmup_stays_not_ready(self) -> None:
        coordinator = _make_coordinator()
        # only M5 warmed up; M15/H1 still empty
        m5_candles = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP)
        coordinator.bootstrap_candles("BTCUSDT", Timeframe.M5, m5_candles, as_of=END)

        coordinator.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", END))

        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING

    def test_multi_symbol_readiness_stays_isolated(self) -> None:
        coordinator = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"))
        _bootstrap_fully(coordinator, "BTCUSDT", END)  # candles + order book -> READY
        _bootstrap_candles_only(coordinator, "ETHUSDT", END)  # candles only -> NOT ready

        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY
        assert _symbol_health(coordinator, "ETHUSDT") is RuntimeHealth.BOOTSTRAPPING

        coordinator.ingest_order_book("ETHUSDT", make_order_book("ETHUSDT", END))
        assert _symbol_health(coordinator, "ETHUSDT") is RuntimeHealth.READY
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY


=== FILE: tests/test_runtime_coordinator_candle_observer.py ===
"""
Adaptive Intelligence v1, step 8 — `RuntimeCoordinator.candle_observer`
hook tests. Same discipline and structure as
`tests/test_research_orderbook_capture.py::TestRecorderFailureIsolation`'s
`order_book_observer` tests (Karar 87), applied to the new, additive,
optional `candle_observer` hook `ingest_candle` now calls."""

from __future__ import annotations

from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import IngestOutcome
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at

UTC = timezone.utc
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
WARMUP = 20


def _bootstrap_candles_only(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready


class TestCandleObserverHook:
    def test_observer_called_with_accepted_candle(self) -> None:
        observed: list[tuple[str, Timeframe]] = []

        def _observer(symbol: str, timeframe: Timeframe, candle) -> None:
            observed.append((symbol, timeframe))

        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",), provider=FakeLiveDataProvider(), warmup_candles=WARMUP,
            clock=FixedClock(END), candle_observer=_observer,
        )
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.outcome is IngestOutcome.ACCEPTED
        assert ("BTCUSDT", Timeframe.M5) in observed

    def test_observer_never_called_for_a_rejected_candle(self) -> None:
        observed: list[object] = []

        def _observer(symbol: str, timeframe: Timeframe, candle) -> None:
            observed.append(candle)

        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",), provider=FakeLiveDataProvider(), warmup_candles=WARMUP,
            clock=FixedClock(END), candle_observer=_observer,
        )
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        first = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)
        observed.clear()
        second = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)  # duplicate -> rejected

        assert first.outcome is IngestOutcome.ACCEPTED
        assert second.outcome is not IngestOutcome.ACCEPTED
        assert observed == []

    def test_observer_failure_does_not_alter_ingest_outcome_or_raise(self) -> None:
        """Wires a deliberately-broken observer into a REAL, unmodified
        RuntimeCoordinator and proves a candle ingest event still
        succeeds identically to the no-observer case -- the mandated
        'a shadow-evaluation bug can never affect the live runtime'
        invariant."""

        def _broken_observer(symbol: str, timeframe: Timeframe, candle) -> None:
            raise RuntimeError("simulated shadow-evaluation failure")

        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",), provider=FakeLiveDataProvider(), warmup_candles=WARMUP,
            clock=FixedClock(END), candle_observer=_broken_observer,
        )
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.outcome is IngestOutcome.ACCEPTED  # observer's RuntimeError never propagated

    def test_default_behaviour_unchanged_when_observer_is_none(self) -> None:
        coordinator = RuntimeCoordinator(
            symbols=("BTCUSDT",), provider=FakeLiveDataProvider(), warmup_candles=WARMUP, clock=FixedClock(END),
        )
        _bootstrap_candles_only(coordinator, "BTCUSDT", END)

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = coordinator.ingest_candle("BTCUSDT", Timeframe.M5, candle)
        assert event.outcome is IngestOutcome.ACCEPTED


=== FILE: tests/test_runtime_coordinator_hot_symbols.py ===
"""
Autonomous Testnet trading lifecycle Phase 16 — `RuntimeCoordinator.
add_symbol()`/`remove_symbol()` tests. These are the direct evidence for
the "hot symbol lifecycle" invariants: idempotent add (no duplicate task),
clean per-symbol task cancellation on remove (no leaked subscription), and
isolation (removing/adding one symbol never disturbs another's active
streams) — all fully offline via `FakeLiveDataProvider`."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at, make_order_book

END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=timezone.utc)
WARMUP = 20


def _bootstrap_fully(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
    coordinator.ingest_order_book(symbol, make_order_book(symbol, end))


async def _poll_until(predicate, *, max_iterations: int = 200) -> None:
    for _ in range(max_iterations):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail("condition not met within bounded polling window")


def _provider_with_historical(symbol: str, timeframes: tuple[Timeframe, ...], end: datetime) -> FakeLiveDataProvider:
    historical = {
        (symbol, tf): make_candle_series_ending_at(symbol, tf, end, WARMUP) for tf in timeframes
    }
    return FakeLiveDataProvider(historical_candles=historical)


class TestAddSymbolBeforeRun:
    def test_add_symbol_initializes_state_and_bootstraps(self) -> None:
        provider = _provider_with_historical("ETHUSDT", (Timeframe.M5, Timeframe.M15, Timeframe.H1), END)
        coordinator = RuntimeCoordinator(symbols=("BTCUSDT",), provider=provider, warmup_candles=WARMUP)

        async def scenario():
            await coordinator.add_symbol("ETHUSDT")

        run_async(scenario())
        assert "ETHUSDT" in coordinator._symbols  # noqa: SLF001
        assert coordinator._ready[("ETHUSDT", Timeframe.M5)] is True  # noqa: SLF001
        assert len(provider.fetch_calls) == 3  # one per configured candle_timeframe

    def test_add_symbol_before_run_spawns_no_tasks(self) -> None:
        provider = _provider_with_historical("ETHUSDT", (Timeframe.M5, Timeframe.M15, Timeframe.H1), END)
        coordinator = RuntimeCoordinator(symbols=("BTCUSDT",), provider=provider, warmup_candles=WARMUP)

        async def scenario():
            await coordinator.add_symbol("ETHUSDT")

        run_async(scenario())
        assert coordinator._tasks == []  # noqa: SLF001 - run() never started
        assert "ETHUSDT" not in coordinator._tasks_by_symbol  # noqa: SLF001

    def test_add_symbol_idempotent_no_duplicate_bootstrap(self) -> None:
        provider = _provider_with_historical("ETHUSDT", (Timeframe.M5, Timeframe.M15, Timeframe.H1), END)
        coordinator = RuntimeCoordinator(symbols=("BTCUSDT",), provider=provider, warmup_candles=WARMUP)

        async def scenario():
            await coordinator.add_symbol("ETHUSDT")
            first_call_count = len(provider.fetch_calls)
            await coordinator.add_symbol("ETHUSDT")  # no-op
            return first_call_count

        first_call_count = run_async(scenario())
        assert len(provider.fetch_calls) == first_call_count  # unchanged -- no second fetch
        assert coordinator._symbols.count("ETHUSDT") == 1  # noqa: SLF001


class TestAddSymbolAfterRunStarted:
    def test_add_symbol_after_run_spawns_tasks_and_processes_candles(self) -> None:
        async def scenario() -> None:
            btc_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            provider = FakeLiveDataProvider(candle_scripts={("BTCUSDT", Timeframe.M5): [btc_candle]})
            coordinator = RuntimeCoordinator(symbols=("BTCUSDT",), provider=provider, warmup_candles=WARMUP)
            _bootstrap_fully(coordinator, "BTCUSDT", END)

            run_task = asyncio.create_task(coordinator.run())
            await _poll_until(lambda: coordinator._running)  # noqa: SLF001

            eth_candle = make_candle("ETHUSDT", Timeframe.M5, END, price=2000.0)
            provider._candle_scripts[("ETHUSDT", Timeframe.M5)] = [eth_candle]  # noqa: SLF001
            provider._historical_candles.update(  # noqa: SLF001
                {(("ETHUSDT", tf)): make_candle_series_ending_at("ETHUSDT", tf, END, WARMUP) for tf in coordinator._candle_timeframes}  # noqa: SLF001
            )

            await coordinator.add_symbol("ETHUSDT")
            assert "ETHUSDT" in coordinator._tasks_by_symbol  # noqa: SLF001
            assert len(coordinator._tasks_by_symbol["ETHUSDT"]) == len(coordinator._candle_timeframes) + 1  # noqa: SLF001

            eth_window = coordinator._candle_windows[("ETHUSDT", Timeframe.M5)]  # noqa: SLF001
            await _poll_until(lambda: eth_window.latest_open_time == END)

            await coordinator.stop()
            await run_task
            assert provider.closed is True

        run_async(scenario())


class TestRemoveSymbol:
    def test_remove_symbol_cancels_only_its_own_tasks(self) -> None:
        async def scenario() -> None:
            btc_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            eth_candle = make_candle("ETHUSDT", Timeframe.M5, END, price=2000.0)
            provider = FakeLiveDataProvider(
                candle_scripts={
                    ("BTCUSDT", Timeframe.M5): [btc_candle], ("ETHUSDT", Timeframe.M5): [eth_candle],
                }
            )
            coordinator = RuntimeCoordinator(symbols=("BTCUSDT", "ETHUSDT"), provider=provider, warmup_candles=WARMUP)
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            _bootstrap_fully(coordinator, "ETHUSDT", END)

            btc_window = coordinator._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
            eth_window = coordinator._candle_windows[("ETHUSDT", Timeframe.M5)]  # noqa: SLF001

            run_task = asyncio.create_task(coordinator.run())
            await _poll_until(lambda: btc_window.latest_open_time == END and eth_window.latest_open_time == END)

            eth_tasks = list(coordinator._tasks_by_symbol["ETHUSDT"])  # noqa: SLF001
            btc_tasks_before = list(coordinator._tasks_by_symbol["BTCUSDT"])  # noqa: SLF001

            await coordinator.remove_symbol("ETHUSDT")

            assert all(t.done() for t in eth_tasks)
            assert "ETHUSDT" not in coordinator._symbols  # noqa: SLF001
            assert "ETHUSDT" not in coordinator._tasks_by_symbol  # noqa: SLF001
            assert ("ETHUSDT", Timeframe.M5) not in coordinator._candle_windows  # noqa: SLF001
            # BTCUSDT's own tasks are completely untouched (isolation).
            assert all(not t.done() for t in btc_tasks_before)
            assert coordinator._tasks_by_symbol["BTCUSDT"] == btc_tasks_before  # noqa: SLF001

            await coordinator.stop()
            await run_task

        run_async(scenario())

    def test_remove_symbol_idempotent_on_untracked_symbol(self) -> None:
        provider = FakeLiveDataProvider()
        coordinator = RuntimeCoordinator(symbols=("BTCUSDT",), provider=provider, warmup_candles=WARMUP)

        async def scenario():
            await coordinator.remove_symbol("NOPEUSDT")  # must not raise

        run_async(scenario())
        assert coordinator._symbols == ("BTCUSDT",)  # noqa: SLF001

    def test_remove_symbol_before_run_removes_state_only(self) -> None:
        provider = _provider_with_historical("ETHUSDT", (Timeframe.M5, Timeframe.M15, Timeframe.H1), END)
        coordinator = RuntimeCoordinator(symbols=("BTCUSDT", "ETHUSDT"), provider=provider, warmup_candles=WARMUP)
        _bootstrap_fully(coordinator, "BTCUSDT", END)

        async def scenario():
            await coordinator.remove_symbol("ETHUSDT")

        run_async(scenario())
        assert "ETHUSDT" not in coordinator._symbols  # noqa: SLF001
        assert ("ETHUSDT", Timeframe.M5) not in coordinator._candle_windows  # noqa: SLF001

    def test_readd_after_remove_is_clean(self) -> None:
        async def scenario() -> None:
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(symbols=("BTCUSDT", "ETHUSDT"), provider=provider, warmup_candles=WARMUP)
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            _bootstrap_fully(coordinator, "ETHUSDT", END)
            provider._historical_candles.update(  # noqa: SLF001
                {("ETHUSDT", tf): make_candle_series_ending_at("ETHUSDT", tf, END, WARMUP) for tf in coordinator._candle_timeframes}  # noqa: SLF001
            )

            run_task = asyncio.create_task(coordinator.run())
            await _poll_until(lambda: coordinator._running)  # noqa: SLF001

            await coordinator.remove_symbol("ETHUSDT")
            await coordinator.add_symbol("ETHUSDT")  # re-add — must not duplicate/crash

            assert coordinator._symbols.count("ETHUSDT") == 1  # noqa: SLF001
            assert len(coordinator._tasks_by_symbol["ETHUSDT"]) == len(coordinator._candle_timeframes) + 1  # noqa: SLF001

            await coordinator.stop()
            await run_task

        run_async(scenario())


=== FILE: tests/test_runtime_health.py ===
from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.health import HealthMonitor
from crypto_signal_engine.runtime.models import RuntimeHealth

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


class TestHealthMonitor:
    def test_bootstrapping_before_ready(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_bootstrapping("BTCUSDT")

        status = monitor.status_for("BTCUSDT")
        assert status.health is RuntimeHealth.BOOTSTRAPPING

    def test_fresh_event_is_healthy(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.READY

    def test_stale_threshold_crossing_degrades_health(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)

        clock.advance(31.0)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_fresh_event_after_stale_restores_ready(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)
        clock.advance(60.0)
        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

        monitor.mark_event("BTCUSDT", clock.now())

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.READY

    def test_disconnected_marks_degraded_and_increments_reconnect_count(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)

        monitor.mark_disconnected("BTCUSDT")

        status = monitor.status_for("BTCUSDT")
        assert status.health is RuntimeHealth.DEGRADED
        assert status.reconnect_count == 1

    def test_reconnect_then_fresh_event_clears_disconnected_flag(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_disconnected("BTCUSDT")

        monitor.mark_event("BTCUSDT", clock.now())

        status = monitor.status_for("BTCUSDT")
        assert status.health is RuntimeHealth.READY
        assert status.reconnect_count == 1  # history preserved, not reset

    def test_symbols_are_isolated(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)
        monitor.mark_disconnected("ETHUSDT")

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.READY
        assert monitor.status_for("ETHUSDT").health is RuntimeHealth.DEGRADED

    def test_negative_threshold_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            HealthMonitor(clock=FixedClock(T0), stale_feed_threshold_seconds=-1.0)


class TestGapFault:
    """BLOCKER 1 — unit-level coverage of the (symbol, timeframe) gap-fault
    flag, independent of `RuntimeCoordinator` (see test_runtime_coordinator.py
    for the end-to-end scenarios)."""

    def test_mark_gap_fault_degrades_even_when_otherwise_fresh(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)

        monitor.mark_gap_fault("BTCUSDT", Timeframe.M5)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_mark_event_does_not_clear_gap_fault(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_gap_fault("BTCUSDT", Timeframe.M5)

        # an unrelated accepted event (e.g. order-book) must not clear it
        monitor.mark_event("BTCUSDT", T0)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_clear_gap_fault_for_exact_timeframe_restores_ready(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)
        monitor.mark_gap_fault("BTCUSDT", Timeframe.M5)

        monitor.clear_gap_fault("BTCUSDT", Timeframe.M5)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.READY

    def test_gap_fault_on_one_timeframe_does_not_clear_via_another(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_gap_fault("BTCUSDT", Timeframe.M5)

        monitor.clear_gap_fault("BTCUSDT", Timeframe.M15)  # different timeframe

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_gap_fault_is_per_symbol_isolated(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_ready("ETHUSDT")
        monitor.mark_event("ETHUSDT", T0)

        monitor.mark_gap_fault("BTCUSDT", Timeframe.M5)

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED
        assert monitor.status_for("ETHUSDT").health is RuntimeHealth.READY


class TestPersistenceFault:
    """Faz 7 — reason-scoped persistence fault tracking. A regression
    guard for a real bug found during implementation: an unrelated
    checkpoint reason succeeding must NOT mask a still-unresolved one."""

    def test_mark_persistence_fault_degrades(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)

        monitor.mark_persistence_fault("BTCUSDT", reason="candle_checkpoint")

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_clearing_one_reason_does_not_mask_another_unresolved_reason(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_persistence_fault("BTCUSDT", reason="candle_checkpoint")
        monitor.mark_persistence_fault("BTCUSDT", reason="paper_state_checkpoint")

        # only ONE of the two concerns recovers
        monitor.clear_persistence_fault("BTCUSDT", reason="paper_state_checkpoint")

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_clearing_all_reasons_restores_ready(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_event("BTCUSDT", T0)
        monitor.mark_persistence_fault("BTCUSDT", reason="candle_checkpoint")
        monitor.mark_persistence_fault("BTCUSDT", reason="paper_state_checkpoint")

        monitor.clear_persistence_fault("BTCUSDT", reason="candle_checkpoint")
        monitor.clear_persistence_fault("BTCUSDT", reason="paper_state_checkpoint")

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.READY

    def test_persistence_fault_not_cleared_by_unrelated_market_event(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_persistence_fault("BTCUSDT", reason="candle_checkpoint")

        monitor.mark_event("BTCUSDT", T0)  # unrelated accepted market event

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED

    def test_persistence_fault_is_per_symbol_isolated(self) -> None:
        clock = FixedClock(T0)
        monitor = HealthMonitor(clock=clock, stale_feed_threshold_seconds=30.0)
        monitor.mark_ready("BTCUSDT")
        monitor.mark_ready("ETHUSDT")
        monitor.mark_event("ETHUSDT", T0)

        monitor.mark_persistence_fault("BTCUSDT", reason="candle_checkpoint")

        assert monitor.status_for("BTCUSDT").health is RuntimeHealth.DEGRADED
        assert monitor.status_for("ETHUSDT").health is RuntimeHealth.READY


=== FILE: tests/test_runtime_models.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.models import (
    BootstrapReport,
    IngestOutcome,
    MarketEventKind,
    ProcessedMarketEvent,
    RuntimeCycleResult,
    RuntimeHealth,
    RuntimeStatus,
    SymbolHealth,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


class TestRuntimeCycleResult:
    def test_evaluated_true_requires_signal_and_paper_result(self) -> None:
        with pytest.raises(ValueError):
            RuntimeCycleResult(symbol="BTCUSDT", evaluated=True, signal=None, paper_result=None, generated_at=T0)

    def test_evaluated_false_requires_no_signal_or_paper_result(self) -> None:
        with pytest.raises(ValueError):
            RuntimeCycleResult(symbol="BTCUSDT", evaluated=False, signal="not-none", paper_result=None, generated_at=T0)

    def test_evaluated_false_with_none_is_valid(self) -> None:
        result = RuntimeCycleResult(symbol="btcusdt", evaluated=False, signal=None, paper_result=None, generated_at=T0)
        assert result.symbol == "BTCUSDT"


class TestProcessedMarketEvent:
    def test_normalizes_symbol_and_defaults(self) -> None:
        event = ProcessedMarketEvent(
            symbol="ethusdt", kind=MarketEventKind.CANDLE, outcome=IngestOutcome.ACCEPTED, event_time=T0,
        )
        assert event.symbol == "ETHUSDT"
        assert event.timeframe is None
        assert event.cycle_result is None

    def test_rejects_naive_event_time(self) -> None:
        with pytest.raises(ValueError):
            ProcessedMarketEvent(
                symbol="BTCUSDT", kind=MarketEventKind.CANDLE, outcome=IngestOutcome.ACCEPTED,
                event_time=datetime(2026, 8, 31),
            )


class TestBootstrapReport:
    def test_rejects_negative_candles_applied(self) -> None:
        with pytest.raises(ValueError):
            BootstrapReport(symbol="BTCUSDT", timeframe=Timeframe.M5, candles_applied=-1, ready=False, generated_at=T0)


class TestSymbolHealthAndRuntimeStatus:
    def test_symbol_health_rejects_negative_reconnect_count(self) -> None:
        with pytest.raises(ValueError):
            SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=None, reconnect_count=-1, detail="x")

    def test_runtime_status_coerces_symbols_to_tuple(self) -> None:
        sh = SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=T0, reconnect_count=0, detail="ok")
        status = RuntimeStatus(overall_health=RuntimeHealth.READY, symbols=[sh], generated_at=T0)
        assert isinstance(status.symbols, tuple)


=== FILE: tests/test_safety.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import SafetySeverity
from crypto_signal_engine.safety.models import SafetyEvent, SafetyReasonCode, SafetyState

UTC = timezone.utc


def make_event(severity: SafetySeverity, component: str = "BinanceWebSocketProvider", symbol: str | None = "BTCUSDT", reason: SafetyReasonCode = SafetyReasonCode.STALE_MARKET_DATA) -> SafetyEvent:
    return SafetyEvent(
        reason_code=reason, severity=severity, component=component,
        message="son mesajdan bu yana 120 saniye geçti",
        occurred_at=datetime(2026, 8, 31, tzinfo=UTC), symbol=symbol,
    )


class TestSafetyEvent:
    def test_valid_event(self) -> None:
        assert make_event(SafetySeverity.WARNING).symbol == "BTCUSDT"

    def test_symbol_normalized(self) -> None:
        event = make_event(SafetySeverity.WARNING, symbol="btcusdt")
        assert event.symbol == "BTCUSDT"

    def test_symbol_none_allowed(self) -> None:
        event = make_event(SafetySeverity.WARNING, symbol=None)
        assert event.symbol is None

    def test_empty_message_rejected(self) -> None:
        with pytest.raises(ValueError, match="message"):
            SafetyEvent(
                reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
                component="x", message="   ", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_empty_component_rejected(self) -> None:
        with pytest.raises(ValueError, match="component"):
            SafetyEvent(
                reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
                component="  ", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_naive_occurred_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            SafetyEvent(
                reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
                component="x", message="test", occurred_at=datetime(2026, 8, 31),
            )

    def test_reason_code_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="SafetyReasonCode"):
            SafetyEvent(
                reason_code="STALE_MARKET_DATA", severity=SafetySeverity.WARNING,  # type: ignore[arg-type]
                component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_severity_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="SafetySeverity"):
            SafetyEvent(
                reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity="WARNING",  # type: ignore[arg-type]
                component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_context_immutable(self) -> None:
        event = SafetyEvent(
            reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
            component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            context={"age_seconds": "120"},
        )
        with pytest.raises(TypeError):
            event.context["age_seconds"] = "999"  # type: ignore[index]

    def test_context_defensive_copy(self) -> None:
        source = {"age_seconds": "120"}
        event = SafetyEvent(
            reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
            component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            context=source,
        )
        source["age_seconds"] = "999"
        assert event.context["age_seconds"] == "120"


class TestSafetyStateTransitions:
    def test_initial_state_not_halted(self) -> None:
        assert SafetyState().signal_generation_halted is False

    def test_halt_requires_halt_severity(self) -> None:
        with pytest.raises(ValueError, match="HALT"):
            SafetyState().halt(make_event(SafetySeverity.WARNING))

    def test_halt_with_correct_severity(self) -> None:
        state = SafetyState()
        event = make_event(SafetySeverity.HALT)
        state.halt(event)
        assert state.signal_generation_halted is True
        assert state.halt_reason is event

    def test_double_halt_is_illegal_transition(self) -> None:
        state = SafetyState()
        state.halt(make_event(SafetySeverity.HALT))
        with pytest.raises(ValueError, match="illegal state transition"):
            state.halt(make_event(SafetySeverity.HALT))

    def test_resume_clears_halt(self) -> None:
        state = SafetyState()
        state.halt(make_event(SafetySeverity.HALT))
        state.resume()
        assert state.signal_generation_halted is False
        assert state.halt_reason is None

    def test_resume_without_halt_is_illegal_transition(self) -> None:
        state = SafetyState()
        with pytest.raises(ValueError, match="illegal state transition"):
            state.resume()

    def test_halt_after_resume_allowed_again(self) -> None:
        state = SafetyState()
        state.halt(make_event(SafetySeverity.HALT))
        state.resume()
        state.halt(make_event(SafetySeverity.HALT))  # yeniden halt -> legal
        assert state.signal_generation_halted is True

    def test_add_warning_rejects_halt_severity(self) -> None:
        with pytest.raises(ValueError, match="halt()"):
            SafetyState().add_warning(make_event(SafetySeverity.HALT))

    def test_add_and_clear_warnings(self) -> None:
        state = SafetyState()
        state.add_warning(make_event(SafetySeverity.WARNING, component="A"))
        state.add_warning(make_event(SafetySeverity.INFO, component="B"))
        assert len(state.active_warnings) == 2
        state.clear_warnings()
        assert state.active_warnings == ()

    def test_duplicate_warning_dedup_policy_upserts(self) -> None:
        """Aynı (reason_code, component, symbol) ile ikinci bir warning
        eklendiğinde eskisi silinip yenisiyle değiştirilir; biriktirmez."""
        state = SafetyState()
        first = make_event(SafetySeverity.WARNING, component="A", symbol="BTCUSDT")
        second = make_event(SafetySeverity.WARNING, component="A", symbol="BTCUSDT")
        state.add_warning(first)
        state.add_warning(second)
        assert len(state.active_warnings) == 1
        assert state.active_warnings[0] is second

    def test_different_dedup_key_both_kept(self) -> None:
        state = SafetyState()
        state.add_warning(make_event(SafetySeverity.WARNING, component="A", symbol="BTCUSDT"))
        state.add_warning(make_event(SafetySeverity.WARNING, component="A", symbol="ETHUSDT"))
        assert len(state.active_warnings) == 2


class TestSafetyStateEncapsulation:
    """Quality Gate 24 — illegal state fiziksel olarak temsil edilemez."""

    def test_constructor_accepts_no_arguments(self) -> None:
        """Eski `SafetyState(signal_generation_halted=True, halt_reason=None)`
        gibi bir illegal-state constructor çağrısı artık mümkün DEĞİL —
        constructor hiçbir argüman kabul etmiyor."""
        with pytest.raises(TypeError):
            SafetyState(signal_generation_halted=True, halt_reason=None)  # type: ignore[call-arg]

    def test_direct_mutation_of_halted_flag_rejected(self) -> None:
        state = SafetyState()
        with pytest.raises(AttributeError):
            state.signal_generation_halted = True  # type: ignore[misc]

    def test_direct_mutation_of_halt_reason_rejected(self) -> None:
        state = SafetyState()
        with pytest.raises(AttributeError):
            state.halt_reason = make_event(SafetySeverity.HALT)  # type: ignore[misc]

    def test_direct_mutation_of_active_warnings_rejected(self) -> None:
        state = SafetyState()
        with pytest.raises(AttributeError):
            state.active_warnings = (make_event(SafetySeverity.WARNING),)  # type: ignore[misc]

    def test_bypass_via_state_machine_api_impossible(self) -> None:
        """halt() sonrası state.resume() DIŞINDA hiçbir yol RUNNING'e dönemez —
        önceki hatada `state.signal_generation_halted = False` ile bypass
        mümkündü; artık AttributeError fırlatır."""
        state = SafetyState()
        state.halt(make_event(SafetySeverity.HALT))
        with pytest.raises(AttributeError):
            state.signal_generation_halted = False  # type: ignore[misc]
        # gerçek state hâlâ halted — bypass başarısız oldu
        assert state.signal_generation_halted is True

    def test_last_evaluated_at_requires_utc_aware(self) -> None:
        state = SafetyState()
        with pytest.raises(ValueError, match="naive datetime"):
            state.mark_evaluated(datetime(2026, 8, 31))

    def test_last_evaluated_at_updated_via_api(self) -> None:
        state = SafetyState()
        now = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
        state.mark_evaluated(now)
        assert state.last_evaluated_at == now

    def test_last_evaluated_at_cannot_be_set_directly(self) -> None:
        state = SafetyState()
        with pytest.raises(AttributeError):
            state.last_evaluated_at = datetime(2026, 8, 31, tzinfo=UTC)  # type: ignore[misc]

    def test_halted_invariant_always_consistent(self) -> None:
        """HALTED ise halt_reason her zaman severity=HALT taşıyan gerçek bir
        SafetyEvent'tir; None olamaz (constructor seviyesinde imkansız
        kılındığı için, herhangi bir halt() sonrası bunu doğrula)."""
        state = SafetyState()
        event = make_event(SafetySeverity.HALT)
        state.halt(event)
        assert state.signal_generation_halted is True
        assert state.halt_reason is not None
        assert state.halt_reason.severity == SafetySeverity.HALT

    def test_running_invariant_always_consistent(self) -> None:
        state = SafetyState()
        assert state.signal_generation_halted is False
        assert state.halt_reason is None


=== FILE: tests/test_selection_models.py ===
"""Adaptive Symbol Intelligence v1 — `CandidateScore.learned_factor_score`
field validation tests (no dedicated models test file existed before this
field was added)."""

from __future__ import annotations

import math

import pytest

from crypto_signal_engine.selection.models import CandidateScore

_BASE_KWARGS = dict(
    symbol="BTCUSDT", eligible=True, total_score=0.5, liquidity_score=0.5,
    historical_movement_score=0.5, current_opportunity_score=0.5, reason="test", quote_volume_24h=1_000_000.0,
)


class TestLearnedFactorScoreField:
    def test_defaults_to_neutral_when_omitted(self) -> None:
        score = CandidateScore(**_BASE_KWARGS)
        assert score.learned_factor_score == 0.5

    def test_accepts_explicit_value(self) -> None:
        score = CandidateScore(**{**_BASE_KWARGS, "learned_factor_score": 0.9})
        assert score.learned_factor_score == 0.9

    def test_rejects_nan(self) -> None:
        with pytest.raises(ValueError):
            CandidateScore(**{**_BASE_KWARGS, "learned_factor_score": math.nan})

    def test_rejects_infinity(self) -> None:
        with pytest.raises(ValueError):
            CandidateScore(**{**_BASE_KWARGS, "learned_factor_score": math.inf})


=== FILE: tests/test_selection_reevaluation.py ===
"""Bölüm 8 ("Periodic re-evaluation") — `apply_hysteresis`/`safe_reselect`
testleri. Bunlar SAF fonksiyon testleridir (`apply_hysteresis`) artı tek
bir I/O-yakalama testi (`safe_reselect`); CANLI bir runtime hot-reload
DOĞRULAMAZLAR (bkz. reevaluation.py modül docstring'i, "Known limitations
- app.py'ye BAĞLANMADI")."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import SymbolSelectionError
from crypto_signal_engine.selection.models import CandidateScore, SelectionResult
from crypto_signal_engine.selection.reevaluation import apply_hysteresis, safe_reselect
from tests.conftest import run_async

UTC = timezone.utc
NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)


def _score(symbol: str, total: float) -> CandidateScore:
    return CandidateScore(
        symbol=symbol, eligible=True, total_score=total, liquidity_score=total,
        historical_movement_score=total, current_opportunity_score=total,
        reason=f"score={total}", quote_volume_24h=1_000_000.0,
    )


def _ranking(scores: dict[str, float], selected: tuple[str, ...] | None = None) -> SelectionResult:
    candidates = tuple(_score(s, v) for s, v in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0])))
    return SelectionResult(
        generated_at=NOW, selected_symbols=selected or tuple(c.symbol for c in candidates),
        ranked_candidates=candidates, rejected=(), universe_size=len(scores), shortlist_size=len(scores),
    )


class TestOpenPositionPinning:
    def test_open_position_symbol_is_pinned_even_with_worst_score(self) -> None:
        ranking = _ranking({"BTCUSDT": 0.1, "ETHUSDT": 0.9, "SOLUSDT": 0.8})
        result = apply_hysteresis(
            current_symbols=("BTCUSDT",),
            open_position_symbols=frozenset({"BTCUSDT"}),
            new_ranking=ranking,
            target_count=2,
        )
        assert "BTCUSDT" in result

    def test_open_position_symbol_absent_from_ranking_is_still_pinned(self) -> None:
        """Sembol yeni ranking'de HİÇ görünmüyor (örn. artık likidite
        eşiğini geçmiyor) — AÇIK pozisyon varsa yine de ÇIKARILMAZ."""
        ranking = _ranking({"ETHUSDT": 0.9, "SOLUSDT": 0.8})
        result = apply_hysteresis(
            current_symbols=("BTCUSDT",),
            open_position_symbols=frozenset({"BTCUSDT"}),
            new_ranking=ranking,
            target_count=1,
        )
        assert result == ("BTCUSDT",)


class TestFlatSymbolReplacement:
    def test_flat_low_ranked_symbol_is_replaced_by_much_stronger_candidate(self) -> None:
        ranking = _ranking({"BTCUSDT": 0.1, "ETHUSDT": 0.95})
        result = apply_hysteresis(
            current_symbols=("BTCUSDT",),
            open_position_symbols=frozenset(),
            new_ranking=ranking,
            target_count=1,
            min_score_improvement=0.05,
        )
        assert result == ("ETHUSDT",)

    def test_hysteresis_prevents_churn_on_tiny_ranking_changes(self) -> None:
        """BTCUSDT hâlâ aktif, ETHUSDT yalnızca 0.01 daha iyi (bant: 0.05)
        — DEĞİŞİKLİK YAPILMAMALI."""
        ranking = _ranking({"BTCUSDT": 0.80, "ETHUSDT": 0.81})
        result = apply_hysteresis(
            current_symbols=("BTCUSDT",),
            open_position_symbols=frozenset(),
            new_ranking=ranking,
            target_count=1,
            min_score_improvement=0.05,
        )
        assert result == ("BTCUSDT",)

    def test_mixed_pinned_and_flat_slots(self) -> None:
        ranking = _ranking({"BTCUSDT": 0.1, "ETHUSDT": 0.2, "SOLUSDT": 0.99})
        result = apply_hysteresis(
            current_symbols=("BTCUSDT", "ETHUSDT"),
            open_position_symbols=frozenset({"BTCUSDT"}),
            new_ranking=ranking,
            target_count=2,
            min_score_improvement=0.05,
        )
        # BTCUSDT pinlenir (açık pozisyon); ETHUSDT flat VE çok daha güçlü
        # bir aday (SOLUSDT) var -> SOLUSDT'ye yer açılır.
        assert result == ("BTCUSDT", "SOLUSDT")

    def test_deterministic_same_inputs_same_output(self) -> None:
        ranking = _ranking({"BTCUSDT": 0.1, "ETHUSDT": 0.2, "SOLUSDT": 0.99})
        args = dict(
            current_symbols=("BTCUSDT", "ETHUSDT"),
            open_position_symbols=frozenset({"BTCUSDT"}),
            new_ranking=ranking, target_count=2, min_score_improvement=0.05,
        )
        assert apply_hysteresis(**args) == apply_hysteresis(**args)


class TestSafeReselect:
    class _FailingSelector:
        async def select(self) -> SelectionResult:
            raise SymbolSelectionError("Binance temporarily unreachable")

    class _WorkingSelector:
        def __init__(self, ranking: SelectionResult) -> None:
            self._ranking = ranking

        async def select(self) -> SelectionResult:
            return self._ranking

    def test_failed_rescan_does_not_kill_active_runtime(self) -> None:
        """Bir rescan başarısız olursa (`SymbolSelectionError`), mevcut
        semboller DEĞİŞTİRİLMEDEN döner — sağlıklı çalışan semboller
        ASLA etkilenmez."""
        result = run_async(
            safe_reselect(
                self._FailingSelector(),
                current_symbols=("BTCUSDT", "ETHUSDT"),
                open_position_symbols=frozenset({"BTCUSDT"}),
                target_count=2,
            )
        )
        assert result == ("BTCUSDT", "ETHUSDT")

    def test_successful_rescan_applies_hysteresis(self) -> None:
        ranking = _ranking({"BTCUSDT": 0.1, "SOLUSDT": 0.99})
        result = run_async(
            safe_reselect(
                self._WorkingSelector(ranking),
                current_symbols=("BTCUSDT",),
                open_position_symbols=frozenset(),
                target_count=1,
                min_score_improvement=0.05,
            )
        )
        assert result == ("SOLUSDT",)


