<!-- all_code_part2.md — Part 2/2 — 13 files -->
<!-- Contents of this part: -->
<!--   - tests/test_selection_selector.py -->
<!--   - tests/test_signal_contract.py -->
<!--   - tests/test_signal_testnet_bridge.py -->
<!--   - tests/test_signal_testnet_bridge_lifecycle_integration.py -->
<!--   - tests/test_sqlite_store.py -->
<!--   - tests/test_stability_determinism.py -->
<!--   - tests/test_stability_harness.py -->
<!--   - tests/test_stability_resource_growth.py -->
<!--   - tests/test_stability_scenarios.py -->
<!--   - tests/test_state_contract.py -->
<!--   - tests/test_state_manager.py -->
<!--   - tests/test_trade_features.py -->
<!--   - tests/test_validation_helpers.py -->

=== FILE: tests/test_selection_selector.py ===
"""`AutomaticSymbolSelector` testleri — TAMAMEN offline (FakeHttpClient),
gerçek Binance ağına ASLA bağımlı değil (bkz. tests/binance_fakes.py)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import SymbolSelectionError
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig
from crypto_signal_engine.selection.selector import AutomaticSymbolSelector
from tests.binance_fakes import FakeHttpClient, json_response
from tests.conftest import run_async

UTC = timezone.utc
NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)


def _exchange_symbol(
    symbol: str, base: str, *, quote: str = "USDT", status: str = "TRADING",
    spot_allowed: bool = True, permissions: list[str] | None = None,
) -> dict:
    return {
        "symbol": symbol, "baseAsset": base, "quoteAsset": quote, "status": status,
        "isSpotTradingAllowed": spot_allowed, "permissions": permissions or ["SPOT"],
    }


def _exchange_info(symbols: list[dict]) -> tuple[int, str]:
    return json_response({"symbols": symbols})


def _ticker(symbol: str, quote_volume: float, *, change_pct: float = 1.0, count: int = 50_000) -> dict:
    return {"symbol": symbol, "quoteVolume": str(quote_volume), "priceChangePercent": str(change_pct), "count": count}


def _tickers(rows: list[dict]) -> tuple[int, str]:
    return json_response(rows)


def _kline_row(open_ms: int, close: float, *, open_: float | None = None, volume: float = 100.0) -> list:
    open_price = open_ if open_ is not None else close
    high = max(open_price, close) * 1.001
    low = min(open_price, close) * 0.999
    return [open_ms, str(open_price), str(high), str(low), str(close), str(volume), open_ms + 5 * 60_000 - 1,
            str(volume * close), 10, str(volume / 2), str(volume * close / 2), "0"]


def _flat_candles(count: int, *, price: float = 100.0, volume: float = 100.0, start_index: int = 0) -> list:
    """Sabit fiyat/hacim — sıfır volatilite/momentum/relative-volume.

    `start_index`: iki segmenti (`a + b`) birleştirirken open_time'ların
    KESİNTİSİZ artan kalması için — bkz. `_trending_candles` docstring'i."""
    return [_kline_row((start_index + i) * 5 * 60_000, price, open_=price, volume=volume) for i in range(count)]


def _trending_candles(
    count: int, *, start: float = 100.0, step_pct: float = 0.01, volume: float = 100.0, start_index: int = 0
) -> list:
    """Her mumda sabit yüzde artış — belirgin, ölçülebilir momentum/ATR.

    `start_index`: bu segment BAŞKA bir segmentin ARDINDAN geliyorsa
    (örn. `_flat_candles(15) + _trending_candles(5, start_index=15, ...)`),
    open_time dizisinin KESİNTİSİZ/artan kalması için önceki segmentin
    uzunluğu buraya verilmelidir — aksi halde iki segment aynı open_time
    aralığını TEKRAR eder ve REST pagination'ın "ascending olmayan candle"
    koruması (bkz. `rest.py::fetch_historical_candles`) haklı olarak
    reddeder."""
    rows = []
    price = start
    for i in range(count):
        open_price = price
        price = price * (1 + step_pct)
        rows.append(_kline_row((start_index + i) * 5 * 60_000, price, open_=open_price, volume=volume))
    return rows


def _klines_response(rows: list) -> tuple[int, str]:
    return json_response(rows)


def make_selector(
    http_responses: list, config: AutoSymbolSelectionConfig | None = None, *, learned_factor_provider=None,
) -> AutomaticSymbolSelector:
    binance_config = BinanceConfig()
    client = BinanceRestClient(binance_config, FakeHttpClient(http_responses), FixedClock(NOW), FakeSleeper())
    return AutomaticSymbolSelector(
        client, FixedClock(NOW), config or AutoSymbolSelectionConfig(),
        learned_factor_provider=learned_factor_provider,
    )


SMALL_CONFIG = AutoSymbolSelectionConfig(
    target_count=2, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
    lookback_candles=20, recent_window_candles=5,
)


class TestCandidateDiscoveryFiltering:
    def test_excludes_non_usdt_non_trading_and_unsuitable_symbols(self) -> None:
        symbols = [
            _exchange_symbol("BTCUSDT", "BTC"),
            _exchange_symbol("ETHBTC", "ETH", quote="BTC"),  # non-USDT
            _exchange_symbol("ADAUSDT", "ADA", status="BREAK"),  # not TRADING
            _exchange_symbol("XRPUSDT", "XRP", spot_allowed=False),  # spot not allowed
            _exchange_symbol("BTCUPUSDT", "BTCUP"),  # leveraged-token-like base asset
            _exchange_symbol("SUPUSDT", "SUP"),  # short "UP"-ending base — must NOT be excluded
        ]
        tickers = [
            _ticker("BTCUSDT", 50_000_000),
            _ticker("ETHBTC", 50_000_000),
            _ticker("ADAUSDT", 50_000_000),
            _ticker("XRPUSDT", 50_000_000),
            _ticker("BTCUPUSDT", 50_000_000),
            _ticker("SUPUSDT", 50_000_000),
        ]
        candles = _trending_candles(20)
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(candles), _klines_response(candles)],
            config=AutoSymbolSelectionConfig(target_count=2, shortlist_size=5, lookback_candles=20, recent_window_candles=5),
        )
        result = run_async(selector.select())
        # Yalnızca BTCUSDT ve SUPUSDT temel eligibility'yi geçebilirdi.
        assert result.universe_size == 2
        assert set(result.selected_symbols) <= {"BTCUSDT", "SUPUSDT"}
        assert "ETHBTC" not in result.selected_symbols
        assert "ADAUSDT" not in result.selected_symbols
        assert "XRPUSDT" not in result.selected_symbols
        assert "BTCUPUSDT" not in result.selected_symbols

    def test_liquidity_threshold_excludes_low_volume_symbols(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("DOGEUSDT", "DOGE")]
        tickers = [_ticker("BTCUSDT", 50_000_000), _ticker("DOGEUSDT", 100.0)]  # DOGE below threshold
        candles = _trending_candles(20)
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(candles)],
            config=AutoSymbolSelectionConfig(
                target_count=2, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
                lookback_candles=20, recent_window_candles=5,
            ),
        )
        result = run_async(selector.select())
        assert result.selected_symbols == ("BTCUSDT",)
        assert result.shortlist_size == 1

    def test_insufficient_history_symbol_is_rejected_not_selected(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 50_000_000), _ticker("ETHUSDT", 40_000_000)]
        good_candles = _trending_candles(20)
        short_candles = _trending_candles(5)  # < lookback_candles=20
        # Shortlist sırası quoteVolume'a göre AZALAN'dır (BTCUSDT 50M > ETHUSDT 40M) —
        # klines yanıtları BU sırayla kuyruğa alınır.
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(good_candles), _klines_response(short_candles)],
            config=SMALL_CONFIG,
        )
        result = run_async(selector.select())
        assert result.selected_symbols == ("BTCUSDT",)
        rejected_symbols = {c.symbol for c in result.rejected}
        assert "ETHUSDT" in rejected_symbols
        rejected = next(c for c in result.rejected if c.symbol == "ETHUSDT")
        assert not rejected.eligible
        assert "insufficient history" in rejected.reason


class TestScoringAndRanking:
    def test_historical_movement_affects_ranking(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 10_000_000), _ticker("ETHUSDT", 10_000_000)]
        volatile = _trending_candles(20, step_pct=0.02)
        # Tamamen düz (sıfır ROC) DEĞİL — mutlak aktivite tabanını GEÇEN
        # ama BTC'den ÇOK daha ILIMLI bir hareket (bkz. min_atr_pct/
        # min_current_move_pct varsayılanları, `SMALL_CONFIG` bunları
        # override ETMEZ).
        mild = _trending_candles(20, step_pct=0.001)
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(volatile), _klines_response(mild)],
            config=SMALL_CONFIG,
        )
        result = run_async(selector.select())
        by_symbol = {c.symbol: c for c in result.ranked_candidates}
        assert by_symbol["BTCUSDT"].historical_movement_score > by_symbol["ETHUSDT"].historical_movement_score
        assert by_symbol["BTCUSDT"].total_score > by_symbol["ETHUSDT"].total_score

    def test_current_activity_affects_ranking(self) -> None:
        """İki sembol AYNI toplam tarihsel harekete sahip ama biri son
        pencerede (recent_window) BELİRGİN şekilde daha aktif/hareketli —
        current_opportunity bunu ayırt edebilmeli."""
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 10_000_000), _ticker("ETHUSDT", 10_000_000)]
        # BTC: tüm hareket EN SON 5 mumda (current aktif); ETH: tüm hareket
        # pencerenin BAŞINDA, son 5 mumda YALNIZCA ÇOK ILIMLI bir kalıntı
        # hareket var (mutlak tabanı GEÇER ama "currently dead"e YAKIN —
        # tamamen sıfır ROC DEĞİL, aksi halde eligibility'de reddedilirdi).
        btc_candles = _flat_candles(15) + _trending_candles(5, start=100.0, step_pct=0.03, volume=500.0, start_index=15)
        eth_trend = _trending_candles(15, step_pct=0.03)
        eth_last_close = float(eth_trend[-1][4])
        eth_candles = eth_trend + _trending_candles(
            5, start=eth_last_close, step_pct=0.0008, volume=100.0, start_index=15
        )
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(btc_candles), _klines_response(eth_candles)],
            config=SMALL_CONFIG,
        )
        result = run_async(selector.select())
        by_symbol = {c.symbol: c for c in result.ranked_candidates}
        assert by_symbol["BTCUSDT"].current_opportunity_score > by_symbol["ETHUSDT"].current_opportunity_score

    def test_highest_raw_volatility_alone_does_not_automatically_win(self) -> None:
        """Bir sembol EN yüksek ham volatiliteye/harekete sahip ama
        likiditesi ve GÜNCEL aktivitesi çok zayıf; diğeri daha ILIMLI
        hareketli ama likit VE şu an aktif. Bileşik skor ikinciyi
        seçmelidir — "en büyük tek-günlük pump otomatik kazanmaz"."""
        symbols = [_exchange_symbol("PUMPUSDT", "PUMP"), _exchange_symbol("STEADYUSDT", "STEADY")]
        tickers = [
            _ticker("PUMPUSDT", 1_100_000.0),  # likidite eşiğinin hemen üstü — zayıf
            _ticker("STEADYUSDT", 200_000_000.0),  # çok likit
        ]
        # PUMP: devasa tarihsel hareket ama SON pencerede NEREDEYSE durgun
        # (mutlak tabanı GEÇEN ama ÇOK zayıf) ve düşük hacim.
        pump_candles = _trending_candles(15, step_pct=0.15, volume=50.0) + _trending_candles(
            5, start=100 * 1.15 ** 15, step_pct=0.0006, volume=10.0, start_index=15
        )
        # STEADY: ılımlı tarihsel hareket ama SON pencerede net aktivite/hacim artışı.
        steady_candles = _trending_candles(15, step_pct=0.01, volume=100.0) + _trending_candles(
            5, start=100 * 1.01 ** 15, step_pct=0.015, volume=1000.0, start_index=15
        )
        # Shortlist sırası quoteVolume'a göre AZALAN'dır (STEADY 200M > PUMP 1.1M) —
        # klines yanıtları BU sırayla kuyruğa alınır.
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(steady_candles), _klines_response(pump_candles)],
            config=AutoSymbolSelectionConfig(
                target_count=1, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
                lookback_candles=20, recent_window_candles=5,
            ),
        )
        result = run_async(selector.select())
        by_symbol = {c.symbol: c for c in result.ranked_candidates}
        assert by_symbol["PUMPUSDT"].historical_movement_score > by_symbol["STEADYUSDT"].historical_movement_score
        assert result.selected_symbols == ("STEADYUSDT",)

    def test_ranking_is_deterministic(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 10_000_000), _ticker("ETHUSDT", 20_000_000)]
        c1, c2 = _trending_candles(20, step_pct=0.01), _trending_candles(20, step_pct=0.02)

        def _run_once():
            selector = make_selector(
                [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
                config=SMALL_CONFIG,
            )
            return run_async(selector.select())

        first, second = _run_once(), _run_once()
        assert first.selected_symbols == second.selected_symbols
        assert [c.total_score for c in first.ranked_candidates] == [c.total_score for c in second.ranked_candidates]
        assert [c.symbol for c in first.ranked_candidates] == [c.symbol for c in second.ranked_candidates]

    def test_returns_configured_count_when_enough_eligible(self) -> None:
        symbols = [_exchange_symbol(f"SYM{i}USDT", f"SYM{i}") for i in range(4)]
        tickers = [_ticker(f"SYM{i}USDT", 10_000_000 + i) for i in range(4)]
        candles_responses = [_klines_response(_trending_candles(20, step_pct=0.01 * (i + 1))) for i in range(4)]
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), *candles_responses],
            config=AutoSymbolSelectionConfig(target_count=3, shortlist_size=10, lookback_candles=20, recent_window_candles=5),
        )
        result = run_async(selector.select())
        assert len(result.selected_symbols) == 3

    def test_handles_insufficient_eligible_symbols_cleanly(self) -> None:
        """Sadece 1 eligible sembol var ama target_count=5 istendi —
        sistem 5 İCAT ETMEZ, mevcut 1 taneyi döner."""
        symbols = [_exchange_symbol("BTCUSDT", "BTC")]
        tickers = [_ticker("BTCUSDT", 10_000_000)]
        candles = _trending_candles(20)
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(candles)],
            config=AutoSymbolSelectionConfig(target_count=5, shortlist_size=10, lookback_candles=20, recent_window_candles=5),
        )
        result = run_async(selector.select())
        assert result.selected_symbols == ("BTCUSDT",)


class TestAbsoluteActivityFloors:
    """Bölüm B/D self-review senaryoları: "Liquidity is NOT opportunity" ve
    "Weak batch normalization" — mutlak (batch-göreli DEĞİL) eligibility
    tabanlarının GERÇEKTEN uygulandığını doğrular."""

    def test_ultra_liquid_but_effectively_inactive_market_cannot_win_from_liquidity_alone(self) -> None:
        """USDCUSDT-benzeri bir "stablecoin çifti" senaryosu: DEVASA
        likidite ama neredeyse SIFIR fiyat hareketi — genel bir "aktiflik"
        kuralıyla (mutlak ATR%/ROC tabanı) tamamen ELENMELİDİR, salt
        likiditesi yüzünden ASLA "top fırsat" olamaz."""
        symbols = [_exchange_symbol("USDCUSDT", "USDC"), _exchange_symbol("ARBUSDT", "ARB")]
        tickers = [
            _ticker("USDCUSDT", 3_000_000_000.0),  # devasa likidite
            _ticker("ARBUSDT", 50_000_000.0),  # mütevazı likidite ama GERÇEKTEN hareketli
        ]
        # USDC: fiyat pratik olarak SABİT (yalnızca kuruş-altı gürültü) — gerçek bir stablecoin gibi.
        stablecoin_candles = _trending_candles(20, step_pct=0.00002)
        active_candles = _trending_candles(20, step_pct=0.015)
        # Shortlist sırası: USDCUSDT (3B) > ARBUSDT (50M) quoteVolume'a göre.
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(stablecoin_candles), _klines_response(active_candles)],
            config=AutoSymbolSelectionConfig(
                target_count=1, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
                lookback_candles=20, recent_window_candles=5,
            ),
        )
        result = run_async(selector.select())
        rejected_symbols = {c.symbol for c in result.rejected}
        assert "USDCUSDT" in rejected_symbols
        rejected = next(c for c in result.rejected if c.symbol == "USDCUSDT")
        assert not rejected.eligible
        assert "absolute" in rejected.reason and "activity floor" in rejected.reason
        assert result.selected_symbols == ("ARBUSDT",)

    def test_weak_batch_cannot_make_least_bad_candidate_look_artificially_strong(self) -> None:
        """TÜM kısa liste adayları mutlak olarak zayıf (gerçek bir "fırsat"
        yok) — batch-göreli min-max normalizasyon, en az kötüsünü SAHTE
        biçimde "1.0/mükemmel" göstermemeli; sistem bunun yerine TÜMÜNÜ
        reddedip AÇIKÇA başarısız olmalıdır (icat edilmiş bir "kazanan" YOK)."""
        symbols = [_exchange_symbol("AUSDT", "A"), _exchange_symbol("BUSDT", "B"), _exchange_symbol("CUSDT", "C")]
        tickers = [_ticker("AUSDT", 5_000_000), _ticker("BUSDT", 5_000_000), _ticker("CUSDT", 5_000_000)]
        # Üçü de neredeyse tamamen durgun (mutlak tabanın ÇOK altında) —
        # aralarında GÖRECELİ farklar olsa bile HİÇBİRİ gerçekten aktif değil.
        weak_candles = [
            _klines_response(_trending_candles(20, step_pct=0.000005)),
            _klines_response(_trending_candles(20, step_pct=0.00001)),
            _klines_response(_trending_candles(20, step_pct=0.000002)),
        ]
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), *weak_candles],
            config=AutoSymbolSelectionConfig(
                target_count=1, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
                lookback_candles=20, recent_window_candles=5,
            ),
        )
        with pytest.raises(SymbolSelectionError):
            run_async(selector.select())


class TestPumpProtectionSaturation:
    def test_extreme_outlier_does_not_flatten_other_candidates_to_zero(self) -> None:
        """Bölüm C — bir sembol ASTRONOMİK (örn. %2000) bir hareket
        gösterse bile, doygunluk tavanı (bkz. selector.py sabitleri)
        SAYESİNDE diğer, GERÇEKTEN aktif adayın skoru sıfıra
        BASTIRILMAMALIDIR (saf min-max normalizasyon bunu yapardı)."""
        symbols = [_exchange_symbol("EXTREMEUSDT", "EXTREME"), _exchange_symbol("NORMALUSDT", "NORMAL")]
        tickers = [_ticker("EXTREMEUSDT", 5_000_000), _ticker("NORMALUSDT", 5_000_000)]
        extreme_candles = _trending_candles(20, step_pct=0.20)  # ~38x üzerinde ~%2000 toplam hareket
        normal_candles = _trending_candles(20, step_pct=0.01)  # sağlıklı, sıradan aktivite
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(extreme_candles), _klines_response(normal_candles)],
            config=AutoSymbolSelectionConfig(
                target_count=2, shortlist_size=5, min_quote_volume_24h=1_000_000.0,
                lookback_candles=20, recent_window_candles=5,
            ),
        )
        result = run_async(selector.select())
        by_symbol = {c.symbol: c for c in result.ranked_candidates}
        # Doygunluk tavanı OLMASAYDI NORMALUSDT'nin historical_movement_score'u
        # 0.0'a çok yakın olurdu (min-max, EXTREME'i 1.0'a, geri kalanı 0'a iter).
        assert by_symbol["NORMALUSDT"].historical_movement_score > 0.05


class TestFailureBehavior:
    def test_binance_discovery_failure_raises_symbol_selection_error(self) -> None:
        from crypto_signal_engine.errors import TransportError

        selector = make_selector([TransportError("network down")] * 4)
        with pytest.raises(SymbolSelectionError):
            run_async(selector.select())

    def test_no_symbols_pass_liquidity_filter_raises_cleanly(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC")]
        tickers = [_ticker("BTCUSDT", 1.0)]  # far below any sane threshold
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers)],
            config=AutoSymbolSelectionConfig(min_quote_volume_24h=1_000_000.0, target_count=1, shortlist_size=5),
        )
        with pytest.raises(SymbolSelectionError):
            run_async(selector.select())

    def test_never_invents_a_symbol_not_in_the_discovered_universe(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC")]
        tickers = [_ticker("BTCUSDT", 10_000_000)]
        candles = _trending_candles(20)
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(candles)],
            config=SMALL_CONFIG,
        )
        result = run_async(selector.select())
        assert all(s == "BTCUSDT" for s in result.selected_symbols)


class TestWeightsSumToOne:
    def test_four_weights_sum_to_exactly_one(self) -> None:
        from crypto_signal_engine.selection.selector import (
            CURRENT_OPPORTUNITY_WEIGHT,
            HISTORICAL_MOVEMENT_WEIGHT,
            LEARNED_FACTOR_WEIGHT,
            LIQUIDITY_WEIGHT,
        )

        total = LIQUIDITY_WEIGHT + HISTORICAL_MOVEMENT_WEIGHT + CURRENT_OPPORTUNITY_WEIGHT + LEARNED_FACTOR_WEIGHT
        assert total == 1.0


class TestLearnedFactorProvider:
    """Adaptive Symbol Intelligence v1 — `learned_factor_provider` wiring
    tests. Same discipline as `exit_policy_provider`/`candle_observer`/
    `m1_candle_observer` before it: `None` default is byte-for-byte
    identical, a raising provider never raises into `select()`."""

    def _two_symbol_setup(self):
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 10_000_000), _ticker("ETHUSDT", 10_000_000)]
        candles_btc = _trending_candles(20, step_pct=0.02)
        candles_eth = _trending_candles(20, step_pct=0.015)
        return symbols, tickers, candles_btc, candles_eth

    def test_none_provider_matches_no_provider_argument_at_all(self) -> None:
        symbols, tickers, c1, c2 = self._two_symbol_setup()
        selector_default = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG,
        )
        selector_explicit_none = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG, learned_factor_provider=None,
        )
        result_default = run_async(selector_default.select())
        result_explicit = run_async(selector_explicit_none.select())
        assert result_default == result_explicit

    def test_none_provider_is_byte_for_byte_identical_to_always_neutral_noop_provider(self) -> None:
        """THE required regression test: a no-op provider that ALWAYS
        returns 0.5 must produce an IDENTICAL `SelectionResult` to
        `learned_factor_provider=None` -- proving the None-default path
        and the "provider present but neutral" path are computationally
        indistinguishable, i.e. behavior is unchanged when nothing is
        wired in production."""
        symbols, tickers, c1, c2 = self._two_symbol_setup()
        selector_none = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG,
        )
        selector_noop = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG, learned_factor_provider=lambda symbol: 0.5,
        )
        result_none = run_async(selector_none.select())
        result_noop = run_async(selector_noop.select())
        assert result_none == result_noop

    def test_learned_factor_score_is_neutral_when_provider_is_none(self) -> None:
        symbols, tickers, c1, c2 = self._two_symbol_setup()
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG,
        )
        result = run_async(selector.select())
        assert all(c.learned_factor_score == 0.5 for c in result.ranked_candidates)

    def test_raising_provider_never_raises_into_select_and_falls_back_to_neutral(self) -> None:
        symbols, tickers, c1, c2 = self._two_symbol_setup()

        def _broken_provider(symbol: str) -> float:
            raise RuntimeError(f"simulated learned_factor failure for {symbol}")

        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG, learned_factor_provider=_broken_provider,
        )
        result = run_async(selector.select())  # must not raise
        assert all(c.learned_factor_score == 0.5 for c in result.ranked_candidates)

    def test_provider_result_correctly_incorporated_into_total_score(self) -> None:
        from crypto_signal_engine.selection.selector import LEARNED_FACTOR_WEIGHT

        symbols, tickers, c1, c2 = self._two_symbol_setup()

        def _provider(symbol: str) -> float:
            return 1.0 if symbol == "BTCUSDT" else 0.0

        with_provider = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG, learned_factor_provider=_provider,
        )
        without_provider = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(c1), _klines_response(c2)],
            config=SMALL_CONFIG,
        )
        result_with = {c.symbol: c for c in run_async(with_provider.select()).ranked_candidates}
        result_without = {c.symbol: c for c in run_async(without_provider.select()).ranked_candidates}

        assert result_with["BTCUSDT"].learned_factor_score == 1.0
        assert result_with["ETHUSDT"].learned_factor_score == 0.0
        # BTC's total_score gains exactly LEARNED_FACTOR_WEIGHT * (1.0 - 0.5)
        # versus the neutral-provider baseline; ETH loses the same amount
        # in the other direction (0.0 - 0.5).
        assert result_with["BTCUSDT"].total_score == pytest.approx(
            result_without["BTCUSDT"].total_score + LEARNED_FACTOR_WEIGHT * 0.5
        )
        assert result_with["ETHUSDT"].total_score == pytest.approx(
            result_without["ETHUSDT"].total_score - LEARNED_FACTOR_WEIGHT * 0.5
        )

    def test_rejected_candidates_have_zero_learned_factor_score(self) -> None:
        symbols = [_exchange_symbol("BTCUSDT", "BTC"), _exchange_symbol("ETHUSDT", "ETH")]
        tickers = [_ticker("BTCUSDT", 50_000_000), _ticker("ETHUSDT", 40_000_000)]
        good_candles = _trending_candles(20)
        short_candles = _trending_candles(5)  # < lookback_candles=20 -> rejected via _rejected()
        selector = make_selector(
            [_exchange_info(symbols), _tickers(tickers), _klines_response(good_candles), _klines_response(short_candles)],
            config=SMALL_CONFIG, learned_factor_provider=lambda symbol: 0.9,
        )
        result = run_async(selector.select())
        rejected = next(c for c in result.rejected if c.symbol == "ETHUSDT")
        assert not rejected.eligible
        assert rejected.learned_factor_score == 0.0  # "not computed", matching the other 3 score fields


=== FILE: tests/test_signal_contract.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.signal_engine import SignalEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_history(m5_overrides=None, m15_overrides=None, m1_overrides=None, h1_overrides=None) -> FeatureHistoryStore:
    history = FeatureHistoryStore()
    m1 = {"SPREAD_BPS": 3.0, "DEPTH_IMBALANCE_10": 0.0, "TOB_IMBALANCE": 0.0, "MID_PRICE": 100.0, "MICROPRICE": 100.0}
    m5 = {"RSI_14": 50.0, "ROC_10": 0.0, "VWAP_DEVIATION_20": 0.0, "RELATIVE_VOLUME_20": 1.0, "BOLLINGER_BANDWIDTH_20_2": 0.03}
    m15 = {"ROC_10": 0.0, "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.03, "RELATIVE_VOLUME_20": 1.0, "BODY_TO_RANGE_RATIO": 0.5}
    h1 = {"ROC_10": 0.0, "RSI_14": 50.0, "BOLLINGER_BANDWIDTH_20_2": 0.03, "RELATIVE_VOLUME_20": 1.0, "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.05}
    if m1_overrides: m1.update(m1_overrides)
    if m5_overrides: m5.update(m5_overrides)
    if m15_overrides: m15.update(m15_overrides)
    if h1_overrides: h1.update(h1_overrides)
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values=m1))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M5, as_of=T0, values=m5))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M15, as_of=T0, values=m15))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.H1, as_of=T0, values=h1))
    return history


class TestSignalContract:
    def test_score_exactly_equals_consensus_raw_score(self) -> None:
        from crypto_signal_engine.agents.context import build_agent_context
        from crypto_signal_engine.agents.market_structure import MarketStructureAgent
        from crypto_signal_engine.agents.order_book import OrderBookAgent
        from crypto_signal_engine.agents.quant import QuantAgent
        from crypto_signal_engine.agents.regime import RegimeAgent
        from crypto_signal_engine.consensus.engine import ConsensusEngine

        history = make_history(m5_overrides={"RSI_14": 65.0, "ROC_10": 1.5})
        context = build_agent_context(history, "BTCUSDT", T0)
        evidence = (
            QuantAgent().evaluate(context), MarketStructureAgent().evaluate(context),
            OrderBookAgent().evaluate(context), RegimeAgent().evaluate(context).evidence,
        )
        regime = RegimeAgent().evaluate(context).regime
        consensus = ConsensusEngine().combine(evidence, regime=regime)

        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.score == consensus.raw_score

    def test_direction_derived_from_score(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 80.0, "ROC_10": 2.5, "VWAP_DEVIATION_20": 0.015})
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        from crypto_signal_engine.domain.enums import SignalDirection

        assert signal.direction == SignalDirection.from_score(signal.score)

    def test_confidence_formula_exact(self) -> None:
        from crypto_signal_engine.agents.context import build_agent_context
        from crypto_signal_engine.agents.market_structure import MarketStructureAgent
        from crypto_signal_engine.agents.order_book import OrderBookAgent
        from crypto_signal_engine.agents.quant import QuantAgent
        from crypto_signal_engine.agents.regime import RegimeAgent
        from crypto_signal_engine.consensus.engine import ConsensusEngine
        from crypto_signal_engine.consensus.risk import RiskOverlay

        history = make_history(m5_overrides={"RSI_14": 65.0, "ROC_10": 1.5})
        context = build_agent_context(history, "BTCUSDT", T0)
        evidence = (
            QuantAgent().evaluate(context), MarketStructureAgent().evaluate(context),
            OrderBookAgent().evaluate(context), RegimeAgent().evaluate(context).evidence,
        )
        regime_output = RegimeAgent().evaluate(context)
        consensus = ConsensusEngine().combine(evidence, regime=regime_output.regime)
        risk = RiskOverlay().assess(consensus, regime_output.regime)
        expected_confidence = abs(consensus.raw_score) * consensus.agreement * risk.confidence_multiplier

        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.confidence == pytest.approx(expected_confidence)

    def test_risk_cannot_increase_confidence_beyond_base(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 65.0, "ROC_10": 1.5})
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        base_confidence_upper_bound = abs(signal.score) * 1.0
        assert signal.confidence <= base_confidence_upper_bound + 1e-9

    def test_correct_symbol_context_timestamp_timeframe(self) -> None:
        history = make_history()
        signal = SignalEngine(history).evaluate("btcusdt", T0)
        assert signal.symbol == "BTCUSDT"
        assert signal.timestamp == T0
        assert signal.primary_timeframe == Timeframe.M5
        assert len(signal.context_id) > 0

    def test_no_duplicate_agents_across_factor_collections(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 80.0, "ROC_10": 2.5})
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        all_agents = [e.agent for e in signal.supporting_factors] + [e.agent for e in signal.contradicting_factors]
        assert len(all_agents) == len(set(all_agents))

    def test_no_evidence_in_both_collections(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 80.0, "ROC_10": 2.5})
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        overlap = [e for e in signal.supporting_factors if e in signal.contradicting_factors]
        assert overlap == []

    def test_neutral_consensus_does_not_receive_falsely_high_confidence(self) -> None:
        history = make_history()
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.confidence == pytest.approx(abs(signal.score) * 1.0, abs=0.05) or signal.confidence < 0.1

    def test_model_version_populated(self) -> None:
        history = make_history()
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.model_version == "phase4-v1"

    def test_no_future_evidence(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 70.0, "ROC_10": 2.0})
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        all_evidence = list(signal.supporting_factors) + list(signal.contradicting_factors)
        assert all(e.as_of <= signal.timestamp for e in all_evidence)

    def test_deterministic_factor_ordering(self) -> None:
        history = make_history(m5_overrides={"RSI_14": 70.0, "ROC_10": 2.0})
        s1 = SignalEngine(history).evaluate("BTCUSDT", T0)
        s2 = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert [e.agent for e in s1.supporting_factors] == [e.agent for e in s2.supporting_factors]
        assert [e.agent for e in s1.contradicting_factors] == [e.agent for e in s2.contradicting_factors]


=== FILE: tests/test_signal_testnet_bridge.py ===
"""Faz 13 — `crypto_signal_engine.execution.signal_bridge` testleri.

Tamamen offline — `FakeTestnetHttpClient` + `FixedClock` + temp SQLite
(Faz 10/11 testlerinin AYNI disiplini, bkz. `tests/test_execution_
reconciliation_service.py`). Bu dosya `SignalTestnetBridge`'in POLİTİKA
motorunu (BUY/SELL/NO_ACTION kararları, bridge-owned envanter yeniden
inşası, idempotency/ambiguity/fail-closed davranışı) doğrudan test eder —
gerçek `SignalEngine`/Quant/Consensus zincirini TETİKLEMEZ (bu dosyanın
kapsamı DIŞINDadır; bkz. `RuntimeCycleResult`/`Signal` doğrudan inşa
edilir, tıpkı `test_paper_trading_engine.py`'nin KENDİ Signal fikstürleri
gibi)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, SignalDirection, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.execution.errors import ExecutionTransportError
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import (
    SignalTestnetBridge,
    bridge_context_id,
    compute_bridge_position,
)
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


def _exchange_info(symbol: str = SYMBOL, *, step_size: str = "0.0001", min_qty: str = "0.0001") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": min_qty, "maxQty": "9000.0", "stepSize": step_size},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_response(*, side: str, qty: str, quote_qty: str = "0", order_id: int = 1, status: str = "FILLED") -> tuple:
    payload = {
        "symbol": SYMBOL, "clientOrderId": "csl-abc", "orderId": order_id, "side": side,
        "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }
    return json_response(payload)


def _bridge(get_responses=None, post_responses=None, *, db_path: Path, notional: float = 10.0, entries_paused_provider=None):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    store = ExecutionStateStore(db_path)
    service = ExecutionReconciliationService(client, store, clock=FixedClock(NOW))
    bridge = SignalTestnetBridge(
        execution_service=service, execution_store=store, notional_usdt=notional, clock=FixedClock(NOW),
        entries_paused_provider=entries_paused_provider,
    )
    return bridge, http, store


def _signal(*, score: float, context_id: str, symbol: str = SYMBOL, ts: datetime = NOW) -> Signal:
    return Signal(
        symbol=symbol, timestamp=ts, context_id=context_id, score=score, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test-bridge",
    )


def _cycle_result(signal: Signal, *, idempotent_replay: bool = False) -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=signal.symbol,
        position=PaperPosition(
            symbol=signal.symbol, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0,
            realized_pnl=0.0, updated_at=signal.timestamp,
        ),
        orders=(), fills=(), idempotent_replay=idempotent_replay,
    )
    return RuntimeCycleResult(
        symbol=signal.symbol, evaluated=True, signal=signal, paper_result=paper_result, generated_at=signal.timestamp
    )


LONG_SCORE = 0.9
SHORT_SCORE = -0.9
NEUTRAL_SCORE = 0.0


class TestOperationalGate:
    def test_not_operational_produces_no_action_and_no_post(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        signal = _signal(score=LONG_SCORE, context_id="ctx-1")
        run_async(bridge.on_cycle_result(_cycle_result(signal)))
        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"
        assert "not operational" in status["last_action_detail"]

    def test_becomes_operational_only_after_explicit_mark(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        assert bridge.operational is False
        bridge.mark_operational(True, "test")
        assert bridge.operational is True
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert len(http.post_calls) == 1


class TestSpotLongOnlyPolicy:
    def test_new_long_while_flat_buys_exactly_once(self, tmp_path: Path) -> None:
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge.mark_operational(True, "test")
        signal = _signal(score=LONG_SCORE, context_id="ctx-1")
        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert len(http.post_calls) == 1
        params = http.post_calls[0][1]
        assert params["side"] == "BUY"
        assert params["quoteOrderQty"] == "10"

        from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType

        expected_intent = OrderIntent(
            symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
            context_id=bridge_context_id(SYMBOL, "OPEN", signal.context_id), timestamp=NOW, quote_quantity=10.0,
        )
        assert params["newClientOrderId"] == expected_intent.client_order_id

        position = compute_bridge_position(store, SYMBOL)
        assert position.owned_quantity == pytest.approx(0.0001)
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "BUY"
        assert status["ambiguous"] is False
        # Regression: the status snapshot must reflect the FRESH owned
        # quantity immediately after a successful submit, not a stale
        # None/prior value (bkz. `_submit()`'in submit-sonrası tazeleme
        # yorumu).
        assert status["owned_quantity"] == pytest.approx(0.0001)

    def test_duplicate_same_context_produces_no_second_post(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge.mark_operational(True, "test")
        signal = _signal(score=LONG_SCORE, context_id="ctx-dup")
        run_async(bridge.on_cycle_result(_cycle_result(signal)))
        run_async(bridge.on_cycle_result(_cycle_result(signal)))
        assert len(http.post_calls) == 1

    def test_concurrent_duplicate_delivery_creates_one_order(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge.mark_operational(True, "test")
        signal = _signal(score=LONG_SCORE, context_id="ctx-concurrent")
        cycle_result = _cycle_result(signal)

        async def body() -> None:
            await asyncio.gather(bridge.on_cycle_result(cycle_result), bridge.on_cycle_result(cycle_result))

        run_async(body())
        assert len(http.post_calls) == 1

    def test_idempotent_paper_replay_never_reaches_submit(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        bridge.mark_operational(True, "test")
        signal = _signal(score=LONG_SCORE, context_id="ctx-replay")
        run_async(bridge.on_cycle_result(_cycle_result(signal, idempotent_replay=True)))
        assert http.post_calls == []
        assert bridge.status_for(SYMBOL)["last_action_detail"].startswith("idempotent paper replay")

    def test_same_direction_long_while_already_long_is_no_action(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert len(http.post_calls) == 1

        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-2"))))
        assert len(http.post_calls) == 1  # no churn, no second POST
        assert bridge.status_for(SYMBOL)["last_action"] == "NO_ACTION"

    def test_neutral_is_no_action(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=NEUTRAL_SCORE, context_id="ctx-1"))))
        assert http.post_calls == []
        assert bridge.status_for(SYMBOL)["last_action"] == "NO_ACTION"

    def test_short_while_flat_is_no_action_no_synthetic_short(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=SHORT_SCORE, context_id="ctx-1"))))
        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"
        assert "synthetic" in status["last_action_detail"]

    def test_short_while_long_sells_exact_bridge_owned_quantity(self, tmp_path: Path) -> None:
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[
                _order_response(side="BUY", qty="0.0001", quote_qty="10.0"),
                _order_response(side="SELL", qty="0.0001", quote_qty="10.0"),
            ],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-open"))))
        owned_after_buy = compute_bridge_position(store, SYMBOL).owned_quantity
        assert owned_after_buy == pytest.approx(0.0001)

        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=SHORT_SCORE, context_id="ctx-close"))))
        assert len(http.post_calls) == 2
        sell_params = http.post_calls[1][1]
        assert sell_params["side"] == "SELL"
        assert sell_params["quantity"] == "0.0001"

        owned_after_sell = compute_bridge_position(store, SYMBOL).owned_quantity
        assert owned_after_sell == pytest.approx(0.0)
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "SELL"
        assert status["owned_quantity"] == pytest.approx(0.0)

    def test_sell_quantity_is_floored_to_step_size(self, tmp_path: Path) -> None:
        """0.0015 gibi step'e hizasız bir bridge-owned miktar, 0.001
        step_size ile 0.001'e AŞAĞI hizalanır — asla owned miktarı AŞMAZ."""
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[
                json_response(_exchange_info(step_size="0.0001", min_qty="0.0001")),
                json_response(_exchange_info(step_size="0.001", min_qty="0.001")),
            ],
            post_responses=[
                _order_response(side="BUY", qty="0.0015", quote_qty="10.0"),
                _order_response(side="SELL", qty="0.001", quote_qty="10.0"),
            ],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-open"))))
        assert compute_bridge_position(store, SYMBOL).owned_quantity == pytest.approx(0.0015)

        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=SHORT_SCORE, context_id="ctx-close"))))
        sell_params = http.post_calls[1][1]
        assert sell_params["quantity"] == "0.001"
        # 0.0005 dust kalır — SATILAMAZ (step'e hizasız), envanter bunu
        # doğru şekilde yansıtır (>= 0 invariant korunur).
        assert compute_bridge_position(store, SYMBOL).owned_quantity == pytest.approx(0.0005)


class TestBridgeOwnedInventoryIsolation:
    def test_manual_lab_records_are_never_counted_as_bridge_inventory(self, tmp_path: Path) -> None:
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        # Manuel `scripts/binance_testnet_lab.py` benzeri, bridge namespace'i
        # DIŞINDA bir manuel kayıt (örn. hesapta zaten 1 BTC'lik manuel
        # aktivite) — bridge_context_id() prefix'i TAŞIMAZ.
        from datetime import datetime as _dt

        from crypto_signal_engine.execution.models import OrderSide, OrderType
        from crypto_signal_engine.execution.reconciliation_models import ExecutionRecord, ExecutionLifecycleState

        manual_record = ExecutionRecord(
            context_id="manual-lab-ctx", symbol=SYMBOL, client_order_id="csl-manual",
            side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=1.0, quote_quantity=None, price=None,
            lifecycle_state=ExecutionLifecycleState.FILLED, exchange_order_id=999, executed_quantity=1.0,
            cumulative_quote_quantity=50000.0, detail="manual lab BUY", created_at=NOW, updated_at=NOW,
            last_reconciled_at=NOW,
        )
        store.save(manual_record)

        position_before = compute_bridge_position(store, SYMBOL)
        assert position_before.owned_quantity == 0.0  # the manual 1.0 BTC is NOT bridge-owned

        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        # Only the bridge's own tiny BUY counts.
        assert compute_bridge_position(store, SYMBOL).owned_quantity == pytest.approx(0.0001)

    def test_restart_restores_bridge_owned_quantity(self, tmp_path: Path) -> None:
        db_path = tmp_path / "e.db"
        bridge1, http1, store1 = _bridge(
            db_path=db_path,
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge1.mark_operational(True, "test")
        run_async(bridge1.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        store1.close()

        # "restart": a brand-new store/service/bridge instance over the SAME db file.
        store2 = ExecutionStateStore(db_path)
        position = compute_bridge_position(store2, SYMBOL)
        assert position.owned_quantity == pytest.approx(0.0001)
        assert position.ambiguous is False
        store2.close()

        # Regression: a brand-new bridge (fresh process, empty in-memory
        # status cache) must show the DURABLE owned quantity via
        # `status_for()` BEFORE any new signal has been observed in this
        # process — restart-recovered inventory is not hidden behind "no
        # signal yet".
        bridge2, _, store3 = _bridge(db_path=db_path, notional=10.0)
        status = bridge2.status_for(SYMBOL)
        assert status["last_action"] == "NONE"
        assert status["owned_quantity"] == pytest.approx(0.0001)
        store3.close()

    def test_multi_symbol_isolation(self, tmp_path: Path) -> None:
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info("BTCUSDT")), json_response(_exchange_info("ETHUSDT"))],
            post_responses=[
                _order_response(side="BUY", qty="0.0001", quote_qty="10.0"),
            ],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, symbol="BTCUSDT", context_id="c1"))))
        eth_position = compute_bridge_position(store, "ETHUSDT")
        assert eth_position.owned_quantity == 0.0
        assert eth_position.ambiguous is False
        btc_position = compute_bridge_position(store, "BTCUSDT")
        assert btc_position.owned_quantity == pytest.approx(0.0001)


class TestAmbiguityAndFailClosed:
    def test_ambiguous_submit_blocks_further_economic_action(self, tmp_path: Path) -> None:
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info()), ExecutionTransportError("query failed")],
            post_responses=[ExecutionTransportError("post failed")],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))

        position = compute_bridge_position(store, SYMBOL)
        assert position.ambiguous is True

        # A second, DIFFERENT new signal context must NOT create a second
        # (conflicting) economic action while ambiguity is unresolved.
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-2"))))
        assert len(http.post_calls) == 1  # no second POST attempted
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"
        assert status["ambiguous"] is True

    def test_execution_ready_false_after_startup_failure_blocks_orders(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        bridge.mark_operational(False, "startup reconciliation FAILED")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert http.post_calls == []

    def test_no_secrets_in_status_or_repr(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(db_path=tmp_path / "e.db")
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=NEUTRAL_SCORE, context_id="ctx-1"))))
        status = bridge.status_for(SYMBOL)
        blob = repr(status) + repr(bridge.__dict__)
        assert FAKE_SECRET not in blob
        assert FAKE_KEY not in blob


class TestEntriesPausedGate:
    """24/7 Ops v1, Step 3 — required test: pause blocks a new-entry
    evaluation deterministically, while an already-open position's
    management code path is provably UNTOUCHED (a SHORT signal against
    an existing bridge-owned LONG still sells, even while paused)."""

    def test_new_long_blocked_while_paused(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
            entries_paused_provider=lambda: True,
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"

    def test_new_long_still_works_when_not_paused(self, tmp_path: Path) -> None:
        """Regression: `entries_paused_provider` returning `False` (the
        never-paused state) is bit-for-bit identical to `None`."""
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
            entries_paused_provider=lambda: False,
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert len(http.post_calls) == 1

    def test_default_none_provider_never_pauses(self, tmp_path: Path) -> None:
        bridge, http, _ = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[_order_response(side="BUY", qty="0.0001", quote_qty="10.0")],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-1"))))
        assert len(http.post_calls) == 1

    def test_existing_open_position_management_untouched_while_paused(self, tmp_path: Path) -> None:
        """The pause gate is checked ONLY on the new-LONG-entry code path.
        An already bridge-owned LONG position's opposite-signal exit
        (SELL) must go through unaffected, even while paused=True."""
        pause_state = {"paused": False}
        bridge, http, store = _bridge(
            db_path=tmp_path / "e.db",
            get_responses=[json_response(_exchange_info())],
            post_responses=[
                _order_response(side="BUY", qty="0.0001", quote_qty="10.0"),
                _order_response(side="SELL", qty="0.0001", quote_qty="10.0"),
            ],
            entries_paused_provider=lambda: pause_state["paused"],
        )
        bridge.mark_operational(True, "test")
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=LONG_SCORE, context_id="ctx-open"))))
        assert compute_bridge_position(store, SYMBOL).owned_quantity == pytest.approx(0.0001)

        # Now pause new entries AFTER the position is already open.
        pause_state["paused"] = True
        run_async(bridge.on_cycle_result(_cycle_result(_signal(score=SHORT_SCORE, context_id="ctx-close"))))
        assert len(http.post_calls) == 2
        sell_params = http.post_calls[1][1]
        assert sell_params["side"] == "SELL"
        assert compute_bridge_position(store, SYMBOL).owned_quantity == pytest.approx(0.0)


=== FILE: tests/test_signal_testnet_bridge_lifecycle_integration.py ===
"""
Autonomous Testnet trading lifecycle — integration tests proving
`SignalTestnetBridge` correctly delegates to a wired `LifecycleManager`:
entry risk gates, fee-aware LONG/FLAT decisions, `on_entry_filled` after a
BUY, and opposite-signal exits routed through the SAME shared per-symbol
lock the M1 evaluator uses (Phase 5/6/7/10)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, PositionLifecycleState, RiskPolicyConfig
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


def _exchange_info(step_size: str = "0.0001", min_qty: str = "0.0001") -> dict:
    return {
        "symbols": [
            {
                "symbol": SYMBOL, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": min_qty, "maxQty": "9000.0", "stepSize": step_size},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int = 1, status: str = "FILLED") -> dict:
    return {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }


def _setup(get_responses=None, post_responses=None, *, tmp_path: Path, risk_policy=None, atr=500.0):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    manager = LifecycleManager(store=lifecycle_store, execution_service=service, risk_policy=risk_policy, clock=FixedClock(NOW))
    bridge = SignalTestnetBridge(
        execution_service=service, execution_store=exec_store, notional_usdt=10.0, clock=FixedClock(NOW),
        lifecycle_manager=manager, atr_provider=lambda _symbol: atr, all_symbols_provider=lambda: (SYMBOL,),
    )
    bridge.mark_operational(True, "test")
    return bridge, manager, http, lifecycle_store


def _signal(*, score: float, context_id: str, ts: datetime = NOW) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=ts, context_id=context_id, score=score, confidence=0.9,
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


LONG_SCORE = 0.9
SHORT_SCORE = -0.9


class TestSharedLock:
    def test_bridge_lock_is_the_same_object_as_lifecycle_manager_lock(self, tmp_path: Path) -> None:
        bridge, manager, _, _ = _setup(tmp_path=tmp_path)
        assert bridge._lock_for(SYMBOL) is manager.lock_for(SYMBOL)


class TestEntryWithLifecycle:
    def test_buy_initializes_durable_long_position(self, tmp_path: Path) -> None:
        # place_order is a POST (BUY intent uses quote_quantity, so no
        # additional price-check GET is needed); my_trades backfill is a
        # separate GET call after the fill (`ExecutionRecord` never carries
        # `fills[]`, see `LifecycleManager._resolve_fills`).
        bridge, manager, http, store = _setup(
            get_responses=[json_response(_exchange_info()), json_response([
                {"symbol": SYMBOL, "id": 1, "orderId": 1, "price": "50000.0", "qty": "0.002",
                 "commission": "0.000002", "commissionAsset": "BTC"},
            ])],
            post_responses=[json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))],
            tmp_path=tmp_path,
        )
        signal = _signal(score=LONG_SCORE, context_id="ctx-1")

        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        position = store.load_position(SYMBOL)
        assert position.state is PositionLifecycleState.LONG
        assert position.gross_entry_vwap == pytest.approx(50000.0)
        assert position.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        assert position.initial_protective_stop == pytest.approx(50000.0 - 2.0 * 500.0)
        assert position.entry_signal_context_id == "ctx-1"

        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "BUY"

    def test_entry_gate_blocks_buy_no_post_sent(self, tmp_path: Path) -> None:
        bridge, manager, http, store = _setup(
            get_responses=[json_response(_exchange_info())], tmp_path=tmp_path,
            risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
        )
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-20.0, trades_counted=1),
            now=NOW,
        )
        signal = _signal(score=LONG_SCORE, context_id="ctx-1")

        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"
        assert "entry risk gate blocked" in status["last_action_detail"]

    def test_already_long_per_lifecycle_state_blocks_duplicate_buy(self, tmp_path: Path) -> None:
        bridge, manager, http, store = _setup(tmp_path=tmp_path)
        store.save_position(BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        signal = _signal(score=LONG_SCORE, context_id="ctx-1")

        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert "already bridge-owned LONG" in status["last_action_detail"]


class TestMissingLifecycleRecordFailsClosed:
    def test_owned_inventory_without_lifecycle_record_blocks_new_buy(self, tmp_path: Path) -> None:
        """Simulates an unmigrated legacy position: a real FILLED bridge
        BUY execution record exists (raw truth shows owned inventory) but
        no `bridge_position` row exists yet — must NEVER be treated as
        FLAT (which would allow a duplicate BUY / pyramiding)."""
        bridge, manager, http, store = _setup(
            get_responses=[json_response(_exchange_info())],
            post_responses=[json_response(_order_payload(side="BUY", qty="0.0002", quote_qty="10.0"))],
            tmp_path=tmp_path,
        )

        from crypto_signal_engine.execution.signal_bridge import bridge_context_id
        from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType

        async def seed_real_buy():
            intent = OrderIntent(
                symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id=bridge_context_id(SYMBOL, "OPEN", "ctx-legacy"), timestamp=NOW, quote_quantity=10.0,
            )
            return await manager._service.submit(intent)

        run_async(seed_real_buy())
        assert store.load_position(SYMBOL) is None  # no lifecycle record -- unmigrated

        signal = _signal(score=LONG_SCORE, context_id="ctx-new")
        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert len(http.post_calls) == 1  # only the seed BUY, no second BUY submitted
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"
        assert "fail-closed" in status["last_action_detail"]
        assert status["ambiguous"] is True


class TestOppositeSignalExitWithLifecycle:
    def test_short_while_long_routes_through_lifecycle_manager(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 2, "orderId": 2, "price": "110.0", "qty": "1.0",
                 "commission": "0.11", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0", order_id=2))]
        bridge, manager, http, store = _setup(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        store.save_position(BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", updated_at=NOW,
        ))
        signal = _signal(score=SHORT_SCORE, context_id="ctx-2")

        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        trades = store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1
        assert trades[0]["exit_reason"] == "OPPOSITE_SIGNAL"
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "SELL"

    def test_short_while_flat_per_lifecycle_state_is_no_action(self, tmp_path: Path) -> None:
        bridge, manager, http, store = _setup(tmp_path=tmp_path)
        signal = _signal(score=SHORT_SCORE, context_id="ctx-2")

        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert http.post_calls == []
        status = bridge.status_for(SYMBOL)
        assert "bridge is FLAT" in status["last_action_detail"]


class TestBuyPersistenceAmbiguityPinning:
    """H3 fix (mainnet-readiness review, "phantom BUY"): when `submit()`
    raises `ExecutionPersistenceError` with `exchange_may_have_accepted_
    order=True` for a BUY (the exchange accepted/possibly filled a real
    order but local persistence failed), the bridge must NOT leave this
    symbol looking FLAT — that would let the very next signal cycle
    submit ANOTHER real BUY on top of a possibly-already-filled one, with
    the phantom half never getting a stop-loss. It must pin the symbol
    AMBIGUOUS instead, which `blocks_new_long_entry` already refuses to
    treat as tradeable."""

    def _setup_with_flaky_exec_store(self, tmp_path: Path, *, get_responses, post_responses, atr=500.0):
        from tests.test_execution_reconciliation_service import _FlakyExecutionStore

        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = _FlakyExecutionStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        bridge = SignalTestnetBridge(
            execution_service=service, execution_store=exec_store, notional_usdt=10.0, clock=FixedClock(NOW),
            lifecycle_manager=manager, atr_provider=lambda _symbol: atr, all_symbols_provider=lambda: (SYMBOL,),
        )
        bridge.mark_operational(True, "test")
        return bridge, manager, http, exec_store, lifecycle_store

    def test_post_ack_persistence_failure_pins_ambiguous_not_silent_flat(self, tmp_path: Path) -> None:
        bridge, manager, http, exec_store, store = self._setup_with_flaky_exec_store(
            tmp_path,
            get_responses=[json_response(_exchange_info())],
            post_responses=[json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))],
        )

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

        signal = _signal(score=LONG_SCORE, context_id="ctx-1")
        run_async(bridge.on_cycle_result(_cycle_result(signal)))

        assert len(http.post_calls) == 1  # the BUY really was sent to the exchange
        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.AMBIGUOUS

        # A second signal cycle (e.g. the next opportunity scan) must NOT
        # submit another BUY -- AMBIGUOUS blocks new entries outright.
        signal2 = _signal(score=LONG_SCORE, context_id="ctx-2")
        run_async(bridge.on_cycle_result(_cycle_result(signal2)))
        assert len(http.post_calls) == 1  # still just the one real POST, no duplicate
        status = bridge.status_for(SYMBOL)
        assert status["last_action"] == "NO_ACTION"


=== FILE: tests/test_sqlite_store.py ===
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.domain.state_contract import CommitOutcome
from crypto_signal_engine.persistence.sqlite_store import SqliteCandleStateStore
from crypto_signal_engine.quality.base import DataQualityResult
from crypto_signal_engine.errors import PersistenceError

UTC = timezone.utc
OPEN_TIME = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def make_update(open_time=OPEN_TIME, close=100.0, is_closed=False, seq=1) -> CandleUpdate:
    candle = Candle(
        symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=100.0, high=max(105.0, close), low=min(95.0, close), close=close, volume=10.0,
        is_closed=is_closed, trade_count=5,
    )
    return CandleUpdate(candle=candle, update_seq=seq, event_time=candle.close_time, received_at=candle.close_time)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "test.db")


class TestSchemaCreation:
    def test_schema_created_on_first_open(self, db_path: str) -> None:
        store = SqliteCandleStateStore(db_path)
        conn = sqlite3.connect(db_path)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "candles" in tables
        assert "schema_version" in tables
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        assert version == 1
        conn.close()
        store.close()

    def test_reopen_does_not_duplicate_schema_version_row(self, db_path: str) -> None:
        SqliteCandleStateStore(db_path).close()
        SqliteCandleStateStore(db_path).close()
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
        assert count == 1
        conn.close()


class TestCommitAndPeek:
    def test_valid_commit_persists(self, db_path: str) -> None:
        store = SqliteCandleStateStore(db_path)
        update = make_update()
        result = store.commit(DataQualityResult.ok(), update)
        assert result.outcome == CommitOutcome.COMMITTED
        assert store.peek_previous(update.identity).close == 100.0
        store.close()

    def test_rejected_quality_never_persisted(self, db_path: str) -> None:
        store = SqliteCandleStateStore(db_path)
        bad_result = DataQualityResult(status=DataQualityStatus.EXTREME_OUTLIER, reason="test")
        update = make_update()
        result = store.commit(bad_result, update)
        assert result.outcome == CommitOutcome.REJECTED_QUALITY
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
        assert count == 0
        conn.close()
        store.close()

    def test_duplicate_open_time_upserts_not_duplicates_row(self, db_path: str) -> None:
        store = SqliteCandleStateStore(db_path)
        store.commit(DataQualityResult.ok(), make_update(close=100.0, seq=1))
        store.commit(DataQualityResult.ok(), make_update(close=101.0, seq=2))
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT close FROM candles").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 101.0  # son değer upsert edilmiş
        conn.close()
        store.close()

    def test_close_time_not_used_as_uniqueness_key(self, db_path: str) -> None:
        """Aynı open_time, farklı close_time (final update) hâlâ tek satır olmalı."""
        store = SqliteCandleStateStore(db_path)
        store.commit(DataQualityResult.ok(), make_update(is_closed=False, seq=1))
        final_update = make_update(is_closed=True, seq=2)
        store.commit(DataQualityResult.ok(), final_update)
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
        assert count == 1
        conn.close()
        store.close()

    def test_sequence_rejection_not_persisted(self, db_path: str) -> None:
        store = SqliteCandleStateStore(db_path)
        store.commit(DataQualityResult.ok(), make_update(seq=5, close=100.0))
        out_of_order = store.commit(DataQualityResult.ok(), make_update(seq=2, close=999.0))
        assert out_of_order.outcome == CommitOutcome.REJECTED_SEQUENCE
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT close FROM candles").fetchone()
        assert row[0] == 100.0
        conn.close()
        store.close()


class TestRestartRecovery:
    def test_state_survives_restart(self, db_path: str) -> None:
        store1 = SqliteCandleStateStore(db_path)
        update = make_update(close=123.0, is_closed=True)
        store1.commit(DataQualityResult.ok(), update)
        store1.close()

        store2 = SqliteCandleStateStore(db_path)
        restored = store2.peek_previous(update.identity)
        assert restored is not None
        assert restored.close == 123.0
        store2.close()

    def test_sequencing_continues_correctly_after_restart(self, db_path: str) -> None:
        store1 = SqliteCandleStateStore(db_path)
        update = make_update(seq=10, close=100.0)
        store1.commit(DataQualityResult.ok(), update)
        store1.close()

        store2 = SqliteCandleStateStore(db_path)
        # Restart sonrası daha düşük seq'li bir update hâlâ out-of-order kabul edilmeli.
        stale = make_update(seq=5, close=999.0)
        result = store2.commit(DataQualityResult.ok(), stale)
        assert result.outcome == CommitOutcome.REJECTED_SEQUENCE
        store2.close()


class TestAtomicityOnPersistFailure:
    """Reviewer probe C: SQLite persist failure → PersistenceError →
    canonical sequencer state UNCHANGED."""

    def test_persist_failure_leaves_sequencer_state_unchanged(self, db_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        store = SqliteCandleStateStore(db_path)
        # Önce geçerli bir state kuruyoruz.
        first = make_update(seq=1, close=100.0)
        store.commit(DataQualityResult.ok(), first)
        assert store.peek_previous(first.identity).close == 100.0

        # _persist'i her zaman sqlite3.Error fırlatacak şekilde bozuyoruz.
        def _boom(self, update):
            raise sqlite3.OperationalError("simulated disk failure")

        monkeypatch.setattr(SqliteCandleStateStore, "_persist", _boom)

        second = make_update(seq=2, close=999.0, open_time=OPEN_TIME + timedelta(minutes=1))
        with pytest.raises(sqlite3.OperationalError):
            store.commit(DataQualityResult.ok(), second)

        # KRİTİK: canonical sequencer state DEĞİŞMEMİŞ olmalı — ikinci
        # candle'ın identity'si için hiçbir şey commit edilmemiş olmalı.
        assert store.peek_previous(second.identity) is None
        # İlk candle'ın state'i de bozulmamış olmalı.
        assert store.peek_previous(first.identity).close == 100.0
        store.close()

    def test_persist_persistence_error_leaves_sequencer_state_unchanged(self, db_path: str) -> None:
        """Gerçek `PersistenceError` (mock değil) ile aynı invariant."""
        store = SqliteCandleStateStore(db_path)
        first = make_update(seq=1, close=100.0)
        store.commit(DataQualityResult.ok(), first)
        store.close()  # bağlantıyı kapatıyoruz — sonraki persist denemesi gerçekten başarısız olacak

        second = make_update(seq=2, close=999.0, open_time=OPEN_TIME + timedelta(minutes=1))
        with pytest.raises(PersistenceError):
            store.commit(DataQualityResult.ok(), second)

        # Bağlantı kapalı olduğu için peek_previous bile in-memory sequencer
        # üzerinden çalışır (SQLite'a dokunmaz) — ikinci candle commit
        # EDİLMEMİŞ olmalı.
        assert store.peek_previous(second.identity) is None
        assert store.peek_previous(first.identity).close == 100.0

    def test_persist_failure_does_not_advance_sequence_for_retry(self, db_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """Persist başarısız olduktan SONRA aynı update tekrar (bu kez
        başarıyla) denendiğinde hâlâ kabul edilebilmelidir — çünkü
        başarısız deneme sequencer'ı hiç ilerletmemiştir."""
        store = SqliteCandleStateStore(db_path)
        update = make_update(seq=5, close=100.0)

        call_count = {"n": 0}
        original_persist = SqliteCandleStateStore._persist

        def _fail_once(self, u):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise sqlite3.OperationalError("simulated transient failure")
            return original_persist(self, u)

        monkeypatch.setattr(SqliteCandleStateStore, "_persist", _fail_once)

        with pytest.raises(sqlite3.OperationalError):
            store.commit(DataQualityResult.ok(), update)
        assert store.peek_previous(update.identity) is None

        # Retry: aynı update artık başarıyla commit edilmeli (sequencer hâlâ
        # bu identity'yi hiç görmemiş durumda olduğundan duplicate/out-of-
        # order reddi OLMAMALI).
        retry_result = store.commit(DataQualityResult.ok(), update)
        assert retry_result.outcome == CommitOutcome.COMMITTED
        assert store.peek_previous(update.identity).close == 100.0
        store.close()
    def test_close_then_operations_fail_explicitly(self, db_path: str) -> None:
        """sqlite3.Connection.close() Python'da idempotenttir (ikinci close()
        hata fırlatmaz); ancak kapatıldıktan SONRA bir işlem denemek açık bir
        `sqlite3.ProgrammingError` fırlatmalıdır (silent failure yasak)."""
        store = SqliteCandleStateStore(db_path)
        store.close()
        store.close()  # idempotent, hata YOK
        with pytest.raises(PersistenceError):
            store.commit(DataQualityResult.ok(), make_update())


=== FILE: tests/test_stability_determinism.py ===
"""Faz 9 — determinism: AYNI hızlandırılmış senaryo, TAZE state'ten iki
kez çalıştırıldığında, işlemsel-olmayan metadata (PID, wall-clock
`generated_at`) HARİÇ, TAMAMEN aynı sonucu üretmelidir (pozisyon, order/
fill listesi, realized PnL, processed context'ler, persistence checkpoint
içeriği, final health)."""

from __future__ import annotations

import dataclasses

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.stability import scenarios
from crypto_signal_engine.stability.harness import SoakHarness
from tests.conftest import run_async

_M5 = Timeframe.M5


def _position_tuple(position):
    return (position.side, position.quantity, position.average_entry_price, position.realized_pnl)


def _fill_tuple(fill):
    return (fill.order_id, fill.side, fill.quantity, fill.price, fill.fee)


async def _run_deterministic_workload(db_path) -> dict[str, object]:
    h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=db_path, warmup_candles=20)
    await h.start()
    scenarios.run_multi_symbol_stress(h, ticks_per_symbol=25)
    outcome = await scenarios.run_gap_recovery(h, symbol="BTCUSDT", gap_size=3)
    await scenarios.run_process_restart_cycle(h, symbol="BTCUSDT", ticks_between=5)

    result: dict[str, object] = {"gap_outcome": outcome}
    for symbol in h.symbols:
        position = h.coordinator._paper_engine.position(symbol)
        fills = h.coordinator._paper_engine.fills(symbol)
        orders = h.coordinator._paper_engine.orders(symbol)
        state = h.store.load_paper_state(symbol)
        result[f"{symbol}:position"] = _position_tuple(position)
        result[f"{symbol}:fills"] = tuple(_fill_tuple(f) for f in fills)
        result[f"{symbol}:orders"] = tuple(o.order_id for o in orders)
        result[f"{symbol}:context_ids"] = tuple(sorted(state.processed_context_ids)) if state else ()
        result[f"{symbol}:checkpoint"] = h.store.load_candle_checkpoint(symbol, _M5)
    result["overall_health"] = h.coordinator.status().overall_health

    await h.stop_gracefully()
    return result


class TestDeterministicReplay:
    def test_identical_workload_from_fresh_state_produces_identical_results(self, tmp_path) -> None:
        async def scenario() -> None:
            result_a = await _run_deterministic_workload(tmp_path / "a.db")
            result_b = await _run_deterministic_workload(tmp_path / "b.db")
            assert result_a == result_b

        run_async(scenario())

    def test_deterministic_candle_factory_is_pure(self, tmp_path) -> None:
        from crypto_signal_engine.stability.harness import make_deterministic_candle
        from datetime import datetime, timezone

        t = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = make_deterministic_candle("BTCUSDT", _M5, t, 42)
        b = make_deterministic_candle("BTCUSDT", _M5, t, 42)
        assert dataclasses.astuple(a) == dataclasses.astuple(b)


=== FILE: tests/test_stability_harness.py ===
"""Faz 9 — `SoakHarness`'in kendisinin sağlığı: temel kompozisyon, bootstrap,
deterministik candle üretimi, restart/crash mekanikleri. Tamamen offline,
`FixedClock` ile hızlandırılmış — gerçek wall-clock sleep YOK."""

from __future__ import annotations

from datetime import timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from crypto_signal_engine.stability.harness import SoakHarness
from tests.conftest import run_async


class TestHarnessBootstrap:
    def test_start_reaches_ready_after_order_book_seed(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            reports = await h.start()
            assert all(r.ready for r in reports)
            assert h.coordinator.status().overall_health is RuntimeHealth.READY

        run_async(scenario())

    def test_multi_symbol_start_all_ready(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT", "BNBUSDT"), db_path=tmp_path / "s.db")
            await h.start()
            status = h.coordinator.status()
            assert all(s.health is RuntimeHealth.READY for s in status.symbols)

        run_async(scenario())

    def test_clock_advances_as_candles_are_generated(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            before = h.clock.now()
            h.ingest_candle("BTCUSDT", Timeframe.M5)
            after = h.clock.now()
            assert after > before

        run_async(scenario())


class TestHarnessDeterministicCandles:
    def test_volume_is_never_constant_across_a_series(self, tmp_path) -> None:
        """Faz 6 dersi: sabit volume, VOLUME_ZSCORE_20 gibi feature'larda
        FeatureCalculationError'a yol açar — bu regresyona karşı korunur."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            volumes = {h.next_candle("BTCUSDT", Timeframe.M5).volume for _ in range(8)}
            assert len(volumes) > 1

        run_async(scenario())

    def test_prices_stay_bounded_over_many_ticks(self, tmp_path) -> None:
        """Fiyat, binlerce tick boyunca absürt değerlere DRIFT ETMEMELİ
        (bounded osilasyon) — uzun soak koşuları gerçekçi kalmalı."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            prices = [h.next_candle("BTCUSDT", Timeframe.M5).close for _ in range(500)]
            assert max(prices) < 200.0
            assert min(prices) > 50.0

        run_async(scenario())

    def test_candles_are_never_nan_or_infinite(self, tmp_path) -> None:
        import math

        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            for _ in range(200):
                candle = h.next_candle("BTCUSDT", Timeframe.M5)
                for value in (candle.open, candle.high, candle.low, candle.close, candle.volume):
                    assert math.isfinite(value)

        run_async(scenario())


class TestHarnessRestartAndCrash:
    def test_restart_reuses_same_durable_store_path(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            h.ingest_candle("BTCUSDT", Timeframe.M5)
            await h.stop_gracefully()
            reports = await h.restart()
            assert h.db_path == tmp_path / "s.db"
            assert isinstance(reports, tuple)

        run_async(scenario())

    def test_crash_does_not_call_store_close_but_data_survives(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            event = h.ingest_candle("BTCUSDT", Timeframe.M5)
            assert event.outcome is IngestOutcome.ACCEPTED
            h.crash()
            assert h.store is None and h.coordinator is None and h.runtime is None
            await h.restart()
            checkpoint = h.store.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
            assert checkpoint is not None

        run_async(scenario())

    def test_advance_clock_moves_fixed_clock_forward(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            before = h.clock.now()
            h.advance_clock(timedelta(hours=2))
            assert h.clock.now() == before + timedelta(hours=2)

        run_async(scenario())


=== FILE: tests/test_stability_resource_growth.py ===
"""Faz 9 — kaynak/büyüme gözlemi: uzun (hızlandırılmış) bir çalışma
boyunca, kabul edilmiş runtime yapılarının BEKLENEN şekilde SINIRLI
(bounded) kaldığını doğrular. Bu testler "flaky exact-byte" eşikleri
KULLANMAZ — yapısal sınırları (maxlen, task sayısı) doğrular.

Kategori ayrımı (bkz. PHASE9_LONG_RUN_STABILITY.md):
A. Beklenen durable/tarihsel büyüme (fills/orders/processed_context_ids,
   SQLite satırları) — BİLEREK sınırlanmaz, Faz 5'in idempotency/ledger
   sözleşmesinin DOĞAL bir sonucudur (bkz. Decision — "do not redesign
   accounting merely to optimize memory").
B. Sınırlı runtime cache'leri (`CandleWindow` maxlen, `FeatureHistoryStore`
   deque maxlen, task listesi) — BU testler bunları doğrular.
C. Kazara/sınırsız operasyonel büyüme — bu testlerde HİÇBİRİ bulunmadı;
   bulunsaydı burada regresyon olarak raporlanırdı."""

from __future__ import annotations

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.stability import scenarios
from crypto_signal_engine.stability.harness import SoakHarness
from tests.conftest import run_async

_M5 = Timeframe.M5


class TestBoundedRuntimeCaches:
    def test_candle_window_never_exceeds_configured_maxlen(self, tmp_path) -> None:
        async def scenario() -> None:
            window_size = 50
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            # coordinator'ın kendi candle_window_size'ını override etmek için
            # yeniden kurulum yerine, varsayılan (500) ile 600 tick üreterek
            # "hiçbir zaman maxlen'i AŞMAZ" invariant'ını doğrudan doğrularız.
            scenarios.run_steady_state(h, ticks_per_symbol=600)
            for timeframe in h.timeframes:
                candle_window = h.coordinator._candle_windows[("BTCUSDT", timeframe)]
                assert len(candle_window.history()) <= 500  # RuntimeCoordinator varsayılan maxlen

        run_async(scenario())

    def test_feature_history_store_never_grows_unbounded(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=400)
            history_store = h.coordinator._history_store
            for key, deque_ in history_store._history.items():  # noqa: SLF001 (white-box soak inspection)
                assert deque_.maxlen is not None
                assert len(deque_) <= deque_.maxlen

        run_async(scenario())

    def test_task_count_does_not_grow_across_many_events(self, tmp_path) -> None:
        """`RuntimeCoordinator._tasks`, `run()` başına SABİT sayıda task
        oluşturur (event başına DEĞİL) — bu, event sayısı arttıkça task
        listesinin BÜYÜMEDİĞİNİ doğrular (yalnızca `run()`/`stop()`
        senaryolarında anlamlıdır; direct-call senaryolarında `_tasks`
        zaten boş kalır, bu da AYRI ve doğru bir invariant'tır)."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=100)
            assert h.coordinator._tasks == []  # direct-call path hiç task oluşturmaz

        run_async(scenario())


class TestExpectedDurableGrowth:
    """Kategori A: bu büyüme BEKLENİR, SINIRLANMAZ — burada yalnızca
    "makul ve deterministik" olduğunu (kontrolsüz/patlayan DEĞİL,
    event sayısıyla ORANTILI) belgeleriz."""

    def test_processed_context_ids_grow_proportionally_not_explosively(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=100)
            state = h.store.load_paper_state("BTCUSDT")
            if state is not None:
                # her context_id yalnızca BİR kez (gerçek bir mutation
                # başına) eklenir — event sayısını AŞAMAZ.
                assert len(state.processed_context_ids) <= h.metrics.candles_ingested

        run_async(scenario())

    def test_fill_and_order_ledgers_match_persisted_row_counts(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=100)
            in_memory_fills = h.coordinator._paper_engine.fills("BTCUSDT")
            state = h.store.load_paper_state("BTCUSDT")
            if state is not None:
                assert len(state.fills) == len(in_memory_fills)

        run_async(scenario())


=== FILE: tests/test_stability_scenarios.py ===
"""Faz 9 — 10 çekirdek soak senaryosu için odaklı, deterministik testler.
Her test `crypto_signal_engine.stability.scenarios`'ın bir sürücü
fonksiyonunu çağırır ve gerekli invariant'ları burada assert eder (bkz.
`scenarios.py` docstring'i — sürücü/assertion ayrımı kasıtlıdır).

Hiçbir test gerçek wall-clock sleep, gerçek Binance bağlantısı, veya
gerçek OS sinyali KULLANMAZ."""

from __future__ import annotations

import math
from datetime import timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from crypto_signal_engine.stability import scenarios
from crypto_signal_engine.stability.faults import FaultyPaperStateStore
from crypto_signal_engine.stability.harness import SoakHarness
from tests.conftest import run_async

_M5 = Timeframe.M5


def _faulty_store_factory(holder: dict):
    def factory(path):
        wrapped = FaultyPaperStateStore(PaperStateStore(path))
        holder["store"] = wrapped
        return wrapped

    return factory


class TestSteadyState:
    def test_extended_operation_produces_no_errors_and_stays_ready(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=80)

            status = h.coordinator.status()
            assert status.overall_health is RuntimeHealth.READY
            assert h.metrics.candles_duplicate == 0
            assert h.metrics.candles_out_of_order == 0
            assert h.metrics.candles_gap_detected == 0
            assert h.metrics.signals_evaluated > 0
            for symbol in h.symbols:
                position = h.coordinator._paper_engine.position(symbol)
                assert math.isfinite(position.quantity)
                assert math.isfinite(position.realized_pnl)

        run_async(scenario())

    def test_finite_accounting_throughout_a_long_run(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(
                symbols=("BTCUSDT",), db_path=tmp_path / "s.db", fee_bps=5.0, slippage_bps=2.0
            )
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=150)

            position = h.coordinator._paper_engine.position("BTCUSDT")
            assert math.isfinite(position.quantity)
            assert math.isfinite(position.average_entry_price)
            assert math.isfinite(position.realized_pnl)
            for fill in h.coordinator._paper_engine.fills("BTCUSDT"):
                assert math.isfinite(fill.fee)
                assert math.isfinite(fill.price)
                assert fill.fee >= 0.0

        run_async(scenario())


class TestReconnectStorm:
    def test_disconnected_symbol_is_degraded_healthy_symbol_is_isolated(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db")
            await h.start()
            run_task = await scenarios.run_reconnect_storm(
                h, disconnecting_symbol="BTCUSDT", healthy_symbol="ETHUSDT", events_per_symbol=4
            )
            status = h.coordinator.status()
            by_symbol = {s.symbol: s for s in status.symbols}
            assert by_symbol["BTCUSDT"].health is RuntimeHealth.DEGRADED
            assert by_symbol["BTCUSDT"].detail == "stream disconnected"
            assert by_symbol["ETHUSDT"].health is RuntimeHealth.READY
            assert not run_task.done()  # run() KENDİSİ çökmedi (gather absorbs it)

            await h.stop_gracefully()
            await run_task

        run_async(scenario())

    def test_no_duplicate_paper_transitions_on_healthy_symbol_during_storm(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db")
            await h.start()
            run_task = await scenarios.run_reconnect_storm(
                h, disconnecting_symbol="BTCUSDT", healthy_symbol="ETHUSDT", events_per_symbol=6
            )
            state = h.store.load_paper_state("ETHUSDT")
            if state is not None:
                # her context_id yalnızca bir kez processed_context'te olmalı (dict zaten garanti eder)
                assert len(state.processed_context_ids) == len(set(state.processed_context_ids))
            await h.stop_gracefully()
            await run_task

        run_async(scenario())


class TestGapRecovery:
    def test_gap_is_detected_then_recovered_and_continuity_restored(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            outcome = await scenarios.run_gap_recovery(h, symbol="BTCUSDT", gap_size=4)
            assert outcome is IngestOutcome.ACCEPTED
            assert h.metrics.candles_gap_detected == 1
            assert h.metrics.gap_recoveries_succeeded == 1
            assert h.coordinator.status().overall_health is RuntimeHealth.READY

        run_async(scenario())

    def test_unresolved_gap_recovery_leaves_symbol_degraded(self, tmp_path) -> None:
        """REST'in EKSİK aralığı tam dolduramadığı (hâlâ bir gap kalan)
        durumda, sembol DEGRADED kalmalı — sahte bir continuity asla
        üretilmemeli."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            # 2 candle atla ama sadece PENDING'i üret (REST kaynağı gap'i TAM
            # dolduramayacak şekilde, aradaki bir candle'ı history'den
            # sonradan gizlice çıkararak simüle edilir).
            skipped = [h.skip_candle("BTCUSDT", _M5) for _ in range(3)]
            pending = h.next_candle("BTCUSDT", _M5)
            # ortadaki candle'ı REST kaynağından (harness._history) kaldır ->
            # resolve_gap'in fetch'i eksik kalır.
            h._history[("BTCUSDT", _M5)].remove(skipped[1])

            event = h.ingest_candle("BTCUSDT", _M5, pending)
            assert event.outcome is IngestOutcome.GAP_DETECTED
            resolved = await h.coordinator.resolve_gap("BTCUSDT", _M5, pending)
            assert resolved.outcome is IngestOutcome.GAP_DETECTED

            status = h.coordinator.status()
            btc = next(s for s in status.symbols if s.symbol == "BTCUSDT")
            assert btc.health is RuntimeHealth.DEGRADED
            assert "gap" in btc.detail

        run_async(scenario())


class TestDuplicateOutOfOrder:
    def test_duplicates_and_replays_are_rejected_without_corruption(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            outcomes = scenarios.run_duplicate_out_of_order(h, symbol="BTCUSDT")

            assert outcomes["first_accept"] is IngestOutcome.ACCEPTED
            assert outcomes["exact_duplicate"] is IngestOutcome.DUPLICATE
            assert outcomes["second_accept"] is IngestOutcome.ACCEPTED
            assert outcomes["older_candle_replayed"] in (IngestOutcome.DUPLICATE, IngestOutcome.OUT_OF_ORDER)
            assert h.coordinator.status().overall_health is RuntimeHealth.READY

        run_async(scenario())


class TestProcessRestart:
    def test_accounting_survives_a_restart_cycle_and_reaches_ready_immediately(self, tmp_path) -> None:
        """BLOCKER FİX SONRASI (Karar 68): PUBLIC REST kaynağı (harness'in
        `_history` tamponu, GERÇEK Binance REST'in derin geçmişini temsil
        eder) checkpoint'in ETRAFINDA yeterli warmup lookback'ine SAHİPSE,
        restart artık YENİ M15/H1 candle'ları GERÇEK zamanda BEKLEMEDEN
        DOĞRUDAN READY'e dönmelidir — bu ARTIK "olabilir" değil, BEKLENEN
        normal davranıştır (bkz. `test_genuinely_insufficient_...` — YALNIZCA
        PUBLIC geçmiş GERÇEKTEN yetersizse BOOTSTRAPPING kalınır)."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=20)
            before = h.coordinator._paper_engine.position("BTCUSDT")

            await scenarios.run_process_restart_cycle(h, symbol="BTCUSDT", ticks_between=1)

            after = h.coordinator._paper_engine.position("BTCUSDT")
            assert after.quantity == before.quantity
            assert after.side == before.side
            assert after.average_entry_price == before.average_entry_price
            assert after.realized_pnl == before.realized_pnl
            # Yeterli PUBLIC geçmiş MEVCUT olduğundan (harness'in _history
            # tamponu), restart artık GERÇEK zamanda yeni candle beklemeden
            # DOĞRUDAN READY'e dönmelidir (fabrikasyon DEĞİL — GERÇEKTEN
            # yeniden çekilmiş warmup'tan).
            assert h.coordinator.status().overall_health is RuntimeHealth.READY

        run_async(scenario())

    def test_restart_before_any_live_candle_reaches_ready_again(self, tmp_path) -> None:
        """Faz 7'nin candle-checkpoint'i YALNIZCA canlı ingest edilmiş
        (`PersistedRuntime.ingest_candle`) candle'lar için yazılır —
        `recover()`'ın bootstrap-restore adımı HİÇBİR ŞEY checkpoint'lemez.
        Checkpoint `None` iken (henüz hiçbir canlı candle işlenmeden bir
        restart olursa) `recover()` TAM warmup penceresini `[now-lookback,
        now)` üzerinden çeker. Karar 68'den SONRA, checkpoint VARKEN de
        (`[checkpoint-lookback, now)`) AYNI garanti geçerlidir — bkz.
        `test_accounting_survives_a_restart_cycle_and_reaches_ready_immediately`."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db", warmup_candles=20)
            await h.start()
            assert h.store.load_candle_checkpoint("BTCUSDT", _M5) is None  # henüz hiçbir canlı candle YOK

            await h.stop_gracefully()
            await h.restart()
            h.ingest_order_book("BTCUSDT")
            assert h.coordinator.status().overall_health is RuntimeHealth.READY

        run_async(scenario())

    def test_genuinely_insufficient_public_history_stays_bootstrapping(self, tmp_path) -> None:
        """PUBLIC REST kaynağı GERÇEKTEN checkpoint etrafında yeterli
        lookback SAĞLAYAMIYORSA (`history_provider` hiçbir zaman
        `warmup_candles*2` kadar geriye gitmeyen KISITLI bir kaynağa
        sarılırsa), restart DÜRÜSTÇE BOOTSTRAPPING kalmalı — SAHTE bir
        READY asla üretilmemelidir."""
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db", warmup_candles=20)
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=5)  # checkpoint alır ama az geçmiş biriktirir

            real_history_provider = h.history_provider

            def truncated_history_provider(symbol, timeframe, start, end):
                candles = real_history_provider(symbol, timeframe, start, end)
                return candles[-2:]  # PUBLIC kaynak yalnızca SON 2 candle'ı "hatırlıyor"

            h.history_provider = truncated_history_provider  # type: ignore[method-assign]

            await h.stop_gracefully()
            await h.restart()
            h.ingest_order_book("BTCUSDT")

            assert h.coordinator.status().overall_health is RuntimeHealth.BOOTSTRAPPING

        run_async(scenario())

    def test_context_id_replay_after_restart_does_not_duplicate(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=20)
            state_before = h.store.load_paper_state("BTCUSDT")
            await h.stop_gracefully()
            await h.restart()
            state_after = h.store.load_paper_state("BTCUSDT")
            if state_before is not None:
                assert state_after is not None
                assert set(state_before.processed_context_ids) <= set(state_after.processed_context_ids)

        run_async(scenario())


class TestCrashLikeRestart:
    def test_crash_preserves_last_completed_checkpoint(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=10)
            checkpoint_before = h.store.load_candle_checkpoint("BTCUSDT", _M5)

            await scenarios.run_crash_restart_cycle(h, symbol="BTCUSDT", ticks_before_crash=1)

            checkpoint_after = h.store.load_candle_checkpoint("BTCUSDT", _M5)
            assert checkpoint_after is not None
            assert checkpoint_after >= checkpoint_before

        run_async(scenario())

    def test_crash_does_not_corrupt_open_position_state(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=20)

            await scenarios.run_crash_restart_cycle(h, symbol="BTCUSDT", ticks_before_crash=2)

            snapshot = h.store.load_paper_state("BTCUSDT")
            if snapshot is not None:
                assert math.isfinite(snapshot.position.quantity)
                assert math.isfinite(snapshot.position.realized_pnl)
                assert math.isfinite(snapshot.entry_fee)

        run_async(scenario())


class TestPersistenceFailure:
    def test_candle_checkpoint_failure_degrades_and_does_not_reset_state(self, tmp_path) -> None:
        async def scenario() -> None:
            holder: dict = {}
            h = SoakHarness(
                symbols=("BTCUSDT",), db_path=tmp_path / "s.db", store_factory=_faulty_store_factory(holder)
            )
            await h.start()
            scenarios.run_steady_state(h, ticks_per_symbol=5)

            holder["store"].fail_next_candle_checkpoints(1)
            scenarios.run_persistence_failure(h, symbol="BTCUSDT")

            status = h.coordinator.status()
            btc = next(s for s in status.symbols if s.symbol == "BTCUSDT")
            assert btc.health is RuntimeHealth.DEGRADED
            assert "candle_checkpoint" in btc.detail

            # sonraki BAŞARILI checkpoint faultu temizlemeli
            scenarios.run_persistence_failure(h, symbol="BTCUSDT")
            btc_after = next(s for s in h.coordinator.status().symbols if s.symbol == "BTCUSDT")
            assert btc_after.health is RuntimeHealth.READY

        run_async(scenario())

    def test_persistence_fault_on_one_symbol_does_not_affect_another(self, tmp_path) -> None:
        async def scenario() -> None:
            holder: dict = {}
            h = SoakHarness(
                symbols=("BTCUSDT", "ETHUSDT"),
                db_path=tmp_path / "s.db",
                store_factory=_faulty_store_factory(holder),
            )
            await h.start()
            scenarios.run_multi_symbol_stress(h, ticks_per_symbol=3)

            holder["store"].fail_next_paper_state_checkpoints(1)
            # ETHUSDT'yi tetikleyip BTCUSDT'nin fault'unu asla üretmediğinden emin ol
            scenarios.run_persistence_failure(h, symbol="ETHUSDT")

            status = h.coordinator.status()
            by_symbol = {s.symbol: s for s in status.symbols}
            assert "paper_state_checkpoint" in by_symbol["ETHUSDT"].detail or by_symbol["ETHUSDT"].health is RuntimeHealth.DEGRADED
            assert by_symbol["BTCUSDT"].detail != by_symbol["ETHUSDT"].detail

        run_async(scenario())


class TestStaleFeed:
    def test_stalled_symbol_becomes_degraded_active_symbol_stays_ready(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(
                symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db", stale_feed_threshold_seconds=60.0
            )
            await h.start()

            scenarios.run_stale_feed(h, stalled_symbol="BTCUSDT", active_symbol="ETHUSDT", ticks=10)
            # BTCUSDT event almadı ama clock, ETHUSDT tick'leri ile ilerledi ->
            # BTCUSDT'nin son event'i şimdi eşik dışı, kalmalı.
            status = h.coordinator.status()
            by_symbol = {s.symbol: s for s in status.symbols}
            assert by_symbol["BTCUSDT"].health is RuntimeHealth.DEGRADED
            assert "stale" in by_symbol["BTCUSDT"].detail
            assert by_symbol["ETHUSDT"].health is RuntimeHealth.READY

        run_async(scenario())

    def test_resumed_feed_after_stale_clears_via_normal_event(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(
                symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db", stale_feed_threshold_seconds=60.0
            )
            await h.start()
            scenarios.run_stale_feed(h, stalled_symbol="BTCUSDT", active_symbol="ETHUSDT", ticks=10)

            h.ingest_candle("BTCUSDT", _M5)
            status = h.coordinator.status()
            btc = next(s for s in status.symbols if s.symbol == "BTCUSDT")
            assert btc.health is RuntimeHealth.READY

        run_async(scenario())


class TestMultiSymbolStress:
    def test_many_symbols_isolated_and_deterministic(self, tmp_path) -> None:
        async def scenario() -> None:
            symbols = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT")
            h = SoakHarness(symbols=symbols, db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_multi_symbol_stress(h, ticks_per_symbol=25)

            status = h.coordinator.status()
            assert status.overall_health is RuntimeHealth.READY
            assert len(status.symbols) == len(symbols)
            for symbol in symbols:
                position = h.coordinator._paper_engine.position(symbol)
                assert math.isfinite(position.realized_pnl)

        run_async(scenario())

    def test_cross_symbol_checkpoints_are_isolated_by_symbol_column(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db")
            await h.start()
            scenarios.run_multi_symbol_stress(h, ticks_per_symbol=15)

            btc_checkpoint = h.store.load_candle_checkpoint("BTCUSDT", _M5)
            eth_checkpoint = h.store.load_candle_checkpoint("ETHUSDT", _M5)
            assert btc_checkpoint is not None and eth_checkpoint is not None

        run_async(scenario())


class TestShutdownUnderActivity:
    def test_no_candle_is_processed_after_stop_completes(self, tmp_path) -> None:
        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            pending = await scenarios.run_shutdown_under_activity(
                h, symbol="BTCUSDT", ticks_before_stop=3, ticks_after_stop=4
            )
            assert pending == 4  # hiçbiri tüketilmedi
            assert h.coordinator._stopped is True

        run_async(scenario())

    def test_shutdown_is_bounded_and_store_closes(self, tmp_path) -> None:
        import sqlite3

        import pytest

        async def scenario() -> None:
            h = SoakHarness(symbols=("BTCUSDT",), db_path=tmp_path / "s.db")
            await h.start()
            await scenarios.run_shutdown_under_activity(h, symbol="BTCUSDT", ticks_before_stop=2, ticks_after_stop=1)
            with pytest.raises(sqlite3.ProgrammingError):
                h.store._connection.execute("SELECT 1")

        run_async(scenario())


=== FILE: tests/test_state_contract.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.domain.state_contract import (
    CandleStateStore,
    CommitOutcome,
    InMemoryCandleStateStore,
)
from crypto_signal_engine.quality.base import DataQualityResult

UTC = timezone.utc
OPEN_TIME = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def make_update(update_seq: int, close: float = 100.0, is_closed: bool = False) -> CandleUpdate:
    candle = Candle(
        symbol="BTCUSDT", timeframe=Timeframe.M5, open_time=OPEN_TIME,
        close_time=OPEN_TIME + timedelta(minutes=5),
        open=100.0, high=max(105.0, close), low=min(99.0, close), close=close,
        volume=1.0, is_closed=is_closed,
    )
    return CandleUpdate(
        candle=candle, update_seq=update_seq,
        event_time=OPEN_TIME + timedelta(seconds=update_seq),
        received_at=OPEN_TIME + timedelta(seconds=update_seq),
    )


class TestCandleStateStoreIsAbstract:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            CandleStateStore()  # type: ignore[abstract]


class TestInMemoryCandleStateStore:
    def test_quality_pass_commits_to_canonical_state(self) -> None:
        store = InMemoryCandleStateStore()
        update = make_update(update_seq=1, close=101.0)
        result = store.commit(DataQualityResult.ok(), update)
        assert result.outcome == CommitOutcome.COMMITTED
        assert result.canonical_candle.close == 101.0
        assert store.peek_previous(update.identity).close == 101.0

    def test_quality_fail_never_reaches_sequencer(self) -> None:
        """KRİTİK INVARIANT (Quality Gate 10): reddedilen veri canonical
        state'i asla değiştirmez ve sequence katmanına ulaşmaz."""
        store = InMemoryCandleStateStore()
        # Önce geçerli bir state kuruyoruz.
        store.commit(DataQualityResult.ok(), make_update(update_seq=1, close=100.0))

        bad_result = DataQualityResult(status=DataQualityStatus.EXTREME_OUTLIER, reason="fiyat sıçraması")
        rejected_update = make_update(update_seq=2, close=999999.0)
        outcome = store.commit(bad_result, rejected_update)

        assert outcome.outcome == CommitOutcome.REJECTED_QUALITY
        # canonical state DEĞİŞMEDİ — hâlâ ilk kabul edilen değeri taşıyor
        assert store.peek_previous(rejected_update.identity).close == 100.0

    def test_quality_fail_does_not_advance_sequence_state(self) -> None:
        """Reddedilen bir update_seq, daha sonra AYNI seq ile tekrar (geçerli
        olarak) gönderildiğinde hâlâ kabul edilebilmelidir — çünkü reddedilen
        veri sequencer'a hiç ulaşmadı, dolayısıyla 'daha önce görüldü' olarak
        işaretlenmedi."""
        store = InMemoryCandleStateStore()
        store.commit(DataQualityResult.ok(), make_update(update_seq=1, close=100.0))

        bad_result = DataQualityResult(status=DataQualityStatus.STALE_PRICE, reason="test")
        store.commit(bad_result, make_update(update_seq=2, close=888.0))

        # aynı update_seq=2, bu kez KALİTEDEN GEÇEREK tekrar geliyor
        retry = store.commit(DataQualityResult.ok(), make_update(update_seq=2, close=102.0))
        assert retry.outcome == CommitOutcome.COMMITTED
        assert retry.canonical_candle.close == 102.0

    def test_sequence_rejection_reports_correctly(self) -> None:
        store = InMemoryCandleStateStore()
        store.commit(DataQualityResult.ok(), make_update(update_seq=5, close=100.0))
        out_of_order = store.commit(DataQualityResult.ok(), make_update(update_seq=2, close=1.0))
        assert out_of_order.outcome == CommitOutcome.REJECTED_SEQUENCE
        assert out_of_order.canonical_candle.close == 100.0

    def test_peek_previous_unknown_identity_returns_none(self) -> None:
        store = InMemoryCandleStateStore()
        update = make_update(update_seq=1)
        assert store.peek_previous(update.identity) is None


=== FILE: tests/test_state_manager.py ===
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade
from crypto_signal_engine.domain.state_contract import CommitOutcome, InMemoryCandleStateStore
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate
from crypto_signal_engine.state.manager import StateManager

UTC = timezone.utc
ALIGNED_OPEN = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def make_manager(now: datetime) -> StateManager:
    gate = BinanceDataQualityGate(FixedClock(now))
    return StateManager(gate, InMemoryCandleStateStore())


def make_candle_update(open_time=ALIGNED_OPEN, close=100.0, is_closed=False, seq=1) -> CandleUpdate:
    candle = Candle(
        symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=100.0, high=105.0, low=95.0, close=close, volume=10.0,
        is_closed=is_closed, trade_count=5,
    )
    return CandleUpdate(candle=candle, update_seq=seq, event_time=candle.close_time, received_at=candle.close_time)


class TestHandleCandleUpdate:
    def test_valid_update_committed(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        update = make_candle_update()
        result = manager.handle_candle_update(update)
        assert result.commit_result.committed
        assert result.quality_result.passed

    def test_misaligned_candle_rejected_and_not_committed(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        misaligned = ALIGNED_OPEN + timedelta(seconds=15)
        update = make_candle_update(open_time=misaligned)
        result = manager.handle_candle_update(update)
        assert result.commit_result.outcome == CommitOutcome.REJECTED_QUALITY
        assert result.quality_result.status == DataQualityStatus.TIMESTAMP_MISMATCH
        # canonical state hiç değişmemiş olmalı
        assert manager.peek_candle(update.identity) is None

    def test_duplicate_update_rejected_sequence(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        update = make_candle_update(seq=5)
        manager.handle_candle_update(update)
        dup_result = manager.handle_candle_update(update)
        assert dup_result.commit_result.outcome == CommitOutcome.REJECTED_SEQUENCE


class TestHandleTrade:
    def make_trade(self, trade_id=1, ts=ALIGNED_OPEN) -> Trade:
        return Trade(symbol="BTCUSDT", trade_id=trade_id, price=100.0, quantity=1.0, timestamp=ts, is_buyer_maker=False)

    def test_valid_trade_updates_latest(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        trade = self.make_trade()
        result = manager.handle_trade(trade, received_at=ALIGNED_OPEN)
        assert result.passed
        assert manager.latest_trade("BTCUSDT") is trade

    def test_rejected_trade_does_not_update_latest(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        first = self.make_trade(trade_id=10)
        manager.handle_trade(first, received_at=ALIGNED_OPEN)
        duplicate_id_trade = self.make_trade(trade_id=10)  # non-monotonic
        result = manager.handle_trade(duplicate_id_trade, received_at=ALIGNED_OPEN)
        assert not result.passed
        # canonical state hâlâ ilk trade'i taşıyor (ikincisi asla yazılmadı)
        assert manager.latest_trade("BTCUSDT") is first

    def test_symbol_normalized_lookup(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        trade = self.make_trade()
        manager.handle_trade(trade, received_at=ALIGNED_OPEN)
        assert manager.latest_trade("btcusdt") is trade


class TestHandleOrderBook:
    def make_book(self, last_update_id=1, ts=ALIGNED_OPEN) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=ts,
            bids=(OrderBookLevel(price=99.0, quantity=1.0),),
            asks=(OrderBookLevel(price=100.0, quantity=1.0),),
            last_update_id=last_update_id,
        )

    def test_valid_snapshot_updates_latest(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        book = self.make_book()
        result = manager.handle_order_book(book)
        assert result.passed
        assert manager.latest_order_book("BTCUSDT") is book

    def test_rejected_snapshot_does_not_update_latest(self) -> None:
        manager = make_manager(ALIGNED_OPEN + timedelta(seconds=1))
        first = self.make_book(last_update_id=100)
        manager.handle_order_book(first)
        stale = self.make_book(last_update_id=50)  # non-monotonic
        result = manager.handle_order_book(stale)
        assert not result.passed
        assert manager.latest_order_book("BTCUSDT") is first


=== FILE: tests/test_trade_features.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.models import Trade
from crypto_signal_engine.errors import FeatureCalculationError, InsufficientHistoryError
from crypto_signal_engine.features import trade_calculators as tc

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_trade(trade_id: int, qty: float, is_buyer_maker: bool, ts_offset_sec: float = 0.0) -> Trade:
    return Trade(
        symbol="BTCUSDT", trade_id=trade_id, price=100.0, quantity=qty,
        timestamp=T0 + timedelta(seconds=ts_offset_sec), is_buyer_maker=is_buyer_maker,
    )


class TestBuySellSplit:
    def test_buy_sell_volume_split(self) -> None:
        trades = [
            make_trade(1, 2.0, is_buyer_maker=False),  # buy aggression
            make_trade(2, 3.0, is_buyer_maker=True),  # sell aggression
            make_trade(3, 1.0, is_buyer_maker=False),  # buy aggression
        ]
        assert tc.buy_volume(trades) == pytest.approx(3.0)
        assert tc.sell_volume(trades) == pytest.approx(3.0)
        assert tc.total_volume(trades) == pytest.approx(6.0)

    def test_all_buy_volume_imbalance_is_one(self) -> None:
        trades = [make_trade(1, 5.0, is_buyer_maker=False)]
        assert tc.volume_imbalance(trades) == pytest.approx(1.0)

    def test_all_sell_volume_imbalance_is_negative_one(self) -> None:
        trades = [make_trade(1, 5.0, is_buyer_maker=True)]
        assert tc.volume_imbalance(trades) == pytest.approx(-1.0)

    def test_signed_volume(self) -> None:
        trades = [make_trade(1, 5.0, is_buyer_maker=False), make_trade(2, 2.0, is_buyer_maker=True)]
        assert tc.signed_volume(trades) == pytest.approx(3.0)


class TestZeroVolumeBehavior:
    def test_empty_trades_raises_insufficient_history(self) -> None:
        with pytest.raises(InsufficientHistoryError):
            tc.buy_volume([])
        with pytest.raises(InsufficientHistoryError):
            tc.total_volume([])


class TestCountAndAverage:
    def test_trade_count_and_avg_size(self) -> None:
        trades = [make_trade(1, 2.0, False), make_trade(2, 4.0, True), make_trade(3, 6.0, False)]
        assert tc.trade_count(trades) == pytest.approx(3.0)
        assert tc.avg_trade_size(trades) == pytest.approx(4.0)  # (2+4+6)/3


class TestRollingWindowBoundaries:
    def test_trade_intensity_known_value(self) -> None:
        trades = [make_trade(i, 1.0, False, ts_offset_sec=i * 2.0) for i in range(5)]  # 0,2,4,6,8s
        # 5 trade, süre = 8-0=8s -> intensity = 5/8
        assert tc.trade_intensity(trades, window_size=5) == pytest.approx(5 / 8)

    def test_trade_intensity_window_smaller_than_available(self) -> None:
        trades = [make_trade(i, 1.0, False, ts_offset_sec=i * 1.0) for i in range(10)]
        # son 3 trade: offsets 7,8,9 -> süre=2s, count=3 -> intensity=1.5
        assert tc.trade_intensity(trades, window_size=3) == pytest.approx(1.5)

    def test_trade_intensity_zero_duration_raises(self) -> None:
        trades = [make_trade(1, 1.0, False, ts_offset_sec=0.0), make_trade(2, 1.0, False, ts_offset_sec=0.0)]
        with pytest.raises(FeatureCalculationError, match="süre"):
            tc.trade_intensity(trades, window_size=2)

    def test_trade_intensity_insufficient_history(self) -> None:
        with pytest.raises(InsufficientHistoryError):
            tc.trade_intensity([make_trade(1, 1.0, False)], window_size=5)


class TestVolumeImbalanceBounds:
    def test_within_bounds_for_various_mixes(self) -> None:
        import random

        random.seed(7)
        for _ in range(20):
            trades = [
                make_trade(i, random.uniform(0.1, 10.0), random.choice([True, False]))
                for i in range(random.randint(1, 20))
            ]
            value = tc.volume_imbalance(trades)
            assert -1.0 <= value <= 1.0


=== FILE: tests/test_validation_helpers.py ===
from datetime import datetime, timedelta, timezone
from enum import Enum

import pytest

from crypto_signal_engine.domain._validation import (
    freeze_mapping,
    normalize_symbol,
    require_enum,
    require_finite,
    require_utc_aware,
)

UTC = timezone.utc


class _SampleEnum(str, Enum):
    A = "A"
    B = "B"


class TestRequireFinite:
    def test_finite_value_passes(self) -> None:
        assert require_finite(1.5, "x") == 1.5

    def test_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            require_finite(float("nan"), "x")

    def test_positive_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            require_finite(float("inf"), "x")

    def test_negative_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            require_finite(float("-inf"), "x")


class TestRequireUtcAware:
    def test_utc_aware_passes(self) -> None:
        value = datetime(2026, 8, 31, tzinfo=UTC)
        assert require_utc_aware(value, "x") == value

    def test_naive_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            require_utc_aware(datetime(2026, 8, 31), "x")

    def test_non_utc_offset_rejected(self) -> None:
        offset_tz = timezone(timedelta(hours=3))
        with pytest.raises(ValueError, match="UTC olmalı"):
            require_utc_aware(datetime(2026, 8, 31, tzinfo=offset_tz), "x")


class TestNormalizeSymbol:
    @pytest.mark.parametrize("raw", ["btcusdt", "BTCUSDT", " BTCUSDT", "BTCUSDT ", "  btcusdt  "])
    def test_all_variants_normalize_identically(self, raw: str) -> None:
        assert normalize_symbol(raw) == "BTCUSDT"

    def test_empty_after_strip_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            normalize_symbol("   ")

    def test_empty_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            normalize_symbol("")

    def test_none_rejected_with_type_error(self) -> None:
        """Quality Gate 28: incidental AttributeError DEĞİL, deterministik TypeError."""
        with pytest.raises(TypeError, match="str"):
            normalize_symbol(None)  # type: ignore[arg-type]

    def test_int_rejected_with_type_error(self) -> None:
        with pytest.raises(TypeError, match="str"):
            normalize_symbol(123)  # type: ignore[arg-type]

    def test_list_rejected_with_type_error(self) -> None:
        with pytest.raises(TypeError, match="str"):
            normalize_symbol(["BTCUSDT"])  # type: ignore[arg-type]


class TestRequireEnum:
    def test_correct_enum_instance_passes(self) -> None:
        assert require_enum(_SampleEnum.A, _SampleEnum, "x") == _SampleEnum.A

    def test_string_value_rejected_even_if_equal(self) -> None:
        """_SampleEnum(str, Enum) olduğundan "A" == _SampleEnum.A doğrudur,
        ancak require_enum isinstance kontrolü yaptığı için bu YİNE DE
        reddedilir — string'den enum'a sessiz cast YOKTUR."""
        assert "A" == _SampleEnum.A  # str-enum eşitliği (kontrol amaçlı)
        with pytest.raises(TypeError, match="_SampleEnum"):
            require_enum("A", _SampleEnum, "x")  # type: ignore[arg-type]

    def test_wrong_enum_type_rejected(self) -> None:
        class _OtherEnum(str, Enum):
            A = "A"

        with pytest.raises(TypeError, match="_SampleEnum"):
            require_enum(_OtherEnum.A, _SampleEnum, "x")  # type: ignore[arg-type]

    def test_none_rejected(self) -> None:
        with pytest.raises(TypeError):
            require_enum(None, _SampleEnum, "x")  # type: ignore[arg-type]


class TestFreezeMapping:
    def test_result_rejects_item_assignment(self) -> None:
        frozen = freeze_mapping({"a": 1.0}, "x")
        with pytest.raises(TypeError):
            frozen["a"] = 999.0  # type: ignore[index]

    def test_result_rejects_deletion(self) -> None:
        frozen = freeze_mapping({"a": 1.0}, "x")
        with pytest.raises(TypeError):
            del frozen["a"]  # type: ignore[attr-defined]

    def test_defensive_copy_original_mutation_does_not_leak(self) -> None:
        original = {"a": 1.0}
        frozen = freeze_mapping(original, "x")
        original["a"] = 999.0
        original["b"] = 42.0
        assert frozen["a"] == 1.0
        assert "b" not in frozen

    def test_non_mapping_rejected(self) -> None:
        with pytest.raises(TypeError):
            freeze_mapping([("a", 1.0)], "x")  # type: ignore[arg-type]

    def test_values_readable(self) -> None:
        frozen = freeze_mapping({"rsi": 55.0, "atr": 1.2}, "x")
        assert dict(frozen) == {"rsi": 55.0, "atr": 1.2}


