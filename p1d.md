<!-- p1d.md — Part 4/6 of all_code_part1.md — 76 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/selection/selector.py -->
<!--   - crypto_signal_engine/signal_engine.py -->
<!--   - crypto_signal_engine/stability/__init__.py -->
<!--   - crypto_signal_engine/stability/faults.py -->
<!--   - crypto_signal_engine/stability/harness.py -->
<!--   - crypto_signal_engine/stability/metrics.py -->
<!--   - crypto_signal_engine/stability/scenarios.py -->
<!--   - crypto_signal_engine/state/__init__.py -->
<!--   - crypto_signal_engine/state/manager.py -->
<!--   - deploy/env.example -->
<!--   - deploy/systemd/crypto-signal-engine-backup.service -->
<!--   - deploy/systemd/crypto-signal-engine-backup.timer -->
<!--   - deploy/systemd/crypto-signal-engine.service -->
<!--   - pyproject.toml -->
<!--   - research/__init__.py -->
<!--   - research/attribution.py -->
<!--   - research/data_quality.py -->
<!--   - research/drift.py -->
<!--   - research/errors.py -->
<!--   - research/guardrails.py -->
<!--   - research/monte_carlo.py -->
<!--   - research/oos_stability.py -->
<!--   - research/orderbook_capture.py -->
<!--   - research/replay.py -->
<!--   - research/sizing.py -->
<!--   - scripts/adaptive_evaluation_cycle.py -->
<!--   - scripts/backup_sqlite.py -->
<!--   - scripts/binance_testnet_lab.py -->
<!--   - scripts/historical_replay.py -->
<!--   - scripts/lifecycle_replay_sanity.py -->
<!--   - scripts/live_public_smoke_test.py -->
<!--   - scripts/local_readiness_check.py -->
<!--   - scripts/monte_carlo_robustness.py -->
<!--   - scripts/oos_stability.py -->
<!--   - scripts/public_soak_test.py -->
<!--   - scripts/run_with_adaptive_policy.py -->
<!--   - scripts/verify_phase1.py -->
<!--   - start_bot.sh -->
<!--   - tests/__init__.py -->
<!--   - tests/binance_fakes.py -->
<!--   - tests/conftest.py -->
<!--   - tests/execution_fakes.py -->
<!--   - tests/research_fakes.py -->
<!--   - tests/runtime_fakes.py -->
<!--   - tests/test_adaptive_challenger.py -->
<!--   - tests/test_adaptive_cycle.py -->
<!--   - tests/test_adaptive_decision.py -->
<!--   - tests/test_adaptive_drift_signal.py -->
<!--   - tests/test_adaptive_evaluation.py -->
<!--   - tests/test_adaptive_policy.py -->
<!--   - tests/test_adaptive_rollback.py -->
<!--   - tests/test_adaptive_scheduler.py -->
<!--   - tests/test_adaptive_shadow.py -->
<!--   - tests/test_adaptive_state_protection.py -->
<!--   - tests/test_adaptive_store.py -->
<!--   - tests/test_adaptive_symbol_score.py -->
<!--   - tests/test_adaptive_windows.py -->
<!--   - tests/test_agent_context.py -->
<!--   - tests/test_agent_independence.py -->
<!--   - tests/test_agents.py -->
<!--   - tests/test_app_24_7_ops.py -->
<!--   - tests/test_app_adaptive_wiring.py -->
<!--   - tests/test_app_admin_wiring.py -->
<!--   - tests/test_app_bridge_lifecycle_snapshot.py -->
<!--   - tests/test_app_cli.py -->
<!--   - tests/test_app_composition.py -->
<!--   - tests/test_app_lifecycle.py -->
<!--   - tests/test_app_notifier_wiring.py -->
<!--   - tests/test_app_ops_snapshots.py -->
<!--   - tests/test_app_price_freshness.py -->
<!--   - tests/test_app_signal_testnet_bridge.py -->
<!--   - tests/test_app_symbol_resolution.py -->
<!--   - tests/test_app_watchdog_wiring.py -->
<!--   - tests/test_backup_sqlite.py -->
<!--   - tests/test_binance_clock.py -->
<!--   - tests/test_binance_config.py -->

=== FILE: crypto_signal_engine/selection/selector.py ===
"""
`AutomaticSymbolSelector` — PUBLIC Binance Spot market data'sından
deterministik bir sembol evreni/fırsat seçimi üretir.

Tasarım (iki aşamalı, Bölüm "PERFORMANCE" gereksinimi):
    Aşama 0 — `/api/v3/exchangeInfo` (TEK çağrı): USDT-quote, TRADING,
        spot-trading-izinli, leveraged-token-BENZERİ OLMAYAN evren.
    Aşama 1 — `/api/v3/ticker/24hr` (TEK çağrı, TÜM semboller): ucuz
        likidite filtresi + kısa liste (`shortlist_size`). Her sembol
        için AYRI bir ticker çağrısı YAPILMAZ.
    Aşama 2 — YALNIZCA kısa listedeki semboller için `/api/v3/klines`
        (sembol başına TEK çağrı, `lookback_candles` kadar 5m mum):
        tarihsel hareket + güncel fırsat skorlaması.

Reuse kararı (KASITLI sınır, bkz. paket docstring'i): skorlama,
`features/candle_calculators.py`'nin saf fonksiyonlarını (roc, atr,
relative_volume) kullanır — Faz 4'ün TAM agent/consensus/risk pipeline'ı
(canlı çok-zaman-dilimli state + order book context gerektirir) burada
ÇAĞRILMAZ; bu, "ikinci bir bağımsız strateji İCAT ETME" kısıtına uymak
için bilinçli bir mimari sınırdır.

Fail-safe kural: discovery/ranking herhangi bir aşamada güvenle
tamamlanamazsa `SymbolSelectionError` fırlatılır — sembol İCAT EDİLMEZ,
BTCUSDT'ye VEYA başka bir varsayılana SESSİZCE düşülmez (bkz. errors.py).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import (
    CryptoSignalEngineError,
    FeatureCalculationError,
    InsufficientHistoryError,
    SymbolSelectionError,
)
from crypto_signal_engine.features.candle_calculators import atr, relative_volume, roc
from crypto_signal_engine.providers.binance.clock import Clock
from crypto_signal_engine.providers.binance.parser import ExchangeSymbolInfo, Ticker24hr
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig
from crypto_signal_engine.selection.models import CandidateScore, SelectionResult

_LOGGER = logging.getLogger("crypto_signal_engine.selection.selector")

# Deterministik, dokümante edilmiş ağırlıklar — env-configurable DEĞİLDİR
# (Bölüm gereksinimi yalnızca sayı/eşik/lookback'i configurable ister;
# ağırlıkları da configurable yapmak "gereksiz parametre yayılımı" olurdu).
# Toplamları 1.0'dır.
#
# BÖLÜM B FİX ("Liquidity is NOT opportunity"): likidite ağırlığı KASITLI
# OLARAK KÜÇÜK tutulur — likidite ZATEN AYRI, mutlak bir eligibility
# KAPISIDIR (`min_quote_volume_24h`) VE ayrıca mutlak hareket/aktivite
# tabanlarına tabidir (`min_atr_pct`/`min_current_move_pct`, bkz.
# `_meets_absolute_activity_floor`) — bu YÜZDEN ultra-likit ama hareketsiz
# bir piyasa (örn. bir stablecoin çifti) skorlamaya HİÇ ULAŞAMAZ (eligibility
# aşamasında reddedilir); ağırlıklandırılmış toplamdaki KÜÇÜK likidite payı,
# SADECE zaten-eligible adaylar arasında bir tradability tie-break'idir,
# birincil sıralama sürücüsü DEĞİLDİR.
#
# Adaptive Symbol Intelligence v1 — 4. bir OPSİYONEL skorlama terimi
# eklendi (`LEARNED_FACTOR_WEIGHT`, bkz. `adaptive/symbol_score.py`).
# Önceki 3 ağırlık (0.15/0.45/0.40) ORANSAL olarak yeniden ölçeklendi —
# bkz. bu milestone'un DECISIONS.md Karar'ı — böylece 4'ü TOPLAMDA hâlâ
# tam olarak 1.0'dır: 0.15*0.9=0.135, 0.45*0.9=0.405, 0.40*0.9=0.36,
# +0.10 = 1.0. `LEARNED_FACTOR_WEIGHT=0.10`, diğer üçünden KASITLI OLARAK
# küçük tutulur — bu terim, tıpkı likidite gibi, ZATEN eligible olan
# adaylar arasında bir tie-break'tir, birincil sıralama sürücüsü DEĞİLDİR
# (bkz. `adaptive/symbol_score.py` modül docstring'i).
LIQUIDITY_WEIGHT = 0.135
HISTORICAL_MOVEMENT_WEIGHT = 0.405
CURRENT_OPPORTUNITY_WEIGHT = 0.36
LEARNED_FACTOR_WEIGHT = 0.10

_NEUTRAL_LEARNED_FACTOR_SCORE = 0.5

# BÖLÜM C FİX ("Pump protection"): min-max normalizasyonundan ÖNCE
# uygulanan doygunluk (saturation) tavanları — TEK bir aşırı-uç değerin
# (örn. %500'lük bir anlık pump), min-max ölçeğini kendine doğru
# sıkıştırıp DİĞER TÜM adayları yapay olarak "zayıf" göstermesini
# ("bir outlier her şeyi 0'a bastırır") engeller. Bu tavanların ÜZERİNDEKİ
# HERHANGİ bir değer "zaten çok yüksek" kabul edilir — daha da büyük olması
# skoru ARTIK ARTIRMAZ (deterministik, açıklanabilir bir üst sınır).
_ATR_PCT_SATURATION_CEILING = 0.03  # %3 ATR (5m mumlarda ÇOK yüksek)
_ROC_PCT_SATURATION_CEILING = 20.0  # %20 mutlak fiyat değişimi

# Binance'in (artık kullanımdan kaldırılmış olsa da) leveraged-token
# base-asset kalıbı: {BASE}UP / {BASE}DOWN / {BASE}BULL / {BASE}BEAR.
# `_MIN_BASE_PREFIX_LEN`, "SUP" (Supra) gibi kısa/tesadüfi "UP" sonekli
# GERÇEK ticker'ların yanlışlıkla dışlanmasını önlemek için bir alt sınırdır
# (bilinen sınırlama — bkz. selector modülünün "Known limitations" notu).
_LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
_MIN_BASE_PREFIX_LEN = 2


def _looks_like_leveraged_token(base_asset: str) -> bool:
    for suffix in _LEVERAGED_SUFFIXES:
        if base_asset.endswith(suffix) and len(base_asset) - len(suffix) >= _MIN_BASE_PREFIX_LEN:
            return True
    return False


def _is_base_eligible(info: ExchangeSymbolInfo) -> bool:
    if info.quote_asset != "USDT":
        return False
    if info.status != "TRADING":
        return False
    if not info.is_spot_trading_allowed:
        return False
    if info.permissions and "LEVERAGED" in info.permissions:
        return False
    if _looks_like_leveraged_token(info.base_asset):
        return False
    return True


def _normalize(value: float, lo: float, hi: float) -> float:
    """[0, 1] min-max normalizasyonu. `hi == lo` (tüm değerler eşit) ise
    ayırt edici bilgi YOK demektir — nötr `0.5` döner (0.0 DEĞİL; aksi
    halde "hepsi eşit" durumu yanlışlıkla "hepsi en kötü" ile karışırdı)."""
    if hi <= lo:
        return 0.5
    return (value - lo) / (hi - lo)


def _rejected(symbol: str, quote_volume_24h: float, reason: str) -> CandidateScore:
    return CandidateScore(
        symbol=symbol, eligible=False, total_score=0.0, liquidity_score=0.0,
        historical_movement_score=0.0, current_opportunity_score=0.0,
        reason=reason, quote_volume_24h=quote_volume_24h, learned_factor_score=0.0,
    )


def _resolve_learned_factor(
    provider: Callable[[str], float] | None, symbol: str,
) -> float:
    """Adaptive Symbol Intelligence v1 — same defensive-wrap discipline
    as `RuntimeCoordinator.order_book_observer`/`candle_observer`/
    `LifecycleRuntime.m1_candle_observer`: `None` (default) always yields
    the neutral `0.5`, and a RAISING provider is caught here and ALSO
    falls back to neutral for that one symbol — it must never raise into
    `select()`'s pipeline and abort scoring for every other symbol."""
    if provider is None:
        return _NEUTRAL_LEARNED_FACTOR_SCORE
    try:
        return provider(symbol)
    except Exception:  # noqa: BLE001 - a provider failure must NEVER abort selection for this or any other symbol
        _LOGGER.error("selection: learned_factor_provider FAILED for %s (neutral 0.5 used)", symbol, exc_info=True)
        return _NEUTRAL_LEARNED_FACTOR_SCORE


class AutomaticSymbolSelector:
    """PUBLIC market data'dan (kimlik bilgisi YOK) deterministik bir
    sembol evreni seçer. Bu sınıf BUY/SELL sinyali ÜRETMEZ — yalnızca
    "hangi semboller izlenmeye değer" sorusuna cevap verir."""

    def __init__(
        self, rest_client: BinanceRestClient, clock: Clock, config: AutoSymbolSelectionConfig,
        *, learned_factor_provider: Callable[[str], float] | None = None,
    ) -> None:
        self._rest = rest_client
        self._clock = clock
        # Adaptive Symbol Intelligence v1 — additive, optional. `None`
        # (default) makes `select()`'s output BYTE-FOR-BYTE IDENTICAL to
        # before this parameter existed (see `_resolve_learned_factor`
        # and its neutral-0.5 default) — same discipline as
        # `exit_policy_provider`/`candle_observer`/`m1_candle_observer`.
        # This module NEVER imports `adaptive/` — the caller (a script
        # that imports both packages) constructs the concrete callable.
        self._learned_factor_provider = learned_factor_provider
        self._config = config

    async def select(self) -> SelectionResult:
        exchange_symbols, tickers = await self._discover()

        eligible_base = {s.symbol: s for s in exchange_symbols if _is_base_eligible(s)}
        universe_size = len(eligible_base)
        ticker_by_symbol: dict[str, Ticker24hr] = {t.symbol: t for t in tickers}

        shortlist = self._liquidity_shortlist(eligible_base, ticker_by_symbol)
        shortlist_size = len(shortlist)
        if not shortlist:
            raise SymbolSelectionError(
                f"no candidate passed liquidity eligibility "
                f"(universe={universe_size}, min_quote_volume_24h={self._config.min_quote_volume_24h})"
            )

        scored, rejected = await self._score_shortlist(shortlist)
        if not scored:
            raise SymbolSelectionError(
                f"no shortlisted symbol had sufficient/valid candle history "
                f"(shortlist={shortlist_size}, lookback_candles={self._config.lookback_candles})"
            )

        scored.sort(key=lambda c: (-c.total_score, c.symbol))
        selected = tuple(c.symbol for c in scored[: self._config.target_count])
        if not selected:
            raise SymbolSelectionError("scoring produced zero eligible symbols")

        return SelectionResult(
            generated_at=self._clock.now(),
            selected_symbols=selected,
            ranked_candidates=tuple(scored),
            rejected=tuple(rejected),
            universe_size=universe_size,
            shortlist_size=shortlist_size,
        )

    async def _discover(self) -> tuple[list[ExchangeSymbolInfo], list[Ticker24hr]]:
        try:
            exchange_symbols = await self._rest.fetch_exchange_info()
            tickers = await self._rest.fetch_24hr_tickers()
        except CryptoSignalEngineError as exc:
            raise SymbolSelectionError(f"Binance public discovery failed: {exc}") from exc
        return exchange_symbols, tickers

    def _liquidity_shortlist(
        self, eligible_base: dict[str, ExchangeSymbolInfo], ticker_by_symbol: dict[str, Ticker24hr]
    ) -> list[tuple[str, Ticker24hr]]:
        candidates = [
            (symbol, ticker_by_symbol[symbol])
            for symbol in eligible_base
            if symbol in ticker_by_symbol and ticker_by_symbol[symbol].quote_volume >= self._config.min_quote_volume_24h
        ]
        candidates.sort(key=lambda pair: (-pair[1].quote_volume, pair[0]))
        return candidates[: self._config.shortlist_size]

    async def _score_shortlist(
        self, shortlist: list[tuple[str, Ticker24hr]]
    ) -> tuple[list[CandidateScore], list[CandidateScore]]:
        lookback = self._config.lookback_candles
        recent = self._config.recent_window_candles
        now = self._clock.now()
        # Hafta sonu/işlem-molası kaynaklı boşluklara karşı 2x marj — asla
        # `now`'ın ÖTESİNE geçmez (no-lookahead, `end=now` sabit kalır).
        start = now - timedelta(minutes=5 * lookback * 2)

        rejected: list[CandidateScore] = []
        raw: list[tuple[str, float, float, float, float, float]] = []

        for symbol, ticker in shortlist:
            try:
                candles = await self._rest.fetch_historical_candles(symbol, Timeframe.M5, start, now)
            except CryptoSignalEngineError as exc:
                rejected.append(_rejected(symbol, ticker.quote_volume, f"candle fetch failed: {exc}"))
                continue

            candles = candles[-lookback:]
            if len(candles) < lookback:
                rejected.append(
                    _rejected(
                        symbol, ticker.quote_volume,
                        f"insufficient history: {len(candles)}/{lookback} candles",
                    )
                )
                continue

            try:
                vol_pct = atr(candles, period=14) / candles[-1].close
                move_roc = abs(roc(candles, period=lookback - 1))
                rel_vol = relative_volume(candles, period=recent)
                recent_roc = abs(roc(candles, period=recent - 1)) if recent >= 2 else 0.0
            except (InsufficientHistoryError, FeatureCalculationError) as exc:
                rejected.append(_rejected(symbol, ticker.quote_volume, f"scoring failed: {exc}"))
                continue

            # BÖLÜM B/D FİX — MUTLAK aktivite tabanı, batch'e GÖRE DEĞİL:
            # bir sembol, ne kadar likit olursa olsun VEYA aynı kısa
            # listedeki diğer herkesten "daha az durgun" olsa BİLE, hem
            # tarihsel HEM güncel harekette gerçek/mutlak bir eşiği
            # GEÇEMİYORSA doğrudan REDDEDİLİR — min-max normalizasyonun
            # zayıf bir batch'i yapay olarak "iyi" göstermesi YAPISAL
            # OLARAK engellenir (bkz. selector.py modül docstring'i).
            if vol_pct < self._config.min_atr_pct:
                rejected.append(
                    _rejected(
                        symbol, ticker.quote_volume,
                        f"below absolute historical activity floor: ATR%={vol_pct * 100:.4f}% "
                        f"< min_atr_pct={self._config.min_atr_pct * 100:.4f}%",
                    )
                )
                continue
            if recent_roc < self._config.min_current_move_pct:
                rejected.append(
                    _rejected(
                        symbol, ticker.quote_volume,
                        f"below absolute current activity floor: |ROC_{max(recent - 1, 0)}|={recent_roc:.4f}% "
                        f"< min_current_move_pct={self._config.min_current_move_pct:.4f}%",
                    )
                )
                continue

            raw.append((symbol, ticker.quote_volume, vol_pct, move_roc, rel_vol, recent_roc))

        if not raw:
            return [], rejected

        # BÖLÜM C FİX — normalize ETMEDEN ÖNCE doygunluk tavanı uygula: TEK
        # bir aşırı-uç pump, min-max ölçeğini kendine doğru sıkıştırıp
        # DİĞER herkesi yapay olarak ezmesin (bkz. sabitler docstring'i).
        capped = [
            (symbol, qv, min(vp, _ATR_PCT_SATURATION_CEILING), min(mr, _ROC_PCT_SATURATION_CEILING),
             rv, min(rr, _ROC_PCT_SATURATION_CEILING))
            for symbol, qv, vp, mr, rv, rr in raw
        ]

        def _column(index: int) -> list[float]:
            return [row[index] for row in capped]

        qv_lo, qv_hi = min(_column(1)), max(_column(1))
        vp_lo, vp_hi = min(_column(2)), max(_column(2))
        mr_lo, mr_hi = min(_column(3)), max(_column(3))
        rv_lo, rv_hi = min(_column(4)), max(_column(4))
        rr_lo, rr_hi = min(_column(5)), max(_column(5))

        scored: list[CandidateScore] = []
        for (symbol, quote_volume, vol_pct, move_roc, rel_vol, recent_roc), (_, _, raw_vp, raw_mr, _, raw_rr) in zip(
            capped, raw
        ):
            liquidity = _normalize(quote_volume, qv_lo, qv_hi)
            historical = (_normalize(vol_pct, vp_lo, vp_hi) + _normalize(move_roc, mr_lo, mr_hi)) / 2.0
            current = (_normalize(rel_vol, rv_lo, rv_hi) + _normalize(recent_roc, rr_lo, rr_hi)) / 2.0
            learned = _resolve_learned_factor(self._learned_factor_provider, symbol)
            total = (
                LIQUIDITY_WEIGHT * liquidity + HISTORICAL_MOVEMENT_WEIGHT * historical
                + CURRENT_OPPORTUNITY_WEIGHT * current + LEARNED_FACTOR_WEIGHT * learned
            )
            # Reason'da HAM (doygunluk tavanından ÖNCEKİ) değerler gösterilir
            # — "understandable reasons/metrics" gerçek ölçümü yansıtmalı;
            # tavan yalnızca normalizasyon/skor hesaplamasını etkiler.
            reason = (
                f"liquidity={liquidity:.3f} (quoteVolume24h={quote_volume:,.0f} USDT); "
                f"historical_movement={historical:.3f} (ATR%={raw_vp * 100:.3f}%, "
                f"|ROC_{lookback - 1}|={raw_mr:.3f}%); "
                f"current_opportunity={current:.3f} (relVol_{recent}={rel_vol:.2f}x, "
                f"|ROC_{max(recent - 1, 0)}|={raw_rr:.3f}%); "
                f"learned_factor={learned:.3f}"
            )
            scored.append(
                CandidateScore(
                    symbol=symbol, eligible=True, total_score=total, liquidity_score=liquidity,
                    historical_movement_score=historical, current_opportunity_score=current,
                    reason=reason, quote_volume_24h=quote_volume, learned_factor_score=learned,
                )
            )
        return scored, rejected


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


=== FILE: crypto_signal_engine/stability/__init__.py ===
"""
Faz 9 — Long-Run Stability / Soak Testing.

Bu paket, kabul edilmiş Faz 1-8 sistemini (PUBLIC market data -> feature
engine -> SignalEngine -> PaperTradingEngine -> persistence -> operations)
DEĞİŞTİRMEDEN, onu HIZLANDIRILMIŞ (accelerated) sanal zamanda uzun süreli
çalıştırarak durability/idempotency/temporal/accounting invariant'larının
korunduğunu deterministic ve offline olarak kanıtlamak için bir soak/
stability test harness'i sağlar.

Bu paket bir execution/trading paketi DEĞİLDİR — `PaperTradingEngine`
(Faz 5) TEK trading/simülasyon sınırı olmaya devam eder. `ALLOW_LIVE_TRADING
= False` invariant'ı korunur.

Modüller:
- `harness.py` — `SoakHarness`: gerçek Faz 5/6/7 nesnelerini (PaperTradingEngine/
  RuntimeCoordinator/PaperStateStore/PersistedRuntime) `FixedClock` (Faz 2,
  DEĞİŞTİRİLMEDEN) ile süren, deterministik candle/order-book üretimi yapan
  bir sürücü.
- `faults.py` — `ScriptedMarketDataProvider` (yalnızca gerçek async
  stream/run()/stop() mekaniğini gerektiren senaryolar için) ve
  `FaultyPaperStateStore` (deterministik persistence-failure injection).
- `metrics.py` — `SoakMetrics`: senaryo boyunca gözlemlenen sayaçlar.
- `scenarios.py` — 10 çekirdek soak senaryosunun (steady state, reconnect
  storm, gap recovery, duplicate/out-of-order, process restart, crash-like
  restart, persistence failure, stale feed, multi-symbol stress, shutdown
  under activity) yeniden kullanılabilir sürücü fonksiyonları."""

from __future__ import annotations


=== FILE: crypto_signal_engine/stability/faults.py ===
"""
Faz 9 — deterministik fault injection.

Bölüm ("dağınık monkeypatch chaos yerine küçük, açık bir fault API'si"):
bu modül yalnızca İKİ araç sağlar:

1. `ScriptedMarketDataProvider` — `LiveDataProvider` sözleşmesini karşılayan,
   `asyncio.Queue`-tabanlı bir test/soak double'ı. Yalnızca Faz 6'nın
   GERÇEK async `run()`/`stop()`/disconnect-reconnect mekaniğini gerektiren
   senaryolar (reconnect storm, gap recovery, shutdown-under-activity) için
   kullanılır — diğer senaryolar `SoakHarness` üzerinden DOĞRUDAN, senkron
   `ingest_candle()`/`ingest_order_book()` çağrılarıyla sürülür (Faz 6'nın
   per-event iş mantığının TAMAMINI zaten aynı şekilde egzersiz eder).
2. `FaultyPaperStateStore` — gerçek bir `PaperStateStore`'u SARAN (onu
   DEĞİŞTİRMEDEN), belirli bir sonraki checkpoint çağrısını deterministik
   olarak başarısız kılabilen ince bir wrapper (Faz 7'nin kendi
   `test_persistence_recovery.py::_FlakyStore` test double'ının aynı
   ilkesinin, Faz 9 soak harness'i için genelleştirilmiş hâli).

Her ikisi de GERÇEK Faz 5/6/7 nesnelerini SARAR/besler — iş mantığını
TEKRARLAMAZ."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.persistence.paper_state_store import PaperStateSnapshot, PaperStateStore
from crypto_signal_engine.providers.base import ConnectionState, ProviderHealthSnapshot


class _Disconnect:
    """`ScriptedMarketDataProvider`'ın dahili kuyruğuna, bir stream'in
    KOPMASINI simüle etmek için konan sentinel — gerçek bir market event
    DEĞİLDİR."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc


class ScriptedMarketDataProvider:
    """`LiveDataProvider` sözleşmesini (duck-typing) karşılayan, tamamen
    kuyruk-tabanlı (offline, gerçek network YOK) bir provider double'ı.

    `stream_candles`/`stream_order_book`, ilgili kuyruğa bir şey
    `push_*`/`disconnect()` ile eklenene KADAR cooperatively `await
    queue.get()` ile bekler — bu bir wall-clock sleep DEĞİLDİR (gerçek bir
    provider'ın network I/O'da beklemesiyle aynı asyncio idiom'u)."""

    def __init__(self, history_fn=None) -> None:
        """`history_fn(symbol, timeframe, start, end) -> list[Candle]` —
        `fetch_historical_candles()`'ın delege edeceği deterministik
        "PUBLIC REST" kaynağı (tipik olarak `SoakHarness.history_provider`).
        Verilmezse boş liste döner (yalnızca zaten bootstrap edilmiş bir
        harness'e canlı akış EKLEMEK isteyen senaryolar için yeterlidir)."""
        self._candle_queues: dict[tuple[str, Timeframe], asyncio.Queue] = {}
        self._order_book_queues: dict[str, asyncio.Queue] = {}
        self._history_fn = history_fn or (lambda symbol, timeframe, start, end: [])
        self._fetch_failures: dict[tuple[str, Timeframe], Exception] = {}
        self.fetch_calls: list[tuple[str, Timeframe, datetime, datetime]] = []
        self.closed = False

    # -- kurulum / fault injection -------------------------------------------

    def fail_next_fetch(self, symbol: str, timeframe: Timeframe, exc: Exception) -> None:
        self._fetch_failures[(symbol, timeframe)] = exc

    def push_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> None:
        self._queue_for_candles(symbol, timeframe).put_nowait(candle)

    def push_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> None:
        self._queue_for_order_book(symbol).put_nowait(snapshot)

    def disconnect_candles(self, symbol: str, timeframe: Timeframe, exc: Exception | None = None) -> None:
        """Bir sonraki `stream_candles()` tüketiminde bağlantının
        KOPTUĞUNU simüle eder (gerçek Faz 6 `_consume_candles`'ın
        `except Exception: mark_disconnected(...); raise` yolunu tetikler)."""
        self._queue_for_candles(symbol, timeframe).put_nowait(_Disconnect(exc or ConnectionError("simulated disconnect")))

    def disconnect_order_book(self, symbol: str, exc: Exception | None = None) -> None:
        self._queue_for_order_book(symbol).put_nowait(_Disconnect(exc or ConnectionError("simulated disconnect")))

    def pending_candles(self, symbol: str, timeframe: Timeframe) -> int:
        """Hâlâ TÜKETİLMEMİŞ (kimse `await queue.get()` çağırmadığı için
        kuyrukta bekleyen) candle sayısı — shutdown-sonrası push edilen
        event'lerin GERÇEKTEN işlenmediğini (run_task tamamlandıktan sonra
        onları okuyacak hiçbir consumer kalmadığını) doğrulamak için."""
        return self._queue_for_candles(symbol, timeframe).qsize()

    def _queue_for_candles(self, symbol: str, timeframe: Timeframe) -> asyncio.Queue:
        return self._candle_queues.setdefault((symbol, timeframe), asyncio.Queue())

    def _queue_for_order_book(self, symbol: str) -> asyncio.Queue:
        return self._order_book_queues.setdefault(symbol, asyncio.Queue())

    # -- LiveDataProvider sözleşmesi ------------------------------------------

    async def stream_candles(self, symbol: str, timeframe: Timeframe) -> AsyncIterator[Candle]:
        queue = self._queue_for_candles(symbol, timeframe)
        while True:
            item = await queue.get()
            if isinstance(item, _Disconnect):
                raise item.exc
            yield item

    async def stream_order_book(self, symbol: str, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        queue = self._queue_for_order_book(symbol)
        while True:
            item = await queue.get()
            if isinstance(item, _Disconnect):
                raise item.exc
            yield item

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        self.fetch_calls.append((symbol, timeframe, start, end))
        key = (symbol, timeframe)
        if key in self._fetch_failures:
            raise self._fetch_failures.pop(key)
        return list(self._history_fn(symbol, timeframe, start, end))

    def health(self) -> ProviderHealthSnapshot:
        return ProviderHealthSnapshot(
            connection_state=ConnectionState.CONNECTED, last_message_at=None, reconnect_count=0, symbol="UNKNOWN"
        )

    def health_for(self, key: str) -> ProviderHealthSnapshot | None:
        return None

    async def close(self) -> None:
        self.closed = True


class FaultyPaperStateStore:
    """Gerçek bir `PaperStateStore`'u SARAR (DEĞİŞTİRMEZ) — belirli
    checkpoint türlerinin bir sonraki N çağrısını deterministik olarak
    `PersistenceError` ile başarısız kılar, sonra normal delegasyona döner.

    `PersistedRuntime`, `store`'unu yalnızca duck-typing ile kullanır
    (`checkpoint_candle`/`checkpoint_candle_transition`/`checkpoint_paper_state`/
    `load_paper_state`/`load_candle_checkpoint`/`close`) — bu wrapper TAM
    OLARAK bu altı metodu implemente eder, hiçbir Faz 7 sözleşmesini
    DEĞİŞTİRMEDEN.

    Faz 7 pre-audit remediation NOTU (HIGH-1 fix): canlı ingestion ARTIK
    `checkpoint_candle_transition()`'ı (candle + varsa Paper transition,
    TEK atomik transaction'da) kullanır — `checkpoint_candle`/
    `checkpoint_paper_state` bağımsız çağrı yolları olarak KORUNUR (Faz 5
    seviyesi izole testler ve doğrudan store kullanımı için), ama
    `PersistedRuntime.ingest_candle()`/`resolve_gap()` artık bunları ayrı
    ayrı ÇAĞIRMAZ. `fail_next_candle_checkpoints`/
    `fail_next_paper_state_checkpoints`, atomicity'yle TUTARLI kalması
    için `checkpoint_candle_transition`'ı da etkiler (ikisi de armed'sa,
    kombine yazım BAŞARISIZ olur — kısmi bir "yalnızca candle tarafı
    başarısız oldu ama paper tarafı yine de yazıldı" durumu ARTIK
    GÖZLEMLENEMEZ, tam da HIGH-1'in düzelttiği şey)."""

    def __init__(self, inner: PaperStateStore) -> None:
        self._inner = inner
        self._fail_candle_checkpoints = 0
        self._fail_paper_state_checkpoints = 0

    def fail_next_candle_checkpoints(self, count: int = 1) -> None:
        self._fail_candle_checkpoints += count

    def fail_next_paper_state_checkpoints(self, count: int = 1) -> None:
        self._fail_paper_state_checkpoints += count

    def checkpoint_candle(self, symbol: str, timeframe: Timeframe, open_time: datetime) -> None:
        if self._fail_candle_checkpoints > 0:
            self._fail_candle_checkpoints -= 1
            raise PersistenceError("simulated candle checkpoint failure (Faz 9 fault injection)")
        self._inner.checkpoint_candle(symbol, timeframe, open_time)

    def checkpoint_paper_state(self, **kwargs: object) -> None:
        if self._fail_paper_state_checkpoints > 0:
            self._fail_paper_state_checkpoints -= 1
            raise PersistenceError("simulated paper state checkpoint failure (Faz 9 fault injection)")
        self._inner.checkpoint_paper_state(**kwargs)

    def checkpoint_candle_transition(self, **kwargs: object) -> None:
        if self._fail_candle_checkpoints > 0:
            self._fail_candle_checkpoints -= 1
            raise PersistenceError(
                "simulated candle checkpoint failure inside combined candle+paper transaction "
                "(Faz 9 fault injection)"
            )
        if self._fail_paper_state_checkpoints > 0:
            self._fail_paper_state_checkpoints -= 1
            raise PersistenceError(
                "simulated paper state checkpoint failure inside combined candle+paper transaction "
                "(Faz 9 fault injection)"
            )
        self._inner.checkpoint_candle_transition(**kwargs)

    def load_paper_state(self, symbol: str) -> PaperStateSnapshot | None:
        return self._inner.load_paper_state(symbol)

    def load_candle_checkpoint(self, symbol: str, timeframe: Timeframe) -> datetime | None:
        return self._inner.load_candle_checkpoint(symbol, timeframe)

    def close(self) -> None:
        self._inner.close()


=== FILE: crypto_signal_engine/stability/harness.py ===
"""
Faz 9 — `SoakHarness`: gerçek Faz 5/6/7 nesnelerini HIZLANDIRILMIŞ sanal
zamanda süren, deterministik bir soak/stability test sürücüsü.

Mimari kural ("Faz 6 runtime mantığını TEKRARLAMA"): bu sınıf hiçbir
candle-identity/dedup/gap-tespiti/feature/signal/paper-trading/persistence
mantığı İÇERMEZ — yalnızca `PaperTradingEngine` (Faz 5), `RuntimeCoordinator`
(Faz 6), `PaperStateStore`/`PersistedRuntime` (Faz 7) nesnelerini KURAR ve
onların PUBLIC/dokümante edilmiş metodlarını (`ingest_candle`,
`ingest_order_book`, `recover`, `stop`) doğrudan çağırır — aynı Faz 6/7'nin
KENDİ test suite'lerinin yaptığı gibi.

Neden gerçek `run()`/`stream_candles()` YERİNE çoğunlukla doğrudan
`ingest_candle()`/`ingest_order_book()` çağrısı: bu ikisi, `_consume_candles`/
`_consume_order_book`'un HER olayda zaten çağırdığı TEK metotlardır — per-
event iş mantığının (identity/dedup/gap/feature/signal/paper/checkpoint)
TAMAMI böylece egzersiz edilir; yalnızca async generator/`run()`/`stop()`
orkestrasyonunun KENDİSİ (zaten Faz 6'da ayrı ayrı kabul edilmiş/test
edilmiş) atlanır. Reconnect/gap-recovery/shutdown-under-activity senaryoları
İSTİSNAdır — bunlar `run()`/`stop()`'un KENDİSİNİ test eder, bu yüzden
`faults.ScriptedMarketDataProvider` ile GERÇEK async akışı kullanırlar (bkz.
`scenarios.py`).

Hiçbir gerçek wall-clock sleep YOKTUR — zaman, Faz 2'nin kabul edilmiş
`FixedClock`'u (DEĞİŞTİRİLMEDEN) elle ilerletilerek simüle edilir."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import BootstrapReport, ProcessedMarketEvent
from crypto_signal_engine.stability.metrics import SoakMetrics

# `coordinator.py::_TIMEFRAME_DURATIONS`'ın KASITLI, KÜÇÜK bir kopyası —
# aynı gerekçe ile: `recovery.py`'nin KENDİSİ zaten bu tam deseni kullanır
# (bkz. o dosyanın modül docstring'i) — iş mantığı DEĞİL, sabit bir lookup.
_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}

DEFAULT_TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M5, Timeframe.M15, Timeframe.H1)


def make_deterministic_candle(
    symbol: str, timeframe: Timeframe, open_time: datetime, counter: int, *, base_price: float = 100.0
) -> Candle:
    """Sınırlı (bounded), asla sabit-olmayan (VOLUME_ZSCORE_20 gibi
    std-sıfır feature hatalarından kaçınmak için — bkz. Faz 6 test dersi)
    deterministik bir candle üretir. `counter` KEZ arttıkça fiyat, patlamayan
    (bounded) bir salınım izler — binlerce tick boyunca sürse bile absürt
    değerlere DRIFT ETMEZ (gerçekçi bir "yatay piyasa" simülasyonu)."""
    duration = _TIMEFRAME_DURATIONS[timeframe]
    price = base_price + 5.0 * math.sin(counter / 7.0) + (counter % 3) * 0.01
    volume = 10.0 + (counter % 4)
    return Candle(
        symbol=symbol, timeframe=timeframe, open_time=open_time, close_time=open_time + duration,
        open=price, high=price + 1.0, low=price - 1.0, close=price, volume=volume, is_closed=True,
    )


def make_deterministic_order_book(symbol: str, timestamp: datetime, counter: int, *, mid: float = 100.0) -> OrderBookSnapshot:
    spread = 0.5 + (counter % 3) * 0.1
    return OrderBookSnapshot(
        symbol=symbol, timestamp=timestamp,
        bids=(OrderBookLevel(price=mid - spread, quantity=5.0), OrderBookLevel(price=mid - spread * 2, quantity=5.0)),
        asks=(OrderBookLevel(price=mid + spread, quantity=5.0), OrderBookLevel(price=mid + spread * 2, quantity=5.0)),
        last_update_id=counter + 1,
    )


class SoakHarness:
    """Gerçek Faz 5/6/7 nesnelerini `FixedClock` ile süren soak sürücüsü.

    Kullanım deseni:
        harness = SoakHarness(symbols=("BTCUSDT", "ETHUSDT"), db_path=tmp_path / "s.db")
        await harness.start()                      # ilk bootstrap (Faz 7 recover() üzerinden)
        harness.ingest_candle("BTCUSDT", Timeframe.M5)
        harness.advance_clock(timedelta(minutes=5))
        await harness.stop_gracefully()
        await harness.restart()                     # yeni nesneler + Faz 7 recover()
    """

    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        db_path,
        timeframes: tuple[Timeframe, ...] = DEFAULT_TIMEFRAMES,
        warmup_candles: int = 20,
        notional_per_position: float = 1000.0,
        fee_bps: float = 0.0,
        slippage_bps: float = 0.0,
        stale_feed_threshold_seconds: float = 30.0,
        initial_time: datetime | None = None,
        store_factory=PaperStateStore,
    ) -> None:
        self.symbols = tuple(symbols)
        self.db_path = db_path
        self.timeframes = timeframes
        self.warmup_candles = warmup_candles
        self.notional_per_position = notional_per_position
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.stale_feed_threshold_seconds = stale_feed_threshold_seconds
        self._store_factory = store_factory
        self.clock = FixedClock(initial_time or datetime(2026, 1, 1, tzinfo=timezone.utc))
        self.metrics = SoakMetrics()

        # Deterministik "PUBLIC REST" gerçeği kaynağı: her (symbol,timeframe)
        # için üretilen TÜM candle'lar burada tutulur, böylece recovery/
        # gap-fill fetch'leri gerçekçi bir şekilde beslenebilir.
        self._history: dict[tuple[str, Timeframe], list[Candle]] = {}
        self._candle_counter: dict[tuple[str, Timeframe], int] = {}
        self._ob_counter: dict[str, int] = {}
        self._next_open_time: dict[tuple[str, Timeframe], datetime] = {}

        self.store: PaperStateStore | None = None
        self.coordinator: RuntimeCoordinator | None = None
        self.runtime: PersistedRuntime | None = None

    # -- (yeniden) kompozisyon -------------------------------------------------

    def open(self) -> None:
        """Faz 5/6/7 nesnelerini (YENİDEN) kurar — `db_path`'e bağlı,
        AYNI durable state üzerinde. Restart/crash-restart senaryoları
        için: `open()` + `await runtime.recover()`."""
        self.store = self._store_factory(self.db_path)
        paper_engine = PaperTradingEngine(
            notional_per_position=self.notional_per_position, fee_bps=self.fee_bps, slippage_bps=self.slippage_bps
        )
        self.coordinator = RuntimeCoordinator(
            symbols=self.symbols,
            provider=_HistoryOnlyProvider(self.history_provider),
            candle_timeframes=self.timeframes,
            warmup_candles=self.warmup_candles,
            stale_feed_threshold_seconds=self.stale_feed_threshold_seconds,
            paper_engine=paper_engine,
            clock=self.clock,
        )
        self.runtime = PersistedRuntime(self.coordinator, self.store)

    def open_with_provider(self, provider) -> None:
        """`open()` ile AYNI, ama `RuntimeCoordinator`'a (gerçek async
        akış gerektiren senaryolar için, bkz. `faults.ScriptedMarketDataProvider`)
        özel bir provider enjekte eder."""
        self.store = self._store_factory(self.db_path)
        paper_engine = PaperTradingEngine(
            notional_per_position=self.notional_per_position, fee_bps=self.fee_bps, slippage_bps=self.slippage_bps
        )
        self.coordinator = RuntimeCoordinator(
            symbols=self.symbols,
            provider=provider,
            candle_timeframes=self.timeframes,
            warmup_candles=self.warmup_candles,
            stale_feed_threshold_seconds=self.stale_feed_threshold_seconds,
            paper_engine=paper_engine,
            clock=self.clock,
        )
        self.runtime = PersistedRuntime(self.coordinator, self.store)

    async def start(self) -> tuple[BootstrapReport, ...]:
        """İlk açılış: `open()` + warmup candle geçmişini üretir + Faz 7
        `recover()`'ı çağırır (checkpoint YOKSA tam warmup fetch, Faz 7'nin
        KENDİ, DEĞİŞTİRİLMEMİŞ politikası) + her sembol için bir order-book
        snapshot'ı besler (READY için Faz 6'nın gerektirdiği ikinci koşul)."""
        self.open()
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                self._seed_warmup_history(symbol, timeframe)
        reports = await self.runtime.recover()
        for symbol in self.symbols:
            self.ingest_order_book(symbol)
        return reports

    async def start_with_provider(self, provider) -> tuple[BootstrapReport, ...]:
        """`start()` ile AYNI dizi, ama `RuntimeCoordinator`'a özel bir
        provider (tipik olarak `faults.ScriptedMarketDataProvider`) enjekte
        eder — gerçek async `run()`/`stop()`/disconnect mekaniği gerektiren
        senaryolar (reconnect storm, shutdown-under-activity) için."""
        self.open_with_provider(provider)
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                self._seed_warmup_history(symbol, timeframe)
        reports = await self.runtime.recover()
        for symbol in self.symbols:
            self.ingest_order_book(symbol)
        return reports

    async def restart(self) -> tuple[BootstrapReport, ...]:
        """Süreç yeniden başlatmasını simüle eder: YENİ Faz 5/6/7 nesneleri,
        AYNI durable store'a karşı `recover()` çağırır. Çağırandan ÖNCE
        `stop_gracefully()`/`crash()` çağrılmış olmalıdır."""
        self.metrics.restarts += 1
        self.open()
        return await self.runtime.recover()

    async def stop_gracefully(self) -> None:
        await self.runtime.stop()

    def crash(self) -> None:
        """ANİ process kaybını simüle eder: `runtime.stop()`/`store.close()`
        BİLEREK ÇAĞRILMAZ — Faz 7'nin ATOMİK checkpoint transaction'ları
        (`with self._connection:`) sayesinde, o ana kadar TAMAMLANMIŞ her
        checkpoint durable kalır; yalnızca Python referansları düşürülür
        (gerçek bir `kill -9`'da olduğu gibi, temiz kapanış KODU hiç
        ÇALIŞMAZ)."""
        self.metrics.crash_restarts += 1
        self.store = None
        self.coordinator = None
        self.runtime = None

    # -- deterministik veri üretimi/ingestion ----------------------------------

    def _seed_warmup_history(self, symbol: str, timeframe: Timeframe) -> None:
        duration = _TIMEFRAME_DURATIONS[timeframe]
        end = self.clock.now() - duration
        start_counter = self._candle_counter.get((symbol, timeframe), 0)
        candles = []
        for i in range(self.warmup_candles):
            open_time = end - duration * (self.warmup_candles - 1 - i)
            candles.append(make_deterministic_candle(symbol, timeframe, open_time, start_counter + i))
        self._candle_counter[(symbol, timeframe)] = start_counter + self.warmup_candles
        self._next_open_time[(symbol, timeframe)] = candles[-1].open_time + duration
        self._history.setdefault((symbol, timeframe), []).extend(candles)

    def next_candle(self, symbol: str, timeframe: Timeframe) -> Candle:
        """Bir sonraki kronolojik candle'ı üretir (ingest ETMEZ, yalnızca
        üretir) — duplicate/out-of-order senaryoları için, ÜRETİLMİŞ bir
        candle'ın kasıtlı olarak TEKRAR/eski bir sırayla ingest edilmesi
        gerekebilir."""
        counter = self._candle_counter.get((symbol, timeframe), 0)
        open_time = self._next_open_time[(symbol, timeframe)]
        candle = make_deterministic_candle(symbol, timeframe, open_time, counter)
        self._candle_counter[(symbol, timeframe)] = counter + 1
        self._next_open_time[(symbol, timeframe)] = open_time + _TIMEFRAME_DURATIONS[timeframe]
        self._history.setdefault((symbol, timeframe), []).append(candle)
        # "Accelerated time": her yeni candle üretimi, o candle'ın kapanışına
        # KADAR gerçek zamanın GEÇTİĞİ anlamına gelir — clock'u SADECE İLERİ
        # (asla geri) `close_time`'a hizalar. Bu olmadan `now()` dondurulmuş
        # kalır ve `recover()`'ın `[start, now)` REST fetch penceresi
        # candle'ların KENDİSİNİ hiç KAPSAMAZ (Faz 2/7'nin gerçek
        # `[checkpoint, now)` sözleşmesiyle TUTARSIZ olurdu).
        if candle.close_time > self.clock.now():
            self.clock.set(candle.close_time)
        return candle

    def skip_candle(self, symbol: str, timeframe: Timeframe) -> Candle:
        """Bir candle'ı ÜRETİR ve geçmişe (REST kaynağına) ekler ama
        ingest ETMEZ — bir sonraki gerçek ingest'te bir gap açık bırakır
        (gap-recovery senaryosu için)."""
        return self.next_candle(symbol, timeframe)

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle | None = None) -> ProcessedMarketEvent:
        if candle is None:
            candle = self.next_candle(symbol, timeframe)
        event = self.runtime.ingest_candle(symbol, timeframe, candle)
        self.metrics.record_candle_outcome(event.outcome)
        self.metrics.record_cycle_result(event.cycle_result)
        return event

    def ingest_order_book(self, symbol: str) -> ProcessedMarketEvent:
        counter = self._ob_counter.get(symbol, 0)
        snapshot = make_deterministic_order_book(symbol, self.clock.now(), counter)
        self._ob_counter[symbol] = counter + 1
        event = self.runtime.ingest_order_book(symbol, snapshot)
        self.metrics.record_order_book()
        return event

    def advance_clock(self, delta: timedelta) -> None:
        self.clock.advance(delta.total_seconds())

    def history_provider(
        self, symbol: str, timeframe: Timeframe, start: datetime | None, end: datetime | None
    ) -> list[Candle]:
        """Deterministik "PUBLIC REST" kaynağı: `start`/`end` VERİLMİŞSE
        (gerçek `fetch_historical_candles(symbol, timeframe, start, end)`
        çağrısı gibi) yalnızca `[start, end)` aralığındaki candle'ları
        döner — bu, `resolve_gap()`'in "SADECE eksik aralığı çek" davranışını
        DOĞRU şekilde egzersiz etmek için ÖNEMLİDİR (aksi halde `pending`
        candle'ın kendisi de yanlışlıkla dönüp bootstrap tarafından erken
        tüketilir, retry'ı yanlış bir DUPLICATE'e çevirir). `start`/`end`
        VERİLMEZSE (`None, None`) TÜM geçmişi döner (tam warmup/recovery
        fetch'i için)."""
        candles = self._history.get((symbol, timeframe), [])
        if start is None and end is None:
            return list(candles)
        return [c for c in candles if start <= c.open_time < end]


class _HistoryOnlyProvider:
    """`SoakHarness.open()`'ın varsayılan provider'ı: `fetch_historical_candles`
    harness'in KENDİ deterministik geçmişine (bootstrap/recovery/gap-fill
    için) delege eder; `stream_candles`/`stream_order_book` KASITLI OLARAK
    implemente EDİLMEZ — direct-call senaryoları (harness'in
    `ingest_candle()`/`ingest_order_book()` metodları) bunları hiç
    çağırmaz. Gerçek `run()`/`stream_*` gerektiren senaryolar
    `SoakHarness.open_with_provider(ScriptedMarketDataProvider())` kullanır."""

    def __init__(self, history_fn) -> None:
        self._history_fn = history_fn

    async def stream_candles(self, symbol, timeframe):
        raise NotImplementedError(
            "_HistoryOnlyProvider akış sağlamaz — bu senaryo gerçek streaming gerektiriyorsa "
            "SoakHarness.open_with_provider(ScriptedMarketDataProvider()) kullanın"
        )
        yield  # pragma: no cover

    async def stream_order_book(self, symbol, depth):
        raise NotImplementedError(
            "_HistoryOnlyProvider akış sağlamaz — bu senaryo gerçek streaming gerektiriyorsa "
            "SoakHarness.open_with_provider(ScriptedMarketDataProvider()) kullanın"
        )
        yield  # pragma: no cover

    async def fetch_historical_candles(self, symbol, timeframe, start, end):
        return self._history_fn(symbol, timeframe, start, end)

    def health(self):
        from crypto_signal_engine.providers.base import ConnectionState, ProviderHealthSnapshot

        return ProviderHealthSnapshot(
            connection_state=ConnectionState.CONNECTED, last_message_at=None, reconnect_count=0, symbol="UNKNOWN"
        )

    def health_for(self, key):
        return None

    async def close(self) -> None:
        return None


=== FILE: crypto_signal_engine/stability/metrics.py ===
"""Faz 9 — soak senaryosu boyunca gözlemlenen sayaçlar.

Bu bir monitoring/dashboard katmanı DEĞİLDİR — yalnızca testlerin/manuel
soak script'inin assertion/rapor üretmek için okuduğu, saf bir sayaç
kabıdır (iş mantığı YOK)."""

from __future__ import annotations

from dataclasses import dataclass, field

from crypto_signal_engine.domain.enums import SignalDirection
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeCycleResult


@dataclass
class SoakMetrics:
    candles_ingested: int = 0
    candles_accepted: int = 0
    candles_duplicate: int = 0
    candles_out_of_order: int = 0
    candles_gap_detected: int = 0
    candles_unclosed_skipped: int = 0
    order_book_ingested: int = 0
    signals_evaluated: int = 0
    paper_transitions: int = 0
    neutral_signals: int = 0
    idempotent_replays: int = 0
    reconnects: int = 0
    gap_recoveries_attempted: int = 0
    gap_recoveries_succeeded: int = 0
    persistence_faults_marked: int = 0
    persistence_faults_cleared: int = 0
    restarts: int = 0
    crash_restarts: int = 0
    identity_mismatches_rejected: int = 0

    _OUTCOME_COUNTERS: dict[IngestOutcome, str] = field(
        default_factory=lambda: {
            IngestOutcome.ACCEPTED: "candles_accepted",
            IngestOutcome.DUPLICATE: "candles_duplicate",
            IngestOutcome.OUT_OF_ORDER: "candles_out_of_order",
            IngestOutcome.GAP_DETECTED: "candles_gap_detected",
            IngestOutcome.UNCLOSED_SKIPPED: "candles_unclosed_skipped",
        },
        repr=False,
    )

    def record_candle_outcome(self, outcome: IngestOutcome) -> None:
        self.candles_ingested += 1
        attr = self._OUTCOME_COUNTERS.get(outcome)
        if attr is not None:
            setattr(self, attr, getattr(self, attr) + 1)

    def record_order_book(self) -> None:
        self.order_book_ingested += 1

    def record_cycle_result(self, cycle_result: RuntimeCycleResult | None) -> None:
        if cycle_result is None or not cycle_result.evaluated:
            return
        self.signals_evaluated += 1
        if cycle_result.paper_result is not None and cycle_result.paper_result.idempotent_replay:
            self.idempotent_replays += 1
            return
        if cycle_result.signal is not None and cycle_result.signal.direction is SignalDirection.NEUTRAL:
            self.neutral_signals += 1
            return
        self.paper_transitions += 1


=== FILE: crypto_signal_engine/stability/scenarios.py ===
"""
Faz 9 — 10 çekirdek soak senaryosunun yeniden kullanılabilir sürücü
fonksiyonları. Her fonksiyon YALNIZCA simülasyonu SÜRER (mekanik adımlar);
invariant assertion'ları KASITLI OLARAK burada DEĞİL, `tests/test_stability_*.py`
içindedir (senaryo sürücüsü ile "bunun doğru olduğunu kanıtlama" mantığı
ayrı tutulur — aynı senaryo hem otomatik testlerde hem ileride manuel bir
soak script'inde yeniden kullanılabilsin diye).

`Timeframe.M5` senaryo boyunca "PRIMARY_SIGNAL_TIMEFRAME" (Faz 6,
DEĞİŞTİRİLMEDEN) olduğundan, sinyal değerlendirmesini tetiklemek için
M5 candle'ları ingest edilir; M15/H1 de (READY/warm-up sözleşmesini
korumak için) düzenli aralıklarla ilerletilir."""

from __future__ import annotations

import asyncio

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.runtime.models import IngestOutcome
from crypto_signal_engine.stability.faults import ScriptedMarketDataProvider
from crypto_signal_engine.stability.harness import SoakHarness

_PRIMARY = Timeframe.M5


async def _pump(iterations: int = 10) -> None:
    """Kuyruğa `put_nowait` edilmiş bir item'ın, o item'ı bekleyen
    consumer task'ı TARAFINDAN GERÇEKTEN işlenmesi (queue.get()'in
    future'ı çözülüp task yeniden zamanlanır, SONRA çalışır, SONRA
    exception/health-mutation gerçekleşir) BİRDEN FAZLA event-loop
    iterasyonu gerektirebilir. Bu, `asyncio.sleep(N>0)` DEĞİLDİR (gerçek
    zaman geçmez) — yalnızca zaten HAZIR olan callback'lerin işlenmesi
    için event loop'a KOOPERATİF olarak birkaç kez kontrol bırakır."""
    for _ in range(iterations):
        await asyncio.sleep(0)


def _tick_all_timeframes(harness: SoakHarness, symbol: str, primary_ticks: int) -> None:
    """`primary_ticks` kadar M5 candle ingest eder; her 3 M5'te bir M15,
    her 12 M5'te bir H1 ilerletir (gerçekçi, deterministik bir çoklu-
    timeframe kapanış oranı) — Faz 6'nın "M5 = sinyal tetikleyici"
    invariant'ını KORUR, yeniden İCAT ETMEZ."""
    for i in range(primary_ticks):
        harness.ingest_candle(symbol, _PRIMARY)
        if Timeframe.M15 in harness.timeframes and (i + 1) % 3 == 0:
            harness.ingest_candle(symbol, Timeframe.M15)
        if Timeframe.H1 in harness.timeframes and (i + 1) % 12 == 0:
            harness.ingest_candle(symbol, Timeframe.H1)


# -- 1. Steady State -----------------------------------------------------------


def run_steady_state(harness: SoakHarness, *, ticks_per_symbol: int = 60) -> None:
    for symbol in harness.symbols:
        harness.ingest_order_book(symbol)
        _tick_all_timeframes(harness, symbol, ticks_per_symbol)
        harness.ingest_order_book(symbol)


# -- 2. Reconnect Storm (gerçek async run()/stop() + disconnect) ---------------


async def run_reconnect_storm(
    harness: SoakHarness, *, disconnecting_symbol: str, healthy_symbol: str, events_per_symbol: int = 4
) -> asyncio.Task:
    """GERÇEK Faz 6 sözleşmesi (bkz. `RuntimeCoordinator.run()` docstring'i:
    "Faz 2 kendi içinde sınırsız reconnect döngüsü çalıştırır — bu metod
    asla kendi başına yeniden subscribe OLMAZ"): bir stream'in KOPMASI
    (`stream_candles()`'ın exception fırlatması), o (symbol,timeframe)
    consumer task'ı için TERMİNALDİR — Faz 2'nin KENDİ (bu senaryonun
    kapsamı DIŞINDaki, zaten `tests/test_binance_reconnect.py` ile ayrı
    kabul edilmiş) reconnect politikası TÜKENDİĞİNDE/başarısız olduğunda
    ortaya çıkar. Bu YÜZDEN "storm" burada TEK bir sembolün TEKRAR TEKRAR
    kendi kendine reconnect ETMESİ olarak DEĞİL — bunun yerine, GERÇEKTEN
    test edilmesi gereken invariant olarak modellenir: bir sembolün
    stream'inin TERMİNAL olarak kopması (a) ANINDA ve doğru şekilde
    DEGRADED'a yansır, (b) `run()`'ın KENDİSİNİ çökertMEZ (`gather(...,
    return_exceptions=True)`), (c) DİĞER sembollerin/timeframe'lerin akışını
    HİÇ ETKİLEMEZ (Faz 6'nın per-symbol izolasyon invariant'ı)."""
    provider = ScriptedMarketDataProvider(history_fn=harness.history_provider)
    await harness.start_with_provider(provider)

    run_task = asyncio.create_task(harness.runtime.run())
    await asyncio.sleep(0)

    for timeframe in harness.timeframes:
        provider.disconnect_candles(disconnecting_symbol, timeframe)
        await _pump()
    provider.disconnect_order_book(disconnecting_symbol)
    await asyncio.sleep(0)
    harness.metrics.reconnects += 1

    for _ in range(events_per_symbol):
        candle = harness.next_candle(healthy_symbol, _PRIMARY)
        provider.push_candle(healthy_symbol, _PRIMARY, candle)
        await _pump()

    return run_task


# -- 3. Gap Recovery (gerçek Faz 6 `resolve_gap()` PUBLIC sınırı) --------------


async def run_gap_recovery(harness: SoakHarness, *, symbol: str, gap_size: int = 3) -> IngestOutcome:
    for _ in range(gap_size):
        harness.skip_candle(symbol, _PRIMARY)  # REST'e yazılır, ingest EDİLMEZ -> gap
    pending = harness.next_candle(symbol, _PRIMARY)
    event = harness.ingest_candle(symbol, _PRIMARY, pending)
    assert event.outcome is IngestOutcome.GAP_DETECTED
    resolved = await harness.coordinator.resolve_gap(symbol, _PRIMARY, pending)
    harness.metrics.gap_recoveries_attempted += 1
    if resolved.outcome is IngestOutcome.ACCEPTED:
        harness.metrics.gap_recoveries_succeeded += 1
    return resolved.outcome


# -- 4. Duplicate / Out-of-order / Malformed identity --------------------------


def run_duplicate_out_of_order(harness: SoakHarness, *, symbol: str) -> dict[str, IngestOutcome]:
    outcomes: dict[str, IngestOutcome] = {}
    first = harness.next_candle(symbol, _PRIMARY)
    outcomes["first_accept"] = harness.ingest_candle(symbol, _PRIMARY, first).outcome
    outcomes["exact_duplicate"] = harness.ingest_candle(symbol, _PRIMARY, first).outcome
    second = harness.next_candle(symbol, _PRIMARY)
    outcomes["second_accept"] = harness.ingest_candle(symbol, _PRIMARY, second).outcome
    outcomes["older_candle_replayed"] = harness.ingest_candle(symbol, _PRIMARY, first).outcome
    return outcomes


# -- 5. Process Restart ---------------------------------------------------------


async def run_process_restart_cycle(harness: SoakHarness, *, symbol: str, ticks_between: int = 5) -> None:
    _tick_all_timeframes(harness, symbol, ticks_between)
    await harness.stop_gracefully()
    await harness.restart()
    for other in harness.symbols:
        harness.ingest_order_book(other)


# -- 6. Crash-like Restart -------------------------------------------------------


async def run_crash_restart_cycle(harness: SoakHarness, *, symbol: str, ticks_before_crash: int = 5) -> None:
    _tick_all_timeframes(harness, symbol, ticks_before_crash)
    harness.crash()
    await harness.restart()
    for other in harness.symbols:
        harness.ingest_order_book(other)


# -- 7. Persistence Failure -------------------------------------------------------


def run_persistence_failure(harness: SoakHarness, *, symbol: str) -> None:
    _tick_all_timeframes(harness, symbol, 1)


# -- 8. Stale Feed ----------------------------------------------------------------


def run_stale_feed(harness: SoakHarness, *, stalled_symbol: str, active_symbol: str, ticks: int = 5) -> None:
    for _ in range(ticks):
        _tick_all_timeframes(harness, active_symbol, 1)


# -- 9. Multi-Symbol Stress --------------------------------------------------------


def run_multi_symbol_stress(harness: SoakHarness, *, ticks_per_symbol: int = 30) -> None:
    for symbol in harness.symbols:
        harness.ingest_order_book(symbol)
    for _ in range(ticks_per_symbol):
        for symbol in harness.symbols:
            _tick_all_timeframes(harness, symbol, 1)


# -- 10. Shutdown Under Activity (gerçek async run()/stop()) -----------------------


async def run_shutdown_under_activity(
    harness: SoakHarness, *, symbol: str, ticks_before_stop: int = 3, ticks_after_stop: int = 3
) -> int:
    """`run()` sürerken `stop()` çağırır, `run_task` TAMAMLANDIKTAN SONRA
    daha fazla event push eder. Döner: shutdown sonrası kuyrukta kalan
    (yani hiçbir consumer tarafından ASLA tüketilmeyen) candle sayısı —
    bu değer `ticks_after_stop`'a EŞİT olmalıdır (0 kabul EDİLMİŞ olmalı),
    Faz 6'nın KENDİ `_stopped` guard'ının (consumer loop'un İÇİNDE) bu
    event'leri gerçekten reddettiğini kanıtlar."""
    provider = ScriptedMarketDataProvider(history_fn=harness.history_provider)
    await harness.start_with_provider(provider)

    run_task = asyncio.create_task(harness.runtime.run())
    await asyncio.sleep(0)

    for _ in range(ticks_before_stop):
        candle = harness.next_candle(symbol, _PRIMARY)
        provider.push_candle(symbol, _PRIMARY, candle)
        await _pump()

    await harness.stop_gracefully()
    await run_task  # run_task bu noktada TAMAMEN bitmiş olmalı (consumer'lar iptal edildi)

    for _ in range(ticks_after_stop):
        candle = harness.next_candle(symbol, _PRIMARY)
        provider.push_candle(symbol, _PRIMARY, candle)

    return provider.pending_candles(symbol, _PRIMARY)


=== FILE: crypto_signal_engine/state/__init__.py ===


=== FILE: crypto_signal_engine/state/manager.py ===
"""
StateManager.

Kural (Bölüm 11): Phase 1 `CandleStateStore` contract'ını gerçek Phase 2
state/persistence implementasyonu ile GENİŞLETİR — Phase 1 contract'ı
değiştirilmez. `StateManager`, candle/trade/order-book için TEK giriş
noktasıdır: her ikisi de önce `DataQualityGate`'ten geçer, yalnızca kabul
edilen veri canonical state'e yazılır.

Minimum canonical state (Bölüm 11):
- candles          -> `CandleStateStore` (injectable: in-memory veya SQLite)
- latest trade/cursor -> `_latest_trade` + `_trade_cursor` (SequenceCursor)
- order-book snapshot/metadata -> `_latest_order_book`
- sequence cursor  -> `SequenceCursor` (trade/order-book, domain/events.py)

Provider health BURADA TUTULMAZ (Bölüm 11'de "gerekiyorsa ayrı operational
state" olarak belirtilmiş) — bu, Phase 1 `ProviderHealthSnapshot`
sözleşmesi gereği provider'ın kendi sorumluluğunda kalır.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_utc_aware
from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.events import EventType, SequenceCursor
from crypto_signal_engine.domain.models import Candle, CandleIdentity, OrderBookSnapshot, Trade
from crypto_signal_engine.domain.state_contract import CandleStateStore, CommitResult
from crypto_signal_engine.quality.base import DataQualityGate, DataQualityResult


@dataclass(frozen=True)
class CandleHandlingResult:
    """`handle_candle_update`'in tam sonucu: hem quality kararı hem commit kararı.

    Provider katmanının (örn. websocket.py) `MISSING_CANDLE` gibi belirli
    bir red nedenine göre gap-recovery/backfill tetikleyebilmesi için,
    yalnızca `CommitResult` (ki bu quality detayını bir string içinde
    gizler) yeterli değildir — bu yüzden `quality_result` ayrıca taşınır.
    """

    quality_result: DataQualityResult
    commit_result: CommitResult


class StateManager:
    """Candle/trade/order-book canonical state'ini tek noktadan yöneten orkestratör."""

    def __init__(self, quality_gate: DataQualityGate, candle_store: CandleStateStore) -> None:
        self._quality_gate = quality_gate
        self._candle_store = candle_store
        # (symbol, timeframe) -> en son BAŞARIYLA commit edilmiş candle.
        #
        # KRİTİK TASARIM NOTU: `CandleStateStore.peek_previous(identity)`
        # (Phase 1 contract'ı) "BU identity için mevcut canonical state"
        # anlamına gelir — YENİ bir open_time için bu HER ZAMAN None döner.
        # Gap/missing-interval tespiti için gereken şey ise "bu symbol+
        # timeframe için en son bilinen candle, open_time'ı ne olursa
        # olsun"dur. İlk implementasyonda bunun yerine sabit "bir-önceki-
        # interval" identity'si hesaplanmıştı; bu, ÇOKLU interval'lik
        # gap'lerde (örn. 3 dakikalık bir boşluk) "bir önceki interval"in
        # DE hiç commit edilmemiş olması nedeniyle gap tespitini tamamen
        # atlıyordu (bkz. DECISIONS.md — bu, test sırasında bulunup
        # düzeltilen gerçek bir bug'dı). Bu yüzden ayrı, açık bir
        # "en son candle" takibi tutulur.
        self._latest_candle: dict[tuple[str, Timeframe], Candle] = {}
        self._latest_trade: dict[str, Trade] = {}
        self._trade_cursor: dict[str, SequenceCursor] = {}
        self._latest_order_book: dict[str, OrderBookSnapshot] = {}

    # -- Candle ------------------------------------------------------------

    def handle_candle_update(self, update: CandleUpdate) -> CandleHandlingResult:
        """Bir candle update'ini quality-gate'ten geçirip commit etmeyi dener.

        Akış (bkz. domain/state_contract.py): quality gate ÖNCE çalışır;
        yalnızca `passed=True` olan update sequencer'a ulaşır.

        `quality_gate.check_candle`'a verilen `previous`, bu symbol+
        timeframe için EN SON başarıyla commit edilmiş candle'dır (`
        _latest_candle`) — GÜNCEL update'in kendi identity'sinin state'i
        DEĞİL. Bu ayrım, hem "aynı candle'ın kendi partial state'ine göre
        aşırı hareket" (previous.open_time == update.open_time olduğunda
        missing-interval kontrolü otomatik atlanır) hem de "önceki
        interval'e göre gap" (previous.open_time < update.open_time
        olduğunda) senaryolarını doğru ayırt eder.
        """
        key = (update.candle.symbol, update.candle.timeframe)
        previous = self._latest_candle.get(key)
        quality_result = self._quality_gate.check_candle(update.candle, previous)
        commit_result = self._candle_store.commit(quality_result, update)
        if commit_result.committed:
            self._latest_candle[key] = commit_result.canonical_candle
        return CandleHandlingResult(quality_result=quality_result, commit_result=commit_result)

    def peek_candle(self, identity: CandleIdentity) -> Candle | None:
        return self._candle_store.peek_previous(identity)

    def latest_candle(self, symbol: str, timeframe: Timeframe) -> Candle | None:
        """(symbol, timeframe) için en son BAŞARIYLA commit edilmiş candle'ı
        döndürür (open_time'ı ne olursa olsun).

        Bu, gap-recovery gibi Phase 2 orkestrasyon ihtiyaçları için GÜVENLİ
        bir PUBLIC sorgu noktasıdır — provider'ların `StateManager`'ın
        private `_latest_candle` alanına doğrudan erişmesi (private
        attribute hack) GEREKMEZ.
        """
        return self._latest_candle.get((normalize_symbol(symbol), timeframe))

    def handle_historical_candle_update(self, update: CandleUpdate) -> CandleHandlingResult:
        """REST backfill/historical candle'lar için `handle_candle_update`'in
        özel bir varyantı.

        Kural (Faz 2 Bölüm 2 — quality model ayrımı): historical/backfill
        candle'lara YALNIZCA structural validity (alignment, missing-
        interval, zero-volume anomaly, extreme outlier) uygulanır;
        realtime feed freshness (staleness) kontrolü UYGULANMAZ — bir
        backfill candle doğası gereği "şimdi"den eski olur, bu invalid bir
        durum DEĞİLDİR.

        Bu, quality_gate'in (duck-typing ile, Phase 1 ABC'sini
        DEĞİŞTİRMEDEN) opsiyonel bir `check_historical_candle(candle,
        previous)` metodunu destekleyip desteklemediğine bakar; desteklemiyorsa
        (generic/başka bir `DataQualityGate` implementasyonu) güvenli
        şekilde standart `check_candle`'a düşer (fallback).
        """
        key = (update.candle.symbol, update.candle.timeframe)
        previous = self._latest_candle.get(key)
        check_historical = getattr(self._quality_gate, "check_historical_candle", None)
        if callable(check_historical):
            quality_result = check_historical(update.candle, previous)
        else:
            quality_result = self._quality_gate.check_candle(update.candle, previous)
        commit_result = self._candle_store.commit(quality_result, update)
        if commit_result.committed:
            self._latest_candle[key] = commit_result.canonical_candle
        return CandleHandlingResult(quality_result=quality_result, commit_result=commit_result)

    # -- Trade ---------------------------------------------------------------

    def handle_trade(self, trade: Trade, received_at: datetime) -> DataQualityResult:
        """Bir trade'i quality-gate'ten geçirir; yalnızca PASS olursa
        canonical `_latest_trade`/`_trade_cursor` güncellenir."""
        require_utc_aware(received_at, "received_at")
        symbol = normalize_symbol(trade.symbol)
        previous = self._latest_trade.get(symbol)
        quality_result = self._quality_gate.check_trade(trade, previous)
        if quality_result.passed:
            self._latest_trade[symbol] = trade
            self._trade_cursor[symbol] = SequenceCursor(
                symbol=symbol,
                event_type=EventType.TRADE,
                last_sequence_id=trade.trade_id,
                last_received_at=received_at,
            )
        return quality_result

    def latest_trade(self, symbol: str) -> Trade | None:
        return self._latest_trade.get(normalize_symbol(symbol))

    def trade_cursor(self, symbol: str) -> SequenceCursor | None:
        return self._trade_cursor.get(normalize_symbol(symbol))

    # -- Order book ------------------------------------------------------------

    def handle_order_book(self, snapshot: OrderBookSnapshot) -> DataQualityResult:
        """Bir order book snapshot'ını quality-gate'ten geçirir; yalnızca
        PASS olursa canonical `_latest_order_book` güncellenir."""
        symbol = normalize_symbol(snapshot.symbol)
        previous = self._latest_order_book.get(symbol)
        quality_result = self._quality_gate.check_order_book(snapshot, previous)
        if quality_result.passed:
            self._latest_order_book[symbol] = snapshot
        return quality_result

    def latest_order_book(self, symbol: str) -> OrderBookSnapshot | None:
        return self._latest_order_book.get(normalize_symbol(symbol))


=== FILE: deploy/env.example ===
# Crypto Signal Engine — example environment file for systemd's
# EnvironmentFile=/etc/crypto-signal-engine/env
#
# Copy this file to /etc/crypto-signal-engine/env, edit the values you need,
# and restrict its permissions (it contains no secrets — there are none in
# this system — but keep operational config out of world-readable reach as
# good practice: chmod 640, chown root:csengine).
#
# There is NO api key / secret / credential field in this CSE_* namespace,
# and there never will be (see SAFETY_INVARIANTS.md). Phase 12 adds an
# OPT-IN Binance Spot TESTNET execution mode (still not this file's
# concern — see the note near the bottom); by default this service only
# ever reads PUBLIC Binance market data and simulates trades locally.

# Required: comma-separated list of Binance symbols to track.
CSE_SYMBOLS=BTCUSDT,ETHUSDT

# Durable SQLite state (paper positions, candle checkpoints). Use an
# absolute path under /var/lib/crypto-signal-engine/ in production.
CSE_DB_PATH=/var/lib/crypto-signal-engine/paper_state.db

# Optional overrides (safe defaults are used if omitted):
# CSE_LOG_LEVEL=INFO
# CSE_STALE_FEED_THRESHOLD_SECONDS=30
# CSE_FEE_BPS=0
# CSE_SLIPPAGE_BPS=0
# CSE_NOTIONAL_PER_POSITION=1000
# CSE_WARMUP_CANDLES=25
# CSE_ORDER_BOOK_DEPTH=20
# CSE_RECONNECT_INITIAL_DELAY_SECONDS=1
# CSE_RECONNECT_MAX_DELAY_SECONDS=60
# CSE_RECONNECT_MULTIPLIER=2
# CSE_RECONNECT_JITTER_SECONDS=0.5
# CSE_RECONNECT_MAX_ATTEMPTS=
# CSE_LOCK_PATH=/var/lib/crypto-signal-engine/crypto-signal-engine.lock
# CSE_HEALTH_SNAPSHOT_PATH=/var/lib/crypto-signal-engine/health.json
# CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS=5

# Phase 12 — execution mode (default PAPER; only move to Stage B after
# weeks of stable local PAPER operation — see
# PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md):
# CSE_EXECUTION_MODE=PAPER
# CSE_ENABLE_TESTNET_EXECUTION=false
#
# Phase 12 — local read-only operations dashboard (loopback-only by
# default; see the PHASE12 doc's "phone/Tailscale operating model" for
# safe remote viewing — never set CSE_DASHBOARD_HOST to 0.0.0.0):
# CSE_DASHBOARD_ENABLED=true
# CSE_DASHBOARD_HOST=127.0.0.1
# CSE_DASHBOARD_PORT=8787
#
# 24/7 Ops v1 — Control Center admin token (pause/resume/stop on the
# dashboard, see PHASE8_UBUNTU_OPERATIONS.md and DECISIONS.md). Unset
# (default) = admin actions fully disabled, dashboard stays read-only
# exactly as before. This is a LOCAL admin secret, not an exchange
# credential — still keep it out of world-readable reach (this file is
# already chmod 640, chown root:csengine).
# CSE_DASHBOARD_ADMIN_TOKEN=
#
# 24/7 Ops v1 — outbound-only Telegram alerting. Deliberately set OUTSIDE
# this CSE_* namespace (same precedent as BINANCE_TESTNET_API_KEY/
# _SECRET below) since it is a credential, read only by
# crypto_signal_engine/ops/notifier.py, and never appears in
# AppConfig.summary() or any log line. Unset (default, either or both) =
# no notifier configured, every alerting call site becomes a silent
# no-op.
# CSE_TELEGRAM_BOT_TOKEN=
# CSE_TELEGRAM_CHAT_ID=
#
# NOTE: BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET /
# BINANCE_TESTNET_EXECUTION_DB_PATH are deliberately NOT listed in this
# file — they are Binance Spot TESTNET signing credentials (Phase 10/11),
# set separately from this shared template, and are read only by
# crypto_signal_engine/execution/. This file's CSE_* namespace (AppConfig)
# still carries zero credential fields, unconditionally.


=== FILE: deploy/systemd/crypto-signal-engine-backup.service ===
[Unit]
Description=Crypto Signal Engine — daily SQLite online backup (retention-managed)
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md
# Not a hard dependency (a missed live-service start must never block a
# backup of whatever durable state already exists on disk) — just orders
# it after, so a backup taken right after boot sees the freshest state.
After=crypto-signal-engine.service

[Service]
Type=oneshot
User=csengine
Group=csengine
WorkingDirectory=/opt/crypto-signal-engine
EnvironmentFile=/etc/crypto-signal-engine/env

# `scripts/backup_sqlite.py`'nin CORE mantığı (SQLite Online Backup API)
# DEĞİŞTİRİLMEDEN çağrılır (bkz. PHASE8_UBUNTU_OPERATIONS.md Bölüm 9).
# Tarih damgalı hedef dosya adı ve `--keep-last` retention burada,
# ExecStart seviyesinde eklenir — script'in KENDİSİ hâlâ tek bir
# (--source, --destination) çifti üzerinde çalışan, tarih/retention
# kavramından habersiz saf bir araçtır.
ExecStart=/bin/sh -c '\
    SRC="${CSE_DB_PATH:-/var/lib/crypto-signal-engine/paper_state.db}"; \
    STEM="$(basename "$SRC" .db)"; \
    /opt/crypto-signal-engine/venv/bin/python scripts/backup_sqlite.py \
        --source "$SRC" \
        --destination "/var/backups/crypto-signal-engine/${STEM}-$(date -u +%%Y%%m%%dT%%H%%M%%SZ).db" \
        --keep-last 14 \
    '

NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/backups/crypto-signal-engine /var/lib/crypto-signal-engine

StandardOutput=journal
StandardError=journal


=== FILE: deploy/systemd/crypto-signal-engine-backup.timer ===
[Unit]
Description=Daily timer for crypto-signal-engine-backup.service
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md

[Timer]
# Daily is the documented default schedule (see DECISIONS.md, "24/7 Ops
# v1" Karar) — a paper/testnet position book changes at trading pace,
# not multiple-times-a-day pace; a day of loss is an acceptable, bounded
# recovery-time-objective for a non-mainnet system that still keeps
# in-memory recovery + the durable SQLite file itself as the primary
# safety net (this timer is defense-in-depth against disk/host loss, not
# the primary durability mechanism).
OnCalendar=daily
# Spreads actual run time across a 30-minute window instead of every
# machine firing at exactly 00:00 UTC — irrelevant for a single host, but
# free and harmless, and avoids the backup always landing mid-candle at
# the exact same second every day.
RandomizedDelaySec=1800
# Catches up on a missed run (machine was off at the scheduled time) the
# next time the machine boots/is online — a skipped day of backups on a
# host that happened to be down is a real gap, not an acceptable one.
Persistent=true

[Install]
WantedBy=timers.target


=== FILE: deploy/systemd/crypto-signal-engine.service ===
[Unit]
Description=Crypto Signal Engine — public-data paper-trading service (Phase 8)
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=csengine
Group=csengine
WorkingDirectory=/opt/crypto-signal-engine
EnvironmentFile=/etc/crypto-signal-engine/env
ExecStart=/opt/crypto-signal-engine/venv/bin/python -m crypto_signal_engine.app run

# Graceful shutdown: SIGTERM is forwarded by systemd on stop/restart; the
# application handles it itself (idempotent, bounded) — no extra
# ExecStop/kill script is needed or wanted.
KillSignal=SIGTERM
TimeoutStopSec=30

# Bounded crash-restart policy (Section: "no unsafe restart loop"): restart
# on failure only (a clean exit 0 from a deliberate stop is NOT restarted),
# with a fixed delay, and a burst limiter so a persistently-crashing
# deployment stops retrying instead of looping forever.
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=300
StartLimitBurst=5

# 24/7 Ops v1 — watchdog: catches a HUNG process (event loop wedged),
# which a plain exit-code-based Restart=on-failure never detects. The
# app's health loop (default CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS=5s)
# sends a WATCHDOG=1 sd_notify heartbeat on every iteration (see
# crypto_signal_engine/ops/systemd_notify.py); 60s is 12x that default
# interval — generous headroom against an occasional slow tick (a GC
# pause, a slow snapshot write) while still bounding a genuine hang to
# at most one minute before systemd force-kills and restarts it (subject
# to the same Restart=on-failure/StartLimitBurst policy above). Works
# with Type=simple (unchanged) — WatchdogSec is independent of Type; it
# only requires WATCHDOG=1 notifications on $NOTIFY_SOCKET, which this
# unit's default NotifyAccess=main already permits from the main PID.
WatchdogSec=60

# Least-privilege hardening. The service never needs root, never needs to
# write outside its own data/lock/health directory, and makes no private
# Binance calls (public REST/WebSocket only).
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/crypto-signal-engine

# Logs go to journald by default (journalctl -u crypto-signal-engine -f).
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target


=== FILE: pyproject.toml ===
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[project]
name = "crypto-signal-engine"
version = "0.1.0"
description = "Binance Crypto Intelligence / Signal Engine — Phase 1 domain layer (no execution code)"
requires-python = ">=3.10"

# `websockets` is required only to run the REAL runtime (Phase 6/8's
# `RealWebSocketConnectionFactory`, lazy-imported on first real use) against
# live Binance PUBLIC market data. It is intentionally optional: the entire
# offline test suite runs without it (fake/mock transports only), so a fresh
# checkout can be installed and tested with zero extra dependencies. Install
# it for an actual Ubuntu deployment: `pip install .[runtime]`.
[project.optional-dependencies]
runtime = ["websockets>=12.0"]

# Console entry point for a `pip install`-ed deployment. Equivalent to
# `python -m crypto_signal_engine.app` (see PHASE8_UBUNTU_OPERATIONS.md).
[project.scripts]
crypto-signal-engine = "crypto_signal_engine.app:main"

[tool.setuptools.packages.find]
include = ["crypto_signal_engine*"]

[tool.pytest.ini_options]
testpaths = ["tests"]


=== FILE: research/__init__.py ===
"""
Pre-Audit Enhancement Pass — research/validation layer.

This package is ADDITIVE and lives OUTSIDE the `crypto_signal_engine/`
package boundary, in the same posture as `scripts/` and the accepted
`crypto_signal_engine/stability/` soak-testing package. It is a research
and validation harness, not part of the accepted Phase 1-12 production
runtime.

Hard rules that apply to every module in this package (see
PRE_AUDIT_ENHANCEMENTS.md for the full rationale):

- No second implementation of the trading strategy. Historical replay
  reuses `crypto_signal_engine.runtime.coordinator.RuntimeCoordinator`
  (specifically `bootstrap_candles()` / `ingest_candle()` /
  `ingest_order_book()`) exactly as `crypto_signal_engine.stability.
  harness.SoakHarness` already does for offline soak testing — this
  package only supplies historical data and orchestration, never
  feature/signal/consensus/risk/paper-accounting logic.
- No network calls beyond the already-accepted PUBLIC Binance REST
  historical-candle fetch (`providers/binance/rest.py::
  fetch_historical_candles`). No private/signed endpoints, no Testnet
  order submission, ever.
- No wall-clock dependency: all replay/research code drives a
  `crypto_signal_engine.providers.binance.clock.FixedClock` (accepted,
  unmodified) or otherwise takes explicit timestamps.
- No unseeded randomness: the only stochastic component (the Monte Carlo
  robustness screen) requires an explicit, caller-provided seed and
  never touches global `random` state.
- `ALLOW_LIVE_TRADING` is never imported, read, or mutated by this
  package.
"""

from __future__ import annotations


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


=== FILE: start_bot.sh ===
#!/bin/bash
# Sinyal botunu doğru ortam değişkenleriyle başlatır.
#
# Bu script, TESTNET signal-bridge/lifecycle'ının açık kalması için gereken
# ÜÇ config bayrağını (gizli olmayan) gömer. Ama iki GERÇEK KİMLİK BİLGİSİ
# (BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET) bu dosyada YOK ve
# OLMAYACAK — proje tasarımı gereği (SAFETY_INVARIANTS.md) hiçbir dosyada
# saklanmazlar. Bu scripti çalıştırmadan ÖNCE, AYNI terminalde, kendi
# elinle şunları export etmiş olman gerekir:
#
#   export BINANCE_TESTNET_API_KEY=...
#   export BINANCE_TESTNET_API_SECRET=...
#
# Eğer bunlar set değilse script aşağıda seni uyarıp DURACAK, sessizce
# PAPER-only moda düşmeyecek.
#
# Kullanım (proje kök dizininde):
#   chmod +x start_bot.sh   (sadece ilk seferde)
#   export BINANCE_TESTNET_API_KEY=...
#   export BINANCE_TESTNET_API_SECRET=...
#   ./start_bot.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ -z "${BINANCE_TESTNET_API_KEY:-}" || -z "${BINANCE_TESTNET_API_SECRET:-}" ]]; then
  echo "HATA: BINANCE_TESTNET_API_KEY ve/veya BINANCE_TESTNET_API_SECRET set değil."
  echo "Bu ikisini export ETMEDEN bu scripti çalıştırma — aksi halde bot"
  echo "sessizce TESTNET bridge olmadan (basit PAPER modda) başlar."
  echo ""
  echo "  export BINANCE_TESTNET_API_KEY=..."
  echo "  export BINANCE_TESTNET_API_SECRET=..."
  echo "  ./start_bot.sh"
  exit 1
fi

export CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET
export CSE_ENABLE_TESTNET_EXECUTION=true
export CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=true

echo "Başlatılıyor — CSE_EXECUTION_MODE=$CSE_EXECUTION_MODE CSE_ENABLE_TESTNET_EXECUTION=$CSE_ENABLE_TESTNET_EXECUTION CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=$CSE_ENABLE_SIGNAL_TESTNET_BRIDGE"
exec .venv/bin/python -m crypto_signal_engine.app run


=== FILE: tests/__init__.py ===



=== FILE: tests/binance_fakes.py ===
"""
Phase 2 testleri için paylaşılan fake transport implementasyonları.

Kural (Bölüm 22): "Unit testlerde gerçek Binance ağına bağımlı olma."
Bu modül, `HttpClient` ve `WebSocketConnectionFactory` Protocol'lerinin
sahte implementasyonlarını sağlar; hiçbir gerçek network I/O yapılmaz.
"""

from __future__ import annotations

import asyncio
import json

from crypto_signal_engine.errors import RateLimitError, TransportError
from crypto_signal_engine.providers.binance.transport import HttpResponse


class FakeHttpClient:
    """Sıralı, scripted yanıtlar döndüren sahte HTTP client.

    `responses`: her çağrıda sırayla döndürülecek (status_code, body) veya
    Exception instance'larının listesi. Liste tükenirse son eleman tekrar
    kullanılır (sabit yanıt senaryoları için kolaylık).
    """

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    async def get(self, url: str, params: dict, timeout_seconds: float) -> HttpResponse:
        self.calls.append((url, dict(params)))
        if not self._responses:
            raise TransportError("FakeHttpClient: script tükendi")
        item = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        if isinstance(item, Exception):
            raise item
        status_code, body = item
        return HttpResponse(status_code=status_code, body=body, headers={})


def json_response(payload) -> tuple[int, str]:
    return (200, json.dumps(payload))


class FakeWebSocketConnection:
    """Sıralı mesajlar/hatalar oynatan sahte WebSocket bağlantısı.

    `script`: her `recv()` çağrısında sırayla döndürülecek string mesajlar
    veya fırlatılacak Exception instance'ları.
    """

    def __init__(self, script: list) -> None:
        self._script = list(script)
        self.closed = False
        self.sent: list[str] = []

    async def recv(self) -> str:
        if not self._script:
            # Script tükendiğinde sonsuza kadar askıda kalır (test,
            # dışarıdan cancel edene kadar) — gerçek bir WS'in "daha fazla
            # mesaj yok ama bağlantı açık" durumunu simüle eder.
            await asyncio.Event().wait()
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


class FakeWebSocketConnectionFactory:
    """Her `connect()` çağrısında sırayla bir sonraki `FakeWebSocketConnection`'ı
    (veya exception'ı) döndüren sahte fabrika — reconnect senaryolarını
    test etmek için art arda farklı bağlantılar sağlanabilir."""

    def __init__(self, connections: list) -> None:
        self._connections = list(connections)
        self.connect_calls: list[str] = []

    async def connect(self, url: str):
        self.connect_calls.append(url)
        if not self._connections:
            raise TransportError("FakeWebSocketConnectionFactory: script tükendi")
        item = self._connections.pop(0) if len(self._connections) > 1 else self._connections[0]
        if isinstance(item, Exception):
            raise item
        return item


=== FILE: tests/conftest.py ===
"""
Paylaşılan test fixture/yardımcıları.

Kural (Gate 29 ruhu): dış bir pytest-asyncio bağımlılığı eklemek, offline/
reproducible acceptance hedefini (yeni bir paketin önceden kurulu olmasını
gerektirebilir) riske atar. Bunun yerine minimal, stdlib-only bir
`run_async` yardımcı fonksiyonu kullanılır.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import TypeVar

T = TypeVar("T")


def run_async(coro: Coroutine[None, None, T]) -> T:
    """Bir coroutine'i senkron bir test fonksiyonu içinden çalıştırır."""
    return asyncio.run(coro)


=== FILE: tests/execution_fakes.py ===
"""Faz 10 testleri için paylaşılan fake transport — gerçek Binance TESTNET
ağına bağımlılık YOK (bkz. tests/binance_fakes.py ile AYNI disiplin)."""

from __future__ import annotations

import json

from crypto_signal_engine.execution.errors import ExecutionTransportError
from crypto_signal_engine.execution.testnet_client import TestnetHttpResponse


class FakeTestnetHttpClient:
    """Sıralı, scripted GET/POST yanıtları döndüren sahte TESTNET
    transport'u. `get_responses`/`post_responses`: her çağrıda sırayla
    döndürülecek (status_code, body-dict) veya Exception listesi."""

    def __init__(self, get_responses: list | None = None, post_responses: list | None = None) -> None:
        self._get_responses = list(get_responses or [])
        self._post_responses = list(post_responses or [])
        self.get_calls: list[tuple[str, dict, dict]] = []
        self.post_calls: list[tuple[str, dict, dict]] = []

    async def get(self, url: str, params: dict, headers: dict, timeout_seconds: float) -> TestnetHttpResponse:
        self.get_calls.append((url, dict(params), dict(headers)))
        return self._pop(self._get_responses)

    async def post(self, url: str, params: dict, headers: dict, timeout_seconds: float) -> TestnetHttpResponse:
        self.post_calls.append((url, dict(params), dict(headers)))
        return self._pop(self._post_responses)

    @staticmethod
    def _pop(queue: list) -> TestnetHttpResponse:
        if not queue:
            raise ExecutionTransportError("FakeTestnetHttpClient: script tükendi")
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        status_code, body = item
        return TestnetHttpResponse(status_code=status_code, body=body)


def json_response(payload: object, status_code: int = 200) -> tuple[int, str]:
    return (status_code, json.dumps(payload))


=== FILE: tests/research_fakes.py ===
"""
Deterministic fixtures shared by the `research/` test suite. Mirrors the
existing `tests/binance_fakes.py`/`tests/runtime_fakes.py` convention:
never touches the network, never imports `execution/`, purely
synchronous/deterministic data generation.

Candle grid: all timeframes share ONE fixed epoch (`_EPOCH`), and every
supported duration (1/5/15/60 minutes) evenly divides every larger one,
so a boundary shared across timeframes is always generated identically
regardless of which timeframe's series is asked for — exactly mirroring
real, epoch-aligned Binance kline boundaries.

`close_time` uses Binance's real REST convention
(`open_time + duration - 1ms`, see `research/data_quality.py`'s own
`_CLOSE_TIME_CONVENTION_TOLERANCE` docstring) so the test suite exercises
the actual production code path, not an idealized one.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle

TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}

EPOCH = datetime(2020, 1, 1, tzinfo=timezone.utc)
_ONE_MS = timedelta(milliseconds=1)


def deterministic_candle(
    symbol: str, timeframe: Timeframe, open_time: datetime, *, base_price: float = 100.0
) -> Candle:
    duration = TIMEFRAME_DURATIONS[timeframe]
    counter = round((open_time - EPOCH) / duration)
    price = base_price + 5.0 * math.sin(counter / 7.0) + (counter % 3) * 0.01
    volume = 10.0 + (counter % 4)
    return Candle(
        symbol=symbol, timeframe=timeframe, open_time=open_time,
        close_time=open_time + duration - _ONE_MS,
        open=price, high=price + 1.0, low=price - 1.0, close=price, volume=volume, is_closed=True,
    )


def align_to_grid(value: datetime, timeframe: Timeframe) -> datetime:
    """Rounds `value` UP to the next boundary on this timeframe's shared
    epoch-aligned grid."""
    duration = TIMEFRAME_DURATIONS[timeframe]
    steps = math.ceil((value - EPOCH) / duration)
    return EPOCH + duration * steps


def deterministic_series(
    symbol: str, timeframe: Timeframe, start: datetime, end: datetime, *, base_price: float = 100.0
) -> list[Candle]:
    """Chronological, contiguous, gap-free, duplicate-free series
    covering `[start, end)` on the shared grid."""
    duration = TIMEFRAME_DURATIONS[timeframe]
    open_time = align_to_grid(start, timeframe)
    candles: list[Candle] = []
    while open_time < end:
        candles.append(deterministic_candle(symbol, timeframe, open_time, base_price=base_price))
        open_time += duration
    return candles


class FakeHistoricalCandleSource:
    """Structurally satisfies `research.replay.HistoricalCandleSource` —
    an async `fetch_historical_candles(symbol, timeframe, start, end)`
    method, fully offline/deterministic. Records every call it received
    (`self.calls`) so tests can assert replay never over/under-fetches
    and never touches anything beyond this fake."""

    def __init__(self, *, base_price: float = 100.0) -> None:
        self._base_price = base_price
        self.calls: list[tuple[str, Timeframe, datetime, datetime]] = []

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        self.calls.append((symbol, timeframe, start, end))
        return deterministic_series(symbol, timeframe, start, end, base_price=self._base_price)


=== FILE: tests/runtime_fakes.py ===
"""
Faz 6 test fake'leri ve yardımcıları — tamamen offline/deterministic.
Gerçek network, gerçek wall-clock sleep, gerçek Binance bağlantısı YOK.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.providers.base import ConnectionState, ProviderHealthSnapshot

_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}


def timeframe_duration(timeframe: Timeframe) -> timedelta:
    return _DURATIONS[timeframe]


def make_candle(
    symbol: str, timeframe: Timeframe, open_time: datetime, *, price: float = 100.0,
    is_closed: bool = True, volume: float = 10.0,
) -> Candle:
    duration = _DURATIONS[timeframe]
    return Candle(
        symbol=symbol, timeframe=timeframe, open_time=open_time, close_time=open_time + duration,
        open=price, high=price + 1.0, low=price - 1.0, close=price, volume=volume, is_closed=is_closed,
    )


def make_candle_series_ending_at(
    symbol: str, timeframe: Timeframe, end_close_time: datetime, count: int,
    *, base_price: float = 100.0, step: float = 0.05,
) -> list[Candle]:
    """`count` ardışık, kapalı candle üretir; SONUNCUSUNUN `close_time`'ı
    TAM OLARAK `end_close_time`'a eşittir — bootstrap testlerinde birden
    çok timeframe'i ORTAK bir referans zamanına hizalamak için kullanılır.

    Volume KASITLI OLARAK sabit DEĞİLDİR (`10.0 + (i % 4)`) — sabit volume,
    `VOLUME_ZSCORE_20` gibi bazı feature'ların std-sıfır durumunda
    `FeatureCalculationError` fırlatmasına yol açar (bu, warm-up eksikliği
    DEĞİL gerçek bir matematiksel tanımsızlıktır, bkz. Faz 3); gerçekçi
    piyasa verisinde volume neredeyse hiçbir zaman sabit değildir."""
    duration = _DURATIONS[timeframe]
    candles = []
    for i in range(count):
        open_time = end_close_time - duration * (count - i)
        price = base_price + step * i
        candles.append(make_candle(symbol, timeframe, open_time, price=price, volume=10.0 + (i % 4)))
    return candles


def make_order_book(symbol: str, timestamp: datetime, *, mid: float = 100.0, last_update_id: int = 1) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol=symbol,
        timestamp=timestamp,
        bids=(OrderBookLevel(price=mid - 0.5, quantity=5.0), OrderBookLevel(price=mid - 1.0, quantity=5.0)),
        asks=(OrderBookLevel(price=mid + 0.5, quantity=5.0), OrderBookLevel(price=mid + 1.0, quantity=5.0)),
        last_update_id=last_update_id,
    )


class FakeLiveDataProvider:
    """`LiveDataProvider` sözleşmesini (duck-typing ile) karşılayan,
    tamamen scripted/deterministic bir test double'ı — gerçek network YOK.

    `stream_candles`/`stream_order_book`, script tükendiğinde gerçek
    provider gibi sonsuza kadar bloklar (`asyncio.Event().wait()`) — bir
    `run()`/`stop()` testi bu görevi `stop()` ile iptal ederek sonlandırır.
    """

    def __init__(
        self,
        *,
        candle_scripts: dict[tuple[str, Timeframe], list[Candle]] | None = None,
        order_book_scripts: dict[str, list[OrderBookSnapshot]] | None = None,
        historical_candles: dict[tuple[str, Timeframe], list[Candle]] | None = None,
    ) -> None:
        self._candle_scripts = candle_scripts or {}
        self._order_book_scripts = order_book_scripts or {}
        self._historical_candles = historical_candles or {}
        self.closed = False
        self.fetch_calls: list[tuple[str, Timeframe, datetime, datetime]] = []

    async def stream_candles(self, symbol: str, timeframe: Timeframe) -> AsyncIterator[Candle]:
        for candle in self._candle_scripts.get((symbol, timeframe), []):
            yield candle
        await asyncio.Event().wait()

    async def stream_order_book(self, symbol: str, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        for snapshot in self._order_book_scripts.get(symbol, []):
            yield snapshot
        await asyncio.Event().wait()

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        self.fetch_calls.append((symbol, timeframe, start, end))
        return list(self._historical_candles.get((symbol, timeframe), []))

    def health(self) -> ProviderHealthSnapshot:
        return ProviderHealthSnapshot(
            connection_state=ConnectionState.CONNECTED, last_message_at=None, reconnect_count=0, symbol="BTCUSDT"
        )

    def health_for(self, key: str) -> ProviderHealthSnapshot | None:
        return None

    async def close(self) -> None:
        self.closed = True


=== FILE: tests/test_adaptive_challenger.py ===
"""Adaptive Intelligence v1, step 5 — challenger generation tests."""

from __future__ import annotations

from datetime import datetime, timezone

from adaptive.challenger import (
    ChallengerGenerationConfig,
    _FIELD_BOUNDS,
    generate_challengers,
)
from adaptive.policy import PolicySnapshot

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_CHAMPION = PolicySnapshot(
    version_id="champion-1", stop_atr_multiple=2.0, take_profit_atr_multiple=4.0,
    trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=2.0, max_hold_hours=48.0,
    created_at=_NOW, provenance="test champion",
)


class TestGenerateChallengers:
    def test_deterministic_for_same_seed(self) -> None:
        a = generate_challengers(_CHAMPION, seed=42, created_at=_NOW)
        b = generate_challengers(_CHAMPION, seed=42, created_at=_NOW)
        assert a == b

    def test_different_seed_produces_different_challengers(self) -> None:
        a = generate_challengers(_CHAMPION, seed=1, created_at=_NOW)
        b = generate_challengers(_CHAMPION, seed=2, created_at=_NOW)
        assert a != b

    def test_produces_requested_count(self) -> None:
        challengers = generate_challengers(
            _CHAMPION, seed=1, config=ChallengerGenerationConfig(count=5), created_at=_NOW,
        )
        assert len(challengers) == 5

    def test_every_challenger_within_perturbation_band_and_bounds(self) -> None:
        pct = 0.15
        challengers = generate_challengers(
            _CHAMPION, seed=7, config=ChallengerGenerationConfig(perturbation_pct=pct, count=20), created_at=_NOW,
        )
        for challenger in challengers:
            for field, (low, high) in _FIELD_BOUNDS.items():
                base = getattr(_CHAMPION, field)
                value = getattr(challenger, field)
                unclamped_low = base * (1 - pct)
                unclamped_high = base * (1 + pct)
                # Either strictly within the perturbation band, or pinned
                # exactly to a sanity bound (never beyond either).
                assert low <= value <= high
                assert (unclamped_low - 1e-9) <= value <= (unclamped_high + 1e-9) or value in (low, high)

    def test_challengers_are_valid_exit_policy_configs(self) -> None:
        # PolicySnapshot.__post_init__ already reuses ExitPolicyConfig's
        # own validation -- constructing each challenger successfully
        # (done inside generate_challengers) is itself the proof; this
        # just double-checks the round trip explicitly.
        for challenger in generate_challengers(_CHAMPION, seed=3, created_at=_NOW):
            config = challenger.to_exit_policy_config()
            assert config.stop_atr_multiple > 0
            assert config.max_hold_hours > 0

    def test_challenger_version_ids_are_unique_and_reference_champion(self) -> None:
        challengers = generate_challengers(
            _CHAMPION, seed=3, config=ChallengerGenerationConfig(count=4), created_at=_NOW,
        )
        version_ids = [c.version_id for c in challengers]
        assert len(set(version_ids)) == len(version_ids)
        assert all(_CHAMPION.version_id in v for v in version_ids)

    def test_rejects_out_of_range_perturbation_pct(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            ChallengerGenerationConfig(perturbation_pct=0.0)
        with pytest.raises(ValueError):
            ChallengerGenerationConfig(perturbation_pct=1.0)

    def test_rejects_non_positive_count(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            ChallengerGenerationConfig(count=0)

    def test_never_mutates_global_random_state(self) -> None:
        import random

        random.seed(12345)
        state_before = random.getstate()
        generate_challengers(_CHAMPION, seed=99, config=ChallengerGenerationConfig(count=10), created_at=_NOW)
        assert random.getstate() == state_before


=== FILE: tests/test_adaptive_cycle.py ===
"""Adaptive Intelligence v1, step 13 — run_adaptive_cycle() tests. Fully
offline, reusing the FakeHistoricalCandleSource fixture already
established for research/'s and adaptive/'s own evaluation tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from adaptive.challenger import ChallengerGenerationConfig
from adaptive.cycle import run_adaptive_cycle
from adaptive.decision import EvidenceSummary
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

SYMBOL = "BTCUSDT"


@pytest.fixture()
def store(tmp_path) -> AdaptiveStore:  # noqa: ANN001
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


def _window_config() -> WindowConfig:
    return WindowConfig(discovery_window_size=timedelta(hours=6), confirmation_window_size=timedelta(hours=2))


class TestBootstrap:
    def test_first_ever_cycle_bootstraps_default_champion(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=1)  # confirmation window (2h) has not elapsed from anchor yet

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now,
            )

        report = run_async(scenario())
        assert report.champion_version_id == "policy-champion-default"
        assert store.current_champion() is not None
        assert store.current_champion().version_id == "policy-champion-default"

    def test_second_cycle_reuses_existing_champion_not_a_new_bootstrap(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()

        async def scenario():
            first = await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=EPOCH + timedelta(hours=1),
            )
            second = await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=EPOCH + timedelta(hours=2),
            )
            return first, second

        first, second = run_async(scenario())
        assert first.champion_version_id == second.champion_version_id
        assert len(store.champion_history()) == 1  # still just the one bootstrap promotion


class TestWindowNotReadyYet:
    def test_no_windows_means_no_replay_work_and_no_decisions(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(minutes=30)  # confirmation window (2h) far from elapsed

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now,
            )

        report = run_async(scenario())
        assert report.windows is None
        assert report.discovery_survivors == ()
        assert report.decisions == ()
        assert source.calls == []  # zero replay work when there's nothing new to evaluate


class TestChallengerGeneration:
    def test_generates_and_persists_challengers_every_cycle_windows_ready(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)  # discovery(6h)+confirmation(2h) elapsed

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now, challenger_config=ChallengerGenerationConfig(count=3),
            )

        report = run_async(scenario())
        assert len(report.challengers_generated) == 3
        for version_id in report.challengers_generated:
            assert store.load_policy_version(version_id) is not None

    def test_confirmation_cursor_advances_exactly_once_per_ready_cycle(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now, challenger_config=ChallengerGenerationConfig(count=2),
            )

        report = run_async(scenario())
        assert report.windows is not None
        cursor = store.get_cursor("last_confirmation_end")
        assert cursor == report.windows.confirmation.end.isoformat()


class TestSharedFunctionUsedByBothCallers:
    """Static proof that the scheduler and the CLI cannot silently
    diverge: both import and call this exact function, never a
    re-implementation. (Behavioral proof lives in
    tests/test_adaptive_scheduler.py and the CLI script's own smoke
    test; this just confirms run_adaptive_cycle is a plain, directly
    importable async function with a stable name.)"""

    def test_run_adaptive_cycle_is_a_coroutine_function(self) -> None:
        import inspect

        assert inspect.iscoroutinefunction(run_adaptive_cycle)


class TestChampionLiveEvidenceProviderIsOptional:
    def test_missing_provider_defaults_to_empty_shadow_evidence_never_crashes(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now, champion_live_evidence_provider=None,
            )

        report = run_async(scenario())  # must not raise
        assert report is not None

    def test_provider_is_consulted_with_the_champions_version_id_once_windows_are_ready(self, store: AdaptiveStore) -> None:
        """`champion_live_evidence_provider` is called with the CURRENT
        CHAMPION's `version_id` (never a challenger's) -- it supplies the
        champion's real live evidence, the comparison baseline for
        step 7's shadow-evidence class."""
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)  # windows ready
        calls: list[str] = []

        def provider(version_id: str) -> EvidenceSummary:
            calls.append(version_id)
            return EvidenceSummary(sample_count=0, win_rate=None, total_gross_pnl_per_unit=0.0, max_drawdown_per_unit=None)

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now, champion_live_evidence_provider=provider,
            )

        report = run_async(scenario())
        assert calls == [report.champion_version_id]


class TestDriftSignalWiring:
    """Fix for gap #1 (independent-verification round): `run_adaptive_
    cycle()` must actually call `summarize_drift()` and persist its
    result on the cycle's decision-log entry -- not merely have
    `adaptive.drift_signal` exist as dead code exercised only by its own
    unit test."""

    def _seed_shadow_eligible_challenger(self, store: AdaptiveStore, *, now) -> None:
        from adaptive.policy import snapshot_from_exit_policy_config
        from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig

        seeded = snapshot_from_exit_policy_config(
            ExitPolicyConfig(max_hold_hours=1.0), version_id="drift-test-challenger",
            provenance="test", created_at=now,
        )
        store.record_policy_version(seeded)
        store.mark_shadow_eligible("drift-test-challenger", now=now)

    def test_persisted_decision_log_entry_contains_a_real_drift_summary(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)  # windows ready
        self._seed_shadow_eligible_challenger(store, now=now)

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now,
            )

        report = run_async(scenario())
        assert len(report.decisions) >= 1
        assert report.drift_summary is not None

        recent = store.recent_decisions()
        assert len(recent) >= 1
        drift_note = recent[0]["drift_note"]
        assert drift_note is not None
        assert isinstance(drift_note, str) and drift_note != ""

    def test_live_cycle_results_provider_feeds_the_drift_comparison(self, store: AdaptiveStore) -> None:
        from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
        from crypto_signal_engine.domain.models import Signal
        from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
        from crypto_signal_engine.runtime.models import RuntimeCycleResult

        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(hours=10)
        self._seed_shadow_eligible_challenger(store, now=now)

        signal = Signal(
            symbol=SYMBOL, timestamp=now, context_id="paper-ctx-1", score=0.9, confidence=0.9,
            risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
            supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test",
        )
        paper_result = PaperTradingResult(
            symbol=SYMBOL,
            position=PaperPosition(symbol=SYMBOL, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0, realized_pnl=0.0, updated_at=now),
            orders=(), fills=(), idempotent_replay=False,
        )
        live_cycle_result = RuntimeCycleResult(symbol=SYMBOL, evaluated=True, signal=signal, paper_result=paper_result, generated_at=now)

        def live_provider():
            return (live_cycle_result,)

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now, live_cycle_results_provider=live_provider,
            )

        report = run_async(scenario())
        assert report.drift_summary is not None
        # The live-provided context_id never appears in the discovery
        # replay's own signal stream -> MISSING_IN_REPLAY is expected,
        # proving the provider's data actually reached the comparison.
        assert report.drift_summary.total_compared >= 1

    def test_no_windows_means_no_drift_summary(self, store: AdaptiveStore) -> None:
        source = FakeHistoricalCandleSource()
        now = EPOCH + timedelta(minutes=30)  # confirmation window far from elapsed

        async def scenario():
            return await run_adaptive_cycle(
                store=store, candle_source=source, symbols=(SYMBOL,), window_config=_window_config(),
                anchor=EPOCH, seed=1, now=now,
            )

        report = run_async(scenario())
        assert report.drift_summary is None


=== FILE: tests/test_adaptive_decision.py ===
"""Adaptive Intelligence v1, steps 9-10 — promotion gate / decision
function tests, including the REQUIRED test: a challenger with
insufficient shadow evidence is rejected even if its discovery metrics
look strictly better."""

from __future__ import annotations

import pytest

from adaptive.decision import (
    EMPTY_EVIDENCE,
    MAX_DRAWDOWN_TOLERANCE_MULTIPLIER,
    MIN_CONFIRMATION_TRADES,
    MIN_DISCOVERY_TRADES,
    MIN_SHADOW_TRADES,
    Decision,
    EvidenceSummary,
    decide,
    shadow_evidence_from_outcomes,
)
from adaptive.shadow import ShadowOutcome
from crypto_signal_engine.execution.lifecycle import ExitReason
from datetime import datetime, timezone

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _evidence(**overrides) -> EvidenceSummary:
    defaults = dict(sample_count=50, win_rate=0.6, total_gross_pnl_per_unit=10.0, max_drawdown_per_unit=2.0)
    defaults.update(overrides)
    return EvidenceSummary(**defaults)


def _good_champion() -> dict:
    return dict(
        discovery_champion=_evidence(total_gross_pnl_per_unit=5.0, max_drawdown_per_unit=2.0),
        confirmation_champion=_evidence(sample_count=15, total_gross_pnl_per_unit=2.0, max_drawdown_per_unit=1.0),
        shadow_champion=_evidence(sample_count=8, total_gross_pnl_per_unit=1.0, max_drawdown_per_unit=0.5),
    )


class TestInsufficientEvidenceIsDefault:
    """THE mission-required test."""

    def test_excellent_discovery_and_confirmation_but_insufficient_shadow_is_rejected(self) -> None:
        champion = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=_evidence(sample_count=1000, total_gross_pnl_per_unit=500.0, max_drawdown_per_unit=1.0),
            discovery_champion=champion["discovery_champion"],
            confirmation_challenger=_evidence(sample_count=100, total_gross_pnl_per_unit=100.0, max_drawdown_per_unit=1.0),
            confirmation_champion=champion["confirmation_champion"],
            # Strictly better P&L than champion, but sample_count below MIN_SHADOW_TRADES.
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES - 1, total_gross_pnl_per_unit=1000.0, max_drawdown_per_unit=0.1),
            shadow_champion=champion["shadow_champion"],
        )
        assert result.decision is Decision.INSUFFICIENT_EVIDENCE
        assert "shadow" in result.reason.lower()

    def test_zero_evidence_everywhere_is_insufficient(self) -> None:
        result = decide(
            "challenger-1",
            discovery_challenger=EMPTY_EVIDENCE, discovery_champion=EMPTY_EVIDENCE,
            confirmation_challenger=EMPTY_EVIDENCE, confirmation_champion=EMPTY_EVIDENCE,
            shadow_challenger=EMPTY_EVIDENCE, shadow_champion=EMPTY_EVIDENCE,
        )
        assert result.decision is Decision.INSUFFICIENT_EVIDENCE

    def test_insufficient_discovery_alone_blocks_promotion_regardless_of_the_rest(self) -> None:
        champion = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=_evidence(sample_count=MIN_DISCOVERY_TRADES - 1, total_gross_pnl_per_unit=1000.0),
            discovery_champion=champion["discovery_champion"],
            confirmation_challenger=_evidence(sample_count=MIN_CONFIRMATION_TRADES, total_gross_pnl_per_unit=1000.0),
            confirmation_champion=champion["confirmation_champion"],
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES, total_gross_pnl_per_unit=1000.0),
            shadow_champion=champion["shadow_champion"],
        )
        assert result.decision is Decision.INSUFFICIENT_EVIDENCE
        assert "discovery" in result.reason.lower()

    def test_insufficient_confirmation_alone_blocks_promotion(self) -> None:
        champion = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=_evidence(sample_count=MIN_DISCOVERY_TRADES, total_gross_pnl_per_unit=1000.0),
            discovery_champion=champion["discovery_champion"],
            confirmation_challenger=_evidence(sample_count=MIN_CONFIRMATION_TRADES - 1, total_gross_pnl_per_unit=1000.0),
            confirmation_champion=champion["confirmation_champion"],
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES, total_gross_pnl_per_unit=1000.0),
            shadow_champion=champion["shadow_champion"],
        )
        assert result.decision is Decision.INSUFFICIENT_EVIDENCE
        assert "confirmation" in result.reason.lower()


class TestPromotionRequiresAllThreeEvidenceClasses:
    def test_promotes_when_all_three_improve(self) -> None:
        champion = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=_evidence(sample_count=MIN_DISCOVERY_TRADES, total_gross_pnl_per_unit=50.0, max_drawdown_per_unit=2.0),
            discovery_champion=champion["discovery_champion"],
            confirmation_challenger=_evidence(sample_count=MIN_CONFIRMATION_TRADES, total_gross_pnl_per_unit=20.0, max_drawdown_per_unit=1.0),
            confirmation_champion=champion["confirmation_champion"],
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES, total_gross_pnl_per_unit=10.0, max_drawdown_per_unit=0.5),
            shadow_champion=champion["shadow_champion"],
        )
        assert result.decision is Decision.PROMOTE

    def test_keeps_champion_when_shadow_alone_regresses(self) -> None:
        """Not one cherry-picked metric: discovery and confirmation both
        strictly improve, but shadow does not -- must NOT promote."""
        champion = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=_evidence(sample_count=MIN_DISCOVERY_TRADES, total_gross_pnl_per_unit=50.0, max_drawdown_per_unit=2.0),
            discovery_champion=champion["discovery_champion"],
            confirmation_challenger=_evidence(sample_count=MIN_CONFIRMATION_TRADES, total_gross_pnl_per_unit=20.0, max_drawdown_per_unit=1.0),
            confirmation_champion=champion["confirmation_champion"],
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES, total_gross_pnl_per_unit=0.1, max_drawdown_per_unit=0.5),
            shadow_champion=champion["shadow_champion"],  # champion total_gross_pnl_per_unit=1.0 > 0.1
        )
        assert result.decision is Decision.KEEP_CHAMPION
        assert "shadow=False" in result.reason

    def test_worse_drawdown_beyond_tolerance_blocks_promotion_even_with_higher_pnl(self) -> None:
        champion_evidence = _evidence(total_gross_pnl_per_unit=5.0, max_drawdown_per_unit=1.0)
        challenger_evidence = _evidence(
            sample_count=MIN_DISCOVERY_TRADES, total_gross_pnl_per_unit=100.0,
            max_drawdown_per_unit=1.0 * MAX_DRAWDOWN_TOLERANCE_MULTIPLIER + 0.01,
        )
        good = _good_champion()
        result = decide(
            "challenger-1",
            discovery_challenger=challenger_evidence, discovery_champion=champion_evidence,
            confirmation_challenger=_evidence(sample_count=MIN_CONFIRMATION_TRADES, total_gross_pnl_per_unit=100.0),
            confirmation_champion=good["confirmation_champion"],
            shadow_challenger=_evidence(sample_count=MIN_SHADOW_TRADES, total_gross_pnl_per_unit=100.0),
            shadow_champion=good["shadow_champion"],
        )
        assert result.decision is Decision.KEEP_CHAMPION
        assert "discovery=False" in result.reason


class TestShadowEvidenceFromOutcomes:
    def test_empty_outcomes_returns_empty_evidence(self) -> None:
        assert shadow_evidence_from_outcomes(()) == EMPTY_EVIDENCE

    def test_aggregates_win_rate_and_total_pnl(self) -> None:
        outcomes = (
            ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT, exit_price=110.0, exit_time=NOW, gross_pnl_per_unit=10.0),
            ShadowOutcome(exit_reason=ExitReason.STOP_LOSS, exit_price=95.0, exit_time=NOW, gross_pnl_per_unit=-5.0),
        )
        evidence = shadow_evidence_from_outcomes(outcomes)
        assert evidence.sample_count == 2
        assert evidence.win_rate == 0.5
        assert evidence.total_gross_pnl_per_unit == 5.0
        assert evidence.max_drawdown_per_unit is not None


class TestEvidenceFromPnls:
    def test_empty_returns_empty_evidence(self) -> None:
        from adaptive.decision import evidence_from_pnls

        assert evidence_from_pnls([]) == EMPTY_EVIDENCE

    def test_aggregates_plain_float_sequence(self) -> None:
        from adaptive.decision import evidence_from_pnls

        evidence = evidence_from_pnls([10.0, -5.0, 3.0])
        assert evidence.sample_count == 3
        assert evidence.win_rate == 2 / 3
        assert evidence.total_gross_pnl_per_unit == 8.0

    def test_shadow_evidence_from_outcomes_matches_direct_pnl_aggregation(self) -> None:
        from adaptive.decision import evidence_from_pnls

        outcomes = (
            ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT, exit_price=110.0, exit_time=NOW, gross_pnl_per_unit=10.0),
            ShadowOutcome(exit_reason=ExitReason.STOP_LOSS, exit_price=95.0, exit_time=NOW, gross_pnl_per_unit=-5.0),
        )
        assert shadow_evidence_from_outcomes(outcomes) == evidence_from_pnls([10.0, -5.0])


class TestEvidenceSummaryValidation:
    def test_rejects_negative_sample_count(self) -> None:
        with pytest.raises(ValueError):
            EvidenceSummary(sample_count=-1, win_rate=None, total_gross_pnl_per_unit=0.0, max_drawdown_per_unit=None)


=== FILE: tests/test_adaptive_drift_signal.py ===
"""Adaptive Intelligence v1, step 11 — drift awareness tests. Reuses the
exact cycle-result fixture pattern from
tests/test_execution_lifecycle_replay_sanity.py / tests/test_research_drift.py."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from adaptive.drift_signal import summarize_drift
from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
from crypto_signal_engine.runtime.models import RuntimeCycleResult

SYMBOL = "BTCUSDT"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _signal(ts: datetime, context_id: str, *, score: float = 0.9) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=ts, context_id=context_id, score=score, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test",
    )


def _cycle_result(ts: datetime, context_id: str, *, score: float = 0.9) -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=SYMBOL,
        position=PaperPosition(
            symbol=SYMBOL, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0,
            realized_pnl=0.0, updated_at=ts,
        ),
        orders=(), fills=(), idempotent_replay=False,
    )
    return RuntimeCycleResult(
        symbol=SYMBOL, evaluated=True, signal=_signal(ts, context_id, score=score), paper_result=paper_result,
        generated_at=ts,
    )


class TestSummarizeDrift:
    def test_matched_when_scores_identical(self) -> None:
        replay = (_cycle_result(NOW, "ctx-1", score=0.9),)
        paper = (_cycle_result(NOW, "ctx-1", score=0.9),)
        summary = summarize_drift(replay, paper)
        assert summary.finding_counts == {"MATCHED": 1}
        assert summary.total_compared == 1

    def test_score_mismatch_detected(self) -> None:
        replay = (_cycle_result(NOW, "ctx-1", score=0.9),)
        paper = (_cycle_result(NOW, "ctx-1", score=0.5),)
        summary = summarize_drift(replay, paper)
        assert summary.finding_counts == {"SCORE_MISMATCH": 1}

    def test_missing_in_paper_detected(self) -> None:
        replay = (_cycle_result(NOW, "ctx-1"),)
        summary = summarize_drift(replay, ())
        assert summary.finding_counts == {"MISSING_IN_PAPER": 1}

    def test_missing_in_replay_detected(self) -> None:
        paper = (_cycle_result(NOW, "ctx-1"),)
        summary = summarize_drift((), paper)
        assert summary.finding_counts == {"MISSING_IN_REPLAY": 1}

    def test_empty_inputs_produce_empty_summary(self) -> None:
        summary = summarize_drift((), ())
        assert summary.total_compared == 0
        assert summary.as_log_note()  # never raises, never empty string

    def test_log_note_never_implies_it_affected_a_decision(self) -> None:
        summary = summarize_drift((_cycle_result(NOW, "ctx-1"),), ())
        note = summary.as_log_note()
        assert "salt-gözlemsel" in note or "auxiliary" in note.lower()


=== FILE: tests/test_adaptive_evaluation.py ===
"""Adaptive Intelligence v1, step 7 — discovery/confirmation evaluation
tests. Fully offline, reusing the exact fakes `research/`'s and
`lifecycle_replay_sanity`'s own test suites already use."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from adaptive.evaluation import (
    DISCOVERY_FEE_BPS,
    DISCOVERY_SLIPPAGE_BPS,
    default_discovery_replay_config,
    evaluate_challenger,
    evaluate_challengers_against_window,
    run_shared_replay,
)
from adaptive.policy import PolicySnapshot
from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from research.oos_stability import OOSWindow
from research.replay import OrderBookProvenance, ReplayResult
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

SYMBOL = "BTCUSDT"


def _snapshot(**overrides) -> PolicySnapshot:
    defaults = dict(
        version_id="policy-1", stop_atr_multiple=2.0, take_profit_atr_multiple=4.0,
        trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=2.0, max_hold_hours=48.0,
        created_at=EPOCH, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(**defaults)


def _signal(ts: datetime, context_id: str) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=ts, context_id=context_id, score=0.9, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test",
    )


def _cycle_result(ts: datetime, side: PositionSide, *, entry_price: float = 100.0, context_id: str = "ctx") -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=SYMBOL,
        position=PaperPosition(
            symbol=SYMBOL, side=side, quantity=1.0 if side is not PositionSide.FLAT else 0.0,
            average_entry_price=entry_price if side is not PositionSide.FLAT else 0.0,
            realized_pnl=0.0, updated_at=ts,
        ),
        orders=(), fills=(), idempotent_replay=False,
    )
    return RuntimeCycleResult(symbol=SYMBOL, evaluated=True, signal=_signal(ts, context_id), paper_result=paper_result, generated_at=ts)


def _replay_result(cycle_results: tuple[RuntimeCycleResult, ...], *, start: datetime, end: datetime) -> ReplayResult:
    return ReplayResult(
        symbols=(SYMBOL,), start=start, end=end, order_book_provenance=OrderBookProvenance.SYNTHETIC,
        data_quality_reports=(), cycle_results=cycle_results, skipped_evaluation_count=0,
        final_positions=(), limitations=(),
    )


class TestDefaultDiscoveryReplayConfig:
    def test_uses_documented_nonzero_fee_and_slippage(self) -> None:
        config = default_discovery_replay_config()
        assert config.fee_bps == DISCOVERY_FEE_BPS == 10.0
        assert config.slippage_bps == DISCOVERY_SLIPPAGE_BPS == 5.0
        assert config.fee_bps > 0.0
        assert config.slippage_bps > 0.0


class TestRunSharedReplay:
    def test_runs_once_and_uses_default_config_when_none_given(self) -> None:
        source = FakeHistoricalCandleSource()
        window = OOSWindow(index=0, start=EPOCH, end=EPOCH + timedelta(hours=6))

        async def scenario():
            return await run_shared_replay(candle_source=source, symbols=(SYMBOL,), window=window)

        result = run_async(scenario())
        assert result.symbols == (SYMBOL,)
        assert result.start == window.start
        assert result.end == window.end


class TestEvaluateChallenger:
    def test_two_different_challengers_produce_different_outcomes_from_the_same_replay(self) -> None:
        """Proves `evaluate_challenger` genuinely varies exit behavior per
        challenger from a SINGLE shared replay -- the same real entry,
        two different `ExitPolicyConfig`s, two different simulated
        outcomes (foreshadows step 8's shadow-independence requirement)."""
        t1 = EPOCH + timedelta(minutes=5)
        results = (_cycle_result(EPOCH, PositionSide.FLAT), _cycle_result(t1, PositionSide.LONG, entry_price=100.0))
        replay = _replay_result(results, start=EPOCH, end=t1 + timedelta(days=2))
        source = FakeHistoricalCandleSource()

        tight_stop = _snapshot(version_id="tight", stop_atr_multiple=0.5, take_profit_atr_multiple=0.5)
        wide_stop = _snapshot(version_id="wide", stop_atr_multiple=6.0, take_profit_atr_multiple=10.0, max_hold_hours=1.0)

        async def scenario():
            tight_eval = await evaluate_challenger(
                tight_stop, replay_result=replay, candle_source=source, monte_carlo_seed=1,
                exit_window=timedelta(hours=48),
            )
            wide_eval = await evaluate_challenger(
                wide_stop, replay_result=replay, candle_source=source, monte_carlo_seed=1,
                exit_window=timedelta(hours=48),
            )
            return tight_eval, wide_eval

        tight_eval, wide_eval = run_async(scenario())
        assert tight_eval.policy_version_id == "tight"
        assert wide_eval.policy_version_id == "wide"
        # Genuinely different simulated outcomes from the identical entry/candles.
        assert tight_eval.sanity_report.exit_reason_distribution != wide_eval.sanity_report.exit_reason_distribution

    def test_monte_carlo_report_is_none_when_zero_completed_trades(self) -> None:
        results = (_cycle_result(EPOCH, PositionSide.FLAT),)  # never enters LONG at all
        replay = _replay_result(results, start=EPOCH, end=EPOCH + timedelta(days=1))
        source = FakeHistoricalCandleSource()

        async def scenario():
            return await evaluate_challenger(
                _snapshot(), replay_result=replay, candle_source=source, monte_carlo_seed=1,
                exit_window=timedelta(hours=6),
            )

        evaluation = run_async(scenario())
        assert evaluation.sanity_report.trade_count == 0
        assert evaluation.monte_carlo_report is None

    def test_deterministic_for_same_seed(self) -> None:
        t1 = EPOCH + timedelta(minutes=5)
        results = (_cycle_result(EPOCH, PositionSide.FLAT), _cycle_result(t1, PositionSide.LONG, entry_price=100.0))
        replay = _replay_result(results, start=EPOCH, end=t1 + timedelta(days=2))
        source = FakeHistoricalCandleSource()

        async def scenario():
            first = await evaluate_challenger(
                _snapshot(), replay_result=replay, candle_source=source, monte_carlo_seed=7,
                exit_window=timedelta(hours=48),
            )
            second = await evaluate_challenger(
                _snapshot(), replay_result=replay, candle_source=source, monte_carlo_seed=7,
                exit_window=timedelta(hours=48),
            )
            return first, second

        first, second = run_async(scenario())
        assert first.sanity_report == second.sanity_report
        if first.monte_carlo_report is not None:
            assert first.monte_carlo_report == second.monte_carlo_report


class TestEvaluateChallengersAgainstWindow:
    def test_runs_replay_once_for_all_challengers(self) -> None:
        """The shared-replay optimization: N challengers must trigger
        exactly ONE `fetch_historical_candles` pass per (symbol,
        timeframe) from the candle source, never N passes."""
        source = FakeHistoricalCandleSource()
        window = OOSWindow(index=0, start=EPOCH, end=EPOCH + timedelta(hours=6))
        challengers = tuple(_snapshot(version_id=f"c{i}") for i in range(5))

        async def scenario():
            return await evaluate_challengers_against_window(
                challengers, candle_source=source, symbols=(SYMBOL,), window=window, monte_carlo_seed=1,
            )

        evaluations = run_async(scenario())
        assert len(evaluations) == 5
        assert {e.policy_version_id for e in evaluations} == {f"c{i}" for i in range(5)}
        # bootstrap fetch + scored-window fetch per timeframe, NOT multiplied by challenger count.
        replay_fetch_call_count = len(source.calls)
        assert replay_fetch_call_count > 0
        assert replay_fetch_call_count < 5 * 3 * 2  # sanity ceiling: not literally 5x the single-run fetch count


=== FILE: tests/test_adaptive_policy.py ===
"""Adaptive Intelligence v1, step 1 — `PolicySnapshot` schema tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adaptive.policy import (
    PolicySnapshot,
    default_champion_snapshot,
    snapshot_from_exit_policy_config,
)
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _snapshot(**overrides) -> PolicySnapshot:
    defaults = dict(
        version_id="policy-1", stop_atr_multiple=2.0, take_profit_atr_multiple=4.0,
        trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=2.0, max_hold_hours=48.0,
        created_at=_NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(**defaults)


class TestPolicySnapshot:
    def test_never_carries_a_risk_policy_field(self) -> None:
        # Structural guarantee, not just a convention: the dataclass has
        # exactly the five ExitPolicyConfig fields plus version_id/
        # created_at/provenance -- nothing named after any
        # RiskPolicyConfig field (max_open_positions, max_total_exposure_usdt,
        # daily_loss_limit_usdt, sell_commission_headroom_bps, cooldown).
        field_names = set(PolicySnapshot.__dataclass_fields__.keys())
        forbidden = {
            "max_open_positions", "max_total_exposure_usdt", "daily_loss_limit_usdt",
            "sell_commission_headroom_bps", "cooldown", "cooldown_minutes", "risk_policy",
        }
        assert field_names.isdisjoint(forbidden)
        assert field_names == {
            "version_id", "stop_atr_multiple", "take_profit_atr_multiple",
            "trailing_activation_atr_multiple", "trailing_distance_atr_multiple", "max_hold_hours",
            "created_at", "provenance",
        }

    def test_to_exit_policy_config_round_trips(self) -> None:
        snapshot = _snapshot(
            stop_atr_multiple=1.5, take_profit_atr_multiple=3.0, trailing_activation_atr_multiple=1.0,
            trailing_distance_atr_multiple=1.0, max_hold_hours=12.0,
        )
        config = snapshot.to_exit_policy_config()
        assert isinstance(config, ExitPolicyConfig)
        assert config.stop_atr_multiple == 1.5
        assert config.take_profit_atr_multiple == 3.0
        assert config.trailing_activation_atr_multiple == 1.0
        assert config.trailing_distance_atr_multiple == 1.0
        assert config.max_hold_hours == 12.0

    def test_reuses_exit_policy_config_validation_not_a_second_implementation(self) -> None:
        # A non-positive field is rejected via ExitPolicyConfig's OWN
        # __post_init__ (called from PolicySnapshot.__post_init__), never
        # a duplicated bounds check.
        with pytest.raises(ValueError):
            _snapshot(stop_atr_multiple=0.0)
        with pytest.raises(ValueError):
            _snapshot(max_hold_hours=-1.0)

    def test_empty_version_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            _snapshot(version_id="")

    def test_naive_created_at_rejected(self) -> None:
        with pytest.raises(ValueError):
            _snapshot(created_at=datetime(2026, 1, 1))  # no tzinfo

    def test_is_frozen(self) -> None:
        snapshot = _snapshot()
        with pytest.raises(Exception):
            snapshot.stop_atr_multiple = 99.0  # type: ignore[misc]


class TestSnapshotFromExitPolicyConfig:
    def test_wraps_existing_config_by_value_not_reference(self) -> None:
        config = ExitPolicyConfig(stop_atr_multiple=1.5)
        snapshot = snapshot_from_exit_policy_config(config, version_id="v9", provenance="test", created_at=_NOW)
        assert snapshot.stop_atr_multiple == 1.5
        assert snapshot.version_id == "v9"
        # Independent value -- mutating one's underlying config is
        # impossible (both are frozen dataclasses) but confirm no shared
        # mutable state either way.
        assert snapshot.to_exit_policy_config() == config

    def test_auto_generates_version_id_when_omitted(self) -> None:
        s1 = snapshot_from_exit_policy_config(ExitPolicyConfig(), provenance="a", created_at=_NOW)
        s2 = snapshot_from_exit_policy_config(ExitPolicyConfig(), provenance="b", created_at=_NOW)
        assert s1.version_id != s2.version_id


class TestDefaultChampionSnapshot:
    def test_matches_production_default_exit_policy_config_exactly(self) -> None:
        snapshot = default_champion_snapshot(created_at=_NOW)
        assert snapshot.to_exit_policy_config() == ExitPolicyConfig()

    def test_has_a_stable_recognizable_version_id(self) -> None:
        assert default_champion_snapshot(created_at=_NOW).version_id == "policy-champion-default"


=== FILE: tests/test_adaptive_rollback.py ===
"""
Adaptive Intelligence v1, step 14 — rollback tests, including the
independent-verification fix's REQUIRED tests: a single narrowly-losing
trade among otherwise neutral/positive results does NOT trigger
rollback; a genuinely large, sustained degradation DOES."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adaptive.policy import PolicySnapshot
from adaptive.rollback import (
    ROLLBACK_MIN_BASELINE_SAMPLE_COUNT,
    ROLLBACK_MIN_LIVE_SAMPLE_COUNT,
    apply_rollback_if_needed,
    check_rollback,
    performance_metrics_from_pnls,
)
from adaptive.shadow import ShadowOutcome
from adaptive.store import AdaptiveStore
from crypto_signal_engine.execution.lifecycle import ExitReason

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _snapshot(version_id: str, **overrides) -> PolicySnapshot:
    defaults = dict(
        stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
        trailing_distance_atr_multiple=2.0, max_hold_hours=48.0, created_at=NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(version_id=version_id, **defaults)


@pytest.fixture()
def store(tmp_path) -> AdaptiveStore:  # noqa: ANN001
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


def _seed_shadow_outcomes(store: AdaptiveStore, version_id: str, pnls: list[float]) -> None:
    for pnl in pnls:
        store.record_shadow_outcome(
            version_id, symbol="BTCUSDT",
            outcome=ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT if pnl > 0 else ExitReason.STOP_LOSS, exit_price=100.0 + pnl, exit_time=NOW, gross_pnl_per_unit=pnl),
            now=NOW,
        )


class TestPerformanceMetricsFromPnls:
    def test_empty_list(self) -> None:
        metrics = performance_metrics_from_pnls([])
        assert metrics.trade_count == 0
        assert metrics.win_rate is None
        assert metrics.profit_factor is None

    def test_basic_aggregation(self) -> None:
        metrics = performance_metrics_from_pnls([10.0, -5.0, 3.0, 0.0])
        assert metrics.trade_count == 4
        assert metrics.wins == 2
        assert metrics.losses == 1
        assert metrics.breakeven == 1
        assert metrics.win_rate == 0.5
        assert metrics.gross_profit == 13.0
        assert metrics.gross_loss == 5.0
        assert metrics.profit_factor == pytest.approx(13.0 / 5.0)
        assert metrics.net_pnl == 8.0
        assert metrics.total_fees == 0.0
        assert metrics.max_drawdown is None

    def test_all_wins_profit_factor_is_none(self) -> None:
        metrics = performance_metrics_from_pnls([1.0, 2.0, 3.0])
        assert metrics.profit_factor is None  # gross_loss == 0 -- division by zero avoided, never fabricated


class TestCheckRollbackRequiredBehavior:
    def test_single_narrowly_losing_trade_among_neutral_results_does_not_trigger(self) -> None:
        """THE required test: a single -0.01 loss among nine break-even/
        small-positive trades must NOT trigger rollback -- this is
        exactly the naive old rule's failure mode."""
        live_pnls = [0.0] * 8 + [0.02, -0.01]  # net positive overall, one tiny loss
        baseline_pnls = [0.0] * 3 + [0.03, -0.01]  # comparable baseline shape
        live = performance_metrics_from_pnls(live_pnls)
        baseline = performance_metrics_from_pnls(baseline_pnls)
        # net_pnl is actually positive here -- the most obvious non-trigger.
        result = check_rollback(live=live, baseline=baseline, baseline_label="test")
        assert result.should_rollback is False

    def test_narrowly_negative_overall_but_no_magnitude_degradation_does_not_trigger(self) -> None:
        """A slightly-negative-but-comparable-to-baseline champion must
        NOT roll back -- net negative alone is no longer sufficient."""
        live_pnls = [1.0] * 9 + [-9.5]  # net = -0.5, mostly winning trades
        baseline_pnls = [1.0] * 4 + [-4.2]  # comparable profit_factor/win_rate shape
        live = performance_metrics_from_pnls(live_pnls)
        baseline = performance_metrics_from_pnls(baseline_pnls)
        assert live.net_pnl < 0
        result = check_rollback(live=live, baseline=baseline, baseline_label="test")
        assert result.should_rollback is False

    def test_genuinely_large_sustained_degradation_does_trigger(self) -> None:
        """THE required companion test: a real, structural collapse in
        both profit factor and win rate versus baseline DOES trigger."""
        baseline_pnls = [10.0] * 7 + [-2.0] * 3  # profit_factor=70/6≈11.7, win_rate=0.7
        live_pnls = [1.0] * 2 + [-10.0] * 8  # profit_factor=2/80=0.025, win_rate=0.2 -- collapsed
        live = performance_metrics_from_pnls(live_pnls)
        baseline = performance_metrics_from_pnls(baseline_pnls)
        assert live.net_pnl < 0
        result = check_rollback(live=live, baseline=baseline, baseline_label="test")
        assert result.should_rollback is True
        assert "kötüleşti" in result.reason

    def test_insufficient_live_sample_never_triggers(self) -> None:
        live = performance_metrics_from_pnls([-100.0] * (ROLLBACK_MIN_LIVE_SAMPLE_COUNT - 1))
        baseline = performance_metrics_from_pnls([10.0] * ROLLBACK_MIN_BASELINE_SAMPLE_COUNT)
        result = check_rollback(live=live, baseline=baseline, baseline_label="test")
        assert result.should_rollback is False
        assert "örneklem yetersiz" in result.reason

    def test_insufficient_baseline_sample_never_triggers(self) -> None:
        live = performance_metrics_from_pnls([-100.0] * ROLLBACK_MIN_LIVE_SAMPLE_COUNT)
        baseline = performance_metrics_from_pnls([10.0] * (ROLLBACK_MIN_BASELINE_SAMPLE_COUNT - 1))
        result = check_rollback(live=live, baseline=baseline, baseline_label="test")
        assert result.should_rollback is False
        assert "baseline" in result.reason


class TestApplyRollbackIfNeeded:
    def test_no_rollback_when_shadow_baseline_and_live_both_healthy(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 8 + [-2.0] * 2)  # healthy shadow baseline

        def live_pnls_provider(version_id: str) -> list[float]:
            return [10.0] * 8 + [-2.0] * 2  # matches baseline -- no degradation

        result = apply_rollback_if_needed(store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW)
        assert result is None
        assert store.current_champion() == v2

    def test_rolls_back_using_own_shadow_stage_baseline_on_genuine_degradation(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)  # good shadow track record

        def live_pnls_provider(version_id: str) -> list[float]:
            return [1.0] * 2 + [-10.0] * 8  # collapsed in real production

        result = apply_rollback_if_needed(store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW)
        assert result == v1
        assert store.current_champion() == v1

    def test_falls_back_to_prior_champion_live_baseline_when_no_shadow_evidence(self, store: AdaptiveStore) -> None:
        """v2 has NO shadow evidence at all (e.g. promoted via a path
        that skipped shadow monitoring in a test/edge case) -- baseline
        resolution must fall back to v1's own real live performance."""
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        # No shadow outcomes recorded for v2 at all.

        def live_pnls_provider(version_id: str) -> list[float]:
            if version_id == "v1":
                return [10.0] * 7 + [-2.0] * 3  # v1's own real historical track record
            return [1.0] * 2 + [-10.0] * 8  # v2's real collapsed performance

        result = apply_rollback_if_needed(store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW)
        assert result == v1
        assert store.current_champion() == v1

    def test_no_baseline_evidence_at_all_never_rolls_back(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        # No shadow evidence for v2, and v1 has too few live trades too.

        def live_pnls_provider(version_id: str) -> list[float]:
            if version_id == "v1":
                return [10.0]  # only 1 trade -- below ROLLBACK_MIN_BASELINE_SAMPLE_COUNT
            return [-10.0] * ROLLBACK_MIN_LIVE_SAMPLE_COUNT

        result = apply_rollback_if_needed(store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW)
        assert result is None
        assert store.current_champion() == v2  # untouched -- insufficient evidence, not a crash

    def test_rollback_is_logged_in_champion_history(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)

        def live_pnls_provider(version_id: str) -> list[float]:
            return [1.0] * 2 + [-10.0] * 8

        apply_rollback_if_needed(store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW)
        history = store.champion_history()
        assert [h["version_id"] for h in history] == ["v1", "v2", "v1"]
        assert "ROLLBACK" in history[-1]["reason"]

    def test_rollback_notifies(self, store: AdaptiveStore) -> None:
        """24/7 Ops v1, Step 4 — wired at this already-logged decision
        point: a real rollback fires exactly one notification."""
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)

        def live_pnls_provider(version_id: str) -> list[float]:
            return [1.0] * 2 + [-10.0] * 8

        calls: list[str] = []
        result = apply_rollback_if_needed(
            store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW, notifier=calls.append,
        )
        assert result == v1
        assert len(calls) == 1
        assert "v2" in calls[0] and "v1" in calls[0]

    def test_no_rollback_never_notifies(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [1.0] * 3)

        def live_pnls_provider(version_id: str) -> list[float]:
            return [10.0] * ROLLBACK_MIN_LIVE_SAMPLE_COUNT

        calls: list[str] = []
        result = apply_rollback_if_needed(
            store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW, notifier=calls.append,
        )
        assert result is None
        assert calls == []

    def test_a_raising_notifier_never_affects_the_rollback_decision(self, store: AdaptiveStore) -> None:
        """24/7 Ops v1, Step 4 — required per-site test: the underlying
        decision (rollback still applies correctly) is UNAFFECTED even
        when the notifier itself raises."""
        v1 = _snapshot("v1")
        v2 = _snapshot("v2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted", now=NOW)
        _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)

        def live_pnls_provider(version_id: str) -> list[float]:
            return [1.0] * 2 + [-10.0] * 8

        def _raising_notifier(message: str) -> None:
            raise RuntimeError("simulated notifier failure")

        result = apply_rollback_if_needed(
            store, champion=v2, live_pnls_provider=live_pnls_provider, now=NOW, notifier=_raising_notifier,
        )
        assert result == v1
        assert store.current_champion() == v1

    def test_no_prior_champion_to_roll_back_to_returns_none(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("v1")
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        _seed_shadow_outcomes(store, "v1", [10.0] * 7 + [-2.0] * 3)

        def live_pnls_provider(version_id: str) -> list[float]:
            return [1.0] * 2 + [-10.0] * 8

        result = apply_rollback_if_needed(store, champion=v1, live_pnls_provider=live_pnls_provider, now=NOW)
        assert result is None
        assert store.current_champion() == v1  # untouched -- honest limitation, not a crash


=== FILE: tests/test_adaptive_scheduler.py ===
"""Adaptive Intelligence v1, step 13 — AdaptiveScheduler tests. Same
structure/discipline as tests/test_reselection_scheduler.py."""

from __future__ import annotations

from datetime import timedelta

import pytest

from adaptive.scheduler import (
    DEFAULT_CYCLE_INTERVAL_SECONDS,
    AdaptiveScheduler,
    AdaptiveSchedulerConfig,
)
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

SYMBOL = "BTCUSDT"


@pytest.fixture()
def store(tmp_path) -> AdaptiveStore:  # noqa: ANN001
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


def _config(**overrides) -> AdaptiveSchedulerConfig:
    defaults = dict(
        symbols=(SYMBOL,), anchor=EPOCH,
        window_config=WindowConfig(discovery_window_size=timedelta(hours=6), confirmation_window_size=timedelta(hours=2)),
        cycle_interval_seconds=3600.0, seed=1,
    )
    defaults.update(overrides)
    return AdaptiveSchedulerConfig(**defaults)


class TestAdaptiveSchedulerConfigValidation:
    def test_default_interval_is_24h(self) -> None:
        assert DEFAULT_CYCLE_INTERVAL_SECONDS == 86400.0

    def test_rejects_interval_beyond_24h_ceiling(self) -> None:
        with pytest.raises(ValueError):
            _config(cycle_interval_seconds=86400.1)

    def test_rejects_non_positive_interval(self) -> None:
        with pytest.raises(ValueError):
            _config(cycle_interval_seconds=0.0)

    def test_rejects_empty_symbols(self) -> None:
        with pytest.raises(ValueError):
            _config(symbols=())

    def test_accepts_exactly_the_24h_ceiling(self) -> None:
        _config(cycle_interval_seconds=86400.0)  # must not raise


class TestRunOnce:
    def test_run_once_returns_a_report_and_does_not_raise(self, store: AdaptiveStore) -> None:
        scheduler = AdaptiveScheduler(
            store=store, candle_source=FakeHistoricalCandleSource(), config=_config(),
            clock=FixedClock(EPOCH + timedelta(hours=10)),
        )
        report = run_async(scheduler.run_once())
        assert report is not None
        assert report.champion_version_id == "policy-champion-default"

    def test_failed_cycle_is_isolated_and_reported_via_status(self, store: AdaptiveStore) -> None:
        class _BrokenCandleSource:
            async def fetch_historical_candles(self, symbol, timeframe, start, end):  # noqa: ANN001
                raise RuntimeError("simulated candle source failure")

        scheduler = AdaptiveScheduler(
            store=store, candle_source=_BrokenCandleSource(), config=_config(),
            clock=FixedClock(EPOCH + timedelta(hours=10)),
        )
        report = run_async(scheduler.run_once())  # must not raise
        assert report is None
        status = scheduler.status()
        assert status.last_error is not None
        assert "simulated candle source failure" in status.last_error

    def test_status_reflects_last_report(self, store: AdaptiveStore) -> None:
        scheduler = AdaptiveScheduler(
            store=store, candle_source=FakeHistoricalCandleSource(), config=_config(),
            clock=FixedClock(EPOCH + timedelta(hours=10)),
        )
        run_async(scheduler.run_once())
        status = scheduler.status()
        assert status.last_champion_version_id == "policy-champion-default"
        assert status.last_error is None


class TestStartStop:
    def test_status_before_start_is_not_armed(self, store: AdaptiveStore) -> None:
        scheduler = AdaptiveScheduler(store=store, candle_source=FakeHistoricalCandleSource(), config=_config())
        assert scheduler.status().armed is False

    def test_status_armed_after_start(self, store: AdaptiveStore) -> None:
        async def scenario():
            scheduler = AdaptiveScheduler(
                store=store, candle_source=FakeHistoricalCandleSource(), config=_config(),
                clock=FixedClock(EPOCH),
            )
            scheduler.start()
            status = scheduler.status()
            await scheduler.stop()
            return status

        status = run_async(scenario())
        assert status.armed is True
        assert status.next_run_at == EPOCH + timedelta(seconds=3600.0)

    def test_stop_is_idempotent(self, store: AdaptiveStore) -> None:
        async def scenario():
            scheduler = AdaptiveScheduler(
                store=store, candle_source=FakeHistoricalCandleSource(), config=_config(), clock=FixedClock(EPOCH),
            )
            scheduler.start()
            await scheduler.stop()
            await scheduler.stop()  # must not raise

        run_async(scenario())

    def test_starting_twice_is_a_no_op(self, store: AdaptiveStore) -> None:
        async def scenario():
            scheduler = AdaptiveScheduler(
                store=store, candle_source=FakeHistoricalCandleSource(), config=_config(), clock=FixedClock(EPOCH),
            )
            scheduler.start()
            first_task = scheduler._task  # noqa: SLF001
            scheduler.start()
            second_task = scheduler._task  # noqa: SLF001
            await scheduler.stop()
            return first_task is second_task

        assert run_async(scenario()) is True


class TestLiveCycleResultsProviderThreading:
    """Drift-signal wiring fix: `AdaptiveScheduler` must thread
    `live_cycle_results_provider` through to `run_adaptive_cycle`, the
    same way it already threads `champion_live_evidence_provider`."""

    def test_provider_is_invoked_during_run_once(self, store: AdaptiveStore) -> None:
        calls = {"count": 0}

        def live_provider():
            calls["count"] += 1
            return ()

        scheduler = AdaptiveScheduler(
            store=store, candle_source=FakeHistoricalCandleSource(), config=_config(),
            clock=FixedClock(EPOCH + timedelta(hours=10)), live_cycle_results_provider=live_provider,
        )
        run_async(scheduler.run_once())
        assert calls["count"] >= 1

    def test_omitted_provider_never_raises(self, store: AdaptiveStore) -> None:
        scheduler = AdaptiveScheduler(
            store=store, candle_source=FakeHistoricalCandleSource(), config=_config(),
            clock=FixedClock(EPOCH + timedelta(hours=10)),
        )
        report = run_async(scheduler.run_once())
        assert report is not None


=== FILE: tests/test_adaptive_shadow.py ===
"""Adaptive Intelligence v1, step 8 — shadow evaluation tests, including
the REQUIRED shadow-independence test: given the same candle stream, a
shadow challenger with different ExitPolicyConfig values produces a
genuinely different outcome than the real champion position."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from adaptive.policy import PolicySnapshot
from adaptive.shadow import (
    DEFAULT_MAX_CONCURRENT_SHADOWS,
    ShadowCapacityError,
    ShadowMonitor,
    detect_new_long_entries,
    infer_entry_atr,
    start_shadow,
)
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    Candle,
    LifecyclePriceState,
    PositionLifecycleState,
    compute_initial_stop_and_target,
    evaluate_candle,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _snapshot(**overrides) -> PolicySnapshot:
    defaults = dict(
        version_id="challenger-1", stop_atr_multiple=2.0, take_profit_atr_multiple=4.0,
        trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=2.0, max_hold_hours=48.0,
        created_at=NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(**defaults)


class TestStartShadow:
    def test_seeds_from_entry_price_atr_entry_time_only(self) -> None:
        challenger = _snapshot(stop_atr_multiple=1.5, take_profit_atr_multiple=3.0)
        shadow = start_shadow(challenger, symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)
        assert shadow.price_state.entry_price == 100.0
        assert shadow.price_state.entry_timestamp == NOW
        assert shadow.price_state.initial_protective_stop == 100.0 - 1.5 * 10.0
        assert shadow.price_state.take_profit == 100.0 + 3.0 * 10.0
        assert shadow.atr == 10.0
        assert shadow.symbol == "BTCUSDT"
        assert shadow.challenger_version_id == "challenger-1"
        assert not shadow.closed


class TestShadowIndependenceRequiredProof:
    """THE mission-required test: given the SAME candle stream, a shadow
    challenger with a DIFFERENT ExitPolicyConfig produces a genuinely
    different outcome (exit price/reason/timing) than the real champion
    position — proving the shadow result is independently computed, not
    copied from the real position."""

    def test_shadow_and_real_position_diverge_from_identical_candles(self) -> None:
        entry_price = 100.0
        atr = 10.0
        entry_time = NOW

        # The REAL champion position: default-ish policy.
        champion_policy = _snapshot(
            version_id="champion", stop_atr_multiple=2.0, take_profit_atr_multiple=4.0,
            trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=2.0, max_hold_hours=48.0,
        ).to_exit_policy_config()
        real_stop, real_target = compute_initial_stop_and_target(entry_price=entry_price, atr=atr, config=champion_policy)
        real_state = LifecyclePriceState(
            entry_price=entry_price, entry_timestamp=entry_time, initial_protective_stop=real_stop,
            take_profit=real_target, high_water=entry_price, effective_stop=real_stop, trailing_active=False,
            last_stop_mechanism="STOP_LOSS",
        )

        # The SHADOW challenger: a much tighter stop -- same entry inputs.
        challenger = _snapshot(
            version_id="tight-challenger", stop_atr_multiple=0.5, take_profit_atr_multiple=10.0,
            trailing_activation_atr_multiple=10.0, trailing_distance_atr_multiple=10.0, max_hold_hours=48.0,
        )
        shadow = start_shadow(challenger, symbol="BTCUSDT", entry_price=entry_price, atr=atr, entry_time=entry_time)

        # A single candle stream, fed to BOTH identically.
        candle = Candle(open=95, high=96, low=88, close=95, close_time=NOW + timedelta(minutes=5))

        real_result = evaluate_candle(real_state, candle, champion_policy, atr_for_trailing=atr)
        shadow.on_candle(candle)

        # Real position (stop = 100 - 2*10 = 80): candle.low=88 > 80 -> no exit.
        assert real_result.exit_reason is None
        # Shadow (stop = 100 - 0.5*10 = 95): candle.low=88 <= 95 -> STOP_LOSS fires.
        assert shadow.closed
        assert shadow.outcome is not None
        assert shadow.outcome.exit_reason.value == "STOP_LOSS"
        # Genuinely different outcome from the identical candle stream --
        # the shadow's result is independently computed, not copied.
        assert (shadow.outcome.exit_price, shadow.outcome.exit_reason) != (None, real_result.exit_reason)


class TestM1FidelityRequiredProof:
    """THE mission-required test (independent-verification gap #3): a
    scenario where, within what would be a single M5-aggregated window,
    TARGET is genuinely hit BEFORE STOP in true chronological order.
    `evaluate_candle()` has a fixed stop-before-target priority WITHIN
    one evaluated candle — feeding the M5-AGGREGATED candle (the OLD,
    wrong implementation's behavior) reports STOP_LOSS; feeding the
    underlying M1 candles ONE AT A TIME, in true order (the FIXED
    implementation, via `ShadowChallenger.on_candle`/`ShadowMonitor.
    on_m1_candle`), correctly reports TAKE_PROFIT — because the shadow
    closes on the first M1 candle that trips ANY exit, before the later,
    stop-tripping M1 candle is ever evaluated."""

    ENTRY_PRICE = 100.0
    ATR = 5.0  # stop=90 (2.0x ATR), target=120 (4.0x ATR) with default multiples

    def _m1_candles(self) -> tuple:
        # Minute 1: rallies straight through target (121 >= 120) without
        # ever approaching the stop (low=99 > 90).
        c1 = Candle(open=100, high=121, low=99, close=110, close_time=NOW + timedelta(minutes=1))
        # Minute 2: irrelevant filler.
        c2 = Candle(open=110, high=112, low=105, close=108, close_time=NOW + timedelta(minutes=2))
        # Minute 3: crashes through the stop (low=88 <= 90) -- but this
        # must NEVER be reached at true M1 fidelity, since c1 already
        # closed the position on TAKE_PROFIT.
        c3 = Candle(open=108, high=109, low=88, close=95, close_time=NOW + timedelta(minutes=3))
        return (c1, c2, c3)

    def test_m5_aggregated_evaluation_reports_the_wrong_answer_stop_loss(self) -> None:
        """Reproduces the OLD (pre-fix) M5-only behavior directly: one
        aggregated candle spanning the same 3-minute window (open=first
        M1's open, high=max, low=min, close=last M1's close) — proves
        this WOULD have reported STOP_LOSS, the wrong answer, since
        target was actually hit first in true chronological order."""
        m1_candles = self._m1_candles()
        aggregated = Candle(
            open=m1_candles[0].open, high=max(c.high for c in m1_candles), low=min(c.low for c in m1_candles),
            close=m1_candles[-1].close, close_time=m1_candles[-1].close_time,
        )
        challenger = _snapshot(version_id="m5-aggregated-control")
        shadow = start_shadow(challenger, symbol="BTCUSDT", entry_price=self.ENTRY_PRICE, atr=self.ATR, entry_time=NOW)

        shadow.on_candle(aggregated)

        assert shadow.closed
        assert shadow.outcome.exit_reason.value == "STOP_LOSS"  # the WRONG answer -- proves the old bug is real

    def test_true_m1_fidelity_correctly_reports_take_profit(self) -> None:
        """The FIX under test: feeding the same underlying price action
        as SEPARATE, chronologically-ordered M1 candles (via
        `ShadowChallenger.on_candle`, the same method `ShadowMonitor.
        on_m1_candle` dispatches to) correctly reports TAKE_PROFIT --
        the shadow closes on c1 and never evaluates c3's stop-tripping low."""
        challenger = _snapshot(version_id="true-m1-fidelity")
        shadow = start_shadow(challenger, symbol="BTCUSDT", entry_price=self.ENTRY_PRICE, atr=self.ATR, entry_time=NOW)

        for candle in self._m1_candles():
            shadow.on_candle(candle)

        assert shadow.closed
        assert shadow.outcome.exit_reason.value == "TAKE_PROFIT"  # the CORRECT answer
        assert shadow.outcome.exit_time == NOW + timedelta(minutes=1)  # closed on c1, never reached c3

    def test_shadow_monitor_on_m1_candle_produces_the_same_correct_result(self) -> None:
        """End-to-end through the actual production dispatch method."""
        monitor = ShadowMonitor()
        challenger = _snapshot(version_id="via-monitor")
        monitor.start_shadowing(challenger, symbol="BTCUSDT", entry_price=self.ENTRY_PRICE, atr=self.ATR, entry_time=NOW)

        for candle in self._m1_candles():
            monitor.on_m1_candle("BTCUSDT", candle)

        shadow = monitor.active_challengers[0]
        assert shadow.closed
        assert shadow.outcome.exit_reason.value == "TAKE_PROFIT"


class TestShadowMonitor:
    def test_on_candle_only_reacts_to_shadow_observation_timeframe(self) -> None:
        monitor = ShadowMonitor()
        challenger = _snapshot()
        monitor.start_shadowing(challenger, symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)

        h1_candle = Candle(open=50, high=50, low=50, close=50, close_time=NOW + timedelta(minutes=5))
        monitor.on_candle("BTCUSDT", Timeframe.H1, h1_candle)  # wrong timeframe -- ignored
        shadow = monitor.active_challengers[0]
        assert not shadow.closed  # price=50 would have triggered a stop if it were acted on

        m5_candle = Candle(open=50, high=50, low=50, close=50, close_time=NOW + timedelta(minutes=5))
        monitor.on_candle("BTCUSDT", Timeframe.M5, m5_candle)
        assert monitor.active_challengers[0].closed  # now it IS acted on

    def test_on_candle_ignores_other_symbols(self) -> None:
        monitor = ShadowMonitor()
        challenger = _snapshot()
        monitor.start_shadowing(challenger, symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)

        candle = Candle(open=50, high=50, low=50, close=50, close_time=NOW + timedelta(minutes=5))
        monitor.on_candle("ETHUSDT", Timeframe.M5, candle)
        assert not monitor.active_challengers[0].closed

    def test_reusing_same_challenger_returns_existing_shadow(self) -> None:
        monitor = ShadowMonitor()
        challenger = _snapshot()
        first = monitor.start_shadowing(challenger, symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)
        second = monitor.start_shadowing(challenger, symbol="BTCUSDT", entry_price=999.0, atr=1.0, entry_time=NOW)
        assert first is second
        assert len(monitor.active_challengers) == 1

    def test_capacity_cap_enforced(self) -> None:
        monitor = ShadowMonitor(max_concurrent=2)
        for i in range(2):
            monitor.start_shadowing(
                _snapshot(version_id=f"c{i}"), symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW,
            )
        with pytest.raises(ShadowCapacityError):
            monitor.start_shadowing(
                _snapshot(version_id="c-overflow"), symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW,
            )

    def test_default_cap_is_small_and_documented(self) -> None:
        assert 1 <= DEFAULT_MAX_CONCURRENT_SHADOWS <= 3

    def test_stop_shadowing_frees_capacity(self) -> None:
        monitor = ShadowMonitor(max_concurrent=1)
        monitor.start_shadowing(_snapshot(version_id="c0"), symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)
        monitor.stop_shadowing("c0")
        # No error -- capacity was freed.
        monitor.start_shadowing(_snapshot(version_id="c1"), symbol="BTCUSDT", entry_price=100.0, atr=10.0, entry_time=NOW)
        assert len(monitor.active_challengers) == 1

    def test_broken_challenger_never_raises_out_of_on_candle_for_others(self) -> None:
        """Not a hard requirement of the mission text, but consistent
        with the coordinator's own defensive-observer discipline: one
        shadow's `on_candle` must not be allowed to be malformed in a way
        that silently corrupts sibling shadows' state. (Each shadow's
        `on_candle` call is independent -- proven here by two shadows on
        the same symbol evolving correctly and independently.)"""
        monitor = ShadowMonitor(max_concurrent=2)
        monitor.start_shadowing(
            _snapshot(version_id="tight", stop_atr_multiple=0.5), symbol="BTCUSDT", entry_price=100.0, atr=10.0,
            entry_time=NOW,
        )
        monitor.start_shadowing(
            _snapshot(version_id="wide", stop_atr_multiple=6.0), symbol="BTCUSDT", entry_price=100.0, atr=10.0,
            entry_time=NOW,
        )
        candle = Candle(open=90, high=91, low=88, close=90, close_time=NOW + timedelta(minutes=5))
        monitor.on_candle("BTCUSDT", Timeframe.M5, candle)
        by_id = {s.challenger_version_id: s for s in monitor.active_challengers}
        assert by_id["tight"].closed  # stop = 100 - 0.5*10 = 95, low=88 <= 95
        assert not by_id["wide"].closed  # stop = 100 - 6*10 = 40, low=88 > 40


class TestInferEntryAtr:
    def _record(self, **overrides) -> BridgePositionRecord:
        defaults = dict(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        )
        defaults.update(overrides)
        return BridgePositionRecord(**defaults)

    def test_uses_embedded_stop_multiple_when_present(self) -> None:
        record = self._record(initial_protective_stop=90.0, exit_policy_stop_atr_multiple=1.0)
        assert infer_entry_atr(record) == pytest.approx(10.0)

    def test_falls_back_to_default_stop_multiple_for_legacy_position(self) -> None:
        record = self._record(initial_protective_stop=80.0)  # exit_policy_stop_atr_multiple is None
        # default ExitPolicyConfig().stop_atr_multiple == 2.0 -> (100-80)/2 == 10.0
        assert infer_entry_atr(record) == pytest.approx(10.0)

    def test_returns_none_when_fields_missing(self) -> None:
        flat = self._record(state=PositionLifecycleState.FLAT, gross_entry_vwap=None, net_owned_base_quantity=0.0)
        assert infer_entry_atr(flat) is None


class TestDetectNewLongEntries:
    def test_detects_flat_to_long_transition(self) -> None:
        record = BridgePositionRecord(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        )
        new_entries = detect_new_long_entries(
            previous_states={"BTCUSDT": PositionLifecycleState.FLAT}, current_positions=(record,),
        )
        assert new_entries == (record,)

    def test_already_long_is_not_a_new_entry(self) -> None:
        record = BridgePositionRecord(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        )
        new_entries = detect_new_long_entries(
            previous_states={"BTCUSDT": PositionLifecycleState.LONG}, current_positions=(record,),
        )
        assert new_entries == ()

    def test_unseen_symbol_defaults_to_not_previously_long(self) -> None:
        record = BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        )
        new_entries = detect_new_long_entries(previous_states={}, current_positions=(record,))
        assert new_entries == (record,)


=== FILE: tests/test_adaptive_state_protection.py ===
"""
Adaptive Intelligence v1, step 15 — STATE PROTECTION: this milestone must
not alter the schema or content of any existing persisted state (`var/`,
the existing PAPER state DB, execution/reconciliation history, manual-lab
records) beyond the new additive fields from steps 2-3 and the new
`adaptive/` tables.

Proven three ways:
1. `adaptive/store.py` owns an entirely SEPARATE table namespace (no
   name collides with `bridge_position`/`bridge_fee_ledger`/
   `bridge_completed_trade`/`bridge_daily_risk`, `schema_version`/
   `candles`, or `schema_version`/`paper_position`/`processed_context`/
   `paper_fill`/`paper_order`/`candle_checkpoint`) — checked here by
   reading every schema's raw SQL text directly, never re-typing the
   table names by hand (which would drift silently if a schema changes).
2. `adaptive/store.py` never imports any `crypto_signal_engine.
   persistence`/`crypto_signal_engine.execution.lifecycle_store`/
   `crypto_signal_engine.execution.reconciliation_store` module at all —
   it cannot touch their tables even by accident, because it never opens
   a connection to their files.
3. The legacy-position backward-compatibility test (`tests.
   test_execution_lifecycle_store::TestAdaptivePolicyFieldsMigration::
   test_opens_pre_adaptive_database_without_crashing_and_adds_columns`)
   already proves the ONE schema change this milestone makes to an
   EXISTING table (`bridge_position`/`bridge_completed_trade`'s new
   columns) is purely additive and never alters pre-existing rows."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _table_names(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", text))


class TestNoTableNameCollisions:
    def test_adaptive_tables_never_collide_with_existing_persistence_tables(self) -> None:
        adaptive_tables = _table_names(REPO_ROOT / "adaptive" / "store.py")
        assert adaptive_tables, "beklenmedik: adaptive/store.py'da hiç tablo bulunamadı"

        existing_schema_files = (
            REPO_ROOT / "crypto_signal_engine" / "execution" / "lifecycle_store.py",
            REPO_ROOT / "crypto_signal_engine" / "execution" / "reconciliation_store.py",
            REPO_ROOT / "crypto_signal_engine" / "persistence" / "sqlite_store.py",
            REPO_ROOT / "crypto_signal_engine" / "persistence" / "paper_state_store.py",
        )
        for path in existing_schema_files:
            if not path.exists():
                continue
            existing_tables = _table_names(path)
            overlap = adaptive_tables & existing_tables
            assert overlap == set(), f"{path} ile tablo adı çakışması: {overlap}"

    def test_adaptive_new_columns_are_the_only_schema_change_to_bridge_tables(self) -> None:
        """Cross-check against step 2's own additive-column list -- these
        are the ONLY new columns this milestone ever adds to an EXISTING
        table, never a new column beyond this documented set."""
        from crypto_signal_engine.execution.lifecycle_store import (
            _BRIDGE_COMPLETED_TRADE_NEW_COLUMNS,
            _BRIDGE_POSITION_NEW_COLUMNS,
        )

        assert set(name for name, _ in _BRIDGE_POSITION_NEW_COLUMNS) == {
            "exit_policy_stop_atr_multiple", "exit_policy_take_profit_atr_multiple",
            "exit_policy_trailing_activation_atr_multiple", "exit_policy_trailing_distance_atr_multiple",
            "exit_policy_max_hold_hours", "policy_version_id",
        }
        assert set(name for name, _ in _BRIDGE_COMPLETED_TRADE_NEW_COLUMNS) == {"policy_version_id"}


_FORBIDDEN_PERSISTENCE_MODULES = (
    "lifecycle_store", "reconciliation_store", "paper_state_store", "sqlite_store",
)


def _imported_module_names(path: Path) -> set[str]:
    """AST-based: only ACTUAL `import`/`from ... import` statements --
    never flags a module name merely mentioned in a docstring/comment
    explaining what this package deliberately does NOT do (which several
    `adaptive/` docstrings legitimately do, by name, as documentation)."""
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


class TestAdaptiveNeverImportsExistingPersistenceModules:
    def test_no_adaptive_module_actually_imports_any_existing_persistence_module(self) -> None:
        for path in sorted((REPO_ROOT / "adaptive").rglob("*.py")):
            imported = _imported_module_names(path)
            for module_name in imported:
                for forbidden in _FORBIDDEN_PERSISTENCE_MODULES:
                    assert forbidden not in module_name, (
                        f"{path} '{module_name}' import ediyor -- state protection ihlali"
                    )


class TestAdaptiveOwnsADedicatedDatabaseFile:
    def test_adaptive_store_never_hardcodes_a_shared_db_path(self, tmp_path) -> None:  # noqa: ANN001
        """`AdaptiveStore` takes its db path as an explicit constructor
        argument (no hardcoded default pointing at any existing `var/`
        file) -- verified here by successfully opening it at an
        arbitrary, obviously-dedicated path with zero special setup."""
        from adaptive.store import AdaptiveStore

        store = AdaptiveStore(tmp_path / "totally_separate_adaptive_only.db")
        try:
            assert store.current_champion() is None
        finally:
            store.close()


=== FILE: tests/test_adaptive_store.py ===
"""Adaptive Intelligence v1, step 12 — persistence tests, including the
required restart-recovery proof."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adaptive.decision import EvidenceSummary, decide
from adaptive.policy import PolicySnapshot
from adaptive.shadow import ShadowOutcome
from adaptive.store import AdaptiveStore
from crypto_signal_engine.execution.lifecycle import ExitReason

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _snapshot(version_id: str = "policy-1", **overrides) -> PolicySnapshot:
    defaults = dict(
        stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
        trailing_distance_atr_multiple=2.0, max_hold_hours=48.0, created_at=NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(version_id=version_id, **defaults)


@pytest.fixture()
def store(tmp_path) -> AdaptiveStore:  # noqa: ANN001
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


class TestPolicyVersionRoundTrip:
    def test_record_and_load(self, store: AdaptiveStore) -> None:
        snapshot = _snapshot()
        store.record_policy_version(snapshot)
        loaded = store.load_policy_version("policy-1")
        assert loaded == snapshot

    def test_missing_version_returns_none(self, store: AdaptiveStore) -> None:
        assert store.load_policy_version("nope") is None

    def test_re_recording_same_version_id_is_idempotent(self, store: AdaptiveStore) -> None:
        snapshot = _snapshot()
        store.record_policy_version(snapshot)
        store.record_policy_version(snapshot)  # must not raise
        assert len(store.list_policy_versions()) == 1

    def test_list_returns_all_in_creation_order(self, store: AdaptiveStore) -> None:
        store.record_policy_version(_snapshot("p1", created_at=NOW))
        store.record_policy_version(_snapshot("p2", created_at=NOW.replace(hour=1)))
        versions = store.list_policy_versions()
        assert [v.version_id for v in versions] == ["p1", "p2"]


class TestChampionHistory:
    def test_no_champion_initially(self, store: AdaptiveStore) -> None:
        assert store.current_champion() is None

    def test_promote_sets_current_champion(self, store: AdaptiveStore) -> None:
        snapshot = _snapshot("champion-1")
        store.promote_champion(snapshot, reason="bootstrap", now=NOW)
        assert store.current_champion() == snapshot

    def test_second_promotion_replaces_current_champion_but_keeps_history(self, store: AdaptiveStore) -> None:
        v1 = _snapshot("champion-1")
        v2 = _snapshot("champion-2", max_hold_hours=12.0)
        store.promote_champion(v1, reason="bootstrap", now=NOW)
        store.promote_champion(v2, reason="promoted after evidence", now=NOW.replace(hour=5))
        assert store.current_champion() == v2
        history = store.champion_history()
        assert [h["version_id"] for h in history] == ["champion-1", "champion-2"]

    def test_promotion_also_records_the_policy_version(self, store: AdaptiveStore) -> None:
        snapshot = _snapshot("champion-1")
        store.promote_champion(snapshot, reason="bootstrap", now=NOW)
        assert store.load_policy_version("champion-1") == snapshot


class TestDecisionLog:
    def _decision_result(self):
        evidence = EvidenceSummary(sample_count=50, win_rate=0.6, total_gross_pnl_per_unit=10.0, max_drawdown_per_unit=1.0)
        return decide(
            "challenger-1",
            discovery_challenger=evidence, discovery_champion=evidence,
            confirmation_challenger=evidence, confirmation_champion=evidence,
            shadow_challenger=evidence, shadow_champion=evidence,
        )

    def test_record_and_read_back(self, store: AdaptiveStore) -> None:
        result = self._decision_result()
        store.record_decision(result, now=NOW, drift_note="drift: MATCHED=1 (n=1)")
        recent = store.recent_decisions()
        assert len(recent) == 1
        assert recent[0]["challenger_version_id"] == "challenger-1"
        assert recent[0]["decision"] == result.decision.value
        assert recent[0]["drift_note"] == "drift: MATCHED=1 (n=1)"

    def test_evidence_classes_stored_separately_never_merged(self, store: AdaptiveStore) -> None:
        import json

        result = self._decision_result()
        store.record_decision(result, now=NOW)
        row = store.recent_decisions()[0]
        discovery = json.loads(row["discovery_challenger_json"])
        confirmation = json.loads(row["confirmation_challenger_json"])
        shadow = json.loads(row["shadow_challenger_json"])
        # Three genuinely separate JSON blobs -- not one combined/averaged field.
        assert "discovery_challenger_json" in row and "confirmation_challenger_json" in row and "shadow_challenger_json" in row
        assert discovery == confirmation == shadow  # same input values here, but stored as 3 independent columns

    def test_respects_limit_and_recency_order(self, store: AdaptiveStore) -> None:
        for i in range(5):
            result = self._decision_result()
            store.record_decision(result, now=NOW.replace(hour=i))
        recent = store.recent_decisions(limit=2)
        assert len(recent) == 2


class TestCursor:
    def test_missing_cursor_returns_none(self, store: AdaptiveStore) -> None:
        assert store.get_cursor("last_confirmation_end") is None

    def test_set_and_get(self, store: AdaptiveStore) -> None:
        store.set_cursor("last_confirmation_end", NOW.isoformat(), now=NOW)
        assert store.get_cursor("last_confirmation_end") == NOW.isoformat()

    def test_set_overwrites_previous_value(self, store: AdaptiveStore) -> None:
        store.set_cursor("last_confirmation_end", "v1", now=NOW)
        store.set_cursor("last_confirmation_end", "v2", now=NOW)
        assert store.get_cursor("last_confirmation_end") == "v2"


class TestShadowEligible:
    def test_marking_and_listing(self, store: AdaptiveStore) -> None:
        store.mark_shadow_eligible("challenger-1", now=NOW)
        store.mark_shadow_eligible("challenger-2", now=NOW.replace(hour=1))
        assert store.list_shadow_eligible() == ("challenger-1", "challenger-2")

    def test_marking_twice_is_idempotent(self, store: AdaptiveStore) -> None:
        store.mark_shadow_eligible("challenger-1", now=NOW)
        store.mark_shadow_eligible("challenger-1", now=NOW)
        assert store.list_shadow_eligible() == ("challenger-1",)


class TestShadowOutcomes:
    def test_record_and_read_back(self, store: AdaptiveStore) -> None:
        outcome = ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT, exit_price=110.0, exit_time=NOW, gross_pnl_per_unit=10.0)
        store.record_shadow_outcome("challenger-1", symbol="BTCUSDT", outcome=outcome, now=NOW)
        outcomes = store.shadow_outcomes_for("challenger-1")
        assert outcomes == (outcome,)

    def test_accumulates_multiple_outcomes_across_calls(self, store: AdaptiveStore) -> None:
        for i in range(3):
            outcome = ShadowOutcome(
                exit_reason=ExitReason.STOP_LOSS, exit_price=90.0, exit_time=NOW, gross_pnl_per_unit=float(-i),
            )
            store.record_shadow_outcome("challenger-1", symbol="BTCUSDT", outcome=outcome, now=NOW)
        assert len(store.shadow_outcomes_for("challenger-1")) == 3

    def test_isolated_per_challenger(self, store: AdaptiveStore) -> None:
        outcome = ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT, exit_price=110.0, exit_time=NOW, gross_pnl_per_unit=10.0)
        store.record_shadow_outcome("challenger-1", symbol="BTCUSDT", outcome=outcome, now=NOW)
        assert store.shadow_outcomes_for("challenger-2") == ()


class TestRestartRecovery:
    """THE required test: every piece of state this store owns survives
    a fresh connection (restart-equivalent), exactly like
    `LifecycleStore`'s own `test_survives_fresh_connection_restart_equivalent`."""

    def test_full_state_survives_restart(self, tmp_path) -> None:  # noqa: ANN001
        path = tmp_path / "adaptive.db"
        store1 = AdaptiveStore(path)
        champion = _snapshot("champion-1")
        store1.promote_champion(champion, reason="bootstrap", now=NOW)
        result = decide(
            "challenger-1",
            discovery_challenger=EvidenceSummary(30, 0.6, 10.0, 1.0),
            discovery_champion=EvidenceSummary(30, 0.5, 5.0, 1.0),
            confirmation_challenger=EvidenceSummary(10, 0.6, 5.0, 1.0),
            confirmation_champion=EvidenceSummary(10, 0.5, 2.0, 1.0),
            shadow_challenger=EvidenceSummary(5, 0.6, 3.0, 1.0),
            shadow_champion=EvidenceSummary(5, 0.5, 1.0, 1.0),
        )
        store1.record_decision(result, now=NOW)
        store1.set_cursor("last_confirmation_end", NOW.isoformat(), now=NOW)
        store1.mark_shadow_eligible("challenger-1", now=NOW)
        store1.record_shadow_outcome(
            "challenger-1", symbol="BTCUSDT",
            outcome=ShadowOutcome(exit_reason=ExitReason.TAKE_PROFIT, exit_price=110.0, exit_time=NOW, gross_pnl_per_unit=10.0),
            now=NOW,
        )
        store1.close()

        store2 = AdaptiveStore(path)
        assert store2.current_champion() == champion
        assert store2.load_policy_version("champion-1") == champion
        assert len(store2.recent_decisions()) == 1
        assert store2.recent_decisions()[0]["decision"] == "PROMOTE"
        assert store2.get_cursor("last_confirmation_end") == NOW.isoformat()
        assert store2.list_shadow_eligible() == ("challenger-1",)
        assert len(store2.shadow_outcomes_for("challenger-1")) == 1
        store2.close()


=== FILE: tests/test_adaptive_symbol_score.py ===
"""Adaptive Symbol Intelligence v1 — adaptive/symbol_score.py tests,
including the REQUIRED test: a symbol with exactly 1 trade of
net_pnl = -1000.0 must still score exactly 0.5."""

from __future__ import annotations

import pytest

from adaptive.symbol_score import MIN_SYMBOL_TRADES, learned_factor


class TestBelowThresholdIsNeutral:
    def test_single_extreme_outlier_trade_scores_exactly_neutral(self) -> None:
        """THE required test."""
        assert learned_factor("BTCUSDT", [-1000.0]) == 0.5

    def test_empty_history_scores_exactly_neutral(self) -> None:
        assert learned_factor("BTCUSDT", []) == 0.5

    def test_one_trade_below_threshold_of_positive_pnl_still_neutral(self) -> None:
        assert learned_factor("BTCUSDT", [1_000_000.0]) == 0.5

    def test_exactly_one_below_min_symbol_trades_is_neutral(self) -> None:
        pnls = [10.0] * (MIN_SYMBOL_TRADES - 1)
        assert learned_factor("BTCUSDT", pnls) == 0.5

    def test_min_symbol_trades_is_documented_as_ten(self) -> None:
        assert MIN_SYMBOL_TRADES == 10


class TestAboveThresholdFormula:
    def test_at_threshold_all_wins_scores_maximum(self) -> None:
        pnls = [10.0] * MIN_SYMBOL_TRADES
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(1.0)

    def test_at_threshold_all_losses_scores_minimum(self) -> None:
        pnls = [-10.0] * MIN_SYMBOL_TRADES
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(0.0)

    def test_mixed_result_blends_win_rate_and_profitability_sign(self) -> None:
        # 7 wins of +1, 3 losses of -1 -> win_rate=0.7, net=+4 (positive) -> profitability=1.0
        pnls = [1.0] * 7 + [-1.0] * 3
        expected = 0.5 * 0.7 + 0.5 * 1.0
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(expected)

    def test_net_negative_despite_majority_wins_uses_profitability_zero(self) -> None:
        # 8 tiny wins, 2 huge losses -> win_rate=0.8 but net negative.
        pnls = [1.0] * 8 + [-100.0] * 2
        expected = 0.5 * 0.8 + 0.5 * 0.0
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(expected)

    def test_exact_breakeven_net_pnl_uses_neutral_profitability_component(self) -> None:
        pnls = [10.0] * 5 + [-10.0] * 5  # win_rate=0.5, net=0.0 exactly
        expected = 0.5 * 0.5 + 0.5 * 0.5
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(expected)
        assert learned_factor("BTCUSDT", pnls) == pytest.approx(0.5)

    def test_result_always_within_unit_interval(self) -> None:
        for pnls in ([100.0] * 20, [-100.0] * 20, [1.0, -1.0] * 15, [0.0] * 10):
            score = learned_factor("BTCUSDT", pnls)
            assert 0.0 <= score <= 1.0

    def test_deterministic_for_same_input(self) -> None:
        pnls = [3.0, -1.0, 2.0, -2.0, 5.0, -3.0, 1.0, 1.0, -1.0, 4.0]
        assert learned_factor("ETHUSDT", pnls) == learned_factor("ETHUSDT", pnls)

    def test_symbol_argument_does_not_affect_the_result(self) -> None:
        pnls = [1.0] * MIN_SYMBOL_TRADES
        assert learned_factor("BTCUSDT", pnls) == learned_factor("ETHUSDT", pnls)


=== FILE: tests/test_adaptive_windows.py ===
"""Adaptive Intelligence v1, step 6 — discovery/confirmation window split
tests, including the required anti-overfitting isolation proof."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from adaptive.windows import (
    DiscoveryConfirmationWindows,
    WindowConfig,
    compute_cycle_windows,
    next_confirmation_window,
)

_ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)
_CONFIG = WindowConfig(discovery_window_size=timedelta(days=30), confirmation_window_size=timedelta(days=7))


class TestNextConfirmationWindow:
    def test_first_window_starts_at_anchor(self) -> None:
        window = next_confirmation_window(
            last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=_ANCHOR + timedelta(days=10),
        )
        assert window is not None
        assert window.start == _ANCHOR
        assert window.end == _ANCHOR + timedelta(days=7)

    def test_none_when_window_has_not_elapsed_yet(self) -> None:
        window = next_confirmation_window(
            last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=_ANCHOR + timedelta(days=3),
        )
        assert window is None

    def test_advances_from_last_confirmation_end(self) -> None:
        previous_end = _ANCHOR + timedelta(days=7)
        window = next_confirmation_window(
            last_confirmation_end=previous_end, anchor=_ANCHOR, config=_CONFIG, now=previous_end + timedelta(days=8),
        )
        assert window is not None
        assert window.start == previous_end
        assert window.end == previous_end + timedelta(days=7)

    def test_never_extends_past_now(self) -> None:
        # now is exactly at the boundary -- window is exactly consumable, not "one tick short".
        window = next_confirmation_window(
            last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=_ANCHOR + timedelta(days=7),
        )
        assert window is not None
        assert window.end <= _ANCHOR + timedelta(days=7)


class TestComputeCycleWindows:
    def test_pairs_discovery_directly_before_confirmation(self) -> None:
        result = compute_cycle_windows(
            last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=_ANCHOR + timedelta(days=40),
        )
        assert result is not None
        assert result.discovery.end == result.confirmation.start
        assert result.discovery.start == result.confirmation.start - timedelta(days=30)

    def test_returns_none_when_confirmation_not_ready(self) -> None:
        result = compute_cycle_windows(
            last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=_ANCHOR + timedelta(days=1),
        )
        assert result is None

    def test_construction_rejects_overlapping_windows(self) -> None:
        from research.oos_stability import OOSWindow

        overlapping_discovery = OOSWindow(index=0, start=_ANCHOR, end=_ANCHOR + timedelta(days=10))
        overlapping_confirmation = OOSWindow(
            index=0, start=_ANCHOR + timedelta(days=5), end=_ANCHOR + timedelta(days=12),
        )
        with pytest.raises(ValueError):
            DiscoveryConfirmationWindows(discovery=overlapping_discovery, confirmation=overlapping_confirmation)


class TestWindowConfigValidation:
    def test_rejects_non_positive_discovery_size(self) -> None:
        with pytest.raises(ValueError):
            WindowConfig(discovery_window_size=timedelta(0), confirmation_window_size=timedelta(days=7))

    def test_rejects_non_positive_confirmation_size(self) -> None:
        with pytest.raises(ValueError):
            WindowConfig(discovery_window_size=timedelta(days=30), confirmation_window_size=timedelta(0))


class TestConfirmationWindowNeverReusedAcrossCycles:
    def test_second_call_with_same_last_confirmation_end_returns_same_window_not_a_new_one(self) -> None:
        """A caller that has NOT yet advanced/persisted `last_confirmation_
        end` (e.g. a crashed cycle that never completed its promotion
        decision) must get back the exact SAME confirmation window on
        retry -- never silently skip ahead and never silently reuse a
        window whose interval has already been consumed and advanced."""
        now = _ANCHOR + timedelta(days=40)
        first = next_confirmation_window(last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=now)
        second = next_confirmation_window(last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=now)
        assert first == second

    def test_advancing_last_confirmation_end_yields_a_disjoint_later_window(self) -> None:
        now = _ANCHOR + timedelta(days=60)
        first = next_confirmation_window(last_confirmation_end=None, anchor=_ANCHOR, config=_CONFIG, now=now)
        second = next_confirmation_window(
            last_confirmation_end=first.end, anchor=_ANCHOR, config=_CONFIG, now=now,
        )
        assert second.start == first.end
        assert second.start >= first.end  # disjoint, never re-consumes [first.start, first.end)


class TestDiscoveryConfirmationIsolationStaticProof:
    """The mission's REQUIRED test: prove no confirmation-window data is
    EVER read by challenger-generation or discovery-comparison code. This
    is enforced two ways:

    1. Structurally (above): `DiscoveryConfirmationWindows.__post_init__`
       makes it IMPOSSIBLE to even construct a discovery+confirmation
       pair where discovery extends into confirmation.
    2. Statically here: `adaptive/challenger.py` (challenger generation)
       has no notion of "confirmation" at all -- it operates purely on a
       `PolicySnapshot`, a seed, and a config, with zero window/date
       parameters, so it CANNOT read either window's data by
       construction. This is verified by asserting the module never even
       mentions "confirmation" anywhere in its source."""

    def test_challenger_module_has_no_confirmation_window_concept_at_all(self) -> None:
        path = Path(__file__).resolve().parents[1] / "adaptive" / "challenger.py"
        text = path.read_text(encoding="utf-8")
        assert "confirmation" not in text.lower()

    def test_windows_module_never_hands_discovery_code_the_confirmation_object(self) -> None:
        """`compute_cycle_windows()` is the ONE function that ever sees
        both windows together; confirm its discovery-only consumers would
        only ever receive `.discovery` -- i.e. `next_confirmation_window`
        (the confirmation-window constructor) is never called from
        anywhere that also touches challenger generation. Verified via
        AST: `adaptive/challenger.py` contains no call to
        `next_confirmation_window` or `compute_cycle_windows`."""
        path = Path(__file__).resolve().parents[1] / "adaptive" / "challenger.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        called_names = {
            node.func.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "next_confirmation_window" not in called_names
        assert "compute_cycle_windows" not in called_names


=== FILE: tests/test_agent_context.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.agents.context import AgentContext, build_agent_context, compute_context_id
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import AgentInputError
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_snapshot(timeframe, as_of=T0, symbol="BTCUSDT", values=None) -> FeatureSnapshot:
    return FeatureSnapshot(symbol=symbol, timeframe=timeframe, as_of=as_of, values=values or {"X": 1.0})


def make_context(**overrides) -> AgentContext:
    defaults = dict(
        symbol="BTCUSDT", as_of=T0, context_id="abc123",
        m1=make_snapshot(Timeframe.M1), m5=make_snapshot(Timeframe.M5),
        m15=make_snapshot(Timeframe.M15), h1=make_snapshot(Timeframe.H1),
    )
    defaults.update(overrides)
    return AgentContext(**defaults)


class TestContextIdDeterminism:
    def test_same_inputs_same_id(self) -> None:
        id1 = compute_context_id("BTCUSDT", T0, T0, T0, T0, T0)
        id2 = compute_context_id("BTCUSDT", T0, T0, T0, T0, T0)
        assert id1 == id2

    def test_changed_timestamp_changes_id(self) -> None:
        id1 = compute_context_id("BTCUSDT", T0, T0, T0, T0, T0)
        id2 = compute_context_id("BTCUSDT", T0, T0 + timedelta(minutes=1), T0, T0, T0)
        assert id1 != id2

    def test_no_randomness_across_calls(self) -> None:
        ids = {compute_context_id("BTCUSDT", T0, T0, T0, T0, T0) for _ in range(10)}
        assert len(ids) == 1

    def test_symbol_normalized_in_id(self) -> None:
        id1 = compute_context_id("btcusdt", T0, T0, T0, T0, T0)
        id2 = compute_context_id("BTCUSDT", T0, T0, T0, T0, T0)
        assert id1 == id2

    def test_different_symbol_different_id(self) -> None:
        id1 = compute_context_id("BTCUSDT", T0, T0, T0, T0, T0)
        id2 = compute_context_id("ETHUSDT", T0, T0, T0, T0, T0)
        assert id1 != id2


class TestAgentContextValidation:
    def test_valid_context(self) -> None:
        context = make_context()
        assert context.symbol == "BTCUSDT"

    def test_symbol_normalized(self) -> None:
        context = make_context(
            symbol="btcusdt",
            m1=make_snapshot(Timeframe.M1), m5=make_snapshot(Timeframe.M5),
            m15=make_snapshot(Timeframe.M15), h1=make_snapshot(Timeframe.H1),
        )
        assert context.symbol == "BTCUSDT"

    def test_naive_as_of_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            make_context(as_of=datetime(2026, 8, 31))

    def test_mixed_symbol_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            make_context(m1=make_snapshot(Timeframe.M1, symbol="ETHUSDT"))

    def test_wrong_timeframe_for_slot_rejected(self) -> None:
        with pytest.raises(ValueError, match="timeframe"):
            make_context(m1=make_snapshot(Timeframe.M5))  # M1 slotuna M5 snapshot

    def test_future_snapshot_rejected(self) -> None:
        with pytest.raises(ValueError, match="gelecekten"):
            make_context(m1=make_snapshot(Timeframe.M1, as_of=T0 + timedelta(minutes=1)))

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context_id"):
            make_context(context_id="  ")

    def test_snapshot_for_lookup(self) -> None:
        context = make_context()
        assert context.snapshot_for(Timeframe.M5).timeframe == Timeframe.M5


class TestBuildAgentContext:
    def test_builds_from_history(self) -> None:
        history = FeatureHistoryStore()
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            history.commit(make_snapshot(tf))
        context = build_agent_context(history, "BTCUSDT", T0)
        assert context.symbol == "BTCUSDT"
        assert context.m1.timeframe == Timeframe.M1

    def test_missing_timeframe_raises(self) -> None:
        history = FeatureHistoryStore()
        history.commit(make_snapshot(Timeframe.M1))
        history.commit(make_snapshot(Timeframe.M5))
        # M15, H1 eksik
        with pytest.raises(AgentInputError):
            build_agent_context(history, "BTCUSDT", T0)

    def test_historical_lookup_does_not_leak_future_state(self) -> None:
        """KRİTİK no-look-ahead testi: builder, `as_of`'tan SONRAKİ bir
        snapshot'ı ASLA kullanmamalı."""
        history = FeatureHistoryStore()
        early_as_of = T0
        late_as_of = T0 + timedelta(minutes=10)
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            history.commit(make_snapshot(tf, as_of=early_as_of, values={"X": 1.0}))
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            history.commit(make_snapshot(tf, as_of=late_as_of, values={"X": 999.0}))

        context = build_agent_context(history, "BTCUSDT", early_as_of)
        assert context.m1.get("X") == 1.0
        assert context.m5.get("X") == 1.0
        assert context.m15.get("X") == 1.0
        assert context.h1.get("X") == 1.0

    def test_uses_most_recent_snapshot_not_exceeding_as_of(self) -> None:
        history = FeatureHistoryStore()
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            history.commit(make_snapshot(tf, as_of=T0, values={"X": 1.0}))
            history.commit(make_snapshot(tf, as_of=T0 + timedelta(seconds=30), values={"X": 2.0}))
        context = build_agent_context(history, "BTCUSDT", T0 + timedelta(seconds=45))
        assert context.m1.get("X") == 2.0


=== FILE: tests/test_agent_independence.py ===
import inspect

from crypto_signal_engine.agents.market_structure import MarketStructureAgent
from crypto_signal_engine.agents.order_book import OrderBookAgent
from crypto_signal_engine.agents.quant import QuantAgent
from crypto_signal_engine.agents.regime import RegimeAgent


class TestAgentIndependence:
    """Yapısal kanıt: hiçbir agent'ın `evaluate()` imzası AgentEvidence,
    ConsensusResult, RiskAssessment, Signal, veya başka bir agent nesnesi
    ALMAZ. Tek çapraz-ürün nesnesi `AgentContext`'tir."""

    AGENT_CLASSES = [QuantAgent, MarketStructureAgent, OrderBookAgent, RegimeAgent]

    def test_all_agents_evaluate_signature_is_context_only(self) -> None:
        for agent_cls in self.AGENT_CLASSES:
            sig = inspect.signature(agent_cls.evaluate)
            params = list(sig.parameters.keys())
            assert params == ["self", "context"], f"{agent_cls.__name__}.evaluate() imzası beklenmedik: {params}"

    def test_no_agent_module_imports_consensus_or_signal_engine(self) -> None:
        import crypto_signal_engine.agents.market_structure as ms
        import crypto_signal_engine.agents.order_book as ob
        import crypto_signal_engine.agents.quant as q
        import crypto_signal_engine.agents.regime as r

        forbidden_imports = ["crypto_signal_engine.consensus", "crypto_signal_engine.signal_engine"]
        for module in (q, ms, ob, r):
            source = inspect.getsource(module)
            for forbidden in forbidden_imports:
                assert forbidden not in source, f"{module.__name__}, {forbidden} import ediyor (yasak bağımlılık)"

    def test_agents_do_not_reference_evidence_or_consensus_types(self) -> None:
        import crypto_signal_engine.agents.market_structure as ms
        import crypto_signal_engine.agents.order_book as ob
        import crypto_signal_engine.agents.quant as q

        forbidden_type_names = ["ConsensusResult", "RiskAssessment", "Signal("]
        for module in (q, ms, ob):
            source = inspect.getsource(module)
            for forbidden in forbidden_type_names:
                assert forbidden not in source, f"{module.__name__} yasak bir tipe referans veriyor: {forbidden}"


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


