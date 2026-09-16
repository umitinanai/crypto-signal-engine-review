<!-- full_p02.md — Part 2/7 — 34 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/agents/market_structure.py (4624 bytes) -->
<!--   - crypto_signal_engine/agents/order_book.py (3590 bytes) -->
<!--   - crypto_signal_engine/agents/quant.py (3582 bytes) -->
<!--   - crypto_signal_engine/agents/regime.py (6627 bytes) -->
<!--   - crypto_signal_engine/app.py (88536 bytes) -->
<!--   - crypto_signal_engine/consensus/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/consensus/engine.py (3393 bytes) -->
<!--   - crypto_signal_engine/consensus/risk.py (6549 bytes) -->
<!--   - crypto_signal_engine/domain/__init__.py (1 bytes) -->
<!--   - crypto_signal_engine/domain/_validation.py (4495 bytes) -->
<!--   - crypto_signal_engine/domain/candle_sequencing.py (6086 bytes) -->
<!--   - crypto_signal_engine/domain/consensus.py (6878 bytes) -->
<!--   - crypto_signal_engine/domain/enums.py (4894 bytes) -->
<!--   - crypto_signal_engine/domain/events.py (4293 bytes) -->
<!--   - crypto_signal_engine/domain/models.py (16200 bytes) -->
<!--   - crypto_signal_engine/domain/state_contract.py (4754 bytes) -->
<!--   - crypto_signal_engine/errors.py (6974 bytes) -->
<!--   - crypto_signal_engine/execution/__init__.py (2204 bytes) -->
<!--   - crypto_signal_engine/execution/adapter.py (15258 bytes) -->
<!--   - crypto_signal_engine/execution/bridge_runtime.py (10784 bytes) -->
<!--   - crypto_signal_engine/execution/errors.py (7097 bytes) -->
<!--   - crypto_signal_engine/execution/factory.py (4586 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle.py (39404 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_manager.py (54203 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_migration.py (14570 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_replay_sanity.py (9330 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_runtime.py (12473 bytes) -->
<!--   - crypto_signal_engine/execution/lifecycle_store.py (34281 bytes) -->
<!--   - crypto_signal_engine/execution/models.py (9244 bytes) -->
<!--   - crypto_signal_engine/execution/reconciliation_models.py (11833 bytes) -->
<!--   - crypto_signal_engine/execution/reconciliation_service.py (22363 bytes) -->
<!--   - crypto_signal_engine/execution/reconciliation_store.py (12386 bytes) -->
<!--   - crypto_signal_engine/execution/signal_bridge.py (34261 bytes) -->
<!--   - crypto_signal_engine/execution/signer.py (1958 bytes) -->

=== FILE: crypto_signal_engine/agents/market_structure.py ===
"""
MarketStructureAgent — M15 (structural trend timeframe).

Formüller (Faz 4 Bölüm 16):

    roc_component = clamp(ROC_10 / 3.0, -1, 1)
    high_proximity = clamp(1 - DIST_FROM_HIGH_20 / 0.03, 0, 1)
    low_proximity  = clamp(1 - DIST_FROM_LOW_20 / 0.03, 0, 1)
    position_component = high_proximity - low_proximity

VWAP_DEVIATION_20 OPSİYONELDİR (Bölüm 16); mevcutsa küçük bir ek ağırlıkla
karışıma katılır, yoksa yalnızca roc/position ağırlıkları kullanılır (fixed
weight redistribution DEĞİL — basit, dokümante edilmiş iki-mod politikası):

    mevcutsa:  0.45*roc + 0.45*position + 0.10*vwap_component
    değilse:   0.50*roc + 0.50*position

RELATIVE_VOLUME_20 ve BODY_TO_RANGE_RATIO yönü OLUŞTURMAZ; yalnızca
conviction'ı pozitif, sign-preserving çarpanlarla modestly attenuate eder.
"""

from __future__ import annotations

from crypto_signal_engine.agents.base import clamp, require_features
from crypto_signal_engine.agents.context import AgentContext
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.enums import AgentName, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence

REQUIRED_FEATURES = ["ROC_10", "DIST_FROM_HIGH_20", "DIST_FROM_LOW_20", "RELATIVE_VOLUME_20", "BODY_TO_RANGE_RATIO"]
OPTIONAL_VWAP_FEATURE = "VWAP_DEVIATION_20"

_ROC_SCALE = 3.0
_PROXIMITY_SCALE = 0.03
_VWAP_SCALE = 0.02

_WEIGHT_ROC_WITH_VWAP = 0.45
_WEIGHT_POSITION_WITH_VWAP = 0.45
_WEIGHT_VWAP = 0.10
_WEIGHT_ROC_NO_VWAP = 0.50
_WEIGHT_POSITION_NO_VWAP = 0.50


class MarketStructureAgent:
    """M15 üzerinde trend/rolling-high-low pozisyon kompozisyonu."""

    def evaluate(self, context: AgentContext) -> AgentEvidence:
        snapshot = context.m15
        values = require_features(snapshot, REQUIRED_FEATURES, "MarketStructureAgent", Timeframe.M15.value)
        vwap_deviation = snapshot.get(OPTIONAL_VWAP_FEATURE)  # opsiyonel

        roc_component = clamp(values["ROC_10"] / _ROC_SCALE, -1.0, 1.0)
        high_proximity = clamp(1.0 - values["DIST_FROM_HIGH_20"] / _PROXIMITY_SCALE, 0.0, 1.0)
        low_proximity = clamp(1.0 - values["DIST_FROM_LOW_20"] / _PROXIMITY_SCALE, 0.0, 1.0)
        position_component = high_proximity - low_proximity

        if vwap_deviation is not None:
            vwap_component = clamp(vwap_deviation / _VWAP_SCALE, -1.0, 1.0)
            direction = (
                _WEIGHT_ROC_WITH_VWAP * roc_component
                + _WEIGHT_POSITION_WITH_VWAP * position_component
                + _WEIGHT_VWAP * vwap_component
            )
        else:
            vwap_component = None
            direction = _WEIGHT_ROC_NO_VWAP * roc_component + _WEIGHT_POSITION_NO_VWAP * position_component

        relative_volume = values["RELATIVE_VOLUME_20"]
        volume_multiplier = 1.0 if relative_volume >= 1.0 else clamp(0.5 + 0.5 * relative_volume, 0.5, 1.0)

        body_ratio = values["BODY_TO_RANGE_RATIO"]
        body_multiplier = clamp(0.5 + 0.5 * body_ratio, 0.5, 1.0)

        score = require_finite(
            clamp(direction * volume_multiplier * body_multiplier, -1.0, 1.0), "MarketStructureAgent.score"
        )

        vwap_clause = (
            f"VWAP_DEVIATION_20={vwap_deviation:+.4f} (bileşen {vwap_component:+.2f}), "
            if vwap_deviation is not None
            else "VWAP_DEVIATION_20 mevcut değil (opsiyonel), "
        )
        rationale = (
            f"MarketStructureAgent (M15): ROC_10={values['ROC_10']:.2f}% (bileşen {roc_component:+.2f}), "
            f"high_proximity={high_proximity:.2f}, low_proximity={low_proximity:.2f} "
            f"(pozisyon bileşeni {position_component:+.2f}), " + vwap_clause +
            f"relative_volume={relative_volume:.2f} (x{volume_multiplier:.2f}), "
            f"body_to_range_ratio={body_ratio:.2f} (x{body_multiplier:.2f})."
        )

        metrics = {
            "roc_component": roc_component,
            "position_component": position_component,
            "volume_multiplier": volume_multiplier,
            "body_multiplier": body_multiplier,
            **values,
        }
        if vwap_deviation is not None:
            metrics["vwap_component"] = vwap_component
            metrics[OPTIONAL_VWAP_FEATURE] = vwap_deviation

        return AgentEvidence(
            agent=AgentName.MARKET_STRUCTURE,
            score=score,
            rationale=rationale,
            primary_timeframe=Timeframe.M15,
            symbol=context.symbol,
            as_of=context.as_of,
            context_id=context.context_id,
            supporting_metrics=metrics,
        )


=== FILE: crypto_signal_engine/agents/order_book.py ===
"""
OrderBookAgent — M1 (microstructure/timing confirmation timeframe).

Formüller (Faz 4 Bölüm 17):

    depth_component = clamp(DEPTH_IMBALANCE_10, -1, 1)
    tob_component   = clamp(TOB_IMBALANCE, -1, 1)
    microprice_displacement = (MICROPRICE - MID_PRICE) / MID_PRICE
    micro_component = clamp(microprice_displacement / 0.001, -1, 1)   # ~10bps ölçek

    direction = 0.40*depth_component + 0.30*tob_component + 0.30*micro_component

SPREAD_BPS yön OLUŞTURMAZ; yalnızca conviction/market-quality cezası olarak
kullanılır (pozitif, sign-preserving çarpan, taban 0.3):

    spread_multiplier = clamp(1 - SPREAD_BPS / 50.0, 0.3, 1.0)

Rationale "microstructure confirmation" dilini kullanır; "execution
timing" İFADESİ KULLANILMAZ (Faz 4 Bölüm 17/7 — analiz sisteminde
execution kavramı yoktur).
"""

from __future__ import annotations

from crypto_signal_engine.agents.base import clamp, require_features
from crypto_signal_engine.agents.context import AgentContext
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.enums import AgentName, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence

REQUIRED_FEATURES = ["SPREAD_BPS", "DEPTH_IMBALANCE_10", "TOB_IMBALANCE", "MID_PRICE", "MICROPRICE"]

_WEIGHT_DEPTH = 0.40
_WEIGHT_TOB = 0.30
_WEIGHT_MICRO = 0.30

_MICROPRICE_DISPLACEMENT_SCALE = 0.001
_SPREAD_BPS_PENALTY_SCALE = 50.0
_SPREAD_MULTIPLIER_FLOOR = 0.3


class OrderBookAgent:
    """M1 üzerinde order-book depth/TOB/microprice-displacement kompozisyonu."""

    def evaluate(self, context: AgentContext) -> AgentEvidence:
        snapshot = context.m1
        values = require_features(snapshot, REQUIRED_FEATURES, "OrderBookAgent", Timeframe.M1.value)

        depth_component = clamp(values["DEPTH_IMBALANCE_10"], -1.0, 1.0)
        tob_component = clamp(values["TOB_IMBALANCE"], -1.0, 1.0)

        mid_price = values["MID_PRICE"]
        microprice = values["MICROPRICE"]
        displacement = 0.0 if mid_price == 0.0 else (microprice - mid_price) / mid_price
        micro_component = clamp(displacement / _MICROPRICE_DISPLACEMENT_SCALE, -1.0, 1.0)

        direction = _WEIGHT_DEPTH * depth_component + _WEIGHT_TOB * tob_component + _WEIGHT_MICRO * micro_component

        spread_bps = values["SPREAD_BPS"]
        spread_multiplier = clamp(1.0 - spread_bps / _SPREAD_BPS_PENALTY_SCALE, _SPREAD_MULTIPLIER_FLOOR, 1.0)

        score = require_finite(clamp(direction * spread_multiplier, -1.0, 1.0), "OrderBookAgent.score")

        rationale = (
            f"OrderBookAgent (M1, microstructure confirmation): "
            f"depth_imbalance_10={values['DEPTH_IMBALANCE_10']:+.2f}, "
            f"tob_imbalance={values['TOB_IMBALANCE']:+.2f}, "
            f"microprice_displacement={displacement:+.6f} (bileşen {micro_component:+.2f}), "
            f"spread_bps={spread_bps:.2f} (conviction çarpanı x{spread_multiplier:.2f})."
        )

        return AgentEvidence(
            agent=AgentName.ORDER_BOOK,
            score=score,
            rationale=rationale,
            primary_timeframe=Timeframe.M1,
            symbol=context.symbol,
            as_of=context.as_of,
            context_id=context.context_id,
            supporting_metrics={
                "depth_component": depth_component,
                "tob_component": tob_component,
                "micro_component": micro_component,
                "microprice_displacement": displacement,
                "spread_multiplier": spread_multiplier,
                **values,
            },
        )


=== FILE: crypto_signal_engine/agents/quant.py ===
"""
QuantAgent — M5 (primary signal timeframe).

Formüller (Faz 4 Bölüm 15, dokümante edilmiş sabit ağırlıklar):

    rsi_component  = clamp((RSI_14 - 50) / 25, -1, 1)
    roc_component  = clamp(ROC_10 / 3.0, -1, 1)          # ROC_10 yüzde birimindedir
    vwap_component = clamp(VWAP_DEVIATION_20 / 0.02, -1, 1)

    direction = 0.30*rsi_component + 0.40*roc_component + 0.30*vwap_component

RELATIVE_VOLUME_20, direction'ı OLUŞTURMAZ; yalnızca conviction'ı
(pozitif, sign-preserving bir çarpanla) modestly attenuate eder — düşük
hacim, yönü ASLA TERSİNE ÇEVİRMEZ (çarpan her zaman [0.5, 1.0] aralığında,
pozitiftir):

    volume_multiplier = 1.0                          if RELATIVE_VOLUME_20 >= 1.0
                       = clamp(0.5 + 0.5*rel_vol, 0.5, 1.0)  aksi halde

BOLLINGER_BANDWIDTH_20_2 yalnızca rationale/metrics'te kullanılır, skora
KATILMAZ (bu, "must not create direction by themselves" kuralının en
muhafazakâr uygulanışıdır).
"""

from __future__ import annotations

from crypto_signal_engine.agents.base import clamp, require_features
from crypto_signal_engine.agents.context import AgentContext
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.enums import AgentName, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence

REQUIRED_FEATURES = ["RSI_14", "ROC_10", "VWAP_DEVIATION_20", "RELATIVE_VOLUME_20", "BOLLINGER_BANDWIDTH_20_2"]

_WEIGHT_RSI = 0.30
_WEIGHT_ROC = 0.40
_WEIGHT_VWAP = 0.30

_RSI_NEUTRAL = 50.0
_RSI_SCALE = 25.0
_ROC_SCALE = 3.0
_VWAP_SCALE = 0.02


class QuantAgent:
    """M5 üzerinde RSI/ROC/VWAP-deviation directional kompozisyonu."""

    def evaluate(self, context: AgentContext) -> AgentEvidence:
        snapshot = context.m5
        values = require_features(snapshot, REQUIRED_FEATURES, "QuantAgent", Timeframe.M5.value)

        rsi_component = clamp((values["RSI_14"] - _RSI_NEUTRAL) / _RSI_SCALE, -1.0, 1.0)
        roc_component = clamp(values["ROC_10"] / _ROC_SCALE, -1.0, 1.0)
        vwap_component = clamp(values["VWAP_DEVIATION_20"] / _VWAP_SCALE, -1.0, 1.0)

        direction = _WEIGHT_RSI * rsi_component + _WEIGHT_ROC * roc_component + _WEIGHT_VWAP * vwap_component

        relative_volume = values["RELATIVE_VOLUME_20"]
        volume_multiplier = 1.0 if relative_volume >= 1.0 else clamp(0.5 + 0.5 * relative_volume, 0.5, 1.0)

        score = require_finite(clamp(direction * volume_multiplier, -1.0, 1.0), "QuantAgent.score")

        rationale = (
            f"QuantAgent (M5): RSI_14={values['RSI_14']:.2f} (bileşen {rsi_component:+.2f}), "
            f"ROC_10={values['ROC_10']:.2f}% (bileşen {roc_component:+.2f}), "
            f"VWAP_DEVIATION_20={values['VWAP_DEVIATION_20']:+.4f} (bileşen {vwap_component:+.2f}), "
            f"relative_volume={relative_volume:.2f} (conviction çarpanı x{volume_multiplier:.2f}), "
            f"bollinger_bandwidth={values['BOLLINGER_BANDWIDTH_20_2']:.4f} (yalnızca bağlam)."
        )

        return AgentEvidence(
            agent=AgentName.QUANT,
            score=score,
            rationale=rationale,
            primary_timeframe=Timeframe.M5,
            symbol=context.symbol,
            as_of=context.as_of,
            context_id=context.context_id,
            supporting_metrics={
                "rsi_component": rsi_component,
                "roc_component": roc_component,
                "vwap_component": vwap_component,
                "volume_multiplier": volume_multiplier,
                **values,
            },
        )


=== FILE: crypto_signal_engine/agents/regime.py ===
"""
RegimeAgent — H1 (regime timeframe).

RegimeAgent tek başına iki AYRI şey üretir (Faz 4 Bölüm 18-23):
1. Directional `AgentEvidence` (ROC/RSI-around-neutral kompozisyonu —
   yüksek volatilite veya düşük likidite bu skoru OTOMATİK OLARAK
   bearish yapmaz; risk koşulları ile yönlü koşullar kavramsal olarak
   AYRIDIR).
2. `RegimeContext` (StructureRegime + VolatilityRegime + LiquidityRegime —
   üç BAĞIMSIZ eksen, tek bir enum'a indirgenmez).

Sınıflandırma eşikleri (dokümante edilmiş, adlandırılmış sabitler,
BOYUTSUZ/normalize değerler üzerinden — mutlak fiyat seviyesi
KULLANILMAZ):

Structure (öncelik sırası: BREAKOUT > MEAN_REVERSION > TRENDING > RANGING):
- BREAKOUT: |ROC_10| >= 2.0 (%) VE (DIST_FROM_HIGH_20 <= 0.005 VEYA
  DIST_FROM_LOW_20 <= 0.005) — güçlü momentum + rolling extreme'e çok yakınlık.
- MEAN_REVERSION: (RSI_14 >= 70 VE DIST_FROM_HIGH_20 <= 0.01) VEYA
  (RSI_14 <= 30 VE DIST_FROM_LOW_20 <= 0.01) — anlamlı gerilmiş/aşırı
  alım-satım koşulu; GENERIC bir fallback DEĞİLDİR.
- TRENDING: |ROC_10| >= 1.0 (%) (yukarıdaki daha güçlü koşullar
  sağlanmadıysa).
- RANGING: yukarıdakilerin hiçbiri sağlanmazsa (varsayılan, belirsiz durum).

Volatility (BOLLINGER_BANDWIDTH_20_2 — boyutsuz oran):
    < 0.02        -> LOW
    0.02 - 0.05   -> NORMAL
    0.05 - 0.10   -> HIGH
    >= 0.10       -> EXTREME

Liquidity (aligned M1 SPREAD_BPS):
    < 5           -> NORMAL
    5 - 15        -> THIN
    >= 15         -> STRESSED
"""

from __future__ import annotations

from dataclasses import dataclass

from crypto_signal_engine.agents.base import clamp, require_features
from crypto_signal_engine.agents.context import AgentContext
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.consensus import RegimeContext
from crypto_signal_engine.domain.enums import (
    AgentName,
    LiquidityRegime,
    StructureRegime,
    Timeframe,
    VolatilityRegime,
)
from crypto_signal_engine.domain.models import AgentEvidence

REQUIRED_H1_FEATURES = [
    "ROC_10", "RSI_14", "BOLLINGER_BANDWIDTH_20_2", "RELATIVE_VOLUME_20",
    "DIST_FROM_HIGH_20", "DIST_FROM_LOW_20",
]
REQUIRED_M1_FEATURES = ["SPREAD_BPS"]

_BREAKOUT_ROC_THRESHOLD = 2.0
_BREAKOUT_PROXIMITY_THRESHOLD = 0.005
_MEAN_REVERSION_RSI_HIGH = 70.0
_MEAN_REVERSION_RSI_LOW = 30.0
_MEAN_REVERSION_PROXIMITY_THRESHOLD = 0.01
_TREND_ROC_THRESHOLD = 1.0

_VOLATILITY_LOW_MAX = 0.02
_VOLATILITY_NORMAL_MAX = 0.05
_VOLATILITY_HIGH_MAX = 0.10

_LIQUIDITY_NORMAL_MAX = 5.0
_LIQUIDITY_THIN_MAX = 15.0

_ROC_SCALE = 3.0
_RSI_NEUTRAL = 50.0
_RSI_SCALE = 25.0
_WEIGHT_ROC = 0.60
_WEIGHT_RSI = 0.40


@dataclass(frozen=True)
class RegimeAgentOutput:
    """RegimeAgent'ın çıktısı: directional evidence + regime sınıflandırması.

    Bu, `AgentEvidence`'ı DEĞİŞTİRMEZ — ayrı bir Faz 4 wrapper'ıdır.
    """

    evidence: AgentEvidence
    regime: RegimeContext


def _classify_structure(roc: float, rsi: float, dist_from_high: float, dist_from_low: float) -> StructureRegime:
    abs_roc = abs(roc)
    if abs_roc >= _BREAKOUT_ROC_THRESHOLD and (
        dist_from_high <= _BREAKOUT_PROXIMITY_THRESHOLD or dist_from_low <= _BREAKOUT_PROXIMITY_THRESHOLD
    ):
        return StructureRegime.BREAKOUT
    if (rsi >= _MEAN_REVERSION_RSI_HIGH and dist_from_high <= _MEAN_REVERSION_PROXIMITY_THRESHOLD) or (
        rsi <= _MEAN_REVERSION_RSI_LOW and dist_from_low <= _MEAN_REVERSION_PROXIMITY_THRESHOLD
    ):
        return StructureRegime.MEAN_REVERSION
    if abs_roc >= _TREND_ROC_THRESHOLD:
        return StructureRegime.TRENDING
    return StructureRegime.RANGING


def _classify_volatility(bandwidth: float) -> VolatilityRegime:
    if bandwidth < _VOLATILITY_LOW_MAX:
        return VolatilityRegime.LOW
    if bandwidth < _VOLATILITY_NORMAL_MAX:
        return VolatilityRegime.NORMAL
    if bandwidth < _VOLATILITY_HIGH_MAX:
        return VolatilityRegime.HIGH
    return VolatilityRegime.EXTREME


def _classify_liquidity(spread_bps: float) -> LiquidityRegime:
    if spread_bps < _LIQUIDITY_NORMAL_MAX:
        return LiquidityRegime.NORMAL
    if spread_bps < _LIQUIDITY_THIN_MAX:
        return LiquidityRegime.THIN
    return LiquidityRegime.STRESSED


class RegimeAgent:
    """H1 üzerinde regime sınıflandırması + directional evidence.

    Not (Bölüm 19): aligned M1 `SPREAD_BPS`'i (likidite sınıflandırması
    için) `AgentContext`'ten okur — bu, primary feature context'in bir
    parçasıdır; OrderBookAgent'ın `AgentEvidence`'ını ASLA tüketmez.
    """

    def evaluate(self, context: AgentContext) -> RegimeAgentOutput:
        h1 = require_features(context.h1, REQUIRED_H1_FEATURES, "RegimeAgent", Timeframe.H1.value)
        m1 = require_features(context.m1, REQUIRED_M1_FEATURES, "RegimeAgent", f"{Timeframe.M1.value} (aligned)")

        structure = _classify_structure(
            roc=h1["ROC_10"], rsi=h1["RSI_14"],
            dist_from_high=h1["DIST_FROM_HIGH_20"], dist_from_low=h1["DIST_FROM_LOW_20"],
        )
        volatility = _classify_volatility(h1["BOLLINGER_BANDWIDTH_20_2"])
        liquidity = _classify_liquidity(m1["SPREAD_BPS"])
        regime = RegimeContext(structure=structure, volatility=volatility, liquidity=liquidity)

        roc_component = clamp(h1["ROC_10"] / _ROC_SCALE, -1.0, 1.0)
        rsi_component = clamp((h1["RSI_14"] - _RSI_NEUTRAL) / _RSI_SCALE, -1.0, 1.0)
        score = require_finite(
            clamp(_WEIGHT_ROC * roc_component + _WEIGHT_RSI * rsi_component, -1.0, 1.0), "RegimeAgent.score"
        )

        rationale = (
            f"RegimeAgent (H1): structure={structure.value}, volatility={volatility.value}, "
            f"liquidity={liquidity.value} | directional: ROC_10={h1['ROC_10']:.2f}% "
            f"(bileşen {roc_component:+.2f}), RSI_14={h1['RSI_14']:.2f} (bileşen {rsi_component:+.2f}). "
            f"Volatilite/likidite koşulları directional skora DAHİL EDİLMEZ (risk kavramsal olarak ayrıdır)."
        )

        evidence = AgentEvidence(
            agent=AgentName.REGIME,
            score=score,
            rationale=rationale,
            primary_timeframe=Timeframe.H1,
            symbol=context.symbol,
            as_of=context.as_of,
            context_id=context.context_id,
            supporting_metrics={
                "roc_component": roc_component,
                "rsi_component": rsi_component,
                **h1,
                "m1_spread_bps": m1["SPREAD_BPS"],
            },
        )
        return RegimeAgentOutput(evidence=evidence, regime=regime)


=== FILE: crypto_signal_engine/app.py ===
"""
Faz 8 — CLI giriş noktası ve kompozisyon kökü.

Çalıştırma: `python -m crypto_signal_engine.app run` (veya kurulu bir
paket için `crypto-signal-engine run` console script'i, bkz.
pyproject.toml). Konfigürasyon TAMAMEN ortam değişkenlerinden okunur
(`CSE_*`, bkz. `ops/config.py`) — komut satırı argümanı ile parametre
GEÇİLMEZ (systemd `EnvironmentFile=` ile uyumlu, tek kaynak).

Bu modül BİLEREK hiçbir runtime/iş mantığı İÇERMEZ — yalnızca kabul
edilmiş Faz 5 (`PaperTradingEngine`)/Faz 6 (`RuntimeCoordinator`)/Faz 7
(`PersistedRuntime`, `PaperStateStore`) nesnelerini doğru sırayla kurar
ve OS process lifecycle'ına (SIGINT/SIGTERM, exit code) bağlar.

Exit code sözleşmesi:
    0 = temiz shutdown (sinyal veya kontrollü sonlanma)
    1 = fatal/beklenmeyen hata (runtime loop veya startup sırasında)
    2 = geçersiz/eksik konfigürasyon (`CSE_*` ortam değişkenleri)
    3 = process-lock başka bir instance tarafından tutuluyor
    4 = recovery/startup başarısız (runtime KESİNLİKLE başlatılmadı)
    5 = otomatik sembol seçimi başarısız (`CSE_SYMBOLS` verilmedi VE
        PUBLIC Binance discovery/ranking güvenle tamamlanamadı — hiçbir
        sembol İCAT EDİLMEZ/varsayılana düşülmez, runtime başlatılmadı)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.errors import SymbolSelectionError
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.execution.factory import build_testnet_execution_service, testnet_credentials_status
from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig, trading_day_key, unrealized_gross_pnl
from crypto_signal_engine.portfolio.accounting import build_accounting_snapshot
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_migration import (
    finalize_all_legacy_lifecycle_init,
    reconstruct_all_legacy_ownership,
)
from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime, latest_m5_atr
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import ExecutionMode
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge, compute_bridge_position
from crypto_signal_engine.execution.testnet_client import BinanceTestnetConfig
from crypto_signal_engine.ops.admin import AdminController
from crypto_signal_engine.ops.config import AppConfig, load_config
from crypto_signal_engine.ops.dashboard import DashboardServer
from crypto_signal_engine.ops.errors import AppConfigurationError, ConcurrentInstanceError
from crypto_signal_engine.ops.event_log import recent_events
from crypto_signal_engine.ops.health_snapshot import read_snapshot, utc_now, write_snapshot
from crypto_signal_engine.ops.lock import ProcessLock
from crypto_signal_engine.ops.logging_setup import configure_logging
from crypto_signal_engine.ops.notifier import Notifier, build_telegram_notifier, safe_notify
from crypto_signal_engine.ops.systemd_notify import notify_watchdog
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.providers.base import LiveDataProvider
from crypto_signal_engine.providers.binance.clock import AsyncioSleeper, SystemClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.provider import BinanceMarketDataProvider
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.transport import UrllibHttpClient
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.reselection_scheduler import ReselectionScheduler
from crypto_signal_engine.selection.models import SelectionResult
from crypto_signal_engine.selection.selector import AutomaticSymbolSelector

_LOGGER = logging.getLogger("crypto_signal_engine.app")

_CANDLE_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)

EXIT_OK = 0
EXIT_FATAL = 1
EXIT_CONFIG_ERROR = 2
EXIT_LOCK_HELD = 3
EXIT_STARTUP_FAILURE = 4
EXIT_SYMBOL_SELECTION_FAILURE = 5

_DEFAULT_EXECUTION_DB_PATH = "var/lib/crypto-signal-engine/testnet_execution.db"


def _execution_db_path() -> Path:
    """Faz 11'in `scripts/binance_testnet_lab.py::_execution_db_path()`'i
    ile AYNI ortam değişkeni/varsayılan — TEK bir execution-db yolu
    kuralı, hem manuel CLI hem üretim `Application` tarafından
    PAYLAŞILIR (bkz. Bölüm "no redesign")."""
    return Path(os.environ.get("BINANCE_TESTNET_EXECUTION_DB_PATH") or _DEFAULT_EXECUTION_DB_PATH)


def build_execution_service(
    config: AppConfig, *, http_client: object | None = None
) -> tuple[ExecutionReconciliationService, ExecutionStateStore] | None:
    """Faz 12 — TESTNET execution reconciliation servisini KOŞULLU olarak
    kurar. `None` döner (execution TAMAMEN devre dışı) EĞER:
    - `config.execution_mode` != `BINANCE_SPOT_TESTNET`, VEYA
    - `config.enable_testnet_execution` != `True`.

    Bu STRICTLY opt-in kapıdır (Bölüm 2, "Require BOTH") — HER İKİSİ de
    doğru olmadan bu fonksiyon `None` döner ve `Application`, hiçbir
    execution store/client/service İNŞA ETMEZ, dolayısıyla hiçbir private
    order-submission kod yoluna erişim MÜMKÜN OLMAZ. Kimlik bilgileri
    (Faz 10 ilkesiyle AYNI, Karar 71) burada da opsiyoneldir — eksikse
    yalnızca SIGNED bir çağrı (reconciliation query) sırasında
    `MissingCredentialsError` fırlatılır; bu fonksiyonun KENDİSİ
    credential eksikliği için başarısız OLMAZ (fail-closed davranış
    `Application._reconcile_execution_startup()`'ta ele alınır).

    `http_client` (opsiyonel): `build_real_provider`'ın `provider`
    enjeksiyon deseniyle AYNI — offline testler GERÇEK ağa ASLA
    dokunmadan sahte bir testnet HTTP istemcisi enjekte edebilsin diye
    (bkz. tests/execution_fakes.py). Üretimde `None` bırakılır (gerçek
    istemci kullanılır).

    MİMARİ SINIR (bkz. modül docstring'i, "no redesign"): kimlik
    bilgisi okuma/imzalama/servis İNŞASININ KENDİSİ, bu fonksiyonun
    DIŞINDA, `execution/factory.py::build_testnet_execution_service()`
    İÇİNDE kalır — `app.py`'nin KENDİSİ (execution/ paketinin DIŞINDA
    olduğundan) hiçbir HMAC/credential-literal/private-endpoint izi
    İÇERMEZ (bkz. `tests/test_repository_safety_scan.py::
    TestPhase10ExecutionBoundarySafety`)."""
    if config.execution_mode is not ExecutionMode.BINANCE_SPOT_TESTNET or not config.enable_testnet_execution:
        return None
    return build_testnet_execution_service(execution_db_path=_execution_db_path(), http_client=http_client)


def build_signal_testnet_bridge(
    config: AppConfig,
    execution: tuple[ExecutionReconciliationService, ExecutionStateStore] | None,
    *,
    lifecycle_manager: LifecycleManager | None = None,
    atr_provider=None,
    all_symbols_provider=None,
    entries_paused_provider=None,
) -> SignalTestnetBridge | None:
    """Faz 13 — Signal->TESTNET bridge'i KOŞULLU olarak kurar. `None` döner
    (bridge TAMAMEN devre dışı, hiçbir otomatik order kod yoluna erişim
    MÜMKÜN DEĞİL) EĞER:
    - `execution` `None`'sa (yani `build_execution_service()` ZATEN `None`
      döndü — `execution_mode != BINANCE_SPOT_TESTNET` VEYA
      `enable_testnet_execution != True`), VEYA
    - `config.enable_signal_testnet_bridge != True`.

    Bu, talimatın "EXPLICIT SAFETY GATE" bölümünün AND-birleşimini
    (execution mode + testnet gate + bridge gate) YAPISAL olarak sağlar —
    üçünden biri bile eksikse bu fonksiyon `None` döner. `SignalTestnetBridge`
    kendisi hiçbir credential OKUMAZ (`execution_service.client` üzerinden
    ZATEN inşa edilmiş Faz 10 istemcisini yeniden kullanır) — bu yüzden bu
    fonksiyonun `app.py` içinde kalması mimari sınırı (execution/ paketinin
    DIŞINDA HMAC/credential-inşa kodu YOK) İHLAL ETMEZ."""
    if execution is None or not config.enable_signal_testnet_bridge:
        return None
    execution_service, execution_store = execution
    return SignalTestnetBridge(
        execution_service=execution_service, execution_store=execution_store,
        notional_usdt=config.testnet_bridge_notional_usdt, lifecycle_manager=lifecycle_manager,
        atr_provider=atr_provider, all_symbols_provider=all_symbols_provider,
        entries_paused_provider=entries_paused_provider,
    )


def build_lifecycle_manager(
    config: AppConfig, execution: tuple[ExecutionReconciliationService, ExecutionStateStore] | None,
    *, exit_policy_provider: Callable[[], tuple[ExitPolicyConfig, str | None]] | None = None,
    notifier: Notifier | None = None, event_log_path: Path | None = None,
) -> tuple[LifecycleStore, LifecycleManager] | None:
    """Autonomous Testnet trading lifecycle — same STRICT opt-in gate as
    `build_signal_testnet_bridge` (the lifecycle IS the bridge's exit/risk
    half, not a separate feature): `None` unless `execution` is present
    AND `config.enable_signal_testnet_bridge` is true. `LifecycleStore`
    opens its OWN connection to the SAME execution-db file (additive
    tables only, see `lifecycle_store.py`).

    `exit_policy_provider` (Adaptive Intelligence v1, step 14) — additive,
    optional, `None` by default. This module never imports `adaptive/`
    (see the hard dependency-direction rule); the actual champion-reading
    callable is constructed and passed in by `scripts/run_with_adaptive_
    policy.py`. With `exit_policy_provider=None` (every existing caller,
    including the standalone `Application` composition below), behaviour
    is bit-for-bit identical to before this parameter existed —
    `LifecycleManager` itself falls back to its own static `exit_policy`
    for every new entry (see `lifecycle_manager.py::_resolve_entry_policy`)."""
    if execution is None or not config.enable_signal_testnet_bridge:
        return None
    execution_service, _execution_store = execution
    store = LifecycleStore(_execution_db_path())
    manager = LifecycleManager(
        store=store, execution_service=execution_service,
        exit_policy=config.lifecycle_exit_policy, risk_policy=config.lifecycle_risk_policy,
        exit_policy_provider=exit_policy_provider, notifier=notifier, event_log_path=event_log_path,
    )
    return store, manager


def build_real_provider(config: AppConfig) -> BinanceMarketDataProvider:
    """Üretim (gerçek Binance PUBLIC bağlantısı) provider'ını kurar.

    `websockets` paketi burada, yalnızca GERÇEKTEN çağrıldığında lazy
    import edilir (bkz. `real_websocket.py` docstring'i) — offline
    testler bu fonksiyonu HİÇ çağırmaz, bunun yerine `Application`'a
    doğrudan bir sahte (`FakeLiveDataProvider`) provider enjekte eder."""
    from crypto_signal_engine.providers.binance.real_websocket import RealWebSocketConnectionFactory

    return BinanceMarketDataProvider(
        config=config.binance_config(),
        http_client=UrllibHttpClient(),
        ws_factory=RealWebSocketConnectionFactory(),
        clock=SystemClock(),
        sleeper=AsyncioSleeper(),
    )


def build_discovery_rest_client() -> BinanceRestClient:
    """Otomatik sembol seçimi için (kimlik bilgisi GEREKTİRMEYEN) keşif
    amaçlı bir `BinanceRestClient` kurar. `BinanceConfig()` KASITLI OLARAK
    varsayılan (boş `supported_symbols`) ile inşa edilir — bu aşamada
    HANGİ sembollerin izleneceği HENÜZ bilinmiyor (bkz. `resolve_symbols`);
    `fetch_exchange_info`/`fetch_24hr_tickers`/`fetch_historical_candles`
    `supported_symbols`'a bağımlı DEĞİLDİR."""
    return BinanceRestClient(BinanceConfig(), UrllibHttpClient(), SystemClock(), AsyncioSleeper())


def _read_open_position_symbols(db_path: Path) -> tuple[str, ...]:
    """Bölüm A güvenlik kuralı ("Existing open PAPER positions"): durable
    bir AÇIK pozisyonu olan HERHANGİ bir sembol, otomatik fırsat ranking'i
    onu dışarıda bıraksa BİLE runtime izleme kümesinden ASLA düşemez.

    Kısa ömürlü, TAMAMEN AYRI bir `PaperStateStore` açılıp okunduktan
    HEMEN sonra kapatılır — `Application`'ın kendi (uzun ömürlü) store'u
    bundan bağımsızdır. Bu, `resolve_symbols()`'ın (dolayısıyla nihai
    sembol kümesinin) `Application`/`RuntimeCoordinator`/provider İNŞA
    EDİLMEDEN ÖNCE hesaplanabilmesi için gereklidir — `RuntimeCoordinator`
    hangi sembollerin izleneceğini yalnızca CONSTRUCTION anında, bir kez
    alır (bkz. Faz 6, DEĞİŞTİRİLMEDİ); bu yüzden pinning'in provider/
    coordinator inşasından SONRA yapılması zaten ÇOK GEÇ olurdu."""
    store = PaperStateStore(db_path)
    try:
        return store.list_open_position_symbols()
    finally:
        store.close()


def _read_bridge_owned_symbols(config: AppConfig) -> tuple[str, ...]:
    """Autonomous Testnet trading lifecycle Phase 4/16 — extends the
    PAPER-open-position pinning above to ALSO cover REAL bridge-owned
    Testnet inventory (or unresolved ambiguity). Without this, a symbol
    with real owned inventory but no correlated PAPER position (not
    structurally guaranteed to always coincide) could be silently dropped
    from `config.symbols` by opportunity ranking BEFORE `Application` (and
    therefore legacy-position migration) ever sees it — this closes that
    gap at the exact same point `_read_open_position_symbols` already
    closes it for PAPER. `()` whenever the bridge isn't enabled or its
    execution DB doesn't exist yet (fresh install, or PAPER-only run)."""
    if (
        config.execution_mode is not ExecutionMode.BINANCE_SPOT_TESTNET
        or not config.enable_testnet_execution
        or not config.enable_signal_testnet_bridge
    ):
        return ()
    db_path = _execution_db_path()
    if not db_path.exists():
        return ()
    store = ExecutionStateStore(db_path)
    try:
        owned: list[str] = []
        for symbol in store.list_all_symbols():
            position = compute_bridge_position(store, symbol)
            if position.ambiguous or position.owned_quantity > 0:
                owned.append(symbol)
        return tuple(owned)
    finally:
        store.close()


async def resolve_symbols(
    config: AppConfig, *, rest_client: BinanceRestClient | None = None
) -> tuple[AppConfig, SelectionResult | None, tuple[str, ...]]:
    """`CSE_SYMBOLS` AÇIKÇA verilmişse `config`'i DEĞİŞTİRMEDEN döner
    (manual override AYNEN korunur — bkz. Bölüm "manual override remains
    available" — hiçbir pinning/augmentation UYGULANMAZ, `None` seçim
    sonucu ve boş pinned tuple ile).

    Verilmemişse (`config.auto_select_symbols`), PUBLIC Binance market
    data kullanarak `AutomaticSymbolSelector.select()` çalıştırır, SONRA
    durable PAPER state'te AÇIK pozisyonu olan sembolleri okur (bkz.
    `_read_open_position_symbols`) ve nihai `config.symbols`'ı
    `pinned + opportunity-selected` (dedup edilmiş, pinned ÖNCE) birleşimi
    ile DOLDURULMUŞ yeni bir `AppConfig` döner. Pinned bir sembol, hedef
    `target_count`'u aşmak PAHASINA bile ASLA dışarıda bırakılmaz — bu
    KASITLI OLARAK "sayı sabit kalsın" değil "açık pozisyon ASLA
    izlemesiz kalmasın" invariant'ını önceliklendirir.

    Bu fonksiyon `main()`'de, `Application`/`build_real_provider` İNŞA
    EDİLMEDEN ÖNCE çağrılır — ikisi de ARTIK nihai `config.symbols`'a
    bağımlıdır. Discovery/ranking güvenle tamamlanamazsa
    `SymbolSelectionError` YUKARI FIRLATILIR (yakalanmaz) — hiçbir sembole
    sessizce düşülmez; hiçbir durable state'e (lock/SQLite) HENÜZ
    dokunulmamış olur (bkz. `main()`)."""
    if not config.auto_select_symbols:
        return config, None, ()

    client = rest_client if rest_client is not None else build_discovery_rest_client()
    selector = AutomaticSymbolSelector(client, SystemClock(), config.symbol_selection)
    result = await selector.select()

    pinned = tuple(dict.fromkeys((*_read_open_position_symbols(config.db_path), *_read_bridge_owned_symbols(config))))
    final_symbols = tuple(dict.fromkeys((*pinned, *result.selected_symbols)))
    resolved_config = replace(config, symbols=final_symbols)
    return resolved_config, result, pinned


def _compose_m1_candle_observers(
    a: Callable[[str, LifecycleCandle], None] | None,
    b: Callable[[str, LifecycleCandle], None] | None,
) -> Callable[[str, LifecycleCandle], None] | None:
    """Dashboard localization fix — `Application` now needs its OWN M1
    observer (feeding `_latest_price()`'s cache, see `_record_m1_price`)
    at the SAME hook `scripts/run_with_adaptive_policy.py` already wires
    for the shadow-fidelity monitor (`m1_candle_observer` constructor
    param). Composes the two into ONE callable, each independently
    defensively wrapped — a raising `a` must never prevent `b` from
    firing that candle (and vice versa), the SAME "one hook's failure
    never affects another's" discipline every other observer in this
    codebase already follows (`order_book_observer`/`candle_observer`).
    `LifecycleRuntime._consume_m1()` ALSO wraps the whole composed call
    in its own broad `except Exception` — this inner wrapping exists so
    a failure in one half doesn't skip the OTHER half for that candle,
    not merely to protect the M1 loop overall (that outer protection
    would exist even for a single, uncomposed observer).

    Returns `None` only if both are `None` (never happens in practice
    here, since `a` is always `Application._record_m1_price`, but kept
    total/correct); returns the other one as-is if just one is `None`
    (zero indirection overhead in the common — no external observer —
    case)."""
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a

    def _combined(symbol: str, candle: LifecycleCandle) -> None:
        for observer in (a, b):
            try:
                observer(symbol, candle)
            except Exception:  # noqa: BLE001 - one composed observer's failure must never affect the other
                _LOGGER.error(
                    "m1 candle observer %r FAILED for %s (isolated from the other observer)",
                    getattr(observer, "__name__", observer), symbol, exc_info=True,
                )

    return _combined


class Application:
    """Faz 8 kompozisyon kökü + process lifecycle.

    Sıra (Bölüm 5): config -> logging (çağıran tarafından, `main()`'de) ->
    PUBLIC provider -> Faz 5 (`PaperTradingEngine`) -> Faz 6
    (`RuntimeCoordinator`) -> Faz 7 (`PaperStateStore`/`PersistedRuntime`)
    -> recover/bootstrap -> runtime başlat -> SIGINT/SIGTERM -> graceful
    idempotent shutdown -> anlamlı exit code."""

    def __init__(
        self,
        config: AppConfig,
        provider: LiveDataProvider,
        *,
        testnet_http_client: object | None = None,
        selection_result: SelectionResult | None = None,
        pinned_symbols: tuple[str, ...] = (),
        exit_policy_provider: Callable[[], tuple[ExitPolicyConfig, str | None]] | None = None,
        candle_observer: Callable[[str, Timeframe, Candle], None] | None = None,
        m1_candle_observer: Callable[[str, LifecycleCandle], None] | None = None,
        adaptive_status_provider: Callable[[], dict[str, object]] | None = None,
    ) -> None:
        self._config = config
        # UI Polish v1, Step A9 — event log (see ops/event_log.py).
        # Always a real path (unlike the optional hooks below, there is
        # no reason to disable this — it is a pure, local, defensively-
        # wrapped observability write with zero execution/trading risk).
        # A DEDICATED file, never sharing a schema/connection with
        # `paper_state.db` or the Testnet execution DB.
        self._event_log_path = config.db_path.parent / "ops_events.db"
        # Dashboard price/P&L sync fix — `_latest_price()` previously read
        # ONLY the latest CLOSED M5 candle, which can lag real price
        # movement by up to 5 minutes (the M1 lifecycle evaluator itself
        # already reacts every minute; the dashboard's mark-to-market
        # display didn't). In-memory only (never persisted): symbol ->
        # (close, close_time) for the freshest M1 candle this PROCESS has
        # actually observed via `_record_m1_price` (see below), fed by
        # the SAME real M1 stream `LifecycleRuntime` already consumes for
        # stop/target/trailing — NEVER a new REST call. An empty cache
        # after a restart is not a bug: `_latest_price()` simply falls
        # back to the M5 read it already did before this fix (see
        # `_latest_price_source`).
        self._m1_price_cache: dict[str, tuple[float, datetime]] = {}
        # UI Polish v1, Step A3 — Adaptive Intelligence status. `None`
        # unless `scripts/run_with_adaptive_policy.py` supplies one (the
        # ONLY entry point that imports BOTH `crypto_signal_engine/` AND
        # `adaptive/` — this module NEVER imports `adaptive/` itself,
        # same hard dependency-direction rule as `exit_policy_provider`/
        # `m1_candle_observer`). With `None` (every plain `python -m
        # crypto_signal_engine.app run` invocation), the dashboard shows
        # an honest "adaptive intelligence not active in this run" state
        # — never fabricated.
        self._adaptive_status_provider = adaptive_status_provider
        # 24/7 Ops v1, Step 4 — outbound-only alerting. `None` unless
        # both CSE_TELEGRAM_BOT_TOKEN/CSE_TELEGRAM_CHAT_ID are set (see
        # ops/notifier.py) — read directly from the process environment,
        # OUTSIDE AppConfig, same precedent as Binance Testnet signing
        # credentials (execution/factory.py).
        self._notifier: Notifier | None = build_telegram_notifier()
        # 24/7 Ops v1, Step 3 — Control Center. `entries_paused` is a
        # `threading.Event` (safe to read/set from any OS thread with no
        # extra locking) — always created, but can only ever be set by
        # `AdminController.pause()`, which is only constructed when
        # `config.dashboard_admin_token` is set (see `start()`). With no
        # admin token configured, this Event can never be set, so wiring
        # it unconditionally below is behaviorally a no-op (matches the
        # "None default = behaviorally identical to today" requirement).
        self._entries_paused = threading.Event()
        self._admin_controller: AdminController | None = None
        # Bölüm "OBSERVABILITY" — yalnızca gösterim amaçlı, saklanan
        # (immutable) seçim sonucu; `None` <=> bu run MANUAL modda
        # başladı (bkz. `_symbol_selection_snapshot`). `pinned_symbols`,
        # `config.symbols` İÇİNDEKİ hangi sembollerin fırsat ranking'i
        # DEĞİL, durable bir AÇIK pozisyon YÜZÜNDEN mevcut olduğunu
        # gösterim amaçlı ayırt eder (Bölüm A — pinning ZATEN
        # `resolve_symbols()`'da `config.symbols`'a UYGULANMIŞTIR; burada
        # yalnızca HANGİLERİNİN pinned olduğu HATIRLANIR).
        self._selection_result = selection_result
        self._pinned_symbols = pinned_symbols
        self._store = PaperStateStore(config.db_path)
        paper_engine = PaperTradingEngine(
            notional_per_position=config.notional_per_position,
            fee_bps=config.fee_bps,
            slippage_bps=config.slippage_bps,
        )
        coordinator = RuntimeCoordinator(
            symbols=config.symbols,
            provider=provider,
            candle_timeframes=_CANDLE_TIMEFRAMES,
            warmup_candles=config.warmup_candles,
            order_book_depth=config.order_book_depth,
            stale_feed_threshold_seconds=config.stale_feed_threshold_seconds,
            paper_engine=paper_engine,
            candle_observer=candle_observer,
        )
        persisted_runtime = PersistedRuntime(coordinator, self._store)
        self._lock = ProcessLock(config.lock_path)
        self._shutdown_event = asyncio.Event()
        self._shutdown_started = False
        self._fatal_error: BaseException | None = None
        self._started_at = utc_now()
        self._run_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None

        # Faz 12 — TESTNET execution reconciliation (STRICTLY opt-in, bkz.
        # `build_execution_service`). PAPER modda (varsayılan) VEYA
        # `CSE_ENABLE_TESTNET_EXECUTION` açıkça `true` OLMADIKÇA bu HER
        # ZAMAN `None`'dır — hiçbir execution store/client İNŞA EDİLMEZ,
        # dolayısıyla hiçbir private order-submission kod yoluna erişim
        # MÜMKÜN DEĞİLDİR (Bölüm 10 "paper-first safety").
        execution = build_execution_service(config, http_client=testnet_http_client)
        self._execution_service: ExecutionReconciliationService | None = None
        self._execution_store: ExecutionStateStore | None = None
        if execution is not None:
            self._execution_service, self._execution_store = execution
        self._execution_ready = False
        self._execution_detail = (
            "PAPER mode (no execution store)"
            if config.execution_mode is ExecutionMode.PAPER
            else "TESTNET execution not enabled (CSE_ENABLE_TESTNET_EXECUTION is not true)"
            if self._execution_service is None
            else "TESTNET execution enabled, startup reconciliation not yet run"
        )
        self._last_reconciliation_at: object | None = None

        # Faz 13 — Signal->TESTNET execution bridge (STRICTLY opt-in, bkz.
        # `build_signal_testnet_bridge`). `execution` (yukarı) `None`
        # OLMADIKÇA VE `CSE_ENABLE_SIGNAL_TESTNET_BRIDGE=true` OLMADIKÇA bu
        # HER ZAMAN `None`'dır — hiçbir otomatik Signal->order kod yoluna
        # erişim MÜMKÜN DEĞİLDİR (Bölüm "EXPLICIT SAFETY GATE"). `None`
        # iken `self._runtime`, `BridgeRuntime` yerine DOĞRUDAN
        # `PersistedRuntime`dir (Faz 7, DEĞİŞTİRİLMEDEN) — bridge devre
        # dışıyken PAPER davranışı bit-for-bit AYNI kalır.
        lifecycle = build_lifecycle_manager(
            config, execution, exit_policy_provider=exit_policy_provider, notifier=self._notifier,
            event_log_path=self._event_log_path,
        )
        self._lifecycle_store: LifecycleStore | None = None
        self._lifecycle_manager: LifecycleManager | None = None
        if lifecycle is not None:
            self._lifecycle_store, self._lifecycle_manager = lifecycle

        def _atr_provider(symbol: str) -> float | None:
            return latest_m5_atr(coordinator, symbol, now=utc_now())

        self._signal_bridge: SignalTestnetBridge | None = build_signal_testnet_bridge(
            config, execution, lifecycle_manager=self._lifecycle_manager,
            atr_provider=_atr_provider if self._lifecycle_manager is not None else None,
            # Phase 16 self-review fix: `config.symbols` is a static
            # snapshot from startup — Phase 16 hot reselection changes the
            # LIVE coordinator universe afterward without ever touching
            # `config.symbols`. The entry risk gate's max-positions/
            # max-exposure accounting must see the CURRENT universe, not a
            # stale one, or it would under-count exposure after any
            # add/remove.
            all_symbols_provider=(lambda: coordinator._symbols) if self._lifecycle_manager is not None else None,  # noqa: SLF001
            # 24/7 Ops v1, Step 3 — Control Center pause gate (see
            # `ops/admin.py`/`execution/signal_bridge.py`). Always wired
            # to the SAME `threading.Event` `AdminController.pause()`
            # sets — behaviorally a no-op unless `dashboard_admin_token`
            # is configured (the Event can then never be set).
            entries_paused_provider=self._entries_paused.is_set,
        )
        bridge_runtime: PersistedRuntime | BridgeRuntime = (
            BridgeRuntime(persisted_runtime, self._signal_bridge)
            if self._signal_bridge is not None
            else persisted_runtime
        )
        # Autonomous Testnet trading lifecycle (Phase 6/8/9/12) — the M1
        # stop/target/trailing evaluator. `LifecycleRuntime` is a pure
        # pass-through to `bridge_runtime` when `self._lifecycle_manager is
        # None` (mirrors `BridgeRuntime`'s own None-passthrough discipline
        # — PAPER/bridge-disabled behavior is bit-for-bit unaffected).
        self._runtime: PersistedRuntime | BridgeRuntime | LifecycleRuntime = (
            LifecycleRuntime(
                bridge_runtime, self._lifecycle_manager,
                # Dashboard price/P&L sync fix — `self._record_m1_price`
                # composed with whatever external observer was passed in
                # (e.g. `scripts/run_with_adaptive_policy.py`'s shadow
                # monitor), never replacing it. See
                # `_compose_m1_candle_observers()`.
                m1_candle_observer=_compose_m1_candle_observers(self._record_m1_price, m1_candle_observer),
            )
            if isinstance(bridge_runtime, BridgeRuntime)
            else bridge_runtime
        )

        # Autonomous Testnet trading lifecycle Phase 16 — periodic
        # automatic reselection. Only meaningful in AUTOMATIC symbol-
        # selection mode (`CSE_SYMBOLS` unset) — a MANUAL run has no
        # opportunity ranking to periodically re-run. Uses the SAME
        # `rescan_interval_seconds` default as the one-shot startup
        # selection (`config.symbol_selection`), never artificially
        # shortened.
        #
        # PRODUCTION HOT-RESELECTION FIX: `coordinator=self._runtime` (the
        # ACTUAL, live, composed runtime — `LifecycleRuntime`/
        # `BridgeRuntime`/`PersistedRuntime`, whichever this configuration
        # resolved to just above) — NOT the bare `coordinator` variable.
        # `RuntimeCoordinator.add_symbol()`/`remove_symbol()` alone only
        # spawn/cancel LIVE tasks when `coordinator._running` is `True`,
        # which production NEVER sets (production drives `self._runtime.
        # run()`, never `coordinator.run()` directly — see `coordinator.py`/
        # `bridge_runtime.py`/`lifecycle_runtime.py` module docstrings).
        # Wiring the raw coordinator here made periodic reselection able to
        # add a symbol to `coordinator._symbols`/the dashboard while NEVER
        # starting live candle/order-book consumption OR M1 stop/target/
        # trailing evaluation for it — a hot-added symbol could open a
        # position with zero automated protection. `self._runtime` now
        # owns hot-add/hot-remove symmetrically across BOTH consumption
        # layers (see `LifecycleRuntime.add_symbol`/`remove_symbol`).
        self._reselection_scheduler: ReselectionScheduler | None = (
            ReselectionScheduler(
                coordinator=self._runtime,
                selector=AutomaticSymbolSelector(build_discovery_rest_client(), SystemClock(), config.symbol_selection),
                config=config.symbol_selection,
                safety_pinned_provider=self._safety_pinned_symbols,
            )
            if config.auto_select_symbols
            else None
        )

        # Faz 12 — read-only operasyon dashboard'u (Bölüm 4/5/6). Varsayılan
        # bind `127.0.0.1`dir (`AppConfig.dashboard_host`); dashboard
        # BAŞARISIZ olursa (bkz. `start()`) ana runtime ASLA etkilenmez.
        self._dashboard: DashboardServer | None = (
            DashboardServer(
                host=config.dashboard_host, port=config.dashboard_port,
                health_snapshot_path=config.health_snapshot_path,
            )
            if config.dashboard_enabled
            else None
        )

    @property
    def runtime(self) -> PersistedRuntime | BridgeRuntime | LifecycleRuntime:
        return self._runtime

    @property
    def execution_ready(self) -> bool:
        """Faz 12: `True` YALNIZCA startup reconciliation BAŞARIYLA
        tamamlandıysa. PAPER modda VEYA TESTNET etkin değilken HER ZAMAN
        `False`'tur (execution zaten mevcut DEĞİLDİR, "hazır" DEĞİL,
        "uygulanabilir değil" anlamına gelir — dashboard/doctor bu ikisini
        `execution_detail` ile ayırt eder)."""
        return self._execution_ready

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> int:
        try:
            self._lock.acquire()
        except ConcurrentInstanceError as exc:
            _LOGGER.error("startup aborted: %s", exc)
            return EXIT_LOCK_HELD

        _LOGGER.info("starting crypto-signal-engine pid=%d config=%s", os.getpid(), self._config.summary())

        # 24/7 Ops v1, Step 3 — Control Center admin backend. Constructed
        # HERE (not in `__init__`) because `AdminController.stop()` needs
        # a reference to the ACTUAL running asyncio loop for its
        # `call_soon_threadsafe()` path — `__init__` may run before any
        # loop exists (e.g. `main()` constructs `Application` before
        # `asyncio.run(app.start())`). `dashboard_admin_token=None` (the
        # default) means `self._admin_controller` stays `None` for the
        # process's entire life — dashboard behaves EXACTLY as it did
        # before this milestone (see DECISIONS.md Karar).
        if self._config.dashboard_admin_token is not None:
            self._admin_controller = AdminController(
                token=self._config.dashboard_admin_token, loop=asyncio.get_running_loop(),
                request_shutdown=self.request_shutdown, entries_paused=self._entries_paused,
                notifier=self._notifier, event_log_path=self._event_log_path,
            )
            if self._dashboard is not None:
                self._dashboard.set_admin_controller(self._admin_controller)

        # Phase 19 canonical startup order (autonomous Testnet trading
        # lifecycle corrective fix): PERSISTENCE OPEN (already done in
        # __init__, both PAPER and execution/lifecycle stores) ->
        # EXECUTION RECONCILIATION -> BRIDGE/LIFECYCLE RECOVERY (the
        # `LifecycleStore` opened in __init__ already holds any
        # previously-persisted state; no separate action needed) -> LEGACY
        # OWNERSHIP RECONSTRUCTION (stage 1, market-data-free) -> MARKET
        # BOOTSTRAP -> FIRST-FRESH MARKET-DEPENDENT INITIALIZATION (stage
        # 2) -> execution activation boundary -> new live economic
        # actions. Reconciliation now runs BEFORE `self._runtime.recover()`
        # (previously ran after it) specifically so the bridge's
        # operational gate — which BOTH migration stages require before
        # touching anything — is known before any bootstrap network
        # activity, not just before the live signal loop.
        await self._reconcile_execution_startup()
        await self._reconstruct_legacy_ownership_startup()

        try:
            reports = await self._runtime.recover()
        except Exception as exc:
            # Bölüm 12 — "recovery/startup başarısız olursa runtime KESİNLİKLE
            # başlatılmamalı, durable state DOKUNULMADAN kalmalı". `recover()`
            # yalnızca OKUR (restore_paper_engine) ve in-memory bootstrap
            # uygular; bir hata durumunda hiçbir durable checkpoint YAZILMAZ,
            # bu yüzden store zaten olduğu gibi kalır — burada yalnızca temiz
            # bir şekilde kapatılır. Stage 1 above may already have written
            # `RECOVERED` records — that is safe and durable regardless
            # (Phase 19: a later successful restart's stage 1 is idempotent
            # and skips them; stage 2 completes them once bootstrap
            # eventually succeeds).
            _LOGGER.error("recovery FAILED — runtime will NOT start: %s", exc, exc_info=True)
            await self._write_snapshot(recovery_ok=False, recovery_detail=str(exc))
            self._store.close()
            self._lock.release()
            return EXIT_STARTUP_FAILURE

        _LOGGER.info(
            "recovery OK: %d bootstrap report(s) applied (%s)",
            len(reports),
            ", ".join(f"{r.symbol}/{r.timeframe.value}={r.candles_applied}" for r in reports),
        )

        await self._finalize_legacy_lifecycle_init_startup()
        await self._write_snapshot(recovery_ok=True, recovery_detail="recovery completed")

        if self._dashboard is not None:
            try:
                self._dashboard.start()
            except OSError as exc:
                # Bölüm 6 — dashboard BAŞLATILAMASA bile (örn. port zaten
                # kullanımda) ana runtime ASLA bu yüzden durdurulmaz.
                _LOGGER.error("dashboard could NOT start (runtime continues without it): %s", exc)
                self._dashboard = None

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_shutdown, sig.name)

        self._run_task = asyncio.create_task(self._run_guarded())
        self._health_task = asyncio.create_task(self._health_loop())
        if self._reselection_scheduler is not None:
            self._reselection_scheduler.start()

        # 24/7 Ops v1, Step 4 — outbound-only alerting: process start.
        # `self._notifier` is `None` unless CSE_TELEGRAM_BOT_TOKEN/
        # CSE_TELEGRAM_CHAT_ID are both set — a silent no-op otherwise.
        safe_notify(
            self._notifier,
            f"[crypto-signal-engine] started (pid={os.getpid()}, mode={self._config.execution_mode.value}, "
            f"symbols={list(self._config.symbols)})",
        )

        await self._shutdown_event.wait()
        await self._graceful_stop()
        return EXIT_FATAL if self._fatal_error is not None else EXIT_OK

    async def _reconcile_execution_startup(self) -> None:
        """Bölüm 11: `ExecutionReconciliationService` mevcutsa (TESTNET +
        açık enable gate), `reconcile_pending()`'i ÇALIŞTIRIR — hiçbir
        koşulda YENİ bir order gönderim kod yoluna erişmez (bkz.
        `reconciliation_service.py::reconcile_pending()`, Faz 11'de zaten
        kanıtlanmış: yalnızca sorgular, ASLA yeniden gönderir). Başarısız
        olursa fail-closed: `_execution_ready=False` KALIR, detay
        loglanır/snapshot'a yansır — ama exception yukarı SIZDIRILMAZ
        (PUBLIC/PAPER runtime'ı bu yüzden ASLA durdurulmamalı).

        BLOCKER FİX (Faz 12 — bağımsız acceptance review bulgusu):
        `reconcile_pending()`'İ ÇAĞIRMADAN ÖNCE, AÇIKÇA bir kimlik bilgisi
        VARLIK kontrolü (`testnet_credentials_status()`) YAPILIR. Bekleyen
        execution kaydı YOKSA `reconcile_pending()` hiçbir signed çağrı
        YAPMAZ (boş liste üzerinde döner) — bu durumda kimlik bilgisi
        eksikliği HİÇBİR ZAMAN keşfedilmez ve `execution_ready` YANLIŞLIKLA
        `True` olurdu. Bu kontrol, "bekleyen bir signed API çağrısına
        GÜVENME" ve "operatörün `doctor` çalıştırmış OLMASINA güvenme"
        gereksinimlerini karşılar — HİÇBİR ağ çağrısı YAPILMADAN, HER
        `start()` çağrısında OTOMATİK ve KOŞULSUZ çalışır."""
        if self._execution_service is None:
            if self._signal_bridge is not None:
                self._signal_bridge.mark_operational(False, "TESTNET execution not enabled")
            return

        has_key, has_secret = testnet_credentials_status()
        if not (has_key and has_secret):
            self._execution_ready = False
            self._execution_detail = (
                "TESTNET credentials missing — execution stays disabled (never attempted a network call): "
                f"BINANCE_TESTNET_API_KEY={'SET' if has_key else 'UNSET'} "
                f"BINANCE_TESTNET_API_SECRET={'SET' if has_secret else 'UNSET'}"
            )
            _LOGGER.error("testnet execution disabled — credentials missing: %s", self._execution_detail)
            if self._signal_bridge is not None:
                self._signal_bridge.mark_operational(False, self._execution_detail)
            return

        try:
            reconciled = await self._execution_service.reconcile_pending()
        except Exception as exc:  # noqa: BLE001 - fail-closed: execution disabled, runtime devam eder
            self._execution_ready = False
            self._execution_detail = f"startup reconciliation FAILED — execution stays disabled: {exc}"
            _LOGGER.error(
                "testnet execution startup reconciliation FAILED — execution stays disabled: %s", exc, exc_info=True
            )
            if self._signal_bridge is not None:
                self._signal_bridge.mark_operational(False, self._execution_detail)
            return
        self._execution_ready = True
        self._last_reconciliation_at = utc_now()
        self._execution_detail = f"startup reconciliation OK: {len(reconciled)} record(s) swept"
        _LOGGER.info("testnet execution startup reconciliation OK: %d record(s) swept", len(reconciled))
        if self._signal_bridge is not None:
            # Bölüm "EXPLICIT SAFETY GATE": bridge YALNIZCA bu satıra
            # ULAŞILDIĞINDA (mode=TESTNET, execution gate açık, credential
            # mevcut, startup reconciliation BAŞARILI) operational hâle
            # gelir — bu METODA GİRİŞ ÖNCESİ (yukarıdaki HERHANGİ bir erken
            # dönüş) hâlâ `_operational=False` (constructor varsayılanı)
            # kalır (fail-closed).
            self._signal_bridge.mark_operational(True, f"startup reconciliation OK: {len(reconciled)} record(s) swept")

    async def _reconstruct_legacy_ownership_startup(self) -> None:
        """Autonomous Testnet trading lifecycle Phase 4/19 — STAGE 1
        (pre-bootstrap): authoritative ownership reconstruction ONLY for
        any real bridge-owned position that predates this lifecycle
        manager. Runs ONCE, only when the bridge is operational (startup
        reconciliation ALREADY succeeded — this is now called BEFORE
        `self._runtime.recover()`/market bootstrap), creates ZERO exchange
        orders, reads NO market data, and never blocks/fails startup — a
        failure is logged and the symbol stays fail-closed/unreconstructed
        (`signal_bridge.py`'s own guard then keeps it safety-pinned)."""
        if self._lifecycle_manager is None or self._lifecycle_store is None or self._signal_bridge is None:
            return
        if not self._signal_bridge.operational:
            return
        try:
            reconstructed = await reconstruct_all_legacy_ownership(
                self._config.symbols, execution_store=self._execution_store, lifecycle_store=self._lifecycle_store,
                client=self._execution_service.client, clock=SystemClock(),
            )
        except Exception:  # noqa: BLE001 - a reconstruction failure must NEVER block startup
            _LOGGER.error("lifecycle-migration stage 1: unexpected top-level failure (startup continues)", exc_info=True)
            return
        if reconstructed:
            _LOGGER.info(
                "lifecycle-migration stage 1: reconstructed ownership for %d legacy bridge position(s): %s "
                "(state=RECOVERED, awaiting post-bootstrap initialization)", len(reconstructed), reconstructed,
            )

    async def _finalize_legacy_lifecycle_init_startup(self) -> None:
        """Autonomous Testnet trading lifecycle Phase 4/19 — STAGE 2
        (post-bootstrap): initializes market-dependent lifecycle fields
        (`high_water`/`initial_protective_stop`/`effective_stop`/
        `take_profit`) for any position STAGE 1 left in `RECOVERED` state,
        using the first genuinely fresh post-bootstrap M5 price/ATR
        (`self.runtime._coordinator` is now populated — bootstrap already
        ran). A no-op for anything not exactly `RECOVERED` (never
        re-initializes an already-`LONG` position on a normal restart).
        Never blocks/fails startup."""
        if self._lifecycle_manager is None or self._lifecycle_store is None or self._signal_bridge is None:
            return
        if not self._signal_bridge.operational:
            return
        try:
            finalized = await finalize_all_legacy_lifecycle_init(
                self._config.symbols, lifecycle_store=self._lifecycle_store,
                coordinator=self.runtime._coordinator,  # noqa: SLF001 - same private-accessor precedent as bridge_runtime.py
                clock=SystemClock(), exit_policy=self._config.lifecycle_exit_policy,
            )
        except Exception:  # noqa: BLE001 - a finalization failure must NEVER block startup
            _LOGGER.error("lifecycle-migration stage 2: unexpected top-level failure (startup continues)", exc_info=True)
            return
        if finalized:
            _LOGGER.info(
                "lifecycle-migration stage 2: finalized %d legacy bridge position(s): %s (state=LONG, now monitored)",
                len(finalized), finalized,
            )

    async def _run_guarded(self) -> None:
        """`PersistedRuntime.run()`'ı sarar: HİÇBİR fatal exception
        sessizce yutulmaz — loglanır, `_fatal_error` işaretlenir (exit
        code'a yansır) ve shutdown TETİKLENİR (systemd'nin bounded
        `RestartSec` politikası devreye girsin diye — burada sonsuz bir
        iç retry-loop YOKTUR)."""
        try:
            await self._runtime.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOGGER.error("fatal uncaught exception in runtime loop", exc_info=True)
            self._fatal_error = exc
        finally:
            self.request_shutdown("runtime-loop-exit")

    async def _health_loop(self) -> None:
        try:
            while not self._shutdown_event.is_set():
                await self._write_snapshot(recovery_ok=True, recovery_detail="running")
                # 24/7 Ops v1 — systemd watchdog heartbeat. Reuses THIS
                # already-running loop (no second timer/task, see
                # `ops/systemd_notify.py` module docstring) — a total
                # no-op whenever `$NOTIFY_SOCKET` is unset (local dev,
                # PAPER mode without systemd, every test).
                notify_watchdog()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=self._config.health_snapshot_interval_seconds
                    )
        except asyncio.CancelledError:
            raise

    def request_shutdown(self, reason: str = "") -> None:
        """Idempotent: SIGINT+SIGTERM art arda, veya bir sinyal + runtime
        loop'un kendi kendine sonlanması, TEK bir shutdown dizisi üretir.
        Shutdown başladıktan sonra hiçbir YENİ paper-trading transition'ı
        kabul edilmez (bkz. `_graceful_stop` -> `runtime.stop()` -> her
        consumer task'ı iptal edilir, Faz 6'nın KENDİ `_stopped` bayrağı)."""
        if self._shutdown_started:
            return
        self._shutdown_started = True
        _LOGGER.info("shutdown requested (%s)", reason or "unknown")
        self._shutdown_event.set()

    async def _graceful_stop(self) -> None:
        _LOGGER.info("beginning graceful shutdown")
        if self._dashboard is not None:
            self._dashboard.stop()
        if self._reselection_scheduler is not None:
            await self._reselection_scheduler.stop()
        if self._health_task is not None:
            self._health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_task
        await self._runtime.stop()  # Faz 6 stop() (idempotent) + store.close() (Faz 7 PersistedRuntime.stop())
        if self._run_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._run_task
        await self._write_snapshot(recovery_ok=True, recovery_detail="stopped")
        if self._execution_store is not None:
            self._execution_store.close()
        if self._lifecycle_store is not None:
            self._lifecycle_store.close()
        self._lock.release()
        _LOGGER.info("shutdown complete")
        # 24/7 Ops v1, Step 4 — outbound-only alerting: graceful-stop.
        # Called LAST, after everything else already succeeded — a
        # raising/hanging notifier can never affect whether shutdown
        # itself completed.
        safe_notify(self._notifier, f"[crypto-signal-engine] stopped gracefully (pid={os.getpid()})")

    def _pending_reconciliation_count(self) -> int | None:
        """`None` — execution store YOK (PAPER modu VEYA TESTNET devre
        dışı). Bir SQLite okuma hatası bile snapshot yazımını ASLA
        engellememeli (fail-safe: `-1` "okunamadı" anlamına gelir)."""
        if self._execution_store is None:
            return None
        try:
            return len(self._execution_store.list_needing_reconciliation())
        except Exception:  # noqa: BLE001 - snapshot yazımı ASLA bu yüzden başarısız OLMAMALI
            _LOGGER.warning("pending reconciliation count okunamadı", exc_info=True)
            return -1

    def _execution_snapshot(self) -> dict[str, object]:
        return {
            "mode": self._config.execution_mode.value,
            "enabled": self._config.enable_testnet_execution,
            "ready": self._execution_ready,
            "pending_reconciliation_count": self._pending_reconciliation_count(),
            "last_reconciliation_at": (
                self._last_reconciliation_at.isoformat() if self._last_reconciliation_at is not None else None
            ),
            "detail": self._execution_detail,
        }

    def _signal_bridge_snapshot(self) -> dict[str, object]:
        """Bölüm "OBSERVABILITY": bridge etkin/devre dışı, politika,
        sembol başına bridge-owned miktar, son bridge sinyali/aksiyonu ve
        çözülmemiş belirsizlik — HİÇBİR credential/secret İÇERMEZ (bkz.
        `SignalTestnetBridge.status_for()`, yalnızca kimlik/miktar/durum
        alanları taşır)."""
        if self._signal_bridge is None:
            return {
                "enabled": False,
                "policy": SignalTestnetBridge.POLICY,
                "operational": False,
                "detail": (
                    "signal->testnet bridge not enabled (CSE_ENABLE_SIGNAL_TESTNET_BRIDGE is not true, "
                    "or TESTNET execution is not enabled)"
                ),
                "symbols": {},
            }
        return {
            "enabled": True,
            "policy": self._signal_bridge.POLICY,
            "operational": self._signal_bridge.operational,
            "detail": self._signal_bridge.operational_detail,
            "symbols": {symbol: self._signal_bridge.status_for(symbol) for symbol in self._config.symbols},
        }

    def _paper_snapshot(self) -> dict[str, object]:
        """Bölüm 4 — dashboard için sembol başına pozisyon/PnL/son sinyal.
        `RuntimeCoordinator.paper_engine`/`last_cycle_result()` (Faz 12'de
        eklenen salt-okunur accessor'lar, bkz. `runtime/coordinator.py`)
        DIŞINDA hiçbir canlı nesneye erişilmez; bu metod her zaman
        health-loop'un KENDİ asyncio task'ı İÇİNDEN (event-loop thread'i)
        çağrılır — dashboard'un AYRI OS thread'i bu veriye YALNIZCA disk
        üzerindeki JSON anlık görüntüsü ÜZERİNDEN erişir (bkz. `ops/
        dashboard.py` modül docstring'i — cross-thread erişim YOKTUR)."""
        coordinator = self.runtime._coordinator
        paper_engine = coordinator.paper_engine
        result: dict[str, object] = {}
        for symbol in self._config.symbols:
            position = paper_engine.position(symbol)
            if position is None:
                position_payload = {"side": "FLAT", "quantity": 0.0, "average_entry_price": 0.0, "realized_pnl": 0.0}
            else:
                position_payload = {
                    "side": position.side.value,
                    "quantity": position.quantity,
                    "average_entry_price": position.average_entry_price,
                    "realized_pnl": position.realized_pnl,
                }
            cycle_result = coordinator.last_cycle_result(symbol)
            if cycle_result is None or not cycle_result.evaluated:
                last_signal_payload = {"direction": None, "timestamp": None, "context_id": None}
            else:
                last_signal_payload = {
                    "direction": cycle_result.signal.direction.value,
                    "timestamp": cycle_result.signal.timestamp.isoformat(),
                    "context_id": cycle_result.signal.context_id,
                }
            result[symbol] = {"position": position_payload, "last_signal": last_signal_payload}
        return result

    def _symbol_selection_snapshot(self) -> dict[str, object]:
        """Bölüm "OBSERVABILITY": mod (MANUAL/AUTOMATIC), son seçim zamanı,
        fırsat-seçilen semboller, AÇIK-pozisyon YÜZÜNDEN pinlenmiş
        semboller, VE nihai (ikisinin birleşimi) runtime sembol kümesi —
        ayrı ayrı raporlanır (Bölüm A: pinning `target_count`'u aşabilir,
        bu YAPISAL OLARAK BEKLENEN bir durumdur, gizlenmez). AUTOMATIC ise
        ayrıca TAM ranked-candidate listesi + her biri için AÇIK sebep.

        `opportunity_symbols`/`candidates`/`generated_at` reflect ONLY the
        one-shot STARTUP selection (unchanged after that, by design —
        those are this run's original discovery result). `final_symbols`
        below, however, reflects the LIVE current coordinator universe —
        Phase 16's periodic `ReselectionScheduler` can add/remove symbols
        after startup, so `config.symbols` alone would go stale."""
        final_symbols = list(self.runtime._coordinator._symbols)  # noqa: SLF001 - same private-accessor precedent as bridge_runtime.py
        reselection = self._reselection_snapshot()
        if self._selection_result is None:
            return {
                "mode": "MANUAL",
                "opportunity_symbols": list(self._config.symbols),
                "pinned_open_position_symbols": [],
                "final_runtime_symbols": final_symbols,
                "reselection_scheduler": reselection,
            }
        result = self._selection_result
        return {
            "mode": "AUTOMATIC",
            "opportunity_symbols": list(result.selected_symbols),
            "pinned_open_position_symbols": list(self._pinned_symbols),
            "final_runtime_symbols": final_symbols,
            "generated_at": result.generated_at.isoformat(),
            "universe_size": result.universe_size,
            "shortlist_size": result.shortlist_size,
            "candidates": [
                {
                    "symbol": c.symbol,
                    "total_score": c.total_score,
                    "liquidity_score": c.liquidity_score,
                    "historical_movement_score": c.historical_movement_score,
                    "current_opportunity_score": c.current_opportunity_score,
                    # UI Polish v1, Step A4 — Adaptive Symbol Intelligence
                    # (Karar 96). Already cheaply in-memory on `result`
                    # (the SAME `SelectionResult` every other field above
                    # already reads) — no new persistence/plumbing added.
                    # Honest caveat (see DECISIONS.md): production never
                    # wires a `learned_factor_provider` into a live
                    # `resolve_symbols()` run yet (Karar 96's own
                    # documented scope boundary), so this is currently
                    # ALWAYS the neutral `0.5` default for every eligible
                    # candidate in practice — a real, non-fabricated
                    # value, just not yet an informative one in this run.
                    "learned_factor_score": c.learned_factor_score,
                    "reason": c.reason,
                }
                for c in result.ranked_candidates
            ],
            "reselection_scheduler": reselection,
        }

    def _safety_pinned_symbols(self) -> frozenset[str]:
        """Phase 16's "safety-owned symbols" — never removed by
        `ReselectionScheduler`: any symbol with a non-FLAT bridge lifecycle
        position (LONG/DUST/ENTRY_PENDING/EXIT_PENDING/AMBIGUOUS) or an
        open PAPER position. Computed FRESH on every call (never cached) —
        the scheduler calls this at the start of every rescan."""
        coordinator = self.runtime._coordinator  # noqa: SLF001
        pinned: set[str] = set()
        for symbol in coordinator._symbols:  # noqa: SLF001
            if self._lifecycle_manager is not None:
                position = self._lifecycle_manager.position(symbol)
                if position.state.value != "FLAT":
                    pinned.add(symbol)
            if coordinator.paper_engine.position(symbol) is not None:
                pinned.add(symbol)
        return frozenset(pinned)

    def _reselection_snapshot(self) -> dict[str, object]:
        if self._reselection_scheduler is None:
            return {"enabled": False}
        return {"enabled": True, **self._reselection_scheduler.status().as_dict()}

    def _record_m1_price(self, symbol: str, candle: LifecycleCandle) -> None:
        """Dashboard price/P&L sync fix — the M1 observer half composed
        by `_compose_m1_candle_observers()` into `LifecycleRuntime`'s
        `m1_candle_observer` hook. Fires once per REAL, already-closed M1
        candle (the SAME frequency the M1 stop/target/trailing evaluator
        itself runs at) — purely records `(close, close_time)` into
        `self._m1_price_cache`, never a new REST call, never influences
        any trading decision. `candle.close_time` is the candle's OWN
        authoritative timestamp (replay-safe), never wall-clock."""
        self._m1_price_cache[symbol] = (candle.close, candle.close_time)

    def _latest_closed_m5_candle(self, symbol: str) -> tuple[float, datetime] | None:
        """The pre-existing M5 price source, unchanged: the latest CLOSED
        M5 candle already held in the coordinator's own candle window
        (same accessor precedent as `lifecycle_migration.py`'s `_first_
        fresh_post_recovery_price` / `lifecycle_runtime.py`'s `latest_
        m5_atr`). NEVER a new REST call. Returns `None` (never a
        fabricated price) whenever no such candle exists yet."""
        try:
            window = self.runtime._coordinator._candle_windows.get((symbol, Timeframe.M5))  # noqa: SLF001
        except Exception:  # noqa: BLE001 - dashboard snapshot must never fail because of a display-only lookup
            return None
        if window is None:
            return None
        history = window.history()
        if not history:
            return None
        last = history[-1]
        return last.close, last.close_time

    def _latest_price_source(self, symbol: str) -> tuple[float, str, datetime] | None:
        """Dashboard price/P&L sync fix — prefers the freshest REAL M1
        candle this process has observed (`self._m1_price_cache`, fed by
        `_record_m1_price`) over the latest closed M5 candle, WHENEVER the
        M1 one is actually newer (`close_time` comparison, never wall-
        clock) — otherwise falls back to the pre-existing M5 read
        byte-for-byte (an empty/stale M1 cache, e.g. right after a
        restart or when the Testnet bridge is disabled and no M1 stream
        is ever consumed, is not a bug). Still ZERO new REST calls either
        way — both sources are already-flowing candle data. Returns
        `(price, source, as_of)` with `source` `"M1"`/`"M5"` so the
        dashboard can show an honest freshness label instead of a
        fabricated "live" price; `None` when neither source has anything
        yet."""
        m1_entry = self._m1_price_cache.get(symbol)
        m5_entry = self._latest_closed_m5_candle(symbol)
        if m1_entry is not None and (m5_entry is None or m1_entry[1] > m5_entry[1]):
            return m1_entry[0], "M1", m1_entry[1]
        if m5_entry is not None:
            return m5_entry[0], "M5", m5_entry[1]
        return None

    def _latest_price(self, symbol: str) -> float | None:
        """Dashboard localization — read-only live price lookup, now
        preferring the freshest M1 candle over the M5 one (see
        `_latest_price_source()`); this function's own signature/
        semantics (a plain `float | None`) are UNCHANGED, so its other
        caller (`build_accounting_snapshot(price_lookup=self.
        _latest_price, ...)`) needs no changes."""
        result = self._latest_price_source(symbol)
        return result[0] if result is not None else None

    async def _bridge_lifecycle_snapshot(self) -> dict[str, object]:
        """Autonomous Testnet trading lifecycle (Phase 15/21) — TESTNET
        BRIDGE PORTFOLIO + PERFORMANCE + recent trade history, read
        entirely from the durable `LifecycleStore` (never from PAPER,
        wallet balance, or manual `lab-*` records). `None`/empty whenever
        the lifecycle manager isn't wired (bridge disabled) — no secret/
        credential values anywhere.

        Dashboard localization (Turkish operator UI): additionally exposes
        a read-only mark-to-market `latest_price`/`unrealized_gross_pnl`
        per non-flat position (via `_latest_price()` + the pure
        `unrealized_gross_pnl()` formula — no new accounting, no new
        market-data pipeline).

        Portfolio/Accounting v1: the PORTFOLIO-LEVEL numbers in
        `"performance"` (`exposure_usdt`, `non_flat_position_count`,
        `unrealized_gross_pnl_total`, `realized_gross_pnl`, `todays_
        realized_gross_pnl`, `completed_trade_count`) now come from
        `portfolio.accounting.build_accounting_snapshot()` — the ONE
        authoritative accounting layer — instead of being re-derived
        inline here. This dict's KEYS are UNCHANGED (no dashboard
        frontend break — see `ops/dashboard.py`), but several VALUES are
        now genuinely different from before: `realized_gross_pnl`/
        `todays_realized_gross_pnl` are NET of fees (not gross) and TRUE
        lifetime/daily totals via SQL aggregate (not a Python sum over a
        `LIMIT 50` fetch) — see `AccountingSnapshot`'s own docstring and
        this milestone's DECISIONS.md entry for why these key names still
        say "gross" while the values are actually net (a deliberate,
        documented, temporary naming mismatch — renaming the dict key is
        an `ops/dashboard.py` UI change, out of scope for this backend
        correctness milestone). `win_count`/`loss_count`/`win_rate`/
        `average_win_gross`/`average_loss_gross` are UNCHANGED — still
        computed from the same bounded `recent_trades` (limit 50, gross)
        this method already fetches for the "recent trades" table
        display; `AccountingSnapshot` has no per-trade win/loss fields,
        so fixing THEIR bounded/gross blind spot is out of this
        milestone's defined scope.

        Portfolio/Accounting v1, gap fix: previously-dead `check_usdt_
        balance()` (step 5) is now ACTUALLY called here, once per
        snapshot — this method is now `async` (all three call sites,
        `start()`/`_health_loop()`/`_graceful_stop()`, already run inside
        an event loop, see `_write_snapshot`). `"usdt_balance"` is
        `None` whenever the bridge isn't wired, the exchange call fails,
        or no USDT row exists — same defensive-wrap/never-crash/never-
        block-trading discipline `check_usdt_balance()` itself already
        follows; this call site adds NOTHING beyond consuming its
        already-safe `AssetBalance | None` result."""
        if self._lifecycle_manager is None or self._lifecycle_store is None:
            return {"enabled": False, "positions": {}}

        positions: dict[str, object] = {}
        all_records = []
        open_long = 0
        dust_count = 0
        for symbol in self._config.symbols:
            record = self._lifecycle_manager.position(symbol)
            all_records.append(record)
            has_quantity = record.state.value != "FLAT" and record.gross_entry_vwap is not None and record.net_owned_base_quantity > 0
            latest_price: float | None = None
            position_unrealized: float | None = None
            # Dashboard price/P&L sync fix — `source`/`as_of` are ADDITIVE
            # (never change `latest_price`/`unrealized_gross_pnl`'s own
            # values or None-ness, see `_latest_price_source()`) so the
            # dashboard can show an honest "M1, 12sn önce" / "M5, 3dk
            # önce" freshness label instead of leaving the user to guess
            # why a position's P&L looks stale relative to the market.
            latest_price_source: str | None = None
            latest_price_as_of: str | None = None
            if has_quantity:
                price_source = self._latest_price_source(symbol)
                if price_source is not None:
                    latest_price, latest_price_source, as_of = price_source
                    latest_price_as_of = as_of.isoformat()
                    position_unrealized = unrealized_gross_pnl(
                        gross_entry_vwap=record.gross_entry_vwap, net_owned_base_quantity=record.net_owned_base_quantity,
                        current_price=latest_price,
                    )
            positions[symbol] = {
                "state": record.state.value,
                "net_owned_base_quantity": record.net_owned_base_quantity,
                "gross_entry_vwap": record.gross_entry_vwap,
                "effective_stop": record.effective_stop,
                "take_profit": record.take_profit,
                "trailing_active": record.trailing_active,
                "high_water": record.high_water,
                "entry_timestamp": record.entry_timestamp.isoformat() if record.entry_timestamp is not None else None,
                "last_exit_reason": record.last_exit_reason,
                "cumulative_realized_gross_pnl": record.cumulative_realized_gross_pnl,
                "migrated_existing_position": record.migrated_existing_position,
                "cooldown_until": record.cooldown_until.isoformat() if record.cooldown_until is not None else None,
                "latest_price": latest_price,
                "latest_price_source": latest_price_source,
                "latest_price_as_of": latest_price_as_of,
                "unrealized_gross_pnl": position_unrealized,
            }
            if record.state.value == "LONG":
                open_long += 1
            if record.state.value == "DUST":
                dust_count += 1

        recent_trades = list(self._lifecycle_store.recent_completed_trades(limit=50))
        wins = [t for t in recent_trades if t["gross_realized_pnl"] > 0]
        losses = [t for t in recent_trades if t["gross_realized_pnl"] < 0]
        win_rate = (len(wins) / len(recent_trades)) if recent_trades else None

        now = utc_now()
        lifetime_pnl_net, lifetime_count = self._lifecycle_store.total_realized_pnl()
        today_pnl_net, today_count = self._lifecycle_store.realized_pnl_for_day(trading_day_key(now))
        accounting = build_accounting_snapshot(
            positions=all_records, price_lookup=self._latest_price,
            lifetime_realized_pnl_net_usdt=lifetime_pnl_net, lifetime_trade_count=lifetime_count,
            today_realized_pnl_net_usdt=today_pnl_net, today_trade_count=today_count,
            now=now,
        )

        # Portfolio/Accounting v1, gap fix — `self._execution_service` is
        # ALWAYS non-`None` here: `build_lifecycle_manager()` only ever
        # returns a manager when `execution` (which is exactly `(self.
        # _execution_service, self._execution_store)`) was itself
        # non-`None` at construction time (see `Application.__init__`) —
        # the `is not None` check below is defensive belt-and-suspenders,
        # not a sign this invariant is expected to fail. The `try/except`
        # is ADDITIONAL defense-in-depth at THIS call site, on top of
        # `check_usdt_balance()`'s own internal never-raise contract —
        # same "wrap at the call site too" discipline already used for
        # `order_book_observer`/`candle_observer`/`m1_candle_observer`
        # (Karar 87 and its successors): a health/dashboard snapshot must
        # NEVER fail (and so never blocks the health loop, never touches
        # trading) because of a display-only balance lookup, even a
        # hypothetical future bug in `check_usdt_balance()` itself.
        usdt_balance: dict[str, object] | None = None
        if self._execution_service is not None:
            try:
                balance = await self._execution_service.check_usdt_balance()
            except Exception:  # noqa: BLE001 - observability-only: must NEVER fail the whole snapshot or block trading
                _LOGGER.error("bridge lifecycle snapshot: check_usdt_balance FAILED (usdt_balance=None)", exc_info=True)
                balance = None
            if balance is not None:
                usdt_balance = {"asset": balance.asset, "free": balance.free, "locked": balance.locked}

        return {
            "enabled": True,
            "positions": positions,
            "usdt_balance": usdt_balance,
            "performance": {
                "exposure_usdt": accounting.total_exposure_usdt,
                "open_long_count": open_long,
                "dust_count": dust_count,
                "non_flat_position_count": accounting.open_position_count,
                "realized_gross_pnl": accounting.lifetime_realized_pnl_net_usdt,
                "todays_realized_gross_pnl": accounting.today_realized_pnl_net_usdt,
                "unrealized_gross_pnl_total": accounting.unrealized_pnl_usdt,
                "completed_trade_count": accounting.lifetime_trade_count,
                "win_count": len(wins),
                "loss_count": len(losses),
                "win_rate": win_rate,
                "average_win_gross": (sum(t["gross_realized_pnl"] for t in wins) / len(wins)) if wins else None,
                "average_loss_gross": (sum(t["gross_realized_pnl"] for t in losses) / len(losses)) if losses else None,
            },
            "recent_trades": recent_trades,
        }

    def _candle_history_snapshot(self, *, max_candles: int = 60) -> dict[str, object]:
        """Dashboard visual rebuild — the ONLY read-only source for the
        candlestick chart's REAL price history: `RuntimeCoordinator.
        _candle_windows` already holds an in-memory, bounded rolling
        window (`candle_window_size`, default 500) per (symbol,
        timeframe) — the SAME accessor precedent as `_latest_price()`
        above and `lifecycle_migration.py`/`lifecycle_runtime.py`'s own
        private-coordinator reads. No new persistence is introduced: this
        merely serializes the last `max_candles` of an ALREADY-EXISTING
        in-memory buffer into the SAME periodic atomic JSON snapshot the
        rest of the dashboard already reads — the dashboard's HTTP thread
        still never touches a live coordinator object directly (bkz.
        `ops/dashboard.py` modül docstring'i, Bölüm 6)."""
        coordinator = self.runtime._coordinator  # noqa: SLF001 - same private-accessor precedent as elsewhere in this file
        history: dict[str, object] = {}
        for symbol in coordinator._symbols:  # noqa: SLF001
            per_symbol: dict[str, object] = {}
            for timeframe in (Timeframe.M5, Timeframe.M15, Timeframe.H1):
                window = coordinator._candle_windows.get((symbol, timeframe))  # noqa: SLF001
                if window is None:
                    continue
                candles = window.history()[-max_candles:]
                if not candles:
                    continue
                per_symbol[timeframe.value] = [
                    {"t": c.close_time.isoformat(), "o": c.open, "h": c.high, "l": c.low, "c": c.close}
                    for c in candles
                ]
            if per_symbol:
                history[symbol] = per_symbol
        return history

    async def _write_snapshot(self, *, recovery_ok: bool, recovery_detail: str) -> None:
        write_snapshot(
            self._config.health_snapshot_path,
            status=self._runtime.status(),
            recovery_ok=recovery_ok,
            recovery_detail=recovery_detail,
            pid=os.getpid(),
            started_at=self._started_at,
            execution=self._execution_snapshot(),
            paper=self._paper_snapshot(),
            symbol_selection=self._symbol_selection_snapshot(),
            signal_testnet_bridge=self._signal_bridge_snapshot(),
            bridge_lifecycle=await self._bridge_lifecycle_snapshot(),
            candle_history=self._candle_history_snapshot(),
            admin=self._admin_snapshot(),
            adaptive=self._adaptive_snapshot(),
            backup=self._backup_snapshot(),
            event_log=self._event_log_snapshot(),
        )

    def _admin_snapshot(self) -> dict[str, object]:
        """24/7 Ops v1, Step 3 — `{"enabled": False}` whenever
        `dashboard_admin_token` is unset (no `AdminController`
        constructed). Contains no secret — see `AdminController.status()`."""
        if self._admin_controller is None:
            return {"enabled": False}
        return self._admin_controller.status()

    def _adaptive_snapshot(self) -> dict[str, object]:
        """UI Polish v1, Step A3 — `{"active": False}` whenever no
        `adaptive_status_provider` was supplied (see `__init__`). The
        provider itself lives entirely OUTSIDE this package (`adaptive/`
        is never imported here) — defensively wrapped at THIS call site,
        same "wrap at the call site too" discipline already used for
        `order_book_observer`/`candle_observer`/`m1_candle_observer`: a
        failing provider must NEVER stop a health snapshot from being
        written, and must NEVER affect trading."""
        if self._adaptive_status_provider is None:
            return {"active": False}
        try:
            return self._adaptive_status_provider()
        except Exception:  # noqa: BLE001 - a display-only provider must NEVER fail the snapshot
            _LOGGER.error("adaptive status provider FAILED (dashboard will show unavailable)", exc_info=True)
            return {"active": False, "detail": "adaptive status provider error"}

    def _backup_snapshot(self) -> dict[str, object]:
        """UI Polish v1, Step A7 — reads `scripts/backup_sqlite.py`'s
        additive `backup_status.json` (written next to `CSE_DB_PATH`,
        same atomic temp-file + `os.replace` pattern as `ops/health_
        snapshot.py`) READ-ONLY, exactly like every other file this
        health snapshot already reads. `{"available": False}` whenever
        the file is missing (no backup has ever run, or the timer isn't
        installed) or unreadable — NEVER fabricates a "last backup" time."""
        status_path = self._config.db_path.parent / "backup_status.json"
        try:
            if not status_path.exists():
                return {"available": False}
            with status_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            return {
                "available": True,
                "completed_at": data.get("completed_at"),
                "destination": data.get("destination"),
                "size_bytes": data.get("size_bytes"),
            }
        except (OSError, json.JSONDecodeError, ValueError):
            _LOGGER.warning("backup_status.json okunamadı (yoksayılıyor)", exc_info=True)
            return {"available": False}

    def _event_log_snapshot(self) -> dict[str, object]:
        """UI Polish v1, Step A9 — read-only, most-recent-first (see
        `ops/event_log.py::recent_events`, which never raises)."""
        return {"events": recent_events(self._event_log_path, limit=20)}


_SNAPSHOT_STALE_MULTIPLIER = 5
_SNAPSHOT_STALE_FLOOR_SECONDS = 30.0


def _run_status(config: AppConfig) -> int:
    """`generated_at` yaşı KASITLI OLARAK kontrol edilir: `write_snapshot`
    yalnızca çalışan bir process (canlı health-loop veya graceful shutdown)
    tarafından yazılır — process `kill -9`/OOM ile ANİ ölürse (shutdown
    KODU hiç çalışmadan), diskte kalan SON dosya hâlâ eski bir "READY"
    görüntüsü taşıyabilir. Bunu `overall_health` alanına KÖRÜ KÖRÜNE
    güvenerek "sağlıklı" raporlamak, tam olarak önlenmesi istenen
    "status healthy der ama runtime aslında sağlıklı değil" durumudur —
    bu yüzden dosyanın YAŞI da AÇIKÇA değerlendirilir (`systemctl status`
    process'in gerçekten çalışıp çalışmadığını zaten söyler; bu komut onu
    TEKRARLAMAZ, yalnızca KENDİ iddiasının güncelliğini doğrular)."""
    snapshot = read_snapshot(config.health_snapshot_path)
    if snapshot is None:
        print(f"NO SNAPSHOT YET at {config.health_snapshot_path} (service may not have started, or not yet ready)")
        return EXIT_FATAL

    generated_at = datetime.fromisoformat(snapshot["generated_at"])
    age_seconds = (utc_now() - generated_at).total_seconds()
    stale_threshold = max(
        config.health_snapshot_interval_seconds * _SNAPSHOT_STALE_MULTIPLIER, _SNAPSHOT_STALE_FLOOR_SECONDS
    )
    is_stale = age_seconds > stale_threshold

    print(f"overall_health: {snapshot['overall_health']}")
    print(f"generated_at:   {snapshot['generated_at']} (age={age_seconds:.1f}s)")
    print(f"pid:            {snapshot['pid']}")
    print(f"started_at:     {snapshot['started_at']}")
    recovery = snapshot["recovery"]
    print(f"recovery:       ok={recovery['ok']} detail={recovery['detail']!r}")
    for s in snapshot["symbols"]:
        print(
            f"  {s['symbol']:<12} {s['health']:<14} last_event={s['last_event_at']} "
            f"reconnects={s['reconnect_count']} detail={s['detail']!r}"
        )
    if is_stale:
        print(
            f"STALE SNAPSHOT: last written {age_seconds:.1f}s ago (threshold {stale_threshold:.1f}s) — "
            f"the process may have died without a graceful shutdown. Check `systemctl status "
            f"crypto-signal-engine` / journalctl to confirm whether it is still running."
        )
        return EXIT_FATAL
    return EXIT_OK if snapshot["overall_health"] == "READY" else EXIT_FATAL


def _run_doctor(config: AppConfig) -> int:
    """Faz 12 — deterministik, TAMAMEN offline (hiçbir ağ çağrısı YAPMAZ,
    hiçbir order göndermez) preflight. Yalnızca YEREL durumu doğrular:
    config zaten parse edildi (bu fonksiyona ulaşmak için ZATEN başarılı
    olmalı), dosya sistemi, kilit uygunluğu, şema uyumluluğu, execution-
    mode gating, Mainnet'in yapısal olarak İMKANSIZ olduğu, ve dashboard
    bind ayarının güvenli olduğu. Secret DEĞERLERİ ASLA yazdırılmaz —
    yalnızca SET/UNSET durumu (bkz. `BinanceTestnetConfig.__repr__`'in
    AYNI ilkesi, Faz 10 Karar)."""
    ok = True

    def check(passed: bool, label: str, detail: str = "") -> None:
        nonlocal ok
        ok = ok and passed
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {label}" + (f" — {detail}" if detail else ""))

    def warn(label: str, detail: str = "") -> None:
        print(f"[WARN] {label}" + (f" — {detail}" if detail else ""))

    print("crypto-signal-engine doctor — local preflight (offline, no orders, no network calls)")
    check(True, "config parses", "OK")

    db_dir = config.db_path.parent
    try:
        db_dir.mkdir(parents=True, exist_ok=True)
        writable = os.access(db_dir, os.W_OK)
    except OSError as exc:
        writable = False
        db_dir_detail = str(exc)
    else:
        db_dir_detail = str(db_dir)
    check(writable, "state directory writable", db_dir_detail)

    try:
        probe_store = PaperStateStore(config.db_path)
        probe_store.close()
        schema_ok = True
        schema_detail = str(config.db_path)
    except Exception as exc:  # noqa: BLE001 - herhangi bir açma/şema hatası açıkça raporlanır
        schema_ok = False
        schema_detail = str(exc)
    check(schema_ok, "SQLite database open + schema compatible", schema_detail)

    lock = ProcessLock(config.lock_path)
    try:
        lock.acquire()
        lock.release()
        lock_ok = True
        lock_detail = "available (no other instance holding it right now)"
    except ConcurrentInstanceError as exc:
        lock_ok = False
        lock_detail = str(exc)
    check(lock_ok, "singleton process lock available", lock_detail)

    check(isinstance(config.execution_mode, ExecutionMode), "execution mode is a valid ExecutionMode")
    print(f"[INFO] runtime mode: {config.execution_mode.value}")
    if config.execution_mode is ExecutionMode.PAPER:
        print("[PASS] PAPER is the effective mode — no credentials required, no private orders possible")
    elif not config.enable_testnet_execution:
        # KASITLI OLARAK bir FAIL DEĞİL: "TESTNET seçildi ama enable
        # bayrağı yok" GÜVENLİ bir durumdur (Bölüm 10 "paper-first
        # safety") — execution PAPER-eşdeğeri davranır, hiçbir private
        # order kod yoluna erişim YOKTUR. Yalnızca bilgilendirme amaçlı.
        print(
            "[INFO] TESTNET selected but CSE_ENABLE_TESTNET_EXECUTION is not true — "
            "behaves as PAPER-equivalent (no private orders possible)"
        )
    else:
        has_key, has_secret = testnet_credentials_status()
        check(
            has_key and has_secret, "TESTNET credentials configured",
            f"BINANCE_TESTNET_API_KEY={'SET' if has_key else 'UNSET'} "
            f"BINANCE_TESTNET_API_SECRET={'SET' if has_secret else 'UNSET'}",
        )
        try:
            host_ok = BinanceTestnetConfig().base_url == "https://testnet.binance.vision"
        except Exception:  # noqa: BLE001
            host_ok = False
        check(host_ok, "Testnet host exact allowlist match (https://testnet.binance.vision)")
        print("[INFO] TESTNET execution ENABLED — startup will run reconciliation before proceeding")
        if config.enable_signal_testnet_bridge:
            print(
                f"[INFO] Signal->TESTNET bridge ENABLED (policy=SPOT_LONG_ONLY, "
                f"notional={config.testnet_bridge_notional_usdt} USDT) — becomes operational only after "
                f"startup reconciliation succeeds"
            )
        else:
            print("[INFO] Signal->TESTNET bridge disabled (CSE_ENABLE_SIGNAL_TESTNET_BRIDGE is not true)")

    mainnet_impossible = "MAINNET" not in {m.value for m in ExecutionMode}
    check(mainnet_impossible, "Mainnet is not a representable ExecutionMode (structurally impossible)")

    if config.dashboard_enabled:
        loopback = config.dashboard_host in ("127.0.0.1", "localhost", "::1")
        if loopback:
            print(f"[PASS] dashboard bind is loopback-only: {config.dashboard_host}:{config.dashboard_port}")
        else:
            warn(
                "dashboard bind is NOT loopback",
                f"{config.dashboard_host}:{config.dashboard_port} — ensure this is intentional; "
                f"the documented safe pattern is 127.0.0.1 + Tailscale Serve, never a public bind",
            )
    else:
        print("[INFO] dashboard disabled (CSE_DASHBOARD_ENABLED=false)")

    print("[PASS] no secret values printed by this command (only SET/UNSET)")
    print("RESULT: " + ("ALL CHECKS PASSED" if ok else "ONE OR MORE CHECKS FAILED"))
    return EXIT_OK if ok else EXIT_FATAL


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m crypto_signal_engine.app", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run", help="Start the long-running PUBLIC market-data paper-trading service")
    subparsers.add_parser("status", help="Print the last written health snapshot and exit")
    subparsers.add_parser("doctor", help="Run an offline local preflight check and exit (no orders, no network)")
    args = parser.parse_args(argv)

    try:
        config = load_config(os.environ)
    except AppConfigurationError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    if args.command == "status":
        return _run_status(config)
    if args.command == "doctor":
        return _run_doctor(config)

    configure_logging(config.log_level)

    selection_result: SelectionResult | None = None
    pinned_symbols: tuple[str, ...] = ()
    if config.auto_select_symbols:
        _LOGGER.info("CSE_SYMBOLS not set — running automatic symbol selection (PUBLIC market data only)")
        try:
            config, selection_result, pinned_symbols = asyncio.run(resolve_symbols(config))
        except SymbolSelectionError as exc:
            _LOGGER.error(
                "automatic symbol selection FAILED — refusing to invent/default a symbol, "
                "runtime NOT started, existing durable PAPER state untouched: %s", exc,
            )
            print(f"AUTOMATIC SYMBOL SELECTION FAILED: {exc}", file=sys.stderr)
            return EXIT_SYMBOL_SELECTION_FAILURE
        _LOGGER.info(
            "automatic symbol selection OK: opportunities=%s pinned_open_positions=%s "
            "final_runtime_symbols=%s universe_size=%d shortlist_size=%d generated_at=%s",
            list(selection_result.selected_symbols), list(pinned_symbols), list(config.symbols),
            selection_result.universe_size, selection_result.shortlist_size,
            selection_result.generated_at.isoformat(),
        )
        for candidate in selection_result.ranked_candidates:
            _LOGGER.info("  candidate %-12s score=%.4f — %s", candidate.symbol, candidate.total_score, candidate.reason)
        for candidate in selection_result.rejected:
            _LOGGER.info("  rejected  %-12s — %s", candidate.symbol, candidate.reason)
        if pinned_symbols:
            _LOGGER.info(
                "  %d symbol(s) pinned due to an existing durable OPEN PAPER position "
                "(kept regardless of opportunity ranking): %s", len(pinned_symbols), list(pinned_symbols),
            )
    else:
        _LOGGER.info("symbol selection mode=MANUAL symbols=%s (CSE_SYMBOLS explicitly set)", list(config.symbols))

    provider = build_real_provider(config)
    app = Application(config, provider, selection_result=selection_result, pinned_symbols=pinned_symbols)
    try:
        return asyncio.run(app.start())
    except Exception:
        _LOGGER.error("fatal uncaught exception during startup", exc_info=True)
        return EXIT_FATAL


if __name__ == "__main__":
    sys.exit(main())


=== FILE: crypto_signal_engine/consensus/__init__.py ===


=== FILE: crypto_signal_engine/consensus/engine.py ===
"""
ConsensusEngine.

Kural (Faz 4 Bölüm 25): `combine()` yalnızca `AgentEvidence` koleksiyonu
alır; `RegimeContext`'i skorlama/agreement hesaplamasına DAHİL ETMEZ.

BİLİNÇLİ, RAPORLANMIŞ SÖZLEŞME SAPMASI (bkz. PHASE4_QUANT_AGENT_ENGINE.md
"Kabul edilen sözleşme sapması" bölümü): Phase 1'in ZATEN KABUL EDİLMİŞ
`ConsensusResult` domain modeli, `regime: RegimeContext` alanını ZORUNLU
(non-optional) olarak taşır. Bu, Faz 4 talimatının "ConsensusEngine
RegimeContext'i asla parametre olarak almamalı" ifadesiyle DOĞRUDAN
ÇELİŞİR. "Gerçek kod kazanır" ilkesi (ve "accepted prior-phase modelini
sırf daha temiz göstermek için değiştirme" kuralı) gereği, `ConsensusResult`
DEĞİŞTİRİLMEMİŞTİR; bunun yerine `combine()`, `regime`'i keyword-only bir
parametre olarak alır ve YALNIZCA `ConsensusResult`'ın zaten var olan
zorunlu alanını doldurmak için kullanır — raw_score/agreement
hesaplamasının HİÇBİR ADIMINDA regime kullanılmaz (talimatın "do not create
a regime-adjusted consensus score" ruhu tam olarak korunur).
"""

from __future__ import annotations

from collections.abc import Sequence

from crypto_signal_engine.domain.consensus import ConsensusResult, RegimeContext, compute_agreement
from crypto_signal_engine.domain.enums import AgentName
from crypto_signal_engine.domain.models import AgentEvidence
from crypto_signal_engine.errors import ConsensusError

AGENT_WEIGHTS: dict[AgentName, float] = {
    AgentName.QUANT: 0.35,
    AgentName.MARKET_STRUCTURE: 0.30,
    AgentName.ORDER_BOOK: 0.15,
    AgentName.REGIME: 0.20,
}
assert abs(sum(AGENT_WEIGHTS.values()) - 1.0) < 1e-9

_CANONICAL_AGENT_ORDER = [AgentName.QUANT, AgentName.MARKET_STRUCTURE, AgentName.ORDER_BOOK, AgentName.REGIME]


class ConsensusEngine:
    """Bağımsız `AgentEvidence`'ları sabit ağırlıklarla `ConsensusResult`'a birleştirir."""

    def combine(self, evidence: Sequence[AgentEvidence], *, regime: RegimeContext) -> ConsensusResult:
        if not isinstance(evidence, Sequence) or isinstance(evidence, (str, bytes)):
            raise ConsensusError(f"evidence bir Sequence[AgentEvidence] olmalı, alınan: {type(evidence)!r}")
        if len(evidence) == 0:
            raise ConsensusError("evidence boş olamaz")
        for e in evidence:
            if not isinstance(e, AgentEvidence):
                raise ConsensusError(f"evidence yalnızca AgentEvidence içermeli, alınan: {type(e)!r}")

        unknown_agents = [e.agent for e in evidence if e.agent not in AGENT_WEIGHTS]
        if unknown_agents:
            raise ConsensusError(f"bilinmeyen agent(lar) için ağırlık tanımlı değil: {unknown_agents}")

        raw_score = sum(AGENT_WEIGHTS[e.agent] * e.score for e in evidence)
        raw_score = max(-1.0, min(1.0, raw_score))

        agreement = compute_agreement(tuple(e.score for e in evidence))

        sorted_evidence = sorted(evidence, key=lambda e: _CANONICAL_AGENT_ORDER.index(e.agent))

        try:
            return ConsensusResult(
                symbol=evidence[0].symbol,
                context_id=evidence[0].context_id,
                raw_score=raw_score,
                agreement=agreement,
                regime=regime,
                contributing_evidence=tuple(sorted_evidence),
            )
        except ValueError as exc:
            raise ConsensusError(str(exc)) from exc


=== FILE: crypto_signal_engine/consensus/risk.py ===
"""
RiskOverlay.

Kural (Faz 4 Bölüm 26-29): strictly post-consensus. Yalnızca
`ConsensusResult` ve `RegimeContext` alır (`RegimeAgentOutput`, ham
`AgentEvidence`, `AgentContext`, veya agent nesneleri ASLA doğrudan
alınmaz — `consensus.contributing_evidence` üzerinden dolaylı olarak
zaten erişilebilir). Yalnızca `confidence_multiplier`'ı AŞAĞI çekebilir;
`raw_score`, işaret, veya yön ASLA değiştirilmez (bu zaten mümkün değildir
— `RiskAssessment` `ConsensusResult`'ı hiçbir şekilde mutate etmez, yalnızca
YENİ bir `RiskAssessment` nesnesi üretir).

Provenance notu (Bölüm 6): `ConsensusResult` ile `RegimeContext`'in aynı
değerlendirmeye ait olduğu invariant'ı ORKESTRATÖR seviyesinde garanti
edilir (bkz. `signal_engine.py`). `RegimeContext` bu garantiyi ispatlamak
için provenance alanları (context_id/symbol/timestamp) taşıyacak şekilde
YENİDEN TASARLANMAMIŞTIR — mevcut kabul edilmiş model korunmuştur.
"""

from __future__ import annotations

from crypto_signal_engine.agents.base import clamp
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.consensus import ConsensusResult, RegimeContext, RiskAssessment
from crypto_signal_engine.domain.enums import LiquidityRegime, RiskLevel, VolatilityRegime

_AGREEMENT_NO_PENALTY_MIN = 0.75
_AGREEMENT_MILD_MIN = 0.50
_AGREEMENT_MATERIAL_MIN = 0.30

_AGREEMENT_MULT_NONE = 1.0
_AGREEMENT_MULT_MILD = 0.85
_AGREEMENT_MULT_MATERIAL = 0.60
_AGREEMENT_MULT_SEVERE = 0.35

_VOLATILITY_MULT = {
    VolatilityRegime.LOW: 1.0,
    VolatilityRegime.NORMAL: 1.0,
    VolatilityRegime.HIGH: 0.75,
    VolatilityRegime.EXTREME: 0.50,
    VolatilityRegime.UNKNOWN: 0.75,
}

_LIQUIDITY_MULT = {
    LiquidityRegime.NORMAL: 1.0,
    LiquidityRegime.THIN: 0.80,
    LiquidityRegime.STRESSED: 0.55,
    LiquidityRegime.UNKNOWN: 0.80,
}

_CONTRADICTION_SEVERE_THRESHOLD = 0.50
_CONTRADICTION_MILD_THRESHOLD = 0.25
_CONTRADICTION_MULT_SEVERE = 0.60
_CONTRADICTION_MULT_MILD = 0.85
_CONTRADICTION_MULT_NONE = 1.0

_AGREEMENT_LEVEL = {
    "none": RiskLevel.LOW, "mild": RiskLevel.MEDIUM, "material": RiskLevel.HIGH, "severe": RiskLevel.EXTREME,
}
_VOLATILITY_LEVEL = {
    VolatilityRegime.LOW: RiskLevel.LOW, VolatilityRegime.NORMAL: RiskLevel.LOW,
    VolatilityRegime.HIGH: RiskLevel.HIGH, VolatilityRegime.EXTREME: RiskLevel.EXTREME,
    VolatilityRegime.UNKNOWN: RiskLevel.MEDIUM,
}
_LIQUIDITY_LEVEL = {
    LiquidityRegime.NORMAL: RiskLevel.LOW, LiquidityRegime.THIN: RiskLevel.MEDIUM,
    LiquidityRegime.STRESSED: RiskLevel.HIGH, LiquidityRegime.UNKNOWN: RiskLevel.MEDIUM,
}
_CONTRADICTION_LEVEL = {"none": RiskLevel.LOW, "mild": RiskLevel.MEDIUM, "severe": RiskLevel.HIGH}

_RISK_LEVEL_RANK = {RiskLevel.LOW: 0, RiskLevel.MEDIUM: 1, RiskLevel.HIGH: 2, RiskLevel.EXTREME: 3}


def _agreement_band(agreement: float) -> str:
    if agreement >= _AGREEMENT_NO_PENALTY_MIN:
        return "none"
    if agreement >= _AGREEMENT_MILD_MIN:
        return "mild"
    if agreement >= _AGREEMENT_MATERIAL_MIN:
        return "material"
    return "severe"


def _contradiction_band(consensus: ConsensusResult) -> tuple[str, float]:
    scores = [e.score for e in consensus.contributing_evidence]
    if consensus.raw_score == 0.0:
        opposition = sum(abs(s) for s in scores if s != 0.0)
    else:
        sign = 1.0 if consensus.raw_score > 0 else -1.0
        opposition = sum(abs(s) for s in scores if (s * sign) < 0.0)
    total_weight = sum(abs(s) for s in scores) or 1.0
    magnitude = opposition / total_weight
    if magnitude >= _CONTRADICTION_SEVERE_THRESHOLD:
        return "severe", magnitude
    if magnitude >= _CONTRADICTION_MILD_THRESHOLD:
        return "mild", magnitude
    return "none", magnitude


class RiskOverlay:
    """`ConsensusResult` + `RegimeContext` -> `RiskAssessment` (strictly post-consensus)."""

    def assess(self, consensus: ConsensusResult, regime: RegimeContext) -> RiskAssessment:
        if not isinstance(consensus, ConsensusResult):
            raise TypeError(f"consensus bir ConsensusResult olmalı, alınan: {type(consensus)!r}")
        if not isinstance(regime, RegimeContext):
            raise TypeError(f"regime bir RegimeContext olmalı, alınan: {type(regime)!r}")

        agreement_band = _agreement_band(consensus.agreement)
        agreement_mult = {
            "none": _AGREEMENT_MULT_NONE, "mild": _AGREEMENT_MULT_MILD,
            "material": _AGREEMENT_MULT_MATERIAL, "severe": _AGREEMENT_MULT_SEVERE,
        }[agreement_band]

        volatility_mult = _VOLATILITY_MULT[regime.volatility]
        liquidity_mult = _LIQUIDITY_MULT[regime.liquidity]

        contradiction_band, opposition_magnitude = _contradiction_band(consensus)
        contradiction_mult = {
            "none": _CONTRADICTION_MULT_NONE, "mild": _CONTRADICTION_MULT_MILD, "severe": _CONTRADICTION_MULT_SEVERE,
        }[contradiction_band]

        confidence_multiplier = clamp(
            agreement_mult * volatility_mult * liquidity_mult * contradiction_mult, 0.0, 1.0
        )
        confidence_multiplier = require_finite(confidence_multiplier, "confidence_multiplier")

        candidates = [
            _AGREEMENT_LEVEL[agreement_band],
            _VOLATILITY_LEVEL[regime.volatility],
            _LIQUIDITY_LEVEL[regime.liquidity],
            _CONTRADICTION_LEVEL[contradiction_band],
        ]
        risk_level = max(candidates, key=lambda level: _RISK_LEVEL_RANK[level])

        rationale = (
            f"agreement={consensus.agreement:.2f} ({agreement_band}, x{agreement_mult:.2f}); "
            f"volatility={regime.volatility.value} (x{volatility_mult:.2f}); "
            f"liquidity={regime.liquidity.value} (x{liquidity_mult:.2f}); "
            f"contradiction={contradiction_band} (muhalefet oranı {opposition_magnitude:.2f}, "
            f"x{contradiction_mult:.2f}) -> confidence_multiplier={confidence_multiplier:.3f}, "
            f"risk_level={risk_level.value}."
        )

        return RiskAssessment(
            confidence_multiplier=confidence_multiplier,
            risk_level=risk_level,
            rationale=rationale,
            contradicting_metrics={
                "agreement": consensus.agreement,
                "agreement_multiplier": agreement_mult,
                "volatility_multiplier": volatility_mult,
                "liquidity_multiplier": liquidity_mult,
                "opposition_magnitude": opposition_magnitude,
                "contradiction_multiplier": contradiction_mult,
            },
        )


=== FILE: crypto_signal_engine/domain/__init__.py ===



=== FILE: crypto_signal_engine/domain/_validation.py ===
"""
Ortak domain validation yardımcıları.

Bu modül, domain katmanındaki TÜM dataclass'ların paylaştığı tek bir
invariant uygulama noktasıdır. Amaç: aynı kontrolün (finite sayı, UTC-aware
zaman, symbol normalizasyonu, gerçek immutability) her dataclass içinde
ayrı ayrı ve tutarsız biçimde yeniden yazılmasını önlemek.

Hiçbir fonksiyon burada iş/quant mantığı içermez; yalnızca invariant
uygulaması yapar.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import TypeVar

TKey = TypeVar("TKey")
TValue = TypeVar("TValue")
TEnum = TypeVar("TEnum", bound=Enum)


def require_finite(value: float, field_name: str) -> float:
    """Bir sayının NaN veya ±inf olmadığını doğrular.

    `math.isfinite` hem NaN hem de +inf/-inf durumlarını False döndürerek
    yakalar; bu yüzden tek kontrol noktası olarak yeterlidir.
    """
    if not math.isfinite(value):
        raise ValueError(
            f"{field_name} finite bir sayı olmalı (NaN/+inf/-inf kabul edilmez), "
            f"alınan: {value!r}"
        )
    return value


def require_utc_aware(value: datetime, field_name: str) -> datetime:
    """Bir datetime'ın UTC-aware olduğunu doğrular.

    Naive datetime'lar (tzinfo=None) KESİNLİKLE reddedilir — sessizce UTC
    varsayılmaz. Başka bir timezone'daki aware datetime'lar da reddedilir;
    domain katmanına yalnızca UTC girebilir. Dönüşüm sorumluluğu provider
    (boundary) katmanındadır.
    """
    if value.tzinfo is None:
        raise ValueError(
            f"{field_name} naive datetime olamaz; UTC-aware olmalı. "
            f"Provider/boundary katmanında datetime.fromtimestamp(x, tz=timezone.utc) "
            f"ile dönüştürün."
        )
    if value.tzinfo.utcoffset(value) != timezone.utc.utcoffset(value):
        raise ValueError(f"{field_name} UTC olmalı, alınan tzinfo: {value.tzinfo!r}")
    return value


def normalize_symbol(raw: str, field_name: str = "symbol") -> str:
    """Symbol string'ini deterministik canonical forma çevirir.

    Politika: strip + upper-case. "btcusdt", " BTCUSDT", "BTCUSDT" hepsi
    aynı canonical "BTCUSDT" değerine normalize edilir; bu sayede aynı
    varlık için üç farklı state key oluşması önlenir (bkz. DECISIONS.md).
    Normalizasyon sonrası boş string reddedilir.

    HARDENING NOTU (Quality Gate 28): `raw` string OLMAYAN bir değerse
    (None, int, vb.) `AttributeError` gibi incidental bir exception değil,
    açık ve deterministik bir `TypeError` fırlatılır.
    """
    if not isinstance(raw, str):
        raise TypeError(f"{field_name} bir str olmalı, alınan: {type(raw).__name__}")
    normalized = raw.strip().upper()
    if not normalized:
        raise ValueError(f"{field_name} boş olamaz")
    return normalized


def require_enum(value: TEnum, enum_type: type[TEnum], field_name: str) -> TEnum:
    """Bir değerin GERÇEK bir enum instance'ı olduğunu doğrular.

    HARDENING NOTU (Quality Gate 25): type hint (`agent: AgentName`) tek
    başına runtime'da hiçbir şeyi garanti etmez — Python bunu zorlamaz.
    Bu fonksiyon, string değerlerin ("QUANT" gibi) sessizce enum yerine
    kabul edilmesini KESİN olarak engeller. String'den enum'a otomatik
    cast YAPILMAZ; domain boundary strict'tir.
    """
    if not isinstance(value, enum_type):
        raise TypeError(
            f"{field_name} bir {enum_type.__name__} enum değeri olmalı, "
            f"alınan: {value!r} (tip: {type(value).__name__})"
        )
    return value


def freeze_mapping(source: Mapping[TKey, TValue], field_name: str) -> Mapping[TKey, TValue]:
    """Gerçek (nested-safe) immutable bir mapping üretir.

    Yaklaşım: önce `dict(source)` ile DEFENSIVE COPY alınır (çağıranın elindeki
    orijinal dict sonradan mutate edilse bile domain nesnesi etkilenmez),
    ardından bu kopya `MappingProxyType` ile read-only bir view'a sarılır
    (üzerinde `__setitem__`/`__delitem__` çağrısı `TypeError` fırlatır).

    Yalnızca type hint (`Mapping[str, float]`) yazmak runtime'da hiçbir şeyi
    garanti etmez — bu fonksiyon gerçek runtime garantisi sağlar.
    """
    if not isinstance(source, Mapping):
        raise TypeError(f"{field_name} bir Mapping olmalı, alınan: {type(source)!r}")
    return MappingProxyType(dict(source))


=== FILE: crypto_signal_engine/domain/candle_sequencing.py ===
"""
Candle update sequencing.

HARDENING NOTU (Quality Gate 2): close_time bir candle'ın "event sequence"i
olarak KULLANILMAZ. Aynı açık candle WebSocket üzerinden defalarca
güncellenir (örn. 10:00:01, 10:00:08, ..., 10:05:00 final) — bunların hepsi
AYNI candle identity'sine ait FARKLI update'lerdir, aynı "event" değildir.

Bu modül dört kavramı kesin biçimde ayırır:

1. Candle identity        -> (symbol, timeframe, open_time)      [models.CandleIdentity]
2. Update ordering        -> update_seq (bu modülde tanımlı)
3. Exchange event time    -> event_time (Binance'in bildirdiği zaman)
4. received_at            -> sistemin mesajı aldığı UTC zaman

`update_seq`, provider/ingress katmanı tarafından atanan, AYNI candle
identity'si için MONOTONİK artan bir tamsayıdır (örn. WebSocket mesaj
sırası veya Binance'in kline payload'ındaki event time epoch'u — provider
seçer, ancak CandleSequencer'a monotonik artan bir değer olarak ulaşması
ZORUNLUDUR). Farklı candle identity'leri birbirinden BAĞIMSIZ sequence
alanlarına sahiptir.

Bu sınıf saf (pure), in-memory bir sıralama/kabul mantığıdır; kalıcılık
(persistence) içermez. Gerçek StateManager (Faz 2) bu sınıfı bir
DB-backed repository ile sarmalayacaktır.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import require_utc_aware
from crypto_signal_engine.domain.models import Candle, CandleIdentity


class CandleUpdateOutcome(str, Enum):
    """Bir candle update'inin CandleSequencer tarafından değerlendirme sonucu."""

    ACCEPTED_UPDATE = "ACCEPTED_UPDATE"  # kabul edildi, candle henüz açık
    ACCEPTED_FINAL = "ACCEPTED_FINAL"  # kabul edildi, candle şimdi kapandı (is_closed=True)
    REJECTED_DUPLICATE = "REJECTED_DUPLICATE"  # aynı update_seq daha önce işlendi
    REJECTED_OUT_OF_ORDER = "REJECTED_OUT_OF_ORDER"  # update_seq, mevcut latest'ten küçük
    REJECTED_AFTER_FINAL = "REJECTED_AFTER_FINAL"  # bu identity zaten finalize edildi


@dataclass(frozen=True)
class CandleUpdate:
    """Provider katmanından gelen tek bir candle update mesajı."""

    candle: Candle
    update_seq: int
    event_time: datetime  # borsanın bildirdiği zaman (exchange event time)
    received_at: datetime  # sistemin mesajı aldığı UTC zaman

    def __post_init__(self) -> None:
        require_utc_aware(self.event_time, "event_time")
        require_utc_aware(self.received_at, "received_at")
        if self.update_seq < 0:
            raise ValueError("update_seq negatif olamaz")

    @property
    def identity(self) -> CandleIdentity:
        return self.candle.identity


@dataclass(frozen=True)
class CandleUpdateResult:
    """CandleSequencer.apply() çağrısının sonucu."""

    outcome: CandleUpdateOutcome
    canonical_candle: Candle | None  # kabul sonrası (veya reddedilse bile) mevcut canonical state

    @property
    def accepted(self) -> bool:
        return self.outcome in (CandleUpdateOutcome.ACCEPTED_UPDATE, CandleUpdateOutcome.ACCEPTED_FINAL)


@dataclass
class CandleSequencer:
    """(symbol, timeframe, open_time) bazında candle update sıralama/kabul motoru.

    Kasıtlı olarak mutable bir sınıf: tek bir "gerçek" canonical state'i
    (identity -> son kabul edilen update) sahiplenir ve yalnızca kendi
    `apply()` metodu üzerinden güncellenir (global mutable state ile
    karıştırılmamalı — bu nesne açıkça bir tane instantiate edilip
    StateManager tarafından sahiplenilir).
    """

    _latest: dict[CandleIdentity, CandleUpdate] = field(default_factory=dict)
    _finalized: set[CandleIdentity] = field(default_factory=set)

    def apply(self, update: CandleUpdate) -> CandleUpdateResult:
        """Bir candle update'ini değerlendirir ve kabul/red kararını uygular.

        Kurallar:
        - Bu identity zaten finalize edilmişse (final candle daha önce
          kabul edildiyse), her türlü yeni update REJECTED_AFTER_FINAL olur
          (reconnect sonrası eski/stale bir update'in canonical state'i
          geriye götürmesi engellenir).
        - update_seq, aynı identity için daha önce görülen en yüksek
          değerden büyük değilse (eşit -> duplicate, küçük -> out-of-order)
          reddedilir.
        - Kabul edilen update canonical state'e yazılır; candle.is_closed
          True ise identity finalize edilir.
        """
        identity = update.identity

        if identity in self._finalized:
            return CandleUpdateResult(
                outcome=CandleUpdateOutcome.REJECTED_AFTER_FINAL,
                canonical_candle=self._latest[identity].candle,
            )

        previous = self._latest.get(identity)
        if previous is not None:
            if update.update_seq == previous.update_seq:
                return CandleUpdateResult(
                    outcome=CandleUpdateOutcome.REJECTED_DUPLICATE,
                    canonical_candle=previous.candle,
                )
            if update.update_seq < previous.update_seq:
                return CandleUpdateResult(
                    outcome=CandleUpdateOutcome.REJECTED_OUT_OF_ORDER,
                    canonical_candle=previous.candle,
                )

        self._latest[identity] = update
        if update.candle.is_closed:
            self._finalized.add(identity)
            return CandleUpdateResult(
                outcome=CandleUpdateOutcome.ACCEPTED_FINAL, canonical_candle=update.candle
            )
        return CandleUpdateResult(
            outcome=CandleUpdateOutcome.ACCEPTED_UPDATE, canonical_candle=update.candle
        )

    def peek(self, identity: CandleIdentity) -> Candle | None:
        """Read-only: bir identity'nin mevcut canonical candle'ını döndürür (yoksa None)."""
        latest = self._latest.get(identity)
        return latest.candle if latest else None

    def is_finalized(self, identity: CandleIdentity) -> bool:
        return identity in self._finalized


=== FILE: crypto_signal_engine/domain/consensus.py ===
"""
ConsensusEngine ve RiskOverlay için domain sözleşmesi.

Mimari karar: RiskAgent diğer agent çıktısını okuyan bir "agent" DEĞİL,
ConsensusResult üzerinde çalışan ayrı bir RiskOverlay adımıdır (bkz.
DECISIONS.md). Akış:

    AgentEvidence[] -> ConsensusEngine.combine() -> ConsensusResult
    ConsensusResult -> RiskOverlay.apply() -> Signal (nihai)

HARDENING NOTU (Quality Gate 11 — provenance): ConsensusResult, kendisini
oluşturan TÜM evidence'ların (a) aynı symbol'e (b) aynı context_id'ye
sahip olduğunu ZORUNLU kılar. Farklı context/symbol'e ait evidence'ların
karıştırılması reddedilir.

HARDENING NOTU (Quality Gate 17): aynı AgentName'in aynı context içinde
birden fazla evidence üretmesi (ağırlığın yanlışlıkla ikiye katlanması)
reddedilir — one AgentName -> one AgentEvidence per context.

HARDENING NOTU (Quality Gate 12): eski tek `MarketRegime` yerine
`RegimeContext` (StructureRegime + VolatilityRegime + LiquidityRegime)
kullanılır — bu üç eksen mutually-exclusive değildir.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field

from crypto_signal_engine.domain._validation import freeze_mapping, normalize_symbol, require_enum, require_finite
from crypto_signal_engine.domain.enums import LiquidityRegime, RiskLevel, StructureRegime, VolatilityRegime
from crypto_signal_engine.domain.models import AgentEvidence


@dataclass(frozen=True)
class RegimeContext:
    """Üç bağımsız piyasa rejimi ekseninin birleşimi (Quality Gate 12).

    HARDENING NOTU (Quality Gate 25/4): her eksen GERÇEK bir enum instance'ı
    olmak zorundadır; string değerler ("TRENDING" gibi) sessizce kabul
    edilmez.
    """

    structure: StructureRegime
    volatility: VolatilityRegime
    liquidity: LiquidityRegime

    def __post_init__(self) -> None:
        require_enum(self.structure, StructureRegime, "structure")
        require_enum(self.volatility, VolatilityRegime, "volatility")
        require_enum(self.liquidity, LiquidityRegime, "liquidity")


def compute_agreement(scores: tuple[float, ...]) -> float:
    """Agent skorları arasındaki uyumu [0.0, 1.0] aralığında hesaplar.

    Yöntem: ağırlıklı işaret uyumu (sign agreement) ile [-1,1] aralığına
    göre normalize edilmiş dispersiyonun çarpımı. `1 - std_dev(scores)`
    KULLANILMAZ çünkü std_dev [-1,1] aralığında 1'i aşabilir ve sonucu
    negatife düşürüp sezgisel olmayan davranışa yol açabilir.
    """
    if not scores:
        raise ValueError("scores boş olamaz")
    for s in scores:
        require_finite(s, "score")
    if len(scores) == 1:
        return 1.0

    total_weight = sum(abs(s) for s in scores)
    if total_weight == 0:
        sign_agreement = 1.0
    else:
        weighted_sign_sum = sum(scores)
        sign_agreement = abs(weighted_sign_sum) / total_weight

    mean = sum(scores) / len(scores)
    variance = sum((s - mean) ** 2 for s in scores) / len(scores)
    std_dev = math.sqrt(variance)
    dispersion_penalty = min(std_dev / 1.0, 1.0)

    agreement = sign_agreement * (1.0 - dispersion_penalty)
    result = max(0.0, min(1.0, agreement))
    return require_finite(result, "agreement")


@dataclass(frozen=True)
class ConsensusResult:
    """ConsensusEngine'in ürettiği, RiskOverlay'e girmeden önceki ara sonuç.

    HARDENING NOTU: __post_init__ artık şunları ZORUNLU kılar:
    - contributing_evidence boş olamaz
    - TÜM evidence.symbol == self.symbol
    - TÜM evidence.context_id == self.context_id
    - aynı AgentName birden fazla kez geçemez (duplicate agent evidence)
    """

    symbol: str
    context_id: str
    raw_score: float
    agreement: float
    regime: RegimeContext
    contributing_evidence: tuple[AgentEvidence, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz")

        require_finite(self.raw_score, "raw_score")
        if not -1.0 <= self.raw_score <= 1.0:
            raise ValueError(f"raw_score [-1, 1] aralığında olmalı: {self.raw_score}")

        require_finite(self.agreement, "agreement")
        if not 0.0 <= self.agreement <= 1.0:
            raise ValueError(f"agreement [0, 1] aralığında olmalı: {self.agreement}")

        if not isinstance(self.contributing_evidence, tuple):
            object.__setattr__(self, "contributing_evidence", tuple(self.contributing_evidence))
        if not self.contributing_evidence:
            raise ValueError("contributing_evidence boş olamaz")

        for evidence in self.contributing_evidence:
            if evidence.symbol != self.symbol:
                raise ValueError(
                    f"mixed-symbol consensus reddedildi: evidence.symbol={evidence.symbol} "
                    f"!= consensus.symbol={self.symbol}"
                )
            if evidence.context_id != self.context_id:
                raise ValueError(
                    f"mismatched context reddedildi: evidence.context_id={evidence.context_id} "
                    f"!= consensus.context_id={self.context_id}"
                )

        agent_counts = Counter(e.agent for e in self.contributing_evidence)
        duplicates = [agent for agent, count in agent_counts.items() if count > 1]
        if duplicates:
            raise ValueError(
                f"duplicate agent evidence reddedildi: aynı context içinde birden fazla "
                f"evidence üreten agent(lar): {duplicates}"
            )


@dataclass(frozen=True)
class RiskAssessment:
    """RiskOverlay'in ConsensusResult üzerine uyguladığı değerlendirme.

    RiskOverlay YALNIZCA confidence'ı aşağı çekebilir; score'un yönünü/
    büyüklüğünü asla değiştiremez.
    """

    confidence_multiplier: float
    risk_level: RiskLevel
    rationale: str
    contradicting_metrics: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_finite(self.confidence_multiplier, "confidence_multiplier")
        if not 0.0 <= self.confidence_multiplier <= 1.0:
            raise ValueError(
                f"confidence_multiplier [0, 1] aralığında olmalı: {self.confidence_multiplier}"
            )
        require_enum(self.risk_level, RiskLevel, "risk_level")
        if not self.rationale.strip():
            raise ValueError("rationale boş olamaz (explainability zorunlu)")

        for name, value in self.contradicting_metrics.items():
            require_finite(value, f"contradicting_metrics['{name}']")

        object.__setattr__(
            self, "contradicting_metrics", freeze_mapping(self.contradicting_metrics, "contradicting_metrics")
        )


=== FILE: crypto_signal_engine/domain/enums.py ===
"""
Ortak enum'lar.

Bu modül sistemin geri kalanının paylaştığı "vocabulary"dir.
Hiçbir işlem/quant mantığı içermez; sadece kelime dağarcığını sabitler.

HARDENING NOTU (Quality Gate 12): eski tek `MarketRegime` enum'u kaldırıldı.
Yerine üç bağımsız semantik eksen getirildi: StructureRegime, VolatilityRegime,
LiquidityRegime. Sebep: piyasa aynı anda TRENDING + HIGH_VOLATILITY +
LOW_LIQUIDITY olabilir; bunlar mutually-exclusive tek enum içinde tutulamaz.
Bkz. DECISIONS.md — "Breaking domain decision: regime decomposition".
"""

from __future__ import annotations

from enum import Enum


class Timeframe(str, Enum):
    """Desteklenen zaman dilimleri."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"


class TimeframeRole(str, Enum):
    """Bir timeframe'in sinyal üretimindeki mimari rolü (yalnızca sözleşme)."""

    REGIME = "regime"  # 1h -> piyasa rejimi tespiti
    STRUCTURAL_TREND = "structural_trend"  # 15m -> yapısal trend
    PRIMARY_SIGNAL = "primary_signal"  # 5m -> ana sinyal kaynağı
    MICROSTRUCTURE_CONFIRMATION = "microstructure_confirmation"
    # 1m -> yön belirlemez, yalnızca microstructure/timing onayı sağlar.
    # Ana analiz sisteminde "execution" kavramı yoktur (bkz. DECISIONS.md).


class SignalDirection(str, Enum):
    """Sinyal yönü. Yalnızca BUY/SELL değil, kademeli bir ölçek."""

    STRONG_LONG = "STRONG_LONG"
    LONG = "LONG"
    WEAK_LONG = "WEAK_LONG"
    NEUTRAL = "NEUTRAL"
    WEAK_SHORT = "WEAK_SHORT"
    SHORT = "SHORT"
    STRONG_SHORT = "STRONG_SHORT"

    @classmethod
    def from_score(cls, score: float) -> "SignalDirection":
        """Sayısal skoru ([-1, 1]) kademeli yön etiketine çevirir.

        Bu, Signal.direction için TEK kaynak-of-truth mekanizmasıdır
        (Quality Gate 5). score dışında hiçbir yerde bağımsız bir
        direction state'i tutulmaz.
        """
        import math

        if not math.isfinite(score):
            raise ValueError(f"score finite olmalı, alınan: {score!r}")
        if not -1.0 <= score <= 1.0:
            raise ValueError(f"score [-1.0, 1.0] aralığında olmalı, alınan: {score}")

        t = _SIGNAL_DIRECTION_THRESHOLDS
        if score >= t["strong"]:
            return cls.STRONG_LONG
        if score >= t["moderate"]:
            return cls.LONG
        if score >= t["weak"]:
            return cls.WEAK_LONG
        if score <= -t["strong"]:
            return cls.STRONG_SHORT
        if score <= -t["moderate"]:
            return cls.SHORT
        if score <= -t["weak"]:
            return cls.WEAK_SHORT
        return cls.NEUTRAL


# Adlandırılmış eşikler: magic number değil, tek bir yerden yönetilen sabitler.
_SIGNAL_DIRECTION_THRESHOLDS = {
    "weak": 0.15,
    "moderate": 0.40,
    "strong": 0.70,
}


class RiskLevel(str, Enum):
    """Bir sinyalin taşıdığı risk seviyesi."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


class StructureRegime(str, Enum):
    """Piyasa yapısal rejimi (trend/range/breakout ekseni)."""

    TRENDING = "TRENDING"
    RANGING = "RANGING"
    BREAKOUT = "BREAKOUT"
    MEAN_REVERSION = "MEAN_REVERSION"
    UNKNOWN = "UNKNOWN"


class VolatilityRegime(str, Enum):
    """Piyasa volatilite rejimi (bağımsız eksen)."""

    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    EXTREME = "EXTREME"
    UNKNOWN = "UNKNOWN"


class LiquidityRegime(str, Enum):
    """Piyasa likidite rejimi (bağımsız eksen)."""

    NORMAL = "NORMAL"
    THIN = "THIN"
    STRESSED = "STRESSED"
    UNKNOWN = "UNKNOWN"


class DataQualityStatus(str, Enum):
    """Veri kalitesi kontrolünün sonucu."""

    OK = "OK"
    MISSING_CANDLE = "MISSING_CANDLE"
    DUPLICATE_CANDLE = "DUPLICATE_CANDLE"
    TIMESTAMP_MISMATCH = "TIMESTAMP_MISMATCH"
    STALE_PRICE = "STALE_PRICE"
    IMPOSSIBLE_OHLC = "IMPOSSIBLE_OHLC"
    ZERO_VOLUME_ANOMALY = "ZERO_VOLUME_ANOMALY"
    WEBSOCKET_GAP = "WEBSOCKET_GAP"
    EXTREME_OUTLIER = "EXTREME_OUTLIER"


class AgentName(str, Enum):
    """Sistemdeki bağımsız agent'ların sabit kimlikleri.

    RiskAgent burada kasıtlı olarak YOKTUR: risk değerlendirmesi
    ConsensusEngine sonrası çalışan RiskOverlay tarafından yapılır.
    """

    QUANT = "QuantAgent"
    MARKET_STRUCTURE = "MarketStructureAgent"
    ORDER_BOOK = "OrderBookAgent"
    REGIME = "RegimeAgent"


class SafetySeverity(str, Enum):
    """SafetyOrchestrator'ın ürettiği olayların önem derecesi."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    HALT = "HALT"


class LogCategory(str, Enum):
    """Observability için sabit log kategorileri."""

    DATA = "DATA"
    QUANT = "QUANT"
    SIGNAL = "SIGNAL"
    RISK = "RISK"
    SAFETY = "SAFETY"
    DATABASE = "DATABASE"
    WEBSOCKET = "WEBSOCKET"
    TESTNET = "TESTNET"
    NOTIFICATION = "NOTIFICATION"


=== FILE: crypto_signal_engine/domain/events.py ===
"""
Event modelleri.

HARDENING NOTU (Quality Gate 3): MarketDataEvent artık runtime'da
EventType <-> payload type eşleşmesini VE event.symbol == payload.symbol
eşleşmesini zorunlu kılar. Type annotation (Generic[TPayload]) tek başına
hiçbir runtime garantisi sağlamaz; bu yüzden __post_init__ içinde açık
isinstance kontrolü yapılır.

Not: candle identity/update sıralaması ARTIK bu modülde DEĞİL,
candle_sequencing.py'de ele alınır (bkz. Quality Gate 2). MarketDataEvent
+ SequenceCursor burada YALNIZCA trade/order-book gibi doğal, tekil
monotonik ID'si olan akışlar için genel bir zarf/cursor mekanizmasıdır.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generic, TypeVar

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade

TPayload = TypeVar("TPayload", Candle, Trade, OrderBookSnapshot)


class EventType(str, Enum):
    CANDLE = "CANDLE"
    TRADE = "TRADE"
    ORDER_BOOK = "ORDER_BOOK"


_EXPECTED_PAYLOAD_TYPE: dict[EventType, type] = {
    EventType.CANDLE: Candle,
    EventType.TRADE: Trade,
    EventType.ORDER_BOOK: OrderBookSnapshot,
}


@dataclass(frozen=True)
class MarketDataEvent(Generic[TPayload]):
    """Provider katmanından çıkan tek bir olayın zarfı.

    Runtime invariant'lar (Quality Gate 3):
    - `event_type` ile `payload`'ın gerçek tipi eşleşmelidir
      (örn. EventType.TRADE + Candle payload REDDEDİLİR).
    - `symbol` ile `payload.symbol` (normalize edilmiş haliyle) eşleşmelidir.
    """

    event_type: EventType
    symbol: str
    sequence_id: int
    received_at: datetime
    payload: TPayload

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.received_at, "received_at")
        require_enum(self.event_type, EventType, "event_type")
        if self.sequence_id < 0:
            raise ValueError("sequence_id negatif olamaz")

        expected_type = _EXPECTED_PAYLOAD_TYPE[self.event_type]
        if not isinstance(self.payload, expected_type):
            raise TypeError(
                f"event_type={self.event_type} için payload {expected_type.__name__} "
                f"olmalı, alınan: {type(self.payload).__name__}"
            )

        if self.payload.symbol != self.symbol:
            raise ValueError(
                f"event.symbol ({self.symbol}) payload.symbol ({self.payload.symbol}) "
                f"ile eşleşmiyor"
            )


@dataclass(frozen=True)
class SequenceCursor:
    """Bir (symbol, event_type) çifti için son görülen sequence durumu.

    Trade (trade_id) ve order book (last_update_id) gibi DOĞAL, tekil
    monotonik kimliği olan akışlar için kullanılır. Candle update
    sıralaması için candle_sequencing.CandleSequencer kullanılır
    (bkz. Quality Gate 2 — bu ayrım kasıtlıdır).
    """

    symbol: str
    event_type: EventType
    last_sequence_id: int
    last_received_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.event_type, EventType, "event_type")
        if self.last_sequence_id < 0:
            raise ValueError("last_sequence_id negatif olamaz")
        require_utc_aware(self.last_received_at, "last_received_at")

    def is_stale(self, now: datetime, max_age_seconds: float) -> bool:
        """`now` ile son alınan veri arasındaki fark eşik üstündeyse True.

        Boundary: age == max_age_seconds durumu STALE DEĞİLDİR (yalnızca
        kesin olarak eşiği AŞAN yaş stale sayılır — `>`, `>=` değil).
        """
        require_utc_aware(now, "now")
        if max_age_seconds < 0:
            raise ValueError("max_age_seconds negatif olamaz")
        age = (now - self.last_received_at).total_seconds()
        return age > max_age_seconds

    def next_is_valid(self, candidate_sequence_id: int) -> bool:
        """Yeni gelen sequence_id'nin kabul edilebilir olup olmadığını kontrol eder."""
        return candidate_sequence_id > self.last_sequence_id


=== FILE: crypto_signal_engine/domain/models.py ===
"""
Domain modelleri.

Kural: Bu modüldeki hiçbir dataclass Binance'e, CCXT'ye veya herhangi bir
dış kütüphaneye bağımlı değildir. Provider katmanı ham Binance verisini
buradaki modellere çevirmekle yükümlüdür.

Zaman kuralı: UTC-aware datetime only (bkz. _validation.require_utc_aware).
Sayı kuralı: NaN/inf domain'e giremez (bkz. _validation.require_finite).
Symbol kuralı: normalize_symbol ile canonical forma çevrilir.
Immutability kuralı: nested mutable mapping'ler gerçek read-only view'a
sarılır (bkz. _validation.freeze_mapping) — yalnızca frozen=True yeterli
DEĞİLDİR.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from crypto_signal_engine.domain._validation import (
    freeze_mapping,
    normalize_symbol,
    require_enum,
    require_finite,
    require_utc_aware,
)
from crypto_signal_engine.domain.enums import (
    AgentName,
    RiskLevel,
    SignalDirection,
    Timeframe,
)


@dataclass(frozen=True)
class CandleIdentity:
    """Bir candle'ın DEĞİŞMEZ kimliği: (symbol, timeframe, open_time).

    HARDENING NOTU (Quality Gate 2): close_time veya herhangi bir "sequence"
    değeri identity'nin parçası DEĞİLDİR. Aynı identity için birden çok
    update mesajı gelebilir (candle kapanmadan önce defalarca güncellenir);
    bunların hepsi AYNI identity'e ait FARKLI update'lerdir. Identity ile
    update sıralaması (bkz. candle_sequencing.py) kasıtlı olarak ayrı
    kavramlardır.
    """

    symbol: str
    timeframe: Timeframe
    open_time: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")
        require_utc_aware(self.open_time, "open_time")


@dataclass(frozen=True)
class Candle:
    """Tek bir OHLCV mumu (belirli bir update anındaki hali).

    Immutable: bir Candle nesnesi üretildikten sonra mutate edilmez.
    Aynı candle identity için yeni bir update geldiğinde YENİ bir Candle
    nesnesi oluşturulur; sıralama/kabul mantığı candle_sequencing.py'de
    ele alınır, bu sınıf yalnızca tek bir anlık durumu (snapshot) temsil eder.
    """

    symbol: str
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool
    trade_count: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")

        require_utc_aware(self.open_time, "open_time")
        require_utc_aware(self.close_time, "close_time")
        if self.close_time <= self.open_time:
            raise ValueError("close_time, open_time'dan sonra olmalı")

        require_finite(self.open, "open")
        require_finite(self.high, "high")
        require_finite(self.low, "low")
        require_finite(self.close, "close")
        require_finite(self.volume, "volume")

        if self.high < self.low:
            raise ValueError("high, low'dan küçük olamaz (impossible OHLC)")
        if not (self.low <= self.open <= self.high):
            raise ValueError("open, [low, high] aralığında olmalı")
        if not (self.low <= self.close <= self.high):
            raise ValueError("close, [low, high] aralığında olmalı")
        if self.volume < 0:
            raise ValueError("volume negatif olamaz")
        if self.trade_count is not None and self.trade_count < 0:
            raise ValueError("trade_count negatif olamaz")

    @property
    def identity(self) -> CandleIdentity:
        """Bu candle'ın (symbol, timeframe, open_time) kimliği."""
        return CandleIdentity(symbol=self.symbol, timeframe=self.timeframe, open_time=self.open_time)


@dataclass(frozen=True)
class Trade:
    """Tek bir gerçekleşmiş işlem (aggTrade)."""

    symbol: str
    trade_id: int
    price: float
    quantity: float
    timestamp: datetime
    is_buyer_maker: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.timestamp, "timestamp")
        require_finite(self.price, "price")
        require_finite(self.quantity, "quantity")
        if self.price <= 0:
            raise ValueError("price pozitif olmalı")
        if self.quantity <= 0:
            raise ValueError("quantity pozitif olmalı")
        if self.trade_id < 0:
            raise ValueError("trade_id negatif olamaz")


@dataclass(frozen=True)
class OrderBookLevel:
    """Order book'ta tek bir fiyat seviyesi."""

    price: float
    quantity: float

    def __post_init__(self) -> None:
        require_finite(self.price, "price")
        require_finite(self.quantity, "quantity")
        if self.price <= 0:
            raise ValueError("price pozitif olmalı")
        if self.quantity < 0:
            raise ValueError("quantity negatif olamaz")


@dataclass(frozen=True)
class OrderBookSnapshot:
    """Belirli bir andaki order book durumu (top-N derinlik).

    HARDENING NOTU (Quality Gate 7): bids KESİNLİKLE fiyata göre azalan
    (non-increasing), asks KESİNLİKLE fiyata göre artan (non-decreasing)
    sırada olmalıdır; her side içinde duplicate fiyat seviyesi YASAKTIR;
    "locked book" (best_bid == best_ask) da crossed kadar geçersizdir.
    """

    symbol: str
    timestamp: datetime
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    last_update_id: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.timestamp, "timestamp")

        if not isinstance(self.bids, tuple):
            object.__setattr__(self, "bids", tuple(self.bids))
        if not isinstance(self.asks, tuple):
            object.__setattr__(self, "asks", tuple(self.asks))

        if not self.bids:
            raise ValueError("bids boş olamaz")
        if not self.asks:
            raise ValueError("asks boş olamaz")

        if self.last_update_id < 0:
            raise ValueError("last_update_id negatif olamaz")

        _require_sorted_unique(self.bids, descending=True, side_name="bids")
        _require_sorted_unique(self.asks, descending=False, side_name="asks")

        if self.bids[0].price >= self.asks[0].price:
            raise ValueError(
                "best bid, best ask'tan KESİNLİKLE küçük olmalı "
                "(crossed veya locked book kabul edilmez)"
            )

    @property
    def best_bid(self) -> OrderBookLevel:
        return self.bids[0]

    @property
    def best_ask(self) -> OrderBookLevel:
        return self.asks[0]

    @property
    def mid_price(self) -> float:
        return (self.best_bid.price + self.best_ask.price) / 2

    @property
    def spread(self) -> float:
        return self.best_ask.price - self.best_bid.price


def _require_sorted_unique(levels: tuple[OrderBookLevel, ...], *, descending: bool, side_name: str) -> None:
    """Bir order book side'ının monotonik sıralı ve duplicate-free olduğunu doğrular.

    Duplicate kontrolü, sıralama kontrolünden ÖNCE ve AYRI bir hata mesajıyla
    yapılır — böylece "duplicate fiyat seviyesi" ile "yanlış sıralı" durumları
    çağıran kod için ayırt edilebilir kalır.
    """
    seen_prices: set[float] = set()
    for level in levels:
        if level.price in seen_prices:
            raise ValueError(f"{side_name} içinde duplicate fiyat seviyesi: {level.price}")
        seen_prices.add(level.price)

    for i in range(len(levels) - 1):
        current_price = levels[i].price
        next_price = levels[i + 1].price
        if descending:
            if next_price >= current_price:
                raise ValueError(
                    f"{side_name} fiyata göre azalan (best->worst) sırada olmalı; "
                    f"{current_price} sonrası {next_price} geldi"
                )
        else:
            if next_price <= current_price:
                raise ValueError(
                    f"{side_name} fiyata göre artan (best->worst) sırada olmalı; "
                    f"{current_price} sonrası {next_price} geldi"
                )


@dataclass(frozen=True)
class FeatureVector:
    """Tek bir symbol + timeframe için hesaplanmış feature seti.

    `values` gerçek immutable bir mapping'e (MappingProxyType üzerinde
    defensive copy) sarılır (Quality Gate 4).
    """

    symbol: str
    timeframe: Timeframe
    timestamp: datetime
    values: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")
        require_utc_aware(self.timestamp, "timestamp")

        for name, value in self.values.items():
            require_finite(value, f"feature '{name}'")

        object.__setattr__(self, "values", freeze_mapping(self.values, "values"))


@dataclass(frozen=True)
class AgentEvidence:
    """Bir agent'ın (veya RiskOverlay'in) ürettiği tek bir kanıt/skor.

    HARDENING NOTU (Quality Gate 11 — provenance): artık hangi market
    context üzerinden üretildiğini taşır: `symbol`, `as_of`, `context_id`.

    `context_id` deterministik gruplama anahtarıdır: aynı feature
    snapshot'tan üretilen tüm evidence'lar AYNI context_id'yi paylaşır.
    Bu, "as_of zaman damgaları birbirine ne kadar yakın olmalı" gibi
    kırılgan bir tolerance tasarımı gerektirmez — ConsensusEngine yalnızca
    context_id eşitliğini kontrol eder (bkz. DECISIONS.md). `as_of` alanı
    yalnızca gözlemlenebilirlik/debug amaçlıdır, consensus gruplamasında
    KULLANILMAZ.
    """

    agent: AgentName
    score: float  # [-1.0, 1.0]
    rationale: str
    primary_timeframe: Timeframe
    symbol: str
    as_of: datetime
    context_id: str
    supporting_metrics: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.agent, AgentName, "agent")
        require_enum(self.primary_timeframe, Timeframe, "primary_timeframe")
        require_utc_aware(self.as_of, "as_of")

        require_finite(self.score, "score")
        if not -1.0 <= self.score <= 1.0:
            raise ValueError(f"score [-1.0, 1.0] aralığında olmalı: {self.score}")

        if not self.rationale.strip():
            raise ValueError("rationale boş olamaz (explainability zorunlu)")

        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz (provenance zorunlu)")

        for name, value in self.supporting_metrics.items():
            require_finite(value, f"supporting_metrics['{name}']")

        object.__setattr__(
            self, "supporting_metrics", freeze_mapping(self.supporting_metrics, "supporting_metrics")
        )


@dataclass(frozen=True)
class Signal:
    """Nihai, kullanıcıya sunulan sinyal.

    HARDENING NOTU (Quality Gate 5 — single source of truth): `direction`
    artık AYRI bir constructor alanı DEĞİLDİR. `score` canonical
    source-of-truth'tur; `direction` bundan türetilen bir property'dir.

    HARDENING NOTU (Quality Gate 26 — final signal provenance): Signal artık
    canonical bir `context_id` taşır. `supporting_factors` ve
    `contradicting_factors` içindeki TÜM AgentEvidence nesneleri:
    - evidence.symbol == signal.symbol
    - evidence.context_id == signal.context_id
    - evidence.as_of <= signal.timestamp (gelecekten evidence kabul edilmez)
    olmak ZORUNDADIR. Ayrıca:
    - Aynı AgentEvidence nesnesi (değer eşitliği ile) hem supporting hem
      contradicting listesinde bulunamaz.
    - GLOBAL one-agent/one-evidence politikası: aynı AgentName, supporting +
      contradicting birleşiminde İKİ KEZ geçemez (bkz. DECISIONS.md Karar 15
      — bu, ConsensusResult'taki per-context kuralının Signal seviyesinde
      daha da sıkılaştırılmış hâlidir).
    """

    symbol: str
    timestamp: datetime
    context_id: str
    score: float
    confidence: float
    risk_level: RiskLevel
    primary_timeframe: Timeframe
    supporting_factors: tuple[AgentEvidence, ...]
    contradicting_factors: tuple[AgentEvidence, ...]
    invalidation: str | None
    model_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.timestamp, "timestamp")

        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz (provenance zorunlu)")

        require_finite(self.score, "score")
        if not -1.0 <= self.score <= 1.0:
            raise ValueError(f"score [-1.0, 1.0] aralığında olmalı: {self.score}")

        require_finite(self.confidence, "confidence")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence [0.0, 1.0] aralığında olmalı: {self.confidence}")

        require_enum(self.risk_level, RiskLevel, "risk_level")
        require_enum(self.primary_timeframe, Timeframe, "primary_timeframe")

        if not self.model_version.strip():
            raise ValueError(
                "model_version boş olamaz (hangi mantıkla üretildiği izlenebilir olmalı)"
            )

        if not isinstance(self.supporting_factors, tuple):
            object.__setattr__(self, "supporting_factors", tuple(self.supporting_factors))
        if not isinstance(self.contradicting_factors, tuple):
            object.__setattr__(self, "contradicting_factors", tuple(self.contradicting_factors))

        self._validate_evidence_provenance()

    def _validate_evidence_provenance(self) -> None:
        all_evidence = (*self.supporting_factors, *self.contradicting_factors)

        for evidence in all_evidence:
            if evidence.symbol != self.symbol:
                raise ValueError(
                    f"mixed-symbol signal evidence reddedildi: evidence.symbol={evidence.symbol} "
                    f"!= signal.symbol={self.symbol}"
                )
            if evidence.context_id != self.context_id:
                raise ValueError(
                    f"mixed-context signal evidence reddedildi: evidence.context_id="
                    f"{evidence.context_id} != signal.context_id={self.context_id}"
                )
            if evidence.as_of > self.timestamp:
                raise ValueError(
                    f"evidence.as_of ({evidence.as_of}) signal.timestamp ({self.timestamp}) "
                    f"içinden GELECEKTEN olamaz"
                )

        # Aynı evidence nesnesinin (değer eşitliğiyle) hem supporting hem
        # contradicting listesinde bulunması yasak. AgentEvidence, immutable
        # mapping alanı (MappingProxyType) taşıdığı için hash'lenemez;
        # bu yüzden set() yerine eşitlik bazlı `in` kontrolü kullanılır.
        overlap = [e for e in self.supporting_factors if e in self.contradicting_factors]
        if overlap:
            raise ValueError(
                f"aynı evidence hem supporting hem contradicting'te bulunamaz: {overlap}"
            )

        # Global one-agent/one-evidence politikası: aynı AgentName,
        # supporting+contradicting birleşiminde birden fazla kez geçemez.
        agent_counts: dict[AgentName, int] = {}
        for evidence in all_evidence:
            agent_counts[evidence.agent] = agent_counts.get(evidence.agent, 0) + 1
        duplicates = [agent for agent, count in agent_counts.items() if count > 1]
        if duplicates:
            raise ValueError(
                f"duplicate agent evidence reddedildi (global one-agent/one-evidence "
                f"politikası): {duplicates}"
            )

    @property
    def direction(self) -> SignalDirection:
        """score'dan türetilen sinyal yönü. TEK kaynak-of-truth: score."""
        return SignalDirection.from_score(self.score)


=== FILE: crypto_signal_engine/domain/state_contract.py ===
"""
Data quality -> state commit boundary.

HARDENING NOTU (Quality Gate 10): eski dokümantasyondaki "Provider ->
DataQualityGate -> StateManager" akışı ile "StateManager duplicate/out-of-
order kontrolü yapar" anlatımı ÇELİŞİYORDU. Bu modül tek, tutarlı akışı
kod sözleşmesi seviyesinde sabitler:

    Provider
       |
       v
    CandleUpdate (candle_sequencing.py)
       |
       v
    DataQualityGate.check_candle(...)   <- kalite kontrolü ÖNCE yapılır
       |
       v
    CandleStateStore.commit(quality_result, update)
       |
       +-- quality_result.passed == False -> REJECTED_QUALITY
       |      (canonical state DEĞİŞMEZ, sequencer'a hiç ulaşmaz)
       |
       +-- quality_result.passed == True  -> CandleSequencer.apply(update)
              (sequence/duplicate/out-of-order kontrolü BURADA yapılır)
       |
       v
    FeatureEngine  (yalnızca COMMITTED sonucu görür)

Yani: DataQualityGate tarafından reddedilen veri, sequence kontrolüne
ULAŞMADAN önce reddedilir; canonical state'i hiçbir şekilde etkilemez.
Bu invariant `InMemoryCandleStateStore.commit()` içinde kod olarak
uygulanır ve `tests/test_state_contract.py` ile kanıtlanır.

Önceki (peek -> validate -> commit) mantığı yerine (validate -> commit)
sırası tercih edilmiştir çünkü validate işlemi (DataQualityGate) zaten
`previous` parametresi ile mevcut canonical state'i salt-okunur biçimde
görebilir (bkz. CandleStateStore.peek_previous) — ayrı bir "peek" adımına
gerek yoktur, tek adımlık transactional commit yeterlidir.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from crypto_signal_engine.domain.candle_sequencing import (
    CandleSequencer,
    CandleUpdate,
    CandleUpdateOutcome,
)
from crypto_signal_engine.domain.models import Candle, CandleIdentity
from crypto_signal_engine.quality.base import DataQualityResult


class CommitOutcome(str, Enum):
    COMMITTED = "COMMITTED"
    REJECTED_QUALITY = "REJECTED_QUALITY"
    REJECTED_SEQUENCE = "REJECTED_SEQUENCE"


@dataclass(frozen=True)
class CommitResult:
    outcome: CommitOutcome
    canonical_candle: Candle | None
    detail: str

    @property
    def committed(self) -> bool:
        return self.outcome == CommitOutcome.COMMITTED


class CandleStateStore(ABC):
    """StateManager'ın candle canonical state sözleşmesi.

    Bu ABC, gerçek bir persistence implementasyonu (SQLite/DBWriter) DEĞİLDİR
    — Faz 2 kapsamıdır. Burada yalnızca invariant'ın kod seviyesinde
    garanti edilmesi gereken sınır tanımlanır.
    """

    @abstractmethod
    def peek_previous(self, identity: CandleIdentity) -> Candle | None:
        """Read-only: bir identity için mevcut canonical candle'ı döndürür."""
        raise NotImplementedError

    @abstractmethod
    def commit(self, quality_result: DataQualityResult, update: CandleUpdate) -> CommitResult:
        """Kalite kontrolünden geçmiş bir update'i canonical state'e yazmayı dener.

        `quality_result.passed is False` ise bu metod update'i sequencer'a
        HİÇ ULAŞTIRMADAN reddetmelidir.
        """
        raise NotImplementedError


class InMemoryCandleStateStore(CandleStateStore):
    """CandleStateStore'un referans (in-memory) implementasyonu.

    Faz 2'de gerçek DBWriter/SQLite implementasyonu bu ABC'yi implemente
    edecektir; bu sınıf yalnızca invariant'ı test edilebilir kılmak içindir.
    """

    def __init__(self) -> None:
        self._sequencer = CandleSequencer()

    def peek_previous(self, identity: CandleIdentity) -> Candle | None:
        return self._sequencer.peek(identity)

    def commit(self, quality_result: DataQualityResult, update: CandleUpdate) -> CommitResult:
        if not quality_result.passed:
            # KRİTİK INVARIANT: reddedilen veri sequencer'a asla ulaşmaz,
            # canonical state hiçbir şekilde değişmez.
            return CommitResult(
                outcome=CommitOutcome.REJECTED_QUALITY,
                canonical_candle=self._sequencer.peek(update.identity),
                detail=f"data quality reddetti: {quality_result.status} — {quality_result.reason}",
            )

        result = self._sequencer.apply(update)
        if result.accepted:
            return CommitResult(
                outcome=CommitOutcome.COMMITTED,
                canonical_candle=result.canonical_candle,
                detail=f"commit edildi: {result.outcome}",
            )
        return CommitResult(
            outcome=CommitOutcome.REJECTED_SEQUENCE,
            canonical_candle=result.canonical_candle,
            detail=f"sequence reddetti: {result.outcome}",
        )


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


=== FILE: crypto_signal_engine/execution/__init__.py ===
"""
Faz 10 — Binance Spot TESTNET Execution Lab.

Bu paket, sistemin İLK private/signed Binance execution sınırını sunar —
KESİNLİKLE ve YALNIZCA Binance Spot TESTNET'e karşı
(`https://testnet.binance.vision`). Bu bir üretim/execution fazı DEĞİLDİR;
"bir intent'in güvenli şekilde TESTNET'e çevrilip/imzalanıp/gönderilip/
gözlemlenebildiğini" kanıtlayan izole bir LAB'dır.

HARD SAFETY INVARIANT (değişmez, bkz. SAFETY_INVARIANTS.md):
- `ALLOW_LIVE_TRADING = False` (crypto_signal_engine/__init__.py) KORUNUR
  ve bu paket tarafından HİÇBİR ŞEKİLDE değiştirilmez/etkisizleştirilmez.
- Mainnet execution bu paket üzerinden YAPISAL OLARAK İMKANSIZDIR: private
  istemci, sabit-kodlanmış bir host allowlist'i (`testnet.binance.vision`,
  yalnızca `https`) dışında HERHANGİ bir host ile inşa/kullanım
  REDDEDİLİR (bkz. `testnet_client.py::_validate_testnet_host`) — bu
  kontrol substring DEĞİL, tam `urlsplit().hostname` eşleşmesi kullanır.
- Kimlik bilgileri YALNIZCA ortam değişkenlerinden okunur; hiçbir zaman
  loglanmaz, exception mesajlarına/sonuç modellerine/SQLite'a/dokümana
  YAZILMAZ.
- Futures/margin/withdrawal/transfer/broker endpoint'i YOKTUR ve
  OLAMAZ — yalnızca Spot `account`/`exchangeInfo`/`order` (TESTNET).
- Normal PUBLIC runtime (Faz 6/8) + `PaperTradingEngine` (Faz 5) bu
  paketten TAMAMEN BAĞIMSIZDIR ve HİÇBİR sinyal otomatik olarak bu
  pakete yönlendirilmez — yalnızca `scripts/binance_testnet_lab.py`
  üzerinden KASITLI, MANUEL çağrı ile kullanılır.

Modüller:
- `models.py` — `ExecutionMode`, `OrderSide`, `OrderType`, `TimeInForce`,
  `OrderIntent` (deterministik `client_order_id` dahil), `ExecutionResult`.
- `errors.py` — Faz 10 exception taksonomisi (`ExecutionError` alt
  sınıfları, hiçbiri secret/signature İÇERMEZ).
- `signer.py` — HMAC-SHA256 imzalama + kanonik parametre kodlama.
- `testnet_client.py` — TESTNET-only HTTP istemcisi (host allowlist,
  sinyalli GET/POST, hata çevirisi).
- `adapter.py` — `TestnetExecutionAdapter`: intent doğrulama (exchange
  filter'ları) + imzalama + gönderim + sonuç modeli üretimi."""

from __future__ import annotations


=== FILE: crypto_signal_engine/execution/adapter.py ===
"""
Faz 10 — `TestnetExecutionAdapter`: intent doğrulama (exchange filtreleri
+ GEREKİYORSA güncel bir PUBLIC fiyat) + imzalama/gönderim (testnet_client'a
delege) + sonuç üretimi.

BLOCKER FİX (Karar 73 — bağımsız acceptance review bulgusu): bir MARKET
order için `notional = quantity * price` hesaplanabilmesi için bir FİYAT
TABANI gerekir, ama `OrderIntent.price` MARKET order'lar için HER ZAMAN
`None`'dur (bkz. `models.py` — MARKET order price KABUL ETMEZ). Eski
implementasyon bu yüzden MARKET order'lar için MIN_NOTIONAL/NOTIONAL
kontrolünü SESSİZCE hiç ÇALIŞTIRMIYORDU (`intent.price is not None`
koşulu MARKET için asla sağlanmıyordu). Fix: bir MARKET order + base
`quantity` + UYGULANABİLİR (applyMinToMarket/applyMaxToMarket/applyToMarket
= True) bir notional filtresi varsa, gönderim ÖNCESİ GÜNCEL bir TESTNET
PUBLIC fiyatı (`BinanceTestnetClient.symbol_price()`) çekilir ve notional
TABANI olarak kullanılır — asla İCAT EDİLMEZ, asla LIMIT price'ı ÖDÜNÇ
ALINMAZ, fiyat lookup'ı BAŞARISIZ olursa order KESİNLİKLE GÖNDERİLMEZ
(fail-closed). Bu, Binance'in KENDİ execution price'ını/`avgPriceMins`
penceresini TAHMİN ETMEZ — yalnızca ANLIK bir pre-submission filtre-
doğrulama TABANIDIR, bir fill-price GARANTİSİ DEĞİLDİR (bkz.
PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md).

Bu, `scripts/binance_testnet_lab.py`'nin kullandığı TEK yüksek-seviye
giriş noktasıdır. Hiçbir normal runtime bileşeni (Faz 4/5/6/8) bunu
OTOMATİK ÇAĞIRMAZ — yalnızca KASITLI, MANUEL bir CLI çağrısı üzerinden
kullanılır (bkz. modül-üstü `execution/__init__.py` docstring'i)."""

from __future__ import annotations

from dataclasses import dataclass

from crypto_signal_engine.domain._validation import normalize_symbol, require_finite
from crypto_signal_engine.execution.errors import FilterValidationError
from crypto_signal_engine.execution.models import ExecutionResult, OrderIntent, OrderType
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient

_STEP_ALIGNMENT_TOLERANCE = 1e-8


@dataclass(frozen=True)
class SymbolFilters:
    """Binance `exchangeInfo`'dan ayrıştırılmış, bir order intent'ini
    GÖNDERMEDEN ÖNCE doğrulamak için gereken filtre kümesi.

    `market_*` alanları yalnızca sembolün AYRI bir `MARKET_LOT_SIZE`
    filtresi VARSA doludur — Binance, MARKET order quantity sınırlarının
    `LOT_SIZE`'dan FARKLI olabileceğini bu ayrı filtreyle belirtir; VARSA
    MARKET order'lar için `LOT_SIZE` YERİNE bu kullanılır (bkz.
    `validate_intent_against_filters`).

    `min_notional`/`max_notional` ve `*_applies_to_market` bayrakları,
    Binance'in İKİ olası notional filtresini (eski `MIN_NOTIONAL` — tek
    `applyToMarket` bayrağı; yeni `NOTIONAL` — ayrı `applyMinToMarket`/
    `applyMaxToMarket` bayrakları) TEK bir normalize edilmiş şekle
    indirger. `NOTIONAL` VARSA `MIN_NOTIONAL`'a TERCİH edilir (Binance'in
    kendi migration politikasıyla AYNI — `NOTIONAL`, `MIN_NOTIONAL`'ı
    DEĞİŞTİRİR)."""

    symbol: str
    status: str
    step_size: float
    min_qty: float
    max_qty: float
    market_step_size: float | None
    market_min_qty: float | None
    market_max_qty: float | None
    tick_size: float | None
    min_price: float | None
    max_price: float | None
    min_notional: float | None
    max_notional: float | None
    min_notional_applies_to_market: bool
    max_notional_applies_to_market: bool


def parse_symbol_filters(exchange_info_payload: dict, symbol: str) -> SymbolFilters:
    normalized = normalize_symbol(symbol)
    symbols = exchange_info_payload.get("symbols", [])
    match = next((s for s in symbols if s.get("symbol") == normalized), None)
    if match is None:
        raise FilterValidationError(f"{normalized}, exchangeInfo yanıtında bulunamadı")

    status = match.get("status", "")
    filters_by_type = {f.get("filterType"): f for f in match.get("filters", [])}
    lot_size = filters_by_type.get("LOT_SIZE")
    if lot_size is None:
        raise FilterValidationError(f"{normalized} için LOT_SIZE filtresi exchangeInfo'da bulunamadı")

    market_lot_size = filters_by_type.get("MARKET_LOT_SIZE")
    price_filter = filters_by_type.get("PRICE_FILTER")
    notional_filter = filters_by_type.get("NOTIONAL")
    min_notional_filter = filters_by_type.get("MIN_NOTIONAL")

    try:
        min_notional: float | None = None
        max_notional: float | None = None
        min_applies_to_market = False
        max_applies_to_market = False

        if notional_filter is not None:
            # Yeni `NOTIONAL` filtresi — `MIN_NOTIONAL`'ı DEĞİŞTİRİR, VARSA
            # tercih edilir. `applyMinToMarket`/`applyMaxToMarket`,
            # doğrudan anahtar erişimiyle (`.get(..., False)` DEĞİL) okunur
            # — gerçek Binance yanıtı bu alanları HER ZAMAN içerir; EKSİKSE
            # bu, sessizce varsayılan bir davranışa DÜŞMEK yerine (fail-
            # closed) bir `FilterValidationError` (aşağıdaki `except` ile)
            # üretir.
            if "minNotional" in notional_filter:
                min_notional = float(notional_filter["minNotional"])
                min_applies_to_market = bool(notional_filter["applyMinToMarket"])
            if "maxNotional" in notional_filter:
                max_notional = float(notional_filter["maxNotional"])
                max_applies_to_market = bool(notional_filter["applyMaxToMarket"])
        elif min_notional_filter is not None:
            # Eski `MIN_NOTIONAL` filtresi — yalnızca minimum + tek bir
            # `applyToMarket` bayrağı taşır.
            min_notional = float(min_notional_filter["minNotional"])
            min_applies_to_market = bool(min_notional_filter["applyToMarket"])

        return SymbolFilters(
            symbol=normalized,
            status=status,
            step_size=float(lot_size["stepSize"]),
            min_qty=float(lot_size["minQty"]),
            max_qty=float(lot_size["maxQty"]),
            market_step_size=float(market_lot_size["stepSize"]) if market_lot_size is not None else None,
            market_min_qty=float(market_lot_size["minQty"]) if market_lot_size is not None else None,
            market_max_qty=float(market_lot_size["maxQty"]) if market_lot_size is not None else None,
            tick_size=float(price_filter["tickSize"]) if price_filter is not None else None,
            min_price=float(price_filter["minPrice"]) if price_filter is not None else None,
            max_price=float(price_filter["maxPrice"]) if price_filter is not None else None,
            min_notional=min_notional,
            max_notional=max_notional,
            min_notional_applies_to_market=min_applies_to_market,
            max_notional_applies_to_market=max_applies_to_market,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise FilterValidationError(f"{normalized} exchangeInfo filtreleri ayrıştırılamadı: {exc}") from exc


def _is_step_aligned(value: float, step: float, floor: float) -> bool:
    if step <= 0:
        return True
    steps = (value - floor) / step
    return abs(steps - round(steps)) < _STEP_ALIGNMENT_TOLERANCE


def determine_market_price_requirement(intent: OrderIntent, filters: SymbolFilters) -> bool:
    """Bir MARKET order için GÜNCEL bir PUBLIC fiyatın GEREKİP
    GEREKMEDİĞİNİ belirler:

    - Yalnızca `order_type is MARKET` için ANLAMLIDIR (LIMIT her zaman
      `intent.price`'ı kullanır, fiyat lookup'ı ASLA gerekmez).
    - `intent.quote_quantity` VERİLMİŞSE fiyat GEREKMEZ — quoteOrderQty'nin
      KENDİSİ zaten notional TABANIDIR (bkz. Bölüm 3, `validate_intent_against_filters`).
    - `intent.quantity` (base) VERİLMİŞSE, fiyat YALNIZCA en az bir
      UYGULANABİLİR (min VEYA max, `*_applies_to_market=True`) notional
      filtresi VARSA gerekir — Binance "bu filtre MARKET'e uygulanmaz"
      dediğinde (`applyToMarket=False`), fiyat KÖRÜ KÖRÜNE ÇEKİLMEZ."""
    if intent.order_type is not OrderType.MARKET:
        return False
    if intent.quantity is None:
        return False
    applicable_min = filters.min_notional is not None and filters.min_notional_applies_to_market
    applicable_max = filters.max_notional is not None and filters.max_notional_applies_to_market
    return applicable_min or applicable_max


def _check_notional(notional: float, filters: SymbolFilters, *, symbol: str, applies_min: bool, applies_max: bool) -> None:
    if applies_min and filters.min_notional is not None and notional < filters.min_notional:
        raise FilterValidationError(f"{symbol} notional {notional} minimum {filters.min_notional} altında")
    if applies_max and filters.max_notional is not None and notional > filters.max_notional:
        raise FilterValidationError(f"{symbol} notional {notional} maksimum {filters.max_notional} üzerinde")


def validate_intent_against_filters(
    intent: OrderIntent, filters: SymbolFilters, *, market_price: float | None = None
) -> None:
    """Intent'i GÖNDERMEDEN ÖNCE Binance'in filtrelerine karşı doğrular.
    Hiçbir değer SESSİZCE farklı bir ekonomik anlama YUVARLANMAZ — ihlal
    varsa AÇIKÇA `FilterValidationError` fırlatılır.

    `market_price`: yalnızca `determine_market_price_requirement()` TRUE
    döndüğünde ZORUNLUDUR (çağıran — `TestnetExecutionAdapter.submit()` —
    bunu ÖNCEDEN çeker); GEREKLİYKEN `None` ise bu fonksiyon AÇIKÇA
    reddeder (fiyatı İCAT ETMEZ, kontrolü ATLAMAZ)."""
    if filters.status != "TRADING":
        raise FilterValidationError(
            f"{intent.symbol} işlem durumu TRADING değil (mevcut: {filters.status!r}) — order GÖNDERİLEMEZ"
        )

    # -- Quantity (LOT_SIZE veya, MARKET + MARKET_LOT_SIZE varsa, o) -----------
    if intent.quantity is not None:
        require_finite(intent.quantity, "quantity")
        use_market_lot_size = intent.order_type is OrderType.MARKET and filters.market_step_size is not None
        if use_market_lot_size:
            step, floor, ceiling, filter_name = (
                filters.market_step_size, filters.market_min_qty, filters.market_max_qty, "MARKET_LOT_SIZE",
            )
        else:
            step, floor, ceiling, filter_name = filters.step_size, filters.min_qty, filters.max_qty, "LOT_SIZE"
        if not (floor <= intent.quantity <= ceiling):
            raise FilterValidationError(
                f"{intent.symbol} quantity {intent.quantity} {filter_name} aralığı dışında [{floor}, {ceiling}]"
            )
        if not _is_step_aligned(intent.quantity, step, floor):
            raise FilterValidationError(
                f"{intent.symbol} quantity {intent.quantity}, {filter_name} stepSize {step}'e hizalı değil"
            )

    # -- Price (yalnızca LIMIT — MARKET için intent.price her zaman None) ------
    if intent.price is not None:
        if filters.tick_size is not None and filters.min_price is not None and filters.max_price is not None:
            if not (filters.min_price <= intent.price <= filters.max_price):
                raise FilterValidationError(
                    f"{intent.symbol} price {intent.price} PRICE_FILTER aralığı dışında "
                    f"[{filters.min_price}, {filters.max_price}]"
                )
            if not _is_step_aligned(intent.price, filters.tick_size, filters.min_price):
                raise FilterValidationError(
                    f"{intent.symbol} price {intent.price}, tickSize {filters.tick_size}'e hizalı değil"
                )

    # -- Notional ---------------------------------------------------------------
    if intent.order_type is OrderType.LIMIT:
        # LIMIT için notional filtreleri HER ZAMAN uygulanır (applyToMarket/
        # applyMinToMarket/applyMaxToMarket YALNIZCA MARKET'i etkiler).
        if intent.price is not None and intent.quantity is not None:
            notional = intent.quantity * intent.price
            _check_notional(notional, filters, symbol=intent.symbol, applies_min=True, applies_max=True)
    else:  # MARKET
        if intent.quote_quantity is not None:
            # quoteOrderQty'nin KENDİSİ zaten notional TABANIDIR — gereksiz
            # bir fiyat çarpımı YAPILMAZ (Bölüm 3, blocker fix gereksinimi).
            _check_notional(
                intent.quote_quantity, filters, symbol=intent.symbol,
                applies_min=filters.min_notional_applies_to_market,
                applies_max=filters.max_notional_applies_to_market,
            )
        elif intent.quantity is not None and determine_market_price_requirement(intent, filters):
            if market_price is None:
                raise FilterValidationError(
                    f"{intent.symbol} MARKET order notional doğrulaması GÜNCEL bir PUBLIC fiyat GEREKTİRİYOR "
                    f"ama sağlanmadı — order GÖNDERİLEMEZ (fail-closed)"
                )
            require_finite(market_price, "market_price")
            if market_price <= 0:
                raise FilterValidationError(f"{intent.symbol} için geçersiz market_price: {market_price}")
            notional = intent.quantity * market_price
            _check_notional(
                notional, filters, symbol=intent.symbol,
                applies_min=filters.min_notional_applies_to_market,
                applies_max=filters.max_notional_applies_to_market,
            )


class TestnetExecutionAdapter:
    """Faz 10'un tek yüksek-seviye giriş noktası: `validate_symbol()` +
    `validate_intent()` + `submit()`. `SignalEngine`/`RuntimeCoordinator`/
    `PaperTradingEngine` bu sınıftan HABERDAR DEĞİLDİR — yalnızca
    `scripts/binance_testnet_lab.py` tarafından KASITLI olarak çağrılır."""

    __test__ = False  # pytest'in sınıf adındaki "Testnet" önekinden dolayı bunu bir test sınıfı sanmasını önler

    def __init__(self, client: BinanceTestnetClient) -> None:
        self._client = client

    async def validate_symbol(self, symbol: str) -> SymbolFilters:
        payload = await self._client.exchange_info(symbol)
        return parse_symbol_filters(payload, symbol)

    async def validate_intent(self, intent: OrderIntent) -> tuple[SymbolFilters, float | None]:
        """Filtreleri çeker, GEREKİYORSA (bkz. `determine_market_price_requirement`)
        GÜNCEL bir PUBLIC fiyat çeker, ardından intent'i doğrular. Fiyat
        lookup'ı BAŞARISIZ olursa (transport/timeout/malformed/yanlış
        sembol/geçersiz değer), bu fonksiyon o hatayı YAYAR — `submit()`
        bu durumda ASLA `place_order()`'a ULAŞMAZ (fail-closed)."""
        filters = await self.validate_symbol(intent.symbol)
        market_price: float | None = None
        if determine_market_price_requirement(intent, filters):
            market_price = await self._client.symbol_price(intent.symbol)
        validate_intent_against_filters(intent, filters, market_price=market_price)
        return filters, market_price

    async def submit(self, intent: OrderIntent) -> ExecutionResult:
        await self.validate_intent(intent)
        return await self._client.place_order(intent)


=== FILE: crypto_signal_engine/execution/bridge_runtime.py ===
"""
Faz 13 — `BridgeRuntime`: `PersistedRuntime`'ı (Faz 7, DEĞİŞTİRİLMEDEN)
SARAN, kabul edilmiş her CANLI sinyal-değerlendirme döngüsünden HEMEN
SONRA `SignalTestnetBridge.on_cycle_result()`'ı çağıran bir Faz 13
wrapper'ı — `PersistedRuntime`'ın Faz 6 `RuntimeCoordinator`'ı SARDIĞI
AYNI desenle (bkz. persistence/recovery.py modül docstring'i).

NEDEN AYRI BİR SARMALAYICI (Faz 6/7 dosyaları DEĞİŞTİRİLMEZ): talimat
KASITLI OLARAK "Do not directly POST orders from RuntimeCoordinator" ve
"Do NOT reopen Phases 1-12" der — bu yüzden bridge çağrısı, KOORDİNATÖRÜN
(Faz 6) veya `PersistedRuntime`'ın (Faz 7) İÇİNE GÖMÜLMEZ, onun YERİNE bir
ÜÇÜNCÜ katman eklenir. Bu, repo'nun ZATEN kabul ettiği "wrap, don't
modify" mimari disiplinin (bkz. `PersistedRuntime`'ın KENDİSİ) doğrudan bir
devamıdır.

TARİHSEL REPLAY KORUMASI (Bölüm "NO HISTORICAL REPLAY ORDERS"): `recover()`
BURADA `self._persisted.recover()`'a SAF olarak delege edilir — `Persisted
Runtime.recover()` (Faz 7) ASLA `SignalEngine.evaluate()` çağırmadığı için
(bootstrap yalnızca candle/feature state'i yeniden inşa eder, YENİ bir
sinyal/paper-trade ÜRETMEZ), `recover()` sırasında bridge'e TEK bir
`on_cycle_result()` çağrısı bile ULAŞMAZ — bu YAPISAL bir garanti, ayrı bir
"eğer startup ise atla" bayrağı GEREKMEZ. Bridge, YALNIZCA bu sınıfın KENDİ
`_consume_candles`/`resolve_gap` (canlı akış) yollarından beslenir."""

from __future__ import annotations

import asyncio

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome, ProcessedMarketEvent


class BridgeRuntime:
    """`PersistedRuntime`'ı (Faz 7) saran, isteğe bağlı bir `SignalTestnetBridge`
    (Faz 13) enjekte edilebilen runtime katmanı. `bridge is None` iken bu
    sınıf `PersistedRuntime`'a SAF bir pass-through'dur — hiçbir davranış
    DEĞİŞMEZ (Application, bridge devre dışıyken bu sınıfı HİÇ kullanmaz,
    bkz. `app.py`, ama pass-through modu yine de test edilir/korunur)."""

    def __init__(self, persisted: PersistedRuntime, bridge: SignalTestnetBridge | None) -> None:
        self._persisted = persisted
        self._bridge = bridge
        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol task
        # ownership for THIS layer's bridge-aware consumer tasks (never the
        # bare `RuntimeCoordinator._consume_candles`/`_consume_order_book`
        # — that pair never calls `_maybe_bridge`, see module docstring).
        # Same `self._tasks_by_symbol` pattern already accepted in
        # `RuntimeCoordinator`/`PersistedRuntime`.
        self._tasks_by_symbol: dict[str, list[asyncio.Task]] = {}

    @property
    def _coordinator(self):  # noqa: ANN202 - `Application`'ın mevcut `self.runtime._coordinator` erişimiyle AYNI sözleşme
        return self._persisted._coordinator

    async def recover(self) -> tuple[BootstrapReport, ...]:
        return await self._persisted.recover()

    def status(self):  # noqa: ANN201
        return self._persisted.status()

    # -- Canlı ingestion (bridge'e beslenen TEK yol) ---------------------------

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> ProcessedMarketEvent:
        return self._persisted.ingest_candle(symbol, timeframe, candle)

    def ingest_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> ProcessedMarketEvent:
        # Order-book event'leri hiçbir sinyal değerlendirmesi TETİKLEMEZ
        # (Faz 6, DEĞİŞMEDİ) — bridge için hiçbir şey YAPILMAZ.
        return self._persisted.ingest_order_book(symbol, snapshot)

    async def _maybe_bridge(self, event: ProcessedMarketEvent) -> None:
        if self._bridge is None:
            return
        if event.outcome is not IngestOutcome.ACCEPTED or event.cycle_result is None:
            return
        await self._bridge.on_cycle_result(event.cycle_result)

    async def resolve_gap(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> ProcessedMarketEvent:
        event = await self._persisted.resolve_gap(symbol, timeframe, pending_candle)
        await self._maybe_bridge(event)
        return event

    def bootstrap_candles(self, symbol: str, timeframe: Timeframe, candles: list[Candle], as_of) -> BootstrapReport:  # noqa: ANN001
        return self._persisted.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    # -- Async run/stop (bridge'in beslendiği KENDİ tüketim döngüsü) -----------

    async def run(self) -> None:
        """`PersistedRuntime.run()`'ı (Faz 7) DOĞRUDAN kullanmaz — o kendi
        `self.ingest_candle`'ını (bridge'i ASLA çağırmayan) kullanır. Bu
        yüzden `BridgeRuntime`, AYNI akış şeklini, ama `self.ingest_candle`
        SONRASINDA `await self._maybe_bridge(event)` ÇAĞIRACAK şekilde
        yeniden sağlar (`PersistedRuntime`'ın Faz 6 `RuntimeCoordinator.run()`'ı
        SARDIĞI AYNI desen, bkz. modül docstring'i)."""
        coordinator = self._persisted._coordinator
        if coordinator._stopped:
            raise ValueError("stopped bir BridgeRuntime tekrar run() ile başlatılamaz")

        tasks: list[asyncio.Task] = []
        for symbol in coordinator._symbols:
            tasks.extend(self._spawn_symbol_tasks(symbol))

        await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn_symbol_tasks(self, symbol: str) -> list[asyncio.Task]:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same bridge-aware
        task-creation code path — never a second implementation that could
        drift, and NEVER the bare `RuntimeCoordinator._spawn_symbol_tasks`
        (which calls the coordinator's own `_consume_candles`/
        `_consume_order_book` — those never reach `_maybe_bridge`, see
        module docstring). Tracks the created tasks BOTH in the flat
        `coordinator._tasks` (so `coordinator.stop()`'s existing
        full-shutdown cancellation is completely unchanged) AND in
        `self._tasks_by_symbol` (so `remove_symbol` can cancel just this
        symbol's own tasks)."""
        coordinator = self._persisted._coordinator
        created: list[asyncio.Task] = []
        for timeframe in coordinator._candle_timeframes:
            created.append(asyncio.create_task(self._consume_candles(symbol, timeframe)))
        created.append(asyncio.create_task(self._consume_order_book(symbol)))
        coordinator._tasks.extend(created)
        self._tasks_by_symbol[symbol] = created
        return created

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: hot-adds a symbol's OWN
        bridge-aware candle/order-book consumer tasks — this is the ONE
        correct production surface for the CONFIRMED BLOCKER's part (a)
        (a hot-added symbol previously got NO live candle/order-book
        consumption at all when `BridgeRuntime` is the actual production
        composition, since `RuntimeCoordinator.add_symbol()`'s own
        task-spawn only fires when `coordinator._running` is `True`, which
        production never sets). Idempotent: a symbol that already has
        active task ownership here is a no-op (never a duplicate
        subscription).

        Bootstrap/state establishment is delegated ONCE, DIRECTLY to
        `RuntimeCoordinator.add_symbol()` (bypassing `PersistedRuntime.
        add_symbol()` entirely) — `coordinator.add_symbol()` only spawns
        ITS OWN bare tasks when `coordinator._running` is `True`, which is
        structurally never the case in production, so this call is
        guaranteed to be state+bootstrap ONLY: never a second, competing
        set of bare (non-bridge-aware) consumer tasks."""
        coordinator = self._persisted._coordinator
        normalized = normalize_symbol(symbol)
        if normalized in self._tasks_by_symbol:
            return
        await coordinator.add_symbol(normalized)
        if coordinator._stopped:
            return
        self._spawn_symbol_tasks(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cleanly cancels JUST
        this symbol's own bridge-aware tasks, then delegates state cleanup
        DIRECTLY to `RuntimeCoordinator.remove_symbol()` (same bypass of
        `PersistedRuntime.remove_symbol()` as `add_symbol()`, for the same
        reason — this layer owns the actual production tasks, not
        `PersistedRuntime`). Idempotent: removing a symbol with no active
        task ownership here is a no-op."""
        coordinator = self._persisted._coordinator
        normalized = normalize_symbol(symbol)
        tasks = self._tasks_by_symbol.pop(normalized, [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        coordinator._tasks = [t for t in coordinator._tasks if t not in tasks]
        await coordinator.remove_symbol(normalized)

    async def _consume_candles(self, symbol: str, timeframe: Timeframe) -> None:
        coordinator = self._persisted._coordinator
        try:
            async for candle in coordinator._provider.stream_candles(symbol, timeframe):
                if coordinator._stopped:
                    break
                event = self.ingest_candle(symbol, timeframe, candle)
                if event.outcome is IngestOutcome.GAP_DETECTED:
                    await self.resolve_gap(symbol, timeframe, candle)
                else:
                    await self._maybe_bridge(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            coordinator.mark_disconnected(symbol)
            raise

    async def _consume_order_book(self, symbol: str) -> None:
        coordinator = self._persisted._coordinator
        try:
            async for snapshot in coordinator._provider.stream_order_book(symbol, coordinator._order_book_depth):
                if coordinator._stopped:
                    break
                self.ingest_order_book(symbol, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            coordinator.mark_disconnected(symbol)
            raise

    async def stop(self) -> None:
        await self._persisted.stop()
        self._tasks_by_symbol.clear()


=== FILE: crypto_signal_engine/execution/errors.py ===
"""Faz 10/11 exception taksonomisi.

Kural (hard safety invariant): hiçbir hata mesajı/attribute'u API secret,
signature, veya credential DEĞERİ İÇEREMEZ — yalnızca yapılandırma/istek
KİMLİĞİ (host, sembol, hata kodu, client_order_id) taşınabilir. Bu,
`tests/test_execution_*.py` tarafından açıkça test edilir."""

from __future__ import annotations

from crypto_signal_engine.errors import ExecutionError


class ExecutionConfigError(ExecutionError):
    """Geçersiz Faz 10 konfigürasyonu (host, recv_window, vb.) — fail-fast."""


class UnsafeExecutionHostError(ExecutionConfigError):
    """BLOCKER-seviyesi güvenlik hatası: yapılandırılmış host, onaylı
    Binance Spot TESTNET allowlist'inde DEĞİL. Bu, Mainnet/lookalike/
    arbitrary host'a karşı yapısal reddi temsil eder."""


class MissingCredentialsError(ExecutionConfigError):
    """`BINANCE_TESTNET_API_KEY`/`BINANCE_TESTNET_API_SECRET` eksik/boş."""


class SigningError(ExecutionError):
    """İstek imzalama sırasında bir hata oluştu (asla secret İÇERMEZ)."""


class ExecutionTransportError(ExecutionError):
    """Ağ/transport seviyesinde bir hata (bağlantı koptu, DNS, vb.)."""


class ExecutionTimeoutError(ExecutionTransportError):
    """TESTNET isteği zaman aşımına uğradı."""


class BinanceRejectionError(ExecutionError):
    """Binance TESTNET, isteği AÇIKÇA reddetti (HTTP hata kodu + Binance'in
    kendi `code`/`msg` alanları) — yalnızca kod/mesaj taşınır, asla ham
    secret/signature İÇERMEZ."""

    def __init__(self, message: str, *, binance_code: int | None = None) -> None:
        super().__init__(message)
        self.binance_code = binance_code


class TimestampRejectedError(BinanceRejectionError):
    """Binance, timestamp'i `recvWindow` dışında/geçersiz olarak reddetti
    (Binance hata kodu -1021)."""


class MalformedResponseError(ExecutionError):
    """TESTNET yanıtı beklenen JSON şeklini taşımıyor (bozuk JSON, eksik
    zorunlu alan)."""


class FilterValidationError(ExecutionError):
    """Order intent, sembolün Binance exchange-info filtrelerini
    (LOT_SIZE/PRICE_FILTER/MIN_NOTIONAL) ihlal ediyor — sessizce
    yuvarlanıp gönderilmek yerine AÇIKÇA reddedilir."""


class MarketPriceUnavailableError(ExecutionError):
    """Bir MARKET order'ın notional doğrulaması için gereken GÜNCEL PUBLIC
    fiyat GÜVENİLİR şekilde ALINAMADI (yanlış sembol döndü, veya fiyat
    sonlu/pozitif değil). FAIL-CLOSED: bu durumda order KESİNLİKLE
    gönderilmez — "doğrulamayı atla ve gönder" YAPILMAZ. (Transport/
    timeout/malformed-JSON hataları için ayrıca `ExecutionTransportError`/
    `ExecutionTimeoutError`/`MalformedResponseError` fırlatılır — hepsi
    AYNI şekilde `place_order()`'a ULAŞMADAN yayılır.)"""


# -- Faz 11: Execution Safety & Reconciliation -------------------------------


class LocalExecutionRecordNotFoundError(ExecutionError):
    """`reconcile()`/`order-status` bir `context_id`/`client_order_id` ile
    çağrıldı ama durable store'da EŞLEŞEN bir kayıt YOK. Bu bir programlama
    hatası DEĞİLDİR (normal bir runtime durumu — örn. yanlış yazılmış bir
    kimlik) — bu yüzden `ExecutionError` alt sınıfıdır (CLI'nin `except
    ExecutionError` yakalayıcısı tarafından TEMİZ şekilde işlenir, ham bir
    Python traceback'i SIZDIRMAZ)."""


class OrderNotFoundError(ExecutionError):
    """TESTNET, `origClientOrderId` ile sorgulanan order'ı BULAMADI
    (Binance kodu -2013). Bu GERÇEK bir hata DEĞİLDİR — reconciliation
    akışının bir PARÇASIDIR.

    BLOCKER FİX (Karar 78): bu, "order HİÇ Binance'e ULAŞMADI, yeniden
    gönderim GÜVENLİDİR" ANLAMINA GELMEZ — tek bir anlık -2013 yanıtı,
    orijinal ambiguous POST'un Binance tarafından hiç kabul edilmediğini
    KANITLAMAZ (gecikmeli görünürlük, geçici tutarlılık gecikmesi, ya da
    sorgunun kendisinin geçici bir aksaklığı hepsi mümkündür). Bu yüzden
    çağıran (`reconciliation_service.py`), bu hatayı `UNKNOWN_NOT_FOUND`'a
    çevirir — reconciliation-eligible bir "çözülmemiş" durum, ASLA otomatik
    bir ikinci `POST`'un tetikleyicisi DEĞİL."""


class ExecutionIdempotencyConflictError(ExecutionError):
    """Aynı `context_id`, daha önce persist edilenden FARKLI bir
    `client_order_id`/ekonomi ile yeniden kullanılmaya çalışıldı —
    Faz 5'in `IdempotencyConflictError`'ıyla AYNI ilke: idempotency
    key'in stabil olduğu varsayımı ihlal edildi."""


class ReconciliationContradictionError(ExecutionError):
    """Exchange'den dönen GERÇEK durum, yerel durable record ile
    İMKANSIZ bir şekilde ÇELİŞİYOR (`FILLED -> NEW`, executedQty azalması,
    `exchangeOrderId` değişimi, vb.) — bu SESSİZCE ÜZERİNE YAZILMAZ,
    AÇIKÇA reddedilir (muhtemelen bir client_order_id/order eşleşme
    hatasının KANITIDIR)."""


class ExecutionPersistenceError(ExecutionError):
    """Yerel execution-state persistence'ı BAŞARISIZ oldu. Mesaj, GERİ
    kalan tek güvenilir kimliği (`client_order_id`) AÇIKÇA içerir ki
    operatör persistence düzeldiğinde manuel reconciliation
    çalıştırabilsin — hiçbir zaman "dağıtık transaction" TAKLİT EDİLMEZ.

    CRITICAL FIX (H3 — mainnet-readiness review): bu hata iki YAPISAL
    OLARAK FARKLI anlamda fırlatılıyordu ama çağıranlar bunları AYIRT
    EDEMİYORDU: (a) `place_order()` HİÇ çağrılmadan ÖNCE (SUBMISSION_
    ATTEMPTED kaydı yazılamadı) — exchange'e HİÇBİR ŞEY gönderilmedi,
    serbestçe yeniden denemek TAMAMEN GÜVENLİDİR; (b) `place_order()`
    BAŞARIYLA döndükten SONRA (exchange order'ı KABUL ETTİ, muhtemelen
    DOLDURDU) ama bunu yerel diske YAZAMADIK — burada GERÇEK, canlı bir
    exchange order'ı VAR ve yerel sistemin bundan HABERİ YOK. Bir çağıran
    bu ikisini aynı şekilde ele alıp "güvenle yeniden denenebilir" olarak
    yorumlarsa, (b) durumunda GERÇEK bir duplicate order/fantom pozisyon
    riski oluşur. `exchange_may_have_accepted_order=True` YALNIZCA (b)
    için set edilir — çağıranlar (`lifecycle_manager.py::attempt_exit`,
    `signal_bridge.py::_submit`) bunu AÇIKÇA kontrol edip pozisyonu
    otomatik yeniden denemeye KAPATMALI (manuel reconcile gerektirecek
    şekilde durable bir işaretle)."""

    def __init__(
        self,
        message: str,
        *,
        client_order_id: str | None = None,
        context_id: str | None = None,
        exchange_may_have_accepted_order: bool = False,
    ) -> None:
        super().__init__(message)
        self.client_order_id = client_order_id
        self.context_id = context_id
        self.exchange_may_have_accepted_order = exchange_may_have_accepted_order


class ImpossibleLifecycleTransitionError(ExecutionError):
    """Bilinmeyen bir Binance order status'u veya yerel bir state-machine
    ihlali (örn. tanımsız bir `ExecutionLifecycleState` geçişi) tespit
    edildi — sessizce yok sayılmak yerine AÇIKÇA reddedilir."""


=== FILE: crypto_signal_engine/execution/factory.py ===
"""
Faz 12 — üretim `Application` (`crypto_signal_engine/app.py`) için TEK
execution-service kurulum noktası.

Mimari sınır (Faz 10'dan DEĞİŞTİRİLMEDEN korunur, bkz.
`tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`):
HMAC/credential/private-endpoint/`place_order`'a giden TÜM inşa mantığı
`crypto_signal_engine/execution/` paketinin İÇİNDE KALMALIDIR — `app.py`
(veya `ops/`, `runtime/` gibi başka HERHANGİ bir Faz 1-9 modülü) bu
paketin DIŞINDA ASLA credential okuma/imzalama/order-inşa kodu İÇEREMEZ.
Bu modül, `app.py`'nin TEK yaptığı şeyin "opt-in KARARINI vermek" (Faz 12
Bölüm 2 — `ExecutionMode.BINANCE_SPOT_TESTNET` + açık enable bayrağı)
olmasını sağlar; GERÇEK inşa BURADA gerçekleşir."""

from __future__ import annotations

import os
from pathlib import Path

from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig, TestnetHttpClient
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock


def testnet_credentials_status() -> tuple[bool, bool]:
    """Faz 12 — BLOCKER FİX (bağımsız acceptance review bulgusu):
    `(api_key_present, api_secret_present)` döner — kimlik bilgisi
    DEĞERLERİNİ ASLA döndürmez/loglamaz, yalnızca ortam değişkeninin VAR
    olup OLMADIĞINI (bkz. `BinanceTestnetConfig.__repr__`'in AYNI SET/
    UNSET redaksiyon ilkesi).

    NEDEN BU FONKSİYON GEREKLİ: `reconcile_pending()`, bekleyen (`AMBIGUOUS`/
    `UNKNOWN_NOT_FOUND`/vb.) HİÇBİR kayıt YOKSA hiçbir signed `GET /api/v3/order`
    çağrısı YAPMAZ (boş bir liste üzerinde döner) — bu durumda
    `MissingCredentialsError` ASLA TETİKLENMEZ, çünkü hiçbir imzalı istek
    hiç DENENMEZ. Önceki tasarım, credential eksikliğini YALNIZCA "bekleyen
    bir signed çağrı başarısız olursa" keşfediyordu — boş bir execution DB
    ile (örn. TESTNET'in İLK KEZ etkinleştirildiği bir kurulum) bu, kimlik
    bilgisi HİÇ OLMASA BİLE `reconcile_pending()`'in "başarıyla" (sıfır
    kayıt üzerinde) tamamlanmasına ve `execution_ready=True` OLMASINA yol
    AÇIYORDU — YANLIŞ bir "TESTNET execution hazır" sinyali.

    FİX: çağıran (`app.py::Application._reconcile_execution_startup()`),
    HERHANGİ bir ağ çağrısından ÖNCE bu fonksiyonu çağırır — kimlik bilgisi
    eksikse `reconcile_pending()` HİÇ ÇAĞRILMAZ (sıfır network çağrısı,
    sıfır POST, sıfır signed GET) ve `execution_ready` FAIL-CLOSED
    `False` KALIR. Bu, "bekleyen bir signed API çağrısına GÜVENME" ve
    "operatörün `doctor` çalıştırmış OLMASINA güvenme" gereksinimlerini
    YAPISAL olarak sağlar — kontrol HER `start()` çağrısında OTOMATİK
    çalışır."""
    return (
        bool(os.environ.get("BINANCE_TESTNET_API_KEY")),
        bool(os.environ.get("BINANCE_TESTNET_API_SECRET")),
    )


def build_testnet_execution_service(
    *,
    execution_db_path: Path,
    http_client: TestnetHttpClient | None = None,
    clock: Clock | None = None,
) -> tuple[ExecutionReconciliationService, ExecutionStateStore]:
    """KOŞULSUZ kurar — "TESTNET etkinleştirilmeli mi?" KARARI çağıranın
    (`app.py::build_execution_service`) sorumluluğundadır; bu fonksiyon
    yalnızca o karar ZATEN verildikten SONRA çağrılır. Kimlik bilgileri
    (Faz 10 Karar 71 ile AYNI ilke) opsiyoneldir — eksikse yalnızca
    SIGNED bir çağrı sırasında `MissingCredentialsError` fırlatılır.

    `http_client`/`clock` (opsiyonel): offline testlerin GERÇEK ağa
    dokunmadan sahte bir `TestnetHttpClient`/`FixedClock` enjekte etmesi
    için (Faz 10/11'in KENDİ test disipliniyle AYNI)."""
    testnet_config = BinanceTestnetConfig(
        api_key=os.environ.get("BINANCE_TESTNET_API_KEY") or None,
        api_secret=os.environ.get("BINANCE_TESTNET_API_SECRET") or None,
    )
    resolved_clock = clock or SystemClock()
    client = BinanceTestnetClient(testnet_config, http_client or _default_http_client(), clock=resolved_clock)
    store = ExecutionStateStore(execution_db_path)
    service = ExecutionReconciliationService(client, store, clock=resolved_clock)
    return service, store


def _default_http_client() -> TestnetHttpClient:
    from crypto_signal_engine.execution.testnet_client import UrllibTestnetHttpClient

    return UrllibTestnetHttpClient()


=== FILE: crypto_signal_engine/execution/lifecycle.py ===
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


=== FILE: crypto_signal_engine/execution/lifecycle_manager.py ===
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


=== FILE: crypto_signal_engine/execution/lifecycle_migration.py ===
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


=== FILE: crypto_signal_engine/execution/lifecycle_runtime.py ===
"""
`LifecycleRuntime` — the M1-candle-driven lifecycle evaluation loop
(Phase 6/8/9/12). Wraps `BridgeRuntime` the SAME way `BridgeRuntime` wraps
`PersistedRuntime` (an additional layer, never editing the wrapped class —
see `bridge_runtime.py`'s own module docstring for the precedent).

CRITICAL ISOLATION (Phase 9/12): this subscribes to the market-data
provider's M1 candle stream DIRECTLY — it NEVER calls
`RuntimeCoordinator.ingest_candle()`. That means M1 candles never touch
`FeatureEngine`/`SignalEngine`/`PaperTradingEngine` at all: the existing
accepted PAPER simulation and M5/M15/H1 signal cadence are byte-for-byte
unaffected by this module's existence. This is what lets Phase 6's "no
per-second REST polling, evaluate on M1 candle close" requirement be met
without touching the already-accepted signal pipeline in any way.

Per-symbol execution-activation firewall (Phase 12): a freshly (re)started
runtime, or a freshly hot-added symbol, must not fire on stale
already-closed M1 candles from before this process/symbol was live — this
is enforced structurally by only ever consuming the LIVE websocket stream
here (never a backfill/bootstrap replay), exactly like `BridgeRuntime`
already does for the signal path."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle as DomainCandle
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.execution.lifecycle import Candle as LifecycleCandle
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager

_LOGGER = logging.getLogger("crypto_signal_engine.execution.lifecycle_runtime")

_ATR_FEATURE_NAME = "ATR_14"
# The M5 ATR reference is considered stale (Phase 9 "fail closed for that
# trigger only") once it is older than this multiple of the M5 interval —
# two missed M5 closes is a generous, explainable staleness bound that
# tolerates one transient gap without needlessly disabling trailing.
_ATR_STALENESS_M5_MULTIPLES = 2
_M5_INTERVAL = timedelta(minutes=5)


def _to_lifecycle_candle(candle: DomainCandle) -> LifecycleCandle:
    return LifecycleCandle(
        open=candle.open, high=candle.high, low=candle.low, close=candle.close, close_time=candle.close_time,
    )


def latest_m5_atr(coordinator, symbol: str, *, now) -> float | None:  # noqa: ANN001
    """Shared by `LifecycleRuntime` (per-M1-candle trailing recompute) and
    `app.py`'s entry-time `atr_provider` — a single staleness/lookup policy
    for "the latest valid M5 ATR" (Phase 9), reached via the SAME private
    `coordinator._feature_engine` accessor `bridge_runtime.py` already
    uses for `coordinator._symbols`/`coordinator._provider` (existing
    precedent, not a new anti-pattern)."""
    try:
        snapshot = coordinator._feature_engine.latest_snapshot(symbol, Timeframe.M5)
    except Exception:  # noqa: BLE001 - ATR yoksa fail-closed (skip), sinyal/paper etkilenmez
        return None
    if snapshot is None:
        return None
    age = now - snapshot.as_of
    if age > _M5_INTERVAL * _ATR_STALENESS_M5_MULTIPLES:
        _LOGGER.warning(
            "lifecycle: %s M5 ATR STALE (age=%s, snapshot.as_of=%s) — treated as unavailable", symbol, age, snapshot.as_of,
        )
        return None
    value = snapshot.values.get(_ATR_FEATURE_NAME)
    if value is None or value <= 0:
        return None
    return value


class LifecycleRuntime:
    """Wraps a `BridgeRuntime` and adds one independent M1 candle consumer
    task per symbol, feeding `LifecycleManager.evaluate_m1_candle()`.
    `run()`/`stop()` compose with the wrapped runtime's own task
    lifecycle — the M1 tasks are tracked separately here and always
    cancelled in `stop()`, so a `LifecycleRuntime.stop()` leaves no
    dangling subscriptions."""

    def __init__(
        self, bridge_runtime: BridgeRuntime, lifecycle_manager: LifecycleManager | None,
        *, m1_candle_observer: Callable[[str, LifecycleCandle], None] | None = None,
    ) -> None:
        self._bridge_runtime = bridge_runtime
        self._lifecycle_manager = lifecycle_manager
        # Adaptive Intelligence v1 — M1-fidelity shadow-evaluation fix
        # (independent-verification round). Additive, optional, exact
        # same defensive-wrap discipline as `RuntimeCoordinator.
        # candle_observer`/`order_book_observer` (Karar 87): `None`
        # (default) has ZERO effect on any existing behaviour. When
        # supplied, called ONCE PER REAL, ALREADY-CLOSED M1 candle — the
        # SAME frequency `LifecycleManager.evaluate_m1_candle()` itself
        # uses for the real position — purely observationally, AFTER the
        # real evaluation attempt (whether it succeeded or was isolated
        # by the `except Exception` below), never altering this loop's
        # own control flow or timing. This exists because
        # `RuntimeCoordinator.ingest_candle`'s `candle_observer` ONLY
        # ever sees M5/M15/H1 — live M1 candles bypass `RuntimeCoordinator`
        # entirely via `_consume_m1` below, so a shadow evaluator wired
        # to that M5-level hook alone would run at 5-minute-aggregated
        # granularity, not the true 1-minute granularity the real
        # position is evaluated at — see `adaptive.shadow.ShadowMonitor.
        # on_m1_candle` for the consumer.
        self._m1_candle_observer = m1_candle_observer
        self._m1_tasks: list[asyncio.Task] = []
        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol M1 task
        # ownership, additive to `self._m1_tasks` (existing tests assert
        # that flat list directly; `stop()`'s full-shutdown cancellation
        # stays unchanged). This is the CONFIRMED BLOCKER's part (b): a
        # hot-added symbol previously never got an M1 stop/target/trailing
        # evaluator task AT ALL, since `run()` only ever iterated
        # `coordinator._symbols` ONCE, at call time.
        self._m1_tasks_by_symbol: dict[str, asyncio.Task] = {}

    @property
    def _coordinator(self):  # noqa: ANN202 - aynı erişim sözleşmesi (bkz. BridgeRuntime)
        return self._bridge_runtime._coordinator

    async def recover(self):  # noqa: ANN201
        return await self._bridge_runtime.recover()

    def status(self):  # noqa: ANN201
        return self._bridge_runtime.status()

    def ingest_candle(self, symbol, timeframe, candle):  # noqa: ANN001, ANN201
        return self._bridge_runtime.ingest_candle(symbol, timeframe, candle)

    def ingest_order_book(self, symbol, snapshot):  # noqa: ANN001, ANN201
        return self._bridge_runtime.ingest_order_book(symbol, snapshot)

    async def resolve_gap(self, symbol, timeframe, pending_candle):  # noqa: ANN001, ANN201
        return await self._bridge_runtime.resolve_gap(symbol, timeframe, pending_candle)

    def bootstrap_candles(self, symbol, timeframe, candles, as_of):  # noqa: ANN001, ANN201
        return self._bridge_runtime.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    def _latest_m5_atr(self, symbol: str, *, now) -> float | None:  # noqa: ANN001
        if self._lifecycle_manager is None:
            return None
        return latest_m5_atr(self._coordinator, symbol, now=now)

    async def _consume_m1(self, symbol: str) -> None:
        coordinator = self._coordinator
        try:
            async for candle in coordinator._provider.stream_candles(symbol, Timeframe.M1):
                if coordinator._stopped:
                    break
                if not candle.is_closed:
                    continue
                if self._lifecycle_manager is None:
                    continue
                lifecycle_candle = _to_lifecycle_candle(candle)
                atr = self._latest_m5_atr(symbol, now=candle.close_time)
                try:
                    await self._lifecycle_manager.evaluate_m1_candle(
                        symbol, lifecycle_candle, atr_for_trailing=atr,
                    )
                except Exception:  # noqa: BLE001 - bir sembolün lifecycle hatası DİĞER sembolleri/runtime'ı ASLA etkilemez
                    _LOGGER.error("lifecycle: M1 evaluation FAILED for %s (isolated)", symbol, exc_info=True)
                if self._m1_candle_observer is not None:
                    # Bkz. __init__ yorumu — TÜM gerçek M1 değerlendirmesi
                    # ZATEN tamamlandıktan SONRA (başarılı ya da izole
                    # edilmiş), salt-gözlemsel olarak çağrılır; geniş
                    # try/except KASITLI (aynı gerekçe: bir observer hatası
                    # bu döngüyü ASLA etkileyemez).
                    try:
                        self._m1_candle_observer(symbol, lifecycle_candle)
                    except Exception:  # noqa: BLE001 - observer hatası M1 tüketim döngüsünü ASLA etkilemez
                        _LOGGER.error("lifecycle: M1 candle observer FAILED for %s (isolated)", symbol, exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error("lifecycle: M1 stream FAILED for %s", symbol, exc_info=True)

    def _spawn_m1_task(self, symbol: str) -> None:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same M1-task-creation
        code path — never a second implementation that could drift. A
        no-op when no `LifecycleManager` is wired (mirrors `run()`'s own
        existing guard — PAPER/bridge-only behavior is unaffected)."""
        if self._lifecycle_manager is None:
            return
        task = asyncio.create_task(self._consume_m1(symbol))
        self._m1_tasks.append(task)
        self._m1_tasks_by_symbol[symbol] = task

    async def run(self) -> None:
        if self._lifecycle_manager is not None:
            coordinator = self._coordinator
            for symbol in coordinator._symbols:
                self._spawn_m1_task(symbol)
        await self._bridge_runtime.run()

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: this is the ONE correct
        production surface for the CONFIRMED BLOCKER's part (b) — a
        hot-added symbol previously NEVER got an M1 stop/target/trailing
        evaluator task, meaning a position it later opened would sit
        completely unprotected. Delegates bootstrap/state establishment
        AND bridge-aware candle/order-book task creation to
        `self._bridge_runtime.add_symbol()` (already idempotent), then
        additionally spawns this symbol's OWN M1 task — symmetric to how
        `run()` composes with `self._bridge_runtime.run()`. Idempotent:
        a symbol that already has an active M1 task is never given a
        second one."""
        normalized = normalize_symbol(symbol)
        await self._bridge_runtime.add_symbol(normalized)
        if normalized not in self._m1_tasks_by_symbol:
            self._spawn_m1_task(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cancels JUST this
        symbol's M1 task first (symmetric to `add_symbol` spawning it
        last), then delegates to `self._bridge_runtime.remove_symbol()`
        for the candle/order-book layer + state cleanup. Idempotent:
        removing a symbol with no active M1 task here is a no-op for this
        layer (the delegated call below still proceeds)."""
        normalized = normalize_symbol(symbol)
        task = self._m1_tasks_by_symbol.pop(normalized, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._m1_tasks = [t for t in self._m1_tasks if t is not task]
        await self._bridge_runtime.remove_symbol(normalized)

    async def stop(self) -> None:
        for task in self._m1_tasks:
            task.cancel()
        for task in self._m1_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._m1_tasks.clear()
        self._m1_tasks_by_symbol.clear()
        await self._bridge_runtime.stop()


=== FILE: crypto_signal_engine/execution/lifecycle_store.py ===
"""
Durable SQLite persistence for the autonomous Testnet trading lifecycle
(Phase 3/11/15). This EXTENDS the existing Testnet execution database
(same file as `reconciliation_store.py::ExecutionStateStore`) with
ADDITIVE, migration-safe new tables — `execution_record`'s schema and
`SCHEMA_VERSION` are never touched, so all of Phase 11's existing
concurrency/reconciliation guarantees and tests are untouched.

Three new tables, all `CREATE TABLE IF NOT EXISTS` (never a destructive
migration):
- `bridge_position`: one row per symbol, the CURRENT lifecycle state
  (Phase 3's durable fields). Upserted, never deleted.
- `bridge_fee_ledger`: append-only, one row per observed commission
  (Phase 5's fee ledger — grouped by `trade_group_id`, which is the
  position's entry `client_order_id`, so a completed trade's fees can be
  looked up precisely even across many re-entries of the same symbol).
- `bridge_completed_trade`: append-only, one row per fully closed round
  trip (Phase 14/15's trade history).
- `bridge_daily_risk`: one row per UTC trading day, the Phase 11 daily
  conservative-risk-P&L accumulator (never reset by a restart).

HARD SAFETY INVARIANT: no table here has any credential/secret column."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    DailyRiskAccumulator,
    FeeLedgerEntry,
    PositionLifecycleState,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bridge_position (
    symbol TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    gross_entry_vwap REAL,
    net_owned_base_quantity REAL NOT NULL,
    entry_order_client_order_id TEXT,
    initial_protective_stop REAL,
    high_water REAL,
    effective_stop REAL,
    trailing_active INTEGER NOT NULL,
    last_stop_mechanism TEXT NOT NULL,
    take_profit REAL,
    last_evaluated_candle_close TEXT,
    exit_pending_client_order_id TEXT,
    last_exit_reason TEXT,
    entry_timestamp TEXT,
    entry_signal_context_id TEXT,
    entry_client_order_id TEXT,
    cumulative_realized_gross_pnl REAL NOT NULL,
    cooldown_until TEXT,
    migrated_existing_position INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bridge_fee_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_group_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side TEXT NOT NULL,
    amount REAL NOT NULL,
    asset TEXT NOT NULL,
    usdt_equivalent REAL,
    trade_id INTEGER,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bridge_fee_ledger_group ON bridge_fee_ledger(symbol, trade_group_id);

CREATE TABLE IF NOT EXISTS bridge_completed_trade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_group_id TEXT NOT NULL,
    entry_client_order_id TEXT NOT NULL,
    exit_client_order_id TEXT NOT NULL,
    entry_timestamp TEXT NOT NULL,
    exit_timestamp TEXT NOT NULL,
    quantity_closed REAL NOT NULL,
    gross_entry_vwap REAL NOT NULL,
    exit_gross_vwap REAL NOT NULL,
    gross_realized_pnl REAL NOT NULL,
    net_realized_pnl REAL,
    exit_reason TEXT NOT NULL,
    entry_signal_context_id TEXT,
    exit_signal_context_id TEXT,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bridge_completed_trade_symbol ON bridge_completed_trade(symbol, recorded_at);

CREATE TABLE IF NOT EXISTS bridge_daily_risk (
    trading_day TEXT PRIMARY KEY,
    conservative_risk_pnl REAL NOT NULL,
    trades_counted INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
"""

# Adaptive Intelligence v1 — ADDITIVE-only columns (no destructive
# migration, no column ever dropped/renamed). `CREATE TABLE IF NOT
# EXISTS` above never adds columns to an already-existing table, so a
# real Testnet database created before this change needs these applied
# via `ALTER TABLE ... ADD COLUMN` on open (idempotently — SQLite has no
# `ADD COLUMN IF NOT EXISTS`, so a "duplicate column" error is expected
# and swallowed on every run after the first).
_BRIDGE_POSITION_NEW_COLUMNS = (
    ("exit_policy_stop_atr_multiple", "REAL"),
    ("exit_policy_take_profit_atr_multiple", "REAL"),
    ("exit_policy_trailing_activation_atr_multiple", "REAL"),
    ("exit_policy_trailing_distance_atr_multiple", "REAL"),
    ("exit_policy_max_hold_hours", "REAL"),
    ("policy_version_id", "TEXT"),
)
_BRIDGE_COMPLETED_TRADE_NEW_COLUMNS = (
    ("policy_version_id", "TEXT"),
)


def _add_column_if_missing(connection: sqlite3.Connection, table: str, column: str, sql_type: str) -> None:
    try:
        with connection:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class LifecycleStore:
    """Opens its OWN connection to the SAME execution-db file used by
    `ExecutionStateStore` — SQLite's WAL mode makes this safe for
    concurrent readers/writers across connections (same discipline the
    codebase already relies on for `paper_state.db` vs. `testnet_execution.db`
    being separate files; here it is the same file, safe because every
    write here is independently transacted and touches disjoint tables)."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
            for column, sql_type in _BRIDGE_POSITION_NEW_COLUMNS:
                _add_column_if_missing(self._connection, "bridge_position", column, sql_type)
            for column, sql_type in _BRIDGE_COMPLETED_TRADE_NEW_COLUMNS:
                _add_column_if_missing(self._connection, "bridge_completed_trade", column, sql_type)
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle schema oluşturma başarısız: {exc}") from exc

    def close(self) -> None:
        self._connection.close()

    # -- Transaction discipline (CRITICAL FIX M1 — mainnet-readiness review) --
    #
    # This connection is opened with `isolation_level=None` (autocommit) —
    # EXACTLY like `reconciliation_store.py`'s own connection, and for the
    # SAME reason (explicit `BEGIN IMMEDIATE` control, see that module's
    # `__init__` comment). Unlike `reconciliation_store.py`, every write
    # method here used to wrap its statement(s) in `with self._connection:`
    # instead — under `isolation_level=None` that context manager is a
    # SILENT NO-OP (Python's sqlite3 module only auto-manages BEGIN/COMMIT
    # when `isolation_level` is NOT `None`), so it provided the ILLUSION of
    # a transaction while actually running every statement in true SQLite
    # autocommit mode. Two concrete gaps this caused:
    #   1. `append_fee_entries()`'s `executemany()` INSERT of several rows
    #      had NO atomicity guarantee across those rows — a crash/OS error
    #      mid-loop could leave a PARTIAL fee ledger for one trade group.
    #   2. `_finalize_exit()` (lifecycle_manager.py) makes FOUR separate
    #      store calls to close one trade (`append_fee_entries()` ->
    #      `record_completed_trade()` -> `save_daily_risk()` ->
    #      `save_position()`) — each was independently "transacted" (i.e.
    #      not really), so a crash between any two of them could leave
    #      inconsistent cross-table state: e.g. a `bridge_completed_trade`
    #      row recorded but `bridge_position` never actually flipped out of
    #      LONG, or a position flipped FLAT with the trade never recorded.
    #
    # Fix: every write method below now uses the SAME explicit `BEGIN
    # IMMEDIATE` / `commit()` / `rollback()` discipline `reconciliation_
    # store.py::save()` already established, via the `_begin`/`_commit`/
    # `_rollback` helpers and `_locked` inner methods (assume a transaction
    # is already open — never call these directly). `save_position_with_
    # fee_entries()` and `finalize_exit()` are NEW composite methods that
    # run their entire multi-statement write in ONE transaction — the two
    # cross-table call sites above (`on_entry_filled()`/`_finalize_exit()`
    # in lifecycle_manager.py) now use these instead of separate calls.

    def _begin(self) -> None:
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle store: transaction başlatılamadı: {exc}") from exc

    def _commit(self) -> None:
        try:
            self._connection.commit()
        except sqlite3.Error as exc:
            raise PersistenceError(f"lifecycle store: transaction commit başarısız: {exc}") from exc

    def _rollback(self) -> None:
        try:
            self._connection.rollback()
        except sqlite3.Error:
            pass  # best-effort — the ORIGINAL error is what propagates to the caller

    # -- bridge_position ----------------------------------------------------

    def save_position(self, record: BridgePositionRecord) -> None:
        self._begin()
        try:
            self._save_position_locked(record)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_position kaydı başarısız (symbol={record.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _save_position_locked(self, record: BridgePositionRecord) -> None:
        """Assumes a transaction is ALREADY open (`_begin()` already
        called by the caller) — never call directly."""
        self._connection.execute(
            """
            INSERT INTO bridge_position (
                        symbol, state, gross_entry_vwap, net_owned_base_quantity,
                        entry_order_client_order_id, initial_protective_stop, high_water, effective_stop,
                        trailing_active, last_stop_mechanism, take_profit, last_evaluated_candle_close,
                        exit_pending_client_order_id, last_exit_reason, entry_timestamp,
                        entry_signal_context_id, entry_client_order_id, cumulative_realized_gross_pnl,
                        cooldown_until, migrated_existing_position, updated_at,
                        exit_policy_stop_atr_multiple, exit_policy_take_profit_atr_multiple,
                        exit_policy_trailing_activation_atr_multiple, exit_policy_trailing_distance_atr_multiple,
                        exit_policy_max_hold_hours, policy_version_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        state=excluded.state, gross_entry_vwap=excluded.gross_entry_vwap,
                        net_owned_base_quantity=excluded.net_owned_base_quantity,
                        entry_order_client_order_id=excluded.entry_order_client_order_id,
                        initial_protective_stop=excluded.initial_protective_stop,
                        high_water=excluded.high_water, effective_stop=excluded.effective_stop,
                        trailing_active=excluded.trailing_active, last_stop_mechanism=excluded.last_stop_mechanism,
                        take_profit=excluded.take_profit,
                        last_evaluated_candle_close=excluded.last_evaluated_candle_close,
                        exit_pending_client_order_id=excluded.exit_pending_client_order_id,
                        last_exit_reason=excluded.last_exit_reason, entry_timestamp=excluded.entry_timestamp,
                        entry_signal_context_id=excluded.entry_signal_context_id,
                        entry_client_order_id=excluded.entry_client_order_id,
                        cumulative_realized_gross_pnl=excluded.cumulative_realized_gross_pnl,
                        cooldown_until=excluded.cooldown_until,
                        migrated_existing_position=excluded.migrated_existing_position,
                        updated_at=excluded.updated_at,
                        exit_policy_stop_atr_multiple=excluded.exit_policy_stop_atr_multiple,
                        exit_policy_take_profit_atr_multiple=excluded.exit_policy_take_profit_atr_multiple,
                        exit_policy_trailing_activation_atr_multiple=excluded.exit_policy_trailing_activation_atr_multiple,
                        exit_policy_trailing_distance_atr_multiple=excluded.exit_policy_trailing_distance_atr_multiple,
                        exit_policy_max_hold_hours=excluded.exit_policy_max_hold_hours,
                        policy_version_id=excluded.policy_version_id
                    """,
                    (
                        record.symbol, record.state.value, record.gross_entry_vwap,
                        record.net_owned_base_quantity, record.entry_order_client_order_id,
                        record.initial_protective_stop, record.high_water, record.effective_stop,
                        int(record.trailing_active), record.last_stop_mechanism, record.take_profit,
                        _iso(record.last_evaluated_candle_close), record.exit_pending_client_order_id,
                        record.last_exit_reason, _iso(record.entry_timestamp),
                        record.entry_signal_context_id, record.entry_client_order_id,
                        record.cumulative_realized_gross_pnl, _iso(record.cooldown_until),
                        int(record.migrated_existing_position), _iso(record.updated_at),
                        record.exit_policy_stop_atr_multiple, record.exit_policy_take_profit_atr_multiple,
                        record.exit_policy_trailing_activation_atr_multiple,
                        record.exit_policy_trailing_distance_atr_multiple,
                record.exit_policy_max_hold_hours, record.policy_version_id,
            ),
        )

    def load_position(self, symbol: str) -> BridgePositionRecord | None:
        normalized = normalize_symbol(symbol)
        row = self._connection.execute(
            "SELECT * FROM bridge_position WHERE symbol = ?", (normalized,)
        ).fetchone()
        if row is None:
            return None
        try:
            return BridgePositionRecord(
                symbol=row["symbol"], state=PositionLifecycleState(row["state"]),
                gross_entry_vwap=row["gross_entry_vwap"], net_owned_base_quantity=row["net_owned_base_quantity"],
                entry_order_client_order_id=row["entry_order_client_order_id"],
                initial_protective_stop=row["initial_protective_stop"], high_water=row["high_water"],
                effective_stop=row["effective_stop"], trailing_active=bool(row["trailing_active"]),
                last_stop_mechanism=row["last_stop_mechanism"], take_profit=row["take_profit"],
                last_evaluated_candle_close=_parse_dt(row["last_evaluated_candle_close"]),
                exit_pending_client_order_id=row["exit_pending_client_order_id"],
                last_exit_reason=row["last_exit_reason"], entry_timestamp=_parse_dt(row["entry_timestamp"]),
                entry_signal_context_id=row["entry_signal_context_id"],
                entry_client_order_id=row["entry_client_order_id"],
                cumulative_realized_gross_pnl=row["cumulative_realized_gross_pnl"],
                cooldown_until=_parse_dt(row["cooldown_until"]),
                migrated_existing_position=bool(row["migrated_existing_position"]),
                updated_at=_parse_dt(row["updated_at"]),
                # Adaptive Intelligence v1 additive columns — `row[...]`
                # returns SQL NULL as `None` for a legacy row written
                # before these columns existed (they are added via
                # `ALTER TABLE ... ADD COLUMN` with no DEFAULT), which is
                # exactly `BridgePositionRecord`'s own default for these
                # fields — `resolved_exit_policy()` then correctly falls
                # back to the caller's current `ExitPolicyConfig`.
                exit_policy_stop_atr_multiple=row["exit_policy_stop_atr_multiple"],
                exit_policy_take_profit_atr_multiple=row["exit_policy_take_profit_atr_multiple"],
                exit_policy_trailing_activation_atr_multiple=row["exit_policy_trailing_activation_atr_multiple"],
                exit_policy_trailing_distance_atr_multiple=row["exit_policy_trailing_distance_atr_multiple"],
                exit_policy_max_hold_hours=row["exit_policy_max_hold_hours"],
                policy_version_id=row["policy_version_id"],
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise CorruptRecordError(f"bozuk bridge_position satırı ({symbol!r}): {exc}") from exc

    def list_positions(self) -> tuple[BridgePositionRecord, ...]:
        rows = self._connection.execute("SELECT symbol FROM bridge_position ORDER BY symbol ASC").fetchall()
        return tuple(self.load_position(row["symbol"]) for row in rows)  # type: ignore[misc]

    # -- bridge_fee_ledger ----------------------------------------------------

    def append_fee_entries(self, symbol: str, trade_group_id: str, entries: tuple[FeeLedgerEntry, ...]) -> None:
        if not entries:
            return
        self._begin()
        try:
            self._append_fee_entries_locked(symbol, trade_group_id, entries)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(
                f"bridge_fee_ledger yazımı başarısız (symbol={symbol}, trade_group_id={trade_group_id}): {exc}"
            ) from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _append_fee_entries_locked(self, symbol: str, trade_group_id: str, entries: tuple[FeeLedgerEntry, ...]) -> None:
        """Assumes a transaction is ALREADY open — never call directly.
        CRITICAL FIX (M1 — mainnet-readiness review): this `executemany()`
        previously ran under `with self._connection:`, a no-op under
        `isolation_level=None` — a crash mid-loop could leave a PARTIAL
        fee ledger for one trade group (some fills recorded, others not).
        Now runs inside the caller's single explicit transaction, so
        either ALL of these rows land or NONE do."""
        if not entries:
            return
        normalized = normalize_symbol(symbol)
        self._connection.executemany(
            """
            INSERT INTO bridge_fee_ledger (
                symbol, trade_group_id, client_order_id, side, amount, asset,
                usdt_equivalent, trade_id, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    normalized, trade_group_id, e.client_order_id, e.side, e.amount, e.asset,
                    e.usdt_equivalent, e.trade_id, e.recorded_at.isoformat(),
                )
                for e in entries
            ],
        )

    def fee_ledger_for_trade_group(self, symbol: str, trade_group_id: str) -> tuple[FeeLedgerEntry, ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM bridge_fee_ledger WHERE symbol = ? AND trade_group_id = ? ORDER BY id ASC",
            (normalized, trade_group_id),
        ).fetchall()
        return tuple(
            FeeLedgerEntry(
                amount=row["amount"], asset=row["asset"], usdt_equivalent=row["usdt_equivalent"],
                client_order_id=row["client_order_id"], side=row["side"], trade_id=row["trade_id"],
                recorded_at=_parse_dt(row["recorded_at"]),  # type: ignore[arg-type]
            )
            for row in rows
        )

    # -- bridge_completed_trade ----------------------------------------------

    def record_completed_trade(
        self,
        *,
        symbol: str,
        trade_group_id: str,
        entry_client_order_id: str,
        exit_client_order_id: str,
        entry_timestamp: datetime,
        exit_timestamp: datetime,
        quantity_closed: float,
        gross_entry_vwap: float,
        exit_gross_vwap: float,
        gross_realized_pnl: float,
        net_realized_pnl: float | None,
        exit_reason: str,
        entry_signal_context_id: str | None,
        exit_signal_context_id: str | None,
        now: datetime,
        policy_version_id: str | None = None,
    ) -> None:
        self._begin()
        try:
            self._record_completed_trade_locked(
                symbol=symbol, trade_group_id=trade_group_id, entry_client_order_id=entry_client_order_id,
                exit_client_order_id=exit_client_order_id, entry_timestamp=entry_timestamp,
                exit_timestamp=exit_timestamp, quantity_closed=quantity_closed, gross_entry_vwap=gross_entry_vwap,
                exit_gross_vwap=exit_gross_vwap, gross_realized_pnl=gross_realized_pnl,
                net_realized_pnl=net_realized_pnl, exit_reason=exit_reason,
                entry_signal_context_id=entry_signal_context_id, exit_signal_context_id=exit_signal_context_id,
                now=now, policy_version_id=policy_version_id,
            )
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_completed_trade yazımı başarısız (symbol={symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _record_completed_trade_locked(
        self,
        *,
        symbol: str,
        trade_group_id: str,
        entry_client_order_id: str,
        exit_client_order_id: str,
        entry_timestamp: datetime,
        exit_timestamp: datetime,
        quantity_closed: float,
        gross_entry_vwap: float,
        exit_gross_vwap: float,
        gross_realized_pnl: float,
        net_realized_pnl: float | None,
        exit_reason: str,
        entry_signal_context_id: str | None,
        exit_signal_context_id: str | None,
        now: datetime,
        policy_version_id: str | None = None,
    ) -> None:
        """Assumes a transaction is ALREADY open — never call directly."""
        normalized = normalize_symbol(symbol)
        self._connection.execute(
            """
            INSERT INTO bridge_completed_trade (
                symbol, trade_group_id, entry_client_order_id, exit_client_order_id,
                entry_timestamp, exit_timestamp, quantity_closed, gross_entry_vwap, exit_gross_vwap,
                gross_realized_pnl, net_realized_pnl, exit_reason, entry_signal_context_id,
                exit_signal_context_id, recorded_at, policy_version_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized, trade_group_id, entry_client_order_id, exit_client_order_id,
                entry_timestamp.isoformat(), exit_timestamp.isoformat(), quantity_closed,
                gross_entry_vwap, exit_gross_vwap, gross_realized_pnl, net_realized_pnl, exit_reason,
                entry_signal_context_id, exit_signal_context_id, now.isoformat(), policy_version_id,
            ),
        )

    def recent_completed_trades(self, limit: int = 50) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade ORDER BY recorded_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def completed_trades_for_symbol(self, symbol: str) -> tuple[dict[str, object], ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade WHERE symbol = ? ORDER BY recorded_at ASC", (normalized,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def total_realized_pnl(self) -> tuple[float, int]:
        """Portfolio/Accounting v1, step 2 — TRUE lifetime realized P&L:
        `SUM(net_realized_pnl)`/`COUNT(*)` over EVERY completed trade
        EVER, via one SQL aggregate query — no `LIMIT`, no in-Python
        summation of a bounded `recent_completed_trades()` fetch (the
        previous dashboard code's blind spot once more than 50 trades
        exist). `SUM()` already skips `NULL` `net_realized_pnl` rows per
        standard SQL (a trade whose fee USDT-equivalent could not be
        resolved, see `net_realized_pnl()` in `lifecycle.py`) — the
        returned count is `COUNT(*)`, every completed trade regardless of
        whether its net P&L happens to be known, since a trade with
        unknown net P&L is still a completed TRADE. Returns `(0.0, 0)`
        for an empty table (`COALESCE`, never a SQL `NULL` sum)."""
        row = self._connection.execute(
            "SELECT COALESCE(SUM(net_realized_pnl), 0.0) AS total, COUNT(*) AS cnt FROM bridge_completed_trade"
        ).fetchone()
        return float(row["total"]), int(row["cnt"])

    def realized_pnl_for_day(self, trading_day: str) -> tuple[float, int]:
        """Portfolio/Accounting v1, step 2 — same SQL-aggregate discipline
        as `total_realized_pnl()`, filtered to one UTC calendar day.
        `trading_day` MUST be `trading_day_key()`'s exact `"YYYY-MM-DD"`
        format — this reuses that SAME convention rather than inventing a
        second day-boundary definition, by matching the first 10
        characters of the `recorded_at` column (an ISO-8601 string whose
        datetime is always UTC-aware by construction, so its own date
        component is exactly what `trading_day_key()` would compute for
        that same instant).

        WHY `recorded_at`, NOT `exit_timestamp`: `_finalize_exit()`
        passes the EXACT SAME `now` to both `record_completed_trade(...,
        now=now)` (this column) AND `trading_day_key(now)` when updating
        `bridge_daily_risk` — using `recorded_at` here means this
        function's day bucket is anchored to the IDENTICAL moment
        `DailyRiskAccumulator` uses, not the (usually equal, but not
        structurally guaranteed identical) `exit_timestamp` sourced from
        the exchange's own `updated_at`.

        RECONCILING THIS AGAINST `DailyRiskAccumulator.conservative_
        risk_pnl` FOR THE SAME DAY (they are allowed to differ, and here
        is why, so a future reader never has to rediscover this): this
        function's sum is NET (fees subtracted in full, via `lifecycle.
        net_realized_pnl()` at trade-completion time -- possibly `NULL`/
        excluded if any fee's USDT value was unknown) applied to CLOSED
        trades only. `conservative_risk_pnl` is a DELIBERATELY PESSIMISTIC
        risk-breaker accumulator (`compute_trade_risk_contribution()`):
        it subtracts every KNOWN fee AND an additional conservative
        RESERVE for every fee leg whose USDT value could NOT be resolved
        (never simply omits it) -- so `conservative_risk_pnl` is expected
        to run equal to or WORSE (more negative / less positive) than this
        function's true net sum whenever any trade that day had an
        unresolved fee leg, and identical when every fee that day was
        fully known. This is intentional: the risk breaker is deliberately
        pessimistic (fails closed), while this function reports the true,
        undistorted net P&L for observability."""
        row = self._connection.execute(
            "SELECT COALESCE(SUM(net_realized_pnl), 0.0) AS total, COUNT(*) AS cnt "
            "FROM bridge_completed_trade WHERE substr(recorded_at, 1, 10) = ?",
            (trading_day,),
        ).fetchone()
        return float(row["total"]), int(row["cnt"])

    def completed_trades_for_policy_version(self, policy_version_id: str) -> tuple[dict[str, object], ...]:
        """Adaptive Intelligence v1, step 3 — trade/policy attribution: a
        finalized round trip's `policy_version_id` (`None` for every
        trade closed before this milestone, or for a position opened
        without `exit_policy_provider` wired in) survives from the open
        `bridge_position` row through to `bridge_completed_trade`, so a
        champion's REAL Testnet/PAPER evidence can be queried here by
        version — never mixed with discovery/confirmation/shadow
        evidence (those live in `adaptive/`'s own store)."""
        rows = self._connection.execute(
            "SELECT * FROM bridge_completed_trade WHERE policy_version_id = ? ORDER BY recorded_at ASC",
            (policy_version_id,),
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- bridge_daily_risk ----------------------------------------------------

    def load_daily_risk(self, trading_day: str) -> DailyRiskAccumulator:
        row = self._connection.execute(
            "SELECT * FROM bridge_daily_risk WHERE trading_day = ?", (trading_day,)
        ).fetchone()
        if row is None:
            return DailyRiskAccumulator(trading_day=trading_day)
        return DailyRiskAccumulator(
            trading_day=row["trading_day"], conservative_risk_pnl=row["conservative_risk_pnl"],
            trades_counted=row["trades_counted"],
        )

    def save_daily_risk(self, accumulator: DailyRiskAccumulator, *, now: datetime) -> None:
        self._begin()
        try:
            self._save_daily_risk_locked(accumulator, now=now)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_daily_risk yazımı başarısız (trading_day={accumulator.trading_day}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def _save_daily_risk_locked(self, accumulator: DailyRiskAccumulator, *, now: datetime) -> None:
        """Assumes a transaction is ALREADY open — never call directly."""
        self._connection.execute(
            """
            INSERT INTO bridge_daily_risk (trading_day, conservative_risk_pnl, trades_counted, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(trading_day) DO UPDATE SET
                conservative_risk_pnl=excluded.conservative_risk_pnl,
                trades_counted=excluded.trades_counted, updated_at=excluded.updated_at
            """,
            (accumulator.trading_day, accumulator.conservative_risk_pnl, accumulator.trades_counted, now.isoformat()),
        )

    # -- Atomic composite writes (CRITICAL FIX M1) ---------------------------

    def save_position_with_fee_entries(
        self, record: BridgePositionRecord, *, trade_group_id: str, fee_entries: tuple[FeeLedgerEntry, ...]
    ) -> None:
        """Atomically persists a `bridge_position` row together with its
        fee-ledger entries, in ONE transaction — used by `on_entry_filled()`
        (lifecycle_manager.py) so a crash between the two writes can never
        leave fee-ledger rows for a position that was never actually saved,
        or vice versa."""
        self._begin()
        try:
            self._save_position_locked(record)
            self._append_fee_entries_locked(record.symbol, trade_group_id, fee_entries)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"bridge_position+fee_ledger atomik kaydı başarısız (symbol={record.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()

    def finalize_exit(
        self,
        *,
        new_position: BridgePositionRecord,
        trade_group_id: str,
        fee_entries: tuple[FeeLedgerEntry, ...],
        completed_trade: dict[str, object],
        daily_risk: DailyRiskAccumulator,
        daily_risk_now: datetime,
    ) -> None:
        """CRITICAL FIX (M1 — mainnet-readiness review): atomically performs
        ALL FOUR writes a closed trade requires — fee-ledger append,
        completed-trade record, daily-risk accumulator update, and the
        final `bridge_position` save — in ONE transaction. `_finalize_exit()`
        (lifecycle_manager.py) previously made these as four SEPARATE store
        calls; under the (broken) `with self._connection:` no-op discipline
        described above, a crash between any two of them could leave
        cross-table state inconsistent (e.g. a trade recorded as completed
        while `bridge_position` never actually left LONG). `completed_trade`
        holds exactly `record_completed_trade()`'s own keyword arguments
        (minus `self`) — passed through unchanged, never a second,
        divergent schema."""
        self._begin()
        try:
            self._append_fee_entries_locked(new_position.symbol, trade_group_id, fee_entries)
            self._record_completed_trade_locked(**completed_trade)
            self._save_daily_risk_locked(daily_risk, now=daily_risk_now)
            self._save_position_locked(new_position)
        except sqlite3.Error as exc:
            self._rollback()
            raise PersistenceError(f"finalize_exit atomik kaydı başarısız (symbol={new_position.symbol}): {exc}") from exc
        except Exception:
            self._rollback()
            raise
        else:
            self._commit()


=== FILE: crypto_signal_engine/execution/models.py ===
"""
Faz 10 — domain intent modelleri (Binance transport temsili İLE
KARIŞTIRILMAZ — bkz. `testnet_client.py`'nin kendi wire-format kodlaması).

`ExecutionMode`: yalnızca `PAPER` ve `BINANCE_SPOT_TESTNET` GEÇERLİ
değerlerdir. `MAINNET` KASITLI OLARAK bir enum üyesi DEĞİLDİR — Mainnet'i
"reddetme" testleri için temsil etmek gerekirse, `UNSUPPORTED_EXECUTION_MODES`
İÇİNDEKİ bir string kullanılır (asla geçerli bir `ExecutionMode` üyesi
olamaz, `require_enum` onu HER ZAMAN reddeder)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_finite, require_utc_aware


class ExecutionMode(str, Enum):
    """Faz 10'un yalnızca İKİ geçerli çalışma modu. `ALLOW_LIVE_TRADING`
    hiçbir zaman bu enum'la BAĞLANTILANMAZ/repurpose EDİLMEZ — o bayrak
    `False` olarak KALIR, bu enum yalnızca TESTNET/PAPER ayrımını taşır."""

    PAPER = "PAPER"
    BINANCE_SPOT_TESTNET = "BINANCE_SPOT_TESTNET"


# Mainnet/futures/margin, KASITLI OLARAK `ExecutionMode`'un bir üyesi
# DEĞİLDİR. Bu küme yalnızca negative-test amaçlıdır (bkz.
# tests/test_execution_models.py — "reddedilmesi gereken" değerler).
UNSUPPORTED_EXECUTION_MODES: frozenset[str] = frozenset(
    {"MAINNET", "BINANCE_SPOT_MAINNET", "BINANCE_FUTURES", "BINANCE_MARGIN", "BINANCE_MARGIN_ISOLATED"}
)


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """Faz 10 KASITLI OLARAK dar tutulur — yalnızca execution sınırını
    doğrulamak için yeterli iki tip. Tam bir exchange OMS İNŞA EDİLMEZ."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(str, Enum):
    GTC = "GTC"


_CLIENT_ORDER_ID_PREFIX = "csl-"
_CLIENT_ORDER_ID_MAX_LENGTH = 36  # Binance `newClientOrderId` sınırı


def _fmt_numeric(value: float | None) -> str:
    return "" if value is None else repr(value)


def _derive_client_order_id(
    symbol: str, side: OrderSide, order_type: OrderType, quantity: float | None,
    quote_quantity: float | None, price: float | None, time_in_force: TimeInForce | None, context_id: str,
) -> str:
    """`context_id` + intent'in TÜM ekonomik olarak anlamlı alanlarından
    deterministik bir client-order-id türetir: AYNI intent HER ZAMAN AYNI
    id'yi üretir; herhangi bir alan FARKLIYSA id de FARKLIDIR (SHA-256
    çakışma direnci). Bir retry/restart, bu yüzden yanlışlıkla YENİ/ilgisiz
    bir order kimliği ÜRETMEZ (bkz. modül docstring'i — tam exactly-once
    iddiası Faz 11'e aittir, bu yalnızca KİMLİK temelini kurar)."""
    payload = "|".join(
        [
            symbol, side.value, order_type.value,
            _fmt_numeric(quantity), _fmt_numeric(quote_quantity), _fmt_numeric(price),
            time_in_force.value if time_in_force is not None else "",
            context_id,
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    client_order_id = f"{_CLIENT_ORDER_ID_PREFIX}{digest}"
    assert len(client_order_id) <= _CLIENT_ORDER_ID_MAX_LENGTH  # yapısal invariant, asla ihlal edilemez
    return client_order_id


@dataclass(frozen=True)
class OrderIntent:
    """Bir TESTNET order'ının, henüz Binance wire-format'ına ÇEVRİLMEMİŞ,
    doğrulanmış ve immutable temsili. `client_order_id`, KASITLI OLARAK
    constructor argümanı DEĞİLDİR — her zaman diğer alanlardan deterministik
    olarak TÜRETİLİR (bkz. `_derive_client_order_id`)."""

    symbol: str
    side: OrderSide
    order_type: OrderType
    context_id: str
    timestamp: datetime
    quantity: float | None = None
    quote_quantity: float | None = None
    price: float | None = None
    time_in_force: TimeInForce | None = None
    client_order_id: str = field(init=False, default="")

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, OrderSide, "side")
        require_enum(self.order_type, OrderType, "order_type")
        require_utc_aware(self.timestamp, "timestamp")
        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz")

        if self.order_type is OrderType.MARKET:
            if self.quantity is None and self.quote_quantity is None:
                raise ValueError("MARKET order için quantity veya quote_quantity gerekli")
            if self.quantity is not None and self.quote_quantity is not None:
                raise ValueError("MARKET order için quantity VE quote_quantity AYNI ANDA verilemez")
            if self.price is not None:
                raise ValueError("MARKET order için price verilemez")
            if self.time_in_force is not None:
                raise ValueError("MARKET order için time_in_force verilemez")
        else:  # LIMIT
            if self.quantity is None:
                raise ValueError("LIMIT order için quantity zorunlu")
            if self.quote_quantity is not None:
                raise ValueError("LIMIT order için quote_quantity desteklenmez")
            if self.price is None:
                raise ValueError("LIMIT order için price zorunlu")
            if self.time_in_force is None:
                object.__setattr__(self, "time_in_force", TimeInForce.GTC)
            require_enum(self.time_in_force, TimeInForce, "time_in_force")

        for value, name in (
            (self.quantity, "quantity"), (self.quote_quantity, "quote_quantity"), (self.price, "price"),
        ):
            if value is not None:
                require_finite(value, name)
                if value <= 0:
                    raise ValueError(f"{name} pozitif olmalı, alınan: {value}")

        object.__setattr__(
            self,
            "client_order_id",
            _derive_client_order_id(
                self.symbol, self.side, self.order_type, self.quantity,
                self.quote_quantity, self.price, self.time_in_force, self.context_id,
            ),
        )


@dataclass(frozen=True)
class Fill:
    """One authoritative per-fill line from Binance's `fills[]` array
    (present on a MARKET/LIMIT order's FULL response, which is the default
    `newOrderRespType` for both order types) or a backfilled `myTrades`
    row. `commission_asset` is read per-fill — never assumed constant for
    a runtime, since BNB fee-discount eligibility can change fill-to-fill
    (see autonomous-Testnet-lifecycle Phase 5)."""

    price: float
    quantity: float
    commission: float
    commission_asset: str
    trade_id: int | None = None

    def __post_init__(self) -> None:
        require_finite(self.price, "price")
        require_finite(self.quantity, "quantity")
        require_finite(self.commission, "commission")
        if self.price <= 0:
            raise ValueError(f"Fill.price pozitif olmalı, alınan: {self.price}")
        if self.quantity <= 0:
            raise ValueError(f"Fill.quantity pozitif olmalı, alınan: {self.quantity}")
        if self.commission < 0:
            raise ValueError(f"Fill.commission negatif olamaz, alınan: {self.commission}")
        if not self.commission_asset.strip():
            raise ValueError("Fill.commission_asset boş olamaz")


@dataclass(frozen=True)
class ExecutionResult:
    """TESTNET'in `place_order` yanıtının doğrulanmış, minimal temsili.
    Ham/doğrulanmamış Binance JSON'u uygulama genelinde SIZDIRILMAZ (bkz.
    `adapter.py` — `raw_response` yalnızca secret-safe alanlar İÇERİR,
    diagnostik amaçlı, opsiyonel).

    `fills`: additive (default `()`), populated from the initial POST
    response's `fills[]` array when present (MARKET/LIMIT default to a
    FULL response). A `query_order` (GET) response never carries `fills`
    (Binance does not return them there) — callers needing fee data for a
    reconciliation-recovered fill must backfill via `myTrades()` instead
    (see autonomous-Testnet-lifecycle Phase 0-C/5)."""

    symbol: str
    client_order_id: str
    exchange_order_id: int
    side: OrderSide
    status: str
    executed_quantity: float
    cumulative_quote_quantity: float
    transaction_time: datetime
    context_id: str
    fills: tuple[Fill, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, OrderSide, "side")
        require_utc_aware(self.transaction_time, "transaction_time")
        require_finite(self.executed_quantity, "executed_quantity")
        require_finite(self.cumulative_quote_quantity, "cumulative_quote_quantity")
        if self.executed_quantity < 0:
            raise ValueError("executed_quantity negatif olamaz")
        if self.cumulative_quote_quantity < 0:
            raise ValueError("cumulative_quote_quantity negatif olamaz")
        if not self.status.strip():
            raise ValueError("status boş olamaz")
        if not self.client_order_id.strip():
            raise ValueError("client_order_id boş olamaz")
        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz")


=== FILE: crypto_signal_engine/execution/reconciliation_models.py ===
"""
Faz 11 — execution lifecycle state modeli ve durable `ExecutionRecord`.

Bölüm ("No fake exactly-once claim"): bu modül, Binance TESTNET'e karşı
"effectively-once intent handling under tested reconciliation conditions"
sağlar — matematiksel exactly-once İDDİA ETMEZ. Belirsiz bir ağ sonucu
(timeout/bağlantı kopması) ASLA "kesinlikle başarısız" ya da "kesinlikle
başarılı" ya da "sessizce yeniden gönderilmesi güvenli" olarak
YORUMLANMAZ — `AMBIGUOUS` durumuna düşer ve stabil `client_order_id`
üzerinden exchange'e SORULUR (bkz. `reconciliation_service.py`)."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_finite, require_utc_aware
from crypto_signal_engine.execution.errors import ImpossibleLifecycleTransitionError, ReconciliationContradictionError
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType

_QUANTITY_REGRESSION_TOLERANCE = 1e-12


class ExecutionLifecycleState:
    """Faz 11 yerel lifecycle durumları (bkz. modül docstring'i — exact
    enum İSMİ önemli değil, İMKANSIZ karışıklıkların engellenmesi önemli).
    `str` alt sınıfı DEĞİL, KASITLI OLARAK düz string sabitleri — SQLite'a
    doğrudan yazılabilir/okunabilir, `Enum` üyelik kontrolü hâlâ
    `require_enum` ile YAPILIR (bkz. aşağı)."""

    INTENT_CREATED = "INTENT_CREATED"
    SUBMISSION_ATTEMPTED = "SUBMISSION_ATTEMPTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    AMBIGUOUS = "AMBIGUOUS"
    UNKNOWN_NOT_FOUND = "UNKNOWN_NOT_FOUND"

    ALL = frozenset(
        {
            INTENT_CREATED, SUBMISSION_ATTEMPTED, ACKNOWLEDGED, PARTIALLY_FILLED, FILLED,
            CANCELED, EXPIRED, REJECTED, AMBIGUOUS, UNKNOWN_NOT_FOUND,
        }
    )


TERMINAL_STATES = frozenset(
    {
        ExecutionLifecycleState.FILLED, ExecutionLifecycleState.CANCELED,
        ExecutionLifecycleState.EXPIRED, ExecutionLifecycleState.REJECTED,
    }
)

# Reconciliation-sweep'in (`reconcile_pending()`) tekrar SORGULAMASI gereken
# durumlar — terminal DEĞİL, hâlâ "çözülmemiş" durumlar.
#
# BLOCKER FİX (Karar 78 — bağımsız acceptance review bulgusu):
# `UNKNOWN_NOT_FOUND` KASITLI OLARAK burada YER ALIR. Bu durumun anlamı
# "exchange sorgusu bu reconciliation denemesinde order'ı BULAMADI" DEMEKTİR
# — "orijinal ambiguous POST'un Binance tarafından HİÇ kabul edilmediği
# KANITLANMIŞTIR, yeniden gönderim GÜVENLİDİR" DEMEK DEĞİLDİR. Tek bir
# anlık -2013 yanıtı, GERÇEKTEN Binance'e ULAŞMIŞ ama HENÜZ tam olarak
# işlenmemiş/gecikmeli görünür hale gelmiş bir order'ı DIŞLAMAZ. Bu yüzden
# `UNKNOWN_NOT_FOUND` HİÇBİR ZAMAN "otomatik yeniden gönderim güvenlidir"
# anlamına GELMEZ — reconciliation-eligible KALIR (bkz. `submit()`'in
# `reconciliation_service.py`'deki KENDİSİ: bu durumdaki bir kayıt İÇİN
# ASLA otomatik ikinci bir POST YAPILMAZ, yalnızca YENİDEN sorgulanır).
NEEDS_RECONCILIATION_STATES = frozenset(
    {
        ExecutionLifecycleState.SUBMISSION_ATTEMPTED, ExecutionLifecycleState.AMBIGUOUS,
        ExecutionLifecycleState.ACKNOWLEDGED, ExecutionLifecycleState.PARTIALLY_FILLED,
        ExecutionLifecycleState.UNKNOWN_NOT_FOUND,
    }
)

_BINANCE_STATUS_TO_LIFECYCLE: dict[str, str] = {
    "NEW": ExecutionLifecycleState.ACKNOWLEDGED,
    "PENDING_CANCEL": ExecutionLifecycleState.ACKNOWLEDGED,
    "PARTIALLY_FILLED": ExecutionLifecycleState.PARTIALLY_FILLED,
    "FILLED": ExecutionLifecycleState.FILLED,
    "CANCELED": ExecutionLifecycleState.CANCELED,
    "REJECTED": ExecutionLifecycleState.REJECTED,
    "EXPIRED": ExecutionLifecycleState.EXPIRED,
    "EXPIRED_IN_MATCH": ExecutionLifecycleState.EXPIRED,
}


def lifecycle_state_from_binance_status(status: str) -> str:
    """Binance'in KENDİ order status string'ini (`NEW`/`PARTIALLY_FILLED`/
    `FILLED`/...) yerel `ExecutionLifecycleState`'e çevirir. Bilinmeyen bir
    status SESSİZCE bir varsayılana DÜŞMEZ — açıkça reddedilir (yeni bir
    Binance status'u eklendiyse, bu KASITLI bir kod değişikliği
    GEREKTİRİR)."""
    try:
        return _BINANCE_STATUS_TO_LIFECYCLE[status]
    except KeyError as exc:
        raise ImpossibleLifecycleTransitionError(f"bilinmeyen Binance order status'u: {status!r}") from exc


@dataclass(frozen=True)
class ExecutionRecord:
    """Bir TESTNET execution intent'inin durable, immutable yerel gerçeği.
    `context_id` PRIMARY KEY'dir (bkz. `reconciliation_store.py`) — Faz 5'in
    `Signal.context_id` idempotency ilkesiyle AYNI ruhtadır."""

    context_id: str
    symbol: str
    client_order_id: str
    side: OrderSide
    order_type: OrderType
    quantity: float | None
    quote_quantity: float | None
    price: float | None
    lifecycle_state: str
    exchange_order_id: int | None
    executed_quantity: float
    cumulative_quote_quantity: float
    detail: str
    created_at: datetime
    updated_at: datetime
    last_reconciled_at: datetime | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, OrderSide, "side")
        require_enum(self.order_type, OrderType, "order_type")
        if self.lifecycle_state not in ExecutionLifecycleState.ALL:
            raise ValueError(f"bilinmeyen lifecycle_state: {self.lifecycle_state!r}")
        require_utc_aware(self.created_at, "created_at")
        require_utc_aware(self.updated_at, "updated_at")
        if self.last_reconciled_at is not None:
            require_utc_aware(self.last_reconciled_at, "last_reconciled_at")
        require_finite(self.executed_quantity, "executed_quantity")
        require_finite(self.cumulative_quote_quantity, "cumulative_quote_quantity")
        if self.executed_quantity < 0:
            raise ValueError("executed_quantity negatif olamaz")
        if self.cumulative_quote_quantity < 0:
            raise ValueError("cumulative_quote_quantity negatif olamaz")
        if not self.context_id.strip():
            raise ValueError("context_id boş olamaz")
        if not self.client_order_id.strip():
            raise ValueError("client_order_id boş olamaz")

    def is_terminal(self) -> bool:
        return self.lifecycle_state in TERMINAL_STATES

    def needs_reconciliation(self) -> bool:
        return self.lifecycle_state in NEEDS_RECONCILIATION_STATES


def new_record(intent: OrderIntent, *, now: datetime) -> ExecutionRecord:
    """Bir intent'ten TAZE bir `ExecutionRecord` üretir — `INTENT_CREATED`
    durumunda, hiçbir exchange etkileşimi henüz OLMADAN."""
    return ExecutionRecord(
        context_id=intent.context_id,
        symbol=intent.symbol,
        client_order_id=intent.client_order_id,
        side=intent.side,
        order_type=intent.order_type,
        quantity=intent.quantity,
        quote_quantity=intent.quote_quantity,
        price=intent.price,
        lifecycle_state=ExecutionLifecycleState.INTENT_CREATED,
        exchange_order_id=None,
        executed_quantity=0.0,
        cumulative_quote_quantity=0.0,
        detail="",
        created_at=now,
        updated_at=now,
        last_reconciled_at=None,
    )


def transition(record: ExecutionRecord, *, new_state: str, now: datetime, detail: str = "") -> ExecutionRecord:
    """Exchange etkileşimi OLMAYAN saf bir yerel state geçişi (örn.
    `SUBMISSION_ATTEMPTED`, `AMBIGUOUS`, `REJECTED`) — miktar/exchange-id
    DEĞİŞMEZ. Terminal bir durumdan BAŞKA bir duruma geçiş İMKANSIZDIR."""
    if record.lifecycle_state in TERMINAL_STATES and new_state != record.lifecycle_state:
        raise ReconciliationContradictionError(
            f"{record.context_id}: terminal durum {record.lifecycle_state!r} -> {new_state!r} imkansız regresyon"
        )
    return replace(record, lifecycle_state=new_state, detail=detail, updated_at=now)


def assert_monotonic(current: ExecutionRecord, incoming: ExecutionRecord) -> None:
    """`current` (şu anda GERÇEKTEN geçerli/durable kabul edilen kayıt) ile
    `incoming` (yazılmak İSTENEN aday) arasında İMKANSIZ bir regresyon
    olmadığını doğrular — `exchange_order_id` değişimi, `executed_quantity`
    azalması, VEYA terminal bir durumdan FARKLI bir duruma geçiş.

    BLOCKER FİX (HIGH-5 — "concurrent submit aynı deterministic
    clientOrderId ile local durable execution truth'u corrupt edebilir,
    örn. FILLED -> REJECTED"): bu fonksiyon HEM `apply_exchange_truth()`
    (in-memory, TEK bir çağrının kendi elindeki `record` nesnesine karşı)
    HEM `ExecutionStateStore.save()` (durable, `BEGIN IMMEDIATE` altında
    DİSKTEKİ GÜNCEL satıra karşı, bkz. reconciliation_store.py) tarafından
    AYNI, TEK bir kaynaktan kullanılır. Kritik fark: `apply_exchange_truth()`
    yalnızca ÇAĞIRANIN KENDİ (potansiyel olarak BAYAT) in-memory kopyasını
    korur — iki EŞZAMANLI çağrının HER BİRİ kendi bayat kopyasından
    "geçerli" bir geçiş üretebilir (örn. ikisi de SUBMISSION_ATTEMPTED'tan
    başlar, biri FILLED'e biri REJECTED'e gider) ve `save()` bunları KÖRÜ
    KÖRÜNE üst üste yazardı. Store seviyesindeki bu AYNI kontrol, DİSKTEKİ
    GERÇEK güncel durumu (bir ÖNCEKİ eşzamanlı yazarın SONUCUNU) baz alarak
    ikinci (kaybeden) yazarı AÇIKÇA reddeder — kaybeden taraf sonra
    otoriter gerçeği YENİDEN OKUR (bkz. `ExecutionReconciliationService.
    _save_or_defer_to_winner()`)."""
    if (
        current.exchange_order_id is not None
        and incoming.exchange_order_id is not None
        and current.exchange_order_id != incoming.exchange_order_id
    ):
        raise ReconciliationContradictionError(
            f"{current.context_id}: exchange_order_id değişti "
            f"{current.exchange_order_id!r} -> {incoming.exchange_order_id!r} (client_order_id={current.client_order_id})"
        )
    if incoming.executed_quantity < current.executed_quantity - _QUANTITY_REGRESSION_TOLERANCE:
        raise ReconciliationContradictionError(
            f"{current.context_id}: executed_quantity azaldı "
            f"{current.executed_quantity} -> {incoming.executed_quantity} (client_order_id={current.client_order_id})"
        )
    if current.lifecycle_state in TERMINAL_STATES and incoming.lifecycle_state != current.lifecycle_state:
        raise ReconciliationContradictionError(
            f"{current.context_id}: terminal durum {current.lifecycle_state!r} -> {incoming.lifecycle_state!r} "
            f"imkansız regresyon (client_order_id={current.client_order_id})"
        )


def apply_exchange_truth(
    record: ExecutionRecord,
    *,
    new_state: str,
    exchange_order_id: int | None,
    executed_quantity: float,
    cumulative_quote_quantity: float,
    now: datetime,
    detail: str = "",
) -> ExecutionRecord:
    """Exchange'den (submit sonucu VEYA query sonucu) dönen GERÇEĞİ yerel
    record'a uygular — exchange SEMBOL için OTORİTERDİR, ama İMKANSIZ
    regresyonlar (bkz. `assert_monotonic` docstring'i) SESSİZCE kabul
    EDİLMEZ, AÇIKÇA `ReconciliationContradictionError` fırlatılır."""
    candidate = replace(
        record,
        lifecycle_state=new_state,
        exchange_order_id=exchange_order_id if exchange_order_id is not None else record.exchange_order_id,
        executed_quantity=executed_quantity,
        cumulative_quote_quantity=cumulative_quote_quantity,
        detail=detail,
        updated_at=now,
        last_reconciled_at=now,
    )
    assert_monotonic(record, candidate)
    return candidate


=== FILE: crypto_signal_engine/execution/reconciliation_service.py ===
"""
Faz 11 — `ExecutionReconciliationService`: Faz 10'un TESTNET execution
sınırına, stabil kimlik (`client_order_id`/`context_id`) üzerinden
reconciliation ekleyen TEK yüksek-seviye giriş noktası.

ÇEKİRDEK TASARIM İLKESİ (bkz. modül-üstü PHASE11 dokümantasyonu): bir
execution denemesinden SONRA, yerel sistem KÖRÜ KÖRÜNE GÜVENMEZ:
- HTTP yanıtına (timeout/bağlantı kopması ANLAMSIZLIK yaratır),
- yerel intent state'ine (bir restart bunu KAYBEDEBİLİR),
- önceki process belleğine (bir restart bunu SIFIRLAR).

Exchange, exchange-execution state için OTORİTERDİR. Yerel state, stabil
`client_order_id`/exchange order kimliği üzerinden TESTNET'e karşı
reconcile edilir.

Bu, `scripts/binance_testnet_lab.py`'nin (ve YALNIZCA onun) kullandığı
servistir — `RuntimeCoordinator`/`SignalEngine`/`PaperTradingEngine`
bundan HABERDAR DEĞİLDİR, otomatik bir Signal->TESTNET-order yolu YOKTUR."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.adapter import TestnetExecutionAdapter
from crypto_signal_engine.execution.errors import (
    BinanceRejectionError,
    ExecutionIdempotencyConflictError,
    ExecutionPersistenceError,
    ExecutionTransportError,
    LocalExecutionRecordNotFoundError,
    MalformedResponseError,
    OrderNotFoundError,
    ReconciliationContradictionError,
)
from crypto_signal_engine.execution.models import ExecutionResult, OrderIntent
from crypto_signal_engine.execution.reconciliation_models import (
    ExecutionLifecycleState,
    ExecutionRecord,
    apply_exchange_truth,
    lifecycle_state_from_binance_status,
    new_record,
    transition,
)
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient
from crypto_signal_engine.portfolio.accounting import AssetBalance, parse_account_balances
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock


class ExecutionReconciliationService:
    """Faz 11'in tek yüksek-seviye giriş noktası: `submit()` + `reconcile()`
    + `reconcile_pending()`. Bu mantık BİLEREK CLI'ye veya
    `RuntimeCoordinator`'a DOĞRUDAN GÖMÜLMEZ (bkz. modül docstring'i)."""

    def __init__(self, client: BinanceTestnetClient, store: ExecutionStateStore, *, clock: Clock | None = None) -> None:
        self._client = client
        self._adapter = TestnetExecutionAdapter(client)
        self._store = store
        self._clock = clock or SystemClock()
        # BLOCKER FİX (HIGH-5): AYNI context_id için `submit()`/`reconcile()`
        # çağrılarını (bu process İÇİNDE) SERİLEŞTİRİR — iki eşzamanlı
        # `submit()` çağrısının İKİSİNİN DE `place_order()`'a ULAŞMASINI
        # (ve böylece Binance'e AYNI clientOrderId ile İKİ GERÇEK POST
        # göndermesini) YAPISAL OLARAK engeller. Kilit YALNIZCA bu process
        # içindir — çapraz-process/çapraz-restart güvenliği AYRICA
        # `ExecutionStateStore.save()`'in `BEGIN IMMEDIATE` + monotonik
        # doğrulaması tarafından sağlanır (bkz. reconciliation_store.py).
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def client(self) -> BinanceTestnetClient:
        """Faz 13 (Signal->TESTNET bridge) — salt-okunur erişim: bridge,
        KENDİ `TestnetExecutionAdapter`'ını (SELL miktarını exchange
        filtrelerine göre hizalamak için `exchangeInfo` sorgulamak amacıyla)
        bu SERVİSİN ZATEN sahip olduğu AYNI, tek istemci örneğini yeniden
        kullanır — ikinci bir bağlantı/konfigürasyon İCAT EDİLMEZ. Bu
        property mutasyon YAPMAZ, hiçbir credential DEĞERİ döndürmez
        (istemcinin kendisi zaten secret'ları hiçbir yerde loglamaz/
        yazdırmaz, bkz. `BinanceTestnetConfig.__repr__`)."""
        return self._client

    def _now(self) -> datetime:
        return self._clock.now()

    def _lock_for(self, context_id: str) -> asyncio.Lock:
        lock = self._locks.get(context_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[context_id] = lock
        return lock

    def _save_or_defer_to_winner(self, record: ExecutionRecord) -> ExecutionRecord:
        """`store.save(record)` dener. Store, BAŞKA bir eşzamanlı yazarın
        (aynı process İÇİNDE kilit atlanmışsa, VEYA — daha olası —
        TAMAMEN FARKLI bir process/CLI çağrısının) ZATEN daha güçlü/
        terminal bir gerçeği commit ETTİĞİNİ tespit ederse
        `ReconciliationContradictionError` fırlatır: bu durumda KENDİ
        (kaybeden) sonucumuzu SESSİZCE ÜZERİNE YAZMAYA ÇALIŞMAYIZ —
        OTORİTER (kazanan, ZATEN durable) gerçeği YENİDEN OKUYUP döneriz
        (HIGH-5 fix — bkz. reconciliation_store.py::save() docstring'i,
        "FILLED -> REJECTED" senaryosunun tam olarak burada kapandığı
        nokta)."""
        try:
            self._store.save(record)
            return record
        except ReconciliationContradictionError:
            authoritative = self._store.load_by_context_id(record.context_id)
            if authoritative is None:
                raise  # teorik olarak imkansız (çakışma için satır zaten VARdı) — fail-closed
            return authoritative

    # -- Submission (idempotent, ambiguity-safe, concurrency-safe) ---------------

    async def submit(self, intent: OrderIntent) -> ExecutionRecord:
        """Faz 11'in ana giriş noktası. Aynı `context_id` için:
        - zaten TERMİNAL bir kayıt VARSA: onu döner, YENİDEN POST YAPMAZ.
        - TERMİNAL OLMAYAN (`SUBMISSION_ATTEMPTED`, `AMBIGUOUS`,
          `ACKNOWLEDGED`, `PARTIALLY_FILLED`, `UNKNOWN_NOT_FOUND` DAHİL)
          herhangi bir kayıt VARSA: KÖRÜ KÖRÜNE yeniden göndermek YERİNE
          SADECE exchange'e yeniden SORAR (bkz. `_reconcile_record`) — BU
          FONKSİYONDA `place_order()`'a giden İKİNCİ bir kod yolu YOKTUR.
        - hiç kayıt YOKSA: YENİ bir kayıt oluşturur (TEK POST kod yolu).

        BLOCKER FİX (HIGH-5 — "concurrent submit aynı deterministic
        clientOrderId ile local durable truth'u corrupt edebilir"): TÜM
        gövde, `context_id` başına bir `asyncio.Lock` ALTINDA çalışır —
        AYNI context_id için eşzamanlı ikinci bir `submit()` çağrısı,
        BİRİNCİSİ TAMAMEN bitene (durable state'i içerecek şekilde) kadar
        BLOKLANIR; kilit serbest kaldığında ARTIK yukarıdaki "zaten
        TERMİNAL" / "TERMİNAL DEĞİL, yeniden sorgula" dallarından birine
        girer — ASLA `place_order()`'a İKİNCİ bir kez ULAŞMAZ. Her `save()`
        çağrısı AYRICA `_save_or_defer_to_winner()` üzerinden geçer —
        bu, TAMAMEN FARKLI bir process'in AYNI context_id'ye eşzamanlı
        yazdığı (bu kilidin GÖREMEDİĞİ) durumu da güvenli kılar.

        BLOCKER FİX (Karar 78 — bağımsız acceptance review bulgusu):
        `UNKNOWN_NOT_FOUND` ARTIK "yeniden gönderim güvenlidir" ANLAMINA
        GELMEZ — bir tek anlık -2013 yanıtı, orijinal ambiguous POST'un
        Binance tarafından HİÇ kabul edilmediğini KANITLAMAZ (yalnızca "BU
        reconciliation denemesinde bulunamadı" demektir). Bu durumdaki bir
        kayıt için `submit()` SADECE yeniden sorgular, ASLA otomatik ikinci
        bir POST YAPMAZ (bkz. PHASE11 doc, "Ambiguity Policy"). Otomatik
        operatör-onaylı yeniden gönderim politikası KASITLI OLARAK Faz
        11'in kapsamı DIŞINDA bırakılmıştır.

        Aynı `context_id` FARKLI bir `client_order_id` ile (yani FARKLI
        ekonomi ile) tekrar kullanılırsa, `ExecutionIdempotencyConflictError`
        AÇIKÇA fırlatılır (Faz 5'in `IdempotencyConflictError`'ıyla AYNI
        ilke)."""
        async with self._lock_for(intent.context_id):
            existing = self._store.load_by_context_id(intent.context_id)
            if existing is not None:
                if existing.client_order_id != intent.client_order_id:
                    raise ExecutionIdempotencyConflictError(
                        f"context_id={intent.context_id} daha önce client_order_id={existing.client_order_id} "
                        f"ile persist edildi, şimdi FARKLI bir client_order_id={intent.client_order_id} ile "
                        f"tekrar kullanılmaya çalışıldı — idempotency ihlali"
                    )
                if existing.is_terminal():
                    return existing  # idempotent replay: YENİ bir POST YOK
                # TERMİNAL OLMAYAN HER durum (UNKNOWN_NOT_FOUND DAHİL) YALNIZCA
                # yeniden sorgulanır — bu fonksiyonda BAŞKA hiçbir dal
                # `place_order()`'a ULAŞMAZ.
                return await self._reconcile_record(existing)

            record = new_record(intent, now=self._now())

            # Faz 10 filtre/fiyat doğrulaması — network gerektirir ama AMBIGUOUS
            # DEĞİLDİR (yalnızca GET, side-effect-free); başarısız olursa hiçbir
            # durable "attempted" checkpoint'i YAZILMADAN doğrudan yayılır.
            await self._adapter.validate_intent(intent)

            record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=self._now())
            try:
                record = self._save_or_defer_to_winner(record)
            except PersistenceError as exc:
                # SAFE: place_order() HENÜZ hiç çağrılmadı — exchange'e
                # HİÇBİR ŞEY gönderilmedi. exchange_may_have_accepted_order
                # BİLİNÇLİ OLARAK False (varsayılan) — H3 fix, bkz. errors.py.
                raise ExecutionPersistenceError(
                    f"context_id={intent.context_id} (client_order_id={intent.client_order_id}) için submission "
                    f"ÖNCESİ yerel persistence BAŞARISIZ oldu — order GÖNDERİLMEDİ (fail-closed)",
                    client_order_id=intent.client_order_id, context_id=intent.context_id,
                    exchange_may_have_accepted_order=False,
                ) from exc
            if record.lifecycle_state != ExecutionLifecycleState.SUBMISSION_ATTEMPTED:
                # Bir BAŞKA (farklı process'teki) eşzamanlı submit bu
                # context_id'yi BİZDEN ÖNCE ZATEN ilerletti (bkz.
                # `_save_or_defer_to_winner`) — KÖRÜ KÖRÜNE `place_order()`'a
                # DEVAM ETMEYİZ, otoriter (kazanan) gerçeği döneriz.
                return record

            try:
                result = await self._client.place_order(intent)
            except (ExecutionTransportError,) as exc:
                # Timeout dahil (ExecutionTimeoutError, ExecutionTransportError'ı
                # GENİŞLETİR) — Binance'in order'ı GERÇEKTEN alıp almadığı
                # BİLİNMİYOR. KÖRÜ KÖRÜNE yeni bir kimlikle YENİDEN GÖNDERİLMEZ;
                # ANINDA reconciliation DENENİR (stabil client_order_id ile).
                record = transition(
                    record, new_state=ExecutionLifecycleState.AMBIGUOUS, now=self._now(), detail=str(exc)
                )
                record = self._safe_save(record)
                if record.is_terminal():
                    return record
                return await self._reconcile_record(record)
            except BinanceRejectionError as exc:
                record = transition(
                    record, new_state=ExecutionLifecycleState.REJECTED, now=self._now(), detail=str(exc)
                )
                return self._safe_save(record)

            record = self._apply_result(record, result)
            try:
                return self._save_or_defer_to_winner(record)
            except PersistenceError as exc:
                # DANGEROUS: place_order() ZATEN BAŞARIYLA döndü — exchange
                # bu order'ı KABUL ETTİ (muhtemelen doldurdu), ama bunu
                # yerel diske YAZAMADIK. exchange_may_have_accepted_order=
                # True — H3 fix: çağıranlar bu durumu "güvenle yeniden
                # denenebilir" ile ASLA karıştırmamalı (bkz. errors.py).
                raise ExecutionPersistenceError(
                    f"TESTNET order KABUL ETTİ (client_order_id={intent.client_order_id}, "
                    f"exchange_order_id={result.exchange_order_id}, status={result.status}) ama YEREL "
                    f"PERSISTENCE BAŞARISIZ oldu — `reconcile --client-order-id {intent.client_order_id}` ile "
                    f"persistence düzeldiğinde manuel doğrulayın",
                    client_order_id=intent.client_order_id, context_id=intent.context_id,
                    exchange_may_have_accepted_order=True,
                ) from exc

    def _safe_save(self, record: ExecutionRecord) -> ExecutionRecord:
        """Best-effort GÜVENLİ kayıt: bir ÖNCEKİ (`SUBMISSION_ATTEMPTED`)
        satır zaten durable olduğundan, bir I/O `PersistenceError` SESSİZCE
        yutulur (stabil kimlik KAYBOLMAZ — yalnızca bu ARA GÜNCELLEME
        kaybolur, bir sonraki `reconcile_pending()` sweep'i durumu YİNE DE
        düzeltir) VE ÇAĞIRANIN KENDİ `record`'u döner.

        BLOCKER FİX (HIGH-5): `ReconciliationContradictionError` ARTIK
        SESSİZCE yutulmaz — `_save_or_defer_to_winner` üzerinden OTORİTER
        (ZATEN durable, muhtemelen terminal) gerçek YENİDEN OKUNUP döner.
        Bu, TAM OLARAK "FILLED -> REJECTED" regresyon senaryosunun
        kapandığı yerdir: bir BAŞKA eşzamanlı çağrı bu context_id'yi ZATEN
        FILLED'e taşımışsa, bu çağrı kendi REJECTED sonucunu ASLA üzerine
        YAZMAZ — FILLED'i döner."""
        try:
            return self._save_or_defer_to_winner(record)
        except PersistenceError:
            return record

    def _apply_result(self, record: ExecutionRecord, result: ExecutionResult) -> ExecutionRecord:
        new_state = lifecycle_state_from_binance_status(result.status)
        return apply_exchange_truth(
            record, new_state=new_state, exchange_order_id=result.exchange_order_id,
            executed_quantity=result.executed_quantity, cumulative_quote_quantity=result.cumulative_quote_quantity,
            now=self._now(),
        )

    # -- Reconciliation -----------------------------------------------------------

    async def _reconcile_record(self, record: ExecutionRecord) -> ExecutionRecord:
        """`OrderNotFoundError` (Binance -2013), kaydı `UNKNOWN_NOT_FOUND`'a
        taşır — bunun anlamı YALNIZCA "exchange sorgusu BU denemede order'ı
        BULAMADI"DIR; "orijinal submission'ın Binance tarafından HİÇ kabul
        edilmediği KANITLANDI, yeniden gönderim güvenlidir" ANLAMINA
        GELMEZ (bkz. `submit()` docstring'i — Karar 78). Kayıt
        reconciliation-eligible KALIR; bir SONRAKİ `submit()`/`reconcile()`/
        `reconcile_pending()` çağrısı YENİDEN sorgular.

        KİLİT DİSİPLİNİ (HIGH-5): bu private helper KENDİSİ kilit ALMAZ —
        HER ZAMAN ÇAĞIRANIN (bkz. `submit()`, `reconcile()`,
        `reconcile_pending()`) ZATEN `context_id` için kilidi TUTTUĞU bir
        bağlamda çağrılır (asyncio.Lock reentrant DEĞİLDİR — burada AYRICA
        kilitlemek DEADLOCK üretirdi). `is_terminal()` kısa-devresi, bir
        BAŞKA eşzamanlı yazarın (`_save_or_defer_to_winner` üzerinden) bu
        kaydı ZATEN terminale taşımış olabileceği durumu GÜVENLE ele alır."""
        if record.is_terminal():
            return record
        try:
            result = await self._client.query_order(
                record.symbol, client_order_id=record.client_order_id, context_id=record.context_id
            )
        except OrderNotFoundError:
            record = transition(
                record, new_state=ExecutionLifecycleState.UNKNOWN_NOT_FOUND, now=self._now(),
                detail=(
                    "exchange query bu denemede order'ı bulamadı; orijinal submission'ın Binance "
                    "tarafından hiç kabul edilmediği KANITLANMIŞ DEĞİLDİR — otomatik yeniden gönderim "
                    "YAPILMAZ, yalnızca yeniden reconciliation dener"
                ),
            )
            return self._save_or_defer_to_winner(record)
        record = self._apply_result(record, result)
        return self._save_or_defer_to_winner(record)

    async def reconcile(self, *, client_order_id: str | None = None, context_id: str | None = None) -> ExecutionRecord:
        """Manuel/CLI-tetiklemeli reconciliation — `client_order_id` VEYA
        `context_id` ile bir kaydı bulur ve exchange'e karşı yeniden
        sorgulayarak günceller. Kayıt zaten TERMİNAL ise sorgulamaya GEREK
        yoktur (gereksiz bir API çağrısı YAPILMAZ).

        BLOCKER FİX (HIGH-5): asıl çözme/okuma/güncelleme, `context_id`
        başına AYNI kilit ALTINDA yapılır (bkz. `submit()`) — bu, bir
        `reconcile()` çağrısının, AYNI context_id için eşzamanlı bir
        `submit()`/`reconcile_pending()` sweep'iyle YARIŞMASINI engeller.
        `client_order_id` ile çağrılırsa, context_id'yi ÇÖZMEK için YAPILAN
        İLK okuma kilitsizdir (yalnızca ANAHTAR çözümlemesi içindir) —
        kilit ALTINDA kayıt TEKRAR TAZE okunur, bu yüzden bir TOCTOU
        boşluğu YOKTUR."""
        if context_id is None and client_order_id is None:
            raise ValueError("client_order_id veya context_id sağlanmalı")
        if context_id is None:
            pre = self._store.load_by_client_order_id(client_order_id)  # type: ignore[arg-type]
            if pre is None:
                raise LocalExecutionRecordNotFoundError(
                    f"yerel bir execution record bulunamadı (client_order_id={client_order_id})"
                )
            context_id = pre.context_id

        async with self._lock_for(context_id):
            record = self._store.load_by_context_id(context_id)
            if record is None:
                raise LocalExecutionRecordNotFoundError(
                    f"yerel bir execution record bulunamadı (client_order_id={client_order_id}, context_id={context_id})"
                )
            if record.is_terminal():
                return record
            return await self._reconcile_record(record)

    async def reconcile_pending(self) -> tuple[ExecutionRecord, ...]:
        """Reconciliation GEREKTİREN (terminal OLMAYAN, `UNKNOWN_NOT_FOUND`'a
        henüz çözülmemiş) TÜM kayıtları exchange'e karşı yeniden sorgular —
        restart sonrası kurtarma için ana giriş noktası (bkz. PHASE11 doc,
        "Restart Recovery"). Bir kaydın reconciliation'ı BAŞARISIZ olursa
        (transport/malformed/rejection), o kayıt DEĞİŞTİRİLMEDEN bırakılır
        ve DİĞER kayıtların işlenmesine DEVAM edilir (multi-symbol/
        multi-context izolasyonu — bir sembolün geçici hatası diğerlerini
        ETKİLEMEZ).

        BLOCKER FİX (HIGH-5): İLK `list_needing_reconciliation()` anlık
        görüntüsü kilitsizdir (yalnızca ADAY listesi içindir) — HER kayıt,
        KENDİ `context_id` kilidi ALTINDA YENİDEN TAZE okunur (bir başka
        eşzamanlı `submit()`/`reconcile()` bu arada onu ZATEN terminale
        taşımış olabilir) — bu yüzden bu sweep, AYNI context_id için
        eşzamanlı çalışan başka bir çağrıyla ASLA yarışmaz."""
        pending = self._store.list_needing_reconciliation()
        reconciled: list[ExecutionRecord] = []
        for stale in pending:
            try:
                async with self._lock_for(stale.context_id):
                    fresh = self._store.load_by_context_id(stale.context_id)
                    if fresh is None:
                        continue  # teorik olarak imkansız (satır ZATEN vardı) — sessizce atla
                    if fresh.is_terminal():
                        reconciled.append(fresh)
                        continue
                    reconciled.append(await self._reconcile_record(fresh))
            except (ExecutionTransportError, MalformedResponseError, BinanceRejectionError):
                reconciled.append(stale)
        return tuple(reconciled)

    # -- Portfolio/Accounting v1, step 5 — read-only balance check ------------

    async def check_usdt_balance(self) -> AssetBalance | None:
        """Wires the already-implemented, already-working, but previously
        completely UNUSED `testnet_client.py::account_info()` (a real,
        already-signed `GET /api/v3/account` call — zero private
        state-mutating endpoints touched) into ONE new, read-only,
        OBSERVABILITY-ONLY balance-reporting path. This extends the SAME
        "compare internal state against exchange truth" pattern this
        class already owns (see module docstring) rather than inventing
        a parallel mechanism — reuses `self._client` (already constructed,
        already authenticated), never a second connection.

        This method NEVER blocks, gates, or alters trading — it does not
        touch `RiskPolicyConfig`, `entry_gate()`, or any position state.
        A failure to reach the exchange (network error, auth error,
        malformed response — ANY exception) degrades to `None` ("balance
        unknown") — same defensive-wrap discipline as every other
        optional/observability hook in this codebase (`order_book_
        observer`/`candle_observer`/`_latest_price`); it NEVER crashes the
        caller and NEVER fabricates a balance number. Returns `None` (not
        an error) if the account genuinely holds no USDT balance row —
        indistinguishable from "lookup failed" by design, since neither
        case has a real number to report."""
        try:
            raw = await self._client.account_info()
            balances = parse_account_balances(raw)
        except Exception:  # noqa: BLE001 - observability-only: a balance-check failure must NEVER propagate or block trading
            return None
        for balance in balances:
            if balance.asset == "USDT":
                return balance
        return None


=== FILE: crypto_signal_engine/execution/reconciliation_store.py ===
"""
Faz 11 — durable `ExecutionRecord` persistence (SQLite).

Mimari kural ("persistence bir SINIRDIR, domain nesnelerini database-aware
YAPMAZ" — Faz 7 ile AYNI ilke): bu modül `ExecutionRecord`'u DEĞİŞTİRMEZ,
onu SARAR. Explicit `SCHEMA_VERSION` + sessiz migration YOK (Faz 7 ile
AYNI disiplin — bkz. `persistence/paper_state_store.py`).

HARD SAFETY INVARIANT: bu şemada API key/secret/signature alanı YOKTUR ve
OLAMAZ — yalnızca order kimliği/miktarları/durumu/zaman damgaları."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.errors import ReconciliationContradictionError
from crypto_signal_engine.execution.models import OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import (
    NEEDS_RECONCILIATION_STATES,
    ExecutionRecord,
    assert_monotonic,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS execution_record (
    context_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity REAL,
    quote_quantity REAL,
    price REAL,
    lifecycle_state TEXT NOT NULL,
    exchange_order_id INTEGER,
    executed_quantity REAL NOT NULL,
    cumulative_quote_quantity REAL NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_reconciled_at TEXT
);
"""


def _parse_utc(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _row_to_record(row: sqlite3.Row) -> ExecutionRecord:
    try:
        return ExecutionRecord(
            context_id=row["context_id"],
            symbol=row["symbol"],
            client_order_id=row["client_order_id"],
            side=OrderSide(row["side"]),
            order_type=OrderType(row["order_type"]),
            quantity=row["quantity"],
            quote_quantity=row["quote_quantity"],
            price=row["price"],
            lifecycle_state=row["lifecycle_state"],
            exchange_order_id=row["exchange_order_id"],
            executed_quantity=row["executed_quantity"],
            cumulative_quote_quantity=row["cumulative_quote_quantity"],
            detail=row["detail"],
            created_at=_parse_utc(row["created_at"]),
            updated_at=_parse_utc(row["updated_at"]),
            last_reconciled_at=_parse_utc(row["last_reconciled_at"]),
        )
    except (ValueError, TypeError, KeyError) as exc:
        raise CorruptRecordError(f"bozuk execution_record satırı ({row['context_id']!r}): {exc}") from exc


class ExecutionStateStore:
    """Faz 11 execution-lifecycle durumu için durable, injectable SQLite
    store. `db_path` HER ZAMAN çağıran tarafından enjekte edilir."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            # `isolation_level=None` (autocommit) — `save()` artık `BEGIN
            # IMMEDIATE`/`commit()`/`rollback()`'ı ELLE yönetir (bkz. HIGH-5
            # fix notu, aşağı); Python'ın KENDİ implicit transaction
            # yönetimiyle (varsayılan isolation_level) ÇAKIŞMAMASI için
            # KASITLI OLARAK devre dışı bırakılır.
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        # BLOCKER FİX (HIGH-5): `save()` artık `BEGIN IMMEDIATE` kullanır
        # (bkz. aşağı) — bu, AYNI context_id için EŞZAMANLI yazan İKİNCİ bir
        # process'i/connection'ı KISA SÜRELİĞİNE bloklar (ilk yazar commit
        # edene kadar). `busy_timeout` OLMADAN SQLite bu durumda ANINDA
        # `database is locked` hatası fırlatırdı — birkaç saniyelik bir
        # bekleme payı, normal (kötü niyetli olmayan) eşzamanlı submit/
        # reconcile çakışmalarının SESSİZCE (retry'siz, engine seviyesinde)
        # çözülmesini sağlar.
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
                    return
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

        existing = self._connection.execute("SELECT version FROM schema_version").fetchone()[0]
        if existing != SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                f"Durable execution store schema_version={existing}, bu kod SCHEMA_VERSION={SCHEMA_VERSION} "
                f"bekliyor — sessiz migration YAPILMAZ ({self._db_path})"
            )

    # -- Yazma ------------------------------------------------------------------

    def save(self, record: ExecutionRecord) -> None:
        """`context_id` PRIMARY KEY'i üzerinden GÜVENLİ (monotonik) bir
        upsert — TEK bir atomik transaction (kısmi bir yazma asla görünmez).

        BLOCKER FİX (HIGH-5 — "concurrent submit aynı deterministic
        clientOrderId ile local durable truth'u corrupt edebilir, örn.
        FILLED -> REJECTED"): eski implementasyon KÖRÜ KÖRÜNE bir
        `INSERT ... ON CONFLICT DO UPDATE` yapıyordu — çağıranın elindeki
        `record` DİSKTEKİ güncel satırdan daha ESKİ/BAYAT olsa BİLE
        (iki eşzamanlı `submit()`/`reconcile()` çağrısı, HER BİRİ kendi
        in-memory kopyasından "geçerli" bir geçiş üretebilir) SESSİZCE
        üzerine yazardı. ARTIK: `BEGIN IMMEDIATE` ile bu context_id için
        yazma kilidini ÖNCE alır (AYNI context_id'e eşzamanlı yazmaya
        çalışan başka bir process/connection, bu transaction bitene kadar
        BLOKLANIR — bkz. `busy_timeout`, `__init__`), SONRA disk üzerindeki
        GÜNCEL satırı okur, `assert_monotonic()` ile geçişin İMKANSIZ bir
        regresyon OLMADIĞINI doğrular, YALNIZCA O ZAMAN yazar. Bir
        regresyon tespit edilirse `ReconciliationContradictionError`
        fırlatılır (transaction rollback edilir, DURABLE TRUTH
        DOKUNULMADAN kalır) — çağıran (`ExecutionReconciliationService.
        _save_or_defer_to_winner`) bunu yakalayıp OTORİTER (kazanan)
        gerçeği yeniden okur."""
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise PersistenceError(
                f"execution_record checkpoint kilidi alınamadı (context_id={record.context_id}): {exc}"
            ) from exc

        try:
            row = self._connection.execute(
                "SELECT * FROM execution_record WHERE context_id = ?", (record.context_id,)
            ).fetchone()
            if row is not None:
                assert_monotonic(_row_to_record(row), record)
            self._connection.execute(
                """
                INSERT INTO execution_record (
                    context_id, symbol, client_order_id, side, order_type, quantity, quote_quantity,
                    price, lifecycle_state, exchange_order_id, executed_quantity, cumulative_quote_quantity,
                    detail, created_at, updated_at, last_reconciled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(context_id) DO UPDATE SET
                    symbol = excluded.symbol,
                    client_order_id = excluded.client_order_id,
                    side = excluded.side,
                    order_type = excluded.order_type,
                    quantity = excluded.quantity,
                    quote_quantity = excluded.quote_quantity,
                    price = excluded.price,
                    lifecycle_state = excluded.lifecycle_state,
                    exchange_order_id = excluded.exchange_order_id,
                    executed_quantity = excluded.executed_quantity,
                    cumulative_quote_quantity = excluded.cumulative_quote_quantity,
                    detail = excluded.detail,
                    updated_at = excluded.updated_at,
                    last_reconciled_at = excluded.last_reconciled_at
                """,
                (
                    record.context_id, record.symbol, record.client_order_id, record.side.value,
                    record.order_type.value, record.quantity, record.quote_quantity, record.price,
                    record.lifecycle_state, record.exchange_order_id, record.executed_quantity,
                    record.cumulative_quote_quantity, record.detail, record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                    record.last_reconciled_at.isoformat() if record.last_reconciled_at is not None else None,
                ),
            )
        except (ReconciliationContradictionError, CorruptRecordError):
            self._connection.rollback()
            raise
        except sqlite3.Error as exc:
            self._connection.rollback()
            raise PersistenceError(
                f"execution_record checkpoint başarısız (context_id={record.context_id}): {exc}"
            ) from exc
        else:
            try:
                self._connection.commit()
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"execution_record checkpoint commit başarısız (context_id={record.context_id}): {exc}"
                ) from exc

    # -- Okuma --------------------------------------------------------------------

    def load_by_context_id(self, context_id: str) -> ExecutionRecord | None:
        row = self._connection.execute(
            "SELECT * FROM execution_record WHERE context_id = ?", (context_id,)
        ).fetchone()
        return _row_to_record(row) if row is not None else None

    def load_by_client_order_id(self, client_order_id: str) -> ExecutionRecord | None:
        row = self._connection.execute(
            "SELECT * FROM execution_record WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        return _row_to_record(row) if row is not None else None

    def list_for_symbol(self, symbol: str) -> tuple[ExecutionRecord, ...]:
        normalized = normalize_symbol(symbol)
        rows = self._connection.execute(
            "SELECT * FROM execution_record WHERE symbol = ? ORDER BY created_at ASC", (normalized,)
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def list_needing_reconciliation(self) -> tuple[ExecutionRecord, ...]:
        placeholders = ", ".join("?" for _ in NEEDS_RECONCILIATION_STATES)
        rows = self._connection.execute(
            f"SELECT * FROM execution_record WHERE lifecycle_state IN ({placeholders}) ORDER BY created_at ASC",
            tuple(NEEDS_RECONCILIATION_STATES),
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def list_all_symbols(self) -> tuple[str, ...]:
        """Every distinct symbol with at least one execution record
        (bridge- or `lab-`-namespaced alike) — used by startup symbol-set
        computation (Phase 4/16) to find real bridge-owned inventory that
        must never be silently excluded from the runtime universe."""
        rows = self._connection.execute("SELECT DISTINCT symbol FROM execution_record ORDER BY symbol ASC").fetchall()
        return tuple(row["symbol"] for row in rows)

    def close(self) -> None:
        self._connection.close()


=== FILE: crypto_signal_engine/execution/signal_bridge.py ===
"""
Faz 13 — Signal -> Binance Spot TESTNET execution bridge.

Bu modül YENİ bir trading stratejisi DEĞİLDİR: Quant/MarketStructure/
OrderBook/Regime/Consensus/Risk mantığına HİÇ dokunmaz, `SignalDirection`in
KENDİ anlamını DEĞİŞTİRMEZ. Tek sorumluluğu: kabul edilmiş sinyal/runtime
pipeline'ının ZATEN ürettiği bir `RuntimeCycleResult`'ı (Faz 6), Binance
SPOT-only bir yürütme POLİTİKASI üzerinden, Faz 10/11'in ZATEN kabul
edilmiş bileşenlerine (`TestnetExecutionAdapter`, `ExecutionReconciliationService`,
deterministik `client_order_id`, HIGH-5 monotonik durable store)
DELEGE ETMEKTİR — yeni bir order-submission kod yolu İCAT EDİLMEZ, yalnızca
mevcut `ExecutionReconciliationService.submit()` çağrılır.

SPOT-ONLY POLİTİKA (Bölüm "SPOT-ONLY EXECUTION POLICY"):
- LONG-family sinyal + bridge FLAT  -> BUY (küçük sabit notional).
- LONG-family sinyal + bridge LONG  -> NO_ACTION (churn yok).
- SHORT-family sinyal + bridge LONG -> SELL, TAM OLARAK bridge-owned
  miktar (asla daha fazlası, asla hesabın ilgisiz/manuel bakiyesi).
- SHORT-family sinyal + bridge FLAT -> NO_ACTION (sentetik short YOK).
- NEUTRAL -> NO_ACTION.

BRIDGE-OWNED ENVANTER (Bölüm "BRIDGE-OWNED INVENTORY — CRITICAL"): ayrı bir
mutable ledger İNŞA EDİLMEZ — bridge'in sahip olduğu miktar, HER ZAMAN
`ExecutionStateStore`'daki (Faz 11, ZATEN durable/HIGH-5-monotonik)
KENDİ (bridge-namespaced `context_id` — bkz. `bridge_context_id()`) execution
kayıtlarından, TERMİNAL `FILLED` durumdaki `executed_quantity`
toplamından (`BUY` +, `SELL` -) YENİDEN İNŞA edilir (bkz.
`compute_bridge_position`). Bu, restart/reconciliation'dan OTOMATİK olarak
hayatta kalır (Faz 11'in ZATEN kanıtlanmış durable garantisiyle AYNI) ve
manuel `scripts/binance_testnet_lab.py` kaydlarını/hesabın mevcut genel
bakiyesini ASLA karıştırmaz (context_id namespace'i ile YAPISAL olarak
izole, bkz. `bridge_context_id`).

TARİHSEL REPLAY KORUMASI: bu modülün TEK giriş noktası `on_cycle_result()`,
YALNIZCA `bridge_runtime.py::BridgeRuntime`'ın CANLI candle-ingestion
yolundan çağrılır — `PersistedRuntime.recover()` (Faz 7, DEĞİŞTİRİLMEDEN)
ASLA `SignalEngine.evaluate()` çağırmadığı için (bkz. persistence/
recovery.py docstring'i), restart sonrası geri yüklenen HİÇBİR eski Paper
pozisyonu/sinyali bu fonksiyona ASLA ULAŞMAZ — yapısal olarak İMKANSIZDIR,
bu modülde AYRICA bir "startup guard" YAZILMASINA gerek YOKTUR."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from datetime import datetime
from typing import TYPE_CHECKING

from crypto_signal_engine.domain.enums import SignalDirection
from crypto_signal_engine.errors import ExecutionError
from crypto_signal_engine.execution.adapter import TestnetExecutionAdapter
from crypto_signal_engine.execution.errors import ExecutionPersistenceError
from crypto_signal_engine.execution.lifecycle import resolve_lot_size_filter
from crypto_signal_engine.execution.models import ExecutionResult, OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, ExecutionRecord
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock
from crypto_signal_engine.runtime.models import RuntimeCycleResult

if TYPE_CHECKING:
    # Import-cycle avoidance: `lifecycle_manager.py` imports `bridge_context_id`/
    # `_ACTION_CLOSE` FROM this module, so this module cannot import it at
    # runtime — the dependency only exists for type annotations, which
    # `from __future__ import annotations` already makes lazy/string-only.
    from crypto_signal_engine.execution.lifecycle import ExitReason
    from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager

_LOGGER = logging.getLogger("crypto_signal_engine.execution.signal_bridge")

_LONG_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_LONG, SignalDirection.LONG, SignalDirection.WEAK_LONG}
)
_SHORT_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_SHORT, SignalDirection.SHORT, SignalDirection.WEAK_SHORT}
)

_BRIDGE_NAMESPACE = "bridge"
_ACTION_OPEN = "OPEN"
_ACTION_CLOSE = "CLOSE"

SPOT_LONG_ONLY_POLICY = "SPOT_LONG_ONLY"


def bridge_context_id(symbol: str, action: str, signal_context_id: str) -> str:
    """Deterministik, bridge'e-özel bir `context_id` üretir — AYNI sinyal
    context'i (`signal_context_id`) HER ZAMAN AYNI bridge context_id'yi
    üretir (idempotency, Faz 5/11'in KENDİ `context_id` ilkesiyle AYNI),
    ve `{_BRIDGE_NAMESPACE}:{symbol}:` prefix'i, bu kaydın manuel
    `scripts/binance_testnet_lab.py` çağrılarından VEYA hesabın diğer
    faaliyetlerinden YAPISAL olarak AYRIŞTIRILMASINI sağlar (bkz.
    `compute_bridge_position` — yalnızca bu prefix'e sahip kayıtlar
    bridge-owned envantere DAHİL edilir)."""
    return f"{_BRIDGE_NAMESPACE}:{symbol}:{action}:{signal_context_id}"


def _is_bridge_record(record: ExecutionRecord, symbol: str) -> bool:
    return record.context_id.startswith(f"{_BRIDGE_NAMESPACE}:{symbol}:")


@dataclass(frozen=True)
class BridgePosition:
    """Bir sembol için, YALNIZCA bridge'in KENDİ execution kayıtlarından
    yeniden inşa edilmiş envanter görünümü. `ambiguous=True` iken
    `owned_quantity` GÜVENİLİR DEĞİLDİR (çağıran hiçbir yeni ekonomik
    karar ÜRETMEMELİDİR — bkz. `SignalTestnetBridge._decide_and_act`)."""

    symbol: str
    owned_quantity: float
    ambiguous: bool
    unresolved_context_ids: tuple[str, ...] = ()


def compute_bridge_position(store: ExecutionStateStore, symbol: str) -> BridgePosition:
    """`bridge_owned_quantity >= 0` invariant'ını HER ZAMAN sağlar (negatif
    bir toplam asla döndürülmez — politika zaten sahip olunandan fazlasını
    SATMAZ, ama defense-in-depth olarak burada da clamp edilir).

    Herhangi bir bridge-namespaced kayıt TERMİNAL DEĞİLSE (`SUBMISSION_
    ATTEMPTED`/`AMBIGUOUS`/`ACKNOWLEDGED`/`PARTIALLY_FILLED`/
    `UNKNOWN_NOT_FOUND`), bu sembol için `ambiguous=True` döner — bridge
    bu sembol için gerçek envanterini KANITLAYAMAZ, dolayısıyla HİÇBİR YENİ
    ekonomik karar (BUY/SELL) VEREMEZ (fail-closed)."""
    records = [r for r in store.list_for_symbol(symbol) if _is_bridge_record(r, symbol)]
    unresolved = tuple(r.context_id for r in records if not r.is_terminal())
    if unresolved:
        return BridgePosition(symbol=symbol, owned_quantity=0.0, ambiguous=True, unresolved_context_ids=unresolved)

    owned = 0.0
    for record in records:
        if record.lifecycle_state != ExecutionLifecycleState.FILLED:
            continue
        if record.side is OrderSide.BUY:
            owned += record.executed_quantity
        else:
            owned -= record.executed_quantity
    return BridgePosition(symbol=symbol, owned_quantity=max(owned, 0.0), ambiguous=False)


def _floor_to_step(value: float, step: float) -> float:
    """`value`'yu `step`'in katlarına AŞAĞI yuvarlar (asla YUKARI — bir
    SELL miktarının bridge-owned miktarı AŞMASI YAPISAL olarak İMKANSIZ
    olmalıdır). `Decimal` kullanılır (ham `float` bölmesi/çarpımı, step
    hizalamasında yanlış-negatif/yanlış-pozitif kayan-nokta hatalarına yol
    açabilir)."""
    if step <= 0:
        return value
    step_dec = Decimal(str(step))
    value_dec = Decimal(str(value))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * step_dec)


@dataclass(frozen=True)
class BridgeSymbolStatus:
    """Faz 13 gözlemlenebilirlik — bkz. `SignalTestnetBridge.status_for()`.
    Hiçbir alan credential/secret TAŞIMAZ."""

    symbol: str
    policy: str
    owned_quantity: float | None
    last_signal_context_id: str | None
    last_signal_direction: str | None
    last_action: str
    last_action_detail: str
    last_client_order_id: str | None
    last_lifecycle_state: str | None
    ambiguous: bool
    updated_at: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "policy": self.policy,
            "owned_quantity": self.owned_quantity,
            "last_signal_context_id": self.last_signal_context_id,
            "last_signal_direction": self.last_signal_direction,
            "last_action": self.last_action,
            "last_action_detail": self.last_action_detail,
            "last_client_order_id": self.last_client_order_id,
            "last_lifecycle_state": self.last_lifecycle_state,
            "ambiguous": self.ambiguous,
            "updated_at": self.updated_at,
        }


class SignalTestnetBridge:
    """Faz 13'ün TEK yüksek-seviye giriş noktası: `on_cycle_result()`.

    `mark_operational(True, ...)` YALNIZCA `app.py::Application`'ın
    startup execution reconciliation'ı BAŞARIYLA tamamlandıktan SONRA
    çağrılır — bu çağrıya kadar (VEYA reconciliation BAŞARISIZ olursa)
    `_operational=False` KALIR ve `on_cycle_result()` HİÇBİR order
    göndermez (fail-closed, bkz. modül docstring'i)."""

    POLICY = SPOT_LONG_ONLY_POLICY

    def __init__(
        self,
        *,
        execution_service: ExecutionReconciliationService,
        execution_store: ExecutionStateStore,
        notional_usdt: float,
        clock: Clock | None = None,
        lifecycle_manager: LifecycleManager | None = None,
        atr_provider: Callable[[str], float | None] | None = None,
        all_symbols_provider: Callable[[], tuple[str, ...]] | None = None,
        entries_paused_provider: Callable[[], bool] | None = None,
    ) -> None:
        """`lifecycle_manager`/`atr_provider`/`all_symbols_provider` are all
        `None` by default — with `lifecycle_manager=None`, every code path
        below is byte-for-byte the pre-autonomous-lifecycle behavior (the
        existing accepted opposite-signal-only bridge). When
        `lifecycle_manager` is provided (Application wires it whenever the
        bridge is enabled — see `app.py`), entries additionally pass
        `lifecycle_manager.entry_gate()` and initialize a durable LONG
        position (`on_entry_filled`), and opposite-signal exits route
        through `lifecycle_manager.attempt_exit()` instead of this class's
        own `_submit_sell` — the SAME shared per-symbol lock
        (`lifecycle_manager.lock_for`) is then used for `_lock_for()` too,
        so the M1 stop/target/trailing evaluator and this signal-driven
        exit path can never race each other (Phase 7)."""
        if not (notional_usdt > 0):
            raise ValueError(f"notional_usdt pozitif olmalı, alınan: {notional_usdt}")
        self._service = execution_service
        self._store = execution_store
        self._adapter = TestnetExecutionAdapter(execution_service.client)
        self._notional = notional_usdt
        self._clock = clock or SystemClock()
        self._operational = False
        self._operational_detail = "bridge not yet marked operational (startup reconciliation pending)"
        self._symbol_locks: dict[str, asyncio.Lock] = {}
        self._status: dict[str, BridgeSymbolStatus] = {}
        self._lifecycle_manager = lifecycle_manager
        self._atr_provider = atr_provider
        self._all_symbols_provider = all_symbols_provider
        # 24/7 Ops v1, Step 3 — Control Center pause gate. `None` (every
        # existing caller) means "never paused", bit-for-bit identical to
        # before this parameter existed. When provided (only by
        # `app.py::Application` once `dashboard_admin_token` is set), it
        # reads a `threading.Event.is_set()` (see `ops/admin.py`) — an
        # INDEPENDENT gate from `LifecycleManager.entry_gate()`'s own
        # three risk gates, checked here, BEFORE `entry_gate()` is even
        # called, and never applied to an already-open position's
        # management (that code path never reaches this check at all).
        self._entries_paused_provider = entries_paused_provider

    # -- Operational gate (Application startup wires this) -----------------

    def mark_operational(self, ok: bool, detail: str) -> None:
        self._operational = ok
        self._operational_detail = detail

    @property
    def operational(self) -> bool:
        return self._operational

    @property
    def operational_detail(self) -> str:
        return self._operational_detail

    # -- Observability -------------------------------------------------------

    def status_for(self, symbol: str) -> dict[str, object]:
        """Bu process'te bridge HENÜZ bu sembol için bir sinyal İŞLEMEDİYSE
        (örn. restart HEMEN sonrası, ilk sinyalden ÖNCE) `last_action`
        alanları hâlâ "NONE"'dır (in-memory karar geçmişi, restart'ta
        DOĞAL olarak boşalır) — ama `owned_quantity`, YİNE DE durable
        store'dan (bkz. `compute_bridge_position`) TAZE olarak okunur, bu
        yüzden restart sonrası envanter GÖZLEMLENEBİLİRLİĞİ bir sonraki
        sinyali BEKLEMEK ZORUNDA DEĞİLDİR (bkz. Bölüm "OBSERVABILITY" —
        "bridge-owned quantity" HER ZAMAN görülebilir olmalıdır)."""
        status = self._status.get(symbol)
        if status is None:
            owned_quantity: float | None = None
            try:
                owned_quantity = compute_bridge_position(self._store, symbol).owned_quantity
            except Exception as exc:  # noqa: BLE001 - yalnızca gözlemlenebilirlik amaçlı best-effort okuma
                _LOGGER.warning("signal-testnet-bridge: status_for(%s) envanter okunamadı: %s", symbol, exc)
            return BridgeSymbolStatus(
                symbol=symbol, policy=self.POLICY, owned_quantity=owned_quantity,
                last_signal_context_id=None, last_signal_direction=None,
                last_action="NONE", last_action_detail="no signal observed by the bridge yet",
                last_client_order_id=None, last_lifecycle_state=None,
                ambiguous=False, updated_at=None,
            ).as_dict()
        return status.as_dict()

    # -- Main entry point ------------------------------------------------------

    def _lock_for(self, symbol: str) -> asyncio.Lock:
        if self._lifecycle_manager is not None:
            # Shares the EXACT SAME lock object the M1 stop/target/trailing
            # evaluator uses (Phase 7) — a signal-driven opposite-signal
            # exit decision and a candle-driven stop/target decision for
            # the same symbol can then never run concurrently.
            return self._lifecycle_manager.lock_for(symbol)
        lock = self._symbol_locks.get(symbol)
        if lock is None:
            lock = asyncio.Lock()
            self._symbol_locks[symbol] = lock
        return lock

    async def on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        """Bu metod ASLA exception fırlatmaz (bkz. modül docstring'i —
        `BridgeRuntime`'ın canlı candle-consume döngüsü içinden çağrılır;
        bir bridge hatası market-data/paper-trading akışını ASLA
        etkilememelidir, tıpkı `RuntimeCoordinator`'ın `order_book_observer`
        hata izolasyonu ile AYNI disiplin)."""
        try:
            await self._on_cycle_result(cycle_result)
        except Exception:  # noqa: BLE001 - bkz. docstring: bridge hatası runtime'ı ASLA etkilemez
            _LOGGER.error("signal-testnet-bridge: beklenmeyen hata (izole edildi)", exc_info=True)

    async def _on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        if not cycle_result.evaluated or cycle_result.signal is None:
            return
        signal = cycle_result.signal
        symbol = signal.symbol

        if cycle_result.paper_result is not None and cycle_result.paper_result.idempotent_replay:
            # Aynı (symbol, context_id) daha önce zaten değerlendirildi
            # (Faz 5 idempotency) — bu KESİNLİKLE YENİ bir ekonomik karar
            # DEĞİLDİR, bkz. modül docstring'i "at most ONE economic
            # Testnet order intent per signal context".
            self._record(
                symbol, signal, action="NO_ACTION",
                detail="idempotent paper replay — no new economic decision",
            )
            return

        async with self._lock_for(symbol):
            await self._decide_and_act(symbol, signal)

    async def _decide_and_act(self, symbol: str, signal) -> None:  # noqa: ANN001 - Signal, döngüsel import'u önlemek için
        if not self._operational:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"bridge not operational: {self._operational_detail}",
            )
            return

        try:
            position = compute_bridge_position(self._store, symbol)
        except Exception as exc:  # noqa: BLE001 - store okunamazsa fail-closed, order YOK
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"bridge-owned inventory unavailable (fail-closed, no order): {exc}",
                ambiguous=True,
            )
            return

        if position.ambiguous:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=(
                    f"unresolved execution ambiguity for {symbol} "
                    f"({', '.join(position.unresolved_context_ids)}) — economic transitions blocked"
                ),
                ambiguous=True,
            )
            return

        direction = signal.direction

        if direction is SignalDirection.NEUTRAL:
            self._record(
                symbol, signal, action="NO_ACTION", detail="NEUTRAL — no execution",
                owned_quantity=position.owned_quantity,
            )
            return

        # When a `LifecycleManager` is wired (autonomous lifecycle enabled),
        # LONG/FLAT is decided from its fee-aware `net_owned_base_quantity`
        # (Phase 3/5) rather than the raw execution-record sum above — the
        # two can differ by base-asset commission amounts. `position` above
        # (raw sum) remains authoritative ONLY for ambiguity detection,
        # which is unaffected by fee accounting.
        if self._lifecycle_manager is not None:
            lifecycle_position = self._lifecycle_manager.position(symbol)
            if lifecycle_position.state.value == "FLAT" and position.owned_quantity > 0:
                # Raw execution-record truth shows REAL owned inventory but
                # the lifecycle layer has no record for it at all (e.g. a
                # legacy position migration that could not trustworthily
                # reconstruct it, Phase 4's "fail-closed, keep monitored,
                # never blindly sell/never treat as FLAT"). Never treat
                # this as FLAT — that would risk a duplicate BUY on top of
                # real inventory (pyramiding). No economic action either
                # way until this is resolved (operator/migration retry).
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail=(
                        f"bridge-owned inventory exists ({position.owned_quantity}) but has no lifecycle "
                        f"record (fail-closed, no order — needs migration/operator attention)"
                    ),
                    owned_quantity=position.owned_quantity, ambiguous=True,
                )
                return
            currently_long = lifecycle_position.state.value == "LONG"
            # Self-review fix: a new BUY must never be allowed while the
            # symbol owns ANY real bridge inventory in ANY non-FLAT
            # lifecycle state — not just LONG. This matters for every
            # transient state (DUST/ENTRY_PENDING/EXIT_PENDING/AMBIGUOUS)
            # and is REQUIRED for Phase 19's two-stage legacy migration:
            # a RECOVERED position (ownership authoritatively established
            # pre-bootstrap, market-dependent fields not yet initialized)
            # must never be pyramided into with a duplicate BUY before
            # stage 2 completes.
            blocks_new_long_entry = lifecycle_position.state.value != "FLAT"
        else:
            currently_long = position.owned_quantity > 0
            blocks_new_long_entry = currently_long

        if direction in _LONG_DIRECTIONS:
            if blocks_new_long_entry:
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail=(
                        "already bridge-owned LONG — same-direction, no churn" if currently_long
                        else f"bridge already owns inventory (state={lifecycle_position.state.value}) — no duplicate entry"
                    ),
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._entries_paused_provider is not None and self._entries_paused_provider():
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail="new entries PAUSED via Control Center admin action — already-open positions unaffected",
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._lifecycle_manager is not None:
                all_symbols = self._all_symbols_provider() if self._all_symbols_provider is not None else (symbol,)
                # CRITICAL FIX (C1 — mainnet-readiness review): pass this
                # BUY's own prospective notional so the exposure gate checks
                # what exposure would become AFTER this order, not just
                # exposure across every OTHER symbol today. Previously the
                # gate silently ignored the size of the very order it was
                # meant to be gating (see `entry_gate()`'s docstring).
                gate = self._lifecycle_manager.entry_gate(
                    symbol, all_symbols=all_symbols, additional_notional_usdt=self._notional,
                )
                if not gate.allowed:
                    self._record(
                        symbol, signal, action="NO_ACTION", detail=f"entry risk gate blocked: {gate.detail}",
                        owned_quantity=position.owned_quantity,
                    )
                    return
            await self._submit_buy(symbol, signal)
            return

        if direction in _SHORT_DIRECTIONS:
            if not currently_long:
                self._record(
                    symbol, signal, action="NO_ACTION",
                    detail="bridge is FLAT — no synthetic Spot short, no unrelated wallet inventory touched",
                    owned_quantity=position.owned_quantity,
                )
                return
            if self._lifecycle_manager is not None:
                await self._submit_opposite_signal_exit(symbol, signal)
                return
            await self._submit_sell(symbol, signal, position.owned_quantity)
            return

        raise AssertionError(f"unhandled SignalDirection: {direction!r}")  # pragma: no cover - fail-closed guard

    async def _submit_opposite_signal_exit(self, symbol: str, signal) -> None:  # noqa: ANN001
        from crypto_signal_engine.execution.lifecycle import ExitReason

        outcome = await self._lifecycle_manager.attempt_exit(  # type: ignore[union-attr]
            symbol, reason=ExitReason.OPPOSITE_SIGNAL, exit_signal_context_id=signal.context_id,
        )
        action = "SELL" if outcome.action in ("SOLD", "PARTIAL") else "NO_ACTION"
        refreshed_owned_quantity = self._lifecycle_manager.position(symbol).net_owned_base_quantity  # type: ignore[union-attr]
        self._record(
            symbol, signal, action=action, detail=f"lifecycle exit: {outcome.detail}",
            owned_quantity=refreshed_owned_quantity,
        )

    # -- Order construction / submission -----------------------------------

    async def _submit_buy(self, symbol: str, signal) -> None:  # noqa: ANN001
        context_id = bridge_context_id(symbol, _ACTION_OPEN, signal.context_id)
        try:
            intent = OrderIntent(
                symbol=symbol, side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id=context_id, timestamp=self._now(), quote_quantity=self._notional,
            )
        except ValueError as exc:
            self._record(symbol, signal, action="NO_ACTION", detail=f"BUY intent invalid (fail-closed): {exc}")
            return
        await self._submit(symbol, signal, intent, action="BUY")

    async def _submit_sell(self, symbol: str, signal, owned_quantity: float) -> None:  # noqa: ANN001
        try:
            filters = await self._adapter.validate_symbol(symbol)
        except Exception as exc:  # noqa: BLE001 - filtre lookup başarısızsa fail-closed
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=f"SELL filter lookup failed (fail-closed, no order): {exc}",
                owned_quantity=owned_quantity,
            )
            return

        step, floor_qty = resolve_lot_size_filter(
            market_step_size=filters.market_step_size, market_min_qty=filters.market_min_qty,
            step_size=filters.step_size, min_qty=filters.min_qty,
        )
        quantity = _floor_to_step(owned_quantity, step)
        if quantity <= 0 or quantity < floor_qty or quantity > owned_quantity:
            self._record(
                symbol, signal, action="NO_ACTION",
                detail=(
                    f"bridge-owned quantity {owned_quantity} cannot be sold as a valid Binance lot "
                    f"after step-size alignment (aligned={quantity}, minQty={floor_qty}) — no order"
                ),
                owned_quantity=owned_quantity,
            )
            return

        context_id = bridge_context_id(symbol, _ACTION_CLOSE, signal.context_id)
        try:
            intent = OrderIntent(
                symbol=symbol, side=OrderSide.SELL, order_type=OrderType.MARKET,
                context_id=context_id, timestamp=self._now(), quantity=quantity,
            )
        except ValueError as exc:
            self._record(
                symbol, signal, action="NO_ACTION", detail=f"SELL intent invalid (fail-closed): {exc}",
                owned_quantity=owned_quantity,
            )
            return
        await self._submit(symbol, signal, intent, action="SELL")

    async def _submit(self, symbol: str, signal, intent: OrderIntent, *, action: str) -> None:  # noqa: ANN001
        try:
            record = await self._service.submit(intent)
        except ExecutionPersistenceError as exc:
            if exc.exchange_may_have_accepted_order and action == "BUY" and self._lifecycle_manager is not None:
                # CRITICAL FIX (H3 — mainnet-readiness review, "phantom
                # BUY"): the exchange may have already accepted/filled a
                # REAL BUY that we have zero local record of (place_order()
                # succeeded, persisting that fact failed). The old
                # behaviour fell through to the generic fail-closed record
                # below, which left this symbol looking FLAT to every
                # future signal cycle — `entry_gate()`/`blocks_new_long_
                # entry` would happily allow ANOTHER BUY on top of real,
                # invisible inventory (doubled exposure, zero stop-loss
                # ever set for the phantom half). Pin the symbol AMBIGUOUS
                # instead — this already blocks new entries for it
                # (`blocks_new_long_entry = state != FLAT`) until an
                # operator reconciles `intent.client_order_id` and
                # corrects this record.
                self._lifecycle_manager.mark_ambiguous(
                    symbol,
                    detail=(
                        f"BUY may have reached the exchange (client_order_id={intent.client_order_id}) but local "
                        f"persistence failed — pinned fail-safe pending manual reconcile: {exc}"
                    ),
                    client_order_id=intent.client_order_id,
                )
            self._record(
                symbol, signal, action=action,
                detail=f"submission did not produce a confirmed order (fail-closed): {exc}",
                client_order_id=intent.client_order_id,
            )
            return
        except ExecutionError as exc:
            self._record(
                symbol, signal, action=action,
                detail=f"submission did not produce a confirmed order (fail-closed): {exc}",
                client_order_id=intent.client_order_id,
            )
            return
        except Exception as exc:  # noqa: BLE001 - bilinmeyen hata, ASLA gizli retry, ASLA runtime'ı etkileme
            self._record(
                symbol, signal, action=action,
                detail=f"unexpected submission error (fail-closed, no retry): {exc}",
                client_order_id=intent.client_order_id,
            )
            return

        # Gözlemlenebilirlik doğruluğu: `_record()` çağrılmadan ÖNCE, submit
        # SONRASI GÜNCEL bridge-owned miktarı TEKRAR hesaplanır — aksi halde
        # (bu çağrı `_decide_and_act`'te submit ÖNCESİ hesaplanan `position`
        # üzerinden GEÇMEDİĞİ için) `status_for()` bir BAŞARILI BUY/SELL'DEN
        # SONRA BİLE eski (`None`/stale) `owned_quantity`'yi göstermeye devam
        # ederdi. Bu YALNIZCA gözlemlenebilirlik içindir — GERÇEK envanter
        # (`compute_bridge_position()`, bir SONRAKİ sinyal kararında YENİDEN
        # okunur) HER ZAMAN durable store'dan doğru şekilde türetilir; bu
        # tazeleme BAŞARISIZ olsa bile (ağ/store geçici hatası) ekonomik
        # karar mantığı ETKİLENMEZ.
        if (
            self._lifecycle_manager is not None and action == "BUY"
            and record.lifecycle_state == ExecutionLifecycleState.FILLED
        ):
            # Phase 5/6 — a filled BUY becomes a durable LONG lifecycle
            # position (fee-aware quantity, ATR-based stop/target) here,
            # exactly once per entry (idempotent replay is already excluded
            # upstream in `_on_cycle_result`, and `submit()` itself never
            # re-confirms an already-terminal record as freshly FILLED).
            synthetic_result = ExecutionResult(
                symbol=symbol, client_order_id=record.client_order_id, exchange_order_id=record.exchange_order_id,
                side=OrderSide.BUY, status=record.lifecycle_state, executed_quantity=record.executed_quantity,
                cumulative_quote_quantity=record.cumulative_quote_quantity, transaction_time=record.updated_at,
                context_id=record.context_id,
            )
            atr = self._atr_provider(symbol) if self._atr_provider is not None else None
            try:
                await self._lifecycle_manager.on_entry_filled(
                    symbol, result=synthetic_result, signal_context_id=signal.context_id, atr=atr,
                )
            except Exception:  # noqa: BLE001 - a lifecycle bookkeeping failure must never look like a failed BUY
                _LOGGER.error(
                    "signal-testnet-bridge: on_entry_filled FAILED for %s after a real BUY fill "
                    "(money already moved — this needs operator attention)", symbol, exc_info=True,
                )

        refreshed_owned_quantity: float | None = None
        try:
            refreshed_owned_quantity = compute_bridge_position(self._store, symbol).owned_quantity
        except Exception as exc:  # noqa: BLE001 - yalnızca status alanı için best-effort tazeleme
            _LOGGER.warning(
                "signal-testnet-bridge: submit sonrası owned_quantity tazelenemedi (%s): %s", symbol, exc
            )

        self._record(
            symbol, signal, action=action,
            detail=f"submit result: {record.lifecycle_state}"
            + ("" if record.is_terminal() else " (unresolved — reconciliation-eligible)"),
            client_order_id=record.client_order_id, lifecycle_state=record.lifecycle_state,
            ambiguous=not record.is_terminal(), owned_quantity=refreshed_owned_quantity,
        )

    def _now(self) -> datetime:
        return self._clock.now()

    # -- Status recording --------------------------------------------------

    def _record(
        self, symbol: str, signal, *, action: str, detail: str,  # noqa: ANN001
        owned_quantity: float | None = None, client_order_id: str | None = None,
        lifecycle_state: str | None = None, ambiguous: bool = False,
    ) -> None:
        prior = self._status.get(symbol)
        resolved_owned = owned_quantity if owned_quantity is not None else (
            prior.owned_quantity if prior is not None else None
        )
        self._status[symbol] = BridgeSymbolStatus(
            symbol=symbol, policy=self.POLICY, owned_quantity=resolved_owned,
            last_signal_context_id=signal.context_id, last_signal_direction=signal.direction.value,
            last_action=action, last_action_detail=detail,
            last_client_order_id=client_order_id if client_order_id is not None else (
                prior.last_client_order_id if prior is not None else None
            ),
            last_lifecycle_state=lifecycle_state if lifecycle_state is not None else (
                prior.last_lifecycle_state if prior is not None else None
            ),
            ambiguous=ambiguous, updated_at=self._now().isoformat(),
        )
        _LOGGER.info(
            "signal-testnet-bridge symbol=%s context_id=%s direction=%s action=%s detail=%s",
            symbol, signal.context_id, signal.direction.value, action, detail,
        )


=== FILE: crypto_signal_engine/execution/signer.py ===
"""
Faz 10 — Binance request imzalama, izole bir modülde.

Kural: `SignalEngine`/`agents`/`FeatureEngine`/`RuntimeCoordinator`/
`PaperTradingEngine` HTTP signing'den HABERDAR DEĞİLDİR ve olamaz — bu
modül yalnızca `adapter.py`/`testnet_client.py` tarafından kullanılır.

İmzalama, Binance'in resmi Spot API sözleşmesiyle AYNIDIR: parametreler
`application/x-www-form-urlencoded` olarak (INSERTION SIRASINA göre,
alfabetik sıralama GEREKMEZ — Binance imzayı TAM OLARAK gönderilen query
string üzerinden doğrular) kodlanır, ardından bu TAM string HMAC-SHA256
ile `api_secret` kullanılarak imzalanır. Secret hiçbir zaman logda/hata
mesajında GÖRÜNMEZ (bkz. `sign()` — yalnızca hex digest döner, secret'ı
asla TEKRAR ETMEZ)."""

from __future__ import annotations

import hashlib
import hmac
import urllib.parse
from collections import OrderedDict


def canonical_query_string(params: "OrderedDict[str, str]") -> str:
    """Parametreleri, VERİLEN sırayla (insertion order — Binance'in
    signing sözleşmesi budur) `application/x-www-form-urlencoded` olarak
    kodlar. Çağıran, deterministik bir sıra ile bir `OrderedDict`
    OLUŞTURMAKTAN sorumludur (bkz. `testnet_client.py::_build_signed_params`
    — sabit, dokümante edilmiş bir alan sırası kullanır)."""
    return urllib.parse.urlencode(list(params.items()))


class BinanceTestnetSigner:
    """HMAC-SHA256 imzalayıcı. `api_secret`, yalnızca bu sınıfın
    constructor'ında TUTULUR — hiçbir `__repr__`/`__str__` override'ı
    YOKTUR (varsayılan `object.__repr__`, secret'ı asla YAZDIRMAZ)."""

    def __init__(self, api_secret: str) -> None:
        if not api_secret.strip():
            raise ValueError("api_secret boş olamaz")
        self._secret = api_secret.encode("utf-8")

    def sign(self, canonical_query: str) -> str:
        return hmac.new(self._secret, canonical_query.encode("utf-8"), hashlib.sha256).hexdigest()


