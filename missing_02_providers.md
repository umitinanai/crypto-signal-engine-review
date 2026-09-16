<!-- missing_02_providers.md — 13 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/providers/base.py (2826 bytes) -->
<!--   - crypto_signal_engine/providers/__init__.py (1 bytes) -->
<!--   - crypto_signal_engine/providers/binance/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/providers/binance/clock.py (3558 bytes) -->
<!--   - crypto_signal_engine/providers/binance/config.py (6909 bytes) -->
<!--   - crypto_signal_engine/providers/binance/order_book_sync.py (11872 bytes) -->
<!--   - crypto_signal_engine/providers/binance/parser.py (18757 bytes) -->
<!--   - crypto_signal_engine/providers/binance/provider.py (28675 bytes) -->
<!--   - crypto_signal_engine/providers/binance/real_websocket.py (3809 bytes) -->
<!--   - crypto_signal_engine/providers/binance/reconnect.py (3512 bytes) -->
<!--   - crypto_signal_engine/providers/binance/rest.py (9431 bytes) -->
<!--   - crypto_signal_engine/providers/binance/symbols.py (2113 bytes) -->
<!--   - crypto_signal_engine/providers/binance/transport.py (4391 bytes) -->

=== FILE: crypto_signal_engine/providers/base.py ===
"""
Provider contract.

LiveDataProvider yalnızca ham piyasa verisini normalize edilmiş domain
modellerine (Candle, Trade, OrderBookSnapshot) çevirip event olarak
yayınlamakla yükümlüdür. Quant/feature/sinyal mantığı, persistence, veya
gerçek Binance execution kodu bu dosyada YOKTUR ve olamaz.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade


class ConnectionState(str, Enum):
    """Provider'ın bağlantı durum makinesi."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ProviderHealthSnapshot:
    """Provider'ın anlık sağlık durumu.

    HARDENING NOTU (Quality Gate 9): symbol boş olamaz, reconnect_count
    negatif olamaz, last_message_at (varsa) UTC-aware olmak zorundadır.
    """

    connection_state: ConnectionState
    last_message_at: datetime | None
    reconnect_count: int
    symbol: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.connection_state, ConnectionState, "connection_state")
        if self.reconnect_count < 0:
            raise ValueError("reconnect_count negatif olamaz")
        if self.last_message_at is not None:
            require_utc_aware(self.last_message_at, "last_message_at")


class LiveDataProvider(ABC):
    """Canlı piyasa verisi sağlayıcıları için soyut temel sınıf."""

    @abstractmethod
    async def stream_candles(self, symbol: str, timeframe: Timeframe) -> AsyncIterator[Candle]:
        raise NotImplementedError
        yield  # pragma: no cover

    @abstractmethod
    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        raise NotImplementedError
        yield  # pragma: no cover

    @abstractmethod
    async def stream_order_book(self, symbol: str, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        raise NotImplementedError
        yield  # pragma: no cover

    @abstractmethod
    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        raise NotImplementedError

    @abstractmethod
    def health(self) -> ProviderHealthSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


=== FILE: crypto_signal_engine/providers/__init__.py ===



=== FILE: crypto_signal_engine/providers/binance/__init__.py ===


=== FILE: crypto_signal_engine/providers/binance/clock.py ===
"""
Injectable clock / sleeper / jitter kaynağı.

Kural (Bölüm 23): testler gerçek saniyelerce sleep etmemeli; reconnect
backoff, stale detection, retry delay gibi mekanizmalar deterministic ve
test edilebilir olmalı. Bu modül, wall-clock ve `asyncio.sleep`
bağımlılığını business logic'ten ayıran ince bir soyutlama sağlar.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Şu anki UTC zamanını döndüren soyutlama."""

    def now(self) -> datetime: ...


class SystemClock:
    """Gerçek wall-clock zamanını kullanan üretim implementasyonu."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock:
    """Testler için: elle ilerletilen, deterministik bir clock."""

    def __init__(self, initial: datetime) -> None:
        if initial.tzinfo is None:
            raise ValueError("initial UTC-aware olmalı")
        self._current = initial

    def now(self) -> datetime:
        return self._current

    def advance(self, seconds: float) -> None:
        from datetime import timedelta

        self._current = self._current + timedelta(seconds=seconds)

    def set(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("value UTC-aware olmalı")
        self._current = value


@runtime_checkable
class Sleeper(Protocol):
    """`asyncio.sleep`'in cancellation-aware soyutlaması."""

    async def sleep(self, seconds: float) -> None: ...


class AsyncioSleeper:
    """Gerçek `asyncio.sleep` kullanan üretim implementasyonu.

    `asyncio.CancelledError` kasıtlı olarak yutulmaz/yakalanmaz — bu,
    cancellation'ın normal shutdown'dan ayrılması gerektiği kuralının
    (Bölüm 18) bir parçasıdır: sleep sırasında cancel edilen bir task,
    CancelledError'ı doğal olarak yukarı fırlatmaya devam eder.
    """

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class FakeSleeper:
    """Testler için: gerçekte hiç uyumayan, ama çağrıları kaydeden sleeper.

    `advance_time_fn` verilirse, her sahte sleep çağrısında bir FixedClock'u
    da otomatik ilerletmek için kullanılabilir (opsiyonel).
    """

    def __init__(self) -> None:
        self.calls: list[float] = []
        self._cancelled = False

    async def sleep(self, seconds: float) -> None:
        if self._cancelled:
            raise asyncio.CancelledError()
        self.calls.append(seconds)
        # Gerçekten uyumaz — testler saniyelerce beklemez (Bölüm 23).
        await asyncio.sleep(0)

    def trigger_cancellation(self) -> None:
        self._cancelled = True


@runtime_checkable
class JitterSource(Protocol):
    """Reconnect backoff'a eklenen rastgele jitter için soyutlama."""

    def jitter(self, max_seconds: float) -> float: ...


class RandomJitterSource:
    """Gerçek `random.uniform` kullanan üretim implementasyonu."""

    def jitter(self, max_seconds: float) -> float:
        if max_seconds <= 0:
            return 0.0
        return random.uniform(0.0, max_seconds)


class DeterministicJitterSource:
    """Testler için: her zaman sabit bir jitter değeri döndürür."""

    def __init__(self, fixed_value: float = 0.0) -> None:
        self._fixed_value = fixed_value

    def jitter(self, max_seconds: float) -> float:
        return min(self._fixed_value, max_seconds) if max_seconds > 0 else 0.0


=== FILE: crypto_signal_engine/providers/binance/config.py ===
"""
Binance provider konfigürasyonu.

Kural (Bölüm 4): Hard-coded Binance URL'leri business logic içine
dağıtılmaz; tüm bağlantı/timeout/reconnect parametreleri burada, tek bir
immutable config nesnesinde toplanır. Invalid config fail-fast olmalıdır
(construction sırasında ValueError).

Kural (Bölüm 3 — hard safety boundary): bu config hiçbir API key/secret
alanı TAŞIMAZ. Bu, sistemin yalnızca public/read-only endpoint'lere erişimi
olduğunu config seviyesinde garanti eder — signed request için gereken
kimlik bilgisi yapısal olarak mevcut değildir.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from crypto_signal_engine.domain._validation import normalize_symbol, require_finite
from crypto_signal_engine.domain.enums import Timeframe

# Binance'in resmi kline interval string'leri (public Spot API dokümantasyonuna
# göre). Bu mapping, Phase 1 Timeframe enum'unu SOURCE OF TRUTH olarak korur;
# Binance'in wire-format string'i yalnızca bu sınırda (adapter boundary) üretilir
# ve domain katmanına asla sessizce sızmaz (Bölüm 4 — "unknown timeframe Binance
# interval string'ine sessizce çevrilmemeli").
TIMEFRAME_TO_BINANCE_INTERVAL: dict[Timeframe, str] = {
    Timeframe.M1: "1m",
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.H1: "1h",
}

# Binance'in tek bir REST çağrısında döndürebileceği maksimum kline sayısı
# (resmi Spot API dokümantasyonu; sabit, business logic içine dağıtılmaz).
BINANCE_MAX_KLINES_PER_REQUEST = 1000

# Binance'in `depth` REST endpoint'i için izin verilen limit değerleri
# (resmi dokümantasyon: 5, 10, 20, 50, 100, 500, 1000, 5000).
BINANCE_VALID_DEPTH_LIMITS: tuple[int, ...] = (5, 10, 20, 50, 100, 500, 1000, 5000)


def binance_interval_for(timeframe: Timeframe) -> str:
    """Phase 1 Timeframe enum'unu Binance interval string'ine çevirir.

    Bilinmeyen/desteklenmeyen bir Timeframe için SESSİZCE bir string
    üretmez; `KeyError` yerine açık bir `ConfigurationError` fırlatır.
    """
    from crypto_signal_engine.errors import ConfigurationError

    try:
        return TIMEFRAME_TO_BINANCE_INTERVAL[timeframe]
    except KeyError as exc:
        raise ConfigurationError(
            f"Desteklenmeyen Timeframe için Binance interval string'i yok: {timeframe!r}. "
            f"Desteklenenler: {list(TIMEFRAME_TO_BINANCE_INTERVAL)}"
        ) from exc


@dataclass(frozen=True)
class ReconnectPolicyConfig:
    """Exponential backoff + jitter + bounded reconnect parametreleri."""

    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    multiplier: float = 2.0
    jitter_seconds: float = 0.5
    max_attempts: int | None = None  # None => sınırsız deneme (ama her deneme bounded delay ile)

    def __post_init__(self) -> None:
        require_finite(self.initial_delay_seconds, "initial_delay_seconds")
        require_finite(self.max_delay_seconds, "max_delay_seconds")
        require_finite(self.multiplier, "multiplier")
        require_finite(self.jitter_seconds, "jitter_seconds")
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial_delay_seconds pozitif olmalı")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds, initial_delay_seconds'tan küçük olamaz")
        if self.multiplier <= 1.0:
            raise ValueError("multiplier 1.0'dan büyük olmalı (aksi halde backoff büyümez)")
        if self.jitter_seconds < 0:
            raise ValueError("jitter_seconds negatif olamaz")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts en az 1 olmalı (veya None = sınırsız)")


@dataclass(frozen=True)
class BinanceConfig:
    """Binance public market data erişimi için tüm konfigürasyon.

    HARD SAFETY INVARIANT: bu sınıfta API kimlik doğrulama alanları (anahtar,
    parola özeti, vb.) veya
    herhangi bir signed-request alanı YOKTUR ve OLAMAZ.
    """

    rest_base_url: str = "https://api.binance.com"
    ws_base_url: str = "wss://stream.binance.com:9443"
    request_timeout_seconds: float = 10.0
    reconnect: ReconnectPolicyConfig = field(default_factory=ReconnectPolicyConfig)
    order_book_depth: int = 1000
    stale_feed_threshold_seconds: float = 30.0
    supported_symbols: tuple[str, ...] = ()
    supported_timeframes: tuple[Timeframe, ...] = (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1)
    max_klines_per_request: int = BINANCE_MAX_KLINES_PER_REQUEST

    def __post_init__(self) -> None:
        if not self.rest_base_url.startswith("https://"):
            raise ValueError("rest_base_url https:// ile başlamalı (public data, düz http yasak)")
        if not self.ws_base_url.startswith("wss://"):
            raise ValueError("ws_base_url wss:// ile başlamalı (şifresiz ws yasak)")
        require_finite(self.request_timeout_seconds, "request_timeout_seconds")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds pozitif olmalı")
        if not isinstance(self.reconnect, ReconnectPolicyConfig):
            raise TypeError("reconnect bir ReconnectPolicyConfig olmalı")
        if self.order_book_depth not in BINANCE_VALID_DEPTH_LIMITS:
            raise ValueError(
                f"order_book_depth Binance'in izin verdiği limitlerden biri olmalı: "
                f"{BINANCE_VALID_DEPTH_LIMITS}, alınan: {self.order_book_depth}"
            )
        require_finite(self.stale_feed_threshold_seconds, "stale_feed_threshold_seconds")
        if self.stale_feed_threshold_seconds <= 0:
            raise ValueError("stale_feed_threshold_seconds pozitif olmalı")
        if not isinstance(self.supported_timeframes, tuple):
            object.__setattr__(self, "supported_timeframes", tuple(self.supported_timeframes))
        for tf in self.supported_timeframes:
            if not isinstance(tf, Timeframe):
                raise TypeError(f"supported_timeframes yalnızca Timeframe enum değerleri içermeli: {tf!r}")
        if not isinstance(self.supported_symbols, tuple):
            object.__setattr__(self, "supported_symbols", tuple(self.supported_symbols))
        # Her symbol construction sırasında canonical şekilde normalize/
        # validate edilir — geçersiz/boş bir symbol config'e sessizce
        # giremez (normalize_symbol zaten str-olmama/boş durumunda
        # TypeError/ValueError fırlatır).
        object.__setattr__(
            self, "supported_symbols", tuple(normalize_symbol(s) for s in self.supported_symbols)
        )
        if self.max_klines_per_request <= 0 or self.max_klines_per_request > BINANCE_MAX_KLINES_PER_REQUEST:
            raise ValueError(
                f"max_klines_per_request (0, {BINANCE_MAX_KLINES_PER_REQUEST}] aralığında olmalı"
            )


=== FILE: crypto_signal_engine/providers/binance/order_book_sync.py ===
"""
Binance order book snapshot+diff senkronizasyonu.

Bu modül, Binance'in resmi Spot API dokümantasyonundaki "How to manage a
local order book correctly" prosedürünü implemente eder:

  1. Diff depth event'lerini (`@depth`) buffer'a al.
  2. REST'ten bir depth snapshot'ı al (`lastUpdateId` ile).
  3. Snapshot'ın `lastUpdateId`'sinden ESKİ VEYA EŞİT olan (`u <= lastUpdateId`)
     buffer'lanmış event'leri AT.
  4. Kalan buffer'daki İLK event şu koşulu sağlamalı:
         first.U <= lastUpdateId + 1 <= first.u
     Sağlamıyorsa (snapshot ile buffer arasında köprü kurulamıyor), bu
     snapshot/buffer çifti kullanılamaz — resync (yeni snapshot) gerekir.
  5. Bu noktadan sonra gelen HER event için OVERLAP KABUL EDİLİR:
         event.U <= (önceki_uygulanan.u + 1)
     yeterlidir (strict equality ZORUNLU DEĞİLDİR). Binance pratikte
     ardışık diff event'ler arasında küçük bir örtüşme (overlap)
     gönderebilir (örn. yeniden teslim/redelivery); event'in MUTLAK
     miktar temsil etmesi nedeniyle bir aralığın kısmen tekrar
     uygulanması ZARARSIZDIR (idempotent). Yalnızca `event.U >
     önceki_uygulanan.u + 1` durumu GERÇEK bir sequence gap'tir.
  6. Her event'teki fiyat seviyesi MUTLAK miktarı temsil eder;
     `quantity == 0` o seviyeyi SİLER.

Bu sınıf saf (pure) durum makinesidir; hiçbir network I/O içermez —
provider katmanı (websocket.py) REST/WS'ten aldığı veriyi buraya besler.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import normalize_symbol, require_utc_aware
from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.errors import OrderBookSyncError

# Snapshot gelmeden önce buffer'lanabilecek maksimum diff event sayısı.
# Bölüm 14 (backpressure): sınırsız kuyruk yasak. Bu limit aşılırsa
# senkronizasyon başarısız kabul edilir ve resync (yeni snapshot) gerekir
# — bu, "sessiz veri kaybı yasak" kuralına uygun biçimde AÇIK bir
# `OrderBookSyncError` ile gözlemlenebilir hale getirilir.
MAX_PRE_SYNC_BUFFER_SIZE = 2000


class OrderBookSyncState(str, Enum):
    """Senkronizasyon durum makinesi."""

    UNSYNCED = "UNSYNCED"  # henüz geçerli bir snapshot uygulanmadı
    SYNCED = "SYNCED"  # canonical book güncel ve güvenilir


@dataclass(frozen=True)
class DepthDiffEvent:
    """Binance `depthUpdate` WebSocket event'inin ayrıştırılmış hâli.

    Bu, Phase 1 domain modeli DEĞİLDİR — yalnızca bu modülün senkronizasyon
    algoritması için kullandığı Phase 2'ye özgü bir taşıyıcıdır.
    """

    symbol: str
    first_update_id: int  # Binance "U"
    final_update_id: int  # Binance "u"
    event_time: datetime
    bids: tuple[tuple[float, float], ...]  # (price, quantity) — mutlak miktar
    asks: tuple[tuple[float, float], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.event_time, "event_time")
        if self.final_update_id < self.first_update_id:
            raise ValueError("final_update_id (u), first_update_id'den (U) küçük olamaz")


class OrderBookSynchronizer:
    """Tek bir symbol için Binance order book senkronizasyon durum makinesi.

    Kullanım akışı (provider katmanı tarafından sürülür):

        sync = OrderBookSynchronizer(symbol)
        # WS diff event'leri geldikçe:
        result = sync.ingest_diff(event)       # None => henüz sync değil (buffer'landı)
        if result is not None:
            ... canonical OrderBookSnapshot elde edildi ...

        # Senkronize değilken (veya resync gerektiğinde):
        snapshot = sync.ingest_snapshot(last_update_id, bids, asks, timestamp)
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = normalize_symbol(symbol)
        self._state = OrderBookSyncState.UNSYNCED
        self._buffer: deque[DepthDiffEvent] = deque()
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._last_update_id: int | None = None

    @property
    def state(self) -> OrderBookSyncState:
        return self._state

    @property
    def buffered_event_count(self) -> int:
        return len(self._buffer)

    def reset(self) -> None:
        """Senkronizasyonu tamamen sıfırlar (örn. reconnect sonrası).

        Bölüm 8: "order book mutlaka gerekli resync mekanizmasına girmeli."
        Reconnect sonrası provider bu metodu çağırıp yeniden
        buffer+snapshot akışını başlatmalıdır.
        """
        self._state = OrderBookSyncState.UNSYNCED
        self._buffer.clear()
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = None

    def ingest_diff(self, event: DepthDiffEvent) -> OrderBookSnapshot | None:
        """Tek bir diff event'i işler.

        - Henüz SYNCED değilse: event buffer'a eklenir, `None` döner.
        - SYNCED ise: OVERLAP semantiği ile sürekliliği kontrol edilir
          (`event.U <= last_update_id + 1` yeterlidir, strict equality
          ZORUNLU DEĞİLDİR); sağlanıyorsa uygulanır ve güncel canonical
          `OrderBookSnapshot` döner.
        - Sequence gap (`event.U > last_update_id + 1`) tespit edilirse:
          state UNSYNCED'e döner (yeni buffer bu event ile başlar) ve
          `OrderBookSyncError` fırlatılır — çağıran taraf bunu yeni bir
          REST snapshot ile resync tetiklemesi gerektiğinin sinyali
          olarak ele almalıdır.
        """
        if event.symbol != self.symbol:
            raise ValueError(f"symbol uyuşmazlığı: {event.symbol} != {self.symbol}")

        if self._state != OrderBookSyncState.SYNCED:
            self._buffer_event(event)
            return None

        assert self._last_update_id is not None  # SYNCED iken her zaman set edilmiştir
        expected_next_first_id = self._last_update_id + 1
        if event.final_update_id <= self._last_update_id:
            # Bu event zaten uygulanmış aralığın içinde kalıyor (stale/replay);
            # sessizce yok say — Binance dokümantasyonu bu durumun normal
            # olabileceğini belirtir (örn. reconnect sonrası tekrar gelen eski
            # event). Gap DEĞİLDİR, hata fırlatılmaz.
            return self._build_snapshot(event.event_time)
        if event.first_update_id > expected_next_first_id:
            # GERÇEK gap: event.U, beklenenden BÜYÜK (aralarında hiç
            # kapsanmayan update'ler var). event.U <= expected_next_first_id
            # olan durumlar (tam eşleşme VEYA overlap) KABUL EDİLİR.
            self._state = OrderBookSyncState.UNSYNCED
            self._buffer.clear()
            self._buffer_event(event)
            raise OrderBookSyncError(
                f"{self.symbol}: sequence gap — beklenen U<={expected_next_first_id}, "
                f"alınan U={event.first_update_id} (u={event.final_update_id}). "
                f"Resync (yeni REST snapshot) gerekli."
            )

        self._apply_diff_unchecked(event)
        return self._build_snapshot(event.event_time)

    def ingest_snapshot(
        self,
        last_update_id: int,
        bids: tuple[tuple[float, float], ...],
        asks: tuple[tuple[float, float], ...],
        snapshot_timestamp: datetime,
    ) -> OrderBookSnapshot:
        """Bir REST depth snapshot'ını buffer'lanmış diff'lerle birleştirir.

        Binance algoritması adım 3-5: eski event'leri at, köprü event'ini
        doğrula, kalan buffer'ı sırayla uygula.
        """
        require_utc_aware(snapshot_timestamp, "snapshot_timestamp")
        if last_update_id < 0:
            raise ValueError("last_update_id negatif olamaz")

        self._bids = {price: qty for price, qty in bids if qty > 0}
        self._asks = {price: qty for price, qty in asks if qty > 0}
        self._last_update_id = last_update_id

        # Adım 3: snapshot'tan eski/aynı-anlık event'leri at.
        relevant = [e for e in self._buffer if e.final_update_id > last_update_id]
        self._buffer.clear()

        if relevant:
            first = relevant[0]
            # Adım 4: köprü koşulu.
            if not (first.first_update_id <= last_update_id + 1 <= first.final_update_id):
                self._state = OrderBookSyncState.UNSYNCED
                raise OrderBookSyncError(
                    f"{self.symbol}: buffer'daki ilk event snapshot'a köprü kuramıyor "
                    f"(lastUpdateId={last_update_id}, ilk event U={first.first_update_id}, "
                    f"u={first.final_update_id}). Yeni bir snapshot gerekli."
                )
            # Adım 5: kalan event'lerin sürekliliğini OVERLAP semantiğiyle
            # doğrulayıp uygula (strict equality ZORUNLU DEĞİL).
            for i, ev in enumerate(relevant):
                if i > 0:
                    prev = relevant[i - 1]
                    if ev.first_update_id > prev.final_update_id + 1:
                        self._state = OrderBookSyncState.UNSYNCED
                        raise OrderBookSyncError(
                            f"{self.symbol}: buffer içinde sequence gap — beklenen "
                            f"U<={prev.final_update_id + 1}, alınan U={ev.first_update_id}."
                        )
                self._apply_diff_unchecked(ev)

        self._state = OrderBookSyncState.SYNCED
        return self._build_snapshot(snapshot_timestamp)

    def _buffer_event(self, event: DepthDiffEvent) -> None:
        if len(self._buffer) >= MAX_PRE_SYNC_BUFFER_SIZE:
            raise OrderBookSyncError(
                f"{self.symbol}: snapshot gelmeden önce diff buffer'ı "
                f"{MAX_PRE_SYNC_BUFFER_SIZE} sınırını aştı — bounded backpressure "
                f"gereği event alınmadı; resync/backfill gerekli (Bölüm 14)."
            )
        self._buffer.append(event)

    def _apply_diff_unchecked(self, event: DepthDiffEvent) -> None:
        for price, qty in event.bids:
            if qty == 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty
        for price, qty in event.asks:
            if qty == 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty
        self._last_update_id = event.final_update_id

    def _build_snapshot(self, timestamp: datetime) -> OrderBookSnapshot:
        bids_sorted = sorted(self._bids.items(), key=lambda item: item[0], reverse=True)
        asks_sorted = sorted(self._asks.items(), key=lambda item: item[0])

        if not bids_sorted or not asks_sorted:
            self._state = OrderBookSyncState.UNSYNCED
            raise OrderBookSyncError(
                f"{self.symbol}: bir taraf boş kaldı (bids={len(bids_sorted)}, "
                f"asks={len(asks_sorted)}) — canonical snapshot üretilemez, resync gerekli."
            )
        if bids_sorted[0][0] >= asks_sorted[0][0]:
            self._state = OrderBookSyncState.UNSYNCED
            raise OrderBookSyncError(
                f"{self.symbol}: crossed/locked book tespit edildi "
                f"(best_bid={bids_sorted[0][0]} >= best_ask={asks_sorted[0][0]}) — "
                f"canonical olarak kabul edilemez, resync gerekli."
            )

        return OrderBookSnapshot(
            symbol=self.symbol,
            timestamp=timestamp,
            bids=tuple(OrderBookLevel(price=p, quantity=q) for p, q in bids_sorted),
            asks=tuple(OrderBookLevel(price=p, quantity=q) for p, q in asks_sorted),
            last_update_id=self._last_update_id,  # type: ignore[arg-type]
        )


=== FILE: crypto_signal_engine/providers/binance/parser.py ===
"""
Binance payload parser.

Kural (Bölüm 15): Binance response structure'larına kör güvenilmez. Bu
modül, ham Binance JSON/dict payload'larını Faz 1 domain modellerine
çeviren TEK merkezi noktadır. `float(payload["p"])` gibi parsing
işlemleri business logic içine DAĞITILMAZ; hepsi burada, açık hata
mesajlarıyla ele alınır.

Her yardımcı fonksiyon eksik alan / None / yanlış tip / malformed sayı
durumunda `ParseError` fırlatır (silent exception YOK). NaN/inf hiçbir
şekilde domain'e ulaşmaz — bu, Candle/Trade/OrderBookLevel constructor'
larının zaten uyguladığı `require_finite` invariant'ı ile double-checked'tir.

Candle partial/final semantiği (Bölüm 6 dokümantasyon gereksinimi):
- WebSocket kline event'i: Binance'in kendi `k.x` (is this kline closed?)
  alanı DOĞRUDAN kullanılır — ekstra tahmin gerekmez.
- REST historical kline satırı: Binance REST yanıtında "kapalı mı" bilgisi
  YOKTUR (yalnızca open_time/close_time verilir). Bu yüzden
  `parse_kline_rest_row`, `now` parametresi verilirse `close_time <= now`
  karşılaştırmasıyla is_closed türetir; `now=None` ise (tipik backfill/
  geçmiş-aralık sorgusu) tüm satırlar `is_closed=True` kabul edilir. Bu
  politika burada AÇIKÇA dokümante edilmiştir ve rest.py bunu tutarlı
  şekilde kullanır.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, Trade
from crypto_signal_engine.errors import ParseError
from crypto_signal_engine.providers.binance.symbols import from_binance_wire_symbol


def _require_dict(payload: Any, context: str) -> dict:
    if not isinstance(payload, dict):
        raise ParseError(f"{context}: dict bekleniyordu, alınan: {type(payload).__name__}")
    return payload


def _get(payload: dict, key: str, context: str) -> Any:
    if key not in payload:
        raise ParseError(f"{context}: '{key}' alanı eksik. Payload anahtarları: {list(payload.keys())}")
    value = payload[key]
    if value is None:
        raise ParseError(f"{context}: '{key}' alanı None olamaz")
    return value


def _get_str(payload: dict, key: str, context: str) -> str:
    value = _get(payload, key, context)
    if not isinstance(value, str):
        raise ParseError(f"{context}: '{key}' str olmalı, alınan: {type(value).__name__}")
    return value


def _get_int(payload: dict, key: str, context: str) -> int:
    value = _get(payload, key, context)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ParseError(f"{context}: '{key}' int olmalı, alınan: {type(value).__name__}")
    return value


def _get_bool(payload: dict, key: str, context: str) -> bool:
    value = _get(payload, key, context)
    if not isinstance(value, bool):
        raise ParseError(f"{context}: '{key}' bool olmalı, alınan: {type(value).__name__}")
    return value


def _get_numeric_str(payload: dict, key: str, context: str) -> float:
    """Binance'in fiyat/miktar alanları için tipik format: sayısal bir string.

    Malformed string ("", "abc", "NaN", "inf") KESİNLİKLE reddedilir.
    """
    raw = _get(payload, key, context)
    if not isinstance(raw, str):
        raise ParseError(f"{context}: '{key}' sayısal string olmalı, alınan tip: {type(raw).__name__}")
    try:
        value = float(raw)
    except ValueError as exc:
        raise ParseError(f"{context}: '{key}' geçerli bir sayı değil: {raw!r}") from exc
    if value != value or value in (float("inf"), float("-inf")):
        raise ParseError(f"{context}: '{key}' NaN/inf olamaz: {raw!r}")
    return value


def _get_str_list(payload: dict, key: str, context: str) -> tuple[str, ...]:
    value = payload.get(key)
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ParseError(f"{context}: '{key}' bir string listesi olmalı, alınan: {value!r}")
    return tuple(value)


# --------------------------------------------------------------------------
# Otomatik sembol seçimi (`crypto_signal_engine.selection`) — yalnızca PUBLIC
# `/api/v3/exchangeInfo` ve `/api/v3/ticker/24hr` discovery endpoint'leri.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ExchangeSymbolInfo:
    """`/api/v3/exchangeInfo`'nun `symbols[]` dizisindeki TEK bir girdinin,
    otomatik sembol seçimi için gereken alanlara indirgenmiş hali."""

    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    is_spot_trading_allowed: bool
    permissions: tuple[str, ...]


def parse_exchange_info_response(payload: object) -> list[ExchangeSymbolInfo]:
    """`/api/v3/exchangeInfo` yanıtını (credential/imza GEREKTİRMEYEN,
    tamamen public bir endpoint) `ExchangeSymbolInfo` listesine çevirir.

    Binance'in resmi (ve son derece kararlı) response şeması dışında bir
    şey görülürse `ParseError` fırlatılır — bu bir batch parse'tır, TEK bir
    malformed satır bile SESSİZCE atlanmaz (bkz. modül docstring'i,
    "silent exception YOK")."""
    _require_dict(payload, "exchange_info_response")
    raw_symbols = payload.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ParseError("exchange_info_response: 'symbols' bir liste olmalı")

    results: list[ExchangeSymbolInfo] = []
    for row in raw_symbols:
        _require_dict(row, "exchange_info_response.symbols[]")
        results.append(
            ExchangeSymbolInfo(
                symbol=_get_str(row, "symbol", "exchange_info_response.symbols[]"),
                base_asset=_get_str(row, "baseAsset", "exchange_info_response.symbols[]"),
                quote_asset=_get_str(row, "quoteAsset", "exchange_info_response.symbols[]"),
                status=_get_str(row, "status", "exchange_info_response.symbols[]"),
                is_spot_trading_allowed=_get_bool(
                    row, "isSpotTradingAllowed", "exchange_info_response.symbols[]"
                ),
                permissions=_get_str_list(row, "permissions", "exchange_info_response.symbols[]"),
            )
        )
    return results


@dataclass(frozen=True)
class Ticker24hr:
    """`/api/v3/ticker/24hr`'ın (sembol parametresi OLMADAN, TÜM semboller
    için TEK bir çağrıda) döndürdüğü TEK bir satırın, otomatik sembol
    seçimi için gereken alanlara indirgenmiş hali."""

    symbol: str
    quote_volume: float
    price_change_percent: float
    trade_count: int


def parse_24hr_ticker_response(payload: object) -> list[Ticker24hr]:
    """`/api/v3/ticker/24hr` (sembolsüz, tüm semboller) yanıtını
    `Ticker24hr` listesine çevirir. Bkz. `parse_exchange_info_response`
    docstring'i — aynı "malformed satır sessizce atlanmaz" disiplini."""
    if not isinstance(payload, list):
        raise ParseError(f"ticker_24hr_response: liste bekleniyordu, alınan: {type(payload).__name__}")

    results: list[Ticker24hr] = []
    for row in payload:
        _require_dict(row, "ticker_24hr_response[]")
        results.append(
            Ticker24hr(
                symbol=_get_str(row, "symbol", "ticker_24hr_response[]"),
                quote_volume=_get_numeric_str(row, "quoteVolume", "ticker_24hr_response[]"),
                price_change_percent=_get_numeric_str(row, "priceChangePercent", "ticker_24hr_response[]"),
                trade_count=_get_int(row, "count", "ticker_24hr_response[]"),
            )
        )
    return results


def _epoch_ms_to_utc(epoch_ms: int, context: str) -> datetime:
    if epoch_ms < 0:
        raise ParseError(f"{context}: epoch millisecond negatif olamaz: {epoch_ms}")
    try:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ParseError(f"{context}: epoch millisecond geçersiz: {epoch_ms} ({exc})") from exc


# --------------------------------------------------------------------------
# Kline (candle)
# --------------------------------------------------------------------------

def parse_kline_ws_event(payload: dict, *, received_at: datetime) -> CandleUpdate:
    """Binance kline WebSocket event'ini `CandleUpdate`'e çevirir.

    update_seq politikası: Binance'in event-level `E` (event time, ms)
    alanı kullanılır. Bu alan aynı stream içinde monotonik olmayan-azalan
    (non-decreasing) olması beklenir; CandleSequencer aynı değeri
    duplicate, küçük değeri out-of-order olarak ele alır (bkz. DECISIONS.md
    — "Binance update_seq politikası: event time (E)").
    """
    _require_dict(payload, "kline_ws_event")
    if _get_str(payload, "e", "kline_ws_event") != "kline":
        raise ParseError(f"kline_ws_event: beklenmeyen event tipi: {payload.get('e')!r}")

    event_time_ms = _get_int(payload, "E", "kline_ws_event")
    wire_symbol = _get_str(payload, "s", "kline_ws_event")
    k = _require_dict(_get(payload, "k", "kline_ws_event"), "kline_ws_event.k")

    interval_str = _get_str(k, "i", "kline_ws_event.k")
    timeframe = _binance_interval_to_timeframe(interval_str)

    candle = Candle(
        symbol=from_binance_wire_symbol(wire_symbol),
        timeframe=timeframe,
        open_time=_epoch_ms_to_utc(_get_int(k, "t", "kline_ws_event.k"), "kline_ws_event.k.t"),
        close_time=_epoch_ms_to_utc(_get_int(k, "T", "kline_ws_event.k"), "kline_ws_event.k.T"),
        open=_get_numeric_str(k, "o", "kline_ws_event.k"),
        high=_get_numeric_str(k, "h", "kline_ws_event.k"),
        low=_get_numeric_str(k, "l", "kline_ws_event.k"),
        close=_get_numeric_str(k, "c", "kline_ws_event.k"),
        volume=_get_numeric_str(k, "v", "kline_ws_event.k"),
        is_closed=_get_bool(k, "x", "kline_ws_event.k"),
        trade_count=_get_int(k, "n", "kline_ws_event.k"),
    )
    event_time = _epoch_ms_to_utc(event_time_ms, "kline_ws_event.E")
    return CandleUpdate(
        candle=candle,
        update_seq=event_time_ms,
        event_time=event_time,
        received_at=received_at,
    )


def parse_kline_rest_row(
    row: list, *, symbol: str, timeframe: Timeframe, now: datetime | None = None
) -> Candle:
    """Binance REST `/api/v3/klines` yanıtındaki tek bir satırı Candle'a çevirir.

    Satır formatı (resmi Spot API): [open_time, open, high, low, close,
    volume, close_time, quote_volume, trade_count, taker_buy_base,
    taker_buy_quote, ignore].

    `is_closed` türetme politikası için bkz. modül docstring'i.
    """
    context = "kline_rest_row"
    if not isinstance(row, list) or len(row) < 9:
        raise ParseError(f"{context}: en az 9 elemanlı bir liste bekleniyordu, alınan: {row!r}")

    def _row_int(index: int, name: str) -> int:
        value = row[index]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ParseError(f"{context}[{index}] ({name}): int olmalı, alınan: {type(value).__name__}")
        return value

    def _row_numeric_str(index: int, name: str) -> float:
        value = row[index]
        if not isinstance(value, str):
            raise ParseError(f"{context}[{index}] ({name}): sayısal string olmalı, alınan: {type(value).__name__}")
        try:
            parsed = float(value)
        except ValueError as exc:
            raise ParseError(f"{context}[{index}] ({name}): geçerli sayı değil: {value!r}") from exc
        if parsed != parsed or parsed in (float("inf"), float("-inf")):
            raise ParseError(f"{context}[{index}] ({name}): NaN/inf olamaz")
        return parsed

    open_time = _epoch_ms_to_utc(_row_int(0, "open_time"), context)
    close_time = _epoch_ms_to_utc(_row_int(6, "close_time"), context)
    trade_count = _row_int(8, "trade_count")

    is_closed = True if now is None else close_time <= now

    return Candle(
        symbol=symbol,
        timeframe=timeframe,
        open_time=open_time,
        close_time=close_time,
        open=_row_numeric_str(1, "open"),
        high=_row_numeric_str(2, "high"),
        low=_row_numeric_str(3, "low"),
        close=_row_numeric_str(4, "close"),
        volume=_row_numeric_str(5, "volume"),
        is_closed=is_closed,
        trade_count=trade_count,
    )


_BINANCE_INTERVAL_TO_TIMEFRAME: dict[str, Timeframe] = {
    "1m": Timeframe.M1,
    "5m": Timeframe.M5,
    "15m": Timeframe.M15,
    "1h": Timeframe.H1,
}


def _binance_interval_to_timeframe(interval: str) -> Timeframe:
    """Binance interval string'ini Phase 1 Timeframe enum'una çevirir.

    Bilinmeyen interval SESSİZCE kabul edilmez (Bölüm 4) — açık `ParseError`.
    """
    try:
        return _BINANCE_INTERVAL_TO_TIMEFRAME[interval]
    except KeyError as exc:
        raise ParseError(
            f"Desteklenmeyen/bilinmeyen Binance interval: {interval!r}. "
            f"Desteklenenler: {list(_BINANCE_INTERVAL_TO_TIMEFRAME)}"
        ) from exc


# --------------------------------------------------------------------------
# Aggregate trade
# --------------------------------------------------------------------------

def parse_agg_trade_ws_event(payload: dict) -> Trade:
    """Binance `aggTrade` WebSocket event'ini `Trade`'e çevirir."""
    _require_dict(payload, "agg_trade_ws_event")
    if _get_str(payload, "e", "agg_trade_ws_event") != "aggTrade":
        raise ParseError(f"agg_trade_ws_event: beklenmeyen event tipi: {payload.get('e')!r}")

    wire_symbol = _get_str(payload, "s", "agg_trade_ws_event")
    trade_id = _get_int(payload, "a", "agg_trade_ws_event")
    price = _get_numeric_str(payload, "p", "agg_trade_ws_event")
    quantity = _get_numeric_str(payload, "q", "agg_trade_ws_event")
    trade_time_ms = _get_int(payload, "T", "agg_trade_ws_event")
    is_buyer_maker = _get_bool(payload, "m", "agg_trade_ws_event")

    return Trade(
        symbol=from_binance_wire_symbol(wire_symbol),
        trade_id=trade_id,
        price=price,
        quantity=quantity,
        timestamp=_epoch_ms_to_utc(trade_time_ms, "agg_trade_ws_event.T"),
        is_buyer_maker=is_buyer_maker,
    )


# --------------------------------------------------------------------------
# Order book: REST snapshot
# --------------------------------------------------------------------------

def parse_depth_levels(raw_levels: Any, context: str) -> tuple[OrderBookLevel, ...]:
    """Binance'in `[["price", "qty"], ...]` formatındaki seviye listesini
    `OrderBookLevel` tuple'ına çevirir. `quantity == 0` olan seviyeler
    (diff event'lerde "sil" anlamına gelir) burada FİLTRELENMEZ — bu
    kararı çağıran kod (order_book_sync.py) verir, çünkü bir snapshot'ta
    quantity=0 anlamsızken bir diff'te "sil" anlamına gelir.
    """
    if not isinstance(raw_levels, list):
        raise ParseError(f"{context}: liste bekleniyordu, alınan: {type(raw_levels).__name__}")
    levels = []
    for i, entry in enumerate(raw_levels):
        if not isinstance(entry, list) or len(entry) < 2:
            raise ParseError(f"{context}[{i}]: [price, qty] formatında olmalı, alınan: {entry!r}")
        price_raw, qty_raw = entry[0], entry[1]
        if not isinstance(price_raw, str) or not isinstance(qty_raw, str):
            raise ParseError(f"{context}[{i}]: price/qty string olmalı")
        try:
            price = float(price_raw)
            qty = float(qty_raw)
        except ValueError as exc:
            raise ParseError(f"{context}[{i}]: geçersiz sayı: {entry!r}") from exc
        for name, value in (("price", price), ("qty", qty)):
            if value != value or value in (float("inf"), float("-inf")):
                raise ParseError(f"{context}[{i}]: {name} NaN/inf olamaz")
        levels.append((price, qty))
    return tuple(levels)  # type: ignore[return-value]  -- order_book_sync bunu OrderBookLevel'a çevirir


def parse_depth_snapshot_response(payload: dict) -> tuple[int, tuple[tuple[float, float], ...], tuple[tuple[float, float], ...]]:
    """Binance `/api/v3/depth` REST yanıtını parse eder.

    Döndürülen değer: (last_update_id, bids, asks) — (price, qty) çiftleri
    halinde ham tuple'lar. `OrderBookSnapshot` inşası order_book_sync.py'de
    yapılır çünkü snapshot'ın domain `timestamp` alanı Binance tarafından
    SAĞLANMAZ (yalnızca `lastUpdateId` verilir) — bu politika modül
    docstring'inde dokümante edilmiştir: timestamp, çağıran kod tarafından
    `received_at` (fetch anındaki UTC zaman) olarak atanır.
    """
    _require_dict(payload, "depth_snapshot_response")
    last_update_id = _get_int(payload, "lastUpdateId", "depth_snapshot_response")
    bids = parse_depth_levels(_get(payload, "bids", "depth_snapshot_response"), "depth_snapshot_response.bids")
    asks = parse_depth_levels(_get(payload, "asks", "depth_snapshot_response"), "depth_snapshot_response.asks")
    return last_update_id, bids, asks  # type: ignore[return-value]


# --------------------------------------------------------------------------
# Order book: WebSocket diff event
# --------------------------------------------------------------------------

def parse_depth_diff_ws_event(payload: dict) -> "DepthDiffEvent":
    """Binance `depthUpdate` WebSocket event'ini `DepthDiffEvent`'e çevirir.

    `DepthDiffEvent` domain modeli DEĞİLDİR (Phase 1'e eklenmez); bu,
    order_book_sync.py'nin senkronizasyon algoritması için kullandığı
    Phase 2'ye özgü, geçici bir taşıyıcıdır (bkz. order_book_sync.py).
    """
    from crypto_signal_engine.providers.binance.order_book_sync import DepthDiffEvent

    _require_dict(payload, "depth_diff_ws_event")
    if _get_str(payload, "e", "depth_diff_ws_event") != "depthUpdate":
        raise ParseError(f"depth_diff_ws_event: beklenmeyen event tipi: {payload.get('e')!r}")

    wire_symbol = _get_str(payload, "s", "depth_diff_ws_event")
    first_update_id = _get_int(payload, "U", "depth_diff_ws_event")
    final_update_id = _get_int(payload, "u", "depth_diff_ws_event")
    event_time_ms = _get_int(payload, "E", "depth_diff_ws_event")
    bids = parse_depth_levels(_get(payload, "b", "depth_diff_ws_event"), "depth_diff_ws_event.b")
    asks = parse_depth_levels(_get(payload, "a", "depth_diff_ws_event"), "depth_diff_ws_event.a")

    return DepthDiffEvent(
        symbol=from_binance_wire_symbol(wire_symbol),
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        event_time=_epoch_ms_to_utc(event_time_ms, "depth_diff_ws_event.E"),
        bids=bids,
        asks=asks,
    )


=== FILE: crypto_signal_engine/providers/binance/provider.py ===
"""
BinanceMarketDataProvider — `LiveDataProvider` (Phase 1) için Binance
public/read-only market data implementasyonu.

Mimari not (Bölüm 2): Bu dosya, WS mesaj alma + parse + sequencing +
quality + state commit akışının orkestrasyonunu yapar; alt sorumluluklar
(parser, order_book_sync, reconnect, config, symbols, transport, clock)
ayrı modüllere bölünmüştür — hiçbir şey tek bir dev dosyaya sıkıştırılmadı.

Mimari netleştirme (Phase 1 ABC ile ilişki): `LiveDataProvider.stream_candles`
yalnızca `AsyncIterator[Candle]` döndürür — `CandleUpdate` (update_seq/
event_time taşıyan) DIŞARI SIZDIRILMAZ. Bu yüzden candle sequencing +
quality-gate + state-commit TAMAMEN bu provider'ın İÇİNDE, `StateManager`
aracılığıyla yapılır; dışarıya yalnızca ZATEN KABUL EDİLMİŞ (committed)
canonical `Candle` nesneleri yayınlanır. Bu, Phase 1 domain contract'ını
DEĞİŞTİRMEDEN, "DataQualityGate canonical state commit'ten ÖNCE çalışır"
invariant'ını korur.

`health()` imzası (Phase 1 ABC) symbol parametresi ALMAZ. Bu provider
birden fazla symbol/stream'i eşzamanlı besleyebildiğinden, `health()` en
son güncellenen stream'in `ProviderHealthSnapshot`'ını döndürür (hiç
stream başlatılmamışsa `config.supported_symbols[0]` — veya o da boşsa
"UNKNOWN" placeholder'ı — ile DISCONNECTED durumu döner). Bu davranış
PHASE2_MARKET_DATA.md'de açıkça belgelenmiştir.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade
from crypto_signal_engine.domain.state_contract import CandleStateStore, CommitOutcome, InMemoryCandleStateStore
from crypto_signal_engine.errors import OrderBookSyncError, ParseError, StaleFeedError, TransportError
from crypto_signal_engine.providers.base import ConnectionState, LiveDataProvider, ProviderHealthSnapshot
from crypto_signal_engine.providers.binance import parser
from crypto_signal_engine.providers.binance.clock import Clock, Sleeper, SystemClock, AsyncioSleeper
from crypto_signal_engine.providers.binance.config import BinanceConfig, binance_interval_for
from crypto_signal_engine.providers.binance.order_book_sync import OrderBookSynchronizer
from crypto_signal_engine.providers.binance.reconnect import ReconnectPolicy
from crypto_signal_engine.providers.binance.rest import BinanceRestClient
from crypto_signal_engine.providers.binance.symbols import (
    agg_trade_stream_name,
    depth_diff_stream_name,
    kline_stream_name,
)
from crypto_signal_engine.providers.binance.transport import HttpClient, WebSocketConnectionFactory
from crypto_signal_engine.quality.base import DataQualityGate
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate
from crypto_signal_engine.state.manager import StateManager

_TIMEFRAME_DURATION: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}


def _backfill_update_seq(candle: Candle) -> int:
    """REST backfill/historical candle'lar için synthetic update_seq politikası.

    KRİTİK: `close_time` GENEL sequencing identity/update_seq olarak
    KULLANILMAZ (Faz 1 kararı — "close_time sequence değildir" — ihlal
    edilmez). Bunun yerine, candle identity'sinin zaten bir parçası olan
    `open_time` (ms) kullanılır. Bu, live WS update_seq politikasından
    (event time `E`, bkz. parser.py) AÇIKÇA AYRI ve dokümante edilmiş bir
    politikadır — iki politika birbirine karıştırılmamalıdır.
    """
    return int(candle.open_time.timestamp() * 1000)


class BinanceMarketDataProvider(LiveDataProvider):
    """Binance public market data için `LiveDataProvider` implementasyonu."""

    def __init__(
        self,
        config: BinanceConfig,
        http_client: HttpClient,
        ws_factory: WebSocketConnectionFactory,
        clock: Clock | None = None,
        sleeper: Sleeper | None = None,
        quality_gate: DataQualityGate | None = None,
        candle_store: CandleStateStore | None = None,
        queue_maxsize: int = 1000,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError(f"queue_maxsize pozitif olmalı, alınan: {queue_maxsize}")
        self._config = config
        self._http_client = http_client
        self._ws_factory = ws_factory
        self._clock = clock or SystemClock()
        self._sleeper = sleeper or AsyncioSleeper()
        self._quality_gate = quality_gate or BinanceDataQualityGate(self._clock)
        self._state_manager = StateManager(self._quality_gate, candle_store or InMemoryCandleStateStore())
        self._rest = BinanceRestClient(config, http_client, self._clock, self._sleeper)
        self._queue_maxsize = queue_maxsize

        self._health: dict[str, ProviderHealthSnapshot] = {}
        self._last_health_key: str | None = None
        self._background_tasks: set[asyncio.Task] = set()
        self._closed = False

    # -- Health --------------------------------------------------------------

    def _set_health(
        self, key: str, symbol: str, state: ConnectionState, *, last_message_at: datetime | None, reconnect_count: int
    ) -> None:
        self._health[key] = ProviderHealthSnapshot(
            connection_state=state,
            last_message_at=last_message_at,
            reconnect_count=reconnect_count,
            symbol=symbol,
        )
        self._last_health_key = key

    def health(self) -> ProviderHealthSnapshot:
        if self._last_health_key is not None:
            return self._health[self._last_health_key]
        default_symbol = self._config.supported_symbols[0] if self._config.supported_symbols else "UNKNOWN"
        return ProviderHealthSnapshot(
            connection_state=ConnectionState.DISCONNECTED,
            last_message_at=None,
            reconnect_count=0,
            symbol=default_symbol,
        )

    def health_for(self, key: str) -> ProviderHealthSnapshot | None:
        """Belirli bir stream anahtarının ("candle:BTCUSDT:1m" gibi) sağlığını
        döndürür. Phase 1 ABC'sinin parçası DEĞİLDİR; test edilebilirlik ve
        çoklu-stream gözlemlenebilirliği için ek bir sorgu noktasıdır."""
        return self._health.get(key)

    # -- Stale-feed watchdog ---------------------------------------------------

    async def _recv_or_stale(self, connection, threshold_seconds: float) -> str:
        """`connection.recv()`'i `threshold_seconds` ile sınırlar.

        TASARIM NOTU: Bu metod GERÇEK event-loop zamanını (`asyncio.wait_for`)
        kullanır — reconnect backoff'ta olduğu gibi injectable `Sleeper`
        KULLANILMAZ. Bir I/O idle-timeout'u doğası gereği gerçek geçen
        süreye dayanmalıdır; bu, "şimdi" için domain-seviyesi freshness
        kararlarında kullanılan injectable `Clock`'tan (business-level
        semantik) kavramsal olarak FARKLI bir düşük-seviye mekanizmadır.
        Testler bunu saniyelerce değil, çok küçük (örn. 0.02-0.05s) bir
        `stale_feed_threshold_seconds` ile deterministik ve hızlı biçimde
        doğrular (bkz. DECISIONS.md — bu bilinçli bir tasarım kararıdır).

        `asyncio.CancelledError` (normal shutdown) ile `TimeoutError`
        (stale-feed) birbirinden OTOMATİK olarak ayrılır: `wait_for`, dış
        bir cancellation'ı olduğu gibi yukarı fırlatır (CancelledError),
        yalnızca KENDİ iç zaman aşımını `TimeoutError`'a çevirir — bu
        yüzden cancellation asla stale-reconnect olarak yorumlanamaz.
        """
        try:
            return await asyncio.wait_for(connection.recv(), timeout=threshold_seconds)
        except TimeoutError as exc:
            raise StaleFeedError(
                f"{threshold_seconds}s içinde mesaj alınamadı (stale feed)"
            ) from exc

    def _last_message_at_for(self, key: str) -> datetime | None:
        existing = self._health.get(key)
        return existing.last_message_at if existing is not None else None

    # -- Historical (REST) ---------------------------------------------------

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        return await self._rest.fetch_historical_candles(symbol, timeframe, start, end)

    # -- Candle stream ---------------------------------------------------------

    async def stream_candles(self, symbol: str, timeframe: Timeframe) -> AsyncIterator[Candle]:
        symbol = normalize_symbol(symbol)
        if timeframe not in self._config.supported_timeframes:
            raise ValueError(f"timeframe {timeframe} config.supported_timeframes içinde değil")

        key = f"candle:{symbol}:{timeframe.value}"
        queue: asyncio.Queue[Candle] = asyncio.Queue(maxsize=self._queue_maxsize)
        task = asyncio.create_task(self._run_candle_stream(symbol, timeframe, key, queue))
        self._background_tasks.add(task)
        try:
            while True:
                candle = await queue.get()
                yield candle
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._background_tasks.discard(task)

    async def _run_candle_stream(
        self, symbol: str, timeframe: Timeframe, key: str, queue: asyncio.Queue[Candle]
    ) -> None:
        binance_interval = binance_interval_for(timeframe)
        stream_name = kline_stream_name(symbol, binance_interval)
        url = f"{self._config.ws_base_url}/ws/{stream_name}"
        reconnect_policy = ReconnectPolicy(self._config.reconnect, self._sleeper)
        reconnect_count = 0

        while not self._closed:
            self._set_health(key, symbol, ConnectionState.CONNECTING, last_message_at=None, reconnect_count=reconnect_count)
            try:
                connection = await self._ws_factory.connect(url)
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=None, reconnect_count=reconnect_count)
                await reconnect_policy.sleep_before_next_attempt()
                continue

            reconnect_policy.reset()
            self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=None, reconnect_count=reconnect_count)
            try:
                await self._pump_candle_connection(symbol, timeframe, key, connection, queue, reconnect_count)
            except asyncio.CancelledError:
                await connection.close()
                raise
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=self._clock.now(), reconnect_count=reconnect_count)
                await connection.close()
                await reconnect_policy.sleep_before_next_attempt()
                continue

    async def _pump_candle_connection(
        self,
        symbol: str,
        timeframe: Timeframe,
        key: str,
        connection,
        queue: asyncio.Queue[Candle],
        reconnect_count: int,
    ) -> None:
        while True:
            try:
                raw = await self._recv_or_stale(connection, self._config.stale_feed_threshold_seconds)
            except StaleFeedError:
                self._set_health(
                    key, symbol, ConnectionState.DEGRADED,
                    last_message_at=self._last_message_at_for(key), reconnect_count=reconnect_count,
                )
                raise  # StaleFeedError bir TransportError'dır; dış reconnect handler'ı tetikler.
            received_at = self._clock.now()
            self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=received_at, reconnect_count=reconnect_count)

            try:
                payload = json.loads(raw)
                update = parser.parse_kline_ws_event(payload, received_at=received_at)
            except (ParseError, json.JSONDecodeError):
                # Bölüm 15/17: parser hatası retry-safe DEĞİLDİR; bu tek
                # mesaj atlanır (drop OBSERVABLE'dır — health değişmez ama
                # bu davranış PHASE2_MARKET_DATA.md'de dokümante edilmiştir),
                # stream'in kendisi düşürülmez.
                continue

            result = self._state_manager.handle_candle_update(update)

            if result.commit_result.outcome == CommitOutcome.REJECTED_QUALITY:
                if result.quality_result.status == DataQualityStatus.MISSING_CANDLE:
                    try:
                        recovered = await self._backfill_candle_gap(symbol, timeframe, update.candle.open_time)
                    except (ParseError, ValueError) as exc:
                        # Bölüm 17: malformed/structural olarak geçersiz bir
                        # backfill satırı retry-safe DEĞİLDİR ama stream'in
                        # KENDİSİNİ çökertmemelidir — bu deneme başarısız
                        # sayılır (recovered=False), orijinal update tekrar
                        # DENENMEZ; bir sonraki mesajda gap yeniden tespit
                        # edilip yeniden denenecektir.
                        recovered = False
                    if recovered:
                        # Gap kapatıldıktan sonra AYNI update'i tekrar dene.
                        retry_result = self._state_manager.handle_candle_update(update)
                        if retry_result.commit_result.committed:
                            await queue.put(retry_result.commit_result.canonical_candle)
                continue

            if result.commit_result.committed:
                await queue.put(result.commit_result.canonical_candle)
            # REJECTED_SEQUENCE (duplicate/out-of-order/after-final): sessizce
            # atlanır — bu Phase 1 CandleSequencer'ın zaten beklenen/normal
            # bir davranışıdır (örn. reconnect sonrası replay), hata DEĞİLDİR.

    async def _backfill_candle_gap(
        self, symbol: str, timeframe: Timeframe, incoming_open_time: datetime
    ) -> bool:
        """Bölüm 25 — Historical Gap Recovery (DETERMINISTIC).

        Eksik aralık, canonical state'ten (StateManager'ın PUBLIC
        `latest_candle()` API'si üzerinden — private attribute hack
        KULLANILMAZ) tam olarak hesaplanır: `[latest.open_time + duration,
        incoming_open_time)`. Sabit/kör bir pencere KULLANILMAZ.

        Backfill candle'lar için update_seq politikası: `close_time`
        GENEL sequencing identity'si olarak kullanılmaz (Faz 1 kararı —
        "close_time sequence değildir" — ihlal edilmez). Bunun yerine,
        candle identity'sinin zaten bir parçası olan `open_time` (ms)
        synthetic backfill update_seq olarak kullanılır — bu, live WS
        update_seq politikasından (event time `E`) AYRI ve AÇIKÇA
        dokümante edilmiş bir politikadır (bkz. DECISIONS.md).

        Dönüş değeri: backfill'in eksik aralığın TAMAMINI karşılayıp
        karşılamadığı (`True`/`False`). Karşılamıyorsa orijinal update
        TEKRAR DENENMEZ (bir sonraki mesajda gap tekrar tespit edilip
        yeniden denenecektir) — "REST response gap'in tamamını
        doldurmuyorsa recovery başarılı sayılmaz" kuralı.
        """
        duration = _TIMEFRAME_DURATION[timeframe]
        previous = self._state_manager.latest_candle(symbol, timeframe)
        if previous is None:
            # Önceki candle bilinmiyorsa gerçek bir "eksik interval"
            # hesaplanamaz; bu durumda quality-gate zaten MISSING_CANDLE
            # üretmemeliydi (previous=None iken gap kontrolü atlanır) —
            # defensive no-op.
            return False

        gap_start = previous.open_time + duration
        gap_end = incoming_open_time
        if gap_start >= gap_end:
            return False  # gap yok (savunma amaçlı)

        # Beklenen eksik open_time'ların TAM listesi (tamlık doğrulaması için).
        expected_open_times: list[datetime] = []
        cursor = gap_start
        while cursor < gap_end:
            expected_open_times.append(cursor)
            cursor += duration

        backfilled = await self._rest.fetch_historical_candles(symbol, timeframe, gap_start, gap_end)
        # KRİTİK SINIRLAMA: REST yanıtı, Binance'in endTime sınır davranışı
        # nedeniyle `incoming_open_time`'ın kendisini (veya gap dışı başka
        # bir candle'ı) DÖNDÜREBİLİR. Historical recovery yolu YALNIZCA
        # `expected_open_times` kümesindeki (yani KESİNLİKLE eksik olan)
        # candle'ları commit etmelidir — `incoming_open_time`'a ait candle
        # (veya gap dışındaki herhangi biri) bu yoldan ASLA commit edilmez;
        # o, orijinal WS update'i üzerinden (retry ile) canonical hâle gelir.
        expected_open_times_set = set(expected_open_times)
        closed_backfilled = [
            c for c in backfilled if c.is_closed and c.open_time in expected_open_times_set
        ]
        returned_open_times = {c.open_time for c in closed_backfilled}

        recovery_complete = all(t in returned_open_times for t in expected_open_times)

        for candle in sorted(closed_backfilled, key=lambda c: c.open_time):
            # Partial/açık candle historical recovery'ye YANLIŞLIKLA commit
            # edilmez (zaten `closed_backfilled` filtresiyle elenmiş, bu
            # ikinci bir defensive kontroldür).
            if not candle.is_closed:
                continue
            update = CandleUpdate(
                candle=candle,
                update_seq=_backfill_update_seq(candle),
                event_time=candle.close_time,
                received_at=self._clock.now(),
            )
            # StateManager zaten duplicate/out-of-order'ı deterministic
            # olarak ele alır (CandleSequencer) — backfill'in WS ile race
            # koşulunda tekrar denenmesi güvenlidir (idempotent).
            self._state_manager.handle_historical_candle_update(update)

        return recovery_complete

    # -- Trade stream ------------------------------------------------------

    async def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        symbol = normalize_symbol(symbol)
        key = f"trade:{symbol}"
        queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=self._queue_maxsize)
        task = asyncio.create_task(self._run_trade_stream(symbol, key, queue))
        self._background_tasks.add(task)
        try:
            while True:
                trade = await queue.get()
                yield trade
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._background_tasks.discard(task)

    async def _run_trade_stream(self, symbol: str, key: str, queue: asyncio.Queue[Trade]) -> None:
        stream_name = agg_trade_stream_name(symbol)
        url = f"{self._config.ws_base_url}/ws/{stream_name}"
        reconnect_policy = ReconnectPolicy(self._config.reconnect, self._sleeper)
        reconnect_count = 0

        while not self._closed:
            self._set_health(key, symbol, ConnectionState.CONNECTING, last_message_at=None, reconnect_count=reconnect_count)
            try:
                connection = await self._ws_factory.connect(url)
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=None, reconnect_count=reconnect_count)
                await reconnect_policy.sleep_before_next_attempt()
                continue

            reconnect_policy.reset()
            self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=None, reconnect_count=reconnect_count)
            try:
                while True:
                    try:
                        raw = await self._recv_or_stale(connection, self._config.stale_feed_threshold_seconds)
                    except StaleFeedError:
                        self._set_health(
                            key, symbol, ConnectionState.DEGRADED,
                            last_message_at=self._last_message_at_for(key), reconnect_count=reconnect_count,
                        )
                        raise
                    received_at = self._clock.now()
                    self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=received_at, reconnect_count=reconnect_count)
                    try:
                        payload = json.loads(raw)
                        trade = parser.parse_agg_trade_ws_event(payload)
                    except (ParseError, json.JSONDecodeError):
                        continue
                    quality_result = self._state_manager.handle_trade(trade, received_at)
                    if quality_result.passed:
                        await queue.put(trade)
            except asyncio.CancelledError:
                await connection.close()
                raise
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=self._clock.now(), reconnect_count=reconnect_count)
                await connection.close()
                await reconnect_policy.sleep_before_next_attempt()
                continue

    # -- Order book stream ---------------------------------------------------

    async def stream_order_book(self, symbol: str, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        symbol = normalize_symbol(symbol)
        key = f"orderbook:{symbol}"
        queue: asyncio.Queue[OrderBookSnapshot] = asyncio.Queue(maxsize=self._queue_maxsize)
        task = asyncio.create_task(self._run_order_book_stream(symbol, key, queue))
        self._background_tasks.add(task)
        try:
            while True:
                snapshot = await queue.get()
                yield snapshot
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._background_tasks.discard(task)

    async def _resync_order_book(self, symbol: str, synchronizer: OrderBookSynchronizer) -> OrderBookSnapshot:
        """Bölüm D — REST snapshot alıp buffer'lanmış diff'lerle senkronize eder.

        Başarılı resync sonucu üretilen canonical snapshot'ı DÖNDÜRÜR — bu,
        resync anında elde edilen ilk güvenilir book durumudur ve tüketiciye
        (queue) iletilmelidir; sessizce atlanırsa tüketici resync sonrası
        ilk kararlı state'i hiç göremez.
        """
        last_update_id, bids, asks, received_at = await self._rest.fetch_depth_snapshot(symbol)
        return synchronizer.ingest_snapshot(last_update_id, bids, asks, received_at)

    async def _run_order_book_stream(
        self, symbol: str, key: str, queue: asyncio.Queue[OrderBookSnapshot]
    ) -> None:
        stream_name = depth_diff_stream_name(symbol)
        url = f"{self._config.ws_base_url}/ws/{stream_name}"
        reconnect_policy = ReconnectPolicy(self._config.reconnect, self._sleeper)
        resync_policy = ReconnectPolicy(self._config.reconnect, self._sleeper)
        reconnect_count = 0
        synchronizer = OrderBookSynchronizer(symbol)

        while not self._closed:
            self._set_health(key, symbol, ConnectionState.CONNECTING, last_message_at=None, reconnect_count=reconnect_count)
            try:
                connection = await self._ws_factory.connect(url)
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=None, reconnect_count=reconnect_count)
                await reconnect_policy.sleep_before_next_attempt()
                continue

            reconnect_policy.reset()
            # Bölüm 8: reconnect sonrası order book MUTLAKA resync'e girer.
            synchronizer.reset()
            self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=None, reconnect_count=reconnect_count)

            try:
                while True:
                    try:
                        raw = await self._recv_or_stale(connection, self._config.stale_feed_threshold_seconds)
                    except StaleFeedError:
                        self._set_health(
                            key, symbol, ConnectionState.DEGRADED,
                            last_message_at=self._last_message_at_for(key), reconnect_count=reconnect_count,
                        )
                        raise
                    received_at = self._clock.now()
                    self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=received_at, reconnect_count=reconnect_count)
                    try:
                        payload = json.loads(raw)
                        diff_event = parser.parse_depth_diff_ws_event(payload)
                    except (ParseError, json.JSONDecodeError):
                        continue

                    try:
                        snapshot = synchronizer.ingest_diff(diff_event)
                    except OrderBookSyncError:
                        # Sequence gap: senkronizasyon bozuldu, resync gerekli.
                        self._set_health(key, symbol, ConnectionState.DEGRADED, last_message_at=received_at, reconnect_count=reconnect_count)
                        try:
                            resynced_snapshot = await self._resync_order_book(symbol, synchronizer)
                        except (TransportError, OrderBookSyncError):
                            await resync_policy.sleep_before_next_attempt()
                            continue
                        resync_policy.reset()
                        self._set_health(key, symbol, ConnectionState.CONNECTED, last_message_at=received_at, reconnect_count=reconnect_count)
                        quality_result = self._state_manager.handle_order_book(resynced_snapshot)
                        if quality_result.passed:
                            await queue.put(resynced_snapshot)
                        continue

                    if synchronizer.state.value == "UNSYNCED":
                        # Henüz sync değil (buffer'lanıyor); ilk snapshot'ı çek.
                        try:
                            resynced_snapshot = await self._resync_order_book(symbol, synchronizer)
                        except (TransportError, OrderBookSyncError):
                            await resync_policy.sleep_before_next_attempt()
                            continue
                        resync_policy.reset()
                        quality_result = self._state_manager.handle_order_book(resynced_snapshot)
                        if quality_result.passed:
                            await queue.put(resynced_snapshot)
                        continue

                    if snapshot is not None:
                        quality_result = self._state_manager.handle_order_book(snapshot)
                        if quality_result.passed:
                            await queue.put(snapshot)
            except asyncio.CancelledError:
                await connection.close()
                raise
            except TransportError:
                reconnect_count += 1
                self._set_health(key, symbol, ConnectionState.RECONNECTING, last_message_at=self._clock.now(), reconnect_count=reconnect_count)
                await connection.close()
                await reconnect_policy.sleep_before_next_attempt()
                continue

    # -- Shutdown ------------------------------------------------------------

    async def close(self) -> None:
        """Idempotent graceful shutdown: tüm background task'ları cancel eder."""
        self._closed = True
        tasks = list(self._background_tasks)
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._background_tasks.clear()


=== FILE: crypto_signal_engine/providers/binance/real_websocket.py ===
"""
Gerçek (opsiyonel) WebSocket implementasyonu — `websockets` kütüphanesi ile.

Kural (bkz. transport.py docstring'i): `WebSocketConnectionFactory` için
Faz 2'de yalnızca bir Protocol tanımlanmıştı; gerçek implementasyon
"ayrı, opsiyonel bir modülde, yalnızca gerçekten kullanılmak istendiğinde
import edilir" şeklinde bilinçli olarak ERTELENMİŞTİ. Faz 6 bu modülü,
tam olarak anlatıldığı şekilde sağlar:

- Bu dosya `crypto_signal_engine` paketinin NORMAL import zincirinin
  DIŞINDADIR — hiçbir `__init__.py` veya başka modül bunu import ETMEZ.
  Yalnızca gerçekten canlı (public) bir WebSocket bağlantısı kurulmak
  istendiğinde (örn. `scripts/live_public_smoke_test.py` veya kullanıcının
  kendi gerçek-zamanlı çalıştırma script'i tarafından) İÇE AKTARILIR —
  böylece `websockets` paketi kurulu OLMASA BİLE `crypto_signal_engine`
  sorunsuz import edilebilir ve TÜM offline testler etkilenmez.
- `websockets` import'u fonksiyon/metod İÇİNDE, LAZY yapılır (modül
  seviyesinde DEĞİL) — bu iki garantiyi de sağlar.
- YALNIZCA public WebSocket URL'lerine bağlanır (`wss://stream.binance.
  com:9443/...` gibi) — hiçbir API key/secret/listenKey/private user
  stream kavramı bu dosyada YOKTUR ve OLAMAZ.
"""

from __future__ import annotations

from typing import Any

from crypto_signal_engine.errors import TransportError


class RealWebSocketConnection:
    """`WebSocketConnection` protokolünün `websockets` kütüphanesi ile
    gerçek implementasyonu. Yalnızca `RealWebSocketConnectionFactory.
    connect()` tarafından oluşturulur."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def recv(self) -> str:
        try:
            message = await self._connection.recv()
        except Exception as exc:
            raise TransportError(f"WebSocket recv başarısız: {exc}") from exc
        return message if isinstance(message, str) else message.decode("utf-8")

    async def send(self, message: str) -> None:
        try:
            await self._connection.send(message)
        except Exception as exc:
            raise TransportError(f"WebSocket send başarısız: {exc}") from exc

    async def close(self) -> None:
        try:
            await self._connection.close()
        except Exception as exc:
            raise TransportError(f"WebSocket close başarısız: {exc}") from exc


class RealWebSocketConnectionFactory:
    """`WebSocketConnectionFactory` protokolünün `websockets` kütüphanesi
    ile gerçek implementasyonu.

    Kullanım (yalnızca gerçek/canlı çalıştırma için — testlerde ASLA):

        from crypto_signal_engine.providers.binance.real_websocket import (
            RealWebSocketConnectionFactory,
        )
        factory = RealWebSocketConnectionFactory()

    `websockets` paketi kurulu değilse, yalnızca `connect()` ÇAĞRILDIĞINDA
    açık bir `ImportError` fırlatır (import anında DEĞİL — bkz. modül
    docstring'i).
    """

    async def connect(self, url: str) -> RealWebSocketConnection:
        if not url.startswith("wss://") and not url.startswith("ws://"):
            raise TransportError(f"Yalnızca ws(s):// URL'lerine bağlanılabilir, alınan: {url}")
        try:
            import websockets
        except ImportError as exc:
            raise ImportError(
                "RealWebSocketConnectionFactory için 'websockets' paketi kurulu değil. "
                "Kurulum: pip install websockets"
            ) from exc

        try:
            connection = await websockets.connect(url)
        except Exception as exc:
            raise TransportError(f"WebSocket bağlantısı kurulamadı ({url}): {exc}") from exc
        return RealWebSocketConnection(connection)


=== FILE: crypto_signal_engine/providers/binance/reconnect.py ===
"""
Reconnect policy.

Kural (Bölüm 8): WebSocket sonsuza kadar kör biçimde reconnect loop'una
girmemeli. Exponential backoff + bounded delay + jitter + reconnect
counter + cancellation-aware sleep + graceful shutdown burada tek bir
yerde toplanır.
"""

from __future__ import annotations

import asyncio

from crypto_signal_engine.providers.binance.clock import JitterSource, RandomJitterSource, Sleeper
from crypto_signal_engine.providers.binance.config import ReconnectPolicyConfig


class MaxReconnectAttemptsExceeded(Exception):
    """`max_attempts` sınırına ulaşıldığında fırlatılır."""


class ReconnectPolicy:
    """Exponential backoff + jitter uygulayan, durum taşıyan reconnect politikası.

    Her `ReconnectPolicy` örneği TEK bir bağlantının reconnect durumunu
    (attempt sayacı) taşır; başarılı bir bağlantı sonrası `reset()`
    çağrılmalıdır ki bir sonraki kopmada backoff sıfırdan başlasın.
    """

    def __init__(
        self,
        config: ReconnectPolicyConfig,
        sleeper: Sleeper,
        jitter_source: JitterSource | None = None,
    ) -> None:
        self._config = config
        self._sleeper = sleeper
        self._jitter_source = jitter_source or RandomJitterSource()
        self._attempt = 0

    @property
    def attempt_count(self) -> int:
        return self._attempt

    def reset(self) -> None:
        """Başarılı bir (re)bağlantı sonrası çağrılır; sayaç sıfırlanır."""
        self._attempt = 0

    def compute_delay_seconds(self) -> float:
        """Bir sonraki denemeden önce beklenecek süreyi hesaplar (jitter dahil).

        `sleep_before_next_attempt()` çağrılmadan da test edilebilmesi için
        ayrı bir saf fonksiyon olarak sunulur.
        """
        base = self._config.initial_delay_seconds * (self._config.multiplier**self._attempt)
        bounded = min(base, self._config.max_delay_seconds)
        jitter = self._jitter_source.jitter(self._config.jitter_seconds)
        return bounded + jitter

    async def sleep_before_next_attempt(self, delay_override_seconds: float | None = None) -> None:
        """Bir sonraki reconnect denemesi öncesi bekler ve sayaçı artırır.

        `delay_override_seconds` verilirse (örn. Binance'in `Retry-After`
        başlığı), hesaplanan backoff yerine bu değer kullanılır — ancak
        sayaç yine de ilerletilir (bir sonraki hesaplanan backoff'un
        büyümeye devam etmesi için).

        `max_attempts` aşılırsa `MaxReconnectAttemptsExceeded` fırlatılır
        (sonsuz kör loop yasak — Bölüm 8). `asyncio.CancelledError`
        kasıtlı olarak yakalanmaz; graceful shutdown sırasında cancel
        edilen bir reconnect loop'u normal şekilde sonlanır, hatalı bir
        "reconnect başarısız" durumu olarak YORUMLANMAZ (Bölüm 18).
        """
        if self._config.max_attempts is not None and self._attempt >= self._config.max_attempts:
            raise MaxReconnectAttemptsExceeded(
                f"max_attempts ({self._config.max_attempts}) aşıldı"
            )
        delay = self.compute_delay_seconds() if delay_override_seconds is None else delay_override_seconds
        self._attempt += 1
        try:
            await self._sleeper.sleep(delay)
        except asyncio.CancelledError:
            # Cancellation, normal shutdown'ın bir parçasıdır; reconnect
            # sayaçını geri almadan (zaten artırıldı) yeniden fırlatılır.
            raise


=== FILE: crypto_signal_engine/providers/binance/rest.py ===
"""
Binance REST client (public/read-only endpoint'ler).

Kural (Bölüm 1.A): yalnızca public endpoint'ler kullanılır. API key
gerekmez, order endpoint'i YOKTUR.

Kural (Bölüm 6): `fetch_historical_candles` gerçek, deterministic pagination
implementasyonuna sahiptir; Binance'in `limit` sınırını aşmaz, ardışık
sayfalar arasında duplicate candle üretmez, kronolojik artan sırayla döner.

Kural (Bölüm 20-21): retry yalnızca retry-safe hatalarda (TransportError,
RateLimitError) yapılır; parser/validation hataları retry loop'una
SOKULMAZ. Rate-limit (429/418) yanıtları sessizce normal hata sayılmaz;
`Retry-After` varsa dikkate alınır.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.errors import BinanceProtocolError, ParseError, RateLimitError, TransportError
from crypto_signal_engine.providers.binance import parser
from crypto_signal_engine.providers.binance.clock import Clock, Sleeper
from crypto_signal_engine.providers.binance.config import BinanceConfig, binance_interval_for
from crypto_signal_engine.providers.binance.reconnect import ReconnectPolicy
from crypto_signal_engine.providers.binance.transport import HttpClient

# Retry-safe olmayan durumlar için (parser/validation hataları) retry
# denenmez; yalnızca bu türler retry-safe kabul edilir (Bölüm 20).
_RETRYABLE_EXCEPTIONS = (TransportError,)

# REST retry için bağımsız, sınırlı bir deneme sayısı (reconnect'ten
# kavramsal olarak ayrı: bu bir tek istek retry'ı, WS reconnect değil).
DEFAULT_MAX_REST_RETRIES = 3


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


class BinanceRestClient:
    """Binance public REST endpoint'lerine erişim.

    Tüm network I/O `HttpClient` soyutlaması üzerinden yapılır (Bölüm 22 —
    testler gerçek ağa bağımlı olmamalı).
    """

    def __init__(
        self,
        config: BinanceConfig,
        http_client: HttpClient,
        clock: Clock,
        sleeper: Sleeper,
        max_retries: int = DEFAULT_MAX_REST_RETRIES,
    ) -> None:
        self._config = config
        self._http_client = http_client
        self._clock = clock
        self._sleeper = sleeper
        self._max_retries = max_retries

    async def _get_with_retry(self, path: str, params: dict[str, str]) -> object:
        """GET isteğini retry-safe hatalarda bounded backoff ile tekrar dener."""
        url = f"{self._config.rest_base_url}{path}"
        retry_policy = ReconnectPolicy(self._config.reconnect, self._sleeper)
        last_error: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                response = await self._http_client.get(url, params, self._config.request_timeout_seconds)
            except RateLimitError as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    raise
                await retry_policy.sleep_before_next_attempt(delay_override_seconds=exc.retry_after_seconds)
                continue
            except _RETRYABLE_EXCEPTIONS as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    raise
                await retry_policy.sleep_before_next_attempt()
                continue

            if response.status_code == 200:
                return response.json()
            if response.status_code in (429, 418):
                # HttpClient implementasyonu bunu RateLimitError olarak fırlatmalıydı;
                # yine de burada ikinci bir güvenlik ağı olarak ele alınır.
                raise RateLimitError(
                    f"Binance rate limit yanıtı: HTTP {response.status_code}",
                    status_code=response.status_code,
                )
            raise BinanceProtocolError(
                f"Beklenmeyen Binance REST yanıt kodu: {response.status_code}, body: {response.body[:200]!r}"
            )

        # Buraya ulaşılmamalı (döngü ya return eder ya raise eder) ama tip
        # denetleyicisi ve "sessizce None dönme" ihtimaline karşı açık hata:
        raise TransportError(f"REST isteği {self._max_retries} denemeden sonra başarısız: {last_error}")

    async def fetch_historical_candles(
        self, symbol: str, timeframe: Timeframe, start: datetime, end: datetime
    ) -> list[Candle]:
        """`/api/v3/klines` üzerinden geçmiş candle verisini deterministic
        pagination ile çeker.

        Gereksinimler (Bölüm 6): UTC-aware start/end, start < end,
        desteklenen Timeframe, Binance limit'i aşmayan sayfa boyutu,
        duplicate üretmeyen ilerleme, ascending kronolojik sonuç.
        """
        require_utc_aware(start, "start")
        require_utc_aware(end, "end")
        if start >= end:
            raise ValueError(f"start ({start}), end'den ({end}) küçük olmalı")
        symbol = normalize_symbol(symbol)
        if self._config.supported_timeframes and timeframe not in self._config.supported_timeframes:
            raise ValueError(f"timeframe {timeframe} config.supported_timeframes içinde değil")

        binance_interval = binance_interval_for(timeframe)
        limit = self._config.max_klines_per_request

        results: list[Candle] = []
        cursor_start_ms = _to_epoch_ms(start)
        end_ms = _to_epoch_ms(end)

        while cursor_start_ms < end_ms:
            params = {
                "symbol": symbol,
                "interval": binance_interval,
                "startTime": str(cursor_start_ms),
                "endTime": str(end_ms),
                "limit": str(limit),
            }
            rows = await self._get_with_retry("/api/v3/klines", params)
            if not isinstance(rows, list):
                raise BinanceProtocolError(f"/api/v3/klines listesi bekleniyordu, alınan: {type(rows).__name__}")
            if not rows:
                break

            now = self._clock.now()
            for row in rows:
                candle = parser.parse_kline_rest_row(row, symbol=symbol, timeframe=timeframe, now=now)
                if results and candle.open_time <= results[-1].open_time:
                    # Pagination cursor'ı ilerlerken duplicate/geri-giden bir
                    # candle geldiyse bu bir protokol/parser hatasıdır —
                    # sessizce kabul edilip yayılmaz.
                    raise ParseError(
                        f"REST pagination sırasında ascending olmayan candle: "
                        f"{candle.open_time} <= {results[-1].open_time}"
                    )
                results.append(candle)

            if len(rows) < limit:
                break  # son sayfa
            # Bir sonraki sayfa, son dönen candle'ın open_time'ından hemen
            # sonra başlar (aynı candle'ın tekrar istenmesini önler).
            last_open_time_ms = int(rows[-1][0])
            cursor_start_ms = last_open_time_ms + 1

        return results

    async def fetch_exchange_info(self) -> list[parser.ExchangeSymbolInfo]:
        """`/api/v3/exchangeInfo` — public, credential gerektirmez.

        Otomatik sembol seçimi (`crypto_signal_engine.selection`) için
        aday evren keşfinin ilk adımı: hangi semboller var, hangi quote
        asset'e bağlı, TRADING durumunda mı, spot trading'e izinli mi."""
        payload = await self._get_with_retry("/api/v3/exchangeInfo", {})
        return parser.parse_exchange_info_response(payload)

    async def fetch_24hr_tickers(self) -> list[parser.Ticker24hr]:
        """`/api/v3/ticker/24hr` — `symbol` parametresi VERİLMEDEN, TÜM
        semboller için TEK bir çağrıda 24 saatlik istatistik döner (Binance
        resmi davranışı). Otomatik sembol seçiminin ucuz likidite
        filtresi/kısa liste aşaması bunu kullanır — sembol başına ayrı bir
        REST çağrısı YAPILMAZ (rate-limit kötüye kullanımından kaçınmak
        için, bkz. `selection/selector.py`)."""
        payload = await self._get_with_retry("/api/v3/ticker/24hr", {})
        return parser.parse_24hr_ticker_response(payload)

    async def fetch_depth_snapshot(
        self, symbol: str
    ) -> tuple[int, tuple[tuple[float, float], ...], tuple[tuple[float, float], ...], datetime]:
        """`/api/v3/depth` üzerinden order book snapshot'ı çeker.

        Binance yanıtı bir timestamp İÇERMEZ (yalnızca `lastUpdateId`); bu
        yüzden dönüş değeri, fetch anındaki UTC zamanını (`received_at`)
        dördüncü eleman olarak taşır — `OrderBookSnapshot.timestamp` bu
        değerle doldurulmalıdır (bkz. parser.py docstring, aynı politika).
        """
        symbol = normalize_symbol(symbol)
        params = {"symbol": symbol, "limit": str(self._config.order_book_depth)}
        payload = await self._get_with_retry("/api/v3/depth", params)
        if not isinstance(payload, dict):
            raise BinanceProtocolError(f"/api/v3/depth dict bekleniyordu, alınan: {type(payload).__name__}")
        last_update_id, bids, asks = parser.parse_depth_snapshot_response(payload)
        received_at = self._clock.now()
        return last_update_id, bids, asks, received_at


=== FILE: crypto_signal_engine/providers/binance/symbols.py ===
"""
Symbol adapter boundary.

Kural (Bölüm 5): domain symbol canonical formatta (örn. "BTCUSDT") tutulur.
Binance stream/URL formatı (küçük harf, örn. "btcusdt@kline_1m") YALNIZCA
bu sınırda üretilir/tüketilir; domain katmanına asla sızmaz.
"""

from __future__ import annotations

from crypto_signal_engine.domain._validation import normalize_symbol


def to_binance_wire_symbol(domain_symbol: str) -> str:
    """Domain symbol'ünü ("BTCUSDT") Binance wire-format'a ("btcusdt") çevirir.

    Binance REST parametreleri büyük/küçük harfe duyarlı değildir ama
    WebSocket stream isimleri KÜÇÜK HARF zorunludur; bu yüzden tek,
    tutarlı bir wire-format (lower-case) burada üretilir.
    """
    canonical = normalize_symbol(domain_symbol)
    return canonical.lower()


def from_binance_wire_symbol(wire_symbol: str) -> str:
    """Binance'ten gelen (herhangi bir case'te olabilen) symbol string'ini
    domain canonical formatına çevirir. Bu, `normalize_symbol` ile aynı
    politikayı kullanır ama adapter boundary'de niyeti netleştirmek için
    ayrı bir isimle sunulur."""
    return normalize_symbol(wire_symbol)


def kline_stream_name(domain_symbol: str, binance_interval: str) -> str:
    """Binance kline WebSocket stream adını üretir: `btcusdt@kline_1m`."""
    return f"{to_binance_wire_symbol(domain_symbol)}@kline_{binance_interval}"


def agg_trade_stream_name(domain_symbol: str) -> str:
    """Binance aggregate trade WebSocket stream adını üretir: `btcusdt@aggTrade`."""
    return f"{to_binance_wire_symbol(domain_symbol)}@aggTrade"


def depth_diff_stream_name(domain_symbol: str) -> str:
    """Binance diff. depth WebSocket stream adını üretir: `btcusdt@depth`.

    100ms güncelleme hızı (varsayılan) kullanılır; `@depth@100ms` da
    kullanılabilir ama Binance'in varsayılanı zaten 1000ms'lik `@depth`
    değil, 100ms olan `@depth@100ms`'dir — burada açıkça `@depth@100ms`
    kullanılır ki senkronizasyon algoritması güncel veriyle çalışsın.
    """
    return f"{to_binance_wire_symbol(domain_symbol)}@depth@100ms"


=== FILE: crypto_signal_engine/providers/binance/transport.py ===
"""
Transport boundary: HTTP ve WebSocket için injectable soyutlamalar.

Kural (Bölüm 22): "Network boundary mock/fake edilebilir olmalı." Bu
modül, REST/WebSocket provider'ların gerçek network kütüphanelerine
(urllib, websockets, aiohttp, vb.) DOĞRUDAN bağımlı olmasını önler;
bunun yerine ince Protocol'ler tanımlar ve testler bunları sahte
implementasyonlarla değiştirebilir.

`HttpClient` için stdlib-only (`urllib`) gerçek bir implementasyon burada
sağlanır (ekstra bağımlılık gerektirmez). `WebSocketConnectionFactory`
için ise yalnızca Protocol tanımlanır; gerçek bir WebSocket implementasyonu
(örn. `websockets` kütüphanesi ile) ayrı, opsiyonel bir modülde
(`real_websocket.py`) sağlanır ve yalnızca gerçekten kullanılmak
istendiğinde import edilir — böylece paket, bu opsiyonel bağımlılık
kurulu olmasa bile sorunsuz import edilebilir ve TÜM testler offline/
mock transport ile çalışır.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from crypto_signal_engine.errors import RateLimitError, TransportError


@dataclass(frozen=True)
class HttpResponse:
    """Bir HTTP yanıtının transport-agnostik temsili."""

    status_code: int
    body: str
    headers: dict[str, str]

    def json(self) -> object:
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise TransportError(f"Yanıt gövdesi geçerli JSON değil: {exc}") from exc


@runtime_checkable
class HttpClient(Protocol):
    """REST çağrıları için injectable soyutlama."""

    async def get(self, url: str, params: dict[str, str], timeout_seconds: float) -> HttpResponse: ...


class UrllibHttpClient:
    """Yalnızca stdlib kullanan gerçek (üretim) HTTP client implementasyonu.

    `urllib.request` senkron olduğundan `asyncio.to_thread` ile
    event loop'u bloklamadan çalıştırılır (Bölüm 13 — async sınırlar).
    Bu implementasyon TESTLERDE KULLANILMAZ (testler `FakeHttpClient`
    enjekte eder); yalnızca gerçek bir Binance REST çağrısı yapılmak
    istendiğinde devreye girer.
    """

    async def get(self, url: str, params: dict[str, str], timeout_seconds: float) -> HttpResponse:
        return await asyncio.to_thread(self._get_sync, url, params, timeout_seconds)

    @staticmethod
    def _get_sync(url: str, params: dict[str, str], timeout_seconds: float) -> HttpResponse:
        query = urllib.parse.urlencode(params)
        full_url = f"{url}?{query}" if query else url
        request = urllib.request.Request(full_url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                headers = dict(response.headers.items())
                return HttpResponse(status_code=response.status, body=body, headers=headers)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            headers = dict(exc.headers.items()) if exc.headers else {}
            if exc.code in (429, 418):
                retry_after = headers.get("Retry-After")
                raise RateLimitError(
                    f"Binance rate limit/ban yanıtı: HTTP {exc.code}",
                    status_code=exc.code,
                    retry_after_seconds=float(retry_after) if retry_after else None,
                ) from exc
            return HttpResponse(status_code=exc.code, body=body, headers=headers)
        except urllib.error.URLError as exc:
            raise TransportError(f"REST isteği başarısız: {exc}") from exc
        except TimeoutError as exc:
            raise TransportError(f"REST isteği zaman aşımına uğradı: {exc}") from exc


@runtime_checkable
class WebSocketConnection(Protocol):
    """Tek bir açık WebSocket bağlantısının soyutlaması."""

    async def recv(self) -> str: ...

    async def send(self, message: str) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class WebSocketConnectionFactory(Protocol):
    """Bir URL'e bağlanıp `WebSocketConnection` üreten fabrika soyutlaması."""

    async def connect(self, url: str) -> WebSocketConnection: ...


