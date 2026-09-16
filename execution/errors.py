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
