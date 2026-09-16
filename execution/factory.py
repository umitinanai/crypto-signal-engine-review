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
