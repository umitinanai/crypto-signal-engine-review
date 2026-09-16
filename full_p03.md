<!-- full_p03.md — Part 3/7 — 50 files -->
<!-- Contents of this part: -->
<!--   - crypto_signal_engine/execution/testnet_client.py (23721 bytes) -->
<!--   - crypto_signal_engine/features/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/features/candle_calculators.py (18050 bytes) -->
<!--   - crypto_signal_engine/features/domain.py (6970 bytes) -->
<!--   - crypto_signal_engine/features/engine.py (11787 bytes) -->
<!--   - crypto_signal_engine/features/orderbook_calculators.py (4669 bytes) -->
<!--   - crypto_signal_engine/features/registry.py (7774 bytes) -->
<!--   - crypto_signal_engine/features/state.py (4401 bytes) -->
<!--   - crypto_signal_engine/features/trade_calculators.py (4433 bytes) -->
<!--   - crypto_signal_engine/ops/__init__.py (775 bytes) -->
<!--   - crypto_signal_engine/ops/admin.py (7373 bytes) -->
<!--   - crypto_signal_engine/ops/config.py (22994 bytes) -->
<!--   - crypto_signal_engine/ops/dashboard.py (109565 bytes) -->
<!--   - crypto_signal_engine/ops/errors.py (869 bytes) -->
<!--   - crypto_signal_engine/ops/event_log.py (4125 bytes) -->
<!--   - crypto_signal_engine/ops/health_snapshot.py (6590 bytes) -->
<!--   - crypto_signal_engine/ops/lock.py (3214 bytes) -->
<!--   - crypto_signal_engine/ops/logging_setup.py (1783 bytes) -->
<!--   - crypto_signal_engine/ops/notifier.py (6322 bytes) -->
<!--   - crypto_signal_engine/ops/systemd_notify.py (3210 bytes) -->
<!--   - crypto_signal_engine/paper_trading/__init__.py (542 bytes) -->
<!--   - crypto_signal_engine/paper_trading/engine.py (26988 bytes) -->
<!--   - crypto_signal_engine/paper_trading/models.py (7515 bytes) -->
<!--   - crypto_signal_engine/persistence/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/persistence/errors.py (1569 bytes) -->
<!--   - crypto_signal_engine/persistence/paper_state_store.py (19592 bytes) -->
<!--   - crypto_signal_engine/persistence/recovery.py (28204 bytes) -->
<!--   - crypto_signal_engine/persistence/serialization.py (6022 bytes) -->
<!--   - crypto_signal_engine/persistence/sqlite_store.py (10683 bytes) -->
<!--   - crypto_signal_engine/portfolio/__init__.py (615 bytes) -->
<!--   - crypto_signal_engine/portfolio/accounting.py (10216 bytes) -->
<!--   - crypto_signal_engine/providers/__init__.py (1 bytes) -->
<!--   - crypto_signal_engine/providers/base.py (2826 bytes) -->
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
<!--   - crypto_signal_engine/quality/__init__.py (1 bytes) -->
<!--   - crypto_signal_engine/quality/base.py (2643 bytes) -->
<!--   - crypto_signal_engine/quality/binance_rules.py (9882 bytes) -->
<!--   - crypto_signal_engine/runtime/__init__.py (642 bytes) -->
<!--   - crypto_signal_engine/runtime/bootstrap.py (2709 bytes) -->
<!--   - crypto_signal_engine/runtime/candle_window.py (4246 bytes) -->

=== FILE: crypto_signal_engine/execution/testnet_client.py ===
"""
Faz 10 — Binance Spot TESTNET-only HTTP istemcisi.

BLOCKER-seviyesi güvenlik: `validate_testnet_host()`, bir `BinanceTestnetConfig`
inşa edilirken HER ZAMAN çağrılır ve yapılandırılmış host'un
`_ALLOWED_HOSTS` (`{"testnet.binance.vision"}`) İÇİNDE, `https` scheme'iyle
OLMASINI zorunlu kılar — substring/prefix/suffix kontrolü DEĞİL, tam
`urlsplit().hostname` eşleşmesi (bkz. fonksiyon docstring'i, lookalike/
subdomain trick'lerine karşı)."""

from __future__ import annotations

import asyncio
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.execution.errors import (
    BinanceRejectionError,
    ExecutionConfigError,
    ExecutionTimeoutError,
    ExecutionTransportError,
    MalformedResponseError,
    MarketPriceUnavailableError,
    MissingCredentialsError,
    OrderNotFoundError,
    TimestampRejectedError,
    UnsafeExecutionHostError,
)
from crypto_signal_engine.execution.models import ExecutionResult, Fill, OrderIntent, OrderSide
from crypto_signal_engine.execution.signer import BinanceTestnetSigner, canonical_query_string
from crypto_signal_engine.providers.binance.clock import Clock, SystemClock

_ALLOWED_HOSTS: frozenset[str] = frozenset({"testnet.binance.vision"})
_ALLOWED_SCHEME = "https"
_BINANCE_TIMESTAMP_REJECTED_CODE = -1021
_BINANCE_ORDER_NOT_FOUND_CODE = -2013


def validate_testnet_host(base_url: str) -> None:
    """`base_url`'in KESİNLİKLE onaylı Binance Spot TESTNET host'u olduğunu
    doğrular. Tam `urlsplit().hostname` eşleşmesi kullanılır (`.hostname`
    her zaman küçük harfe çevrilir) — bu yüzden:
    - `http://testnet.binance.vision` -> RED (scheme)
    - `https://api.binance.com` -> RED (host)
    - `https://testnet.binance.vision.attacker.com` -> RED (hostname TAM
      olarak `testnet.binance.vision.attacker.com`'dur, PREFIX eşleşmesi
      YOKTUR)
    - `https://attacker.com/testnet.binance.vision` -> RED (path, host DEĞİL)
    - `https://user@testnet.binance.vision@attacker.com` -> RED (hostname
      `attacker.com`'dur)
    hepsi doğru şekilde REDDEDİLİR."""
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme != _ALLOWED_SCHEME:
        raise UnsafeExecutionHostError(
            f"İzin verilmeyen scheme: {parsed.scheme!r} (yalnızca {_ALLOWED_SCHEME!r} kabul edilir)"
        )
    if parsed.username or parsed.password:
        raise UnsafeExecutionHostError("base_url userinfo (kullanıcı adı/parola) İÇEREMEZ")
    if parsed.hostname not in _ALLOWED_HOSTS:
        raise UnsafeExecutionHostError(
            f"İzin verilmeyen TESTNET host'u: {parsed.hostname!r} "
            f"(yalnızca {sorted(_ALLOWED_HOSTS)} kabul edilir — Mainnet/lookalike host YASAK)"
        )


@dataclass(frozen=True)
class BinanceTestnetConfig:
    """Faz 10 TESTNET client konfigürasyonu.

    `api_key`/`api_secret` KASITLI OLARAK opsiyoneldir (`None`) —
    `exchangeInfo`/`ping`/`server_time` gibi PUBLIC-eşdeğeri endpoint'ler
    kimlik bilgisi GEREKTİRMEZ (gerçek Binance API'siyle AYNI); yalnızca
    `account_info()`/`place_order()` gibi SIGNED çağrılar sırasında,
    eksikse `MissingCredentialsError` fırlatılır (bkz. `BinanceTestnetClient`).
    Bu, "credential yokluğu Faz 10 blocker'ı DEĞİLDİR, doğrulama/dry-run
    çalışmaya devam eder" gereksinimini yapısal olarak karşılar."""

    base_url: str = "https://testnet.binance.vision"
    api_key: str | None = None
    api_secret: str | None = None
    recv_window_ms: int = 5000
    request_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        validate_testnet_host(self.base_url)
        if self.api_key is not None and not self.api_key.strip():
            raise MissingCredentialsError("api_key boş bir string olamaz (None veya dolu olmalı)")
        if self.api_secret is not None and not self.api_secret.strip():
            raise MissingCredentialsError("api_secret boş bir string olamaz (None veya dolu olmalı)")
        if not (1 <= self.recv_window_ms <= 60_000):
            raise ExecutionConfigError(f"recv_window_ms [1, 60000] aralığında olmalı, alınan: {self.recv_window_ms}")
        if self.request_timeout_seconds <= 0:
            raise ExecutionConfigError("request_timeout_seconds pozitif olmalı")

    def has_credentials(self) -> bool:
        return bool(self.api_key) and bool(self.api_secret)

    def __repr__(self) -> str:
        # HARD SAFETY: varsayılan dataclass repr'ı api_key/api_secret'i
        # AÇIKÇA yazdırırdı — bu override, secret'ların HİÇBİR ZAMAN log/
        # exception/repr çıktısına SIZMAMASINI garanti eder.
        key_shown = "SET" if self.api_key else "UNSET"
        secret_shown = "SET" if self.api_secret else "UNSET"
        return (
            f"BinanceTestnetConfig(base_url={self.base_url!r}, api_key={key_shown}, "
            f"api_secret={secret_shown}, recv_window_ms={self.recv_window_ms}, "
            f"request_timeout_seconds={self.request_timeout_seconds})"
        )


@dataclass(frozen=True)
class TestnetHttpResponse:
    status_code: int
    body: str

    def json(self) -> object:
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise MalformedResponseError(
                f"TESTNET yanıtı geçerli JSON değil (status={self.status_code})"
            ) from exc


@runtime_checkable
class TestnetHttpClient(Protocol):
    async def get(
        self, url: str, params: dict[str, str], headers: dict[str, str], timeout_seconds: float
    ) -> TestnetHttpResponse: ...

    async def post(
        self, url: str, params: dict[str, str], headers: dict[str, str], timeout_seconds: float
    ) -> TestnetHttpResponse: ...


class UrllibTestnetHttpClient:
    """stdlib-only gerçek (TESTNET) HTTP istemcisi. Testlerde KULLANILMAZ
    (testler sahte bir `TestnetHttpClient` enjekte eder)."""

    async def get(
        self, url: str, params: dict[str, str], headers: dict[str, str], timeout_seconds: float
    ) -> TestnetHttpResponse:
        return await asyncio.to_thread(self._request_sync, "GET", url, params, headers, timeout_seconds)

    async def post(
        self, url: str, params: dict[str, str], headers: dict[str, str], timeout_seconds: float
    ) -> TestnetHttpResponse:
        return await asyncio.to_thread(self._request_sync, "POST", url, params, headers, timeout_seconds)

    @staticmethod
    def _request_sync(
        method: str, url: str, params: dict[str, str], headers: dict[str, str], timeout_seconds: float
    ) -> TestnetHttpResponse:
        query = urllib.parse.urlencode(params)
        if method == "GET":
            full_url = f"{url}?{query}" if query else url
            request = urllib.request.Request(full_url, method="GET", headers=dict(headers))
        else:
            request = urllib.request.Request(
                url, data=query.encode("utf-8"), method="POST", headers=dict(headers)
            )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return TestnetHttpResponse(status_code=response.status, body=body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            return TestnetHttpResponse(status_code=exc.code, body=body)
        except TimeoutError as exc:
            raise ExecutionTimeoutError(f"TESTNET isteği {timeout_seconds}s içinde zaman aşımına uğradı") from exc
        except urllib.error.URLError as exc:
            raise ExecutionTransportError(f"TESTNET isteği başarısız: {exc.reason}") from exc


def _format_decimal(value: float) -> str:
    """Binance, bilimsel gösterimi (`1e-05`) REDDEDER — sabit noktalı,
    gereksiz sıfırları kırpılmış bir string üretir."""
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text if text else "0"


def _order_params(intent: OrderIntent) -> "OrderedDict[str, str]":
    params: OrderedDict[str, str] = OrderedDict()
    params["symbol"] = intent.symbol
    params["side"] = intent.side.value
    params["type"] = intent.order_type.value
    if intent.quantity is not None:
        params["quantity"] = _format_decimal(intent.quantity)
    if intent.quote_quantity is not None:
        params["quoteOrderQty"] = _format_decimal(intent.quote_quantity)
    if intent.price is not None:
        params["price"] = _format_decimal(intent.price)
    if intent.time_in_force is not None:
        params["timeInForce"] = intent.time_in_force.value
    params["newClientOrderId"] = intent.client_order_id
    return params


def _sign(
    params: "OrderedDict[str, str]", *, api_secret: str, recv_window_ms: int, timestamp_ms: int
) -> "OrderedDict[str, str]":
    signed: OrderedDict[str, str] = OrderedDict(params)
    signed["recvWindow"] = str(recv_window_ms)
    signed["timestamp"] = str(timestamp_ms)
    query = canonical_query_string(signed)
    signed["signature"] = BinanceTestnetSigner(api_secret).sign(query)
    return signed


def _parse_fills(payload: dict) -> tuple[Fill, ...]:
    """Parses Binance's optional `fills[]` array (present on a FULL
    response — the default `newOrderRespType` for MARKET/LIMIT orders — but
    absent from a `GET /api/v3/order` query response). A missing/empty
    array is NOT an error here — it just means fee data must be backfilled
    separately via `my_trades()` (see autonomous-Testnet-lifecycle Phase
    0-C: fee capture never existed before this, and a GET query genuinely
    cannot carry it)."""
    raw_fills = payload.get("fills")
    if not raw_fills:
        return ()
    try:
        return tuple(
            Fill(
                price=float(f["price"]),
                quantity=float(f["qty"]),
                commission=float(f["commission"]),
                commission_asset=f["commissionAsset"],
                trade_id=int(f["tradeId"]) if "tradeId" in f else None,
            )
            for f in raw_fills
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MalformedResponseError(f"TESTNET fills[] beklenen alanları içermiyor: {exc}") from exc


def _raise_for_error(status_code: int, payload: object) -> None:
    code = payload.get("code") if isinstance(payload, dict) else None
    msg = payload.get("msg") if isinstance(payload, dict) else None
    message = f"Binance TESTNET isteği reddetti (HTTP {status_code}, code={code}): {msg or 'bilinmeyen hata'}"
    if code == _BINANCE_TIMESTAMP_REJECTED_CODE:
        raise TimestampRejectedError(message, binance_code=code)
    raise BinanceRejectionError(message, binance_code=code)


class BinanceTestnetClient:
    """Faz 10 TESTNET-only client. `config.base_url`'in allowlist'te
    olduğu, İNŞA edildiği ANDA (`BinanceTestnetConfig.__post_init__`
    üzerinden) garanti edilir — bu sınıf o garantiyi TEKRAR DOĞRULAMAZ,
    ama ASLA farklı bir host'a istek ATMAZ (yalnızca `config.base_url`
    kullanılır)."""

    def __init__(
        self, config: BinanceTestnetConfig, http_client: TestnetHttpClient, clock: Clock | None = None
    ) -> None:
        self._config = config
        self._http = http_client
        self._clock = clock or SystemClock()

    def _require_credentials(self) -> tuple[str, str]:
        if not self._config.has_credentials():
            raise MissingCredentialsError(
                "TESTNET API key/secret eksik — signed bir çağrı (account/order) için ZORUNLU "
                "(BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET)"
            )
        return self._config.api_key, self._config.api_secret  # type: ignore[return-value]

    async def ping(self) -> None:
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/ping", {}, {}, self._config.request_timeout_seconds
        )
        if response.status_code >= 400:
            _raise_for_error(response.status_code, response.json())

    async def server_time(self) -> datetime:
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/time", {}, {}, self._config.request_timeout_seconds
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        try:
            return datetime.fromtimestamp(payload["serverTime"] / 1000, tz=timezone.utc)
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedResponseError("TESTNET /api/v3/time yanıtı beklenen alanı içermiyor") from exc

    async def exchange_info(self, symbol: str) -> dict:
        """PUBLIC-eşdeğeri, kimlik bilgisi GEREKTİRMEZ (gerçek Binance
        API'siyle AYNI — `/api/v3/exchangeInfo` unsigned'dır)."""
        normalized = normalize_symbol(symbol)
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/exchangeInfo", {"symbol": normalized}, {},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, dict):
            raise MalformedResponseError("TESTNET /api/v3/exchangeInfo yanıtı bir JSON obje değil")
        return payload

    async def symbol_price(self, symbol: str) -> float:
        """PUBLIC-eşdeğeri (kimlik bilgisi GEREKTİRMEZ), GÜNCEL bir TESTNET
        fiyatı döner — MARKET order'ların notional filtre doğrulaması için
        (bkz. `adapter.py::determine_market_price_requirement`).

        ÖNEMLİ (dokümantasyon netliği): bu, Binance'in KENDİ execution
        price'ını/`avgPriceMins` penceresini TAHMİN ETMEZ — yalnızca ANLIK
        `/api/v3/ticker/price` değeridir; pre-submission bir filtre-
        doğrulama TABANIDIR, bir fill-price GARANTİSİ DEĞİLDİR (bkz.
        PHASE10 doc — Karar 73).

        FAIL-CLOSED: transport hatası/timeout/malformed JSON için ilgili
        spesifik hata (`ExecutionTransportError`/`ExecutionTimeoutError`/
        `MalformedResponseError`) fırlatılır; yanlış sembol döndüyse veya
        fiyat sonlu/pozitif DEĞİLSE `MarketPriceUnavailableError`
        fırlatılır — HİÇBİR durumda sessizce bir "varsayılan" fiyat
        ÜRETİLMEZ."""
        normalized = normalize_symbol(symbol)
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/ticker/price", {"symbol": normalized}, {},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, dict):
            raise MalformedResponseError("TESTNET /api/v3/ticker/price yanıtı bir JSON obje değil")
        try:
            returned_symbol = normalize_symbol(payload["symbol"])
            price = float(payload["price"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedResponseError(
                f"TESTNET /api/v3/ticker/price yanıtı beklenen alanları içermiyor: {exc}"
            ) from exc
        if returned_symbol != normalized:
            raise MarketPriceUnavailableError(
                f"{normalized} için fiyat istendi ama {returned_symbol} için yanıt alındı"
            )
        if not math.isfinite(price) or price <= 0:
            raise MarketPriceUnavailableError(f"{normalized} için geçersiz fiyat alındı: {price}")
        return price

    async def account_info(self) -> dict:
        api_key, api_secret = self._require_credentials()
        timestamp_ms = int(self._clock.now().timestamp() * 1000)
        signed = _sign(OrderedDict(), api_secret=api_secret, recv_window_ms=self._config.recv_window_ms, timestamp_ms=timestamp_ms)
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/account", dict(signed), {"X-MBX-APIKEY": api_key},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, dict):
            raise MalformedResponseError("TESTNET /api/v3/account yanıtı bir JSON obje değil")
        return payload

    async def place_order(self, intent: OrderIntent) -> ExecutionResult:
        api_key, api_secret = self._require_credentials()
        timestamp_ms = int(self._clock.now().timestamp() * 1000)
        signed = _sign(
            _order_params(intent), api_secret=api_secret,
            recv_window_ms=self._config.recv_window_ms, timestamp_ms=timestamp_ms,
        )
        response = await self._http.post(
            f"{self._config.base_url}/api/v3/order", dict(signed), {"X-MBX-APIKEY": api_key},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, dict):
            raise MalformedResponseError("TESTNET /api/v3/order yanıtı bir JSON obje değil")
        try:
            return ExecutionResult(
                symbol=payload["symbol"],
                client_order_id=payload["clientOrderId"],
                exchange_order_id=int(payload["orderId"]),
                side=OrderSide(payload["side"]),
                status=payload["status"],
                executed_quantity=float(payload["executedQty"]),
                cumulative_quote_quantity=float(payload["cummulativeQuoteQty"]),
                transaction_time=datetime.fromtimestamp(payload["transactTime"] / 1000, tz=timezone.utc),
                context_id=intent.context_id,
                fills=_parse_fills(payload),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedResponseError(f"TESTNET /api/v3/order yanıtı beklenen alanları içermiyor: {exc}") from exc

    async def my_trades(self, symbol: str, *, order_id: int) -> tuple[Fill, ...]:
        """Faz 0-C backfill path — `GET /api/v3/myTrades` (SIGNED), scoped
        to one exchange `orderId`, returns the authoritative per-fill
        commission history for an order whose FILLED status was learned via
        `query_order()` (which never carries `fills[]`) rather than the
        original `place_order()` response — used by reconciliation-recovered
        fills and by legacy-position migration (Phase 4/17), never by the
        normal entry/exit path (which already has `fills[]` from the FULL
        response, see `place_order`)."""
        api_key, api_secret = self._require_credentials()
        normalized = normalize_symbol(symbol)
        params: OrderedDict[str, str] = OrderedDict()
        params["symbol"] = normalized
        params["orderId"] = str(order_id)
        timestamp_ms = int(self._clock.now().timestamp() * 1000)
        signed = _sign(
            params, api_secret=api_secret, recv_window_ms=self._config.recv_window_ms, timestamp_ms=timestamp_ms
        )
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/myTrades", dict(signed), {"X-MBX-APIKEY": api_key},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, list):
            raise MalformedResponseError("TESTNET /api/v3/myTrades yanıtı bir JSON dizi değil")
        try:
            return tuple(
                Fill(
                    price=float(t["price"]), quantity=float(t["qty"]), commission=float(t["commission"]),
                    commission_asset=t["commissionAsset"], trade_id=int(t["id"]),
                )
                for t in payload
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedResponseError(f"TESTNET /api/v3/myTrades yanıtı beklenen alanları içermiyor: {exc}") from exc

    async def query_order(self, symbol: str, *, client_order_id: str, context_id: str) -> ExecutionResult:
        """Faz 11 — sinyalli TESTNET order sorgusu (`GET /api/v3/order`,
        `origClientOrderId` ile) — stabil kimlik üzerinden exchange
        TRUTH'unu sorgulayarak belirsiz (timeout/bağlantı kopması) bir
        submission'ı ÇÖZMEK için kullanılır (bkz.
        `reconciliation_service.py`). Order TESTNET'te YOKSA (Binance kodu
        -2013), bu GERÇEK bir hata DEĞİLDİR — `OrderNotFoundError`
        fırlatılır.

        BLOCKER FİX (Karar 78): bu -2013 yanıtı "BU denemede order
        bulunamadı" DEMEKTİR — "orijinal submission'ın Binance tarafından
        HİÇ kabul edilmediği KANITLANDI, yeniden gönderim güvenlidir"
        ANLAMINA GELMEZ (bkz. `reconciliation_service.py::submit()`
        docstring'i — bu belirsizlik ASLA otomatik bir ikinci POST'a
        DÖNÜŞMEZ, yalnızca kaydı reconciliation-eligible bir "çözülmemiş"
        duruma taşır).

        `context_id`, Binance'in yanıtında YOKTUR — çağıran (durable yerel
        record'un SAHİBİ) tarafından sağlanır, yalnızca dönen
        `ExecutionResult`'ı tamamlamak için kullanılır."""
        api_key, api_secret = self._require_credentials()
        normalized = normalize_symbol(symbol)
        params: OrderedDict[str, str] = OrderedDict()
        params["symbol"] = normalized
        params["origClientOrderId"] = client_order_id
        timestamp_ms = int(self._clock.now().timestamp() * 1000)
        signed = _sign(
            params, api_secret=api_secret, recv_window_ms=self._config.recv_window_ms, timestamp_ms=timestamp_ms
        )
        response = await self._http.get(
            f"{self._config.base_url}/api/v3/order", dict(signed), {"X-MBX-APIKEY": api_key},
            self._config.request_timeout_seconds,
        )
        payload = response.json()
        if response.status_code >= 400:
            code = payload.get("code") if isinstance(payload, dict) else None
            if code == _BINANCE_ORDER_NOT_FOUND_CODE:
                raise OrderNotFoundError(
                    f"{normalized} client_order_id={client_order_id} TESTNET'te bulunamadı (Binance code {code})"
                )
            _raise_for_error(response.status_code, payload)
        if not isinstance(payload, dict):
            raise MalformedResponseError("TESTNET /api/v3/order (query) yanıtı bir JSON obje değil")
        try:
            return ExecutionResult(
                symbol=payload["symbol"],
                client_order_id=payload["clientOrderId"],
                exchange_order_id=int(payload["orderId"]),
                side=OrderSide(payload["side"]),
                status=payload["status"],
                executed_quantity=float(payload["executedQty"]),
                cumulative_quote_quantity=float(payload["cummulativeQuoteQty"]),
                transaction_time=datetime.fromtimestamp(payload["updateTime"] / 1000, tz=timezone.utc),
                context_id=context_id,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedResponseError(
                f"TESTNET /api/v3/order (query) yanıtı beklenen alanları içermiyor: {exc}"
            ) from exc


=== FILE: crypto_signal_engine/features/__init__.py ===


=== FILE: crypto_signal_engine/features/candle_calculators.py ===
"""
Candle-tabanlı feature hesaplayıcıları.

KURAL (Faz 3 Bölüm 9 — closed-candle policy): Bu modüldeki TÜM
fonksiyonlar, çağıranın YALNIZCA `is_closed=True` candle'lar içeren bir
liste geçirdiğini VARSAYAR. Kapalı-candle zorunluluğunun uygulanması
`engine.py::FeatureEngine.compute_candle_features()` seviyesinde yapılır
(bu modül saf hesaplama fonksiyonlarından oluşur, I/O veya politika
uygulaması içermez).

KURAL (Bölüm 10 — warm-up): her fonksiyon, gereken minimum örnek sayısını
AÇIKÇA dokümante eder ve yetersizse `InsufficientHistoryError` fırlatır.
NaN/inf ASLA üretilmez; sıfıra bölme gibi matematiksel olarak tanımsız
durumlar `FeatureCalculationError` ile ele alınır.

Girdi sözleşmesi: her fonksiyon, `candles: Sequence[Candle]` alır — bu
liste ASCENDING open_time sırasında olmalı ve son eleman "as of" anını
temsil eder (yani `candles[-1]` "şimdiki" kapanmış candle'dır). Pencere
hesaplamaları HER ZAMAN listenin SONUNDAN geriye doğru alınır (mevcut
candle dahil) — bu, "current candle dahil mi?" sorusuna verilen AÇIK
cevaptır: EVET, `candles[-1]` (en son kapanmış candle) pencereye DAHİLDİR.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence

from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.errors import FeatureCalculationError, InsufficientHistoryError


def _require_history(candles: Sequence[Candle], required: int, feature_name: str) -> None:
    if len(candles) < required:
        raise InsufficientHistoryError(feature_name, required, len(candles))


def _closes(candles: Sequence[Candle]) -> list[float]:
    return [c.close for c in candles]


def _finite(value: float, context: str) -> float:
    if not math.isfinite(value):
        raise FeatureCalculationError(f"{context}: sonuç finite değil ({value!r})")
    return value


# ---------------------------------------------------------------------------
# Trend / moving averages
# ---------------------------------------------------------------------------

def sma(candles: Sequence[Candle], period: int) -> float:
    """Simple Moving Average. Pencere: son `period` kapanmış candle'ın close'u.
    Minimum geçmiş: `period`."""
    _require_history(candles, period, f"SMA_{period}")
    window = _closes(candles)[-period:]
    return _finite(sum(window) / period, f"SMA_{period}")


def ema(candles: Sequence[Candle], period: int) -> float:
    """Exponential Moving Average.

    İlklendirme: ilk `period` candle'ın SMA'sı ile başlar (endüstri standart
    yaklaşımı). Alpha = 2 / (period + 1). Ardından recurrence:
        EMA_t = close_t * alpha + EMA_{t-1} * (1 - alpha)
    kalan candle'lar üzerinden SIRAYLA (deterministic) uygulanır.
    Minimum geçmiş: `period` (yalnızca ilk SMA hesaplanabilirse EMA da
    hesaplanabilir; period'dan fazla veri varsa recurrence ilerler).
    """
    _require_history(candles, period, f"EMA_{period}")
    closes = _closes(candles)
    alpha = 2.0 / (period + 1)
    ema_value = sum(closes[:period]) / period  # ilk SMA ile ilklendirme
    for close in closes[period:]:
        ema_value = close * alpha + ema_value * (1 - alpha)
    return _finite(ema_value, f"EMA_{period}")


# ---------------------------------------------------------------------------
# Momentum
# ---------------------------------------------------------------------------

def rsi(candles: Sequence[Candle], period: int = 14) -> float:
    """Relative Strength Index — Wilder smoothing kullanılır.

    Minimum geçmiş: `period + 1` (period adet fiyat DEĞİŞİMİ gerekir).
    Wilder ilklendirmesi: ilk `period` değişimin ortalama gain/loss'u ile
    başlar; sonraki değişimler Wilder recurrence ile smooth edilir:
        avg_gain_t = (avg_gain_{t-1} * (period-1) + gain_t) / period
    (aynısı loss için).

    Sıfır-average-loss davranışı: avg_loss == 0 VE avg_gain > 0 ise
    RSI = 100 (fiyat yalnızca yükseldi). Hem avg_gain hem avg_loss == 0 ise
    (fiyat hiç değişmedi) RSI = 50 (nötr) — bu AÇIKÇA tanımlanmış bir
    konvansiyondur (bazı kütüphaneler 100 döndürür; bu implementasyon 50'yi
    "değişim yok = nötr" olarak tercih eder ve dokümante eder).
    """
    _require_history(candles, period + 1, f"RSI_{period}")
    closes = _closes(candles)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:], strict=True):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    result = 100.0 - (100.0 / (1.0 + rs))
    return _finite(result, f"RSI_{period}")


def roc(candles: Sequence[Candle], period: int) -> float:
    """Rate of Change: (close_now - close_{now-period}) / close_{now-period} * 100.
    Minimum geçmiş: `period + 1`. Sıfır baz fiyatı `FeatureCalculationError`."""
    _require_history(candles, period + 1, f"ROC_{period}")
    closes = _closes(candles)
    base = closes[-1 - period]
    if base == 0.0:
        raise FeatureCalculationError(f"ROC_{period}: baz fiyat (close) sıfır, ROC tanımsız")
    result = (closes[-1] - base) / base * 100.0
    return _finite(result, f"ROC_{period}")


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------

def true_range(current: Candle, previous: Candle | None) -> float:
    """True Range: max(high-low, |high-prev_close|, |low-prev_close|).
    `previous=None` ise (ilk candle) yalnızca high-low kullanılır."""
    if previous is None:
        return _finite(current.high - current.low, "TRUE_RANGE")
    result = max(
        current.high - current.low,
        abs(current.high - previous.close),
        abs(current.low - previous.close),
    )
    return _finite(result, "TRUE_RANGE")


def atr(candles: Sequence[Candle], period: int = 14) -> float:
    """Average True Range — Wilder smoothing.

    Minimum geçmiş: `period + 1` (period adet true range için bir önceki
    candle gerekir). İlklendirme: ilk `period` true range'in basit
    ortalaması; ardından Wilder recurrence:
        ATR_t = (ATR_{t-1} * (period-1) + TR_t) / period
    """
    _require_history(candles, period + 1, f"ATR_{period}")
    true_ranges = [true_range(candles[i], candles[i - 1]) for i in range(1, len(candles))]
    atr_value = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr_value = (atr_value * (period - 1) + tr) / period
    return _finite(atr_value, f"ATR_{period}")


def rolling_std(candles: Sequence[Candle], period: int, ddof: int = 0) -> float:
    """Kapanış fiyatlarının rolling standart sapması.

    ddof=0 (population std, varsayılan) — istatistiksel örneklem
    çıkarımı DEĞİL, tanımlayıcı bir volatilite ölçüsü olduğundan bu
    tercih edilmiştir. `ddof=1` istenirse örneklem std'si kullanılabilir.
    Minimum geçmiş: `period`.
    """
    _require_history(candles, period, f"ROLLING_STD_{period}")
    window = _closes(candles)[-period:]
    if ddof == 0:
        result = statistics.pstdev(window)
    elif ddof == 1:
        if period < 2:
            raise FeatureCalculationError(f"ROLLING_STD_{period}: ddof=1 için period>=2 gerekli")
        result = statistics.stdev(window)
    else:
        raise FeatureCalculationError(f"ROLLING_STD_{period}: desteklenmeyen ddof={ddof}")
    return _finite(result, f"ROLLING_STD_{period}")


def bollinger_basis(candles: Sequence[Candle], period: int) -> float:
    """Bollinger orta bant = SMA(period)."""
    return sma(candles, period)


def bollinger_upper(candles: Sequence[Candle], period: int, num_std: float = 2.0) -> float:
    """Bollinger üst bant = basis + num_std * rolling_std(ddof=0)."""
    basis = bollinger_basis(candles, period)
    std = rolling_std(candles, period, ddof=0)
    return _finite(basis + num_std * std, f"BOLLINGER_UPPER_{period}_{num_std}")


def bollinger_lower(candles: Sequence[Candle], period: int, num_std: float = 2.0) -> float:
    """Bollinger alt bant = basis - num_std * rolling_std(ddof=0)."""
    basis = bollinger_basis(candles, period)
    std = rolling_std(candles, period, ddof=0)
    return _finite(basis - num_std * std, f"BOLLINGER_LOWER_{period}_{num_std}")


def bollinger_bandwidth(candles: Sequence[Candle], period: int, num_std: float = 2.0) -> float:
    """Bollinger bandwidth = (upper - lower) / basis. basis == 0 -> hata."""
    basis = bollinger_basis(candles, period)
    upper = bollinger_upper(candles, period, num_std)
    lower = bollinger_lower(candles, period, num_std)
    if basis == 0.0:
        raise FeatureCalculationError(f"BOLLINGER_BANDWIDTH_{period}_{num_std}: basis sıfır, bandwidth tanımsız")
    return _finite((upper - lower) / basis, f"BOLLINGER_BANDWIDTH_{period}_{num_std}")


# ---------------------------------------------------------------------------
# Volume
# ---------------------------------------------------------------------------

def rolling_volume_mean(candles: Sequence[Candle], period: int) -> float:
    """Son `period` candle'ın hacim ortalaması."""
    _require_history(candles, period, f"VOLUME_MEAN_{period}")
    window = [c.volume for c in candles[-period:]]
    return _finite(sum(window) / period, f"VOLUME_MEAN_{period}")


def relative_volume(candles: Sequence[Candle], period: int) -> float:
    """Güncel candle'ın hacmi / rolling ortalama hacim. Ortalama sıfırsa hata."""
    mean_volume = rolling_volume_mean(candles, period)
    if mean_volume == 0.0:
        raise FeatureCalculationError(f"RELATIVE_VOLUME_{period}: ortalama hacim sıfır, relative volume tanımsız")
    result = candles[-1].volume / mean_volume
    return _finite(result, f"RELATIVE_VOLUME_{period}")


def volume_zscore(candles: Sequence[Candle], period: int, ddof: int = 0) -> float:
    """(güncel hacim - rolling ortalama) / rolling std. std sıfırsa hata."""
    _require_history(candles, period, f"VOLUME_ZSCORE_{period}")
    window = [c.volume for c in candles[-period:]]
    mean_volume = sum(window) / period
    std = statistics.pstdev(window) if ddof == 0 else statistics.stdev(window)
    if std == 0.0:
        raise FeatureCalculationError(f"VOLUME_ZSCORE_{period}: hacim std'si sıfır, z-score tanımsız")
    result = (candles[-1].volume - mean_volume) / std
    return _finite(result, f"VOLUME_ZSCORE_{period}")


# ---------------------------------------------------------------------------
# Price structure
# ---------------------------------------------------------------------------

def highest_high(candles: Sequence[Candle], period: int) -> float:
    """Son `period` candle'ın en yüksek high değeri (güncel candle DAHİL)."""
    _require_history(candles, period, f"HIGHEST_HIGH_{period}")
    return _finite(max(c.high for c in candles[-period:]), f"HIGHEST_HIGH_{period}")


def lowest_low(candles: Sequence[Candle], period: int) -> float:
    """Son `period` candle'ın en düşük low değeri (güncel candle DAHİL)."""
    _require_history(candles, period, f"LOWEST_LOW_{period}")
    return _finite(min(c.low for c in candles[-period:]), f"LOWEST_LOW_{period}")


def distance_from_high(candles: Sequence[Candle], period: int) -> float:
    """(highest_high - güncel close) / highest_high. highest_high==0 -> hata."""
    hh = highest_high(candles, period)
    if hh == 0.0:
        raise FeatureCalculationError(f"DIST_FROM_HIGH_{period}: highest_high sıfır")
    result = (hh - candles[-1].close) / hh
    return _finite(result, f"DIST_FROM_HIGH_{period}")


def distance_from_low(candles: Sequence[Candle], period: int) -> float:
    """(güncel close - lowest_low) / lowest_low. lowest_low==0 -> hata."""
    ll = lowest_low(candles, period)
    if ll == 0.0:
        raise FeatureCalculationError(f"DIST_FROM_LOW_{period}: lowest_low sıfır")
    result = (candles[-1].close - ll) / ll
    return _finite(result, f"DIST_FROM_LOW_{period}")


def body_size(candles: Sequence[Candle]) -> float:
    """|close - open|. Minimum geçmiş: 1."""
    _require_history(candles, 1, "BODY_SIZE")
    c = candles[-1]
    return _finite(abs(c.close - c.open), "BODY_SIZE")


def upper_wick(candles: Sequence[Candle]) -> float:
    """high - max(open, close). Minimum geçmiş: 1."""
    _require_history(candles, 1, "UPPER_WICK")
    c = candles[-1]
    return _finite(c.high - max(c.open, c.close), "UPPER_WICK")


def lower_wick(candles: Sequence[Candle]) -> float:
    """min(open, close) - low. Minimum geçmiş: 1."""
    _require_history(candles, 1, "LOWER_WICK")
    c = candles[-1]
    return _finite(min(c.open, c.close) - c.low, "LOWER_WICK")


def body_to_range_ratio(candles: Sequence[Candle]) -> float:
    """body_size / (high - low). Range sıfırsa (doji, high==low) SONUÇ 0.0
    olarak tanımlanır (0/0 matematiksel olarak tanımsız olsa da, "hiç
    hareket yok" durumunda oran anlamlı biçimde 0 kabul edilir — bu AÇIK
    bir konvansiyondur, NaN üretilmez)."""
    _require_history(candles, 1, "BODY_TO_RANGE_RATIO")
    c = candles[-1]
    price_range = c.high - c.low
    if price_range == 0.0:
        return 0.0
    result = abs(c.close - c.open) / price_range
    return _finite(result, "BODY_TO_RANGE_RATIO")


# ---------------------------------------------------------------------------
# Returns
# ---------------------------------------------------------------------------

def simple_return(candles: Sequence[Candle], period: int = 1) -> float:
    """(close_now - close_{now-period}) / close_{now-period}. Minimum: period+1."""
    _require_history(candles, period + 1, f"SIMPLE_RETURN_{period}")
    closes = _closes(candles)
    base = closes[-1 - period]
    if base == 0.0:
        raise FeatureCalculationError(f"SIMPLE_RETURN_{period}: baz fiyat sıfır")
    result = (closes[-1] - base) / base
    return _finite(result, f"SIMPLE_RETURN_{period}")


def log_return(candles: Sequence[Candle], period: int = 1) -> float:
    """ln(close_now / close_{now-period}). Minimum: period+1.
    Baz veya güncel fiyat <= 0 ise (geçersiz logaritma domain'i) hata."""
    _require_history(candles, period + 1, f"LOG_RETURN_{period}")
    closes = _closes(candles)
    base = closes[-1 - period]
    current = closes[-1]
    if base <= 0.0 or current <= 0.0:
        raise FeatureCalculationError(f"LOG_RETURN_{period}: fiyat <= 0, logaritma tanımsız")
    result = math.log(current / base)
    return _finite(result, f"LOG_RETURN_{period}")


def cumulative_return(candles: Sequence[Candle], period: int) -> float:
    """(close_now / close_{now-period}) - 1 — `period` candle'lık kümülatif getiri."""
    _require_history(candles, period + 1, f"CUMULATIVE_RETURN_{period}")
    closes = _closes(candles)
    base = closes[-1 - period]
    if base == 0.0:
        raise FeatureCalculationError(f"CUMULATIVE_RETURN_{period}: baz fiyat sıfır")
    result = (closes[-1] / base) - 1.0
    return _finite(result, f"CUMULATIVE_RETURN_{period}")


# ---------------------------------------------------------------------------
# VWAP
# ---------------------------------------------------------------------------

def _typical_price(c: Candle) -> float:
    return (c.high + c.low + c.close) / 3.0


def rolling_vwap(candles: Sequence[Candle], period: int) -> float:
    """Rolling VWAP = sum(typical_price * volume) / sum(volume), son `period`
    candle üzerinden. Fiyat bazı: typical price ((H+L+C)/3) — Binance kline
    yanıtı doğrudan trade-bazlı VWAP sağlamadığından bu, candle-seviyesi en
    yaygın yaklaşımdır. Toplam hacim sıfırsa VWAP tanımsızdır -> hata."""
    _require_history(candles, period, f"VWAP_{period}")
    window = candles[-period:]
    total_volume = sum(c.volume for c in window)
    if total_volume == 0.0:
        raise FeatureCalculationError(f"VWAP_{period}: toplam hacim sıfır, VWAP tanımsız")
    weighted_sum = sum(_typical_price(c) * c.volume for c in window)
    result = weighted_sum / total_volume
    return _finite(result, f"VWAP_{period}")


def vwap_deviation(candles: Sequence[Candle], period: int) -> float:
    """(güncel close - VWAP) / VWAP. VWAP sıfırsa hata."""
    vwap_value = rolling_vwap(candles, period)
    if vwap_value == 0.0:
        raise FeatureCalculationError(f"VWAP_DEVIATION_{period}: VWAP sıfır")
    result = (candles[-1].close - vwap_value) / vwap_value
    return _finite(result, f"VWAP_DEVIATION_{period}")


# ---------------------------------------------------------------------------
# Calculator dispatch tablosu (registry.py::FeatureConfig.calculator_key -> fonksiyon)
# ---------------------------------------------------------------------------

CANDLE_CALCULATORS = {
    "sma": sma,
    "ema": ema,
    "rsi": rsi,
    "roc": roc,
    "atr": atr,
    "rolling_std": rolling_std,
    "bollinger_basis": bollinger_basis,
    "bollinger_upper": bollinger_upper,
    "bollinger_lower": bollinger_lower,
    "bollinger_bandwidth": bollinger_bandwidth,
    "rolling_volume_mean": rolling_volume_mean,
    "relative_volume": relative_volume,
    "volume_zscore": volume_zscore,
    "highest_high": highest_high,
    "lowest_low": lowest_low,
    "distance_from_high": distance_from_high,
    "distance_from_low": distance_from_low,
    "body_size": body_size,
    "upper_wick": upper_wick,
    "lower_wick": lower_wick,
    "body_to_range_ratio": body_to_range_ratio,
    "simple_return": simple_return,
    "log_return": log_return,
    "cumulative_return": cumulative_return,
    "rolling_vwap": rolling_vwap,
    "vwap_deviation": vwap_deviation,
}


=== FILE: crypto_signal_engine/features/domain.py ===
"""
Feature Engine domain modelleri.

Kural (Faz 3 Bölüm 4): invalid feature state fiziksel olarak temsil
edilemez. Type hint'ler runtime'da hiçbir şeyi garanti etmez — bu modüldeki
her dataclass, Phase 1'in ortak validation yardımcılarını (`_validation.py`)
yeniden kullanarak kendi invariant'larını `__post_init__` içinde uygular.

Bu modül hiçbir Binance/network bağımlılığı içermez; yalnızca Phase 1
domain modellerine (Candle, OrderBookSnapshot, Trade, Timeframe) ve ortak
validation yardımcılarına bağımlıdır.
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
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import FeatureValidationError


@dataclass(frozen=True)
class FeatureIdentity:
    """Tek bir feature değerinin değişmez kimliği.

    (symbol, timeframe, feature_name) üçlüsü, bir feature'ın "ne" olduğunu
    tanımlar; `as_of` ise "ne zamana ait" olduğunu. `feature_name`
    deterministic ve parametre-kodlu olmalıdır (örn. "EMA_20") — bkz.
    registry.py::FeatureConfig.
    """

    symbol: str
    timeframe: Timeframe
    feature_name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")
        if not isinstance(self.feature_name, str) or not self.feature_name.strip():
            raise FeatureValidationError("feature_name boş olmayan bir str olmalı")


@dataclass(frozen=True)
class FeatureValue:
    """Tek bir (identity, as_of) çifti için hesaplanmış finite sayısal değer."""

    identity: FeatureIdentity
    as_of: datetime
    value: float

    def __post_init__(self) -> None:
        if not isinstance(self.identity, FeatureIdentity):
            raise FeatureValidationError(f"identity bir FeatureIdentity olmalı, alınan: {type(self.identity)!r}")
        require_utc_aware(self.as_of, "as_of")
        require_finite(self.value, "value")

    @property
    def symbol(self) -> str:
        return self.identity.symbol

    @property
    def timeframe(self) -> Timeframe:
        return self.identity.timeframe

    @property
    def feature_name(self) -> str:
        return self.identity.feature_name


@dataclass(frozen=True)
class FeatureSnapshot:
    """Belirli bir (symbol, timeframe, as_of) için birden çok feature değerinin birleşimi.

    HARDENING (Faz 3 Bölüm 4 invariantları):
    - symbol normalize edilir
    - timeframe gerçek bir Timeframe enum değeri olmalı (require_enum)
    - as_of UTC-aware olmalı
    - `values` içindeki TÜM değerler finite olmalı
    - `values` gerçek immutable bir mapping'e sarılır (freeze_mapping) —
      duplicate feature adı zaten bir dict'in doğası gereği imkansızdır
      (aynı anahtara son yazılan değer kalır; bu yüzden `from_feature_values`
      constructor'ı EK OLARAK açık bir duplicate kontrolü yapar, sessiz
      üzerine-yazmayı ENGELLER)
    - snapshot yapısal olarak TEK bir symbol/timeframe'e bağlıdır (ayrı alan
      seviyesinde "karışma" fiziksel olarak imkansızdır)
    """

    symbol: str
    timeframe: Timeframe
    as_of: datetime
    values: Mapping[str, float] = field(default_factory=dict)
    context_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.timeframe, Timeframe, "timeframe")
        require_utc_aware(self.as_of, "as_of")
        for name, v in self.values.items():
            if not isinstance(name, str) or not name.strip():
                raise FeatureValidationError("feature adı boş olmayan bir str olmalı")
            require_finite(v, f"values['{name}']")
        object.__setattr__(self, "values", freeze_mapping(self.values, "values"))
        if self.context_id is not None and not self.context_id.strip():
            raise FeatureValidationError("context_id verilmişse boş olamaz")

    def get(self, feature_name: str) -> float | None:
        return self.values.get(feature_name)

    def __contains__(self, feature_name: str) -> bool:
        return feature_name in self.values

    @staticmethod
    def from_feature_values(
        symbol: str,
        timeframe: Timeframe,
        as_of: datetime,
        feature_values: tuple[FeatureValue, ...],
        context_id: str | None = None,
    ) -> "FeatureSnapshot":
        """Bir dizi `FeatureValue`'dan tutarlılığı doğrulanmış bir snapshot inşa eder.

        Doğrulanan invariantlar (Faz 3 Bölüm 4):
        - TÜM `FeatureValue.symbol`, snapshot symbol'üyle (normalize edilmiş
          hâliyle) eşleşmeli — mixed-symbol snapshot REDDEDİLİR.
        - TÜM `FeatureValue.timeframe`, snapshot timeframe'iyle eşleşmeli —
          mixed-timeframe snapshot REDDEDİLİR.
        - TÜM `FeatureValue.as_of`, snapshot `as_of`'undan SONRA OLAMAZ —
          "gelecekten feature değeri" REDDEDİLİR (no-look-ahead).
        - Aynı `feature_name`'e sahip birden fazla `FeatureValue` REDDEDİLİR
          (sessiz üzerine-yazma yasak — dict constructor'ı bunu tespit
          edemezdi, bu yüzden burada AÇIKÇA kontrol edilir).
        """
        canonical_symbol = normalize_symbol(symbol)
        require_enum(timeframe, Timeframe, "timeframe")
        require_utc_aware(as_of, "as_of")

        seen_names: set[str] = set()
        values: dict[str, float] = {}
        for fv in feature_values:
            if not isinstance(fv, FeatureValue):
                raise FeatureValidationError(f"feature_values yalnızca FeatureValue içermeli, alınan: {type(fv)!r}")
            if fv.symbol != canonical_symbol:
                raise FeatureValidationError(
                    f"mixed-symbol snapshot reddedildi: {fv.symbol} != {canonical_symbol}"
                )
            if fv.timeframe != timeframe:
                raise FeatureValidationError(
                    f"mixed-timeframe snapshot reddedildi: {fv.timeframe} != {timeframe}"
                )
            if fv.as_of > as_of:
                raise FeatureValidationError(
                    f"gelecekten feature değeri reddedildi: {fv.feature_name}.as_of "
                    f"({fv.as_of}) > snapshot.as_of ({as_of})"
                )
            if fv.feature_name in seen_names:
                raise FeatureValidationError(f"duplicate feature adı reddedildi: {fv.feature_name}")
            seen_names.add(fv.feature_name)
            values[fv.feature_name] = fv.value

        return FeatureSnapshot(
            symbol=canonical_symbol, timeframe=timeframe, as_of=as_of, values=values, context_id=context_id
        )


=== FILE: crypto_signal_engine/features/engine.py ===
"""
FeatureEngine.

Kural (Bölüm 14): deterministic, testable, Binance transport'tan bağımsız,
ağ çağrısı YOK, feature storage dışında yan etki YOK, execution
bağımlılığı YOK.

KRİTİK NO-LOOK-AHEAD GARANTİSİ (Bölüm 17): `compute_candle_features` ve
`compute_trade_features`, çağırana verilen listeyi `as_of`'a göre
FİLTRELER (yalnızca `close_time <= as_of` olan candle'lar / `timestamp <=
as_of` olan trade'ler kullanılır). Bu, çağıranın listeye `as_of`'tan SONRAKİ
veri eklemesinin sonucu DEĞİŞTİRMEMESİNİ yapısal olarak garanti eder —
adversarial "future data does not change past feature" testleri bu
filtrelemeye dayanır.

KRİTİK CLOSED-CANDLE POLİTİKASI (Bölüm 9): `compute_candle_features`,
girdideki TÜM candle'ların `is_closed=True` olduğunu doğrular; açık
(in-progress) bir candle bulunursa `FeatureValidationError` fırlatır.
Provisional/in-progress feature desteği bu sürümde YOKTUR (Bölüm 9 —
"basitleştirilmiş kabul edilebilir implementasyon: yalnızca kapalı
candle'lar").
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade
from crypto_signal_engine.errors import (
    FeatureCalculationError,
    FeatureValidationError,
    InsufficientHistoryError,
)
from crypto_signal_engine.features.candle_calculators import CANDLE_CALCULATORS
from crypto_signal_engine.features.domain import FeatureIdentity, FeatureSnapshot, FeatureValue
from crypto_signal_engine.features.orderbook_calculators import ORDER_BOOK_CALCULATORS
from crypto_signal_engine.features.registry import (
    FeatureRegistry,
    default_candle_feature_registry,
    default_order_book_feature_registry,
    default_trade_feature_registry,
)
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.features.trade_calculators import TRADE_CALCULATORS


def _prepare_candles(candles: list[Candle], symbol: str, timeframe: Timeframe, as_of: datetime) -> list[Candle]:
    """Closed-candle politikasını, symbol/timeframe tutarlılığını ve
    no-look-ahead filtrelemesini uygulayıp ASCENDING open_time sıralı,
    tekrarsız bir liste döndürür.

    KRİTİK SIRALAMA (no-look-ahead blocker düzeltmesi): `as_of` ufkunun
    DIŞINDaki (`close_time > as_of`) candle'lar, `is_closed` durumları NE
    OLURSA OLSUN, closed-candle kontrolünden ÖNCE sessizce ATLANIR. Bu,
    "gelecekte, henüz kapanmamış bir candle" (tamamen normal ve beklenen
    bir durum — o candle henüz oluşmaktadır) ile "ufkun İÇİNDE ama
    kapanmamış bir candle" (gerçek bir closed-candle politikası ihlali)
    arasındaki AYRIMI doğru kurar. Önceki sıralama (önce is_closed kontrolü)
    gelecekteki açık bir candle'ı yanlışlıkla reddediyordu — bu, çağıranın
    ileri-tarihli/açık candle içeren GERÇEKÇİ bir veri kümesi (örn. canonical
    state'in en son AÇIK candle'ı) geçirdiği her durumda sahte bir hataydı.
    """
    filtered: list[Candle] = []
    seen_open_times = set()
    for c in candles:
        if c.symbol != symbol:
            raise FeatureValidationError(f"mixed-symbol candle listesi reddedildi: {c.symbol} != {symbol}")
        if c.timeframe != timeframe:
            raise FeatureValidationError(f"mixed-timeframe candle listesi reddedildi: {c.timeframe} != {timeframe}")
        if c.close_time > as_of:
            continue  # no-look-ahead: as_of ufkunun DIŞINDA — is_closed'a BAKILMAKSIZIN atlanır
        if not c.is_closed:
            raise FeatureValidationError(
                f"açık (kapanmamış) candle reddedildi (open_time={c.open_time}) — "
                f"closed-candle politikası ihlali (Bölüm 9)"
            )
        if c.open_time in seen_open_times:
            raise FeatureValidationError(f"duplicate open_time reddedildi: {c.open_time}")
        seen_open_times.add(c.open_time)
        filtered.append(c)

    filtered.sort(key=lambda c: c.open_time)
    return filtered


class FeatureEngine:
    """Market data -> deterministic, validated feature snapshot dönüştürücüsü."""

    def __init__(
        self,
        candle_registry: FeatureRegistry | None = None,
        order_book_registry: FeatureRegistry | None = None,
        trade_registry: FeatureRegistry | None = None,
        history_store: FeatureHistoryStore | None = None,
    ) -> None:
        self._candle_registry = candle_registry or default_candle_feature_registry()
        self._order_book_registry = order_book_registry or default_order_book_feature_registry()
        self._trade_registry = trade_registry or default_trade_feature_registry()
        self._history = history_store or FeatureHistoryStore()

    # -- Candle features -----------------------------------------------------

    def compute_single_candle_feature(
        self, candles: list[Candle], symbol: str, timeframe: Timeframe, as_of: datetime, feature_name: str
    ) -> FeatureValue:
        """Tek bir candle feature'ını hesaplar; warm-up/calculation hataları
        DOĞRUDAN fırlatılır (yutulmaz) — bu, açık bir "insufficient history"
        sinyali istendiğinde kullanılacak düşük seviyeli API'dir."""
        canonical_symbol = normalize_symbol(symbol)
        require_enum(timeframe, Timeframe, "timeframe")
        require_utc_aware(as_of, "as_of")

        config = self._candle_registry.get(feature_name)
        prepared = _prepare_candles(candles, canonical_symbol, timeframe, as_of)
        calculator = CANDLE_CALCULATORS[config.calculator_key]
        value = calculator(prepared, **config.params)
        identity = FeatureIdentity(symbol=canonical_symbol, timeframe=timeframe, feature_name=feature_name)
        return FeatureValue(identity=identity, as_of=as_of, value=value)

    def compute_candle_features(
        self,
        candles: list[Candle],
        symbol: str,
        timeframe: Timeframe,
        as_of: datetime,
        feature_names: list[str] | None = None,
    ) -> FeatureSnapshot:
        """Birden çok candle feature'ını hesaplayıp bir `FeatureSnapshot`'a
        birleştirir.

        Politika: `InsufficientHistoryError` fırlatan feature'lar snapshot'tan
        SESSİZCE OMİT edilir (Bölüm 10 — "omit that feature from a clearly
        defined incomplete snapshot"). `FeatureCalculationError` İSE
        YUTULMAZ, doğrudan yukarı fırlatılır (bu, warm-up DEĞİL gerçek bir
        hesaplama defektidir — örn. sıfıra bölme).
        """
        canonical_symbol = normalize_symbol(symbol)
        require_enum(timeframe, Timeframe, "timeframe")
        require_utc_aware(as_of, "as_of")

        prepared = _prepare_candles(candles, canonical_symbol, timeframe, as_of)
        names = feature_names or [c.name for c in self._candle_registry.all()]

        feature_values: list[FeatureValue] = []
        for name in names:
            config = self._candle_registry.get(name)
            calculator = CANDLE_CALCULATORS[config.calculator_key]
            try:
                value = calculator(prepared, **config.params)
            except InsufficientHistoryError:
                continue  # incomplete snapshot: bu feature atlanır
            identity = FeatureIdentity(symbol=canonical_symbol, timeframe=timeframe, feature_name=name)
            feature_values.append(FeatureValue(identity=identity, as_of=as_of, value=value))

        return FeatureSnapshot.from_feature_values(canonical_symbol, timeframe, as_of, tuple(feature_values))

    # -- Order book features ---------------------------------------------------

    def compute_order_book_features(
        self, book: OrderBookSnapshot, feature_names: list[str] | None = None
    ) -> FeatureSnapshot:
        """Order book feature'larını hesaplar. `as_of`, `book.timestamp`'tır
        (Bölüm 6 — "feature timestamp must correspond to snapshot timestamp").

        `timeframe` parametresi order book snapshot'larında doğal olarak
        yoktur; provenance/state-isolation amacıyla `Timeframe.M1` "raporlama
        çerçevesi" olarak kullanılır (bu, dokümante edilmiş bir modelleme
        tercihidir — order book'un kendisi bir timeframe'e bağlı DEĞİLDİR).
        """
        names = feature_names or [c.name for c in self._order_book_registry.all()]
        feature_values: list[FeatureValue] = []
        for name in names:
            config = self._order_book_registry.get(name)
            calculator = ORDER_BOOK_CALCULATORS[config.calculator_key]
            value = calculator(book, **config.params)
            identity = FeatureIdentity(symbol=book.symbol, timeframe=Timeframe.M1, feature_name=name)
            feature_values.append(FeatureValue(identity=identity, as_of=book.timestamp, value=value))

        return FeatureSnapshot.from_feature_values(book.symbol, Timeframe.M1, book.timestamp, tuple(feature_values))

    # -- Trade features ------------------------------------------------------

    def compute_trade_features(
        self,
        trades: list[Trade],
        symbol: str,
        timeframe: Timeframe,
        as_of: datetime,
        feature_names: list[str] | None = None,
    ) -> FeatureSnapshot:
        """Trade-flow feature'larını hesaplar; no-look-ahead için `trades`
        listesi `timestamp <= as_of` olacak şekilde filtrelenir."""
        canonical_symbol = normalize_symbol(symbol)
        require_enum(timeframe, Timeframe, "timeframe")
        require_utc_aware(as_of, "as_of")

        filtered = []
        seen_ids = set()
        for t in trades:
            if t.symbol != canonical_symbol:
                raise FeatureValidationError(f"mixed-symbol trade listesi reddedildi: {t.symbol} != {canonical_symbol}")
            if t.timestamp > as_of:
                continue
            if t.trade_id in seen_ids:
                raise FeatureValidationError(f"duplicate trade_id reddedildi: {t.trade_id}")
            seen_ids.add(t.trade_id)
            filtered.append(t)
        filtered.sort(key=lambda t: t.trade_id)

        names = feature_names or [c.name for c in self._trade_registry.all()]
        feature_values: list[FeatureValue] = []
        for name in names:
            config = self._trade_registry.get(name)
            calculator = TRADE_CALCULATORS[config.calculator_key]
            try:
                value = calculator(filtered, **config.params)
            except InsufficientHistoryError:
                continue
            identity = FeatureIdentity(symbol=canonical_symbol, timeframe=timeframe, feature_name=name)
            feature_values.append(FeatureValue(identity=identity, as_of=as_of, value=value))

        return FeatureSnapshot.from_feature_values(canonical_symbol, timeframe, as_of, tuple(feature_values))

    # -- State/history ---------------------------------------------------------

    def commit_snapshot(self, snapshot: FeatureSnapshot) -> None:
        self._history.commit(snapshot)

    def latest_snapshot(self, symbol: str, timeframe: Timeframe) -> FeatureSnapshot | None:
        return self._history.latest(symbol, timeframe)

    def snapshot_as_of(self, symbol: str, timeframe: Timeframe, timestamp: datetime) -> FeatureSnapshot | None:
        """Bir üst-timeframe snapshot'ını, yalnızca `timestamp`'a kadar
        (dahil) görünür olacak şekilde sorgular — cross-timeframe alignment
        için TEK güvenli erişim noktası (Bölüm 8): `timestamp`'tan SONRAKİ
        bir snapshot ASLA döndürülmez."""
        return self._history.as_of(symbol, timeframe, timestamp)


=== FILE: crypto_signal_engine/features/orderbook_calculators.py ===
"""
Order book / microstructure feature hesaplayıcıları.

Girdi: Phase 2 `OrderBookSnapshot` (zaten canonical, sıralı, crossed-olmayan
— bu invariant'lar Phase 1 domain modeli tarafından ZATEN garanti edilir).
Bu modül hiçbir "fabrikasyon" yapmaz: boş/eksik seviye durumunda sessizce
uydurma fiyat üretmek YERİNE `FeatureCalculationError` fırlatılır.
"""

from __future__ import annotations

import math

from crypto_signal_engine.domain.models import OrderBookSnapshot
from crypto_signal_engine.errors import FeatureCalculationError, FeatureValidationError


def _finite(value: float, context: str) -> float:
    if not math.isfinite(value):
        raise FeatureCalculationError(f"{context}: sonuç finite değil ({value!r})")
    return value


def _require_valid_depth(n: int, context: str) -> None:
    if not isinstance(n, int) or isinstance(n, bool) or n < 1:
        raise FeatureValidationError(f"{context}: depth N >= 1 bir int olmalı, alınan: {n!r}")


def best_bid(book: OrderBookSnapshot) -> float:
    return _finite(book.best_bid.price, "BEST_BID")


def best_ask(book: OrderBookSnapshot) -> float:
    return _finite(book.best_ask.price, "BEST_ASK")


def mid_price(book: OrderBookSnapshot) -> float:
    return _finite(book.mid_price, "MID_PRICE")


def spread_abs(book: OrderBookSnapshot) -> float:
    return _finite(book.spread, "SPREAD_ABS")


def spread_bps(book: OrderBookSnapshot) -> float:
    """Spread, mid price'a göre basis point cinsinden: (spread/mid) * 10000.
    mid_price sıfırsa (teorik olarak imkansız, Candle/OrderBookLevel pozitif
    fiyat zorunlu kılar, ama defensive) hata."""
    mid = book.mid_price
    if mid == 0.0:
        raise FeatureCalculationError("SPREAD_BPS: mid_price sıfır")
    result = (book.spread / mid) * 10000.0
    return _finite(result, "SPREAD_BPS")


def bid_depth(book: OrderBookSnapshot, n: int) -> float:
    """İlk N bid seviyesinin toplam miktarı. N, mevcut seviye sayısından
    büyükse mevcut TÜM seviyeler kullanılır (kısmi derinlik) — bu, "az
    likidite" durumunda sessizce sıfır/hata üretmek yerine gerçekçi bir
    davranıştır ve dokümante edilmiştir."""
    _require_valid_depth(n, "BID_DEPTH")
    levels = book.bids[:n]
    return _finite(sum(level.quantity for level in levels), f"BID_DEPTH_{n}")


def ask_depth(book: OrderBookSnapshot, n: int) -> float:
    """İlk N ask seviyesinin toplam miktarı (bkz. `bid_depth` davranış notu)."""
    _require_valid_depth(n, "ASK_DEPTH")
    levels = book.asks[:n]
    return _finite(sum(level.quantity for level in levels), f"ASK_DEPTH_{n}")


def depth_imbalance(book: OrderBookSnapshot, n: int) -> float:
    """(bid_depth - ask_depth) / (bid_depth + ask_depth), [-1, 1] aralığında.
    Toplam derinlik sıfırsa (teorik olarak imkansız, seviyeler pozitif
    miktar zorunlu kılar) hata."""
    bd = bid_depth(book, n)
    ad = ask_depth(book, n)
    total = bd + ad
    if total == 0.0:
        raise FeatureCalculationError(f"DEPTH_IMBALANCE_{n}: toplam derinlik sıfır")
    result = (bd - ad) / total
    return _finite(result, f"DEPTH_IMBALANCE_{n}")


def microprice(book: OrderBookSnapshot) -> float:
    """Microprice (ağırlıklı orta fiyat), best bid/ask miktarlarına göre:
        (best_bid.price * best_ask.quantity + best_ask.price * best_bid.quantity)
        / (best_bid.quantity + best_ask.quantity)
    Bu, mid_price'tan farklı olarak hangi tarafın daha "ağır" olduğunu
    yansıtır. Toplam miktar sıfırsa (teorik olarak imkansız) hata."""
    bb, ba = book.best_bid, book.best_ask
    total_qty = bb.quantity + ba.quantity
    if total_qty == 0.0:
        raise FeatureCalculationError("MICROPRICE: best seviyelerin toplam miktarı sıfır")
    result = (bb.price * ba.quantity + ba.price * bb.quantity) / total_qty
    return _finite(result, "MICROPRICE")


def top_of_book_imbalance(book: OrderBookSnapshot) -> float:
    """(best_bid.qty - best_ask.qty) / (best_bid.qty + best_ask.qty), [-1,1]."""
    bb, ba = book.best_bid, book.best_ask
    total_qty = bb.quantity + ba.quantity
    if total_qty == 0.0:
        raise FeatureCalculationError("TOB_IMBALANCE: best seviyelerin toplam miktarı sıfır")
    result = (bb.quantity - ba.quantity) / total_qty
    return _finite(result, "TOB_IMBALANCE")


ORDER_BOOK_CALCULATORS = {
    "best_bid": best_bid,
    "best_ask": best_ask,
    "mid_price": mid_price,
    "spread_abs": spread_abs,
    "spread_bps": spread_bps,
    "bid_depth": bid_depth,
    "ask_depth": ask_depth,
    "depth_imbalance": depth_imbalance,
    "microprice": microprice,
    "top_of_book_imbalance": top_of_book_imbalance,
}


=== FILE: crypto_signal_engine/features/registry.py ===
"""
Feature registry.

Kural (Faz 3 Bölüm 13): feature'lar tek bir dev fonksiyona hardcode
edilmez; her feature bir `FeatureConfig` ile tanımlanır (isim, parametreler,
gerekli source tipi, gerekli timeframe, minimum geçmiş, hesaplayıcı
fonksiyon). Dynamic eval/import KULLANILMAZ. `FeatureRegistry` bir
instance'tır (module-level global mutable state DEĞİLDİR) — her
`FeatureEngine` kendi registry'sini enjekte eder.

İsimlendirme çakışma güvenliği: aynı `name` ile farklı parametrelerde iki
kayıt denemesi REDDEDİLİR (Bölüm 13 — "iki feature configürasyonu...
aynı identity'yi paylaşmamalı").
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum

from crypto_signal_engine.domain._validation import freeze_mapping, require_enum
from crypto_signal_engine.errors import FeatureValidationError


class FeatureSourceType(str, Enum):
    """Bir feature'ın hangi Phase 2 veri kaynağından hesaplandığı."""

    CANDLE = "CANDLE"
    ORDER_BOOK = "ORDER_BOOK"
    TRADE = "TRADE"


@dataclass(frozen=True)
class FeatureConfig:
    """Tek bir feature'ın deterministic, runtime-validated tanımı."""

    name: str  # deterministic, parametre-kodlu (örn. "EMA_20")
    source: FeatureSourceType
    calculator_key: str  # candle_calculators/orderbook_calculators/trade_calculators içindeki fonksiyon anahtarı
    params: Mapping[str, object] = field(default_factory=dict)
    min_history: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise FeatureValidationError("FeatureConfig.name boş olmayan bir str olmalı")
        require_enum(self.source, FeatureSourceType, "source")
        if not isinstance(self.calculator_key, str) or not self.calculator_key.strip():
            raise FeatureValidationError("FeatureConfig.calculator_key boş olmayan bir str olmalı")
        if not isinstance(self.min_history, int) or isinstance(self.min_history, bool) or self.min_history < 1:
            raise FeatureValidationError(f"min_history >= 1 bir int olmalı, alınan: {self.min_history!r}")
        object.__setattr__(self, "params", freeze_mapping(dict(self.params), "params"))


class FeatureRegistry:
    """Feature konfigürasyonlarının çakışma-güvenli, instance-scoped kayıt defteri.

    NOT: Bu bir MODULE-LEVEL global DEĞİLDİR (Bölüm 3 — "no hidden global
    state"). Her `FeatureEngine` kendi `FeatureRegistry` instance'ını alır.
    """

    def __init__(self) -> None:
        self._configs: dict[str, FeatureConfig] = {}

    def register(self, config: FeatureConfig) -> None:
        """Bir feature config'i kaydeder.

        Aynı `name` ile FARKLI bir config zaten kayıtlıysa (çakışan
        parametreler) `FeatureValidationError` fırlatılır — iki feature
        configürasyonu aynı identity'yi PAYLAŞAMAZ. Aynı `name` ile AYNI
        config tekrar kaydedilmeye çalışılırsa bu idempotent bir no-op'tur.
        """
        existing = self._configs.get(config.name)
        if existing is not None and existing != config:
            raise FeatureValidationError(
                f"feature identity çakışması: '{config.name}' zaten farklı parametrelerle "
                f"kayıtlı (mevcut: {existing.params}, yeni: {config.params})"
            )
        self._configs[config.name] = config

    def get(self, name: str) -> FeatureConfig:
        try:
            return self._configs[name]
        except KeyError as exc:
            raise FeatureValidationError(f"bilinmeyen feature: {name!r}") from exc

    def __contains__(self, name: str) -> bool:
        return name in self._configs

    def all(self) -> tuple[FeatureConfig, ...]:
        return tuple(self._configs.values())

    def by_source(self, source: FeatureSourceType) -> tuple[FeatureConfig, ...]:
        return tuple(c for c in self._configs.values() if c.source == source)


def default_candle_feature_registry() -> FeatureRegistry:
    """Bölüm 5'teki minimum candle feature setini kapsayan varsayılan registry.

    Her isim deterministic ve parametre-kodludur (örn. "SMA_20").
    """
    reg = FeatureRegistry()
    C = FeatureSourceType.CANDLE

    def add(name: str, key: str, params: dict, min_history: int) -> None:
        reg.register(FeatureConfig(name=name, source=C, calculator_key=key, params=params, min_history=min_history))

    add("SMA_20", "sma", {"period": 20}, 20)
    add("EMA_20", "ema", {"period": 20}, 20)
    add("RSI_14", "rsi", {"period": 14}, 15)
    add("ROC_10", "roc", {"period": 10}, 11)
    add("ATR_14", "atr", {"period": 14}, 15)
    add("ROLLING_STD_20", "rolling_std", {"period": 20, "ddof": 0}, 20)
    add("BOLLINGER_BASIS_20", "bollinger_basis", {"period": 20}, 20)
    add("BOLLINGER_UPPER_20_2", "bollinger_upper", {"period": 20, "num_std": 2.0}, 20)
    add("BOLLINGER_LOWER_20_2", "bollinger_lower", {"period": 20, "num_std": 2.0}, 20)
    add("BOLLINGER_BANDWIDTH_20_2", "bollinger_bandwidth", {"period": 20, "num_std": 2.0}, 20)
    add("VOLUME_MEAN_20", "rolling_volume_mean", {"period": 20}, 20)
    add("RELATIVE_VOLUME_20", "relative_volume", {"period": 20}, 20)
    add("VOLUME_ZSCORE_20", "volume_zscore", {"period": 20, "ddof": 0}, 20)
    add("HIGHEST_HIGH_20", "highest_high", {"period": 20}, 20)
    add("LOWEST_LOW_20", "lowest_low", {"period": 20}, 20)
    add("DIST_FROM_HIGH_20", "distance_from_high", {"period": 20}, 20)
    add("DIST_FROM_LOW_20", "distance_from_low", {"period": 20}, 20)
    add("BODY_SIZE", "body_size", {}, 1)
    add("UPPER_WICK", "upper_wick", {}, 1)
    add("LOWER_WICK", "lower_wick", {}, 1)
    add("BODY_TO_RANGE_RATIO", "body_to_range_ratio", {}, 1)
    add("SIMPLE_RETURN_1", "simple_return", {"period": 1}, 2)
    add("LOG_RETURN_1", "log_return", {"period": 1}, 2)
    add("CUMULATIVE_RETURN_20", "cumulative_return", {"period": 20}, 21)
    add("VWAP_20", "rolling_vwap", {"period": 20}, 20)
    add("VWAP_DEVIATION_20", "vwap_deviation", {"period": 20}, 20)
    return reg


def default_order_book_feature_registry(depth: int = 10) -> FeatureRegistry:
    """Bölüm 6'daki minimum order-book/microstructure feature setini kapsar."""
    reg = FeatureRegistry()
    O = FeatureSourceType.ORDER_BOOK

    def add(name: str, key: str, params: dict) -> None:
        reg.register(FeatureConfig(name=name, source=O, calculator_key=key, params=params, min_history=1))

    add("BEST_BID", "best_bid", {})
    add("BEST_ASK", "best_ask", {})
    add("MID_PRICE", "mid_price", {})
    add("SPREAD_ABS", "spread_abs", {})
    add("SPREAD_BPS", "spread_bps", {})
    add(f"BID_DEPTH_{depth}", "bid_depth", {"n": depth})
    add(f"ASK_DEPTH_{depth}", "ask_depth", {"n": depth})
    add(f"DEPTH_IMBALANCE_{depth}", "depth_imbalance", {"n": depth})
    add("MICROPRICE", "microprice", {})
    add("TOB_IMBALANCE", "top_of_book_imbalance", {})
    return reg


def default_trade_feature_registry(window_size: int = 100) -> FeatureRegistry:
    """Bölüm 7'deki minimum trade-flow feature setini kapsar."""
    reg = FeatureRegistry()
    T = FeatureSourceType.TRADE

    def add(name: str, key: str, params: dict) -> None:
        reg.register(FeatureConfig(name=name, source=T, calculator_key=key, params=params, min_history=1))

    add("BUY_VOLUME", "buy_volume", {})
    add("SELL_VOLUME", "sell_volume", {})
    add("TOTAL_VOLUME", "total_volume", {})
    add("VOLUME_IMBALANCE", "volume_imbalance", {})
    add("SIGNED_VOLUME", "signed_volume", {})
    add("TRADE_COUNT", "trade_count", {})
    add("AVG_TRADE_SIZE", "avg_trade_size", {})
    add(f"TRADE_INTENSITY_{window_size}", "trade_intensity", {"window_size": window_size})
    return reg


=== FILE: crypto_signal_engine/features/state.py ===
"""
Feature history/state store.

Kural (Faz 3 Bölüm 15): Phase 2 market-data state'i (StateManager,
CandleStateStore) ile KARIŞTIRILMAZ — bu, ayrı, kendi kendine yeten bir
in-memory feature history soyutlamasıdır.

Invariant'lar:
- eski bir snapshot, daha yeni canonical bir snapshot'ı SESSİZCE EZEMEZ
  (`as_of` küçükse commit reddedilir)
- aynı `as_of` ile ikinci bir commit UPSERT edilir (idempotent, son yazan
  kazanır) — bu, "aynı zaman damgası duplicate politikası"nın AÇIK tercihidir
- symbol/timeframe izolasyonu: her (symbol, timeframe) çifti kendi bağımsız
  geçmişine sahiptir
- döndürülen snapshot'lar zaten immutable'dır (FeatureSnapshot, Phase 1
  freeze_mapping ile), bu store ekstra bir kopyalama YAPMAZ (gerek yok)
- bounded retention: her (symbol, timeframe) için en fazla `max_history`
  snapshot tutulur (FIFO — en eski silinir)
"""

from __future__ import annotations

from collections import deque
from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import FeatureStateError
from crypto_signal_engine.features.domain import FeatureSnapshot

_DEFAULT_MAX_HISTORY = 500


class FeatureHistoryStore:
    """(symbol, timeframe) bazında bounded, deterministic feature snapshot geçmişi."""

    def __init__(self, max_history: int = _DEFAULT_MAX_HISTORY) -> None:
        if not isinstance(max_history, int) or isinstance(max_history, bool) or max_history < 1:
            raise FeatureStateError(f"max_history >= 1 bir int olmalı, alınan: {max_history!r}")
        self._max_history = max_history
        self._history: dict[tuple[str, Timeframe], deque[FeatureSnapshot]] = {}

    def commit(self, snapshot: FeatureSnapshot) -> None:
        """Bir snapshot'ı history'ye ekler.

        - `as_of`, mevcut EN SON snapshot'tan KESİNLİKLE ESKİYSE
          (`snapshot.as_of < latest.as_of`) reddedilir (`FeatureStateError`)
          — eski veri, daha yeni canonical state'i SESSİZCE EZEMEZ.
        - `as_of` mevcut en son snapshot'la AYNIYSA, UPSERT edilir (son
          yazan kazanır, idempotent).
        - `as_of` daha yeniyse, deque'e eklenir; `max_history` aşılırsa en
          eski silinir (FIFO, bounded retention).
        """
        key = (snapshot.symbol, snapshot.timeframe)
        history = self._history.setdefault(key, deque(maxlen=self._max_history))

        if history and snapshot.as_of < history[-1].as_of:
            raise FeatureStateError(
                f"{snapshot.symbol}/{snapshot.timeframe}: eski snapshot ({snapshot.as_of}) "
                f"mevcut en son snapshot'tan ({history[-1].as_of}) daha eski, reddedildi"
            )
        if history and snapshot.as_of == history[-1].as_of:
            history[-1] = snapshot  # upsert (idempotent)
            return
        history.append(snapshot)

    def latest(self, symbol: str, timeframe: Timeframe) -> FeatureSnapshot | None:
        """(symbol, timeframe) için en son commit edilmiş snapshot'ı döndürür."""
        key = (normalize_symbol(symbol), require_enum(timeframe, Timeframe, "timeframe"))
        history = self._history.get(key)
        return history[-1] if history else None

    def as_of(self, symbol: str, timeframe: Timeframe, timestamp: datetime) -> FeatureSnapshot | None:
        """(symbol, timeframe) için, `timestamp`'tan BÜYÜK OLMAYAN en son
        snapshot'ı döndürür (no-look-ahead: `timestamp`'tan SONRAKİ bir
        snapshot ASLA döndürülmez).
        """
        require_utc_aware(timestamp, "timestamp")
        key = (normalize_symbol(symbol), require_enum(timeframe, Timeframe, "timeframe"))
        history = self._history.get(key)
        if not history:
            return None
        result: FeatureSnapshot | None = None
        for snapshot in history:
            if snapshot.as_of <= timestamp:
                result = snapshot
            else:
                break  # history ascending as_of sırasında; daha ileri gitmeye gerek yok
        return result

    def history_length(self, symbol: str, timeframe: Timeframe) -> int:
        key = (normalize_symbol(symbol), require_enum(timeframe, Timeframe, "timeframe"))
        history = self._history.get(key)
        return len(history) if history else 0


=== FILE: crypto_signal_engine/features/trade_calculators.py ===
"""
Trade-flow feature hesaplayıcıları.

Aggressor yorumu (Binance aggTrade semantiği, Phase 2 `Trade.is_buyer_maker`
alanı üzerinden — DEĞİŞTİRİLMEDEN kullanılır):
- `is_buyer_maker=True`  -> alıcı taraf MAKER'dır -> agresif taraf SATICIDIR
  (sell-side aggression) -> bu trade "sell volume"e sayılır.
- `is_buyer_maker=False` -> agresif taraf ALICIDIR (buy-side aggression)
  -> bu trade "buy volume"e sayılır.
Bu yorum Phase 2 `parser.py`'nin zaten belgelediği semantiktir; burada
YENİDEN YORUMLANMAZ, doğrudan kullanılır.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from crypto_signal_engine.domain.models import Trade
from crypto_signal_engine.errors import FeatureCalculationError, InsufficientHistoryError


def _finite(value: float, context: str) -> float:
    if not math.isfinite(value):
        raise FeatureCalculationError(f"{context}: sonuç finite değil ({value!r})")
    return value


def _require_history(trades: Sequence[Trade], required: int, feature_name: str) -> None:
    if len(trades) < required:
        raise InsufficientHistoryError(feature_name, required, len(trades))


def _is_buy_aggressor(trade: Trade) -> bool:
    """`is_buyer_maker=False` -> agresif taraf alıcı (buy aggression)."""
    return not trade.is_buyer_maker


def buy_volume(trades: Sequence[Trade]) -> float:
    _require_history(trades, 1, "BUY_VOLUME")
    return _finite(sum(t.quantity for t in trades if _is_buy_aggressor(t)), "BUY_VOLUME")


def sell_volume(trades: Sequence[Trade]) -> float:
    _require_history(trades, 1, "SELL_VOLUME")
    return _finite(sum(t.quantity for t in trades if not _is_buy_aggressor(t)), "SELL_VOLUME")


def total_volume(trades: Sequence[Trade]) -> float:
    _require_history(trades, 1, "TOTAL_VOLUME")
    return _finite(sum(t.quantity for t in trades), "TOTAL_VOLUME")


def volume_imbalance(trades: Sequence[Trade]) -> float:
    """(buy_volume - sell_volume) / total_volume, [-1, 1]. total==0 -> hata."""
    bv = buy_volume(trades)
    sv = sell_volume(trades)
    total = bv + sv
    if total == 0.0:
        raise FeatureCalculationError("VOLUME_IMBALANCE: toplam hacim sıfır")
    return _finite((bv - sv) / total, "VOLUME_IMBALANCE")


def signed_volume(trades: Sequence[Trade]) -> float:
    """buy_volume - sell_volume (yönlü net hacim, normalize edilmemiş)."""
    return _finite(buy_volume(trades) - sell_volume(trades), "SIGNED_VOLUME")


def trade_count(trades: Sequence[Trade]) -> float:
    _require_history(trades, 1, "TRADE_COUNT")
    return _finite(float(len(trades)), "TRADE_COUNT")


def avg_trade_size(trades: Sequence[Trade]) -> float:
    """total_volume / trade_count. trade_count zaten >=1 garanti edilir
    (_require_history üzerinden), bu yüzden sıfıra bölme burada oluşamaz."""
    _require_history(trades, 1, "AVG_TRADE_SIZE")
    result = total_volume(trades) / len(trades)
    return _finite(result, "AVG_TRADE_SIZE")


def trade_intensity(trades: Sequence[Trade], window_size: int) -> float:
    """Rolling trade intensity: son `window_size` trade içindeki (veya
    mevcut TÜM trade'ler `window_size`'dan azsa mevcut tümü) saniye
    başına ortalama trade sayısı.

    Pencere semantiği: `trades[-window_size:]` — zaman bazlı DEĞİL, SAYI
    bazlı bir pencere (son N trade). Süre, bu pencerenin ilk ve son trade'i
    arasındaki zaman farkıdır; süre sıfırsa (tüm trade'ler aynı zaman
    damgasında, örn. tek bir batch) `FeatureCalculationError` fırlatılır
    (saniye başına sonsuz trade tanımsızdır).
    """
    if len(trades) < 2:
        raise InsufficientHistoryError(f"TRADE_INTENSITY_{window_size}", 2, len(trades))
    window = trades[-window_size:] if len(trades) >= window_size else trades
    duration_seconds = (window[-1].timestamp - window[0].timestamp).total_seconds()
    if duration_seconds <= 0.0:
        raise FeatureCalculationError(f"TRADE_INTENSITY_{window_size}: pencere süresi sıfır/negatif")
    result = len(window) / duration_seconds
    return _finite(result, f"TRADE_INTENSITY_{window_size}")


TRADE_CALCULATORS = {
    "buy_volume": buy_volume,
    "sell_volume": sell_volume,
    "total_volume": total_volume,
    "volume_imbalance": volume_imbalance,
    "signed_volume": signed_volume,
    "trade_count": trade_count,
    "avg_trade_size": avg_trade_size,
    "trade_intensity": trade_intensity,
}


=== FILE: crypto_signal_engine/ops/__init__.py ===
"""
Faz 8 — Operations & Ubuntu Deployment.

Bu paket, kabul edilmiş Faz 1-7 sisteminin (domain/provider/feature/
signal/paper-trading/runtime/persistence) davranışını DEĞİŞTİRMEDEN,
onu uzun süre çalışan, gözlemlenebilir, güvenli bir Ubuntu servisi olarak
işletmek için gereken operasyonel "kabuk"u sağlar: konfigürasyon,
loglama, process-lock, sağlık anlık görüntüsü ve CLI giriş noktası.

HARD SAFETY INVARIANT (değişmez, bkz. SAFETY_INVARIANTS.md):
`ALLOW_LIVE_TRADING = False` — bu paket hiçbir API key/secret/HMAC/
signed-endpoint/Testnet/Mainnet-execution alanı TAŞIMAZ. Bu paket
yalnızca PUBLIC Binance market data + paper trading + persistence'ı
bir Ubuntu process/service olarak SARAR.
"""

from __future__ import annotations


=== FILE: crypto_signal_engine/ops/admin.py ===
"""
24/7 Ops v1, Step 3 — Control Center admin backend.

`ops/dashboard.py` is READ-ONLY BY DESIGN (see its module docstring,
"Bölüm 6" — the dashboard's HTTP handlers run in their OWN OS thread and
must never touch a live coordinator/asyncio object directly, only the
periodic atomic JSON snapshot file). This module is the ONE, narrowly
scoped exception: a thread-safe bridge that lets the dashboard's HTTP
thread request exactly THREE actions — pause new entries, resume new
entries, stop the process — without ever touching the live
`RuntimeCoordinator`/paper engine/execution store directly.

Why this is safe despite the "dashboard never touches live objects"
rule: every primitive `AdminController` exposes is one that is ALREADY
safe to mutate from any OS thread without extra locking:
  - `entries_paused` is a `threading.Event` — `set()`/`clear()`/
    `is_set()` are atomic, GIL-protected, cross-thread-safe by design
    (this is exactly what `threading.Event` is for).
  - `stop()` schedules the EXISTING, already-idempotent
    `Application.request_shutdown()` onto the owning asyncio loop via
    `loop.call_soon_threadsafe()` — never called directly from the HTTP
    thread (an `asyncio.Event.set()` is only safe to call on its own
    loop's thread).
  - The small `_last_action` record is guarded by an explicit
    `threading.Lock` (belt-and-suspenders — a dict/dataclass
    reassignment is atomic under the GIL anyway, but this makes the
    "safe to read+write from two threads" invariant explicit rather
    than implicit).

Disabled by default: `AppConfig.dashboard_admin_token=None` means
`Application` never constructs an `AdminController` at all — the
dashboard then behaves EXACTLY as it did before this milestone (see
`tests/test_ops_admin.py::TestAdminDisabledByDefault` and
`tests/test_ops_dashboard.py`'s existing regression coverage)."""

from __future__ import annotations

import asyncio
import hmac
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from crypto_signal_engine.ops.event_log import record_event
from crypto_signal_engine.ops.health_snapshot import utc_now
from crypto_signal_engine.ops.notifier import Notifier, safe_notify

_LOGGER = logging.getLogger("crypto_signal_engine.ops.admin")

# Documented choice (see this milestone's DECISIONS.md Karar): the admin
# token is sent as a request HEADER, never a query-string parameter — a
# query param is far more likely to end up copied into a browser history
# entry, a proxy access log, or a shared dashboard URL than a header is.
ADMIN_TOKEN_HEADER = "X-Admin-Token"


@dataclass(frozen=True)
class AdminActionRecord:
    action: str  # "pause" | "resume" | "stop"
    at: str  # ISO-8601 UTC
    detail: str

    def as_dict(self) -> dict[str, object]:
        return {"action": self.action, "at": self.at, "detail": self.detail}


class AdminController:
    """The ONE sanctioned mutation surface for the dashboard's HTTP
    thread. Constructed only when `AppConfig.dashboard_admin_token` is
    set (see `app.py::Application.start()`)."""

    def __init__(
        self,
        *,
        token: str,
        loop: asyncio.AbstractEventLoop,
        request_shutdown: Callable[[str], None],
        entries_paused: threading.Event | None = None,
        notifier: Notifier | None = None,
        event_log_path: Path | None = None,
    ) -> None:
        self._token = token
        self._loop = loop
        self._request_shutdown = request_shutdown
        self._notifier = notifier
        # UI Polish v1, Step A9 — event log (additive, optional, `None`
        # by default). `record_event()` never raises (see
        # `ops/event_log.py`) — written ALONGSIDE, never instead of, the
        # `safe_notify()` call already at each action below.
        self._event_log_path = event_log_path
        # `entries_paused` is the SAME `threading.Event` object the
        # caller (`app.py::Application`) already wired into
        # `SignalTestnetBridge.entries_paused_provider` — never a second,
        # disconnected Event. Defaults to a fresh, private one only for
        # callers that don't need that wiring (e.g. isolated unit tests).
        self.entries_paused = entries_paused if entries_paused is not None else threading.Event()
        self._lock = threading.Lock()
        self._last_action: AdminActionRecord | None = None

    def check_token(self, provided: str | None) -> bool:
        """Constant-time comparison (`hmac.compare_digest`) — a local
        admin token is still a credential; a naive `==` would leak
        timing information about how many leading characters matched."""
        if provided is None:
            return False
        return hmac.compare_digest(provided, self._token)

    def _record(self, action: str, detail: str) -> AdminActionRecord:
        now = utc_now()
        record = AdminActionRecord(action=action, at=now.isoformat(), detail=detail)
        with self._lock:
            self._last_action = record
        if self._event_log_path is not None:
            record_event(self._event_log_path, event_type=f"admin_{action}", detail=detail, occurred_at=now)
        return record

    def status(self) -> dict[str, object]:
        with self._lock:
            last = self._last_action
        return {
            "enabled": True,
            "entries_paused": self.entries_paused.is_set(),
            "last_action": None if last is None else last.as_dict(),
        }

    def pause(self) -> dict[str, object]:
        was_paused = self.entries_paused.is_set()
        self.entries_paused.set()
        record = self._record("pause", f"entries_paused: {was_paused} -> True")
        _LOGGER.warning("admin action: pause (%s)", record.detail)
        safe_notify(self._notifier, f"[crypto-signal-engine] Yeni girişler DURAKLATILDI (admin action, {record.at})")
        return self.status()

    def resume(self) -> dict[str, object]:
        was_paused = self.entries_paused.is_set()
        self.entries_paused.clear()
        record = self._record("resume", f"entries_paused: {was_paused} -> False")
        _LOGGER.warning("admin action: resume (%s)", record.detail)
        safe_notify(self._notifier, f"[crypto-signal-engine] Yeni girişler DEVAM ETTİRİLDİ (admin action, {record.at})")
        return self.status()

    def stop(self) -> dict[str, object]:
        """Reuses `Application.request_shutdown()` VERBATIM — the exact
        same idempotent shutdown chain a SIGTERM triggers (see
        `app.py::Application._graceful_stop`). Never a parallel shutdown
        implementation. Scheduled via `call_soon_threadsafe` because this
        method runs on the dashboard's HTTP thread, not the asyncio
        loop's own thread — `request_shutdown()` sets an `asyncio.Event`,
        which is only safe to mutate on its owning loop's thread."""
        record = self._record("stop", "graceful shutdown requested via admin API (call_soon_threadsafe)")
        _LOGGER.warning("admin action: stop — scheduling request_shutdown() on the runtime event loop")
        safe_notify(self._notifier, f"[crypto-signal-engine] Acil DURDUR admin action ile tetiklendi ({record.at})")
        self._loop.call_soon_threadsafe(self._request_shutdown, "admin-stop")
        return self.status()


=== FILE: crypto_signal_engine/ops/config.py ===
"""
Faz 8 — ortam değişkeni tabanlı uygulama konfigürasyonu.

Kural (Bölüm 8 — "generic bir settings framework İNŞA ETME"): bu modül
tek bir immutable `AppConfig` dataclass'ı ve tek bir `load_config()`
fonksiyonu tanımlar — YAML/TOML parser, plugin sistemi veya çok katmanlı
override zinciri YOKTUR. Kaynak, açıkça bir `Mapping[str, str]` (üretimde
`os.environ`, testte sahte bir dict) — `.env` dosyası okuma bile bu
modülün sorumluluğunda DEĞİLDİR (systemd'nin kendi `EnvironmentFile=`
mekanizması bunu zaten yapar, bkz. deploy/systemd/).

HARD SAFETY INVARIANT: bu modülde API key/secret/HMAC/credential alanı
YOKTUR ve OLAMAZ (bkz. tests/test_repository_safety_scan.py — tüm
`crypto_signal_engine/` paketini tarar, bu dosya da otomatik kapsanır).

Fail-fast: geçersiz/eksik HERHANGİ bir alan, `load_config()` sırasında
AÇIK bir `AppConfigurationError` fırlatır — sessiz bir varsayılana
düşme veya kısmi bir config asla üretilmez.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig, RiskPolicyConfig
from crypto_signal_engine.execution.models import ExecutionMode
from crypto_signal_engine.ops.errors import AppConfigurationError
from crypto_signal_engine.providers.binance.config import (
    BINANCE_VALID_DEPTH_LIMITS,
    BinanceConfig,
    ReconnectPolicyConfig,
)
from crypto_signal_engine.selection.config import AutoSymbolSelectionConfig

_ENV_PREFIX = "CSE_"

_DEFAULT_DB_PATH = "var/lib/crypto-signal-engine/paper_state.db"
_DEFAULT_LOG_LEVEL = "INFO"
_VALID_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


def _env(env: Mapping[str, str], name: str, default: str | None) -> str | None:
    value = env.get(_ENV_PREFIX + name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _parse_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = _env(env, name, None)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise AppConfigurationError(f"{_ENV_PREFIX}{name} geçerli bir sayı değil: {raw!r}") from exc


def _parse_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _env(env, name, None)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise AppConfigurationError(f"{_ENV_PREFIX}{name} geçerli bir tamsayı değil: {raw!r}") from exc


def _parse_optional_int(env: Mapping[str, str], name: str) -> int | None:
    raw = _env(env, name, None)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise AppConfigurationError(f"{_ENV_PREFIX}{name} geçerli bir tamsayı değil: {raw!r}") from exc


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _parse_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    """Faz 12: `CSE_ENABLE_TESTNET_EXECUTION`/`CSE_DASHBOARD_ENABLED` gibi
    açık-onay bayrakları için. KASITLI OLARAK dar bir kabul kümesi
    (`_TRUE_VALUES`/`_FALSE_VALUES`) — anlaşılmaz bir değer SESSİZCE
    `False`'a düşmez, AÇIKÇA `AppConfigurationError` fırlatır (özellikle
    TESTNET execution gate'i için: bir typo'nun SESSİZCE "devre dışı"
    yerine "etkin" anlamına gelmesi asla İSTENMEZ, ama tersi de belirsiz
    bırakılmamalı — her iki yön de AÇIK olmalı)."""
    raw = _env(env, name, None)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise AppConfigurationError(
        f"{_ENV_PREFIX}{name} geçerli bir boolean değil: {raw!r}. "
        f"İzin verilenler: {sorted(_TRUE_VALUES | _FALSE_VALUES)}"
    )


@dataclass(frozen=True)
class AppConfig:
    """Faz 8 uygulamasının TÜM ortam-tabanlı konfigürasyonu.

    HARD SAFETY INVARIANT: bu sınıfın hiçbir alanı API key/secret/HMAC/
    credential TAŞIMAZ — yalnızca PUBLIC Binance erişimi + paper-trading
    parametreleri + operasyonel dosya yolları."""

    symbols: tuple[str, ...]
    db_path: Path
    # -- Otomatik sembol seçimi ("AUTOMATIC SYMBOL UNIVERSE / OPPORTUNITY
    # SELECTION") ------------------------------------------------------
    # `auto_select_symbols=True` <=> `CSE_SYMBOLS` HİÇ verilmedi (bkz.
    # `load_config`). Bu durumda `symbols` BOŞ (`()`) olarak üretilir ve
    # gerçek seçim `main()`'de, `AutomaticSymbolSelector` ile (network I/O
    # gerektirdiği için `load_config()`'in SAF/senkron sınırının DIŞINDA)
    # yapılır — `resolve_symbols()` sonra bu config'i seçilen sembollerle
    # `dataclasses.replace()` eder. `auto_select_symbols` alanı, seçim
    # sonrası da DEĞİŞTİRİLMEZ (observability: "bu run'ın sembolleri
    # otomatik mi geldi" bilgisi kalıcı kalır).
    auto_select_symbols: bool = False
    symbol_selection: AutoSymbolSelectionConfig = field(default_factory=AutoSymbolSelectionConfig)
    log_level: str = _DEFAULT_LOG_LEVEL
    stale_feed_threshold_seconds: float = 30.0
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    notional_per_position: float = 1000.0
    warmup_candles: int = 25
    order_book_depth: int = 20
    reconnect: ReconnectPolicyConfig = field(default_factory=ReconnectPolicyConfig)
    lock_path: Path | None = None
    health_snapshot_path: Path | None = None
    health_snapshot_interval_seconds: float = 5.0
    # -- Faz 12: execution mode + read-only dashboard -------------------
    # HARD SAFETY: bu alanlar ASLA bir credential TAŞIMAZ (bkz. modül
    # docstring'i). TESTNET imza kimlik bilgileri, KASITLI OLARAK bu
    # dataclass'ın DIŞINDA (`crypto_signal_engine/execution/factory.py`'nin
    # okuduğu, Faz 10/11 CLI'sıyla — `scripts/binance_testnet_lab.py` —
    # AYNI ortam değişkeni kalıbı) okunur — AppConfig'e ASLA eklenmez
    # (bkz. `tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`,
    # bu dosyanın execution/ SINIRI DIŞINDA olduğu için credential-literal
    # İÇEREMEYECEĞİNİ ZORUNLU kılar).
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    enable_testnet_execution: bool = False
    dashboard_enabled: bool = True
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8787
    # -- 24/7 Ops v1, Step 3: Control Center admin token ----------------
    # HARD SAFETY: this is a LOCAL admin token (gates pause/resume/stop
    # on the read-only dashboard's new, narrowly-scoped admin endpoints,
    # see `ops/admin.py`) — it is NOT a Binance/exchange credential, so
    # it is read the same way as every other `AppConfig` field (unlike
    # `BINANCE_TESTNET_API_KEY`/`_SECRET`, which are kept OUT of this
    # class — see `execution/factory.py`). It is still deliberately
    # EXCLUDED from `summary()` below (that dict is logged wholesale on
    # every startup, and a credential — even a local one — must never
    # appear in a log line; see `tests/test_repository_safety_scan.py`'s
    # new leak test for this milestone). `None` (default) = admin
    # actions fully disabled; the dashboard then behaves EXACTLY as it
    # did before this milestone (see DECISIONS.md Karar).
    dashboard_admin_token: str | None = None
    # -- Faz 13: Signal -> Binance Spot TESTNET execution bridge -------
    # HARD SAFETY: bu bayrak, `enable_testnet_execution` VE
    # `execution_mode == BINANCE_SPOT_TESTNET` ile BİRLİKTE (AND) ele
    # alınır (bkz. `app.py::Application.__init__`) — tek başına `true`
    # olması YETMEZ, otomatik order gönderimi için ÜÇÜNÜN DE doğru olması
    # ZORUNLUDUR. Varsayılan `False`dır (Faz 12'nin "paper-first safety"
    # ilkesiyle AYNI disiplin, bkz. `enable_testnet_execution`).
    enable_signal_testnet_bridge: bool = False
    testnet_bridge_notional_usdt: float = 10.0
    # -- Autonomous Testnet trading lifecycle (entry/exit/risk policy) --
    # Active whenever the bridge above is (same opt-in gate, no separate
    # flag — the lifecycle IS the bridge's exit/risk half). Defaults are
    # the conservative, explainable policy documented in
    # `execution/lifecycle.py::ExitPolicyConfig`/`RiskPolicyConfig`.
    lifecycle_exit_policy: ExitPolicyConfig = field(default_factory=ExitPolicyConfig)
    lifecycle_risk_policy: RiskPolicyConfig = field(default_factory=RiskPolicyConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.auto_select_symbols, bool):
            raise TypeError("auto_select_symbols bir bool olmalı")
        if not self.symbols and not self.auto_select_symbols:
            raise AppConfigurationError(
                f"{_ENV_PREFIX}SYMBOLS boş olamaz (VEYA hiç verilmeyip otomatik seçime bırakılmalı — "
                f"bkz. crypto_signal_engine.selection)"
            )
        if self.symbols:
            normalized = tuple(dict.fromkeys(normalize_symbol(s) for s in self.symbols))
            object.__setattr__(self, "symbols", normalized)
        if not isinstance(self.symbol_selection, AutoSymbolSelectionConfig):
            raise TypeError("symbol_selection bir AutoSymbolSelectionConfig olmalı")

        if self.log_level not in _VALID_LOG_LEVELS:
            raise AppConfigurationError(
                f"{_ENV_PREFIX}LOG_LEVEL geçersiz: {self.log_level!r}. İzin verilenler: {_VALID_LOG_LEVELS}"
            )

        for value, name in (
            (self.stale_feed_threshold_seconds, "STALE_FEED_THRESHOLD_SECONDS"),
            (self.notional_per_position, "NOTIONAL_PER_POSITION"),
            (self.health_snapshot_interval_seconds, "HEALTH_SNAPSHOT_INTERVAL_SECONDS"),
        ):
            if not (value > 0):
                raise AppConfigurationError(f"{_ENV_PREFIX}{name} pozitif olmalı, alınan: {value}")

        for value, name in ((self.fee_bps, "FEE_BPS"), (self.slippage_bps, "SLIPPAGE_BPS")):
            if value < 0:
                raise AppConfigurationError(f"{_ENV_PREFIX}{name} negatif olamaz, alınan: {value}")

        if self.warmup_candles < 20:
            raise AppConfigurationError(
                f"{_ENV_PREFIX}WARMUP_CANDLES en az 20 olmalı (Faz 3/4 min_history gereksinimi), "
                f"alınan: {self.warmup_candles}"
            )

        if self.order_book_depth not in BINANCE_VALID_DEPTH_LIMITS:
            raise AppConfigurationError(
                f"{_ENV_PREFIX}ORDER_BOOK_DEPTH Binance'in izin verdiği limitlerden biri olmalı: "
                f"{BINANCE_VALID_DEPTH_LIMITS}, alınan: {self.order_book_depth}"
            )

        if not isinstance(self.reconnect, ReconnectPolicyConfig):
            raise TypeError("reconnect bir ReconnectPolicyConfig olmalı")

        object.__setattr__(self, "db_path", Path(self.db_path))
        if str(self.db_path) == "":
            raise AppConfigurationError(f"{_ENV_PREFIX}DB_PATH boş olamaz")

        object.__setattr__(
            self, "lock_path", Path(self.lock_path) if self.lock_path is not None
            else self.db_path.parent / "crypto-signal-engine.lock"
        )
        object.__setattr__(
            self, "health_snapshot_path", Path(self.health_snapshot_path) if self.health_snapshot_path is not None
            else self.db_path.parent / "health.json"
        )

        if not isinstance(self.execution_mode, ExecutionMode):
            raise TypeError("execution_mode bir ExecutionMode olmalı")
        if not isinstance(self.enable_testnet_execution, bool):
            raise TypeError("enable_testnet_execution bir bool olmalı")
        if not isinstance(self.dashboard_enabled, bool):
            raise TypeError("dashboard_enabled bir bool olmalı")
        if not self.dashboard_host.strip():
            raise AppConfigurationError(f"{_ENV_PREFIX}DASHBOARD_HOST boş olamaz")
        if not (1 <= self.dashboard_port <= 65535):
            raise AppConfigurationError(
                f"{_ENV_PREFIX}DASHBOARD_PORT [1, 65535] aralığında olmalı, alınan: {self.dashboard_port}"
            )
        if self.dashboard_admin_token is not None and not isinstance(self.dashboard_admin_token, str):
            raise TypeError("dashboard_admin_token bir str veya None olmalı")

        if not isinstance(self.enable_signal_testnet_bridge, bool):
            raise TypeError("enable_signal_testnet_bridge bir bool olmalı")
        if not (self.testnet_bridge_notional_usdt > 0):
            raise AppConfigurationError(
                f"{_ENV_PREFIX}TESTNET_BRIDGE_NOTIONAL_USDT pozitif olmalı, alınan: "
                f"{self.testnet_bridge_notional_usdt}"
            )
        if not isinstance(self.lifecycle_exit_policy, ExitPolicyConfig):
            raise TypeError("lifecycle_exit_policy bir ExitPolicyConfig olmalı")
        if not isinstance(self.lifecycle_risk_policy, RiskPolicyConfig):
            raise TypeError("lifecycle_risk_policy bir RiskPolicyConfig olmalı")

    def binance_config(self) -> BinanceConfig:
        """Faz 2 `BinanceConfig`'ini bu uygulama config'inden türetir.

        DİKKAT: `rest_base_url`/`ws_base_url` KASITLI OLARAK burada
        override EDİLEMEZ (env değişkeni yoktur) — bu, konfigürasyon
        yüzeyinin yanlışlıkla Testnet/başka bir execution endpoint'ine
        işaret edecek şekilde kötüye kullanılmasını yapısal olarak
        engeller (defense-in-depth, bkz. PHASE8_UBUNTU_OPERATIONS.md)."""
        return BinanceConfig(
            supported_symbols=self.symbols,
            order_book_depth=self.order_book_depth,
            stale_feed_threshold_seconds=self.stale_feed_threshold_seconds,
            reconnect=self.reconnect,
        )

    def summary(self) -> dict[str, object]:
        """Loglama için GÜVENLİ (secret İÇERMEYEN) bir özet — zaten hiçbir
        alan secret olmadığından tüm alanlar dahil edilebilir."""
        return {
            "symbols": list(self.symbols),
            "auto_select_symbols": self.auto_select_symbols,
            "symbol_selection": {
                "target_count": self.symbol_selection.target_count,
                "shortlist_size": self.symbol_selection.shortlist_size,
                "min_quote_volume_24h": self.symbol_selection.min_quote_volume_24h,
                "lookback_candles": self.symbol_selection.lookback_candles,
                "recent_window_candles": self.symbol_selection.recent_window_candles,
                "rescan_interval_seconds": self.symbol_selection.rescan_interval_seconds,
                "min_atr_pct": self.symbol_selection.min_atr_pct,
                "min_current_move_pct": self.symbol_selection.min_current_move_pct,
            },
            "db_path": str(self.db_path),
            "log_level": self.log_level,
            "stale_feed_threshold_seconds": self.stale_feed_threshold_seconds,
            "fee_bps": self.fee_bps,
            "slippage_bps": self.slippage_bps,
            "notional_per_position": self.notional_per_position,
            "warmup_candles": self.warmup_candles,
            "order_book_depth": self.order_book_depth,
            "lock_path": str(self.lock_path),
            "health_snapshot_path": str(self.health_snapshot_path),
            "health_snapshot_interval_seconds": self.health_snapshot_interval_seconds,
            "execution_mode": self.execution_mode.value,
            "enable_testnet_execution": self.enable_testnet_execution,
            "dashboard_enabled": self.dashboard_enabled,
            "dashboard_host": self.dashboard_host,
            "dashboard_port": self.dashboard_port,
            "enable_signal_testnet_bridge": self.enable_signal_testnet_bridge,
            "testnet_bridge_notional_usdt": self.testnet_bridge_notional_usdt,
            "lifecycle_exit_policy": {
                "stop_atr_multiple": self.lifecycle_exit_policy.stop_atr_multiple,
                "take_profit_atr_multiple": self.lifecycle_exit_policy.take_profit_atr_multiple,
                "trailing_activation_atr_multiple": self.lifecycle_exit_policy.trailing_activation_atr_multiple,
                "trailing_distance_atr_multiple": self.lifecycle_exit_policy.trailing_distance_atr_multiple,
                "max_hold_hours": self.lifecycle_exit_policy.max_hold_hours,
            },
            "lifecycle_risk_policy": {
                "max_open_positions": self.lifecycle_risk_policy.max_open_positions,
                "max_total_exposure_usdt": self.lifecycle_risk_policy.max_total_exposure_usdt,
                "cooldown_minutes": self.lifecycle_risk_policy.cooldown_minutes,
                "daily_loss_limit_usdt": self.lifecycle_risk_policy.daily_loss_limit_usdt,
                "unknown_fee_conservative_reserve_usdt": self.lifecycle_risk_policy.unknown_fee_conservative_reserve_usdt,
                "sell_commission_headroom_bps": self.lifecycle_risk_policy.sell_commission_headroom_bps,
            },
        }


def load_config(env: Mapping[str, str]) -> AppConfig:
    """`env` (üretimde `os.environ`) içinden bir `AppConfig` üretir.

    Herhangi bir alan geçersizse (eksik zorunlu alan, parse edilemeyen
    sayı, aralık dışı değer) `AppConfigurationError` fırlatır — kısmi/
    varsayılan bir config'e SESSİZCE düşülmez."""
    symbols_raw = _env(env, "SYMBOLS", None)
    if symbols_raw is None:
        # Bölüm "AUTOMATIC SYMBOL UNIVERSE / OPPORTUNITY SELECTION": artık
        # ZORUNLU DEĞİL — verilmezse otomatik seçim modu aktive edilir
        # (gerçek seçim burada DEĞİL, `app.py::resolve_symbols()`'da olur;
        # bu fonksiyon SAF/senkron kalır, network I/O YAPMAZ).
        symbols: tuple[str, ...] = ()
        auto_select_symbols = True
    else:
        symbols = tuple(s.strip() for s in symbols_raw.split(",") if s.strip())
        auto_select_symbols = False

    db_path_raw = _env(env, "DB_PATH", _DEFAULT_DB_PATH)

    max_attempts = _parse_optional_int(env, "RECONNECT_MAX_ATTEMPTS")
    reconnect = ReconnectPolicyConfig(
        initial_delay_seconds=_parse_float(env, "RECONNECT_INITIAL_DELAY_SECONDS", 1.0),
        max_delay_seconds=_parse_float(env, "RECONNECT_MAX_DELAY_SECONDS", 60.0),
        multiplier=_parse_float(env, "RECONNECT_MULTIPLIER", 2.0),
        jitter_seconds=_parse_float(env, "RECONNECT_JITTER_SECONDS", 0.5),
        max_attempts=max_attempts,
    )

    lock_path_raw = _env(env, "LOCK_PATH", None)
    health_path_raw = _env(env, "HEALTH_SNAPSHOT_PATH", None)

    execution_mode_raw = _env(env, "EXECUTION_MODE", ExecutionMode.PAPER.value)
    try:
        execution_mode = ExecutionMode(execution_mode_raw)
    except ValueError as exc:
        raise AppConfigurationError(
            f"{_ENV_PREFIX}EXECUTION_MODE geçersiz: {execution_mode_raw!r}. İzin verilenler: "
            f"{[m.value for m in ExecutionMode]} (MAINNET bir seçenek DEĞİLDİR ve OLAMAZ)"
        ) from exc

    try:
        return AppConfig(
            symbols=symbols,
            auto_select_symbols=auto_select_symbols,
            symbol_selection=AutoSymbolSelectionConfig(
                target_count=_parse_int(env, "AUTO_SYMBOL_COUNT", 5),
                shortlist_size=_parse_int(env, "AUTO_SYMBOL_SHORTLIST_SIZE", 30),
                min_quote_volume_24h=_parse_float(env, "AUTO_SYMBOL_MIN_QUOTE_VOLUME", 5_000_000.0),
                lookback_candles=_parse_int(env, "AUTO_SYMBOL_LOOKBACK_CANDLES", 96),
                recent_window_candles=_parse_int(env, "AUTO_SYMBOL_RECENT_WINDOW_CANDLES", 12),
                rescan_interval_seconds=_parse_float(env, "AUTO_SYMBOL_RESCAN_INTERVAL_SECONDS", 3600.0),
                min_atr_pct=_parse_float(env, "AUTO_SYMBOL_MIN_ATR_PCT", 0.0005),
                min_current_move_pct=_parse_float(env, "AUTO_SYMBOL_MIN_CURRENT_MOVE_PCT", 0.05),
            ),
            db_path=Path(db_path_raw),
            log_level=_env(env, "LOG_LEVEL", _DEFAULT_LOG_LEVEL).upper(),
            stale_feed_threshold_seconds=_parse_float(env, "STALE_FEED_THRESHOLD_SECONDS", 30.0),
            fee_bps=_parse_float(env, "FEE_BPS", 0.0),
            slippage_bps=_parse_float(env, "SLIPPAGE_BPS", 0.0),
            notional_per_position=_parse_float(env, "NOTIONAL_PER_POSITION", 1000.0),
            warmup_candles=_parse_int(env, "WARMUP_CANDLES", 25),
            order_book_depth=_parse_int(env, "ORDER_BOOK_DEPTH", 20),
            reconnect=reconnect,
            lock_path=Path(lock_path_raw) if lock_path_raw is not None else None,
            health_snapshot_path=Path(health_path_raw) if health_path_raw is not None else None,
            health_snapshot_interval_seconds=_parse_float(env, "HEALTH_SNAPSHOT_INTERVAL_SECONDS", 5.0),
            execution_mode=execution_mode,
            enable_testnet_execution=_parse_bool(env, "ENABLE_TESTNET_EXECUTION", False),
            dashboard_enabled=_parse_bool(env, "DASHBOARD_ENABLED", True),
            dashboard_host=_env(env, "DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=_parse_int(env, "DASHBOARD_PORT", 8787),
            dashboard_admin_token=_env(env, "DASHBOARD_ADMIN_TOKEN", None),
            enable_signal_testnet_bridge=_parse_bool(env, "ENABLE_SIGNAL_TESTNET_BRIDGE", False),
            testnet_bridge_notional_usdt=_parse_float(env, "TESTNET_BRIDGE_NOTIONAL_USDT", 10.0),
            lifecycle_exit_policy=ExitPolicyConfig(
                stop_atr_multiple=_parse_float(env, "LIFECYCLE_STOP_ATR_MULTIPLE", 2.0),
                take_profit_atr_multiple=_parse_float(env, "LIFECYCLE_TAKE_PROFIT_ATR_MULTIPLE", 4.0),
                trailing_activation_atr_multiple=_parse_float(env, "LIFECYCLE_TRAILING_ACTIVATION_ATR_MULTIPLE", 2.0),
                trailing_distance_atr_multiple=_parse_float(env, "LIFECYCLE_TRAILING_DISTANCE_ATR_MULTIPLE", 2.0),
                max_hold_hours=_parse_float(env, "LIFECYCLE_MAX_HOLD_HOURS", 48.0),
            ),
            lifecycle_risk_policy=RiskPolicyConfig(
                max_open_positions=_parse_int(env, "LIFECYCLE_MAX_OPEN_POSITIONS", 8),
                max_total_exposure_usdt=_parse_float(env, "LIFECYCLE_MAX_TOTAL_EXPOSURE_USDT", 100.0),
                cooldown_minutes=_parse_float(env, "LIFECYCLE_COOLDOWN_MINUTES", 30.0),
                daily_loss_limit_usdt=_parse_float(env, "LIFECYCLE_DAILY_LOSS_LIMIT_USDT", 50.0),
                unknown_fee_conservative_reserve_usdt=_parse_float(env, "LIFECYCLE_UNKNOWN_FEE_RESERVE_USDT", 0.05),
                sell_commission_headroom_bps=_parse_float(env, "LIFECYCLE_SELL_COMMISSION_HEADROOM_BPS", 15.0),
            ),
        )
    except AppConfigurationError:
        raise
    except (ValueError, TypeError) as exc:
        raise AppConfigurationError(str(exc)) from exc


=== FILE: crypto_signal_engine/ops/dashboard.py ===
"""
Faz 12 / Görsel yeniden tasarım — READ-ONLY, salt-gözlemsel yerel operasyon
dashboard'u.

Tasarım ilkesi (Bölüm 6 — "dashboard failure must NEVER kill... Use
snapshots/read-only state"): bu modül CANLI Python nesnelerine (paper
engine, coordinator, execution store) ASLA doğrudan ERİŞMEZ. TEK veri
kaynağı, `ops/health_snapshot.py::write_snapshot()`'ın ZATEN periyodik
olarak ATOMİK yazdığı JSON dosyasıdır (`os.replace` ile). Bu, iki şeyi
YAPISAL OLARAK garanti eder:

1. Dashboard HTTP handler'ları (ayrı OS thread'lerinde, `ThreadingHTTPServer`
   ile) hiçbir zaman runtime'ın asyncio event loop'una veya SQLite
   bağlantılarına eş-zamanlı ERİŞMEZ — cross-thread race/lock riski
   YAPISAL OLARAK YOKTUR.
2. Dashboard, ÖNCEDEN (Faz 12'de) tanım gereği MUTASYON YAPAMIYORDU. 24/7
   Ops v1 (Step 3, Control Center) bu kuralı TEK, dar kapsamlı bir
   istisnayla genişletir: `dashboard_admin_token` AÇIKÇA ayarlandığında,
   `POST /admin/{pause,resume,stop}` üç endpoint'i mevcut olur (bkz.
   `ops/admin.py::AdminController`) — HİÇBİRİ order-submission/config-
   mutation İÇERMEZ, üçü de zaten var olan, thread-safe primitiflere
   (bir `threading.Event`, `Application.request_shutdown()`'ın
   `call_soon_threadsafe` ile zamanlanması) delege eder. Token
   AYARLANMADIYSA (varsayılan, `None`) bu üç yol da mevcut DEĞİLDİR —
   dashboard davranışı bu milestone'dan ÖNCEKİYLE bit-for-bit AYNIDIR.
   Bunların DIŞINDA hiçbir POST/PUT/DELETE endpoint'i, hiçbir order-
   submission/config-mutation kod yolu YOKTUR.

GÖRSEL YENİDEN TASARIM (bu fazın konusu): `GET /` artık kullanıcı-onaylı
`ops/dashboard_design_reference.html` mockup'ının BİREBİR aynı CSS'i ve
JS iskeletiyle (nav, ticker, mum grafiği, KPI kartları, tablolar, Control
Center) render edilir — tasarım YENİDEN YORUMLANMAZ. Fark: mockup'ın
`<script>` başındaki SABİT/UYDURMA diziler (`open`/`dust`/`trades`/
`genCandles()`) tamamen KALDIRILMIŞTIR; bunların yerine bu modülün
`_build_view_model()`'i, `/api/status`'ın ZATEN ürettiği GERÇEK veriden
(bkz. `app.py::_bridge_lifecycle_snapshot`/`_symbol_selection_snapshot`/
`_candle_history_snapshot`) inşa edilmiş TEK bir JSON nesnesini
(`window.__DASHBOARD_DATA__`) sayfaya gömer; JS bunu tüketir. UI Polish
v1'den beri "Son yedekleme" (`scripts/backup_sqlite.py`'nin additive
`backup_status.json`'ı) ve "Olay Geçmişi" (`ops/event_log.py`'nin
`ops_event_log` tablosu) ARTIK GERÇEKTİR — bkz. Bölüm A7/A9. Hâlâ gerçek
bir karşılığı OLMAYAN mockup öğeleri (Bu Hafta, Binance/Veritabanı
servis kutuları) mockup'taki "Faz 2" rozet deseniyle AYNEN dürüst bir
yer tutucu olarak KALIR — asla yeşil/başarılı bir durum UYDURULMAZ. Mum
grafiği GERÇEK M5/M15/H1 geçmişini kullanır (bkz.
`_candle_history_snapshot`, `RuntimeCoordinator._candle_windows`'ın
ZATEN var olan bounded in-memory penceresinden, YENİ bir persistence
KATMANI OLMADAN) — bu geçmiş bir sembol için henüz mevcut DEĞİLSE,
grafik panelinde AÇIKÇA "örnek veri" rozeti gösterilir, ASLA gerçek gibi
sunulmaz.

Control Center'daki Duraklat/Devam Et/Acil Durdur butonları 24/7 Ops v1'
den (Karar 99) beri GERÇEKTİR ve token-gated'dir (bkz. `ops/admin.py::
AdminController`, `POST /admin/{pause,resume,stop}`) — `dashboard_admin_
token` ayarlıysa bu üç kontrol GERÇEKTEN çalışır (Duraklat/Devam Et
bağımsız bir `entries_paused` kapısını açar/kapatır, açık pozisyon
yönetimini ETKİLEMEZ; Acil Durdur ZATEN var olan `request_shutdown()`'ı
çağırır). YALNIZCA "Yeniden Başlat" KASITLI OLARAK bağlı DEĞİLDİR —
sunucuda `systemctl` erişimi gerektirir ve güvenilir bir uzaktan yeniden
başlatma mekanizması İNŞA ETMEK Karar 99'da AÇIKÇA kapsam DIŞI
bırakılmıştır (bkz. DECISIONS.md Karar 99 — bu sınır bu milestone'da da
DEĞİŞTİRİLMEDİ); tıklandığında dürüst bir "kasıtlı olarak bağlanmadı"
toast'ı gösterir.

`GET /api/status` (makine-okur JSON) DEĞİŞMEDEN kalır — TÜM eski alan
adları korunur, yalnızca EKLENEN (additive) `candle_history` alanı
vardır."""

from __future__ import annotations

import contextlib
import html
import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from crypto_signal_engine.ops.admin import ADMIN_TOKEN_HEADER, AdminController
from crypto_signal_engine.ops.health_snapshot import read_snapshot

_LOGGER = logging.getLogger("crypto_signal_engine.ops.dashboard")

_INDEX_PATH = "/"
_STATUS_PATH = "/api/status"
_HEALTHZ_PATH = "/healthz"
# 24/7 Ops v1, Step 3 — Control Center admin endpoints. Only reachable
# when `DashboardServer` was given an `AdminController` (i.e.
# `dashboard_admin_token` is set) — otherwise these fall through to the
# existing, unchanged `_reject_mutation()` 405 path (see `do_POST`).
_ADMIN_PAUSE_PATH = "/admin/pause"
_ADMIN_RESUME_PATH = "/admin/resume"
_ADMIN_STOP_PATH = "/admin/stop"


def _snapshot_or_none(path: Path) -> dict[str, object] | None:
    try:
        return read_snapshot(path)
    except (OSError, json.JSONDecodeError) as exc:
        # Bir yarış anında (health-loop TAM o anda dosyayı yeniden
        # yazıyorken) teorik olarak ortaya çıkabilecek bir okuma
        # hatası bile dashboard'u ÇÖKERTMEMELİDİR — `write_snapshot`'ın
        # `os.replace` atomikliği bunu pratikte İMKANSIZ kılar, ama
        # savunma amaçlı burada da fail-safe davranılır.
        _LOGGER.warning("health snapshot okunamadı (geçici olabilir): %s", exc)
        return None


# ============================================================================
# TÜRKÇE ÇEVİRİ SÖZLÜKLERİ — sadece GÖRÜNTÜ çevirisi, saklanan enum/state
# değerlerini DEĞİŞTİRMEZ.
# ============================================================================

_STATE_TR: dict[str, str] = {
    "FLAT": "Kapalı",
    "LONG": "Açık",
    "DUST": "Satılamayacak kadar küçük kalan",
    "ENTRY_PENDING": "Alış emri sonuç bekliyor",
    "EXIT_PENDING": "Satış emri sonuç bekliyor",
    "AMBIGUOUS": "Binance sonucu kontrol ediliyor",
    "RECOVERED": "Geri yüklendi / aktifleşme bekliyor",
}

_EXIT_REASON_TR: dict[str, str] = {
    "STOP_LOSS": "Güvenlik seviyesi",
    "TAKE_PROFIT": "Kâr hedefi",
    "TRAILING_STOP": "Takip eden stop",
    "MAX_HOLD": "Maksimum bekleme süresi",
    "OPPOSITE_SIGNAL": "Ters sinyal",
}

_TIMEFRAME_TR: dict[str, str] = {"1m": "1 dakikalık", "5m": "5 dakikalık", "15m": "15 dakikalık", "1h": "1 saatlik"}


def _translate(mapping: dict[str, str], value: object) -> str | None:
    """Bilinen bir teknik değeri Türkçeye çevirir; BİLİNMEYEN bir değer
    için ASLA bir yorum UYDURMAZ — ham değeri olduğu gibi döner. `None`
    girdi için `None` döner (JS tarafında "—" olarak gösterilir)."""
    if value is None:
        return None
    text = str(value)
    return mapping.get(text, text)


def _translate_symbol_detail(detail: object) -> str:
    """`HealthMonitor.status_for()`'ın (bkz. `runtime/health.py`) ürettiği
    TAM, sonlu teknik `detail` dizgisi kümesini normal Türkçeye çevirir.
    Bilinmeyen/yeni bir dizgi için ASLA bir yorum UYDURULMAZ — ham metin
    güvenle olduğu gibi döner."""
    text = str(detail)
    if text == "healthy":
        return "Sorun yok"
    if text == "bootstrap in progress":
        return "Başlangıç verileri hazırlanıyor"
    if text == "runtime stopped":
        return "Sistem durduruldu"
    if text == "no event observed yet":
        return "Henüz piyasa verisi alınmadı"
    if text == "stream disconnected":
        return "Piyasa veri bağlantısı koptu, yeniden bağlanmaya çalışılıyor"
    if text.startswith("unresolved gap:"):
        raw_list = text.split(":", 1)[1].strip()
        parts = [p.strip() for p in raw_list.split(",") if p.strip()]
        tr_parts = ", ".join(_TIMEFRAME_TR.get(p, p) for p in parts)
        return f"{tr_parts} piyasa verisinde eksik veri algılandı"
    if text.startswith("stale:"):
        return "Piyasa verisi bir süredir güncellenmiyor"
    if text.startswith("durable checkpoint write failed"):
        return "Sistem durumu diske kaydedilirken geçici bir sorun oluştu"
    return text


def _degraded_symbols(symbols: list[dict]) -> list[dict[str, object]]:
    """DEGRADED durumdaki semboller için GERÇEK, ham sağlık verisinden
    türetilmiş bir liste — uyarı bandosu VE Control Center'ın "Bakım ve
    Uyarılar" listesi TARAFINDAN paylaşılır (tek kaynak, iki görüntü)."""
    affected = [s for s in symbols if str(s.get("health")) == "DEGRADED"]
    return [
        {
            "symbol": str(s.get("symbol", "?")),
            "detail_tr": _translate_symbol_detail(s.get("detail", "")),
            "last_event_at": s.get("last_event_at"),
        }
        for s in affected
    ]


# ============================================================================
# GÖRÜNÜM MODELİ (view model) — /api/status snapshot'ından, JS'in
# tüketeceği TEK bir gerçek-veri JSON nesnesi inşa eder. Sayılar HAM
# (float/int/None) bırakılır — Türkçe sayı biçimlendirmesi (1.240,50)
# referans dosyanın kendi `fmt()` yardımcısıyla JS tarafında yapılır.
# ============================================================================


def _build_view_model(snapshot: dict[str, object]) -> dict[str, object]:
    overall_health = str(snapshot.get("overall_health", ""))
    execution = snapshot.get("execution", {}) or {}
    bridge_lifecycle = snapshot.get("bridge_lifecycle", {}) or {}
    symbol_selection = snapshot.get("symbol_selection", {}) or {}
    candle_history = snapshot.get("candle_history", {}) or {}
    raw_symbols = list(snapshot.get("symbols", []) or [])

    bridge_enabled = bool(bridge_lifecycle.get("enabled"))
    positions_raw = bridge_lifecycle.get("positions", {}) or {} if bridge_enabled else {}
    performance = (bridge_lifecycle.get("performance", {}) or {}) if bridge_enabled else {}
    recent_trades_raw = (bridge_lifecycle.get("recent_trades", []) or []) if bridge_enabled else []

    positions: list[dict[str, object]] = []
    dust: list[dict[str, object]] = []
    for symbol, p in positions_raw.items():
        if not isinstance(p, dict) or p.get("state") == "FLAT":
            continue
        gross_entry_vwap = p.get("gross_entry_vwap")
        net_qty = p.get("net_owned_base_quantity")
        position_value = (
            float(gross_entry_vwap) * float(net_qty) if gross_entry_vwap is not None and net_qty is not None else None
        )
        entry = {
            "symbol": symbol,
            "state": p.get("state"),
            "state_tr": _translate(_STATE_TR, p.get("state")),
            "gross_entry_vwap": gross_entry_vwap,
            "latest_price": p.get("latest_price"),
            # Dashboard price/P&L sync fix — additive freshness metadata
            # for `latest_price` (`"M1"`/`"M5"`/`None`, ISO timestamp or
            # `None`), see `app.py::Application._latest_price_source()`.
            "latest_price_source": p.get("latest_price_source"),
            "latest_price_as_of": p.get("latest_price_as_of"),
            "net_owned_base_quantity": net_qty,
            "position_value": position_value,
            "effective_stop": p.get("effective_stop"),
            "take_profit": p.get("take_profit"),
            "trailing_active": p.get("trailing_active"),
            "unrealized_gross_pnl": p.get("unrealized_gross_pnl"),
            "cumulative_realized_gross_pnl": p.get("cumulative_realized_gross_pnl"),
            "last_exit_reason": p.get("last_exit_reason"),
            "last_exit_reason_tr": _translate(_EXIT_REASON_TR, p.get("last_exit_reason")),
            "high_water": p.get("high_water"),
            "cooldown_until": p.get("cooldown_until"),
        }
        if p.get("state") == "DUST":
            dust.append(entry)
        else:
            positions.append(entry)

    trades = []
    for t in recent_trades_raw[:20]:
        gross = t.get("gross_realized_pnl")
        net = t.get("net_realized_pnl")
        trades.append({
            "recorded_at": t.get("recorded_at"),
            "symbol": t.get("symbol"),
            "quantity_closed": t.get("quantity_closed"),
            "gross_entry_vwap": t.get("gross_entry_vwap"),
            "exit_gross_vwap": t.get("exit_gross_vwap"),
            "gross_realized_pnl": gross,
            "net_realized_pnl": net,
            "commission_usdt": (float(gross) - float(net)) if gross is not None and net is not None else None,
            "exit_reason": t.get("exit_reason"),
            "exit_reason_tr": _translate(_EXIT_REASON_TR, t.get("exit_reason")),
        })

    completed_count = int(performance.get("completed_trade_count", 0) or 0)
    kpi = {
        "unrealized_total": performance.get("unrealized_gross_pnl_total"),
        "open_count": performance.get("non_flat_position_count", 0),
        "todays_realized": performance.get("todays_realized_gross_pnl"),
        "realized_total": performance.get("realized_gross_pnl"),
        "completed_trade_count": completed_count,
        "win_count": performance.get("win_count", 0),
        "loss_count": performance.get("loss_count", 0),
        "win_rate": performance.get("win_rate"),
        "average_win_gross": performance.get("average_win_gross"),
        "average_loss_gross": performance.get("average_loss_gross"),
        "exposure_usdt": performance.get("exposure_usdt"),
    } if bridge_enabled else None

    mode = symbol_selection.get("mode", "MANUAL")
    reselection = symbol_selection.get("reselection_scheduler", {}) or {}
    # UI Polish v1, Step A4 — Adaptive Symbol Intelligence (Karar 96):
    # `learned_factor_score` per symbol, already cheaply present on the
    # SAME `candidates` list `app.py::_symbol_selection_snapshot()`
    # already builds from the in-memory `SelectionResult` — no new
    # persistence/plumbing. `{}` when MANUAL mode (no candidates at all).
    candidates_raw = symbol_selection.get("candidates", []) or []
    learned_factors = {
        str(c.get("symbol")): c.get("learned_factor_score")
        for c in candidates_raw if isinstance(c, dict) and c.get("symbol") is not None
    }
    scan = {
        "mode": mode,
        "scanned_count": symbol_selection.get("shortlist_size"),
        "universe_size": symbol_selection.get("universe_size"),
        "new_opportunities": symbol_selection.get("opportunity_symbols", []) or [],
        "protected_symbols": symbol_selection.get("pinned_open_position_symbols", []) or [],
        "tracked_symbols": symbol_selection.get("final_runtime_symbols", []) or [],
        "armed": bool(reselection.get("enabled")) and bool(reselection.get("armed")),
        "reselection_enabled": bool(reselection.get("enabled")),
        "last_scan_at": reselection.get("last_rescan_at"),
        "next_scan_at": reselection.get("next_rescan_at"),
        "interval_seconds": reselection.get("rescan_interval_seconds"),
        "learned_factors": learned_factors,
    }

    degraded = _degraded_symbols(raw_symbols)
    admin = snapshot.get("admin", {}) or {"enabled": False}
    # UI Polish v1, Step A2 — already computed on EVERY snapshot since
    # Karar 98 (`_bridge_lifecycle_snapshot()`), just never read here
    # until now. `None` whenever unavailable (bridge disabled, lookup
    # failed) — NEVER a fabricated number, same discipline as every
    # other "Hesaplanamıyor" value in this view model.
    usdt_balance = bridge_lifecycle.get("usdt_balance") if bridge_enabled else None
    # Step A3 — Adaptive Intelligence, and Step A7 — backup status, and
    # Step A9 — event log: all additive/optional, all honestly degrade
    # to their own explicit "not active"/"unavailable"/"empty" shape.
    adaptive = snapshot.get("adaptive", {}) or {"active": False}
    backup = snapshot.get("backup", {}) or {"available": False}
    event_log = snapshot.get("event_log", {}) or {"events": []}

    return {
        "generated_at": snapshot.get("generated_at"),
        "overall_health": overall_health,
        "bot_running": overall_health not in ("STOPPED", ""),
        "testnet_enabled": bool(execution.get("enabled")),
        "testnet_ready": bool(execution.get("ready")),
        "recovery_ok": bool((snapshot.get("recovery") or {}).get("ok")),
        "bridge_enabled": bridge_enabled,
        "degraded_symbols": degraded,
        "kpi": kpi,
        "positions": positions,
        "dust": dust,
        "recent_trades": trades,
        "scan": scan,
        "candles": candle_history,
        "pid": snapshot.get("pid"),
        "started_at": snapshot.get("started_at"),
        "admin": admin,
        "usdt_balance": usdt_balance,
        "adaptive": adaptive,
        "backup": backup,
        "events": event_log.get("events", []) or [],
    }


def _embed_json(data: dict[str, object]) -> str:
    """`window.__DASHBOARD_DATA__` için güvenli JSON gömme — `</script>`
    dizisini kaçışlayarak (bkz. OWASP "JSON in HTML" tavsiyesi) erken
    script-kapanışı/enjeksiyonu ÖNLER. Bu, salt-okunur/yerel bir araç
    olsa da mevcut `html.escape` disiplinine (defense in depth) uygundur."""
    raw = json.dumps(data, ensure_ascii=False, default=str)
    return raw.replace("</", "<\\/")


def render_dashboard_html(snapshot: dict[str, object] | None, *, health_snapshot_path: Path) -> str:
    """Kullanıcı-onaylı `dashboard_design_reference.html` mockup'ının
    CSS'i ve JS iskeletiyle, GERÇEK `/api/status` verisinden inşa edilen
    tek bir `window.__DASHBOARD_DATA__` JSON nesnesini gömerek render
    eder (bkz. modül docstring'i). Hâlâ script'siz bir SUNUCU render'ı
    DEĞİLDİR — mockup'ın kendisi bir istemci-taraflı JS uygulamasıdır;
    bu fonksiyonun tek işi, o JS'in tükettiği veriyi salt-okunur olarak
    sağlamaktır (hiçbir eylem/mutasyon endpoint'i eklenmez)."""
    if snapshot is None:
        view_model: dict[str, object] = {
            "generated_at": None, "overall_health": "", "bot_running": False, "testnet_enabled": False,
            "testnet_ready": False, "recovery_ok": False, "bridge_enabled": False, "degraded_symbols": [],
            "kpi": None, "positions": [], "dust": [], "recent_trades": [], "scan": None, "candles": {},
            "pid": None, "started_at": None, "no_snapshot_yet": True,
            "health_snapshot_path": str(health_snapshot_path),
            "admin": {"enabled": False},
            "usdt_balance": None,
            "adaptive": {"active": False},
            "backup": {"available": False},
            "events": [],
        }
    else:
        view_model = _build_view_model(snapshot)
        view_model["no_snapshot_yet"] = False

    # UI Polish v1, Step A1 — proper document structure (doctype/html
    # lang/head/body). No visual/CSS change: `_PAGE_HEAD`/`_PAGE_STYLE`
    # go inside `<head>`, `_PAGE_BODY` + the embedded data script +
    # `_PAGE_SCRIPT` go inside `<body>`, exactly where they already
    # rendered before this wrapper existed.
    return (
        "<!doctype html>\n"
        '<html lang="tr">\n'
        "<head>\n" + _PAGE_HEAD + _PAGE_STYLE + "</head>\n"
        "<body>\n" + _PAGE_BODY
        + f'<script>window.__DASHBOARD_DATA__ = {_embed_json(view_model)};</script>'
        + _PAGE_SCRIPT + "\n</body>\n</html>\n"
    )


_PAGE_HEAD = """<title>Sinyal Botu Terminali</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta charset="utf-8">
"""

# Bölüm CSS: kullanıcı-onaylı `ops/dashboard_design_reference.html`'den
# BİREBİR (renk, font, layout DEĞİŞTİRİLMEDEN) kopyalanmıştır.
_PAGE_STYLE = """<style>
  :root{
    --bg:#0A0B10; --bg-2:#0C0E15;
    --surface:#12141C; --surface-2:#171A24; --surface-3:#1D212C;
    --border:#242835; --border-strong:#343A4A;
    --ink:#F1EFEA; --ink-2:#9CA3B8; --ink-3:#666D82;
    --accent:#F2994A; --accent-2:#4C8DFF; --accent-ink:#1A1108;
    --accent-rgb:242,153,74; --accent-2-rgb:76,141,255;
    --accent-soft:#2A1D10; --accent-soft-ink:#FFC98A;
    --success:#22C55E; --success-soft:#132A1C; --success-soft-ink:#5EE895;
    --danger:#FF5A5F; --danger-soft:#301719; --danger-soft-ink:#FF9195;
    --warning:#F2C94C; --warning-soft:#2E2711; --warning-soft-ink:#FFDD79;
    --neutral-soft:#1C2029; --neutral-soft-ink:#9CA3B8;
    --shadow: 0 1px 0 rgba(255,255,255,.04) inset, 0 0 0 1px rgba(var(--accent-rgb),.05), 0 18px 40px -18px rgba(0,0,0,.65);
    --glow-accent: 0 0 0 1px rgba(var(--accent-rgb),.22), 0 10px 30px -8px rgba(var(--accent-rgb),.32);
    --radius-card: 18px; --radius-control: 10px; --radius-pill: 999px;
    color-scheme: dark;
  }
  *{ box-sizing:border-box; }
  body{
    margin:0; background:
      radial-gradient(1100px 520px at 14% -8%, rgba(var(--accent-rgb),.13), transparent 60%),
      radial-gradient(900px 500px at 92% 4%, rgba(var(--accent-2-rgb),.12), transparent 55%),
      var(--bg);
    color:var(--ink); font-family:"Plus Jakarta Sans",-apple-system,"Segoe UI",sans-serif;
    -webkit-font-smoothing:antialiased;
  }
  .num,.mono{ font-family:"JetBrains Mono",ui-monospace,"SFMono-Regular",monospace; font-variant-numeric:tabular-nums; }
  h1,h2,h3,.disp{ font-family:"Chakra Petch",sans-serif; margin:0; letter-spacing:-.01em; text-wrap:balance; }
  p{ margin:0; }
  button{ font-family:inherit; }
  ::selection{ background:var(--accent-soft); color:var(--accent-soft-ink); }
  a{ color:inherit; }

  /* ---------- shell ---------- */
  .shell{ display:flex; min-height:100vh; }
  .rail{
    width:88px; flex:0 0 auto; background:linear-gradient(180deg,var(--surface),var(--bg-2));
    border-right:1px solid var(--border); display:flex; flex-direction:column; align-items:center;
    padding:20px 0; gap:26px; position:sticky; top:0; height:100vh; z-index:30;
  }
  .brand-mark{
    width:40px; height:40px; border-radius:12px; display:flex; align-items:center; justify-content:center;
    background:linear-gradient(135deg,var(--accent),var(--accent-2)); color:var(--accent-ink); font-family:"Chakra Petch";
    font-weight:700; font-size:16px; box-shadow:var(--glow-accent);
  }
  .nav{ display:flex; flex-direction:column; gap:6px; align-items:center; }
  .nav-btn{
    width:52px; height:52px; border-radius:14px; border:none; background:transparent; color:var(--ink-3);
    display:flex; flex-direction:column; align-items:center; justify-content:center; gap:3px; cursor:pointer;
    font-size:9.5px; font-weight:700; letter-spacing:.02em;
  }
  .nav-btn .ic{ font-size:17px; }
  .nav-btn:hover{ background:var(--surface-2); color:var(--ink); }
  .nav-btn.active{ background:var(--accent-soft); color:var(--accent-soft-ink); box-shadow:0 0 0 1px rgba(var(--accent-rgb),.28) inset; }
  .rail-foot{ margin-top:auto; display:flex; flex-direction:column; align-items:center; gap:10px; }
  .testnet-badge{
    writing-mode:vertical-rl; text-orientation:mixed; font-size:10px; font-weight:800; letter-spacing:.08em;
    color:var(--warning-soft-ink); background:var(--warning-soft); padding:10px 6px; border-radius:var(--radius-pill);
  }
  button:focus-visible, .nav-btn:focus-visible{ outline:2px solid var(--accent-2); outline-offset:2px; }

  .main{ flex:1 1 auto; min-width:0; padding:22px 28px 60px; max-width:1520px; }
  .view{ display:none; flex-direction:column; gap:20px; }
  .view.active{ display:flex; }

  .topbar{ display:flex; align-items:center; justify-content:space-between; gap:14px; flex-wrap:wrap; }
  .topbar h1{ font-size:19px; }
  .topbar .sub{ font-size:12.5px; color:var(--ink-3); margin-top:3px; }
  .top-stats{ display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
  .stat-chip{ display:flex; align-items:center; gap:7px; font-size:12.5px; font-weight:700; color:var(--ink-2); }
  .led{ width:8px; height:8px; border-radius:50%; }
  .led-ok{ background:var(--success); box-shadow:0 0 10px 1px rgba(34,197,94,.55); }
  .led-warn{ background:var(--warning); box-shadow:0 0 10px 1px rgba(242,201,76,.5); }

  .preview-flag{ background:var(--accent-soft); color:var(--accent-soft-ink); border:1px solid rgba(var(--accent-rgb),.28); border-radius:var(--radius-card); padding:10px 16px; font-size:12px; font-weight:600; display:flex; gap:8px; align-items:center; }

  /* ---------- ticker marquee ---------- */
  .ticker{
    background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-pill);
    overflow:hidden; position:relative; box-shadow:var(--shadow);
  }
  .ticker-track{ display:flex; gap:0; width:max-content; animation:tickerScroll 38s linear infinite; }
  .ticker:hover .ticker-track{ animation-play-state:paused; }
  @keyframes tickerScroll{ from{ transform:translateX(0); } to{ transform:translateX(-50%); } }
  .ticker-item{ display:flex; align-items:center; gap:8px; padding:10px 20px; border-right:1px solid var(--border); font-size:12.5px; white-space:nowrap; }
  .ticker-item b{ font-weight:700; }
  .ticker-item .chg{ font-weight:700; }

  /* ---------- warn banner ---------- */
  .warn-banner{ display:flex; align-items:flex-start; gap:10px; background:var(--warning-soft); color:var(--warning-soft-ink); border:1px solid rgba(242,201,76,.25); border-radius:var(--radius-card); padding:12px 16px; font-size:13px; font-weight:600; }
  .warn-banner small{ display:block; font-weight:500; opacity:.85; margin-top:2px; }

  /* ---------- KPI cards ---------- */
  .kpi-row{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }
  .kpi{
    position:relative; background:linear-gradient(160deg,var(--surface-2),var(--surface));
    border:1px solid var(--border); border-radius:var(--radius-card); padding:16px 18px; box-shadow:var(--shadow);
    overflow:hidden;
  }
  .kpi::after{ content:""; position:absolute; inset:0; background:radial-gradient(120px 60px at 88% -10%, rgba(var(--accent-rgb),.16), transparent 70%); pointer-events:none; }
  .kpi-label{ font-size:11.5px; font-weight:700; color:var(--ink-3); text-transform:uppercase; letter-spacing:.04em; }
  .kpi-value{ font-family:"Chakra Petch"; font-size:26px; font-weight:700; margin-top:6px; }
  .kpi-value.pos{ color:var(--success); } .kpi-value.neg{ color:var(--danger); }
  .kpi-sub{ font-size:11.5px; color:var(--ink-3); margin-top:5px; font-weight:500; }

  /* ---------- coin list + candlestick hero ---------- */
  .hero-grid{ display:grid; grid-template-columns:280px 1fr; gap:14px; align-items:start; }
  .coin-list{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-card); box-shadow:var(--shadow); overflow:hidden; }
  .coin-list-head{ padding:14px 16px 10px; font-size:12px; font-weight:700; color:var(--ink-3); text-transform:uppercase; letter-spacing:.04em; }
  .coin-row{ display:flex; align-items:center; gap:10px; padding:10px 14px; cursor:pointer; border-top:1px solid var(--border); }
  .coin-row:hover{ background:var(--surface-2); }
  .coin-row.active{ background:var(--accent-soft); box-shadow:inset 3px 0 0 var(--accent); }
  .avatar{ width:30px; height:30px; border-radius:9px; display:flex; align-items:center; justify-content:center; font-size:11.5px; font-weight:800; color:var(--accent-ink); flex:0 0 auto; font-family:"Chakra Petch"; }
  .coin-row-mid{ flex:1 1 auto; min-width:0; }
  .coin-row-sym{ font-size:13px; font-weight:700; }
  .coin-row-status{ font-size:10.5px; color:var(--ink-3); font-weight:600; }
  .coin-row-right{ text-align:right; }
  .coin-row-price{ font-size:12.5px; font-weight:600; }
  .coin-row-chg{ font-size:11px; font-weight:700; }

  .chart-panel{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-card); box-shadow:var(--shadow); overflow:hidden; }
  .chart-panel-top{ display:flex; align-items:center; justify-content:space-between; padding:16px 20px; border-bottom:1px solid var(--border); flex-wrap:wrap; gap:12px; }
  .cp-sym{ display:flex; align-items:center; gap:12px; }
  .cp-sym-name{ font-family:"Chakra Petch"; font-size:19px; font-weight:700; }
  .cp-ohlc{ display:flex; gap:12px; font-size:11.5px; color:var(--ink-3); font-weight:600; }
  .cp-ohlc b{ color:var(--ink-2); }
  .cp-price-block{ text-align:right; }
  .cp-price{ font-family:"Chakra Petch"; font-size:22px; font-weight:700; }
  .tf-tabs{ display:flex; gap:2px; background:var(--surface-2); border:1px solid var(--border); border-radius:var(--radius-pill); padding:3px; }
  .tf-tabs button{ border:none; background:transparent; padding:5px 12px; border-radius:var(--radius-pill); font-size:11.5px; font-weight:700; color:var(--ink-3); cursor:pointer; }
  .tf-tabs button.active{ background:var(--surface-3); color:var(--ink); box-shadow:0 0 0 1px var(--border-strong) inset; }

  .chart-body{ padding:14px 12px 6px; position:relative; }
  .chart-legend{ display:flex; gap:16px; padding:0 20px 12px; flex-wrap:wrap; }
  .leg-item{ display:flex; align-items:center; gap:6px; font-size:11px; font-weight:600; color:var(--ink-3); }
  .leg-swatch{ width:14px; height:2px; border-radius:2px; }

  .exit-explain{ padding:14px 20px 18px; background:var(--surface-2); border-top:1px solid var(--border); font-size:13px; color:var(--ink-2); line-height:1.55; display:flex; gap:10px; }
  .exit-explain b{ color:var(--ink); }

  .example-badge{ font-size:10px; font-weight:700; color:var(--accent-soft-ink); background:var(--accent-soft); padding:2px 8px; border-radius:var(--radius-pill); border:1px solid rgba(var(--accent-rgb),.28); }

  .tooltip{ position:absolute; pointer-events:none; background:#05060A; color:#F1EFEA; font-size:11px; font-weight:600; padding:8px 10px; border-radius:9px; opacity:0; transform:translate(-50%,-112%); transition:opacity .08s ease; white-space:nowrap; z-index:5; font-family:"JetBrains Mono",monospace; border:1px solid var(--border-strong); box-shadow:0 12px 24px -8px rgba(0,0,0,.6); }
  .tooltip.show{ opacity:1; }

  /* ---------- section head ---------- */
  .section-head{ display:flex; align-items:baseline; justify-content:space-between; gap:12px; }
  .section-title{ font-family:"Chakra Petch"; font-size:15px; font-weight:700; }
  .section-note{ font-size:12px; color:var(--ink-3); font-weight:500; }

  .two-col{ display:grid; grid-template-columns:1.5fr 1fr; gap:14px; align-items:stretch; }
  .chart-card{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-card); padding:16px 18px 10px; box-shadow:var(--shadow); }

  /* ---------- table ---------- */
  .table-wrap{ overflow-x:auto; border-radius:var(--radius-card); border:1px solid var(--border); background:var(--surface); }
  table{ width:100%; border-collapse:collapse; font-size:13px; min-width:640px; }
  thead th{ text-align:left; font-size:10.5px; font-weight:700; color:var(--ink-3); text-transform:uppercase; letter-spacing:.05em; padding:11px 16px; border-bottom:1px solid var(--border); background:var(--surface-2); white-space:nowrap; }
  tbody td{ padding:11px 16px; border-bottom:1px solid var(--border); white-space:nowrap; }
  tbody tr:last-child td{ border-bottom:none; }
  tbody tr:hover{ background:var(--surface-2); }
  td.num, th.num{ text-align:right; }
  .row-coin{ display:flex; align-items:center; gap:9px; font-weight:700; }
  .row-coin .avatar{ width:24px; height:24px; border-radius:7px; font-size:9.5px; }

  .pill{ display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:var(--radius-pill); font-size:11px; font-weight:700; }
  .pill-open{ background:var(--success-soft); color:var(--success-soft-ink); }
  .pill-dust{ background:var(--neutral-soft); color:var(--neutral-soft-ink); }
  .pill-wait{ background:var(--accent-soft); color:var(--accent-soft-ink); }

  .scan-grid{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
  .scan-tile{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-card); padding:14px 16px; box-shadow:var(--shadow); }
  .scan-tile .v{ font-family:"Chakra Petch"; font-size:21px; font-weight:700; margin-top:2px; }
  .taglist{ display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .tag{ font-size:11.5px; font-weight:600; background:var(--surface-2); border:1px solid var(--border); color:var(--ink-2); padding:3px 9px; border-radius:var(--radius-pill); }

  /* ---------- control center ---------- */
  .health-card{ display:flex; align-items:center; gap:16px; padding:20px 22px; border-radius:var(--radius-card); border:1px solid rgba(34,197,94,.25); box-shadow:var(--shadow); background:linear-gradient(120deg,var(--success-soft),var(--surface)); color:var(--success-soft-ink); }
  .health-ic{ font-size:24px; }
  .health-title{ font-family:"Chakra Petch"; font-weight:700; font-size:16px; }
  .health-sub{ font-size:12.5px; font-weight:500; opacity:.85; margin-top:2px; color:var(--ink-2); }

  .svc-grid{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
  .svc-card{ background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-card); padding:14px 16px; box-shadow:var(--shadow); }
  .svc-label{ font-size:11px; font-weight:700; color:var(--ink-3); margin-bottom:6px; text-transform:uppercase; letter-spacing:.03em; }
  .svc-value{ display:flex; align-items:center; gap:7px; font-weight:700; font-size:14px; }

  .ctl-note{ display:flex; gap:9px; background:var(--accent-soft); color:var(--accent-soft-ink); border:1px solid rgba(var(--accent-rgb),.28); border-radius:var(--radius-card); padding:12px 16px; font-size:12.5px; font-weight:600; line-height:1.5; }
  .ctl-actions{ display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
  .ctl-group{ display:flex; gap:10px; }
  .ctl-btn{ border:1px solid var(--border-strong); background:var(--surface-2); color:var(--ink); font-weight:700; font-size:13px; padding:10px 16px; border-radius:var(--radius-control); cursor:pointer; }
  .ctl-btn:hover{ background:var(--surface-3); }
  .ctl-btn:disabled{ opacity:.35; cursor:not-allowed; }
  .ctl-btn.warn{ border-color:transparent; background:var(--warning-soft); color:var(--warning-soft-ink); }
  .ctl-btn.go{ border-color:transparent; background:linear-gradient(135deg,var(--accent),var(--accent-2)); color:var(--accent-ink); }
  .ctl-btn.danger{ border-color:transparent; background:var(--danger); color:var(--accent-ink); }

  .maint-list{ display:flex; flex-direction:column; gap:1px; background:var(--border); border:1px solid var(--border); border-radius:var(--radius-card); overflow:hidden; }
  .maint-row{ background:var(--surface); padding:11px 16px; display:flex; align-items:center; gap:10px; font-size:13px; }
  .maint-row .t{ margin-left:auto; font-size:11px; color:var(--ink-3); font-weight:600; }
  .maint-msg{ color:var(--ink-2); font-weight:500; }

  .log-filters{ display:flex; gap:6px; }
  .log-filters button{ border:1px solid var(--border); background:var(--surface-2); color:var(--ink-2); font-size:11px; font-weight:700; padding:5px 11px; border-radius:var(--radius-pill); cursor:pointer; }
  .log-filters button.active{ background:var(--accent); color:var(--accent-ink); border-color:var(--accent); }
  .log-row{ display:flex; gap:12px; padding:9px 4px; border-bottom:1px solid var(--border); font-size:12.5px; align-items:baseline; }
  .log-row:last-child{ border-bottom:none; }
  .log-time{ color:var(--ink-3); flex:0 0 52px; }
  .log-badge{ font-weight:700; flex:0 0 auto; }
  .log-msg{ color:var(--ink-2); }

  .toast-wrap{ position:fixed; bottom:22px; right:22px; display:flex; flex-direction:column; gap:8px; z-index:60; }
  .toast{ background:var(--surface-3); color:var(--ink); border:1px solid var(--border-strong); padding:10px 16px; border-radius:10px; font-size:13px; font-weight:600; box-shadow:0 16px 34px -10px rgba(0,0,0,.6); opacity:0; transform:translateY(6px); transition:.18s ease; }
  .toast.show{ opacity:1; transform:translateY(0); }

  /* ---------- modal (Step A6 — kullanıcı-onaylı dashboard_design_reference.html'den BİREBİR taşındı) ---------- */
  .modal-veil{ position:fixed; inset:0; background:rgba(4,6,12,.6); backdrop-filter:blur(2px); display:none; align-items:center; justify-content:center; z-index:70; padding:20px; }
  .modal-veil.show{ display:flex; }
  .modal{ background:var(--surface); border-radius:18px; padding:22px 24px; width:100%; max-width:400px; box-shadow:0 30px 70px -14px rgba(0,0,0,.6); border:1px solid var(--border-strong); }
  .modal h3{ font-size:16px; margin-bottom:8px; font-family:"Chakra Petch"; }
  .modal p{ font-size:13px; color:var(--ink-2); line-height:1.55; }
  .modal-check{ display:flex; gap:8px; align-items:flex-start; margin-top:14px; font-size:12.5px; color:var(--ink-2); background:var(--surface-2); padding:10px 12px; border-radius:10px; }
  .modal-actions{ display:flex; justify-content:flex-end; gap:8px; margin-top:18px; }
  .modal-input{ width:100%; margin-top:12px; padding:10px 12px; border-radius:9px; border:1px solid var(--border-strong); background:var(--surface-2); color:var(--ink); font-size:13.5px; }
  .modal-input:focus-visible{ outline:2px solid var(--accent-2); outline-offset:1px; }

  @media (max-width: 1080px){
    .hero-grid{ grid-template-columns:1fr; }
    .coin-list{ display:flex; overflow-x:auto; }
    .coin-list-head{ display:none; }
  }
  @media (max-width: 900px){
    .rail{ position:fixed; bottom:0; left:0; right:0; top:auto; width:auto; height:auto; flex-direction:row; padding:8px 6px; z-index:40; border-right:none; border-top:1px solid var(--border); }
    .rail-foot,.brand-mark{ display:none; }
    .nav{ flex-direction:row; width:100%; justify-content:space-around; }
    .main{ padding:16px 12px 90px; }
    .kpi-row,.scan-grid,.svc-grid{ grid-template-columns:repeat(2,1fr); }
    .two-col{ grid-template-columns:1fr; }
  }
</style>
"""

# Bölüm HTML gövde: mockup'ın statik iskeleti AYNEN korunur; TÜM sayısal/
# metinsel içerik artık JS tarafından `window.__DASHBOARD_DATA__`'dan
# render edildiği için (bkz. `_PAGE_SCRIPT`), sabit örnek değerler
# kaldırılıp yerlerine boş/ID'li kapsayıcılar konmuştur. Kontrol
# butonlarının onay modali KALDIRILMIŞTIR (bkz. modül docstring'i — gerçek
# süreç kontrolü kapsam dışı; buton tıklaması artık doğrudan "bağlanmadı"
# toast'ı gösterir, sahte bir onay adımına GEREK YOKTUR).
_PAGE_BODY = """
<div class="shell">
  <nav class="rail">
    <div class="brand-mark">S</div>
    <div class="nav">
      <button class="nav-btn active" data-view="genel"><span class="ic">◧</span>Genel</button>
      <button class="nav-btn" data-view="islemler"><span class="ic">☰</span>İşlemler</button>
      <button class="nav-btn" data-view="control"><span class="ic">⌁</span>Kontrol</button>
    </div>
    <div class="rail-foot">
      <div class="testnet-badge">TESTNET</div>
    </div>
  </nav>

  <main class="main">

    <!-- ============ GENEL ============ -->
    <section class="view active" id="view-genel">
      <div class="topbar">
        <div>
          <h1>Sinyal Botu Terminali</h1>
          <div class="sub">Binance Spot Testnet · gerçek para kullanılmıyor</div>
        </div>
        <div class="top-stats" id="topStats"></div>
      </div>

      <div class="ticker">
        <div class="ticker-track" id="tickerTrack"></div>
      </div>

      <div id="warnBannerWrap"></div>

      <div class="kpi-row" id="kpiRow"></div>

      <div class="section-head"><div class="section-title">Coin Geçmişi — bir satıra tıkla</div><div class="section-note">Fiyat, koruma seviyesi ve kâr hedefi grafik üzerinde işaretlenir</div></div>
      <div class="hero-grid">
        <div class="coin-list">
          <div class="coin-list-head">Takip edilen coinler</div>
          <div id="coinListBody"></div>
        </div>
        <div class="chart-panel">
          <div class="chart-panel-top">
            <div class="cp-sym">
              <div class="avatar" id="cpAvatar" style="width:38px;height:38px;border-radius:11px;font-size:14px;"></div>
              <div>
                <div class="cp-sym-name" id="cpName">—</div>
                <div class="cp-ohlc num" id="cpOhlc"></div>
              </div>
            </div>
            <div class="tf-tabs" id="tfTabs">
              <button data-tf="5m" class="active">5dk</button><button data-tf="15m">15dk</button><button data-tf="1h">1s</button>
            </div>
            <div class="cp-price-block">
              <div class="cp-price num" id="cpPrice"></div>
              <div class="num" id="cpChange" style="font-size:12.5px;font-weight:700;"></div>
              <div class="num" id="cpPriceFreshness" style="font-size:11px;font-weight:500;color:var(--ink-3);"></div>
            </div>
          </div>
          <div class="chart-legend">
            <div class="leg-item"><span class="leg-swatch" style="background:var(--accent-2);"></span>Ortalama alış</div>
            <div class="leg-item"><span class="leg-swatch" style="background:var(--danger);"></span>Otomatik satış seviyesi</div>
            <div class="leg-item"><span class="leg-swatch" style="background:var(--success);"></span>Kâr hedefi</div>
            <div class="leg-item" id="chartExampleBadgeWrap"></div>
          </div>
          <div class="chart-body"><div id="candleWrap" style="position:relative;"></div></div>
          <div class="exit-explain">
            <span>💬</span>
            <div id="exitExplain"><b>Bot bu pozisyondan ne zaman çıkacak?</b><br>—</div>
          </div>
        </div>
      </div>

      <div class="two-col">
        <div class="chart-card">
          <div class="section-title" style="margin-bottom:8px;">Açık Pozisyonların Durumu</div>
          <div id="barWrap" style="position:relative;"></div>
        </div>
        <div class="chart-card">
          <div class="section-title" style="margin-bottom:8px;">İşlem Sonuçları</div>
          <div id="donutWrap"></div>
        </div>
      </div>

      <div class="section-head"><div class="section-title">Son İşlemler</div></div>
      <div class="table-wrap">
        <table><thead><tr><th>Coin</th><th class="num">Alındı</th><th class="num">Satıldı</th><th class="num">Sonuç</th><th>Neden</th></tr></thead>
        <tbody id="tradesBody"></tbody></table>
      </div>

      <div class="section-head"><div class="section-title">Bot Şu Anda Neyi Takip Ediyor?</div></div>
      <div class="scan-grid" id="scanGrid"></div>
      <div class="chart-card" style="flex-direction:row;align-items:center;gap:14px;flex-wrap:wrap;display:flex;">
        <span style="font-size:13px;color:var(--ink-2);">Yeni seçilenler:</span>
        <div class="taglist" id="scanTagList"></div>
      </div>
    </section>

    <!-- ============ İŞLEMLER ============ -->
    <section class="view" id="view-islemler">
      <div class="section-head"><div class="section-title">Açık İşlemler</div><div class="section-note" id="openCountNote"></div></div>
      <div class="table-wrap">
        <table><thead><tr><th>Coin</th><th>Durum</th><th class="num">Anlık fiyat</th><th class="num">Alış fiyatı</th><th class="num">Değer</th><th class="num">Sonuç</th><th>Kâr koruması</th></tr></thead>
        <tbody id="openBody"></tbody></table>
      </div>
      <div class="section-head"><div class="section-title">Çok Küçük Kalan Bakiyeler</div><div class="section-note" id="dustCountNote"></div></div>
      <div class="table-wrap">
        <table><thead><tr><th>Coin</th><th class="num">Kalan miktar</th><th class="num">Değeri</th><th>Önceki işlem neden kapandı?</th></tr></thead>
        <tbody id="dustBody"></tbody></table>
      </div>
    </section>

    <!-- ============ CONTROL ============ -->
    <section class="view" id="view-control">
      <div>
        <h1 style="font-size:20px;">Control Center</h1>
        <p style="font-size:13px;color:var(--ink-2);margin-top:4px;">Botun çalışma ve sistem durumunu yönetin.</p>
      </div>
      <div class="ctl-note">🛈 Bu ekrandaki kontroller alım veya satım emri vermez. Duraklat/Devam Et YENİ girişleri açar/kapatır — açık pozisyonlar HER ZAMAN normal şekilde yönetilmeye devam eder. Acil Durdur, süreci tamamen sonlandırır; yeniden başlatmak için sunucuda systemd erişimi gerekir (uzaktan yeniden başlatma bu sürümün kapsamı DIŞINDADIR). <b>CSE_DASHBOARD_ADMIN_TOKEN ayarlı değilse bu kontroller devre dışıdır.</b></div>
      <div class="health-card" id="healthCard"></div>
      <div class="svc-grid" id="svcGrid"></div>
      <div class="section-head"><div class="section-title">Adaptive Intelligence</div></div>
      <div class="chart-card" id="adaptiveCard"></div>
      <div class="section-head"><div class="section-title">Bot Kontrolleri</div></div>
      <div class="chart-card">
        <p style="font-size:12.5px;color:var(--ink-3);margin-bottom:14px;">Açık pozisyon için doğrudan alım veya satım emri göndermez. Yeniden Başlat kasıtlı olarak bağlı değildir (bkz. yukarıdaki not).</p>
        <div class="ctl-actions">
          <div class="ctl-group">
            <button class="ctl-btn warn" id="btnPause">Botu Duraklat</button>
            <button class="ctl-btn" id="btnResume">Botu Devam Ettir</button>
            <button class="ctl-btn go" id="btnRestart">Botu Yeniden Başlat</button>
          </div>
          <button class="ctl-btn danger" id="btnStop">Acil Durdur</button>
        </div>
      </div>
      <div class="section-head"><div class="section-title">Bakım ve Uyarılar</div></div>
      <div class="maint-list" id="maintList"></div>
      <div class="section-head"><div class="section-title">Olay Geçmişi</div></div>
      <div class="chart-card"><div class="log-row-list" id="eventLogList"></div></div>
    </section>

  </main>
</div>

<div class="modal-veil" id="modalVeil" aria-hidden="true"><div class="modal" id="modalBox" role="dialog" aria-modal="true"></div></div>
<div class="toast-wrap" id="toastWrap" aria-live="polite" aria-atomic="false"></div>
"""

# Bölüm JS: mockup'ın nav/ticker/coin-list/mum-grafiği/bar-donut/tablo
# çizim mantığı (SVG üretimi dahil) BİREBİR korunur (bkz. modül
# docstring'i — "reuse its CSS and chart-drawing JS as-is"). TEK fark:
# başta sabit/uydurma `open`/`dust`/`trades`/`genCandles()` dizileri
# KALDIRILMIŞ, yerine `window.__DASHBOARD_DATA__` (GERÇEK veri, bkz.
# `_build_view_model`) okunmuştur. Kontrol butonları artık HİÇBİR
# onay-modali/sahte-başarı YOLU izlemez — doğrudan "Bu kontrol henüz
# bağlanmadı" toast'ı gösterir (bkz. modül docstring'i — kapsam dışı).
#
# CANLI YENİLEME (bu fazın konusu — "sayfa manuel yenilenmeden güncel
# kalsın"): sayfa artık `/api/status`'ı (sayfanın İLK render'ını
# BESLEYEN AYNI, TEK endpoint — YENİ bir endpoint İCAT EDİLMEDİ)
# periyodik olarak `fetch()` ile yeniden çeker ve `buildViewModel()`
# (Python `_build_view_model`'in JS'e BİREBİR taşınmış hâli) ile AYNI
# şekle dönüştürüp `applyViewModel()` üzerinden DOM'u YERİNDE günceller
# — asla `location.reload()`/tam sayfa yenileme YOK. Önceki bir istek
# HÂLÂ sürüyorsa (`inFlight`) bir sonraki interval tık'ı ATLANIR (üst
# üste binen istek YOK). Bir poll BAŞARISIZ olursa (ağ hatası/200
# olmayan yanıt) son iyi veri EKRANDA KALIR — yalnızca görünür bir
# "veri güncel değil" göstergesi güncellenir, sayfa ASLA boşaltılmaz/
# çökmez; bir sonraki interval'da yeniden denenir.
_PAGE_SCRIPT = r"""
<script>
(function(){
  var D = window.__DASHBOARD_DATA__ || {no_snapshot_yet:true};
  function fmt(n,dec){ dec=(dec===undefined)?2:dec; if(n===null||n===undefined||isNaN(n)) return '—'; return Number(n).toLocaleString('tr-TR',{minimumFractionDigits:dec,maximumFractionDigits:dec}); }
  function pnlStr(n,dec){ if(n===null||n===undefined) return 'Hesaplanamıyor'; return (n>=0?'+':'')+fmt(n,dec===undefined?4:dec)+' USDT'; }
  function decFor(v){ return (v!==null&&v!==undefined&&v<1)?6:2; }
  // Dashboard price/P&L sync fix — honest "M1, 12sn önce" / "M5, 3dk önce"
  // freshness label so a lagging-vs-market P&L is self-explanatory
  // instead of confusing (see app.py::Application._latest_price_source).
  function priceFreshnessText(p){
    if(!p || !p.latest_price_source || !p.latest_price_as_of) return '';
    var ts; try{ ts=new Date(p.latest_price_as_of).getTime(); }catch(e){ return ''; }
    if(isNaN(ts)) return '';
    var secsAgo=Math.max(0, Math.round((Date.now()-ts)/1000));
    var agoTxt = secsAgo<60 ? secsAgo+'sn önce' : Math.round(secsAgo/60)+'dk önce';
    return p.latest_price_source+', '+agoTxt;
  }
  var success=getComputedStyle(document.documentElement).getPropertyValue('--success').trim();
  var danger=getComputedStyle(document.documentElement).getPropertyValue('--danger').trim();
  var accent=getComputedStyle(document.documentElement).getPropertyValue('--accent').trim();
  var accent2=getComputedStyle(document.documentElement).getPropertyValue('--accent-2').trim();
  var inkMuted=getComputedStyle(document.documentElement).getPropertyValue('--ink-3').trim();

  var palette=['#F2994A','#4C8DFF','#F2C94C','#FF7A59','#5CC8FF','#FF6B6B','#6EE7B7','#C99A3C','#8AA6FF','#E2984E'];
  function colorFor(sym){ var h=0; for(var i=0;i<sym.length;i++) h=(h*31+sym.charCodeAt(i))>>>0; return palette[h%palette.length]; }
  function short(sym){ return sym.replace('USDT','/USDT'); }
  function esc(s){ var d=document.createElement('div'); d.textContent=(s===null||s===undefined)?'':String(s); return d.innerHTML; }

  // ---------------- nav (veriden bağımsız, BİR KEZ bağlanır) ----------------
  var navBtns=document.querySelectorAll('.nav-btn');
  var views={genel:document.getElementById('view-genel'), islemler:document.getElementById('view-islemler'), control:document.getElementById('view-control')};
  navBtns.forEach(function(btn){
    btn.addEventListener('click', function(){
      navBtns.forEach(function(b){ b.classList.remove('active'); });
      btn.classList.add('active');
      Object.keys(views).forEach(function(k){ views[k].classList.remove('active'); });
      views[btn.dataset.view].classList.add('active');
    });
  });

  // ---------------- toasts (aria-live="polite" — bkz. HTML: assistive tech'e otomatik duyurulur) ----------------
  var toastWrap=document.getElementById('toastWrap');
  function toast(msg,icon){ var t=document.createElement('div'); t.className='toast'; t.textContent=(icon||'🛈')+' '+msg; toastWrap.appendChild(t);
    requestAnimationFrame(function(){ t.classList.add('show'); }); setTimeout(function(){ t.classList.remove('show'); setTimeout(function(){ t.remove(); },200); },2600); }

  // ---------------- modal (Step A6 — kullanıcı-onaylı mockup'ın modal deseni; Step A8 — klavye ile tam kullanılabilir) ----------------
  var modalVeil=document.getElementById('modalVeil'), modalBox=document.getElementById('modalBox');
  var modalReturnFocus=null;
  function modalFocusableEls(){ return modalBox.querySelectorAll('button, input, [tabindex]:not([tabindex="-1"])'); }
  function modalKeyHandler(e){
    if(e.key==='Escape'){ closeModal(); return; }
    if(e.key==='Tab'){
      var f=modalFocusableEls();
      if(!f.length) return;
      var first=f[0], last=f[f.length-1];
      if(e.shiftKey && document.activeElement===first){ e.preventDefault(); last.focus(); }
      else if(!e.shiftKey && document.activeElement===last){ e.preventDefault(); first.focus(); }
    }
  }
  function openModal(html){
    modalReturnFocus = document.activeElement;
    modalBox.innerHTML = html;
    modalVeil.classList.add('show');
    modalVeil.setAttribute('aria-hidden','false');
    document.addEventListener('keydown', modalKeyHandler);
    var f=modalFocusableEls();
    if(f.length) f[0].focus();
  }
  function closeModal(){
    modalVeil.classList.remove('show');
    modalVeil.setAttribute('aria-hidden','true');
    document.removeEventListener('keydown', modalKeyHandler);
    modalBox.innerHTML='';
    if(modalReturnFocus && typeof modalReturnFocus.focus==='function') modalReturnFocus.focus();
    modalReturnFocus=null;
  }
  modalVeil.addEventListener('click', function(e){ if(e.target===modalVeil) closeModal(); });

  // ---------------- Control Center: GERÇEK admin çağrıları (24/7 Ops v1, Step 3) — token/onay artık native prompt/confirm DEĞİL, yukarıdaki modal (Step A6) ----------------
  // Token TARAYICI belleğinde tutulur (sayfaya/HTML'e GÖMÜLMEZ, disk/
  // localStorage'a YAZILMAZ) — sekme kapanınca kaybolur, operatör her
  // yeni oturumda bir kez girer. Yeniden Başlat KASITLI OLARAK bağlı
  // DEĞİL (bkz. modül docstring'i — uzaktan yeniden başlatma bu
  // milestone'un kapsamı DIŞINDA, Acil Durdur + systemd gerektirir).
  var cachedAdminToken = null;
  function openTokenModal(onSubmit){
    openModal(
      '<h3>Yönetici token\'ı</h3><p>Control Center işlemi için gerekli.</p>'+
      '<input type="password" id="mTokenInput" class="modal-input mono" autocomplete="off" aria-label="Yönetici token\'ı">'+
      '<div class="modal-actions"><button class="ctl-btn" id="mCancel">Vazgeç</button><button class="ctl-btn go" id="mOk">Onayla</button></div>'
    );
    var input=document.getElementById('mTokenInput');
    document.getElementById('mCancel').addEventListener('click', closeModal);
    function submit(){ var v=input.value; if(!v) return; closeModal(); onSubmit(v); }
    document.getElementById('mOk').addEventListener('click', submit);
    input.addEventListener('keydown', function(e){ if(e.key==='Enter'){ e.preventDefault(); submit(); } });
  }
  function callAdmin(path, label){
    if(!D.admin || !D.admin.enabled){ toast('Control Center devre dışı (CSE_DASHBOARD_ADMIN_TOKEN ayarlı değil)'); return; }
    function proceed(token){
      fetch(path, {method:'POST', headers:{'X-Admin-Token':token}}).then(function(resp){
        if(resp.status===401){ cachedAdminToken=null; toast('Token geçersiz — yetkisiz'); return; }
        if(!resp.ok){ toast(label+' başarısız (HTTP '+resp.status+')'); return; }
        toast(label+' uygulandı');
        pollOnce();
      }).catch(function(){ toast(label+' başarısız (ağ hatası)'); });
    }
    if(cachedAdminToken){ proceed(cachedAdminToken); return; }
    openTokenModal(function(token){ cachedAdminToken=token; proceed(token); });
  }
  var btnPause=document.getElementById('btnPause'), btnResume=document.getElementById('btnResume');
  var btnRestart=document.getElementById('btnRestart'), btnStop=document.getElementById('btnStop');
  if(btnPause) btnPause.addEventListener('click', function(){ callAdmin('/admin/pause','Duraklatma'); });
  if(btnResume) btnResume.addEventListener('click', function(){ callAdmin('/admin/resume','Devam ettirme'); });
  if(btnRestart) btnRestart.addEventListener('click', function(){ toast('Bu kontrol kasıtlı olarak bağlanmadı — bkz. Acil Durdur + sunucuda systemd restart'); });
  if(btnStop) btnStop.addEventListener('click', function(){
    if(!D.admin || !D.admin.enabled){ toast('Control Center devre dışı (CSE_DASHBOARD_ADMIN_TOKEN ayarlı değil)'); return; }
    openModal(
      '<h3>Botu TAMAMEN durdurmak istediğinize emin misiniz?</h3>'+
      '<p>Süreç hemen durdurulacak. Doğrudan açık pozisyonu satmaz veya emir göndermez. Yeniden başlatmak için sunucuda systemd erişimi gerekir.</p>'+
      '<label class="modal-check"><input type="checkbox" id="mChk" style="margin-top:2px;">Yalnızca bot sürecinin durdurulacağını anlıyorum.</label>'+
      '<div class="modal-actions"><button class="ctl-btn" id="mCancel">Vazgeç</button><button class="ctl-btn danger" id="mOk" disabled>Acil Durdur</button></div>'
    );
    document.getElementById('mCancel').addEventListener('click', closeModal);
    var chk=document.getElementById('mChk'), ok=document.getElementById('mOk');
    chk.addEventListener('change', function(){ ok.disabled=!chk.checked; });
    ok.addEventListener('click', function(){ if(chk.checked){ closeModal(); callAdmin('/admin/stop','Acil durdurma'); } });
  });

  function svgEl(tag,attrs){ var el=document.createElementNS('http://www.w3.org/2000/svg',tag); for(var k in attrs) el.setAttribute(k,attrs[k]); return el; }

  // ============================================================
  // GÖRÜNÜM MODELİ (JS tarafı) — `dashboard.py::_build_view_model()`'in
  // BİREBİR taşınmış hâli. `/api/status` HER poll'da AYNEN bu şekle
  // dönüştürülür — sayfanın İLK yüklemede kullandığı TEK gerçek veri
  // kaynağıyla (aynı endpoint) tutarlılık BÖYLECE korunur.
  // ============================================================
  var STATE_TR = {
    FLAT:'Kapalı', LONG:'Açık', DUST:'Satılamayacak kadar küçük kalan',
    ENTRY_PENDING:'Alış emri sonuç bekliyor', EXIT_PENDING:'Satış emri sonuç bekliyor',
    AMBIGUOUS:'Binance sonucu kontrol ediliyor', RECOVERED:'Geri yüklendi / aktifleşme bekliyor'
  };
  var EXIT_REASON_TR = {
    STOP_LOSS:'Güvenlik seviyesi', TAKE_PROFIT:'Kâr hedefi', TRAILING_STOP:'Takip eden stop',
    MAX_HOLD:'Maksimum bekleme süresi', OPPOSITE_SIGNAL:'Ters sinyal'
  };
  var TIMEFRAME_TR = {'1m':'1 dakikalık','5m':'5 dakikalık','15m':'15 dakikalık','1h':'1 saatlik'};

  function translate(map, value){
    if(value===null||value===undefined) return null;
    var text=String(value);
    return Object.prototype.hasOwnProperty.call(map,text) ? map[text] : text;
  }
  function translateSymbolDetail(detail){
    var text=String(detail===null||detail===undefined?'':detail);
    if(text==='healthy') return 'Sorun yok';
    if(text==='bootstrap in progress') return 'Başlangıç verileri hazırlanıyor';
    if(text==='runtime stopped') return 'Sistem durduruldu';
    if(text==='no event observed yet') return 'Henüz piyasa verisi alınmadı';
    if(text==='stream disconnected') return 'Piyasa veri bağlantısı koptu, yeniden bağlanmaya çalışılıyor';
    if(text.indexOf('unresolved gap:')===0){
      var rawList=text.split(':').slice(1).join(':').trim();
      var parts=rawList.split(',').map(function(p){return p.trim();}).filter(Boolean);
      var trParts=parts.map(function(p){ return TIMEFRAME_TR[p]||p; }).join(', ');
      return trParts+' piyasa verisinde eksik veri algılandı';
    }
    if(text.indexOf('stale:')===0) return 'Piyasa verisi bir süredir güncellenmiyor';
    if(text.indexOf('durable checkpoint write failed')===0) return 'Sistem durumu diske kaydedilirken geçici bir sorun oluştu';
    return text;
  }
  function degradedSymbolsFrom(symbols){
    return (symbols||[]).filter(function(s){ return String(s.health)==='DEGRADED'; }).map(function(s){
      return {symbol:String(s.symbol||'?'), detail_tr:translateSymbolDetail(s.detail||''), last_event_at:(s.last_event_at!==undefined?s.last_event_at:null)};
    });
  }
  function orNull(v){ return v===undefined ? null : v; }

  function buildViewModel(snapshot){
    var overallHealth=String(snapshot.overall_health||'');
    var execution=snapshot.execution||{};
    var bridgeLifecycle=snapshot.bridge_lifecycle||{};
    var symbolSelection=snapshot.symbol_selection||{};
    var candleHistory=snapshot.candle_history||{};
    var rawSymbols=snapshot.symbols||[];

    var bridgeEnabled=!!bridgeLifecycle.enabled;
    var positionsRaw=bridgeEnabled ? (bridgeLifecycle.positions||{}) : {};
    var performance=bridgeEnabled ? (bridgeLifecycle.performance||{}) : {};
    var recentTradesRaw=bridgeEnabled ? (bridgeLifecycle.recent_trades||[]) : [];

    var positionsOut=[], dustOut=[];
    Object.keys(positionsRaw).forEach(function(symbol){
      var p=positionsRaw[symbol];
      if(!p || typeof p!=='object' || p.state==='FLAT') return;
      var grossEntryVwap=orNull(p.gross_entry_vwap), netQty=orNull(p.net_owned_base_quantity);
      var positionValue=(grossEntryVwap!==null && netQty!==null) ? (grossEntryVwap*netQty) : null;
      var entry={
        symbol:symbol, state:p.state, state_tr:translate(STATE_TR,p.state),
        gross_entry_vwap:grossEntryVwap, latest_price:orNull(p.latest_price),
        latest_price_source:orNull(p.latest_price_source), latest_price_as_of:orNull(p.latest_price_as_of),
        net_owned_base_quantity:netQty,
        position_value:positionValue, effective_stop:orNull(p.effective_stop), take_profit:orNull(p.take_profit),
        trailing_active:!!p.trailing_active, unrealized_gross_pnl:orNull(p.unrealized_gross_pnl),
        cumulative_realized_gross_pnl:orNull(p.cumulative_realized_gross_pnl), last_exit_reason:orNull(p.last_exit_reason),
        last_exit_reason_tr:translate(EXIT_REASON_TR,p.last_exit_reason), high_water:orNull(p.high_water),
        cooldown_until:orNull(p.cooldown_until)
      };
      if(p.state==='DUST') dustOut.push(entry); else positionsOut.push(entry);
    });

    var tradesOut=(recentTradesRaw.slice(0,20)).map(function(t){
      var gross=orNull(t.gross_realized_pnl), net=orNull(t.net_realized_pnl);
      return {
        recorded_at:orNull(t.recorded_at), symbol:orNull(t.symbol), quantity_closed:orNull(t.quantity_closed),
        gross_entry_vwap:orNull(t.gross_entry_vwap), exit_gross_vwap:orNull(t.exit_gross_vwap),
        gross_realized_pnl:gross, net_realized_pnl:net,
        commission_usdt:(gross!==null && net!==null) ? (gross-net) : null,
        exit_reason:orNull(t.exit_reason), exit_reason_tr:translate(EXIT_REASON_TR,t.exit_reason)
      };
    });

    var completedCount=performance.completed_trade_count||0;
    var kpi=bridgeEnabled ? {
      unrealized_total:orNull(performance.unrealized_gross_pnl_total), open_count:performance.non_flat_position_count||0,
      todays_realized:orNull(performance.todays_realized_gross_pnl), realized_total:orNull(performance.realized_gross_pnl),
      completed_trade_count:completedCount, win_count:performance.win_count||0, loss_count:performance.loss_count||0,
      win_rate:orNull(performance.win_rate), average_win_gross:orNull(performance.average_win_gross),
      average_loss_gross:orNull(performance.average_loss_gross), exposure_usdt:orNull(performance.exposure_usdt)
    } : null;

    var mode=symbolSelection.mode||'MANUAL';
    var reselection=symbolSelection.reselection_scheduler||{};
    var candidatesRaw=symbolSelection.candidates||[];
    var learnedFactors={};
    candidatesRaw.forEach(function(c){ if(c && c.symbol!==undefined && c.symbol!==null) learnedFactors[c.symbol]=c.learned_factor_score; });
    var scan={
      mode:mode, scanned_count:orNull(symbolSelection.shortlist_size), universe_size:orNull(symbolSelection.universe_size),
      new_opportunities:symbolSelection.opportunity_symbols||[], protected_symbols:symbolSelection.pinned_open_position_symbols||[],
      tracked_symbols:symbolSelection.final_runtime_symbols||[], armed:!!reselection.enabled && !!reselection.armed,
      reselection_enabled:!!reselection.enabled, last_scan_at:orNull(reselection.last_rescan_at),
      next_scan_at:orNull(reselection.next_rescan_at), interval_seconds:orNull(reselection.rescan_interval_seconds),
      learned_factors:learnedFactors
    };

    return {
      generated_at:orNull(snapshot.generated_at), overall_health:overallHealth,
      bot_running:overallHealth!=='STOPPED' && overallHealth!=='', testnet_enabled:!!execution.enabled,
      testnet_ready:!!execution.ready, recovery_ok:!!((snapshot.recovery||{}).ok), bridge_enabled:bridgeEnabled,
      degraded_symbols:degradedSymbolsFrom(rawSymbols), kpi:kpi, positions:positionsOut, dust:dustOut,
      recent_trades:tradesOut, scan:scan, candles:candleHistory, pid:orNull(snapshot.pid),
      started_at:orNull(snapshot.started_at), no_snapshot_yet:false,
      admin: snapshot.admin || {enabled:false},
      usdt_balance: bridgeEnabled ? orNull(bridgeLifecycle.usdt_balance) : null,
      adaptive: snapshot.adaptive || {active:false},
      backup: snapshot.backup || {available:false},
      events: (snapshot.event_log && snapshot.event_log.events) || []
    };
  }

  // ---------------- top-stats (GERÇEK: bot çalışıyor mu / tarama aktif mi + "canlı" göstergesi) ----------------
  function renderTopStats(){
    var el=document.getElementById('topStats');
    if(D.no_snapshot_yet){
      el.innerHTML = '<div class="stat-chip"><span class="led led-warn"></span>Sistem durum dosyası henüz oluşmadı</div>'+
        '<span class="stat-chip" id="freshnessChip"></span>';
      return;
    }
    var runLed = D.bot_running ? 'led-ok' : 'led-warn';
    var runTxt = D.bot_running ? 'Bot çalışıyor' : 'Bot çalışmıyor';
    var scanLed = (D.scan && D.scan.armed) ? 'led-ok' : 'led-warn';
    var scanTxt = (D.scan && D.scan.armed) ? 'Tarama aktif' : (D.scan && D.scan.mode==='MANUAL' ? 'Tarama kapalı (manuel mod)' : 'Tarama aktif değil');
    var now = D.generated_at ? new Date(D.generated_at) : null;
    var timeTxt = now ? now.toLocaleTimeString('tr-TR',{hour:'2-digit',minute:'2-digit'}) : '';
    el.innerHTML =
      '<div class="stat-chip"><span class="led '+runLed+'"></span>'+runTxt+'</div>'+
      '<div class="stat-chip"><span class="led '+scanLed+'"></span>'+scanTxt+'</div>'+
      '<div class="stat-chip num" style="color:var(--ink-3);">'+timeTxt+'</div>'+
      '<span class="stat-chip" id="freshnessChip"></span>';
  }

  // ---------------- canlı-veri göstergesi ("Xs önce güncellendi" / "veri güncel değil") ----------------
  var lastUpdateAt = D.no_snapshot_yet ? null : Date.now();
  var lastPollOk = true;
  function renderFreshness(){
    var chip=document.getElementById('freshnessChip');
    if(!chip) return;
    if(lastUpdateAt===null){ chip.innerHTML='<span class="led led-warn"></span>Henüz veri yok'; return; }
    var secsAgo=Math.max(0, Math.round((Date.now()-lastUpdateAt)/1000));
    var agoTxt = secsAgo<1 ? 'az önce' : secsAgo+' sn önce';
    if(lastPollOk){
      chip.innerHTML='<span class="led led-ok"></span>Güncellendi: '+agoTxt;
    } else {
      chip.innerHTML='<span class="led led-warn"></span>Veri güncel değil — son güncelleme '+agoTxt+' (yeniden deneniyor)';
    }
  }

  // ---------------- warn banner (GERÇEK: DEGRADED sembol nedenleri) ----------------
  function renderWarnBanner(){
    var wrap=document.getElementById('warnBannerWrap');
    var degraded = D.degraded_symbols||[];
    if(!degraded.length){ wrap.innerHTML=''; return; }
    var first=degraded[0];
    var extra = degraded.length>1 ? ' (+'+(degraded.length-1)+' coin daha)' : '';
    wrap.innerHTML = '<div class="warn-banner"><span>⚠</span><div>Dikkat — <b>'+esc(first.symbol)+'</b> için: '+esc(first.detail_tr)+extra+
      '<small>Bot yeni kararlarda beklemeyi tercih edebilir.</small></div></div>';
  }

  // ---------------- KPI cards (GERÇEK) ----------------
  function renderKpis(){
    var row=document.getElementById('kpiRow');
    var k = D.kpi;
    if(!k){ row.innerHTML = '<div class="kpi"><div class="kpi-label">Testnet işlem köprüsü</div><div class="kpi-value">Devre dışı</div></div>'; return; }
    var un = k.unrealized_total, tot = (k.completed_trade_count>0 ? k.realized_total : 0);
    var combined = (un===null||un===undefined) ? null : (tot + un);
    var realizedShown = k.completed_trade_count>0 ? k.realized_total : null;
    var cards = [
      {label:'Açık Pozisyon Sonucu', val: un, sub: k.open_count+' açık işlem üzerinden'},
      {label:'Bugünkü Kâr / Zarar', val: k.todays_realized, sub:'Bugün tamamlanan işlemler dahil'},
      {label:'Toplam Kâr / Zarar', val: combined, sub:'Kesin: '+(realizedShown===null?'—':pnlStr(realizedShown,2))+' · Açık: '+(un===null?'—':pnlStr(un,2)), rawSub:true},
      {label:'Kazanan İşlem Oranı', val:null, custom:(k.win_rate===null||k.win_rate===undefined)?'—':('%'+fmt(k.win_rate*100,1)), sub:k.win_count+' kazanan · '+k.loss_count+' kaybeden'}
    ];
    row.innerHTML = cards.map(function(c){
      var cls = c.custom ? '' : (c.val===null||c.val===undefined ? '' : (c.val>=0?'pos':'neg'));
      var valTxt = c.custom!==undefined ? c.custom : pnlStr(c.val,2);
      return '<div class="kpi"><div class="kpi-label">'+c.label+'</div><div class="kpi-value '+cls+' num">'+valTxt+'</div><div class="kpi-sub num">'+c.sub+'</div></div>';
    }).join('');
  }

  // ---------------- coin list (GERÇEK açık pozisyonlar) ----------------
  var positions = D.positions || [];
  var selected = positions.length ? positions[0].symbol : null;
  var coinListBody = document.getElementById('coinListBody');
  function renderCoinList(){
    if(!positions.length){ coinListBody.innerHTML='<p style="padding:14px 16px;color:var(--ink-3);font-size:12.5px;">Şu anda açık gerçek Testnet işlemi yok.</p>'; return; }
    var html='';
    positions.forEach(function(p){
      var pnl=p.unrealized_gross_pnl; var up = pnl===null||pnl===undefined ? true : pnl>=0;
      var priceTxt = p.latest_price===null||p.latest_price===undefined ? 'Hesaplanamıyor' : fmt(p.latest_price,decFor(p.latest_price));
      html += '<div class="coin-row'+(p.symbol===selected?' active':'')+'" data-c="'+p.symbol+'">'+
        '<div class="avatar" style="background:'+colorFor(p.symbol)+';">'+p.symbol.slice(0,2)+'</div>'+
        '<div class="coin-row-mid"><div class="coin-row-sym">'+short(p.symbol)+'</div><div class="coin-row-status">'+(p.trailing_active?'Kâr koruması aktif':(p.state_tr||p.state))+'</div></div>'+
        '<div class="coin-row-right"><div class="coin-row-price num">'+priceTxt+'</div><div class="coin-row-chg num" style="color:'+(up?success:danger)+'">'+pnlStr(pnl)+'</div></div>'+
        '</div>';
    });
    coinListBody.innerHTML = html;
    coinListBody.querySelectorAll('.coin-row').forEach(function(row){
      row.addEventListener('click', function(){ selected = row.dataset.c; renderCoinList(); renderChart(); });
    });
  }

  // ---------------- seeded RNG (SADECE gerçek mum verisi YOKSA, "örnek veri" rozetiyle) ----------------
  function mulberry32(seed){ return function(){ seed|=0; seed=seed+0x6D2B79F5|0; var t=Math.imul(seed^seed>>>15,1|seed); t=t+Math.imul(t^t>>>7,61|t)^t; return ((t^t>>>14)>>>0)/4294967296; }; }
  function hashSeed(s){ var h=0; for(var i=0;i<s.length;i++) h=(h*31+s.charCodeAt(i))>>>0; return h; }
  function genCandles(sym, entry, current, n, tf){
    var rnd = mulberry32(hashSeed(sym+tf));
    var vol = Math.abs(current-entry)/Math.max(entry,1e-9);
    vol = Math.max(vol*0.6, 0.006);
    var candles=[];
    var logEntry=Math.log(entry), logCur=Math.log(current);
    var price = logEntry;
    for(var i=0;i<n;i++){
      var target = logEntry + (logCur-logEntry) * (i/(n-1));
      var drift = (target-price)*0.35;
      var shock = (rnd()-0.5)*vol;
      var o = price;
      price = price + drift + shock;
      if(i===n-1) price = logCur;
      var c = price;
      var hi = Math.max(o,c) + Math.abs(rnd())*vol*0.6;
      var lo = Math.min(o,c) - Math.abs(rnd())*vol*0.6;
      candles.push({o:Math.exp(o), h:Math.exp(hi), l:Math.exp(lo), c:Math.exp(c)});
    }
    return candles;
  }
  function realCandles(sym, tf){
    var bySym = D.candles && D.candles[sym];
    var arr = bySym && bySym[tf];
    if(!arr || !arr.length) return null;
    return arr.map(function(x){ return {o:x.o,h:x.h,l:x.l,c:x.c}; });
  }

  var currentTf = '5m';
  document.getElementById('tfTabs').addEventListener('click', function(e){
    if(e.target.tagName!=='BUTTON') return;
    document.querySelectorAll('#tfTabs button').forEach(function(b){ b.classList.remove('active'); });
    e.target.classList.add('active');
    currentTf = e.target.dataset.tf;
    renderChart();
  });

  function reasonText(p){
    var stopTxt = p.effective_stop===null||p.effective_stop===undefined ? 'Hesaplanamıyor' : fmt(p.effective_stop,decFor(p.effective_stop))+' USDT';
    var targetTxt = p.take_profit===null||p.take_profit===undefined ? 'Hesaplanamıyor' : fmt(p.take_profit,decFor(p.take_profit))+' USDT';
    var lines = '<b>Bot bu pozisyondan ne zaman çıkacak?</b><br>Fiyat '+stopTxt+' altına düşerse bot pozisyonu otomatik kapatır. Fiyat '+targetTxt+' seviyesine ulaşırsa kârı almak için kapatabilir.';
    if(p.trailing_active) lines += ' Kâr koruması şu anda <b>aktif</b> — fiyat yükseldikçe satış seviyesi yukarı taşınıyor.';
    return lines;
  }

  function renderChart(){
    var wrap = document.getElementById('candleWrap');
    var badgeWrap = document.getElementById('chartExampleBadgeWrap');
    if(!selected){
      document.getElementById('cpName').textContent='—'; document.getElementById('cpPrice').textContent='';
      document.getElementById('cpOhlc').innerHTML=''; document.getElementById('cpChange').textContent='';
      document.getElementById('cpPriceFreshness').textContent='';
      document.getElementById('exitExplain').innerHTML='<b>Bot bu pozisyondan ne zaman çıkacak?</b><br>Şu anda açık gerçek işlem yok.';
      wrap.innerHTML=''; badgeWrap.innerHTML=''; return;
    }
    var p = positions.find(function(x){ return x.symbol===selected; });
    document.getElementById('cpAvatar').style.background = colorFor(p.symbol);
    document.getElementById('cpAvatar').textContent = p.symbol.slice(0,2);
    document.getElementById('cpName').textContent = short(p.symbol);
    var priceTxt = p.latest_price===null||p.latest_price===undefined ? 'Hesaplanamıyor' : fmt(p.latest_price,decFor(p.latest_price))+' USDT';
    document.getElementById('cpPrice').textContent = priceTxt;
    document.getElementById('cpPriceFreshness').textContent = priceFreshnessText(p);
    var chg = document.getElementById('cpChange');
    chg.textContent = pnlStr(p.unrealized_gross_pnl);
    chg.style.color = (p.unrealized_gross_pnl===null||p.unrealized_gross_pnl===undefined) ? inkMuted : (p.unrealized_gross_pnl>=0? success:danger);
    document.getElementById('exitExplain').innerHTML = reasonText(p);

    var real = realCandles(p.symbol, currentTf);
    var candles, isExample;
    if(real && real.length>=2){
      candles = real; isExample=false;
    } else if(p.gross_entry_vwap && p.latest_price){
      var n = currentTf==='5m'?48 : currentTf==='15m'?48:48;
      candles = genCandles(p.symbol, p.gross_entry_vwap, p.latest_price, n, currentTf); isExample=true;
    } else {
      candles = null; isExample=true;
    }
    badgeWrap.innerHTML = isExample ? '<span class="example-badge">örnek veri</span>' : '<span class="example-badge" style="background:var(--success-soft);color:var(--success-soft-ink);border-color:rgba(34,197,94,.3);">gerçek veri</span>';

    if(!candles || !candles.length){
      wrap.innerHTML = '<p style="padding:20px;color:var(--ink-3);font-size:12.5px;">Bu coin için henüz yeterli fiyat geçmişi yok.</p>';
      document.getElementById('cpOhlc').innerHTML='';
      return;
    }
    var last=candles[candles.length-1], first=candles[0];
    document.getElementById('cpOhlc').innerHTML = 'A <b>'+fmt(first.o,decFor(first.o))+'</b>  Y '+fmt(Math.max.apply(null,candles.map(function(x){return x.h;})),decFor(p.latest_price||first.o))+'  D '+fmt(Math.min.apply(null,candles.map(function(x){return x.l;})),decFor(p.latest_price||first.o))+'  K <b>'+fmt(last.c,decFor(last.c))+'</b>';

    wrap.innerHTML='';
    var w=1000, h=320, pad={t:16,r:64,b:24,l:8};
    var allVals = candles.reduce(function(a,c){ a.push(c.h,c.l); return a; }, []);
    if(p.effective_stop!==null&&p.effective_stop!==undefined) allVals.push(p.effective_stop);
    if(p.take_profit!==null&&p.take_profit!==undefined) allVals.push(p.take_profit);
    if(p.gross_entry_vwap!==null&&p.gross_entry_vwap!==undefined) allVals.push(p.gross_entry_vwap);
    var vmin=Math.min.apply(null,allVals), vmax=Math.max.apply(null,allVals);
    if(vmin===vmax){ vmin=vmin*0.98; vmax=vmax*1.02; }
    var vpad=(vmax-vmin)*0.08; vmin-=vpad; vmax+=vpad;
    var plotH = h-pad.t-pad.b-8;
    var y = function(v){ return pad.t + (1-(v-vmin)/(vmax-vmin))*plotH; };
    var cw = (w-pad.l-pad.r)/candles.length;
    var x = function(i){ return pad.l + i*cw + cw/2; };

    var svg = svgEl('svg',{viewBox:'0 0 '+w+' '+h, width:'100%', height:h, role:'img','aria-label':p.symbol+' fiyat geçmişi'});

    for(var g=0; g<=3; g++){
      var gy = pad.t + g*plotH/3;
      svg.appendChild(svgEl('line',{x1:pad.l,x2:w-pad.r,y1:gy,y2:gy, stroke:'var(--border)', 'stroke-width':1}));
    }

    function refLine(val, color){
      if(val===null||val===undefined) return;
      var yy=y(val);
      svg.appendChild(svgEl('line',{x1:pad.l,x2:w-pad.r,y1:yy,y2:yy, stroke:color,'stroke-width':1.3,'stroke-dasharray':'4,4', opacity:0.85}));
      var lbl = svgEl('text',{x:w-pad.r+6, y:yy+3.5, 'font-size':10.5, fill:color, 'font-family':'JetBrains Mono'});
      lbl.textContent = fmt(val,decFor(val));
      svg.appendChild(lbl);
    }
    refLine(p.gross_entry_vwap, accent2);
    refLine(p.effective_stop, danger);
    refLine(p.take_profit, success);

    candles.forEach(function(cd,i){
      var up = cd.c>=cd.o;
      var color = up?success:danger;
      var cx = x(i);
      svg.appendChild(svgEl('line',{x1:cx,x2:cx,y1:y(cd.h),y2:y(cd.l), stroke:color,'stroke-width':1, opacity:0.9}));
      var bodyTop = y(Math.max(cd.o,cd.c)), bodyBot = y(Math.min(cd.o,cd.c));
      var bh = Math.max(bodyBot-bodyTop, 1.6);
      var bar = svgEl('rect',{
        x:cx-cw*0.34, y:bodyTop, width:cw*0.68, height:bh, rx:1.5, fill:color,
        stroke:'rgba(5,6,10,.4)','stroke-width':0.6,
      });
      svg.appendChild(bar);
    });

    // ---- crosshair: dikey (mum'a snap) + yatay (mouse Y'sindeki fiyat) + fiyat ekseni etiketi ----
    var hoverLineX = svgEl('line',{x1:0,x2:0,y1:pad.t,y2:h-pad.b, stroke:accent2,'stroke-width':1,'stroke-dasharray':'2,3', opacity:0});
    svg.appendChild(hoverLineX);
    var hoverLineY = svgEl('line',{x1:pad.l,x2:w-pad.r,y1:0,y2:0, stroke:accent2,'stroke-width':1,'stroke-dasharray':'2,3', opacity:0});
    svg.appendChild(hoverLineY);
    var priceLabelBg = svgEl('rect',{x:w-pad.r+2,y:0,width:56,height:16,rx:3,fill:accent2,opacity:0});
    svg.appendChild(priceLabelBg);
    var priceLabelText = svgEl('text',{x:w-pad.r+7,y:0,'font-size':10.5,'font-weight':700,fill:'var(--accent-ink)','font-family':'JetBrains Mono',opacity:0});
    svg.appendChild(priceLabelText);
    var hitRect = svgEl('rect',{x:pad.l,y:pad.t,width:w-pad.l-pad.r,height:h-pad.t-pad.b, fill:'transparent'});
    svg.appendChild(hitRect);
    wrap.appendChild(svg);
    var tip=document.createElement('div'); tip.className='tooltip'; wrap.appendChild(tip);

    hitRect.addEventListener('mousemove', function(e){
      var rect=svg.getBoundingClientRect();
      var relX=(e.clientX-rect.left)*(w/rect.width);
      var relY=(e.clientY-rect.top)*(h/rect.height);
      var i=Math.round((relX-pad.l-cw/2)/cw);
      i=Math.max(0,Math.min(candles.length-1,i));
      hoverLineX.setAttribute('x1',x(i)); hoverLineX.setAttribute('x2',x(i)); hoverLineX.setAttribute('opacity',1);

      var clampedY = Math.max(pad.t, Math.min(h-pad.b, relY));
      hoverLineY.setAttribute('y1',clampedY); hoverLineY.setAttribute('y2',clampedY); hoverLineY.setAttribute('opacity',1);
      var priceAtY = vmin + (1-(clampedY-pad.t)/plotH)*(vmax-vmin);
      var priceTxt = fmt(priceAtY, decFor(priceAtY));
      var labelW = Math.max(50, priceTxt.length*6.6+12);
      priceLabelBg.setAttribute('x', w-pad.r+2); priceLabelBg.setAttribute('y', clampedY-8);
      priceLabelBg.setAttribute('width', labelW); priceLabelBg.setAttribute('opacity',1);
      priceLabelText.setAttribute('y', clampedY+3.5); priceLabelText.setAttribute('opacity',1);
      priceLabelText.textContent = priceTxt;

      var wrapRect=wrap.getBoundingClientRect();
      var px=(x(i)/w)*wrapRect.width, py=(y(candles[i].c)/h)*wrapRect.height;
      tip.style.left=px+'px'; tip.style.top=py+'px';
      var cd=candles[i];
      tip.textContent = 'A '+fmt(cd.o,decFor(cd.o))+'  Y '+fmt(cd.h,decFor(cd.h))+'  D '+fmt(cd.l,decFor(cd.l))+'  K '+fmt(cd.c,decFor(cd.c));
      tip.classList.add('show');
    });
    hitRect.addEventListener('mouseleave', function(){
      hoverLineX.setAttribute('opacity',0); hoverLineY.setAttribute('opacity',0);
      priceLabelBg.setAttribute('opacity',0); priceLabelText.setAttribute('opacity',0);
      tip.classList.remove('show');
    });
  }

  // ---------------- ticker marquee (GERÇEK fiyat + gerçek son-N-mum değişimi) ----------------
  function renderTicker(){
    var track = document.getElementById('tickerTrack');
    if(!positions.length){ track.innerHTML=''; return; }
    function pctChange(sym){
      var arr = D.candles && D.candles[sym] && D.candles[sym]['5m'];
      if(!arr || arr.length<2) return null;
      var f=arr[0].c, l=arr[arr.length-1].c;
      if(!f) return null;
      return (l-f)/f*100;
    }
    var items = positions.map(function(p){ return {sym:p.symbol, price:p.latest_price, pct:pctChange(p.symbol)}; });
    var html='';
    items.concat(items).forEach(function(t){
      var priceTxt = t.price===null||t.price===undefined?'—':fmt(t.price,decFor(t.price));
      var chgHtml = '';
      if(t.pct!==null && t.pct!==undefined && isFinite(t.pct)){
        var up=t.pct>=0;
        chgHtml = '<span class="chg num" style="color:'+(up?success:danger)+'">'+(up?'▲':'▼')+' '+(up?'+':'')+fmt(t.pct,2)+'%</span>';
      }
      html += '<div class="ticker-item"><span style="width:7px;height:7px;border-radius:2px;background:'+colorFor(t.sym)+';display:inline-block;"></span>'+
        '<b>'+t.sym.replace('USDT','')+'</b><span class="num">'+priceTxt+'</span>'+chgHtml+'</div>';
    });
    track.innerHTML = html;
  }

  // ---------------- bar (GERÇEK anlık kâr/zarar karşılaştırması) ----------------
  function renderBar(){
    var wrap=document.getElementById('barWrap');
    var data = positions.filter(function(p){ return p.unrealized_gross_pnl!==null && p.unrealized_gross_pnl!==undefined; })
      .map(function(p){ return {c:p.symbol.replace('USDT',''), v:p.unrealized_gross_pnl}; });
    wrap.innerHTML='';
    if(!data.length){ wrap.innerHTML='<p style="padding:10px;color:var(--ink-3);font-size:12.5px;">Anlık kâr/zarar hesaplanabilen açık işlem yok.</p>'; return; }
    var w=520, rowH=28, pad={t:6,r:60,b:6,l:56}; var h=pad.t+pad.b+data.length*rowH;
    var maxAbs=Math.max.apply(null,data.map(function(d){return Math.abs(d.v);}))*1.25 || 1;
    var zero=pad.l+(w-pad.l-pad.r)/2; var scale=(w-pad.l-pad.r)/2/maxAbs;
    var svg=svgEl('svg',{viewBox:'0 0 '+w+' '+h,width:'100%',height:h,role:'img','aria-label':'Pozisyon kâr zarar karşılaştırması'});
    svg.appendChild(svgEl('line',{x1:zero,x2:zero,y1:pad.t,y2:h-pad.b,stroke:'var(--border-strong)','stroke-width':1.4}));
    var tip=document.createElement('div'); tip.className='tooltip';
    data.forEach(function(d,i){
      var cy=pad.t+i*rowH+rowH/2; var barW=Math.abs(d.v)*scale; var barX=d.v>=0?zero:zero-barW;
      var color=d.v>=0?success:danger;
      var lbl=svgEl('text',{x:pad.l-8,y:cy+3.5,'text-anchor':'end','font-size':11,'font-weight':600,fill:'var(--ink-2)'}); lbl.textContent=d.c; svg.appendChild(lbl);
      var bar=svgEl('rect',{x:barX,y:cy-7,width:Math.max(barW,2),height:14,rx:4,fill:color}); bar.style.cursor='pointer';
      bar.addEventListener('mousemove', function(e){ var wrapRect=wrap.getBoundingClientRect(); tip.style.left=(e.clientX-wrapRect.left)+'px'; tip.style.top=(e.clientY-wrapRect.top)+'px'; tip.textContent=d.c+'/USDT  '+pnlStr(d.v); tip.classList.add('show'); });
      bar.addEventListener('mouseleave', function(){ tip.classList.remove('show'); });
      svg.appendChild(bar);
      var vlbl=svgEl('text',{x:(d.v>=0?barX+barW+6:barX-6),y:cy+3.5,'text-anchor':d.v>=0?'start':'end','font-size':10.5,fill:color}); vlbl.textContent=(d.v>=0?'+':'')+fmt(d.v,4); svg.appendChild(vlbl);
    });
    wrap.appendChild(svg); wrap.appendChild(tip);
  }

  // ---------------- donut (GERÇEK kazanan/kaybeden) ----------------
  function renderDonut(){
    var wrap=document.getElementById('donutWrap');
    wrap.innerHTML='';
    var k = D.kpi;
    var win = k? (k.win_count||0):0, loss = k? (k.loss_count||0):0, total=win+loss;
    if(!total){ wrap.innerHTML='<p style="padding:10px;color:var(--ink-3);font-size:12.5px;">Henüz kapanmış işlem yok.</p>'; return; }
    var size=200, stroke=22, r=(size-stroke)/2, c=size/2, circ=2*Math.PI*r, winFrac=win/total;
    var svg=svgEl('svg',{viewBox:'0 0 '+size+' '+size,width:size,height:size,role:'img','aria-label':'Kazanan kaybeden oranı'});
    svg.appendChild(svgEl('circle',{cx:c,cy:c,r:r,fill:'none',stroke:'var(--border)','stroke-width':stroke}));
    svg.appendChild(svgEl('circle',{cx:c,cy:c,r:r,fill:'none',stroke:success,'stroke-width':stroke,'stroke-dasharray':(circ*winFrac)+' '+circ,'stroke-linecap':'round',transform:'rotate(-90 '+c+' '+c+')'}));
    svg.appendChild(svgEl('circle',{cx:c,cy:c,r:r,fill:'none',stroke:danger,'stroke-width':stroke,'stroke-dasharray':(circ*(1-winFrac))+' '+circ,'stroke-dashoffset':-(circ*winFrac),'stroke-linecap':'round',transform:'rotate(-90 '+c+' '+c+')'}));
    var pct=svgEl('text',{x:c,y:c-2,'text-anchor':'middle','font-size':26,'font-weight':700,fill:'var(--ink)','font-family':'Chakra Petch'}); pct.textContent='%'+fmt(winFrac*100,1); svg.appendChild(pct);
    var lbl=svgEl('text',{x:c,y:c+18,'text-anchor':'middle','font-size':10.5,'font-weight':700,fill:'var(--ink-3)'}); lbl.textContent='başarı oranı'; svg.appendChild(lbl);
    var row=document.createElement('div'); row.style.cssText='display:flex;align-items:center;gap:18px;flex-wrap:wrap;justify-content:center;';
    var legend=document.createElement('div'); legend.style.cssText='display:flex;flex-direction:column;gap:8px;font-size:13px;font-weight:700;';
    legend.innerHTML='<div style="display:flex;align-items:center;gap:7px;"><span style="width:10px;height:10px;border-radius:3px;background:'+success+';display:inline-block;"></span>'+win+' kazanan</div>'+
      '<div style="display:flex;align-items:center;gap:7px;"><span style="width:10px;height:10px;border-radius:3px;background:'+danger+';display:inline-block;"></span>'+loss+' kaybeden</div>';
    row.appendChild(svg); row.appendChild(legend); wrap.appendChild(row);
  }

  // ---------------- tables (GERÇEK) ----------------
  function renderTrades(){
    var tbody=document.getElementById('tradesBody');
    var trades = D.recent_trades||[];
    if(!trades.length){ tbody.innerHTML='<tr><td colspan="5" style="color:var(--ink-3);">Henüz kapanmış işlem yok.</td></tr>'; return; }
    tbody.innerHTML = trades.map(function(t){
      var pnl=t.gross_realized_pnl, up = pnl>=0;
      return '<tr><td><div class="row-coin"><span class="avatar" style="background:'+colorFor(t.symbol)+';">'+t.symbol.slice(0,2)+'</span>'+short(t.symbol)+'</div></td>'+
        '<td class="num">'+fmt(t.gross_entry_vwap,decFor(t.gross_entry_vwap))+'</td><td class="num">'+fmt(t.exit_gross_vwap,decFor(t.exit_gross_vwap))+'</td>'+
        '<td class="num" style="font-weight:700;color:'+(up?success:danger)+'">'+pnlStr(pnl)+'</td>'+
        '<td style="color:var(--ink-2);">'+esc(t.exit_reason_tr||'—')+'</td></tr>';
    }).join('');
  }

  function renderOpenTable(){
    var openBody=document.getElementById('openBody');
    document.getElementById('openCountNote').textContent = positions.length+' pozisyon — bot fiyatı otomatik izliyor';
    if(!positions.length){ openBody.innerHTML='<tr><td colspan="7" style="color:var(--ink-3);">Şu anda açık gerçek Testnet işlemi yok.</td></tr>'; return; }
    var stateBadge = {LONG:['pill-open','● Açık'], ENTRY_PENDING:['pill-wait','⏳ Alış Bekleniyor'], EXIT_PENDING:['pill-wait','⏳ Satış Bekleniyor'],
      AMBIGUOUS:['pill-wait','🔍 Kontrol Ediliyor'], RECOVERED:['pill-wait','↻ Aktifleşme Bekliyor']};
    openBody.innerHTML = positions.map(function(p){
      var badge = stateBadge[p.state] || ['pill-wait', esc(p.state_tr||p.state)];
      var priceTxt = p.latest_price===null||p.latest_price===undefined?'Hesaplanamıyor':fmt(p.latest_price,decFor(p.latest_price));
      var valTxt = p.position_value===null||p.position_value===undefined?'Hesaplanamıyor':fmt(p.position_value,4);
      return '<tr><td><div class="row-coin"><span class="avatar" style="background:'+colorFor(p.symbol)+';">'+p.symbol.slice(0,2)+'</span>'+short(p.symbol)+'</div></td>'+
        '<td><span class="pill '+badge[0]+'">'+badge[1]+'</span></td>'+
        '<td class="num">'+priceTxt+'</td><td class="num">'+fmt(p.gross_entry_vwap,decFor(p.gross_entry_vwap))+'</td><td class="num">'+valTxt+'</td>'+
        '<td class="num" style="font-weight:700;color:'+((p.unrealized_gross_pnl||0)>=0?success:danger)+'">'+pnlStr(p.unrealized_gross_pnl)+'</td>'+
        '<td>'+(p.trailing_active?'<span class="pill pill-open">✓ Aktif</span>':'<span style="color:var(--ink-3);font-size:12.5px;">Henüz aktif değil</span>')+'</td></tr>';
    }).join('');
  }

  function renderDustTable(){
    var dustBody=document.getElementById('dustBody');
    var dust = D.dust||[];
    document.getElementById('dustCountNote').textContent = dust.length+' kalıntı — satılamayacak kadar küçük, hata değildir';
    if(!dust.length){ dustBody.innerHTML='<tr><td colspan="4" style="color:var(--ink-3);">Şu anda çok küçük kalan bakiye yok.</td></tr>'; return; }
    dustBody.innerHTML = dust.map(function(p){
      var valTxt = p.position_value===null||p.position_value===undefined?'Hesaplanamıyor':fmt(p.position_value,4)+' USDT';
      return '<tr><td><div class="row-coin"><span class="avatar" style="background:'+colorFor(p.symbol)+';">'+p.symbol.slice(0,2)+'</span>'+short(p.symbol)+'</div></td>'+
        '<td class="num">'+fmt(p.net_owned_base_quantity,6)+'</td><td class="num">'+valTxt+'</td><td style="color:var(--ink-2);">'+esc(p.last_exit_reason_tr||'—')+'</td></tr>';
    }).join('');
  }

  // ---------------- scan tiles + tag list (GERÇEK) ----------------
  function renderScanTiles(){
    var s = D.scan;
    var grid = document.getElementById('scanGrid');
    if(!s){ grid.innerHTML=''; document.getElementById('scanTagList').innerHTML=''; return; }
    var nextTxt = '—';
    if(s.next_scan_at){ try{ nextTxt = new Date(s.next_scan_at).toLocaleTimeString('tr-TR',{hour:'2-digit',minute:'2-digit'}); }catch(e){} }
    grid.innerHTML =
      '<div class="scan-tile"><div class="svc-label">Taranan</div><div class="v num">'+(s.scanned_count===null||s.scanned_count===undefined?'—':s.scanned_count)+'</div></div>'+
      '<div class="scan-tile"><div class="svc-label">Yeni seçilen fırsat</div><div class="v num">'+s.new_opportunities.length+'</div></div>'+
      '<div class="scan-tile"><div class="svc-label">Korunan</div><div class="v num">'+s.protected_symbols.length+'</div></div>'+
      '<div class="scan-tile"><div class="svc-label">Sonraki tarama</div><div class="v num" style="font-size:16px;">'+nextTxt+'</div></div>';
    var tagList=document.getElementById('scanTagList');
    var learned = s.learned_factors || {};
    tagList.innerHTML = s.new_opportunities.length ? s.new_opportunities.map(function(sym){
      var score = learned[sym];
      var scoreTxt = (score===null||score===undefined||isNaN(score)) ? '' : ' · öğr. '+fmt(score,2);
      return '<span class="tag">'+esc(sym)+scoreTxt+'</span>';
    }).join('') : '<span style="color:var(--ink-3);font-size:12.5px;">Bu taramada yeni fırsat seçilmedi.</span>';
  }

  // ---------------- Control Center: health-card + svc-grid + maint-list (GERÇEK + dürüst "Faz 2" yer tutucular) ----------------
  function renderControlCenter(){
    var card=document.getElementById('healthCard');
    var degraded = D.degraded_symbols||[];
    if(!D.bot_running){
      card.style.borderColor='rgba(255,90,95,.35)'; card.style.background='linear-gradient(120deg,var(--danger-soft),var(--surface))'; card.style.color='var(--danger-soft-ink)';
      card.innerHTML='<span class="health-ic">✕</span><div><div class="health-title">Bot çalışmıyor</div><div class="health-sub">overall_health: '+esc(D.overall_health)+'</div></div>';
    } else if(degraded.length){
      card.style.borderColor='rgba(242,201,76,.3)'; card.style.background='linear-gradient(120deg,var(--warning-soft),var(--surface))'; card.style.color='var(--warning-soft-ink)';
      card.innerHTML='<span class="health-ic">⚠</span><div><div class="health-title">Dikkat gerekiyor</div><div class="health-sub">'+degraded.length+' coin için piyasa verisi sorunu var</div></div>';
    } else {
      card.style.borderColor=''; card.style.background=''; card.style.color='';
      card.innerHTML='<span class="health-ic">✓</span><div><div class="health-title">Her şey normal çalışıyor</div><div class="health-sub">Son kontrol: '+(D.generated_at?new Date(D.generated_at).toLocaleTimeString('tr-TR',{hour:'2-digit',minute:'2-digit'}):'—')+'</div></div>';
    }

    var svc=document.getElementById('svcGrid');
    var botLed = D.bot_running?'led-ok':'led-warn', botTxt=D.bot_running?'Çalışıyor':'Çalışmıyor';
    var tnLed = D.testnet_ready?'led-ok':'led-warn', tnTxt = !D.testnet_enabled?'Devre dışı':(D.testnet_ready?'Hazır':'Hazır değil');
    var admin = D.admin || {enabled:false};
    var pausedLed = admin.entries_paused ? 'led-warn' : 'led-ok';
    var pausedTxt = !admin.enabled ? 'Control Center devre dışı' : (admin.entries_paused ? 'Yeni girişler DURAKLATILDI' : 'Yeni girişler aktif');

    // Step A2 — usdt_balance: None-safe, ASLA uydurma bir sayı göstermez.
    var bal = D.usdt_balance;
    var balTxt = 'bilinmiyor';
    if(bal && bal.free!==null && bal.free!==undefined && !isNaN(bal.free)){
      balTxt = fmt(bal.free,2)+' '+(bal.asset||'USDT');
      if(bal.locked!==null && bal.locked!==undefined && !isNaN(bal.locked) && bal.locked>0){
        balTxt += ' (+'+fmt(bal.locked,2)+' kilitli)';
      }
    }

    // Step A7 — backup: gerçek son yedekleme zamanı, veya dürüst "bulunamadı" durumu.
    var backup = D.backup || {available:false};
    var backupTxt, backupLed;
    if(!backup.available){
      backupTxt = 'Yedekleme çalışmadı / bulunamadı'; backupLed = 'led-warn';
    } else {
      var bt = '—';
      try{ bt = new Date(backup.completed_at).toLocaleString('tr-TR'); }catch(e){}
      backupTxt = bt; backupLed = 'led-ok';
    }

    svc.innerHTML =
      '<div class="svc-card"><div class="svc-label">Bot</div><div class="svc-value"><span class="led '+botLed+'"></span>'+botTxt+'</div></div>'+
      '<div class="svc-card"><div class="svc-label">Testnet Emir Sistemi</div><div class="svc-value"><span class="led '+tnLed+'"></span>'+tnTxt+'</div></div>'+
      '<div class="svc-card"><div class="svc-label">Yeni Giriş Durumu</div><div class="svc-value"><span class="led '+pausedLed+'"></span>'+pausedTxt+'</div></div>'+
      '<div class="svc-card"><div class="svc-label">USDT Bakiyesi</div><div class="svc-value num">'+esc(balTxt)+'</div></div>'+
      '<div class="svc-card"><div class="svc-label">Son yedekleme</div><div class="svc-value"><span class="led '+backupLed+'"></span>'+esc(backupTxt)+'</div></div>';

    var btnPauseEl=document.getElementById('btnPause'), btnResumeEl=document.getElementById('btnResume'), btnStopEl=document.getElementById('btnStop');
    if(btnPauseEl) btnPauseEl.disabled = !admin.enabled;
    if(btnResumeEl) btnResumeEl.disabled = !admin.enabled;
    if(btnStopEl) btnStopEl.disabled = !admin.enabled;

    var maint=document.getElementById('maintList');
    var rows=[];
    if(degraded.length){
      degraded.forEach(function(d){
        var t = d.last_event_at ? (function(){ try{ return new Date(d.last_event_at).toLocaleTimeString('tr-TR',{hour:'2-digit',minute:'2-digit'}); }catch(e){ return ''; } })() : '';
        rows.push('<div class="maint-row">⚠ <span class="maint-msg">'+esc(d.symbol)+' — '+esc(d.detail_tr)+'</span><span class="t num">'+t+'</span></div>');
      });
    }
    if(admin.enabled){
      if(admin.entries_paused){
        rows.push('<div class="maint-row">⏸ <span class="maint-msg">Yeni girişler admin tarafından DURAKLATILDI — açık pozisyonlar normal şekilde yönetiliyor.</span></div>');
      }
      if(admin.last_action){
        var lt = (function(){ try{ return new Date(admin.last_action.at).toLocaleString('tr-TR'); }catch(e){ return admin.last_action.at; } })();
        rows.push('<div class="maint-row">🛠 <span class="maint-msg">Son admin işlemi: <b>'+esc(admin.last_action.action)+'</b> — '+esc(admin.last_action.detail)+'</span><span class="t num">'+esc(lt)+'</span></div>');
      }
    }
    if(!rows.length) rows.push('<div class="maint-row">✓ <span class="maint-msg">Bilinen bir sorun yok.</span></div>');
    maint.innerHTML = rows.join('');
  }

  // ---------------- Adaptive Intelligence (Step A3 — GERÇEK, veya dürüst "aktif değil" durumu) ----------------
  function renderAdaptiveCard(){
    var wrap=document.getElementById('adaptiveCard');
    var a = D.adaptive || {active:false};
    if(!a.active){
      wrap.innerHTML = '<p style="font-size:13px;color:var(--ink-3);">Adaptive Intelligence bu çalıştırmada aktif değil (scripts/run_with_adaptive_policy.py ile başlatılmadı).</p>';
      return;
    }
    var html='';
    var c = a.champion;
    if(!c){
      html += '<p style="font-size:13px;color:var(--ink-3);">Henüz bir champion promote edilmedi (bootstrap bekleniyor).</p>';
    } else {
      html += '<div class="scan-grid">'+
        '<div class="scan-tile"><div class="svc-label">Champion</div><div class="v" style="font-size:13px;overflow-wrap:break-word;">'+esc(c.version_id)+'</div></div>'+
        '<div class="scan-tile"><div class="svc-label">Stop ATR</div><div class="v num">'+fmt(c.stop_atr_multiple,2)+'</div></div>'+
        '<div class="scan-tile"><div class="svc-label">Take-Profit ATR</div><div class="v num">'+fmt(c.take_profit_atr_multiple,2)+'</div></div>'+
        '<div class="scan-tile"><div class="svc-label">Max Hold (saat)</div><div class="v num">'+fmt(c.max_hold_hours,1)+'</div></div>'+
        '</div>'+
        '<p style="font-size:12px;color:var(--ink-3);margin-top:10px;">Trailing aktivasyon '+fmt(c.trailing_activation_atr_multiple,2)+' ATR · Trailing mesafe '+fmt(c.trailing_distance_atr_multiple,2)+' ATR · kaynak: '+esc(c.provenance)+'</p>';
    }
    var ev = a.last_event;
    if(ev){
      var et='—'; try{ et=new Date(ev.promoted_at).toLocaleString('tr-TR'); }catch(e){}
      var kind = ev.is_rollback ? 'ROLLBACK' : 'Promotion';
      html += '<p style="font-size:12.5px;color:var(--ink-2);margin-top:12px;"><b>Son olay ('+esc(kind)+'):</b> '+esc(ev.version_id)+' — '+esc(ev.reason)+' <span style="color:var(--ink-3);">('+et+')</span></p>';
    }
    wrap.innerHTML = html;
  }

  // ---------------- Olay Geçmişi (Step A9 — ops_event_log'dan GERÇEK, en yeni önce) ----------------
  var EVENT_TYPE_TR = {
    admin_pause:'Admin: Duraklatıldı', admin_resume:'Admin: Devam Ettirildi', admin_stop:'Admin: Durduruldu',
    daily_loss_breaker_tripped:'Günlük zarar sigortası tetiklendi', adaptive_rollback:'Adaptive rollback'
  };
  function renderEventLog(){
    var wrap=document.getElementById('eventLogList');
    var events = D.events || [];
    if(!events.length){ wrap.innerHTML = '<p style="font-size:13px;color:var(--ink-3);">Henüz kayıtlı bir olay yok.</p>'; return; }
    wrap.innerHTML = events.map(function(e){
      var t='—'; try{ t=new Date(e.occurred_at).toLocaleString('tr-TR'); }catch(err){}
      var label = EVENT_TYPE_TR[e.event_type] || e.event_type;
      return '<div class="log-row"><span class="log-time num">'+esc(t)+'</span><span class="log-badge">'+esc(label)+'</span><span class="log-msg">'+esc(e.detail)+'</span></div>';
    }).join('');
  }

  // ============================================================
  // TÜM görünümü verilen bir görünüm modeliyle YENİDEN çizer — hem İLK
  // yüklemede (gömülü `window.__DASHBOARD_DATA__` ile) hem de HER
  // başarılı poll'da (taze `/api/status` ile) ÇAĞRILAN TEK giriş noktası.
  // Seçili coin (`selected`) ve mum zaman dilimi (`currentTf`) poll'lar
  // arasında KORUNUR (operatörün seçimini KAYBETMEZ) — yalnızca seçili
  // sembol artık listede YOKSA ilk pozisyona (veya hiçbiri yoksa null'a)
  // düşer.
  // ============================================================
  function applyViewModel(newD){
    D = newD;
    positions = D.positions || [];
    if(!(selected && positions.some(function(p){ return p.symbol===selected; }))){
      selected = positions.length ? positions[0].symbol : null;
    }
    renderTopStats();
    renderFreshness();
    renderWarnBanner();
    renderKpis();
    renderCoinList();
    renderChart();
    renderTicker();
    renderBar();
    renderDonut();
    renderTrades();
    renderOpenTable();
    renderDustTable();
    renderScanTiles();
    renderControlCenter();
    renderAdaptiveCard();
    renderEventLog();
  }

  // ============================================================
  // CANLI POLLING — AYNI `/api/status` endpoint'i (sayfanın ilk
  // render'ını besleyen TEK gerçek veri kaynağı) periyodik olarak
  // yeniden çekilir. Üst üste binen istek YOK (`inFlight` koruması);
  // başarısız bir poll ekranı ASLA boşaltmaz, yalnızca "veri güncel
  // değil" göstergesini günceller ve bir sonraki interval'da yeniden
  // dener.
  // ============================================================
  var POLL_INTERVAL_MS = 7000;
  var inFlight = false;
  function pollOnce(){
    if(inFlight) return;
    inFlight = true;
    fetch('/api/status', {cache:'no-store'}).then(function(resp){
      if(!resp.ok) throw new Error('HTTP '+resp.status);
      return resp.json();
    }).then(function(raw){
      lastPollOk = true;
      lastUpdateAt = Date.now();
      applyViewModel(buildViewModel(raw));
    }).catch(function(){
      lastPollOk = false;
      renderFreshness();
    }).then(function(){
      inFlight = false;
    });
  }

  applyViewModel(D);
  setInterval(pollOnce, POLL_INTERVAL_MS);
  setInterval(renderFreshness, 1000);

})();
</script>
"""


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    """Yalnızca GET destekler. POST/PUT/DELETE/PATCH HER ZAMAN 405 döner —
    mutasyon için HİÇBİR kod yolu YOKTUR (bkz. modül docstring'i)."""

    server: _DashboardHTTPServer  # type: ignore[assignment]
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        _LOGGER.debug("dashboard request: %s - %s", self.address_string(), format % args)

    def _write_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _write_html(self, status: HTTPStatus, page: str) -> None:
        body = page.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        try:
            snapshot_path = self.server.health_snapshot_path
            if self.path == _HEALTHZ_PATH:
                self._write_json(HTTPStatus.OK, {"dashboard": "ok"})
                return
            snapshot = _snapshot_or_none(snapshot_path)
            if self.path == _STATUS_PATH:
                if snapshot is None:
                    self._write_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "no snapshot yet"})
                    return
                self._write_json(HTTPStatus.OK, snapshot)
                return
            if self.path == _INDEX_PATH:
                self._write_html(HTTPStatus.OK, render_dashboard_html(snapshot, health_snapshot_path=snapshot_path))
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError):
            # Yavaş/kopan bir telefon istemcisi — dashboard'un/thread'in
            # KENDİSİNİ etkilemez, sadece bu tek isteği sonlandırır.
            pass
        except Exception:  # noqa: BLE001 - dashboard bir istek için ASLA process'i çökertmemeli
            _LOGGER.error("dashboard request handler hatası", exc_info=True)
            with contextlib.suppress(Exception):
                self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal error"})

    def _reject_mutation(self) -> None:
        self._write_json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "read-only dashboard: no mutation endpoints exist"})

    def do_POST(self) -> None:  # noqa: N802
        # 24/7 Ops v1, Step 3 — Control Center. The ONLY three mutation
        # endpoints that exist anywhere in this dashboard, and ONLY when
        # `AdminController` was actually constructed (dashboard_admin_
        # token set) — every other path (including these three, when
        # `admin_controller is None`) falls straight through to the
        # UNCHANGED `_reject_mutation()` 405.
        admin = self.server.admin_controller
        if self.path in (_ADMIN_PAUSE_PATH, _ADMIN_RESUME_PATH, _ADMIN_STOP_PATH) and admin is not None:
            self._handle_admin_action(admin)
            return
        self._reject_mutation()

    def _handle_admin_action(self, admin: AdminController) -> None:
        provided = self.headers.get(ADMIN_TOKEN_HEADER)
        if not admin.check_token(provided):
            # The ATTEMPT is logged — NEVER the token value itself.
            _LOGGER.warning(
                "admin action REJECTED (missing/invalid token): path=%s client=%s",
                self.path, self.address_string(),
            )
            self._write_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        if self.path == _ADMIN_PAUSE_PATH:
            result = admin.pause()
        elif self.path == _ADMIN_RESUME_PATH:
            result = admin.resume()
        else:
            result = admin.stop()
        self._write_json(HTTPStatus.OK, result)

    def do_PUT(self) -> None:  # noqa: N802
        self._reject_mutation()

    def do_DELETE(self) -> None:  # noqa: N802
        self._reject_mutation()

    def do_PATCH(self) -> None:  # noqa: N802
        self._reject_mutation()


class _DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self, address: tuple[str, int], health_snapshot_path: Path, admin_controller: AdminController | None
    ) -> None:
        super().__init__(address, _DashboardRequestHandler)
        self.health_snapshot_path = health_snapshot_path
        self.admin_controller = admin_controller


class DashboardServer:
    """`ThreadingHTTPServer`'ı AYRI bir daemon thread'inde çalıştıran ince
    bir sarmalayıcı. `start()`/`stop()` idempotent'tir; `start()` bir bind
    hatasıyla (örn. port zaten kullanımda) BAŞARISIZ olursa çağıran
    (`app.py`) bunu YAKALAYIP dashboard'u devre dışı bırakarak devam
    edebilir — runtime'ın KENDİSİ bu yüzden ASLA durmaz."""

    def __init__(self, *, host: str, port: int, health_snapshot_path: Path) -> None:
        self._host = host
        self._port = port
        self._health_snapshot_path = health_snapshot_path
        self._server: _DashboardHTTPServer | None = None
        self._thread: threading.Thread | None = None
        # 24/7 Ops v1, Step 3 — `None` unless `Application.start()` sets
        # it (only when `dashboard_admin_token` is configured). Must be
        # set BEFORE `start()` is called — `_DashboardHTTPServer` reads
        # it once, at construction.
        self._admin_controller: AdminController | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}/"

    def set_admin_controller(self, admin_controller: AdminController) -> None:
        self._admin_controller = admin_controller

    def start(self) -> None:
        if self._server is not None:
            return
        server = _DashboardHTTPServer((self._host, self._port), self._health_snapshot_path, self._admin_controller)
        thread = threading.Thread(target=server.serve_forever, name="cse-dashboard", daemon=True)
        thread.start()
        self._server = server
        self._thread = thread
        _LOGGER.info("dashboard listening on %s (loopback-only unless explicitly reconfigured)", self.url)

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._server = None
        self._thread = None


=== FILE: crypto_signal_engine/ops/errors.py ===
"""Faz 8 operasyon-katmanı exception taksonomisi."""

from __future__ import annotations

from crypto_signal_engine.errors import CryptoSignalEngineError


class OpsError(CryptoSignalEngineError):
    """Faz 8 (operations/deployment) kaynaklı tüm hataların ortak atası."""


class AppConfigurationError(OpsError):
    """Geçersiz/eksik ortam-değişkeni konfigürasyonu — fail-fast için kullanılır."""


class ConcurrentInstanceError(OpsError):
    """Aynı durable store'a karşı ikinci bir çalışan instance tespit edildi
    (process-lock zaten başka bir process tarafından tutuluyor)."""


class StartupError(OpsError):
    """Uygulama normal runtime işlemeye BAŞLAYAMADI (recovery/bootstrap
    başarısız oldu, şema uyuşmazlığı, corrupt state, vb.). Bu hata
    fırlatıldığında runtime KESİNLİKLE başlatılmamış olmalıdır."""


=== FILE: crypto_signal_engine/ops/event_log.py ===
"""
UI Polish / Final Acceptance v1, Step A9 — a small, append-only event log
replacing the dashboard's honest "Faz 2" Olay Geçmişi stub.

This module invents NO new event detection: it persists events that are
ALREADY detected at three existing call sites, using the SAME trigger
points as 24/7 Ops v1's `safe_notify()` wiring (Karar 99) — admin pause/
resume/stop (`ops/admin.py::AdminController`), the daily-loss-breaker
TRANSITION into tripped (`execution/lifecycle_manager.py::entry_gate()`),
and an adaptive rollback event (`adaptive/rollback.py::
apply_rollback_if_needed`). Each call site adds ONE additional, defensive
`record_event()` call alongside its existing `safe_notify()` call — never
replacing it.

Storage: a small, DEDICATED SQLite file (`ops_events.db`, next to the
main durable state — see `app.py::Application._event_log_path`), never
sharing a schema/connection with `paper_state.db` or the Testnet
execution DB. A short-lived `sqlite3.connect()` per call (these events
fire at most a few times per day — a pause/resume, a rare breaker trip,
a rarer rollback) — no persistent connection object needs to be threaded
through `LifecycleManager`/`AdminController`/`adaptive/rollback.py`.

`record_event()` NEVER raises — any `sqlite3.Error`/`OSError` is caught
and logged, mirroring `ops/notifier.py::safe_notify()`'s exact
discipline: a failure to write this log must never affect the
notification that already fired alongside it, and must never affect the
underlying decision (a breaker trip, a rollback, an admin action) that
was already applied before this function is even called."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

_LOGGER = logging.getLogger("crypto_signal_engine.ops.event_log")

_CREATE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS ops_event_log ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "occurred_at TEXT NOT NULL, "
    "event_type TEXT NOT NULL, "
    "detail TEXT NOT NULL"
    ")"
)


def record_event(db_path: Path, *, event_type: str, detail: str, occurred_at: datetime) -> None:
    """The ONE write path for `ops_event_log`. NEVER raises — see module
    docstring. Callers pass this function's own `db_path` (never a
    shared/long-lived connection) so this stays a true fire-and-forget
    write, just like `safe_notify()`."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(db_path))
        try:
            with connection:
                connection.execute(_CREATE_TABLE_SQL)
                connection.execute(
                    "INSERT INTO ops_event_log (occurred_at, event_type, detail) VALUES (?, ?, ?)",
                    (occurred_at.isoformat(), event_type, detail),
                )
        finally:
            connection.close()
    except (sqlite3.Error, OSError):
        _LOGGER.warning("ops_event_log yazımı BAŞARISIZ (yoksayılıyor): event_type=%s", event_type, exc_info=True)


def recent_events(db_path: Path, *, limit: int = 20) -> list[dict[str, object]]:
    """Read path for the dashboard snapshot (see `app.py::Application.
    _event_log_snapshot`). Returns `[]` — NEVER raises — whenever the
    file/table doesn't exist yet (fresh install, no event ever recorded)
    or any read error occurs; the dashboard's existing "Olay Geçmişi"
    stub honestly shows "no events yet" for this same empty-list case,
    never fabricating history. Most-recent-first."""
    try:
        if not db_path.exists():
            return []
        connection = sqlite3.connect(str(db_path))
        try:
            connection.row_factory = sqlite3.Row
            cursor = connection.execute(
                "SELECT id, occurred_at, event_type, detail FROM ops_event_log ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            connection.close()
    except sqlite3.Error:
        _LOGGER.warning("ops_event_log okunamadı (yoksayılıyor)", exc_info=True)
        return []


=== FILE: crypto_signal_engine/ops/health_snapshot.py ===
"""
Faz 8 — sağlık anlık görüntüsü (health snapshot).

Tasarım kararı ("dashboard İNŞA ETME, en küçük tasarımı tercih et"):
localhost HTTP endpoint YOKTUR (ekstra dinleyen bir port/güvenlik yüzeyi
gerektirir) — bunun yerine `RuntimeCoordinator.status()` çıktısı + Faz 7
recovery sonucu, periyodik olarak yerel bir JSON dosyasına ATOMİK olarak
yazılır (`os.replace` ile geçici dosyadan taşıma — okuyan bir `status`
komutu asla yarım yazılmış bir dosya görmez). `crypto_signal_engine.app
status` komutu bu dosyayı okuyup insan-okur biçimde özetler.

Ne içerir: genel sağlık, sembol başına sağlık (son event zamanı,
reconnect sayısı, unresolved gap/persistence fault detayı varsa), ve
son recovery/startup durumu. Keyfi/rastgele internal state DÖKÜLMEZ."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.runtime.models import RuntimeStatus


def write_snapshot(
    path: Path,
    *,
    status: RuntimeStatus,
    recovery_ok: bool,
    recovery_detail: str,
    pid: int,
    started_at: datetime,
    execution: dict[str, object] | None = None,
    paper: dict[str, object] | None = None,
    symbol_selection: dict[str, object] | None = None,
    signal_testnet_bridge: dict[str, object] | None = None,
    bridge_lifecycle: dict[str, object] | None = None,
    candle_history: dict[str, object] | None = None,
    admin: dict[str, object] | None = None,
    adaptive: dict[str, object] | None = None,
    backup: dict[str, object] | None = None,
    event_log: dict[str, object] | None = None,
) -> None:
    """Anlık görüntüyü ATOMİK olarak yazar (aynı dizinde bir geçici dosyaya
    yazıp `os.replace` ile hedefe taşır — kısmi bir dosya asla görünmez).

    Faz 12 (`execution`/`paper`/`symbol_selection`, HEPSİ opsiyonel, KASITLI OLARAK): read-only
    dashboard'un (`ops/dashboard.py`) TEK veri kaynağı bu dosyadır — dashboard
    HTTP handler'ları (ayrı OS thread'lerinde çalışır) canlı Python
    nesnelerine ASLA doğrudan ERİŞMEZ, yalnızca bu ATOMİK JSON dosyasını
    okur (bkz. PHASE12 doc, Bölüm 6 "Use snapshots/read-only state"). Bu
    alanlar secret/credential İÇEREMEZ (`execution` yalnızca mode/enabled/
    ready/pending-count/detail taşır, ASLA imza kimlik bilgilerini/
    signature değerini)."""
    payload = {
        "generated_at": status.generated_at.isoformat(),
        "overall_health": status.overall_health.value,
        "pid": pid,
        "started_at": started_at.isoformat(),
        "recovery": {"ok": recovery_ok, "detail": recovery_detail},
        "symbols": [
            {
                "symbol": s.symbol,
                "health": s.health.value,
                "last_event_at": s.last_event_at.isoformat() if s.last_event_at is not None else None,
                "reconnect_count": s.reconnect_count,
                "detail": s.detail,
            }
            for s in status.symbols
        ],
        "execution": execution if execution is not None else {"mode": "PAPER", "enabled": False, "ready": False},
        "paper": paper if paper is not None else {},
        "symbol_selection": symbol_selection if symbol_selection is not None else {"mode": "MANUAL", "symbols": []},
        "signal_testnet_bridge": (
            signal_testnet_bridge if signal_testnet_bridge is not None else {"enabled": False, "operational": False}
        ),
        "bridge_lifecycle": bridge_lifecycle if bridge_lifecycle is not None else {"enabled": False, "positions": {}},
        # Dashboard visual rebuild — a compact, already-in-memory recent
        # candle window per (symbol, timeframe), reused from `RuntimeCoordinator.
        # _candle_windows` (bkz. `app.py::_candle_history_snapshot`) so the
        # dashboard's candlestick chart can use REAL market data without a
        # new persistence subsystem AND without the dashboard's HTTP thread
        # ever touching a live coordinator object directly (bkz. modül
        # docstring'i Bölüm 6 — bu, TEK yol: periyodik atomik snapshot).
        "candle_history": candle_history if candle_history is not None else {},
        # 24/7 Ops v1, Step 3 — Control Center admin state (additive,
        # optional; `{"enabled": False}` whenever `dashboard_admin_token`
        # is unset — see `ops/admin.py`). Contains NO secret: never the
        # admin token itself, only `entries_paused`/`last_action`.
        "admin": admin if admin is not None else {"enabled": False},
        # UI Polish v1, Step A3 — Adaptive Intelligence status (additive,
        # optional; `{"active": False}` whenever no `adaptive_status_
        # provider` was wired — see `app.py::Application._adaptive_
        # snapshot`). Contains only champion `ExitPolicyConfig` fields +
        # version_id + promotion/rollback history — never a credential.
        "adaptive": adaptive if adaptive is not None else {"active": False},
        # UI Polish v1, Step A7 — last-backup evidence (additive,
        # optional; `{"available": False}` whenever `backup_status.json`
        # is missing/unreadable — see `scripts/backup_sqlite.py` and
        # `app.py::Application._backup_snapshot`).
        "backup": backup if backup is not None else {"available": False},
        # UI Polish v1, Step A9 — recent ops_event_log rows (additive,
        # optional; `{"events": []}` whenever the table is empty/absent —
        # see `ops/event_log.py`).
        "event_log": event_log if event_log is not None else {"events": []},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".health-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


def read_snapshot(path: Path) -> dict[str, object] | None:
    """Anlık görüntüyü okur; dosya HENÜZ hiç yazılmamışsa `None` döner
    (bu, "runtime henüz başlamadı" ile "runtime bozuk" durumunu ayırt
    etmek için AÇIKÇA farklıdır — çağıran, `None` durumunu kendi mesajıyla
    ele alır)."""
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


=== FILE: crypto_signal_engine/ops/lock.py ===
"""
Faz 8 — process-lock: aynı durable SQLite store'a karşı YANLIŞLIKLA
ikinci bir çalışan instance'ın başlatılmasını engeller.

Tasarım kararı ("distributed lock sistemi İNŞA ETME, overengineer ETME"):
POSIX `flock(2)` (`fcntl.flock`, `LOCK_EX | LOCK_NB`) kullanılır. Bu,
tek-makine Ubuntu/systemd dağıtımı için YETERLİDİR ve iki önemli özelliği
KENDİLİĞİNDEN sağlar:

1. "Stale lock recovery güvenli olmalı": `flock`, bir *open file
   description*'a bağlıdır — kilidi tutan process (temiz kapansın ya da
   crash etsin FARK ETMEZ) sonlandığında, işletim çekirdeği kilidi
   OTOMATİK olarak serbest bırakır. Bu yüzden manuel bir "PID dosyası
   oku + process hâlâ canlı mı kontrol et" mantığına (kendi başına bir
   race-condition kaynağı) hiç ihtiyaç YOKTUR.
2. Deterministik/offline test edilebilir: `flock`, bir `LOCK_NB` isteğini
   tutulamıyorsa HEMEN (`BlockingIOError`) reddeder — gerçek bir ikinci
   process spawn etmeye gerek kalmadan, aynı process içinde AYRI iki
   dosya tanımlayıcısı açarak testte doğrulanabilir (bkz.
   tests/test_ops_lock.py — flock, aynı process içindeki farklı open
   file description'lar arasında da normal şekilde çalışır)."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

from crypto_signal_engine.ops.errors import ConcurrentInstanceError


class ProcessLock:
    """Belirli bir dosya yolu üzerinde tek-sahiplik (exclusive ownership)
    garanti eden, non-blocking bir `flock` sarmalayıcısı.

    Kullanım:
        lock = ProcessLock(path)
        lock.acquire()   # zaten tutuluyorsa ConcurrentInstanceError
        ...
        lock.release()   # idempotent — zaten serbestse hiçbir şey yapmaz
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> None:
        if self._fd is not None:
            raise ConcurrentInstanceError(f"lock zaten bu instance tarafından tutuluyor: {self._path}")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise ConcurrentInstanceError(
                f"Başka bir crypto-signal-engine instance'ı zaten bu durable store'u "
                f"kullanıyor (lock tutuluyor): {self._path}. Aynı SQLite state'ine "
                f"karşı iki instance'ın eşzamanlı çalışması güvenli DEĞİLDİR."
            ) from exc
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("ascii"))
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> ProcessLock:
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()


=== FILE: crypto_signal_engine/ops/logging_setup.py ===
"""
Faz 8 — loglama kurulumu.

Kural (Bölüm 9 — "yeni bir loglama soyutlaması İNŞA ETME"): yalnızca
stdlib `logging` kullanılır. Formatlanmış çıktı stdout'a yazılır —
systemd/journald bunu otomatik olarak yakalar (`journalctl -u
crypto-signal-engine`); ayrı bir dosya-tabanlı log handler'a GEREK
YOKTUR (operasyon dokümanı, isteğe bağlı olarak `StandardOutput=append:
...` ile dosyaya yönlendirmeyi ayrıca açıklar).

Ne loglanır (bkz. PHASE8_UBUNTU_OPERATIONS.md): başlangıç + config özeti
(secret İÇERMEZ, zaten hiç secret alanı yok) + recovery başlangıç/
başarı/başarısızlık + READY/DEGRADED geçişleri + WS disconnect/reconnect
+ gap tespiti/çözümü + persistence checkpoint hataları + paper pozisyon
open/close/reversal özeti + graceful shutdown + fatal exception'lar.

Ne loglanmaz: ham order book/candle payload'ları (sürekli/gürültülü),
hiçbir secret/credential (zaten hiçbiri yok)."""

from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging(level: str) -> None:
    """Kök `crypto_signal_engine` logger'ını yapılandırır.

    Idempotent: birden fazla çağrılırsa handler'lar TEKRARLANMAZ (mevcut
    handler'lar temizlenip yeniden kurulur) — testlerin veya bir
    yeniden-başlatma yolunun logu ikiye katlaması engellenir."""
    root = logging.getLogger("crypto_signal_engine")
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(handler)
    root.propagate = False


=== FILE: crypto_signal_engine/ops/notifier.py ===
"""
24/7 Ops v1, Step 4 — outbound-only alerting.

Scope (see this milestone's DECISIONS.md Karar): a MINIMAL `Notifier`
callable type plus ONE concrete implementation (Telegram bot API, plain
HTTP POST — no new pip dependency, `urllib` only, the SAME stdlib-HTTP
pattern already used in `providers/binance/transport.py`). This is
OUTBOUND-ONLY — there is no inbound command channel anywhere in this
module or its callers; a bot token/chat-id here can only ever be used to
PUSH a message, never to receive or act on one.

Credential handling (same precedent as Binance Testnet signing
credentials, see `execution/factory.py::testnet_credentials_status()`/
`build_testnet_execution_service()`): the bot token and chat id are read
directly from the process environment, OUTSIDE `AppConfig` — they must
never appear in `AppConfig.summary()`, in any log line, or in the health
snapshot (see `tests/test_repository_safety_scan.py`'s new credential-
leak tests for this milestone).

Every wiring site in this codebase calls a notifier ONLY through
`safe_notify()` below — never a bare `notifier(message)` — so a raising
(or merely slow/network-hanging) notifier can NEVER affect the caller's
own decision, pause state, or shutdown sequence: `safe_notify()` catches
any synchronous exception, and `build_telegram_notifier()`'s own
`notify()` callable does the actual HTTP POST on a short-lived background
thread (fire-and-forget, bounded timeout) so a slow/unreachable Telegram
API can never block a trading decision, an admin action, or the shutdown
path that calls it."""

from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping

_LOGGER = logging.getLogger("crypto_signal_engine.ops.notifier")

# `Notifier` is intentionally the smallest possible surface — one method,
# fire-and-forget, no return value, no async requirement (see module
# docstring: the concrete Telegram implementation is itself internally
# non-blocking, so callers may call it from sync OR async code alike).
Notifier = Callable[[str], None]

_TELEGRAM_BOT_TOKEN_ENV = "CSE_TELEGRAM_BOT_TOKEN"
_TELEGRAM_CHAT_ID_ENV = "CSE_TELEGRAM_CHAT_ID"
_DEFAULT_TIMEOUT_SECONDS = 5.0


def telegram_credentials_status(*, env: Mapping[str, str] | None = None) -> tuple[bool, bool]:
    """`(bot_token_present, chat_id_present)` — SET/UNSET only, mirrors
    `execution/factory.py::testnet_credentials_status()`'s EXACT
    redaction discipline (never returns/logs the actual values)."""
    source = env if env is not None else os.environ
    return bool(source.get(_TELEGRAM_BOT_TOKEN_ENV)), bool(source.get(_TELEGRAM_CHAT_ID_ENV))


def _send_telegram_message(*, bot_token: str, chat_id: str, text: str, timeout_seconds: float) -> None:
    """The ONE actual network call. Plain `urllib` POST to Telegram's
    public Bot API `sendMessage` endpoint — no SDK, no new dependency,
    same stdlib-only discipline as `providers/binance/transport.py`."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    request = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response.read()


def build_telegram_notifier(
    *, env: Mapping[str, str] | None = None, timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
) -> Notifier | None:
    """Returns `None` (no notifier configured) unless BOTH
    `CSE_TELEGRAM_BOT_TOKEN` and `CSE_TELEGRAM_CHAT_ID` are set — same
    "opt-in, None-by-default" discipline as every other optional hook in
    this codebase (`candle_observer`, `exit_policy_provider`, ...).
    `None` is a completely safe, expected, silent value everywhere a
    `Notifier | None` is threaded through — see `safe_notify()`.

    Deliberately named `CSE_*` while still living OUTSIDE `AppConfig`:
    the `CSE_` prefix here only signals "this project's own env var
    namespace", not "parsed by `ops/config.py`" — the same distinction
    `BINANCE_TESTNET_API_KEY`/`_SECRET` draw by NOT using the `CSE_`
    prefix at all. Kept as plain `CSE_*` (rather than mirroring the
    Binance naming) because a Telegram bot token is not an exchange
    credential and carries no execution risk if merely present in an
    operator's shell env — it is still kept out of `AppConfig` because
    `AppConfig.summary()` is logged wholesale on every startup, and a
    bot token must never appear in a log line (see this milestone's
    DECISIONS.md Karar)."""
    source = env if env is not None else os.environ
    bot_token = source.get(_TELEGRAM_BOT_TOKEN_ENV)
    chat_id = source.get(_TELEGRAM_CHAT_ID_ENV)
    if not bot_token or not chat_id:
        return None

    def notify(message: str) -> None:
        def _worker() -> None:
            try:
                _send_telegram_message(
                    bot_token=bot_token, chat_id=chat_id, text=message, timeout_seconds=timeout_seconds
                )
            except (urllib.error.URLError, OSError, ValueError) as exc:
                _LOGGER.warning("telegram notifier: message could NOT be delivered (ignored): %s", exc)

        threading.Thread(target=_worker, name="cse-notifier", daemon=True).start()

    return notify


def safe_notify(notifier: Notifier | None, message: str) -> None:
    """The ONE sanctioned call site every wiring point in this codebase
    uses — never call a `Notifier` directly. A `None` notifier (nothing
    configured) is a silent no-op. Any SYNCHRONOUS exception a notifier
    implementation raises (a custom/future one, not just the Telegram
    implementation above, which already never raises synchronously
    itself) is caught and logged here, never propagated — a notifier
    must NEVER affect trading, pausing, or shutdown (see this
    milestone's DECISIONS.md Karar and the per-site tests proving it)."""
    if notifier is None:
        return
    try:
        notifier(message)
    except Exception:  # noqa: BLE001 - a notifier must NEVER affect the caller, by design
        _LOGGER.warning("notifier raised synchronously — ignored", exc_info=True)


=== FILE: crypto_signal_engine/ops/systemd_notify.py ===
"""
24/7 Ops v1 — systemd watchdog heartbeat (`WATCHDOG=1` via `sd_notify`).

`ops/lock.py` already protects against a SECOND instance starting; it says
nothing about a FIRST instance that is still running but has gone HUNG
(event loop wedged, a task stuck forever) — systemd's own crash-restart
policy (`Restart=on-failure`) only fires on a process EXIT, never on a
process that is merely no longer making progress. `WatchdogSec=` closes
that gap: systemd requires a periodic `WATCHDOG=1` datagram on
`$NOTIFY_SOCKET` and, if none arrives within the configured interval,
kills and restarts the service exactly as if it had crashed.

Design decision ("no new dependency, no new abstraction"): rather than
add `python-systemd` (a C-extension dependency this offline-testable,
pure-stdlib codebase has never needed), this module speaks the trivial
`sd_notify` wire protocol directly — a `AF_UNIX SOCK_DGRAM` write of the
literal bytes `b"WATCHDOG=1"` to the path in `$NOTIFY_SOCKET`. This is
the SAME protocol `libsystemd`'s `sd_notify()` C call uses; no socket is
opened or kept — a short-lived datagram socket per heartbeat, mirroring
the "no persistent extra resource" discipline of the rest of `ops/`.

Total no-op, NEVER raises, whenever `$NOTIFY_SOCKET` is unset (local
dev, PAPER mode without systemd, every test) — same defensive discipline
as every other optional hook in this codebase (`candle_observer`,
`m1_candle_observer`, the notifier in `ops/notifier.py`)."""

from __future__ import annotations

import logging
import os
import socket

_LOGGER = logging.getLogger("crypto_signal_engine.ops.systemd_notify")

_WATCHDOG_PAYLOAD = b"WATCHDOG=1"
_NOTIFY_SOCKET_ENV = "NOTIFY_SOCKET"


def notify_watchdog(*, env: dict[str, str] | None = None) -> bool:
    """Sends one `WATCHDOG=1` heartbeat datagram. Returns `True` if a
    datagram was actually sent, `False` if `$NOTIFY_SOCKET` was unset
    (the normal case outside systemd) — the return value is for tests/
    observability only, never something a caller needs to branch on.

    `env` defaults to `os.environ` (real process environment); a caller
    may inject a fake mapping for deterministic, offline testing. NEVER
    raises: any socket error (permission, non-existent path, a systemd
    that stopped listening mid-run) is caught and logged at DEBUG —
    a missed heartbeat is not a reason to crash a live trading process,
    and systemd's own watchdog timeout is what should decide a genuine
    hang, not an exception from this best-effort notifier."""
    source = env if env is not None else os.environ
    socket_path = source.get(_NOTIFY_SOCKET_ENV)
    if not socket_path:
        return False
    # sd_notify protocol: a leading '@' denotes a Linux abstract-namespace
    # socket, addressed in Python by a leading NUL byte instead.
    if socket_path.startswith("@"):
        socket_path = "\0" + socket_path[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(_WATCHDOG_PAYLOAD, socket_path)
        return True
    except OSError as exc:
        _LOGGER.debug("systemd watchdog heartbeat gönderilemedi (yoksayılıyor): %s", exc)
        return False


=== FILE: crypto_signal_engine/paper_trading/__init__.py ===
"""
Faz 5 — Paper Trading / Simulation Engine.

Bu paket tamamen dahili bir muhasebe (bookkeeping) katmanıdır: hiçbir
gerçek Binance API çağrısı yapmaz, Testnet'e bağlanmaz, hiçbir API
key/secret almaz, hiçbir emir hiçbir yere gönderilmez (bkz.
SAFETY_INVARIANTS.md, tests/test_repository_safety_scan.py).

Yalnızca Faz 4'ün tamamlanmış `Signal` nesnelerini (crypto_signal_engine.
domain.models.Signal) tüketir; `ConsensusResult`/`RegimeContext`'e hiçbir
bağımlılığı yoktur.
"""

from __future__ import annotations


=== FILE: crypto_signal_engine/paper_trading/engine.py ===
"""
PaperTradingEngine — Faz 5 (Paper Trading / Simulation Engine).

Kapsam ve hard constraint'ler:
- Binance Testnet YOK, Mainnet execution YOK, private/signed Binance
  endpoint YOK, API key/secret YOK (bkz. SAFETY_INVARIANTS.md,
  tests/test_repository_safety_scan.py). Bu modül hiçbir network/Binance
  bağımlılığı içermez; tamamen in-memory bir muhasebe katmanıdır.
- `ALLOW_LIVE_TRADING = False` korunur; bu modül onu hiçbir şekilde
  import/mutate etmez.
- Yalnızca tamamlanmış `Signal` nesnelerini TÜKETİR (`crypto_signal_engine.
  domain.models.Signal`); `ConsensusResult`/`RegimeContext`'e hiç dokunmaz
  (Faz 4'ün regime keyword-only sözleşme sapması, DECISIONS.md Karar
  46/47, derinleştirilmez).
- Deterministic: aynı (Signal, MarketPriceSnapshot) girdi dizisi HER ZAMAN
  aynı order/fill/position/PnL sonucunu üretir — uuid/random/wall-clock
  hiçbir yerde kullanılmaz; tüm id'ler ve zaman damgaları girdiden türetilir.
- No-look-ahead: `price.as_of`, işlendiği sinyalin `timestamp`'inden SONRA
  olamaz; `price.symbol`, `signal.symbol`'e eşit olmalı; bir sembol için
  sinyaller KRONOLOJİK sırayla işlenmek zorundadır (geriye dönük/karışık
  sıralı sinyal reddedilir). İhlal `NoLookAheadViolationError` fırlatır.
- Idempotency: aynı (symbol, context_id) ikinci kez işlenirse state
  MUTATE EDİLMEZ; önceki sonuç `idempotent_replay=True` ile aynen
  döndürülür. Aynı context_id FARKLI bir Signal içeriğiyle gelirse
  `IdempotencyConflictError` fırlatılır (context_id'nin stabil bir
  provenance anahtarı olduğu varsayımı ihlal edildi).
- Atomic state transitions: `process_signal()` içindeki TÜM hesaplama
  (order/fill/position üretimi, validasyon) yerel değişkenler üzerinde
  tamamlanır; gerçek `self._states[symbol]` yalnızca hesaplama TAMAMEN
  başarılı olduğunda, çağrının SONUNDA güncellenir. Herhangi bir aşamada
  (örn. bir dataclass validation hatası) exception fırlarsa, `self._states`
  çağrı ÖNCESİNDEKİ hâliyle DEĞİŞMEDEN kalır — kısmi state hiçbir zaman
  gözlemlenemez (bkz. persistence/sqlite_store.py'deki dry-run-then-commit
  deseniyle aynı disiplin, burada in-memory karşılığı).
- Multi-symbol isolation: her sembolün state'i `self._states` sözlüğünde
  ayrı bir girdidir; bir sembolde reddedilen/hata veren bir sinyal başka
  hiçbir sembolün state'ini etkilemez.

Strateji (kasıtlı olarak basit ve deterministik — Faz 5 kapsamı):
- `SignalDirection` iki aktif gruba ayrılır: LONG grubu (STRONG_LONG/LONG/
  WEAK_LONG) -> hedef pozisyon LONG; SHORT grubu -> hedef pozisyon SHORT.
- NEUTRAL -> NO_ACTION (Faz 5 acceptance blocker düzeltmesi). NEUTRAL bir
  "hedef pozisyon FLAT" DEĞİLDİR — hiçbir PaperOrder/PaperFill üretmez,
  mevcut pozisyonu KAPATMAZ, realized/unrealized PnL'i DEĞİŞTİRMEZ ve
  `self._states`'e HİÇ dokunmaz (bkz. `_no_action_result`). Bu, doğrudan
  `process_signal()` başında, LONG/SHORT akışından tamamen ayrı bir erken
  dönüş (early return) olarak uygulanır.
- Hedef pozisyon (yalnızca LONG/SHORT sinyaller için) mevcut pozisyonla
  AYNIYSA hiçbir yeni order/fill üretilmez (gereksiz churn önlenir);
  yalnızca pozisyonun `updated_at`'i bu çağrının fiyat zaman damgasına
  (`price.as_of`) yenilenir.
- Hedef pozisyon FARKLIYSA: önce mevcut pozisyon (varsa) TAMAMEN kapatılır
  (realized PnL hesaplanır ve kümülatif olarak taşınır), ardından sabit
  `notional_per_position` büyüklüğünde yeni pozisyon açılır. Bu iki adım
  TEK bir `process_signal()` çağrısında, atomik olarak uygulanır.
- Pozisyon büyüklüğü YALNIZCA `notional_per_position / fill_price` ile
  belirlenir — confidence/risk_level'a göre ölçeklendirme bu fazın kapsamı
  DIŞINDADIR (bilinçli bir basitleştirme, tech debt değil: gelecekte bir
  position-sizing politikası eklenmek istenirse, bu tek bir yerde —
  `_open_position` — genişletilebilir).

Deterministic fee/slippage modeli (acceptance blocker düzeltmesi, Karar 54):
- `PaperTradingEngine(fee_bps: float = 0.0, slippage_bps: float = 0.0)` —
  her ikisi de finite VE `>= 0` olmak ZORUNDADIR (negatif/NaN/±inf
  reddedilir). Varsayılan `0.0` değerleri, önceki (fee/slippage'sız) sıfır
  maliyetli davranışı BİREBİR korur.
- Fill fiyatı, YÖNE göre deterministik olarak kaydırılır (uuid/random/ağ/
  wall-clock KULLANILMAZ, yalnızca `price.price` ve `slippage_bps`'in saf
  bir fonksiyonudur):
  `BUY  fill_price = price.price * (1 + slippage_bps / 10000)`
  `SELL fill_price = price.price * (1 - slippage_bps / 10000)`
- Her `PaperFill.fee = abs(quantity * fill_price) * fee_bps / 10000` —
  fee, HAM `price.price` üzerinden değil, ACTUAL (slippage uygulanmış)
  `fill_price` üzerinden hesaplanır ve `PaperFill.fee` alanında KALICI
  olarak taşınır.
- Tüm entry/exit muhasebesi (`average_entry_price`, gross/realized PnL)
  ACTUAL `fill_price`'ı kullanır — HAM `price.price`'ı DEĞİL.
- Realized PnL, kapatılan bacağın giriş fee'sini VE çıkış fee'sini
  düşer (bkz. `_close_position`) — fee'ler yalnızca BİR KEZ, kapanış
  anında netleştirilir (double-count YOKTUR).
- NEUTRAL (NO_ACTION) hiçbir fill üretmediği için hiçbir fee/slippage
  UYGULANMAZ; idempotent replay hiçbir yeni fill üretmediği için hiçbir
  fee'yi İKİNCİ KEZ ÜCRETLENDİRMEZ (bkz. yukarı — idempotency).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

from crypto_signal_engine.domain._validation import normalize_symbol, require_finite
from crypto_signal_engine.domain.enums import SignalDirection
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError, NoLookAheadViolationError
from crypto_signal_engine.paper_trading.models import (
    MarketPriceSnapshot,
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)

_LONG_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_LONG, SignalDirection.LONG, SignalDirection.WEAK_LONG}
)
_SHORT_DIRECTIONS = frozenset(
    {SignalDirection.STRONG_SHORT, SignalDirection.SHORT, SignalDirection.WEAK_SHORT}
)


def _target_side(direction: SignalDirection) -> PositionSide:
    """LONG/SHORT sinyal yönünü hedef pozisyon yönüne eşler.

    YALNIZCA LONG/SHORT grubundaki yönler için çağrılabilir — NEUTRAL,
    `process_signal()` içinde bu fonksiyona hiç ulaşmadan, ayrı bir
    NO_ACTION erken dönüşüyle ele alınır (bkz. modül docstring'i)."""
    if direction in _LONG_DIRECTIONS:
        return PositionSide.LONG
    if direction in _SHORT_DIRECTIONS:
        return PositionSide.SHORT
    raise AssertionError(f"_target_side NEUTRAL için çağrılmamalı: {direction!r}")


def _slippage_adjusted_price(reference_price: float, side: OrderSide, slippage_bps: float) -> float:
    """Referans fiyata, YÖNE göre deterministik slippage uygular.

    BUY:  price * (1 + slippage_bps / 10000)
    SELL: price * (1 - slippage_bps / 10000)

    Rastgelelik/spread modeli/ağ/wall-clock YOKTUR — saf, deterministik bir
    fonksiyondur (`slippage_bps == 0.0` iken `reference_price`'ı BİREBİR
    değiştirmeden döner)."""
    if side is OrderSide.BUY:
        return reference_price * (1 + slippage_bps / 10000)
    return reference_price * (1 - slippage_bps / 10000)


def _fee_for_fill(quantity: float, fill_price: float, fee_bps: float) -> float:
    """`fee = abs(quantity * fill_price) * fee_bps / 10000` — ACTUAL
    (slippage uygulanmış) `fill_price` üzerinden, HAM referans fiyat
    üzerinden DEĞİL."""
    return abs(quantity * fill_price) * fee_bps / 10000


def _close_position(
    *,
    position: PaperPosition,
    entry_fee: float,
    signal: Signal,
    price: MarketPriceSnapshot,
    seq: int,
    fee_bps: float,
    slippage_bps: float,
) -> tuple[PaperOrder, PaperFill, PaperPosition]:
    """Mevcut (FLAT olmayan) bir pozisyonu tamamen kapatır.

    Realized PnL, GROSS (fill-price bazlı) kâr/zarardan hem bu kapanışın
    fee'sini HEM DE pozisyon açılırken ödenen `entry_fee`'yi düşerek NET
    olarak hesaplanır ve kümülatif `realized_pnl`'e eklenir — fee'ler
    yalnızca burada, bir kez netleştirilir (double-count YOKTUR)."""
    close_side = OrderSide.SELL if position.side is PositionSide.LONG else OrderSide.BUY
    fill_price = _slippage_adjusted_price(price.price, close_side, slippage_bps)
    fee = _fee_for_fill(position.quantity, fill_price, fee_bps)
    order_id = f"{signal.symbol}:{signal.context_id}:{seq}"

    order = PaperOrder(
        order_id=order_id,
        symbol=signal.symbol,
        side=close_side,
        quantity=position.quantity,
        signal_context_id=signal.context_id,
        created_at=price.as_of,
    )
    fill = PaperFill(
        fill_id=f"{order_id}:fill",
        order_id=order_id,
        symbol=signal.symbol,
        side=close_side,
        quantity=position.quantity,
        price=fill_price,
        fee=fee,
        filled_at=price.as_of,
    )

    if position.side is PositionSide.LONG:
        gross = (fill_price - position.average_entry_price) * position.quantity
    else:
        gross = (position.average_entry_price - fill_price) * position.quantity
    net = gross - entry_fee - fee

    new_position = PaperPosition(
        symbol=signal.symbol,
        side=PositionSide.FLAT,
        quantity=0.0,
        average_entry_price=0.0,
        realized_pnl=position.realized_pnl + net,
        updated_at=price.as_of,
    )
    return order, fill, new_position


def _open_position(
    *,
    symbol: str,
    side: PositionSide,
    notional: float,
    signal: Signal,
    price: MarketPriceSnapshot,
    seq: int,
    realized_pnl_carry: float,
    fee_bps: float,
    slippage_bps: float,
) -> tuple[PaperOrder, PaperFill, PaperPosition]:
    """FLAT bir pozisyondan hedef yönde (LONG/SHORT) yeni bir pozisyon açar.

    `average_entry_price`, ACTUAL (slippage uygulanmış) `fill_price`'tır —
    HAM `price.price` DEĞİL. Açılış fee'si burada TAHSİL EDİLMEZ (realized
    PnL'e hemen yansımaz) — çağıran taraf (`process_signal`) bu fill'in
    `fee`'sini, pozisyon kapatılana kadar `entry_fee` olarak SAKLAR ve o an
    net realized PnL hesabında kullanır (bkz. `_close_position`)."""
    order_side = OrderSide.BUY if side is PositionSide.LONG else OrderSide.SELL
    fill_price = _slippage_adjusted_price(price.price, order_side, slippage_bps)
    quantity = notional / fill_price
    fee = _fee_for_fill(quantity, fill_price, fee_bps)
    order_id = f"{symbol}:{signal.context_id}:{seq}"

    order = PaperOrder(
        order_id=order_id,
        symbol=symbol,
        side=order_side,
        quantity=quantity,
        signal_context_id=signal.context_id,
        created_at=price.as_of,
    )
    fill = PaperFill(
        fill_id=f"{order_id}:fill",
        order_id=order_id,
        symbol=symbol,
        side=order_side,
        quantity=quantity,
        price=fill_price,
        fee=fee,
        filled_at=price.as_of,
    )
    new_position = PaperPosition(
        symbol=symbol,
        side=side,
        quantity=quantity,
        average_entry_price=fill_price,
        realized_pnl=realized_pnl_carry,
        updated_at=price.as_of,
    )
    return order, fill, new_position


@dataclass
class _SymbolState:
    """Tek bir sembol için mutable internal state.

    PRIVATE — yalnızca `PaperTradingEngine` içinde kullanılır; dış koda HER
    ZAMAN immutable `PaperPosition`/`PaperTradingResult` snapshot'ları
    döner, bu sınıfın kendisi asla sızmaz.

    `entry_fee`: mevcut açık pozisyonun açılış fill'inde ödenen fee — bu
    pozisyon kapatılana kadar SAKLANIR ve o an net realized PnL hesabında
    (`_close_position`) kullanılır. Pozisyon FLAT iken anlamsızdır (bir
    sonraki açılışta o açılışın fee'siyle DEĞİŞTİRİLİR).
    """

    position: PaperPosition
    entry_fee: float = 0.0
    orders: tuple[PaperOrder, ...] = ()
    fills: tuple[PaperFill, ...] = ()
    processed_context_ids: dict[str, tuple[Signal, PaperTradingResult]] = field(default_factory=dict)
    last_signal_timestamp: datetime | None = None


class PaperTradingEngine:
    """Faz 4 `Signal` nesnelerini tüketen, dahili/deterministik bir paper
    trading (kağıt üzerinde işlem) simülasyon motoru.

    HİÇBİR gerçek Binance API çağrısı yapmaz, hiçbir API key/secret almaz,
    hiçbir emir hiçbir borsaya gönderilmez — tamamen in-memory bir
    muhasebe (bookkeeping) katmanıdır (bkz. modül docstring'i).
    """

    def __init__(
        self,
        *,
        notional_per_position: float = 1000.0,
        fee_bps: float = 0.0,
        slippage_bps: float = 0.0,
    ) -> None:
        if notional_per_position <= 0:
            raise ValueError("notional_per_position pozitif olmalı")
        require_finite(fee_bps, "fee_bps")
        if fee_bps < 0:
            raise ValueError("fee_bps negatif olamaz")
        require_finite(slippage_bps, "slippage_bps")
        if slippage_bps < 0:
            raise ValueError("slippage_bps negatif olamaz")
        self._notional_per_position = notional_per_position
        self._fee_bps = fee_bps
        self._slippage_bps = slippage_bps
        self._states: dict[str, _SymbolState] = {}

    def process_signal(
        self, signal: Signal, price: MarketPriceSnapshot, *, notional_override: float | None = None
    ) -> PaperTradingResult:
        """Bir `Signal`'i işler; gerekiyorsa order/fill üretip pozisyonu
        günceller. Tüm invariant kontrolleri ve hesaplama GERÇEK state'e
        dokunmadan önce tamamlanır (bkz. modül docstring'i — atomicity).

        PRE-AUDIT ENHANCEMENT PASS — additive, backward-compatible
        extension (bkz. PRE_AUDIT_ENHANCEMENTS.md, `research/sizing.py`):
        `notional_override`, keyword-only, opsiyonel, varsayılan `None`.
        `None` (varsayılan) iken davranış Faz 5'in kabul edilmiş
        sözleşmesiyle BİREBİR AYNIDIR — `self._notional_per_position`
        kullanılır, hiçbir yeni kod yolu tetiklenmez. Motorun kendi
        `notional_per_position` constructor-sabiti hiçbir şekilde mutate
        EDİLMEZ (sonraki çağrılar yine kendi varsayılanına döner).
        `notional_override` DEĞERİ yalnızca YENİ bir pozisyon AÇILIRKEN
        (`_open_position`) TÜKETİLİR.

        DOĞRULAMA NOKTASI (finite VE pozitif OLMAK ZORUNDADIR, aksi halde
        `ValueError` — fail-closed) — BLOCKER FİX (bağımsız acceptance
        review, iki tur; bkz. DECISIONS.md Karar 90/91):

        - Zaten işlenmiş, AYNI içerikli bir `(symbol, context_id)`
          (idempotent replay): `notional_override` HİÇ DOĞRULANMAZ —
          idempotency kontrolü (aşağı) BUNA ULAŞMADAN ÖNCE cached sonucu
          aynen döndürür. Aynı `context_id`'nin FARKLI (hatta geçersiz —
          `0`/`NaN`/`inf`/negatif) bir `notional_override` ile tekrar
          sunulması ASLA `ValueError` fırlatmaz VE ASLA ikinci bir
          tahsis ÜRETMEZ.
        - Zaten işlenmiş ama FARKLI bir Signal içeriğiyle çakışan bir
          `context_id` (conflicting replay): `notional_override`'a HİÇ
          BAKILMADAN `IdempotencyConflictError` fırlatılır.
        - Bunların İKİSİ de DEĞİLSE (yani bu KESİNLİKLE YENİ bir
          `(symbol, context_id)`'dir) — idempotency/conflict
          çözümlemesi TAMAMLANDIKTAN HEMEN SONRA, `target_side`
          hesaplanmadan/dallanmadan ÖNCE, KOŞULSUZ olarak doğrulanır.
          Bu, YENİ sinyalin nihayetinde bir açılış mı, bir reversal mı,
          yoksa aynı-yöndeki bir NO_ACTION tekrarı mı üreteceğinden
          TAMAMEN BAĞIMSIZDIR — Karar 90'ın çözdüğü ilk uç durumdan
          (idempotent replay) SONRA kalan ikinci uç durum TAM OLARAK
          buydu: aynı-yön tekrarı (`else` dalı, hiçbir yeni order/fill
          ÜRETMEZ) YENİ bir context için bile geçersiz bir override'ı
          doğrulamadan geçebiliyordu — artık geçemez."""
        if price.symbol != signal.symbol:
            raise NoLookAheadViolationError(
                f"price.symbol ({price.symbol}) != signal.symbol ({signal.symbol}) — "
                f"cross-symbol fiyat kabul edilmez (multi-symbol isolation)"
            )
        if price.as_of > signal.timestamp:
            raise NoLookAheadViolationError(
                f"price.as_of ({price.as_of}) signal.timestamp ({signal.timestamp}) "
                f"içinden SONRA olamaz (no-look-ahead ihlali)"
            )

        if signal.direction is SignalDirection.NEUTRAL:
            # NO_ACTION (Faz 5 acceptance blocker düzeltmesi): NEUTRAL asla
            # order/fill üretmez, mevcut pozisyonu kapatmaz, realized/
            # unrealized PnL'i değiştirmez ve `self._states`'e HİÇ dokunmaz.
            return self._no_action_result(signal)

        state = self._states.get(signal.symbol)

        if state is not None:
            cached_entry = state.processed_context_ids.get(signal.context_id)
            if cached_entry is not None:
                cached_signal, cached_result = cached_entry
                if cached_signal != signal:
                    raise IdempotencyConflictError(
                        f"context_id {signal.context_id!r} ({signal.symbol}) daha önce "
                        f"FARKLI bir Signal içeriğiyle işlenmiş — idempotency key'in "
                        f"stabil olduğu varsayımı ihlal edildi"
                    )
                # Idempotent replay: state MUTATE EDİLMEZ; bu çağrı hiçbir
                # yeni order/fill üretmedi (orders/fills boş döner), ama
                # geçerli pozisyon snapshot'ı aynen taşınır.
                return replace(cached_result, orders=(), fills=(), idempotent_replay=True)

            if state.last_signal_timestamp is not None and signal.timestamp < state.last_signal_timestamp:
                raise NoLookAheadViolationError(
                    f"{signal.symbol} için sinyaller kronolojik sırayla işlenmeli: "
                    f"yeni sinyal timestamp={signal.timestamp} < son işlenen "
                    f"timestamp={state.last_signal_timestamp}"
                )

        # BLOCKER FİX (Karar 91) — bkz. process_signal docstring'i: buraya
        # ulaşıldıysa (idempotent replay ERKEN dönüşü YAPILMADI, conflict
        # fırlatılMADI) bu KESİNLİKLE YENİ bir (symbol, context_id)'dir —
        # `notional_override` burada, KOŞULSUZ olarak doğrulanır. Bu, bu
        # sinyalin nihayetinde bir açılış/reversal ÜRETİP ÜRETMEYECEĞİNDEN
        # (aşağıdaki `target_side != current_side` dalı) TAMAMEN BAĞIMSIZDIR
        # — aynı-yön tekrarı (hiçbir yeni order/fill ÜRETMEYEN `else` dalı,
        # aşağı bkz.) DAHİL, YENİ bir context için geçersiz bir override HER
        # ZAMAN fail-closed olmalıdır (Karar 90'ın ÇÖZMEDİĞİ kalan uç durum).
        if notional_override is not None:
            require_finite(notional_override, "notional_override")
            if notional_override <= 0:
                raise ValueError("notional_override pozitif olmalı")

        current_position: PaperPosition | None = state.position if state is not None else None
        current_side = current_position.side if current_position is not None else PositionSide.FLAT
        current_entry_fee = state.entry_fee if state is not None else 0.0
        target_side = _target_side(signal.direction)

        new_orders: list[PaperOrder] = []
        new_fills: list[PaperFill] = []
        position = current_position
        new_entry_fee = current_entry_fee
        seq = 0

        if target_side != current_side:
            realized_pnl_carry = current_position.realized_pnl if current_position is not None else 0.0

            if current_side is not PositionSide.FLAT:
                assert current_position is not None
                order, fill, position = _close_position(
                    position=current_position,
                    entry_fee=current_entry_fee,
                    signal=signal,
                    price=price,
                    seq=seq,
                    fee_bps=self._fee_bps,
                    slippage_bps=self._slippage_bps,
                )
                new_orders.append(order)
                new_fills.append(fill)
                seq += 1
                realized_pnl_carry = position.realized_pnl

            if target_side is not PositionSide.FLAT:
                # `notional_override` bu noktaya ULAŞMADAN ÖNCE ZATEN
                # doğrulanmıştır (bkz. yukarı, Karar 91 — bu, YENİ bir
                # context için tek/erken doğrulama noktasıdır; burada
                # TEKRAR doğrulanmaz).
                order, fill, position = _open_position(
                    symbol=signal.symbol,
                    side=target_side,
                    notional=notional_override if notional_override is not None else self._notional_per_position,
                    signal=signal,
                    price=price,
                    seq=seq,
                    realized_pnl_carry=realized_pnl_carry,
                    fee_bps=self._fee_bps,
                    slippage_bps=self._slippage_bps,
                )
                new_orders.append(order)
                new_fills.append(fill)
                # Bu açılışın fee'si, pozisyon kapatılana kadar SAKLANIR —
                # bkz. `_close_position` (net realized PnL hesabı).
                new_entry_fee = fill.fee
        else:
            # Hedef, mevcut pozisyonla aynı (yalnızca LONG/SHORT tekrarı —
            # NEUTRAL zaten yukarıda ayrı ele alındığı için current_position
            # burada HER ZAMAN mevcuttur): churn üretilmez, yalnızca "en son
            # bu fiyatta gözlemlendi" damgası yenilenir. `entry_fee`
            # DEĞİŞMEZ (`new_entry_fee` zaten `current_entry_fee`'ye eşit).
            assert current_position is not None
            position = replace(current_position, updated_at=price.as_of)

        assert position is not None
        result = PaperTradingResult(
            symbol=signal.symbol,
            position=position,
            orders=tuple(new_orders),
            fills=tuple(new_fills),
            idempotent_replay=False,
        )

        if state is None:
            state = _SymbolState(position=position, entry_fee=new_entry_fee)
            self._states[signal.symbol] = state

        state.position = position
        state.entry_fee = new_entry_fee
        state.orders = state.orders + tuple(new_orders)
        state.fills = state.fills + tuple(new_fills)
        state.processed_context_ids[signal.context_id] = (signal, result)
        state.last_signal_timestamp = signal.timestamp

        return result

    def _no_action_result(self, signal: Signal) -> PaperTradingResult:
        """NEUTRAL sinyaller için NO_ACTION sonucu üretir.

        `self._states` burada YALNIZCA OKUNUR, hiçbir şekilde mutate
        edilmez: mevcut bir pozisyon varsa aynı nesne aynen döner (hiçbir
        alanı, `updated_at` dahil, DEĞİŞTİRİLMEZ); hiç pozisyon yoksa
        (sembol için daha önce hiç sinyal işlenmemiş), `self._states`'e
        hiçbir girdi EKLEMEDEN salt bir FLAT görünümü döndürülür — bu
        çağrıdan sonra `self.position(symbol)` hâlâ `None` döner.
        """
        state = self._states.get(signal.symbol)
        if state is not None:
            current_position = state.position
        else:
            current_position = PaperPosition(
                symbol=signal.symbol,
                side=PositionSide.FLAT,
                quantity=0.0,
                average_entry_price=0.0,
                realized_pnl=0.0,
                updated_at=signal.timestamp,
            )
        return PaperTradingResult(
            symbol=signal.symbol,
            position=current_position,
            orders=(),
            fills=(),
            idempotent_replay=False,
        )

    def position(self, symbol: str) -> PaperPosition | None:
        """Bir sembol için en son bilinen pozisyon snapshot'ı; hiç sinyal
        işlenmediyse `None`."""
        state = self._states.get(normalize_symbol(symbol))
        return state.position if state is not None else None

    def has_processed(self, symbol: str, context_id: str) -> bool:
        """PRE-AUDIT ENHANCEMENT PASS — additive, read-only accessor
        (bkz. PRE_AUDIT_ENHANCEMENTS.md, `research/sizing.py`). Bu
        sembol için verilen `context_id`, `process_signal()` tarafından
        DAHA ÖNCE (idempotency ledger'ında — `state.processed_context_ids`)
        kaydedilmiş mi? Salt-okunur bir sorgu — state'i hiçbir şekilde
        MUTATE ETMEZ; `position()`/`orders()`/`fills()` ile AYNI
        desendir. Bir dış sarmalayıcının (örn. `SizedPaperTradingEngine`)
        aynı `context_id`'yi ikinci kez policy-hesaplamasından
        GEÇİRMEDEN doğrudan `process_signal()`'e (kendi idempotency'sine)
        yönlendirip yönlendiremeyeceğine karar vermesi için gereklidir —
        NEUTRAL sinyaller (hiçbir zaman `processed_context_ids`'e
        yazılmaz, bkz. `_no_action_result`) için her zaman `False` döner."""
        state = self._states.get(normalize_symbol(symbol))
        if state is None:
            return False
        return context_id in state.processed_context_ids

    def orders(self, symbol: str) -> tuple[PaperOrder, ...]:
        state = self._states.get(normalize_symbol(symbol))
        return state.orders if state is not None else ()

    def fills(self, symbol: str) -> tuple[PaperFill, ...]:
        state = self._states.get(normalize_symbol(symbol))
        return state.fills if state is not None else ()

    def unrealized_pnl(self, symbol: str, mark_price: MarketPriceSnapshot) -> float:
        """Açık pozisyon için, verilen `mark_price`'a göre realize
        EDİLMEMİŞ PnL. Pozisyon FLAT veya hiç mevcut değilse 0.0 döner.
        Bu metod state'i MUTATE ETMEZ (salt-okunur bir projeksiyondur)."""
        normalized = normalize_symbol(symbol)
        if mark_price.symbol != normalized:
            raise NoLookAheadViolationError(
                f"mark_price.symbol ({mark_price.symbol}) != {normalized}"
            )
        state = self._states.get(normalized)
        if state is None or state.position.side is PositionSide.FLAT:
            return 0.0
        position = state.position
        if position.side is PositionSide.LONG:
            return (mark_price.price - position.average_entry_price) * position.quantity
        return (position.average_entry_price - mark_price.price) * position.quantity


=== FILE: crypto_signal_engine/paper_trading/models.py ===
"""
Faz 5 (Paper Trading / Simulation Engine) domain modelleri.

Kural: Bu modül Binance'e, execution API'sine veya herhangi bir dış
kütüphaneye bağımlı DEĞİLDİR ve olamaz. Paper trading tamamen dahili bir
simülasyondur; hiçbir gerçek emir gönderilmez, hiçbir API key/secret
kullanılmaz (bkz. SAFETY_INVARIANTS.md).

Bu modül Faz 4 `Signal` nesnesini TÜKETİR (`crypto_signal_engine.domain.
models.Signal`) — `ConsensusResult`/`RegimeContext`'e hiçbir bağımlılığı
yoktur (Faz 4'ün regime keyword-only sözleşme sapmasını, bkz. DECISIONS.md
Karar 46/47, derinleştirmez; onunla hiç temas etmez).

`Signal` kendisi bir fiyat taşımaz (Faz 4 sözleşmesi gereği) — bu yüzden
gerçekleşme fiyatı ayrı bir `MarketPriceSnapshot` ile sağlanır. Bunun
`as_of`'u, işlendiği sinyalin `timestamp`'inden SONRA olamaz (no-look-ahead,
`PaperTradingEngine` tarafından zorunlu kılınır — bkz. engine.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import (
    normalize_symbol,
    require_enum,
    require_finite,
    require_utc_aware,
)


class OrderSide(str, Enum):
    """Simüle edilmiş bir emrin yönü."""

    BUY = "BUY"
    SELL = "SELL"


class PositionSide(str, Enum):
    """Bir sembol için anlık pozisyon yönü."""

    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass(frozen=True)
class MarketPriceSnapshot:
    """Bir sembol için, belirli bir anda gözlemlenen gerçekleşme fiyatı.

    Faz 1/2 market data'sından (örn. kapanmış bir candle'ın close'u ya da
    order book mid_price'ı) türetilmiş, Faz 5'e özgü minimal bir
    projeksiyondur — `Candle`/`OrderBookSnapshot`'ın tamamına bağımlılık
    kasıtlı olarak eklenmez. Bu değeri türetmek ve no-look-ahead'i
    orkestrasyon seviyesinde sağlamak çağıran kodun sorumluluğundadır;
    `PaperTradingEngine` invariant'ı yine de bağımsız olarak zorunlu kılar.
    """

    symbol: str
    price: float
    as_of: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_utc_aware(self.as_of, "as_of")
        require_finite(self.price, "price")
        if self.price <= 0:
            raise ValueError("price pozitif olmalı")


@dataclass(frozen=True)
class PaperOrder:
    """Simüle edilmiş bir market emri (asla gerçek bir borsaya gönderilmez).

    `order_id` deterministik olarak `(symbol, signal_context_id, seq)`
    üçlüsünden türetilir — uuid/random/wall-clock KULLANILMAZ, böylece aynı
    girdi HER ZAMAN aynı order_id'yi üretir (determinism).
    """

    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    signal_context_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, OrderSide, "side")
        require_utc_aware(self.created_at, "created_at")
        require_finite(self.quantity, "quantity")
        if self.quantity <= 0:
            raise ValueError("quantity pozitif olmalı")
        if not self.order_id.strip():
            raise ValueError("order_id boş olamaz")
        if not self.signal_context_id.strip():
            raise ValueError("signal_context_id boş olamaz")


@dataclass(frozen=True)
class PaperFill:
    """Bir `PaperOrder`'ın anlık ve tam (partial fill YOK) gerçekleşmesi.

    Faz 5 kapsamı kasıtlı olarak market-order/tam-fill modeliyle
    sınırlıdır; partial fill, limit order gibi genişletmeler bu fazın
    kapsamı DIŞINDADIR (bilinçli bir sınırlama).

    `price`, girdi `MarketPriceSnapshot.price`'ın YÖNE göre deterministik
    slippage uygulanmış hâlidir (BUY: `P*(1+slippage_bps/10000)`, SELL:
    `P*(1-slippage_bps/10000)`) — HAM referans fiyat DEĞİLDİR. `fee`,
    BU (slippage uygulanmış) `price` üzerinden `abs(quantity*price)*
    fee_bps/10000` ile hesaplanır ve doğrudan bu fill'de KALICI olarak
    taşınır (bkz. `engine.py` — deterministic fee/slippage modeli).
    """

    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    fee: float
    filled_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, OrderSide, "side")
        require_utc_aware(self.filled_at, "filled_at")
        require_finite(self.quantity, "quantity")
        require_finite(self.price, "price")
        require_finite(self.fee, "fee")
        if self.quantity <= 0:
            raise ValueError("quantity pozitif olmalı")
        if self.price <= 0:
            raise ValueError("price pozitif olmalı")
        if self.fee < 0:
            raise ValueError("fee negatif olamaz")
        if not self.fill_id.strip():
            raise ValueError("fill_id boş olamaz")
        if not self.order_id.strip():
            raise ValueError("order_id boş olamaz")


@dataclass(frozen=True)
class PaperPosition:
    """Bir sembol için anlık pozisyon durumu (immutable snapshot).

    FLAT iken `quantity == 0.0` VE `average_entry_price == 0.0` ZORUNLUDUR;
    LONG/SHORT iken `quantity > 0` ZORUNLUDUR — illegal bir state'in
    dataclass seviyesinde bile temsil edilememesi için (bkz. SafetyState
    encapsulation deseni, safety/models.py).
    """

    symbol: str
    side: PositionSide
    quantity: float
    average_entry_price: float
    realized_pnl: float
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        require_enum(self.side, PositionSide, "side")
        require_utc_aware(self.updated_at, "updated_at")
        require_finite(self.quantity, "quantity")
        require_finite(self.average_entry_price, "average_entry_price")
        require_finite(self.realized_pnl, "realized_pnl")

        if self.quantity < 0:
            raise ValueError("quantity negatif olamaz")
        if self.average_entry_price < 0:
            raise ValueError("average_entry_price negatif olamaz")

        if self.side is PositionSide.FLAT:
            if self.quantity != 0.0 or self.average_entry_price != 0.0:
                raise ValueError(
                    "FLAT pozisyon quantity=0.0 VE average_entry_price=0.0 olmalı"
                )
        elif self.quantity <= 0.0:
            raise ValueError(f"{self.side} pozisyon quantity pozitif olmalı")


@dataclass(frozen=True)
class PaperTradingResult:
    """`PaperTradingEngine.process_signal()`'ın tam sonucu.

    `idempotent_replay=True` ise `orders`/`fills` HER ZAMAN boştur — aynı
    (symbol, context_id) daha önce işlenmiş, bu çağrı state'i tekrar
    MUTATE ETMEMİŞTİR; `position` yine de o context için geçerli olan
    (önceden hesaplanmış) pozisyon snapshot'ını taşır.
    """

    symbol: str
    position: PaperPosition
    orders: tuple[PaperOrder, ...]
    fills: tuple[PaperFill, ...]
    idempotent_replay: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if not isinstance(self.orders, tuple):
            object.__setattr__(self, "orders", tuple(self.orders))
        if not isinstance(self.fills, tuple):
            object.__setattr__(self, "fills", tuple(self.fills))


=== FILE: crypto_signal_engine/persistence/__init__.py ===


=== FILE: crypto_signal_engine/persistence/errors.py ===
"""
Faz 7 (Persistence & Recovery) exception taxonomy.

Kural: sığ ve yararlı. Mevcut `crypto_signal_engine.errors.PersistenceError`
(Faz 2'de tanımlı — "SQLite/persistence katmanında bir hata") TEKRAR
TANIMLANMAZ; bu iki yeni tip, Faz 7'nin GERÇEKTEN farklı davranış
gerektiren iki durumunu ayırt eder: sürüm uyuşmazlığı (migration YOK,
sessiz geçiş YOK) ve bozuk/okunamaz kayıt (sessizce atlamak idempotency/
PnL koruma garantisini bozar — bkz. modül docstring'leri).
"""

from __future__ import annotations

from crypto_signal_engine.errors import PersistenceError


class SchemaVersionMismatchError(PersistenceError):
    """Durable store'un `schema_version`'ı, bu kod sürümünün beklediği
    `SCHEMA_VERSION` ile UYUŞMUYOR.

    Sessiz bir migration veya "en iyi çaba" okuma YAPILMAZ — bu, "eski
    formatı yanlış yorumlayıp PnL/idempotency state'ini bozma" riskini
    fiziksel olarak imkansız kılar (bkz. PHASE7_PERSISTENCE_RECOVERY.md
    — "corruption/failure behavior")."""


class CorruptRecordError(PersistenceError):
    """Durable store'daki bir kayıt (JSON blob, enum değeri, sayısal alan)
    beklenen şemaya UYMUYOR veya deserialize EDİLEMİYOR.

    Bu kayıt SESSİZCE ATLANMAZ — bir `processed_context`/`paper_position`
    kaydını sessizce yok saymak, idempotency/PnL koruma garantisini
    doğrudan ihlal eder (aynı context_id'nin "hiç işlenmemiş" gibi tekrar
    kabul edilmesine yol açabilir). Recovery, bu hatayı AÇIKÇA yukarı
    fırlatır; runtime NOT-READY/DEGRADED kalır."""


=== FILE: crypto_signal_engine/persistence/paper_state_store.py ===
"""
Faz 7 — `PaperStateStore`: Faz 5 (paper trading) + Faz 6 (runtime candle
checkpoint) durable state'i için SQLite-backed store.

Kural (Bölüm 12 — Faz 2 `sqlite_store.py` ile AYNI disiplin):
- parameterized SQL (hiçbir yerde string interpolation ile SQL kurulmaz)
- explicit schema creation + deterministic `SCHEMA_VERSION`
- transaction boundaries (`with self._connection:`) — sembol başına
  pozisyon + processed_context + fill + order yazımı TEK bir transaction'da
- no global hidden connection (her instance kendi connection'ını sahiplenir)
- sürüm uyuşmazlığı/bozuk kayıt SESSİZCE yutulmaz (`SchemaVersionMismatchError`/
  `CorruptRecordError`, bkz. errors.py)

Bu dosya `crypto_signal_engine/persistence/sqlite_store.py`'nin (Faz 2,
candle canonical state) YANINDA, AYRI bir concern için AYRI bir SQLite
veritabanıdır — Faz 2'nin candle store'unu DEĞİŞTİRMEZ/paylaşmaz.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.models import (
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError
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

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_position (
    symbol TEXT PRIMARY KEY,
    side TEXT NOT NULL,
    quantity REAL NOT NULL,
    average_entry_price REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL,
    entry_fee REAL NOT NULL,
    last_signal_timestamp TEXT
);

CREATE TABLE IF NOT EXISTS processed_context (
    symbol TEXT NOT NULL,
    context_id TEXT NOT NULL,
    signal_json TEXT NOT NULL,
    position_json TEXT NOT NULL,
    PRIMARY KEY (symbol, context_id)
);

CREATE TABLE IF NOT EXISTS paper_fill (
    fill_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    fill_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_order (
    order_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    order_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candle_checkpoint (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    last_open_time TEXT NOT NULL,
    PRIMARY KEY (symbol, timeframe)
);
"""


@dataclass(frozen=True)
class PaperStateSnapshot:
    """Bir sembol için durable store'dan geri okunmuş TAM Faz 5 state'i —
    `recovery.py::restore_paper_engine`'in `PaperTradingEngine._states`'i
    yeniden inşa etmek için ihtiyaç duyduğu her şey."""

    position: PaperPosition
    entry_fee: float
    last_signal_timestamp: datetime | None
    processed_context_ids: dict[str, tuple[Signal, PaperTradingResult]]
    fills: tuple[PaperFill, ...]
    orders: tuple[PaperOrder, ...]


class PaperStateStore:
    """Faz 5 paper-trading state'i ve Faz 6 candle checkpoint'leri için
    durable, injectable SQLite store. `db_path` HER ZAMAN çağıran tarafından
    enjekte edilir — hiçbir geliştirici-makinesi path'i hardcode EDİLMEZ."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
                    return
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

        existing = self._connection.execute("SELECT version FROM schema_version").fetchone()[0]
        if existing != SCHEMA_VERSION:
            raise SchemaVersionMismatchError(
                f"Durable store schema_version={existing}, bu kod SCHEMA_VERSION={SCHEMA_VERSION} "
                f"bekliyor — sessiz migration YAPILMAZ ({self._db_path})"
            )

    # -- Yazma (checkpoint) ---------------------------------------------------

    def _write_candle_checkpoint(self, symbol: str, timeframe: Timeframe, open_time: datetime) -> None:
        """AÇIK bir SQLite transaction İÇİNDE (çağıranın `with self._connection:`
        bloğu) tek bir candle_checkpoint upsert'i yazar — KENDİ transaction'ını
        AÇMAZ/KAPATMAZ (bkz. `checkpoint_candle`/`checkpoint_candle_transition`,
        bu ikisi TEK gerçek çağrı noktasıdır, aynı SQL'i AYRI/kombine
        transaction'larda yeniden kullanmak için)."""
        self._connection.execute(
            "INSERT INTO candle_checkpoint (symbol, timeframe, last_open_time) VALUES (?, ?, ?) "
            "ON CONFLICT(symbol, timeframe) DO UPDATE SET last_open_time = excluded.last_open_time",
            (symbol, timeframe.value, open_time.isoformat()),
        )

    def _write_paper_transition(
        self,
        symbol: str,
        position: PaperPosition,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """AÇIK bir SQLite transaction İÇİNDE (bkz. `_write_candle_checkpoint`
        yorumu — AYNI desen) bir Paper transition'ın TÜM durable
        sonuçlarını (pozisyon upsert + yeni processed_context + yeni
        fill/order kayıtları) yazar."""
        self._connection.execute(
            """
            INSERT INTO paper_position
                (symbol, side, quantity, average_entry_price, realized_pnl,
                 updated_at, entry_fee, last_signal_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                side = excluded.side,
                quantity = excluded.quantity,
                average_entry_price = excluded.average_entry_price,
                realized_pnl = excluded.realized_pnl,
                updated_at = excluded.updated_at,
                entry_fee = excluded.entry_fee,
                last_signal_timestamp = excluded.last_signal_timestamp
            """,
            (
                symbol, position.side.value, position.quantity, position.average_entry_price,
                position.realized_pnl, position.updated_at.isoformat(), entry_fee,
                last_signal_timestamp.isoformat() if last_signal_timestamp is not None else None,
            ),
        )
        for context_id, signal, ctx_position in new_context_entries:
            self._connection.execute(
                "INSERT OR REPLACE INTO processed_context (symbol, context_id, signal_json, position_json) "
                "VALUES (?, ?, ?, ?)",
                (
                    symbol, context_id,
                    json.dumps(signal_to_dict(signal)), json.dumps(position_to_dict(ctx_position)),
                ),
            )
        for fill in new_fills:
            self._connection.execute(
                "INSERT OR REPLACE INTO paper_fill (fill_id, symbol, fill_json) VALUES (?, ?, ?)",
                (fill.fill_id, symbol, json.dumps(fill_to_dict(fill))),
            )
        for order in new_orders:
            self._connection.execute(
                "INSERT OR REPLACE INTO paper_order (order_id, symbol, order_json) VALUES (?, ?, ?)",
                (order.order_id, symbol, json.dumps(order_to_dict(order))),
            )

    def checkpoint_paper_state(
        self,
        *,
        symbol: str,
        position: PaperPosition,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """Bir sembolün TAM güncel Faz 5 durumunu TEK bir transaction'da
        durable hâle getirir: pozisyon (upsert) + yeni processed_context
        kayıtları + yeni fill/order kayıtları. Ya HEPSİ ya HİÇBİRİ —
        kısmi bir checkpoint asla başarılı görünmez (Bölüm — atomicity).

        Faz 7 pre-audit remediation NOTU: `PersistedRuntime` bu metodu
        ARTIK canlı ingestion'dan DOĞRUDAN çağırmaz (bkz.
        `checkpoint_candle_transition` — candle checkpoint'iyle AYNI
        transaction'da, HIGH-1 fix). Bu metod, Faz 5 seviyesinde izole
        test/entegrasyon senaryoları (bkz. `tests/test_persistence_recovery.py::
        _checkpoint_from_result`) ve doğrudan store kullanımı için PUBLIC
        API olarak KORUNUR — davranışı DEĞİŞMEDİ."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_paper_transition(
                    normalized, position, entry_fee, last_signal_timestamp,
                    new_context_entries, new_fills, new_orders,
                )
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper state checkpoint başarısız ({normalized}): {exc}") from exc

    def checkpoint_candle(self, symbol: str, timeframe: Timeframe, open_time: datetime) -> None:
        """Bir (symbol, timeframe) için en son KABUL EDİLMİŞ candle'ın
        `open_time`'ını durable hâle getirir. Yalnızca `RuntimeCoordinator.
        ingest_candle()` ZATEN `ACCEPTED` (kesin olarak monoton artan)
        döndürdüğünde çağrılır — bu store KENDİSİ hiçbir sıralama/gap
        kontrolü YAPMAZ (bu, Faz 6'nın `CandleWindow`'unun sorumluluğudur;
        burada TEKRARLANMAZ). `PersistedRuntime` bu metodu YALNIZCA o
        candle'ın hiçbir durable Paper transition ÜRETMEDİĞİ (NEUTRAL/
        idempotent replay/aynı-yön no-churn) durumda kullanır — bir
        transition VARSA `checkpoint_candle_transition` kullanılır."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_candle_checkpoint(normalized, timeframe, open_time)
        except sqlite3.Error as exc:
            raise PersistenceError(f"candle checkpoint başarısız ({normalized}/{timeframe.value}): {exc}") from exc

    def checkpoint_candle_transition(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        candle_open_time: datetime,
        position: PaperPosition | None,
        entry_fee: float,
        last_signal_timestamp: datetime | None,
        new_context_entries: list[tuple[str, Signal, PaperPosition]],
        new_fills: tuple[PaperFill, ...],
        new_orders: tuple[PaperOrder, ...],
    ) -> None:
        """Faz 7 pre-audit remediation (HIGH-1 fix): bir kabul edilmiş
        candle'ın checkpoint'ini VE (varsa) o candle'ın ÜRETTİĞİ Paper
        transition'ı TEK bir SQLite transaction'da durable hâle getirir —
        ya HEPSİ ya HİÇBİRİ. Önceki tasarımda bu ikisi AYRI transaction'lar
        (`checkpoint_candle()` + `checkpoint_paper_state()`) İDİ — bir
        crash/write-failure ikisinin ARASINA düşerse, candle checkpoint'i
        durable olarak İLERLEYEBİLİYORDU ama ilişkili Paper transition'ı
        HİÇ yazılmamış olabiliyordu (bkz. DECISIONS.md, HIGH-1). ARTIK bu
        durum YAPISAL OLARAK İMKANSIZDIR: her ikisi de AYNI `with
        self._connection:` bloğu içinde yazılır.

        `position` `None` ise (candle kabul edildi ama hiçbir durable Paper
        transition ÜRETMEDİ) YALNIZCA candle checkpoint'i yazılır —
        `PersistedRuntime` bu durumda zaten bu metodu ÇAĞIRMAZ (bkz.
        `checkpoint_candle`), ama bu metod da tek başına çağrılırsa AYNI
        güvenli davranışı sağlar (gereksiz boş paper-state yazımı YAPMAZ)."""
        normalized = normalize_symbol(symbol)
        try:
            with self._connection:
                self._write_candle_checkpoint(normalized, timeframe, candle_open_time)
                if position is not None:
                    self._write_paper_transition(
                        normalized, position, entry_fee, last_signal_timestamp,
                        new_context_entries, new_fills, new_orders,
                    )
        except sqlite3.Error as exc:
            raise PersistenceError(
                f"candle+paper transition checkpoint başarısız ({normalized}/{timeframe.value}): {exc}"
            ) from exc

    # -- Okuma (recovery) ---------------------------------------------------

    def load_paper_state(self, symbol: str) -> PaperStateSnapshot | None:
        """Bir sembol için durable store'da HİÇ kayıt yoksa `None` döner
        (bu sembol için bootstrap'tan başlanmalı — Faz 7 tarafından SAHTE
        bir state ÜRETİLMEZ). Bozuk bir kayıt bulunursa `CorruptRecordError`
        AÇIKÇA fırlatılır — SESSİZCE atlanmaz."""
        normalized = normalize_symbol(symbol)
        try:
            position_row = self._connection.execute(
                "SELECT side, quantity, average_entry_price, realized_pnl, updated_at, "
                "entry_fee, last_signal_timestamp FROM paper_position WHERE symbol = ?",
                (normalized,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_position okunamadı ({normalized}): {exc}") from exc

        if position_row is None:
            return None

        try:
            side_str, quantity, avg_entry, realized_pnl, updated_at_str, entry_fee, last_signal_str = position_row
            position = PaperPosition(
                symbol=normalized, side=PositionSide(side_str), quantity=quantity,
                average_entry_price=avg_entry, realized_pnl=realized_pnl,
                updated_at=datetime.fromisoformat(updated_at_str),
            )
            last_signal_timestamp = datetime.fromisoformat(last_signal_str) if last_signal_str else None
        except (ValueError, TypeError, KeyError) as exc:
            raise CorruptRecordError(f"paper_position kaydı bozuk ({normalized}): {exc}") from exc

        processed_context_ids: dict[str, tuple[Signal, PaperTradingResult]] = {}
        try:
            context_rows = self._connection.execute(
                "SELECT context_id, signal_json, position_json FROM processed_context WHERE symbol = ?",
                (normalized,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"processed_context okunamadı ({normalized}): {exc}") from exc

        for context_id, signal_json, position_json in context_rows:
            try:
                signal = dict_to_signal(json.loads(signal_json))
                ctx_position = dict_to_position(json.loads(position_json))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(
                    f"processed_context kaydı bozuk ({normalized}/{context_id}): {exc}"
                ) from exc
            result = PaperTradingResult(
                symbol=normalized, position=ctx_position, orders=(), fills=(), idempotent_replay=False,
            )
            processed_context_ids[context_id] = (signal, result)

        fills: list[PaperFill] = []
        try:
            fill_rows = self._connection.execute(
                "SELECT fill_json FROM paper_fill WHERE symbol = ?", (normalized,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_fill okunamadı ({normalized}): {exc}") from exc
        for (fill_json,) in fill_rows:
            try:
                fills.append(dict_to_fill(json.loads(fill_json)))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(f"paper_fill kaydı bozuk ({normalized}): {exc}") from exc
        fills.sort(key=lambda f: f.filled_at)

        orders: list[PaperOrder] = []
        try:
            order_rows = self._connection.execute(
                "SELECT order_json FROM paper_order WHERE symbol = ?", (normalized,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_order okunamadı ({normalized}): {exc}") from exc
        for (order_json,) in order_rows:
            try:
                orders.append(dict_to_order(json.loads(order_json)))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                raise CorruptRecordError(f"paper_order kaydı bozuk ({normalized}): {exc}") from exc
        orders.sort(key=lambda o: o.created_at)

        return PaperStateSnapshot(
            position=position, entry_fee=entry_fee, last_signal_timestamp=last_signal_timestamp,
            processed_context_ids=processed_context_ids, fills=tuple(fills), orders=tuple(orders),
        )

    def list_open_position_symbols(self) -> tuple[str, ...]:
        """Durable `paper_position` tablosunda `side != 'FLAT'` olan (yani
        GERÇEKTEN açık) tüm sembolleri, deterministik (alfabetik) sırayla
        döner. Otomatik sembol seçiminin "açık pozisyonlu bir sembol ASLA
        izlemenin dışına düşmemeli" güvenlik kuralı (bkz. `app.py::
        Application.__init__` — pinning) İÇİN eklenmiştir; salt-okunur,
        hiçbir state MUTATE ETMEZ."""
        try:
            rows = self._connection.execute(
                "SELECT symbol FROM paper_position WHERE side != ? ORDER BY symbol", (PositionSide.FLAT.value,)
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"paper_position (open positions) okunamadı: {exc}") from exc
        return tuple(row[0] for row in rows)

    def load_candle_checkpoint(self, symbol: str, timeframe: Timeframe) -> datetime | None:
        normalized = normalize_symbol(symbol)
        try:
            row = self._connection.execute(
                "SELECT last_open_time FROM candle_checkpoint WHERE symbol = ? AND timeframe = ?",
                (normalized, timeframe.value),
            ).fetchone()
        except sqlite3.Error as exc:
            raise PersistenceError(f"candle_checkpoint okunamadı ({normalized}/{timeframe.value}): {exc}") from exc
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row[0])
        except (ValueError, TypeError) as exc:
            raise CorruptRecordError(
                f"candle_checkpoint kaydı bozuk ({normalized}/{timeframe.value}): {exc}"
            ) from exc

    def close(self) -> None:
        self._connection.close()


=== FILE: crypto_signal_engine/persistence/recovery.py ===
"""
Faz 7 — recovery.py: process-restart sonrası Faz 5 (paper trading) ve
Faz 6 (runtime candle continuity) state'ini durable store'dan geri yükleyen
AÇIK bir recovery boundary, ve normal çalışma sırasında (her kabul edilmiş
mutation'dan HEMEN sonra) checkpoint yazan `PersistedRuntime` wrapper'ı.

MİMARİ KURAL ("persistence bir SINIRDIR, domain nesnelerini
database-aware YAPMAZ"): Bu modül `PaperTradingEngine`/`RuntimeCoordinator`
sınıflarını DEĞİŞTİRMEZ — onları SARAR (wrap). `PaperTradingEngine`'in
KASITLI OLARAK sunmadığı tek şey (bir "restore/seed state" API'si —
Faz 5'in kendi kabul edilmiş sözleşmesi, TÜM mutation'ların YALNIZCA
`process_signal()` üzerinden geçmesini zorunlu kılar) için, bu dosyada
TEK ve AÇIKÇA dokümante edilmiş bir istisna yapılır: `restore_paper_engine()`
doğrudan `PaperTradingEngine`'in private `_states` sözlüğüne yazar. Bunun
DIŞINDA hiçbir yerde Faz 5/6'nın private state'ine YAZILMAZ.

NEDEN `process_signal()` ÜZERİNDEN "REPLAY" EDİLEMEZ: Restart sonrası
geçmiş sinyalleri `process_signal()`'a yeniden vermek, onları "yeni" gibi
işlemeye ÇALIŞMAK anlamına gelir — ki bu TAM OLARAK önlenmesi gereken
şeydir (duplicate paper action/PnL). Bu yüzden restore, mutation
YOLUNDAN GEÇMEDEN, doğrudan state seed eder.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import timedelta

from crypto_signal_engine.domain._validation import normalize_symbol
from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine, _SymbolState
from crypto_signal_engine.paper_trading.models import PaperPosition
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome, ProcessedMarketEvent, RuntimeCycleResult

# `coordinator.py::_TIMEFRAME_DURATIONS`'ın KASITLI, KÜÇÜK bir kopyası —
# bu sabit, iş mantığı DEĞİLDİR (yalnızca Timeframe -> timedelta lookup),
# bu yüzden coordinator'ın private isim alanına erişmek yerine burada
# bağımsız olarak tanımlanması tercih edilmiştir (bkz. modül docstring'i —
# "persistence bir sınırdır", coordinator'ın iç mimarisine sıkı sıkıya
# bağlanmaz).
_TIMEFRAME_DURATIONS: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}


def restore_paper_engine(
    store: PaperStateStore, engine: PaperTradingEngine, symbols: Iterable[str]
) -> dict[str, bool]:
    """Her sembol için durable store'da state VARSA, `PaperTradingEngine`'in
    private `_states` sözlüğüne DOĞRUDAN yazarak geri yükler (bkz. modül
    docstring'i — bu, dokümante edilmiş TEK istisnadır). State YOKSA
    (sembol daha önce hiç işlem görmemiş), o sembol için HİÇBİR ŞEY
    YAPILMAZ — sahte bir başlangıç state'i ÜRETİLMEZ.

    Döner: `{symbol: bool}` — her sembol için state geri yüklenip
    yüklenmediği (testler ve raporlama için)."""
    restored: dict[str, bool] = {}
    for symbol in symbols:
        normalized = normalize_symbol(symbol)
        snapshot = store.load_paper_state(normalized)
        if snapshot is None:
            restored[normalized] = False
            continue
        engine._states[normalized] = _SymbolState(
            position=snapshot.position,
            entry_fee=snapshot.entry_fee,
            orders=snapshot.orders,
            fills=snapshot.fills,
            processed_context_ids=dict(snapshot.processed_context_ids),
            last_signal_timestamp=snapshot.last_signal_timestamp,
        )
        restored[normalized] = True
    return restored


class PersistedRuntime:
    """`RuntimeCoordinator`'ı (Faz 6, DEĞİŞTİRİLMEDEN) SARAN, kabul edilmiş
    her state-değiştirici mutation'dan HEMEN SONRA durable checkpoint yazan
    bir Faz 7 wrapper'ı.

    Checkpoint yazma politikası (Bölüm — "persistence write policy"):
    - Yeni bir candle `ACCEPTED` olduğunda -> `checkpoint_candle()`.
    - Bir sinyal değerlendirmesi GERÇEK bir mutation ürettiğinde
      (`evaluated=True`, idempotent replay DEĞİL, NEUTRAL DEĞİL) ->
      `checkpoint_paper_state()`.
    - NEUTRAL (NO_ACTION), idempotent replay, ve aynı-yön tekrarının
      ürettiği "hiçbir yeni fill/order yok" durumları İÇİN GEREKSİZ YAZMA
      YAPILMAZ (Faz 5'in `same-direction no churn`/`NEUTRAL NO_ACTION`
      davranışı BOZULMAZ) — bkz. `_is_durable_transition`.

    Checkpoint yazımı BAŞARISIZ olursa (`PersistenceError`): in-memory
    Faz 5/6 state'i GERİ ALINMAZ (Faz 5 yeniden tasarlanmadan bu mümkün
    değildir) — ilgili sembol `RuntimeCoordinator.mark_persistence_fault()`
    ile AÇIKÇA DEGRADED işaretlenir (fail-closed, "pretend atomicity"
    YOKTUR)."""

    def __init__(self, coordinator: RuntimeCoordinator, store: PaperStateStore) -> None:
        self._coordinator = coordinator
        self._store = store
        # SINGLE FINAL ACCEPTANCE BLOCKER fix (bkz. DECISIONS.md): bir
        # (symbol, timeframe) için BAŞARISIZ olan `checkpoint_candle_transition`
        # yazımı, o payload'ı BURADA (in-memory, FIFO) bir "pending-unsynced"
        # backlog'a EKLER — atılmaz, GÖRMEZDEN GELİNMEZ. Aynı (symbol,
        # timeframe) için HER sonraki checkpoint denemesi (yeni bir candle
        # kabul edildiğinde), ÖNCE bu backlog'u BAŞTAN (en eski/başarısız
        # olan ÖNCE) SIRAYLA flush etmeye ÇALIŞIR — kendi (daha yeni)
        # payload'ını backlog'un SONUNA ekleyerek. Bir öğe flush BAŞARISIZ
        # olursa, o öğeden SONRAKİ HİÇBİR öğe (yeni gelen dahil) yazılmaz;
        # persistence-fault AÇIK kalır. Backlog TAMAMEN boşaldığında
        # (TÜM bekleyen transition'lar durable hâle geldiğinde) fault
        # temizlenir. Böylece daha SONRAKİ bir candle'ın checkpoint'i, HÂLÂ
        # eksik olan DAHA ESKİ bir failed transition'ı asla "es geçip"
        # sessizce durable görünüme SAHİP OLAMAZ (bkz. `_flush_pending`).
        #
        # Bu backlog KASITLI OLARAK SADECE in-memory'dir (durable hâle
        # GETİRİLMEZ) — bir process crash'i sırasında hâlâ boşaltılmamış bir
        # backlog kaybolur (Faz 7'nin ZATEN kabul edilmiş fail-closed
        # tasarımıyla AYNI sınır: in-memory state hiçbir zaman durable
        # store'a geri YAZILMADAN "rollback" edilmez). Bu, GENEL bir 2PC
        # DEĞİLDİR: yalnızca ZATEN hesaplanmış (Signal YENİDEN
        # DEĞERLENDİRİLMEDEN) bir SQL yazımının retry'ıdır.
        self._pending: dict[tuple[str, Timeframe], list[dict[str, object]]] = {}

        # Phase 16 PRODUCTION HOT-RESELECTION FIX — per-symbol task
        # ownership, additive to the flat task list `run()` extends into
        # `coordinator._tasks` (full-shutdown cancellation via
        # `coordinator.stop()` is completely unchanged: it still just
        # iterates `coordinator._tasks`). This dict exists so `add_symbol`/
        # `remove_symbol` (below) can create/cancel EXACTLY one symbol's
        # OWN checkpoint-writing consumer tasks, mirroring the identical
        # `self._tasks`/`self._tasks_by_symbol` pattern already accepted in
        # `RuntimeCoordinator` — this is ONLY ever the actual task owner
        # when `PersistedRuntime` is itself the top-level composed runtime
        # (bridge disabled); when wrapped by `BridgeRuntime`, that class
        # owns task spawning instead (see its own module docstring) and
        # this dict simply stays empty.
        self._tasks_by_symbol: dict[str, list[asyncio.Task]] = {}

    # -- Recovery (AÇIK, tek seferlik restart boundary) ------------------------

    async def recover(self) -> tuple[BootstrapReport, ...]:
        """Restart sonrası recovery sırası:

        1. Faz 5 paper state'ini (`restore_paper_engine`) geri yükler.
        2. Her (symbol, timeframe) için, durable bir candle checkpoint'i
           VARSA veya YOKSA FARK ETMEKSİZİN, AYNI warmup-lookback penceresi
           (`_TIMEFRAME_DURATIONS[timeframe] * warmup_candles * 2` — Faz
           6'nın kendi `bootstrap()`'ıyla AYNI, mevcut/gerçek runtime
           gereksinimi, YENİ bir sabit İCAT EDİLMEDİ) kullanılır:
           - checkpoint YOKSA: `[now - lookback, now)`.
           - checkpoint VARSA: `[checkpoint - lookback, now)` — checkpoint
             candle'ının KENDİSİ bu aralığa HER ZAMAN dahildir (`checkpoint
             - lookback < checkpoint < now`), böylece hem `CandleWindow`
             doğru şekilde yeniden "çapalanır" hem de FULL warmup GERÇEKTEN
             yeniden inşa edilir (BLOCKER FİX — bkz. DECISIONS.md Karar 68:
             eski dar `[checkpoint, now)` penceresi, checkpoint "now"a
             yakınken warmup'ı yeniden inşa etmeye YETMİYORDU, restart'ı
             gereksiz yere BOOTSTRAPPING'de bırakıyordu).
        3. Fetch edilen candle'lar `coordinator.bootstrap_candles()`'a
           (Faz 6, DEĞİŞTİRİLMEDEN) uygulanır — dedup/kronolojik
           sıralama/gap tespiti ZATEN orada yapılır, burada TEKRARLANMAZ.
           PUBLIC REST'in GERÇEKTEN dönebildiğinden FAZLASI asla
           FABRİKE EDİLMEZ — yetersiz geçmiş, dürüstçe `ready=False`
           (BOOTSTRAPPING) ile sonuçlanır.
        4. READY, Faz 6'nın KENDİ `_maybe_mark_symbol_ready()` kuralı
           (TÜM candle timeframe'leri + en az bir order-book feature
           snapshot'ı) tekrar GERÇEKTEN sağlandığında, doğal olarak
           `bootstrap_candles()` üzerinden oluşur — burada AYRICA/ERKEN
           bir READY işaretlemesi YAPILMAZ.

        Bu fazda GEÇMİŞTE (offline iken) kapanmış hiçbir M5 candle için
        YENİDEN bir sinyal değerlendirmesi/paper trade TETİKLENMEZ —
        yalnızca state (candle window + feature history) yeniden inşa
        edilir (`bootstrap_candles()` ASLA `SignalEngine.evaluate()`
        çağırmaz); restart sonrası İLK canlı M5 kapanışı normal Faz 6
        sınırı üzerinden bir sonraki değerlendirmeyi tetikler (bkz.
        PHASE7_PERSISTENCE_RECOVERY.md — "restart re-evaluation policy")."""
        restore_paper_engine(self._store, self._coordinator._paper_engine, self._coordinator._symbols)

        reports: list[BootstrapReport] = []
        now = self._coordinator._clock.now()
        for symbol in self._coordinator._symbols:
            for timeframe in self._coordinator._candle_timeframes:
                checkpoint = self._store.load_candle_checkpoint(symbol, timeframe)
                lookback = _TIMEFRAME_DURATIONS[timeframe] * self._coordinator._warmup_candles * 2
                anchor = now if checkpoint is None else checkpoint
                start = anchor - lookback
                candles = await self._coordinator._provider.fetch_historical_candles(symbol, timeframe, start, now)
                reports.append(self._coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=now))
        return tuple(reports)

    # -- Canlı ingestion (checkpoint'li) ---------------------------------------

    def ingest_candle(self, symbol: str, timeframe: Timeframe, candle: Candle) -> ProcessedMarketEvent:
        event = self._coordinator.ingest_candle(symbol, timeframe, candle)
        if event.outcome is IngestOutcome.ACCEPTED:
            durable = event.cycle_result if event.cycle_result is not None and self._is_durable_transition(
                event.cycle_result
            ) else None
            self._checkpoint_candle_transition(symbol, timeframe, candle, durable)
        return event

    async def resolve_gap(self, symbol: str, timeframe: Timeframe, pending_candle: Candle) -> ProcessedMarketEvent:
        """BLOCKER FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        eski davranış, `_consume_candles`'ın bir gap tespit ettiğinde
        DOĞRUDAN `self._coordinator.resolve_gap(...)`'i (bare coordinator)
        çağırmasıydı — bu, backfill'in SONUNDA bare coordinator'ın KENDİ
        (checkpoint YAZMAYAN) `ingest_candle()`'ını çağırıyordu. Sonuç:
        gap resolution'ın ürettiği bir Paper transition in-memory state'i
        mutate EDEBİLİYORDU ama HİÇBİR ZAMAN durable hâle GELMİYORDU (yalnızca
        candle checkpoint'i ilerliyordu) — restart sonrası bu trade KALICI
        olarak kayboluyordu (bootstrap tarihi sinyalleri asla yeniden
        DEĞERLENDİRMEZ, bkz. `recover()` docstring'i).

        Fix: `RuntimeCoordinator.resolve_gap_backfill()`'i (backfill adımı,
        HİÇBİR gap-tespit/REST-fetch mantığı burada TEKRARLANMADAN) yeniden
        kullanır, ardından `pending_candle`'ı bare coordinator yerine BU
        wrapper'ın KENDİ `ingest_candle()`'ı (candle+paper transition'ı TEK
        atomik transaction'da checkpoint'leyen, bkz. HIGH-1 fix) üzerinden
        ingest eder — gap resolution'ın ürettiği bir Paper transition,
        normal kabul edilmiş bir M5 transition ile TAMAMEN AYNI durable
        checkpoint garantisini alır."""
        await self._coordinator.resolve_gap_backfill(symbol, timeframe, pending_candle)
        normalized = normalize_symbol(symbol)
        return self.ingest_candle(normalized, timeframe, pending_candle)

    def ingest_order_book(self, symbol: str, snapshot: OrderBookSnapshot) -> ProcessedMarketEvent:
        # Order-book event'leri hiçbir durable checkpoint TETİKLEMEZ — M1
        # order-book state'i, Faz 6'nın kendi tasarımıyla zaten "en son
        # snapshot" bazlıdır ve restart sonrası yeniden canlı akıştan
        # DOĞAL olarak yeniden elde edilir (bkz. PHASE7 "rebuildable state").
        return self._coordinator.ingest_order_book(symbol, snapshot)

    def bootstrap_candles(self, symbol: str, timeframe: Timeframe, candles: list[Candle], as_of) -> BootstrapReport:
        return self._coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=as_of)

    def status(self):
        return self._coordinator.status()

    def _is_durable_transition(self, cycle_result: RuntimeCycleResult) -> bool:
        if not cycle_result.evaluated:
            return False
        if cycle_result.paper_result.idempotent_replay:
            return False
        if cycle_result.signal.direction is SignalDirection.NEUTRAL:
            return False
        return True

    def _checkpoint_candle_transition(
        self, symbol: str, timeframe: Timeframe, candle: Candle, cycle_result: RuntimeCycleResult | None
    ) -> None:
        """HIGH-1 FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        candle checkpoint'i VE (varsa, `cycle_result is not None`) o
        candle'ın ürettiği Paper transition'ı `PaperStateStore.
        checkpoint_candle_transition()` üzerinden TEK bir atomik SQLite
        transaction'da yazar — eski tasarımda bu ikisi (`checkpoint_candle`
        + `checkpoint_paper_state`) İKİ AYRI transaction'dı; bir crash/write
        failure aralarına düşerse candle checkpoint'i durable ilerlerken
        ilişkili Paper transition'ı HİÇ yazılmamış olabiliyordu. ARTIK bu
        yapısal olarak İMKANSIZDIR: ya İKİSİ DE durable olur ya da HİÇBİRİ.

        Health fault semantiği de bu atomicity'yi DOĞRU yansıtır: yazım
        BAŞARISIZ olursa, `cycle_result` mevcutsa HEM "candle_checkpoint"
        HEM "paper_state_checkpoint" reason'ı DEGRADED işaretlenir (ikisi
        de GERÇEKTEN aynı anda başarısız oldu — kısmi başarı YOKTUR);
        `cycle_result` yoksa yalnızca "candle_checkpoint" işaretlenir
        (yazılacak hiçbir Paper transition zaten YOKTU). Aynı simetri
        başarı/clear yolunda da geçerlidir.

        SINGLE FINAL ACCEPTANCE BLOCKER fix: bu payload doğrudan store'a
        YAZILMAZ — `self._pending` backlog'una (bkz. `__init__` yorumu)
        EKLENİR, ardından `_flush_pending()` çağrılır. Bu, bir ÖNCEKİ
        (aynı symbol/timeframe için) BAŞARISIZ kalmış transition'ın, bu
        DAHA YENİ candle'ın checkpoint'i tarafından ASLA "es geçilip"
        (overtake) sessizce durable görünüme sahip OLAMAYACAĞINI yapısal
        olarak garanti eder — flush her zaman backlog'un EN BAŞINDAN,
        sırayla dener (bkz. `_flush_pending`)."""
        normalized = normalize_symbol(symbol)
        payload = self._build_transition_payload(normalized, candle, cycle_result)
        key = (normalized, timeframe)
        self._pending.setdefault(key, []).append(payload)
        self._flush_pending(normalized, timeframe)

    def _build_transition_payload(
        self, symbol: str, candle: Candle, cycle_result: RuntimeCycleResult | None
    ) -> dict[str, object]:
        """Bir candle+ (varsa) Paper transition'ının, `PaperStateStore.
        checkpoint_candle_transition()`'a AYNEN geçirilebilecek durable
        yazım payload'ını inşa eder — HİÇBİR SQL çağrısı yapmaz (bkz.
        `_checkpoint_candle_transition`/`_flush_pending`, gerçek çağrı
        buradan AYRIŞTIRILMIŞTIR ki AYNI payload, ilk denemede VE
        sonraki bir retry'da BİREBİR AYNI şekilde yeniden kullanılabilsin
        — Signal YENİDEN DEĞERLENDİRİLMEZ, yalnızca ZATEN hesaplanmış
        sonuç yeniden yazılmaya ÇALIŞILIR)."""
        position: PaperPosition | None = None
        entry_fee = 0.0
        last_signal_timestamp = None
        new_context_entries: list[tuple[str, object, PaperPosition]] = []
        new_fills: tuple = ()
        new_orders: tuple = ()

        if cycle_result is not None:
            # `entry_fee`/`last_signal_timestamp`, Faz 5'in PUBLIC yüzeyinde
            # YOKTUR (Faz 5'e yeni bir public API eklemek "redesign" olurdu)
            # — bu iki skaler alan için, dokümante edilmiş minimal bir
            # salt-okunur private erişim yapılır (bkz. modül docstring'i).
            state = self._coordinator._paper_engine._states.get(symbol)
            entry_fee = state.entry_fee if state is not None else 0.0
            last_signal_timestamp = state.last_signal_timestamp if state is not None else None
            position = cycle_result.paper_result.position
            new_context_entries = [
                (cycle_result.signal.context_id, cycle_result.signal, cycle_result.paper_result.position)
            ]
            new_fills = cycle_result.paper_result.fills
            new_orders = cycle_result.paper_result.orders

        return {
            "candle_open_time": candle.open_time,
            "position": position,
            "entry_fee": entry_fee,
            "last_signal_timestamp": last_signal_timestamp,
            "new_context_entries": new_context_entries,
            "new_fills": new_fills,
            "new_orders": new_orders,
        }

    def _flush_pending(self, symbol: str, timeframe: Timeframe) -> None:
        """SINGLE FINAL ACCEPTANCE BLOCKER fix — backlog'u (bkz. `__init__`
        yorumu) baştan (en eski/ilk başarısız olan payload ÖNCE) sırayla
        flush etmeye çalışır:

        - Bir payload BAŞARIYLA yazılırsa backlog'dan ÇIKARILIR, sıradaki
          (varsa) denenir.
        - Bir payload BAŞARISIZ olursa, DÖNGÜ ORADA DURUR — o payload VE
          ondan SONRAKİ (backlog'da hâlâ bekleyen, bu ÇAĞRIYI TETİKLEYEN
          en yeni candle'ınki DAHİL) HİÇBİRİ yazılmaz; persistence-fault
          AÇIK bırakılır/YENİDEN işaretlenir.
        - Backlog TAMAMEN boşalırsa (bu (symbol, timeframe) için TÜM
          bekleyen transition'lar artık durable), fault temizlenir.

        Bu, gerekli invaryantı DOĞRUDAN uygular: health ANCAK backlog boş
        olduğunda READY'ye dönebilir — yani ANCAK eksik durable
        sonuçların HEPSİ başarıyla yazıldığında. Daha yeni bir candle'ın
        checkpoint'i, backlog'da bekleyen daha eski bir öğe flush
        edilmeden ASLA store'a ULAŞMAZ (FIFO sıra korunur)."""
        key = (symbol, timeframe)
        backlog = self._pending.get(key)
        if not backlog:
            return
        while backlog:
            payload = backlog[0]
            try:
                self._store.checkpoint_candle_transition(symbol=symbol, timeframe=timeframe, **payload)
            except PersistenceError:
                self._coordinator.mark_persistence_fault(symbol, reason="candle_checkpoint")
                if payload["position"] is not None:
                    self._coordinator.mark_persistence_fault(symbol, reason="paper_state_checkpoint")
                return
            backlog.pop(0)
        del self._pending[key]
        self._coordinator.clear_persistence_fault(symbol, reason="candle_checkpoint")
        self._coordinator.clear_persistence_fault(symbol, reason="paper_state_checkpoint")

    # -- Async run/stop (checkpoint'li run döngüsü) ------------------------

    async def run(self) -> None:
        """`RuntimeCoordinator.run()`'ın (Faz 6) KENDİSİ, iç döngüsünde
        `self.ingest_candle` (COORDINATOR'ın kendi metodu) çağırır —
        checkpoint yazan WRAPPER metodunu DEĞİL. Bu yüzden sürekli/canlı
        çalıştırma için `PersistedRuntime` KENDİ tüketim döngüsünü sağlar
        (aynı akış şekli, ama `self.ingest_candle`/`ingest_order_book`
        ÜZERİNDEN — bu ikisi checkpoint yazar). Üretilen task'lar
        `coordinator._tasks`'a EKLENİR, böylece `coordinator.stop()`
        (DEĞİŞTİRİLMEDEN) bunları da doğru şekilde iptal eder."""
        if self._coordinator._stopped:
            raise ValueError("stopped bir PersistedRuntime tekrar run() ile başlatılamaz")

        tasks: list[asyncio.Task] = []
        for symbol in self._coordinator._symbols:
            tasks.extend(self._spawn_symbol_tasks(symbol))

        await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn_symbol_tasks(self, symbol: str) -> list[asyncio.Task]:
        """Factored out of `run()` so `add_symbol` (Phase 16 PRODUCTION
        HOT-RESELECTION FIX, below) reuses the EXACT same checkpoint-aware
        task-creation code path — never a second implementation that could
        drift. Tracks the created tasks BOTH in the flat `coordinator.
        _tasks` (so `coordinator.stop()`'s existing full-shutdown
        cancellation is completely unchanged) AND in `self._tasks_by_symbol`
        (so `remove_symbol` can cancel just this symbol's own tasks)."""
        created: list[asyncio.Task] = []
        for timeframe in self._coordinator._candle_timeframes:
            created.append(asyncio.create_task(self._consume_candles(symbol, timeframe)))
        created.append(asyncio.create_task(self._consume_order_book(symbol)))
        self._coordinator._tasks.extend(created)
        self._tasks_by_symbol[symbol] = created
        return created

    async def add_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: hot-adds a symbol's
        OWN checkpoint-writing consumer tasks — ONLY meaningful when this
        `PersistedRuntime` is itself the top-level composed runtime (bridge
        disabled, `Application._runtime is persisted_runtime`). Idempotent:
        a symbol that already has active task ownership here is a no-op
        (never a duplicate subscription).

        Bootstrap/state establishment is delegated ONCE to
        `RuntimeCoordinator.add_symbol()` — that method spawns tasks itself
        only when `coordinator._running` is `True`, which is NEVER the case
        in production (production never calls `RuntimeCoordinator.run()`
        directly, see `coordinator.py`/`app.py`), so this call is
        structurally guaranteed to be state+bootstrap ONLY here — never a
        second, competing set of bare coordinator tasks."""
        normalized = normalize_symbol(symbol)
        if normalized in self._tasks_by_symbol:
            return
        await self._coordinator.add_symbol(normalized)
        if self._coordinator._stopped:
            return
        self._spawn_symbol_tasks(normalized)

    async def remove_symbol(self, symbol: str) -> None:
        """Phase 16 PRODUCTION HOT-RESELECTION FIX: cleanly cancels JUST
        this symbol's own checkpoint-writing tasks, then delegates state
        cleanup to `RuntimeCoordinator.remove_symbol()`. Idempotent:
        removing a symbol with no active task ownership here is a no-op."""
        normalized = normalize_symbol(symbol)
        tasks = self._tasks_by_symbol.pop(normalized, [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._coordinator._tasks = [t for t in self._coordinator._tasks if t not in tasks]
        await self._coordinator.remove_symbol(normalized)

    async def _consume_candles(self, symbol: str, timeframe: Timeframe) -> None:
        """Bölüm (Faz 9 soak testinde bulunan BLOCKER'ın düzeltmesi):
        `RuntimeCoordinator._consume_candles`'ın (Faz 6, DEĞİŞTİRİLMEDEN)
        `except Exception: mark_disconnected(symbol); raise` disiplini
        BURADA DA uygulanır — aksi halde GERÇEK üretim giriş noktasında
        (Faz 8 `Application` -> bu metod) bir stream kopması `gather(...,
        return_exceptions=True)` tarafından SESSİZCE yutulur ve health,
        staleness eşiği dolana KADAR yanlışlıkla READY görünmeye devam
        eder (bkz. DECISIONS.md Karar 67).

        BLOCKER-1 FİX (Faz 7 pre-audit remediation — bkz. DECISIONS.md):
        bir gap tespit edildiğinde, eski davranış bare `self._coordinator.
        resolve_gap(...)`'i çağırıp SONRA yalnızca candle checkpoint'ini
        elle yazıyordu — gap resolution'ın ürettiği bir Paper transition
        (bare coordinator'ın ingest'i mutation ÜRETEBİLİR) hiçbir zaman
        durable hâle GELMİYORDU. ARTIK `self.resolve_gap(...)` (BU
        wrapper'ın KENDİ, checkpoint yazan metodu) çağrılır — backfill
        AYNI kalır (`RuntimeCoordinator.resolve_gap_backfill()` üzerinden,
        hiçbir mantık TEKRARLANMADAN), ama son (`pending_candle`) ingestion'ı
        `self.ingest_candle()` üzerinden geçer, dolayısıyla candle checkpoint'i
        VE (varsa) ürettiği Paper transition AYNI atomik transaction'da
        (bkz. HIGH-1 fix) checkpoint'lenir — elle ayrı bir `_checkpoint_candle`
        çağrısına ARTIK gerek YOKTUR (`self.resolve_gap` içindeki
        `self.ingest_candle` bunu ZATEN yapar)."""
        try:
            async for candle in self._coordinator._provider.stream_candles(symbol, timeframe):
                if self._coordinator._stopped:
                    break
                event = self.ingest_candle(symbol, timeframe, candle)
                if event.outcome is IngestOutcome.GAP_DETECTED:
                    await self.resolve_gap(symbol, timeframe, candle)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._coordinator.mark_disconnected(symbol)
            raise

    async def _consume_order_book(self, symbol: str) -> None:
        """Bkz. `_consume_candles` docstring'i — aynı disiplin."""
        try:
            async for snapshot in self._coordinator._provider.stream_order_book(
                symbol, self._coordinator._order_book_depth
            ):
                if self._coordinator._stopped:
                    break
                self.ingest_order_book(symbol, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._coordinator.mark_disconnected(symbol)
            raise

    async def stop(self) -> None:
        await self._coordinator.stop()
        self._tasks_by_symbol.clear()
        self._store.close()


=== FILE: crypto_signal_engine/persistence/serialization.py ===
"""
Faz 7 — durable serialization: kabul edilmiş Faz 4/5 domain nesnelerini
(Signal, AgentEvidence, PaperPosition, PaperFill, PaperOrder) JSON-safe
dict'lere ve GERİYE çevirir.

Kural (Bölüm — "Persistence is a boundary around accepted runtime/domain
state"): Bu modül, Faz 1/4/5 domain modellerini DEĞİŞTİRMEZ (hiçbir
`to_dict()`/`from_dict()` metodu domain sınıflarına EKLENMEZ) — dönüşüm
tamamen bu modülün DIŞARIDAN, saf fonksiyonlarıyla yapılır.

Deterministic: aynı domain nesnesi HER ZAMAN aynı dict'i üretir (sabit
alan sırası, ISO-8601 UTC zaman damgaları, enum `.value` string'leri).
Deserialize edilen nesneler, domain sınıflarının KENDİ `__post_init__`
validasyonundan geçer — bozuk/geçersiz veri (yanlış enum değeri, naive
datetime, sonsuz/NaN sayı, vb.) domain katmanının KENDİ hata tipleriyle
(ValueError/TypeError) reddedilir; bu modül bunları YUTMAZ, çağıran
(`paper_state_store.py`) bunları `CorruptRecordError`'a çevirir.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.paper_trading.models import (
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PositionSide,
)


def _dt_to_str(value: datetime) -> str:
    return value.isoformat()


def _str_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


# -- AgentEvidence -----------------------------------------------------------

def agent_evidence_to_dict(evidence: AgentEvidence) -> dict:
    return {
        "agent": evidence.agent.value,
        "score": evidence.score,
        "rationale": evidence.rationale,
        "primary_timeframe": evidence.primary_timeframe.value,
        "symbol": evidence.symbol,
        "as_of": _dt_to_str(evidence.as_of),
        "context_id": evidence.context_id,
        "supporting_metrics": dict(evidence.supporting_metrics),
    }


def dict_to_agent_evidence(data: dict) -> AgentEvidence:
    return AgentEvidence(
        agent=AgentName(data["agent"]),
        score=data["score"],
        rationale=data["rationale"],
        primary_timeframe=Timeframe(data["primary_timeframe"]),
        symbol=data["symbol"],
        as_of=_str_to_dt(data["as_of"]),
        context_id=data["context_id"],
        supporting_metrics=dict(data["supporting_metrics"]),
    )


# -- Signal --------------------------------------------------------------

def signal_to_dict(signal: Signal) -> dict:
    return {
        "symbol": signal.symbol,
        "timestamp": _dt_to_str(signal.timestamp),
        "context_id": signal.context_id,
        "score": signal.score,
        "confidence": signal.confidence,
        "risk_level": signal.risk_level.value,
        "primary_timeframe": signal.primary_timeframe.value,
        "supporting_factors": [agent_evidence_to_dict(e) for e in signal.supporting_factors],
        "contradicting_factors": [agent_evidence_to_dict(e) for e in signal.contradicting_factors],
        "invalidation": signal.invalidation,
        "model_version": signal.model_version,
    }


def dict_to_signal(data: dict) -> Signal:
    return Signal(
        symbol=data["symbol"],
        timestamp=_str_to_dt(data["timestamp"]),
        context_id=data["context_id"],
        score=data["score"],
        confidence=data["confidence"],
        risk_level=RiskLevel(data["risk_level"]),
        primary_timeframe=Timeframe(data["primary_timeframe"]),
        supporting_factors=tuple(dict_to_agent_evidence(e) for e in data["supporting_factors"]),
        contradicting_factors=tuple(dict_to_agent_evidence(e) for e in data["contradicting_factors"]),
        invalidation=data["invalidation"],
        model_version=data["model_version"],
    )


# -- PaperPosition -------------------------------------------------------

def position_to_dict(position: PaperPosition) -> dict:
    return {
        "symbol": position.symbol,
        "side": position.side.value,
        "quantity": position.quantity,
        "average_entry_price": position.average_entry_price,
        "realized_pnl": position.realized_pnl,
        "updated_at": _dt_to_str(position.updated_at),
    }


def dict_to_position(data: dict) -> PaperPosition:
    return PaperPosition(
        symbol=data["symbol"],
        side=PositionSide(data["side"]),
        quantity=data["quantity"],
        average_entry_price=data["average_entry_price"],
        realized_pnl=data["realized_pnl"],
        updated_at=_str_to_dt(data["updated_at"]),
    )


# -- PaperFill / PaperOrder ------------------------------------------------

def fill_to_dict(fill: PaperFill) -> dict:
    return {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "symbol": fill.symbol,
        "side": fill.side.value,
        "quantity": fill.quantity,
        "price": fill.price,
        "fee": fill.fee,
        "filled_at": _dt_to_str(fill.filled_at),
    }


def dict_to_fill(data: dict) -> PaperFill:
    return PaperFill(
        fill_id=data["fill_id"],
        order_id=data["order_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        quantity=data["quantity"],
        price=data["price"],
        fee=data["fee"],
        filled_at=_str_to_dt(data["filled_at"]),
    )


def order_to_dict(order: PaperOrder) -> dict:
    return {
        "order_id": order.order_id,
        "symbol": order.symbol,
        "side": order.side.value,
        "quantity": order.quantity,
        "signal_context_id": order.signal_context_id,
        "created_at": _dt_to_str(order.created_at),
    }


def dict_to_order(data: dict) -> PaperOrder:
    return PaperOrder(
        order_id=data["order_id"],
        symbol=data["symbol"],
        side=OrderSide(data["side"]),
        quantity=data["quantity"],
        signal_context_id=data["signal_context_id"],
        created_at=_str_to_dt(data["created_at"]),
    )


=== FILE: crypto_signal_engine/persistence/sqlite_store.py ===
"""
SQLite tabanlı `CandleStateStore` implementasyonu.

Kural (Bölüm 12):
- parameterized SQL (hiçbir yerde string interpolation ile SQL kurulmaz)
- explicit schema creation + deterministic schema_version
- transaction boundaries (`with self._connection:`)
- proper close()
- no global hidden connection (her instance kendi connection'ını sahiplenir)
- thread/async ownership AÇIK: bu sınıf SENKRONDUR; async bağlamda
  kullanılacaksa çağıran taraf `asyncio.to_thread(...)` ile sarmalamalıdır
  (Bölüm 13 — domain/persistence senkron kalır, yalnızca network async'tir)
- duplicate handling explicit: `INSERT ... ON CONFLICT DO UPDATE`, ama bu
  yalnızca CandleSequencer'ın ZATEN kabul ettiği (duplicate/out-of-order
  reddedilmiş) update'ler için çağrılır — gerçek "duplicate" asla persist
  edilmez, yalnızca AYNI identity'nin ardışık kabul edilen update'leri
  upsert edilir.
- primary key: (symbol, timeframe, open_time_ms) — `close_time` KESİNLİKLE
  uniqueness key olarak kullanılmaz (Phase 1 Karar: candle identity
  close_time içermez).
- Rejected quality data buraya HİÇ ULAŞMAZ (bkz. commit() — quality
  reddi sequencer'a bile gitmeden erken döner).
"""

from __future__ import annotations

import copy
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.domain.candle_sequencing import CandleSequencer, CandleUpdate
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, CandleIdentity
from crypto_signal_engine.domain.state_contract import CandleStateStore, CommitOutcome, CommitResult
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.quality.base import DataQualityResult

SCHEMA_VERSION = 1

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time_ms INTEGER NOT NULL,
    close_time_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    is_closed INTEGER NOT NULL,
    trade_count INTEGER,
    update_seq INTEGER NOT NULL,
    PRIMARY KEY (symbol, timeframe, open_time_ms)
);
"""


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


class SqliteCandleStateStore(CandleStateStore):
    """`CandleStateStore` contract'ının SQLite-backed implementasyonu.

    Sequencing kararı (duplicate/out-of-order/after-final) her zaman
    in-memory `CandleSequencer` tarafından verilir — SQLite yalnızca kabul
    edilmiş sonucun DAYANIKLI (durable) kopyasını tutar. Bu ayrım, aynı
    saf sequencing mantığının hem `InMemoryCandleStateStore` hem
    `SqliteCandleStateStore` arasında TUTARLI kalmasını sağlar.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        try:
            self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise PersistenceError(f"SQLite bağlantısı açılamadı ({self._db_path}): {exc}") from exc
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._sequencer = CandleSequencer()
        self._init_schema()
        self._restore_sequencer_from_disk()

    def _init_schema(self) -> None:
        try:
            with self._connection:
                self._connection.executescript(_CREATE_SCHEMA_SQL)
                row = self._connection.execute("SELECT COUNT(*) FROM schema_version").fetchone()
                if row[0] == 0:
                    self._connection.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
        except sqlite3.Error as exc:
            raise PersistenceError(f"Schema oluşturma/doğrulama başarısız: {exc}") from exc

    def _restore_sequencer_from_disk(self) -> None:
        """Restart sonrası diskteki mevcut candle'ları in-memory sequencer'a
        yükler ki canonical state süreklilik göstersin.

        Not: burada quality-gate TEKRAR çalıştırılmaz — bu satırlar daha
        önce zaten quality-gate'ten geçip commit edilmiş verilerdir.
        """
        try:
            cursor = self._connection.execute(
                "SELECT symbol, timeframe, open_time_ms, close_time_ms, open, high, low, "
                "close, volume, is_closed, trade_count, update_seq FROM candles "
                "ORDER BY symbol, timeframe, open_time_ms ASC"
            )
            rows = cursor.fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(f"Mevcut candle'lar okunamadı: {exc}") from exc

        for row in rows:
            (symbol, timeframe_str, open_ms, close_ms, open_, high, low, close, volume,
             is_closed, trade_count, update_seq) = row
            candle = Candle(
                symbol=symbol,
                timeframe=Timeframe(timeframe_str),
                open_time=_from_epoch_ms(open_ms),
                close_time=_from_epoch_ms(close_ms),
                open=open_, high=high, low=low, close=close, volume=volume,
                is_closed=bool(is_closed), trade_count=trade_count,
            )
            update = CandleUpdate(
                candle=candle, update_seq=update_seq,
                event_time=candle.close_time, received_at=candle.close_time,
            )
            # Diskten okunan satırlar zaten sırayla ve geçerli; taze bir
            # sequencer'a uygulanması her zaman kabul edilir.
            self._sequencer.apply(update)

    def peek_previous(self, identity: CandleIdentity) -> Candle | None:
        return self._sequencer.peek(identity)

    def commit(self, quality_result: DataQualityResult, update: CandleUpdate) -> CommitResult:
        """Quality-gate'ten geçen bir update'i ATOMIK olarak commit etmeyi dener.

        ATOMICITY INVARIANT: SQLite persist BAŞARISIZ olursa, canonical
        in-memory `self._sequencer` durumu DEĞİŞMEMİŞ olmalıdır — ne
        `_latest` ne `_finalized` ilerlemiş olmalı. Bunu sağlamak için:

        1. Sequencing kararı önce bir SCRATCH (deep-copy) sequencer üzerinde
           "dry-run" edilir — bu, GERÇEK `self._sequencer`'ı HİÇ MUTATE
           ETMEZ.
        2. Dry-run sonucu REJECTED_SEQUENCE ise, gerçek sequencer'a hiç
           dokunulmadan doğrudan döner.
        3. Dry-run sonucu ACCEPTED ise, ÖNCE SQLite'a persist edilir.
           Persist başarısız olursa `PersistenceError` YUKARI FIRLATILIR
           ve `self._sequencer` HÂLÂ ESKİ HÂLİNDEDİR (hiç çağrılmadı).
        4. Persist başarılı olursa, AYNI update GERÇEK `self._sequencer`'a
           uygulanır (dry-run'da doğrulanmış olduğundan bu adım her zaman
           deterministik olarak aynı ACCEPTED sonucu üretir).

        Bu, `CandleSequencer`'ın (Phase 1 domain modülü) kaynak kodunu
        HİÇ DEĞİŞTİRMEDEN, yalnızca standart kütüphane `copy.deepcopy` ile
        Phase 2 tarafında sağlanan bir atomicity garantisidir.
        """
        if not quality_result.passed:
            # KRİTİK INVARIANT: reddedilen veri ne sequencer'a ne de
            # SQLite'a ulaşır.
            return CommitResult(
                outcome=CommitOutcome.REJECTED_QUALITY,
                canonical_candle=self._sequencer.peek(update.identity),
                detail=f"data quality reddetti: {quality_result.status} — {quality_result.reason}",
            )

        # 1-2. Dry-run: gerçek sequencer'ı mutate ETMEDEN sonucu öğren.
        scratch_sequencer = copy.deepcopy(self._sequencer)
        dry_run_result = scratch_sequencer.apply(update)
        if not dry_run_result.accepted:
            return CommitResult(
                outcome=CommitOutcome.REJECTED_SEQUENCE,
                canonical_candle=dry_run_result.canonical_candle,
                detail=f"sequence reddetti: {dry_run_result.outcome}",
            )

        # 3. Persist ÖNCE dener; başarısız olursa gerçek sequencer'a hiç
        #    dokunulmamış olarak (aşağıdaki satıra hiç ulaşılmadan)
        #    PersistenceError yukarı fırlatılır.
        self._persist(update)

        # 4. Persist başarılı: şimdi GERÇEK sequencer'a aynı update'i
        #    uygula — dry-run'da doğrulandığı için deterministik olarak
        #    aynı ACCEPTED sonucu üretecektir.
        real_result = self._sequencer.apply(update)
        return CommitResult(
            outcome=CommitOutcome.COMMITTED,
            canonical_candle=real_result.canonical_candle,
            detail=f"commit edildi (SQLite): {real_result.outcome}",
        )

    def _persist(self, update: CandleUpdate) -> None:
        c = update.candle
        try:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO candles
                        (symbol, timeframe, open_time_ms, close_time_ms, open, high, low,
                         close, volume, is_closed, trade_count, update_seq)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, timeframe, open_time_ms) DO UPDATE SET
                        close_time_ms = excluded.close_time_ms,
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        is_closed = excluded.is_closed,
                        trade_count = excluded.trade_count,
                        update_seq = excluded.update_seq
                    """,
                    (
                        c.symbol, c.timeframe.value, _to_epoch_ms(c.open_time), _to_epoch_ms(c.close_time),
                        c.open, c.high, c.low, c.close, c.volume, int(c.is_closed), c.trade_count,
                        update.update_seq,
                    ),
                )
        except sqlite3.Error as exc:
            raise PersistenceError(f"Candle persist edilemedi ({c.symbol}/{c.timeframe}): {exc}") from exc

    def close(self) -> None:
        """Bağlantıyı kapatır. Idempotent değildir; iki kez çağrılırsa
        sqlite3 kendi hatasını fırlatır (bu, açık ve beklenen bir davranıştır)."""
        self._connection.close()


=== FILE: crypto_signal_engine/portfolio/__init__.py ===
"""
Portfolio/Accounting v1 — the authoritative, reusable accounting layer
for the autonomous Testnet trading lifecycle bridge.

Kapsam sınırı (KASITLI): bu paket YALNIZCA zaten var olan durable state
(`BridgePositionRecord`, `bridge_completed_trade` toplamları, exchange
bakiye sorgusu) üzerinden salt-okunur, saf muhasebe/gözlemlenebilirlik
hesaplar — hiçbir order göndermez, hiçbir mevcut risk kapısını
(`RiskPolicyConfig`/`entry_gate()`) değiştirmez veya BLOKE ETMEZ. Bu bir
düzeltme + gözlemlenebilirlik milestone'udur, YENİ bir kontrol DEĞİLDİR."""

from __future__ import annotations


=== FILE: crypto_signal_engine/portfolio/accounting.py ===
"""
Portfolio/Accounting v1, step 3 — the ONE authoritative, reusable,
PURE accounting snapshot for the autonomous Testnet trading lifecycle
bridge, replacing the ad-hoc, duplicated/inconsistent math previously
scattered across `LifecycleManager.entry_gate()` (exposure) and
`app.py::_bridge_lifecycle_snapshot()` (exposure AGAIN, plus a
bounded-limit-50/GROSS "realized P&L" and "lifetime P&L" that were
silently different numbers from the real, authoritative,
`bridge_daily_risk`-persisted, NET risk-breaker accumulator).

PURE, READ-ONLY, NO I/O OF ITS OWN — same discipline as `adaptive.
decision.evidence_from_pnls`/`research.attribution._compute_metrics`:
this module takes already-fetched data as plain arguments and performs
zero database/network access itself. It deliberately does NOT import
`crypto_signal_engine.execution.lifecycle_store.LifecycleStore` (the
data-access layer) — only pure types/functions from `crypto_signal_
engine.execution.lifecycle` (`BridgePositionRecord`, `PositionLifecycleState`,
`compute_total_exposure`, `unrealized_gross_pnl`) — mirroring the
`adaptive/symbol_score.py` precedent of keeping a pure computation
module decoupled from the store that happens to feed it. The caller
(`app.py`) owns fetching `LifecycleStore.list_positions()`/`total_
realized_pnl()`/`realized_pnl_for_day()` and wiring the results in here.

Nothing in this module can submit an order, block an entry, or alter any
existing `RiskPolicyConfig` gate — it is read-only observability +
correctness, never a new control.

Step 5 — `AssetBalance`/`parse_account_balances()`: minimal, typed
parsing for Binance's `GET /api/v3/account` payload (`testnet_client.
py::account_info()` — already implemented, already working, previously
called from EXACTLY ZERO places). Deliberately kept PURE here too (no
network call — this module only parses an already-fetched raw `dict`);
the actual `account_info()` call, and its defensive "never raise, never
block trading, degrade to `None` on any failure" wrapping, lives in
`ExecutionReconciliationService` (see `execution/reconciliation_service.
py::check_usdt_balance` and this milestone's DECISIONS.md entry for why
it lives there rather than here — it already owns the exchange client
and the "compare internal state against exchange truth" pattern this
extends, and this module stays free of any exchange-client dependency)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from crypto_signal_engine.domain._validation import require_finite, require_utc_aware
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    PositionLifecycleState,
    compute_total_exposure,
    unrealized_gross_pnl,
)


@dataclass(frozen=True)
class AccountingSnapshot:
    """One point-in-time, fully-computed portfolio accounting snapshot.

    `unrealized_pnl_usdt` is `None` whenever ANY open position's live
    price lookup failed or returned nothing — NEVER a partial/fabricated
    sum over only the positions that happened to resolve (same
    discipline `app.py`'s existing `unrealized_gross_pnl_total` already
    followed; this module reuses that discipline rather than inventing a
    new one). `lifetime_realized_pnl_net_usdt`/`lifetime_trade_count` and
    `today_realized_pnl_net_usdt`/`today_trade_count` are expected to be
    computed via `LifecycleStore.total_realized_pnl()`/`realized_pnl_
    for_day()` (step 2's SQL-aggregate functions) — TRUE lifetime/daily
    totals, NET of fees, never a bounded/gross approximation."""

    generated_at: datetime
    open_position_count: int
    total_exposure_usdt: float
    unrealized_pnl_usdt: float | None
    lifetime_realized_pnl_net_usdt: float
    lifetime_trade_count: int
    today_realized_pnl_net_usdt: float
    today_trade_count: int

    def __post_init__(self) -> None:
        require_utc_aware(self.generated_at, "generated_at")
        for value, name in (
            (self.total_exposure_usdt, "total_exposure_usdt"),
            (self.lifetime_realized_pnl_net_usdt, "lifetime_realized_pnl_net_usdt"),
            (self.today_realized_pnl_net_usdt, "today_realized_pnl_net_usdt"),
        ):
            require_finite(value, name)
        if self.unrealized_pnl_usdt is not None:
            require_finite(self.unrealized_pnl_usdt, "unrealized_pnl_usdt")
        if self.open_position_count < 0:
            raise ValueError("open_position_count negatif olamaz")
        if self.lifetime_trade_count < 0:
            raise ValueError("lifetime_trade_count negatif olamaz")
        if self.today_trade_count < 0:
            raise ValueError("today_trade_count negatif olamaz")


def _has_sellable_quantity(position: BridgePositionRecord) -> bool:
    """Mirrors `app.py`'s existing `has_quantity` condition exactly (the
    same test that already gated per-position `latest_price`/`unrealized_
    gross_pnl` lookups there) — never a second, divergent definition of
    "this position needs a live price"."""
    return (
        position.state is not PositionLifecycleState.FLAT
        and position.gross_entry_vwap is not None
        and position.net_owned_base_quantity > 0
    )


def build_accounting_snapshot(
    *,
    positions: Sequence[BridgePositionRecord],
    price_lookup: Callable[[str], float | None],
    lifetime_realized_pnl_net_usdt: float,
    lifetime_trade_count: int,
    today_realized_pnl_net_usdt: float,
    today_trade_count: int,
    now: datetime,
) -> AccountingSnapshot:
    """The ONE function that computes a full `AccountingSnapshot`.

    `positions`: every currently-tracked `BridgePositionRecord` (e.g.
    `LifecycleStore.list_positions()`'s result) — this function itself
    never fetches them.

    `price_lookup(symbol)`: caller-supplied, e.g. `Application.
    _latest_price` — called ONCE per open (non-FLAT, quantified) position
    to obtain a live mark for unrealized P&L. Defensively wrapped here:
    if it RAISES for any relevant symbol, that is treated exactly like a
    `None` return (unresolvable price) — `unrealized_pnl_usdt` becomes
    `None` for the WHOLE snapshot, never a partial sum silently excluding
    the failed symbol.

    `lifetime_realized_pnl_net_usdt`/`lifetime_trade_count`/`today_
    realized_pnl_net_usdt`/`today_trade_count`: already-computed
    aggregates (see `LifecycleStore.total_realized_pnl()`/`realized_pnl_
    for_day()`) — this function performs no trade-history aggregation of
    its own, only the exposure/unrealized computation over `positions`.

    Deterministic for identical inputs (no wall-clock read beyond the
    caller-supplied `now`, no hidden state)."""
    require_utc_aware(now, "now")

    open_position_count = sum(1 for position in positions if position.state is not PositionLifecycleState.FLAT)
    total_exposure_usdt = compute_total_exposure(positions)

    unrealized_total = 0.0
    unrealized_known = True
    for position in positions:
        if not _has_sellable_quantity(position):
            continue
        try:
            price = price_lookup(position.symbol)
        except Exception:  # noqa: BLE001 - a price-lookup failure must NEVER crash this snapshot, only mark it unknown
            price = None
        if price is None:
            unrealized_known = False
            continue
        unrealized_total += unrealized_gross_pnl(
            gross_entry_vwap=position.gross_entry_vwap,  # type: ignore[arg-type]
            net_owned_base_quantity=position.net_owned_base_quantity,
            current_price=price,
        )

    return AccountingSnapshot(
        generated_at=now,
        open_position_count=open_position_count,
        total_exposure_usdt=total_exposure_usdt,
        unrealized_pnl_usdt=unrealized_total if unrealized_known else None,
        lifetime_realized_pnl_net_usdt=lifetime_realized_pnl_net_usdt,
        lifetime_trade_count=lifetime_trade_count,
        today_realized_pnl_net_usdt=today_realized_pnl_net_usdt,
        today_trade_count=today_trade_count,
    )


@dataclass(frozen=True)
class AssetBalance:
    """One asset's free/locked balance, parsed from Binance's `GET
    /api/v3/account` response. Deliberately minimal — the raw payload
    (which also carries account-level permission flags, commission
    rates, and update timestamps this bridge has no use for) is NEVER
    leaked further than `parse_account_balances()` below, mirroring
    `ExecutionResult`'s own "raw_response never sızdırılır" rule."""

    asset: str
    free: float
    locked: float

    def __post_init__(self) -> None:
        if not self.asset:
            raise ValueError("AssetBalance.asset boş olamaz")
        require_finite(self.free, "free")
        require_finite(self.locked, "locked")
        if self.free < 0:
            raise ValueError(f"AssetBalance.free negatif olamaz, alınan: {self.free}")
        if self.locked < 0:
            raise ValueError(f"AssetBalance.locked negatif olamaz, alınan: {self.locked}")


def parse_account_balances(raw: dict) -> tuple[AssetBalance, ...]:
    """Parses `testnet_client.py::account_info()`'s raw JSON payload's
    `"balances"` array into typed `AssetBalance` records. Every OTHER
    top-level field (`accountType`, `permissions`, `commissionRates`,
    `updateTime`, ...) is deliberately ignored — this function's only job
    is the balances array, and the raw `dict` itself is never returned or
    otherwise propagated by this function's caller (see `Execution
    ReconciliationService.check_usdt_balance`)."""
    balances_raw = raw.get("balances", [])
    if not isinstance(balances_raw, list):
        raise ValueError("account_info payload'ında 'balances' bir liste değil")
    balances = []
    for entry in balances_raw:
        asset = entry.get("asset")
        free = entry.get("free")
        locked = entry.get("locked")
        if asset is None or free is None or locked is None:
            raise ValueError(f"eksik balance alanı (asset/free/locked): {entry!r}")
        balances.append(AssetBalance(asset=asset, free=float(free), locked=float(locked)))
    return tuple(balances)


=== FILE: crypto_signal_engine/providers/__init__.py ===



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


=== FILE: crypto_signal_engine/quality/__init__.py ===



=== FILE: crypto_signal_engine/quality/base.py ===
"""
Data quality contract.

Kural: quant sonuçlarından önce veri kalitesi kontrol edilmelidir.
Sorunlu veri quant engine'e DOĞRUDAN VERİLMEZ (bkz. domain/state_contract.py
— bu invariant orada kod seviyesinde uygulanır).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field

from crypto_signal_engine.domain._validation import freeze_mapping, require_enum
from crypto_signal_engine.domain.enums import DataQualityStatus
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade


@dataclass(frozen=True)
class DataQualityResult:
    """Bir veri kalitesi kontrolünün sonucu.

    `details` gerçek immutable bir mapping'e sarılır (Quality Gate 4).

    HARDENING NOTU (Quality Gate 27):
    - `status` GERÇEK bir DataQualityStatus enum instance'ı olmak zorundadır.
    - `reason` bir str olmak zorundadır (TypeError, aksi halde).
    - `status != OK` iken `reason` BOŞ OLAMAZ — "neden reddedildiği" bilgisi
      olmadan bir red kararı invalid state kabul edilir.
    """

    status: DataQualityStatus
    reason: str
    details: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_enum(self.status, DataQualityStatus, "status")
        if not isinstance(self.reason, str):
            raise TypeError(f"reason bir str olmalı, alınan: {type(self.reason).__name__}")
        if self.status != DataQualityStatus.OK and not self.reason.strip():
            raise ValueError(
                f"status={self.status} (OK değil) iken reason boş olamaz — "
                f"red kararının gerekçesi izlenebilir olmalı"
            )
        object.__setattr__(self, "details", freeze_mapping(self.details, "details"))

    @property
    def passed(self) -> bool:
        return self.status == DataQualityStatus.OK

    @staticmethod
    def ok() -> "DataQualityResult":
        return DataQualityResult(status=DataQualityStatus.OK, reason="")


class DataQualityGate(ABC):
    """Provider -> DataQualityGate -> CandleStateStore.commit() sınırındaki kontrol katmanı."""

    @abstractmethod
    def check_candle(self, candle: Candle, previous: Candle | None) -> DataQualityResult:
        raise NotImplementedError

    @abstractmethod
    def check_trade(self, trade: Trade, previous: Trade | None) -> DataQualityResult:
        raise NotImplementedError

    @abstractmethod
    def check_order_book(
        self, snapshot: OrderBookSnapshot, previous: OrderBookSnapshot | None
    ) -> DataQualityResult:
        raise NotImplementedError


=== FILE: crypto_signal_engine/quality/binance_rules.py ===
"""
Binance market data için concrete `DataQualityGate` implementasyonu.

Kural (Bölüm 10): Quality = "veri güvenilir mi?" sorusuna cevap verir.
Strategy = "fiyat nereye gider?" sorusuna cevap verir. Bu modülde
İKİNCİSİNE dair HİÇBİR ŞEY yoktur — yalnızca güvenilirlik kontrolleri.

NAMING NOTU (dürüst, dokümante edilmiş bir sınır): Phase 1'in
`DataQualityStatus` enum'u üç check_* metodunda da (candle/trade/
order_book) PAYLAŞILAN tek bir status kümesidir. Bazı isimler ("
MISSING_CANDLE", "DUPLICATE_CANDLE") candle'a özgü görünse de, bu
Phase 1'in kasıtlı tasarımıdır (`DataQualityResult` üç check_* metodu
için de aynı tip). Bu modül, trade/order-book bağlamında candle-isimli
status'leri EN YAKIN semantik anlamlarıyla YENİDEN KULLANIR (örn. trade
sequence sorunları için TIMESTAMP_MISMATCH, order-book gap için
WEBSOCKET_GAP) — Phase 1 enum'unu SESSİZCE genişletmek yerine. Bu karar
PHASE2_MARKET_DATA.md'de açıkça belgelenmiştir.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookSnapshot, Trade
from crypto_signal_engine.providers.binance.clock import Clock
from crypto_signal_engine.quality.base import DataQualityGate, DataQualityResult

_TIMEFRAME_DURATION: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(minutes=1),
    Timeframe.M5: timedelta(minutes=5),
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.H1: timedelta(hours=1),
}


@dataclass(frozen=True)
class BinanceQualityThresholds:
    """Baseline outlier/staleness eşikleri (magic number değil, adlandırılmış).

    Bunlar güvenli/konservatif başlangıç değerleridir; config/risk.yaml
    benzeri bir dosyaya taşınması Faz 3+ için deferred bir karardır
    (bkz. DECISIONS.md).
    """

    max_candle_staleness_seconds: float = 300.0  # kapanmış bir candle'ın "şimdi"den ne kadar eski olabileceği
    max_trade_staleness_seconds: float = 60.0
    max_order_book_staleness_seconds: float = 30.0
    max_single_candle_move_ratio: float = 0.20  # önceki close'a göre %20 üstü tek-candle hareket -> outlier
    max_single_trade_move_ratio: float = 0.20


class BinanceDataQualityGate(DataQualityGate):
    """Binance market data için baseline güvenilirlik kontrolleri."""

    def __init__(self, clock: Clock, thresholds: BinanceQualityThresholds | None = None) -> None:
        self._clock = clock
        self._thresholds = thresholds or BinanceQualityThresholds()

    def check_candle(self, candle: Candle, previous: Candle | None) -> DataQualityResult:
        """Realtime (WebSocket) candle kontrolü — hem structural validity
        (A) hem realtime feed freshness (B) uygulanır."""
        return self._check_candle_impl(candle, previous, skip_staleness=False)

    def check_historical_candle(self, candle: Candle, previous: Candle | None) -> DataQualityResult:
        """REST backfill/historical candle kontrolü — YALNIZCA structural
        validity (A) uygulanır; realtime feed freshness (B, staleness)
        UYGULANMAZ.

        Faz 2 Bölüm 2 gereksinimi: historical bir candle doğası gereği
        "şimdi"den eski olur; bu invalid bir durum DEĞİLDİR ve
        `STALE_PRICE` ile reddedilmemelidir. Bu metod Phase 1
        `DataQualityGate` ABC'sinin PARÇASI DEĞİLDİR — yalnızca bu concrete
        sınıfın ek, opsiyonel bir metodudur (bkz. `StateManager.
        handle_historical_candle_update`, duck-typing ile çağırır).
        """
        return self._check_candle_impl(candle, previous, skip_staleness=True)

    def _check_candle_impl(
        self, candle: Candle, previous: Candle | None, *, skip_staleness: bool
    ) -> DataQualityResult:
        # 1. Timestamp/timeframe alignment: open_time, timeframe sınırına hizalı olmalı.
        # (A) — structural validity, her zaman uygulanır.
        duration = _TIMEFRAME_DURATION[candle.timeframe]
        epoch = datetime(1970, 1, 1, tzinfo=candle.open_time.tzinfo)
        offset = (candle.open_time - epoch) % duration
        if offset != timedelta(0):
            return DataQualityResult(
                status=DataQualityStatus.TIMESTAMP_MISMATCH,
                reason=f"open_time ({candle.open_time}) {candle.timeframe} sınırına hizalı değil",
            )

        # 2. Stale timestamp — (B) realtime feed freshness. Yalnızca
        #    `skip_staleness=False` (realtime WS ingestion) iken uygulanır;
        #    historical/backfill candle'lar için ATLANIR (bkz. docstring).
        if not skip_staleness and candle.is_closed:
            now = self._clock.now()
            age = (now - candle.close_time).total_seconds()
            if age > self._thresholds.max_candle_staleness_seconds:
                return DataQualityResult(
                    status=DataQualityStatus.STALE_PRICE,
                    reason=f"candle {age:.1f}s önce kapanmış, eşik {self._thresholds.max_candle_staleness_seconds}s",
                )

        # 3. Suspicious/missing interval — (A) structural, her zaman uygulanır.
        if previous is not None and candle.open_time > previous.open_time:
            expected_open_time = previous.open_time + duration
            if candle.open_time != expected_open_time:
                return DataQualityResult(
                    status=DataQualityStatus.MISSING_CANDLE,
                    reason=(
                        f"beklenen open_time {expected_open_time}, alınan {candle.open_time} "
                        f"— aradaki interval(lar) eksik, backfill gerekli"
                    ),
                )

        # 4. Zero-volume anomaly — (A) structural, her zaman uygulanır.
        if candle.trade_count is not None:
            if candle.volume == 0 and candle.trade_count > 0:
                return DataQualityResult(
                    status=DataQualityStatus.ZERO_VOLUME_ANOMALY,
                    reason=f"volume=0 ama trade_count={candle.trade_count}",
                )

        # 5. Extreme outlier — (A) structural, her zaman uygulanır.
        if previous is not None and previous.close > 0:
            move_ratio = abs(candle.close - previous.close) / previous.close
            if move_ratio > self._thresholds.max_single_candle_move_ratio:
                return DataQualityResult(
                    status=DataQualityStatus.EXTREME_OUTLIER,
                    reason=(
                        f"tek-candle hareketi %{move_ratio * 100:.1f}, eşik "
                        f"%{self._thresholds.max_single_candle_move_ratio * 100:.1f}"
                    ),
                )

        return DataQualityResult.ok()

    def check_trade(self, trade: Trade, previous: Trade | None) -> DataQualityResult:
        now = self._clock.now()
        age = (now - trade.timestamp).total_seconds()
        if age > self._thresholds.max_trade_staleness_seconds:
            return DataQualityResult(
                status=DataQualityStatus.STALE_PRICE,
                reason=f"trade {age:.1f}s önce gerçekleşmiş, eşik {self._thresholds.max_trade_staleness_seconds}s",
            )

        if previous is not None:
            if trade.trade_id <= previous.trade_id:
                # Non-monotonic/duplicate trade ID — Phase 1 enum'unda trade'e
                # özgü bir status olmadığından TIMESTAMP_MISMATCH yeniden
                # kullanılır (bkz. modül docstring'i).
                return DataQualityResult(
                    status=DataQualityStatus.TIMESTAMP_MISMATCH,
                    reason=f"non-monotonic trade_id: {trade.trade_id} <= önceki {previous.trade_id}",
                )
            if previous.price > 0:
                move_ratio = abs(trade.price - previous.price) / previous.price
                if move_ratio > self._thresholds.max_single_trade_move_ratio:
                    return DataQualityResult(
                        status=DataQualityStatus.EXTREME_OUTLIER,
                        reason=(
                            f"ardışık trade fiyat hareketi %{move_ratio * 100:.1f}, eşik "
                            f"%{self._thresholds.max_single_trade_move_ratio * 100:.1f}"
                        ),
                    )

        return DataQualityResult.ok()

    def check_order_book(
        self, snapshot: OrderBookSnapshot, previous: OrderBookSnapshot | None
    ) -> DataQualityResult:
        now = self._clock.now()
        age = (now - snapshot.timestamp).total_seconds()
        if age > self._thresholds.max_order_book_staleness_seconds:
            return DataQualityResult(
                status=DataQualityStatus.STALE_PRICE,
                reason=f"order book {age:.1f}s önce alınmış, eşik {self._thresholds.max_order_book_staleness_seconds}s",
            )

        if previous is not None and snapshot.last_update_id <= previous.last_update_id:
            # Non-monotonic update ID — resync/gap belirtisi.
            return DataQualityResult(
                status=DataQualityStatus.WEBSOCKET_GAP,
                reason=(
                    f"non-monotonic last_update_id: {snapshot.last_update_id} <= "
                    f"önceki {previous.last_update_id}"
                ),
            )

        # Crossed/locked book zaten OrderBookSnapshot constructor'ında
        # engellenir (bu satıra ulaşan bir snapshot yapısal olarak crossed
        # OLAMAZ); yine de defensive bir ikinci kontrol:
        if snapshot.best_bid.price >= snapshot.best_ask.price:
            return DataQualityResult(
                status=DataQualityStatus.EXTREME_OUTLIER,
                reason="crossed/locked book (yapısal olarak beklenmiyordu)",
            )

        return DataQualityResult.ok()


=== FILE: crypto_signal_engine/runtime/__init__.py ===
"""
Faz 6 — Real-Time Market Data & Runtime.

Bu paket, ZATEN kabul edilmiş Faz 2 (Binance PUBLIC market data), Faz 3
(Feature Engine), Faz 4 (agents/consensus/risk/SignalEngine) ve Faz 5
(PaperTradingEngine) bileşenlerini KOORDİNE eder — hiçbirinin mantığını
yeniden uygulamaz veya kopyalamaz (bkz. coordinator.py).

Hiçbir private/signed Binance endpoint'i, API key/secret, order
placement/cancellation, veya Testnet/Mainnet execution İÇERMEZ.
`ALLOW_LIVE_TRADING = False` korunur. Faz 6 SADECE paper trading (Faz 5)
ile PUBLIC market data'yı sürekli/gerçek-zamanlı olarak besler.
"""

from __future__ import annotations


=== FILE: crypto_signal_engine/runtime/bootstrap.py ===
"""
Faz 6 — bootstrap: canlı işlemeye başlamadan önce Faz 3/4 hesaplamalarının
geçerli olması için yeterli public geçmiş veriyi elde eder.

Kural (Bölüm 9 — talimat): "Bootstrap must be deterministic for the same
input data. If insufficient data exists, expose an explicit NOT_READY
state rather than fabricating features/signals." Bu modül YALNIZCA
zaten-fetch-edilmiş bir `Candle` listesini deterministik olarak uygular;
REST çağrısının KENDİSİ (async, Faz 2'nin `fetch_historical_candles()`'ı)
`coordinator.py::RuntimeCoordinator.bootstrap()`'ta yapılır — bu ayrım,
bu fonksiyonun (ve dolayısıyla bootstrap mantığının) ağ olmadan,
senkron ve doğrudan test edilebilir kalmasını sağlar (Bölüm 22).

Aynı REST çağrısı iki kez yapılırsa veya bootstrap ile canlı akış overlap
ederse, uygulama `CandleWindow.offer()` üzerinden geçer — bu yüzden
duplicate/out-of-order candle'lar burada da (Bölüm 12 ile aynı garantiyle)
güvenle SESSİZCE atlanır.
"""

from __future__ import annotations

from datetime import datetime

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.features.engine import FeatureEngine
from crypto_signal_engine.runtime.candle_window import CandleWindow
from crypto_signal_engine.runtime.models import BootstrapReport, IngestOutcome


def apply_bootstrap_candles(
    *,
    window: CandleWindow,
    feature_engine: FeatureEngine,
    symbol: str,
    timeframe: Timeframe,
    candles: list[Candle],
    as_of: datetime,
    min_candles: int,
) -> BootstrapReport:
    """Fetch edilmiş (REST veya canlı gap-fill) geçmiş candle'ları
    kronolojik sırayla pencereye uygular; yeterliyse bir `FeatureSnapshot`
    commit eder.

    `candles` GİRDİ SIRASINA GÜVENİLMEZ — açıkça `open_time`'a göre
    sıralanır (Bölüm 9 — "bootstrap chronological ordering"), ardından
    `window.offer()` ile TEK TEK uygulanır (Bölüm 9 — "duplicate bootstrap
    data handling").
    """
    applied = 0
    for candle in sorted(candles, key=lambda c: c.open_time):
        outcome = window.offer(candle)
        if outcome is IngestOutcome.ACCEPTED:
            applied += 1

    history = window.history()
    ready = len(history) >= min_candles

    generated_at = history[-1].close_time if history else as_of
    if ready:
        snapshot = feature_engine.compute_candle_features(list(history), symbol, timeframe, as_of=generated_at)
        feature_engine.commit_snapshot(snapshot)

    return BootstrapReport(
        symbol=symbol, timeframe=timeframe, candles_applied=applied, ready=ready, generated_at=generated_at
    )


=== FILE: crypto_signal_engine/runtime/candle_window.py ===
"""
Faz 6 — per-(symbol, timeframe) canlı candle penceresi.

NEDEN GEREKLİ (bkz. PHASE6_REALTIME_RUNTIME.md — "Neden ayrı bir candle
penceresi"): `BinanceMarketDataProvider`'ın kendi `StateManager`'ı (Faz 2)
TAMAMEN İÇ (private) bir bileşendir — dışarıdan erişilemez. Faz 3
`FeatureEngine.compute_candle_features()` ise STATELESS'tir: her çağrıda
TAM candle listesini parametre olarak bekler, kendi geçmişini tutmaz.
Runtime'ın feature hesaplaması için kullanacağı candle geçmişini BİR
YERDE tutması gerekir — bu sınıf o yerdir.

KRİTİK AYRIM: Bu sınıf OHLC/kalite doğrulaması YAPMAZ (`DataQualityGate`,
`CandleSequencer` zaten Faz 2'de bunu tamamlamıştır — provider'dan çıkan
her `Candle` zaten kalite-onaylı, kanonik bir candle'dır). Bu sınıf
YALNIZCA runtime'ın KENDİ rolling penceresi için identity-bazlı
(`open_time`) dedup + monoton sıralama garantisi sağlar — bir reconnect
sonrası aynı kapalı candle'ın tekrar teslim edilmesi veya bootstrap/canlı
akış overlap'i gibi runtime-seviyesi senaryolara karşı (Bölüm 12).
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.runtime.models import IngestOutcome


class CandleWindow:
    """Tek bir (symbol, timeframe) için bounded, deterministic candle penceresi."""

    def __init__(self, maxlen: int = 500, *, duration: timedelta | None = None) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen pozitif olmalı")
        self._candles: deque[Candle] = deque(maxlen=maxlen)
        self._last_open_time: datetime | None = None
        # `duration` verilirse (bkz. coordinator.py — HER ZAMAN verilir),
        # gap detection AKTİFTİR: `open_time`'da tam olarak bir interval'lik
        # ilerlemeden FAZLASI bir "atlanmış aralık" (gap) olarak SAYILIR ve
        # ASLA sessizce kabul EDİLMEZ (Bölüm 14 — "never hide a gap by
        # jumping directly to the latest event"). `None` ise (yalnızca
        # geriye-uyumluluk/genel amaçlı kullanım için) gap detection KAPALI.
        self._duration = duration

    def offer(self, candle: Candle) -> IngestOutcome:
        """Bir candle'ı pencereye eklemeyi dener.

        - `is_closed=False` (hâlâ oluşmakta olan candle) HİÇBİR ZAMAN
          pencereye girmez (Bölüm 6 — "never treat a still-forming candle
          as completed") -> `UNCLOSED_SKIPPED`.
        - Aynı `open_time` ikinci kez gelirse (reconnect sonrası yeniden
          teslimat, bootstrap/canlı overlap) -> `DUPLICATE` (no-op; state
          SESSİZCE ezilmez — "conflicting duplicate data must not silently
          overwrite trusted state").
        - Daha eski bir `open_time` gelirse -> `OUT_OF_ORDER` (no-op; state
          ASLA geriye alınmaz).
        - Bir sonraki BEKLENEN `open_time`'dan (son + `duration`) DAHA
          İLERİ bir `open_time` gelirse -> `GAP_DETECTED` (no-op; candle
          HENÜZ pencereye KABUL EDİLMEZ — çağıran taraf önce eksik aralığı
          doldurmalıdır, bkz. `coordinator.py::resolve_gap`).
        - Aksi halde (kesin olarak bir sonraki beklenen `open_time`)
          -> `ACCEPTED`.
        """
        if not candle.is_closed:
            return IngestOutcome.UNCLOSED_SKIPPED

        if self._last_open_time is not None:
            if candle.open_time == self._last_open_time:
                return IngestOutcome.DUPLICATE
            if candle.open_time < self._last_open_time:
                return IngestOutcome.OUT_OF_ORDER
            if self._duration is not None and candle.open_time > self._last_open_time + self._duration:
                return IngestOutcome.GAP_DETECTED

        self._candles.append(candle)
        self._last_open_time = candle.open_time
        return IngestOutcome.ACCEPTED

    def history(self) -> tuple[Candle, ...]:
        """Kabul edilmiş TÜM (bounded) candle'lar, ascending open_time sırayla."""
        return tuple(self._candles)

    def __len__(self) -> int:
        return len(self._candles)

    @property
    def latest_open_time(self) -> datetime | None:
        return self._last_open_time


