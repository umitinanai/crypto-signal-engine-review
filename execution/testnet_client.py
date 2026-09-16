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
