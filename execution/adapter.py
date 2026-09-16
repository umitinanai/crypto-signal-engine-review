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
