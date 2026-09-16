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
