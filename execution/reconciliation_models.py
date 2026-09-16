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
