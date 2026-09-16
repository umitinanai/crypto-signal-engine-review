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
