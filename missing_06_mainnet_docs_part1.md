<!-- missing_06_mainnet_docs_part1.md — Part 1/2 — 16 files -->
<!-- Contents of this part: -->
<!--   - ARCHITECTURE.md (30490 bytes) -->
<!--   - DECISIONS.md (182556 bytes) -->
<!--   - MAINNET_READINESS_CHECKLIST.md (7511 bytes) -->
<!--   - PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md (19594 bytes) -->
<!--   - PHASE8_UBUNTU_OPERATIONS.md (16515 bytes) -->
<!--   - PHASE9_LONG_RUN_STABILITY.md (12873 bytes) -->
<!--   - PROJECT_HANDOFF.md (73431 bytes) -->
<!--   - PROJECT_SUMMARY.md (15862 bytes) -->
<!--   - SAFETY_INVARIANTS.md (12301 bytes) -->
<!--   - adaptive/shadow.py (12224 bytes) -->
<!--   - adaptive/store.py (13638 bytes) -->
<!--   - deploy/systemd/crypto-signal-engine.service (2353 bytes) -->
<!--   - deploy/systemd/crypto-signal-engine-backup.service (1542 bytes) -->
<!--   - deploy/systemd/crypto-signal-engine-backup.timer (1177 bytes) -->
<!--   - deploy/env.example (3462 bytes) -->
<!--   - pyproject.toml (1119 bytes) -->

=== FILE: ARCHITECTURE.md ===
# ARCHITECTURE.md — Faz 1 (domain contract layer)

## Paket yapısı

```
crypto_signal_engine/            <- pip-installable gerçek package
    __init__.py                  <- ALLOW_LIVE_TRADING = False (hard invariant)
    domain/
        _validation.py           <- require_finite, require_utc_aware, normalize_symbol, freeze_mapping
        enums.py                 <- Timeframe, SignalDirection, StructureRegime, VolatilityRegime, LiquidityRegime, ...
        models.py                <- Candle, CandleIdentity, Trade, OrderBookSnapshot, FeatureVector, AgentEvidence, Signal
        events.py                <- MarketDataEvent (payload type-safe), SequenceCursor (trade/orderbook için)
        candle_sequencing.py     <- CandleSequencer (candle identity vs update ordering ayrımı)
        state_contract.py        <- CandleStateStore (ABC) + InMemoryCandleStateStore (referans impl)
        consensus.py             <- ConsensusResult, RiskAssessment, RegimeContext, compute_agreement
    providers/
        base.py                  <- LiveDataProvider (ABC), ProviderHealthSnapshot
    quality/
        base.py                  <- DataQualityGate (ABC), DataQualityResult
    safety/
        models.py                <- SafetyEvent, SafetyState (state machine)
tests/                            <- repo kökünde, package'ın DIŞINDA
pyproject.toml                    <- gerçek package tanımı (pip install -e .)
```

## Veri akışı (candle örneği üzerinden)

```
Provider (Faz 2)
   |
   v
CandleUpdate { candle, update_seq, event_time, received_at }
   |
   v
DataQualityGate.check_candle(candle, previous)   <- ÖNCE kalite kontrolü
   |
   v
CandleStateStore.commit(quality_result, update)
   |
   +-- quality_result.passed == False -> REJECTED_QUALITY
   |      (CandleSequencer'a hiç ulaşmaz, canonical state DEĞİŞMEZ)
   |
   +-- quality_result.passed == True
          -> CandleSequencer.apply(update)
             +-- update_seq daha önce görüldü -> REJECTED_DUPLICATE
             +-- update_seq < son görülen -> REJECTED_OUT_OF_ORDER
             +-- identity zaten finalize -> REJECTED_AFTER_FINAL
             +-- aksi halde -> ACCEPTED_UPDATE veya ACCEPTED_FINAL (is_closed=True ise)
   |
   v
FeatureEngine (Faz 3)  <- yalnızca COMMITTED candle'ları görür
```

Trade ve order-book akışları için (doğal tekil sequence ID'leri olduğundan)
daha basit bir zarf kullanılır: `MarketDataEvent` + `SequenceCursor`.

## Candle identity vs. update sıralaması (kritik ayrım)

- **Identity** = `(symbol, timeframe, open_time)` — DEĞİŞMEZ, bir candle'ın
  "hangi candle olduğu" sorusuna cevap verir.
- **Update ordering** = `update_seq` — aynı identity için gelen farklı
  update mesajlarının sırasını belirler. `close_time` bu amaçla KULLANILMAZ.
- Bir identity "finalize" (`is_closed=True`) olduktan sonra o identity için
  gelen HER türlü yeni update reddedilir (`REJECTED_AFTER_FINAL`) — reconnect
  sonrası eski verinin canonical state'i geriye götürmesi bu şekilde engellenir.

## Consensus akışı ve provenance

```
AgentEvidence[] (aynı symbol + aynı context_id ZORUNLU, agent duplication YASAK)
   |
   v
ConsensusEngine.combine()  (Faz 2+, henüz implemente edilmedi)
   |
   v
ConsensusResult { symbol, context_id, raw_score, agreement, regime: RegimeContext, contributing_evidence }
   |
   v
RiskOverlay.apply()  (Faz 2+, henüz implemente edilmedi)
   |
   v
RiskAssessment { confidence_multiplier, risk_level, rationale, contradicting_metrics }
   |
   v
Signal { score (canonical), confidence, ... }
   direction = SignalDirection.from_score(score)   <- property, ayrı state DEĞİL
```

`RegimeContext` üç bağımsız ekseni birleştirir: `StructureRegime` +
`VolatilityRegime` + `LiquidityRegime` — bunlar mutually-exclusive değildir
(piyasa aynı anda TRENDING + HIGH_VOLATILITY + THIN olabilir).

## Timestamp / symbol / numeric politikaları (tek noktadan uygulama)

Tüm bu politikalar `domain/_validation.py` üzerinden merkezi olarak
uygulanır; her dataclass kendi kontrolünü ayrı ayrı yeniden yazmaz:

- **UTC-aware only**: `require_utc_aware()` — naive datetime her yerde reddedilir.
- **Finite numbers only**: `require_finite()` — NaN/±inf domain'e giremez.
- **Symbol normalization**: `normalize_symbol()` — strip + upper-case.
- **Gerçek immutability**: `freeze_mapping()` — defensive copy + MappingProxyType.
- **Runtime enum type safety**: `require_enum()` — string'lerin sessizce enum yerine kabul edilmesini engeller.

## Final Signal provenance (Quality Gate 26)

`Signal` kendi `context_id`'sini taşır. `supporting_factors` ve
`contradicting_factors` içindeki HER `AgentEvidence` için şunlar
`__post_init__` içinde zorunlu kılınır:

- `evidence.symbol == signal.symbol` (mixed-symbol reddi)
- `evidence.context_id == signal.context_id` (mixed-context reddi)
- `evidence.as_of <= signal.timestamp` (gelecekten evidence reddi)
- aynı evidence nesnesi iki listede birden bulunamaz
- aynı `AgentName`, supporting+contradicting birleşiminde yalnızca bir kez
  geçebilir (global one-agent/one-evidence politikası)

## SafetyState encapsulation (Quality Gate 24)

`SafetyState`, `@dataclass` DEĞİLDİR — private alanlar + read-only
property'ler kullanan sıradan bir sınıftır. Constructor argüman kabul
etmez (her zaman RUNNING başlar). Mutation yalnızca `halt()`, `resume()`,
`add_warning()`, `clear_warnings()`, `mark_evaluated()` üzerinden mümkündür;
doğrudan attribute ataması `AttributeError` fırlatır.

## Faz 1 kapsamı DIŞINDA olanlar (bilinçli olarak yazılmadı)

- ~~Binance WebSocket/REST implementasyonları~~ → **Faz 2'de implemente edildi** (bkz. aşağı)
- CCXT Testnet entegrasyonu (Faz 7)
- FeatureEngine, quant strateji kodu (Faz 3+)
- Dashboard, Telegram (Faz 8)
- `BinanceLiveExecutionAdapter` — YAZILMAYACAK, hiçbir zaman

---

# FAZ 2 — BINANCE MARKET DATA MVP

Faz 2, Faz 1'in domain contract layer'ı ÜZERİNE inşa edilmiştir; hiçbir
Phase 1 domain modeli/enum/invariant yeniden yazılmadı. Detaylı mimari,
veri akışları, order book senkronizasyon algoritması ve tasarım kararları
için bkz. **PHASE2_MARKET_DATA.md**.

Özet:
- `providers/binance/` — REST/WebSocket client'ları, parser, order book
  sync, reconnect policy, config, symbol adapter, injectable clock/transport
- `quality/binance_rules.py` — Phase 1 `DataQualityGate` ABC'sinin concrete
  implementasyonu
- `state/manager.py` — Phase 1 `CandleStateStore` contract'ını trade/
  order-book canonical state ile genişleten orkestratör
- `persistence/sqlite_store.py` — Phase 1 `CandleStateStore`'un SQLite-backed
  implementasyonu

Faz 2 kapsamı DIŞINDA (Faz 3+ için): RSI/MACD/EMA, QuantAgent/
MarketStructureAgent/OrderBookAgent/RegimeAgent, consensus engine
implementasyonu, trading signal, Telegram, dashboard, paper trading,
execution.

## Faz 2 v2 — bağımsız reviewer hardening (ikinci tur)

Bağımsız reviewer'ın ikinci tur bulguları üzerine dört açık kapatıldı:
order book overlap semantiği, SQLite commit atomicity, stale-feed
detection (gerçek implementasyon), deterministic candle gap recovery.
Ayrıca iki küçük boundary-hardening maddesi (`queue_maxsize` ve
`supported_symbols` validasyonu) eklendi. Detaylar: PHASE2_MARKET_DATA.md,
DECISIONS.md (Karar 31-36).

---

# FAZ 3 — FEATURE ENGINE

Faz 3, Phase 1 domain contract'ları VE Phase 2 canonical market data'sı
ÜZERİNE inşa edilen, salt analitik bir katmandır. Hiçbir trading signal,
agent consensus, RiskOverlay, veya execution mantığı İÇERMEZ. Detaylı
mimari, formüller, warm-up semantikleri, no-look-ahead garantisi için bkz.
**PHASE3_FEATURE_ENGINE.md**.

Özet:
- `features/domain.py` — `FeatureIdentity`, `FeatureValue`, `FeatureSnapshot`
  (Phase 1 validation yardımcılarını yeniden kullanır)
- `features/registry.py` — `FeatureConfig`, `FeatureRegistry` (instance-scoped,
  çakışma-güvenli)
- `features/candle_calculators.py`, `orderbook_calculators.py`,
  `trade_calculators.py` — saf, deterministic hesaplama fonksiyonları
- `features/engine.py` — `FeatureEngine` (public API, closed-candle
  politikası + no-look-ahead filtrelemesi burada uygulanır)
- `features/state.py` — `FeatureHistoryStore` (Phase 2 market-data
  state'inden AYRI, bounded, symbol/timeframe izole)

Faz 3 kapsamı DIŞINDA (Faz 4+ için): QuantAgent, MarketStructureAgent,
OrderBookAgent, RegimeAgent, consensus engine, Signal, RiskOverlay, position
sizing, backtesting, paper trading, execution, Telegram, dashboard.

---

# FAZ 4 — QUANT & AGENT ENGINE

Faz 4, Faz 1'in `AgentEvidence`/`ConsensusResult`/`RiskAssessment`/`Signal`
domain modellerini (DEĞİŞTİRMEDEN) ve Faz 3'ün `FeatureSnapshot`/
`FeatureHistoryStore`'unu tüketen, salt analitik bir agent/consensus/risk
katmanıdır. Detaylı mimari, formüller, provenance garantileri için bkz.
**PHASE4_QUANT_AGENT_ENGINE.md**.

Özet:
- `agents/context.py` — `AgentContext` (4 timeframe hizalı, immutable,
  no-look-ahead), `build_agent_context()`, deterministic SHA-256 context-ID
- `agents/{quant,market_structure,order_book,regime}.py` — dört bağımsız agent
- `consensus/engine.py` — `ConsensusEngine` (sabit ağırlıklar, mevcut
  `compute_agreement()`)
- `consensus/risk.py` — `RiskOverlay` (strictly post-consensus, tek yönlü)
- `signal_engine.py` — `SignalEngine` orkestratörü

**Kabul edilen sözleşme sapması** (dürüstçe raporlanmış): `ConsensusEngine.
combine()`, Faz 1'in ZATEN KABUL EDİLMİŞ `ConsensusResult` modelinin zorunlu
`regime` alanını doldurmak için `regime`'i keyword-only bir parametre olarak
alır — `ConsensusResult` DEĞİŞTİRİLMEDİ; detaylar PHASE4_QUANT_AGENT_ENGINE.md'de.

Faz 4 kapsamı DIŞINDA (Faz 5+ için): execution, paper trading, Testnet,
backtesting, portfolio/position management, ML training, Telegram, dashboard.

---

# FAZ 5 — PAPER TRADING / SIMULATION ENGINE

Faz 5, Faz 4'ün tamamlanmış `Signal` nesnesini (DEĞİŞTİRMEDEN) tüketen,
tamamen dahili/deterministik bir paper trading muhasebe katmanıdır.
`ConsensusResult`/`RegimeContext`/`agents/*`/`consensus/*`'e HİÇBİR
bağımlılığı yoktur — Faz 4'ün `regime` keyword-only sözleşme sapması
(Karar 46/47) bu fazda ne düzeltilir ne derinleştirilir, DEĞİŞTİRİLMEDEN
kalır. Hiçbir Binance Testnet/Mainnet execution, private/signed endpoint,
API key/secret İÇERMEZ. Detaylı mimari, strateji, determinism/idempotency/
atomicity/no-look-ahead garantileri için bkz. **PHASE5_PAPER_TRADING.md**.

Özet:
- `paper_trading/models.py` — `MarketPriceSnapshot`, `PaperOrder`,
  `PaperFill` (fee dahil), `PaperPosition`, `PaperTradingResult`,
  `OrderSide`, `PositionSide` (Phase 1 validation yardımcılarını yeniden
  kullanır)
- `paper_trading/engine.py` — `PaperTradingEngine` (in-memory, sabit
  notional-based sizing, deterministic fee/slippage, per-symbol izole
  state)

**NEUTRAL = NO_ACTION** (acceptance review blocker düzeltmesi, Karar 53):
NEUTRAL sinyal hiçbir order/fill üretmez, açık bir pozisyonu kapatmaz,
PnL'i değiştirmez.

**Deterministic fee/slippage** (acceptance review blocker düzeltmesi, Karar
54) — İMPLEMENTE EDİLMİŞTİR: `PaperTradingEngine(fee_bps=0.0,
slippage_bps=0.0)`, YÖNE göre deterministik slippage-uygulanmış fill
fiyatı (`BUY: P*(1+slippage_bps/10000)`, `SELL: P*(1-slippage_bps/10000)`),
her `PaperFill` için `fee = abs(qty*fill_price)*fee_bps/10000`, net
realized PnL'de entry+exit fee netleştirmesi (double-count yok). Sıfır
varsayılanlar önceki sıfır-maliyetli davranışı korur.

Faz 5 kapsamı DIŞINDA (Faz 6+ için): Binance Testnet Lab, gerçek execution,
partial fill/limit order, confidence-bazlı position sizing, backtesting,
portfolio-level risk limitleri, Telegram, dashboard.

---

# FAZ 6 — REAL-TIME MARKET DATA & RUNTIME

Faz 6, ZATEN kabul edilmiş Faz 2 (Binance PUBLIC market data), Faz 3
(`FeatureEngine`), Faz 4 (`SignalEngine`) ve Faz 5 (`PaperTradingEngine`)
bileşenlerini KOORDİNE eden, sürekli/gerçek-zamanlı çalışabilecek bir
runtime katmanıdır — hiçbirinin mantığını yeniden uygulamaz. **Execution
fazı DEĞİLDİR**: hiçbir gerçek borsa emri gönderilmez, hiçbir private/
signed Binance endpoint'i/API key/secret İÇERMEZ. `ALLOW_LIVE_TRADING =
False` korunur. Detaylı mimari, bootstrap, gap-recovery, stale-detection,
no-look-ahead garantileri için bkz. **PHASE6_REALTIME_RUNTIME.md**.

Özet:
- `runtime/models.py` — `RuntimeHealth`, `IngestOutcome`, `RuntimeCycleResult`,
  `ProcessedMarketEvent`, `BootstrapReport`, `SymbolHealth`, `RuntimeStatus`
- `runtime/candle_window.py` — `CandleWindow` (per symbol+timeframe,
  identity-bazlı dedup/sıralama/gap tespiti — Faz 2'nin private
  `StateManager`'ına TAMAMLAYICI, ona ayna DEĞİL, bkz. Karar 55)
- `runtime/health.py` — `HealthMonitor` (staleness-bazlı, Faz 2'nin
  `Clock` protokolünü yeniden kullanır, yeni bir zaman soyutlaması yok)
- `runtime/bootstrap.py` — `apply_bootstrap_candles()` (deterministic,
  senkron, ağ gerektirmez)
- `runtime/coordinator.py` — `RuntimeCoordinator` (instance-scoped,
  `SignalEngine`'in TEK çağrıldığı yer — yalnızca M5 kapanışında, Karar 56)
- `providers/binance/real_websocket.py` — opsiyonel, lazy-import
  `websockets` tabanlı gerçek `WebSocketConnectionFactory` (yalnızca
  gerçek/canlı çalıştırma için; offline testleri etkilemez)
- `scripts/live_public_smoke_test.py` — opsiyonel, manuel, offline pytest
  suite'inin DIŞINDA, bounded-timeout canlı public-data smoke test

**Gap tespiti** (Karar 57): `CandleWindow`, bir sonraki beklenen
`open_time`'dan daha ileri bir candle gelirse `GAP_DETECTED` döner (ASLA
sessizce kabul etmez); `RuntimeCoordinator.resolve_gap()` Faz 2'nin PUBLIC
REST mekanizmasıyla yalnızca eksik aralığı doldurur.

Faz 6 kapsamı DIŞINDA (Faz 7+ için): persistence/restart recovery,
database checkpoint, systemd/Docker/deployment, çok-günlük soak test,
Binance Testnet trading, private Binance API, Mainnet order execution,
leverage/futures, portfolio optimizer, backtesting, Telegram/dashboard.

---

# FAZ 7 — PERSISTENCE & RECOVERY

Faz 7, kabul edilmiş Faz 5 (paper trading) + Faz 6 (real-time runtime)
sistemine durable local persistence ve deterministic process-restart
recovery ekler. **Execution fazı DEĞİLDİR** — Testnet/private API/Mainnet
execution YOK. `PaperTradingEngine`/`RuntimeCoordinator` DEĞİŞTİRİLMEDİ
(yalnızca 2 küçük, additive PUBLIC metod eklendi — aşağı bkz.); persistence
onları SARAR (wrap). Detaylı mimari, schema, recovery sırası, atomicity/
failure semantiği için bkz. **PHASE7_PERSISTENCE_RECOVERY.md**.

Özet:
- `persistence/errors.py` — `SchemaVersionMismatchError`,
  `CorruptRecordError` (mevcut `PersistenceError`'ı genişletir, Faz 2 ile
  aynı taban)
- `persistence/serialization.py` — `Signal`/`AgentEvidence`/`PaperPosition`/
  `PaperFill`/`PaperOrder` <-> JSON-safe dict (domain modelleri
  DEĞİŞTİRİLMEDEN, tamamen dışarıdan saf fonksiyonlarla)
- `persistence/paper_state_store.py` — `PaperStateStore` (SQLite, explicit
  `SCHEMA_VERSION`, tek transaction'da atomik checkpoint)
- `persistence/recovery.py` — `restore_paper_engine()` (Faz 5'in private
  `_states`'ine YAZAN, dokümante edilmiş TEK istisna) + `PersistedRuntime`
  (Faz 6 `RuntimeCoordinator`'ı saran, checkpoint yazan wrapper)
- `runtime/health.py`/`coordinator.py`'ye additive ek: `mark_persistence_fault`/
  `clear_persistence_fault` (REASON-bazlı — bkz. Karar 61)

Faz 7 kapsamı DIŞINDA (Faz 8+ için): systemd/Docker/deployment, çok-günlük
soak test, Binance Testnet execution, execution safety/reconciliation,
final production readiness.

---

# FAZ 8 — OPERATIONS & UBUNTU DEPLOYMENT

Faz 8, kabul edilmiş Faz 1-7 sistemini (`phase7-accepted`) DEĞİŞTİRMEDEN,
onu bir Ubuntu makinesinde uzun süre çalışan, gözlemlenebilir, güvenli
bir systemd servisi olarak işletmek için gereken operasyonel "kabuk"u
ekler. **Execution fazı DEĞİLDİR** — Testnet/private API/Mainnet
execution YOK. Detaylı kurulum/runbook için bkz.
**PHASE8_UBUNTU_OPERATIONS.md**.

Özet:
- `crypto_signal_engine/ops/config.py` — `AppConfig`/`load_config()`:
  `CSE_*` ortam değişkenlerinden tek bir immutable, fail-fast config
  (secret alanı YOK; REST/WS base URL override EDİLEMEZ — Karar 65)
- `crypto_signal_engine/ops/lock.py` — `ProcessLock`: `flock`-tabanlı,
  aynı durable store'a karşı ikinci bir instance'ı engeller (Karar 63)
- `crypto_signal_engine/ops/health_snapshot.py` — periyodik, atomik yerel
  JSON sağlık anlık görüntüsü (Karar 64)
- `crypto_signal_engine/ops/logging_setup.py` — stdlib `logging` kurulumu
  (stdout/journald)
- `crypto_signal_engine/app.py` — `Application` kompozisyon kökü +
  `python -m crypto_signal_engine.app {run|status}` CLI'ı; Faz 5/6/7
  nesnelerini bağlar, SIGINT/SIGTERM'i graceful/idempotent shutdown'a
  çevirir, anlamlı exit code'lar üretir (0/1/2/3/4)
- `deploy/systemd/crypto-signal-engine.service` — non-root, bounded
  restart, hardening direktifleri (`NoNewPrivileges`, `ProtectSystem=strict`)
- `deploy/env.example` — örnek `EnvironmentFile=` (secret İÇERMEZ)
- `scripts/backup_sqlite.py` — SQLite Online Backup API tabanlı güvenli
  yedekleme (ham `cp` YASAK — WAL tutarlılık riski)
- `pyproject.toml` — `[project.optional-dependencies].runtime =
  ["websockets>=12.0"]` (Karar 66) + `crypto-signal-engine` console script

Faz 8 kapsamı DIŞINDA (Faz 9+ için): çok-günlük soak test, Binance
Testnet execution lab, execution safety/reconciliation, final production
readiness.

---

# FAZ 9 — LONG-RUN STABILITY / SOAK TESTING

Faz 9, kabul edilmiş Faz 1-8 sistemini DEĞİŞTİRMEDEN, `FixedClock`
(Faz 2, DEĞİŞTİRİLMEDEN) ile hızlandırılmış sanal zamanda süren,
tamamen offline/deterministik bir soak/stability harness ekler.
**Execution fazı DEĞİLDİR.** Detaylı mimari, 10 senaryo, bulgular
(reconnect storm'un gerçek kapsamı, restart re-warmup karakteristiği),
invariant'lar, kaynak-büyüme kategorileri için bkz.
**PHASE9_LONG_RUN_STABILITY.md**.

Özet:
- `crypto_signal_engine/stability/harness.py` — `SoakHarness`: Faz 5/6/7
  nesnelerini kurar/sürer, deterministik candle/order-book üretir
- `crypto_signal_engine/stability/faults.py` — `ScriptedMarketDataProvider`
  (gerçek async run()/stop()/disconnect senaryoları için),
  `FaultyPaperStateStore` (deterministik persistence-failure injection)
- `crypto_signal_engine/stability/metrics.py` — `SoakMetrics`
- `crypto_signal_engine/stability/scenarios.py` — 10 çekirdek senaryo
  sürücüsü (steady state, reconnect storm, gap recovery, duplicate/
  out-of-order, process restart, crash-like restart, persistence failure,
  stale feed, multi-symbol stress, shutdown under activity)
- `scripts/public_soak_test.py` — opsiyonel, manuel gerçek PUBLIC soak
- BLOCKER FİX (Karar 67): `PersistedRuntime`'ın stream-tüketim döngüleri
  Faz 6'nın `mark_disconnected` disiplinine SAHİP DEĞİLDİ — düzeltildi
- BLOCKER FİX (Karar 68, bağımsız acceptance review bulgusu): restart
  recovery, checkpoint VARKEN yalnızca dar `[checkpoint, now)` penceresini
  çekiyordu — yeterli PUBLIC geçmiş MEVCUT olsa bile warmup'ı yeniden
  inşa edemiyordu; artık checkpoint ETRAFINDA tam lookback penceresi
  (`[checkpoint-lookback, now)`) çekilir

Faz 9 kapsamı DIŞINDA (Faz 10+ için): Binance Testnet execution lab,
execution safety/reconciliation, final production readiness.

---

# FAZ 10 — BINANCE SPOT TESTNET EXECUTION LAB

Faz 10, sistemin İLK private/signed Binance execution sınırını ekler —
KESİNLİKLE ve YALNIZCA `https://testnet.binance.vision`'a karşı. Mainnet
execution bu sınır üzerinden YAPISAL OLARAK İMKANSIZDIR (host allowlist,
`urlsplit().hostname` TAM eşleşmesi — substring DEĞİL). `ALLOW_LIVE_TRADING`
korunur (`False`). Detaylı mimari, güvenlik sınırı, CLI davranışı için
bkz. **PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md**.

Özet:
- `crypto_signal_engine/execution/models.py` — `ExecutionMode` (yalnızca
  `PAPER`/`BINANCE_SPOT_TESTNET`, `MAINNET` enum üyesi DEĞİL),
  `OrderIntent` (deterministik `client_order_id` — SHA-256 türetilmiş),
  `ExecutionResult`
- `crypto_signal_engine/execution/signer.py` — HMAC-SHA256, kanonik
  parametre kodlama, injectable `Clock` (Faz 2, DEĞİŞTİRİLMEDEN)
- `crypto_signal_engine/execution/testnet_client.py` — `validate_testnet_host()`
  (BLOCKER-seviyesi host allowlist), `BinanceTestnetConfig` (credential
  redaction, credential'lar opsiyonel — yalnızca signed çağrılar için
  zorunlu), `BinanceTestnetClient`
- `crypto_signal_engine/execution/adapter.py` — `TestnetExecutionAdapter`
  (exchange-filter doğrulama + gönderim, tek yüksek-seviye giriş noktası)
- `scripts/binance_testnet_lab.py` — manuel CLI (`account-check`,
  `validate-symbol`, `place-market`, `place-limit`); order gönderimi
  `--confirm-testnet-order` OLMADAN yalnızca dry-run yapar
- `tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`
  — mimari-farkında tarama: HMAC/credential izleri YALNIZCA `execution/`
  içinde, `execution/` İÇİNDE BİLE futures/margin/Mainnet host YOK
- BLOCKER FİX (Karar 73, bağımsız acceptance review bulgusu): MARKET
  order'lar için MIN_NOTIONAL/NOTIONAL kontrolü, `OrderIntent.price`
  MARKET'te her zaman `None` olduğundan sessizce hiç çalışmıyordu; artık
  gerektiğinde `BinanceTestnetClient.symbol_price()` ile GÜNCEL bir
  TESTNET PUBLIC fiyatı çekilip notional tabanı olarak kullanılır
  (fail-closed — fiyat lookup'ı başarısız olursa order gönderilmez)

Hiçbir Faz 1-9 bileşeni (`SignalEngine`/`RuntimeCoordinator`/
`PaperTradingEngine`/Faz 8 `Application`) bu paketten HABERDAR DEĞİLDİR —
otomatik Signal->TESTNET-order yolu YOKTUR, yalnızca KASITLI/MANUEL CLI
çağrısı.

Faz 10 kapsamı DIŞINDA (Faz 11+ için): tam execution reconciliation,
unknown-order recovery, exactly-once garantisi, partial-fill lifecycle
tamlığı, cancel/replace, execution-vs-paper divergence yönetimi.

---

# FAZ 11 — EXECUTION SAFETY & RECONCILIATION

Faz 11, Faz 10'un TESTNET execution sınırına, stabil `client_order_id`/
`context_id` kimliği üzerinden reconciliation ekler. **Hâlâ TESTNET
ONLY'dir** — Mainnet execution YAPISAL OLARAK İMKANSIZ kalır (Faz 10'un
host allowlist'i DEĞİŞTİRİLMEDEN). Çekirdek ilke: exchange, exchange-
execution state için OTORİTERDİR; yerel sistem HTTP yanıtına/yerel intent
state'ine/önceki process belleğine KÖRÜ KÖRÜNE GÜVENMEZ. Detaylı lifecycle/
ambiguity/reconciliation/restart-recovery/persistence-failure tasarımı
için bkz. **PHASE11_EXECUTION_SAFETY_RECONCILIATION.md**.

Özet:
- `crypto_signal_engine/execution/reconciliation_models.py` —
  `ExecutionLifecycleState` (10 durum, `TERMINAL_STATES`/
  `NEEDS_RECONCILIATION_STATES`), `ExecutionRecord`, `apply_exchange_truth()`
  (contradiction-korumalı: FILLED->NEW, executedQty azalması,
  exchangeOrderId değişimi HEPSİ reddedilir)
- `crypto_signal_engine/execution/reconciliation_store.py` —
  `ExecutionStateStore` (SQLite, Faz 7'nin `PaperStateStore` disiplini,
  Faz 7'nin `SchemaVersionMismatchError`/`CorruptRecordError`'ını
  YENİDEN KULLANIR — tekrar tanımlamaz)
- `crypto_signal_engine/execution/reconciliation_service.py` —
  `ExecutionReconciliationService`: `submit()` (idempotent, ambiguity-safe),
  `reconcile()`, `reconcile_pending()` (restart-sonrası kurtarma)
- `BinanceTestnetClient.query_order()` — sinyalli `GET /api/v3/order`
  (`origClientOrderId` ile), AYNI hard Testnet host allowlist'i
- `scripts/binance_testnet_lab.py` — `order-status`/`reconcile`/
  `reconcile-pending` komutları eklendi; `place-market`/`place-limit
  --confirm-testnet-order` artık reconciliation service üzerinden geçer

Hiçbir Faz 1-10 bileşeni bu katmandan HABERDAR DEĞİLDİR — otomatik
Signal->TESTNET-order yolu YOKTUR. "No fake exactly-once": bu katman
stabil kimlik + duplicate suppression + reconciliation + güvenli
ambiguous-outcome handling SAĞLAR, matematiksel exactly-once İDDİA ETMEZ.

**BLOCKER FİX (Karar 78):** `UNKNOWN_NOT_FOUND` (exchange sorgusunun bir
ambiguous POST sonrası order'ı BULAMADIĞI durum, Binance kodu -2013),
`NEEDS_RECONCILIATION_STATES` İÇİNDEDİR — "yeniden gönderim GÜVENLİDİR"
ANLAMINA GELMEZ. Tek bir -2013 yanıtı, orijinal POST'un Binance
tarafından hiç kabul edilmediğini KANITLAMAZ; `submit()`/`reconcile()`/
`reconcile_pending()` bu durumdaki bir kayıt için SADECE yeniden sorgular,
ASLA otomatik bir ikinci `POST` YAPMAZ. Bkz. DECISIONS.md Karar 74
(düzeltilmiş) ve Karar 78.

Faz 11 kapsamı DIŞINDA (Faz 12+ için): toplu `openOrders` sorgusu,
cancel/replace, otomatik retry-with-backoff döngüsü, tam execution-vs-
paper divergence yönetimi, final production readiness.

---

# FAZ 12 — FINAL LOCAL PRODUCTION READINESS

Faz 12, Faz 1-11'in kabul edilmiş sistemini birkaç haftalık kesintisiz
YEREL (Ubuntu/WSL) çalıştırma için hazırlar — Mainnet üretim DEĞİLDİR.
Detaylı tasarım için bkz. **PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md**.

Özet:
- `crypto_signal_engine/ops/config.py` — YENİ `AppConfig` alanları:
  `execution_mode` (varsayılan `PAPER`), `enable_testnet_execution`
  (varsayılan `False`), `dashboard_enabled`/`dashboard_host`
  (varsayılan `127.0.0.1`)/`dashboard_port`. Credential alanı YOKTUR
  (hard invariant DEĞİŞMEDEN).
- `crypto_signal_engine/execution/factory.py` — YENİ, execution/ SINIRI
  İÇİNDE: `build_testnet_execution_service()`, TESTNET kimlik bilgisi
  okuma/imzalama/servis inşasının TEK yeri (`app.py`'nin KENDİSİ
  execution/ SINIRI DIŞINDA olduğundan hiçbir credential-literal/
  private-endpoint izi İÇEREMEZ — bkz. `tests/
  test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`).
- `crypto_signal_engine/app.py::Application` — Faz 8 kompozisyon kökü
  GENİŞLETİLDİ (YENİDEN TASARLANMADI): TESTNET etkinse `start()`,
  run/health loop'lardan ÖNCE `ExecutionReconciliationService.
  reconcile_pending()`'i çalıştırır (fail-closed: başarısız olursa
  `execution_ready=False` kalır, PUBLIC/PAPER runtime ETKİLENMEZ); YENİ
  `doctor` alt komutu (tamamen offline preflight); YENİ salt-okunur
  dashboard başlatma/durdurma (bind hatası runtime'ı DURDURMAZ).
- `crypto_signal_engine/ops/dashboard.py` — YENİ, stdlib-only
  (`http.server`), salt-okunur (`GET` dışı HER ŞEY `405`), TEK veri
  kaynağı disk üzerindeki atomik health-snapshot JSON'u (canlı Python
  nesnelerine ASLA doğrudan erişmez — cross-thread race YAPISAL OLARAK
  YOKTUR). Varsayılan bind `127.0.0.1`.
- `crypto_signal_engine/ops/health_snapshot.py` — `write_snapshot()`'a
  YENİ opsiyonel `execution`/`paper` alanları eklendi (geriye-uyumlu).
- `crypto_signal_engine/runtime/coordinator.py` — İKİ KÜÇÜK salt-okunur
  accessor eklendi: `paper_engine` property, `last_cycle_result(symbol)`
  (sembol başına TEK kayıt, sınırlı bellek) — dashboard için.
- `scripts/local_readiness_check.py` — YENİ, tek-komutluk offline
  hazırlık script'i (doctor + compileall + disk alanı + websockets
  bilgilendirmesi).

**Blocker-seviyesi, kasıtlı erteleme (bkz. PHASE12 doc Bölüm 2):**
otomatik sürekli Signal->TESTNET execution entegrasyonu bu Faz'da
İMPLEMENTE EDİLMEDİ — Faz 6'nın senkron sinyal kod yolunu Faz 11'in
asenkron execution servisine "no redesign" kısıtı ALTINDA güvenle
köprülemek başlı başına bir mimari tasarım kararı gerektirir. TESTNET
gözlemi HÂLÂ yalnızca `scripts/binance_testnet_lab.py`'nin manuel CLI'sı
üzerinden yapılır (Faz 10/11 invariant'ı DEĞİŞMEDEN).

Faz 12 kapsamı DIŞINDA (kasıtlı, "do not overbuild"): VPS deployment/
otomasyonu, Docker/Kubernetes, Prometheus/Grafana, Redis/message queue,
mikroservis mimarisi, React/karmaşık frontend framework, mobil uygulama,
Mainnet desteği.

**GÜNCELLEME (bu satır aksi hâlde bayatlardı):** "PLANLANMIŞ bir Faz 13
YOKTUR" hâlâ DOĞRUDUR — hiçbir zaman resmi bir "Faz 13" ilan edilmedi ve
yukarıdaki "do not overbuild" listesi (VPS/Docker/Kubernetes/Prometheus/
mikroservis/mobil/Mainnet) hâlâ AYNEN geçerlidir. Ama Faz 12'nin kabul
edilmesinden bu yana BEŞ ayrı, additive, isimlendirilmiş milestone daha
kabul edildi — her biri kendi DECISIONS.md Karar'ıyla belgelendi: PRE-AUDIT
ENHANCEMENT PASS (aşağıda), Adaptive Intelligence v1 (Karar 93-96),
Portfolio/Accounting v1 (Karar 97-98), Control Center/Backup/24/7 Ops v1
(Karar 99), ve UI Polish/Final Acceptance v1 (Karar 100 — event log,
dashboard'un GERÇEK veri kaynaklarına tam bağlanması, `MAINNET_READINESS_
CHECKLIST.md`). Mainnet desteği HÂLÂ İMPLEMENTE EDİLMEDİ ve HÂLÂ
YAPISAL OLARAK İMKANSIZ bırakıldı (bkz. SAFETY_INVARIANTS.md #15/#16) —
ama artık bu konudaki GERÇEK hazırlık durumu, uydurma bir "yakındayız/
uzağız" izlenimi yerine, `MAINNET_READINESS_CHECKLIST.md`'de dürüstçe
madde madde takip edilmektedir (bir taahhüt/tarih DEĞİLDİR).

---

# PRE-AUDIT ENHANCEMENT PASS (Faz 12 ÜZERİNE — Faz 13 DEĞİL)

Faz 12'nin kabul edilmiş temeli (`phase12-accepted`, commit `33dc6cd`)
ÜZERİNE, final bağımsız adversarial audit ÖNCESİNDE, additive bir
araştırma/doğrulama katmanı eklendi: yeni, bağımsız bir üst-seviye
`research/` paketi (deterministic historical replay, data-quality
validation, rolling OOS stability, performance attribution, Monte Carlo
robustness screen, portfolio/risk-bazlı PAPER sizing + guardrails,
order-book research capture, backtest-vs-paper drift scaffolding) ve üç
yeni CLI script'i (`scripts/historical_replay.py`, `oos_stability.py`,
`monte_carlo_robustness.py`). Kabul edilmiş `crypto_signal_engine/`
mimarisi DEĞİŞTİRİLMEDİ — yalnızca ÜÇ küçük, additive, varsayılan-
davranış-koruyan touch yapıldı: `PaperTradingEngine.process_signal`'a
opsiyonel `notional_override`, `PaperTradingEngine.has_processed()`
(yeni salt-okunur accessor), `RuntimeCoordinator`'a opsiyonel
`order_book_observer` hook'u. Detaylı mimari, sınırlamalar, ve tam
gerekçeler için bkz. **PRE_AUDIT_ENHANCEMENTS.md** ve DECISIONS.md
Karar 82-89.


=== FILE: DECISIONS.md ===
# DECISIONS.md

## Karar 1 — RiskAgent yerine RiskOverlay

**Date:** 2026-08-31
**Decision:** Risk değerlendirmesi, diğer agent'ların çıktısını okuyan bir "agent"
olarak değil, `ConsensusEngine.combine()` sonrası çalışan ayrı bir `RiskOverlay`
adımı olarak modellenecek (`domain/consensus.py::RiskAssessment`).
**Reason:** "Agent'lar birbirinin çıktısını kör biçimde tekrar etmez / okumaz"
kuralının istisnasız uygulanması.
**Alternatives:** RiskAgent'ı diğer agent çıktılarını okuyabilen özel bir agent
olarak bırakmak.
**Consequences:** ConsensusEngine iki aşamalı: (1) bağımsız agent kanıtlarının
birleşimi → `ConsensusResult`, (2) RiskOverlay'in bu sonuç üzerine uyguladığı
`RiskAssessment` → nihai `Signal`.

## Karar 2 — Agreement formülü: std_dev yerine ağırlıklı işaret uyumu

**Date:** 2026-08-31
**Decision:** `agreement = 1 - std_dev(scores)` yerine `compute_agreement()`:
ağırlıklı işaret uyumu × normalize edilmiş dispersiyon.
**Reason:** Ham `std_dev`, [-1,1] aralığında 1'i aşabilir; `1 - std_dev` negatife
düşüp sezgisel olmayan davranışa yol açabilir.
**Consequences:** `compute_agreement()` her zaman [0.0, 1.0] aralığında,
deterministik sonuç döndürür.

## Karar 3 — "execution timing" yerine "microstructure/timing confirmation"

**Date:** 2026-08-31
**Decision:** 1m timeframe rolü `TimeframeRole.MICROSTRUCTURE_CONFIRMATION`.
**Reason:** Ana analiz sisteminde "execution" kavramı yok; bu kavram yalnızca
Paper Trading / Testnet Lab'da var.

---

# FAZ 1 HARDENING KARARLARI (Quality Gate revizyonu, 2026-08-31)

## Karar 4 — Gerçek Python package: `crypto_signal_engine`

**Date:** 2026-08-31
**Decision:** Proje, `pyproject.toml` ile pip-installable gerçek bir package
olarak yeniden yapılandırıldı. Tüm production importları
`from crypto_signal_engine.domain.X import Y` biçimindedir. `pytest.ini`
içinde `pythonpath = .` gibi bir hack KULLANILMAMAKTADIR.
**Reason:** Önceki yapıda `from domain...` gibi importlar yalnızca pytest'in
sys.path manipülasyonu sayesinde çalışıyordu; gerçek bir Python process'te
(`python -c "import crypto_signal_engine"`) parent directory'den import
BAŞARISIZ oluyordu.
**Alternatives:** `sys.path.insert(...)` ile manuel path ekleme (reddedildi —
gizleme, gerçek çözüm değil).
**Consequences:** `pip install -e .` zorunlu kuruluş adımı oldu.
`tests/test_package_import.py` bunu subprocess ile bağımsız olarak doğrular.

## Karar 5 — Candle identity ile update sıralaması kesin ayrımı

**Date:** 2026-08-31
**Decision:** `CandleIdentity = (symbol, timeframe, open_time)` candle'ın
DEĞİŞMEZ kimliğidir. `close_time` hiçbir şekilde sequence/identity olarak
kullanılmaz. Yeni `domain/candle_sequencing.py::CandleSequencer`, her identity
için ayrı bir `update_seq` monotonik sayacı takip eder; aynı identity'e ait
birden fazla update (final'a kadar) kabul edilir, duplicate/out-of-order/
after-final durumları ayrı ayrı reddedilir.
**Reason:** Aynı açık candle WebSocket üzerinden defalarca güncellenir; bunların
close_time'a göre "aynı event" sayılması, ara update'lerin yanlışlıkla
duplicate reddedilmesine yol açardı.
**Consequences:** `MarketDataEvent`/`SequenceCursor` artık YALNIZCA trade/order-
book gibi doğal tekil sequence'i olan akışlar için kullanılır; candle update
sıralaması ayrı ve özel bir mekanizmadır.

## Karar 6 — DataQualityGate → StateManager commit sınırı

**Date:** 2026-08-31
**Decision:** Akış kesinleştirildi: `Provider → CandleUpdate →
DataQualityGate.check_candle() → CandleStateStore.commit(quality_result,
update) → FeatureEngine`. `commit()`, `quality_result.passed is False` ise
update'i `CandleSequencer`'a HİÇ ULAŞTIRMADAN reddeder.
**Reason:** Eski dokümantasyondaki "StateManager duplicate/out-of-order
kontrolü yapar" anlatımı ile "DataQualityGate önce çalışır" anlatımı
çelişiyordu.
**Consequences:** `domain/state_contract.py::CandleStateStore` (ABC) +
`InMemoryCandleStateStore` (referans implementasyon, Faz 1 kapsamında) eklendi.
Gerçek DBWriter/SQLite implementasyonu Faz 2 kapsamındadır.

## Karar 7 — AgentEvidence provenance: context_id (BREAKING)

**Date:** 2026-08-31
**Decision:** `AgentEvidence` artık zorunlu `symbol`, `as_of`, `context_id`
alanları taşır. `ConsensusResult` oluşturulurken TÜM evidence'ların aynı
`symbol` VE aynı `context_id`'ye sahip olması zorunludur; aksi halde
`ValueError`.
**Reason:** Provenance izlenebilirliği ve mixed-symbol/mixed-context consensus
riskinin ortadan kaldırılması.
**Timestamp politikası:** `context_id` DETERMİNİSTİK gruplama anahtarıdır
(aynı feature snapshot'tan üretilen tüm evidence'lar aynı context_id'yi
paylaşır). `as_of` yalnızca gözlemlenebilirlik/debug amaçlıdır ve consensus
gruplamasında KULLANILMAZ — bu, kırılgan bir zaman-toleransı tasarımından
(örn. "as_of'lar birbirine 500ms'den yakın olmalı") kaçınmak için bilinçli bir
tercihtir.
**Consequences:** Eski `AgentEvidence(agent, score, rationale,
primary_timeframe)` imzası BREAKING olarak değişti.

## Karar 8 — Regime decomposition: MarketRegime → 3 bağımsız eksen (BREAKING)

**Date:** 2026-08-31
**Decision:** Tek `MarketRegime` enum'u kaldırıldı. Yerine:
`StructureRegime` (TRENDING/RANGING/BREAKOUT/MEAN_REVERSION/UNKNOWN),
`VolatilityRegime` (LOW/NORMAL/HIGH/EXTREME/UNKNOWN),
`LiquidityRegime` (NORMAL/THIN/STRESSED/UNKNOWN) ve bunları birleştiren
`RegimeContext` dataclass'ı eklendi.
**Reason:** Piyasa aynı anda TRENDING + HIGH_VOLATILITY + LOW_LIQUIDITY
olabilir; bunlar mutually-exclusive tek enum içinde temsil edilemez.
**Consequences:** `ConsensusResult.market_regime: MarketRegime` alanı
`ConsensusResult.regime: RegimeContext` olarak değişti (BREAKING). `Signal`
şu an için ayrı bir regime alanı taşımıyor (ileride gerekirse eklenecek).

## Karar 9 — Signal single source of truth: direction artık property

**Date:** 2026-08-31
**Decision:** `Signal.direction` constructor alanı olmaktan çıkarıldı;
`score`'dan türetilen bir `@property` oldu (`SignalDirection.from_score`).
**Reason:** `direction=STRONG_LONG, score=-1.0` gibi çelişkili bir illegal
state'in fiziksel olarak temsil edilebilmesi kabul edilemezdi.
**Consequences:** `Signal(...)` çağrılarında `direction=` argümanı artık
KABUL EDİLMEZ (TypeError). Bu BREAKING bir değişikliktir.

## Karar 10 — Gerçek immutability: MappingProxyType + defensive copy

**Date:** 2026-08-31
**Decision:** `FeatureVector.values`, `AgentEvidence.supporting_metrics`,
`RiskAssessment.contradicting_metrics`, `SafetyEvent.context`,
`DataQualityResult.details` alanları artık `_validation.freeze_mapping()`
ile (önce defensive copy, sonra `MappingProxyType`) sarılıyor.
**Reason:** `@dataclass(frozen=True)` yalnızca top-level attribute
assignment'ı engeller; nested mutable dict'ler `frozen=True` ile korunmaz.
**Consequences:** Bu alanlara `obj.mapping["x"] = y` yazmak `TypeError`
fırlatır; constructor'a verilen orijinal dict sonradan mutate edilse bile
domain nesnesi etkilenmez.

## Karar 11 — Symbol normalization merkezi politika

**Date:** 2026-08-31
**Decision:** `_validation.normalize_symbol()`: strip + upper-case. Symbol
taşıyan TÜM domain modelleri (Candle, CandleIdentity, Trade, OrderBookSnapshot,
FeatureVector, AgentEvidence, Signal, SafetyEvent, SequenceCursor,
MarketDataEvent, ProviderHealthSnapshot, ConsensusResult) construction
sırasında bunu uygular.
**Reason:** "btcusdt" / "BTCUSDT" / " BTCUSDT " aynı varlık için üç farklı
state key oluşturma riskinin ortadan kaldırılması.

## Karar 12 — Finite-number invariant merkezi hale getirildi

**Date:** 2026-08-31
**Decision:** `_validation.require_finite()` (`math.isfinite`), NaN/±inf
taşıyabilecek TÜM numeric domain alanlarına uygulandı: Candle OHLCV,
Trade price/quantity, OrderBookLevel price/quantity, FeatureVector values,
AgentEvidence score + supporting_metrics, Signal score/confidence,
RiskAssessment confidence_multiplier + contradicting_metrics,
compute_agreement() girdi/çıktısı.

## Karar 13 — Order book: strict monotonic + duplicate-free + locked-book reddi

**Date:** 2026-08-31
**Decision:** `OrderBookSnapshot` artık bids'in KESİN azalan, asks'ın KESİN
artan sırada olduğunu, her side içinde duplicate fiyat seviyesi olmadığını,
ve `best_bid < best_ask` (locked book dahil crossed sayılır) olduğunu
zorunlu kılıyor.

## Karar 14 — Repository temizliği: yanlışlıkla oluşan stray klasör kaldırıldı

**Date:** 2026-08-31
**Decision:** Önceki teslimde bash brace-expansion hatası sonucu oluşan
`crypto_signal_engine/{domain,providers,quality,safety,tests}` adlı boş
klasör ve eski (package-yanlış) `crypto_signal_engine/` kök dizini tamamen
silindi; proje `pyproject.toml` ile sıfırdan, doğru package yapısıyla
yeniden inşa edildi.
**Not:** Bu klasörün varlığı, parent-directory import testinin (Karar 4)
name-collision nedeniyle başarısız olmasına da yol açmıştı — temizlik hem
kozmetik hem işlevsel bir düzeltmeydi.

---

# FAZ 1 FINAL HARDENING KARARLARI (bağımsız adversarial inceleme, 2026-08-31)

## Karar 15 — SafetyState: dataclass'tan tam encapsulate sınıfa (BREAKING)

**Date:** 2026-08-31
**Decision:** `SafetyState` artık `@dataclass` DEĞİL; private alanlar
(`_signal_generation_halted`, `_halt_reason`, `_active_warnings`,
`_last_evaluated_at`) + read-only property'ler kullanan sıradan bir
sınıftır. Constructor (`__init__`) hiçbir argüman KABUL ETMEZ — her
`SafetyState()` her zaman RUNNING durumunda başlar. Mutation yalnızca
`halt()`, `resume()`, `add_warning()`, `clear_warnings()`,
`mark_evaluated()` üzerinden mümkündür.
**Reason:** Önceki mutable `@dataclass` tasarımı iki bağımsız problem
taşıyordu: (1) `SafetyState(signal_generation_halted=True, halt_reason=None)`
gibi illegal bir state constructor seviyesinde üretilebiliyordu, (2)
`state.signal_generation_halted = False` gibi doğrudan attribute atamasıyla
`halt()`/`resume()` state machine API'si tamamen bypass edilebiliyordu.
**Consequences:** `SafetyState(...)` argümanlı constructor çağrıları artık
`TypeError`. Dış kod `state.signal_generation_halted = X` yazamaz
(`AttributeError`, property'nin setter'ı yok). HALTED/RUNNING invariant'ı
(`halted <=> halt_reason is not None ve severity==HALT`) yalnızca
`halt()`/`resume()`'un birlikte garanti ettiği bir eş-değişim olduğu için
hiçbir ara/bozuk durum gözlemlenemez.

## Karar 16 — Ortak `require_enum()`: runtime enum type safety

**Date:** 2026-08-31
**Decision:** `_validation.require_enum(value, enum_type, field_name)` eklendi
ve TÜM enum-typed domain alanlarına uygulandı: `AgentEvidence.agent`,
`AgentEvidence.primary_timeframe`, `Signal.risk_level`,
`Signal.primary_timeframe`, `RegimeContext.structure/volatility/liquidity`,
`SafetyEvent.reason_code/severity`, `ProviderHealthSnapshot.connection_state`,
`MarketDataEvent.event_type`, `SequenceCursor.event_type`,
`DataQualityResult.status`, `RiskAssessment.risk_level`.
**Reason:** Type hint (`agent: AgentName`) runtime'da hiçbir şeyi garanti
etmiyordu; bağımsız inceleme `AgentEvidence(agent="QUANT", ...)` gibi string
değerlerin sessizce kabul edildiğini gösterdi. Ayrıca `EventType(str, Enum)`
olduğundan `"CANDLE" == EventType.CANDLE` DOĞRU olduğu için eski dict-lookup
bazlı kontrol (`_EXPECTED_PAYLOAD_TYPE.get(self.event_type)`) bir plain
string'i YAKALAYAMIYORDU — yalnızca isinstance bazlı kontrol bunu çözer.
**Consequences:** Bu alanlardan herhangi birine string veya başka bir tip
verilmesi artık `TypeError` fırlatır; hiçbir sessiz cast yapılmaz.
**Not (bulunan ek bug):** `AgentEvidence.agent` önceki revizyonda HİÇ
doğrulanmıyordu (yalnızca `primary_timeframe` kontrol ediliyordu) — bu,
bağımsız incelemenin doğru şekilde yakaladığı gerçek bir eksiklikti.

## Karar 17 — Final Signal provenance: context_id + as_of + duplicate/overlap kontrolü (BREAKING)

**Date:** 2026-08-31
**Decision:** `Signal`'a zorunlu `context_id: str` alanı eklendi.
`supporting_factors` ve `contradicting_factors` içindeki TÜM AgentEvidence
nesneleri için:
- `evidence.symbol == signal.symbol` (mixed-symbol reddi)
- `evidence.context_id == signal.context_id` (mixed-context reddi)
- `evidence.as_of <= signal.timestamp` (gelecekten evidence reddi;
  eşitlik KABUL edilir — "aynı an" geçerli bir provenance'tır)
Ayrıca: aynı evidence nesnesi (değer eşitliğiyle) hem supporting hem
contradicting'te bulunamaz; ve GLOBAL bir one-agent/one-evidence
politikası uygulanır — aynı `AgentName`, supporting+contradicting
birleşiminde yalnızca BİR KEZ geçebilir (ConsensusResult'taki per-context
kuralından daha sıkı: burada kategori sınırı da yok).
**Reason:** `ConsensusResult` provenance kontrolü yapıyordu ama final
`Signal` aynı bütünlüğü korumuyordu; bağımsız test
`Signal.symbol=BTCUSDT` + `supporting_factor.symbol=ETHUSDT` gibi bir
durumun kabul edildiğini gösterdi.
**Timestamp politikası:** `context_id` (Karar 7'deki gibi) deterministik
gruplama anahtarıdır; `as_of <= timestamp` invariantı ayrıca ve ek olarak
uygulanır çünkü bu, provenance'ın zamansal tutarlılığı için farklı bir
boyuttur (context_id eşleşmesi "aynı snapshot'tan mı" sorusuna, as_of
kontrolü "mantıksal olarak imkânsız bir gelecekten mi geliyor" sorusuna
cevap verir).
**Consequences:** `Signal(...)` artık zorunlu `context_id` argümanı ister
(BREAKING). Var olan tüm `Signal(...)` çağrıları güncellenmelidir.

## Karar 18 — DataQualityResult: enum + reason invariant hardening

**Date:** 2026-08-31
**Decision:** `DataQualityResult.status` artık `require_enum` ile
doğrulanır; `reason` bir `str` olmak zorundadır (`TypeError`, aksi halde);
`status != OK` iken `reason` boş/whitespace olamaz (`ValueError`).
**Reason:** `status="STALE"` (string) veya `status=STALE, reason=""` gibi
"neden reddedildiği bilinmeyen bir red kararı" invalid state olarak
değerlendirildi.

## Karar 19 — normalize_symbol: deterministik TypeError

**Date:** 2026-08-31
**Decision:** `normalize_symbol()` artık girdi tipini kontrol eder;
`None`/`int`/`list` gibi str-olmayan girdilerde incidental
`AttributeError` yerine açık `TypeError` fırlatır.
**Reason:** Domain boundary'nin hata davranışı deterministik olmalı;
"hangi hatayı alacağım" implementasyon detayına (str metodunun iç
davranışına) bağlı olmamalı.

## Karar 20 — Reproducible acceptance procedure (Quality Gate 29)

**Date:** 2026-08-31
**Decision:** İki tamamlayıcı mekanizma eklendi:
1. `scripts/verify_phase1.py` — repo kökünden tek komutla editable install
   + parent-dir import smoke test + tam pytest + compileall + repository
   safety scan çalıştıran resmi acceptance script'i.
2. `tests/test_package_import.py` tamamen self-contained hale getirildi:
   artık module-scoped bir fixture içinde SIFIRDAN izole bir venv kurup
   (`--no-build-isolation`, `--system-site-packages` ile ana ortamdaki
   setuptools'u miras alarak) paketi oraya kurar ve importları o izole
   venv'in python'ıyla doğrular.
**Reason:** Önceki revizyonda temiz bir ZIP açılıp doğrudan `pytest`
çalıştırıldığında (`pip install -e .` önceden yapılmadan) iki test FAIL
veriyordu — acceptance prosedürü kendi başına reproducible değildi.
**Consequences:** `python -m pytest` artık HERHANGİ bir ortamda (paket
önceden kurulu olsun ya da olmasın) 0 failure ile tamamlanır. `python -m
build` ile wheel+sdist üretimi ayrıca manuel olarak doğrulandı (bkz.
PHASE1_FINAL_ACCEPTANCE_REPORT.md).

## Karar 21 — Package import testi: build backend bağımlılığı tamamen kaldırıldı (v2 reproducibility düzeltmesi)

**Date:** 2026-08-31
**Decision:** `tests/test_package_import.py`'nin izole venv fixture'ı artık
`venv.create(..., system_site_packages=True)` + `pip install
--no-build-isolation` YAKLAŞIMINI KULLANMIYOR. Bunun yerine: teslimat
sırasında önceden inşa edilmiş bir wheel (`dist/crypto_signal_engine-*-py3-
none-any.whl`, repoya dahil) tamamen izole (`system_site_packages=False`)
bir venv'e `pip install --no-index --no-deps <wheel>` ile OFFLINE kurulur.
**Reason:** Bağımsız bir reviewer ortamında önceki yaklaşım
`BackendUnavailable: Cannot import 'setuptools.build_meta'` hatasıyla
FAIL verdi — "izole venv, ana ortamdaki setuptools'u miras alır ve bu
sayede build backend bulunabilir" varsayımı ortam-bağımlıydı ve genellenemedi.
**Consequences:** Wheel kurulumu salt unzip+copy olduğundan, hedef ortamda
setuptools'un varlığı, ağ erişimi, veya PEP 668/externally-managed-environment
kısıtları SONUCU HİÇ ETKİLEMEZ. Ek olarak `test_installed_package_is_not_
the_source_tree` testi, kurulan paketin gerçekten venv'in site-packages'ına
kopyalandığını (source-tree-via-cwd false-positive'inin imkansız olduğunu)
doğrular. `dist/` klasörü artık repo teslimatının kalıcı bir parçasıdır.

## Karar 22 — scripts/verify_phase1.py: pip install -e . adımı tamamen kaldırıldı (v3 reproducibility düzeltmesi)

**Date:** 2026-08-31
**Decision:** `scripts/verify_phase1.py`'nin 1. adımı olan `pip install -e .`
tamamen kaldırıldı. Script artık Karar 21'deki wheel-tabanlı offline
kurulum stratejisini (`dist/` altındaki önceden inşa edilmiş wheel'i izole
venv'e `--no-index --no-deps` ile kurma) kullanıyor; ardından tam pytest
suite'i doğrudan repo source ağacından (kurulum GEREKMEDEN) çalıştırılıyor.
**Reason:** `pyproject.toml`'daki `setuptools>=68.0` build-dependency'si,
`pip install -e .` çağrıldığında pip'in bunu PyPI'den indirmeye çalışmasına
yol açıyordu — ağsız/temiz bir ortamda bu adım FAIL veriyordu ve script'in
kendi "offline, reproducible acceptance" (Gate 29) hedefiyle doğrudan
çelişiyordu.
**Consequences:** Script artık hiçbir adımda build backend çağırmaz, ağ
erişimi gerektirmez, ve `--break-system-packages` fallback'ine ihtiyaç
duymaz. Wheel bulunamazsa (silinmiş/eksikse) script deterministik olarak
FAIL verir; build backend'i tetikleyerek "sessizce" yeniden inşa etmeye
ÇALIŞMAZ.

---

# FAZ 2 KARARLARI (Binance Market Data MVP, 2026-08-31)

## Karar 23 — update_seq politikası: Binance kline event time (E)

**Date:** 2026-08-31
**Decision:** Candle sequencing için `update_seq` olarak Binance kline
WebSocket event'inin `E` (event time, milisaniye) alanı kullanılıyor.
**Reason:** Binance kline payload'ı, aynı candle için gelen art arda
update'ler arasında ayrı bir monoton sayaç sağlamıyor; `E` pratikte
monotonik artan tek alan.
**Known limitation:** Aynı milisaniyede iki farklı update teorik olarak
mümkündür (çok nadir); bu durumda `CandleSequencer` ikinci update'i
duplicate olarak reddeder. Bu, Faz 1'in candle sequencing invariant'ının
(update_seq aynıysa duplicate) doğal bir sonucudur ve kabul edilebilir bir
sınırlama olarak dokümante edilmiştir.

## Karar 24 — DataQualityStatus enum reuse (candle-odaklı isimler, trade/order-book'ta yeniden kullanım)

**Date:** 2026-08-31
**Decision:** Phase 1'in `DataQualityStatus` enum'u (candle-odaklı isimler:
`MISSING_CANDLE`, `DUPLICATE_CANDLE`) DEĞİŞTİRİLMEDİ. Faz 2'nin
`BinanceDataQualityGate`'i, trade/order-book bağlamında en yakın semantik
karşılıkları yeniden kullanıyor: trade non-monotonic ID →
`TIMESTAMP_MISMATCH`, order-book gap → `WEBSOCKET_GAP`.
**Reason:** Phase 1'in `DataQualityResult`/`DataQualityStatus` tasarımı
zaten üç `check_*` metodu için PAYLAŞILAN tek bir status kümesi
öngörüyordu; bu bir Faz 2 icadı değil, Faz 1'in kendi tasarım tercihiydi.
Yeni status değerleri eklemek (Faz 2'nin görevi olmayan) bir Phase 1 domain
değişikliği gerektirirdi; bunun yerine dürüstçe dokümante edilen bir
reuse tercih edildi.
**Alternatives:** `DataQualityStatus`'a yeni değerler eklemek (reddedildi
— Phase 1 domain'ini gereksiz yere genişletirdi); trade/order-book için
ayrı bir enum oluşturmak (reddedildi — `DataQualityResult.status` tipi tek
bir enum ile sabit, Phase 1 contract'ı bozulurdu).

## Karar 25 — `health()` çoklu-stream agregasyonu

**Date:** 2026-08-31
**Decision:** Phase 1 `LiveDataProvider.health()` parametre almadığından,
`BinanceMarketDataProvider.health()` en son güncellenen stream'in
`ProviderHealthSnapshot`'ını döndürüyor. Belirli bir stream'in sağlığını
sorgulamak için Phase 1 ABC'sinin parçası OLMAYAN ek bir `health_for(key)`
metodu sağlandı.
**Reason:** Provider birden fazla symbol/stream'i eşzamanlı besleyebiliyor;
ABC imzası bunu öngörmüyor. Bu, Phase 1 contract'ını ihlal etmeyen,
dokümante edilmiş bir yorumlama kararıdır.

## Karar 26 — StateManager: previous-candle tracking düzeltmesi (test sırasında bulunan bug)

**Date:** 2026-08-31
**Decision:** `StateManager`, quality-gate'in `previous` argümanı için
`(symbol, timeframe) -> en son BAŞARIYLA commit edilmiş candle` şeklinde
ayrı bir sözlük (`_latest_candle`) tutuyor; `CandleStateStore.peek_previous`
(Phase 1 contract'ı, "BU identity için state" anlamına gelir) DEĞİL.
**Reason:** İlk implementasyon `candle_store.peek_previous(update.identity)`
kullanıyordu — bu, YENİ bir `open_time` için HER ZAMAN `None` döner,
gap/missing-interval tespiti hiç tetiklenmiyordu. Ara düzeltme (sabit
"bir-önceki-interval" hesaplaması) da çoklu-interval gap'lerde başarısız
oluyordu çünkü o interval de commit edilmemiş oluyordu. Bu, adversarial
test yazımı sırasında yakalanan GERÇEK bir bug'dı (bkz.
`tests/test_binance_provider.py::TestCandleGapRecovery`).
**Consequences:** Phase 1 `CandleStateStore`/`peek_previous` contract'ı
DEĞİŞMEDİ (hâlâ "bu identity için state" anlamına geliyor, sequencer'ın
kendi dedup/out-of-order mantığı için doğru kullanılıyor); yalnızca Faz 2
`StateManager`'ın kendi ek state'i (gap-detection amaçlı) eklendi.

## Karar 27 — Order book resync sonucu queue'ya iletilir (bug fix)

**Date:** 2026-08-31
**Decision:** `_resync_order_book`, `ingest_snapshot()`'ın döndürdüğü
canonical `OrderBookSnapshot`'ı ARTIK DÖNDÜRÜYOR; çağıran kod bunu
quality-gate'ten geçirip queue'ya (tüketiciye) iletiyor.
**Reason:** İlk implementasyonda resync sonucu sessizce atlanıyordu —
resync sonrası ilk güvenilir book durumu tüketiciye hiç ulaşmıyordu. Bu da
adversarial test yazımı sırasında yakalanan gerçek bir bug'dı.

## Karar 28 — Gerçek WebSocket network implementasyonu Faz 2 testleri kapsamı DIŞINDA

**Date:** 2026-08-31
**Decision:** `transport.py`, yalnızca `HttpClient`/`WebSocketConnection`
Protocol'lerini + stdlib-only gerçek bir `UrllibHttpClient` implementasyonu
sağlar. Gerçek bir network-backed `WebSocketConnectionFactory`
implementasyonu (örn. `websockets` kütüphanesi ile) BU FAZDA YAZILMADI.
**Reason:** Bölüm 22 gereği testler mock/fake transport kullanmalı, gerçek
Binance ağına bağımlı olmamalı. `websockets` gibi bir kütüphaneyi hard
dependency yapmak, offline/reproducible acceptance (Gate 29) hedefini
riske atardı. Provider mimarisi (dependency injection ile
`WebSocketConnectionFactory`) gerçek bir implementasyonun sonradan
eklenmesini mümkün kılacak şekilde tasarlandı.
**Consequences (deferred):** Gerçek WS network implementasyonu, opsiyonel
bir bağımlılık olarak Faz 2 sonrası (veya Faz 2'nin bir "production
adapter" eki olarak) eklenebilir; bu DECISIONS.md'de deferred karar olarak
kaydedilmiştir.

## Karar 29 — Deferred: quality threshold'ları config dosyasına taşınmadı

**Date:** 2026-08-31
**Decision:** `BinanceQualityThresholds` (staleness saniyeleri, outlier
oranı) şu an kod içinde adlandırılmış sabitler olarak tanımlı;
`config/risk.yaml` gibi bir dosyaya taşınmadı.
**Reason:** Faz 2 kapsamı "correctness → sequencing → recovery → data
quality → persistence → observability → performance" önceliğine göre
sınırlandırıldı; config-dosyası entegrasyonu davranışı değiştirmeyen bir
altyapı iyileştirmesidir, Faz 3+'a ertelendi.

## Karar 30 — Deferred: backfill penceresi sabit, gerçek gap boyutuna göre değil

**Date:** 2026-08-31
**Decision:** `_backfill_candle_gap`, eksik interval'i tam olarak hesaplamak
yerine sabit bir pencere (`duration * 20`) kullanıyor.
**Reason:** Basit ve güvenli bir ilk implementasyon; StateManager'ın
`_latest_candle` takibi kullanılarak gerçek gap boyutunun hesaplanması
mümkündür ama bu, Faz 2'nin "correctness first" önceliğinin ötesinde bir
optimizasyondur — Faz 3+'a ertelendi.
**STATUS: SÜPERSEDE EDİLDİ — bkz. Karar 34.** Bağımsız reviewer'ın ikinci
turu bu kararı "yetersiz" bulup deterministic recovery talep etti; sabit
pencere yaklaşımı tamamen kaldırıldı.

---

# FAZ 2 v2 HARDENING KARARLARI (bağımsız reviewer ikinci tur, 2026-08-31)

## Karar 31 — Order book: strict equality yerine overlap semantiği

**Date:** 2026-08-31
**Decision:** `OrderBookSynchronizer.ingest_diff()` (SYNCED durumdayken) ve
`ingest_snapshot()`'ın buffer-süreklilik kontrolü, artık strict equality
(`event.first_update_id == expected + 1`) yerine OVERLAP semantiği
kullanıyor: `event.first_update_id <= expected_next_first_id` yeterlidir;
yalnızca `first_update_id > expected` GERÇEK bir gap sayılır.
**Reason:** Binance pratikte ardışık diff event'ler arasında küçük bir
örtüşme (redelivery) gönderebilir. Event'in MUTLAK miktar temsil etmesi
nedeniyle bir aralığın kısmen tekrar uygulanması zararsızdır (idempotent).
Reviewer probe'ları: A) local=100, incoming U=100,u=102 → ACCEPT; B)
local=100, incoming U=102,u=103 → REJECT/RESYNC. İkisi de test edildi.
**Consequences:** `tests/test_binance_order_book_sync.py::TestOutOfOrderDiff
::test_out_of_order_after_sync_is_gap` testinin verisi yeni semantikle
tutarlı hale getirildi (artık gerçek bir gap senaryosu — first_update_id
açıkça expected'ın üstünde — kullanıyor).

## Karar 32 — SQLite commit atomicity: dry-run + persist-then-apply

**Date:** 2026-08-31
**Decision:** `SqliteCandleStateStore.commit()` artık iki aşamalı: (1)
`copy.deepcopy` ile alınan bir SCRATCH sequencer üzerinde sequencing kararı
"dry-run" edilir (gerçek `self._sequencer` HİÇ MUTATE EDİLMEZ), (2) yalnızca
dry-run ACCEPTED ise SQLite'a persist edilir, (3) persist BAŞARILI olursa
AYNI update gerçek `self._sequencer`'a uygulanır.
**Reason:** Önceki implementasyon `self._sequencer.apply(update)`'i
persist'ten ÖNCE çağırıyordu — persist başarısız olsa bile in-memory
sequencer state'i zaten ilerlemiş oluyordu (atomicity ihlali). Reviewer
probe C: "SQLite persist failure → PersistenceError → canonical sequencer
state UNCHANGED" bunu yakaladı.
**Consequences:** Bu düzeltme, Phase 1 `CandleSequencer` (domain modülü)
kaynak kodunu HİÇ DEĞİŞTİRMEDEN, yalnızca standart kütüphane
`copy.deepcopy` ile Faz 2 tarafında sağlanan bir garantidir. Persist
başarısız olduktan sonra AYNI update retry edildiğinde hâlâ kabul
edilebilir (sequencer hiç ilerlememiş olduğundan) — bu da test edildi.

## Karar 33 — Stale-feed detection: gerçek implementasyon (asyncio.wait_for)

**Date:** 2026-08-31
**Decision:** `BinanceConfig.stale_feed_threshold_seconds` artık GERÇEKTEN
kullanılıyor. `provider.py::_recv_or_stale()`, `connection.recv()`'i
`asyncio.wait_for(..., timeout=threshold_seconds)` ile sınırlıyor;
timeout'ta yeni `StaleFeedError(TransportError)` fırlatılıyor (candle,
trade, order-book stream'lerinin ÜÇÜNDE de).
**Reason (tasarım kararı — injectable clock/sleeper ile ilişkisi):** Bu
mekanizma BİLİNÇLİ OLARAK gerçek event-loop zamanını kullanır, injectable
`Sleeper`'ı DEĞİL. `Sleeper` soyutlaması reconnect backoff (potansiyel
olarak onlarca saniyeye varan gecikmeler) için test hızını korumak üzere
tanıtılmıştı. Bir I/O idle-timeout'u ise doğası gereği gerçek geçen süreye
dayanır; bunu injectable hale getirmek (örn. iki task'ı Sleeper ile
yarıştırmak) FakeSleeper'ın "her zaman anında çözülme" davranışı
(reconnect backoff testleri için gerekli) ile ÇAKIŞIYOR ve non-deterministik
race'lere yol açıyordu (denendi, terk edildi). Bunun yerine testler çok
küçük (saniyelerce DEĞİL, örn. 0.03s) gerçek `stale_feed_threshold_seconds`
değerleri kullanır — bu hem deterministik hem hızlıdır.
**Cancellation ayrımı:** `asyncio.wait_for`'ın kendi iç timeout'u
`TimeoutError` (→ `StaleFeedError`) fırlatırken, DIŞARIDAN bir cancellation
`asyncio.CancelledError` olarak PROPAGATE OLUR (wait_for tarafından
otomatik ayrıştırılır) — bu yüzden normal shutdown asla stale-reconnect
olarak yorumlanamaz. Test edildi (`test_5_cancellation_does_not_produce_
stale_reconnect`).
**Consequences:** Order book stream'i için stale-reconnect, MEVCUT
reconnect yolunu (`synchronizer.reset()` zaten her reconnect'te çağrılıyor)
kullandığından ek kod gerektirmeden otomatik resync tetikler.

## Karar 34 — Deterministic candle gap recovery (Karar 30'u süperseder)

**Date:** 2026-08-31
**Decision:** `_backfill_candle_gap`, sabit `duration*20` penceresi yerine
`StateManager.latest_candle(symbol, timeframe)` (YENİ, güvenli PUBLIC API —
private attribute hack YOK) üzerinden GERÇEK eksik aralığı
(`[latest.open_time + duration, incoming_open_time)`) hesaplıyor. Backfill
tamlık doğrulaması eklendi: REST yanıtı beklenen TÜM open_time'ları
karşılamıyorsa recovery başarısız sayılır, orijinal update tekrar
denenmez.
**close_time sequencing ihlali önlendi:** Backfill candle'lar için
synthetic update_seq, `close_time` DEĞİL, candle identity'sinin zaten bir
parçası olan `open_time` (ms) kullanılarak üretilir
(`_backfill_update_seq()`). Bu, live WS update_seq politikasından (event
time `E`) AÇIKÇA AYRI ve dokümante edilmiş bir politikadır.
**Reason:** Reviewer'ın ikinci turu, sabit pencerenin "deterministic gap
recovery değil" olduğunu ve `close_time` kullanımının Faz 1'in "close_time
sequence değildir" kararını ihlal riski taşıdığını belirtti.
**Consequences:** 10 adversarial test eklendi (`tests/test_binance_provider.py
::TestCandleGapRecovery`): multi-interval gap, eski-ama-geçerli historical
candle reddedilmemesi, malformed historical candle reddi (stream'i
çökertmeden), chronological commit, duplicate backfill idempotency,
orijinal update retry, eksik backfill'in başarısız sayılması, cross-symbol
izolasyon, açık candle'ın historical recovery'ye commit edilmemesi.

## Karar 35 — Quality model ayrımı: structural validity vs. realtime freshness

**Date:** 2026-08-31
**Decision:** `BinanceDataQualityGate`'e YENİ, Phase 1 ABC'sinin PARÇASI
OLMAYAN bir `check_historical_candle(candle, previous)` metodu eklendi —
yalnızca structural validity (alignment, missing-interval, zero-volume
anomaly, extreme outlier) uygular, realtime staleness UYGULAMAZ.
`StateManager.handle_historical_candle_update()` bunu duck-typing ile
(`hasattr`/`callable` kontrolü) çağırır; gate bu metodu desteklemiyorsa
(generic bir `DataQualityGate` implementasyonu) güvenli şekilde standart
`check_candle`'a düşer.
**Reason:** Reviewer'ın ikinci turu: historical/backfill candle'lar doğası
gereği "şimdi"den eski olur; bunları `max_candle_staleness_seconds`
nedeniyle reddetmek yanlıştır. Realtime WS ingestion'da ise staleness
kontrolü DEVAM eder.
**Consequences:** Phase 1 `quality/base.py` ABC'si DEĞİŞMEDİ — bu, yalnızca
concrete `BinanceDataQualityGate` sınıfının ek, opsiyonel bir metodudur.

## Karar 36 — Küçük boundary-hardening: queue_maxsize ve supported_symbols

**Date:** 2026-08-31
**Decision:** `BinanceMarketDataProvider.__init__`, `queue_maxsize <= 0`
değerini `ValueError` ile reddeder. `BinanceConfig.supported_symbols`,
construction sırasında her symbol'ü `normalize_symbol()` ile
normalize/validate eder (str-olmayan/boş symbol sessizce config'e giremez).
**Reason:** Reviewer'ın ikinci turunda belirtilen küçük sınır sıkılaştırma
maddeleri; mimari değişiklik değildir.

## Karar 37 — Backfill: REST yanıtı `expected_open_times` ile sınırlanır

**Date:** 2026-08-31
**Decision:** `_backfill_candle_gap`, REST'ten dönen candle'ları artık
YALNIZCA önceden hesaplanan `expected_open_times` kümesindeki open_time'lara
sahip olanlarla filtreliyor (`c.open_time in expected_open_times_set`).
**Reason:** Binance'in gerçek `endTime` sınır davranışı, `gap_end`
(incoming candle'ın open_time'ı) ile TAM eşleşen veya onu içeren bir
candle'ı da REST yanıtına dahil edebilir. Bu durumda, incoming candle'ın
kendisi (veya gap dışındaki başka bir candle) yanlışlıkla historical
recovery yolundan (farklı/stale bir değerle) commit edilebiliyordu — bu,
canonical state'in canlı WS update'i yerine REST'in (potansiyel olarak
gecikmeli) verisiyle oluşmasına yol açardı.
**Consequences:** Incoming candle'ın identity'si HER ZAMAN orijinal WS
update'i üzerinden (backfill sonrası retry ile) canonical hâle gelir;
REST yanıtındaki aynı open_time'lı bir satır sessizce YOK SAYILIR. Yeni
regresyon testi: `tests/test_binance_provider.py::TestCandleGapRecovery::
test_11_rest_response_including_incoming_candle_is_not_committed_via_backfill`.

---

# FAZ 3 KARARLARI (Feature Engine, 2026-08-31)

## Karar 38 — Hata taksonomisi tek merkezi modülde toplanır

**Date:** 2026-08-31
**Decision:** `FeatureError`, `FeatureValidationError`, `InsufficientHistoryError`,
`FeatureCalculationError`, `FeatureStateError` mevcut `crypto_signal_engine/errors.py`
dosyasına eklendi; ayrı bir `features/errors.py` modülü OLUŞTURULMADI.
**Reason:** Tek bir merkezi hata hiyerarşisi (Phase 2'den beri) korunuyor;
Faz 3'e özgü bir hata modülü ayırmak gereksiz parçalanma yaratırdı.

## Karar 39 — No-look-ahead: yapısal filtreleme (opsiyonel değil, zorunlu)

**Date:** 2026-08-31
**Decision:** `FeatureEngine.compute_candle_features()`/`compute_trade_features()`,
çağırana verilen ham listeyi HER ZAMAN `as_of`'a göre filtreler (candle:
`close_time <= as_of`; trade: `timestamp <= as_of`) — bu, opsiyonel bir
parametre veya "çağıranın sorumluluğu" DEĞİLDİR, engine'in kendisi
tarafından her çağrıda otomatik uygulanır.
**Reason:** Bölüm 17'nin "hard acceptance gate" gereksinimini (future
observations must not change past feature values) İMKANSIZ-BAŞARISIZLIK
(fail-impossible) seviyesinde garanti etmek için — bir geliştiricinin
"filtrelemeyi unutması" ile look-ahead bias'ın sızması yapısal olarak
ENGELLENDİ.
**Consequences:** `FeatureHistoryStore.as_of()` de aynı ilkeyi cross-timeframe
sorgular için uygular (bkz. PHASE3_FEATURE_ENGINE.md).

## Karar 40 — RSI "değişim yok" konvansiyonu: 50 (100 değil)

**Date:** 2026-08-31
**Decision:** `avg_gain == 0 and avg_loss == 0` (fiyat hiç değişmedi)
durumunda RSI = 50.0 (nötr) döner; bazı popüler kütüphaneler bu durumda
100 döndürür.
**Reason:** "Fiyat hiç hareket etmedi" durumunun "güçlü yükseliş trendi"
(RSI=100, ki bu yalnızca avg_loss=0 VE avg_gain>0 iken anlamlıdır) ile
karıştırılmaması için AÇIKÇA farklı bir değer (nötr 50) tercih edildi ve
dokümante edildi (Bölüm 12 — "industry-standard ambiguity" çözülmesi
gerekiyordu).

## Karar 41 — Insufficient-history: omit (raise değil) snapshot seviyesinde

**Date:** 2026-08-31
**Decision:** `compute_candle_features()`/`compute_trade_features()`,
`InsufficientHistoryError` fırlatan bir feature'ı snapshot'tan sessizce
OMİT EDER (kısmi/eksik bir snapshot döner); `FeatureCalculationError` İSE
yutulmaz. Düşük seviyeli `compute_single_candle_feature()` API'si her iki
hatayı da DOĞRUDAN fırlatır.
**Reason:** Bölüm 10, iki seçenek sunuyordu ("explicit insufficient-history
outcome" VEYA "omit from incomplete snapshot"); çoklu-feature toplu
hesaplama senaryosunda (`compute_candle_features`) tek bir eksik feature
yüzünden TÜM snapshot'ın başarısız olması pratik değildi; bu yüzden omit
tercih edildi, ama düşük seviyeli API'de doğrudan raise korunarak "explicit
outcome" ihtiyacı da karşılandı.

## Karar 42 — Order book feature'ları için timeframe: M1 placeholder

**Date:** 2026-08-31
**Decision:** `compute_order_book_features()`, `FeatureIdentity`/`FeatureSnapshot`
inşası için `Timeframe.M1`'i "raporlama çerçevesi" placeholder'ı olarak kullanır.
**Reason:** Order book snapshot'ları doğası gereği bir timeframe'e bağlı
değildir (Phase 1 `OrderBookSnapshot` modelinde timeframe alanı yoktur),
ama `FeatureIdentity`/`FeatureHistoryStore` state-isolation için bir
timeframe alanı gerektirir. Bu, dokümante edilmiş bir modelleme tercihidir;
Phase 1 domain modeli DEĞİŞTİRİLMEDİ.

## Karar 43 — VWAP fiyat bazı: typical price (candle-seviyesi)

**Date:** 2026-08-31
**Decision:** `rolling_vwap`, trade-bazlı gerçek VWAP yerine candle
typical price `(H+L+C)/3` kullanır.
**Reason:** Phase 2 candle domain modeli trade-içi fiyat dağılımını
taşımaz (yalnızca OHLCV); candle-seviyesinde VWAP hesaplamak için
endüstride yaygın kabul gören yaklaşım typical price'tır. Gerçek
trade-bazlı VWAP, Phase 2 `Trade` verisi kullanılarak ayrı bir
trade-feature olarak Faz 4+'ta eklenebilir (deferred).

## Karar 44 — Persistence: in-memory yeterli, SQLite eklenmedi

**Date:** 2026-08-31
**Decision:** `FeatureHistoryStore` yalnızca in-memory'dir; Faz 3 için
SQLite persistence OPSİYONEL olarak belirtildiği ve "in-memory feature
history Faz 3 acceptance için yeterli" (Bölüm 16) olduğu için eklenmedi.
**Reason:** Basit ve doğru bir Faz 3 kapsamı; gereksiz karmaşıklık
eklenmedi (Bölüm 16 — "do not compromise core feature correctness to add
persistence").

## Karar 45 — No-look-ahead blocker düzeltmesi: as_of filtresi is_closed kontrolünden ÖNCE

**Date:** 2026-08-31
**Decision:** `FeatureEngine._prepare_candles()`, artık `close_time > as_of`
filtresini `is_closed` kontrolünden ÖNCE uyguluyor. Ufkun DIŞINDAKİ
(gelecekteki) bir candle, `is_closed` durumu ne olursa olsun, closed-candle
kontrolüne HİÇ ULAŞMADAN sessizce atlanır.
**Reason:** Önceki sıralama, "gelecekte henüz oluşmakta olan, doğası
gereği açık bir candle" (tamamen normal) ile "değerlendirme ufkunun
İÇİNDE kalan ama yanlışlıkla kapanmamış bir candle" (gerçek bir politika
ihlali) arasındaki ayrımı YANLIŞ kuruyordu — gelecekteki açık candle'lar
sahte bir `FeatureValidationError` ile reddediliyordu. Bu, T anındaki bir
feature'ın "dataset T'de bitiyor" ile "dataset T'den sonraki (açık)
gözlemler de içeriyor" senaryolarında AYNI (ve hatasız) sonucu üretmesi
gereken no-look-ahead hard acceptance gate'inin (Bölüm 17) doğrudan bir
ihlaliydi.
**Consequences:** Ufkun İÇİNDE kalan (`close_time <= as_of`) kapanmamış
bir candle HÂLÂ reddedilir (closed-candle politikası değişmedi) — yalnızca
ufkun DIŞINDAKİ candle'lar için davranış düzeltildi. İki yeni regresyon
testi eklendi:
`tests/test_feature_engine.py::TestNoLookAheadAdversarial::test_future_open_candle_ignored_without_exception`
ve `test_unfinished_candle_within_horizon_still_rejected`.

---

# FAZ 4 KARARLARI (Quant & Agent Engine, 2026-08-31)

## Karar 46 — ConsensusEngine.combine(): regime keyword-only parametre (raporlanmış sözleşme sapması)

**Date:** 2026-08-31
**Decision:** `ConsensusEngine.combine(evidence: Sequence[AgentEvidence], *,
regime: RegimeContext) -> ConsensusResult` — `regime` keyword-only bir
parametredir.
**Reason:** Faz 4 talimatı `combine()`'ın `RegimeContext` ALMAMASI
gerektiğini açıkça belirtiyordu, ANCAK Faz 1'in ZATEN KABUL EDİLMİŞ
`ConsensusResult` modeli `regime: RegimeContext` alanını ZORUNLU (default'suz)
olarak taşıyor (Karar 12). Bu iki gereksinim birbiriyle DOĞRUDAN çelişiyordu.
"Gerçek kod kazanır" ve "accepted prior-phase kontratını sırf talimat
gereği değiştirme" ilkeleri gereği, `ConsensusResult` DEĞİŞTİRİLMEDİ;
`combine()` yalnızca şemayı doldurmak için `regime`'i keyword-only alır.
**Consequences:** `raw_score`/`agreement` hesaplamasının HİÇBİR ADIMINDA
`regime` kullanılmaz (test: `test_does_not_require_regime_context_influence_
on_score` — aynı evidence farklı regime ile aynı raw_score/agreement
üretir). Bu, mimari niyetin ("regime consensus skorunu ayarlamaz") korunduğu,
yalnızca fonksiyon imzasının pre-existing şemaya uyum sağladığı, dürüstçe
raporlanmış ve test edilmiş bir tasarım kararıdır. Detaylı gerekçe:
PHASE4_QUANT_AGENT_ENGINE.md.

## Karar 47 — RegimeAgentOutput: AgentEvidence değiştirilmeden wrapper

**Date:** 2026-08-31
**Decision:** `RegimeAgent.evaluate()`, `AgentEvidence`'ı DEĞİŞTİRMEDEN,
`RegimeAgentOutput(evidence, regime)` adlı yeni bir Faz 4 wrapper dataclass'ı
döndürür.
**Reason:** RegimeAgent hem directional evidence hem regime sınıflandırması
üretmek zorunda; bu, Phase 1 `AgentEvidence` modelini genişletmeden
(mevcut kontratı bozmadan) sağlanan en az invaziv yoldur.

## Karar 48 — Deterministic context_id: SHA-256, kanonik sabit-sıra string

**Date:** 2026-08-31
**Decision:** `agents/context.py::compute_context_id()`, UUID/random/hash()/
wall-clock KULLANMADAN, sabit sıradaki (schema-version, symbol, context
as_of, 4 snapshot as_of) canonical string'inden SHA-256 ile üretilir.
**Reason:** Faz 4 Bölüm 10'un zorunlu kıldığı determinism (aynı girdi ->
aynı ID, değişen timestamp -> değişen ID) — stdlib-only, denetlenebilir.

## Karar 49 — Agent skorlama: conviction-attenuation her zaman pozitif çarpan

**Date:** 2026-08-31
**Decision:** QuantAgent/MarketStructureAgent/OrderBookAgent'ın hacim/
spread/body-ratio "conviction attenuation" çarpanları HER ZAMAN [0.3, 1.0]
veya [0.5, 1.0] aralığında POZİTİF değerlerdir — asla negatif veya sıfır
olamaz.
**Reason:** Bu, "düşük hacim/geniş spread yönü ASLA tersine çeviremez"
sert gereksinimini (Bölüm 14-17) matematiksel olarak İMKANSIZ-BAŞARISIZLIK
seviyesinde garanti eder (pozitif bir çarpanla çarpmak işareti asla
değiştiremez).

## Karar 50 — Signal nötr-consensus supporting/contradicting politikası

**Date:** 2026-08-31
**Decision:** `raw_score == 0.0` (tam nötr) durumunda, sıfır-olmayan TÜM
evidence "contradicting" listesine atanır (Bölüm 36'nın belirttiği kabul
edilebilir iki politikadan biri, seçilip dokümante edildi).
**Reason:** "Ortak bir yön kazanamadı" yorumu en doğal ve test edilebilir
seçenekti; alternatif (tümünü omit etmek) nötr sinyaller için hiçbir
provenance/açıklama bırakmazdı.

---

# FAZ 5 KARARLARI (Paper Trading / Simulation Engine, 2026-09-01)

## Karar 51 — Signal-only sınır: `ConsensusResult`/`RegimeContext`'e bağımlılık YOK

**Date:** 2026-09-01
**Decision:** `paper_trading/` paketi (`models.py`, `engine.py`), YALNIZCA
Faz 4'ün tamamlanmış `Signal` nesnesini (`domain.models.Signal`) tüketir.
`ConsensusResult`, `RegimeContext`, `agents/*`, `consensus/engine.py`,
`consensus/risk.py` modüllerinin HİÇBİRİ import edilmez veya dolaylı olarak
kullanılmaz.
**Reason:** Faz 5 talimatı açıkça bu sınırı zorunlu kılıyordu ("Phase 5
consumes completed Signal objects only") ve Faz 4'ün `ConsensusEngine.
combine()`'ın `regime`'i keyword-only parametre olarak alması şeklindeki
raporlanmış sözleşme sapmasının (Karar 46/47) DERİNLEŞTİRİLMEMESİNİ
gerektiriyordu.
**Consequences:** Faz 4'ün `regime` sözleşme sapması bu fazda DEĞİŞTİRİLMEDİ,
DÜZELTİLMEDİ VE İYİLEŞTİRİLMEDİ — hâlâ raporlanmış, test edilmiş,
non-blocking bir Faz 4 tech-debt kalemi olarak AYNEN KALIR (bkz.
PROJECT_HANDOFF.md Bölüm 14, PHASE4_QUANT_AGENT_ENGINE.md). Faz 5 bu
sözleşmeyle hiç temas etmediği için ne onu düzeltme iddiasında bulunur ne
de onu derinleştirir. Detaylı gerekçe: PHASE5_PAPER_TRADING.md.

## Karar 52 — Sabit notional-based position sizing (bilinçli kapsam sınırlaması)

**Date:** 2026-09-01
**Decision:** `PaperTradingEngine(notional_per_position: float = 1000.0)`
— her yeni pozisyon açılışında `quantity = notional_per_position /
price.price`. `Signal.confidence`/`risk_level`'a göre ölçeklendirme
YAPILMAZ.
**Reason:** Faz 5 hard constraint'leri position-sizing politikası
belirtmiyordu; deterministic ve test edilebilir en basit seçenek budur.
Kabul sürecinde bu politika AÇIKÇA onaylanmış ve "DEĞİŞTİRİLMEMESİ"
istenmiştir.
**Consequences:** Bu bir tech-debt DEĞİL, bilinçli bir kapsam sınırlamasıdır
— gelecekte bir position-sizing politikası eklenmek istenirse tek bir
yerde (`engine.py::_open_position`) genişletilebilir.

## Karar 53 — NEUTRAL = NO_ACTION (acceptance review BLOCKER düzeltmesi)

**Date:** 2026-09-01
**Decision:** `PaperTradingEngine.process_signal()`, `SignalDirection.
NEUTRAL` için artık ayrı, erken bir `NO_ACTION` dönüşü (`_no_action_result()`)
uygular: hiçbir `PaperOrder`/`PaperFill` üretilmez, açık bir LONG/SHORT
pozisyon KAPATILMAZ, realized/unrealized PnL DEĞİŞMEZ, `self._states`'e
HİÇ dokunulmaz.
**Reason:** İlk implementasyonda NEUTRAL, "hedef pozisyon FLAT" olarak ele
alınıyordu — yani açık bir pozisyon varsa NEUTRAL onu KAPATIYORDU. Bu, bir
serious blocker-focused adversarial acceptance review sırasında BLOCKER
olarak tespit edildi: Faz 5 sözleşmesi NEUTRAL'i "hiçbir aksiyon yok"
olarak tanımlıyordu, "flatten" DEĞİL.
**Consequences:** `_target_side()` artık YALNIZCA LONG/SHORT grubundaki
yönler için çağrılabilir (NEUTRAL için çağrılırsa `AssertionError`, defensive
guard). Eski, hatalı davranışı doğrulayan test
(`test_neutral_flattens_open_position`) kaldırıldı; yerine
`TestNeutralIsNoAction` (flat/long/short + idempotent replay, 4 test)
eklendi. Detaylı gerekçe: PHASE5_PAPER_TRADING.md.

## Karar 54 — Deterministic fee/slippage modeli (acceptance BLOCKER düzeltmesi)

**Date:** 2026-09-01
**Decision:** `PaperTradingEngine(fee_bps: float = 0.0, slippage_bps:
float = 0.0)` — her ikisi de finite VE `>= 0` zorunlu (negatif/NaN/±inf
`ValueError` ile reddedilir). Fill fiyatı yöne göre deterministik olarak
kaydırılır: `BUY fill_price = P*(1+slippage_bps/10000)`, `SELL fill_price
= P*(1-slippage_bps/10000)`. Her `PaperFill.fee = abs(quantity*fill_price)
*fee_bps/10000` — ACTUAL (slippage uygulanmış) `fill_price` üzerinden,
kalıcı olarak `PaperFill`'de taşınır. Tüm entry/exit muhasebesi
(`average_entry_price`, gross/net realized PnL) ACTUAL `fill_price`'ı
kullanır. Net realized PnL, kapanan bacağın giriş VE çıkış fee'sini düşer
(`_SymbolState.entry_fee`, PRIVATE, açılıştan kapanışa kadar saklanır) —
fee'ler yalnızca BİR KEZ, kapanış anında netleştirilir.
**Reason:** Bir acceptance review turunda, spesifikasyonun (`fee_bps`,
`slippage_bps`, yöne göre slippage-uygulanmış fill fiyatı, `fee =
abs(qty*fill_price)*fee_bps/10000`, net realized PnL'de fee netleştirmesi)
Faz 5'te HİÇ İMPLEMENTE EDİLMEMİŞ olduğu tespit edildi ve bir BLOCKER
olarak raporlandı — önceki dokümantasyon bunu yanlışlıkla "bilinçli bir
Faz 5 kapsam sınırlaması" (Faz 6+ scope) olarak sunmuştu; bu bir
dokümantasyon hatası değil, gerçek bir eksik implementasyondu.
**Consequences:** `fee_bps=0.0`/`slippage_bps=0.0` varsayılanları önceki
(fee/slippage'sız) sıfır maliyetli davranışı BİREBİR korur — mevcut hiçbir
test bozulmadı (tüm önceki Faz 5 testleri varsayılan konfigürasyonla hâlâ
geçer). Ayrı bir "cash" alanı EKLENMEDİ: spesifikasyonun `cash_delta`
formülasyonu, bir pozisyonun açılış+kapanış cash_delta'larının toplamının
tam olarak net realized PnL formülüne indirgendiği matematiksel eşdeğerlik
nedeniyle, `realized_pnl` tek kaynak-of-truth olarak yeterlidir (bkz.
PHASE5_PAPER_TRADING.md). NEUTRAL (Karar 53) ve idempotency davranışı
DEĞİŞMEDİ — ikisi de hiçbir yeni `PaperFill` üretmediği için hiçbir fee
UYGULANMAZ/TEKRAR TAHSİL EDİLMEZ. `_target_side()`/`_close_position()`/
`_open_position()` dışında hiçbir mevcut kod yolu değiştirilmedi. Yeni
testler: `TestFeeSlippageConfigValidation` (9), `TestFeeAndSlippage` (12).
Detaylı gerekçe: PHASE5_PAPER_TRADING.md.

---

# FAZ 6 KARARLARI (Real-Time Market Data & Runtime, 2026-09-01)

## Karar 55 — Runtime'ın kendi `CandleWindow`'u: Faz 2'nin private state'ine ayna DEĞİL, tamamlayıcı bir katman

**Date:** 2026-09-01
**Decision:** `runtime/candle_window.py::CandleWindow`, her (symbol,
timeframe) için identity-bazlı (`open_time`) dedup + monoton sıralama +
gap tespiti sağlayan, runtime'a özel, bounded bir yapıdır.
**Reason:** `BinanceMarketDataProvider`'ın kendi `StateManager`'ı (Faz 2)
tamamen private'tır — dışarıdan erişilemez; `FeatureEngine`
(Faz 3) stateless'tir, her çağrıda TAM candle listesi bekler. Runtime'ın
feature hesaplaması için kullanacağı candle geçmişini tutacağı bir yer
OLMAK ZORUNDADIR.
**Consequences:** Bu, Faz 2'nin `CandleSequencer`/`DataQualityGate`'inin
YENİDEN UYGULANMASI DEĞİLDİR — OHLC/kalite doğrulaması burada YAPILMAZ
(zaten Faz 2'de tamamlanmıştır); yalnızca identity-bazlı dedup/sıralama/
gap tespiti eklenir. Detaylı gerekçe: PHASE6_REALTIME_RUNTIME.md.

## Karar 56 — Sinyal üretimi TEK sınır: yalnızca M5 (primary_timeframe) kapanışında

**Date:** 2026-09-01
**Decision:** `RuntimeCoordinator._maybe_evaluate_signal()`, Faz 4
`SignalEngine.evaluate()`'in TEK çağrıldığı yerdir; yalnızca bir M5
candle `ACCEPTED` olarak pencereye girdiğinde tetiklenir. M15/H1/order-book
event'leri feature state'ini günceller ama KENDİ BAŞLARINA sinyal
değerlendirmesi tetiklemez.
**Reason:** Talimat (Bölüm 10) "meaningful completed-data boundary"
gerektiriyordu, her ham paket için sinyal üretimi DEĞİL; Faz 4'ün
`primary_timeframe`'i (M5) doğal ve tek anlamlı tetikleyici sınırdır.
**Consequences:** Referans fiyat (`_reference_price()`), sinyalin BİZZAT
ÜRETİLDİĞİ M5 candle kapanışını kullanır — `price.as_of ==
signal.timestamp` eşitliği YAPISALDIR (tesadüfi değil), bu da Faz 5'in
no-look-ahead invariant'ını (`price.as_of <= signal.timestamp`) HİÇBİR
ZAYIFLATMA olmadan sağlar.

## Karar 57 — Gap tespiti: `CandleWindow`'a opsiyonel `duration` parametresi

**Date:** 2026-09-01
**Decision:** `CandleWindow(maxlen, duration=...)` — `duration` verilirse,
bir sonraki BEKLENEN `open_time`'dan (son kabul edilen + `duration`) DAHA
İLERİ bir `open_time` gelmesi `GAP_DETECTED` olarak SINIFLANDIRILIR (candle
HENÜZ pencereye kabul EDİLMEZ); `RuntimeCoordinator.resolve_gap()` bu
durumda Faz 2'nin `fetch_historical_candles()` REST mekanizmasıyla
YALNIZCA eksik aralığı doldurur.
**Reason:** İlk tasarımda `CandleWindow.offer()`, herhangi bir ileri
`open_time`'ı koşulsuz `ACCEPTED` kabul ediyordu — bu, talimatın (Bölüm 14)
açıkça yasakladığı "never hide a gap by jumping directly to the latest
event" ilkesini ihlal ediyordu (bir reconnect sonrası atlanmış candle'lar
sessizce, hiç doldurulmadan kabul edilmiş olurdu). Bu, implementasyon
sırasında (testler yazılmadan ÖNCE) fark edilip düzeltildi.
**Consequences:** `duration=None` (varsayılan, `RuntimeCoordinator`
DIŞINDAKİ genel kullanım için) gap tespitini KAPATIR — geriye dönük
uyumluluk için. `RuntimeCoordinator`, HER `CandleWindow`'u kendi
timeframe süresiyle oluşturur; gap tespiti runtime'da HER ZAMAN AKTİFTİR.

## Karar 58 — `run()`'daki sessiz hata yutma bulgusu (acceptance review BLOCKER düzeltmesi)

**Date:** 2026-09-01
**Decision:** `_consume_candles()`/`_consume_order_book()`, beklenmeyen
(non-`CancelledError`) bir exception'ı yakalar, İLGİLİ SEMBOLÜ AÇIKÇA
`mark_disconnected()` ile DEGRADED'e geçirir, ardından exception'ı TEKRAR
fırlatır (`raise`). `stop()`, task'ları `asyncio.gather(...,
return_exceptions=True)` ile bekler — bu sonuçları AYRICA incelemez, çünkü
hata ZATEN health-state geçişiyle gözlemlenebilir hâle getirilmiştir.
**Reason:** Bir serious blocker-focused adversarial acceptance review
sırasında BLOCKER olarak tespit edildi: `run()`'ın kendi `asyncio.gather(
*self._tasks, return_exceptions=True)` çağrısı, bir consumer task'ında
oluşan HERHANGİ bir beklenmeyen hatayı (Faz 2'nin kendi reconnect
döngüsünün yakalayamadığı, gerçekten olağandışı bir durum) hiçbir yere
yansıtmadan SESSİZCE yutuyordu — bu, talimatın (Bölüm 28) açıkça aradığı
"swallowed network/runtime errors" bulgu kategorisiyle BİREBİR eşleşiyordu.
**Consequences:** İki yeni regresyon testi eklendi:
`TestReconnect::test_unexpected_stream_error_marks_symbol_degraded_not_silent`
(hatanın health'e yansıdığını doğrular) ve `stop()`'un, ZATEN başarısız
olmuş bir task'ı beklerken kendisinin çökmediğini doğrulayan aynı testin
shutdown bacağı. Repo genelinde henüz bir logging altyapısı OLMADIĞI için
(Faz 8'e kadar kasıtlı olarak ertelenmiş), `status()`/`RuntimeStatus` bu
fazın TEK hata-gözlemlenebilirlik mekanizmasıdır — bu, yeterli ve
dokümante edilmiş bir tasarım kararıdır.

## Karar 59 — Signal-only sınır ve fee/slippage/idempotency: Faz 4/5 DEĞİŞTİRİLMEDİ

**Date:** 2026-09-01
**Decision:** `runtime/` paketi `ConsensusResult`/`RegimeContext`/
`agents/*`/`consensus/*`'e hiçbir bağımlılığı olmadan, Faz 4'ün `Signal`
çıktısını TEK bir yerden (`_maybe_evaluate_signal`) tüketir; Faz 5'in
`PaperTradingEngine`'ini (fee/slippage/idempotency/NEUTRAL=NO_ACTION dahil)
DEĞİŞTİRMEDEN, TEK bir yerden çağırır.
**Reason:** Talimat (Bölüm 3, 16, 17) bunu açıkça zorunlu kılıyordu; Faz
4'ün `regime` keyword-only sözleşme sapması (Karar 46/47) bu fazda
DERİNLEŞTİRİLMEMELİYDİ.
**Consequences:** Faz 4/5 tech-debt'i (regime sapması, sabit notional
sizing) AYNEN KORUNUR — Faz 6 bunlarla hiç temas etmez. Detaylı gerekçe:
PHASE6_REALTIME_RUNTIME.md.

---

# FAZ 7 KARARLARI (Persistence & Recovery, 2026-09-01)

## Karar 60 — Recovery, `PaperTradingEngine`'in private `_states`'ine YAZAN dokümante edilmiş TEK istisna

**Date:** 2026-09-01
**Decision:** `persistence/recovery.py::restore_paper_engine()`, restart
sonrası Faz 5 state'ini `PaperTradingEngine._states`'e DOĞRUDAN yazarak
geri yükler. `PersistedRuntime._checkpoint_paper_transition()`, Faz 5'in
public yüzeyinde OLMAYAN iki skaler alanı (`entry_fee`,
`last_signal_timestamp`) okumak için AYNI şekilde salt-okunur erişir.
**Reason:** Faz 5 KASITLI OLARAK hiçbir "restore/seed state" API'si
SUNMAZ — tüm mutation'lar YALNIZCA `process_signal()` üzerinden geçer.
Restart sonrası state'i `process_signal()` üzerinden "replay" etmek
YANLIŞ olurdu (geçmiş context'leri TEKRAR "yeni" gibi işlemeye çalışmak
— tam olarak önlenmesi gereken şey). Faz 5'e yeni bir public API eklemek
ise "redesign" olurdu (talimat ile YASAK).
**Consequences:** Bu, Faz 5'in hiçbir public sözleşmesini
değiştirmez/genişletmez — dokümante edilmiş, process-restart'a özel,
minimal bir recovery boundary'sidir. Bunun DIŞINDA hiçbir yerde Faz 5/6
private state'ine YAZILMAZ. Detaylı gerekçe: PHASE7_PERSISTENCE_RECOVERY.md.

## Karar 61 — Persistence fault: sembol+REASON bazlı (acceptance review bulgusu)

**Date:** 2026-09-01
**Decision:** `HealthMonitor.mark_persistence_fault(symbol, reason)`/
`clear_persistence_fault(symbol, reason)` — `_persistence_faults:
dict[str, set[str]]` (sembol -> reason kümesi). Yalnızca AYNI (symbol,
reason) için başarılı bir sonraki checkpoint yazımı o reason'ı temizler.
**Reason:** İlk implementasyon düz bir `set[str]` (sembol başına TEK
bayrak) kullanıyordu. Bir serious blocker-focused adversarial acceptance
review sırasında GERÇEK bir bulgu tespit edildi: AYNI `ingest_candle()`
çağrısı içinde candle-checkpoint yazımı BAŞARISIZ olup paper-state
checkpoint yazımı BAŞARILI olabiliyordu — düz bayrak ile, ikincinin
başarısı BİRİNCİNİN hâlâ çözülmemiş hatasını YANLIŞLIKLA MASKELİYORDU
(talimatın "do not let unrelated market activity erase a fault" ilkesinin
dolaylı bir ihlali).
**Consequences:** İki checkpoint türü ("candle_checkpoint",
"paper_state_checkpoint") artık BAĞIMSIZ olarak izlenir; regresyon testleri
eklendi (`tests/test_runtime_health.py::TestPersistenceFault`,
`tests/test_persistence_recovery.py::test_successful_checkpoint_clears_persistence_fault`).
Bu, `RuntimeCoordinator`/`HealthMonitor`'a yapılan tek davranış
düzeltmesidir — `mark_gap_fault`/`clear_gap_fault` (Faz 6, Karar 55)
ZATEN doğru (timeframe-bazlı) izolasyona sahipti, ETKİLENMEDİ.

## Karar 62 — Rebuild edilebilir state persist EDİLMEZ: candle/feature geçmişi REST'ten yeniden inşa edilir

**Date:** 2026-09-01
**Decision:** Durable olarak YALNIZCA sembol+timeframe başına TEK bir
`open_time` (candle checkpoint) persist edilir — `CandleWindow`'un TAM
candle geçmişi veya `FeatureHistoryStore`'un snapshot'ları BLINDLY
serialize EDİLMEZ. Recovery, checkpoint'ten `now`'a kadar olan aralığı
Faz 2'nin PUBLIC REST mekanizmasıyla YENİDEN çeker.
**Reason:** Bu veri, Faz 2/3'ün PUBLIC market data'sından deterministik
olarak TAMAMEN yeniden inşa edilebilir; blindly serialize etmek "do not
persist arbitrary caches simply because they exist" ilkesini ihlal
ederdi VE persisted candle içeriğinin PUBLIC kaynaktan SAPMASI riskini
taşırdı (REST her zaman authoritative'dir).
**Consequences:** Durable store boyutu küçük kalır (yalnızca pozisyon/
context/fill/order/checkpoint); recovery, REST erişimi gerektirir (offline
bir recovery senaryosu — hiç internet olmadan — mümkün DEĞİLDİR, bu
kabul edilebilir bir sınırlamadır, zira Faz 6'nın KENDİSİ de canlı
PUBLIC veri gerektirir). Detaylı gerekçe: PHASE7_PERSISTENCE_RECOVERY.md.

## Karar 63 — Process-lock: POSIX `flock`, PID-dosyası veya distributed lock DEĞİL

**Date:** 2026-09-01
**Decision:** `ops/lock.py::ProcessLock`, aynı durable SQLite store'a
karşı ikinci bir çalışan instance'ı engellemek için `fcntl.flock(fd,
LOCK_EX | LOCK_NB)` kullanır.
**Reason:** Tek-makine bir systemd dağıtımı için distributed lock
(Redis/etcd/vb.) AŞIRI mühendisliktir. Bir PID-dosyası + "process hâlâ
canlı mı" kontrolü ise KENDİ BAŞINA bir race-condition kaynağıdır (PID
yeniden kullanılabilir). `flock`, bir open file description'a bağlıdır —
kilidi tutan process TEMİZ kapansın ya da CRASH etsin FARK ETMEZ,
işletim çekirdeği kilidi OTOMATİK serbest bırakır ("stale lock recovery
güvenli olmalı" gereksinimini KENDİLİĞİNDEN karşılar).
**Consequences:** Offline/deterministic test edilebilir (aynı process
içinde iki ayrı file descriptor açarak, gerçek bir ikinci process spawn
etmeye gerek KALMADAN, bkz. `tests/test_ops_lock.py`). Tek makine dışına
(birden fazla makine aynı SQLite dosyasını paylaşamaz zaten — SQLite
network dosya sistemlerinde güvenli DEĞİLDİR) ölçeklenmez — bu KASITLI
bir sınırlamadır, Faz 8'in hedefi (tek Ubuntu makinesi) için yeterlidir.

## Karar 64 — Health/observability: yerel JSON anlık görüntü + CLI, HTTP endpoint DEĞİL

**Date:** 2026-09-01
**Decision:** `ops/health_snapshot.py`, `RuntimeCoordinator.status()`'u
periyodik olarak yerel bir dosyaya ATOMİK (`os.replace`) yazar;
`python -m crypto_signal_engine.app status` bunu okuyup özetler.
**Reason:** Bir localhost HTTP endpoint'i (talimatın izin verdiği bir
seçenek) ek bir dinleyen port + input-validation/DoS yüzeyi getirir —
"dashboard İNŞA ETME, en küçük tasarımı tercih et" ilkesine göre, hiçbir
ağ soketi AÇMAYAN bir dosya-tabanlı anlık görüntü tercih edilmiştir.
**Consequences:** Sağlık bilgisine erişim, servisin ÇALIŞTIĞI makinede
(veya SSH ile) bir CLI komutu gerektirir — uzaktan bir monitoring
sisteminin doğrudan HTTP scrape etmesi bu tasarımda YOKTUR (ileride,
GERÇEKTEN gerekirse, ayrı bir faz olarak eklenebilir; Faz 8'in kapsamı
bu değildir). Kendi implementasyonu sırasında (dış bir review'dan ÖNCE)
tespit edilen bir tasarım açığı DÜZELTİLDİ: dosya-tabanlı bir yaklaşımın
doğal riski, process `kill -9`/OOM ile ANİ ölürse diskte eski bir
"READY" görüntüsünün KALMASIDIR — `_run_status()` bu yüzden anlık
görüntünün YAŞINI da (`generated_at` vs. şimdi) kontrol eder ve eşik
aşılırsa `STALE SNAPSHOT` ile fail eder (bkz.
`tests/test_app_cli.py::TestStatusStalenessDetection`).

## Karar 65 — `BinanceConfig` REST/WS base URL, Faz 8 config yüzeyinden KASITLI OLARAK override EDİLEMEZ

**Date:** 2026-09-01
**Decision:** `AppConfig.binance_config()`, `rest_base_url`/`ws_base_url`
için Faz 2'nin kendi varsayılanlarını (`https://api.binance.com`,
`wss://stream.binance.com:9443`) KULLANIR — bunlar için hiçbir `CSE_*`
ortam değişkeni TANIMLANMAMIŞTIR.
**Reason:** Defense-in-depth: eğer bu alanlar env-configurable olsaydı,
konfigürasyon yüzeyinin KENDİSİ, sistemi Testnet'e veya başka bir
execution endpoint'ine işaret edecek şekilde YANLIŞLIKLA (veya kötü
niyetle) kötüye kullanılabilecek bir kapı OLURDU — talimatın hard safety
boundary'sini ("Testnet/Mainnet execution YOK") yalnızca kod seviyesinde
değil, KONFİGÜRASYON YÜZEYİ seviyesinde de yapısal olarak korur.
**Consequences:** Bir geliştirici/operatör, farklı bir Binance mirror'ına
bağlanmak isterse KODU değiştirmek ZORUNDADIR (config ile DEĞİL) — bu
KASITLI bir sürtünmedir, kaza ile yanlış endpoint'e bağlanmayı
engellemek içindir.

## Karar 66 — `websockets`, zorunlu değil OPSİYONEL bir bağımlılıktır (`pip install .[runtime]`)

**Date:** 2026-09-01
**Decision:** `pyproject.toml`, `websockets>=12.0`'ı
`[project.optional-dependencies].runtime` altında tanımlar; temel paket
kurulumu (ve TÜM offline test suite'i) bunu GEREKTİRMEZ.
**Reason:** `RealWebSocketConnectionFactory` (Faz 6) zaten lazy-import
edilecek şekilde tasarlanmıştı — bu ilkeyi paketleme seviyesine taşımak,
bir geliştiricinin/CI'ın hiçbir ekstra bağımlılık kurmadan `pip install .`
+ `pytest` çalıştırabilmesini korur; yalnızca GERÇEK bir Ubuntu dağıtımı
(`pip install .[runtime]`) ekstra paketi ister.
**Consequences:** `PHASE8_UBUNTU_OPERATIONS.md`'nin kurulum talimatı
AÇIKÇA `.[runtime]` kullanır; bunu unutmak, `crypto_signal_engine.app
run` çalıştırıldığında (yalnızca gerçekten bir WS bağlantısı kurulmaya
çalışıldığında) açık bir `ImportError` ile fail-fast olur — sessizce
yanlış bir transport'a düşülmez.

## Karar 67 — BLOCKER FİX (Faz 9 soak geliştirmesi sırasında bulundu): `PersistedRuntime`'ın stream-tüketim döngüleri disconnect'i hiç raporlamıyordu

**Date:** 2026-09-02
**Decision:** `persistence/recovery.py::PersistedRuntime._consume_candles()`/
`_consume_order_book()`'a, `RuntimeCoordinator._consume_candles()`/
`_consume_order_book()`'un (Faz 6, DEĞİŞTİRİLMEDEN) zaten sahip olduğu
`except asyncio.CancelledError: raise` / `except Exception:
mark_disconnected(symbol); raise` sarmalayıcısı EKLENDİ.
**Reason:** Faz 9'un soak/stability harness'i GERÇEK `PersistedRuntime.run()`
üzerinden (Faz 8'in `Application` giriş noktasıyla AYNI kod yolu) bir
stream kopması simüle ederken, health'in HİÇBİR ŞEKİLDE `DEGRADED`
olmadığını (`mark_disconnected()` HİÇ ÇAĞRILMIYORDU) tespit etti. Kök
neden: Faz 7, `RuntimeCoordinator`'ın KENDİ `run()`'ını KULLANMAZ (çünkü
checkpoint yazan wrapper metodları — `self.ingest_candle`/
`ingest_order_book` — üzerinden geçmesi gerekir), bu yüzden KENDİ ayrı
`_consume_candles`/`_consume_order_book` döngüsünü yazdı (bkz. `run()`
docstring'i) — ama bu yeniden-yazım sırasında Faz 6'nın disconnect-raporlama
disiplinini SESSİZCE KOPYALAMADI. Sonuç: GERÇEK üretimde (Faz 8) bir
PUBLIC stream koptuğunda, sembol yalnızca staleness eşiği DOLANA KADAR
(varsayılan 30s) yanlışlıkla READY/healthy görünmeye devam ediyordu —
Faz 6'nın kendi "must immediately be observably unhealthy" invariant'ının
(gap-fault'lar için zaten dokümante edilmiş, disconnect'ler için de AYNI
ruhla geçerli olması gereken) sessiz bir ihlali.
**Consequences:** Reprodüksiyon + regresyon testi ÖNCE eklendi
(`tests/test_persistence_recovery.py::TestPersistedRuntimeIntegration::test_stream_disconnect_marks_health_degraded_via_persisted_runtime_run`
— fix'ten ÖNCE FAIL ettiği doğrulandı, fix'ten SONRA PASS), sonra en küçük
mümkün fix uygulandı (Faz 6'nın KENDİ deseninin BİREBİR kopyası — yeni bir
davranış İCAT EDİLMEDİ). Bu, Faz 7'nin "persistence bir sınırdır"
mimarisini DEĞİŞTİRMEZ; yalnızca Faz 7'nin KENDİ döngüsünün Faz 6'nın
ZATEN kabul edilmiş hata-raporlama sözleşmesini DOĞRU şekilde
yansıttığını garanti eder. Tüm Faz 1-8 regresyon suite'i (971 test)
etkilenmeden geçmeye devam eder.

## Karar 68 — BLOCKER FİX (bağımsız Faz 9 acceptance review bulgusu): restart recovery artık checkpoint ETRAFINDA tam warmup lookback'i çeker

**Date:** 2026-09-02
**Decision:** `persistence/recovery.py::PersistedRuntime.recover()`'ın
fetch-penceresi formülü değiştirildi. ÖNCESİ: checkpoint VARSA
`start = checkpoint` (dar `[checkpoint, now)` penceresi); checkpoint
YOKSA `start = now - lookback` (tam warmup penceresi). SONRASI: HER İKİ
durumda da AYNI `lookback = _TIMEFRAME_DURATIONS[timeframe] *
warmup_candles * 2` formülü (Faz 6'nın kendi `bootstrap()`'ıyla AYNI,
YENİ bir sabit İCAT EDİLMEDİ) kullanılır — `anchor = checkpoint ya da
now`, `start = anchor - lookback`. Checkpoint VARKEN de checkpoint'in
KENDİSİ bu aralığa HER ZAMAN dahildir (`checkpoint - lookback <
checkpoint < now`).
**Reason:** Bağımsız acceptance review, önceki dar `[checkpoint, now)`
penceresinin — checkpoint HER ZAMAN "en son işlenen candle" olduğundan —
normal operasyonda GERÇEK PUBLIC geçmiş verisi BOL miktarda MEVCUT olsa
bile neredeyse hiç candle DÖNDÜRMEDİĞİNİ tespit etti: restart, M15/H1
warmup'ını yeniden inşa edemiyor, gereksiz yere BOOTSTRAPPING'de
kalıyordu (saatlerce GERÇEK zamanda yeniden ısınma gerektirerek). Kabul
edilmiş mimari (Decision 62), yeniden inşa edilebilir state'in
authoritative PUBLIC Binance geçmiş verisinden YENİDEN İNŞA EDİLMESİNİ
gerektirir — MEVCUT olduğunda bunu kullanmamak bu ilkenin ihlaliydi;
bu bir BLOCKER olarak ele alındı.
**Consequences:** Reprodüksiyon + regresyon testleri ÖNCE eklendi
(`tests/test_persistence_recovery.py::TestRestartRecoveryRebuildsWarmupHistory`
— fix'ten ÖNCE FAIL ettiği doğrulandı: `candles_applied=1, ready=False`),
sonra en küçük mümkün fix uygulandı (mevcut `warmup_candles*2` formülünün
her iki dala da UYGULANMASI — yeni bir sabit/politika İCAT EDİLMEDİ).
Checkpoint semantiği DEĞİŞMEDİ (hâlâ yalnızca OKUNUR, asla GERİYE
taşınmaz); `bootstrap_candles()` hâlâ ASLA sinyal değerlendirmesi
tetiklemez (state reconstruction, retroactive trading DEĞİL — historical
rebuild sırasında YENİ hiçbir paper order/fill/PnL üretilmediği ayrıca
doğrulandı). PUBLIC kaynak GERÇEKTEN yetersizse (test double'ı kasıtlı
KISITLANMIŞ), sembol DÜRÜSTÇE BOOTSTRAPPING kalır — sahte bir READY
ÜRETİLMEZ (ayrı bir regresyon testiyle doğrulandı). Faz 9'un
`tests/test_stability_scenarios.py::TestProcessRestart` testleri, ESKİ
"restart sonrası BOOTSTRAPPING kabul edilebilir" varsayımını ARTIK
"yeterli PUBLIC geçmiş varken READY beklenir" olarak GÜÇLENDİRECEK
şekilde güncellendi (testler ZAYIFLATILMADI). Tüm Faz 1-9 regresyon
suite'i etkilenmeden geçmeye devam eder.

## Karar 69 — Host allowlist: `urlsplit().hostname` TAM eşleşmesi, substring/prefix/suffix DEĞİL

**Date:** 2026-09-02
**Decision:** `execution/testnet_client.py::validate_testnet_host()`,
bir `base_url`'i yalnızca `urllib.parse.urlsplit(base_url).hostname`'in
(HER ZAMAN küçük harfe çevrilir) `{"testnet.binance.vision"}` kümesiyle
TAM olarak eşleştiği VE scheme'in tam olarak `https` olduğu durumda kabul
eder; ayrıca userinfo (kullanıcı adı/parola) içeren bir `base_url`'i de
reddeder.
**Reason:** Bir substring/prefix/suffix kontrolü (`"testnet.binance.vision"
in base_url` gibi), `https://testnet.binance.vision.attacker.com` (suffix
lookalike), `https://attacker-testnet.binance.vision` (prefix lookalike),
veya `https://testnet.binance.vision@attacker.com` (userinfo trick — GERÇEK
hostname `attacker.com`'dur) gibi TÜM yaygın host-spoofing tekniklerine
KARŞI SAVUNMASIZ olurdu. `urlsplit().hostname` ile TAM eşleşme, bunların
HEPSİNİ yapısal olarak İMKANSIZ kılar.
**Consequences:** `tests/test_execution_testnet_client.py::TestTestnetHostAllowlist`
bu spesifik saldırı sınıflarının HER BİRİNİ ayrı ayrı test eder. Bu
kontrol, `BinanceTestnetConfig.__post_init__()` içinde HER inşa edilişte
ÇALIŞIR — atlanabilir bir "isteğe bağlı" doğrulama DEĞİLDİR.

## Karar 70 — `client_order_id`, constructor argümanı DEĞİL, SHA-256 türetilmiş bir alan

**Date:** 2026-09-02
**Decision:** `OrderIntent.client_order_id` (`field(init=False, default="")`)
constructor'dan ASLA doğrudan atanamaz; `__post_init__` içinde
`symbol|side|order_type|quantity|quote_quantity|price|time_in_force|context_id`
alanlarının SHA-256 özetinden (`csl-` + ilk 24 hex karakter) deterministik
olarak türetilir.
**Reason:** "Aynı intent -> aynı id, farklı intent -> farklı id" garantisi,
çağıranın id'yi DOĞRU hesapladığına GÜVENMEK yerine yapısal olarak
GARANTİ EDİLMELİDİR — bu, bir retry/restart'ın yanlışlıkla YENİ/ilgisiz
bir order kimliği üretmesini İMKANSIZ kılar (Faz 11'in reconciliation
katmanının dayanacağı KİMLİK temeli budur).
**Consequences:** `OrderIntent(..., client_order_id="x")` bir `TypeError`
ile reddedilir (dataclass `init=False` alanı). Binance'in 36-karakter
`newClientOrderId` sınırı bir `assert` ile yapısal olarak garanti edilir.

## Karar 71 — Kimlik bilgileri KASITLI OLARAK opsiyonel (`None`), yalnızca signed çağrılarda zorunlu

**Date:** 2026-09-02
**Decision:** `BinanceTestnetConfig(api_key=None, api_secret=None)`
İNŞA EDİLEBİLİR ve geçerlidir. `MissingCredentialsError`, yalnızca
`account_info()`/`place_order()` gibi SIGNED bir çağrı GERÇEKTEN
yapılmaya çalışıldığında (`_require_credentials()` üzerinden) fırlatılır.
**Reason:** Gerçek Binance API'sinde `exchangeInfo`/`ping`/`server_time`
zaten PUBLIC'tir (imza/API-key gerektirmez) — bu gerçeği YOK SAYIP
TÜM client'ı kimlik bilgisi ZORUNLU kılmak, "kimlik bilgisi yokluğu bir
Faz 10 blocker'ı DEĞİLDİR, doğrulama/dry-run çalışmaya devam eder"
gereksinimini İHLAL EDERDİ.
**Consequences:** `scripts/binance_testnet_lab.py validate-symbol` ve
`place-market`/`place-limit` (onay bayrağı OLMADAN, dry-run) kimlik
bilgisi HİÇ OLMADAN çalışır — yalnızca `account-check` ve GERÇEK gönderim
(`--confirm-testnet-order`) kimlik bilgisi ister. Bu davranış
`tests/test_execution_cli.py`/`test_execution_testnet_client.py`'de
açıkça test edilir.

## Karar 72 — Repository safety scan mimari-farkında hâle getirildi (ZAYIFLATILMADI)

**Date:** 2026-09-02
**Decision:** `tests/test_repository_safety_scan.py::TestPhase2PublicDataOnlyBoundary.test_no_forbidden_execution_or_private_endpoint_strings`,
artık `crypto_signal_engine/execution/`'ı TARAMA KAPSAMI DIŞINDA bırakır
(`_non_execution_source_files()`); yeni `TestPhase10ExecutionBoundarySafety`
sınıfı EKLENDİ — bu, (1) HMAC/credential/private-endpoint izlerinin
YALNIZCA `execution/` içinde kaldığını (repo'nun geri kalanına
SIZMADIĞINI), (2) `execution/` İÇİNDE BİLE futures/margin/withdrawal/
transfer (`/sapi/`, `/fapi/`, `/dapi/`) veya bir Mainnet host literal'inin
ASLA bulunmadığını doğrular.
**Reason:** Faz 10, KASITLI OLARAK bu izleri İZOLE bir pakette TAŞIR —
eski blanket tarama bunu YANLIŞLIKLA bir ihlal olarak raporlardı. Talimat
AÇIKÇA "Do NOT simply delete/weaken safety tests" der — bu yüzden eski
scan SİLİNMEDİ, yalnızca doğru mimari sınıra (execution/ İÇİ vs. DIŞI)
BÖLÜNDÜ; her iki tarafta da SIFIR tolerans KORUNDU.
**Consequences:** Mainnet host literal'i ve CLI host-override bayrağı
kontrolleri, kendi güvenlik-dokümantasyonu metnini (backtick'li prose,
`` `api.binance.com` `` gibi) YANLIŞ POZİTİF SAYMAMAK için gerçek bir
Python string literal'i (tırnak İÇİNDE) arayan bir regex kullanır — bare
substring KULLANILMAZ (bu, dosyanın KENDİ güvenlik açıklamalarını
işaretlerdi).

## Karar 73 — BLOCKER FİX (bağımsız Faz 10 acceptance review bulgusu): MARKET order notional doğrulaması GÜNCEL bir TESTNET PUBLIC fiyatı kullanır

**Date:** 2026-09-02
**Decision:** `TestnetExecutionAdapter.validate_intent()`, bir MARKET
order + base `quantity` + en az bir UYGULANABİLİR (`applyToMarket`/
`applyMinToMarket`/`applyMaxToMarket = true`) notional filtresi varsa,
`place_order()`'dan ÖNCE `BinanceTestnetClient.symbol_price()`
(`/api/v3/ticker/price`, TESTNET, PUBLIC-eşdeğeri) ile GÜNCEL bir fiyat
çeker ve bunu notional TABANI (`quantity * price`) olarak kullanır.
`SymbolFilters` artık `MARKET_LOT_SIZE`'ı ve hem eski `MIN_NOTIONAL`
(`applyToMarket`) hem yeni `NOTIONAL` (`applyMinToMarket`/
`applyMaxToMarket`, min/max BAĞIMSIZ) filtrelerini AYRI AYRI ayrıştırır.
**Reason:** `OrderIntent.price`, MARKET order'lar için YAPISAL OLARAK
HER ZAMAN `None`'dur (bkz. `models.py` — MARKET price KABUL ETMEZ). Eski
implementasyon notional kontrolünü `intent.price is not None`
koşuluyla KAPILIYORDU — bu MARKET için ASLA sağlanmadığından, MIN_NOTIONAL/
NOTIONAL MARKET order'lar için SESSİZCE HİÇ ÇALIŞTIRILMIYORDU (testler
bunu YAKALAMAMIŞTI çünkü mevcut notional testi YALNIZCA LIMIT
kullanıyordu). Bu, "hiçbir quantity/price sessizce farklı bir ekonomik
anlama yuvarlanmaz" ilkesinin dolaylı ama GERÇEK bir ihlaliydi — bir
notional kontrolü SESSİZCE hiç ÇALIŞMAMAK, yuvarlamaktan DAHA KÖTÜDÜR.
**Consequences:** Reprodüksiyon + regresyon testleri ÖNCE eklendi (41
yeni/güncellenmiş test, `tests/test_execution_adapter.py` — MARKET BUY/
SELL min-notional reddi, quoteOrderQty'nin doğrudan kullanımı,
`applyToMarket=false` durumunda kontrolün HİÇ tetiklenmemesi,
`NOTIONAL`'ın `applyMinToMarket`/`applyMaxToMarket` semantiği,
`MARKET_LOT_SIZE`, ve fiyat-lookup'ının 6 farklı BAŞARISIZLIK modunun
(transport/timeout/malformed/yanlış-sembol/sıfır/negatif/sonsuz-olmayan)
HEPSİNİN `place_order()`'a ULAŞMADAN engellendiğinin kanıtı). Fiyat
KASITLI OLARAK İCAT EDİLMEZ, LIMIT price'ı ÖDÜNÇ ALINMAZ, ve bir fiyat-
lookup hatası ASLA "kontrolü atla ve gönder"e DÖNÜŞMEZ (fail-closed). Bu,
Binance'in KENDİ execution price'ını/`avgPriceMins` penceresini TAHMİN
ETMEZ — yalnızca gönderim ÖNCESİ bir filtre-doğrulama TABANIDIR, bir
fill-price GARANTİSİ DEĞİLDİR (dokümantasyonda AÇIKÇA belirtilmiştir,
bkz. PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md Bölüm 7). LIMIT davranışı
DEĞİŞMEDİ (hâlâ yalnızca `intent.price` kullanır, hiçbir price-lookup
gerektirmez — regresyon testiyle doğrulandı). `scripts/binance_testnet_lab.py`,
kendi ayrı `validate_intent_against_filters()` çağrısını KALDIRDI ve
`adapter.validate_intent()`'i kullanacak şekilde güncellendi — dry-run
çıktısı da artık MARKET notional doğrulamasını DOĞRU yansıtır (kod
tekrarı ORTADAN KALDIRILDI, tek doğruluk kaynağı adapter'dadır).

## Karar 74 — Ambiguous submission: KÖRÜ KÖRÜNE yeniden gönderim YERİNE stabil-kimlik sorgusu

**Date:** 2026-09-02
**Decision:** `ExecutionReconciliationService.submit()`, `POST
/api/v3/order` bir timeout/bağlantı-kopması (`ExecutionTransportError`,
`ExecutionTimeoutError`'ı GENİŞLETİR) ile başarısız olursa, kaydı
`AMBIGUOUS`'a taşır ve ANINDA AYNI `client_order_id` ile `GET
/api/v3/order (origClientOrderId=...)` sorgular — YENİ bir kimlikle KÖRÜ
KÖRÜNE yeniden POST YAPMAZ.
**Reason:** Bir timeout/bağlantı kopması, Binance'in order'ı GERÇEKTEN
ALIP ALMADIĞINI YAPISAL OLARAK BİLİNMEZ KILAR. Yeni bir kimlikle kör
yeniden gönderim, order GERÇEKTEN oluşturulmuşsa DUPLICATE bir order
ÜRETİR — bu, execution safety'nin en temel ihlalidir. Deterministik
`client_order_id` (Faz 10, Karar 70) tam olarak BU sorguyu MÜMKÜN kılmak
için VARDI; Faz 11 onu GERÇEKTEN kullanır.
**Consequences:** `tests/test_execution_reconciliation_service.py::TestAmbiguousSubmission`
tüm dallanmaları test eder: exchange order'ı BULURSA reconcile edilir;
BULAMAZSA (`-2013`) `UNKNOWN_NOT_FOUND`'a taşınır; reconciliation
sorgusunun KENDİSİ başarısız olursa kayıt `AMBIGUOUS` KALIR (bir sonraki
denemeye bırakılır). Hiçbir senaryoda ikinci bir bağımsız order-kimliği
ÜRETİLMEZ.
**DÜZELTME (Karar 78 — bağımsız acceptance review bulgusu, bu Karar'ın
ORİJİNAL metni yanlıştı):** yukarıdaki "`UNKNOWN_NOT_FOUND`'a taşınır
(yeniden gönderim o zaman GÜVENLİDİR)" ifadesi HATALIYDI ve KALDIRILDI.
`-2013` yanıtı, "exchange sorgusu BU denemede order'ı BULAMADI" ANLAMINA
GELİR — "orijinal ambiguous POST'un Binance tarafından HİÇ kabul
edilmediği KANITLANDI" ANLAMINA GELMEZ. Tek bir anlık -2013 yanıtı,
GERÇEKTEN Binance'e ulaşmış ama henüz tam olarak işlenmemiş/gecikmeli
görünür hale gelmiş bir order'ı DIŞLAMAZ. `UNKNOWN_NOT_FOUND`, otomatik
resubmission'ı ASLA TETİKLEMEZ — bkz. Karar 78 (bu belgede aşağıda), tam
düzeltilmiş politika ve gerekçe için.

## Karar 75 — `context_id` PRIMARY KEY: Faz 5'in idempotency ilkesinin execution'a taşınması

**Date:** 2026-09-02
**Decision:** `ExecutionStateStore`'da `context_id`, `execution_record`
tablosunun PRIMARY KEY'idir (`client_order_id` ayrıca UNIQUE'tir).
`submit()`, AYNI `context_id` FARKLI bir `client_order_id` (yani FARKLI
ekonomi) ile tekrar kullanılırsa `ExecutionIdempotencyConflictError`
fırlatır.
**Reason:** Faz 5'in `PaperTradingEngine`'i AYNI ilkeyi zaten kanıtlamıştı
(`Signal.context_id` + `IdempotencyConflictError`) — Faz 11 bunu YENİDEN
İCAT ETMEDİ, AYNI deseni execution kimliğine UYGULADI. Bu, "aynı execution
intent/aynı context identity HER ZAMAN aynı client_order_id'ye çözülmeli,
asla ikinci bağımsız bir order YARATMAMALI" gereksinimini yapısal olarak
garanti eder.
**Consequences:** `tests/test_execution_reconciliation_service.py::TestDuplicateSuppression`
hem "aynı intent replay -> tek POST" hem "çakışan aynı context_id farklı
ekonomi -> AÇIK hata, POST YOK" durumlarını doğrular.

## Karar 76 — Persistence-önce-submission: fail-closed, dağıtık transaction TAKLİDİ YOK

**Date:** 2026-09-02
**Decision:** `submit()`, `POST /api/v3/order`'dan **ÖNCE** kaydı
`SUBMISSION_ATTEMPTED` durumunda persist eder. Bu ÖN-yazı BAŞARISIZ
olursa, order KESİNLİKLE GÖNDERİLMEZ (`ExecutionPersistenceError`,
fail-closed). POST **SONRASI** persistence başarısız olursa, hata mesajı
`client_order_id`/`exchange_order_id`'yi AÇIKÇA içerir — ama "dağıtık
transaction" TAKLİT EDİLMEZ (Binance + yerel SQLite arasında GERÇEK bir
2-phase-commit YOKTUR ve OLAMAZ).
**Reason:** Talimat AÇIKÇA "Do not invent a distributed transaction" der.
Tehlikeli split ("exchange kabul etti AMA yerel persistence başarısız")
dürüstçe bir "surface et + kimliği koru" stratejisiyle ele alınır — SESSİZCE
gizlenmez. Submission-ÖNCESİ yazı BAŞARILI olduğu sürece (yaygın durum),
stabil kimlik HER ZAMAN durable'dır — bir sonraki `reconcile_pending()`
süpürmesi POST-sonrası kaybı GERİ KAZANABİLİR.
**Consequences:** `tests/test_execution_reconciliation_service.py::TestPersistenceFailureHandling`
üç senaryoyu ayrı ayrı doğrular: submission-ÖNCESİ hata (POST hiç
YAPILMADI), submission-SONRASI hata (POST YAPILDI, hata AÇIKÇA
fırlatıldı), ve kurtarılabilirlik (submission-ÖNCESİ satır, SONRAKİ hataya
RAĞMEN hayatta kalır — stabil kimlik KAYBOLMAZ).

## Karar 77 — `reconcile()`'ın "kayıt bulunamadı" hatası bir `ExecutionError` alt sınıfı olmalı (kendi geliştirme sürecinde bulunan blocker)

**Date:** 2026-09-02
**Decision:** `ExecutionReconciliationService.reconcile()`, bilinmeyen bir
`context_id`/`client_order_id` için artık `LocalExecutionRecordNotFoundError`
(YENİ, `ExecutionError` alt sınıfı) fırlatır — önceki taslak ham bir
`ValueError` fırlatıyordu.
**Reason:** Faz 11'in KENDİ CLI testleri geliştirilirken (dış bir review
DEĞİL, kendi test suite'i YAZILIRKEN) tespit edildi: `scripts/binance_testnet_lab.py`'nin
`run_cli_async()`'ı yalnızca `except ExecutionError` yakalar — ham bir
`ValueError`, CLI'nin KENDİSİNİ ÇÖKERTİR (temiz bir "ERROR: ..." mesajı
YERİNE ham bir Python traceback'i kullanıcıya SIZDIRIR). Bu, projenin
"hiçbir hata sessizce/çirkin bir şekilde SIZMAZ" ilkesinin bir ihlaliydi.
**Consequences:** Regresyon testi hem service-seviyesinde
(`test_execution_reconciliation_service.py::TestReconcileUnknownIdentity`)
hem CLI-seviyesinde (`test_execution_cli.py::TestReconcileCommand::test_reconcile_unknown_identity_fails_cleanly_not_a_crash`)
eklendi. "İki tanımlayıcı da verilmedi" (programlama hatası, CLI'nin
`argparse` mutually-exclusive-required grubu tarafından ZATEN engellenir)
durumu KASITLI olarak `ValueError` olarak BIRAKILDI — bu GERÇEK bir API
yanlış kullanımı sinyalidir, normal bir runtime durumu DEĞİLDİR.

## Karar 78 — BLOCKER FİX (bağımsız acceptance review bulgusu): `UNKNOWN_NOT_FOUND`, ASLA otomatik yeniden gönderime YOL AÇMAZ

**Date:** 2026-09-02
**Decision:** `ExecutionLifecycleState.UNKNOWN_NOT_FOUND`,
`NEEDS_RECONCILIATION_STATES` kümesine EKLENDİ (önceden bu kümede
DEĞİLDİ). `ExecutionReconciliationService.submit()`'in eski dallanma
mantığı — AYNI `context_id` için mevcut bir `UNKNOWN_NOT_FOUND` kaydı
bulunduğunda bunu "yeniden gönderim GÜVENLİ" sayıp TAZE bir
`place_order()` POST'u YAPAN kod yolu — TAMAMEN KALDIRILDI. Artık AYNI
`context_id` için TERMİNAL OLMAYAN herhangi bir mevcut kayıt
(`SUBMISSION_ATTEMPTED`, `AMBIGUOUS`, `ACKNOWLEDGED`, `PARTIALLY_FILLED`,
`UNKNOWN_NOT_FOUND` DAHİL), `submit()` içinde SADECE `_reconcile_record()`
üzerinden YENİDEN SORGULANIR — bu fonksiyonda `place_order()`'a giden
İKİNCİ bir kod yolu YOKTUR.
**Reason:** Bağımsız acceptance review, önceki Karar 74'ün "exchange
order'ı BULAMAZSA (`-2013`), `UNKNOWN_NOT_FOUND`'a taşınır — yeniden
gönderim o zaman GÜVENLİDİR" ifadesinin YANLIŞ olduğunu tespit etti: tek
bir anlık -2013 yanıtı, orijinal ambiguous POST'un Binance tarafından
HİÇ kabul edilmediğini KANITLAMAZ (sadece "bu denemede bulunamadı" demektir
— gecikmeli görünürlük, geçici tutarlılık gecikmesi, ya da sorgunun
KENDİSİNİN geçici bir aksaklığı, hepsi "order aslında var ama şu an
görünmüyor" senaryolarını AÇIK bırakır). Faz 11'in amacı execution
safety'dir — "muhtemelen güvenli" bir varsayımla otomatik ikinci bir POST
üretmek, execution safety'nin TEMEL ihlalidir. Doğru güvenlik invariant'ı:
"şu anda bulunamadı" ≠ "hiç kabul edilmediği kanıtlandı." Sistem, olası
bir duplicate order yerine takılı kalmış/çözülmemiş bir TESTNET execution
kaydını TERCİH ETMELİDİR (fail closed).
**Consequences:**
- `UNKNOWN_NOT_FOUND`'un anlamı KESİN OLARAK değişti: "exchange sorgusu şu
  anki reconciliation denemesinde order'ı bulamadı; orijinal submission
  sonucu HÂLÂ kanıtlanmamış bir şekilde bilinmiyor" — ASLA "yeniden
  gönderim güvenlidir" DEĞİL.
- `submit(intent)`'in AYNI `context_id`/`client_order_id` ile TEKRAR
  çağrılması (kayıt `UNKNOWN_NOT_FOUND` iken), exchange'i YENİDEN sorgular
  ama ASLA yeni bir POST ÜRETMEZ. Sorgu HÂLÂ -2013 dönerse kayıt
  `UNKNOWN_NOT_FOUND` olarak KALIR.
- `reconcile_pending()`, `UNKNOWN_NOT_FOUND` kayıtlarını SÜPÜRMEYE devam
  eder (bu durum `NEEDS_RECONCILIATION_STATES`'te olduğu için) — restart
  sonrası bu kayıtlar YENİDEN sorgulanır, ASLA yeniden POST edilmez.
- Sonraki bir reconciliation denemesi exchange'de order'ı GERÇEKTEN
  bulursa (örn. gecikmeli görünürlük çözüldüğünde), kayıt AYNI
  `client_order_id` üzerinden gerçek exchange truth'una (`FILLED`,
  `ACKNOWLEDGED`, vb.) reconcile edilir — TOPLAM POST sayısı HER ZAMAN
  BİR olarak KALIR.
- Bir sembol için çözülmemiş bir `UNKNOWN_NOT_FOUND` kaydı, `reconcile_pending()`'in
  BAŞKA sembollerin kayıtlarını reconcile etmesini ENGELLEMEZ — her kayıt
  KENDİ `client_order_id`'si ile BAĞIMSIZ sorgulanır.
- Operatör-onaylı, açıkça tasarlanmış bir gelecekteki yeniden gönderim
  politikası (örn. "N başarısız reconciliation denemesinden sonra, açık
  bir `--force-resubmit` bayrağıyla") KASITLI OLARAK Faz 11'in kapsamı
  DIŞINDA bırakıldı — bu Faz, hiçbir OTOMATİK resubmission YAPMAZ.
- Regresyon testleri: `tests/test_execution_reconciliation_models.py::TestStateSetMembership::test_needs_reconciliation_excludes_terminal_but_includes_unknown_not_found`;
  `tests/test_execution_reconciliation_service.py::TestAmbiguousSubmission::test_unknown_not_found_never_allows_automatic_resubmission`,
  `::test_later_reconciliation_finds_original_order_after_unknown_not_found`;
  `tests/test_execution_reconciliation_service.py::TestRestartRecovery::test_restart_with_unknown_not_found_is_swept_by_reconcile_pending_no_post`,
  `::test_restart_with_unknown_not_found_later_resolves_to_filled_no_duplicate`;
  `tests/test_execution_reconciliation_service.py::TestIsolation::test_unresolved_unknown_not_found_on_one_symbol_does_not_block_another`;
  `tests/test_execution_cli.py::TestReconcileCommand::test_reconcile_pending_requeries_unknown_not_found_without_new_post`,
  `::test_replay_confirmed_order_after_unknown_not_found_produces_no_second_post`.
  Eski (artık geçersiz varsayımı test eden) `test_unknown_not_found_allows_safe_resubmission`
  KALDIRILDI.

## Karar 79 — Faz 12: TESTNET execution kurulumu `execution/factory.py`'ye taşındı (`app.py`'nin KENDİSİ execution/ sınırının DIŞINDA kalmalı)

**Date:** 2026-09-02
**Decision:** `crypto_signal_engine/app.py::build_execution_service()`
(Faz 12), `BinanceTestnetConfig`/`BinanceTestnetClient`'ı DOĞRUDAN İNŞA
ETMEZ — bunun yerine YENİ `crypto_signal_engine/execution/factory.py::
build_testnet_execution_service()`'e delege eder. `app.py`, YALNIZCA
"TESTNET etkinleştirilmeli mi?" KARARINI verir (`ExecutionMode` +
`enable_testnet_execution` kontrolü); GERÇEK kimlik bilgisi okuma/
imzalama/servis inşası `execution/` paketinin İÇİNDE KALIR.
**Reason:** İlk taslak, `app.py` içinde doğrudan `os.environ.get("BINANCE_TESTNET_API_SECRET")`
çağırıyordu — bu, `tests/test_repository_safety_scan.py::
TestPhase10ExecutionBoundarySafety::test_execution_only_patterns_never_leak_outside_execution_package`
tarafından DERHAL yakalandı: bu tarama, Faz 10'dan beri `place_order(`/
`api_secret`/`hmac.new`/`X-MBX-APIKEY`/`/api/v3/order` pattern'lerinin
`execution/` paketinin DIŞINDA (kod VEYA docstring/comment FARK ETMEZ)
SIFIR olmasını ZORUNLU KILAR — bu, Faz 10'un "private execution İZOLE
bir sınırda kalır" mimari invariant'ının otomatik doğrulamasıdır. Faz
12, bu invariant'ı ZAYIFLATMADI/istisna EKLEMEDİ — bunun yerine kompozisyon
kodunu (`app.py`) invariant'a UYACAK şekilde YENİDEN DÜZENLEDİ.
**Consequences:** `app.py` artık `BinanceTestnetConfig` sınıf adını
(yalnızca `doctor`'ın host-doğrulama kontrolü için, credential-literal
İÇERMEDEN) import eder, ama `api_key=`/`api_secret=` keyword'lerini veya
`place_order(` string'ini HİÇBİR YERDE İÇERMEZ. `ops/config.py` ve
`ops/health_snapshot.py`'nin docstring'lerindeki "api_secret" kelimesi
de AYNI nedenle "kimlik bilgisi"/"imza kimlik bilgileri" ifadeleriyle
YENİDEN yazıldı — bu tarama yalnızca GERÇEK kod DEĞİL, docstring/comment
metnini de tarar (kasıtlı: bir yorum bile yanlışlıkla "buraya credential
mantığı eklemek normaldir" izlenimi VERMEMELİDİR). Doğrulama:
`tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`
(tekrar PASS), `tests/test_phase12_local_production_readiness.py`
(TESTNET gating/reconciliation davranışı DEĞİŞMEDEN doğrulanmaya devam
eder).

## Karar 80 — Faz 12: otomatik sürekli Signal->TESTNET execution entegrasyonu KASITLI OLARAK ERTELENDİ (blocker olarak raporlandı, force edilmedi)

**Date:** 2026-09-02
**Decision:** Faz 12, `RuntimeCoordinator`'ın (Faz 6) her sinyal
değerlendirmesinde OTOMATİK olarak `ExecutionReconciliationService.
submit()`'i çağıran bir entegrasyon İÇERMEZ. Bunun yerine: (1) execution-
mode gating (`CSE_EXECUTION_MODE`/`CSE_ENABLE_TESTNET_EXECUTION`), (2)
startup'ta bekleyen kayıtların reconciliation'ı, (3) doctor/dashboard
görünürlüğü implemente edildi — otomatik order-gönderim kod yolu
EKLENMEDİ.
**Reason:** `RuntimeCoordinator._maybe_evaluate_signal()` senkron bir
kod yoludur (Faz 6, KABUL EDİLMİŞ); `ExecutionReconciliationService.
submit()` asenkron ağ I/O yapar (Faz 11, KABUL EDİLMİŞ). Bu ikisini
GÜVENLE köprülemek — runtime'ı BLOKE ETMEDEN, signal-context idempotency
ile execution-context idempotency'yi DOĞRU eşleyerek, submit() gecikmesi/
hatası candle-consumption loop'unu ETKİLEMEDEN, restart sırasında
"hangi sinyaller ZATEN execution'a gönderildi" sorusunu YANITLAYARAK —
BAŞLI BAŞINA bir mimari tasarım kararı GEREKTİRİR (yeni bir kuyruk/
event modeli, ya da coordinator'ın KENDİSİNİN asenkronlaştırılması).
Bu, görev tanımının AÇIKÇA yasakladığı bir "redesign"dir ("Do not
redesign accepted Phase 1-11 architecture. Fix blockers only."). Görev
tanımı AYRICA açık bir kaçış sağlar: "If implementing this integration
would require unsafe architectural shortcuts, do NOT force it. Report
it as a blocker instead." Bu karar, TAM OLARAK o kaçışın KULLANILMASIDIR
— sahte/yarım bir entegrasyon (örn. `asyncio.create_task(service.submit(...))`
ile "ateşle ve unut," hata izleme/idempotency garantisi OLMADAN) ZORLA
EKLENMEDİ.
**Consequences:** Stage B (Faz 12 sonrası operasyon planı, bkz. PHASE12
doc) TESTNET gözlemini `scripts/binance_testnet_lab.py`'nin manuel
CLI'sı üzerinden yapar — bu ZATEN Faz 10/11'de kabul edilmiş, test
edilmiş bir yoldur, YENİ bir risk YARATMAZ. Otomatik entegrasyon,
gelecekte (bu projenin AÇIKLANMIŞ mevcut fazlarının DIŞINDA, "there is
NO Phase 13" ifadesiyle tutarlı bir şekilde bu belgede tekrar
DEĞERLENDİRİLMEDEN) yalnızca özel bir tasarım turu ile ele ALINABİLİR.
Bu Faz'ın acceptance kriteri, bu ertelemenin KENDİSİNİN blocker olarak
AÇIKÇA raporlanmasıdır — sessizce atlanmadı.

## Karar 81 — BLOCKER FİX (bağımsız acceptance review bulgusu): `execution_ready`, kimlik bilgisi eksikken bekleyen bir signed çağrıya GÜVENEMEZ

**Date:** 2026-09-02
**Decision:** `execution/factory.py`'ye YENİ bir `testnet_credentials_status()`
fonksiyonu eklendi — `(api_key_present, api_secret_present)` bool çifti
döner, DEĞERLERİ ASLA. `app.py::Application._reconcile_execution_startup()`,
`ExecutionReconciliationService.reconcile_pending()`'i çağırmadan ÖNCE
bu fonksiyonu çağırır; İKİSİ de `True` DEĞİLSE `reconcile_pending()` HİÇ
ÇAĞRILMAZ (sıfır ağ çağrısı) ve `execution_ready` fail-closed `False`
KALIR.
**Reason:** Önceki tasarımda (Faz 12'nin ilk kabul turu), kimlik bilgisi
eksikliği YALNIZCA `reconcile_pending()`'in İÇİNDE, bir signed
`GET /api/v3/order` çağrısı GERÇEKTEN denendiğinde (`MissingCredentialsError`
fırlatılarak) keşfediliyordu. Ama `reconcile_pending()`, bekleyen
(`NEEDS_RECONCILIATION_STATES`'teki) HİÇBİR kayıt YOKSA — ki bu, TESTNET
execution'ın İLK KEZ etkinleştirildiği, hiçbir önceki CLI submission'ının
OLMADIĞI normal bir başlangıç durumudur — boş bir liste üzerinde SIFIR
ağ çağrısı yaparak "başarıyla" tamamlanır. Bu durumda kimlik bilgisi HİÇ
OLMASA BİLE reconciliation "başarılı" sayılıyor ve `execution_ready=True`
oluyordu — bağımsız review'ın tespit ettiği TAM OLARAK budur: "TESTNET
mode + explicit enable=true can potentially reach execution_ready=True
when credentials are missing IF there are zero pending execution
records." Gerekli invariant AÇIKÇA: bu durum "bekleyen bir signed API
çağrısına" VEYA "operatörün `doctor` çalıştırmış olmasına" GÜVENEMEZ —
kontrol HER `start()` çağrısında OTOMATİK ve KOŞULSUZ çalışmalıdır.
**Consequences:**
- Davranış matrisi (görev tanımının "Preferred behavior" listesiyle
  BİREBİR): PAPER -> değişmedi (sıfır kimlik bilgisi gerekir); TESTNET +
  enable=false -> değişmedi (execution service hiç yok); TESTNET +
  enable=true + key eksik -> execution disabled/fail-closed (YENİ); aynı
  + secret eksik -> disabled/fail-closed (YENİ); aynı + HER İKİSİ VAR ->
  YALNIZCA O ZAMAN startup reconciliation çalışır; `execution_ready=True`
  YALNIZCA credential-preflight BAŞARILI + reconciliation BAŞARILI iken
  mümkündür.
- `doctor` komutunun ÖNCEDEN kendi başına yaptığı `os.environ.get(...)`
  kontrolü, AYNI `testnet_credentials_status()` fonksiyonuna delege
  edecek şekilde YENİDEN DÜZENLENDİ — tek bir doğruluk kaynağı (DRY),
  `doctor` ile `Application.start()`'ın kimlik-bilgisi-varlık kontrolü
  ASLA birbirinden SAPAMAZ.
- Mimari sınır KORUNDU (Karar 79 ile AYNI ilke): `testnet_credentials_status()`
  `execution/` paketinin İÇİNDEDİR — `app.py`, ortam değişkeni ADINI bile
  DOĞRUDAN OKUMAZ, yalnızca bool çiftini TÜKETİR. `tests/
  test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`
  DEĞİŞTİRİLMEDEN PASS eder.
- Regresyon testleri: `tests/test_phase12_local_production_readiness.py::TestCredentialPreflightBlockerFix`
  — boş DB + kimlik bilgisi YOK/yalnızca key/yalnızca secret -> HER
  ZAMAN `execution_ready=False`, SIFIR ağ çağrısı; boş DB + kimlik
  bilgisi TAM -> `execution_ready=True`; bekleyen bir kayıt VARKEN bile
  eksik kimlik bilgisi -> SIFIR ağ çağrısı; kimlik bilgisi TAM olduğunda
  `UNKNOWN_NOT_FOUND` reconciliation davranışı (Karar 78) DEĞİŞMEDİ;
  kimlik bilgisi DEĞERİ `_execution_detail`'e (log/dashboard'a) ASLA
  SIZMAZ.

---

# PRE-AUDIT ENHANCEMENT PASS (Faz 12 ÜZERİNE, Faz 13 DEĞİL)

Aşağıdaki kararlar (82-89), `phase12-accepted` (commit `33dc6cd`) kabul
edilmiş temeli DEĞİŞTİRMEDEN, final bağımsız adversarial audit ÖNCESİNDE
eklenen bir araştırma/doğrulama katmanına aittir. Detaylı mimari,
sınırlamalar ve gerekçeler için bkz. **PRE_AUDIT_ENHANCEMENTS.md**. Bu
BİR Faz 13 DEĞİLDİR — proje için PLANLANMIŞ bir Faz 13 YOKTUR (Faz 12
doc'un kendi ifadesiyle tutarlı).

## Karar 82 — Pre-Audit Enhancement Pass: ayrı bir üst-seviye `research/` paketi, `crypto_signal_engine/` SINIRI DIŞINDA

**Date:** 2026-09-02
**Decision:** Tüm yeni araştırma/doğrulama kodu (`data_quality.py`,
`replay.py`, `oos_stability.py`, `attribution.py`, `monte_carlo.py`,
`sizing.py`, `guardrails.py`, `orderbook_capture.py`, `drift.py`) yeni,
bağımsız bir üst-seviye `research/` paketinde yaşar — `scripts/` ve
kabul edilmiş `crypto_signal_engine/stability/` (Faz 9 soak harness) ile
AYNI mimari duruşta: `crypto_signal_engine/` paketinin İÇİNDE DEĞİL,
onun PUBLIC sözleşmelerini dışarıdan TÜKETİR.
**Reason:** Görev tanımının "prefer additive components that reuse
accepted contracts" ve "do not rewrite accepted Phase 1-12 architecture"
kısıtları, yeni kodun kabul edilmiş paketin İÇİNE karışmaması ile en
güvenli şekilde sağlanır. `pyproject.toml`'daki `[tool.setuptools.
packages.find] include = ["crypto_signal_engine*"]` bu yeni paketi
otomatik olarak PAKETLEMEZ (kasıtlı — `scripts/`/`tests/` gibi, pip
dağıtımının bir parçası değildir, yalnızca repo-içi bir geliştirme/
araştırma aracıdır).
**Consequences:** `tests/test_repository_safety_scan.py` (kabul edilmiş)
`research/`'ü TARAMAZ (kapsamı yalnızca `crypto_signal_engine/` +
`scripts/binance_testnet_lab.py`'dir) — bu yüzden YENİ, AYRI bir
`tests/test_repository_safety_scan_research.py` eklendi (Karar 89'a
bkz.), kabul edilmiş dosya SIFIR diff ile bırakıldı.

## Karar 83 — Deterministic historical replay, `RuntimeCoordinator.bootstrap_candles()`/`ingest_candle()`/`ingest_order_book()`'u DEĞİŞTİRMEDEN yeniden kullanır — ikinci bir strateji YOK

**Date:** 2026-09-02
**Decision:** `research/replay.py::HistoricalReplayDriver`, her `run()`
çağrısında TAZE bir `RuntimeCoordinator` + `PaperTradingEngine` +
`FeatureHistoryStore` inşa eder ve YALNIZCA bu üçünün PUBLIC, senkron,
ZATEN kabul edilmiş metodlarını çağırır — `FeatureEngine`/`SignalEngine`/
agent/consensus/risk/paper-accounting mantığının TEK BİR SATIRI bile
yeniden yazılmadı.
**Reason:** `RuntimeCoordinator.ingest_candle()`, canlı async `run()`
döngüsünün HER candle event'i için çağırdığı TAM OLARAK AYNI metoddur
(bkz. `coordinator.py` modül docstring'i) — bu yüzden onu doğrudan
çağırmak, "ikinci bir strateji implementasyonu" riskini YAPISAL OLARAK
ORTADAN KALDIRIR (iki kod yolu senkronize TUTULMASI gereken bir şey
değildir — TEK bir kod yolu vardır). Faz 9'un `SoakHarness`'ı ZATEN bu
AYNI deseni (senkron `ingest_candle()`/`ingest_order_book()` çağrısı,
`run()`'ın KENDİSİ DEĞİL) sentetik veriyle kanıtlamış, kabul edilmiş bir
mimari emsaldir; bu Faz yalnızca veri kaynağını (sentetik -> gerçek
`fetch_historical_candles()`) değiştirir.
**Consequences:** `RuntimeCoordinator`/`FeatureEngine`/`SignalEngine`/
`PaperTradingEngine` kaynak dosyalarının HİÇBİRİ bu Faz'da DEĞİŞTİRİLMEDİ
(bkz. Karar 86/87 — YALNIZCA iki KÜÇÜK, additive, varsayılan-davranış-
korur touch: `PaperTradingEngine`'e `notional_override`/`has_processed`,
`RuntimeCoordinator`'a opsiyonel `order_book_observer`).

## Karar 84 — Multi-timeframe replay sıralaması: close_time'a göre gruplama, sabit tie-break [H1, M15, ORDER_BOOK, M5]

**Date:** 2026-09-02
**Decision:** `research/replay.py::build_deterministic_event_plan()`,
aynı `close_time`'a sahip event'leri TEK bir grup olarak ele alır ve
grup İÇİNDE SABİT bir uygulama sırası dayatır: H1 (varsa) -> M15 (varsa)
-> sentetik order-book -> M5. Gruplar KENDİLERİ, close_time'a göre KESİN
ARTAN sırayla uygulanır.
**Reason:** Binance kline sınırları epoch-hizalıdır: her M15 kapanışı
bir M5 kapanışıyla, her H1 kapanışı hem M15 hem M5 kapanışıyla ÇAKIŞIR
(60 % 15 == 0, 15 % 5 == 0) — bu yüzden "aynı ana ait" event'leri
tanımlamak matematiksel olarak sağlamdır. Bu ana ait bir H1/M15 candle,
GERÇEKTEN o ana kadar gerçekleşmiş bilgidir (gelecekten GELMEZ) — bu
yüzden onu AYNI ana ait M5 değerlendirmesinden ÖNCE uygulamak look-ahead
İHLALİ DEĞİLDİR; tam tersine, tie-break'i input-listesi sırasına (rastgele
bir Python implementasyon detayına) BIRAKMAK, replay sonucunun kendi
kodunun bir yapıtına bağımlı olmasına yol açardı — bu KASITLI OLARAK
ÖNLENDİ. Sentetik order-book event'i M5'ten HEMEN ÖNCE konumlandırılır
çünkü `OrderBookAgent` kanıtı HER değerlendirme için ZORUNLUDUR (Faz 4).
**Consequences:** `tests/test_research_replay.py::
TestDeterministicEventPlanOrdering` bu politikayı doğrudan test eder
(sabit sıra, dict key-insertion-order'dan BAĞIMSIZLIK, close_time'a göre
KESİN monotonluk); `build_deterministic_event_plan()` KENDİSİ,
monotonluk ihlali durumunda (should-never-happen) `ReplayIntegrityError`
fırlatan bir defansif guard içerir — sessizce devam ETMEZ.

## Karar 85 — Sentetik order-book: `ReplayResult.order_book_provenance` HER ZAMAN `SYNTHETIC` — asla "live parity" iddia edilmez

**Date:** 2026-09-02
**Decision:** Binance public REST'in geçmişe dönük bir order-book/depth
endpoint'i YOKTUR. `research/replay.py`, her M5 kapanışı İÇİN, o
candle'ın KENDİ OHLC'sinden deterministik olarak türetilmiş bir sentetik
order-book snapshot'ı üretir (`synthetic_order_book_from_candle()`) ve
`ReplayResult.order_book_provenance` alanını DAİMA `OrderBookProvenance.
SYNTHETIC` olarak işaretler; `ReplayResult.LIVE_EQUIVALENT` sabiti DAİMA
`False`'tur. `limitations` alanı bu durumu AÇIKÇA, HER replay çıktısında
taşır.
**Reason:** `OrderBookAgent`, Faz 4 consensus'una MADDİ OLARAK katkıda
bulunur — sentetik kanıtla üretilen bir sinyal, GERÇEK order-book
koşullarının üreteceği sinyalle AYNI DEĞİLDİR. Bunu gizlemek veya
"tam live-parity" gibi göstermek, görev tanımının AÇIKÇA yasakladığı bir
şeydir ("DO NOT claim historical candle-only replay is fully live-
equivalent... never label it 'full live parity'"). `OrderBookProvenance`
enum'u `REAL_RECORDED`/`UNAVAILABLE` üyelerini de içerir (Karar 88/89 ile
ilişkili gelecekteki gerçek kayıt entegrasyonu için ileriye dönük
uyumluluk) ama bu Faz'da SADECE `SYNTHETIC` üretilir.
**Consequences:** `scripts/historical_replay.py`/`oos_stability.py`/
`monte_carlo_robustness.py`'nin HİÇBİRİ "PROFITABLE = READY FOR LIVE
TRADING" gibi bir mesaj YAZDIRMAZ; her biri limitasyonu AÇIKÇA
yazdırır.

## Karar 86 — `PaperTradingEngine`'e İKİ küçük, additive, varsayılan-davranış-koruyan touch: `notional_override` + `has_processed`

**Date:** 2026-09-02
**Decision:** `process_signal(signal, price)`'a keyword-only, opsiyonel
`notional_override: float | None = None` eklendi (varsayılan `None` ->
davranış BİREBİR AYNI, `self._notional_per_position` kullanılır); ayrıca
salt-okunur, YENİ `has_processed(symbol, context_id) -> bool` accessor'ı
eklendi (`position()`/`orders()`/`fills()` ile AYNI desen).
**Reason:** Görev tanımı (Bölüm I) bunu AÇIKÇA öngörür: "A safe approach
may require a small additive Phase 5 API extension such as an optional
per-decision notional override." Portfolio/risk-bazlı boyutlandırma
(Karar 88), `PaperTradingEngine.process_signal`'in KENDİSİNE erişmeden
uygulanamaz — sizing, Signal ile PaperTradingEngine arasında KESİNLİKLE
downstream bir katman olmak ZORUNDADIR (Bölüm I), ve bu iki nokta TEK
güvenli, backward-compatible giriş noktasıdır. `has_processed()`,
`research/sizing.py::SizedPaperTradingEngine`'in idempotent bir replay'i
sizing politikasını TEKRAR ÇALIŞTIRMADAN doğrudan inner engine'e
yönlendirebilmesi için gereklidir (aksi halde her replay, guardrail
durumunu yeniden değerlendirip TUTARSIZ bir karar üretebilirdi).
**Consequences:** `tests/test_paper_trading_engine.py` (kabul edilmiş,
55 test) DEĞİŞTİRİLMEDEN, SIFIR diff ile, TAMAMEN YEŞİL kalır — YENİ,
AYRI `tests/test_paper_trading_notional_override.py` eklendi. Idempotency
davranışı KORUNDU: `notional_override`, `process_signal`'in idempotency
kontrolünden SONRA tüketilir — aynı `context_id`'nin FARKLI bir
`notional_override` ile tekrar sunulması İKİNCİ bir tahsis ÜRETMEZ
(cached sonuç aynen döner, override YOK SAYILIR).

## Karar 87 — `RuntimeCoordinator`'a opsiyonel, defansif-sarmalı `order_book_observer` hook'u (Faz 12 canlı çalıştırma için order-book kaydı)

**Date:** 2026-09-02
**Decision:** `RuntimeCoordinator.__init__`'e keyword-only, opsiyonel
`order_book_observer: Callable[[str, FeatureSnapshot], None] | None =
None` eklendi. `None` (varsayılan) iken SIFIR davranış değişikliği.
Verildiğinde, `ingest_order_book()` İÇİNDE — order-book `FeatureSnapshot`
BAŞARIYLA commit edildikten SONRA — çağrılır; çağrı `try/except
Exception` ile SARILIR, bir observer hatası `HealthMonitor.
mark_persistence_fault(reason="order_book_observer")`'a yansıtılır
(sessizce YUTULMAZ) ama ASLA `ingest_order_book()`'un dönüş değerini/
davranışını DEĞİŞTİRMEZ.
**Reason:** Görev tanımının Bölüm E'si ("Future Real Order-Book Research
Capture"), gelecekteki çok-haftalık 24/7 PAPER çalıştırması İÇİN gerçek
order-book kanıtının BİRİKTİRİLMESİNİ AÇIKÇA ister — bu, Faz 4'ün
`OrderBookAgent`'ının TAM OLARAK TÜKETTİĞİ anda (`FeatureSnapshot`
commit) bir gözlem noktası GEREKTİRİR; bu nokta YALNIZCA
`RuntimeCoordinator`'ın İÇİNDEDİR (Faz 6, private). "Recorder failure
must not corrupt trading state" gereksinimi, hook'un İKİ KATMANLI
savunmasıyla sağlanır: (1) `research/orderbook_capture.py::
OrderBookEvidenceRecorder.record()`'un KENDİSİ hiçbir zaman fırlatmaz
(broad-ama-sınıflandırılmış `except Exception`, `errors.py`'nin KENDİ
"lifecycle boundary" istisnasına uygun), (2) hook'un çağrı sitesi bunu
BAĞIMSIZ OLARAK TEKRAR sarar — herhangi bir GELECEKTEKİ observer
implementasyonu için de aynı garanti geçerli olsun diye.
**Consequences:** `tests/test_runtime_coordinator.py` (kabul edilmiş, 84
test dahil daha geniş runtime/stability/persistence/app süiti) SIFIR diff
ile TAMAMEN YEŞİL kalır (bkz. bu Faz'ın doğrulama koşusu). YENİ
`tests/test_research_orderbook_capture.py`, KIRIK bir observer'ın
GERÇEK bir `RuntimeCoordinator` üzerinde `ingest_order_book()` sonucunu
DEĞİŞTİRMEDİĞİNİ doğrudan kanıtlar.

## Karar 88 — Portfolio/risk-bazlı PAPER sizing: `SizedPaperTradingEngine` duck-typed sarmalayıcı, `RuntimeCoordinator`'a SIFIR ek dokunuş

**Date:** 2026-09-02
**Decision:** `research/sizing.py::SizedPaperTradingEngine`,
`PortfolioRiskSizingPolicy` (+ `research/guardrails.py::
evaluate_guardrails`) ile GERÇEK bir `PaperTradingEngine`'i sarar ve
`RuntimeCoordinator`'ın ZATEN kabul edilmiş, opsiyonel `paper_engine`
constructor parametresine (duck-typing ile, `isinstance` kontrolü
OLMADAN) doğrudan geçirilebilir bir `process_signal(signal, price) ->
PaperTradingResult` yüzeyi sunar.
**Reason:** Sizing'i `Signal` ile `PaperTradingEngine.process_signal`
çağrısı ARASINA (Bölüm I'in istediği TAM konum) yerleştirmenin TEK
mümkün noktası, `RuntimeCoordinator._maybe_evaluate_signal()`'ın
İÇİDİR — ama bu private, kabul edilmiş bir metoddur. Bir sarmalayıcı
paper-engine (RuntimeCoordinator'ın KENDİSİ hiç DEĞİŞMEDEN) bu sorunu
mimari olarak ÇÖZER: `RuntimeCoordinator` kime "process_signal"
çağırdığını BİLMEZ/UMURSAMAZ, `paper_engine` parametresi ZATEN duck-typed
bir sözleşmedir (bkz. yorum, `paper_engine: PaperTradingEngine | None =
None`, hiçbir yerde `isinstance` kontrolü YOK).
**Consequences (DÜRÜST, BELGELİ SINIRLAMA):** `SizingOutcome.DENIED` bir
kararı, `RuntimeCoordinator`/`RuntimeCycleResult` seviyesinde bir
DIRECTIONAL TRADE'İ BASTIRAMAZ — `PaperTradingEngine`'in "bu Signal'i
atla" sözleşmesi YOKTUR. `SizedPaperTradingEngine`, bir DENIED kararında
inner engine'i HİÇ ÇAĞIRMAZ ve `PaperTradingEngine._no_action_result` ile
AYNI ŞEKİLDE sentezlenmiş bir NO_ACTION sonucu döner (sipariş/fill YOK,
pozisyon DEĞİŞMEZ) — bu, `RuntimeCoordinator` üzerinden GERÇEK, canlı bir
Signal akışına bağlandığında AYNI davranışı üretir (coordinator sonucu
olduğu gibi kabul eder), bu yüzden PRATİKTE ÇALIŞIR; ama bu, Faz 5'e
"bu Signal'i atla" birinci-sınıf bir kavram EKLEMEDEN elde edilen bir
SONUÇTUR, bir TASARIM DEĞİLDİR — DECISIONS.md'nin gelecekteki bir okuru
bunu "denial otomatik olarak coordinator seviyesinde de doğru çalışıyor,
ama coordinator'ın KENDİSİ bunu 'anlamıyor'" şeklinde okumalıdır. Var
olan `notional_override`/`has_processed` DIŞINDA `PaperTradingEngine`'e
HİÇBİR üçüncü dokunuş YAPILMADI (Karar 86 ile sınırlı kalındı) — panic-
flatten YOK, otomatik Testnet bağlantısı YOK (Bölüm L KORUNDU).

## Karar 89 — Signal -> PortfolioTarget/RiskBudget -> ExecutionIntent ara sözleşmesi: KASITLI OLARAK EKLENMEDİ (erken soyutlama)

**Date:** 2026-09-02
**Decision:** Bu Faz, `Signal`/`PortfolioTarget`/`ExecutionIntent`
adında YENİ, biçimsel bir domain sözleşme katmanı EKLEMEDİ. `research/
sizing.py::SizingDecision` (Signal'in downstream'i) ile Faz 10/11'in
`OrderIntent`'i (TAMAMEN AYRI, manuel-CLI-tetiklemeli) arasında HİÇBİR
otomatik köprü YOKTUR.
**Reason:** Bugün `Signal`'in TEK tüketicisi `PaperTradingEngine.
process_signal`'dir; TESTNET execution yolu (`OrderIntent`) HÂLÂ
YALNIZCA `scripts/binance_testnet_lab.py`'nin manuel CLI'sı üzerinden
tetiklenir (Faz 10/11/12, DEĞİŞMEDEN) — otomatik Signal->TESTNET köprüsü
Faz 12'nin KENDİSİ tarafından KASITLI OLARAK ertelenmiştir (bkz. Karar
80: "büyük başlı başına bir mimari tasarım kararı gerektirir"). Bugün var
olmayan bir otomatik köprü İÇİN biçimsel bir ara sözleşme tasarlamak,
o köprünün NASIL çalışacağına dair varsayımlarda bulunmayı GEREKTİRİR —
yanlış tahmin edilirse yeniden tasarlanması gerekir. Bu, klasik bir
erken/gereksiz soyutlamadır.
**Consequences:** Bu ara katman erteleniyor; YALNIZCA otomatik Signal->
TESTNET execution köprüsü KASITLI OLARAK planlandığında (kendi özel
tasarım turuyla, Karar 80'in ima ettiği gibi) yeniden değerlendirilmelidir.
`research/drift.py`, bu köprü OLMADAN bile faydalı olan minimal, uyumlu
provenance yapıları (`SignalProvenanceRecord`) hazırlar — bu, ara
sözleşmenin bir ön-versiyonu DEĞİLDİR, yalnızca gelecekteki bir replay-
vs-paper karşılaştırması için bir veri şeklidir.

## Karar 90 — BLOCKER FİX (bağımsız acceptance review bulgusu): `notional_override` doğrulaması idempotency kontrolünden ÖNCE çalışıyordu — artık YALNIZCA gerçek bir açılış anında çalışır

**Date:** 2026-09-02
**Decision:** `PaperTradingEngine.process_signal()`'daki `notional_override`
doğrulaması (`require_finite` + `> 0`), fonksiyonun BAŞINDAN kaldırılıp
`_open_position()`'ın GERÇEKTEN çağrılacağı TEK noktaya (`target_side is
not PositionSide.FLAT` bloğunun İÇİNE) taşındı.
**Reason:** Karar 86, `notional_override`'ın idempotent bir replay'de
"HER ZAMAN önce idempotency kontrolünden geçip TAMAMEN YOK SAYILDIĞINI"
iddia ediyordu — ama GERÇEK implementasyon bu doğrulamayı fonksiyonun
BAŞINDA, idempotency kontrolüne ULAŞMADAN ÖNCE çalıştırıyordu. Bağımsız
acceptance review'ın tespit ettiği TAM OLARAK budur: zaten işlenmiş bir
`(symbol, context_id)`'nin `0`/`NaN`/`inf`/başka bir geçersiz
`notional_override` ile tekrar sunulması, cached sonucu döndürmesi
GEREKİRKEN `ValueError` fırlatıyordu — idempotent bir replay'in sonucu,
İLGİSİZ OLMASI GEREKEN bir parametreye bağımlı hale geliyordu. Doğrulamayı
`_open_position()`'ın TEK çağrı noktasına taşımak, idempotency/conflict/
NEUTRAL/aynı-yön-tekrarı kontrollerinin TAMAMININ `notional_override`
DEĞERİNE HİÇ BAKMADAN ÖNCE tamamlanmasını YAPISAL OLARAK garanti eder —
doğrulama yalnızca GERÇEKTEN yeni bir pozisyon açılışı (brand-new context
İÇİN İLK işleme VEYA bir reversal'ın reopen bacağı) gerektiğinde çalışır.
**Consequences:** `tests/test_paper_trading_notional_override.py`'ye 7
YENİ regresyon testi eklendi (zaten işlenmiş context + sıfır/NaN/inf/
negatif/farklı-geçerli override → HER ZAMAN cached sonuç, HİÇBİR yeni
order/fill/fee/allocation; brand-new context + inf → hâlâ fail-closed;
conflicting replay + geçersiz override → hâlâ `IdempotencyConflictError`,
sessizce cached sonuca DÜŞMEZ). `tests/test_paper_trading_engine.py`
(kabul edilmiş, 55 test) VE `tests/test_research_sizing.py` (18 test,
`SizedPaperTradingEngine` — yalnızca GEÇERLİ override'lar kullanır, bu
düzeltmeden ETKİLENMEZ) SIFIR diff ile TAMAMEN YEŞİL kalır. Karar 86'nın
İLGİLİ cümlesi ("idempotent replay HER ZAMAN önce kendi kontrolünden
geçer ve bu parametreyi TAMAMEN YOK SAYAR") artık DOĞRU/DOĞRULANMIŞ bir
ifadedir — DAHA ÖNCE niyet edilen ama YANLIŞ implemente edilmiş bir
davranıştı, şimdi implementasyon niyetle EŞLEŞİR.

## Karar 91 — BLOCKER FİX (bağımsız acceptance review, İKİNCİ tur): brand-new aynı-yön tekrarı `notional_override`'ı hiç doğrulamadan geçebiliyordu

**Date:** 2026-09-02
**Decision:** `notional_override` doğrulaması, Karar 90'ın taşıdığı yerden
(`_open_position()`'ın TEK çağrı noktası) BİR KEZ DAHA taşındı — artık
idempotency/conflict çözümlemesi TAMAMLANDIKTAN HEMEN SONRA, `target_side`
hesaplanmadan/`if target_side != current_side:` dallanmasından ÖNCE,
KOŞULSUZ olarak çalışır. `_open_position()`'ın çağrı sitesindeki YİNELENEN
doğrulama KALDIRILDI (artık gereksiz — bu noktaya ulaşıldığında değer ZATEN
doğrulanmıştır).
**Reason:** Karar 90, idempotent-replay/conflict uç durumlarını DOĞRU
çözdü ama YENİ bir kalan uç durumu KAÇIRDI: `target_side == current_side`
olan (yani mevcut pozisyonla AYNI yönde) brand-new bir context, `else`
dalına (aynı-yön tekrarı — HİÇBİR yeni order/fill üretmeyen, yalnızca
`updated_at` yenileyen dal) girer ve `_open_position()`'a HİÇ ULAŞMAZ —
dolayısıyla Karar 90'ın validasyonu bu duruma HİÇ ÇALIŞMIYORDU. Bağımsız
review'ın tespit ettiği TAM OLARAK budur: mevcut bir BTCUSDT LONG
pozisyonu varken, YENİ bir `context_id` ile gelen BAŞKA bir LONG sinyali,
`notional_override=0`/`NaN`/`inf`/negatif ile sunulsa bile SESSİZCE kabul
ediliyordu (çünkü bu değer o çağrıda zaten hiç KULLANILMAYACAKTI) — ama
kabul edilen sözleşme, bir çağıranın (örn. `research/sizing.py`'nin
sizing politikası bir hata sonucu bozuk bir değer üretirse) GEÇERSİZ bir
`notional_override` ile YENİ bir sinyal sunmasının, o sinyalin NİHAİ
sonucundan (açılış/reversal/aynı-yön-NO_ACTION) BAĞIMSIZ olarak HER ZAMAN
tutarlı bir şekilde reddedilmesini GEREKTİRİR — validasyonun sonucun bir
YAN ETKİSİ (o özel çağrının portföy durumuna göre override'ın KULLANILIP
KULLANILMAYACAĞI) olması SÜRPRİZ ve TUTARSIZ bir davranıştır.
**Consequences:** `tests/test_paper_trading_notional_override.py`'ye
17 YENİ regresyon testi eklendi (`TestBrandNewContextAlwaysValidatesRegardlessOfEventualOutcome`)
— aynı-yön tekrarı + geçersiz override (0/NaN/inf/negatif, parametrized)
→ `ValueError`, HİÇBİR mutasyon YOK, `has_processed` hâlâ `False`;
aynı-yön tekrarı + GEÇERLİ override → hâlâ NO_ACTION semantiği (override
KULLANILMAZ ama artık en azından DOĞRULANIR); reversal + geçersiz
override → `ValueError`, mevcut pozisyon TAMAMEN el değmemiş kalır
(kısmi close YOK); zaten-işlenmiş replay + geçersiz override → HÂLÂ
cached sonuç (Karar 90'ın garantisi KORUNDU); conflicting replay +
geçersiz override → HÂLÂ `IdempotencyConflictError` (Karar 90'ın
garantisi KORUNDU); override YOK → hem aynı-yön hem reversal hâlâ sabit-
notional varsayılan davranışı üretir. `tests/test_paper_trading_engine.py`
(kabul edilmiş, 55 test) VE `tests/test_research_sizing.py` (18 test)
SIFIR diff ile TAMAMEN YEŞİL kalır (`SizedPaperTradingEngine` yalnızca
GEÇERLİ override'lar ürettiği için bu değişiklikten hiç ETKİLENMEZ).
Faz 4 (`agents/`/`consensus/`), sizing politikası anlamı (`research/
sizing.py`/`guardrails.py`), ve persistence/execution/runtime mimarisi
BU DÜZELTMEDE DE HİÇ DOKUNULMADI (`git diff --stat` bu paketler için
SIFIR).

## Karar 92 — BLOCKER FİX: `attempt_exit()` MIN_NOTIONAL'ın altında kalan bir kalıntı için doomed bir SELL göndermeye devam ediyordu

**Date:** 2026-09-05
**Decision:** `LifecycleManager.attempt_exit()`'e, mevcut LOT_SIZE/DUST
kontrolünden HEMEN SONRA ve `self._service.submit()`'e ULAŞMADAN ÖNCE bir
MIN_NOTIONAL ön-kontrolü (pre-flight check) eklendi. `filters.min_notional`
VARSA VE `filters.min_notional_applies_to_market` `True` İSE: (a)
`attempt_exit()`'e YENİ eklenen opsiyonel `current_price` parametresi
VERİLMİŞSE onu kullanır — `evaluate_m1_candle()` KENDİ tetikleyici M1
mumunun `close`'unu buraya geçirir (sıfır ek ağ maliyeti, bu fiyat ZATEN
elde mevcuttur); (b) `current_price` YOKSA (yalnızca `signal_bridge.py`'nin
ters-sinyal çıkış yolu — elde bir mum YOKTUR) `self._client.symbol_price()`
ile TEK bir taze fiyat çeker. `prospective_notional = price * sizing.
quantity` eşiğin ALTINDAYSA: `self._service.submit()` HİÇ ÇAĞRILMAZ,
`ExitOutcome(action="NO_ACTION", detail="blocked by MIN_NOTIONAL
pre-check: ...", exit_reason=reason)` döner, `position.state` KASITLI
OLARAK `LONG` kalır (ASLA `DUST`'a geçmez). Fiyat lookup'ı BAŞARISIZ
olursa dosyanın MEVCUT fail-closed kuralıyla (filtre-lookup hatasıyla
AYNI desen) aynı şekilde: order GÖNDERİLMEZ, `NO_ACTION` döner.
`evaluate_m1_candle()`'ın kendi çağrı sitesi bu yeni `current_price`
parametresini geçirecek şekilde güncellendi.
**Reason:** LOT_SIZE tabanlı DUST kontrolü (quantity KALICI olarak
satılamaz — asla kendiliğinden değişemez) DOĞRU ve DEĞİŞTİRİLMEDİ. Ama
`sizing.quantity > 0` (LOT_SIZE'ı GEÇEN) bir kalıntı için HİÇBİR
MIN_NOTIONAL ön-kontrolü YOKTU: `attempt_exit()` doğrudan `self._service.
submit(intent)`'e ulaşıyor, `submit()`'in KENDİ `validate_intent()`'i
(exchangeInfo + `symbol_price()` — İKİ ek network round-trip) notional'ı
eşiğin altında bulup `FilterValidationError` fırlatıyor (gerçek log
kanıtı: `"ARBUSDT notional 0.0130700... minimum 5.0 altında"` — bu, TAM
OLARAK `adapter.py::_check_notional`'ın ürettiği metindir, yani Binance'e
GERÇEK bir POST asla ULAŞMIYORDU ama İKİ GEREKSİZ GET'i HER SEFERİNDE
harcanıyordu), `attempt_exit()`'in `except ExecutionError` dalı bunu
yakalayıp `NO_ACTION` dönüyordu — AMA bu, aynı doomed denemenin M5 sinyal
her ters yöne döndüğünde (`ARBUSDT`/`HEMIUSDT`/`PUMPUSDT`/`TRUMPUSDT`
gibi gerçek Testnet pozisyonlarında canlı olarak GÖZLEMLENDİ, 5 dakikada
bir tekrarlanan `"SELL submission did not confirm"` log gürültüsü olarak)
SONSUZA KADAR TEKRARLANMASI anlamına geliyordu — gerçek exchange API/order
budget'ı boşa harcanıyordu. `DUST`'a geçirmek YANLIŞ olurdu:
`evaluate_m1_candle()`'ın `state is not LONG: return None` kapısı DUST'ı
TEK YÖNLÜ, kalıcı bir durum yapar (bkz. `lifecycle.py`), ama
`notional = price * quantity` fiyat geri yükseldiğinde KENDİLİĞİNDEN
tekrar satılabilir hâle GELEBİLİR — DUST'a geçirmek, ileride gerçek parayla
kapanabilecek bir pozisyonu KALICI OLARAK terk ederdi.
**Consequences:** `tests/test_execution_lifecycle_manager.py`'ye
`TestMinNotionalPreflight` sınıfı altında 4 yeni test eklendi: eşiğin
altında kalan bir kalıntının `NO_ACTION` döndüğünü, `submit()`'in HİÇ
çağrılmadığını ve pozisyonun `LONG` kaldığını kanıtlayan test; AYNI
pozisyonun, fiyat DAHA SONRA yükseldiğinde (ikinci `attempt_exit()`
çağrısı) gerçek bir SELL'in GÖNDERİLİP `FLAT`'e sonuçlandığını kanıtlayan
kurtarma (recovery) testi — pozisyonun ASLA kalıcı olarak kilitlenmediğini
gösterir; MIN_NOTIONAL filtresi VARKEN notional'ın eşiği AÇIKÇA temizlediği
durumda davranışın Karar-öncesi "full sell" yoluyla BİREBİR aynı kaldığını
kanıtlayan regresyon-yok testi; `current_price` verilmediğinde VE
`symbol_price()` lookup'ının BAŞARISIZ olduğunda fail-closed'ın (submit
YOK, `NO_ACTION`, `LONG` DEĞİŞMEDEN) gerçekleştiğini kanıtlayan test.
`TestEvaluateM1Candle`'a, M1 mumunun `close`'unun GERÇEKTEN kullanıldığını
(HİÇBİR ek `symbol_price()` GET'i OLMADIĞINI, tek GET'in `validate_symbol`
olduğunu sayarak) kanıtlayan bir test daha eklendi. Tam paket bu
düzeltmeden SONRA 1722 test ile TAMAMEN YEŞİL kaldı (önceki taban 1717 +
5 yeni test). Canlı Testnet runtime'ı temiz bir restart ile doğrulandı:
ARBUSDT/HEMIUSDT/PUMPUSDT/TRUMPUSDT restart sonrası hâlâ `LONG` (asla
`DUST`), ve PUMPUSDT için doğal olarak oluşan bir WEAK_SHORT sinyalinde
`/api/status` üzerinden `"blocked by MIN_NOTIONAL pre-check: ..."`
detayı GERÇEK piyasa verisiyle GÖZLEMLENDİ, eski tarz
`"SELL submission did not confirm"` satırı bir DAHA ÜRETİLMEDİ. Sinyal/
strateji mantığı, `testnet_bridge_notional_usdt`, risk parametreleri VE
`autonomous-testnet-trading-accepted` etiketi BU DÜZELTMEDE HİÇ
DOKUNULMADI/TAŞINMADI.

## Karar 93 — Adaptive Intelligence v1: otomatik `ExitPolicyConfig` ayarlama alt-sistemi eklendi

**Date:** 2026-09-05
**Decision:** Repo köküne, `research/`'ün kardeşi olarak, yeni bir
`adaptive/` paketi eklendi. Bu paket YALNIZCA `crypto_signal_engine.
execution.lifecycle.ExitPolicyConfig`'in 5 alanını (stop/take-profit/
trailing-activation/trailing-distance ATR çarpanları, max_hold_hours)
otomatik ayarlar — `RiskPolicyConfig`, `ConsensusEngine` ağırlıkları ve
RiskOverlay eşikleri BU MİLESTONE'DA ASLA değiştirilmez, hiçbir versiyonda.
`crypto_signal_engine/` PAKETİ `adaptive/`'ı ASLA import ETMEZ — tek
istisna, HER İKİ paketi de import eden ayrı bir giriş noktası
(`scripts/run_with_adaptive_policy.py`), `Application`'ın ZATEN sahip
olduğu tiplerden (`Callable[[], tuple[ExitPolicyConfig, str | None]]` vb.)
somut callable'lar inşa eder.

EN KRİTİK yapısal düzeltme (Karar 2'nin devamı, aynı milestone'un ilk
adımı): `LifecycleManager.evaluate_m1_candle()` DAHA ÖNCE her M1 mumunda
`self._exit_policy`'yi TAZE okuyordu — pozisyonun NE ZAMAN açıldığından
BAĞIMSIZ. `BridgePositionRecord`'a 6 yeni EKLEMELİ (additive), `None`
varsayılanlı alan eklendi (5 gömülü `ExitPolicyConfig` değeri +
`policy_version_id`) ve `resolved_exit_policy(default)` metodu: pozisyonun
KENDİ gömülü politikasını, TÜM 5 alan doluysa döndürür, aksi halde
(eski/legacy pozisyonlar) `default`'a düşer. `evaluate_m1_candle()` artık
`self._exit_policy` yerine `position.resolved_exit_policy(default=self.
_exit_policy)` kullanıyor — AÇIK BİR POZİSYON, champion değiştiğinde
ASLA retroaktif olarak etkilenmez (position-pinning invariant). `on_entry_
filled()` VE `lifecycle_migration.py`'nin stage-2 finalizasyonu (ikisi de
gerçek "giriş anı") YENİ opsiyonel `exit_policy_provider` callable'ı
üzerinden politika çözer ve gömer. `lifecycle_store.py`'ye idempotent
`ALTER TABLE` migrasyonları eklendi (`bridge_position` + `bridge_
completed_trade`).

Challenger üretimi (`adaptive/challenger.py`): her `ExitPolicyConfig`
alanı BAĞIMSIZ olarak ±%15 bandında (dokümante edilmiş: ATR ile çarpımsal
ilişki nedeniyle anlamlı bir fark yaratacak kadar büyük, ama champion'ın
tek adımda niteliksel olarak farklı bir risk duruşuna asla SIÇRAYAMAYACAĞI
kadar küçük) pertürbe edilir, `ExitPolicyConfig`'in kendi pozitiflik
kuralından DAHA SIKI ek makul sınırlara (`stop_atr_multiple ∈ [0.5, 6.0]`
vb.) clamp edilir. Yerel `random.Random(seed)` — global `random` state'e
ASLA dokunulmaz (`research.monte_carlo`'nun kendi disipliniyle aynı).

Kanıt eşikleri (`adaptive/decision.py`, üç AYRI kanıt sınıfı — discovery/
confirmation/shadow — ASLA karıştırılmaz/ortalanmaz): `MIN_DISCOVERY_
TRADES=30` (discovery penceresi ucuz ve serbestçe tekrar kullanılabilir,
büyük örneklem talep etmek düşük maliyetli), `MIN_CONFIRMATION_TRADES=10`
(confirmation penceresi TEK KULLANIMLIK ve genelde çok daha kısa —
30 talep etmek neredeyse hiçbir challenger'ın hayatta kalmamasına yol
açardı), `MIN_SHADOW_TRADES=5` (shadow kanıtı yalnızca GERÇEK piyasa
aktivitesiyle birikir, hızlandırılamaz — 30 talep etmek AYLARCA hiçbir
promotion'a izin vermezdi). `INSUFFICIENT_EVIDENCE`, üçünden biri eşiği
geçmeden HERHANGİ bir iyileşme karşılaştırmasından ÖNCE kontrol edilir —
mükemmel discovery/confirmation ama yetersiz shadow, tıpkı kanıt hiç
yokmuş gibi REDDEDİLİR.

Discovery/confirmation değerlendirmesi (`adaptive/evaluation.py`),
`research.replay.HistoricalReplayDriver` + `execution.lifecycle_replay_
sanity.simulate_lifecycle_exits` + `research.monte_carlo`'yu DEĞİŞTİRMEDEN
yeniden kullanır — sıfır ikinci backtest implementasyonu. `fee_bps=10.0`
(Binance'in yayınlanmış standart spot taker ücreti, VIP/BNB indirimi
varsayılmadan) ve `slippage_bps=5.0` (dokümante edilmiş, KORUNAKLI bir
TAHMİN — resmi yayınlanmış bir rakam yok, taker ücretinin kabaca yarısı)
— `ReplayConfig()`'in kendi sıfır varsayılanı ASLA kullanılmaz.

Adaptive Intelligence v1 kapsamı 16 adımda tamamlandı: `PolicySnapshot`
şeması, position-pinning düzeltmesi, trade/policy attribution
(`policy_version_id` `bridge_completed_trade`'e kadar hayatta kalır),
`adaptive/` paketi + allowlist-tabanlı güvenlik taraması, challenger
üretimi, discovery/confirmation pencere ayrımı, discovery/confirmation
değerlendirmesi, bağımsız state'li shadow değerlendirmesi (bkz. Karar 94),
promotion gate, drift farkındalığı (`research.drift` DEĞİŞTİRİLMEDEN,
salt-gözlemsel), append-only restart-dayanıklı persistans
(`AdaptiveStore`), otomatik periyodik tetikleyici (`AdaptiveScheduler`,
`ReselectionScheduler`'ın AYNI deseniyle, 24 saatlik dokümante edilmiş
üst sınır) + manuel CLI (`scripts/adaptive_evaluation_cycle.py`, AYNI
paylaşılan `run_adaptive_cycle()` fonksiyonunu çağırır), üretim bağlaması
(`app.py`'ye eklemeli, opsiyonel `exit_policy_provider`/`candle_observer`/
`m1_candle_observer` parametreleri + `scripts/run_with_adaptive_policy.
py`), rollback mekanizması (bkz. Karar 95), ve state protection kanıtı
(tablo adı çakışması YOK, `adaptive/` mevcut hiçbir persistans modülünü
import ETMEZ).

**KASITLI SAPMA (mission'ın kendi talimatına göre AÇIKÇA bildiriliyor,
sessizce atlanmadı):** Mission, `scripts/run_with_adaptive_policy.py`'nin
PAPER modda canlı bir smoke-test ile çalıştırılıp yeni açılan bir
pozisyonun champion'ın `policy_version_id`'sini aldığının GÖSTERİLMESİNİ
istiyordu. Kodun doğrudan okunmasıyla DOĞRULANDI: bu PAPER modda
İMKANSIZDIR — `policy_version_id`/`ExitPolicyConfig` YALNIZCA
`BridgePositionRecord`'da (Testnet-bridge'in KENDİ pozisyon kaydı) var
olan kavramlardır; `PaperPosition`'ın böyle bir alanı YOKTUR (PAPER
çıkışları saf sinyal-tersine-dönüş mantığıdır, ATR-tabanlı politika
kavramı hiç yoktur — Adım 3'te zaten tespit edilmişti). Kullanıcıya bu
gerçek mimari kısıt sunuldu; kullanıcı canlı Testnet-bridge smoke-test'i
YERİNE, mevcut offline test paketinin (`test_run_with_adaptive_policy_
script.py`'nin bit-for-bit-eşdeğerlik testi, `test_app_adaptive_wiring.
py`'nin provider-manager'a-ulaşıyor testi, `TestAdaptivePolicyPinning`'in
uçtan-uca gömme testleri) yeterli kanıt olarak KABUL EDİLMESİNİ SEÇTİ —
canlı smoke-test BİLİNÇLİ OLARAK ATLANDI.
**Reason:** Mission'ın 16 adımlık spesifikasyonu, mevcut production exit/
stop/target/trailing politikasının (`ExitPolicyConfig`) elle ayarlanan,
asla yeniden optimize edilmeyen sabit bir varsayılan olduğunu ve bunun
GERÇEK, güvenlik-sınırlı bir keşif/doğrulama/shadow/promotion döngüsüyle
kademeli olarak iyileştirilebileceğini belirledi — ama HERHANGİ bir
otomatik ayarlamanın: (a) asla `RiskPolicyConfig` gibi insan-belirlenmiş
güvenlik tavanlarına dokunmaması, (b) asla açık bir pozisyonu retroaktif
olarak etkilememesi (position-pinning), (c) asla ikinci bir strateji/
sinyal/consensus implementasyonu YARATMAMASI (mevcut `research/`/
`execution/lifecycle_replay_sanity.py` ZATEN kabul edilmiş modüllerinin
yeniden kullanılması), ve (d) `crypto_signal_engine/`'in `adaptive/`'ı
ASLA import etmemesi (bağımlılık yönü tersine dönemez) gerektiği
belirtildi.
**Consequences:** 21 commit, ~150 yeni test (`tests/test_adaptive_*.py`,
`tests/test_repository_safety_scan_adaptive.py`, `tests/test_execution_
lifecycle*.py`'ye eklenen position-pinning testleri, `tests/test_app_
adaptive_wiring.py`). Tam paket 1880 test ile TAMAMEN YEŞİL kaldı (önceki
taban 1722 + adaptive milestone'un tüm testleri). `tests/test_repository_
safety_scan.py` ve `tests/test_repository_safety_scan_research.py`
DEĞİŞTİRİLMEDEN kaldı ve geçmeye devam ediyor. Bağımsız doğrulama
turunda 5 boşluk bulundu ve düzeltildi — bkz. Karar 94 (M1-fidelity
shadow düzeltmesi) ve Karar 95 (rollback kriteri düzeltmesi); ayrıca
drift sinyalinin `run_adaptive_cycle()`'a bağlanması ve güvenlik
taramasının denylist'ten allowlist'e çevrilmesi bu Kararın KENDİ
kapsamındaki commit'lerde (bağımsız doğrulama turu) düzeltildi, ayrı bir
Karar gerektirmeyecek kadar dar kapsamlı bulundu.

## Karar 94 — BAĞIMSIZ DOĞRULAMA DÜZELTMESİ: shadow değerlendirmesi artık M5 yerine GERÇEK M1 hassasiyetinde çalışıyor

**Date:** 2026-09-05
**Decision:** `crypto_signal_engine/execution/lifecycle_runtime.py`'nin
`LifecycleRuntime`'ına, `order_book_observer`/`candle_observer` ile AYNI
savunmacı-sarmalama disipliniyle, EKLEMELİ ve opsiyonel bir
`m1_candle_observer` hook'u eklendi. `_consume_m1()`'in GERÇEK M1 akışını
tükettiği TAM noktada, HER gerçek, zaten-kapanmış M1 mumu için TAM OLARAK
BİR KEZ çağrılır — `LifecycleManager.evaluate_m1_candle()`'ın kendisinin
kullandığı AYNI frekansta. `adaptive/shadow.py`'ye YENİ `ShadowMonitor.
on_m1_candle(symbol, candle)` metodu eklendi — bu, PRODUCTION shadow
gözlem yolu haline geldi; `scripts/run_with_adaptive_policy.py`, ESKİ M5
seviyeli `RuntimeCoordinator.candle_observer` bağlamasını (`ShadowMonitor.
on_candle`) bu YENİ M1 hook'uyla DEĞİŞTİRDİ (`Application`'a hem M5 hem
M1 hook'unu AYNI ANDA bağlamak, AYNI 5 dakikalık pencereyi iki kez
değerlendirip trailing-stop'u ÇİFT UYGULARDI — bilinçli olarak
YAPILMADI). `ShadowMonitor.on_candle` (M5 seviyesi) `RuntimeCoordinator`
seviyesinde bağımsız kullanım için KALDI ama artık production'da
KULLANILMIYOR.
**Reason:** Bağımsız doğrulama turu, `candle_observer`'ın YALNIZCA
`RuntimeCoordinator.ingest_candle()` üzerinden (M5/M15/H1) tetiklendiğini,
GERÇEK M1 mumlarının `RuntimeCoordinator`'ı TAMAMEN ATLAYIP doğrudan
`lifecycle_runtime.py::_consume_m1`'den aktığını tespit etti — yani shadow
değerlendirmesi 5-dakika-toplulaştırılmış OHLC'ye karşı, GERÇEK
pozisyonun HER 1-dakikalık mumda değerlendirilmesine karşı çalışıyordu.
`evaluate_candle()`'ın BİR değerlendirilen mum İÇİNDE sabit bir
stop-önce-target önceliği vardır — GERÇEK M1 hassasiyetinde bu belirsizlik
penceresi 1 dakikadır (nadir); M5'e toplulaştırıldığında 5 KAT daha
olasıdır, ve GERÇEK kronolojik sırada target aslında stop'tan ÖNCE
vurulmuşsa, M5-toplulaştırılmış bir değerlendirme YANLIŞ çıkış nedenini
(target yerine stop) bildirir — promotion'ın DAYANDIĞI shadow kanıtının
GÜVENİLİRLİĞİNİ baltalar.
**Consequences:** `tests/test_lifecycle_runtime.py`'ye `TestM1CandleObserver`
sınıfı eklendi (5 test: her kapalı M1 mumunda TAM BİR kez çağrılıyor,
kapanmamış mumlarda ASLA çağrılmıyor, GERÇEK değerlendirme BAŞARISIZ
olsa bile hâlâ çağrılıyor, observer hatası M1 tüketimini ASLA
durdurmuyor, `None` iken davranış DEĞİŞMİYOR). `tests/test_adaptive_
shadow.py`'ye `TestM1FidelityRequiredProof` sınıfı eklendi — target'ın
stop'tan ÖNCE vurulduğu GERÇEK bir senaryoda, M5-toplulaştırılmış
değerlendirmenin YANLIŞ cevabı (`STOP_LOSS`) verdiğini, GERÇEK M1
hassasiyetinin ise DOĞRU cevabı (`TAKE_PROFIT`) verdiğini KANITLAR —
eski implementasyonun altında YANLIŞ cevap verecek bir test. `app.py`'ye
eklemeli, opsiyonel `m1_candle_observer` parametresi eklendi (varsayılan
`None`, mevcut davranış bit-for-bit değişmedi). Tam paket + üç güvenlik
taraması TAMAMEN YEŞİL kaldı.

## Karar 95 — BAĞIMSIZ DOĞRULAMA DÜZELTMESİ: rollback kriteri artık kanıta dayalı, tek bir küçük kayıptan TETİKLENMİYOR

**Date:** 2026-09-05
**Decision:** `adaptive/rollback.py`'nin ESKİ kriteri ("sample_count >= 10
VE net PnL < 0") TAMAMEN DEĞİŞTİRİLDİ. YENİ kriter: champion'ın GERÇEK
canlı performansını dokümante edilmiş bir BASELINE'a karşı karşılaştırır
— ÖNCELİKLE champion'ın KENDİ promotion-öncesi shadow-aşaması kanıtına
(`AdaptiveStore.shadow_outcomes_for()`, zaten persist edilmiş), shadow
kanıtı yoksa/yetersizse bir ÖNCEKİ champion'ın KENDİ gerçek canlı
performansına düşerek. `research.attribution.PerformanceMetrics`'in
ŞEKLİNİ (win_rate, profit_factor) yeniden kullanan YENİ
`performance_metrics_from_pnls()` fonksiyonu — `research.attribution.
_compute_metrics` private ve Signal `TradeRecord`'a göre şekillenmiş
olduğundan, AYNI formüller düz bir float listesine uygulanarak (ikinci,
farklı bir istatistik İCAT EDİLMEDEN) yeniden kullanıldı. Rollback ARTIK
şunları GEREKTİRİR: >=10 gerçek işlem, baseline'da >=5 işlem, net PnL
negatif OLMASI (gerekli ama YETERLİ DEĞİL), VE aşağıdaki İKİ BAĞIMSIZ,
büyüklük-farkında sinyalden EN AZ BİRİNİN gerçek bir bozulma göstermesi:
profit_factor baseline'ın YARISININ ALTINA çökmüş (`ROLLBACK_PROFIT_
FACTOR_DEGRADATION_RATIO=0.5`) VEYA win_rate baseline'a göre 15 yüzde
puanından FAZLA düşmüş (`ROLLBACK_WIN_RATE_DROP=0.15`) — TEK BAŞINA bir
işaret asla yeterli değildir. `champion_live_pnls_provider` (düz PnL
listesi döndüren, `champion_live_evidence_provider`'dan AYRI YENİ bir
callable) `run_adaptive_cycle`/`AdaptiveScheduler`/`run_with_adaptive_
policy.py` üzerinden bağlandı.
**Reason:** Bağımsız doğrulama turu, ESKİ kuralın DOKUZ break-even işlem
arasındaki TEK BİR -0.01 USDT'lik kayıptan bile TETİKLENECEĞİNİ tespit
etti — bu, bu projenin HER YERDE AÇIKÇA REDDETTİĞİ "birkaç kayıp ->
politikayı değiştir" desenidir (`INSUFFICIENT_EVIDENCE`-varsayılan,
küçük örneklemlere tepki YOK, gürültüye curve-fit YOK, küçük Testnet
örneklemlerinden sabit kodlanmış sonuçlar YOK — bu projenin promotion
gate'inin KENDİ felsefesiyle DOĞRUDAN ÇELİŞİYORDU).
**Consequences:** `tests/test_adaptive_rollback.py` TAMAMEN yeniden
yazıldı (14 test). İKİ ZORUNLU test dahil: DOKUZ nötr/pozitif sonuç
arasındaki TEK BİR küçük kayıp işlemin rollback'i TETİKLEMEDİĞİNİ, ama
GERÇEKTEN büyük ve sürdürülen bir bozulmanın (profit_factor VE win_rate
ikisi de çökmüş) TETİKLEDİĞİNİ kanıtlayan testler. Ayrıca: yetersiz canlı/
baseline örneklemin ASLA tetiklemediğini, shadow baseline yokken ÖNCEKİ
champion'ın canlı performansına DOĞRU düştüğünü, hiçbir baseline kanıtı
yokken rollback'in DEĞERLENDİRİLMEDİĞİNİ (crash değil), ve her rollback'in
`champion_history()`'de AÇIKÇA loglandığını kanıtlayan testler. Tam
paket + üç güvenlik taraması TAMAMEN YEŞİL kaldı.

## Karar 96 — Adaptive Symbol Intelligence v1: `AutomaticSymbolSelector`'a 4. bir öğrenilmiş (learned) skorlama terimi eklendi

**Date:** 2026-09-06
**Decision:** `AutomaticSymbolSelector`'ın 3 mevcut skorlama formülüne
(likidite/tarihsel hareket/güncel fırsat) DOKUNULMADAN, YENİ, OPSİYONEL
4. bir terim eklendi: `learned_factor_score` — bir sembolün KENDİ GERÇEK
tamamlanmış işlem geçmişinden türetilen bir "öğrenilmiş faktör". Yeni
`adaptive/symbol_score.py::learned_factor(symbol, pnls) -> float`
([0.0, 1.0] aralığında), `adaptive/decision.py::evidence_from_pnls`'i
(YENİDEN implemente ETMEDEN) kullanarak `EvidenceSummary` üretir.
`MIN_SYMBOL_TRADES = 10` altında KESİNLİKLE `0.5` (nötr) döner — TEK bir
aşırı-uç işlem (örn. 1 işlemlik -1000 USDT kayıp) skoru ETKİLEYEMEZ. Eşik
ÜZERİNDE: `0.5 * win_rate + 0.5 * profitability_component` (profitability_
component: net PnL pozitifse 1.0, negatifse 0.0, tam sıfırsa 0.5) — HER
İKİ bileşen ZATEN `[0, 1]` aralığında, ek bir clamp/ölçekleme sabiti
İCAT EDİLMEDEN.

`AutomaticSymbolSelector.__init__`'e opsiyonel, varsayılanı `None` olan
`learned_factor_provider: Callable[[str], float] | None` parametresi
eklendi — `exit_policy_provider`/`candle_observer`/`m1_candle_observer`
İLE AYNI disiplin: `None` iken davranış ÖNCEKİYLE BİT-FOR-BİT AYNIDIR
(hem "parametre hiç verilmedi" hem "her zaman 0.5 döndüren no-op bir
provider verildi" durumları AYNI `SelectionResult`'ı üretir — test edildi).
Provider fırlatırsa (`try/except`), o sembol için nötr `0.5`'e düşülür —
tüm `select()` çağrısı ASLA etkilenmez (`order_book_observer`/
`candle_observer`/`m1_candle_observer` İLE AYNI savunmacı-sarmalama
deseni). YENİ `LEARNED_FACTOR_WEIGHT = 0.10`; önceki 3 ağırlık ORANSAL
olarak (×0.9) yeniden ölçeklendi: `LIQUIDITY_WEIGHT` 0.15→0.135,
`HISTORICAL_MOVEMENT_WEIGHT` 0.45→0.405, `CURRENT_OPPORTUNITY_WEIGHT`
0.40→0.36 — 4'ü TOPLAMDA hâlâ tam olarak `1.0` (test-enforced).
`LEARNED_FACTOR_WEIGHT=0.10` KASITLI OLARAK küçük seçildi: likidite
ağırlığıyla (0.135) AYNI mertebede, birincil sıralama sürücüsü DEĞİL,
ZATEN eligible olan adaylar arasında bir tie-break. `CandidateScore`'a
`learned_factor_score: float` alanı eklendi (`require_finite` ile
doğrulanır, diğer 3 skor alanıyla AYNI disiplin); reddedilen adaylarda
`0.0` ("hesaplanmadı", diğer 3 alanla AYNI kural), eligible adaylarda
provider yokken `0.5` (nötr).

`adaptive/symbol_score.py`, `crypto_signal_engine.execution`'ın HİÇBİR
alt-modülünü import ETMEZ (ne `lifecycle_store` ne başka biri) — saf,
zaten-elde-mevcut bir `pnls: Sequence[float]` listesi üzerinde çalışır.
GERÇEK `LifecycleStore.completed_trades_for_symbol()` erişimi, HER İKİ
paketi de import eden bir canlı-bağlama script'inde (`scripts/
run_with_adaptive_policy.py` benzeri) kalır — `adaptive/symbol_score.py`
KENDİSİ asla oraya erişmez. `crypto_signal_engine/selection/*.py`
`adaptive/`'ı ASLA import ETMEZ (AST-tabanlı, pozitif-kontrol testli
güvenlik taraması genişletmesiyle KANITLANMIŞTIR) — bağlama TEK yönlü,
yalnızca enjekte edilen `learned_factor_provider` callable'ı üzerinden.

**GÖREV METNİNDEKİ HATALI VARSAYIM (sessizce uygulanmadı, açıkça
düzeltildi):** Görev metni, `adaptive/symbol_score.py`'nin `crypto_signal_
engine.execution`'a YALNIZCA `lifecycle_store.py` ÜZERİNDEN erişebileceğini
ve bunun "ZATEN mevcut AST allowlist'inde izinli bir alt-modül" olduğunu
iddia ediyordu. Bu YANLIŞTIR: `tests/test_repository_safety_scan_
adaptive.py`'nin `ALLOWED_EXECUTION_SUBMODULES`'ı YALNIZCA `lifecycle` ve
`lifecycle_replay_sanity`'yi içerir — `lifecycle_store`, dosyanın KENDİ
`TestAllowlistCatchesTransitiveAndEveryImportForm.test_catches_every_
other_named_missing_submodule` testinde AÇIKÇA "yakalanması gereken"
(yasak) bir örnek olarak listelenmiştir. Bu çelişki, `learned_factor()`'ı
TAMAMEN saf/veri-erişimsiz tasarlayarak (Görev metninin KENDİ Adım 1
ifadesiyle tutarlı: "Build a small provider function ... mirroring the
champion-live-evidence provider pattern already used in scripts/run_
with_adaptive_policy.py" — bu pattern, `LifecycleStore` erişimini
`adaptive/`'ın DIŞINDA tutar) çözüldü — allowlist'e `lifecycle_store`
EKLENMEDİ, mevcut pozitif-kontrol testi DEĞİŞTİRİLMEDEN kaldı.
**Reason:** Bu mekanizma, exit-policy `adaptive/` sisteminden (Karar 93)
BİLİNÇLİ OLARAK DAHA DÜŞÜK riskli tasarlandı ve HİÇBİR promotion/shadow/
rollback kapısı GEREKTİRMEZ — çünkü: (1) sıfır order-execution blast
radius'u vardır — YANLIŞ bir `learned_factor_score`, EN FAZLA zaten-
eligible adaylar arasında SUBOPTIMAL bir sembol seçimine yol açar, ASLA
güvensiz bir işleme yol açmaz (hiçbir eligibility kapısını/mutlak tabanı
ASLA etkilemez, hiçbir order ASLA göndermez); (2) None-varsayılan disiplini,
provider hiç bağlanmazsa (bu milestone'da HENÜZ bağlanmadı — `Reselection
Scheduler`/canlı `resolve_symbols()` bağlaması KASITLI OLARAK kapsam
DIŞI bırakıldı, bkz. görev metni) davranışın SIFIR değiştiğini garanti
eder; (3) bu, exit-policy sisteminin KENDİ üç-kanıt-sınıflı promotion
gate'inin var olma sebebiyle (yanlış bir promotion GERÇEK para kaybettirebilir,
GERÇEK bir pozisyonu retroaktif etkileyebilir) TEMELDEN FARKLIDIR — burada
öyle bir risk YOKTUR, dolayısıyla böyle bir kapı YOKLUĞU savunulabilirdir.
**Consequences:** `adaptive/symbol_score.py` (13 test, `tests/test_
adaptive_symbol_score.py` — ZORUNLU test dahil: 1 işlemlik -1000 PnL'in
KESİNLİKLE 0.5 skorladığını kanıtlar), `crypto_signal_engine/selection/
selector.py`/`models.py` değişiklikleri (`tests/test_selection_selector.py`'ye
8 yeni test — None-vs-no-op-provider bit-for-bit eşdeğerlik, fırlatan
provider'ın ASLA `select()`'e sızmadığını, 4 ağırlığın TAM `1.0`'a
toplandığını, `learned_factor_score`'un `total_score`'a DOKÜMANTE
EDİLMİŞ ağırlıkla DOĞRU katıldığını kanıtlayan testler; `tests/test_
selection_models.py` — YENİ dosya, `learned_factor_score`'un `require_
finite` ile doğrulandığını kanıtlar), `tests/test_repository_safety_
scan_adaptive.py`'ye AST-tabanlı `crypto_signal_engine/selection/` →
`adaptive/` import kontrolü + pozitif-kontrol testleri eklendi. Tam
paket + üç güvenlik taraması TAMAMEN YEŞİL kaldı (bkz. bu milestone'un
final raporu için tam pass/fail sayıları). Ayrı bir PAPER canlı smoke-
test'e GEREK YOKTUR (Karar 93'ün PAPER-smoke-test-atlama kararından
FARKLI bir gerekçeyle): `learned_factor()` ve bağlaması saf, deterministik,
process-içi hesaplamalardır — sıfır ağ çağrısı, `completed_trades_for_
symbol()`'ın MEVCUT okuma yolunun ÖTESİNDE sıfır yeni DB yazımı; kanıtlanacak
GERÇEK, canlı, zamana-bağlı bir davranış YOKTUR (Karar 93'ün gerekçesi
FARKLIYDI: orada PAPER modun `policy_version_id` kavramını YAPISAL OLARAK
TAŞIYAMAMASI vardı — burada ise böyle bir yapısal engel bile YOK, yalnızca
"kanıtlanacak canlı bir şey yok" durumu var).

## Karar 97 — Portfolio/Accounting v1: tekilleştirilmiş exposure/realized-P&L muhasebe katmanı + ölü `account_info()` bağlantısı

**Date:** 2026-09-06
**Decision:** Dört ayrı düzeltme/ekleme:

(1) **DRY ihlali düzeltmesi:** `sum(gross_entry_vwap * net_owned_base_
quantity, FLAT-olmayan pozisyonlar üzerinden)` formülü — DAHA ÖNCE
`LifecycleManager.entry_gate()` VE `app.py::_bridge_lifecycle_snapshot()`
içinde BAĞIMSIZ olarak İKİ KEZ yazılmıştı (gerçek bir sessiz-drift
riski) — `lifecycle.py::compute_total_exposure(positions)` adında TEK bir
saf fonksiyona çıkarıldı. Her iki çağrı sitesi ARTIK bu AYNI fonksiyonu
kullanıyor. `entry_gate()`'in davranışı BİT-FOR-BİT DEĞİŞMEDİ (aynı
per-symbol `position()` çağrıları, aynı sıra, aynı `open_slots` mantığı —
YALNIZCA exposure toplamı artık paylaşılan fonksiyonu çağırıyor); karma
durumlu (LONG/DUST/FLAT) bir fixture ile ÖNCE/SONRA `EntryGateResult`'ın
(hem `allowed` HEM `detail` string'i, ki bu hesaplanan exposure sayısını
GÖMER) AYNI kaldığını kanıtlayan bir test eklendi.

(2) **Gerçek lifetime + bugünün NET realized P&L'i (SQL aggregate):**
`lifecycle_store.py`'ye `total_realized_pnl()` (TÜM zamanların
`SUM(net_realized_pnl)`/`COUNT(*)`'ı, LIMIT YOK) ve `realized_pnl_
for_day(trading_day)` (AYNI, `trading_day_key()`'in KENDİ `"YYYY-MM-DD"`
formatını yeniden kullanarak TEK bir UTC takvim gününe filtrelenmiş)
eklendi. Gün filtresi `recorded_at` sütunu üzerinden çalışır (`exit_
timestamp` DEĞİL) — çünkü `_finalize_exit()` AYNI `now`'ı HEM bu sütuna
HEM `bridge_daily_risk`'i güncelleyen `trading_day_key(now)`'a geçirir;
bu, bu fonksiyonun gün sınırını `DailyRiskAccumulator`'ın KULLANDIĞI
TAM AYNI ana sabitler.

**Dashboard'un ve risk-breaker'ın "bugünün P&L'i" rakamlarının NEDEN
İKİ FARKLI SAYI olmasına (dokümante edilmeden) izin verildiğinin
KEŞFİ, ve bunun artık NASIL belgelendiği:** `app.py::_bridge_lifecycle_
snapshot()` DAHA ÖNCE kendi "bugünün realized P&L'i"ni `recent_
completed_trades(limit=50)` üzerinden, Python'da, GROSS (`gross_
realized_pnl`) olarak, bugünün tarihine göre filtreleyerek YENİDEN
TÜRETİYORDU — bu, `daily_loss_breaker_tripped()`'in GERÇEKTEN güvendiği,
kalıcı (`bridge_daily_risk`), NET/fee-ayarlı `DailyRiskAccumulator.
conservative_risk_pnl`'DEN TAMAMEN FARKLI bir sayıydı, HİÇBİR yerde bu
fark AÇIKLANMAMIŞTI. Artık `lifecycle_store.py::realized_pnl_for_day()`'ın
KENDİ docstring'i (ve bunu ÇALIŞTIRILABİLİR bir testle KANITLAYAN
`test_realized_pnl_for_day_reconcilable_against_daily_risk_accumulator_
when_all_fees_known`) BU İKİ SAYININ NEDEN farklı OLABİLECEĞİNİ (fee'ler
TAM bilinmiyorsa `conservative_risk_pnl` KASITLI OLARAK daha kötümser bir
REZERV çıkarır, bu fonksiyon ise SADECE bilinen net değeri raporlar) VE
HER İKİ fee de TAM biliniyorsa AYNI sayıya İNDİKLERİNİ AÇIKÇA belgeliyor
— bu, iki sayıyı TEK bir sayıya BİRLEŞTİRMEZ (ikisi farklı AMAÇLARA
hizmet eder — biri gözlemlenebilirlik, diğeri kötümser bir devre kesici),
ama artık SESSİZCE ÇELİŞMEZ.

(3) **Yeni modül `crypto_signal_engine/portfolio/accounting.py`:**
`AccountingSnapshot` (+ `build_accounting_snapshot()`) — TEK yetkili,
SAF (kendi I/O'su YOK) muhasebe hesaplayıcısı. `adaptive/symbol_score.py`
emsaliyle AYNI disiplin: zaten-elde-mevcut veriyi (pozisyonlar, `lifecycle_
store.py`'nin YENİ SQL-aggregate fonksiyonlarının sonuçları, bir fiyat-
sorgu callable'ı) düz argüman olarak alır, `LifecycleStore`'u ASLA
DOĞRUDAN import ETMEZ (veri-erişim katmanından KASITLI olarak
AYRIŞTIRILMIŞ kalır — çağıran, yani `app.py`, ikisini birbirine bağlar).
`unrealized_pnl_usdt`, price_lookup HERHANGİ bir açık pozisyon için
fırlatırsa VEYA `None` dönerse KESİNLİKLE `None`'dır — ASLA kısmi/uydurma
bir toplam (mevcut `app.py` disiplini YENİDEN KULLANILDI, YENİ bir tane
İCAT EDİLMEDİ).

(4) **`app.py::_bridge_lifecycle_snapshot()` yeniden bağlandı:** artık
exposure/realized/unrealized/bugün rakamlarını inline YENİDEN TÜRETMEK
YERİNE `build_accounting_snapshot()`'ı çağırıyor. `ops/dashboard.py`'nin
tükettiği dict ANAHTARLARI DEĞİŞMEDİ (`exposure_usdt`, `non_flat_
position_count`, `unrealized_gross_pnl_total`, `realized_gross_pnl`,
`todays_realized_gross_pnl`, `completed_trade_count`) — bu, KASITLI
OLARAK bu milestone'un dashboard UI'ını YENİDEN TASARLAMAMASI kuralına
uyar. AMA `realized_gross_pnl`/`todays_realized_gross_pnl` anahtarları
ARTIK GROSS DEĞİL NET değerler taşıyor (anahtar adı "gross" demeye DEVAM
EDİYOR — bu, anahtarı yeniden adlandırmanın bir `ops/dashboard.py` UI
değişikliği, dolayısıyla bu backend-doğruluk milestone'unun KAPSAMI
DIŞINDA olması nedeniyle KASITLI, DOKÜMANTE EDİLMİŞ, GEÇİCİ bir isim
uyuşmazlığıdır — `_bridge_lifecycle_snapshot()`'ın KENDİ docstring'inde
VE burada AÇIKÇA belirtilir, sessizce bırakılmaz). `win_count`/`win_rate`/
`average_win_gross` vb. DEĞİŞMEDEN bırakıldı (hâlâ `recent_completed_
trades(limit=50)`/GROSS üzerinden) — `AccountingSnapshot`'ın hiçbir
per-trade kazanç/kayıp alanı YOK, bu yüzden ONLARIN bounded/gross kör
noktasını düzeltmek bu milestone'un TANIMLI kapsamının DIŞINDA bırakıldı.

(5) **Ölü `account_info()` bağlantısı:** `testnet_client.py::account_
info()` (GERÇEK, ZATEN çalışan, imzalı bir `GET /api/v3/account`
çağrısı) DAHA ÖNCE SIFIR yerden çağrılıyordu. `portfolio/accounting.py`'ye
`AssetBalance`/`parse_account_balances()` (saf JSON ayrıştırma, ham
payload'ı ASLA daha ileri SIZDIRMAZ) eklendi. GERÇEK exchange çağrısı VE
onun savunmacı sarmalaması `execution/reconciliation_service.py::
ExecutionReconciliationService.check_usdt_balance()`'a EKLENDİ —
`portfolio/accounting.py`'YE DEĞİL. **Yer seçimi gerekçesi:**
`ExecutionReconciliationService` ZATEN `self._client`'a (imzalı,
kimlik-doğrulanmış istemci) sahip VE ZATEN "iç durumu exchange gerçeğine
karşı karşılaştır" desenini SAHİPLENİYOR (bkz. modül docstring'i) —
bu metod bu deseni GENİŞLETİR, PARALEL bir mekanizma İCAT ETMEZ.
`portfolio/accounting.py`'yi (Adım 3'ün KENDİ disiplini gereği) exchange-
istemci bağımlılığından ARINMIŞ tutmak, onu `adaptive/symbol_score.py`
ile AYNI "saf hesaplama, veri-erişiminden AYRIŞMIŞ" ilkesinde bırakır.
`check_usdt_balance()` YALNIZCA gözlemlenebilirliktir — HİÇBİR trading
davranışını BLOKE ETMEZ/DEĞİŞTİRMEZ, execution store'a HİÇBİR YAZI
YAPMAZ (test edildi), VE herhangi bir hata (ağ, kimlik doğrulama, bozuk
yanıt) `None`'a ("bakiye bilinmiyor") DEĞER — ASLA çağırıcıya ÇÖKMEZ,
ASLA bir sayı UYDURMAZ.
**Reason:** Bu milestone SIFIR yeni order-gönderme yolu VE SIFIR yeni
BLOKE EDİCİ kapı EKLER — mevcut `RiskPolicyConfig`'in ÜÇ kapısı
(`max_open_positions`/`max_total_exposure_usdt`/`daily_loss_breaker_
tripped`) davranış OLARAK HİÇ DEĞİŞMEDİ, YALNIZCA exposure formülünün
TEK doğruluk kaynağı düzeltildi. Bu SAF bir doğruluk + gözlemlenebilirlik
milestone'udur — Karar 93'ün exit-policy `adaptive/` sisteminin AKSİNE,
BURADA hiçbir promotion/shadow/rollback kapısına HİÇ GEREK YOKTUR: bu
kod hiçbir zaman bir order göndermez veya bir trading kararını
etkilemez, yalnızca ZATEN gerçekleşmiş/mevcut durumu DOĞRU şekilde
RAPORLAR.
**Consequences:** `tests/test_execution_lifecycle.py`'ye `TestCompute
TotalExposure` (5 test), `tests/test_execution_lifecycle_manager.py`'ye
byte-for-bit regresyon testi, `tests/test_execution_lifecycle_store.py`'ye
`TestRealizedPnlAggregates` (7 test, >50 işlemlik ZORUNLU test dahil),
YENİ `tests/test_portfolio_accounting.py` (26 test: `AccountingSnapshot`
validasyonu, `unrealized_pnl_usdt`'nin None-on-failure disiplini,
determinism, `AssetBalance`/`parse_account_balances`), YENİ `tests/
test_app_bridge_lifecycle_snapshot.py` (6 test: dict anahtarları
DEĞİŞMEDİ, değerler artık NET/true-lifetime), `tests/test_execution_
reconciliation_service.py`'ye `TestCheckUsdtBalance` (5 test, fırlatan
`account_info()`'nin ASLA sızmadığını kanıtlayan ZORUNLU test dahil)
eklendi. `tests/test_repository_safety_scan.py` (zaten `crypto_signal_
engine/`'in TAMAMINI tarıyor, `portfolio/` OTOMATİK dahil) DEĞİŞİKLİK
GEREKTİRMEDEN geçti — ne `ALLOW_LIVE_TRADING`, ne ham HTTP, ne credential
literal'i (imzalama TAMAMEN `testnet_client.py`'nin İÇİNDE kalır, bu
modül ona YALNIZCA ÇAĞRI yapar). Tam paket + güvenlik taraması TAMAMEN
YEŞİL kaldı (bkz. bu milestone'un final raporu için tam pass/fail
sayıları).

## Karar 98 — GÖZDEN KAÇAN BOŞLUK DÜZELTMESİ: `check_usdt_balance()` hiçbir runtime yoldan ÇAĞRILMIYORDU — artık HER `_bridge_lifecycle_snapshot()` çalışmasında GERÇEKTEN tetikleniyor

**Date:** 2026-09-06
**Decision:** Karar 97'nin Adım 5'i, `ExecutionReconciliationService.
check_usdt_balance()`'ı yazdı VE test etti ama HİÇBİR üretim yoluna
(`app.py`, dashboard, periyodik döngü) BAĞLAMADI — yalnızca KENDİ test
dosyasında çalışıyordu, üretimde ASLA tetiklenmiyordu. Bu şimdi
düzeltildi: `Application._bridge_lifecycle_snapshot()` `async def`'e
çevrildi (ve onu çağıran `_write_snapshot()` de) — HER ÜÇ çağrı sitesi
(`start()`'ın recovery-success/recovery-failure dalları, `_health_loop()`
— varsayılan `HEALTH_SNAPSHOT_INTERVAL_SECONDS=5.0` ile HER 5 saniyede
bir, VE `_graceful_stop()`) ZATEN bir event loop İÇİNDE çalıştığından
`await` eklemek YETERLİYDİ, yeni bir task/loop İCAT EDİLMEDİ.
`_bridge_lifecycle_snapshot()` artık her çalışmasında `self._execution_
service.check_usdt_balance()`'ı GERÇEKTEN çağırıyor ve sonucu YENİ bir
üst-seviye `"usdt_balance"` anahtarına ({"asset", "free", "locked"} veya
`None`) ekliyor — `AccountingSnapshot`'ın KENDİSİNE EKLENMEDİ (o SAF ve
I/O'suz kalmaya devam eder, Karar 97 Adım 3'ün KENDİ disiplini), ayrı bir
dict alanı olarak. Savunmacı sarmalama İKİ KATMANDA korunur: `check_usdt_
balance()`'ın KENDİ `except Exception: return None`'ı (Karar 97'den
DEĞİŞMEDEN) VE ARTIK bu çağrı sitesinin KENDİ EK `try/except`'i (defense-
in-depth — `order_book_observer`/`candle_observer`/`m1_candle_observer`
çağrı sitelerinin AYNI "hem çağrılan HEM çağıran sarmalar" deseni) — bir
hata olursa `usdt_balance=None`, snapshot'ın GERİ KALANI ETKİLENMEZ,
health loop ASLA çökmez, trading ASLA bloke edilmez.
**Reason:** Bu boşluk, Karar 97'nin "check_usdt_balance() eklendi ve test
edildi" ifadesinin, "GERÇEKTEN kullanılıyor" anlamına GELMEDİĞİ bir
durumdu — kod GERÇEK ve DOĞRU çalışıyordu ama HİÇBİR canlı çağıran onu
HİÇ ÇAĞIRMIYORDU, bu yüzden üretimde asla bir sinyal ÜRETMİYORDU (aynen
`testnet_client.py::account_info()`'un KENDİSİNİN Karar 97'den ÖNCE
"gerçek, çalışan ama sıfır yerden çağrılan ölü kod" olması gibi — bu
kez BİR SEVİYE YUKARIDA, yeni yazılan wrapper'ın KENDİSİ için). Bu,
"kod var VE test edildi" ile "kod GERÇEKTEN üretim yolunda ÇALIŞIYOR"
arasındaki farkın somut bir örneğidir — bağımsız bir gözden geçirme bunu
YAKALADI, bu Karar bunu KAYDA GEÇİRİR ki gelecekte AYNI hata tekrar
edilmesin: yeni bir gözlemlenebilirlik fonksiyonu yazmak, onu bir çağrı
grafiğine BAĞLAMADAN "bitti" sayılamaz.
**Consequences:** `tests/test_app_bridge_lifecycle_snapshot.py`'ye YENİ
`TestUsdtBalanceGapFix` sınıfı eklendi (4 test): normal bir snapshot
çağrısının mock'lanmış `check_usdt_balance()`'ı GERÇEKTEN (TAM OLARAK bir
kez, her çağrıda YENİDEN) tetiklediğini, `None` sonucun dict'e DOĞRU
yansıdığını, VE fırlatan bir `check_usdt_balance()`'ın (hipotetik gelecek
bir hata) snapshot'ın GERİ KALANINI ETKİLEMEDEN `usdt_balance=None`'a
düştüğünü kanıtlar. Mevcut `TestDashboardDictKeysUnchanged` testi, YENİ
`"usdt_balance"` anahtarını içerecek şekilde güncellendi (bu, Karar 97'nin
"dict anahtarları DEĞİŞMEDİ" kuralına bir İSTİSNA DEĞİLDİR — o kural
MEVCUT anahtarlara uygulanır; `"usdt_balance"` KASITLI OLARAK YENİ bir
alandır, bu Kararın KENDİ kapsamındadır). Tam paket 1993 test ile
TAMAMEN YEŞİL kaldı (Karar 97 sonrası taban 1989 + bu düzeltmenin 4 yeni
testi).

## Karar 99 — Control Center / Backup / 24/7 Ops v1

**Date:** 2026-09-07
**Decision:** Sistemi mainnet'e hiç dokunmadan, gözetimsiz (unattended)
7/24 çalıştırmak için DÖRT bağımsız, katmanlı, additive sertleştirme
adımı eklendi. Hiçbiri exit-policy/risk-gate/order-submission
MANTIĞINI DEĞİŞTİRMEZ — dördü de ya salt-okunur gözlemlenebilirlik, ya
tersinir/bağımsız yeni bir anahtar, ya da ZATEN var olan güvenli bir
manuel script'in otomasyonudur.

**(1) Otomatik yedekleme (`deploy/systemd/crypto-signal-engine-backup.
{service,timer}`):** `scripts/backup_sqlite.py`'nin ÇEKİRDEK mantığı
(SQLite Online Backup API) DEĞİŞTİRİLMEDEN, GÜNLÜK olarak (`OnCalendar=
daily`, `Persistent=true` — makine kapalıyken kaçırılan bir gün, bir
sonraki boot'ta telafi edilir) çağrılır. **Zamanlama gerekçesi:** bir
paper/testnet pozisyon defteri, saatlik değil GÜNLÜK ölçekte anlamlı
değişir; bu timer, birincil dayanıklılık mekanizması (in-memory recovery
+ SQLite dosyasının kendisi, bkz. `persistence/recovery.py`) DEĞİL,
disk/host kaybına karşı EK bir savunma katmanıdır — bir günlük RTO
(recovery time objective) bunun için yeterlidir. Script'e TEK, additive
bir `--keep-last N` bayrağı eklendi (varsayılan `None` = eski davranış,
DEĞİŞMEDEN); verildiğinde backup BAŞARILI olduktan SONRA aynı kaynağa
ait en yeni N dosya DIŞINDAKİLER silinir. `--keep-last 14` (yaklaşık iki
haftalık günlük geçmiş) varsayılan olarak `.service` dosyasında
kullanılır. Silinecek dosyaları seçen `select_backups_to_delete()` SAF
bir fonksiyondur (gerçek dosya sistemi I/O'su YAPMAZ, `keep_last<=0`
KASITLI OLARAK "hiç silme" anlamına gelir — fail-safe varsayılan, bir
backup aracı için "yanlış konfigürasyon SESSİZCE her şeyi siler" mümkün
olan EN KÖTÜ hata modudur).

**(2) systemd watchdog (`WatchdogSec=60`):** ZATEN çalışan health loop'a
(varsayılan `CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS=5s`) YENİ bir
`sd_notify(WATCHDOG=1)` heartbeat çağrısı eklendi (`ops/systemd_
notify.py` — bağımlılıksız, ham `AF_UNIX SOCK_DGRAM` yazımı, YENİ bir
pip paketi YOK). **60s (5s'nin 12 katı) gerekçesi:** talimat >= 3x
istiyordu; 12x seçildi çünkü bu, ARA SIRA yaşanan yavaş bir tick'e (GC
duraklaması, yavaş bir disk yazımı) karşı GENİŞ bir tolerans tanırken,
GERÇEK bir kilitlenmeyi (event loop tıkanması) hâlâ EN FAZLA bir dakika
içinde systemd'nin (`Restart=on-failure`/`StartLimitBurst=5` politikası
ALTINDA) yakalamasını sağlar — `ops/lock.py`'nin `flock`'u SADECE ikinci
bir instance'ı engeller, HUNG bir tekil instance'ı ASLA tespit ETMEZ; bu
boşluğu kapatan TEK mekanizma budur. `$NOTIFY_SOCKET` yokken (yerel
geliştirme, PAPER modu, HER test) TAM bir no-op — ASLA fırlatmaz.

**(3) Control Center — PAUSE/RESUME/STOP (Bilinçli sınır):**
`dashboard_admin_token` (`AppConfig` İÇİNDE — bu bir Binance/exchange
kimlik bilgisi DEĞİLDİR, yalnızca yerel bir admin token'ıdır, bkz.
aşağıdaki "Credential yerleşimi" bölümü) `None` (varsayılan) OLDUĞU
SÜRECE dashboard bu milestone'dan ÖNCEKİYLE bit-for-bit AYNI davranır —
`ops/admin.py::AdminController` HİÇ İNŞA EDİLMEZ. Token AYARLANDIĞINDA:
`POST /admin/pause`/`/admin/resume` YENİ, bağımsız bir `entries_paused`
(`threading.Event`) kapısını açar/kapatır — bu, `LifecycleManager.
entry_gate()`'in ÜÇ mevcut risk kapısından TAMAMEN AYRI bir kontroldür
(`execution/signal_bridge.py`'de, `entry_gate()` ÇAĞRILMADAN ÖNCE
kontrol edilir) ve YALNIZCA YENİ giriş değerlendirmesini bloke eder —
zaten AÇIK bir pozisyonun yönetimi (M1 stop/target/trailing, ters-sinyal
SELL) bu kontrolden HİÇ geçmez, tamamen etkilenmeden devam eder (bkz.
`tests/test_signal_testnet_bridge.py::TestEntriesPausedGate::
test_existing_open_position_management_untouched_while_paused`).
`POST /admin/stop`, ZATEN var olan `Application.request_shutdown()`'ı
BİREBİR ÇAĞIRIR (`loop.call_soon_threadsafe` ile — dashboard'un KENDİ
HTTP thread'inden, çünkü `request_shutdown()` bir `asyncio.Event`'i
mutasyona uğratır, bu YALNIZCA kendi loop'unun thread'inde güvenlidir);
YENİ, PARALEL bir shutdown mekanizması İCAT EDİLMEDİ. Token header'dan
gönderilir (`X-Admin-Token`, query-string DEĞİL — bir query param'ın
tarayıcı geçmişine/proxy log'una sızma riski BELİRGİN ölçüde daha
yüksektir); eksik/yanlış token HER ZAMAN 401 döner ve ASLA state'i
mutasyona UĞRATMAZ (sabit-zamanlı `hmac.compare_digest` karşılaştırması
kullanılır); DENEME (token DEĞERİ DEĞİL) loglanır. **Bilinçli sınır —
bu milestone NE YAPMAZ:** (a) hiçbir "pozisyonu kapat" butonu/endpoint'i
YOKTUR — bu, ağa açık bir admin yüzeyinden GERÇEK bir piyasa emri
göndermek anlamına gelirdi, buradaki her şeyden TAMAMEN FARKLI bir risk
sınıfı; bu milestone için AÇIKÇA ERTELENDİ (gelecekte, ayrı bir
güvenlik-hassas görev olarak ele alınabilir). (b) `/admin/stop` süreci
TAMAMEN durdurur — YENİDEN BAŞLATMAYI OTOMATİKLEŞTİRMEZ ("Yeniden
Başlat" butonu KASITLI OLARAK bağlı DEĞİLDİR); `systemd`'nin `Restart=
on-failure`'ı temiz/kasıtlı bir exit'te ASLA tetiklenmez (bu tam olarak
İSTENEN — `stop` bir crash DEĞİLDİR) ve güvenilir bir UZAKTAN yeniden
başlatma mekanizması İNŞA ETMEK bu milestone'un AÇIKÇA kapsamı
DIŞINDADIR — operatör, sunucuda `systemctl start` ile elle başlatır.

**(4) Outbound-only alerting (`ops/notifier.py`):** Minimal bir
`Notifier = Callable[[str], None]` tipi + TEK somut implementasyon
(Telegram Bot API, düz `urllib` POST — YENİ bir pip paketi YOK). Dört
ZATEN var olan karar noktasına bağlandı: `LifecycleManager.entry_gate()`
İÇİNDE `daily_loss_breaker_tripped()` GEÇİŞİ (günde BİR kez, tripped
duruma GİRİŞTE — her bloke edilen girişte DEĞİL), `adaptive/rollback.
py::apply_rollback_if_needed()`'ın ZATEN loglanmış rollback kararı,
`app.py::Application`'ın süreç başlangıcı/graceful-stop'u, ve
`ops/admin.py::AdminController`'ın pause/resume/stop aksiyonları. HER
yerde `safe_notify()` ÜZERİNDEN çağrılır — bu tek fonksiyon, HERHANGİ
bir notifier implementasyonunun fırlattığı SENKRON bir exception'ı
YUTAR; Telegram implementasyonunun KENDİSİ AYRICA gönderimi bir
arka-plan (`daemon=True`) thread'inde yapar (fire-and-forget, sınırlı
timeout) — böylece yavaş/erişilemez bir Telegram API'si bile hiçbir
zaman bir trading kararını, pause'u veya shutdown'u BLOKE EDEMEZ. Her
bağlanma noktası için "raising notifier ASLA altındaki kararı
etkilemez" ZORUNLU testi eklendi (bkz. `tests/test_execution_lifecycle_
manager.py::test_a_raising_notifier_never_affects_entry_gate_decision`,
`tests/test_adaptive_rollback.py::test_a_raising_notifier_never_
affects_the_rollback_decision`).

**Credential yerleşimi (iki AYRI, KASITLI farklı desen):**
`dashboard_admin_token` bir Binance/exchange kimlik bilgisi DEĞİLDİR —
bu YEREL bir admin secret'ıdır, dolayısıyla `AppConfig` İÇİNDE, diğer
her alan gibi okunur (istisna, `tests/test_ops_config.py::
TestAppConfigHasNoCredentialFields`'a DOKÜMANTE edilmiş, TEK, dar bir
allowlist girdisi olarak eklendi) — ama yine de `AppConfig.summary()`'ye
EKLENMEDİ (o dict her başlangıçta TOPLU loglanır). Telegram bot
token/chat-id ise `CSE_TELEGRAM_BOT_TOKEN`/`CSE_TELEGRAM_CHAT_ID`
üzerinden, `AppConfig`'İN TAMAMEN DIŞINDA okunur — `BINANCE_TESTNET_
API_KEY`/`_SECRET`'İN (bkz. `execution/factory.py::
testnet_credentials_status()`) AYNI ÖNCEDENİ: SET/UNSET DURUMU
raporlanabilir, DEĞER ASLA. Bu ikili ayrım BİLİNÇLİDİR: birincisi
yerel/düşük-risk bir kontrol token'ı, ikincisi harici bir mesajlaşma
API'sine giden GERÇEK bir credential'dır.
**Reason:** Sistem gerçek parayla ASLA çalışmıyor olsa bile (`Execution
Mode` hâlâ yalnızca PAPER/BINANCE_SPOT_TESTNET), gözetimsiz uzun süreli
çalışma üç GERÇEK operasyonel riski taşır: (a) disk/host kaybında durable
state'in kurtarılamaması, (b) HUNG (crash değil, tıkanmış) bir process'in
sonsuza dek fark edilmeden kalması, (c) operatörün, bir sorunu (daily-loss
breaker tetiklendi, bir adaptive rollback oldu) GÜNLER sonra journalctl'i
elle taramadan ÖĞRENEMEMESİ. Dördü de bu üç riski, MEVCUT trading
mantığına SIFIR değişiklikle kapatır.
**Alternatives:** Control Center'a bir "force-close position" butonu
eklemek (reddedildi — ayrı, daha yüksek riskli bir görev); inbound
Telegram komut kanalı (reddedildi — bu milestone KASITLI OLARAK
outbound-only); generic bir notification framework/plugin sistemi
(reddedildi — Bölüm 8'in "generic bir framework İNŞA ETME" ilkesiyle
AYNI, TEK somut implementasyon yeterli).
**Consequences:** Yeni dosyalar: `crypto_signal_engine/ops/admin.py`,
`crypto_signal_engine/ops/notifier.py`, `crypto_signal_engine/ops/
systemd_notify.py`, `deploy/systemd/crypto-signal-engine-backup.
{service,timer}`. Değişen dosyalar: `scripts/backup_sqlite.py` (additive
`--keep-last`), `deploy/systemd/crypto-signal-engine.service`
(`WatchdogSec=60`), `crypto_signal_engine/ops/config.py`
(`dashboard_admin_token`), `crypto_signal_engine/ops/health_snapshot.py`
(additive `admin` alanı), `crypto_signal_engine/ops/dashboard.py`
(`/admin/{pause,resume,stop}` + GERÇEK Control Center UI durumu),
`crypto_signal_engine/execution/signal_bridge.py`
(`entries_paused_provider`), `crypto_signal_engine/execution/lifecycle_
manager.py` (`notifier` + breaker-geçiş bildirimi), `crypto_signal_
engine/app.py` (tüm bağlanma noktaları), `adaptive/rollback.py`/
`adaptive/cycle.py`/`adaptive/scheduler.py`/`scripts/run_with_adaptive_
policy.py` (`notifier` zinciri). Yeni test dosyaları: `tests/test_
backup_sqlite.py`, `tests/test_ops_systemd_backup_template.py`, `tests/
test_ops_systemd_notify.py`, `tests/test_ops_admin.py`, `tests/test_ops_
notifier.py`, `tests/test_app_24_7_ops.py`; genişletilen dosyalar:
`tests/test_ops_dashboard.py`, `tests/test_ops_config.py`, `tests/test_
execution_lifecycle_manager.py`, `tests/test_signal_testnet_bridge.py`,
`tests/test_adaptive_rollback.py`, `tests/test_repository_safety_scan.
py`. Tam paket 2079 test ile TAMAMEN YEŞİL kaldı (Karar 98 sonrası taban
1993 + bu milestone'un 86 yeni testi), safety scan dahil.

## Karar 100 — UI Polish / Final Acceptance v1'in KAPANIŞI: eksik A2/A3/A4/A7/A9 testleri + `MAINNET_READINESS_CHECKLIST.md` + ARCHITECTURE.md bayat satır düzeltmesi

**Date:** 2026-09-12
**Decision:** Karar 99'dan sonra, ayrı bir kesinti/devam döngüsünde,
dashboard'a GERÇEK veri kaynakları bağlanmıştı (`ops/dashboard.py`/
`app.py` kod yorumlarındaki "UI Polish v1, Step A1-A9" etiketleri) —
Step A9 (`crypto_signal_engine/ops/event_log.py`, append-only olay
kaydı) dahil TÜM kod ve wiring ZATEN tamdı ve mevcut test paketini
bozmuyordu, ama Step A2 (`usdt_balance` None-safety), A3 (`_adaptive_
snapshot()`), A4 (`learned_factor_score` kaynaklaması), A7 (`write_
backup_status()`/`_backup_snapshot()`), ve A9'un (`record_event()`)
KENDİ İZOLE testleri hiç yazılmamıştı (yalnızca A6/A8'in dashboard-render
testleri vardı) — ve bu milestone'un DECISIONS.md/ARCHITECTURE.md/
MAINNET hazırlık dokümantasyonu hiç TAMAMLANMAMIŞTI. Bu karar, o
kapanışı tamamlar; UI Polish v1'in KENDİSİNİN mantığında hiçbir değişiklik
YAPMAZ, yalnızca (a) eksik test kapsamını ekler, (b) üç dokümantasyon
eksiğini kapatır.

**(1) `ops/event_log.py` + dört çağrı noktasının (`ops/admin.py`,
`execution/lifecycle_manager.py`, `adaptive/rollback.py`, ve `adaptive/
cycle.py` -> `adaptive/scheduler.py` pass-through zinciri) "defensive-wrap"
disiplini kanıtı — yeni `tests/test_ops_event_log.py`:** `record_event()`
`safe_notify()`'den FARKLI bir şekle sahiptir — enjekte edilebilir bir
callable DEĞİL, modül-seviyesinde import edilen, doğrudan çağrılan bir
fonksiyondur; çağıran hiçbir yer (`admin.py`/`lifecycle_manager.py`/
`rollback.py`) kendi try/except'ini SARMAZ, TAMAMEN `record_event()`'in
KENDİ `except (sqlite3.Error, OSError)` yakalamasına GÜVENİR. Bu yüzden
testler, fonksiyonu SAHTE bir şekilde fırlatmaya ZORLAMAK (mock) yerine,
`event_log_path`'in GERÇEK yazılamaz olduğu bir senaryo (`blocker` adında
bir DOSYA'nın, `db_path`'in dizin bileşeni olarak kullanılması — `mkdir`
her zaman `FileExistsError` ile başarısız olur) üretir ve HER BEŞ çağrı
noktasının kararının (pause/resume/stop sonucu, breaker-trip detayı,
rollback'in gerçekleşmesi, cycle'ın rapor ettiği champion, scheduler'ın
`last_error`'ı) TAMAMEN ETKİLENMEDİĞİNİ kanıtlar — `adaptive/scheduler.
py::run_once()`'un KENDİ `except Exception` sarmalayıcısına bile hiç
ULAŞMADIĞINI (yani bozuk bir event log'un bir cycle'ı SAHTE OLARAK
"başarısız" göstermediğini) doğrudan doğrular.

**(2) A2/A3/A4/A7'nin kendi izole birim testleri:** `tests/test_ops_
dashboard.py`'ye `_build_view_model()`'i DOĞRUDAN çağıran (tam HTML
render'ından GEÇMEDEN) iki yeni sınıf eklendi — `usdt_balance`'ın
`bridge_lifecycle` kapalıyken/anahtar eksikken/mevcutken üç ayrı
davranışı (A2), ve `scan.learned_factors`'ın `candidates`'ten doğru
inşası + sembolsüz bir candidate'in sessizce ATLANMASI (A4, asla
uydurulmaması). YENİ `tests/test_app_ops_snapshots.py`: `Application.
_adaptive_snapshot()`'ın varsayılan `{"active": False}`'ü, sağlayıcı
sonucunun birebir geçmesi, VE fırlatan bir sağlayıcının snapshot'ı asla
ÇÖKERTMEMESİ (A3); `_backup_snapshot()`'ın dosya yokken/bozukken/
geçerliyken üç davranışı (A7). `tests/test_backup_sqlite.py`'ye `write_
backup_status()`'ın ATOMİK yazımı + doğru alanları için iki test eklendi
(A7'nin YAZMA tarafı — okuma tarafı yukarıda).

**(3) Track B dokümantasyon kapanışı:** YENİ `MAINNET_READINESS_
CHECKLIST.md` — bir taahhüt/tarih DEĞİL, Mainnet execution'ın bugün NEDEN
YAPISAL OLARAK İMKANSIZ bırakıldığının (`ALLOW_LIVE_TRADING=False`,
host allowlist, `ExecutionMode` enum'unda `MAINNET` üyesinin YOKLUĞU) ve
bir gün bu karar (AYRI, İNCELENMİŞ bir kararla) alınırsa neyin GERÇEKTEN
eksik olduğunun (execution güvenliği, test bar'ı, operasyonel hazırlık,
sır yönetimi, yasal/uyumluluk) dürüst bir envanteri. ARCHITECTURE.md'nin
"Bu proje için PLANLANMIŞ bir Faz 13 YOKTUR" cümlesi (hâlâ doğru, ama artık
YANILTICI — Faz 12'den beri BEŞ isimlendirilmiş milestone daha kabul
edildi) bir açıklama notuyla güncellendi; hiçbir kapsam kararı
DEĞİŞTİRİLMEDİ, yalnızca liste güncel milestone'ları/yeni checklist'i
işaret edecek şekilde GENİŞLETİLDİ.

**Reason:** Bir önceki oturumun kesintiye uğraması, kod/wiring'i tam
ama gözlemlenebilirliğini (test + doküman) yarım bıraktı — bu, "ZATEN
çalışıyor" ile "GERÇEKTEN kanıtlanmış ve belgelenmiş" arasındaki, bu
projenin her yerde reddettiği türden bir boşluktur. `record_event()`
özelinde bu boşluk özellikle riskliydi: dört gerçek karar noktasına
(admin aksiyonu, breaker trip, rollback) bağlı bir yazma yolunun "asla
etkilemez" iddiası, en az `safe_notify()`'ninki kadar güçlü bir kanıt
gerektirir.
**Alternatives:** `record_event()`'i mock'layarak fırlatmasını sağlamak
(reddedildi — çağıran taraflarda hiç try/except OLMADIĞI için bu yalnızca
"mock'un fırlattığını" kanıtlar, GERÇEK disiplinin `record_event()`'in
KENDİ iç yakalamasında yattığını KANITLAMAZ); `MAINNET_READINESS_
CHECKLIST.md`'yi bir "yakında Mainnet" yol haritası olarak yazmak
(reddedildi — SAFETY_INVARIANTS.md'nin kendisiyle ÇELİŞİRDİ).
**Consequences:** Yeni dosyalar: `tests/test_ops_event_log.py`, `tests/
test_app_ops_snapshots.py`, `MAINNET_READINESS_CHECKLIST.md`. Genişletilen
dosyalar: `tests/test_ops_dashboard.py`, `tests/test_backup_sqlite.py`,
`ARCHITECTURE.md`. Kod (`crypto_signal_engine/`, `adaptive/`,
`scripts/`) bu kararda HİÇ DEĞİŞMEDİ — yalnızca test/doküman eklendi.
Tam paket 2116 test ile TAMAMEN YEŞİL (Karar 99 sonrası taban 2082 + bu
kararın 34 yeni testi), `tests/test_repository_safety_scan.py`'nin 18
testi dahil (ayrıca izole çalıştırılıp doğrulandı).

## Karar 101 — Dashboard fiyat/P&L senkronizasyon düzeltmesi: M5-yerine-M1 fiyat kaynağı

**Date:** 2026-09-12
**Decision:** `app.py::Application._latest_price()` yalnızca SON KAPANMIŞ
M5 mumunun close'unu okuyordu — bu, `_bridge_lifecycle_snapshot()`'taki
`position_unrealized` hesaplamasının gerçek fiyat hareketinden 5 dakikaya
kadar geride kalmasına yol açıyordu ("coin yükseliyor ama kârlılık
değişmiyor" izlenimi). `execution/lifecycle_runtime.py::LifecycleRuntime`
zaten GERÇEK M1 mumlarını (stop/target/trailing için) işliyordu ve
Adaptive Intelligence v1'in shadow-fidelity düzeltmesi (Karar 87) için
opsiyonel bir `m1_candle_observer` hook'u vardı — bu hook `scripts/
run_with_adaptive_policy.py`'nin shadow monitor'ü tarafından ZATEN
kullanılıyordu, dolayısıyla `Application`'ın KENDİ M1 fiyat-cache
observer'ının onu BOZMADAN COMPOSE edilmesi gerekiyordu.

**Değişiklik (SIFIR yeni REST çağrısı, yalnızca zaten akan candle
verisi):** `Application.__init__`'e symbol -> `(close, close_time)`
tutan küçük, in-memory (persist edilmeyen, restart'ta boş başlayan)
`self._m1_price_cache` eklendi; `_record_m1_price()` bunu her GERÇEK,
kapanmış M1 mumunda günceller. YENİ modül-seviyesi `_compose_m1_candle_
observers(a, b)`, `LifecycleRuntime`'a geçirilen `m1_candle_observer`'ı
`self._record_m1_price` ile (varsa) DIŞARIDAN verilen observer'ı (`scripts/
run_with_adaptive_policy.py`'nin shadow monitor'ü) TEK bir callable'a
compose eder — her ikisi BAĞIMSIZ `try/except` ile korunur (biri
patlarsa diğeri ETKİLENMEZ; her ikisi de aynı candle için ÇAĞRILIR).
`_latest_price()`, `_latest_price_source()` (YENİ, `(price, "M1"|"M5",
as_of)` döner) üzerinden implemente edildi: M1 cache'te bir kayıt VARSA
VE M5'in kendi son mumundan `close_time`'a göre daha YENİYSE o tercih
edilir, aksi halde ESKİ M5 davranışına BİREBİR (byte-for-byte) düşülür.
`_bridge_lifecycle_snapshot()`'taki her pozisyon dict'ine additive
`latest_price_source`/`latest_price_as_of` alanları eklendi (mevcut
`latest_price`/`unrealized_gross_pnl` alanlarının kendisi/None-luğu
DEĞİŞMEDİ). Dashboard'a (`ops/dashboard.py`, hem sunucu-taraflı Python
`_build_view_model()` hem de canlı-yenileme için AYNI şekli üreten JS
`buildViewModel()` — ikisi de güncellendi) fiyatın "M1, 12sn önce" / "M5,
3dk önce" şeklinde ne kadar taze olduğunu gösteren küçük bir etiket
eklendi (`#cpPriceFreshness`, seçili coin'in fiyat panelinde).

**Davranış değişikliği notu:** `LifecycleRuntime`'ın kendi `m1_candle_
observer`'ı artık VARSAYILAN OLARAK `None` DEĞİLDİR — `Application` HER
ZAMAN kendi `_record_m1_price`'ını takar (dışarıdan bir observer
verilmese bile), böylece M1 fiyat cache'i her zaman beslenir.
`tests/test_app_adaptive_wiring.py::TestM1CandleObserverWiring`'in iki
testi bu YENİ (ama doğru) beklentiye güncellendi — dışarıdan verilen
observer'ın KENDİSİ hâlâ `None` varsayılanlı ve opsiyoneldir, değişen
yalnızca `LifecycleRuntime`'a ULAŞAN composed observer'ın artık asla
`None` olmaması.

**Manuel doğrulama (commit'ten ÖNCE, gerçek çalışan süreçle):**
PAPER + Signal->Testnet bridge açık (fake `FakeTestnetHttpClient`, sıfır
gerçek ağ çağrısı), `LifecycleStore`'a doğrudan bir LONG BTCUSDT
pozisyonu (`gross_entry_vwap=95000.0`) yazılıp `_record_m1_price()` ile
M1 cache'e `close=97012.34` beslenerek dashboard `127.0.0.1:8788`'de
GERÇEKTEN çalıştırıldı (zaten çalışan, dokunulmayan production instance
`127.0.0.1:8787`'den TAMAMEN ayrı). `curl http://127.0.0.1:8788/api/
status` -> `bridge_lifecycle.positions.BTCUSDT` içinde `"latest_price":
97012.34, "latest_price_source": "M1", "latest_price_as_of":
"2026-09-12T17:38:06.823838+00:00", "unrealized_gross_pnl":
20.1234...` GERÇEKTEN gözlemlendi (M5-only eski davranışla bu pozisyon
NEGATİF görünürdü). `curl http://127.0.0.1:8788/` ile alınan render
edilmiş HTML'in `window.__DASHBOARD_DATA__`'ında AYNI alanlar birebir
göründü; HTML kaynağında `id="cpPriceFreshness"` elementi ve
`priceFreshnessText()` fonksiyonunun `renderChart()`'a bağlı olduğu
doğrulandı. Doğrulama sonrası test süreci temiz kapatıldı, geçici
dosyalar silindi, production instance (`127.0.0.1:8787`) etkilenmediği
`/healthz` ile teyit edildi.
**Reason:** Kullanıcı doğrudan gözlemledi ("coin yükseliyor ama kârlılık
değişmiyor") — kök neden zaten M1/M5 granülerlik farkıydı; M1 mumları
ZATEN akıyordu (lifecycle evaluator için), tek eksik onları dashboard'un
mark-to-market hesabına da (yeni bir veri kaynağı İCAT ETMEDEN) bağlamaktı.
**Alternatives:** `_latest_price()`'a yeni bir REST/websocket fiyat
sorgusu eklemek (reddedildi — görev AÇIKÇA "yeni REST çağrısı yok"
kısıtlıyordu, ayrıca zaten akan M1 verisiyle gereksiz); M1 hook'u
shadow monitor'den ALIP tamamen fiyat-cache'e AYIRMAK (reddedildi —
iki bağımsız amaç için AYRI hook'lar İCAT etmek yerine, ZATEN var olan
TEK hook'u compose etmek daha az yüzey alanı bırakır).
**Consequences:** Değişen dosyalar: `crypto_signal_engine/app.py`
(`_m1_price_cache`, `_record_m1_price`, `_latest_closed_m5_candle`,
`_latest_price_source`, `_compose_m1_candle_observers`, `_bridge_
lifecycle_snapshot()`'a additive alanlar), `crypto_signal_engine/ops/
dashboard.py` (Python `_build_view_model` + JS `buildViewModel` +
`#cpPriceFreshness` etiketi). YENİ test dosyası: `tests/test_app_price_
freshness.py`. Güncellenen test dosyası: `tests/test_app_adaptive_
wiring.py` (`TestM1CandleObserverWiring`'in iki testi, yeni "her zaman
composed" davranışına). Tam paket 2129 test ile TAMAMEN YEŞİL (Karar 100
sonrası taban 2116 + bu kararın 13 yeni testi), 1 skip (Karar 100'den
DEĞİŞMEDEN).

## Karar 102 — Dashboard görsel/tema düzeltmesi: koyu lacivert + amber/mavi palet, mum grafiği kalitesi, crosshair yatay çizgi + fiyat etiketi

**Date:** 2026-09-12
**Decision:** SADECE görsel/CSS + salt-sunum JS (`renderChart()`'ın
çizim kodu) değişikliği — `_build_view_model()`, `/api/status`, admin
endpoint'leri, JS polling/modal/admin mantığı (`TestReadOnlySafety`,
`TestControlCenterAdminEndpoints`, `TestJsonEmbeddingSafety` vb.
testlerin kapsadığı HİÇBİR şey) DOKUNULMADI. Üç bağımsız alt-değişiklik:

**(1) Renk paleti:** `:root` CSS değişkenleri (`crypto_signal_engine/
ops/dashboard.py::_PAGE_STYLE` VE `ops/dashboard_design_reference.html`
— ikisi de, referans tutarlılığı için) eski koyu mor/turkuaz (`#8B7CF6`/
`#33D6C4`) paletten koyu lacivert/siyah zemin (`--bg:#0A0B10`) + turuncu-
amber birincil vurgu (`--accent:#F2994A`) + mavi ikincil/nötr bilgi rengi
(`--accent-2:#4C8DFF`) paletine geçirildi. YENİ `--accent-rgb`/`--accent-
2-rgb` değişkenleri eklendi (`rgba(var(--accent-rgb),X)` deseni) — daha
önce HER rgba() çağrısında AYRI AYRI gömülü olan mor RGB üçlüsü artık
TEK bir kaynaktan türetiliyor (gelecekte tekrar bir palet değişikliği
gerekirse tek satır değişir). `--radius-card` 16px'ten 18px'e çıkarıldı
(daha yuvarlak köşeler). `--shadow`'a düşük-opasiteli bir `rgba(var(
--accent-rgb),.05)` halkası eklendi — TÜM kartlar (`.kpi`, `.chart-
panel`, `.coin-list`, `.scan-tile`, `.svc-card` vb., hiçbiri TEK TEK
DEĞİŞTİRİLMEDEN) artık hafif bir "border-glow" ile ayrışıyor. Kâr/zarar
renklendirmesi (`--success`/`--danger`) YEŞİL/KIRMIZI semantiği
KORUNARAK yeni palete uyarlandı (`#22C55E`/`#FF5A5F` — okunabilirlik ve
kontrast için hafif tazelendi, anlam DEĞİŞMEDİ). Sembol avatarlarının
rotasyonlu renk dizisi (`var palette=[...]`) da eski mor/turkuaz'ı
İÇERMEYECEK şekilde tazelendi (görsel çeşitlilik amaçlı, semantik
DEĞİLDİR).
**(2) Mum grafiği kalitesi:** `renderChart()`'ın candlestick çizimi
(SVG, `rect`/`line`, harici kütüphane YOK, DEĞİŞTİRİLMEDEN) — gövde
genişliği `cw*0.64` -> `cw*0.68` (daha dolgun gövde/fitil oranı), gövde
köşe yarıçapı `1.2` -> `1.5`, minimum gövde yüksekliği `1.4` -> `1.6`
(doji mumlarda görünürlük), fitil kalınlığı `1.2` -> `1` + `opacity:.9`
(gövdeye göre daha ince, kontrast için), gövdeye ince bir koyu outline
(`stroke:rgba(5,6,10,.4)`) eklendi (kenar netliği). Sıfır veri/mantık
değişikliği — yalnızca ÇİZİM parametreleri.
**(3) Crosshair düzeltmesi (fonksiyonel eksiklik):** Önceden yalnızca
DİKEY (mum'a snap edilmiş) noktalı çizgi vardı; artık mouse'un GERÇEK Y
pozisyonundan (mum'a snap edilmeden, `y()` fonksiyonunun tersiyle
`vmin`/`vmax`'tan fiyata çevrilerek) YATAY bir noktalı çizgi + sağ fiyat
ekseninde arka planlı bir fiyat etiketi (`priceLabelBg`/`priceLabelText`)
eklendi — standart trading-chart crosshair davranışı (dikey+yatay+fiyat
ekseni etiketi). Her iki çizgi de artık NÖTR mavi (`accent2`) renginde
(önceden dikey çizgi `var(--ink-3)` idi) — kâr/zarar/referans-çizgisi
renkleriyle (yeşil/kırmızı/amber) KARIŞMAMASI için bilinçli seçim,
"mavi = nötr bilgi" palet ilkesiyle tutarlı. `dashboard_design_
reference.html`'in KENDİ (hacim çubuklu, ayrı) `renderChart()`'ına da
AYNI değişiklik uygulandı (yatay çizgi/etiket hacim alanının ÜSTÜNDEKİ
fiyat bölgesiyle sınırlı — `volY`'ye clamp edilir).
**Reason:** Kullanıcı doğrudan geri bildirimi: "mevcut tema çok koyu/
okunaksız", mum çizimi "kaliteli/temiz görünmeli", ve crosshair'de
yatay çizginin EKSİK olması ("kullanıcı mouse'u herhangi bir noktaya
götürdüğünde ... o noktadaki fiyat seviyesinden yatay bir çizgi de
çıksın") gerçek bir fonksiyonel eksiklikti, salt estetik değil — yatay
çizgi olmadan hover edilen bir noktanın geçmiş fiyatlara göre yüksek mi
düşük mü olduğu tek bakışta GÖRÜLEMİYORDU.
**Alternatives:** Harici bir charting kütüphanesi (örn. lightweight-
charts) eklemek (reddedildi — bu proje `http.server`/stdlib-only, sıfır
yeni JS bağımlılığı disiplinini her yerde korur; mevcut el-yapımı SVG
zaten yeterli esneklikte); crosshair fiyat çizgisini en yakın mum
kapanışına snap etmek (reddedildi — görev AÇIKÇA "mouse'un olduğu
NOKTADAKİ fiyat seviyesi" istiyordu, dikey çizgi zaten mum'a snap
ediyor, ikisinin farklı davranması KASITLI).
**Consequences:** Değişen dosyalar: `crypto_signal_engine/ops/
dashboard.py` (`_PAGE_STYLE`, `renderChart()`), `ops/dashboard_design_
reference.html` (AYNI palet + candle + crosshair, referans tutarlılığı
için). Kod/veri/API/JS-iş-mantığı dosyalarının HİÇBİRİ değişmedi. Tam
paket 2129 test ile TAMAMEN YEŞİL — Karar 101'den beri SIFIR yeni/
kırılan test (beklenen: bu değişiklik CSS-değeri/çizim-parametresi
seviyesindedir, hiçbir test bu değerlere karşı assert etmiyordu; `tests/
test_ops_dashboard.py`'nin 45 testi, dahil `TestReadOnlySafety`/
`TestJsonEmbeddingSafety`/Control Center admin testleri, izole
çalıştırılıp AYRICA doğrulandı). Bot YENİDEN BAŞLATILMADI (kullanıcı
talebi — fiyat düzeltmesi + bu görsel değişiklikler birlikte, ayrı bir
adımda test edilecek).


=== FILE: MAINNET_READINESS_CHECKLIST.md ===
# MAINNET_READINESS_CHECKLIST.md

## Bu doküman NEDİR, NE DEĞİLDİR

Bu bir yol haritası, taahhüt, veya "Faz 13" İLANI DEĞİLDİR — Mainnet
desteği bu proje için hâlâ PLANLANMAMIŞTIR (bkz. ARCHITECTURE.md, "Faz 12
kapsamı DIŞINDA" notu). Bu, yalnızca DÜRÜST bir envanterdir: sistemin şu
ANKİ gerçek durumuna göre, gerçek parayla Mainnet'te çalıştırmadan ÖNCE
neyin KESİNLİKLE gerekeceğinin bir listesi — hiçbiri şu an TAMAMLANMIŞ
DEĞİLDİR, hiçbiri şu an ÜZERİNDE ÇALIŞILMIYOR. Amaç, "biz aslında ne
kadar uzaktayız" sorusuna, gelecekte biri sorduğunda, UYDURMA bir cevap
yerine bu listeye bakarak dürüst bir cevap verebilmektir.

Bu proje bugün YALNIZCA iki `ExecutionMode`'da çalışır: `PAPER` ve
`BINANCE_SPOT_TESTNET` (bkz. SAFETY_INVARIANTS.md #16). `MAINNET` bir
enum üyesi bile DEĞİLDİR.

## Şu anki durum: Mainnet execution YAPISAL OLARAK İMKANSIZ (kasıtlı)

Bu bir eksiklik değil, BİLİNÇLİ bir güvenlik tasarımıdır — kaldırılması
gereken, dağınık "henüz yapılmadı" maddeleri değil, AÇIKÇA kaldırılması
GEREKEN, kasıtlı olarak inşa edilmiş engellerdir:

- `ALLOW_LIVE_TRADING = False` — `crypto_signal_engine/__init__.py`
  içinde hard invariant; kod içinde hiçbir yerde `True` yapılamaz
  (SAFETY_INVARIANTS.md #1).
- `BinanceTestnetConfig.__post_init__` her inşa edilişte
  `validate_testnet_host()`'u çağırır: hostname TAM OLARAK
  `testnet.binance.vision` olmak ZORUNDADIR; `api.binance.com`
  (Mainnet) dahil her şey `UnsafeExecutionHostError` ile REDDEDİLİR
  (SAFETY_INVARIANTS.md #15).
- `ExecutionMode` enum'unun yalnızca İKİ üyesi vardır: `PAPER`,
  `BINANCE_SPOT_TESTNET`. `ExecutionMode("MAINNET")` her zaman
  `ValueError` fırlatır (SAFETY_INVARIANTS.md #16).
- `BinanceLiveExecutionAdapter` diye bir sınıf repoda YOKTUR ve
  yazılmayacaktır (SAFETY_INVARIANTS.md #2).
- `tests/test_repository_safety_scan.py` bunların HİÇBİRİNİN
  gelecekte sessizce delinmediğini her test koşusunda otomatik olarak
  doğrular (host allowlist, `ALLOW_LIVE_TRADING` backdoor taraması,
  `execution/` sınırı dışında credential/private-endpoint izi taraması).

**Sonuç:** Mainnet'e geçmek "bir config bayrağını çevirmek" DEĞİLDİR —
yukarıdaki invariant'ların HER BİRİNİN, ayrı, açık, incelenmiş bir kararla
BİLİNÇLİ OLARAK gevşetilmesini gerektirir. Bu doküman o kararın NASIL
verileceğini değil, o karar verilirse önce nelerin GERÇEKTEN eksik
olduğunu listeler.

## Kategori 1 — Execution güvenliği (şu an: TESTNET-seviyesinde var, Mainnet-seviyesinde YOK)

- [ ] Mainnet host'a bağlanmayı bilinçli olarak İZİN VEREN, ayrı ve
      açıkça etiketlenmiş bir `ExecutionMode` (bugünkü `BINANCE_SPOT_
      TESTNET`'in BİREBİR kopyası olamaz — ayrı bir risk sınıfı).
- [ ] Gerçek fon kaybı riskine göre YENİDEN değerlendirilmiş
      `RiskPolicyConfig` limitleri (`max_open_positions`,
      `max_total_exposure_usdt`, `daily_loss_limit_usdt`) — bugünkü
      varsayılanlar TESTNET/paper sermayesi için seçildi, gerçek sermaye
      için asla doğrudan kopyalanmamalı.
- [ ] Mainnet API anahtarları için AYRI bir credential deposu/rotasyon
      politikası — bugünkü `BINANCE_TESTNET_API_KEY`/`_SECRET` ortam
      değişkeni deseni tek başına gerçek fon erişimi için yeterli
      görülmemeli (bkz. Kategori 4 — sır yönetimi).
- [ ] Withdrawal/transfer API scope'unun API anahtarında KESİNLİKLE
      devre dışı olduğunun harici doğrulaması (yalnızca spot trade
      scope) — bugün bu yalnızca dokümantasyon seviyesinde bir varsayım.
- [ ] "Force-close position"/acil manuel müdahale için ağa açık,
      token-gated bir yol (Karar 99'da AÇIKÇA reddedildi/ertelendi —
      Mainnet'te bu artık gerçek para demektir, ayrı bir güvenlik
      incelemesi gerektirir).

## Kategori 2 — Test/doğrulama bar'ı (şu an: TESTNET replay/lab ile sınırlı)

- [ ] Gerçek Mainnet piyasa koşullarına karşı (yalnızca TESTNET'in
      düşük likidite/farklı spread karakteristiği değil) uzun süreli,
      canlı bir paralel-gözlem periyodu (öneri: en az birkaç hafta,
      SIFIR gerçek emir, yalnızca public market data + sinyal/karar
      kaydı karşılaştırması).
- [ ] `research/` paketinin (historical replay, OOS stability, Monte
      Carlo robustness) Mainnet-benzeri gerçek volatilite/slipaj
      varsayımlarıyla YENİDEN çalıştırılması — bugünkü sonuçlar TESTNET/
      paper varsayımlarıyla üretildi.
- [ ] Bağımsız, adversarial bir güvenlik incelemesi — spesifik olarak
      "Mainnet host'a geçişin AÇTIĞI yeni saldırı yüzeyi" odaklı (host
      allowlist'in gevşetilmesinin KENDİSİ yeni bir risk sınıfıdır).

## Kategori 3 — Operasyonel hazırlık (şu an: 24/7 Ops v1 ile TESTNET/paper seviyesinde var)

- [x] Otomatik yedekleme, systemd watchdog, outbound alerting, Control
      Center pause/resume/stop (Karar 99 — zaten mevcut, execution
      mode'dan BAĞIMSIZ çalışır, bu yüzden Mainnet'e geçişte YENİDEN
      İCAT edilmesi gerekmez).
- [ ] Bir olay (rollback, breaker trip, admin stop) sonrası kim/nasıl
      müdahale eder — bugün outbound bildirim VAR (Telegram), ama
      resmi bir incident-response prosedürü/nöbet planı YOK.
- [ ] Gerçek fon için ayrı bir restore/felaket kurtarma tatbikatı
      (bugünkü restore prosedürü — bkz. PHASE8_UBUNTU_OPERATIONS.md —
      yalnızca paper/testnet state'i için tatbik edildi).

## Kategori 4 — Sır yönetimi ve erişim kontrolü (şu an: ortam değişkeni deseni, TESTNET için yeterli)

- [ ] Gerçek Mainnet API anahtarları için ortam değişkeninden DAHA
      güçlü bir depo (örn. bir secrets manager/HSM) — bugünkü desen
      (`BINANCE_TESTNET_API_KEY`/`_SECRET` ortam değişkeni, hiçbir
      zaman loglanmaz/diske yazılmaz, bkz. SAFETY_INVARIANTS.md #17)
      TESTNET riski için yeterli kabul edildi, gerçek fon riski için
      YENİDEN değerlendirilmeli.
- [ ] `dashboard_admin_token`'ın Mainnet'te taşıdığı riskin yeniden
      değerlendirilmesi — bugün bu token yalnızca pause/resume/stop
      yapabilir (order-submission YOK), ama Mainnet'te "stop" bile
      gerçek açık pozisyonları etkileyen bir operasyonel karardır.

## Kategori 5 — Yasal/uyumluluk (şu an: hiç ele alınmadı — proje kapsamı dışı kaldı)

- [ ] Hangi yargı alanında, hangi tüzel/gerçek kişi adına, hangi
      düzenleyici çerçevede çalışılacağının netleştirilmesi — bu proje
      şimdiye kadar hiçbir aşamada bunu ele ALMADI (kasıtlı, kapsam
      dışı bırakıldı).
- [ ] Vergi/muhasebe kaydı gereksinimlerinin `portfolio/accounting.py`
      (Karar 97) ile karşılaştırılması — bugünkü modül yalnızca
      GÖZLEM/dashboard amaçlı, resmi muhasebe kaydı OLARAK
      tasarlanmadı.

## Bu listeyi nasıl kullanmalı

Bu, yukarıdan aşağı sırayla "tamamlanacak" bir TODO listesi değildir —
her madde kendi başına AYRI bir karar/görev gerektirir ve bazıları
(Kategori 5 gibi) bu projenin teknik kapsamının tamamen DIŞINDADIR. Bu
liste yalnızca ŞU sorunun dürüst cevabıdır: "bugün Mainnet'e ne kadar
uzağız?" — cevap: **çok uzağız, ve bu KASITLI**. Herhangi bir gelecekteki
Mainnet kararı, bu listenin GÜNCELLENMESİYLE başlamalı, var olan
maddelerin sessizce atlanmasıyla DEĞİL.


=== FILE: PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md ===
# FAZ 12 — Final Local Production Readiness

Bu belge, Faz 1-11'in kabul edilmiş sistemini birkaç haftalık kesintisiz
YEREL (Ubuntu/WSL) çalıştırma için hazır hale getiren son fazı açıklar.
**Bu Faz Mainnet üretim DEĞİLDİR.** `ALLOW_LIVE_TRADING` `False` kalır,
Mainnet private execution YAPISAL OLARAK İMKANSIZDIR (Faz 10'un host
allowlist'i DEĞİŞTİRİLMEDEN korunur).

Beklenen gerçek-dünya sırası (bu Faz'dan SONRA):

- **Stage A**: yerel 24/7 PUBLIC Binance + PAPER trading, birkaç hafta gözlem.
- **Stage B**: yerel Binance Spot TESTNET execution'ın etkinleştirilmesi,
  reconciliation'ın gözlemlenmesi.
- **Stage C**: yalnızca Stage A/B'den yerel istikrar kanıtı SONRA, bir VPS
  düşünülür (bu Faz'ın KAPSAMI DIŞINDA).

Mainnet, bu projenin readiness kilometre taşının DIŞINDA kalır.

## 1. Üretim kompozisyonu

`crypto_signal_engine/app.py::Application`, Faz 8'de zaten kurulmuş
kompozisyon kökünün ÜZERİNE inşa edilir — YENİDEN TASARLANMAZ:

```
PUBLIC Binance data
  -> Faz 2 market data (BinanceMarketDataProvider)
  -> Faz 3 features (FeatureEngine)
  -> Faz 4 signals (SignalEngine)
  -> Faz 5 paper trading (PaperTradingEngine)
  -> Faz 6 runtime (RuntimeCoordinator)
  -> Faz 7 persistence/recovery (PaperStateStore, PersistedRuntime)
  -> Faz 8 operations (AppConfig, ProcessLock, health snapshot)
  -> Faz 9 stability (HealthMonitor, reconnect/gap/stale-data handling)
```

Faz 12'nin EKLEDİĞİ TEK şey: execution-mode gating + startup reconciliation
(Bölüm 2-3) ve salt-okunur bir dashboard (Bölüm 4). Hiçbir Faz 1-11 iş
mantığı KOPYALANMADI/tekrar YAZILMADI.

`ExecutionMode` (Faz 10'dan DEĞİŞTİRİLMEDEN): yalnızca `PAPER` ve
`BINANCE_SPOT_TESTNET`. `MAINNET` bir enum üyesi DEĞİLDİR ve OLAMAZ.
**Varsayılan: `PAPER`.**

## 2. TESTNET runtime gating

`CSE_EXECUTION_MODE` (varsayılan `PAPER`) ve `CSE_ENABLE_TESTNET_EXECUTION`
(varsayılan `false`) — YENİ iki `AppConfig` alanı
(`crypto_signal_engine/ops/config.py`). Testnet SADECE HER İKİSİ de doğru
olduğunda "etkin" sayılır:

```
CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET
CSE_ENABLE_TESTNET_EXECUTION=true
```

artı geçerli `BINANCE_TESTNET_API_KEY`/`BINANCE_TESTNET_API_SECRET`
(Faz 10/11 ile AYNI ortam değişkenleri). Bunlardan HERHANGİ biri eksikse
(mode PAPER, veya enable bayrağı yok), `Application`, hiçbir execution
store/client/service İNŞA ETMEZ (`app._execution_service is None`,
`app._execution_store is None`) — bu, "yanlışlıkla bir TESTNET order
gönderme" için erişilebilir HİÇBİR kod yolunun bulunmadığını YAPISAL
olarak garanti eder (bkz. `tests/test_phase12_local_production_readiness.py::TestTestnetGating`).

**Blocker-seviyesi karar (bilinçli sınırlama, bkz. Bölüm 15 "known
non-blocking limitations"):** bu Faz, otomatik bir sürekli
Signal->TESTNET execution ENTEGRASYONU İÇERMEZ. `RuntimeCoordinator`'ın
(Faz 6) sinyal-değerlendirme kod yolu senkron ve KABUL EDİLMİŞTİR;
`ExecutionReconciliationService.submit()` (Faz 11) ASENKRON ağ I/O
yapar. Bu ikisini "no redesign" kısıtı ALTINDA güvenli bir şekilde
köprülemek (backpressure, submit() gecikmesi runtime'ı bloke etmeden,
signal-context idempotency ile execution-context idempotency'nin doğru
eşlenmesi, vb.) BAŞLI BAŞINA mimari bir tasarım kararı gerektirir — bu
Faz'ın "sadece blocker düzelt, redesign YAPMA" ilkesiyle ÇELİŞİR. Bu
YÜZDEN kasıtlı olarak ERTELENMİŞTİR ve BLOCKER olarak RAPORLANMIŞTIR
(görev tanımının izin verdiği açık kaçış: "If implementing this
integration would require unsafe architectural shortcuts, do NOT force
it. Report it as a blocker instead"). Zaten beklenen operasyon planı
(Stage B) da bunu GEREKTİRMEZ: TESTNET gözlemi `scripts/
binance_testnet_lab.py`'nin KASITLI/MANUEL CLI'sı üzerinden yapılır
(Faz 10/11'de ZATEN kabul edilmiş).

Faz 12'nin TESTNET tarafında GERÇEKTEN yaptığı: (1) opt-in gating
(yukarıda), (2) startup reconciliation (Bölüm 3), (3) doctor/dashboard
görünürlüğü (Bölüm 4-5). Otomatik order gönderimi İÇİN erişilebilir
HİÇBİR yeni kod yolu YOKTUR — `ExecutionReconciliationService`'e erişim
HÂLÂ yalnızca `scripts/binance_testnet_lab.py` (manuel CLI) üzerinden
mümkündür (Faz 11 invariant'ı DEĞİŞMEDEN).

## 3. Startup reconciliation

TESTNET etkinse, `Application.start()`, recovery'den SONRA, run/health
loop'ları BAŞLAMADAN ÖNCE `ExecutionReconciliationService.reconcile_pending()`'i
çalıştırır (`app.py::Application._reconcile_execution_startup()`). Bu:

1. **BLOCKER FİX (Karar 81 — bağımsız acceptance review bulgusu):**
   HERHANGİ bir ağ çağrısından ÖNCE, `execution/factory.py::
   testnet_credentials_status()` ile kimlik bilgisi VARLIĞI kontrol
   edilir (DEĞERİ ASLA okunmaz/loglanmaz — yalnızca SET/UNSET). İKİSİ de
   `SET` DEĞİLSE, `reconcile_pending()` HİÇ ÇAĞRILMAZ — sıfır ağ çağrısı
   — ve `execution_ready` FAIL-CLOSED `False` KALIR. Bu kontrol,
   "bekleyen bir signed API çağrısına GÜVENME" ilkesini karşılar: Durable
   execution DB BOŞKEN (hiçbir bekleyen kayıt yokken),
   `reconcile_pending()` zaten SIFIR signed çağrı yapardı — bu yüzden
   kimlik bilgisi eksikliği ESKİDEN yalnızca bir signed çağrı GERÇEKTEN
   denenirse keşfedilirdi, bu da "boş bir DB + eksik kimlik bilgisi" ile
   `execution_ready=True` YANLIŞ pozitifine yol AÇIYORDU (bkz. DECISIONS.md
   Karar 81).
2. Durable execution store'u açar (`ExecutionStateStore`).
3. TÜM bekleyen (`AMBIGUOUS`/`UNKNOWN_NOT_FOUND`/`ACKNOWLEDGED`/
   `PARTIALLY_FILLED`/`SUBMISSION_ATTEMPTED`) kayıtları exchange'e karşı
   YENİDEN SORGULAR (Faz 11'in `reconcile_pending()`'i — DEĞİŞTİRİLMEDEN).
4. `UNKNOWN_NOT_FOUND` DAHİL hiçbir durum otomatik bir ikinci `POST`'a
   YOL AÇMAZ (Faz 11 Karar 78 — DEĞİŞTİRİLMEDEN).
5. YALNIZCA credential-preflight BAŞARILI OLDUKTAN VE reconciliation
   BAŞARIYLA tamamlandıktan SONRA `app._execution_ready = True` olur.

Reconciliation BAŞARISIZ olursa (örn. kimlik bilgisi eksik, transport
hatası): `_execution_ready = False` olarak FAIL-CLOSED kalır, detay
loglanır/health-snapshot'a yansır — ama PUBLIC market-data/PAPER runtime
ASLA bu yüzden durdurulmaz (Bölüm 11'in son paragrafı: "PUBLIC market
data and observational components may remain operational... private
execution must stay disabled"). Zaten bu Faz'da otomatik bir private
execution kod yolu YOK — bu yüzden "private execution disabled" doğal
olarak HER ZAMAN doğrudur; bu adım pratikte önceki bir CLI oturumundan
kalan kayıtların restart-recovery ruhuyla erken çözülmesini sağlar.

Kanıt (offline test): `tests/test_phase12_local_production_readiness.py::TestStartupReconciliation`
— bekleyen bir `UNKNOWN_NOT_FOUND` kaydı, startup'ta YENİDEN sorgulanır,
TOPLAM POST sayısı SIFIR kalır; restart sonrası AYNI garanti tekrar
doğrulanır. Credential-preflight fix'i için: `tests/
test_phase12_local_production_readiness.py::TestCredentialPreflightBlockerFix`
— boş DB + eksik/kısmi kimlik bilgisi HİÇBİR ZAMAN `execution_ready=True`
üretmez (sıfır ağ çağrısı ile), TAM kimlik bilgisi + boş DB BAŞARIYLA
`True` olur, kimlik bilgisi değerleri hiçbir zaman `_execution_detail`'e
(dolayısıyla log/dashboard'a) SIZMAZ.

## 4. `doctor` — startup preflight

```bash
python -m crypto_signal_engine.app doctor
```

TAMAMEN offline (hiçbir ağ çağrısı, hiçbir order). Doğrular: config
parse, state dizini yazılabilirliği, SQLite açma+şema uyumluluğu,
singleton lock uygunluğu, execution mode + PAPER/TESTNET gating durumu,
TESTNET seçiliyse kimlik bilgisi VARLIĞI (asla DEĞERİ) + host tam
eşleşmesi, Mainnet'in yapısal olarak temsil EDİLEMEZ olduğu, dashboard
bind'inin loopback olduğu, ve HİÇBİR secret DEĞERİNİN yazdırılmadığı.
Her satır `[PASS]`/`[FAIL]`/`[WARN]`/`[INFO]` ile etiketlenir; exit code
0 = tüm kontroller geçti, 1 = en az biri başarısız.

## 5. Salt-okunur yerel dashboard

`crypto_signal_engine/ops/dashboard.py` — stdlib-only (`http.server`),
YENİ bir bağımlılık EKLEMEZ. Varsayılan bind: `127.0.0.1:8787`
(`CSE_DASHBOARD_HOST`/`CSE_DASHBOARD_PORT`, `CSE_DASHBOARD_ENABLED=true`
varsayılan). Gösterir: genel sağlık, uptime/başlangıç zamanı, execution
mode (PAPER/TESTNET) + hazır/enabled durumu, runtime READY/DEGRADED/
BOOTSTRAPPING, semboller, son market-data zaman damgaları, sembol başına
son tamamlanmış sinyal, paper pozisyon + realized PnL, bekleyen
reconciliation sayısı, son reconciliation zamanı/detayı.

**Asla göstermez:** API key, API secret, signature, ham imzalı istek,
credential ortam değeri. **Hiçbir** mutasyon endpoint'i YOKTUR — yalnızca
`GET` desteklenir; `POST`/`PUT`/`DELETE`/`PATCH` HER ZAMAN `405` döner
(bkz. `_DashboardRequestHandler`). Buy/sell/cancel/order-submission/
config-mutation butonu/endpoint'i YOKTUR.

**Veri kaynağı — YALNIZCA disk üzerindeki atomik JSON anlık görüntüsü**
(`ops/health_snapshot.py::write_snapshot()`, Faz 8'den DEĞİŞTİRİLMEDEN
genişletilmiş: yeni `execution`/`paper` alanları eklendi). Dashboard'un
HTTP handler'ları (`ThreadingHTTPServer`, ayrı OS thread'leri) canlı
Python nesnelerine (paper engine, coordinator, execution store) ASLA
doğrudan ERİŞMEZ — bu, cross-thread race/lock riskini YAPISAL OLARAK
ORTADAN KALDIRIR ve "dashboard failure never kills the runtime"
garantisini basitleştirir: dashboard bir istek için ÇÖKSE bile, ana
asyncio event loop'u/runtime'ı hiçbir şekilde ETKİLEMEZ (ayrı thread,
ayrı hata sınırı). Dashboard PORT ÇAKIŞMASI (bind hatası) bile
`Application.start()`'ı BAŞARISIZ KILMAZ — yalnızca dashboard devre dışı
bırakılır, runtime NORMAL devam eder (`tests/
test_phase12_local_production_readiness.py::TestDashboardSafety::test_dashboard_bind_failure_does_not_stop_runtime`).

## 6. Ağ güvenliği — telefon/Tailscale operasyon modeli

Varsayılan bind `127.0.0.1`dir. Bu proje HİÇBİR router port-forwarding
talimatı İÇERMEZ, `0.0.0.0`'ı ASLA ZORUNLU KILMAZ, ve hiçbir
kimliksiz genel internet erişimi ÖNERMEZ.

Planlanan telefon-erişim yöntemi:

```
Laptop (127.0.0.1:8787 dashboard)
  -> Tailscale özel tailnet
  -> Telefon (Tailscale uygulaması ile aynı tailnet'te)
```

Pratik adımlar (özet — resmi Tailscale dokümantasyonuna bakın):
1. Laptop'ta Tailscale kurun ve giriş yapın (`tailscale up`).
2. Dashboard'u normal şekilde başlatın (varsayılan `127.0.0.1:8787`).
3. `tailscale serve https / http://127.0.0.1:8787` (veya eşdeğer bir
   Tailscale Serve/Funnel-DIŞI komut — **Funnel KULLANMAYIN**, o genel
   internete açar; yalnızca `serve`, tailnet'inizin İÇİNDE kalır)
   çalıştırarak bu localhost portunu tailnet'inize proxy'leyin.
4. Telefonunuzda Tailscale uygulamasını AÇIK/bağlı tutun, laptop'ınızın
   Tailscale adını (örn. `https://laptop-adi.your-tailnet.ts.net`)
   ziyaret edin.

`tailscale` HİÇBİR ZAMAN bir Python bağımlılığı OLARAK eklenmez —
`pyproject.toml`'a dokunulmadı, tamamen OS-seviyesi, opsiyonel bir araçtır.

## 7. Uzun-süreli yerel operasyon

Faz 8/9'dan DEĞİŞTİRİLMEDEN korunan, Faz 12'nin ÜZERİNE inşa ettiği
mekanizmalar:

- **Graceful shutdown**: SIGINT/SIGTERM, `Application.request_shutdown()`
  (idempotent) — Faz 8'den DEĞİŞTİRİLMEDEN; Faz 12 dashboard'u da bu
  sıraya eklendi (`_graceful_stop()`, runtime/health-loop'tan ÖNCE
  durdurulur).
- **Singleton process protection**: `ProcessLock` (flock), Faz 8'den
  DEĞİŞTİRİLMEDEN.
- **Restart recovery**: `PersistedRuntime.recover()`, Faz 7'den
  DEĞİŞTİRİLMEDEN; TESTNET etkinse Faz 12'nin startup reconciliation'ı
  (Bölüm 3) buna EK olarak, recovery'den SONRA, run loop'tan ÖNCE çalışır.
- **Log rotasyonu**: `journald`/systemd altında `journalctl`'in KENDİ
  rotasyonu (Faz 8, Bölüm 6); yerel (non-systemd) WSL kullanımında
  stdout bir dosyaya yönlendiriliyorsa `logrotate` VEYA basitçe periyodik
  `>> app.log 2>&1` yerine `| tee -a` + harici `logrotate.d` girdisi
  ÖNERİLİR (bu proje kendi log-rotasyon kodunu YAZMAZ — stdlib
  `logging`'in kendisi zaten dosya değil, stream'e yazar; Bölüm 9'daki
  `start`/`stop` komutları bunu gösterir).
- **SQLite backup**: `scripts/backup_sqlite.py` (Faz 8'den DEĞİŞTİRİLMEDEN,
  online backup API'si kullanır) — HEM `paper_state.db` HEM (TESTNET
  etkinse) `testnet_execution.db` için ÇALIŞIR (jenerik `--source`/
  `--destination`, Faz 12 hiçbir yeni backup aracı İCAT ETMEDİ).
- **Stale health detection**: `HealthMonitor` (Faz 6/9), DEĞİŞTİRİLMEDEN.
- **Bounded in-memory history**: Faz 12'nin YENİ `RuntimeCoordinator.
  last_cycle_result()` önbelleği, sembol başına TEK bir kayıt tutar
  (üzerine yazılır, asla büyümez) — dashboard'un "son sinyal" alanı için.
- **WebSocket reconnect / REST recovery**: Faz 6/9'dan DEĞİŞTİRİLMEDEN.
- **Runaway task/temp-venv birikimi YOK**: normal `run`/`doctor`/
  `dashboard` operasyonu HİÇBİR geçici venv/dosya OLUŞTURMAZ (yalnızca
  `scripts/verify_phase1.py`, paketleme doğrulaması için, kendi geçici
  venv'ini yaratır — bu HER ZAMAN kullanımdan SONRA elle temizlenmelidir,
  bkz. bu belgenin "Bilinen sınırlamalar" bölümü DEĞİL, geliştirme
  disiplini notu).
- **Dizinler**: durable state `CSE_DB_PATH`'in dizini (varsayılan
  `var/lib/crypto-signal-engine/`), TESTNET execution db AYNI dizin
  altında `testnet_execution.db` (`BINANCE_TESTNET_EXECUTION_DB_PATH`
  ile override edilebilir, Faz 10/11'den DEĞİŞTİRİLMEDEN), loglar
  stdout/journald, health snapshot `CSE_HEALTH_SNAPSHOT_PATH`
  (varsayılan `<db_dir>/health.json`).

## 8. Güç / laptop operasyon gereksinimleri

- Laptop fişe TAKILI kalmalıdır (pil tasarrufu modu process'i
  DURDURABİLİR).
- Ekran KAPANABİLİR — process arka planda ÇALIŞMAYA DEVAM EDER (ekran
  kapanması bir process'i durdurmaz).
- **Uyku/hazırda bekletme (sleep/hibernate) ASLA olmamalıdır** çalışma
  sırasında — bu, WSL/Ubuntu process'ini DONDURUR (network bağlantıları
  KOPAR, reconnect mantığı devreye girer ama saatlerce askıda kalabilir).
  Windows Güç Seçenekleri'nde "Uyku" süresini "Hiçbir Zaman"a ayarlayın
  (bu proje Windows güç ayarlarını OTOMATİK DEĞİŞTİRMEZ — elle yapılmalı).
- WSL/Ubuntu/process aktif KALMALIDIR: bir Windows oturum kapatma/
  yeniden başlatma, WSL'i de DURDURUR — process'i bir terminal
  multiplexer'da (`tmux`/`screen`) veya bir WSL arka plan servisinde
  ÇALIŞTIRIN, sıradan bir kapatılabilir terminal penceresinde DEĞİL.
- Temiz kapatma: `Ctrl+C` (SIGINT) veya `kill -TERM <pid>` — HER İKİSİ de
  `Application`'ın idempotent, graceful shutdown sırasını (Bölüm 7)
  tetikler.
- Yeniden başlatma: aynı komutu tekrar çalıştırın (`python -m
  crypto_signal_engine.app run`) — `PersistedRuntime.recover()` kaldığı
  yerden devam eder (Faz 7).
- Sağlık/durum kontrolü: `python -m crypto_signal_engine.app status`
  VEYA dashboard (`http://127.0.0.1:8787/`).
- Yedekleme: Bölüm 9'a bakın.

## 9. Start / Stop / Status komutları

```bash
# Preflight (offline, no orders, no network)
python -m crypto_signal_engine.app doctor

# Start (foreground — bir terminal multiplexer içinde çalıştırın)
python -m crypto_signal_engine.app run

# Stop
Ctrl+C   # veya: kill -TERM <pid>

# Status
python -m crypto_signal_engine.app status

# Dashboard (varsayılan bind aktifken, ayrı komut GEREKMEZ — `run` ile
# birlikte otomatik başlar)
open http://127.0.0.1:8787/          # veya tarayıcıda ziyaret edin

# Backup (HEM paper HEM — TESTNET etkinse — execution state için)
python3 scripts/backup_sqlite.py --source var/lib/crypto-signal-engine/paper_state.db \
    --destination backups/paper_state-$(date +%F).db
python3 scripts/backup_sqlite.py --source var/lib/crypto-signal-engine/testnet_execution.db \
    --destination backups/testnet_execution-$(date +%F).db

# Restore/restart recovery: bkz. PHASE8_UBUNTU_OPERATIONS.md Bölüm 10
# (aynı prosedür — servisi durdur, dosyayı DEĞİŞTİR, yeniden başlat,
# `status` ile recovery'yi doğrula).

# PAPER mode (varsayılan — hiçbir ekstra ayar GEREKMEZ)
CSE_SYMBOLS=BTCUSDT,ETHUSDT python -m crypto_signal_engine.app run

# TESTNET mode (Stage B, yalnızca haftalarca istikrarlı PAPER
# gözleminden SONRA)
export CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET
export CSE_ENABLE_TESTNET_EXECUTION=true
export BINANCE_TESTNET_API_KEY=...      # kendi TESTNET anahtarınız
export BINANCE_TESTNET_API_SECRET=...   # kendi TESTNET secret'ınız
python -m crypto_signal_engine.app doctor   # ÖNCE doğrulayın
python -m crypto_signal_engine.app run

# Local readiness (tek komut, offline)
python3 scripts/local_readiness_check.py
```

Günlük operasyon için geliştirici bilgisi GEREKMEZ — yukarıdaki komutlar
YETERLİDİR.

## 10. Paper-first safety — kanıt

- `load_config()`'in varsayılanı `execution_mode=PAPER`'dır (`CSE_EXECUTION_MODE`
  hiç ayarlanmazsa).
- `Application.__init__`, `execution_mode != BINANCE_SPOT_TESTNET OR NOT
  enable_testnet_execution` iken `self._execution_service = None`,
  `self._execution_store = None` ATAR — bu durumda erişilebilir HİÇBİR
  execution nesnesi YOKTUR (bir eksik config/typo/restart/stale-env/
  dashboard isteği, bu yüzden order gönderemez — gönderecek NESNE
  yoktur).
- `AppConfig`'de credential alanı YOKTUR (hard invariant, Faz 8'den
  DEĞİŞTİRİLMEDEN) — PAPER modda kimlik bilgisi GEREKMEZ.
- Kanıt: `tests/test_phase12_local_production_readiness.py::TestPaperFirstSafety`.

## 11. Bilinen NON-BLOCKING sınırlamalar

- **Otomatik sürekli Signal->TESTNET execution entegrasyonu YOK**
  (Bölüm 2'de gerekçelendirildi — kasıtlı, blocker olarak raporlanmış bir
  erteleme, "no redesign" ilkesiyle ÇATIŞTIĞI için). TESTNET gözlemi
  Faz 10/11'in KENDİ manuel CLI'sı üzerinden yapılır.
  `ExecutionReconciliationService`'e erişim HÂLÂ yalnızca `scripts/
  binance_testnet_lab.py` üzerinden mümkündür.
- Dashboard, tarayıcı tarafında JavaScript OTOMATİK-YENİLEME İÇERMEZ
  (script'siz, salt-metin sayfa) — kullanıcı elle yeniler veya
  `GET /api/status`'u kendi (opsiyonel) bir polling aracıyla izler. Bu
  KASITLI bir minimalizm kararıdır (Bölüm 15, "dashboard should be
  minimal").
- `openOrders` toplu sorgusu (Faz 11'den devralınan bilinen sınırlama)
  hâlâ implemente EDİLMEDİ — `reconcile_pending()` her kaydı KENDİ
  `client_order_id`'si ile ayrı ayrı sorgular.
- Log rotasyonu için özel bir Python aracı YAZILMADI — `journald`/
  `logrotate` gibi standart OS araçları KULLANILMASI belgelenmiştir
  (Bölüm 7).

## 12. Faz 1-11 sözleşmesi korunumu

DEĞİŞTİRİLMEDEN korunanlar: `ALLOW_LIVE_TRADING = False`, Faz 10 host
allowlist'i (`https://testnet.binance.vision`, TAM eşleşme), `ExecutionMode`
enum'unun yalnızca iki üyesi, Faz 11'in TÜM reconciliation/idempotency/
contradiction-koruma/UNKNOWN_NOT_FOUND semantiği (Karar 78), Faz 8'in
config/lock/health-snapshot/CLI sözleşmesi, Faz 9'un stability
mekanizmaları. Hiçbir Faz 1-11 dosyası YENİDEN TASARLANMADI — yalnızca
`ops/config.py` (yeni alanlar), `ops/health_snapshot.py` (yeni opsiyonel
alanlar, geriye-uyumlu), `app.py` (yeni kompozisyon adımları), ve
`runtime/coordinator.py` (iki KÜÇÜK salt-okunur accessor, `paper_engine`
property + `last_cycle_result()`) EK yapıldı.


=== FILE: PHASE8_UBUNTU_OPERATIONS.md ===
# FAZ 8 — Operations & Ubuntu Deployment

Bu belge, kabul edilmiş Faz 1-7 sistemini (PUBLIC Binance market data →
feature engine → signal engine → paper trading → SQLite persistence) bir
Ubuntu makinesinde uzun süre çalışan, gözlemlenebilir, güvenli bir
systemd servisi olarak işletmek için gereken TÜM operasyonel adımları
kapsar.

**Faz 8, bir execution fazı DEĞİLDİR.** Bu belgedeki hiçbir adım Binance
Testnet'e, Mainnet execution'a, veya herhangi bir private/signed API
endpoint'ine bağlanmaz. Sistem yalnızca PUBLIC market data okur ve
tamamen yerel, simüle (paper) pozisyonlar tutar. `ALLOW_LIVE_TRADING =
False` invariant'ı (bkz. `crypto_signal_engine/__init__.py`,
`SAFETY_INVARIANTS.md`) Faz 8 tarafından da korunur ve
`tests/test_repository_safety_scan.py` tarafından otomatik olarak
doğrulanır (Faz 8'in eklediği `crypto_signal_engine/ops/` ve `app.py`
dahil — bu tarayıcı TÜM `crypto_signal_engine/` paketini kapsar).

**Not (Faz 12):** bu belge systemd/Ubuntu VPS-tarzı dağıtımı kapsar —
hâlâ geçerlidir ve DEĞİŞTİRİLMEDİ. Kesintisiz YEREL (laptop/WSL)
çalıştırma, `doctor` preflight komutu, salt-okunur operasyon dashboard'u,
ve telefon/Tailscale erişim modeli için bkz.
**PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md** — bu iki belge birbirini
TAMAMLAR (Faz 8 = systemd/VPS-hazır dağıtım mekaniği, Faz 12 = günlük
yerel operasyon + execution-mode gating + dashboard).

## İçindekiler

1. Önkoşullar
2. Servis kullanıcısı ve dizinlerin oluşturulması
3. Uygulamanın kurulumu
4. Konfigürasyon
5. Servisin başlatılması
6. Logları inceleme
7. Sağlığı inceleme (`status` komutu)
8. Yeniden başlatma / durdurma
9. SQLite yedekleme
10. SQLite geri yükleme
11. Uygulama sürümünü geri alma (git tag ile rollback)
12. Yaygın hata durumları
13. Mimari özet (config/logging/lock/health/systemd tasarım kararları)

---

## 1. Önkoşullar

- Ubuntu 22.04 LTS veya üzeri (systemd tabanlı herhangi bir modern
  Ubuntu sürümü uygundur).
- Python `>=3.10` (bkz. `pyproject.toml` — `requires-python`).
- Giden internet erişimi YALNIZCA `api.binance.com` (HTTPS) ve
  `stream.binance.com:9443` (WSS) için — PUBLIC market data.
- root veya `sudo` erişimi (yalnızca kurulum adımları için; servisin
  KENDİSİ asla root olarak ÇALIŞMAZ).

Bu belge bir VPS/bulut sağlayıcısı SATIN ALMANIZI istemez veya varsaymaz
— herhangi bir Ubuntu makinesinde (yerel, ev sunucusu, mevcut bir VPS)
aynı şekilde uygulanabilir.

## 2. Servis kullanıcısı ve dizinlerin oluşturulması

```bash
sudo useradd --system --create-home --home-dir /opt/crypto-signal-engine \
    --shell /usr/sbin/nologin csengine

sudo mkdir -p /opt/crypto-signal-engine
sudo mkdir -p /etc/crypto-signal-engine
sudo mkdir -p /var/lib/crypto-signal-engine

sudo chown -R csengine:csengine /opt/crypto-signal-engine
sudo chown -R csengine:csengine /var/lib/crypto-signal-engine
sudo chown root:csengine /etc/crypto-signal-engine
sudo chmod 750 /etc/crypto-signal-engine
```

Dizin ayrımı (Bölüm 10):

| Dizin | İçerik |
|---|---|
| `/opt/crypto-signal-engine/` | Uygulama kodu + Python venv |
| `/etc/crypto-signal-engine/env` | Konfigürasyon (`CSE_*` ortam değişkenleri, secret İÇERMEZ) |
| `/var/lib/crypto-signal-engine/` | Durable SQLite state + process-lock + health snapshot |
| journald | Loglar (`journalctl -u crypto-signal-engine`) — bkz. Bölüm 6 için isteğe bağlı dosya-tabanlı alternatif |

## 3. Uygulamanın kurulumu

```bash
sudo -u csengine git clone <bu repo> /opt/crypto-signal-engine
cd /opt/crypto-signal-engine
sudo -u csengine git checkout phase7-accepted   # veya en son kabul edilmiş tag

sudo -u csengine python3 -m venv /opt/crypto-signal-engine/venv
sudo -u csengine /opt/crypto-signal-engine/venv/bin/pip install --upgrade pip
# `[runtime]` extra'sı `websockets`'i kurar — GERÇEK Binance PUBLIC WS
# bağlantısı için gereklidir (bkz. Bölüm 13 — Packaging).
sudo -u csengine /opt/crypto-signal-engine/venv/bin/pip install ".[runtime]"
```

Kurulumu doğrula (ağ gerektirmez):

```bash
sudo -u csengine /opt/crypto-signal-engine/venv/bin/python -m pytest -q
sudo -u csengine /opt/crypto-signal-engine/venv/bin/python -m crypto_signal_engine.app status
# beklenen çıktı: "NO SNAPSHOT YET ..." (servis henüz hiç çalışmadı — NORMAL)
```

## 4. Konfigürasyon

```bash
sudo cp deploy/env.example /etc/crypto-signal-engine/env
sudo chown root:csengine /etc/crypto-signal-engine/env
sudo chmod 640 /etc/crypto-signal-engine/env
sudo nano /etc/crypto-signal-engine/env   # CSE_SYMBOLS ve CSE_DB_PATH'i ayarla
```

TÜM `CSE_*` değişkenleri ve varsayılanları için bkz. `deploy/env.example`
ve `crypto_signal_engine/ops/config.py`. **Bu dosyada hiçbir zaman bir
API key/secret alanı OLMAYACAK** — sistem yalnızca PUBLIC endpoint'lere
erişir, kimlik doğrulaması gerektirmez.

Geçersiz/eksik bir değer, servis BAŞLAMADAN ÖNCE (`main()` içinde,
runtime'a hiç girmeden) açık bir hata ile fail-fast olur — bkz. Bölüm 12.

```bash
sudo cp deploy/systemd/crypto-signal-engine.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 5. Servisin başlatılması

```bash
sudo systemctl enable --now crypto-signal-engine
sudo systemctl status crypto-signal-engine
```

İlk başlatmada recovery, durable checkpoint YOKSA standart tam warmup
penceresini PUBLIC REST'ten çeker (Faz 7, `PersistedRuntime.recover()`);
sonraki her restart, kaldığı yerden (`open_time` checkpoint'inden)
devam eder.

## 6. Logları inceleme

```bash
journalctl -u crypto-signal-engine -f              # canlı takip
journalctl -u crypto-signal-engine -n 200           # son 200 satır
journalctl -u crypto-signal-engine --since "1 hour ago"
```

Loglanan olaylar: başlangıç + config özeti, recovery başlangıç/başarı/
başarısızlık, READY/DEGRADED geçişleri, WS disconnect/reconnect, gap
tespiti/çözümü, persistence checkpoint hataları, paper pozisyon açma/
kapama özetleri, graceful shutdown, fatal exception'lar (bkz.
`crypto_signal_engine/ops/logging_setup.py`). Ham order book/candle
payload'ları SÜREKLİ loglanmaz.

İsteğe bağlı — logları AYRICA bir dosyaya yönlendirmek isterseniz, unit
dosyasında `StandardOutput=journal` satırını `StandardOutput=append:/var/log/crypto-signal-engine/service.log`
ile değiştirip `/var/log/crypto-signal-engine/` dizinini `csengine`
kullanıcısına ait olacak şekilde oluşturun; varsayılan kurulum bunu
GEREKTİRMEZ (journald zaten kalıcı ve döndürmeli/rotate'lidir).

## 7. Sağlığı inceleme (`status` komutu)

```bash
sudo -u csengine /opt/crypto-signal-engine/venv/bin/python -m crypto_signal_engine.app status
```

Örnek çıktı:

```
overall_health: READY
generated_at:   2026-01-01T12:00:03+00:00
pid:            48213
started_at:     2026-01-01T11:58:00+00:00
recovery:       ok=True detail='recovery completed'
  BTCUSDT      READY          last_event=2026-01-01T12:00:02+00:00 reconnects=0 detail='ok'
  ETHUSDT      READY          last_event=2026-01-01T12:00:01+00:00 reconnects=0 detail='ok'
```

Bu komut, servisin process'i tarafından periyodik olarak yazılan yerel
bir JSON anlık görüntüsünü (`CSE_HEALTH_SNAPSHOT_PATH`, varsayılan
`<db_dir>/health.json`) okur — HTTP/network erişimi gerektirmez, root
gerektirmez. Exit code `0` yalnızca `overall_health == READY` VE anlık
görüntü YETERİNCE GÜNCEL iken; her DEGRADED/BOOTSTRAPPING durumda,
"henüz hiç anlık görüntü yok" durumunda, VE anlık görüntü ÇOK ESKİYSE
(bkz. aşağı) `1` döner — bu, cron/monitoring entegrasyonu için
kullanılabilir.

**Staleness kontrolü:** process `kill -9`/OOM ile ANİ ölürse (graceful
shutdown kodu hiç çalışmadan), diskte kalan SON dosya eski bir "READY"
görüntüsü olarak KALABİLİR. `status` komutu bu yüzden dosyanın YAŞINI da
kontrol eder (`CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS`'ın 5 katı, en az 30
saniye) — eşik aşılırsa `STALE SNAPSHOT` uyarısı ile `1` döner. Bu, bir
process'in GERÇEKTEN çalışıp çalışmadığını `systemctl status
crypto-signal-engine`'in yerine GEÇMEZ — ikisi BİRLİKTE kullanılmalıdır.

## 8. Yeniden başlatma / durdurma

```bash
sudo systemctl restart crypto-signal-engine   # graceful SIGTERM + systemd'nin kendi restart'ı
sudo systemctl stop crypto-signal-engine      # graceful SIGTERM, KESİN durdurma
```

`stop`/`restart`, servise SIGTERM gönderir; uygulama bunu YAKALAR, yeni
hiçbir market-data event'ini kabul ETMEZ, açık stream'leri temiz kapatır,
son bir health snapshot yazar, process-lock'u serbest bırakır ve `0`
exit code ile TEMİZ sonlanır (bkz. `crypto_signal_engine/app.py::Application._graceful_stop`).
Bu davranış idempotent'tir — art arda birden fazla sinyal güvenlidir.

## 9. SQLite yedekleme

```bash
sudo -u csengine /opt/crypto-signal-engine/venv/bin/python scripts/backup_sqlite.py \
    --source /var/lib/crypto-signal-engine/paper_state.db \
    --destination /var/backups/crypto-signal-engine/paper_state-$(date +%F).db
```

Bu script SQLite'ın resmi Online Backup API'sini (`sqlite3.Connection.backup()`)
kullanır — servis ÇALIŞIRKEN bile tutarlı bir kopya üretir; ham `cp`
KULLANILMAZ (WAL modunda aktif bir dosyayı `cp` ile kopyalamak bozuk bir
anlık görüntü üretebilir).

### 9.1 Otomatik günlük yedekleme (systemd timer, 24/7 Ops v1)

Yukarıdaki komutu HER GÜN elle çalıştırmak yerine, `crypto-signal-engine-
backup.timer` bunu otomatikleştirir — script'in KENDİSİ DEĞİŞTİRİLMEDEN
çağrılır (yalnızca tarih damgalı hedef dosya adı + `--keep-last 14`
retention'ı, timer'ın `.service` dosyası seviyesinde eklenir). Zamanlama
kararı ve gerekçesi için bkz. DECISIONS.md ("24/7 Ops v1" Kararı).

```bash
sudo mkdir -p /var/backups/crypto-signal-engine
sudo chown csengine:csengine /var/backups/crypto-signal-engine

sudo cp deploy/systemd/crypto-signal-engine-backup.service /etc/systemd/system/
sudo cp deploy/systemd/crypto-signal-engine-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-signal-engine-backup.timer
```

Doğrulama:

```bash
systemctl list-timers crypto-signal-engine-backup.timer
sudo systemctl start crypto-signal-engine-backup.service   # elle bir kez tetikle
journalctl -u crypto-signal-engine-backup.service -n 50
ls -la /var/backups/crypto-signal-engine/
```

Varsayılan program: günlük (`OnCalendar=daily`), en son **14** yedek
tutulur (yaklaşık iki haftalık günlük geçmiş) — daha eskiler otomatik
silinir. `--keep-last` değeri `.service` dosyasındaki `ExecStart`
satırında değiştirilebilir.

## 10. SQLite geri yükleme

1. `sudo systemctl stop crypto-signal-engine` — servisi DURDUR.
2. Mevcut dosyayı YOK ETMEDEN kenara taşı:
   `sudo -u csengine mv /var/lib/crypto-signal-engine/paper_state.db /var/lib/crypto-signal-engine/paper_state.db.before-restore`
3. Yedeği yerine kopyala:
   `sudo -u csengine cp /var/backups/crypto-signal-engine/paper_state-2026-01-01.db /var/lib/crypto-signal-engine/paper_state.db`
4. `sudo systemctl start crypto-signal-engine`
5. Doğrula: `journalctl -u crypto-signal-engine -n 100` ve
   `python -m crypto_signal_engine.app status` — recovery'nin
   `ok=True` olduğunu ve `overall_health`'in makul bir sürede
   `READY`'e ulaştığını kontrol et.

Recovery başarısız olursa (bozuk/şema-uyumsuz dosya), servis Bölüm 12'de
açıklandığı gibi BAŞLAMAZ ve `paper_state.db.before-restore` dokunulmadan
kalır — hiçbir veri kaybolmaz.

## 11. Uygulama sürümünü geri alma (git tag ile rollback)

```bash
sudo systemctl stop crypto-signal-engine
cd /opt/crypto-signal-engine
sudo -u csengine git checkout phase7-accepted   # veya istenen önceki kabul edilmiş tag
sudo -u csengine venv/bin/pip install --force-reinstall ".[runtime]"
sudo systemctl start crypto-signal-engine
```

Durable SQLite dosyası bir git nesnesi DEĞİLDİR — kod rollback'i durable
state'i ETKİLEMEZ (Faz 7'nin şema versiyonlaması, eski bir kod sürümünün
daha yeni bir şemayla karşılaşması durumunda `SchemaVersionMismatchError`
ile fail-fast olur, sessizce devam ETMEZ).

## 12. Yaygın hata durumları

| Durum | Davranış | Exit code |
|---|---|---|
| Eksik/geçersiz `CSE_*` env değişkeni | Runtime hiç başlamaz, hata stderr'e ve loga yazılır | 2 |
| Aynı `CSE_DB_PATH`'e karşı ikinci bir instance | İkinci instance başlamaz, birinciye DOKUNMAZ | 3 |
| Şema uyumsuzluğu / bozuk SQLite kaydı / recovery sırasında PUBLIC REST erişilemez | Runtime KESİNLİKLE başlamaz, durable state DOKUNULMADAN kalır | 4 |
| Runtime loop'ta beklenmeyen fatal exception | Loglanır, graceful shutdown tetiklenir, systemd `RestartSec=10` ile yeniden dener (`StartLimitBurst=5` sonrası durur) | 1 |
| SIGINT/SIGTERM | Graceful, idempotent shutdown | 0 |

Hiçbir durumda uygulama, bozuk/eksik bir durable state'ten SESSİZCE boş
bir state'e "sıfırlanmaz" — bkz. `PHASE7_PERSISTENCE_RECOVERY.md`.

## 13. Mimari özet

- **Konfigürasyon** (`ops/config.py`): tek kaynak `CSE_*` ortam
  değişkenleri (systemd `EnvironmentFile=` ile uyumlu); tek bir immutable
  `AppConfig`; hiçbir YAML/plugin/çok-katmanlı override YOK; hiçbir
  secret alanı YOK; REST/WS base URL KASITLI OLARAK override
  EDİLEMEZ (Testnet/başka bir endpoint'e kötüye kullanım riskine karşı
  defense-in-depth).
- **CLI/entrypoint** (`app.py`): `python -m crypto_signal_engine.app
  {run|status}` (eşdeğer console script: `crypto-signal-engine`).
  `Application` kompozisyon kökü, hiçbir iş mantığını TEKRARLAMAZ —
  yalnızca kabul edilmiş Faz 5/6/7 nesnelerini bağlar.
- **Loglama** (`ops/logging_setup.py`): yalnızca stdlib `logging`,
  stdout'a (journald tarafından yakalanır); yeni bir soyutlama YOK.
- **Process-lock** (`ops/lock.py`): POSIX `flock` (`LOCK_EX|LOCK_NB`) —
  stale-lock kurtarma OS tarafından OTOMATİK sağlanır (process crash
  edince kilit kendiliğinden serbest kalır); distributed lock sistemi
  YOK.
- **Health/observability** (`ops/health_snapshot.py`): periyodik, atomik
  (`os.replace`) yerel JSON dosyası + `status` CLI komutu; HTTP endpoint
  YOK (ekstra dinleyen port/güvenlik yüzeyi istenmedi).
- **systemd** (`deploy/systemd/crypto-signal-engine.service`): non-root
  `csengine` kullanıcısı, `EnvironmentFile=`, bounded `Restart=on-failure`
  + `RestartSec=10` + `StartLimitBurst=5`, `KillSignal=SIGTERM` +
  `TimeoutStopSec=30`, `NoNewPrivileges=yes`/`ProtectSystem=strict`.
- **Backup** (`scripts/backup_sqlite.py`): SQLite'ın resmi Online Backup
  API'si; ham `cp` YASAK (WAL tutarlılık riski). 24/7 Ops v1'den beri
  `crypto-signal-engine-backup.timer` (günlük, `--keep-last 14`
  retention) bunu otomatikleştirir — bkz. Bölüm 9.1 ve DECISIONS.md
  Karar 99.
- **Watchdog** (`ops/systemd_notify.py`, 24/7 Ops v1): ZATEN çalışan
  health loop'a eklenen `sd_notify(WATCHDOG=1)` heartbeat'i +
  `WatchdogSec=60` — HUNG (crash değil, tıkanmış) bir process'i
  `flock`-tabanlı process-lock'un YAKALAYAMADIĞI durumlarda systemd'ye
  yakalatır. `$NOTIFY_SOCKET` yokken tam bir no-op.
- **Control Center** (`ops/admin.py`, 24/7 Ops v1): `CSE_DASHBOARD_
  ADMIN_TOKEN` AYARLANDIĞINDA dashboard'a `POST /admin/{pause,resume,
  stop}` ekler — pause/resume bağımsız bir `entries_paused` kapısını
  aç/kapatır (açık pozisyon yönetimini ETKİLEMEZ), stop ZATEN var olan
  `request_shutdown()`'ı çağırır (yeniden başlatmayı OTOMATİKLEŞTİRMEZ).
  Token AYARLANMAMIŞSA (varsayılan) dashboard tamamen salt-okunur kalır.
  Bkz. DECISIONS.md Karar 99.
- **Alerting** (`ops/notifier.py`, 24/7 Ops v1): opsiyonel, outbound-only
  Telegram bildirimi — günlük zarar sigortası tetiklenmesi, adaptive
  rollback, süreç başlangıcı/durması, ve admin aksiyonlarında. `CSE_
  TELEGRAM_BOT_TOKEN`/`CSE_TELEGRAM_CHAT_ID` (AppConfig DIŞINDA,
  Testnet imza kimlik bilgileriyle AYNI desen) verilmezse tamamen
  devre dışı.
- **Packaging** (`pyproject.toml`): `[project.optional-dependencies]
  runtime = ["websockets>=12.0"]` — offline test suite'i BUNU
  GEREKTİRMEZ; yalnızca gerçek bir WS bağlantısı kurulmak istendiğinde
  (üretim veya `scripts/live_public_smoke_test.py`) gereklidir.

Faz 8 kapsamı DIŞINDA (Faz 9+ için): çok-günlük soak test, Binance
Testnet execution lab, execution safety/reconciliation, final production
readiness.


=== FILE: PHASE9_LONG_RUN_STABILITY.md ===
# FAZ 9 — Long-Run Stability / Soak Testing

Bu belge, kabul edilmiş Faz 1-8 sistemini (PUBLIC market data → feature
engine → SignalEngine → PaperTradingEngine → persistence → operations)
uzun süre çalıştırmanın GÜVENLİ olduğunu, HIZLANDIRILMIŞ (accelerated)
sanal zamanda, tamamen offline/deterministik bir soak harness ile
kanıtlar. **Bu bir execution fazı DEĞİLDİR** — `PaperTradingEngine` (Faz
5) tek trading/simülasyon sınırı olmaya devam eder, `ALLOW_LIVE_TRADING =
False` korunur.

## 1. Neden "hızlandırılmış" ve gerçek saatlerce beklemeden

Otomatik test suite'i GERÇEK saatler/günler boyunca çalışamaz (CI/geliştirme
döngüsü için pratik değil). Bunun yerine `crypto_signal_engine/stability/`
paketi, Faz 2'nin ZATEN kabul edilmiş `FixedClock`'unu (test-amaçlı,
elle ilerletilen bir clock — DEĞİŞTİRİLMEDEN yeniden kullanılır) ve
deterministik candle/order-book üretimini kullanarak, "24 saat", "birkaç
gün" gibi periyotları saniyeler içinde temsil eder. Otomatik suite hiçbir
zaman gerçek `time.sleep`/`asyncio.sleep(N>0)` KULLANMAZ.

**Açıkça belirtilmesi gereken sınır:** hızlandırılmış bir simülasyon,
GERÇEK çok-günlük ağ/OS davranışının (örn. günlerce süren bir TCP
bağlantısının gerçek işletim sistemi kaynak sınırlarına çarpması, gerçek
Binance'in haftalar içindeki API şema değişiklikleri, gerçek disk
doluluğu) TÜM yönlerini KANITLAYAMAZ. Bunun için Bölüm 8'deki OPSİYONEL
gerçek PUBLIC soak script'i vardır — ama bu MANUEL'dir ve acceptance
otomatik test suite'ine BAĞLI değildir.

## 2. Soak harness mimarisi

```
crypto_signal_engine/stability/
    __init__.py
    harness.py     — SoakHarness: gerçek Faz 5/6/7 nesnelerini kurar/sürer
    faults.py      — ScriptedMarketDataProvider, FaultyPaperStateStore
    metrics.py     — SoakMetrics: senaryo sayaçları
    scenarios.py   — 10 çekirdek senaryonun sürücü fonksiyonları
```

`SoakHarness`, Faz 6/7 mantığını TEKRARLAMAZ — yalnızca `PaperTradingEngine`,
`RuntimeCoordinator`, `PaperStateStore`, `PersistedRuntime`'ı kurar ve
onların PUBLIC metodlarını (`ingest_candle`, `ingest_order_book`, `recover`,
`stop`, `resolve_gap`) DOĞRUDAN çağırır — Faz 6/7'nin KENDİ test
suite'lerinin yaptığı ile AYNI disiplin. Çoğu senaryo senkron doğrudan
çağrılarla sürülür (bu, per-event iş mantığının TAMAMINI egzersiz eder);
yalnızca reconnect/shutdown-under-activity senaryoları GERÇEK async
`run()`/`stop()` mekaniğini (`ScriptedMarketDataProvider` ile) kullanır.

## 3. Uygulanan 10 çekirdek senaryo

1. **Steady State** — çoklu sembol, sürekli candle/order-book, sürekli
   sinyal değerlendirmesi, hata YOK.
2. **Reconnect Storm** — GERÇEK Faz 6 sözleşmesi netleştirildi (bkz. Bölüm
   4): bir stream kopması TEK bir process içinde kendi kendine
   "reconnect" ETMEZ (Faz 2'nin kendi iç reconnect politikası TÜKENMİŞ
   demektir) — bu yüzden senaryo, terminal bir disconnect'in (a) ANINDA
   DEGRADED'a yansıdığını, (b) `run()`'ı ÇÖKERTMEDİĞİNİ, (c) DİĞER
   sembolleri ETKİLEMEDİĞİNİ doğrular.
3. **Gap Recovery** — gap tespiti + GERÇEK `RuntimeCoordinator.resolve_gap()`
   ile REST-tabanlı kurtarma; hem BAŞARILI hem BAŞARISIZ (kalıcı DEGRADED)
   durumlar test edildi.
4. **Duplicate/Out-of-order** — aynı candle'ın tekrar/eski sırayla
   ingest'i state'i BOZMAZ, fail-fast davranış KORUNUR.
5. **Process Restart** — GERÇEK bir bulgu netleştirildi (bkz. Bölüm 5):
   restart SONRASI READY'e HEMEN dönüş, checkpoint durumuna bağlıdır.
6. **Crash-like Restart** — `stop()`/`close()` ÇAĞRILMADAN referanslar
   düşürülür; son TAMAMLANMIŞ checkpoint durable kalır (Faz 7 atomicity).
7. **Persistence Failure** — `FaultyPaperStateStore` ile deterministik
   checkpoint hatası; fault sembol-izole, reason-bazlı temizlenir.
8. **Stale Feed** — bir sembolün feed'i durur, DİĞERİ etkilenmez; resume
   normal `mark_event()` ile temizlenir.
9. **Multi-Symbol Stress** — 5 sembol eşzamanlı, izolasyon + determinism.
10. **Shutdown Under Activity** — `stop()` sonrası push edilen HİÇBİR
    event işlenmez (kanıtlanmış, tahmin EDİLMEMİŞ).

## 4. BULGU: Reconnect Storm'un gerçek kapsamı (blocker fix — Karar 67)

Harness geliştirmesi sırasında, `PersistedRuntime._consume_candles()`/
`_consume_order_book()`'un (Faz 7, Faz 8'in GERÇEK üretim giriş noktası
tarafından kullanılır) Faz 6'nın KENDİ `except Exception:
mark_disconnected(symbol); raise` disiplinine SAHİP OLMADIĞI keşfedildi
— gerçek üretimde bir stream kopması, health'i HİÇ DEGRADED işaretlemeden
sessizce yutuluyordu (yalnızca staleness eşiği dolana kadar). Bu bir
BLOCKER olarak ele alındı: önce regresyon testi eklendi (fix'ten önce
FAIL ettiği doğrulandı), sonra Faz 6'nın deseninin BİREBİR kopyası
uygulandı. Detaylar: DECISIONS.md Karar 67.

Ayrıca netleştirilen (GERÇEK Faz 6 tasarımı, DEĞİŞTİRİLMEDİ): `RuntimeCoordinator.run()`
KENDİ BAŞINA asla yeniden subscribe OLMAZ ("Faz 2 kendi içinde sınırsız
reconnect döngüsü çalıştırır"). Bu yüzden bir Phase-6/7-seviyesi
"disconnect", TEK bir process çalışması içinde KENDİ KENDİNE iyileşmez —
iyileşme, ya Faz 2'nin (bu fazın kapsamı DIŞINDaki, ayrı test edilmiş)
kendi iç reconnect'i BAŞARILI olduğunda (hiç coordinator seviyesine
sızmaz) ya da Faz 8'in systemd `Restart=on-failure` politikasıyla TÜM
process yeniden başladığında olur.

## 5. BLOCKER FİX (Karar 68): restart recovery artık checkpoint ETRAFINDA tam warmup lookback'i yeniden çeker

**Önceki (BLOCKER) davranış:** Faz 7'nin candle-checkpoint'i YALNIZCA
canlı ingest edilmiş candle'lar için yazılır (Decision 62 — rebuildable
state blindly serialize edilmez); ama `recover()`, bir checkpoint VARSA
yalnızca dar `[checkpoint, now)` aralığını REST'ten çekiyordu. Checkpoint
HER ZAMAN "en son işlenen candle" olduğundan, bu aralık normal
operasyonda GERÇEK PUBLIC geçmiş verisi bol miktarda MEVCUT olsa bile
neredeyse hiç candle içermiyordu — restart'ı gereksiz yere
BOOTSTRAPPING'de bırakıyor, M15/H1'in GERÇEK zamanda saatlerce yeniden
ısınmasını gerektiriyordu. Bağımsız acceptance review bunu bir BLOCKER
olarak işaretledi: kabul edilmiş mimari, yeniden inşa edilebilir state'in
authoritative PUBLIC Binance geçmiş verisinden YENİDEN İNŞA EDİLMESİNİ
gerektirir — mevcut olduğunda kullanılmaması bu ilkenin ihlaliydi.

**Fix:** `recover()`, checkpoint VARSA/YOKSA fark etmeksizin AYNI
warmup-lookback formülünü (`_TIMEFRAME_DURATIONS[timeframe] *
warmup_candles * 2` — Faz 6'nın kendi `bootstrap()`'ıyla AYNI, YENİ bir
sabit İCAT EDİLMEDİ) kullanır: `[checkpoint - lookback, now)`. Checkpoint
candle'ının KENDİSİ bu aralığa HER ZAMAN dahildir (`checkpoint - lookback
< checkpoint < now`), böylece hem doğru "çapalama" hem de TAM warmup'ın
yeniden inşası GARANTİ edilir — checkpoint asla GERİYE hareket ETMEZ,
sahte hiçbir candle ÜRETİLMEZ (PUBLIC kaynağın GERÇEKTEN dönebildiğinden
fazlası yoktur), ve `bootstrap_candles()` ASLA sinyal değerlendirmesi
tetiklemez (state reconstruction, retroactive trading DEĞİL).

**Yeni sözleşme:**
- Yeterli authoritative PUBLIC geçmiş MEVCUTSA (gerçek Binance için normal
  durum — yıllarca candle geçmişi tutar): restart, YENİ M15/H1
  candle'larını GERÇEK zamanda BEKLEMEDEN doğrudan READY'e döner (yalnızca
  M1 order-book'un yeniden edinilmesi gerekir — Faz 6/7 hiçbir zaman
  order-book state'i persist ETMEZ, bu KASITLI).
- PUBLIC kaynak GERÇEKTEN yetersizse (test double'ı kasıtlı olarak
  KISITLANMIŞ): sembol dürüstçe BOOTSTRAPPING/DEGRADED kalır, SAHTE bir
  READY asla ÜRETİLMEZ.

Regresyon testleri: `tests/test_persistence_recovery.py::TestRestartRecoveryRebuildsWarmupHistory`
(checkpoint-etrafı warmup yeniden inşası + multi-symbol izolasyonu +
yetersiz-geçmiş-durumu, fix'ten ÖNCE FAIL ettiği doğrulandı) ve
`tests/test_stability_scenarios.py::TestProcessRestart::test_accounting_survives_a_restart_cycle_and_reaches_ready_immediately`/
`test_genuinely_insufficient_public_history_stays_bootstrapping`. Detaylı
gerekçe: DECISIONS.md Karar 68.

## 6. Muhasebe/temporal invariant'lar (test edildi)

- Sonlu (finite) quantity/price/PnL/fee — asla NaN/inf.
- `context_id` başına TEK mutation (dict zaten garanti eder); replay PnL'i
  DEĞİŞTİRMEZ (determinism testi).
- Entry fee, restart'ı KESİNTİSİZ hayatta kalır (kapanışa kadar).
- Multi-symbol: her sembolün checkpoint/pozisyonu BAĞIMSIZ.
- UTC-aware timestamp'ler, candle `open_time` identity, kronolojik
  accepted candle ilerleyişi — hepsi Faz 1/6'nın KENDİ validasyonu
  üzerinden (yeniden İCAT EDİLMEDİ).
- Duplicate/eski candle'lar temporal state'i asla İLERİ TAŞIMAZ.
- Restart checkpoint'i asla GERİYE gitmez (yalnızca en son ACCEPTED
  candle'ı işaret eder).

## 7. Kaynak/büyüme bulguları

| Yapı | Kategori | Bulgu |
|---|---|---|
| `CandleWindow` | B (sınırlı) | `maxlen` (varsayılan 500) ASLA aşılmadı — test edildi |
| `FeatureHistoryStore` | B (sınırlı) | Her `deque` kendi `maxlen`'ini ASLA aşmadı — test edildi |
| `RuntimeCoordinator._tasks` | B (sınırlı) | `run()` başına SABİT sayıda task; event başına BÜYÜMEZ |
| `processed_context_ids` (in-memory + SQLite) | A (beklenen durable) | Event sayısıyla ORANTILI büyür, asla onu AŞMAZ — Faz 5'in idempotency ledger'ı KASITLI OLARAK budanmaz |
| fills/orders (in-memory + SQLite) | A (beklenen durable) | Aynı — bir trading ledger'ı doğası gereği eklemeli (append-only) |

**C kategorisi (kazara/sınırsız büyüme) bulunmadı.** `processed_context_ids`/
fills/orders'ın SONSUZA KADAR büyümesi, çok-yıllık bir tek-process
çalışması için teorik olarak bir husustur, ama bunu "optimize etmek" Faz
5'in muhasebe modelini REDESIGN etmek anlamına gelir — bu fazın kapsamı
DIŞINDADIR (talimatın kendisi de "Do not redesign accounting merely to
optimize memory" der). Pratikte Faz 8'in systemd servis modeli zaten
periyodik yeniden başlatmaları (deploy/rollback/OS güncellemesi) doğal
kılar; bu, dolaylı bir sınırlama sağlar.

## 8. Determinism

`tests/test_stability_determinism.py`, TAZE state'ten AYNI hızlandırılmış
iş yükünü (multi-symbol stress + gap recovery + restart) iki KEZ çalıştırıp
pozisyon/fill/order/context-id/checkpoint/final-health'in TAM olarak
eşleştiğini doğrular (PID gibi işlemsel metadata hariç tutulur — zaten
karşılaştırılan alanlarda YOKTUR).

## 9. Opsiyonel gerçek PUBLIC soak

```bash
python3 scripts/public_soak_test.py --symbols BTCUSDT,ETHUSDT \
    --duration-seconds 3600 --db-path /tmp/soak.db --report-path /tmp/soak-report.json
```

- PUBLIC endpoint'ler ve paper trading DIŞINDA hiçbir şey KULLANMAZ.
- Credential ALMAZ.
- SIGINT/SIGTERM'de graceful durur.
- Periyodik (varsayılan 30s) sağlık özeti YAZDIRIR.
- Fatal recovery/runtime hatasında non-zero exit döner.
- Normal pytest suite'inin PARÇASI DEĞİLDİR.

Bu script, bu teslimat sırasında GERÇEK Binance PUBLIC verisine karşı
15 saniyelik kısa bir smoke ile doğrulandı (recovery başarılı, READY'e
ulaşıldı, graceful shutdown, geçerli JSON rapor) — ÇOK SAATLİK bir soak
KOŞULMADI (talimatın kendisi de bunu istemez).

## 10. Ne test edildi, ne GERÇEK elapsed-time gözlemi gerektirir

**Test edildi (offline, hızlandırılmış, deterministik):** yukarıdaki 10
senaryo, muhasebe/temporal invariant'lar, kaynak sınırları, determinism.

**GERÇEK elapsed-time gözlemi gerektirir (bu fazın OTOMATİK suite'i
tarafından KANITLANMAZ):** günlerce süren gerçek TCP bağlantı kararlılığı,
gerçek disk/dosya sistemi davranışı (SQLite WAL dosyasının çok büyük
boyutlarda gerçek disk I/O performansı), gerçek Binance'in uzun vadeli
API/şema kararlılığı, gerçek Ubuntu OS-seviyesi kaynak sınırları (fd
limitleri, bellek fragmantasyonu). Bunlar Faz 9'un OPSİYONEL gerçek soak
script'i ile MANUEL olarak, ayrı bir operasyonel adım olarak
gözlemlenmelidir — bu belge bunu iddia ETMEZ.

## 11. Güvenlik sınırı

`ALLOW_LIVE_TRADING = False` korunur. Bu paket hiçbir API key/secret/HMAC/
signed-endpoint/Testnet/Mainnet-execution alanı TAŞIMAZ — yalnızca PUBLIC
Binance market data + paper trading + persistence'ı hızlandırılmış sanal
zamanda simüle eder. `tests/test_repository_safety_scan.py`, tüm
`crypto_signal_engine/` paketini taradığından, `stability/` alt paketi de
otomatik olarak kapsanır.

Faz 9 kapsamı DIŞINDA (Faz 10+ için): Binance Testnet execution lab,
execution safety/reconciliation, final production readiness.


=== FILE: PROJECT_HANDOFF.md ===
# PROJECT_HANDOFF.md

This document is the single entry point for continuing this project in a
fresh conversation (ChatGPT, Claude, or any other assistant). It summarizes
the current state accurately as of the latest accepted baseline. If this
file ever disagrees with the actual code, `DECISIONS.md`, `ARCHITECTURE.md`,
or `PROJECT_SUMMARY.md`, the code and those files are authoritative — this
document should be corrected to match them, not the other way around.

**Pre-Audit Enhancement Pass note:** the accepted baseline is now
`phase12-accepted` (commit `33dc6cd`), and an additive research/
validation layer (`research/` package + three new `scripts/*.py` CLI
tools) has been added on top of it, ahead of the final independent
adversarial audit — see **PRE_AUDIT_ENHANCEMENTS.md** and DECISIONS.md
Karar 82-89 for the authoritative, current description. This is not
Phase 13. The phase table in Section 2 below predates Phase 8-12 and is
kept as-is for historical continuity — see ARCHITECTURE.md for the
authoritative, up-to-date phase summary.

## 1. Project objective

**Binance Crypto Intelligence / Signal Engine**: a Binance
public-market-data pipeline that ingests, validates, and transforms
live/historical crypto market data into deterministic quantitative
features, with the eventual goal (later phases) of producing explainable,
risk-aware trading signals — **without ever executing real trades**. The
project is built in strict, reviewed phases; each phase is independently
accepted before the next begins.

## 2. Accepted phases

| Phase | Scope | Status |
|---|---|---|
| Phase 1 | Domain contract layer: models, enums, provider/quality/safety ABCs, candle identity/sequencing, consensus/regime contracts | ACCEPTED (final hardened, two adversarial review rounds) |
| Phase 2 | Binance public market-data pipeline: REST + WebSocket providers, order-book sync, data-quality rules, SQLite persistence, reconnect/stale-feed handling, gap recovery | ACCEPTED (v3, two independent reviewer rounds) |
| Phase 3 | Feature Engine: deterministic candle/order-book/trade features, no-look-ahead guarantee, multi-timeframe feature state | ACCEPTED (v2, no-look-ahead blocker fixed) |
| Phase 4 | Quant & Agent Engine: four independent agents, ConsensusEngine, RiskOverlay, SignalEngine orchestrator | ACCEPTED (one serious adversarial review, no blockers found) |
| Phase 5 | Paper Trading / Simulation Engine: deterministic in-memory paper order/fill/position/PnL bookkeeping (including configurable fee/slippage) over completed `Signal` objects only | ACCEPTED (two review rounds each found and fixed one BLOCKER — NEUTRAL was incorrectly flattening positions, corrected to NO_ACTION; and configurable deterministic fee/slippage was missing entirely, now implemented. Committed and tagged: `phase5-accepted`.) |
| Phase 6 | Real-Time Market Data & Runtime: `RuntimeCoordinator` continuously coordinates Phase 2 (public data) -> Phase 3 (features) -> Phase 4 (`SignalEngine`) -> Phase 5 (`PaperTradingEngine`); bootstrap, gap detection/recovery, staleness health, graceful shutdown | ACCEPTED (an independent blocker review found and fixed three BLOCKERs — failed gap recovery falsely stayed healthy, candle/order-book identity was not validated before mutation, READY did not require order-book evidence; plus one earlier BLOCKER, a silently-absorbed stream error. Committed and tagged: `phase6-accepted` (commit `5978c0c`).) |
| Phase 7 | Persistence & Recovery: durable SQLite-backed state for Phase 5 paper-trading positions/idempotency/ledger and Phase 6 candle continuity checkpoints, plus deterministic process-restart recovery (`PersistedRuntime`) | ACCEPTANCE-READY (one serious adversarial review found and fixed one BLOCKER — a successful checkpoint of one kind could mask an unrelated, still-unresolved checkpoint failure of another kind; persistence faults are now tracked per-reason. Not yet formally ACCEPTED — commit/tag pending.) |

## 3. Official baseline

As of Phase 5, the project baseline moved from a delivered ZIP to the
**local Git repository**: the tag `phase4-accepted` marks the accepted
Phase 4 source tree, `phase5-accepted` marks the accepted Phase 5 source
tree (commit `ae94494`), `phase6-accepted` marks the accepted Phase 6
source tree (commit `5978c0c`), and Phase 7 was built directly on top of
that tagged commit (no ZIP was created for Phase 5, 6, or 7; none should
be). Any future phase must be built directly on top of the latest
accepted Git tag's actual source tree — never recreated from a
prompt/spec description from scratch.

For the historical record: **`crypto_signal_engine_phase4_FINAL.zip`** was
the official, accepted baseline through the end of Phase 4.

## 4. Architecture pipeline

```
Binance Public Market Data (REST + WebSocket, read-only)
    |
    v
Phase 2: parse -> normalize -> sequence -> quality-gate -> canonical state (SQLite/in-memory)
    |
    v
Phase 3: Feature Engine (candle / order-book / trade features, no-look-ahead)
    |
    v
FeatureSnapshot / FeatureHistoryStore (bounded, symbol+timeframe isolated)
    |
    v
Phase 4 (IMPLEMENTED): AgentContext -> QuantAgent / MarketStructureAgent / OrderBookAgent /
    RegimeAgent -> ConsensusEngine.combine() -> ConsensusResult -> RiskOverlay.assess() -> RiskAssessment -> Signal
    |
    v
Phase 5 (IMPLEMENTED, ACCEPTED): Signal -> PaperTradingEngine.process_signal()
    -> PaperOrder/PaperFill -> PaperPosition (deterministic, in-memory, no ConsensusResult/
    RegimeContext dependency)
    |
    v
Phase 6 (IMPLEMENTED, ACCEPTED): RuntimeCoordinator continuously drives the
    above pipeline from live Binance PUBLIC WebSocket/REST data (bootstrap, candle
    window dedup/gap-detection, staleness health, graceful shutdown)
    |
    v
Phase 7 (IMPLEMENTED, ACCEPTANCE-READY): PersistedRuntime wraps RuntimeCoordinator,
    durably checkpointing Phase 5 paper state + Phase 6 candle continuity (SQLite),
    and restores both deterministically after a process restart
    |
    v
Phase 8+ (NOT YET IMPLEMENTED, far future): Ubuntu deployment/operations, then
    long-run soak testing, then Binance Testnet Lab (execution NEVER touches Mainnet)
```

## 5. Immutable core contracts / invariants (do not silently change)

- `crypto_signal_engine` is a real, pip-installable Python package
  (`pyproject.toml`), not a script collection.
- `ALLOW_LIVE_TRADING = False` (`crypto_signal_engine/__init__.py`) — a
  hard, code-level safety invariant, verified by an automated repository
  safety scan (`tests/test_repository_safety_scan.py`).
- No `BinanceLiveExecutionAdapter` exists or may ever be created.
- No private/signed Binance endpoints, API keys, HMAC signing, or
  execution capability exist anywhere in the codebase.
- `CandleIdentity = (symbol, timeframe, open_time)` — `close_time` is
  NEVER used as a sequencing or uniqueness key anywhere in the system
  (Phase 1 decision, re-affirmed and protected through Phase 2 and 3).
- All domain timestamps are UTC-aware; naive datetimes are rejected.
- All domain numeric fields must be finite (no NaN/inf anywhere).
- All enum-typed fields are runtime-validated via `require_enum()` — type
  hints alone are never trusted.
- Nested mutable collections (dict/list fields on frozen dataclasses) are
  wrapped in real immutable views (`freeze_mapping()`), not just annotated.
- `SafetyState` is fully encapsulated (private fields + read-only
  properties); illegal states are physically unrepresentable.
- `Signal.direction` (Phase 1) is a derived property of `score`, never a
  separately settable field — no dual source-of-truth.

## 6. Mainnet / Paper / Testnet safety boundary (permanent, cross-phase)

```
PRODUCTION BINANCE MAINNET
        |
        +-- READ-ONLY MARKET DATA ONLY
             |
             v
        ANALYSIS ENGINE (Phase 1-4 agents)
PAPER TRADING (Phase 5, ACCEPTED; driven continuously in real time by Phase 6,
    ACCEPTED; durably persisted and restart-recoverable via Phase 7, ACCEPTANCE-READY)
        |
        +-- INTERNAL SIMULATION ONLY, NO Binance order API — consumes only
            completed Signal objects; deterministic in-memory bookkeeping
BINANCE TESTNET LAB (future phase, isolated, not yet implemented)
        |
        +-- TEST FUNDS ONLY, isolated execution testing
```

No phase may create a shortcut between Mainnet and any execution path.
`create_order`, `place_order`, `cancel_order`, and all account/trading
endpoints are forbidden in every phase implemented so far and must remain
forbidden until an explicit, dedicated execution phase (not yet reached)
deliberately and narrowly introduces Testnet-only, non-executing-by-default
scaffolding under its own hard invariants.

## 7. Phase 1 — domain rules (summary)

- Domain models: `Candle`, `Trade`, `OrderBookLevel`, `OrderBookSnapshot`,
  `AgentEvidence`, `Signal`, `SafetyEvent`, `SafetyState`,
  `ConsensusResult`, `RiskAssessment`, `RegimeContext`
  (`StructureRegime` + `VolatilityRegime` + `LiquidityRegime` — three
  independent, non-mutually-exclusive axes, not one enum).
- `CandleSequencer` (domain-level, pure, in-memory): decides
  ACCEPTED_UPDATE / ACCEPTED_FINAL / REJECTED_DUPLICATE /
  REJECTED_OUT_OF_ORDER / REJECTED_AFTER_FINAL for candle updates, keyed
  by `CandleIdentity`, using an explicit `update_seq` (never `close_time`).
- `CandleStateStore` (ABC) + `InMemoryCandleStateStore`: quality gate runs
  **before** any commit; rejected-quality data never reaches the sequencer
  or persistence.
- `DataQualityGate` (ABC): `check_candle`, `check_trade`, `check_order_book`
  — quality is about "is this data trustworthy", never about strategy.
- Full details: `ARCHITECTURE.md` (Phase 1 section), `DECISIONS.md`
  (Decisions 1-22), `SAFETY_INVARIANTS.md`.

## 8. Phase 2 — Binance market-data rules (summary)

- `providers/binance/`: REST client (deterministic pagination, retry,
  rate-limit awareness), WebSocket provider (kline/aggTrade/depth streams),
  parser (hardened against malformed/missing/wrong-type payloads),
  reconnect policy (exponential backoff + jitter + bounded, injectable
  clock/sleeper for fast deterministic tests).
- **Order-book sync** (`order_book_sync.py`) implements Binance's official
  snapshot+diff algorithm with **overlap semantics**: an incoming diff is
  accepted if `event.U <= local_last_update_id + 1` (not strict equality)
  — Binance may legitimately redeliver overlapping ranges. A true gap
  (`event.U > local_last_update_id + 1`) triggers resync.
- **Stale-feed detection** is real: `_recv_or_stale()` bounds
  `connection.recv()` with `asyncio.wait_for` (real event-loop time, by
  deliberate design — see Decision 33) using
  `BinanceConfig.stale_feed_threshold_seconds`; on timeout, health goes
  DEGRADED, the connection is closed, and the existing reconnect path
  (which always resyncs the order book) is triggered. Cancellation and
  stale-timeout are structurally distinct exception paths.
- **Candle gap recovery** is deterministic: `StateManager.latest_candle()`
  (a safe public API, not a private-attribute hack) is used to compute the
  exact missing interval `[latest.open_time + duration, incoming_open_time)`.
  The REST response is filtered to **only** the expected missing
  open_times — Binance's `endTime` boundary behavior can return the
  incoming candle itself (or other out-of-range candles), and those are
  never committed via the historical/backfill path. The original live WS
  update is always the one that becomes canonical for its own identity.
- **SQLite commit atomicity**: `SqliteCandleStateStore.commit()` dry-runs
  sequencing on a `copy.deepcopy` scratch sequencer first; only if the SQL
  persist succeeds is the real in-memory sequencer advanced. A persist
  failure leaves canonical in-memory state completely unchanged.
- Structural vs. freshness quality split: `BinanceDataQualityGate.
  check_historical_candle()` (concrete-class-only, not part of the Phase 1
  ABC) applies structural checks only (alignment, gap, zero-volume,
  outlier) and skips staleness — a backfilled candle is naturally "old"
  and that is not an error.
- Full details: `PHASE2_MARKET_DATA.md`, `DECISIONS.md` (Decisions 23-37).

## 9. Phase 3 — Feature Engine contracts (summary)

- `features/domain.py`: `FeatureIdentity` (symbol, timeframe, feature_name),
  `FeatureValue` (identity, as_of, finite value), `FeatureSnapshot`
  (symbol, timeframe, as_of, immutable `values` mapping, optional
  `context_id`). `FeatureSnapshot.from_feature_values()` rejects
  mixed-symbol, mixed-timeframe, future-relative-to-snapshot, and
  duplicate-name evidence.
- `features/registry.py`: `FeatureRegistry` is **instance-scoped** (no
  hidden global state); `FeatureConfig` names are deterministic and
  parameter-encoded (e.g. `EMA_20`, `BOLLINGER_UPPER_20_2`,
  `BID_DEPTH_10`); registering the same name with different parameters is
  rejected.
- Calculators (`candle_calculators.py`, `orderbook_calculators.py`,
  `trade_calculators.py`) are pure functions with explicit warm-up
  (`InsufficientHistoryError`) and calculation-failure
  (`FeatureCalculationError`) semantics — no NaN/inf ever escapes.
- `FeatureEngine` (`engine.py`) is the only place that:
  - enforces the **closed-candle policy** (any `is_closed=False` candle
    **within** the evaluation horizon is rejected), and
  - enforces the **no-look-ahead guarantee** (Section 11 below).
- `FeatureHistoryStore` (`state.py`) is bounded, symbol/timeframe isolated,
  and completely separate from Phase 2's market-data state — it never
  silently lets an older snapshot overwrite a newer canonical one; same
  `as_of` upserts idempotently.
- Full details: `PHASE3_FEATURE_ENGINE.md`, `DECISIONS.md`
  (Decisions 38-45).

## 10. Timeframe architecture

Four independent timeframes are supported end-to-end (Phase 2 config,
Phase 3 feature state): **1h** (regime), **15m** (structural trend),
**5m** (primary signal), **1m** (microstructure/timing confirmation — this
exact term is used deliberately; "execution timing" is avoided because no
execution concept exists in the analysis engine, see Decision 3).
`FeatureHistoryStore` keys everything by `(symbol, timeframe)`; the four
timeframes never share state.

## 11. Closed-candle and no-look-ahead rules (hard acceptance gates)

- **Closed-candle policy**: candle-based technical indicators operate
  **only** on `is_closed=True` candles. There is no provisional/in-progress
  feature support in this codebase yet; if added later it must be
  explicitly separate, clearly named, and opt-in (never silently blended
  with production features).
- **No-look-ahead** (Phase 3 Decision 39 + 45, hard gate): `FeatureEngine.
  compute_candle_features()` / `compute_trade_features()` **structurally**
  filter their input to `close_time <= as_of` (candles) /
  `timestamp <= as_of` (trades) **before** any other validation. This
  ordering is critical: a candle whose `close_time > as_of` is skipped
  **regardless of its `is_closed` status** (a future, still-forming candle
  is normal and must not raise); only a candle **within** the horizon that
  is not yet closed is rejected. This was a real bug found and fixed
  post-acceptance (Decision 45) — any future change to `_prepare_candles`
  must preserve this exact ordering and its two regression tests
  (`test_future_open_candle_ignored_without_exception`,
  `test_unfinished_candle_within_horizon_still_rejected`).
- `FeatureHistoryStore.as_of()` and `FeatureEngine.snapshot_as_of()` never
  return a snapshot whose `as_of` is later than the requested timestamp —
  this is the only safe cross-timeframe alignment access point.


## 12. DECIDED decisions (see DECISIONS.md for full text)

50 decisions are recorded in `DECISIONS.md`, grouped by phase:
- **1-14**: original Phase 1 architecture (RiskOverlay separation,
  agreement formula, terminology).
- **15-22**: Phase 1 final hardening (package structure, SafetyState
  encapsulation, runtime enum safety, Signal provenance, immutability,
  symbol/finite policies, repo cleanup, reproducible offline acceptance
  via prebuilt wheel).
- **23-30 → 31-37**: Phase 2 initial implementation, then reviewer
  hardening rounds (order-book overlap semantics, SQLite atomicity, real
  stale-feed detection, deterministic gap recovery, REST-endTime-boundary
  exclusion fix, minor boundary hardening).
- **38-45**: Phase 3 Feature Engine (error taxonomy placement, structural
  no-look-ahead filtering, RSI neutral convention, insufficient-history
  omission policy, order-book timeframe placeholder, VWAP price basis,
  in-memory-only persistence choice, no-look-ahead ordering bugfix).
- **46-50**: Phase 4 Quant & Agent Engine (ConsensusEngine regime
  keyword-only parameter — reported contract adaptation, RegimeAgentOutput
  wrapper, deterministic SHA-256 context-ID, positive-only conviction
  attenuation multipliers, neutral-consensus supporting/contradicting
  policy).
- **51-54**: Phase 5 Paper Trading / Simulation Engine (Signal-only
  boundary with no dependency on `ConsensusResult`/`RegimeContext`, fixed
  notional-based position sizing as a deliberate scope limit, NEUTRAL =
  NO_ACTION acceptance-review blocker fix, configurable deterministic
  fee/slippage acceptance-review blocker fix).
- **55-59**: Phase 6 Real-Time Market Data & Runtime (runtime's own
  `CandleWindow` as a complement to, not a mirror of, Phase 2's private
  provider state; single signal-generation boundary at M5 close only; gap
  detection via an optional `duration` parameter on `CandleWindow`; the
  `run()` silent-error-swallowing acceptance-review blocker fix; Signal-only
  boundary with Phase 4/5 left unmodified).
- **60-62**: Phase 7 Persistence & Recovery (`restore_paper_engine()` as
  the one documented exception that writes to Phase 5's private
  `_states`; reason-scoped persistence-fault tracking acceptance-review
  blocker fix; rebuildable candle/feature history deliberately not
  persisted — reconstructed from PUBLIC REST instead).

## 13. Backlog / tech debt / deferred items

- Quality thresholds (`BinanceQualityThresholds`) are hardcoded constants;
  moving them to a `config/risk.yaml`-style file is deferred (Decision 29).
- Real network-backed `WebSocketConnectionFactory` (via the `websockets`
  library) was deferred at Phase 2 time (Decision 28) but **has since
  been provided by Phase 6**: `providers/binance/real_websocket.py`
  (lazy-import, so the core package still has no hard dependency on
  `websockets`; only code that actually wants a live connection imports
  it). Verified against real Binance PUBLIC WebSocket/REST during Phase 6
  delivery. Do not reintroduce the earlier "not implemented" claim.
- Trade-based (true, trade-level) VWAP is deferred in favor of
  candle-level typical-price VWAP (Decision 43).
- SQLite persistence for Feature Engine snapshots was intentionally not
  added; in-memory `FeatureHistoryStore` was judged sufficient (Decision 44).
- `DataQualityStatus` enum names are candle-oriented but reused for
  trade/order-book contexts via closest semantic match (Decision 24).
- Trade-flow feature integration into OrderBookAgent's M1 snapshot was
  deliberately NOT pursued (Phase 4 Section 12) — would require changing
  accepted Phase 3 state semantics (separate feature-family snapshots are
  not merged); out of scope.
- Phase 5 `PaperTradingEngine` position sizing is a fixed notional per
  position, not scaled by `Signal.confidence`/`risk_level` — a deliberate
  Phase 5 scope limit, explicitly confirmed during acceptance and not to
  be changed without a new decision (Decision 52).
- Phase 5 models no partial fills/limit orders — market-order, full-fill
  simulation only (see PHASE5_PAPER_TRADING.md, "Sınırlamalar"). **Fee and
  slippage ARE modeled** (`fee_bps`/`slippage_bps`, deterministic,
  configurable, default `0.0`) — this was corrected after an acceptance
  review found it missing and misdocumented (Decision 54); do not
  reintroduce the earlier "no fee/slippage" claim.
- Phase 6's own gap detection (`CandleWindow`) only sees gaps from the
  *runtime's* perspective (the stream it actually consumed) — it cannot
  detect a gap that occurred entirely inside Phase 2's private canonical
  state before ever reaching the runtime. In practice this is extremely
  unlikely (the provider only ever emits candles it has itself accepted
  into its own canonical state), and remains Phase 2's own responsibility
  regardless (Decision 55).
- Phase 6 does not consume the trade stream (`stream_trades`) — no Phase 4
  agent depends on trade-flow features (an existing, already-documented
  Phase 4 scope decision, Section 13 above); Phase 6 does not introduce a
  new dependency that would change this.
- Phase 7 deliberately does NOT persist `CandleWindow`'s full candle
  history or `FeatureHistoryStore`'s snapshots — only a single `open_time`
  checkpoint per (symbol, timeframe). This data is reconstructed from
  PUBLIC REST on recovery (Decision 62); this means recovery requires
  network access, matching Phase 6's own live-data requirement (not a new
  limitation Phase 7 introduces).
- Phase 7's checkpoint write happens *after* `PaperTradingEngine.
  process_signal()` has already mutated in-memory state — true two-phase
  commit is not possible without redesigning Phase 5 (explicitly
  forbidden). A failed checkpoint degrades the affected symbol
  (`mark_persistence_fault`) rather than attempting a rollback; this is
  documented, accepted behavior, not a bug (Decision 60,
  PHASE7_PERSISTENCE_RECOVERY.md).

## 14. Known non-blocking limitations

- `LiveDataProvider.health()` has no symbol parameter in the Phase 1 ABC;
  `BinanceMarketDataProvider.health()` returns the most-recently-updated
  stream's snapshot as a documented interpretation (Decision 25).
- Backfill's synthetic `update_seq` uses `open_time` (ms), intentionally
  distinct from live WS `update_seq` (event time `E`); documented, does
  not cause incorrect sequencing in practice.
- **Reported Phase 4 contract deviation (non-blocking)**:
  `ConsensusEngine.combine(evidence, *, regime: RegimeContext)` accepts
  `regime` as a keyword-only parameter, because Phase 1's ALREADY-ACCEPTED
  `ConsensusResult` model has a mandatory (non-optional) `regime` field
  (Decision 12) — this directly conflicts with the Phase 4 instruction's
  literal "combine() must never accept RegimeContext" rule. Resolved per
  the "actual code wins" meta-rule: `ConsensusResult` was NOT modified;
  `regime` participates in NO scoring/agreement math (verified by test
  `test_does_not_require_regime_context_influence_on_score`). Full
  rationale: `PHASE4_QUANT_AGENT_ENGINE.md` and Decision 46.
  **Status as of Phase 5: still unresolved and unchanged.** Phase 5 does
  not touch `ConsensusResult`, `RegimeContext`, or `ConsensusEngine` in any
  way (Decision 51) — it neither fixes nor deepens this item. It remains
  exactly the same reported, tested, non-blocking Phase 4 item described
  above. **Status as of Phase 6: still unresolved and unchanged** — Phase
  6 also has zero dependency on `ConsensusResult`/`RegimeContext`
  (Decision 59); it is untouched by a second phase in a row. **Status as
  of Phase 7: still unresolved and unchanged** — Phase 7 persists/restores
  `Signal` objects verbatim and has zero dependency on `ConsensusResult`/
  `RegimeContext` (Decision 60); untouched by a third phase in a row.

## 15. Latest test / build / verification status

As of `crypto_signal_engine_phase4_FINAL.zip`:

- **745 tests passing, 0 failing** (Phase 1: 290, Phase 2: 204, Phase 3:
  133, Phase 4: 118) when run from a fully offline, freshly-unzipped copy
  with no prior `pip install` (1 test skips gracefully in that scenario —
  expected, not a failure, see below).
- `python3 -m pytest -q` — self-contained: `tests/test_package_import.py`
  builds its own isolated venv and installs the shipped `dist/*.whl`
  offline (`--no-index --no-deps`); one test in that file skips gracefully
  if the package is not already installed in the ambient environment.
- `python3 scripts/verify_phase1.py` — runs wheel check, isolated offline
  install, external-directory import smoke test, full pytest, `compileall`,
  and the repository safety scan, in one command, with no network access
  and no build-backend invocation. PASS.
- `python3 -m compileall crypto_signal_engine tests scripts` — PASS.
- Repository safety scan (`tests/test_repository_safety_scan.py`) — PASS.
- One serious adversarial acceptance review was performed for Phase 4
  (per its master prompt's Section 56); no BLOCKERs were found. The one
  material discrepancy identified (ConsensusEngine/RegimeContext, Section
  14 above) was classified as a reported, tested, non-blocking contract
  adaptation, not a defect.
- The shipped `dist/*.whl` was rebuilt from the final source tree
  immediately before packaging (no stale wheel) and independently
  re-verified after re-extracting the final ZIP into a separate clean
  directory.

**Corrected Phase 4 baseline count** (for anyone comparing against later
phases): **744 passed, 1 skipped, 0 failed** (745 collected) — this is the
precise breakdown of the "745 tests" figure above.

**As of Phase 5** (committed and tagged: `phase5-accepted`, on top of
`phase4-accepted`):

- **799 passed, 1 skipped, 0 failed** (800 collected) — the same 1
  expected skip as Phase 4, plus 55 new Phase 5 tests
  (`tests/test_paper_trading_models.py`, `tests/test_paper_trading_engine.py`),
  0 Phase 1-4 regressions.
- `python3 -m compileall crypto_signal_engine tests scripts` — PASS.
- `python3 scripts/verify_phase1.py` — PASS (all 6 steps).
- Repository safety scan (`tests/test_repository_safety_scan.py`) —
  9 passed.
- Phase 5 went through two acceptance review rounds, each finding and
  fixing one BLOCKER:
  1. NEUTRAL signals were incorrectly mapped to a "flatten position"
     target, closing open positions; corrected to a strict NO_ACTION
     early-return that never creates an order/fill or mutates
     position/account state (Decision 53).
  2. The specification's required configurable deterministic fee/slippage
     (`fee_bps`, `slippage_bps`, directional slippage-adjusted fill
     pricing, a `fee` persisted on every fill, fee-netted realized PnL)
     was entirely unimplemented — the prior documentation had incorrectly
     described this as a deliberate Phase 5 scope limit rather than a
     gap. Now implemented; zero-default (`fee_bps=0.0`, `slippage_bps=0.0`)
     preserves the prior zero-cost behavior exactly (Decision 54).
  A third, change-focused review pass (scoped to the fee/slippage change
  only) found no further blockers. Full detail: `PHASE5_PAPER_TRADING.md`.

**As of Phase 6** (committed and tagged: `phase6-accepted`, commit
`5978c0c`, on top of `phase5-accepted`):

- **869 passed, 1 skipped, 0 failed** (870 collected) — the same 1
  expected skip as Phase 1-5, plus 70 Phase 6 tests
  (`tests/test_runtime_models.py`, `tests/test_runtime_candle_window.py`,
  `tests/test_runtime_health.py`, `tests/test_runtime_bootstrap.py`,
  `tests/test_runtime_coordinator.py`), 0 Phase 1-5 regressions.
- `python3 -m compileall crypto_signal_engine tests scripts` — PASS.
- `python3 scripts/verify_phase1.py` — PASS (all 6 steps).
- Repository safety scan (`tests/test_repository_safety_scan.py`) —
  9 passed (automatically covers the new `runtime/` package and
  `providers/binance/real_websocket.py`).
- Two acceptance review rounds were performed for Phase 6:
  1. An in-session review found and fixed **one BLOCKER**:
     `RuntimeCoordinator.run()`'s own `asyncio.gather(*self._tasks,
     return_exceptions=True)` could silently absorb an unexpected
     exception from a stream-consumer task with no observable trace
     anywhere; corrected so the affected symbol is explicitly marked
     `DEGRADED` before the exception is re-raised and gathered
     (Decision 58).
  2. An **independent** blocker review (against the actual repository
     snapshot) found and fixed **three further BLOCKERs**: (a) failed
     runtime gap recovery could leave a symbol falsely `READY` — fixed
     with a per-(symbol, timeframe) gap fault that only clears on
     genuine continuity restoration; (b) `ingest_candle`/`bootstrap_candles`/
     `ingest_order_book` trusted caller-supplied `(symbol, timeframe)`
     parameters instead of validating the actual domain object's own
     identity — fixed with fail-fast, pre-mutation `IdentityMismatchError`
     checks; (c) `READY` could be reached from candle warmup alone,
     without the M1 order-book evidence Phase 4's `SignalEngine` actually
     requires — fixed by gating `mark_ready()` on both conditions.
- Additionally verified against **real Binance PUBLIC market data** (not
  part of the offline pytest suite, and not required for it to pass):
  `scripts/live_public_smoke_test.py` received and normalized live M1
  candle events, and `RuntimeCoordinator.bootstrap()` successfully
  brought BTCUSDT's M5/M15/H1 all to `ready=True`
  (`status().overall_health == READY`) against the real REST API. Full
  detail: `PHASE6_REALTIME_RUNTIME.md`.

**As of Phase 7** (current working tree, on top of Git tag
`phase6-accepted`, not yet committed/tagged itself):

- **903 passed, 1 skipped, 0 failed** (904 collected) — the same 1
  expected skip as Phase 1-6, plus 34 new Phase 7 tests
  (`tests/test_persistence_serialization.py`,
  `tests/test_persistence_paper_state_store.py`,
  `tests/test_persistence_recovery.py`, plus 5 new tests added to
  `tests/test_runtime_health.py`), 0 Phase 1-6 regressions.
- `python3 -m compileall crypto_signal_engine` — PASS.
- `python3 scripts/verify_phase1.py` — PASS (all 6 steps).
- Repository safety scan (`tests/test_repository_safety_scan.py`) —
  9 passed (automatically covers the new `persistence/{errors,
  serialization,paper_state_store,recovery}.py` files).
- One serious blocker-focused adversarial acceptance review was performed
  for Phase 7. **One BLOCKER was found and fixed**: a successful
  checkpoint of one kind (e.g. paper-state) could clear the SAME
  per-symbol persistence-fault flag that an unrelated, still-unresolved
  checkpoint failure of another kind (e.g. candle) had just set —
  masking a real durability gap. Corrected: persistence faults are now
  tracked per `(symbol, reason)`, mirroring the Phase 6 gap-fault design
  (Decision 61). Full detail: `PHASE7_PERSISTENCE_RECOVERY.md`.

## 16. Next phase: Phase 8+

Phase 7 (Persistence & Recovery) is now ACCEPTANCE-READY (see Section 19
below and `PHASE7_PERSISTENCE_RECOVERY.md`) — implemented, tested, and
reviewed, but not yet formally ACCEPTED (commit/tag to Git still pending
the project owner's sign-off). The agreed roadmap for what follows: Phase
8 (Ubuntu deployment/operations), Phase 9 (long-run stability/soak
testing), Phase 10 (Binance Testnet Execution Lab, isolated,
test-funds-only), Phase 11 (execution safety & reconciliation), Phase 12
(final local production readiness). None of Phase 8-12 has been started.
Do not reinterpret earlier documentation language (e.g. old "Testnet
Phase 7+" phrasing predating this roadmap) as permission to move Testnet
earlier than Phase 10.

### Non-goals carried forward

- No real/Testnet/Mainnet execution, no order placement against any live
  exchange, exists anywhere in the codebase — Phase 5's paper trading is
  purely an internal, in-memory simulation (see Section 6); Phase 6 only
  feeds it real-time PUBLIC data, Phase 7 only persists/restores it —
  neither adds execution.
- No systemd/Docker/deployment (deferred to Phase 8), no multi-day soak
  test campaign (Phase 9), no Binance Testnet execution (Phase 10), no
  execution safety/reconciliation layer (Phase 11), no dashboard, no
  Telegram notifications.
- No backtesting engine. **UPDATE (Pre-Audit Enhancement Pass, post-
  Phase 12, see PRE_AUDIT_ENHANCEMENTS.md and DECISIONS.md Karar 83):** a
  deterministic historical replay/backtest harness now exists in a
  separate, additive `research/` package, reusing `RuntimeCoordinator`
  unmodified. This bullet stays here as the accurate historical record
  of Phase 1-7's own scope — it is no longer true of the repository as a
  whole as of the Pre-Audit Enhancement Pass.
- No confidence-based position sizing (Phase 5 scope limit, Decision 52),
  no partial fills/limit orders (market-order, full-fill only) — see
  `PHASE5_PAPER_TRADING.md`. Fee/slippage **are** modeled (Decision 54);
  do not list them as absent. **UPDATE (Pre-Audit Enhancement Pass, see
  DECISIONS.md Karar 86/88):** confidence/risk_level-based PAPER sizing
  now exists in `research/sizing.py`, wired through a small, additive,
  default-preserving `PaperTradingEngine.process_signal(...,
  notional_override=None)` extension — Phase 5's OWN default
  (fixed-notional) behavior is unchanged and remains the default when no
  sizing policy is used. Partial fills/limit orders remain out of scope,
  unchanged.
- Phase 6 does not consume the trade stream and does not implement its
  own reconnect/backoff loop — it reuses Phase 2's `stream_candles()`/
  `stream_order_book()` (which already reconnect internally) as-is; see
  `PHASE6_REALTIME_RUNTIME.md`.
- Phase 7 does not persist rebuildable candle/feature history (only a
  single per-(symbol, timeframe) checkpoint) — see `PHASE7_PERSISTENCE_RECOVERY.md`.
  Do not "improve" this into full state serialization without a genuine
  reason; it is a deliberate design decision (Decision 62), not an
  oversight.
- `ALLOW_LIVE_TRADING` must remain `False`; no execution adapter of any
  kind may be introduced without an explicit, dedicated future phase that
  narrowly and deliberately scopes it under its own hard invariants.

### Continuation instructions for a fresh conversation

1. Start from the local Git repository, at or after the tag
   `phase6-accepted` (or a later accepted Phase 7 tag, once created) —
   treat its actual tracked source tree as the only source of truth. Do
   not regenerate Phases 1-7 from any prior prompt text.
2. Read this file, then `PROJECT_SUMMARY.md`, `ARCHITECTURE.md`,
   `DECISIONS.md` (especially Decisions 1, 12, 24, 25, 39, 45, 46, 51-62),
   `PHASE4_QUANT_AGENT_ENGINE.md`, `PHASE5_PAPER_TRADING.md`,
   `PHASE6_REALTIME_RUNTIME.md`, and `PHASE7_PERSISTENCE_RECOVERY.md`
   before writing any new code.
3. Preserve every invariant listed in Sections 5, 6, 11 of this document,
   the Phase 4 agent-independence and no-look-ahead guarantees described
   in `PHASE4_QUANT_AGENT_ENGINE.md`, the Phase 5 determinism/idempotency/
   atomicity/no-look-ahead/multi-symbol-isolation guarantees described in
   `PHASE5_PAPER_TRADING.md`, the Phase 6 bootstrap/gap-detection/
   staleness/instance-isolation/single-signal-boundary guarantees
   described in `PHASE6_REALTIME_RUNTIME.md`, and the Phase 7 durable
   checkpoint/schema-versioning/corruption-safety/restart-idempotency
   guarantees described in `PHASE7_PERSISTENCE_RECOVERY.md`.
4. Treat the Mainnet/Paper/Testnet boundary (Section 6) as the single most
   important constraint for any execution-adjacent work: Mainnet stays
   read-only market data forever (Phase 6's `RuntimeCoordinator`/
   `real_websocket.py` only ever connect to PUBLIC streams); Phase 5's
   paper trading never touches a real order API; any future Testnet
   execution code must be isolated and never default to live.
5. Do not modify Phase 1-7 domain models/contracts unless a genuine
   contract gap blocks implementation, and if so, stop and report the gap
   before changing anything (the working convention throughout Phases
   1-7). In particular, do not modify `ConsensusResult`/`RegimeContext` to
   "clean up" the Phase 4 reported deviation (Section 14) — it is
   accepted, non-blocking, and out of scope for any phase that does not
   explicitly target it. Do not add a public "restore/seed state" API to
   `PaperTradingEngine` either — Phase 7's `restore_paper_engine()` is a
   documented, minimal exception, not a precedent for expanding Phase 5's
   public surface.
6. Run the full existing suite (904 tests: 744 passed + 1 skipped from
   Phase 1-4, + 55 from Phase 5, + 70 from Phase 6, + 34 from Phase 7)
   before adding anything, keep it passing throughout, and run
   `scripts/verify_phase1.py` before final delivery.
7. Deliver following the same verification discipline used in Phases 1-4
   (full suite, `compileall`, repository safety scan) — Phase 5 onward
   uses the Git tag as the baseline artifact instead of a delivered ZIP;
   do not create a ZIP unless explicitly asked to. Phase 8 (Ubuntu
   deployment/operations) is the natural next candidate once Phase 7 is
   formally accepted.

## 17. Phase 5 — Paper Trading (summary)

- `paper_trading/models.py`: `MarketPriceSnapshot`, `PaperOrder`,
  `PaperFill` (carries a `fee` field), `PaperPosition`, `PaperTradingResult`,
  `OrderSide`, `PositionSide` — all frozen dataclasses reusing Phase 1's
  `domain._validation` helpers.
- `paper_trading/engine.py`: `PaperTradingEngine.process_signal(signal,
  price)` — the only entry point. Consumes `Signal` (Phase 4) plus a
  `MarketPriceSnapshot` (Phase 5's own minimal price projection; Phase 5
  does not consume `Candle`/`OrderBookSnapshot` directly).
- Strategy: LONG-group/SHORT-group `SignalDirection` maps to a target
  LONG/SHORT position (fixed notional sizing, Decision 52); a reversal
  closes then reopens atomically; a same-direction repeat is a no-op
  (only `updated_at` refreshes). **NEUTRAL is NO_ACTION** (Decision 53) —
  never closes a position, never creates an order/fill.
- **Configurable deterministic fee/slippage** (Decision 54):
  `PaperTradingEngine(fee_bps=0.0, slippage_bps=0.0)`, both finite and
  `>= 0` (rejects negative/NaN/inf). Fill price is shifted directionally
  (`BUY: P*(1+slippage_bps/10000)`, `SELL: P*(1-slippage_bps/10000)`);
  every `PaperFill.fee = abs(quantity*fill_price)*fee_bps/10000`, computed
  on the actual (slippage-adjusted) fill price and persisted on the fill.
  All entry/exit accounting (`average_entry_price`, gross/net realized
  PnL) uses the actual fill price, never the raw `MarketPriceSnapshot.price`.
  Net realized PnL nets out both the entry fee (retained privately across
  the position's lifetime) and the exit fee exactly once — no double
  counting. Zero defaults preserve the prior zero-cost behavior exactly.
- Guarantees: determinism (no uuid/random/wall-clock), idempotency
  (replaying a `(symbol, context_id)` is a no-op, including no second fee
  charge; a conflicting replay raises `IdempotencyConflictError`),
  atomicity (state only commits after all computation succeeds),
  no-look-ahead (`NoLookAheadViolationError` on future prices, cross-symbol
  prices, or out-of-order signals per symbol), multi-symbol isolation
  (independent per-symbol state and fee accounting, independent
  per-engine-instance state).
- Zero dependency on `ConsensusResult`/`RegimeContext`/`agents/*`/
  `consensus/*` (Decision 51) — the Phase 4 `regime` contract deviation
  (Section 14) is untouched by Phase 5.
- Full detail: `PHASE5_PAPER_TRADING.md`, `DECISIONS.md` (Decisions 51-54).

## 18. Phase 6 — Real-Time Market Data & Runtime (summary)

- `runtime/models.py`: `RuntimeHealth`, `IngestOutcome` (incl.
  `GAP_DETECTED`), `MarketEventKind`, `RuntimeCycleResult`,
  `ProcessedMarketEvent`, `BootstrapReport`, `SymbolHealth`,
  `RuntimeStatus` — frozen dataclasses reusing Phase 1's
  `domain._validation` helpers.
- `runtime/candle_window.py`: `CandleWindow` — a per-(symbol, timeframe),
  bounded, in-memory window the runtime maintains itself, because Phase
  2's `BinanceMarketDataProvider` keeps its own canonical state fully
  private and Phase 3's `FeatureEngine` is stateless (expects the full
  candle list on every call). Identity-based (`open_time`) dedup +
  monotonic ordering + gap detection (an optional `duration` constructor
  parameter — Decision 55) — NOT a reimplementation of Phase 2's own
  `CandleSequencer`/`DataQualityGate` (no OHLC/quality validation here;
  that's already done before a `Candle` ever reaches this window).
- `runtime/health.py`: `HealthMonitor` — reuses Phase 2's existing `Clock`
  protocol (no new time abstraction); staleness policy is "time since the
  last ACCEPTED event exceeds `stale_feed_threshold_seconds`", which
  subsumes disconnect detection without a separate connection-state
  poller.
- `runtime/bootstrap.py`: `apply_bootstrap_candles()` — deterministic,
  synchronous, network-free; called both by `RuntimeCoordinator.
  bootstrap()` (async REST orchestration) and by live gap-fill
  (`resolve_gap()`), so both code paths share one aggregation/readiness
  rule.
- `runtime/coordinator.py`: `RuntimeCoordinator` — instance-scoped (no
  module-level mutable state anywhere); owns its own `CandleWindow`s,
  `HealthMonitor`, shared `FeatureHistoryStore`, one `SignalEngine`
  instance, and the injected `PaperTradingEngine`. `SignalEngine.evaluate()`
  is called from exactly one place (`_maybe_evaluate_signal`), triggered
  only when an M5 (primary_timeframe) candle is newly `ACCEPTED` —
  M15/H1/order-book updates feed features but never trigger evaluation
  (Decision 56). The reference price handed to `PaperTradingEngine.
  process_signal()` is always the closing M5 candle that produced the
  signal, so `price.as_of == signal.timestamp` structurally, never
  weakening Phase 5's `price.as_of <= signal.timestamp` check.
- **Minimum bootstrap warm-up is 20 closed candles** per M5/M15/H1 (the
  largest `min_history` among the candle-features any Phase 4 agent
  actually requires — `RSI_14`, `ROC_10`, `VWAP_DEVIATION_20`,
  `RELATIVE_VOLUME_20`, `BOLLINGER_BANDWIDTH_20_2`, `DIST_FROM_HIGH_20`,
  `DIST_FROM_LOW_20`); `RuntimeCoordinator` enforces `warmup_candles >= 20`
  at construction (default 25). M1 needs no candle warm-up — only
  order-book microstructure features are committed under the M1 bucket,
  matching the existing Phase 4 wiring.
- **Gap recovery**: `resolve_gap()` fetches only the missing range via
  Phase 2's existing `provider.fetch_historical_candles()` (no new REST
  mechanism), applies it through the same `bootstrap_candles()` path, then
  retries the pending candle. A gap is never silently jumped over;
  `ingest_candle()` marks the affected `(symbol, timeframe)` explicitly
  `DEGRADED` the instant `GAP_DETECTED` occurs (not only after a failed
  recovery attempt), and only clears it on a genuine subsequent `ACCEPTED`
  candle for that exact key — an unrelated order-book event never clears
  it (independent-review blocker fix, Decision 55).
- **Identity validation**: `ingest_candle`/`bootstrap_candles`/
  `ingest_order_book` validate the actual `Candle`/`OrderBookSnapshot`
  object's own `symbol`/`timeframe` against the caller-declared
  parameters, fail-fast with `IdentityMismatchError` before any state
  mutation (independent-review blocker fix).
- **READY requires order-book evidence too**: `_maybe_mark_symbol_ready()`
  gates `READY` on BOTH full M5/M15/H1 candle warmup AND at least one
  accepted M1 order-book feature snapshot — matching what Phase 4's
  `SignalEngine`/`build_agent_context()` actually requires
  (independent-review blocker fix).
- **Reconnect**: Phase 2's `stream_candles()`/`stream_order_book()`
  already reconnect (bounded backoff, already tested in Phase 2) inside a
  single, never-recreated subscription per (symbol, timeframe)/symbol —
  `RuntimeCoordinator.run()` does not implement or test its own
  reconnect/backoff loop; it only adds staleness-based health and its own
  `CandleWindow` dedup as a second line of defense against any
  reconnect-induced redelivery.
- Optional, non-core additions for actually running live: `providers/
  binance/real_websocket.py` (lazy-imports `websockets`, so the package's
  core import chain has no hard dependency on it) and
  `scripts/live_public_smoke_test.py` (bounded timeout, no credentials,
  not part of the mandatory offline pytest suite). Both were verified
  against real Binance PUBLIC data during this delivery (Section 15).
- Zero dependency on `ConsensusResult`/`RegimeContext`/`agents/*`/
  `consensus/*` (Decision 59, mirroring Decision 51) — the Phase 4
  `regime` contract deviation (Section 14) is untouched by Phase 6, same
  as Phase 5.
- Full detail: `PHASE6_REALTIME_RUNTIME.md`, `DECISIONS.md` (Decisions 55-59).

## 19. Phase 7 — Persistence & Recovery (summary)

- `persistence/errors.py`: `SchemaVersionMismatchError`, `CorruptRecordError`
  — both extend the existing Phase 2 `PersistenceError`, not reinvented.
- `persistence/serialization.py`: pure `Signal`/`AgentEvidence`/
  `PaperPosition`/`PaperFill`/`PaperOrder` <-> JSON-safe dict functions.
  Domain models were NOT modified — no `to_dict()`/`from_dict()` method
  was added to any Phase 1/4/5 class.
- `persistence/paper_state_store.py`: `PaperStateStore` — SQLite, explicit
  `SCHEMA_VERSION`, one transaction per checkpoint (position + new
  processed-context rows + new fill/order rows, all-or-nothing).
- `persistence/recovery.py`: `restore_paper_engine()` — the ONE documented
  exception that writes directly to `PaperTradingEngine`'s private
  `_states` (Phase 5 offers no public restore API by design, and adding
  one would be "redesigning" Phase 5, which is forbidden; replaying
  through `process_signal()` would itself violate restart-idempotency).
  `PersistedRuntime` wraps `RuntimeCoordinator` (unmodified) and checkpoints
  after every `ACCEPTED` candle and every real (non-NEUTRAL, non-replay)
  paper-trading transition.
- **Durable**: per-symbol position/entry_fee/last_signal_timestamp; per
  processed-context the full `Signal` (for exact conflict detection) plus
  a position snapshot; the full fill/order ledger; per-(symbol, timeframe)
  a single candle `open_time` checkpoint.
- **Rebuilt, not persisted**: `CandleWindow`'s full history and
  `FeatureHistoryStore`'s snapshots — reconstructed from Phase 2's PUBLIC
  REST on recovery (Decision 62), anchored at the persisted checkpoint.
  M1 order-book state is never persisted either — it is inherently a
  "latest snapshot" that a live process naturally reacquires.
- **Restart idempotency**: replaying the same `(symbol, context_id)` after
  a full process restart (brand-new engine/coordinator/store objects) is
  still a no-op via Phase 5's own (unmodified) idempotency check; a
  conflicting replay still raises `IdempotencyConflictError`. Verified by
  direct blocker-level tests, not just integration tests.
- **Open-position recovery**: `entry_fee`/`average_entry_price` are never
  reset on restart; closing a recovered position produces gross/net
  realized PnL and fees identical to an uninterrupted single-process run.
- **Multi-symbol**: durable rows are isolated by `symbol` column;
  recovering/corrupting one symbol never touches another's durable or
  in-memory state.
- **Failure semantics**: `SchemaVersionMismatchError`/`CorruptRecordError`
  are raised explicitly, never silently swallowed into a fresh/empty
  state. A failed checkpoint write degrades the affected symbol
  (`mark_persistence_fault(symbol, reason)`) rather than attempting an
  in-memory rollback of Phase 5 state (not possible without redesigning
  Phase 5) — a documented, accepted limitation, not a defect.
- Zero dependency on `ConsensusResult`/`RegimeContext`/`agents/*`/
  `consensus/*` (Decision 60) — the Phase 4 `regime` contract deviation
  (Section 14) is untouched by Phase 7, same as Phase 5 and 6.
- Full detail: `PHASE7_PERSISTENCE_RECOVERY.md`, `DECISIONS.md` (Decisions 60-62).

## 20. Phase 8 — Operations & Ubuntu Deployment (summary)

- `crypto_signal_engine/ops/config.py`: `AppConfig`/`load_config()` — a
  single immutable, fail-fast config sourced entirely from `CSE_*`
  environment variables (no YAML/plugin framework). No field carries an
  API key/secret/credential (enforced by a dedicated test and, since this
  file lives under `crypto_signal_engine/`, automatically covered by the
  existing repository-wide safety scan). `AppConfig.binance_config()`
  deliberately does NOT expose REST/WS base URL overrides — defense in
  depth against the config surface being misused to point at Testnet or
  another execution endpoint (Decision 65).
- `crypto_signal_engine/ops/lock.py`: `ProcessLock` — POSIX `flock`
  (`LOCK_EX|LOCK_NB`); a crashed holder's lock is released by the kernel
  automatically, so stale-lock recovery needs no PID-file bookkeeping
  (Decision 63).
- `crypto_signal_engine/ops/health_snapshot.py`: writes
  `RuntimeCoordinator.status()` plus the last recovery outcome to a local
  JSON file atomically (write-temp-then-`os.replace`); no HTTP listener
  (Decision 64). `python -m crypto_signal_engine.app status` reads it.
- `crypto_signal_engine/ops/logging_setup.py`: stdlib `logging` only,
  stdout (captured by journald under systemd); idempotent configuration.
- `crypto_signal_engine/app.py`: `Application` is the Phase 8 composition
  root — it wires the already-accepted `PaperTradingEngine` (Phase 5),
  `RuntimeCoordinator` (Phase 6), and `PaperStateStore`/`PersistedRuntime`
  (Phase 7) together, and duplicates none of their logic. Lifecycle:
  acquire lock -> log config summary -> `await runtime.recover()` (any
  exception here means the runtime is NEVER started, exit code 4) ->
  register SIGINT/SIGTERM handlers -> start the runtime + a periodic
  health-snapshot loop -> on shutdown request (idempotent,
  `request_shutdown()`), stop the runtime (Phase 6's own idempotent
  `stop()`), close the persistence store, release the lock, exit 0 (or 1
  if a fatal exception was caught in the run loop). No new paper-trading
  transitions are accepted once shutdown begins — this reuses Phase 6's
  own `_stopped` guard unchanged, not a new mechanism.
- `deploy/systemd/crypto-signal-engine.service`: runs as a dedicated
  non-root user, bounded `Restart=on-failure` (`RestartSec=10`,
  `StartLimitBurst=5`/`StartLimitIntervalSec=300`), graceful
  `KillSignal=SIGTERM`/`TimeoutStopSec=30`, and hardening
  (`NoNewPrivileges=yes`, `ProtectSystem=strict`, `ReadWritePaths=` scoped
  to the durable-state directory only).
- `scripts/backup_sqlite.py`: uses SQLite's own Online Backup API
  (`sqlite3.Connection.backup()`) rather than copying the live database
  file, so a backup taken while the service is running is guaranteed
  consistent.
- `pyproject.toml`: `websockets` moved to `[project.optional-dependencies]
  .runtime` (Decision 66) — the entire offline test suite still requires
  zero extra dependencies; a real Ubuntu deployment installs
  `pip install ".[runtime]"`. A `crypto-signal-engine` console script was
  added as an equivalent to `python -m crypto_signal_engine.app`.
- Testing: all new Phase 8 tests are offline/deterministic — no real
  OS signals are sent (shutdown is exercised by calling
  `request_shutdown()` directly), no real network/`websockets` is
  required (a `FakeLiveDataProvider` is injected directly into
  `Application`), and no real wall-clock `sleep(N)` guesses are used in
  the lifecycle tests (a small bounded polling helper is used instead,
  consistent with the project's existing "no wall-clock sleeps" testing
  discipline). The systemd unit and the Phase 8 file set are also checked
  for hardcoded WSL/Windows/developer-machine paths.
- Full detail: `PHASE8_UBUNTU_OPERATIONS.md`, `DECISIONS.md` (Decisions 63-66).

## 21. Phase 9 — Long-Run Stability / Soak Testing (summary)

- `crypto_signal_engine/stability/harness.py`: `SoakHarness` composes real
  Phase 5/6/7 objects (`PaperTradingEngine`, `RuntimeCoordinator`,
  `PaperStateStore`, `PersistedRuntime`) and drives them via their own
  public methods (`ingest_candle`, `ingest_order_book`, `recover`, `stop`,
  `resolve_gap`) — no runtime logic is duplicated. Time is accelerated via
  Phase 2's existing `FixedClock` (unmodified), advanced automatically as
  each deterministic candle is generated (never a real wall-clock sleep).
- `crypto_signal_engine/stability/faults.py`: `ScriptedMarketDataProvider`
  (a queue-backed async provider double, used only for the two scenarios
  that genuinely need the real `run()`/`stop()`/disconnect mechanics —
  reconnect storm and shutdown-under-activity) and
  `FaultyPaperStateStore` (a thin wrapper around a real `PaperStateStore`
  that can fail its next N checkpoint calls deterministically — the same
  idea as Phase 7's own one-off `_FlakyStore` test double, generalized).
- `crypto_signal_engine/stability/scenarios.py`: 10 reusable scenario
  driver functions (steady state, reconnect storm, gap recovery,
  duplicate/out-of-order, process restart, crash-like restart,
  persistence failure, stale feed, multi-symbol stress, shutdown under
  activity) — drivers only, invariant assertions live in the tests.
- **BLOCKER found and fixed (Decision 67):** while building the reconnect
  scenario against `PersistedRuntime.run()` (the actual production
  entrypoint via Phase 8's `Application`), a stream disconnect was
  discovered to NOT mark the symbol degraded at all — Phase 7's own
  `_consume_candles`/`_consume_order_book` never had the
  `except Exception: mark_disconnected(symbol); raise` wrapper that Phase
  6's `RuntimeCoordinator._consume_candles`/`_consume_order_book` already
  has. A regression test was added and confirmed to fail against the
  unfixed code first; the fix is a verbatim copy of Phase 6's own pattern
  (no new behavior invented).
- **Finding, not a blocker (documented):** a Phase 6/7-level stream
  disconnect does not self-heal within a single process — Phase 6's own
  `run()` "never resubscribes on its own" (existing, unmodified
  docstring); recovery happens either inside Phase 2's own reconnect
  policy (invisible to the coordinator when it succeeds) or via a full
  process restart (Phase 8's systemd `Restart=on-failure`). The
  "Reconnect Storm" scenario was designed around this real contract
  (isolation + no-crash, not "the same stream recovers repeatedly").
- **BLOCKER found and fixed (Decision 68, independent Phase 9 acceptance
  review):** because Phase 7 only checkpoints live-ingested candles
  (Decision 62), the original `recover()` fetched only a narrow
  `[checkpoint, now)` window once a checkpoint existed — which, since a
  checkpoint always points at the *most recent* candle, normally returns
  almost nothing even when Binance's PUBLIC REST holds ample historical
  depth. This left restarts stuck in BOOTSTRAPPING for hours of real time
  on the higher timeframes even though the authoritative data to rebuild
  warmup immediately was available. Fixed by fetching
  `[checkpoint - lookback, now)` (checkpoint present) using the *same*
  `warmup_candles * 2` lookback formula Phase 6's own `bootstrap()`
  already uses for the no-checkpoint case — no new constant invented,
  checkpoint semantics and no-look-ahead unchanged, `bootstrap_candles()`
  still never triggers signal evaluation. When sufficient PUBLIC history
  exists, restart now reaches READY immediately (once order book is
  reacquired); when it is genuinely insufficient, the symbol honestly
  stays BOOTSTRAPPING (no fabricated READY) — both proven by regression
  tests that were confirmed to fail against the unfixed code first.
- Resource-growth categorization: `CandleWindow`/`FeatureHistoryStore`
  (bounded, verified to never exceed `maxlen`), `RuntimeCoordinator._tasks`
  (constant per `run()`, not per-event) vs. `processed_context_ids`/fills/
  orders (expected durable, append-only ledger growth — proportional to
  event count, not redesigned to "optimize" memory, per instruction).
- Determinism: an identical accelerated workload (multi-symbol stress +
  gap recovery + restart) run twice from fresh state produces byte-for-
  byte identical position/fill/order/context-id/checkpoint/health results.
- `scripts/public_soak_test.py`: optional, manual, real-PUBLIC-data soak
  script (credential-free, explicit duration, graceful SIGINT/SIGTERM,
  periodic health printout, JSON report); verified with a real 15-second
  run against live Binance during this delivery (not a multi-hour soak).
- Full detail: `PHASE9_LONG_RUN_STABILITY.md`, `DECISIONS.md` (Decisions 67-68).

## 22. Phase 10 — Binance Spot TESTNET Execution Lab (summary)

- `crypto_signal_engine/execution/`: the system's first private/signed
  Binance boundary, strictly against `https://testnet.binance.vision`.
  Mainnet is structurally unreachable — `validate_testnet_host()` checks
  `urlsplit().hostname` for an exact match (never substring), rejecting
  Mainnet, lookalike subdomains, userinfo tricks, and non-HTTPS hosts
  (Decision 69). `ALLOW_LIVE_TRADING` is untouched by this package and
  stays `False`.
- `models.py`: `ExecutionMode` has exactly two valid members (`PAPER`,
  `BINANCE_SPOT_TESTNET`) — `MAINNET` is not a member and
  `ExecutionMode("MAINNET")` always raises. `OrderIntent.client_order_id`
  is not a constructor argument; it's derived deterministically (SHA-256
  over the intent's economically-meaningful fields) so the same intent
  always yields the same id and a different intent always yields a
  different one (Decision 70) — this establishes the identity Phase 11's
  reconciliation layer will need, without claiming exactly-once yet.
- `signer.py`: HMAC-SHA256 matching Binance's own signing contract
  (insertion-order query encoding, not alphabetical); timestamps come
  from an injected `Clock` (Phase 2's, unmodified) — no scattered
  `datetime.now()` in execution code.
- `testnet_client.py`: `BinanceTestnetConfig` makes credentials
  intentionally optional (`None`) — public-equivalent calls
  (`exchangeInfo`/`ping`/`server_time`) need none, matching real
  Binance behavior; only `account_info()`/`place_order()` require them
  and raise `MissingCredentialsError` cleanly if absent (Decision 71).
  `__repr__` is overridden to redact credentials (`SET`/`UNSET` only).
- `adapter.py`: `TestnetExecutionAdapter.submit()` always fetches
  `exchangeInfo` and validates LOT_SIZE/PRICE_FILTER/MIN_NOTIONAL before
  ever calling `place_order()` — a filter violation is rejected before
  transport, never silently rounded into different economics.
- `scripts/binance_testnet_lab.py`: manual CLI (`account-check`,
  `validate-symbol`, `place-market`, `place-limit`). Always prints the
  target host before any call; order submission requires the explicit
  `--confirm-testnet-order` flag — without it, only validation/dry-run
  runs (no transport call at all). No `--base-url`/host-override flag
  exists. `run_cli_async()` accepts an injectable HTTP client so the
  whole CLI is tested offline with a fake transport.
- Order types deliberately narrow: Spot MARKET (quantity or
  quoteOrderQty) and Spot LIMIT GTC only — no cancel/replace, no OCO, no
  full OMS.
- Safety scan (Decision 72): `tests/test_repository_safety_scan.py`'s
  blanket private-endpoint scan now excludes `execution/` (which
  intentionally contains HMAC/credential/private-endpoint traces — that
  is the point of Phase 10), and a new
  `TestPhase10ExecutionBoundarySafety` class instead verifies those
  traces (a) never leak outside `execution/`, and (b) never include
  futures/margin/withdrawal/transfer endpoints or a Mainnet host literal
  even inside `execution/`. The old scan was split along the correct
  architectural boundary, not weakened.
- Testing: 126 focused offline tests (`tests/test_execution_models.py`,
  `test_execution_signer.py`, `test_execution_testnet_client.py`,
  `test_execution_adapter.py`, `test_execution_cli.py`) — fake
  credentials, fake HTTP transport (`tests/execution_fakes.py`),
  deterministic clock, zero real network. No accepted Phase 1-9 module
  imports `crypto_signal_engine.execution` (verified both by grep and by
  a dedicated safety-scan test) — there is no automatic Signal ->
  Testnet-order path.
- **BLOCKER found and fixed (Decision 73, independent acceptance
  review):** `OrderIntent.price` is structurally always `None` for
  MARKET orders, but the original `validate_intent_against_filters()`
  only ever computed notional as `quantity * intent.price` — meaning
  MIN_NOTIONAL/NOTIONAL was silently never enforced for any MARKET
  order (the one existing notional test used LIMIT only, so this never
  surfaced). Fixed by having `TestnetExecutionAdapter.validate_intent()`
  fetch a current TESTNET public price via a new
  `BinanceTestnetClient.symbol_price()` (`/api/v3/ticker/price`,
  unsigned) whenever a MARKET order uses base `quantity` and an
  applicable (`applyToMarket`/`applyMinToMarket`/`applyMaxToMarket`)
  notional filter exists; `quoteOrderQty` MARKET orders use the quote
  amount directly (no price fetch needed); `MARKET_LOT_SIZE` is now
  parsed and used for MARKET quantity bounds when present, in place of
  `LOT_SIZE`. Any price-lookup failure (transport, timeout, malformed
  JSON, wrong symbol, non-finite/non-positive value) propagates before
  `place_order()` is ever reached — fail-closed, never "skip and
  submit." LIMIT behavior is unchanged (still uses `intent.price`
  directly, no price lookup). The CLI's duplicate validation call was
  removed in favor of calling `adapter.validate_intent()` directly, so
  dry-run output now reflects the same logic path as real submission.
- Full detail: `PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md`, `DECISIONS.md`
  (Decisions 69-73), `SAFETY_INVARIANTS.md` (items 15-18).

## 23. Phase 11 — Execution Safety & Reconciliation (summary)

- `crypto_signal_engine/execution/reconciliation_models.py`:
  `ExecutionLifecycleState` (INTENT_CREATED, SUBMISSION_ATTEMPTED,
  ACKNOWLEDGED, PARTIALLY_FILLED, FILLED, CANCELED, EXPIRED, REJECTED,
  AMBIGUOUS, UNKNOWN_NOT_FOUND) and `ExecutionRecord`. `apply_exchange_truth()`
  enforces monotonic progress and rejects impossible regressions
  (FILLED -> anything else, executedQty decreasing, exchangeOrderId
  changing) with `ReconciliationContradictionError` — never silently
  overwritten.
- `crypto_signal_engine/execution/reconciliation_store.py`:
  `ExecutionStateStore` — SQLite, same discipline as Phase 7's
  `PaperStateStore` (explicit `SCHEMA_VERSION`, no silent migration);
  reuses Phase 7's `SchemaVersionMismatchError`/`CorruptRecordError`
  rather than redefining them. No API key/secret/signature column exists
  in the schema.
- `BinanceTestnetClient.query_order()`: signed `GET /api/v3/order`
  (`origClientOrderId`), same hard Testnet host allowlist as every other
  Phase 10 call; raises `OrderNotFoundError` (not a real error — part of
  the reconciliation flow) on Binance code -2013.
- `crypto_signal_engine/execution/reconciliation_service.py`:
  `ExecutionReconciliationService` is the single high-level entry point.
  `submit()`: on `POST /api/v3/order` timeout/connection loss, the
  record moves to `AMBIGUOUS` and is *immediately* reconciled via a
  query on the same stable `client_order_id` — never a blind resubmit
  with a new identity (Decision 74). `context_id` is the store's primary
  key, mirroring Phase 5's `Signal.context_id` idempotency principle
  (Decision 75): same context_id + terminal record → returned as-is, no
  duplicate POST; same context_id + non-terminal record → reconciled,
  never resubmitted; same context_id with *different* economics (a
  different resulting `client_order_id`) → explicit
  `ExecutionIdempotencyConflictError`. The pre-submission record write
  happens *before* the POST call and is fail-closed (Decision 76): if
  that write fails, the order is never submitted; if the *post*-submission
  write fails, the error message carries the `client_order_id`/
  `exchange_order_id` explicitly so an operator can `reconcile` once
  persistence is restored — no distributed-transaction fiction is
  attempted. `reconcile_pending()` sweeps every non-terminal record
  (restart-recovery entry point) and isolates per-record failures so one
  symbol's transient query error never blocks another's reconciliation.
- **BLOCKER found and fixed during Phase 11's own test development
  (Decision 77):** `reconcile()`'s "no local record found" path raised a
  bare `ValueError`, which the CLI's `except ExecutionError` handler
  does not catch — an unknown `--context-id` would have crashed the CLI
  with a raw traceback instead of a clean error message. Fixed by adding
  `LocalExecutionRecordNotFoundError(ExecutionError)`; covered by both a
  service-level and a CLI-level regression test.
- **BLOCKER found by independent acceptance review, fixed (Decision 78):**
  the original `submit()` treated `UNKNOWN_NOT_FOUND` (reached when
  `query_order()` gets a Binance -2013 immediately after an ambiguous
  POST) as "safe to automatically resubmit," and would issue a *fresh*
  `place_order()` POST for the same context on the next `submit()` call.
  This was unsafe: a single -2013 does not prove the original POST was
  never accepted by Binance (delayed visibility, eventual consistency, or
  a transient query glitch can all produce this). Fixed by adding
  `UNKNOWN_NOT_FOUND` to `NEEDS_RECONCILIATION_STATES` and removing the
  "safe resubmission" branch entirely — any non-terminal existing record
  (including `UNKNOWN_NOT_FOUND`) now *only* triggers `_reconcile_record()`
  (a fresh query, never a fresh POST). `reconcile_pending()` continues to
  sweep `UNKNOWN_NOT_FOUND` records on restart, querying but never
  resubmitting. If the exchange later confirms the order exists, it
  reconciles to actual exchange truth under the same `client_order_id` —
  total POST count stays one. Covered by regression tests in
  `TestAmbiguousSubmission`, `TestRestartRecovery`, `TestIsolation`
  (`test_execution_reconciliation_service.py`) and
  `TestReconcileCommand` (`test_execution_cli.py`); the old
  `test_unknown_not_found_allows_safe_resubmission` (which asserted the
  unsafe behavior) was removed.
- `scripts/binance_testnet_lab.py`: new `order-status` (local-only read,
  no network call), `reconcile`, and `reconcile-pending` commands.
  `place-market`/`place-limit --confirm-testnet-order` now go through
  `ExecutionReconciliationService.submit()` instead of calling
  `client.place_order()` directly, so CLI-triggered submissions get the
  same idempotency/ambiguity/persistence guarantees. New env var
  `BINANCE_TESTNET_EXECUTION_DB_PATH` (safe relative default, Phase
  8-style override) — still no `--base-url`/host flag of any kind.
- Testing: all new tests offline (`tests/execution_fakes.py`, fixed
  clock, temp SQLite). Found and fixed a genuine *test* bug along the
  way — `FakeTestnetHttpClient`'s "reuse the last queued item" convenience
  (inherited from Phase 2's `tests/binance_fakes.py`) interacts badly
  with appending new responses to the queue *after* the first network
  call has already consumed it; fixed by front-loading all expected
  responses in call order instead of appending mid-test.
- Full detail: `PHASE11_EXECUTION_SAFETY_RECONCILIATION.md`,
  `DECISIONS.md` (Decisions 74-78).

## 24. Phase 12 — Final Local Production Readiness (summary)

Phase 12 makes the accepted Phase 1-11 system ready for several weeks of
continuous LOCAL (Ubuntu/WSL) operation — it is explicitly NOT Mainnet
production. Planned sequence: Stage A (local 24/7 PUBLIC + PAPER, weeks
of observation) -> Stage B (local Binance Spot TESTNET, opt-in) -> Stage
C (VPS, only after local stability evidence — out of this phase's
scope). There is no planned Phase 13.

- `crypto_signal_engine/ops/config.py`: new `AppConfig` fields —
  `execution_mode` (`ExecutionMode`, default `PAPER`),
  `enable_testnet_execution` (default `False`), `dashboard_enabled`
  (default `True`), `dashboard_host` (default `127.0.0.1`),
  `dashboard_port` (default `8787`). No credential field — that hard
  invariant is unchanged from Phase 8.
- `crypto_signal_engine/execution/factory.py` (new): the *only* place
  outside `scripts/binance_testnet_lab.py` that constructs a real
  `BinanceTestnetConfig`/`BinanceTestnetClient` and reads
  `BINANCE_TESTNET_API_KEY`/`BINANCE_TESTNET_API_SECRET`. This exists
  because the initial draft put that construction directly in `app.py`,
  which `tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`
  immediately caught (that scan forbids `place_order(`/`api_secret`/
  `hmac.new`/`X-MBX-APIKEY`/`/api/v3/order` — even in a comment —
  anywhere under `crypto_signal_engine/` outside `execution/`). Fixed by
  moving the construction into `execution/factory.py` and having `app.py`
  call it only after deciding (from config alone) that TESTNET is
  opted-in; see DECISIONS.md Decision 79.
- `crypto_signal_engine/app.py::Application`: extended, not redesigned.
  If TESTNET is opted-in (`CSE_EXECUTION_MODE=BINANCE_SPOT_TESTNET` AND
  `CSE_ENABLE_TESTNET_EXECUTION=true`), `start()` runs
  `ExecutionReconciliationService.reconcile_pending()` *before* creating
  the run/health-loop tasks — fail-closed (`execution_ready=False`) on
  any error, without ever stopping the PUBLIC/PAPER runtime. New
  `doctor` subcommand: a fully offline preflight (config, state
  directory, SQLite schema, singleton lock, execution-mode gating,
  credential *presence* only, Testnet host match, Mainnet structural
  impossibility, dashboard bind safety) — never prints a credential
  value, never touches the network, never places an order. New
  `DashboardServer` wiring: started after startup reconciliation, a bind
  failure (e.g. port already in use) is caught and only disables the
  dashboard — it never fails `start()`.
- `crypto_signal_engine/ops/dashboard.py` (new): stdlib-only
  (`http.server.ThreadingHTTPServer`), read-only — every method other
  than `GET` returns 405 (no mutation endpoint exists at all). Its
  *only* data source is the atomic JSON health-snapshot file
  (`ops/health_snapshot.py::write_snapshot()`/`read_snapshot()`); the
  dashboard's request-handler threads never touch live Python objects
  (paper engine, coordinator, execution store) directly, which
  structurally rules out any cross-thread race and means a dashboard
  request failure can never affect the runtime. Default bind:
  `127.0.0.1`. The documented phone-access pattern is Tailscale Serve
  proxying that loopback port into a private tailnet — `tailscale` is
  never added as a Python dependency, and there is no router
  port-forwarding instruction or 0.0.0.0 requirement anywhere.
- `crypto_signal_engine/ops/health_snapshot.py::write_snapshot()`: two
  new optional keyword arguments, `execution` and `paper` — additive,
  backward compatible (existing callers/tests unaffected). `execution`
  carries only mode/enabled/ready/pending-count/last-reconciliation/
  detail — never a credential. `paper` carries per-symbol position and
  last-signal summaries.
- `crypto_signal_engine/runtime/coordinator.py`: two small additive,
  read-only accessors for the dashboard — `paper_engine` (property) and
  `last_cycle_result(symbol)` (a bounded, one-entry-per-symbol cache of
  the most recently completed `RuntimeCycleResult`, overwritten not
  appended). No existing behavior changed.
- `scripts/local_readiness_check.py` (new): one reproducible, fully
  offline command — runs `doctor`, `compileall`, checks whether
  `websockets` is installed (informational only), and checks free disk
  space at the state directory. Never places an order, never requires
  real network access, never prints a credential value. Optional real
  smoke tests (`scripts/live_public_smoke_test.py`,
  `scripts/binance_testnet_lab.py account-check`) are named but not run
  automatically.
- **Deliberate, reported-as-a-blocker scope decision (Decision 80):**
  Phase 12 does *not* add automatic, continuous Signal->TESTNET
  execution (i.e., `RuntimeCoordinator` never calls
  `ExecutionReconciliationService.submit()` on every evaluated signal).
  Bridging Phase 6's synchronous signal-evaluation path to Phase 11's
  asynchronous execution service safely (without blocking the
  candle-consumption loop, with correct idempotency-key mapping, with a
  well-defined restart-recovery story for "was this signal already
  submitted") is itself an architectural design decision — attempting
  it under a "fix blockers only, no redesign" constraint would mean
  either silently redesigning Phase 6 or bolting on an unsafe
  fire-and-forget shortcut. The task instructions explicitly permitted
  this: "If implementing this integration would require unsafe
  architectural shortcuts, do NOT force it. Report it as a blocker
  instead." TESTNET execution stays reachable only through the existing,
  already-tested manual CLI (`scripts/binance_testnet_lab.py`); Phase 12
  only adds the opt-in gate, startup reconciliation, and visibility
  (doctor/dashboard) around that unchanged boundary.
- Full detail: `PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md`,
  `DECISIONS.md` (Decisions 79-80), `tests/
  test_phase12_local_production_readiness.py`, `tests/test_ops_dashboard.py`,
  `tests/test_local_readiness_check.py`.


=== FILE: PROJECT_SUMMARY.md ===
# PROJECT_SUMMARY.md

## Durum: FAZ 1 (FINAL HARDENED) + FAZ 2 (BINANCE MARKET DATA MVP, v3) + FAZ 3 (FEATURE ENGINE, v2) + FAZ 4 (QUANT & AGENT ENGINE) + FAZ 5 (PAPER TRADING / SIMULATION ENGINE, ACCEPTED) + FAZ 6 (REAL-TIME MARKET DATA & RUNTIME, ACCEPTED) + FAZ 7 (PERSISTENCE & RECOVERY, ACCEPTANCE-READY)

Binance Crypto Intelligence / Signal Engine, şu an Faz 1 domain contract
layer'ı ÜZERİNE inşa edilmiş, çalışan bir Binance public market data
pipeline'ı (Faz 2), deterministic feature engine (Faz 3), dört bağımsız
agent + consensus + risk overlay'den oluşan salt analitik bir quant/agent
katmanı (Faz 4), Faz 4'ün `Signal` nesnesini tüketen dahili/deterministik
bir paper trading simülasyon katmanı (Faz 5), bu ikisini gerçek-zamanlı
PUBLIC Binance market data ile sürekli çalıştıran bir runtime katmanı
(Faz 6), ve bu sistemi process-restart sonrası durable/deterministik
şekilde geri yükleyen bir persistence/recovery katmanı (Faz 7) içerir.

Hiçbir private/signed Binance endpoint'i, API key, Testnet/Mainnet
execution, veya gerçek order gönderme yeteneği yoktur. `ALLOW_LIVE_TRADING
= False` korunmaktadır ve otomatik repository safety scan ile doğrulanmaktadır.

Paket gerçek bir Python package'ıdır (`crypto_signal_engine`), **904 unit
test** ile korunmaktadır (Faz 1'in 290'ı + Faz 2'nin 204'ü + Faz 3'ün
133'ü + Faz 4'ün 118'i (Faz 1-4 toplamı: 744 passed, 1 skipped, 0 failed)
+ Faz 5'in 55'i + Faz 6'nın 54'ü + Faz 7'nin 34'ü). Faz 1-6 testlerinin
HİÇBİRİ zayıflatılmadı veya silinmedi.

## Faz 1 özeti

Domain modelleri, enum'lar, provider/quality/safety ABC'leri, candle
identity/sequencing contract'ı, consensus/regime contract'ları. `SafetyState`
tam encapsulate edilmiştir; tüm enum-typed alanlar runtime'da isinstance ile
doğrulanır; `Signal` tam provenance zinciri taşır. Detaylar: ARCHITECTURE.md,
DECISIONS.md (Karar 1-22), SAFETY_INVARIANTS.md.

## Faz 2 özeti

`providers/binance/` (REST/WS client'ları, parser, order book sync —
overlap semantiği, reconnect policy, stale-feed detection),
`quality/binance_rules.py` (concrete `DataQualityGate`, structural/
freshness ayrımı), `state/manager.py` (candle/trade/order-book
orkestrasyon), `persistence/sqlite_store.py` (SQLite-backed
`CandleStateStore`, atomic commit). Bağımsız reviewer'ın üç turluk
incelemesi sonucu tüm açıklar kapatıldı. Detaylar: **PHASE2_MARKET_DATA.md**,
DECISIONS.md (Karar 23-37).

## Faz 3 özeti

`features/` — candle/order-book/trade feature hesaplayıcıları
(SMA/EMA/RSI/ATR/Bollinger/VWAP/volume/price-structure/microstructure/
trade-flow), instance-scoped `FeatureRegistry`, `FeatureEngine` (closed-candle
politikası + yapısal no-look-ahead filtrelemesi), `FeatureHistoryStore`
(bounded, symbol/timeframe izole). Detaylar: **PHASE3_FEATURE_ENGINE.md**,
DECISIONS.md (Karar 38-45).

## Faz 4 özeti

Faz 1'in `AgentEvidence`/`ConsensusResult`/`RiskAssessment`/`Signal`
domain modellerini (DEĞİŞTİRMEDEN) ve Faz 3'ün `FeatureSnapshot`/
`FeatureHistoryStore`'unu tüketen dört bağımsız agent (`QuantAgent` M5,
`MarketStructureAgent` M15, `OrderBookAgent` M1, `RegimeAgent` H1) +
`ConsensusEngine` (sabit ağırlıklar, mevcut `compute_agreement()`) +
`RiskOverlay` (strictly post-consensus, tek yönlü) + `SignalEngine`
orkestratörü. Deterministic SHA-256 context-ID, yapısal no-look-ahead
garantisi (`FeatureHistoryStore.as_of()` üzerinden), eksik-feature'da
açık `AgentInputError`. Bir sözleşme sapması (ConsensusEngine'in
`ConsensusResult`'ın zorunlu `regime` alanını doldurmak için `regime`'i
keyword-only parametre olarak alması) dürüstçe raporlandı ve test edildi.
Detaylar: **PHASE4_QUANT_AGENT_ENGINE.md**, DECISIONS.md (Karar 46-50).

## Faz 5 özeti

Faz 4'ün tamamlanmış `Signal` nesnesini (DEĞİŞTİRMEDEN) tüketen, tamamen
dahili/deterministik bir paper trading (kağıt üzerinde işlem) muhasebe
katmanı: `PaperTradingEngine` (`paper_trading/engine.py`), sabit
notional-based position sizing, **configurable deterministic fee/slippage**
(`fee_bps`/`slippage_bps`, varsayılan `0.0` — sıfır maliyetli davranışı
korur), per-symbol izole state, deterministic order/fill kimlikleri
(uuid/random/wall-clock KULLANILMAZ), idempotency, atomic state
transitions, no-look-ahead (fiyat/sinyal zaman sıralaması zorunlu kılınır).
`ConsensusResult`/`RegimeContext`/`agents/*`/`consensus/*`'e HİÇBİR
bağımlılığı yoktur — Faz 4'ün `regime` keyword-only sözleşme sapması
(Karar 46/47) bu fazda DEĞİŞTİRİLMEDİ, ne düzeltildi ne derinleştirildi.
İki ayrı acceptance review turunda bulunan iki BLOCKER düzeltildi: (1)
NEUTRAL yanlışlıkla pozisyonu kapatıyordu — **NEUTRAL artık kesinlikle
NO_ACTION'dır** (Karar 53); (2) spesifikasyonun gerektirdiği fee/slippage
modeli HİÇ implemente edilmemişti — artık İMPLEMENTE EDİLMİŞTİR (Karar 54).
Faz 5 resmi olarak ACCEPTED edildi ve Git'e commit/tag edildi (`phase5-accepted`).
Detaylar: **PHASE5_PAPER_TRADING.md**, DECISIONS.md (Karar 51-54).

## Faz 6 özeti

Faz 2 (public market data)/3 (`FeatureEngine`)/4 (`SignalEngine`)/5
(`PaperTradingEngine`) bileşenlerini KOORDİNE eden, kullanıcının kendi
Ubuntu makinesinde sürekli/gerçek-zamanlı çalışabilecek bir runtime
katmanı: `RuntimeCoordinator` (`runtime/coordinator.py`), kendi
`CandleWindow`'u (Faz 2'nin private state'ine tamamlayıcı, ona ayna
DEĞİL — identity-bazlı dedup/sıralama/gap tespiti), staleness-bazlı
`HealthMonitor` (Faz 2'nin `Clock` protokolünü yeniden kullanır),
deterministic bootstrap (Faz 3/4'ün min 20 kapalı candle gereksinimini
karşılar), REST-bazlı gap-recovery (`resolve_gap()`), ve `SignalEngine`'in
TEK çağrıldığı, yalnızca M5 kapanışında tetiklenen bir sinyal-üretim
sınırı. `ConsensusResult`/`RegimeContext`/`agents/*`/`consensus/*`'e
HİÇBİR bağımlılığı yoktur; Faz 5'in `PaperTradingEngine`'ini (fee/slippage/
NEUTRAL=NO_ACTION/idempotency dahil) DEĞİŞTİRMEDEN, TEK bir yerden çağırır.
Faz 6, **execution fazı DEĞİLDİR** — hiçbir gerçek borsa emri gönderilmez;
opsiyonel `real_websocket.py` (lazy-import, `websockets` tabanlı) ve
`scripts/live_public_smoke_test.py` yalnızca PUBLIC market data içindir.
Bu teslimat sırasında gerçek Binance PUBLIC REST/WebSocket'e karşı hem
smoke test hem tam `RuntimeCoordinator.bootstrap()` başarıyla doğrulandı.
Bir serious blocker-focused adversarial acceptance review'da bulunan tek
BLOCKER (bir consumer task hatasının `run()`'ın kendi
`asyncio.gather(return_exceptions=True)`'ı tarafından sessizce
yutulabilmesi) düzeltildi: artık hata önce health'e AÇIKÇA yansıtılır.
Faz 6, ayrıca bağımsız bir blocker review'dan geçti (üç ek düzeltme:
gap-recovery sonrası kalıcı DEGRADED, identity mismatch validasyonu,
READY'nin order-book evidence'ı da gerektirmesi) ve resmi olarak ACCEPTED
edilip Git'e commit/tag edildi (`phase6-accepted`).
Detaylar: **PHASE6_REALTIME_RUNTIME.md**, DECISIONS.md (Karar 55-59).

## Faz 7 özeti

Faz 5 (`PaperTradingEngine`) + Faz 6 (`RuntimeCoordinator`) sistemine
durable local persistence (SQLite) ve deterministic process-restart
recovery ekler — `PaperTradingEngine`/`RuntimeCoordinator`'ı DEĞİŞTİRMEDEN
SARAR (`persistence/recovery.py::PersistedRuntime`). Durable olarak
YALNIZCA: sembol başına pozisyon/entry_fee/last_signal_timestamp, işlenmiş
her context için (idempotency/conflict-detection için TAM `Signal`),
fill/order ledger'ı, ve sembol+timeframe başına TEK bir candle checkpoint
(`open_time`) persist edilir. Candle/feature GEÇMİŞİ blindly serialize
EDİLMEZ — PUBLIC REST'ten (Faz 2, DEĞİŞTİRİLMEDEN) deterministik olarak
YENİDEN İNŞA EDİLİR. Explicit `SCHEMA_VERSION` (sessiz migration YOK),
bozuk kayıtlar `CorruptRecordError` ile AÇIKÇA reddedilir (sessizce
atlanmaz). Restart idempotency, açık pozisyon/fee/PnL recovery, ve
multi-symbol izolasyonu blocker-seviyesinde test edilmiştir. `PaperTradingEngine`'in
private state'ine YAZAN TEK, dokümante edilmiş istisna
(`restore_paper_engine()`) DIŞINDA Faz 5 hiçbir şekilde değiştirilmedi.
Faz 7, **execution fazı DEĞİLDİR** — Testnet/private API/Mainnet YOK. Bir
serious blocker-focused adversarial acceptance review'da bulunan tek
BLOCKER (bir checkpoint türünün başarısının, İLGİSİZ ve HÂLÂ çözülmemiş
başka bir checkpoint türünün hatasını maskeleyebilmesi) düzeltildi:
persistence fault artık reason-bazlı izlenir. Detaylar:
**PHASE7_PERSISTENCE_RECOVERY.md**, DECISIONS.md (Karar 60-62).

Faz 8 (Operations & Ubuntu Deployment), kabul edilmiş Faz 1-7 sistemini
DEĞİŞTİRMEDEN, `python -m crypto_signal_engine.app {run|status}` CLI'ı +
`ops/` paketi (config/lock/health-snapshot/logging) + bir systemd unit
şablonu + SQLite backup script'i ile bir Ubuntu servisi hâline getirir.
Hâlâ **execution fazı DEĞİLDİR** — hiçbir yeni config alanı Testnet/
Mainnet/private API'ye işaret EDEMEZ (Karar 65). Detaylar:
**PHASE8_UBUNTU_OPERATIONS.md**, DECISIONS.md (Karar 63-66).

Faz 9 (Long-Run Stability / Soak Testing), `FixedClock` (Faz 2,
DEĞİŞTİRİLMEDEN) ile hızlandırılmış sanal zamanda 10 çekirdek soak
senaryosunu (steady state, reconnect storm, gap recovery, duplicate/
out-of-order, process restart, crash-like restart, persistence failure,
stale feed, multi-symbol stress, shutdown under activity) tamamen
offline/deterministik çalıştırır. Harness geliştirmesi sırasında bir
BLOCKER bulundu ve düzeltildi (Karar 67): `PersistedRuntime`'ın (Faz 7,
Faz 8'in üretim giriş noktası) stream-tüketim döngüleri Faz 6'nın kendi
`mark_disconnected` disiplinine sahip DEĞİLDİ. Bağımsız acceptance
review'da İKİNCİ bir BLOCKER bulundu ve düzeltildi (Karar 68): restart
recovery, checkpoint VARKEN yalnızca dar `[checkpoint, now)` penceresini
çekiyordu — yeterli PUBLIC geçmiş MEVCUT olsa bile M15/H1 warmup'ını
yeniden inşa edemiyordu. Artık checkpoint VARKEN de checkpoint'in
ETRAFINDA tam warmup-lookback penceresi (`[checkpoint-lookback, now)`)
çekilir; yeterli PUBLIC geçmiş varken restart artık GERÇEK zamanda yeni
candle beklemeden DOĞRUDAN READY'e döner, GERÇEKTEN yetersizse dürüstçe
BOOTSTRAPPING kalır. Detaylar: **PHASE9_LONG_RUN_STABILITY.md**,
DECISIONS.md (Karar 67-68).

Faz 10 (Binance Spot TESTNET Execution Lab), sistemin İLK private/signed
Binance execution sınırını ekler — KESİNLİKLE ve YALNIZCA
`https://testnet.binance.vision`'a karşı; Mainnet YAPISAL OLARAK
İMKANSIZDIR (host allowlist, tam `urlsplit().hostname` eşleşmesi).
`ALLOW_LIVE_TRADING` `False` kalır. Yalnızca Spot MARKET/LIMIT GTC,
deterministik (SHA-256 türetilmiş) `client_order_id`, exchange-filter
doğrulaması, ve `--confirm-testnet-order` OLMADAN yalnızca dry-run yapan
manuel bir CLI (`scripts/binance_testnet_lab.py`). Hiçbir Faz 1-9
bileşeni bu paketten haberdar değildir — otomatik Signal->TESTNET-order
yolu YOKTUR. Bağımsız acceptance review'da bulunan tek BLOCKER (MARKET
order'lar için MIN_NOTIONAL/NOTIONAL kontrolünün, `OrderIntent.price`
MARKET'te her zaman `None` olduğundan, sessizce hiç çalışmaması)
düzeltildi: artık gerektiğinde GÜNCEL bir TESTNET PUBLIC fiyatı
(`/api/v3/ticker/price`) çekilip notional tabanı olarak kullanılır,
fiyat lookup'ı başarısız olursa order fail-closed reddedilir. Detaylar:
**PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md**, DECISIONS.md (Karar 69-73).

Faz 11 (Execution Safety & Reconciliation), Faz 10'un TESTNET execution
sınırına stabil kimlik üzerinden reconciliation ekler — hâlâ TESTNET ONLY,
Mainnet YAPISAL OLARAK İMKANSIZ. Bir `POST /api/v3/order` timeout/bağlantı
kopmasıyla başarısız olursa, sistem KÖRÜ KÖRÜNE yeni bir kimlikle yeniden
GÖNDERMEZ — AYNI `client_order_id` ile exchange'e SORAR (`AMBIGUOUS` ->
reconcile -> exchange truth veya `UNKNOWN_NOT_FOUND`). `UNKNOWN_NOT_FOUND`
("bu sorguda bulunamadı," -2013) BİLE ASLA otomatik bir ikinci POST'a YOL
AÇMAZ — sadece reconciliation-eligible bir çözülmemiş durumda KALIR,
yeniden sorgulanmaya devam eder (BLOCKER FİX, Karar 78; bkz.
DECISIONS.md). `context_id` + durable SQLite (`ExecutionStateStore`) ile
idempotent replay/restart recovery/partial-fill monotonicity/contradiction
koruması (FILLED->NEW, executedQty azalması, exchangeOrderId değişimi
HEPSİ reddedilir) sağlanır. "No fake exactly-once" — yalnızca test edilmiş
koşullar altında "effectively-once" iddia edilir. Detaylar:
**PHASE11_EXECUTION_SAFETY_RECONCILIATION.md**, DECISIONS.md (Karar 74-78).

## Resmi doğrulama prosedürü (reproducible, offline)

Repo kökünden tek komut:

```bash
python3 scripts/verify_phase1.py
```

Bu script, teslim edilen önceden-inşa edilmiş wheel'i (`dist/`) izole bir
venv'e offline (`--no-index --no-deps`) kurar, external-directory import
smoke test yapar, tam pytest suite'ini (Faz 1-4), `compileall`'ı, ve
repository safety scan'i sırayla çalıştırır. Hiçbir adımda ağ erişimi veya
build backend çağrısı gerekmez.

Alternatif olarak, paket ÖNCEDEN kurulmamış olsa bile doğrudan:

```bash
python3 -m pytest
```

çalıştırılabilir — `tests/test_package_import.py` kendi izole venv'ini
kurup testi self-contained hale getirir.

## Bir sonraki adım

Faz 7, Faz 8, Faz 9 VE Faz 10 ACCEPTED durumdadır (`phase7-accepted`,
`phase8-accepted`, `phase9-accepted`, `phase10-accepted` tag'leri). Faz 11
(Execution Safety & Reconciliation) `phase11-accepted` etiketiyle KABUL
EDİLMİŞTİR.

Faz 12 (Final Local Production Readiness) ACCEPTANCE-READY durumdadır
(henüz resmi olarak ACCEPTED ilan edilmedi — commit/tag kabul sonrasına
bırakıldı). Bu faz, Faz 1-11'i birkaç haftalık kesintisiz YEREL (Ubuntu/
WSL) çalıştırma için hazırlar — hâlâ Mainnet üretim DEĞİLDİR. Ekler:
execution-mode gating (`CSE_EXECUTION_MODE`/`CSE_ENABLE_TESTNET_EXECUTION`,
varsayılan PAPER + devre dışı), startup'ta TESTNET reconciliation, tam
offline bir `doctor` preflight komutu, ve stdlib-only, salt-okunur,
loopback-varsayılanlı bir operasyon dashboard'u
(`crypto_signal_engine/ops/dashboard.py`). `ALLOW_LIVE_TRADING=False` ve
Mainnet'in yalnızca public read-only market data için kullanılması
kuralı DEĞİŞMEDEN korunur — Faz 10/11/12'nin hiçbiri Mainnet execution'a
DOKUNMAZ, yalnızca TESTNET (ve o da STRICTLY opt-in). Faz 12, KASITLI
OLARAK otomatik sürekli Signal->TESTNET execution entegrasyonu İÇERMEZ
(bkz. DECISIONS.md Karar 80 — "no redesign" kısıtı altında güvenle
yapılamayacağı için blocker olarak raporlanmış bir erteleme). Bu proje
için planlanmış bir Faz 13 YOKTUR.

Detaylar için bkz. PROJECT_HANDOFF.md, ARCHITECTURE.md, DECISIONS.md,
SAFETY_INVARIANTS.md, PHASE2_MARKET_DATA.md, PHASE3_FEATURE_ENGINE.md,
PHASE4_QUANT_AGENT_ENGINE.md, PHASE5_PAPER_TRADING.md,
PHASE6_REALTIME_RUNTIME.md, PHASE7_PERSISTENCE_RECOVERY.md,
PHASE8_UBUNTU_OPERATIONS.md, PHASE9_LONG_RUN_STABILITY.md,
PHASE10_BINANCE_TESTNET_EXECUTION_LAB.md,
PHASE11_EXECUTION_SAFETY_RECONCILIATION.md,
PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md.

## Pre-Audit Enhancement Pass (Faz 12 ÜZERİNE — Faz 13 DEĞİL)

`phase12-accepted` (commit `33dc6cd`) temeli ÜZERİNE, final bağımsız
adversarial audit ÖNCESİNDE, additive bir araştırma/doğrulama katmanı
eklendi (deterministic historical replay, rolling OOS stability,
performance attribution, Monte Carlo robustness screen, portfolio/
risk-bazlı PAPER sizing + guardrails, order-book research capture) —
yeni, bağımsız bir `research/` paketinde, `crypto_signal_engine/`
mimarisi DEĞİŞTİRİLMEDEN. Bu geçiş uncommitted bırakıldı (bağımsız
acceptance review İÇİN). Detaylar: **PRE_AUDIT_ENHANCEMENTS.md**,
DECISIONS.md Karar 82-89.


=== FILE: SAFETY_INVARIANTS.md ===
# SAFETY_INVARIANTS.md

1. `ALLOW_LIVE_TRADING = False` — `crypto_signal_engine/__init__.py` içinde
   tanımlı hard invariant. Kod içinde hiçbir yerde `True` yapılamaz.
   Doğrulama: `tests/test_repository_safety_scan.py::TestNoLiveTradingBackdoor`.
2. `BinanceLiveExecutionAdapter` sınıfı repoda YOKTUR ve yazılmayacaktır.
   Doğrulama: aynı test dosyası, otomatik grep taraması.
3. Production Binance bağlantısı (Faz 2+) yalnızca read-only market data
   scope kullanabilir; trade yetkisi gerektiren hiçbir kod yolu olamaz.
4. Execution öncesi (Faz 7, Testnet Lab) fail-closed doğrulama zorunlu:
   `environment_verified AND sandbox_enabled AND NOT ALLOW_LIVE_TRADING`.
5. `DataQualityGate` tarafından reddedilen veri, `CandleStateStore.commit()`
   içinde sequence katmanına HİÇ ULAŞMADAN reddedilir; canonical state
   hiçbir şekilde etkilenmez (bkz. ARCHITECTURE.md, DECISIONS.md Karar 6).
   Doğrulama: `tests/test_state_contract.py`.
6. `except Exception: pass` ve eşdeğer silent exception pattern'leri repoda
   yasaktır. Doğrulama: `tests/test_repository_safety_scan.py`
   (hem regex hem AST bazlı tarama).
7. Tüm domain timestamp'leri UTC-aware olmak zorundadır; naive datetime
   sessizce kabul edilmez, `ValueError` fırlatır.
8. Tüm domain numeric alanları finite olmak zorundadır (NaN/±inf reddedilir).
9. `SafetyState.halt()` yalnızca `SafetySeverity.HALT` ile ve yalnızca sistem
   hâlihazırda halted değilken çağrılabilir (açık state machine, illegal
   transition'lar `ValueError` fırlatır).
10. Candle canonical state, finalize edildikten (`is_closed=True` kabul
    edildikten) sonra HİÇBİR update ile geriye alınamaz
    (`REJECTED_AFTER_FINAL`).
11. `SafetyState` tam encapsulate edilmiştir: kritik alanlar
    (`signal_generation_halted`, `halt_reason`) private olup yalnızca
    read-only property üzerinden okunabilir; constructor hiçbir argüman
    kabul etmez (her zaman RUNNING başlar); doğrudan attribute ataması
    `AttributeError` fırlatır. HALTED <=> halt_reason.severity==HALT
    invariantı hiçbir zaman ihlal edilemez (bkz. DECISIONS.md Karar 15).
12. Tüm enum-typed domain alanları `require_enum()` ile runtime'da
    isinstance doğrulamasından geçer; string değerlerin sessizce enum
    yerine kabul edilmesi (`AgentEvidence(agent="QUANT", ...)` gibi)
    `TypeError` ile reddedilir (bkz. DECISIONS.md Karar 16).
13. `Signal`, kendi `context_id`'sine sahiptir ve `supporting_factors`/
    `contradicting_factors` içindeki her `AgentEvidence` için
    `evidence.symbol == signal.symbol`, `evidence.context_id ==
    signal.context_id`, `evidence.as_of <= signal.timestamp` invariant'larını
    zorunlu kılar; aksi halde `ValueError` (bkz. DECISIONS.md Karar 17).
14. `normalize_symbol()` str-olmayan girdilerde deterministik `TypeError`
    fırlatır; incidental `AttributeError` üretmez (bkz. DECISIONS.md Karar 19).
15. `crypto_signal_engine/execution/` (Faz 10) İSTİSNAİ olarak private/
    signed Binance çağrıları YAPAR — ama YALNIZCA `https://
    testnet.binance.vision`'a karşı. `BinanceTestnetConfig.__post_init__`,
    HER inşa edilişinde `validate_testnet_host()`'u çağırır: scheme tam
    olarak `https`, hostname (`urlsplit().hostname`, substring DEĞİL, TAM
    eşleşme) tam olarak `testnet.binance.vision` olmalıdır — aksi halde
    `UnsafeExecutionHostError` (BLOCKER-seviyesi, fail-closed). Mainnet
    (`api.binance.com`), lookalike/subdomain trick'leri, userinfo trick'leri,
    ve `http://` (TLS'siz) HEPSİ bu kontrolle REDDEDİLİR. Doğrulama:
    `tests/test_execution_testnet_client.py::TestTestnetHostAllowlist`,
    `tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`.
16. `ExecutionMode` enum'unun (Faz 10) YALNIZCA İKİ geçerli üyesi vardır:
    `PAPER`, `BINANCE_SPOT_TESTNET`. `MAINNET` bir enum üyesi DEĞİLDİR ve
    OLAMAZ — `ExecutionMode("MAINNET")` her zaman `ValueError` fırlatır.
    `ALLOW_LIVE_TRADING` bu enum'la HİÇBİR ZAMAN bağlantılandırılmaz/
    repurpose edilmez; `False` olarak kalır.
17. Binance TESTNET kimlik bilgileri (`BINANCE_TESTNET_API_KEY`/
    `BINANCE_TESTNET_API_SECRET`) YALNIZCA ortam değişkenlerinden okunur;
    hiçbir zaman loglanmaz, exception mesajına/CLI çıktısına/SQLite'a/
    dokümana YAZILMAZ. `BinanceTestnetConfig.__repr__()` bunları
    AÇIKÇA redakte eder (`SET`/`UNSET`, gerçek değer asla YAZDIRILMAZ).
    Doğrulama: `tests/test_execution_cli.py::TestSecretNeverPrinted`,
    `tests/test_execution_testnet_client.py` (signature/secret redaction).
18. Hiçbir normal runtime bileşeni (`SignalEngine`/`RuntimeCoordinator`/
    `PaperTradingEngine`/Faz 8 `Application`) `crypto_signal_engine.execution`'ı
    İTHAL ETMEZ/otomatik ÇAĞIRMAZ — TESTNET execution'a erişim YALNIZCA
    `scripts/binance_testnet_lab.py` üzerinden, KASITLI/MANUEL bir CLI
    çağrısıyla mümkündür. Order gönderimi AYRICA `--confirm-testnet-order`
    bayrağı OLMADAN gerçekleşmez (bayraksız çağrı yalnızca doğrulama/
    dry-run yapar).
19. (Faz 11) Belirsiz (timeout/bağlantı kopması) bir `POST /api/v3/order`
    denemesi ASLA yeni bir bağımsız kimlikle KÖRÜ KÖRÜNE yeniden
    GÖNDERİLMEZ — `ExecutionReconciliationService`, AYNI deterministik
    `client_order_id` ile exchange'i SORGULAR (`GET /api/v3/order
    origClientOrderId=...`). Sorgu exchange'de order'ı BULURSA, kayıt o
    GERÇEK duruma reconcile edilir. Sorgu order'ı BULAMAZSA (Binance kodu
    -2013, `UNKNOWN_NOT_FOUND`), bu ASLA "yeniden gönderim güvenlidir"
    ANLAMINA GELMEZ (bkz. madde 22) — hiçbir kod yolu bu sonuçtan bir
    ikinci `POST`'a ULAŞMAZ. Doğrulama:
    `tests/test_execution_reconciliation_service.py::TestAmbiguousSubmission`.
20. (Faz 11) `ExecutionStateStore` şemasında API key/secret/signature
    alanı YOKTUR ve OLAMAZ — yalnızca order kimliği/miktarları/durumu/
    zaman damgaları persist edilir. Doğrulama:
    `tests/test_execution_reconciliation_store.py::TestSaveAndLoad::test_no_api_key_secret_or_signature_columns`,
    `tests/test_execution_reconciliation_service.py::TestNoCredentialPersistence`.
21. (Faz 11) Exchange'den dönen gerçek durum, yerel durable record ile
    İMKANSIZ bir şekilde çelişiyorsa (`FILLED -> NEW`, executedQty
    azalması, `exchangeOrderId` değişimi) SESSİZCE ÜZERİNE YAZILMAZ —
    `ReconciliationContradictionError` AÇIKÇA fırlatılır. Doğrulama:
    `tests/test_execution_reconciliation_models.py::TestApplyExchangeTruth`,
    `tests/test_execution_reconciliation_service.py::TestContradictionProtection`.
22. (Faz 11, BLOCKER FİX — Karar 78) `ExecutionLifecycleState.UNKNOWN_NOT_FOUND`
    (bir ambiguous POST sonrası, exchange sorgusunun -2013 döndürdüğü
    durum) `NEEDS_RECONCILIATION_STATES` İÇİNDEDİR ve ASLA terminal/
    "güvenli yeniden gönderim" durumu OLARAK ele ALINMAZ. Tek bir anlık
    -2013 yanıtı, orijinal ambiguous POST'un Binance tarafından HİÇ kabul
    edilmediğini KANITLAMAZ. `submit(intent)`'in AYNI context/
    `client_order_id` için TEKRAR çağrılması, kayıt bu durumdayken SADECE
    exchange'i yeniden SORGULAR — HİÇBİR kod yolu otomatik bir ikinci
    `POST /api/v3/order` ÜRETMEZ. `reconcile_pending()` bu kayıtları
    restart sonrası SÜPÜRMEYE devam eder (yine yalnızca sorgulayarak).
    Operatör-onaylı bir gelecekteki yeniden gönderim politikası KASITLI
    OLARAK Faz 11 kapsamı DIŞINDA bırakılmıştır. Doğrulama:
    `tests/test_execution_reconciliation_models.py::TestStateSetMembership`,
    `tests/test_execution_reconciliation_service.py::TestAmbiguousSubmission`,
    `::TestRestartRecovery`, `::TestIsolation`,
    `tests/test_execution_cli.py::TestReconcileCommand`.
23. (Faz 12) `AppConfig.execution_mode` varsayılanı `PAPER`dır ve
    `AppConfig`'de HİÇBİR credential alanı YOKTUR/OLAMAZ (Faz 8'den
    DEĞİŞTİRİLMEDEN). `Application`, `execution_mode !=
    BINANCE_SPOT_TESTNET` VEYA `enable_testnet_execution != True` iken
    `self._execution_service`/`self._execution_store`'u `None` OLARAK
    BIRAKIR — bu durumda erişilebilir HİÇBİR execution nesnesi
    YOKTUR (eksik config/typo/restart/stale-env/dashboard isteği bir
    TESTNET order'a ASLA yol AÇAMAZ, çünkü gönderecek nesne mevcut
    DEĞİLDİR). Doğrulama:
    `tests/test_phase12_local_production_readiness.py::TestPaperFirstSafety`,
    `::TestTestnetGating`.
24. (Faz 12) TESTNET etkinse, `Application.start()`,
    `ExecutionReconciliationService.reconcile_pending()`'i run/health
    loop'ları BAŞLAMADAN ÖNCE çalıştırır; bu adım BAŞARISIZ olursa
    `execution_ready=False` olarak fail-closed KALIR — ama PUBLIC/PAPER
    runtime ASLA bu yüzden durdurulmaz. Bu adım Faz 11'in
    `reconcile_pending()`'ini (Karar 78 DAHİL, DEĞİŞTİRİLMEDEN) ÇAĞIRIR
    — hiçbir koşulda otomatik bir `POST` üretemez.

    **BLOCKER FİX (Karar 81):** `reconcile_pending()` çağrılmadan ÖNCE,
    `execution/factory.py::testnet_credentials_status()` ile kimlik
    bilgisi VARLIĞI (DEĞERİ DEĞİL) kontrol edilir. Bekleyen kayıt YOKSA
    `reconcile_pending()` sıfır ağ çağrısı yaparak "başarıyla" tamamlanır
    — bu yüzden kimlik bilgisi eksikliği BİR SIGNED ÇAĞRIYA GÜVENEREK
    keşfedilemez; `execution_ready=True`, YALNIZCA HEM
    `BINANCE_TESTNET_API_KEY` HEM `BINANCE_TESTNET_API_SECRET` SET
    OLDUĞUNDA VE reconciliation BAŞARIYLA tamamlandığında MÜMKÜNDÜR.
    Doğrulama: `tests/test_phase12_local_production_readiness.py::TestStartupReconciliation`,
    `::TestCredentialPreflightBlockerFix`.
25. (Faz 12) `crypto_signal_engine/ops/dashboard.py`, salt-okunur bir
    HTTP sunucusudur: `GET` DIŞINDA HER metod (`POST`/`PUT`/`DELETE`/
    `PATCH`) `405` döner — hiçbir order-submission/config-mutation kod
    yolu YOKTUR. TEK veri kaynağı, disk üzerindeki atomik health-snapshot
    JSON dosyasıdır (canlı Python nesnelerine ASLA doğrudan erişmez).
    Varsayılan bind `127.0.0.1`dir; dashboard BAŞLATILAMASA (port
    çakışması) bile `Application.start()` BAŞARIYLA devam eder — ana
    runtime bu yüzden ASLA durmaz. Dashboard yanıtı HİÇBİR credential
    DEĞERİ İÇEREMEZ (yalnızca mode/enabled/ready/pending-count/detail).
    Doğrulama: `tests/test_ops_dashboard.py`,
    `tests/test_phase12_local_production_readiness.py::TestDashboardSafety`.
26. (Faz 12) TESTNET kimlik bilgisi okuma/imzalama/servis inşası,
    YALNIZCA `crypto_signal_engine/execution/factory.py` İÇİNDE kalır —
    `app.py` (execution/ SINIRININ DIŞINDA) hiçbir credential-literal/
    `place_order(`/private-endpoint izi İÇEREMEZ. Doğrulama:
    `tests/test_repository_safety_scan.py::TestPhase10ExecutionBoundarySafety`
    (DEĞİŞTİRİLMEDEN, artık `app.py`/`ops/*.py`'yi de otomatik kapsar).

27. (Pre-Audit Enhancement Pass — bkz. PRE_AUDIT_ENHANCEMENTS.md,
    DECISIONS.md Karar 82-89) Yeni `research/` paketi ve üç yeni CLI
    script'i (`scripts/historical_replay.py`/`oos_stability.py`/
    `monte_carlo_robustness.py`), `crypto_signal_engine/execution/`
    paketini HİÇBİR ZAMAN import ETMEZ, hiçbir TESTNET/private/signed
    endpoint izi İÇERMEZ, `ALLOW_LIVE_TRADING`'e HİÇ DOKUNMAZ — YALNIZCA
    `BinanceRestClient.fetch_historical_candles` (Faz 2, PUBLIC, kabul
    edilmiş) üzerinden gerçek geçmiş candle verisi çeker. Doğrulama:
    `tests/test_repository_safety_scan_research.py` (kabul edilmiş
    `tests/test_repository_safety_scan.py`'ı DEĞİŞTİRMEDEN, AYRI, additive
    bir tarama). Bu geçişte `crypto_signal_engine/` paketine yapılan
    YALNIZCA üç dokunuş — hepsi additive, varsayılan-davranış-koruyan,
    kendi testleriyle doğrulanmış: `PaperTradingEngine.process_signal`'a
    opsiyonel `notional_override` + yeni salt-okunur `has_processed()`
    accessor'ı (`paper_trading/engine.py`), `RuntimeCoordinator`'a
    opsiyonel, defansif-sarmalı `order_book_observer` hook'u
    (`runtime/coordinator.py`). Otomatik bir Signal->TESTNET execution
    köprüsü bu geçişte de EKLENMEDİ (Faz 12'nin Karar 80 ile ertelediği
    aynı sınır, DEĞİŞTİRİLMEDEN korunur).


=== FILE: adaptive/shadow.py ===
"""
Adaptive Intelligence v1, step 8 — live shadow evaluation with
INDEPENDENT state.

A challenger that has cleared discovery + confirmation is monitored in
SHADOW mode: it maintains its OWN independent hypothetical
`LifecyclePriceState`, seeded from the SAME `entry_price`/`atr`/
`entry_time` as the real champion position, but evolved candle-by-candle
using the CHALLENGER's OWN `ExitPolicyConfig` via the exact same
`evaluate_candle()` the live runtime uses (see `crypto_signal_engine.
execution.lifecycle.evaluate_candle` — no second implementation). It
NEVER reads or copies the real position's live stop/target/trailing
state after that one seeding moment.

WIRING (M1-fidelity fix, independent-verification round): production
shadow evaluation subscribes to `ShadowMonitor.on_m1_candle`, wired to
`crypto_signal_engine.execution.lifecycle_runtime.LifecycleRuntime`'s
`m1_candle_observer` hook — the SAME frequency `LifecycleManager.
evaluate_m1_candle()` itself uses for the real position, since that hook
fires once per REAL, already-closed M1 candle at the exact point
`_consume_m1` consumes the live M1 stream directly (bypassing
`RuntimeCoordinator` entirely). This matters because `evaluate_candle()`
has a fixed stop-before-target priority WITHIN one evaluated candle: at
true M1 granularity that ambiguity window is one minute (rare in
practice); aggregated to M5 it becomes 5x as likely, and if true
chronological order within that 5-minute window actually hit target
BEFORE stop, an M5-aggregated evaluation reports the WRONG exit reason
(stop, when target should have fired first) — undermining the very
shadow evidence promotion depends on. `ShadowMonitor.on_candle` (a valid
`RuntimeCoordinator.candle_observer` callable, M5/M15/H1-only) remains
available for coordinator-level use, but production shadow wiring uses
ONLY `on_m1_candle` — evaluating the SAME 5-minute window at both M1 and
M5 granularity would double-apply trailing-stop advancement. Both hooks
are defensively-wrapped, optional, and never raise into or alter their
respective host's control flow. Zero real order submission, zero effect
on the real position, zero new Binance REST/WS calls (candles arrive
purely from what the already-running live stream already fetched).

ATR is held FIXED for a shadow's whole life, seeded once at entry — the
same documented simplification already accepted in `crypto_signal_
engine.execution.lifecycle_replay_sanity.simulate_lifecycle_exits`.

CONCURRENCY CAP: `DEFAULT_MAX_CONCURRENT_SHADOWS = 3` — small enough that
shadow evaluation's per-candle overhead (a handful of pure-Python
dataclass updates) is never a live-runtime latency concern, large enough
to compare more than one candidate at a time (the mission's own "1-3").

NEW-ENTRY DETECTION: no new hook is needed beyond the candle observer —
`detect_new_long_entries()` periodically diffs `LifecycleStore.
list_positions()` (already-accepted, READ-ONLY) against the
previously-seen state, and `infer_entry_atr()` reconstructs the
entry-time ATR reference from the real position's OWN already-persisted
fields (`(gross_entry_vwap - initial_protective_stop) / stop_atr_
multiple`) — never a new FeatureEngine read, never a new network call."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    Candle as LifecycleCandle,
    ExitPolicyConfig,
    ExitReason,
    LifecyclePriceState,
    PositionLifecycleState,
    compute_initial_stop_and_target,
    evaluate_candle,
)

from adaptive.policy import PolicySnapshot

DEFAULT_MAX_CONCURRENT_SHADOWS = 3

# See `crypto_signal_engine/runtime/coordinator.py`'s `candle_observer`
# docstring: `ingest_candle` only ever receives M5/M15/H1 — this is the
# finest granularity available to any observer, live M1 never reaches it.
SHADOW_OBSERVATION_TIMEFRAME = Timeframe.M5


class ShadowCapacityError(Exception):
    """Raised when starting a new shadow would exceed the documented
    concurrency cap (`DEFAULT_MAX_CONCURRENT_SHADOWS` / caller override)."""


@dataclass(frozen=True)
class ShadowOutcome:
    exit_reason: ExitReason
    exit_price: float
    exit_time: datetime
    gross_pnl_per_unit: float


@dataclass
class ShadowChallenger:
    """One challenger's OWN, fully independent hypothetical position for
    ONE real entry. Mutable (candle-by-candle state evolution), but
    every mutation goes through `evaluate_candle()` — the identical pure
    function the live runtime uses — so this is never a second exit-logic
    implementation."""

    challenger_version_id: str
    symbol: str
    exit_policy: ExitPolicyConfig
    atr: float
    price_state: LifecyclePriceState
    outcome: ShadowOutcome | None = None

    @property
    def closed(self) -> bool:
        return self.outcome is not None

    def on_candle(self, candle: LifecycleCandle) -> None:
        if self.closed:
            return
        if candle.close_time <= self.price_state.entry_timestamp:
            return  # no-lookahead guard -- identical discipline to live evaluate_m1_candle
        result = evaluate_candle(self.price_state, candle, self.exit_policy, atr_for_trailing=self.atr)
        entry_price = self.price_state.entry_price
        self.price_state = result.updated_state
        if result.exit_reason is not None:
            self.outcome = ShadowOutcome(
                exit_reason=result.exit_reason, exit_price=candle.close, exit_time=candle.close_time,
                gross_pnl_per_unit=candle.close - entry_price,
            )


def start_shadow(
    challenger: PolicySnapshot, *, symbol: str, entry_price: float, atr: float, entry_time: datetime,
) -> ShadowChallenger:
    """Seeds a brand-new, fully independent shadow position from the SAME
    `entry_price`/`atr`/`entry_time` as the real champion position — and
    NOTHING else about that position is ever read. `compute_initial_
    stop_and_target` is the exact same pure entry-time function
    `on_entry_filled()` uses for the real position, called here with the
    CHALLENGER's own config, producing a genuinely independent stop/
    target whenever the challenger's config differs from the champion's."""
    policy = challenger.to_exit_policy_config()
    stop, target = compute_initial_stop_and_target(entry_price=entry_price, atr=atr, config=policy)
    price_state = LifecyclePriceState(
        entry_price=entry_price, entry_timestamp=entry_time, initial_protective_stop=stop,
        take_profit=target, high_water=entry_price, effective_stop=stop, trailing_active=False,
        last_stop_mechanism="STOP_LOSS",
    )
    return ShadowChallenger(
        challenger_version_id=challenger.version_id, symbol=symbol, exit_policy=policy, atr=atr,
        price_state=price_state,
    )


class ShadowMonitor:
    """Owns the bounded set of concurrently-shadowed challengers and IS a
    valid `RuntimeCoordinator.candle_observer` callable via `on_candle`."""

    def __init__(self, *, max_concurrent: int = DEFAULT_MAX_CONCURRENT_SHADOWS) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent en az 1 olmalı")
        self._max_concurrent = max_concurrent
        self._active: dict[str, ShadowChallenger] = {}  # keyed by challenger_version_id

    @property
    def active_challengers(self) -> tuple[ShadowChallenger, ...]:
        return tuple(self._active.values())

    def start_shadowing(
        self, challenger: PolicySnapshot, *, symbol: str, entry_price: float, atr: float, entry_time: datetime,
    ) -> ShadowChallenger:
        if challenger.version_id in self._active:
            return self._active[challenger.version_id]
        if len(self._active) >= self._max_concurrent:
            raise ShadowCapacityError(
                f"shadow kapasitesi doldu ({self._max_concurrent}) -- yeni challenger başlatılamıyor: "
                f"{challenger.version_id}"
            )
        shadow = start_shadow(challenger, symbol=symbol, entry_price=entry_price, atr=atr, entry_time=entry_time)
        self._active[challenger.version_id] = shadow
        return shadow

    def stop_shadowing(self, challenger_version_id: str) -> ShadowChallenger | None:
        return self._active.pop(challenger_version_id, None)

    def on_candle(self, symbol: str, timeframe: Timeframe, candle) -> None:  # noqa: ANN001
        """Valid directly as a `RuntimeCoordinator.candle_observer`
        callable. Only `SHADOW_OBSERVATION_TIMEFRAME` (M5) candles are
        acted on; every other timeframe is silently ignored — not an
        error, just outside this monitor's scope. Never reads any
        `crypto_signal_engine.execution` state — the only input is the
        candle the coordinator already fetched for its own pipeline.

        NOT used by production wiring (see module docstring's M1-fidelity
        fix) — kept for direct `RuntimeCoordinator`-level use/tests only.
        Never wire this AND `on_m1_candle` to the same live symbol
        simultaneously: both would evaluate the same 5-minute window,
        double-applying trailing-stop advancement."""
        if timeframe is not SHADOW_OBSERVATION_TIMEFRAME:
            return
        lifecycle_candle = LifecycleCandle(
            open=candle.open, high=candle.high, low=candle.low, close=candle.close, close_time=candle.close_time,
        )
        for shadow in self._active.values():
            if shadow.symbol == symbol:
                shadow.on_candle(lifecycle_candle)

    def on_m1_candle(self, symbol: str, candle: LifecycleCandle) -> None:
        """The PREFERRED, PRODUCTION shadow-observation path (M1-fidelity
        fix, independent-verification round). Valid directly as a
        `LifecycleRuntime.m1_candle_observer` callable — fires once per
        REAL, already-closed M1 candle, the SAME frequency
        `LifecycleManager.evaluate_m1_candle()` itself uses for the real
        position. No timeframe filter needed: this hook only ever fires
        M1 candles by construction (unlike `on_candle` above, which must
        filter `RuntimeCoordinator`'s mixed M5/M15/H1 stream)."""
        for shadow in self._active.values():
            if shadow.symbol == symbol:
                shadow.on_candle(candle)


def infer_entry_atr(position: BridgePositionRecord) -> float | None:
    """Reconstructs the entry-time ATR reference from an already-
    persisted `BridgePositionRecord`'s own durable fields — no new
    FeatureEngine read, no new network call. Uses the position's OWN
    pinned stop multiple (step 2's embedded field) when present, falling
    back to `ExitPolicyConfig()`'s bare default otherwise (mirrors
    `BridgePositionRecord.resolved_exit_policy()`'s own fallback
    discipline) — consistent with whichever multiple actually produced
    this position's `initial_protective_stop`."""
    if position.gross_entry_vwap is None or position.initial_protective_stop is None:
        return None
    stop_multiple = position.exit_policy_stop_atr_multiple or ExitPolicyConfig().stop_atr_multiple
    if stop_multiple <= 0:
        return None
    atr = (position.gross_entry_vwap - position.initial_protective_stop) / stop_multiple
    return atr if atr > 0 else None


def detect_new_long_entries(
    *, previous_states: dict[str, PositionLifecycleState], current_positions: tuple[BridgePositionRecord, ...],
) -> tuple[BridgePositionRecord, ...]:
    """Pure diff: a symbol that is LONG now but was NOT LONG in
    `previous_states` is a genuine new entry worth seeding a shadow for.
    Mirrors `lifecycle_replay_sanity.simulate_lifecycle_exits`'s own
    `previous_side` FLAT->LONG detection discipline, applied to
    `LifecycleStore.list_positions()` (already-accepted, READ-ONLY)
    instead of replay cycle results."""
    new_entries = []
    for position in current_positions:
        was_long = previous_states.get(position.symbol) is PositionLifecycleState.LONG
        if position.state is PositionLifecycleState.LONG and not was_long:
            new_entries.append(position)
    return tuple(new_entries)


=== FILE: adaptive/store.py ===
"""
Adaptive Intelligence v1, step 12 — append-only, restart-durable
persistence for the full policy version history, current champion, and
the full decision log across all three evidence classes.

Mirrors `crypto_signal_engine.execution.lifecycle_store.LifecycleStore`'s
existing SQLite discipline (own dedicated connection, WAL mode,
`CREATE TABLE IF NOT EXISTS`, explicit column-by-column mapping — never
`SELECT *` reconstructed straight into a dataclass) rather than importing
it, keeping `adaptive/` a standalone, additive package with its own
dedicated database file (never sharing a file with, or writing a single
byte into, any table `crypto_signal_engine/` owns — see step 15's state-
protection requirement).

Four tables, all append-only (rows are never UPDATEd or DELETEd, matching
`bridge_fee_ledger`/`bridge_completed_trade`'s own append-only
discipline):

- `adaptive_policy_version`: every `PolicySnapshot` ever created
  (challenger or champion), keyed by its own `version_id`.
- `adaptive_champion_history`: one row per promotion (including the
  initial bootstrap champion) — "the current champion" is simply the
  most recent row here, never a separately-tracked mutable pointer.
- `adaptive_decision_log`: one row per `adaptive.decision.decide()` call
  ever made, regardless of outcome (PROMOTE/KEEP_CHAMPION/
  INSUFFICIENT_EVIDENCE all logged identically) — the three evidence
  classes are stored as three SEPARATE JSON blobs, never merged into one.
- `adaptive_cursor`: a tiny generic key-value table for restart-durable
  scalars this package needs to remember across restarts (currently just
  `adaptive.windows`'s `last_confirmation_end`)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from adaptive.decision import DecisionResult, EvidenceSummary
from adaptive.policy import PolicySnapshot
from adaptive.shadow import ShadowOutcome
from crypto_signal_engine.execution.lifecycle import ExitReason

_CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS adaptive_policy_version (
    version_id TEXT PRIMARY KEY,
    stop_atr_multiple REAL NOT NULL,
    take_profit_atr_multiple REAL NOT NULL,
    trailing_activation_atr_multiple REAL NOT NULL,
    trailing_distance_atr_multiple REAL NOT NULL,
    max_hold_hours REAL NOT NULL,
    created_at TEXT NOT NULL,
    provenance TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_champion_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL,
    promoted_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adaptive_champion_history_time ON adaptive_champion_history(promoted_at);

CREATE TABLE IF NOT EXISTS adaptive_decision_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    challenger_version_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    discovery_challenger_json TEXT NOT NULL,
    discovery_champion_json TEXT NOT NULL,
    confirmation_challenger_json TEXT NOT NULL,
    confirmation_champion_json TEXT NOT NULL,
    shadow_challenger_json TEXT NOT NULL,
    shadow_champion_json TEXT NOT NULL,
    drift_note TEXT
);
CREATE INDEX IF NOT EXISTS idx_adaptive_decision_log_time ON adaptive_decision_log(recorded_at);

CREATE TABLE IF NOT EXISTS adaptive_cursor (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_shadow_eligible (
    version_id TEXT PRIMARY KEY,
    marked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adaptive_shadow_outcome (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    challenger_version_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exit_reason TEXT NOT NULL,
    exit_price REAL NOT NULL,
    exit_time TEXT NOT NULL,
    gross_pnl_per_unit REAL NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adaptive_shadow_outcome_version ON adaptive_shadow_outcome(challenger_version_id);
"""


def _iso(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _evidence_to_json(evidence: EvidenceSummary) -> str:
    return json.dumps(asdict(evidence))


def _evidence_from_json(text: str) -> EvidenceSummary:
    return EvidenceSummary(**json.loads(text))


class AdaptiveStore:
    """Opens its OWN dedicated connection to its OWN dedicated database
    file — never the same file as `LifecycleStore`/`ExecutionStateStore`/
    `PaperStateStore` (see step 15: this package must not alter the
    schema or content of any existing persisted state)."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._connection = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        with self._connection:
            self._connection.executescript(_CREATE_SCHEMA_SQL)

    def close(self) -> None:
        self._connection.close()

    # -- adaptive_policy_version ---------------------------------------------

    def record_policy_version(self, snapshot: PolicySnapshot) -> None:
        """Append-only: `INSERT OR IGNORE` makes re-recording an
        already-known `version_id` a safe no-op (idempotent under
        crash-and-retry), never an overwrite of a differently-valued row
        under the same id."""
        with self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO adaptive_policy_version (
                    version_id, stop_atr_multiple, take_profit_atr_multiple,
                    trailing_activation_atr_multiple, trailing_distance_atr_multiple, max_hold_hours,
                    created_at, provenance
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.version_id, snapshot.stop_atr_multiple, snapshot.take_profit_atr_multiple,
                    snapshot.trailing_activation_atr_multiple, snapshot.trailing_distance_atr_multiple,
                    snapshot.max_hold_hours, _iso(snapshot.created_at), snapshot.provenance,
                ),
            )

    def load_policy_version(self, version_id: str) -> PolicySnapshot | None:
        row = self._connection.execute(
            "SELECT * FROM adaptive_policy_version WHERE version_id = ?", (version_id,)
        ).fetchone()
        if row is None:
            return None
        return PolicySnapshot(
            version_id=row["version_id"], stop_atr_multiple=row["stop_atr_multiple"],
            take_profit_atr_multiple=row["take_profit_atr_multiple"],
            trailing_activation_atr_multiple=row["trailing_activation_atr_multiple"],
            trailing_distance_atr_multiple=row["trailing_distance_atr_multiple"],
            max_hold_hours=row["max_hold_hours"], created_at=_parse_dt(row["created_at"]),
            provenance=row["provenance"],
        )

    def list_policy_versions(self) -> tuple[PolicySnapshot, ...]:
        rows = self._connection.execute("SELECT version_id FROM adaptive_policy_version ORDER BY created_at ASC").fetchall()
        return tuple(self.load_policy_version(row["version_id"]) for row in rows)  # type: ignore[misc]

    # -- adaptive_champion_history --------------------------------------------

    def promote_champion(self, snapshot: PolicySnapshot, *, reason: str, now: datetime) -> None:
        """Records the policy version (idempotent) AND appends a new
        champion-history row — "the current champion" is always just the
        latest row here, never a separately-tracked mutable pointer that
        could drift out of sync with the version history."""
        self.record_policy_version(snapshot)
        with self._connection:
            self._connection.execute(
                "INSERT INTO adaptive_champion_history (version_id, promoted_at, reason) VALUES (?, ?, ?)",
                (snapshot.version_id, _iso(now), reason),
            )

    def current_champion(self) -> PolicySnapshot | None:
        row = self._connection.execute(
            "SELECT version_id FROM adaptive_champion_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return self.load_policy_version(row["version_id"])

    def champion_history(self) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT version_id, promoted_at, reason FROM adaptive_champion_history ORDER BY id ASC"
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- adaptive_decision_log -------------------------------------------------

    def record_decision(self, result: DecisionResult, *, now: datetime, drift_note: str | None = None) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_decision_log (
                    recorded_at, challenger_version_id, decision, reason,
                    discovery_challenger_json, discovery_champion_json,
                    confirmation_challenger_json, confirmation_champion_json,
                    shadow_challenger_json, shadow_champion_json, drift_note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _iso(now), result.challenger_version_id, result.decision.value, result.reason,
                    _evidence_to_json(result.discovery_challenger), _evidence_to_json(result.discovery_champion),
                    _evidence_to_json(result.confirmation_challenger), _evidence_to_json(result.confirmation_champion),
                    _evidence_to_json(result.shadow_challenger), _evidence_to_json(result.shadow_champion),
                    drift_note,
                ),
            )

    def recent_decisions(self, limit: int = 50) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT * FROM adaptive_decision_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return tuple(dict(row) for row in rows)

    # -- adaptive_cursor (generic key-value, e.g. windows.py's last_confirmation_end) ---

    def get_cursor(self, name: str) -> str | None:
        row = self._connection.execute("SELECT value FROM adaptive_cursor WHERE name = ?", (name,)).fetchone()
        return row["value"] if row is not None else None

    def set_cursor(self, name: str, value: str, *, now: datetime) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_cursor (name, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (name, value, _iso(now)),
            )

    # -- adaptive_shadow_eligible ----------------------------------------------

    def mark_shadow_eligible(self, version_id: str, *, now: datetime) -> None:
        """A challenger that cleared discovery + confirmation (step 9) is
        marked here — restart-durable, so a live runtime that restarts
        mid-shadow-observation does not lose track of which challengers
        it was supposed to be shadowing."""
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO adaptive_shadow_eligible (version_id, marked_at) VALUES (?, ?)",
                (version_id, _iso(now)),
            )

    def list_shadow_eligible(self) -> tuple[str, ...]:
        rows = self._connection.execute(
            "SELECT version_id FROM adaptive_shadow_eligible ORDER BY marked_at ASC"
        ).fetchall()
        return tuple(row["version_id"] for row in rows)

    # -- adaptive_shadow_outcome -------------------------------------------------

    def record_shadow_outcome(self, challenger_version_id: str, *, symbol: str, outcome: ShadowOutcome, now: datetime) -> None:
        """Append-only: EVERY completed shadow trade for a challenger is
        kept, across restarts — `adaptive.decision.shadow_evidence_from_
        outcomes` accumulates evidence across however many of these a
        challenger has collected so far, never just the most recent one."""
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO adaptive_shadow_outcome (
                    challenger_version_id, symbol, exit_reason, exit_price, exit_time,
                    gross_pnl_per_unit, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    challenger_version_id, symbol, outcome.exit_reason.value, outcome.exit_price,
                    _iso(outcome.exit_time), outcome.gross_pnl_per_unit, _iso(now),
                ),
            )

    def shadow_outcomes_for(self, challenger_version_id: str) -> tuple[ShadowOutcome, ...]:
        rows = self._connection.execute(
            "SELECT * FROM adaptive_shadow_outcome WHERE challenger_version_id = ? ORDER BY id ASC",
            (challenger_version_id,),
        ).fetchall()
        return tuple(
            ShadowOutcome(
                exit_reason=ExitReason(row["exit_reason"]), exit_price=row["exit_price"],
                exit_time=_parse_dt(row["exit_time"]), gross_pnl_per_unit=row["gross_pnl_per_unit"],
            )
            for row in rows
        )


=== FILE: deploy/systemd/crypto-signal-engine.service ===
[Unit]
Description=Crypto Signal Engine — public-data paper-trading service (Phase 8)
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=csengine
Group=csengine
WorkingDirectory=/opt/crypto-signal-engine
EnvironmentFile=/etc/crypto-signal-engine/env
ExecStart=/opt/crypto-signal-engine/venv/bin/python -m crypto_signal_engine.app run

# Graceful shutdown: SIGTERM is forwarded by systemd on stop/restart; the
# application handles it itself (idempotent, bounded) — no extra
# ExecStop/kill script is needed or wanted.
KillSignal=SIGTERM
TimeoutStopSec=30

# Bounded crash-restart policy (Section: "no unsafe restart loop"): restart
# on failure only (a clean exit 0 from a deliberate stop is NOT restarted),
# with a fixed delay, and a burst limiter so a persistently-crashing
# deployment stops retrying instead of looping forever.
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=300
StartLimitBurst=5

# 24/7 Ops v1 — watchdog: catches a HUNG process (event loop wedged),
# which a plain exit-code-based Restart=on-failure never detects. The
# app's health loop (default CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS=5s)
# sends a WATCHDOG=1 sd_notify heartbeat on every iteration (see
# crypto_signal_engine/ops/systemd_notify.py); 60s is 12x that default
# interval — generous headroom against an occasional slow tick (a GC
# pause, a slow snapshot write) while still bounding a genuine hang to
# at most one minute before systemd force-kills and restarts it (subject
# to the same Restart=on-failure/StartLimitBurst policy above). Works
# with Type=simple (unchanged) — WatchdogSec is independent of Type; it
# only requires WATCHDOG=1 notifications on $NOTIFY_SOCKET, which this
# unit's default NotifyAccess=main already permits from the main PID.
WatchdogSec=60

# Least-privilege hardening. The service never needs root, never needs to
# write outside its own data/lock/health directory, and makes no private
# Binance calls (public REST/WebSocket only).
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/crypto-signal-engine

# Logs go to journald by default (journalctl -u crypto-signal-engine -f).
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target


=== FILE: deploy/systemd/crypto-signal-engine-backup.service ===
[Unit]
Description=Crypto Signal Engine — daily SQLite online backup (retention-managed)
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md
# Not a hard dependency (a missed live-service start must never block a
# backup of whatever durable state already exists on disk) — just orders
# it after, so a backup taken right after boot sees the freshest state.
After=crypto-signal-engine.service

[Service]
Type=oneshot
User=csengine
Group=csengine
WorkingDirectory=/opt/crypto-signal-engine
EnvironmentFile=/etc/crypto-signal-engine/env

# `scripts/backup_sqlite.py`'nin CORE mantığı (SQLite Online Backup API)
# DEĞİŞTİRİLMEDEN çağrılır (bkz. PHASE8_UBUNTU_OPERATIONS.md Bölüm 9).
# Tarih damgalı hedef dosya adı ve `--keep-last` retention burada,
# ExecStart seviyesinde eklenir — script'in KENDİSİ hâlâ tek bir
# (--source, --destination) çifti üzerinde çalışan, tarih/retention
# kavramından habersiz saf bir araçtır.
ExecStart=/bin/sh -c '\
    SRC="${CSE_DB_PATH:-/var/lib/crypto-signal-engine/paper_state.db}"; \
    STEM="$(basename "$SRC" .db)"; \
    /opt/crypto-signal-engine/venv/bin/python scripts/backup_sqlite.py \
        --source "$SRC" \
        --destination "/var/backups/crypto-signal-engine/${STEM}-$(date -u +%%Y%%m%%dT%%H%%M%%SZ).db" \
        --keep-last 14 \
    '

NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/backups/crypto-signal-engine /var/lib/crypto-signal-engine

StandardOutput=journal
StandardError=journal


=== FILE: deploy/systemd/crypto-signal-engine-backup.timer ===
[Unit]
Description=Daily timer for crypto-signal-engine-backup.service
Documentation=file:/opt/crypto-signal-engine/PHASE8_UBUNTU_OPERATIONS.md

[Timer]
# Daily is the documented default schedule (see DECISIONS.md, "24/7 Ops
# v1" Karar) — a paper/testnet position book changes at trading pace,
# not multiple-times-a-day pace; a day of loss is an acceptable, bounded
# recovery-time-objective for a non-mainnet system that still keeps
# in-memory recovery + the durable SQLite file itself as the primary
# safety net (this timer is defense-in-depth against disk/host loss, not
# the primary durability mechanism).
OnCalendar=daily
# Spreads actual run time across a 30-minute window instead of every
# machine firing at exactly 00:00 UTC — irrelevant for a single host, but
# free and harmless, and avoids the backup always landing mid-candle at
# the exact same second every day.
RandomizedDelaySec=1800
# Catches up on a missed run (machine was off at the scheduled time) the
# next time the machine boots/is online — a skipped day of backups on a
# host that happened to be down is a real gap, not an acceptable one.
Persistent=true

[Install]
WantedBy=timers.target


=== FILE: deploy/env.example ===
# Crypto Signal Engine — example environment file for systemd's
# EnvironmentFile=/etc/crypto-signal-engine/env
#
# Copy this file to /etc/crypto-signal-engine/env, edit the values you need,
# and restrict its permissions (it contains no secrets — there are none in
# this system — but keep operational config out of world-readable reach as
# good practice: chmod 640, chown root:csengine).
#
# There is NO api key / secret / credential field in this CSE_* namespace,
# and there never will be (see SAFETY_INVARIANTS.md). Phase 12 adds an
# OPT-IN Binance Spot TESTNET execution mode (still not this file's
# concern — see the note near the bottom); by default this service only
# ever reads PUBLIC Binance market data and simulates trades locally.

# Required: comma-separated list of Binance symbols to track.
CSE_SYMBOLS=BTCUSDT,ETHUSDT

# Durable SQLite state (paper positions, candle checkpoints). Use an
# absolute path under /var/lib/crypto-signal-engine/ in production.
CSE_DB_PATH=/var/lib/crypto-signal-engine/paper_state.db

# Optional overrides (safe defaults are used if omitted):
# CSE_LOG_LEVEL=INFO
# CSE_STALE_FEED_THRESHOLD_SECONDS=30
# CSE_FEE_BPS=0
# CSE_SLIPPAGE_BPS=0
# CSE_NOTIONAL_PER_POSITION=1000
# CSE_WARMUP_CANDLES=25
# CSE_ORDER_BOOK_DEPTH=20
# CSE_RECONNECT_INITIAL_DELAY_SECONDS=1
# CSE_RECONNECT_MAX_DELAY_SECONDS=60
# CSE_RECONNECT_MULTIPLIER=2
# CSE_RECONNECT_JITTER_SECONDS=0.5
# CSE_RECONNECT_MAX_ATTEMPTS=
# CSE_LOCK_PATH=/var/lib/crypto-signal-engine/crypto-signal-engine.lock
# CSE_HEALTH_SNAPSHOT_PATH=/var/lib/crypto-signal-engine/health.json
# CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS=5

# Phase 12 — execution mode (default PAPER; only move to Stage B after
# weeks of stable local PAPER operation — see
# PHASE12_FINAL_LOCAL_PRODUCTION_READINESS.md):
# CSE_EXECUTION_MODE=PAPER
# CSE_ENABLE_TESTNET_EXECUTION=false
#
# Phase 12 — local read-only operations dashboard (loopback-only by
# default; see the PHASE12 doc's "phone/Tailscale operating model" for
# safe remote viewing — never set CSE_DASHBOARD_HOST to 0.0.0.0):
# CSE_DASHBOARD_ENABLED=true
# CSE_DASHBOARD_HOST=127.0.0.1
# CSE_DASHBOARD_PORT=8787
#
# 24/7 Ops v1 — Control Center admin token (pause/resume/stop on the
# dashboard, see PHASE8_UBUNTU_OPERATIONS.md and DECISIONS.md). Unset
# (default) = admin actions fully disabled, dashboard stays read-only
# exactly as before. This is a LOCAL admin secret, not an exchange
# credential — still keep it out of world-readable reach (this file is
# already chmod 640, chown root:csengine).
# CSE_DASHBOARD_ADMIN_TOKEN=
#
# 24/7 Ops v1 — outbound-only Telegram alerting. Deliberately set OUTSIDE
# this CSE_* namespace (same precedent as BINANCE_TESTNET_API_KEY/
# _SECRET below) since it is a credential, read only by
# crypto_signal_engine/ops/notifier.py, and never appears in
# AppConfig.summary() or any log line. Unset (default, either or both) =
# no notifier configured, every alerting call site becomes a silent
# no-op.
# CSE_TELEGRAM_BOT_TOKEN=
# CSE_TELEGRAM_CHAT_ID=
#
# NOTE: BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET /
# BINANCE_TESTNET_EXECUTION_DB_PATH are deliberately NOT listed in this
# file — they are Binance Spot TESTNET signing credentials (Phase 10/11),
# set separately from this shared template, and are read only by
# crypto_signal_engine/execution/. This file's CSE_* namespace (AppConfig)
# still carries zero credential fields, unconditionally.


=== FILE: pyproject.toml ===
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[project]
name = "crypto-signal-engine"
version = "0.1.0"
description = "Binance Crypto Intelligence / Signal Engine — Phase 1 domain layer (no execution code)"
requires-python = ">=3.10"

# `websockets` is required only to run the REAL runtime (Phase 6/8's
# `RealWebSocketConnectionFactory`, lazy-imported on first real use) against
# live Binance PUBLIC market data. It is intentionally optional: the entire
# offline test suite runs without it (fake/mock transports only), so a fresh
# checkout can be installed and tested with zero extra dependencies. Install
# it for an actual Ubuntu deployment: `pip install .[runtime]`.
[project.optional-dependencies]
runtime = ["websockets>=12.0"]

# Console entry point for a `pip install`-ed deployment. Equivalent to
# `python -m crypto_signal_engine.app` (see PHASE8_UBUNTU_OPERATIONS.md).
[project.scripts]
crypto-signal-engine = "crypto_signal_engine.app:main"

[tool.setuptools.packages.find]
include = ["crypto_signal_engine*"]

[tool.pytest.ini_options]
testpaths = ["tests"]


