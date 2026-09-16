"""
Faz 10 — Binance Spot TESTNET Execution Lab.

Bu paket, sistemin İLK private/signed Binance execution sınırını sunar —
KESİNLİKLE ve YALNIZCA Binance Spot TESTNET'e karşı
(`https://testnet.binance.vision`). Bu bir üretim/execution fazı DEĞİLDİR;
"bir intent'in güvenli şekilde TESTNET'e çevrilip/imzalanıp/gönderilip/
gözlemlenebildiğini" kanıtlayan izole bir LAB'dır.

HARD SAFETY INVARIANT (değişmez, bkz. SAFETY_INVARIANTS.md):
- `ALLOW_LIVE_TRADING = False` (crypto_signal_engine/__init__.py) KORUNUR
  ve bu paket tarafından HİÇBİR ŞEKİLDE değiştirilmez/etkisizleştirilmez.
- Mainnet execution bu paket üzerinden YAPISAL OLARAK İMKANSIZDIR: private
  istemci, sabit-kodlanmış bir host allowlist'i (`testnet.binance.vision`,
  yalnızca `https`) dışında HERHANGİ bir host ile inşa/kullanım
  REDDEDİLİR (bkz. `testnet_client.py::_validate_testnet_host`) — bu
  kontrol substring DEĞİL, tam `urlsplit().hostname` eşleşmesi kullanır.
- Kimlik bilgileri YALNIZCA ortam değişkenlerinden okunur; hiçbir zaman
  loglanmaz, exception mesajlarına/sonuç modellerine/SQLite'a/dokümana
  YAZILMAZ.
- Futures/margin/withdrawal/transfer/broker endpoint'i YOKTUR ve
  OLAMAZ — yalnızca Spot `account`/`exchangeInfo`/`order` (TESTNET).
- Normal PUBLIC runtime (Faz 6/8) + `PaperTradingEngine` (Faz 5) bu
  paketten TAMAMEN BAĞIMSIZDIR ve HİÇBİR sinyal otomatik olarak bu
  pakete yönlendirilmez — yalnızca `scripts/binance_testnet_lab.py`
  üzerinden KASITLI, MANUEL çağrı ile kullanılır.

Modüller:
- `models.py` — `ExecutionMode`, `OrderSide`, `OrderType`, `TimeInForce`,
  `OrderIntent` (deterministik `client_order_id` dahil), `ExecutionResult`.
- `errors.py` — Faz 10 exception taksonomisi (`ExecutionError` alt
  sınıfları, hiçbiri secret/signature İÇERMEZ).
- `signer.py` — HMAC-SHA256 imzalama + kanonik parametre kodlama.
- `testnet_client.py` — TESTNET-only HTTP istemcisi (host allowlist,
  sinyalli GET/POST, hata çevirisi).
- `adapter.py` — `TestnetExecutionAdapter`: intent doğrulama (exchange
  filter'ları) + imzalama + gönderim + sonuç modeli üretimi."""

from __future__ import annotations
