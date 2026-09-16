"""
Faz 10 — Binance request imzalama, izole bir modülde.

Kural: `SignalEngine`/`agents`/`FeatureEngine`/`RuntimeCoordinator`/
`PaperTradingEngine` HTTP signing'den HABERDAR DEĞİLDİR ve olamaz — bu
modül yalnızca `adapter.py`/`testnet_client.py` tarafından kullanılır.

İmzalama, Binance'in resmi Spot API sözleşmesiyle AYNIDIR: parametreler
`application/x-www-form-urlencoded` olarak (INSERTION SIRASINA göre,
alfabetik sıralama GEREKMEZ — Binance imzayı TAM OLARAK gönderilen query
string üzerinden doğrular) kodlanır, ardından bu TAM string HMAC-SHA256
ile `api_secret` kullanılarak imzalanır. Secret hiçbir zaman logda/hata
mesajında GÖRÜNMEZ (bkz. `sign()` — yalnızca hex digest döner, secret'ı
asla TEKRAR ETMEZ)."""

from __future__ import annotations

import hashlib
import hmac
import urllib.parse
from collections import OrderedDict


def canonical_query_string(params: "OrderedDict[str, str]") -> str:
    """Parametreleri, VERİLEN sırayla (insertion order — Binance'in
    signing sözleşmesi budur) `application/x-www-form-urlencoded` olarak
    kodlar. Çağıran, deterministik bir sıra ile bir `OrderedDict`
    OLUŞTURMAKTAN sorumludur (bkz. `testnet_client.py::_build_signed_params`
    — sabit, dokümante edilmiş bir alan sırası kullanır)."""
    return urllib.parse.urlencode(list(params.items()))


class BinanceTestnetSigner:
    """HMAC-SHA256 imzalayıcı. `api_secret`, yalnızca bu sınıfın
    constructor'ında TUTULUR — hiçbir `__repr__`/`__str__` override'ı
    YOKTUR (varsayılan `object.__repr__`, secret'ı asla YAZDIRMAZ)."""

    def __init__(self, api_secret: str) -> None:
        if not api_secret.strip():
            raise ValueError("api_secret boş olamaz")
        self._secret = api_secret.encode("utf-8")

    def sign(self, canonical_query: str) -> str:
        return hmac.new(self._secret, canonical_query.encode("utf-8"), hashlib.sha256).hexdigest()
