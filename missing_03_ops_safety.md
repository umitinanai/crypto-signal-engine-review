<!-- missing_03_ops_safety.md — 16 files -->
<!-- Contents of this part: -->
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
<!--   - crypto_signal_engine/safety/__init__.py (1 bytes) -->
<!--   - crypto_signal_engine/safety/models.py (7981 bytes) -->
<!--   - crypto_signal_engine/consensus/__init__.py (0 bytes) -->
<!--   - crypto_signal_engine/consensus/engine.py (3393 bytes) -->
<!--   - crypto_signal_engine/consensus/risk.py (6549 bytes) -->

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


=== FILE: crypto_signal_engine/safety/__init__.py ===



=== FILE: crypto_signal_engine/safety/models.py ===
"""
Safety domain modelleri.

SafetyOrchestrator'ın (Faz 2+) üzerinde çalışacağı veri sözleşmesi. Bu
modülde hiçbir execution/trade kodu YOKTUR ve olamaz — SafetyOrchestrator
yalnızca analiz sistemini durdurur.

HARDENING NOTU (Quality Gate 18): SafetyState artık açık bir state machine
sözleşmesi uygular:
- `halt()` yalnızca HALT severity ile çağrılabilir VE yalnızca sistem
  hâlihazırda halted DEĞİLKEN çağrılabilir (zaten halted iken tekrar
  halt() çağrısı illegal transition'dır, ValueError fırlatır).
- `resume()` yalnızca sistem HALTED iken çağrılabilir (halted değilken
  resume() çağrısı illegal transition'dır).
- `add_warning()` aynı (reason_code, component, symbol) üçlüsüne sahip bir
  event zaten aktif warning listesindeyse, ESKİSİNİ YENİSİYLE DEĞİŞTİRİR
  (idempotent upsert) — biriktirerek duplicate şişmesi yaratmaz. Bu,
  "aynı safety event'in duplicate işlenmesi" için deterministik politikadır.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from crypto_signal_engine.domain._validation import freeze_mapping, normalize_symbol, require_enum, require_utc_aware
from crypto_signal_engine.domain.enums import SafetySeverity


class SafetyReasonCode(str, Enum):
    """Master Prompt Bölüm 6'daki tetikleyicilerin sabit kodları."""

    WEBSOCKET_DATA_LOSS = "WEBSOCKET_DATA_LOSS"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"
    BINANCE_CONNECTION_ISSUE = "BINANCE_CONNECTION_ISSUE"
    ABNORMAL_SPREAD = "ABNORMAL_SPREAD"
    EXTREME_VOLATILITY = "EXTREME_VOLATILITY"
    INSUFFICIENT_ORDER_BOOK_DEPTH = "INSUFFICIENT_ORDER_BOOK_DEPTH"
    DATA_MISMATCH = "DATA_MISMATCH"
    CLOCK_DRIFT = "CLOCK_DRIFT"
    API_RATE_LIMIT = "API_RATE_LIMIT"
    DATABASE_CORRUPTION = "DATABASE_CORRUPTION"
    EXCESSIVE_MODEL_DISAGREEMENT = "EXCESSIVE_MODEL_DISAGREEMENT"
    INVALID_FEATURE_VALUE = "INVALID_FEATURE_VALUE"
    TESTNET_ENVIRONMENT_VERIFICATION_FAILED = "TESTNET_ENVIRONMENT_VERIFICATION_FAILED"


@dataclass(frozen=True)
class SafetyEvent:
    """Sistemde herhangi bir bileşenin ürettiği tek bir güvenlik olayı."""

    reason_code: SafetyReasonCode
    severity: SafetySeverity
    component: str
    message: str
    occurred_at: datetime
    symbol: str | None = None
    context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_enum(self.reason_code, SafetyReasonCode, "reason_code")
        require_enum(self.severity, SafetySeverity, "severity")
        require_utc_aware(self.occurred_at, "occurred_at")
        if not self.message.strip():
            raise ValueError("message boş olamaz")
        if not self.component.strip():
            raise ValueError("component boş olamaz")
        if self.symbol is not None:
            object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        object.__setattr__(self, "context", freeze_mapping(self.context, "context"))

    @property
    def dedup_key(self) -> tuple[SafetyReasonCode, str, str | None]:
        """Duplicate tespiti için deterministik anahtar: (reason_code, component, symbol)."""
        return (self.reason_code, self.component, self.symbol)


class SafetyState:
    """SafetyOrchestrator'ın mevcut anlık durumu.

    HARDENING NOTU (Quality Gate 24): bu sınıf artık bir `@dataclass` DEĞİL,
    tamamen encapsulate edilmiş bir sınıftır. Kritik state alanları
    (`_signal_generation_halted`, `_halt_reason`) PRIVATE'tir ve yalnızca
    read-only property'ler üzerinden okunabilir. Dış kod:

        state.signal_generation_halted = False   # AttributeError (property, setter yok)

    gibi bir atama YAPAMAZ; mutation yalnızca `halt()`, `resume()`,
    `add_warning()`, `clear_warnings()`, `mark_evaluated()` metodları
    üzerinden, kontrollü biçimde mümkündür.

    Constructor hiçbir argüman ALMAZ — bir SafetyState her zaman RUNNING
    (halted=False, halt_reason=None) durumunda başlar. Bu, önceki
    `SafetyState(signal_generation_halted=True, halt_reason=None)` gibi bir
    illegal state'in constructor seviyesinde bile üretilebilmesini
    FİZİKSEL OLARAK İMKANSIZ kılar.

    Invariant (her zaman, her okumada doğru olmalı):
    - HALTED ise: `signal_generation_halted is True` VE `halt_reason is not
      None` VE `halt_reason.severity == SafetySeverity.HALT`.
    - RUNNING ise: `signal_generation_halted is False` VE `halt_reason is
      None`.
    Bu invariant yalnızca `halt()`/`resume()` metodlarının bir arada
    garanti ettiği bir eş-değişim (co-variation) olduğu için sağlanır;
    hiçbir zaman birini diğerinden bağımsız güncelleyen bir kod yolu yoktur.
    """

    __slots__ = ("_signal_generation_halted", "_halt_reason", "_active_warnings", "_last_evaluated_at")

    def __init__(self) -> None:
        self._signal_generation_halted: bool = False
        self._halt_reason: SafetyEvent | None = None
        self._active_warnings: tuple[SafetyEvent, ...] = ()
        self._last_evaluated_at: datetime | None = None

    @property
    def signal_generation_halted(self) -> bool:
        return self._signal_generation_halted

    @property
    def halt_reason(self) -> SafetyEvent | None:
        return self._halt_reason

    @property
    def active_warnings(self) -> tuple[SafetyEvent, ...]:
        return self._active_warnings

    @property
    def last_evaluated_at(self) -> datetime | None:
        return self._last_evaluated_at

    def halt(self, event: SafetyEvent) -> None:
        """Sinyal üretimini durdurur.

        Illegal transition'lar reddedilir:
        - severity != HALT
        - sistem zaten halted (önce resume() çağrılmalı)
        """
        if event.severity != SafetySeverity.HALT:
            raise ValueError(
                f"halt() yalnızca SafetySeverity.HALT taşıyan event ile çağrılabilir, "
                f"alınan: {event.severity}"
            )
        if self._signal_generation_halted:
            raise ValueError(
                "illegal state transition: sistem zaten halted; önce resume() çağrılmalı"
            )
        # Bu iki atama HER ZAMAN birlikte yapılır — invariant'ın bozulabileceği
        # bir ara durum (yalnızca biri set edilmiş) hiçbir zaman gözlemlenemez.
        self._signal_generation_halted = True
        self._halt_reason = event

    def resume(self) -> None:
        """Durdurma durumunu kaldırır.

        Illegal transition: sistem zaten halted DEĞİLKEN resume() çağrısı.
        """
        if not self._signal_generation_halted:
            raise ValueError("illegal state transition: sistem zaten halted değil")
        self._signal_generation_halted = False
        self._halt_reason = None

    def add_warning(self, event: SafetyEvent) -> None:
        """Bir uyarı ekler.

        Dedup politikası: aynı dedup_key'e (reason_code, component, symbol)
        sahip bir warning zaten aktifse, ESKİSİ YENİSİYLE DEĞİŞTİRİLİR
        (upsert) — biriktirerek sınırsız büyümez.
        """
        if event.severity == SafetySeverity.HALT:
            raise ValueError("HALT severity add_warning() ile değil halt() ile işlenmeli")
        remaining = tuple(w for w in self._active_warnings if w.dedup_key != event.dedup_key)
        self._active_warnings = (*remaining, event)

    def clear_warnings(self) -> None:
        self._active_warnings = ()

    def mark_evaluated(self, now: datetime) -> None:
        """SafetyOrchestrator'ın en son ne zaman değerlendirme yaptığını kaydeder.

        `last_evaluated_at` YALNIZCA bu metod üzerinden, UTC-aware bir
        datetime ile güncellenebilir — doğrudan naive bir datetime ile
        mutate edilemez (property'nin setter'ı yoktur).
        """
        require_utc_aware(now, "now")
        self._last_evaluated_at = now


=== FILE: crypto_signal_engine/consensus/__init__.py ===


=== FILE: crypto_signal_engine/consensus/engine.py ===
"""
ConsensusEngine.

Kural (Faz 4 Bölüm 25): `combine()` yalnızca `AgentEvidence` koleksiyonu
alır; `RegimeContext`'i skorlama/agreement hesaplamasına DAHİL ETMEZ.

BİLİNÇLİ, RAPORLANMIŞ SÖZLEŞME SAPMASI (bkz. PHASE4_QUANT_AGENT_ENGINE.md
"Kabul edilen sözleşme sapması" bölümü): Phase 1'in ZATEN KABUL EDİLMİŞ
`ConsensusResult` domain modeli, `regime: RegimeContext` alanını ZORUNLU
(non-optional) olarak taşır. Bu, Faz 4 talimatının "ConsensusEngine
RegimeContext'i asla parametre olarak almamalı" ifadesiyle DOĞRUDAN
ÇELİŞİR. "Gerçek kod kazanır" ilkesi (ve "accepted prior-phase modelini
sırf daha temiz göstermek için değiştirme" kuralı) gereği, `ConsensusResult`
DEĞİŞTİRİLMEMİŞTİR; bunun yerine `combine()`, `regime`'i keyword-only bir
parametre olarak alır ve YALNIZCA `ConsensusResult`'ın zaten var olan
zorunlu alanını doldurmak için kullanır — raw_score/agreement
hesaplamasının HİÇBİR ADIMINDA regime kullanılmaz (talimatın "do not create
a regime-adjusted consensus score" ruhu tam olarak korunur).
"""

from __future__ import annotations

from collections.abc import Sequence

from crypto_signal_engine.domain.consensus import ConsensusResult, RegimeContext, compute_agreement
from crypto_signal_engine.domain.enums import AgentName
from crypto_signal_engine.domain.models import AgentEvidence
from crypto_signal_engine.errors import ConsensusError

AGENT_WEIGHTS: dict[AgentName, float] = {
    AgentName.QUANT: 0.35,
    AgentName.MARKET_STRUCTURE: 0.30,
    AgentName.ORDER_BOOK: 0.15,
    AgentName.REGIME: 0.20,
}
assert abs(sum(AGENT_WEIGHTS.values()) - 1.0) < 1e-9

_CANONICAL_AGENT_ORDER = [AgentName.QUANT, AgentName.MARKET_STRUCTURE, AgentName.ORDER_BOOK, AgentName.REGIME]


class ConsensusEngine:
    """Bağımsız `AgentEvidence`'ları sabit ağırlıklarla `ConsensusResult`'a birleştirir."""

    def combine(self, evidence: Sequence[AgentEvidence], *, regime: RegimeContext) -> ConsensusResult:
        if not isinstance(evidence, Sequence) or isinstance(evidence, (str, bytes)):
            raise ConsensusError(f"evidence bir Sequence[AgentEvidence] olmalı, alınan: {type(evidence)!r}")
        if len(evidence) == 0:
            raise ConsensusError("evidence boş olamaz")
        for e in evidence:
            if not isinstance(e, AgentEvidence):
                raise ConsensusError(f"evidence yalnızca AgentEvidence içermeli, alınan: {type(e)!r}")

        unknown_agents = [e.agent for e in evidence if e.agent not in AGENT_WEIGHTS]
        if unknown_agents:
            raise ConsensusError(f"bilinmeyen agent(lar) için ağırlık tanımlı değil: {unknown_agents}")

        raw_score = sum(AGENT_WEIGHTS[e.agent] * e.score for e in evidence)
        raw_score = max(-1.0, min(1.0, raw_score))

        agreement = compute_agreement(tuple(e.score for e in evidence))

        sorted_evidence = sorted(evidence, key=lambda e: _CANONICAL_AGENT_ORDER.index(e.agent))

        try:
            return ConsensusResult(
                symbol=evidence[0].symbol,
                context_id=evidence[0].context_id,
                raw_score=raw_score,
                agreement=agreement,
                regime=regime,
                contributing_evidence=tuple(sorted_evidence),
            )
        except ValueError as exc:
            raise ConsensusError(str(exc)) from exc


=== FILE: crypto_signal_engine/consensus/risk.py ===
"""
RiskOverlay.

Kural (Faz 4 Bölüm 26-29): strictly post-consensus. Yalnızca
`ConsensusResult` ve `RegimeContext` alır (`RegimeAgentOutput`, ham
`AgentEvidence`, `AgentContext`, veya agent nesneleri ASLA doğrudan
alınmaz — `consensus.contributing_evidence` üzerinden dolaylı olarak
zaten erişilebilir). Yalnızca `confidence_multiplier`'ı AŞAĞI çekebilir;
`raw_score`, işaret, veya yön ASLA değiştirilmez (bu zaten mümkün değildir
— `RiskAssessment` `ConsensusResult`'ı hiçbir şekilde mutate etmez, yalnızca
YENİ bir `RiskAssessment` nesnesi üretir).

Provenance notu (Bölüm 6): `ConsensusResult` ile `RegimeContext`'in aynı
değerlendirmeye ait olduğu invariant'ı ORKESTRATÖR seviyesinde garanti
edilir (bkz. `signal_engine.py`). `RegimeContext` bu garantiyi ispatlamak
için provenance alanları (context_id/symbol/timestamp) taşıyacak şekilde
YENİDEN TASARLANMAMIŞTIR — mevcut kabul edilmiş model korunmuştur.
"""

from __future__ import annotations

from crypto_signal_engine.agents.base import clamp
from crypto_signal_engine.domain._validation import require_finite
from crypto_signal_engine.domain.consensus import ConsensusResult, RegimeContext, RiskAssessment
from crypto_signal_engine.domain.enums import LiquidityRegime, RiskLevel, VolatilityRegime

_AGREEMENT_NO_PENALTY_MIN = 0.75
_AGREEMENT_MILD_MIN = 0.50
_AGREEMENT_MATERIAL_MIN = 0.30

_AGREEMENT_MULT_NONE = 1.0
_AGREEMENT_MULT_MILD = 0.85
_AGREEMENT_MULT_MATERIAL = 0.60
_AGREEMENT_MULT_SEVERE = 0.35

_VOLATILITY_MULT = {
    VolatilityRegime.LOW: 1.0,
    VolatilityRegime.NORMAL: 1.0,
    VolatilityRegime.HIGH: 0.75,
    VolatilityRegime.EXTREME: 0.50,
    VolatilityRegime.UNKNOWN: 0.75,
}

_LIQUIDITY_MULT = {
    LiquidityRegime.NORMAL: 1.0,
    LiquidityRegime.THIN: 0.80,
    LiquidityRegime.STRESSED: 0.55,
    LiquidityRegime.UNKNOWN: 0.80,
}

_CONTRADICTION_SEVERE_THRESHOLD = 0.50
_CONTRADICTION_MILD_THRESHOLD = 0.25
_CONTRADICTION_MULT_SEVERE = 0.60
_CONTRADICTION_MULT_MILD = 0.85
_CONTRADICTION_MULT_NONE = 1.0

_AGREEMENT_LEVEL = {
    "none": RiskLevel.LOW, "mild": RiskLevel.MEDIUM, "material": RiskLevel.HIGH, "severe": RiskLevel.EXTREME,
}
_VOLATILITY_LEVEL = {
    VolatilityRegime.LOW: RiskLevel.LOW, VolatilityRegime.NORMAL: RiskLevel.LOW,
    VolatilityRegime.HIGH: RiskLevel.HIGH, VolatilityRegime.EXTREME: RiskLevel.EXTREME,
    VolatilityRegime.UNKNOWN: RiskLevel.MEDIUM,
}
_LIQUIDITY_LEVEL = {
    LiquidityRegime.NORMAL: RiskLevel.LOW, LiquidityRegime.THIN: RiskLevel.MEDIUM,
    LiquidityRegime.STRESSED: RiskLevel.HIGH, LiquidityRegime.UNKNOWN: RiskLevel.MEDIUM,
}
_CONTRADICTION_LEVEL = {"none": RiskLevel.LOW, "mild": RiskLevel.MEDIUM, "severe": RiskLevel.HIGH}

_RISK_LEVEL_RANK = {RiskLevel.LOW: 0, RiskLevel.MEDIUM: 1, RiskLevel.HIGH: 2, RiskLevel.EXTREME: 3}


def _agreement_band(agreement: float) -> str:
    if agreement >= _AGREEMENT_NO_PENALTY_MIN:
        return "none"
    if agreement >= _AGREEMENT_MILD_MIN:
        return "mild"
    if agreement >= _AGREEMENT_MATERIAL_MIN:
        return "material"
    return "severe"


def _contradiction_band(consensus: ConsensusResult) -> tuple[str, float]:
    scores = [e.score for e in consensus.contributing_evidence]
    if consensus.raw_score == 0.0:
        opposition = sum(abs(s) for s in scores if s != 0.0)
    else:
        sign = 1.0 if consensus.raw_score > 0 else -1.0
        opposition = sum(abs(s) for s in scores if (s * sign) < 0.0)
    total_weight = sum(abs(s) for s in scores) or 1.0
    magnitude = opposition / total_weight
    if magnitude >= _CONTRADICTION_SEVERE_THRESHOLD:
        return "severe", magnitude
    if magnitude >= _CONTRADICTION_MILD_THRESHOLD:
        return "mild", magnitude
    return "none", magnitude


class RiskOverlay:
    """`ConsensusResult` + `RegimeContext` -> `RiskAssessment` (strictly post-consensus)."""

    def assess(self, consensus: ConsensusResult, regime: RegimeContext) -> RiskAssessment:
        if not isinstance(consensus, ConsensusResult):
            raise TypeError(f"consensus bir ConsensusResult olmalı, alınan: {type(consensus)!r}")
        if not isinstance(regime, RegimeContext):
            raise TypeError(f"regime bir RegimeContext olmalı, alınan: {type(regime)!r}")

        agreement_band = _agreement_band(consensus.agreement)
        agreement_mult = {
            "none": _AGREEMENT_MULT_NONE, "mild": _AGREEMENT_MULT_MILD,
            "material": _AGREEMENT_MULT_MATERIAL, "severe": _AGREEMENT_MULT_SEVERE,
        }[agreement_band]

        volatility_mult = _VOLATILITY_MULT[regime.volatility]
        liquidity_mult = _LIQUIDITY_MULT[regime.liquidity]

        contradiction_band, opposition_magnitude = _contradiction_band(consensus)
        contradiction_mult = {
            "none": _CONTRADICTION_MULT_NONE, "mild": _CONTRADICTION_MULT_MILD, "severe": _CONTRADICTION_MULT_SEVERE,
        }[contradiction_band]

        confidence_multiplier = clamp(
            agreement_mult * volatility_mult * liquidity_mult * contradiction_mult, 0.0, 1.0
        )
        confidence_multiplier = require_finite(confidence_multiplier, "confidence_multiplier")

        candidates = [
            _AGREEMENT_LEVEL[agreement_band],
            _VOLATILITY_LEVEL[regime.volatility],
            _LIQUIDITY_LEVEL[regime.liquidity],
            _CONTRADICTION_LEVEL[contradiction_band],
        ]
        risk_level = max(candidates, key=lambda level: _RISK_LEVEL_RANK[level])

        rationale = (
            f"agreement={consensus.agreement:.2f} ({agreement_band}, x{agreement_mult:.2f}); "
            f"volatility={regime.volatility.value} (x{volatility_mult:.2f}); "
            f"liquidity={regime.liquidity.value} (x{liquidity_mult:.2f}); "
            f"contradiction={contradiction_band} (muhalefet oranı {opposition_magnitude:.2f}, "
            f"x{contradiction_mult:.2f}) -> confidence_multiplier={confidence_multiplier:.3f}, "
            f"risk_level={risk_level.value}."
        )

        return RiskAssessment(
            confidence_multiplier=confidence_multiplier,
            risk_level=risk_level,
            rationale=rationale,
            contradicting_metrics={
                "agreement": consensus.agreement,
                "agreement_multiplier": agreement_mult,
                "volatility_multiplier": volatility_mult,
                "liquidity_multiplier": liquidity_mult,
                "opposition_magnitude": opposition_magnitude,
                "contradiction_multiplier": contradiction_mult,
            },
        )


