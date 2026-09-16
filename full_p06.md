<!-- full_p06.md — Part 6/7 — 40 files -->
<!-- Contents of this part: -->
<!--   - tests/test_execution_lifecycle_migration.py (29564 bytes) -->
<!--   - tests/test_execution_lifecycle_replay_sanity.py (6198 bytes) -->
<!--   - tests/test_execution_lifecycle_store.py (32446 bytes) -->
<!--   - tests/test_execution_models.py (6689 bytes) -->
<!--   - tests/test_execution_reconciliation_models.py (8760 bytes) -->
<!--   - tests/test_execution_reconciliation_service.py (40184 bytes) -->
<!--   - tests/test_execution_reconciliation_store.py (5258 bytes) -->
<!--   - tests/test_execution_signer.py (2511 bytes) -->
<!--   - tests/test_execution_testnet_client.py (27155 bytes) -->
<!--   - tests/test_feature_domain.py (6583 bytes) -->
<!--   - tests/test_feature_engine.py (13006 bytes) -->
<!--   - tests/test_feature_registry.py (4331 bytes) -->
<!--   - tests/test_feature_state.py (5440 bytes) -->
<!--   - tests/test_gap_and_atomic_checkpoint_remediation.py (33346 bytes) -->
<!--   - tests/test_immutability.py (3623 bytes) -->
<!--   - tests/test_lifecycle_concurrency.py (6814 bytes) -->
<!--   - tests/test_lifecycle_runtime.py (12108 bytes) -->
<!--   - tests/test_local_readiness_check.py (1898 bytes) -->
<!--   - tests/test_ops_admin.py (5046 bytes) -->
<!--   - tests/test_ops_config.py (11577 bytes) -->
<!--   - tests/test_ops_dashboard.py (34414 bytes) -->
<!--   - tests/test_ops_event_log.py (18052 bytes) -->
<!--   - tests/test_ops_health_snapshot.py (3110 bytes) -->
<!--   - tests/test_ops_lock.py (2867 bytes) -->
<!--   - tests/test_ops_logging.py (1876 bytes) -->
<!--   - tests/test_ops_no_hardcoded_paths.py (1718 bytes) -->
<!--   - tests/test_ops_notifier.py (5034 bytes) -->
<!--   - tests/test_ops_systemd_backup_template.py (2965 bytes) -->
<!--   - tests/test_ops_systemd_notify.py (2656 bytes) -->
<!--   - tests/test_ops_systemd_template.py (2825 bytes) -->
<!--   - tests/test_orchestrator.py (4508 bytes) -->
<!--   - tests/test_orderbook_features.py (4012 bytes) -->
<!--   - tests/test_package_import.py (8247 bytes) -->
<!--   - tests/test_paper_trading_engine.py (26125 bytes) -->
<!--   - tests/test_paper_trading_models.py (4156 bytes) -->
<!--   - tests/test_paper_trading_notional_override.py (19746 bytes) -->
<!--   - tests/test_persistence_paper_state_store.py (10178 bytes) -->
<!--   - tests/test_persistence_recovery.py (31492 bytes) -->
<!--   - tests/test_persistence_serialization.py (3159 bytes) -->
<!--   - tests/test_phase12_local_production_readiness.py (29509 bytes) -->

=== FILE: tests/test_execution_lifecycle_migration.py ===
"""
Autonomous Testnet trading lifecycle — legacy position migration tests
(Phase 4/19), covering the two-stage split:

- STAGE 1 (`reconstruct_legacy_ownership`/`reconstruct_all_legacy_ownership`)
  runs BEFORE market bootstrap: authoritative ownership reconstruction
  ONLY, from real FILLED fills (never PAPER/wallet), creating ZERO
  exchange orders and reading ZERO market data. Persists
  `PositionLifecycleState.RECOVERED`.
- STAGE 2 (`finalize_legacy_lifecycle_init`/`finalize_all_legacy_lifecycle_init`)
  runs AFTER market bootstrap: initializes market-dependent fields
  (`high_water`/`initial_protective_stop`/`effective_stop`/`take_profit`)
  for an ALREADY-`RECOVERED` position using the first fresh post-bootstrap
  M5 price/ATR, transitioning it to `LONG`. A no-op for anything not
  exactly `RECOVERED` (never remigrates/reinitializes an already-LONG
  position on a normal restart).

Fully offline: `FakeTestnetHttpClient` + `FixedClock` + temp SQLite + a
minimal fake "coordinator" double exposing only `_candle_windows`/
`_feature_engine` (stage 2 needs no other coordinator behavior)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, ExitPolicyConfig, PositionLifecycleState
from crypto_signal_engine.execution.lifecycle_migration import (
    finalize_all_legacy_lifecycle_init,
    finalize_legacy_lifecycle_init,
    reconstruct_all_legacy_ownership,
    reconstruct_legacy_ownership,
)
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import bridge_context_id
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response
from tests.runtime_fakes import make_candle, make_candle_series_ending_at

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"
SYMBOL = "BTCUSDT"
WARMUP = 20


def _exchange_info() -> dict:
    return {
        "symbols": [
            {
                "symbol": SYMBOL, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int = 1) -> dict:
    return {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": "FILLED", "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }


def _setup(get_responses=None, post_responses=None, *, tmp_path: Path):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    return service, exec_store, lifecycle_store, http, client


def _seed_real_bridge_buy(service: ExecutionReconciliationService, *, quantity: float = 0.002) -> None:
    intent = OrderIntent(
        symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id=bridge_context_id(SYMBOL, "OPEN", "ctx-legacy"), timestamp=NOW, quantity=quantity,
    )
    run_async(service.submit(intent))


class _FakeCandleWindow:
    def __init__(self, candles) -> None:
        self._candles = tuple(candles)

    def history(self):
        return self._candles


class _FakeFeatureEngine:
    def __init__(self, snapshots: dict) -> None:
        self._snapshots = snapshots

    def latest_snapshot(self, symbol: str, timeframe: Timeframe):
        return self._snapshots.get((symbol, timeframe))


class _Snapshot:
    def __init__(self, values: dict) -> None:
        self.values = values


class _FakeCoordinator:
    """Minimal double — stage 2 only ever touches `_candle_windows` and
    `_feature_engine`, exactly like the real `RuntimeCoordinator`."""

    def __init__(self, *, m5_candles=(), atr: float | None = None) -> None:
        self._candle_windows = {(SYMBOL, Timeframe.M5): _FakeCandleWindow(m5_candles)} if m5_candles else {}
        snapshots = {}
        if atr is not None:
            snapshots[(SYMBOL, Timeframe.M5)] = _Snapshot({"ATR_14": atr})
        self._feature_engine = _FakeFeatureEngine(snapshots)


def _recovered_record(**overrides) -> BridgePositionRecord:
    defaults = dict(
        symbol=SYMBOL, state=PositionLifecycleState.RECOVERED, gross_entry_vwap=100.0,
        net_owned_base_quantity=1.0, entry_timestamp=NOW, entry_client_order_id="csl-entry-1",
        migrated_existing_position=True, updated_at=NOW,
    )
    defaults.update(overrides)
    return BridgePositionRecord(**defaults)  # type: ignore[arg-type]


from pathlib import Path  # noqa: E402 - kept near usage for readability of the fixtures above


class TestStage1ReconstructOwnershipCreatesNoOrdersReadsNoMarketData:
    def test_creates_zero_exchange_orders_and_reads_zero_market_data(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response([{"symbol": SYMBOL, "id": 1, "orderId": 1, "price": "50000.0", "qty": "0.002",
                             "commission": "0.000002", "commissionAsset": "BTC"}]),
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        service, exec_store, lifecycle_store, http, client = _setup(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
        )
        _seed_real_bridge_buy(service)
        posts_before = len(http.post_calls)

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store,
                client=client, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record is not None
        assert len(http.post_calls) == posts_before  # zero NEW orders (only the seed BUY exists)
        # The only GET calls made are exchangeInfo (seed's own submit) + myTrades
        # (stage 1's backfill) — never klines/exchangeInfo for a price/ATR lookup.
        get_urls = [url for url, _, _ in http.get_calls]
        assert all("myTrades" in u or "exchangeInfo" in u for u in get_urls)

    def test_persists_recovered_state_not_long(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response([{"symbol": SYMBOL, "id": 1, "orderId": 1, "price": "50000.0", "qty": "0.002",
                             "commission": "0.000002", "commissionAsset": "BTC"}]),
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        service, exec_store, lifecycle_store, http, client = _setup(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
        )
        _seed_real_bridge_buy(service)

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store,
                client=client, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.state is PositionLifecycleState.RECOVERED
        assert record.gross_entry_vwap == pytest.approx(50000.0)
        assert record.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        assert record.migrated_existing_position is True
        # Market-dependent fields are NOT fabricated at this stage.
        assert record.initial_protective_stop is None
        assert record.high_water is None
        assert record.effective_stop is None
        assert record.take_profit is None

        persisted = lifecycle_store.load_position(SYMBOL)
        assert persisted == record

    def test_idempotent_second_call_is_a_no_op(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response([{"symbol": SYMBOL, "id": 1, "orderId": 1, "price": "50000.0", "qty": "0.002",
                             "commission": "0.0", "commissionAsset": "USDT"}]),
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        service, exec_store, lifecycle_store, http, client = _setup(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
        )
        _seed_real_bridge_buy(service)

        async def scenario():
            first = await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )
            second = await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )
            return first, second

        first, second = run_async(scenario())
        assert first is not None
        assert second is None

    def test_already_long_position_is_never_touched(self, tmp_path: Path) -> None:
        """Requirement #7 (stage-1 half): a symbol that already has a
        fresh, fully-initialized LONG position must never be reconstructed
        again."""
        service, exec_store, lifecycle_store, http, client = _setup(tmp_path=tmp_path)
        long_record = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        )
        lifecycle_store.save_position(long_record)

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None
        assert lifecycle_store.load_position(SYMBOL) == long_record

    def test_flat_symbol_is_not_reconstructed(self, tmp_path: Path) -> None:
        service, exec_store, lifecycle_store, http, client = _setup(tmp_path=tmp_path)

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None
        assert lifecycle_store.load_position(SYMBOL) is None

    def test_ambiguous_symbol_is_never_reconstructed(self, tmp_path: Path) -> None:
        from crypto_signal_engine.execution.errors import ExecutionTransportError

        service, exec_store, lifecycle_store, http, client = _setup(
            get_responses=[json_response(_exchange_info()), ExecutionTransportError("query also unresolved")],
            post_responses=[ExecutionTransportError("connection reset")],
            tmp_path=tmp_path,
        )
        with pytest.raises(ExecutionTransportError):
            _seed_real_bridge_buy(service)

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None
        assert lifecycle_store.load_position(SYMBOL) is None

    def test_manual_lab_position_never_reconstructed_as_bridge_inventory(self, tmp_path: Path) -> None:
        post_responses = [json_response(_order_payload(side="BUY", qty="0.001", quote_qty="50.0"))]
        service, exec_store, lifecycle_store, http, client = _setup(
            get_responses=[json_response(_exchange_info())], post_responses=post_responses, tmp_path=tmp_path,
        )
        lab_intent = OrderIntent(
            symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
            context_id="lab-abc123", timestamp=NOW, quantity=0.001,
        )
        run_async(service.submit(lab_intent))

        async def scenario():
            return await reconstruct_legacy_ownership(
                SYMBOL, execution_store=exec_store, lifecycle_store=lifecycle_store, client=client, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None
        assert lifecycle_store.load_position(SYMBOL) is None


class TestStage2FinalizeInitOnlyActsOnRecovered:
    def test_finalizes_high_water_as_max_of_entry_and_first_fresh_price(self, tmp_path: Path) -> None:
        """Requirement #4 — high_water = max(authoritative_entry_fill_price,
        first_fresh_post_recovery_live_market_price), never fabricated."""
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        m5_candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, NOW, WARMUP, base_price=105.0)
        coordinator = _FakeCoordinator(m5_candles=m5_candles, atr=5.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.state is PositionLifecycleState.LONG
        fresh_price = m5_candles[-1].close
        assert fresh_price > 100.0  # sanity: the series trends above entry
        assert record.high_water == pytest.approx(max(100.0, fresh_price))

    def test_high_water_falls_back_to_entry_when_fresh_price_below_entry(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=1000.0))
        m5_candles = make_candle_series_ending_at(SYMBOL, Timeframe.M5, NOW, WARMUP, base_price=10.0)
        coordinator = _FakeCoordinator(m5_candles=m5_candles, atr=1.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.high_water == pytest.approx(1000.0)  # never fabricated below the real entry price

    def test_no_fresh_price_available_falls_back_to_entry_never_fabricated(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=(), atr=5.0)  # bootstrap produced nothing for this symbol

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.high_water == pytest.approx(100.0)

    def test_stop_and_target_use_first_valid_post_recovery_atr(self, tmp_path: Path) -> None:
        """Requirement #5 — initial stop/take-profit use the first valid
        post-recovery volatility snapshot, derived from `gross_entry_vwap`
        exactly like a fresh entry."""
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.initial_protective_stop == pytest.approx(100.0 - 2.0 * 5.0)
        assert record.take_profit == pytest.approx(100.0 + 4.0 * 5.0)
        assert record.effective_stop == record.initial_protective_stop

    def test_no_atr_available_uses_fallback_never_fabricated_atr(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=[], atr=None)  # no ATR snapshot yet

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.initial_protective_stop == pytest.approx(98.0)
        assert record.take_profit == pytest.approx(104.0)

    def test_non_recovered_state_is_a_no_op_never_reinitialized(self, tmp_path: Path) -> None:
        """Requirement #7 — an already-LONG position on a normal restart
        is untouched by stage 2 (never remigrated/reinitialized)."""
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        long_record = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=150.0,
            effective_stop=95.0, take_profit=120.0, trailing_active=True, entry_timestamp=NOW, updated_at=NOW,
        )
        lifecycle_store.save_position(long_record)
        coordinator = _FakeCoordinator(m5_candles=[make_candle(SYMBOL, Timeframe.M5, NOW)], atr=999.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None
        assert lifecycle_store.load_position(SYMBOL) == long_record  # byte-for-byte unchanged

    def test_missing_record_is_a_no_op(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None

    def test_flat_record_is_a_no_op(self, tmp_path: Path) -> None:
        from crypto_signal_engine.execution.lifecycle import flat_record

        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(flat_record(SYMBOL, now=NOW))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        assert run_async(scenario()) is None

    def test_finalization_embeds_policy_fields_and_version_id(self, tmp_path: Path) -> None:
        """Adaptive Intelligence v1, step 2 — stage-2 finalization IS this
        position's entry moment for policy-pinning purposes (it is the
        first time stop/target/trailing become active), so `exit_policy`'s
        five fields and `policy_version_id` must be embedded inline here
        exactly like a fresh `on_entry_filled()` entry."""
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)
        policy = ExitPolicyConfig(
            stop_atr_multiple=1.5, take_profit_atr_multiple=3.0, trailing_activation_atr_multiple=1.0,
            trailing_distance_atr_multiple=1.0, max_hold_hours=12.0,
        )

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
                exit_policy=policy, policy_version_id="policy-v9",
            )

        record = run_async(scenario())
        assert record.policy_version_id == "policy-v9"
        assert record.exit_policy_stop_atr_multiple == 1.5
        assert record.exit_policy_take_profit_atr_multiple == 3.0
        assert record.exit_policy_trailing_activation_atr_multiple == 1.0
        assert record.exit_policy_trailing_distance_atr_multiple == 1.0
        assert record.exit_policy_max_hold_hours == 12.0
        # Stop/target math itself used this SAME policy, not the bare default.
        assert record.initial_protective_stop == pytest.approx(100.0 - 1.5 * 5.0)
        assert record.take_profit == pytest.approx(100.0 + 3.0 * 5.0)

    def test_finalization_without_policy_version_id_leaves_it_none(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_legacy_lifecycle_init(
                SYMBOL, lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        record = run_async(scenario())
        assert record.policy_version_id is None
        default = ExitPolicyConfig()
        assert record.exit_policy_max_hold_hours == default.max_hold_hours


class TestDurabilityAndOrdering:
    def test_recovered_state_persists_durably_across_a_fresh_store_connection(self, tmp_path: Path) -> None:
        """Requirement #6 — migrated (stage-1) state is durable BEFORE any
        automated lifecycle exit could ever act on it: a fresh `LifecycleStore`
        connection (simulating a crash between stage 1 and stage 2) sees the
        exact same RECOVERED record."""
        path = tmp_path / "lifecycle.db"
        store1 = LifecycleStore(path)
        store1.save_position(_recovered_record())
        store1.close()

        store2 = LifecycleStore(path)
        loaded = store2.load_position(SYMBOL)
        assert loaded is not None
        assert loaded.state is PositionLifecycleState.RECOVERED
        store2.close()

    def test_stage1_requires_no_coordinator_or_market_data_dependency(self, tmp_path: Path) -> None:
        """Requirement #1 — stage 1 can run (and completes) with ONLY
        execution-truth inputs, structurally proving it does not depend on
        anything market bootstrap would produce (no coordinator parameter
        exists on `reconstruct_legacy_ownership` at all)."""
        import inspect

        params = inspect.signature(reconstruct_legacy_ownership).parameters
        assert "coordinator" not in params
        assert "rest_client" not in params  # no ATR/price REST dependency either

    def test_recovered_position_blocks_new_bridge_entry(self, tmp_path: Path) -> None:
        """Requirement #3 (entry half) — no new economic order (a BUY) can
        occur for a symbol stuck in RECOVERED (stage 1 done, stage 2 not
        yet run)."""
        from crypto_signal_engine.domain.enums import RiskLevel
        from crypto_signal_engine.domain.models import Signal
        from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
        from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
        from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
        from crypto_signal_engine.runtime.models import RuntimeCycleResult

        service, exec_store, lifecycle_store, http, client = _setup(tmp_path=tmp_path)
        lifecycle_store.save_position(_recovered_record())
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        bridge = SignalTestnetBridge(
            execution_service=service, execution_store=exec_store, notional_usdt=10.0, clock=FixedClock(NOW),
            lifecycle_manager=manager, atr_provider=lambda _s: 5.0, all_symbols_provider=lambda: (SYMBOL,),
        )
        bridge.mark_operational(True, "test")
        signal = Signal(
            symbol=SYMBOL, timestamp=NOW, context_id="ctx-1", score=0.9, confidence=0.9,
            risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
            supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test",
        )
        paper_result = PaperTradingResult(
            symbol=SYMBOL,
            position=PaperPosition(symbol=SYMBOL, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0, realized_pnl=0.0, updated_at=NOW),
            orders=(), fills=(), idempotent_replay=False,
        )
        cycle_result = RuntimeCycleResult(symbol=SYMBOL, evaluated=True, signal=signal, paper_result=paper_result, generated_at=NOW)

        run_async(bridge.on_cycle_result(cycle_result))

        assert http.post_calls == []  # no BUY submitted
        assert lifecycle_store.load_position(SYMBOL).state is PositionLifecycleState.RECOVERED  # unchanged

    def test_recovered_position_never_evaluated_for_exit(self, tmp_path: Path) -> None:
        """Requirement #3 (exit half) — the M1 lifecycle evaluator never
        acts on a RECOVERED position (no stop/target exist yet to evaluate
        against)."""
        from crypto_signal_engine.execution.lifecycle import Candle
        from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager

        service, exec_store, lifecycle_store, http, client = _setup(tmp_path=tmp_path)
        lifecycle_store.save_position(_recovered_record())
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        candle = Candle(open=1, high=1, low=1, close=1, close_time=NOW)

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        result = run_async(scenario())
        assert result is None
        assert http.post_calls == []
        assert lifecycle_store.load_position(SYMBOL).state is PositionLifecycleState.RECOVERED


class TestOrchestrators:
    def test_reconstruct_all_isolates_one_symbol_failure(self, tmp_path: Path) -> None:
        service, exec_store, lifecycle_store, http, client = _setup(tmp_path=tmp_path)

        async def scenario():
            return await reconstruct_all_legacy_ownership(
                ("BTCUSDT", "ETHUSDT"), execution_store=exec_store, lifecycle_store=lifecycle_store,
                client=client, clock=FixedClock(NOW),
            )

        reconstructed = run_async(scenario())
        assert reconstructed == ()  # both FLAT -- nothing to reconstruct, no exception raised

    def test_finalize_all_isolates_one_symbol_failure_and_only_touches_recovered(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(symbol="BTCUSDT"))
        lifecycle_store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_all_legacy_lifecycle_init(
                ("BTCUSDT", "ETHUSDT"), lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
            )

        finalized = run_async(scenario())
        assert finalized == ("BTCUSDT",)  # ETHUSDT already LONG -- untouched, not "finalized" again
        assert lifecycle_store.load_position("BTCUSDT").state is PositionLifecycleState.LONG

    def test_finalize_all_threads_policy_version_id_through_to_each_symbol(self, tmp_path: Path) -> None:
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        lifecycle_store.save_position(_recovered_record(symbol="BTCUSDT", gross_entry_vwap=100.0))
        coordinator = _FakeCoordinator(m5_candles=[], atr=5.0)

        async def scenario():
            return await finalize_all_legacy_lifecycle_init(
                ("BTCUSDT",), lifecycle_store=lifecycle_store, coordinator=coordinator, clock=FixedClock(NOW),
                policy_version_id="policy-v9",
            )

        finalized = run_async(scenario())
        assert finalized == ("BTCUSDT",)
        assert lifecycle_store.load_position("BTCUSDT").policy_version_id == "policy-v9"


=== FILE: tests/test_execution_lifecycle_replay_sanity.py ===
"""
Autonomous Testnet trading lifecycle Phase 20 —
`crypto_signal_engine.execution.lifecycle_replay_sanity` tests. Fully offline (`FakeHistoricalCandleSource`, deterministic). A
`ReplayResult` is constructed directly with fabricated (but structurally
real) `cycle_results` to precisely control which FLAT->LONG entries exist,
so the exit-simulation mechanics (stop/target detection, no-lookahead,
still-open handling) can be tested deterministically without depending on
whether a full Quant/Consensus pass happens to fire a signal from
synthetic sine-wave data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.execution.lifecycle import ExitPolicyConfig
from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from crypto_signal_engine.execution.lifecycle_replay_sanity import simulate_lifecycle_exits
from research.replay import OrderBookProvenance, ReplayResult
from tests.conftest import run_async
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

SYMBOL = "BTCUSDT"


def _signal(ts: datetime, context_id: str) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=ts, context_id=context_id, score=0.9, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test",
    )


def _cycle_result(ts: datetime, side: PositionSide, *, entry_price: float = 100.0, context_id: str = "ctx") -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=SYMBOL,
        position=PaperPosition(
            symbol=SYMBOL, side=side, quantity=1.0 if side is not PositionSide.FLAT else 0.0,
            average_entry_price=entry_price if side is not PositionSide.FLAT else 0.0,
            realized_pnl=0.0, updated_at=ts,
        ),
        orders=(), fills=(), idempotent_replay=False,
    )
    return RuntimeCycleResult(symbol=SYMBOL, evaluated=True, signal=_signal(ts, context_id), paper_result=paper_result, generated_at=ts)


def _replay_result(cycle_results: tuple[RuntimeCycleResult, ...], *, start: datetime, end: datetime) -> ReplayResult:
    return ReplayResult(
        symbols=(SYMBOL,), start=start, end=end, order_book_provenance=OrderBookProvenance.SYNTHETIC,
        data_quality_reports=(), cycle_results=cycle_results, skipped_evaluation_count=0,
        final_positions=(), limitations=(),
    )


class TestEntryDetection:
    def test_flat_to_long_transition_is_the_only_entry_trigger(self) -> None:
        t0 = EPOCH
        t1 = EPOCH + timedelta(minutes=5)
        t2 = EPOCH + timedelta(minutes=10)
        results = (
            _cycle_result(t0, PositionSide.FLAT),
            _cycle_result(t1, PositionSide.LONG, entry_price=100.0),
            _cycle_result(t2, PositionSide.LONG, entry_price=100.0),  # still LONG -- not a new entry
        )
        replay = _replay_result(results, start=t0, end=t2 + timedelta(days=1))
        source = FakeHistoricalCandleSource()

        async def scenario():
            return await simulate_lifecycle_exits(
                replay, candle_source=source, exit_policy=ExitPolicyConfig(), exit_window=timedelta(hours=6),
            )

        report = run_async(scenario())
        assert report.trade_count + report.open_at_end_count == 1  # exactly ONE entry detected


class TestExitDetection:
    def test_stop_or_target_eventually_fires_or_trade_stays_open(self) -> None:
        t1 = EPOCH + timedelta(minutes=5)
        results = (_cycle_result(EPOCH, PositionSide.FLAT), _cycle_result(t1, PositionSide.LONG, entry_price=100.0))
        replay = _replay_result(results, start=EPOCH, end=t1 + timedelta(days=2))
        source = FakeHistoricalCandleSource()

        async def scenario():
            return await simulate_lifecycle_exits(
                replay, candle_source=source, exit_policy=ExitPolicyConfig(), exit_window=timedelta(hours=48),
            )

        report = run_async(scenario())
        assert report.trade_count + report.open_at_end_count == 1
        if report.trade_count == 1:
            sim = report.simulations[0]
            assert sim.exit_reason in {"STOP_LOSS", "TRAILING_STOP", "TAKE_PROFIT", "MAX_HOLD"}
            assert sim.exit_time > sim.entry_time  # no-lookahead

    def test_no_atr_data_skips_entry_without_crashing(self) -> None:
        t1 = EPOCH + timedelta(minutes=5)
        results = (_cycle_result(EPOCH, PositionSide.FLAT), _cycle_result(t1, PositionSide.LONG, entry_price=100.0))
        replay = _replay_result(results, start=EPOCH, end=t1 + timedelta(days=1))

        class _EmptySource:
            async def fetch_historical_candles(self, symbol, timeframe, start, end):  # noqa: ANN001
                return []

        async def scenario():
            return await simulate_lifecycle_exits(
                replay, candle_source=_EmptySource(), exit_policy=ExitPolicyConfig(), exit_window=timedelta(hours=6),
            )

        report = run_async(scenario())
        assert report.trade_count == 0
        assert report.open_at_end_count == 0  # skipped entirely -- never counted as open or completed

    def test_exit_reason_distribution_and_totals_are_consistent(self) -> None:
        t1 = EPOCH + timedelta(minutes=5)
        results = (_cycle_result(EPOCH, PositionSide.FLAT), _cycle_result(t1, PositionSide.LONG, entry_price=100.0))
        replay = _replay_result(results, start=EPOCH, end=t1 + timedelta(days=2))
        source = FakeHistoricalCandleSource()

        async def scenario():
            return await simulate_lifecycle_exits(
                replay, candle_source=source, exit_policy=ExitPolicyConfig(), exit_window=timedelta(hours=48),
            )

        report = run_async(scenario())
        assert sum(report.exit_reason_distribution.values()) == report.trade_count
        assert report.win_count + report.loss_count <= report.trade_count


=== FILE: tests/test_execution_lifecycle_store.py ===
"""
Autonomous Testnet trading lifecycle — durable persistence tests for
`LifecycleStore` (Phase 3/11/15/19). Proves the additive tables persist
and restore every required field, including across a fresh connection
(restart-equivalent)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    DailyRiskAccumulator,
    FeeLedgerEntry,
    PositionLifecycleState,
    flat_record,
)
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture()
def store(tmp_path) -> LifecycleStore:  # noqa: ANN001
    s = LifecycleStore(tmp_path / "lifecycle.db")
    yield s
    s.close()


def _long_record(**overrides: object) -> BridgePositionRecord:
    defaults: dict[str, object] = dict(
        symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
        net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
        effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW,
        entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=_NOW,
    )
    defaults.update(overrides)
    return BridgePositionRecord(**defaults)  # type: ignore[arg-type]


class TestPositionRoundTrip:
    def test_flat_round_trip(self, store: LifecycleStore) -> None:
        store.save_position(flat_record("ETHUSDT", now=_NOW))
        loaded = store.load_position("ETHUSDT")
        assert loaded is not None
        assert loaded.state is PositionLifecycleState.FLAT

    def test_long_round_trip_preserves_every_field(self, store: LifecycleStore) -> None:
        record = _long_record(trailing_active=True, last_stop_mechanism="TRAILING_STOP", cumulative_realized_gross_pnl=5.0)
        store.save_position(record)
        loaded = store.load_position("BTCUSDT")
        assert loaded == record

    def test_upsert_overwrites_previous_state(self, store: LifecycleStore) -> None:
        store.save_position(_long_record())
        store.save_position(flat_record("BTCUSDT", now=_NOW))
        loaded = store.load_position("BTCUSDT")
        assert loaded.state is PositionLifecycleState.FLAT

    def test_missing_symbol_returns_none(self, store: LifecycleStore) -> None:
        assert store.load_position("NOPEUSDT") is None

    def test_list_positions_returns_all(self, store: LifecycleStore) -> None:
        store.save_position(flat_record("AAAUSDT", now=_NOW))
        store.save_position(flat_record("BBBUSDT", now=_NOW))
        symbols = {p.symbol for p in store.list_positions()}
        assert symbols == {"AAAUSDT", "BBBUSDT"}

    def test_survives_fresh_connection_restart_equivalent(self, tmp_path) -> None:  # noqa: ANN001
        path = tmp_path / "lifecycle.db"
        store1 = LifecycleStore(path)
        store1.save_position(_long_record())
        store1.close()

        store2 = LifecycleStore(path)
        loaded = store2.load_position("BTCUSDT")
        assert loaded is not None
        assert loaded.state is PositionLifecycleState.LONG
        assert loaded.effective_stop == 90.0
        store2.close()


_PRE_ADAPTIVE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bridge_position (
    symbol TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    gross_entry_vwap REAL,
    net_owned_base_quantity REAL NOT NULL,
    entry_order_client_order_id TEXT,
    initial_protective_stop REAL,
    high_water REAL,
    effective_stop REAL,
    trailing_active INTEGER NOT NULL,
    last_stop_mechanism TEXT NOT NULL,
    take_profit REAL,
    last_evaluated_candle_close TEXT,
    exit_pending_client_order_id TEXT,
    last_exit_reason TEXT,
    entry_timestamp TEXT,
    entry_signal_context_id TEXT,
    entry_client_order_id TEXT,
    cumulative_realized_gross_pnl REAL NOT NULL,
    cooldown_until TEXT,
    migrated_existing_position INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bridge_completed_trade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    trade_group_id TEXT NOT NULL,
    entry_client_order_id TEXT NOT NULL,
    exit_client_order_id TEXT NOT NULL,
    entry_timestamp TEXT NOT NULL,
    exit_timestamp TEXT NOT NULL,
    quantity_closed REAL NOT NULL,
    gross_entry_vwap REAL NOT NULL,
    exit_gross_vwap REAL NOT NULL,
    gross_realized_pnl REAL NOT NULL,
    net_realized_pnl REAL,
    exit_reason TEXT NOT NULL,
    entry_signal_context_id TEXT,
    exit_signal_context_id TEXT,
    recorded_at TEXT NOT NULL
);
"""


class TestAdaptivePolicyFieldsMigration:
    """Adaptive Intelligence v1, step 2/15 -- proves the new
    `bridge_position`/`bridge_completed_trade` columns are added
    ADDITIVELY, idempotently, and never touch any pre-existing data. A
    real Testnet database created before this milestone has neither
    table with these columns at all (simulated here by hand-building the
    OLD schema and inserting a row with SQLite's raw driver, bypassing
    `LifecycleStore` entirely)."""

    def test_new_position_fields_round_trip(self, store: LifecycleStore) -> None:
        record = _long_record(
            exit_policy_stop_atr_multiple=1.5, exit_policy_take_profit_atr_multiple=3.0,
            exit_policy_trailing_activation_atr_multiple=1.0, exit_policy_trailing_distance_atr_multiple=1.0,
            exit_policy_max_hold_hours=12.0, policy_version_id="policy-v3",
        )
        store.save_position(record)
        loaded = store.load_position("BTCUSDT")
        assert loaded == record
        assert loaded.policy_version_id == "policy-v3"

    def test_completed_trade_policy_version_id_round_trip(self, store: LifecycleStore) -> None:
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id="ctx-1", exit_signal_context_id="ctx-2", now=_NOW,
            policy_version_id="policy-v3",
        )
        recent = store.recent_completed_trades()
        assert recent[0]["policy_version_id"] == "policy-v3"

    def test_completed_trade_policy_version_id_defaults_to_none(self, store: LifecycleStore) -> None:
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id="ctx-1", exit_signal_context_id="ctx-2", now=_NOW,
        )
        assert store.recent_completed_trades()[0]["policy_version_id"] is None

    def test_completed_trades_for_policy_version_filters_correctly(self, store: LifecycleStore) -> None:
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-1", entry_client_order_id="csl-1",
            exit_client_order_id="csl-2", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=10.0, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW, policy_version_id="v1",
        )
        store.record_completed_trade(
            symbol="ETHUSDT", trade_group_id="csl-3", entry_client_order_id="csl-3",
            exit_client_order_id="csl-4", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=90.0,
            gross_realized_pnl=-10.0, net_realized_pnl=-10.0, exit_reason="STOP_LOSS",
            entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW, policy_version_id="v2",
        )
        v1_trades = store.completed_trades_for_policy_version("v1")
        assert len(v1_trades) == 1
        assert v1_trades[0]["symbol"] == "BTCUSDT"

    def test_opens_pre_adaptive_database_without_crashing_and_adds_columns(self, tmp_path) -> None:  # noqa: ANN001
        path = tmp_path / "legacy.db"
        raw = sqlite3.connect(str(path))
        try:
            raw.executescript(_PRE_ADAPTIVE_SCHEMA_SQL)
            raw.execute(
                """
                INSERT INTO bridge_position (
                    symbol, state, gross_entry_vwap, net_owned_base_quantity,
                    entry_order_client_order_id, initial_protective_stop, high_water, effective_stop,
                    trailing_active, last_stop_mechanism, take_profit, last_evaluated_candle_close,
                    exit_pending_client_order_id, last_exit_reason, entry_timestamp,
                    entry_signal_context_id, entry_client_order_id, cumulative_realized_gross_pnl,
                    cooldown_until, migrated_existing_position, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "BTCUSDT", "LONG", 100.0, 1.0, None, 90.0, 100.0, 90.0, 0, "STOP_LOSS", 120.0, None,
                    None, None, _NOW.isoformat(), "ctx-1", "csl-entry-1", 0.0, None, 0, _NOW.isoformat(),
                ),
            )
            raw.commit()
        finally:
            raw.close()

        # Opening via `LifecycleStore` must not crash (idempotent ALTER
        # TABLE) and the pre-existing row must load with the new fields
        # defaulting to `None` -- unchanged behavior via
        # `resolved_exit_policy()`'s fallback, never a crash.
        migrated = LifecycleStore(path)
        try:
            loaded = migrated.load_position("BTCUSDT")
            assert loaded is not None
            assert loaded.state is PositionLifecycleState.LONG
            assert loaded.gross_entry_vwap == 100.0  # pre-existing data untouched
            assert loaded.policy_version_id is None
            assert loaded.exit_policy_max_hold_hours is None

            # Opening a SECOND time (e.g. a process restart) must also not
            # crash -- the "duplicate column name" path is exercised here.
            migrated.close()
            reopened = LifecycleStore(path)
            try:
                assert reopened.load_position("BTCUSDT").gross_entry_vwap == 100.0
            finally:
                reopened.close()
        finally:
            pass


class TestFeeLedger:
    def test_append_and_read_back(self, store: LifecycleStore) -> None:
        entries = (
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=100.0, client_order_id="csl-entry-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        store.append_fee_entries("BTCUSDT", "csl-entry-1", entries)
        loaded = store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1")
        assert loaded == entries

    def test_ledger_grouped_by_trade_group_id_not_mixed_across_reentries(self, store: LifecycleStore) -> None:
        entry1 = (
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=100.0, client_order_id="csl-entry-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        entry2 = (
            FeeLedgerEntry(
                amount=0.002, asset="BTC", usdt_equivalent=200.0, client_order_id="csl-entry-2",
                side="ENTRY", trade_id=2, recorded_at=_NOW,
            ),
        )
        store.append_fee_entries("BTCUSDT", "csl-entry-1", entry1)
        store.append_fee_entries("BTCUSDT", "csl-entry-2", entry2)
        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1") == entry1
        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-2") == entry2

    def test_empty_entries_is_a_no_op(self, store: LifecycleStore) -> None:
        store.append_fee_entries("BTCUSDT", "csl-entry-1", ())
        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1") == ()

    def test_no_symbol_isolation_leak(self, store: LifecycleStore) -> None:
        entries = (
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=100.0, client_order_id="csl-entry-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        store.append_fee_entries("BTCUSDT", "csl-shared-id", entries)
        assert store.fee_ledger_for_trade_group("ETHUSDT", "csl-shared-id") == ()


class TestCompletedTrades:
    def test_record_and_read_recent(self, store: LifecycleStore) -> None:
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id="ctx-1", exit_signal_context_id="ctx-2", now=_NOW,
        )
        recent = store.recent_completed_trades()
        assert len(recent) == 1
        assert recent[0]["symbol"] == "BTCUSDT"
        assert recent[0]["gross_realized_pnl"] == 10.0

    def test_net_realized_pnl_nullable_for_unknown(self, store: LifecycleStore) -> None:
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=None, exit_reason="STOP_LOSS",
            entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW,
        )
        recent = store.recent_completed_trades()
        assert recent[0]["net_realized_pnl"] is None

    def test_completed_trades_for_symbol_excludes_others(self, store: LifecycleStore) -> None:
        for symbol in ("BTCUSDT", "ETHUSDT"):
            store.record_completed_trade(
                symbol=symbol, trade_group_id="csl-1", entry_client_order_id="csl-1",
                exit_client_order_id="csl-2", entry_timestamp=_NOW, exit_timestamp=_NOW,
                quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
                gross_realized_pnl=10.0, net_realized_pnl=10.0, exit_reason="TAKE_PROFIT",
                entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW,
            )
        trades = store.completed_trades_for_symbol("BTCUSDT")
        assert len(trades) == 1
        assert trades[0]["symbol"] == "BTCUSDT"

    def test_recent_completed_trades_respects_limit(self, store: LifecycleStore) -> None:
        for i in range(5):
            store.record_completed_trade(
                symbol="BTCUSDT", trade_group_id=f"csl-{i}", entry_client_order_id=f"csl-{i}",
                exit_client_order_id=f"csl-exit-{i}", entry_timestamp=_NOW, exit_timestamp=_NOW,
                quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
                gross_realized_pnl=10.0, net_realized_pnl=10.0, exit_reason="TAKE_PROFIT",
                entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW,
            )
        assert len(store.recent_completed_trades(limit=2)) == 2


class TestRealizedPnlAggregates:
    """Portfolio/Accounting v1, step 2 — `total_realized_pnl()`/
    `realized_pnl_for_day()`, SQL-aggregate based (no Python-side loop
    over a bounded fetch)."""

    def _record_trade(
        self, store: LifecycleStore, *, index: int, net_pnl: float | None, recorded_at: datetime,
        symbol: str = "BTCUSDT",
    ) -> None:
        store.record_completed_trade(
            symbol=symbol, trade_group_id=f"csl-{index}", entry_client_order_id=f"csl-{index}",
            exit_client_order_id=f"csl-exit-{index}", entry_timestamp=recorded_at, exit_timestamp=recorded_at,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=net_pnl, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=recorded_at,
        )

    def test_total_realized_pnl_empty_table(self, store: LifecycleStore) -> None:
        assert store.total_realized_pnl() == (0.0, 0)

    def test_total_realized_pnl_sums_all_known_net_pnl(self, store: LifecycleStore) -> None:
        self._record_trade(store, index=0, net_pnl=10.0, recorded_at=_NOW)
        self._record_trade(store, index=1, net_pnl=-3.0, recorded_at=_NOW)
        self._record_trade(store, index=2, net_pnl=None, recorded_at=_NOW)  # unknown fee -- excluded from SUM
        total, count = store.total_realized_pnl()
        assert total == pytest.approx(7.0)  # 10.0 + (-3.0), None skipped by SQL SUM
        assert count == 3  # but COUNT(*) still counts the unknown-fee trade as a completed trade

    def test_total_realized_pnl_includes_all_trades_beyond_the_old_limit_50_blind_spot(self, store: LifecycleStore) -> None:
        """THE required test: >50 completed trades must ALL be included,
        proving the fix against `recent_completed_trades(limit=50)`'s old
        blind spot (which `app.py` used to sum over directly)."""
        trade_count = 63
        for i in range(trade_count):
            self._record_trade(store, index=i, net_pnl=1.0, recorded_at=_NOW)
        total, count = store.total_realized_pnl()
        assert count == trade_count
        assert total == pytest.approx(float(trade_count))
        # Sanity: confirm this really would have been missed by the
        # bounded accessor alone.
        assert len(store.recent_completed_trades(limit=50)) == 50
        assert count > 50

    def test_realized_pnl_for_day_filters_to_the_given_utc_calendar_day(self, store: LifecycleStore) -> None:
        from crypto_signal_engine.execution.lifecycle import trading_day_key

        day1 = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
        day2 = datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc)
        self._record_trade(store, index=0, net_pnl=5.0, recorded_at=day1)
        self._record_trade(store, index=1, net_pnl=7.0, recorded_at=day1)
        self._record_trade(store, index=2, net_pnl=100.0, recorded_at=day2)

        total_day1, count_day1 = store.realized_pnl_for_day(trading_day_key(day1))
        assert total_day1 == pytest.approx(12.0)
        assert count_day1 == 2

        total_day2, count_day2 = store.realized_pnl_for_day(trading_day_key(day2))
        assert total_day2 == pytest.approx(100.0)
        assert count_day2 == 1

    def test_realized_pnl_for_day_empty_day_returns_zero(self, store: LifecycleStore) -> None:
        from crypto_signal_engine.execution.lifecycle import trading_day_key

        self._record_trade(store, index=0, net_pnl=5.0, recorded_at=_NOW)
        assert store.realized_pnl_for_day(trading_day_key(datetime(2099, 1, 1, tzinfo=timezone.utc))) == (0.0, 0)

    def test_realized_pnl_for_day_uses_the_exact_trading_day_key_convention(self, store: LifecycleStore) -> None:
        """Reuses `trading_day_key()`'s own `"YYYY-MM-DD"` format rather
        than inventing a second day-boundary definition -- proven by
        passing its literal output straight through."""
        from crypto_signal_engine.execution.lifecycle import trading_day_key

        moment = datetime(2026, 6, 15, 23, 59, 59, tzinfo=timezone.utc)
        assert trading_day_key(moment) == "2026-06-15"
        self._record_trade(store, index=0, net_pnl=42.0, recorded_at=moment)
        total, count = store.realized_pnl_for_day("2026-06-15")
        assert (total, count) == (pytest.approx(42.0), 1)

    def test_realized_pnl_for_day_reconcilable_against_daily_risk_accumulator_when_all_fees_known(
        self, store: LifecycleStore,
    ) -> None:
        """Documents (via a passing, executable test, not just a
        docstring claim) the case where the two numbers ARE identical:
        when every trade that day had a fully-known fee, `net_realized_
        pnl` (this function's basis) and `conservative_risk_pnl`
        (`DailyRiskAccumulator`'s basis, via `compute_trade_risk_
        contribution`) collapse to the same formula -- gross P&L minus
        known fees, no reserve needed."""
        from crypto_signal_engine.execution.lifecycle import (
            DailyRiskAccumulator,
            TradeRiskContribution,
            apply_trade_to_daily_accumulator,
            trading_day_key,
        )

        day_key = trading_day_key(_NOW)
        # A trade with a fully-known net_realized_pnl of 9.5 (gross 10.0,
        # 0.5 known fee) -- record_completed_trade's net_realized_pnl IS
        # this already-fee-adjusted number.
        store.record_completed_trade(
            symbol="BTCUSDT", trade_group_id="csl-0", entry_client_order_id="csl-0",
            exit_client_order_id="csl-exit-0", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id=None, exit_signal_context_id=None, now=_NOW,
        )
        # The SAME trade's risk contribution, computed the way
        # `_finalize_exit()` actually does it: KNOWN fee basis, identical
        # gross-minus-known-fees arithmetic.
        contribution = TradeRiskContribution(conservative_pnl=9.5, fee_basis="KNOWN")
        accumulator = apply_trade_to_daily_accumulator(DailyRiskAccumulator(trading_day=day_key), contribution)

        total, _count = store.realized_pnl_for_day(day_key)
        assert total == pytest.approx(accumulator.conservative_risk_pnl)  # identical when fees are fully known


class TestDailyRisk:
    def test_missing_day_returns_fresh_zero_accumulator(self, store: LifecycleStore) -> None:
        acc = store.load_daily_risk("2026-01-01")
        assert acc.conservative_risk_pnl == 0.0
        assert acc.trades_counted == 0

    def test_save_and_reload(self, store: LifecycleStore) -> None:
        acc = DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-12.5, trades_counted=3)
        store.save_daily_risk(acc, now=_NOW)
        loaded = store.load_daily_risk("2026-01-01")
        assert loaded.conservative_risk_pnl == -12.5
        assert loaded.trades_counted == 3

    def test_survives_restart(self, tmp_path) -> None:  # noqa: ANN001
        path = tmp_path / "lifecycle.db"
        store1 = LifecycleStore(path)
        store1.save_daily_risk(DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-5.0, trades_counted=1), now=_NOW)
        store1.close()

        store2 = LifecycleStore(path)
        loaded = store2.load_daily_risk("2026-01-01")
        assert loaded.conservative_risk_pnl == -5.0
        store2.close()

    def test_different_days_isolated(self, store: LifecycleStore) -> None:
        store.save_daily_risk(DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-5.0, trades_counted=1), now=_NOW)
        store.save_daily_risk(DailyRiskAccumulator(trading_day="2026-01-02", conservative_risk_pnl=-1.0, trades_counted=1), now=_NOW)
        assert store.load_daily_risk("2026-01-01").conservative_risk_pnl == -5.0
        assert store.load_daily_risk("2026-01-02").conservative_risk_pnl == -1.0


class TestAtomicity:
    """CRITICAL FIX (M1 — mainnet-readiness review) regression tests.

    Before this fix, every multi-statement write here ran under `with
    self._connection:` — a SILENT NO-OP under this connection's
    `isolation_level=None` (autocommit) setting, since Python's sqlite3
    module only auto-manages BEGIN/COMMIT when `isolation_level` is NOT
    `None` (see the module-level comment above `LifecycleStore._begin()`).
    These tests inject a failure PARTWAY through a multi-statement write
    and assert NOTHING from that write landed — proving the fix actually
    provides real transactional atomicity, not just the appearance of it."""

    def _entries(self, *, n: int = 3, trade_group_id: str = "csl-entry-1") -> tuple[FeeLedgerEntry, ...]:
        return tuple(
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=10.0, client_order_id=trade_group_id,
                side="ENTRY", trade_id=i, recorded_at=_NOW,
            )
            for i in range(n)
        )

    class _FaultInjectingConnectionProxy:
        """A raw `sqlite3.Connection` object's `execute`/`executemany`
        attributes are C-level and read-only — they cannot be monkeypatched
        directly. This thin proxy forwards everything to the real
        connection except `execute`/`executemany`, which it can fail on
        demand, letting these tests inject a failure at a precise point
        inside an otherwise-real transaction."""

        def __init__(self, real_connection):
            self._real = real_connection
            self.fail_execute_containing: str | None = None
            self.fail_executemany: bool = False
            self.execute_fail_count = 0

        def execute(self, sql, *args, **kwargs):
            if self.fail_execute_containing is not None and self.fail_execute_containing in sql:
                self.execute_fail_count += 1
                raise sqlite3.OperationalError("simulated fault injection (execute)")
            return self._real.execute(sql, *args, **kwargs)

        def executemany(self, *args, **kwargs):
            if self.fail_executemany:
                raise sqlite3.OperationalError("simulated fault injection (executemany)")
            return self._real.executemany(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._real, name)

    def test_append_fee_entries_partial_executemany_failure_leaves_zero_rows(self, store: LifecycleStore) -> None:
        """The old `with self._connection:` wrapper around `executemany()`
        gave NO cross-row atomicity guarantee — a mid-batch failure could
        leave some rows committed and others not. Simulate a failure
        immediately after the (now-explicit) `BEGIN IMMEDIATE` by making
        the underlying `executemany` raise, and confirm the whole batch
        is rolled back (zero rows), not partially applied."""
        entries = self._entries(n=5)
        real_connection = store._connection
        proxy = self._FaultInjectingConnectionProxy(real_connection)
        proxy.fail_executemany = True
        store._connection = proxy
        with pytest.raises(Exception):
            store.append_fee_entries("BTCUSDT", "csl-entry-1", entries)
        store._connection = real_connection

        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1") == ()

    def test_save_position_with_fee_entries_rolls_back_both_on_mid_transaction_failure(self, store: LifecycleStore) -> None:
        """If the fee-ledger append half of this atomic composite fails,
        the position half must NOT have landed either — a crash here must
        never leave a fee-ledger row for a position that was never
        actually saved (the exact M1 gap this fix closes for
        `on_entry_filled()`)."""
        record = _long_record()
        entries = self._entries(n=2, trade_group_id="csl-entry-1")
        real_connection = store._connection
        proxy = self._FaultInjectingConnectionProxy(real_connection)
        proxy.fail_executemany = True
        store._connection = proxy
        with pytest.raises(Exception):
            store.save_position_with_fee_entries(record, trade_group_id="csl-entry-1", fee_entries=entries)
        store._connection = real_connection

        assert store.load_position("BTCUSDT") is None
        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1") == ()

    def test_finalize_exit_rolls_back_all_four_writes_on_late_failure(self, store: LifecycleStore) -> None:
        """The most important atomicity guarantee: if the LAST of the four
        writes (`save_position`, i.e. flipping the symbol out of LONG)
        fails, the fee-ledger append, the completed-trade record, AND the
        daily-risk update that already ran earlier in THIS SAME
        transaction must ALL be rolled back too — never a completed
        trade recorded while the position stays (incorrectly) LONG, or
        any other partial cross-table state."""
        position = _long_record()
        store.save_position(position)
        new_position = flat_record("BTCUSDT", now=_NOW)
        entries = self._entries(n=2, trade_group_id="csl-entry-1")
        completed_trade = dict(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id="ctx-1", exit_signal_context_id="ctx-2", now=_NOW,
        )
        daily_risk = DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-1.0, trades_counted=1)

        real_connection = store._connection
        proxy = self._FaultInjectingConnectionProxy(real_connection)
        # Let every read/BEGIN through untouched; fail only the FINAL
        # write (`INSERT INTO bridge_position`) so the first three writes
        # have ALREADY happened inside this same transaction before the
        # failure occurs.
        proxy.fail_execute_containing = "INSERT INTO bridge_position"
        store._connection = proxy
        with pytest.raises(Exception):
            store.finalize_exit(
                new_position=new_position, trade_group_id="csl-entry-1", fee_entries=entries,
                completed_trade=completed_trade, daily_risk=daily_risk, daily_risk_now=_NOW,
            )
        store._connection = real_connection
        assert proxy.execute_fail_count == 1  # confirms the failure point was actually reached

        # Nothing from this transaction landed: position is UNCHANGED
        # (still the original LONG record), no completed trade, no
        # fee-ledger rows, no daily-risk update.
        assert store.load_position("BTCUSDT") == position
        assert store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1") == ()
        assert store.completed_trades_for_symbol("BTCUSDT") == ()
        assert store.load_daily_risk("2026-01-01").conservative_risk_pnl == 0.0

    def test_finalize_exit_succeeds_atomically_when_nothing_fails(self, store: LifecycleStore) -> None:
        """Sanity companion to the rollback test above — the happy path
        commits all four writes together."""
        position = _long_record()
        store.save_position(position)
        new_position = flat_record("BTCUSDT", now=_NOW)
        entries = self._entries(n=2, trade_group_id="csl-entry-1")
        completed_trade = dict(
            symbol="BTCUSDT", trade_group_id="csl-entry-1", entry_client_order_id="csl-entry-1",
            exit_client_order_id="csl-exit-1", entry_timestamp=_NOW, exit_timestamp=_NOW,
            quantity_closed=1.0, gross_entry_vwap=100.0, exit_gross_vwap=110.0,
            gross_realized_pnl=10.0, net_realized_pnl=9.5, exit_reason="TAKE_PROFIT",
            entry_signal_context_id="ctx-1", exit_signal_context_id="ctx-2", now=_NOW,
        )
        daily_risk = DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-1.0, trades_counted=1)

        store.finalize_exit(
            new_position=new_position, trade_group_id="csl-entry-1", fee_entries=entries,
            completed_trade=completed_trade, daily_risk=daily_risk, daily_risk_now=_NOW,
        )

        assert store.load_position("BTCUSDT").state is PositionLifecycleState.FLAT
        assert len(store.fee_ledger_for_trade_group("BTCUSDT", "csl-entry-1")) == 2
        assert len(store.completed_trades_for_symbol("BTCUSDT")) == 1
        assert store.load_daily_risk("2026-01-01").conservative_risk_pnl == -1.0


=== FILE: tests/test_execution_models.py ===
"""Faz 10 — execution/models.py testleri: OrderIntent validasyonu,
deterministik client_order_id, ExecutionMode/ExecutionResult."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.models import (
    UNSUPPORTED_EXECUTION_MODES,
    ExecutionMode,
    ExecutionResult,
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _market_intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.001,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


class TestExecutionMode:
    def test_only_paper_and_testnet_are_valid(self) -> None:
        assert {m.value for m in ExecutionMode} == {"PAPER", "BINANCE_SPOT_TESTNET"}

    def test_mainnet_is_not_a_member(self) -> None:
        assert "MAINNET" not in {m.value for m in ExecutionMode}
        with pytest.raises(ValueError):
            ExecutionMode("MAINNET")

    def test_unsupported_modes_are_never_valid_execution_mode_values(self) -> None:
        valid_values = {m.value for m in ExecutionMode}
        assert valid_values.isdisjoint(UNSUPPORTED_EXECUTION_MODES)


class TestOrderIntentClientOrderId:
    def test_deterministic_same_intent_same_id(self) -> None:
        assert _market_intent().client_order_id == _market_intent().client_order_id

    def test_different_side_different_id(self) -> None:
        assert _market_intent(side=OrderSide.BUY).client_order_id != _market_intent(side=OrderSide.SELL).client_order_id

    def test_different_quantity_different_id(self) -> None:
        assert _market_intent(quantity=0.001).client_order_id != _market_intent(quantity=0.002).client_order_id

    def test_different_context_id_different_id(self) -> None:
        assert _market_intent(context_id="ctx-1").client_order_id != _market_intent(context_id="ctx-2").client_order_id

    def test_different_symbol_different_id(self) -> None:
        assert _market_intent(symbol="BTCUSDT").client_order_id != _market_intent(symbol="ETHUSDT").client_order_id

    def test_id_within_binance_length_limit(self) -> None:
        assert len(_market_intent().client_order_id) <= 36

    def test_id_cannot_be_overridden_by_constructor(self) -> None:
        with pytest.raises(TypeError):
            OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id="ctx-1", timestamp=NOW, quantity=0.001, client_order_id="attacker-controlled",
            )


class TestOrderIntentValidation:
    def test_market_requires_quantity_or_quote_quantity(self) -> None:
        with pytest.raises(ValueError):
            OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
                context_id="ctx-1", timestamp=NOW,
            )

    def test_market_rejects_both_quantity_and_quote_quantity(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(quantity=0.001, quote_quantity=10.0)

    def test_market_rejects_price(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(price=100.0)

    def test_market_quote_quantity_variant_is_valid(self) -> None:
        intent = _market_intent(quantity=None, quote_quantity=15.0)
        assert intent.quote_quantity == 15.0

    def test_limit_requires_price(self) -> None:
        with pytest.raises(ValueError):
            OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
                context_id="ctx-1", timestamp=NOW, quantity=0.001,
            )

    def test_limit_requires_quantity(self) -> None:
        with pytest.raises(ValueError):
            OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
                context_id="ctx-1", timestamp=NOW, price=100.0,
            )

    def test_limit_defaults_time_in_force_to_gtc(self) -> None:
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.001, price=100.0,
        )
        assert intent.time_in_force is TimeInForce.GTC

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(quantity=-0.001)

    def test_zero_quantity_rejected(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(quantity=0.0)

    def test_nan_quantity_rejected(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(quantity=float("nan"))

    def test_infinite_price_rejected(self) -> None:
        with pytest.raises(ValueError):
            OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
                context_id="ctx-1", timestamp=NOW, quantity=0.001, price=float("inf"),
            )

    def test_naive_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(timestamp=datetime(2026, 1, 1))

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            _market_intent(context_id="   ")

    def test_symbol_is_normalized(self) -> None:
        assert _market_intent(symbol="btcusdt").symbol == "BTCUSDT"


class TestExecutionResult:
    def test_valid_result_constructs(self) -> None:
        result = ExecutionResult(
            symbol="BTCUSDT", client_order_id="csl-abc", exchange_order_id=1,
            side=OrderSide.BUY, status="FILLED", executed_quantity=0.001,
            cumulative_quote_quantity=100.0, transaction_time=NOW, context_id="ctx-1",
        )
        assert result.symbol == "BTCUSDT"

    def test_negative_executed_quantity_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExecutionResult(
                symbol="BTCUSDT", client_order_id="csl-abc", exchange_order_id=1,
                side=OrderSide.BUY, status="FILLED", executed_quantity=-1.0,
                cumulative_quote_quantity=100.0, transaction_time=NOW, context_id="ctx-1",
            )

    def test_naive_transaction_time_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExecutionResult(
                symbol="BTCUSDT", client_order_id="csl-abc", exchange_order_id=1,
                side=OrderSide.BUY, status="FILLED", executed_quantity=1.0,
                cumulative_quote_quantity=100.0, transaction_time=datetime(2026, 1, 1), context_id="ctx-1",
            )


=== FILE: tests/test_execution_reconciliation_models.py ===
"""Faz 11 — execution/reconciliation_models.py testleri: lifecycle state
model, monotonic/contradiction koruması, Binance status mapping."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.errors import ImpossibleLifecycleTransitionError, ReconciliationContradictionError
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import (
    NEEDS_RECONCILIATION_STATES,
    TERMINAL_STATES,
    ExecutionLifecycleState,
    apply_exchange_truth,
    lifecycle_state_from_binance_status,
    new_record,
    transition,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
LATER = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)


def _intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


class TestBinanceStatusMapping:
    @pytest.mark.parametrize(
        "status,expected",
        [
            ("NEW", ExecutionLifecycleState.ACKNOWLEDGED),
            ("PENDING_CANCEL", ExecutionLifecycleState.ACKNOWLEDGED),
            ("PARTIALLY_FILLED", ExecutionLifecycleState.PARTIALLY_FILLED),
            ("FILLED", ExecutionLifecycleState.FILLED),
            ("CANCELED", ExecutionLifecycleState.CANCELED),
            ("REJECTED", ExecutionLifecycleState.REJECTED),
            ("EXPIRED", ExecutionLifecycleState.EXPIRED),
            ("EXPIRED_IN_MATCH", ExecutionLifecycleState.EXPIRED),
        ],
    )
    def test_known_statuses_map_correctly(self, status, expected) -> None:
        assert lifecycle_state_from_binance_status(status) == expected

    def test_unknown_status_raises(self) -> None:
        with pytest.raises(ImpossibleLifecycleTransitionError):
            lifecycle_state_from_binance_status("SOME_FUTURE_STATUS")


class TestNewRecord:
    def test_new_record_starts_at_intent_created(self) -> None:
        record = new_record(_intent(), now=NOW)
        assert record.lifecycle_state == ExecutionLifecycleState.INTENT_CREATED
        assert record.executed_quantity == 0.0
        assert record.exchange_order_id is None
        assert record.is_terminal() is False

    def test_new_record_carries_intent_identity(self) -> None:
        intent = _intent(context_id="ctx-xyz")
        record = new_record(intent, now=NOW)
        assert record.context_id == "ctx-xyz"
        assert record.client_order_id == intent.client_order_id


class TestTransition:
    def test_simple_transition_updates_state_and_timestamp(self) -> None:
        record = new_record(_intent(), now=NOW)
        moved = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=LATER)
        assert moved.lifecycle_state == ExecutionLifecycleState.SUBMISSION_ATTEMPTED
        assert moved.updated_at == LATER

    def test_transition_out_of_terminal_state_rejected(self) -> None:
        record = new_record(_intent(), now=NOW)
        filled = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=1,
            executed_quantity=0.01, cumulative_quote_quantity=500.0, now=LATER,
        )
        with pytest.raises(ReconciliationContradictionError):
            transition(filled, new_state=ExecutionLifecycleState.AMBIGUOUS, now=LATER)

    def test_transition_within_same_terminal_state_is_a_noop(self) -> None:
        record = new_record(_intent(), now=NOW)
        rejected = transition(record, new_state=ExecutionLifecycleState.REJECTED, now=LATER)
        again = transition(rejected, new_state=ExecutionLifecycleState.REJECTED, now=LATER)
        assert again.lifecycle_state == ExecutionLifecycleState.REJECTED


class TestApplyExchangeTruth:
    def test_acknowledged_to_filled_is_valid_progress(self) -> None:
        record = new_record(_intent(), now=NOW)
        ack = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=5,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        filled = apply_exchange_truth(
            ack, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=5,
            executed_quantity=0.01, cumulative_quote_quantity=500.0, now=LATER,
        )
        assert filled.lifecycle_state == ExecutionLifecycleState.FILLED
        assert filled.executed_quantity == 0.01
        assert filled.last_reconciled_at == LATER

    def test_filled_cannot_regress_to_new(self) -> None:
        record = new_record(_intent(), now=NOW)
        filled = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=5,
            executed_quantity=0.01, cumulative_quote_quantity=500.0, now=NOW,
        )
        with pytest.raises(ReconciliationContradictionError):
            apply_exchange_truth(
                filled, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=5,
                executed_quantity=0.01, cumulative_quote_quantity=500.0, now=LATER,
            )

    def test_executed_quantity_cannot_decrease(self) -> None:
        record = new_record(_intent(), now=NOW)
        partial = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=5,
            executed_quantity=0.005, cumulative_quote_quantity=250.0, now=NOW,
        )
        with pytest.raises(ReconciliationContradictionError):
            apply_exchange_truth(
                partial, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=5,
                executed_quantity=0.001, cumulative_quote_quantity=50.0, now=LATER,
            )

    def test_exchange_order_id_cannot_change(self) -> None:
        record = new_record(_intent(), now=NOW)
        ack = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=5,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        with pytest.raises(ReconciliationContradictionError):
            apply_exchange_truth(
                ack, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=999,
                executed_quantity=0.0, cumulative_quote_quantity=0.0, now=LATER,
            )

    def test_partial_fill_progress_is_monotonic_and_preserved(self) -> None:
        record = new_record(_intent(), now=NOW)
        step1 = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=5,
            executed_quantity=0.003, cumulative_quote_quantity=150.0, now=NOW,
        )
        step2 = apply_exchange_truth(
            step1, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=5,
            executed_quantity=0.006, cumulative_quote_quantity=300.0, now=LATER,
        )
        assert step2.executed_quantity == 0.006
        assert step2.lifecycle_state == ExecutionLifecycleState.PARTIALLY_FILLED

    def test_repeated_identical_terminal_reconciliation_is_a_harmless_noop(self) -> None:
        record = new_record(_intent(), now=NOW)
        filled = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=5,
            executed_quantity=0.01, cumulative_quote_quantity=500.0, now=NOW,
        )
        again = apply_exchange_truth(
            filled, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=5,
            executed_quantity=0.01, cumulative_quote_quantity=500.0, now=LATER,
        )
        assert again.lifecycle_state == ExecutionLifecycleState.FILLED
        assert again.executed_quantity == 0.01


class TestStateSetMembership:
    def test_terminal_states_are_exactly_expected(self) -> None:
        assert TERMINAL_STATES == {
            ExecutionLifecycleState.FILLED, ExecutionLifecycleState.CANCELED,
            ExecutionLifecycleState.EXPIRED, ExecutionLifecycleState.REJECTED,
        }

    def test_needs_reconciliation_excludes_terminal_but_includes_unknown_not_found(self) -> None:
        """BLOCKER FİX (Karar 78): `UNKNOWN_NOT_FOUND`, tek bir -2013
        yanıtının orijinal ambiguous POST'un HİÇ kabul edilmediğini
        KANITLAMADIĞI için reconciliation-eligible KALIR — "güvenli/
        çözülmüş" bir durum DEĞİLDİR, terminal de DEĞİLDİR."""
        assert NEEDS_RECONCILIATION_STATES.isdisjoint(TERMINAL_STATES)
        assert ExecutionLifecycleState.UNKNOWN_NOT_FOUND in NEEDS_RECONCILIATION_STATES
        assert ExecutionLifecycleState.INTENT_CREATED not in NEEDS_RECONCILIATION_STATES


=== FILE: tests/test_execution_reconciliation_service.py ===
"""Faz 11 — execution/reconciliation_service.py testleri: ambiguous-timeout
handling, duplicate suppression, stable client_order_id ile query-based
reconciliation, restart recovery, partial-fill monotonicity, contradiction
protection, persistence-failure surfacing, multi-symbol/multi-context
izolasyonu. Tamamen offline — `FakeTestnetHttpClient` + `FixedClock` +
temp SQLite kullanılır, gerçek Binance TESTNET bağımlılığı YOK."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.errors import (
    BinanceRejectionError,
    ExecutionIdempotencyConflictError,
    ExecutionPersistenceError,
    ExecutionTimeoutError,
    ExecutionTransportError,
    MalformedResponseError,
    ReconciliationContradictionError,
)
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _exchange_info(symbol: str = "BTCUSDT") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


def _order_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.01", "cummulativeQuoteQty": "500.0",
        "transactTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _query_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
        "updateTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _service(get_responses=None, post_responses=None, *, db_path, store_factory=ExecutionStateStore):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    store = store_factory(db_path)
    service = ExecutionReconciliationService(client, store, clock=FixedClock(NOW))
    return service, http, store


class TestSuccessfulSubmit:
    def test_successful_submit_persists_filled_state(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.FILLED
            assert record.exchange_order_id == 1
            persisted = store.load_by_context_id("ctx-1")
            assert persisted.lifecycle_state == ExecutionLifecycleState.FILLED
            store.close()

        run_async(scenario())

    def test_acknowledged_state_for_new_order(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0")],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.ACKNOWLEDGED
            store.close()

        run_async(scenario())


class TestDuplicateSuppression:
    def test_same_intent_replay_produces_no_duplicate_post(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            first = await service.submit(intent)
            second = await service.submit(intent)
            assert first == second
            assert len(http.post_calls) == 1
            store.close()

        run_async(scenario())

    def test_conflicting_same_context_id_different_economics_raises(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            await service.submit(_intent(context_id="ctx-dup", quantity=0.01))
            with pytest.raises(ExecutionIdempotencyConflictError):
                await service.submit(_intent(context_id="ctx-dup", quantity=0.02))  # farklı ekonomik -> farklı client_order_id
            assert len(http.post_calls) == 1  # ikinci intent İÇİN hiç POST YAPILMADI
            store.close()

        run_async(scenario())


class TestAmbiguousSubmission:
    def test_timeout_after_post_then_query_finds_order_reconciles(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info()), _query_response(status="NEW")],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.ACKNOWLEDGED
            assert record.exchange_order_id == 1
            assert len(http.post_calls) == 1  # KESİNLİKLE yeniden POST YAPILMADI
            store.close()

        run_async(scenario())

    def test_timeout_then_query_says_not_found_marks_unknown(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),
                ],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND
            assert len(http.post_calls) == 1
            store.close()

        run_async(scenario())

    def test_unknown_not_found_never_allows_automatic_resubmission(self, tmp_path) -> None:
        """BLOCKER FİX (Karar 78): `UNKNOWN_NOT_FOUND`, ARTIK "yeniden
        gönderim güvenlidir" ANLAMINA GELMEZ. Aynı intent'in TEKRAR
        `submit()` edilmesi, KESİNLİKLE İKİNCİ bir POST ÜRETMEZ — yalnızca
        YENİDEN sorgular. Sorgu YİNE `-2013` dönerse, kayıt ÇÖZÜLMEMİŞ
        (`UNKNOWN_NOT_FOUND`) KALIR."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service, http, store = _service(
                get_responses=[
                    json_response(_exchange_info()),  # submit()#1 -> validate_intent
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),  # submit()#1 -> reconcile query
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),  # submit()#2 -> YENİDEN sorgu (POST YOK)
                ],
                post_responses=[ExecutionTimeoutError("timed out")],  # yalnızca TEK POST script'i — ikinci bir POST asla YAPILMAMALI
                db_path=db_path,
            )
            intent = _intent()
            first = await service.submit(intent)
            assert first.lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND

            second = await service.submit(intent)
            assert second.lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND  # HÂLÂ çözülmemiş
            assert second.client_order_id == first.client_order_id
            assert len(http.post_calls) == 1  # KESİNLİKLE ikinci bir POST YOK
            assert len(http.get_calls) == 3  # exchangeInfo + 2 sorgu (POST asla tekrarlanmadı)
            store.close()

        run_async(scenario())

    def test_later_reconciliation_finds_original_order_after_unknown_not_found(self, tmp_path) -> None:
        """Belirsizlik SONRASI `UNKNOWN_NOT_FOUND`'a düşen bir kayıt, DAHA
        SONRA (örn. Binance'in gecikmeli tutarlılığı nedeniyle) GERÇEKTEN
        bulunursa, AYNI `client_order_id` ile GERÇEK exchange truth'una
        reconcile edilir — TOPLAM POST sayısı HÂLÂ TEKTİR."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service, http, store = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),
                    _query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0"),
                ],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=db_path,
            )
            intent = _intent()
            first = await service.submit(intent)
            assert first.lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND

            second = await service.submit(intent)  # YENİ bir POST DEĞİL — yalnızca yeniden sorgu
            assert second.lifecycle_state == ExecutionLifecycleState.FILLED
            assert second.client_order_id == first.client_order_id
            assert second.exchange_order_id == 1
            assert len(http.post_calls) == 1  # TOPLAM POST sayısı HÂLÂ TEK
            store.close()

        run_async(scenario())

    def test_transport_error_after_post_also_treated_as_ambiguous(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info()), _query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                post_responses=[ExecutionTransportError("connection reset")],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http.post_calls) == 1
            store.close()

        run_async(scenario())


class TestRejection:
    def test_binance_rejection_marks_rejected_no_reconciliation_needed(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[(400, '{"code": -2010, "msg": "Account has insufficient balance"}')],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.lifecycle_state == ExecutionLifecycleState.REJECTED
            assert len(http.get_calls) == 1  # yalnızca exchangeInfo — hiçbir query_order çağrısı YAPILMADI
            store.close()

        run_async(scenario())

    def test_replay_after_rejected_returns_same_terminal_record_without_post(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[(400, '{"code": -2010, "msg": "Account has insufficient balance"}')],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            first = await service.submit(intent)
            second = await service.submit(intent)
            assert first == second
            assert len(http.post_calls) == 1
            store.close()

        run_async(scenario())


class TestReconcileUnknownIdentity:
    def test_unknown_context_id_raises_execution_error_not_bare_value_error(self, tmp_path) -> None:
        """Regresyon: `reconcile()` bir `ExecutionError` alt sınıfı
        fırlatmalıdır ki CLI'nin `except ExecutionError` yakalayıcısı ham
        bir traceback SIZDIRMADAN temiz bir hata ile sonuçlanabilsin."""
        from crypto_signal_engine.execution.errors import ExecutionError, LocalExecutionRecordNotFoundError

        assert issubclass(LocalExecutionRecordNotFoundError, ExecutionError)

        async def scenario() -> None:
            service, http, store = _service(db_path=tmp_path / "s.db")
            with pytest.raises(LocalExecutionRecordNotFoundError):
                await service.reconcile(context_id="does-not-exist")
            store.close()

        run_async(scenario())


class TestRestartRecovery:
    def test_restart_after_acknowledged_reconciles_without_resubmit(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()

            # "restart": yeni store + yeni client + yeni service, AYNI db_path
            service2, http2, store2 = _service(
                get_responses=[_query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile(context_id="ctx-1")
            assert reconciled.lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http2.post_calls) == 0  # restart SONRASI kesinlikle YENİ order YOK
            store2.close()

        run_async(scenario())

    def test_restart_with_unknown_not_found_is_swept_by_reconcile_pending_no_post(self, tmp_path) -> None:
        """BLOCKER FİX (Karar 78) regresyonu: `UNKNOWN_NOT_FOUND`,
        `NEEDS_RECONCILIATION_STATES` İÇİNDEDİR — restart sonrası
        `reconcile_pending()` bu kaydı DA süpürür, YENİDEN sorgular, ASLA
        yeni bir POST YAPMAZ."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),
                ],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()

            persisted = ExecutionStateStore(db_path)
            pre_restart = persisted.load_by_context_id("ctx-1")
            assert pre_restart.lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND
            persisted.close()

            # "restart": yeni store + yeni client + yeni service, AYNI db_path
            service2, http2, store2 = _service(
                get_responses=[(400, '{"code": -2013, "msg": "Order does not exist."}')],
                db_path=db_path,
            )
            reconciled = await service2.reconcile_pending()
            assert len(reconciled) == 1
            assert reconciled[0].lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND  # HÂLÂ çözülmemiş
            assert len(http2.post_calls) == 0  # KESİNLİKLE yeni order YOK
            store2.close()

        run_async(scenario())

    def test_restart_with_unknown_not_found_later_resolves_to_filled_no_duplicate(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),
                ],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()

            service2, http2, store2 = _service(
                get_responses=[_query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile_pending()
            assert len(reconciled) == 1
            assert reconciled[0].lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http2.post_calls) == 0  # restart SONRASI kesinlikle YENİ order YOK
            store2.close()

        run_async(scenario())

    def test_restart_after_partially_filled_preserves_progress(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="PARTIALLY_FILLED", executedQty="0.004", cummulativeQuoteQty="200.0")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()

            service2, http2, store2 = _service(
                get_responses=[_query_response(status="PARTIALLY_FILLED", executedQty="0.007", cummulativeQuoteQty="350.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile(context_id="ctx-1")
            assert reconciled.lifecycle_state == ExecutionLifecycleState.PARTIALLY_FILLED
            assert reconciled.executed_quantity == 0.007
            store2.close()

        run_async(scenario())

    def test_restart_with_exchange_already_filled(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()

            service2, http2, store2 = _service(
                get_responses=[_query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile(context_id="ctx-1")
            assert reconciled.lifecycle_state == ExecutionLifecycleState.FILLED
            store2.close()

        run_async(scenario())

    def test_restart_after_ambiguous_timeout_reconciles_on_pending_sweep(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[ExecutionTransportError("connection reset")],
                db_path=db_path,
            )
            # reconciliation'ın KENDİSİ DE bu turda başarısız olsun (process
            # tam da ambiguous işaretlendikten hemen sonra çöktü varsayımı):
            http1._get_responses.append(ExecutionTransportError("connection reset"))  # noqa: SLF001
            with pytest.raises(ExecutionTransportError):
                await service1.submit(_intent())
            store1.close()

            persisted = ExecutionStateStore(db_path)
            mid_crash_state = persisted.load_by_context_id("ctx-1")
            assert mid_crash_state.lifecycle_state == ExecutionLifecycleState.AMBIGUOUS
            persisted.close()

            service2, http2, store2 = _service(
                get_responses=[_query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile_pending()
            assert len(reconciled) == 1
            assert reconciled[0].lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http2.post_calls) == 0
            store2.close()

        run_async(scenario())

    def test_restart_before_final_reconciliation_persistence_still_recoverable(self, tmp_path) -> None:
        """Restart sonrası PENDING (henüz reconcile edilmemiş) bir kayıt,
        `reconcile_pending()` süpürmesiyle GERÇEKTEN çözülür."""

        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0")],
                db_path=db_path,
            )
            await service1.submit(_intent())
            store1.close()  # HİÇ manuel reconcile ÇAĞRILMADI — restart SİMÜLE edilir

            service2, http2, store2 = _service(
                get_responses=[_query_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=db_path,
            )
            reconciled = await service2.reconcile_pending()
            assert len(reconciled) == 1
            assert reconciled[0].lifecycle_state == ExecutionLifecycleState.FILLED
            store2.close()

        run_async(scenario())


class TestContradictionProtection:
    def test_query_reporting_decreased_executed_quantity_raises(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    _query_response(status="PARTIALLY_FILLED", executedQty="0.001", cummulativeQuoteQty="50.0"),
                ],
                post_responses=[_order_response(status="PARTIALLY_FILLED", executedQty="0.006", cummulativeQuoteQty="300.0")],
                db_path=tmp_path / "s.db",
            )
            await service.submit(_intent())
            with pytest.raises(ReconciliationContradictionError):
                await service.reconcile(context_id="ctx-1")
            store.close()

        run_async(scenario())

    def test_query_reporting_filled_regressing_to_new_raises(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response(status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0")],
                db_path=tmp_path / "s.db",
            )
            record = await service.submit(_intent())
            assert record.is_terminal()
            # zaten terminal olduğundan reconcile() sorgulamaya bile GİTMEZ:
            reconciled_again = await service.reconcile(context_id="ctx-1")
            assert reconciled_again.lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http.get_calls) == 1  # yalnızca exchangeInfo, hiçbir query_order çağrısı EKLENMEDİ
            store.close()

        run_async(scenario())

    def test_query_reporting_different_exchange_order_id_raises(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info()), _query_response(status="NEW", orderId=999)],
                post_responses=[_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0", orderId=1)],
                db_path=tmp_path / "s.db",
            )
            await service.submit(_intent())
            with pytest.raises(ReconciliationContradictionError):
                await service.reconcile(context_id="ctx-1")
            store.close()

        run_async(scenario())


class TestQueryFailureModes:
    def test_malformed_query_response_propagates(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=tmp_path / "s.db",
            )
            http._get_responses.append((200, "not json{{"))  # noqa: SLF001
            with pytest.raises(MalformedResponseError):
                await service.submit(_intent())
            store.close()

        run_async(scenario())

    def test_binance_query_rejection_propagates(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=tmp_path / "s.db",
            )
            http._get_responses.append((400, '{"code": -1100, "msg": "Illegal characters"}'))  # noqa: SLF001
            with pytest.raises(BinanceRejectionError):
                await service.submit(_intent())
            store.close()

        run_async(scenario())

    def test_query_timeout_propagates(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[ExecutionTimeoutError("timed out")],
                db_path=tmp_path / "s.db",
            )
            http._get_responses.append(ExecutionTimeoutError("query timed out"))  # noqa: SLF001
            with pytest.raises(ExecutionTimeoutError):
                await service.submit(_intent())
            store.close()

        run_async(scenario())


class _FlakyExecutionStore:
    """Bir sonraki N `save()` çağrısını deterministik olarak BAŞARISIZ
    kılan test double'ı (Faz 7'nin kendi `_FlakyStore`/`FaultyPaperStateStore`
    ilkesinin AYNISI) — gerçek SQLite yaşam döngüsünü karmaşıklaştırmadan
    "exchange kabul etti AMA yerel persistence başarısız oldu" senaryosunu
    deterministik test eder."""

    def __init__(self, db_path) -> None:
        self._inner = ExecutionStateStore(db_path)
        self._fail_next = 0

    def fail_next_saves(self, count: int) -> None:
        self._fail_next += count

    def save(self, record) -> None:
        if self._fail_next > 0:
            self._fail_next -= 1
            raise PersistenceError("simulated execution-store save failure (Faz 11 fault injection)")
        self._inner.save(record)

    def load_by_context_id(self, context_id):
        return self._inner.load_by_context_id(context_id)

    def load_by_client_order_id(self, client_order_id):
        return self._inner.load_by_client_order_id(client_order_id)

    def list_for_symbol(self, symbol):
        return self._inner.list_for_symbol(symbol)

    def list_needing_reconciliation(self):
        return self._inner.list_needing_reconciliation()

    def close(self) -> None:
        self._inner.close()


class TestPersistenceFailureHandling:
    def test_persistence_failure_before_submission_prevents_post(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())],
                db_path=tmp_path / "s.db", store_factory=_FlakyExecutionStore,
            )
            store.fail_next_saves(1)  # İLK save (SUBMISSION_ATTEMPTED, POST'tan ÖNCE) başarısız olsun
            with pytest.raises(ExecutionPersistenceError) as excinfo:
                await service.submit(_intent())
            assert len(http.post_calls) == 0  # order KESİNLİKLE gönderilmedi (fail-closed)
            # H3 fix (mainnet-readiness review): nothing reached the
            # exchange, so this MUST be flagged safe-to-retry.
            assert excinfo.value.exchange_may_have_accepted_order is False
            store.close()

        run_async(scenario())

    def test_persistence_failure_after_exchange_ack_surfaces_explicit_error(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=tmp_path / "s.db", store_factory=_FlakyExecutionStore,
            )
            # İLK (pre-submission, POST'tan ÖNCE) save BAŞARILI olsun — yalnızca
            # İKİNCİ (post-submission, exchange ACK'ten SONRA) save'i başarısız kıl:
            original_save = store.save
            calls = {"n": 0}

            def flaky_second_save(record):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise PersistenceError("simulated post-ack persistence failure")
                return original_save(record)

            store.save = flaky_second_save
            with pytest.raises(ExecutionPersistenceError) as excinfo:
                await service.submit(_intent())
            # exchange GERÇEKTEN order'ı kabul ETTİ (POST yapıldı) — bu,
            # "dağıtık transaction taklidi" YAPILMADIĞININ kanıtıdır.
            assert len(http.post_calls) == 1
            # H3 fix (mainnet-readiness review): the exchange DID accept
            # this order — callers (lifecycle_manager.attempt_exit,
            # signal_bridge._submit) MUST be able to tell this apart from
            # the safe pre-submission case and refuse to silently retry.
            assert excinfo.value.exchange_may_have_accepted_order is True
            assert excinfo.value.client_order_id == _intent().client_order_id
            store.close()

        run_async(scenario())

    def test_row_from_pre_submission_checkpoint_survives_post_ack_persistence_failure(self, tmp_path) -> None:
        """Post-ACK persist'i başarısız olsa BİLE, submission-ÖNCESİ satır
        zaten durable olduğundan, kimlik KAYBOLMAZ — bir sonraki
        `reconcile_pending()` süpürmesi durumu YİNE DE düzeltebilir."""

        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=db_path, store_factory=_FlakyExecutionStore,
            )
            original_save = store.save
            calls = {"n": 0}

            def flaky_second_save(record):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise PersistenceError("simulated post-ack persistence failure")
                return original_save(record)

            store.save = flaky_second_save
            with pytest.raises(ExecutionPersistenceError):
                await service.submit(_intent())
            store.close()

            recovery_store = ExecutionStateStore(db_path)
            surviving = recovery_store.load_by_context_id("ctx-1")
            assert surviving is not None
            assert surviving.client_order_id  # stabil kimlik KORUNDU
            recovery_store.close()

        run_async(scenario())


class TestIsolation:
    def test_multi_symbol_isolation(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info("BTCUSDT")), json_response(_exchange_info("ETHUSDT"))],
                post_responses=[
                    _order_response(symbol="BTCUSDT"),
                    _order_response(symbol="ETHUSDT", clientOrderId="csl-eth"),
                ],
                db_path=tmp_path / "s.db",
            )
            btc = await service.submit(_intent(symbol="BTCUSDT", context_id="ctx-btc"))
            eth = await service.submit(_intent(symbol="ETHUSDT", context_id="ctx-eth"))
            assert btc.symbol == "BTCUSDT"
            assert eth.symbol == "ETHUSDT"
            assert btc.client_order_id != eth.client_order_id
            assert store.list_for_symbol("BTCUSDT") == (btc,)
            assert store.list_for_symbol("ETHUSDT") == (eth,)
            store.close()

        run_async(scenario())

    def test_same_symbol_independent_context_isolation(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info()), json_response(_exchange_info())],
                post_responses=[
                    _order_response(clientOrderId="csl-a"),
                    _order_response(clientOrderId="csl-b", orderId=2),
                ],
                db_path=tmp_path / "s.db",
            )
            first = await service.submit(_intent(context_id="ctx-a", quantity=0.01))
            second = await service.submit(_intent(context_id="ctx-b", quantity=0.02))
            assert first.context_id != second.context_id
            assert first.client_order_id != second.client_order_id
            assert store.load_by_context_id("ctx-a") == first
            assert store.load_by_context_id("ctx-b") == second
            store.close()

        run_async(scenario())

    def test_unresolved_unknown_not_found_on_one_symbol_does_not_block_another(self, tmp_path) -> None:
        """BLOCKER FİX (Karar 78) regresyonu: BTCUSDT'nin ÇÖZÜLMEMİŞ
        (`UNKNOWN_NOT_FOUND`) kaydı, `reconcile_pending()`'in ETHUSDT'yi
        GERÇEKTEN reconcile etmesini ENGELLEMEZ — her sembol KENDİ
        `client_order_id`'si ile BAĞIMSIZ sorgulanır."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            service1, http1, store1 = _service(
                get_responses=[
                    json_response(_exchange_info("BTCUSDT")),
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),
                    json_response(_exchange_info("ETHUSDT")),
                ],
                post_responses=[
                    ExecutionTimeoutError("timed out"),
                    _order_response(symbol="ETHUSDT", clientOrderId="csl-eth", status="NEW", executedQty="0", cummulativeQuoteQty="0"),
                ],
                db_path=db_path,
            )
            await service1.submit(_intent(symbol="BTCUSDT", context_id="ctx-btc"))
            await service1.submit(_intent(symbol="ETHUSDT", context_id="ctx-eth"))
            store1.close()

            service2, http2, store2 = _service(
                get_responses=[
                    (400, '{"code": -2013, "msg": "Order does not exist."}'),  # BTCUSDT: HÂLÂ çözülmemiş
                    _query_response(symbol="ETHUSDT", status="FILLED", executedQty="0.01", cummulativeQuoteQty="500.0"),
                ],
                db_path=db_path,
            )
            reconciled = await service2.reconcile_pending()
            by_symbol = {r.symbol: r for r in reconciled}
            assert by_symbol["BTCUSDT"].lifecycle_state == ExecutionLifecycleState.UNKNOWN_NOT_FOUND
            assert by_symbol["ETHUSDT"].lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(http2.post_calls) == 0
            store2.close()

        run_async(scenario())


class TestCheckUsdtBalance:
    """Portfolio/Accounting v1, step 5 — the new, read-only balance-check
    path wired to the previously-dead `testnet_client.py::account_info()`."""

    def test_returns_usdt_balance_on_success(self, tmp_path) -> None:
        async def scenario():
            service, http, store = _service(
                get_responses=[
                    json_response({
                        "balances": [
                            {"asset": "USDT", "free": "1234.56", "locked": "10.0"},
                            {"asset": "BTC", "free": "0.01", "locked": "0.0"},
                        ],
                    }),
                ],
                db_path=tmp_path / "s.db",
            )
            balance = await service.check_usdt_balance()
            store.close()
            return balance

        balance = run_async(scenario())
        assert balance is not None
        assert balance.asset == "USDT"
        assert balance.free == 1234.56
        assert balance.locked == 10.0

    def test_returns_none_when_no_usdt_row_present(self, tmp_path) -> None:
        async def scenario():
            service, http, store = _service(
                get_responses=[json_response({"balances": [{"asset": "BTC", "free": "0.01", "locked": "0.0"}]})],
                db_path=tmp_path / "s.db",
            )
            balance = await service.check_usdt_balance()
            store.close()
            return balance

        assert run_async(scenario()) is None

    def test_raising_account_info_never_propagates_past_this_check(self, tmp_path) -> None:
        """THE required test: a raising `account_info()` (here, the
        transport itself failing) must NEVER propagate out of
        `check_usdt_balance()` -- it degrades to `None`."""
        async def scenario():
            service, http, store = _service(get_responses=[], db_path=tmp_path / "s.db")
            balance = await service.check_usdt_balance()  # must not raise
            store.close()
            return balance

        assert run_async(scenario()) is None

    def test_malformed_response_degrades_to_none(self, tmp_path) -> None:
        async def scenario():
            service, http, store = _service(
                get_responses=[json_response({"balances": [{"asset": "USDT", "free": "1.0"}]})],  # missing "locked"
                db_path=tmp_path / "s.db",
            )
            balance = await service.check_usdt_balance()  # must not raise
            store.close()
            return balance

        assert run_async(scenario()) is None

    def test_check_never_writes_to_the_execution_store(self, tmp_path) -> None:
        """Observability-only: this must never persist anything -- a
        purely read-only exchange call."""
        async def scenario():
            service, http, store = _service(
                get_responses=[json_response({"balances": [{"asset": "USDT", "free": "1.0", "locked": "0.0"}]})],
                db_path=tmp_path / "s.db",
            )
            await service.check_usdt_balance()
            pending = store.list_needing_reconciliation()
            store.close()
            return pending

        assert run_async(scenario()) == ()


class TestNoCredentialPersistence:
    def test_persisted_record_never_contains_secret_or_key_substring(self, tmp_path) -> None:
        async def scenario() -> None:
            service, http, store = _service(
                get_responses=[json_response(_exchange_info())], post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            await service.submit(_intent())
            persisted = store.load_by_context_id("ctx-1")
            rendered = repr(persisted)
            assert FAKE_KEY not in rendered
            assert FAKE_SECRET not in rendered
            store.close()

        run_async(scenario())


=== FILE: tests/test_execution_reconciliation_store.py ===
"""Faz 11 — execution/reconciliation_store.py testleri: SQLite persistence,
schema versioning, corruption safety, no credential storage."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, new_record, transition
from crypto_signal_engine.execution.reconciliation_store import SCHEMA_VERSION, ExecutionStateStore
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


class TestSaveAndLoad:
    def test_round_trip_by_context_id(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        record = new_record(_intent(), now=NOW)
        store.save(record)
        loaded = store.load_by_context_id("ctx-1")
        assert loaded == record
        store.close()

    def test_round_trip_by_client_order_id(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        record = new_record(_intent(), now=NOW)
        store.save(record)
        loaded = store.load_by_client_order_id(record.client_order_id)
        assert loaded == record
        store.close()

    def test_missing_record_returns_none(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        assert store.load_by_context_id("does-not-exist") is None
        store.close()

    def test_upsert_updates_existing_row(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        record = new_record(_intent(), now=NOW)
        store.save(record)
        updated = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        store.save(updated)
        loaded = store.load_by_context_id("ctx-1")
        assert loaded.lifecycle_state == ExecutionLifecycleState.SUBMISSION_ATTEMPTED
        store.close()

    def test_no_api_key_secret_or_signature_columns(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        columns = {
            row[1].lower()
            for row in store._connection.execute("PRAGMA table_info(execution_record)").fetchall()  # noqa: SLF001
        }
        forbidden_fragments = ["key", "secret", "signature", "credential", "password"]
        violations = [c for c in columns if any(fragment in c for fragment in forbidden_fragments)]
        assert violations == []
        store.close()


class TestListing:
    def test_list_for_symbol_isolated(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        store.save(new_record(_intent(symbol="BTCUSDT", context_id="ctx-btc"), now=NOW))
        store.save(new_record(_intent(symbol="ETHUSDT", context_id="ctx-eth"), now=NOW))
        btc_records = store.list_for_symbol("BTCUSDT")
        assert len(btc_records) == 1
        assert btc_records[0].symbol == "BTCUSDT"
        store.close()

    def test_list_needing_reconciliation_excludes_terminal(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        pending = transition(
            new_record(_intent(context_id="ctx-pending"), now=NOW),
            new_state=ExecutionLifecycleState.AMBIGUOUS, now=NOW,
        )
        done = transition(
            new_record(_intent(context_id="ctx-done"), now=NOW),
            new_state=ExecutionLifecycleState.REJECTED, now=NOW,
        )
        store.save(pending)
        store.save(done)
        needing = store.list_needing_reconciliation()
        assert {r.context_id for r in needing} == {"ctx-pending"}
        store.close()


class TestSchemaVersioning:
    def test_schema_version_written_on_first_open(self, tmp_path) -> None:
        db_path = tmp_path / "s.db"
        store = ExecutionStateStore(db_path)
        version = store._connection.execute("SELECT version FROM schema_version").fetchone()[0]  # noqa: SLF001
        assert version == SCHEMA_VERSION
        store.close()

    def test_mismatched_schema_version_raises(self, tmp_path) -> None:
        db_path = tmp_path / "s.db"
        store = ExecutionStateStore(db_path)
        store._connection.execute("UPDATE schema_version SET version = 999")  # noqa: SLF001
        store._connection.commit()  # noqa: SLF001
        store.close()

        with pytest.raises(SchemaVersionMismatchError):
            ExecutionStateStore(db_path)

    def test_corrupt_row_raises_rather_than_silently_skipped(self, tmp_path) -> None:
        db_path = tmp_path / "s.db"
        store = ExecutionStateStore(db_path)
        store.save(new_record(_intent(), now=NOW))
        store._connection.execute("UPDATE execution_record SET side = 'GARBAGE'")  # noqa: SLF001
        store._connection.commit()  # noqa: SLF001

        with pytest.raises(CorruptRecordError):
            store.load_by_context_id("ctx-1")
        store.close()


=== FILE: tests/test_execution_signer.py ===
"""Faz 10 — execution/signer.py testleri: deterministik HMAC-SHA256
imzalama, kanonik parametre kodlama. Ağ GEREKMEZ."""

from __future__ import annotations

import hashlib
import hmac
from collections import OrderedDict

import pytest

from crypto_signal_engine.execution.signer import BinanceTestnetSigner, canonical_query_string


class TestCanonicalQueryString:
    def test_preserves_insertion_order(self) -> None:
        params = OrderedDict([("symbol", "BTCUSDT"), ("side", "BUY"), ("type", "MARKET")])
        assert canonical_query_string(params) == "symbol=BTCUSDT&side=BUY&type=MARKET"

    def test_url_encodes_special_characters(self) -> None:
        params = OrderedDict([("newClientOrderId", "csl-abc+def")])
        encoded = canonical_query_string(params)
        assert "+" not in encoded or "%2B" in encoded  # urlencode escapes '+'


class TestBinanceTestnetSigner:
    def test_deterministic_known_vector(self) -> None:
        """Bilinen, yerel olarak inşa edilmiş bir vektör — stdlib `hmac`
        ile BAĞIMSIZ olarak hesaplanan beklenen imza ile karşılaştırılır."""
        secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
        query = "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=1&timestamp=1499827319559"
        expected = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

        signer = BinanceTestnetSigner(secret)
        assert signer.sign(query) == expected

    def test_same_input_same_signature(self) -> None:
        signer = BinanceTestnetSigner("supersecret")
        assert signer.sign("a=1&b=2") == signer.sign("a=1&b=2")

    def test_different_query_different_signature(self) -> None:
        signer = BinanceTestnetSigner("supersecret")
        assert signer.sign("a=1&b=2") != signer.sign("a=1&b=3")

    def test_different_secret_different_signature(self) -> None:
        assert BinanceTestnetSigner("secret-a").sign("a=1") != BinanceTestnetSigner("secret-b").sign("a=1")

    def test_signature_never_contains_secret_substring(self) -> None:
        secret = "my-super-secret-value-12345"
        signature = BinanceTestnetSigner(secret).sign("a=1&b=2")
        assert secret not in signature

    def test_empty_secret_rejected(self) -> None:
        with pytest.raises(ValueError):
            BinanceTestnetSigner("   ")

    def test_repr_never_exposes_secret(self) -> None:
        signer = BinanceTestnetSigner("top-secret-value")
        assert "top-secret-value" not in repr(signer)


=== FILE: tests/test_execution_testnet_client.py ===
"""Faz 10 — execution/testnet_client.py testleri: host allowlist (BLOCKER-
seviyesi), sinyalli GET/POST istek inşası, hata çevirisi, credential
redaction. Tamamen offline — `FakeTestnetHttpClient` kullanılır."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

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
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType, TimeInForce
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig, validate_testnet_host
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-testnet-api-key"
FAKE_SECRET = "fake-testnet-api-secret-value"  # noqa: S105 - test-only fake credential


def _market_intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.001,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


class TestTestnetHostAllowlist:
    def test_accepts_the_approved_testnet_host(self) -> None:
        validate_testnet_host("https://testnet.binance.vision")  # raise etmemeli

    def test_rejects_mainnet_host(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://api.binance.com")

    def test_rejects_mainnet_alias_used_elsewhere_in_repo(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://stream.binance.com:9443")

    def test_rejects_arbitrary_https_host(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://example.com")

    def test_rejects_non_tls_host(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("http://testnet.binance.vision")

    def test_rejects_lookalike_subdomain_suffix(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://testnet.binance.vision.attacker.com")

    def test_rejects_lookalike_subdomain_prefix(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://attacker-testnet.binance.vision")

    def test_rejects_host_in_path_not_hostname(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://attacker.com/testnet.binance.vision")

    def test_rejects_userinfo_trick(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            validate_testnet_host("https://testnet.binance.vision@attacker.com")

    def test_rejects_case_variation_is_still_checked_correctly(self) -> None:
        # urlsplit().hostname lowercases automatically; this must still
        # resolve to the approved host, not silently bypass validation.
        validate_testnet_host("https://TESTNET.BINANCE.VISION")

    def test_config_construction_enforces_allowlist(self) -> None:
        with pytest.raises(UnsafeExecutionHostError):
            BinanceTestnetConfig(base_url="https://api.binance.com", api_key=FAKE_KEY, api_secret=FAKE_SECRET)


class TestConfigValidation:
    def test_credentials_are_optional_at_construction(self) -> None:
        config = BinanceTestnetConfig()
        assert config.has_credentials() is False

    def test_recv_window_bounds_enforced(self) -> None:
        with pytest.raises(ExecutionConfigError):
            BinanceTestnetConfig(recv_window_ms=0)
        with pytest.raises(ExecutionConfigError):
            BinanceTestnetConfig(recv_window_ms=70_000)

    def test_negative_timeout_rejected(self) -> None:
        with pytest.raises(ExecutionConfigError):
            BinanceTestnetConfig(request_timeout_seconds=-1.0)

    def test_repr_redacts_credentials(self) -> None:
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        rendered = repr(config)
        assert FAKE_KEY not in rendered
        assert FAKE_SECRET not in rendered
        assert "SET" in rendered


class TestMissingCredentials:
    def _client(self, get_responses=None, post_responses=None, **config_kwargs) -> BinanceTestnetClient:
        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(**config_kwargs)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW))

    def test_account_info_requires_credentials(self) -> None:
        async def scenario() -> None:
            client = self._client()
            with pytest.raises(MissingCredentialsError):
                await client.account_info()

        run_async(scenario())

    def test_place_order_requires_credentials(self) -> None:
        async def scenario() -> None:
            client = self._client()
            with pytest.raises(MissingCredentialsError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_exchange_info_does_not_require_credentials(self) -> None:
        async def scenario() -> None:
            client = self._client(
                get_responses=[json_response({"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "filters": []}]})]
            )
            payload = await client.exchange_info("BTCUSDT")
            assert payload["symbols"][0]["symbol"] == "BTCUSDT"

        run_async(scenario())

    def test_symbol_price_does_not_require_credentials(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT", "price": "50000.0"})])
            price = await client.symbol_price("BTCUSDT")
            assert price == 50000.0

        run_async(scenario())


class TestSymbolPrice:
    """BLOCKER FİX (Karar 73) — `BinanceTestnetClient.symbol_price()`
    doğrudan client-seviyesi testleri (adapter-seviyesi fail-closed
    testleri `test_execution_adapter.py::TestMarketPriceLookupFailClosed`'da)."""

    def _client(self, get_responses=None) -> BinanceTestnetClient:
        http = FakeTestnetHttpClient(get_responses)
        config = BinanceTestnetConfig()
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW))

    def test_returns_finite_positive_price_for_matching_symbol(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT", "price": "50123.45"})])
            assert await client.symbol_price("BTCUSDT") == 50123.45

        run_async(scenario())

    def test_rejects_mismatched_symbol_in_response(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "ETHUSDT", "price": "3000.0"})])
            with pytest.raises(MarketPriceUnavailableError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_rejects_zero_price(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT", "price": "0"})])
            with pytest.raises(MarketPriceUnavailableError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_rejects_negative_price(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT", "price": "-5.0"})])
            with pytest.raises(MarketPriceUnavailableError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_rejects_non_finite_price(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT", "price": "Infinity"})])
            with pytest.raises(MarketPriceUnavailableError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_malformed_json_raises(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[(200, "not json{{")])
            with pytest.raises(MalformedResponseError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_missing_price_field_raises_malformed(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[json_response({"symbol": "BTCUSDT"})])
            with pytest.raises(MalformedResponseError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())

    def test_transport_failure_propagates(self) -> None:
        async def scenario() -> None:
            client = self._client(get_responses=[ExecutionTransportError("connection reset")])
            with pytest.raises(ExecutionTransportError):
                await client.symbol_price("BTCUSDT")

        run_async(scenario())


class TestSignedRequestBuilding:
    def _client(self, get_responses=None, post_responses=None) -> BinanceTestnetClient:
        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW)), http

    def test_market_buy_request_contains_expected_params(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
                            "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            result = await client.place_order(_market_intent(side=OrderSide.BUY))
            assert result.side is OrderSide.BUY
            url, params, headers = http.post_calls[0]
            assert url.endswith("/api/v3/order")
            assert params["side"] == "BUY"
            assert params["type"] == "MARKET"
            assert params["symbol"] == "BTCUSDT"
            assert "signature" in params
            assert headers["X-MBX-APIKEY"] == FAKE_KEY

        run_async(scenario())

    def test_market_sell_request_contains_expected_params(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 2, "side": "SELL",
                            "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            result = await client.place_order(_market_intent(side=OrderSide.SELL))
            assert result.side is OrderSide.SELL
            _, params, _ = http.post_calls[0]
            assert params["side"] == "SELL"

        run_async(scenario())

    def test_limit_request_contains_price_and_time_in_force(self) -> None:
        async def scenario() -> None:
            intent = OrderIntent(
                symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
                context_id="ctx-1", timestamp=NOW, quantity=0.001, price=50000.0,
                time_in_force=TimeInForce.GTC,
            )
            client, http = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": intent.client_order_id, "orderId": 3,
                            "side": "BUY", "status": "NEW", "executedQty": "0.0", "cummulativeQuoteQty": "0.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            await client.place_order(intent)
            _, params, _ = http.post_calls[0]
            assert params["type"] == "LIMIT"
            assert params["price"] == "50000"
            assert params["timeInForce"] == "GTC"
            assert params["newClientOrderId"] == intent.client_order_id

        run_async(scenario())

    def test_signature_is_last_signed_param_and_not_reused_verbatim_as_secret(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
                            "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            await client.place_order(_market_intent())
            _, params, _ = http.post_calls[0]
            assert FAKE_SECRET not in params["signature"]
            assert FAKE_SECRET not in str(params)

        run_async(scenario())


class TestQueryOrder:
    """Faz 11 — `BinanceTestnetClient.query_order()` client-seviyesi
    testleri (reconciliation service-seviyesi entegrasyon testleri
    `test_execution_reconciliation_service.py`'dedir)."""

    def _client(self, get_responses=None) -> BinanceTestnetClient:
        http = FakeTestnetHttpClient(get_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW)), http

    def test_requires_credentials(self) -> None:
        async def scenario() -> None:
            http = FakeTestnetHttpClient()
            config = BinanceTestnetConfig()
            client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
            with pytest.raises(MissingCredentialsError):
                await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")

        run_async(scenario())

    def test_found_order_returns_execution_result_with_supplied_context_id(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                get_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 5, "side": "BUY",
                            "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
                            "updateTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            result = await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")
            assert result.exchange_order_id == 5
            assert result.context_id == "ctx-1"
            _, params, headers = http.get_calls[0]
            assert params["origClientOrderId"] == "csl-abc"
            assert "signature" in params
            assert headers["X-MBX-APIKEY"] == FAKE_KEY

        run_async(scenario())

    def test_order_not_found_raises_order_not_found_error(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                get_responses=[(400, '{"code": -2013, "msg": "Order does not exist."}')]
            )
            with pytest.raises(OrderNotFoundError):
                await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")

        run_async(scenario())

    def test_other_rejection_raises_binance_rejection_error(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                get_responses=[(400, '{"code": -1100, "msg": "Illegal characters"}')]
            )
            with pytest.raises(BinanceRejectionError):
                await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")

        run_async(scenario())

    def test_malformed_response_raises(self) -> None:
        async def scenario() -> None:
            client, http = self._client(get_responses=[(200, "not json{{")])
            with pytest.raises(MalformedResponseError):
                await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")

        run_async(scenario())

    def test_missing_fields_raises_malformed(self) -> None:
        async def scenario() -> None:
            client, http = self._client(get_responses=[json_response({"symbol": "BTCUSDT"})])
            with pytest.raises(MalformedResponseError):
                await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")

        run_async(scenario())

    def test_signature_never_leaked_in_params_string(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                get_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 5, "side": "BUY",
                            "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
                            "updateTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            await client.query_order("BTCUSDT", client_order_id="csl-abc", context_id="ctx-1")
            _, params, _ = http.get_calls[0]
            assert FAKE_SECRET not in str(params)

        run_async(scenario())


class TestErrorTranslation:
    def _client(self, get_responses=None, post_responses=None) -> BinanceTestnetClient:
        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW)), http

    def test_malformed_json_response_raises(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(post_responses=[(200, "not json{{")])
            with pytest.raises(MalformedResponseError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_missing_required_field_raises_malformed(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(post_responses=[json_response({"symbol": "BTCUSDT"})])
            with pytest.raises(MalformedResponseError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_binance_rejection_raises_with_code_no_secret(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(
                post_responses=[json_response({"code": -2010, "msg": "Account has insufficient balance"}, status_code=400)]
            )
            with pytest.raises(BinanceRejectionError) as exc_info:
                await client.place_order(_market_intent())
            assert exc_info.value.binance_code == -2010
            assert FAKE_SECRET not in str(exc_info.value)

        run_async(scenario())

    def test_timestamp_rejection_raises_specific_error(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(
                post_responses=[json_response({"code": -1021, "msg": "Timestamp outside recvWindow"}, status_code=400)]
            )
            with pytest.raises(TimestampRejectedError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_transport_failure_raises_execution_transport_error(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(post_responses=[ExecutionTransportError("connection reset")])
            with pytest.raises(ExecutionTransportError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_timeout_raises_execution_timeout_error(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(post_responses=[ExecutionTimeoutError("timed out")])
            with pytest.raises(ExecutionTimeoutError):
                await client.place_order(_market_intent())

        run_async(scenario())

    def test_non_dict_json_payload_raises_malformed(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(post_responses=[json_response([1, 2, 3])])
            with pytest.raises(MalformedResponseError):
                await client.place_order(_market_intent())

        run_async(scenario())


class TestFillsParsing:
    """Autonomous-Testnet-lifecycle Phase 0-C/5 — fee/commission capture
    never existed before; these are the first tests proving `fills[]` is
    actually parsed from a MARKET order's FULL response into
    `ExecutionResult.fills`."""

    def _client(self, post_responses=None, get_responses=None) -> tuple[BinanceTestnetClient, FakeTestnetHttpClient]:
        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW)), http

    def test_place_order_parses_fills_with_base_asset_commission(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
                            "status": "FILLED", "executedQty": "0.002", "cummulativeQuoteQty": "100.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                            "fills": [
                                {
                                    "price": "50000.00", "qty": "0.001", "commission": "0.0000010",
                                    "commissionAsset": "BTC", "tradeId": 11,
                                },
                                {
                                    "price": "50000.00", "qty": "0.001", "commission": "0.0000010",
                                    "commissionAsset": "BTC", "tradeId": 12,
                                },
                            ],
                        }
                    )
                ]
            )
            result = await client.place_order(_market_intent(side=OrderSide.BUY))
            assert len(result.fills) == 2
            assert result.fills[0].commission_asset == "BTC"
            assert result.fills[0].trade_id == 11
            assert result.fills[0].price == 50000.00

        run_async(scenario())

    def test_place_order_without_fills_field_defaults_to_empty_tuple(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
                            "status": "NEW", "executedQty": "0.0", "cummulativeQuoteQty": "0.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    )
                ]
            )
            result = await client.place_order(_market_intent(side=OrderSide.BUY))
            assert result.fills == ()

        run_async(scenario())

    def test_malformed_fill_entry_raises_malformed_response(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(
                post_responses=[
                    json_response(
                        {
                            "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
                            "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                            "fills": [{"price": "50000.00"}],  # missing qty/commission/commissionAsset
                        }
                    )
                ]
            )
            with pytest.raises(MalformedResponseError):
                await client.place_order(_market_intent(side=OrderSide.BUY))

        run_async(scenario())


class TestMyTrades:
    """Backfill path for reconciliation-recovered fills / legacy migration
    (Phase 0-C/4/17) — a `query_order()` GET never carries `fills[]`."""

    def _client(self, get_responses=None) -> tuple[BinanceTestnetClient, FakeTestnetHttpClient]:
        http = FakeTestnetHttpClient(get_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        return BinanceTestnetClient(config, http, clock=FixedClock(NOW)), http

    def test_my_trades_parses_trade_list(self) -> None:
        async def scenario() -> None:
            client, http = self._client(
                get_responses=[
                    json_response(
                        [
                            {
                                "symbol": "BTCUSDT", "id": 501, "orderId": 42, "price": "50000.0",
                                "qty": "0.001", "commission": "0.000001", "commissionAsset": "BTC",
                            }
                        ]
                    )
                ]
            )
            fills = await client.my_trades("BTCUSDT", order_id=42)
            assert len(fills) == 1
            assert fills[0].trade_id == 501
            assert fills[0].commission_asset == "BTC"
            url, params, _ = http.get_calls[0]
            assert url.endswith("/api/v3/myTrades")
            assert params["orderId"] == "42"

        run_async(scenario())

    def test_my_trades_requires_credentials(self) -> None:
        async def scenario() -> None:
            http = FakeTestnetHttpClient()
            config = BinanceTestnetConfig()
            client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
            with pytest.raises(MissingCredentialsError):
                await client.my_trades("BTCUSDT", order_id=42)

        run_async(scenario())

    def test_my_trades_malformed_payload_raises(self) -> None:
        async def scenario() -> None:
            client, _ = self._client(get_responses=[json_response({"not": "a list"})])
            with pytest.raises(MalformedResponseError):
                await client.my_trades("BTCUSDT", order_id=42)

        run_async(scenario())


=== FILE: tests/test_feature_domain.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import FeatureValidationError
from crypto_signal_engine.features.domain import FeatureIdentity, FeatureSnapshot, FeatureValue

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_identity(**overrides) -> FeatureIdentity:
    defaults = dict(symbol="BTCUSDT", timeframe=Timeframe.M1, feature_name="SMA_20")
    defaults.update(overrides)
    return FeatureIdentity(**defaults)


class TestFeatureIdentity:
    def test_valid_identity(self) -> None:
        identity = make_identity()
        assert identity.symbol == "BTCUSDT"

    def test_symbol_normalized(self) -> None:
        identity = make_identity(symbol="btcusdt")
        assert identity.symbol == "BTCUSDT"

    def test_string_timeframe_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_identity(timeframe="1m")

    def test_empty_feature_name_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="feature_name"):
            make_identity(feature_name="  ")

    def test_non_string_feature_name_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="feature_name"):
            make_identity(feature_name=123)  # type: ignore[arg-type]


class TestFeatureValue:
    def test_valid_value(self) -> None:
        fv = FeatureValue(identity=make_identity(), as_of=T0, value=42.0)
        assert fv.value == 42.0
        assert fv.symbol == "BTCUSDT"
        assert fv.feature_name == "SMA_20"

    def test_naive_as_of_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            FeatureValue(identity=make_identity(), as_of=datetime(2026, 8, 31), value=1.0)

    def test_nan_value_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureValue(identity=make_identity(), as_of=T0, value=float("nan"))

    def test_inf_value_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureValue(identity=make_identity(), as_of=T0, value=float("inf"))

    def test_wrong_identity_type_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="FeatureIdentity"):
            FeatureValue(identity="not-an-identity", as_of=T0, value=1.0)  # type: ignore[arg-type]


class TestFeatureSnapshot:
    def test_valid_snapshot(self) -> None:
        snap = FeatureSnapshot(symbol="btcusdt", timeframe=Timeframe.M1, as_of=T0, values={"SMA_20": 100.0})
        assert snap.symbol == "BTCUSDT"
        assert snap.get("SMA_20") == 100.0
        assert "SMA_20" in snap
        assert snap.get("MISSING") is None

    def test_naive_as_of_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=datetime(2026, 8, 31), values={})

    def test_nan_in_values_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={"X": float("nan")})

    def test_inf_in_values_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={"X": float("inf")})

    def test_string_timeframe_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            FeatureSnapshot(symbol="BTCUSDT", timeframe="1m", as_of=T0, values={})  # type: ignore[arg-type]

    def test_values_immutable(self) -> None:
        snap = FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={"X": 1.0})
        with pytest.raises(TypeError):
            snap.values["X"] = 999.0  # type: ignore[index]

    def test_original_dict_mutation_does_not_leak(self) -> None:
        source = {"X": 1.0}
        snap = FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values=source)
        source["X"] = 999.0
        assert snap.values["X"] == 1.0

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="context_id"):
            FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={}, context_id="  ")


class TestFeatureSnapshotFromFeatureValues:
    def _value(self, name="SMA_20", symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, value=1.0) -> FeatureValue:
        identity = FeatureIdentity(symbol=symbol, timeframe=timeframe, feature_name=name)
        return FeatureValue(identity=identity, as_of=as_of, value=value)

    def test_valid_construction(self) -> None:
        snap = FeatureSnapshot.from_feature_values(
            "BTCUSDT", Timeframe.M1, T0, (self._value("SMA_20"), self._value("EMA_20", value=2.0))
        )
        assert snap.get("SMA_20") == 1.0
        assert snap.get("EMA_20") == 2.0

    def test_mixed_symbol_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="mixed-symbol"):
            FeatureSnapshot.from_feature_values(
                "BTCUSDT", Timeframe.M1, T0, (self._value(symbol="ETHUSDT"),)
            )

    def test_mixed_timeframe_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="mixed-timeframe"):
            FeatureSnapshot.from_feature_values(
                "BTCUSDT", Timeframe.M1, T0, (self._value(timeframe=Timeframe.M5),)
            )

    def test_future_feature_value_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="gelecekten"):
            FeatureSnapshot.from_feature_values(
                "BTCUSDT", Timeframe.M1, T0, (self._value(as_of=T0 + timedelta(seconds=1)),)
            )

    def test_as_of_exactly_equal_accepted(self) -> None:
        snap = FeatureSnapshot.from_feature_values("BTCUSDT", Timeframe.M1, T0, (self._value(as_of=T0),))
        assert snap.get("SMA_20") == 1.0

    def test_duplicate_feature_name_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="duplicate feature"):
            FeatureSnapshot.from_feature_values(
                "BTCUSDT", Timeframe.M1, T0,
                (self._value("SMA_20", value=1.0), self._value("SMA_20", value=2.0)),
            )

    def test_empty_feature_values_produces_empty_snapshot(self) -> None:
        snap = FeatureSnapshot.from_feature_values("BTCUSDT", Timeframe.M1, T0, ())
        assert dict(snap.values) == {}


=== FILE: tests/test_feature_engine.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade
from crypto_signal_engine.errors import FeatureValidationError
from crypto_signal_engine.features.engine import FeatureEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_candles(n: int, closes=None, is_closed=True, symbol="BTCUSDT", timeframe=Timeframe.M1) -> list[Candle]:
    closes = closes or [100.0 + i * 0.5 for i in range(n)]
    candles = []
    for i in range(n):
        open_time = T0 + timedelta(minutes=i)
        c = closes[i]
        candles.append(Candle(
            symbol=symbol, timeframe=timeframe, open_time=open_time,
            close_time=open_time + timedelta(minutes=1),
            open=c, high=c + 1.0, low=c - 1.0, close=c, volume=10.0,
            is_closed=is_closed, trade_count=1,
        ))
    return candles


class TestClosedCandlePolicy:
    def test_open_candle_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25, is_closed=False)
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])

    def test_closed_candles_accepted(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])
        assert "SMA_20" in snap


class TestMixedSymbolTimeframeRejected:
    def test_mixed_symbol_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(21, symbol="BTCUSDT") + make_candles(1, symbol="ETHUSDT")
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="mixed-symbol"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])

    def test_mixed_timeframe_rejected(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(21, timeframe=Timeframe.M1) + make_candles(1, timeframe=Timeframe.M5)
        as_of = candles[-1].close_time
        with pytest.raises(FeatureValidationError, match="mixed-timeframe"):
            engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])


class TestInsufficientHistoryOmission:
    def test_feature_omitted_when_insufficient_history(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(5)  # SMA_20 için yetersiz
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["SMA_20"])
        assert "SMA_20" not in snap
        assert dict(snap.values) == {}

    def test_partial_snapshot_with_mixed_sufficient_insufficient(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(5)
        as_of = candles[-1].close_time
        snap = engine.compute_candle_features(
            candles, "BTCUSDT", Timeframe.M1, as_of, feature_names=["BODY_SIZE", "SMA_20"]
        )
        assert "BODY_SIZE" in snap  # min_history=1, yeterli
        assert "SMA_20" not in snap  # min_history=20, yetersiz


class TestNoLookAheadAdversarial:
    """Bölüm 17 — HARD ACCEPTANCE GATE: T anındaki feature, T'den sonraki
    veri var olsun ya da olmasın AYNI olmalı."""

    def test_future_open_candle_ignored_without_exception(self) -> None:
        """KRİTİK regresyon (no-look-ahead blocker düzeltmesi): T anında
        SMA_20, dataset T'de bitse de T'den SONRAKİ AÇIK (henüz kapanmamış)
        bir candle içerse de AYNI olmalı ve HİÇBİR exception fırlatılmamalı.

        Önceki hatalı implementasyon, `close_time > as_of` filtresinden ÖNCE
        `is_closed` kontrolü yaptığından, ufkun DIŞINDAKİ (gelecekteki) açık
        bir candle'ı yanlışlıkla closed-candle politikası ihlali olarak
        reddediyordu — bu tamamen sahte bir hataydı (o candle henüz
        oluşmaktadır, bu NORMAL ve BEKLENEN bir durumdur).
        """
        engine = FeatureEngine()
        closed_candles = make_candles(20)  # T = candles[19].close_time
        t = closed_candles[-1].close_time

        # A) dataset T'de bitiyor (yalnızca 20 kapalı candle)
        snap_a = engine.compute_candle_features(closed_candles, "BTCUSDT", Timeframe.M1, t, feature_names=["SMA_20"])

        # B) AYNI 20 kapalı candle + T'den SONRAKİ, HENÜZ KAPANMAMIŞ bir candle.
        future_open_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=t, close_time=t + timedelta(minutes=1),
            open=999.0, high=1000.0, low=998.0, close=999.5, volume=1.0, is_closed=False,
        )
        dataset_b = closed_candles + [future_open_candle]
        snap_b = engine.compute_candle_features(dataset_b, "BTCUSDT", Timeframe.M1, t, feature_names=["SMA_20"])

        assert snap_a.get("SMA_20") == snap_b.get("SMA_20")
        assert snap_b.get("SMA_20") == pytest.approx(sum(c.close for c in closed_candles) / 20)

    def test_unfinished_candle_within_horizon_still_rejected(self) -> None:
        """Regresyona karşı koruma: değerlendirme ufkunun İÇİNDE
        (`close_time <= as_of`) kalan, henüz kapanmamış bir candle HÂLÂ
        reddedilmelidir — yalnızca ufkun DIŞINDAKİ açık candle'lar
        sessizce atlanır."""
        engine = FeatureEngine()
        closed_candles = make_candles(20)
        unfinished_within_horizon = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=closed_candles[-1].open_time, close_time=closed_candles[-1].close_time,
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=False,
        )
        as_of = unfinished_within_horizon.close_time  # ufkun İÇİNDE (close_time <= as_of)
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(
                closed_candles[:-1] + [unfinished_within_horizon], "BTCUSDT", Timeframe.M1, as_of,
                feature_names=["SMA_20"],
            )

    def test_ema_at_t_identical_with_or_without_future_candles(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of_t = candles[19].close_time  # 20. candle'ın (index 19) close_time'ı

        # A) dataset T'de bitiyor
        dataset_a = candles[:20]
        snap_a = engine.compute_candle_features(dataset_a, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["EMA_20"])

        # B) T'den sonraki candle'lar da storage'da mevcut
        dataset_b = candles  # tüm 25 candle
        snap_b = engine.compute_candle_features(dataset_b, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["EMA_20"])

        assert snap_a.get("EMA_20") == snap_b.get("EMA_20")

    def test_rsi_at_t_unaffected_by_future_candles(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(30)
        as_of_t = candles[19].close_time

        snap_a = engine.compute_candle_features(candles[:20], "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["RSI_14"])
        snap_b = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["RSI_14"])
        assert snap_a.get("RSI_14") == snap_b.get("RSI_14")

    def test_rolling_high_at_t_unaffected_by_future_candles(self) -> None:
        engine = FeatureEngine()
        # Gelecekteki candle'lar çok daha yüksek high'a sahip — eğer look-ahead
        # olsaydı bu, T anındaki highest_high'ı DEĞİŞTİRİRDİ.
        closes = [100.0 + i * 0.1 for i in range(20)] + [500.0] * 10
        candles = make_candles(30, closes=closes)
        as_of_t = candles[19].close_time

        snap_a = engine.compute_candle_features(candles[:20], "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["HIGHEST_HIGH_20"])
        snap_b = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["HIGHEST_HIGH_20"])
        assert snap_a.get("HIGHEST_HIGH_20") == snap_b.get("HIGHEST_HIGH_20")
        assert snap_b.get("HIGHEST_HIGH_20") < 500.0  # gelecekteki sıçrama SIZMADI

    def test_future_candles_in_list_are_silently_excluded_not_erroring(self) -> None:
        engine = FeatureEngine()
        candles = make_candles(25)
        as_of_t = candles[19].close_time
        # 25 candle veriliyor ama as_of yalnızca ilk 20'sini kapsıyor.
        snap = engine.compute_candle_features(candles, "BTCUSDT", Timeframe.M1, as_of_t, feature_names=["SMA_20"])
        expected = sum(c.close for c in candles[:20]) / 20
        assert snap.get("SMA_20") == pytest.approx(expected)

    def test_cross_timeframe_snapshot_as_of_never_returns_future(self) -> None:
        engine = FeatureEngine()
        candles_1h = make_candles(25, timeframe=Timeframe.H1)
        as_of_early = candles_1h[19].close_time
        as_of_late = candles_1h[24].close_time

        snap_early = engine.compute_candle_features(candles_1h[:20], "BTCUSDT", Timeframe.H1, as_of_early, feature_names=["SMA_20"])
        engine.commit_snapshot(snap_early)

        snap_late = engine.compute_candle_features(candles_1h[:25], "BTCUSDT", Timeframe.H1, as_of_late, feature_names=["SMA_20"])
        engine.commit_snapshot(snap_late)

        # 1h evaluation zamanı early'ye eşit bir 5m değerlendirme yapılıyor
        # varsayımıyla: snapshot_as_of(as_of_early) yalnızca early'yi (ya da
        # ondan ÖNCEKİ) döndürmeli, ASLA late'i değil.
        result = engine.snapshot_as_of("BTCUSDT", Timeframe.H1, as_of_early)
        assert result.as_of == as_of_early
        assert result.get("SMA_20") == snap_early.get("SMA_20")


class TestMultiTimeframeIsolation:
    def test_four_timeframes_independent_histories(self) -> None:
        engine = FeatureEngine()
        for tf in (Timeframe.H1, Timeframe.M15, Timeframe.M5, Timeframe.M1):
            candles = make_candles(25, timeframe=tf, closes=[100.0 + i for i in range(25)])
            as_of = candles[-1].close_time
            snap = engine.compute_candle_features(candles, "BTCUSDT", tf, as_of, feature_names=["SMA_20"])
            engine.commit_snapshot(snap)

        for tf in (Timeframe.H1, Timeframe.M15, Timeframe.M5, Timeframe.M1):
            latest = engine.latest_snapshot("BTCUSDT", tf)
            assert latest is not None
            assert latest.timeframe == tf

    def test_unfinished_higher_timeframe_candle_not_exposed(self) -> None:
        """Bir üst-timeframe candle'ı henüz kapanmadıysa (is_closed=False),
        FeatureEngine bunu candle listesine dahil edilse bile REDDEDER
        (closed-candle policy) — kapanmamış bir 1h candle asla kapalıymış
        gibi kullanılamaz."""
        engine = FeatureEngine()
        candles = make_candles(20, timeframe=Timeframe.H1)
        unfinished = make_candles(1, timeframe=Timeframe.H1, is_closed=False)
        unfinished[0] = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.H1,
            open_time=candles[-1].open_time + timedelta(hours=1),
            close_time=candles[-1].open_time + timedelta(hours=2),
            open=100.0, high=101.0, low=99.0, close=100.0, volume=1.0, is_closed=False,
        )
        with pytest.raises(FeatureValidationError, match="açık"):
            engine.compute_candle_features(
                candles + unfinished, "BTCUSDT", Timeframe.H1,
                unfinished[0].close_time, feature_names=["SMA_20"],
            )


class TestOrderBookFeaturesViaEngine:
    def test_compute_order_book_features(self) -> None:
        engine = FeatureEngine()
        book = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=T0,
            bids=(OrderBookLevel(price=99.0, quantity=1.0),),
            asks=(OrderBookLevel(price=101.0, quantity=1.0),),
            last_update_id=1,
        )
        snap = engine.compute_order_book_features(book, feature_names=["MID_PRICE", "SPREAD_ABS"])
        assert snap.get("MID_PRICE") == pytest.approx(100.0)
        assert snap.get("SPREAD_ABS") == pytest.approx(2.0)
        assert snap.as_of == T0


class TestTradeFeaturesViaEngine:
    def test_compute_trade_features_with_future_trade_excluded(self) -> None:
        engine = FeatureEngine()
        trades = [
            Trade(symbol="BTCUSDT", trade_id=1, price=100.0, quantity=1.0, timestamp=T0, is_buyer_maker=False),
            Trade(symbol="BTCUSDT", trade_id=2, price=100.0, quantity=5.0, timestamp=T0 + timedelta(minutes=10), is_buyer_maker=False),
        ]
        as_of = T0  # ikinci (gelecekteki) trade dışlanmalı
        snap = engine.compute_trade_features(trades, "BTCUSDT", Timeframe.M1, as_of, feature_names=["BUY_VOLUME"])
        assert snap.get("BUY_VOLUME") == pytest.approx(1.0)  # yalnızca ilk trade


=== FILE: tests/test_feature_registry.py ===
import pytest

from crypto_signal_engine.errors import FeatureValidationError
from crypto_signal_engine.features.registry import (
    FeatureConfig,
    FeatureRegistry,
    FeatureSourceType,
    default_candle_feature_registry,
    default_order_book_feature_registry,
    default_trade_feature_registry,
)


class TestFeatureConfig:
    def test_valid_config(self) -> None:
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20}, min_history=20)
        assert config.params["period"] == 20

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="name"):
            FeatureConfig(name="  ", source=FeatureSourceType.CANDLE, calculator_key="sma")

    def test_string_source_rejected(self) -> None:
        with pytest.raises(TypeError, match="FeatureSourceType"):
            FeatureConfig(name="X", source="CANDLE", calculator_key="sma")  # type: ignore[arg-type]

    def test_zero_min_history_rejected(self) -> None:
        with pytest.raises(FeatureValidationError, match="min_history"):
            FeatureConfig(name="X", source=FeatureSourceType.CANDLE, calculator_key="sma", min_history=0)

    def test_params_immutable(self) -> None:
        config = FeatureConfig(name="X", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        with pytest.raises(TypeError):
            config.params["period"] = 999  # type: ignore[index]


class TestFeatureRegistry:
    def test_register_and_get(self) -> None:
        reg = FeatureRegistry()
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        reg.register(config)
        assert reg.get("SMA_20") is config
        assert "SMA_20" in reg

    def test_unknown_feature_rejected(self) -> None:
        reg = FeatureRegistry()
        with pytest.raises(FeatureValidationError, match="bilinmeyen feature"):
            reg.get("NOPE")

    def test_identical_reregistration_is_idempotent(self) -> None:
        reg = FeatureRegistry()
        config = FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20})
        reg.register(config)
        reg.register(config)  # aynı config -> no-op
        assert len(reg.all()) == 1

    def test_conflicting_params_same_name_rejected(self) -> None:
        reg = FeatureRegistry()
        reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 20}))
        with pytest.raises(FeatureValidationError, match="çakışması"):
            reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma", params={"period": 50}))

    def test_by_source_filters_correctly(self) -> None:
        reg = FeatureRegistry()
        reg.register(FeatureConfig(name="SMA_20", source=FeatureSourceType.CANDLE, calculator_key="sma"))
        reg.register(FeatureConfig(name="SPREAD_ABS", source=FeatureSourceType.ORDER_BOOK, calculator_key="spread_abs"))
        candle_configs = reg.by_source(FeatureSourceType.CANDLE)
        assert len(candle_configs) == 1
        assert candle_configs[0].name == "SMA_20"


class TestDefaultRegistries:
    def test_default_candle_registry_has_no_collisions(self) -> None:
        reg = default_candle_feature_registry()
        names = [c.name for c in reg.all()]
        assert len(names) == len(set(names))
        assert "SMA_20" in reg
        assert "EMA_20" in reg
        assert "RSI_14" in reg
        assert "ATR_14" in reg

    def test_default_order_book_registry(self) -> None:
        reg = default_order_book_feature_registry(depth=10)
        assert "BID_DEPTH_10" in reg
        assert "DEPTH_IMBALANCE_10" in reg

    def test_default_trade_registry(self) -> None:
        reg = default_trade_feature_registry(window_size=50)
        assert "BUY_VOLUME" in reg
        assert "TRADE_INTENSITY_50" in reg

    def test_different_depth_produces_different_identity(self) -> None:
        reg5 = default_order_book_feature_registry(depth=5)
        reg10 = default_order_book_feature_registry(depth=10)
        assert "BID_DEPTH_5" in reg5
        assert "BID_DEPTH_5" not in reg10
        assert "BID_DEPTH_10" in reg10


=== FILE: tests/test_feature_state.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import FeatureStateError
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_snapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values=None) -> FeatureSnapshot:
    return FeatureSnapshot(symbol=symbol, timeframe=timeframe, as_of=as_of, values=values or {"X": 1.0})


class TestCommitAndLatest:
    def test_commit_and_latest(self) -> None:
        store = FeatureHistoryStore()
        snap = make_snapshot()
        store.commit(snap)
        assert store.latest("BTCUSDT", Timeframe.M1) is snap

    def test_latest_unknown_returns_none(self) -> None:
        store = FeatureHistoryStore()
        assert store.latest("BTCUSDT", Timeframe.M1) is None

    def test_symbol_normalized_lookup(self) -> None:
        store = FeatureHistoryStore()
        snap = make_snapshot()
        store.commit(snap)
        assert store.latest("btcusdt", Timeframe.M1) is snap


class TestOlderSnapshotRejected:
    def test_older_as_of_rejected(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5)))
        with pytest.raises(FeatureStateError, match="eski"):
            store.commit(make_snapshot(as_of=T0))

    def test_canonical_state_unchanged_after_rejection(self) -> None:
        store = FeatureHistoryStore()
        newer = make_snapshot(as_of=T0 + timedelta(minutes=5), values={"X": 999.0})
        store.commit(newer)
        with pytest.raises(FeatureStateError):
            store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 999.0


class TestSameTimestampUpsert:
    def test_same_as_of_upserts(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(values={"X": 1.0}))
        store.commit(make_snapshot(values={"X": 2.0}))  # aynı as_of, farklı değer
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 2.0
        assert store.history_length("BTCUSDT", Timeframe.M1) == 1  # duplicate satır YOK


class TestSymbolTimeframeIsolation:
    def test_different_symbols_independent(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(symbol="BTCUSDT", values={"X": 1.0}))
        store.commit(make_snapshot(symbol="ETHUSDT", values={"X": 2.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 1.0
        assert store.latest("ETHUSDT", Timeframe.M1).get("X") == 2.0

    def test_different_timeframes_independent(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(timeframe=Timeframe.M1, values={"X": 1.0}))
        store.commit(make_snapshot(timeframe=Timeframe.M5, values={"X": 2.0}))
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 1.0
        assert store.latest("BTCUSDT", Timeframe.M5).get("X") == 2.0


class TestAsOfLookup:
    def test_as_of_returns_most_recent_not_exceeding_timestamp(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5), values={"X": 2.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=10), values={"X": 3.0}))

        result = store.as_of("BTCUSDT", Timeframe.M1, T0 + timedelta(minutes=7))
        assert result.get("X") == 2.0  # 5dk'lık snapshot, 10dk'lık DEĞİL

    def test_as_of_before_any_snapshot_returns_none(self) -> None:
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=5)))
        result = store.as_of("BTCUSDT", Timeframe.M1, T0)
        assert result is None

    def test_as_of_never_returns_future_snapshot(self) -> None:
        """KRİTİK no-look-ahead testi: `timestamp`'tan SONRAKİ bir snapshot
        ASLA döndürülmemeli."""
        store = FeatureHistoryStore()
        store.commit(make_snapshot(as_of=T0, values={"X": 1.0}))
        store.commit(make_snapshot(as_of=T0 + timedelta(minutes=100), values={"X": 999.0}))
        result = store.as_of("BTCUSDT", Timeframe.M1, T0 + timedelta(minutes=1))
        assert result.get("X") == 1.0
        assert result.as_of <= T0 + timedelta(minutes=1)

    def test_as_of_naive_timestamp_rejected(self) -> None:
        store = FeatureHistoryStore()
        with pytest.raises(ValueError, match="naive datetime"):
            store.as_of("BTCUSDT", Timeframe.M1, datetime(2026, 8, 31))


class TestBoundedRetention:
    def test_bounded_retention_evicts_oldest(self) -> None:
        store = FeatureHistoryStore(max_history=3)
        for i in range(5):
            store.commit(make_snapshot(as_of=T0 + timedelta(minutes=i), values={"X": float(i)}))
        assert store.history_length("BTCUSDT", Timeframe.M1) == 3
        assert store.latest("BTCUSDT", Timeframe.M1).get("X") == 4.0
        # en eski (0, 1) silinmiş olmalı -> as_of=T0'a sorgulandığında None dönmeli
        assert store.as_of("BTCUSDT", Timeframe.M1, T0) is None

    def test_invalid_max_history_rejected(self) -> None:
        with pytest.raises(FeatureStateError, match="max_history"):
            FeatureHistoryStore(max_history=0)


=== FILE: tests/test_gap_and_atomic_checkpoint_remediation.py ===
"""
Faz 7 pre-audit remediation — regression tests for two independently
confirmed defects:

BLOCKER-1: `PersistedRuntime._consume_candles()` used to resolve a
detected gap via the BARE `RuntimeCoordinator.resolve_gap()`, whose final
step (`ingest_candle()` on the recovered pending candle) could produce a
real Paper transition — but control returned to `_consume_candles()`
without that transition ever being durably checkpointed (only the candle
checkpoint advanced). A restart after such a gap-triggered trade silently
and permanently lost it, because bootstrap/recovery intentionally never
re-evaluates historical candles.

HIGH-1: the normal (non-gap) ingestion path persisted the candle
checkpoint and the associated Paper-state transition in two independent
SQLite transactions. A crash/write-failure between them could leave the
candle checkpoint durably advanced while the Paper transition it caused
was never persisted.

Both are fixed by (a) `PaperStateStore.checkpoint_candle_transition()`,
which commits the candle checkpoint and (if present) its Paper transition
in ONE atomic SQLite transaction, and (b) routing gap-resolution's final
`ingest_candle()` call through `PersistedRuntime`'s own (checkpointing)
`ingest_candle()`/`resolve_gap()` instead of the bare coordinator's.

These tests exercise the REAL `RuntimeCoordinator`/`PaperTradingEngine`/
`PaperStateStore`/`PersistedRuntime` stack (no mocking of signal
evaluation) against a temporary SQLite database, using a strong,
deterministic uptrend candle series + a bid-imbalanced order book to
reliably produce a genuine (non-NEUTRAL) directional trade through the
full Phase 3/4/5/6 pipeline.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at

UTC = timezone.utc
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
WARMUP = 20
_UPTREND_STEP = 3.0
_M5 = Timeframe.M5


def _strong_bid_order_book(symbol: str, timestamp: datetime, *, last_update_id: int = 1) -> OrderBookSnapshot:
    """Heavily bid-weighted order book — combined with a strong candle
    uptrend, this reliably produces a directional (non-NEUTRAL) signal
    through the real Phase 3/4 feature/agent/consensus pipeline
    (verified: STRONG_LONG, RSI_14=100, ROC_10 strongly positive across
    all timeframes) — no mocked/injected Signal objects are used anywhere
    in this file."""
    return OrderBookSnapshot(
        symbol=symbol, timestamp=timestamp,
        bids=(OrderBookLevel(price=99.5, quantity=500.0), OrderBookLevel(price=99.0, quantity=500.0)),
        asks=(OrderBookLevel(price=100.5, quantity=5.0), OrderBookLevel(price=101.0, quantity=5.0)),
        last_update_id=last_update_id,
    )


def _bootstrap_strong_uptrend(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in (Timeframe.M5, Timeframe.M15, Timeframe.H1):
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP, base_price=100.0, step=_UPTREND_STEP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready
    coordinator.ingest_order_book(symbol, _strong_bid_order_book(symbol, end))


def _uptrend_price(steps_past_warmup_end: int) -> float:
    # last warmup candle price is 100 + step*(WARMUP-1); continue the SAME
    # monotonic uptrend for candles arriving after warmup.
    return 100.0 + _UPTREND_STEP * (WARMUP - 1 + steps_past_warmup_end)


def _gap_candles(symbol: str) -> tuple[list, "object"]:
    """Two genuinely missing M5 candles (continuing the uptrend) plus a
    live pending candle arriving 3 intervals ahead — the exact shape a
    reconnect-induced gap produces."""
    missing = [
        make_candle(symbol, _M5, END, price=_uptrend_price(1)),
        make_candle(symbol, _M5, END + timedelta(minutes=5), price=_uptrend_price(2)),
    ]
    pending = make_candle(symbol, _M5, END + timedelta(minutes=10), price=_uptrend_price(3))
    return missing, pending


class TestBlocker1GapResolutionDurability:
    """BLOCKER-1: gap resolution's Paper transition must receive the SAME
    durable guarantees as a normal accepted M5 transition, and restart
    must reconstruct exactly the same state an uninterrupted process
    would have reached — no double evaluation, no lost ledger entries."""

    def test_gap_resolution_produces_durable_restart_equivalent_trade(self, tmp_path) -> None:
        async def scenario() -> None:
            symbol = "BTCUSDT"

            # -- 1. REFERENCE: uninterrupted process, bare Phase 6 coordinator,
            #    NO persistence involved at all. This is the ground-truth for
            #    "what should the final state be after this exact gap is
            #    resolved" (bare RuntimeCoordinator.resolve_gap is accepted,
            #    unmodified Phase 6 behavior).
            missing, pending = _gap_candles(symbol)
            ref_provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing})
            ref_engine = PaperTradingEngine(notional_per_position=1000.0)
            ref_coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=ref_provider, warmup_candles=WARMUP, paper_engine=ref_engine
            )
            _bootstrap_strong_uptrend(ref_coordinator, symbol, END)

            ref_gap_event = ref_coordinator.ingest_candle(symbol, _M5, pending)
            assert ref_gap_event.outcome is IngestOutcome.GAP_DETECTED

            ref_resolved = await ref_coordinator.resolve_gap(symbol, _M5, pending)
            assert ref_resolved.outcome is IngestOutcome.ACCEPTED
            assert ref_resolved.cycle_result is not None
            assert ref_resolved.cycle_result.evaluated is True
            assert ref_resolved.cycle_result.signal.direction is not SignalDirection.NEUTRAL  # (1) genuine trade
            assert len(ref_resolved.cycle_result.paper_result.orders) == 1
            assert len(ref_resolved.cycle_result.paper_result.fills) == 1

            reference_position = ref_engine.position(symbol)
            reference_fills = ref_engine.fills(symbol)
            reference_orders = ref_engine.orders(symbol)
            reference_context_ids = set(ref_engine._states[symbol].processed_context_ids)  # noqa: SLF001
            reference_entry_fee = ref_engine._states[symbol].entry_fee  # noqa: SLF001

            # -- 2. GAP + PERSISTENCE path: identical scenario, but driven
            #    through PersistedRuntime (the real production entry point).
            missing2, pending2 = _gap_candles(symbol)  # fresh, structurally identical candle objects
            db_path = tmp_path / "state.db"
            gap_provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing2})
            store1 = PaperStateStore(db_path)
            engine1 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator1 = RuntimeCoordinator(
                symbols=(symbol,), provider=gap_provider, warmup_candles=WARMUP, paper_engine=engine1
            )
            runtime1 = PersistedRuntime(coordinator1, store1)
            _bootstrap_strong_uptrend(coordinator1, symbol, END)

            gap_event = runtime1.ingest_candle(symbol, _M5, pending2)
            assert gap_event.outcome is IngestOutcome.GAP_DETECTED

            resolved = await runtime1.resolve_gap(symbol, _M5, pending2)  # BLOCKER-1 fix under test
            assert resolved.outcome is IngestOutcome.ACCEPTED
            assert resolved.cycle_result is not None and resolved.cycle_result.evaluated is True
            assert resolved.cycle_result.signal.direction == ref_resolved.cycle_result.signal.direction

            # in-memory state matches the reference exactly
            assert engine1.position(symbol) == reference_position
            assert engine1.fills(symbol) == reference_fills
            assert engine1.orders(symbol) == reference_orders

            # (2) paper state is durably checkpointed, (7) candle checkpoint
            # is consistent with the paper transition — both written by the
            # SAME atomic call (checkpoint_candle_transition).
            assert store1.load_candle_checkpoint(symbol, _M5) == pending2.open_time
            durable = store1.load_paper_state(symbol)
            assert durable is not None
            assert durable.position == reference_position

            # (3) processed context identity is durable
            assert set(durable.processed_context_ids) == reference_context_ids

            # (4) generated orders are durable, (5) generated fills are durable
            assert [o.order_id for o in durable.orders] == [o.order_id for o in reference_orders]
            assert [f.fill_id for f in durable.fills] == [f.fill_id for f in reference_fills]

            # (6) fees / realized PnL / entry fee / position are durable
            assert durable.entry_fee == reference_entry_fee
            assert durable.position.realized_pnl == reference_position.realized_pnl

            assert _health_of(coordinator1, symbol) is RuntimeHealth.READY  # no false DEGRADED
            store1.close()

            # -- 3. RESTART: brand-new engine/store/coordinator/runtime,
            #    recover from the SAME durable SQLite database.
            full_m5 = (
                make_candle_series_ending_at(symbol, _M5, END, WARMUP, base_price=100.0, step=_UPTREND_STEP)
                + missing2 + [pending2]
            )
            recover_provider = FakeLiveDataProvider(
                historical_candles={
                    (symbol, _M5): full_m5,
                    (symbol, Timeframe.M15): make_candle_series_ending_at(
                        symbol, Timeframe.M15, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                    (symbol, Timeframe.H1): make_candle_series_ending_at(
                        symbol, Timeframe.H1, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                }
            )
            store2 = PaperStateStore(db_path)
            engine2 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator2 = RuntimeCoordinator(
                symbols=(symbol,), provider=recover_provider, warmup_candles=WARMUP, paper_engine=engine2
            )
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready  # (9, part 1) plenty of history — genuinely READY, not fabricated

            # (8) restart restores the same state as uninterrupted execution
            assert engine2.position(symbol) == reference_position
            assert engine2.fills(symbol) == reference_fills
            assert engine2.orders(symbol) == reference_orders
            assert set(engine2._states[symbol].processed_context_ids) == reference_context_ids  # noqa: SLF001
            assert engine2._states[symbol].entry_fee == reference_entry_fee  # noqa: SLF001

            # (9) the recovered historical candle is NOT evaluated twice,
            # (10) no duplicate trade occurs after restart
            assert len(engine2.fills(symbol)) == 1
            assert len(engine2.orders(symbol)) == 1
            assert len(engine2._states[symbol].processed_context_ids) == 1  # noqa: SLF001

            store2.close()

        run_async(scenario())

    def test_gap_resolution_health_stays_ready_no_stuck_gap_fault(self, tmp_path) -> None:
        """A resolved gap must not leave a phantom DEGRADED gap-fault once
        the checkpointed path is used (same Phase 6 clear_gap_fault
        semantics as the bare-coordinator path, unaffected by BLOCKER-1's
        fix)."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            missing, pending = _gap_candles(symbol)
            db_path = tmp_path / "state.db"
            provider = FakeLiveDataProvider(historical_candles={(symbol, _M5): missing})
            store = PaperStateStore(db_path)
            engine = PaperTradingEngine(notional_per_position=1000.0)
            coordinator = RuntimeCoordinator(symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine)
            runtime = PersistedRuntime(coordinator, store)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            gap_event = runtime.ingest_candle(symbol, _M5, pending)
            assert gap_event.outcome is IngestOutcome.GAP_DETECTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED

            resolved = await runtime.resolve_gap(symbol, _M5, pending)
            assert resolved.outcome is IngestOutcome.ACCEPTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.READY
            store.close()

        run_async(scenario())


class _FailAfterNCalls:
    """Wraps a REAL sqlite3 connection and raises on the Nth `execute()`
    call — used to prove HIGH-1's atomicity at the actual SQL boundary:
    an exception raised INSIDE the `with self._connection:` block must
    roll back everything written earlier in that same block, including
    statements the real connection already executed successfully."""

    def __init__(self, real: sqlite3.Connection, fail_on_call: int) -> None:
        self._real = real
        self._fail_on_call = fail_on_call
        self.calls = 0

    def execute(self, *args, **kwargs):
        self.calls += 1
        if self.calls == self._fail_on_call:
            raise sqlite3.OperationalError("simulated mid-transaction failure (HIGH-1 regression test)")
        return self._real.execute(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._real.executescript(*args, **kwargs)

    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._real.__exit__(exc_type, exc, tb)

    def close(self) -> None:
        self._real.close()


class TestHigh1AtomicCheckpoint:
    """HIGH-1: candle checkpoint + its associated Paper transition must
    commit atomically — a failure at any point must roll back the WHOLE
    combined write, never leave the candle checkpoint durably advanced
    while the Paper transition it caused is missing."""

    def test_failure_between_candle_and_paper_write_rolls_back_both(self, tmp_path) -> None:
        """(A) NORMAL PATH ATOMICITY — direct store-level proof: inject a
        failure AFTER the candle_checkpoint write has already executed
        against the real connection but BEFORE the paper_position write
        completes, and assert NOTHING survives (no candle checkpoint, no
        paper position, no processed context, no orders/fills)."""
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)

        from crypto_signal_engine.domain.enums import RiskLevel
        from crypto_signal_engine.domain.models import Signal
        from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide

        signal = Signal(
            symbol="BTCUSDT", timestamp=END, context_id="ctx-atomic-1", score=0.6, confidence=0.7,
            risk_level=RiskLevel.LOW, primary_timeframe=_M5, supporting_factors=(),
            contradicting_factors=(), invalidation=None, model_version="test-v1",
        )
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.LONG, quantity=1.0, average_entry_price=100.0,
            realized_pnl=0.0, updated_at=END,
        )
        order = PaperOrder(
            order_id="BTCUSDT:ctx-atomic-1:0", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=1.0, signal_context_id="ctx-atomic-1", created_at=END,
        )
        fill = PaperFill(
            fill_id="BTCUSDT:ctx-atomic-1:0:fill", order_id=order.order_id, symbol="BTCUSDT",
            side=OrderSide.BUY, quantity=1.0, price=100.0, fee=0.0, filled_at=END,
        )

        # 1st execute() = candle_checkpoint INSERT (succeeds against the
        # real connection, uncommitted). 2nd execute() = paper_position
        # INSERT — this is where we inject the failure.
        real_connection = store._connection  # noqa: SLF001
        store._connection = _FailAfterNCalls(real_connection, fail_on_call=2)  # noqa: SLF001

        with pytest.raises(PersistenceError):
            store.checkpoint_candle_transition(
                symbol="BTCUSDT", timeframe=_M5, candle_open_time=END,
                position=position, entry_fee=0.0, last_signal_timestamp=END,
                new_context_entries=[(signal.context_id, signal, position)],
                new_fills=(fill,), new_orders=(order,),
            )

        store._connection = real_connection  # noqa: SLF001 - restore for clean reads below

        assert store.load_candle_checkpoint("BTCUSDT", _M5) is None  # no advanced candle checkpoint survives
        assert store.load_paper_state("BTCUSDT") is None  # no partial paper transition/context/orders/fills survives
        store.close()

    def test_end_to_end_persistence_failure_leaves_no_partial_durable_state(self, tmp_path) -> None:
        """(A) NORMAL PATH ATOMICITY, exercised through the real production
        entry point (`PersistedRuntime.ingest_candle`) rather than the bare
        store — proves the wiring, not just the SQL. In-memory Phase 5/6
        state must still reflect the mutation (fail-closed does not roll
        back in-memory state, by accepted Phase 7 design) while durable
        state must show NOTHING from the failed candle."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            store = PaperStateStore(db_path)
            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine)
            runtime = PersistedRuntime(coordinator, store)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            checkpoint_before = store.load_candle_checkpoint(symbol, _M5)
            paper_state_before = store.load_paper_state(symbol)
            assert checkpoint_before is None
            assert paper_state_before is None

            store.close()  # force the next durable write to fail entirely (closed connection)

            candle = make_candle(symbol, _M5, END, price=_uptrend_price(1))
            event = runtime.ingest_candle(symbol, _M5, candle)

            # (F) FAILURE HEALTH: in-memory Phase 6 state is unaffected
            # (fail-closed does not retroactively undo Phase 5, by accepted
            # design) — the candle IS accepted in-memory...
            assert event.outcome is IngestOutcome.ACCEPTED
            # ...but health must correctly reflect the persistence failure,
            # never silently presenting in-memory/durable divergence as a
            # successful checkpoint.
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED

            # ...and NOTHING durable exists for this candle/transition —
            # not even the candle checkpoint alone (this is exactly the
            # non-atomic HIGH-1 hazard: with the store closed, the OLD
            # code could not have written either piece; the atomic store
            # method must reject the whole write up front, same result).
            reopened = PaperStateStore(db_path)
            assert reopened.load_candle_checkpoint(symbol, _M5) is None
            assert reopened.load_paper_state(symbol) is None
            reopened.close()

        run_async(scenario())

    # NOTE: the old `test_next_successful_checkpoint_clears_fault_and_backfills_nothing_partial`
    # test that lived here only asserted the LATER candle's checkpoint and
    # health — it did not compare the complete in-memory/durable ledger, so
    # it could not distinguish "the failed transition was correctly flushed
    # first" from "the failed transition was silently skipped/overtaken".
    # It has been replaced by `TestPendingUnsyncedTransitionBacklog` below
    # (SINGLE FINAL ACCEPTANCE BLOCKER fix), which asserts full ledger
    # equality (candle chronology, position, realized PnL, entry_fee,
    # last_signal_timestamp, ALL processed context ids, ALL orders, ALL
    # fills) and proves the originally-failed trade's order/fill/context
    # are present — not merely that health returned to READY.


class _FlakyTransitionStore:
    """Wraps a REAL `PaperStateStore`; fails the next N calls to
    `checkpoint_candle_transition` (one `PersistenceError` per call,
    consuming the counter), then delegates transparently. All other
    methods pass straight through — this double is scoped to exactly the
    single real call site `PersistedRuntime` uses for durable writes."""

    def __init__(self, inner: PaperStateStore, *, fail_next: int = 0) -> None:
        self._inner = inner
        self._fail_remaining = fail_next

    def checkpoint_candle_transition(self, **kwargs: object) -> None:
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise PersistenceError("simulated pending-unsynced fault injection")
        self._inner.checkpoint_candle_transition(**kwargs)

    def checkpoint_candle(self, *args: object, **kwargs: object) -> None:
        self._inner.checkpoint_candle(*args, **kwargs)

    def checkpoint_paper_state(self, **kwargs: object) -> None:
        self._inner.checkpoint_paper_state(**kwargs)

    def load_paper_state(self, *args: object, **kwargs: object):
        return self._inner.load_paper_state(*args, **kwargs)

    def load_candle_checkpoint(self, *args: object, **kwargs: object):
        return self._inner.load_candle_checkpoint(*args, **kwargs)

    def close(self) -> None:
        self._inner.close()


class TestPendingUnsyncedTransitionBacklog:
    """SINGLE FINAL ACCEPTANCE BLOCKER fix: a failed atomic
    candle+Paper-transition checkpoint must NOT be overtaken by a later
    candle's successful checkpoint. `PersistedRuntime` now holds the
    failed payload in an in-memory, per-(symbol, timeframe) FIFO backlog
    (`_pending`) and refuses to let a later checkpoint reach the store
    until every earlier backlog entry has been durably flushed (bkz.
    `PersistedRuntime._flush_pending`)."""

    def test_failed_transition_flushed_before_later_checkpoint_and_full_ledger_equality(self, tmp_path) -> None:
        """CRITICAL TEST (A-E scenario):

        A. a real M5 event opens a genuine Paper LONG position.
        B. its atomic checkpoint fails exactly once.
        C. health is DEGRADED and nothing durable was written.
        D. a later valid M5 event is submitted.
        E. persistence becomes available again (the flaky store stops
           failing) -> health returns to READY.

        After READY, in-memory and durable state must be FULLY equal —
        including the FIRST (originally-failed) trade's order/fill/
        processed-context entries, which the old (replaced) regression
        never checked. Also proves restart-equivalence and no duplicate
        trade after recovery of the originally-failed transition."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            real_store = PaperStateStore(db_path)
            flaky = _FlakyTransitionStore(real_store, fail_next=1)

            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine
            )
            runtime = PersistedRuntime(coordinator, flaky)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            # A + B: a genuine directional trade whose checkpoint fails once.
            first_candle = make_candle(symbol, _M5, END, price=_uptrend_price(1))
            first = runtime.ingest_candle(symbol, _M5, first_candle)
            assert first.outcome is IngestOutcome.ACCEPTED
            assert first.cycle_result is not None and first.cycle_result.evaluated is True
            assert first.cycle_result.signal.direction is not SignalDirection.NEUTRAL
            assert len(first.cycle_result.paper_result.orders) == 1
            assert len(first.cycle_result.paper_result.fills) == 1
            first_order_id = first.cycle_result.paper_result.orders[0].order_id
            first_fill_id = first.cycle_result.paper_result.fills[0].fill_id
            first_context_id = first.cycle_result.signal.context_id

            # C: DEGRADED, nothing durable yet — the failed transition is
            # held in the pending backlog, not discarded.
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
            assert real_store.load_candle_checkpoint(symbol, _M5) is None
            assert real_store.load_paper_state(symbol) is None
            assert runtime._pending[(symbol, _M5)]  # noqa: SLF001 - backlog holds the failed payload

            # D: a later valid M5 event (store already recovered by now).
            later_time = END + timedelta(minutes=5)
            later_candle = make_candle(symbol, _M5, later_time, price=_uptrend_price(2))
            second = runtime.ingest_candle(symbol, _M5, later_candle)
            assert second.outcome is IngestOutcome.ACCEPTED

            # E: persistence available again -> backlog fully flushed (the
            # FIRST payload first, in order) -> health READY.
            assert _health_of(coordinator, symbol) is RuntimeHealth.READY
            assert (symbol, _M5) not in runtime._pending  # noqa: SLF001 - backlog fully drained

            # FULL EQUALITY: candle checkpoint chronology.
            assert real_store.load_candle_checkpoint(symbol, _M5) == later_time

            durable = real_store.load_paper_state(symbol)
            assert durable is not None
            live_position = engine.position(symbol)
            live_fills = engine.fills(symbol)
            live_orders = engine.orders(symbol)
            live_state = engine._states[symbol]  # noqa: SLF001

            # current position / realized PnL / entry_fee / last_signal_timestamp
            assert durable.position == live_position
            assert durable.position.realized_pnl == live_position.realized_pnl
            assert durable.entry_fee == live_state.entry_fee
            assert durable.last_signal_timestamp == live_state.last_signal_timestamp

            # ALL processed context ids / ALL orders / ALL fills
            assert set(durable.processed_context_ids) == set(live_state.processed_context_ids)
            assert {o.order_id for o in durable.orders} == {o.order_id for o in live_orders}
            assert {f.fill_id for f in durable.fills} == {f.fill_id for f in live_fills}

            # Specifically: the FIRST (originally-failed) trade's order/
            # fill/context are present, not lost/skipped by the overtake.
            assert first_context_id in durable.processed_context_ids
            assert first_order_id in {o.order_id for o in durable.orders}
            assert first_fill_id in {f.fill_id for f in durable.fills}

            # -- RESTART: fresh engine/store/coordinator/runtime recovers
            #    from the SAME durable database.
            real_store.close()
            full_m5 = (
                make_candle_series_ending_at(symbol, _M5, END, WARMUP, base_price=100.0, step=_UPTREND_STEP)
                + [first_candle, later_candle]
            )
            recover_provider = FakeLiveDataProvider(
                historical_candles={
                    (symbol, _M5): full_m5,
                    (symbol, Timeframe.M15): make_candle_series_ending_at(
                        symbol, Timeframe.M15, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                    (symbol, Timeframe.H1): make_candle_series_ending_at(
                        symbol, Timeframe.H1, END, WARMUP, base_price=100.0, step=_UPTREND_STEP
                    ),
                }
            )
            store2 = PaperStateStore(db_path)
            engine2 = PaperTradingEngine(notional_per_position=1000.0)
            coordinator2 = RuntimeCoordinator(
                symbols=(symbol,), provider=recover_provider, warmup_candles=WARMUP, paper_engine=engine2
            )
            runtime2 = PersistedRuntime(coordinator2, store2)
            reports = await runtime2.recover()
            for report in reports:
                assert report.ready

            # restart restores exactly the uninterrupted in-memory state...
            assert engine2.position(symbol) == live_position
            assert engine2.fills(symbol) == live_fills
            assert engine2.orders(symbol) == live_orders
            assert set(engine2._states[symbol].processed_context_ids) == set(  # noqa: SLF001
                live_state.processed_context_ids
            )
            assert engine2._states[symbol].entry_fee == live_state.entry_fee  # noqa: SLF001

            # ...and contains the ORIGINALLY-FAILED trade's order/fill/context.
            assert first_context_id in engine2._states[symbol].processed_context_ids  # noqa: SLF001
            assert first_order_id in {o.order_id for o in engine2.orders(symbol)}
            assert first_fill_id in {f.fill_id for f in engine2.fills(symbol)}

            # no re-evaluation / no duplicate trade: exactly one order/fill
            # survived (the historical candle carrying the real trade is
            # never re-evaluated by recover()/bootstrap_candles()).
            assert len(engine2.orders(symbol)) == len(live_orders)
            assert len(engine2.fills(symbol)) == len(live_fills)

            store2.close()

        run_async(scenario())

    def test_repeated_persistence_failure_keeps_degraded_and_blocks_overtake(self, tmp_path) -> None:
        """If the store keeps failing across MULTIPLE checkpoint attempts,
        health must stay DEGRADED and NONE of the later candles' checkpoints
        may reach the durable store — the backlog grows, but nothing is
        ever silently allowed to overtake the still-unflushed first entry."""
        async def scenario() -> None:
            symbol = "BTCUSDT"
            db_path = tmp_path / "state.db"
            real_store = PaperStateStore(db_path)
            flaky = _FlakyTransitionStore(real_store, fail_next=1000)  # never succeeds in this test

            engine = PaperTradingEngine(notional_per_position=1000.0)
            provider = FakeLiveDataProvider()
            coordinator = RuntimeCoordinator(
                symbols=(symbol,), provider=provider, warmup_candles=WARMUP, paper_engine=engine
            )
            runtime = PersistedRuntime(coordinator, flaky)
            _bootstrap_strong_uptrend(coordinator, symbol, END)

            first = runtime.ingest_candle(symbol, _M5, make_candle(symbol, _M5, END, price=_uptrend_price(1)))
            assert first.outcome is IngestOutcome.ACCEPTED
            assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
            assert real_store.load_candle_checkpoint(symbol, _M5) is None

            for step in range(2, 5):
                candle_time = END + timedelta(minutes=5 * (step - 1))
                event = runtime.ingest_candle(
                    symbol, _M5, make_candle(symbol, _M5, candle_time, price=_uptrend_price(step))
                )
                assert event.outcome is IngestOutcome.ACCEPTED
                # every later candle is accepted in-memory (Phase 6 continuity
                # is unaffected), but repeated persistence failure means NONE
                # of them can overtake the still-unflushed first transition.
                assert _health_of(coordinator, symbol) is RuntimeHealth.DEGRADED
                assert real_store.load_candle_checkpoint(symbol, _M5) is None
                assert real_store.load_paper_state(symbol) is None

            # the backlog accumulated one entry per accepted M5 candle,
            # still ordered with the original failure first.
            assert len(runtime._pending[(symbol, _M5)]) == 4  # noqa: SLF001

            real_store.close()

        run_async(scenario())


def _health_of(coordinator: RuntimeCoordinator, symbol: str) -> RuntimeHealth:
    status = coordinator.status()
    return next(s for s in status.symbols if s.symbol == symbol).health


=== FILE: tests/test_immutability.py ===
"""
Quality Gate 4 — kapsamlı nested immutability regresyon testi.

Bu dosya, `@dataclass(frozen=True)` kullanımının TEK BAŞINA yeterli
olmadığını, nested mutable mapping'lerin ayrıca gerçek immutable bir
view'a sarılması gerektiğini kanıtlar. Her ilgili alan için hem
"construction sonrası mutation" hem "orijinal dict mutation sonrası sızma"
senaryoları test edilir.
"""

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.consensus import RiskAssessment
from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, FeatureVector
from crypto_signal_engine.quality.base import DataQualityResult
from crypto_signal_engine.domain.enums import DataQualityStatus
from crypto_signal_engine.safety.models import SafetyEvent, SafetyReasonCode, SafetySeverity

UTC = timezone.utc


@pytest.mark.parametrize(
    "build_and_get_mapping",
    [
        lambda source: (
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values=source,
            ).values
        ),
        lambda source: (
            AgentEvidence(
                agent=AgentName.QUANT, score=0.5, rationale="test",
                primary_timeframe=Timeframe.M5, symbol="BTCUSDT",
                as_of=datetime(2026, 8, 31, tzinfo=UTC), context_id="ctx-1",
                supporting_metrics=source,
            ).supporting_metrics
        ),
        lambda source: (
            RiskAssessment(
                confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
                contradicting_metrics=source,
            ).contradicting_metrics
        ),
    ],
    ids=["FeatureVector.values", "AgentEvidence.supporting_metrics", "RiskAssessment.contradicting_metrics"],
)
class TestNumericMappingImmutability:
    def test_item_assignment_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            mapping["x"] = 999.0

    def test_new_key_assignment_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            mapping["y"] = 5.0

    def test_deletion_rejected(self, build_and_get_mapping) -> None:
        mapping = build_and_get_mapping({"x": 1.0})
        with pytest.raises(TypeError):
            del mapping["x"]

    def test_original_dict_mutation_after_construction_does_not_leak(self, build_and_get_mapping) -> None:
        source = {"x": 1.0}
        mapping = build_and_get_mapping(source)
        source["x"] = 999.0
        source["y"] = 42.0
        assert mapping["x"] == 1.0
        assert "y" not in mapping


class TestStringMappingImmutability:
    """context / details gibi str->str mapping'ler için aynı garanti."""

    def test_safety_event_context_immutable(self) -> None:
        event = SafetyEvent(
            reason_code=SafetyReasonCode.STALE_MARKET_DATA, severity=SafetySeverity.WARNING,
            component="x", message="test", occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
            context={"k": "v"},
        )
        with pytest.raises(TypeError):
            event.context["k"] = "changed"

    def test_data_quality_result_details_immutable(self) -> None:
        result = DataQualityResult(
            status=DataQualityStatus.STALE_PRICE, reason="test", details={"k": "v"}
        )
        with pytest.raises(TypeError):
            result.details["k"] = "changed"


=== FILE: tests/test_lifecycle_concurrency.py ===
"""
Autonomous Testnet trading lifecycle Phase 7/17 — concurrency proof: the
M1 candle-driven stop/target evaluator and the signal-driven
opposite-signal exit path share the EXACT SAME per-symbol lock
(`LifecycleManager.lock_for` == `SignalTestnetBridge._lock_for` whenever a
`LifecycleManager` is wired). This test drives both concurrently for the
same LONG position and proves at most one SELL is ever submitted — the
loser observes the winner's fresh state and no-ops, never a double sell."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.execution.lifecycle import BridgePositionRecord, Candle, PositionLifecycleState
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.signal_bridge import SignalTestnetBridge
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.paper_trading.models import PaperPosition, PaperTradingResult, PositionSide
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.runtime.models import RuntimeCycleResult
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"
SYMBOL = "BTCUSDT"


def _exchange_info() -> dict:
    return {
        "symbols": [
            {
                "symbol": SYMBOL, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int) -> dict:
    return {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": "FILLED", "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }


class _SlowFakeHttp(FakeTestnetHttpClient):
    """Adds a cooperative yield before responding, to widen the window in
    which two concurrent callers could (incorrectly, if the lock were
    missing) both observe a stale LONG position and both submit a SELL."""

    async def get(self, url, params, headers, timeout_seconds):  # noqa: ANN001
        await asyncio.sleep(0)
        return await super().get(url, params, headers, timeout_seconds)

    async def post(self, url, params, headers, timeout_seconds):  # noqa: ANN001
        await asyncio.sleep(0)
        return await super().post(url, params, headers, timeout_seconds)


def _signal(*, score: float, context_id: str) -> Signal:
    return Signal(
        symbol=SYMBOL, timestamp=NOW, context_id=context_id, score=score, confidence=0.9,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test-bridge",
    )


def _cycle_result(signal: Signal) -> RuntimeCycleResult:
    paper_result = PaperTradingResult(
        symbol=signal.symbol,
        position=PaperPosition(
            symbol=signal.symbol, side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0,
            realized_pnl=0.0, updated_at=signal.timestamp,
        ),
        orders=(), fills=(), idempotent_replay=False,
    )
    return RuntimeCycleResult(
        symbol=signal.symbol, evaluated=True, signal=signal, paper_result=paper_result, generated_at=signal.timestamp
    )


class TestSharedLockPreventsDoubleSell:
    def test_concurrent_m1_stop_and_opposite_signal_produce_at_most_one_sell(self, tmp_path: Path) -> None:
        # Two DIFFERENT SELL fill responses queued -- if the shared lock
        # failed to serialize the two paths, BOTH would be consumed
        # (two POSTs); with the lock working, only one is ever submitted.
        get_responses = [json_response(_exchange_info())] * 4
        post_responses = [
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="80.0", order_id=1)),
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0", order_id=2)),
        ]
        http = _SlowFakeHttp(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = ExecutionStateStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        bridge = SignalTestnetBridge(
            execution_service=service, execution_store=exec_store, notional_usdt=10.0, clock=FixedClock(NOW),
            lifecycle_manager=manager, atr_provider=lambda _s: 5.0, all_symbols_provider=lambda: (SYMBOL,),
        )
        bridge.mark_operational(True, "test")

        lifecycle_store.save_position(BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-entry", updated_at=NOW,
        ))

        stop_candle = Candle(open=91, high=91, low=89, close=90, close_time=NOW + timedelta(minutes=1))
        short_signal = _signal(score=-0.9, context_id="ctx-short")

        async def scenario():
            await asyncio.gather(
                manager.evaluate_m1_candle(SYMBOL, stop_candle, atr_for_trailing=5.0),
                bridge.on_cycle_result(_cycle_result(short_signal)),
            )

        run_async(scenario())

        assert len(http.post_calls) == 1, "exactly one SELL must be submitted, never two"
        final = lifecycle_store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        trades = lifecycle_store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1


=== FILE: tests/test_lifecycle_runtime.py ===
"""
Autonomous Testnet trading lifecycle — `LifecycleRuntime` wiring tests
(Phase 6/8/9/12). Proves: an M1 consumer task exists per symbol only when
a `LifecycleManager` is wired; unclosed candles are skipped; a stale/absent
M5 ATR snapshot is passed through as `atr_for_trailing=None`; one symbol's
evaluation failure never kills another symbol's task; `stop()` leaves no
dangling M1 tasks."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.lifecycle_runtime import LifecycleRuntime
from tests.conftest import run_async
from tests.runtime_fakes import make_candle

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeFeatureEngine:
    def __init__(self, snapshot=None) -> None:
        self._snapshot = snapshot

    def latest_snapshot(self, symbol, timeframe):  # noqa: ANN001
        return self._snapshot


class _Snapshot:
    def __init__(self, as_of, values) -> None:  # noqa: ANN001
        self.as_of = as_of
        self.values = values


class _FakeProvider:
    def __init__(self, candles_by_symbol: dict[str, list]) -> None:
        self._candles_by_symbol = candles_by_symbol

    async def stream_candles(self, symbol, timeframe):  # noqa: ANN001
        for candle in self._candles_by_symbol.get(symbol, []):
            yield candle


class _FakeCoordinator:
    def __init__(self, symbols, provider, feature_engine) -> None:  # noqa: ANN001
        self._symbols = symbols
        self._provider = provider
        self._feature_engine = feature_engine
        self._stopped = False


class _FakeBridgeRuntime:
    def __init__(self, coordinator) -> None:  # noqa: ANN001
        self._coord = coordinator
        self.run_called = False
        self.stop_called = False

    @property
    def _coordinator(self):  # noqa: ANN202
        return self._coord

    async def recover(self):  # noqa: ANN201
        return ()

    def status(self):  # noqa: ANN201
        return "status"

    async def run(self) -> None:
        self.run_called = True
        await asyncio.sleep(0.05)

    async def stop(self) -> None:
        self.stop_called = True


class _FakeLifecycleManager:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def evaluate_m1_candle(self, symbol, candle, *, atr_for_trailing):  # noqa: ANN001
        self.calls.append((symbol, candle, atr_for_trailing))
        return None


class _FailingLifecycleManager:
    async def evaluate_m1_candle(self, symbol, candle, *, atr_for_trailing):  # noqa: ANN001
        raise RuntimeError("boom")


def _m1_candle(symbol: str, minute: int, *, closed: bool = True):
    return make_candle(symbol, Timeframe.M1, NOW + timedelta(minutes=minute), is_closed=closed)


class TestTaskCreation:
    def test_no_lifecycle_manager_creates_no_m1_tasks(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        runtime = LifecycleRuntime(bridge, None)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            await runtime.stop()
            await task

        run_async(scenario())
        assert runtime._m1_tasks == []
        assert bridge.run_called is True
        assert bridge.stop_called is True

    def test_one_m1_task_per_symbol_when_lifecycle_manager_present(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT", "ETHUSDT"), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            count = len(runtime._m1_tasks)
            await runtime.stop()
            await task
            return count

        count = run_async(scenario())
        assert count == 2


class TestM1Evaluation:
    def test_closed_candle_triggers_evaluation(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert len(manager.calls) == 1
        assert manager.calls[0][0] == "BTCUSDT"

    def test_unclosed_candle_is_skipped(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=False)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls == []

    def test_fresh_atr_snapshot_is_passed_through(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        snapshot = _Snapshot(as_of=candle.close_time - timedelta(minutes=1), values={"ATR_14": 42.0})
        coordinator = _FakeCoordinator(
            ("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(snapshot),
        )
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] == 42.0

    def test_stale_atr_snapshot_yields_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        snapshot = _Snapshot(as_of=candle.close_time - timedelta(minutes=30), values={"ATR_14": 42.0})
        coordinator = _FakeCoordinator(
            ("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(snapshot),
        )
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] is None

    def test_missing_atr_snapshot_yields_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine(None))
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert manager.calls[0][2] is None

    def test_one_symbol_evaluation_failure_does_not_raise(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        runtime = LifecycleRuntime(bridge, _FailingLifecycleManager())

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())  # no exception == pass


class TestStopCleansUpTasks:
    def test_stop_cancels_all_m1_tasks(self) -> None:
        coordinator = _FakeCoordinator(("BTCUSDT", "ETHUSDT"), _FakeProvider({}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)

        async def scenario():
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0.01)
            tasks_before_stop = list(runtime._m1_tasks)
            await runtime.stop()
            await task
            return tasks_before_stop

        tasks_before_stop = run_async(scenario())
        assert all(t.done() for t in tasks_before_stop)
        assert runtime._m1_tasks == []


class TestM1CandleObserver:
    """Adaptive Intelligence v1 — M1-fidelity shadow-evaluation fix
    (independent-verification round). Same discipline as `RuntimeCoordinator.
    candle_observer`'s own test suite."""

    def test_observer_called_once_per_closed_m1_candle(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        observed: list[tuple] = []
        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=lambda symbol, c: observed.append((symbol, c)))

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert len(observed) == 1
        assert observed[0][0] == "BTCUSDT"

    def test_observer_never_called_for_unclosed_candle(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=False)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        observed: list[tuple] = []
        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=lambda symbol, c: observed.append((symbol, c)))

        async def scenario():
            await runtime._consume_m1("BTCUSDT")

        run_async(scenario())
        assert observed == []

    def test_observer_still_called_when_real_evaluation_fails(self) -> None:
        """The observer must fire regardless of whether the REAL
        evaluation succeeded or was isolated -- it is purely
        observational and must never depend on the real path's outcome."""
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        observed: list[tuple] = []
        runtime = LifecycleRuntime(
            bridge, _FailingLifecycleManager(), m1_candle_observer=lambda symbol, c: observed.append((symbol, c)),
        )

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(observed) == 1

    def test_observer_failure_never_raises_or_stops_m1_consumption(self) -> None:
        candles = [_m1_candle("BTCUSDT", 1, closed=True), _m1_candle("BTCUSDT", 2, closed=True)]
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": candles}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()

        def _broken_observer(symbol, candle):  # noqa: ANN001
            raise RuntimeError("simulated shadow-evaluation failure")

        runtime = LifecycleRuntime(bridge, manager, m1_candle_observer=_broken_observer)

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(manager.calls) == 2  # both candles still reached the REAL evaluation path

    def test_default_behaviour_unchanged_when_observer_is_none(self) -> None:
        candle = _m1_candle("BTCUSDT", 1, closed=True)
        coordinator = _FakeCoordinator(("BTCUSDT",), _FakeProvider({"BTCUSDT": [candle]}), _FakeFeatureEngine())
        bridge = _FakeBridgeRuntime(coordinator)
        manager = _FakeLifecycleManager()
        runtime = LifecycleRuntime(bridge, manager)  # no m1_candle_observer

        async def scenario():
            await runtime._consume_m1("BTCUSDT")  # must not raise

        run_async(scenario())
        assert len(manager.calls) == 1


=== FILE: tests/test_local_readiness_check.py ===
"""Faz 12 — `scripts/local_readiness_check.py` testi: tamamen offline,
gerçek ağ çağrısı/order YOK, credential DEĞERİ asla stdout'a YAZILMAZ."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(tmp_path: Path, env_overrides: dict | None = None) -> subprocess.CompletedProcess:
    import os

    env = dict(os.environ)
    env.pop("BINANCE_TESTNET_API_KEY", None)
    env.pop("BINANCE_TESTNET_API_SECRET", None)
    env["CSE_SYMBOLS"] = "BTCUSDT"
    env["CSE_DB_PATH"] = str(tmp_path / "state.db")
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "scripts/local_readiness_check.py"],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=60,
    )


class TestLocalReadinessCheck:
    def test_passes_for_a_clean_paper_configuration(self, tmp_path: Path) -> None:
        result = _run(tmp_path)
        assert result.returncode == 0
        assert "LOCAL READINESS: PASS" in result.stdout

    def test_never_prints_credential_values(self, tmp_path: Path) -> None:
        result = _run(
            tmp_path,
            {
                "BINANCE_TESTNET_API_KEY": "readiness-secret-key",
                "BINANCE_TESTNET_API_SECRET": "readiness-secret-value",
                "CSE_EXECUTION_MODE": "BINANCE_SPOT_TESTNET",
                "CSE_ENABLE_TESTNET_EXECUTION": "true",
            },
        )
        combined = result.stdout + result.stderr
        assert "readiness-secret-key" not in combined
        assert "readiness-secret-value" not in combined

    def test_does_not_create_execution_db(self, tmp_path: Path) -> None:
        exec_db = tmp_path / "should-not-exist.db"
        _run(tmp_path, {"BINANCE_TESTNET_EXECUTION_DB_PATH": str(exec_db)})
        assert not exec_db.exists()


=== FILE: tests/test_ops_admin.py ===
"""24/7 Ops v1, Step 3 — `ops/admin.py::AdminController` unit tests.

Fully offline — no real asyncio loop, no real HTTP server. `loop` is a
`MagicMock` standing in for `asyncio.AbstractEventLoop`; `stop()`'s
`call_soon_threadsafe` call is asserted on directly (see
`tests/test_ops_dashboard.py` for the real HTTP-level integration
covering the 401/token-header path end to end)."""

from __future__ import annotations

from unittest.mock import MagicMock

from crypto_signal_engine.ops.admin import AdminController


def _controller(*, notifier=None) -> tuple[AdminController, MagicMock, MagicMock]:
    loop = MagicMock()
    request_shutdown = MagicMock()
    controller = AdminController(token="s3cr3t-token", loop=loop, request_shutdown=request_shutdown, notifier=notifier)
    return controller, loop, request_shutdown


class TestCheckToken:
    def test_correct_token_accepted(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("s3cr3t-token") is True

    def test_wrong_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("wrong") is False

    def test_missing_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token(None) is False

    def test_empty_string_token_rejected(self) -> None:
        controller, _, _ = _controller()
        assert controller.check_token("") is False

    def test_wrong_token_never_toggles_state(self) -> None:
        """Required test: a rejected token must NEVER be able to mutate
        `entries_paused` by any side channel — `check_token()` alone has
        no mutation capability at all (only `pause()`/`resume()`/`stop()`
        do, and the HTTP layer only calls those AFTER a successful
        `check_token()`, see `ops/dashboard.py::_handle_admin_action`)."""
        controller, _, _ = _controller()
        assert controller.check_token("wrong") is False
        assert controller.entries_paused.is_set() is False


class TestPauseResume:
    def test_pause_sets_entries_paused(self) -> None:
        controller, _, _ = _controller()
        result = controller.pause()
        assert controller.entries_paused.is_set() is True
        assert result["entries_paused"] is True
        assert result["enabled"] is True

    def test_resume_clears_entries_paused(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        result = controller.resume()
        assert controller.entries_paused.is_set() is False
        assert result["entries_paused"] is False

    def test_pause_is_idempotent(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        controller.pause()
        assert controller.entries_paused.is_set() is True

    def test_last_action_recorded_with_timestamp_and_detail(self) -> None:
        controller, _, _ = _controller()
        controller.pause()
        status = controller.status()
        assert status["last_action"]["action"] == "pause"
        assert status["last_action"]["at"]
        assert "False -> True" in status["last_action"]["detail"]

    def test_pause_and_resume_notify(self) -> None:
        calls: list[str] = []
        controller, _, _ = _controller(notifier=calls.append)
        controller.pause()
        controller.resume()
        assert len(calls) == 2
        assert "DURAKLATILDI" in calls[0]
        assert "DEVAM ETTİRİLDİ" in calls[1]

    def test_raising_notifier_never_affects_pause_result(self) -> None:
        def _raising(message: str) -> None:
            raise RuntimeError("boom")

        controller, _, _ = _controller(notifier=_raising)
        result = controller.pause()
        assert result["entries_paused"] is True


class TestStop:
    def test_stop_schedules_request_shutdown_via_call_soon_threadsafe(self) -> None:
        controller, loop, request_shutdown = _controller()
        controller.stop()
        loop.call_soon_threadsafe.assert_called_once_with(request_shutdown, "admin-stop")
        # The exact same idempotent chain as SIGTERM -- never a parallel
        # shutdown implementation. `request_shutdown` itself is not
        # called directly here (only scheduled), since this runs on a
        # non-loop thread in production.
        request_shutdown.assert_not_called()

    def test_stop_notifies(self) -> None:
        calls: list[str] = []
        controller, _, _ = _controller(notifier=calls.append)
        controller.stop()
        assert len(calls) == 1
        assert "Acil DURDUR" in calls[0]

    def test_stop_records_last_action(self) -> None:
        controller, _, _ = _controller()
        controller.stop()
        status = controller.status()
        assert status["last_action"]["action"] == "stop"


class TestStatus:
    def test_no_action_yet_has_none_last_action(self) -> None:
        controller, _, _ = _controller()
        status = controller.status()
        assert status == {"enabled": True, "entries_paused": False, "last_action": None}


=== FILE: tests/test_ops_config.py ===
"""Faz 8 — ops/config.py testleri: env-var parsing, validation, fail-fast."""

from __future__ import annotations

from pathlib import Path

import pytest

from crypto_signal_engine.ops.config import AppConfig, load_config
from crypto_signal_engine.ops.errors import AppConfigurationError
from crypto_signal_engine.providers.binance.config import ReconnectPolicyConfig


class TestLoadConfigDefaults:
    def test_minimal_env_produces_safe_defaults(self) -> None:
        config = load_config({"CSE_SYMBOLS": "btcusdt"})
        assert config.symbols == ("BTCUSDT",)
        assert config.log_level == "INFO"
        assert config.fee_bps == 0.0
        assert config.slippage_bps == 0.0
        assert config.notional_per_position == 1000.0
        assert config.warmup_candles == 25
        assert config.order_book_depth == 20
        assert config.stale_feed_threshold_seconds == 30.0
        assert isinstance(config.reconnect, ReconnectPolicyConfig)

    def test_symbols_are_deduplicated_and_normalized(self) -> None:
        config = load_config({"CSE_SYMBOLS": "btcusdt, BTCUSDT ,ethusdt"})
        assert config.symbols == ("BTCUSDT", "ETHUSDT")

    def test_lock_and_health_paths_derived_from_db_path_by_default(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DB_PATH": "/var/lib/cse/state.db"})
        assert config.lock_path == Path("/var/lib/cse/crypto-signal-engine.lock")
        assert config.health_snapshot_path == Path("/var/lib/cse/health.json")

    def test_explicit_lock_and_health_paths_are_respected(self) -> None:
        config = load_config(
            {
                "CSE_SYMBOLS": "BTCUSDT",
                "CSE_LOCK_PATH": "/tmp/custom.lock",
                "CSE_HEALTH_SNAPSHOT_PATH": "/tmp/custom-health.json",
            }
        )
        assert config.lock_path == Path("/tmp/custom.lock")
        assert config.health_snapshot_path == Path("/tmp/custom-health.json")


class TestLoadConfigFailFast:
    def test_missing_symbols_activates_automatic_mode(self) -> None:
        """`CSE_SYMBOLS` artık zorunlu DEĞİL — verilmezse `load_config()`
        HATA VERMEZ, `auto_select_symbols=True` ile boş bir `symbols`
        üretir (gerçek seçim `app.py::resolve_symbols()`'da, network I/O
        ile yapılır — bkz. tests/test_selection_config_integration.py)."""
        config = load_config({})
        assert config.auto_select_symbols is True
        assert config.symbols == ()

    def test_whitespace_only_symbols_also_activates_automatic_mode(self) -> None:
        """Boşluk-only bir değer, `_env()` seviyesinde "verilmemiş" ile
        AYIRT EDİLEMEZ (bkz. `_env()` — strip sonrası boşsa default'a
        düşer) — bu yüzden KASITLI OLARAK "yok" ile aynı, otomatik modu
        tetikler (garbage bir değere değil, gerçekten hiçbir sembol
        listesi VERİLMEMİŞ gibi davranılır)."""
        config = load_config({"CSE_SYMBOLS": "   "})
        assert config.auto_select_symbols is True
        assert config.symbols == ()

    def test_explicit_empty_symbols_list_raises(self) -> None:
        """`CSE_SYMBOLS` AÇIKÇA verilmiş ama hiçbir geçerli sembol
        İÇERMİYORSA (örn. yalnızca virgül) — bu "otomatik moda bırakma"
        DEĞİL, geçersiz bir manuel değerdir, hâlâ AÇIKÇA reddedilir."""
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": ","})

    def test_invalid_log_level_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="LOG_LEVEL"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_LOG_LEVEL": "VERBOSE"})

    def test_non_numeric_fee_bps_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="FEE_BPS"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_FEE_BPS": "not-a-number"})

    def test_negative_fee_bps_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_FEE_BPS": "-1"})

    def test_invalid_order_book_depth_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="ORDER_BOOK_DEPTH"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_ORDER_BOOK_DEPTH": "7"})

    def test_warmup_candles_below_minimum_raises(self) -> None:
        with pytest.raises(AppConfigurationError, match="WARMUP_CANDLES"):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_WARMUP_CANDLES": "5"})

    def test_non_integer_warmup_candles_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_WARMUP_CANDLES": "abc"})

    def test_zero_stale_feed_threshold_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_STALE_FEED_THRESHOLD_SECONDS": "0"})


class TestAppConfigHasNoCredentialFields:
    def test_no_key_secret_token_credential_fields(self) -> None:
        """24/7 Ops v1, Step 3 — mimari-farkında güncelleme: `dashboard_
        admin_token` KASITLI OLARAK AppConfig içinde YAŞAR (bkz.
        `ops/admin.py`/DECISIONS.md) — bu bir Binance/exchange kimlik
        bilgisi DEĞİLDİR, yalnızca yerel Control Center admin token'ıdır
        (`BINANCE_TESTNET_API_KEY`/`_SECRET`'in AKSİNE, KASITLI OLARAK bu
        sınıfın DIŞINDA tutulur — bkz. `execution/factory.py`). Bu tarama
        ZAYIFLATILMADI — yalnızca TEK, dokümante edilmiş bir istisnaya
        izin verir; başka HİÇBİR "key/secret/token/credential/password"
        parçalı alan adı hâlâ YASAKTIR."""
        import dataclasses

        field_names = {f.name.lower() for f in dataclasses.fields(AppConfig)}
        forbidden_fragments = ["key", "secret", "token", "credential", "password"]
        allowed_exceptions = {"dashboard_admin_token"}
        violations = [
            name for name in field_names
            if any(f in name for f in forbidden_fragments) and name not in allowed_exceptions
        ]
        assert violations == [], f"AppConfig içinde kimlik bilgisi alanı bulundu: {violations}"
        assert "dashboard_admin_token" in field_names, "dashboard_admin_token alanı beklenmedik şekilde eksik"

    def test_binance_config_url_is_not_overridable_via_env(self) -> None:
        """Defense-in-depth: config yüzeyi, Testnet/başka bir execution
        endpoint'ine işaret edecek şekilde KÖTÜYE KULLANILAMAZ — REST/WS
        base URL için hiçbir CSE_* alanı yoktur."""
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        binance_config = config.binance_config()
        assert binance_config.rest_base_url == "https://api.binance.com"
        assert binance_config.ws_base_url == "wss://stream.binance.com:9443"


class TestDashboardAdminTokenConfig:
    """24/7 Ops v1, Step 3 — `dashboard_admin_token` parsing. Default
    `None` = admin actions fully disabled (see `ops/admin.py`/
    `app.py::Application.start()`)."""

    def test_defaults_to_none(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        assert config.dashboard_admin_token is None

    def test_set_via_env(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "my-token"})
        assert config.dashboard_admin_token == "my-token"

    def test_whitespace_only_value_is_treated_as_unset(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "   "})
        assert config.dashboard_admin_token is None

    def test_never_appears_in_summary(self) -> None:
        config = load_config({"CSE_SYMBOLS": "BTCUSDT", "CSE_DASHBOARD_ADMIN_TOKEN": "my-token"})
        assert "dashboard_admin_token" not in config.summary()
        assert "my-token" not in str(config.summary())


class TestAutoSymbolSelectionConfigParsing:
    """`CSE_AUTO_SYMBOL_*` env-parsing + validation (Bölüm "CONFIGURATION").
    Ağ/discovery testleri BURADA DEĞİL — bkz. tests/test_selection_selector.py."""

    def test_defaults_when_unset(self) -> None:
        config = load_config({})
        sel = config.symbol_selection
        assert sel.target_count == 5
        assert sel.shortlist_size == 30
        assert sel.min_quote_volume_24h == 5_000_000.0
        assert sel.lookback_candles == 96
        assert sel.recent_window_candles == 12
        assert sel.rescan_interval_seconds == 3600.0
        assert sel.min_atr_pct == 0.0005
        assert sel.min_current_move_pct == 0.05

    def test_all_fields_overridable_via_env(self) -> None:
        config = load_config(
            {
                "CSE_AUTO_SYMBOL_COUNT": "3",
                "CSE_AUTO_SYMBOL_SHORTLIST_SIZE": "10",
                "CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME": "1000000",
                "CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "50",
                "CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES": "6",
                "CSE_AUTO_SYMBOL_RESCAN_INTERVAL_SECONDS": "1800",
            }
        )
        sel = config.symbol_selection
        assert (sel.target_count, sel.shortlist_size, sel.min_quote_volume_24h) == (3, 10, 1_000_000.0)
        assert (sel.lookback_candles, sel.recent_window_candles, sel.rescan_interval_seconds) == (50, 6, 1800.0)

    def test_manual_symbols_still_parse_selection_config_with_defaults(self) -> None:
        """Manual modda bile `symbol_selection` GEÇERLİ bir config'tir
        (kullanılmaz ama var olması, `AppConfig` şemasını dallandırmaz)."""
        config = load_config({"CSE_SYMBOLS": "BTCUSDT"})
        assert config.auto_select_symbols is False
        assert config.symbol_selection.target_count == 5

    def test_target_count_below_one_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_COUNT": "0"})

    def test_shortlist_smaller_than_target_count_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_COUNT": "10", "CSE_AUTO_SYMBOL_SHORTLIST_SIZE": "5"})

    def test_negative_min_quote_volume_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_QUOTE_VOLUME": "-1"})

    def test_lookback_below_minimum_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "5"})

    def test_recent_window_larger_than_lookback_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config(
                {"CSE_AUTO_SYMBOL_LOOKBACK_CANDLES": "20", "CSE_AUTO_SYMBOL_RECENT_WINDOW_CANDLES": "50"}
            )

    def test_zero_rescan_interval_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_RESCAN_INTERVAL_SECONDS": "0"})

    def test_negative_min_atr_pct_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_ATR_PCT": "-0.001"})

    def test_negative_min_current_move_pct_raises(self) -> None:
        with pytest.raises(AppConfigurationError):
            load_config({"CSE_AUTO_SYMBOL_MIN_CURRENT_MOVE_PCT": "-0.01"})

    def test_absolute_activity_floors_overridable_via_env(self) -> None:
        config = load_config(
            {"CSE_AUTO_SYMBOL_MIN_ATR_PCT": "0.002", "CSE_AUTO_SYMBOL_MIN_CURRENT_MOVE_PCT": "0.2"}
        )
        assert config.symbol_selection.min_atr_pct == 0.002
        assert config.symbol_selection.min_current_move_pct == 0.2


=== FILE: tests/test_ops_dashboard.py ===
"""Faz 12 / Görsel yeniden tasarım — `ops/dashboard.py` birim testleri.

Dashboard artık kullanıcı-onaylı `dashboard_design_reference.html`
mockup'ının CSS/JS'iyle render edilir; TÜM gerçek veri, sayfaya gömülen
TEK bir `window.__DASHBOARD_DATA__` JSON nesnesi üzerinden geçer (bkz.
`dashboard.py::_build_view_model`). Bu yüzden testler artık sunucu
tarafında render edilmiş Türkçe HTML metnini DEĞİL, gömülü JSON'u
doğrudan ayrıştırıp GERÇEK pozisyon/performans alanlarının doğru
taşındığını doğrular — bkz. `_extract_view_model()`.

Tamamen offline — gerçek runtime/asyncio event loop'a bağımlı DEĞİLDİR
(bkz. tests/test_phase12_local_production_readiness.py — o dosya
`Application` ile TAM entegrasyonu, bu dosya modülün KENDİSİNİ test
eder)."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from crypto_signal_engine.ops.admin import ADMIN_TOKEN_HEADER, AdminController
from crypto_signal_engine.ops.dashboard import DashboardServer, _build_view_model, _embed_json, render_dashboard_html
from crypto_signal_engine.ops.health_snapshot import write_snapshot
from crypto_signal_engine.runtime.models import RuntimeHealth, RuntimeStatus, SymbolHealth

_DATA_RE = re.compile(r"window\.__DASHBOARD_DATA__ = (.*?);</script>", re.S)


def _extract_view_model(page: str) -> dict:
    match = _DATA_RE.search(page)
    assert match is not None, "window.__DASHBOARD_DATA__ bulunamadı"
    return json.loads(match.group(1))


def _minimal_snapshot(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "overall_health": "READY", "generated_at": "2026-01-01T00:00:00+00:00", "pid": 1,
        "started_at": "2026-01-01T00:00:00+00:00",
        "recovery": {"ok": True, "detail": "running"},
        "symbols": [],
        "execution": {"mode": "PAPER", "enabled": False, "ready": False, "pending_reconciliation_count": None,
                      "last_reconciliation_at": None, "detail": ""},
        "paper": {},
        "symbol_selection": {"mode": "MANUAL", "opportunity_symbols": [], "pinned_open_position_symbols": [],
                              "final_runtime_symbols": [], "reselection_scheduler": {"enabled": False}},
        "candle_history": {},
    }
    base.update(overrides)
    return base


def _testnet_position(**overrides: object) -> dict[str, object]:
    base = {
        "state": "LONG", "net_owned_base_quantity": 0.20000000000000284, "gross_entry_vwap": 81108.02,
        "effective_stop": 80752.67659074345, "take_profit": 81818.70681851312, "trailing_active": False,
        "cumulative_realized_gross_pnl": 0.0, "migrated_existing_position": False, "last_exit_reason": None,
        "latest_price": 81500.0, "unrealized_gross_pnl": 78.396,
    }
    base.update(overrides)
    return base


def _bridge_lifecycle(positions: dict, *, performance: dict | None = None, recent_trades: list | None = None) -> dict:
    return {
        "enabled": True, "positions": positions,
        "performance": performance if performance is not None else {
            "exposure_usdt": 0.0, "non_flat_position_count": len([p for p in positions.values() if p.get("state") != "FLAT"]),
            "completed_trade_count": 0, "win_count": 0, "loss_count": 0, "win_rate": None,
            "realized_gross_pnl": 0.0, "todays_realized_gross_pnl": 0.0, "unrealized_gross_pnl_total": 0.0,
            "average_win_gross": None, "average_loss_gross": None,
        },
        "recent_trades": recent_trades if recent_trades is not None else [],
    }


class TestNoSnapshotYet:
    def test_renders_without_crashing_and_marks_no_snapshot(self, tmp_path: Path) -> None:
        page = render_dashboard_html(None, health_snapshot_path=tmp_path / "health.json")
        assert "Sinyal Botu Terminali" in page
        model = _extract_view_model(page)
        assert model["no_snapshot_yet"] is True


class TestJsonEmbeddingSafety:
    def test_script_breakout_is_neutralized(self) -> None:
        payload = {"x": "</script><script>alert(1)</script>"}
        embedded = _embed_json(payload)
        assert "</script>" not in embedded
        # round-trips back to the ORIGINAL string -- escaping is presentation-only, never lossy
        assert json.loads(embedded)["x"] == "</script><script>alert(1)</script>"

    def test_malicious_detail_survives_as_data_never_as_a_live_script_tag(self) -> None:
        snapshot = _minimal_snapshot(
            overall_health="DEGRADED",
            symbols=[{"symbol": "BTCUSDT", "health": "DEGRADED", "last_event_at": "x", "reconnect_count": 0,
                      "detail": "</script><script>alert(1)</script>"}],
        )
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        assert "</script><script>alert(1)</script>" not in page  # no live breakout in the HTML source
        model = _extract_view_model(page)
        assert model["degraded_symbols"][0]["detail_tr"] == "</script><script>alert(1)</script>"  # data preserved

    def test_free_text_fields_are_escaped_at_every_dom_insertion_site(self) -> None:
        """Structural guarantee: every JS call site that inserts a
        free-text (detail/exit-reason) field into innerHTML wraps it in
        `esc(...)` -- source-level check, since this test suite has no JS
        engine to execute the script against a real DOM."""
        src = Path("crypto_signal_engine/ops/dashboard.py").read_text(encoding="utf-8")
        for needle in (
            "esc(first.symbol)", "esc(first.detail_tr)", "esc(t.exit_reason_tr",
            "esc(p.last_exit_reason_tr", "esc(d.symbol)", "esc(d.detail_tr)",
        ):
            assert needle in src, f"{needle} bulunamadı — bir DOM-insertion noktası escape edilmemiş olabilir"


class TestReadOnlySafety:
    def test_no_forms_present(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "<form" not in page

    def test_no_inline_onclick_mutation_handlers(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "onclick=" not in page

    def test_restart_button_still_shows_not_connected_toast_not_fake_success(self) -> None:
        """24/7 Ops v1, Step 3 — Pause/Resume/Stop are now real (see
        `TestControlCenterAdminWiring` below); Restart remains
        deliberately NOT wired (see module docstring: remote-restart is
        out of scope) and still shows an honest "not connected" message,
        never a fabricated success string."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "kasıtlı olarak bağlanmadı" in page
        for fake_success in ("Bot duraklatıldı", "Bot devam ettirildi", "Yeniden başlatma tamamlandı", "Bot acil olarak durduruldu"):
            assert fake_success not in page

    def test_pause_resume_stop_wired_to_real_admin_endpoints(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "/admin/pause" in page
        assert "/admin/resume" in page
        assert "/admin/stop" in page

    def test_admin_token_and_stop_confirmation_use_a_real_modal_not_native_dialogs(self) -> None:
        """UI Polish v1, Step A6 — the approved reference mockup's modal
        pattern (`dashboard_design_reference.html`) now backs the admin
        token prompt and the Emergency Stop confirmation, replacing
        `window.prompt()`/`window.confirm()` entirely."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "modal-veil" in page
        assert "modal-actions" in page
        assert "modal-check" in page
        assert "window.prompt(" not in page
        assert "window.confirm(" not in page

    def test_modal_is_keyboard_operable(self) -> None:
        """Step A8 — Escape closes it, focus is trapped/managed."""
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert "Escape" in page
        assert "modalReturnFocus" in page

    def test_toast_wrap_is_aria_live_polite(self) -> None:
        page = render_dashboard_html(None, health_snapshot_path=Path("/tmp/health.json"))
        assert 'aria-live="polite"' in page

    def test_buy_sell_wording_absent(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position()}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        assert "buy" not in page.lower().replace("btnrestart", "")  # symbol-free lowercase scan
        assert "<button" in page  # control buttons still visually present (per reference design)


class TestViewModelRealPositionData:
    def test_known_positions_stop_and_target_appear_correctly(self) -> None:
        """Mission-required: a known open position's effective_stop/
        take_profit values appear correctly in the embedded view model."""
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({
            "ARBUSDT": _testnet_position(effective_stop=0.13530768, take_profit=0.13925248, gross_entry_vwap=0.1352),
        }))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        arb = next(p for p in model["positions"] if p["symbol"] == "ARBUSDT")
        assert arb["effective_stop"] == 0.13530768
        assert arb["take_profit"] == 0.13925248
        assert arb["gross_entry_vwap"] == 0.1352

    def test_flat_positions_excluded(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"ETHUSDT": {"state": "FLAT"}}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"] == []
        assert model["dust"] == []

    def test_dust_positions_separated_from_open_positions(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({
            "ARBUSDT": _testnet_position(state="LONG"),
            "BTCUSDT": _testnet_position(state="DUST", last_exit_reason="STOP_LOSS"),
        }))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert [p["symbol"] for p in model["positions"]] == ["ARBUSDT"]
        assert [p["symbol"] for p in model["dust"]] == ["BTCUSDT"]
        assert model["dust"][0]["last_exit_reason_tr"] == "Güvenlik seviyesi"

    def test_each_lifecycle_state_translates_to_turkish(self) -> None:
        expected = {
            "LONG": "Açık", "ENTRY_PENDING": "Alış emri sonuç bekliyor", "EXIT_PENDING": "Satış emri sonuç bekliyor",
            "AMBIGUOUS": "Binance sonucu kontrol ediliyor", "RECOVERED": "Geri yüklendi / aktifleşme bekliyor",
        }
        for raw, turkish in expected.items():
            snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position(state=raw)}))
            page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
            model = _extract_view_model(page)
            assert model["positions"][0]["state_tr"] == turkish

    def test_each_exit_reason_translates_to_turkish(self) -> None:
        expected = {
            "STOP_LOSS": "Güvenlik seviyesi", "TAKE_PROFIT": "Kâr hedefi", "TRAILING_STOP": "Takip eden stop",
            "MAX_HOLD": "Maksimum bekleme süresi", "OPPOSITE_SIGNAL": "Ters sinyal",
        }
        for raw, turkish in expected.items():
            snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
                {}, recent_trades=[{"recorded_at": "x", "symbol": "BTCUSDT", "quantity_closed": 1.0,
                                     "gross_entry_vwap": 100.0, "exit_gross_vwap": 105.0, "gross_realized_pnl": 5.0,
                                     "net_realized_pnl": 4.9, "exit_reason": raw}],
            ))
            page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
            model = _extract_view_model(page)
            assert model["recent_trades"][0]["exit_reason_tr"] == turkish

    def test_unknown_exit_reason_falls_back_to_raw_value(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position(last_exit_reason="SOME_FUTURE_REASON")}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"][0]["last_exit_reason_tr"] == "SOME_FUTURE_REASON"

    def test_commission_derived_correctly_from_gross_and_net(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {}, recent_trades=[{"recorded_at": "x", "symbol": "BTCUSDT", "quantity_closed": 1.0,
                                 "gross_entry_vwap": 100.0, "exit_gross_vwap": 105.0, "gross_realized_pnl": 5.0,
                                 "net_realized_pnl": 4.5, "exit_reason": "TAKE_PROFIT"}],
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["recent_trades"][0]["commission_usdt"] == 0.5

    def test_numeric_precision_is_not_altered_by_the_view_model(self) -> None:
        """Formatting (1.240,50 style) happens in JS at render time --
        the Python-side view model must NEVER truncate/round the
        underlying stored precision."""
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {"ARBUSDT": _testnet_position(net_owned_base_quantity=0.20000000000000284)},
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["positions"][0]["net_owned_base_quantity"] == 0.20000000000000284


class TestViewModelKpisAndScan:
    def test_kpi_fields_pass_through_from_performance(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle(
            {"ARBUSDT": _testnet_position()},
            performance={
                "exposure_usdt": 16.0, "non_flat_position_count": 1, "completed_trade_count": 8,
                "win_count": 2, "loss_count": 6, "win_rate": 0.25, "realized_gross_pnl": -1.08,
                "todays_realized_gross_pnl": 0.42, "unrealized_gross_pnl_total": 78.396,
                "average_win_gross": 0.28, "average_loss_gross": -0.27,
            },
        ))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        kpi = model["kpi"]
        assert kpi["unrealized_total"] == 78.396
        assert kpi["todays_realized"] == 0.42
        assert kpi["win_count"] == 2 and kpi["loss_count"] == 6
        assert kpi["win_rate"] == 0.25

    def test_bridge_disabled_kpi_is_none(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle={"enabled": False, "positions": {}})
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["kpi"] is None
        assert model["bridge_enabled"] is False

    def test_scan_fields_map_from_symbol_selection(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTOMATIC", "opportunity_symbols": ["ZENUSDT", "DASHUSDT"],
            "pinned_open_position_symbols": ["ARBUSDT", "BTCUSDT"], "final_runtime_symbols": ["ARBUSDT", "BTCUSDT", "ZENUSDT", "DASHUSDT"],
            "universe_size": 486, "shortlist_size": 30,
            "reselection_scheduler": {"enabled": True, "armed": True, "last_rescan_at": "2026-01-01T00:00:00+00:00",
                                       "next_rescan_at": "2026-01-01T01:00:00+00:00", "rescan_interval_seconds": 3600.0,
                                       "last_added": [], "last_removed": [], "last_error": None},
        })
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        scan = model["scan"]
        assert scan["scanned_count"] == 30
        assert scan["new_opportunities"] == ["ZENUSDT", "DASHUSDT"]
        assert scan["protected_symbols"] == ["ARBUSDT", "BTCUSDT"]
        assert scan["armed"] is True

    def test_degraded_symbols_translated_and_listed(self) -> None:
        snapshot = _minimal_snapshot(
            overall_health="DEGRADED",
            symbols=[
                {"symbol": "PROMUSDT", "health": "DEGRADED", "last_event_at": "x", "reconnect_count": 0, "detail": "unresolved gap: 5m"},
                {"symbol": "BTCUSDT", "health": "READY", "last_event_at": "x", "reconnect_count": 0, "detail": "healthy"},
            ],
        )
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert len(model["degraded_symbols"]) == 1
        assert model["degraded_symbols"][0]["symbol"] == "PROMUSDT"
        assert model["degraded_symbols"][0]["detail_tr"] == "5 dakikalık piyasa verisinde eksik veri algılandı"


class TestCandleHistory:
    def test_real_candle_history_passes_through_unaltered(self) -> None:
        candles = [{"t": "2026-01-01T00:00:00+00:00", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5}]
        snapshot = _minimal_snapshot(candle_history={"BTCUSDT": {"5m": candles}})
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["candles"]["BTCUSDT"]["5m"] == candles

    def test_missing_candle_history_is_an_empty_dict_not_fabricated(self) -> None:
        snapshot = _minimal_snapshot()
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        model = _extract_view_model(page)
        assert model["candles"] == {}
        # the JS-side fallback generator is explicitly marked with the example-data badge
        assert "örnek veri" in page
        assert "gerçek veri" in page


class TestMainStructurePresent:
    def test_reference_design_sections_present(self) -> None:
        snapshot = _minimal_snapshot(bridge_lifecycle=_bridge_lifecycle({"ARBUSDT": _testnet_position()}))
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        for marker in (
            "Sinyal Botu Terminali", "view-genel", "view-islemler", "view-control",
            "coinListBody", "candleWrap", "tradesBody", "openBody", "dustBody", "scanGrid", "healthCard", "svcGrid",
        ):
            assert marker in page

    def test_credentials_never_appear(self) -> None:
        snapshot = _minimal_snapshot(execution={
            "mode": "BINANCE_SPOT_TESTNET", "enabled": True, "ready": True,
            "pending_reconciliation_count": 0, "last_reconciliation_at": "x", "detail": "ok",
        })
        page = render_dashboard_html(snapshot, health_snapshot_path=Path("/tmp/health.json"))
        for forbidden in ("api_key", "api_secret", "secret", "signature"):
            assert forbidden not in page.lower()


class TestDashboardServerLifecycle:
    def _write(self, path: Path) -> None:
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(
            overall_health=RuntimeHealth.READY,
            symbols=(SymbolHealth(symbol="BTCUSDT", health=RuntimeHealth.READY, last_event_at=now,
                                   reconnect_count=0, detail="healthy"),),
            generated_at=now,
        )
        write_snapshot(
            path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now,
            execution={"mode": "PAPER", "enabled": False, "ready": False, "pending_reconciliation_count": None,
                       "last_reconciliation_at": None, "detail": ""},
            paper={"BTCUSDT": {"position": {"side": "FLAT", "quantity": 0.0, "average_entry_price": 0.0,
                                             "realized_pnl": 0.0},
                                "last_signal": {"direction": None, "timestamp": None, "context_id": None}}},
            bridge_lifecycle=_bridge_lifecycle({"BTCUSDT": _testnet_position()}),
            candle_history={"BTCUSDT": {"5m": [{"t": now.isoformat(), "o": 1, "h": 1, "l": 1, "c": 1}]}},
        )

    def test_start_stop_is_idempotent_and_serves_status_and_html(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=18999, health_snapshot_path=snap_path)
        server.start()
        server.start()  # idempotent
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:18999/healthz", timeout=2) as resp:
                assert resp.status == 200
            with urllib.request.urlopen("http://127.0.0.1:18999/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            assert data["overall_health"] == "READY"
            assert "candle_history" in data
            with urllib.request.urlopen("http://127.0.0.1:18999/", timeout=2) as resp:
                assert resp.status == 200
                page = resp.read().decode("utf-8")
            model = _extract_view_model(page)
            assert model["positions"][0]["symbol"] == "BTCUSDT"
        finally:
            server.stop()
            server.stop()  # idempotent

    def test_unknown_path_returns_404(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19000, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            try:
                urllib.request.urlopen("http://127.0.0.1:19000/nonexistent", timeout=2)
                raise AssertionError("expected HTTPError")
            except urllib.error.HTTPError as exc:
                assert exc.code == 404
        finally:
            server.stop()

    def test_mutation_methods_are_rejected(self, tmp_path: Path) -> None:
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19001, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                request = urllib.request.Request("http://127.0.0.1:19001/", method=method)
                try:
                    urllib.request.urlopen(request, timeout=2)
                    raise AssertionError(f"{method} should have been rejected")
                except urllib.error.HTTPError as exc:
                    assert exc.code == 405
        finally:
            server.stop()

    def test_api_status_preserves_existing_top_level_keys(self, tmp_path: Path) -> None:
        """Bölüm "OUT OF SCOPE"/"IMPLEMENTATION SCOPE" — `/api/status`
        mevcut alan adlarını KORUR; `candle_history` yalnızca EKLENİR."""
        snap_path = tmp_path / "health.json"
        self._write(snap_path)
        server = DashboardServer(host="127.0.0.1", port=19002, health_snapshot_path=snap_path)
        server.start()
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:19002/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            for key in (
                "generated_at", "overall_health", "pid", "started_at", "recovery", "symbols",
                "execution", "paper", "symbol_selection", "signal_testnet_bridge", "bridge_lifecycle",
                "candle_history",
            ):
                assert key in data
            position = data["bridge_lifecycle"]["positions"]["BTCUSDT"]
            for key in ("state", "net_owned_base_quantity", "gross_entry_vwap", "effective_stop", "take_profit"):
                assert key in position
        finally:
            server.stop()


class TestControlCenterAdminEndpoints:
    """24/7 Ops v1, Step 3 — real HTTP-level coverage of `POST /admin/
    {pause,resume,stop}`. Required tests: wrong/missing token always 401
    and NEVER toggles state; `dashboard_admin_token` unset (no
    `AdminController` set) behaves EXACTLY like before (405, same as
    every other mutation method)."""

    def _server(self, tmp_path: Path, *, port: int) -> DashboardServer:
        snap_path = tmp_path / "health.json"
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(overall_health=RuntimeHealth.READY, symbols=(), generated_at=now)
        write_snapshot(snap_path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now)
        return DashboardServer(host="127.0.0.1", port=port, health_snapshot_path=snap_path)

    def _post(self, url: str, *, token: str | None = None):
        headers = {ADMIN_TOKEN_HEADER: token} if token is not None else {}
        request = urllib.request.Request(url, method="POST", headers=headers)
        return urllib.request.urlopen(request, timeout=2)

    def test_admin_paths_405_when_no_admin_controller_configured(self, tmp_path: Path) -> None:
        """Regression: with no `AdminController` set (the `dashboard_
        admin_token=None` default path), `/admin/pause` is not special —
        it falls through to the SAME 405 every other POST gets, exactly
        like before this milestone."""
        server = self._server(tmp_path, port=19100)
        server.start()
        try:
            time.sleep(0.1)
            for path in ("/admin/pause", "/admin/resume", "/admin/stop"):
                try:
                    self._post(f"http://127.0.0.1:19100{path}")
                    raise AssertionError(f"{path} should have been rejected (405)")
                except urllib.error.HTTPError as exc:
                    assert exc.code == 405
        finally:
            server.stop()

    def test_missing_token_returns_401_and_never_toggles_state(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19101)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            try:
                self._post("http://127.0.0.1:19101/admin/pause")
                raise AssertionError("expected 401")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_wrong_token_returns_401_and_never_toggles_state(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19102)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            try:
                self._post("http://127.0.0.1:19102/admin/pause", token="totally-wrong")
                raise AssertionError("expected 401")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_correct_token_pauses_and_resumes(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19103)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with self._post("http://127.0.0.1:19103/admin/pause", token="s3cr3t") as resp:
                assert resp.status == 200
                body = json.loads(resp.read())
            assert body["entries_paused"] is True
            assert admin.entries_paused.is_set() is True

            with self._post("http://127.0.0.1:19103/admin/resume", token="s3cr3t") as resp:
                body = json.loads(resp.read())
            assert body["entries_paused"] is False
            assert admin.entries_paused.is_set() is False
        finally:
            server.stop()

    def test_stop_reuses_request_shutdown_not_a_parallel_mechanism(self, tmp_path: Path) -> None:
        server = self._server(tmp_path, port=19104)
        loop = MagicMock()
        request_shutdown = MagicMock()
        admin = AdminController(token="s3cr3t", loop=loop, request_shutdown=request_shutdown)
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with self._post("http://127.0.0.1:19104/admin/stop", token="s3cr3t") as resp:
                assert resp.status == 200
            loop.call_soon_threadsafe.assert_called_once_with(request_shutdown, "admin-stop")
        finally:
            server.stop()

    def test_admin_state_reflected_in_api_status_and_view_model(self, tmp_path: Path) -> None:
        """The dashboard's GET responses (both `/api/status` and `/`)
        NEVER read `AdminController` directly — they read ONLY the
        periodic atomic snapshot file (see `ops/dashboard.py` module
        docstring, "Bölüm 6"); `Application._write_snapshot()` is what
        threads `AdminController.status()` into that file in production
        (via `_admin_snapshot()`). This test proves the READ side of
        that contract by writing the snapshot with `admin=` explicitly,
        exactly like `Application` does."""
        snap_path = tmp_path / "health.json"
        now = datetime.now(timezone.utc)
        status = RuntimeStatus(overall_health=RuntimeHealth.READY, symbols=(), generated_at=now)
        admin = AdminController(token="s3cr3t", loop=MagicMock(), request_shutdown=MagicMock())
        admin.pause()
        write_snapshot(
            snap_path, status=status, recovery_ok=True, recovery_detail="running", pid=1, started_at=now,
            admin=admin.status(),
        )
        server = DashboardServer(host="127.0.0.1", port=19105, health_snapshot_path=snap_path)
        server.set_admin_controller(admin)
        server.start()
        try:
            time.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:19105/api/status", timeout=2) as resp:
                data = json.loads(resp.read())
            assert data["admin"]["enabled"] is True
            assert data["admin"]["entries_paused"] is True

            with urllib.request.urlopen("http://127.0.0.1:19105/", timeout=2) as resp:
                page = resp.read().decode("utf-8")
            model = _extract_view_model(page)
            assert model["admin"]["enabled"] is True
        finally:
            server.stop()


class TestStepA2UsdtBalanceNoneSafety:
    """UI Polish v1, Step A2 — `_build_view_model()` reads
    `bridge_lifecycle.usdt_balance` via `.get()` (never fabricates a
    number) and ignores it entirely when the bridge itself is disabled."""

    def test_present_when_bridge_enabled(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})
        bridge_lifecycle["usdt_balance"] = 1234.56
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] == 1234.56

    def test_none_when_key_missing_even_though_bridge_enabled(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})  # no "usdt_balance" key at all
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None

    def test_none_when_bridge_disabled_even_if_value_present(self) -> None:
        bridge_lifecycle = _bridge_lifecycle({})
        bridge_lifecycle["usdt_balance"] = 999.0
        bridge_lifecycle["enabled"] = False
        snapshot = _minimal_snapshot(bridge_lifecycle=bridge_lifecycle)
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None

    def test_none_when_bridge_key_absent_from_snapshot_entirely(self) -> None:
        snapshot = _minimal_snapshot()  # no "bridge_lifecycle" key at all
        model = _build_view_model(snapshot)
        assert model["usdt_balance"] is None


class TestStepA4LearnedFactorScoreSourcing:
    """UI Polish v1, Step A4 — `_build_view_model()` builds `scan.
    learned_factors` from `symbol_selection.candidates` (already produced
    by `app.py::_symbol_selection_snapshot()`), never inventing a score
    for a symbol with no candidate entry."""

    def test_built_from_candidates(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTO", "opportunity_symbols": [], "pinned_open_position_symbols": [],
            "final_runtime_symbols": ["BTCUSDT", "ETHUSDT"], "reselection_scheduler": {"enabled": True},
            "candidates": [
                {"symbol": "BTCUSDT", "learned_factor_score": 0.7},
                {"symbol": "ETHUSDT", "learned_factor_score": 0.3},
            ],
        })
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {"BTCUSDT": 0.7, "ETHUSDT": 0.3}

    def test_empty_when_no_candidates_present_manual_mode(self) -> None:
        snapshot = _minimal_snapshot()  # default MANUAL mode, no "candidates" key
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {}

    def test_candidate_without_a_symbol_is_skipped_not_fabricated(self) -> None:
        snapshot = _minimal_snapshot(symbol_selection={
            "mode": "AUTO", "opportunity_symbols": [], "pinned_open_position_symbols": [],
            "final_runtime_symbols": [], "reselection_scheduler": {"enabled": True},
            "candidates": [{"learned_factor_score": 0.9}, {"symbol": None, "learned_factor_score": 0.5}],
        })
        model = _build_view_model(snapshot)
        assert model["scan"]["learned_factors"] == {}


=== FILE: tests/test_ops_event_log.py ===
"""
UI Polish / Final Acceptance v1, Step A9 — `ops/event_log.py` unit tests,
plus the defensive-wrap discipline proof at every real call site
(`ops/admin.py`, `execution/lifecycle_manager.py`, `adaptive/rollback.py`,
and the `adaptive/cycle.py` -> `adaptive/scheduler.py` pass-through chain
above it — see `ops/event_log.py`'s module docstring for the full list).

`record_event()` NEVER raises (any `sqlite3.Error`/`OSError` is caught
INSIDE the function itself) — which is exactly what lets every call site
above call it with no try/except of its own; unlike `safe_notify()`
(itself a wrapper every caller goes through), `record_event()` is called
directly, so the only faithful way to prove a caller is unaffected by a
failure is to force `record_event()`'s REAL internal failure path (an
`event_log_path` that can never be written to) rather than mock the
function to raise arbitrarily — mocking it to raise would just prove the
caller has no try/except of its own (true, and by design: the safety
net is entirely inside `record_event()`), not that the documented
discipline actually holds. Same standard `tests/test_adaptive_rollback.
py::test_a_raising_notifier_never_affects_the_rollback_decision` and
`tests/test_execution_lifecycle_manager.py::
test_a_raising_notifier_never_affects_entry_gate_decision` already set
for `safe_notify()`, adapted to `record_event()`'s different (non-
injectable, module-level) call shape."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from adaptive.cycle import run_adaptive_cycle
from adaptive.policy import PolicySnapshot
from adaptive.rollback import apply_rollback_if_needed
from adaptive.scheduler import AdaptiveScheduler, AdaptiveSchedulerConfig
from adaptive.shadow import ShadowOutcome
from adaptive.store import AdaptiveStore
from adaptive.windows import WindowConfig
from crypto_signal_engine.execution.lifecycle import (
    DailyRiskAccumulator,
    ExitReason,
    RiskPolicyConfig,
    trading_day_key,
)
from crypto_signal_engine.execution.lifecycle_manager import LifecycleManager
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.ops.admin import AdminController
from crypto_signal_engine.ops.event_log import record_event, recent_events
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.research_fakes import EPOCH, FakeHistoricalCandleSource

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SYMBOL = "BTCUSDT"
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _unwritable_path(tmp_path: Path) -> Path:
    """A `db_path` whose parent can never be created: `blocker` already
    exists as a plain FILE, so `db_path.parent.mkdir(parents=True,
    exist_ok=True)` inside `record_event()` always raises `FileExistsError`
    (an `OSError` subclass) — the exact real-world failure mode
    `record_event()`'s own internal `except (sqlite3.Error, OSError)` is
    documented to swallow. This is a REAL failure, not a mock, so a test
    passing this path proves the documented discipline, not just that a
    mock was installed correctly."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    return blocker / "events.db"


# ============================================================================
# ops/event_log.py itself
# ============================================================================


class TestRecordEventAndRecentEvents:
    def test_round_trip(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        record_event(db_path, event_type="admin_pause", detail="entries_paused: False -> True", occurred_at=NOW)
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "admin_pause"
        assert events[0]["detail"] == "entries_paused: False -> True"
        assert events[0]["occurred_at"] == NOW.isoformat()

    def test_most_recent_first_and_limit_respected(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        for i in range(5):
            record_event(db_path, event_type=f"event_{i}", detail="d", occurred_at=NOW + timedelta(minutes=i))
        events = recent_events(db_path, limit=3)
        assert [e["event_type"] for e in events] == ["event_4", "event_3", "event_2"]

    def test_recent_events_empty_when_file_never_written(self, tmp_path: Path) -> None:
        assert recent_events(tmp_path / "never_written.db") == []

    def test_table_and_parent_directory_created_lazily_on_first_write(self, tmp_path: Path) -> None:
        db_path = tmp_path / "nested" / "dir" / "events.db"
        assert not db_path.parent.exists()
        record_event(db_path, event_type="x", detail="y", occurred_at=NOW)
        assert db_path.exists()
        assert recent_events(db_path) != []

    def test_record_event_never_raises_when_parent_directory_cannot_be_created(self, tmp_path: Path) -> None:
        record_event(_unwritable_path(tmp_path), event_type="x", detail="y", occurred_at=NOW)  # must not raise

    def test_recent_events_never_raises_on_a_corrupt_db_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        db_path.write_bytes(b"not a real sqlite file at all")
        assert recent_events(db_path) == []

    def test_record_event_never_raises_on_a_corrupt_db_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        db_path.write_bytes(b"not a real sqlite file at all")
        record_event(db_path, event_type="x", detail="y", occurred_at=NOW)  # must not raise


# ============================================================================
# Call site 1/4 — ops/admin.py::AdminController
# ============================================================================


def _admin_controller(*, event_log_path: Path | None) -> AdminController:
    from unittest.mock import MagicMock

    return AdminController(
        token="s3cr3t-token", loop=MagicMock(), request_shutdown=MagicMock(), event_log_path=event_log_path,
    )


class TestAdminControllerEventLog:
    def test_pause_records_a_real_event(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        controller = _admin_controller(event_log_path=db_path)
        controller.pause()
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "admin_pause"

    def test_pause_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        result = controller.pause()  # must not raise
        assert result["entries_paused"] is True
        assert controller.entries_paused.is_set() is True

    def test_resume_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        controller.pause()
        result = controller.resume()  # must not raise
        assert result["entries_paused"] is False

    def test_stop_result_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        controller = _admin_controller(event_log_path=_unwritable_path(tmp_path))
        result = controller.stop()  # must not raise
        assert result["last_action"]["action"] == "stop"


# ============================================================================
# Call site 2/4 — execution/lifecycle_manager.py::LifecycleManager.entry_gate
# ============================================================================


def _lifecycle_manager(tmp_path: Path, *, event_log_path: Path | None, risk_policy=None) -> tuple[LifecycleManager, LifecycleStore]:
    http = FakeTestnetHttpClient()
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    manager = LifecycleManager(
        store=lifecycle_store, execution_service=service, risk_policy=risk_policy,
        clock=FixedClock(NOW), event_log_path=event_log_path,
    )
    return manager, lifecycle_store


class TestLifecycleManagerEventLog:
    def test_daily_loss_breaker_transition_records_a_real_event(self, tmp_path: Path) -> None:
        db_path = tmp_path / "events.db"
        manager, store = _lifecycle_manager(
            tmp_path, event_log_path=db_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "daily_loss_breaker_tripped"

    def test_entry_gate_decision_unaffected_by_an_unwritable_event_log(self, tmp_path: Path) -> None:
        manager, store = _lifecycle_manager(
            tmp_path, event_log_path=_unwritable_path(tmp_path),
            risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))  # must not raise
        assert result.allowed is False
        assert "daily loss" in result.detail


# ============================================================================
# Call site 3/4 — adaptive/rollback.py::apply_rollback_if_needed
# ============================================================================


def _policy_snapshot(version_id: str, **overrides) -> PolicySnapshot:
    defaults = dict(
        stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
        trailing_distance_atr_multiple=2.0, max_hold_hours=48.0, created_at=NOW, provenance="test",
    )
    defaults.update(overrides)
    return PolicySnapshot(version_id=version_id, **defaults)


def _seed_shadow_outcomes(store: AdaptiveStore, version_id: str, pnls: list[float]) -> None:
    for pnl in pnls:
        store.record_shadow_outcome(
            version_id, symbol=SYMBOL,
            outcome=ShadowOutcome(
                exit_reason=ExitReason.TAKE_PROFIT if pnl > 0 else ExitReason.STOP_LOSS,
                exit_price=100.0 + pnl, exit_time=NOW, gross_pnl_per_unit=pnl,
            ),
            now=NOW,
        )


def _degraded_champion_pair(store: AdaptiveStore) -> tuple[PolicySnapshot, PolicySnapshot]:
    """v2 is champion with a healthy shadow-stage track record, but has
    genuinely collapsed in real production -- the exact
    `test_rolls_back_using_own_shadow_stage_baseline_on_genuine_
    degradation` scenario from `tests/test_adaptive_rollback.py`, reused
    here purely as a realistic rollback-triggering fixture."""
    v1 = _policy_snapshot("v1")
    v2 = _policy_snapshot("v2", max_hold_hours=12.0)
    store.promote_champion(v1, reason="bootstrap", now=NOW)
    store.promote_champion(v2, reason="promoted", now=NOW)
    _seed_shadow_outcomes(store, "v2", [10.0] * 7 + [-2.0] * 3)
    return v1, v2


def _degraded_live_pnls(version_id: str) -> list[float]:
    return [1.0] * 2 + [-10.0] * 8


@pytest.fixture()
def adaptive_store(tmp_path: Path) -> AdaptiveStore:
    s = AdaptiveStore(tmp_path / "adaptive.db")
    yield s
    s.close()


class TestRollbackEventLog:
    def test_rollback_records_a_real_event(self, adaptive_store: AdaptiveStore, tmp_path: Path) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"
        result = apply_rollback_if_needed(
            adaptive_store, champion=v2, live_pnls_provider=_degraded_live_pnls, now=NOW, event_log_path=db_path,
        )
        assert result == v1
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "adaptive_rollback"
        assert "v2" in events[0]["detail"] and "v1" in events[0]["detail"]

    def test_rollback_decision_unaffected_by_an_unwritable_event_log(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        result = apply_rollback_if_needed(
            adaptive_store, champion=v2, live_pnls_provider=_degraded_live_pnls, now=NOW,
            event_log_path=_unwritable_path(tmp_path),
        )  # must not raise
        assert result == v1
        assert adaptive_store.current_champion() == v1


# ============================================================================
# Call site 4/4 (pass-through) — adaptive/cycle.py -> adaptive/scheduler.py
# ============================================================================


def _window_config() -> WindowConfig:
    return WindowConfig(discovery_window_size=timedelta(hours=6), confirmation_window_size=timedelta(hours=2))


class TestCycleEventLogWiring:
    """`adaptive/cycle.py` never calls `record_event()` itself -- it only
    threads `event_log_path` through to `apply_rollback_if_needed()`
    (already proven defensive above). These tests prove the wiring
    actually reaches it, end to end, with a REAL rollback and a REAL
    (or genuinely broken) event log."""

    def test_rollback_wired_through_cycle_records_a_real_event(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"

        async def scenario():
            return await run_adaptive_cycle(
                store=adaptive_store, candle_source=FakeHistoricalCandleSource(), symbols=(SYMBOL,),
                window_config=_window_config(), anchor=EPOCH, seed=1, now=EPOCH + timedelta(minutes=30),
                champion_live_pnls_provider=_degraded_live_pnls, event_log_path=db_path,
            )

        report = run_async(scenario())
        assert report.champion_version_id == "v1"
        events = recent_events(db_path)
        assert len(events) == 1
        assert events[0]["event_type"] == "adaptive_rollback"

    def test_cycle_result_unaffected_by_an_unwritable_event_log(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)

        async def scenario():
            return await run_adaptive_cycle(
                store=adaptive_store, candle_source=FakeHistoricalCandleSource(), symbols=(SYMBOL,),
                window_config=_window_config(), anchor=EPOCH, seed=1, now=EPOCH + timedelta(minutes=30),
                champion_live_pnls_provider=_degraded_live_pnls, event_log_path=_unwritable_path(tmp_path),
            )

        report = run_async(scenario())  # must not raise, must not silently drop the rollback
        assert report.champion_version_id == "v1"
        assert adaptive_store.current_champion() == v1


class TestSchedulerEventLogWiring:
    """`adaptive/scheduler.py::AdaptiveScheduler` never calls
    `record_event()` itself either -- it only threads `event_log_path`
    through to `run_adaptive_cycle()`. `run_once()` ALSO wraps the whole
    cycle in a broad `except Exception` (a failed cycle must never kill
    the scheduler loop) -- these tests prove a broken event log is
    swallowed by `record_event()` itself and never even reaches that
    outer guard as a reported cycle failure."""

    def _config(self) -> AdaptiveSchedulerConfig:
        return AdaptiveSchedulerConfig(
            symbols=(SYMBOL,), anchor=EPOCH, window_config=_window_config(), cycle_interval_seconds=3600.0, seed=1,
        )

    def test_run_once_wires_event_log_path_and_records_a_real_event(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        db_path = tmp_path / "events.db"
        scheduler = AdaptiveScheduler(
            store=adaptive_store, candle_source=FakeHistoricalCandleSource(), config=self._config(),
            champion_live_pnls_provider=_degraded_live_pnls, clock=FixedClock(EPOCH + timedelta(minutes=30)),
            event_log_path=db_path,
        )
        report = run_async(scheduler.run_once())
        assert report is not None
        assert report.champion_version_id == "v1"
        assert recent_events(db_path) != []
        assert scheduler.status().last_error is None

    def test_run_once_not_reported_as_a_failed_cycle_when_event_log_is_unwritable(
        self, adaptive_store: AdaptiveStore, tmp_path: Path,
    ) -> None:
        v1, v2 = _degraded_champion_pair(adaptive_store)
        scheduler = AdaptiveScheduler(
            store=adaptive_store, candle_source=FakeHistoricalCandleSource(), config=self._config(),
            champion_live_pnls_provider=_degraded_live_pnls, clock=FixedClock(EPOCH + timedelta(minutes=30)),
            event_log_path=_unwritable_path(tmp_path),
        )
        report = run_async(scheduler.run_once())
        assert report is not None  # NOT swallowed into "cycle failed" by the outer except Exception
        assert report.champion_version_id == "v1"
        status = scheduler.status()
        assert status.last_error is None
        assert status.last_promoted_to is None  # a rollback is not a PROMOTE decision


=== FILE: tests/test_ops_health_snapshot.py ===
"""Faz 8 — ops/health_snapshot.py testleri: atomik yazma, round-trip okuma."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from crypto_signal_engine.ops.health_snapshot import read_snapshot, write_snapshot
from crypto_signal_engine.runtime.models import RuntimeHealth, RuntimeStatus, SymbolHealth


def _status(*, health: RuntimeHealth = RuntimeHealth.READY, detail: str = "ok") -> RuntimeStatus:
    now = datetime.now(timezone.utc)
    return RuntimeStatus(
        overall_health=health,
        symbols=(
            SymbolHealth(symbol="BTCUSDT", health=health, last_event_at=now, reconnect_count=0, detail=detail),
        ),
        generated_at=now,
    )


class TestWriteReadRoundTrip:
    def test_read_missing_file_returns_none(self, tmp_path: Path) -> None:
        assert read_snapshot(tmp_path / "does-not-exist.json") is None

    def test_write_then_read_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        started_at = datetime.now(timezone.utc)
        write_snapshot(
            path, status=_status(), recovery_ok=True, recovery_detail="recovery completed",
            pid=1234, started_at=started_at,
        )
        snapshot = read_snapshot(path)
        assert snapshot is not None
        assert snapshot["overall_health"] == "READY"
        assert snapshot["pid"] == 1234
        assert snapshot["recovery"] == {"ok": True, "detail": "recovery completed"}
        assert len(snapshot["symbols"]) == 1
        assert snapshot["symbols"][0]["symbol"] == "BTCUSDT"
        assert snapshot["symbols"][0]["detail"] == "ok"

    def test_write_leaves_no_temp_file_behind(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        write_snapshot(
            path, status=_status(), recovery_ok=True, recovery_detail="x",
            pid=1, started_at=datetime.now(timezone.utc),
        )
        leftovers = [p for p in tmp_path.iterdir() if p.name != "health.json"]
        assert leftovers == []

    def test_repeated_writes_overwrite_cleanly(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        for detail in ("first", "second", "third"):
            write_snapshot(
                path, status=_status(detail=detail), recovery_ok=True, recovery_detail=detail,
                pid=1, started_at=datetime.now(timezone.utc),
            )
        snapshot = read_snapshot(path)
        assert snapshot["recovery"]["detail"] == "third"
        assert snapshot["symbols"][0]["detail"] == "third"

    def test_degraded_symbol_detail_is_preserved(self, tmp_path: Path) -> None:
        path = tmp_path / "health.json"
        write_snapshot(
            path,
            status=_status(health=RuntimeHealth.DEGRADED, detail="unresolved gap: M5"),
            recovery_ok=True, recovery_detail="running",
            pid=1, started_at=datetime.now(timezone.utc),
        )
        snapshot = read_snapshot(path)
        assert snapshot["overall_health"] == "DEGRADED"
        assert snapshot["symbols"][0]["detail"] == "unresolved gap: M5"


=== FILE: tests/test_ops_lock.py ===
"""Faz 8 — ops/lock.py testleri: flock-tabanlı process-lock, offline/deterministic."""

from __future__ import annotations

from pathlib import Path

import pytest

from crypto_signal_engine.ops.errors import ConcurrentInstanceError
from crypto_signal_engine.ops.lock import ProcessLock


class TestProcessLock:
    def test_acquire_creates_lock_file_with_pid(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        lock = ProcessLock(lock_path)
        lock.acquire()
        try:
            assert lock_path.exists()
            assert lock_path.read_text().strip().isdigit()
        finally:
            lock.release()

    def test_second_acquire_on_same_path_is_rejected(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        first = ProcessLock(lock_path)
        first.acquire()
        try:
            second = ProcessLock(lock_path)
            with pytest.raises(ConcurrentInstanceError):
                second.acquire()
        finally:
            first.release()

    def test_after_release_a_new_lock_can_be_acquired(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        first = ProcessLock(lock_path)
        first.acquire()
        first.release()

        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_release_is_idempotent(self, tmp_path: Path) -> None:
        lock = ProcessLock(tmp_path / "cse.lock")
        lock.acquire()
        lock.release()
        lock.release()  # ikinci çağrı no-op olmalı, exception fırlatmamalı

    def test_context_manager_releases_on_exit(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        with ProcessLock(lock_path):
            pass
        # context manager çıktıktan sonra yeniden acquire edilebilmeli
        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_context_manager_releases_even_on_exception(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "cse.lock"
        with pytest.raises(ValueError):
            with ProcessLock(lock_path):
                raise ValueError("boom")
        second = ProcessLock(lock_path)
        second.acquire()
        second.release()

    def test_parent_directory_is_created_if_missing(self, tmp_path: Path) -> None:
        lock_path = tmp_path / "nested" / "dir" / "cse.lock"
        lock = ProcessLock(lock_path)
        lock.acquire()
        try:
            assert lock_path.exists()
        finally:
            lock.release()

    def test_double_acquire_on_same_instance_raises(self, tmp_path: Path) -> None:
        lock = ProcessLock(tmp_path / "cse.lock")
        lock.acquire()
        try:
            with pytest.raises(ConcurrentInstanceError):
                lock.acquire()
        finally:
            lock.release()


=== FILE: tests/test_ops_logging.py ===
"""Faz 8 — ops/logging_setup.py testleri: stdlib logging kurulumu,
idempotency. Gerçek dosya sistemine bağımlı DEĞİLDİR (stdout handler)."""

from __future__ import annotations

import logging

from crypto_signal_engine.ops.logging_setup import configure_logging


class TestConfigureLogging:
    def test_sets_requested_level(self) -> None:
        configure_logging("DEBUG")
        logger = logging.getLogger("crypto_signal_engine")
        assert logger.level == logging.DEBUG

    def test_does_not_duplicate_handlers_on_repeated_calls(self) -> None:
        configure_logging("INFO")
        configure_logging("INFO")
        configure_logging("WARNING")
        logger = logging.getLogger("crypto_signal_engine")
        assert len(logger.handlers) == 1

    def test_logger_does_not_propagate_to_root(self) -> None:
        configure_logging("INFO")
        logger = logging.getLogger("crypto_signal_engine")
        assert logger.propagate is False

    def test_child_logger_messages_are_captured_by_caplog(self, caplog) -> None:
        """`configure_logging` bilerek `propagate=False` ayarlar (root
        logger'a çift loglama YAPILMASIN diye) — bu yüzden caplog'un
        varsayılan root-tabanlı capture handler'ı BUNU GÖRMEZ; test, capture
        handler'ını doğrudan logger'a ekleyerek bunu (production
        davranışını DEĞİŞTİRMEDEN) doğrular."""
        configure_logging("INFO")
        logger = logging.getLogger("crypto_signal_engine.app")
        logger.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.INFO, logger="crypto_signal_engine.app"):
                logger.info("startup config summary: %s", {"symbols": ["BTCUSDT"]})
        finally:
            logger.removeHandler(caplog.handler)
        assert any("startup config summary" in record.message for record in caplog.records)


=== FILE: tests/test_ops_no_hardcoded_paths.py ===
"""Faz 8 — Faz 8'in eklediği dosyaların (kod + deploy + backup script), bu
geliştirme ortamına özgü hiçbir hardcoded WSL/Windows/developer-machine
yolu İÇERMEDİĞİNİ doğrular (Bölüm: "no hardcoded developer-machine paths").

Bu, mevcut kabul edilmiş `tests/test_repository_safety_scan.py`'ı
DEĞİŞTİRMEDEN, Faz 8'in kendi ek dosya kümesini (o dosyanın taramadığı
`deploy/` ve `scripts/` dahil) tarayan AYRI/ek bir regresyon testidir."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_SCANNED_PATHS = [
    REPO_ROOT / "crypto_signal_engine",
    REPO_ROOT / "deploy",
    REPO_ROOT / "scripts" / "backup_sqlite.py",
]

_FORBIDDEN_PATTERNS = [
    re.compile(r"/mnt/c/", re.IGNORECASE),
    re.compile(r"OneDrive", re.IGNORECASE),
    re.compile(r"[A-Za-z]:\\\\"),
    re.compile(r"/home/[a-zA-Z0-9_]+/"),
    re.compile(r"C:\\Users", re.IGNORECASE),
]


def _all_files() -> list[Path]:
    files: list[Path] = []
    for base in _SCANNED_PATHS:
        if base.is_file():
            files.append(base)
        elif base.is_dir():
            files.extend(p for p in base.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    return files


class TestNoHardcodedDeveloperMachinePaths:
    def test_no_wsl_or_windows_or_home_paths(self) -> None:
        violations = []
        for path in _all_files():
            text = path.read_text(encoding="utf-8")
            for pattern in _FORBIDDEN_PATTERNS:
                if pattern.search(text):
                    violations.append(f"{path}: {pattern.pattern!r}")
        assert violations == [], f"hardcoded developer-machine yolu bulundu: {violations}"


=== FILE: tests/test_ops_notifier.py ===
"""24/7 Ops v1, Step 4 — `ops/notifier.py` testleri: credential SET/UNSET
tespiti (değer ASLA döner/loglanmaz), Telegram HTTP çağrısının şekli
(mock `urlopen` — GERÇEK ağa ASLA dokunulmaz), ve `safe_notify()`'ın
"raising/None notifier ASLA çağıranı etkilemez" garantisi."""

from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock, patch

from crypto_signal_engine.ops.notifier import (
    build_telegram_notifier,
    safe_notify,
    telegram_credentials_status,
)


class TestTelegramCredentialsStatus:
    def test_both_unset(self) -> None:
        assert telegram_credentials_status(env={}) == (False, False)

    def test_only_bot_token_set(self) -> None:
        assert telegram_credentials_status(env={"CSE_TELEGRAM_BOT_TOKEN": "abc"}) == (True, False)

    def test_only_chat_id_set(self) -> None:
        assert telegram_credentials_status(env={"CSE_TELEGRAM_CHAT_ID": "123"}) == (False, True)

    def test_both_set(self) -> None:
        env = {"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "123"}
        assert telegram_credentials_status(env=env) == (True, True)

    def test_never_returns_actual_values(self) -> None:
        """SET/UNSET olarak `bool` döner — dönen değer ASLA credential
        değerinin kendisini İÇERMEZ."""
        env = {"CSE_TELEGRAM_BOT_TOKEN": "super-secret-token-value", "CSE_TELEGRAM_CHAT_ID": "999"}
        result = telegram_credentials_status(env=env)
        assert "super-secret-token-value" not in repr(result)
        assert result == (True, True)


class TestBuildTelegramNotifier:
    def test_returns_none_when_bot_token_missing(self) -> None:
        assert build_telegram_notifier(env={"CSE_TELEGRAM_CHAT_ID": "123"}) is None

    def test_returns_none_when_chat_id_missing(self) -> None:
        assert build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc"}) is None

    def test_returns_none_when_both_missing(self) -> None:
        assert build_telegram_notifier(env={}) is None

    def test_returns_a_callable_when_both_configured(self) -> None:
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "123"})
        assert callable(notifier)

    def test_sends_expected_url_and_payload(self) -> None:
        """`notify()` fires the HTTP POST on a background thread
        (fire-and-forget) — this test waits for that thread to finish
        before asserting on the mocked `urlopen` call."""
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc123", "CSE_TELEGRAM_CHAT_ID": "999"})
        assert notifier is not None

        fake_response = MagicMock()
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)
        fake_response.read = MagicMock(return_value=b"{}")

        done = threading.Event()

        def _tracking_urlopen(request, timeout):
            result = fake_response
            done.set()
            return result

        with patch("crypto_signal_engine.ops.notifier.urllib.request.urlopen", side_effect=_tracking_urlopen) as mocked:
            notifier("test message")
            assert done.wait(timeout=2.0), "background thread did not call urlopen in time"

        request = mocked.call_args.args[0]
        assert request.full_url == "https://api.telegram.org/botabc123/sendMessage"
        payload = json.loads(request.data.decode("utf-8"))
        assert payload == {"chat_id": "999", "text": "test message"}

    def test_bot_token_never_appears_in_notifier_repr(self) -> None:
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "top-secret-xyz", "CSE_TELEGRAM_CHAT_ID": "1"})
        assert "top-secret-xyz" not in repr(notifier)

    def test_network_failure_never_raises(self) -> None:
        """The background worker's own try/except must swallow a network
        failure — nothing propagates back to the caller of `notify()`."""
        notifier = build_telegram_notifier(env={"CSE_TELEGRAM_BOT_TOKEN": "abc", "CSE_TELEGRAM_CHAT_ID": "1"})
        assert notifier is not None
        import urllib.error

        with patch(
            "crypto_signal_engine.ops.notifier.urllib.request.urlopen",
            side_effect=urllib.error.URLError("unreachable"),
        ):
            notifier("message")  # must not raise
            time.sleep(0.05)  # let the background thread run to completion


class TestSafeNotify:
    def test_none_notifier_is_a_silent_no_op(self) -> None:
        safe_notify(None, "message")  # must not raise

    def test_raising_notifier_never_propagates(self) -> None:
        def _raising(message: str) -> None:
            raise RuntimeError("boom")

        safe_notify(_raising, "message")  # must not raise

    def test_working_notifier_receives_exact_message(self) -> None:
        received: list[str] = []
        safe_notify(received.append, "hello world")
        assert received == ["hello world"]


=== FILE: tests/test_ops_systemd_backup_template.py ===
"""24/7 Ops v1, Step 1 — `deploy/systemd/crypto-signal-engine-backup.{service,timer}`
için statik sağlık kontrolleri (dosya içeriği üzerinde string/regex
assertion'ları — gerçek systemd hiçbir zaman çalıştırılmaz, root
gerekmez), `tests/test_ops_systemd_template.py`'nin AYNI deseniyle."""

from __future__ import annotations

import re
from pathlib import Path

_DEPLOY_DIR = Path(__file__).resolve().parents[1] / "deploy" / "systemd"
SERVICE_PATH = _DEPLOY_DIR / "crypto-signal-engine-backup.service"
TIMER_PATH = _DEPLOY_DIR / "crypto-signal-engine-backup.timer"


class TestBackupServiceUnitFile:
    def test_file_exists(self) -> None:
        assert SERVICE_PATH.exists()

    def test_does_not_run_as_root(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        match = re.search(r"^User=(.+)$", text, re.MULTILINE)
        assert match is not None
        assert match.group(1).strip() != "root"

    def test_invokes_existing_backup_script_unchanged(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        assert "scripts/backup_sqlite.py" in text
        assert "--source" in text
        assert "--destination" in text
        assert "--keep-last" in text

    def test_no_sudo_or_privilege_escalation(self) -> None:
        assert "sudo" not in SERVICE_PATH.read_text(encoding="utf-8").lower()

    def test_no_embedded_secrets_or_credential_strings(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8").lower()
        for forbidden in ("api_key", "apikey", "secret", "hmac", "password", "listenkey"):
            assert forbidden not in text, f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_no_execution_or_private_endpoint_strings(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        for forbidden in ("/api/v3/order", "/sapi/", "/fapi/", "testnet", "mainnet"):
            assert forbidden.lower() not in text.lower(), f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_environment_file_is_referenced_not_inlined(self) -> None:
        assert re.search(r"^EnvironmentFile=", SERVICE_PATH.read_text(encoding="utf-8"), re.MULTILINE)

    def test_hardening_directives_present(self) -> None:
        text = SERVICE_PATH.read_text(encoding="utf-8")
        for directive in ("NoNewPrivileges=yes", "ProtectSystem=strict"):
            assert directive in text


class TestBackupTimerUnitFile:
    def test_file_exists(self) -> None:
        assert TIMER_PATH.exists()

    def test_has_a_schedule(self) -> None:
        assert re.search(r"^OnCalendar=", TIMER_PATH.read_text(encoding="utf-8"), re.MULTILINE)

    def test_is_persistent_across_missed_runs(self) -> None:
        assert "Persistent=true" in TIMER_PATH.read_text(encoding="utf-8")

    def test_installed_by_timers_target(self) -> None:
        assert re.search(r"^WantedBy=timers\.target", TIMER_PATH.read_text(encoding="utf-8"), re.MULTILINE)


=== FILE: tests/test_ops_systemd_notify.py ===
"""24/7 Ops v1, Step 2 — `ops/systemd_notify.py`'nin watchdog heartbeat
testleri: `$NOTIFY_SOCKET` yokken TAM bir no-op (ASLA fırlatmaz), varken
GERÇEK bir UNIX datagram soketine BEKLENEN payload'ı yazar (mocked
socket — gerçek bir dosya sistemi soketi AÇILMAZ)."""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

from crypto_signal_engine.ops.systemd_notify import _WATCHDOG_PAYLOAD, notify_watchdog


class TestNotifyWatchdogWithoutNotifySocket:
    def test_returns_false_and_never_raises_when_env_var_absent(self) -> None:
        assert notify_watchdog(env={}) is False

    def test_returns_false_when_env_var_is_empty_string(self) -> None:
        assert notify_watchdog(env={"NOTIFY_SOCKET": ""}) is False

    def test_never_raises_even_with_a_hostile_env_mapping(self) -> None:
        # A no-op must be provably safe regardless of what else is in env.
        assert notify_watchdog(env={"UNRELATED": "value", "PATH": "/usr/bin"}) is False


class TestNotifyWatchdogWithNotifySocket:
    def test_sends_exact_watchdog_payload_to_configured_path(self) -> None:
        fake_socket = MagicMock()
        fake_socket.__enter__ = MagicMock(return_value=fake_socket)
        fake_socket.__exit__ = MagicMock(return_value=False)
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", return_value=fake_socket) as ctor:
            result = notify_watchdog(env={"NOTIFY_SOCKET": "/run/systemd/notify"})
        assert result is True
        ctor.assert_called_once_with(socket.AF_UNIX, socket.SOCK_DGRAM)
        fake_socket.sendto.assert_called_once_with(_WATCHDOG_PAYLOAD, "/run/systemd/notify")

    def test_abstract_namespace_socket_path_is_translated(self) -> None:
        fake_socket = MagicMock()
        fake_socket.__enter__ = MagicMock(return_value=fake_socket)
        fake_socket.__exit__ = MagicMock(return_value=False)
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", return_value=fake_socket):
            notify_watchdog(env={"NOTIFY_SOCKET": "@systemd-notify"})
        fake_socket.sendto.assert_called_once_with(_WATCHDOG_PAYLOAD, "\0systemd-notify")

    def test_socket_error_is_caught_and_never_raises(self) -> None:
        with patch("crypto_signal_engine.ops.systemd_notify.socket.socket", side_effect=OSError("boom")):
            result = notify_watchdog(env={"NOTIFY_SOCKET": "/run/systemd/notify"})
        assert result is False

    def test_defaults_to_real_os_environ_when_env_omitted(self, monkeypatch) -> None:
        monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
        assert notify_watchdog() is False


=== FILE: tests/test_ops_systemd_template.py ===
"""Faz 8 — deploy/systemd/crypto-signal-engine.service için statik sağlık
kontrolleri (dosya içeriği üzerinde string/regex assertion'ları — gerçek
systemd hiçbir zaman çalıştırılmaz, root gerekmez)."""

from __future__ import annotations

import re
from pathlib import Path

UNIT_PATH = Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "crypto-signal-engine.service"


def _text() -> str:
    return UNIT_PATH.read_text(encoding="utf-8")


class TestSystemdUnitFile:
    def test_file_exists(self) -> None:
        assert UNIT_PATH.exists()

    def test_does_not_run_as_root(self) -> None:
        text = _text()
        match = re.search(r"^User=(.+)$", text, re.MULTILINE)
        assert match is not None, "User= satırı bulunamadı"
        assert match.group(1).strip() != "root"

    def test_execstart_invokes_python_module_not_a_shell_script(self) -> None:
        text = _text()
        match = re.search(r"^ExecStart=(.+)$", text, re.MULTILINE)
        assert match is not None
        assert "python" in match.group(1)
        assert "-m crypto_signal_engine.app run" in match.group(1)

    def test_no_sudo_or_privilege_escalation(self) -> None:
        text = _text()
        assert "sudo" not in text.lower()

    def test_has_bounded_restart_policy(self) -> None:
        text = _text()
        assert "Restart=on-failure" in text
        assert re.search(r"^RestartSec=\d+", text, re.MULTILINE)
        assert re.search(r"^StartLimitBurst=\d+", text, re.MULTILINE)
        assert re.search(r"^StartLimitIntervalSec=\d+", text, re.MULTILINE)

    def test_supports_graceful_sigterm_shutdown(self) -> None:
        text = _text()
        assert "KillSignal=SIGTERM" in text
        assert re.search(r"^TimeoutStopSec=\d+", text, re.MULTILINE)

    def test_hardening_directives_present(self) -> None:
        text = _text()
        for directive in ("NoNewPrivileges=yes", "ProtectSystem=strict", "ReadWritePaths="):
            assert directive in text, f"eksik hardening satırı: {directive}"

    def test_no_embedded_secrets_or_credential_strings(self) -> None:
        text = _text().lower()
        for forbidden in ("api_key", "apikey", "secret", "hmac", "password", "listenkey"):
            assert forbidden not in text, f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_no_execution_or_private_endpoint_strings(self) -> None:
        text = _text()
        for forbidden in ("/api/v3/order", "/sapi/", "/fapi/", "testnet", "mainnet"):
            assert forbidden.lower() not in text.lower(), f"unit dosyasında yasak string bulundu: {forbidden}"

    def test_environment_file_is_referenced_not_inlined(self) -> None:
        text = _text()
        assert re.search(r"^EnvironmentFile=", text, re.MULTILINE), "EnvironmentFile= satırı bulunamadı"


=== FILE: tests/test_orchestrator.py ===
from datetime import datetime, timezone
from unittest.mock import patch

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.signal_engine import SignalEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_history() -> FeatureHistoryStore:
    history = FeatureHistoryStore()
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M1, as_of=T0, values={
        "SPREAD_BPS": 3.0, "DEPTH_IMBALANCE_10": 0.2, "TOB_IMBALANCE": 0.1, "MID_PRICE": 100.0, "MICROPRICE": 100.02,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M5, as_of=T0, values={
        "RSI_14": 60.0, "ROC_10": 1.0, "VWAP_DEVIATION_20": 0.005, "RELATIVE_VOLUME_20": 1.1, "BOLLINGER_BANDWIDTH_20_2": 0.03,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.M15, as_of=T0, values={
        "ROC_10": 0.8, "DIST_FROM_HIGH_20": 0.02, "DIST_FROM_LOW_20": 0.04, "RELATIVE_VOLUME_20": 1.0, "BODY_TO_RANGE_RATIO": 0.6,
    }))
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=Timeframe.H1, as_of=T0, values={
        "ROC_10": 0.5, "RSI_14": 55.0, "BOLLINGER_BANDWIDTH_20_2": 0.03, "RELATIVE_VOLUME_20": 1.0,
        "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.05,
    }))
    return history


class TestOrchestrationChain:
    def test_consensus_engine_receives_evidence_not_regime_context_positionally(self) -> None:
        from crypto_signal_engine.consensus.engine import ConsensusEngine

        engine = SignalEngine(make_history())
        original_combine = ConsensusEngine.combine
        captured = {}

        def spy_combine(self, evidence, *, regime):
            captured["evidence_types"] = [type(e).__name__ for e in evidence]
            captured["regime_is_kwonly"] = True
            return original_combine(self, evidence, regime=regime)

        with patch.object(ConsensusEngine, "combine", spy_combine):
            engine.evaluate("BTCUSDT", T0)

        assert all(t == "AgentEvidence" for t in captured["evidence_types"])
        assert captured["regime_is_kwonly"] is True

    def test_risk_overlay_receives_consensus_result_and_regime_context_not_output(self) -> None:
        from crypto_signal_engine.consensus.risk import RiskOverlay

        engine = SignalEngine(make_history())
        original_assess = RiskOverlay.assess
        captured = {}

        def spy_assess(self, consensus, regime):
            captured["consensus_type"] = type(consensus).__name__
            captured["regime_type"] = type(regime).__name__
            return original_assess(self, consensus, regime)

        with patch.object(RiskOverlay, "assess", spy_assess):
            engine.evaluate("BTCUSDT", T0)

        assert captured["consensus_type"] == "ConsensusResult"
        assert captured["regime_type"] == "RegimeContext"

    def test_regime_context_passed_to_risk_is_same_object_from_regime_agent(self) -> None:
        """KRİTİK provenance testi: RiskOverlay'e geçirilen RegimeContext,
        TAM OLARAK aynı `RegimeAgent.evaluate()` çağrısından gelen nesne
        olmalı (başka bir değerlendirmeden GELMEMELİ)."""
        from crypto_signal_engine.agents.regime import RegimeAgent
        from crypto_signal_engine.consensus.risk import RiskOverlay

        engine = SignalEngine(make_history())
        original_regime_evaluate = RegimeAgent.evaluate
        original_risk_assess = RiskOverlay.assess
        captured = {}

        def spy_regime_evaluate(self, context):
            output = original_regime_evaluate(self, context)
            captured["regime_from_agent"] = output.regime
            return output

        def spy_risk_assess(self, consensus, regime):
            captured["regime_to_risk"] = regime
            return original_risk_assess(self, consensus, regime)

        with patch.object(RegimeAgent, "evaluate", spy_regime_evaluate), patch.object(RiskOverlay, "assess", spy_risk_assess):
            engine.evaluate("BTCUSDT", T0)

        assert captured["regime_to_risk"] is captured["regime_from_agent"]

    def test_evaluate_produces_valid_signal(self) -> None:
        engine = SignalEngine(make_history())
        signal = engine.evaluate("BTCUSDT", T0)
        assert signal.symbol == "BTCUSDT"
        assert signal.timestamp == T0
        assert signal.primary_timeframe == Timeframe.M5


=== FILE: tests/test_orderbook_features.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.models import OrderBookLevel, OrderBookSnapshot
from crypto_signal_engine.errors import FeatureCalculationError, FeatureValidationError
from crypto_signal_engine.features import orderbook_calculators as oc

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_book(bids, asks, last_update_id=1) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol="BTCUSDT", timestamp=T0,
        bids=tuple(OrderBookLevel(price=p, quantity=q) for p, q in bids),
        asks=tuple(OrderBookLevel(price=p, quantity=q) for p, q in asks),
        last_update_id=last_update_id,
    )


class TestBasicPrices:
    def test_best_bid_ask_mid(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        assert oc.best_bid(book) == pytest.approx(99.0)
        assert oc.best_ask(book) == pytest.approx(101.0)
        assert oc.mid_price(book) == pytest.approx(100.0)

    def test_spread_abs(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        assert oc.spread_abs(book) == pytest.approx(2.0)

    def test_spread_bps_known_value(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        # spread=2, mid=100 -> bps = 2/100*10000 = 200
        assert oc.spread_bps(book) == pytest.approx(200.0)


class TestDepth:
    def test_bid_ask_depth_sum(self) -> None:
        book = make_book([(99.0, 1.0), (98.0, 2.0), (97.0, 3.0)], [(101.0, 1.0), (102.0, 2.0)])
        assert oc.bid_depth(book, 2) == pytest.approx(3.0)  # 1+2
        assert oc.ask_depth(book, 2) == pytest.approx(3.0)  # 1+2

    def test_depth_beyond_available_uses_all_levels(self) -> None:
        book = make_book([(99.0, 1.0), (98.0, 2.0)], [(101.0, 1.0)])
        assert oc.bid_depth(book, 100) == pytest.approx(3.0)

    def test_invalid_depth_rejected(self) -> None:
        book = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        with pytest.raises(FeatureValidationError, match="depth"):
            oc.bid_depth(book, 0)
        with pytest.raises(FeatureValidationError, match="depth"):
            oc.bid_depth(book, -1)

    def test_depth_imbalance_known_value(self) -> None:
        book = make_book([(99.0, 3.0)], [(101.0, 1.0)])
        # (3-1)/(3+1) = 0.5
        assert oc.depth_imbalance(book, 1) == pytest.approx(0.5)

    def test_depth_imbalance_within_bounds(self) -> None:
        book = make_book([(99.0, 100.0)], [(101.0, 0.001)])
        value = oc.depth_imbalance(book, 1)
        assert -1.0 <= value <= 1.0


class TestMicropriceAndTOB:
    def test_microprice_weighted_toward_larger_opposite_side(self) -> None:
        # best_bid qty daha büyükse microprice ask'a daha yakın olmalı
        # (ağırlıklama karşı tarafın miktarına göre yapılır)
        book = make_book([(99.0, 10.0)], [(101.0, 1.0)])
        mp = oc.microprice(book)
        mid = oc.mid_price(book)
        # microprice = (99*1 + 101*10)/11 = (99+1010)/11 = 1109/11 = 100.818...
        assert mp == pytest.approx((99.0 * 1.0 + 101.0 * 10.0) / 11.0)
        assert mp > mid  # ağır bid tarafı fiyatı ask'a doğru çeker

    def test_top_of_book_imbalance_known_value(self) -> None:
        book = make_book([(99.0, 3.0)], [(101.0, 1.0)])
        assert oc.top_of_book_imbalance(book) == pytest.approx(0.5)

    def test_top_of_book_imbalance_within_bounds(self) -> None:
        book = make_book([(99.0, 1000.0)], [(101.0, 0.0001)])
        value = oc.top_of_book_imbalance(book)
        assert -1.0 <= value <= 1.0


class TestSymbolTimeIsolation:
    def test_different_snapshots_independent(self) -> None:
        book1 = make_book([(99.0, 1.0)], [(101.0, 1.0)])
        book2 = OrderBookSnapshot(
            symbol="ETHUSDT", timestamp=T0,
            bids=(OrderBookLevel(price=50.0, quantity=1.0),),
            asks=(OrderBookLevel(price=51.0, quantity=1.0),),
            last_update_id=1,
        )
        assert oc.mid_price(book1) != oc.mid_price(book2)


=== FILE: tests/test_package_import.py ===
"""
Quality Gate 1, 21.C & 29 — package import smoke test (environment-robust, offline).

HARDENING NOTU (v2 — reproducibility düzeltmesi): önceki revizyon, izole
venv'in `system_site_packages=True` ile ana ortamdaki `setuptools`'u miras
alacağını ve bu sayede `pip install --no-build-isolation` çağrısının
`setuptools.build_meta` backend'ini bulabileceğini VARSAYIYORDU. Bağımsız
bir reviewer ortamında bu varsayım YANLIŞ çıktı:

    pip._vendor.pyproject_hooks._impl.BackendUnavailable:
        Cannot import 'setuptools.build_meta'

Kök neden: "system site-packages inherit edilir" garantisi platform/ortam
bağımlıdır (venv'in nasıl kurulduğuna, base Python'un setuptools'u nereye
kurduğuna, PEP 668 kısıtlarına göre değişir) — bu YANLIŞ bir zemin üzerine
inşa edilmiş bir testti.

ÇÖZÜM: Bu test artık build backend'i (`setuptools.build_meta`) HİÇ
DEVREYE SOKMAZ. Paket, teslimat sırasında ÖNCEDEN inşa edilmiş bir wheel
dosyasından (`dist/crypto_signal_engine-*-py3-none-any.whl`) `pip install
--no-index --no-deps <wheel>` ile OFFLINE kurulur. Wheel kurulumu salt bir
unzip+copy işlemidir; hedef ortamda setuptools'un bulunup bulunmaması,
ağ erişiminin olup olmaması, veya ana ortamın nasıl yapılandırıldığı HİÇBİR
ŞEKİLDE önemli değildir — yalnızca pip'in kendisi (venv ile birlikte gelir)
yeterlidir.

Bu, aynı zamanda "yalnızca source-tree cwd sayesinde import oluyor" gibi bir
false-positive'i de engeller: kurulum GERÇEKTEN izole bir venv'in
site-packages'ına yapılır (source ağacına hiçbir referans/sys.path hilesi
yoktur) ve import testi repo'dan tamamen bağımsız dizinlerden (`/tmp`,
repo'nun ebeveyni) çalıştırılır.
"""

from __future__ import annotations

import glob
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"

IMPORT_SCRIPT = """
import crypto_signal_engine
import crypto_signal_engine.domain.models
import crypto_signal_engine.domain.events
import crypto_signal_engine.domain.enums
import crypto_signal_engine.domain.consensus
import crypto_signal_engine.domain.candle_sequencing
import crypto_signal_engine.domain.state_contract
import crypto_signal_engine.providers.base
import crypto_signal_engine.quality.base
import crypto_signal_engine.safety.models

assert crypto_signal_engine.ALLOW_LIVE_TRADING is False, "safety invariant bozuldu"
print("IMPORT_SMOKE_TEST_OK")
"""


def _find_prebuilt_wheel() -> Path:
    """Repo ile birlikte teslim edilen önceden-inşa edilmiş wheel'i bulur.

    Bu dosya `dist/` altında, teslimat sırasında `python -m build --wheel`
    ile ÖNCEDEN üretilip repoya dahil edilmiştir. Bu test onu YENİDEN
    İNŞA ETMEZ — build backend'i hiç çağırmaz, yalnızca var olan wheel'i
    offline kurar.
    """
    candidates = sorted(glob.glob(str(DIST_DIR / "crypto_signal_engine-*-py3-none-any.whl")))
    if not candidates:
        pytest.fail(
            f"Önceden inşa edilmiş wheel bulunamadı: {DIST_DIR}/crypto_signal_engine-*-py3-none-any.whl. "
            f"Bu dosya repo teslimatının bir parçası olmalıdır (bkz. scripts/verify_phase1.py "
            f"veya `python -m build --wheel .` ile yeniden üretin ve dist/ altına koyun)."
        )
    return Path(candidates[-1])


@pytest.fixture(scope="module")
def isolated_venv_python() -> str:
    """Tamamen izole (system_site_packages=False) bir venv kurar ve içine
    önceden-inşa edilmiş wheel'i OFFLINE (`--no-index --no-deps`) kurar.

    Build backend hiç devreye girmez; ağ erişimi GEREKMEZ; ana ortamın
    setuptools/wheel durumu SONUCU ETKİLEMEZ.
    """
    wheel_path = _find_prebuilt_wheel()

    tmp_dir = tempfile.mkdtemp(prefix="phase1_verify_venv_")
    venv.create(tmp_dir, with_pip=True, system_site_packages=False)
    venv_python = str(Path(tmp_dir) / "bin" / "python")

    install = subprocess.run(
        [
            venv_python, "-m", "pip", "install",
            "--no-index",       # PyPI'ye HİÇ gitme
            "--no-deps",        # bağımlılık çözümü yok (paketin zaten bağımlılığı yok)
            "--quiet",
            str(wheel_path),
        ],
        capture_output=True, text=True, timeout=60,
    )
    if install.returncode != 0:
        pytest.fail(
            f"İzole venv'e offline wheel kurulumu başarısız oldu (wheel={wheel_path}).\n"
            f"stdout: {install.stdout}\nstderr: {install.stderr}"
        )
    return venv_python


def test_isolated_install_and_import_from_repo_parent(isolated_venv_python: str) -> None:
    """Offline kurulmuş izole venv'in python'ı ile, repo kökünün
    EBEVEYNİNDEN (tamamen bağımsız bir cwd) import testi.

    cwd repo'dan bağımsız olduğu için, import'un başarılı olması YALNIZCA
    venv'in site-packages'ına GERÇEKTEN kurulmuş olmasıyla mümkündür —
    "source tree cwd sayesinde çalışıyor" false-positive'i yapısal olarak
    imkansızdır.
    """
    result = subprocess.run(
        [isolated_venv_python, "-c", IMPORT_SCRIPT],
        cwd=str(REPO_ROOT.parent),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, (
        f"İzole offline kurulum sonrası import FAILED.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "IMPORT_SMOKE_TEST_OK" in result.stdout


def test_isolated_install_and_import_from_tmp(isolated_venv_python: str) -> None:
    """Aynı testi /tmp'den (repo ile hiçbir ilişkisi olmayan bir dizin) tekrar dener."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        result = subprocess.run(
            [isolated_venv_python, "-c", IMPORT_SCRIPT],
            cwd=tmp_dir, capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, (
            f"Import from {tmp_dir} FAILED.\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "IMPORT_SMOKE_TEST_OK" in result.stdout


def test_installed_package_is_not_the_source_tree(isolated_venv_python: str) -> None:
    """İzole venv'e kurulan paketin GERÇEKTEN venv'in site-packages'ında
    olduğunu, repo'nun source ağacına işaret eden bir editable install
    (.pth dosyası vb.) OLMADIĞINI doğrular — false-positive'i yapısal
    olarak ekarte eder."""
    check_script = (
        "import crypto_signal_engine, os; "
        "p = os.path.abspath(crypto_signal_engine.__file__); "
        "print(p)"
    )
    result = subprocess.run(
        [isolated_venv_python, "-c", check_script],
        cwd=str(REPO_ROOT.parent), capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    installed_path = result.stdout.strip()
    repo_root_str = str(REPO_ROOT)
    assert not installed_path.startswith(repo_root_str), (
        f"Kurulan paket hâlâ repo source ağacına işaret ediyor ({installed_path}); "
        f"bu, wheel'in gerçekten site-packages'a KOPYALANMADIĞI anlamına gelir."
    )
    assert "site-packages" in installed_path


def test_current_environment_import_also_works() -> None:
    """Ek olarak: eğer bu test suite'i ZATEN (editable veya normal) kurulu
    bir ortamda çalıştırılıyorsa, o ortamdaki import da başarılı olmalıdır.
    Bu, izole venv testlerinin YERİNE değil, EK OLARAK çalışır; mevcut
    ortamda paket kurulu değilse bu tek test skip edilir (asıl gate,
    yukarıdaki izole/offline testlerle zaten karşılanmıştır)."""
    result = subprocess.run(
        [sys.executable, "-c", IMPORT_SCRIPT],
        cwd=str(REPO_ROOT.parent),
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 and "ModuleNotFoundError" in result.stderr:
        pytest.skip(
            "Mevcut ortamda package kurulu değil; asıl Gate 1/29 gereksinimi "
            "yukarıdaki izole+offline wheel testleriyle zaten bağımsız olarak "
            "karşılanıyor. Bu test yalnızca ek bir sinyal sağlar."
        )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert "IMPORT_SMOKE_TEST_OK" in result.stdout


=== FILE: tests/test_paper_trading_engine.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError, NoLookAheadViolationError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, OrderSide, PositionSide

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)


def make_signal(
    symbol: str = "BTCUSDT",
    timestamp: datetime = T0,
    context_id: str = "ctx-0",
    score: float = 0.0,
    model_version: str = "test-v1",
) -> Signal:
    return Signal(
        symbol=symbol,
        timestamp=timestamp,
        context_id=context_id,
        score=score,
        confidence=0.8,
        risk_level=RiskLevel.LOW,
        primary_timeframe=Timeframe.M5,
        supporting_factors=(),
        contradicting_factors=(),
        invalidation=None,
        model_version=model_version,
    )


def make_price(symbol: str = "BTCUSDT", price: float = 100.0, as_of: datetime = T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


LONG_SCORE = 0.5  # >= moderate (0.40), < strong (0.70) -> LONG
SHORT_SCORE = -0.5  # -> SHORT
NEUTRAL_SCORE = 0.0  # -> NEUTRAL


class TestOpenPosition:
    def test_long_signal_opens_long_position(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE)
        price = make_price(price=100.0, as_of=T0)

        result = engine.process_signal(signal, price)

        assert len(result.orders) == 1
        assert result.orders[0].side is OrderSide.BUY
        assert len(result.fills) == 1
        assert result.position.side is PositionSide.LONG
        assert result.position.quantity == pytest.approx(10.0)
        assert result.position.average_entry_price == pytest.approx(100.0)
        assert result.position.realized_pnl == pytest.approx(0.0)
        assert result.idempotent_replay is False

    def test_short_signal_opens_short_position(self) -> None:
        engine = PaperTradingEngine(notional_per_position=500.0)
        signal = make_signal(context_id="ctx-1", score=SHORT_SCORE)
        price = make_price(price=50.0)

        result = engine.process_signal(signal, price)

        assert result.orders[0].side is OrderSide.SELL
        assert result.position.side is PositionSide.SHORT
        assert result.position.quantity == pytest.approx(10.0)

    def test_neutral_signal_with_no_prior_position_is_noop(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(context_id="ctx-1", score=NEUTRAL_SCORE)
        price = make_price()

        result = engine.process_signal(signal, price)

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.side is PositionSide.FLAT


class TestSameDirectionRepeat:
    def test_repeat_long_signal_produces_no_new_orders(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        p1 = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        t1 = T0 + timedelta(minutes=5)
        p2 = engine.process_signal(
            make_signal(context_id="ctx-2", score=LONG_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        assert p2.orders == ()
        assert p2.fills == ()
        # position size/entry unchanged (no re-open), but observation timestamp refreshed
        assert p2.position.quantity == pytest.approx(p1.position.quantity)
        assert p2.position.average_entry_price == pytest.approx(p1.position.average_entry_price)
        assert p2.position.updated_at == t1


class TestFlipAndFlatten:
    def test_flip_long_to_short_realizes_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        # closed 10 units bought at 100, sold at 120 -> realized = (120-100)*10 = 200
        assert len(result.orders) == 2
        assert len(result.fills) == 2
        assert result.position.side is PositionSide.SHORT
        assert result.position.realized_pnl == pytest.approx(200.0)
        assert result.position.average_entry_price == pytest.approx(120.0)


class TestNeutralIsNoAction:
    """Faz 5 acceptance blocker fix: NEUTRAL sinyal NO_ACTION olmalı — hiçbir
    order/fill üretmemeli, mevcut pozisyonu (LONG/SHORT) kapatmamalı, cash/
    realized/unrealized PnL'i değiştirmemeli ve position/account state'i
    mutate etmemelidir."""

    def test_neutral_while_flat_is_no_action(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=NEUTRAL_SCORE, timestamp=T0)

        result = engine.process_signal(signal, make_price(price=100.0, as_of=T0))

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.side is PositionSide.FLAT
        assert result.position.quantity == pytest.approx(0.0)
        assert result.position.realized_pnl == pytest.approx(0.0)
        # no state was ever created for this symbol — truly untouched
        assert engine.position("BTCUSDT") is None
        assert engine.orders("BTCUSDT") == ()
        assert engine.fills("BTCUSDT") == ()

    def test_neutral_while_long_is_no_action_long_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=90.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        # position is byte-for-byte unchanged, including updated_at (no
        # "observed at this price" refresh for NEUTRAL — truly no mutation)
        assert result.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills

    def test_neutral_while_short_is_no_action_short_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=110.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills

    def test_neutral_replay_is_idempotent_no_action(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0),
            make_price(price=100.0, as_of=T0),
        )

        t1 = T0 + timedelta(minutes=5)
        neutral_signal = make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1)
        neutral_price = make_price(price=90.0, as_of=t1)

        first = engine.process_signal(neutral_signal, neutral_price)
        second = engine.process_signal(neutral_signal, neutral_price)

        assert first.orders == () and first.fills == ()
        assert second.orders == () and second.fills == ()
        assert first.position == opened.position
        assert second.position == opened.position
        assert engine.position("BTCUSDT") == opened.position
        assert engine.orders("BTCUSDT") == opened.orders
        assert engine.fills("BTCUSDT") == opened.fills


class TestIdempotency:
    def test_replaying_same_signal_returns_cached_result_without_mutation(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)

        first = engine.process_signal(signal, price)
        second = engine.process_signal(signal, price)

        assert first.idempotent_replay is False
        assert second.idempotent_replay is True
        assert second.orders == ()
        assert second.fills == ()
        assert second.position == first.position
        # engine-level state confirms no duplicate fills were recorded
        assert engine.fills("BTCUSDT") == first.fills

    def test_same_context_id_different_signal_raises_and_state_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)
        first = engine.process_signal(signal, price)

        conflicting = make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0)
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(conflicting, price)

        assert engine.position("BTCUSDT") == first.position


class TestNoLookAhead:
    def test_price_after_signal_timestamp_raises(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        future_price = make_price(price=100.0, as_of=T0 + timedelta(seconds=1))

        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(signal, future_price)

        assert engine.position("BTCUSDT") is None

    def test_cross_symbol_price_mismatch_raises(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(symbol="BTCUSDT", context_id="ctx-1", score=LONG_SCORE)
        price = make_price(symbol="ETHUSDT", price=100.0)

        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(signal, price)

    def test_out_of_order_signal_raises_and_state_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        t1 = T0 + timedelta(minutes=5)
        first = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=t1),
            make_price(price=100.0, as_of=t1),
        )

        earlier_signal = make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=T0)
        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(earlier_signal, make_price(price=90.0, as_of=T0))

        assert engine.position("BTCUSDT") == first.position
        assert engine.orders("BTCUSDT") == first.orders


class TestMultiSymbolIsolation:
    def test_symbols_are_independent_and_errors_do_not_cross(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        btc = engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-1", score=LONG_SCORE, timestamp=T0),
            make_price(symbol="BTCUSDT", price=100.0, as_of=T0),
        )
        eth = engine.process_signal(
            make_signal(symbol="ETHUSDT", context_id="ctx-eth-1", score=SHORT_SCORE, timestamp=T0),
            make_price(symbol="ETHUSDT", price=10.0, as_of=T0),
        )

        assert btc.position.symbol == "BTCUSDT"
        assert btc.position.side is PositionSide.LONG
        assert eth.position.symbol == "ETHUSDT"
        assert eth.position.side is PositionSide.SHORT

        # an out-of-order signal on ETHUSDT must not affect BTCUSDT state
        with pytest.raises(NoLookAheadViolationError):
            engine.process_signal(
                make_signal(symbol="ETHUSDT", context_id="ctx-eth-2", score=LONG_SCORE, timestamp=T0 - timedelta(minutes=1)),
                make_price(symbol="ETHUSDT", price=9.0, as_of=T0 - timedelta(minutes=1)),
            )

        assert engine.position("BTCUSDT") == btc.position
        assert engine.position("ETHUSDT") == eth.position


class TestDeterminism:
    def test_identical_input_sequence_yields_identical_ids_across_instances(self) -> None:
        def run() -> tuple:
            engine = PaperTradingEngine(notional_per_position=1000.0)
            engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))
            t1 = T0 + timedelta(minutes=5)
            engine.process_signal(make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1), make_price(price=110.0, as_of=t1))
            return engine.orders("BTCUSDT"), engine.fills("BTCUSDT")

        orders_a, fills_a = run()
        orders_b, fills_b = run()

        assert orders_a == orders_b
        assert fills_a == fills_b
        assert [o.order_id for o in orders_a] == ["BTCUSDT:ctx-1:0", "BTCUSDT:ctx-2:0", "BTCUSDT:ctx-2:1"]


class TestUnrealizedPnl:
    def test_flat_position_has_zero_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine()
        assert engine.unrealized_pnl("BTCUSDT", make_price(price=100.0)) == 0.0

    def test_long_position_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        pnl = engine.unrealized_pnl("BTCUSDT", make_price(price=110.0, as_of=T0))
        assert pnl == pytest.approx((110.0 - 100.0) * 10.0)

    def test_short_position_unrealized_pnl(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0))

        pnl = engine.unrealized_pnl("BTCUSDT", make_price(price=90.0, as_of=T0))
        assert pnl == pytest.approx((100.0 - 90.0) * 10.0)

    def test_mark_price_symbol_mismatch_raises(self) -> None:
        engine = PaperTradingEngine()
        with pytest.raises(NoLookAheadViolationError):
            engine.unrealized_pnl("BTCUSDT", make_price(symbol="ETHUSDT", price=100.0))


class TestFeeSlippageConfigValidation:
    """Faz 5 acceptance blocker fix: configurable deterministic fee/slippage."""

    @pytest.mark.parametrize("bad_value", [-0.01, float("nan"), float("inf"), float("-inf")])
    def test_invalid_fee_bps_rejected(self, bad_value: float) -> None:
        with pytest.raises(ValueError):
            PaperTradingEngine(fee_bps=bad_value)

    @pytest.mark.parametrize("bad_value", [-0.01, float("nan"), float("inf"), float("-inf")])
    def test_invalid_slippage_bps_rejected(self, bad_value: float) -> None:
        with pytest.raises(ValueError):
            PaperTradingEngine(slippage_bps=bad_value)

    def test_zero_defaults_are_valid(self) -> None:
        engine = PaperTradingEngine()
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))
        assert result.fills[0].fee == 0.0


class TestFeeAndSlippage:
    def test_buy_open_uses_slippage_formula(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        expected_fill_price = 100.0 * (1 + 25.0 / 10000)
        assert result.fills[0].side is OrderSide.BUY
        assert result.fills[0].price == pytest.approx(expected_fill_price)

    def test_sell_open_uses_slippage_formula(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE), make_price(price=100.0))

        expected_fill_price = 100.0 * (1 - 25.0 / 10000)
        assert result.fills[0].side is OrderSide.SELL
        assert result.fills[0].price == pytest.approx(expected_fill_price)

    def test_fee_formula_uses_actual_slippage_adjusted_fill_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=15.0, slippage_bps=25.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        fill = result.fills[0]
        assert fill.fee == pytest.approx(abs(fill.quantity * fill.price) * 15.0 / 10000)
        # quantity == notional / fill_price by construction, so fee == notional * fee_bps/10000
        assert fill.fee == pytest.approx(1000.0 * 15.0 / 10000)

    def test_zero_fee_and_slippage_preserves_legacy_pricing(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        assert result.fills[0].price == pytest.approx(100.0)
        assert result.fills[0].fee == pytest.approx(0.0)
        assert result.position.average_entry_price == pytest.approx(100.0)
        assert result.position.quantity == pytest.approx(10.0)

    def test_long_open_entry_uses_slippage_adjusted_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=50.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=LONG_SCORE), make_price(price=100.0))

        expected_entry = 100.0 * (1 + 50.0 / 10000)
        assert result.position.average_entry_price == pytest.approx(expected_entry)
        assert result.position.quantity == pytest.approx(1000.0 / expected_entry)

    def test_short_open_entry_uses_slippage_adjusted_price(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, slippage_bps=50.0)
        result = engine.process_signal(make_signal(context_id="ctx-1", score=SHORT_SCORE), make_price(price=100.0))

        expected_entry = 100.0 * (1 - 50.0 / 10000)
        assert result.position.average_entry_price == pytest.approx(expected_entry)
        assert result.position.quantity == pytest.approx(1000.0 / expected_entry)

    def test_long_close_realized_gross_and_net_pnl_with_fees(self) -> None:
        # Chosen so every intermediate value is a clean, exact decimal:
        # entry fill_price=110 (100*1.10), qty=10, entry_fee=11.0 (1%% of 1100)
        # exit  fill_price=135 (150*0.90), close_fee=13.5 (1%% of 1350)
        engine = PaperTradingEngine(notional_per_position=1100.0, fee_bps=100.0, slippage_bps=1000.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=150.0, as_of=t1),
        )

        close_fill = result.fills[0]
        assert close_fill.price == pytest.approx(135.0)
        assert close_fill.fee == pytest.approx(13.5)
        gross = (135.0 - 110.0) * 10.0
        expected_net = gross - 11.0 - 13.5
        assert result.position.realized_pnl == pytest.approx(expected_net)

    def test_short_close_realized_gross_and_net_pnl_with_fees(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1100.0, fee_bps=100.0, slippage_bps=1000.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=SHORT_SCORE, timestamp=T0), make_price(price=150.0, as_of=T0)
        )
        entry_fill_price = 150.0 * (1 - 1000.0 / 10000)  # 135.0
        closed_qty = 1100.0 / entry_fill_price
        entry_fee = 1100.0 * 100.0 / 10000  # 11.0

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=LONG_SCORE, timestamp=t1),
            make_price(price=100.0, as_of=t1),
        )

        close_fill = result.fills[0]
        exit_fill_price = 100.0 * (1 + 1000.0 / 10000)  # 110.0
        assert close_fill.price == pytest.approx(exit_fill_price)
        expected_close_fee = abs(closed_qty * exit_fill_price) * 100.0 / 10000
        assert close_fill.fee == pytest.approx(expected_close_fee)

        gross = (entry_fill_price - exit_fill_price) * closed_qty
        expected_net = gross - entry_fee - expected_close_fee
        assert result.position.realized_pnl == pytest.approx(expected_net)

    def test_reversal_charges_fees_on_both_legs(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1),
            make_price(price=120.0, as_of=t1),
        )

        assert len(result.fills) == 2
        close_fill, open_fill = result.fills
        assert close_fill.fee > 0.0
        assert open_fill.fee > 0.0
        assert close_fill.fee == pytest.approx(abs(close_fill.quantity * close_fill.price) * 10.0 / 10000)
        assert open_fill.fee == pytest.approx(abs(open_fill.quantity * open_fill.price) * 10.0 / 10000)
        # new position's cost basis is its own actual (slippage-adjusted) opening fill price
        assert result.position.average_entry_price == pytest.approx(open_fill.price)
        assert result.position.side is PositionSide.SHORT

    def test_no_fee_double_counting_across_consecutive_reversals(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        r1 = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )
        t1 = T0 + timedelta(minutes=5)
        r2 = engine.process_signal(
            make_signal(context_id="ctx-2", score=SHORT_SCORE, timestamp=t1), make_price(price=110.0, as_of=t1)
        )
        leg1_open, leg1_close = r1.fills[0], r2.fills[0]
        leg1_net = (leg1_close.price - leg1_open.price) * leg1_open.quantity - leg1_open.fee - leg1_close.fee
        assert r2.position.realized_pnl == pytest.approx(leg1_net)

        t2 = t1 + timedelta(minutes=5)
        r3 = engine.process_signal(
            make_signal(context_id="ctx-3", score=LONG_SCORE, timestamp=t2), make_price(price=105.0, as_of=t2)
        )
        leg2_open, leg2_close = r2.fills[1], r3.fills[0]
        leg2_net = (leg2_open.price - leg2_close.price) * leg2_open.quantity - leg2_open.fee - leg2_close.fee
        # total realized PnL is the SUM of each leg's own net — no fee is
        # counted against more than one leg, and none is dropped.
        assert r3.position.realized_pnl == pytest.approx(leg1_net + leg2_net)

    def test_idempotent_replay_charges_no_second_fee(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        signal = make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0)
        price = make_price(price=100.0, as_of=T0)

        first = engine.process_signal(signal, price)
        second = engine.process_signal(signal, price)

        assert second.idempotent_replay is True
        assert second.fills == ()
        assert engine.fills("BTCUSDT") == first.fills
        assert len(engine.fills("BTCUSDT")) == 1

    def test_neutral_charges_no_fee_and_causes_no_mutation(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=50.0, slippage_bps=100.0)
        opened = engine.process_signal(
            make_signal(context_id="ctx-1", score=LONG_SCORE, timestamp=T0), make_price(price=100.0, as_of=T0)
        )

        t1 = T0 + timedelta(minutes=5)
        result = engine.process_signal(
            make_signal(context_id="ctx-2", score=NEUTRAL_SCORE, timestamp=t1),
            make_price(price=90.0, as_of=t1),
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position == opened.position
        assert engine.fills("BTCUSDT") == opened.fills
        assert engine.position("BTCUSDT") == opened.position

    def test_multi_symbol_fee_accounting_is_isolated(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        btc = engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-1", score=LONG_SCORE, timestamp=T0),
            make_price(symbol="BTCUSDT", price=100.0, as_of=T0),
        )
        eth = engine.process_signal(
            make_signal(symbol="ETHUSDT", context_id="ctx-eth-1", score=SHORT_SCORE, timestamp=T0),
            make_price(symbol="ETHUSDT", price=10.0, as_of=T0),
        )

        assert engine.fills("BTCUSDT")[0].symbol == "BTCUSDT"
        assert engine.fills("ETHUSDT")[0].symbol == "ETHUSDT"
        assert engine.fills("BTCUSDT")[0].fee != engine.fills("ETHUSDT")[0].fee

        t1 = T0 + timedelta(minutes=5)
        engine.process_signal(
            make_signal(symbol="BTCUSDT", context_id="ctx-btc-2", score=SHORT_SCORE, timestamp=t1),
            make_price(symbol="BTCUSDT", price=120.0, as_of=t1),
        )
        # closing/reversing BTCUSDT must not touch ETHUSDT's recorded fills/fees
        assert engine.fills("ETHUSDT") == eth.fills


=== FILE: tests/test_paper_trading_models.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.paper_trading.models import (
    MarketPriceSnapshot,
    OrderSide,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperTradingResult,
    PositionSide,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


class TestMarketPriceSnapshot:
    def test_normalizes_symbol(self) -> None:
        snap = MarketPriceSnapshot(symbol="btcusdt", price=100.0, as_of=T0)
        assert snap.symbol == "BTCUSDT"

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=100.0, as_of=datetime(2026, 8, 31))

    def test_rejects_non_positive_price(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=0.0, as_of=T0)
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=-5.0, as_of=T0)

    def test_rejects_non_finite_price(self) -> None:
        with pytest.raises(ValueError):
            MarketPriceSnapshot(symbol="BTCUSDT", price=float("nan"), as_of=T0)


class TestPaperOrder:
    def test_rejects_non_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            PaperOrder(
                order_id="X:ctx:0", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=0.0, signal_context_id="ctx", created_at=T0,
            )

    def test_rejects_empty_order_id(self) -> None:
        with pytest.raises(ValueError):
            PaperOrder(
                order_id="  ", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, signal_context_id="ctx", created_at=T0,
            )


class TestPaperFill:
    def test_rejects_non_positive_price(self) -> None:
        with pytest.raises(ValueError):
            PaperFill(
                fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, price=0.0, fee=0.0, filled_at=T0,
            )

    def test_rejects_negative_fee(self) -> None:
        with pytest.raises(ValueError):
            PaperFill(
                fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
                quantity=1.0, price=100.0, fee=-0.01, filled_at=T0,
            )

    def test_zero_fee_is_valid(self) -> None:
        fill = PaperFill(
            fill_id="f", order_id="o", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=1.0, price=100.0, fee=0.0, filled_at=T0,
        )
        assert fill.fee == 0.0


class TestPaperPosition:
    def test_flat_requires_zero_quantity_and_entry(self) -> None:
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.FLAT, quantity=1.0,
                average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
            )
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
                average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
            )

    def test_flat_zero_zero_is_valid(self) -> None:
        pos = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
        )
        assert pos.side is PositionSide.FLAT

    def test_long_requires_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            PaperPosition(
                symbol="BTCUSDT", side=PositionSide.LONG, quantity=0.0,
                average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
            )


class TestPaperTradingResult:
    def test_coerces_orders_and_fills_to_tuple(self) -> None:
        pos = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=0.0, updated_at=T0,
        )
        result = PaperTradingResult(
            symbol="BTCUSDT", position=pos, orders=[], fills=[], idempotent_replay=False,
        )
        assert isinstance(result.orders, tuple)
        assert isinstance(result.fills, tuple)


=== FILE: tests/test_paper_trading_notional_override.py ===
"""
Pre-Audit Enhancement Pass — tests for the additive, backward-compatible
`PaperTradingEngine.process_signal(..., notional_override=...)` and
`.has_processed()` extensions. `tests/test_paper_trading_engine.py`
(accepted, Phase 5) is left completely untouched — this is a NEW,
separate file so that file's own diff stays zero.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, PositionSide

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)

LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def make_signal(symbol="BTCUSDT", timestamp=T0, context_id="ctx-0", score=0.0, model_version="test-v1") -> Signal:
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version=model_version,
    )


def make_price(symbol="BTCUSDT", price=100.0, as_of=T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


class TestDefaultBehaviourUnchanged:
    def test_no_override_matches_pre_extension_behaviour_byte_for_byte(self) -> None:
        engine_a = PaperTradingEngine(notional_per_position=1000.0)
        engine_b = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        price = make_price()

        result_a = engine_a.process_signal(signal, price)
        result_b = engine_b.process_signal(signal, price, notional_override=None)
        assert result_a == result_b
        assert result_a.position.quantity == 10.0  # 1000 notional / 100 price


class TestOverrideAppliesOnlyToFreshOpen:
    def test_override_changes_opened_quantity(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        result = engine.process_signal(signal, make_price(price=100.0), notional_override=500.0)
        assert result.position.quantity == 5.0  # 500 / 100

    def test_zero_or_negative_override_fails_closed(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=0.0)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=-10.0)

    def test_nan_override_fails_closed(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=float("nan"))

    def test_inf_override_fails_closed(self) -> None:
        """Regression (BLOCKER FIX, Decision 90, item 6): a BRAND-NEW
        context must still fail closed on an invalid override — this
        must NOT regress just because validation moved deeper into the
        control flow."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE)
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=float("inf"))
        with pytest.raises(ValueError):
            engine.process_signal(signal, make_price(), notional_override=0.0)


class TestIdempotencyIgnoresOverrideOnReplay:
    """Regression suite for the BLOCKER FIX (Decision 90, independent
    acceptance review finding): an already-processed (symbol,
    context_id) replay must ALWAYS return the cached result untouched,
    regardless of whether the replay's `notional_override` is a
    different valid value, or outright invalid (0/NaN/inf) — the
    override must never even be evaluated once idempotency has already
    established this is a replay, not a new transition."""

    def test_replay_with_different_override_returns_original_unchanged(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price(), notional_override=500.0)
        second = engine.process_signal(signal, make_price(), notional_override=9999.0)
        assert second.idempotent_replay is True
        assert second.position.quantity == first.position.quantity == 5.0  # override IGNORED on replay

    def test_replay_with_zero_override_returns_cached_result_no_raise(self) -> None:
        """Item 2: processed context + zero override -> cached result
        (must NOT raise ValueError, even though 0.0 is an invalid
        notional in isolation)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=0.0)
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_nan_override_returns_cached_result_no_raise(self) -> None:
        """Item 3."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=float("nan"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_inf_override_returns_cached_result_no_raise(self) -> None:
        """Item 4."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=float("inf"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_replay_with_negative_override_returns_cached_result_no_raise(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        first = engine.process_signal(signal, make_price())
        second = engine.process_signal(signal, make_price(), notional_override=-50.0)
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_no_additional_order_fill_fee_across_all_invalid_override_replays(self) -> None:
        """Item 5: none of the replay cases (different-valid / zero /
        NaN / inf / negative override) may produce a new order, fill,
        fee, or allocation — `orders`/`fills` must be empty on every
        replay result, and the engine's cumulative order/fill history
        for the symbol must stay at exactly one entry (the original
        open)."""
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())

        for override in (500.0, 9999.0, 0.0, float("nan"), float("inf"), -50.0):
            replay_result = engine.process_signal(signal, make_price(), notional_override=override)
            assert replay_result.orders == ()
            assert replay_result.fills == ()
            assert replay_result.idempotent_replay is True

        assert len(engine.orders("BTCUSDT")) == 1
        assert len(engine.fills("BTCUSDT")) == 1

    def test_conflicting_signal_content_still_raises(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1")
        s2 = make_signal(score=SHORT_SCORE, context_id="c1")  # same context_id, different content
        engine.process_signal(s1, make_price(), notional_override=500.0)
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(), notional_override=500.0)

    def test_conflicting_signal_raises_conflict_error_not_swallowed_by_invalid_override(self) -> None:
        """Item 7 (unchanged conflict semantics): a conflicting replay
        must still raise `IdempotencyConflictError` — and specifically
        NOT get reinterpreted as "ignore the invalid override, return
        cached result" just because its own `notional_override` also
        happens to be invalid. Conflict detection runs before the
        override is ever consulted, so this must raise the conflict
        error, never a ValueError and never a silent cached return."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1")
        s2 = make_signal(score=SHORT_SCORE, context_id="c1")
        engine.process_signal(s1, make_price())
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(), notional_override=float("nan"))


class TestReversalAccountingCorrectWithOverride:
    def test_reversal_realized_pnl_uses_correct_notional_on_each_leg(self) -> None:
        engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=5.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        s2 = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        opened = engine.process_signal(s1, make_price(price=100.0, as_of=T0), notional_override=500.0)
        result = engine.process_signal(
            s2, make_price(price=110.0, as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )
        assert len(result.fills) == 2
        close_fill, open_fill = result.fills
        opened_quantity = opened.position.quantity  # slippage-adjusted quantity from the FIRST leg
        assert close_fill.quantity == pytest.approx(opened_quantity)  # closes the ORIGINAL 500-notional leg exactly
        assert open_fill.quantity == pytest.approx(250.0 / open_fill.price)  # opens the NEW 250-notional leg
        assert close_fill.fee > 0 and open_fill.fee > 0  # fees still applied


class TestHasProcessed:
    def test_false_before_processing(self) -> None:
        engine = PaperTradingEngine()
        assert engine.has_processed("BTCUSDT", "c1") is False

    def test_true_after_processing_directional_signal(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())
        assert engine.has_processed("BTCUSDT", "c1") is True

    def test_false_for_neutral_signal_no_action(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=0.0, context_id="c1")  # NEUTRAL
        engine.process_signal(signal, make_price())
        assert engine.has_processed("BTCUSDT", "c1") is False

    def test_read_only_does_not_mutate_state(self) -> None:
        engine = PaperTradingEngine()
        signal = make_signal(score=LONG_SCORE, context_id="c1")
        engine.process_signal(signal, make_price())
        position_before = engine.position("BTCUSDT")
        engine.has_processed("BTCUSDT", "c1")
        engine.has_processed("BTCUSDT", "unknown-context")
        assert engine.position("BTCUSDT") == position_before


class TestBrandNewContextAlwaysValidatesRegardlessOfEventualOutcome:
    """Regression suite for the SECOND-ROUND blocker fix (Decision 91,
    independent acceptance review finding): validation was moved all the
    way down to the `_open_position()` call site by Decision 90, which
    fixed idempotent-replay/conflict cases but left a remaining gap — a
    BRAND-NEW context whose direction matches an already-open position
    takes the same-direction NO_ACTION `else` branch (no `_open_position`
    call at all), so an invalid `notional_override` on that path was
    never validated. These tests prove: once idempotency/conflict
    resolution has established a signal is genuinely NEW (not a replay,
    not a conflict), `notional_override` is validated unconditionally —
    BEFORE branching into same-direction/open/reversal behavior — while
    the already-processed-replay and conflicting-replay cases (Decision
    90) remain completely unaffected."""

    def _engine_with_existing_long(self) -> PaperTradingEngine:
        engine = PaperTradingEngine(notional_per_position=1000.0)
        engine.process_signal(make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0), make_price(as_of=T0))
        return engine

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_same_direction_brand_new_context_invalid_override_fails_closed(self, bad_override: float) -> None:
        """Items 1-4: existing LONG + brand-new LONG context + invalid
        override -> ValueError, even though this signal would otherwise
        take the same-direction NO_ACTION path and never call
        `_open_position` at all."""
        engine = self._engine_with_existing_long()
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))
        with pytest.raises(ValueError):
            engine.process_signal(new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override)

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_same_direction_invalid_override_produces_no_mutation(self, bad_override: float) -> None:
        """Item 5: the rejected call must leave state completely
        untouched — no new order/fill, no position/timestamp change,
        and the (never-processed) new context_id must not be recorded."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        orders_before = engine.orders("BTCUSDT")
        fills_before = engine.fills("BTCUSDT")
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        with pytest.raises(ValueError):
            engine.process_signal(new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override)

        assert engine.position("BTCUSDT") == position_before
        assert engine.orders("BTCUSDT") == orders_before
        assert engine.fills("BTCUSDT") == fills_before
        assert engine.has_processed("BTCUSDT", "c2") is False

    def test_same_direction_brand_new_context_valid_override_retains_no_action_behaviour(self) -> None:
        """Item 6: a VALID override on a brand-new same-direction context
        must not error, and must retain the accepted Phase 5
        same-direction NO_ACTION semantics exactly — no new order/fill,
        only the position's `updated_at` timestamp refreshes."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        new_signal = make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        result = engine.process_signal(
            new_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )

        assert result.orders == ()
        assert result.fills == ()
        assert result.position.quantity == position_before.quantity  # unchanged -- override was NEVER consumed here
        assert result.position.average_entry_price == position_before.average_entry_price
        assert result.position.updated_at == T0 + timedelta(minutes=5)
        assert engine.has_processed("BTCUSDT", "c2") is True

    def test_already_processed_identical_replay_with_invalid_override_still_cached(self) -> None:
        """Item 7 (unaffected by this fix — Decision 90's guarantee)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        signal = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        first = engine.process_signal(signal, make_price(as_of=T0))
        second = engine.process_signal(signal, make_price(as_of=T0), notional_override=float("nan"))
        assert second.idempotent_replay is True
        assert second.position == first.position

    def test_conflicting_replay_with_invalid_override_still_raises_conflict(self) -> None:
        """Item 8 (unaffected by this fix — Decision 90's guarantee)."""
        engine = PaperTradingEngine(notional_per_position=1000.0)
        s1 = make_signal(score=LONG_SCORE, context_id="c1", timestamp=T0)
        s2 = make_signal(score=SHORT_SCORE, context_id="c1", timestamp=T0)
        engine.process_signal(s1, make_price(as_of=T0))
        with pytest.raises(IdempotencyConflictError):
            engine.process_signal(s2, make_price(as_of=T0), notional_override=float("inf"))

    @pytest.mark.parametrize("bad_override", [0.0, float("nan"), float("inf"), -10.0])
    def test_reversal_brand_new_context_invalid_override_fails_closed_before_mutation(
        self, bad_override: float
    ) -> None:
        """Item 9: a brand-new context that would REVERSE an existing
        position must also fail closed on an invalid override, and must
        do so before any state mutation — the existing LONG position
        must remain exactly as it was (not partially closed)."""
        engine = self._engine_with_existing_long()
        position_before = engine.position("BTCUSDT")
        reversal_signal = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))

        with pytest.raises(ValueError):
            engine.process_signal(
                reversal_signal, make_price(as_of=T0 + timedelta(minutes=5)), notional_override=bad_override
            )

        assert engine.position("BTCUSDT") == position_before  # still LONG, untouched -- no partial close
        assert engine.has_processed("BTCUSDT", "c2") is False

    def test_reversal_brand_new_context_valid_override_still_works(self) -> None:
        engine = self._engine_with_existing_long()
        reversal_signal = make_signal(score=SHORT_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5))
        result = engine.process_signal(
            reversal_signal, make_price(price=110.0, as_of=T0 + timedelta(minutes=5)), notional_override=250.0
        )
        assert len(result.fills) == 2
        assert result.position.side is PositionSide.SHORT

    def test_default_fixed_notional_behaviour_unchanged(self) -> None:
        """Item 10: no override at all -- brand-new same-direction and
        brand-new reversal contexts behave exactly as accepted Phase 5
        (fixed notional, no ValueError from this validation at all)."""
        engine = self._engine_with_existing_long()
        same_direction = engine.process_signal(
            make_signal(score=LONG_SCORE, context_id="c2", timestamp=T0 + timedelta(minutes=5)),
            make_price(as_of=T0 + timedelta(minutes=5)),
        )
        assert same_direction.orders == ()

        reversal = engine.process_signal(
            make_signal(score=SHORT_SCORE, context_id="c3", timestamp=T0 + timedelta(minutes=10)),
            make_price(price=110.0, as_of=T0 + timedelta(minutes=10)),
        )
        assert len(reversal.fills) == 2
        assert reversal.position.quantity == pytest.approx(1000.0 / 110.0)  # fixed notional default


=== FILE: tests/test_persistence_paper_state_store.py ===
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide
from crypto_signal_engine.persistence.errors import CorruptRecordError, SchemaVersionMismatchError
from crypto_signal_engine.persistence.paper_state_store import SCHEMA_VERSION, PaperStateStore

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def _signal(context_id="ctx-1") -> Signal:
    return Signal(
        symbol="BTCUSDT", timestamp=T0, context_id=context_id, score=0.5, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="test-v1",
    )


def _position(**overrides) -> PaperPosition:
    defaults = dict(
        symbol="BTCUSDT", side=PositionSide.LONG, quantity=10.0,
        average_entry_price=100.0, realized_pnl=0.0, updated_at=T0,
    )
    defaults.update(overrides)
    return PaperPosition(**defaults)


class TestPaperStateStoreBasics:
    def test_fresh_database_has_no_paper_state(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.load_paper_state("BTCUSDT") is None
        store.close()

    def test_fresh_database_has_no_candle_checkpoint(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) is None
        store.close()

    def test_checkpoint_and_load_position_round_trip(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        position = _position()
        fill = PaperFill(
            fill_id="f1", order_id="o1", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=10.0, price=100.0, fee=1.0, filled_at=T0,
        )
        order = PaperOrder(
            order_id="o1", symbol="BTCUSDT", side=OrderSide.BUY,
            quantity=10.0, signal_context_id="ctx-1", created_at=T0,
        )
        signal = _signal()

        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=position, entry_fee=2.5, last_signal_timestamp=T0,
            new_context_entries=[("ctx-1", signal, position)], new_fills=(fill,), new_orders=(order,),
        )

        snapshot = store.load_paper_state("BTCUSDT")
        assert snapshot is not None
        assert snapshot.position == position
        assert snapshot.entry_fee == 2.5
        assert snapshot.last_signal_timestamp == T0
        assert "ctx-1" in snapshot.processed_context_ids
        restored_signal, restored_result = snapshot.processed_context_ids["ctx-1"]
        assert restored_signal == signal
        assert restored_result.position == position
        assert restored_result.idempotent_replay is False
        assert snapshot.fills == (fill,)
        assert snapshot.orders == (order,)
        store.close()

    def test_checkpoint_upserts_position_not_duplicates(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(realized_pnl=0.0), entry_fee=1.0,
            last_signal_timestamp=T0, new_context_entries=[], new_fills=(), new_orders=(),
        )
        later = T0 + timedelta(minutes=5)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(realized_pnl=50.0, updated_at=later), entry_fee=3.0,
            last_signal_timestamp=later, new_context_entries=[], new_fills=(), new_orders=(),
        )

        snapshot = store.load_paper_state("BTCUSDT")
        assert snapshot.position.realized_pnl == 50.0
        assert snapshot.entry_fee == 3.0
        assert snapshot.last_signal_timestamp == later
        store.close()

    def test_candle_checkpoint_round_trip(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == T0

        later = T0 + timedelta(minutes=5)
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, later)
        assert store.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == later
        store.close()

    def test_symbols_are_isolated(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT"), entry_fee=1.0,
            last_signal_timestamp=T0, new_context_entries=[], new_fills=(), new_orders=(),
        )
        assert store.load_paper_state("ETHUSDT") is None
        assert store.load_paper_state("BTCUSDT") is not None
        store.close()


class TestListOpenPositionSymbols:
    """`list_open_position_symbols()` — otomatik sembol seçiminin Bölüm A
    ("Existing open PAPER positions") pinning kuralının okuma tarafı."""

    def test_empty_database_returns_empty_tuple(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        assert store.list_open_position_symbols() == ()
        store.close()

    def test_only_non_flat_positions_are_returned(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT", side=PositionSide.LONG),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.checkpoint_paper_state(
            symbol="ETHUSDT",
            position=_position(symbol="ETHUSDT", side=PositionSide.FLAT, quantity=0.0, average_entry_price=0.0),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.checkpoint_paper_state(
            symbol="BNBUSDT", position=_position(symbol="BNBUSDT", side=PositionSide.SHORT),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        assert store.list_open_position_symbols() == ("BNBUSDT", "BTCUSDT")  # alfabetik, deterministik
        store.close()

    def test_is_read_only_does_not_mutate_state(self, tmp_path) -> None:
        store = PaperStateStore(tmp_path / "state.db")
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(symbol="BTCUSDT", side=PositionSide.LONG),
            entry_fee=0.0, last_signal_timestamp=None, new_context_entries=[], new_fills=(), new_orders=(),
        )
        before = store.load_paper_state("BTCUSDT")
        store.list_open_position_symbols()
        after = store.load_paper_state("BTCUSDT")
        assert before == after
        store.close()


class TestSchemaVersioning:
    def test_reopening_same_version_succeeds(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        store1.close()
        store2 = PaperStateStore(db_path)  # must not raise
        store2.close()

    def test_schema_version_mismatch_raises_on_open(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))
        conn.commit()
        conn.close()

        with pytest.raises(SchemaVersionMismatchError):
            PaperStateStore(db_path)


class TestCorruptRecordHandling:
    def test_corrupt_position_side_raises_not_silently_skipped(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(), entry_fee=1.0, last_signal_timestamp=T0,
            new_context_entries=[], new_fills=(), new_orders=(),
        )
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE paper_position SET side = 'NOT_A_REAL_SIDE' WHERE symbol = 'BTCUSDT'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_paper_state("BTCUSDT")
        reopened.close()

    def test_corrupt_context_json_raises(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_paper_state(
            symbol="BTCUSDT", position=_position(), entry_fee=1.0, last_signal_timestamp=T0,
            new_context_entries=[("ctx-1", _signal(), _position())], new_fills=(), new_orders=(),
        )
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE processed_context SET signal_json = 'not valid json{{' WHERE context_id = 'ctx-1'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_paper_state("BTCUSDT")
        reopened.close()

    def test_corrupt_candle_checkpoint_raises(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)
        store.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE candle_checkpoint SET last_open_time = 'not-a-timestamp'")
        conn.commit()
        conn.close()

        reopened = PaperStateStore(db_path)
        with pytest.raises(CorruptRecordError):
            reopened.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
        reopened.close()


class TestFailureIsExplicit:
    def test_checkpoint_after_close_raises_persistence_error(self, tmp_path) -> None:
        from crypto_signal_engine.errors import PersistenceError

        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        store.close()

        with pytest.raises(PersistenceError):
            store.checkpoint_candle("BTCUSDT", Timeframe.M5, T0)


=== FILE: tests/test_persistence_recovery.py ===
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import RiskLevel, Timeframe
from crypto_signal_engine.domain.models import Signal
from crypto_signal_engine.errors import IdempotencyConflictError, PersistenceError
from crypto_signal_engine.paper_trading.engine import PaperTradingEngine
from crypto_signal_engine.paper_trading.models import MarketPriceSnapshot, PositionSide
from crypto_signal_engine.persistence.errors import CorruptRecordError
from crypto_signal_engine.persistence.paper_state_store import PaperStateStore
from crypto_signal_engine.persistence.recovery import PersistedRuntime, restore_paper_engine
from crypto_signal_engine.runtime.coordinator import RuntimeCoordinator
from crypto_signal_engine.runtime.models import IngestOutcome, RuntimeHealth
from tests.conftest import run_async
from tests.runtime_fakes import FakeLiveDataProvider, make_candle, make_candle_series_ending_at, make_order_book

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)
END = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
WARMUP = 20

LONG_SCORE = 0.5
SHORT_SCORE = -0.5


def _signal(symbol="BTCUSDT", context_id="ctx-1", score=LONG_SCORE, timestamp=T0) -> Signal:
    return Signal(
        symbol=symbol, timestamp=timestamp, context_id=context_id, score=score, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5, supporting_factors=(),
        contradicting_factors=(), invalidation=None, model_version="test-v1",
    )


def _price(symbol="BTCUSDT", price=100.0, as_of=T0) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(symbol=symbol, price=price, as_of=as_of)


def _checkpoint_from_result(store: PaperStateStore, engine: PaperTradingEngine, symbol: str, signal: Signal, result) -> None:
    """Testlerde `PersistedRuntime._checkpoint_paper_transition` ile AYNI
    şeyi doğrudan Faz 5 seviyesinde yapan küçük bir yardımcı (tam
    RuntimeCoordinator pipeline'ı kurmadan Faz 5 restart-idempotency
    sözleşmesini izole test etmek için)."""
    state = engine._states[symbol]  # noqa: SLF001 (test introspection, aynı desen coordinator.py'de de var)
    store.checkpoint_paper_state(
        symbol=symbol, position=result.position, entry_fee=state.entry_fee,
        last_signal_timestamp=state.last_signal_timestamp,
        new_context_entries=[(signal.context_id, signal, result.position)],
        new_fills=result.fills, new_orders=result.orders,
    )


class TestRestartIdempotency:
    """Blocker-level: process signal/context A -> persist -> brand new
    engine/store objects -> recover -> replay A -> no duplicate mutation."""

    def test_replaying_same_context_after_restart_causes_no_duplicate_mutation(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        signal = _signal(context_id="ctx-A")
        price = _price()

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        result1 = engine1.process_signal(signal, price)
        assert len(result1.fills) == 1
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result1)
        store1.close()

        # entirely new objects
        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT"])
        assert restored == {"BTCUSDT": True}

        replay_result = engine2.process_signal(signal, price)

        assert replay_result.idempotent_replay is True
        assert replay_result.orders == ()
        assert replay_result.fills == ()
        assert replay_result.position == result1.position
        assert engine2.fills("BTCUSDT") == result1.fills  # nothing duplicated
        store2.close()

    def test_conflicting_replay_after_restart_preserves_phase5_conflict_behavior(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        signal = _signal(context_id="ctx-A", score=LONG_SCORE)
        price = _price()

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        result1 = engine1.process_signal(signal, price)
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result1)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restore_paper_engine(store2, engine2, ["BTCUSDT"])

        conflicting = _signal(context_id="ctx-A", score=SHORT_SCORE)
        with pytest.raises(IdempotencyConflictError):
            engine2.process_signal(conflicting, price)

        # state must remain exactly as recovered, not corrupted by the conflict attempt
        assert engine2.position("BTCUSDT") == result1.position
        store2.close()


class TestOpenPositionRecovery:
    """Blocker-level: open position (with fee/slippage), persist, destroy
    old objects, recover, close/reverse — PnL/fees must match an
    uninterrupted equivalent run. Entry fee/average entry must NOT reset."""

    def test_recovered_close_matches_uninterrupted_run(self, tmp_path) -> None:
        open_signal = _signal(context_id="ctx-open", score=LONG_SCORE, timestamp=T0)
        close_signal = _signal(context_id="ctx-close", score=SHORT_SCORE, timestamp=T0 + timedelta(minutes=5))
        open_price = _price(price=100.0, as_of=T0)
        close_price = _price(price=120.0, as_of=T0 + timedelta(minutes=5))

        # reference: uninterrupted single-process run
        reference_engine = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        reference_engine.process_signal(open_signal, open_price)
        reference_result = reference_engine.process_signal(close_signal, close_price)

        # interrupted run: open, persist, destroy, recover, close
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        open_result = engine1.process_signal(open_signal, open_price)
        assert open_result.position.side is PositionSide.LONG
        _checkpoint_from_result(store1, engine1, "BTCUSDT", open_signal, open_result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0, fee_bps=10.0, slippage_bps=20.0)
        restore_paper_engine(store2, engine2, ["BTCUSDT"])

        recovered_position_before_close = engine2.position("BTCUSDT")
        assert recovered_position_before_close.average_entry_price == pytest.approx(open_result.position.average_entry_price)

        recovered_result = engine2.process_signal(close_signal, close_price)

        assert recovered_result.position.side == reference_result.position.side
        assert recovered_result.position.realized_pnl == pytest.approx(reference_result.position.realized_pnl)
        assert recovered_result.position.average_entry_price == pytest.approx(
            reference_result.position.average_entry_price
        )
        store2.close()


class TestMultiSymbolRecovery:
    def test_two_symbols_recover_in_isolation(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        btc_signal = _signal(symbol="BTCUSDT", context_id="ctx-btc", score=LONG_SCORE, timestamp=T0)
        eth_signal = _signal(symbol="ETHUSDT", context_id="ctx-eth", score=SHORT_SCORE, timestamp=T0)

        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        btc_result = engine1.process_signal(btc_signal, _price("BTCUSDT", 100.0, T0))
        eth_result = engine1.process_signal(eth_signal, _price("ETHUSDT", 10.0, T0))
        _checkpoint_from_result(store1, engine1, "BTCUSDT", btc_signal, btc_result)
        _checkpoint_from_result(store1, engine1, "ETHUSDT", eth_signal, eth_result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT", "ETHUSDT"])
        assert restored == {"BTCUSDT": True, "ETHUSDT": True}

        assert engine2.position("BTCUSDT").side is PositionSide.LONG
        assert engine2.position("ETHUSDT").side is PositionSide.SHORT

        # replaying BTC's context must not touch ETH's recovered state
        engine2.process_signal(btc_signal, _price("BTCUSDT", 100.0, T0))
        assert engine2.position("ETHUSDT") == eth_result.position
        store2.close()

    def test_symbol_never_persisted_is_not_restored(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        signal = _signal(symbol="BTCUSDT")
        result = engine1.process_signal(signal, _price("BTCUSDT", 100.0, T0))
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result)
        store1.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        restored = restore_paper_engine(store2, engine2, ["BTCUSDT", "ETHUSDT"])

        assert restored == {"BTCUSDT": True, "ETHUSDT": False}
        assert engine2.position("ETHUSDT") is None  # no fabricated state
        store2.close()


class TestRecoveryCorruptionSafety:
    def test_corrupt_persisted_record_blocks_recovery_rather_than_resetting(self, tmp_path) -> None:
        import sqlite3

        db_path = tmp_path / "state.db"
        store1 = PaperStateStore(db_path)
        engine1 = PaperTradingEngine(notional_per_position=1000.0)
        signal = _signal()
        result = engine1.process_signal(signal, _price())
        _checkpoint_from_result(store1, engine1, "BTCUSDT", signal, result)
        store1.close()

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE paper_position SET side = 'GARBAGE'")
        conn.commit()
        conn.close()

        store2 = PaperStateStore(db_path)
        engine2 = PaperTradingEngine(notional_per_position=1000.0)
        with pytest.raises(CorruptRecordError):
            restore_paper_engine(store2, engine2, ["BTCUSDT"])

        # the engine must NOT have been silently seeded with empty/fresh state
        assert engine2.position("BTCUSDT") is None
        store2.close()


def _bootstrap_fully(coordinator: RuntimeCoordinator, symbol: str, end: datetime) -> None:
    for timeframe in coordinator._candle_timeframes:  # noqa: SLF001
        candles = make_candle_series_ending_at(symbol, timeframe, end, WARMUP)
        report = coordinator.bootstrap_candles(symbol, timeframe, candles, as_of=end)
        assert report.ready
    coordinator.ingest_order_book(symbol, make_order_book(symbol, end))


def _make_coordinator(symbols=("BTCUSDT",), **kwargs) -> RuntimeCoordinator:
    provider = kwargs.pop("provider", None) or FakeLiveDataProvider()
    return RuntimeCoordinator(symbols=symbols, provider=provider, warmup_candles=WARMUP, **kwargs)


def _symbol_health(coordinator: RuntimeCoordinator, symbol: str) -> RuntimeHealth:
    status = coordinator.status()
    return next(s for s in status.symbols if s.symbol == symbol).health


class _RangeAwareProvider(FakeLiveDataProvider):
    """`FakeLiveDataProvider.fetch_historical_candles` (varsayılan) scripted
    listeyi `start`/`end`'i YOK SAYARAK döner — bu, `recover()`'ın GERÇEKTEN
    doğru `[start, end)` penceresini istediğini kanıtlamak için YETERSİZDİR
    (bir GERÇEK PUBLIC REST endpoint'i istenmeyen aralık dışındaki veriyi
    DÖNMEZ). Bu fake, gerçek bir REST endpoint'i gibi yalnızca `[start,
    end)` içindeki candle'ları döner — restart-recovery penceresinin
    GERÇEKTEN yeterli lookback istediğini doğrulayan blocker regresyon
    testi için kullanılır."""

    async def fetch_historical_candles(self, symbol, timeframe, start, end):
        self.fetch_calls.append((symbol, timeframe, start, end))
        candles = self._historical_candles.get((symbol, timeframe), [])
        return [c for c in candles if start <= c.open_time < end]


class TestRestartRecoveryRebuildsWarmupHistory:
    """BLOCKER (bağımsız acceptance review bulgusu): restart recovery,
    checkpoint VARKEN yalnızca dar `[checkpoint, now)` aralığını çekiyordu
    — bu aralık normalde `warmup_candles` gereksinimini KARŞILAMAYA
    YETMEZ, restart'ı gereksiz yere BOOTSTRAPPING'de bırakıyordu (GERÇEK
    PUBLIC geçmiş veri MEVCUT olsa bile). Fix: checkpoint VARKEN de,
    checkpoint'ten ÖNCEYE doğru aynı warmup-lookback penceresi (`checkpoint
    is None` dalıyla AYNI formül) eklenir — `[checkpoint - lookback, now)`."""

    def test_restart_with_existing_checkpoint_rebuilds_warmup_and_reaches_ready_immediately(
        self, tmp_path
    ) -> None:
        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            # Faz canlı candle: checkpoint'i "now"a YAKIN bir noktaya taşır.
            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            event = runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)
            assert event.outcome is IngestOutcome.ACCEPTED
            checkpoint_before = store1.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
            assert checkpoint_before == live_candle.open_time
            state_before = store1.load_paper_state("BTCUSDT")
            store1.close()

            # TÜM runtime/paper nesnelerini yok et, AYNI SQLite DB ile
            # baştan kur (gerçek bir process restart'ını simüle eder).
            now2 = END + timedelta(minutes=5)  # restart, son canlı candle'dan KISA süre sonra
            store2 = PaperStateStore(db_path)
            full_m5_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP) + [live_candle]
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): full_m5_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready, f"{report.symbol}/{report.timeframe.value} warmup yeniden inşa edilemedi"

            # M1 order-book YENİDEN edinilmeli (Faz 6/7 hiçbir zaman
            # order-book state'i persist ETMEZ — bu KASITLI).
            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))

            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.READY

            # checkpoint GERİYE gitmedi.
            checkpoint_after = store2.load_candle_checkpoint("BTCUSDT", Timeframe.M5)
            assert checkpoint_after == checkpoint_before

            # candle window GERÇEKTEN warmup'a sahip (fabrikasyon DEĞİL,
            # `_RangeAwareProvider`'ın GERÇEKTEN döndürdüğü candle'lardan).
            window = coordinator2._candle_windows[("BTCUSDT", Timeframe.M5)]
            assert len(window.history()) >= WARMUP

            # historical rebuild hiçbir YENİ paper order/fill/PnL ÜRETMEDİ
            # (state reconstruction, retroactive trading DEĞİL).
            state_after = store2.load_paper_state("BTCUSDT")
            assert len(state_after.fills) == len(state_before.fills)
            assert len(state_after.orders) == len(state_before.orders)
            assert state_after.position.realized_pnl == state_before.position.realized_pnl
            assert set(state_after.processed_context_ids) == set(state_before.processed_context_ids)

            store2.close()

        run_async(scenario())

    def test_restart_recovery_preserves_multi_symbol_isolation(self, tmp_path) -> None:
        """İkinci sembol HİÇ canlı candle görmedi (checkpoint hâlâ `None`)
        — bu, BİRİNCİ sembolün checkpoint-bazlı geniş-pencere recovery'siyle
        AYNI `recover()` çağrısında karışmadan doğru işlenmeli."""

        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"), clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            _bootstrap_fully(coordinator1, "ETHUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)  # yalnızca BTCUSDT checkpoint alır
            store1.close()

            now2 = END + timedelta(minutes=5)
            store2 = PaperStateStore(db_path)
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP)
                    + [live_candle],
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                    ("ETHUSDT", Timeframe.M5): make_candle_series_ending_at("ETHUSDT", Timeframe.M5, now2, WARMUP),
                    ("ETHUSDT", Timeframe.M15): make_candle_series_ending_at("ETHUSDT", Timeframe.M15, now2, WARMUP),
                    ("ETHUSDT", Timeframe.H1): make_candle_series_ending_at("ETHUSDT", Timeframe.H1, now2, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(symbols=("BTCUSDT", "ETHUSDT"), provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            for report in reports:
                assert report.ready

            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))
            coordinator2.ingest_order_book("ETHUSDT", make_order_book("ETHUSDT", now2))

            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.READY
            assert _symbol_health(coordinator2, "ETHUSDT") is RuntimeHealth.READY
            assert store2.load_candle_checkpoint("ETHUSDT", Timeframe.M5) is None  # hiç canlı checkpoint YOK

            store2.close()

        run_async(scenario())

    def test_genuinely_insufficient_public_history_stays_bootstrapping_not_fabricated_ready(
        self, tmp_path
    ) -> None:
        """PUBLIC REST kaynağı GERÇEKTEN yetersizse (yalnızca birkaç
        candle mevcutsa), sembol dürüstçe BOOTSTRAPPING kalmalı — SAHTE
        bir READY asla ÜRETİLMEMELİDİR."""

        async def scenario() -> None:
            from crypto_signal_engine.providers.binance.clock import FixedClock

            db_path = tmp_path / "state.db"
            clock1 = FixedClock(END)
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator(clock=clock1)
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            live_candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            runtime1.ingest_candle("BTCUSDT", Timeframe.M5, live_candle)
            store1.close()

            now2 = END + timedelta(minutes=5)
            store2 = PaperStateStore(db_path)
            # PUBLIC kaynak yalnızca SON birkaç candle'ı "hatırlıyor" (GERÇEKTEN
            # yetersiz geçmiş) — WARMUP=20 gereksinimini KARŞILAMAZ.
            insufficient_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, 3) + [live_candle]
            provider2 = _RangeAwareProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): insufficient_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2, clock=FixedClock(now2))
            runtime2 = PersistedRuntime(coordinator2, store2)

            reports = await runtime2.recover()
            m5_report = next(r for r in reports if r.timeframe is Timeframe.M5)
            assert m5_report.ready is False  # sahte bir READY asla üretilmedi

            coordinator2.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", now2))
            assert _symbol_health(coordinator2, "BTCUSDT") is RuntimeHealth.BOOTSTRAPPING

            store2.close()

        run_async(scenario())


class TestPersistedRuntimeIntegration:
    """Uçtan uca (RuntimeCoordinator + PersistedRuntime + PaperStateStore)
    entegrasyon testleri — Faz 6 pipeline'ının Faz 7 checkpoint'lerle
    birlikte doğru çalıştığını doğrular."""

    def test_accepted_candle_checkpoints_and_recovery_restores_window_anchor(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "state.db"
            store1 = PaperStateStore(db_path)
            coordinator1 = _make_coordinator()
            _bootstrap_fully(coordinator1, "BTCUSDT", END)
            runtime1 = PersistedRuntime(coordinator1, store1)

            candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
            event = runtime1.ingest_candle("BTCUSDT", Timeframe.M5, candle)
            assert event.outcome is IngestOutcome.ACCEPTED
            assert store1.load_candle_checkpoint("BTCUSDT", Timeframe.M5) == candle.open_time
            store1.close()

            # brand new provider/coordinator/store — configure provider2 to
            # answer the recovery REST re-fetch with the exact same history.
            store2 = PaperStateStore(db_path)
            full_m5_history = make_candle_series_ending_at("BTCUSDT", Timeframe.M5, END, WARMUP) + [candle]
            provider2 = FakeLiveDataProvider(
                historical_candles={
                    ("BTCUSDT", Timeframe.M5): full_m5_history,
                    ("BTCUSDT", Timeframe.M15): make_candle_series_ending_at("BTCUSDT", Timeframe.M15, END, WARMUP),
                    ("BTCUSDT", Timeframe.H1): make_candle_series_ending_at("BTCUSDT", Timeframe.H1, END, WARMUP),
                }
            )
            coordinator2 = _make_coordinator(provider=provider2)
            runtime2 = PersistedRuntime(coordinator2, store2)

            await runtime2.recover()

            window = coordinator2._candle_windows[("BTCUSDT", Timeframe.M5)]  # noqa: SLF001
            assert window.latest_open_time == candle.open_time  # re-anchored, not fabricated
            store2.close()

        run_async(scenario())

    def test_persistence_write_failure_degrades_health_not_falsely_healthy(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

        store.close()  # force subsequent durable writes to fail

        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=101.0)
        event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        assert event.outcome is IngestOutcome.ACCEPTED  # in-memory Phase 6 state unaffected
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

    def test_unrelated_order_book_event_does_not_clear_persistence_fault(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)
        store.close()

        runtime.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        later = END + timedelta(seconds=1)
        event = runtime.ingest_order_book("BTCUSDT", make_order_book("BTCUSDT", later, last_update_id=2))

        assert event.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

    def test_successful_checkpoint_clears_persistence_fault(self, tmp_path) -> None:
        class _FlakyStore:
            """Bir sonraki 1 checkpoint çağrısında BAŞARISIZ OLAN, sonra
            normal çalışan bir test double'ı — gerçek SQLite bağlantı
            yaşam döngüsünü karmaşıklaştırmadan "geçici arıza -> düzelme"
            senaryosunu deterministik test eder."""

            def __init__(self, real: PaperStateStore) -> None:
                self._real = real
                self._fail_remaining = 1

            def checkpoint_candle(self, *args, **kwargs) -> None:
                if self._fail_remaining > 0:
                    self._fail_remaining -= 1
                    raise PersistenceError("simulated transient failure")
                self._real.checkpoint_candle(*args, **kwargs)

            def checkpoint_paper_state(self, *args, **kwargs) -> None:
                self._real.checkpoint_paper_state(*args, **kwargs)

            def checkpoint_candle_transition(self, *args, **kwargs) -> None:
                # HIGH-1 fix: PersistedRuntime.ingest_candle() now always
                # goes through this combined atomic call (never the two
                # separate methods above) — same fail-once-then-recover
                # fault injection, applied to the single real call site.
                if self._fail_remaining > 0:
                    self._fail_remaining -= 1
                    raise PersistenceError("simulated transient failure")
                self._real.checkpoint_candle_transition(*args, **kwargs)

            def load_candle_checkpoint(self, *args, **kwargs):
                return self._real.load_candle_checkpoint(*args, **kwargs)

            def load_paper_state(self, *args, **kwargs):
                return self._real.load_paper_state(*args, **kwargs)

            def close(self) -> None:
                self._real.close()

        db_path = tmp_path / "state.db"
        real_store = PaperStateStore(db_path)
        flaky = _FlakyStore(real_store)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, flaky)

        first = runtime.ingest_candle("BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, END, price=101.0))
        assert first.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

        later = END + timedelta(minutes=5)
        second = runtime.ingest_candle(
            "BTCUSDT", Timeframe.M5, make_candle("BTCUSDT", Timeframe.M5, later, price=102.0)
        )
        assert second.outcome is IngestOutcome.ACCEPTED
        assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY
        real_store.close()

    def test_neutral_signal_produces_no_persistence_write(self, tmp_path) -> None:
        db_path = tmp_path / "state.db"
        store = PaperStateStore(db_path)
        coordinator = _make_coordinator()
        _bootstrap_fully(coordinator, "BTCUSDT", END)
        runtime = PersistedRuntime(coordinator, store)

        # flat prices -> NEUTRAL-leaning score; regardless of the exact
        # direction produced, assert the *policy*: idempotent replay /
        # NEUTRAL paths never call checkpoint_paper_state. We verify this
        # by checking that no processed_context row exists yet for a
        # context_id that was never durably confirmed as a real mutation.
        candle = make_candle("BTCUSDT", Timeframe.M5, END, price=100.0)
        event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle)

        if event.cycle_result is not None and event.cycle_result.evaluated:
            from crypto_signal_engine.domain.enums import SignalDirection

            if event.cycle_result.signal.direction is SignalDirection.NEUTRAL:
                snapshot = store.load_paper_state("BTCUSDT")
                assert snapshot is None or event.cycle_result.signal.context_id not in snapshot.processed_context_ids
        store.close()

    def test_stream_disconnect_marks_health_degraded_via_persisted_runtime_run(self, tmp_path) -> None:
        """BLOCKER (Faz 9 soak harness geliştirmesi sırasında, GERÇEK
        `PersistedRuntime.run()` üzerinden bulundu): `RuntimeCoordinator._consume_candles`
        (Faz 6) bir stream hatasını `except Exception: mark_disconnected(symbol);
        raise` ile ANINDA gözlemlenebilir kılar — ama `PersistedRuntime._consume_candles`/
        `_consume_order_book` (Faz 7, Faz 8'in GERÇEK üretim giriş noktası
        `Application.start()` -> `runtime.run()` tarafından kullanılır) bu
        sarmalayıcıya SAHİP DEĞİLDİ. Sonuç: GERÇEK üretimde bir stream kopması,
        `mark_disconnected()` HİÇ ÇAĞRILMADAN sessizce `gather(...,
        return_exceptions=True)` tarafından yutuluyordu — sembol yalnızca
        staleness eşiği DOLANA KADAR yanlışlıkla READY/healthy görünmeye
        devam ediyordu (Faz 6'nın KENDİ "must immediately be observably
        unhealthy" invariant'ının ihlali)."""

        class _DisconnectingProvider(FakeLiveDataProvider):
            async def stream_candles(self, symbol, timeframe):
                raise ConnectionError("simulated PUBLIC stream disconnect")
                yield  # pragma: no cover - jeneratör imzasını korumak için, asla ulaşılmaz

        async def scenario() -> None:
            db_path = tmp_path / "state.db"
            store = PaperStateStore(db_path)
            provider = _DisconnectingProvider()
            coordinator = _make_coordinator(provider=provider)
            _bootstrap_fully(coordinator, "BTCUSDT", END)
            runtime = PersistedRuntime(coordinator, store)
            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.READY

            run_task = asyncio.create_task(runtime.run())
            for _ in range(10):
                await asyncio.sleep(0)

            assert _symbol_health(coordinator, "BTCUSDT") is RuntimeHealth.DEGRADED

            await runtime.stop()
            await run_task

        run_async(scenario())


=== FILE: tests/test_persistence_serialization.py ===
from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import AgentName, RiskLevel, Timeframe
from crypto_signal_engine.domain.models import AgentEvidence, Signal
from crypto_signal_engine.paper_trading.models import OrderSide, PaperFill, PaperOrder, PaperPosition, PositionSide
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

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def _make_evidence(agent=AgentName.QUANT, context_id="ctx-1") -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=0.4, rationale="test rationale", primary_timeframe=Timeframe.M5,
        symbol="BTCUSDT", as_of=T0, context_id=context_id, supporting_metrics={"RSI_14": 55.0},
    )


def _make_signal() -> Signal:
    return Signal(
        symbol="BTCUSDT", timestamp=T0, context_id="ctx-1", score=0.5, confidence=0.8,
        risk_level=RiskLevel.LOW, primary_timeframe=Timeframe.M5,
        supporting_factors=(_make_evidence(AgentName.QUANT),),
        contradicting_factors=(_make_evidence(AgentName.MARKET_STRUCTURE),),
        invalidation="test invalidation", model_version="test-v1",
    )


class TestSerializationRoundTrip:
    def test_signal_round_trip(self) -> None:
        signal = _make_signal()
        restored = dict_to_signal(signal_to_dict(signal))
        assert restored == signal

    def test_position_round_trip(self) -> None:
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.LONG, quantity=10.0,
            average_entry_price=100.0, realized_pnl=5.0, updated_at=T0,
        )
        assert dict_to_position(position_to_dict(position)) == position

    def test_flat_position_round_trip(self) -> None:
        position = PaperPosition(
            symbol="BTCUSDT", side=PositionSide.FLAT, quantity=0.0,
            average_entry_price=0.0, realized_pnl=12.5, updated_at=T0,
        )
        assert dict_to_position(position_to_dict(position)) == position

    def test_fill_round_trip(self) -> None:
        fill = PaperFill(
            fill_id="BTCUSDT:ctx-1:0:fill", order_id="BTCUSDT:ctx-1:0", symbol="BTCUSDT",
            side=OrderSide.BUY, quantity=10.0, price=100.5, fee=1.0, filled_at=T0,
        )
        assert dict_to_fill(fill_to_dict(fill)) == fill

    def test_order_round_trip(self) -> None:
        order = PaperOrder(
            order_id="BTCUSDT:ctx-1:0", symbol="BTCUSDT", side=OrderSide.SELL,
            quantity=10.0, signal_context_id="ctx-1", created_at=T0,
        )
        assert dict_to_order(order_to_dict(order)) == order

    def test_signal_with_no_evidence_round_trips(self) -> None:
        signal = Signal(
            symbol="ETHUSDT", timestamp=T0, context_id="ctx-2", score=0.0, confidence=0.0,
            risk_level=RiskLevel.MEDIUM, primary_timeframe=Timeframe.M5,
            supporting_factors=(), contradicting_factors=(), invalidation=None, model_version="test-v1",
        )
        assert dict_to_signal(signal_to_dict(signal)) == signal


=== FILE: tests/test_phase12_local_production_readiness.py ===
"""Faz 12 — Final Local Production Readiness: yüksek-değerli entegrasyon
testleri (Bölüm 12'nin 20 maddelik senaryo listesi). Tamamen offline —
gerçek ağ/websockets/Binance TESTNET bağlantısı YOK; `FakeLiveDataProvider`
(Faz 6/8) ve `FakeTestnetHttpClient` (Faz 10/11) enjekte edilir.

Bu dosya, YENİ Faz 12 davranışına ODAKLANIR (execution-mode gating,
startup reconciliation, read-only dashboard, doctor). Zaten Faz 1-11'in
KENDİ test dosyalarında kanıtlanmış invariant'lar (multi-symbol izolasyon,
stale-data health, contradiction protection, vb.) burada TEKRAR
test EDİLMEZ — yalnızca üretim kompozisyonunun (`app.py::Application`)
bu invariant'ları GERÇEKTEN doğru şekilde BAĞLADIĞI doğrulanır."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.app import EXIT_FATAL, EXIT_OK, Application, _run_doctor
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.models import ExecutionMode, OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import ExecutionLifecycleState, new_record, transition
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.ops.config import load_config
from scripts.backup_sqlite import backup_sqlite_database
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient
from tests.runtime_fakes import FakeLiveDataProvider, make_candle_series_ending_at

_SYMBOL = "BTCUSDT"
_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H1)
_NOT_FOUND_RESPONSE = (400, '{"code": -2013, "msg": "Order does not exist."}')


def _base_env(tmp_path: Path, **overrides: str) -> dict:
    env = {
        "CSE_SYMBOLS": _SYMBOL,
        "CSE_DB_PATH": str(tmp_path / "state.db"),
        "CSE_WARMUP_CANDLES": "20",
        "CSE_HEALTH_SNAPSHOT_INTERVAL_SECONDS": "0.05",
    }
    env.update(overrides)
    return env


def _bootstrappable_provider() -> FakeLiveDataProvider:
    end = datetime.now(timezone.utc) - timedelta(seconds=2)
    historical = {(_SYMBOL, tf): make_candle_series_ending_at(_SYMBOL, tf, end, 20) for tf in _TIMEFRAMES}
    return FakeLiveDataProvider(historical_candles=historical)


async def _wait_until(predicate, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(f"koşul {timeout}s içinde gerçekleşmedi")
        await asyncio.sleep(interval)


async def _run_until_ready_then_shutdown(app: Application, *, extra_wait: float = 0.2) -> int:
    run_task = asyncio.create_task(app.start())
    await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
    await asyncio.sleep(extra_wait)
    app.request_shutdown("test")
    return await asyncio.wait_for(run_task, timeout=5.0)


def _pending_unknown_not_found_record(execution_db_path: Path, *, context_id: str = "ctx-pending") -> None:
    store = ExecutionStateStore(execution_db_path)
    intent = OrderIntent(
        symbol=_SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id=context_id, timestamp=datetime.now(timezone.utc), quantity=0.001,
    )
    record = new_record(intent, now=datetime.now(timezone.utc))
    record = transition(
        record, new_state=ExecutionLifecycleState.UNKNOWN_NOT_FOUND, now=datetime.now(timezone.utc),
        detail="pending from a prior session",
    )
    store.save(record)
    store.close()


class TestPaperFirstSafety:
    """#1 fresh PAPER startup, #3 default config never executes TESTNET,
    #20 PAPER requires zero credentials."""

    def test_fresh_paper_startup_reaches_ready(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        assert config.execution_mode is ExecutionMode.PAPER
        app = Application(config, _bootstrappable_provider())
        exit_code = run_async(_run_until_ready_then_shutdown(app))
        assert exit_code == EXIT_OK
        assert app._execution_service is None
        assert app._execution_store is None

    def test_default_config_has_no_execution_service_at_all(self, tmp_path: Path, monkeypatch) -> None:
        """#3: bir order göndermek için GEREKLİ olan `ExecutionReconciliationService`
        nesnesinin KENDİSİ, varsayılan config ile HİÇ İNŞA EDİLMEZ — bu
        yüzden "yanlışlıkla bir TESTNET order gönderme" için erişilebilir
        HİÇBİR kod yolu YOKTUR (in-memory olarak yapısal olarak imkansız)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        try:
            assert app._execution_service is None
            assert app._execution_store is None
        finally:
            app._store.close()
            app._lock.release()

    def test_paper_mode_requires_zero_credentials(self, tmp_path: Path, monkeypatch) -> None:
        """#20: PAPER modda, ortamda HİÇBİR TESTNET kimlik bilgisi
        YOKKEN bile kompozisyon/startup tamamen BAŞARILI olur."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        exit_code = run_async(_run_until_ready_then_shutdown(app))
        assert exit_code == EXIT_OK


class TestTestnetGating:
    """#4 TESTNET without explicit enable -> no execution possible,
    #5 TESTNET enabled without credentials -> doctor FAILs (rejected)."""

    def test_testnet_selected_without_enable_flag_has_no_execution_service(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET"))
        assert config.enable_testnet_execution is False
        app = Application(config, _bootstrappable_provider())
        try:
            # STRICTLY opt-in kapı: mode seçilmiş olsa bile enable=False
            # iken execution service HİÇ İNŞA EDİLMEZ.
            assert app._execution_service is None
            assert app._execution_store is None
        finally:
            app._store.close()
            app._lock.release()

    def test_testnet_enabled_without_credentials_fails_doctor(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        exit_code = _run_doctor(config)
        out = capsys.readouterr().out
        assert exit_code == EXIT_FATAL
        assert "[FAIL] TESTNET credentials configured" in out
        assert "UNSET" in out

    def test_mainnet_is_not_a_selectable_execution_mode(self, tmp_path: Path) -> None:
        """#19: Mainnet private execution YAPISAL OLARAK imkansızdır —
        `CSE_EXECUTION_MODE=MAINNET` HER ZAMAN bir config hatasıdır."""
        from crypto_signal_engine.ops.errors import AppConfigurationError

        with pytest.raises(AppConfigurationError):
            load_config(_base_env(tmp_path, CSE_EXECUTION_MODE="MAINNET"))


class TestCredentialPreflightBlockerFix:
    """BLOCKER FİX (bağımsız acceptance review bulgusu): TESTNET + enable=true,
    execution DB BOŞKEN (hiçbir bekleyen kayıt yokken) kimlik bilgisi eksik
    OLSA BİLE `reconcile_pending()` hiçbir signed çağrı yapmadığından
    `execution_ready=True` olabiliyordu. Fix:
    `execution/factory.py::testnet_credentials_status()`, HERHANGİ bir
    ağ çağrısından ÖNCE kimlik bilgisi VARLIĞINI kontrol eder — bekleyen
    bir signed API çağrısına VEYA operatörün `doctor` çalıştırmış
    olmasına GÜVENMEZ."""

    def _testnet_config(self, tmp_path: Path) -> object:
        exec_db = tmp_path / "exec.db"
        return exec_db, load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )

    def test_empty_db_no_credentials_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#1: enabled TESTNET + boş execution DB + kimlik bilgisi YOK ->
        `execution_ready` HER ZAMAN `False` kalmalıdır (regresyon: eskiden
        boş DB üzerinde `reconcile_pending()` sıfır signed çağrı yapıp
        "başarıyla" tamamlanır ve YANLIŞLIKLA `True` olurdu)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()  # BOŞ script — herhangi bir çağrı denenirse HATA verir
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        exit_code = run_async(_run_until_ready_then_shutdown(app))

        assert exit_code == EXIT_OK
        assert app.execution_ready is False
        assert "credentials missing" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_key_only_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#2: yalnızca `BINANCE_TESTNET_API_KEY` SET -> hâlâ `False`."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "only-the-key")
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert "BINANCE_TESTNET_API_KEY=SET" in app._execution_detail
        assert "BINANCE_TESTNET_API_SECRET=UNSET" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_secret_only_never_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#3: yalnızca `BINANCE_TESTNET_API_SECRET` SET -> hâlâ `False`."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "only-the-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert "BINANCE_TESTNET_API_KEY=UNSET" in app._execution_detail
        assert "BINANCE_TESTNET_API_SECRET=SET" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_empty_db_both_credentials_present_becomes_ready(self, tmp_path: Path, monkeypatch) -> None:
        """#4: kimlik bilgisi TAM iken boş DB üzerinde reconciliation
        BAŞARIYLA tamamlanır ve `execution_ready=True` olur (sıfır kayıt
        üzerinde sıfır ağ çağrısı ile — bu YİNE DE geçerli bir "hazır"
        durumudur, çünkü kimlik bilgisi ÖNCEDEN doğrulanmıştır)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "real-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "real-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is True
        assert "reconciliation OK: 0 record(s)" in app._execution_detail
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_missing_credentials_with_pending_record_still_zero_network_calls(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """#5: kimlik bilgisi eksikken, HATTA bekleyen bir kayıt VARKEN
        bile, credential-preflight `reconcile_pending()`'e HİÇ ULAŞMAZ —
        sıfır POST, sıfır signed GET (eski davranışta bu senaryo zaten
        bir `MissingCredentialsError`'a yol açardı, ama en azından BİR ağ
        denemesi/imzalama girişimi OLURDU; şimdi hiç DENENMEZ bile)."""
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-no-creds")
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is False
        assert len(fake_http.get_calls) == 0
        assert len(fake_http.post_calls) == 0

    def test_unknown_not_found_reconciliation_with_credentials_is_unchanged(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """#7: kimlik bilgisi TAM olduğunda, bekleyen `UNKNOWN_NOT_FOUND`
        kaydının startup reconciliation davranışı (Faz 11 Karar 78 ile
        AYNI) DEĞİŞMEDİ — tek bir yeniden sorgu, sıfır POST."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "real-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "real-secret")
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-unchanged")
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert app.execution_ready is True
        assert len(fake_http.get_calls) == 1
        assert len(fake_http.post_calls) == 0

    def test_credential_status_never_leaks_secret_values(self, tmp_path: Path, monkeypatch) -> None:
        """#8: gerçek credential DEĞERLERİ hiçbir zaman `_execution_detail`
        (health-snapshot/dashboard'a yansıyan alan) İÇİNE SIZMAZ — SADECE
        SET/UNSET etiketleri görünür."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "leak-check-key-value")
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exec_db, config = self._testnet_config(tmp_path)
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        run_async(_run_until_ready_then_shutdown(app))

        assert "leak-check-key-value" not in app._execution_detail
        assert "SET" in app._execution_detail and "UNSET" in app._execution_detail


class TestStartupReconciliation:
    """#6 pending reconciliation runs before new execution is possible,
    #7 unresolved UNKNOWN_NOT_FOUND at startup -> zero POST,
    #8 duplicate context via the production-wired service -> zero duplicate POST."""

    def test_pending_unknown_not_found_is_reconciled_at_startup_with_zero_posts(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db)

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        exit_code = run_async(_run_until_ready_then_shutdown(app))

        assert exit_code == EXIT_OK
        assert len(fake_http.post_calls) == 0
        assert len(fake_http.get_calls) == 1
        assert app.execution_ready is True  # reconciliation ÇALIŞTI (BAŞARISIZ olmadı), kayıt hâlâ çözülmemiş olabilir

    def test_startup_reconciliation_runs_before_run_loop_starts(self, tmp_path: Path, monkeypatch) -> None:
        """#6: reconciliation, run-loop/health-loop BAŞLAMADAN ÖNCE
        tamamlanmış OLMALIDIR — bu, `Application.start()`'ın kaynak
        kodundaki SIRA ile yapısal olarak garanti edilir; burada dolaylı
        olarak `execution_ready`'nin run_task oluşturulmadan ÖNCE zaten
        belirlenmiş OLDUĞUNU (reconcile_pending tamamlanmadan run_task
        asla create edilmez) doğrularız."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db)

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE])
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            # run_task zaten oluşturulmuş olduğuna göre, reconciliation
            # ZATEN tamamlanmış olmalı (bkz. start()'ın kaynak sırası).
            assert app._execution_ready is True
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK
        assert len(fake_http.post_calls) == 0

    def test_restart_with_unresolved_record_still_never_posts(self, tmp_path: Path, monkeypatch) -> None:
        """#7 + #8 (restart açısı): execution store'u kapatıp AYNI
        db path ile YENİ bir `Application` (restart simülasyonu)
        kurulduğunda, kayıt hâlâ `UNKNOWN_NOT_FOUND` iken TEK bir GET
        dışında hiçbir ağ çağrısı OLMAZ — özellikle POST asla ÇAĞRILMAZ."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        _pending_unknown_not_found_record(exec_db, context_id="ctx-restart")

        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        fake_http = FakeTestnetHttpClient(get_responses=[_NOT_FOUND_RESPONSE, _NOT_FOUND_RESPONSE])

        first = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)
        run_async(_run_until_ready_then_shutdown(first))

        second = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)
        run_async(_run_until_ready_then_shutdown(second))

        assert len(fake_http.post_calls) == 0
        assert len(fake_http.get_calls) == 2


class TestDashboardSafety:
    """#9 read does not mutate, #10 cannot submit orders, #11 no secrets,
    #12 dashboard failure never stops runtime, #13 loopback default."""

    def test_dashboard_host_defaults_to_loopback(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        assert config.dashboard_host == "127.0.0.1"
        assert config.dashboard_enabled is True

    def test_dashboard_read_does_not_mutate_paper_state(self, tmp_path: Path, monkeypatch) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18901"))
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.2)
            before = app.runtime._coordinator.paper_engine.position(_SYMBOL)
            for _ in range(5):
                with urllib.request.urlopen("http://127.0.0.1:18901/api/status", timeout=2) as resp:
                    assert resp.status == 200
                with urllib.request.urlopen("http://127.0.0.1:18901/", timeout=2) as resp:
                    assert resp.status == 200
            after = app.runtime._coordinator.paper_engine.position(_SYMBOL)
            assert before == after  # dashboard okuması pozisyonu DEĞİŞTİRMEDİ
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_rejects_all_mutation_methods(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18902"))
        app = Application(config, _bootstrappable_provider())

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.1)
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                req = urllib.request.Request("http://127.0.0.1:18902/", method=method)
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req, timeout=2)
                assert exc_info.value.code == 405
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_response_never_contains_credential_values(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "super-secret-key-value")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "super-secret-secret-value")
        exec_db = tmp_path / "exec.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(
            _base_env(
                tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true",
                CSE_DASHBOARD_PORT="18903",
            )
        )
        fake_http = FakeTestnetHttpClient()
        app = Application(config, _bootstrappable_provider(), testnet_http_client=fake_http)

        async def body() -> int:
            run_task = asyncio.create_task(app.start())
            await _wait_until(lambda: app._run_task is not None or app._shutdown_started)
            await asyncio.sleep(0.1)
            with urllib.request.urlopen("http://127.0.0.1:18903/api/status", timeout=2) as resp:
                raw = resp.read().decode("utf-8")
            with urllib.request.urlopen("http://127.0.0.1:18903/", timeout=2) as resp:
                raw += resp.read().decode("utf-8")
            assert "super-secret-key-value" not in raw
            assert "super-secret-secret-value" not in raw
            app.request_shutdown("test")
            return await asyncio.wait_for(run_task, timeout=5.0)

        assert run_async(body()) == EXIT_OK

    def test_dashboard_bind_failure_does_not_stop_runtime(self, tmp_path: Path) -> None:
        """#12: dashboard portu ZATEN kullanımdaysa (bind hatası),
        `Application.start()` HÂLÂ BAŞARIYLA tamamlanır — ana runtime
        (market-data/paper-trading/persistence) bu yüzden ASLA durmaz."""
        import socket

        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        busy_port = blocker.getsockname()[1]
        try:
            config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT=str(busy_port)))
            app = Application(config, _bootstrappable_provider())
            exit_code = run_async(_run_until_ready_then_shutdown(app))
            assert exit_code == EXIT_OK
            assert app._dashboard is None  # başlatma başarısız oldu, devre dışı bırakıldı
        finally:
            blocker.close()


class TestGracefulShutdownAndRestart:
    """#2 PAPER restart from persisted state, #14 graceful shutdown."""

    def test_paper_restart_from_persisted_state_recovers_cleanly(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        config = load_config(env)
        first = Application(config, _bootstrappable_provider())
        first_exit = run_async(_run_until_ready_then_shutdown(first))
        assert first_exit == EXIT_OK

        second = Application(load_config(env), _bootstrappable_provider())
        second_exit = run_async(_run_until_ready_then_shutdown(second))
        assert second_exit == EXIT_OK

    def test_graceful_shutdown_releases_lock_and_stops_dashboard(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path, CSE_DASHBOARD_PORT="18904"))
        app = Application(config, _bootstrappable_provider())
        run_async(_run_until_ready_then_shutdown(app))

        with pytest.raises(sqlite3.ProgrammingError):
            app._store._connection.execute("SELECT 1")
        with pytest.raises((urllib.error.URLError, ConnectionRefusedError)):
            urllib.request.urlopen("http://127.0.0.1:18904/api/status", timeout=1)


class TestBackupStillValid:
    """#18: mevcut Faz 8 `backup_sqlite.py` yardımcı programı, üretim
    `Application`'ın GERÇEKTEN ürettiği bir paper_state.db üzerinde HÂLÂ
    çalışır ve geçerli, yeniden açılabilir bir kopya üretir."""

    def test_backup_of_a_real_running_apps_database_is_valid(self, tmp_path: Path) -> None:
        config = load_config(_base_env(tmp_path))
        app = Application(config, _bootstrappable_provider())
        run_async(_run_until_ready_then_shutdown(app))

        backup_path = tmp_path / "backup.db"
        backup_sqlite_database(config.db_path, backup_path)

        conn = sqlite3.connect(str(backup_path))
        try:
            (result,) = conn.execute("PRAGMA integrity_check").fetchone()
            assert result == "ok"
        finally:
            conn.close()


class TestDoctorOfflineNoSideEffects:
    def test_doctor_passes_for_a_clean_paper_configuration(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        config = load_config(_base_env(tmp_path))
        exit_code = _run_doctor(config)
        out = capsys.readouterr().out
        assert exit_code == EXIT_OK
        assert "ALL CHECKS PASSED" in out
        assert "127.0.0.1" not in out or "[PASS] dashboard bind is loopback-only" in out

    def test_doctor_never_prints_credential_values(self, tmp_path: Path, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "doctor-secret-key")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "doctor-secret-value")
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        _run_doctor(config)
        out = capsys.readouterr().out
        assert "doctor-secret-key" not in out
        assert "doctor-secret-value" not in out
        assert "SET" in out

    def test_doctor_does_not_create_execution_db_or_touch_network(self, tmp_path: Path, monkeypatch) -> None:
        """No side effects: doctor, execution store dosyasını asla
        oluşturmaz (offline, TAMAMEN yerel kontroller)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "fake")
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "fake")
        exec_db = tmp_path / "should-not-be-created.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(exec_db))
        config = load_config(
            _base_env(tmp_path, CSE_EXECUTION_MODE="BINANCE_SPOT_TESTNET", CSE_ENABLE_TESTNET_EXECUTION="true")
        )
        _run_doctor(config)
        assert not exec_db.exists()


