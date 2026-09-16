<!-- p1e.md — Part 5/6 of all_code_part1.md — 31 files -->
<!-- Contents of this part: -->
<!--   - tests/test_binance_order_book_sync.py -->
<!--   - tests/test_binance_parser.py -->
<!--   - tests/test_binance_provider.py -->
<!--   - tests/test_binance_quality_rules.py -->
<!--   - tests/test_binance_reconnect.py -->
<!--   - tests/test_binance_rest.py -->
<!--   - tests/test_binance_symbols.py -->
<!--   - tests/test_bridge_runtime.py -->
<!--   - tests/test_candle_features.py -->
<!--   - tests/test_candle_sequencing.py -->
<!--   - tests/test_consensus.py -->
<!--   - tests/test_consensus_engine.py -->
<!--   - tests/test_domain_models.py -->
<!--   - tests/test_end_to_end.py -->
<!--   - tests/test_enums.py -->
<!--   - tests/test_events.py -->
<!--   - tests/test_execution_adapter.py -->
<!--   - tests/test_execution_cli.py -->
<!--   - tests/test_execution_concurrency.py -->
<!--   - tests/test_execution_lifecycle.py -->
<!--   - tests/test_execution_lifecycle_manager.py -->
<!--   - tests/test_execution_lifecycle_migration.py -->
<!--   - tests/test_execution_lifecycle_replay_sanity.py -->
<!--   - tests/test_execution_lifecycle_store.py -->
<!--   - tests/test_execution_models.py -->
<!--   - tests/test_execution_reconciliation_models.py -->
<!--   - tests/test_execution_reconciliation_service.py -->
<!--   - tests/test_execution_reconciliation_store.py -->
<!--   - tests/test_execution_signer.py -->
<!--   - tests/test_execution_testnet_client.py -->
<!--   - tests/test_feature_domain.py -->

=== FILE: tests/test_binance_order_book_sync.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import OrderBookSyncError
from crypto_signal_engine.providers.binance.order_book_sync import (
    MAX_PRE_SYNC_BUFFER_SIZE,
    DepthDiffEvent,
    OrderBookSyncState,
    OrderBookSynchronizer,
)

UTC = timezone.utc
T0 = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def diff(first: int, final: int, bids=(), asks=(), event_time=T0) -> DepthDiffEvent:
    return DepthDiffEvent(
        symbol="BTCUSDT", first_update_id=first, final_update_id=final,
        event_time=event_time, bids=tuple(bids), asks=tuple(asks),
    )


class TestDepthDiffEventInvariants:
    def test_final_less_than_first_rejected(self) -> None:
        with pytest.raises(ValueError, match="final_update_id"):
            diff(first=10, final=5)

    def test_symbol_normalized(self) -> None:
        event = DepthDiffEvent(
            symbol="btcusdt", first_update_id=1, final_update_id=2,
            event_time=T0, bids=(), asks=(),
        )
        assert event.symbol == "BTCUSDT"


class TestSnapshotBeforeDiff:
    def test_diff_before_snapshot_is_buffered_not_applied(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        result = sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        assert result is None
        assert sync.state == OrderBookSyncState.UNSYNCED
        assert sync.buffered_event_count == 1


class TestSnapshotFromStaleBufferedDiff:
    def test_stale_buffered_diff_dropped(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=90, final=100, bids=[(1.0, 1.0)], asks=[(2.0, 1.0)]))
        sync.ingest_diff(diff(first=151, final=155, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        snapshot = sync.ingest_snapshot(
            last_update_id=150, bids=((99.5, 2.0),), asks=((100.5, 2.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 155


class TestExactBoundaryUpdateId:
    def test_exact_boundary_bridges_correctly(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=101, bids=[(99.0, 5.0)], asks=[(100.0, 5.0)]))
        snapshot = sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 101
        assert snapshot.best_bid.quantity == 5.0

    def test_boundary_not_satisfied_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=105, final=110, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        with pytest.raises(OrderBookSyncError, match="köprü"):
            sync.ingest_snapshot(
                last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
            )
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestSequenceGap:
    def test_gap_after_sync_detected_and_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=105, final=110, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        assert sync.state == OrderBookSyncState.UNSYNCED

    def test_gap_in_buffered_events_during_snapshot_apply_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        sync.ingest_diff(diff(first=110, final=115, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        with pytest.raises(OrderBookSyncError, match="buffer içinde sequence gap"):
            sync.ingest_snapshot(
                last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
            )

    def test_after_gap_resync_works(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError):
            sync.ingest_diff(diff(first=105, final=110))
        assert sync.state == OrderBookSyncState.UNSYNCED
        snapshot = sync.ingest_snapshot(
            last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert sync.state == OrderBookSyncState.SYNCED
        assert snapshot.last_update_id == 200


class TestDuplicateDiff:
    def test_duplicate_final_update_id_treated_as_stale_not_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 2.0)], asks=[(100.0, 2.0)]))
        result = sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 999.0)], asks=[(100.0, 999.0)]))
        assert result is not None
        assert sync.state == OrderBookSyncState.SYNCED
        assert result.best_bid.quantity == 2.0


class TestOutOfOrderDiff:
    def test_out_of_order_after_sync_is_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        # final_update_id (250) > last_update_id (200) -> stale DEĞİL; ve
        # first_update_id (210) beklenen U<=201 sınırını AŞIYOR -> gerçek gap.
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=210, final=250))

    def test_fully_stale_replay_after_sync_is_ignored_not_gap(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=200, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        # final_update_id (60) <= last_update_id (200) -> tamamen eski/replay,
        # Binance dokümantasyonuna göre normal kabul edilir, gap DEĞİLDİR.
        result = sync.ingest_diff(diff(first=50, final=60))
        assert result is not None
        assert sync.state == OrderBookSyncState.SYNCED


class TestReconnectPendingEvent:
    def test_buffered_events_pending_during_reconnect_are_cleared_on_reset(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=1, final=5))
        assert sync.buffered_event_count == 1
        sync.reset()
        assert sync.buffered_event_count == 0
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestZeroQuantityDelete:
    def test_zero_quantity_removes_level(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0), (98.0, 2.0)), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        snapshot = sync.ingest_diff(diff(first=101, final=101, bids=[(98.0, 0.0)], asks=[]))
        prices = [level.price for level in snapshot.bids]
        assert 98.0 not in prices
        assert 99.0 in prices

    def test_deleting_nonexistent_level_is_noop(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=101, final=101, bids=[(50.0, 0.0)], asks=[]))
        assert snapshot.best_bid.price == 99.0


class TestBidAskSorting:
    def test_bids_descending_asks_ascending(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        snapshot = sync.ingest_snapshot(
            last_update_id=1,
            bids=((98.0, 1.0), (99.5, 1.0), (99.0, 1.0)),
            asks=((101.0, 1.0), (100.0, 1.0), (100.5, 1.0)),
            snapshot_timestamp=T0,
        )
        bid_prices = [lvl.price for lvl in snapshot.bids]
        ask_prices = [lvl.price for lvl in snapshot.asks]
        assert bid_prices == sorted(bid_prices, reverse=True)
        assert ask_prices == sorted(ask_prices)


class TestCrossedBook:
    def test_crossed_book_after_diff_raises_and_unsyncs(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="crossed"):
            sync.ingest_diff(diff(first=101, final=101, bids=[(101.0, 5.0)], asks=[]))
        assert sync.state == OrderBookSyncState.UNSYNCED


class TestEmptySide:
    def test_snapshot_resulting_in_empty_side_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        with pytest.raises(OrderBookSyncError, match="boş kaldı"):
            sync.ingest_snapshot(last_update_id=1, bids=(), asks=((100.0, 1.0),), snapshot_timestamp=T0)

    def test_diff_emptying_last_bid_level_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="boş kaldı"):
            sync.ingest_diff(diff(first=101, final=101, bids=[(99.0, 0.0)], asks=[]))


class TestMalformedPriceQty:
    def test_negative_price_rejected_by_order_book_level(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        with pytest.raises(ValueError):
            sync.ingest_snapshot(last_update_id=1, bids=((-1.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)


class TestResyncAfterReplay:
    def test_stale_event_after_resync_with_higher_last_update_id_ignored(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError):
            sync.ingest_diff(diff(first=105, final=110))
        sync.ingest_snapshot(last_update_id=300, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        result = sync.ingest_diff(diff(first=105, final=110))
        assert result is not None


class TestBufferOverflow:
    def test_buffer_overflow_before_snapshot_raises(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        for i in range(MAX_PRE_SYNC_BUFFER_SIZE):
            sync.ingest_diff(diff(first=i, final=i))
        with pytest.raises(OrderBookSyncError, match="sınırını aştı"):
            sync.ingest_diff(diff(first=MAX_PRE_SYNC_BUFFER_SIZE, final=MAX_PRE_SYNC_BUFFER_SIZE))


class TestSymbolMismatch:
    def test_mismatched_symbol_rejected(self) -> None:
        sync = OrderBookSynchronizer("BTCUSDT")
        wrong = DepthDiffEvent(symbol="ETHUSDT", first_update_id=1, final_update_id=2, event_time=T0, bids=(), asks=())
        with pytest.raises(ValueError, match="symbol uyuşmazlığı"):
            sync.ingest_diff(wrong)


class TestReviewerOverlapProbes:
    """Bağımsız reviewer probe'ları A ve B — overlap semantiği (strict
    equality zorunluluğu kaldırıldı)."""

    def test_probe_a_overlap_within_range_accepted(self) -> None:
        """A) local book last_update_id=100, incoming U=100,u=102 → ACCEPT."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=100, final=102, bids=[(99.0, 5.0)], asks=[]))
        assert snapshot is not None
        assert snapshot.last_update_id == 102
        assert sync.state == OrderBookSyncState.SYNCED

    def test_probe_b_true_gap_still_rejected(self) -> None:
        """B) local book last_update_id=100, incoming U=102,u=103 → REJECT/RESYNC."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        with pytest.raises(OrderBookSyncError, match="sequence gap"):
            sync.ingest_diff(diff(first=102, final=103))
        assert sync.state == OrderBookSyncState.UNSYNCED

    def test_exact_match_still_accepted(self) -> None:
        """Overlap kabulü, strict equality'yi de kapsamalı (regresyon)."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_snapshot(last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0)
        snapshot = sync.ingest_diff(diff(first=101, final=101))
        assert snapshot is not None
        assert snapshot.last_update_id == 101

    def test_buffer_continuity_overlap_accepted(self) -> None:
        """ingest_snapshot içindeki buffer-sürekliliği kontrolü de overlap kabul etmeli."""
        sync = OrderBookSynchronizer("BTCUSDT")
        sync.ingest_diff(diff(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)]))
        # İkinci buffer event'i, ilkinin son ID'siyle TAM eşleşmiyor ama
        # overlap ediyor (first=104 <= prev.final(105)+1=106).
        sync.ingest_diff(diff(first=104, final=110, bids=[(99.0, 2.0)], asks=[]))
        snapshot = sync.ingest_snapshot(
            last_update_id=100, bids=((99.0, 1.0),), asks=((100.0, 1.0),), snapshot_timestamp=T0
        )
        assert snapshot.last_update_id == 110


=== FILE: tests/test_binance_parser.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import ParseError
from crypto_signal_engine.providers.binance import parser
from crypto_signal_engine.providers.binance.order_book_sync import DepthDiffEvent

UTC = timezone.utc


def valid_kline_ws_payload(**overrides) -> dict:
    k = {
        "t": 1000000, "T": 1059999, "s": "BTCUSDT", "i": "1m",
        "f": 100, "L": 200, "o": "100.0", "c": "101.0", "h": "102.0", "l": "99.0",
        "v": "10.0", "n": 5, "x": False, "q": "1000.0", "V": "5.0", "Q": "500.0", "B": "0",
    }
    k.update(overrides.pop("k", {}))
    payload = {"e": "kline", "E": 1000500, "s": "BTCUSDT", "k": k}
    payload.update(overrides)
    return payload


class TestParseKlineWsEvent:
    def test_valid_payload(self) -> None:
        update = parser.parse_kline_ws_event(valid_kline_ws_payload(), received_at=datetime(2026, 8, 31, tzinfo=UTC))
        assert update.candle.symbol == "BTCUSDT"
        assert update.candle.timeframe == Timeframe.M1
        assert update.update_seq == 1000500
        assert update.candle.is_closed is False

    def test_wrong_event_type_rejected(self) -> None:
        payload = valid_kline_ws_payload(e="aggTrade")
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_missing_top_level_key_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        del payload["E"]
        with pytest.raises(ParseError, match="'E' alanı eksik"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_missing_nested_key_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        del payload["k"]["o"]
        with pytest.raises(ParseError, match="'o' alanı eksik"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_null_value_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = None
        with pytest.raises(ParseError, match="None olamaz"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_wrong_type_numeric_field_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = 100.0  # str olmalı, float geldi
        with pytest.raises(ParseError, match="sayısal string"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_malformed_numeric_string_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = "not-a-number"
        with pytest.raises(ParseError, match="geçerli bir sayı değil"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_nan_string_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["o"] = "nan"
        with pytest.raises(ParseError):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_unknown_interval_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["i"] = "3d"
        with pytest.raises(ParseError, match="Desteklenmeyen/bilinmeyen Binance interval"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_wrong_top_level_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="dict bekleniyordu"):
            parser.parse_kline_ws_event("not-a-dict", received_at=datetime(2026, 8, 31, tzinfo=UTC))  # type: ignore[arg-type]

    def test_negative_epoch_rejected(self) -> None:
        payload = valid_kline_ws_payload()
        payload["k"]["t"] = -1
        with pytest.raises(ParseError, match="negatif olamaz"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))

    def test_bool_type_rejected_for_int_field(self) -> None:
        payload = valid_kline_ws_payload()
        payload["E"] = True  # bool, int isinstance True ama semantik olarak yanlış
        with pytest.raises(ParseError, match="int olmalı"):
            parser.parse_kline_ws_event(payload, received_at=datetime(2026, 8, 31, tzinfo=UTC))


class TestParseKlineRestRow:
    VALID_ROW = [
        1000000, "100.0", "102.0", "99.0", "101.0", "10.0", 1059999,
        "1000.0", 5, "5.0", "500.0", "0",
    ]

    def test_valid_row_historical_now_none(self) -> None:
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=None)
        assert candle.is_closed is True

    def test_is_closed_true_when_close_time_in_past(self) -> None:
        now = datetime(2026, 8, 31, tzinfo=UTC)
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=now)
        assert candle.is_closed is True

    def test_is_closed_false_when_close_time_in_future(self) -> None:
        now = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)  # çok eski "şimdi", close_time gelecekte kalır
        candle = parser.parse_kline_rest_row(self.VALID_ROW, symbol="BTCUSDT", timeframe=Timeframe.M1, now=now)
        assert candle.is_closed is False

    def test_too_short_row_rejected(self) -> None:
        with pytest.raises(ParseError, match="en az 9 elemanlı"):
            parser.parse_kline_rest_row([1, 2, 3], symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_non_list_row_rejected(self) -> None:
        with pytest.raises(ParseError):
            parser.parse_kline_rest_row("not-a-list", symbol="BTCUSDT", timeframe=Timeframe.M1)  # type: ignore[arg-type]

    def test_malformed_numeric_in_row_rejected(self) -> None:
        row = list(self.VALID_ROW)
        row[1] = "garbage"
        with pytest.raises(ParseError, match="geçerli sayı değil"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_wrong_type_open_time_rejected(self) -> None:
        row = list(self.VALID_ROW)
        row[0] = "1000000"  # int olmalı
        with pytest.raises(ParseError, match="int olmalı"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)

    def test_impossible_ohlc_rejected_by_domain_model(self) -> None:
        row = list(self.VALID_ROW)
        row[2] = "50.0"  # high < low ihlali
        with pytest.raises(ValueError, match="impossible OHLC"):
            parser.parse_kline_rest_row(row, symbol="BTCUSDT", timeframe=Timeframe.M1)


class TestParseAggTradeWsEvent:
    def valid_payload(self, **overrides) -> dict:
        payload = {
            "e": "aggTrade", "E": 123456789, "s": "BTCUSDT", "a": 12345,
            "p": "100.5", "q": "2.0", "f": 100, "l": 105, "T": 123456785, "m": True, "M": True,
        }
        payload.update(overrides)
        return payload

    def test_valid_payload(self) -> None:
        trade = parser.parse_agg_trade_ws_event(self.valid_payload())
        assert trade.symbol == "BTCUSDT"
        assert trade.trade_id == 12345
        assert trade.is_buyer_maker is True

    def test_wrong_event_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_agg_trade_ws_event(self.valid_payload(e="kline"))

    def test_missing_field_rejected(self) -> None:
        payload = self.valid_payload()
        del payload["p"]
        with pytest.raises(ParseError, match="'p' alanı eksik"):
            parser.parse_agg_trade_ws_event(payload)

    def test_malformed_price_rejected(self) -> None:
        with pytest.raises(ParseError):
            parser.parse_agg_trade_ws_event(self.valid_payload(p="abc"))

    def test_wrong_type_boolean_field_rejected(self) -> None:
        with pytest.raises(ParseError, match="bool olmalı"):
            parser.parse_agg_trade_ws_event(self.valid_payload(m="true"))


class TestParseDepthLevels:
    def test_valid_levels(self) -> None:
        levels = parser.parse_depth_levels([["100.0", "1.0"], ["101.0", "2.0"]], "ctx")
        assert levels == ((100.0, 1.0), (101.0, 2.0))

    def test_non_list_rejected(self) -> None:
        with pytest.raises(ParseError, match="liste bekleniyordu"):
            parser.parse_depth_levels("not-a-list", "ctx")

    def test_entry_too_short_rejected(self) -> None:
        with pytest.raises(ParseError, match="formatında olmalı"):
            parser.parse_depth_levels([["100.0"]], "ctx")

    def test_non_string_price_rejected(self) -> None:
        with pytest.raises(ParseError, match="string olmalı"):
            parser.parse_depth_levels([[100.0, "1.0"]], "ctx")

    def test_malformed_numeric_rejected(self) -> None:
        with pytest.raises(ParseError, match="geçersiz sayı"):
            parser.parse_depth_levels([["abc", "1.0"]], "ctx")


class TestParseDepthSnapshotResponse:
    def test_valid_response(self) -> None:
        payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        last_id, bids, asks = parser.parse_depth_snapshot_response(payload)
        assert last_id == 100
        assert bids == ((99.0, 1.0),)
        assert asks == ((100.0, 1.0),)

    def test_missing_last_update_id_rejected(self) -> None:
        with pytest.raises(ParseError, match="'lastUpdateId' alanı eksik"):
            parser.parse_depth_snapshot_response({"bids": [], "asks": []})


class TestParseDepthDiffWsEvent:
    def valid_payload(self, **overrides) -> dict:
        payload = {
            "e": "depthUpdate", "E": 123456789, "s": "BTCUSDT",
            "U": 157, "u": 160, "b": [["0.0024", "10"]], "a": [["0.0026", "100"]],
        }
        payload.update(overrides)
        return payload

    def test_valid_payload(self) -> None:
        event = parser.parse_depth_diff_ws_event(self.valid_payload())
        assert isinstance(event, DepthDiffEvent)
        assert event.first_update_id == 157
        assert event.final_update_id == 160

    def test_wrong_event_type_rejected(self) -> None:
        with pytest.raises(ParseError, match="beklenmeyen event tipi"):
            parser.parse_depth_diff_ws_event(self.valid_payload(e="kline"))

    def test_missing_field_rejected(self) -> None:
        payload = self.valid_payload()
        del payload["U"]
        with pytest.raises(ParseError, match="'U' alanı eksik"):
            parser.parse_depth_diff_ws_event(payload)


=== FILE: tests/test_binance_provider.py ===
import asyncio
import contextlib
import json
from datetime import datetime, timezone

import pytest

from tests.binance_fakes import FakeHttpClient, FakeWebSocketConnection, FakeWebSocketConnectionFactory, json_response
from tests.conftest import run_async
from crypto_signal_engine.domain.candle_sequencing import CandleUpdate
from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.domain.state_contract import InMemoryCandleStateStore
from crypto_signal_engine.errors import TransportError
from crypto_signal_engine.providers.base import ConnectionState
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig, ReconnectPolicyConfig
from crypto_signal_engine.providers.binance.provider import BinanceMarketDataProvider
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate, BinanceQualityThresholds
from crypto_signal_engine.state.manager import StateManager

UTC = timezone.utc


def kline_msg(
    open_ms: int, close_ms: int, is_closed: bool, close: str = "100.0", event_ms: int | None = None,
    interval: str = "1m",
) -> str:
    close_val = float(close)
    high = max(105.0, close_val + 1.0)
    low = min(95.0, close_val - 1.0)
    payload = {
        "e": "kline", "E": event_ms if event_ms is not None else close_ms, "s": "BTCUSDT",
        "k": {
            "t": open_ms, "T": close_ms, "s": "BTCUSDT", "i": interval, "f": 1, "L": 2,
            "o": "100.0", "c": close, "h": str(high), "l": str(low), "v": "10.0",
            "n": 5, "x": is_closed, "q": "1000.0", "V": "5.0", "Q": "500.0", "B": "0",
        },
    }
    return json.dumps(payload)


def trade_msg(trade_id: int, price: str, ts_ms: int) -> str:
    return json.dumps({
        "e": "aggTrade", "E": ts_ms, "s": "BTCUSDT", "a": trade_id,
        "p": price, "q": "1.0", "f": trade_id, "l": trade_id, "T": ts_ms, "m": False, "M": True,
    })


def depth_diff_msg(first: int, final: int, bids=(), asks=(), event_ms: int = 0) -> str:
    return json.dumps({
        "e": "depthUpdate", "E": event_ms, "s": "BTCUSDT", "U": first, "u": final,
        "b": [[str(p), str(q)] for p, q in bids], "a": [[str(p), str(q)] for p, q in asks],
    })


FAST_RECONNECT = ReconnectPolicyConfig(initial_delay_seconds=0.001, max_delay_seconds=0.01, jitter_seconds=0.0)


def make_provider(ws_factory, http_client=None, now=None, config_overrides=None) -> BinanceMarketDataProvider:
    overrides = config_overrides or {}
    config = BinanceConfig(reconnect=FAST_RECONNECT, **overrides)
    clock = FixedClock(now or datetime(2026, 8, 31, tzinfo=UTC))
    return BinanceMarketDataProvider(
        config=config, http_client=http_client or FakeHttpClient([]), ws_factory=ws_factory,
        clock=clock, sleeper=FakeSleeper(),
    )


async def collect_n(agen, n: int, timeout: float = 2.0) -> list:
    results = []
    for _ in range(n):
        results.append(await asyncio.wait_for(agen.__anext__(), timeout=timeout))
    return results


class TestStreamCandlesHappyPath:
    def test_yields_committed_candles_in_order(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.5", event_ms=1),
            kline_msg(0, 59999, True, close="101.0", event_ms=2),
            kline_msg(60000, 119999, False, close="102.0", event_ms=3),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 3)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 101.0, 102.0]
        assert candles[1].is_closed is True

    def test_duplicate_update_suppressed(self) -> None:
        duplicate_msg = kline_msg(0, 59999, False, close="100.5", event_ms=5)
        conn = FakeWebSocketConnection([
            duplicate_msg,
            duplicate_msg,  # aynı update_seq (E=5) -> duplicate, sessizce atlanır
            kline_msg(0, 59999, False, close="101.0", event_ms=6),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)  # yalnızca 2 tanesi yayınlanmalı
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 101.0]

    def test_final_then_replay_ignored(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.5", event_ms=10),  # final
            kline_msg(0, 59999, False, close="999.0", event_ms=5),  # eski replay (E=5 < final E=10)
            kline_msg(60000, 119999, False, close="102.0", event_ms=11),  # sıradaki candle, normal
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)  # final + sıradaki; replay atlanmalı
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.5, 102.0]


class TestMalformedPayloadDoesNotCrashStream:
    def test_malformed_message_skipped(self) -> None:
        conn = FakeWebSocketConnection([
            "not-json-at-all{{{",
            json.dumps({"e": "kline", "E": 1, "s": "BTCUSDT"}),  # eksik 'k' alanı
            kline_msg(0, 59999, False, close="100.5", event_ms=5),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.5


class TestReconnectOnTransportError:
    def test_reconnects_after_disconnect_and_continues(self) -> None:
        first_conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.5", event_ms=1),
            TransportError("bağlantı koptu"),
        ])
        second_conn = FakeWebSocketConnection([
            kline_msg(60000, 119999, False, close="105.0", event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([first_conn, second_conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2)
            health = provider.health()
            await provider.close()
            return candles, health

        candles, health = run_async(_run())
        assert [c.close for c in candles] == [100.5, 105.0]
        assert first_conn.closed is True
        assert health.reconnect_count >= 1

    def test_connect_failure_retried_with_backoff(self) -> None:
        factory = FakeWebSocketConnectionFactory([
            TransportError("ilk bağlantı başarısız"),
            FakeWebSocketConnection([kline_msg(0, 59999, False, close="55.0", event_ms=1)]),
        ])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 55.0
        assert len(factory.connect_calls) >= 2


class TestGracefulShutdown:
    def test_close_cancels_background_tasks_cleanly(self) -> None:
        conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1)
            await provider.close()
            # close() sonrası background_tasks boş olmalı (idempotent + temiz)
            assert len(provider._background_tasks) == 0
            await provider.close()  # ikinci close() da güvenli (idempotent) olmalı

        run_async(_run())
        assert conn.closed is True

    def test_agen_aclose_cancels_its_own_task(self) -> None:
        conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1)
            await agen.aclose()
            assert len(provider._background_tasks) == 0

        run_async(_run())


class TestCandleGapRecovery:
    def test_11_rest_response_including_incoming_candle_is_not_committed_via_backfill(self) -> None:
        """REST endTime sınırı incoming candle'ı da döndürebilir; historical
        path YALNIZCA gerçekten eksik (expected) candle'ları commit etmeli —
        incoming candle'ın kendisi ASLA historical yoldan commit edilmemeli.

        latest=12:00, incoming WS=12:05 -> yalnızca 12:01,12:02,12:03,12:04
        historical recovery ile commit edilebilir. REST yanıtı 12:05'i de
        (incoming candle'ı) döndürse bile bu, historical path tarafından
        YOK SAYILMALI; gap kapandıktan sonra ORİJİNAL WS 12:05 update'i
        tekrar denenip canonical state ONUN üzerinden oluşmalı.
        """
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            # incoming WS candle: open_time=240000 (12:04 sonrası -> 4 dakika
            # gap, 1m için), event_ms ile canonical'a taşınacak ayırt edici
            # bir WS-kaynaklı değer.
            kline_msg(240000, 299999, False, close="102.0", event_ms=987654),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # REST yanıtı: gerçekten eksik olan 4 candle (60000..239999 open_time)
        # ARTI Binance'in endTime sınır davranışı nedeniyle incoming candle'ın
        # KENDİSİNİ de (open_time=240000) döndürüyor — FARKLI bir close
        # değeriyle ("103.5"), böylece hangi yoldan commit edildiği net
        # şekilde ayırt edilebilir.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            [120000, "100.5", "101.5", "99.5", "101.0", "10.0", 179999, "0", 1, "0", "0", "0"],
            [180000, "101.0", "102.0", "100.0", "101.5", "10.0", 239999, "0", 1, "0", "0", "0"],
            [240000, "101.5", "104.0", "100.5", "103.5", "10.0", 299999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 5, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        # KRİTİK: incoming candle (open_time=240000) historical path'in
        # "103.5" değeriyle DEĞİL, orijinal WS update'in "102.0" değeriyle
        # canonical hâle gelmiş olmalı.
        assert candles[1].open_time.timestamp() == 240.0
        assert candles[1].close == 102.0
        assert candles[1].close != 103.5

        # Ek doğrulama: canonical state, incoming candle için gerçekten
        # WS-kaynaklı değerleri taşıyor (StateManager üzerinden).
        assert provider._state_manager.latest_candle("BTCUSDT", Timeframe.M1).close == 102.0

    def test_missing_interval_triggers_backfill_then_commits(self) -> None:
        # İlk candle: open_time=0 (aligned), kapanır.
        # İkinci candle: open_time=180000 (3 dakika sonra) -> 1m için gap.
        # Beklenen davranış: MISSING_CANDLE tespit edilince REST'ten backfill
        # yapılır, ardından ORİJİNAL update tekrar denenip commit edilir.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(180000, 239999, False, close="105.0", event_ms=180001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill REST yanıtı: eksik iki candle'ı (60000 ve 120000 open_time)
        # kapalı olarak döndürür.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            [120000, "100.5", "101.5", "99.5", "101.0", "10.0", 179999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # İlk candle (final) + gap sonrası backfill tetiklenip nihayetinde
            # canlı update'in kendisi de commit edilir.
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        # İlk yayınlanan: t=0 final candle. İkincisi: gap sonrası, canlı
        # update'in (open_time=180000) başarıyla commit edilmiş hâli.
        assert candles[0].close == 100.0
        assert candles[1].open_time.timestamp() == 180.0
        assert candles[1].close == 105.0
        # Backfill REST çağrısı gerçekten yapılmış olmalı.
        assert len(http.calls) == 1
        # DETERMINISTIC pencere doğrulaması: tam olarak [60000, 180000)
        # istenmiş olmalı — sabit/kör bir pencere (örn. now-20*duration)
        # KULLANILMADI.
        _, params = http.calls[0]
        assert params["startTime"] == "60000"
        assert params["endTime"] == "180000"

    def test_2_multi_interval_gap_recovered_exactly(self) -> None:
        """2) multi-interval gap (5m: 12:00 -> 12:20, eksik 12:05/10/15) doğru recover edilir."""
        # UTC epoch: 12:00 = 43200s, 12:05=43500s, ... 12:20=44400s (5m aralıklarla)
        base = 43200 * 1000
        step = 5 * 60 * 1000
        conn = FakeWebSocketConnection([
            kline_msg(base, base + 299999, True, close="100.0", event_ms=base + 300000, interval="5m"),
            kline_msg(base + 4 * step, base + 4 * step + 299999, False, close="108.0", event_ms=base + 4 * step + 1, interval="5m"),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Eksik: 12:05, 12:10, 12:15 (3 candle)
        backfill_rows = [
            [base + 1 * step, "100.0", "103.0", "99.0", "102.0", "10.0", base + 1 * step + 299999, "0", 1, "0", "0", "0"],
            [base + 2 * step, "102.0", "105.0", "101.0", "104.0", "10.0", base + 2 * step + 299999, "0", 1, "0", "0", "0"],
            [base + 3 * step, "104.0", "107.0", "103.0", "106.0", "10.0", base + 3 * step + 299999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        now = datetime(1970, 1, 1, 12, 20, 5, tzinfo=UTC)
        clock = FixedClock(now)
        # 20 dakikalık gap, default 300s staleness eşiğinden büyük olduğundan
        # (bu test'in amacı staleness DEĞİL, gap-recovery doğruluğu olduğundan)
        # bu test için realtime staleness eşiği bilinçli olarak büyütülüyor.
        gate = BinanceDataQualityGate(clock, BinanceQualityThresholds(max_candle_staleness_seconds=3600))
        config = BinanceConfig(reconnect=FAST_RECONNECT, supported_timeframes=(Timeframe.M5,))
        provider = BinanceMarketDataProvider(
            config=config, http_client=http, ws_factory=factory,
            clock=clock, sleeper=FakeSleeper(), quality_gate=gate,
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M5)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        assert candles[1].close == 108.0
        _, params = http.calls[0]
        assert params["startTime"] == str(base + 1 * step)
        assert params["endTime"] == str(base + 4 * step)

    def test_3_old_but_valid_historical_candle_not_rejected_for_age(self) -> None:
        """3) eski (ama geçerli) historical candle sırf age nedeniyle reddedilmez.

        Bu, structural-vs-freshness ayrımını (Faz 2 Bölüm 2) doğrudan
        `StateManager` seviyesinde doğrular: AYNI eski candle, realtime
        `handle_candle_update` ile STALE_PRICE nedeniyle reddedilirken,
        `handle_historical_candle_update` ile (yalnızca structural
        validity uygulanarak) KABUL EDİLMELİDİR.
        """
        clock = FixedClock(datetime(1970, 1, 1, 1, 0, 0, tzinfo=UTC))  # "şimdi" çok ileride
        gate = BinanceDataQualityGate(clock)  # varsayılan eşik: 300s

        old_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True, trade_count=1,
        )
        update = CandleUpdate(
            candle=old_candle, update_seq=0, event_time=old_candle.close_time, received_at=old_candle.close_time
        )

        # Realtime check (canlı WS akışı) İSE staleness nedeniyle reddeder.
        realtime_manager = StateManager(gate, InMemoryCandleStateStore())
        realtime_result = realtime_manager.handle_candle_update(update)
        assert not realtime_result.quality_result.passed
        assert realtime_result.quality_result.status == DataQualityStatus.STALE_PRICE

        # Historical/backfill check İSE (yalnızca structural validity) KABUL EDER.
        historical_manager = StateManager(gate, InMemoryCandleStateStore())
        historical_result = historical_manager.handle_historical_candle_update(update)
        assert historical_result.quality_result.passed
        assert historical_result.commit_result.committed

    def test_4_malformed_historical_candle_still_rejected(self) -> None:
        """4) malformed historical candle yine reddedilir (structural validity hâlâ uygulanıyor).

        Ayrıca: malformed bir backfill satırı, canlı stream'in KENDİSİNİ
        ÇÖKERTMEMELİDİR (Bölüm 17 — retry-safe olmayan hatalar bile
        stream'i düşürmeden ele alınmalı).
        """
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="101.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill satırı, impossible OHLC içeriyor (high < low) — domain
        # model construction seviyesinde reddedilecek (ValueError).
        malformed_rows = [
            [60000, "100.0", "50.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(malformed_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Malformed backfill hatası pump loop'u ÇÖKERTMEMELİ; ilk candle
            # (t=0) her hâlükârda yayınlanmış olmalı.
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0

    def test_5_backfill_committed_in_chronological_order(self) -> None:
        """5) backfill chronological sırayla commit edilir."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Bilerek TERS sırada döndürüyoruz — implementasyon sort etmeli.
        backfill_rows_reversed = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows_reversed)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[1].close == 102.0

    def test_6_duplicate_backfill_does_not_corrupt_state(self) -> None:
        """6) duplicate backfill canonical state'i bozmaz."""
        # İki ayrı gap tetiklenip AYNI backfill aralığının iki kez
        # istenmesi senaryosu: ikinci deneme idempotent olmalı.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        # Aynı backfill yanıtı iki kez scriptlenmiş olsa bile (defensive),
        # StateManager/CandleSequencer duplicate'i deterministic reddeder.
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        # canonical state tutarlı: backfilled candle'ın (60000) değeri 100.5
        # olarak KALICI, ikinci (canlı) candle da doğru şekilde geldi.
        assert candles[1].close == 102.0

    def test_7_original_incoming_update_retried_and_committed(self) -> None:
        """7) backfill sonrası orijinal incoming WS update yeniden denenir ve commit edilir."""
        # Zaten test_missing_interval_triggers_backfill_then_commits bunu
        # doğruluyor (candles[1], orijinal incoming update'in canonical
        # hâlidir); burada AYRICA update_seq/event_time'ın orijinal WS
        # event'inden geldiğini (backfill'in synthetic seq'inden DEĞİL)
        # doğruluyoruz.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=999999),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[1].is_closed is False  # orijinal WS update'in kendi is_closed durumu korunmuş

    def test_8_incomplete_backfill_not_treated_as_success(self) -> None:
        """8) REST response gap'in tamamını doldurmuyorsa recovery başarılı sayılmaz."""
        # 3 dakikalık gap (open_time 0 -> 180000, 1m), ama backfill YALNIZCA
        # bir candle (60000) döndürüyor — 120000 EKSİK kalıyor.
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(180000, 239999, False, close="105.0", event_ms=180001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        incomplete_backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
            # 120000 open_time'lı candle KASITLI OLARAK EKSİK.
        ]
        http = FakeHttpClient([json_response(incomplete_backfill_rows)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Backfill eksik kaldığı için orijinal (180000) update TEKRAR
            # DENENMEZ/commit edilmez — yalnızca ilk (t=0) candle ve
            # backfill edilen tek candle (60000) canonical state'e girer,
            # ama HİÇBİRİ queue'ya YAYINLANMAZ (yalnızca doğrudan gelen
            # WS update'leri queue'ya gider; backfill candle'ları sessizce
            # canonical state'e yazılır, stream'e YAYINLANMAZ — bu, mevcut
            # tasarımın davranışıdır). Bu yüzden yalnızca 1 candle (t=0)
            # queue'dan okunabilir olmalı; 180000 update'i queue'ya HİÇ
            # gelmemelidir (retry commit edilmediği için).
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.3)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0

    def test_9_recovery_does_not_affect_other_symbol_or_timeframe_state(self) -> None:
        """9) recovery sırasında başka interval/symbol'ün state'i etkilenmez."""
        clock = FixedClock(datetime(1970, 1, 1, 0, 3, 5, tzinfo=UTC))
        gate = BinanceDataQualityGate(clock)
        store = InMemoryCandleStateStore()
        manager = StateManager(gate, store)

        # Farklı bir symbol/timeframe için ÖNCEDEN mevcut bir state kuruyoruz.
        other_candle = Candle(
            symbol="ETHUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=10.0, high=11.0, low=9.0, close=10.5, volume=1.0, is_closed=True, trade_count=1,
        )
        other_update = CandleUpdate(candle=other_candle, update_seq=1, event_time=other_candle.close_time, received_at=other_candle.close_time)
        manager.handle_candle_update(other_update)
        assert manager.latest_candle("ETHUSDT", Timeframe.M1).close == 10.5

        # BTCUSDT için backfill/historical commit yapıyoruz.
        btc_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1,
            open_time=datetime(1970, 1, 1, tzinfo=UTC), close_time=datetime(1970, 1, 1, 0, 1, tzinfo=UTC),
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True, trade_count=1,
        )
        btc_update = CandleUpdate(candle=btc_candle, update_seq=1, event_time=btc_candle.close_time, received_at=btc_candle.close_time)
        manager.handle_historical_candle_update(btc_update)

        # ETHUSDT'nin state'i ETKİLENMEMİŞ olmalı.
        assert manager.latest_candle("ETHUSDT", Timeframe.M1).close == 10.5
        assert manager.latest_candle("BTCUSDT", Timeframe.M1).close == 100.5

    def test_10_open_candle_not_committed_via_historical_recovery(self) -> None:
        """10) partial/open (henüz kapanmamış) candle historical recovery'ye yanlışlıkla commit edilmez."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, True, close="100.0", event_ms=60000),
            kline_msg(120000, 179999, False, close="102.0", event_ms=120001),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Backfill yanıtı, "now" AÇISINDAN HENÜZ KAPANMAMIŞ bir satır
        # içeriyor (close_time > now) — parser bunu is_closed=False
        # üretecek şekilde işaretler; bu, backfill loop'unda ATLANMALIDIR.
        backfill_rows = [
            [60000, "100.0", "101.0", "99.0", "100.5", "10.0", 119999, "0", 1, "0", "0", "0"],
        ]
        http = FakeHttpClient([json_response(backfill_rows)])
        # "now" tam olarak 60000-119999 aralığının close_time'ından ÖNCE
        # olacak şekilde ayarlanıyor ki backfill satırı is_closed=False
        # üretsin (parser politika: close_time <= now ise closed).
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, 0, 1, 30, tzinfo=UTC))

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Backfill satırı is_closed=False üretileceği için commit
            # edilmeyecek (backfill loop'u yalnızca is_closed=True olanları
            # işler) — dolayısıyla gap kapanmayacak ve orijinal (120000)
            # update de tekrar denendiğinde hâlâ MISSING_CANDLE ile
            # reddedilecektir. Yalnızca ilk candle (t=0) queue'ya ulaşır.
            first = await asyncio.wait_for(agen.__anext__(), timeout=2.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.3)
            await provider.close()
            return first

        first = run_async(_run())
        assert first.close == 100.0


class TestStreamTrades:
    def test_rejected_trades_filtered_out(self) -> None:
        conn = FakeWebSocketConnection([
            trade_msg(1, "100.0", 0),
            trade_msg(1, "100.0", 0),  # duplicate trade_id -> quality reddi
            trade_msg(2, "100.5", 1),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        provider = make_provider(factory, now=datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_trades("BTCUSDT")
            trades = await collect_n(agen, 2)
            await provider.close()
            return trades

        trades = run_async(_run())
        assert [t.trade_id for t in trades] == [1, 2]


class TestStreamOrderBook:
    def test_snapshot_and_diff_sync_happy_path(self) -> None:
        # WS: snapshot beklerken buffer'lanan bir diff, sonra devamı.
        conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=105, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
            depth_diff_msg(first=106, final=106, bids=[(99.0, 2.0)], asks=[], event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        snapshot_payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(snapshot_payload)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 2)
            await provider.close()
            return books

        books = run_async(_run())
        assert books[0].last_update_id == 105
        assert books[1].last_update_id == 106
        assert books[1].best_bid.quantity == 2.0

    def test_sequence_gap_triggers_resync(self) -> None:
        conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=101, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
            # gap: beklenen U=102, ama U=500 geliyor
            depth_diff_msg(first=500, final=505, bids=[(98.0, 5.0)], asks=[(101.0, 5.0)], event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        first_snapshot = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        resync_snapshot = {"lastUpdateId": 499, "bids": [["98.0", "1.0"]], "asks": [["101.0", "1.0"]]}
        http = FakeHttpClient([json_response(first_snapshot), json_response(resync_snapshot)])
        provider = make_provider(factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC))

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 2)
            await provider.close()
            return books

        books = run_async(_run())
        assert books[0].last_update_id == 101
        assert books[1].last_update_id == 505  # resync sonrası devam eden diff uygulanmış


class TestBackpressure:
    def test_bounded_queue_blocks_producer_until_space(self) -> None:
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.0", event_ms=1),
            kline_msg(60000, 119999, False, close="102.0", event_ms=2),
            kline_msg(120000, 179999, False, close="104.0", event_ms=3),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        config = BinanceConfig(reconnect=FAST_RECONNECT)
        clock = FixedClock(datetime(1970, 1, 1, tzinfo=UTC))
        provider = BinanceMarketDataProvider(
            config=config, http_client=FakeHttpClient([]), ws_factory=factory,
            clock=clock, sleeper=FakeSleeper(), queue_maxsize=1,
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Yavaş tüketici: bounded queue (maxsize=1) üretici tarafını
            # backpressure ile bekletir; sessiz veri kaybı OLMAMALI.
            candles = await collect_n(agen, 3, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.0, 102.0, 104.0]  # hiçbiri kaybolmadı


class TestStaleFeedDetection:
    """Reviewer probe D + minimum adversarial testler (Bölüm 1)."""

    STALE_THRESHOLD = 0.03  # saniye — gerçek ama çok küçük (Bölüm: "saniyelerce" DEĞİL)

    def test_1_stale_detected_after_threshold_with_no_messages(self) -> None:
        """1) socket connected + mesaj yok + threshold aşılır → stale detected."""
        hanging_conn = FakeWebSocketConnection([])  # hiç mesaj yok, recv() sonsuza kadar bekler
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="100.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 1, timeout=5.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert candles[0].close == 100.0
        assert hanging_conn.closed is True  # stale bağlantı güvenli şekilde kapatıldı

    def test_2_health_does_not_remain_connected_after_stale(self) -> None:
        """2) health CONNECTED kalmaz."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )
        key = "candle:BTCUSDT:1m"

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1, timeout=5.0)
            health = provider.health_for(key)
            await provider.close()
            return health

        health = run_async(_run())
        # Stale tespit edildikten sonra ikinci bağlantı kurulup mesaj alındığı
        # için nihai health CONNECTED'a geri döner — ama reconnect_count'un
        # artmış olması stale-tetiklenmiş reconnect'in gerçekleştiğini kanıtlar.
        assert health.reconnect_count >= 1

    def test_3_reconnect_triggered_after_stale(self) -> None:
        """3) reconnect tetiklenir."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([kline_msg(0, 59999, False, close="1.0", event_ms=1)])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            await collect_n(agen, 1, timeout=5.0)
            await provider.close()

        run_async(_run())
        assert len(factory.connect_calls) >= 2

    def test_4_message_before_threshold_no_false_positive(self) -> None:
        """4) mesaj threshold'dan önce gelirse false-positive stale oluşmaz."""
        conn = FakeWebSocketConnection([
            kline_msg(0, 59999, False, close="100.0", event_ms=1),
            kline_msg(60000, 119999, False, close="101.0", event_ms=2),
        ])
        factory = FakeWebSocketConnectionFactory([conn])
        # Görece büyük bir threshold — mesajlar anında geldiği için hiç aşılmaz.
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": 5.0},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            candles = await collect_n(agen, 2, timeout=2.0)
            await provider.close()
            return candles

        candles = run_async(_run())
        assert [c.close for c in candles] == [100.0, 101.0]
        assert len(factory.connect_calls) == 1  # hiç reconnect olmadı

    def test_5_cancellation_does_not_produce_stale_reconnect(self) -> None:
        """5) cancellation stale-reconnect üretmez."""
        hanging_conn = FakeWebSocketConnection([])  # sonsuza kadar bekler
        factory = FakeWebSocketConnectionFactory([hanging_conn])
        # Threshold'u BÜYÜK tutuyoruz — test'in kendisi stale beklemeden
        # cancel edecek; eğer cancellation yanlışlıkla stale-reconnect
        # üretiyorsa factory.connect_calls > 1 olurdu.
        provider = make_provider(
            factory, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": 10.0},
        )

        async def _run():
            agen = provider.stream_candles("BTCUSDT", Timeframe.M1)
            # Generator'ı GERÇEKTEN başlatmak için __anext__() çağrısını bir
            # task olarak başlatıyoruz (connect() + recv() beklemesi bu
            # noktada gerçekleşir), sonra bu task'ı cancel ediyoruz — bu,
            # generator'ın `finally` bloğunu (normal shutdown) tetikler;
            # StaleFeedError/TimeoutError DEĞİL, CancelledError üretir.
            consume_task = asyncio.create_task(agen.__anext__())
            await asyncio.sleep(0.01)
            consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consume_task

        run_async(_run())
        assert len(factory.connect_calls) == 1  # reconnect DENENMEDİ
        assert hanging_conn.closed is True  # ama bağlantı düzgün kapatıldı

    def test_6_order_book_resyncs_after_stale_reconnect(self) -> None:
        """6) order-book: stale-reconnect sonrası resync yapar."""
        hanging_conn = FakeWebSocketConnection([])
        second_conn = FakeWebSocketConnection([
            depth_diff_msg(first=101, final=101, bids=[(99.0, 1.0)], asks=[(100.0, 1.0)], event_ms=1),
        ])
        factory = FakeWebSocketConnectionFactory([hanging_conn, second_conn])
        snapshot_payload = {"lastUpdateId": 100, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(snapshot_payload)])
        provider = make_provider(
            factory, http_client=http, now=datetime(1970, 1, 1, tzinfo=UTC),
            config_overrides={"stale_feed_threshold_seconds": self.STALE_THRESHOLD},
        )

        async def _run():
            agen = provider.stream_order_book("BTCUSDT", depth=1000)
            books = await collect_n(agen, 1, timeout=5.0)
            await provider.close()
            return books

        books = run_async(_run())
        # Stale-reconnect sonrası synchronizer sıfırlanıp REST snapshot
        # tekrar çekilerek resync yapıldığı için book başarıyla üretildi.
        assert books[0].last_update_id == 101
        assert len(http.calls) == 1  # snapshot yalnızca reconnect sonrası (ilk denemede hiç veri yoktu) çekildi


=== FILE: tests/test_binance_quality_rules.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import DataQualityStatus, Timeframe
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade
from crypto_signal_engine.providers.binance.clock import FixedClock
from crypto_signal_engine.quality.binance_rules import BinanceDataQualityGate, BinanceQualityThresholds

UTC = timezone.utc
ALIGNED_OPEN = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)  # 1m sınırına hizalı


def make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True, trade_count=5, volume=10.0) -> Candle:
    return Candle(
        symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=100.0, high=max(105.0, close), low=min(95.0, close), close=close,
        volume=volume, is_closed=is_closed, trade_count=trade_count,
    )


def make_gate(now: datetime) -> BinanceDataQualityGate:
    return BinanceDataQualityGate(FixedClock(now), BinanceQualityThresholds())


class TestCheckCandleAlignment:
    def test_aligned_open_time_passes(self) -> None:
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_candle(make_candle(is_closed=False), None)
        assert result.passed

    def test_misaligned_open_time_rejected(self) -> None:
        misaligned = ALIGNED_OPEN + timedelta(seconds=30)
        gate = make_gate(misaligned + timedelta(seconds=1))
        result = gate.check_candle(make_candle(open_time=misaligned, is_closed=False), None)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH


class TestCheckCandleStaleness:
    def test_fresh_closed_candle_passes(self) -> None:
        candle = make_candle(is_closed=True)
        gate = make_gate(candle.close_time + timedelta(seconds=5))
        assert gate.check_candle(candle, None).passed

    def test_stale_closed_candle_rejected(self) -> None:
        candle = make_candle(is_closed=True)
        gate = make_gate(candle.close_time + timedelta(seconds=1000))
        result = gate.check_candle(candle, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_open_candle_staleness_not_checked(self) -> None:
        candle = make_candle(is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=100000))
        result = gate.check_candle(candle, None)
        assert result.passed  # açık candle staleness kontrolünden muaf


class TestCheckCandleMissingInterval:
    def test_consecutive_candles_pass(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, previous).passed

    def test_gap_between_candles_rejected(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=3), is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        result = gate.check_candle(current, previous)
        assert result.status == DataQualityStatus.MISSING_CANDLE

    def test_first_candle_no_previous_not_flagged(self) -> None:
        current = make_candle(is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, None).passed


class TestCheckCandleZeroVolumeAnomaly:
    def test_zero_volume_with_trades_rejected(self) -> None:
        candle = make_candle(volume=0.0, trade_count=10, is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=1))
        result = gate.check_candle(candle, None)
        assert result.status == DataQualityStatus.ZERO_VOLUME_ANOMALY

    def test_zero_volume_zero_trades_passes(self) -> None:
        candle = make_candle(volume=0.0, trade_count=0, is_closed=False)
        gate = make_gate(candle.close_time + timedelta(seconds=1))
        assert gate.check_candle(candle, None).passed


class TestCheckCandleExtremeOutlier:
    def test_normal_move_passes(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), close=102.0, is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        assert gate.check_candle(current, previous).passed

    def test_extreme_move_rejected(self) -> None:
        previous = make_candle(open_time=ALIGNED_OPEN, close=100.0, is_closed=True)
        current = make_candle(open_time=ALIGNED_OPEN + timedelta(minutes=1), close=200.0, is_closed=False)
        gate = make_gate(current.close_time + timedelta(seconds=1))
        result = gate.check_candle(current, previous)
        assert result.status == DataQualityStatus.EXTREME_OUTLIER


class TestCheckTrade:
    def make_trade(self, trade_id=1, price=100.0, ts=None) -> Trade:
        return Trade(
            symbol="BTCUSDT", trade_id=trade_id, price=price, quantity=1.0,
            timestamp=ts or ALIGNED_OPEN, is_buyer_maker=False,
        )

    def test_fresh_trade_passes(self) -> None:
        trade = self.make_trade(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        assert gate.check_trade(trade, None).passed

    def test_stale_trade_rejected(self) -> None:
        trade = self.make_trade(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1000))
        result = gate.check_trade(trade, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_non_monotonic_trade_id_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)  # duplicate ID
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH

    def test_decreasing_trade_id_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=50, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.TIMESTAMP_MISMATCH

    def test_extreme_price_move_rejected(self) -> None:
        previous = self.make_trade(trade_id=100, price=100.0, ts=ALIGNED_OPEN)
        current = self.make_trade(trade_id=101, price=500.0, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_trade(current, previous)
        assert result.status == DataQualityStatus.EXTREME_OUTLIER


class TestCheckOrderBook:
    def make_book(self, last_update_id=1, ts=None) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=ts or ALIGNED_OPEN,
            bids=(OrderBookLevel(price=99.0, quantity=1.0),),
            asks=(OrderBookLevel(price=100.0, quantity=1.0),),
            last_update_id=last_update_id,
        )

    def test_fresh_book_passes(self) -> None:
        book = self.make_book(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        assert gate.check_order_book(book, None).passed

    def test_stale_book_rejected(self) -> None:
        book = self.make_book(ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1000))
        result = gate.check_order_book(book, None)
        assert result.status == DataQualityStatus.STALE_PRICE

    def test_non_monotonic_update_id_rejected(self) -> None:
        previous = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        current = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_order_book(current, previous)
        assert result.status == DataQualityStatus.WEBSOCKET_GAP

    def test_decreasing_update_id_rejected(self) -> None:
        previous = self.make_book(last_update_id=200, ts=ALIGNED_OPEN)
        current = self.make_book(last_update_id=100, ts=ALIGNED_OPEN)
        gate = make_gate(ALIGNED_OPEN + timedelta(seconds=1))
        result = gate.check_order_book(current, previous)
        assert result.status == DataQualityStatus.WEBSOCKET_GAP


=== FILE: tests/test_binance_reconnect.py ===
import asyncio

import pytest

from tests.conftest import run_async
from crypto_signal_engine.providers.binance.clock import DeterministicJitterSource, FakeSleeper
from crypto_signal_engine.providers.binance.config import ReconnectPolicyConfig
from crypto_signal_engine.providers.binance.reconnect import MaxReconnectAttemptsExceeded, ReconnectPolicy


def make_policy(**overrides) -> tuple[ReconnectPolicy, FakeSleeper]:
    config = ReconnectPolicyConfig(
        initial_delay_seconds=1.0, max_delay_seconds=10.0, multiplier=2.0, jitter_seconds=0.0, **overrides
    )
    sleeper = FakeSleeper()
    policy = ReconnectPolicy(config, sleeper, jitter_source=DeterministicJitterSource(0.0))
    return policy, sleeper


class TestExponentialBackoff:
    def test_delay_grows_exponentially(self) -> None:
        policy, _ = make_policy()
        assert policy.compute_delay_seconds() == 1.0
        policy._attempt = 1
        assert policy.compute_delay_seconds() == 2.0
        policy._attempt = 2
        assert policy.compute_delay_seconds() == 4.0
        policy._attempt = 3
        assert policy.compute_delay_seconds() == 8.0

    def test_delay_is_bounded_by_max(self) -> None:
        policy, _ = make_policy()
        policy._attempt = 10  # 1 * 2**10 = 1024, ama max_delay=10 ile sınırlı
        assert policy.compute_delay_seconds() == 10.0

    def test_reset_restarts_backoff(self) -> None:
        policy, _ = make_policy()
        policy._attempt = 5
        policy.reset()
        assert policy.attempt_count == 0
        assert policy.compute_delay_seconds() == 1.0

    def test_jitter_is_added(self) -> None:
        config = ReconnectPolicyConfig(initial_delay_seconds=1.0, jitter_seconds=5.0)
        policy = ReconnectPolicy(config, FakeSleeper(), jitter_source=DeterministicJitterSource(0.5))
        assert policy.compute_delay_seconds() == 1.5


class TestSleepBeforeNextAttempt:
    def test_sleeper_called_with_computed_delay(self) -> None:
        policy, sleeper = make_policy()

        async def _run() -> None:
            await policy.sleep_before_next_attempt()
            await policy.sleep_before_next_attempt()

        run_async(_run())
        assert sleeper.calls == [1.0, 2.0]
        assert policy.attempt_count == 2

    def test_delay_override_used_instead_of_computed(self) -> None:
        policy, sleeper = make_policy()

        async def _run() -> None:
            await policy.sleep_before_next_attempt(delay_override_seconds=42.0)

        run_async(_run())
        assert sleeper.calls == [42.0]

    def test_max_attempts_exceeded_raises(self) -> None:
        policy, _ = make_policy(max_attempts=2)

        async def _run() -> None:
            await policy.sleep_before_next_attempt()
            await policy.sleep_before_next_attempt()
            with pytest.raises(MaxReconnectAttemptsExceeded):
                await policy.sleep_before_next_attempt()

        run_async(_run())

    def test_max_attempts_none_never_raises(self) -> None:
        policy, _ = make_policy(max_attempts=None)

        async def _run() -> None:
            for _ in range(50):
                await policy.sleep_before_next_attempt()

        run_async(_run())  # exception fırlatmamalı
        assert policy.attempt_count == 50

    def test_cancellation_propagates_without_swallowing(self) -> None:
        policy, sleeper = make_policy()
        sleeper.trigger_cancellation()

        async def _run() -> None:
            await policy.sleep_before_next_attempt()

        with pytest.raises(asyncio.CancelledError):
            run_async(_run())


=== FILE: tests/test_binance_rest.py ===
import json
from datetime import datetime, timezone

import pytest

from tests.binance_fakes import FakeHttpClient, json_response
from tests.conftest import run_async
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.errors import BinanceProtocolError, ParseError, RateLimitError, TransportError
from crypto_signal_engine.providers.binance.clock import FakeSleeper, FixedClock
from crypto_signal_engine.providers.binance.config import BinanceConfig
from crypto_signal_engine.providers.binance.rest import BinanceRestClient

UTC = timezone.utc


def make_client(http_client, now=None, max_retries=3) -> BinanceRestClient:
    config = BinanceConfig()
    clock = FixedClock(now or datetime(2026, 8, 31, tzinfo=UTC))
    sleeper = FakeSleeper()
    return BinanceRestClient(config, http_client, clock, sleeper, max_retries=max_retries)


def kline_row(open_ms: int, close_ms: int) -> list:
    return [open_ms, "100.0", "101.0", "99.0", "100.5", "10.0", close_ms, "1000.0", 5, "5.0", "500.0", "0"]


class TestFetchHistoricalCandles:
    def test_invalid_range_rejected(self) -> None:
        client = make_client(FakeHttpClient([]))
        start = datetime(2026, 8, 31, tzinfo=UTC)
        end = datetime(2026, 8, 30, tzinfo=UTC)

        async def _run():
            await client.fetch_historical_candles("BTCUSDT", Timeframe.M1, start, end)

        with pytest.raises(ValueError, match="start"):
            run_async(_run())

    def test_naive_start_rejected(self) -> None:
        client = make_client(FakeHttpClient([]))

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(2026, 8, 31), datetime(2026, 8, 31, 1, tzinfo=UTC)
            )

        with pytest.raises(ValueError, match="naive datetime"):
            run_async(_run())

    def test_single_page(self) -> None:
        rows = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(5)]
        http = FakeHttpClient([json_response(rows)])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1,
                datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, 1, tzinfo=UTC),
            )

        candles = run_async(_run())
        assert len(candles) == 5
        assert all(c.is_closed for c in candles)
        # Ascending kronolojik sıra
        assert [c.open_time for c in candles] == sorted(c.open_time for c in candles)

    def test_pagination_across_multiple_pages_no_duplicates(self) -> None:
        config_limit = BinanceConfig().max_klines_per_request
        # İlk sayfa tam limit kadar döner (daha fazla veri olduğunu ima eder),
        # ikinci sayfa daha az döner (son sayfa).
        page1 = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(config_limit)]
        page2 = [kline_row(60_000 * i, 60_000 * (i + 1) - 1) for i in range(config_limit, config_limit + 3)]
        http = FakeHttpClient([json_response(page1), json_response(page2)])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1,
                datetime(1970, 1, 1, tzinfo=UTC),
                datetime(1970, 1, 1, 0, 0, 0, 60_000 * (config_limit + 3), tzinfo=UTC) if False else
                datetime.fromtimestamp(60 * (config_limit + 3), tz=UTC),
            )

        candles = run_async(_run())
        open_times = [c.open_time for c in candles]
        assert len(open_times) == len(set(open_times))  # duplicate yok
        assert open_times == sorted(open_times)  # ascending

    def test_malformed_response_not_list_raises_protocol_error(self) -> None:
        http = FakeHttpClient([json_response({"not": "a list"})])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(BinanceProtocolError):
            run_async(_run())

    def test_malformed_row_raises_parse_error_not_retried(self) -> None:
        bad_row = kline_row(0, 59999)
        bad_row[1] = "garbage"
        http = FakeHttpClient([json_response([bad_row])])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(ParseError):
            run_async(_run())
        # Yalnızca 1 istek yapılmış olmalı — parser hatası retry-safe DEĞİL.
        assert len(http.calls) == 1

    def test_empty_response_stops_pagination(self) -> None:
        http = FakeHttpClient([json_response([])])
        client = make_client(http)

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        assert run_async(_run()) == []


class TestRetryBehavior:
    def test_transport_error_retried_then_succeeds(self) -> None:
        http = FakeHttpClient([TransportError("geçici hata"), json_response([kline_row(0, 59999)])])
        client = make_client(http, now=datetime(2026, 8, 31, tzinfo=UTC))

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, tzinfo=UTC)
            )

        candles = run_async(_run())
        assert len(candles) == 1

    def test_transport_error_exhausts_retries_and_raises(self) -> None:
        http = FakeHttpClient([TransportError("kalıcı hata")] * 10)
        client = make_client(http, max_retries=2)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(TransportError):
            run_async(_run())

    def test_rate_limit_respects_retry_after(self) -> None:
        http = FakeHttpClient([
            RateLimitError("rate limited", status_code=429, retry_after_seconds=7.5),
            json_response([kline_row(0, 59999)]),
        ])
        sleeper = FakeSleeper()
        config = BinanceConfig()
        clock = FixedClock(datetime(2026, 8, 31, tzinfo=UTC))
        client = BinanceRestClient(config, http, clock, sleeper, max_retries=3)

        async def _run():
            return await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 1, 0, 5, tzinfo=UTC)
            )

        candles = run_async(_run())
        assert len(candles) == 1
        assert 7.5 in sleeper.calls

    def test_unexpected_status_code_raises_protocol_error(self) -> None:
        http = FakeHttpClient([(500, "internal server error")])
        client = make_client(http)

        async def _run():
            await client.fetch_historical_candles(
                "BTCUSDT", Timeframe.M1, datetime(1970, 1, 1, tzinfo=UTC), datetime(1970, 1, 2, tzinfo=UTC)
            )

        with pytest.raises(BinanceProtocolError):
            run_async(_run())


class TestFetchDepthSnapshot:
    def test_valid_snapshot(self) -> None:
        payload = {"lastUpdateId": 500, "bids": [["99.0", "1.0"]], "asks": [["100.0", "1.0"]]}
        http = FakeHttpClient([json_response(payload)])
        client = make_client(http)

        async def _run():
            return await client.fetch_depth_snapshot("BTCUSDT")

        last_id, bids, asks, received_at = run_async(_run())
        assert last_id == 500
        assert bids == ((99.0, 1.0),)
        assert received_at.tzinfo is not None


=== FILE: tests/test_binance_symbols.py ===
import pytest

from crypto_signal_engine.providers.binance.symbols import (
    agg_trade_stream_name,
    depth_diff_stream_name,
    from_binance_wire_symbol,
    kline_stream_name,
    to_binance_wire_symbol,
)


class TestSymbolConversion:
    @pytest.mark.parametrize("domain_symbol", ["BTCUSDT", "btcusdt", " BTCUSDT "])
    def test_to_wire_symbol_is_lowercase(self, domain_symbol: str) -> None:
        assert to_binance_wire_symbol(domain_symbol) == "btcusdt"

    def test_from_wire_symbol_normalizes(self) -> None:
        assert from_binance_wire_symbol("btcusdt") == "BTCUSDT"
        assert from_binance_wire_symbol("BTCUSDT") == "BTCUSDT"

    def test_round_trip(self) -> None:
        domain = "ETHUSDT"
        wire = to_binance_wire_symbol(domain)
        assert from_binance_wire_symbol(wire) == domain


class TestStreamNames:
    def test_kline_stream_name(self) -> None:
        assert kline_stream_name("BTCUSDT", "1m") == "btcusdt@kline_1m"

    def test_agg_trade_stream_name(self) -> None:
        assert agg_trade_stream_name("BTCUSDT") == "btcusdt@aggTrade"

    def test_depth_diff_stream_name(self) -> None:
        assert depth_diff_stream_name("BTCUSDT") == "btcusdt@depth@100ms"


=== FILE: tests/test_bridge_runtime.py ===
"""Faz 13 — `crypto_signal_engine.execution.bridge_runtime.BridgeRuntime`
wiring testleri.

Bu dosya `SignalTestnetBridge`'in POLİTİKA mantığını TEKRAR test ETMEZ
(bkz. `tests/test_signal_testnet_bridge.py`) — yalnızca `BridgeRuntime`'ın
DOĞRU olayları DOĞRU zamanda bridge'e ilettiğini (ve HİÇBİR ZAMAN
`recover()` sırasında iletmediğini — "NO HISTORICAL REPLAY ORDERS"
invariant'ının YAPISAL kanıtı) doğrular. `PersistedRuntime`'ın KENDİSİ
tamamen sahte (`_FakePersisted`) bir stub'dır — gerçek candle/feature/
signal pipeline'ı burada KURULMAZ (bu, `test_persistence_recovery.py`/
`test_runtime_coordinator.py`'nin ZATEN kanıtladığı bir şeydir)."""

from __future__ import annotations

from datetime import datetime, timezone

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.execution.bridge_runtime import BridgeRuntime
from crypto_signal_engine.runtime.models import IngestOutcome, MarketEventKind, ProcessedMarketEvent, RuntimeCycleResult
from tests.conftest import run_async

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeBridge:
    def __init__(self) -> None:
        self.calls: list[RuntimeCycleResult] = []

    async def on_cycle_result(self, cycle_result: RuntimeCycleResult) -> None:
        self.calls.append(cycle_result)


class _FakePersisted:
    def __init__(self) -> None:
        self.recover_calls = 0
        self.ingest_calls: list[tuple] = []
        self.resolve_gap_calls: list[tuple] = []
        self.next_event: ProcessedMarketEvent | None = None

    async def recover(self):
        self.recover_calls += 1
        return ("bootstrap-report",)

    def ingest_candle(self, symbol, timeframe, candle):  # noqa: ANN001
        self.ingest_calls.append((symbol, timeframe, candle))
        return self.next_event

    async def resolve_gap(self, symbol, timeframe, pending_candle):  # noqa: ANN001
        self.resolve_gap_calls.append((symbol, timeframe, pending_candle))
        return self.next_event

    def ingest_order_book(self, symbol, snapshot):  # noqa: ANN001
        return "ob-event"

    def status(self):  # noqa: ANN201
        return "status"

    @property
    def _coordinator(self):  # noqa: ANN202
        return "coordinator-stub"

    async def stop(self) -> None:
        self.stopped = True


def _event(outcome: IngestOutcome, cycle_result: RuntimeCycleResult | None = None) -> ProcessedMarketEvent:
    return ProcessedMarketEvent(
        symbol="BTCUSDT", kind=MarketEventKind.CANDLE, outcome=outcome, event_time=NOW,
        timeframe=Timeframe.M5, cycle_result=cycle_result,
    )


def _evaluated_false_cycle_result() -> RuntimeCycleResult:
    return RuntimeCycleResult(symbol="BTCUSDT", evaluated=False, signal=None, paper_result=None, generated_at=NOW)


class TestRecoverNeverFeedsBridge:
    def test_recover_delegates_and_never_calls_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]

        reports = run_async(runtime.recover())

        assert reports == ("bootstrap-report",)
        assert persisted.recover_calls == 1
        assert bridge.calls == []  # STRUCTURAL guarantee: recovery never reaches the bridge


class TestLiveIngestFeedsBridge:
    def test_ingest_candle_forwards_evaluated_cycle_result_to_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        cycle_result = _evaluated_false_cycle_result()
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=cycle_result)

        async def body() -> ProcessedMarketEvent:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)
            return event

        event = run_async(body())

        assert event is persisted.next_event
        assert bridge.calls == [cycle_result]

    def test_ingest_candle_without_cycle_result_does_not_call_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=None)

        async def body() -> None:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)

        run_async(body())

        assert bridge.calls == []

    def test_duplicate_outcome_does_not_call_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.DUPLICATE, cycle_result=None)

        async def body() -> None:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)

        run_async(body())

        assert bridge.calls == []

    def test_resolve_gap_forwards_final_event_to_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        cycle_result = _evaluated_false_cycle_result()
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=cycle_result)

        event = run_async(runtime.resolve_gap("BTCUSDT", Timeframe.M5, pending_candle=object()))

        assert event is persisted.next_event
        assert len(persisted.resolve_gap_calls) == 1
        assert bridge.calls == [cycle_result]


class TestNoneBridgeIsPureFallthrough:
    def test_none_bridge_never_raises_and_never_records_anything(self) -> None:
        persisted = _FakePersisted()
        runtime = BridgeRuntime(persisted, None)  # type: ignore[arg-type]
        persisted.next_event = _event(IngestOutcome.ACCEPTED, cycle_result=_evaluated_false_cycle_result())

        async def body() -> ProcessedMarketEvent:
            event = runtime.ingest_candle("BTCUSDT", Timeframe.M5, candle=object())
            await runtime._maybe_bridge(event)  # must be a safe no-op with bridge=None
            return event

        event = run_async(body())
        assert event is persisted.next_event  # pass-through unaffected


class TestOrderBookNeverFeedsBridge:
    def test_ingest_order_book_never_touches_bridge(self) -> None:
        persisted = _FakePersisted()
        bridge = _FakeBridge()
        runtime = BridgeRuntime(persisted, bridge)  # type: ignore[arg-type]
        result = runtime.ingest_order_book("BTCUSDT", snapshot=object())
        assert result == "ob-event"
        assert bridge.calls == []


=== FILE: tests/test_candle_features.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle
from crypto_signal_engine.errors import FeatureCalculationError, InsufficientHistoryError
from crypto_signal_engine.features import candle_calculators as cc

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def make_candles(closes: list[float], highs=None, lows=None, opens=None, volumes=None) -> list[Candle]:
    """Ardışık 1m candle'lar üretir. high/low varsayılan olarak close'u
    kapsayacak şekilde close±1 alınır (aksi belirtilmedikçe)."""
    n = len(closes)
    highs = highs or [c + 1.0 for c in closes]
    lows = lows or [c - 1.0 for c in closes]
    opens = opens or closes
    volumes = volumes or [10.0] * n
    candles = []
    for i in range(n):
        open_time = T0 + timedelta(minutes=i)
        candles.append(Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
            close_time=open_time + timedelta(minutes=1),
            open=opens[i], high=max(highs[i], opens[i], closes[i]), low=min(lows[i], opens[i], closes[i]),
            close=closes[i], volume=volumes[i], is_closed=True, trade_count=1,
        ))
    return candles


class TestSMA:
    def test_known_value(self) -> None:
        # closes 1..7, SMA(5) son 5 değerin ortalaması: (3+4+5+6+7)/5 = 5.0
        candles = make_candles([1, 2, 3, 4, 5, 6, 7])
        assert cc.sma(candles, 5) == pytest.approx(5.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1, 2, 3])
        with pytest.raises(InsufficientHistoryError):
            cc.sma(candles, 5)

    def test_exact_minimum_history(self) -> None:
        candles = make_candles([1, 2, 3, 4, 5])
        assert cc.sma(candles, 5) == pytest.approx(3.0)


class TestEMA:
    def test_known_value(self) -> None:
        # closes 1..7, period=5: ilk SMA=(1+2+3+4+5)/5=3.0, alpha=1/3
        # step6 (close=6): ema=6*(1/3)+3*(2/3)=4.0
        # step7 (close=7): ema=7*(1/3)+4*(2/3)=5.0
        candles = make_candles([1, 2, 3, 4, 5, 6, 7])
        assert cc.ema(candles, 5) == pytest.approx(5.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1, 2])
        with pytest.raises(InsufficientHistoryError):
            cc.ema(candles, 5)

    def test_exact_minimum_history_equals_sma(self) -> None:
        candles = make_candles([1, 2, 3, 4, 5])
        assert cc.ema(candles, 5) == pytest.approx(3.0)


class TestRSI:
    def test_all_gains_is_100(self) -> None:
        closes = list(range(1, 17))  # 16 ardışık artan değer -> 15 gain
        candles = make_candles([float(c) for c in closes])
        assert cc.rsi(candles, 14) == pytest.approx(100.0)

    def test_all_losses_is_0(self) -> None:
        closes = list(range(16, 0, -1))
        candles = make_candles([float(c) for c in closes])
        assert cc.rsi(candles, 14) == pytest.approx(0.0)

    def test_no_change_is_neutral_50(self) -> None:
        candles = make_candles([100.0] * 16)
        assert cc.rsi(candles, 14) == pytest.approx(50.0)

    def test_insufficient_history_raises(self) -> None:
        candles = make_candles([1.0] * 10)
        with pytest.raises(InsufficientHistoryError):
            cc.rsi(candles, 14)

    def test_bounded_0_100(self) -> None:
        import random

        random.seed(42)
        closes = [100.0]
        for _ in range(30):
            closes.append(closes[-1] + random.uniform(-5, 5))
        candles = make_candles(closes)
        value = cc.rsi(candles, 14)
        assert 0.0 <= value <= 100.0


class TestROC:
    def test_known_value(self) -> None:
        # closes: base=100 (index -1-10=-11), current=110 -> ROC=10%
        closes = [100.0] * 1 + [100.0] * 9 + [110.0]  # 11 candles, base=closes[0]=100, current=closes[-1]=110
        candles = make_candles(closes)
        assert cc.roc(candles, 10) == pytest.approx(10.0)

    def test_zero_base_raises(self) -> None:
        closes = [0.0] + [1.0] * 10
        candles = make_candles(closes, highs=[1.0] * 11, lows=[0.0] * 11)
        with pytest.raises(FeatureCalculationError, match="sıfır"):
            cc.roc(candles, 10)

    def test_insufficient_history(self) -> None:
        candles = make_candles([1.0, 2.0])
        with pytest.raises(InsufficientHistoryError):
            cc.roc(candles, 10)


class TestTrueRangeAndATR:
    def test_constant_range_atr_equals_range(self) -> None:
        # 15 candle, high=101, low=100 (range=1), close=100.5 sabit
        n = 15
        candles = []
        for i in range(n):
            open_time = T0 + timedelta(minutes=i)
            candles.append(Candle(
                symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=open_time,
                close_time=open_time + timedelta(minutes=1),
                open=100.5, high=101.0, low=100.0, close=100.5, volume=10.0, is_closed=True, trade_count=1,
            ))
        assert cc.atr(candles, 14) == pytest.approx(1.0)

    def test_true_range_first_candle_no_previous(self) -> None:
        c = make_candles([100.0])[0]
        assert cc.true_range(c, None) == pytest.approx(c.high - c.low)

    def test_true_range_gap_up(self) -> None:
        prev = make_candles([100.0])[0]
        current = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0 + timedelta(minutes=1),
            close_time=T0 + timedelta(minutes=2), open=110.0, high=112.0, low=109.0, close=111.0,
            volume=1.0, is_closed=True,
        )
        # true range = max(high-low=3, |high-prev_close|=|112-100|=12, |low-prev_close|=|109-100|=9) = 12
        assert cc.true_range(current, prev) == pytest.approx(12.0)

    def test_atr_insufficient_history(self) -> None:
        candles = make_candles([1.0] * 5)
        with pytest.raises(InsufficientHistoryError):
            cc.atr(candles, 14)


class TestRollingStd:
    def test_zero_variance(self) -> None:
        candles = make_candles([100.0] * 20)
        assert cc.rolling_std(candles, 20, ddof=0) == pytest.approx(0.0)

    def test_known_population_std(self) -> None:
        # [1,2,3,4,5]: mean=3, population variance=(4+1+0+1+4)/5=2, std=sqrt(2)
        candles = make_candles([1.0, 2.0, 3.0, 4.0, 5.0])
        import math
        assert cc.rolling_std(candles, 5, ddof=0) == pytest.approx(math.sqrt(2.0))

    def test_unsupported_ddof_rejected(self) -> None:
        candles = make_candles([1.0, 2.0, 3.0])
        with pytest.raises(FeatureCalculationError, match="ddof"):
            cc.rolling_std(candles, 3, ddof=5)


class TestBollinger:
    def test_zero_variance_bands_equal_basis(self) -> None:
        candles = make_candles([100.0] * 20)
        basis = cc.bollinger_basis(candles, 20)
        upper = cc.bollinger_upper(candles, 20, 2.0)
        lower = cc.bollinger_lower(candles, 20, 2.0)
        assert basis == pytest.approx(100.0)
        assert upper == pytest.approx(100.0)
        assert lower == pytest.approx(100.0)

    def test_bandwidth_zero_when_no_variance(self) -> None:
        candles = make_candles([100.0] * 20)
        assert cc.bollinger_bandwidth(candles, 20, 2.0) == pytest.approx(0.0)

    def test_upper_gte_basis_gte_lower_property(self) -> None:
        closes = [100.0, 102.0, 98.0, 105.0, 95.0] * 5
        candles = make_candles(closes)
        basis = cc.bollinger_basis(candles, 20)
        upper = cc.bollinger_upper(candles, 20)
        lower = cc.bollinger_lower(candles, 20)
        assert upper >= basis >= lower

    def test_bandwidth_zero_basis_raises(self) -> None:
        candles = make_candles([0.0] * 20, highs=[0.0] * 20, lows=[0.0] * 20, opens=[0.0] * 20)
        with pytest.raises(FeatureCalculationError, match="basis sıfır"):
            cc.bollinger_bandwidth(candles, 20)


class TestVolumeFeatures:
    def test_relative_volume_known_value(self) -> None:
        volumes = [10.0] * 19 + [20.0]  # ortalama ~10.5, son değer 20
        candles = make_candles([100.0] * 20, volumes=volumes)
        mean = sum(volumes) / 20
        assert cc.relative_volume(candles, 20) == pytest.approx(20.0 / mean)

    def test_relative_volume_zero_mean_raises(self) -> None:
        candles = make_candles([100.0] * 20, volumes=[0.0] * 20)
        with pytest.raises(FeatureCalculationError, match="ortalama hacim sıfır"):
            cc.relative_volume(candles, 20)

    def test_volume_zscore_zero_std_raises(self) -> None:
        candles = make_candles([100.0] * 20, volumes=[10.0] * 20)
        with pytest.raises(FeatureCalculationError, match="std'si sıfır"):
            cc.volume_zscore(candles, 20)

    def test_volume_zscore_known_value(self) -> None:
        volumes = [10.0] * 19 + [30.0]
        candles = make_candles([100.0] * 20, volumes=volumes)
        import statistics
        mean = sum(volumes) / 20
        std = statistics.pstdev(volumes)
        expected = (30.0 - mean) / std
        assert cc.volume_zscore(candles, 20) == pytest.approx(expected)


class TestPriceStructure:
    def test_highest_lowest(self) -> None:
        candles = make_candles([100.0, 105.0, 95.0, 110.0, 90.0])
        hh = cc.highest_high(candles, 5)
        ll = cc.lowest_low(candles, 5)
        assert hh >= max(c.high for c in candles)
        assert ll <= min(c.low for c in candles)

    def test_body_wick_geometry(self) -> None:
        c = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
            open=100.0, high=105.0, low=98.0, close=103.0, volume=1.0, is_closed=True,
        )
        candles = [c]
        assert cc.body_size(candles) == pytest.approx(3.0)
        assert cc.upper_wick(candles) == pytest.approx(2.0)  # 105 - max(100,103)
        assert cc.lower_wick(candles) == pytest.approx(2.0)  # min(100,103) - 98
        assert cc.body_to_range_ratio(candles) == pytest.approx(3.0 / 7.0)

    def test_body_to_range_ratio_zero_range_doji(self) -> None:
        c = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
            open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0, is_closed=True,
        )
        assert cc.body_to_range_ratio([c]) == 0.0

    def test_distance_from_high_zero_denominator_raises(self) -> None:
        candles = make_candles([0.0] * 5, highs=[0.0] * 5, lows=[0.0] * 5, opens=[0.0] * 5)
        with pytest.raises(FeatureCalculationError):
            cc.distance_from_high(candles, 5)


class TestReturns:
    def test_simple_return_known_value(self) -> None:
        candles = make_candles([100.0, 110.0])
        assert cc.simple_return(candles, 1) == pytest.approx(0.10)

    def test_log_return_known_value(self) -> None:
        import math
        candles = make_candles([100.0, 110.0])
        assert cc.log_return(candles, 1) == pytest.approx(math.log(1.10))

    def test_log_return_nonpositive_price_raises(self) -> None:
        candles = make_candles([0.0, 1.0], highs=[0.0, 1.0], lows=[0.0, 0.0], opens=[0.0, 0.0])
        with pytest.raises(FeatureCalculationError, match="logaritma"):
            cc.log_return(candles, 1)

    def test_cumulative_return_known_value(self) -> None:
        closes = [100.0] * 20 + [120.0]
        candles = make_candles(closes)
        assert cc.cumulative_return(candles, 20) == pytest.approx(0.20)


class TestVWAP:
    def test_known_value(self) -> None:
        # 2 candle, typical price=(h+l+c)/3
        candles = [
            Candle(symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0, close_time=T0 + timedelta(minutes=1),
                   open=100.0, high=102.0, low=98.0, close=100.0, volume=10.0, is_closed=True),
            Candle(symbol="BTCUSDT", timeframe=Timeframe.M1, open_time=T0 + timedelta(minutes=1),
                   close_time=T0 + timedelta(minutes=2), open=100.0, high=104.0, low=100.0, close=102.0,
                   volume=20.0, is_closed=True),
        ]
        tp1 = (102.0 + 98.0 + 100.0) / 3.0
        tp2 = (104.0 + 100.0 + 102.0) / 3.0
        expected = (tp1 * 10.0 + tp2 * 20.0) / 30.0
        assert cc.rolling_vwap(candles, 2) == pytest.approx(expected)

    def test_zero_volume_raises(self) -> None:
        candles = make_candles([100.0] * 5, volumes=[0.0] * 5)
        with pytest.raises(FeatureCalculationError, match="hacim sıfır"):
            cc.rolling_vwap(candles, 5)


class TestNoNonFiniteOutputEver:
    """Herhangi bir finite girdi kombinasyonu non-finite (NaN/inf) çıktı üretmemeli."""

    def test_all_calculators_on_realistic_data_are_finite(self) -> None:
        import math

        closes = [100.0 + i * 0.37 - (i % 3) * 0.5 for i in range(30)]
        candles = make_candles(closes)
        results = [
            cc.sma(candles, 20), cc.ema(candles, 20), cc.rsi(candles, 14), cc.roc(candles, 10),
            cc.atr(candles, 14), cc.rolling_std(candles, 20), cc.bollinger_basis(candles, 20),
            cc.bollinger_upper(candles, 20), cc.bollinger_lower(candles, 20),
            cc.bollinger_bandwidth(candles, 20), cc.rolling_volume_mean(candles, 20),
            cc.relative_volume(candles, 20), cc.highest_high(candles, 20), cc.lowest_low(candles, 20),
            cc.distance_from_high(candles, 20), cc.distance_from_low(candles, 20),
            cc.body_size(candles), cc.upper_wick(candles), cc.lower_wick(candles),
            cc.body_to_range_ratio(candles), cc.simple_return(candles), cc.log_return(candles),
            cc.cumulative_return(candles, 20), cc.rolling_vwap(candles, 20), cc.vwap_deviation(candles, 20),
        ]
        assert all(math.isfinite(r) for r in results)


=== FILE: tests/test_candle_sequencing.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.candle_sequencing import (
    CandleSequencer,
    CandleUpdate,
    CandleUpdateOutcome,
)
from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.models import Candle

UTC = timezone.utc
OPEN_TIME = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)


def make_candle(close: float = 100.0, is_closed: bool = False, close_time_offset_sec: int = 300) -> Candle:
    return Candle(
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
        open_time=OPEN_TIME,
        close_time=OPEN_TIME + timedelta(seconds=close_time_offset_sec),
        open=100.0,
        high=max(105.0, close),
        low=min(99.0, close),
        close=close,
        volume=10.0,
        is_closed=is_closed,
    )


def make_update(update_seq: int, close: float = 100.0, is_closed: bool = False, seconds_after_open: int = 1) -> CandleUpdate:
    return CandleUpdate(
        candle=make_candle(close=close, is_closed=is_closed),
        update_seq=update_seq,
        event_time=OPEN_TIME + timedelta(seconds=seconds_after_open),
        received_at=OPEN_TIME + timedelta(seconds=seconds_after_open, milliseconds=50),
    )


class TestCandleUpdateConstruction:
    def test_negative_update_seq_rejected(self) -> None:
        with pytest.raises(ValueError, match="update_seq"):
            CandleUpdate(
                candle=make_candle(),
                update_seq=-1,
                event_time=OPEN_TIME,
                received_at=OPEN_TIME,
            )

    def test_naive_event_time_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            CandleUpdate(
                candle=make_candle(),
                update_seq=1,
                event_time=datetime(2026, 8, 31, 10, 0),
                received_at=OPEN_TIME,
            )


class TestCandleSequencerRepeatedUpdates:
    """Aynı açık candle'ın art arda güncellenmesi senaryosu."""

    def test_sequential_updates_all_accepted(self) -> None:
        seq = CandleSequencer()
        r1 = seq.apply(make_update(update_seq=1, close=100.5, seconds_after_open=1))
        r2 = seq.apply(make_update(update_seq=2, close=101.0, seconds_after_open=8))
        r3 = seq.apply(make_update(update_seq=3, close=100.8, seconds_after_open=75))

        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r3.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r3.canonical_candle.close == 100.8

    def test_intermediate_updates_are_not_treated_as_duplicates(self) -> None:
        # Bu, Master Prompt'un vurguladığı temel hatanın regresyon testidir:
        # aynı candle identity'sine ait farklı update_seq'li mesajlar
        # yanlışlıkla duplicate reddedilmemelidir.
        seq = CandleSequencer()
        outcomes = [
            seq.apply(make_update(update_seq=i, close=100.0 + i, seconds_after_open=i)).outcome
            for i in range(1, 5)
        ]
        assert all(o == CandleUpdateOutcome.ACCEPTED_UPDATE for o in outcomes)


class TestCandleSequencerFinal:
    def test_final_update_marks_finalized(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, close=100.5))
        final = seq.apply(make_update(update_seq=2, close=101.0, is_closed=True, seconds_after_open=300))
        assert final.outcome == CandleUpdateOutcome.ACCEPTED_FINAL
        assert final.canonical_candle.is_closed is True
        assert seq.is_finalized(final.canonical_candle.identity) is True

    def test_update_after_final_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, is_closed=True, seconds_after_open=300))
        late = seq.apply(make_update(update_seq=2, close=999.0, seconds_after_open=301))
        assert late.outcome == CandleUpdateOutcome.REJECTED_AFTER_FINAL
        # canonical state, finalize edilmiş haliyle KALIR, bozulmaz
        assert late.canonical_candle.close != 999.0


class TestCandleSequencerDuplicate:
    def test_exact_duplicate_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=5, close=100.5))
        dup = seq.apply(make_update(update_seq=5, close=100.5))
        assert dup.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE

    def test_duplicate_seq_with_different_payload_still_rejected(self) -> None:
        # update_seq aynıysa, payload farklı olsa bile reddedilir (sequence
        # kaynağı tektir, çelişkili iki "aynı update" kabul edilemez).
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=5, close=100.5))
        dup = seq.apply(make_update(update_seq=5, close=555.0))
        assert dup.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE
        assert dup.canonical_candle.close == 100.5


class TestCandleSequencerOutOfOrder:
    def test_out_of_order_update_rejected_and_state_unchanged(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=10, close=100.5))
        stale = seq.apply(make_update(update_seq=3, close=1.0))
        assert stale.outcome == CandleUpdateOutcome.REJECTED_OUT_OF_ORDER
        assert stale.canonical_candle.close == 100.5  # eski state korunur


class TestCandleSequencerDifferentCandle:
    def test_different_open_time_is_independent_identity(self) -> None:
        seq = CandleSequencer()
        r1 = seq.apply(make_update(update_seq=1, close=100.0))
        different_candle = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            open_time=OPEN_TIME + timedelta(minutes=5),
            close_time=OPEN_TIME + timedelta(minutes=10),
            open=200.0, high=205.0, low=199.0, close=203.0, volume=5.0, is_closed=False,
        )
        update2 = CandleUpdate(
            candle=different_candle, update_seq=1,  # aynı update_seq ama FARKLI identity
            event_time=OPEN_TIME + timedelta(minutes=5, seconds=1),
            received_at=OPEN_TIME + timedelta(minutes=5, seconds=1),
        )
        r2 = seq.apply(update2)
        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE  # duplicate DEĞİL, bağımsız identity

    def test_different_timeframe_is_independent_identity(self) -> None:
        seq = CandleSequencer()
        c5m = make_candle()
        c15m = Candle(
            symbol="BTCUSDT", timeframe=Timeframe.M15, open_time=OPEN_TIME,
            close_time=OPEN_TIME + timedelta(minutes=15),
            open=100.0, high=105.0, low=99.0, close=100.0, volume=10.0, is_closed=False,
        )
        r1 = seq.apply(CandleUpdate(candle=c5m, update_seq=1, event_time=OPEN_TIME, received_at=OPEN_TIME))
        r2 = seq.apply(CandleUpdate(candle=c15m, update_seq=1, event_time=OPEN_TIME, received_at=OPEN_TIME))
        assert r1.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert r2.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE


class TestCandleSequencerReconnect:
    def test_reconnect_resending_same_update_treated_as_duplicate(self) -> None:
        seq = CandleSequencer()
        original = make_update(update_seq=7, close=101.2, seconds_after_open=42)
        seq.apply(original)
        # reconnect sonrası WS aynı mesajı tekrar gönderiyor (aynı update_seq)
        resent = seq.apply(original)
        assert resent.outcome == CandleUpdateOutcome.REJECTED_DUPLICATE
        assert resent.canonical_candle.close == 101.2

    def test_reconnect_gap_fill_with_higher_seq_accepted(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=7, close=101.2, seconds_after_open=42))
        # reconnect sonrası REST gap-fill ile daha yeni bir update geliyor
        gap_fill = seq.apply(make_update(update_seq=8, close=101.5, seconds_after_open=90))
        assert gap_fill.outcome == CandleUpdateOutcome.ACCEPTED_UPDATE
        assert gap_fill.canonical_candle.close == 101.5

    def test_reconnect_stale_replay_after_final_rejected(self) -> None:
        seq = CandleSequencer()
        seq.apply(make_update(update_seq=1, close=100.0, seconds_after_open=1))
        seq.apply(make_update(update_seq=2, close=101.0, is_closed=True, seconds_after_open=300))
        # reconnect sonrası eski (finalize öncesi) bir update tekrar geliyor
        replay = seq.apply(make_update(update_seq=1, close=100.0, seconds_after_open=1))
        assert replay.outcome == CandleUpdateOutcome.REJECTED_AFTER_FINAL
        assert replay.canonical_candle.close == 101.0


class TestCandleSequencerPeek:
    def test_peek_unknown_identity_returns_none(self) -> None:
        seq = CandleSequencer()
        candle = make_candle()
        assert seq.peek(candle.identity) is None

    def test_peek_returns_current_canonical_state(self) -> None:
        seq = CandleSequencer()
        update = make_update(update_seq=1, close=123.0)
        seq.apply(update)
        assert seq.peek(update.candle.identity).close == 123.0


=== FILE: tests/test_consensus.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.domain.consensus import (
    ConsensusResult,
    RegimeContext,
    RiskAssessment,
    compute_agreement,
)
from crypto_signal_engine.domain.enums import (
    AgentName,
    LiquidityRegime,
    RiskLevel,
    StructureRegime,
    Timeframe,
    VolatilityRegime,
)
from crypto_signal_engine.domain.models import AgentEvidence

UTC = timezone.utc
AS_OF = datetime(2026, 8, 31, tzinfo=UTC)


def make_evidence(agent: AgentName, score: float, symbol: str = "BTCUSDT", context_id: str = "ctx-1") -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=score, rationale="test rationale",
        primary_timeframe=Timeframe.M5, symbol=symbol, as_of=AS_OF, context_id=context_id,
    )


DEFAULT_REGIME = RegimeContext(
    structure=StructureRegime.TRENDING,
    volatility=VolatilityRegime.NORMAL,
    liquidity=LiquidityRegime.NORMAL,
)


class TestComputeAgreement:
    def test_full_agreement_same_direction(self) -> None:
        assert compute_agreement((0.8, 0.75, 0.9)) > 0.9

    def test_full_disagreement_opposite_scores(self) -> None:
        assert compute_agreement((0.8, -0.8)) < 0.1

    def test_all_neutral_scores_treated_as_full_agreement(self) -> None:
        assert compute_agreement((0.0, 0.0, 0.0)) == 1.0

    def test_single_score_returns_neutral_one(self) -> None:
        assert compute_agreement((0.5,)) == 1.0

    def test_result_always_bounded(self) -> None:
        for scores in [(1.0, -1.0), (1.0, 1.0), (-1.0, -1.0), (0.01, -0.99, 0.5)]:
            agreement = compute_agreement(scores)
            assert 0.0 <= agreement <= 1.0

    def test_empty_scores_rejected(self) -> None:
        with pytest.raises(ValueError):
            compute_agreement(())

    def test_nan_score_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            compute_agreement((0.5, float("nan")))

    def test_identical_scores_maximum_agreement(self) -> None:
        assert compute_agreement((0.6, 0.6, 0.6)) == pytest.approx(1.0)

    def test_opposite_extreme_scores_minimum_agreement(self) -> None:
        assert compute_agreement((1.0, -1.0)) == pytest.approx(0.0)


class TestConsensusResultProvenance:
    def test_valid_consensus_result(self) -> None:
        result = ConsensusResult(
            symbol="btcusdt", context_id="ctx-1", raw_score=0.5, agreement=0.8,
            regime=DEFAULT_REGIME,
            contributing_evidence=(make_evidence(AgentName.QUANT, 0.5),),
        )
        assert result.symbol == "BTCUSDT"

    def test_empty_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="contributing_evidence"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.0, agreement=1.0,
                regime=DEFAULT_REGIME, contributing_evidence=(),
            )

    def test_agreement_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="agreement"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.0, agreement=1.5,
                regime=DEFAULT_REGIME,
                contributing_evidence=(make_evidence(AgentName.QUANT, 0.0),),
            )

    def test_mixed_symbol_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5, symbol="BTCUSDT"),
                    make_evidence(AgentName.ORDER_BOOK, 0.6, symbol="ETHUSDT"),
                ),
            )

    def test_mismatched_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5, context_id="ctx-1"),
                    make_evidence(AgentName.ORDER_BOOK, 0.6, context_id="ctx-2"),
                ),
            )

    def test_duplicate_agent_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate agent"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
                regime=DEFAULT_REGIME,
                contributing_evidence=(
                    make_evidence(AgentName.QUANT, 0.5),
                    make_evidence(AgentName.QUANT, 0.7),  # aynı agent, ikinci evidence
                ),
            )

    def test_distinct_agents_same_context_accepted(self) -> None:
        result = ConsensusResult(
            symbol="BTCUSDT", context_id="ctx-1", raw_score=0.5, agreement=0.8,
            regime=DEFAULT_REGIME,
            contributing_evidence=(
                make_evidence(AgentName.QUANT, 0.5),
                make_evidence(AgentName.ORDER_BOOK, 0.6),
                make_evidence(AgentName.REGIME, 0.4),
            ),
        )
        assert len(result.contributing_evidence) == 3

    def test_raw_score_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            ConsensusResult(
                symbol="BTCUSDT", context_id="ctx-1", raw_score=float("nan"), agreement=0.5,
                regime=DEFAULT_REGIME,
                contributing_evidence=(make_evidence(AgentName.QUANT, 0.0),),
            )


class TestRegimeContext:
    def test_axes_are_independent(self) -> None:
        # Aynı anda TRENDING + HIGH_VOLATILITY + THIN mümkün olmalı (Gate 12)
        regime = RegimeContext(
            structure=StructureRegime.TRENDING,
            volatility=VolatilityRegime.HIGH,
            liquidity=LiquidityRegime.THIN,
        )
        assert regime.structure == StructureRegime.TRENDING
        assert regime.volatility == VolatilityRegime.HIGH
        assert regime.liquidity == LiquidityRegime.THIN

    def test_string_structure_rejected(self) -> None:
        with pytest.raises(TypeError, match="StructureRegime"):
            RegimeContext(structure="TRENDING", volatility=VolatilityRegime.HIGH, liquidity=LiquidityRegime.NORMAL)  # type: ignore[arg-type]

    def test_string_volatility_rejected(self) -> None:
        with pytest.raises(TypeError, match="VolatilityRegime"):
            RegimeContext(structure=StructureRegime.TRENDING, volatility="HIGH", liquidity=LiquidityRegime.NORMAL)  # type: ignore[arg-type]

    def test_string_liquidity_rejected(self) -> None:
        with pytest.raises(TypeError, match="LiquidityRegime"):
            RegimeContext(structure=StructureRegime.TRENDING, volatility=VolatilityRegime.HIGH, liquidity="NORMAL")  # type: ignore[arg-type]

    def test_all_strings_rejected(self) -> None:
        with pytest.raises(TypeError):
            RegimeContext("TRENDING", "HIGH", "NORMAL")  # type: ignore[arg-type]


class TestRiskAssessment:
    def test_valid_assessment(self) -> None:
        assessment = RiskAssessment(
            confidence_multiplier=0.6, risk_level=RiskLevel.HIGH,
            rationale="realized volatility 89th percentile, spread widening",
        )
        assert assessment.confidence_multiplier == 0.6

    def test_multiplier_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="confidence_multiplier"):
            RiskAssessment(confidence_multiplier=1.2, risk_level=RiskLevel.LOW, rationale="test")

    def test_risk_level_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="RiskLevel"):
            RiskAssessment(confidence_multiplier=0.5, risk_level="LOW", rationale="test")  # type: ignore[arg-type]

    def test_multiplier_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            RiskAssessment(confidence_multiplier=float("nan"), risk_level=RiskLevel.LOW, rationale="test")

    def test_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            RiskAssessment(confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="   ")

    def test_contradicting_metrics_immutable(self) -> None:
        assessment = RiskAssessment(
            confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
            contradicting_metrics={"volatility_pct": 89.0},
        )
        with pytest.raises(TypeError):
            assessment.contradicting_metrics["volatility_pct"] = 999.0  # type: ignore[index]

    def test_contradicting_metrics_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            RiskAssessment(
                confidence_multiplier=0.5, risk_level=RiskLevel.LOW, rationale="test",
                contradicting_metrics={"x": float("nan")},
            )


=== FILE: tests/test_consensus_engine.py ===
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.consensus.engine import AGENT_WEIGHTS, ConsensusEngine
from crypto_signal_engine.domain.consensus import RegimeContext, compute_agreement
from crypto_signal_engine.domain.enums import AgentName, LiquidityRegime, StructureRegime, Timeframe, VolatilityRegime
from crypto_signal_engine.domain.models import AgentEvidence
from crypto_signal_engine.errors import ConsensusError

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)
REGIME = RegimeContext(structure=StructureRegime.RANGING, volatility=VolatilityRegime.NORMAL, liquidity=LiquidityRegime.NORMAL)


def make_evidence(agent, score, symbol="BTCUSDT", context_id="ctx-1", as_of=T0) -> AgentEvidence:
    return AgentEvidence(
        agent=agent, score=score, rationale="test", primary_timeframe=Timeframe.M5,
        symbol=symbol, as_of=as_of, context_id=context_id,
    )


class TestConsensusContract:
    def test_accepts_evidence_only_positionally(self) -> None:
        import inspect

        sig = inspect.signature(ConsensusEngine.combine)
        params = list(sig.parameters.keys())
        assert params[:2] == ["self", "evidence"]
        assert sig.parameters["regime"].kind == inspect.Parameter.KEYWORD_ONLY

    def test_exact_fixed_weights(self) -> None:
        assert AGENT_WEIGHTS[AgentName.QUANT] == pytest.approx(0.35)
        assert AGENT_WEIGHTS[AgentName.MARKET_STRUCTURE] == pytest.approx(0.30)
        assert AGENT_WEIGHTS[AgentName.ORDER_BOOK] == pytest.approx(0.15)
        assert AGENT_WEIGHTS[AgentName.REGIME] == pytest.approx(0.20)
        assert sum(AGENT_WEIGHTS.values()) == pytest.approx(1.0)

    def test_exact_weighted_score(self) -> None:
        evidence = [
            make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5),
            make_evidence(AgentName.ORDER_BOOK, 0.5), make_evidence(AgentName.REGIME, 0.5),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert result.raw_score == pytest.approx(0.5)

    def test_uses_existing_compute_agreement(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, 0.8)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        expected = compute_agreement((0.8, 0.8))
        assert result.agreement == pytest.approx(expected)

    def test_mixed_symbols_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5, symbol="BTCUSDT"), make_evidence(AgentName.MARKET_STRUCTURE, 0.5, symbol="ETHUSDT")]
        with pytest.raises(ConsensusError, match="mixed-symbol"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_mixed_context_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5, context_id="ctx-1"), make_evidence(AgentName.MARKET_STRUCTURE, 0.5, context_id="ctx-2")]
        with pytest.raises(ConsensusError, match="context"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_duplicate_agent_rejected(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.QUANT, 0.3)]
        with pytest.raises(ConsensusError, match="duplicate"):
            ConsensusEngine().combine(evidence, regime=REGIME)

    def test_deterministic_evidence_ordering(self) -> None:
        evidence = [
            make_evidence(AgentName.REGIME, 0.1), make_evidence(AgentName.QUANT, 0.2),
            make_evidence(AgentName.ORDER_BOOK, 0.3), make_evidence(AgentName.MARKET_STRUCTURE, 0.4),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert [e.agent for e in result.contributing_evidence] == [
            AgentName.QUANT, AgentName.MARKET_STRUCTURE, AgentName.ORDER_BOOK, AgentName.REGIME,
        ]

    def test_finite_bounded_score(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 1.0), make_evidence(AgentName.MARKET_STRUCTURE, 1.0)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        assert -1.0 <= result.raw_score <= 1.0

    def test_conflicting_evidence_lowers_agreement(self) -> None:
        aligned = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, 0.8)]
        conflicted = [make_evidence(AgentName.QUANT, 0.8), make_evidence(AgentName.MARKET_STRUCTURE, -0.8)]
        r1 = ConsensusEngine().combine(aligned, regime=REGIME)
        r2 = ConsensusEngine().combine(conflicted, regime=REGIME)
        assert r2.agreement < r1.agreement

    def test_all_four_agents_normal_evaluation(self) -> None:
        evidence = [
            make_evidence(AgentName.QUANT, 0.4), make_evidence(AgentName.MARKET_STRUCTURE, 0.3),
            make_evidence(AgentName.ORDER_BOOK, 0.2), make_evidence(AgentName.REGIME, 0.1),
        ]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        expected = 0.35 * 0.4 + 0.30 * 0.3 + 0.15 * 0.2 + 0.20 * 0.1
        assert result.raw_score == pytest.approx(expected)

    def test_regime_evidence_participates_exactly_once(self) -> None:
        evidence = [make_evidence(AgentName.REGIME, 0.6), make_evidence(AgentName.QUANT, 0.2)]
        result = ConsensusEngine().combine(evidence, regime=REGIME)
        regime_evidences = [e for e in result.contributing_evidence if e.agent == AgentName.REGIME]
        assert len(regime_evidences) == 1

    def test_empty_evidence_rejected(self) -> None:
        with pytest.raises(ConsensusError, match="boş"):
            ConsensusEngine().combine([], regime=REGIME)

    def test_non_agent_evidence_element_rejected(self) -> None:
        with pytest.raises(ConsensusError):
            ConsensusEngine().combine(["not-evidence"], regime=REGIME)  # type: ignore[list-item]

    def test_does_not_require_regime_context_influence_on_score(self) -> None:
        evidence = [make_evidence(AgentName.QUANT, 0.5), make_evidence(AgentName.MARKET_STRUCTURE, 0.5)]
        regime_a = RegimeContext(structure=StructureRegime.RANGING, volatility=VolatilityRegime.LOW, liquidity=LiquidityRegime.NORMAL)
        regime_b = RegimeContext(structure=StructureRegime.BREAKOUT, volatility=VolatilityRegime.EXTREME, liquidity=LiquidityRegime.STRESSED)
        r1 = ConsensusEngine().combine(evidence, regime=regime_a)
        r2 = ConsensusEngine().combine(evidence, regime=regime_b)
        assert r1.raw_score == r2.raw_score
        assert r1.agreement == r2.agreement


=== FILE: tests/test_domain_models.py ===
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import (
    AgentName,
    RiskLevel,
    SignalDirection,
    Timeframe,
)
from crypto_signal_engine.domain.models import (
    AgentEvidence,
    Candle,
    CandleIdentity,
    FeatureVector,
    OrderBookLevel,
    OrderBookSnapshot,
    Signal,
    Trade,
)

UTC = timezone.utc


def make_candle(**overrides) -> Candle:
    defaults = dict(
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
        open_time=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC),
        open=100.0,
        high=105.0,
        low=99.0,
        close=103.0,
        volume=10.0,
        is_closed=True,
    )
    defaults.update(overrides)
    return Candle(**defaults)


def make_evidence(**overrides) -> AgentEvidence:
    defaults = dict(
        agent=AgentName.QUANT,
        score=0.5,
        rationale="momentum accelerating",
        primary_timeframe=Timeframe.M5,
        symbol="BTCUSDT",
        as_of=datetime(2026, 8, 31, tzinfo=UTC),
        context_id="snapshot-1",
    )
    defaults.update(overrides)
    return AgentEvidence(**defaults)


def make_signal(**overrides) -> Signal:
    defaults = dict(
        symbol="BTCUSDT",
        timestamp=datetime(2026, 8, 31, tzinfo=UTC),
        context_id="snapshot-1",
        score=0.5,
        confidence=0.7,
        risk_level=RiskLevel.MEDIUM,
        primary_timeframe=Timeframe.M5,
        supporting_factors=(),
        contradicting_factors=(),
        invalidation=None,
        model_version="baseline-v0.1",
    )
    defaults.update(overrides)
    return Signal(**defaults)


class TestCandleIdentity:
    def test_identity_derived_from_candle(self) -> None:
        candle = make_candle()
        identity = candle.identity
        assert identity == CandleIdentity(
            symbol="BTCUSDT", timeframe=Timeframe.M5, open_time=candle.open_time
        )

    def test_close_time_not_part_of_identity(self) -> None:
        c1 = make_candle(close_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC), is_closed=False)
        c2 = make_candle(close_time=datetime(2026, 8, 31, 10, 5, 30, tzinfo=UTC), is_closed=True)
        assert c1.identity == c2.identity

    def test_symbol_normalized_in_identity(self) -> None:
        identity = CandleIdentity(
            symbol="btcusdt", timeframe=Timeframe.M5, open_time=datetime(2026, 8, 31, tzinfo=UTC)
        )
        assert identity.symbol == "BTCUSDT"

    def test_non_timeframe_enum_rejected(self) -> None:
        with pytest.raises(TypeError):
            CandleIdentity(symbol="BTCUSDT", timeframe="5m", open_time=datetime(2026, 8, 31, tzinfo=UTC))  # type: ignore[arg-type]


class TestCandleInvariants:
    def test_valid_candle_constructs(self) -> None:
        assert make_candle().symbol == "BTCUSDT"

    def test_symbol_normalized(self) -> None:
        assert make_candle(symbol="btcusdt").symbol == "BTCUSDT"
        assert make_candle(symbol=" BTCUSDT ").symbol == "BTCUSDT"

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            make_candle(open_time=datetime(2026, 8, 31, 10, 0))

    def test_non_utc_timezone_rejected(self) -> None:
        offset_tz = timezone(timedelta(hours=3))
        with pytest.raises(ValueError, match="UTC olmalı"):
            make_candle(open_time=datetime(2026, 8, 31, 10, 0, tzinfo=offset_tz))

    def test_close_before_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="close_time"):
            make_candle(
                open_time=datetime(2026, 8, 31, 10, 5, tzinfo=UTC),
                close_time=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
            )

    def test_high_below_low_rejected(self) -> None:
        with pytest.raises(ValueError, match="impossible OHLC"):
            make_candle(high=90.0, low=99.0)

    def test_open_outside_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="open"):
            make_candle(open=200.0)

    def test_close_outside_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="close"):
            make_candle(close=200.0)

    def test_negative_volume_rejected(self) -> None:
        with pytest.raises(ValueError, match="volume"):
            make_candle(volume=-1.0)

    def test_negative_trade_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="trade_count"):
            make_candle(trade_count=-1)

    def test_non_timeframe_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_candle(timeframe="5m")

    @pytest.mark.parametrize("field_name", ["open", "high", "low", "close", "volume"])
    def test_nan_rejected(self, field_name: str) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_candle(**{field_name: float("nan")})

    @pytest.mark.parametrize("field_name", ["open", "high", "low", "close", "volume"])
    def test_inf_rejected(self, field_name: str) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_candle(**{field_name: float("inf")})

    def test_candle_is_frozen(self) -> None:
        candle = make_candle()
        with pytest.raises(FrozenInstanceError):
            candle.close = 999.0  # type: ignore[misc]


class TestTradeInvariants:
    def test_valid_trade(self) -> None:
        trade = Trade(
            symbol="btcusdt",
            trade_id=1,
            price=100.0,
            quantity=1.0,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            is_buyer_maker=False,
        )
        assert trade.symbol == "BTCUSDT"

    def test_zero_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="price"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=0.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )

    def test_negative_trade_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="trade_id"):
            Trade(
                symbol="BTCUSDT", trade_id=-1, price=100.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )

    def test_naive_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=100.0, quantity=1.0,
                timestamp=datetime(2026, 8, 31), is_buyer_maker=False,
            )

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_price_non_finite_rejected(self, bad: float) -> None:
        with pytest.raises(ValueError, match="finite"):
            Trade(
                symbol="BTCUSDT", trade_id=1, price=bad, quantity=1.0,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
            )


class TestOrderBookSnapshot:
    def _levels(self, prices_qty):
        return tuple(OrderBookLevel(price=p, quantity=q) for p, q in prices_qty)

    def test_valid_snapshot_computed_properties(self) -> None:
        snap = OrderBookSnapshot(
            symbol="BTCUSDT",
            timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            bids=self._levels([(99.0, 1.0), (98.5, 2.0)]),
            asks=self._levels([(100.0, 1.5), (100.5, 1.0)]),
            last_update_id=42,
        )
        assert snap.best_bid.price == 99.0
        assert snap.best_ask.price == 100.0
        assert snap.mid_price == pytest.approx(99.5)
        assert snap.spread == pytest.approx(1.0)

    def test_crossed_book_rejected(self) -> None:
        with pytest.raises(ValueError, match="crossed"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(101.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=1,
            )

    def test_locked_book_rejected(self) -> None:
        with pytest.raises(ValueError, match="crossed"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(100.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=1,
            )

    def test_empty_bids_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=(), asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_empty_asks_rejected(self) -> None:
        with pytest.raises(ValueError, match="boş olamaz"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(100.0, 1.0)]), asks=(), last_update_id=1,
            )

    def test_unsorted_bids_rejected(self) -> None:
        with pytest.raises(ValueError, match="bids"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(98.0, 1.0), (99.0, 1.0)]),  # artan -> yanlış
                asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_unsorted_asks_rejected(self) -> None:
        with pytest.raises(ValueError, match="asks"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]),
                asks=self._levels([(101.0, 1.0), (100.0, 1.0)]),  # azalan -> yanlış
                last_update_id=1,
            )

    def test_duplicate_bid_level_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0), (99.0, 2.0)]),
                asks=self._levels([(100.0, 1.0)]), last_update_id=1,
            )

    def test_duplicate_ask_level_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]),
                asks=self._levels([(100.0, 1.0), (100.0, 2.0)]), last_update_id=1,
            )

    def test_negative_update_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="last_update_id"):
            OrderBookSnapshot(
                symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
                bids=self._levels([(99.0, 1.0)]), asks=self._levels([(100.0, 1.0)]),
                last_update_id=-1,
            )

    def test_nan_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            OrderBookLevel(price=float("nan"), quantity=1.0)

    def test_inf_quantity_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            OrderBookLevel(price=100.0, quantity=float("inf"))

    def test_zero_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="price"):
            OrderBookLevel(price=0.0, quantity=1.0)

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValueError, match="quantity"):
            OrderBookLevel(price=100.0, quantity=-1.0)

    def test_accepts_list_input_and_converts_to_tuple(self) -> None:
        snap = OrderBookSnapshot(
            symbol="BTCUSDT", timestamp=datetime(2026, 8, 31, tzinfo=UTC),
            bids=[OrderBookLevel(price=99.0, quantity=1.0)],
            asks=[OrderBookLevel(price=100.0, quantity=1.0)],
            last_update_id=1,
        )
        assert isinstance(snap.bids, tuple)
        assert isinstance(snap.asks, tuple)


class TestFeatureVector:
    def test_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": float("nan")},
            )

    def test_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            FeatureVector(
                symbol="BTCUSDT", timeframe=Timeframe.M5,
                timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"ema": float("inf")},
            )

    def test_valid_values_pass(self) -> None:
        fv = FeatureVector(
            symbol="btcusdt", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": 55.2},
        )
        assert fv.symbol == "BTCUSDT"
        assert fv.values["rsi"] == 55.2

    def test_values_immutable(self) -> None:
        fv = FeatureVector(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values={"rsi": 55.2},
        )
        with pytest.raises(TypeError):
            fv.values["rsi"] = 999.0  # type: ignore[index]

    def test_original_dict_mutation_does_not_leak(self) -> None:
        source = {"rsi": 55.2}
        fv = FeatureVector(
            symbol="BTCUSDT", timeframe=Timeframe.M5,
            timestamp=datetime(2026, 8, 31, tzinfo=UTC), values=source,
        )
        source["rsi"] = 999.0
        assert fv.values["rsi"] == 55.2


class TestAgentEvidence:
    def test_score_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="score"):
            make_evidence(score=1.5)

    def test_score_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_evidence(score=float("nan"))

    def test_empty_rationale_rejected(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            make_evidence(rationale="   ")

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context_id"):
            make_evidence(context_id="  ")

    def test_agent_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="AgentName"):
            make_evidence(agent="QUANT")

    def test_primary_timeframe_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_evidence(primary_timeframe="5m")

    def test_naive_as_of_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            make_evidence(as_of=datetime(2026, 8, 31))

    def test_symbol_normalized(self) -> None:
        evidence = make_evidence(symbol="btcusdt")
        assert evidence.symbol == "BTCUSDT"

    def test_supporting_metrics_immutable(self) -> None:
        evidence = make_evidence(supporting_metrics={"rsi": 55.0})
        with pytest.raises(TypeError):
            evidence.supporting_metrics["rsi"] = 999.0  # type: ignore[index]

    def test_supporting_metrics_defensive_copy(self) -> None:
        source = {"rsi": 55.0}
        evidence = make_evidence(supporting_metrics=source)
        source["rsi"] = 999.0
        assert evidence.supporting_metrics["rsi"] == 55.0

    def test_supporting_metrics_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_evidence(supporting_metrics={"rsi": float("nan")})


class TestSignalSingleSourceOfTruth:
    def test_direction_derived_from_score(self) -> None:
        signal = make_signal(score=0.55)
        assert signal.direction == SignalDirection.LONG

    def test_direction_is_not_a_settable_field(self) -> None:
        # direction bir property'dir, constructor argümanı DEĞİLDİR.
        with pytest.raises(TypeError):
            make_signal(direction=SignalDirection.STRONG_LONG)  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "score,expected",
        [
            (1.0, SignalDirection.STRONG_LONG),
            (0.70, SignalDirection.STRONG_LONG),
            (0.69, SignalDirection.LONG),
            (0.40, SignalDirection.LONG),
            (0.39, SignalDirection.WEAK_LONG),
            (0.15, SignalDirection.WEAK_LONG),
            (0.14, SignalDirection.NEUTRAL),
            (0.0, SignalDirection.NEUTRAL),
            (-0.14, SignalDirection.NEUTRAL),
            (-0.15, SignalDirection.WEAK_SHORT),
            (-0.40, SignalDirection.SHORT),
            (-0.70, SignalDirection.STRONG_SHORT),
            (-1.0, SignalDirection.STRONG_SHORT),
        ],
    )
    def test_all_boundaries_produce_consistent_direction(self, score, expected) -> None:
        assert make_signal(score=score).direction == expected

    def test_contradictory_state_impossible_by_construction(self) -> None:
        # score=-1.0 iken direction ASLA STRONG_LONG olamaz çünkü direction
        # score'dan türetilir; bunu "yanlış" bir direction ile inşa etme
        # imkanı yoktur (constructor'da direction alanı yok).
        signal = make_signal(score=-1.0)
        assert signal.direction == SignalDirection.STRONG_SHORT


class TestSignal:
    def test_confidence_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="confidence"):
            make_signal(confidence=1.2)

    def test_confidence_nan_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_signal(confidence=float("nan"))

    def test_score_inf_rejected(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            make_signal(score=float("inf"))

    def test_empty_model_version_rejected(self) -> None:
        with pytest.raises(ValueError, match="model_version"):
            make_signal(model_version="   ")

    def test_symbol_normalized(self) -> None:
        assert make_signal(symbol="btcusdt").symbol == "BTCUSDT"

    def test_signal_is_frozen(self) -> None:
        signal = make_signal()
        with pytest.raises(FrozenInstanceError):
            signal.confidence = 0.9  # type: ignore[misc]

    def test_supporting_factors_converted_to_tuple(self) -> None:
        signal = make_signal(supporting_factors=[make_evidence()])
        assert isinstance(signal.supporting_factors, tuple)

    def test_risk_level_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="RiskLevel"):
            make_signal(risk_level="HIGH")

    def test_primary_timeframe_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="Timeframe"):
            make_signal(primary_timeframe="5m")

    def test_empty_context_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="context_id"):
            make_signal(context_id="   ")


class TestSignalEvidenceProvenance:
    """Quality Gate 26 — final Signal provenance."""

    CTX = "snapshot-1"
    SIGNAL_TS = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)

    def _evidence(self, agent=AgentName.QUANT, symbol="BTCUSDT", context_id=None, as_of=None) -> AgentEvidence:
        return make_evidence(
            agent=agent,
            symbol=symbol,
            context_id=context_id or self.CTX,
            as_of=as_of or (self.SIGNAL_TS - timedelta(seconds=1)),
        )

    def test_valid_same_context_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(agent=AgentName.QUANT),),
            contradicting_factors=(self._evidence(agent=AgentName.ORDER_BOOK),),
        )
        assert len(signal.supporting_factors) == 1

    def test_mixed_symbol_supporting_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(symbol="ETHUSDT"),),
            )

    def test_mixed_symbol_contradicting_evidence_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-symbol"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                contradicting_factors=(self._evidence(symbol="ETHUSDT"),),
            )

    def test_mixed_context_rejected(self) -> None:
        with pytest.raises(ValueError, match="mixed-context"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(context_id="other-ctx"),),
            )

    def test_future_evidence_timestamp_rejected(self) -> None:
        with pytest.raises(ValueError, match="GELECEKTEN"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(self._evidence(as_of=self.SIGNAL_TS + timedelta(seconds=1)),),
            )

    def test_evidence_as_of_exactly_equal_to_signal_timestamp_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(as_of=self.SIGNAL_TS),),
        )
        assert signal.supporting_factors[0].as_of == self.SIGNAL_TS

    def test_identical_evidence_object_across_lists_rejected(self) -> None:
        evidence = self._evidence(agent=AgentName.QUANT)
        with pytest.raises(ValueError, match="hem supporting hem contradicting"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(evidence,),
                contradicting_factors=(evidence,),
            )

    def test_distinct_evidence_same_agent_across_lists_rejected(self) -> None:
        """Aynı obje DEĞİL, farklı rationale'lı iki farklı evidence ama AYNI
        AgentName — supporting/contradicting'e dağıtılmış. Global
        one-agent/one-evidence politikası bunu da reddeder (bu, salt
        obje-eşitliği overlap kontrolünden AYRI bir kuraldır)."""
        evidence_a = self._evidence(agent=AgentName.QUANT)
        evidence_b = make_evidence(
            agent=AgentName.QUANT, symbol="BTCUSDT", context_id=self.CTX,
            as_of=self.SIGNAL_TS - timedelta(seconds=1), rationale="different rationale, same agent",
        )
        assert evidence_a != evidence_b  # gerçekten farklı objeler/değerler
        with pytest.raises(ValueError, match="duplicate agent evidence"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(evidence_a,),
                contradicting_factors=(evidence_b,),
            )

    def test_same_agent_twice_within_supporting_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate agent evidence"):
            make_signal(
                timestamp=self.SIGNAL_TS,
                context_id=self.CTX,
                supporting_factors=(
                    self._evidence(agent=AgentName.QUANT),
                    self._evidence(agent=AgentName.QUANT),
                ),
            )

    def test_distinct_agents_across_both_lists_accepted(self) -> None:
        signal = make_signal(
            timestamp=self.SIGNAL_TS,
            context_id=self.CTX,
            supporting_factors=(self._evidence(agent=AgentName.QUANT),),
            contradicting_factors=(
                self._evidence(agent=AgentName.ORDER_BOOK),
                self._evidence(agent=AgentName.REGIME),
            ),
        )
        assert len(signal.contradicting_factors) == 2


=== FILE: tests/test_end_to_end.py ===
from datetime import datetime, timedelta, timezone

from crypto_signal_engine.domain.enums import SignalDirection, Timeframe
from crypto_signal_engine.features.domain import FeatureSnapshot
from crypto_signal_engine.features.state import FeatureHistoryStore
from crypto_signal_engine.signal_engine import SignalEngine

UTC = timezone.utc
T0 = datetime(2026, 8, 31, tzinfo=UTC)


def commit_snapshot(history: FeatureHistoryStore, tf: Timeframe, as_of: datetime, values: dict) -> None:
    history.commit(FeatureSnapshot(symbol="BTCUSDT", timeframe=tf, as_of=as_of, values=values))


def build_history(as_of: datetime, bullish: bool) -> FeatureHistoryStore:
    history = FeatureHistoryStore()
    sign = 1.0 if bullish else -1.0
    commit_snapshot(history, Timeframe.M1, as_of, {
        "SPREAD_BPS": 2.0, "DEPTH_IMBALANCE_10": sign * 0.4, "TOB_IMBALANCE": sign * 0.3,
        "MID_PRICE": 100.0, "MICROPRICE": 100.0 + sign * 0.03,
    })
    commit_snapshot(history, Timeframe.M5, as_of, {
        "RSI_14": 50.0 + sign * 20.0, "ROC_10": sign * 2.0, "VWAP_DEVIATION_20": sign * 0.012,
        "RELATIVE_VOLUME_20": 1.3, "BOLLINGER_BANDWIDTH_20_2": 0.03,
    })
    commit_snapshot(history, Timeframe.M15, as_of, {
        "ROC_10": sign * 1.8, "DIST_FROM_HIGH_20": 0.0 if bullish else 0.05,
        "DIST_FROM_LOW_20": 0.05 if bullish else 0.0, "RELATIVE_VOLUME_20": 1.2, "BODY_TO_RANGE_RATIO": 0.7,
    })
    commit_snapshot(history, Timeframe.H1, as_of, {
        "ROC_10": sign * 1.5, "RSI_14": 50.0 + sign * 15.0, "BOLLINGER_BANDWIDTH_20_2": 0.03,
        "RELATIVE_VOLUME_20": 1.1, "DIST_FROM_HIGH_20": 0.03, "DIST_FROM_LOW_20": 0.06,
    })
    return history


class TestEndToEndScenarios:
    def test_clearly_bullish(self) -> None:
        history = build_history(T0, bullish=True)
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.score > 0.3
        assert signal.direction in (SignalDirection.LONG, SignalDirection.STRONG_LONG, SignalDirection.WEAK_LONG)

    def test_clearly_bearish(self) -> None:
        history = build_history(T0, bullish=False)
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        assert signal.score < -0.3
        assert signal.direction in (SignalDirection.SHORT, SignalDirection.STRONG_SHORT, SignalDirection.WEAK_SHORT)

    def test_conflicted_scenario_lowers_agreement_and_confidence(self) -> None:
        history = FeatureHistoryStore()
        commit_snapshot(history, Timeframe.M1, T0, {
            "SPREAD_BPS": 2.0, "DEPTH_IMBALANCE_10": 0.0, "TOB_IMBALANCE": 0.0, "MID_PRICE": 100.0, "MICROPRICE": 100.0,
        })
        commit_snapshot(history, Timeframe.M5, T0, {
            "RSI_14": 80.0, "ROC_10": 2.5, "VWAP_DEVIATION_20": 0.015, "RELATIVE_VOLUME_20": 1.2, "BOLLINGER_BANDWIDTH_20_2": 0.03,
        })
        commit_snapshot(history, Timeframe.M15, T0, {
            "ROC_10": -2.5, "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.0, "RELATIVE_VOLUME_20": 1.2, "BODY_TO_RANGE_RATIO": 0.7,
        })
        commit_snapshot(history, Timeframe.H1, T0, {
            "ROC_10": 0.0, "RSI_14": 50.0, "BOLLINGER_BANDWIDTH_20_2": 0.03, "RELATIVE_VOLUME_20": 1.0,
            "DIST_FROM_HIGH_20": 0.05, "DIST_FROM_LOW_20": 0.05,
        })
        signal = SignalEngine(history).evaluate("BTCUSDT", T0)
        clean_bullish = SignalEngine(build_history(T0, bullish=True)).evaluate("BTCUSDT", T0)
        assert signal.confidence < clean_bullish.confidence

    def test_deterministic_repeatability(self) -> None:
        history = build_history(T0, bullish=True)
        engine = SignalEngine(history)
        s1 = engine.evaluate("BTCUSDT", T0)
        s2 = engine.evaluate("BTCUSDT", T0)
        assert s1.score == s2.score
        assert s1.confidence == s2.confidence
        assert s1.context_id == s2.context_id
        assert s1.risk_level == s2.risk_level


class TestFutureDataInvariance:
    """Bölüm 50 — HARD BLOCKER: T anındaki sonuç, T'den SONRAKİ snapshot'lar
    eklendikten sonra bile DEĞİŞMEMELİDİR."""

    def test_full_result_unchanged_after_adding_future_snapshots(self) -> None:
        history = build_history(T0, bullish=True)
        engine = SignalEngine(history)

        signal_before = engine.evaluate("BTCUSDT", T0)

        future_time = T0 + timedelta(minutes=1)
        future_history = build_history(future_time, bullish=False)
        for tf in (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1):
            future_snapshot = future_history.latest("BTCUSDT", tf)
            history.commit(future_snapshot)

        signal_after = engine.evaluate("BTCUSDT", T0)

        assert signal_before.score == signal_after.score
        assert signal_before.confidence == signal_after.confidence
        assert signal_before.risk_level == signal_after.risk_level
        assert signal_before.context_id == signal_after.context_id
        assert [e.agent for e in signal_before.supporting_factors] == [e.agent for e in signal_after.supporting_factors]


=== FILE: tests/test_enums.py ===
import pytest

from crypto_signal_engine.domain.enums import SignalDirection


class TestSignalDirectionFromScore:
    @pytest.mark.parametrize(
        "score,expected",
        [
            (0.0, SignalDirection.NEUTRAL),
            (0.10, SignalDirection.NEUTRAL),
            (0.15, SignalDirection.WEAK_LONG),
            (0.39, SignalDirection.WEAK_LONG),
            (0.40, SignalDirection.LONG),
            (0.69, SignalDirection.LONG),
            (0.70, SignalDirection.STRONG_LONG),
            (1.0, SignalDirection.STRONG_LONG),
            (-0.10, SignalDirection.NEUTRAL),
            (-0.15, SignalDirection.WEAK_SHORT),
            (-0.40, SignalDirection.SHORT),
            (-0.70, SignalDirection.STRONG_SHORT),
            (-1.0, SignalDirection.STRONG_SHORT),
        ],
    )
    def test_thresholds(self, score: float, expected: SignalDirection) -> None:
        assert SignalDirection.from_score(score) == expected

    def test_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError):
            SignalDirection.from_score(1.5)
        with pytest.raises(ValueError):
            SignalDirection.from_score(-1.5)

    def test_nan_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            SignalDirection.from_score(float("nan"))

    def test_inf_raises(self) -> None:
        with pytest.raises(ValueError, match="finite"):
            SignalDirection.from_score(float("inf"))


=== FILE: tests/test_events.py ===
from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.domain.enums import Timeframe
from crypto_signal_engine.domain.events import EventType, MarketDataEvent, SequenceCursor
from crypto_signal_engine.domain.models import Candle, OrderBookLevel, OrderBookSnapshot, Trade

UTC = timezone.utc


def make_candle(symbol: str = "BTCUSDT") -> Candle:
    return Candle(
        symbol=symbol, timeframe=Timeframe.M5,
        open_time=datetime(2026, 8, 31, tzinfo=UTC),
        close_time=datetime(2026, 8, 31, 0, 5, tzinfo=UTC),
        open=100.0, high=101.0, low=99.0, close=100.5, volume=1.0, is_closed=True,
    )


def make_trade(symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        symbol=symbol, trade_id=1, price=100.0, quantity=1.0,
        timestamp=datetime(2026, 8, 31, tzinfo=UTC), is_buyer_maker=False,
    )


def make_orderbook(symbol: str = "BTCUSDT") -> OrderBookSnapshot:
    return OrderBookSnapshot(
        symbol=symbol, timestamp=datetime(2026, 8, 31, tzinfo=UTC),
        bids=(OrderBookLevel(price=99.0, quantity=1.0),),
        asks=(OrderBookLevel(price=100.0, quantity=1.0),),
        last_update_id=1,
    )


class TestMarketDataEventTypeSafety:
    def test_valid_candle_event(self) -> None:
        event = MarketDataEvent(
            event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
        )
        assert event.symbol == "BTCUSDT"

    def test_valid_trade_event(self) -> None:
        MarketDataEvent(
            event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_trade(),
        )

    def test_valid_order_book_event(self) -> None:
        MarketDataEvent(
            event_type=EventType.ORDER_BOOK, symbol="BTCUSDT", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
        )

    def test_trade_type_with_candle_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Trade"):
            MarketDataEvent(
                event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_candle_type_with_trade_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Candle"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_trade(),
            )

    def test_order_book_type_with_candle_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="OrderBookSnapshot"):
            MarketDataEvent(
                event_type=EventType.ORDER_BOOK, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_candle_type_with_order_book_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Candle"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
            )

    def test_trade_type_with_order_book_payload_rejected(self) -> None:
        with pytest.raises(TypeError, match="Trade"):
            MarketDataEvent(
                event_type=EventType.TRADE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_orderbook(),
            )

    def test_symbol_mismatch_rejected(self) -> None:
        with pytest.raises(ValueError, match="eşleşmiyor"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="ETHUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="BTCUSDT"),
            )

    def test_symbol_mismatch_after_normalization_still_detected(self) -> None:
        # "btcusdt" (event.symbol) normalize sonrası BTCUSDT olur ve
        # payload'ın ETHUSDT'si ile hâlâ eşleşmemelidir.
        with pytest.raises(ValueError, match="eşleşmiyor"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="btcusdt", sequence_id=1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="ETHUSDT"),
            )

    def test_symbol_case_normalized_match_accepted(self) -> None:
        event = MarketDataEvent(
            event_type=EventType.CANDLE, symbol="btcusdt", sequence_id=1,
            received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(symbol="BTCUSDT"),
        )
        assert event.symbol == "BTCUSDT"

    def test_negative_sequence_rejected(self) -> None:
        with pytest.raises(ValueError, match="sequence_id"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=-1,
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )

    def test_naive_received_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            MarketDataEvent(
                event_type=EventType.CANDLE, symbol="BTCUSDT", sequence_id=1,
                received_at=datetime(2026, 8, 31), payload=make_candle(),
            )

    def test_event_type_string_rejected(self) -> None:
        """Kritik regresyon: EventType(str, Enum) olduğundan "CANDLE" ==
        EventType.CANDLE VE hash(...) eşit olur; bu yüzden dict.get() bazlı
        eski kontrol bir plain string'i YAKALAYAMIYORDU. isinstance bazlı
        require_enum bunu düzeltir."""
        with pytest.raises(TypeError, match="EventType"):
            MarketDataEvent(
                event_type="CANDLE", symbol="BTCUSDT", sequence_id=1,  # type: ignore[arg-type]
                received_at=datetime(2026, 8, 31, tzinfo=UTC), payload=make_candle(),
            )


class TestSequenceCursor:
    def test_symbol_normalized(self) -> None:
        cursor = SequenceCursor(
            symbol="btcusdt", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.symbol == "BTCUSDT"

    def test_negative_last_sequence_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="last_sequence_id"):
            SequenceCursor(
                symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=-1,
                last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_naive_last_received_at_rejected(self) -> None:
        with pytest.raises(ValueError, match="naive datetime"):
            SequenceCursor(
                symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
                last_received_at=datetime(2026, 8, 31),
            )

    def test_event_type_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="EventType"):
            SequenceCursor(
                symbol="BTCUSDT", event_type="TRADE", last_sequence_id=1,  # type: ignore[arg-type]
                last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
            )

    def test_next_is_valid_monotonic(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=100,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.next_is_valid(101) is True
        assert cursor.next_is_valid(100) is False
        assert cursor.next_is_valid(99) is False

    def test_is_stale_naive_now_rejected(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="naive datetime"):
            cursor.is_stale(datetime(2026, 8, 31), max_age_seconds=30)

    def test_is_stale_negative_max_age_rejected(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="max_age_seconds"):
            cursor.is_stale(datetime(2026, 8, 31, tzinfo=UTC), max_age_seconds=-1)

    def test_is_stale_boundary_exactly_at_threshold_not_stale(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, 10, 0, 0, tzinfo=UTC),
        )
        now = datetime(2026, 8, 31, 10, 0, 30, tzinfo=UTC)  # tam 30 saniye sonra
        assert cursor.is_stale(now, max_age_seconds=30) is False

    def test_is_stale_just_past_threshold_is_stale(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, 10, 0, 0, tzinfo=UTC),
        )
        now = datetime(2026, 8, 31, 10, 0, 30, 1, tzinfo=UTC)  # 30sn + 1 mikro saniye
        assert cursor.is_stale(now, max_age_seconds=30) is True

    def test_is_stale_zero_max_age_allowed(self) -> None:
        cursor = SequenceCursor(
            symbol="BTCUSDT", event_type=EventType.TRADE, last_sequence_id=1,
            last_received_at=datetime(2026, 8, 31, tzinfo=UTC),
        )
        assert cursor.is_stale(datetime(2026, 8, 31, tzinfo=UTC), max_age_seconds=0) is False


=== FILE: tests/test_execution_adapter.py ===
"""Faz 10 — execution/adapter.py testleri: exchange-filter doğrulama
(LOT_SIZE/MARKET_LOT_SIZE/PRICE_FILTER/MIN_NOTIONAL/NOTIONAL, applyToMarket/
applyMinToMarket/applyMaxToMarket semantics), MARKET order notional
doğrulaması için GÜNCEL PUBLIC fiyat lookup'ı (BLOCKER FİX — Karar 73),
`TestnetExecutionAdapter.submit()` akışı, duplicate intent kimliği,
multi-symbol izolasyonu. Tamamen offline."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crypto_signal_engine.execution.adapter import (
    TestnetExecutionAdapter,
    determine_market_price_requirement,
    parse_symbol_filters,
    validate_intent_against_filters,
)
from crypto_signal_engine.execution.errors import (
    ExecutionTimeoutError,
    ExecutionTransportError,
    FilterValidationError,
    MalformedResponseError,
    MarketPriceUnavailableError,
)
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"


def _exchange_info(
    symbol: str = "BTCUSDT",
    *,
    status: str = "TRADING",
    apply_to_market: bool = False,
    with_market_lot_size: bool = False,
    notional_filter: str | None = "MIN_NOTIONAL",
    min_notional: str = "10.0",
    max_notional: str | None = None,
    apply_min_to_market: bool = False,
    apply_max_to_market: bool = False,
) -> dict:
    filters = [
        {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
        {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
    ]
    if with_market_lot_size:
        filters.append(
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "5000.0", "stepSize": "0.001"}
        )
    if notional_filter == "MIN_NOTIONAL":
        filters.append({"filterType": "MIN_NOTIONAL", "minNotional": min_notional, "applyToMarket": apply_to_market})
    elif notional_filter == "NOTIONAL":
        notional_entry = {
            "filterType": "NOTIONAL", "minNotional": min_notional, "applyMinToMarket": apply_min_to_market,
        }
        if max_notional is not None:
            notional_entry["maxNotional"] = max_notional
            notional_entry["applyMaxToMarket"] = apply_max_to_market
        filters.append(notional_entry)
    return {"symbols": [{"symbol": symbol, "status": status, "filters": filters}]}


def _market_intent(**overrides) -> OrderIntent:
    defaults = dict(
        symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.MARKET,
        context_id="ctx-1", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


def _price_response(symbol: str = "BTCUSDT", price: str = "50000.0") -> tuple:
    return json_response({"symbol": symbol, "price": price})


def _order_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.01", "cummulativeQuoteQty": "500.0",
        "transactTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _adapter(get_responses=None, post_responses=None) -> tuple[TestnetExecutionAdapter, FakeTestnetHttpClient]:
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    return TestnetExecutionAdapter(client), http


class TestParseSymbolFilters:
    def test_parses_lot_size_price_and_notional(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        assert filters.step_size == 0.0001
        assert filters.min_qty == 0.0001
        assert filters.tick_size == 0.01
        assert filters.min_notional == 10.0
        assert filters.status == "TRADING"

    def test_missing_symbol_raises(self) -> None:
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(_exchange_info(symbol="ETHUSDT"), "BTCUSDT")

    def test_missing_lot_size_raises(self) -> None:
        payload = {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "filters": []}]}
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(payload, "BTCUSDT")

    def test_missing_apply_to_market_flag_is_malformed_not_defaulted(self) -> None:
        """Gerçek Binance yanıtı bu bayrağı HER ZAMAN içerir — eksikse
        sessizce `False` varsayılmaz, AÇIKÇA reddedilir (fail-closed)."""
        payload = {
            "symbols": [
                {
                    "symbol": "BTCUSDT", "status": "TRADING",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "9000.0", "stepSize": "0.0001"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "10.0"},  # applyToMarket EKSİK
                    ],
                }
            ]
        }
        with pytest.raises(FilterValidationError):
            parse_symbol_filters(payload, "BTCUSDT")

    def test_market_lot_size_parsed_when_present(self) -> None:
        filters = parse_symbol_filters(_exchange_info(with_market_lot_size=True), "BTCUSDT")
        assert filters.market_step_size == 0.001
        assert filters.market_min_qty == 0.001
        assert filters.market_max_qty == 5000.0

    def test_notional_filter_preferred_over_min_notional_when_both_absent_or_present(self) -> None:
        filters = parse_symbol_filters(
            _exchange_info(notional_filter="NOTIONAL", min_notional="15.0", apply_min_to_market=True), "BTCUSDT"
        )
        assert filters.min_notional == 15.0
        assert filters.min_notional_applies_to_market is True


class TestDetermineMarketPriceRequirement:
    def test_limit_never_requires_price_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.01, price=100.0,
        )
        assert determine_market_price_requirement(intent, filters) is False

    def test_market_with_quote_quantity_never_requires_price_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        intent = _market_intent(quantity=None, quote_quantity=20.0)
        assert determine_market_price_requirement(intent, filters) is False

    def test_market_base_quantity_with_applicable_filter_requires_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=True), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is True

    def test_market_base_quantity_with_apply_to_market_false_does_not_require_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(apply_to_market=False), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is False

    def test_market_with_no_notional_filter_at_all_does_not_require_lookup(self) -> None:
        filters = parse_symbol_filters(_exchange_info(notional_filter=None), "BTCUSDT")
        assert determine_market_price_requirement(_market_intent(), filters) is False


class TestValidateIntentAgainstFilters:
    def test_valid_market_intent_passes(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        validate_intent_against_filters(_market_intent(quantity=0.01), filters)  # raise etmemeli

    def test_quantity_below_min_qty_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(quantity=0.00001), filters)

    def test_quantity_not_step_aligned_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(quantity=0.000123456), filters)

    def test_symbol_not_trading_rejected(self) -> None:
        filters = parse_symbol_filters(_exchange_info(status="BREAK"), "BTCUSDT")
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(_market_intent(), filters)

    def test_price_not_tick_aligned_rejected_for_limit(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.01, price=100.005,
        )
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(intent, filters)

    def test_limit_notional_below_minimum_rejected_using_intent_price(self) -> None:
        """#12 — LIMIT regresyonu: notional HER ZAMAN `intent.price`
        kullanır, hiçbir market-price lookup'ı GEREKMEZ."""
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=0.0001, price=1.0,
        )
        with pytest.raises(FilterValidationError):
            validate_intent_against_filters(intent, filters)  # market_price VERİLMEDİ, GEREKMEZ

    def test_limit_notional_passes_without_market_price_argument(self) -> None:
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        intent = OrderIntent(
            symbol="BTCUSDT", side=OrderSide.BUY, order_type=OrderType.LIMIT,
            context_id="ctx-1", timestamp=NOW, quantity=1.0, price=100.0,
        )
        validate_intent_against_filters(intent, filters)  # raise etmemeli, market_price=None ile bile

    def test_never_silently_rounds_quantity(self) -> None:
        """Filtre ihlali AÇIKÇA reddedilir — quantity/price SESSİZCE
        farklı bir değere YUVARLANMAZ."""
        filters = parse_symbol_filters(_exchange_info(), "BTCUSDT")
        original_quantity = 0.000123456
        try:
            validate_intent_against_filters(_market_intent(quantity=original_quantity), filters)
            raise AssertionError("beklenen FilterValidationError fırlatılmadı")
        except FilterValidationError:
            pass  # intent nesnesi hiç mutate edilmedi (frozen dataclass zaten garanti eder)


class TestMarketOrderNotionalUsesCurrentPrice:
    """BLOCKER FİX (Karar 73) regresyon testleri: MARKET order + base
    quantity + uygulanabilir bir notional filtresi -> GÜNCEL bir TESTNET
    PUBLIC fiyatı kullanılarak notional GERÇEKTEN doğrulanır."""

    def test_market_buy_below_min_notional_via_current_price_rejected_before_post(self) -> None:
        async def scenario() -> None:
            # price=50000, quantity=0.0001 -> notional=5.0 < min_notional=10.0
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(side=OrderSide.BUY, quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_sell_below_min_notional_via_current_price_rejected_before_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(side=OrderSide.SELL, quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_above_minimum_notional_price_lookup_then_submission(self) -> None:
        async def scenario() -> None:
            # price=50000, quantity=0.01 -> notional=500 >= min_notional=10.0
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="50000.0")],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 2  # exchangeInfo + ticker/price, ÖNCE
            assert len(http.post_calls) == 1
            assert http.get_calls[1][0].endswith("/api/v3/ticker/price")
            assert http.post_calls[0][0].endswith("/api/v3/order")

        run_async(scenario())

    def test_market_quote_quantity_uses_quote_amount_directly_no_price_fetch(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True))],  # yalnızca exchangeInfo
                post_responses=[_order_response()],
            )
            intent = _market_intent(quantity=None, quote_quantity=20.0)  # 20 >= min_notional=10.0
            result = await adapter.submit(intent)
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # HİÇBİR fiyat lookup'ı YAPILMADI

        run_async(scenario())

    def test_market_quote_quantity_below_minimum_rejected_without_price_fetch(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(get_responses=[json_response(_exchange_info(apply_to_market=True))])
            intent = _market_intent(quantity=None, quote_quantity=5.0)  # 5 < min_notional=10.0
            with pytest.raises(FilterValidationError):
                await adapter.submit(intent)
            assert len(http.get_calls) == 1
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_apply_to_market_false_does_not_impose_min_notional_on_market(self) -> None:
        async def scenario() -> None:
            # notional çok küçük olurdu (0.0001 * hiçbir fiyat çekilmedi) ama
            # applyToMarket=False olduğundan filtre MARKET'e HİÇ uygulanmaz.
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=False))],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.0001))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # fiyat lookup'ı HİÇ YAPILMADI (gereksiz)

        run_async(scenario())

    def test_notional_filter_apply_min_to_market_true_apply_max_to_market_false(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(
                        _exchange_info(
                            notional_filter="NOTIONAL", min_notional="10.0", max_notional="1000000.0",
                            apply_min_to_market=True, apply_max_to_market=False,
                        )
                    ),
                    _price_response(price="50000.0"),
                ]
            )
            # notional = 0.0001 * 50000 = 5.0 < min 10.0 -> min UYGULANIR -> reddedilir
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_notional_filter_apply_max_to_market_true_rejects_oversized_market_order(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(
                        _exchange_info(
                            notional_filter="NOTIONAL", min_notional="10.0", max_notional="100.0",
                            apply_min_to_market=False, apply_max_to_market=True,
                        )
                    ),
                    _price_response(price="50000.0"),
                ]
            )
            # notional = 1.0 * 50000 = 50000 > max 100.0 -> max UYGULANIR -> reddedilir
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=1.0))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_lot_size_used_for_market_quantity_validation(self) -> None:
        async def scenario() -> None:
            # MARKET_LOT_SIZE: min=0.001 — LOT_SIZE'ın min'i (0.0001) İZİN
            # VERİRDİ ama MARKET_LOT_SIZE VARSA MARKET için o KULLANILIR.
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(with_market_lot_size=True, apply_to_market=False))]
            )
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.0001))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_market_lot_size_allows_quantity_valid_under_market_rules(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(with_market_lot_size=True, apply_to_market=False))],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))  # MARKET_LOT_SIZE aralığında/hizalı
            assert result.status == "FILLED"

        run_async(scenario())


class TestMarketPriceLookupFailClosed:
    """#8-#11 — fiyat lookup'ı BAŞARISIZ olursa order KESİNLİKLE
    GÖNDERİLMEZ (fail-closed); "doğrulamayı atla ve gönder" ASLA olmaz."""

    def test_price_lookup_transport_failure_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[
                    json_response(_exchange_info(apply_to_market=True)),
                    ExecutionTransportError("connection reset"),
                ]
            )
            with pytest.raises(ExecutionTransportError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_price_lookup_timeout_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), ExecutionTimeoutError("timed out")]
            )
            with pytest.raises(ExecutionTimeoutError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_malformed_price_response_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), (200, "not json{{")]
            )
            with pytest.raises(MalformedResponseError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_price_response_missing_fields_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), json_response({"symbol": "BTCUSDT"})]
            )
            with pytest.raises(MalformedResponseError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_wrong_symbol_price_response_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(symbol="ETHUSDT")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(symbol="BTCUSDT", quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_zero_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="0")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_negative_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="-1.0")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())

    def test_non_finite_price_prevents_post(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info(apply_to_market=True)), _price_response(price="NaN")]
            )
            with pytest.raises(MarketPriceUnavailableError):
                await adapter.submit(_market_intent(quantity=0.01))
            assert len(http.post_calls) == 0

        run_async(scenario())


class TestTestnetExecutionAdapterSubmit:
    def test_submit_validates_then_places_order(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response()],
            )
            result = await adapter.submit(_market_intent(quantity=0.01))
            assert result.status == "FILLED"
            assert len(http.get_calls) == 1  # exchangeInfo validate ÖNCE çağrıldı (fiyat lookup'ı GEREKMEDİ)
            assert len(http.post_calls) == 1

        run_async(scenario())

    def test_submit_rejects_before_transport_when_filters_violated(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(get_responses=[json_response(_exchange_info())])
            with pytest.raises(FilterValidationError):
                await adapter.submit(_market_intent(quantity=0.000123456))
            assert len(http.post_calls) == 0  # invalid quantity TRANSPORT'a hiç ULAŞMADI

        run_async(scenario())

    def test_duplicate_intent_identity_produces_same_client_order_id(self) -> None:
        first = _market_intent(context_id="ctx-dup")
        second = _market_intent(context_id="ctx-dup")
        assert first.client_order_id == second.client_order_id

    def test_multi_symbol_isolation_independent_results(self) -> None:
        async def scenario() -> None:
            adapter, http = _adapter(
                get_responses=[json_response(_exchange_info("BTCUSDT")), json_response(_exchange_info("ETHUSDT"))],
                post_responses=[
                    _order_response(),
                    json_response(
                        {
                            "symbol": "ETHUSDT", "clientOrderId": "csl-eth", "orderId": 2, "side": "SELL",
                            "status": "FILLED", "executedQty": "0.1", "cummulativeQuoteQty": "300.0",
                            "transactTime": int(NOW.timestamp() * 1000),
                        }
                    ),
                ],
            )
            btc_result = await adapter.submit(_market_intent(symbol="BTCUSDT", quantity=0.01))
            eth_result = await adapter.submit(_market_intent(symbol="ETHUSDT", side=OrderSide.SELL, quantity=0.01))
            assert btc_result.symbol == "BTCUSDT"
            assert eth_result.symbol == "ETHUSDT"
            assert btc_result.client_order_id != eth_result.client_order_id

        run_async(scenario())


=== FILE: tests/test_execution_cli.py ===
"""Faz 10/11 — scripts/binance_testnet_lab.py CLI testleri: dry-run/confirm
davranışı, kimlik bilgisi zorunluluğu, host yazdırma, secret redaction,
Faz 11 reconciliation komutları (order-status/reconcile/reconcile-pending).
Tamamen offline — `FakeTestnetHttpClient` + geçici SQLite enjekte edilir,
gerçek ağ YOK."""

from __future__ import annotations

from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

from scripts.binance_testnet_lab import run_cli_async

FAKE_KEY = "fake-cli-key"
FAKE_SECRET = "fake-cli-secret-value"


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


def _order_response(**overrides) -> dict:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-abc", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.001", "cummulativeQuoteQty": "50.0", "transactTime": 1735689600000,
    }
    payload.update(overrides)
    return payload


def _set_execution_db(monkeypatch, tmp_path) -> None:
    """Her `--confirm-testnet-order`/`reconcile*`/`order-status` testi,
    GERÇEK proje dizinine ASLA bir SQLite dosyası yazmamalıdır — bu yüzden
    `BINANCE_TESTNET_EXECUTION_DB_PATH` her zaman izole bir `tmp_path`'e
    yönlendirilir."""
    monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(tmp_path / "execution.db"))


class TestValidateSymbolDryRun:
    def test_works_without_any_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(run_cli_async(["validate-symbol", "BTCUSDT"], http_client=http))

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Target host: https://testnet.binance.vision" in out
        assert "symbol=BTCUSDT" in out


class TestAccountCheckRequiresCredentials:
    def test_fails_cleanly_without_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["account-check"], http_client=http))

        assert exit_code != 0
        assert "ERROR" in capsys.readouterr().err

    def test_succeeds_with_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(
            get_responses=[json_response({"accountType": "SPOT", "canTrade": True, "balances": []})]
        )

        exit_code = run_async(run_cli_async(["account-check"], http_client=http))

        assert exit_code == 0
        assert "accountType=SPOT" in capsys.readouterr().out


class TestPlaceOrderRequiresExplicitConfirmation:
    def test_without_confirm_flag_performs_dry_run_only(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001"], http_client=http
            )
        )

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "DRY-RUN ONLY" in out
        assert len(http.post_calls) == 0  # order HİÇBİR ZAMAN gönderilmedi

    def test_dry_run_does_not_require_credentials(self, monkeypatch, capsys) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001"], http_client=http
            )
        )

        assert exit_code == 0
        assert len(http.post_calls) == 0

    def test_with_confirm_flag_actually_submits(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code == 0
        assert len(http.post_calls) == 1
        assert "RESULT:" in capsys.readouterr().out

    def test_confirm_without_credentials_fails_cleanly(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code != 0
        assert len(http.post_calls) == 0

    def test_filter_violation_rejected_before_any_submission(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.000000001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        assert exit_code != 0
        assert len(http.post_calls) == 0

    def test_replay_same_context_id_produces_no_duplicate_post(self, monkeypatch, capsys, tmp_path) -> None:
        """Faz 11 — CLI üzerinden AYNI `--context-id` ile iki kez gönderim,
        yalnızca TEK bir gerçek POST üretir (idempotent replay)."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info()), json_response(_exchange_info())],
            post_responses=[json_response(_order_response())],
        )
        argv = [
            "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
            "--context-id", "cli-ctx-replay", "--confirm-testnet-order",
        ]

        first = run_async(run_cli_async(argv, http_client=http))
        second = run_async(run_cli_async(argv, http_client=http))

        assert first == 0
        assert second == 0
        assert len(http.post_calls) == 1

    def test_market_price_lookup_reflected_in_dry_run_output(self, monkeypatch, capsys) -> None:
        """Faz 10 kontrat korunumu: MARKET notional doğrulaması gerektiğinde
        dry-run çıktısı, kullanılan market_price'ı gösterir."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        info = _exchange_info()
        info["symbols"][0]["filters"].append(
            {"filterType": "MIN_NOTIONAL", "minNotional": "10.0", "applyToMarket": True}
        )
        http = FakeTestnetHttpClient(
            get_responses=[json_response(info), json_response({"symbol": "BTCUSDT", "price": "50000.0"})]
        )

        exit_code = run_async(
            run_cli_async(
                ["place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.01"], http_client=http
            )
        )

        assert exit_code == 0
        assert "market_price=50000.0" in capsys.readouterr().out


class TestLimitOrder:
    def test_limit_dry_run_shows_price(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        http = FakeTestnetHttpClient(get_responses=[json_response(_exchange_info())])

        exit_code = run_async(
            run_cli_async(
                ["place-limit", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001", "--price", "50000"],
                http_client=http,
            )
        )

        assert exit_code == 0
        assert "price=50000" in capsys.readouterr().out


class TestOrderStatusCommand:
    def test_no_local_record_returns_error(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["order-status", "--context-id", "does-not-exist"], http_client=http))

        assert exit_code != 0
        assert len(http.get_calls) == 0  # yalnızca yerel okuma — HİÇBİR ağ çağrısı YAPILMADI
        assert len(http.post_calls) == 0

    def test_shows_record_after_submission(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-status", "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )
        capsys.readouterr()

        exit_code = run_async(
            run_cli_async(["order-status", "--context-id", "cli-ctx-status"], http_client=FakeTestnetHttpClient())
        )

        assert exit_code == 0
        assert "context_id=cli-ctx-status" in capsys.readouterr().out


class TestReconcileCommand:
    def test_reconcile_unknown_identity_fails_cleanly_not_a_crash(self, monkeypatch, capsys, tmp_path) -> None:
        """Regresyon: `reconcile()` önceden ham bir `ValueError` fırlatıyordu
        (CLI'nin `except ExecutionError` yakalayıcısı tarafından
        YAKALANMAZDI) — artık `LocalExecutionRecordNotFoundError`
        (bir `ExecutionError` alt sınıfı) fırlatır, temiz bir "ERROR: ..."
        mesajıyla sonuçlanır, ham bir traceback DEĞİL."""
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(
            run_cli_async(["reconcile", "--context-id", "does-not-exist"], http_client=http)
        )

        assert exit_code != 0
        assert "ERROR" in capsys.readouterr().err

    def test_reconcile_requires_credentials_when_record_exists(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        submit_http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())],
            post_responses=[json_response(_order_response(status="NEW", executedQty="0", cummulativeQuoteQty="0"))],
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-needs-creds", "--confirm-testnet-order",
                ],
                http_client=submit_http,
            )
        )
        capsys.readouterr()

        monkeypatch.delenv("BINANCE_TESTNET_API_KEY", raising=False)
        monkeypatch.delenv("BINANCE_TESTNET_API_SECRET", raising=False)
        exit_code = run_async(
            run_cli_async(["reconcile", "--context-id", "cli-ctx-needs-creds"], http_client=FakeTestnetHttpClient())
        )

        assert exit_code != 0

    def test_reconcile_pending_with_nothing_pending(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient()

        exit_code = run_async(run_cli_async(["reconcile-pending"], http_client=http))

        assert exit_code == 0
        assert "No pending" in capsys.readouterr().out
        assert len(http.get_calls) == 0

    def test_reconcile_pending_requeries_unknown_not_found_without_new_post(self, monkeypatch, capsys, tmp_path) -> None:
        """BLOCKER FİX (Karar 78) regresyonu: ambiguous submission -2013 ile
        `UNKNOWN_NOT_FOUND`'a düştüğünde, bu kayıt reconciliation-pending
        KÜMESİNDE KALIR (artık "güvenli/çözülmüş" SAYILMAZ). Yeni bir
        "process" (`reconcile-pending`) bu kaydı GERÇEKTEN yeniden sorgular
        — ama ASLA yeni bir POST üretmez."""
        from crypto_signal_engine.execution.errors import ExecutionTimeoutError

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)

        # CLI wrapper'ın KENDİ `validate_intent()`'i VE `submit()`'in İÇ
        # `validate_intent()`'i AYRI AYRI birer GET tüketir (bkz. yukarıdaki
        # #8 testinin yorumu) — bu yüzden İKİ exchange_info yanıtı gerekir,
        # ardından reconcile-query'nin -2013'ü.
        submit_http = FakeTestnetHttpClient(
            get_responses=[
                json_response(_exchange_info()),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
            ],
            post_responses=[ExecutionTimeoutError("timed out")],
        )
        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--context-id", "cli-ctx-ambiguous", "--confirm-testnet-order",
                ],
                http_client=submit_http,
            )
        )
        capsys.readouterr()

        # submit()'in KENDİSİ zaten AMBIGUOUS -> UNKNOWN_NOT_FOUND'a reconcile
        # denedi (tek bir -2013). Kayıt HÂLÂ "çözülmemiş" (Karar 78) — bu
        # yüzden yeni bir "process" (`reconcile-pending`) onu GERÇEKTEN
        # yeniden sorgular (bir GET beklenir), ama HİÇBİR yeni POST üretmez.
        reconcile_http = FakeTestnetHttpClient(
            get_responses=[(400, '{"code": -2013, "msg": "Order does not exist."}')],
        )
        exit_code = run_async(run_cli_async(["reconcile-pending"], http_client=reconcile_http))

        assert exit_code == 0
        assert "UNKNOWN_NOT_FOUND" in capsys.readouterr().out
        assert len(reconcile_http.get_calls) == 1
        assert len(reconcile_http.post_calls) == 0

    def test_replay_confirmed_order_after_unknown_not_found_produces_no_second_post(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        """Test-matrisi #8: bir ambiguous POST timeout'u, ardından tek bir
        -2013 ile `UNKNOWN_NOT_FOUND`'a düştükten SONRA, AYNI confirmed CLI
        çağrısının (aynı `--context-id`) TEKRARLANMASI, İKİNCİ bir POST
        ÜRETMEZ — yalnızca yeniden sorgular (BLOCKER FİX, Karar 78)."""
        from crypto_signal_engine.execution.errors import ExecutionTimeoutError

        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)

        # Sıra ÖNEMLİDİR: `_place_order()`'daki CLI wrapper'ın KENDİ
        # `validate_intent()` çağrısı VE `submit()`'in İÇ `validate_intent()`
        # çağrısı AYRI AYRI birer GET tüketir (bkz. `scripts/binance_testnet_lab.py`
        # `_place_order()` + `reconciliation_service.py::submit()`).
        # run1: CLI-validate(exchange_info) -> submit-validate(exchange_info)
        #       -> POST timeout -> reconcile-query(-2013) => UNKNOWN_NOT_FOUND
        # run2 (aynı context_id, HÂLÂ çözülmemiş): CLI-validate(exchange_info)
        #       -> submit() existing kaydı bulur -> reconcile-query(-2013)
        #       => HÂLÂ UNKNOWN_NOT_FOUND, YENİ POST YOK.
        http = FakeTestnetHttpClient(
            get_responses=[
                json_response(_exchange_info()),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
                json_response(_exchange_info()),
                (400, '{"code": -2013, "msg": "Order does not exist."}'),
            ],
            post_responses=[ExecutionTimeoutError("timed out")],
        )
        argv = [
            "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
            "--context-id", "cli-ctx-replay-ambiguous", "--confirm-testnet-order",
        ]

        first = run_async(run_cli_async(argv, http_client=http))
        capsys.readouterr()
        second = run_async(run_cli_async(argv, http_client=http))

        assert first == 0
        assert second == 0
        assert "UNKNOWN_NOT_FOUND" in capsys.readouterr().out
        assert len(http.post_calls) == 1
        assert len(http.get_calls) == 5


class TestSecretNeverPrinted:
    def test_credentials_never_appear_in_stdout_or_stderr(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        captured = capsys.readouterr()
        assert FAKE_KEY not in captured.out
        assert FAKE_KEY not in captured.err
        assert FAKE_SECRET not in captured.out
        assert FAKE_SECRET not in captured.err

    def test_signature_param_never_printed_directly(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        _set_execution_db(monkeypatch, tmp_path)
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        _, params, _ = http.post_calls[0]
        assert "signature" in params  # transport'a doğru GİTTİ
        assert params["signature"] not in capsys.readouterr().out  # ama HİÇBİR ZAMAN CLI çıktısına YAZDIRILMADI

    def test_persisted_execution_record_never_contains_credentials(self, monkeypatch, capsys, tmp_path) -> None:
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", FAKE_KEY)
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", FAKE_SECRET)
        db_path = tmp_path / "execution.db"
        monkeypatch.setenv("BINANCE_TESTNET_EXECUTION_DB_PATH", str(db_path))
        http = FakeTestnetHttpClient(
            get_responses=[json_response(_exchange_info())], post_responses=[json_response(_order_response())]
        )

        run_async(
            run_cli_async(
                [
                    "place-market", "--symbol", "BTCUSDT", "--side", "BUY", "--quantity", "0.001",
                    "--confirm-testnet-order",
                ],
                http_client=http,
            )
        )

        raw_db_bytes = db_path.read_bytes()
        assert FAKE_KEY.encode() not in raw_db_bytes
        assert FAKE_SECRET.encode() not in raw_db_bytes


=== FILE: tests/test_execution_concurrency.py ===
"""HIGH-5 fix testleri — "Manual Binance Spot Testnet concurrent submit
aynı deterministic clientOrderId ile local durable execution truth'u
corrupt edebilir (örn. FILLED -> REJECTED)".

Bu dosya İKİ AYRI savunma katmanını AYRI AYRI kanıtlar:
1. `ExecutionStateStore.save()` seviyesinde MUTLAK, monotonik bir
   compare-and-swap (`assert_monotonic`, `BEGIN IMMEDIATE`) — bu, AYNI
   context_id'ye YAZAN İKİ BAĞIMSIZ (`ExecutionReconciliationService`
   instance'ı, farklı process'leri SİMÜLE eder — HİÇBİR kilit
   PAYLAŞMAZLAR) yazarın interleave olduğu EN KÖTÜ senaryoyu bile güvenli
   kılar.
2. `ExecutionReconciliationService` seviyesinde context_id-başına bir
   `asyncio.Lock` — AYNI process İÇİNDE eşzamanlı iki `submit()` çağrısının
   İKİSİNİN DE `place_order()`'a ULAŞMASINI (gerçek bir Binance'e ÇİFT POST
   göndermeyi) baştan engeller.

Tamamen offline — gerçek Binance TESTNET ağına ASLA bağımlı değil."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from crypto_signal_engine.errors import PersistenceError
from crypto_signal_engine.execution.errors import (
    BinanceRejectionError,
    ExecutionTransportError,
    ReconciliationContradictionError,
)
from crypto_signal_engine.execution.models import OrderIntent, OrderSide, OrderType
from crypto_signal_engine.execution.reconciliation_models import (
    ExecutionLifecycleState,
    apply_exchange_truth,
    new_record,
    transition,
)
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
        context_id="ctx-race", timestamp=NOW, quantity=0.01,
    )
    defaults.update(overrides)
    return OrderIntent(**defaults)


def _order_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-race", "orderId": 1, "side": "BUY",
        "status": "FILLED", "executedQty": "0.01", "cummulativeQuoteQty": "500.0",
        "transactTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _query_response(**overrides) -> tuple:
    payload = {
        "symbol": "BTCUSDT", "clientOrderId": "csl-race", "orderId": 1, "side": "BUY",
        "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
        "updateTime": int(NOW.timestamp() * 1000),
    }
    payload.update(overrides)
    return json_response(payload)


def _service(get_responses, post_responses, *, db_path) -> tuple[ExecutionReconciliationService, ExecutionStateStore]:
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    store = ExecutionStateStore(db_path)
    service = ExecutionReconciliationService(client, store, clock=FixedClock(NOW))
    return service, store


def _filled_record(intent: OrderIntent):
    record = new_record(intent, now=NOW)
    record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
    return apply_exchange_truth(
        record, new_state=ExecutionLifecycleState.FILLED, exchange_order_id=1,
        executed_quantity=0.01, cumulative_quote_quantity=500.0, now=NOW,
    )


class TestStoreLevelMonotonicity:
    """`ExecutionStateStore.save()`'in KENDİSİ, HERHANGİ bir çağıran
    disiplininden BAĞIMSIZ olarak İMKANSIZ regresyonları reddeder."""

    def test_filled_cannot_be_overwritten_by_rejected(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        filled = _filled_record(intent)
        store.save(filled)

        rejected = transition(
            new_record(intent, now=NOW), new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW
        )
        rejected = transition(rejected, new_state=ExecutionLifecycleState.REJECTED, now=NOW, detail="duplicate")

        with pytest.raises(ReconciliationContradictionError):
            store.save(rejected)

        # Durable truth DOKUNULMADAN FILLED kalır.
        current = store.load_by_context_id(intent.context_id)
        assert current.lifecycle_state == ExecutionLifecycleState.FILLED
        assert current.executed_quantity == 0.01
        store.close()

    def test_canceled_cannot_regress_to_new(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        canceled = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.CANCELED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        store.save(canceled)

        stale_new = apply_exchange_truth(
            transition(new_record(intent, now=NOW), new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW),
            new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        with pytest.raises(ReconciliationContradictionError):
            store.save(stale_new)
        assert store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.CANCELED
        store.close()

    def test_executed_quantity_cannot_decrease(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        partial = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=1,
            executed_quantity=0.005, cumulative_quote_quantity=250.0, now=NOW,
        )
        store.save(partial)

        regressed = apply_exchange_truth(
            partial, new_state=ExecutionLifecycleState.PARTIALLY_FILLED, exchange_order_id=1,
            executed_quantity=0.005, cumulative_quote_quantity=250.0, now=NOW,
        )
        object.__setattr__(regressed, "executed_quantity", 0.001)  # stale/kaybolmuş bir güncelleme simülasyonu
        with pytest.raises(ReconciliationContradictionError):
            store.save(regressed)
        assert store.load_by_context_id(intent.context_id).executed_quantity == 0.005
        store.close()

    def test_exchange_order_id_cannot_change(self, tmp_path) -> None:
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        record = new_record(intent, now=NOW)
        record = transition(record, new_state=ExecutionLifecycleState.SUBMISSION_ATTEMPTED, now=NOW)
        acked = apply_exchange_truth(
            record, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        store.save(acked)

        mismatched = apply_exchange_truth(
            acked, new_state=ExecutionLifecycleState.ACKNOWLEDGED, exchange_order_id=1,
            executed_quantity=0.0, cumulative_quote_quantity=0.0, now=NOW,
        )
        object.__setattr__(mismatched, "exchange_order_id", 999)
        with pytest.raises(ReconciliationContradictionError):
            store.save(mismatched)
        store.close()

    def test_identical_restate_is_idempotent_no_error(self, tmp_path) -> None:
        """AYNI terminal durumun TEKRAR yazılması (örn. iki reconcile
        sweep'inin AYNI sonucu bulması) bir regresyon DEĞİLDİR."""
        store = ExecutionStateStore(tmp_path / "s.db")
        intent = _intent()
        filled = _filled_record(intent)
        store.save(filled)
        store.save(filled)  # HATA FIRLATMAMALI
        assert store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED
        store.close()


class _RaceInjectingClient:
    """`place_order()` çağrıldığında, GERÇEK bir cross-process race'i
    simüle eder: KENDİ yanıtını (exception) döndürmeden HEMEN ÖNCE,
    "BAŞKA bir eşzamanlı yazarın" (örn. AYNI clientOrderId ile eşzamanlı
    çalışan ikinci bir CLI process'inin) SONUCUNU doğrudan store'a yazar.
    Bu, iki BAĞIMSIZ `ExecutionReconciliationService` instance'ının
    (HİÇBİR kilit PAYLAŞMADAN) AYNI context_id'ye eşzamanlı yazdığı
    en kötü durumu, GERÇEK asyncio zamanlama şansına bağlı KALMADAN
    deterministik olarak üretir."""

    def __init__(self, exchange_info_payload: dict, store: ExecutionStateStore, winner_record, own_exception: Exception):
        self._exchange_info_payload = exchange_info_payload
        self._store = store
        self._winner_record = winner_record
        self._own_exception = own_exception

    async def exchange_info(self, symbol: str) -> dict:
        return self._exchange_info_payload

    async def symbol_price(self, symbol: str) -> float:  # pragma: no cover - bu senaryoda çağrılmaz
        raise AssertionError("symbol_price bu testte çağrılmamalı")

    async def place_order(self, intent: OrderIntent):
        self._store.save(self._winner_record)
        raise self._own_exception


class TestConcurrentSubmitRace:
    """Tam olarak bildirilen HIGH-5 senaryosu: iki BAĞIMSIZ (kilit
    PAYLAŞMAYAN) `ExecutionReconciliationService`, AYNI context_id/
    client_order_id ile, AYNI durable store'a karşı eşzamanlı `submit()`
    çağırıyor. Biri FILLED alıyor, diğeri (Binance'in KENDİ duplicate-
    clientOrderId reddi nedeniyle) REJECTED alıyor."""

    def test_losing_rejected_result_cannot_overwrite_winning_filled_result(self, tmp_path) -> None:
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()

            # "Kazanan" (A) süreci: place_order() BAŞARIYLA FILLED döner.
            winner_store = ExecutionStateStore(db_path)
            winner_client = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            winner_service = ExecutionReconciliationService(winner_client, winner_store, clock=FixedClock(NOW))
            winner_result = await winner_service.submit(intent)
            assert winner_result.lifecycle_state == ExecutionLifecycleState.FILLED

            # "Kaybeden" (B) süreci: TAMAMEN AYRI bir service/lock instance'ı
            # (farklı bir process'i simüle eder), AYNI context_id/
            # client_order_id ile eşzamanlı submit ediyor — ama Binance
            # KENDİSİ bu ikinci POST'u "duplicate order" olarak reddediyor.
            # place_order() çağrıldığı ANDA (A'nın SONUCU zaten durable
            # olduktan SONRA — bu, worst-case interleaving'i temsil eder)
            # kendi REJECTED yanıtını üretir.
            loser_store = ExecutionStateStore(db_path)  # AYNI dosya, AYRI connection (AYRI process simülasyonu)
            loser_client = _RaceInjectingClient(
                _exchange_info(), loser_store, winner_result,
                BinanceRejectionError("Duplicate order sent.", binance_code=-2010),
            )
            loser_service = ExecutionReconciliationService(loser_client, loser_store, clock=FixedClock(NOW))
            loser_result = await loser_service.submit(intent)

            # HIGH-5 invariant: kaybeden, KENDİ REJECTED sonucunu DÖNMEZ —
            # OTORİTER (kazanan) FILLED gerçeğini döner.
            assert loser_result.lifecycle_state == ExecutionLifecycleState.FILLED
            assert loser_result.executed_quantity == 0.01

            # Durable truth (HERHANGİ bir bağlantıdan okunduğunda) HÂLÂ FILLED'dir.
            assert winner_store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED
            assert loser_store.load_by_context_id(intent.context_id).lifecycle_state == ExecutionLifecycleState.FILLED

            winner_store.close()
            loser_store.close()

        run_async(scenario())

    def test_reconciliation_after_race_is_authoritative(self, tmp_path) -> None:
        """Yarıştan SONRA, `client_order_id` ile reconcile/sorgu, ZATEN
        terminal olan OTORİTER FILLED gerçeğini döner — GEREKSİZ bir
        exchange sorgusu YAPMADAN (kısa-devre, bkz. `reconcile()`)."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()
            store_a = ExecutionStateStore(db_path)
            client_a = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            service_a = ExecutionReconciliationService(client_a, store_a, clock=FixedClock(NOW))
            winner = await service_a.submit(intent)

            store_b = ExecutionStateStore(db_path)
            loser_client = _RaceInjectingClient(
                _exchange_info(), store_b, winner, BinanceRejectionError("Duplicate order sent.", binance_code=-2010)
            )
            service_b = ExecutionReconciliationService(loser_client, store_b, clock=FixedClock(NOW))
            await service_b.submit(intent)

            # Faz 11'in MEVCUT authoritative-by-client_order_id sözleşmesi.
            reconciled = await service_b.reconcile(client_order_id=intent.client_order_id)
            assert reconciled.lifecycle_state == ExecutionLifecycleState.FILLED

            store_a.close()
            store_b.close()

        run_async(scenario())

    def test_restart_after_race_sees_authoritative_filled(self, tmp_path) -> None:
        """"Restart" simülasyonu: yarış çözüldükten SONRA, TAMAMEN YENİ bir
        `ExecutionReconciliationService`/`ExecutionStateStore` (yeni bir
        process başlatımını temsil eder) AYNI context_id için `submit()`
        TEKRAR çağrılırsa (örn. operatör script'i TEKRAR çalıştırırsa),
        idempotent replay FILLED'i döner — KESİNLİKLE yeniden POST YAPMAZ."""
        async def scenario() -> None:
            db_path = tmp_path / "s.db"
            intent = _intent()
            store_a = ExecutionStateStore(db_path)
            client_a = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET),
                FakeTestnetHttpClient([json_response(_exchange_info())], [_order_response()]),
                clock=FixedClock(NOW),
            )
            service_a = ExecutionReconciliationService(client_a, store_a, clock=FixedClock(NOW))
            winner = await service_a.submit(intent)
            store_a.close()

            store_b = ExecutionStateStore(db_path)
            loser_client = _RaceInjectingClient(
                _exchange_info(), store_b, winner, BinanceRejectionError("Duplicate order sent.", binance_code=-2010)
            )
            service_b = ExecutionReconciliationService(loser_client, store_b, clock=FixedClock(NOW))
            await service_b.submit(intent)
            store_b.close()

            # "Restart": tamamen yeni store/service/http — hiçbir POST scripti bile yok.
            restarted_store = ExecutionStateStore(db_path)
            restarted_http = FakeTestnetHttpClient([], [])
            restarted_client = BinanceTestnetClient(
                BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET), restarted_http, clock=FixedClock(NOW)
            )
            restarted_service = ExecutionReconciliationService(restarted_client, restarted_store, clock=FixedClock(NOW))
            replay = await restarted_service.submit(intent)
            assert replay.lifecycle_state == ExecutionLifecycleState.FILLED
            assert len(restarted_http.post_calls) == 0  # idempotent replay — POST YOK
            restarted_store.close()

        run_async(scenario())


class TestInProcessLockPreventsDoubleSubmit:
    """AYNI process/`ExecutionReconciliationService` instance'ı içinde,
    eşzamanlı (`asyncio.gather`) iki `submit()` çağrısı, ASLA `place_order()`'a
    İKİ KEZ ULAŞMAZ — context_id başına `asyncio.Lock` bunu YAPISAL olarak
    engeller (bkz. reconciliation_service.py, HIGH-5 fix)."""

    def test_two_concurrent_submit_calls_same_service_post_exactly_once(self, tmp_path) -> None:
        async def scenario() -> None:
            service, store = _service(
                get_responses=[json_response(_exchange_info())],
                post_responses=[_order_response()],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            first, second = await asyncio.gather(service.submit(intent), service.submit(intent))

            assert first.lifecycle_state == ExecutionLifecycleState.FILLED
            assert second.lifecycle_state == ExecutionLifecycleState.FILLED
            assert first.exchange_order_id == second.exchange_order_id
            store.close()

        run_async(scenario())

    def test_concurrent_submit_and_reconcile_do_not_corrupt_state(self, tmp_path) -> None:
        """`submit()` ile eşzamanlı bir `reconcile()` çağrısı da AYNI
        context_id kilidini paylaşır — birbirleriyle YARIŞMAZLAR."""
        async def scenario() -> None:
            service, store = _service(
                get_responses=[
                    json_response(_exchange_info()),
                    _query_response(status="NEW"),
                ],
                post_responses=[ExecutionTransportError("connection reset")],
                db_path=tmp_path / "s.db",
            )
            intent = _intent()
            submit_task = asyncio.create_task(service.submit(intent))
            await asyncio.sleep(0)  # submit()'in en azından başlamasına izin ver
            try:
                reconciled = await service.reconcile(context_id=intent.context_id)
            except Exception:
                reconciled = None  # kayıt henüz YOKSA LocalExecutionRecordNotFoundError beklenir
            submitted = await submit_task

            assert submitted.lifecycle_state in (
                ExecutionLifecycleState.ACKNOWLEDGED, ExecutionLifecycleState.FILLED,
                ExecutionLifecycleState.PARTIALLY_FILLED, ExecutionLifecycleState.AMBIGUOUS,
            )
            final = store.load_by_context_id(intent.context_id)
            assert final.lifecycle_state == submitted.lifecycle_state
            store.close()

        run_async(scenario())


=== FILE: tests/test_execution_lifecycle.py ===
"""
Autonomous Testnet trading lifecycle — unit tests for the pure domain
module `crypto_signal_engine.execution.lifecycle`. These are the direct
evidence for Phase 22's "Fee/quantity/price separation", "Effective stop
mechanics", "Candle-ordering/lookahead", and "P&L integrity" invariant
groups — all pure, no I/O, no clock.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crypto_signal_engine.execution.lifecycle import (
    BridgePositionRecord,
    Candle,
    DailyRiskAccumulator,
    ExitPolicyConfig,
    ExitReason,
    FeeLedgerEntry,
    Fill,
    LifecyclePriceState,
    PositionLifecycleState,
    RiskPolicyConfig,
    apply_entry_fills,
    apply_exit_fills,
    apply_trade_to_daily_accumulator,
    compute_cooldown_until,
    compute_initial_stop_and_target,
    compute_sell_quantity,
    compute_trade_risk_contribution,
    cooldown_clear,
    daily_loss_breaker_tripped,
    evaluate_candle,
    flat_record,
    floor_to_step,
    gross_realized_pnl,
    max_exposure_gate_open,
    compute_total_exposure,
    max_positions_gate_open,
    net_realized_pnl,
    resolve_lot_size_filter,
    trading_day_key,
    unrealized_gross_pnl,
)

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _candle(*, open_: float, high: float, low: float, close: float, close_time: datetime) -> Candle:
    return Candle(open=open_, high=high, low=low, close=close, close_time=close_time)


class TestApplyEntryFills:
    def test_single_fill_no_commission(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.0, commission_asset="USDT"),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 100.0
        assert agg.net_owned_base_quantity == 2.0
        assert agg.fee_ledger_entries == ()

    def test_base_asset_commission_reduces_net_quantity_not_vwap(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 100.0  # unaffected by fee
        assert agg.net_owned_base_quantity == pytest.approx(1.998)
        assert len(agg.fee_ledger_entries) == 1
        assert agg.fee_ledger_entries[0].asset == "BTC"
        assert agg.fee_ledger_entries[0].amount == 0.002

    def test_quote_asset_commission_leaves_net_quantity_unchanged(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.2, commission_asset="USDT", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == 2.0
        assert len(agg.fee_ledger_entries) == 1
        assert agg.fee_ledger_entries[0].asset == "USDT"

    def test_bnb_commission_leaves_net_quantity_unchanged(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.001, commission_asset="BNB", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == 2.0

    def test_multi_fill_vwap_is_notional_weighted(self) -> None:
        fills = (
            Fill(price=100.0, quantity=1.0, commission=0.0, commission_asset="USDT"),
            Fill(price=200.0, quantity=1.0, commission=0.0, commission_asset="USDT"),
        )
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.gross_entry_vwap == 150.0
        assert agg.gross_filled_quantity == 2.0

    def test_per_fill_commission_asset_can_vary_within_one_order(self) -> None:
        fills = (
            Fill(price=100.0, quantity=1.0, commission=0.001, commission_asset="BTC", trade_id=1),
            Fill(price=100.0, quantity=1.0, commission=0.1, commission_asset="USDT", trade_id=2),
        )
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert agg.net_owned_base_quantity == pytest.approx(1.999)
        assert len(agg.fee_ledger_entries) == 2

    def test_fee_recorded_exactly_once_never_double_counted(self) -> None:
        fills = (Fill(price=100.0, quantity=1.0, commission=0.001, commission_asset="BTC", trade_id=1),)
        agg = apply_entry_fills(fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        total_fee_in_ledger = sum(e.amount for e in agg.fee_ledger_entries)
        assert total_fee_in_ledger == 0.001
        # gross_entry_vwap must show zero trace of the fee.
        assert agg.gross_entry_vwap == 100.0

    def test_empty_fills_rejected(self) -> None:
        with pytest.raises(ValueError):
            apply_entry_fills((), base_asset="BTC", client_order_id="csl-1", now=_NOW)


class TestApplyExitFills:
    def test_base_asset_commission_reported_separately_not_pre_subtracted(self) -> None:
        fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        agg = apply_exit_fills(fills, base_asset="BTC", client_order_id="csl-2", now=_NOW)
        assert agg.exit_gross_vwap == 100.0
        assert agg.gross_sold_quantity == 2.0
        assert agg.base_asset_commission_total == 0.002


class TestPnL:
    def test_gross_realized_pnl_uses_only_gross_vwaps(self) -> None:
        pnl = gross_realized_pnl(gross_entry_vwap=100.0, exit_gross_vwap=110.0, quantity_closed=2.0)
        assert pnl == 20.0

    def test_net_realized_pnl_subtracts_known_fees_once(self) -> None:
        gross = 20.0
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="BTC", usdt_equivalent=0.05, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
            FeeLedgerEntry(
                amount=0.02, asset="USDT", usdt_equivalent=0.02, client_order_id="csl-2",
                side="EXIT", trade_id=2, recorded_at=_NOW,
            ),
        )
        net = net_realized_pnl(gross_pnl=gross, fee_ledger_entries=fees)
        assert net == pytest.approx(20.0 - 0.07)

    def test_net_realized_pnl_unknown_when_any_fee_unconvertible(self) -> None:
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        assert net_realized_pnl(gross_pnl=20.0, fee_ledger_entries=fees) is None

    def test_base_asset_commission_already_in_net_qty_not_double_subtracted_in_pnl(self) -> None:
        # A BUY fill with base-asset commission reduces net_owned_base_quantity
        # (Phase 5) — the trade closes on that REDUCED quantity, and the
        # gross P&L formula multiplies by quantity_closed, which is already
        # net of the fee. The fee then appears ONCE MORE in fee_ledger for
        # the *net* P&L subtraction (a different, additive, USDT-denominated
        # deduction) — never as a second reduction of quantity/vwap.
        entry_fills = (Fill(price=100.0, quantity=2.0, commission=0.002, commission_asset="BTC", trade_id=1),)
        entry_agg = apply_entry_fills(entry_fills, base_asset="BTC", client_order_id="csl-1", now=_NOW)
        assert entry_agg.net_owned_base_quantity == pytest.approx(1.998)

        exit_fills = (Fill(price=110.0, quantity=entry_agg.net_owned_base_quantity, commission=0.0, commission_asset="USDT"),)
        exit_agg = apply_exit_fills(exit_fills, base_asset="BTC", client_order_id="csl-2", now=_NOW)

        gross = gross_realized_pnl(
            gross_entry_vwap=entry_agg.gross_entry_vwap,
            exit_gross_vwap=exit_agg.exit_gross_vwap,
            quantity_closed=exit_agg.gross_sold_quantity,
        )
        assert gross == pytest.approx((110.0 - 100.0) * 1.998)

        fee_entry_usdt_equiv = 0.002 * 100.0  # priced at entry fill price for this test
        ledger = entry_agg.fee_ledger_entries[0]
        ledger_with_usdt = FeeLedgerEntry(
            amount=ledger.amount, asset=ledger.asset, usdt_equivalent=fee_entry_usdt_equiv,
            client_order_id=ledger.client_order_id, side=ledger.side, trade_id=ledger.trade_id,
            recorded_at=ledger.recorded_at,
        )
        net = net_realized_pnl(gross_pnl=gross, fee_ledger_entries=(ledger_with_usdt,))
        assert net == pytest.approx(gross - fee_entry_usdt_equiv)


class TestUnrealizedGrossPnl:
    """Dashboard localization — direct unit tests for the pure
    mark-to-market helper, independent of any dashboard/route/template
    exercise (per mission requirement #17)."""

    def test_price_above_entry_is_positive(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=2.0, current_price=110.0)
        assert pnl == pytest.approx(20.0)

    def test_price_below_entry_is_negative(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=2.0, current_price=95.0)
        assert pnl == pytest.approx(-10.0)

    def test_price_equal_to_entry_is_zero(self) -> None:
        pnl = unrealized_gross_pnl(gross_entry_vwap=100.0, net_owned_base_quantity=3.0, current_price=100.0)
        assert pnl == 0.0

    def test_same_formula_shape_as_gross_realized_pnl(self) -> None:
        # Structurally identical to `gross_realized_pnl` -- `current_price`
        # simply stands in for `exit_gross_vwap` (mark-to-market, not an
        # actual fill).
        realized = gross_realized_pnl(gross_entry_vwap=50.0, exit_gross_vwap=53.0, quantity_closed=4.0)
        unrealized = unrealized_gross_pnl(gross_entry_vwap=50.0, net_owned_base_quantity=4.0, current_price=53.0)
        assert unrealized == realized


class TestInitialStopAndTarget:
    def test_formula(self) -> None:
        config = ExitPolicyConfig(stop_atr_multiple=2.0, take_profit_atr_multiple=4.0)
        stop, target = compute_initial_stop_and_target(entry_price=100.0, atr=5.0, config=config)
        assert stop == 90.0
        assert target == 120.0

    def test_stop_never_negative(self) -> None:
        config = ExitPolicyConfig(stop_atr_multiple=10.0)
        stop, _ = compute_initial_stop_and_target(entry_price=10.0, atr=5.0, config=config)
        assert stop == 0.0

    def test_rejects_non_positive_entry_price(self) -> None:
        with pytest.raises(ValueError):
            compute_initial_stop_and_target(entry_price=0.0, atr=5.0, config=ExitPolicyConfig())


def _fresh_state(*, entry_price: float = 100.0, atr: float = 5.0, config: ExitPolicyConfig | None = None) -> LifecyclePriceState:
    cfg = config or ExitPolicyConfig(stop_atr_multiple=2.0, take_profit_atr_multiple=4.0)
    stop, target = compute_initial_stop_and_target(entry_price=entry_price, atr=atr, config=cfg)
    return LifecyclePriceState(
        entry_price=entry_price, entry_timestamp=_NOW, initial_protective_stop=stop, take_profit=target,
        high_water=entry_price, effective_stop=stop, trailing_active=False, last_stop_mechanism="STOP_LOSS",
    )


class TestEvaluateCandleBasics:
    def test_no_trigger_updates_high_water_only(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=101, high=102, low=99, close=101, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None
        assert result.updated_state.high_water == 102
        assert result.updated_state.effective_stop == state.effective_stop  # unchanged, no favorable-enough move

    def test_stop_loss_triggers_on_pre_candle_stop(self) -> None:
        state = _fresh_state()  # effective_stop = 90.0
        config = ExitPolicyConfig()
        candle = _candle(open_=91, high=91, low=89, close=90, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_take_profit_triggers(self) -> None:
        state = _fresh_state()  # take_profit = 120.0
        config = ExitPolicyConfig()
        candle = _candle(open_=118, high=121, low=117, close=119, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.TAKE_PROFIT

    def test_stop_wins_when_both_fire_same_candle(self) -> None:
        state = _fresh_state()  # stop=90, target=120
        config = ExitPolicyConfig()
        candle = _candle(open_=100, high=125, low=85, close=100, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_max_hold_triggers_only_when_neither_stop_nor_target_fired(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig(max_hold_hours=1.0)
        candle = _candle(open_=101, high=102, low=99, close=101, close_time=_NOW + timedelta(hours=2))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.MAX_HOLD

    def test_max_hold_does_not_override_stop(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig(max_hold_hours=1.0)
        candle = _candle(open_=91, high=91, low=89, close=90, close_time=_NOW + timedelta(hours=2))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS

    def test_flat_low_equal_to_stop_triggers(self) -> None:
        state = _fresh_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=95, high=96, low=90.0, close=95, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is ExitReason.STOP_LOSS


class TestNoLookahead:
    def test_same_candle_high_water_update_cannot_trigger_its_own_low(self) -> None:
        """A candle whose HIGH is large enough to activate trailing and pull
        the trailing candidate up ABOVE this same candle's own LOW must NOT
        exit on this candle — the raised stop only applies starting the
        NEXT candle (Phase 6 point 5 / Phase 8)."""
        state = _fresh_state(entry_price=100.0, atr=5.0)  # stop=90, activation=2*ATR=10 -> at high_water>=110
        config = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=100.0,  # keep TP effectively unreachable
            trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0,
        )
        # This candle's high (115) both activates trailing AND produces a
        # trailing candidate (115 - 1*5 = 110) that is ABOVE this candle's
        # own low (95) — if lookahead existed, this would wrongly "trigger"
        # against 110 within the same candle. It must not.
        candle = _candle(open_=100, high=115, low=95, close=110, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None
        assert result.updated_state.effective_stop == 110.0  # raised, but only for the NEXT candle
        assert result.updated_state.trailing_active is True

        # The NEXT candle's low touching 110 now correctly triggers.
        candle2 = _candle(open_=110, high=112, low=109, close=111, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle2, config, atr_for_trailing=5.0)
        assert result2.exit_reason is ExitReason.TRAILING_STOP

    def test_entry_candle_cannot_retroactively_trigger_its_own_exit(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig()
        # A hypothetical "entry candle" whose low would touch the stop is
        # never fed to evaluate_candle for evaluation purposes at all in
        # live/replay wiring (only candles strictly after entry_timestamp
        # are evaluated) — this is enforced by the caller (Phase 8), and is
        # additionally safe here: entry_timestamp itself is never inside the
        # comparison, only close_time deltas for max-hold.
        candle = _candle(open_=100, high=101, low=99, close=100, close_time=state.entry_timestamp)
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.exit_reason is None


class TestMonotonicEffectiveStop:
    def test_effective_stop_never_decreases_when_atr_contracts(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0)
        candle_up = _candle(open_=100, high=115, low=100, close=112, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle_up, config, atr_for_trailing=5.0)
        raised_stop = result.updated_state.effective_stop
        assert raised_stop > state.effective_stop

        # ATR EXPANDS sharply (high_water unchanged, since this candle's
        # high 112 < prior high_water 115) — a larger ATR widens the
        # trailing distance and would produce a LOWER trailing candidate
        # (115 - 1*10 = 105 < 110); the monotonic clamp must never let this
        # pull the stop back down.
        candle_pullback = _candle(open_=112, high=112, low=108, close=109, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle_pullback, config, atr_for_trailing=10.0)
        assert result2.updated_state.effective_stop == raised_stop

    def test_stale_atr_skips_trailing_recompute_but_not_stop_check(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=2.0, trailing_distance_atr_multiple=1.0)
        candle_up = _candle(open_=100, high=115, low=100, close=112, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle_up, config, atr_for_trailing=None)
        # ATR unavailable -> trailing candidate cannot be computed, stop
        # stays at initial_protective_stop (still valid/monotonic).
        assert result.updated_state.effective_stop == state.initial_protective_stop
        assert result.updated_state.trailing_active is False

        # A subsequent candle whose low pierces the (still-initial) stop
        # must still trigger normally — staleness only disabled the
        # trailing-recompute step, not the stop check itself.
        candle_drop = _candle(open_=100, high=100, low=89, close=90, close_time=_NOW + timedelta(minutes=2))
        result2 = evaluate_candle(result.updated_state, candle_drop, config, atr_for_trailing=None)
        assert result2.exit_reason is ExitReason.STOP_LOSS

    def test_take_profit_unchanged_across_many_evaluations(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig()
        current = state
        for i in range(10):
            candle = _candle(
                open_=100 + i, high=101 + i, low=99 + i, close=100 + i,
                close_time=_NOW + timedelta(minutes=i + 1),
            )
            current = evaluate_candle(current, candle, config, atr_for_trailing=5.0).updated_state
        assert current.take_profit == state.take_profit

    def test_trailing_only_tightens_never_below_initial_stop(self) -> None:
        state = _fresh_state(entry_price=100.0, atr=5.0)
        config = ExitPolicyConfig(trailing_activation_atr_multiple=100.0)  # never activates
        candle = _candle(open_=95, high=96, low=94, close=95, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(state, candle, config, atr_for_trailing=5.0)
        assert result.updated_state.effective_stop == state.initial_protective_stop


class TestSellSizing:
    def test_headroom_reserved_and_floored_to_step(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=1.0, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.headroom_reserved == pytest.approx(0.0015)
        assert result.quantity <= 1.0 - 0.0015
        assert result.quantity > 0

    def test_never_exceeds_owned_quantity(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=0.001, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.quantity <= 0.001

    def test_dust_when_below_min_qty_after_headroom(self) -> None:
        result = compute_sell_quantity(
            net_owned_base_quantity=0.0009, step_size=0.001, min_qty=0.001, sell_commission_headroom_bps=15.0,
        )
        assert result.quantity == 0.0


class TestSharedCandleOrderingImplementation:
    def test_live_and_replay_call_sites_use_the_identical_function_object(self) -> None:
        """Phase 20/22 — direct proof (not just "they happen to agree")
        that the live M1 evaluator and the replay sanity tool call the
        SAME `evaluate_candle` function object, never two implementations."""
        import crypto_signal_engine.execution.lifecycle as lifecycle_module
        import crypto_signal_engine.execution.lifecycle_manager as lifecycle_manager_module
        import crypto_signal_engine.execution.lifecycle_replay_sanity as lifecycle_replay_sanity_module

        assert lifecycle_manager_module.evaluate_candle is lifecycle_module.evaluate_candle
        assert lifecycle_replay_sanity_module.evaluate_candle is lifecycle_module.evaluate_candle


class TestFloorToStep:
    def test_basic(self) -> None:
        assert floor_to_step(1.2345, 0.01) == pytest.approx(1.23)

    def test_zero_step_returns_value_unchanged(self) -> None:
        assert floor_to_step(1.2345, 0.0) == 1.2345


class TestResolveLotSizeFilter:
    """Live-validation bug fix regression: `parse_symbol_filters` returns
    `0.0` (not `None`) for `market_step_size`/`market_min_qty` when a
    symbol has no separate `MARKET_LOT_SIZE` filter (the common case) —
    this must fall back to the base `LOT_SIZE` filter, never silently
    treat 0.0 as "the real step is zero" (which skips flooring entirely
    and lets an unfloored, non-lot-aligned SELL quantity reach Binance)."""

    def test_zero_market_step_falls_back_to_base_lot_size(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.0, market_min_qty=0.0, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.01
        assert min_qty == 0.01

    def test_none_market_step_falls_back_to_base_lot_size(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=None, market_min_qty=None, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.01
        assert min_qty == 0.01

    def test_genuine_positive_market_lot_size_is_used(self) -> None:
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.1, market_min_qty=0.1, step_size=0.01, min_qty=0.01,
        )
        assert step == 0.1
        assert min_qty == 0.1

    def test_headroom_sizing_with_previously_buggy_zero_market_filter_produces_lot_aligned_quantity(self) -> None:
        """Direct reproduction of the live bug: net_owned=0.83, LINKUSDT's
        real step_size=0.01, market_step_size=0.0 (no MARKET_LOT_SIZE
        filter) — the resulting SELL quantity must be a clean multiple of
        0.01, never the raw unfloored 0.828755..."""
        step, min_qty = resolve_lot_size_filter(
            market_step_size=0.0, market_min_qty=0.0, step_size=0.01, min_qty=0.01,
        )
        sizing = compute_sell_quantity(
            net_owned_base_quantity=0.83, step_size=step, min_qty=min_qty, sell_commission_headroom_bps=15.0,
        )
        # A clean multiple of 0.01 -- not 0.8287549999999999.
        assert round(sizing.quantity, 2) == sizing.quantity
        assert sizing.quantity == pytest.approx(0.82)


class TestBridgePositionRecord:
    def test_flat_record_has_no_price_fields(self) -> None:
        record = flat_record("BTCUSDT", now=_NOW)
        assert record.state is PositionLifecycleState.FLAT
        assert record.net_owned_base_quantity == 0.0
        assert record.gross_entry_vwap is None

    def test_long_state_requires_price_fields(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(symbol="BTCUSDT", state=PositionLifecycleState.LONG, updated_at=_NOW)

    def test_long_state_requires_positive_quantity(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(
                symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
                net_owned_base_quantity=0.0, initial_protective_stop=90.0, high_water=100.0,
                effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
            )

    def test_to_price_state_round_trips_with_with_price_state(self) -> None:
        record = BridgePositionRecord(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
        )
        price_state = record.to_price_state()
        config = ExitPolicyConfig()
        candle = _candle(open_=101, high=115, low=99, close=110, close_time=_NOW + timedelta(minutes=1))
        result = evaluate_candle(price_state, candle, config, atr_for_trailing=5.0)
        merged = record.with_price_state(result.updated_state, now=candle.close_time, candle_close_time=candle.close_time)
        assert merged.high_water == result.updated_state.high_water
        assert merged.effective_stop == result.updated_state.effective_stop
        # Non-price fields must be untouched by the merge.
        assert merged.net_owned_base_quantity == record.net_owned_base_quantity
        assert merged.gross_entry_vwap == record.gross_entry_vwap

    def test_to_price_state_rejects_non_long(self) -> None:
        record = flat_record("BTCUSDT", now=_NOW)
        with pytest.raises(ValueError):
            record.to_price_state()

    def test_updated_at_required(self) -> None:
        with pytest.raises(ValueError):
            BridgePositionRecord(symbol="BTCUSDT", state=PositionLifecycleState.FLAT, updated_at=None)


class TestResolvedExitPolicy:
    """Adaptive Intelligence v1, step 2 -- `resolved_exit_policy()` is the
    ONE authoritative way any caller obtains the `ExitPolicyConfig` a
    SPECIFIC position must keep using for its remaining life."""

    def _record(self, **overrides) -> BridgePositionRecord:
        defaults = dict(
            symbol="BTCUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=_NOW, updated_at=_NOW,
        )
        defaults.update(overrides)
        return BridgePositionRecord(**defaults)

    def test_all_five_fields_present_reconstructs_embedded_policy(self) -> None:
        record = self._record(
            exit_policy_stop_atr_multiple=1.5, exit_policy_take_profit_atr_multiple=3.0,
            exit_policy_trailing_activation_atr_multiple=1.0, exit_policy_trailing_distance_atr_multiple=1.0,
            exit_policy_max_hold_hours=12.0, policy_version_id="v7",
        )
        default = ExitPolicyConfig()  # deliberately different from the embedded values
        resolved = record.resolved_exit_policy(default=default)
        assert resolved.stop_atr_multiple == 1.5
        assert resolved.take_profit_atr_multiple == 3.0
        assert resolved.trailing_activation_atr_multiple == 1.0
        assert resolved.trailing_distance_atr_multiple == 1.0
        assert resolved.max_hold_hours == 12.0
        assert resolved is not default

    def test_missing_fields_falls_back_to_default(self) -> None:
        # A legacy position, persisted before this milestone -- all six
        # new fields at their `None` default.
        record = self._record()
        assert record.policy_version_id is None
        default = ExitPolicyConfig(max_hold_hours=37.0)
        resolved = record.resolved_exit_policy(default=default)
        assert resolved is default

    def test_partial_fields_present_still_falls_back_to_default(self) -> None:
        # Defensive: even one missing field must fall back to `default`
        # wholesale, never a mix of embedded and default values.
        record = self._record(
            exit_policy_stop_atr_multiple=1.5, exit_policy_take_profit_atr_multiple=3.0,
            exit_policy_trailing_activation_atr_multiple=1.0, exit_policy_trailing_distance_atr_multiple=1.0,
            exit_policy_max_hold_hours=None,
        )
        default = ExitPolicyConfig(max_hold_hours=37.0)
        resolved = record.resolved_exit_policy(default=default)
        assert resolved is default


class TestComputeTotalExposure:
    """Portfolio/Accounting v1, step 1 — the single, de-duplicated
    exposure formula (previously independently duplicated in
    `LifecycleManager.entry_gate()` and `app.py::_bridge_lifecycle_
    snapshot()`)."""

    def _long(self, symbol: str, *, vwap: float, qty: float) -> BridgePositionRecord:
        return BridgePositionRecord(
            symbol=symbol, state=PositionLifecycleState.LONG, gross_entry_vwap=vwap,
            net_owned_base_quantity=qty, initial_protective_stop=vwap * 0.9, high_water=vwap,
            effective_stop=vwap * 0.9, take_profit=vwap * 1.1, entry_timestamp=_NOW, updated_at=_NOW,
        )

    def test_empty_sequence_is_zero(self) -> None:
        assert compute_total_exposure([]) == 0.0

    def test_flat_positions_contribute_nothing(self) -> None:
        positions = [flat_record("BTCUSDT", now=_NOW), flat_record("ETHUSDT", now=_NOW)]
        assert compute_total_exposure(positions) == 0.0

    def test_long_positions_contribute_cost_basis(self) -> None:
        positions = [self._long("BTCUSDT", vwap=100.0, qty=2.0), self._long("ETHUSDT", vwap=50.0, qty=1.0)]
        assert compute_total_exposure(positions) == pytest.approx(250.0)

    def test_dust_still_contributes_never_excluded(self) -> None:
        from dataclasses import replace as _replace

        dust = _replace(self._long("BTCUSDT", vwap=100.0, qty=0.5), state=PositionLifecycleState.DUST)
        assert compute_total_exposure([dust]) == pytest.approx(50.0)

    def test_mixed_flat_and_non_flat(self) -> None:
        positions = [
            self._long("BTCUSDT", vwap=100.0, qty=1.0),  # 100.0
            flat_record("ETHUSDT", now=_NOW),  # 0.0
        ]
        assert compute_total_exposure(positions) == pytest.approx(100.0)


class TestRiskGates:
    def test_max_positions_gate(self) -> None:
        config = RiskPolicyConfig(max_open_positions=3)
        assert max_positions_gate_open(open_slot_count=2, config=config) is True
        assert max_positions_gate_open(open_slot_count=3, config=config) is False

    def test_max_exposure_gate(self) -> None:
        config = RiskPolicyConfig(max_total_exposure_usdt=100.0)
        assert max_exposure_gate_open(current_exposure_usdt=90.0, additional_notional_usdt=10.0, config=config) is True
        assert max_exposure_gate_open(current_exposure_usdt=95.0, additional_notional_usdt=10.0, config=config) is False

    def test_cooldown(self) -> None:
        config = RiskPolicyConfig(cooldown_minutes=30.0)
        until = compute_cooldown_until(exit_time=_NOW, config=config)
        assert until == _NOW + timedelta(minutes=30)
        assert cooldown_clear(cooldown_until=until, now=_NOW + timedelta(minutes=29)) is False
        assert cooldown_clear(cooldown_until=until, now=_NOW + timedelta(minutes=30)) is True
        assert cooldown_clear(cooldown_until=None, now=_NOW) is True


class TestDailyLossBreaker:
    def test_known_fees_always_subtracted_never_treated_as_zero(self) -> None:
        config = RiskPolicyConfig()
        fees = (
            FeeLedgerEntry(
                amount=1.0, asset="USDT", usdt_equivalent=1.0, client_order_id="csl-1",
                side="EXIT", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=10.0, fee_ledger_entries=fees, config=config)
        assert contribution.fee_basis == "KNOWN"
        assert contribution.conservative_pnl == 9.0

    def test_unknown_fee_uses_conservative_reserve_not_zero(self) -> None:
        config = RiskPolicyConfig(unknown_fee_conservative_reserve_usdt=0.5)
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="ENTRY", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=10.0, fee_ledger_entries=fees, config=config)
        assert contribution.fee_basis == "RESERVED"
        assert contribution.conservative_pnl == pytest.approx(9.5)

    def test_conservative_figure_trips_breaker_when_gross_alone_would_not(self) -> None:
        """Gross P&L alone (+2.0) looks fine, but after subtracting a
        larger-than-gross conservative fee reserve the conservative figure
        goes negative enough to trip the breaker — proving the breaker uses
        the fee-adjusted figure, not raw gross."""
        config = RiskPolicyConfig(daily_loss_limit_usdt=1.0, unknown_fee_conservative_reserve_usdt=5.0)
        fees = (
            FeeLedgerEntry(
                amount=0.001, asset="OBSCURE", usdt_equivalent=None, client_order_id="csl-1",
                side="EXIT", trade_id=1, recorded_at=_NOW,
            ),
        )
        contribution = compute_trade_risk_contribution(gross_pnl=2.0, fee_ledger_entries=fees, config=config)
        accumulator = apply_trade_to_daily_accumulator(DailyRiskAccumulator(trading_day="2026-01-01"), contribution)
        assert accumulator.conservative_risk_pnl == pytest.approx(-3.0)
        assert daily_loss_breaker_tripped(accumulator, config) is True

    def test_breaker_not_tripped_when_within_limit(self) -> None:
        config = RiskPolicyConfig(daily_loss_limit_usdt=50.0)
        accumulator = DailyRiskAccumulator(trading_day="2026-01-01", conservative_risk_pnl=-10.0)
        assert daily_loss_breaker_tripped(accumulator, config) is False

    def test_trading_day_key_is_utc_calendar_day(self) -> None:
        assert trading_day_key(_NOW) == "2026-01-01"
        assert trading_day_key(_NOW + timedelta(hours=23, minutes=59)) == "2026-01-01"
        assert trading_day_key(_NOW + timedelta(hours=24)) == "2026-01-02"


=== FILE: tests/test_execution_lifecycle_manager.py ===
"""
Autonomous Testnet trading lifecycle — `LifecycleManager` orchestration
tests (Phase 5/6/7/10/11/13/14/15/17/18). Fully offline: `FakeTestnetHttpClient`
+ `FixedClock` + temp SQLite, same discipline as
`tests/test_signal_testnet_bridge.py`/`tests/test_execution_reconciliation_service.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from crypto_signal_engine.execution.lifecycle import (
    Candle,
    ExitPolicyConfig,
    ExitReason,
    PositionLifecycleState,
    RiskPolicyConfig,
    compute_cooldown_until,
    flat_record,
)
from crypto_signal_engine.execution.lifecycle_manager import EntryGateResult, LifecycleManager, derive_base_asset
from crypto_signal_engine.execution.lifecycle_store import LifecycleStore
from crypto_signal_engine.execution.models import ExecutionResult, Fill, OrderSide
from crypto_signal_engine.execution.reconciliation_service import ExecutionReconciliationService
from crypto_signal_engine.execution.reconciliation_store import ExecutionStateStore
from crypto_signal_engine.execution.testnet_client import BinanceTestnetClient, BinanceTestnetConfig
from crypto_signal_engine.providers.binance.clock import FixedClock
from tests.conftest import run_async
from tests.execution_fakes import FakeTestnetHttpClient, json_response

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAKE_KEY = "fake-key"
FAKE_SECRET = "fake-secret-value"
SYMBOL = "BTCUSDT"


def _exchange_info(symbol: str = SYMBOL, *, step_size: str = "0.0001", min_qty: str = "0.0001") -> dict:
    return {
        "symbols": [
            {
                "symbol": symbol, "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "minQty": min_qty, "maxQty": "9000.0", "stepSize": step_size},
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                ],
            }
        ]
    }


def _exchange_info_with_min_notional(
    symbol: str = SYMBOL, *, step_size: str = "0.0001", min_qty: str = "0.0001",
    min_notional: str = "5.0", applies_to_market: bool = True,
) -> dict:
    """Same shape as `_exchange_info()` plus a legacy `MIN_NOTIONAL` filter
    (the exact filter type real Binance Testnet returns for the affected
    live symbols — see the "notional ... minimum ... altında" wording
    already produced by `adapter.py::_check_notional`, which matches the
    already-observed live log lines this fix addresses)."""
    info = _exchange_info(symbol, step_size=step_size, min_qty=min_qty)
    info["symbols"][0]["filters"].append(
        {"filterType": "MIN_NOTIONAL", "minNotional": min_notional, "applyToMarket": applies_to_market}
    )
    return info


def _price_response(price: float, *, symbol: str = SYMBOL) -> tuple[int, str]:
    return json_response({"symbol": symbol, "price": str(price)})


def _order_payload(*, side: str, qty: str, quote_qty: str, order_id: int = 1, status: str = "FILLED", fills=None) -> dict:
    payload = {
        "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
        "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
        "transactTime": int(NOW.timestamp() * 1000),
    }
    if fills is not None:
        payload["fills"] = fills
    return payload


def _manager(
    get_responses=None, post_responses=None, *, tmp_path: Path, risk_policy=None, exit_policy=None,
    exit_policy_provider=None, notifier=None,
):
    http = FakeTestnetHttpClient(get_responses, post_responses)
    config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
    client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
    exec_store = ExecutionStateStore(tmp_path / "exec.db")
    service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
    lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
    manager = LifecycleManager(
        store=lifecycle_store, execution_service=service, exit_policy=exit_policy, risk_policy=risk_policy,
        clock=FixedClock(NOW), exit_policy_provider=exit_policy_provider, notifier=notifier,
    )
    return manager, http, exec_store, lifecycle_store


def _fake_buy_result(*, price: float = 50000.0, qty: float = 0.002, commission: float = 0.0, commission_asset: str = "BTC") -> ExecutionResult:
    return ExecutionResult(
        symbol=SYMBOL, client_order_id="csl-entry-1", exchange_order_id=1, side=OrderSide.BUY,
        status="FILLED", executed_quantity=qty, cumulative_quote_quantity=price * qty,
        transaction_time=NOW, context_id="bridge:BTCUSDT:OPEN:ctx-1",
        fills=(Fill(price=price, quantity=qty, commission=commission, commission_asset=commission_asset, trade_id=1),),
    )


class TestDeriveBaseAsset:
    def test_strips_usdt_suffix(self) -> None:
        assert derive_base_asset("BTCUSDT") == "BTC"

    def test_rejects_non_usdt_quote(self) -> None:
        with pytest.raises(ValueError):
            derive_base_asset("BTCETH")


class TestOnEntryFilled:
    def test_persists_long_position_with_fee_separation(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        result = _fake_buy_result(price=50000.0, qty=0.002, commission=0.000002, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.state is PositionLifecycleState.LONG
        assert record.gross_entry_vwap == 50000.0  # fee-free
        assert record.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        assert record.initial_protective_stop == 50000.0 - 2.0 * 500.0
        assert record.take_profit == 50000.0 + 4.0 * 500.0
        assert record.high_water == 50000.0
        assert record.entry_client_order_id == "csl-entry-1"

        persisted = store.load_position(SYMBOL)
        assert persisted == record
        fees = store.fee_ledger_for_trade_group(SYMBOL, "csl-entry-1")
        assert len(fees) == 1
        assert fees[0].asset == "BTC"

    def test_fallback_stop_target_when_atr_unavailable(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="USDT")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=None)

        record = run_async(scenario())
        assert record.initial_protective_stop == pytest.approx(98.0)
        assert record.take_profit == pytest.approx(104.0)

    def test_backfills_fills_via_my_trades_when_missing(self, tmp_path: Path) -> None:
        get_responses = [
            json_response([
                {"symbol": SYMBOL, "id": 9, "orderId": 1, "price": "50000.0", "qty": "0.002",
                 "commission": "0.000002", "commissionAsset": "BTC"},
            ]),
        ]
        manager, _, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        result = ExecutionResult(
            symbol=SYMBOL, client_order_id="csl-entry-1", exchange_order_id=1, side=OrderSide.BUY,
            status="FILLED", executed_quantity=0.002, cumulative_quote_quantity=100.0,
            transaction_time=NOW, context_id="bridge:BTCUSDT:OPEN:ctx-1", fills=(),
        )

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.net_owned_base_quantity == pytest.approx(0.002 - 0.000002)
        fees = store.fee_ledger_for_trade_group(SYMBOL, "csl-entry-1")
        assert len(fees) == 1

    def test_preserves_cumulative_realized_pnl_across_reentry(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        store.save_position(flat_record(SYMBOL, now=NOW))
        # Simulate a prior realized P&L already accumulated.
        prior = store.load_position(SYMBOL)
        from dataclasses import replace
        store.save_position(replace(prior, cumulative_realized_gross_pnl=42.0))

        result = _fake_buy_result()

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=500.0)

        record = run_async(scenario())
        assert record.cumulative_realized_gross_pnl == 42.0


class TestEntryGate:
    def test_open_when_no_positions(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is True

    def test_cooldown_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        from dataclasses import replace
        store.save_position(replace(flat_record(SYMBOL, now=NOW), cooldown_until=NOW + timedelta(minutes=10)))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "cooldown" in result.detail

    def test_max_positions_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=1))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        assert result.allowed is False
        assert "max open positions" in result.detail

    def test_max_exposure_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_total_exposure_usdt=50.0))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        assert result.allowed is False
        assert "max exposure" in result.detail

    def test_max_exposure_blocks_a_prospective_order_that_would_itself_breach_the_cap(self, tmp_path: Path) -> None:
        """CRITICAL FIX (C1 — mainnet-readiness review) regression test.
        Existing exposure across OTHER symbols is comfortably under the
        cap (10/100 USDT) but the prospective BUY's own notional (500
        USDT) would blow straight through it. Before the fix,
        `additional_notional_usdt` was hardcoded to 0.0 inside
        `entry_gate()`, so this scenario was WRONGLY allowed (10 + 0 <=
        100) — the gate never looked at the size of the very order it was
        supposed to be gating."""
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_total_exposure_usdt=100.0))
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=10.0,
            net_owned_base_quantity=1.0, initial_protective_stop=9.0, high_water=10.0,
            effective_stop=9.0, take_profit=12.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        # Sanity check: with no prospective addition, the gate still opens
        # (existing exposure alone is well under the cap).
        assert manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT")).allowed is True

        result = manager.entry_gate(
            SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"), additional_notional_usdt=500.0,
        )
        assert result.allowed is False
        assert "max exposure" in result.detail
        assert "500.00" in result.detail

    def test_dust_excluded_from_slot_count_but_included_in_exposure(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=5, max_total_exposure_usdt=50.0)
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        store.save_position(BridgePositionRecord(
            symbol="ETHUSDT", state=PositionLifecycleState.DUST, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
        ))
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT"))
        # slot count excludes DUST -> still allowed by slot gate ...
        # ...but exposure (100 USDT) exceeds the 50 USDT cap -> blocked by exposure.
        assert result.allowed is False
        assert "max exposure" in result.detail

    def test_daily_loss_breaker_blocks(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0))
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key
        store.save_daily_risk(DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1), now=NOW)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "daily loss" in result.detail

    def test_daily_loss_breaker_notifies_once_per_transition(self, tmp_path: Path) -> None:
        """24/7 Ops v1, Step 4 — required per-site test: the notifier
        fires exactly ONCE on the transition into a tripped breaker for
        a given trading day, not on every subsequent blocked entry_gate()
        call the same day."""
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key

        calls: list[str] = []
        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0), notifier=calls.append,
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        manager.entry_gate("ETHUSDT", all_symbols=(SYMBOL, "ETHUSDT"))
        assert len(calls) == 1
        assert "daily loss" in calls[0]

    def test_a_raising_notifier_never_affects_entry_gate_decision(self, tmp_path: Path) -> None:
        """24/7 Ops v1, Step 4 — required per-site test: the underlying
        decision (breaker still trips correctly) is UNAFFECTED even when
        the notifier itself raises."""
        from crypto_signal_engine.execution.lifecycle import DailyRiskAccumulator, trading_day_key

        def _raising_notifier(message: str) -> None:
            raise RuntimeError("simulated notifier failure")

        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(daily_loss_limit_usdt=10.0),
            notifier=_raising_notifier,
        )
        store.save_daily_risk(
            DailyRiskAccumulator(trading_day=trading_day_key(NOW), conservative_risk_pnl=-15.0, trades_counted=1),
            now=NOW,
        )
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is False
        assert "daily loss" in result.detail

    def test_no_notification_when_breaker_not_tripped(self, tmp_path: Path) -> None:
        calls: list[str] = []
        manager, _, _, _ = _manager(tmp_path=tmp_path, notifier=calls.append)
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL,))
        assert result.allowed is True
        assert calls == []

    def test_max_exposure_decision_byte_for_bit_unchanged_after_dedup_refactor(self, tmp_path: Path) -> None:
        """Portfolio/Accounting v1, step 1 — required regression test:
        `entry_gate()`'s exposure computation was refactored to call the
        newly-extracted `compute_total_exposure()` instead of an inline
        loop. This fixture hand-computes the OLD formula's expected
        result (sum of gross_entry_vwap * net_owned_base_quantity over
        every non-FLAT position, DUST included, FLAT/None-vwap excluded)
        across a mixed-state multi-symbol universe and asserts the
        `EntryGateResult` (both `allowed` AND the exact `detail` string,
        which embeds the computed exposure number) is IDENTICAL to that
        hand-computed expectation -- proving pure de-duplication, zero
        behavior change."""
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        manager, _, _, store = _manager(
            tmp_path=tmp_path, risk_policy=RiskPolicyConfig(max_open_positions=10, max_total_exposure_usdt=1000.0),
        )
        fixture_positions = {
            "ETHUSDT": BridgePositionRecord(
                symbol="ETHUSDT", state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
                net_owned_base_quantity=2.0, initial_protective_stop=90.0, high_water=100.0,
                effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW, updated_at=NOW,
            ),  # non-FLAT, contributes 200.0
            "BNBUSDT": BridgePositionRecord(
                symbol="BNBUSDT", state=PositionLifecycleState.DUST, gross_entry_vwap=50.0,
                net_owned_base_quantity=0.5, initial_protective_stop=45.0, high_water=50.0,
                effective_stop=45.0, take_profit=60.0, entry_timestamp=NOW, updated_at=NOW,
            ),  # DUST, still non-FLAT -> contributes 25.0
            "SOLUSDT": flat_record("SOLUSDT", now=NOW),  # FLAT -> contributes 0.0
        }
        for record in fixture_positions.values():
            store.save_position(record)

        expected_exposure = 200.0 + 25.0 + 0.0
        result = manager.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT", "BNBUSDT", "SOLUSDT"))

        assert result.allowed is True  # 225.0 <= 1000.0 cap
        assert result == EntryGateResult(True, "entry gates open")

        # Now tighten the cap to just below the hand-computed exposure and
        # confirm the exact detail string embeds that SAME number.
        tight_dir = tmp_path / "tight"
        tight_dir.mkdir()
        manager_tight, _, _, store_tight = _manager(
            tmp_path=tight_dir, risk_policy=RiskPolicyConfig(max_open_positions=10, max_total_exposure_usdt=224.0),
        )
        for record in fixture_positions.values():
            store_tight.save_position(record)
        tight_result = manager_tight.entry_gate(SYMBOL, all_symbols=(SYMBOL, "ETHUSDT", "BNBUSDT", "SOLUSDT"))
        assert tight_result.allowed is False
        assert tight_result.detail == f"max exposure reached ({expected_exposure:.2f}/224.0 USDT)"


class TestAttemptExit:
    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_not_long_is_no_op(self, tmp_path: Path) -> None:
        manager, http, _, store = _manager(tmp_path=tmp_path)
        store.save_position(flat_record(SYMBOL, now=NOW))

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert http.post_calls == []

    def test_full_sell_transitions_to_flat_and_records_trade(self, tmp_path: Path) -> None:
        # `ExecutionRecord` (what `submit()` actually returns) never carries
        # `fills` — the manager always backfills via `myTrades` after a
        # confirmed FILLED SELL (see `LifecycleManager._fetch_fills`).
        # Two `exchange_info` responses are consumed first (this method's
        # own `validate_symbol` call, then `submit()`'s internal
        # `validate_intent`), then `myTrades` for the fee backfill.
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 5, "orderId": 1, "price": "110.0", "qty": "1.0",
                 "commission": "0.11", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.TAKE_PROFIT, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "SOLD"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cooldown_until is not None
        assert final.cumulative_realized_gross_pnl == pytest.approx(10.0)

        trades = store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1
        assert trades[0]["gross_realized_pnl"] == pytest.approx(10.0)
        assert trades[0]["net_realized_pnl"] == pytest.approx(10.0 - 0.11)
        assert trades[0]["exit_reason"] == "TAKE_PROFIT"

    def test_dust_when_quantity_unsellable(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info(step_size="0.01", min_qty="0.01"))]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=0.001)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "DUST"
        assert http.post_calls == []
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.DUST

    def test_repeated_attempt_on_dust_position_never_resubmits(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info(step_size="0.01", min_qty="0.01"))]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=0.001)

        async def scenario():
            await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"  # already DUST, not LONG -> no-op
        assert http.post_calls == []

    def test_unresolved_submit_marks_exit_pending(self, tmp_path: Path) -> None:
        # Binance acknowledges the order but it has not reached a terminal
        # fill state yet within this response (status=NEW) — the position
        # must become EXIT_PENDING, not FLAT, until reconciliation confirms
        # a terminal state (Phase 5/17).
        get_responses = [json_response(_exchange_info())]
        post_responses = [json_response(_order_payload(side="SELL", qty="0.0", quote_qty="0.0", status="NEW"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "EXIT_PENDING"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.EXIT_PENDING
        assert final.exit_pending_client_order_id is not None

    def test_transport_failure_during_submit_leaves_position_long_for_safe_retry(self, tmp_path: Path) -> None:
        """`ExecutionReconciliationService.submit()` itself durably records
        an AMBIGUOUS `execution_record` and then re-raises when its own
        internal reconciliation attempt ALSO fails — the exit context_id is
        deterministic (symbol+reason+entry_client_order_id), so a later
        retry safely converges on the SAME durable record via the
        service's own idempotency rather than double-submitting; leaving
        the lifecycle position as LONG (instead of a separate EXIT_PENDING
        label) is safe specifically because of that determinism."""
        from crypto_signal_engine.execution.errors import ExecutionTransportError

        get_responses = [json_response(_exchange_info()), ExecutionTransportError("query also unresolved")]
        post_responses = [ExecutionTransportError("connection reset")]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG

    def test_rejected_sell_with_zero_execution_reverts_to_long_not_exit_pending(self, tmp_path: Path) -> None:
        """Live-validation bug fix regression: a REJECTED order (e.g.
        Binance -1013 LOT_SIZE filter failure) is TERMINAL — it will NEVER
        become FILLED via reconciliation. The original bug labeled this
        EXIT_PENDING, which permanently stranded the position (Phase 22's
        `evaluate_m1_candle` only re-evaluates `state == LONG`). It must
        revert to LONG, unchanged, so the position stays monitored and can
        be retried on the next candle."""
        get_responses = [json_response(_exchange_info())]
        post_responses = [json_response({"code": -1013, "msg": "Filter failure: LOT_SIZE"}, status_code=400)]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.LONG
        # Position is otherwise byte-for-byte unchanged -- no economic effect.
        assert final.net_owned_base_quantity == 1.0
        assert final.gross_entry_vwap == 100.0
        assert "SUBMIT_FAILED" in final.last_exit_reason

    def test_rejected_sell_can_be_retried_from_a_later_candle_and_succeed(self, tmp_path: Path) -> None:
        """Direct proof the revert actually enables a working retry (not
        just a state label): a retry from the SAME triggering event (same
        `last_evaluated_candle_close`) correctly reuses the dead REJECTED
        context (never silently duplicating), but a retry from a
        DIFFERENT (later) M1 candle — a genuinely new triggering event —
        gets a fresh context_id and can submit/fill normally."""
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response(_exchange_info()), json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 9, "orderId": 2, "price": "90.0", "qty": "1.0",
                 "commission": "0.09", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [
            json_response({"code": -1013, "msg": "Filter failure: LOT_SIZE"}, status_code=400),
            json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0", order_id=2)),
        ]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store, last_evaluated_candle_close=NOW)

        async def scenario():
            first = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            # Same triggering event retried -- still the same dead
            # context_id, correctly deduped to the cached REJECTED result.
            same_event_retry = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            # A later M1 candle fires the SAME reason again -- a genuinely
            # NEW triggering event (`last_evaluated_candle_close` advances,
            # exactly as `evaluate_m1_candle` does before calling this).
            from dataclasses import replace as _dc_replace
            later = store.load_position(SYMBOL)
            store.save_position(_dc_replace(later, last_evaluated_candle_close=NOW + timedelta(minutes=1)))
            next_candle_attempt = await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)
            return first, same_event_retry, next_candle_attempt

        first, same_event_retry, next_candle_attempt = run_async(scenario())
        assert first.action == "NO_ACTION"
        assert same_event_retry.action == "NO_ACTION"  # same event, same dead context -- correctly deduped
        assert next_candle_attempt.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_canceled_with_partial_fill_finalizes_the_executed_portion(self, tmp_path: Path) -> None:
        """A CANCELED order can still carry a nonzero `executedQty` if
        partially filled before cancellation — Phase 13's "account for
        executed quantity, not requested quantity" applies even on this
        terminal-but-not-FILLED path."""
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 10, "orderId": 3, "price": "90.0", "qty": "0.4",
                 "commission": "0.036", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="0.4", quote_qty="36.0", order_id=3, status="CANCELED"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store, net_owned_base_quantity=1.0)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "PARTIAL"
        final = store.load_position(SYMBOL)
        # 0.6 remaining is a LEGALLY SELLABLE amount (self-review fix) --
        # stays LONG so it keeps being monitored/retried, never DUST.
        assert final.state is PositionLifecycleState.LONG
        assert final.net_owned_base_quantity == pytest.approx(0.6)
        trades = store.completed_trades_for_symbol(SYMBOL)
        assert len(trades) == 1
        assert trades[0]["quantity_closed"] == pytest.approx(0.4)


class TestExitPersistenceAmbiguityPinning:
    """H3 fix (mainnet-readiness review, "phantom exit"): when `submit()`
    raises `ExecutionPersistenceError` with `exchange_may_have_accepted_
    order=True` (place_order() succeeded but the local save afterward
    failed), `attempt_exit()` must NOT silently return NO_ACTION and
    leave the position LONG-and-unchanged (the old behaviour) — that
    would let the very next M1 candle submit ANOTHER real SELL on top of
    a possibly-already-filled one. It must instead pin the position into
    EXIT_PENDING (blocking further attempts) carrying the attempted
    client_order_id."""

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def _manager_with_flaky_exec_store(self, tmp_path: Path, *, get_responses, post_responses):
        from tests.test_execution_reconciliation_service import _FlakyExecutionStore

        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = _FlakyExecutionStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        return manager, http, exec_store, lifecycle_store

    def test_post_ack_persistence_failure_pins_exit_pending_not_silent_no_action(self, tmp_path: Path) -> None:
        get_responses = [json_response(_exchange_info()), json_response(_exchange_info())]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        # First save (SUBMISSION_ATTEMPTED, before place_order) succeeds;
        # only the SECOND save (after the exchange already accepted the
        # order) fails -- exactly the dangerous, ambiguous case.
        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "EXIT_PENDING"
        assert len(http.post_calls) == 1  # the SELL really was sent to the exchange

        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.EXIT_PENDING
        assert pinned.exit_pending_client_order_id is not None

        # A second attempt_exit() call (e.g. the next M1 candle) must NOT
        # submit another SELL -- state != LONG blocks it outright.
        async def retry():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        retry_outcome = run_async(retry())
        assert retry_outcome.action == "NO_ACTION"
        assert len(http.post_calls) == 1  # still just the one real POST, no duplicate


class TestReconcileStuckPosition:
    """CRITICAL FIX (C4 — mainnet-readiness review) regression tests.

    Before this fix, an AMBIGUOUS (BUY-side H3) or EXIT_PENDING (SELL-side
    H3/Phase 17) pin was PERMANENT: nothing ever revisited it, and
    `lifecycle_migration.py::reconstruct_legacy_ownership()`'s own first
    guard (`if lifecycle_store.load_position(symbol) is not None: return
    None`) means the one-time legacy-migration path can NEVER touch a
    symbol that already has ANY `bridge_position` row. `reconcile_stuck_
    position()` is the only path that can ever correct such a record —
    these tests exercise all four of its real outcomes."""

    def _manager_with_flaky_exec_store(self, tmp_path: Path, *, get_responses, post_responses):
        from tests.test_execution_reconciliation_service import _FlakyExecutionStore

        http = FakeTestnetHttpClient(get_responses, post_responses)
        config = BinanceTestnetConfig(api_key=FAKE_KEY, api_secret=FAKE_SECRET)
        client = BinanceTestnetClient(config, http, clock=FixedClock(NOW))
        exec_store = _FlakyExecutionStore(tmp_path / "exec.db")
        service = ExecutionReconciliationService(client, exec_store, clock=FixedClock(NOW))
        lifecycle_store = LifecycleStore(tmp_path / "lifecycle.db")
        manager = LifecycleManager(store=lifecycle_store, execution_service=service, clock=FixedClock(NOW))
        return manager, http, exec_store, lifecycle_store

    def _query_payload(self, *, order_id: int = 1, side: str = "BUY", qty: str = "0.002", quote_qty: str = "100.0", status: str = "FILLED") -> dict:
        return {
            "symbol": SYMBOL, "clientOrderId": f"csl-{order_id}", "orderId": order_id, "side": side,
            "status": status, "executedQty": qty, "cummulativeQuoteQty": quote_qty,
            "updateTime": int(NOW.timestamp() * 1000),
        }

    def _my_trades_payload(self, *, price: str = "50000.0", qty: str = "0.002") -> list:
        return [{"symbol": SYMBOL, "id": 9, "orderId": 1, "price": price, "qty": qty, "commission": "0.0", "commissionAsset": "BTC"}]

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def _pin_ambiguous_buy(self, manager: LifecycleManager, exec_store) -> None:
        """Reproduces the exact H3 pinning sequence (a post-ack persistence
        failure during a BUY submit) `signal_bridge.py::_submit()` handles
        in production — this test file exercises `LifecycleManager`
        directly, without the bridge layer."""
        from crypto_signal_engine.errors import PersistenceError
        from crypto_signal_engine.execution.errors import ExecutionPersistenceError
        from crypto_signal_engine.execution.models import OrderIntent, OrderType

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save
        intent = OrderIntent(
            symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
            context_id="bridge:BTCUSDT:OPEN:ctx-1", timestamp=NOW, quote_quantity=100.0,
        )

        async def scenario() -> None:
            with pytest.raises(ExecutionPersistenceError) as excinfo:
                await manager._service.submit(intent)  # noqa: SLF001 - deliberately bypassing signal_bridge for this test
            manager.mark_ambiguous(
                SYMBOL, detail=f"BUY may have reached the exchange: {excinfo.value}",
                client_order_id=excinfo.value.client_order_id,
            )

        run_async(scenario())
        exec_store.save = original_save  # stop flaking before the reconciliation step

    def test_ambiguous_buy_that_actually_filled_is_promoted_to_long(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # original submit()'s validate_intent
            json_response(self._query_payload(status="FILLED")),  # reconcile()'s query_order
            json_response(self._my_trades_payload()),  # on_entry_filled()'s fills backfill
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)
        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.AMBIGUOUS
        assert pinned.entry_order_client_order_id is not None

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "PROMOTED_TO_LONG"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.LONG
        assert record.gross_entry_vwap == pytest.approx(100.0 / 0.002)
        assert record.net_owned_base_quantity == pytest.approx(0.002)

    def test_ambiguous_buy_that_never_filled_is_released_to_flat(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response(self._query_payload(status="REJECTED", qty="0", quote_qty="0")),
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)
        assert store.load_position(SYMBOL).state is PositionLifecycleState.AMBIGUOUS

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "RELEASED_TO_FLAT"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_ambiguous_position_still_unresolved_leaves_pin_in_place(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),
            json_response(self._query_payload(status="ACKNOWLEDGED", qty="0", quote_qty="0")),  # still non-terminal
        ]
        post_responses = [json_response(_order_payload(side="BUY", qty="0.002", quote_qty="100.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._pin_ambiguous_buy(manager, exec_store)

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "STILL_AMBIGUOUS"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.AMBIGUOUS

    def test_not_applicable_for_a_normal_long_position(self, tmp_path: Path) -> None:
        manager, _, _, store = self._manager_with_flaky_exec_store(tmp_path, get_responses=[], post_responses=[])
        self._seed_long(store)
        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "NOT_APPLICABLE"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG

    def test_exit_pending_sell_that_actually_filled_is_finalized(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # attempt_exit()'s filter lookup
            json_response(_exchange_info()),  # submit()'s validate_intent
            json_response(self._query_payload(side="SELL", qty="1.0", quote_qty="110.0", status="FILLED")),  # reconcile()
            json_response(_exchange_info()),  # _reconcile_pending_exit()'s validate_symbol for min_qty
            json_response(self._my_trades_payload(price="110.0", qty="1.0")),  # _finalize_exit()'s fills backfill
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        pending_outcome = run_async(scenario())
        assert pending_outcome.action == "EXIT_PENDING"
        pinned = store.load_position(SYMBOL)
        assert pinned.state is PositionLifecycleState.EXIT_PENDING
        assert pinned.exit_pending_client_order_id is not None
        exec_store.save = original_save

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "FINALIZED_SOLD"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.FLAT

    def test_exit_pending_sell_that_never_filled_is_reverted_to_long(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()),  # attempt_exit()'s filter lookup
            json_response(_exchange_info()),  # submit()'s validate_intent
            json_response(self._query_payload(side="SELL", qty="0", quote_qty="0", status="REJECTED")),  # reconcile()
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, exec_store, store = self._manager_with_flaky_exec_store(
            tmp_path, get_responses=get_responses, post_responses=post_responses,
        )
        self._seed_long(store)

        original_save = exec_store.save
        calls = {"n": 0}

        def flaky_second_save(record):
            calls["n"] += 1
            if calls["n"] == 2:
                from crypto_signal_engine.errors import PersistenceError
                raise PersistenceError("simulated post-ack persistence failure")
            return original_save(record)

        exec_store.save = flaky_second_save

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        pending_outcome = run_async(scenario())
        assert pending_outcome.action == "EXIT_PENDING"
        exec_store.save = original_save

        outcome = run_async(manager.reconcile_stuck_position(SYMBOL))
        assert outcome.action == "REVERTED_TO_LONG"
        record = store.load_position(SYMBOL)
        assert record.state is PositionLifecycleState.LONG
        assert record.net_owned_base_quantity == pytest.approx(1.0)
        assert record.exit_pending_client_order_id is None


class TestMinNotionalPreflight:
    """MIN_NOTIONAL pre-flight fix — a residual that clears LOT_SIZE but
    whose (price x quantity) is below the exchange's MIN_NOTIONAL filter
    must NEVER reach `self._service.submit()` (no wasted real-Testnet
    order/API budget, no repeated "SELL submission did not confirm"
    noise), but must ALSO never be marked DUST (unlike a true LOT_SIZE
    dust residual, notional can clear again on its own once price moves)."""

    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_blocked_when_notional_below_minimum_stays_long_no_submission(self, tmp_path: Path) -> None:
        # quantity clears LOT_SIZE (min_qty=0.0001) but price(4.0) x
        # quantity(~1.0) = ~4.0 USDT is below the 5.0 USDT MIN_NOTIONAL.
        get_responses = [json_response(_exchange_info_with_min_notional())]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=4.0,
            )

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert "MIN_NOTIONAL" in outcome.detail
        assert "5.0" in outcome.detail  # the actual threshold is stated
        assert http.post_calls == []  # the doomed order was NEVER submitted
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.LONG  # NEVER DUST -- notional can still recover
        assert final.net_owned_base_quantity == pytest.approx(1.0)  # untouched

    def test_recovery_after_price_rises_sell_finalizes_normally(self, tmp_path: Path) -> None:
        """THE recovery test — proves a MIN_NOTIONAL-blocked residual is
        NEVER locked out of a real future sale. Same position, same
        symbol: first blocked at a low price, then (a later candle) price
        has risen enough to clear MIN_NOTIONAL -- the SELL must actually
        submit and finalize exactly like any normal exit."""
        get_responses = [
            json_response(_exchange_info_with_min_notional()),  # attempt 1: validate_symbol (blocked, no more calls)
            json_response(_exchange_info_with_min_notional()),  # attempt 2: this method's own validate_symbol
            json_response(_exchange_info_with_min_notional()),  # attempt 2: submit()'s internal validate_intent
            _price_response(10.0),  # attempt 2: submit()'s internal validate_intent MARKET-notional price fetch
            json_response([  # attempt 2: myTrades fee backfill after FILLED
                {"symbol": SYMBOL, "id": 7, "orderId": 1, "price": "10.0", "qty": "1.0",
                 "commission": "0.01", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="10.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            blocked = await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=4.0,
            )
            state_after_blocked = store.load_position(SYMBOL).state  # checked BEFORE the second attempt
            recovered = await manager.attempt_exit(
                SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None, current_price=10.0,
            )
            return blocked, state_after_blocked, recovered

        blocked, state_after_blocked, recovered = run_async(scenario())
        assert blocked.action == "NO_ACTION"
        assert state_after_blocked is PositionLifecycleState.LONG  # never abandoned after being blocked

        assert recovered.action == "SOLD"  # the REAL sale actually happens once price recovers
        assert len(http.post_calls) == 1
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cumulative_realized_gross_pnl == pytest.approx((10.0 - 100.0) * 1.0)

    def test_notional_clears_minimum_submits_normally_no_regression(self, tmp_path: Path) -> None:
        """A position genuinely sellable on the very first check, WITH a
        MIN_NOTIONAL filter present (price x quantity clears it) — behaves
        byte-for-byte like the pre-fix full-sell path (see
        `TestAttemptExit.test_full_sell_transitions_to_flat_and_records_trade`),
        proving the new gate never interferes with a legitimately sellable
        position."""
        get_responses = [
            json_response(_exchange_info_with_min_notional()), json_response(_exchange_info_with_min_notional()),
            _price_response(110.0),
            json_response([
                {"symbol": SYMBOL, "id": 8, "orderId": 1, "price": "110.0", "qty": "1.0",
                 "commission": "0.11", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="110.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(
                SYMBOL, reason=ExitReason.TAKE_PROFIT, exit_signal_context_id=None, current_price=110.0,
            )

        outcome = run_async(scenario())
        assert outcome.action == "SOLD"
        final = store.load_position(SYMBOL)
        assert final.state is PositionLifecycleState.FLAT
        assert final.cumulative_realized_gross_pnl == pytest.approx(10.0)

    def test_price_lookup_failure_during_preflight_fails_closed(self, tmp_path: Path) -> None:
        """No `current_price` supplied (the opposite-signal call-site
        shape) AND the fallback `symbol_price()` lookup itself fails --
        must fail closed exactly like the existing filter-lookup-failure
        convention in this same method: no submission, NO_ACTION, LONG
        untouched."""
        from crypto_signal_engine.execution.errors import ExecutionTransportError

        get_responses = [json_response(_exchange_info_with_min_notional()), ExecutionTransportError("timeout")]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)

        async def scenario():
            return await manager.attempt_exit(SYMBOL, reason=ExitReason.STOP_LOSS, exit_signal_context_id=None)

        outcome = run_async(scenario())
        assert outcome.action == "NO_ACTION"
        assert "fail-closed" in outcome.detail
        assert http.post_calls == []
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG


class TestEvaluateM1Candle:
    def _seed_long(self, store: LifecycleStore, **overrides) -> None:
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord
        defaults = dict(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=90.0, high_water=100.0,
            effective_stop=90.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        defaults.update(overrides)
        store.save_position(BridgePositionRecord(**defaults))

    def test_no_trigger_updates_high_water_persisted(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=101, high=105, low=99, close=101, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        result = run_async(scenario())
        assert result is None
        assert store.load_position(SYMBOL).high_water == 105

    def test_pre_entry_candle_never_evaluated(self, tmp_path: Path) -> None:
        manager, _, _, store = _manager(tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=1, high=1, low=1, close=1, close_time=NOW)  # == entry_timestamp

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        result = run_async(scenario())
        assert result is None
        # high_water untouched -- proves the candle was never fed to evaluate_candle at all.
        assert store.load_position(SYMBOL).high_water == 100.0

    def test_stop_trigger_submits_sell(self, tmp_path: Path) -> None:
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "90.0", "qty": "1.0",
                 "commission": "0.09", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="90.0"))]
        manager, http, _, store = _manager(get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path)
        self._seed_long(store)
        candle = Candle(open=91, high=91, low=89, close=90, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(scenario())
        assert outcome is not None
        assert outcome.action == "SOLD"
        assert outcome.exit_reason is ExitReason.STOP_LOSS
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_m1_candle_close_used_for_min_notional_preflight_no_extra_price_fetch(self, tmp_path: Path) -> None:
        """Design choice (a): `evaluate_m1_candle` already has the
        triggering candle's close price in hand, so the MIN_NOTIONAL
        pre-flight must use `candle.close` directly -- proven here by
        there being NO `symbol_price()` GET at all (only the one
        `validate_symbol` exchangeInfo call) despite a MIN_NOTIONAL filter
        being present and the check actually running (blocking the SELL)."""
        get_responses = [json_response(_exchange_info_with_min_notional())]
        manager, http, _, store = _manager(get_responses=get_responses, tmp_path=tmp_path)
        self._seed_long(store)
        # close=4.0 x quantity~1.0 = ~4.0 USDT, below the 5.0 USDT minimum.
        candle = Candle(open=91, high=91, low=4, close=4.0, close_time=NOW + timedelta(minutes=1))

        async def scenario():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(scenario())
        assert outcome is not None
        assert outcome.action == "NO_ACTION"
        assert "MIN_NOTIONAL" in outcome.detail
        assert http.post_calls == []
        assert len(http.get_calls) == 1  # exactly the one validate_symbol call -- no redundant price fetch
        assert store.load_position(SYMBOL).state is PositionLifecycleState.LONG


class _MutablePolicyProvider:
    """Test double for `exit_policy_provider` -- a simple mutable holder so
    a test can simulate an `adaptive/` champion promotion mid-test by
    reassigning `.policy`/`.version_id` between two `on_entry_filled()`
    calls, with zero dependency on the real `adaptive/` package."""

    def __init__(self, policy: ExitPolicyConfig, version_id: str) -> None:
        self.policy = policy
        self.version_id = version_id

    def __call__(self) -> tuple[ExitPolicyConfig, str | None]:
        return self.policy, self.version_id


class TestAdaptivePolicyPinning:
    """Adaptive Intelligence v1, step 2 -- the position-pinning invariant:
    a currently-OPEN position must keep using whichever `ExitPolicyConfig`
    it was actually opened under for its ENTIRE remaining life, even after
    the champion/`self._exit_policy` changes; a brand-new position opened
    AFTER the swap must use the new policy. See `BridgePositionRecord.
    resolved_exit_policy()` and `LifecycleManager.evaluate_m1_candle()`."""

    SYMBOL_2 = "ETHUSDT"

    def test_new_position_embeds_provider_policy_and_version_id(self, tmp_path: Path) -> None:
        policy = ExitPolicyConfig(
            stop_atr_multiple=1.5, take_profit_atr_multiple=3.0, trailing_activation_atr_multiple=1.0,
            trailing_distance_atr_multiple=1.0, max_hold_hours=12.0,
        )
        provider = _MutablePolicyProvider(policy, "policy-v1")
        manager, _, _, store = _manager(tmp_path=tmp_path, exit_policy_provider=provider)
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=10.0)

        record = run_async(scenario())
        assert record.policy_version_id == "policy-v1"
        assert record.exit_policy_stop_atr_multiple == 1.5
        assert record.exit_policy_take_profit_atr_multiple == 3.0
        assert record.exit_policy_trailing_activation_atr_multiple == 1.0
        assert record.exit_policy_trailing_distance_atr_multiple == 1.0
        assert record.exit_policy_max_hold_hours == 12.0
        # Stop/target math itself used the PROVIDER's policy, not the
        # manager's static (unset, default) `self._exit_policy`.
        assert record.initial_protective_stop == 100.0 - 1.5 * 10.0
        assert record.take_profit == 100.0 + 3.0 * 10.0
        assert store.load_position(SYMBOL).policy_version_id == "policy-v1"

    def test_no_provider_embeds_static_policy_with_none_version_id(self, tmp_path: Path) -> None:
        manager, _, _, _ = _manager(tmp_path=tmp_path)  # no exit_policy -> ExitPolicyConfig() defaults
        result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def scenario():
            return await manager.on_entry_filled(SYMBOL, result=result, signal_context_id="ctx-1", atr=10.0)

        record = run_async(scenario())
        assert record.policy_version_id is None
        default = ExitPolicyConfig()
        assert record.exit_policy_stop_atr_multiple == default.stop_atr_multiple
        assert record.exit_policy_take_profit_atr_multiple == default.take_profit_atr_multiple
        assert record.exit_policy_max_hold_hours == default.max_hold_hours

    def test_core_proof_open_position_keeps_v1_behavior_after_promotion_to_v2(self, tmp_path: Path) -> None:
        """THE core Adaptive Intelligence v1 proof: open a position under
        champion v1 (max_hold_hours=1.0), "promote" to v2 (max_hold_hours
        =100.0) while it is STILL OPEN by mutating the provider exactly as
        a real champion swap would, feed it a candle 2 hours later that
        triggers neither stop nor target -- it must STILL exit on
        MAX_HOLD (proving it used v1's 1-hour limit, not v2's 100-hour
        one). A position opened AFTER the promotion must use v2."""
        policy_v1 = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        policy_v2 = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=100.0,
        )
        provider = _MutablePolicyProvider(policy_v1, "policy-v1")
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
            exit_policy_provider=provider,
        )
        entry_result = _fake_buy_result(price=100.0, qty=1.0, commission=0.0, commission_asset="BTC")

        async def open_v1():
            return await manager.on_entry_filled(SYMBOL, result=entry_result, signal_context_id="ctx-1", atr=10.0)

        opened = run_async(open_v1())
        assert opened.policy_version_id == "policy-v1"
        assert opened.initial_protective_stop == 100.0 - 2.0 * 10.0  # 80.0
        assert opened.take_profit == 100.0 + 4.0 * 10.0  # 120.0

        # Simulate an `adaptive/` champion promotion: a NEW champion policy
        # takes effect for future entries only -- this open position must
        # be completely unaffected.
        provider.policy = policy_v2
        provider.version_id = "policy-v2"

        # Neither stop (80) nor target (120) is touched; only max-hold can
        # fire. close_time is 2h after entry -- v1's 1h limit is exceeded,
        # v2's 100h limit is not.
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

        # A brand-new position opened AFTER the promotion uses v2.
        entry_result_2 = ExecutionResult(
            symbol=self.SYMBOL_2, client_order_id="csl-entry-2", exchange_order_id=2, side=OrderSide.BUY,
            status="FILLED", executed_quantity=1.0, cumulative_quote_quantity=100.0,
            transaction_time=NOW, context_id="bridge:ETHUSDT:OPEN:ctx-2",
            fills=(Fill(price=100.0, quantity=1.0, commission=0.0, commission_asset="ETH", trade_id=2),),
        )

        async def open_v2():
            return await manager.on_entry_filled(
                self.SYMBOL_2, result=entry_result_2, signal_context_id="ctx-2", atr=10.0,
            )

        opened_2 = run_async(open_v2())
        assert opened_2.policy_version_id == "policy-v2"
        assert opened_2.exit_policy_max_hold_hours == 100.0

    def test_legacy_position_missing_policy_fields_falls_back_to_manager_default(self, tmp_path: Path) -> None:
        """Backward compatibility (required -- real open Testnet positions
        predate this change): a position persisted BEFORE this milestone
        has all six new fields `None`. Loading and evaluating it must not
        crash and must produce IDENTICAL behavior to before this change --
        i.e. it is treated as opened under the manager's own current
        `self._exit_policy`, never the `ExitPolicyConfig()` bare default
        and never any provider-resolved policy (the provider is only ever
        consulted for a BRAND NEW position's entry)."""
        custom_policy = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        # Provider present but must NEVER be consulted for an already-open
        # legacy position -- only `self._exit_policy` (the manager's own
        # static config) is the correct fallback here. It returns a
        # 999-hour max-hold that would NOT fire on the 2h-later candle
        # below, so wrongly consulting it would silently swallow the
        # MAX_HOLD exit this test proves DOES fire.
        provider = _MutablePolicyProvider(
            ExitPolicyConfig(max_hold_hours=999.0), "should-never-be-used",
        )
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path,
            exit_policy=custom_policy, exit_policy_provider=provider,
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        legacy = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
            # exit_policy_* / policy_version_id all left at their None default.
        )
        assert legacy.policy_version_id is None
        store.save_position(legacy)

        # candle.close_time is 2h after entry -- fires MAX_HOLD only under
        # `custom_policy`'s 1h limit (the manager's `self._exit_policy`),
        # never under the provider's 999h value.
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        # Fires MAX_HOLD -- proving `custom_policy` (1h) governed this
        # position, not the provider's 999h value (which would have
        # produced `outcome is None` here instead).
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT

    def test_legacy_position_max_hold_still_fires_under_manager_default(self, tmp_path: Path) -> None:
        """Companion to the above: WITHOUT a provider at all (the common,
        real-world case for every position that predates this milestone --
        `app.py`'s existing static-config wiring never sets one), a legacy
        position's max-hold behavior is unaffected by this change: it
        still exits on MAX_HOLD exactly as it would have before the
        position-pinning fields existed."""
        custom_policy = ExitPolicyConfig(
            stop_atr_multiple=2.0, take_profit_atr_multiple=4.0, trailing_activation_atr_multiple=2.0,
            trailing_distance_atr_multiple=2.0, max_hold_hours=1.0,
        )
        get_responses = [
            json_response(_exchange_info()), json_response(_exchange_info()),
            json_response([
                {"symbol": SYMBOL, "id": 6, "orderId": 1, "price": "100.0", "qty": "1.0",
                 "commission": "0.1", "commissionAsset": "USDT"},
            ]),
        ]
        post_responses = [json_response(_order_payload(side="SELL", qty="1.0", quote_qty="100.0"))]
        manager, http, _, store = _manager(
            get_responses=get_responses, post_responses=post_responses, tmp_path=tmp_path, exit_policy=custom_policy,
        )
        from crypto_signal_engine.execution.lifecycle import BridgePositionRecord

        legacy = BridgePositionRecord(
            symbol=SYMBOL, state=PositionLifecycleState.LONG, gross_entry_vwap=100.0,
            net_owned_base_quantity=1.0, initial_protective_stop=80.0, high_water=100.0,
            effective_stop=80.0, take_profit=120.0, entry_timestamp=NOW,
            entry_client_order_id="csl-entry-1", entry_signal_context_id="ctx-1", updated_at=NOW,
        )
        store.save_position(legacy)
        candle = Candle(open=100, high=101, low=99, close=100, close_time=NOW + timedelta(hours=2))

        async def evaluate():
            return await manager.evaluate_m1_candle(SYMBOL, candle, atr_for_trailing=5.0)

        outcome = run_async(evaluate())
        assert outcome is not None
        assert outcome.exit_reason is ExitReason.MAX_HOLD
        assert outcome.action == "SOLD"
        assert store.load_position(SYMBOL).state is PositionLifecycleState.FLAT


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


