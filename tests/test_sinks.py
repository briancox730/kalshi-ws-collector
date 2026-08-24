"""Round-trip tests for the reference sinks (Parquet + JSONL)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from kalshi_ws_collector.sinks import (
    JsonlSink,
    MemorySink,
    ParquetSink,
    Record,
    read_jsonl,
    read_parquet,
)

EVENT_TS = datetime(2026, 6, 7, 12, 3, 4, 123456, tzinfo=UTC)
INGEST_TS = datetime(2026, 6, 7, 12, 3, 5, tzinfo=UTC)
WINDOW = datetime(2026, 6, 7, 12, 0, 0, tzinfo=UTC)
BTC_TICKER = "KXBTC15M-26JUN0712-T50000"


def _book_row(seq, yes_bid):
    return {
        "event_ts": EVENT_TS,
        "ingest_ts": INGEST_TS,
        "seq": seq,
        "raw": json.dumps({"type": "orderbook_snapshot", "seq": seq}),
        "market_ticker": BTC_TICKER,
        "window_start": WINDOW,
        "yes_bid": yes_bid,
        "yes_ask": round(1.0 - 0.55, 4),
        "no_bid": 0.55,
        "no_ask": round(1.0 - yes_bid, 4),
        "yes_levels": json.dumps([[yes_bid, 60.0]]),
        "no_levels": json.dumps([[0.55, 20.0]]),
    }


def test_parquet_round_trip_preserves_values(tmp_path):
    sink = ParquetSink(tmp_path)
    rows = [_book_row(1, 0.45), _book_row(2, 0.48)]
    for row in rows:
        sink.write(Record("kalshi_book", "BTC", row))
    sink.close()

    table = read_parquet(tmp_path, "kalshi_book", "BTC")
    assert table.num_rows == 2
    got = table.to_pylist()
    for original, roundtripped in zip(rows, got, strict=True):
        for key, value in original.items():
            assert roundtripped[key] == value, key
    # Timestamp precision preserved to microseconds.
    assert got[0]["event_ts"] == EVENT_TS


def test_parquet_drops_extra_keys(tmp_path):
    sink = ParquetSink(tmp_path)
    row = _book_row(1, 0.45)
    row["not_in_schema"] = "ignore me"
    sink.write(Record("kalshi_book", "BTC", row))
    sink.close()
    table = read_parquet(tmp_path, "kalshi_book", "BTC")
    assert "not_in_schema" not in table.column_names


def test_parquet_partitions_by_data_type_and_symbol(tmp_path):
    sink = ParquetSink(tmp_path)
    sink.write(Record("kalshi_book", "BTC", _book_row(1, 0.45)))
    sink.write(Record("kalshi_book", "ETH", _book_row(1, 0.30)))
    sink.close()
    assert (tmp_path / "data_type=kalshi_book" / "symbol=BTC").is_dir()
    assert (tmp_path / "data_type=kalshi_book" / "symbol=ETH").is_dir()
    assert read_parquet(tmp_path, "kalshi_book", "BTC").num_rows == 1
    assert read_parquet(tmp_path, "kalshi_book", "ETH").num_rows == 1


def test_parquet_flushes_on_max_rows(tmp_path):
    sink = ParquetSink(tmp_path, max_rows=2)
    for i in range(5):
        sink.write(Record("kalshi_book", "BTC", _book_row(i, 0.45)))
    # 5 rows with max_rows=2 → two auto-flushed files (4 rows), 1 buffered.
    files = list((tmp_path / "data_type=kalshi_book" / "symbol=BTC").glob("part-*.parquet"))
    assert len(files) == 2
    sink.close()  # flush the remaining buffered row
    assert read_parquet(tmp_path, "kalshi_book", "BTC").num_rows == 5


def test_parquet_unknown_data_type_raises(tmp_path):
    sink = ParquetSink(tmp_path)
    with pytest.raises(KeyError):
        sink.write(Record("not_a_type", "BTC", {"event_ts": EVENT_TS}))


def test_jsonl_round_trip(tmp_path):
    sink = JsonlSink(tmp_path)
    rows = [_book_row(1, 0.45), _book_row(2, 0.48)]
    for row in rows:
        sink.write(Record("kalshi_book", "BTC", row))
    sink.close()

    got = read_jsonl(tmp_path, "kalshi_book", "BTC")
    assert len(got) == 2
    # Datetimes serialize to ISO-8601 strings; everything else is identical.
    for original, roundtripped in zip(rows, got, strict=True):
        expected = {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in original.items()
        }
        assert roundtripped == expected


def test_memory_sink_collects():
    sink = MemorySink()
    sink.write(Record("kalshi_trade", "BTC", {"count": 3}))
    sink.flush()
    sink.close()
    assert len(sink.records) == 1
    assert sink.records[0].data_type == "kalshi_trade"
