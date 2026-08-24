"""Unit tests for KalshiBookCollector.parse (snapshot/delta → top-of-book row)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pyarrow as pa

from kalshi_ws_collector.collectors import KalshiBookCollector
from kalshi_ws_collector.schema import KALSHI_BOOK
from kalshi_ws_collector.sinks import MemorySink

BTC_TICKER = "KXBTC15M-26JUN0712-T50000"


def _collector():
    return KalshiBookCollector(["BTC", "ETH", "SOL", "XRP"], MemorySink(), discovery=None)


def _snap(yes, no, seq=1):
    return json.dumps({
        "type": "orderbook_snapshot", "seq": seq,
        "msg": {"market_ticker": BTC_TICKER, "yes_dollars_fp": yes, "no_dollars_fp": no},
    })


def _delta(side="yes", price="0.48", d="25", seq=2):
    return json.dumps({
        "type": "orderbook_delta", "seq": seq,
        "msg": {"market_ticker": BTC_TICKER, "side": side, "price_dollars": price, "delta_fp": d},
    })


def test_snapshot_top_of_book():
    col = _collector()
    rows = list(col.parse(_snap([["0.40", "100"], ["0.45", "60"]], [["0.50", "30"], ["0.55", "20"]])))
    assert len(rows) == 1
    dt, asset, row = rows[0]
    assert dt == "kalshi_book"
    assert asset == "BTC"
    assert row["market_ticker"] == BTC_TICKER
    assert row["yes_bid"] == 0.45
    assert row["no_bid"] == 0.55
    # yes_ask = 1 - top_no_bid; no_ask = 1 - top_yes_bid
    assert row["yes_ask"] == 0.45
    assert row["no_ask"] == 0.55
    assert json.loads(row["yes_levels"]) == [[0.40, 100.0], [0.45, 60.0]]
    ws = row["window_start"]
    assert ws.tzinfo is not None and ws.second == 0 and ws.minute % 15 == 0


def test_delta_updates_top():
    col = _collector()
    list(col.parse(_snap([["0.45", "60"]], [["0.55", "20"]])))
    # New best yes bid at 0.48 → no_ask should become 1 - 0.48 = 0.52.
    _, _, row = list(col.parse(_delta(price="0.48", d="25")))[0]
    assert row["yes_bid"] == 0.48
    assert row["no_ask"] == 0.52


def test_delta_removes_level():
    col = _collector()
    list(col.parse(_snap([["0.45", "60"], ["0.40", "10"]], [])))
    _, _, row = list(col.parse(_delta(price="0.45", d="-60")))[0]
    assert row["yes_bid"] == 0.40  # 0.45 removed, next best is 0.40


def test_row_matches_schema():
    col = _collector()
    _, _, row = list(col.parse(_snap([["0.45", "60"]], [["0.55", "20"]])))[0]
    row["ingest_ts"] = datetime.now(UTC)
    assert pa.Table.from_pylist([row], schema=KALSHI_BOOK).num_rows == 1


def test_ignores_non_book_frames():
    col = _collector()
    assert list(col.parse(json.dumps({"type": "subscribed", "sid": 1}))) == []


def test_delta_before_snapshot_is_dropped():
    # A delta arriving before any snapshot (e.g. straddling a reconnect) must
    # not apply to an empty/stale book.
    col = _collector()
    assert list(col.parse(_delta())) == []
    list(col.parse(_snap([["0.45", "60"]], [])))  # snapshot establishes state
    rows = list(col.parse(_delta()))  # now the delta applies
    assert rows and rows[0][2]["yes_bid"] == 0.48


def test_on_connect_reset_clears_state():
    col = _collector()
    list(col.parse(_snap([["0.45", "60"]], [])))
    assert BTC_TICKER in col._snapshotted and col._books
    col._on_connect_reset()
    assert col._snapshotted == set() and col._books == {}
    # Post-reconnect delta (before the new snapshot) is dropped, not corrupting.
    assert list(col.parse(_delta())) == []
