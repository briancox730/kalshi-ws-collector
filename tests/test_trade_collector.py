"""Unit tests for KalshiTradeCollector.parse."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from kalshi_ws_collector.collectors import KalshiTradeCollector
from kalshi_ws_collector.schema import KALSHI_TRADE
from kalshi_ws_collector.sinks import MemorySink

BTC_TICKER = "KXBTC15M-26JUN0712-T50000"
ETH_TICKER = "KXETH15M-26JUN0712-T3000"


def _collector():
    return KalshiTradeCollector(["BTC", "ETH", "SOL", "XRP"], MemorySink(), discovery=None)


def test_trade_parse():
    col = _collector()
    frame = json.dumps({
        "type": "trade", "seq": 6,
        "msg": {"trade_id": "abc", "market_ticker": ETH_TICKER,
                "yes_price_dollars": "0.6200", "no_price_dollars": "0.3800",
                "count_fp": "150.00", "taker_side": "yes",
                "ts": 1781096983, "ts_ms": 1781096983749},
    })
    rows = list(col.parse(frame))
    assert len(rows) == 1
    dt, asset, row = rows[0]
    assert dt == "kalshi_trade"
    assert asset == "ETH"
    assert row["yes_price"] == 0.62
    assert row["count"] == 150
    assert row["taker_side"] == "yes"
    ws = row["window_start"]
    assert ws.tzinfo is not None and ws.minute % 15 == 0 and ws.second == 0
    row["ingest_ts"] = datetime.now(UTC)
    assert pa.Table.from_pylist([row], schema=KALSHI_TRADE).num_rows == 1


def test_trade_legacy_field_fallback():
    col = _collector()
    frame = json.dumps({
        "type": "trade",
        "msg": {"market_ticker": BTC_TICKER, "yes_price": "0.5", "count": 2, "taker_side": "no"},
    })
    rows = list(col.parse(frame))
    assert rows and rows[0][2]["yes_price"] == 0.5 and rows[0][2]["count"] == 2


@pytest.mark.parametrize("msg", [
    {"market_ticker": BTC_TICKER, "yes_price_dollars": "0.5", "count_fp": "0.00", "taker_side": "yes"},
    {"market_ticker": BTC_TICKER, "yes_price_dollars": None, "count_fp": "2.00", "taker_side": "yes"},
    {"market_ticker": BTC_TICKER, "yes_price_dollars": "0.5", "count_fp": "2.00", "taker_side": "maybe"},
])
def test_trade_rejects_bad_rows(msg):
    col = _collector()
    assert list(col.parse(json.dumps({"type": "trade", "msg": msg}))) == []


def test_trade_ignores_non_trade_frames():
    col = _collector()
    assert list(col.parse(json.dumps({"type": "orderbook_snapshot"}))) == []
