"""Unit tests for settlement parsing, incl. the determined-ts fallback chain."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from kalshi_ws_collector.parsing import floor_15m, parse_kalshi_ts
from kalshi_ws_collector.schema import KALSHI_SETTLE
from kalshi_ws_collector.settlement import (
    market_from_single_response,
    parse_settled_market,
)

BTC_TICKER = "KXBTC15M-26JUN0712-T50000"

SETTLED = "2026-06-07T12:15:01Z"
DETERMINATION = "2026-06-07T12:15:02Z"
CLOSE = "2026-06-07T12:15:03Z"
EXPIRATION = "2026-06-07T12:15:04Z"


def test_market_from_single_response():
    assert market_from_single_response({"market": {"ticker": BTC_TICKER}})["ticker"] == BTC_TICKER
    # Bare market object (no wrapper) is accepted too.
    assert market_from_single_response({"ticker": BTC_TICKER})["ticker"] == BTC_TICKER
    assert market_from_single_response({}) is None


def test_parse_settled_market_full_row():
    market = {
        "ticker": BTC_TICKER,
        "open_time": "2026-06-07T12:00:00Z",
        "close_time": "2026-06-07T12:15:00Z",
        "floor_strike": 50000.0,
        "result": "yes",
        "settlement_value": 50123.4,
    }
    asset, row = parse_settled_market(market)
    assert asset == "BTC"
    assert row["market_ticker"] == BTC_TICKER
    assert row["result"] == "yes"
    assert row["ref_price"] == 50000.0
    assert row["settlement_value"] == 50123.4
    assert row["window_start"] == datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    assert row["determined_ts"] == datetime(2026, 6, 7, 12, 15, tzinfo=UTC)
    row["ingest_ts"] = datetime.now(UTC)
    assert pa.Table.from_pylist([row], schema=KALSHI_SETTLE).num_rows == 1


@pytest.mark.parametrize("fields,expected_src", [
    # Full set present → settled_time wins.
    ({"settled_time": SETTLED, "determination_time": DETERMINATION,
      "close_time": CLOSE, "expiration_time": EXPIRATION}, SETTLED),
    # settled_time missing → determination_time.
    ({"determination_time": DETERMINATION, "close_time": CLOSE,
      "expiration_time": EXPIRATION}, DETERMINATION),
    # settled + determination missing → close_time.
    ({"close_time": CLOSE, "expiration_time": EXPIRATION}, CLOSE),
    # only expiration_time → expiration_time.
    ({"expiration_time": EXPIRATION}, EXPIRATION),
])
def test_determined_ts_fallback_chain(fields, expected_src):
    market = {"ticker": BTC_TICKER, "open_time": "2026-06-07T12:00:00Z", "result": "yes", **fields}
    _, row = parse_settled_market(market)
    assert row["determined_ts"] == parse_kalshi_ts(expected_src)


def test_determined_ts_none_when_all_missing():
    # No timestamp fields at all: determined_ts is None; event_ts falls back to
    # window_start (from open_time). The label is still written.
    market = {"ticker": BTC_TICKER, "open_time": "2026-06-07T12:00:00Z", "result": "no"}
    _, row = parse_settled_market(market)
    assert row["determined_ts"] is None
    assert row["result"] == "no"
    assert row["event_ts"] == datetime(2026, 6, 7, 12, 0, tzinfo=UTC)


def test_window_start_falls_back_to_floor_of_determined():
    # open_time missing → window_start is the 15-min floor of determined_ts.
    market = {"ticker": BTC_TICKER, "result": "yes", "close_time": "2026-06-07T12:15:03Z"}
    _, row = parse_settled_market(market)
    assert row["window_start"] == floor_15m(parse_kalshi_ts("2026-06-07T12:15:03Z"))
    assert row["window_start"] == datetime(2026, 6, 7, 12, 15, tzinfo=UTC)


@pytest.mark.parametrize("fields,expected", [
    ({"settlement_value": 50123.4}, 50123.4),
    ({"expiration_value": 50200.0}, 50200.0),
    ({"result_value": 50300.0}, 50300.0),
    ({}, None),
])
def test_settlement_value_fallback(fields, expected):
    market = {"ticker": BTC_TICKER, "open_time": "2026-06-07T12:00:00Z", "result": "yes", **fields}
    _, row = parse_settled_market(market)
    assert row["settlement_value"] == expected


def test_no_result_is_skipped():
    # An open/closed-but-undetermined market has no result → not a label.
    market = {"ticker": BTC_TICKER, "open_time": "2026-06-07T12:00:00Z", "result": ""}
    assert parse_settled_market(market) is None


def test_unknown_ticker_skipped():
    assert parse_settled_market({"ticker": "KXFOO-1", "result": "yes"}) is None


def test_derives_asset_from_ticker():
    market = {"ticker": "KXSOL15M-26JUN0712-T150", "open_time": "2026-06-07T12:00:00Z", "result": "no"}
    asset, row = parse_settled_market(market)
    assert asset == "SOL"
    assert row["result"] == "no"
