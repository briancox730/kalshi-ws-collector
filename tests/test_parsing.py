"""Unit tests for the pure value/timestamp/ticker helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_ws_collector.parsing import (
    asset_from_ticker,
    coerce_count,
    coerce_price,
    event_ts_from_msg,
    floor_15m,
    maybe_float,
    maybe_int,
    parse_kalshi_ts,
)

BTC_TICKER = "KXBTC15M-26JUN0712-T50000"
ETH_TICKER = "KXETH15M-26JUN0712-T3000"


@pytest.mark.parametrize("value,expected", [
    (97, 0.97),      # int cents > 1.5 → dollars
    ("0.97", 0.97),  # dollar-string passes through
    ("97", 0.97),    # cent-string > 1.5 → dollars
    (0.45, 0.45),    # already dollars
    (62, 0.62),
    # Values <= 1.5 are treated as already-dollars. Integer 1 is ambiguous
    # ($1.00 vs 1c) and resolves to $1.00 — `raw` always preserves the payload.
    (1, 1.0),
    (None, None),
    ("", None),
    ("abc", None),
])
def test_coerce_price(value, expected):
    assert coerce_price(value) == expected


@pytest.mark.parametrize("value,expected", [
    ("150.00", 150),
    (2, 2),
    ("0.00", 0),
    (None, 0),
    ("nope", 0),
])
def test_coerce_count(value, expected):
    assert coerce_count(value) == expected


@pytest.mark.parametrize("value,expected", [
    ("3.5", 3.5),
    (7, 7.0),
    (None, None),
    ("", None),
    ("x", None),
])
def test_maybe_float(value, expected):
    assert maybe_float(value) == expected


@pytest.mark.parametrize("value,expected", [
    ("5", 5),
    (5, 5),
    (None, None),
    ("x", None),
])
def test_maybe_int(value, expected):
    assert maybe_int(value) == expected


@pytest.mark.parametrize("ticker,asset", [
    (BTC_TICKER, "BTC"),
    (ETH_TICKER, "ETH"),
    ("KXSOL15M-x", "SOL"),
    ("KXXRP15M-x", "XRP"),
    ("KXDOGE15M-x", None),
    ("", None),
    (None, None),
])
def test_asset_from_ticker(ticker, asset):
    assert asset_from_ticker(ticker) == asset


def test_floor_15m():
    dt = datetime(2026, 6, 7, 12, 37, 30, 500000, tzinfo=UTC)
    assert floor_15m(dt) == datetime(2026, 6, 7, 12, 30, 0, tzinfo=UTC)


def test_parse_kalshi_ts_iso_z():
    assert parse_kalshi_ts("2026-06-07T12:00:00Z") == datetime(
        2026, 6, 7, 12, 0, tzinfo=UTC
    )


def test_parse_kalshi_ts_epoch_ms():
    assert parse_kalshi_ts(1_717_761_600_000) == datetime(
        2024, 6, 7, 12, 0, tzinfo=UTC
    )


def test_parse_kalshi_ts_epoch_seconds():
    assert parse_kalshi_ts(1_717_761_600) == datetime(
        2024, 6, 7, 12, 0, tzinfo=UTC
    )


@pytest.mark.parametrize("bad", [None, "", "not-a-date"])
def test_parse_kalshi_ts_rejects(bad):
    assert parse_kalshi_ts(bad) is None


def test_event_ts_prefers_ts_ms():
    msg = {"ts": 1781096983, "ts_ms": 1781096983749}
    assert event_ts_from_msg(msg) == parse_kalshi_ts(1781096983749)


def test_event_ts_falls_back_to_now():
    before = datetime.now(UTC)
    got = event_ts_from_msg({})
    assert got >= before
