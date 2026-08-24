"""Unit tests for market discovery: parsing, boundary cadence, and rollover."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kalshi_ws_collector.discovery import (
    KalshiMarketDiscovery,
    MarketInfo,
    active_market_from_response,
    parse_open_market,
)
from kalshi_ws_collector.parsing import floor_15m
from kalshi_ws_collector.sinks import MemorySink

BTC_A = "KXBTC15M-26JUN0912-T50000"
BTC_B = "KXBTC15M-26JUN0912B-T50100"
ETH_A = "KXETH15M-26JUN0912-T3000"


def _disc(assets=("BTC",), **kw):
    return KalshiMarketDiscovery(list(assets), MemorySink(), **kw)


# --- parsing ---------------------------------------------------------------


def test_active_market_from_response():
    resp = {"markets": [{"ticker": BTC_A}, {"ticker": "other"}]}
    assert active_market_from_response(resp)["ticker"] == BTC_A
    assert active_market_from_response({"markets": []}) is None
    assert active_market_from_response({}) is None


def test_parse_open_market_resolves_ticker_and_ref():
    market = {"ticker": BTC_A, "open_time": "2026-06-09T12:00:00Z", "floor_strike": 50000.0}
    info = parse_open_market(market)
    assert info is not None
    assert info.asset == "BTC"
    assert info.ticker == BTC_A
    assert info.window_start == datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    assert info.ref_price == 50000.0


def test_parse_open_market_rejects_unknown_series():
    assert parse_open_market({"ticker": "KXDOGE15M-1"}) is None
    assert parse_open_market({}) is None


# --- boundary-aware cadence ------------------------------------------------


@pytest.mark.parametrize("when,expected", [
    (datetime(2026, 6, 9, 12, 0, 0, tzinfo=UTC), 900.0),
    (datetime(2026, 6, 9, 12, 7, 30, tzinfo=UTC), 450.0),
    (datetime(2026, 6, 9, 12, 14, 59, tzinfo=UTC), 1.0),
    (datetime(2026, 6, 9, 12, 45, 0, tzinfo=UTC), 900.0),
    (datetime(2026, 6, 9, 12, 59, 59, tzinfo=UTC), 1.0),
])
def test_seconds_to_next_boundary(when, expected):
    assert KalshiMarketDiscovery._seconds_to_next_boundary(when) == pytest.approx(expected)


def test_next_poll_delay_fast_then_idle():
    disc = _disc()
    just_after = datetime(2026, 6, 9, 12, 0, 5, tzinfo=UTC)
    # Nothing captured yet, still inside the fast window → poll fast.
    assert disc._next_poll_delay(just_after) == disc._fast_interval
    # Capture the current window's market w/ ref_price → idle until next boundary.
    disc._active = {"BTC": MarketInfo("BTC", BTC_A, floor_15m(just_after), 50000.0)}
    assert disc._next_poll_delay(just_after) > 800  # ~896s
    # Past the fast window with nothing captured → also idle (give up this window).
    disc._active = {}
    assert disc._next_poll_delay(datetime(2026, 6, 9, 12, 1, 0, tzinfo=UTC)) > 800


def test_next_poll_delay_stays_fast_until_ref_price():
    disc = _disc()
    now = datetime(2026, 6, 9, 12, 0, 8, tzinfo=UTC)
    # Market open but floor_strike/ref_price not yet populated → keep polling.
    disc._active = {"BTC": MarketInfo("BTC", BTC_A, floor_15m(now), None)}
    assert disc._next_poll_delay(now) == disc._fast_interval


# --- open marker + map -----------------------------------------------------


def test_write_open_marker_emits_and_map_reads():
    disc = _disc()
    market = {"ticker": BTC_A, "open_time": "2026-06-09T12:00:00Z", "floor_strike": 50000.0}
    info = parse_open_market(market, "BTC")
    disc._write_open_marker(info, market)
    assert len(disc.sink.records) == 1
    rec = disc.sink.records[0]
    assert rec.data_type == "kalshi_settle"
    assert rec.fields["result"] is None  # open marker: not yet settled
    assert rec.fields["ref_price"] == 50000.0
    # Map accessors work after a manual set (simulating refresh_once).
    disc._by_ticker = {info.ticker: info}
    assert disc.asset_for(BTC_A) == "BTC"
    assert disc.window_start_for(BTC_A) == info.window_start
    assert disc.ref_price_for(BTC_A) == 50000.0


def test_seen_tickers_priming_suppresses_duplicate_open_marker():
    # A restart primed with a known ticker must not re-write its open marker.
    disc = _disc(seen_tickers=[BTC_A])
    info = parse_open_market({"ticker": BTC_A, "open_time": "2026-06-09T12:00:00Z",
                              "floor_strike": 50000.0}, "BTC")
    # Simulate refresh: ticker already seen, so no marker is written.
    assert info.ticker in disc._seen_open


# --- window/market rollover (refresh_once) ---------------------------------


def _resp(ticker, open_time, floor_strike=50000.0):
    market = {"ticker": ticker, "open_time": open_time}
    if floor_strike is not None:
        market["floor_strike"] = floor_strike
    return {"markets": [market]}


async def test_refresh_once_discovers_then_rolls_over(monkeypatch):
    disc = _disc(assets=("BTC",))
    disc._api_key_id = "k"
    disc._private_key = object()

    rounds = {
        "KXBTC15M": [
            _resp(BTC_A, "2026-06-09T12:00:00Z"),  # window A
            _resp(BTC_B, "2026-06-09T12:15:00Z"),  # window B (rollover)
        ],
    }

    async def fake_get(client, api_key_id, pk, path, params=None):
        return rounds[params["series_ticker"]].pop(0)

    monkeypatch.setattr("kalshi_ws_collector.discovery.kalshi_rest_get", fake_get)

    opened = await disc.refresh_once(None)
    assert opened == 1
    assert disc.active_tickers() == [BTC_A]
    assert disc.asset_for(BTC_A) == "BTC"
    assert len(disc.sink.records) == 1

    # Next poll: the window rolled over to a new ticker.
    opened = await disc.refresh_once(None)
    assert opened == 1
    assert disc.active_tickers() == [BTC_B]     # old ticker dropped from the map
    assert disc.asset_for(BTC_A) is None
    assert disc.window_start_for(BTC_B) == datetime(2026, 6, 9, 12, 15, tzinfo=UTC)
    assert len(disc.sink.records) == 2          # one open marker per window


async def test_refresh_once_defers_marker_until_ref_price(monkeypatch):
    disc = _disc(assets=("BTC",))
    disc._api_key_id = "k"
    disc._private_key = object()

    rounds = {
        "KXBTC15M": [
            _resp(BTC_A, "2026-06-09T12:00:00Z", floor_strike=None),  # ref not set yet
            _resp(BTC_A, "2026-06-09T12:00:00Z", floor_strike=50000.0),  # now set
        ],
    }

    async def fake_get(client, api_key_id, pk, path, params=None):
        return rounds[params["series_ticker"]].pop(0)

    monkeypatch.setattr("kalshi_ws_collector.discovery.kalshi_rest_get", fake_get)

    opened = await disc.refresh_once(None)
    assert opened == 0                       # deferred: no ref_price yet
    assert disc.active_tickers() == [BTC_A]   # but the ticker is known/subscribable
    assert len(disc.sink.records) == 0

    opened = await disc.refresh_once(None)
    assert opened == 1                       # ref_price landed → marker written once
    assert len(disc.sink.records) == 1


async def test_refresh_once_retains_last_known_on_fetch_error(monkeypatch):
    disc = _disc(assets=("BTC",))
    disc._api_key_id = "k"
    disc._private_key = object()
    disc._active = {"BTC": MarketInfo("BTC", BTC_A, datetime(2026, 6, 9, 12, 0, tzinfo=UTC), 50000.0)}

    async def boom(client, api_key_id, pk, path, params=None):
        raise RuntimeError("transient")

    monkeypatch.setattr("kalshi_ws_collector.discovery.kalshi_rest_get", boom)
    opened = await disc.refresh_once(None)
    assert opened == 0
    # Last-known ticker is retained so the WS collectors don't lose the subscription.
    assert disc.active_tickers() == [BTC_A]
