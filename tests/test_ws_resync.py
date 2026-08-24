"""Sequence-gap detection and resync behaviour in the WS driver.

Drives the collector's frame handler directly with a fake socket (no network):
a contiguous sequence proceeds normally, while a gap on the book channel
triggers a ``get_snapshot`` resync and drops the out-of-order frame. The trade
channel, which does not resync, keeps consuming across a gap.
"""

from __future__ import annotations

import json

from kalshi_ws_collector.collectors import KalshiBookCollector, KalshiTradeCollector
from kalshi_ws_collector.sinks import MemorySink

BTC_TICKER = "KXBTC15M-26JUN0712-T50000"


class FakeWS:
    """Records outbound commands; never receives."""

    def __init__(self):
        self.sent = []

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))


def _snapshot(seq):
    return json.dumps({
        "type": "orderbook_snapshot", "seq": seq,
        "msg": {"market_ticker": BTC_TICKER,
                "yes_dollars_fp": [["0.45", "60"]], "no_dollars_fp": [["0.55", "20"]]},
    })


def _delta(seq, price="0.46", d="10"):
    return json.dumps({
        "type": "orderbook_delta", "seq": seq,
        "msg": {"market_ticker": BTC_TICKER, "side": "yes", "price_dollars": price, "delta_fp": d},
    })


def _trade(seq):
    return json.dumps({
        "type": "trade", "seq": seq,
        "msg": {"market_ticker": BTC_TICKER, "yes_price_dollars": "0.50",
                "count_fp": "3.00", "taker_side": "yes"},
    })


def _resync_cmds(ws):
    return [
        c for c in ws.sent
        if c.get("cmd") == "update_subscription"
        and c.get("params", {}).get("action") == "get_snapshot"
    ]


async def test_contiguous_sequence_does_not_resync():
    col = KalshiBookCollector(["BTC"], MemorySink())
    col._sid = "sid-1"
    col._subscribed = {BTC_TICKER}
    ws = FakeWS()
    await col._handle_raw(ws, _snapshot(5))   # establishes book, watermark=5
    await col._handle_raw(ws, _delta(6))      # contiguous
    await col._handle_raw(ws, _delta(7))      # contiguous
    assert _resync_cmds(ws) == []
    assert col._last_seq == 7


async def test_gap_triggers_resync_and_drops_frame():
    col = KalshiBookCollector(["BTC"], MemorySink())
    col._sid = "sid-1"
    col._subscribed = {BTC_TICKER}
    ws = FakeWS()
    await col._handle_raw(ws, _snapshot(5))
    rows_before = len(col.sink.records)
    await col._handle_raw(ws, _delta(8))  # expected 6 → gap
    resyncs = _resync_cmds(ws)
    assert len(resyncs) == 1
    assert resyncs[0]["params"]["market_tickers"] == [BTC_TICKER]
    # Watermark advances to the observed seq so we resync once, not every frame.
    assert col._last_seq == 8
    # The out-of-order frame is dropped — no book row emitted for it.
    assert len(col.sink.records) == rows_before


async def test_snapshot_resets_watermark():
    col = KalshiBookCollector(["BTC"], MemorySink())
    col._sid = "sid-1"
    col._subscribed = {BTC_TICKER}
    ws = FakeWS()
    await col._handle_raw(ws, _snapshot(100))
    await col._handle_raw(ws, _delta(101))
    assert _resync_cmds(ws) == []
    # A fresh snapshot at a lower seq resets the watermark (new subscription).
    await col._handle_raw(ws, _snapshot(3))
    await col._handle_raw(ws, _delta(4))
    assert _resync_cmds(ws) == []
    assert col._last_seq == 4


async def test_subscribed_frame_sets_sid():
    col = KalshiBookCollector(["BTC"], MemorySink())
    ws = FakeWS()
    await col._handle_raw(ws, json.dumps({"type": "subscribed", "sid": "sid-xyz"}))
    assert col._sid == "sid-xyz"


async def test_trade_channel_does_not_resync_on_gap():
    col = KalshiTradeCollector(["BTC"], MemorySink())
    col._sid = "sid-1"
    col._subscribed = {BTC_TICKER}
    ws = FakeWS()
    await col._handle_raw(ws, _trade(10))
    await col._handle_raw(ws, _trade(20))  # gap, but trades never resync
    assert _resync_cmds(ws) == []
    # Both trades were consumed and emitted.
    assert len(col.sink.records) == 2
    assert col._last_seq == 20
