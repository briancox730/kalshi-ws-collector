"""WebSocket collectors for the order book and trade tape.

Two authenticated WebSocket collectors share one driver, :class:`_KalshiWSCollector`:

  * :class:`KalshiBookCollector` drives the ``orderbook_delta`` channel, mirrors
    each contract's book locally (snapshot then deltas), and emits a top-of-book
    row plus full ladders on every message. It requests a fresh snapshot when it
    detects a sequence gap.
  * :class:`KalshiTradeCollector` drives the ``trade`` channel and emits one row
    per contract trade.

The active ticker rotates every 15 minutes, so neither collector can use the base
class's static subscribe model. Each keeps one authenticated socket per channel,
subscribes the current ticker set on connect, and reconciles add/delete-market
commands as market discovery rolls the window over — while reusing the base
lifecycle (heartbeat / idle-flush / stop).

Ladder convention: the ``yes`` / ``no`` stacks are resting BUY orders sorted
ascending by price, so the best (highest) bid is the top of each stack and
``yes_ask = 1 - top_no_bid``. Prices are normalised to dollars in [0, 1].
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime

import websockets

from kalshi_ws_collector.auth import (
    KALSHI_ASSETS,
    KALSHI_WS_URL,
    load_private_key,
    ws_auth_headers,
)
from kalshi_ws_collector.base import Collector
from kalshi_ws_collector.discovery import KalshiMarketDiscovery
from kalshi_ws_collector.orderbook import (
    Book,
    apply_delta,
    apply_snapshot,
    empty_book,
    levels_json,
    top_bid,
)
from kalshi_ws_collector.parsing import (
    asset_from_ticker,
    coerce_count,
    coerce_price,
    event_ts_from_msg,
    floor_15m,
    maybe_int,
)

logger = logging.getLogger(__name__)


class _KalshiWSCollector(Collector):
    """Shared authenticated-WS driver with discovery-driven re-subscribe.

    One socket per channel. Subscribes the current discovery ticker set on
    connect and reconciles add/delete-market commands every ``RECONCILE_INTERVAL``
    as windows roll over. Tracks the per-sid sequence counter; subclasses that set
    ``RESYNC_ON_GAP`` request a fresh snapshot on a detected gap.
    """

    venue = "kalshi"
    CHANNEL: str = ""  # subclass: 'orderbook_delta' | 'trade'
    RESYNC_ON_GAP: bool = False
    # Poll discovery often so a window rollover is (re)subscribed within ~1s of
    # discovery resolving it — the set comparison is free (no network).
    RECONCILE_INTERVAL: float = 1.0
    WS_PING_TIMEOUT: float = 20.0

    def __init__(
        self,
        assets: list[str],
        sink,
        *,
        discovery: KalshiMarketDiscovery | None = None,
        **kwargs,
    ) -> None:
        super().__init__(assets or KALSHI_ASSETS, sink, **kwargs)
        self._discovery = discovery
        self._sid = None
        self._last_seq: int | None = None
        self._subscribed: set[str] = set()
        self._cmd_id = 0
        self._api_key_id: str | None = None
        self._private_key = None

    # ---- identity / hooks -------------------------------------------

    def ws_url(self) -> str:
        return KALSHI_WS_URL

    def subscribe_message(self) -> str:  # unused — custom loop
        return ""

    # ---- helpers ----------------------------------------------------

    def _asset_for(self, ticker: str | None) -> str | None:
        if not ticker:
            return None
        if self._discovery is not None:
            a = self._discovery.asset_for(ticker)
            if a:
                return a
        return asset_from_ticker(ticker)

    def _window_start(self, ticker, event_ts):
        if self._discovery is not None:
            ws = self._discovery.window_start_for(ticker)
            if ws is not None:
                return ws
        return floor_15m(event_ts)

    def _ensure_creds(self) -> None:
        if self._private_key is None:
            self._api_key_id = os.getenv("KALSHI_API_KEY_ID", "")
            self._private_key = load_private_key()

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    def _desired(self) -> set[str]:
        return set(self._discovery.active_tickers()) if self._discovery else set()

    def _on_connect_reset(self) -> None:
        """Reset per-connection state on every (re)connect. Default no-op.

        Stateful subclasses (the book collector mirrors the book locally)
        override this to drop state carried over from a dropped connection, so a
        delta on the new connection cannot apply to a stale book.
        """
        return

    # ---- lifecycle --------------------------------------------------

    async def run(self) -> None:
        await self._run_lifecycle(self._ws_loop())

    def _connect(self, headers: dict):
        kwargs = dict(
            ping_interval=self.WS_PING_INTERVAL,
            ping_timeout=self.WS_PING_TIMEOUT,
            max_size=self.WS_MAX_MESSAGE_SIZE,
            open_timeout=self.WS_OPEN_TIMEOUT,
        )
        # websockets >= 14 takes ``additional_headers``; fall back to the legacy
        # ``extra_headers`` name on older releases.
        try:
            return websockets.connect(self.ws_url(), additional_headers=headers, **kwargs)
        except TypeError:
            return websockets.connect(self.ws_url(), extra_headers=headers, **kwargs)

    async def _ws_loop(self) -> None:
        backoff = self.INITIAL_BACKOFF
        while not self._stop.is_set():
            try:
                self._ensure_creds()
                headers = ws_auth_headers(self._api_key_id, self._private_key)
                async with self._connect(headers) as ws:
                    self._sid = None
                    self._last_seq = None
                    self._subscribed = set()
                    self._on_connect_reset()
                    await self._subscribe(ws)
                    backoff = self.INITIAL_BACKOFF
                    self._beat("running")
                    recv_task = asyncio.create_task(self._recv_loop(ws))
                    recon_task = asyncio.create_task(self._reconcile_loop(ws))
                    done, pending = await asyncio.wait(
                        {recv_task, recon_task}, return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):  # noqa: BLE001
                            pass
                    for t in done:
                        exc = t.exception()
                        if exc:
                            raise exc
                    if self._stop.is_set():
                        break
                    # Clean end of a task on an always-on consumer is abnormal.
                    raise ConnectionError("WS task ended; reconnecting")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: WS error %r; reconnecting in %.1fs", self.collector_id, exc, backoff,
                )
                self._beat("reconnecting", error_message=repr(exc))
                if await self._sleep_or_stop(backoff):
                    break
                backoff = min(backoff * 2, self.MAX_BACKOFF)

    async def _subscribe(self, ws) -> None:
        tickers = sorted(self._desired())
        if not tickers:
            logger.info("%s: no active tickers yet; awaiting discovery", self.collector_id)
            return
        cmd = {
            "id": self._next_id(),
            "cmd": "subscribe",
            "params": {"channels": [self.CHANNEL], "market_tickers": tickers},
        }
        await ws.send(json.dumps(cmd))
        self._subscribed = set(tickers)
        logger.info("%s: subscribe %d tickers", self.collector_id, len(tickers))

    async def _send_update(self, ws, action: str, tickers: list[str]) -> None:
        if self._sid is None or not tickers:
            return
        cmd = {
            "id": self._next_id(),
            "cmd": "update_subscription",
            "params": {"sid": self._sid, "action": action, "market_tickers": tickers},
        }
        await ws.send(json.dumps(cmd))

    async def _resync(self, ws) -> None:
        await self._send_update(ws, "get_snapshot", sorted(self._subscribed))
        logger.warning(
            "%s: resync requested (%d tickers)", self.collector_id, len(self._subscribed),
        )

    async def _reconcile_loop(self, ws) -> None:
        while not self._stop.is_set():
            if await self._sleep_or_stop(self.RECONCILE_INTERVAL):
                return
            desired = self._desired()
            if desired == self._subscribed:
                continue
            if self._sid is None:
                # Initial subscribe hasn't completed (e.g. no tickers at connect).
                if desired and not self._subscribed:
                    await self._subscribe(ws)
                continue
            to_add = sorted(desired - self._subscribed)
            to_remove = sorted(self._subscribed - desired)
            if to_add:
                await self._send_update(ws, "add_markets", to_add)
                logger.info("%s: +%s", self.collector_id, to_add)
            if to_remove:
                await self._send_update(ws, "delete_markets", to_remove)
                logger.info("%s: -%s", self.collector_id, to_remove)
            self._subscribed = desired

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            if self._stop.is_set():
                break
            await self._handle_raw(ws, raw)

    async def _handle_raw(self, ws, raw) -> None:
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        mtype = data.get("type")
        if mtype == "subscribed":
            self._sid = data.get("sid") or (data.get("msg") or {}).get("sid")
            self._last_seq = None  # seq counter restarts per subscription
            logger.info("%s: subscribed sid=%s", self.collector_id, self._sid)
            return
        if mtype == "error":
            logger.warning("%s: server error %s", self.collector_id, str(data)[:200])
            return

        # Per-sid sequence tracking. Snapshots reset the watermark; deltas/trades
        # must increment by exactly one or a frame was missed.
        seq = maybe_int(data.get("seq"))
        if mtype == "orderbook_snapshot":
            if seq is not None:
                self._last_seq = seq
        elif mtype in ("orderbook_delta", "trade"):
            if seq is not None:
                if self._last_seq is not None and seq != self._last_seq + 1:
                    logger.warning("%s: seq gap %s -> %s", self.collector_id, self._last_seq, seq)
                    self._last_seq = seq
                    if self.RESYNC_ON_GAP:
                        await self._resync(ws)
                        return  # drop the out-of-order frame; snapshot rebuilds
                else:
                    self._last_seq = seq

        try:
            rows = list(self.parse(raw))
        except Exception:  # noqa: BLE001
            logger.exception("%s: parse failed", self.collector_id)
            return
        ingest_ts = datetime.now(UTC)
        for data_type, symbol, row in rows:
            row.setdefault("ingest_ts", ingest_ts)
            self._emit(data_type, symbol, row)


class KalshiBookCollector(_KalshiWSCollector):
    """``orderbook_delta`` channel → ``kalshi_book`` top-of-book + ladder rows."""

    data_type = "kalshi_book"
    CHANNEL = "orderbook_delta"
    RESYNC_ON_GAP = True

    def __init__(self, assets, sink, *, discovery=None, **kwargs):
        super().__init__(assets, sink, discovery=discovery, **kwargs)
        # ticker -> {"yes": {price_key: size}, "no": {...}}
        self._books: dict[str, Book] = {}
        # Tickers that received a snapshot on the CURRENT connection. A delta for
        # a ticker not in this set is dropped (it would otherwise apply to a
        # stale/empty book straddling a reconnect).
        self._snapshotted: set[str] = set()

    def _on_connect_reset(self) -> None:
        # Drop book state carried over from the previous connection; Kalshi
        # re-snapshots on (re)subscribe, so we rebuild from scratch.
        self._books.clear()
        self._snapshotted.clear()

    def parse(self, message: str) -> Iterable[tuple[str, str, dict]]:
        data = json.loads(message)
        mtype = data.get("type")
        if mtype not in ("orderbook_snapshot", "orderbook_delta"):
            return
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker")
        asset = self._asset_for(ticker)
        if asset is None:
            return
        if mtype == "orderbook_snapshot":
            book = self._books.setdefault(ticker, empty_book())
            apply_snapshot(book, msg)
            self._snapshotted.add(ticker)
        else:
            if ticker not in self._snapshotted:
                # Delta before this connection's snapshot — applying it would
                # corrupt the book. Drop it; the snapshot (or a resync) rebuilds.
                return
            book = self._books[ticker]
            apply_delta(book, msg)

        event_ts = event_ts_from_msg(msg)
        yes_bid = top_bid(book["yes"])
        no_bid = top_bid(book["no"])
        yield "kalshi_book", asset, {
            "event_ts": event_ts,
            "seq": maybe_int(data.get("seq")),
            "raw": message,
            "market_ticker": ticker,
            "window_start": self._window_start(ticker, event_ts),
            "yes_bid": yes_bid,
            "no_bid": no_bid,
            "yes_ask": round(1.0 - no_bid, 4) if no_bid is not None else None,
            "no_ask": round(1.0 - yes_bid, 4) if yes_bid is not None else None,
            "yes_levels": levels_json(book["yes"]),
            "no_levels": levels_json(book["no"]),
        }


class KalshiTradeCollector(_KalshiWSCollector):
    """``trade`` channel → ``kalshi_trade`` rows."""

    data_type = "kalshi_trade"
    CHANNEL = "trade"
    RESYNC_ON_GAP = False

    def parse(self, message: str) -> Iterable[tuple[str, str, dict]]:
        data = json.loads(message)
        if data.get("type") != "trade":
            return
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker")
        asset = self._asset_for(ticker)
        if asset is None:
            return
        # Live frame uses ``yes_price_dollars`` (string) + ``count_fp``
        # (fixed-point string); keep the bare ``yes_price``/``count`` as a fallback.
        yes_price = coerce_price(msg.get("yes_price_dollars", msg.get("yes_price")))
        taker_side = msg.get("taker_side")
        count = coerce_count(msg.get("count_fp", msg.get("count")))
        if count <= 0 or yes_price is None or taker_side not in ("yes", "no"):
            return
        event_ts = event_ts_from_msg(msg)
        yield "kalshi_trade", asset, {
            "event_ts": event_ts,
            "seq": maybe_int(data.get("seq")),
            "raw": message,
            "market_ticker": ticker,
            "window_start": self._window_start(ticker, event_ts),
            "yes_price": yes_price,
            "count": count,
            "taker_side": taker_side,
        }
