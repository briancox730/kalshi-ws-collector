"""Market discovery for the rotating 15-minute crypto contracts.

The active market for each series (KXBTC15M / KXETH15M / KXSOL15M / KXXRP15M)
changes every 15 minutes at the :00/:15/:30/:45 UTC boundaries. This poller
resolves the current market ticker per series on a short REST cadence and
exposes the ``asset → ticker / window_start / ref_price`` map to the WebSocket
collectors so they can (re)subscribe as windows roll over.

It also stamps an open-time ``kalshi_settle`` marker row the first time each
window's ticker appears with a populated reference price — pairing with the
settlement poller's close-time row to give every window a full open→settle
record.

Cadence is boundary-aware. The active ticker only changes at window boundaries,
so blind fixed-interval polling all day is almost entirely wasted. Instead: poll
fast (``FAST_INTERVAL``) only in the first ``FAST_WINDOW_SEC`` after a boundary —
until each market's reference price lands — then idle until just after the next
boundary.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx

from kalshi_ws_collector.auth import (
    KALSHI_API_BASE,
    KALSHI_ASSETS,
    KALSHI_SERIES,
    auth_headers,
    load_private_key,
)
from kalshi_ws_collector.base import Collector
from kalshi_ws_collector.parsing import (
    asset_from_ticker,
    floor_15m,
    maybe_float,
    parse_kalshi_ts,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketInfo:
    asset: str
    ticker: str
    window_start: datetime
    ref_price: float | None


def active_market_from_response(resp: dict) -> dict | None:
    """Return the first market from a ``GET /markets`` response (or None)."""
    markets = (resp or {}).get("markets") or []
    return markets[0] if markets else None


def parse_open_market(market: dict, asset: str | None = None) -> MarketInfo | None:
    """Parse one open-market object into a :class:`MarketInfo`.

    ``window_start`` is the market's ``open_time``; ``ref_price`` is its
    ``floor_strike`` (the reference price Kalshi sets a few seconds after each
    window opens). Falls back to deriving the asset from the ticker prefix and
    the window start from the current 15-minute boundary if fields are missing.
    """
    ticker = market.get("ticker")
    if not ticker:
        return None
    asset = asset or asset_from_ticker(ticker)
    if asset is None:
        return None
    window_start = parse_kalshi_ts(market.get("open_time")) or floor_15m(datetime.now(UTC))
    ref_price = maybe_float(market.get("floor_strike"))
    return MarketInfo(asset=asset, ticker=ticker, window_start=window_start, ref_price=ref_price)


async def kalshi_rest_get(
    client: httpx.AsyncClient,
    api_key_id: str,
    private_key,
    path: str,
    params: dict | None = None,
) -> dict:
    """Signed GET against the Kalshi REST API.

    The signature covers the FULL path including the ``/trade-api/v2`` prefix and
    excludes the query string.
    """
    sign_path = urlparse(KALSHI_API_BASE + path).path
    headers = auth_headers(api_key_id, private_key, "GET", sign_path)
    resp = await client.get(path, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


class KalshiMarketDiscovery(Collector):
    """Resolves the active 15-minute market per series and exposes the mapping.

    Reuses the ``Collector`` lifecycle (heartbeat / idle-flush / stop) and
    overrides ``run()`` with a REST refresh loop; the WS hooks are unused. The
    ``data_type`` class attr is a heartbeat label only — this task writes
    ``kalshi_settle`` open-marker rows.
    """

    venue = "kalshi"
    data_type = "kalshi_discovery"  # heartbeat id only

    WINDOW_SECONDS = 15 * 60
    FAST_INTERVAL = 2.0
    FAST_WINDOW_SEC = 45.0
    POST_BOUNDARY_LEAD = 1.0  # poll this many seconds AFTER the boundary
    REST_TIMEOUT = 10.0

    def __init__(
        self,
        assets: list[str],
        sink,
        *,
        fast_interval: float | None = None,
        series: dict[str, str] | None = None,
        seen_tickers: Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(assets or KALSHI_ASSETS, sink, **kwargs)
        self._fast_interval = fast_interval or self.FAST_INTERVAL
        self._series = dict(series) if series else {
            a: KALSHI_SERIES[a] for a in self.symbols if a in KALSHI_SERIES
        }
        self._active: dict[str, MarketInfo] = {}  # asset -> info
        self._by_ticker: dict[str, MarketInfo] = {}  # ticker -> info
        # Prime the idempotency set so a restart does not re-write open markers
        # for windows already recorded upstream.
        self._seen_open: set[str] = set(seen_tickers or ())
        self._api_key_id: str | None = None
        self._private_key = None

    # ---- public map read by the WS collectors -----------------------

    def active_tickers(self) -> list[str]:
        return sorted(self._by_ticker)

    def asset_for(self, ticker: str | None) -> str | None:
        info = self._by_ticker.get(ticker) if ticker else None
        return info.asset if info else None

    def window_start_for(self, ticker: str | None) -> datetime | None:
        info = self._by_ticker.get(ticker) if ticker else None
        return info.window_start if info else None

    def ref_price_for(self, ticker: str | None) -> float | None:
        info = self._by_ticker.get(ticker) if ticker else None
        return info.ref_price if info else None

    # ---- unused abstract hooks --------------------------------------

    def ws_url(self) -> str:
        return ""

    def subscribe_message(self) -> str:
        return ""

    def parse(self, message: str) -> Iterable[tuple[str, str, dict]]:
        return iter([])

    # ---- lifecycle --------------------------------------------------

    def _ensure_creds(self) -> None:
        if self._private_key is None:
            self._api_key_id = os.getenv("KALSHI_API_KEY_ID", "")
            self._private_key = load_private_key()

    async def run(self) -> None:
        await self._run_lifecycle(self._discovery_loop())

    # ---- boundary-aware cadence -------------------------------------

    @classmethod
    def _seconds_to_next_boundary(cls, now: datetime) -> float:
        """Seconds from ``now`` to the next 15-min boundary (3600 is a multiple
        of 900, so seconds-into-the-hour suffices)."""
        secs_in_hour = now.minute * 60 + now.second + now.microsecond / 1e6
        into = secs_in_hour % cls.WINDOW_SECONDS
        return cls.WINDOW_SECONDS - into

    def _all_current_captured(self, current_boundary: datetime) -> bool:
        """True iff every series has an active market for the CURRENT window with
        a non-null ref_price — nothing left to fast-poll for."""
        for asset in self._series:
            info = self._active.get(asset)
            if info is None or info.window_start != current_boundary or info.ref_price is None:
                return False
        return True

    def _next_poll_delay(self, now: datetime) -> float:
        """How long to sleep before the next discovery poll.

        Fast (``_fast_interval``) while still within the post-boundary window AND
        not every market's ref_price has landed; otherwise idle until just after
        the next boundary.
        """
        current_boundary = floor_15m(now)
        secs_into = (now - current_boundary).total_seconds()
        if secs_into < self.FAST_WINDOW_SEC and not self._all_current_captured(current_boundary):
            return self._fast_interval
        return self._seconds_to_next_boundary(now) + self.POST_BOUNDARY_LEAD

    async def _discovery_loop(self) -> None:
        backoff = self.INITIAL_BACKOFF
        async with httpx.AsyncClient(base_url=KALSHI_API_BASE, timeout=self.REST_TIMEOUT) as client:
            self._beat("running")
            while not self._stop.is_set():
                try:
                    self._ensure_creds()
                    opened = await self.refresh_once(client)
                    if opened:
                        logger.info("discovery: %d new window(s) opened", opened)
                    backoff = self.INITIAL_BACKOFF
                    self._beat("running")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("discovery: refresh failed: %r", exc)
                    self._beat("reconnecting", error_message=repr(exc))
                    if await self._sleep_or_stop(backoff):
                        return
                    backoff = min(backoff * 2, self.MAX_BACKOFF)
                    continue
                if await self._sleep_or_stop(self._next_poll_delay(datetime.now(UTC))):
                    return

    async def refresh_once(self, client: httpx.AsyncClient) -> int:
        """Resolve the active market per series; return # of newly-opened windows."""
        new_active: dict[str, MarketInfo] = {}
        new_by_ticker: dict[str, MarketInfo] = {}
        opened = 0
        for asset, series in self._series.items():
            try:
                resp = await kalshi_rest_get(
                    client, self._api_key_id, self._private_key,
                    "/markets", params={"series_ticker": series, "status": "open", "limit": 1},
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("discovery: %s fetch failed: %r", series, exc)
                prev = self._active.get(asset)  # retain last-known on transient failure
                if prev is not None:
                    new_active[asset] = prev
                    new_by_ticker[prev.ticker] = prev
                continue
            market = active_market_from_response(resp)
            if market is None:
                continue
            info = parse_open_market(market, asset)
            if info is None:
                continue
            new_active[asset] = info
            new_by_ticker[info.ticker] = info
            # Defer the open marker until ref_price is populated — Kalshi sets it
            # a few seconds after open. Until then leave the ticker unseen so a
            # later fast-poll writes it with a real ref_price rather than NULL.
            if info.ticker not in self._seen_open and info.ref_price is not None:
                self._seen_open.add(info.ticker)
                self._write_open_marker(info, market)
                opened += 1
        self._active = new_active
        self._by_ticker = new_by_ticker
        return opened

    def _write_open_marker(self, info: MarketInfo, market: dict) -> None:
        """Stamp an immutable open-time ``kalshi_settle`` row (result NULL)."""
        now = datetime.now(UTC)
        row = {
            "event_ts": info.window_start,
            "ingest_ts": now,
            "seq": None,
            "raw": json.dumps(market),
            "market_ticker": info.ticker,
            "window_start": info.window_start,
            "ref_price": info.ref_price,
            "settlement_value": None,
            "result": None,
            "determined_ts": None,
        }
        self._emit("kalshi_settle", info.asset, row)
        logger.info("discovery: open %s %s ref=%s", info.asset, info.ticker, info.ref_price)
