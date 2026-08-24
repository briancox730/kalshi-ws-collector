"""Settlement poller — the label source for each 15-minute window.

A REST poller (overrides ``run()``; the WS hooks are unused). Every
``poll_interval`` it asks Kalshi for recently *settled* markets per series and
emits a ``kalshi_settle`` row carrying the authoritative ``result`` ('yes' /
'no') — the label a downstream model trains against.

Idempotent: a window's ticker is emitted at most once. Prime the in-process seen
set via ``seen_tickers`` (e.g. from tickers already stored) so restarts do not
double-write.

This complements :class:`~kalshi_ws_collector.discovery.KalshiMarketDiscovery`,
which writes the open-time ``kalshi_settle`` marker (ref_price, result NULL).
Together they give each window a full open→settle record. Filter
``result IS NOT NULL`` for labels.

The determined timestamp uses a fallback chain — ``settled_time`` →
``determination_time`` → ``close_time`` → ``expiration_time`` — because which
field is populated varies with how and when a window resolved. Only rows whose
``result`` is 'yes' or 'no' are emitted.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime

import httpx

from kalshi_ws_collector.auth import (
    KALSHI_API_BASE,
    KALSHI_ASSETS,
    KALSHI_SERIES,
    load_private_key,
)
from kalshi_ws_collector.base import Collector
from kalshi_ws_collector.discovery import kalshi_rest_get
from kalshi_ws_collector.parsing import (
    asset_from_ticker,
    floor_15m,
    maybe_float,
    parse_kalshi_ts,
)

logger = logging.getLogger(__name__)


def market_from_single_response(resp: dict) -> dict | None:
    """Extract the market from a ``GET /markets/{ticker}`` response.

    Handles both the ``{"market": {...}}`` wrapper and a bare market object.
    """
    if not resp:
        return None
    return resp.get("market") or (resp if resp.get("ticker") else None)


def parse_settled_market(market: dict, asset: str | None = None) -> tuple[str, dict] | None:
    """Map a settled market object to ``(asset, kalshi_settle_row)``.

    Returns ``None`` unless the market carries a determined ``result``
    ('yes'/'no') — i.e. only real labels are emitted. Works for both the
    single-market and list-element response shapes since both yield the same
    market object.

    ``determined_ts`` follows the fallback chain settled_time →
    determination_time → close_time → expiration_time. ``window_start`` falls
    back to the 15-minute floor of the determined time (or now) when
    ``open_time`` is absent. ``settlement_value`` falls back through
    settlement_value → expiration_value → result_value.
    """
    if not market:
        return None
    ticker = market.get("ticker")
    if not ticker:
        return None
    asset = asset or asset_from_ticker(ticker)
    if asset is None:
        return None
    result = market.get("result")
    if result not in ("yes", "no"):
        return None  # not yet determined — skip until a real label exists

    determined_ts = parse_kalshi_ts(
        market.get("settled_time")
        or market.get("determination_time")
        or market.get("close_time")
        or market.get("expiration_time")
    )
    window_start = parse_kalshi_ts(market.get("open_time")) or floor_15m(
        determined_ts or datetime.now(UTC)
    )
    ref_price = maybe_float(market.get("floor_strike"))
    settlement_value = maybe_float(
        market.get("settlement_value")
        or market.get("expiration_value")
        or market.get("result_value")
    )
    row = {
        "event_ts": determined_ts or window_start,
        "ingest_ts": datetime.now(UTC),
        "seq": None,
        "raw": json.dumps(market),
        "market_ticker": ticker,
        "window_start": window_start,
        "ref_price": ref_price,
        "settlement_value": settlement_value,
        "result": result,
        "determined_ts": determined_ts,
    }
    return asset, row


class KalshiSettlementPoller(Collector):
    """Poll recently-settled markets and emit ``kalshi_settle`` label rows."""

    venue = "kalshi"
    data_type = "kalshi_settle"

    DEFAULT_POLL_INTERVAL = 300.0  # 5 minutes
    REST_TIMEOUT = 10.0
    # Kalshi market status for a determined window. Overridable via env in case
    # the exact status string needs correcting on a first live run — the parse
    # only emits rows where ``result`` is set, so a wrong status just yields
    # nothing rather than bad data.
    SETTLE_STATUS = "settled"

    def __init__(
        self,
        assets: list[str],
        sink,
        *,
        poll_interval: float | None = None,
        series: dict[str, str] | None = None,
        lookback: int = 100,
        seen_tickers: Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(assets or KALSHI_ASSETS, sink, **kwargs)
        self._poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        self._series = dict(series) if series else {
            a: KALSHI_SERIES[a] for a in self.symbols if a in KALSHI_SERIES
        }
        self._status = os.getenv("KALSHI_SETTLE_STATUS", self.SETTLE_STATUS)
        self._lookback = lookback
        self._settled_seen: set[str] = set(seen_tickers or ())
        self._api_key_id: str | None = None
        self._private_key = None

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
        await self._run_lifecycle(self._poll_loop())

    async def _poll_loop(self) -> None:
        backoff = self.INITIAL_BACKOFF
        async with httpx.AsyncClient(base_url=KALSHI_API_BASE, timeout=self.REST_TIMEOUT) as client:
            self._beat("running")
            while not self._stop.is_set():
                try:
                    self._ensure_creds()
                    written = await self.poll_once(client)
                    if written:
                        logger.info("settle: wrote %d new settlement(s)", written)
                    backoff = self.INITIAL_BACKOFF
                    self._beat("running")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("settle: poll failed: %r", exc)
                    self._beat("reconnecting", error_message=repr(exc))
                    if await self._sleep_or_stop(backoff):
                        return
                    backoff = min(backoff * 2, self.MAX_BACKOFF)
                    continue
                if await self._sleep_or_stop(self._poll_interval):
                    return

    async def poll_once(self, client: httpx.AsyncClient) -> int:
        """One poll pass over every series; return # of new settlement rows emitted."""
        written = 0
        scanned = 0
        for asset, series in self._series.items():
            try:
                resp = await kalshi_rest_get(
                    client, self._api_key_id, self._private_key,
                    "/markets",
                    params={
                        "series_ticker": series,
                        "status": self._status,
                        "limit": self._lookback,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("settle: %s fetch failed: %r", series, exc)
                continue
            markets = (resp or {}).get("markets") or []
            scanned += len(markets)
            for market in markets:
                parsed = parse_settled_market(market, asset)
                if parsed is None:
                    continue
                asset_, row = parsed
                ticker = row["market_ticker"]
                if ticker in self._settled_seen:
                    continue
                self._settled_seen.add(ticker)
                self._emit("kalshi_settle", asset_, row)
                written += 1
                logger.info("settle: %s %s result=%s", asset_, ticker, row["result"])
        logger.info(
            "settle: scanned %d market(s) (status=%s), wrote %d new",
            scanned, self._status, written,
        )
        return written
