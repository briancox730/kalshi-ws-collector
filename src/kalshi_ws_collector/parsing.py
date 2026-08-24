"""Pure value coercion and timestamp/ticker helpers.

Everything here is free of network and credentials so it can be unit-tested in
isolation. The Kalshi feed is loosely typed — prices arrive as integer cents,
dollar-strings, or fixed-point strings; timestamps as ISO-8601 or epoch
seconds/milliseconds — so each parsed field goes through one of these coercers.
"""

from __future__ import annotations

from datetime import UTC, datetime

from kalshi_ws_collector.auth import KALSHI_SERIES


def maybe_float(value) -> float | None:
    """Coerce to float, returning None for None / empty string / unparseable."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def maybe_int(value) -> int | None:
    """Coerce to int, returning None for None / unparseable values."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_count(value) -> int:
    """Coerce a trade size to a non-negative int.

    Kalshi's ``count_fp`` is a fixed-point string ('1.00', '150.00'); the bare
    ``count`` is an int. Returns 0 for missing/unparseable values.
    """
    if value is None:
        return 0
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def coerce_price(value) -> float | None:
    """Normalise a Kalshi price to dollars in [0, 1].

    Kalshi sends prices as integer cents (``97``), dollar-strings (``"0.97"``),
    or cent-strings (``"97"``). The heuristic: a numeric value greater than 1.5
    is cents and is divided by 100; a value of 1.5 or less is already dollars.
    A dollar-string (contains a ".") always passes through unscaled.
    """
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            v = float(value)
            return v / 100.0 if v > 1.5 else v
        s = str(value)
        v = float(s)
        if "." in s:
            return v
        return v / 100.0 if v > 1.5 else v
    except (TypeError, ValueError):
        return None


def asset_from_ticker(ticker: str | None) -> str | None:
    """Map a market ticker (e.g. ``KXBTC15M-...``) to its asset, or None.

    A self-contained fallback for when the discovery map does not yet know a
    ticker.
    """
    if not ticker:
        return None
    for asset, series in KALSHI_SERIES.items():
        if ticker.startswith(series):
            return asset
    return None


def floor_15m(dt: datetime) -> datetime:
    """Floor a UTC datetime to the start of its 15-minute window."""
    dt = dt.astimezone(UTC)
    return dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)


def parse_kalshi_ts(value) -> datetime | None:
    """Parse a Kalshi timestamp (ISO-8601 string, or epoch s / ms) to UTC.

    Returns None for None, empty, or unparseable input.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        # Heuristic: > ~1e12 is milliseconds, otherwise seconds.
        return datetime.fromtimestamp(v / 1000.0 if v > 1e12 else v, tz=UTC)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def event_ts_from_msg(msg: dict) -> datetime:
    """Best event timestamp for a WS frame's ``msg`` body.

    Kalshi frames carry ``ts_ms`` (epoch ms, preferred) and/or ``ts`` (epoch s).
    Falls back to now(UTC) when neither is present or parseable.
    """
    ts = msg.get("ts_ms")
    if ts is None:
        ts = msg.get("ts")
    parsed = parse_kalshi_ts(ts) if ts is not None else None
    return parsed if parsed is not None else datetime.now(UTC)
