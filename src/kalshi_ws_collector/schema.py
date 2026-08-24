"""PyArrow schemas for the three Kalshi record types.

Every record carries a small envelope (``event_ts``, ``ingest_ts``, ``seq``,
``raw``) plus type-specific fields. ``raw`` is the original venue payload (a
JSON string) — the source of truth — and the parsed fields beside it exist for
query convenience.

``data_type`` and ``symbol`` are NOT columns here: the Parquet sink encodes them
in the directory path (Hive style) and they are recovered at read time. Prices
are in dollars, [0, 1].

PyArrow does not enforce ``nullable=False`` at table-construction time — it is
metadata only. Treat the declaration as documentation of intent; null-rejection,
if needed, happens in the collector or a downstream query engine.
"""

from __future__ import annotations

import pyarrow as pa

TS_TYPE = pa.timestamp("us", tz="UTC")

ENVELOPE_FIELDS: list[pa.Field] = [
    pa.field("event_ts", TS_TYPE, nullable=False),
    pa.field("ingest_ts", TS_TYPE, nullable=False),
    pa.field("seq", pa.int64(), nullable=True),
    pa.field("raw", pa.string(), nullable=True),
]


def _schema(*extra_fields: pa.Field) -> pa.Schema:
    return pa.schema(ENVELOPE_FIELDS + list(extra_fields))


# Top-of-book (+ full ladders) for one 15-minute contract.
KALSHI_BOOK = _schema(
    pa.field("market_ticker", pa.string(), nullable=False),
    pa.field("window_start", TS_TYPE, nullable=False),
    pa.field("yes_bid", pa.float64(), nullable=True),
    pa.field("yes_ask", pa.float64(), nullable=True),  # 1 - top_no_bid
    pa.field("no_bid", pa.float64(), nullable=True),
    pa.field("no_ask", pa.float64(), nullable=True),  # 1 - top_yes_bid
    pa.field("yes_levels", pa.string(), nullable=True),  # JSON [[price,size],...] ascending
    pa.field("no_levels", pa.string(), nullable=True),
)

# Contract trade tape.
KALSHI_TRADE = _schema(
    pa.field("market_ticker", pa.string(), nullable=False),
    pa.field("window_start", TS_TYPE, nullable=False),
    pa.field("yes_price", pa.float64(), nullable=True),
    pa.field("count", pa.int64(), nullable=False),
    pa.field("taker_side", pa.string(), nullable=True),  # 'yes' | 'no'
)

# Window settlement (the label) + open-time reference price. Up to two rows per
# window: an open-time marker (ref_price set, result NULL) written by market
# discovery, and the settled row (result set) written by the settlement poller.
# Filter ``result IS NOT NULL`` for labels.
KALSHI_SETTLE = _schema(
    pa.field("market_ticker", pa.string(), nullable=False),
    pa.field("window_start", TS_TYPE, nullable=False),
    pa.field("ref_price", pa.float64(), nullable=True),  # reference price at open
    pa.field("settlement_value", pa.float64(), nullable=True),  # reference value at close
    pa.field("result", pa.string(), nullable=True),  # 'yes' | 'no'
    pa.field("determined_ts", TS_TYPE, nullable=True),
)

SCHEMAS: dict[str, pa.Schema] = {
    "kalshi_book": KALSHI_BOOK,
    "kalshi_trade": KALSHI_TRADE,
    "kalshi_settle": KALSHI_SETTLE,
}
