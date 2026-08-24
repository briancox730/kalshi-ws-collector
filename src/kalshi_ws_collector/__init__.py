"""A production-grade WebSocket collector for Kalshi 15-minute crypto markets.

Discovers the rotating 15-minute contracts, mirrors each contract's order book
from ``orderbook_snapshot`` / ``orderbook_delta`` frames (with sequence-gap
resync), captures the trade tape, and polls settlements for the labels — writing
everything through a small pluggable sink interface.
"""

from __future__ import annotations

from kalshi_ws_collector.auth import (
    KALSHI_API_BASE,
    KALSHI_ASSETS,
    KALSHI_SERIES,
    KALSHI_WS_URL,
    auth_headers,
    creds_present,
    load_private_key,
    ws_auth_headers,
)
from kalshi_ws_collector.base import Collector
from kalshi_ws_collector.collectors import (
    KalshiBookCollector,
    KalshiTradeCollector,
)
from kalshi_ws_collector.discovery import (
    KalshiMarketDiscovery,
    MarketInfo,
    parse_open_market,
)
from kalshi_ws_collector.settlement import (
    KalshiSettlementPoller,
    parse_settled_market,
)
from kalshi_ws_collector.sinks import (
    JsonlSink,
    MemorySink,
    ParquetSink,
    Record,
    Sink,
    read_jsonl,
    read_parquet,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # auth
    "KALSHI_API_BASE",
    "KALSHI_WS_URL",
    "KALSHI_ASSETS",
    "KALSHI_SERIES",
    "auth_headers",
    "ws_auth_headers",
    "load_private_key",
    "creds_present",
    # collectors
    "Collector",
    "KalshiMarketDiscovery",
    "KalshiBookCollector",
    "KalshiTradeCollector",
    "KalshiSettlementPoller",
    "MarketInfo",
    "parse_open_market",
    "parse_settled_market",
    # sinks
    "Sink",
    "Record",
    "JsonlSink",
    "ParquetSink",
    "MemorySink",
    "read_parquet",
    "read_jsonl",
]
