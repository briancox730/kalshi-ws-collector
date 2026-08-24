"""Run the full Kalshi collector stack against live market data.

Wires market discovery, the order-book and trade WebSocket collectors, and the
settlement poller to a single sink, then runs them concurrently until you
interrupt with Ctrl+C.

Usage:
    # credentials come from the environment (see .env.example)
    export KALSHI_API_KEY_ID=...
    export KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi.pem

    python examples/run_collectors.py --sink parquet --out ./data
    python examples/run_collectors.py --sink jsonl   --out ./data --assets BTC ETH
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from kalshi_ws_collector import (
    KALSHI_ASSETS,
    JsonlSink,
    KalshiBookCollector,
    KalshiMarketDiscovery,
    KalshiSettlementPoller,
    KalshiTradeCollector,
    ParquetSink,
    creds_present,
)


def build_sink(kind: str, out: str):
    if kind == "parquet":
        return ParquetSink(out)
    if kind == "jsonl":
        return JsonlSink(out)
    raise ValueError(f"unknown sink: {kind!r}")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink", choices=["parquet", "jsonl"], default="parquet")
    parser.add_argument("--out", default="./data", help="output directory")
    parser.add_argument("--assets", nargs="+", default=KALSHI_ASSETS,
                        help="assets to collect (default: BTC ETH SOL XRP)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not creds_present():
        raise SystemExit(
            "no Kalshi credentials found — set KALSHI_API_KEY_ID and "
            "KALSHI_PRIVATE_KEY_PATH (or KALSHI_PRIVATE_KEY_PEM). See .env.example."
        )

    sink = build_sink(args.sink, args.out)

    # Discovery resolves the rotating tickers; the WS collectors read its map.
    discovery = KalshiMarketDiscovery(args.assets, sink)
    book = KalshiBookCollector(args.assets, sink, discovery=discovery)
    trade = KalshiTradeCollector(args.assets, sink, discovery=discovery)
    settle = KalshiSettlementPoller(args.assets, sink)
    collectors = [discovery, book, trade, settle]

    stop = asyncio.Event()

    def request_stop(*_):
        logging.info("shutdown requested")
        stop.set()
        for c in collectors:
            c.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:  # Windows: fall back to KeyboardInterrupt
            pass

    tasks = [asyncio.create_task(c.run()) for c in collectors]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        request_stop()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        sink.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
