# kalshi-ws-collector

A production-grade market-data collector for **Kalshi's 15-minute crypto
markets** (BTC, ETH, SOL, XRP). It discovers the rotating 15-minute contracts,
mirrors each contract's order book from the live WebSocket feed with
sequence-gap resync, captures the trade tape, and polls settlements for the
outcome labels. Everything writes through a small, pluggable sink interface.

Capture and storage are separate concerns here. Point the collectors at the
bundled Parquet or JSONL sink, or implement a three-method protocol and send
records to Kafka, a database, or S3.

## Why

Kalshi's 15-minute crypto markets open, trade, and settle on a tight cadence:
a new market per asset every 15 minutes, aligned to the `:00 / :15 / :30 / :45`
UTC boundaries. A naive `websocket.recv()` loop gets three things wrong:

1. **The instrument keeps changing.** The active ticker rotates every 15
   minutes. You can't subscribe once and forget it, you have to discover the
   current market and re-subscribe as windows roll over.
2. **The order book is a delta stream.** You get one snapshot, then incremental
   changes. Miss a message and your book silently diverges from the exchange's
   until you rebuild it. You need to detect that and recover.
3. **The label lives somewhere else.** The outcome (`yes`/`no`) only exists
   after the window settles, via REST, not on the trade socket.

This project handles all three and keeps the storage decision behind a sink, so
the capture logic stays reusable and testable.

## Install

Requires Python 3.11+.

```bash
pip install -e ".[dev]"     # editable install with test/lint extras
```

## Credentials

Kalshi generates the key pair for you. There is no `openssl` generate-and-upload
step. On your [kalshi.com](https://kalshi.com) account page, open the API keys
section and create a key. You get back:

- an **API key id**, and
- a one-time download of the **private key** as a `.pem` file.

Provide them via environment variables (see [`.env.example`](.env.example)):

```bash
export KALSHI_API_KEY_ID="<your key id>"
export KALSHI_PRIVATE_KEY_PATH="/path/to/kalshi.pem"
# or, inline instead of a file (takes precedence if both are set):
# export KALSHI_PRIVATE_KEY_PEM="$(cat /path/to/kalshi.pem)"
```

Every REST request and the WebSocket handshake are signed with RSA-PSS
(`timestamp + METHOD + path`, base64-encoded) as Kalshi requires.

## Quickstart

Run the whole stack against live data:

```bash
python examples/run_collectors.py --sink parquet --out ./data
python examples/run_collectors.py --sink jsonl --out ./data --assets BTC ETH
```

Or wire it up yourself:

```python
import asyncio
from kalshi_ws_collector import (
    KalshiMarketDiscovery, KalshiBookCollector,
    KalshiTradeCollector, KalshiSettlementPoller, ParquetSink,
)

async def main():
    sink = ParquetSink("./data")
    assets = ["BTC", "ETH", "SOL", "XRP"]

    discovery = KalshiMarketDiscovery(assets, sink)
    book = KalshiBookCollector(assets, sink, discovery=discovery)
    trade = KalshiTradeCollector(assets, sink, discovery=discovery)
    settle = KalshiSettlementPoller(assets, sink)

    await asyncio.gather(
        discovery.run(), book.run(), trade.run(), settle.run(),
    )

asyncio.run(main())
```

Read the Parquet back with any Arrow-aware tool:

```python
from kalshi_ws_collector import read_parquet
table = read_parquet("./data", "kalshi_book", "BTC")
print(table.to_pandas().tail())
```

## Architecture

```
                    ┌─────────────────────────┐
   REST /markets ──▶│  KalshiMarketDiscovery   │  boundary-aware polling
                    │  asset → ticker map      │  writes open-time marker
                    └────────────┬─────────────┘
                       ticker map │ (shared in-process)
             ┌────────────────────┼────────────────────┐
             ▼                    ▼                     │
   ┌───────────────────┐  ┌───────────────────┐        │
   │ KalshiBookColl.   │  │ KalshiTradeColl.  │        │
   │ orderbook_delta   │  │ trade             │        │
   │ • snapshot+delta  │  │ • trade tape      │        │
   │ • seq-gap resync  │  │                   │        │
   └─────────┬─────────┘  └─────────┬─────────┘        │
             │                      │                  ▼
             │                      │        ┌───────────────────────┐
             │                      │        │ KalshiSettlementPoller│
             │                      │        │ REST → result label   │
             │                      │        └───────────┬───────────┘
             ▼                      ▼                    ▼
         ┌──────────────────────────────────────────────────┐
         │             Sink.write(Record)                    │
         │        ParquetSink │ JsonlSink │ your own          │
         └──────────────────────────────────────────────────┘
```

### 1. Discovery (`discovery.py`)

`KalshiMarketDiscovery` polls `GET /markets` per series and maintains the
`asset → (ticker, window_start, ref_price)` map the WS collectors read. Polling
is boundary-aware: the active ticker only changes at 15-minute boundaries, so
it polls fast for a short stretch right after each boundary (until each
market's reference price lands), then idles until just before the next one.
That keeps discovery REST traffic far below a blind fixed interval. The first
time a window's ticker appears with a reference price, it emits an open-time
`kalshi_settle` marker row (reference price set, result `NULL`).

### 2. Book mirror (`orderbook.py`, `collectors.py`)

`KalshiBookCollector` drives the `orderbook_delta` channel. Kalshi sends one
`orderbook_snapshot` establishing the full book, then a stream of
`orderbook_delta` size changes. The collector mirrors each contract's book
locally as two ascending price/size stacks (`yes` and `no`) and emits
top-of-book plus full ladders on every message. Buying `no` at price *p* is
economically selling `yes` at `1 - p`, so the asks are derived:
`yes_ask = 1 - top_no_bid` and `no_ask = 1 - top_yes_bid`. The book-mirroring
math lives in pure functions with no I/O, which is what makes it fully
unit-testable.

### 3. Resync (`collectors.py`)

Each message carries a per-subscription sequence number. Snapshots reset the
watermark; deltas must increment it by exactly one. If the collector sees a gap
(a frame was dropped), it requests a fresh snapshot via `get_snapshot` and
discards the out-of-order frame. The snapshot rebuilds the book from scratch
rather than letting it silently drift. It also drops any delta that arrives
before this connection's snapshot (straddling a reconnect, say), so a stale
book can never be updated. On reconnect, all per-connection book state is
cleared.

### 4. Settlement labeling (`settlement.py`)

`KalshiSettlementPoller` polls for settled markets and emits the `kalshi_settle`
row carrying the authoritative `result` (`yes`/`no`). The determined timestamp
uses a fallback chain (`settled_time → determination_time → close_time →
expiration_time`) because which field is populated varies with how a window
resolved. Only rows whose `result` is `yes` or `no` are written. Paired with
discovery's open-time marker, every window ends up with a full open-to-settle
record; filter `result IS NOT NULL` for training labels.

### 5. Sinks (`sinks.py`)

A `Sink` is anything with `write(record)`, `flush()`, and `close()`. A `Record`
is `(data_type, symbol, fields)`. Two references ship:

- **`ParquetSink`** - buffered, partitioned Parquet
  (`data_type=<dt>/symbol=<sym>/part-NNNNN.parquet`), written atomically
  (temp file + rename) so readers never see partial files. Columnar and
  query-friendly.
- **`JsonlSink`** - one newline-delimited JSON file per `(data_type, symbol)`.
  Stdlib-only, good for quick capture and debugging.

Implement the protocol to send records anywhere else.

## Data model

Every record carries an envelope (`event_ts`, `ingest_ts`, `seq`, and `raw`,
the original JSON payload kept as the source of truth) plus type-specific
fields. Prices are normalised to **dollars in [0, 1]**. Schemas are defined in
[`schema.py`](src/kalshi_ws_collector/schema.py).

| data_type       | key fields                                                        |
|-----------------|-------------------------------------------------------------------|
| `kalshi_book`   | `market_ticker`, `window_start`, `yes_bid/ask`, `no_bid/ask`, `yes_levels`, `no_levels` |
| `kalshi_trade`  | `market_ticker`, `window_start`, `yes_price`, `count`, `taker_side` |
| `kalshi_settle` | `market_ticker`, `window_start`, `ref_price`, `settlement_value`, `result`, `determined_ts` |

## Design notes

- **Resilience.** A shared `Collector` base provides exponential-backoff
  reconnect (1s to 30s), a periodic heartbeat, periodic idle flushing of the
  sink, an optional stale-tick guard, and cooperative shutdown via `stop()`.
- **Idempotency.** Discovery and the settlement poller each write a window's row
  at most once per process. Prime their `seen_tickers` set on startup to keep
  restarts from double-writing.
- **Testability.** All parsing, book mirroring, sequence-gap handling, cadence
  math, and settlement fallback logic run under an offline test suite. No
  network or credentials required.

## Development

```bash
pip install -e ".[dev]"
pytest          # offline test suite
ruff check .    # lint
```

CI runs the suite and lint on Python 3.11 and 3.12
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## License

MIT. See [LICENSE](LICENSE).
