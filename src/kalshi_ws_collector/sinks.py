"""Record sinks: where parsed rows go.

The collectors do not know how records are stored. They emit :class:`Record`
objects to a :class:`Sink`, an object with ``write`` / ``flush`` / ``close``.
Two reference implementations ship here:

  * :class:`JsonlSink` — one newline-delimited JSON file per (data_type, symbol).
    Zero dependencies beyond the stdlib; ideal for quick capture and debugging.
  * :class:`ParquetSink` — buffered, partitioned Parquet via PyArrow, laid out as
    ``<root>/data_type=<dt>/symbol=<sym>/part-NNNNN.parquet``. Columnar and
    query-friendly for analytics.

Bring your own by implementing the :class:`Sink` protocol (e.g. a Kafka
producer, a database writer, an S3 uploader).
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_ws_collector.schema import SCHEMAS


@dataclass(frozen=True)
class Record:
    """One parsed row bound for a sink.

    ``data_type`` names the record shape ("kalshi_book" / "kalshi_trade" /
    "kalshi_settle"), ``symbol`` is the asset (BTC/ETH/SOL/XRP), and ``fields``
    is the row payload (which includes the envelope: event_ts, ingest_ts, ...).
    """

    data_type: str
    symbol: str
    fields: Mapping[str, Any]


@runtime_checkable
class Sink(Protocol):
    """The contract every sink implements."""

    def write(self, record: Record) -> None:
        """Store one record (may buffer)."""

    def flush(self) -> None:
        """Flush any buffered records to durable storage."""

    def close(self) -> None:
        """Flush and release resources. Idempotent."""


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"not JSON-serializable: {type(obj).__name__}")


class JsonlSink:
    """Append records as newline-delimited JSON, one file per (data_type, symbol).

    Files land at ``<root>/<data_type>/<symbol>.jsonl``. Datetimes are written as
    ISO-8601 strings. Thread-safe.
    """

    def __init__(self, root: str | os.PathLike) -> None:
        self._root = Path(root)
        self._handles: dict[tuple[str, str], Any] = {}
        self._lock = threading.Lock()

    def _handle(self, data_type: str, symbol: str):
        key = (data_type, symbol)
        handle = self._handles.get(key)
        if handle is None:
            path = self._root / data_type / f"{symbol}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = path.open("a", encoding="utf-8")
            self._handles[key] = handle
        return handle

    def write(self, record: Record) -> None:
        line = json.dumps(dict(record.fields), default=_json_default)
        with self._lock:
            handle = self._handle(record.data_type, record.symbol)
            handle.write(line + "\n")

    def flush(self) -> None:
        with self._lock:
            for handle in self._handles.values():
                handle.flush()

    def close(self) -> None:
        with self._lock:
            for handle in self._handles.values():
                try:
                    handle.close()
                except Exception:  # noqa: BLE001
                    pass
            self._handles.clear()


class ParquetSink:
    """Buffered, partitioned Parquet writer.

    Rows are buffered per (data_type, symbol) and flushed to
    ``<root>/data_type=<dt>/symbol=<sym>/part-NNNNN.parquet`` when the buffer
    reaches ``max_rows`` or on an explicit ``flush`` / ``close``. Each file is
    written to a temporary name and atomically renamed, so a reader never sees a
    partial file. Thread-safe.

    A row may carry keys absent from the registered schema — they are dropped;
    missing schema keys are written as null.
    """

    def __init__(
        self,
        root: str | os.PathLike,
        schemas: dict[str, pa.Schema] | None = None,
        *,
        max_rows: int = 10_000,
        compression: str = "zstd",
    ) -> None:
        self._root = Path(root)
        self._schemas = schemas if schemas is not None else SCHEMAS
        self._max_rows = max_rows
        self._compression = compression
        self._buffers: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def write(self, record: Record) -> None:
        if record.data_type not in self._schemas:
            raise KeyError(f"no schema registered for data_type={record.data_type!r}")
        with self._lock:
            key = (record.data_type, record.symbol)
            buf = self._buffers.setdefault(key, [])
            buf.append(dict(record.fields))
            if len(buf) >= self._max_rows:
                self._flush_key(key)

    def flush(self) -> None:
        with self._lock:
            for key in list(self._buffers):
                self._flush_key(key)

    def close(self) -> None:
        self.flush()

    # -- internals ---------------------------------------------------------

    def _flush_key(self, key: tuple[str, str]) -> None:
        rows = self._buffers.get(key)
        if not rows:
            return
        data_type, symbol = key
        schema = self._schemas[data_type]
        table = pa.Table.from_pylist(rows, schema=schema)
        part_dir = self._root / f"data_type={data_type}" / f"symbol={symbol}"
        part_dir.mkdir(parents=True, exist_ok=True)
        index = _next_part_index(part_dir)
        final = part_dir / f"part-{index:05d}.parquet"
        tmp = part_dir / f".part-{index:05d}.parquet.tmp"
        pq.write_table(table, tmp, compression=self._compression)
        os.replace(tmp, final)
        self._buffers[key] = []


def _next_part_index(part_dir: Path) -> int:
    max_idx = -1
    for p in part_dir.glob("part-*.parquet"):
        try:
            idx = int(p.stem.split("-", 1)[1])
        except (IndexError, ValueError):
            continue
        max_idx = max(max_idx, idx)
    return max_idx + 1


def read_parquet(root: str | os.PathLike, data_type: str, symbol: str) -> pa.Table:
    """Read every part file written for one (data_type, symbol) into a Table."""
    part_dir = Path(root) / f"data_type={data_type}" / f"symbol={symbol}"
    files = sorted(part_dir.glob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet files under {part_dir}")
    return pa.concat_tables([pq.read_table(f) for f in files])


def read_jsonl(root: str | os.PathLike, data_type: str, symbol: str) -> list[dict[str, Any]]:
    """Read every record written for one (data_type, symbol) from JSONL."""
    path = Path(root) / data_type / f"{symbol}.jsonl"
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


@dataclass
class MemorySink:
    """In-process sink that keeps records in a list. Handy for tests and demos."""

    records: list[Record] = field(default_factory=list)

    def write(self, record: Record) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return

    def close(self) -> None:
        return
