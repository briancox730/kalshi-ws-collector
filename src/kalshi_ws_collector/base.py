"""Async WebSocket collector base class.

Encapsulates the lifecycle that is identical across streaming collectors:

  * connect → subscribe → consume forever
  * exponential-backoff reconnect on dropout (1s → 30s, capped)
  * periodic heartbeat (logged, plus an optional callback)
  * stamps ``ingest_ts`` on every parsed row
  * optional stale-tick guard (drop rows older than ``stale_seconds``)
  * emits rows to a pluggable :class:`~kalshi_ws_collector.sinks.Sink`
  * flushes the sink on a periodic idle tick
  * cooperative shutdown via :meth:`stop`

Subclasses implement three small hooks — :meth:`ws_url`,
:meth:`subscribe_message`, :meth:`parse` — and must set the ``venue`` and
``data_type`` class attributes. A subclass whose work loop is not the default
connect/subscribe/consume (e.g. a REST poller) overrides :meth:`run` to pass its
own coroutine to :meth:`_run_lifecycle`, keeping the same heartbeat / flush /
stop semantics.
"""

from __future__ import annotations

import abc
import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime

import websockets

from kalshi_ws_collector.sinks import Record, Sink

logger = logging.getLogger(__name__)

HeartbeatCallback = Callable[[str, str, "str | None"], None]


class Collector(abc.ABC):
    """One asyncio task per running instance. Safe to run many in one loop."""

    # Class attrs — subclasses must override.
    venue: str = ""
    data_type: str = ""  # primary label for the heartbeat id

    INITIAL_BACKOFF: float = 1.0
    MAX_BACKOFF: float = 30.0
    HEARTBEAT_INTERVAL: float = 5.0
    IDLE_FLUSH_INTERVAL: float = 5.0
    WS_PING_INTERVAL: float = 20.0
    # Default websockets max frame size is 1 MiB — too small for large L2
    # snapshots. 16 MiB is comfortably bigger than any snapshot we expect.
    WS_MAX_MESSAGE_SIZE: int = 16 * 1024 * 1024
    # Default open_timeout is 10s. With many collectors handshaking at startup,
    # TLS bring-up can serialize enough to trip it; 30s gives slack without
    # changing steady-state reconnect behaviour.
    WS_OPEN_TIMEOUT: float = 30.0

    def __init__(
        self,
        symbols: list[str],
        sink: Sink,
        *,
        stale_seconds: float | None = None,
        on_heartbeat: HeartbeatCallback | None = None,
    ) -> None:
        if not self.venue or not self.data_type:
            raise TypeError(
                f"{type(self).__name__} must set class attrs 'venue' and 'data_type'"
            )
        self.symbols = list(symbols)
        self.sink = sink
        self._stale_seconds = stale_seconds
        self._on_heartbeat = on_heartbeat
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def ws_url(self) -> str: ...

    @abc.abstractmethod
    def subscribe_message(self) -> str:
        """Return the JSON-encoded subscribe payload to send on connect."""

    @abc.abstractmethod
    def parse(self, message: str) -> Iterable[tuple[str, str, dict]]:
        """Parse one WS frame into ``(data_type, symbol, row)`` tuples.

        Yield nothing to drop the message. Each row must include a datetime
        ``event_ts``; ``ingest_ts`` is added if missing.
        """

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def collector_id(self) -> str:
        if not self.symbols:
            sym_label = "all"
        elif len(self.symbols) <= 3:
            sym_label = ",".join(self.symbols)
        else:
            sym_label = f"{len(self.symbols)}sym"
        return f"{self.venue}:{self.data_type}:{sym_label}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self._run_lifecycle(self._ws_loop())

    async def _run_lifecycle(self, work: Awaitable[None]) -> None:
        """Start flush + heartbeat tasks, await ``work``, tear down cleanly."""
        self._beat("starting")
        flush_task = asyncio.create_task(self._idle_flush_loop())
        beat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await work
        finally:
            for t in (flush_task, beat_task):
                t.cancel()
            for t in (flush_task, beat_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            try:
                self.sink.flush()
            except Exception:  # noqa: BLE001
                logger.exception("%s: final flush failed", self.collector_id)
            self._beat("stopped")

    async def _ws_loop(self) -> None:
        """Connect → subscribe → consume → reconnect with exponential backoff."""
        backoff = self.INITIAL_BACKOFF
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self.ws_url(),
                    ping_interval=self.WS_PING_INTERVAL,
                    max_size=self.WS_MAX_MESSAGE_SIZE,
                    open_timeout=self.WS_OPEN_TIMEOUT,
                ) as ws:
                    await ws.send(self.subscribe_message())
                    backoff = self.INITIAL_BACKOFF
                    try:
                        await self._resync_after_reconnect()
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "%s: _resync_after_reconnect failed", self.collector_id
                        )
                    # Beat "running" AFTER resync so the heartbeat does not lie
                    # while a slow resync is still catching up.
                    self._beat("running")
                    keepalive = asyncio.create_task(self._application_keepalive(ws))
                    try:
                        async for message in ws:
                            self._handle(message)
                            if self._stop.is_set():
                                break
                    finally:
                        keepalive.cancel()
                        try:
                            await keepalive
                        except (asyncio.CancelledError, Exception):  # noqa: BLE001
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "%s: WS error %r; reconnecting in %.1fs",
                    self.collector_id, exc, backoff,
                )
                self._beat("reconnecting", error_message=repr(exc))
                if await self._sleep_or_stop(backoff):
                    break
                backoff = min(backoff * 2, self.MAX_BACKOFF)

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _handle(self, message: str | bytes) -> None:
        if isinstance(message, (bytes, bytearray)):
            message = bytes(message).decode("utf-8", errors="replace")
        if self.is_keepalive_frame(message):
            return
        try:
            rows = list(self.parse(message))
        except Exception:  # noqa: BLE001
            logger.exception("%s: parse failed", self.collector_id)
            return
        ingest_ts = datetime.now(UTC)
        for data_type, symbol, row in rows:
            row.setdefault("ingest_ts", ingest_ts)
            if self._stale_seconds is not None and self._is_stale(row, ingest_ts):
                continue
            self._emit(data_type, symbol, row)

    def _emit(self, data_type: str, symbol: str, row: dict) -> None:
        try:
            self.sink.write(Record(data_type=data_type, symbol=symbol, fields=row))
        except Exception:  # noqa: BLE001
            logger.exception(
                "%s: sink.write failed (dt=%s sym=%s)",
                self.collector_id, data_type, symbol,
            )

    def _is_stale(self, row: dict, ingest_ts: datetime) -> bool:
        event_ts = row.get("event_ts")
        if not isinstance(event_ts, datetime):
            return False
        et = event_ts if event_ts.tzinfo else event_ts.replace(tzinfo=UTC)
        return (ingest_ts - et).total_seconds() > self._stale_seconds

    async def _resync_after_reconnect(self) -> None:
        """Hook: re-establish state after a WS reconnect. Default no-op."""
        return

    def is_keepalive_frame(self, message: str) -> bool:
        """Return True for protocol frames the parser should skip. Default False."""
        return False

    async def _application_keepalive(self, ws) -> None:
        """Hook: send venue-specific application-level pings. Default no-op."""
        return

    async def _idle_flush_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.IDLE_FLUSH_INTERVAL)
                try:
                    self.sink.flush()
                except Exception:  # noqa: BLE001
                    logger.exception("%s: idle flush failed", self.collector_id)
        except asyncio.CancelledError:
            pass

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                self._beat("running")
        except asyncio.CancelledError:
            pass

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep up to ``seconds``; return True if stop was requested meanwhile."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
            return True
        except TimeoutError:
            return False

    def _beat(self, status: str, *, error_message: str | None = None) -> None:
        logger.debug("%s: heartbeat %s", self.collector_id, status)
        if self._on_heartbeat is not None:
            try:
                self._on_heartbeat(self.collector_id, status, error_message)
            except Exception:  # noqa: BLE001
                logger.exception("%s: heartbeat callback failed", self.collector_id)
