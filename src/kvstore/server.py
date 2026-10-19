"""asyncio TCP server: one task per connection, one event loop for everything.

Single-threaded by design (like Redis): commands never interleave, so INCR and
friends are atomic without locks. Throughput comes from cheap connections and
pipelining, not parallelism.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .aof import AOF
from .engine import Engine
from .protocol import MAX_INLINE, Error, ProtocolError, Simple, encode, read_command
from .store import Store

log = logging.getLogger("kvstore.server")


class Server:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        aof_path: Optional[str] = None,
        fsync: str = "everysec",
        tick: float = 0.1,
    ):
        self.host, self._port, self.tick = host, port, tick
        aof = AOF(aof_path, fsync) if aof_path else None
        self.engine = Engine(Store(), aof)
        self._server: Optional[asyncio.AbstractServer] = None
        self._housekeeping: Optional[asyncio.Task] = None
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def port(self) -> int:
        """The bound port (useful when started with port=0)."""
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> None:
        replayed = self.engine.load()
        if replayed:
            log.info("replayed %d commands from the append-only file", replayed)
        self._server = await asyncio.start_server(
            self._handle, self.host, self._port, limit=MAX_INLINE
        )
        self._housekeeping = asyncio.create_task(self._housekeep())

    async def serve_forever(self) -> None:
        await self._server.serve_forever()

    async def stop(self) -> None:
        if self._housekeeping:
            self._housekeeping.cancel()
        if self._server:
            self._server.close()
        for w in list(self._writers):
            w.close()
        if self._server:
            await self._server.wait_closed()
        self.engine.close()

    async def _housekeep(self) -> None:
        """Active key expiry every tick; fsync the log about once a second."""
        ticks = 0
        while True:
            await asyncio.sleep(self.tick)
            self.engine.active_expire()
            ticks += 1
            if ticks % max(1, round(1 / self.tick)) == 0 and self.engine.aof:
                self.engine.aof.sync()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        try:
            while True:
                try:
                    args = await read_command(reader)
                except ProtocolError as e:
                    writer.write(encode(Error(f"ERR Protocol error: {e}")))
                    await writer.drain()
                    break
                if args is None:
                    break
                if not args:
                    continue
                if args[0].upper() == "QUIT":
                    writer.write(encode(Simple("OK")))
                    await writer.drain()
                    break
                writer.write(encode(self.engine.execute(args)))
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
