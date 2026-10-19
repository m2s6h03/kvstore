from __future__ import annotations

import argparse
import asyncio
import logging

from .aof import FSYNC_POLICIES
from .server import Server


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="kvstore", description="Redis-compatible key-value server.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("--aof", metavar="PATH", help="enable persistence with this append-only file")
    p.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    async def run() -> None:
        server = Server(args.host, args.port, args.aof, args.fsync)
        await server.start()
        logging.info("listening on %s:%d (aof=%s)", args.host, server.port, args.aof or "off")
        try:
            await server.serve_forever()
        finally:
            await server.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("shut down")


if __name__ == "__main__":
    main()
