"""Append-only file (AOF) persistence.

Every write command is appended to a file as one JSON array per line. On start
the file is replayed to rebuild the dataset. Two details keep replay exact:

  1. Commands are logged in canonical form with *absolute* deadlines
     (`SET k v EX 10` is logged as `SET k v PXAT <epoch-ms>`).
  2. When a key expires, a DEL is logged, and expiry is switched off while
     replaying. So replay never has to guess whether a key "would have" expired
     by the time a later command ran.

A crash can leave a half-written last line; replay stops there and truncates it.
`rewrite` compacts the log into one SET per live key.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Callable, Iterable

log = logging.getLogger("kvstore.aof")

FSYNC_POLICIES = ("always", "everysec", "no")


class AOF:
    def __init__(self, path: str, fsync: str = "everysec"):
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}")
        self.path = path
        self.fsync = fsync
        self._f = None
        self._dirty = False

    def replay(self, execute: Callable[[list], object]) -> int:
        """Feed every logged command to `execute`. Returns the number replayed."""
        if not os.path.exists(self.path):
            return 0
        count = 0
        good_offset = 0
        corrupt = False
        with open(self.path, "rb") as f:
            for raw in f:
                try:
                    args = json.loads(raw.decode("utf-8"))
                    if not (isinstance(args, list) and args and all(isinstance(a, str) for a in args)):
                        raise ValueError("not a command")
                except ValueError:
                    corrupt = True
                    break
                execute(args)
                good_offset += len(raw)
                count += 1
        if corrupt:
            log.warning("corrupt AOF tail at byte %d; truncating", good_offset)
            with open(self.path, "r+b") as f:
                f.truncate(good_offset)
        return count

    def open(self) -> None:
        self._f = open(self.path, "ab")

    def append(self, args: list) -> None:
        line = json.dumps(args, ensure_ascii=False) + "\n"
        self._f.write(line.encode("utf-8"))
        self._f.flush()
        if self.fsync == "always":
            os.fsync(self._f.fileno())
        else:
            self._dirty = True

    def sync(self) -> None:
        """Flush to disk if anything is pending (called ~1/sec for 'everysec')."""
        if self._f and self._dirty and self.fsync != "no":
            self._f.flush()
            os.fsync(self._f.fileno())
            self._dirty = False

    def rewrite(self, entries: Iterable[list]) -> None:
        """Replace the log with a compact one built from `entries`."""
        tmp = self.path + ".rewrite"
        with open(tmp, "wb") as f:
            for args in entries:
                f.write((json.dumps(args, ensure_ascii=False) + "\n").encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        self._f.close()  # must be closed before replacing the file on Windows
        os.replace(tmp, self.path)
        self.open()
        self._dirty = False

    def close(self) -> None:
        if self._f:
            self.sync()
            self._f.close()
            self._f = None
