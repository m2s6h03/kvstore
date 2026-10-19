"""Command execution: parses arguments, calls the Store, logs writes to the AOF.

The engine is synchronous and does not know about sockets, which makes it easy
to test. Because the server runs it on a single asyncio event loop, each
command runs to completion before the next starts: every command is atomic and
no locks are needed.
"""
from __future__ import annotations

import logging
import re
from typing import Callable, Optional

from .aof import AOF
from .protocol import Error, Simple
from .store import INT_MAX, INT_MIN, Store

log = logging.getLogger("kvstore.engine")

OK = Simple("OK")
_INT_RE = re.compile(r"-?[0-9]+")  # ASCII only: \d would accept e.g. Arabic-Indic digits


class CommandError(Exception):
    """Raised by handlers; turned into a RESP error reply."""


def parse_int(text: str) -> int:
    if not _INT_RE.fullmatch(text):
        raise CommandError("ERR value is not an integer or out of range")
    n = int(text)
    if not INT_MIN <= n <= INT_MAX:
        raise CommandError("ERR value is not an integer or out of range")
    return n


SYNTAX = "ERR syntax error"


class Engine:
    def __init__(self, store: Optional[Store] = None, aof: Optional[AOF] = None):
        self.store = store or Store()
        self.aof = aof
        self._replaying = False
        self.store.on_expire = lambda key: self._log(["DEL", key])
        # name -> (handler, min_args, max_args or None for unlimited)
        self._commands: dict[str, tuple[Callable, int, Optional[int]]] = {
            "PING": (self.c_ping, 0, 1),
            "ECHO": (self.c_echo, 1, 1),
            "SET": (self.c_set, 2, None),
            "GET": (self.c_get, 1, 1),
            "DEL": (self.c_del, 1, None),
            "EXISTS": (self.c_exists, 1, None),
            "EXPIRE": (self.c_expire, 2, 2),
            "PEXPIRE": (self.c_pexpire, 2, 2),
            "PEXPIREAT": (self.c_pexpireat, 2, 2),
            "TTL": (self.c_ttl, 1, 1),
            "PTTL": (self.c_pttl, 1, 1),
            "PERSIST": (self.c_persist, 1, 1),
            "INCR": (lambda k: self._incr(k, 1), 1, 1),
            "DECR": (lambda k: self._incr(k, -1), 1, 1),
            "INCRBY": (lambda k, n: self._incr(k, parse_int(n)), 2, 2),
            "DECRBY": (lambda k, n: self._incr(k, -parse_int(n)), 2, 2),
            "KEYS": (self.c_keys, 1, 1),
            "DBSIZE": (self.c_dbsize, 0, 0),
            "FLUSHALL": (self.c_flushall, 0, 0),
            "REWRITEAOF": (self.c_rewriteaof, 0, 0),
            "COMMAND": (lambda *a: [], 0, None),  # keeps redis-cli happy on connect
            "HELLO": (self.c_hello, 0, None),
            "CLIENT": (self.c_client, 1, None),
            "SELECT": (self.c_select, 1, 1),
        }

    # -- lifecycle ---------------------------------------------------------
    def load(self) -> int:
        """Replay the AOF (if any) and start appending to it."""
        if not self.aof:
            return 0
        self._replaying = True
        self.store.expiry_enabled = False
        try:
            count = self.aof.replay(self._replay_one)
        finally:
            self._replaying = False
            self.store.expiry_enabled = True
        self.aof.open()
        return count

    def _replay_one(self, args: list) -> None:
        reply = self.execute(args)
        if isinstance(reply, Error):
            log.warning("AOF command %r failed on replay: %s", args[0], reply)

    def active_expire(self) -> int:
        return self.store.active_expire()

    def close(self) -> None:
        if self.aof:
            self.aof.close()

    def _log(self, args: list) -> None:
        if self.aof and not self._replaying:
            self.aof.append(args)

    # -- dispatch ----------------------------------------------------------
    def execute(self, args: list):
        name = args[0].upper()
        entry = self._commands.get(name)
        if entry is None:
            return Error(f"ERR unknown command '{args[0]}'")
        handler, lo, hi = entry
        n = len(args) - 1
        if n < lo or (hi is not None and n > hi):
            return Error(f"ERR wrong number of arguments for '{name.lower()}' command")
        try:
            return handler(*args[1:])
        except CommandError as e:
            return Error(str(e))
        except Exception:  # a bug must not take the connection down
            log.exception("internal error running %r", name)
            return Error("ERR internal error")

    # -- commands ----------------------------------------------------------
    def c_ping(self, message=None):
        return message if message is not None else Simple("PONG")

    # Connection-handshake commands that real client libraries send on connect.
    def c_hello(self, *args):
        if args and args[0] != "2":
            # RESP3 is not implemented; clients fall back to RESP2 on this error.
            raise CommandError("NOPROTO sorry, this protocol version is not supported")
        return ["server", "kvstore", "version", "0.1.0", "proto", 2, "id", 1,
                "mode", "standalone", "role", "master", "modules", []]

    def c_client(self, sub, *args):
        if sub.upper() in ("SETNAME", "SETINFO"):
            return OK
        raise CommandError(f"ERR unknown subcommand '{sub}'")

    def c_select(self, index):
        if index != "0":
            raise CommandError("ERR DB index is out of range (only database 0 exists)")
        return OK

    def c_echo(self, message):
        return message

    def c_set(self, key, value, *opts):
        nx = xx = False
        deadline = None
        i = 0
        while i < len(opts):
            opt = opts[i].upper()
            if opt == "NX":
                nx = True
            elif opt == "XX":
                xx = True
            elif opt in ("EX", "PX", "PXAT"):
                if deadline is not None or i + 1 >= len(opts):
                    raise CommandError(SYNTAX)
                i += 1
                n = parse_int(opts[i])
                if n <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")
                if opt == "EX":
                    deadline = self.store.now_ms() + n * 1000
                elif opt == "PX":
                    deadline = self.store.now_ms() + n
                else:
                    deadline = n
            else:
                raise CommandError(SYNTAX)
            i += 1
        if nx and xx:
            raise CommandError(SYNTAX)
        exists = self.store.exists(key)
        if (nx and exists) or (xx and not exists):
            return None
        self.store.set(key, value, deadline)
        entry = ["SET", key, value]
        if deadline is not None:
            entry += ["PXAT", str(deadline)]
        self._log(entry)
        return OK

    def c_get(self, key):
        return self.store.get(key)

    def c_del(self, *keys):
        deleted = [k for k in keys if self.store.delete(k)]
        if deleted:
            self._log(["DEL", *deleted])
        return len(deleted)

    def c_exists(self, *keys):
        return sum(1 for k in keys if self.store.exists(k))

    def _expire_at(self, key: str, deadline: int) -> int:
        if not self.store.exists(key):
            return 0
        if deadline <= self.store.now_ms():  # already in the past: delete now
            self.store.delete(key)
            self._log(["DEL", key])
            return 1
        self.store.set_deadline(key, deadline)
        self._log(["PEXPIREAT", key, str(deadline)])
        return 1

    def c_expire(self, key, seconds):
        return self._expire_at(key, self.store.now_ms() + parse_int(seconds) * 1000)

    def c_pexpire(self, key, ms):
        return self._expire_at(key, self.store.now_ms() + parse_int(ms))

    def c_pexpireat(self, key, deadline):
        return self._expire_at(key, parse_int(deadline))

    def c_ttl(self, key):
        ms = self.store.ttl_ms(key)
        return ms if ms < 0 else (ms + 500) // 1000

    def c_pttl(self, key):
        return self.store.ttl_ms(key)

    def c_persist(self, key):
        if self.store.persist(key):
            self._log(["PERSIST", key])
            return 1
        return 0

    def _incr(self, key, delta):
        try:
            value = self.store.incrby(key, delta)
        except ValueError:
            raise CommandError("ERR value is not an integer or out of range") from None
        self._log(["INCRBY", key, str(delta)])
        return value

    def c_keys(self, pattern):
        return self.store.keys(pattern)

    def c_dbsize(self):
        return self.store.dbsize()

    def c_flushall(self):
        self.store.flush()
        self._log(["FLUSHALL"])
        return OK

    def c_rewriteaof(self):
        if not self.aof:
            raise CommandError("ERR AOF is not enabled")
        self.aof.rewrite(self._dump())
        return Simple("Background append only file rewriting started")

    def _dump(self):
        for key, value, deadline in self.store.items():
            entry = ["SET", key, value]
            if deadline is not None:
                entry += ["PXAT", str(deadline)]
            yield entry
