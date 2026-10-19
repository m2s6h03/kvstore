"""The in-memory data structure: a dict of strings plus per-key expiry times.

Expiry is handled two ways, like Redis:
  * lazily  - a key past its deadline is removed the moment anyone touches it
  * actively - a min-heap of deadlines is swept periodically so keys nobody
    reads still get reclaimed

Whenever a key is removed because it expired, `on_expire(key)` is called. The
engine uses that to write a DEL to the append-only file, which is what makes
replaying the log deterministic (see aof.py).
"""
from __future__ import annotations

import fnmatch
import heapq
import time
from typing import Callable, Iterator, Optional

INT_MIN, INT_MAX = -(2**63), 2**63 - 1


class Store:
    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        on_expire: Optional[Callable[[str], None]] = None,
    ):
        self._data: dict[str, str] = {}
        self._expires: dict[str, int] = {}  # key -> absolute deadline, ms since epoch
        self._heap: list[tuple[int, str]] = []  # (deadline, key); may hold stale entries
        self._clock = clock
        self.on_expire = on_expire
        # Turned off while replaying the log so that time passing during startup
        # cannot change what the replayed commands see.
        self.expiry_enabled = True

    def now_ms(self) -> int:
        return int(self._clock() * 1000)

    # -- internals ---------------------------------------------------------
    def _remove(self, key: str) -> None:
        self._data.pop(key, None)
        self._expires.pop(key, None)

    def _expire_if_needed(self, key: str) -> bool:
        if not self.expiry_enabled:
            return False
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= self.now_ms():
            self._remove(key)
            if self.on_expire:
                self.on_expire(key)
            return True
        return False

    # -- reads -------------------------------------------------------------
    def get(self, key: str) -> Optional[str]:
        self._expire_if_needed(key)
        return self._data.get(key)

    def exists(self, key: str) -> bool:
        self._expire_if_needed(key)
        return key in self._data

    def keys(self, pattern: str = "*") -> list[str]:
        out = []
        for key in list(self._data):
            if self._expire_if_needed(key):
                continue
            if fnmatch.fnmatchcase(key, pattern):
                out.append(key)
        return out

    def dbsize(self) -> int:
        return len(self.keys("*"))

    def ttl_ms(self, key: str) -> int:
        """-2 if the key does not exist, -1 if it has no expiry, else ms left."""
        if not self.exists(key):
            return -2
        deadline = self._expires.get(key)
        if deadline is None:
            return -1
        return max(0, deadline - self.now_ms())

    def items(self) -> Iterator[tuple[str, str, Optional[int]]]:
        """Live (key, value, deadline) triples, used to rewrite the log."""
        now = self.now_ms()
        for key, value in self._data.items():
            deadline = self._expires.get(key)
            if self.expiry_enabled and deadline is not None and deadline <= now:
                continue
            yield key, value, deadline

    # -- writes ------------------------------------------------------------
    def set(self, key: str, value: str, deadline_ms: Optional[int] = None) -> None:
        self._data[key] = value
        if deadline_ms is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = deadline_ms
            heapq.heappush(self._heap, (deadline_ms, key))

    def delete(self, key: str) -> bool:
        if self._expire_if_needed(key):
            return False
        existed = key in self._data
        self._remove(key)
        return existed

    def set_deadline(self, key: str, deadline_ms: int) -> bool:
        if not self.exists(key):
            return False
        self._expires[key] = deadline_ms
        heapq.heappush(self._heap, (deadline_ms, key))
        return True

    def persist(self, key: str) -> bool:
        if not self.exists(key):
            return False
        return self._expires.pop(key, None) is not None

    def incrby(self, key: str, delta: int) -> int:
        """Add to an integer stored as a string. Keeps the key's expiry."""
        self._expire_if_needed(key)
        current = self._data.get(key, "0")
        try:
            value = int(current) + delta
        except ValueError:
            raise ValueError("value is not an integer") from None
        if not INT_MIN <= value <= INT_MAX:
            raise ValueError("increment would overflow")
        self._data[key] = str(value)
        return value

    def flush(self) -> None:
        self._data.clear()
        self._expires.clear()
        self._heap.clear()

    def active_expire(self, limit: int = 1000) -> int:
        """Remove keys whose deadline has passed. Returns how many were removed."""
        if not self.expiry_enabled:
            return 0
        now = self.now_ms()
        removed = 0
        while self._heap and self._heap[0][0] <= now and removed < limit:
            deadline, key = heapq.heappop(self._heap)
            if self._expires.get(key) == deadline:  # skip stale heap entries
                self._remove(key)
                removed += 1
                if self.on_expire:
                    self.on_expire(key)
        return removed
