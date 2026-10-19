# kvstore

A small in-memory key-value server written from scratch in Python. It speaks the Redis wire protocol (RESP2), so real Redis client libraries can talk to it. Built on `asyncio` with no runtime dependencies.

**What it demonstrates:** TCP networking and a binary-safe wire protocol, an async single-threaded server, key expiry (lazy + active), crash-safe persistence with an append-only log, and a test suite that includes concurrency and crash-recovery cases.

## Quick start

```bash
pip install -e ".[dev]"
pytest -q

kvstore --port 6380 --aof data.aof        # or: python -m kvstore ...
```

Talk to it with the bundled client:

```python
from kvstore import Client

with Client(port=6380) as c:
    c.set("greeting", "hello", ex=60)     # expires in 60 s
    print(c.get("greeting"))              # hello
    print(c.execute("INCR", "visits"))    # 1
```

...or with the official `redis-py` library (it must use RESP2, since RESP3 is not implemented):

```python
import redis
r = redis.Redis(port=6380, protocol=2, decode_responses=True)
r.set("a", "1"); print(r.get("a"))
```

...or with a plain socket, since inline commands work too (`SET a 1\r\n`).

## Supported commands

| Group | Commands |
|---|---|
| Strings | `SET key value [EX s] [PX ms] [PXAT ms] [NX] [XX]`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Expiry | `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` |
| Keyspace | `KEYS pattern`, `DBSIZE`, `FLUSHALL` |
| Server | `PING`, `ECHO`, `REWRITEAOF`, `QUIT`, plus `HELLO` / `CLIENT SETINFO` / `SELECT 0` for client handshakes |

## How it works

```
client ──TCP──▶ server.py ──▶ protocol.py (RESP parse) ──▶ engine.py ──▶ store.py (dict + expiry heap)
                    ▲                                          │
                    └──────────── RESP reply ◀─────────────────┴──▶ aof.py (append-only log on disk)
```

- **Single-threaded event loop.** Like Redis, every command runs to completion before the next one starts, so `INCR` and friends are atomic without any locks. A test hammers one counter from 20 concurrent clients and checks the exact total.
- **RESP2 protocol, binary safe.** Requests are length-prefixed, so values may contain spaces, newlines or any Unicode. Argument counts, bulk sizes and line lengths are capped, and malformed input gets an error reply and a closed connection rather than a crash.
- **Key expiry, two ways.** Lazy: an expired key is removed when touched. Active: a min-heap of deadlines is swept every 100 ms, so keys nobody reads are still reclaimed. Overwriting a key leaves stale heap entries that are ignored when popped.
- **Persistence (AOF).** Each write is appended to a log as one JSON line; on startup the log is replayed. `--fsync always|everysec|no` trades durability against speed.
  - Commands are logged with *absolute* expiry times, so a restart does not give keys a fresh lease on life.
  - Expirations are logged as `DEL`, and expiry is frozen during replay. Otherwise a log like `SET k 5 EX 10` → `INCR k` replayed hours later would see `k` as already expired at the `INCR` and rebuild a different dataset. There is a regression test for exactly this case.
  - A half-written last line (crash mid-write) is detected and truncated.
  - `REWRITEAOF` compacts the log to one `SET` per live key, writing a temp file and atomically replacing the old one.

## Testing

```bash
pytest -q
```

76 tests: unit tests for the store (using a fake clock, so expiry tests never sleep), the command engine, the protocol parser (including malformed input), the AOF (restart, crash-truncation, compaction), and integration tests against a real running server (pipelining, 500 KB values, concurrent clients, garbage input, restart persistence). CI runs them on Linux and Windows.

The server was also checked against the official `redis-py` client, and after `kill -9` the dataset and TTLs were recovered from the log.

## Benchmark

```bash
kvstore --port 6380 &
python examples/bench.py
```

Results on my machine: _run the script and paste your numbers here._

## Limitations (deliberate scope cuts)

- Strings only: no lists, hashes, sets or sorted sets.
- RESP2 only (a `HELLO 3` gets a `NOPROTO` error), no auth, no TLS, one database, no replication, no transactions or pub/sub.
- `KEYS` and `DBSIZE` are O(n) (same caveat as Redis's `KEYS`).
- `KEYS` patterns use Python's `fnmatch`, which differs slightly from Redis globbing in escape handling.
- `REWRITEAOF` runs synchronously and blocks the server while it works; there is no automatic rewrite threshold.
- Values are limited to 1 MiB per argument.

## Ideas for extension

Hash and list types, a background (forked or threaded) AOF rewrite triggered by log size, RESP3, `SCAN` instead of `KEYS`, a `MULTI/EXEC` transaction queue.
