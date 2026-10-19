"""Rough throughput benchmark.

    python -m kvstore --port 6380          # terminal 1
    python examples/bench.py               # terminal 2

Prints operations/second for request-response round trips (one command at a time,
latency-bound) and for pipelined batches (throughput-bound).
"""
import argparse
import threading
import time

from kvstore import Client


def timed(label: str, ops: int, fn) -> None:
    start = time.perf_counter()
    fn()
    secs = time.perf_counter() - start
    print(f"{label:<34}{ops / secs:>12,.0f} ops/s   ({secs:.2f}s for {ops:,} ops)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("-n", type=int, default=20_000, help="operations per test")
    p.add_argument("--clients", type=int, default=8)
    a = p.parse_args()

    with Client(a.host, a.port) as c:
        c.execute("FLUSHALL")

        def sequential():
            for i in range(a.n):
                c.set(f"key:{i}", "value")
                c.get(f"key:{i}")

        timed("1 client, one at a time (SET+GET)", 2 * a.n, sequential)

        batch = 1000

        def pipelined():
            for start in range(0, a.n, batch):
                c.pipeline([("SET", f"p:{i}", "value") for i in range(start, start + batch)])

        timed(f"1 client, pipelined x{batch} (SET)", a.n, pipelined)

    def worker():
        with Client(a.host, a.port) as wc:
            for i in range(a.n // a.clients):
                wc.execute("INCR", "shared_counter")

    def concurrent():
        threads = [threading.Thread(target=worker) for _ in range(a.clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    timed(f"{a.clients} clients at once (INCR)", a.n // a.clients * a.clients, concurrent)


if __name__ == "__main__":
    main()
