import asyncio
import threading

import pytest

from kvstore import Client, Server


class FakeClock:
    """Deterministic time source: tests move it by hand."""

    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class ServerThread:
    """Runs a real Server on its own event loop in a background thread."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.loop = asyncio.new_event_loop()
        self.server = None
        self.thread = None
        self._stopped = False

    def start(self):
        ready = threading.Event()

        def run():
            asyncio.set_event_loop(self.loop)
            self.server = Server(port=0, **self.kwargs)
            self.loop.run_until_complete(self.server.start())
            ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        assert ready.wait(5), "server did not start"
        return self

    @property
    def port(self):
        return self.server.port

    def client(self) -> Client:
        return Client(port=self.port)

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        asyncio.run_coroutine_threadsafe(self.server.stop(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def server():
    s = ServerThread().start()
    yield s
    s.stop()


@pytest.fixture
def start_server():
    """Factory so a test can start (and restart) servers with custom options."""
    started = []

    def factory(**kwargs):
        s = ServerThread(**kwargs).start()
        started.append(s)
        return s

    yield factory
    for s in started:
        try:
            s.stop()
        except Exception:
            pass
