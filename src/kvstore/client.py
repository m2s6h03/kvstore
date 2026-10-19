"""A tiny blocking client, handy for scripts, tests and the benchmark."""
from __future__ import annotations

import socket
from typing import Optional

from .protocol import encode_command


class ResponseError(Exception):
    """The server replied with an error."""


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._f = self._sock.makefile("rb")

    def execute(self, *args):
        self._sock.sendall(encode_command(args))
        reply = self._read()
        if isinstance(reply, ResponseError):
            raise reply
        return reply

    def pipeline(self, commands: list[tuple]) -> list:
        """Send many commands in one write, then read all replies in order."""
        self._sock.sendall(b"".join(encode_command(c) for c in commands))
        return [self._read() for _ in commands]

    def get(self, key: str) -> Optional[str]:
        return self.execute("GET", key)

    def set(self, key: str, value: str, ex: Optional[int] = None):
        if ex is not None:
            return self.execute("SET", key, value, "EX", ex)
        return self.execute("SET", key, value)

    def _read(self):
        line = self._f.readline()
        if not line:
            raise ConnectionError("server closed the connection")
        kind, body = line[:1], line[1:-2]
        if kind == b"+":
            return body.decode()
        if kind == b"-":
            return ResponseError(body.decode())
        if kind == b":":
            return int(body)
        if kind == b"$":
            n = int(body)
            if n == -1:
                return None
            return self._f.read(n + 2)[:-2].decode()
        if kind == b"*":
            return [self._read() for _ in range(int(body))]
        raise ConnectionError(f"bad reply: {line!r}")

    def close(self) -> None:
        self._f.close()
        self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
