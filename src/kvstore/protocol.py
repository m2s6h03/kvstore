"""RESP2, the Redis wire protocol, so redis-cli and redis client libraries work.

Requests are either RESP arrays of bulk strings (what clients send) or plain
"inline" lines such as `SET a 1` (handy for telnet/netcat). Replies are encoded
from small Python types: Simple, Error, int, str (bulk), None (nil), list (array).
"""
from __future__ import annotations

import asyncio
import shlex
from typing import Optional

MAX_ARGS = 1024
MAX_BULK = 1 << 20  # 1 MiB per argument
MAX_INLINE = 64 * 1024


class ProtocolError(Exception):
    pass


class Simple(str):
    """A RESP simple string such as +OK."""


class Error(str):
    """A RESP error such as -ERR unknown command."""


def _clean(s: str) -> bytes:
    return s.replace("\r", " ").replace("\n", " ").encode("utf-8")


def encode(reply) -> bytes:
    if reply is None:
        return b"$-1\r\n"
    if isinstance(reply, Error):
        return b"-" + _clean(reply) + b"\r\n"
    if isinstance(reply, Simple):
        return b"+" + _clean(reply) + b"\r\n"
    if isinstance(reply, int):
        return b":%d\r\n" % reply
    if isinstance(reply, str):
        data = reply.encode("utf-8")
        return b"$%d\r\n" % len(data) + data + b"\r\n"
    if isinstance(reply, (list, tuple)):
        return b"*%d\r\n" % len(reply) + b"".join(encode(r) for r in reply)
    raise TypeError(f"cannot encode {type(reply).__name__}")


def encode_command(args) -> bytes:
    """Encode a command the way a client sends it (array of bulk strings)."""
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        data = str(a).encode("utf-8")
        parts.append(b"$%d\r\n" % len(data) + data + b"\r\n")
    return b"".join(parts)


async def _readline(reader: asyncio.StreamReader) -> Optional[bytes]:
    try:
        line = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError):
        raise ProtocolError("request line too long") from None
    if not line or not line.endswith(b"\n"):
        return None  # EOF (a trailing partial line is dropped)
    return line.rstrip(b"\r\n")


def _parse_len(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError("invalid length") from None


async def read_command(reader: asyncio.StreamReader) -> Optional[list[str]]:
    """Read one command. Returns None on EOF, [] for an empty request."""
    while True:
        line = await _readline(reader)
        if line is None:
            return None
        if line.strip():
            break

    if line.startswith(b"*"):
        count = _parse_len(line[1:])
        if count == 0:
            return []
        if count < 0 or count > MAX_ARGS:
            raise ProtocolError("invalid multibulk length")
        args = []
        for _ in range(count):
            header = await _readline(reader)
            if header is None:
                raise ProtocolError("unexpected end of stream")
            if not header.startswith(b"$"):
                raise ProtocolError("expected '$' bulk string header")
            size = _parse_len(header[1:])
            if size < 0 or size > MAX_BULK:
                raise ProtocolError("invalid bulk length")
            try:
                data = await reader.readexactly(size + 2)
            except asyncio.IncompleteReadError:
                raise ProtocolError("unexpected end of stream") from None
            if data[-2:] != b"\r\n":
                raise ProtocolError("bulk string not terminated by CRLF")
            args.append(data[:-2].decode("utf-8", "replace"))
        return args

    try:
        return shlex.split(line.decode("utf-8", "replace"))
    except ValueError:
        raise ProtocolError("unbalanced quotes in request") from None
