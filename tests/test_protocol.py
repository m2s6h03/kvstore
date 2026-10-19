import asyncio

import pytest

from kvstore.protocol import Error, ProtocolError, Simple, encode, encode_command, read_command


def parse(data: bytes):
    async def go():
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        out = []
        while True:
            cmd = await read_command(reader)
            if cmd is None:
                return out
            out.append(cmd)

    return asyncio.run(go())


def test_resp_array():
    assert parse(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n") == [["SET", "a", "1"]]


def test_binary_safe_bulk_with_crlf_inside():
    assert parse(b"*2\r\n$3\r\nGET\r\n$4\r\na\r\nb\r\n") == [["GET", "a\r\nb"]]


def test_pipelined_commands():
    data = encode_command(["SET", "a", "1"]) + encode_command(["GET", "a"])
    assert parse(data) == [["SET", "a", "1"], ["GET", "a"]]


def test_inline_commands_and_quotes():
    assert parse(b'SET key "hello world"\r\nGET key\n') == [["SET", "key", "hello world"], ["GET", "key"]]


def test_blank_lines_ignored():
    assert parse(b"\r\n\r\nPING\r\n") == [["PING"]]


def test_empty_array_returns_empty_list():
    assert parse(b"*0\r\n") == [[]]


@pytest.mark.parametrize(
    "data",
    [
        b"*abc\r\n",
        b"*-5\r\n",
        b"*99999\r\n",
        b"*1\r\nGET\r\n",  # missing $ header
        b"*1\r\n$3\r\nGETXX\r\n",  # wrong length
        b"*1\r\n$99999999\r\n",  # bulk too large
        b"*2\r\n$3\r\nGET\r\n",  # stream ends mid-command
        b'SET a "unterminated\r\n',
    ],
)
def test_malformed_requests_raise(data):
    with pytest.raises(ProtocolError):
        parse(data)


def test_partial_inline_line_at_eof_is_dropped():
    assert parse(b"PING") == []


def test_encode_all_types():
    assert encode(Simple("OK")) == b"+OK\r\n"
    assert encode(Error("ERR boom")) == b"-ERR boom\r\n"
    assert encode(42) == b":42\r\n"
    assert encode(-2) == b":-2\r\n"
    assert encode("hi") == b"$2\r\nhi\r\n"
    assert encode("") == b"$0\r\n\r\n"
    assert encode(None) == b"$-1\r\n"
    assert encode(["a", 1, None]) == b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"
    assert encode("č") == b"$2\r\n\xc4\x8d\r\n"  # length is in bytes, not characters


def test_error_replies_cannot_inject_extra_lines():
    assert b"\n" not in encode(Error("bad\r\n+OK"))[:-2]
