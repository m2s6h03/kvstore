import socket
import threading
import time

import pytest

from kvstore import Client, ResponseError


def test_ping_set_get(server):
    with server.client() as c:
        assert c.execute("PING") == "PONG"
        assert c.set("a", "1") == "OK"
        assert c.get("a") == "1"
        assert c.get("missing") is None
        assert c.execute("DEL", "a") == 1


def test_error_replies_raise(server):
    with server.client() as c:
        with pytest.raises(ResponseError, match="unknown command"):
            c.execute("NOPE")
        with pytest.raises(ResponseError, match="wrong number"):
            c.execute("GET")
        assert c.execute("PING") == "PONG"  # connection still usable after errors


def test_inline_commands_over_raw_socket(server):
    s = socket.create_connection(("127.0.0.1", server.port), timeout=5)
    s.sendall(b"SET greeting hello\r\nGET greeting\r\n")
    time.sleep(0.2)
    assert s.recv(4096) == b"+OK\r\n$5\r\nhello\r\n"
    s.close()


def test_pipelining(server):
    with server.client() as c:
        replies = c.pipeline([("SET", f"k{i}", i) for i in range(500)] + [("GET", "k499")])
        assert replies[:-1] == ["OK"] * 500
        assert replies[-1] == "499"


def test_large_value_roundtrip(server):
    big = "x" * 500_000
    with server.client() as c:
        c.set("big", big)
        assert c.get("big") == big


def test_expiry_over_the_wire(server):
    with server.client() as c:
        c.execute("SET", "a", "1", "PX", 150)
        assert c.get("a") == "1"
        time.sleep(0.4)
        assert c.get("a") is None


def test_active_expiry_removes_unread_keys(server):
    with server.client() as c:
        for i in range(20):
            c.execute("SET", f"k{i}", "v", "PX", 100)
        time.sleep(0.5)
        # check the store directly so a lazy expiry on read can't hide a failure
        assert len(server.server.engine.store._data) == 0


def test_concurrent_incr_is_atomic(server):
    clients, per_client = 20, 200

    def worker():
        with server.client() as c:
            for _ in range(per_client):
                c.execute("INCR", "counter")

    threads = [threading.Thread(target=worker) for _ in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    with server.client() as c:
        assert c.get("counter") == str(clients * per_client)


def test_clients_see_each_others_writes(server):
    with server.client() as a, server.client() as b:
        a.set("shared", "yes")
        assert b.get("shared") == "yes"


def test_protocol_error_closes_connection_with_message(server):
    s = socket.create_connection(("127.0.0.1", server.port), timeout=5)
    s.sendall(b"*1\r\nGET\r\n")  # missing $ header
    data = s.recv(4096)
    assert data.startswith(b"-ERR Protocol error")
    assert s.recv(4096) == b""  # server closed the connection
    s.close()


def test_server_survives_garbage_and_abrupt_disconnects(server):
    for payload in (b"\x00\xff\xfe\r\n", b"*1\r\n$5\r\nab", b'SET a "oops\r\n'):
        s = socket.create_connection(("127.0.0.1", server.port), timeout=5)
        s.sendall(payload)
        s.close()
    with server.client() as c:
        assert c.execute("PING") == "PONG"


def test_quit(server):
    with server.client() as c:
        assert c.execute("QUIT") == "OK"
        with pytest.raises(ConnectionError):
            c.execute("PING")


def test_persistence_across_server_restart(start_server, tmp_path):
    path = str(tmp_path / "data.aof")
    s1 = start_server(aof_path=path, fsync="always")
    with s1.client() as c:
        c.set("name", "marko")
        c.execute("INCRBY", "visits", 3)
        c.execute("SET", "temp", "x", "EX", 1000)
    s1.stop()

    s2 = start_server(aof_path=path)
    with s2.client() as c:
        assert c.get("name") == "marko"
        assert c.get("visits") == "3"
        assert 990 <= c.execute("TTL", "temp") <= 1000
        assert c.execute("REWRITEAOF").startswith("Background")
        assert c.get("name") == "marko"
