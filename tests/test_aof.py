import json
import os

import pytest

from kvstore.aof import AOF
from kvstore.engine import Engine
from kvstore.store import Store


def make(path, clock, fsync="always"):
    eng = Engine(Store(clock), AOF(str(path), fsync))
    eng.load()
    return eng


def run(eng, line):
    return eng.execute(line.split())


def test_state_survives_restart(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET name marko")
    run(a, "SET counter 0")
    run(a, "INCRBY counter 41")
    run(a, "SET temp x")
    run(a, "DEL temp")
    a.close()

    b = make(path, clock)
    assert run(b, "GET name") == "marko"
    assert run(b, "GET counter") == "41"
    assert run(b, "GET temp") is None
    b.close()


def test_ttl_survives_restart_using_absolute_deadline(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET a 1 EX 100")
    a.close()

    clock.advance(30)  # 30 s pass while the server is down
    b = make(path, clock)
    assert run(b, "TTL a") == 70
    clock.advance(71)
    assert run(b, "GET a") is None
    b.close()


def test_key_that_expired_while_down_is_gone_after_restart(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET a 1 EX 10")
    a.close()
    clock.advance(500)
    b = make(path, clock)
    assert run(b, "GET a") is None
    b.close()


def test_incr_on_key_that_later_expires_replays_correctly(tmp_path, clock):
    """The classic replay trap: SET k EX, INCR k, then time passes. A naive
    replay at a later time would see k as already expired when it reaches INCR
    and produce a different dataset. Freezing expiry during replay avoids it."""
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET k 5 EX 10")
    clock.advance(5)
    assert run(a, "INCR k") == 6
    a.close()

    clock.advance(100)
    b = make(path, clock)
    assert run(b, "GET k") is None  # expired, just like the original would have
    b.close()


def test_lazy_expiry_then_reset_replays_to_new_value(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET k 1 EX 1")
    clock.advance(5)
    assert run(a, "GET k") is None  # triggers lazy expiry, which logs a DEL
    run(a, "SET k 7")
    a.close()

    b = make(path, clock)
    assert run(b, "GET k") == "7"
    assert run(b, "TTL k") == -1
    b.close()


def test_active_expiry_is_logged(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET k 1 EX 1")
    clock.advance(5)
    assert a.active_expire() == 1
    a.close()
    with open(path) as f:
        logged = [json.loads(line) for line in f]
    assert ["DEL", "k"] in logged


def test_corrupt_tail_is_truncated(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET a 1")
    run(a, "SET b 2")
    a.close()
    with open(path, "ab") as f:
        f.write(b'["SET","c","3')  # crash mid-write

    b = make(path, clock)
    assert run(b, "GET a") == "1" and run(b, "GET b") == "2"
    assert run(b, "GET c") is None
    run(b, "SET d 4")  # appending after truncation must stay valid
    b.close()

    c = make(path, clock)
    assert run(c, "GET d") == "4"
    c.close()


def test_rewrite_compacts_and_preserves_state(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    for _ in range(200):
        run(a, "INCR counter")
    run(a, "SET gone 1")
    run(a, "DEL gone")
    run(a, "SET keep v EX 1000")
    before = os.path.getsize(path)
    assert not isinstance(run(a, "REWRITEAOF"), Exception)
    after = os.path.getsize(path)
    assert after < before / 10
    run(a, "SET after rewrite")  # logging still works on the new file
    a.close()

    b = make(path, clock)
    assert run(b, "GET counter") == "200"
    assert run(b, "GET gone") is None
    assert run(b, "TTL keep") == 1000
    assert run(b, "GET after") == "rewrite"
    b.close()


def test_flushall_replays(tmp_path, clock):
    path = tmp_path / "db.aof"
    a = make(path, clock)
    run(a, "SET a 1")
    run(a, "FLUSHALL")
    run(a, "SET b 2")
    a.close()
    b = make(path, clock)
    assert run(b, "KEYS *") == ["b"]
    b.close()


def test_invalid_fsync_policy():
    with pytest.raises(ValueError):
        AOF("x.aof", fsync="sometimes")
