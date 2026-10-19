import pytest

from kvstore.engine import Engine
from kvstore.protocol import Error, Simple
from kvstore.store import Store


@pytest.fixture
def eng(clock):
    return Engine(Store(clock))


def run(eng, line):
    return eng.execute(line.split())


def test_ping_echo(eng):
    assert run(eng, "PING") == "PONG" and isinstance(run(eng, "PING"), Simple)
    assert run(eng, "PING hi") == "hi"
    assert run(eng, "ECHO hello") == "hello"


def test_commands_are_case_insensitive(eng):
    assert run(eng, "set a 1") == "OK"
    assert run(eng, "GeT a") == "1"


def test_set_get_del_exists(eng):
    assert run(eng, "SET a 1") == "OK"
    assert run(eng, "GET a") == "1"
    assert run(eng, "GET nope") is None
    assert run(eng, "EXISTS a a nope") == 2  # duplicates count, like Redis
    assert run(eng, "DEL a nope") == 1
    assert run(eng, "GET a") is None


def test_set_nx_xx(eng):
    assert run(eng, "SET a 1 NX") == "OK"
    assert run(eng, "SET a 2 NX") is None
    assert run(eng, "GET a") == "1"
    assert run(eng, "SET b 1 XX") is None
    assert run(eng, "SET a 3 XX") == "OK"
    assert run(eng, "GET a") == "3"


def test_set_ex_px_and_ttl(eng, clock):
    run(eng, "SET a 1 EX 10")
    assert run(eng, "TTL a") == 10
    assert run(eng, "PTTL a") == 10_000
    clock.advance(4)
    assert run(eng, "TTL a") == 6
    clock.advance(7)
    assert run(eng, "GET a") is None
    assert run(eng, "TTL a") == -2
    run(eng, "SET b 1")
    assert run(eng, "TTL b") == -1


@pytest.mark.parametrize(
    "line",
    ["SET a 1 EX", "SET a 1 EX abc", "SET a 1 EX 0", "SET a 1 NX XX", "SET a 1 EX 5 PX 5", "SET a 1 BOGUS"],
)
def test_set_bad_options(eng, line):
    assert isinstance(run(eng, line), Error)


def test_expire_persist(eng, clock):
    run(eng, "SET a 1")
    assert run(eng, "EXPIRE a 5") == 1
    assert run(eng, "EXPIRE missing 5") == 0
    assert run(eng, "PERSIST a") == 1
    assert run(eng, "PERSIST a") == 0
    clock.advance(100)
    assert run(eng, "GET a") == "1"


def test_expire_with_non_positive_deletes_immediately(eng):
    run(eng, "SET a 1")
    assert run(eng, "EXPIRE a 0") == 1
    assert run(eng, "GET a") is None


def test_incr_family(eng):
    assert run(eng, "INCR n") == 1
    assert run(eng, "INCRBY n 10") == 11
    assert run(eng, "DECR n") == 10
    assert run(eng, "DECRBY n 4") == 6
    run(eng, "SET s hello")
    assert isinstance(run(eng, "INCR s"), Error)
    assert isinstance(run(eng, "INCRBY n 1.5"), Error)
    assert run(eng, "GET n") == "6"


def test_arity_and_unknown(eng):
    assert "wrong number of arguments for 'get'" in run(eng, "GET")
    assert "wrong number of arguments for 'get'" in run(eng, "GET a b")
    assert "unknown command 'NOPE'" in run(eng, "NOPE")


@pytest.mark.parametrize("text", ["1_0", " 1", "+1", "1.0", "", "0x10", "٣"])
def test_integer_parsing_is_strict(eng, text):
    assert isinstance(eng.execute(["INCRBY", "n", text]), Error)


def test_keys_dbsize_flushall(eng):
    for k in ("a1", "a2", "b1"):
        run(eng, f"SET {k} x")
    assert sorted(run(eng, "KEYS a*")) == ["a1", "a2"]
    assert run(eng, "DBSIZE") == 3
    assert run(eng, "FLUSHALL") == "OK"
    assert run(eng, "DBSIZE") == 0


def test_rewriteaof_needs_aof(eng):
    assert isinstance(run(eng, "REWRITEAOF"), Error)


def test_values_with_spaces_and_unicode(eng):
    assert eng.execute(["SET", "ključ", "vrednost s presledki 🙂"]) == "OK"
    assert eng.execute(["GET", "ključ"]) == "vrednost s presledki 🙂"


def test_client_handshake_commands(eng):
    reply = run(eng, "HELLO 2")
    assert reply[:2] == ["server", "kvstore"] and reply[reply.index("proto") + 1] == 2
    assert isinstance(run(eng, "HELLO"), list)
    err = run(eng, "HELLO 3")  # RESP3 unsupported: clients must fall back to RESP2
    assert isinstance(err, Error) and err.startswith("NOPROTO")
    assert run(eng, "CLIENT SETINFO LIB-NAME x") == "OK"
    assert run(eng, "CLIENT SETNAME me") == "OK"
    assert isinstance(run(eng, "CLIENT BOGUS"), Error)
    assert run(eng, "SELECT 0") == "OK"
    assert isinstance(run(eng, "SELECT 1"), Error)
