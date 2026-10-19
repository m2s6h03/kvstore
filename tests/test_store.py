import pytest

from kvstore.store import Store


def test_set_get_delete(clock):
    s = Store(clock)
    s.set("a", "1")
    assert s.get("a") == "1"
    assert s.delete("a") is True
    assert s.delete("a") is False
    assert s.get("a") is None


def test_key_expires_lazily_and_notifies(clock):
    expired = []
    s = Store(clock, on_expire=expired.append)
    s.set("a", "1", deadline_ms=s.now_ms() + 5000)
    clock.advance(4.9)
    assert s.get("a") == "1"
    clock.advance(0.2)
    assert s.get("a") is None
    assert expired == ["a"]


def test_active_expire_reclaims_unread_keys(clock):
    expired = []
    s = Store(clock, on_expire=expired.append)
    for i in range(5):
        s.set(f"k{i}", "v", deadline_ms=s.now_ms() + 1000)
    s.set("keep", "v")
    clock.advance(2)
    assert s.active_expire() == 5
    assert sorted(expired) == [f"k{i}" for i in range(5)]
    assert s.keys("*") == ["keep"]


def test_overwriting_with_new_deadline_ignores_stale_heap_entry(clock):
    s = Store(clock)
    s.set("a", "1", deadline_ms=s.now_ms() + 1000)
    s.set("a", "2", deadline_ms=s.now_ms() + 100_000)
    clock.advance(5)
    assert s.active_expire() == 0
    assert s.get("a") == "2"


def test_plain_set_clears_ttl(clock):
    s = Store(clock)
    s.set("a", "1", deadline_ms=s.now_ms() + 1000)
    s.set("a", "2")
    clock.advance(10)
    assert s.get("a") == "2"
    assert s.ttl_ms("a") == -1


def test_ttl_values(clock):
    s = Store(clock)
    assert s.ttl_ms("missing") == -2
    s.set("a", "1")
    assert s.ttl_ms("a") == -1
    s.set_deadline("a", s.now_ms() + 3000)
    assert s.ttl_ms("a") == 3000
    assert s.persist("a") is True
    assert s.ttl_ms("a") == -1
    assert s.persist("a") is False


def test_incrby_basic_and_errors(clock):
    s = Store(clock)
    assert s.incrby("n", 5) == 5
    assert s.incrby("n", -7) == -2
    s.set("text", "hello")
    with pytest.raises(ValueError):
        s.incrby("text", 1)
    s.set("big", str(2**63 - 1))
    with pytest.raises(ValueError):
        s.incrby("big", 1)
    assert s.get("big") == str(2**63 - 1)  # unchanged after the failed call


def test_incrby_keeps_ttl(clock):
    s = Store(clock)
    s.set("n", "1", deadline_ms=s.now_ms() + 5000)
    s.incrby("n", 1)
    assert s.ttl_ms("n") == 5000


def test_keys_glob_and_dbsize(clock):
    s = Store(clock)
    for k in ("user:1", "user:2", "post:1"):
        s.set(k, "x")
    assert sorted(s.keys("user:*")) == ["user:1", "user:2"]
    assert s.keys("user:?") == ["user:1", "user:2"]
    assert s.dbsize() == 3
    s.flush()
    assert s.dbsize() == 0


def test_items_skips_expired(clock):
    s = Store(clock)
    s.set("live", "1")
    s.set("dead", "2", deadline_ms=s.now_ms() + 1000)
    clock.advance(5)
    assert [k for k, _, _ in s.items()] == ["live"]
