import threading
import time

import pytest

from doorbell import Doorbell, MessageBus


@pytest.fixture()
def bus():
    b = MessageBus()
    yield b
    b.close()


def test_roundtrip(bus):
    bus.subscribe("w", "ch")
    msg = bus.post("ch", "hello", sender="s")
    assert msg.seq == 1
    got = bus.catch_up("w")
    assert len(got) == 1 and got[0].body == "hello"
    assert bus.ack("w", "ch", got[-1].seq) == 1
    assert bus.catch_up("w") == []


def test_seq_is_per_channel(bus):
    bus.post("a", "1", sender="s")
    bus.post("a", "2", sender="s")
    assert bus.post("b", "1", sender="s").seq == 1
    assert bus.store.head("a") == 2


def test_ack_unknown_channel_raises(bus):
    with pytest.raises(KeyError):
        bus.ack("w", "nope", 1)


def test_ack_unsubscribed_handle_raises(bus):
    bus.post("ch", "x", sender="s")
    with pytest.raises(KeyError):
        bus.ack("stranger", "ch", 1)


def test_concurrent_posts_get_unique_contiguous_seqs(bus):
    """10 threads x 30 posts: sequence numbers must come out unique and
    contiguous, or ack cursors would skip or double-deliver."""
    errors = []

    def worker(n):
        try:
            for i in range(30):
                bus.post("ch", f"{n}-{i}", sender=f"t{n}")
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    bus.subscribe("reader", "ch", at="start")
    seqs = [m.seq for m in bus.catch_up("reader")]
    assert seqs == list(range(1, 301))


def test_wait_returns_event_for_live_post(bus):
    bus.subscribe("w", "ch")
    bell = Doorbell(bus, "w")
    result = {}

    def waiter():
        result["event"] = bell.wait(timeout=10)

    t = threading.Thread(target=waiter)
    t.start()
    bus.post("ch", "ping", sender="s")
    t.join(timeout=10)
    assert not t.is_alive()
    event = result["event"]
    assert event is not None and event.reason == "post" and event.seq == 1


def test_wait_times_out_quietly(bus):
    bus.subscribe("w", "ch")
    assert Doorbell(bus, "w").wait(timeout=0.2) is None


def test_bus_close_unblocks_live_waiter():
    """Shutdown order should not matter: closing the bus under a live
    doorbell wakes it and returns None, same as the bell's own close()."""
    bus = MessageBus()
    bus.subscribe("w", "ch")
    bell = Doorbell(bus, "w")
    result = {}

    def waiter():
        result["event"] = bell.wait(timeout=10)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.1)
    bus.close()
    t.join(timeout=10)
    assert not t.is_alive()
    assert result["event"] is None


def test_post_or_subscribe_after_close_raises_actionable_error():
    """Using the bus after close() is a documented caller error, so it
    surfaces a clear RuntimeError, not the SQLite driver's closed-database
    message leaking through."""
    bus = MessageBus()
    bus.close()
    with pytest.raises(RuntimeError, match="closed"):
        bus.post("ch", "x", sender="s")
    with pytest.raises(RuntimeError, match="closed"):
        bus.subscribe("w", "ch")


def test_match_filter_skips_but_does_not_lose(bus):
    bus.subscribe("w", "ch")
    bell = Doorbell(bus, "w", match=lambda m: "urgent" in m.body)
    bus.post("ch", "routine", sender="s")
    assert bell.wait(timeout=0.3) is None
    bus.post("ch", "urgent thing", sender="s")
    event = bell.wait(timeout=5)
    assert event is not None and event.seq == 2
    # the skipped message is still deliverable
    assert [m.body for m in bus.catch_up("w")] == ["routine", "urgent thing"]


def test_durability_across_bus_instances(tmp_path):
    """The store is the durable truth: a brand-new process (new bus over
    the same file) inherits subscriptions, cursors, and backlog."""
    db = str(tmp_path / "bus.sqlite3")
    first = MessageBus(db)
    first.subscribe("w", "ch")
    first.post("ch", "before-crash", sender="s")
    first.close()  # process dies

    second = MessageBus(db)
    got = second.catch_up("w")
    assert [m.body for m in got] == ["before-crash"]
    second.ack("w", "ch", got[-1].seq)
    second.close()

    third = MessageBus(db)
    assert third.catch_up("w") == []
    third.close()
