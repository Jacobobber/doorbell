"""Two Store instances on one file stand in for two processes: each has
its own in-process lock, so only the SQL-level discipline (BEGIN IMMEDIATE
write transactions plus the SQL-side cursor clamp) separates them. These
are the races a per-process lock cannot see."""

import threading

import pytest

from doorbell.store import Store


@pytest.fixture()
def db(tmp_path):
    return str(tmp_path / "bus.sqlite3")


def test_stale_ack_from_another_connection_never_regresses(db):
    """The flagship invariant, cross-process: a zombie session's stale ack
    arriving through a different connection must clamp, not regress. The
    clamp is MAX(acked_seq, ?) in SQL, so it cannot lose this race."""
    s1, s2 = Store(db), Store(db)
    s1.subscribe("w", "ch", at="start")
    for i in range(5):
        s1.post("ch", "s", f"m{i}")
    assert s1.ack("w", "ch", 5) == 5
    assert s2.ack("w", "ch", 3) == 5  # stale ack: clamped, and says so
    assert s1.acked_seq("w", "ch") == 5
    s1.close()
    s2.close()


def test_concurrent_posts_across_connections_stay_contiguous(db):
    """Sequence assignment is read-then-write; without the write lock held
    across the read, two connections compute the same seq and one dies
    with IntegrityError. BEGIN IMMEDIATE serializes them instead."""
    stores = [Store(db) for _ in range(4)]
    errors: list[Exception] = []

    def producer(store: Store, n: int) -> None:
        try:
            for i in range(25):
                store.post("ch", f"t{n}", f"{n}-{i}")
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=producer, args=(s, n)) for n, s in enumerate(stores)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []

    reader = Store(db)
    msgs, pos = [], 0
    while True:
        page = reader.messages_after("ch", pos, limit=40)
        if not page:
            break
        msgs.extend(page)
        pos = page[-1].seq
    assert [m.seq for m in msgs] == list(range(1, 101))
    for s in stores:
        s.close()
    reader.close()


def test_idempotency_holds_across_connections(db):
    """A producer retry after a crash is likely to be a NEW process; the
    dedup contract has to hold across connections, not just within one."""
    s1, s2 = Store(db), Store(db)
    original = s1.post("ch", "s", "job", idempotency_key="k1")
    retry = s2.post("ch", "s", "job retry", idempotency_key="k1")
    assert (retry.seq, retry.body) == (original.seq, "job")
    assert s1.head("ch") == 1
    s1.close()
    s2.close()
