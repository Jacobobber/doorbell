"""Each test names a failure mode of naive agent-messaging designs and
proves this design kills it. This file is the point of the project."""

import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from doorbell import Doorbell, MessageBus


@pytest.fixture()
def bus():
    b = MessageBus()
    yield b
    b.close()


def test_killed_waiter_loses_nothing(bus):
    """Failure mode: a waiter dies (session reap, crash, restart) and the
    messages that arrive afterwards are lost or eaten. Here: kill the
    doorbell mid-wait, post, and catch up -- nothing is gone."""
    bus.subscribe("worker", "jobs")
    bell = Doorbell(bus, "worker")
    t = threading.Thread(target=bell.wait, daemon=True)
    t.start()
    bell.close()  # simulate the harness reaping the waiter
    t.join(timeout=5)
    assert not t.is_alive()

    bus.post("jobs", "job-1", sender="dispatcher")
    got = bus.catch_up("worker")
    assert [m.body for m in got] == ["job-1"]


def test_orphaned_consumer_class_eliminated(bus):
    """Failure mode: a resident poller outlives its consumer and keeps
    consuming messages delivered to nobody -- delivery looks green while
    posts vanish. Peek-only waiting makes this unrepresentable: waking is
    not consuming, so a wake that reaches nobody costs nothing."""
    bus.subscribe("worker", "jobs")
    bell = Doorbell(bus, "worker")
    bus.post("jobs", "job-1", sender="dispatcher")

    event = bell.wait(timeout=5)
    assert event is not None
    # The orphan scenario: the session that armed this bell is gone; the
    # wake event is dropped on the floor, never handled, never acked.
    del event

    # A fresh session (new doorbell, same durable handle) still sees it.
    replacement = Doorbell(bus, "worker")
    again = replacement.wait(timeout=5)
    assert again is not None and again.seq == 1
    assert [m.body for m in bus.catch_up("worker")] == ["job-1"]


def test_ack_only_after_handling_redelivers_on_crash(bus):
    """Failure mode: consumer fetches, crashes before processing, and the
    message is gone because fetching consumed it. Here fetch and ack are
    separate; a crash between them means redelivery, not loss."""
    bus.subscribe("worker", "jobs")
    bus.post("jobs", "job-1", sender="dispatcher")

    first_session = bus.catch_up("worker")
    assert len(first_session) == 1
    # -- crash: no ack --

    second_session = bus.catch_up("worker")
    assert [m.body for m in second_session] == ["job-1"]

    bus.ack("worker", "jobs", second_session[-1].seq)
    assert bus.catch_up("worker") == []


def test_ack_is_forward_only(bus):
    """Failure mode: a stale or replayed ack regresses the cursor and
    triggers a redelivery storm. Acks only move forward."""
    bus.subscribe("worker", "jobs")
    for i in range(3):
        bus.post("jobs", f"job-{i}", sender="dispatcher")
    bus.ack("worker", "jobs", 3)
    bus.ack("worker", "jobs", 1)  # stale ack: no-op, not a regression
    assert bus.store.acked_seq("worker", "jobs") == 3
    assert bus.catch_up("worker") == []


def test_cannot_ack_beyond_head(bus):
    """Failure mode: acking past head marks unseen future messages as
    handled, silently dropping them when they arrive."""
    bus.subscribe("worker", "jobs")
    bus.post("jobs", "job-1", sender="dispatcher")
    with pytest.raises(ValueError):
        bus.ack("worker", "jobs", 99)


def test_join_at_head_no_seed_flood(bus):
    """Failure mode: a new consumer joins with cursor 0 and gets blasted
    with the channel's entire history (and floods whatever it notifies)."""
    for i in range(500):
        bus.post("busy", f"old-{i}", sender="dispatcher")
    bus.subscribe("newcomer", "busy")  # default: at="head"
    assert bus.catch_up("newcomer") == []
    bus.post("busy", "fresh", sender="dispatcher")
    assert [m.body for m in bus.catch_up("newcomer")] == ["fresh"]


def test_join_at_start_gets_history_deliberately(bus):
    """History replay stays available, but only as an explicit choice."""
    bus.post("audit", "first", sender="dispatcher")
    bus.subscribe("auditor", "audit", at="start")
    assert [m.body for m in bus.catch_up("auditor")] == ["first"]


def test_resubscribe_does_not_move_cursor(bus):
    """Failure mode: a reconnecting session re-subscribes and its cursor
    resets -- to 0 (history flood) or to head (silent loss of the unhandled
    backlog). Resubscribing is a no-op for an existing cursor."""
    bus.post("jobs", "ancient", sender="dispatcher")
    bus.subscribe("worker", "jobs")  # joins at head: "ancient" not deliverable
    bus.post("jobs", "job-2", sender="dispatcher")

    bus.subscribe("worker", "jobs")  # reconnect: must not jump to new head
    assert [m.body for m in bus.catch_up("worker")] == ["job-2"]

    bus.subscribe("worker", "jobs", at="start")  # must not reset to 0 either
    assert bus.store.acked_seq("worker", "jobs") == 1
    assert [m.body for m in bus.catch_up("worker")] == ["job-2"]


def test_idempotent_post_dedup(bus):
    """Failure mode: at-least-once producers double-post on retry."""
    a = bus.post("jobs", "job-1", sender="dispatcher", idempotency_key="k1")
    b = bus.post("jobs", "job-1 retry", sender="dispatcher", idempotency_key="k1")
    assert a.seq == b.seq
    assert bus.store.head("jobs") == 1
    assert b.body == "job-1"  # the original wins


def test_distinct_handles_never_collapse(bus):
    """Failure mode: two consumers share one identity and reap each
    other's deliveries. Cursors are per-handle; each sees everything."""
    bus.subscribe("worker-a", "jobs")
    bus.subscribe("worker-b", "jobs")
    bus.post("jobs", "job-1", sender="dispatcher")
    assert [m.body for m in bus.catch_up("worker-a")] == ["job-1"]
    assert [m.body for m in bus.catch_up("worker-b")] == ["job-1"]
    bus.ack("worker-a", "jobs", 1)
    assert bus.catch_up("worker-a") == []
    assert [m.body for m in bus.catch_up("worker-b")] == ["job-1"]


def test_shared_handle_degrades_to_duplicate_not_loss(bus):
    """Failure mode: two live sessions end up sharing one handle (a restart
    re-registered it). Naive consume-on-fetch splits the stream between
    them: each sees half, both look healthy. Here fetch does not consume,
    so both twins see the full backlog, and idempotent forward-only acks
    make the overlap harmless: duplicate handling at worst, never loss."""
    bus.subscribe("worker", "jobs")
    bus.post("jobs", "job-1", sender="dispatcher")
    bus.post("jobs", "job-2", sender="dispatcher")

    session_a = bus.catch_up("worker")
    session_b = bus.catch_up("worker")  # concurrent twin, same handle
    assert [m.body for m in session_a] == ["job-1", "job-2"]
    assert session_b == session_a  # both saw everything: no split-brain

    bus.ack("worker", "jobs", session_a[-1].seq)
    bus.ack("worker", "jobs", session_b[-1].seq)  # twin's ack: no-op
    assert bus.store.acked_seq("worker", "jobs") == 2
    assert bus.catch_up("worker") == []


def test_backlog_rings_immediately(bus):
    """Failure mode: a session arms its waiter and sits silent while
    already-delivered, unhandled messages rot behind it. Arming a
    doorbell over unacked backlog rings at once."""
    bus.subscribe("worker", "jobs")
    bus.post("jobs", "job-1", sender="dispatcher")
    bell = Doorbell(bus, "worker")
    start = time.monotonic()
    event = bell.wait(timeout=5)
    assert event is not None and event.reason == "backlog"
    assert time.monotonic() - start < 1.0


def test_catch_up_pages_to_true_head(bus):
    """Failure mode: treating a bounded batch's end as the channel head
    and stranding everything behind it. catch_up pages until empty."""
    bus.subscribe("worker", "jobs")
    for i in range(257):
        bus.post("jobs", f"job-{i}", sender="dispatcher")
    got = bus.catch_up("worker", page_size=25)
    assert len(got) == 257
    assert [m.seq for m in got] == list(range(1, 258))


def test_own_posts_do_not_ring(bus):
    """Failure mode: a consumer wakes on its own posts and ping-pongs."""
    bus.subscribe("worker", "jobs")
    bell = Doorbell(bus, "worker")
    bus.post("jobs", "note to self", sender="worker")
    assert bell.wait(timeout=0.5) is None


def test_wake_delivers_no_content(bus):
    """The doorbell is a wake signal, not a delivery path: acting on the
    (unsanitized) body of a wake would bypass the handle-then-ack
    discipline. WakeEvent carries no body at all, and every string field
    it does carry is sanitized -- including a hostile sender name."""
    bus.subscribe("worker", "jobs")
    bell = Doorbell(bus, "worker")
    bus.post("jobs", "payload \x1b[31mred", sender="dis\x1b]0;x\x07patcher")
    event = bell.wait(timeout=5)
    assert event is not None
    assert not hasattr(event, "body")
    for field in (event.channel, event.sender, event.wake_line):
        assert "\x1b" not in field and "\n" not in field
    assert event.sender == "dispatcher"
    assert "payload" in event.wake_line  # visible, but sanitized and framed
    assert "[untrusted]" in event.wake_line


def test_hard_killed_waiter_process_loses_nothing(tmp_path):
    """The strongest claim in the prose, tested literally: a waiter
    process hard-killed mid-wait (TerminateProcess / SIGKILL semantics,
    no cleanup runs) loses nothing, because the waiter holds no durable
    state -- the store, in another process, is the only truth."""
    db = str(tmp_path / "bus.sqlite3")
    setup = MessageBus(db)
    setup.subscribe("worker", "jobs")
    setup.close()

    child_code = (
        "import sys\n"
        "from doorbell import Doorbell, MessageBus\n"
        "bus = MessageBus(sys.argv[1])\n"
        "bell = Doorbell(bus, 'worker')\n"
        "print('armed', flush=True)\n"
        "bell.wait(timeout=30)\n"
    )
    with subprocess.Popen(
        [sys.executable, "-c", child_code, db],
        stdout=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            assert proc.stdout.readline().strip() == "armed"
            proc.kill()
            proc.wait(timeout=10)
        finally:
            if proc.poll() is None:  # pragma: no cover
                proc.kill()

    # Windows releases a hard-killed process's WAL file locks
    # asynchronously, so the survivor's open can transiently fail with
    # "disk I/O error". The brief retry is part of the scenario -- the
    # lock release is OS cleanup in progress, not data loss.
    deadline = time.monotonic() + 10
    while True:
        try:
            survivor = MessageBus(db)
            break
        except sqlite3.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    survivor.post("jobs", "job-1", sender="dispatcher")
    assert [m.body for m in survivor.catch_up("worker")] == ["job-1"]
    survivor.close()
