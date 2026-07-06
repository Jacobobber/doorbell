"""The doorbell: a disposable, finite, peek-only waiter.

Arm one while on duty. It observes the bus without consuming anything: it
never advances an ack cursor, and its own scan position lives in memory
only, so killing it at any moment -- mid-wait, mid-scan, and with a
file-backed store the whole process, hard-kill included -- loses nothing.
It returns on the first matching post (or immediately, if unacked backlog
already exists) or ``None`` on idle timeout. Either way the caller does
the same thing: catch up, handle, ack, arm a fresh one.

The failure mode this class exists to kill is the resident poller that
outlives its consumer and keeps consuming messages delivered to nobody.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Callable

from .bus import MessageBus
from .store import Message
from .wakeline import sanitize, wake_line

__all__ = ["Doorbell", "WakeEvent"]


@dataclass(frozen=True)
class WakeEvent:
    channel: str  # sanitized for display; route handling through catch_up()
    seq: int
    sender: str  # sanitized for display; handles are unverified strings
    reason: str  # "backlog" (existed before arming) or "post" (arrived after)
    wake_line: str  # control-free single line, safe to print/log; content
    #                 remains untrusted data even when placed in a prompt

    # Deliberately no body: the doorbell peeks, it does not deliver.
    # Content comes from catch_up(), which is where handling -- and only
    # then acking -- happens.


class Doorbell:
    """Peek-only waiter for one handle.

    Arm AFTER subscribing: the channel set is snapshotted at construction.
    ``match`` filters which messages ring the bell (default: any message
    not sent by this handle). A non-matching message will not ring THIS
    doorbell again -- its in-memory floor advances past it -- but it stays
    unacked, so a later doorbell (which re-scans from the durable ack
    cursor and applies its own filter) may ring on it, and catch_up always
    delivers it until acked. Nothing is lost, only not woken for.
    """

    def __init__(
        self,
        bus: MessageBus,
        handle: str,
        *,
        match: Callable[[Message], bool] | None = None,
        idle_timeout: float = 300.0,
        poll_interval: float = 0.5,
    ) -> None:
        self._bus = bus
        self._handle = handle
        self._match = match
        self._idle_timeout = idle_timeout
        self._poll_interval = poll_interval
        self._closed = threading.Event()
        # In-memory only. Scan floors start at the durable ack cursor so
        # pre-existing unhandled backlog rings immediately instead of
        # rotting behind an armed-but-silent waiter.
        self._floors: dict[str, int] = {
            channel: bus.store.acked_seq(handle, channel)
            for channel in bus.store.subscriptions(handle)
        }
        # Head at arm time distinguishes "backlog" rings from "post" rings.
        self._arm_heads: dict[str, int] = {
            channel: bus.store.head(channel) for channel in self._floors
        }

    def close(self) -> None:
        """Kill the waiter. Always safe: no durable state to corrupt."""
        self._closed.set()
        with self._bus.activity:
            self._bus.activity.notify_all()

    def _rings(self, msg: Message) -> bool:
        if msg.sender == self._handle:
            return False
        return True if self._match is None else self._match(msg)

    def _scan(self) -> WakeEvent | None:
        for channel in self._floors:
            while True:
                page = self._bus.store.messages_after(
                    channel, self._floors[channel], limit=50
                )
                if not page:
                    break
                for msg in page:
                    self._floors[channel] = msg.seq
                    if self._rings(msg):
                        reason = (
                            "backlog" if msg.seq <= self._arm_heads[channel] else "post"
                        )
                        return WakeEvent(
                            channel=sanitize(msg.channel, max_len=64),
                            seq=msg.seq,
                            sender=sanitize(msg.sender, max_len=64),
                            reason=reason,
                            wake_line=wake_line(msg),
                        )
        return None

    def wait(self, timeout: float | None = None) -> WakeEvent | None:
        """Block until the first matching message or the idle timeout.

        Returns a WakeEvent, or None on timeout, close(), or bus close.
        All outcomes lead to the same next step: catch_up -> handle -> ack
        -> re-arm. The doorbell is reusable within a session (its floors
        advance past rung messages), but the canonical pattern is one wait
        per arming.
        """
        deadline = time.monotonic() + (
            self._idle_timeout if timeout is None else timeout
        )
        with self._bus.activity:
            while not self._closed.is_set() and not self._bus.closed:
                try:
                    event = self._scan()
                except sqlite3.ProgrammingError:
                    # The bus was closed out from under us mid-scan; that
                    # is a shutdown, not an error -- nothing durable is
                    # affected either way.
                    if self._bus.closed:
                        return None
                    raise
                if event is not None:
                    return event
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                # Bounded wait so close(), bus close, and the deadline are
                # honored even if a notification is missed; correctness
                # never depends on the notification, only latency does.
                self._bus.activity.wait(timeout=min(remaining, self._poll_interval))
            return None
