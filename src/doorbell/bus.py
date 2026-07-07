"""The doorbell delivery contract over a Store.

The contract, in order:

1. ``catch_up()`` on session start -- a new session inherits everything its
   predecessor left unhandled.
2. Handle, THEN ``ack()``. Never ack a message you have not processed.
3. While on duty, arm exactly one Doorbell (see ``doorbell.waiter``). It
   peeks and never consumes; when it rings: catch up, handle, ack, re-arm.
4. Recovery from any doubt is another catch-up, never process forensics.
"""

from __future__ import annotations

import threading

from .store import Message, Store

__all__ = ["MessageBus"]


class MessageBus:
    def __init__(self, path: str = ":memory:") -> None:
        self.store = Store(path)
        # In-process notification for waiters. Public on purpose: Doorbell
        # holds this condition across its scan-then-wait so a post between
        # the two is never missed. Durability never depends on it -- a
        # lost notification costs latency (one poll interval), and the
        # next catch_up reads the store regardless. It also does not cross
        # processes; remote posts surface via the waiter's bounded poll.
        self.activity = threading.Condition()
        self._closed = threading.Event()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def _require_open(self) -> None:
        if self._closed.is_set():
            raise RuntimeError(
                "MessageBus is closed; create a new MessageBus to post or subscribe"
            )

    def close(self) -> None:
        """Close the bus. Live doorbells wake and return None; arming new
        ones (or posting) after close is a caller error."""
        self._closed.set()
        with self.activity:
            self.activity.notify_all()
        self.store.close()

    def post(
        self,
        channel: str,
        body: str,
        *,
        sender: str,
        idempotency_key: str | None = None,
    ) -> Message:
        self._require_open()
        msg = self.store.post(channel, sender, body, idempotency_key=idempotency_key)
        with self.activity:
            self.activity.notify_all()
        return msg

    def subscribe(self, handle: str, channel: str, *, at: str = "head") -> None:
        self._require_open()
        self.store.subscribe(handle, channel, at=at)

    def catch_up(self, handle: str, *, page_size: int = 100) -> list[Message]:
        """Every unacked message on every channel the handle subscribes to,
        paged to the true head (an empty batch, not batch-end, is the stop
        condition). Does NOT ack: the caller acks after handling.

        Returns the full backlog as one in-memory list, ordered per channel
        (channels sorted by name). That is a deliberate simplicity trade:
        a handle that may accumulate very deep backlogs should page
        ``store.messages_after`` directly and ack incrementally instead.

        A handle with no subscriptions (including a never-subscribed or
        misspelled one) has nothing to deliver and returns ``[]``; it is not
        an error, so a typo reads as "fully caught up".
        """
        out: list[Message] = []
        for channel in self.store.subscriptions(handle):
            position = self.store.acked_seq(handle, channel)
            while True:
                page = self.store.messages_after(channel, position, limit=page_size)
                if not page:
                    break
                out.extend(page)
                position = page[-1].seq
        return out

    def ack(self, handle: str, channel: str, up_to: int) -> int:
        return self.store.ack(handle, channel, up_to)
