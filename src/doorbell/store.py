"""SQLite persistence for the doorbell delivery contract.

Everything that must survive a consumer crash lives here: channels, an
append-only message log with per-channel sequence numbers, subscriptions,
and forward-only ack cursors. Consumers keep no durable state of their
own; that asymmetry is the design.

Concurrency model: thread-safe within a process (a lock serializes the
shared connection), and write-safe across processes (every
read-modify-write runs inside a BEGIN IMMEDIATE transaction, so the
guarded reads happen under SQLite's write lock -- concurrent writers on
the same file serialize instead of racing). The forward-only cursor clamp
is additionally done in SQL, so a stale ack can never regress the cursor
no matter which process it arrives from.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

__all__ = ["Message", "Store"]


@dataclass(frozen=True)
class Message:
    channel: str
    seq: int
    sender: str
    body: str
    created_at: float


_SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS messages (
    channel_id      INTEGER NOT NULL REFERENCES channels(id),
    seq             INTEGER NOT NULL,
    sender          TEXT NOT NULL,
    body            TEXT NOT NULL,
    idempotency_key TEXT,
    created_at      REAL NOT NULL,
    PRIMARY KEY (channel_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idempotency
    ON messages(channel_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS subscriptions (
    handle     TEXT NOT NULL,
    channel_id INTEGER NOT NULL REFERENCES channels(id),
    PRIMARY KEY (handle, channel_id)
);

CREATE TABLE IF NOT EXISTS cursors (
    handle     TEXT NOT NULL,
    channel_id INTEGER NOT NULL REFERENCES channels(id),
    acked_seq  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (handle, channel_id)
);
"""


class Store:
    """Pass a file path for durability across processes; the default
    ``:memory:`` suits tests and demos (and dies with the process)."""

    def __init__(self, path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        # isolation_level=None puts the connection in autocommit; every
        # write below opens an explicit BEGIN IMMEDIATE transaction. The
        # 5s timeout is the busy handler that makes cross-process writers
        # queue on the write lock instead of failing.
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None, timeout=5.0
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _write_txn(self) -> Iterator[None]:
        """All read-modify-write goes through here: the reads inside the
        block see the database under SQLite's write lock, which is what
        makes head computation, idempotency checks, and cursor updates
        atomic across processes, not just across threads."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # -- channels ---------------------------------------------------------

    def _channel_id(self, name: str) -> int | None:
        row = self._conn.execute(
            "SELECT id FROM channels WHERE name = ?", (name,)
        ).fetchone()
        return int(row[0]) if row else None

    def _ensure_channel(self, name: str) -> int:
        channel_id = self._channel_id(name)
        if channel_id is not None:
            return channel_id
        cur = self._conn.execute("INSERT INTO channels(name) VALUES (?)", (name,))
        assert cur.lastrowid is not None  # INSERT always assigns a rowid
        return cur.lastrowid

    def channels(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM channels ORDER BY name"
            ).fetchall()
            return [r[0] for r in rows]

    def _head(self, channel_id: int) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return int(row[0])

    def head(self, channel: str) -> int:
        """Highest sequence number in the channel (0 if empty or absent)."""
        with self._lock:
            channel_id = self._channel_id(channel)
            return 0 if channel_id is None else self._head(channel_id)

    # -- messages ---------------------------------------------------------

    def post(
        self,
        channel: str,
        sender: str,
        body: str,
        *,
        idempotency_key: str | None = None,
    ) -> Message:
        """Append a message. With an idempotency key, a repeated post --
        from any thread or process -- returns the original message instead
        of appending a duplicate."""
        with self._write_txn():
            channel_id = self._ensure_channel(channel)
            if idempotency_key is not None:
                row = self._conn.execute(
                    "SELECT seq, sender, body, created_at FROM messages"
                    " WHERE channel_id = ? AND idempotency_key = ?",
                    (channel_id, idempotency_key),
                ).fetchone()
                if row:
                    return Message(channel, row[0], row[1], row[2], row[3])
            msg = Message(
                channel, self._head(channel_id) + 1, sender, body, time.time()
            )
            self._conn.execute(
                "INSERT INTO messages"
                " (channel_id, seq, sender, body, idempotency_key, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (channel_id, msg.seq, sender, body, idempotency_key, msg.created_at),
            )
            return msg

    def messages_after(
        self, channel: str, after_seq: int, *, limit: int = 100
    ) -> list[Message]:
        """A bounded batch. The last element's seq is batch-end, NOT the
        channel head; callers that need everything must page again until
        an empty batch comes back."""
        with self._lock:
            channel_id = self._channel_id(channel)
            if channel_id is None:
                return []
            rows = self._conn.execute(
                "SELECT seq, sender, body, created_at FROM messages"
                " WHERE channel_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (channel_id, after_seq, limit),
            ).fetchall()
            return [Message(channel, *row) for row in rows]

    # -- subscriptions and cursors ----------------------------------------

    def subscribe(self, handle: str, channel: str, *, at: str = "head") -> None:
        """Subscribe a handle. ``at='head'`` starts the cursor at the current
        head so a new consumer is not flooded with history; ``at='start'``
        makes all history deliverable. Resubscribing NEVER moves an existing
        cursor -- a returning session inherits exactly what it left unacked."""
        if at not in ("head", "start"):
            raise ValueError("at must be 'head' or 'start'")
        with self._write_txn():
            channel_id = self._ensure_channel(channel)
            self._conn.execute(
                "INSERT OR IGNORE INTO subscriptions(handle, channel_id) VALUES (?, ?)",
                (handle, channel_id),
            )
            floor = self._head(channel_id) if at == "head" else 0
            self._conn.execute(
                "INSERT OR IGNORE INTO cursors(handle, channel_id, acked_seq)"
                " VALUES (?, ?, ?)",
                (handle, channel_id, floor),
            )

    def subscriptions(self, handle: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT c.name FROM subscriptions s JOIN channels c ON c.id = s.channel_id"
                " WHERE s.handle = ? ORDER BY c.name",
                (handle,),
            ).fetchall()
            return [r[0] for r in rows]

    def acked_seq(self, handle: str, channel: str) -> int:
        with self._lock:
            channel_id = self._channel_id(channel)
            if channel_id is None:
                raise KeyError(f"unknown channel: {channel!r}")
            row = self._conn.execute(
                "SELECT acked_seq FROM cursors WHERE handle = ? AND channel_id = ?",
                (handle, channel_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"{handle!r} is not subscribed to {channel!r}")
            return int(row[0])

    def ack(self, handle: str, channel: str, up_to: int) -> int:
        """Advance the cursor and return its new value. Forward-only: the
        clamp is MAX(acked_seq, ?) in SQL, inside a write transaction, so
        a stale or replayed ack -- from any thread or process -- is a
        no-op, never a regression. Acking beyond head is refused: you
        cannot have handled a message that does not exist."""
        with self._write_txn():
            channel_id = self._channel_id(channel)
            if channel_id is None:
                raise KeyError(f"unknown channel: {channel!r}")
            row = self._conn.execute(
                "SELECT acked_seq FROM cursors WHERE handle = ? AND channel_id = ?",
                (handle, channel_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"{handle!r} is not subscribed to {channel!r}")
            if up_to > self._head(channel_id):
                raise ValueError("cannot ack beyond channel head")
            self._conn.execute(
                "UPDATE cursors SET acked_seq = MAX(acked_seq, ?)"
                " WHERE handle = ? AND channel_id = ?",
                (up_to, handle, channel_id),
            )
            new_row = self._conn.execute(
                "SELECT acked_seq FROM cursors WHERE handle = ? AND channel_id = ?",
                (handle, channel_id),
            ).fetchone()
            return int(new_row[0])
