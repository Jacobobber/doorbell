# doorbell

[![CI](https://github.com/Jacobobber/doorbell/actions/workflows/ci.yml/badge.svg)](https://github.com/Jacobobber/doorbell/actions/workflows/ci.yml)

Durable message delivery for crash-prone agent sessions. Zero dependencies (Python stdlib + SQLite), about 600 lines of source, and a failure-mode suite ([tests/test_failure_modes.py](tests/test_failure_modes.py)) where every test names a failure mode and proves the design kills it.

The design essay behind this, including the failure taxonomy: [The orphaned consumer](docs/the-orphaned-consumer.md).

## Try it

```bash
git clone https://github.com/Jacobobber/doorbell && cd doorbell
uv run python examples/two_agents.py    # zero setup: uv builds the env
uv run --group dev pytest -q            # the failure-mode suite
```

Or install into your own environment: `pip install git+https://github.com/Jacobobber/doorbell` (the distribution is named `doorbell-delivery`, it imports as `doorbell`). Python 3.10+, zero runtime dependencies; CI covers 3.10/3.12/3.14 on Linux and Windows.

## The failure class

LLM agent sessions are the least durable component you will ever build messaging for. They hit context limits, get restarted, get reaped by their harness after an hour, crash mid-turn, and go idle without warning. The obvious design - each session runs a resident poller that consumes from a queue - fails in a specific, nasty way:

**The poller and its consumer die separately.**

A polling loop whose session has died keeps consuming messages that are delivered to nobody. Its heartbeat file stays fresh, so monitoring says coverage is green while posts vanish. Or the session dies first and the loop lingers as an orphan; or two sessions share an identity and reap each other's deliveries; or a fresh consumer joins at cursor zero and floods itself with the channel's entire history. Every patch to the consumer side (heartbeats, supervisors, cursor files) adds another resident thing that can itself orphan.

The fix is an asymmetry:

> **Durable state lives server-side. Everything client-side is disposable.**
> Nothing resident means nothing can orphan.

## The contract

1. **The store is the only truth.** Append-only per-channel log with monotonic sequence numbers, plus one forward-only ack cursor per (handle, channel). Handles are stable role names ("worker", "reviewer"), not session IDs - sessions come and go, the handle's cursor persists.
2. **Catch up first.** A session's first act is `catch_up()`: page every unacked message to the true head (an empty batch is the stop condition - batch-end is *not* head). A new session inherits exactly what its predecessor left unhandled.
3. **Ack only after handling.** Fetching is not consuming. A crash between fetch and ack means redelivery, never loss. At-least-once, by design; producers that retry use idempotency keys.
4. **Wait with a doorbell, not a poller.** While on duty, arm one `Doorbell`: finite, peek-only, position held in memory only. It rings on the first relevant post (or instantly, if unacked backlog already exists), then you catch up, handle, ack, and arm a fresh one. Killing it at any moment loses nothing - with a file-backed store, that includes hard-killing the whole waiter process (there's a test that does exactly that).
5. **Wake lines are untrusted input.** Message bodies can carry ANSI escapes, control characters, and prose crafted to read as instructions to whatever displays them (including a model). The wake line strips the mechanical attacks, repr-quotes the untrusted fields so content can't forge the line's own skeleton, and frames what's left as `[untrusted]` data - a label, not a defense: the consumer treating it as data is the defense. The doorbell deliberately delivers no message body at all - content only flows through catch-up, where the handling discipline lives.

## Quickstart

Runs end to end as pasted:

```python
from doorbell import Doorbell, MessageBus

bus = MessageBus("team.sqlite3")        # file path -> durable across processes
bus.subscribe("worker", "events")       # joins at head: no history flood
bus.post("events", "deploy finished", sender="ci")

# on duty: one doorbell. The unhandled post above rings it immediately.
bell = Doorbell(bus, "worker", idle_timeout=5)
event = bell.wait()                     # WakeEvent, or None on idle timeout
print(event.wake_line)                  # 'events'#1 from 'ci': [untrusted] 'deploy finished'

# the ring carries no content: handle via catch-up, ack only after handling
for msg in bus.catch_up("worker"):
    print("handled:", msg.body)
    bus.ack("worker", msg.channel, msg.seq)

bus.close()
```

In a real session the same three moves run in a loop: catch up on start (a new session inherits whatever its predecessor left unhandled), arm one doorbell while on duty, and on every ring or timeout: catch up, handle, ack, re-arm. Producers that retry pass an idempotency key; a repeated post returns the original message instead of appending a duplicate:

```python
bus.post("events", "deploy finished", sender="ci", idempotency_key="deploy-42")
```

## Failure-mode matrix

Every row is a test in `tests/test_failure_modes.py` unless another file is named.

| Failure mode of naive designs | What kills it here | Test |
|---|---|---|
| Waiter dies; later messages lost | Waiters are peek-only and stateless; store retains everything unacked | `test_killed_waiter_loses_nothing` |
| Orphaned poller consumes messages delivered to nobody | Waking is not consuming; an unheard ring costs nothing | `test_orphaned_consumer_class_eliminated` |
| Crash between fetch and process loses the message | Ack is separate from fetch and comes after handling | `test_ack_only_after_handling_redelivers_on_crash` |
| Stale/replayed ack regresses the cursor | Cursors are forward-only | `test_ack_is_forward_only` |
| Ack beyond head marks unseen messages handled | Refused with an error | `test_cannot_ack_beyond_head` |
| New consumer floods itself with channel history | Subscriptions join at head unless history is requested | `test_join_at_head_no_seed_flood` |
| Reconnect resets the cursor (flood or silent loss) | Resubscribing never moves an existing cursor | `test_resubscribe_does_not_move_cursor` |
| Producer retry double-posts | Idempotency-key dedup at the store | `test_idempotent_post_dedup` |
| Two consumers share an identity and reap each other | Fetch does not consume: both twins see everything, and idempotent acks make the overlap duplicate-at-worst | `test_shared_handle_degrades_to_duplicate_not_loss` (+ `test_distinct_handles_never_collapse` for the remedy) |
| Backlog rots behind an armed, silent waiter | Arming over unacked backlog rings immediately | `test_backlog_rings_immediately` |
| Batch-end treated as channel head strands the tail | Catch-up pages until an empty batch | `test_catch_up_pages_to_true_head` |
| Consumer wakes on its own posts and ping-pongs | Own posts never ring | `test_own_posts_do_not_ring` |
| Acting on unsanitized content at wake time | WakeEvent carries no body; every string field is sanitized | `test_wake_delivers_no_content` (+ `tests/test_wakeline.py` for the sanitizer itself) |
| Waiter process hard-killed mid-wait loses messages | Waiters hold no durable state; the store is another process | `test_hard_killed_waiter_process_loses_nothing` |
| Stale ack from another process regresses the cursor | The clamp is `MAX()` in SQL inside an `IMMEDIATE` write transaction | `tests/test_cross_process.py` |
| Two processes post concurrently and collide on seq / dedup | Every read-modify-write runs under SQLite's write lock (`BEGIN IMMEDIATE`) | `tests/test_cross_process.py` |
| Message content forges the wake line's own skeleton | Untrusted fields are repr-quoted; boundaries stay unambiguous | `tests/test_wakeline.py` |

Run them:

```bash
uv run --group dev pytest -q
```

## What this is not

This is a reference implementation of a **delivery discipline**, not a message broker. SQLite keeps it dependency-free and readable; multiple processes on one host can safely share a file-backed store (WAL + `IMMEDIATE` write transactions serialize the writers), and the contract ports directly to Postgres (the cursor table and append-only log translate one-to-one) or to Redis streams when you need cross-host transport.

Every subscribed handle sees every message: this is durable pub/sub with per-role cursors. Competing consumers and work claiming are deliberately out of scope - two workers cannot split one channel's stream with this library. To distribute work, give each worker its own channel or put claim semantics in the handler. It also does not do retention policies or networking; if you need those, put this contract in front of infrastructure that has them. The interesting part is the shape: durable cursors server-side, disposable peek-only waiters client-side, ack strictly after handling, recovery by catch-up.

## Design notes

- **Why peek-only waiting?** Any waiter that consumes couples delivery to the waiter's lifetime, and the waiter is the least durable thing in the system. Peeking makes waiter death free, which makes aggressive timeouts and harness reaping free, which is what ephemeral sessions need.
- **Why forward-only cursors?** Acks arrive late, duplicated, and out of order from crashy clients. Idempotent, monotonic acks make every replay harmless.
- **Why join-at-head?** Defaulting new subscribers to history replay punishes exactly the wrong moment - onboarding - with the biggest flood. History stays available (`at="start"`), but as an explicit choice.
- **Why at-least-once instead of exactly-once?** Exactly-once delivery to a process that can die between any two instructions is a fiction; what's achievable is exactly-once *effect*, which belongs to the handler (and gets producer-side help from idempotency keys).
- **Doesn't the doorbell just... poll?** Yes: it bottoms out in a bounded poll (`poll_interval`, default 0.5s) with a condition-variable fast path in-process. The poller the design bans is the resident, consuming kind. The doorbell never consumes and never outlives its arming, and that is the distinction that matters; poll-vs-push is transport, peek-vs-consume is correctness.
- **Notification is an optimization, never a dependency.** The in-process condition variable trims latency; if every notification were lost, catch-up still delivers everything. It also doesn't cross processes - a doorbell sees remote posts within one poll interval. Correctness lives in the store.
- **Why does `catch_up` return a list?** Simplicity: the recovery loop reads better and acks deterministically. The trade is that the whole unacked backlog materializes in memory - deliberate for a reference implementation, and stated here so it isn't a surprise. A handle that can accumulate very deep backlogs should page `store.messages_after` directly and ack incrementally.
- **Why is the forward-only clamp in SQL?** A per-process lock can't order acks arriving from two processes; `MAX(acked_seq, ?)` inside an `IMMEDIATE` transaction can't lose that race no matter who runs it. Invariants you advertise belong where every writer has to pass through.

## Contributing

Issues and questions are welcome. The scope is deliberately frozen: this is a reference implementation of a delivery contract, not a growing library. Correctness and portability fixes are welcome; feature PRs will likely be declined. Security reports: see [SECURITY.md](SECURITY.md).

## License

MIT

---

Built with AI pair-assistance (the commit trailer says so). The design, the failure taxonomy, and the claims are mine; the test suite backs them.
