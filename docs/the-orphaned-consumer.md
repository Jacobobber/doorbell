# The orphaned consumer

*Message delivery for consumers whose lifetime is the unreliable part.*

The worst delivery bug I have debugged in a multi-agent system did not look like a bug. Heartbeats were fresh. Monitors were green. Messages were being consumed at a normal rate. They were being consumed by nobody.

## The setup

The system was ordinary. LLM agent sessions coordinated through named channels: an orchestrator posts work, workers pick it up, results come back. Each session ran a resident poller, a small loop that long-polled its channel and handed new messages to the session that owned it. This is the design almost everyone builds first, because it is the design that works everywhere else.

It fails here because of one property of the consumers. Agent sessions die constantly, and they are supposed to. They hit context limits and restart. Their harness reaps them after an hour. They crash mid-turn. They go idle and get collected. A session is not a service; it is closer to a request.

So the poller and the session it serves die separately. That sentence is the entire failure class.

## The taxonomy

Once we knew what to look for, the variants were everywhere.

**The orphan.** The session dies; its poller keeps running. The loop consumes messages and hands them to nothing. Its heartbeat file stays fresh, so monitoring reports healthy coverage while posts vanish. This is the worst variant because every signal you would normally alarm on says things are fine.

**The deaf session.** The poller dies first (a supervisor reaped it, a timeout fired) and the session sits waiting for messages that are arriving in the same process.

**The identity collapse.** Two sessions share one consumer identity, usually after a restart re-registered the same handle. Each consumes half the stream. Both look alive; neither has a full conversation.

**The seed flood.** A new consumer joins with its cursor at zero and receives the channel's entire history at once, then floods whatever it notifies.

**The reset.** A reconnecting session re-subscribes and its cursor moves, either to zero (flood) or to head (everything unhandled, silently skipped).

We patched each variant as it appeared, and every patch made the next one more likely, because every patch added another resident thing: a supervisor loop to restart dead pollers, heartbeat files to detect orphans, cursor files with locking conventions to survive restarts. Each of those can itself orphan, race a sibling, or go stale. At the peak we had dozens of monitor-script variants, and finding the orphaned ones was itself a monitoring problem. The failure class was never any single bug. It was residency.

## The inversion

The fix was to stop hardening the consumer and change what the consumer is asked to hold.

Durable state lives server-side: an append-only message log per channel, and one forward-only ack cursor per (handle, channel), where a handle is a stable role name ("worker", "reviewer"), not a session ID. Everything client-side is disposable. Nothing resident means nothing can orphan.

The contract that falls out has one premise (the store is the only truth) and four rules.

1. **Catch up first.** A session's first act is to page every unacked message to the true head. A new session inherits exactly what its predecessor left unhandled, because the cursor belongs to the role, not to the session that died holding it.

2. **Ack only after handling.** Fetching is not consuming. A crash between fetch and ack means redelivery, never loss. Delivery is at-least-once, which is the honest option: exactly-once delivery to a process that can die between any two instructions is a fiction, and the achievable version (exactly-once effect) belongs to the handler.

3. **Wait with a doorbell, not a poller.** While on duty, the session arms one finite, peek-only waiter. It never advances the cursor; its scan position lives in memory only. It returns on the first relevant message or on an idle timeout, and either way the session does the same thing next: catch up, handle, ack, arm a fresh one. Killing it at any moment costs nothing, which makes aggressive reaping free, which is what ephemeral sessions need. And because waking is not consuming, a wake that reaches nobody is harmless. The orphaned waiter is not fixed; it is unrepresentable. Ack-after-handling remains a promise the consumer keeps, and the library makes keeping it the shortest path: the ack sits at the end of the same catch-up loop the session runs anyway. The deaf-session variant dies with the same move: the waiter lives inside the session and is finite, so there is no separate poller process whose death the session could outlive, and a doorbell armed over existing backlog rings immediately.

4. **Wake lines are untrusted input.** In an agent system, the wake notification may be read by a model. Message bodies can carry terminal escape sequences, control characters, and prose crafted to read as instructions. The waiter therefore delivers no message body at all, and the one line it renders is stripped of control characters, quoted so content cannot forge the line's own structure, and framed as untrusted. The framing is a label, not a defense. The defense is the consumer treating content as data.

## Why not just a message queue

Brokers have thought hard about redelivery, and if you need retention, fan-out, or cross-host transport, use one. Two things do not transfer.

First, broker liveness machinery (consumer-group rebalancing, visibility timeouts, session heartbeats) is tuned for consumers that are usually alive and occasionally die. Agent sessions invert that ratio. A consumer that dies every hour by design keeps the group in permanent rebalance and makes every visibility timeout a delivery-latency problem. Static group membership and cooperative rebalancing soften this, but then you are tuning broker liveness machinery to pretend your consumers are services, and they are not.

Second, and more important: the failure class lives in the consumer's discipline, not in the broker. A session that acks on fetch, or runs its consumer loop in a thread that outlives the session's reason to exist, recreates the orphan on top of any broker you buy. The contract above is a set of promises the consumer makes. You can implement it on Kafka, Postgres, or Redis; the reference implementation uses SQLite because the contract, not the transport, is the point.

## Tests as the spec

The discipline that made this stick: every failure mode in the taxonomy became a named test before it became a fix. `test_killed_waiter_loses_nothing`. `test_orphaned_consumer_class_eliminated`. `test_resubscribe_does_not_move_cursor`. `test_join_at_head_no_seed_flood`. The test names are the taxonomy, the assertions are the contract, and a design change that reintroduces a variant fails with the name of the failure it brought back.

If you take one practice from this essay, take that one. When you find a failure class, write the taxonomy down as tests before you write the fix. Prose forgets; suites do not.

The reference implementation, about 600 lines of stdlib Python plus the full failure-mode suite, is at [github.com/Jacobobber/doorbell](https://github.com/Jacobobber/doorbell).
