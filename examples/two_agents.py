"""A dispatcher and a crash-prone worker, sharing one durable bus.

Run:  uv run python examples/two_agents.py

The worker's first session is killed after it receives (but before it
acks) a job. The second session inherits the job via catch-up, which is
the entire point: recovery is catch-up, not process forensics.
"""

import threading
import time

from doorbell import Doorbell, MessageBus, sanitize


def main() -> None:
    bus = MessageBus()  # pass a file path for cross-process durability
    bus.subscribe("worker", "jobs")

    def worker_session(name: str, *, crash_before_ack: bool) -> None:
        # 1. Catch up: inherit whatever the previous session left unhandled.
        # Bodies are untrusted content; sanitize() before displaying them.
        for msg in bus.catch_up("worker"):
            print(
                f"[{name}] handling inherited {msg.channel}#{msg.seq}: {sanitize(msg.body)}"
            )
            bus.ack("worker", msg.channel, msg.seq)

        # 2. Arm one doorbell and wait for the ring.
        print(f"[{name}] waiting up to 3s for a ring")
        bell = Doorbell(bus, "worker")
        event = bell.wait(timeout=3)
        if event is None:
            print(f"[{name}] idle timeout, exiting")
            return
        print(f"[{name}] ring: {event.wake_line}")

        # 3. Handle via catch-up, then ack -- unless we crash first.
        for msg in bus.catch_up("worker"):
            if crash_before_ack:
                print(f"[{name}] CRASH before ack of {msg.channel}#{msg.seq}")
                return  # session dies; the cursor never moved
            print(f"[{name}] handled {msg.channel}#{msg.seq}: {sanitize(msg.body)}")
            bus.ack("worker", msg.channel, msg.seq)

    first = threading.Thread(
        target=worker_session, args=("session-1",), kwargs={"crash_before_ack": True}
    )
    first.start()
    time.sleep(0.2)
    bus.post("jobs", "resize the images in /data/batch-7", sender="dispatcher")
    first.join()

    print("--- worker restarts ---")
    second = threading.Thread(
        target=worker_session, args=("session-2",), kwargs={"crash_before_ack": False}
    )
    second.start()
    second.join()
    bus.close()


if __name__ == "__main__":
    main()
