"""doorbell: durable message delivery for crash-prone agent sessions.

Durable state lives server-side; everything client-side is disposable.
See README.md for the failure class this design exists to kill, and
tests/ for proof that it does.
"""

from .bus import MessageBus
from .store import Message, Store
from .waiter import Doorbell, WakeEvent
from .wakeline import sanitize, wake_line

__version__ = "0.1.0"

__all__ = [
    "Doorbell",
    "Message",
    "MessageBus",
    "Store",
    "WakeEvent",
    "sanitize",
    "wake_line",
]
