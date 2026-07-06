"""Render untrusted message content as a safe single-line notification.

A wake line ends up in logs, terminals, and -- in agent systems -- model
prompts. Message bodies are attacker-controlled input: they can carry ANSI
escapes that corrupt a terminal, control and format characters that hide
or reorder text, or prose crafted to read as instructions to whatever
reads them. ``sanitize`` strips the mechanical attacks; the ``[untrusted]``
framing merely LABELS the semantic one -- it is a marker, not a defense.
The only defense against instruction-shaped content is the consumer
treating wake-line content as data, never as instructions.
"""

from __future__ import annotations

import re
import unicodedata

from .store import Message

__all__ = ["sanitize", "wake_line"]

# Best-effort clean removal of well-formed 7-bit escape sequences (CSI, OSC
# terminated by BEL or ST, and lone two-byte escapes) so their parameter
# bytes do not leak into the output as residue. This regex is cosmetic, not
# the security boundary: the category-C strip below removes every C0/C1
# introducer that any escape sequence -- 8-bit, malformed, or unterminated
# -- would need to function.
_ANSI = re.compile(
    r"(?:\x1b\[[0-?]*[ -/]*[@-~])"
    r"|(?:\x1b\][^\x07\x1b]*(?:\x07|\x1b\\))"
    r"|(?:\x1b[@-Z\\^_])"
)


def sanitize(text: str, *, max_len: int = 160) -> str:
    """Strip ANSI escapes and all Unicode category-C characters (controls,
    format chars such as zero-width and bidi overrides, surrogates), then
    collapse whitespace to single spaces and truncate to ``max_len``.

    Does NOT defend against visual deception built from ordinary printable
    characters: homoglyphs, invisible non-category-C characters (e.g.
    U+3164 Hangul filler, U+2800 braille blank), or reordering inherent to
    RTL scripts. For a single log line those alter appearance, not the
    terminal or the line structure.
    """
    if max_len < 4:
        raise ValueError("max_len must be >= 4 (one character plus '...')")
    text = _ANSI.sub("", text)
    text = "".join(
        " " if unicodedata.category(ch).startswith("C") else ch for ch in text
    )
    text = " ".join(text.split())
    if len(text) > max_len:
        text = text[: max_len - 3].rstrip() + "..."
    return text


def wake_line(msg: Message, *, max_len: int = 160) -> str:
    """One printable line describing a message without trusting any of it.

    The untrusted fields are repr-quoted, so their boundaries are
    unambiguous even when the content contains the line's own delimiters
    (a sender named ``admin: [trusted]`` cannot forge the skeleton -- it
    renders inside quotes). Sender names pass through sanitize too:
    handles are caller-supplied strings, not verified identities.
    """
    return (
        f"{sanitize(msg.channel, max_len=64)!r}#{msg.seq}"
        f" from {sanitize(msg.sender, max_len=64)!r}:"
        f" [untrusted] {sanitize(msg.body, max_len=max_len)!r}"
    )
