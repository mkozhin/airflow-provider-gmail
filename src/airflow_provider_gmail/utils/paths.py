"""Pure S3 object-key construction for the Gmail → S3 layout.

This module is deliberately *pure*: no Airflow, no S3, no Gmail imports — only
string joining. It provides the shared key join used across the Gmail → S3
layout

    <prefix>/dt=<dt>/<message_id>/<filename>
    <prefix>/dt=<dt>/<message_id>/_manifest.json

:func:`join_key` is the one primitive: the S3 operator composes an attachment
key straight through it as ``join_key(prefix, rel_path)`` (no
decompose/recompose of the ``dt=`` segment). :func:`manifest_key` (and its
building block :func:`message_dir`) is the *structured* helper the
storage-aware S3 sensor (Task 12) uses to locate a message's ``_manifest.json``
from ``dt`` and ``message_id`` it holds separately — so operator and sensor can
never disagree on where the manifest lives, next to the message's files.

Normalization rules (so ``prefix`` may be given with or without a trailing
slash, or empty): leading/trailing ``/`` are stripped from each segment and
empty segments collapsed, so an empty ``prefix`` never yields a key that starts
with ``/`` and double slashes never appear.
"""

from __future__ import annotations

MANIFEST_FILENAME = "_manifest.json"

#: S3 caps a *whole* object key at 1024 UTF-8 bytes (not per component).
S3_MAX_KEY_BYTES = 1024


def join_key(*segments: str) -> str:
    """Join path segments into an S3 key, dropping empties and stray slashes.

    Each segment is split on ``/`` so an inner double slash (``gmail//avito``) or
    a trailing slash (``gmail/avito/``) collapses; empty parts vanish. The result
    never has a leading, trailing, or doubled slash. The S3 operator joins its
    already-composed ``<prefix>`` + ``rel_path`` straight through this, so there
    is no decompose-then-recompose of the ``dt=`` segment.
    """
    parts: list[str] = []
    for segment in segments:
        for part in segment.split("/"):
            if part:
                parts.append(part)
    return "/".join(parts)


def s3_key(*segments: str) -> str:
    """Join segments into an S3 object key, guarding the 1024-byte S3 limit.

    Unlike a local filesystem (which caps each path *component* at 255 bytes),
    S3 caps the *whole* key at 1024 UTF-8 bytes. A filename that fits the
    255-byte component cap (``sanitize_filename``) can still push the total key
    over the limit under a long ``prefix`` — and because the attachment write and
    the manifest read/write all key through here, S3 would fail cryptically on
    *every* retry, before the manifest is written: a permanent stall.

    Fail fast instead, naming the offending key so the fix (shorten the prefix)
    is obvious. The check sees the final key, so the ``_N`` collision suffix
    already baked into the filename is counted — a 1023-byte key that a suffix
    tips to 1025 is caught here rather than by S3.
    """
    key = join_key(*segments)
    size = len(key.encode("utf-8"))
    if size > S3_MAX_KEY_BYTES:
        raise ValueError(
            f"S3 object key is {size} bytes, over S3's {S3_MAX_KEY_BYTES}-byte "
            f"limit: {key!r}. Shorten the operator's prefix."
        )
    return key


def message_dir(prefix: str, dt: str, message_id: str) -> str:
    """The key prefix of one message's directory: ``<prefix>/dt=<dt>/<message_id>``."""
    return join_key(prefix, f"dt={dt}", message_id)


def manifest_key(prefix: str, dt: str, message_id: str) -> str:
    """The object key of the message's ``_manifest.json`` (next to its files).

    Kept as a *structured* ``(prefix, dt, message_id)`` helper for the
    storage-aware S3 sensor, which has ``dt`` and ``message_id`` separately and
    never composes a ``rel_dir``.

    Routes through :func:`s3_key` (not the raw :func:`join_key`) so the
    1024-byte guard fires here too: the sensor's manifest lookup then fails fast
    with the same clear :class:`ValueError` the S3 operator raises, instead of
    an over-limit key reaching S3 and producing a cryptic backend error. The
    produced key is unchanged for normal prefixes.
    """
    return s3_key(message_dir(prefix, dt, message_id), MANIFEST_FILENAME)
