"""Pure S3 object-key construction for the Gmail → S3 layout.

This module is deliberately *pure*: no Airflow, no S3, no Gmail imports — only
string joining. It single-sources the object-key format

    <prefix>/dt=<dt>/<message_id>/<filename>
    <prefix>/dt=<dt>/<message_id>/_manifest.json

shared by :class:`GmailAttachmentsToS3Operator` (Task 9) and the storage-aware
S3 sensor (Task 12) so the two can never disagree on where a message's manifest
lives. Both keys are built from the *same* directory join, so the manifest
always sits next to the message's files.

Normalization rules (so ``prefix`` may be given with or without a trailing
slash, or empty): leading/trailing ``/`` are stripped from each segment and
empty segments collapsed, so an empty ``prefix`` never yields a key that starts
with ``/`` and double slashes never appear.
"""

from __future__ import annotations

MANIFEST_FILENAME = "_manifest.json"


def _join(*segments: str) -> str:
    """Join path segments into an S3 key, dropping empties and stray slashes.

    Each segment is split on ``/`` so an inner double slash (``gmail//avito``) or
    a trailing slash (``gmail/avito/``) collapses; empty parts vanish. The result
    never has a leading, trailing, or doubled slash.
    """
    parts: list[str] = []
    for segment in segments:
        for part in segment.split("/"):
            if part:
                parts.append(part)
    return "/".join(parts)


def message_dir(prefix: str, dt: str, message_id: str) -> str:
    """The key prefix of one message's directory: ``<prefix>/dt=<dt>/<message_id>``."""
    return _join(prefix, f"dt={dt}", message_id)


def s3_object_key(prefix: str, dt: str, message_id: str, filename: str) -> str:
    """The object key of one attachment: ``<prefix>/dt=<dt>/<message_id>/<filename>``."""
    return _join(message_dir(prefix, dt, message_id), filename)


def manifest_key(prefix: str, dt: str, message_id: str) -> str:
    """The object key of the message's ``_manifest.json`` (next to its files)."""
    return _join(message_dir(prefix, dt, message_id), MANIFEST_FILENAME)
