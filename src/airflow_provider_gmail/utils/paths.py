"""Pure object-key / relative-layout construction for the Gmail → storage layout.

This module is deliberately *pure*: no Airflow, no S3, no Gmail imports — only
string joining. It is the **single owner** of the ``dt=<dt>/<message_id>``
layout: the S3 operator and sensor build object keys through it, and the local
operator builds its relative ``rel_dir`` through :func:`message_dir` with an
empty prefix — so no caller re-invents the ``dt=`` / ``message_id`` scheme. It
provides the shared key join used across the Gmail → S3 layout

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

#: The ``s3://`` URI scheme — the **only** place this literal is used to build URIs.
S3_URI_SCHEME = "s3://"

#: Characters that make an S3 key hostile to third-party ``s3://`` URL parsers:
#: S3's own "characters to avoid" (``{ } ^ [ ] < > ~ | # %`` and the backtick)
#: plus ``?`` and the double quote. This set is specced for **file names**:
#: killed at the source (:func:`airflow_provider_gmail.utils.mime.sanitize_filename`
#: replaces each with ``_``) so produced object keys are URL-safe by
#: construction. ``\`` and ASCII control characters are deliberately **absent**:
#: the basename step of ``sanitize_filename`` strips ``\``/``/`` and its own
#: ``_CONTROL_CHARS`` step handles controls *before* the replacement runs,
#: preserving the ``a\b\c.xlsx → c.xlsx`` contract. ``validate_prefix`` reuses
#: this set but deliberately checks **more** (``\`` + ASCII C0/DEL), because a
#: ``prefix`` never passes through ``sanitize_filename`` — see there.
FORBIDDEN_KEY_CHARS = frozenset('?#%{}^[]<>~|"`')


def validate_prefix(prefix: str) -> None:
    """Reject a **rendered** ``prefix`` carrying URL-hostile characters.

    Attachment names are sanitized at the source
    (:func:`airflow_provider_gmail.utils.mime.sanitize_filename`), but the
    operator/sensor ``prefix`` is caller-supplied and reaches the object key
    verbatim — it never passes through ``sanitize_filename``. So the check here
    is deliberately **wider** than :data:`FORBIDDEN_KEY_CHARS` (which is specced
    for file names, where the basename/control-stripping steps already handle
    ``\\`` and control characters). A character is rejected if it is in
    :data:`FORBIDDEN_KEY_CHARS`, **or** it is a backslash (``\\``), **or** it is
    an ASCII C0 control (``ord < 0x20``) or ``DEL`` (``0x7F``). A ``\\`` or a
    control char (e.g. a newline or tab) would otherwise reach the key verbatim
    and break the "keys are URL-safe by construction" guarantee — a downstream
    ``urlsplit()``/``parse_s3_url`` would silently strip a newline and address a
    *different* key (codex example: ``reports\\narchive``). Unicode C1 controls
    (``0x80–0x9F``) are intentionally **not** rejected — consistent with
    ``sanitize_filename``'s ``_CONTROL_CHARS`` (also capped at ``127``).

    Called on the **rendered** value — the S3 operator validates in
    ``pre_execute()``, the sensor in ``poke()`` — not in ``__init__`` — because
    ``prefix`` is a template field: a Jinja expression
    such as ``{{ ds }}`` itself contains ``{``/``}`` (both forbidden), so an
    ``__init__`` check would reject valid templates. ``/`` is allowed (a prefix is
    a path). Raises :class:`ValueError` naming the offending characters (via
    ``!r`` so control characters are visible).
    """
    bad = sorted(
        ch
        for ch in set(prefix)
        if ch in FORBIDDEN_KEY_CHARS or ch == "\\" or ord(ch) < 0x20 or ord(ch) == 0x7F
    )
    if bad:
        raise ValueError(
            f"prefix contains characters not allowed in an object key: "
            f"{''.join(bad)!r} (in {prefix!r}). Remove them from the prefix."
        )


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


def is_s3_uri(uri: str) -> bool:
    """Return ``True`` iff ``uri`` starts with the ``s3://`` scheme.

    The predicate the resolver branches on: an ``s3://…`` path is read via
    ``S3Hook`` and its attachment keys are re-anchored to the manifest's bucket;
    anything else (an absolute local path, ``http://…``) is read from the local
    filesystem. This is the **only** place, together with :func:`s3_uri` /
    :func:`split_s3_uri`, that knows the ``s3://`` literal.
    """
    return uri.startswith(S3_URI_SCHEME)


def s3_uri(bucket: str, *segments: str) -> str:
    """Build a full ``s3://<bucket>/<key>`` URI from a bucket and key segments.

    The key is composed through :func:`s3_key`, so the same normalization
    (collapsed empty segments and stray slashes) and the same 1024-byte guard
    apply as for every other key in the layout. The builder is the symmetric
    partner of :func:`split_s3_uri`: an empty ``bucket`` or an empty composed
    key raises :class:`ValueError` rather than emitting an ``s3://`` URI that
    :func:`split_s3_uri` would then refuse — so the round-trip has no holes.
    """
    if not bucket:
        raise ValueError("s3_uri requires a non-empty bucket")
    key = s3_key(*segments)
    if not key:
        raise ValueError("s3_uri requires a non-empty key")
    return f"{S3_URI_SCHEME}{bucket}/{key}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://<bucket>/<key>`` URI into ``(bucket, key)`` by raw string.

    Deliberately a **direct** split — the scheme is stripped, the bucket is the
    text up to the first ``/``, and the key is *everything* after it, verbatim
    (no ``urlsplit``, no ``parse_s3_url``): defense in depth for old or foreign
    keys that may carry ``? # %``. A non-``s3://`` input, an empty bucket
    (``s3:///key``) or an empty key (``s3://bucket``, ``s3://bucket/``) raises
    :class:`ValueError` — the symmetric partner of :func:`s3_uri`.
    """
    if not is_s3_uri(uri):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    bucket, sep, key = uri[len(S3_URI_SCHEME) :].partition("/")
    if not bucket:
        raise ValueError(f"s3:// URI has an empty bucket: {uri!r}")
    if not sep or not key:
        raise ValueError(f"s3:// URI has an empty key: {uri!r}")
    return bucket, key
