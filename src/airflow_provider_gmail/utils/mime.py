"""MIME parsing helpers for Gmail ``messages.get(format=full)`` payloads.

The Gmail API already hands us ``MessagePart.filename`` as a ready string, so
there is no ``Content-Disposition`` to parse for the *name* and no
``decode_filename`` here (see "Декодирование имени" in the plan). What still
needs decoding are the ``Subject``/``From`` headers, which arrive RFC 2047
encoded (``=?UTF-8?B?...?=``).

Everything in this module is pure: it takes plain ``dict`` payloads (parsed
Gmail JSON) and returns plain values. No Airflow, no network.
"""

from __future__ import annotations

import email.header
import os.path
import re
from collections.abc import Iterator
from dataclasses import dataclass

from airflow_provider_gmail.utils.paths import FORBIDDEN_KEY_CHARS

_ENCODED_WORD = re.compile(r"=\?[^?]+\?[BbQq]\?[^?]*\?=")
_CONTROL_CHARS = {chr(c) for c in range(0, 32)} | {chr(127)}

# Most local filesystems cap a single path component at 255 bytes (ext4, xfs,
# apfs) and S3 caps a whole key at 1024 — 255 is the binding constraint. A few
# bytes are reserved so the ``_N`` collision suffix appended by
# ``resolve_collisions`` still fits under the limit.
_MAX_FILENAME_BYTES = 255
_FILENAME_SUFFIX_RESERVE = 8


def decode_header_value(raw: str) -> str:
    """Decode an RFC 2047 header value (``Subject``/``From``) to a plain string.

    Handles ASCII, base64 (``?B?``) and quoted-printable (``?Q?``) encoded
    words as well as several concatenated encoded words. An unknown or invalid
    charset never raises: the bytes are decoded as UTF-8 with replacement.
    """
    decoded_parts: list[str] = []
    for text, charset in email.header.decode_header(raw):
        if isinstance(text, bytes):
            try:
                decoded_parts.append(text.decode(charset or "utf-8", errors="replace"))
            except (LookupError, ValueError):
                # Unknown charset (LookupError) — fall back to UTF-8.
                decoded_parts.append(text.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(text)
    return "".join(decoded_parts)


def _cut_to_bytes(text: str, max_bytes: int) -> str:
    """Truncate ``text`` to at most ``max_bytes`` UTF-8 bytes on a char boundary.

    Slicing the encoded bytes can leave a partial trailing multibyte character;
    decoding with ``errors="ignore"`` drops it, so the result is always valid
    UTF-8 and never splits a code point.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _cap_filename_length(name: str) -> str:
    """Cap ``name`` at a filesystem/S3-safe UTF-8 byte length, keeping the extension.

    A long or multibyte attachment name would overflow the 255-byte filesystem
    component limit (ENAMETOOLONG in the local operator's write) or the S3 key
    limit, failing the whole message on every retry — a permanent stall. The
    stem is truncated on a UTF-8 character boundary so ``report.<ext>`` stays
    valid; if the extension itself is absurdly long, the whole name is capped.

    The name is split with :func:`os.path.splitext` (the same splitter as
    :func:`airflow_provider_gmail.operators.gmail._first_free_name`), so an
    extensionless name keeps its whole self as the stem (never truncates to
    ``""``) and a dotfile such as ``.bashrc`` is treated as an extensionless
    name rather than a bare extension.
    """
    budget = _MAX_FILENAME_BYTES - _FILENAME_SUFFIX_RESERVE
    if len(name.encode("utf-8")) <= budget:
        return name

    stem, suffix = os.path.splitext(name)
    stem_budget = budget - len(suffix.encode("utf-8"))
    if stem_budget <= 0:
        # The extension alone blows the budget — cap the whole name.
        return _cut_to_bytes(name, budget)
    return _cut_to_bytes(stem, stem_budget) + suffix


def sanitize_filename(name: str, fallback: str) -> str:
    """Turn an attacker-controlled attachment name into a safe path segment.

    Takes the basename only, drops ``/``, ``\\``, ``..``, null bytes and control
    characters, replaces URL-hostile characters (:data:`FORBIDDEN_KEY_CHARS`)
    with ``_``, then caps the result at a filesystem/S3-safe byte length. If
    nothing usable is left, returns ``fallback``.

    The special-character replacement runs *after* the basename step (``/`` and
    ``\\`` are already gone, so they are absent from the forbidden set — the
    ``a\\b\\c.xlsx → c.xlsx`` contract holds) and *after* the empty/dots-only
    fallback check, so a name made entirely of forbidden characters becomes
    ``_``\\ s (``??? → ___``) rather than falling back. Spaces and Unicode are
    left untouched — they are safe for both ``urlsplit`` and the aws CLI.

    A defensive RFC 2047 fallback runs first: Gmail normally gives the filename
    already decoded, but if a fixture ever shows an encoded word we still decode
    it rather than write ``=?UTF-8?B?...?=`` to disk.
    """
    if name and _ENCODED_WORD.search(name):
        name = decode_header_value(name)

    # Basename: strip everything up to the last path separator (either style).
    candidate = re.split(r"[\\/]", name)[-1] if name else ""
    # Drop null bytes and control characters.
    candidate = "".join(ch for ch in candidate if ch not in _CONTROL_CHARS)
    # Drop parent-directory sequences.
    candidate = candidate.replace("..", "")
    candidate = candidate.strip()

    # Empty, or nothing but dots ("." / "..." after the strip) — not a real name.
    if not candidate or set(candidate) <= {"."}:
        return fallback

    # Replace URL-hostile characters so produced keys are safe for third-party
    # s3:// URL parsers. Runs after the fallback check so an all-special name
    # becomes "___", not the fallback.
    candidate = "".join(
        "_" if ch in FORBIDDEN_KEY_CHARS else ch for ch in candidate
    )
    return _cap_filename_length(candidate)


@dataclass
class Attachment:
    """A selected MIME part carrying a file.

    ``data`` holds the base64url body when Gmail inlined it (``body.data``) and
    is ``None`` when the body must be fetched via ``attachment_id``. An empty
    file yields ``data == ""`` — distinct from ``None``.

    There is deliberately no ``size`` (known only after decoding the bytes) and
    no ``safe_name`` (added by ``resolve_collisions`` in the operator layer).
    """

    filename: str
    mime_type: str
    attachment_id: str | None
    data: str | None


def iter_attachments(payload: dict) -> Iterator[Attachment]:
    """Yield attachments from a message payload, walking the MIME tree recursively.

    A part is an attachment when it has a non-empty ``filename`` and at least one
    body source — a **non-empty** ``attachmentId`` or the ``data`` key in
    ``body``. The ``data`` key is checked by *presence*, not truthiness, so an
    empty file with ``data == ""`` still counts; but an empty-string
    ``attachmentId`` with no ``data`` key is **not** a body source (it points at
    nothing). Inline images are dropped: a part is skipped
    only if it is ``inline`` **and** its ``mimeType`` starts with ``image/``.
    Inline non-images (PDF, xlsx) stay attachments — a Content-ID does not
    demote them.

    A part that qualifies as an attachment is yielded **as-is** and its own
    nested MIME tree (``parts``) is **not** descended into. This matters for a
    forwarded ``message/rfc822`` part with a ``filename`` and a body source: it
    is delivered as a single ``.eml`` attachment rather than having the inner
    message's own attachments leak out under the outer message's id. The
    guarantee has a boundary — an ``rfc822`` part that carries a ``filename``
    but **no** body source (only ``parts``, no ``attachmentId``/``data``) is
    *not* an attachment, so the walk still recurses into it.
    """
    yield from _walk(payload)


def _walk(part: dict) -> Iterator[Attachment]:
    # Test the part as an attachment first. A qualifying attachment (non-empty
    # filename + body source, not an inline image) is yielded and we do NOT
    # recurse into any nested tree it carries — a forwarded message/rfc822 part
    # stays one .eml instead of leaking the inner message's attachments.
    attachment = _as_attachment(part)
    if attachment is not None:
        yield attachment
        return
    # Not an attachment: a multipart/* container (no filename) or an rfc822 part
    # with a filename but no body source. Recurse into its children as before.
    for child in part.get("parts") or []:
        yield from _walk(child)


def _as_attachment(part: dict) -> Attachment | None:
    filename = part.get("filename")
    if not filename:
        return None

    body = part.get("body") or {}
    has_source = bool(body.get("attachmentId")) or ("data" in body)
    if not has_source:
        return None

    mime_type = part.get("mimeType", "")
    headers = part.get("headers") or []
    if _is_inline_image(mime_type, headers):
        return None

    return Attachment(
        filename=filename,
        mime_type=mime_type,
        attachment_id=body.get("attachmentId"),
        data=body.get("data"),
    )


def _is_inline_image(mime_type: str, headers: list[dict]) -> bool:
    disposition = _find_header(headers, "Content-Disposition")
    if disposition is None:
        return False
    # "inline" is the first token before ';', case-insensitive — not a whole
    # string compare: real dispositions look like `inline; filename="logo.png"`.
    first_token = disposition.split(";", 1)[0].strip().lower()
    if first_token != "inline":
        return False
    # Drop only inline *images*. A Content-ID alone is not enough: gateways and
    # Apple Mail stamp Content-ID onto real PDF/xlsx parts, so keying off it
    # would silently drop genuine attachments — the worst kind of failure.
    return mime_type.lower().startswith("image/")


def _find_header(headers: list[dict], name: str) -> str | None:
    """Case-insensitive raw header lookup in a part's ``headers`` list."""
    name_lower = name.lower()
    for entry in headers or []:
        if entry.get("name", "").lower() == name_lower:
            return entry.get("value")
    return None


def header(payload: dict, name: str) -> str | None:
    """Return a decoded ``Subject``/``From`` header, or ``None`` if absent.

    ``payload.headers`` is a list of ``{"name", "value"}`` with arbitrary case
    for the name. The value is run through :func:`decode_header_value`.
    """
    raw = _find_header(payload.get("headers") or [], name)
    if raw is None:
        return None
    return decode_header_value(raw)
