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
import re
from collections.abc import Iterator
from dataclasses import dataclass

_ENCODED_WORD = re.compile(r"=\?[^?]+\?[BbQq]\?[^?]*\?=")
_CONTROL_CHARS = {chr(c) for c in range(0, 32)} | {chr(127)}


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


def sanitize_filename(name: str, fallback: str) -> str:
    """Turn an attacker-controlled attachment name into a safe path segment.

    Takes the basename only, drops ``/``, ``\\``, ``..``, null bytes and control
    characters. If nothing usable is left, returns ``fallback``.

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
    return candidate


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
    body source — the ``attachmentId`` key or the ``data`` key in ``body``
    (checked by key *presence*, not truthiness, so an empty file with
    ``data == ""`` still counts). Inline images are dropped: a part is skipped
    only if it is ``inline`` **and** (its ``mimeType`` starts with ``image/``
    **or** it carries a ``Content-ID`` header).
    """
    yield from _walk(payload)


def _walk(part: dict) -> Iterator[Attachment]:
    children = part.get("parts")
    if children:
        # A multipart container is not itself an attachment; recurse into it.
        for child in children:
            yield from _walk(child)
        return
    attachment = _as_attachment(part)
    if attachment is not None:
        yield attachment


def _as_attachment(part: dict) -> Attachment | None:
    filename = part.get("filename")
    if not filename:
        return None

    body = part.get("body") or {}
    has_source = ("attachmentId" in body) or ("data" in body)
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
    if mime_type.lower().startswith("image/"):
        return True
    return _find_header(headers, "Content-ID") is not None


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
