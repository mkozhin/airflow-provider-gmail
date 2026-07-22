"""``Manifest`` — the pure domain module for the ``_manifest.json`` contract.

This module is deliberately *pure*: no Airflow, no S3, no Gmail, no paths and no
timezones (see ``CONTEXT.md``). The one S3-read seam it exposes,
:meth:`Manifest.from_s3`, keeps the imports pure: the ``hook`` is injected and
only duck-typed (``.read_key`` is the sole call), so ``S3Hook`` is never
imported here. It owns two things and nothing else:

- the **content and serialization** of the manifest that layer 1 writes next to
  a message's attachments and passes down to layer 2 (schema is single-sourced
  here and mirrored in ``docs/gmail-pipeline-layers-2-3.md``);
- the **delivery tri-state** of ADR-0001 as the free function :func:`decide`.

The manifest is written *per message*, after all of its attachments, and carries
``run_id`` so that (in S3 mode) layer 1 can tell a manifest written by a failed
attempt of the *current* run (deliver, do not re-download) from one written by a
*past* run (do not deliver) — see :class:`Decision` and :func:`decide`.

The serialized key for the sender is ``"from"`` (matching the layer-2 schema);
the dataclass field is ``from_`` because ``from`` is a Python keyword.

Any violation of the minimal schema in :meth:`Manifest.from_json` is raised as a
single :class:`ManifestError`; ``KeyError`` / ``TypeError`` / ``ValueError`` /
``json.JSONDecodeError`` never leak out. Corruption handling is concentrated
here rather than duplicated across storage backends, and a corrupt manifest is a
loud failure rather than a silently-stuck message.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass
from typing import Any


class ManifestError(Exception):
    """Raised on *any* minimal-schema violation while parsing a manifest.

    :meth:`Manifest.from_json` translates every underlying failure (broken JSON,
    non-object, missing required key including ``run_id``, ``files`` not a list,
    element without ``path``, wrong types) into this single exception. No
    ``KeyError`` / ``TypeError`` / ``ValueError`` / ``json.JSONDecodeError``
    escapes to the caller.
    """


@dataclass
class FileEntry:
    """A flat entry of :attr:`Manifest.files`: one written attachment.

    - ``name`` — the original attachment name, before sanitization.
    - ``size`` — ``len(data)`` after decoding (actual bytes written).
    - ``path`` — the canonical destination path (S3 key or absolute disk path),
      prepared by the caller.
    """

    name: str
    size: int
    path: str


@dataclass
class Manifest:
    """The ``_manifest.json`` content — the contract with layer 2.

    Field order here is the natural content order and is **not** the argument
    order of :meth:`build` (which mirrors the ``execute()`` pseudocode, with
    ``files`` before ``run_id``). Do not conflate the two.
    """

    source: str
    message_id: str
    internal_date: str
    subject: str
    from_: str
    run_id: str
    files: list[FileEntry]

    @classmethod
    def build(
        cls,
        source: str,
        message_id: str,
        internal_date_iso: str,
        subject: str,
        from_: str,
        files: list[FileEntry],
        run_id: str,
    ) -> "Manifest":
        """Construct a manifest from already-prepared values.

        The argument order is fixed to match the ``execute()`` pseudocode
        (``files`` before ``run_id``) and differs from the dataclass field
        order on purpose. ``internal_date_iso`` and every ``files[].path`` are
        expected ready: the timezone/path policy is the caller's job (ADR-0002),
        not this pure module's.
        """
        return cls(
            source=source,
            message_id=message_id,
            internal_date=internal_date_iso,
            subject=subject,
            from_=from_,
            run_id=run_id,
            files=list(files),
        )

    def to_json(self) -> bytes:
        """Serialize to UTF-8 JSON bytes (Cyrillic kept literal, key ``from``)."""
        payload = {
            "source": self.source,
            "message_id": self.message_id,
            "internal_date": self.internal_date,
            "subject": self.subject,
            "from": self.from_,
            "run_id": self.run_id,
            "files": [
                {"name": f.name, "size": f.size, "path": f.path} for f in self.files
            ],
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Manifest":
        """Parse and validate a manifest, raising :class:`ManifestError` on any
        minimal-schema violation.

        ``raw`` may be ``str`` (e.g. ``S3Hook.read_key()``) or ``bytes`` (local
        read); ``str`` is normalized via ``.encode()`` before ``json.loads``.
        """
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        try:
            obj: Any = json.loads(raw)
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc

        if not isinstance(obj, dict):
            raise ManifestError(
                f"manifest must be a JSON object, got {type(obj).__name__}"
            )

        required = (
            "source",
            "message_id",
            "internal_date",
            "subject",
            "from",
            "run_id",
            "files",
        )
        for key in required:
            if key not in obj:
                raise ManifestError(f"manifest is missing required key {key!r}")

        # Presence is not enough: a corrupt manifest whose scalar fields are the
        # wrong JSON type (a number/null/object where a string is contracted)
        # must fail loudly, not be mistaken for a valid past-run manifest and
        # silently skip the message (CONTEXT.md: loud failure on corruption).
        for key in (
            "source",
            "message_id",
            "internal_date",
            "subject",
            "from",
            "run_id",
        ):
            if not isinstance(obj[key], str):
                raise ManifestError(
                    f"manifest {key!r} must be a string, "
                    f"got {type(obj[key]).__name__}"
                )

        raw_files = obj["files"]
        if not isinstance(raw_files, list):
            raise ManifestError(
                f"manifest 'files' must be a list, got {type(raw_files).__name__}"
            )

        files: list[FileEntry] = []
        for i, item in enumerate(raw_files):
            if not isinstance(item, dict):
                raise ManifestError(
                    f"manifest files[{i}] must be an object, "
                    f"got {type(item).__name__}"
                )
            for key in ("name", "size", "path"):
                if key not in item:
                    raise ManifestError(
                        f"manifest files[{i}] is missing required key {key!r}"
                    )
            for key in ("name", "path"):
                if not isinstance(item[key], str):
                    raise ManifestError(
                        f"manifest files[{i}] {key!r} must be a string, "
                        f"got {type(item[key]).__name__}"
                    )
            size = item["size"]
            # bool is a subclass of int; a JSON boolean is not a valid size.
            if not isinstance(size, int) or isinstance(size, bool):
                raise ManifestError(
                    f"manifest files[{i}] 'size' must be an int, "
                    f"got {type(size).__name__}"
                )
            files.append(
                FileEntry(name=item["name"], size=size, path=item["path"])
            )

        return cls(
            source=obj["source"],
            message_id=obj["message_id"],
            internal_date=obj["internal_date"],
            subject=obj["subject"],
            from_=obj["from"],
            run_id=obj["run_id"],
            files=files,
        )

    @classmethod
    def from_s3(cls, hook: Any, bucket: str, key: str) -> "Manifest":
        """Read + parse an S3 manifest via ``hook.read_key`` (str, UTF-8-decoded).

        The single S3-read seam for the three call sites (resolver, S3 operator,
        S3 sensor), so the corruption contract lives in one place. ``hook`` is
        duck-typed — only ``.read_key(key, bucket_name=...)`` is called — so this
        pure module never imports ``S3Hook``.

        ``S3Hook.read_key`` decodes the body as UTF-8 *before* parsing, so a
        non-UTF-8 body raises ``UnicodeDecodeError`` (which the local bytes path
        would have surfaced as a :class:`ManifestError` via ``from_json``); wrap
        it so corrupt content surfaces the same way everywhere. A **missing**
        object raises *before* the decode (S3 ``ClientError``) and is
        deliberately **not** caught here — it stays the caller's own contract.
        """
        try:
            raw = hook.read_key(key, bucket_name=bucket)
        except UnicodeDecodeError as exc:
            raise ManifestError(
                f"manifest at s3://{bucket}/{key} is not valid UTF-8: {exc}"
            ) from exc
        return cls.from_json(raw)


class Decision(enum.Enum):
    """The delivery tri-state of ADR-0001 (label catch-up is orthogonal).

    - :attr:`DOWNLOAD_AND_DELIVER` — no manifest (or ``overwrite``): download the
      attachments and deliver the manifest path downstream.
    - :attr:`DELIVER_ONLY` — a manifest of the *current* ``run_id`` (a failed
      attempt of this same run): the files are already on storage, deliver the
      path without re-downloading.
    - :attr:`SKIP` — a manifest of a *past* ``run_id``: neither download nor
      deliver (it already went through layer 2 on its own day).
    """

    DOWNLOAD_AND_DELIVER = "download_and_deliver"
    DELIVER_ONLY = "deliver_only"
    SKIP = "skip"


def decide(manifest: Manifest | None, run_id: str, overwrite: bool) -> Decision:
    """Resolve the ADR-0001 delivery tri-state for one message.

    ``overwrite`` forces a re-download regardless of any manifest; ``None`` is a
    valid input (no manifest / local mode) and also yields
    :attr:`Decision.DOWNLOAD_AND_DELIVER`.
    """
    if overwrite:
        return Decision.DOWNLOAD_AND_DELIVER
    if manifest is None:
        return Decision.DOWNLOAD_AND_DELIVER
    if manifest.run_id == run_id:
        return Decision.DELIVER_ONLY
    return Decision.SKIP
