"""Manifest resolver — expand manifests into a flat list of full attachment paths.

This module is the **official client** of the ``_manifest.json`` layer-2 contract
(``manifest.py``): given the manifest paths a Gmail download operator emits in
XCom, it reads each manifest and returns a flat list of *full* attachment paths a
downstream consumer (e.g. ``airflow-provider-tablefile``) can open without knowing
anything about manifests.

The public I/O edge is :func:`resolve_attachments` (Task 2c); the heart is the
**pure** :func:`_resolve` core below — no I/O, so tests build :class:`Manifest`
fixtures directly and import it without mocks.
"""

from __future__ import annotations

from .dates import from_local_iso
from .manifest import Manifest
from .utils.paths import is_s3_uri, s3_uri, split_s3_uri

#: The accepted ``pick`` modes — validated in both the public facade and the core.
_PICK_MODES = ("all", "latest")


def _attachment_paths(manifest_path: str, manifest: Manifest) -> list[str]:
    """Full paths of one manifest's attachments, anchored to the manifest's location.

    The manifest schema keeps ``files[].path`` as an S3 *key* or an absolute local
    path (ADR-0001, unchanged). The bucket for an S3 attachment therefore comes
    from the manifest's own URI, not from the manifest body:

    - **S3 manifest** (``is_s3_uri`` true): the bucket is parsed off the manifest
      URI with :func:`split_s3_uri`, and each ``files[].path`` key is re-anchored to
      a full ``s3://<bucket>/<key>`` URI via :func:`s3_uri`.
    - **local manifest**: ``files[].path`` is already an absolute local path and is
      returned verbatim — :func:`split_s3_uri` is **not** called (there is no
      bucket to parse).
    """
    if is_s3_uri(manifest_path):
        bucket, _ = split_s3_uri(manifest_path)
        return [s3_uri(bucket, entry.path) for entry in manifest.files]
    return [entry.path for entry in manifest.files]


def _resolve(pairs: list[tuple[str, Manifest]], pick: str) -> list[str]:
    """Pure core: turn ``(manifest_path, Manifest)`` pairs into a flat path list.

    No I/O — the caller has already read every manifest. Both winner selection and
    full-path assembly live here, branching per pair on ``is_s3_uri`` of the
    *manifest's own path* (see :func:`_attachment_paths`).

    ``pick``:

    - ``"all"`` (default) — every manifest's attachments, in input order.
      ``from_local_iso`` is **not** called; ``internal_date`` is never read or
      validated. Duplicates on input are **not** deduplicated — a duplicate
      manifest yields duplicate paths (repairing or second-guessing a foreign input
      is not the resolver's job; the duplication may be intentional).
    - ``"latest"`` — attachments of the single winning manifest: the maximum
      ``internal_date`` compared as an aware :class:`datetime` (via
      :func:`from_local_iso`, called **only** here), tie-broken by the larger
      ``message_id``. A winner whose ``files`` is empty returns ``[]`` **as is**,
      with no fallback to the next-by-date manifest — a fallback would silently
      deliver a stale report. A duplicate manifest is merely a repeated candidate:
      the winner is one manifest and its attachments are returned once.

    An unknown ``pick`` raises :class:`ValueError`.
    """
    if pick not in _PICK_MODES:
        raise ValueError(
            f"pick must be one of {_PICK_MODES!r}, got {pick!r}"
        )

    if pick == "all":
        result: list[str] = []
        for manifest_path, manifest in pairs:
            result.extend(_attachment_paths(manifest_path, manifest))
        return result

    # pick == "latest"
    if not pairs:
        return []
    winner_path, winner = max(
        pairs,
        key=lambda pair: (from_local_iso(pair[1].internal_date), pair[1].message_id),
    )
    return _attachment_paths(winner_path, winner)


def resolve_attachments(
    manifests: list[str],
    pick: str = "all",
    aws_conn_id: str = "aws_default",
) -> list[str]:
    """Read manifest paths and expand them to a flat list of full attachment paths.

    The public I/O edge over the pure :func:`_resolve` core: each entry of
    ``manifests`` is a full manifest path (an ``s3://<bucket>/<key>`` URI or an
    absolute local path, e.g. a Gmail download operator's XCom), read into a
    :class:`Manifest` and handed as a ``(path, Manifest)`` pair to :func:`_resolve`.

    ``pick`` is validated **first, before any I/O** — an unknown ``pick`` raises
    :class:`ValueError` rather than letting a storage error surface first, so
    ``resolve_attachments([], pick="invalid")`` raises rather than returning ``[]``.
    :func:`_resolve` re-validates it (an internal-seam guard).

    Reading:

    - **S3 URI** (:func:`is_s3_uri` true): the ``(bucket, key)`` is parsed with
      :func:`split_s3_uri` (**not** ``parse_s3_url``) and read via
      ``S3Hook.read_key``. The Amazon provider is imported **lazily** and exactly
      **one** ``S3Hook`` is created per call — on the first S3 URI — and reused for
      the rest, with ``aws_conn_id`` passed to its constructor. A purely-local
      input never touches the Amazon provider (the extra ``s3`` is not required).
    - **local path**: read straight off disk as bytes.

    Both feed :meth:`Manifest.from_json`, so a broken manifest raises
    :class:`~airflow_provider_gmail.manifest.ManifestError` up to the caller (never
    swallowed). A **missing** manifest (the URI/path is well-formed but the object
    is gone — deleted, retention, or ``aws_conn_id`` points at the wrong store)
    surfaces as its natural error with no wrapping and no ``check_for_key``
    pre-check: S3 raises a ``ClientError`` (NoSuchKey / NoSuchBucket / AccessDenied),
    local raises ``FileNotFoundError``. ``ManifestError`` stays strictly about
    manifest *content*.

    An empty ``manifests`` list yields ``[]``. ``None`` is **not** masked — there
    is deliberately no ``if not manifests`` short-circuit (it would swallow a
    ``None`` returned by ``xcom_pull`` on a wrong ``task_id`` into a forever-green
    empty pipeline); iterating ``None`` raises a natural ``TypeError``.
    """
    if pick not in _PICK_MODES:
        raise ValueError(
            f"pick must be one of {_PICK_MODES!r}, got {pick!r}"
        )

    hook = None  # one lazily-created S3Hook, reused across every S3 URI
    pairs: list[tuple[str, Manifest]] = []
    for manifest_path in manifests:
        if is_s3_uri(manifest_path):
            bucket, key = split_s3_uri(manifest_path)
            if hook is None:
                from airflow.providers.amazon.aws.hooks.s3 import S3Hook

                hook = S3Hook(aws_conn_id=aws_conn_id)
            raw = hook.read_key(key, bucket_name=bucket)
        else:
            with open(manifest_path, "rb") as fh:
                raw = fh.read()
        pairs.append((manifest_path, Manifest.from_json(raw)))

    return _resolve(pairs, pick)
