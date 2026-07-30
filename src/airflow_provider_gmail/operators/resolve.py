"""``GmailResolveAttachmentsOperator`` — a thin operator over :func:`resolve_attachments`.

The declarative-DAG face of the manifest resolver: given the manifest paths a
Gmail download operator emits in XCom, it returns a flat list of *full*
attachment paths (``s3://<bucket>/<key>`` URIs or absolute local paths) a
downstream consumer (e.g. ``airflow-provider-tablefile``) can open without
knowing anything about manifests. All resolution logic lives in
:func:`airflow_provider_gmail.resolve.resolve_attachments`; this operator only
adapts it to the operator interface (template fields + XCom return).

The operator is **not** registered in ``get_provider_info()`` — operators carry
no ``connection-types``/``python-modules`` there and are imported directly by
users.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.models import BaseOperator

from airflow_provider_gmail.resolve import resolve_attachments

__all__ = ["GmailResolveAttachmentsOperator"]


class GmailResolveAttachmentsOperator(BaseOperator):
    """Expand download-operator manifest paths into full attachment paths (XCom).

    A thin wrapper over :func:`resolve_attachments`:

    - ``manifests`` — the manifest paths emitted in XCom by a Gmail download
      operator (``GmailAttachmentsToS3Operator``/``GmailAttachmentsToLocalOperator``);
      typically ``download.output``. A template field, so ``download.output``
      (an XComArg) is rendered before ``execute`` runs.
    - ``pick`` — ``"all"`` (default; every manifest's attachments, sorted
      chronologically by ``internal_date`` ascending — oldest message first,
      ADR-0008) or ``"latest"`` (attachments of the single most-recent manifest
      by ``internal_date``). A template field too.
    - ``aws_conn_id`` — the connection used to read S3 manifests; ignored for a
      purely-local input (the Amazon provider is imported lazily).

    ``execute`` returns the flat path list, which Airflow pushes to XCom. An
    unknown ``pick``, a broken manifest
    (:class:`~airflow_provider_gmail.manifest.ManifestError`), and — under
    ``pick="all"`` or ``pick="latest"`` — a malformed ``internal_date``
    (:class:`ValueError`) all surface up so the task fails loudly rather than
    emitting a silently-empty result.
    """

    template_fields: Sequence[str] = ("manifests", "pick")

    def __init__(
        self,
        *,
        manifests: list[str],
        pick: str = "all",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.manifests = manifests
        self.pick = pick
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Any) -> list[str]:
        result = resolve_attachments(
            self.manifests, pick=self.pick, aws_conn_id=self.aws_conn_id
        )
        self.log.info(
            "Resolved %d manifest(s) (pick=%s) → %d attachment(s).",
            len(self.manifests),
            self.pick,
            len(result),
        )
        for path in result:
            self.log.info("  %r", path)  # %r — path is untrusted (foreign manifest)
        return result
