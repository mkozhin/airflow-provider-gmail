"""Gmail hook: authentication and the cached API service.

The Gmail Connection is **read-only**. On every run the hook does a refresh
grant into memory and never stores the short-lived ``access_token`` anywhere:
writing a refreshed token back into the Connection would race between parallel
tasks, need write access to the Airflow metadata DB and cause mysterious 401s.
The price is one extra HTTP request per task (see the plan, "access_token не
хранится").

``userId`` is a **required** parameter of every Gmail API call. The hook reads
``extra.user_id`` (default ``"me"``) into :attr:`user_id` and passes it to every
Gmail call in later tasks.
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from functools import cached_property

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from airflow_provider_gmail.utils.mime import Attachment, header, iter_attachments
from airflow_provider_gmail.window import Window

_TOKEN_URI = "https://oauth2.googleapis.com/token"

_LABEL_BASE = "airflow/processed"


def resolve_label_name(suffix: str | None) -> str:
    """Return the full processed-label name — pure, no Gmail API involved.

    ``airflow/processed`` by default, ``airflow/processed/<suffix>`` when a
    ``label_suffix`` is given. Declared here because :meth:`GmailHook.build_query`
    needs it for the ``-label:`` term; turning the name into a ``labelId`` (and
    creating the label) is a later task.

    Nested Gmail labels are **not** hierarchical for search, so the exact string
    resolved here is both what gets attached and what goes into ``-label:``.
    """
    if suffix:
        return f"{_LABEL_BASE}/{suffix}"
    return _LABEL_BASE


def _escape(value: str) -> str:
    """Escape inner ``\\`` and ``"`` for use inside a double-quoted Gmail term."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _field_value(value: str) -> str:
    """Format a structured-field value: quote when it needs it, escape inside.

    A value is wrapped in double quotes when it contains whitespace or a
    character that must be escaped (``"`` or ``\\``); otherwise it is emitted
    bare. Escaping of inner ``"`` and ``\\`` always happens inside the quotes.
    """
    needs_quote = any(c.isspace() for c in value) or '"' in value or "\\" in value
    escaped = _escape(value)
    return f'"{escaped}"' if needs_quote else escaped

def _b64url_decode(data: str) -> bytes:
    """Decode a base64url attachment body, fixing up missing ``=`` padding.

    Gmail hands attachment bodies as base64url (``-``/``_`` alphabet) and often
    omits the trailing padding. ``urlsafe_b64decode`` needs the length to be a
    multiple of four, so the padding is restored first. An empty string decodes
    to empty bytes (an empty file legitimately arrives as ``data == ""``).
    """
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


@dataclass
class MessageWithAttachments:
    """A matched message plus everything the operator needs for the manifest.

    Carries the opaque ``message_id``, the ``internal_date`` (epoch milliseconds,
    as Gmail returns it — the operator converts it to the operator timezone), the
    already-decoded ``subject`` and ``from_`` headers, and only the
    ``attachments`` that matched ``attachment_pattern``. Holding these here keeps
    the operator out of the raw ``payload`` so the MIME parsing lives in one
    place.
    """

    message_id: str
    internal_date: int
    subject: str | None
    from_: str | None
    attachments: list[Attachment]


_REFRESH_HINT = (
    "Gmail refresh_token was rejected (revoked or expired). Re-issue it by "
    "walking through the OAuth consent again, and make sure the OAuth "
    "application is published ('In production'): in 'Testing' status Google "
    "issues a refresh_token that lives only 7 days and the pipeline dies "
    "silently after a week."
)


class GmailAuthError(AirflowException):
    """Authentication against Gmail failed (bad/missing/expired credentials)."""


_MODIFY_HINT = (
    "Gmail refused a label modification with 403 insufficientPermissions. "
    "Marking messages needs the 'https://www.googleapis.com/auth/gmail.modify' "
    "scope, but the refresh_token was issued with a narrower scope (e.g. "
    "gmail.readonly). Scopes cannot be widened on an existing refresh_token: "
    "re-issue it by walking through the OAuth consent again with gmail.modify "
    "granted."
)

_MODIFY_BATCH_SIZE = 1000


class GmailPermissionError(AirflowException):
    """A Gmail label modification was refused for lack of the gmail.modify scope."""


def _is_insufficient_permissions(exc: HttpError) -> bool:
    """True when ``exc`` is a 403 whose reason is ``insufficientPermissions``."""
    status = getattr(getattr(exc, "resp", None), "status", None)
    if status != 403:
        return False
    content = getattr(exc, "content", b"") or b""
    if isinstance(content, bytes):
        content = content.decode("utf-8", "replace")
    return "insufficientPermissions" in content


class GmailHook(BaseHook):
    """Authenticate to Gmail and expose a cached ``googleapiclient`` service.

    The Connection carries the OAuth *installed* client in ``login`` (Client ID)
    and ``password`` (Client Secret), plus ``extra.refresh_token``. See the
    "Connection" section of the plan.
    """

    conn_name_attr = "gmail_conn_id"
    default_conn_name = "gmail_default"
    conn_type = "gmail"
    hook_name = "Gmail"

    def __init__(
        self, gmail_conn_id: str = default_conn_name, num_retries: int = 3
    ) -> None:
        super().__init__()
        self.gmail_conn_id = gmail_conn_id
        self.num_retries = num_retries
        self._service = None
        self._label_ids: dict[str, str] = {}

    @cached_property
    def user_id(self) -> str:
        """Required ``userId`` for every Gmail API call; ``extra.user_id`` or ``"me"``."""
        extra = self.get_connection(self.gmail_conn_id).extra_dejson
        return extra.get("user_id") or "me"

    def get_conn(self):
        """Build (once) and return the cached Gmail API service.

        Constructs ``Credentials`` from the Connection, performs an in-memory
        refresh grant and builds the ``gmail`` v1 service. Subsequent calls
        return the cached service, so the refresh happens exactly once per hook.
        A revoked/expired ``refresh_token`` surfaces as :class:`GmailAuthError`
        with an actionable hint rather than a raw ``RefreshError``.
        """
        if self._service is not None:
            return self._service

        conn = self.get_connection(self.gmail_conn_id)
        extra = conn.extra_dejson
        refresh_token = extra.get("refresh_token")
        if not refresh_token:
            raise GmailAuthError(
                f"Connection {self.gmail_conn_id!r} has no 'refresh_token' in its "
                "extra. The Gmail Connection needs {\"refresh_token\": \"1//...\"} "
                "(see the provider README)."
            )

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=conn.login,
            client_secret=conn.password,
            token_uri=_TOKEN_URI,
            scopes=extra.get("scopes"),
        )
        try:
            creds.refresh(Request())
        except RefreshError as exc:
            raise GmailAuthError(_REFRESH_HINT) from exc

        # cache_discovery=False: no file cache, avoids noisy warnings on 3.10+.
        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return self._service

    def build_query(
        self,
        window: Window,
        filter_processed_label: bool,
        label_name: str | None,
        from_email: str | None,
        subject_contains: str | None,
        has_attachment: bool,
        filename_contains: str | None,
        raw_query: str | None,
    ) -> str:
        """Assemble the Gmail search query string.

        The window arrives **already resolved** (:meth:`Window.resolve` is called
        by the operator/sensor after rendering the dates): this method never
        rebuilds the window and knows nothing about timezones. It only appends
        numeric ``after:``/``before:`` when the corresponding bound is not
        ``None`` — never ``after:YYYY/MM/DD``.

        A raw ``raw_query`` wins: when given, the structured fields
        (``from_email``, ``subject_contains``, ``has_attachment``,
        ``filename_contains``) are ignored (two sources of truth are not
        allowed) and a WARNING is logged if both were supplied. Otherwise the
        structured fields are ANDed together (space is Gmail's AND).

        ``has_attachment=True`` adds ``has:attachment``; ``False`` adds nothing
        (``-has:attachment`` is never emitted). ``-label:"<label_name>"`` is
        added **iff** ``filter_processed_label`` is ``True`` — the caller decides
        that policy (ADR-0001: S3 always passes ``False`` so the label never
        filters the search; local passes ``mark_processed``). The hook itself
        does not look at ``mark_processed``/``overwrite``.
        """
        structured_given = any(
            [from_email, subject_contains, has_attachment, filename_contains]
        )

        terms: list[str] = []
        if raw_query:
            if structured_given:
                self.log.warning(
                    "Both a raw 'query' and structured search fields were given; "
                    "the raw query wins and the structured fields are ignored."
                )
            terms.append(raw_query)
        else:
            if from_email:
                terms.append(f"from:{_field_value(from_email)}")
            if subject_contains:
                terms.append(f"subject:{_field_value(subject_contains)}")
            if has_attachment:
                terms.append("has:attachment")
            if filename_contains:
                terms.append(f"filename:{_field_value(filename_contains)}")

        after = window.after_epoch()
        if after is not None:
            terms.append(f"after:{after}")
        before = window.before_epoch()
        if before is not None:
            terms.append(f"before:{before}")

        if filter_processed_label and label_name:
            terms.append(f'-label:"{_escape(label_name)}"')

        return " ".join(terms)

    def _execute(self, request):
        """Run a Gmail API request with the hook's built-in retry.

        ``googleapiclient`` does **not** retry by default; Gmail throttles at
        250 quota units/user/second, so 429s are a matter of time under dozens
        of exports. ``execute(num_retries=N)`` adds exponential backoff for 429
        and 5xx. Every API call in the hook goes through here.
        """
        return request.execute(num_retries=self.num_retries)

    def search(self, query: str) -> list[str]:
        """Return the message ids matching ``query``, following pagination.

        Walks ``users.messages.list`` page by page via ``nextPageToken`` and
        reads ``.get("messages", [])`` — an empty result carries no ``messages``
        key, so indexing would raise ``KeyError`` instead of an honest "no mail".
        """
        service = self.get_conn()
        message_ids: list[str] = []
        page_token: str | None = None
        while True:
            request = (
                service.users()
                .messages()
                .list(userId=self.user_id, q=query, pageToken=page_token)
            )
            response = self._execute(request)
            for message in response.get("messages", []):
                message_ids.append(message["id"])
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return message_ids

    def get_message(self, message_id: str) -> dict:
        """Fetch a single message with the full MIME payload (``format="full"``)."""
        service = self.get_conn()
        request = (
            service.users()
            .messages()
            .get(userId=self.user_id, id=message_id, format="full")
        )
        return self._execute(request)

    def download_attachment(self, message_id: str, attachment: Attachment) -> bytes:
        """Return the decoded bytes of ``attachment``.

        The body arrives two ways: usually as ``attachment_id`` (fetched with a
        separate ``attachments.get`` call), but small attachments may inline the
        base64url body in ``data``. The choice is made on the **presence** of
        ``data`` (``attachment.data is not None``), not its truthiness — an empty
        file legitimately yields ``data == ""``. One attachment loads whole into
        memory; Gmail caps incoming messages at 25 MB, so the upper bound is
        known.
        """
        if attachment.data is not None:
            return _b64url_decode(attachment.data)

        service = self.get_conn()
        request = (
            service.users()
            .messages()
            .attachments()
            .get(userId=self.user_id, messageId=message_id, id=attachment.attachment_id)
        )
        response = self._execute(request)
        return _b64url_decode(response["data"])

    def find_messages_with_attachments(
        self, query: str, pattern: str | None
    ) -> list[MessageWithAttachments]:
        """Search, fetch, walk the MIME tree and keep the matching attachments.

        This is the **single** place that holds search + ``get_message`` +
        ``iter_attachments`` + pattern filtering + the "not our email" INFO log.
        Both the operator and both sensors go through here; there must be no
        second copy of this logic.

        ``pattern`` is an :func:`re.search` over the **decoded** filename; when it
        is ``None`` every attachment matches (inline images are already dropped
        by :func:`iter_attachments`). A message that passed the search but
        matched no attachment is logged at INFO with the real attachment
        filenames and the pattern, then dropped — a renamed report otherwise
        turns into a green task with no data.
        """
        results: list[MessageWithAttachments] = []
        for message_id in self.search(query):
            message = self.get_message(message_id)
            payload = message.get("payload") or {}
            attachments = list(iter_attachments(payload))
            if pattern is None:
                matched = attachments
            else:
                matched = [a for a in attachments if re.search(pattern, a.filename)]

            if not matched:
                self.log.info(
                    "Message %s passed the search but no attachment matched "
                    "pattern %r; attachments: [%s]",
                    message_id,
                    pattern,
                    ", ".join(a.filename for a in attachments),
                )
                continue

            results.append(
                MessageWithAttachments(
                    message_id=message_id,
                    internal_date=int(message["internalDate"]),
                    subject=header(payload, "Subject"),
                    from_=header(payload, "From"),
                    attachments=matched,
                )
            )
        return results

    def _execute_modify(self, request):
        """Run a mutating Gmail request, translating a 403 into a clear error.

        ``labels.create`` and ``messages.batchModify`` both need the
        ``gmail.modify`` scope. When the ``refresh_token`` was issued read-only,
        Gmail answers 403 ``insufficientPermissions``; that surfaces here as
        :class:`GmailPermissionError` with a hint to re-issue the token, instead
        of a raw ``HttpError``.
        """
        try:
            return self._execute(request)
        except HttpError as exc:
            if _is_insufficient_permissions(exc):
                raise GmailPermissionError(_MODIFY_HINT) from exc
            raise

    def get_or_create_label(self, name: str) -> str:
        """Return the ``labelId`` for ``name``, creating the label if missing.

        Lists ``users.labels`` and matches on the **full** name (slashes and
        all — nested Gmail labels are flat strings, there is no parent field);
        on a miss, ``labels.create`` makes it with the full name in one call
        (``airflow/processed/avito`` is created directly even when neither
        ``airflow`` nor ``airflow/processed`` exists yet). The result is cached
        for the hook's lifetime, so a repeated call for the same name issues no
        further ``labels.list``/``labels.create``.

        Concurrency: two overlapping ``DagRun``s of the same export may both miss
        the label and both create it, yielding two labels with the same name.
        This is non-critical — both labels are functional and searching by name
        still works — so no lock is introduced; ``max_active_runs=1`` on the DAG
        avoids it entirely.
        """
        cached = self._label_ids.get(name)
        if cached is not None:
            return cached

        service = self.get_conn()
        request = service.users().labels().list(userId=self.user_id)
        response = self._execute(request)
        for label in response.get("labels", []):
            if label.get("name") == name:
                label_id = label["id"]
                self._label_ids[name] = label_id
                return label_id

        request = (
            service.users()
            .labels()
            .create(userId=self.user_id, body={"name": name})
        )
        created = self._execute_modify(request)
        label_id = created["id"]
        self._label_ids[name] = label_id
        return label_id

    def mark_processed(self, message_ids: list[str], label_name: str) -> None:
        """Attach ``label_name`` to every message id via ``messages.batchModify``.

        The label name is resolved to a ``labelId`` once through
        :meth:`get_or_create_label`. Ids are sent in batches of at most 1000
        (``messages.batchModify`` caps at 1000 per call), each through the
        retrying :meth:`_execute`. A missing ``gmail.modify`` scope surfaces as
        :class:`GmailPermissionError`.
        """
        if not message_ids:
            return

        label_id = self.get_or_create_label(label_name)
        service = self.get_conn()
        for start in range(0, len(message_ids), _MODIFY_BATCH_SIZE):
            chunk = message_ids[start : start + _MODIFY_BATCH_SIZE]
            request = (
                service.users()
                .messages()
                .batchModify(
                    userId=self.user_id,
                    body={"ids": chunk, "addLabelIds": [label_id]},
                )
            )
            self._execute_modify(request)

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Extra fields shown on the Connection form.

        ``refresh_token`` is a password widget: it is long-lived and secret and
        must never be rendered in clear text. ``scopes`` is documentation only
        (the refreshed token carries exactly the scopes granted at consent).
        """
        from flask_appbuilder.fieldwidgets import (
            BS3PasswordFieldWidget,
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "refresh_token": PasswordField(
                lazy_gettext("Refresh Token"),
                widget=BS3PasswordFieldWidget(),
            ),
            "user_id": StringField(
                lazy_gettext("User ID"),
                widget=BS3TextFieldWidget(),
                default="me",
            ),
            "scopes": StringField(
                lazy_gettext("Scopes (reference only)"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Relabel login/password to the OAuth client fields; hide the unused ones."""
        return {
            "hidden_fields": ["host", "port", "schema"],
            "relabeling": {
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {
                "login": "OAuth client_id",
                "password": "OAuth client_secret",
                "refresh_token": "1//09fy...",
                "user_id": "me",
                "scopes": "https://www.googleapis.com/auth/gmail.readonly",
            },
        }
