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

from functools import cached_property

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

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

_REFRESH_HINT = (
    "Gmail refresh_token was rejected (revoked or expired). Re-issue it by "
    "walking through the OAuth consent again, and make sure the OAuth "
    "application is published ('In production'): in 'Testing' status Google "
    "issues a refresh_token that lives only 7 days and the pipeline dies "
    "silently after a week."
)


class GmailAuthError(AirflowException):
    """Authentication against Gmail failed (bad/missing/expired credentials)."""


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

    def __init__(self, gmail_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.gmail_conn_id = gmail_conn_id
        self._service = None

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
