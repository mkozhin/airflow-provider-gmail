"""Tests for :class:`GmailHook` authentication and connection handling.

No network: ``Credentials``/``Request``/``build`` are patched in the hook module
and the Connection is stubbed via ``get_connection``. The googleapiclient service
is mocked at the ``build()`` level (``hook.get_conn()``), the credentials refresh
grant is mocked, so nothing ever leaves the process.
"""

from __future__ import annotations

from unittest import mock

import pytest
from airflow.models.connection import Connection
from google.auth.exceptions import RefreshError

from airflow_provider_gmail.hooks.gmail import GmailAuthError, GmailHook

MODULE = "airflow_provider_gmail.hooks.gmail"


def _conn(extra: dict | None = None, login="client-id", password="client-secret"):
    import json

    if extra is None:
        extra = {"refresh_token": "1//refresh"}
    return Connection(
        conn_id="gmail_default",
        conn_type="gmail",
        login=login,
        password=password,
        extra=json.dumps(extra),
    )


def _hook(conn):
    hook = GmailHook()
    # Stub Connection lookup so no metadata DB is touched.
    hook.get_connection = mock.Mock(return_value=conn)  # type: ignore[method-assign]
    return hook


def test_credentials_built_from_connection():
    hook = _hook(_conn({"refresh_token": "1//abc"}))
    with mock.patch(f"{MODULE}.Credentials") as creds_cls, mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build") as build:
        service = hook.get_conn()

    creds_cls.assert_called_once()
    kwargs = creds_cls.call_args.kwargs
    assert kwargs["refresh_token"] == "1//abc"
    assert kwargs["client_id"] == "client-id"
    assert kwargs["client_secret"] == "client-secret"
    assert kwargs["token"] is None
    assert kwargs["token_uri"] == "https://oauth2.googleapis.com/token"
    # 'scopes' is reference-only and must never reach Credentials.
    assert "scopes" not in kwargs
    build.assert_called_once()
    assert build.call_args.args[:2] == ("gmail", "v1")
    assert service is build.return_value


def test_scopes_extra_is_reference_only_not_passed_to_credentials():
    # Regression: 'scopes' is a decorative reference-only extra. It arrives from
    # the UI as a STRING; google-auth treats Credentials(scopes=...) as an
    # iterable of scope strings, so passing the string would be split
    # character-by-character into bogus per-char scopes and break the refresh
    # grant for anyone who fills the field. It must not reach Credentials.
    scope_str = "https://www.googleapis.com/auth/gmail.readonly"
    hook = _hook(_conn({"refresh_token": "1//x", "scopes": scope_str}))
    with mock.patch(f"{MODULE}.Credentials") as creds_cls, mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build"):
        creds = creds_cls.return_value
        hook.get_conn()

    # Credentials is built without any 'scopes' argument...
    kwargs = creds_cls.call_args.kwargs
    assert "scopes" not in kwargs
    # ...and certainly not the string that would be iterated into per-char scopes.
    assert kwargs.get("scopes") != scope_str
    # The refresh still happens (auth is not broken by a filled 'scopes' field).
    creds.refresh.assert_called_once()


def test_refresh_called_once_across_repeated_get_conn():
    hook = _hook(_conn())
    with mock.patch(f"{MODULE}.Credentials") as creds_cls, mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build") as build:
        creds = creds_cls.return_value
        first = hook.get_conn()
        second = hook.get_conn()

    assert first is second
    creds.refresh.assert_called_once()
    build.assert_called_once()


def test_no_access_token_stored_in_connection():
    # The refreshed access_token must never be written back anywhere.
    conn = _conn()
    hook = _hook(conn)
    with mock.patch(f"{MODULE}.Credentials") as creds_cls, mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build"):
        creds_cls.return_value.token = "ya29.access-token"
        hook.get_conn()

    # Credentials constructed with token=None; nothing set an access_token on the conn.
    assert creds_cls.call_args.kwargs["token"] is None
    assert "access_token" not in conn.extra_dejson


def test_missing_refresh_token_raises_clear_error():
    hook = _hook(_conn({"user_id": "me"}))  # no refresh_token
    with mock.patch(f"{MODULE}.Credentials"), mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build"):
        with pytest.raises(GmailAuthError, match="refresh_token"):
            hook.get_conn()


def test_refresh_error_wrapped_with_hint():
    hook = _hook(_conn())
    with mock.patch(f"{MODULE}.Credentials") as creds_cls, mock.patch(
        f"{MODULE}.Request"
    ), mock.patch(f"{MODULE}.build"):
        creds_cls.return_value.refresh.side_effect = RefreshError("bad token")
        with pytest.raises(GmailAuthError) as exc_info:
            hook.get_conn()

    message = str(exc_info.value)
    assert "7 days" in message
    assert "In production" in message
    # Original RefreshError preserved as the cause.
    assert isinstance(exc_info.value.__cause__, RefreshError)


def test_user_id_from_extra():
    hook = _hook(_conn({"refresh_token": "1//x", "user_id": "team@example.com"}))
    assert hook.user_id == "team@example.com"


def test_user_id_defaults_to_me():
    hook = _hook(_conn({"refresh_token": "1//x"}))
    assert hook.user_id == "me"


def test_form_hides_refresh_token_behind_password_widget():
    from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
    from wtforms import PasswordField

    widgets = GmailHook.get_connection_form_widgets()
    for field in ("refresh_token", "user_id", "scopes"):
        assert field in widgets

    rt = widgets["refresh_token"]
    assert rt.field_class is PasswordField
    assert isinstance(rt.kwargs["widget"], BS3PasswordFieldWidget)


def test_form_relabels_and_hides_fields():
    behaviour = GmailHook.get_ui_field_behaviour()
    assert behaviour["relabeling"] == {
        "login": "Client ID",
        "password": "Client Secret",
    }
    for hidden in ("host", "port", "schema"):
        assert hidden in behaviour["hidden_fields"]


def test_hook_class_attributes():
    assert GmailHook.conn_type == "gmail"
    assert GmailHook.hook_name == "Gmail"
    assert GmailHook.default_conn_name == "gmail_default"
    assert GmailHook.conn_name_attr == "gmail_conn_id"
