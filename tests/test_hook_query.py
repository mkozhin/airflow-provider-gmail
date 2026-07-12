"""Tests for :meth:`GmailHook.build_query` and :func:`resolve_label_name`.

Pure string assembly: no Connection, no network. The window is supplied as a
plain :class:`Window` value object; all timezone/day-boundary subtlety is tested
separately in ``test_window.py``.
"""

from __future__ import annotations

import logging

from airflow_provider_gmail.hooks.gmail import GmailHook, resolve_label_name
from airflow_provider_gmail.window import Window

# A window with no bounds keeps field-only tests free of after:/before: noise.
NO_BOUNDS = Window(after=None, before=None)


def _hook() -> GmailHook:
    return GmailHook()


def _q(
    hook: GmailHook,
    window: Window = NO_BOUNDS,
    *,
    from_email: str | None = None,
    subject_contains: str | None = None,
    has_attachment: bool = False,
    filename_contains: str | None = None,
    raw_query: str | None = None,
) -> str:
    return hook.build_query(
        window,
        from_email,
        subject_contains,
        has_attachment,
        filename_contains,
        raw_query,
    )


# --- resolve_label_name --------------------------------------------------------


def test_resolve_label_name_default():
    assert resolve_label_name(None) == "airflow/processed"
    assert resolve_label_name("") == "airflow/processed"


def test_resolve_label_name_with_suffix():
    assert resolve_label_name("avito") == "airflow/processed/avito"


# --- structured fields ---------------------------------------------------------


def test_fields_only_anded():
    q = _q(
        _hook(),
        from_email="reports@avito.ru",
        subject_contains="report",
        has_attachment=True,
        filename_contains="report",
    )
    assert q == "from:reports@avito.ru subject:report has:attachment filename:report"


def test_raw_query_only():
    q = _q(_hook(), raw_query="from:x subject:y")
    assert q == "from:x subject:y"


def test_raw_query_wins_over_fields_and_warns(caplog):
    hook = _hook()
    with caplog.at_level(logging.WARNING):
        q = _q(
            hook,
            from_email="ignored@example.com",
            subject_contains="ignored",
            raw_query="from:winner@example.com",
        )
    assert q == "from:winner@example.com"
    assert "ignored@example.com" not in q
    assert "subject:ignored" not in q
    assert any("raw query wins" in r.message for r in caplog.records)


def test_raw_query_without_fields_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING):
        _q(_hook(), raw_query="from:x")
    assert not caplog.records


# --- quoting / escaping --------------------------------------------------------


def test_cyrillic_subject_with_space_is_quoted():
    q = _q(_hook(), subject_contains="Отчёт за")
    assert q == 'subject:"Отчёт за"'


def test_cyrillic_single_word_not_quoted():
    q = _q(_hook(), subject_contains="Отчёт")
    assert q == "subject:Отчёт"


def test_value_with_space_is_quoted():
    q = _q(_hook(), from_email="John Doe")
    assert q == 'from:"John Doe"'


def test_value_with_quote_and_backslash_is_escaped():
    q = _q(_hook(), subject_contains='a"b\\c')
    assert q == 'subject:"a\\"b\\\\c"'


def test_value_with_colon_is_quoted():
    # A bare colon would parse as an operator; quoting keeps it literal.
    q = _q(_hook(), subject_contains="re:invoice")
    assert q == 'subject:"re:invoice"'


def test_value_with_braces_is_quoted():
    q = _q(_hook(), subject_contains="{urgent}")
    assert q == 'subject:"{urgent}"'


def test_value_with_parens_is_quoted():
    q = _q(_hook(), subject_contains="(a)")
    assert q == 'subject:"(a)"'


def test_email_value_not_quoted():
    q = _q(_hook(), from_email="user@example.com")
    assert q == "from:user@example.com"


# --- has_attachment ------------------------------------------------------------


def test_has_attachment_true_adds_term():
    q = _q(_hook(), has_attachment=True)
    assert q == "has:attachment"


def test_has_attachment_false_adds_nothing():
    q = _q(_hook(), has_attachment=False)
    assert q == ""
    # never the negation
    assert "-has:attachment" not in q


# --- processed-label dedup is NOT a query term ---------------------------------


def test_build_query_never_emits_label_term():
    # The processed-label dedup moved out of the query (it is a labelIds
    # comparison in code — see find_messages_with_attachments's exclude_label_id).
    # build_query has no label parameters and can never emit a -label: term, for
    # any combination of fields or window bounds.
    q = _q(
        _hook(),
        Window(after=1000, before=2000),
        from_email="x@y.z",
        subject_contains="Отчёт",
        has_attachment=True,
        filename_contains="report",
    )
    assert "-label:" not in q
    assert "label:" not in q


# --- window bounds embedded ----------------------------------------------------


def test_after_and_before_embedded_from_window():
    window = Window(after=1000, before=2000)
    q = _q(_hook(), window, from_email="x@y.z")
    assert q == "from:x@y.z after:1000 before:2000"


def test_after_is_numeric_epoch_not_calendar_date():
    window = Window(after=1751317200, before=None)
    q = _q(_hook(), window)
    assert q == "after:1751317200"
    assert "after:2026" not in q  # never after:YYYY/MM/DD


def test_date_to_only_window_has_before_no_after():
    # after_epoch() is None => no after: term, only before:.
    window = Window(after=None, before=2000)
    q = _q(_hook(), window, from_email="x@y.z")
    assert q == "from:x@y.z before:2000"
    assert "after:" not in q
