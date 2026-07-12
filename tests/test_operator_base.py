"""Tests for :mod:`airflow_provider_gmail.operators.gmail` — base interface + helpers.

Covers the pure helpers (``to_local_date``/``to_local_iso``, ``resolve_collisions``,
``parse_date_range``), the ``template_fields`` set and ``__init__`` validation.
``execute()`` orchestration is Task 8 and is not exercised here. The abstract
operator is instantiated through a tiny concrete subclass implementing the
storage seam.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow.exceptions import AirflowSkipException

from airflow_provider_gmail.hooks.gmail import MessageWithAttachments
from airflow_provider_gmail.manifest import Manifest
from airflow_provider_gmail.operators.gmail import (
    GmailAttachmentsBaseOperator,
    parse_date_range,
    resolve_collisions,
    to_local_date,
    to_local_iso,
)
from airflow_provider_gmail.utils.mime import Attachment
from airflow_provider_gmail.utils.paths import MANIFEST_FILENAME

MSK = "Europe/Moscow"


def _epoch_ms(dt: datetime) -> int:
    """Gmail ``internalDate`` (epoch milliseconds) as the hook hands it on — an int."""
    return int(dt.timestamp() * 1000)


def _attachment(filename: str) -> Attachment:
    return Attachment(
        filename=filename, mime_type="application/octet-stream", attachment_id="x", data=None
    )


class _ConcreteOperator(GmailAttachmentsBaseOperator):
    """Minimal concrete subclass so the abstract base can be instantiated."""

    def _write(self, rel_path, data):  # pragma: no cover - trivial
        pass

    def _destination_path(self, rel_path):  # pragma: no cover - trivial
        return rel_path

    def _read_manifest(self, rel_dir):  # pragma: no cover - trivial
        return None

    def _filter_processed_label(self):  # pragma: no cover - trivial
        return False


def _make_operator(**kwargs) -> _ConcreteOperator:
    params = {"task_id": "t", "source": "avito"}
    params.update(kwargs)
    return _ConcreteOperator(**params)


# -- to_local_date / to_local_iso -------------------------------------------


def test_to_local_iso_has_operator_zone_offset():
    dt = datetime(2026, 7, 10, 9, 14, 22, tzinfo=ZoneInfo(MSK))
    iso = to_local_iso(_epoch_ms(dt), MSK)
    assert iso == "2026-07-10T09:14:22+03:00"


def test_to_local_date_and_iso_consistent_for_same_internal_date():
    dt = datetime(2026, 7, 10, 23, 45, 0, tzinfo=ZoneInfo(MSK))
    internal = _epoch_ms(dt)
    iso = to_local_iso(internal, MSK)
    partition = to_local_date(internal, MSK)
    assert iso.startswith(partition.isoformat())


def test_to_local_date_after_midnight_msk_is_current_day():
    # 01:30 MSK on 2026-07-12 is still 2026-07-12, not 2026-07-11 (which a UTC
    # reading would give, since 01:30 MSK == 22:30 UTC the previous day).
    dt = datetime(2026, 7, 12, 1, 30, 0, tzinfo=ZoneInfo(MSK))
    assert to_local_date(_epoch_ms(dt), MSK) == date(2026, 7, 12)


def test_to_local_date_zone_taken_from_argument():
    dt = datetime(2026, 7, 12, 1, 30, 0, tzinfo=ZoneInfo(MSK))
    # Same instant, read in UTC, falls on the previous day.
    assert to_local_date(_epoch_ms(dt), "UTC") == date(2026, 7, 11)


# -- resolve_collisions ------------------------------------------------------


def test_resolve_collisions_two_identical_names():
    a, b = _attachment("report.xlsx"), _attachment("report.xlsx")
    resolved = resolve_collisions([a, b])
    assert [name for _, name in resolved] == ["report.xlsx", "report_1.xlsx"]
    # Attachment is not mutated (no safe_name field).
    assert not hasattr(a, "safe_name")


def test_resolve_collisions_tracks_occupied_set():
    atts = [_attachment("report.xlsx"), _attachment("report.xlsx"), _attachment("report_1.xlsx")]
    names = [name for _, name in resolve_collisions(atts)]
    assert names == ["report.xlsx", "report_1.xlsx", "report_1_1.xlsx"]
    assert len(set(names)) == 3


def test_resolve_collisions_sanitizes_hostile_name():
    resolved = resolve_collisions([_attachment("../../evil.xlsx")])
    assert [name for _, name in resolved] == ["evil.xlsx"]


def test_resolve_collisions_returns_pairs_with_attachment():
    a = _attachment("report.xlsx")
    resolved = resolve_collisions([a])
    assert resolved[0][0] is a


def test_resolve_collisions_two_extensionless_names():
    # No extension: the suffix is appended to the whole name (no ".").
    atts = [_attachment("README"), _attachment("README")]
    names = [name for _, name in resolve_collisions(atts)]
    assert names == ["README", "README_1"]


def test_resolve_collisions_two_dotfiles():
    # A dotfile (leading dot, no extension) collides as ".bashrc_1", not
    # "_1.bashrc" — os.path.splitext treats it as an extensionless name.
    atts = [_attachment(".bashrc"), _attachment(".bashrc")]
    names = [name for _, name in resolve_collisions(atts)]
    assert names == [".bashrc", ".bashrc_1"]


def test_resolve_collisions_multi_increment_when_candidate_occupied():
    # The first free suffix must skip an already-occupied "_1" candidate.
    atts = [
        _attachment("report.xlsx"),
        _attachment("report_1.xlsx"),
        _attachment("report.xlsx"),
    ]
    names = [name for _, name in resolve_collisions(atts)]
    assert names == ["report.xlsx", "report_1.xlsx", "report_2.xlsx"]
    assert len(set(names)) == 3


def test_resolve_collisions_reserves_manifest_filename():
    # An attachment sanitizing to the manifest filename must never be assigned
    # _manifest.json — that key is written LAST by the manifest, which would
    # silently overwrite the attachment (data loss, ADR-0001).
    resolved = resolve_collisions([_attachment(MANIFEST_FILENAME)])
    names = [name for _, name in resolved]
    assert names == ["_manifest_1.json"]
    assert MANIFEST_FILENAME not in names


def test_resolve_collisions_two_manifest_named_both_avoid_reserved():
    # Two attachments both named _manifest.json avoid the reserved name and each
    # other, yielding two distinct suffixed names.
    atts = [_attachment(MANIFEST_FILENAME), _attachment(MANIFEST_FILENAME)]
    names = [name for _, name in resolve_collisions(atts)]
    assert names == ["_manifest_1.json", "_manifest_2.json"]
    assert MANIFEST_FILENAME not in names
    assert len(set(names)) == 2


# -- parse_date_range --------------------------------------------------------


def test_parse_date_range_valid():
    assert parse_date_range("2026-07-01", "2026-07-10") == (
        date(2026, 7, 1),
        date(2026, 7, 10),
    )


def test_parse_date_range_only_date_from():
    assert parse_date_range("2026-07-01", None) == (date(2026, 7, 1), None)


def test_parse_date_range_only_date_to():
    assert parse_date_range(None, "2026-07-10") == (None, date(2026, 7, 10))


def test_parse_date_range_both_none():
    assert parse_date_range(None, None) == (None, None)


def test_parse_date_range_invalid_iso_raises():
    with pytest.raises(ValueError):
        parse_date_range("10/07/2026", None)


def test_parse_date_range_reversed_raises():
    with pytest.raises(ValueError):
        parse_date_range("2026-07-10", "2026-07-01")


# -- template_fields ---------------------------------------------------------


def test_template_fields_expected_set():
    expected = {
        "query",
        "source",
        "from_email",
        "subject_contains",
        "filename_contains",
        "date_from",
        "date_to",
    }
    assert expected <= set(GmailAttachmentsBaseOperator.template_fields)
    # attachment_pattern must NOT be templated (ADR-0005).
    assert "attachment_pattern" not in GmailAttachmentsBaseOperator.template_fields


# -- __init__ validation -----------------------------------------------------


def test_init_valid_defaults():
    op = _make_operator()
    assert op.lookback_days == 7
    assert op.timezone == MSK
    assert op._compiled_pattern is None


def test_init_negative_lookback_days_raises():
    with pytest.raises(ValueError):
        _make_operator(lookback_days=-1)


def test_init_unknown_timezone_raises():
    with pytest.raises(ValueError):
        _make_operator(timezone="Mars/Olympus")


def test_init_bad_attachment_pattern_raises():
    with pytest.raises(Exception):
        _make_operator(attachment_pattern="[")


def test_init_compiles_valid_pattern():
    op = _make_operator(attachment_pattern=r"^report_\d{8}\.xlsx$")
    assert op._compiled_pattern is not None
    assert op._compiled_pattern.search("report_20260710.xlsx")


# -- abstract enforcement ----------------------------------------------------


def test_seam_methods_are_abstract_on_base():
    # A subclass that forgets the seam must fail loudly. We assert the base
    # bodies raise rather than silently no-op.
    class _Broken(GmailAttachmentsBaseOperator):
        pass

    op = _Broken(task_id="b", source="avito")
    with pytest.raises(NotImplementedError):
        op._write("x", b"")
    with pytest.raises(NotImplementedError):
        op._destination_path("x")
    with pytest.raises(NotImplementedError):
        op._read_manifest("x")
    with pytest.raises(NotImplementedError):
        op._filter_processed_label()


# -- manifest assembly helper ------------------------------------------------


def test_write_manifest_helper_builds_and_writes():
    written: dict[str, bytes] = {}

    class _Recording(_ConcreteOperator):
        def _write(self, rel_path, data):
            written[rel_path] = data

    op = _Recording(task_id="t", source="avito", timezone=MSK)
    dt = datetime(2026, 7, 10, 9, 14, 22, tzinfo=ZoneInfo(MSK))
    manifest = op._write_manifest(
        "dt=2026-07-10/msg1", "msg1", _epoch_ms(dt), "Отчёт", "a@b.ru", [], "run-1"
    )
    assert "dt=2026-07-10/msg1/_manifest.json" in written
    reparsed = Manifest.from_json(written["dt=2026-07-10/msg1/_manifest.json"])
    assert reparsed.internal_date == "2026-07-10T09:14:22+03:00"
    assert reparsed.run_id == "run-1"
    assert manifest.subject == "Отчёт"


# ===========================================================================
# Task 8: execute() orchestration + run_id dedup
# ===========================================================================

CURRENT_RUN = "scheduled__2026-07-12T06:00:00+00:00"
PAST_RUN = "scheduled__2026-07-11T06:00:00+00:00"


def _internal_ms(dt: datetime) -> int:
    """Gmail ``internalDate`` as an int (epoch milliseconds)."""
    return int(dt.timestamp() * 1000)


def _message(message_id: str, *filenames: str, day: date = date(2026, 7, 12)):
    """A matched message on ``day`` (09:00 MSK) with the named attachments."""
    dt = datetime(day.year, day.month, day.day, 9, 0, 0, tzinfo=ZoneInfo(MSK))
    return MessageWithAttachments(
        message_id=message_id,
        internal_date=_internal_ms(dt),
        subject="Отчёт",
        from_="reports@avito.ru",
        attachments=[_attachment(f) for f in filenames],
    )


def _rel_dir(msg: MessageWithAttachments) -> str:
    return f"dt={to_local_date(msg.internal_date, MSK)}/{msg.message_id}"


def _seed_manifest(store: dict, msg: MessageWithAttachments, run_id: str) -> None:
    """Pre-write a manifest for ``msg`` into ``store`` (as a prior run would)."""
    manifest = Manifest.build(
        "avito",
        msg.message_id,
        to_local_iso(msg.internal_date, MSK),
        msg.subject,
        msg.from_,
        [],
        run_id,
    )
    store[f"{_rel_dir(msg)}/_manifest.json"] = manifest.to_json()


class _FakeHook:
    """A stand-in for :class:`GmailHook` recording calls and returning canned messages."""

    def __init__(self, messages, mark_error: Exception | None = None):
        self._messages = list(messages)
        self.mark_error = mark_error
        self.window = None
        self.filter_processed_label = None
        self.built_query = None
        self.search_pattern = "<unset>"
        self.downloaded: list[tuple[str, str]] = []
        self.marked: list[tuple[list[str], str]] = []

    def build_query(
        self,
        window,
        filter_processed_label,
        label_name,
        from_email,
        subject_contains,
        has_attachment,
        filename_contains,
        raw_query,
    ) -> str:
        self.window = window
        self.filter_processed_label = filter_processed_label
        self.label_name = label_name
        self.built_query = "QUERY"
        return "QUERY"

    def find_messages_with_attachments(self, query, pattern):
        self.search_pattern = pattern
        return list(self._messages)

    def download_attachment(self, message_id, attachment) -> bytes:
        self.downloaded.append((message_id, attachment.filename))
        return b"bytes-of-" + attachment.filename.encode()

    def mark_processed(self, message_ids, label_name) -> None:
        self.marked.append((list(message_ids), label_name))
        if self.mark_error is not None:
            raise self.mark_error


class _DictOperator(GmailAttachmentsBaseOperator):
    """Concrete operator whose storage seam is an in-memory ``dict`` (``rel_path`` → bytes).

    ``_read_manifest`` parses from the *same* ``store`` that ``_write`` fills, so a
    retry over one shared ``store`` behaves like a real re-run of the task.
    """

    def __init__(
        self,
        *,
        store: dict | None = None,
        fail_write_containing: str | None = None,
        filter_label: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.store = {} if store is None else store
        self._fail_write_containing = fail_write_containing
        self._filter_label = filter_label

    def _write(self, rel_path, data):
        if self._fail_write_containing and self._fail_write_containing in rel_path:
            raise RuntimeError(f"write failed for {rel_path}")
        self.store[rel_path] = data

    def _destination_path(self, rel_path):
        return f"s3://bucket/{rel_path}"

    def _read_manifest(self, rel_dir):
        raw = self.store.get(f"{rel_dir}/_manifest.json")
        return None if raw is None else Manifest.from_json(raw)

    def _filter_processed_label(self):
        return self._filter_label


def _context(run_id: str = CURRENT_RUN, hour: int = 6) -> dict:
    """A minimal execute() context; ``data_interval_end`` is the stable reference day."""
    return {
        "run_id": run_id,
        "data_interval_end": datetime(2026, 7, 12, hour, 0, 0, tzinfo=ZoneInfo("UTC")),
    }


def _run(op: _DictOperator, hook: _FakeHook, context: dict | None = None):
    op.hook = hook  # cached_property is non-data → instance dict wins
    return op.execute(context or _context())


# -- write order / "not our email" / empty search ---------------------------


def test_execute_writes_manifest_last():
    op = _DictOperator(task_id="t", source="avito")
    hook = _FakeHook([_message("msg1", "a.xlsx", "b.xlsx")])
    result = _run(op, hook)

    keys = list(op.store.keys())
    assert keys == [
        "dt=2026-07-12/msg1/a.xlsx",
        "dt=2026-07-12/msg1/b.xlsx",
        "dt=2026-07-12/msg1/_manifest.json",
    ]
    assert result == ["s3://bucket/dt=2026-07-12/msg1/_manifest.json"]


def test_execute_attachment_named_manifest_not_overwritten_by_manifest():
    # An attachment literally named _manifest.json must land under a suffixed key
    # and the real manifest must still be written — the manifest never clobbers
    # the attachment, and its files[].path points at the suffixed key (ADR-0001).
    op = _DictOperator(task_id="t", source="avito")
    hook = _FakeHook([_message("msg1", MANIFEST_FILENAME)])
    result = _run(op, hook)

    attachment_key = "dt=2026-07-12/msg1/_manifest_1.json"
    manifest_key = "dt=2026-07-12/msg1/_manifest.json"
    # Both keys exist and are distinct: the attachment survived.
    assert op.store[attachment_key] == b"bytes-of-_manifest.json"
    manifest = Manifest.from_json(op.store[manifest_key])
    assert [f.path for f in manifest.files] == [f"s3://bucket/{attachment_key}"]
    assert [f.name for f in manifest.files] == [MANIFEST_FILENAME]
    assert result == [f"s3://bucket/{manifest_key}"]


def test_execute_not_our_email_writes_no_manifest_and_no_label():
    # The hook already drops "not our email" (no attachment matched), so from the
    # operator's view it simply returns nothing: no writes, no label, and a skip.
    op = _DictOperator(task_id="t", source="avito", mark_processed=True)
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    assert op.store == {}
    assert hook.marked == []


def test_execute_empty_search_skips():
    op = _DictOperator(task_id="t", source="avito")
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)


def test_execute_passes_compiled_attachment_pattern_to_hook():
    # execute() passes the COMPILED pattern (validation is load-bearing), not the
    # raw string, so the __init__-time re.compile is what actually filters.
    op = _DictOperator(
        task_id="t", source="avito", attachment_pattern=r"\.xlsx$"
    )
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    assert isinstance(hook.search_pattern, re.Pattern)
    assert hook.search_pattern.pattern == r"\.xlsx$"


def test_execute_passes_none_pattern_when_unset():
    op = _DictOperator(task_id="t", source="avito")
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    assert hook.search_pattern is None


# -- date-range validation after render (ADR-0004) --------------------------


def test_execute_bad_iso_date_raises():
    op = _DictOperator(task_id="t", source="avito", date_from="10/07/2026")
    hook = _FakeHook([])
    with pytest.raises(ValueError):
        _run(op, hook)


def test_execute_reversed_range_raises():
    op = _DictOperator(
        task_id="t", source="avito", date_from="2026-07-10", date_to="2026-07-01"
    )
    hook = _FakeHook([])
    with pytest.raises(ValueError):
        _run(op, hook)


def test_execute_one_sided_range_only_from_reaches_build_query():
    op = _DictOperator(task_id="t", source="avito", date_from="2026-07-01")
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    # date_from → after_epoch present; no date_to → before_epoch None.
    assert hook.window.after_epoch() is not None
    assert hook.window.before_epoch() is None


def test_execute_one_sided_range_only_to_reaches_build_query():
    op = _DictOperator(task_id="t", source="avito", date_to="2026-07-10")
    hook = _FakeHook([])
    with pytest.raises(AirflowSkipException):
        _run(op, hook)
    # only date_to → no lower bound (ADR-0004), before present.
    assert hook.window.after_epoch() is None
    assert hook.window.before_epoch() is not None


def test_execute_nondefault_lookback_with_range_warns(caplog):
    op = _DictOperator(
        task_id="t", source="avito", lookback_days=3, date_from="2026-07-01"
    )
    hook = _FakeHook([])
    with caplog.at_level(logging.WARNING):
        with pytest.raises(AirflowSkipException):
            _run(op, hook)
    assert any("lookback_days" in r.message for r in caplog.records)


def test_execute_default_lookback_with_range_does_not_warn(caplog):
    # Default lookback_days (7) + a range must NOT warn: nothing was overridden.
    op = _DictOperator(task_id="t", source="avito", date_from="2026-07-01")
    hook = _FakeHook([])
    with caplog.at_level(logging.WARNING):
        with pytest.raises(AirflowSkipException):
            _run(op, hook)
    assert not any("lookback_days" in r.message for r in caplog.records)


# -- orchestration tri-state -------------------------------------------------


def test_execute_past_run_manifest_not_delivered_label_caught_up():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, PAST_RUN)
    op = _DictOperator(task_id="t", source="avito", store=store, mark_processed=True)
    hook = _FakeHook([msg])

    with pytest.raises(AirflowSkipException):
        _run(op, hook)  # nothing delivered → skip

    assert hook.downloaded == []  # past run → not re-downloaded
    assert hook.marked == [(["msg1"], "airflow/processed")]  # label caught up


def test_execute_current_run_manifest_delivered_not_downloaded():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")
    _seed_manifest(store, msg, CURRENT_RUN)
    op = _DictOperator(task_id="t", source="avito", store=store)
    hook = _FakeHook([msg])

    result = _run(op, hook)

    assert result == ["s3://bucket/dt=2026-07-12/msg1/_manifest.json"]
    assert hook.downloaded == []  # failed prior attempt of THIS run → deliver, no download


def test_execute_no_manifest_downloads_and_delivers():
    op = _DictOperator(task_id="t", source="avito")
    hook = _FakeHook([_message("msg1", "a.xlsx")])

    result = _run(op, hook)

    assert result == ["s3://bucket/dt=2026-07-12/msg1/_manifest.json"]
    assert hook.downloaded == [("msg1", "a.xlsx")]
    assert "dt=2026-07-12/msg1/_manifest.json" in op.store


def test_execute_write_failure_on_second_message_propagates():
    store: dict = {}
    msgs = [_message("msg1", "a.xlsx"), _message("msg2", "b.xlsx")]
    op = _DictOperator(
        task_id="t", source="avito", store=store, fail_write_containing="msg2"
    )
    hook = _FakeHook(msgs)

    with pytest.raises(RuntimeError):
        _run(op, hook)

    # First message fully written with the current run_id; second has no manifest.
    first_manifest = Manifest.from_json(store["dt=2026-07-12/msg1/_manifest.json"])
    assert first_manifest.run_id == CURRENT_RUN
    assert "dt=2026-07-12/msg2/_manifest.json" not in store


def test_execute_retry_same_run_delivers_first_downloads_second():
    # First attempt wrote msg1's manifest then died before msg2; on retry msg1 is
    # delivered from its current-run manifest (no re-download), msg2 is downloaded.
    store: dict = {}
    msg1, msg2 = _message("msg1", "a.xlsx"), _message("msg2", "b.xlsx")
    _seed_manifest(store, msg1, CURRENT_RUN)
    op = _DictOperator(task_id="t", source="avito", store=store)
    hook = _FakeHook([msg1, msg2])

    result = _run(op, hook)

    assert result == [
        "s3://bucket/dt=2026-07-12/msg1/_manifest.json",
        "s3://bucket/dt=2026-07-12/msg2/_manifest.json",
    ]
    assert hook.downloaded == [("msg2", "b.xlsx")]  # only the missing one


def test_execute_mark_processed_failure_then_retry_redelivers():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")

    # Attempt 1: manifests written, then mark_processed() fails → task fails.
    op1 = _DictOperator(task_id="t", source="avito", store=store, mark_processed=True)
    hook1 = _FakeHook([msg], mark_error=RuntimeError("batchModify failed"))
    with pytest.raises(RuntimeError):
        _run(op1, hook1)
    assert Manifest.from_json(store["dt=2026-07-12/msg1/_manifest.json"]).run_id == CURRENT_RUN

    # Retry (same run_id, same store): message re-delivered from current-run
    # manifest, not re-downloaded; mark_processed now succeeds.
    op2 = _DictOperator(task_id="t", source="avito", store=store, mark_processed=True)
    hook2 = _FakeHook([msg])
    result = _run(op2, hook2)
    assert result == ["s3://bucket/dt=2026-07-12/msg1/_manifest.json"]
    assert hook2.downloaded == []
    assert hook2.marked == [(["msg1"], "airflow/processed")]


def test_execute_all_past_manifests_in_window_skips():
    store: dict = {}
    msgs = [
        _message("msg1", "a.xlsx", day=date(2026, 7, 12)),
        _message("msg2", "b.xlsx", day=date(2026, 7, 11)),
        _message("msg3", "c.xlsx", day=date(2026, 7, 10)),
    ]
    for m in msgs:
        _seed_manifest(store, m, PAST_RUN)
    op = _DictOperator(task_id="t", source="avito", store=store, lookback_days=2)
    hook = _FakeHook(msgs)

    with pytest.raises(AirflowSkipException):  # every message SKIP → empty XCom
        _run(op, hook)
    assert hook.downloaded == []


# -- window stability across midnight ---------------------------------------


def test_execute_window_stable_across_midnight_and_retry_delivers():
    store: dict = {}
    msg = _message("msg1", "a.xlsx")

    # Attempt 1 at 23:59-ish "now" (irrelevant: window uses data_interval_end).
    op1 = _DictOperator(task_id="t", source="avito", store=store)
    hook1 = _FakeHook([msg])
    _run(op1, hook1, _context(hour=6))
    after1 = hook1.window.after_epoch()

    # Attempt 2 (retry) with the SAME data_interval_end but a different wall clock.
    op2 = _DictOperator(task_id="t", source="avito", store=store)
    hook2 = _FakeHook([msg])
    result = _run(op2, hook2, _context(hour=6))
    after2 = hook2.window.after_epoch()

    assert after1 == after2  # window did not move between attempts
    # The current-run message is still found and delivered on retry, not lost.
    assert result == ["s3://bucket/dt=2026-07-12/msg1/_manifest.json"]
    assert hook2.downloaded == []  # already downloaded in attempt 1
