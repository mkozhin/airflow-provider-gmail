"""Tests for :mod:`airflow_provider_gmail.operators.gmail` — base interface + helpers.

Covers the pure helpers (``to_local_date``/``to_local_iso``, ``resolve_collisions``,
``parse_date_range``), the ``template_fields`` set and ``__init__`` validation.
``execute()`` orchestration is Task 8 and is not exercised here. The abstract
operator is instantiated through a tiny concrete subclass implementing the
storage seam.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_provider_gmail.manifest import Manifest
from airflow_provider_gmail.operators.gmail import (
    GmailAttachmentsBaseOperator,
    parse_date_range,
    resolve_collisions,
    to_local_date,
    to_local_iso,
)
from airflow_provider_gmail.utils.mime import Attachment

MSK = "Europe/Moscow"


def _epoch_ms(dt: datetime) -> str:
    """Gmail ``internalDate`` string (epoch milliseconds) for an aware datetime."""
    return str(int(dt.timestamp() * 1000))


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
