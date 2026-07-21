"""Tests for :func:`airflow_provider_gmail.resolve._resolve` — the pure core.

Pure: no I/O, no mocks. The core takes ``(manifest_path, Manifest)`` pairs the
caller has already read and returns a flat list of full attachment paths. These
tests build :class:`Manifest` fixtures directly and import ``_resolve`` — the
internal seam — verifying the winner-selection and full-path-assembly semantics
in isolation from the S3/local read edge (:func:`resolve_attachments`, Task 2c).
"""

from __future__ import annotations

import pytest

from airflow_provider_gmail.manifest import FileEntry, Manifest
from airflow_provider_gmail.resolve import _resolve

BUCKET = "reports-bucket"


def _mk(
    message_id: str,
    internal_date: str,
    file_paths: list[str],
) -> Manifest:
    """A minimal manifest carrying the fields the resolver reads."""
    return Manifest.build(
        source="avito",
        message_id=message_id,
        internal_date_iso=internal_date,
        subject="Отчёт",
        from_="reports@avito.ru",
        files=[FileEntry(name="f.xlsx", size=1, path=p) for p in file_paths],
        run_id="scheduled__2026-07-10T06:00:00+00:00",
    )


def _s3_manifest_path(msg: str, dt: str = "2026-07-10") -> str:
    return f"s3://{BUCKET}/gmail/avito/dt={dt}/{msg}/_manifest.json"


def _s3_key(msg: str, filename: str, dt: str = "2026-07-10") -> str:
    return f"gmail/avito/dt={dt}/{msg}/{filename}"


# -- local manifest: paths verbatim, split_s3_uri never called ----------------


def test_local_manifest_returns_paths_verbatim():
    key = "/data/gmail/avito/dt=2026-07-10/MSG/report.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    assert _resolve([("/data/gmail/avito/dt=2026-07-10/MSG/_manifest.json", manifest)],
                    "all") == [key]


def test_local_manifest_does_not_call_split_s3_uri(monkeypatch):
    def _boom(uri):  # pragma: no cover - must never run
        raise AssertionError("split_s3_uri must not be called for a local manifest")

    monkeypatch.setattr("airflow_provider_gmail.resolve.split_s3_uri", _boom)
    key = "/data/x/report.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    assert _resolve([("/data/x/_manifest.json", manifest)], "all") == [key]
    # "latest" reaches the winner path assembly too — still no split for local.
    assert _resolve([("/data/x/_manifest.json", manifest)], "latest") == [key]


# -- s3 manifest: keys re-anchored to the manifest's bucket -------------------


def test_s3_manifest_reanchors_keys_to_bucket():
    key = _s3_key("MSG", "report.xlsx")
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{key}"
    ]


def test_all_flattens_in_input_order():
    m1 = _mk("A", "2026-07-10T09:00:00+03:00", [_s3_key("A", "a1.xlsx"),
                                                _s3_key("A", "a2.xlsx")])
    m2 = _mk("B", "2026-07-10T10:00:00+03:00", [_s3_key("B", "b1.xlsx")])
    result = _resolve(
        [(_s3_manifest_path("A"), m1), (_s3_manifest_path("B"), m2)], "all"
    )
    assert result == [
        f"s3://{BUCKET}/{_s3_key('A', 'a1.xlsx')}",
        f"s3://{BUCKET}/{_s3_key('A', 'a2.xlsx')}",
        f"s3://{BUCKET}/{_s3_key('B', 'b1.xlsx')}",
    ]


# -- latest: chronological, not lexicographic ---------------------------------


def test_latest_uses_instant_not_lexicographic_order():
    # m_early's ISO text sorts *after* m_late lexicographically ("09" > "07"),
    # but as an instant m_late (07:00Z) is later than m_early (09:00+03:00 = 06:00Z).
    m_early = _mk("EARLY", "2026-07-10T09:00:00+03:00", [_s3_key("EARLY", "e.xlsx")])
    m_late = _mk("LATE", "2026-07-10T07:00:00+00:00", [_s3_key("LATE", "l.xlsx")])
    assert "2026-07-10T09:00:00+03:00" > "2026-07-10T07:00:00+00:00"  # lexicographic
    result = _resolve(
        [(_s3_manifest_path("EARLY"), m_early), (_s3_manifest_path("LATE"), m_late)],
        "latest",
    )
    assert result == [f"s3://{BUCKET}/{_s3_key('LATE', 'l.xlsx')}"]


def test_latest_tie_breaks_on_larger_message_id():
    # Same instant, different message_id → the larger message_id wins.
    m_a = _mk("aaa", "2026-07-10T09:00:00+03:00", [_s3_key("aaa", "a.xlsx")])
    m_b = _mk("bbb", "2026-07-10T09:00:00+03:00", [_s3_key("bbb", "b.xlsx")])
    result = _resolve(
        [(_s3_manifest_path("aaa"), m_a), (_s3_manifest_path("bbb"), m_b)], "latest"
    )
    assert result == [f"s3://{BUCKET}/{_s3_key('bbb', 'b.xlsx')}"]


def test_latest_empty_winner_returns_empty_no_fallback():
    # The latest manifest has no files; there is NO fallback to the older one.
    winner = _mk("WIN", "2026-07-10T10:00:00+03:00", [])
    older = _mk("OLD", "2026-07-10T08:00:00+03:00", [_s3_key("OLD", "old.xlsx")])
    result = _resolve(
        [(_s3_manifest_path("WIN"), winner), (_s3_manifest_path("OLD"), older)],
        "latest",
    )
    assert result == []


def test_latest_empty_pairs_returns_empty():
    assert _resolve([], "latest") == []


def test_all_empty_pairs_returns_empty():
    assert _resolve([], "all") == []


# -- internal_date validation only touched by "latest" ------------------------


@pytest.mark.parametrize("bad_date", ["2026-07-10T09:00:00", "not-a-date", ""])
def test_latest_raises_on_naive_or_malformed_date(bad_date):
    manifest = _mk("MSG", bad_date, [_s3_key("MSG", "x.xlsx")])
    with pytest.raises(ValueError):
        _resolve([(_s3_manifest_path("MSG"), manifest)], "latest")


@pytest.mark.parametrize("bad_date", ["2026-07-10T09:00:00", "not-a-date", ""])
def test_all_ignores_internal_date(bad_date):
    # "all" never reads internal_date, so an invalid one does not raise.
    manifest = _mk("MSG", bad_date, [_s3_key("MSG", "x.xlsx")])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{_s3_key('MSG', 'x.xlsx')}"
    ]


# -- duplicates are neither deduplicated nor validated ------------------------


def test_all_keeps_duplicates():
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [_s3_key("MSG", "x.xlsx")])
    pair = (_s3_manifest_path("MSG"), manifest)
    result = _resolve([pair, pair], "all")
    assert result == [f"s3://{BUCKET}/{_s3_key('MSG', 'x.xlsx')}"] * 2


def test_latest_duplicate_returns_winner_once():
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [_s3_key("MSG", "x.xlsx")])
    pair = (_s3_manifest_path("MSG"), manifest)
    result = _resolve([pair, pair], "latest")
    assert result == [f"s3://{BUCKET}/{_s3_key('MSG', 'x.xlsx')}"]


# -- unknown pick -------------------------------------------------------------


@pytest.mark.parametrize("pairs", [[], None])
def test_unknown_pick_raises(pairs):
    # Unknown pick is rejected regardless of the (even empty/None) pairs.
    with pytest.raises(ValueError, match="pick must be one of"):
        _resolve(pairs if pairs is not None else [], "invalid")


# -- URI assembly for spaces / Unicode / defense-in-depth ---------------------


def test_uri_assembly_preserves_spaces_and_unicode():
    key = _s3_key("MSG", "отчёт за июль.xlsx")
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{key}"
    ]


def test_uri_assembly_preserves_old_special_chars_defense_in_depth():
    # Defense in depth: new keys are sanitized (Task 1a), but a foreign/old key
    # carrying ? # % must pass through untouched. Built MANUALLY (raw key), NOT
    # via sanitize_filename — otherwise the test would assert on nothing.
    raw_key = "gmail/avito/dt=2026-07-10/MSG/report?v=1#frag%20.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [raw_key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{raw_key}"
    ]
