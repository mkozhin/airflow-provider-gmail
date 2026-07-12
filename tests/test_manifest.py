"""Tests for :mod:`airflow_provider_gmail.manifest` — the pure domain module.

Pure: no operator, no hook, no S3, no Gmail, no network. Covers the schema
roundtrip, the single-``ManifestError`` error contract of ``from_json``, and the
ADR-0001 ``decide`` tri-state.
"""

from __future__ import annotations

import json

import pytest

from airflow_provider_gmail.manifest import (
    Decision,
    FileEntry,
    Manifest,
    ManifestError,
    decide,
)

RUN_ID = "scheduled__2026-07-10T06:00:00+00:00"

# The layer-2 contract schema (docs/gmail-pipeline-layers-2-3.md).
LAYER2_SCHEMA = {
    "source": "avito",
    "message_id": "18c2f4a9b3d5e6f7",
    "internal_date": "2026-07-10T09:14:22+03:00",
    "subject": "Отчёт за 09.07",
    "from": "reports@avito.ru",
    "run_id": RUN_ID,
    "files": [
        {
            "name": "report.xlsx",
            "size": 148223,
            "path": "gmail/avito/dt=2026-07-10/18c2f4a9b3d5e6f7/report.xlsx",
        }
    ],
}


def _sample_manifest() -> Manifest:
    return Manifest.build(
        "avito",
        "18c2f4a9b3d5e6f7",
        "2026-07-10T09:14:22+03:00",
        "Отчёт за 09.07",
        "reports@avito.ru",
        [
            FileEntry(
                name="report.xlsx",
                size=148223,
                path="gmail/avito/dt=2026-07-10/18c2f4a9b3d5e6f7/report.xlsx",
            )
        ],
        RUN_ID,
    )


# --- build / field order --------------------------------------------------


def test_build_arg_order_differs_from_field_order():
    m = _sample_manifest()
    # build(..., files, run_id) — files was the 6th positional, run_id the 7th,
    # but on the dataclass run_id precedes files.
    assert m.run_id == RUN_ID
    assert m.files[0].name == "report.xlsx"
    assert m.internal_date == "2026-07-10T09:14:22+03:00"
    assert m.from_ == "reports@avito.ru"


# --- schema / serialization -----------------------------------------------


def test_to_json_returns_bytes():
    assert isinstance(_sample_manifest().to_json(), bytes)


def test_to_json_uses_from_key_and_matches_layer2_schema():
    payload = json.loads(_sample_manifest().to_json())
    assert payload == LAYER2_SCHEMA
    assert "from" in payload
    assert "from_" not in payload


def test_roundtrip():
    m = _sample_manifest()
    back = Manifest.from_json(m.to_json())
    assert back == m


def test_from_json_accepts_layer2_schema():
    m = Manifest.from_json(json.dumps(LAYER2_SCHEMA))
    assert m.source == "avito"
    assert m.run_id == RUN_ID
    assert m.files[0].size == 148223


def test_cyrillic_subject_and_from_preserved():
    m = Manifest.build(
        "src",
        "id1",
        "2026-07-10T09:14:22+03:00",
        "Отчёт за июль",
        "Отправитель <отдел@пример.рф>",
        [FileEntry(name="файл.xlsx", size=1, path="p/файл.xlsx")],
        RUN_ID,
    )
    back = Manifest.from_json(m.to_json())
    assert back.subject == "Отчёт за июль"
    assert back.from_ == "Отправитель <отдел@пример.рф>"
    assert back.files[0].name == "файл.xlsx"


def test_str_and_bytes_inputs_equivalent():
    text = json.dumps(LAYER2_SCHEMA)
    from_str = Manifest.from_json(text)
    from_bytes = Manifest.from_json(text.encode("utf-8"))
    assert from_str == from_bytes


# --- error contract: everything becomes ManifestError ---------------------


def test_broken_json_raises_manifest_error():
    with pytest.raises(ManifestError):
        Manifest.from_json("{not json")


def test_json_array_instead_of_object_raises_manifest_error():
    with pytest.raises(ManifestError):
        Manifest.from_json("[]")


@pytest.mark.parametrize(
    "key",
    ["source", "message_id", "internal_date", "subject", "from", "run_id", "files"],
)
def test_missing_required_key_raises_manifest_error(key):
    data = dict(LAYER2_SCHEMA)
    del data[key]
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


def test_files_not_a_list_raises_manifest_error():
    data = dict(LAYER2_SCHEMA)
    data["files"] = {"name": "x", "size": 1, "path": "p"}
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


def test_files_element_without_path_raises_manifest_error():
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    del data["files"][0]["path"]
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


def test_files_element_not_object_raises_manifest_error():
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    data["files"] = ["just a string"]
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


def test_size_wrong_type_raises_manifest_error():
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    data["files"][0]["size"] = "148223"
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


def test_size_bool_raises_manifest_error():
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    data["files"][0]["size"] = True
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "[]",
        '{"files": []}',
        '{"source": 1, "message_id": 2, "internal_date": 3, "subject": 4, '
        '"from": 5, "run_id": 6, "files": "nope"}',
    ],
)
def test_no_low_level_exception_leaks(raw):
    # Only ManifestError may surface; a leaked KeyError/TypeError/ValueError/
    # JSONDecodeError would fail these assertions.
    with pytest.raises(ManifestError):
        try:
            Manifest.from_json(raw)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            pytest.fail(f"low-level exception leaked: {exc!r}")


# --- decide: ADR-0001 tri-state -------------------------------------------


def test_decide_none_downloads_and_delivers():
    assert decide(None, RUN_ID, overwrite=False) is Decision.DOWNLOAD_AND_DELIVER


def test_decide_current_run_id_delivers_only():
    m = _sample_manifest()  # run_id == RUN_ID
    assert decide(m, RUN_ID, overwrite=False) is Decision.DELIVER_ONLY


def test_decide_past_run_id_skips():
    m = _sample_manifest()
    assert decide(m, "some_other_run", overwrite=False) is Decision.SKIP


@pytest.mark.parametrize(
    "manifest_run_id, current_run_id",
    [
        (None, RUN_ID),  # no manifest
        (RUN_ID, RUN_ID),  # current run
        ("past_run", RUN_ID),  # past run
    ],
)
def test_decide_overwrite_always_downloads(manifest_run_id, current_run_id):
    if manifest_run_id is None:
        manifest = None
    else:
        manifest = Manifest.build(
            "s", "id", "2026-07-10T09:14:22+03:00", "sub", "f", [], manifest_run_id
        )
    assert (
        decide(manifest, current_run_id, overwrite=True)
        is Decision.DOWNLOAD_AND_DELIVER
    )
