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


@pytest.mark.parametrize(
    "key",
    ["source", "message_id", "internal_date", "subject", "from", "run_id"],
)
@pytest.mark.parametrize("bad", [123, None, 3.5, {"x": 1}, ["a"]])
def test_scalar_field_wrong_type_raises_manifest_error(key, bad):
    # A present-but-mistyped scalar (run_id as int, subject as number, from as
    # null, ...) must fail loudly rather than be accepted as a valid past-run
    # manifest and silently skip the message.
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    data[key] = bad
    with pytest.raises(ManifestError):
        Manifest.from_json(json.dumps(data))


@pytest.mark.parametrize("key", ["name", "path"])
@pytest.mark.parametrize("bad", [123, None, 3.5, {"x": 1}])
def test_file_string_field_wrong_type_raises_manifest_error(key, bad):
    data = json.loads(json.dumps(LAYER2_SCHEMA))
    data["files"][0][key] = bad
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


# --- error contract: non-UTF-8 / BOM on the local bytes path --------------


@pytest.mark.parametrize("encoding", ["utf-16", "utf-32"])
def test_non_utf8_bytes_raises_not_valid_utf8(encoding):
    # UTF-16/UTF-32-encoded bytes used to be silently accepted (json.loads
    # auto-detects the BOM); the local bytes path now decodes strictly as UTF-8
    # first, so they surface as a ManifestError pinned to "not valid UTF-8" —
    # uniform with the S3 path. A type-only assertion would pass on the old code
    # for b"\xff", so the match pins the new branch.
    raw = json.dumps(LAYER2_SCHEMA).encode(encoding)
    with pytest.raises(ManifestError, match="not valid UTF-8"):
        Manifest.from_json(raw)


def test_raw_non_utf8_bytes_raises_not_valid_utf8():
    # A bare non-UTF-8 byte: previously ManifestError("... not valid JSON ...")
    # (UnicodeDecodeError is a ValueError subclass); now the dedicated UTF-8
    # branch produces "not valid UTF-8". This direct local-bytes case had no
    # prior test.
    with pytest.raises(ManifestError, match="not valid UTF-8") as excinfo:
        Manifest.from_json(b"\xff")
    assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)


def test_utf8_with_bom_bytes_raises_manifest_error():
    # UTF-8-with-BOM (utf-8-sig): the leading BOM is no longer stripped and
    # survives to json.loads, so it becomes a ManifestError (via the JSON
    # branch). Only the type is pinned here, not the message.
    raw = json.dumps(LAYER2_SCHEMA).encode("utf-8-sig")
    with pytest.raises(ManifestError):
        Manifest.from_json(raw)


def test_utf8_with_bom_str_raises_manifest_error():
    # str-analogue: a leading BOM character reaches json.loads and fails.
    text = "﻿" + json.dumps(LAYER2_SCHEMA)
    with pytest.raises(ManifestError):
        Manifest.from_json(text)


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "[]",
        '{"files": []}',
        '{"source": 1, "message_id": 2, "internal_date": 3, "subject": 4, '
        '"from": 5, "run_id": 6, "files": "nope"}',
        # Mistyped scalars but a structurally valid files list: must still be a
        # clean ManifestError, not a leaked TypeError at dataclass construction.
        '{"source": 1, "message_id": "m", "internal_date": "d", "subject": "s", '
        '"from": "f", "run_id": "r", "files": [{"name": "n", "size": 1, "path": "p"}]}',
        # File path as null (JSON null) with everything else valid.
        '{"source": "s", "message_id": "m", "internal_date": "d", "subject": "s", '
        '"from": "f", "run_id": "r", "files": [{"name": "n", "size": 1, "path": null}]}',
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


# --- from_s3: the S3-read seam --------------------------------------------


class _FakeReadHook:
    """Duck-typed stand-in: only ``read_key`` is called by ``Manifest.from_s3``.

    ``body`` is returned decoded (like the real ``S3Hook.read_key``); ``exc`` is
    raised instead, to model a hook whose ``read_key`` fails.
    """

    def __init__(self, body: str | None = None, exc: Exception | None = None):
        self.body = body
        self.exc = exc
        self.calls: list[tuple[str, str]] = []

    def read_key(self, key, bucket_name=None) -> str:
        self.calls.append((bucket_name, key))
        if self.exc is not None:
            raise self.exc
        assert self.body is not None
        return self.body


def test_from_s3_parses_valid_utf8_body():
    hook = _FakeReadHook(body=json.dumps(LAYER2_SCHEMA))
    m = Manifest.from_s3(hook, "reports", "gmail/avito/_manifest.json")
    assert m == _sample_manifest()
    # duck-typed: called read_key once with (bucket_name, key)
    assert hook.calls == [("reports", "gmail/avito/_manifest.json")]


def test_from_s3_non_utf8_body_raises_manifest_error():
    # The real S3Hook.read_key decodes UTF-8 before parsing; a non-UTF-8 body
    # surfaces as UnicodeDecodeError, which from_s3 wraps into ManifestError so
    # corrupt content matches the local (bytes) path.
    try:
        b"\xff".decode("utf-8")
    except UnicodeDecodeError as exc:
        decode_error = exc
    hook = _FakeReadHook(exc=decode_error)
    with pytest.raises(ManifestError, match="not valid UTF-8") as excinfo:
        Manifest.from_s3(hook, "reports", "gmail/avito/_manifest.json")
    # The neutral message keeps the object context (bucket/key) but must not
    # reintroduce the s3:// literal (S3_URI_SCHEME is the only place it lives).
    # Use a distinctive key that cannot appear incidentally in the template.
    msg = str(excinfo.value)
    assert "reports" in msg
    assert "gmail/avito/_manifest.json" in msg
    assert "s3://" not in msg


def test_from_s3_other_read_error_propagates_unwrapped():
    # A missing object raises before the decode (S3 ClientError-like); the narrow
    # except UnicodeDecodeError must not touch it — it propagates as-is.
    class _NoSuchKey(Exception):
        pass

    err = _NoSuchKey("missing")
    hook = _FakeReadHook(exc=err)
    with pytest.raises(_NoSuchKey):
        Manifest.from_s3(hook, "reports", "k")


def test_from_s3_invalid_json_body_raises_manifest_error():
    # A valid-UTF-8 but non-JSON body still becomes ManifestError via from_json.
    hook = _FakeReadHook(body="{ not json")
    with pytest.raises(ManifestError):
        Manifest.from_s3(hook, "reports", "k")


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
