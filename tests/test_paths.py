"""Tests for :mod:`airflow_provider_gmail.utils.paths` — the pure S3-key join.

Covers ``prefix`` normalization (empty, plain, trailing-slash, doubled slash)
and the consistency of the attachment key with :func:`manifest_key`: the
manifest must sit next to the message's files, since that is the exact key
contract the storage-aware S3 sensor (Task 12) relies on. The attachment key is
built the way the operator builds it — ``join_key(message_dir(...), filename)``,
which is the same as the operator's ``join_key(prefix, rel_path)``.
"""

from __future__ import annotations

import pytest

from airflow_provider_gmail.utils.paths import (
    S3_MAX_KEY_BYTES,
    is_s3_uri,
    join_key,
    manifest_key,
    message_dir,
    s3_key,
    s3_uri,
    split_s3_uri,
)

DT = "2026-07-12"
MSG = "18c2f4a9b3d5e6f7"


def _file_key(prefix: str, filename: str) -> str:
    return join_key(message_dir(prefix, DT, MSG), filename)


# -- prefix normalization ----------------------------------------------------


@pytest.mark.parametrize(
    "prefix,expected",
    [
        ("", f"dt={DT}/{MSG}/report.xlsx"),
        ("gmail/avito", f"gmail/avito/dt={DT}/{MSG}/report.xlsx"),
        ("gmail/avito/", f"gmail/avito/dt={DT}/{MSG}/report.xlsx"),
        ("/gmail/avito", f"gmail/avito/dt={DT}/{MSG}/report.xlsx"),
        ("gmail//avito", f"gmail/avito/dt={DT}/{MSG}/report.xlsx"),
    ],
)
def test_file_key_prefix_normalization(prefix, expected):
    assert _file_key(prefix, "report.xlsx") == expected


def test_empty_prefix_key_has_no_leading_slash():
    key = _file_key("", "report.xlsx")
    assert not key.startswith("/")


def test_no_key_has_double_slash():
    for prefix in ("", "gmail/avito", "gmail/avito/", "/gmail//avito/"):
        assert "//" not in _file_key(prefix, "report.xlsx")
        assert "//" not in manifest_key(prefix, DT, MSG)


# -- file key / manifest_key consistency -------------------------------------


def test_manifest_sits_next_to_files():
    # The manifest key must share the message directory with the file key.
    file_key = _file_key("gmail/avito", "report.xlsx")
    mkey = manifest_key("gmail/avito", DT, MSG)
    assert file_key.rsplit("/", 1)[0] == mkey.rsplit("/", 1)[0]
    assert mkey.endswith("/_manifest.json")


def test_manifest_key_matches_message_dir():
    assert manifest_key("gmail/avito", DT, MSG) == (
        f"{message_dir('gmail/avito', DT, MSG)}/_manifest.json"
    )


def test_manifest_key_empty_prefix_no_leading_slash():
    assert not manifest_key("", DT, MSG).startswith("/")


def test_operator_rel_dir_layout_matches_old_literal():
    # The operator builds its rel_dir through message_dir with an empty prefix;
    # the result must be byte-identical to the historical f"dt={dt}/{message_id}".
    rel_dir = message_dir("", DT, MSG)
    assert rel_dir == f"dt={DT}/{MSG}"


def test_operator_manifest_key_matches_sensor_lookup():
    # "Operator writes where sensor looks": the S3 operator keys its manifest as
    # s3_key(prefix, rel_dir, MANIFEST_FILENAME) where rel_dir = message_dir("",…),
    # while the S3 sensor locates it via manifest_key(prefix, dt, message_id).
    # Both must resolve to the same object key for the same (dt, message_id).
    prefix = "gmail/avito"
    rel_dir = message_dir("", DT, MSG)
    operator_key = s3_key(prefix, rel_dir, "_manifest.json")
    assert operator_key == manifest_key(prefix, DT, MSG)


# -- s3_key: 1024-byte whole-key guard ---------------------------------------


def test_s3_key_within_limit_joins_normally():
    # A normal key equals a plain join and stays well under the limit.
    key = s3_key("gmail/avito", f"dt={DT}/{MSG}/report.xlsx")
    assert key == f"gmail/avito/dt={DT}/{MSG}/report.xlsx"
    assert len(key.encode("utf-8")) <= S3_MAX_KEY_BYTES


def test_s3_key_at_limit_is_allowed():
    # Exactly 1024 bytes is fine; only *over* the limit fails.
    filename = "a" * (S3_MAX_KEY_BYTES - len("p/"))
    key = s3_key("p", filename)
    assert len(key.encode("utf-8")) == S3_MAX_KEY_BYTES


def test_s3_key_over_limit_raises_clear_error():
    # A pathologically long prefix pushes a component-legal filename over the
    # whole-key limit — fail fast with an actionable message, not a silent stall.
    huge_prefix = "p" * 1100
    with pytest.raises(ValueError) as exc:
        s3_key(huge_prefix, f"dt={DT}/{MSG}/report.xlsx")
    msg = str(exc.value)
    assert str(S3_MAX_KEY_BYTES) in msg
    assert "prefix" in msg.lower()


def test_s3_key_collision_suffix_tips_over_limit():
    # A key that fits at 1023 bytes but whose `_N` collision suffix tips it to
    # 1025 is caught here (the suffix is already baked into the filename).
    prefix = "p" * 500
    # Build a filename so the un-suffixed key is <= 1024 but the suffixed is not.
    base_len = S3_MAX_KEY_BYTES - len(prefix) - len("/") - len(".xlsx")
    stem = "a" * base_len
    ok_key = s3_key(prefix, f"{stem}.xlsx")
    assert len(ok_key.encode("utf-8")) <= S3_MAX_KEY_BYTES
    with pytest.raises(ValueError):
        s3_key(prefix, f"{stem}_1.xlsx")


# -- s3_uri / split_s3_uri round-trip ----------------------------------------


@pytest.mark.parametrize(
    "bucket,key",
    [
        ("bucket", "gmail/avito/dt=2026-07-12/abc/report.xlsx"),
        ("my-bucket", "prefix/file with spaces.xlsx"),
        ("bucket", "prefix/Отчёт за июль.xlsx"),
        # Defense-in-depth: old/foreign keys may carry ? # % — the round-trip
        # must preserve them verbatim (direct split, no urlsplit).
        ("bucket", "prefix/weird?name#1%20.xlsx"),
    ],
)
def test_s3_uri_split_round_trip(bucket, key):
    uri = s3_uri(bucket, key)
    assert uri.startswith("s3://")
    assert split_s3_uri(uri) == (bucket, key)


def test_s3_uri_composes_multiple_segments():
    uri = s3_uri("bucket", "gmail/avito", f"dt={DT}/{MSG}", "report.xlsx")
    assert uri == f"s3://bucket/gmail/avito/dt={DT}/{MSG}/report.xlsx"


def test_s3_uri_normalizes_unnormalized_key():
    # A key with a doubled slash / leading slash goes into the URI in its
    # normalized form (a//b -> a/b, /a -> a) — behavior pinned explicitly.
    assert s3_uri("bucket", "a//b") == "s3://bucket/a/b"
    assert s3_uri("bucket", "/a") == "s3://bucket/a"
    # And it still round-trips to the *normalized* key.
    assert split_s3_uri(s3_uri("bucket", "a//b")) == ("bucket", "a/b")


# -- s3_uri / split_s3_uri error cases (symmetric) ---------------------------


def test_s3_uri_empty_bucket_raises():
    with pytest.raises(ValueError):
        s3_uri("", "key")


def test_s3_uri_empty_key_raises():
    with pytest.raises(ValueError):
        s3_uri("bucket", "")
    # A key of nothing but slashes normalizes to empty -> also rejected.
    with pytest.raises(ValueError):
        s3_uri("bucket", "///")


def test_split_s3_uri_not_s3_raises():
    for bad in ("/absolute/local/path", "http://example.com/x", "report.xlsx"):
        with pytest.raises(ValueError):
            split_s3_uri(bad)


def test_split_s3_uri_empty_bucket_raises():
    with pytest.raises(ValueError):
        split_s3_uri("s3:///key")


def test_split_s3_uri_empty_key_raises():
    with pytest.raises(ValueError):
        split_s3_uri("s3://bucket")
    with pytest.raises(ValueError):
        split_s3_uri("s3://bucket/")


# -- is_s3_uri ---------------------------------------------------------------


def test_is_s3_uri_predicate():
    assert is_s3_uri("s3://bucket/key")
    assert is_s3_uri("s3://bucket/dt=2026/abc/report.xlsx")
    assert not is_s3_uri("/absolute/local/path/report.xlsx")
    assert not is_s3_uri("http://example.com/report.xlsx")
    assert not is_s3_uri("report.xlsx")
