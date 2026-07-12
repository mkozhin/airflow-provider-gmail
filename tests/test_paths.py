"""Tests for :mod:`airflow_provider_gmail.utils.paths` — the pure S3-key join.

Covers ``prefix`` normalization (empty, plain, trailing-slash, doubled slash)
and the consistency of :func:`s3_object_key` with :func:`manifest_key`: the
manifest must sit next to the message's files, since that is the exact key
contract the storage-aware S3 sensor (Task 12) relies on.
"""

from __future__ import annotations

import pytest

from airflow_provider_gmail.utils.paths import (
    manifest_key,
    message_dir,
    s3_object_key,
)

DT = "2026-07-12"
MSG = "18c2f4a9b3d5e6f7"


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
def test_s3_object_key_prefix_normalization(prefix, expected):
    assert s3_object_key(prefix, DT, MSG, "report.xlsx") == expected


def test_empty_prefix_key_has_no_leading_slash():
    key = s3_object_key("", DT, MSG, "report.xlsx")
    assert not key.startswith("/")


def test_no_key_has_double_slash():
    for prefix in ("", "gmail/avito", "gmail/avito/", "/gmail//avito/"):
        assert "//" not in s3_object_key(prefix, DT, MSG, "report.xlsx")
        assert "//" not in manifest_key(prefix, DT, MSG)


# -- s3_object_key / manifest_key consistency --------------------------------


def test_manifest_sits_next_to_files():
    # The manifest key must share the message directory with the file key.
    file_key = s3_object_key("gmail/avito", DT, MSG, "report.xlsx")
    mkey = manifest_key("gmail/avito", DT, MSG)
    assert file_key.rsplit("/", 1)[0] == mkey.rsplit("/", 1)[0]
    assert mkey.endswith("/_manifest.json")


def test_manifest_key_matches_message_dir():
    assert manifest_key("gmail/avito", DT, MSG) == (
        f"{message_dir('gmail/avito', DT, MSG)}/_manifest.json"
    )


def test_manifest_key_empty_prefix_no_leading_slash():
    assert not manifest_key("", DT, MSG).startswith("/")
