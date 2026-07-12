"""Tests for MIME parsing helpers (Task 2)."""

from __future__ import annotations

import base64
import json
import pathlib
from email.header import Header

import pytest

from airflow_provider_gmail.utils.mime import (
    Attachment,
    decode_header_value,
    header,
    iter_attachments,
    sanitize_filename,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "gmail"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def payload_of(name: str) -> dict:
    return load(name)["payload"]


def rfc2047_b(text: str) -> str:
    return Header(text, "utf-8").encode()


# --------------------------------------------------------------------------
# decode_header_value
# --------------------------------------------------------------------------


def test_decode_header_ascii():
    assert decode_header_value("Weekly report") == "Weekly report"


def test_decode_header_cyrillic_base64():
    encoded = rfc2047_b("Отчёт за 09.07")
    assert "=?" in encoded  # sanity: it really is an encoded word
    assert decode_header_value(encoded) == "Отчёт за 09.07"


def test_decode_header_cyrillic_quoted_printable():
    # =?UTF-8?Q?...?= quoted-printable encoding of a cyrillic word.
    qp = "=?UTF-8?Q?=D0=9E=D1=82=D1=87=D1=91=D1=82?="
    assert decode_header_value(qp) == "Отчёт"


def test_decode_header_multiple_encoded_words():
    first = rfc2047_b("Отчёт")
    second = rfc2047_b("июль")
    # Two adjacent encoded words should concatenate into one string.
    assert decode_header_value(f"{first} {second}") == "Отчётиюль"


def test_decode_header_invalid_charset_does_not_raise():
    # Unknown charset "x-unknown" must not raise; bytes fall back to UTF-8.
    raw = "=?x-unknown?B?SGVsbG8=?="
    result = decode_header_value(raw)
    assert result == "Hello"


# --------------------------------------------------------------------------
# sanitize_filename
# --------------------------------------------------------------------------


def test_sanitize_path_traversal():
    assert sanitize_filename("../../etc/passwd", "attachment_0") == "passwd"


def test_sanitize_takes_basename():
    assert sanitize_filename("a/b.xlsx", "attachment_0") == "b.xlsx"


def test_sanitize_backslash_basename():
    assert sanitize_filename("a\\b\\c.xlsx", "attachment_0") == "c.xlsx"


def test_sanitize_dots_only_falls_back():
    assert sanitize_filename("...", "attachment_3") == "attachment_3"
    assert sanitize_filename("..", "attachment_3") == "attachment_3"


def test_sanitize_null_byte_removed():
    assert sanitize_filename("re\x00port.xlsx", "attachment_0") == "report.xlsx"


def test_sanitize_control_chars_removed():
    assert sanitize_filename("re\tport\n.csv", "attachment_0") == "report.csv"


def test_sanitize_empty_falls_back():
    assert sanitize_filename("", "attachment_7") == "attachment_7"


def test_sanitize_decodes_encoded_word_defensively():
    encoded = rfc2047_b("отчёт.xlsx")
    assert sanitize_filename(encoded, "attachment_0") == "отчёт.xlsx"


# --------------------------------------------------------------------------
# iter_attachments
# --------------------------------------------------------------------------


def test_flat_multipart_mixed():
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {"mimeType": "text/plain", "filename": "", "body": {"data": "aGk="}},
            {
                "mimeType": "application/pdf",
                "filename": "a.pdf",
                "body": {"attachmentId": "id-a"},
            },
            {
                "mimeType": "text/csv",
                "filename": "b.csv",
                "body": {"attachmentId": "id-b"},
            },
        ],
    }
    result = list(iter_attachments(payload))
    assert [a.filename for a in result] == ["a.pdf", "b.csv"]
    assert all(isinstance(a, Attachment) for a in result)


def test_nested_mime_tree():
    result = list(iter_attachments(payload_of("nested_mime.json")))
    assert [a.filename for a in result] == ["deep_report.pdf"]
    assert result[0].attachment_id == "ANGjdJ_nested_pdf"
    assert result[0].data is None


def test_message_without_parts():
    payload = {
        "mimeType": "text/plain",
        "filename": "",
        "body": {"data": "aGVsbG8="},
    }
    assert list(iter_attachments(payload)) == []


def test_attachment_via_body_data_without_attachment_id():
    result = list(iter_attachments(payload_of("body_data.json")))
    assert len(result) == 1
    att = result[0]
    assert att.filename == "small.csv"
    assert att.attachment_id is None
    assert att.data == base64.urlsafe_b64encode(b"a,b,c").decode()


def test_empty_file_recognized_as_attachment():
    result = list(iter_attachments(payload_of("empty_file.json")))
    assert len(result) == 1
    att = result[0]
    assert att.filename == "empty.dat"
    # Key presence, not truthiness: an empty file gives data == "".
    assert att.data == ""
    assert att.attachment_id is None


def test_part_without_any_body_source_is_not_attachment():
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "mimeType": "application/pdf",
                "filename": "ghost.pdf",
                "body": {"size": 10},
            }
        ],
    }
    assert list(iter_attachments(payload)) == []


# --------------------------------------------------------------------------
# inline filtering
# --------------------------------------------------------------------------


def test_inline_logo_filtered_but_real_attachment_kept():
    result = list(iter_attachments(payload_of("inline_image.json")))
    # The inline logo (image/png + Content-ID) is dropped; the xlsx stays.
    assert [a.filename for a in result] == ["real.xlsx"]


def test_inline_with_params_and_capitalization_filtered():
    payload = {
        "mimeType": "multipart/related",
        "parts": [
            {
                "mimeType": "image/png",
                "filename": "logo.png",
                "headers": [
                    {
                        "name": "content-disposition",
                        "value": 'Inline; filename="logo.png"',
                    }
                ],
                "body": {"attachmentId": "id-logo"},
            }
        ],
    }
    assert list(iter_attachments(payload)) == []


def test_inline_image_by_content_id_without_image_mime_still_needs_inline():
    # Not inline (attachment disposition) even though Content-ID present -> kept.
    payload = {
        "mimeType": "multipart/related",
        "parts": [
            {
                "mimeType": "image/png",
                "filename": "chart.png",
                "headers": [
                    {"name": "Content-Disposition", "value": "attachment"},
                    {"name": "Content-ID", "value": "<chart@x>"},
                ],
                "body": {"attachmentId": "id-chart"},
            }
        ],
    }
    assert [a.filename for a in iter_attachments(payload)] == ["chart.png"]


def test_inline_pdf_remains_attachment():
    result = list(iter_attachments(payload_of("inline_pdf.json")))
    # inline; filename="report.pdf" but not an image and no Content-ID -> kept.
    assert [a.filename for a in result] == ["report.pdf"]


# --------------------------------------------------------------------------
# header()
# --------------------------------------------------------------------------


def test_header_cyrillic_subject():
    payload = payload_of("cyrillic_filename.json")
    assert header(payload, "Subject") == "Отчёт за 09.07"


def test_header_missing_returns_none():
    payload = {"headers": [{"name": "Subject", "value": "x"}]}
    assert header(payload, "Cc") is None


def test_header_name_case_insensitive():
    payload = {"headers": [{"name": "Subject", "value": "hello"}]}
    assert header(payload, "subject") == "hello"
    assert header(payload, "SUBJECT") == "hello"


def test_header_absent_headers_key():
    assert header({}, "Subject") is None


def test_cyrillic_filename_attachment_and_sanitize():
    payload = payload_of("cyrillic_filename.json")
    result = list(iter_attachments(payload))
    assert [a.filename for a in result] == ["Отчёт за июль.xlsx"]
    assert sanitize_filename(result[0].filename, "attachment_0") == "Отчёт за июль.xlsx"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
