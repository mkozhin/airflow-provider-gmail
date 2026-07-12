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


def test_sanitize_short_name_unchanged():
    # A normal short name is not touched by the length cap.
    assert sanitize_filename("report.xlsx", "attachment_0") == "report.xlsx"


def test_sanitize_long_stem_truncated_extension_preserved():
    # A 300-char stem overflows the 255-byte component limit; it is truncated to
    # <= 255 bytes with the extension preserved so `report.<ext>` stays valid.
    name = "a" * 300 + ".xlsx"
    result = sanitize_filename(name, "attachment_0")
    assert result.endswith(".xlsx")
    assert len(result.encode("utf-8")) <= 255
    # The stem is truncated, not the whole thing dropped.
    assert result.startswith("a")
    assert len(result) < len(name)


def test_sanitize_multibyte_name_truncated_on_char_boundary():
    # Many Cyrillic chars (2 bytes each in UTF-8) overflow 255 bytes; truncation
    # must land on a character boundary — the result is still valid UTF-8.
    name = "я" * 300 + ".pdf"
    result = sanitize_filename(name, "attachment_0")
    assert result.endswith(".pdf")
    assert len(result.encode("utf-8")) <= 255
    # Valid UTF-8, no split multibyte char (round-trips cleanly).
    assert result == result.encode("utf-8").decode("utf-8")
    assert set(result[:-4]) == {"я"}


def test_sanitize_absurd_extension_capped_whole():
    # If the extension itself blows the budget, the whole name is capped.
    name = "x." + "e" * 400
    result = sanitize_filename(name, "attachment_0")
    assert len(result.encode("utf-8")) <= 255


def test_sanitize_long_extensionless_name_not_dropped_to_empty():
    # Regression: a long name with NO extension (130 Cyrillic chars = 260 bytes,
    # over the 247-byte budget) used to split with rpartition(".") into an empty
    # stem and cap down to "" — silent data loss. os.path.splitext keeps the
    # whole name as the stem, so the truncated result is non-empty.
    name = "а" * 130
    result = sanitize_filename(name, "attachment_1")
    assert result != ""
    assert result != "attachment_1"  # a real (truncated) name, not the fallback
    assert len(result.encode("utf-8")) <= 247
    assert set(result) == {"а"}


def test_sanitize_long_name_with_extension_keeps_extension():
    # A long name with an extension keeps that extension after truncation.
    name = "b" * 300 + ".csv"
    result = sanitize_filename(name, "attachment_0")
    assert result.endswith(".csv")
    assert len(result.encode("utf-8")) <= 255
    assert result.startswith("b")


def test_sanitize_dotfile_passes_through_undistorted():
    # A short dotfile (leading dot, no extension) is under the cap and is not
    # split into a bare extension — it passes through unchanged.
    assert sanitize_filename(".bashrc", "attachment_0") == ".bashrc"


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


def test_forwarded_eml_delivered_as_single_attachment():
    # A message/rfc822 part with a filename + body source is one .eml
    # attachment; the inner message's own attachment (inner.xlsx) must NOT leak
    # out under the outer message's id.
    result = list(iter_attachments(payload_of("forwarded_eml.json")))
    assert [a.filename for a in result] == ["forwarded.eml"]
    assert result[0].mime_type == "message/rfc822"
    assert result[0].attachment_id == "ANGjdJ_forwarded_eml"
    assert "inner.xlsx" not in [a.filename for a in result]


def test_rfc822_without_body_source_still_recurses():
    # Boundary: an rfc822 part with a filename but NO body source (only parts,
    # no attachmentId/data) is not an attachment, so the walk recurses into it
    # and yields the inner attachment.
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "mimeType": "message/rfc822",
                "filename": "forwarded.eml",
                "parts": [
                    {
                        "mimeType": "application/pdf",
                        "filename": "inner.pdf",
                        "body": {"attachmentId": "id-inner"},
                    }
                ],
            }
        ],
    }
    assert [a.filename for a in iter_attachments(payload)] == ["inner.pdf"]


def test_nested_mime_tree_behavior_unchanged():
    # Ordinary multipart containers (no filename) still recurse as before.
    result = list(iter_attachments(payload_of("nested_mime.json")))
    assert [a.filename for a in result] == ["deep_report.pdf"]


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


def test_empty_attachment_id_without_data_is_not_attachment():
    # An empty-string attachmentId with no data key points at nothing — it is
    # not a body source, so the part is not yielded.
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "mimeType": "application/pdf",
                "filename": "ghost.pdf",
                "body": {"attachmentId": ""},
            }
        ],
    }
    assert list(iter_attachments(payload)) == []


def test_empty_attachment_id_with_data_is_attachment_from_data():
    # attachmentId == "" is not a source, but a present data key is: the part is
    # an attachment whose body comes from data.
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "mimeType": "text/csv",
                "filename": "small.csv",
                "body": {
                    "attachmentId": "",
                    "data": base64.urlsafe_b64encode(b"a,b,c").decode(),
                },
            }
        ],
    }
    result = list(iter_attachments(payload))
    assert len(result) == 1
    att = result[0]
    assert att.filename == "small.csv"
    assert att.attachment_id == ""
    assert att.data == base64.urlsafe_b64encode(b"a,b,c").decode()


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
    # inline; filename="report.pdf" but not an image -> kept.
    assert [a.filename for a in result] == ["report.pdf"]


def test_inline_pdf_with_content_id_remains_attachment():
    # Gateways / Apple Mail stamp Content-ID onto genuine PDF/xlsx parts.
    # A Content-ID must NOT demote a non-image part to an inline image.
    payload = {
        "mimeType": "multipart/related",
        "parts": [
            {
                "mimeType": "application/pdf",
                "filename": "report.pdf",
                "headers": [
                    {"name": "Content-Disposition", "value": "inline"},
                    {"name": "Content-ID", "value": "<report@x>"},
                ],
                "body": {"attachmentId": "id-report"},
            }
        ],
    }
    assert [a.filename for a in iter_attachments(payload)] == ["report.pdf"]


def test_inline_image_with_content_id_still_dropped():
    payload = {
        "mimeType": "multipart/related",
        "parts": [
            {
                "mimeType": "image/png",
                "filename": "logo.png",
                "headers": [
                    {"name": "Content-Disposition", "value": "inline"},
                    {"name": "Content-ID", "value": "<logo@x>"},
                ],
                "body": {"attachmentId": "id-logo"},
            }
        ],
    }
    assert list(iter_attachments(payload)) == []


def test_inline_image_without_content_id_still_dropped():
    payload = {
        "mimeType": "multipart/related",
        "parts": [
            {
                "mimeType": "image/gif",
                "filename": "spacer.gif",
                "headers": [
                    {"name": "Content-Disposition", "value": "inline"},
                ],
                "body": {"attachmentId": "id-spacer"},
            }
        ],
    }
    assert list(iter_attachments(payload)) == []


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
