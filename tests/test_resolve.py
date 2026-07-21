"""Tests for :func:`airflow_provider_gmail.resolve._resolve` — the pure core.

Pure: no I/O, no mocks. The core takes ``(manifest_path, Manifest)`` pairs the
caller has already read and returns a flat list of full attachment paths. These
tests build :class:`Manifest` fixtures directly and import ``_resolve`` — the
internal seam — verifying the winner-selection and full-path-assembly semantics
in isolation from the S3/local read edge (:func:`resolve_attachments`, Task 2c).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from airflow_provider_gmail.manifest import FileEntry, Manifest, ManifestError
from airflow_provider_gmail.resolve import _resolve, resolve_attachments

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
    # Unknown pick is rejected regardless of the pairs — including ``None``,
    # which is passed through unchanged so the case genuinely asserts that pick
    # validation precedes any iteration of the pairs (None would raise TypeError
    # if iterated first).
    with pytest.raises(ValueError, match="pick must be one of"):
        _resolve(pairs, "invalid")


# -- URI assembly for spaces / Unicode / defense-in-depth ---------------------


def test_uri_assembly_preserves_spaces_and_unicode():
    key = _s3_key("MSG", "отчёт за июль.xlsx")
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{key}"
    ]


def test_uri_assembly_preserves_old_special_chars_defense_in_depth():
    # Defense in depth: new keys are sanitized (Task 1a), but a foreign/old key
    # carrying ? # % is preserved verbatim when re-anchored — those characters
    # survive s3_key/join_key normalization untouched. Built MANUALLY (raw key),
    # NOT via sanitize_filename — otherwise the test would assert on nothing.
    raw_key = "gmail/avito/dt=2026-07-10/MSG/report?v=1#frag%20.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [raw_key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/{raw_key}"
    ]


def test_s3_reanchor_collapses_double_slash_in_key():
    # The "preserved verbatim" guarantee holds only for NORMALIZED keys: re-anchoring
    # an S3 key routes through s3_uri -> s3_key -> join_key, which collapses a doubled
    # slash and strips edge slashes. A foreign files[].path of "a//b" is therefore
    # rewritten to the normalized "a/b" (a DIFFERENT object) — behavior pinned here
    # so the divergence is documented, not silently assumed away.
    raw_key = "gmail/avito/dt=2026-07-10/MSG//report.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [raw_key])
    assert _resolve([(_s3_manifest_path("MSG"), manifest)], "all") == [
        f"s3://{BUCKET}/gmail/avito/dt=2026-07-10/MSG/report.xlsx"
    ]


# =============================================================================
# resolve_attachments — the public I/O edge (Task 2c). Mocks S3Hook; local reads
# hit real files under tmp_path. The pure semantics live in the _resolve tests
# above, so these focus on reading, the hook lifecycle, and error propagation.
# =============================================================================


@pytest.fixture
def fake_s3(monkeypatch):
    """Patch the lazily-imported ``S3Hook`` with an in-memory fake.

    ``store`` is keyed by ``(bucket, key)`` → ``str`` body (``read_key`` returns a
    decoded ``str``, like the real hook). ``instances`` records every constructed
    hook so a test can assert exactly one hook per call and its ``aws_conn_id``.
    A per-key ``errors`` map lets a test make ``read_key`` raise (missing object).
    """
    import airflow.providers.amazon.aws.hooks.s3 as s3mod

    store: dict[tuple[str, str], str] = {}
    errors: dict[tuple[str, str], Exception] = {}
    instances: list = []

    class _FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            self.aws_conn_id = aws_conn_id
            self.read_calls: list[tuple[str, str]] = []
            instances.append(self)

        def read_key(self, key, bucket_name=None) -> str:
            self.read_calls.append((bucket_name, key))
            if (bucket_name, key) in errors:
                raise errors[(bucket_name, key)]
            return store[(bucket_name, key)]

    monkeypatch.setattr(s3mod, "S3Hook", _FakeS3Hook)
    return SimpleNamespace(store=store, errors=errors, instances=instances)


def _put(fake, msg: str, manifest: Manifest, dt: str = "2026-07-10") -> str:
    """Store a manifest body in the fake S3 and return its full URI."""
    uri = _s3_manifest_path(msg, dt)
    bucket, key = uri[len("s3://") :].split("/", 1)
    fake.store[(bucket, key)] = manifest.to_json().decode("utf-8")
    return uri


# -- s3 reading ---------------------------------------------------------------


def test_reads_s3_manifest_and_reanchors(fake_s3):
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [_s3_key("MSG", "report.xlsx")])
    uri = _put(fake_s3, "MSG", manifest)
    assert resolve_attachments([uri], "all") == [
        f"s3://{BUCKET}/{_s3_key('MSG', 'report.xlsx')}"
    ]


def test_one_hook_reused_across_s3_uris_with_aws_conn_id(fake_s3):
    m1 = _mk("A", "2026-07-10T09:00:00+03:00", [_s3_key("A", "a.xlsx")])
    m2 = _mk("B", "2026-07-10T10:00:00+03:00", [_s3_key("B", "b.xlsx")])
    u1, u2 = _put(fake_s3, "A", m1), _put(fake_s3, "B", m2)

    result = resolve_attachments([u1, u2], "all", aws_conn_id="reports_s3")

    assert result == [
        f"s3://{BUCKET}/{_s3_key('A', 'a.xlsx')}",
        f"s3://{BUCKET}/{_s3_key('B', 'b.xlsx')}",
    ]
    # Exactly one hook created for the whole call, bound to the given conn id,
    # and it served both reads.
    assert len(fake_s3.instances) == 1
    assert fake_s3.instances[0].aws_conn_id == "reports_s3"
    assert len(fake_s3.instances[0].read_calls) == 2


# -- local reading ------------------------------------------------------------


def test_reads_local_manifest_verbatim(fake_s3, tmp_path):
    key = "/data/gmail/avito/dt=2026-07-10/MSG/report.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [key])
    path = tmp_path / "_manifest.json"
    path.write_bytes(manifest.to_json())

    assert resolve_attachments([str(path)], "all") == [key]
    # A purely-local input must never construct an S3Hook (Amazon provider unused).
    assert fake_s3.instances == []


def test_local_input_does_not_construct_s3_hook(fake_s3, tmp_path):
    # Even the lazy S3Hook import path must not run for local-only input.
    manifest = _mk("M", "2026-07-10T09:00:00+03:00", ["/data/M/x.xlsx"])
    path = tmp_path / "_manifest.json"
    path.write_bytes(manifest.to_json())
    resolve_attachments([str(path)], "latest")
    assert fake_s3.instances == []


# -- error propagation --------------------------------------------------------


def test_broken_s3_manifest_raises_manifest_error(fake_s3):
    uri = _s3_manifest_path("MSG")
    bucket, key = uri[len("s3://") :].split("/", 1)
    fake_s3.store[(bucket, key)] = "{ not json"
    with pytest.raises(ManifestError):
        resolve_attachments([uri], "all")


def test_broken_local_manifest_raises_manifest_error(fake_s3, tmp_path):
    path = tmp_path / "_manifest.json"
    path.write_bytes(b"{ not json")
    with pytest.raises(ManifestError):
        resolve_attachments([str(path)], "all")


def test_missing_s3_manifest_propagates_without_wrapping(fake_s3):
    # No check_for_key pre-check: read_key raises and the error surfaces as-is.
    from botocore.exceptions import ClientError

    uri = _s3_manifest_path("GONE")
    bucket, key = uri[len("s3://") :].split("/", 1)
    err = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
    )
    fake_s3.errors[(bucket, key)] = err
    with pytest.raises(ClientError):
        resolve_attachments([uri], "all")


def test_missing_local_manifest_raises_file_not_found(fake_s3, tmp_path):
    missing = tmp_path / "nope" / "_manifest.json"
    with pytest.raises(FileNotFoundError):
        resolve_attachments([str(missing)], "all")


# -- empty / None inputs ------------------------------------------------------


def test_empty_list_returns_empty(fake_s3):
    assert resolve_attachments([], "all") == []
    assert resolve_attachments([], "latest") == []
    assert fake_s3.instances == []  # no I/O at all


def test_none_input_raises_type_error_not_masked(fake_s3):
    # None (e.g. an xcom_pull on a wrong task_id) must NOT be swallowed into [].
    with pytest.raises(TypeError):
        resolve_attachments(None, "all")


# -- pick validated before any I/O --------------------------------------------


def test_unknown_pick_raises_before_touching_storage(fake_s3):
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [_s3_key("MSG", "x.xlsx")])
    uri = _put(fake_s3, "MSG", manifest)
    with pytest.raises(ValueError, match="pick must be one of"):
        resolve_attachments([uri], "invalid")
    # The manifest was never read — validation is the first line.
    assert fake_s3.instances == []


def test_unknown_pick_on_empty_list_raises_not_empty(fake_s3):
    with pytest.raises(ValueError, match="pick must be one of"):
        resolve_attachments([], "invalid")


# -- special-char key end-to-end (defense in depth) ---------------------------


def test_special_char_key_round_trip_through_s3_read(fake_s3):
    raw_key = "gmail/avito/dt=2026-07-10/MSG/report?v=1#frag%20 space.xlsx"
    manifest = _mk("MSG", "2026-07-10T09:00:00+03:00", [raw_key])
    uri = _put(fake_s3, "MSG", manifest)
    assert resolve_attachments([uri], "all") == [f"s3://{BUCKET}/{raw_key}"]


# -- mixed S3 + local manifests in one call -----------------------------------


def test_mixed_s3_and_local_manifests_resolve_in_one_call(fake_s3, tmp_path):
    # An interleaved list (S3 URI, local path, S3 URI): S3 entries read through a
    # single lazily-created hook, local entries read straight off disk.
    m_s3a = _mk("A", "2026-07-10T09:00:00+03:00", [_s3_key("A", "a.xlsx")])
    m_s3b = _mk("B", "2026-07-10T10:00:00+03:00", [_s3_key("B", "b.xlsx")])
    u_a = _put(fake_s3, "A", m_s3a)
    u_b = _put(fake_s3, "B", m_s3b)

    local_key = "/data/gmail/avito/dt=2026-07-10/LOC/local.xlsx"
    m_local = _mk("LOC", "2026-07-10T09:30:00+03:00", [local_key])
    local_path = tmp_path / "_manifest.json"
    local_path.write_bytes(m_local.to_json())

    result = resolve_attachments([u_a, str(local_path), u_b], "all")

    assert result == [
        f"s3://{BUCKET}/{_s3_key('A', 'a.xlsx')}",
        local_key,
        f"s3://{BUCKET}/{_s3_key('B', 'b.xlsx')}",
    ]
    # Exactly one S3Hook for the whole call — created for the S3 entries, and it
    # served both S3 reads (the local entry never touched it).
    assert len(fake_s3.instances) == 1
    assert len(fake_s3.instances[0].read_calls) == 2


# -- public edge: pick="latest" winner after real reads -----------------------


def test_latest_selects_winner_through_public_read_edge(fake_s3):
    # End-to-end: multiple S3 manifests read via the mocked hook, then pick="latest"
    # selects the winner by internal_date. The early manifest's ISO text sorts AFTER
    # the late one lexicographically, so this also pins chronological-not-lexicographic
    # ordering driven through from_local_iso at the I/O edge.
    m_early = _mk("EARLY", "2026-07-10T09:00:00+03:00", [_s3_key("EARLY", "e.xlsx")])
    m_late = _mk("LATE", "2026-07-10T07:00:00+00:00", [_s3_key("LATE", "l.xlsx")])
    assert "2026-07-10T09:00:00+03:00" > "2026-07-10T07:00:00+00:00"  # lexicographic
    u_early = _put(fake_s3, "EARLY", m_early)
    u_late = _put(fake_s3, "LATE", m_late)

    result = resolve_attachments([u_early, u_late], "latest")

    assert result == [f"s3://{BUCKET}/{_s3_key('LATE', 'l.xlsx')}"]
