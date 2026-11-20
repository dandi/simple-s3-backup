import hashlib
import pathlib

import pytest
from click.testing import CliRunner

from simple_s3_backup._base import _checksums
from simple_s3_backup._base._dandi import _blob_subdirectory_is_fully_copied
from simple_s3_backup._base._display import _get_local_size_in_bytes_and_object_count
from simple_s3_backup._command_line_interface._cli import _s3backup

MIB = 2**20


def _etag_of_parts(*parts: bytes) -> str:
    return f"{hashlib.md5(b''.join(hashlib.md5(part).digest() for part in parts)).hexdigest()}-{len(parts)}"


@pytest.mark.parametrize(
    ("file_size", "expected_count", "expected_part_size"),
    [
        (0, 0, None),
        (1, 1, 1),
        (64 * MIB, 1, 64 * MIB),
        (64 * MIB + 1, 2, 64 * MIB),
        (10_000 * 64 * MIB, 10_000, 64 * MIB),
        (10_000 * 64 * MIB + 1, 10_000, 64 * MIB + 1),
    ],
)
def test_part_sizes(file_size, expected_count, expected_part_size):
    parts = _checksums._part_sizes(file_size)
    assert sum(parts) == file_size
    assert len(parts) == expected_count
    assert (parts[0] if parts else None) == expected_part_size


def test_compute_etag(tmp_path, monkeypatch):
    monkeypatch.setattr(_checksums, "_DEFAULT_PART_SIZE", 4)
    monkeypatch.setattr(_checksums, "_READ_SIZE", 3)  # Reads that cross the parts' boundaries
    file_path = tmp_path / "blob"
    file_path.write_bytes(b"abcdefghij")

    assert _checksums.compute_etag(file_path) == _etag_of_parts(b"abcd", b"efgh", b"ij")
    assert _checksums.compute_etag(file_path, multipart=False) == hashlib.md5(b"abcdefghij").hexdigest()


def test_is_checksum_cache():
    assert _checksums.is_checksum_cache(".0123-abcd.checksum")
    assert _checksums.is_checksum_cache(pathlib.Path("blobs/012/3ab/.0123-abcd.checksum.partial"))
    assert not _checksums.is_checksum_cache("0123-abcd")
    assert not _checksums.is_checksum_cache("0123-abcd.checksum")


@pytest.fixture
def mirror(tmp_path, monkeypatch):
    """A mirror of head 0 with one blob of each kind, and the bucket listing it is checked against."""
    monkeypatch.setattr(_checksums, "_MIRROR_ROOT", tmp_path)
    monkeypatch.setattr(_checksums, "LEDGER_PATH", tmp_path / "checksum_mismatches.tsv")
    monkeypatch.setattr(_checksums, "RECENT_SECONDS", 0)
    blobs_directory = tmp_path / "001" / "s3dandiarchive" / "blobs"

    contents = {
        "blobs/012/345/01234567-good": b"the right content",
        "blobs/012/345/01234567-corrupt": b"the right content",
        "blobs/0ab/cde/0abcdef0-short": b"the right content",
        "blobs/0ab/cde/0abcdef0-missing": b"the right content",
    }
    local = {
        "blobs/012/345/01234567-good": b"the right content",
        "blobs/012/345/01234567-corrupt": b"the wrong content",
        "blobs/0ab/cde/0abcdef0-short": b"the right",
        "blobs/0ab/cde/0abcdef0-deleted": b"gone from the bucket",
    }
    for key, data in local.items():
        path = blobs_directory / key.removeprefix("blobs/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    listing = [_checksums.S3Object(key, len(data), _etag_of_parts(data)) for key, data in contents.items()]
    monkeypatch.setattr(
        _checksums, "list_s3_objects", lambda prefix: [item for item in listing if item.key.startswith(prefix)]
    )
    return blobs_directory, contents


def test_verify(mirror, capsys):
    blobs_directory, _ = mirror

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.verified == 1
    assert counts.hashed == 2
    assert [mismatch.split(":")[0] for mismatch in counts.mismatched] == [
        "blobs/012/345/01234567-corrupt",
        "blobs/0ab/cde/0abcdef0-short",
    ]
    assert counts.deleted == ["blobs/012/345/01234567-corrupt", "blobs/0ab/cde/0abcdef0-short"]
    assert counts.unrepaired == 0
    assert counts.not_mirrored == 1
    assert counts.not_in_bucket == ["blobs/0ab/cde/0abcdef0-deleted"]

    # The cache holds the ETag alone, stamped with the blob's modification time.
    good = blobs_directory / "012/345/01234567-good"
    cache_path = _checksums.checksum_cache_path(good)
    assert cache_path.read_text() == _etag_of_parts(b"the right content") + "\n"
    assert cache_path.stat().st_mtime_ns == good.stat().st_mtime_ns

    # The second run trusts the cache, until the file changes or the cache is older than allowed. The copies
    # that did not match were deleted, so they are now only not mirrored.
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified, counts.not_mirrored) == (0, 1, 1, 3)
    good.touch()
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 0, 1)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=0)
    assert (counts.hashed, counts.reused) == (1, 0)
    cache_path.write_text("not a checksum\n")
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 0, 1)

    assert "[MISMATCH] blobs/012/345/01234567-corrupt" in capsys.readouterr().out


def _ledger_events() -> list[tuple[str, str, str, str]]:
    """The ledger's lines after its header, as (event, key, observed, expected)."""
    lines = _checksums.LEDGER_PATH.read_text().splitlines()
    assert lines[0].split("\t") == list(_checksums._LEDGER_COLUMNS)
    return [tuple(fields[i] for i in (1, 2, 4, 5)) for fields in (line.split("\t") for line in lines[1:])]


def test_ledger(mirror):
    blobs_directory, contents = mirror
    corrupt_etag = _etag_of_parts(b"the wrong content")
    right_etag = _etag_of_parts(b"the right content")
    found = [
        ("etag-mismatch", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("deleted", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
        ("deleted", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
    ]

    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _ledger_events() == found

    # Nothing is recorded while the deleted copies have not been copied again.
    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _ledger_events() == found

    # The blob backup copies both again, which a later run records as resolved; should one go wrong again,
    # that is a new mismatch, and its copy is deleted again.
    for key in ("blobs/012/345/01234567-corrupt", "blobs/0ab/cde/0abcdef0-short"):
        (blobs_directory / key.removeprefix("blobs/")).write_bytes(contents[key])
    _checksums.verify_dandi_blob_checksums(task_id=0)
    resolved = [
        ("resolved", "blobs/012/345/01234567-corrupt", right_etag, right_etag),
        ("resolved", "blobs/0ab/cde/0abcdef0-short", right_etag, right_etag),
    ]
    assert _ledger_events() == [*found, *resolved]
    short = blobs_directory / "0ab/cde/0abcdef0-short"
    short.write_bytes(b"the right")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _ledger_events() == [*found, *resolved, *found[2:]]
    assert not short.exists()


def test_ledger_records_which_job(mirror, monkeypatch):
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "1234")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "0")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID")
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    (mirror[0] / "012/345/01234567-good").write_bytes(b"damaged")
    _checksums.verify_dandi_blob_checksums(task_id=0)

    jobs = [line.split("\t")[-1] for line in _checksums.LEDGER_PATH.read_text().splitlines()[1:]]
    assert jobs == ["1234_0"] * 4 + ["manual"] * 2


def test_verify_repairs(mirror, monkeypatch):
    blobs_directory, contents = mirror

    def copy_from_bucket(command, ignore_errors):
        _, _, source, destination = command.split()
        pathlib.Path(destination).write_bytes(contents[source.removeprefix("s3://dandiarchive/")])

    monkeypatch.setattr(_checksums, "_deploy_subprocess", copy_from_bucket)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, repair=True)
    assert counts.repaired == ["blobs/012/345/01234567-corrupt", "blobs/0ab/cde/0abcdef0-short"]
    assert counts.unrepaired == 0

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.verified == 3
    assert not counts.mismatched

    assert [event[:2] for event in _ledger_events()] == [
        ("etag-mismatch", "blobs/012/345/01234567-corrupt"),
        ("repaired", "blobs/012/345/01234567-corrupt"),
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short"),
        ("repaired", "blobs/0ab/cde/0abcdef0-short"),
    ]


def test_verify_leaves_recent_files(mirror, monkeypatch):
    monkeypatch.setattr(_checksums, "RECENT_SECONDS", 3600)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.recent == 3
    assert counts.verified == 0
    assert not counts.mismatched


def test_caches_are_not_counted_as_objects(mirror):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _checksums.checksum_cache_path(blobs_directory / "012/345/01234567-good").exists()

    # The copies that did not match were deleted, leaving the good one and the one no longer in the bucket.
    _, count = _get_local_size_in_bytes_and_object_count(blobs_directory)
    assert count == 2

    target = blobs_directory.parent / "copy"
    (target / "345").mkdir(parents=True)
    for blob in ("01234567-good",):
        (target / "345" / blob).write_bytes((blobs_directory / "012/345" / blob).read_bytes())
    assert _blob_subdirectory_is_fully_copied(source=blobs_directory / "012", target=target) == (True, "")


def test_deletes_copies_that_do_not_match(mirror, capsys):
    blobs_directory, _ = mirror
    corrupt = blobs_directory / "012/345/01234567-corrupt"
    short = blobs_directory / "0ab/cde/0abcdef0-short"
    # A cache left by an earlier run, from when the short copy had another content.
    _checksums.checksum_cache_path(short).write_text(_etag_of_parts(b"earlier") + "\n")

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.deleted == ["blobs/012/345/01234567-corrupt", "blobs/0ab/cde/0abcdef0-short"]
    for path in (corrupt, short):
        assert not path.exists()
        assert not _checksums.checksum_cache_path(path).exists()
    good = blobs_directory / "012/345/01234567-good"
    assert good.exists()
    assert _checksums.checksum_cache_path(good).exists()

    out = capsys.readouterr().out
    assert (
        f"[DELETED] blobs/012/345/01234567-corrupt: ETag {_etag_of_parts(b'the wrong content')}, "
        f"expected {_etag_of_parts(b'the right content')}; the next blob backup copies it again"
    ) in out
    assert "[DELETED] blobs/0ab/cde/0abcdef0-short: 9 bytes, expected 17; the next blob backup copies it again" in out
    assert "2 do not match, of which 0 were copied again and now match and 2 were deleted" in out


def test_deletes_copies_already_unresolved_in_ledger(mirror):
    """The mismatches recorded before copies were deleted are deleted on the next run, and not recorded twice."""
    blobs_directory, _ = mirror
    corrupt_etag = _etag_of_parts(b"the wrong content")
    right_etag = _etag_of_parts(b"the right content")
    recorded = [
        ("etag-mismatch", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
    ]
    lines = ["\t".join(_checksums._LEDGER_COLUMNS)] + [
        "\t".join(("2026-11-18T10:00:00+00:00", event, key, "/somewhere", observed, expected, "1_0"))
        for event, key, observed, expected in recorded
    ]
    _checksums.LEDGER_PATH.write_text("\n".join(lines) + "\n")

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.deleted == ["blobs/012/345/01234567-corrupt", "blobs/0ab/cde/0abcdef0-short"]
    assert not (blobs_directory / "012/345/01234567-corrupt").exists()
    assert not (blobs_directory / "0ab/cde/0abcdef0-short").exists()
    assert _ledger_events() == [
        *recorded,
        ("deleted", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("deleted", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
    ]


def test_keeps_recent_copies_that_would_not_match(mirror, monkeypatch):
    blobs_directory, _ = mirror
    monkeypatch.setattr(_checksums, "RECENT_SECONDS", 3600)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert not counts.deleted
    assert (blobs_directory / "012/345/01234567-corrupt").exists()
    assert (blobs_directory / "0ab/cde/0abcdef0-short").exists()


def test_records_a_copy_that_could_not_be_deleted(mirror, monkeypatch, capsys):
    blobs_directory, _ = mirror
    corrupt = blobs_directory / "012/345/01234567-corrupt"
    unlink = pathlib.Path.unlink

    def refuse_corrupt(path, missing_ok=False):
        if path == corrupt:
            raise PermissionError("Permission denied")
        unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(pathlib.Path, "unlink", refuse_corrupt)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.deleted == ["blobs/0ab/cde/0abcdef0-short"]
    assert counts.unrepaired == 1
    assert corrupt.exists()
    assert ("delete-failed", "blobs/012/345/01234567-corrupt") in [event[:2] for event in _ledger_events()]
    assert "[NOT DELETED] blobs/012/345/01234567-corrupt" in capsys.readouterr().out


def test_exit_codes(mirror, monkeypatch):
    blobs_directory, _ = mirror
    runner = CliRunner()

    # Copies that do not match were deleted: 3, which the runner's Check backup tells from other failures.
    assert runner.invoke(_s3backup, ["dandi", "verify-checksums", "0"]).exit_code == 3

    # Nothing left that does not match.
    assert runner.invoke(_s3backup, ["dandi", "verify-checksums", "0"]).exit_code == 0

    # A copy that could not be deleted is a failure like any other: 1.
    def refuse(path, missing_ok=False):
        raise PermissionError("Permission denied")

    (blobs_directory / "012/345/01234567-good").write_bytes(b"damaged")
    monkeypatch.setattr(pathlib.Path, "unlink", refuse)
    assert runner.invoke(_s3backup, ["dandi", "verify-checksums", "0"]).exit_code == 1


def test_exit_code_of_an_unlisted_prefix_wins(mirror, monkeypatch):
    listing = _checksums.list_s3_objects

    def fail_on_one_prefix(prefix):
        if prefix == "blobs/0ff/":
            raise TimeoutError("timed out")
        return listing(prefix)

    monkeypatch.setattr(_checksums, "list_s3_objects", fail_on_one_prefix)
    assert CliRunner().invoke(_s3backup, ["dandi", "verify-checksums", "0"]).exit_code == 1
