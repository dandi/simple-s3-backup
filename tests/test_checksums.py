import datetime
import hashlib
import pathlib

import pytest

from simple_s3_backup._base import _checksums, _not_in_bucket
from simple_s3_backup._base._dandi import _blob_subdirectory_is_fully_copied
from simple_s3_backup._base._display import _get_local_size_in_bytes_and_object_count

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
    monkeypatch.setattr(_checksums, "NOT_IN_BUCKET_RECORD_PATH", tmp_path / "not_in_bucket.tsv")
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
    assert counts.unrepaired == 2
    assert counts.not_mirrored == 1
    assert counts.not_in_bucket == ["blobs/0ab/cde/0abcdef0-deleted"]

    # The cache holds the ETag alone, stamped with the blob's modification time.
    good = blobs_directory / "012/345/01234567-good"
    cache_path = _checksums.checksum_cache_path(good)
    assert cache_path.read_text() == _etag_of_parts(b"the right content") + "\n"
    assert cache_path.stat().st_mtime_ns == good.stat().st_mtime_ns

    # The second run trusts the cache, until the file changes or the cache is older than allowed.
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (0, 2, 1)
    good.touch()
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 1, 1)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=0)
    assert (counts.hashed, counts.reused) == (2, 0)
    cache_path.write_text("not a checksum\n")
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 1, 1)

    assert "[MISMATCH] blobs/012/345/01234567-corrupt" in capsys.readouterr().out


def _ledger_events() -> list[tuple[str, str, str, str]]:
    """The ledger's lines after its header, as (event, key, observed, expected)."""
    lines = _checksums.LEDGER_PATH.read_text().splitlines()
    assert lines[0].split("\t") == list(_checksums._LEDGER_COLUMNS)
    return [tuple(fields[i] for i in (1, 2, 4, 5)) for fields in (line.split("\t") for line in lines[1:])]


def test_ledger(mirror):
    blobs_directory, _ = mirror
    corrupt_etag = _etag_of_parts(b"the wrong content")
    right_etag = _etag_of_parts(b"the right content")
    found = [
        ("etag-mismatch", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
    ]

    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _ledger_events() == found

    # A mismatch already recorded is not recorded again while it lasts.
    _checksums.verify_dandi_blob_checksums(task_id=0)
    _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=0)
    assert _ledger_events() == found

    # The blob backup copies the short one again, which a later run records as resolved; should it go
    # wrong again, that is a new mismatch.
    short = blobs_directory / "0ab/cde/0abcdef0-short"
    short.write_bytes(b"the right content")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    resolved = ("resolved", "blobs/0ab/cde/0abcdef0-short", right_etag, right_etag)
    assert _ledger_events() == [*found, resolved]
    short.write_bytes(b"the right")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _ledger_events() == [*found, resolved, found[1]]


def test_ledger_records_which_job(mirror, monkeypatch):
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "1234")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "0")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID")
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    (mirror[0] / "012/345/01234567-good").write_bytes(b"damaged")
    _checksums.verify_dandi_blob_checksums(task_id=0)

    jobs = [line.split("\t")[-1] for line in _checksums.LEDGER_PATH.read_text().splitlines()[1:]]
    assert jobs == ["1234_0", "1234_0", "manual"]


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

    _, count = _get_local_size_in_bytes_and_object_count(blobs_directory)
    assert count == 4

    target = blobs_directory.parent / "copy"
    (target / "345").mkdir(parents=True)
    for blob in ("01234567-good", "01234567-corrupt"):
        (target / "345" / blob).write_bytes((blobs_directory / "012/345" / blob).read_bytes())
    assert _blob_subdirectory_is_fully_copied(source=blobs_directory / "012", target=target) == (True, "")


DAY = 24 * 3600
UUID = "0abcdef0-1234-4567-89ab-0123456789ab"


def test_is_partial_download():
    assert _not_in_bucket.is_partial_download(f"{UUID}7")
    assert _not_in_bucket.is_partial_download(pathlib.Path(f"blobs/0ab/cde/{UUID}2887375472"))
    assert not _not_in_bucket.is_partial_download(UUID)  # The blob itself
    assert not _not_in_bucket.is_partial_download(f"{UUID}28873754721")  # More digits than a uint32 has
    assert not _not_in_bucket.is_partial_download(f".{UUID}.checksum")
    assert not _not_in_bucket.is_partial_download(f"{UUID}-1")


def _record_events() -> list[tuple[str, str]]:
    """The record's lines after its header, as (event, key)."""
    lines = _checksums.NOT_IN_BUCKET_RECORD_PATH.read_text().splitlines()
    assert lines[0].split("\t") == list(_not_in_bucket._RECORD_COLUMNS)
    return [tuple(line.split("\t")[1:3]) for line in lines[1:]]


def _backdate_record(days: float) -> None:
    """Move every event of the record ``days`` into the past, as if it had been recorded then."""
    lines = _checksums.NOT_IN_BUCKET_RECORD_PATH.read_text().splitlines()
    for index, line in enumerate(lines[1:], start=1):
        fields = line.split("\t")
        then = datetime.datetime.fromisoformat(fields[0]) - datetime.timedelta(days=days)
        lines[index] = "\t".join([then.isoformat(), *fields[1:]])
    _checksums.NOT_IN_BUCKET_RECORD_PATH.write_text("\n".join(lines) + "\n")


def test_not_in_bucket_is_recorded_once_and_deleted_after_the_grace_period(mirror, capsys):
    blobs_directory, _ = mirror
    deleted = blobs_directory / "0ab/cde/0abcdef0-deleted"
    key = "blobs/0ab/cde/0abcdef0-deleted"

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == [key]
    assert _record_events() == [("missing", key)]
    assert f"[NOT IN BUCKET] {key} (missing since " in capsys.readouterr().out

    # Not recorded again while it stays missing, and kept until the grace period is over.
    _checksums.checksum_cache_path(deleted).write_text("a stale checksum\n")
    _backdate_record(days=_not_in_bucket.GRACE_DAYS - 1)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == [key]
    assert deleted.exists()
    assert _record_events() == [("missing", key)]

    _backdate_record(days=1)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == []
    assert not deleted.exists()
    assert not _checksums.checksum_cache_path(deleted).exists()
    assert _record_events() == [("missing", key), ("deleted", key)]
    assert f"[DELETED] {key}" in capsys.readouterr().out

    # Once deleted it is forgotten: the next run records nothing.
    _checksums.verify_dandi_blob_checksums(task_id=0)
    assert _record_events() == [("missing", key), ("deleted", key)]


@pytest.mark.parametrize(("missing_for", "is_deleted"), [(-1, False), (0, True), (1, True)])
def test_grace_period_boundary(tmp_path, missing_for, is_deleted):
    record = _not_in_bucket.NotInBucketRecord(path=tmp_path / "not_in_bucket.tsv")
    path = tmp_path / "blob"
    path.write_bytes(b"gone from the bucket")
    key = "blobs/0ab/cde/blob"
    record.record("missing", key, path)
    missing_since = record.missing_since(key)

    counts = _not_in_bucket.NotInBucketCounts()
    _not_in_bucket.reconcile_prefix(
        "blobs/0ab/",
        bucket_keys=set(),
        unmatched_paths={key: path},
        record=record,
        counts=counts,
        recent_seconds=0,
        now=missing_since + _not_in_bucket.GRACE_DAYS * DAY + missing_for,
    )
    assert path.exists() is not is_deleted
    assert counts.deleted == ([key] if is_deleted else [])
    assert counts.kept == ([] if is_deleted else [key])


def test_blob_back_in_the_bucket_is_forgotten(mirror, monkeypatch):
    blobs_directory, contents = mirror
    key = "blobs/0ab/cde/0abcdef0-deleted"
    _checksums.verify_dandi_blob_checksums(task_id=0)
    _backdate_record(days=_not_in_bucket.GRACE_DAYS)

    # Back in the bucket, the blob is forgotten rather than deleted.
    listing = [_checksums.S3Object(key, len(data), _etag_of_parts(data)) for key, data in contents.items()]
    listing.append(_checksums.S3Object(key, 20, _etag_of_parts(b"gone from the bucket")))
    monkeypatch.setattr(
        _checksums, "list_s3_objects", lambda prefix: [item for item in listing if item.key.startswith(prefix)]
    )
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == []
    assert (blobs_directory / "0ab/cde/0abcdef0-deleted").exists()
    assert _record_events() == [("missing", key), ("reappeared", key)]

    # Gone again, its grace period starts afresh.
    listing.pop()
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == [key]
    assert (blobs_directory / "0ab/cde/0abcdef0-deleted").exists()
    assert _record_events() == [("missing", key), ("reappeared", key), ("missing", key)]


def test_blob_deleted_by_other_means_is_forgotten(mirror):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    (blobs_directory / "0ab/cde/0abcdef0-deleted").unlink()
    _checksums.verify_dandi_blob_checksums(task_id=0)
    key = "blobs/0ab/cde/0abcdef0-deleted"
    assert _record_events() == [("missing", key), ("gone", key)]


def test_nothing_is_deleted_under_a_prefix_that_could_not_be_listed(mirror, monkeypatch):
    blobs_directory, _ = mirror
    partial = blobs_directory / f"0ab/cde/{UUID}2887375472"
    partial.write_bytes(b"half a blob")
    _checksums.verify_dandi_blob_checksums(task_id=0)
    _backdate_record(days=_not_in_bucket.GRACE_DAYS)
    monkeypatch.setattr(_not_in_bucket, "STALE_DOWNLOAD_SECONDS", 0)

    listed = _checksums.list_s3_objects

    def fail_for_0ab(prefix):
        if prefix == "blobs/0ab/":
            raise TimeoutError("the listing timed out")
        return listed(prefix)

    monkeypatch.setattr(_checksums, "list_s3_objects", fail_for_0ab)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.unlisted_prefixes == ["0ab"]
    assert (blobs_directory / "0ab/cde/0abcdef0-deleted").exists()
    assert partial.exists()
    assert _record_events() == [("missing", "blobs/0ab/cde/0abcdef0-deleted")]


def test_partial_downloads_are_removed_once_stale(mirror, monkeypatch, capsys):
    blobs_directory, _ = mirror
    partial = blobs_directory / f"0ab/cde/{UUID}2887375472"
    partial.write_bytes(b"half a blob")
    partial_key = f"blobs/0ab/cde/{UUID}2887375472"

    # One that changed in the last day may still be written, so it is left; neither is a blob missing
    # from the bucket.
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert partial.exists()
    assert partial_key not in counts.not_in_bucket
    assert partial_key not in capsys.readouterr().out
    assert _record_events() == [("missing", "blobs/0ab/cde/0abcdef0-deleted")]

    monkeypatch.setattr(_not_in_bucket, "STALE_DOWNLOAD_SECONDS", 0)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert not partial.exists()
    assert partial_key not in counts.not_in_bucket
    assert "1 partial downloads left by killed backup tasks were removed" in capsys.readouterr().out
    assert _record_events() == [("missing", "blobs/0ab/cde/0abcdef0-deleted")]


def test_recent_files_not_in_the_bucket_are_left_for_the_next_run(mirror, monkeypatch):
    monkeypatch.setattr(_checksums, "RECENT_SECONDS", 3600)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.not_in_bucket == []
    assert not _checksums.NOT_IN_BUCKET_RECORD_PATH.exists()


def test_backup_dandi_blobs_copies_on_size_alone(monkeypatch):
    from simple_s3_backup._base import _dandi

    commands = []
    monkeypatch.setattr(_dandi, "_deploy_subprocess", lambda command, ignore_errors: commands.append(command))
    _dandi.backup_dandi_blobs(task_id=2)

    assert len(commands) == 256
    assert all(command.startswith("s5cmd cp --if-size-differ s3://dandiarchive/blobs/2") for command in commands)
    assert not any("--if-source-newer" in command for command in commands)
