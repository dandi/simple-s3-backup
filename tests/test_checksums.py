import hashlib
import json
import pathlib

import pytest

from simple_s3_backup._base import _checksums
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

    good = blobs_directory / "012/345/01234567-good"
    cache = json.loads(_checksums.checksum_cache_path(good).read_text())
    assert cache["etag"] == _etag_of_parts(b"the right content")
    assert cache["matches"] is True
    assert cache["mtime_ns"] == good.stat().st_mtime_ns

    # The second run trusts the cache, until the file changes or the cache is older than allowed.
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (0, 2, 1)
    good.touch()
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 1, 1)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=0)
    assert (counts.hashed, counts.reused) == (2, 0)

    assert "[MISMATCH] blobs/012/345/01234567-corrupt" in capsys.readouterr().out


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
