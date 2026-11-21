import datetime
import hashlib
import os
import pathlib
import signal
import time

import pytest
from click.testing import CliRunner

from simple_s3_backup._base import _checksums, _not_in_bucket
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
    # A copy of the wrong size needs no reading, so it is found while the head is listed, before those read.
    assert [mismatch.split(":")[0] for mismatch in counts.mismatched] == [
        "blobs/0ab/cde/0abcdef0-short",
        "blobs/012/345/01234567-corrupt",
    ]
    assert counts.deleted == ["blobs/0ab/cde/0abcdef0-short", "blobs/012/345/01234567-corrupt"]
    assert counts.unrepaired == 0
    assert counts.not_mirrored == 1
    assert counts.not_in_bucket == ["blobs/0ab/cde/0abcdef0-deleted"]

    # The cache holds the ETag and when it was computed, and has the blob's modification time.
    good = blobs_directory / "012/345/01234567-good"
    cache_path = _checksums.checksum_cache_path(good)
    etag, computed = cache_path.read_text().split()
    assert etag == _etag_of_parts(b"the right content")
    assert abs(datetime.datetime.fromisoformat(computed).timestamp() - time.time()) < 60
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


def _date_cache(blob_path: pathlib.Path, days_ago: float | None) -> None:
    """Rewrite the blob's cache as if its checksum was computed ``days_ago``, or without a date if None."""
    cache_path = _checksums.checksum_cache_path(blob_path)
    etag = cache_path.read_text().split()[0]
    stat = blob_path.stat()
    if days_ago is None:
        cache_path.write_text(f"{etag}\n")
        os.utime(cache_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    else:
        _checksums._write_cache(blob_path, stat, etag, computed=time.time() - days_ago * DAY)


def _cached_date(blob_path: pathlib.Path) -> float:
    return datetime.datetime.fromisoformat(_checksums.checksum_cache_path(blob_path).read_text().split()[1]).timestamp()


def test_cache_holds_the_etag_and_when_it_was_computed(tmp_path):
    blob = tmp_path / "blob"
    blob.write_bytes(b"content")
    stat = blob.stat()
    etag = _etag_of_parts(b"content")
    cache_path = _checksums.checksum_cache_path(blob)

    _checksums._write_cache(blob, stat, etag, computed=1_790_000_000.0)
    assert cache_path.read_text() == f"{etag} 2026-09-21T14:13:20+00:00\n"
    assert _checksums._read_cache(blob, stat) == _checksums.CachedChecksum(etag=etag, computed=1_790_000_000.0)

    # Written before the date was stored: the ETag alone, still read.
    cache_path.write_text(f"{etag}\n")
    os.utime(cache_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert _checksums._read_cache(blob, stat) == _checksums.CachedChecksum(etag=etag, computed=None)

    # Anything else is no cache at all, and so is one whose modification time is not the blob's.
    for text in (f"{etag} yesterday\n", f"{etag} 2026-09-21T12:53:20\n", f"{etag} 2026-09-21T12:53:20+00:00 more\n"):
        cache_path.write_text(text)
        os.utime(cache_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        assert _checksums._read_cache(blob, stat) is None
    _checksums._write_cache(blob, stat, etag)
    os.utime(cache_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
    assert _checksums._read_cache(blob, stat) is None


def test_cache_without_a_date_is_dated_today_without_reading_the_blob(mirror, monkeypatch):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    good = blobs_directory / "012/345/01234567-good"
    _date_cache(good, days_ago=None)

    compute_etag = _checksums.compute_etag

    def must_not_read(file_path, multipart=True):
        raise AssertionError(f"{file_path} was read")

    monkeypatch.setattr(_checksums, "compute_etag", must_not_read)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified, counts.stamped, counts.overdue) == (0, 1, 1, 1, 0)
    assert abs(_cached_date(good) - time.time()) < 60
    assert _checksums.checksum_cache_path(good).stat().st_mtime_ns == good.stat().st_mtime_ns

    # Dated once only.
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.stamped) == (0, 1, 0)

    # Unless every blob is to be read again anyway.
    _date_cache(good, days_ago=None)
    monkeypatch.setattr(_checksums, "compute_etag", compute_etag)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=0)
    assert (counts.hashed, counts.reused, counts.stamped) == (1, 0, 0)


def test_blob_is_read_again_once_its_checksum_is_90_days_old(mirror, capsys):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    good = blobs_directory / "012/345/01234567-good"

    _date_cache(good, days_ago=89)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.overdue) == (0, 1, 0)

    _date_cache(good, days_ago=91)
    capsys.readouterr()
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.verified) == (1, 0, 1)
    assert (counts.overdue, counts.overdue_bytes, counts.overdue_read, counts.still_overdue) == (1, 17, 1, 0)
    assert counts.bytes_read == 17
    assert abs(_cached_date(good) - time.time()) < 60
    out = capsys.readouterr().out
    assert "17 B read in " in out
    assert "1 blobs (17 B) were overdue, their checksum computed 90 days ago or more, the oldest computed " in out
    assert "1 were read again and 0 are still overdue" in out
    assert "Reading all 17 B mirrored under this head once every 90 days takes 0.0 MB/s around the clock" in out

    # The interval can be changed.
    _date_cache(good, days_ago=91)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0, rehash_older_than_days=120)
    assert (counts.hashed, counts.reused) == (0, 1)


def test_damage_that_keeps_the_modification_time_is_found_once_the_checksum_is_due(mirror):
    """As after a write followed by `touch -d`, `cp -p` or `rsync -a`, or damage below the filesystem."""
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    good = blobs_directory / "012/345/01234567-good"
    stat = good.stat()
    good.write_bytes(b"the wrong content")
    os.utime(good, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert not counts.mismatched

    _date_cache(good, days_ago=90)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.deleted == ["blobs/012/345/01234567-good"]


def test_changed_blob_is_read_again_at_once(mirror):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    good = blobs_directory / "012/345/01234567-good"
    _date_cache(good, days_ago=1)
    os.utime(good, (good.stat().st_atime, good.stat().st_mtime - 10))

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert (counts.hashed, counts.reused, counts.unread, counts.overdue, counts.verified) == (1, 0, 1, 0, 1)


def test_blobs_are_read_never_read_first_then_oldest_checksum_first(tmp_path, monkeypatch):
    monkeypatch.setattr(_checksums, "_MIRROR_ROOT", tmp_path)
    monkeypatch.setattr(_checksums, "LEDGER_PATH", tmp_path / "checksum_mismatches.tsv")
    monkeypatch.setattr(_checksums, "NOT_IN_BUCKET_RECORD_PATH", tmp_path / "not_in_bucket.tsv")
    monkeypatch.setattr(_checksums, "RECENT_SECONDS", 0)
    blobs_directory = tmp_path / "001" / "s3dandiarchive" / "blobs"
    # Listed in this order; the never read one last, and the oldest checksum after a newer one.
    days_ago = {
        "000/000/0000-fresh": 1,
        "001/000/0010-100-days": 100,
        "0fe/000/0fe0-120-days": 120,
        "0ff/000/0ff0-new": None,
    }
    listing = []
    for name in days_ago:
        path = blobs_directory / name
        path.parent.mkdir(parents=True)
        path.write_bytes(name.encode())
        listing.append(_checksums.S3Object(f"blobs/{name}", len(name), _etag_of_parts(name.encode())))
        if days_ago[name] is not None:
            _checksums._write_cache(
                path, path.stat(), _etag_of_parts(name.encode()), time.time() - days_ago[name] * DAY
            )
    monkeypatch.setattr(
        _checksums, "list_s3_objects", lambda prefix: [item for item in listing if item.key.startswith(prefix)]
    )
    read = []
    compute_etag = _checksums.compute_etag

    def record_reads(file_path, multipart=True):
        read.append(file_path.name)
        return compute_etag(file_path, multipart=multipart)

    monkeypatch.setattr(_checksums, "compute_etag", record_reads)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert read == ["0ff0-new", "0fe0-120-days", "0010-100-days"]
    assert (counts.verified, counts.reused, counts.unread, counts.overdue, counts.overdue_read) == (4, 1, 1, 2, 2)
    assert counts.oldest_overdue == pytest.approx(time.time() - 120 * DAY, abs=60)


def test_sigterm_stops_the_run_with_its_summary(mirror, monkeypatch, capsys):
    blobs_directory, _ = mirror
    _checksums.verify_dandi_blob_checksums(task_id=0)
    _date_cache(blobs_directory / "012/345/01234567-good", days_ago=91)
    compute_etag = _checksums.compute_etag

    def terminated_while_reading(file_path, multipart=True):
        os.kill(os.getpid(), signal.SIGTERM)
        return compute_etag(file_path, multipart=multipart)

    monkeypatch.setattr(_checksums, "compute_etag", terminated_while_reading)
    handler = signal.getsignal(signal.SIGTERM)
    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.stopped_by == "SIGTERM"
    assert (counts.hashed, counts.overdue, counts.still_overdue) == (0, 1, 1)
    assert signal.getsignal(signal.SIGTERM) is handler
    out = capsys.readouterr().out
    assert ", stopped by SIGTERM before it was done:" in out
    assert "0 were read again and 1 are still overdue" in out
    # The cache it was reading for is left as it was.
    assert _cached_date(blobs_directory / "012/345/01234567-good") < time.time() - 90 * DAY


def test_cli_reads_again_every_90_days_by_default(mirror, monkeypatch):
    received = {}
    monkeypatch.setattr(
        "simple_s3_backup._command_line_interface._cli.verify_dandi_blob_checksums",
        lambda **kwargs: received.update(kwargs) or _checksums.VerificationCounts(),
    )
    assert CliRunner().invoke(_s3backup, ["dandi", "verify-checksums", "0"]).exit_code == 0
    assert received["rehash_older_than_days"] == 90
    assert CliRunner().invoke(_s3backup, ["dandi", "verify-checksums", "0", "--rehash-older-than", "30"]).exit_code == 0
    assert received["rehash_older_than_days"] == 30


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
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
        ("deleted", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
        ("etag-mismatch", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
        ("deleted", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
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
    assert _ledger_events() == [*found, *resolved, *found[:2]]
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
    assert counts.repaired == ["blobs/0ab/cde/0abcdef0-short", "blobs/012/345/01234567-corrupt"]
    assert counts.unrepaired == 0

    counts = _checksums.verify_dandi_blob_checksums(task_id=0)
    assert counts.verified == 3
    assert not counts.mismatched

    assert [event[:2] for event in _ledger_events()] == [
        ("size-mismatch", "blobs/0ab/cde/0abcdef0-short"),
        ("repaired", "blobs/0ab/cde/0abcdef0-short"),
        ("etag-mismatch", "blobs/012/345/01234567-corrupt"),
        ("repaired", "blobs/012/345/01234567-corrupt"),
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
    assert counts.deleted == ["blobs/0ab/cde/0abcdef0-short", "blobs/012/345/01234567-corrupt"]
    for path in (corrupt, short):
        assert not path.exists()
        assert not _checksums.checksum_cache_path(path).exists()
    good = blobs_directory / "012/345/01234567-good"
    assert good.exists()
    assert _checksums.checksum_cache_path(good).exists()

    out = capsys.readouterr().out
    assert (
        f"[MISMATCH DELETED] blobs/012/345/01234567-corrupt: ETag {_etag_of_parts(b'the wrong content')}, "
        f"expected {_etag_of_parts(b'the right content')}; the next blob backup copies it again"
    ) in out
    assert (
        "[MISMATCH DELETED] blobs/0ab/cde/0abcdef0-short: 9 bytes, expected 17; the next blob backup copies it again"
        in out
    )
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
    assert counts.deleted == ["blobs/0ab/cde/0abcdef0-short", "blobs/012/345/01234567-corrupt"]
    assert not (blobs_directory / "012/345/01234567-corrupt").exists()
    assert not (blobs_directory / "0ab/cde/0abcdef0-short").exists()
    assert _ledger_events() == [
        *recorded,
        ("deleted", "blobs/0ab/cde/0abcdef0-short", "9", "17"),
        ("deleted", "blobs/012/345/01234567-corrupt", corrupt_etag, right_etag),
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
    assert "[MISMATCH NOT DELETED] blobs/012/345/01234567-corrupt" in capsys.readouterr().out


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
    captured = []

    def deploy(command, ignore_errors, capture_output=True):
        commands.append(command)
        captured.append(capture_output)

    monkeypatch.setattr(_dandi, "_deploy_subprocess", deploy)
    _dandi.backup_dandi_blobs(task_id=2)

    assert len(commands) == 256
    assert all(command.startswith("s5cmd cp --if-size-differ s3://dandiarchive/blobs/2") for command in commands)
    assert not any("--if-source-newer" in command for command in commands)
    # s5cmd's line for each copied blob goes to the task's log rather than being captured.
    assert not any(captured)


def test_deploy_subprocess_without_capture_writes_output_as_it_goes(capfd):
    from simple_s3_backup._base._utils import _deploy_subprocess

    assert _deploy_subprocess(command="echo cp s3://dandiarchive/blobs/abc", capture_output=False) is None
    assert capfd.readouterr().out == "cp s3://dandiarchive/blobs/abc\n"
    assert _deploy_subprocess(command="exit 1", capture_output=False, ignore_errors=True) is None
