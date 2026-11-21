"""
Verifying the content of the blob mirror against the S3 bucket.

The blob backup copies with ``s5cmd cp --if-size-differ``, which compares only sizes, so a copy of
the right size with the wrong content is never replaced. This checks the content of every mirrored
blob against the ETag of its object in the bucket.

DANDI uploads every blob in parts (``DandiETag`` in dandischema), so its ETag is the MD5 of the MD5s
of its parts, followed by ``-`` and the number of parts. The parts are 64 MiB, or larger for a file
that would otherwise need 10,000 parts or more. The ETag of the local copy is computed the same way.

Computing it reads the whole file, so the ETag is cached next to the blob, in ``.{blob_id}.checksum``,
as ``{etag} {when it was computed, ISO 8601 in UTC}``, and the cache file is given the blob's modification
time. A later run reuses it while the blob keeps that modification time and the checksum was computed less
than ``REHASH_DAYS`` ago, so that damage that leaves the modification time alone is still found within that
time. A cache written before the date was stored holds the ETag alone; it is taken as computed on the day a
run first sees it, and rewritten with that date, without reading the blob.

Each run first lists the bucket and checks every blob it can from its cache. It then reads the others: those
never read, or changed since, first, then those whose checksum is oldest, so that a run that cannot read all
that is due before it stops leaves the longest-unread for the next run, and never starves new blobs.

A copy that does not match is deleted, with its cached checksum, since the blob backup copies a missing
blob whatever its conditions, so that the next blob backup copies it afresh.

Every mismatch, and what became of it, is also recorded for good in a ledger (see ``Ledger``).
"""

import dataclasses
import datetime
import fcntl
import hashlib
import math
import os
import pathlib
import re
import signal
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree

from ._globals import BLOBS_HEAD_TO_PARTITION, MIRROR_ROOT
from ._not_in_bucket import (
    GRACE_DAYS,
    NOT_IN_BUCKET_RECORD_PATH,
    NotInBucketCounts,
    NotInBucketRecord,
    print_summary,
    reconcile_prefix,
)
from ._utils import _deploy_subprocess

_BUCKET = "dandiarchive"
_BUCKET_URL = f"https://{_BUCKET}.s3.amazonaws.com"
_S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_MIRROR_ROOT = MIRROR_ROOT

_DEFAULT_PART_SIZE = 64 * 2**20
_MAX_PARTS = 10_000
_READ_SIZE = 8 * 2**20
_LISTING_ATTEMPTS = 4

#: A file changed this recently may still be being written by the backup, so it is left for the next run.
RECENT_SECONDS = 3600

#: How many blobs that are on the partition but not in the bucket are named, per head.
_NOT_IN_BUCKET_SHOWN = 20

#: What a cached checksum holds: an ETag, in parts or not.
_ETAG_PATTERN = re.compile(r"[0-9a-f]{32}(-[0-9]+)?")

#: How many days a cached checksum is trusted, from when it was computed, before the blob is read again.
REHASH_DAYS = 90.0

#: How often, in seconds, a run reports how far it has got in reading the blobs.
_PROGRESS_SECONDS = 600

#: The ledger of mismatches, never pruned. The runner of DANDI Compute publishes it to the
#: checksum-mismatches branch of its global logs.
LEDGER_PATH = pathlib.Path("/orcd/data/dandi/001/backup/checksum_mismatches.tsv")
_LEDGER_COLUMNS = ("time", "event", "key", "path", "observed", "expected", "job")
_MISMATCH_EVENTS = ("etag-mismatch", "size-mismatch")
_RESOLVING_EVENTS = ("repaired", "resolved")


@dataclasses.dataclass(frozen=True)
class S3Object:
    key: str
    size: int
    etag: str


@dataclasses.dataclass
class VerificationCounts:
    verified: int = 0
    hashed: int = 0
    reused: int = 0
    not_mirrored: int = 0
    recent: int = 0
    unverifiable: int = 0
    mismatched: list[str] = dataclasses.field(default_factory=list)
    repaired: list[str] = dataclasses.field(default_factory=list)
    deleted: list[str] = dataclasses.field(default_factory=list)
    not_in_bucket: list[str] = dataclasses.field(default_factory=list)
    unlisted_prefixes: list[str] = dataclasses.field(default_factory=list)
    #: Caches written before the date was stored, now stamped with the date of this run without reading the blob.
    stamped: int = 0
    #: The size of the mirrored blobs that are in the bucket, all of which are to be read once every interval.
    mirrored_bytes: int = 0
    #: The blobs never read, or changed since they were, found by this run, and their size.
    unread: int = 0
    unread_bytes: int = 0
    #: The blobs whose checksum was computed longer ago than the interval when this run started, and their size.
    overdue: int = 0
    overdue_bytes: int = 0
    #: When the oldest of their checksums was computed, as a POSIX timestamp.
    oldest_overdue: float | None = None
    #: How many of the overdue blobs this run read again.
    overdue_read: int = 0
    #: What reading blobs took: bytes read, and seconds spent reading them.
    bytes_read: int = 0
    seconds_reading: float = 0.0
    #: The signal that stopped the run before it was done, if one did.
    stopped_by: str | None = None

    @property
    def unrepaired(self) -> int:
        """The mismatches left as they were: neither copied again by ``--repair`` nor deleted."""
        return len(self.mismatched) - len(self.repaired) - len(self.deleted)

    @property
    def still_overdue(self) -> int:
        """The blobs overdue for reading again when this run started that it did not read."""
        return self.overdue - self.overdue_read


def is_checksum_cache(path: pathlib.Path | str) -> bool:
    """Whether ``path`` is a cached checksum (or one being written) rather than a mirrored object."""
    name = pathlib.PurePath(path).name
    return name.startswith(".") and (name.endswith(".checksum") or name.endswith(".checksum.partial"))


def checksum_cache_path(blob_path: pathlib.Path) -> pathlib.Path:
    """Where the checksum of a mirrored blob is cached: ``.{blob_id}.checksum`` beside it."""
    return blob_path.parent / f".{blob_path.name}.checksum"


class _Stopped(Exception):
    """Raised where it is safe to stop, once a signal asked the run to stop."""


#: The name of the signal that asked the run to stop, once one has.
_stop_requested: str | None = None


def _request_stop(signal_number: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = signal.Signals(signal_number).name


def _raise_if_stopping() -> None:
    if _stop_requested is not None:
        raise _Stopped(_stop_requested)


def _part_sizes(file_size: int) -> list[int]:
    """The sizes of the parts DANDI uploads a file of ``file_size`` bytes in."""
    if file_size == 0:
        return []
    part_size = _DEFAULT_PART_SIZE
    if math.ceil(file_size / part_size) >= _MAX_PARTS:
        part_size = math.ceil(file_size / _MAX_PARTS)
    full_parts, final_part_size = divmod(file_size, part_size)
    return [part_size] * full_parts + ([final_part_size] if final_part_size else [])


def compute_etag(file_path: pathlib.Path, multipart: bool = True) -> str:
    """
    The S3 ETag the file would have: in DANDI's parts, or its plain MD5 when ``multipart`` is False.

    Parameters
    ----------
    file_path : pathlib.Path
        The file to read.
    multipart : bool, default True
        Whether the object was uploaded in parts, as an ETag ending in ``-{number of parts}`` shows.
    """
    with file_path.open(mode="rb") as file_stream:
        if not multipart:
            whole = hashlib.md5()
            for chunk in iter(lambda: file_stream.read(_READ_SIZE), b""):
                _raise_if_stopping()
                whole.update(chunk)
            return whole.hexdigest()

        part_digests = []
        for part_size in _part_sizes(os.fstat(file_stream.fileno()).st_size):
            part = hashlib.md5()
            remaining = part_size
            while remaining:
                _raise_if_stopping()
                chunk = file_stream.read(min(remaining, _READ_SIZE))
                if not chunk:
                    break
                part.update(chunk)
                remaining -= len(chunk)
            part_digests.append(part.digest())
    return f"{hashlib.md5(b''.join(part_digests)).hexdigest()}-{len(part_digests)}"


def list_s3_objects(prefix: str) -> list[S3Object]:
    """Every object of the bucket under ``prefix``, with its size and ETag, listed anonymously."""
    objects = []
    continuation_token = None
    while True:
        parameters = {"list-type": "2", "prefix": prefix}
        if continuation_token:
            parameters["continuation-token"] = continuation_token
        root = _get_listing_page(url=f"{_BUCKET_URL}/?{urllib.parse.urlencode(parameters)}")
        for contents in root.findall("s3:Contents", _S3_NAMESPACE):
            objects.append(
                S3Object(
                    key=contents.findtext("s3:Key", namespaces=_S3_NAMESPACE),
                    size=int(contents.findtext("s3:Size", namespaces=_S3_NAMESPACE)),
                    etag=contents.findtext("s3:ETag", namespaces=_S3_NAMESPACE).strip('"'),
                )
            )
        if root.findtext("s3:IsTruncated", namespaces=_S3_NAMESPACE) != "true":
            return objects
        continuation_token = root.findtext("s3:NextContinuationToken", namespaces=_S3_NAMESPACE)


def _get_listing_page(url: str) -> xml.etree.ElementTree.Element:
    for attempt in range(1, _LISTING_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return xml.etree.ElementTree.fromstring(response.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt == _LISTING_ATTEMPTS:
                raise
            print(f"Listing {url} failed ({error}); trying again.", flush=True)
            time.sleep(2**attempt)


def _local_blob_paths(prefix_directory: pathlib.Path) -> dict[str, pathlib.Path]:
    """Every mirrored blob under one prefix directory, by its key in the bucket."""
    blob_paths = {}
    for directory, _, file_names in os.walk(prefix_directory):
        for file_name in file_names:
            if is_checksum_cache(file_name):
                continue
            blob_path = pathlib.Path(directory) / file_name
            blob_paths[f"blobs/{blob_path.relative_to(prefix_directory.parent).as_posix()}"] = blob_path
    return blob_paths


@dataclasses.dataclass(frozen=True)
class CachedChecksum:
    etag: str
    #: When it was computed, as a POSIX timestamp; None for a cache written before the date was stored.
    computed: float | None


def _format_stamp(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(int(timestamp), tz=datetime.timezone.utc).isoformat()


def _read_cache(blob_path: pathlib.Path, stat: os.stat_result) -> CachedChecksum | None:
    """
    The checksum cached for the blob, if it was computed for the file as it is now.

    The cache holds ``{etag} {when it was computed}``, or the ETag alone if written before the date was stored.
    Its modification time is set to the blob's when it is written, so a blob changed since no longer has the
    same one.
    """
    cache_path = checksum_cache_path(blob_path)
    try:
        if cache_path.stat().st_mtime_ns != stat.st_mtime_ns:
            return None
        fields = cache_path.read_text().split()
    except OSError:
        return None
    if not 1 <= len(fields) <= 2 or not _ETAG_PATTERN.fullmatch(fields[0]):
        return None
    if len(fields) == 1:
        return CachedChecksum(etag=fields[0], computed=None)
    try:
        computed = datetime.datetime.fromisoformat(fields[1])
    except ValueError:
        return None
    if computed.tzinfo is None:
        return None
    return CachedChecksum(etag=fields[0], computed=computed.timestamp())


def _write_cache(blob_path: pathlib.Path, stat: os.stat_result, etag: str, computed: float | None = None) -> None:
    """
    Cache the ETag computed from the blob as ``stat`` describes it, with when it was computed (by default now).

    The cache file is given the blob's modification time, so that it no longer matches once the blob changes.
    """
    cache_path = checksum_cache_path(blob_path)
    partial_path = cache_path.with_name(f"{cache_path.name}.partial")
    partial_path.write_text(f"{etag} {_format_stamp(time.time() if computed is None else computed)}\n")
    os.utime(partial_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    partial_path.replace(cache_path)


def _trusted_etag(
    blob_path: pathlib.Path,
    stat: os.stat_result,
    *,
    now: float,
    rehash_after_seconds: float | None,
    counts: VerificationCounts,
) -> tuple[str | None, float | None]:
    """
    Whether the blob can be checked from its cached checksum, or when that checksum was computed.

    Returns ``(etag, None)`` when the cached ETag can be trusted; ``(None, computed)`` when it was computed
    ``rehash_after_seconds`` or longer before ``now``, so the blob is due to be read again; and ``(None, None)``
    when there is no cache for the blob as it is now: it was never read, or has changed since.

    A cache without a date is taken as computed ``now``, and rewritten with that date unless it is due anyway.
    """
    cached = _read_cache(blob_path, stat)
    if cached is None:
        return None, None
    computed = now if cached.computed is None else cached.computed
    if rehash_after_seconds is not None and now - computed >= rehash_after_seconds:
        return None, computed
    if cached.computed is None:
        try:
            _write_cache(blob_path, stat, cached.etag, computed=now)
            counts.stamped += 1
        except OSError as error:
            print(f"[NOT STAMPED] {blob_path}: could not date its cached checksum ({error})", flush=True)
    return cached.etag, None


class Ledger:
    """
    The record, kept for good, of every blob found not to match the bucket and of what became of it.

    One tab-separated line per event, appended under a lock, so tasks of several heads can share it:

    - ``etag-mismatch`` or ``size-mismatch``: the local copy does not match. One still unresolved is not
      recorded again when a later run finds it unchanged.
    - ``deleted``: the copy that does not match, and its cached checksum, were deleted, so the next blob
      backup copies it afresh. Recorded every time a copy is deleted, with the mismatch's observed and
      expected values. The mismatch stays unresolved until a later run finds the new copy matching.
    - ``delete-failed``: the copy that does not match could not be deleted.
    - ``repaired``: ``--repair`` copied it again and it now matches.
    - ``repair-failed``: ``--repair`` copied it again and it still does not match.
    - ``resolved``: a later run found it matching without ``--repair``, the blob backup having copied it again.
    """

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.recorded = 0
        # The unresolved mismatch of each blob that has one, as (event, observed, expected).
        self._unresolved: dict[str, tuple[str, str, str]] = {}
        try:
            with path.open() as stream:
                for line in stream:
                    fields = line.rstrip("\n").split("\t")
                    if len(fields) == len(_LEDGER_COLUMNS) and fields[0] != _LEDGER_COLUMNS[0]:
                        self._track(event=fields[1], key=fields[2], observed=fields[4], expected=fields[5])
        except OSError:
            pass

    def _track(self, event: str, key: str, observed: str, expected: str) -> None:
        if event in _MISMATCH_EVENTS:
            self._unresolved[key] = (event, observed, expected)
        elif event in _RESOLVING_EVENTS:
            self._unresolved.pop(key, None)

    def is_unresolved(self, key: str) -> bool:
        return key in self._unresolved

    def record(self, event: str, key: str, path: pathlib.Path, observed: object, expected: object) -> None:
        observed, expected = str(observed), str(expected)
        if event in _MISMATCH_EVENTS and self._unresolved.get(key) == (event, observed, expected):
            return
        self._track(event=event, key=key, observed=observed, expected=expected)

        job = os.environ.get("SLURM_JOB_ID", "manual")
        if "SLURM_ARRAY_JOB_ID" in os.environ:
            job = f"{os.environ['SLURM_ARRAY_JOB_ID']}_{os.environ.get('SLURM_ARRAY_TASK_ID', '')}"
        time_now = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0).isoformat()
        fields = (time_now, event, key, str(path), observed, expected, job)
        line = "\t".join(" ".join(field.split()) for field in fields) + "\n"
        try:
            with self.path.open(mode="a") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                if stream.seek(0, os.SEEK_END) == 0:
                    stream.write("\t".join(_LEDGER_COLUMNS) + "\n")
                stream.write(line)
            self.recorded += 1
        except OSError as error:
            print(f"[NOT IN LEDGER] could not record {event} of {key} in {self.path}: {error}", flush=True)


def _read_etag(blob_path: pathlib.Path, multipart: bool, counts: VerificationCounts) -> str:
    """Read the whole blob for its ETag, counting the bytes read and the time it took."""
    start = time.monotonic()
    try:
        return compute_etag(blob_path, multipart=multipart)
    finally:
        counts.seconds_reading += time.monotonic() - start
        try:
            counts.bytes_read += blob_path.stat().st_size
        except OSError:
            pass


def _local_etag(
    blob_path: pathlib.Path,
    s3_etag: str,
    stat: os.stat_result,
    cached_etag: str | None,
    counts: VerificationCounts,
) -> str | None:
    """The ETag of the local copy, cached or read from the file; None if it changed while read."""
    if cached_etag is not None:
        counts.reused += 1
        return cached_etag

    etag = _read_etag(blob_path, multipart="-" in s3_etag, counts=counts)
    after = blob_path.stat()
    if (after.st_size, after.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
        return None
    counts.hashed += 1
    _write_cache(blob_path, stat, etag)
    return etag


def _is_unverifiable(s3_object: S3Object) -> bool:
    """Whether the object was uploaded in other parts than DANDI's, whose sizes its ETag does not tell."""
    return "-" in s3_object.etag and s3_object.etag.rpartition("-")[2] != str(len(_part_sizes(s3_object.size)))


def _is_recent(stat: os.stat_result) -> bool:
    return time.time() - stat.st_ctime < RECENT_SECONDS


def verify_object(
    s3_object: S3Object,
    blob_path: pathlib.Path,
    *,
    repair: bool,
    counts: VerificationCounts,
    ledger: Ledger,
    cached_etag: str | None = None,
    stat: os.stat_result | None = None,
) -> None:
    """
    Check the local copy of one object against its ETag.

    It is checked against ``cached_etag`` if given, which must have been computed for the file as ``stat``
    describes it, and otherwise read in full.

    On a mismatch, copy it again if ``repair``, and otherwise delete it, with its cached checksum, for the
    next blob backup to copy afresh.
    """
    if stat is None:
        try:
            stat = blob_path.stat()
        except FileNotFoundError:
            # Gone since the mirror was listed.
            counts.not_mirrored += 1
            return
    if _is_recent(stat):
        counts.recent += 1
        return

    if _is_unverifiable(s3_object):
        counts.unverifiable += 1
        print(f"[UNVERIFIABLE] {s3_object.key}: its ETag {s3_object.etag} is not of DANDI's parts")
        return

    if stat.st_size != s3_object.size:
        event, observed, expected = "size-mismatch", stat.st_size, s3_object.size
        reason = f"{stat.st_size} bytes, expected {s3_object.size}"
    else:
        etag = _local_etag(blob_path, s3_object.etag, stat, cached_etag, counts)
        if etag is None:
            counts.recent += 1
            return
        if etag == s3_object.etag:
            counts.verified += 1
            if ledger.is_unresolved(s3_object.key):
                ledger.record("resolved", s3_object.key, blob_path, observed=etag, expected=s3_object.etag)
            return
        event, observed, expected = "etag-mismatch", etag, s3_object.etag
        reason = f"ETag {etag}, expected {s3_object.etag}"

    print(f"[MISMATCH] {s3_object.key}: {reason}", flush=True)
    counts.mismatched.append(f"{s3_object.key}: {reason}")
    ledger.record(event, s3_object.key, blob_path, observed=observed, expected=expected)
    if not repair:
        _delete_copy(
            s3_object, blob_path, reason=reason, observed=observed, expected=expected, counts=counts, ledger=ledger
        )
        return

    # Without --if-size-differ, s5cmd copies over the file whatever it holds.
    _deploy_subprocess(command=f"s5cmd cp s3://{_BUCKET}/{s3_object.key} {blob_path}", ignore_errors=True)
    try:
        stat = blob_path.stat()
        etag = _read_etag(blob_path, multipart="-" in s3_object.etag, counts=counts)
    except OSError as error:
        print(f"[NOT REPAIRED] {s3_object.key}: could not read it after copying it again ({error})", flush=True)
        ledger.record("repair-failed", s3_object.key, blob_path, observed=f"unreadable: {error}", expected=expected)
        return
    _write_cache(blob_path, stat, etag)
    if etag == s3_object.etag:
        counts.repaired.append(s3_object.key)
        print(f"[REPAIRED] {s3_object.key}: copied again from the bucket", flush=True)
        ledger.record("repaired", s3_object.key, blob_path, observed=etag, expected=s3_object.etag)
    else:
        print(f"[NOT REPAIRED] {s3_object.key}: copied again, it still has ETag {etag}", flush=True)
        ledger.record("repair-failed", s3_object.key, blob_path, observed=etag, expected=s3_object.etag)


def _delete_copy(
    s3_object: S3Object,
    blob_path: pathlib.Path,
    *,
    reason: str,
    observed: object,
    expected: object,
    counts: VerificationCounts,
    ledger: Ledger,
) -> None:
    """
    Delete a copy that does not match, and its cached checksum, so the next blob backup copies it afresh.

    Whatever the conditions of that backup's ``s5cmd cp``, it copies a blob that is missing, while a copy of the
    right size with the wrong content can stay as it is.
    """
    try:
        blob_path.unlink(missing_ok=True)
        checksum_cache_path(blob_path).unlink(missing_ok=True)
    except OSError as error:
        print(f"[MISMATCH NOT DELETED] {s3_object.key}: {reason}; could not delete {blob_path} ({error})", flush=True)
        ledger.record("delete-failed", s3_object.key, blob_path, observed=f"error: {error}", expected=expected)
        return
    counts.deleted.append(s3_object.key)
    print(f"[MISMATCH DELETED] {s3_object.key}: {reason}; the next blob backup copies it again", flush=True)
    ledger.record("deleted", s3_object.key, blob_path, observed=observed, expected=expected)


@dataclasses.dataclass(order=True)
class _ToRead:
    """A blob to be read in full, in the order they are: by ``group``, then oldest ``computed`` first."""

    #: 0 for a blob never read, or changed since; 1 for one whose cached checksum is overdue.
    group: int
    #: When its cached checksum was computed, as a POSIX timestamp; 0 for a blob never read.
    computed: float
    key: str
    s3_object: S3Object = dataclasses.field(compare=False)
    blob_path: pathlib.Path = dataclasses.field(compare=False)
    size: int = dataclasses.field(compare=False)


def _human_bytes(number: float) -> str:
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if number < 1000:
            return f"{number:.0f} {unit}" if unit == "B" else f"{number:.2f} {unit}"
        number /= 1000
    return f"{number:.2f} PB"


def _rate(number_of_bytes: float, seconds: float) -> str:
    return f"{number_of_bytes / seconds / 1e6:.1f} MB/s" if seconds > 0 else "n/a"


def _check_from_caches(
    head: str,
    blobs_directory: pathlib.Path,
    *,
    now: float,
    rehash_after_seconds: float | None,
    repair: bool,
    counts: VerificationCounts,
    ledger: Ledger,
    not_in_bucket: NotInBucketCounts,
    not_in_bucket_record: NotInBucketRecord,
    not_in_bucket_grace_days: float,
) -> list[_ToRead]:
    """
    List the head in the bucket and on the mirror, and check every blob that needs no reading.

    That is each blob checked from its cached checksum, and each changed in the last hour, of the wrong size or
    uploaded in other parts than DANDI's. Returns the others, in the order they are to be read: those never
    read, or changed since, first, then those whose checksum is overdue, oldest first.
    """
    to_read = []
    for second in range(16):
        for third in range(16):
            _raise_if_stopping()
            prefix = f"{head}{second:01x}{third:01x}"
            try:
                s3_objects = list_s3_objects(prefix=f"blobs/{prefix}/")
            except (urllib.error.URLError, TimeoutError, ConnectionError, xml.etree.ElementTree.ParseError) as error:
                print(f"[NOT LISTED] blobs/{prefix}/: {error}", flush=True)
                counts.unlisted_prefixes.append(prefix)
                continue

            local_blob_paths = _local_blob_paths(blobs_directory / prefix)
            for s3_object in s3_objects:
                blob_path = local_blob_paths.pop(s3_object.key, None)
                if blob_path is None:
                    counts.not_mirrored += 1
                    continue
                try:
                    stat = blob_path.stat()
                except FileNotFoundError:
                    counts.not_mirrored += 1
                    continue
                counts.mirrored_bytes += stat.st_size

                if _is_recent(stat) or _is_unverifiable(s3_object) or stat.st_size != s3_object.size:
                    verify_object(s3_object, blob_path, repair=repair, counts=counts, ledger=ledger, stat=stat)
                    continue
                etag, computed = _trusted_etag(
                    blob_path, stat, now=now, rehash_after_seconds=rehash_after_seconds, counts=counts
                )
                if etag is not None:
                    verify_object(
                        s3_object, blob_path, repair=repair, counts=counts, ledger=ledger, cached_etag=etag, stat=stat
                    )
                elif computed is None:
                    counts.unread += 1
                    counts.unread_bytes += stat.st_size
                    to_read.append(_ToRead(0, 0.0, s3_object.key, s3_object, blob_path, stat.st_size))
                else:
                    counts.overdue += 1
                    counts.overdue_bytes += stat.st_size
                    if counts.oldest_overdue is None or computed < counts.oldest_overdue:
                        counts.oldest_overdue = computed
                    to_read.append(_ToRead(1, computed, s3_object.key, s3_object, blob_path, stat.st_size))

            kept_before = len(not_in_bucket.kept)
            reconcile_prefix(
                key_prefix=f"blobs/{prefix}/",
                bucket_keys={s3_object.key for s3_object in s3_objects},
                unmatched_paths=local_blob_paths,
                record=not_in_bucket_record,
                counts=not_in_bucket,
                grace_days=not_in_bucket_grace_days,
                recent_seconds=RECENT_SECONDS,
            )
            counts.not_in_bucket.extend(not_in_bucket.kept[kept_before:])

        print(
            f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} listed through {head}{second:01x}f: {counts.verified} match"
            f" from their cached checksum or need no reading, {len(counts.mismatched)} do not; {counts.unread} never"
            f" read or changed since and {counts.overdue} overdue are to be read",
            flush=True,
        )
    to_read.sort()
    return to_read


def _read_in_order(to_read: list[_ToRead], *, repair: bool, counts: VerificationCounts, ledger: Ledger) -> None:
    """Read the blobs ``_check_from_caches`` left, in its order, reporting the progress every so often."""
    total_bytes = sum(item.size for item in to_read)
    print(
        f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} reading {len(to_read)} blobs ({_human_bytes(total_bytes)}):"
        f" {counts.unread} never read or changed since, then {counts.overdue} overdue, oldest first",
        flush=True,
    )
    last_report = time.monotonic()
    for done, item in enumerate(to_read, start=1):
        _raise_if_stopping()
        hashed_before = counts.hashed
        verify_object(item.s3_object, item.blob_path, repair=repair, counts=counts, ledger=ledger)
        if item.group == 1 and counts.hashed > hashed_before:
            counts.overdue_read += 1
        if time.monotonic() - last_report >= _PROGRESS_SECONDS:
            last_report = time.monotonic()
            print(
                f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} read {done} of {len(to_read)} blobs,"
                f" {_human_bytes(counts.bytes_read)} at {_rate(counts.bytes_read, counts.seconds_reading)};"
                f" {counts.still_overdue} still overdue",
                flush=True,
            )


def _print_reading_summary(counts: VerificationCounts, rehash_older_than_days: float | None, seconds: float) -> None:
    """What reading the blobs took, and what reading all of them once every interval would."""
    print(
        f"\nReading:\n"
        f"  {_human_bytes(counts.bytes_read)} read in {counts.seconds_reading / 3600:.2f} h, at"
        f" {_rate(counts.bytes_read, counts.seconds_reading)} while reading"
        f" ({_rate(counts.bytes_read, seconds)} over the {seconds / 3600:.2f} h of the run)\n"
        f"  {counts.unread} blobs ({_human_bytes(counts.unread_bytes)}) had never been read or had changed since"
    )
    if rehash_older_than_days is None:
        return
    oldest = "" if counts.oldest_overdue is None else f", the oldest computed {_format_stamp(counts.oldest_overdue)}"
    print(
        f"  {counts.overdue} blobs ({_human_bytes(counts.overdue_bytes)}) were overdue, their checksum computed"
        f" {rehash_older_than_days:g} days ago or more{oldest}; {counts.overdue_read} were read again and"
        f" {counts.still_overdue} are still overdue\n"
        f"  {counts.stamped} cached checksums without a date were dated today, without reading their blob\n"
        f"  Reading all {_human_bytes(counts.mirrored_bytes)} mirrored under this head once every"
        f" {rehash_older_than_days:g} days takes"
        f" {_rate(counts.mirrored_bytes, rehash_older_than_days * 24 * 3600)} around the clock"
    )


def verify_dandi_blob_checksums(
    task_id: int,
    rehash_older_than_days: float | None = REHASH_DAYS,
    repair: bool = False,
    ledger_path: pathlib.Path | None = None,
    not_in_bucket_grace_days: float = GRACE_DAYS,
    not_in_bucket_record_path: pathlib.Path | None = None,
) -> VerificationCounts:
    """
    Verify the mirrored blobs of one top-level hash head against their ETags in the bucket.

    The head is first listed, and every blob that can be checked from its cached checksum is. Then the others
    are read: those never read, or changed since, first, then those whose checksum is overdue, oldest first.

    A SIGTERM, which SLURM sends at the time limit or on preemption, stops the run between two reads, or
    within one, and it prints its summary before returning, with ``stopped_by`` set.

    Parameters
    ----------
    task_id : int
        The head, 0 to 15, as for ``s3backup dandi blobs``: the blobs whose ID starts with that hex digit.
    rehash_older_than_days : float or None, default 90
        Read a file again when its checksum was computed this many days ago or more, even if the file has not
        changed since. 0 reads every file again. None trusts a cached checksum for as long as the file keeps
        the modification time it had.
    repair : bool, default False
        Copy a blob again from the bucket when its local copy does not match. Without it, that copy and its
        cached checksum are deleted, and the next blob backup copies it afresh.
    ledger_path : pathlib.Path, optional
        The ledger each mismatch, and what became of it, is recorded in (see ``Ledger``). Defaults to
        ``LEDGER_PATH``.
    not_in_bucket_grace_days : float, default 183
        How long a mirrored blob that is no longer in the bucket is kept from when it was first found
        missing, before it is deleted from the mirror (see ``_not_in_bucket``).
    not_in_bucket_record_path : pathlib.Path, optional
        The record of those blobs (see ``NotInBucketRecord``). Defaults to ``NOT_IN_BUCKET_RECORD_PATH``.
    """
    global _stop_requested
    blobs_directory = _MIRROR_ROOT / BLOBS_HEAD_TO_PARTITION[task_id] / "s3dandiarchive" / "blobs"
    head = f"{task_id:01x}"
    rehash_after_seconds = None if rehash_older_than_days is None else rehash_older_than_days * 24 * 3600
    ledger = Ledger(path=ledger_path or LEDGER_PATH)
    not_in_bucket_record = NotInBucketRecord(path=not_in_bucket_record_path or NOT_IN_BUCKET_RECORD_PATH)
    not_in_bucket = NotInBucketCounts()

    every = "never" if rehash_older_than_days is None else f"once {rehash_older_than_days:g} days old"
    print(
        f"Verifying head {head} in {blobs_directory} against s3://{_BUCKET}/blobs/{head}*;"
        f" a cached checksum is read again {every}",
        flush=True,
    )
    counts = VerificationCounts()
    start = time.monotonic()
    _stop_requested = None
    try:
        previous_handler = signal.signal(signal.SIGTERM, _request_stop)
    except ValueError:  # Not in the main thread, where signals cannot be handled.
        previous_handler = None
    try:
        to_read = _check_from_caches(
            head,
            blobs_directory,
            now=time.time(),
            rehash_after_seconds=rehash_after_seconds,
            repair=repair,
            counts=counts,
            ledger=ledger,
            not_in_bucket=not_in_bucket,
            not_in_bucket_record=not_in_bucket_record,
            not_in_bucket_grace_days=not_in_bucket_grace_days,
        )
        _read_in_order(to_read, repair=repair, counts=counts, ledger=ledger)
    except _Stopped as stop:
        counts.stopped_by = str(stop)
        print(f"\n{datetime.datetime.now():%Y-%m-%d %H:%M:%S} stopped by {stop} before it was done", flush=True)
    finally:
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)
        _stop_requested = None

    print(
        f"\nHead {head} in {blobs_directory}"
        + (f", stopped by {counts.stopped_by} before it was done" if counts.stopped_by else "")
        + ":\n"
        f"  {counts.verified} match the bucket\n"
        f"  {len(counts.mismatched)} do not match, of which {len(counts.repaired)} were copied again and now match"
        f" and {len(counts.deleted)} were deleted for the next blob backup to copy again\n"
        f"  {counts.recent} changed in the last hour, left for the next run\n"
        f"  {counts.unverifiable} were uploaded in other parts than DANDI's, so cannot be verified\n"
        f"  {counts.not_mirrored} are in the bucket but not mirrored yet\n"
        f"  {len(counts.not_in_bucket)} are mirrored but no longer in the bucket\n"
        f"  {len(counts.unlisted_prefixes)} of 256 prefixes could not be listed"
        + (f": {' '.join(counts.unlisted_prefixes)}" if counts.unlisted_prefixes else "")
        + f"\n  {counts.hashed} files were read in this run, {counts.reused} checked from their cached checksum"
    )
    _print_reading_summary(counts, rehash_older_than_days, seconds=time.monotonic() - start)
    print_summary(not_in_bucket, not_in_bucket_record, not_in_bucket_grace_days, shown=_NOT_IN_BUCKET_SHOWN)
    if ledger.recorded:
        print(f"\n{ledger.recorded} new events recorded in the ledger {ledger.path}")
    if counts.mismatched:
        print(f"\n{len(counts.mismatched)} blobs do not match the bucket:")
        for mismatch in counts.mismatched:
            print(f"  {mismatch}")
    return counts
