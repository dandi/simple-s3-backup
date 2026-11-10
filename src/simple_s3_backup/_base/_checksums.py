"""
Verifying the content of the blob mirror against the S3 bucket.

The blob backup copies with ``s5cmd cp --if-size-differ --if-source-newer``, which compares only sizes
and times, so a copy of the right size with the wrong content is never replaced. This checks the
content of every mirrored blob against the ETag of its object in the bucket.

DANDI uploads every blob in parts (``DandiETag`` in dandischema), so its ETag is the MD5 of the MD5s
of its parts, followed by ``-`` and the number of parts. The parts are 64 MiB, or larger for a file
that would otherwise need 10,000 parts or more. The ETag of the local copy is computed the same way.

Computing it reads the whole file, so the ETag alone is cached next to the blob, in
``.{blob_id}.checksum``, with the blob's modification time. A later run reuses it while the blob keeps
that modification time, unless the cache is older than it is told to trust.

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
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree

from ._globals import BLOBS_HEAD_TO_PARTITION
from ._utils import _deploy_subprocess

_BUCKET = "dandiarchive"
_BUCKET_URL = f"https://{_BUCKET}.s3.amazonaws.com"
_S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_MIRROR_ROOT = pathlib.Path("/orcd/data/dandi")

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
    not_in_bucket: list[str] = dataclasses.field(default_factory=list)
    unlisted_prefixes: list[str] = dataclasses.field(default_factory=list)

    @property
    def unrepaired(self) -> int:
        return len(self.mismatched) - len(self.repaired)


def is_checksum_cache(path: pathlib.Path | str) -> bool:
    """Whether ``path`` is a cached checksum (or one being written) rather than a mirrored object."""
    name = pathlib.PurePath(path).name
    return name.startswith(".") and (name.endswith(".checksum") or name.endswith(".checksum.partial"))


def checksum_cache_path(blob_path: pathlib.Path) -> pathlib.Path:
    """Where the checksum of a mirrored blob is cached: ``.{blob_id}.checksum`` beside it."""
    return blob_path.parent / f".{blob_path.name}.checksum"


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
                whole.update(chunk)
            return whole.hexdigest()

        part_digests = []
        for part_size in _part_sizes(os.fstat(file_stream.fileno()).st_size):
            part = hashlib.md5()
            remaining = part_size
            while remaining:
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


def _cached_etag(blob_path: pathlib.Path, stat: os.stat_result, oldest_trusted: float | None) -> str | None:
    """
    The cached ETag of the blob, if it was computed for the file as it is now and recently enough.

    The cache holds the ETag alone. Its modification time is set to the blob's when it is written, so a
    blob changed since no longer has the same one, and its change time is when it was written.
    """
    cache_path = checksum_cache_path(blob_path)
    try:
        cache_stat = cache_path.stat()
        if cache_stat.st_mtime_ns != stat.st_mtime_ns:
            return None
        if oldest_trusted is not None and cache_stat.st_ctime < oldest_trusted:
            return None
        etag = cache_path.read_text().strip()
    except OSError:
        return None
    return etag if _ETAG_PATTERN.fullmatch(etag) else None


def _write_cache(blob_path: pathlib.Path, stat: os.stat_result, etag: str) -> None:
    """Cache the ETag computed from the blob as ``stat`` describes it, stamped with its modification time."""
    cache_path = checksum_cache_path(blob_path)
    partial_path = cache_path.with_name(f"{cache_path.name}.partial")
    partial_path.write_text(f"{etag}\n")
    os.utime(partial_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    partial_path.replace(cache_path)


class Ledger:
    """
    The record, kept for good, of every blob found not to match the bucket and of what became of it.

    One tab-separated line per event, appended under a lock, so tasks of several heads can share it:

    - ``etag-mismatch`` or ``size-mismatch``: the local copy does not match. One still unresolved is not
      recorded again when a later run finds it unchanged.
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


def _local_etag(
    blob_path: pathlib.Path,
    s3_etag: str,
    stat: os.stat_result,
    oldest_trusted: float | None,
    counts: VerificationCounts,
) -> str | None:
    """The ETag of the local copy, from its cache or read from the file; None if it changed while read."""
    cached = _cached_etag(blob_path, stat, oldest_trusted)
    if cached is not None:
        counts.reused += 1
        return cached

    etag = compute_etag(blob_path, multipart="-" in s3_etag)
    after = blob_path.stat()
    if (after.st_size, after.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
        return None
    counts.hashed += 1
    _write_cache(blob_path, stat, etag)
    return etag


def verify_object(
    s3_object: S3Object,
    blob_path: pathlib.Path,
    *,
    oldest_trusted: float | None,
    repair: bool,
    counts: VerificationCounts,
    ledger: Ledger,
) -> None:
    """Check the local copy of one object against its ETag, and copy it again on a mismatch if ``repair``."""
    stat = blob_path.stat()
    if time.time() - stat.st_ctime < RECENT_SECONDS:
        counts.recent += 1
        return

    if "-" in s3_object.etag and s3_object.etag.rpartition("-")[2] != str(len(_part_sizes(s3_object.size))):
        # Uploaded in other parts than DANDI's, whose sizes the ETag does not tell.
        counts.unverifiable += 1
        print(f"[UNVERIFIABLE] {s3_object.key}: its ETag {s3_object.etag} is not of DANDI's parts")
        return

    if stat.st_size != s3_object.size:
        event, observed, expected = "size-mismatch", stat.st_size, s3_object.size
        reason = f"{stat.st_size} bytes, expected {s3_object.size}"
    else:
        etag = _local_etag(blob_path, s3_object.etag, stat, oldest_trusted, counts)
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
        return

    # Without --if-size-differ or --if-source-newer, s5cmd copies over the file whatever it holds.
    _deploy_subprocess(command=f"s5cmd cp s3://{_BUCKET}/{s3_object.key} {blob_path}", ignore_errors=True)
    try:
        stat = blob_path.stat()
        etag = compute_etag(blob_path, multipart="-" in s3_object.etag)
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


def verify_dandi_blob_checksums(
    task_id: int,
    rehash_older_than_days: float | None = None,
    repair: bool = False,
    ledger_path: pathlib.Path | None = None,
) -> VerificationCounts:
    """
    Verify the mirrored blobs of one top-level hash head against their ETags in the bucket.

    Parameters
    ----------
    task_id : int
        The head, 0 to 15, as for ``s3backup dandi blobs``: the blobs whose ID starts with that hex digit.
    rehash_older_than_days : float, optional
        Read a file again when its checksum was cached more than this many days ago, even if the file has
        not changed since. 0 reads every file again. By default a cached checksum is trusted while the file
        keeps the modification time it had.
    repair : bool, default False
        Copy a blob again from the bucket when its local copy does not match.
    ledger_path : pathlib.Path, optional
        The ledger each mismatch, and what became of it, is recorded in (see ``Ledger``). Defaults to
        ``LEDGER_PATH``.
    """
    blobs_directory = _MIRROR_ROOT / BLOBS_HEAD_TO_PARTITION[task_id] / "s3dandiarchive" / "blobs"
    head = f"{task_id:01x}"
    oldest_trusted = None
    if rehash_older_than_days is not None:
        oldest_trusted = time.time() - rehash_older_than_days * 24 * 3600
    ledger = Ledger(path=ledger_path or LEDGER_PATH)

    print(f"Verifying head {head} in {blobs_directory} against s3://{_BUCKET}/blobs/{head}*", flush=True)
    counts = VerificationCounts()
    for second in range(16):
        for third in range(16):
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
                verify_object(
                    s3_object, blob_path, oldest_trusted=oldest_trusted, repair=repair, counts=counts, ledger=ledger
                )
            counts.not_in_bucket.extend(sorted(local_blob_paths))

        print(
            f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} through {head}{second:01x}f: {counts.verified} match, "
            f"{len(counts.mismatched)} do not; {counts.hashed} read, {counts.reused} from their cached checksum",
            flush=True,
        )

    print(
        f"\nHead {head} in {blobs_directory}:\n"
        f"  {counts.verified} match the bucket\n"
        f"  {len(counts.mismatched)} do not match, of which {len(counts.repaired)} were copied again and now match\n"
        f"  {counts.recent} changed in the last hour, left for the next run\n"
        f"  {counts.unverifiable} were uploaded in other parts than DANDI's, so cannot be verified\n"
        f"  {counts.not_mirrored} are in the bucket but not mirrored yet\n"
        f"  {len(counts.not_in_bucket)} are mirrored but no longer in the bucket\n"
        f"  {len(counts.unlisted_prefixes)} of 256 prefixes could not be listed"
        + (f": {' '.join(counts.unlisted_prefixes)}" if counts.unlisted_prefixes else "")
        + f"\n  {counts.hashed} files were read in this run, {counts.reused} checked from their cached checksum"
    )
    for key in counts.not_in_bucket[:_NOT_IN_BUCKET_SHOWN]:
        print(f"  [NOT IN BUCKET] {key}")
    if len(counts.not_in_bucket) > _NOT_IN_BUCKET_SHOWN:
        print(f"  [NOT IN BUCKET] ... and {len(counts.not_in_bucket) - _NOT_IN_BUCKET_SHOWN} more")
    if ledger.recorded:
        print(f"\n{ledger.recorded} new events recorded in the ledger {ledger.path}")
    if counts.mismatched:
        print(f"\n{len(counts.mismatched)} blobs do not match the bucket:")
        for mismatch in counts.mismatched:
            print(f"  {mismatch}")
    return counts
