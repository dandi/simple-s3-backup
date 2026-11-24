"""
What becomes of mirrored files that are not in the bucket, as the checksum verification finds them.

A mirrored file under a prefix whose listing of the bucket was complete, but which is not in that
listing, is one of two things:

- A partial download of the blob backup. ``s5cmd cp`` downloads each object into a temporary file in
  the destination's directory, named after the blob with a random number of up to 10 digits appended
  (Go's ``os.CreateTemp``), and renames it into place once the download is complete. It deletes it when
  the download fails, but not when SLURM kills the task, at its time limit or on preemption. Such a file
  was never in the bucket, so it is removed once it has not changed for ``STALE_DOWNLOAD_SECONDS``. A
  live download keeps writing to its file, which keeps its change time recent however long the task
  runs (up to 48 hours): only a file that no download has written to for a whole day is removed.
- A blob deleted from the archive. It is kept for ``GRACE_DAYS`` from when it was first found missing,
  so the mirror is a delayed backup of whatever DANDI deletes, and deleted then, with its cached
  checksum. When was it first found missing, and what became of it, is recorded in a shared record (see
  ``NotInBucketRecord``). One that is in the bucket again is forgotten.

Nothing is deleted under a prefix whose listing failed: the caller does not get this far for it.
"""

import dataclasses
import datetime
import fcntl
import os
import pathlib
import re
import time

from ._globals import NOT_IN_BUCKET_GRACE_DAYS

#: How long a blob is kept once it is found to be no longer in the bucket: about 6 months, set by
#: ``not_in_bucket_grace_days`` in partitions.json, which the runner's report reads too.
GRACE_DAYS = NOT_IN_BUCKET_GRACE_DAYS

#: A partial download is removed once it has not changed for this long.
STALE_DOWNLOAD_SECONDS = 24 * 3600

#: The record of blobs found to be no longer in the bucket, shared by the tasks of every head. The runner
#: of DANDI Compute publishes it to the not-in-bucket branch of its global logs.
NOT_IN_BUCKET_RECORD_PATH = pathlib.Path("/orcd/data/dandi/001/backup/not_in_bucket.tsv")
_RECORD_COLUMNS = ("time", "event", "key", "path", "job")

#: A blob's name, its UUID, with the random number s5cmd's temporary download file appends to it.
_DOWNLOAD_TEMPORARY_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}[0-9]{1,10}")


def is_partial_download(path: pathlib.Path | str) -> bool:
    """Whether ``path`` is named as s5cmd names the temporary file it downloads a blob into."""
    return _DOWNLOAD_TEMPORARY_PATTERN.fullmatch(pathlib.PurePath(path).name) is not None


@dataclasses.dataclass
class NotInBucketCounts:
    #: Keys of the blobs no longer in the bucket that are kept for now.
    kept: list[str] = dataclasses.field(default_factory=list)
    #: Keys of the blobs deleted from the mirror in this run, their grace period over.
    deleted: list[str] = dataclasses.field(default_factory=list)
    #: Keys first found missing in this run.
    newly_missing: list[str] = dataclasses.field(default_factory=list)
    #: Keys back in the bucket in this run, forgotten.
    reappeared: list[str] = dataclasses.field(default_factory=list)
    #: Partial downloads removed in this run, and those left as they may still be written.
    partial_downloads_removed: list[str] = dataclasses.field(default_factory=list)
    partial_downloads_left: int = 0
    #: Files not in the bucket that changed too recently to say: they may be blobs the backup just copied.
    recent: int = 0


class NotInBucketRecord:
    """
    The record of every blob found to be no longer in the bucket, and of what became of it.

    One tab-separated line per event, appended under a lock, so tasks of several heads can share it:

    - ``missing``: first found mirrored but not in the bucket. Its time starts the grace period.
    - ``reappeared``: in the bucket again; the blob is forgotten, and starts afresh if it goes again.
    - ``deleted``: deleted from the mirror, its grace period over.
    - ``gone``: neither in the bucket nor in the mirror any more, deleted by other means.

    A blob is pending while its last event is ``missing``.
    """

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.recorded = 0
        # When each pending blob was first found missing, as a POSIX time.
        self._missing_since: dict[str, float] = {}
        try:
            with path.open() as stream:
                for line in stream:
                    fields = line.rstrip("\n").split("\t")
                    if len(fields) == len(_RECORD_COLUMNS) and fields[0] != _RECORD_COLUMNS[0]:
                        self._track(event=fields[1], key=fields[2], time_text=fields[0])
        except OSError:
            pass

    def _track(self, event: str, key: str, time_text: str) -> None:
        if event == "missing":
            try:
                self._missing_since.setdefault(key, datetime.datetime.fromisoformat(time_text).timestamp())
            except ValueError:
                pass
        else:
            self._missing_since.pop(key, None)

    def missing_since(self, key: str) -> float | None:
        """When the blob was first found missing, if it is pending."""
        return self._missing_since.get(key)

    def pending_under(self, key_prefix: str) -> list[str]:
        return [key for key in self._missing_since if key.startswith(key_prefix)]

    def record(self, event: str, key: str, path: pathlib.Path | str) -> None:
        job = os.environ.get("SLURM_JOB_ID", "manual")
        if "SLURM_ARRAY_JOB_ID" in os.environ:
            job = f"{os.environ['SLURM_ARRAY_JOB_ID']}_{os.environ.get('SLURM_ARRAY_TASK_ID', '')}"
        time_text = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0).isoformat()
        fields = (time_text, event, key, str(path), job)
        line = "\t".join(" ".join(field.split()) for field in fields) + "\n"
        try:
            with self.path.open(mode="a") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                if stream.seek(0, os.SEEK_END) == 0:
                    stream.write("\t".join(_RECORD_COLUMNS) + "\n")
                stream.write(line)
        except OSError as error:
            # Not tracked either, so a blob whose `missing` was not recorded is not deleted on its strength.
            print(f"[NOT RECORDED] could not record {event} of {key} in {self.path}: {error}", flush=True)
            return
        self._track(event=event, key=key, time_text=time_text)
        self.recorded += 1


def _remove(path: pathlib.Path) -> None:
    """Remove a mirrored file and the checksum cached beside it, if any."""
    from ._checksums import checksum_cache_path

    path.unlink()
    cache_path = checksum_cache_path(path)
    for sidecar in (cache_path, cache_path.with_name(f"{cache_path.name}.partial")):
        sidecar.unlink(missing_ok=True)


def reconcile_prefix(
    key_prefix: str,
    bucket_keys: set[str],
    unmatched_paths: dict[str, pathlib.Path],
    record: NotInBucketRecord,
    counts: NotInBucketCounts,
    *,
    grace_days: float = GRACE_DAYS,
    recent_seconds: float,
    now: float | None = None,
) -> None:
    """
    Settle the mirrored files of one prefix that are not in its complete listing of the bucket.

    Parameters
    ----------
    key_prefix : str
        The prefix listed, as ``blobs/abc/``.
    bucket_keys : set of str
        Every key of the bucket under the prefix.
    unmatched_paths : dict
        The mirrored files under the prefix whose key is not in ``bucket_keys``, by key.
    recent_seconds : float
        A file changed more recently than this is left alone, as the backup may have copied it after the
        listing.
    """
    now = time.time() if now is None else now

    for key in record.pending_under(key_prefix):
        if key in bucket_keys:
            record.record("reappeared", key, "")
            counts.reappeared.append(key)
        elif key not in unmatched_paths:
            record.record("gone", key, "")

    for key, path in sorted(unmatched_paths.items()):
        try:
            changed = path.stat().st_ctime
        except FileNotFoundError:
            continue

        if is_partial_download(path):
            if now - changed < STALE_DOWNLOAD_SECONDS:
                counts.partial_downloads_left += 1
                continue
            try:
                _remove(path)
            except OSError as error:
                print(f"[NOT REMOVED] partial download {key}: {error}", flush=True)
                continue
            counts.partial_downloads_removed.append(key)
            continue

        missing_since = record.missing_since(key)
        if missing_since is None:
            if now - changed < recent_seconds:
                counts.recent += 1
                continue
            record.record("missing", key, path)
            missing_since = record.missing_since(key)
            if missing_since is None:  # Could not be recorded; kept, and tried again next run.
                counts.kept.append(key)
                continue
            counts.newly_missing.append(key)

        if now - missing_since < grace_days * 24 * 3600:
            counts.kept.append(key)
            continue
        try:
            _remove(path)
        except OSError as error:
            print(f"[NOT DELETED] {key}: {error}", flush=True)
            counts.kept.append(key)
            continue
        record.record("deleted", key, path)
        counts.deleted.append(key)


def print_summary(counts: NotInBucketCounts, record: NotInBucketRecord, grace_days: float, shown: int) -> None:
    """Report what became of the mirrored files that are not in the bucket."""
    print(
        f"  {len(counts.kept)} blobs no longer in the bucket are kept for {grace_days:g} days from when they were "
        f"first found missing ({len(counts.newly_missing)} first found in this run, {len(counts.reappeared)} "
        f"back in the bucket and forgotten)\n"
        f"  {len(counts.deleted)} blobs no longer in the bucket for {grace_days:g} days were deleted from the mirror\n"
        f"  {len(counts.partial_downloads_removed)} partial downloads left by killed backup tasks were removed, "
        f"{counts.partial_downloads_left} changed in the last day left for now"
    )
    for key in counts.kept[:shown]:
        since = datetime.datetime.fromtimestamp(record.missing_since(key) or time.time(), tz=datetime.timezone.utc)
        print(f"  [NOT IN BUCKET] {key} (missing since {since:%Y-%m-%d})")
    if len(counts.kept) > shown:
        print(f"  [NOT IN BUCKET] ... and {len(counts.kept) - shown} more")
    for key in counts.deleted[:shown]:
        print(f"  [DELETED] {key}")
    if len(counts.deleted) > shown:
        print(f"  [DELETED] ... and {len(counts.deleted) - shown} more")
    if record.recorded:
        print(f"\n{record.recorded} new events recorded in {record.path}")
