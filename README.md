# Simple S3 Backup

Simple scripts for running backup on MIT cluster.

Monitor current status at: https://github.com/dandi/backup-status

See also https://github.com/dandi/s3invsync for a more complicated approach that may be necessary for handling large Zarr stores.



# Crontab

The jobs are scheduled by the crontab of
[dandi-compute-runner](https://github.com/dandi-compute/dandi-compute-runner/blob/main/launcher/crontab),
the only crontab installed on the cluster's login node. It submits each of them through
[`launcher/backup.sh`](https://github.com/dandi-compute/dandi-compute-runner/blob/main/launcher/backup.sh):

```bash
0 22 * * * /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh blobs > /dev/null 2>&1
#0 19 * * 0,3 /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh zarr > /dev/null 2>&1  # Zarr was disabled in May 2026
0 18 * * * /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh nonblobs > /dev/null 2>&1
0 6 * * * /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh dashboard > /dev/null 2>&1
0 10 * * * /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh checksums > /dev/null 2>&1
30 11 * * * /orcd/data/dandi/001/dandi-compute/dandi-compute-runner/launcher/backup.sh verify-blobs > /dev/null 2>&1
```

`backup.sh` names the jobs (`DANDI-Backup-Blobs`, `DANDI-Backup-Nonblobs`, `DANDI-Update-Backup-Dashboard`, `DANDI-Verify-Checksums`, `DANDI-Verify-Blobs`),
keeps the per-kind `flock` around `sbatch`, and records each submission in DANDI Compute's global logs. It
passes `--output` and `--error` itself, which take precedence over the `#SBATCH` ones in these scripts, so
each job's output goes to `/orcd/data/dandi/001/backup/logs/{job name}-{job ID}.log` (or
`{job name}-{array job}_{task}.log`). The runner's Check backup workflow emails when one of these jobs fails
or the blob mirror stops growing.

The scripts are run from cron's environment, which sets neither `MODULEPATH` nor most other variables. Each
turns on `set -u` only after `module load` and `conda activate`, since the cluster's module scripts read
`MODULEPATH` and bash would otherwise stop the job at once.

`backup_blobs_batch_deployment.sh` runs each task of the blob array for up to 48 hours, the most
`mit_preemptable` allows. s5cmd downloads a blob into a temporary file and cannot resume it, so a task killed
at its time limit throws away the blobs it was still downloading, and the next run downloads them again from
zero; at 12 hours every task was killed at its limit, and a blob that never fitted in what was left of one
was never mirrored. Since `backup.sh` submits nothing while a job of the same name is pending or running, an
array now runs for up to two days, and the next is submitted at the first 22:00 after it ends. A task that
is preempted is requeued and starts again from its first prefix, also losing the downloads it had in flight,
which is now the main reason a large blob is downloaded more than once. A task waits up to 15 minutes for
its head's lock, which a task SLURM has just killed may still hold, and fails if it does not get it.

`verify_blobs_backup_deployment.sh` runs `s3backup dandi verify-blobs` every morning at 11:30. It lists
directories only, so it can run while the blob array is still copying. It fails, and the Check backup
workflow emails its log's tail, when a blob head is missing from its assigned partition or misplaced on
another one. It only lists the top of each partition's `blobs/` directory, never the files. It deletes
nothing: see below to fix a misplaced head. It can also be run by hand:

```bash
s3backup dandi verify-blobs
```


# Partitions

[`src/simple_s3_backup/partitions.json`](src/simple_s3_backup/partitions.json) is the single source of
truth for where the mirror is (`mirror_root`), its partitions, and which partition holds each blob head and
Zarr head (the first hex digit of the blob ID, or of the Zarr ID). A blob is mirrored at
`{mirror_root}/{partition}/s3dandiarchive/blobs/{id[0:3]}/{id[3:6]}/{id}`. The backup, `verify-blobs`,
`cleanup-misplaced-blobs`, `verify-checksums` and the dashboard read it from the installed package, and
dandi-compute-core and dandi-compute-page read the same file, from
`https://raw.githubusercontent.com/dandi/simple-s3-backup/main/src/simple_s3_backup/partitions.json` or from
this checkout on the cluster. To move a head, change it there, and nowhere else.
It also holds `not_in_bucket_grace_days`, how long `verify-checksums` keeps a mirrored blob that is no
longer in the bucket before deleting it, which dandi-compute-runner's report of those blobs reads too.


# Dashboard

`s3backup dandi dashboard` writes the README of [dandi/backup-status](https://github.com/dandi/backup-status)
and, beside it, `data/disk.json`, `data/content.json` and `data/blob_heads.json`. The last is what each blob
head should take, from the bucket, and where it is, from directory listings only, so that nothing has to walk
the mirror's files to know it:

```json
{
  "updated": "2026-11-20T06:12:00-05:00",
  "partitions": {"001": {"size_bytes": 0, "used_bytes": 0}, "002": {"size_bytes": 0, "used_bytes": 0}},
  "heads": [
    {"head": "0", "partition": "001", "remote_bytes": 0, "remote_objects": 0,
     "subdirectories": {"001": 4096, "002": 0}}
  ]
}
```

- `updated`: when it was written, in ISO 8601 with the offset of America/New_York.
- `partitions`: each partition's size and used bytes, from `df`.
- `heads`: one entry per head, `0` to `f` in order: the partition `partitions.json` assigns it, the size
  and object count of `s3://dandiarchive/blobs/{head}*` from `s5cmd du`, and the number of `blobs/{head}??`
  directories on each partition. A head is misplaced when another partition has any of them, and missing
  when its assigned partition has none.

The bucket's `blobs/` is listed once, one head at a time, and its total in the Content table is the sum of
the heads. The mirror's `blobs/` is never walked, on either partition: the Content table gives it no local
figures (`—`), and the Blob Heads table reports what each head should take instead, with whether it is on its
assigned partition. Everything else in the bucket, on partition 001, is small and still measured file by file.
The figures are cached for the day in `/orcd/data/dandi/001/display_cache/`.


# Fixing blob heads stranded on the wrong partition

Before the 001/002 split, heads a-f were backed up into partition `001`; they now route to `002`,
but `s5cmd cp` never deletes, so the old a-f copies stay behind in `001`. `verify-blobs` reports
these as `MISPLACED`. To clean them up safely:

```bash
# 1. Make sure the correct partition holds the data (the nightly backup already does this):
s3backup dandi blobs <task_id>        # for the affected heads, or just let the array run

# 2. Preview what would be deleted (dry run - deletes nothing):
s3backup dandi cleanup-misplaced-blobs

# 3. Actually delete the misplaced copies:
s3backup dandi cleanup-misplaced-blobs --apply
```

The cleanup only deletes a misplaced sub-directory once the correct partition holds a byte-for-byte
complete copy, so no data is lost; anything not yet fully copied is reported and skipped.


# Verifying the content of the blob mirror

The blob backup copies with `s5cmd cp --if-size-differ`, which compares only sizes (blobs never change
once uploaded, so a truncated copy is copied again): a copy of the right size with the wrong content is
never replaced. `verify-checksums` compares the content of every mirrored blob with its object in the
bucket, one head (first hex digit) at a time:

```bash
s3backup dandi verify-checksums <task_id>                   # 0-15, as for `s3backup dandi blobs`
s3backup dandi verify-checksums <task_id> --repair          # also copy each blob that does not match again
s3backup dandi verify-checksums <task_id> --rehash-older-than 30   # re-read every 30 days instead of 90
```

It lists the bucket anonymously for each blob's size and ETag. DANDI uploads every blob in parts of
64 MiB (larger for a file that would otherwise take 10,000 parts or more), so the ETag is the MD5 of the
parts' MD5s followed by `-{number of parts}`. The local copy's ETag is computed the same way and compared.
A blob whose ETag shows other parts than DANDI's is reported as unverifiable, since its part sizes are
unknown.

Computing an ETag reads the whole file, so it is cached beside the blob as `.{blob_id}.checksum`, which
holds the ETag and the date it was computed, in UTC, e.g.
`0123456789abcdef0123456789abcdef-3 2026-11-21T10:00:00+00:00`. The cache file is given the blob's
modification time when it is written, so a blob changed since is read again at the next run. A blob that
keeps its modification time is read again once its checksum is 90 days old (`--rehash-older-than DAYS`
changes that; `0` re-reads everything), which catches what the modification time does not show: damage
below the filesystem, or a write after which the modification time was restored (`touch -d`, `cp -p`,
`rsync -a`). A cache written before the date was stored holds the ETag alone. It is taken as computed on
the day a run first sees it and rewritten with that date, without reading the blob, so these come due 90
days later. Each cache is one more file next to each blob, which doubles the file count of the blob
directories. The dashboard and `cleanup-misplaced-blobs` skip them.

Each run first lists the whole head, in the bucket and on the mirror, and checks every blob it can without
reading it: from its cached checksum, or because its size is wrong. Then it reads the others, in this order:
blobs never read or changed since, then those whose checksum is overdue, oldest first. When there is more to
read than a run gets through, the overdue ones wait, the longest-unread first, and new blobs are never held
up behind them; the cycle simply stretches to what the job can read. The run's summary says how much it
read and how fast (both while reading and over the whole run), how many blobs were never read and how many
overdue (with their size and the oldest date), how many of these it read and how many are still overdue,
and how fast the head would need to be read to read all of it once every interval. The same figures, so far,
are printed every 10 minutes while it reads. A SIGTERM, which SLURM sends at the time limit or on
preemption, stops the run between two reads or within one; it prints its summary, then dies of that signal.

A copy that does not match is deleted, with its cached checksum. The blob backup copies a missing blob
whatever its conditions, so the next one copies it afresh.

A file changed in the last hour may still be being written by the backup, so it is left for the next run.
The command also reports the blobs not mirrored yet and those mirrored but no longer in the bucket. It
exits 1 when part of the bucket could not be listed or a copy that does not match could be neither deleted
nor repaired, otherwise 3 when it deleted a copy that does not match (DANDI Compute's Check backup workflow
reports that exit code as a checksum mismatch, with the blobs named in the log's `[MISMATCH DELETED]` lines), and
otherwise 0.

Every mismatch is also recorded for good in the ledger `/orcd/data/dandi/001/backup/checksum_mismatches.tsv`,
one tab-separated line per event: `time`, `event`, `key`, `path`, `observed`, `expected` and `job` (the
SLURM job, `{array job}_{task}`, whose log has the rest, or `manual`). The events are `etag-mismatch` and
`size-mismatch` when a blob is found not to match, `deleted` or `delete-failed` when its copy is deleted for
the blob backup to copy again, `repaired` or `repair-failed` after `--repair` copies it again, and `resolved`
when a later run finds it matching without that, the backup having copied it again. A mismatch still
unresolved is not recorded again by the runs that find it unchanged, but its copy is deleted, and
`deleted` recorded, every time. The ledger is never
pruned; DANDI Compute's runner publishes it to the `checksum-mismatches` branch of
[its global logs](https://github.com/dandi-compute/dandi-compute-global-logs/blob/checksum-mismatches/checksum-mismatches.tsv).

`verify_checksums_batch_deployment.sh` runs it as a 16-task array, 4 tasks at a time. The first run reads
the whole mirror, which takes longer than the 12-hour limit, but a task that times out or is preempted
keeps what it has cached and the next day's run carries on from there. A task that finds the previous
day's task for its head still running leaves the head to it.

Reading the whole mirror, about 1.34 PB, once every 90 days is about 15 TB a day, or 170 MB/s around the
clock. Whether the job keeps up shows in its summaries: while the number still overdue at the end of each
task stays near 0, it does; while it grows, the job reads each blob less often than every 90 days, and
needs more tasks at once, longer ones, or a longer interval.

## Blobs no longer in the bucket

Mirrored files missing from a fully listed prefix are handled in one of two ways:

- **Partial downloads** (a blob's UUID followed by 1-10 digits) are temporary files that s5cmd leaves
  behind when SLURM kills a backup task. They are removed once unchanged for a day.
- **Blobs deleted from the archive** are kept for 183 days (about 6 months) from when they were first
  found missing, then deleted with their cached checksum. A blob back in the bucket before then is
  forgotten.

The record of the latter is `/orcd/data/dandi/001/backup/not_in_bucket.tsv` (`time`, `event`, `key`,
`path`, `job`; events `missing`, `reappeared`, `deleted`, `gone`), published to the `not-in-bucket` branch of
[DANDI Compute's global logs](https://github.com/dandi-compute/dandi-compute-global-logs/blob/not-in-bucket/not-in-bucket.tsv).
