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
```

`backup.sh` names the jobs (`DANDI-Backup-Blobs`, `DANDI-Backup-Nonblobs`, `DANDI-Update-Backup-Dashboard`, `DANDI-Verify-Checksums`),
keeps the per-kind `flock` around `sbatch`, and records each submission in DANDI Compute's global logs. It
passes `--output` and `--error` itself, which take precedence over the `#SBATCH` ones in these scripts, so
each job's output goes to `/orcd/data/dandi/001/backup/logs/{job name}-{job ID}.log` (or
`{job name}-{array job}_{task}.log`). The runner's Check backup workflow emails when one of these jobs fails
or the blob mirror stops growing.

The scripts are run from cron's environment, which sets neither `MODULEPATH` nor most other variables. Each
turns on `set -u` only after `module load` and `conda activate`, since the cluster's module scripts read
`MODULEPATH` and bash would otherwise stop the job at once.

`verify_blobs_backup_deployment.sh` is not scheduled. To run the partition check after a blob backup, queue
it behind the array, or run it by hand:

```bash
sbatch --dependency=afterany:<array job ID> /orcd/data/dandi/001/backup/simple-s3-backup/scripts/verify_blobs_backup_deployment.sh
```

```bash
s3backup dandi verify-blobs
```

Submitted on its own, it writes its logs to `/orcd/data/dandi/001/backup/logs/`, which must exist
(`backup.sh` creates it).


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

The blob backup copies with `s5cmd cp --if-size-differ --if-source-newer`, which compares only sizes and
times: a copy of the right size with the wrong content is never replaced. `verify-checksums` compares the
content of every mirrored blob with its object in the bucket, one head (first hex digit) at a time:

```bash
s3backup dandi verify-checksums <task_id>                   # 0-15, as for `s3backup dandi blobs`
s3backup dandi verify-checksums <task_id> --repair          # also copy each blob that does not match again
s3backup dandi verify-checksums <task_id> --rehash-older-than 180   # re-read files checked over 180 days ago
```

It lists the bucket anonymously for each blob's size and ETag. DANDI uploads every blob in parts of
64 MiB (larger for a file that would otherwise take 10,000 parts or more), so the ETag is the MD5 of the
parts' MD5s followed by `-{number of parts}`. The local copy's ETag is computed the same way and compared.
A blob whose ETag shows other parts than DANDI's is reported as unverifiable, since its part sizes are
unknown.

Computing an ETag reads the whole file, so it is cached beside the blob as `.{blob_id}.checksum`, a JSON
file with the ETag and the size and modification time it was computed for. A later run trusts it while
the file keeps that size and modification time, so only blobs copied or changed since are read again.
`--rehash-older-than DAYS` re-reads files whose checksum was cached longer ago than that, to catch damage
on disk that leaves the modification time alone; `0` re-reads everything. Each cache is one more file next
to each blob, which doubles the file count of the blob directories. The dashboard and
`cleanup-misplaced-blobs` skip them.

A file changed in the last hour may still be being written by the backup, so it is left for the next run.
The command also reports the blobs not mirrored yet and those mirrored but no longer in the bucket. It
exits non-zero when a blob does not match (and `--repair` did not fix it) or part of the bucket could not
be listed.

`verify_checksums_batch_deployment.sh` runs it as a 16-task array, 4 tasks at a time. The first run reads
the whole mirror, which takes longer than the 12-hour limit, but a task that times out or is preempted
keeps what it has cached and the next day's run carries on from there. A task that finds the previous
day's task for its head still running leaves the head to it.
