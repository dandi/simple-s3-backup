# Simple S3 Backup

Simple scripts for running backup on MIT cluster.

Monitor current status at: https://github.com/dandi/backup-status

See also https://github.com/dandi/s3invsync for a more complicated approach that may be necessary for handling large Zarr stores.



# Crontab

```bash
0 22 * * * flock -n /orcd/data/dandi/001/backup/flocks/backup_blobs_batch.lock bash -c 'jid=$(sbatch --parsable /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_blobs_batch_deployment.sh) && sbatch --dependency=afterany:${jid} /orcd/data/dandi/001/backup/simple-s3-backup/scripts/verify_blobs_backup_deployment.sh'
#0 19 * * 0,3 flock -n /orcd/data/dandi/001/backup/flocks/backup_zarr_batch.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_zarr_batch_deployment.sh  # Zarr was disabled in May 2026
0 18 * * * flock -n /orcd/data/dandi/001/backup/flocks/backup_nonblobs.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_nonblobs_deployment.sh
0 6 * * * flock -n /orcd/data/dandi/001/backup/flocks/update_dashboard.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/update_dashboard.sh
```

The blob-backup line submits the 16-task array job and then queues `verify_blobs_backup_deployment.sh`
with a `afterany` dependency, so verification runs once every array task has finished and alerts (via a
non-zero exit and a logged `ALERT`) if either storage partition received zero writes. Note the outer
`flock` only guards the *submission* (`sbatch` returns as soon as the array is queued); it does not
constrain how many of the 16 array tasks actually run.

One-time setup on the cluster (the per-task `--output`/`--error` log directory must exist before jobs
are submitted):

```bash
mkdir -p /orcd/data/dandi/001/backup/logs
```

You can also run the partition check by hand at any time:

```bash
s3backup dandi verify-blobs
```


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
