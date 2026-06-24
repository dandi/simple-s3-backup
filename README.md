# Simple S3 Backup

Simple scripts for running backup on MIT cluster.

Monitor current status at: https://github.com/dandi/backup-status

See also https://github.com/dandi/s3invsync for a more complicated approach that may be necessary for handling large Zarr stores.



# Dashboard

The backup status dashboard is available at https://github.com/dandi/backup-status. It is updated daily via the `update_dashboard` cron job (see below).

## Blob Partition Separation

Blobs are distributed across two disk partitions (`001` and `002`) based on the first hexadecimal digit of each blob's head hash (task IDs 0–15, corresponding to hex digits `0`–`f`):

| Partition | Head hash digits (hex) | Task IDs |
|-----------|------------------------|----------|
| `001`     | `0`–`9`                | 0–9      |
| `002`     | `a`–`f`                | 10–15    |

Zarr stores are stored entirely on partition `002` regardless of head hash.

The `blobs <int>` subcommand accepts a task ID (0–15) and backs up all blobs whose three-character hex prefix starts with that digit (e.g., task ID `5` covers prefixes `500`–`5ff`). This allows 16 parallel backup jobs to run independently.

The dashboard aggregates blob statistics from both partitions when reporting totals.

# Crontab

```bash
0 22 * * * flock -n /orcd/data/dandi/001/backup/flocks/backup_blobs_batch.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_blobs_batch_deployment.sh
#0 19 * * 0,3 flock -n /orcd/data/dandi/001/backup/flocks/backup_zarr_batch.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_zarr_batch_deployment.sh  # Zarr was disabled in May 2026
0 18 * * * flock -n /orcd/data/dandi/001/backup/flocks/backup_nonblobs.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/backup_nonblobs_deployment.sh
0 6 * * * flock -n /orcd/data/dandi/001/backup/flocks/update_dashboard.lock sbatch /orcd/data/dandi/001/backup/simple-s3-backup/scripts/update_dashboard.sh
```
