# Simple S3 Backup

Simple scripts for running backup on MIT cluster.

Monitor current status at: https://github.com/dandi/backup-status

See also https://github.com/dandi/s3invsync for a more complicated approach that may be necessary for handling large Zarr stores.



# Crontab

On the MIT cluster, set up scheduled runs via:
`crontab -e` (press `i` to edit content, then `Esc` to exit insert mode, and `:wq` to save and quit)

```bash
0 22 * * * flock -n /orcd/data/dandi/001/flocks/backup_blobs_batch.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_blobs_batch_deployment.sh
#0 19 * * 0,3 flock -n /orcd/data/dandi/001/flocks/backup_zarr_batch.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_zarr_batch_deployment.sh  # Zarr was disabled in May 2026
0 18 * * * flock -n /orcd/data/dandi/001/flocks/backup_nonblobs.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_nonblobs_deployment.sh
0 6 * * * flock -n /orcd/data/dandi/001/flocks/update_dashboard.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_dashboard.sh
```
