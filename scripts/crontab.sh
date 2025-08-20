# On the MIT cluster, setup scheduled runs via:
# `crontab -e` (press `i` to edit content, then `Esc` to exit insert mode, and `:wq` to save and quit)

0 0 * * * flock -n /orcd/data/dandi/001/flocks/backup_blobs_batch.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_blobs_batch_deployment.sh
0 0 * * * flock -n /orcd/data/dandi/001/flocks/backup_nonblobs.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_nonblobs_deployment.sh
0 6 * * * flock -n /orcd/data/dandi/001/flocks/update_dashboard.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_dashboard.sh
