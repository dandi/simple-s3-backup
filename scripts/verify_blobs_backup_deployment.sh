#!/bin/bash
#SBATCH --job-name=DANDI-Verify-Blobs
#SBATCH --partition mit_preemptable
#SBATCH --requeue
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --time=1:00:00
#SBATCH --open-mode=append
#SBATCH --output=/orcd/data/dandi/001/backup/logs/verify_blobs_backup_%j.out
#SBATCH --error=/orcd/data/dandi/001/backup/logs/verify_blobs_backup_%j.err

# Daily check that every blob head is on its assigned partition (partitions.json) and nowhere else.
# dandi-compute-runner's `launcher/backup.sh verify-blobs` submits it each morning, after the nightly
# blob array, as DANDI-Verify-Blobs. It exits non-zero (with a loud ALERT) when a head is missing from
# its assigned partition or misplaced on another one, and the runner's Check backup workflow emails the
# failure with the log's tail. Nothing is deleted here; misplaced copies are removed by hand with
# `s3backup dandi cleanup-misplaced-blobs [--apply]` (see README).
#
# backup.sh passes --output and --error itself. Submitted on its own, the log directory must exist
# first; create it once with:
#     mkdir -p /orcd/data/dandi/001/backup/logs

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/name-s3+backup_env

# Only after the setup above, which reads variables cron does not set (see
# backup_blobs_batch_deployment.sh).
set -uo pipefail

flock -n /orcd/data/dandi/001/backup/flocks/verify_blobs_backup.lock s3backup dandi verify-blobs
status=$?

if [ "${status}" -ne 0 ]; then
    echo "$(date --iso-8601=seconds) ALERT: blob placement verification FAILED with status ${status}; a blob head is missing from its assigned partition or misplaced on another one." >&2
fi

exit "${status}"
