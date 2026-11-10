#!/bin/bash
#SBATCH --job-name=DANDI-Verify-Blobs-Backup
#SBATCH --partition mit_preemptable
#SBATCH --requeue
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --time=1:00:00
#SBATCH --open-mode=append
#SBATCH --output=/orcd/data/dandi/001/backup/logs/verify_blobs_backup_%j.out
#SBATCH --error=/orcd/data/dandi/001/backup/logs/verify_blobs_backup_%j.err

# Post-run guard for the blob backup array job. Checks that BOTH storage partitions (001 and 002)
# received blob writes and exits non-zero (with a loud ALERT) if any partition got zero - the exact
# bug where array tasks 10-15 (partition 002) never ran. Intended to be submitted with a SLURM
# dependency on the backup array job so it runs only after every task has finished; see README.
#
# The log directory must exist before submission; create it once with:
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
    echo "$(date --iso-8601=seconds) ALERT: blob backup verification FAILED with status ${status}; at least one partition received zero writes." >&2
fi

exit "${status}"
