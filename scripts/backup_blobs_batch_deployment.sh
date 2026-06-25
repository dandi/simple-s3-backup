#!/bin/bash
#SBATCH --job-name=DANDI-Backup-Blobs
#SBATCH --partition mit_preemptable
#SBATCH --requeue
#SBATCH --array 0-15
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --time=12:00:00
#SBATCH --open-mode=append
#SBATCH --output=/orcd/data/dandi/001/backup/logs/backup_blobs_batch_%A_%a.out
#SBATCH --error=/orcd/data/dandi/001/backup/logs/backup_blobs_batch_%A_%a.err

# NOTE on partition choice and reliability:
#   `mit_preemptable` is cheap but jobs can be preempted (killed) at any time, and SLURM places
#   array tasks in ascending index order. Previously this job ran without `--requeue`, so any task
#   that was preempted - or that never won a slot on the contended preemptable partition - was lost
#   silently. The high-index tasks (10-15, which map to storage partition 002) are scheduled last
#   and were the ones that never ran. The `--requeue` directive above now resubmits preempted tasks
#   automatically. If preemption keeps causing misses, move this job to a non-preemptable partition.
#
#   The per-task `--output`/`--error` logs (one file per array task) make any future failure visible.
#   The log directory must exist before submission; create it once with:
#       mkdir -p /orcd/data/dandi/001/backup/logs

set -uo pipefail

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/name-s3+backup_env

task_id="${SLURM_ARRAY_TASK_ID}"
echo "$(date --iso-8601=seconds) starting backup_blobs task ${task_id} (job ${SLURM_ARRAY_JOB_ID:-?} on $(hostname))"

lock_file="/orcd/data/dandi/001/backup/flocks/backup_blobs_batch_${task_id}.lock"

# Acquire the per-task lock on a dedicated file descriptor. We still use non-blocking `flock`, but
# unlike the previous `flock -n <cmd>` form (which silently skipped the work when the lock was held),
# we now FAIL LOUDLY with a logged error and a non-zero exit code if the lock cannot be acquired.
exec {lock_fd}>"${lock_file}"
if ! flock --nonblocking "${lock_fd}"; then
    echo "$(date --iso-8601=seconds) ERROR task ${task_id}: could not acquire lock ${lock_file}; another run is still active. Refusing to skip silently." >&2
    exit 1
fi

s3backup dandi blobs "${task_id}"
status=$?

if [ "${status}" -ne 0 ]; then
    echo "$(date --iso-8601=seconds) ERROR task ${task_id}: 's3backup dandi blobs ${task_id}' exited with status ${status}" >&2
else
    echo "$(date --iso-8601=seconds) completed backup_blobs task ${task_id}"
fi

exit "${status}"
