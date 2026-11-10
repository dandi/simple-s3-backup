#!/bin/bash
#SBATCH --job-name=DANDI-Verify-Checksums
#SBATCH --partition mit_preemptable
#SBATCH --requeue
#SBATCH --array 0-15%4
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --time=12:00:00
#SBATCH --open-mode=append
#SBATCH --output=/orcd/data/dandi/001/backup/logs/verify_checksums_batch_%A_%a.out
#SBATCH --error=/orcd/data/dandi/001/backup/logs/verify_checksums_batch_%A_%a.err

# Verifies the content of the blob mirror against the bucket, one task per first hex digit of the
# content ID, as the blob backup copies it (see `s3backup dandi verify-checksums --help`). Each blob's
# checksum is cached beside it, so a task that times out or is preempted loses nothing: the next run
# reads only what is left. The first run reads the whole mirror and takes several; later ones read
# only the blobs copied since. At most 4 tasks run at once, so as not to take the partitions' whole
# read bandwidth from the jobs that use them.
#
# The task exits non-zero when a blob does not match the bucket, which the runner's Check backup
# workflow reports. To copy those blobs again from the bucket:
#     s3backup dandi verify-checksums <task_id> --repair
#
# The log directory must exist before submission; create it once with:
#     mkdir -p /orcd/data/dandi/001/backup/logs

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/name-s3+backup_env

# Only after the setup above, which reads variables cron does not set (see
# backup_blobs_batch_deployment.sh).
set -uo pipefail

task_id="${SLURM_ARRAY_TASK_ID}"
echo "$(date --iso-8601=seconds) starting verify_checksums task ${task_id} (job ${SLURM_ARRAY_JOB_ID:-?} on $(hostname))"

# A task of the previous day's array can still be verifying the same head, during the first runs
# especially. Two would only read the same files, so this one leaves it to that one.
lock_file="/orcd/data/dandi/001/backup/flocks/verify_checksums_batch_${task_id}.lock"
exec {lock_fd}>"${lock_file}"
if ! flock --nonblocking "${lock_fd}"; then
    echo "$(date --iso-8601=seconds) task ${task_id}: another run still holds ${lock_file} and is verifying this head; leaving it to that one."
    exit 0
fi

s3backup dandi verify-checksums "${task_id}"
status=$?

if [ "${status}" -ne 0 ]; then
    echo "$(date --iso-8601=seconds) ERROR task ${task_id}: 's3backup dandi verify-checksums ${task_id}' exited with status ${status}; the summary above names each blob that does not match." >&2
else
    echo "$(date --iso-8601=seconds) completed verify_checksums task ${task_id}"
fi

exit "${status}"
