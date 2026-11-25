#!/bin/bash
#SBATCH --job-name=DANDI-Backup-Blobs
#SBATCH --partition mit_preemptable
#SBATCH --requeue
#SBATCH --array 0-15
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --time=48:00:00
#SBATCH --open-mode=append
#SBATCH --output=/orcd/data/dandi/001/backup/logs/backup_blobs_batch_%A_%a.out
#SBATCH --error=/orcd/data/dandi/001/backup/logs/backup_blobs_batch_%A_%a.err

# NOTE on partition choice and reliability:
#   `mit_preemptable` is cheap but jobs can be preempted (killed) at any time, and SLURM places
#   array tasks in ascending index order. Previously this job ran without `--requeue`, so any task
#   that was preempted - or that never won a slot on the contended `mit_preemptable` partition - was lost
#   silently. The high-index tasks (10-15, which map to storage partition 002) are scheduled last
#   and were the ones that never ran. The `--requeue` directive above now resubmits preempted tasks
#   automatically. If preemption keeps causing misses, move this job to a non-preemptable partition.
#
# NOTE on the time limit:
#   48 hours, the most `mit_preemptable` allows. s5cmd downloads each blob into a temporary file
#   (`{blob}{random digits}`) and cannot resume one, so whatever a task is still downloading when
#   SLURM kills it is thrown away, and the next run starts that blob again from zero. At 12 hours every
#   task of every array was killed at its limit, and a blob too large to finish in what was left of
#   12 hours was never mirrored (on 2026-11-21 the partial-download cleanup removed about 148 TB of
#   such files from partition 001). dandi-compute-runner submits this array every day at 22:00, but
#   not while a job of the same name is pending or running, so an array now runs for up to 48 hours
#   and the next is submitted at the first 22:00 after it ends.
#   Preemption still kills a task's downloads in flight: the requeued task starts again from its
#   first prefix, and what it was downloading is downloaded again from zero.
#
#   The per-task `--output`/`--error` logs (one file per array task) make any future failure visible.
#   The log directory must exist before submission; create it once with:
#       mkdir -p /orcd/data/dandi/001/backup/logs

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/name-s3+backup_env

# Only after the setup above: the cluster's module scripts read MODULEPATH, which cron does not
# set, and under `set -u` bash stops the job at the first such read. With `set -u` first, every
# task exited with status 1 within a second, from 2026-06-25 until this was moved.
set -uo pipefail

task_id="${SLURM_ARRAY_TASK_ID}"
echo "$(date --iso-8601=seconds) starting backup_blobs task ${task_id} (job ${SLURM_ARRAY_JOB_ID:-?} on $(hostname))"

lock_file="/orcd/data/dandi/001/backup/flocks/backup_blobs_batch_${task_id}.lock"

# Acquire the per-task lock on a dedicated file descriptor. Unlike the previous `flock -n <cmd>` form
# (which silently skipped the work when the lock was held), we FAIL LOUDLY with a logged error and a
# non-zero exit code if the lock cannot be acquired.
#
# We wait up to lock_wait_seconds for it first. The runner submits no array while one is pending or
# running, but a task that SLURM has just killed (at its time limit or on preemption) can hold its
# lock for a little while as it completes, which the runner does not count as running. A 48-hour
# array started at 22:00 reaches its limit just as the 22:00 submission two days later is due, and a
# requeued task can start while its killed predecessor is still completing.
lock_wait_seconds=900
exec {lock_fd}>"${lock_file}"
if ! flock --wait "${lock_wait_seconds}" "${lock_fd}"; then
    echo "$(date --iso-8601=seconds) ERROR task ${task_id}: could not acquire lock ${lock_file} within ${lock_wait_seconds} seconds; another run is still active. Refusing to skip silently." >&2
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
