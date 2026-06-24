#!/bin/bash
#SBATCH --partition mit_preemptable
#SBATCH --mem=100MB
#SBATCH --cpus-per-task 1
#SBATCH --time=12:00:00

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/name-s3+backup_env

flock -n /orcd/data/dandi/001/backup/flocks/backup_nonblobs_batch.lock s3backup dandi nonblobs $SLURM_ARRAY_TASK_ID
