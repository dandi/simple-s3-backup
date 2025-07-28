#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 2
#SBATCH --output /orcd/data/dandi/001/backup_logs/myjob.log-%A-%a
#SBATCH --array 1000-1003

# TODO: adjust range to 4096

module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

backup dandi blobs $SLURM_ARRAY_TASK_ID
