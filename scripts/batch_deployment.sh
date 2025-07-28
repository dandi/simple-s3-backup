#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /orcd/data/dandi/001/backup_logs/myjob.log-%A-%a
#SBATCH --array 0-256

module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

backup dandi blobs $SLURM_ARRAY_TASK_ID
