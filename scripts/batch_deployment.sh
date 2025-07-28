#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 2
#SBATCH --output /orcd/data/dandi/001/backup_logs/myjob.log-%A-%a
#SBATCH --array 0-3

# TODO: adjust range to 4096

# Load Anaconda Module
module load miniforge
source /orcd/data/dandi/001/s3-backup-environment/bin/activate

backup dandi blobs $SLURM_ARRAY_TASK_ID
