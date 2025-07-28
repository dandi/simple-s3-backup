#!/bin/bash

#SBATCH -p mit_normal
#SBATCH -o /orcd/data/dandi/001/backup_logs/myjob.log-%A-%a
#SBATCH -a 1000-1003

# TODO: adjust range to 4096

# Load Anaconda Module
module load miniforge
source /orcd/data/dandi/001/s3-backup-environment/bin/activate

backup dandi blobs $SLURM_ARRAY_TASK_ID
