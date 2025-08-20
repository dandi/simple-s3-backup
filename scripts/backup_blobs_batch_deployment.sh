#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /dev/null
#SBATCH --array 0-11

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

backup dandi blobs $SLURM_ARRAY_TASK_ID

# On the MIT cluster, set this up to run twice a day (midnight and noon) via:
# crontab -e
# 0 0 * * * flock -n /orcd/data/dandi/001/flocks/backup_blobs_batch.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/backup_blobs_batch_deployment.sh
