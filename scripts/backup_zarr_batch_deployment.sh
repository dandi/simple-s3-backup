#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --output /dev/null
#SBATCH --array 0-255

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

s3backup dandi blobs $SLURM_ARRAY_TASK_ID
