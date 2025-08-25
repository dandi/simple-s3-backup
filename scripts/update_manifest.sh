#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --output /dev/null

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

s3backup dandi manifest

# crontab -e
# 0 */6 * * * flock -n /orcd/data/dandi/001/flocks/update_manifest.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_manifest.sh
