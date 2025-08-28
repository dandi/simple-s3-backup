#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --output /dev/null

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

cd /orcd/data/dandi/001/backup-status
git pull
s3backup dandi dashboard
git add .
git commit --message "update"
git push
