#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /orcd/data/dandi/001/backup_logs/update_dashboard.log-%A

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

cd /orcd/data/dandi/001/backup-status
git pull
backup dandi dashboard
git add README.md
git commit --message "update"
git push

# On the MIT cluster, set this up to run once a day (6am) via:
# crontab -e
# 0 6 * * * flock -n /orcd/data/dandi/001/backup_logs/update_dashboard.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_dashboard.sh
