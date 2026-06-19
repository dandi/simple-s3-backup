#!/bin/bash
#SBATCH --job-name=DANDI-Update-Backup-Dashboard
#SBATCH --mem=1GB
#SBATCH --cpus-per-task 1
#SBATCH --partition=mit_preemptable
#SBATCH --time=12:00:00

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge
conda activate /orcd/data/dandi/001/environments/s3-backup-environment

cd /orcd/data/dandi/001/backup/backup-status
git pull
flock -n /orcd/data/dandi/001/backup/flocks/update_dashboard.lock sbatch s3backup dandi dashboard
git add .
git commit --message "update"
git push
