#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /orcd/data/dandi/001/backup_logs/clean_logs_directory.log-%A

find /orcd/data/dandi/001/backup_logs/ -type f -name "*.log-*" -mtime +3 -exec rm {} \;

# On the MIT cluster, set this up to run once a week (at 3am on Sunday) via:
# crontab -e
# 0 3 * * 0 flock -n /orcd/data/dandi/001/backup_logs/clean_logs.lock sbatch  \
# /orcd/data/dandi/001/simple-s3-backup/scripts/clean_logs_directory_deployment.sh
