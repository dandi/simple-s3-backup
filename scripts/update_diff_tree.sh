#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /dev/null

cd /orcd/data/dandi/001/backup-status
git pull
mkdir diffs
for x in {0..9} {a..b}; do
for x in {0..1}; do
  mkdir diffs/$x
  s5cmd ls s3://dandiarchive/blobs/$x* | awk '{print $4, $3}' | sort > diffs/$x/s5cmd.txt
  find /orcd/data/dandi/001/s3dandiarchive/blobs/$x* -type f -exec du -b {} + | awk '{split($2, object_key_split, "/"); print object_key_split[8] "/" object_key_split[9] "/" object_key_split[10], $1}' | sort > diffs/$x/du.txt
  diff diffs/$x/s5cmd.txt diffs/$x/du.txt | grep -v '^[0-9]*a[0-9,]*' > diffs/$x/diff.txt
done
git commit --message "update"
git push

# On the MIT cluster, set this up to run once a day (6am) via:
# crontab -e
# 0 6 * * * flock -n /orcd/data/dandi/001/backup_logs/update_dashboard.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_dashboard.sh
