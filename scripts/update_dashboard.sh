#!/bin/bash

#SBATCH --partition mit_normal
#SBATCH --cpus-per-task 1
#SBATCH --output /dev/null

source /etc/profile.d/modules.sh  # When run via crontab, this is needed to load the modules
module load miniforge

conda activate /orcd/data/dandi/001/s3-backup-environment

cd /orcd/data/dandi/001/backup-status
git pull
mkdir diffs
for x in {0..1}; do
  mkdir diffs/$x
  s5cmd --dry-run cp --if-size-differ s3://dandiarchive/blobs/$x* . | awk '{print $3}' | sort > diffs/$x/size_difference.txt
  # mtimes on MIT cluster are date of copy from bucket, not mtime of file on bucket
  # TODO: test if mtime on bucket is upload time or actual file mtime
  s5cmd --dry-run cp --if-source-newer s3://dandiarchive/blobs/$x* . | awk '{print $3}' | sort > diffs/$x/newer_on_bucket.txt
done
backup dandi dashboard
git add README.md
git commit --message "update"
git push

# On the MIT cluster, set this up to run once a day (6am) via:
# crontab -e
# 0 6 * * * flock -n /orcd/data/dandi/001/flocks/update_dashboard.lock sbatch /orcd/data/dandi/001/simple-s3-backup/scripts/update_dashboard.sh
