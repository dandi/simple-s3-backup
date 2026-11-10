# CHANGELOG

# Upcoming

## Features

Added CLI entrypoint `backup dandi` with subcommands `nonblobs` and `blobs <int>` to backup all non-blob (and non-Zarr) and blob subdirectories (still non-Zarr) objects, respectively.

Added simple display through `backup dandi dashboard` to show the current backup status of the S3 bucket.

Updated the timestamp in the backup status dashboard to display in a human-readable format (e.g., "January 15, 2024 at 02:30 PM ET") instead of ISO 8601 format.

Switched markdown table generation in the backup status dashboard from a custom implementation to `tabulate2`.

Added `s3backup dandi verify-blobs`, which checks that every blob head is on its assigned partition and only there, and `verify_blobs_backup_deployment.sh` to run it as a job.

Added `s3backup dandi cleanup-misplaced-blobs [--apply]`, which removes blob heads left on the wrong partition once the right one holds a complete copy.

The blob backup job requeues preempted tasks, logs each task, and fails loudly when its lock is held.

Added `s3backup dandi verify-checksums <task_id> [--repair] [--rehash-older-than DAYS]`, which compares the content of each mirrored blob with the ETag of its object in the bucket, caching each blob's ETag beside it in `.{blob_id}.checksum` and recording every mismatch and its outcome in the ledger `/orcd/data/dandi/001/backup/checksum_mismatches.tsv`, and `verify_checksums_batch_deployment.sh` to run it for every head as a job. The dashboard and `cleanup-misplaced-blobs` skip the cached checksums.

## Fixes

The blob backup and verify jobs turn on `set -u` only after `module load` and `conda activate`. With it first, the cluster's module scripts stopped every task within a second on the unset `MODULEPATH`, so no blob was backed up from 2026-06-25 until this fix.
