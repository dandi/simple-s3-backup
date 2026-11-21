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

`s3backup dandi verify-checksums` deletes a mirrored blob that does not match the bucket, with its cached checksum, so that the next blob backup copies it afresh, records `deleted` (or `delete-failed`) in the ledger, and exits 3 when it did, for DANDI Compute's Check backup workflow to report it as a checksum mismatch. The blob backup never replaced a copy of the right size with the wrong content. `--repair` still copies it again at once instead.

`s3backup dandi verify-checksums` now deletes a mirrored blob once it has been missing from the bucket for 183 days (about 6 months), recording when each was first found missing, and what became of it, in `/orcd/data/dandi/001/backup/not_in_bucket.tsv`; a blob back in the bucket is forgotten. It also removes the partial downloads (`{blob_id}{random digits}`) that a blob backup task killed by SLURM leaves behind, once they have not changed for a day, instead of reporting them as not in the bucket. Nothing is deleted under a prefix whose listing failed.

`src/simple_s3_backup/partitions.json` is the single source of truth for the mirror's root, its partitions and the partition of each blob and Zarr head, shipped with the package; the code, dandi-compute-core and dandi-compute-page read it instead of keeping their own copies.

The dashboard lists the bucket's blobs one head at a time with `s5cmd du`, takes the `blobs/` total from their sum instead of listing it a second time, and writes each head's size and object count in the bucket, its assigned partition and its number of sub-directories on each partition to `data/blob_heads.json` of dandi/backup-status, with a Blob Heads table in its README. It no longer walks the mirror's blobs (a `stat` of every file and a `du` of each partition's `blobs/`), so the Content table shows no local figures for `blobs/`. A daily cache written before this is recomputed.

`s3backup dandi verify-blobs` and the dashboard count each head's sub-directories with the same function. `verify-blobs` now also fails when a head is missing from its assigned partition, not only when a whole partition is empty, and runs daily from dandi-compute-runner's `backup.sh verify-blobs` as `DANDI-Verify-Blobs`.

`s3backup dandi verify-checksums` stores, in each cached checksum, the date it was computed (`.{blob_id}.checksum` now holds `{etag} {ISO 8601 date in UTC}`), and reads a blob again once its checksum is 90 days old, even if its modification time has not changed, so that damage below the filesystem, or a write after which the modification time was restored, is found within 3 months. `--rehash-older-than DAYS` now defaults to 90, is measured from that date rather than from the cache file's change time, and still reads a blob again at once when its modification time changes. A cache written before holds the ETag alone: it is still read, taken as computed on the day a run first sees it, and rewritten with that date without reading the blob. Each run now lists the whole head first, checks every blob it can from its cache (or by its size), then reads the others: those never read, or changed since, first, then the overdue ones, oldest first. Its summary, and a progress line every 10 minutes, give the bytes read and the read rate, how many blobs were never read and how many overdue, with their size, how many are still overdue, and the rate at which the head would have to be read to read all of it every 90 days. A SIGTERM (SLURM's time limit or preemption) stops it with its summary printed.

## Fixes

The blob backup copies with `s5cmd cp --if-size-differ` alone, without `--if-source-newer`. s5cmd lets the last of these conditions decide, so the local copy, always newer than its object in the bucket, was never copied again, even when truncated. Blobs never change once uploaded, so a size that differs is enough to copy one again.

The blob backup and verify jobs turn on `set -u` only after `module load` and `conda activate`. With it first, the cluster's module scripts stopped every task within a second on the unset `MODULEPATH`, so no blob was backed up from 2026-06-25 until this fix.
