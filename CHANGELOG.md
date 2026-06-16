# CHANGELOG

# Upcoming

## Features

Added CLI entrypoint `backup dandi` with subcommands `nonblobs` and `blobs <int>` to backup all non-blob (and non-Zarr) and blob subdirectories (still non-Zarr) objects, respectively.

Added simple display through `backup dandi dashboard` to show the current backup status of the S3 bucket.

Updated the timestamp in the backup status dashboard to display in a human-readable format (e.g., "January 15, 2024 at 02:30 PM ET") instead of ISO 8601 format.

Switched markdown table generation in the backup status dashboard from a custom implementation to `tabulate2`.
