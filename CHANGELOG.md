# CHANGELOG

# Upcoming

## Features

Added CLI entrypoint `backup dandi` with subcommands `nonblobs` and `blobs <int>` to backup all non-blob (and non-Zarr) and blob subdirectories (still non-Zarr) objects, respectively.

Added simple display through `backup dandi display` to show the current backup status of the S3 bucket.
