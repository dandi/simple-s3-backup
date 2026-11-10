import click

from .._base import (
    backup_dandi_blobs,
    backup_dandi_nonblobs,
    backup_dandi_zarr,
    cleanup_misplaced_dandi_blobs,
    update_display,
    update_manifest,
    verify_dandi_blob_checksums,
    verify_dandi_blobs,
)


# s3backup
@click.group(name="s3backup")
def _s3backup():
    pass


# s3backup dandi
@_s3backup.group(name="dandi")
def _s3backup_dandi() -> None:
    """
    Backup DANDI bucket content.
    """
    pass


# s3backup dandi manifest
@_s3backup_dandi.command(name="manifest")
@click.option(
    "--limit",
    type=int,
    required=False,
    default=None,
)
def _s3backup_dandi_manifest(limit: int | None = None) -> None:
    """
    Form the latest manifest of what assets require backup.
    """
    update_manifest(limit=limit)


# s3backup dandi nonblobs
@_s3backup_dandi.command(name="nonblobs")
def _s3backup_dandi_nonblobs() -> None:
    """
    Backup all DANDI bucket non-blob directories.
    """
    backup_dandi_nonblobs()


# s3backup dandi blobs <int>
@_s3backup_dandi.command(name="blobs")
@click.argument("task_id", type=int)
def _s3backup_dandi_blobs(task_id: int) -> None:
    """
    Backup DANDI blob directories correspond to the `task_id`.
    """
    backup_dandi_blobs(task_id=task_id)


# s3backup dandi zarr <int>
@_s3backup_dandi.command(name="zarr")
@click.argument("task_id", type=int)
def _s3backup_dandi_zarr(task_id: int) -> None:
    """
    Backup DANDI zarr directories correspond to the `task_id`.
    """
    backup_dandi_zarr(task_id=task_id)


# s3backup dandi verify-blobs
@_s3backup_dandi.command(name="verify-blobs")
def _s3backup_dandi_verify_blobs() -> None:
    """
    Verify each blob head is on its assigned partition and nowhere else; exit non-zero otherwise.
    """
    verify_dandi_blobs()


# s3backup dandi verify-checksums <int>
@_s3backup_dandi.command(name="verify-checksums")
@click.argument("task_id", type=click.IntRange(0, 15))
@click.option(
    "--rehash-older-than",
    "rehash_older_than_days",
    type=click.FloatRange(min=0),
    default=None,
    metavar="DAYS",
    help=(
        "Read a file again when its checksum was cached more than DAYS days ago, even if the file has not "
        "changed since; 0 reads every file again. By default a cached checksum is trusted while the file keeps "
        "the modification time it had."
    ),
)
@click.option(
    "--repair",
    is_flag=True,
    default=False,
    help="Copy a blob again from the bucket when its local copy does not match.",
)
def _s3backup_dandi_verify_checksums(
    task_id: int, rehash_older_than_days: float | None = None, repair: bool = False
) -> None:
    """
    Verify the content of the mirrored blobs whose ID starts with hex digit `task_id` against the bucket.

    Each blob's ETag is computed as DANDI uploads it and compared with the ETag of its object in the bucket.
    The ETag is cached beside the blob in `.{blob_id}.checksum`. Each mismatch, and what became of it, is
    recorded in the ledger /orcd/data/dandi/001/backup/checksum_mismatches.tsv. Exits non-zero if a blob does
    not match (and was not repaired) or part of the bucket could not be listed.
    """
    counts = verify_dandi_blob_checksums(task_id=task_id, rehash_older_than_days=rehash_older_than_days, repair=repair)
    if counts.unrepaired or counts.unlisted_prefixes:
        raise SystemExit(1)


# s3backup dandi cleanup-misplaced-blobs
@_s3backup_dandi.command(name="cleanup-misplaced-blobs")
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Actually delete the misplaced directories. Without this flag the command is a dry run.",
)
def _s3backup_dandi_cleanup_misplaced_blobs(apply: bool = False) -> None:
    """
    Remove blob heads stranded on the wrong partition (e.g. a-f left in 001 before the 001/002 split).

    Safe by default: a directory is only deleted once the correct partition holds a complete copy.
    """
    cleanup_misplaced_dandi_blobs(apply=apply)


# s3backup dandi dashboard
@_s3backup_dandi.command(name="dashboard")
def _s3backup_dandi_dashboard() -> None:
    """
    Pretty rendering for summary of current backup status.
    """
    update_display()
