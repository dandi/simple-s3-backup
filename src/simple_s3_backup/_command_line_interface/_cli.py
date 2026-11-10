import click

from .._base import (
    backup_dandi_blobs,
    backup_dandi_nonblobs,
    backup_dandi_zarr,
    cleanup_misplaced_dandi_blobs,
    update_display,
    update_manifest,
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
