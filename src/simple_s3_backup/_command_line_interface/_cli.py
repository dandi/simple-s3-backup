import click

from .._base import backup_dandi_blobs, backup_dandi_nonblobs, update_display, update_manifest


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


# s3backup dandi dashboard
@_s3backup_dandi.command(name="dashboard")
def _s3backup_dandi_dashboard() -> None:
    """
    Pretty rendering for summary of current backup status.
    """
    update_display()
