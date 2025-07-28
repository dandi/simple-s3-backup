import click

from .._base import backup_dandi_blobs, backup_dandi_nonblobs


# backup
@click.group(name="backup")
def _main():
    pass


# backup dandi
@_main.group(name="dandi")
def _dandi() -> None:
    """
    Backup DANDI bucket content.
    """
    pass


# backup dandi nonblobs
@_dandi.command(name="nonblobs")
def _backup_dandi_nonblobs() -> None:
    """
    Backup all DANDI bucket non-blob directories.
    """
    backup_dandi_nonblobs()


# backup dandi blobs <int>
@_dandi.command(name="blobs")
@click.argument("task_id", type=int)
def _backup_dandi_blobs(task_id: int) -> None:
    """
    Backup DANDI blob directories correspond to the `task_id`.
    """
    backup_dandi_blobs(task_id=task_id)
