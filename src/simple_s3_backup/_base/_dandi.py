import pathlib

from ._globals import BLOBS_TASK_ID_TO_PARTITION
from ._utils import _deploy_subprocess


def backup_dandi_nonblobs() -> None:
    backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive")

    ls_command = "s5cmd ls s3://dandiarchive"
    ls_output = _deploy_subprocess(command=ls_command, ignore_errors=True)

    skip_keys = ("blobs", "zarr", "dandiarchive")
    ls_locations = [
        line.split(" ")[-1] for line in ls_output.splitlines() if not any(skip_key in line for skip_key in skip_keys)
    ]

    for location in ls_locations:
        source = f"{location}*" if location.endswith("/") else location
        destination = f"{backup_directory}/{location}"
        command = f"s5cmd cp --if-size-differ --if-source-newer s3://dandiarchive/{source} {destination}"
        print(command)
        _deploy_subprocess(command=command, ignore_errors=True)


def backup_dandi_blobs(task_id: int) -> None:
    partition = BLOBS_TASK_ID_TO_PARTITION[task_id]
    blobs_backup_directory = pathlib.Path(f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs")

    top_blob_hexcode = f"{task_id:01x}"
    for sub_blob_hexcode_1 in range(16):
        for sub_blob_hexcode_2 in range(16):
            blob_subdirectory = f"{top_blob_hexcode}{sub_blob_hexcode_1:01x}{sub_blob_hexcode_2:01x}"
            source = f"s3://dandiarchive/blobs/{blob_subdirectory}/*"
            destination = f"{blobs_backup_directory}/{blob_subdirectory}/"
            command = f"s5cmd cp --if-size-differ --if-source-newer {source} {destination}"
            print(command)
            _deploy_subprocess(command=command, ignore_errors=True)


def backup_dandi_zarr(task_id: int) -> None:
    top_zarr_hexcode = f"{task_id:02x}"
    partition_key = int(top_zarr_hexcode[0], 16)
    partition = BLOBS_TASK_ID_TO_PARTITION[partition_key]
    zarr_backup_directory = pathlib.Path(f"/orcd/data/dandi/{partition}/s3dandiarchive/zarr")

    for sub_zarr_hexcode_1 in range(16):
        zarr_subdirectory = f"{top_zarr_hexcode}{sub_zarr_hexcode_1:01x}"
        source = f"s3://dandiarchive/zarr/{zarr_subdirectory}*"
        destination = zarr_backup_directory  # No nested structure yet
        command = f"s5cmd cp --if-size-differ --if-source-newer {source} {destination}"
        print(command)
        _deploy_subprocess(command=command, ignore_errors=True)
