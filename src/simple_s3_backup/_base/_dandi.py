import pathlib

from ._globals import BLOBS_HEAD_TO_PARTITION, ZARR_HEAD_TO_PARTITION
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
    partition = BLOBS_HEAD_TO_PARTITION[task_id]
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


def verify_dandi_blobs() -> None:
    """
    Verify that *both* storage partitions received blob writes after a backup run.

    Each SLURM array task ``task_id`` in 0-15 maps to a single top-level hash head (0-f) and to a
    storage partition via ``BLOBS_HEAD_TO_PARTITION`` (heads 0-9 -> 001, heads a-f -> 002). This guard
    inspects the on-disk backup directories and alerts loudly if an entire partition received zero
    writes - the exact failure mode where array tasks 10-15 (which map to partition 002) never ran.

    Raises
    ------
    RuntimeError
        If any partition received zero blob sub-directories, i.e. its array tasks did not run.
    """
    per_head_subdirectory_counts: dict[int, int] = {}
    per_partition_subdirectory_counts: dict[str, int] = {}

    for task_id in range(16):
        partition = BLOBS_HEAD_TO_PARTITION[task_id]
        per_partition_subdirectory_counts.setdefault(partition, 0)

        blobs_backup_directory = pathlib.Path(f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs")
        top_blob_hexcode = f"{task_id:01x}"

        matching_subdirectories = (
            [path for path in blobs_backup_directory.glob(f"{top_blob_hexcode}*") if path.is_dir()]
            if blobs_backup_directory.exists()
            else []
        )
        count = len(matching_subdirectories)
        per_head_subdirectory_counts[task_id] = count
        per_partition_subdirectory_counts[partition] += count

        status = "OK" if count > 0 else "MISSING"
        print(f"[{status}] head {top_blob_hexcode} -> partition {partition}: {count} sub-directories")

    print("")
    for partition, count in sorted(per_partition_subdirectory_counts.items()):
        print(f"Partition {partition}: {count} total blob sub-directories")

    empty_partitions = sorted(
        partition for partition, count in per_partition_subdirectory_counts.items() if count == 0
    )
    empty_heads = [f"{task_id:01x}" for task_id in range(16) if per_head_subdirectory_counts[task_id] == 0]

    if empty_partitions:
        message = (
            f"\nALERT: the following partition(s) received zero blob writes: {', '.join(empty_partitions)}.\n"
            f"Heads with no sub-directories: {empty_heads}.\n"
            "This almost always means the corresponding SLURM array tasks did not run."
        )
        raise RuntimeError(message)

    if empty_heads:
        # An individual empty head is suspicious but may be legitimate (no blobs with that prefix yet),
        # so it is only a warning as long as the partition as a whole received writes.
        print(f"\nWARNING: the following heads have zero sub-directories: {empty_heads}")

    print("\nVerification passed: every partition received blob writes.")


def backup_dandi_zarr(task_id: int) -> None:
    top_zarr_hexcode = f"{task_id:02x}"
    partition_key = int(top_zarr_hexcode[0], 16)
    partition = ZARR_HEAD_TO_PARTITION[partition_key]
    zarr_backup_directory = pathlib.Path(f"/orcd/data/dandi/{partition}/s3dandiarchive/zarr")

    for sub_zarr_hexcode_1 in range(16):
        zarr_subdirectory = f"{top_zarr_hexcode}{sub_zarr_hexcode_1:01x}"
        source = f"s3://dandiarchive/zarr/{zarr_subdirectory}*"
        destination = zarr_backup_directory  # No nested structure yet
        command = f"s5cmd cp --if-size-differ --if-source-newer {source} {destination}"
        print(command)
        _deploy_subprocess(command=command, ignore_errors=True)
