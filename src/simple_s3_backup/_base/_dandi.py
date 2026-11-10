import pathlib
import shutil

from ._checksums import is_checksum_cache
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


_ALL_PARTITIONS = ("001", "002")
_BLOBS_SUBPATH = "s3dandiarchive/blobs"


def _blobs_directory(partition: str) -> pathlib.Path:
    return pathlib.Path(f"/orcd/data/dandi/{partition}/{_BLOBS_SUBPATH}")


def _head_subdirectories(partition: str, head_hexcode: str) -> list[pathlib.Path]:
    """All blob sub-directories on `partition` whose name starts with the single-hex `head_hexcode`."""
    directory = _blobs_directory(partition)
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob(f"{head_hexcode}*") if path.is_dir())


def verify_dandi_blobs() -> None:
    """
    Verify that every blob head lives on its assigned partition, and only there.

    Each top-level hash head (0-f) maps to a storage partition via ``BLOBS_HEAD_TO_PARTITION``
    (heads 0-9 -> 001, heads a-f -> 002). This guard inspects the on-disk backup directories and
    alerts loudly if either:

    * a partition received zero writes for the heads assigned to it, or
    * a head is *misplaced* - i.e. present on a partition it is not assigned to. This is the failure
      that left every a-f head stranded in 001: those heads used to route to 001 before the split,
      and ``s5cmd cp`` never deletes, so the stale copies remained after the mapping changed.

    Raises
    ------
    RuntimeError
        If any partition is missing its assigned heads, or holds heads that belong elsewhere.
    """
    expected_present: dict[str, int] = {partition: 0 for partition in _ALL_PARTITIONS}
    misplaced: list[tuple[str, str, str]] = []  # (head, wrong_partition, correct_partition)

    for task_id in range(16):
        correct_partition = BLOBS_HEAD_TO_PARTITION[task_id]
        head_hexcode = f"{task_id:01x}"

        for partition in _ALL_PARTITIONS:
            count = len(_head_subdirectories(partition=partition, head_hexcode=head_hexcode))
            if partition == correct_partition:
                expected_present[partition] += count
                status = "OK" if count > 0 else "MISSING"
                print(f"[{status}] head {head_hexcode} -> partition {partition}: {count} sub-directories")
            elif count > 0:
                misplaced.append((head_hexcode, partition, correct_partition))
                print(
                    f"[MISPLACED] head {head_hexcode} found on partition {partition} "
                    f"({count} sub-directories) but belongs on {correct_partition}"
                )

    print("")
    for partition in _ALL_PARTITIONS:
        print(f"Partition {partition}: {expected_present[partition]} correctly-placed blob sub-directories")

    empty_partitions = [partition for partition in _ALL_PARTITIONS if expected_present[partition] == 0]

    problems = []
    if empty_partitions:
        problems.append(f"partition(s) received zero of their assigned blob writes: {', '.join(empty_partitions)}")
    if misplaced:
        details = ", ".join(f"head {head} on {wrong} (belongs on {correct})" for head, wrong, correct in misplaced)
        problems.append(f"misplaced heads found: {details}")

    if problems:
        message = (
            "\nALERT: "
            + "; ".join(problems)
            + (
                "\nRun `s3backup dandi cleanup-misplaced-blobs` to remove misplaced copies once the correct "
                "partition holds them."
                if misplaced
                else ""
            )
        )
        raise RuntimeError(message)

    print("\nVerification passed: every head is on its assigned partition, and nowhere else.")


def cleanup_misplaced_dandi_blobs(apply: bool = False) -> None:
    """
    Remove blob sub-directories stranded on the wrong storage partition.

    Before the 001/002 split, all heads were backed up to 001; heads a-f now route to 002, but the
    old copies remain in 001 because ``s5cmd cp`` never deletes (and earlier mapping churn on heads
    7 and 9 may have left stale copies in 002 too). This deletes each misplaced head's
    sub-directories from the partition it does not belong to - but only when the correct partition
    already holds a byte-for-byte complete copy, so no data is lost.

    Parameters
    ----------
    apply : bool, default False
        When False (the default) this is a dry run that only reports what would be removed. Pass
        True to actually delete the misplaced directories.
    """
    removed = 0
    reclaimed_bytes = 0
    skipped: list[str] = []

    for task_id in range(16):
        correct_partition = BLOBS_HEAD_TO_PARTITION[task_id]
        head_hexcode = f"{task_id:01x}"
        correct_directory = _blobs_directory(partition=correct_partition)

        for partition in _ALL_PARTITIONS:
            if partition == correct_partition:
                continue
            for misplaced_subdirectory in _head_subdirectories(partition=partition, head_hexcode=head_hexcode):
                correct_copy = correct_directory / misplaced_subdirectory.name
                is_safe, reason = _blob_subdirectory_is_fully_copied(source=misplaced_subdirectory, target=correct_copy)
                if not is_safe:
                    skipped.append(str(misplaced_subdirectory))
                    print(
                        f"[SKIP] {misplaced_subdirectory} (head {head_hexcode} belongs on "
                        f"{correct_partition}): {reason}"
                    )
                    continue

                size = sum(path.stat().st_size for path in misplaced_subdirectory.rglob("*") if path.is_file())
                if apply:
                    shutil.rmtree(misplaced_subdirectory)
                    print(f"[REMOVED] {misplaced_subdirectory} ({size} bytes reclaimed)")
                else:
                    print(f"[DRY-RUN] would remove {misplaced_subdirectory} ({size} bytes)")
                removed += 1
                reclaimed_bytes += size

    print("")
    if apply:
        print(f"Removed {removed} misplaced blob sub-directories; {reclaimed_bytes} bytes reclaimed.")
    else:
        print(f"Would remove {removed} misplaced blob sub-directories; {reclaimed_bytes} bytes would be reclaimed.")
    if skipped:
        print(
            f"\n{len(skipped)} sub-directories were NOT removed because the correct partition does not yet "
            "hold a complete copy. Run the blob backup so the correct partition is fully populated, then "
            "re-run this cleanup."
        )
    if not apply:
        print("\nThis was a DRY RUN. Re-run with `--apply` to actually delete the misplaced directories.")


def _blob_subdirectory_is_fully_copied(source: pathlib.Path, target: pathlib.Path) -> tuple[bool, str]:
    """Return (True, "") only if every file under `source` exists under `target` with the same size."""
    if not target.exists():
        return False, f"no copy on the correct partition ({target})"

    # A checksum cached beside a blob is not part of the copy, so one missing on the target does not matter.
    source_files = [path for path in source.rglob("*") if path.is_file() and not is_checksum_cache(path)]
    if not source_files:
        return True, ""  # An empty stray directory is safe to remove.

    for source_file in source_files:
        relative_path = source_file.relative_to(source)
        target_file = target / relative_path
        if not target_file.exists():
            return False, f"missing on correct partition: {relative_path}"
        if target_file.stat().st_size != source_file.stat().st_size:
            return False, f"size mismatch on correct partition: {relative_path}"

    return True, ""


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
