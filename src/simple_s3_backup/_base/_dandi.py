import collections
import datetime
import math
import pathlib
import subprocess

import yaml


def display_current_status(use_cache: bool = True) -> None:
    backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive")

    outer_ls_command = "s5cmd ls s3://dandiarchive"
    outer_ls_output = _deploy_subprocess(command=outer_ls_command)

    today = datetime.date.today().isoformat()
    filename = f"{today}.yaml"
    cache_directory = backup_directory.parent / "display_cache"
    cache_directory.mkdir(exist_ok=True)
    collections.deque(
        (path.unlink() for path in cache_directory.iterdir() if path.name != filename),
        maxlen=0,
    )
    daily_cache_file_path = cache_directory / filename

    if use_cache is False or not daily_cache_file_path.exists():
        skip_outer_keys = ("blobs", "zarr", "dandiarchive")
        outer_ls_locations = [
            line.split(" ")[-1]
            for line in outer_ls_output.splitlines()
            if not any(skip_key in line for skip_key in skip_outer_keys)
        ]

        outer_directory_to_remote_size = dict()
        outer_directory_to_remote_object_count = dict()
        for location in outer_ls_locations:
            du_command = f"s5cmd du s3://dandiarchive/{location}*"
            du_output = _deploy_subprocess(command=du_command)
            du_output_split = du_output.split(" ")

            remote_size_in_bytes = int(du_output_split[0])
            remote_object_count = int(du_output_split[3])

            outer_directory_to_remote_size[location] = remote_size_in_bytes
            outer_directory_to_remote_object_count[location] = remote_object_count

        outer_directory_to_local_size = dict()
        outer_directory_to_local_object_count = dict()
        for location in outer_ls_locations:
            local_path = backup_directory / location.removesuffix("/")
            local_size_in_bytes, local_object_count = _get_local_size_in_bytes_and_object_count(path=local_path)

            outer_directory_to_local_size[location] = local_size_in_bytes
            outer_directory_to_local_object_count[location] = local_object_count

        cache_data = {
            "outer_directory_to_remote_size": outer_directory_to_remote_size,
            "outer_directory_to_remote_object_count": outer_directory_to_remote_object_count,
            "outer_directory_to_local_size": outer_directory_to_local_size,
            "outer_directory_to_local_object_count": outer_directory_to_local_object_count,
        }
        with daily_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=cache_data, stream=file_stream)
    else:
        with daily_cache_file_path.open(mode="r") as file_stream:
            cache_data = yaml.safe_load(stream=file_stream)

        outer_directory_to_remote_size = cache_data["outer_directory_to_remote_size"]
        outer_directory_to_remote_object_count = cache_data["outer_directory_to_remote_object_count"]
        outer_directory_to_local_size = cache_data["outer_directory_to_local_size"]
        outer_directory_to_local_object_count = cache_data["outer_directory_to_local_object_count"]

        outer_ls_locations = list(outer_directory_to_remote_size.keys())

    padding = (20, 40, 40)
    print(f"\n\nCurrent status of S3 bucket backup of 'dandiarchive' as of {today}\n")
    print(f"{'Location':<{padding[0]}} {'Size':<{padding[1]}} {'Number of Objects':<{padding[2]}}")
    print(f"{"":<{padding[0]}} {'Local / Remote (%)':<{padding[1]}} {"Local / Remote (%)":<{padding[2]}}")
    print("=" * sum(padding))
    for location in outer_ls_locations:
        local_size = outer_directory_to_local_size[location]
        remote_size = outer_directory_to_remote_size[location]

        local_object_count = outer_directory_to_local_object_count[location]
        remote_object_count = outer_directory_to_remote_object_count[location]

        human_sizes = [_human_readable_size(size_in_bytes=size) for size in (local_size, remote_size)]
        size_ratio = _format_ratio(numerator=local_size, denominator=remote_size)
        size_string = f"{human_sizes[0]} / {human_sizes[1]} ({size_ratio})"

        object_count_ratio = _format_ratio(numerator=local_object_count, denominator=remote_object_count)
        object_count_string = f"{local_object_count} / {remote_object_count} ({object_count_ratio})"

        print(f"{location:<{padding[0]}} {size_string:<{padding[1]}} {object_count_string:<{padding[2]}}")
    print("\n")
    print("Note: reported percentage may exceed 100% due to delayed garbage collection.")
    print("\n")


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
        _deploy_subprocess(command=command)


def backup_dandi_blobs(task_id: int) -> None:
    blobs_backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive/blobs")

    top_blob_hexcode = f"{task_id:02x}"
    for sub_blob_hexcode in range(16):
        blob_subdirectory = top_blob_hexcode + f"{sub_blob_hexcode:01x}"
        source = f"s3://dandiarchive/blobs/{blob_subdirectory}/*"
        destination = f"{blobs_backup_directory}/{blob_subdirectory}/"
        command = f"s5cmd cp --if-size-differ --if-source-newer {source} {destination}"
        print(command)
        _deploy_subprocess(command=command)


def _deploy_subprocess(
    *,
    command: str | list[str],
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
) -> str | None:
    error_message = error_message or "An error occurred while executing the command."

    result = subprocess.run(
        args=command,
        env=environment_variables,
        shell=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0 and ignore_errors is False:
        message = (
            f"\n\nError code {result.returncode}\n"
            f"{error_message}\n\n"
            f"stdout: {result.stdout}\n\n"
            f"stderr: {result.stderr}\n\n"
        )
        raise RuntimeError(message)
    if result.returncode != 0 and ignore_errors is True:
        return None

    return result.stdout


def _get_local_size_in_bytes_and_object_count(path: pathlib.Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0

    if path.is_file():
        return path.stat().st_size, 1

    if path.is_dir():
        sizes = [subpath.stat().st_size for subpath in path.rglob(pattern="*") if subpath.is_file()]
        count_correction = len([path for subpath in path.rglob(pattern="*") if not subpath.is_file()])
        return sum(sizes), len(sizes) + count_correction


def _format_ratio(numerator: int, denominator: int) -> str:
    ratio = f"{numerator / denominator:.2%}"
    if ratio == "100%" and numerator != denominator:
        ratio = "99.99%"
    return ratio


def _human_readable_size(size_in_bytes: int, binary: bool = False) -> str:
    """
    Convert a file size given in bytes to a human-readable format using division
    and remainder instead of iteration.

    Parameters
    ----------
    size_in_bytes : int
        The size in bytes.
    binary : bool, default=False
        If True, use binary prefixes (KiB, MiB, etc.). If False, use SI prefixes (KB, MB, etc.).

    Returns
    -------
    str
        A human-readable string representation of the size.

    Examples
    --------
    >>> human_readable_size(123)
    '123 B'
    >>> human_readable_size(1234, binary=True)
    '1.21 KiB'
    >>> human_readable_size(123456789)
    '123.46 MB'
    """
    # Check if size is negative
    if size_in_bytes < 0:
        raise ValueError("Size must be non-negative")

    if size_in_bytes == 0:
        return "0 B"

    # Define the suffixes for each size unit
    suffixes = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]

    # Calculate base and the exponent
    base = 1024 if binary else 1000
    exponent = int(math.log(size_in_bytes, base))

    if exponent == 0:
        return f"{size_in_bytes} B"

    # Calculate the human-readable size
    human_readable_value = size_in_bytes / (base**exponent)

    # Return formatted size with suffix
    return f"{human_readable_value:.2f} {suffixes[exponent]}{'i' if binary else ''}B"
