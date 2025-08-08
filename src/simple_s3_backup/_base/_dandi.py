import collections
import datetime
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
            du_command = f"s5cmd du s3://dandiarchive/{location}"
            du_output = _deploy_subprocess(command=du_command)
            du_output_split = du_output.split(" ")

            remote_size_in_bytes = int(du_output_split[0])
            object_count = int(du_output_split[3])

            outer_directory_to_remote_size[location] = remote_size_in_bytes
            outer_directory_to_remote_object_count[location] = object_count

        outer_directory_to_local_size = dict()
        outer_directory_to_local_object_count = dict()
        for location in outer_ls_locations:
            local_path = backup_directory / location
            local_size_in_bytes, local_object_count = _get_local_size_in_bytes_and_object_count(path=local_path)

            outer_directory_to_local_size[location] = remote_size_in_bytes
            outer_directory_to_local_object_count[location] = local_object_count

        with daily_cache_file_path.open(mode="w", encoding="utf-8") as file_stream:
            cache_data = {
                "outer_directory_to_remote_size": outer_directory_to_remote_size,
                "outer_directory_to_remote_object_count": outer_directory_to_remote_object_count,
                "outer_directory_to_local_size": outer_directory_to_local_size,
                "outer_directory_to_local_object_count": outer_directory_to_local_object_count,
            }
            file_stream.write(yaml.dump(cache_data, allow_unicode=True, sort_keys=False))
    else:
        with daily_cache_file_path.open(mode="r", encoding="utf-8") as file_stream:
            cache_data = yaml.safe_load(stream=file_stream)
        outer_directory_to_remote_size = cache_data["outer_directory_to_remote_size"]
        outer_directory_to_remote_object_count = cache_data["outer_directory_to_remote_object_count"]
        outer_directory_to_local_size = cache_data["outer_directory_to_local_size"]
        outer_directory_to_local_object_count = cache_data["outer_directory_to_local_object_count"]

    print(f"\n\nCurrent status of S3 bucket backup of 'dandiarchive' as of {today}:")
    header = f"{'Location':<20} {'Size (Bytes)':<31} {'Number of Objects':<31}"
    print(header)
    print("=" * len(header))
    for location in outer_ls_locations:
        remote_size = outer_directory_to_remote_size[location]
        remote_object_count = outer_directory_to_remote_object_count[location]
        local_size = outer_directory_to_local_size[location]
        local_object_count = outer_directory_to_local_object_count[location]

        print(f"{location:<20} {remote_size:<15}/{local_size:<15} {remote_object_count:<15}/{local_object_count:<20}")
    print("\n\n")


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
        sizes = list(subpath.stat().st_size for subpath in path.rglob(pattern="*") if subpath.is_file())
        return sum(sizes), len(sizes)
