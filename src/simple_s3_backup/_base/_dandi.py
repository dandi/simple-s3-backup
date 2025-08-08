import pathlib
import subprocess


def display_current_status(use_cache: bool = True) -> None:
    # backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive")
    #
    # outer_ls_command = "s5cmd ls s3://dandiarchive"
    # outer_ls_output = _deploy_subprocess(command=outer_ls_command)
    #
    # # For each of these outer directories, recurse their contents and make a mirror of
    # today = datetime.date.today().isoformat()
    # filename = f"{today}.yaml"
    # cache_directory = backup_directory.parent / "display_cache"
    # cache_directory.mkdir(exist_ok=True)
    # collections.deque(
    #     (path.unlink() for path in cache_directory.iterdir() if path.name != filename),
    #     maxlen=0,
    # )
    # daily_cache_file_path = cache_directory / filename
    #
    # if use_cache is False or not daily_cache_file_path.exists():
    #     skip_outer_keys = ("blobs", "zarr", "dandiarchive")
    #     outer_ls_locations = [
    #         line.split(" ")[-1]
    #         for line in ls_output.splitlines()
    #         if not any(skip_key in line for skip_key in skip_outer_keys)
    #     ]
    #     outer_directory_to_remote_size = dict()
    #     outer_directory_to_remote_number_of_objects = dict()
    #     for location in outer_ls_locations:
    #         du_command = f"s5cmd du s3://dandiarchive/{source}"
    #         du_output = _deploy_subprocess(command=du_command)
    #         du_output_split = du_output.splitlines()
    #
    #         remote_size_in_bytes = int(du_output_split[0])
    #         number_of_objects = int(du_output_split[3])
    #
    #         outer_directory_to_remote_size[location] = remote_size_in_bytes
    #         outer_directory_to_number_of_objects[location] = number_of_objects
    #
    #     outer_directory_to_local_size = dict()
    #     outer_directory_to_local_number_of_objects = dict()
    #     for location in outer_ls_locations:
    #         local_size_in_bytes = backup_directory / location
    #
    #         outer_directory_to_local_size[location] = remote_size_in_bytes
    #         outer_directory_to_local_number_of_objects[location] = number_of_objects
    #
    #     with daily_cache_file_path.open(mode="w", encoding="utf-8") as file_stream:
    #         cache_data = {
    #             "outer_directory_to_remote_size": outer_directory_to_remote_size,
    #             "outer_directory_to_remote_number_of_objects": outer_directory_to_remote_number_of_objects,
    #         }
    #         file_stream.write(yaml.dump(cache_data, allow_unicode=True, sort_keys=False))
    # else:
    #     with daily_cache_file_path.open(mode="r", encoding="utf-8") as file_stream:
    #         cache_data = yaml.safe_load(stream=file_stream)
    #     outer_directory_to_remote_size = cache_data["outer_directory_to_remote_size"]
    #     outer_directory_to_remote_number_of_objects = cache_data["outer_directory_to_remote_number_of_objects"]
    #
    # # nicely print this to console by iterating over the outer directories (display rather like du)
    # print(f"Current status of S3 bucket backup of 'dandiarchive' as of {today}:")
    # print(f"{'Location':<50} {'Remote Size':<15} {'Local Size':<15} {'Number of Objects':<20}")
    # print("=" * 100)
    # for location in outer_ls_locations:
    #     remote_size = outer_directory_to_remote_size.get(location, 0)
    #     number_of_objects = outer_directory_to_number_of_objects.get(location, 0)
    #
    #     # Calculate local size
    #     destination = backup_directory / location
    #     local_size_output = subprocess.run(
    #         ["du", "-sh", str(destination)],
    #         capture_output=True,
    #         text=True,
    #         encoding="utf-8",
    #     ).stdout.strip()
    #     local_size = local_size_output.split()[0] if local_size_output else "0"
    #
    #     print(f"{location:<50} {remote_size:<15} {local_size:<15} {number_of_objects:<20}")
    pass


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
