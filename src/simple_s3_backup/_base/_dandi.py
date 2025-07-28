import pathlib
import subprocess


def backup_dandi_nonblobs() -> None:
    backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive.cody")

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
        _deploy_subprocess(command=command)


def backup_dandi_blobs(task_id: int) -> None:
    backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive.cody")

    blob_subdirectory = f"{task_id:03x}"
    source = f"s3://dandiarchive/blobs/{blob_subdirectory}/*"
    destination = f"{backup_directory}/{blob_subdirectory}/"
    command = f"s5cmd cp --if-size-differ --if-source-newer {source} {destination}"
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
