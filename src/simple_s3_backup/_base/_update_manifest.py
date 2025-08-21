import collections
import functools
import hashlib
import json
import time
import yaml

from ._utils import _deploy_subprocess

def update_manifest() -> None:
    """Update the manifest file."""
    manifests_directory = pathlib.Path("/orcd/data/dandi/001/manifests")
    manifests_directory.mkdir(exist_ok=True)

    s5cmd_ls_blobs_file_path = manifests_directory / "s5cmd_ls_blobs.txt"
    s5cmd_ls_needs_update = (
        not manifest_path.exists() or
        (time.time() - manifest_path.stat().st_mtime) > 86_400
    )
    if s5cmd_ls_needs_update is True:
        command = f"s5cmd ls s3://dandiarchive/blobs/* > {s5cmd_ls_blobs_file_path}"
        print(f"Updating local `s5cmd ls` copy!\n{command}")
        _deploy_subprocess(command=command)

    remote_checksums_file_path = manifests_directory / "remote_checksums.json"
    remote_checksum_needs_update = (
        not remote_checksums_file_path.exists() or
        (time.time() - remote_checksums_file_path.stat().st_mtime) > 86_400
    )
    if remote_checksum_needs_update is True:
        # TODO for Kitware
        pass
        # command = f"s5cmd ls s3://dandiarchive/blobs/* > {s5cmd_ls_blobs_file_path}"
        # print(f"Updating local `s5cmd ls` copy!\n{command}")
        # _deploy_subprocess(command=command)

    remote_blob_id_to_info: dict[str, dict[str, str]] = dict()
    with s5cmd_ls_blobs_file_path as file_stream:
        collections.deque(
            (_process_s5cmd_ls_line(line=line.strip(), info=remote_blob_id_to_info) for line in file_stream.readlines()),
            maxlen=0,
        )

    remote_checksums_file_path = manifests_directory / "remote_checksums.json"
    with remote_checksums_file_path.open(mode="r") as file_stream:
        remote_blob_id_to_checksum: dict[str, str] = json.load(fp=file_stream)

    local_checksums_file_path = manifests_directory / "local_checksums.json"
    with local_checksums_file_path.open(mode="r") as file_stream:
        local_blob_id_to_checksum: dict[str, str] = json.load(fp=file_stream)

    problematic_blob_ids_file_path = manifests_directory / "problematic_blob_ids.yaml"
    with problematic_blob_ids_file_path.open(mode="r") as file_stream:
        problematic_blob_ids: dict[str, str] = yaml.safe_load(stream=file_stream)

    blob_ids_to_update = []
    blobs_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive/blobs")
    for blob_id, info in remote_blob_id_to_info.items():
        local_blob_file_path = blobs_directory / blob_id[:3] / blob_id[3:6] / blob_id

        # Case 1: Local copy of blob on remote does not exist - always download
        if local_blob_file_path.exists() is False:
            blob_ids_to_update.append(blob_id)
            continue

        # Case 2: Local copy of blob exists, but mtime on remote is newer
        # Keep in mind that local mtime is when the file was first downloaded
        # Compare the local and remote checksums
        local_mtime = int(local_blob_file_path.stat().st_mtime)
        if local_mtime <= info["mtime"]:
            local_checksum = local_blob_id_to_checksum.get(blob_id, None)
            remote_checksum = remote_blob_id_to_checksum.get(blob_id, None)

            if local_checksum is None:
                local_checksum = _calculate_checksum(file_path=local_blob_file_path)
                local_blob_id_to_checksum[blob_id] = local_checksum

            if remote_checksum is None:
                problematic_blob_ids[blob_id] = "Remote checksum is missing."
                continue

            # Case 2a: Local content does not match remote - mark local copy for removal and download from remote
            if local_checksum != remote_checksum:
                new_path = local_blob_file_path.parent / f"{local_blob_file_path.name}.rmv.0"
                local_blob_file_path.rename(new_path)

                blob_ids_to_update.append(blob_id)
            # Case 2b: It is a question why the mtimes differ so add this to the problematic blob list
            else:
                problematic_blob_ids[blob_id] = "Remote mtime is newer than local mtime, but checksums match."
            continue

        # Case 3: Local mtime is after remote mtime, but size differs
        # If local size is less than remote, this can only mean the first attempt to download the asset failed
        # If local size is greater than remote, then something is very wrong so add it to the problematic blob list
        local_size = local_blob_file_path.stat().st_size
        if local_size <= info["size"]:
            new_path = local_blob_file_path.parent / f"{local_blob_file_path.name}.rmv.180"
            local_blob_file_path.rename(new_path)

            blob_ids_to_update.append(blob_id)
        else:
            problematic_blob_ids[blob_id] = "Local size is greater than remote size, but mtime is older."

        # Case 4: Local mtime is after remote mtime, size matches, so ensure the checksums match
        # If they do not, mark the local copy for removal and download from remote
        # Unclear why this would happen - possible corruption that occurred locally (or conceptually, remotely)
        # TODO: perhaps add warning to the dashboard if this occurs so we can investigate
        local_checksum = local_blob_id_to_checksum.get(blob_id, None)
        remote_checksum = remote_blob_id_to_checksum.get(blob_id, None)

        if local_checksum is None:
            local_checksum = _calculate_checksum(file_path=local_blob_file_path)
            local_blob_id_to_checksum[blob_id] = local_checksum

        if remote_checksum is None:
            problematic_blob_ids[blob_id] = "Remote checksum is missing."
            continue

        if local_checksum != remote_checksum:
            new_path = local_blob_file_path.parent / f"{local_blob_file_path.name}.rmv.0"
            local_blob_file_path.rename(new_path)

            blob_ids_to_update.append(blob_id)

    blobs_to_update_file_path = manifests_directory / "blobs_to_update.txt"
    blobs_to_update_file_path.write_text("\n".join(blob_ids_to_update))

    with local_checksums_file_path.open(mode="w") as file_stream:
        json.dump(obj=local_blob_id_to_checksum, fp=file_stream, indent=1)

    with problematic_blob_ids_file_path.open(mode="w") as file_stream:
        file_stream.write("\n".join(sorted(problematic_blob_ids)))


def _process_s5cmd_ls_line(line: str, info: dict) -> None:
    """
    Process a line from the `s5cmd ls` output.

    Parameters
    ----------
    line : str
        A line from the `s5cmd ls` output.

    Returns
    -------
    tuple[str, int, int]
        A tuple containing the blob ID, size in bytes, and modification time.
    """
    parts = line.split("\t")

    blob_path = parts[3]
    blobs_id = blob_path.split("/")[-1]

    size = int(parts[2])
    mtime = int(parts[1])

    info[blobs_id] = {"size": size, "mtime": mtime}

@functools.lru_cache(maxsize=None)
def _calculate_checksum(file_path: pathlib.Path) -> str:
    """
    Calculate the SHA-256 checksum of a file.

    Processes the file in 10 MiB chunks to avoid memory issues with large files.

    Parameters
    ----------
    file_path : pathlib.Path
        The path to the file.

    Returns
    -------
    str
        The SHA-256 checksum of the file.
    """
    chunk_size_in_bytes = 10_485_760  # 10 MiB

    hasher = hashlib.sha256()
    with file_path.open(mode="rb") as file_stream:
        for chunk in iter(lambda: file_stream.read(chunk_size_in_bytes), b""):
            hasher.update(chunk)
    hex = hasher.hexdigest()
    return hex