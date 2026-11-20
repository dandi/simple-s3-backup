import collections
import datetime
import json
import math
import pathlib
import zoneinfo

import yaml
from tabulate2 import tabulate

from ._checksums import is_checksum_cache
from ._dandi import blob_head_placement, count_blob_head_subdirectories
from ._globals import BLOBS_HEAD_TO_PARTITION, MIRROR_ROOT, PARTITIONS
from ._utils import _deploy_subprocess, _get_today

#: The checkout of dandi/backup-status, which the dashboard job commits and pushes.
STATUS_DIRECTORY = MIRROR_ROOT / "001" / "backup" / "backup-status"

#: The partition that holds everything in the bucket but blobs and Zarr.
_NONBLOBS_PARTITION = "001"

#: What a daily cache must hold to be used; one written before a key was added is recomputed instead.
_CACHE_KEYS = (
    "partitions",
    "outer_directory_to_remote_size",
    "outer_directory_to_remote_object_count",
    "outer_directory_to_local_size",
    "outer_directory_to_local_object_count",
    "blob_head_to_remote_size",
    "blob_head_to_remote_object_count",
    "blob_head_to_subdirectories",
)


def update_display(use_cache: bool = True) -> None:
    """
    Update the README file of the DANDI backup status tracking repository.
    """
    data = _load_data(use_cache=use_cache)

    partition_to_disk = data["partitions"]

    outer_directory_to_remote_size = data["outer_directory_to_remote_size"]
    outer_directory_to_remote_object_count = data["outer_directory_to_remote_object_count"]
    outer_directory_to_local_size = data["outer_directory_to_local_size"]
    outer_directory_to_local_object_count = data["outer_directory_to_local_object_count"]

    outer_ls_locations = list(outer_directory_to_remote_size.keys())
    outer_ls_locations.sort(key=lambda path: (not path.endswith("/"), path))

    now_datetime = datetime.datetime.now().astimezone(zoneinfo.ZoneInfo(key="America/New_York"))
    now = now_datetime.strftime("%B %-d, %Y at %I:%M %p ET")
    readme_lines = [
        "# DANDI Backup Status",
        "",
        "Current status of S3 bucket backup of the DANDI Archive.",
        "",
        f"Last update: {now}",
        "",
    ]

    data_directory = STATUS_DIRECTORY / "data"
    partitions = list(partition_to_disk)
    disk_space_json = {
        "subtitle": "Disk Space",
        "data": {
            "Partition": partitions,
            "Size (Used / Total)": [
                (
                    f"{_human_readable_size(size_in_bytes=(used := partition_to_disk[partition]['used_bytes']))} / "
                    f"{_human_readable_size(size_in_bytes=(total := partition_to_disk[partition]['size_bytes']))} "
                    f"({_format_ratio(numerator=used, denominator=total)})"
                )
                for partition in partitions
            ],
        },
    }
    with (data_directory / "disk.json").open(mode="w") as file_stream:
        json.dump(obj=disk_space_json, fp=file_stream)

    readme_lines += json_to_markdown_table(json_table=disk_space_json)
    readme_lines += ["", "", ""]

    content_json = {
        "subtitle": "Content",
        "tails": [
            "[^1]: Reported percentage may exceed 100% due to delayed garbage collection.",
            "",
            "The mirror's blobs/ is not walked, so it has no local figures: see Blob Heads for what each head "
            "should take and whether it is on its assigned partition.",
        ],
        "data": {
            "Location": outer_ls_locations,
            "Size (Local / Remote)": [
                _format_local_and_remote(
                    local=outer_directory_to_local_size.get(location),
                    remote=outer_directory_to_remote_size[location],
                    formatter=_human_readable_size,
                )
                for location in outer_ls_locations
            ],
            "Number of Objects (Local / Remote)[^1]": [
                _format_local_and_remote(
                    local=outer_directory_to_local_object_count.get(location),
                    remote=outer_directory_to_remote_object_count[location],
                    formatter=str,
                )
                for location in outer_ls_locations
            ],
        },
    }
    with (data_directory / "content.json").open(mode="w") as file_stream:
        json.dump(obj=content_json, fp=file_stream)

    readme_lines += json_to_markdown_table(json_table=content_json)

    readme_lines += ["", "", ""]
    readme_lines += json_to_markdown_table(json_table=_build_blob_partition_json())

    blob_heads_json = _build_blob_heads_json(data=data, updated=now_datetime)
    with (data_directory / "blob_heads.json").open(mode="w") as file_stream:
        json.dump(obj=blob_heads_json, fp=file_stream, indent=2)

    readme_lines += ["", "", ""]
    readme_lines += json_to_markdown_table(json_table=_build_blob_heads_table_json(blob_heads_json=blob_heads_json))

    readme = "\n".join(readme_lines)
    with (STATUS_DIRECTORY / "README.md").open(mode="w") as file_stream:
        file_stream.write(readme)


def _build_blob_heads_json(data: dict, updated: datetime.datetime) -> dict:
    """
    What each blob head should take, from the bucket, and where it is, from directory listings only.

    Published as data/blob_heads.json for DANDI Compute's page, so that nothing needs to walk the mirror.
    """
    heads = []
    for task_id in range(16):
        subdirectories = data["blob_head_to_subdirectories"][task_id]
        heads.append(
            {
                "head": f"{task_id:x}",
                "partition": BLOBS_HEAD_TO_PARTITION[task_id],
                "remote_bytes": data["blob_head_to_remote_size"][task_id],
                "remote_objects": data["blob_head_to_remote_object_count"][task_id],
                "subdirectories": {partition: subdirectories.get(partition, 0) for partition in PARTITIONS},
            }
        )
    return {
        "updated": updated.isoformat(timespec="seconds"),
        "partitions": {
            partition: {"size_bytes": disk["size_bytes"], "used_bytes": disk["used_bytes"]}
            for partition, disk in data["partitions"].items()
        },
        "heads": heads,
    }


def _build_blob_heads_table_json(blob_heads_json: dict) -> dict:
    placements = []
    for entry in blob_heads_json["heads"]:
        missing, misplaced_on = blob_head_placement(
            task_id=int(entry["head"], 16), subdirectories=entry["subdirectories"]
        )
        problems = (["missing"] if missing else []) + [f"misplaced on {partition}" for partition in misplaced_on]
        placements.append(", ".join(problems) if problems else "OK")

    return {
        "subtitle": "Blob Heads",
        "headers": [
            "Size and object count in the bucket of each head (first hex digit of the blob ID), and whether "
            "its sub-directories are on its assigned partition only."
        ],
        "data": {
            "Head": [f"`{entry['head']}`" for entry in blob_heads_json["heads"]],
            "Partition": [entry["partition"] for entry in blob_heads_json["heads"]],
            "Bucket Size": [_human_readable_size(entry["remote_bytes"]) for entry in blob_heads_json["heads"]],
            "Objects": [entry["remote_objects"] for entry in blob_heads_json["heads"]],
            "Placement": placements,
        },
    }


def _build_blob_partition_json() -> dict:
    partition_to_hex_digits = collections.defaultdict(list)
    for task_id, partition in BLOBS_HEAD_TO_PARTITION.items():
        partition_to_hex_digits[partition].append(f"{task_id:x}")

    partitions = sorted(partition_to_hex_digits)
    return {
        "subtitle": "Blob Partition Separation",
        "headers": ["Blobs are distributed across partitions based on the first hex digit of each blob's head hash."],
        "data": {
            "Partition": partitions,
            "Head Hash Digits (hex)": [", ".join(f"`{d}`" for d in partition_to_hex_digits[p]) for p in partitions],
        },
    }


def json_to_markdown_table(json_table: dict) -> list[str]:
    """
    Convert a JSON object to a Markdown table using tabulate2.

    Parameters
    ----------
    json_table : dict
        The JSON data to convert. Supports optional keys: "title", "subtitle", "headers", "tails",
        and required key "data" (dict mapping column names to lists of values).

    Returns
    -------
    list[str]
        Lines representing the Markdown table with optional title, subtitle, headers, and tails.
    """
    title = json_table.get("title", None)
    subtitle = json_table.get("subtitle", None)
    headers = json_table.get("headers", None)
    tails = json_table.get("tails", None)

    data = json_table["data"]
    column_names = list(data.keys())
    rows = list(zip(*(data[column_name] for column_name in column_names)))

    markdown_table = []
    if title is not None:
        markdown_table += [f"# {title}", ""]
    if subtitle is not None:
        markdown_table += [f"## {subtitle}", ""]
    if headers is not None:
        markdown_table += headers
        markdown_table += [""]

    table_str = tabulate(rows, headers=column_names, tablefmt="github")
    markdown_table += table_str.splitlines()

    if tails is not None:
        markdown_table += [""]
        markdown_table += tails

    return markdown_table


def _load_data(use_cache: bool = True) -> dict:
    cache_directory = MIRROR_ROOT / _NONBLOBS_PARTITION / "display_cache"
    cache_directory.mkdir(exist_ok=True)

    today = _get_today()
    filename = f"{today}.yaml"

    collections.deque(
        (path.unlink() for path in cache_directory.iterdir() if path.name != filename),
        maxlen=0,
    )
    daily_cache_file_path = cache_directory / filename

    if use_cache is True and daily_cache_file_path.exists():
        with daily_cache_file_path.open(mode="r") as file_stream:
            cache_data = yaml.safe_load(stream=file_stream)
        if isinstance(cache_data, dict) and all(key in cache_data for key in _CACHE_KEYS):
            return cache_data

    cache_data = _compute_data()
    with daily_cache_file_path.open(mode="w") as file_stream:
        yaml.dump(data=cache_data, stream=file_stream)
    return cache_data


def _compute_data() -> dict:
    partition_to_disk = dict()
    for partition in PARTITIONS:
        df_output = _deploy_subprocess(command=f"df -B1 {MIRROR_ROOT / partition}")
        df_values = df_output.splitlines()[1].split()
        partition_to_disk[partition] = {"size_bytes": int(df_values[1]), "used_bytes": int(df_values[2])}

    outer_ls_command = "s5cmd ls s3://dandiarchive"
    outer_ls_output = _deploy_subprocess(command=outer_ls_command)

    skip_outer_keys = ("zarr", "dandiarchive")
    outer_ls_locations = [
        line.split(" ")[-1]
        for line in outer_ls_output.splitlines()
        if not any(skip_key in line for skip_key in skip_outer_keys)
    ]

    # blobs/ is listed one head at a time, and its total is their sum, so that the bucket's largest prefix is
    # listed once rather than twice.
    blob_head_to_remote_size = dict()
    blob_head_to_remote_object_count = dict()
    for task_id in range(16):
        size, count = _remote_size_and_object_count(prefix=f"blobs/{task_id:x}")
        blob_head_to_remote_size[task_id] = size
        blob_head_to_remote_object_count[task_id] = count

    outer_directory_to_remote_size = dict()
    outer_directory_to_remote_object_count = dict()
    for location in outer_ls_locations:
        if location == "blobs/":
            size = sum(blob_head_to_remote_size.values())
            count = sum(blob_head_to_remote_object_count.values())
        else:
            size, count = _remote_size_and_object_count(prefix=location)
        outer_directory_to_remote_size[location] = size
        outer_directory_to_remote_object_count[location] = count

    # Only what sits on the non-blob partition is measured locally. blobs/ is not walked: what it should hold
    # comes from the bucket, one head at a time, and where it is from the per-head sub-directory counts, which
    # list directories only. `verify-blobs` checks that placement; `verify-checksums` checks the files.
    backup_directory = MIRROR_ROOT / _NONBLOBS_PARTITION / "s3dandiarchive"
    outer_directory_to_local_size = dict()
    outer_directory_to_local_object_count = dict()
    for location in outer_ls_locations:
        if location == "blobs/":
            continue
        local_path = backup_directory / location.removesuffix("/")
        local_size_in_bytes, local_object_count = _get_local_size_in_bytes_and_object_count(path=local_path)
        outer_directory_to_local_size[location] = local_size_in_bytes
        outer_directory_to_local_object_count[location] = local_object_count

    return {
        "partitions": partition_to_disk,
        "outer_directory_to_remote_size": outer_directory_to_remote_size,
        "outer_directory_to_remote_object_count": outer_directory_to_remote_object_count,
        "outer_directory_to_local_size": outer_directory_to_local_size,
        "outer_directory_to_local_object_count": outer_directory_to_local_object_count,
        "blob_head_to_remote_size": blob_head_to_remote_size,
        "blob_head_to_remote_object_count": blob_head_to_remote_object_count,
        "blob_head_to_subdirectories": count_blob_head_subdirectories(),
    }


def _remote_size_and_object_count(prefix: str) -> tuple[int, int]:
    """The total size and object count of `s3://dandiarchive/{prefix}*`, from `s5cmd du`."""
    du_output = _deploy_subprocess(command=f"s5cmd du s3://dandiarchive/{prefix}*")
    du_output_split = du_output.split(" ")
    return int(du_output_split[0]), int(du_output_split[3])


def _get_local_size_in_bytes_and_object_count(path: pathlib.Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0

    if path.is_file():
        return path.stat().st_size, 1

    if path.is_dir():
        # Not the checksums `verify-checksums` caches beside each blob, which are not objects of the bucket.
        sizes = [
            subpath.stat().st_size
            for subpath in path.rglob(pattern="*")
            if subpath.is_file() and not is_checksum_cache(subpath)
        ]
        return sum(sizes), len(sizes)


def _format_local_and_remote(local: int | None, remote: int, formatter) -> str:
    """`local / remote (ratio)`, or `— / remote` for a location not measured locally (blobs/)."""
    if local is None:
        return f"— / {formatter(remote)}"
    return f"{formatter(local)} / {formatter(remote)} ({_format_ratio(numerator=local, denominator=remote)})"


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
