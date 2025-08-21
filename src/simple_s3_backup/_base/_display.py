import collections
import json
import math
import pathlib

import yaml

from ._utils import _deploy_subprocess, _get_today


def update_display(use_cache: bool = True) -> None:
    """
    Update the README file of the DANDI backup status tracking repository.
    """
    data = _load_data(use_cache=use_cache)

    outer_directory_to_remote_size = data["outer_directory_to_remote_size"]
    outer_directory_to_remote_object_count = data["outer_directory_to_remote_object_count"]
    outer_directory_to_local_size = data["outer_directory_to_local_size"]
    outer_directory_to_local_object_count = data["outer_directory_to_local_object_count"]

    outer_ls_locations = list(outer_directory_to_remote_size.keys())
    outer_ls_locations.sort(key=lambda path: (not path.endswith("/"), path))

    today = _get_today()
    readme_json = {
        "title": "DANDI Backup Status",
        "headers": [
            f"Current status of S3 bucket backup of the DANDI Archive' as of {today.replace('-', '/')}.",
        ],
        "tails": ["[^1]: Reported percentage may exceed 100% due to delayed garbage collection."],
        "data": {
            "Location": outer_ls_locations,
            "Size (Local / Remote)": [
                f"{_human_readable_size(size_in_bytes=(local_size:=outer_directory_to_local_size[location]))} / "
                f"{_human_readable_size(size_in_bytes=(remote_size:=outer_directory_to_remote_size[location]))} "
                f"({_format_ratio(numerator=local_size, denominator=remote_size)})"
                for location in outer_ls_locations
            ],
            "Number of Objects (Local / Remote)[^1]": [
                f"{(local_count:=outer_directory_to_local_object_count[location])} / "
                f"{(remote_count:=outer_directory_to_remote_object_count[location])} "
                f"({_format_ratio(numerator=local_count, denominator=remote_count)})"
                for location in outer_ls_locations
            ],
        },
    }

    readme_json_file_path = pathlib.Path("/orcd") / "data" / "dandi" / "001" / "backup-status" / "readme.json"
    with readme_json_file_path.open(mode="w") as file_stream:
        json.dump(obj=readme_json, fp=file_stream)

    padding = (20, 40, 40)
    readme_content = json_to_markdown_table(json_table=readme_json, padding=padding)

    readme_file_path = pathlib.Path("/orcd") / "data" / "dandi" / "001" / "backup-status" / "README.md"
    with readme_file_path.open(mode="w") as file_stream:
        file_stream.write(readme_content)


def json_to_markdown_table(json_table: dict, *, padding: tuple[int, ...] | None = None) -> str:
    """
    Convert a JSON object to a Markdown table.

    Parameters
    ----------
    json_table : dict
        The JSON data to convert.

    Returns
    -------
    str
        A string representing the Markdown table.

    Examples
    --------
    json_table = {
        "title": "My Example Table",
        "headers": ["My header 1.", "My header 2."],
        "tails": ["This is a tail.", "This is another tail."],
        "data": {
             "Name": ["Alice", "Bob", "Charlie"],
             "Age": [30, 25, 35],
             "City": ["New York", "Los Angeles", "Chicago"]
         }
    }

    print(json_to_markdown_table(json_table=json_table))
    >>> # My Example Table

    My header 1.
    My header 2.

    | Name    | Age | City         |
    | :---: | :---: | :----: |
    | Alice   | 30  | New York     |
    | Bob     | 25  | Los Angeles  |
    | Charlie | 35  | Chicago      |

    This is a tail.
    This is another tail.
    """
    title = json_table.get("title", None)
    headers = json_table.get("headers", None)
    tails = json_table.get("tails", None)

    data = json_table["data"]
    column_names = list(data.keys())
    rows: list[list[str, ...]] = [list(row) for row in zip(*(data[column_name] for column_name in column_names))]

    if padding is None:
        padding = tuple(
            max(len(column_name), max(len(str(value)) for value in [column_name] + [row[column_index] for row in rows]))
            for column_index, column_name in enumerate(column_names)
        )

    markdown_table = []
    if title is not None:
        markdown_table += [f"# {title}", ""]
    if headers is not None:
        markdown_table += headers
        markdown_table += [""]
    formatted_column_names = [
        f"{column_name:<{padding[column_index]}}" for column_index, column_name in enumerate(column_names)
    ]
    markdown_table += ["| " + " | ".join(formatted_column_names) + " |"]
    formatted_dashes = [":" + "-" * (padding[column_index] - 2) + ":" for column_index in range(len(column_names))]
    markdown_table += ["| " + " | ".join(formatted_dashes) + " |"]
    for row in rows:
        markdown_table += [
            "| " + " | ".join(f"{value:<{padding[column_index]}}" for column_index, value in enumerate(row)) + " |"
        ]
    if tails is not None:
        markdown_table += [""]
        markdown_table += tails

    return "\n".join(markdown_table)


def _load_data(use_cache: bool = True) -> dict:
    backup_directory = pathlib.Path("/orcd/data/dandi/001/s3dandiarchive")
    cache_directory = backup_directory.parent / "display_cache"
    cache_directory.mkdir(exist_ok=True)

    today = _get_today()
    filename = f"{today}.yaml"

    collections.deque(
        (path.unlink() for path in cache_directory.iterdir() if path.name != filename),
        maxlen=0,
    )
    daily_cache_file_path = cache_directory / filename

    if use_cache is False or not daily_cache_file_path.exists():
        outer_ls_command = "s5cmd ls s3://dandiarchive"
        outer_ls_output = _deploy_subprocess(command=outer_ls_command)

        skip_outer_keys = ("zarr", "dandiarchive")
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

    return cache_data


def _get_local_size_in_bytes_and_object_count(path: pathlib.Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0

    if path.is_file():
        return path.stat().st_size, 1

    if path.is_dir():
        sizes = [subpath.stat().st_size for subpath in path.rglob(pattern="*") if subpath.is_file()]
        return sum(sizes), len(sizes)


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
