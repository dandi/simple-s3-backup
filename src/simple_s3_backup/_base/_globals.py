import importlib.resources
import json
import pathlib

# partitions.json is the single source of truth for where each head is mirrored; dandi-compute-core and
# dandi-compute-page read the same file, so it is kept as data rather than code.
PARTITIONS_SPEC = json.loads(importlib.resources.files("simple_s3_backup").joinpath("partitions.json").read_text())

MIRROR_ROOT = pathlib.Path(PARTITIONS_SPEC["mirror_root"])
PARTITIONS: tuple[str, ...] = tuple(PARTITIONS_SPEC["partitions"])

# Keyed by the head as an int (0-15), which is also the task ID of the blob and checksum arrays.
BLOBS_HEAD_TO_PARTITION = {int(head, 16): partition for head, partition in PARTITIONS_SPEC["blobs"].items()}
ZARR_HEAD_TO_PARTITION = {int(head, 16): partition for head, partition in PARTITIONS_SPEC["zarr"].items()}

# How long a mirrored blob no longer in the bucket is kept before it is deleted (see _not_in_bucket.py).
NOT_IN_BUCKET_GRACE_DAYS: int = PARTITIONS_SPEC["not_in_bucket_grace_days"]
