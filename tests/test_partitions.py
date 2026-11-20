import datetime
import json
import zoneinfo

import pytest

from simple_s3_backup._base import _dandi, _display, _globals

HEX_DIGITS = [f"{digit:x}" for digit in range(16)]


def test_partitions_spec_covers_every_head():
    spec = _globals.PARTITIONS_SPEC
    assert set(spec) == {"mirror_root", "partitions", "blobs", "zarr", "not_in_bucket_grace_days"}
    assert spec["not_in_bucket_grace_days"] == 183
    assert spec["mirror_root"] == str(_globals.MIRROR_ROOT)
    assert list(_globals.PARTITIONS) == spec["partitions"]
    for kind in ("blobs", "zarr"):
        assert sorted(spec[kind]) == HEX_DIGITS
        assert set(spec[kind].values()) <= set(spec["partitions"])

    assert list(_globals.BLOBS_HEAD_TO_PARTITION) == list(range(16))
    assert list(_globals.ZARR_HEAD_TO_PARTITION) == list(range(16))


@pytest.fixture
def mirror_root(tmp_path, monkeypatch):
    monkeypatch.setattr(_dandi, "MIRROR_ROOT", tmp_path)
    monkeypatch.setattr(_dandi, "PARTITIONS", ("001", "002"))
    monkeypatch.setattr(
        _dandi, "BLOBS_HEAD_TO_PARTITION", {task_id: "001" if task_id < 8 else "002" for task_id in range(16)}
    )
    return tmp_path


def _make_subdirectories(mirror_root, partition, *names):
    for name in names:
        (mirror_root / partition / "s3dandiarchive" / "blobs" / name / "abc").mkdir(parents=True)


def _place_every_head(mirror_root):
    for task_id in range(16):
        _make_subdirectories(mirror_root, "001" if task_id < 8 else "002", f"{task_id:x}00")


def test_count_blob_head_subdirectories(mirror_root):
    _make_subdirectories(mirror_root, "001", "000", "001", "0ff", "a00")
    _make_subdirectories(mirror_root, "002", "a00", "a01")
    # Neither a three-character directory nor a directory at all, so not counted.
    (mirror_root / "001" / "s3dandiarchive" / "blobs" / "0abc").mkdir()
    (mirror_root / "001" / "s3dandiarchive" / "blobs" / "0aa").write_text("")

    counts = _dandi.count_blob_head_subdirectories()

    assert list(counts) == list(range(16))
    assert counts[0] == {"001": 3, "002": 0}
    assert counts[10] == {"001": 1, "002": 2}
    assert counts[1] == {"001": 0, "002": 0}


def test_blob_head_placement(mirror_root):
    assert _dandi.blob_head_placement(task_id=0, subdirectories={"001": 5, "002": 0}) == (False, [])
    assert _dandi.blob_head_placement(task_id=0, subdirectories={"001": 0, "002": 0}) == (True, [])
    assert _dandi.blob_head_placement(task_id=10, subdirectories={"001": 3, "002": 4}) == (False, ["001"])


def test_verify_dandi_blobs_passes(mirror_root, capsys):
    _place_every_head(mirror_root)
    _dandi.verify_dandi_blobs()
    assert "Verification passed" in capsys.readouterr().out


def test_verify_dandi_blobs_fails_on_missing_head(mirror_root):
    _place_every_head(mirror_root)
    (mirror_root / "002" / "s3dandiarchive" / "blobs" / "f00" / "abc").rmdir()
    (mirror_root / "002" / "s3dandiarchive" / "blobs" / "f00").rmdir()
    with pytest.raises(RuntimeError, match="head f \\(belongs on 002\\)"):
        _dandi.verify_dandi_blobs()


def test_verify_dandi_blobs_fails_on_misplaced_head(mirror_root):
    _place_every_head(mirror_root)
    _make_subdirectories(mirror_root, "001", "a12")
    with pytest.raises(RuntimeError, match="head a on 001 \\(belongs on 002\\)"):
        _dandi.verify_dandi_blobs()


def _dashboard_data():
    return {
        "partitions": {
            "001": {"size_bytes": 1000, "used_bytes": 400},
            "002": {"size_bytes": 2000, "used_bytes": 500},
        },
        "outer_directory_to_remote_size": {"blobs/": 16 * 100, "README.md": 10},
        "outer_directory_to_remote_object_count": {"blobs/": 16 * 2, "README.md": 1},
        "outer_directory_to_local_size": {"README.md": 10},
        "outer_directory_to_local_object_count": {"README.md": 1},
        "blob_head_to_remote_size": {task_id: 100 for task_id in range(16)},
        "blob_head_to_remote_object_count": {task_id: 2 for task_id in range(16)},
        "blob_head_to_subdirectories": {
            task_id: {"001": 1 if task_id < 10 else 0, "002": 0 if task_id < 10 else 1} for task_id in range(16)
        }
        | {10: {"001": 2, "002": 3}, 11: {"001": 0, "002": 0}},
    }


def test_build_blob_heads_json():
    updated = datetime.datetime(2026, 11, 20, 6, 30, tzinfo=zoneinfo.ZoneInfo(key="America/New_York"))
    blob_heads = _display._build_blob_heads_json(data=_dashboard_data(), updated=updated)

    assert blob_heads["updated"] == "2026-11-20T06:30:00-05:00"
    assert blob_heads["partitions"] == {
        "001": {"size_bytes": 1000, "used_bytes": 400},
        "002": {"size_bytes": 2000, "used_bytes": 500},
    }
    assert [entry["head"] for entry in blob_heads["heads"]] == HEX_DIGITS
    assert blob_heads["heads"][10] == {
        "head": "a",
        "partition": "002",
        "remote_bytes": 100,
        "remote_objects": 2,
        "subdirectories": {"001": 2, "002": 3},
    }
    # JSON round trip, as DANDI Compute reads it.
    assert json.loads(json.dumps(blob_heads)) == blob_heads


def test_update_display_writes_blob_heads(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    monkeypatch.setattr(_display, "STATUS_DIRECTORY", tmp_path)
    monkeypatch.setattr(_display, "_load_data", lambda use_cache: _dashboard_data())

    _display.update_display()

    blob_heads = json.loads((tmp_path / "data" / "blob_heads.json").read_text())
    assert [entry["partition"] for entry in blob_heads["heads"]] == [
        _globals.BLOBS_HEAD_TO_PARTITION[t] for t in range(16)
    ]
    assert datetime.datetime.fromisoformat(blob_heads["updated"]).utcoffset() is not None
    assert (tmp_path / "data" / "disk.json").exists()
    assert (tmp_path / "data" / "content.json").exists()

    readme = (tmp_path / "README.md").read_text()
    assert "## Blob Heads" in readme
    assert "— / 1.60 KB" in readme  # blobs/ has no local figures; it is not walked.
    assert "misplaced on 001" in readme
    assert "missing" in readme


def test_load_data_lists_blobs_by_head_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(_display, "MIRROR_ROOT", tmp_path)
    monkeypatch.setattr(_dandi, "MIRROR_ROOT", tmp_path)
    for partition in ("001", "002"):
        (tmp_path / partition).mkdir()
    (tmp_path / "001" / "s3dandiarchive" / "blobs" / "012" / "345").mkdir(parents=True)
    (tmp_path / "001" / "s3dandiarchive" / "blobs" / "012" / "345" / "0123-blob").write_bytes(b"12345")

    commands = []

    def fake_subprocess(command, **kwargs):
        commands.append(command)
        if command.startswith("df"):
            return "Filesystem 1B-blocks Used Available Use% Mounted\nfs  1000   400 600 40% /x\n"
        if command == "s5cmd ls s3://dandiarchive":
            return "    DIR  blobs/\n    DIR  zarr/\n2024/01/01 00:00:00  10 README.md\n"
        if command.startswith("s5cmd du s3://dandiarchive/blobs/"):
            head = command.removeprefix("s5cmd du s3://dandiarchive/blobs/").removesuffix("*")
            return f"{int(head, 16) * 10} bytes in {int(head, 16)} objects: s3://dandiarchive/blobs/{head}*\n"
        if command.startswith("s5cmd du"):
            return "10 bytes in 1 objects: s3://dandiarchive/README.md*\n"
        raise AssertionError(command)

    monkeypatch.setattr(_display, "_deploy_subprocess", fake_subprocess)

    # A cache written before the per-head data existed is recomputed rather than crashing.
    cache_directory = tmp_path / "001" / "display_cache"
    cache_directory.mkdir()
    (cache_directory / f"{_display._get_today()}.yaml").write_text("partition_001_used: 1\n")

    data = _display._load_data()

    assert "s5cmd du s3://dandiarchive/blobs/*" not in commands
    assert sum(command.startswith("s5cmd du s3://dandiarchive/blobs/") for command in commands) == 16
    assert data["outer_directory_to_remote_size"]["blobs/"] == sum(head * 10 for head in range(16))
    assert data["outer_directory_to_remote_object_count"]["blobs/"] == sum(range(16))
    assert data["blob_head_to_remote_size"][15] == 150
    assert data["blob_head_to_subdirectories"][0] == {"001": 1, "002": 0}
    # The mirror's blobs are never walked: no local figures for blobs/, and no `du` over it.
    assert "blobs/" not in data["outer_directory_to_local_object_count"]
    assert "blobs/" not in data["outer_directory_to_local_size"]
    assert not any(command.startswith("du") for command in commands)
    assert data["partitions"]["002"] == {"size_bytes": 1000, "used_bytes": 400}

    commands.clear()
    assert _display._load_data() == data
    assert commands == []
