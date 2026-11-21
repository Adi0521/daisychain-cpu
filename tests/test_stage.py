"""daisychain-stage: verified staging from shared storage (runs locally, no Docker)."""

import hashlib
import io
import json
import os
import tarfile
from pathlib import Path

import pytest

from daisychain import emu, stage, storage
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


def publish(root: Path, kind: str, name: str, files: dict[str, bytes]) -> str:
    d = root / kind / name
    d.mkdir(parents=True)
    lines = []
    for rel, data in sorted(files.items()):
        (d / rel).parent.mkdir(parents=True, exist_ok=True)
        (d / rel).write_bytes(data)
        lines.append(f"{hashlib.sha256(data).hexdigest()}  {rel}\n")
    manifest = "".join(lines).encode()
    (d / "MANIFEST.sha256").write_bytes(manifest)
    return hashlib.sha256(manifest).hexdigest()


def run(capsys, *argv) -> tuple[int, dict]:
    code = stage.main(list(argv))
    return code, json.loads(capsys.readouterr().out.strip().splitlines()[-1])


@pytest.fixture
def dirs(tmp_path):
    return tmp_path / "shared", tmp_path / "local"


def test_stage_copies_verifies_and_marks(dirs, capsys):
    shared, local = dirs
    aid = publish(shared, "models", "m", {"a.bin": b"x" * 5000, "sub/b.bin": b"y" * 10})
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 0 and out["status"] == "ok" and not out["cache_hit"]
    assert out["bytes"] == 5010 and out["files"] == 2 and out["artifact_id"] == aid
    assert (local / "m/sub/b.bin").read_bytes() == b"y" * 10
    assert (local / "m" / stage.MARKER).read_text().strip() == aid


def test_second_stage_is_cache_hit(dirs, capsys):
    shared, local = dirs
    publish(shared, "models", "m", {"a.bin": b"x"})
    run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 0 and out["cache_hit"] and out["bytes"] == 0


def test_changed_artifact_replaces_local_copy(dirs, capsys):
    shared, local = dirs
    publish(shared, "models", "m", {"a.bin": b"old"})
    run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    import shutil
    shutil.rmtree(shared / "models/m")
    new = publish(shared, "models", "m", {"a.bin": b"new"})
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 0 and not out["cache_hit"] and out["artifact_id"] == new
    assert (local / "m/a.bin").read_bytes() == b"new"
    assert [p.name for p in local.iterdir() if p.name.startswith("m.old")] == []


def test_corruption_fails_and_leaves_nothing(dirs, capsys):
    shared, local = dirs
    publish(shared, "models", "m", {"a.bin": b"good" * 100, "b.bin": b"fine"})
    (shared / "models/m/a.bin").write_bytes(b"evil" * 100)
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 3 and out["status"] == "checksum_mismatch" and "a.bin" in out["error"]
    assert sorted(p.name for p in local.iterdir() if not p.name.endswith(".lock")) == []


def test_missing_file_is_io_error(dirs, capsys):
    shared, local = dirs
    publish(shared, "models", "m", {"a.bin": b"x"})
    (shared / "models/m/a.bin").unlink()
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 4 and out["status"] == "error"


@pytest.mark.parametrize("bad", ["../escape", "/abs/path", "a/../../b"])
def test_manifest_path_traversal_rejected(dirs, capsys, bad):
    shared, local = dirs
    d = shared / "models/m"
    d.mkdir(parents=True)
    (d / "MANIFEST.sha256").write_text(f"{'0' * 64}  {bad}\n")
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local))
    assert code == 4 and "malformed" in out["error"]


def test_untar_image(dirs, capsys):
    shared, local = dirs
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        data = b"#!/bin/sh\necho hi\n"
        info = tarfile.TarInfo("./usr/bin/hello")
        info.size, info.mode = len(data), 0o755
        tf.addfile(info, io.BytesIO(data))
    publish(shared, "images", "img", {"image.tar": buf.getvalue()})
    code, out = run(capsys, "images/img", "--root", str(shared), "--dest", str(local), "--untar")
    assert code == 0, out
    hello = local / "img/usr/bin/hello"
    assert hello.read_bytes().startswith(b"#!/bin/sh") and os.access(hello, os.X_OK)
    assert not (local / "img/image.tar").exists()


def test_untar_requires_single_tar(dirs, capsys):
    shared, local = dirs
    publish(shared, "images", "img", {"image.tar": b"", "extra": b"x"})
    code, out = run(capsys, "images/img", "--root", str(shared), "--dest", str(local), "--untar")
    assert code == 4 and "only image.tar" in out["error"]


def test_predicted_makespan_matches_c0_nic_bound():
    chain = emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"))
    star = emu.make_plan(Topology.load(ROOT / "config/topology/star.yaml"))
    size = 256 * 2**20
    for plan in (chain, star):
        assert storage.predicted_makespan(plan, ["W1"], size) == pytest.approx(2.147, abs=1e-3)
        nodes = [f"W{i}" for i in range(1, 9)]
        assert storage.predicted_makespan(plan, nodes, size) == pytest.approx(17.18, abs=0.01)


def test_metrics_written_for_textfile_collector(dirs, capsys, tmp_path):
    shared, local = dirs
    metrics = tmp_path / "metrics"
    metrics.mkdir()
    publish(shared, "models", "m", {"a.bin": b"x" * 100})
    code, out = run(capsys, "models/m", "--root", str(shared), "--dest", str(local),
                    "--metrics-dir", str(metrics))
    assert code == 0
    text = (metrics / "stage-models_m.prom").read_text()
    assert 'daisychain_stage_bytes{artifact="models/m"} 100' in text
    assert 'daisychain_stage_success{artifact="models/m"} 1' in text
    assert [p.name for p in metrics.iterdir()] == ["stage-models_m.prom"]


def test_metrics_record_failure_and_missing_dir_is_ignored(dirs, capsys, tmp_path):
    shared, local = dirs
    metrics = tmp_path / "metrics"
    metrics.mkdir()
    publish(shared, "models", "m", {"a.bin": b"good"})
    (shared / "models/m/a.bin").write_bytes(b"evil")
    code, _ = run(capsys, "models/m", "--root", str(shared), "--dest", str(local),
                  "--metrics-dir", str(metrics))
    assert code == 3
    assert 'daisychain_stage_success{artifact="models/m"} 0' in (metrics / "stage-models_m.prom").read_text()
    code, _ = run(capsys, "models/m", "--root", str(shared), "--dest", str(local),
                  "--metrics-dir", str(tmp_path / "absent"))
    assert code == 3
