import copy
import json
from pathlib import Path

import pytest

from daisychain import collect_inventory, inventory, schemas
from daisychain.cli import main
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]

NODE = {
    "schema_version": 1,
    "collected_at": "2026-11-19T10:00:00Z",
    "hostname": "W1",
    "cluster_node_supported": True,
    "os": {"system": "Linux", "distro": "ubuntu", "distro_version": "24.04",
           "pretty_name": "Ubuntu 24.04 LTS", "kernel": "6.8.0", "cgroup_version": "v2"},
    "cpu": {"model": "Example CPU", "architecture": "x86_64", "sockets": 1, "cores_per_socket": 8,
            "threads_per_core": 2, "physical_cores": 8, "logical_cpus": 16, "isa_flags": ["avx2"],
            "scaling_governor": "performance"},
    "memory": {"total_bytes": 32 * 2**30, "available_bytes": 30 * 2**30, "swap_total_bytes": 0},
    "numa": {"node_count": 1, "nodes": []},
    "disks": [{"name": "nvme0n1", "size_bytes": 512 * 10**9, "rotational": False}],
    "nics": [{"name": "eno1", "physical": True, "operstate": "up", "speed_mbps": 1000, "mtu": 1500,
              "ipv4": ["10.0.0.11/24"]}],
    "nfs_mounts": [],
    "time_sync": {"ntp_synchronized": True, "service": "chrony"},
    "software": {"python3": "3.12.3", "slurm": "slurm 23.11.4", "mpirun": "mpirun (Open MPI) 4.1.6",
                 "docker_server": "27.0.0", "pmix": None, "nfs_utils": "1:2.6.4"},
    "warnings": [],
}


def node(name, **patch):
    rec = copy.deepcopy(NODE)
    rec["hostname"] = name
    for path, value in patch.items():
        target = rec
        keys = path.split("__")
        for k in keys[:-1]:
            target = target[k]
        target[keys[-1]] = value
    return rec


def write(tmp_path, recs):
    paths = []
    for r in recs:
        p = tmp_path / f"{r['hostname']}.json"
        p.write_text(json.dumps(r))
        paths.append(p)
    return paths


def test_homogeneous_nodes_pass(tmp_path):
    rep = inventory.check(write(tmp_path, [node("W1"), node("W2")]))
    assert rep.ok
    assert rep.findings == []


def test_software_mismatch_is_error(tmp_path):
    rep = inventory.check(write(tmp_path, [node("W1"), node("W2", software__slurm="slurm 24.05.1")]))
    assert not rep.ok
    assert any("slurm" in f.message and f.level == "error" for f in rep.findings)


def test_heterogeneity_and_hygiene_warnings(tmp_path):
    recs = [node("W1"), node("W2", cpu__model="Other CPU", memory__total_bytes=16 * 2**30,
                             memory__swap_total_bytes=2**30, cpu__scaling_governor="powersave")]
    rep = inventory.check(write(tmp_path, recs))
    msgs = " | ".join(f.message for f in rep.findings)
    assert rep.ok
    for needle in ("CPU model", "RAM", "swap", "powersave"):
        assert needle in msgs


def test_unsynced_clock_and_no_nic_are_errors(tmp_path):
    rep = inventory.check(write(tmp_path, [node("W1", time_sync__ntp_synchronized=False, nics=[])]))
    errs = [f.message for f in rep.findings if f.level == "error"]
    assert any("clock" in e for e in errs) and any("NIC" in e for e in errs)


def test_topology_coverage(tmp_path):
    topo = Topology.load(ROOT / "config/topology/chain.yaml")
    rep = inventory.check(write(tmp_path, [node("C0"), node("W1")]), topo)
    assert any("without inventory" in f.message and "W8" in f.message for f in rep.findings)


def test_schema_violation_reported(tmp_path):
    bad = node("W1")
    del bad["cpu"]
    rep = inventory.check(write(tmp_path, [bad]))
    assert not rep.ok and "schema" in rep.findings[0].message


def test_collector_output_matches_schema():
    rec = collect_inventory.collect()
    assert schemas.errors("inventory", rec) == []


@pytest.mark.parametrize("schema,file", [
    ("experiment_manifest", "manifest.example.json"),
    ("request_record", "requests.example.jsonl"),
    ("run_summary", "summary.example.json"),
])
def test_examples_validate(schema, file):
    for rec in schemas.load_records(ROOT / "examples" / file):
        assert schemas.errors(schema, rec) == []


def test_manifest_rejects_bad_run_id_and_tier():
    rec = schemas.load_records(ROOT / "examples/manifest.example.json")[0]
    rec["run_id"] = "run-1"
    rec["evidence_tier"] = "probably_works"
    errs = schemas.errors("experiment_manifest", rec)
    assert any("run_id" in e for e in errs) and any("evidence_tier" in e for e in errs)


def test_run_id_matches_schema_pattern(capsys):
    assert main(["run-id", "worker-scaling"]) == 0
    run_id = capsys.readouterr().out.strip()
    rec = schemas.load_records(ROOT / "examples/manifest.example.json")[0]
    rec["run_id"] = run_id
    assert schemas.errors("experiment_manifest", rec) == []


def test_cli_topology_commands(capsys):
    assert main(["topology", "validate", str(ROOT / "config/topology/chain.yaml"),
                 str(ROOT / "config/topology/star.yaml")]) == 0
    assert main(["topology", "load", str(ROOT / "config/topology/chain.yaml"),
                 "--payload-mb", "1000"]) == 0
    out = capsys.readouterr().out
    assert "64.0 s" in out  # 8 flows × 1000 MB × 8 bit / 1000 Mb/s
