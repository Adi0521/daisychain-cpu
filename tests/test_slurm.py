"""Slurm config rendering for the emulated cluster. Docker is not required.

The live check is `daisychain emu up && daisychain emu slurm-verify`.
"""

from pathlib import Path

import pytest

from daisychain import emu, slurm
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]
HW = {"CPUs": 8, "Boards": 1, "SocketsPerBoard": 1, "CoresPerSocket": 8, "ThreadsPerCore": 1}


@pytest.fixture
def plan():
    return emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"))


def node_lines(conf: str) -> dict[str, str]:
    return {l.split()[0].split("=")[1]: l for l in conf.splitlines() if l.startswith("NodeName=")}


def test_parse_slurmd_c():
    text = ("NodeName=W1 CPUs=8 Boards=1 SocketsPerBoard=1 CoresPerSocket=8 ThreadsPerCore=1 "
            "RealMemory=7837\nUpTime=0-00:03:00\n")
    assert slurm.parse_slurmd_c(text) == HW


def test_worker_cpusets_round_robin():
    assert emu.worker_cpusets(4, 2, 8) == ["0,1", "2,3", "4,5", "6,7"]
    # Overlapping: sliding windows, so no two workers have identical CPU sets.
    assert emu.worker_cpusets(8, 2, 8) == ["0,1", "1,2", "2,3", "3,4", "4,5", "5,6", "6,7", "7,0"]
    assert emu.worker_cpusets(6, 2, 8) == ["0,1", "1,2", "2,3", "4,5", "5,6", "6,7"]
    with pytest.raises(emu.EmuError):
        emu.worker_cpusets(2, 9, 8)


def test_conf_declares_detected_hw_and_reserves_other_cpus(plan):
    conf = slurm.render_slurm_conf(plan, cores=2, memory_mb=1024, hw=HW)
    nodes = node_lines(conf)
    assert list(nodes) == [f"W{i}" for i in range(1, 9)]
    assert "CPUs=8 Boards=1 SocketsPerBoard=1 CoresPerSocket=8 ThreadsPerCore=1" in nodes["W1"]
    assert "CpuSpecList=2,3,4,5,6,7" in nodes["W1"]
    assert "CpuSpecList=0,1,2,5,6,7" in nodes["W4"]  # W4 owns 3,4 (sliding windows)
    assert "NodeAddr=10.10.0.18" in nodes["W8"]
    assert f"RealMemory={1024 - slurm.SLURMD_RESERVED_MB}" in nodes["W1"]


def test_conf_no_spec_list_when_worker_gets_every_cpu(plan):
    conf = slurm.render_slurm_conf(plan, cores=8, memory_mb=2048, hw=HW)
    assert "CpuSpecList" not in conf


def test_conf_enforcement_settings(plan):
    conf = slurm.render_slurm_conf(plan, cores=2, memory_mb=1024, hw=HW)
    for line in ("SlurmctldHost=C0", "ProctrackType=proctrack/cgroup", "TaskPlugin=task/cgroup,task/affinity",
                 "SelectTypeParameters=CR_Core_Memory", "Epilog=/etc/slurm/epilog.sh",
                 f"KillWait={slurm.KILL_WAIT_S}"):
        assert line in conf.splitlines()
    assert "PartitionName=emu Nodes=W1,W2,W3,W4,W5,W6,W7,W8" in conf
    assert "ConstrainRAMSpace=yes" in slurm.CGROUP_CONF
    assert "AllowedSwapSpace=0" in slurm.CGROUP_CONF


def test_conf_worker_subset(plan):
    small = emu.make_plan(plan.topo, n_workers=2)
    conf = slurm.render_slurm_conf(small, cores=2, memory_mb=1024, hw=HW)
    assert list(node_lines(conf)) == ["W1", "W2"]
    assert "Nodes=W1,W2 " in conf
