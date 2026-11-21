"""MPI launch helpers and benchmark predictions. Docker is not required.

The live checks are `daisychain emu up && daisychain emu mpi-verify`.
"""

import re
from pathlib import Path

import pytest

from daisychain import emu, mpi, slurm
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def chain():
    return emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"))


@pytest.fixture
def star():
    return emu.make_plan(Topology.load(ROOT / "config/topology/star.yaml"))


def test_launch_uses_pmix_container_and_tcp_on_data_subnet():
    cmd = mpi.srun_cmd(["W1", "W8"], "info")
    assert "srun --mpi=pmix -N2 -w W1,W8" in cmd
    assert "apptainer exec /var/lib/daisychain/images/mpi dcbench info" in cmd
    assert "export OMPI_MCA_btl=self,vader,tcp;" in cmd
    assert "export OMPI_MCA_btl_tcp_if_include=10.10.0.0/24;" in cmd
    assert "export OMPI_MCA_pml=ob1;" in cmd
    native = mpi.srun_cmd(["W1"], "info", container=False)
    assert "apptainer" not in native and native.endswith("dcbench info")


def test_slurm_conf_ends_steps_on_rank_failure(chain):
    hw = {"CPUs": 8, "Boards": 1, "SocketsPerBoard": 1, "CoresPerSocket": 8, "ThreadsPerCore": 1}
    conf = slurm.render_slurm_conf(chain, 2, 1024, hw)
    assert "KillOnBadExit=1" in conf.splitlines()
    # Above slurmstepd's 60 s reverse-tree wait, or node failures drain healthy nodes.
    assert "UnkillableStepTimeout=120" in conf.splitlines()


def test_images_share_mpi_build():
    node = (ROOT / "emu/node.Dockerfile").read_text()
    job = (ROOT / "emu/mpi/Dockerfile").read_text()
    # Same release, same packages, same source and flags: inside == outside the container.
    assert set(re.findall(r"FROM (ubuntu:\S+)", node)) == set(re.findall(r"FROM (ubuntu:\S+)", job))
    compile_line = "mpicc -O2 -Wall -Werror -o /usr/local/bin/dcbench"
    assert compile_line in node and compile_line in job
    assert "openmpi-bin" in node and "openmpi-bin" in job
    # Slurm's PMIx plugin needs the unversioned libpmix.so from libpmix-dev.
    assert "libpmix-dev" in node
    assert "!emu/mpi/dcbench.c" in (ROOT / ".dockerignore").read_text()


def test_window_matches_dcbench():
    src = (ROOT / "emu/mpi/dcbench.c").read_text()
    assert f"#define WINDOW {mpi.WINDOW}" in src


def test_parse_ignores_noise():
    out = 'srun: job 3 queued\n{"bench":"bw","bytes":8}\n{broken\n'
    assert mpi.parse(out) == [{"bench": "bw", "bytes": 8}]


def test_pair_sets_cover_switch_distances(chain, star):
    pairs = mpi.pair_sets(chain)
    assert pairs == {"0-isl": ["W1", "W2"], "1-isl": ["W2", "W3"], "2-isl": ["W1", "W5"],
                     "3-isl": ["W1", "W7"]}
    assert mpi.pair_sets(star) == {"0-isl": ["W1", "W2"]}


def test_cross_pairs_share_middle_link_on_chain_only(chain, star):
    nodes = [f"W{i}" for i in range(1, 9)]
    flows = mpi.pair_flows(nodes)
    assert flows == [("W1", "W5"), ("W2", "W6"), ("W3", "W7"), ("W4", "W8")]
    assert sum(emu.predicted_rates(chain, flows)) == pytest.approx(1000)
    assert sum(emu.predicted_rates(star, flows)) == pytest.approx(4000)


def test_alltoall_bound_set_by_middle_link(chain, star):
    nodes = [f"W{i}" for i in range(1, 9)]
    mib = 2**20
    # Chain: S2->S3 carries 4 senders x 4 receivers = 16 flows. Star: each NIC carries 7.
    assert mpi.alltoall_min_s(chain, nodes, mib) == pytest.approx(16 * mib * 8 / 1e9)
    assert mpi.alltoall_min_s(star, nodes, mib) == pytest.approx(7 * mib * 8 / 1e9)
    # Two workers on one switch: identical on both topologies.
    assert mpi.alltoall_min_s(chain, ["W1", "W2"], mib) == mpi.alltoall_min_s(star, ["W1", "W2"], mib)


def test_pipeline_is_topology_insensitive(chain, star):
    nodes = [f"W{i}" for i in range(1, 9)]
    assert mpi.pipeline_interval_min_s(chain, nodes, 2**20) == \
        mpi.pipeline_interval_min_s(star, nodes, 2**20) == pytest.approx(2**20 * 8 / 1e9)


def test_tbf_burst():
    assert emu.tbf_burst_bytes(1) == 500_000
    assert emu.tbf_burst_bytes(0.1) == 256 * 1024
    assert "burst 500000 " in emu.shaping_commands("x", "eth0", 1, None)[0]
