"""Failure-experiment helpers that need no Docker.

The live experiments are `daisychain emu fail <scenario>`.
"""

from pathlib import Path

from daisychain import cli, emu, failures
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


def chain():
    return emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"))


def test_recable_touches_only_the_restored_host():
    plan = chain()
    script = emu.wiring_script(plan, {n: 100 + i for i, n in enumerate(plan.nodes)}, only_host="W5")
    assert "ip -n W5 addr add 10.10.0.15/24 dev eth0" in script
    assert "ip -n S3 link set p-W5 master br0" in script
    assert "tc -n S3 qdisc replace dev p-W5" in script and "tc -n W5 qdisc replace dev eth0" in script
    # No bridge re-creation, no other cables, no other nodes.
    assert "type bridge" not in script
    for other in ("W4", "W6", "S2", "S4", "C0"):
        assert f"-n {other} " not in script
    assert script.count("ip link add") == 1


def test_full_wiring_unchanged_by_only_host_option():
    plan = chain()
    full = emu.wiring_script(plan, {n: 1 for n in plan.nodes})
    assert full.count("ip link add") == len(plan.cables)
    assert full.count("type bridge") == len(plan.switches)


def _watch(events):
    res = failures.Result("x", {})
    w = failures.Watch.__new__(failures.Watch)
    w.res, w.first = res, dict(events)
    return w


def test_first_not_responding_includes_star_and_down():
    w = _watch({"slurm W5=allocated": 0.0, "slurm W5=allocated*": 14.5,
                "slurm W5=down*": 40.8, "slurm W6=idle*": 3.0})
    assert failures._first_not_responding(w, "W5") == 14.5
    w = _watch({"slurm W5=allocated": 0.0, "slurm W5=down": 30.0})
    assert failures._first_not_responding(w, "W5") == 30.0
    w = _watch({"slurm W5=allocated": 0.0})
    assert failures._first_not_responding(w, "W5") is None


def test_first_time_matches_terminal_job_states():
    w = _watch({"job 3=RUNNING": 0.0, "job 3=COMPLETING": 40.8, "job 3=NODE_FAIL": 106.8})
    terminal = ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL")
    assert w.first_time("job 3", terminal) == 106.8
    assert w.first_time("ranks alive on survivors", ("0/",)) is None


def test_cli_accepts_scenarios():
    p = cli.build_parser()
    a = p.parse_args(["emu", "fail", "link-down", "--seconds", "10", "--link", "S2-S3"])
    assert a.scenario == "link-down" and a.seconds == 10 and a.link == "S2-S3"
