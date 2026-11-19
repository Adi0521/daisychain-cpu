"""Unit tests for the emulated-cluster plan. Docker is not required.

The live check is `daisychain emu up && daisychain emu verify` (see docs/emulated-cluster.md).
"""

from pathlib import Path

import pytest

from daisychain import emu
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def chain():
    return Topology.load(ROOT / "config/topology/chain.yaml")


@pytest.fixture
def star():
    return Topology.load(ROOT / "config/topology/star.yaml")


def test_plan_cables_match_topology(chain):
    plan = emu.make_plan(chain)
    assert plan.hosts == ["C0"] + [f"W{i}" for i in range(1, 9)]
    assert plan.switches == ["S1", "S2", "S3", "S4"]
    assert len(plan.cables) == 9 + 3
    assert emu.Cable("W8", "eth0", "S4", "p-W8") in plan.cables
    assert emu.Cable("S2", "p-S3", "S3", "p-S2") in plan.cables


def test_addresses_stable_under_worker_subset(chain):
    full = emu.make_plan(chain).addresses
    sub = emu.make_plan(chain, n_workers=2).addresses
    assert sub == {k: full[k] for k in ("C0", "W1", "W2")}
    assert full["C0"] == "10.10.0.10" and full["W8"] == "10.10.0.18"


def test_worker_bounds(chain):
    with pytest.raises(emu.EmuError):
        emu.make_plan(chain, n_workers=0)
    with pytest.raises(emu.EmuError):
        emu.make_plan(chain, n_workers=9)


def test_wiring_shapes_both_ends_of_every_cable(chain):
    plan = emu.make_plan(chain, gbps=2.5, latency_us=100)
    script = emu.wiring_script(plan, {n: 1000 + i for i, n in enumerate(plan.nodes)})
    tbf = [l for l in script.splitlines() if " tbf " in l]
    netem = [l for l in script.splitlines() if " netem " in l]
    assert len(tbf) == len(netem) == 2 * len(plan.cables)
    assert all("rate 2500000000bit" in l for l in tbf)
    assert all("delay 100us" in l for l in netem)
    assert "ip -n S1 link set p-S2 master br0" in script
    assert "ip -n W8 addr add 10.10.0.18/24 dev eth0" in script
    assert "stp_state 0" in script


def test_predicted_rates_fanout_and_cross(chain, star):
    for topo in (chain, star):
        plan = emu.make_plan(topo)
        assert emu.predicted_rates(plan, plan.topo.pattern_flows("fanout")) == pytest.approx(
            [125.0] * 8)
    cross = [("W1", "W5"), ("W2", "W6"), ("W3", "W7"), ("W4", "W8")]
    assert emu.predicted_rates(emu.make_plan(chain), cross) == pytest.approx([250.0] * 4)
    assert emu.predicted_rates(emu.make_plan(star), cross) == pytest.approx([1000.0] * 4)


def test_predicted_rates_max_min_not_equal_split(chain):
    # W3→W5 and W4→W5 share W5's NIC (500 each); W1→W2 stays local to S1 at full rate.
    plan = emu.make_plan(chain)
    rates = emu.predicted_rates(plan, [("W3", "W5"), ("W4", "W5"), ("W1", "W2")])
    assert rates == pytest.approx([500.0, 500.0, 1000.0])
    # Unequal bottlenecks: C0→W1 shares C0's NIC with C0→W3, which also shares S2's
    # downlink to W3 with W5→W3; all three end at 500 under max-min fairness.
    rates = emu.predicted_rates(plan, [("C0", "W1"), ("C0", "W3"), ("W5", "W3")])
    assert rates == pytest.approx([500.0, 500.0, 500.0])


def test_verify_tests_cover_chain_middle(chain):
    plan = emu.make_plan(chain)
    tests = emu.verify_tests(plan)
    assert tests["cross"] == [("W1", "W5"), ("W2", "W6"), ("W3", "W7"), ("W4", "W8")]
    assert "cross" not in emu.verify_tests(emu.make_plan(chain, n_workers=1))
