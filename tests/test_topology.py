from pathlib import Path

import pytest

from daisychain.topology import Topology, TopologyError

ROOT = Path(__file__).resolve().parents[1]
CHAIN = ROOT / "config/topology/chain.yaml"
STAR = ROOT / "config/topology/star.yaml"


@pytest.fixture
def chain():
    return Topology.load(CHAIN)


@pytest.fixture
def star():
    return Topology.load(STAR)


def base(**over):
    d = {
        "name": "t",
        "switches": [{"id": "S1"}, {"id": "S2"}, {"id": "S3"}],
        "switch_links": [{"a": "S1", "b": "S2"}, {"a": "S2", "b": "S3"}],
        "hosts": [
            {"id": "C0", "role": "controller", "switch": "S1"},
            {"id": "W1", "role": "worker", "switch": "S3"},
        ],
    }
    d.update(over)
    return d


def test_reference_topologies_match_prd(chain, star):
    assert [w.id for w in chain.workers] == [f"W{i}" for i in range(1, 9)]
    assert chain.controller.id == "C0" and chain.controller.switch == "S1"
    # PRD §4: C0-to-W8 traverses all three inter-switch links.
    assert chain.inter_switch_hops("C0", "W8") == 3
    assert chain.route("C0", "W8") == [
        ("C0", "S1"), ("S1", "S2"), ("S2", "S3"), ("S3", "S4"), ("S4", "W8")]
    # Same hosts and endpoint speed in the star, only topology differs.
    assert set(star.hosts) == set(chain.hosts)
    assert star.link_gbps == chain.link_gbps
    assert all(star.inter_switch_hops("C0", w.id) == 0 for w in star.workers)


def test_routes_are_directed(chain):
    assert chain.route("W8", "C0") == [
        ("W8", "S4"), ("S4", "S3"), ("S3", "S2"), ("S2", "S1"), ("S1", "C0")]


def test_loop_rejected():
    links = [{"a": "S1", "b": "S2"}, {"a": "S2", "b": "S3"}, {"a": "S3", "b": "S1"}]
    with pytest.raises(TopologyError, match="loop"):
        Topology.from_dict(base(switch_links=links))


def test_parallel_link_rejected():
    links = [{"a": "S1", "b": "S2"}, {"a": "S2", "b": "S1"}]
    with pytest.raises(TopologyError, match="duplicate switch link"):
        Topology.from_dict(base(switches=[{"id": "S1"}, {"id": "S2"}], switch_links=links,
                                hosts=base()["hosts"][:1]))


def test_disconnected_rejected():
    with pytest.raises(TopologyError, match="not connected"):
        Topology.from_dict(base(switch_links=[{"a": "S1", "b": "S2"}]))


def test_controller_count_enforced():
    hosts = [{"id": "W1", "role": "worker", "switch": "S1"}]
    with pytest.raises(TopologyError, match="exactly one controller"):
        Topology.from_dict(base(hosts=hosts))


def test_unknown_switch_rejected():
    hosts = [{"id": "C0", "role": "controller", "switch": "S9"}]
    with pytest.raises(TopologyError, match="unknown switch"):
        Topology.from_dict(base(hosts=hosts))


def test_fanout_bottleneck_is_controller_nic_in_both(chain, star):
    for t in (chain, star):
        load = t.link_load(t.pattern_flows("fanout"))
        assert max(load.values()) == 8
        assert load[("C0", t.controller.switch)] == 8
    chain_load = chain.link_load(chain.pattern_flows("fanout"))
    assert chain_load[("S1", "S2")] == 6
    assert chain_load[("S2", "S3")] == 4
    assert chain_load[("S3", "S4")] == 2


def test_all_to_all_concentrates_on_middle_chain_link(chain, star):
    chain_load = chain.link_load(chain.pattern_flows("all-to-all"))
    # 4 workers on each side of S2–S3, so 4*4 flows per direction.
    assert chain_load[("S2", "S3")] == chain_load[("S3", "S2")] == 16
    star_load = star.link_load(star.pattern_flows("all-to-all"))
    assert max(star_load.values()) == 7


def test_pipeline_and_worker_subset(chain):
    assert chain.pattern_flows("pipeline", 4) == [("W1", "W2"), ("W2", "W3"), ("W3", "W4")]
    assert max(chain.link_load(chain.pattern_flows("pipeline")).values()) == 1
    assert len(chain.pattern_flows("fanout", 2)) == 2
    with pytest.raises(TopologyError):
        chain.pattern_flows("fanout", 9)
