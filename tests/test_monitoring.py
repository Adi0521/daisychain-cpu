"""Monitoring configuration rendering. Docker is not required.

The live checks are `daisychain emu up && daisychain emu monitor-verify`.
"""

import json
from pathlib import Path

import pytest
import yaml

from daisychain import emu, monitoring
from daisychain.topology import Topology

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def chain():
    return emu.make_plan(Topology.load(ROOT / "config/topology/chain.yaml"),
                         topology_path="config/topology/chain.yaml")


@pytest.fixture
def star():
    return emu.make_plan(Topology.load(ROOT / "config/topology/star.yaml"))


def test_committed_config_matches_generator(chain):
    # config/monitoring is the reviewable deliverable; regenerate with
    # `daisychain monitoring render` after changing monitoring.py.
    for rel, content in monitoring.render(chain).items():
        assert (ROOT / "config/monitoring" / rel).read_text() == content, rel


def test_scrape_every_5s_and_every_node_and_switch_targeted(chain):
    cfg = yaml.safe_load(monitoring.prometheus_config(chain))
    assert cfg["global"]["scrape_interval"] == "5s"
    jobs = {j["job_name"]: j["static_configs"] for j in cfg["scrape_configs"]}
    nodes = {c["labels"]["node"]: c["targets"][0] for c in jobs["node"]}
    assert nodes == {h: f"{chain.addresses[h]}:9100" for h in chain.hosts}
    switches = {c["labels"]["node"]: c["targets"][0] for c in jobs["switch"]}
    assert switches == {"S1": "10.10.0.2:9100", "S2": "10.10.0.3:9100",
                        "S3": "10.10.0.4:9100", "S4": "10.10.0.5:9100"}
    assert jobs["slurm"][0]["targets"] == ["10.10.0.10:9341"]


def test_switch_addresses_do_not_collide_with_hosts(chain, star):
    for plan in (chain, star):
        assert not set(plan.switch_addresses.values()) & set(plan.addresses.values())
    assert star.switch_addresses == {"S0": "10.10.0.2"}
    wiring = emu.wiring_script(chain, {n: 1 for n in chain.nodes})
    assert "ip -n S2 addr add 10.10.0.3/24 dev br0" in wiring


def test_missing_telemetry_alert_has_no_extra_delay():
    rule = next(r for r in monitoring.RULES["groups"][0]["rules"]
                if r.get("alert") == "TelemetryMissing")
    assert "for" not in rule  # PRD §9 30 s bound: scrape interval alone sets detection time


def test_fabric_metrics_use_shaper_counters():
    rec = next(r for r in monitoring.RULES["groups"][0]["rules"]
               if r.get("record") == "daisychain:switch_port_tx_bps")
    assert "node_qdisc_bytes_total" in rec["expr"] and 'kind="tbf"' in rec["expr"]


def test_dashboard_covers_prd_views(chain):
    d = monitoring.dashboard(chain)
    exprs = " ".join(t["expr"] for p in d["panels"] for t in p.get("targets", []))
    # PRD §9: CPU, RAM, swap, disk I/O, NIC bytes/errors, worker health, queue depth,
    # in-flight, latency, throughput, model-load time, Slurm job state, switch ports.
    for needle in ("node_cpu_seconds_total", "MemAvailable", "Swap", "node_disk_",
                   "node_network_receive_bytes_total", "errs_total", 'up{job=~"node|switch"}',
                   "daisychain_router_queue_depth", "daisychain_requests_in_flight",
                   "daisychain_request_duration_seconds_bucket", "daisychain_requests_total",
                   "daisychain_stage_seconds", "daisychain_slurm_jobs",
                   "daisychain:switch_port_tx_bps"):
        assert needle in exprs, needle
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids))
    for p in d["panels"]:  # no overlapping or overflowing panels
        g = p["gridPos"]
        assert g["x"] + g["w"] <= 24


def test_isl_panel_uses_link_capacity(chain):
    d = monitoring.dashboard(chain)
    p = next(p for p in d["panels"] if p["title"] == "Inter-switch link utilisation")
    assert p["targets"][0]["expr"].endswith("/ 1e+09")
    assert 'to=~"S1|S2|S3|S4"' in p["targets"][0]["expr"]


def test_switch_exporter_collects_only_ports(chain):
    cmd = monitoring.exporter_command(chain, "S2")
    assert "--collector.disable-defaults" in cmd and "--collector.qdisc" in cmd
    assert "--web.listen-address=10.10.0.3:9100" in cmd
    host = monitoring.exporter_command(chain, "W1")
    assert f"--collector.textfile.directory={monitoring.METRICS_DIR}" in host


def test_grafana_provisioning_points_at_rendered_dashboard(chain):
    files = monitoring.render(chain)
    prov = yaml.safe_load(files["grafana/provisioning/dashboards/daisychain.yml"])
    assert prov["providers"][0]["options"]["path"] == monitoring.GRAFANA_DASHBOARDS
    assert json.loads(files["grafana/dashboards/cluster.json"])["uid"] == monitoring.DASHBOARD_UID
