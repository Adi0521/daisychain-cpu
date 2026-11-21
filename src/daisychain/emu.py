"""Emulated cluster: the topology as Docker containers wired with direct veth links.

Every host and every switch is a container started with `--network none`. A privileged
helper running in the Docker VM's namespaces then creates one veth pair per cable in the
topology, turns each switch container into a Linux bridge, and rate-limits the egress of
every endpoint (both directions of each full-duplex link) with tbf. No traffic between
nodes can bypass the emulated switches, because nodes have no other network interface.

What this validates: orchestration, protocols, routing through the chain, link sharing and
failure handling. What it does NOT validate: CPU scaling (all containers share the host's
CPUs), real switch buffering, or NIC behaviour. Results are evidence tier "designed"
or "simulated", never "physically_tested".
"""

from __future__ import annotations

import ipaddress
import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .topology import Topology

IMAGE = "daisychain/node:dev"
LABEL = "daisychain.emu"
PREFIX = "dc-"
SHARED_VOLUME = "dc-shared"  # C0's NFS export; an ext4-backed volume (overlayfs can't be exported)
DATA_SUBNET = ipaddress.ip_network("10.10.0.0/24")
FIRST_HOST_OFFSET = 10
FIRST_SWITCH_OFFSET = 2  # switch management addresses: .2 … .9
TBF_QUEUE_LATENCY_MS = 50
REPO_ROOT = Path(__file__).resolve().parents[2]


class EmuError(RuntimeError):
    pass


# ---- plan (pure, unit-testable) -----------------------------------------------------------


@dataclass(frozen=True)
class Cable:
    a: str
    a_if: str
    b: str
    b_if: str


@dataclass
class Plan:
    topo: Topology
    hosts: list[str]          # controller first, then selected workers
    switches: list[str]
    addresses: dict[str, str]  # host -> IPv4
    cables: list[Cable]
    gbps: float
    latency_us: float | None
    labels: dict[str, str] = field(default_factory=dict)
    # In-band management address on each switch's bridge, as a managed switch would have;
    # its node_exporter serves port counters there (monitoring.py).
    switch_addresses: dict[str, str] = field(default_factory=dict)

    @property
    def nodes(self) -> list[str]:
        return self.hosts + self.switches


def container(node: str) -> str:
    return PREFIX + node


def make_plan(topo: Topology, n_workers: int | None = None, gbps: float | None = None,
              latency_us: float | None = None, topology_path: str | None = None) -> Plan:
    workers = [w.id for w in topo.workers]
    if n_workers is not None:
        if not 1 <= n_workers <= len(workers):
            raise EmuError(f"--workers must be in 1..{len(workers)}")
        workers = workers[:n_workers]
    hosts = [topo.controller.id] + workers
    # Keep every switch so the chain is intact even if some have no workers attached.
    switches = list(topo.switches)

    # Stable addresses from the topology's host order, independent of the subset chosen.
    ordered = [topo.controller.id] + [w.id for w in topo.workers]
    base = int(DATA_SUBNET.network_address) + FIRST_HOST_OFFSET
    addresses = {h: str(ipaddress.ip_address(base + ordered.index(h))) for h in hosts}

    cables = [Cable(h, "eth0", topo.hosts[h].switch, f"p-{h}") for h in hosts]
    cables += [Cable(a, f"p-{b}", b, f"p-{a}") for a, b in topo.switch_links]

    if len(switches) > FIRST_HOST_OFFSET - FIRST_SWITCH_OFFSET:
        raise EmuError(f"at most {FIRST_HOST_OFFSET - FIRST_SWITCH_OFFSET} switches supported")
    switch_addresses = {sw: str(ipaddress.ip_address(
        int(DATA_SUBNET.network_address) + FIRST_SWITCH_OFFSET + i)) for i, sw in enumerate(switches)}

    plan = Plan(
        topo=topo, hosts=hosts, switches=switches, addresses=addresses, cables=cables,
        switch_addresses=switch_addresses,
        gbps=gbps if gbps is not None else topo.link_gbps,
        latency_us=latency_us if latency_us is not None else topo.link_latency_us,
    )
    plan.labels = {
        LABEL: topo.name,
        "daisychain.topology": str(Path(topology_path).resolve()) if topology_path else "",
        "daisychain.workers": str(len(workers)),
        "daisychain.gbps": str(plan.gbps),
        "daisychain.latency_us": "" if plan.latency_us is None else str(plan.latency_us),
    }
    return plan


def tbf_burst_bytes(gbps: float) -> int:
    """Token-bucket depth for a shaped link.

    Must hold at least one GSO super-packet (64 KiB) and one timer tick of traffic: in the
    Docker Desktop VM, smaller buckets cap a 1 Gb/s link at 470–590 Mb/s. The cost is that a
    link idle for a tick lets this many bytes through at memory speed, so per-message
    timings below a few times this size are not representative (see mpi.py).
    """
    return max(256 * 1024, int(gbps * 1e9) // 8 // 250)


def shaping_commands(ns: str, dev: str, gbps: float, latency_us: float | None) -> list[str]:
    rate_bps = int(gbps * 1e9)
    burst = tbf_burst_bytes(gbps)
    cmds = [f"tc -n {ns} qdisc replace dev {dev} root handle 1: tbf rate {rate_bps}bit "
            f"burst {burst} latency {TBF_QUEUE_LATENCY_MS}ms"]
    if latency_us:
        cmds.append(f"tc -n {ns} qdisc add dev {dev} parent 1:1 handle 10: netem "
                    f"delay {int(latency_us)}us")
    return cmds


def wiring_script(plan: Plan, pids: dict[str, int], only_host: str | None = None) -> str:
    """Bash run in a privileged helper with --pid=host --net=host (the Docker VM's view).

    With `only_host`, re-cable just that host to its switch (after the host's container
    was restarted: a veth pair dies with either end's network namespace, so the switch
    port disappeared too). Bridges and other cables are left alone.
    """
    prefix_len = DATA_SUBNET.prefixlen
    cables = [c for c in plan.cables if only_host in (None, c.a)]
    nodes = plan.nodes if only_host is None else sorted({only_host} | {c.b for c in cables})
    lines = ["set -euo pipefail", "mkdir -p /var/run/netns"]
    for node in nodes:
        lines.append(f"ln -sfn /proc/{pids[node]}/ns/net /var/run/netns/{node}")
    for i, c in enumerate(cables):
        ta, tb = f"dcv{i}a", f"dcv{i}b"
        lines += [
            f"ip link add {ta} type veth peer name {tb}",
            f"ip link set {ta} netns {c.a}",
            f"ip -n {c.a} link set {ta} name {c.a_if}",
            f"ip link set {tb} netns {c.b}",
            f"ip -n {c.b} link set {tb} name {c.b_if}",
        ]
    if only_host is None:
        for sw in plan.switches:
            lines += [f"ip -n {sw} link add br0 type bridge stp_state 0",
                      f"ip -n {sw} addr add {plan.switch_addresses[sw]}/{prefix_len} dev br0",
                      f"ip -n {sw} link set br0 up"]
    for c in cables:
        for node, dev in ((c.a, c.a_if), (c.b, c.b_if)):
            if node in plan.switches:
                lines.append(f"ip -n {node} link set {dev} master br0")
            else:
                lines.append(f"ip -n {node} addr add {plan.addresses[node]}/{prefix_len} dev {dev}")
            lines.append(f"ip -n {node} link set {dev} up")
            lines += shaping_commands(node, dev, plan.gbps, plan.latency_us)
    for node in nodes:
        if node not in plan.switches or only_host is None:
            lines.append(f"ip -n {node} link set lo up")
    if only_host is not None:
        lines += [f"rm -f /var/run/netns/{n}" for n in nodes]
    lines.append("echo wired")
    return "\n".join(lines) + "\n"


# ---- docker operations ------------------------------------------------------------------


def _docker(*args: str, input: str | None = None, check: bool = True,
            capture: bool = True) -> subprocess.CompletedProcess:
    try:
        p = subprocess.run(["docker", *args], input=input, text=True,
                           stdout=subprocess.PIPE if capture else None,
                           stderr=subprocess.PIPE if capture else None)
    except FileNotFoundError:
        raise EmuError("docker CLI not found") from None
    if check and p.returncode != 0:
        raise EmuError(f"docker {' '.join(args[:3])} failed: {(p.stderr or '').strip()}")
    return p


def build_image(quiet: bool = False) -> None:
    args = ["build", "-t", IMAGE, "-f", str(REPO_ROOT / "emu/node.Dockerfile"), str(REPO_ROOT)]
    if quiet:
        args.insert(1, "-q")
    _docker(*args, capture=False)


def image_exists() -> bool:
    return _docker("image", "inspect", IMAGE, check=False).returncode == 0


def running_nodes() -> dict[str, dict]:
    """Emulated containers by node id, with their labels."""
    out = _docker("ps", "-a", "--filter", f"label={LABEL}", "--format", "{{json .}}").stdout
    nodes = {}
    for line in out.splitlines():
        info = json.loads(line)
        name = info["Names"]
        labels = dict(kv.split("=", 1) for kv in info.get("Labels", "").split(",") if "=" in kv)
        nodes[name[len(PREFIX):]] = {"container": name, "state": info["State"], "labels": labels}
    return nodes


def docker_cpus() -> int:
    return int(_docker("info", "-f", "{{.NCPU}}").stdout.strip())


def worker_cpusets(n_workers: int, cores: int, host_cpus: int) -> list[str]:
    """Assign each worker `cores` CPU ids from the Docker VM's CPUs.

    Containers are not cpuset-pinned (Slurm's hwloc view inside a pinned container doesn't
    match its CPU numbering, which breaks job binding). Instead Slurm reserves every other
    CPU via CpuSpecList so jobs on worker i can only use its assigned ids.

    When they fit, workers get disjoint blocks. Otherwise they overlap, and the windows
    start at evenly spread CPUs (8 workers x 2 cores on 8 CPUs: {0,1}, {1,2}, ... {7,0})
    rather than wrapping in blocks ({0,1} twice). Wrapping made workers i and i+4 identical,
    so their busy-polling MPI ranks shared one core and took turns in ~3.5 ms scheduler
    slices; with sliding windows, whole-worker ranks can each find a CPU of their own.
    Overlap is fine for function and network tests, and one reason this emulator says
    nothing about compute scaling.
    """
    if not 1 <= cores <= host_cpus:
        raise EmuError(f"--worker-cores must be in 1..{host_cpus} (Docker VM CPUs)")
    if n_workers * cores <= host_cpus:
        starts = [cores * i for i in range(n_workers)]
    else:
        starts = [i * host_cpus // n_workers for i in range(n_workers)]
    return [",".join(str((s + j) % host_cpus) for j in range(cores)) for s in starts]


def up(plan: Plan, build: bool = False, worker_cores: int = 2, worker_memory_mb: int = 1024,
       slurm: bool = True, keep_on_failure: bool = False, monitoring: bool = True,
       ui_ports: dict[str, int] | None = None, log=print) -> None:
    existing = running_nodes()
    if existing:
        raise EmuError(f"emulated cluster already exists ({len(existing)} containers); "
                       "run `daisychain emu down` first")
    if build or not image_exists():
        log(f"building {IMAGE} …")
        build_image()

    hosts_args = []
    for h, ip in plan.addresses.items():
        hosts_args += ["--add-host", f"{h}:{ip}"]
    plan.labels["daisychain.worker_cores"] = str(worker_cores)
    plan.labels["daisychain.worker_memory_mb"] = str(worker_memory_mb)
    worker_cpusets(len(plan.hosts) - 1, worker_cores, docker_cpus())  # validates cores
    label_args = []
    for k, v in plan.labels.items():
        label_args += ["--label", f"{k}={v}"]

    try:
        _docker("volume", "rm", "-f", SHARED_VOLUME, check=False)
        _docker("volume", "create", "--label", f"{LABEL}={plan.topo.name}", SHARED_VOLUME)
        for node in plan.nodes:
            role = ("switch" if node in plan.switches
                    else plan.topo.hosts[node].role)
            args = ["run", "-d", "--init", "--name", container(node), "--hostname", node,
                    "--network", "none", "--label", f"daisychain.role={role}", *label_args]
            if role != "switch":
                args += hosts_args
            if role == "worker":
                # slurmd needs a writable, private cgroup v2 tree to enforce job limits.
                # --privileged is confined to this local emulation (see docs).
                args += ["--privileged", "--cgroupns=private",
                         "--cpus", str(worker_cores),
                         "--memory", f"{worker_memory_mb}m", "--memory-swap",
                         f"{worker_memory_mb}m"]
            if role == "controller":
                # Kernel nfsd needs privileges; it serves only C0's (emulated) interface
                # because nfsd is per network namespace.
                args += ["--privileged", "--mount", f"type=volume,src={SHARED_VOLUME},dst=/export"]
            _docker(*args, IMAGE)
        log(f"started {len(plan.nodes)} containers")

        pids = {}
        for node in plan.nodes:
            pid = _docker("inspect", "-f", "{{.State.Pid}}", container(node)).stdout.strip()
            pids[node] = int(pid)

        _docker("run", "--rm", "-i", "--privileged", "--pid=host", "--net=host", IMAGE,
                "bash", "-s", input=wiring_script(plan, pids))
        log(f"wired {len(plan.cables)} links at {plan.gbps} Gb/s per direction"
            + (f", {plan.latency_us} µs one-way delay" if plan.latency_us else ""))
        if slurm:
            from . import slurm as slurm_mod
            slurm_mod.start(plan, worker_cores, worker_memory_mb, log=log)
        if monitoring:
            from . import monitoring as monitoring_mod
            monitoring_mod.start(plan, ui_ports, log=log)
    except Exception:
        if keep_on_failure:
            log("bring-up failed; containers kept for debugging (`daisychain emu down` to remove)")
        else:
            log("bring-up failed; removing partial cluster")
            down(log=lambda *_: None)
        raise


def down(log=print) -> int:
    """Remove the cluster. NFS clients are unmounted first and workers removed before C0:
    removing the server while a client still has the mount can leave the client container
    unkillable (its exiting tasks wait on NFS forever)."""
    # Monitoring containers share C0's network namespace; remove them first.
    aux = _docker("ps", "-a", "--filter", "label=daisychain.aux", "--format", "{{.Names}}").stdout.split()
    if aux:
        _docker("rm", "-f", *aux)
    nodes = running_nodes()
    workers = [n for n, v in nodes.items() if v["labels"].get("daisychain.role") == "worker"]
    for w in workers:
        if nodes[w]["state"] == "running":
            try:
                subprocess.run(["docker", "exec", container(w), "umount", "-f", "-l", "/shared"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
            except subprocess.TimeoutExpired:
                log(f"warning: unmounting /shared on {w} timed out")
    if workers:
        _docker("rm", "-f", *[container(w) for w in workers])
    rest = [v["container"] for n, v in nodes.items() if n not in workers]
    if rest:
        _docker("rm", "-f", *rest)
    _docker("volume", "rm", "-f", SHARED_VOLUME, check=False)
    log(f"removed {len(nodes)} containers")
    return len(nodes)


def restore_host(plan: Plan, host: str, log=print) -> None:
    """Power a stopped worker back on: start it, re-cable it, restart its services.

    Its filesystem survives (munge key, slurm.conf, staged artifacts); its network, NFS
    mount and daemons do not.
    """
    _docker("start", container(host))
    pids = {n: int(_docker("inspect", "-f", "{{.State.Pid}}", container(n)).stdout.strip())
            for n in {host, plan.topo.hosts[host].switch}}
    _docker("run", "--rm", "-i", "--privileged", "--pid=host", "--net=host", IMAGE,
            "bash", "-s", input=wiring_script(plan, pids, only_host=host))
    role = plan.topo.hosts[host].role
    exec_(host, "/usr/local/sbin/node-init.sh", role)
    log(f"restored {host}: re-cabled to {plan.topo.hosts[host].switch}, services restarted")


def plan_from_running() -> Plan:
    nodes = running_nodes()
    if not nodes:
        raise EmuError("no emulated cluster running; run `daisychain emu up`")
    labels = next(iter(nodes.values()))["labels"]
    path = labels.get("daisychain.topology")
    if not path:
        raise EmuError("running cluster has no topology label; recreate it")
    lat = labels.get("daisychain.latency_us")
    return make_plan(Topology.load(path), int(labels["daisychain.workers"]),
                     float(labels["daisychain.gbps"]), float(lat) if lat else None, path)


def exec_(node: str, *cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return _docker("exec", container(node), *cmd, check=check)


# ---- verification -----------------------------------------------------------------------


@dataclass
class FlowResult:
    src: str
    dst: str
    predicted_mbps: float
    measured_mbps: float | None


def predicted_rates(plan: Plan, flows: list[tuple[str, str]]) -> list[float]:
    """Max-min fair share per flow (Mb/s) via progressive filling over directed links."""
    cap = plan.gbps * 1000
    routes = [plan.topo.route(s, d) for s, d in flows]
    rate = [0.0] * len(flows)
    active = set(range(len(flows)))
    remaining: dict = {}
    for r in routes:
        for hop in r:
            remaining[hop] = cap
    while active:
        users: dict = {}
        for i in active:
            for hop in routes[i]:
                users.setdefault(hop, []).append(i)
        # Grow every active flow equally until the tightest link saturates.
        inc = min(remaining[h] / len(u) for h, u in users.items())
        for i in active:
            rate[i] += inc
        for h, u in users.items():
            remaining[h] -= inc * len(u)
        # Flows crossing a saturated link are frozen at their current rate.
        for h, u in users.items():
            if remaining[h] <= 1e-6 * cap:
                active -= set(u)
    return rate


def _bytes_sent(node: str, dev: str) -> int:
    out = exec_(node, "tc", "-s", "-j", "qdisc", "show", "dev", dev).stdout
    for q in json.loads(out):
        if q.get("kind") == "tbf":
            return int(q["bytes"])
    return 0


def run_flows(plan: Plan, flows: list[tuple[str, str]], seconds: int) -> list[float | None]:
    """Run concurrent iperf3 flows; one server port per flow. Returns receiver Mb/s."""
    ports = {}
    for i, (_, dst) in enumerate(flows):
        port = 5201 + i
        ports[i] = port
        exec_(dst, "iperf3", "-s", "-D", "-1", "-p", str(port))
    time.sleep(0.5)
    procs = [
        subprocess.Popen(
            ["docker", "exec", container(src), "iperf3", "-c", plan.addresses[dst], "-p",
             str(ports[i]), "-t", str(seconds), "-O", "1", "-J"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for i, (src, dst) in enumerate(flows)
    ]
    results: list[float | None] = []
    for p in procs:
        out, _ = p.communicate(timeout=seconds + 30)
        try:
            results.append(json.loads(out)["end"]["sum_received"]["bits_per_second"] / 1e6)
        except (ValueError, KeyError):
            results.append(None)
    return results


def verify_tests(plan: Plan) -> dict[str, list[tuple[str, str]]]:
    c, ws = plan.topo.controller.id, plan.hosts[1:]
    tests = {"single": [(c, ws[-1])], "fanout": [(c, w) for w in ws]}
    if len(ws) >= 2:
        half = len(ws) // 2
        tests["cross"] = [(ws[i], ws[i + half]) for i in range(half)]
    return tests


def verify(plan: Plan, seconds: int = 5, tolerance: float = 0.15, log=print) -> dict:
    report: dict = {"topology": plan.topo.name, "workers": len(plan.hosts) - 1,
                    "gbps": plan.gbps, "ping": {}, "path": {}, "throughput": {}, "ok": True}
    c = plan.topo.controller.id

    # 1. Reachability over the emulated fabric.
    for dst in plan.hosts[1:]:
        r = exec_(c, "ping", "-c", "1", "-W", "2", plan.addresses[dst], check=False)
        report["ping"][f"{c}->{dst}"] = r.returncode == 0
    if not all(report["ping"].values()):
        report["ok"] = False
        log("ping failures: " + ", ".join(k for k, v in report["ping"].items() if not v))
        return report
    log(f"ping: {len(report['ping'])}/{len(report['ping'])} reachable")

    # 1b. Round-trip time to the farthest worker. Delay is applied at every egress point,
    # so the expected added RTT is 2 × (links on the route) × one-way delay.
    far = plan.hosts[-1]
    r = exec_(c, "ping", "-c", "20", "-i", "0.2", "-q", plan.addresses[far])
    rtt_avg = float(r.stdout.strip().splitlines()[-1].split("=")[1].split("/")[1])
    expected = 2 * len(plan.topo.route(c, far)) * (plan.latency_us or 0) / 1000
    rtt = {"dst": far, "avg_ms": rtt_avg, "expected_added_ms": expected}
    if plan.latency_us:
        # Allow 50% + 0.3 ms of slack for the unshaped baseline and scheduling noise.
        rtt["ok"] = abs(rtt_avg - expected) <= 0.5 * expected + 0.3
        report["ok"] &= rtt["ok"]
    report["rtt"] = rtt
    log(f"rtt:  {c}->{far} avg {rtt_avg:.3f} ms"
        + (f" (expected ≈{expected:.3f} ms) {'OK' if rtt['ok'] else 'MISMATCH — netem delay not honoured at this resolution'}"
           if plan.latency_us else ""))

    # 2. Path: traffic to the farthest worker must cross every inter-switch link on its route.
    far = plan.hosts[-1]
    isl = [(u, v) for u, v in plan.topo.route(c, far)
           if u in plan.switches and v in plan.switches]
    before = {h: _bytes_sent(h[0], f"p-{h[1]}") for h in isl}
    exec_(far, "iperf3", "-s", "-D", "-1", "-p", "5299")
    time.sleep(0.5)
    exec_(c, "iperf3", "-c", plan.addresses[far], "-p", "5299", "-n", "20M")
    for h in isl:
        delta = _bytes_sent(h[0], f"p-{h[1]}") - before[h]
        ok = delta >= 20_000_000
        report["path"][f"{h[0]}->{h[1]}"] = {"bytes": delta, "ok": ok}
        report["ok"] &= ok
    if isl:
        log("path: " + ", ".join(f"{k} {v['bytes'] / 1e6:.1f} MB"
                                  for k, v in report["path"].items()))

    # 3. Throughput vs max-min fair prediction from the topology.
    for name, flows in verify_tests(plan).items():
        pred = predicted_rates(plan, flows)
        meas = run_flows(plan, flows, seconds)
        tot_p = sum(pred)
        tot_m = sum(m or 0 for m in meas)
        ratio = tot_m / tot_p if tot_p else 0
        ok = all(m is not None for m in meas) and (1 - tolerance) <= ratio <= 1.05
        report["throughput"][name] = {
            "flows": [{"src": s, "dst": d, "predicted_mbps": round(p, 1),
                       "measured_mbps": None if m is None else round(m, 1)}
                      for (s, d), p, m in zip(flows, pred, meas)],
            "predicted_total_mbps": round(tot_p, 1),
            "measured_total_mbps": round(tot_m, 1),
            "ratio": round(ratio, 3),
            "ok": ok,
        }
        report["ok"] &= ok
        log(f"{name:7s} {len(flows)} flow(s): predicted {tot_p:7.0f} Mb/s, "
            f"measured {tot_m:7.0f} Mb/s ({ratio:.0%}) {'OK' if ok else 'MISMATCH'}")
    return report
