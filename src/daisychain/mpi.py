"""MPI inside containerised Slurm steps, and the F6 microbenchmarks.

Launch approach (decision 4):

    srun --mpi=pmix … apptainer exec /var/lib/daisychain/images/mpi dcbench …

* Slurm 23.11's mpi/pmix_v5 plugin and the image's Open MPI 4.1.6 use the same
  distribution PMIx (5.0.1). The plugin dlopens the unversioned libpmix.so, so nodes need
  libpmix-dev; without it the plugin fails with "can not load PMIx library".
* Inside the container, the PMIx client reaches the step's PMIx server over loopback TCP
  (PMIX_SERVER_URI=tcp4://127.0.0.1:…). Apptainer shares the host network namespace, so
  no bind mounts are needed, but running containers with an isolated network would break
  the launch.
* Open MPI is pinned to the ob1 PML and the TCP BTL on the data subnet, so inter-node
  traffic can only take the cabled fabric. The link-counter check confirms it does.
* KillOnBadExit=1 (slurm.conf): if one rank dies, the step ends instead of its peers
  waiting forever.

Emulation caveat: links are shaped by a token bucket whose depth (emu.tbf_burst_bytes,
500 KB at 1 Gb/s) is set by the VM's timer resolution. Messages that fit in it after an
idle gap cross at memory speed, so small-message latency and ping-pong bandwidth below a
few MB are not representative. Streaming bandwidth, contention between flows, and
collectives that move many MB per link are. Rows report `burst_affected` so analyses can
tell which is which.
"""

from __future__ import annotations

import json
import shlex
import statistics
import subprocess
import time
from collections import Counter
from dataclasses import dataclass, field

from . import containers, emu, slurm, storage
from .slurm import Check

IMAGE = "daisychain/mpi:dev"
NAME = "mpi"
STEP_MEM = "384M"  # per node; bw/alltoall at 4 MiB need ~70 MB per rank
WINDOW = 16  # dcbench bw: messages in flight per pair (matches WINDOW in dcbench.c)

MCA = {
    "pml": "ob1",
    "btl": "self,vader,tcp",
    "btl_tcp_if_include": str(emu.DATA_SUBNET),
    # Ranks on one node run in separate Apptainer user namespaces, so cross-memory attach
    # between them is not permitted; shared memory still works without single-copy.
    "btl_vader_single_copy_mechanism": "none",
}


def env_exports() -> str:
    return "".join(f"export OMPI_MCA_{k}={shlex.quote(v)}; " for k, v in MCA.items())


def build_image() -> None:
    emu._docker("build", "-q", "-t", IMAGE, str(emu.REPO_ROOT / "emu/mpi"))


def ensure_staged(plan: emu.Plan, rebuild: bool = False, log=print) -> str:
    """Stage the MPI job image on every worker unless an identical copy is already there."""
    ids = containers.staged_ids(plan, NAME)
    if rebuild or None in ids.values() or len(set(ids.values())) != 1:
        build_image()
        return containers.stage_image(plan, IMAGE, NAME, log=log)
    return next(iter(ids.values()))


def srun_cmd(nodes: list[str], args: str, tasks_per_node: int = 1, container: bool = True,
             minutes: int = 5, cpus_per_task: int = 1) -> str:
    run = containers.launcher(NAME) if container else ""
    return (f"{env_exports()}srun --mpi=pmix -N{len(nodes)} -w {','.join(nodes)} "
            f"--ntasks-per-node={tasks_per_node} -c{cpus_per_task} --mem={STEP_MEM} "
            f"-t {minutes} {run}dcbench {args}")


def worker_cores() -> int:
    return int(next(iter(emu.running_nodes().values()))["labels"]["daisychain.worker_cores"])


def parse(stdout: str) -> list[dict]:
    rows = []
    for line in stdout.splitlines():
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows


def run(plan: emu.Plan, nodes: list[str], args: str, tasks_per_node: int = 1,
        container: bool = True, timeout: int = 600) -> tuple[list[dict], subprocess.CompletedProcess]:
    # Each rank gets its share of the worker's cores. Busy-polling ranks pinned to one
    # core each would collide where emulated workers' CPU sets overlap.
    cpus = max(1, worker_cores() // tasks_per_node)
    r = slurm._as_user(plan, srun_cmd(nodes, args, tasks_per_node, container,
                                      minutes=max(2, timeout // 60 + 1), cpus_per_task=cpus),
                       timeout=timeout)
    return parse(r.stdout), r


# ---- predictions (pure) ---------------------------------------------------------------------


def link_loads(plan: emu.Plan, flows: list[tuple[str, str]]) -> Counter:
    """Number of flows on each directed link."""
    return Counter(hop for s, d in flows for hop in plan.topo.route(s, d))


def alltoall_min_s(plan: emu.Plan, nodes: list[str], bytes_per_dest: int) -> float:
    """Lower bound for one alltoall: the busiest directed link must carry all its flows."""
    flows = [(a, b) for a in nodes for b in nodes if a != b]
    worst = max(link_loads(plan, flows).values())
    return worst * bytes_per_dest * 8 / (plan.gbps * 1e9)


def pair_flows(nodes: list[str]) -> list[tuple[str, str]]:
    """dcbench bw pairing: rank i sends to rank i + k (ranks follow node order)."""
    k = len(nodes) // 2
    return [(nodes[i], nodes[i + k]) for i in range(k)]


def pipeline_interval_min_s(plan: emu.Plan, nodes: list[str], nbytes: int) -> float:
    """Lower bound on the steady-state interval of a stage-to-stage pipeline."""
    flows = list(zip(nodes, nodes[1:]))
    worst = max(link_loads(plan, flows).values())
    return worst * nbytes * 8 / (plan.gbps * 1e9)


# ---- checks ---------------------------------------------------------------------------------


PACKAGES = ("libopenmpi3t64", "libpmix2t64")


def package_versions(node: str, image: bool) -> dict[str, str]:
    q = ["dpkg-query", "-W", "-f", "${Package} ${Version}\\n", *PACKAGES]
    cmd = (["apptainer", "exec", containers.image_path(NAME)] if image else []) + q
    r = emu.exec_(node, *cmd, check=False)
    return dict(line.split(" ", 1) for line in r.stdout.splitlines() if " " in line)


def check_versions(plan: emu.Plan) -> Check:
    """Same Open MPI and PMIx inside and outside the image; both launch through Slurm."""
    w = plan.hosts[1:3]
    host_pkgs = package_versions(w[0], image=False)
    img_pkgs = package_versions(w[0], image=True)
    native, rn = run(plan, w, "info", container=False, timeout=120)
    inside, ri = run(plan, w, "info", timeout=120)
    lib_n = native[0]["library"] if native else None
    lib_i = inside[0]["library"] if inside else None
    ok = (bool(host_pkgs) and host_pkgs == img_pkgs and len(host_pkgs) == len(PACKAGES)
          and lib_n is not None and lib_n == lib_i)
    detail = (f"packages host={host_pkgs} image={img_pkgs}; dcbench reports native "
              f"'{lib_n}', container '{lib_i}'")
    if not ok and (rn.returncode or ri.returncode):
        detail += f"; stderr: {(rn.stderr + ri.stderr).strip()[-300:]}"
    return Check("mpi_versions_match", ok, detail,
                 {"host": host_pkgs, "image": img_pkgs, "library": lib_i})


def check_placement(plan: emu.Plan) -> Check:
    """One rank per allocated worker, inside the image, as the user, in the step cgroup."""
    workers = plan.hosts[1:]
    rows, r = run(plan, workers, "info", timeout=120)
    per = rows[0]["per_rank"] if rows else []
    good = [p for p in per
            if p["image"] == NAME and p["uid"] == slurm.USER_UID
            and "/job_" in p["cgroup"] and "/step_" in p["cgroup"]]
    hosts = [p["host"] for p in per]
    ok = r.returncode == 0 and len(good) == len(workers) and hosts == workers
    detail = (f"{len(per)} ranks on {','.join(hosts)}; in image '{NAME}', uid "
              f"{slurm.USER_UID}, step cgroup: {len(good)}/{len(workers)}")
    if not ok:
        detail += f"; {r.stderr.strip()[-300:]}"
    return Check("mpi_placement", ok, detail, {"per_rank": per})


def check_intranode(plan: emu.Plan) -> Check:
    """Two ranks on one worker (two containers) talk over shared memory."""
    w = plan.hosts[1]
    rows, r = run(plan, [w], "pingpong --sizes 8,1048576", tasks_per_node=2, timeout=120)
    ok = r.returncode == 0 and len(rows) == 2
    detail = (", ".join(f"{x['bytes']} B: {x['lat_us_p50']:.1f} µs" for x in rows)
              if ok else r.stderr.strip()[-300:])
    return Check("mpi_intranode", ok, f"2 ranks on {w}: {detail}", {"rows": rows})


def check_transport(plan: emu.Plan) -> Check:
    """Inter-node bytes appear on every switch egress port along the route."""
    a, b = plan.hosts[1], plan.hosts[-1]
    if a == b:
        return Check("mpi_transport_tcp", True, "skipped: needs 2 workers")
    hops = [h for h in plan.topo.route(a, b) if h[0] in plan.switches]
    before = storage.link_counters(plan)
    rows, r = run(plan, [a, b], "bw --sizes 4194304", timeout=180)
    after = storage.link_counters(plan)
    if r.returncode != 0 or not rows:
        return Check("mpi_transport_tcp", False, r.stderr.strip()[-300:])
    x = rows[0]
    payload = x["bytes"] * WINDOW * (x["iters"] + 2)  # dcbench runs 2 warm-up rounds
    carried = {f"{s}->{d}": after[(s, d)] - before[(s, d)] for s, d in hops}
    ok = all(v >= payload for v in carried.values())
    detail = (f"{a}->{b} streamed {payload / 1e6:.0f} MB at {x['total_mbps']:.0f} Mb/s "
              f"(BTL {MCA['btl']}, TCP restricted to {MCA['btl_tcp_if_include']}); "
              "switch egress: " + ", ".join(f"{k} {v / 1e6:.0f} MB" for k, v in carried.items()))
    return Check("mpi_transport_tcp", ok, detail,
                 {"payload_bytes": payload, "carried": carried, "mbps": x["total_mbps"]})


def check_rank_failure(plan: emu.Plan, bound_s: float = 30) -> Check:
    """A rank that dies ends the step promptly; the job fails; nothing is left running.

    srun's own exit code is 143 here, not the rank's code: Slurm reports the highest task
    exit code, and the surviving rank, killed by SIGTERM, counts as 128+15. The failing
    rank's code is in srun's stderr ("task N: Exited with exit code C").
    """
    nodes = [plan.hosts[1], plan.hosts[-1]] if len(plan.hosts) > 2 else plan.hosts[1:] * 2
    tpn = 1 if len(set(nodes)) == 2 else 2
    nodes = sorted(set(nodes), key=plan.hosts.index)
    t0 = time.monotonic()
    _, r = run(plan, nodes, "fail --rank 1 --code 3", tasks_per_node=tpn, timeout=300)
    elapsed = time.monotonic() - t0
    reported = "Exited with exit code 3" in r.stderr
    left = slurm._user_procs(plan)
    ok = r.returncode != 0 and reported and elapsed <= bound_s and not left
    return Check("mpi_rank_failure", ok,
                 f"rank 1 exits 3 while rank 0 waits on it: step ended after {elapsed:.1f}s "
                 f"(bound {bound_s:.0f}s), srun rc={r.returncode}, failing rank's code "
                 f"{'reported' if reported else 'NOT reported'} on stderr, leftover "
                 f"processes: {left or 'none'}",
                 {"elapsed_s": round(elapsed, 2), "srun_rc": r.returncode})


def check_cancel(plan: emu.Plan) -> Check:
    """scancel of a batch job running containerised MPI ranks removes every process."""
    nodes = plan.hosts[1:3]
    script = f"#!/bin/bash\n{srun_cmd(nodes, 'sleep --seconds 600', minutes=5)}\n"
    # The allocation must cover the step's --mem; the default (DefMemPerCPU) does not.
    r = slurm._as_user(plan, f"sbatch --parsable -N{len(nodes)} -w {','.join(nodes)} -t 5 "
                             f"--mem={STEP_MEM} -o /dev/null <<'EOF'\n{script}EOF")
    job = r.stdout.strip().split(";")[0]
    if r.returncode != 0 or not job:
        return Check("mpi_cancel_cleanup", False, f"submit failed: {r.stderr.strip()}")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        procs = slurm._user_procs(plan)
        if all(any("dcbench" in p for p in procs.get(n, [])) for n in nodes):
            break
        time.sleep(1)
    else:
        slurm._as_user(plan, f"scancel {job}")
        return Check("mpi_cancel_cleanup", False, f"job {job}: ranks never started")
    root = slurm._root_container_procs(nodes)
    t0 = time.monotonic()
    slurm._as_user(plan, f"scancel {job}")
    left = slurm._user_procs(plan)
    while left and time.monotonic() - t0 < slurm.KILL_WAIT_S + 30:
        time.sleep(0.5)
        left = slurm._user_procs(plan)
    elapsed = time.monotonic() - t0
    state = slurm._wait_terminal(plan, job)
    cgroups = slurm._job_cgroups(plan, job, nodes)
    ok = state == "CANCELLED" and not left and not cgroups and not root
    return Check("mpi_cancel_cleanup", ok,
                 f"job {job} on {','.join(nodes)}: state={state}, "
                 f"{'all processes gone' if not left else 'LEFTOVER ' + str(left)} after "
                 f"{elapsed:.1f}s; job cgroups left: {cgroups or 'none'}; root-owned runtime "
                 f"processes while running: {root or 'none'}",
                 {"cleanup_s": round(elapsed, 1)})


def verify(plan: emu.Plan, log=print) -> list[Check]:
    steps = [check_versions, check_placement, check_intranode, check_transport,
             check_rank_failure, check_cancel]
    results = []
    for step in steps:
        c = step(plan)
        results.append(c)
        log(f"{'OK  ' if c.ok else 'FAIL'}  {c.name:20s} {c.detail}")
        if c.name == "mpi_versions_match" and not c.ok:
            break
    return results


# ---- benchmarks -----------------------------------------------------------------------------

PINGPONG_SIZES = [1, 64, 4096, 65536, 1048576, 4194304]
BW_SIZES = [65536, 1048576, 4194304]
COLL_SIZES = {"allreduce": [4096, 262144, 4194304], "bcast": [4096, 262144, 4194304],
              "alltoall": [1024, 65536, 1048576]}
LAYER_SIZES = [65536, 1048576, 4194304]


@dataclass
class Row:
    group: str
    bench: str
    nodes: list[str]
    bytes: int
    measured: dict
    predicted: dict = field(default_factory=dict)
    burst_affected: bool = False


def pair_sets(plan: emu.Plan) -> dict[str, list[str]]:
    """Worker pairs at increasing switch distance, from the topology."""
    ws = plan.hosts[1:]
    out: dict[str, list[str]] = {}
    for b in ws[1:]:
        hops = plan.topo.inter_switch_hops(ws[0], b)
        out.setdefault(f"{hops}-isl", [ws[0], b])
    # Prefer W2-W3 for one inter-switch hop when it exists, to keep W1 out of every pair.
    for a, b in zip(ws, ws[1:]):
        if plan.topo.inter_switch_hops(a, b) == 1:
            out["1-isl"] = [a, b]
            break
    return dict(sorted(out.items()))


def bench(plan: emu.Plan, groups: list[str], counts: list[int], repeats: int = 1,
          microbatches: int = 32, compute_us: float = 0, log=print) -> list[Row]:
    burst = emu.tbf_burst_bytes(plan.gbps)
    cap_bps = plan.gbps * 1e9
    ws = plan.hosts[1:]
    rows: list[Row] = []

    def go(nodes, args, timeout=900):
        out = []
        for _ in range(repeats):
            res, r = run(plan, nodes, args, timeout=timeout)
            if r.returncode != 0:
                raise emu.EmuError(f"dcbench {args} on {nodes} failed: {r.stderr.strip()[-400:]}")
            out.append(res)
        return out

    def merge(runs: list[list[dict]], keys: list[str]) -> list[dict]:
        """Median of each metric across repeats, matched by position."""
        merged = []
        for parts in zip(*runs):
            m = dict(parts[0])
            for k in keys:
                m[k] = statistics.median(p[k] for p in parts)
            merged.append(m)
        return merged

    if "p2p" in groups:
        for label, pair in pair_sets(plan).items():
            log(f"p2p {label}: {pair[0]}<->{pair[1]}")
            sizes = ",".join(map(str, PINGPONG_SIZES))
            for x in merge(go(pair, f"pingpong --sizes {sizes}"),
                           ["lat_us_min", "lat_us_p50", "mbps_p50"]):
                rows.append(Row("p2p", "pingpong", pair, x["bytes"],
                                {k: x[k] for k in ("lat_us_min", "lat_us_p50", "mbps_p50")},
                                {"mbps_max": round(plan.gbps * 1000, 1),
                                 "serialization_us": round(x["bytes"] * 8 / cap_bps * 1e6, 2)},
                                burst_affected=x["bytes"] < 8 * burst))
            sizes = ",".join(map(str, BW_SIZES))
            for x in merge(go(pair, f"bw --sizes {sizes}"), ["total_mbps"]):
                rows.append(Row("p2p", "bw", pair, x["bytes"], {"mbps": x["total_mbps"]},
                                {"mbps": round(emu.predicted_rates(plan, [tuple(pair)])[0], 1)},
                                burst_affected=x["bytes"] * WINDOW < 8 * burst))

    if "cross" in groups and len(ws) >= 4:
        for n in [c for c in counts if c >= 4 and c % 2 == 0]:
            nodes = ws[:n]
            flows = pair_flows(nodes)
            pred = emu.predicted_rates(plan, flows)
            log(f"cross: {len(flows)} simultaneous pairs " + ", ".join(f"{a}->{b}" for a, b in flows))
            for x in merge(go(nodes, f"bw --sizes {','.join(map(str, BW_SIZES[1:]))}"),
                           ["total_mbps"]):
                rows.append(Row("cross", "bw", nodes, x["bytes"],
                                {"mbps": x["total_mbps"], "pair_mbps": x["pair_mbps"]},
                                {"mbps": round(sum(pred), 1),
                                 "pair_mbps": [round(p, 1) for p in pred]}))

    if "coll" in groups:
        for n in [c for c in counts if 2 <= c <= len(ws)]:
            nodes = ws[:n]
            for name, sizes in COLL_SIZES.items():
                log(f"coll: {name} on {n} ranks")
                for x in merge(go(nodes, f"{name} --sizes {','.join(map(str, sizes))}"),
                               ["us_min", "us_p50"]):
                    pred = ({"us_min": round(alltoall_min_s(plan, nodes, x["bytes"]) * 1e6, 1)}
                            if name == "alltoall" else {})
                    per_link = x["bytes"] * (n - 1) if name == "alltoall" else x["bytes"]
                    rows.append(Row("coll", name, nodes, x["bytes"],
                                    {"us_min": x["us_min"], "us_p50": x["us_p50"]}, pred,
                                    burst_affected=per_link < 8 * burst))

    if "layer" in groups:
        for n in [c for c in counts if 2 <= c <= len(ws)]:
            nodes = ws[:n]
            log(f"layer: {n}-stage pipeline, {microbatches} micro-batches, "
                f"{compute_us:g} µs compute per stage")
            args = (f"layer --sizes {','.join(map(str, LAYER_SIZES))} "
                    f"--microbatches {microbatches} --compute-us {compute_us}")
            for x in merge(go(nodes, args), ["makespan_ms", "first_ms", "interval_ms", "per_s"]):
                comm = pipeline_interval_min_s(plan, nodes, x["bytes"])
                rows.append(Row("layer", "layer", nodes, x["bytes"],
                                {k: x[k] for k in ("makespan_ms", "first_ms", "interval_ms", "per_s")},
                                {"interval_ms": round(max(comm, compute_us * 1e-6) * 1e3, 3)},
                                burst_affected=x["bytes"] < 8 * burst))
    return rows
