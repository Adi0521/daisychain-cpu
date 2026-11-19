"""Shared storage on the emulated cluster: publish to NFS, stage to local disk, measure.

C0 runs an NFSv4 kernel server exporting `/export`; workers mount it at `/shared` over the
emulated fabric. Artifacts are published once by the controller (written under a
temporary name, then renamed, so jobs never see partial files and nobody else writes
them) and staged to each worker's local disk by `daisychain-stage` running as a Slurm
step (see stage.py). Covers PRD F7 and the "Storage startup" experiment in §8.
"""

from __future__ import annotations

import json
import shlex
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from . import emu, slurm

EXPORT = "/export"
STAGE_DIR = "/var/lib/daisychain/stage"
IMAGE_DIR = "/var/lib/daisychain/images"


# ---- publishing (runs on C0 as root) -----------------------------------------------------


def _publish_script(kind: str, name: str, fill: str, replace: bool) -> str:
    final = f"{EXPORT}/{kind}/{name}"
    tmp = f"{EXPORT}/{kind}/.{name}.tmp"
    return f"""set -euo pipefail
final={shlex.quote(final)}; tmp={shlex.quote(tmp)}
if [ -e "$final" ]; then
  {"rm -rf \"$final\"" if replace else "echo \"$final already published (immutable)\" >&2; exit 17"}
fi
rm -rf "$tmp"; mkdir -p "$tmp"; cd "$tmp"
{fill}
find . -type f ! -name MANIFEST.sha256 -printf '%P\\n' | sort | xargs -d '\\n' sha256sum > MANIFEST.sha256
chmod -R a+rX "$tmp"
mv "$tmp" "$final"
sha256sum "$final/MANIFEST.sha256" | cut -d' ' -f1
"""


def publish_synthetic(plan: emu.Plan, name: str, size_mb: int, files: int = 4,
                      replace: bool = False) -> str:
    """Random 'weights' of the given size, split into shards. Returns the artifact ID."""
    per = size_mb * 2**20 // files
    fill = "\n".join(f"head -c {per} /dev/urandom > shard-{i:02d}.bin" for i in range(files))
    r = emu.exec_(plan.hosts[0], "bash", "-c", _publish_script("models", name, fill, replace))
    return r.stdout.strip().splitlines()[-1]


def publish_image(plan: emu.Plan, docker_image: str, name: str, replace: bool = True) -> str:
    """Docker image → Apptainer sandbox (converted once, on C0) → images/<name>/image.tar."""
    image_id = emu._docker("image", "inspect", "-f", "{{.Id}}", docker_image).stdout.strip()
    c0 = plan.hosts[0]
    with tempfile.TemporaryDirectory() as tmp:
        tar = Path(tmp) / f"{name}.tar"
        emu._docker("save", "-o", str(tar), docker_image)
        emu._docker("cp", str(tar), f"{emu.container(c0)}:/tmp/{name}.docker.tar")
    fill = (f"apptainer -q build --sandbox /tmp/{name}.sandbox "
            f"docker-archive:///tmp/{name}.docker.tar\n"
            f"tar -C /tmp/{name}.sandbox -cf image.tar .\n"
            f"rm -rf /tmp/{name}.sandbox /tmp/{name}.docker.tar")
    r = emu.exec_(c0, "bash", "-c", _publish_script("images", name, fill, replace))
    artifact_id = r.stdout.strip().splitlines()[-1]
    # Provenance next to (not inside) the artifact; staging copies only manifest entries.
    emu.exec_(c0, "bash", "-c",
              f"echo {shlex.quote(json.dumps({'docker_image': docker_image, 'image_id': image_id}))}"
              f" > {EXPORT}/images/{name}/SOURCE.json")
    return artifact_id


def artifact_size(plan: emu.Plan, artifact: str) -> int:
    """Total bytes of the files listed in the artifact's manifest."""
    r = emu.exec_(plan.hosts[0], "bash", "-c",
                  f"cd {EXPORT}/{artifact} && cut -d' ' -f3- MANIFEST.sha256 | "
                  "xargs -d '\\n' stat -c %s | awk '{s+=$1} END {print s}'")
    return int(r.stdout.strip())


def unpublish(plan: emu.Plan, artifact: str) -> None:
    emu.exec_(plan.hosts[0], "rm", "-rf", f"{EXPORT}/{artifact}")


# ---- staging (Slurm step on workers) ------------------------------------------------------


def stage(plan: emu.Plan, artifact: str, nodes: int | None = None, nodelist: str | None = None,
          tasks_per_node: int = 1, untar: bool = False, timeout: int = 600) -> list[dict]:
    """Run daisychain-stage on `nodes` workers (one task each); returns one dict per task."""
    n = nodes or len(plan.hosts) - 1
    where = f"-w {nodelist}" if nodelist else ""
    flags = " --untar" if untar else ""
    r = slurm._as_user(
        plan,
        f"srun -N{n} {where} --ntasks-per-node={tasks_per_node} -c1 --mem=512M -t 10 "
        f"daisychain-stage {shlex.quote(artifact)}{flags} --timeout {timeout}",
        timeout=timeout + 60)
    out = []
    for line in r.stdout.splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    if not out:
        raise emu.EmuError(f"staging {artifact} produced no results: {r.stderr.strip()[-300:]}")
    return out


def clear_local(plan: emu.Plan, artifact: str, workers: list[str] | None = None) -> None:
    name = artifact.rstrip("/").split("/")[-1]
    base = IMAGE_DIR if artifact.startswith("images/") else STAGE_DIR
    for w in workers or plan.hosts[1:]:
        emu.exec_(w, "rm", "-rf", f"{base}/{name}")


def drop_caches(plan: emu.Plan) -> None:
    """Drop the Docker VM's page cache (VM-wide: server and clients become cold)."""
    emu.exec_(plan.hosts[1], "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches")


def _tbf_bytes(node: str, dev: str) -> int:
    r = emu.exec_(node, "tc", "-s", "-j", "qdisc", "show", "dev", dev)
    return sum(int(q["bytes"]) for q in json.loads(r.stdout) if q.get("kind") == "tbf")


def link_counters(plan: emu.Plan) -> dict[tuple[str, str], int]:
    """Bytes sent on every directed link out of a switch (switch→switch and switch→host)."""
    out = {}
    for c in plan.cables:
        for sw, peer in ((c.a, c.b), (c.b, c.a)):
            if sw in plan.switches:
                out[(sw, peer)] = _tbf_bytes(sw, f"p-{peer}")
    return out


# ---- checks -------------------------------------------------------------------------------


Check = slurm.Check


def check_mounts(plan: emu.Plan) -> Check:
    bad = {}
    for w in plan.hosts[1:]:
        r = emu.exec_(w, "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "/shared", check=False)
        cols = r.stdout.split()  # findmnt pads columns
        opts = cols[2].split(",") if len(cols) == 3 else []
        if not (cols[:2] == ["C0:/", "nfs4"] and f"addr={plan.addresses['C0']}" in opts
                and "soft" in opts):
            bad[w] = r.stdout.strip() or "not mounted"
    return Check("nfs_mounted", not bad,
                 f"all {len(plan.hosts) - 1} workers mount C0:/ (nfs4, soft) from "
                 f"{plan.addresses['C0']} over the fabric" if not bad else f"problems: {bad}", bad)


def check_permissions(plan: emu.Plan) -> Check:
    w = plan.hosts[1]
    user = emu._docker("exec", "-u", slurm.USER, emu.container(w), "bash", "-c",
                       "touch /shared/models/.probe 2>/dev/null && echo models-writable; "
                       "f=/shared/results/.probe-$$; touch $f && echo results-ok && rm $f",
                       check=False).stdout
    root = emu.exec_(w, "bash", "-c", "touch /shared/models/.probe 2>/dev/null && echo root-writable",
                     check=False).stdout
    ok = "models-writable" not in user and "results-ok" in user and "root-writable" not in root
    return Check("nfs_permissions", ok,
                 "artifacts read-only to jobs and to worker root (root_squash); results/ writable "
                 "by the experiment user" if ok else f"user: {user.strip()!r}, root: {root.strip()!r}")


def check_stage_all(plan: emu.Plan, artifact: str, size_bytes: int, artifact_id: str) -> Check:
    workers = plan.hosts[1:]
    clear_local(plan, artifact)
    before = link_counters(plan)
    res = stage(plan, artifact)
    after = link_counters(plan)
    hosts = sorted(r["host"] for r in res)
    ok_rows = [r for r in res if r["status"] == "ok" and not r["cache_hit"]
               and r["bytes"] == size_bytes and r["artifact_id"] == artifact_id]
    # Every worker's download must show up on its switch's downlink port.
    short = [w for w in workers
             if after[(plan.topo.hosts[w].switch, w)] - before[(plan.topo.hosts[w].switch, w)]
             < size_bytes]
    ok = hosts == sorted(workers) and len(ok_rows) == len(workers) and not short
    secs = [r["seconds"] for r in res]
    return Check("stage_verified", ok,
                 f"{len(ok_rows)}/{len(workers)} workers staged {size_bytes / 2**20:.0f} MiB, "
                 f"checksums verified, same artifact ID; slowest {max(secs):.1f}s; "
                 f"downlink bytes ≥ artifact size on every worker port: {not short}",
                 {"results": res})


def check_warm(plan: emu.Plan, artifact: str) -> Check:
    res = stage(plan, artifact)
    hits = [r for r in res if r["status"] == "ok" and r["cache_hit"] and r["bytes"] == 0]
    return Check("stage_cache_hit", len(hits) == len(res),
                 f"{len(hits)}/{len(res)} workers reused the verified local copy (no data read)")


def check_same_node_lock(plan: emu.Plan, artifact: str) -> Check:
    w = plan.hosts[1]
    clear_local(plan, artifact, [w])
    res = stage(plan, artifact, nodes=1, nodelist=w, tasks_per_node=2)
    copied = [r for r in res if r["status"] == "ok" and not r["cache_hit"]]
    waited = [r for r in res if r["status"] == "ok" and r["cache_hit"] and r.get("waited_for_lock")]
    ok = len(res) == 2 and len(copied) == 1 and len(waited) == 1
    return Check("stage_same_node_lock", ok,
                 f"two tasks on {w}: {len(copied)} copied, {len(waited)} waited for the lock and "
                 "reused it (no concurrent writes)" if ok else f"results: {res}")


def check_corruption(plan: emu.Plan) -> Check:
    art = "models/verify-corrupt"
    publish_synthetic(plan, "verify-corrupt", 8, files=2, replace=True)
    emu.exec_(plan.hosts[0], "bash", "-c",
              f"printf X | dd of={EXPORT}/{art}/shard-01.bin bs=1 seek=4096 conv=notrunc status=none")
    w = plan.hosts[1]
    clear_local(plan, art, [w])
    res = stage(plan, art, nodes=1, nodelist=w)
    left = emu.exec_(w, "bash", "-c",
                     f"ls -a {STAGE_DIR} | grep -E '^(verify-corrupt|\\.verify-corrupt\\.partial)'",
                     check=False).stdout.split()
    unpublish(plan, art)
    ok = len(res) == 1 and res[0]["status"] == "checksum_mismatch" and not left
    return Check("stage_rejects_corruption", ok,
                 f"one flipped byte on the server: status={res[0]['status']} "
                 f"({res[0].get('error')}); local leftovers: {left or 'none'}")


def check_results_publication(plan: emu.Plan) -> Check:
    run = f"verify-{int(time.time())}"
    n = len(plan.hosts) - 1
    slurm._as_user(plan, f"srun -N{n} --ntasks-per-node=1 -c1 -t 1 bash -c "
                         f"'d=/shared/results/{run}; h=$(hostname); mkdir -p $d && "
                         f"hostname > $d/.$h.tmp && mv $d/.$h.tmp $d/$h.json'")
    files = emu.exec_(plan.hosts[0], "ls", f"{EXPORT}/results/{run}", check=False).stdout.split()
    emu.exec_(plan.hosts[0], "rm", "-rf", f"{EXPORT}/results/{run}")
    ok = sorted(files) == sorted(f"{w}.json" for w in plan.hosts[1:])
    return Check("results_publication", ok,
                 f"{len(files)}/{n} workers wrote results/{run}/<host>.json via temp+rename")


def verify(plan: emu.Plan, size_mb: int = 64, log=print) -> list[Check]:
    art_name = "verify-artifact"
    artifact = f"models/{art_name}"
    artifact_id = publish_synthetic(plan, art_name, size_mb, files=4, replace=True)
    size = (size_mb * 2**20 // 4) * 4
    steps = [
        lambda: check_mounts(plan),
        lambda: check_permissions(plan),
        lambda: check_stage_all(plan, artifact, size, artifact_id),
        lambda: check_warm(plan, artifact),
        lambda: check_same_node_lock(plan, artifact),
        lambda: check_corruption(plan),
        lambda: check_results_publication(plan),
    ]
    results = []
    for step in steps:
        c = step()
        results.append(c)
        log(f"{'OK  ' if c.ok else 'FAIL'}  {c.name:26s} {c.detail}")
    unpublish(plan, artifact)
    clear_local(plan, artifact)
    return results


# ---- storage-startup benchmark ------------------------------------------------------------


@dataclass
class BenchRun:
    n: int
    cache: str
    repeat: int
    nodes: list[str]
    makespan_s: float
    wall_s: float
    predicted_s: float
    aggregate_mbps: float
    per_node_s: dict[str, float]
    isl_bytes: dict[str, int]
    isl_expected: dict[str, int]
    downlink_ok: bool  # every staged worker's switch port carried ≥ the artifact size


def predicted_makespan(plan: emu.Plan, nodes: list[str], size_bytes: int) -> float:
    rates = emu.predicted_rates(plan, [(plan.hosts[0], w) for w in nodes])  # Mb/s
    return max(size_bytes * 8 / 1e6 / r for r in rates)


def bench(plan: emu.Plan, artifact: str, counts: list[int], repeats: int,
          caches: list[str], log=print) -> list[BenchRun]:
    c0 = plan.hosts[0]
    size = artifact_size(plan, artifact)
    isl = [(u, v) for (u, v) in link_counters(plan) if v in plan.switches]
    runs = []
    for cache in caches:
        for n in counts:
            for rep in range(1, repeats + 1):
                clear_local(plan, artifact)
                # Always drop the VM-wide page cache: otherwise workers' NFS *client* caches
                # from the previous repeat serve the reads and nothing crosses the fabric.
                drop_caches(plan)
                if cache == "warm-server":  # then warm only the server's copy
                    emu.exec_(c0, "bash", "-c", f"cat {EXPORT}/{artifact}/* > /dev/null")
                elif cache != "cold":
                    raise emu.EmuError(f"unknown cache mode {cache!r}")
                before = link_counters(plan)
                t0 = time.monotonic()
                res = stage(plan, artifact, nodes=n)
                wall = time.monotonic() - t0
                after = link_counters(plan)
                bad = [r for r in res if r["status"] != "ok" or r["cache_hit"]]
                if bad:
                    raise emu.EmuError(f"staging failed during benchmark: {bad}")
                nodes = sorted((r["host"] for r in res), key=lambda h: plan.hosts.index(h))
                per = {r["host"]: r["seconds"] for r in res}
                makespan = max(per.values())
                isl_bytes = {f"{u}->{v}": after[(u, v)] - before[(u, v)] for (u, v) in isl}
                downlink_ok = all(
                    after[(plan.topo.hosts[w].switch, w)] - before[(plan.topo.hosts[w].switch, w)]
                    >= size for w in nodes)
                expected = {}
                for (u, v) in isl:
                    k = sum(1 for w in nodes if (u, v) in plan.topo.route(c0, w))
                    expected[f"{u}->{v}"] = k * size
                run = BenchRun(n, cache, rep, nodes, round(makespan, 2), round(wall, 2),
                               round(predicted_makespan(plan, nodes, size), 2),
                               round(n * size * 8 / 1e6 / makespan, 1), per, isl_bytes, expected,
                               downlink_ok)
                runs.append(run)
                log(f"{cache:12s} N={n} rep {rep}: makespan {makespan:6.2f}s "
                    f"(predicted ≥{run.predicted_s:.2f}s), aggregate {run.aggregate_mbps:6.0f} Mb/s"
                    + ("" if downlink_ok else "  INVALID: data did not cross the fabric"))
    clear_local(plan, artifact)
    return runs


def summarize(runs: list[BenchRun]) -> list[dict]:
    rows = []
    for cache in dict.fromkeys(r.cache for r in runs):
        for n in sorted({r.n for r in runs if r.cache == cache}):
            rs = [r for r in runs if r.cache == cache and r.n == n]
            ms = [r.makespan_s for r in rs]
            first = rs[0]
            isl_meas = {k: statistics.mean(r.isl_bytes[k] for r in rs) for k in first.isl_bytes}
            rows.append({
                "cache": cache, "n": n, "nodes": first.nodes, "repeats": len(rs),
                "makespan_mean_s": round(statistics.mean(ms), 2),
                "makespan_sd_s": round(statistics.stdev(ms), 2) if len(ms) > 1 else 0.0,
                "predicted_min_s": first.predicted_s,
                "efficiency": round(first.predicted_s / statistics.mean(ms), 3),
                "aggregate_mbps": round(statistics.mean(r.aggregate_mbps for r in rs), 1),
                "isl_mb_measured": {k: round(v / 1e6, 1) for k, v in isl_meas.items()},
                "isl_mb_expected": {k: round(v / 1e6, 1) for k, v in first.isl_expected.items()},
                "valid": all(r.downlink_ok for r in rs),
            })
    return rows
