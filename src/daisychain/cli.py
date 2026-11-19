"""`daisychain` command-line interface."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import secrets
import sys
from pathlib import Path

from . import collect_inventory, emu, inventory, schemas
from .topology import PATTERNS, Topology, TopologyError, describe_link


def _table(header: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(str(c)) for c in col) for col in zip(header, *rows)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*header), fmt.format(*("-" * w for w in widths))]
    lines += [fmt.format(*map(str, r)) for r in rows]
    return "\n".join(lines)


# ---- inventory ---------------------------------------------------------------------------


def cmd_inventory_collect(args) -> int:
    rec = collect_inventory.collect()
    text = json.dumps(rec, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(text)
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


def cmd_inventory_check(args) -> int:
    topo = Topology.load(args.topology) if args.topology else None
    rep = inventory.check([Path(p) for p in args.files], topo)
    if rep.hosts:
        print(_table(["host", "cpu", "cores p/l", "RAM GiB", "NUMA", "SSD GB", "NICs Mb/s", "OS"],
                     inventory.summary_rows(rep.hosts)))
        print()
    for f in rep.findings:
        print(f"[{f.level.upper()}] {f.message}")
    print(f"\n{len(rep.hosts)} host(s); "
          f"{sum(f.level == 'error' for f in rep.findings)} error(s), "
          f"{sum(f.level == 'warn' for f in rep.findings)} warning(s)")
    return 0 if rep.ok else 1


# ---- topology ----------------------------------------------------------------------------


def cmd_topology_validate(args) -> int:
    rc = 0
    for p in args.files:
        try:
            t = Topology.load(p)
            print(f"OK    {p}: {t.name} — {len(t.switches)} switch(es), "
                  f"{len(t.workers)} worker(s), link speeds: {t.link_gbps} Gb/s ({t.link_source})")
        except (TopologyError, OSError) as e:
            print(f"FAIL  {p}: {e}")
            rc = 1
    return rc


def cmd_topology_routes(args) -> int:
    t = Topology.load(args.file)
    c = t.controller.id
    rows = []
    for w in t.workers:
        hops = t.route(c, w.id)
        rows.append([f"{c}→{w.id}", t.inter_switch_hops(c, w.id),
                     " → ".join([hops[0][0]] + [v for _, v in hops])])
    print(f"{t.name}: controller-to-worker routes")
    print(_table(["flow", "inter-switch hops", "path"], rows))
    return 0


def cmd_topology_load(args) -> int:
    t = Topology.load(args.file)
    flows = t.pattern_flows(args.pattern, args.workers)
    load = t.link_load(flows)
    cap_mbps = t.link_gbps * 1000
    print(f"{t.name}: pattern={args.pattern}, workers={args.workers or len(t.workers)}, "
          f"{len(flows)} concurrent flow(s), {t.link_gbps} Gb/s per direction ({t.link_source})")
    print("First-order static analysis — equal sharing, no protocol overhead. Not a simulation.\n")
    rows = []
    for hop, n in sorted(load.items(), key=lambda kv: (-kv[1], describe_link(t, kv[0]))):
        rows.append([describe_link(t, hop), n, f"{cap_mbps / n:.0f}"])
    print(_table(["directed link", "flows", "equal share Mb/s"], rows))
    if args.payload_mb:
        busiest = max(load.values())
        bound = busiest * args.payload_mb * 8 / cap_mbps
        print(f"\nLower bound to move {args.payload_mb} MB per flow: {bound:.1f} s "
              f"(busiest link carries {busiest} flow(s))")
    return 0


# ---- schemas / manifests ------------------------------------------------------------------


def cmd_validate(args) -> int:
    rc = 0
    for p in args.files:
        path = Path(p)
        try:
            records = schemas.load_records(path)
        except (OSError, ValueError) as e:
            print(f"FAIL  {p}: {e}")
            rc = 1
            continue
        bad = 0
        for i, rec in enumerate(records, 1):
            for err in schemas.errors(args.schema, rec):
                prefix = f"{p}:{i}" if len(records) > 1 else str(p)
                print(f"FAIL  {prefix}: {err}")
                bad += 1
        if bad:
            rc = 1
        else:
            print(f"OK    {p} ({len(records)} record(s))")
    return rc


def cmd_run_id(args) -> int:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"{ts}-{args.experiment}-{secrets.token_hex(3)}")
    return 0


# ---- emulated cluster ---------------------------------------------------------------------


def cmd_emu_up(args) -> int:
    topo = Topology.load(args.topology)
    plan = emu.make_plan(topo, args.workers, args.gbps, args.latency_us, args.topology)
    emu.up(plan, build=args.build, worker_cores=args.worker_cores,
           worker_memory_mb=args.worker_memory_mb, slurm=not args.no_slurm,
           keep_on_failure=args.keep_on_failure)
    print(_table(["node", "address", "switch"],
                 [[h, plan.addresses[h], topo.hosts[h].switch] for h in plan.hosts]))
    print(f"\nshell: docker exec -it {emu.container(plan.hosts[0])} bash")
    return 0


def cmd_emu_down(args) -> int:
    emu.down()
    return 0


def cmd_emu_status(args) -> int:
    nodes = emu.running_nodes()
    if not nodes:
        print("no emulated cluster")
        return 1
    labels = next(iter(nodes.values()))["labels"]
    print(f"topology {labels.get(emu.LABEL)}, {labels.get('daisychain.workers')} worker(s), "
          f"{labels.get('daisychain.gbps')} Gb/s links")
    print(_table(["node", "role", "container", "state"],
                 [[n, v["labels"].get("daisychain.role", "?"), v["container"], v["state"]]
                  for n, v in sorted(nodes.items())]))
    return 0 if all(v["state"] == "running" for v in nodes.values()) else 1


def cmd_emu_verify(args) -> int:
    plan = emu.plan_from_running()
    print(f"verifying {plan.topo.name} with {len(plan.hosts) - 1} worker(s) — "
          "emulation, not hardware evidence")
    report = emu.verify(plan, seconds=args.seconds, tolerance=args.tolerance)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if report["ok"] else "FAIL")
    return 0 if report["ok"] else 1


def cmd_emu_slurm_verify(args) -> int:
    from dataclasses import asdict

    from . import slurm

    plan = emu.plan_from_running()
    print(f"Slurm checks on {plan.topo.name}, {len(plan.hosts) - 1} worker(s) — emulation")
    checks = slurm.verify(plan, skip_slow=args.skip_slow)
    ok = all(c.ok for c in checks)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"topology": plan.topo.name, "workers": len(plan.hosts) - 1, "ok": ok,
             "checks": [asdict(c) for c in checks]}, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def cmd_emu_stage_image(args) -> int:
    from . import containers

    plan = emu.plan_from_running()
    if args.image == containers.PROBE_IMAGE:
        containers.build_probe()
    containers.stage_image(plan, args.image, args.name)
    return 0


def cmd_emu_container_verify(args) -> int:
    from dataclasses import asdict

    from . import containers

    plan = emu.plan_from_running()
    if None in containers.staged_ids(plan, containers.PROBE_NAME).values():
        containers.build_probe()
        containers.stage_image(plan, containers.PROBE_IMAGE, containers.PROBE_NAME)
    print(f"Containerised Slurm steps on {plan.topo.name}, {len(plan.hosts) - 1} worker(s) "
          "— emulation")
    checks = containers.verify(plan)
    ok = all(c.ok for c in checks)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"topology": plan.topo.name, "workers": len(plan.hosts) - 1, "ok": ok,
             "launcher": containers.launcher().strip(),
             "checks": [asdict(c) for c in checks]}, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def cmd_emu_nfs_verify(args) -> int:
    from dataclasses import asdict

    from . import storage

    plan = emu.plan_from_running()
    print(f"NFS and staging checks on {plan.topo.name}, {len(plan.hosts) - 1} worker(s) — emulation")
    checks = storage.verify(plan, size_mb=args.size_mb)
    ok = all(c.ok for c in checks)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"topology": plan.topo.name, "workers": len(plan.hosts) - 1, "ok": ok,
             "checks": [asdict(c) for c in checks]}, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def cmd_emu_publish_synthetic(args) -> int:
    from . import storage

    plan = emu.plan_from_running()
    aid = storage.publish_synthetic(plan, args.name, args.size_mb, args.files, args.replace)
    print(f"published models/{args.name}: {args.size_mb} MiB in {args.files} file(s), "
          f"artifact {aid}")
    return 0


def cmd_emu_staging_bench(args) -> int:
    from dataclasses import asdict

    from . import storage

    plan = emu.plan_from_running()
    n_workers = len(plan.hosts) - 1
    counts = [int(c) for c in args.counts.split(",") if int(c) <= n_workers]
    caches = args.cache.split(",")
    run_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-storage-startup-{secrets.token_hex(3)}"
    print(f"{run_id}: staging {args.artifact} on {plan.topo.name}, N={counts}, "
          f"cache={caches}, {args.repeats} repeat(s) — emulation, evidence tier 'designed'")
    runs = storage.bench(plan, args.artifact, counts, args.repeats, caches)
    rows = storage.summarize(runs)
    print()
    isl_keys = list(rows[0]["isl_mb_measured"]) if rows else []
    fwd = [k for k in isl_keys if k in ("S1->S2", "S2->S3", "S3->S4")]
    print(_table(
        ["cache", "N", "workers", "makespan s", "min possible s", "efficiency", "aggregate Mb/s",
         "valid"]
        + [f"{k} MB meas/exp" for k in fwd],
        [[r["cache"], r["n"], ",".join(r["nodes"]),
          f"{r['makespan_mean_s']:.2f} ± {r['makespan_sd_s']:.2f}", f"{r['predicted_min_s']:.2f}",
          f"{r['efficiency']:.0%}", f"{r['aggregate_mbps']:.0f}", "yes" if r["valid"] else "NO"]
         + [f"{r['isl_mb_measured'][k]:.0f}/{r['isl_mb_expected'][k]:.0f}" for k in fwd]
         for r in rows]))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"run_id": run_id, "experiment": "storage-startup", "evidence_tier": "designed",
             "topology": plan.topo.name, "gbps": plan.gbps, "artifact": args.artifact,
             "summary": rows, "runs": [asdict(r) for r in runs]}, indent=2) + "\n")
        print(f"\nwrote {args.output}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="daisychain", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    inv = sub.add_parser("inventory", help="collect and check host inventories").add_subparsers(
        dest="sub", required=True)
    c = inv.add_parser("collect", help="inventory this host")
    c.add_argument("-o", "--output")
    c.set_defaults(func=cmd_inventory_collect)
    c = inv.add_parser("check", help="validate and compare inventory records")
    c.add_argument("files", nargs="+")
    c.add_argument("--topology", help="require a record for every host in this topology")
    c.set_defaults(func=cmd_inventory_check)

    top = sub.add_parser("topology", help="validate and analyse topologies").add_subparsers(
        dest="sub", required=True)
    c = top.add_parser("validate")
    c.add_argument("files", nargs="+")
    c.set_defaults(func=cmd_topology_validate)
    c = top.add_parser("routes", help="controller-to-worker paths")
    c.add_argument("file")
    c.set_defaults(func=cmd_topology_routes)
    c = top.add_parser("load", help="flows per directed link for a traffic pattern")
    c.add_argument("file")
    c.add_argument("--pattern", choices=PATTERNS, default="fanout")
    c.add_argument("--workers", type=int, help="use only the first N workers")
    c.add_argument("--payload-mb", type=float, help="per-flow payload for a transfer-time bound")
    c.set_defaults(func=cmd_topology_load)

    em = sub.add_parser("emu", help="emulated cluster in Docker").add_subparsers(
        dest="sub", required=True)
    c = em.add_parser("up", help="start containers and wire the topology")
    c.add_argument("--topology", default="config/topology/chain.yaml")
    c.add_argument("--workers", type=int, help="use only the first N workers")
    c.add_argument("--gbps", type=float, help="override link speed per direction")
    c.add_argument("--latency-us", type=float, help="add one-way delay per link endpoint")
    c.add_argument("--worker-cores", type=int, default=2, help="CPUs pinned per worker")
    c.add_argument("--worker-memory-mb", type=int, default=1024, help="memory cap per worker")
    c.add_argument("--no-slurm", action="store_true", help="network fabric only")
    c.add_argument("--keep-on-failure", action="store_true", help="leave containers for debugging")
    c.add_argument("--build", action="store_true", help="rebuild the node image")
    c.set_defaults(func=cmd_emu_up)
    c = em.add_parser("down", help="remove all emulated containers")
    c.set_defaults(func=cmd_emu_down)
    c = em.add_parser("status")
    c.set_defaults(func=cmd_emu_status)
    c = em.add_parser("verify", help="check reachability, paths and link sharing")
    c.add_argument("--seconds", type=int, default=5)
    c.add_argument("--tolerance", type=float, default=0.15,
                   help="allowed shortfall of measured vs predicted aggregate")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_verify)
    c = em.add_parser("slurm-verify", help="Slurm allocation, limits, exit codes, cleanup")
    c.add_argument("--skip-slow", action="store_true", help="skip the ~1 min timeout check")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_slurm_verify)

    c = em.add_parser("stage-image", help="convert a Docker image and stage it on every worker")
    c.add_argument("--image", default="daisychain/probe:dev")
    c.add_argument("--name", default="probe", help="directory name under the image store")
    c.set_defaults(func=cmd_emu_stage_image)
    c = em.add_parser("container-verify",
                      help="Slurm/container gate: identity, limits, exit codes, cleanup")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_container_verify)

    c = em.add_parser("nfs-verify", help="NFS mounts, permissions, verified staging, results")
    c.add_argument("--size-mb", type=int, default=64)
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_nfs_verify)
    c = em.add_parser("publish-synthetic", help="publish random 'weights' to models/<name>")
    c.add_argument("name")
    c.add_argument("--size-mb", type=int, default=256)
    c.add_argument("--files", type=int, default=4)
    c.add_argument("--replace", action="store_true")
    c.set_defaults(func=cmd_emu_publish_synthetic)
    c = em.add_parser("staging-bench", help="cold-start staging time vs simultaneous workers")
    c.add_argument("--artifact", default="models/synthetic-256m")
    c.add_argument("--counts", default="1,2,4,8")
    c.add_argument("--repeats", type=int, default=3)
    c.add_argument("--cache", default="cold", help="comma list of cold,warm-server")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_staging_bench)

    c = sub.add_parser("validate", help="validate JSON/JSONL files against a schema")
    c.add_argument("schema", choices=schemas.SCHEMA_NAMES)
    c.add_argument("files", nargs="+")
    c.set_defaults(func=cmd_validate)

    c = sub.add_parser("run-id", help="generate a run ID for a new manifest")
    c.add_argument("experiment")
    c.set_defaults(func=cmd_run_id)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (TopologyError, emu.EmuError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
