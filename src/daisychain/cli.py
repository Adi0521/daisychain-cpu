"""`daisychain` command-line interface."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import secrets
import sys
import time
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
           keep_on_failure=args.keep_on_failure, monitoring=not args.no_monitoring,
           ui_ports={"grafana": args.grafana_port, "prometheus": args.prometheus_port})
    print(_table(["node", "address", "switch"],
                 [[h, plan.addresses[h], topo.hosts[h].switch] for h in plan.hosts]))
    print(f"\nshell: docker exec -it {emu.container(plan.hosts[0])} bash")
    return 0


def cmd_monitoring_render(args) -> int:
    from . import monitoring

    plan = emu.make_plan(Topology.load(args.topology), topology_path=args.topology)
    out = Path(args.out)
    for rel, content in monitoring.render(plan).items():
        (out / rel).parent.mkdir(parents=True, exist_ok=True)
        (out / rel).write_text(content)
        print(f"wrote {out / rel}")
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


def cmd_emu_mpi_verify(args) -> int:
    from dataclasses import asdict

    from . import mpi

    plan = emu.plan_from_running()
    mpi.ensure_staged(plan, rebuild=args.rebuild)
    print(f"MPI in containerised Slurm steps on {plan.topo.name}, {len(plan.hosts) - 1} "
          "worker(s) — emulation")
    checks = mpi.verify(plan)
    ok = all(c.ok for c in checks)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"topology": plan.topo.name, "workers": len(plan.hosts) - 1, "ok": ok,
             "launch": mpi.srun_cmd(["<nodes>"], "<args>"),
             "checks": [asdict(c) for c in checks]}, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def _fmt_measured(r) -> str:
    m = r.measured
    if r.bench == "pingpong":
        return f"{m['lat_us_p50']:.1f} µs, {m['mbps_p50']:.0f} Mb/s"
    if r.bench == "bw":
        return f"{m['mbps']:.0f} Mb/s"
    if r.bench == "layer":
        return f"{m['interval_ms']:.2f} ms/batch"
    return f"{m['us_p50'] / 1000:.2f} ms"


def _fmt_predicted(r) -> str:
    p = r.predicted
    if "mbps" in p:
        return f"{p['mbps']:.0f} Mb/s"
    if "us_min" in p:
        return f"≥{p['us_min'] / 1000:.2f} ms"
    if "interval_ms" in p:
        return f"≥{p['interval_ms']:.2f} ms/batch"
    if "serialization_us" in p:
        return f"≥{p['serialization_us']:.1f} µs wire"
    return ""


def cmd_emu_mpi_bench(args) -> int:
    from dataclasses import asdict

    from . import mpi

    plan = emu.plan_from_running()
    image_id = mpi.ensure_staged(plan)
    groups = args.groups.split(",")
    n_workers = len(plan.hosts) - 1
    counts = [int(c) for c in args.counts.split(",") if int(c) <= n_workers]
    burst = emu.tbf_burst_bytes(plan.gbps)
    run_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-mpi-microbench-{secrets.token_hex(3)}"
    print(f"{run_id}: {','.join(groups)} on {plan.topo.name}, N={counts}, {args.repeats} "
          f"repeat(s) — emulation, evidence tier 'designed'")
    t0 = time.monotonic()
    rows = mpi.bench(plan, groups, counts, args.repeats, args.microbatches, args.compute_us)
    print(f"\n{len(rows)} measurements in {time.monotonic() - t0:.0f}s "
          f"(* = within the shaper's {burst // 1000} KB burst; not representative)\n")
    print(_table(["group", "bench", "nodes", "bytes", "measured (median)", "bound"],
                 [[r.group, r.bench, f"{r.nodes[0]}..{r.nodes[-1]} ({len(r.nodes)})",
                   f"{r.bytes}{'*' if r.burst_affected else ''}", _fmt_measured(r),
                   _fmt_predicted(r)] for r in rows]))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"run_id": run_id, "experiment": "mpi-microbench", "evidence_tier": "designed",
             "topology": plan.topo.name, "gbps": plan.gbps, "shaper_burst_bytes": burst,
             "image_artifact": image_id, "mca": mpi.MCA,
             "launch": mpi.srun_cmd(["<nodes>"], "<args>"),
             "rows": [asdict(r) for r in rows]}, indent=2) + "\n")
        print(f"\nwrote {args.output}")
    return 0


def cmd_emu_monitor_verify(args) -> int:
    from dataclasses import asdict

    from . import monitoring

    plan = emu.plan_from_running()
    print(f"Monitoring checks on {plan.topo.name}, {len(plan.hosts) - 1} worker(s) — emulation")
    checks = monitoring.verify(plan)
    ok = all(c.ok for c in checks)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"topology": plan.topo.name, "workers": len(plan.hosts) - 1, "ok": ok,
             "scrape_interval_s": monitoring.SCRAPE_S,
             "checks": [asdict(c) for c in checks]}, indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def cmd_emu_monitor_overhead(args) -> int:
    from . import monitoring

    plan = emu.plan_from_running()
    run_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-monitoring-overhead-{secrets.token_hex(3)}"
    print(f"{run_id}: scraping on vs off (Prometheus paused) on {plan.topo.name} — emulation")
    rep = monitoring.overhead(plan, idle_s=args.idle_seconds, repeats=args.repeats)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"run_id": run_id, "experiment": "monitoring-overhead", "evidence_tier": "designed",
             "topology": plan.topo.name, "scrape_interval_s": monitoring.SCRAPE_S, **rep},
            indent=2) + "\n")
        print(f"wrote {args.output}")
    return 0


def cmd_emu_monitor_export(args) -> int:
    from . import monitoring

    plan = emu.plan_from_running()
    end = time.time()
    start = end - args.minutes * 60
    run_id = args.run_id or f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-metrics-export-{secrets.token_hex(3)}"
    series = monitoring.export(start, end)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(
        {"run_id": run_id, "topology": plan.topo.name, "evidence_tier": "designed",
         "start": start, "end": end, "step_s": monitoring.SCRAPE_S,
         "queries": monitoring.EXPORT_SERIES, "series": series}, indent=1) + "\n")
    n = sum(len(v) for v in series.values())
    print(f"wrote {args.output}: {len(series)} metrics, {n} series, {args.minutes} min")
    return 0


def cmd_emu_fail(args) -> int:
    from dataclasses import asdict

    from . import failures

    plan = emu.plan_from_running()
    ws = plan.hosts[1:]
    victim = args.victim or ws[len(ws) // 2]
    run_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-failure-{args.scenario}-{secrets.token_hex(3)}"
    print(f"{run_id} on {plan.topo.name} — emulation, evidence tier 'designed'")
    if args.scenario in ("worker-kill", "worker-freeze"):
        res = failures.worker_loss(plan, victim, mode=args.scenario.split("-")[1],
                                   freeze_s=args.seconds)
    elif args.scenario == "link-down":
        if not plan.topo.switch_links:
            raise emu.EmuError("link-down needs a topology with inter-switch links")
        a, b = args.link.split("-") if args.link else plan.topo.switch_links[len(plan.topo.switch_links) // 2]
        res = failures.link_down(plan, a, b, args.seconds)
    else:
        res = failures.nfs_down(plan, args.seconds)
    ok = all(v[0] for v in res.checks.values())
    print()
    for name, (good, detail) in res.checks.items():
        print(f"{'OK  ' if good else 'FAIL'}  {name:26s} {detail}")
    print("metrics: " + ", ".join(f"{k}={v}" for k, v in res.metrics.items()))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(
            {"run_id": run_id, "experiment": "failure", "evidence_tier": "designed",
             "topology": plan.topo.name, "ok": ok,
             **{k: v for k, v in asdict(res).items() if k != "checks"},
             "checks": {k: {"ok": g, "detail": d} for k, (g, d) in res.checks.items()}},
            indent=2) + "\n")
        print(f"wrote {args.output}")
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


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

    mon = sub.add_parser("monitoring", help="monitoring configuration").add_subparsers(
        dest="sub", required=True)
    c = mon.add_parser("render", help="write Prometheus and Grafana config for a topology")
    c.add_argument("--topology", default="config/topology/chain.yaml")
    c.add_argument("--out", default="config/monitoring")
    c.set_defaults(func=cmd_monitoring_render)

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
    c.add_argument("--no-monitoring", action="store_true",
                   help="skip exporters, Prometheus and Grafana")
    c.add_argument("--grafana-port", type=int, default=3000, help="on 127.0.0.1 of this machine")
    c.add_argument("--prometheus-port", type=int, default=9090, help="on 127.0.0.1 of this machine")
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

    c = em.add_parser("mpi-verify",
                      help="MPI in containers: versions, placement, TCP path, failure, cleanup")
    c.add_argument("--rebuild", action="store_true", help="rebuild and restage the MPI image")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_mpi_verify)
    c = em.add_parser("mpi-bench", help="F6 ping-pong, bandwidth, collective, layer benchmarks")
    c.add_argument("--groups", default="p2p,cross,coll,layer")
    c.add_argument("--counts", default="2,4,8", help="rank counts for collectives/pipelines")
    c.add_argument("--repeats", type=int, default=3)
    c.add_argument("--microbatches", type=int, default=32)
    c.add_argument("--compute-us", type=float, default=0, help="per-stage busy time (layer)")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_mpi_bench)

    c = em.add_parser("monitor-verify",
                      help="exporters, switch counters, Slurm/staging metrics, 30 s detection")
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_monitor_verify)

    c = em.add_parser("monitor-overhead", help="fabric/CPU cost of monitoring and workload impact")
    c.add_argument("--idle-seconds", type=int, default=30)
    c.add_argument("--repeats", type=int, default=3)
    c.add_argument("--output", help="write JSON report here")
    c.set_defaults(func=cmd_emu_monitor_overhead)
    c = em.add_parser("monitor-export", help="export recent metrics as JSON (time series)")
    c.add_argument("--minutes", type=float, default=15)
    c.add_argument("--run-id", help="link the export to an experiment run")
    c.add_argument("--output", required=True)
    c.set_defaults(func=cmd_emu_monitor_export)

    c = em.add_parser("fail", help="failure experiment: inject a fault under load, time "
                                   "detection, cleanup and recovery")
    c.add_argument("scenario", choices=["worker-kill", "worker-freeze", "link-down", "nfs-down"])
    c.add_argument("--victim", help="worker to kill/freeze (default: middle worker)")
    c.add_argument("--link", help="switch link to cut, e.g. S2-S3 (default: middle link)")
    c.add_argument("--seconds", type=float, default=60, help="fault duration (freeze/link/nfs)")
    c.add_argument("--output", help="write JSON record here")
    c.set_defaults(func=cmd_emu_fail)

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
