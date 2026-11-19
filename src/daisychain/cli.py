"""`daisychain` command-line interface."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import secrets
import sys
from pathlib import Path

from . import collect_inventory, inventory, schemas
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
    except TopologyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
