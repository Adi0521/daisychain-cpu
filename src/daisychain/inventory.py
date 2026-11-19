"""Cross-host inventory checks: schema validity, consistency (F1), and straggler risk (§12)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from . import schemas
from .topology import Topology

# Software that must be identical on every node for results to be comparable (F1, §14.2).
PINNED_SOFTWARE = ("docker_server", "slurm", "mpirun", "pmix", "python3", "nfs_utils")
RAM_TOLERANCE = 0.05  # flag RAM totals that differ by more than 5% between nodes


@dataclass
class Finding:
    level: str  # "error" | "warn"
    message: str


@dataclass
class InventoryReport:
    hosts: dict[str, dict] = field(default_factory=dict)
    findings: list[Finding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(f.level == "error" for f in self.findings)

    def add(self, level: str, msg: str) -> None:
        self.findings.append(Finding(level, msg))


def _distinct(hosts: dict[str, dict], getter) -> dict:
    values: dict = {}
    for name, rec in hosts.items():
        try:
            v = getter(rec)
        except (KeyError, TypeError):
            v = None
        values.setdefault(v, []).append(name)
    return values


def check(paths: list[Path], topology: Topology | None = None) -> InventoryReport:
    rep = InventoryReport()
    for p in paths:
        try:
            rec = json.loads(p.read_text())
        except (OSError, ValueError) as e:
            rep.add("error", f"{p}: unreadable ({e})")
            continue
        errs = schemas.errors("inventory", rec)
        if errs:
            rep.add("error", f"{p}: schema violations: " + "; ".join(errs[:5]))
            continue
        name = rec["hostname"]
        if name in rep.hosts:
            rep.add("error", f"{p}: duplicate hostname {name}")
            continue
        rep.hosts[name] = rec
        for w in rec["warnings"]:
            rep.add("warn", f"{name}: {w}")

    hosts = rep.hosts
    if not hosts:
        rep.add("error", "no valid inventory records")
        return rep

    if topology is not None:
        missing = sorted(set(topology.hosts) - set(hosts))
        extra = sorted(set(hosts) - set(topology.hosts))
        if missing:
            rep.add("error", f"topology hosts without inventory: {', '.join(missing)}")
        if extra:
            rep.add("warn", f"inventory hosts not in topology {topology.name}: {', '.join(extra)}")

    for name, rec in hosts.items():
        if rec["time_sync"]["ntp_synchronized"] is False:
            rep.add("error", f"{name}: clock not synchronized")
        swap = rec["memory"].get("swap_total_bytes")
        if swap:
            rep.add("warn", f"{name}: swap enabled ({swap / 2**30:.1f} GiB); "
                            "swapping will distort latency — record or disable")
        cpu = rec["cpu"]
        if cpu.get("architecture") not in (None, "x86_64"):
            rep.add("warn", f"{name}: architecture {cpu.get('architecture')} differs from the "
                            "x86-64 planning assumption")
        gov = cpu.get("scaling_governor")
        if gov and gov != "performance":
            rep.add("warn", f"{name}: CPU governor is {gov!r}; frequency scaling adds variance")
        phys = [n for n in rec["nics"] if n["physical"] and n["operstate"] == "up"]
        if rec["cluster_node_supported"] and not phys:
            rep.add("error", f"{name}: no physical NIC with link up")
        if rec["cluster_node_supported"] and rec["disks"] and all(d["rotational"] for d in rec["disks"]):
            rep.add("warn", f"{name}: no SSD found; PRD assumes local SSD for staged weights")
        if rec["os"].get("cgroup_version") == "v1":
            rep.add("warn", f"{name}: cgroup v1; confirm Slurm/container limit enforcement")

    if len(hosts) > 1:
        for label, getter in (
            ("CPU model", lambda r: r["cpu"]["model"]),
            ("physical cores", lambda r: r["cpu"]["physical_cores"]),
            ("OS release", lambda r: r["os"]["pretty_name"]),
            ("kernel", lambda r: r["os"]["kernel"]),
            ("NIC speeds (Mb/s)",
             lambda r: tuple(sorted(n["speed_mbps"] or 0 for n in r["nics"] if n["physical"]))),
        ):
            vals = _distinct(hosts, getter)
            if len(vals) > 1:
                detail = "; ".join(f"{v!r}: {', '.join(sorted(h))}" for v, h in vals.items())
                rep.add("warn", f"heterogeneous {label} (straggler/consistency risk): {detail}")

        rams = {n: r["memory"]["total_bytes"] for n, r in hosts.items() if r["memory"]["total_bytes"]}
        if rams and (max(rams.values()) - min(rams.values())) / max(rams.values()) > RAM_TOLERANCE:
            rep.add("warn", "heterogeneous RAM: " + ", ".join(
                f"{n}={b / 2**30:.1f}GiB" for n, b in sorted(rams.items())))

        for key in PINNED_SOFTWARE:
            vals = _distinct(hosts, lambda r, k=key: r["software"].get(k))
            if len(vals) > 1:
                detail = "; ".join(f"{v!r}: {', '.join(sorted(h))}" for v, h in vals.items())
                rep.add("error", f"software version mismatch for {key}: {detail}")

    return rep


def summary_rows(hosts: dict[str, dict]) -> list[list[str]]:
    rows = []
    for name in sorted(hosts):
        r = hosts[name]
        cpu, mem = r["cpu"], r["memory"]
        nics = ",".join(
            f"{n['name']}:{n['speed_mbps'] or '?'}" for n in r["nics"] if n["physical"]
        ) or "-"
        ssd = sum((d["size_bytes"] or 0) for d in r["disks"] if not d["rotational"])
        rows.append([
            name,
            (cpu.get("model") or "?")[:34],
            f"{cpu.get('physical_cores') or '?'}/{cpu.get('logical_cpus') or '?'}",
            f"{(mem['total_bytes'] or 0) / 2**30:.0f}",
            str(r["numa"].get("node_count") or "?"),
            f"{ssd / 1e9:.0f}" if ssd else "-",
            nics,
            r["os"].get("distro_version") or "?",
        ])
    return rows
