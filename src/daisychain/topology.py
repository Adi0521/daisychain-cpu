"""Topology definitions: validation, routing, and first-order link-sharing analysis.

This is a static analysis aid for M0 design decisions, not a simulator. It counts how
many concurrent flows of a traffic pattern cross each *directed* link (full duplex means
each direction has its own capacity) and reports a naive equal-share bandwidth bound.
Contention, protocol overhead, and latency are left to the calibrated SimGrid model (M4).
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

import yaml


class TopologyError(ValueError):
    pass


@dataclass(frozen=True)
class Host:
    id: str
    role: str  # "controller" | "worker"
    switch: str


@dataclass
class Topology:
    name: str
    kind: str
    hosts: dict[str, Host]
    switches: list[str]
    switch_links: list[tuple[str, str]]
    link_gbps: float
    link_source: str
    link_latency_us: float | None = None
    switch_capacity_gbps: dict[str, float | None] = field(default_factory=dict)

    # ---- construction -------------------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> "Topology":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "Topology":
        try:
            defaults = data.get("link_defaults") or {}
            switches_raw = data["switches"]
            topo = cls(
                name=data["name"],
                kind=data.get("kind", "custom"),
                hosts={},
                switches=[s["id"] for s in switches_raw],
                switch_links=[(l["a"], l["b"]) for l in data.get("switch_links") or []],
                link_gbps=float(defaults.get("gbps", 1.0)),
                link_source=defaults.get("source", "unspecified"),
                link_latency_us=defaults.get("latency_us"),
                switch_capacity_gbps={
                    s["id"]: s.get("switching_capacity_gbps") for s in switches_raw
                },
            )
            for h in data["hosts"]:
                if h["id"] in topo.hosts:
                    raise TopologyError(f"duplicate host id {h['id']!r}")
                topo.hosts[h["id"]] = Host(h["id"], h["role"], h["switch"])
        except KeyError as e:
            raise TopologyError(f"missing required field {e}") from None
        topo.validate()
        return topo

    # ---- validation -----------------------------------------------------------------

    def validate(self) -> None:
        sw = set(self.switches)
        if len(sw) != len(self.switches):
            raise TopologyError("duplicate switch id")
        if sw & set(self.hosts):
            raise TopologyError("host and switch ids overlap")

        controllers = [h for h in self.hosts.values() if h.role == "controller"]
        if len(controllers) != 1:
            raise TopologyError(f"expected exactly one controller, found {len(controllers)}")
        for h in self.hosts.values():
            if h.role not in ("controller", "worker"):
                raise TopologyError(f"host {h.id}: unknown role {h.role!r}")
            if h.switch not in sw:
                raise TopologyError(f"host {h.id}: unknown switch {h.switch!r}")

        seen: set[frozenset[str]] = set()
        for a, b in self.switch_links:
            if a not in sw or b not in sw:
                raise TopologyError(f"switch link {a}-{b} references unknown switch")
            if a == b:
                raise TopologyError(f"switch link {a}-{b} is a self-loop")
            key = frozenset((a, b))
            if key in seen:
                raise TopologyError(f"duplicate switch link {a}-{b} (parallel links form a loop)")
            seen.add(key)

        # A connected graph with V-1 edges is a tree, i.e. has no loops.
        if len(self._switch_component(self.switches[0])) != len(sw):
            raise TopologyError("switch graph is not connected")
        if len(self.switch_links) != len(sw) - 1:
            raise TopologyError(
                "switch graph contains a loop; the PRD forbids physical loops "
                "(no spanning-tree reliance)"
            )

    def _adjacency(self) -> dict[str, list[str]]:
        adj: dict[str, list[str]] = {s: [] for s in self.switches}
        for a, b in self.switch_links:
            adj[a].append(b)
            adj[b].append(a)
        return adj

    def _switch_component(self, start: str) -> set[str]:
        adj, seen, q = self._adjacency(), {start}, deque([start])
        while q:
            for n in adj[q.popleft()]:
                if n not in seen:
                    seen.add(n)
                    q.append(n)
        return seen

    # ---- routing --------------------------------------------------------------------

    @property
    def controller(self) -> Host:
        return next(h for h in self.hosts.values() if h.role == "controller")

    @property
    def workers(self) -> list[Host]:
        return sorted(
            (h for h in self.hosts.values() if h.role == "worker"),
            key=lambda h: (len(h.id), h.id),
        )

    def _switch_path(self, src: str, dst: str) -> list[str]:
        adj, prev, q = self._adjacency(), {src: None}, deque([src])
        while q:
            cur = q.popleft()
            if cur == dst:
                break
            for n in adj[cur]:
                if n not in prev:
                    prev[n] = cur
                    q.append(n)
        path, cur = [], dst
        while cur is not None:
            path.append(cur)
            cur = prev[cur]
        return path[::-1]

    def route(self, src: str, dst: str) -> list[tuple[str, str]]:
        """Directed links traversed from host src to host dst (host NICs included)."""
        if src == dst:
            return []
        a, b = self.hosts[src], self.hosts[dst]
        sw_path = self._switch_path(a.switch, b.switch)
        hops = [(src, a.switch)]
        hops += list(zip(sw_path, sw_path[1:]))
        hops.append((b.switch, dst))
        return hops

    def inter_switch_hops(self, src: str, dst: str) -> int:
        return sum(1 for u, v in self.route(src, dst) if u in self.switches and v in self.switches)

    # ---- traffic patterns -----------------------------------------------------------

    def pattern_flows(self, pattern: str, n_workers: int | None = None) -> list[tuple[str, str]]:
        workers = [w.id for w in self.workers]
        if n_workers is not None:
            if not 1 <= n_workers <= len(workers):
                raise TopologyError(f"n_workers must be in 1..{len(workers)}")
            workers = workers[:n_workers]
        c = self.controller.id
        if pattern == "fanout":  # e.g. NFS model load, request dispatch
            return [(c, w) for w in workers]
        if pattern == "fanin":  # e.g. responses, result publication, metric scrapes
            return [(w, c) for w in workers]
        if pattern == "pipeline":  # Mode B activations W1→W2→…→Wn
            return list(zip(workers, workers[1:]))
        if pattern == "all-to-all":  # collective stress
            return [(a, b) for a in workers for b in workers if a != b]
        raise TopologyError(f"unknown pattern {pattern!r}")

    def link_load(self, flows: list[tuple[str, str]]) -> Counter:
        load: Counter = Counter()
        for s, d in flows:
            for hop in self.route(s, d):
                load[hop] += 1
        return load


PATTERNS = ("fanout", "fanin", "pipeline", "all-to-all")


def describe_link(topo: Topology, hop: tuple[str, str]) -> str:
    u, v = hop
    if u in topo.hosts:
        return f"{u} NIC tx"
    if v in topo.hosts:
        return f"{v} NIC rx"
    return f"{u}→{v}"
