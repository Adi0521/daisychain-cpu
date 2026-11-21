#!/usr/bin/env python3
"""Minimal Prometheus exporter for Slurm job and node state (PRD F8). Runs on C0.

Standard library only. Each scrape runs `sinfo` and `squeue` once (a few ms on this
cluster); there is no caching, so the scrape interval bounds the load on slurmctld.

    daisychain-slurm-exporter --listen 10.10.0.10:9341
"""

import argparse
import subprocess
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

JOB_STATES = ("PENDING", "RUNNING", "COMPLETING", "COMPLETED", "FAILED", "CANCELLED",
              "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY")


def _run(*cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True, timeout=10)


def _esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def collect():
    t0 = time.monotonic()
    lines = []
    ok = 1

    def metric(name, help_, kind, samples):
        lines.append("# HELP %s %s" % (name, help_))
        lines.append("# TYPE %s %s" % (name, kind))
        for labels, value in samples:
            lab = ",".join('%s="%s"' % (k, _esc(v)) for k, v in sorted(labels.items()))
            lines.append("%s%s %s" % (name, "{%s}" % lab if lab else "", value))

    r = _run("sinfo", "-h", "-N", "-o", "%N %T %C")
    if r.returncode == 0:
        nodes = {}
        cpus = Counter()
        for line in r.stdout.split("\n"):
            parts = line.split()
            if len(parts) != 3:
                continue
            node, state, c = parts
            # "idle*" etc.: a trailing * means the node is not responding.
            nodes[node] = state
            a, i, o, t = (int(x) for x in c.split("/"))
            cpus.update(alloc=a, idle=i, other=o, total=t)
        metric("daisychain_slurm_node_state", "1 for the node's current Slurm state",
               "gauge", [({"node": n, "state": s.rstrip("*~#!%$@^-+"),
                           "responding": "false" if s.endswith("*") else "true"}, 1)
                         for n, s in sorted(nodes.items())])
        by_state = Counter(s.rstrip("*~#!%$@^-+") for s in nodes.values())
        metric("daisychain_slurm_nodes", "Nodes by Slurm state", "gauge",
               [({"state": s}, n) for s, n in sorted(by_state.items())])
        metric("daisychain_slurm_cpus", "CPUs by allocation state", "gauge",
               [({"state": s}, n) for s, n in sorted(cpus.items())])
    else:
        ok = 0

    r = _run("squeue", "-h", "-t", "all", "-o", "%T")
    if r.returncode == 0:
        jobs = Counter(s.strip() for s in r.stdout.split("\n") if s.strip())
        # Always emit the common states so dashboards show 0 rather than "no data".
        metric("daisychain_slurm_jobs", "Jobs known to slurmctld by state (finished jobs "
               "stay visible for MinJobAge)", "gauge",
               [({"state": s}, jobs.get(s, 0)) for s in sorted(set(JOB_STATES) | set(jobs))])
    else:
        ok = 0

    metric("daisychain_slurm_scrape_success", "1 if sinfo and squeue both succeeded",
           "gauge", [({}, ok)])
    metric("daisychain_slurm_scrape_duration_seconds", "Time spent querying Slurm", "gauge",
           [({}, round(time.monotonic() - t0, 4))])
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] != "/metrics":
            self.send_error(404)
            return
        body = collect().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--listen", default="0.0.0.0:9341")
    a = p.parse_args()
    host, port = a.listen.rsplit(":", 1)
    ThreadingHTTPServer((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
