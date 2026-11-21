#!/usr/bin/env python3
"""Stage an immutable artifact from shared storage to local disk, verifying checksums (F7).

Standard library only; installed on every node as `daisychain-stage` and normally run as a
Slurm step, one task per node:

    srun -N4 --ntasks-per-node=1 daisychain-stage models/synthetic-256m

Artifact layout on shared storage (written once by the publisher, then read-only):

    /shared/<kind>/<name>/MANIFEST.sha256   lines of "<sha256>  <relative path>"
    /shared/<kind>/<name>/<files…>

The artifact ID is the SHA-256 of the manifest itself. Staging:

1. Skips the copy if `<dest>/<name>/.daisychain-artifact` already holds the same ID.
2. Takes a per-node lock, so concurrent tasks on one node never write the same files.
3. Streams each file from shared storage to a private temporary directory, hashing while
   copying, and fails without publishing anything on the first mismatch.
4. Optionally unpacks a single `image.tar` (container images), then renames the result
   into place atomically.

Prints one JSON line describing the outcome. Exit codes: 0 ok, 3 checksum mismatch,
4 I/O error or timeout.

If --metrics-dir exists, the outcome is also written there for node_exporter's textfile
collector (PRD F8 model-load time), one file per artifact, replaced atomically.
"""

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

MARKER = ".daisychain-artifact"
METRICS_DIR = "/var/lib/daisychain/metrics"
CHUNK = 4 << 20


class StageError(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


def read_manifest(src):
    path = os.path.join(src, "MANIFEST.sha256")
    with open(path, "rb") as f:
        raw = f.read()
    entries = []
    for line in raw.decode().splitlines():
        if not line.strip():
            continue
        digest, _, rel = line.partition("  ")
        rel = rel.lstrip("*")
        if len(digest) != 64 or not rel or rel.startswith("/") or ".." in rel.split("/"):
            raise StageError(4, "malformed manifest line: %r" % line)
        entries.append((digest, rel))
    if not entries:
        raise StageError(4, "empty manifest")
    return hashlib.sha256(raw).hexdigest(), entries


def copy_verified(src_file, dst_file, want):
    h = hashlib.sha256()
    n = 0
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
    with open(src_file, "rb") as fi, open(dst_file, "wb") as fo:
        while True:
            buf = fi.read(CHUNK)
            if not buf:
                break
            h.update(buf)
            fo.write(buf)
            n += len(buf)
        fo.flush()
        os.fsync(fo.fileno())
    if h.hexdigest() != want:
        raise StageError(3, "checksum mismatch for %s" % os.path.basename(src_file))
    return n


def current_id(final):
    try:
        with open(os.path.join(final, MARKER)) as f:
            return f.read().strip()
    except OSError:
        return None


def stage(artifact, root, dest, untar):
    name = artifact.rstrip("/").split("/")[-1]
    src = os.path.join(root, artifact)
    final = os.path.join(dest, name)
    os.makedirs(dest, exist_ok=True)

    artifact_id, entries = read_manifest(src)
    if current_id(final) == artifact_id:
        return {"cache_hit": True, "artifact_id": artifact_id, "bytes": 0, "files": 0}

    with open(os.path.join(dest, ".%s.lock" % name), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if current_id(final) == artifact_id:  # another task on this node finished first
            return {"cache_hit": True, "artifact_id": artifact_id, "bytes": 0, "files": 0,
                    "waited_for_lock": True}
        tmp = os.path.join(dest, ".%s.partial-%d" % (name, os.getpid()))
        shutil.rmtree(tmp, ignore_errors=True)
        try:
            total = 0
            for digest, rel in entries:
                total += copy_verified(os.path.join(src, rel), os.path.join(tmp, rel), digest)
            if untar:
                if [rel for _, rel in entries] != ["image.tar"]:
                    raise StageError(4, "--untar needs an artifact containing only image.tar")
                root_dir = tmp + ".root"
                os.makedirs(root_dir)
                r = subprocess.run(["tar", "-xf", os.path.join(tmp, "image.tar"), "-C", root_dir,
                                    "--no-same-owner"], stderr=subprocess.PIPE,
                                   universal_newlines=True)
                if r.returncode != 0:
                    shutil.rmtree(root_dir, ignore_errors=True)
                    raise StageError(4, "untar failed: %s" % r.stderr.strip()[-300:])
                shutil.rmtree(tmp)
                tmp = root_dir
            with open(os.path.join(tmp, MARKER), "w") as f:
                f.write(artifact_id + "\n")
            if os.path.exists(final):
                old = "%s.old-%d" % (final, os.getpid())
                os.rename(final, old)
                os.rename(tmp, final)
                shutil.rmtree(old, ignore_errors=True)
            else:
                os.rename(tmp, final)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.rmtree(tmp + ".root", ignore_errors=True)
            raise
    return {"cache_hit": False, "artifact_id": artifact_id, "bytes": total,
            "files": len(entries)}


def write_metrics(out, metrics_dir):
    """Best effort: metrics must never turn a good staging run into a failure."""
    if not metrics_dir or not os.path.isdir(metrics_dir):
        return
    art = out["artifact"].replace("\\", "/").replace('"', "")
    lab = 'artifact="%s"' % art
    lines = [
        "# HELP daisychain_stage_seconds Wall time of the last staging run",
        "# TYPE daisychain_stage_seconds gauge",
        "daisychain_stage_seconds{%s} %s" % (lab, out["seconds"]),
        "# HELP daisychain_stage_bytes Bytes copied by the last staging run (0 on cache hit)",
        "# TYPE daisychain_stage_bytes gauge",
        "daisychain_stage_bytes{%s} %d" % (lab, out.get("bytes") or 0),
        "# HELP daisychain_stage_success 1 if the last staging run succeeded",
        "# TYPE daisychain_stage_success gauge",
        "daisychain_stage_success{%s} %d" % (lab, out["status"] == "ok"),
        "# HELP daisychain_stage_cache_hit 1 if the last run reused a verified local copy",
        "# TYPE daisychain_stage_cache_hit gauge",
        "daisychain_stage_cache_hit{%s} %d" % (lab, bool(out.get("cache_hit"))),
        "# HELP daisychain_stage_timestamp_seconds When the last staging run finished",
        "# TYPE daisychain_stage_timestamp_seconds gauge",
        "daisychain_stage_timestamp_seconds{%s} %.3f" % (lab, time.time()),
    ]
    name = "stage-%s.prom" % art.replace("/", "_")
    tmp = os.path.join(metrics_dir, ".%s.%d" % (name, os.getpid()))
    try:
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.rename(tmp, os.path.join(metrics_dir, name))
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("artifact", help="<kind>/<name>, e.g. models/synthetic-256m")
    p.add_argument("--root", default="/shared")
    p.add_argument("--dest", default=None,
                   help="default: /var/lib/daisychain/images for images/*, else …/stage")
    p.add_argument("--untar", action="store_true", help="unpack image.tar after verifying")
    p.add_argument("--timeout", type=int, default=600, help="overall bound in seconds")
    p.add_argument("--metrics-dir", default=METRICS_DIR,
                   help="node_exporter textfile directory ('' to disable)")
    args = p.parse_args(argv)

    dest = args.dest or ("/var/lib/daisychain/images" if args.artifact.startswith("images/")
                         else "/var/lib/daisychain/stage")

    def on_alarm(signum, frame):
        raise StageError(4, "timed out after %ds" % args.timeout)

    signal.signal(signal.SIGALRM, on_alarm)
    signal.alarm(args.timeout)
    out = {"host": socket.gethostname(), "artifact": args.artifact, "dest": dest}
    t0 = time.monotonic()
    code = 0
    try:
        out.update(stage(args.artifact, args.root, dest, args.untar))
        out["status"] = "ok"
    except StageError as e:
        out.update(status="checksum_mismatch" if e.code == 3 else "error", error=str(e))
        code = e.code
    except OSError as e:
        out.update(status="error", error="%s: %s" % (type(e).__name__, e))
        code = 4
    finally:
        signal.alarm(0)
    elapsed = time.monotonic() - t0
    out["seconds"] = round(elapsed, 3)
    if out.get("bytes") and elapsed > 0:
        out["mbps"] = round(out["bytes"] * 8 / 1e6 / elapsed, 1)
    write_metrics(out, args.metrics_dir)
    print(json.dumps(out), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
