"""Containerised job steps inside Slurm allocations (PRD §6 Slurm/container gate).

Chosen launch approach: images are built with Docker, converted to Apptainer sandboxes,
staged onto each worker's local disk, and run from inside Slurm steps with
`srun … apptainer exec <image> …`. Apptainer is the non-setuid build: the container runs
as the submitting user in a user namespace with no daemon, so its processes stay inside
the Slurm step's cgroup, where Slurm tracks, limits and cleans them up.

Images reach workers the same way as model weights: published once to NFS, then copied
to local disk with checksum verification (storage.py, stage.py).

Emulator-only detail: running SIF files fails inside the Docker Desktop VM because
squashfuse cannot exec binaries there (`exec … failed: invalid argument`), so images are
staged as sandbox directories. Re-test SIF on real hardware.
"""

from __future__ import annotations

from . import emu, slurm

IMAGE_DIR = "/var/lib/daisychain/images"
PROBE_IMAGE = "daisychain/probe:dev"
PROBE_NAME = "probe"
ID_FILE = ".daisychain-artifact"  # written by daisychain-stage


def image_path(name: str) -> str:
    return f"{IMAGE_DIR}/{name}"


def launcher(name: str = PROBE_NAME) -> str:
    return f"apptainer exec {image_path(name)} "


def build_probe() -> None:
    emu._docker("build", "-q", "-t", PROBE_IMAGE, str(emu.REPO_ROOT / "emu/probe"))


def staged_ids(plan: emu.Plan, name: str) -> dict[str, str | None]:
    """Artifact ID recorded in each worker's staged copy (None if absent)."""
    ids = {}
    for w in plan.hosts[1:]:
        r = emu.exec_(w, "cat", f"{image_path(name)}/{ID_FILE}", check=False)
        ids[w] = r.stdout.strip() or None
    return ids


def stage_image(plan: emu.Plan, docker_image: str, name: str, log=print) -> str:
    """Publish a Docker image to NFS as an Apptainer sandbox tarball, then stage it.

    Conversion happens once on C0; every worker copies images/<name>/image.tar over the
    emulated fabric with checksum verification and unpacks it into its local image store
    (daisychain-stage --untar). Returns the artifact ID.
    """
    from . import storage

    artifact_id = storage.publish_image(plan, docker_image, name)
    log(f"published images/{name} from {docker_image} (artifact {artifact_id[:12]})")
    res = storage.stage(plan, f"images/{name}", untar=True)
    bad = [r for r in res if r["status"] != "ok"]
    if bad:
        raise emu.EmuError(f"staging images/{name} failed: {bad}")
    ids = staged_ids(plan, name)
    if set(ids.values()) != {artifact_id}:
        raise emu.EmuError(f"staged artifact IDs differ across workers: {ids}")
    slowest = max(r["seconds"] for r in res)
    log(f"staged {image_path(name)} on {len(ids)} worker(s) via NFS (slowest {slowest:.1f}s)")
    return artifact_id


IDENTITY_PROBE = (
    'echo "$(hostname) uid=$(id -u) os=$(. /etc/os-release; echo $ID-$VERSION_ID) '
    'cg=$(cut -d: -f3 /proc/self/cgroup)"'
)


def check_identity(plan: emu.Plan, name: str = PROBE_NAME) -> slurm.Check:
    """Tasks run inside the image, as the submitting user, inside the Slurm step cgroup."""
    workers = plan.hosts[1:]
    n = len(workers)
    r = slurm._as_user(plan, f"srun -N{n} --ntasks-per-node=1 -c1 --mem=256M -t 1 "
                             f"{launcher(name)}bash -c '{IDENTITY_PROBE}'")
    rows = [line.split() for line in r.stdout.splitlines() if line.strip()]
    good = [row for row in rows
            if len(row) == 4 and row[0] in workers and row[1] == f"uid={slurm.USER_UID}"
            and row[2] == "os=debian-12" and "/job_" in row[3] and "/step_" in row[3]]
    ok = r.returncode == 0 and len(good) == n and len({row[0] for row in good}) == n
    if ok:
        detail = (f"all {n} workers: uid={slurm.USER_UID}, os=debian-12, task cgroup under "
                  f"its Slurm step (e.g. {good[0][3][3:]})")
    else:
        detail = "; ".join(" ".join(row) for row in rows) or r.stderr.strip()[-300:]
    return slurm.Check("container_identity", ok, detail, {"rows": rows})


def check_staged(plan: emu.Plan, name: str = PROBE_NAME) -> slurm.Check:
    ids = staged_ids(plan, name)
    ok = None not in ids.values() and len(set(ids.values())) == 1
    detail = (f"{image_path(name)} on {len(ids)} worker(s), artifact {next(iter(ids.values()))[:12]}"
              if ok else f"missing or inconsistent: {ids}")
    return slurm.Check("container_staged", ok, detail, {"ids": ids})


def verify(plan: emu.Plan, log=print) -> list[slurm.Check]:
    cores = int(next(iter(emu.running_nodes().values()))["labels"]["daisychain.worker_cores"])
    host_cpus = slurm.parse_slurmd_c(emu.exec_(plan.hosts[1], "slurmd", "-C").stdout)["CPUs"]
    run = launcher()
    label = "container_"
    steps = [
        lambda: check_staged(plan),
        lambda: check_identity(plan),
        lambda: slurm.check_limits(plan, cores, host_cpus, launcher=run, label=label),
        lambda: slurm.check_oom(plan, launcher=run, label=label),
        lambda: slurm.check_exit_codes(plan, launcher=run, label=label),
        lambda: slurm.check_cancel_cleanup(plan, launcher=run, label=label),
    ]
    results = []
    for step in steps:
        c = step()
        results.append(c)
        log(f"{'OK  ' if c.ok else 'FAIL'}  {c.name:28s} {c.detail}")
        if c.name in ("container_staged",) and not c.ok:
            break
    return results
