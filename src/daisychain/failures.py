"""Failure experiments on the emulated cluster (PRD §8 "Failure", §10, F10).

Each scenario runs while a real workload is active, injects one fault, and records a
timeline relative to the injection, from three independent observers:

* telemetry: Prometheus `up` for the victim (5 s scrape);
* Slurm: the victim's node state as slurmctld sees it;
* the workload: its job state, and whether traffic still flows.

It then checks cleanup on the surviving nodes, undoes the fault, and times recovery.

The request router does not exist yet, so "request outcomes" here are job outcomes. The
timings show what the router must handle itself. For example, if Slurm takes longer than
30 s to notice a dead worker, the router's own health checks must enforce the PRD's 30 s
bound.

Workload: an 8-stage MPI pipeline (dcbench layer, 64 KiB activations, 0.5 ms of compute
per stage per micro-batch). It runs for minutes and crosses every inter-switch link, so
every fault interrupts it. It is submitted with --no-requeue: by default Slurm requeues
a batch job after a node failure, and retry policy belongs to the harness and router,
not to Slurm.
"""

from __future__ import annotations

import shlex
import time
from dataclasses import dataclass, field

from . import emu, mpi, slurm, storage

PIPELINE_ARGS = "layer --sizes 65536 --microbatches 1000000 --compute-us 500"
POLL_S = 0.5


@dataclass
class Event:
    t: float
    what: str


@dataclass
class Result:
    scenario: str
    params: dict
    events: list[Event] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    checks: dict = field(default_factory=dict)  # name -> (ok, detail)

    def log(self, t: float, what: str, echo=print) -> None:
        self.events.append(Event(round(t, 2), what))
        echo(f"  t={t:6.1f}s  {what}")


# ---- observers -------------------------------------------------------------------------------


def _monitoring_running() -> bool:
    from . import monitoring
    return bool(monitoring.aux_containers())


def _telemetry_up(node: str) -> bool | None:
    from . import monitoring
    try:
        r = monitoring.query(f'up{{job="node",node="{node}"}}')
        return bool(r) and float(r[0]["value"][1]) == 1
    except Exception:  # noqa: BLE001
        return None


def _slurm_state(plan: emu.Plan, node: str) -> str:
    r = emu.exec_(plan.hosts[0], "sinfo", "-h", "-n", node, "-N", "-o", "%T", check=False)
    return r.stdout.strip() or "?"


def _job_state(plan: emu.Plan, job: str) -> str:
    return slurm._job_field(plan, job, "JobState") or "?"


class Watch:
    """Poll the observers and log every change, with times relative to the fault."""

    def __init__(self, plan: emu.Plan, res: Result, victims: list[str], job: str | None,
                 echo=print, survivors: list[str] | None = None):
        self.plan, self.res, self.victims, self.job, self.echo = plan, res, victims, job, echo
        self.survivors = survivors or []
        self.mon = _monitoring_running()
        self.t0 = time.monotonic()
        self.last: dict[str, object] = {}
        self.first: dict[str, float] = {}

    def now(self) -> float:
        return time.monotonic() - self.t0

    def mark(self, what: str) -> float:
        t = self.now()
        self.res.log(t, what, self.echo)
        return t

    def sample(self) -> None:
        obs: dict[str, object] = {}
        for v in self.victims:
            if self.mon:
                obs[f"telemetry {v}"] = {True: "up", False: "DOWN", None: "?"}[_telemetry_up(v)]
            obs[f"slurm {v}"] = _slurm_state(self.plan, v)
        if self.job:
            obs[f"job {self.job}"] = _job_state(self.plan, self.job)
        if self.survivors:
            alive = sum(_ranks_alive(n) for n in self.survivors)
            obs["ranks alive on survivors"] = f"{alive}/{len(self.survivors)}"
        t = self.now()
        for k, v in obs.items():
            if self.last.get(k) != v:
                if k in self.last:
                    self.res.log(t, f"{k}: {self.last[k]} -> {v}", self.echo)
                else:
                    self.res.log(t, f"{k}: {v}", self.echo)
                self.first.setdefault(f"{k}={v}", t)
                self.last[k] = v

    def baseline(self) -> None:
        """Record the pre-fault state at t=0, then start the clock."""
        self.sample()
        for e in self.res.events:
            e.t = 0.0
        self.t0 = time.monotonic()

    def until(self, pred, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            self.sample()
            if pred(self):
                return True
            time.sleep(POLL_S)
        return False

    def first_time(self, key_prefix: str, values: tuple[str, ...]) -> float | None:
        ts = [t for k, t in self.first.items()
              if k.startswith(key_prefix + "=") and k.split("=", 1)[1].rstrip("*").startswith(values)]
        return min(ts) if ts else None


# ---- workload --------------------------------------------------------------------------------


def start_pipeline(plan: emu.Plan, nodes: list[str], minutes: int = 10) -> str:
    """Submit the MPI pipeline as a batch job; return its id once every rank is running."""
    mpi.ensure_staged(plan, log=lambda *_: None)
    cores = mpi.worker_cores()
    step = mpi.srun_cmd(nodes, PIPELINE_ARGS, minutes=minutes, cpus_per_task=cores)
    script = f"#!/bin/bash\n{step}\n"
    r = slurm._as_user(plan, f"sbatch --parsable --no-requeue -N{len(nodes)} "
                             f"-w {','.join(nodes)} -c{cores} --mem={mpi.STEP_MEM} -t {minutes} "
                             f"-o /dev/null <<'EOF'\n{script}EOF")
    job = r.stdout.strip().split(";")[0]
    if r.returncode != 0 or not job:
        raise emu.EmuError(f"pipeline submit failed: {r.stderr.strip()}")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        procs = slurm._user_procs(plan)
        if all(any("dcbench" in p for p in procs.get(n, [])) for n in nodes):
            time.sleep(2)  # let the pipeline fill
            return job
        time.sleep(1)
    slurm._as_user(plan, f"scancel {job}")
    raise emu.EmuError(f"pipeline job {job}: ranks never started")


def _ranks_alive(node: str) -> int:
    r = emu.exec_(node, "pgrep", "-u", slurm.USER, "-x", "dcbench", check=False)
    return 1 if r.stdout.strip() else 0


def _leftovers(nodes: list[str]) -> dict[str, list[str]]:
    left = {}
    for n in nodes:
        r = emu.exec_(n, "pgrep", "-u", slurm.USER, "-a", check=False)
        if r.stdout.strip():
            left[n] = r.stdout.strip().splitlines()
    return left


def _isl_bytes(plan: emu.Plan, a: str, b: str) -> int:
    return storage._tbf_bytes(a, f"p-{b}")


def _job_flowing(plan: emu.Plan, a: str, b: str, seconds: float = 2) -> bool:
    """Is the pipeline still moving data across a→b?"""
    x = _isl_bytes(plan, a, b)
    time.sleep(seconds)
    return _isl_bytes(plan, a, b) - x > 1_000_000


def _wait_idle(plan: emu.Plan, nodes: list[str], timeout_s: float = 120) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        st = slurm.node_states(plan)
        if all(st.get(n) == "idle" for n in nodes):
            return True
        time.sleep(1)
    return False


# ---- scenarios -------------------------------------------------------------------------------


def worker_loss(plan: emu.Plan, victim: str, mode: str = "kill", freeze_s: float = 60,
                echo=print) -> Result:
    """Kill (power loss) or freeze (hang) one worker during the pipeline."""
    ws = plan.hosts[1:]
    res = Result(f"worker-{mode}", {"victim": victim, "workers": ws,
                                    **({"freeze_s": freeze_s} if mode == "freeze" else {})})
    job = start_pipeline(plan, ws)
    echo(f"pipeline job {job} running on {','.join(ws)}; {mode} {victim}")
    w = Watch(plan, res, [victim], job, echo, survivors=[n for n in ws if n != victim])
    w.baseline()
    emu._docker("kill" if mode == "kill" else "pause", emu.container(victim))
    w.mark(f"FAULT: docker {'kill' if mode == 'kill' else 'pause'} {victim}")
    job_key = f"job {job}"
    terminal = ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY")

    if mode == "freeze":
        w.until(lambda s: s.now() >= freeze_s, freeze_s + 5)
        emu._docker("unpause", emu.container(victim))
        w.mark(f"UNDO: docker unpause {victim}")
        w.until(lambda s: str(s.last.get(job_key, "")).startswith(terminal)
                and s.last.get(f"slurm {victim}", "").rstrip("*") in ("idle", "down", "drained"),
                180)
    else:
        w.until(lambda s: str(s.last.get(job_key, "")).startswith(terminal), 240)
    t_end = w.first_time(job_key, terminal)
    survivors = [n for n in ws if n != victim]
    left = _leftovers(survivors)
    states = slurm.node_states(plan)
    drained = {n: states.get(n) for n in survivors if "drain" in str(states.get(n))}
    res.checks["survivors_not_drained"] = (not drained, f"healthy nodes drained: {drained or 'none'}")
    w.mark(f"survivors' leftover processes: {left or 'none'}")
    if str(w.last.get(job_key, "")) == "RUNNING":
        slurm._as_user(plan, f"scancel {job}")
        w.mark("job still RUNNING; cancelled it")

    if mode == "kill":
        t_r = w.mark(f"UNDO: power {victim} back on")
        from . import monitoring
        emu.restore_host(plan, victim, log=lambda *_: None)
        if w.mon:
            monitoring.start_exporters(plan, [victim])
        w.mark(f"{victim} restarted and re-cabled")
        back = w.until(lambda s: s.last.get(f"slurm {victim}") == "idle"
                       and (not s.mon or s.last.get(f"telemetry {victim}") == "up"), 180)
        res.metrics["recovery_s"] = round(w.now() - t_r, 1) if back else None
        nfs = emu.exec_(victim, "timeout", "20", "cat", "/shared/.ready", check=False).returncode == 0
        res.checks["victim_recovered"] = (back and nfs, f"{victim} idle in Slurm, telemetry up, "
                                          f"NFS {'mounted' if nfs else 'NOT mounted'}, "
                                          f"{res.metrics['recovery_s']}s after power-on")
    else:
        back = w.until(lambda s: s.last.get(f"slurm {victim}") == "idle"
                       and (not s.mon or s.last.get(f"telemetry {victim}") == "up"), 120)
        res.checks["victim_recovered"] = (back, f"{victim} {'back to idle' if back else 'NOT idle'} "
                                          f"after unfreezing")

    tel = w.first_time(f"telemetry {victim}", ("DOWN",))
    nr = _first_not_responding(w, victim)
    down = w.first_time(f"slurm {victim}", ("down",))
    ranks_gone = w.first_time("ranks alive on survivors", ("0/",))
    res.metrics.update({
        "telemetry_down_s": tel, "slurm_not_responding_s": nr, "slurm_down_s": down,
        "survivor_ranks_gone_s": ranks_gone, "job_end_s": t_end,
        "job_state": str(w.last.get(job_key)),
    })
    res.checks["telemetry_within_30s"] = (tel is not None and tel <= 30,
                                          f"telemetry DOWN after {tel}s")
    res.checks["job_resolved"] = (t_end is not None,
                                  f"job ended {w.last.get(job_key)} after {t_end}s")
    res.checks["survivors_clean"] = (not left, f"leftover processes: {left or 'none'}")
    return res


def _first_not_responding(w: Watch, victim: str) -> float | None:
    """First time Slurm showed the node as not responding ("*" suffix) or down."""
    ts = [t for k, t in w.first.items() if k.startswith(f"slurm {victim}=")
          and (k.endswith("*") or k.split("=", 1)[1].startswith(("down", "drain", "fail")))]
    return min(ts) if ts else None


def link_down(plan: emu.Plan, a: str, b: str, seconds: float, echo=print) -> Result:
    """Pull the a–b cable for `seconds` during the pipeline (both ends lose carrier)."""
    ws = plan.hosts[1:]
    far = [h for h in ws if b in [x for hop in plan.topo.route(plan.hosts[0], h) for x in hop]]
    res = Result("link-down", {"link": f"{a}-{b}", "seconds": seconds, "cut_off": far})
    job = start_pipeline(plan, ws)
    echo(f"pipeline job {job} running; cutting {a}-{b} for {seconds:g}s "
         f"({','.join(far)} unreachable from C0)")
    victim = far[0]
    w = Watch(plan, res, [victim], job, echo, survivors=[n for n in ws if n not in far])
    w.baseline()
    emu.exec_(a, "ip", "link", "set", f"p-{b}", "down")
    w.mark(f"FAULT: {a}:p-{b} down (cable pulled)")
    w.until(lambda s: s.now() >= seconds, seconds + 5)
    emu.exec_(a, "ip", "link", "set", f"p-{b}", "up")
    t_up = w.mark(f"UNDO: {a}:p-{b} up")
    job_key = f"job {job}"
    # Give TCP retransmission backoff time to resume traffic.
    flowing = False
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        w.sample()
        if not str(w.last.get(job_key, "")).startswith("RUNNING"):
            break
        if _job_flowing(plan, a, b):
            flowing = True
            break
    if flowing:
        w.mark(f"pipeline traffic flowing across {a}->{b} again")
    t_flow = w.now() - t_up if flowing else None
    state = str(w.last.get(job_key))
    w.until(lambda s: s.last.get(f"slurm {victim}") == "idle"
            or str(s.last.get(f"slurm {victim}")).startswith(("alloc", "mix")), 120)
    if state.startswith("RUNNING"):
        slurm._as_user(plan, f"scancel {job}")
        w.mark("job survived the outage; cancelled it")
    else:
        w.until(lambda s: s.last.get(f"slurm {victim}") == "idle", 120)
    left = _leftovers(ws) if not state.startswith("RUNNING") else {}
    nr = _first_not_responding(w, victim)
    res.metrics.update({"traffic_resumed_after_s": round(t_flow, 1) if t_flow is not None else None,
                        "job_state_after": state, "slurm_not_responding_s": nr,
                        "slurm_down_s": w.first_time(f"slurm {victim}", ("down",)),
                        "telemetry_down_s": w.first_time(f"telemetry {victim}", ("DOWN",))})
    res.checks["job_resolved"] = (flowing or not state.startswith("RUNNING"),
                                  "job " + ("resumed" if flowing else f"ended {state}"))
    res.checks["survivors_clean"] = (not left, f"leftover processes: {left or 'none'}")
    ok_idle = _wait_idle(plan, ws, 120)
    res.checks["nodes_recovered"] = (ok_idle, "all workers idle in Slurm afterwards"
                                     if ok_idle else f"not idle: {slurm.node_states(plan)}")
    return res


NFS_ART = "models/failure-nfs"


def nfs_down(plan: emu.Plan, seconds: float = 60, echo=print) -> Result:
    """Stop the NFS server; staging and result writes must fail within a bound, leaving
    nothing partial, while workers with a staged model keep working (PRD §10)."""
    ws = plan.hosts[1:]
    loaded, fresh = ws[: len(ws) // 2], ws[len(ws) // 2:]
    res = Result("nfs-down", {"seconds": seconds, "loaded": loaded, "fresh": fresh})
    c0 = plan.hosts[0]
    storage.publish_synthetic(plan, "failure-nfs", 32, files=4, replace=True)
    storage.clear_local(plan, NFS_ART)
    pre = storage.stage(plan, NFS_ART, nodes=len(loaded), nodelist=",".join(loaded))
    if any(r["status"] != "ok" for r in pre):
        raise emu.EmuError(f"pre-staging failed: {pre}")
    echo(f"{NFS_ART} staged on {','.join(loaded)}; stopping NFS server for {seconds:g}s")
    w = Watch(plan, res, [], None, echo)
    emu.exec_(c0, "rpc.nfsd", "0")
    w.mark("FAULT: rpc.nfsd 0 on C0 (NFS server stopped)")

    # 1. Staging a model that is not local yet: must end with an error, within a bound.
    t = time.monotonic()
    out = storage.stage(plan, NFS_ART, nodes=len(fresh), nodelist=",".join(fresh), timeout=60)
    stage_s = time.monotonic() - t
    w.mark(f"staging on {','.join(fresh)}: " + ", ".join(
        f"{r['host']} {r['status']} ({r.get('error', '')[:40]})" for r in out))
    partial = {}
    for n in fresh:
        r = emu.exec_(n, "bash", "-c", f"ls -a {storage.STAGE_DIR} | grep failure-nfs", check=False)
        if r.stdout.strip():
            partial[n] = r.stdout.split()
    res.checks["staging_bounded_error"] = (
        all(r["status"] != "ok" for r in out) and stage_s <= 90 and not partial,
        f"{len(out)}/{len(fresh)} staging tasks failed after {stage_s:.1f}s; "
        f"partial files: {partial or 'none'}")
    res.metrics["staging_error_s"] = round(stage_s, 1)

    # 2. Result publication from a worker: bounded error, no partial file.
    name = f"results/failure-{int(time.time())}"
    t = time.monotonic()
    r = slurm._as_user(plan, f"srun -N1 -w {loaded[0]} -t 2 bash -c "
                             + shlex.quote(f"echo '{{}}' > /shared/{name}.tmp && "
                                           f"mv /shared/{name}.tmp /shared/{name}.json"),
                       timeout=200)
    pub_s = time.monotonic() - t
    w.mark(f"result publication from {loaded[0]}: rc={r.returncode} after {pub_s:.1f}s "
           f"({r.stderr.strip().splitlines()[-1][:80] if r.stderr.strip() else ''})")
    res.checks["publication_bounded_error"] = (r.returncode != 0 and pub_s <= 90,
                                               f"rc={r.returncode} after {pub_s:.1f}s")
    res.metrics["publication_error_s"] = round(pub_s, 1)

    # 3. Workers that already have the model keep working without storage.
    t = time.monotonic()
    rr = slurm._as_user(plan, f"srun -N{len(loaded)} -w {','.join(loaded)} -t 2 bash -c "
                              + shlex.quote(f"cat {storage.STAGE_DIR}/failure-nfs/shard-*.bin "
                                            "> /dev/null && echo ok"),
                        timeout=120)
    rows, rm = mpi.run(plan, loaded, "layer --sizes 65536 --microbatches 200", timeout=120)
    ok_loaded = rr.returncode == 0 and rr.stdout.count("ok") == len(loaded) and rm.returncode == 0
    w.mark(f"loaded workers {','.join(loaded)}: read local model {'OK' if rr.returncode == 0 else 'FAILED'}, "
           f"MPI pipeline {'OK' if rm.returncode == 0 else 'FAILED'} ({time.monotonic() - t:.1f}s)")
    res.checks["loaded_workers_continue"] = (ok_loaded, "local model readable and MPI jobs run "
                                             "with storage down" if ok_loaded else
                                             f"{rr.stderr.strip()[-200:]} {rm.stderr.strip()[-200:]}")

    # 4. Restore and time until staging works again.
    remaining = seconds - w.now()
    if remaining > 0:
        time.sleep(remaining)
    emu.exec_(c0, "rpc.nfsd", "-N", "3", "8")
    t_r = w.mark("UNDO: rpc.nfsd -N 3 8 (NFS server restarted)")
    back = None
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if emu.exec_(fresh[0], "timeout", "10", "cat", "/shared/.ready", check=False).returncode == 0:
            back = w.now() - t_r
            break
        time.sleep(1)
    if back is not None:
        w.mark(f"/shared readable again on {fresh[0]}")
    out2 = storage.stage(plan, NFS_ART, nodes=len(fresh), nodelist=",".join(fresh))
    ok2 = all(r["status"] == "ok" for r in out2)
    w.mark(f"staging retried on {','.join(fresh)}: {'all ok' if ok2 else out2}")
    res.metrics["nfs_recovery_s"] = round(back, 1) if back is not None else None
    res.checks["storage_recovered"] = (back is not None and ok2,
                                       f"readable {back and round(back, 1)}s after restart "
                                       f"(NFSv4 grace period), staging retry {'ok' if ok2 else 'FAILED'}")
    storage.clear_local(plan, NFS_ART)
    storage.unpublish(plan, NFS_ART)
    return res
