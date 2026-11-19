# Emulated cluster

A Docker-based stand-in for the cluster while no hardware exists (PRD §7: "Multiple
containers on one machine can validate orchestration and protocol behavior, but do not
validate physical multi-node scaling").

## How it works

- Every host (C0, W1–W8) and every switch (S1–S4, or S0 for the star) is a container
  started with `--network none`.
- A privileged helper running in the Docker VM's namespaces creates **one veth pair per
  cable** in the topology file, connecting containers directly. Docker bridge networks are
  not used, so traffic cannot route around the emulated switches.
- Each switch container runs a Linux bridge (`br0`, STP off; the topology validator already
  rejects loops), so it forwards like an Ethernet switch.
- Hosts get addresses on `10.10.0.0/24` (C0 = .10, Wn = .10+n) and `/etc/hosts` entries for
  every host name.
- **Every endpoint's egress** gets a `tbf` rate limit at the link speed, so each direction
  of each full-duplex link has its own capacity, as in the PRD.

```sh
uv run daisychain emu up                     # chain, 8 workers, 1 Gb/s, Slurm running
uv run daisychain emu up --topology config/topology/star.yaml
uv run daisychain emu up --workers 2         # first N workers; all switches kept
uv run daisychain emu up --worker-cores 2 --worker-memory-mb 1024   # defaults
uv run daisychain emu up --no-slurm          # network fabric only
uv run daisychain emu up --build             # rebuild the node image after editing emu/
uv run daisychain emu status
uv run daisychain emu verify --output results/emu/verify.json
uv run daisychain emu slurm-verify --output results/emu/slurm-verify.json
docker exec -it -u dcuser dc-C0 bash         # submit jobs as the experiment user
docker exec -it dc-C0 bash                   # nodes are dc-<id>
uv run daisychain emu down
```

## Verification (`emu verify`)

1. **Reachability:** C0 can ping every worker over the fabric.
2. **Round-trip time:** to the farthest worker. When a delay is configured, the check
   fails if the measured RTT doesn't match it.
3. **Path:** a 20 MB transfer to the farthest worker must increase the byte counters on
   every inter-switch link along its route.
4. **Link sharing:** concurrent iperf3 flows are compared with max-min fair shares computed
   from the topology:
   - `single`: C0 → farthest worker
   - `fanout`: C0 → every worker (C0's NIC is the shared bottleneck)
   - `cross`: W1→W5, W2→W6, … (every flow crosses the middle of the cluster)

   The test passes when the measured total is within 15% below the prediction (`--tolerance`).

### Observed on the dev machine (2026-11-19, Docker Desktop 27.5.1, 8 vCPU)

| Test | Chain predicted | Chain measured | Star predicted | Star measured |
|---|---|---|---|---|
| single | 1000 Mb/s | 936 | 1000 | 937 |
| fanout (8 flows) | 1000 | 931 | 1000 | 924 |
| cross (4 flows) | 1000 | 932 | 4000 | 3679 |

Measured TCP goodput is 92–94% of the shaped rate, which matches header overhead. The
emulator reproduces the topology effect that the static analysis predicts.

These are emulation results (evidence tier **designed**), not hardware measurements.

## Slurm

`emu up` starts Slurm after wiring the network (Slurm 23.11.4 from Ubuntu 24.04):

- `slurmctld` runs on C0 and `slurmd` on every worker, all talking over the emulated
  fabric. munge authentication uses a fresh random key for each cluster.
- `slurm.conf` is generated from the plan (`src/daisychain/slurm.py`): partition `emu`,
  `select/cons_tres` with `CR_Core_Memory`, `proctrack/cgroup`, `task/cgroup,task/affinity`,
  cgroup v2 with `ConstrainCores`, `ConstrainRAMSpace` and swap disabled for jobs,
  `KillWait=5`, and `jobcomp/filetxt` records in `/var/log/slurm/jobcomp.log`.
- Each worker is a 2-core, 1 GiB node by default. Jobs run as `dcuser` (UID 2000 on every
  node).

### How worker resources are modelled

Worker containers are **not** cpuset-pinned. Inside a pinned container, hwloc still sees
all 8 VM CPUs, so Slurm's CPU numbering didn't match the real CPU IDs. On every worker not
pinned to CPUs 0–1, binding then either failed (`task_g_set_affinity: Invalid argument`)
or silently left the job unconstrained. So instead:

- each worker is declared with the hardware topology `slurmd -C` actually detects;
- `CpuSpecList` reserves every CPU outside that worker's share, so jobs on worker *i* can
  only use their assigned CPUs (W1: 0–1, W2: 2–3, … wrapping round after the 8 VM CPUs);
- `docker --cpus` caps each container's total CPU time to its share, and `--memory`
  (with swap equal to memory) caps its RAM. Slurm's `RealMemory` leaves 128 MiB for the
  daemons.

Workers are `--privileged` with a private cgroup namespace, because slurmd needs a
writable cgroup v2 tree. They still have no network except the emulated fabric. This is
acceptable for a local emulator only; the PRD's rootless/least-privilege requirement
applies to the real cluster.

Two workarounds that only containers need (on real hosts systemd handles both):

- `node-init.sh` moves the container's processes into a leaf cgroup and creates
  `system.slice`. cgroup v2 forbids enabling controllers for children of a cgroup that has
  processes, and the namespace root of a container is not exempt the way the real root is.
- For the same reason `slurmstepd` cannot move itself back to the root at exit, which
  leaves empty `job_*` cgroup directories behind. `emu/slurm-epilog.sh` deletes empty
  ones after each job. `rmdir` fails on a cgroup that still has processes, so running jobs
  are safe.

### Acceptance checks (`emu slurm-verify`)

| Check | What it proves | PRD |
|---|---|---|
| `nodes_idle` | every worker registered and idle | F2 |
| `alloc_N_nodes` | 1/2/4/8-node jobs run on exactly N distinct allocated workers | F2 |
| `limits_on_process` | on W1 **and** the last worker, a `-c1 --mem=200M` task sees `memory.max=200 MiB`, 1 CPU, inside that worker's share | F3, §9 resource control |
| `memory_enforced` | a 300 MiB allocation under `--mem=100M` is OOM-killed | F3 |
| `exit_codes` | `srun` and `sbatch --wait` return the task's exit code; `scontrol` records `ExitCode=4:0` | §6 gate |
| `cancel_cleanup` | after `scancel`, tasks that ignore SIGTERM, their children and a `setsid` escapee are all gone, the job is `CANCELLED`, and no job cgroup remains | F3, §6 gate |
| `timeout_enforced` | a job over its time limit ends `TIMEOUT` with nothing left running (~1–2 min; `--skip-slow` skips it) | F3 |

Result on the dev machine (8 workers, chain): **all 10 checks pass**. Cancellation cleanup
took 6.6 s with `KillWait=5`. A 15 s time limit ended the job after 106 s, because Slurm
stores limits in whole minutes and checks them about every 30 s.

**Implication for F10/§9:** don't rely on Slurm time limits to detect failures within
30 s. The router's own deadlines and health checks must enforce that bound.

Not covered yet: containerised job steps (the rest of the §6 gate), MPI launch through
Slurm (PMIx fails to load: `mpi/pmix_v5: can not load PMIx library`, harmless while
`MpiDefault=none`), and `sacct` accounting (needs `slurmdbd` plus a database; job records
currently go to `jobcomp.log`).

## Known limitations

- **CPU is shared.** All containers share the Docker VM's CPUs, so this setup cannot
  measure inference scaling. Use it for function, protocol, orchestration and network-
  sharing behaviour only.
- **Per-flow fairness is not max-min.** In the chain `cross` test the total matched, but
  individual flows got 125–324 Mb/s. TCP flows sharing one drop-tail `tbf` queue don't
  split it evenly. Real switch buffers will behave differently, so don't calibrate per-flow
  latency from the emulator.
- **Latency emulation is not accurate on Docker Desktop.** `netem` adds about 1 ms per
  application point on top of the configured delay (100 µs per link gave a 4.3 ms RTT
  instead of 0.4 ms; 2 ms gave 12.6 ms instead of 8 ms), probably because of coarse timer
  resolution in the LinuxKit VM. `--latency-us` is therefore off by default, and `verify`
  fails when the measured RTT doesn't match. Without netem the fabric's baseline RTT is
  about 0.12 ms.
- **No management network yet.** Nodes can only reach each other, so Grafana and similar
  web UIs aren't reachable from the Mac until C0 gets a separate port-published interface
  (planned with the monitoring increment).

## Next increments

1. Containerised job steps inside Slurm allocations (rest of the PRD §6 gate):
   choose the runtime (e.g. rootless podman or apptainer) and verify limits and cleanup.
2. NFS: export from C0 (kernel nfsd may not be available in the Docker Desktop VM, in which
   case use NFS-Ganesha in userspace), plus checksum-verified staging to worker local disk (F7).
3. MPI: Open MPI with the TCP BTL, ping-pong/bandwidth/collective benchmarks (F6).
4. Monitoring: node_exporter, Prometheus and Grafana on C0, plus a management interface for
   browser access (F8).
