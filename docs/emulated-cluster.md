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

Not covered yet: `sacct` accounting (needs `slurmdbd` plus a database; job records
currently go to `jobcomp.log`). MPI launch is covered in [MPI](#mpi) below.

## Containerised job steps

Covers the rest of the PRD §6 Slurm/container gate. The approach and why it was chosen are
in [m0-decisions.md](m0-decisions.md#4-slurmcontainer-launch-approach-and-mpi-transport--container-launch-proposed-mpi-open).

```sh
uv run daisychain emu container-verify --output results/emu/container-verify.json
uv run daisychain emu stage-image --image <docker image> --name <name>   # any image
docker exec -u dcuser dc-C0 srun -N2 apptainer exec /var/lib/daisychain/images/probe python3 --version
```

- **Images:** built with Docker. `stage-image` converts the image to an Apptainer sandbox
  once on C0 and publishes it to NFS as `images/<name>/image.tar` with a checksum manifest.
  Every worker then stages it over the emulated fabric with `daisychain-stage --untar`
  (see [Shared storage](#shared-storage-nfs)). The artifact ID is recorded in each staged
  copy and checked for consistency.
- **Probe image:** `emu/probe` (Python 3.12 on Debian 12). The nodes run Ubuntu 24.04, so
  checks can tell they really are inside the image.
- **Runtime:** non-setuid Apptainer 1.5.5. A running containerised step on a worker looks
  like this (from `ps`):

  ```
  root     slurmstepd: [10.0]
  dcuser     └ Apptainer runtime parent: probe
  dcuser         └ /usr/bin/sleep 120
  ```

| Check | Result (8 workers) |
|---|---|
| `container_staged` | same artifact ID on all 8 workers, delivered over NFS |
| `container_identity` | on every worker the task runs as UID 2000, sees Debian 12, and sits in its Slurm step's cgroup |
| `container_limits_on_process` | `memory.max` = 200 MiB and 1 CPU within the worker's share, inside the container, on W1 and W8 |
| `container_memory_enforced` | 300 MiB allocation under `--mem=100M` is OOM-killed |
| `container_exit_codes` | exit codes propagate through `srun` and `sbatch --wait` |
| `container_cancel_cleanup` | everything gone 6.7 s after `scancel`; no job cgroups left; no root-owned runtime processes while running |

**Emulator-only workaround:** running `.sif` files fails inside the Docker Desktop VM
(`exec /usr/bin/true failed: invalid argument` once squashfuse mounts the image; running
the same image unpacked works). So images are staged as sandbox directories. Re-test SIF
on real hardware before choosing the image format there.

## Shared storage (NFS)

C0 runs the kernel NFSv4 server (NFSv4.2 only, no rpcbind) and exports `/export`, a Docker
volume: the container's overlay root filesystem can't be exported. Workers mount it at
`/shared` over the emulated fabric. Kernel nfsd runs per network namespace, so it serves
only C0's emulated interface.

```
/shared/models/<name>/    published artifacts: MANIFEST.sha256 + files (root-owned, read-only)
/shared/images/<name>/    image.tar (Apptainer sandbox) + manifest; SOURCE.json = Docker provenance
/shared/datasets/
/shared/results/          writable by dcuser; jobs write <tmp> then rename
```

```sh
uv run daisychain emu nfs-verify --output results/emu/nfs-verify.json
uv run daisychain emu publish-synthetic synthetic-256m --size-mb 256
uv run daisychain emu staging-bench --artifact models/synthetic-256m --counts 1,2,4,8 \
    --repeats 3 --cache cold,warm-server --output results/emu/staging-bench.json
docker exec -u dcuser dc-C0 srun -N4 daisychain-stage models/synthetic-256m
```

**Publishing** (`storage.publish_*`, on C0 as root): files are written to a temporary
directory, `MANIFEST.sha256` is computed, and the directory is renamed into place.
Republishing an existing name fails unless replacement is explicitly requested. Jobs can't
write under `models/` or `images/`, and root on a worker can't either (`root_squash`), so
shared model files are never written concurrently (F7).

**Staging** (`daisychain-stage`, `src/daisychain/stage.py`, standard library only, run as
one Slurm task per node): it reads the manifest (the artifact ID is its SHA-256), skips
the copy if the local copy carries the same ID, and takes a per-node lock. It then
streams every file to a private temporary directory, hashing while copying, and either
renames the result into place or fails without leaving anything behind. It prints one JSON
line with bytes, seconds and Mb/s.

**Mount options:** `vers=4.2,soft,timeo=50,retrans=2,noatime`. `soft` turns a lost server
into an error within about 15 s instead of an unkillable hang (PRD §10: "NFS failure must
produce a bounded error"). That is safe because staging verifies checksums and results
are renamed into place.

### Acceptance checks (`emu nfs-verify`)

| Check | Result (8 workers, chain) |
|---|---|
| `nfs_mounted` | every worker mounts `C0:/` (nfs4, soft) from 10.10.0.10 over the fabric |
| `nfs_permissions` | artifacts are read-only to jobs and to worker root; `results/` is writable by dcuser |
| `stage_verified` | 64 MiB artifact staged on all 8 workers with checksums verified and the same artifact ID; every worker's switch port carried ≥ the artifact size |
| `stage_cache_hit` | a second staging reads no data on any worker |
| `stage_same_node_lock` | two tasks on one node: one copies, the other waits for the lock and reuses the copy |
| `stage_rejects_corruption` | one flipped byte on the server → `checksum_mismatch`, nothing left locally |
| `results_publication` | every worker writes `results/<run>/<host>.json` via temp file + rename |

### Storage-startup experiment (PRD §8)

256 MiB synthetic artifact (4 shards), staged simultaneously to the first N workers, 3
repeats each, VM page cache dropped before every repeat. "Min possible" is the max-min
fair-share bound from the topology: every flow shares C0's 1 Gb/s link.

| Topology | Cache | N | Makespan (s) | Min possible (s) | Efficiency | Aggregate Mb/s |
|---|---|---|---|---|---|---|
| chain | cold | 1 | 2.49 ± 0.09 | 2.15 | 86% | 863 |
| chain | cold | 2 | 4.66 ± 0.07 | 4.29 | 92% | 922 |
| chain | cold | 4 | 9.16 ± 0.14 | 8.59 | 94% | 938 |
| chain | cold | 8 | 18.40 ± 0.18 | 17.18 | 93% | 934 |
| chain | warm server | 8 | 18.44 ± 0.25 | 17.18 | 93% | 932 |
| star | cold | 1 | 2.80 ± 0.30 | 2.15 | 77% | 773 |
| star | cold | 2 | 4.83 ± 0.23 | 4.29 | 89% | 892 |
| star | cold | 4 | 9.40 ± 0.49 | 8.59 | 91% | 916 |
| star | cold | 8 | 18.29 ± 0.24 | 17.18 | 94% | 939 |

Bytes on the chain's inter-switch links for N=8 were 1685/1123/562 MB measured against
1611/1074/537 MB expected for S1→S2/S2→S3/S3→S4: the expected 6, 4 and 2 copies, plus about
4.6% NFS/TCP/IP overhead.

What it shows (emulation, evidence tier "designed"):

- **Cold start grows linearly with N, and C0's link is the bottleneck.** Every simultaneous
  start adds about 2.3 s per 256 MiB. Eight workers loading a real model of size M from NFS
  would take roughly 8·M / 117 MB/s.
- **The chain doesn't change cold-start time.** The star is within noise at every N, as
  the static analysis predicted. Topology starts to matter only for worker↔worker traffic.
- **Server disk cache doesn't matter here:** cold ≈ warm server, because the network
  bottleneck hides the VM disk. On hardware the server's SSD may matter; measure it there.
- **Mitigations to test** (PRD §12, "stage locally, stagger startup"): the staged local
  copy already makes the warm path free (`stage_cache_hit`). A faster C0 uplink would raise
  the ceiling but is a hardware change, so it must stay out of the chain-vs-star
  comparison.

### Pitfalls found (and how the code handles them)

1. **NFSv4 grace period.** After any server start or restart, NFSv4 blocks opens for its
   grace period, 90 s by default. The first star benchmark repeat took 91 s because of it.
   The emulator sets lease and grace times to 15 s (`NFS_LEASE_S`, `NFS_GRACE_S` in
   `node-init.sh`), and `emu up` waits until a worker can read `/shared`. **For the PRD's NFS
   failure experiments this is the recovery time to expect after a server restart**;
   choose and record the value for hardware.
2. **Client page cache hides the network.** Deleting the local staged copy is not enough
   for a cold run: the worker's NFS *client* cache still holds the file, and the first
   "warm server" runs moved almost nothing over the fabric (0.47 s instead of 2.5 s).
   The benchmark now drops caches before every repeat and marks a run invalid unless every
   worker's switch port carried at least the artifact size.
3. **Hard mounts can wedge the host.** With a default `hard` mount, removing the server
   container before a client left the client's exiting task stuck in
   `nfs4_proc_delegreturn` forever. Its namespaces were already gone, so neither `umount -f`
   nor the 6.6+ `/sys/fs/nfs/*/shutdown` switch could reach it, and only restarting Docker
   Desktop cleared it. Fixes: `soft` mounts, and `emu down` unmounts the clients and
   removes workers before C0.

## MPI

Covers PRD F6 and the MPI part of the §6 Slurm/container gate.

```sh
uv run daisychain emu mpi-verify                 # builds and stages the MPI image if needed
uv run daisychain emu mpi-bench --output results/emu/mpi-bench-chain.json
```

**Launch:** `srun --mpi=pmix … apptainer exec /var/lib/daisychain/images/mpi dcbench …`

- **One build inside and outside the container.** `emu/mpi/dcbench.c` is compiled with
  the same flags against Ubuntu 24.04's Open MPI 4.1.6 in both the node image (for native
  diagnostic runs) and the MPI job image `emu/mpi/Dockerfile`. The job image reaches
  workers through NFS staging like any other image.
- **PMIx:** Slurm 23.11's `mpi/pmix_v5` plugin and Open MPI share the distribution's
  PMIx 5.0.1. The plugin `dlopen`s the unversioned `libpmix.so`, which only `libpmix-dev`
  ships; without it the plugin fails with `can not load PMIx library`. Inside the
  container, the PMIx client reaches the step's PMIx server over loopback TCP
  (`PMIX_SERVER_URI…=tcp4://127.0.0.1:…`). Apptainer shares the host network namespace,
  so no bind mounts are needed, but running containers with network isolation would break
  the launch.
- **Transport:** Open MPI is pinned to `pml=ob1`, `btl=self,vader,tcp`, and
  `btl_tcp_if_include=10.10.0.0/24` (`mpi.MCA`), so inter-node traffic can only use the
  cabled fabric. Two ranks on one worker run in separate Apptainer user namespaces, which
  rules out cross-memory attach, so `btl_vader_single_copy_mechanism=none`.
- **Rank failure:** `KillOnBadExit=1` in `slurm.conf`. Without it, a rank whose peer has
  died waits forever: measured, the survivor was still waiting when the test gave up at
  120 s. With it, the step ends in 0.3–0.4 s.

### Acceptance checks (`emu mpi-verify`)

| Check | What it proves |
|---|---|
| `mpi_versions_match` | `libopenmpi3t64` and `libpmix2t64` versions are identical on the host and in the image, and `dcbench` launches both natively and in the container |
| `mpi_placement` | one rank per allocated worker, in rank order, inside the image, as UID 2000, in the Slurm step cgroup |
| `mpi_intranode` | two containerised ranks on one worker communicate over shared memory |
| `mpi_transport_tcp` | a W1→W8 stream appears on every switch egress port along the route (S1→S2→S3→S4→W8) |
| `mpi_rank_failure` | rank 1 exits 3 while rank 0 blocks waiting for it: the step ends within 30 s, the failing code shows up in srun's stderr, and nothing is left running |
| `mpi_cancel_cleanup` | `scancel` of a batch job with containerised ranks leaves no processes, no job cgroups, and no root-owned runtime processes |

Result on the dev machine (8 workers, chain): **all 6 checks pass.** W1→W8 streamed
403 MB at 957 Mb/s, and each of the four switch egress ports on the route carried 421 MB
(payload plus TCP/IP headers). A rank failure ended the step in 0.4 s, and cancellation
cleaned up in 1.5 s.

**Exit codes:** after a rank failure, `srun` (and the batch job) return **143**, not the
rank's code. Slurm reports the highest task exit code, and the surviving rank, killed by
SIGTERM, counts as 128+15. The real cause is only in srun's stderr
(`task 1: Exited with exit code 3`). The harness must record per-task outcomes rather than
trust the step's exit code.

### Microbenchmarks (`emu mpi-bench`, PRD F6)

<!-- MPI-BENCH-RESULTS -->

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
- **Per-message timing is not representative.** The shaper's token bucket must hold
  about 4 ms of traffic (500 KB at 1 Gb/s): with smaller buckets the VM's timer
  resolution caps a 1 Gb/s link at 470–590 Mb/s. After an idle gap, a message that fits
  in the bucket crosses at memory speed. So MPI small-message latency is a few µs instead
  of tens of µs, and ping-pong bandwidth below a few MB is inflated (1 MiB measured
  1.7 Gb/s on a 1 Gb/s path). `mpi-bench` flags these rows `burst_affected`. Streaming
  bandwidth, contention, and collectives moving many MB per link are unaffected.
- **No management network yet.** Nodes can only reach each other, so Grafana and similar
  web UIs aren't reachable from the Mac until C0 gets a separate port-published interface
  (planned with the monitoring increment).

## Next increments

1. Monitoring: node_exporter, Prometheus and Grafana on C0, plus a management interface for
   browser access (F8).
2. Failure experiments (PRD §8, §10): kill a worker, interrupt a link (take a veth down),
   remove NFS access; measure detection time and request outcomes.
