# M0 — inventory and choices

Status of the M0 milestone (PRD §11) and the decisions listed in PRD §14. A decision is
**closed** only when the evidence named here is committed to the repository.

## M0 exit evidence

| Evidence | Where | Status |
|---|---|---|
| Hardware inventory | `inventory/hosts/<host>.json`, checked with `daisychain inventory check` | Tooling ready; **no cluster nodes inventoried yet** |
| Model/runtime choice | `config/versions.yaml` → `model`, `inference_runtime` | Open: needs single-node measurements (overlaps with M1) |
| Topology | `config/topology/chain.yaml`, `star.yaml` | Defined; link speeds are the 1 GbE planning assumption |
| Version matrix | `config/versions.yaml` | Template with candidates; nothing pinned |
| Benchmark schema | `schemas/*.schema.json`, `examples/` | Draft v1 |

## Decisions (PRD §14)

### 1. Hardware: CPUs, RAM, SSD, NICs, switches, node count — **open**

Run on every node:

```sh
python3 collect_inventory.py > inventory/hosts/$(hostname -s).json
```

Then `daisychain inventory check inventory/hosts/*.json --topology config/topology/chain.yaml`.
The checker reports a mismatched pinned software version, an unsynchronized clock, a
missing topology host, or a node without a physical NIC that is up as an **error**. It
warns about heterogeneous CPU, RAM, kernel or NIC speed, enabled swap, a non-`performance`
governor, cgroup v1, and missing SSDs.

The collector cannot see switch details, so record them by hand here: model, port speeds,
switching capacity, whether port counters are readable (SNMP or a management UI), and
the speed of each inter-switch link.

| Switch | Model | Port speed | Switching capacity | Counters available | Uplink speed |
|---|---|---|---|---|---|
| S1 | | | | | |
| S2 | | | | | |
| S3 | | | | | |
| S4 | | | | | |
| S0 (star) | | | | | |

### 2. Software versions — **open**

Tracked in `config/versions.yaml`. Pin a version only after it is installed and the
inventory check shows the same version on every node.

### 3. Model, precision, prompts, output lengths, threads, latency limit — **open**

Chosen during M1 from measured memory and phase timing. The p95 latency limit is frozen
before any scaling run and recorded in each manifest's `measurement.p95_latency_limit_ms`.

### 4. Slurm/container launch approach and MPI transport — **container launch proposed; MPI open**

Must pass the PRD §6 two-node gate during M2: launch on the allocated hosts, enforce or
verify limits on the real container processes, propagate exit codes, and clean up on
cancellation. Record the chosen integration and the evidence here.

**Proposed launch approach:** build images with Docker, convert them to Apptainer images,
stage them to each worker's local disk, and launch with
`srun … apptainer exec <image> <cmd>` (Apptainer 1.5.5, non-setuid).

Why this over the alternatives on [Slurm's containers page](https://slurm.schedmd.com/containers.html):

- **Rootless with no daemon:** containers run as the submitting user through user
  namespaces. Nobody needs Docker daemon access to launch a job (PRD §6).
- **Slurm keeps control:** container processes stay in the Slurm step's cgroup, so Slurm's
  own process tracking, cgroup limits and kill-on-cancel apply unchanged. Rootless podman
  without systemd needs `--cgroups=disabled`-style workarounds to get the same result.
- **Fewer moving parts than Slurm's native OCI mode** (`srun --container` + `oci.conf` +
  crun/runc), which in 23.11 needs a hand-written runtime command set and OCI bundles.
- **Fits the storage plan:** an image is a single file or directory that can be staged
  like model weights.

Evidence so far (emulated cluster, 8 workers, `daisychain emu container-verify`):
tasks run inside the image as UID 2000 and inside their Slurm step cgroup on every
worker; `-c1 --mem=200M` limits apply inside the container on W1 and W8; a 300 MiB
allocation under `--mem=100M` is OOM-killed; exit codes propagate through `srun` and
`sbatch`; after `scancel`, tasks that ignore SIGTERM are gone within 6.7 s, leaving no job
cgroups; and no root-owned runtime process exists while jobs run. **Not yet
satisfied:** the gate needs two *physical* nodes. MPI launch inside the container is still
open (the Slurm PMIx plugin currently fails to load).

### 5. Separate monitoring/storage host — **open; initial analysis below**

Running `daisychain topology load <file> --pattern fanout --payload-mb <model MB>` under
the planning assumption (every link 1 Gb/s) gives:

- In **both** chain and star, the busiest link for controller→worker traffic (NFS model
  loads, request dispatch) is **C0's own NIC**, shared by all 8 flows (125 Mb/s each
  with equal sharing). The chain's S1→S2 link carries 6 of those flows, so while C0 has a
  single 1 GbE link the chain adds no fan-out bottleneck beyond C0's NIC.
- So cold-start storage experiments will mainly measure C0's NIC and NFS server, not the
  chain. Staging weights to local SSD (PRD default) avoids this during warm serving.
- Worker↔worker traffic (`--pattern all-to-all`) is where the chain differs: S2↔S3
  carries 16 of 56 flows in each direction, while in the star the busiest link is a
  worker NIC (7 flows). Collective and MPI benchmarks are the experiments that will
  show topology effects.
- A sequential pipeline (`--pattern pipeline`) puts at most one flow on each link in
  both topologies.

Implications to confirm on hardware: a faster C0 uplink (or bonded NICs) changes the
fan-out picture, but it is a hardware upgrade and must not be mixed into the chain-vs-star
comparison (PRD §4). Measure controller saturation before blaming workers or switches
(PRD §10).

These numbers are static flow counts, not simulation results or measurements.

## Revision log

| Date | Change | Reason |
|---|---|---|
| 2026-11-19 | Proposed container launch: Docker-built images run with non-setuid Apptainer 1.5.5 under `srun`; all container gate checks pass in emulation | Decision 4 |
| 2026-11-19 | Slurm 23.11.4 on the emulated cluster: 1/2/4/8-node allocation, cgroup v2 CPU/RAM limits, exit codes, cancellation and timeout cleanup all pass; Slurm time-limit enforcement takes ~1–2 min, so the 30 s failure bound must come from the router | Decision 4 groundwork |
| 2026-11-19 | Emulated cluster added (docs/emulated-cluster.md); chain vs star cross-traffic in emulation: 932 vs 3679 Mb/s, matching the static prediction | No hardware yet |
| 2026-11-19 | Initial M0 scaffolding: topology files, inventory tooling, version matrix template, benchmark schemas v1 | Start of M0 |
