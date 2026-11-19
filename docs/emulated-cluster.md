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
uv run daisychain emu up                     # chain, 8 workers, 1 Gb/s
uv run daisychain emu up --topology config/topology/star.yaml
uv run daisychain emu up --workers 2         # first N workers; all switches kept
uv run daisychain emu status
uv run daisychain emu verify --output results/emu/verify.json
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

1. Slurm: `slurmctld` on C0, `slurmd` on workers, munge, cgroup v2 limits; batch jobs on
   1/2/4/8 nodes; cancellation and cleanup audit (F2, F3).
2. NFS: export from C0 (kernel nfsd may not be available in the Docker Desktop VM, in which
   case use NFS-Ganesha in userspace), plus checksum-verified staging to worker local disk (F7).
3. MPI: Open MPI with the TCP BTL, ping-pong/bandwidth/collective benchmarks (F6).
4. Monitoring: node_exporter, Prometheus and Grafana on C0, plus a management interface for
   browser access (F8).
