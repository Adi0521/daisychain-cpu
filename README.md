# daisychain-cpu

Distributed CPU inference cluster: a study of how a chain of Ethernet switches affects
inference throughput, latency, startup and fault behavior as the cluster grows from 1 to
8 workers. See [the PRD](Distributed_CPU_Inference_Cluster_PRD.md).

**Current stage: M0 (inventory and choices), with no hardware yet.** Status and open
decisions are tracked in [docs/m0-decisions.md](docs/m0-decisions.md). Until hardware
arrives, an [emulated cluster](docs/emulated-cluster.md) of Docker containers wired as the
chain or star topology stands in for it. Nothing here is a hardware measurement yet.

## Layout

| Path | Contents |
|---|---|
| `config/topology/` | Reference chain (S1→S2→S3→S4) and comparison star topologies |
| `config/versions.yaml` | Version matrix: candidates now, exact pins once verified on nodes |
| `schemas/` | JSON schemas: host inventory, experiment manifest, per-request records, run summary |
| `examples/` | Illustrative (not real) manifest, request records and summary |
| `inventory/hosts/` | One inventory JSON per cluster node (`*.dev.json` is git-ignored) |
| `emu/` | Node image for the emulated cluster |
| `src/daisychain/` | CLI and library |

## Setup

```sh
uv sync
uv run pytest
```

## Usage

```sh
# On each cluster node: stdlib-only, no install needed
python3 src/daisychain/collect_inventory.py > inventory/hosts/$(hostname -s).json

# Check consistency across nodes and coverage of the topology
uv run daisychain inventory check inventory/hosts/*.json --topology config/topology/chain.yaml

# Topology: validate, show routes, count flows per directed link for a traffic pattern
uv run daisychain topology validate config/topology/*.yaml
uv run daisychain topology routes config/topology/chain.yaml
uv run daisychain topology load config/topology/chain.yaml --pattern all-to-all
uv run daisychain topology load config/topology/star.yaml --pattern fanout --payload-mb 1500

# Emulated cluster (Docker): see docs/emulated-cluster.md
uv run daisychain emu up            # Grafana on http://127.0.0.1:3000 once it's up
uv run daisychain emu verify        # also: slurm-, nfs-, container-, mpi-, monitor-verify
uv run daisychain emu down

# Benchmark records
uv run daisychain run-id worker-scaling
uv run daisychain validate experiment_manifest path/to/manifest.json
uv run daisychain validate request_record path/to/requests.jsonl
```

`topology load` counts flows per link with equal sharing and gives a bandwidth lower
bound. It is a sanity check for M0 decisions, **not** a simulation. The SimGrid model
arrives in M4.

## Benchmark record model

Every run has one immutable **manifest** (`experiment_manifest.schema.json`), written
before the run, that pins code, topology, node inventories, software, model checksums,
execution mode, workload and measurement window. The run then produces
`requests.jsonl` (one record per request attempt, `request_record.schema.json`) and a
**summary** (`run_summary.schema.json`). All of them share the `run_id`. Failures,
timeouts and overload rejections stay in the success-rate denominator. A summary is
marked `low_sample` when the window is shorter than 120 s or fewer than 1,000 requests
complete.
