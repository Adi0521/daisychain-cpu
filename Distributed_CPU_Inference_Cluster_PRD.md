# Distributed CPU Inference Cluster — Product Requirements Document

**Project:** CPU Daisy Chaining  
**Date:** November 19, 2026  
**Status:** Proposed; this document does not establish that the system has been built  
**Target:** Eight CPU worker nodes, validated first through simulation and then through hardware experiments

## 1. Purpose

Build a reproducible local CPU inference cluster using Ubuntu, Docker, TCP sockets, MPI, Slurm, NFS, and Grafana. Evaluate how a chain of Ethernet switches affects inference throughput, latency, model startup, and fault behavior as the system grows from one to eight workers.

The project must answer three questions:

1. How much aggregate inference throughput can independent workers provide under a fixed workload and latency requirement?
2. When do shared network links, NFS, CPU capacity, or memory become the limiting resource?
3. Can a calibrated SimGrid model predict the direction and approximate magnitude of these effects well enough to guide the hardware design?

Faster inference from splitting a single model across machines is a hypothesis to test. Success can include demonstrating that replication performs better for the selected hardware and workload.

## 2. Users and workflows

| User | Need | Expected workflow |
|---|---|---|
| Developer | Run repeatable distributed inference experiments | Select configuration, submit a Slurm job, collect outputs and metrics |
| Cluster operator | Identify resource contention and failures | Inspect node, link, storage, container, and job dashboards |
| Researcher/reviewer | Reproduce and assess performance claims | Read experiment manifests, rerun benchmarks, compare measured and simulated results |

The primary interface is a CLI plus configuration files. Grafana provides live inspection; a benchmark report provides experiment comparisons. A custom web application is outside the MVP.

## 3. Scope

### Required MVP

- SimGrid model of one, two, four, and eight workers with explicit routes and shared bottlenecks.
- At least a two-worker physical prototype before claiming hardware validation; eight workers remain the final deployment target.
- Containerized CPU inference with a pinned model, runtime, and input set.
- Slurm batch allocation, launch, resource limits, logs, timeout, and cleanup.
- Shared NFS storage for canonical artifacts and results, with model weights staged to local disk for the default warm-serving path.
- TCP request transport and MPI communication benchmarks, with a controlled comparison between their application communication paths.
- Prometheus-backed Grafana dashboards and reproducible benchmark exports.
- Failure experiments covering worker loss, network disruption, and unavailable shared storage.

### Follow-on experiment

Implement layer pipeline inference for one supported small PyTorch model after the replicated baseline is stable. Compare two, four, and eight stages where hardware permits; simulate larger cases when only a smaller prototype is available. Report those cases separately.

### Outside scope

- GPU inference, distributed training, tensor parallelism, automatic graph partitioning, and arbitrary model support.
- Production multi-tenancy, billing, internet-facing access, and high-availability control services.
- Implementing an Ethernet stack, MPI library, scheduler, or filesystem.
- Claiming cycle-accurate CPU behavior, packet-level networking, or full Slurm/NFS emulation from a SimGrid experiment.

## 4. Architecture and topology

### Reference topology

Use eight worker hosts connected through four switches in a physical chain. Each switch connects two workers. A separate controller/storage host attaches to the first switch. Each worker has one data-network connection; compute hosts do not forward traffic for other workers.

| Component | Reference placement | Responsibility |
|---|---|---|
| Controller C0 | Switch S1 | CLI, Slurm controller, request router, NFS server, Prometheus, Grafana |
| Workers W1–W2 | Switch S1 | CPU inference containers and Slurm worker agents |
| Workers W3–W4 | Switch S2 | CPU inference containers and Slurm worker agents |
| Workers W5–W6 | Switch S3 | CPU inference containers and Slurm worker agents |
| Workers W7–W8 | Switch S4 | CPU inference containers and Slurm worker agents |
| Shared links | S1–S2, S2–S3, S3–S4 | Carry inference, storage, MPI, and monitoring traffic |

The physical chain is S1 → S2 → S3 → S4. For example, C0-to-W8 traffic traverses all three inter-switch links. Represent opposite directions and full-duplex capacity correctly in simulation. Do not add a physical loop.

Compare this design with a single-switch star using the same workers, endpoint link speeds, workload, and controller. Record switch port speeds and switching capacity; the comparison must isolate topology from hardware upgrades.

**Planning assumptions:** Ubuntu LTS on a trusted private LAN, x86-64 CPU workers, local SSD storage, and 1 GbE links as the initial simulation baseline. These are proposed inputs, not observed hardware specifications. Sweep higher bandwidths in simulation; inventory actual hardware before purchasing or deploying. No budget is approved by this PRD.

### Service responsibilities

- **Slurm:** allocates nodes and CPU resources for bounded experiment jobs. A serving experiment runs inside a time-limited allocation; Slurm is not the per-request router.
- **Router:** assigns request IDs, enforces deadlines and a bounded queue, routes to healthy allocated workers, and records latency and errors.
- **Docker:** packages the inference runtime and worker process. Images are preloaded before timed runs.
- **TCP sockets:** carry framed inference requests/responses and the custom point-to-point transport experiment.
- **MPI:** supplies rank management and point-to-point/collective communication experiments. Record the actual transport selected by MPI and verify that the intended inter-node path uses TCP.
- **NFS:** stores immutable model sources, datasets, configuration, and experiment results. Each job stages weights to local SSD and writes results into a unique run directory.
- **Prometheus and Grafana:** collect and display host, application, storage, and available network metrics.

## 5. Inference execution modes

### Mode A — replicated workers: MVP

Each allocated worker loads the complete model and handles independent requests. Begin with round-robin routing; add least-outstanding-request routing as one measured optimization. No MPI coordination is necessary for the main replicated request path. MPI still has an explicit role in the communication benchmark suite.

Use one small causal language model that fits comfortably on one worker, with fixed tokenizer, model revision, weight precision/quantization, and CPU runtime. Select the exact model during the single-node milestone after measuring memory and runtime compatibility. Do not change precision between compared configurations.

For LLM workloads, separately measure prefill, decode, time to first token, inter-token latency, completed requests/s, and generated tokens/s. Freeze prompt lengths, output lengths, decoding settings, and concurrency per experiment.

### Mode B — layer pipeline: follow-on

Partition a supported model into consecutive layer groups. Implement matching activation-transfer interfaces using custom TCP and MPI point-to-point operations. Record tensor shape, dtype, payload bytes, serialization/copy time, transfer time, stage compute time, and synchronization wait.

Start with batch size one and no overlap. Add microbatching only after correctness is established. Account for autoregressive feedback, stage imbalance, pipeline fill/drain, and per-request KV-cache ownership. Workers preserve cache state locally; they do not transfer full KV caches on every token. Define session cleanup on timeout or failure.

A pipeline can make a larger model fit across nodes without reducing single-request latency. Compare only configurations with the same model and inference settings, and identify capacity experiments separately from speed comparisons.

## 6. Functional requirements

| ID | Requirement | Acceptance evidence |
|---|---|---|
| F1 | Provision consistent hostnames, time synchronization, users, mounts, and software versions | Inventory and repeatable setup instructions |
| F2 | Submit one-, two-, four-, and eight-worker jobs with explicit CPU, RAM, and time requests | Batch scripts and scheduler/accounting output for available hardware |
| F3 | Launch container workers inside the allocation and terminate them on cancellation or timeout | Process/container audit after success, failure, and cancellation |
| F4 | Frame TCP messages with protocol version, request ID, payload length, dtype/shape where needed, and status | Correct handling of partial reads/writes, malformed messages, disconnects, and deadlines |
| F5 | Bound requests in flight and queue capacity; reject excess load explicitly | Load experiment showing overload errors without unbounded memory growth |
| F6 | Run MPI ping-pong, bandwidth, collective, and synthetic layer-exchange benchmarks | Per-message-size measurements and transport configuration |
| F7 | Verify model checksums before staging; avoid simultaneous writes to shared model files | Staging logs and checksum validation |
| F8 | Export host, worker, request, job, and storage metrics | Dashboard definitions and sample metric exports |
| F9 | Generate results linked to one immutable experiment manifest | Manifest, raw logs, metrics, and report share a run ID |
| F10 | Detect worker loss and prevent indefinite waits | Bounded failure time and explicit failed/retried request status |

### Slurm/container integration gate

Do not assume that installing Docker and Slurm makes their CPU limits, task tracking, or MPI launch compatible. Select and document one supported launch approach for the pinned versions. Prefer rootless operation where feasible; do not grant unrestricted Docker daemon access merely to launch a job.

Before scaling, prove on two nodes that the chosen integration launches ranks/workers on allocated hosts, enforces or verifies resource limits for the actual container processes, propagates exit codes, and cleans up all tasks. Validate MPI library and launcher compatibility inside and outside the image. A native-host diagnostic run can isolate launcher problems, but does not satisfy the containerized acceptance criterion.

## 7. Simulation design

### SimGrid implementation

Use S4U for the cluster/workload simulator: hosts execute calibrated compute activities, requests and activations become communication activities, and all traffic follows explicit network routes. Use SMPI for compatible MPI benchmark programs when evaluating actual MPI communication patterns. Keep synthetic workload scheduling separate from real Slurm execution.

Model the following inputs:

- Host compute service times by workload phase, thread count, and batch size; host capacity limits.
- Link bandwidth, latency, route, contention policy, and switch/uplink placement.
- Model size, local storage throughput, shared storage throughput, and concurrent startup demand.
- Request arrivals, service-time distributions, routing policy, queue capacity, deadlines, and failures.
- Pipeline stage times, activation bytes, microbatch count, and communication dependencies.

CPU LLM inference may be memory-bound. Fit compute activities to measured phase times rather than treating peak FLOPS as an adequate predictor. Track model memory footprints as explicit admission constraints; a timing model alone does not prove a deployment fits RAM.

Represent NFS as calibrated storage/network service demand and contention, with distinct cold-load and warm-cache cases. This abstraction does not reproduce NFS protocol details, Linux caching, lock semantics, or server failure behavior. Validate those on hardware.

### Calibration and validation

1. Measure single-node inference phase times and memory use.
2. Measure TCP and MPI latency/bandwidth across a range of message sizes, plus simultaneous flows.
3. Measure local reads and NFS reads with one and multiple clients; record cache conditions.
4. Fit parameters using designated calibration configurations.
5. Predict separate held-out workloads or concurrency levels, then compare against hardware measurements.
6. Freeze the calibrated configuration before reporting eight-node extrapolations.

If only one machine is available, label results **uncalibrated scenario estimates**. Multiple containers on one machine can validate orchestration and protocol behavior, but do not validate physical multi-node scaling.

## 8. Benchmark and experiment plan

Run a staged matrix instead of every possible combination. Establish the replicated baseline, isolate each bottleneck, then test combined stress.

| Experiment | Variables | Question and evidence |
|---|---|---|
| Worker scaling | 1, 2, 4, 8 workers; fixed per-worker model | Throughput, latency, CPU utilization, scaling efficiency |
| Topology | Chain vs star; matched link capacities | Which shared links explain performance differences? |
| Network stress | Concurrent flows and activation payload sizes | Where does useful throughput flatten as communication demand rises? |
| Storage startup | 1, 2, 4, 8 simultaneous starts; NFS vs staged weights | Cold-start duration, NFS throughput, network overlap |
| Compute stress | Threads, CPU affinity, batch size, concurrency | Oversubscription, NUMA effects where relevant, memory pressure |
| Transport | TCP vs MPI; equal payloads and process placement | Communication overhead and correctness under matched conditions |
| Pipeline | Stage count, partition balance, microbatching | Capacity benefit, idle time, latency, and throughput tradeoffs |
| Failure | Kill worker, interrupt link, remove NFS access | Detection time, request outcomes, task cleanup, recovery behavior |

Use fixed-work strong scaling and fixed-load-per-worker weak scaling as separate experiments. Report offered load and achieved load. Use an open-loop arrival generator for latency-under-load tests so slow responses do not silently reduce offered traffic.

For steady-state cases, use at least five independent runs, a documented warmup, and a measurement window of at least 120 seconds and 1,000 completed requests when feasible. Extend slow workloads or mark low-sample results. Separate cold startup from warm serving. Report run-to-run variation and confidence intervals; retain failures and timeouts in all success-rate denominators. Measure with monotonic clocks for elapsed time and synchronized wall clocks for correlating logs.

## 9. Metrics and success criteria

These are proposed acceptance targets, not achieved results. Adjust only through an explicit recorded revision after the baseline establishes practical limits.

| Area | Target / completion criterion |
|---|---|
| Correctness | Every transport passes a fixed deterministic input suite; pipeline outputs match the single-node reference within a predefined numerical tolerance |
| Reliability | At least 99% successful requests during each healthy steady-state acceptance run at a declared sustainable offered load |
| Resource control | No worker/container remains after cancellation; allocated CPU/RAM limits are verified on the actual worker processes |
| Replicated scaling | Investigate a goal of at least 5× one-worker throughput at eight workers, under the same model, settings, and declared p95 latency limit |
| Scaling decision | Missing the throughput goal is acceptable research evidence only with a quantified bottleneck and a controlled mitigation experiment; it does not pass the performance target |
| Simulation accuracy | Median absolute percentage error ≤20% for held-out job duration and aggregate throughput on measured hardware configurations; report worst-case error and scope |
| Failure handling | Worker loss detected and affected requests resolved as failed or safely retried within a configured 30-second bound |
| Visibility | 5-second metric scrape target; missing worker telemetry visible within 30 seconds; monitoring overhead measured against a disabled-monitoring run |
| Reproducibility | A fresh setup can repeat the reference run from pinned inputs and produce the same report fields |

Choose and freeze the p95 latency limit after single-node measurements and before scaling. Define throughput speedup as X(N)/X(1) and replicated scaling efficiency as X(N)/(N·X(1)), where X is successful requests/s or tokens/s under matched conditions. Do not equate aggregate throughput speedup with lower latency for one request.

## 10. Observability and fault behavior

Grafana views must show CPU utilization, RAM, swap, disk I/O, NIC bytes/errors, worker health, queue depth, in-flight requests, latency distributions, throughput, model-load time, and Slurm job state. Add switch port counters when hardware supports them. If switch telemetry is unavailable, identify per-link utilization as inferred and validate with controlled network tests.

Tag bounded-cardinality metrics with node, service, and execution mode; put request IDs and run IDs in structured logs or result files rather than unbounded metric labels. Keep completed benchmark exports after time-series retention expires.

Replicated mode may retry an independent inference request once on a healthy worker within its original deadline. Log attempt IDs and return a single terminal result per request. Pipeline worker failure aborts the affected pipeline job and marks in-flight requests failed; transparent stage recovery is outside the MVP. NFS failure must produce a bounded error during staging or result publication. Already loaded workers must be tested for continued operation without storage access.

The combined controller/storage/metrics host is a documented single point of failure. Reserve resources for those services and measure controller saturation before attributing every limit to workers or switches.

## 11. Implementation milestones

Indicative schedule: six to eight weeks of part-time work, dependent on hardware availability. Progress is gated by evidence, not dates.

| Milestone | Indicative window | Exit evidence |
|---|---|---|
| M0: inventory and choices | Week 1 | Hardware inventory, model/runtime choice, topology, version matrix, benchmark schema |
| M1: single-node baseline | Weeks 1–2 | Container inference, correctness suite, timing/memory measurements, repeatable benchmark |
| M2: two-node prototype | Weeks 2–3 | TCP/MPI tests, NFS staging, Slurm launch/limits/cancellation, calibrated network measurements |
| M3: monitoring and failures | Weeks 3–4 | Grafana dashboards, overload tests, failure logs, cleanup evidence |
| M4: calibrated simulation | Weeks 4–5 | Chain/star platform definitions, held-out error report, 1/2/4/8-node scenario results |
| M5: physical scale-out | Weeks 5–6 | Measurements for every available node count; eight-node deployment only if hardware exists |
| M6: pipeline and final analysis | Weeks 6–8 | Supported-model pipeline comparison, bottleneck study, reproduction guide and demo |

Simulation work can proceed before all workers arrive. Mark unavailable physical configurations as simulated; do not replace the missing measurements with extrapolations presented as observations.

## 12. Risks and mitigations

| Risk | Mitigation / decision rule |
|---|---|
| Ethernet communication dominates pipeline compute | Measure bytes and stage times; compare replication and local inference; retain a negative pipeline result |
| Model copies exceed RAM or memory bandwidth saturates | Measure peak RSS/cache usage, reserve headroom, reduce concurrency, and select a smaller fixed baseline model |
| Shared uplinks saturate during model startup | Stagger startup, stage locally, compare cold and warm runs |
| MPI/container/Slurm integration fails | Prove the two-node launch path before scaling; pin compatible versions and verify cleanup |
| Thread oversubscription masks scaling | Set worker thread pools and CPU affinity explicitly; record physical/logical cores and NUMA placement |
| Simulation overstates performance | Calibrate with measured phase times, validate on held-out cases, publish parameter sensitivity |
| Heterogeneous workers create stragglers | Inventory per-node performance; measure homogeneous and mixed configurations separately |
| Load generator or router becomes the bottleneck | Profile both and run a generator-capacity check before worker comparisons |

## 13. Deliverables and definition of done

- Versioned setup scripts/configurations, Docker image recipe, Slurm job files, TCP protocol specification, and MPI benchmark sources.
- Inventory and pinned software/model manifests with checksums and reproducible input generation.
- SimGrid platform files, workload simulator, calibration data, and held-out validation report.
- Grafana dashboard definitions, metrics configuration, structured logs, and benchmark exports.
- Comparison report covering chain vs star, scaling, storage staging, communication costs, and failure outcomes.
- Short demo: submit a job, observe inference, introduce a worker failure, inspect the result, and reproduce a simulation comparison.
- A status table marking each capability as designed, simulated, physically tested, or completed at eight nodes.

The MVP is complete when replicated inference, two-node physical integration, calibrated simulation, dashboards, and failure handling meet their acceptance criteria. The full eight-node hardware milestone and model-pipeline experiment are tracked independently. Every public or resume claim must match the evidence tier and the tested node count.

## 14. Decisions to resolve during M0

1. Actual CPU models, RAM, SSD capacity, NICs, switch capacities, and available node count.
2. Exact Ubuntu, Docker, Slurm, MPI, SimGrid, and model/runtime versions.
3. Model revision, precision, prompts, output lengths, CPU thread settings, and baseline latency limit.
4. Slurm/container launch approach, resource enforcement, and MPI transport selection.
5. Whether monitoring/storage require a separate host to avoid controller contention.

## 15. Technical references

The requirements above are project design decisions. These primary references support implementation choices and limitations; they do not establish project results.

- [SimGrid: calibration](https://simgrid.org/doc/latest/Calibrating_the_models.html) — derive simulation inputs from measured executions and validate predictions.
- [SimGrid: model configuration](https://simgrid.org/doc/latest/Configuring_SimGrid.html) — configure network factors and tracing.
- [SimGrid: SMPI](https://simgrid.org/doc/latest/app_smpi.html) — simulate supported MPI applications.
- [Slurm: containers](https://slurm.schedmd.com/containers.html) — select a supported container launch integration.
- [Slurm: MPI guide](https://slurm.schedmd.com/mpi_guide.html) — match MPI launch and process tracking to the cluster configuration.
- [Open MPI: launching with Slurm](https://docs.open-mpi.org/en/main/launching-apps/slurm.html) — verify the chosen launcher and PMIx configuration.
