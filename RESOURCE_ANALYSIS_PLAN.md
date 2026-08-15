# Experimental Plan: Diagnosing the Peer-Side Resource Bottleneck at Auriga's ~1100 TPS Commit Ceiling

## 1. Objective

Identify **which physical or system-level resource on the Fabric peer
VMs is at capacity** when Auriga's transaction pipeline stalls at its
~1074-1173 TPS commit-rate ceiling on the paper configuration
(MMC=1000, BatchTimeout=200 ms, N=15 workers).

The answer will point to the next optimization target for the Auriga
system (peer CPU tuning, disk provisioning, LevelDB replacement,
gRPC pool sizing, kernel network stack, etc.) or, if no single
resource saturates, reframe the investigation toward Fabric-internal
serialization points.

## 2. Background

The Auriga scheduler + custom submitter reproduces the ICDE paper's
numbers (~1200 TPS at ~99% success). Above roughly **1200 TPS target
send rate**, throughput plateaus at a hard commit-side ceiling
regardless of how much we push the client:

- **Client-side is not the bottleneck.** Our existing diagnostic
  `diagnose_ceiling.py` shows send-side TPS exceeds commit-side TPS
  throughout the ceiling window, and returns the verdict
  *"peer validation + ledger write is the wall."*
- **Orderer is not the bottleneck.** Broadcast-side TPS keeps pace
  with send; only commit falls behind.
- **What we do not yet know** is *which specific peer resource* is
  the wall: CPU, memory, disk bandwidth, disk IOPS, NIC bandwidth,
  gRPC handler concurrency, or LevelDB internals (compaction, WAL
  flush).

Answering this is a prerequisite for the "what's next after the
paper" scoping — we cannot propose the right follow-up without
knowing what physical constraint we are pushing against.

## 3. Experimental Method

### 3.1 Setup

- **Test bed** (unchanged): 8 peers across two VMs
  (192.168.0.153: Org1 + orderers 0/1; 192.168.0.212: Org2 +
  orderer 2). LevelDB backend. Paper's `best_config.json` on the
  orderer. Passwordless SSH root from the experiment driver.
- **Workload** (unchanged): the paper's Smallbank profile —
  Zipf α=0.65 over 1000 accounts, ~21% offline conflict rate,
  10,000-tx run.
- **Driver** (unchanged): `run_submitjs_pertx.py` with 15 workers,
  target 1200 TPS, `broadcast` gate mode.

### 3.2 Measurement additions

Add a **high-resolution telemetry layer** that runs during the
workload on both peer VMs. To catch sub-second dynamics
(block-cut jitter, LevelDB WAL bursts, gRPC handler stalls) the OS
metrics are sampled at **100 ms** by default, parameterizable via
`--fast-interval-ms` on the wrapper.

**(a) Operating-system metrics — fast loop (default 100 ms)**.
Read directly from `/proc` and cgroup v2 files (unified hierarchy at
`/sys/fs/cgroup/` — confirmed on both VMs). **No subprocess spawns
in the hot path** — a single Python process reads open file
descriptors each tick and emits JSONL:
- Per-container from `/sys/fs/cgroup/system.slice/docker-<id>.scope/`:
  `cpu.stat` → usage_usec delta → CPU%; `memory.current` → RSS;
  `io.stat` → per-device rbytes/wbytes/rios/wios.
- Per-VM from `/proc/stat` (per-CPU user/sys/iowait/idle),
  `/proc/diskstats` (per-device I/O counters + queue-time),
  `/proc/net/dev` (per-NIC RX/TX bytes and packets),
  `/proc/loadavg`, `/proc/meminfo`.

**Network bandwidth utilization** is derived from the per-NIC
counters. Each tick we compute `bytes/s` per direction (RX and TX
are full-duplex — reported separately) from consecutive `/proc/net/
dev` samples. Absolute Mbps is *always* recorded. To turn Mbps into
a **utilization percentage** we need a link-capacity denominator;
both peer VMs' primary NIC is virtualized (`eth0` reports
`speed=-1` in `/sys/class/net/eth0/speed`, confirmed on both hosts),
so the physical link speed cannot be read from sysfs. Handling:

- `/sys/class/net/<iface>/speed` is still read at collector start;
  when it returns a valid value (any non-virtual NIC) it seeds the
  denominator automatically.
- A `--nic-cap-mbps <N>` CLI flag lets the operator supply the
  physical cap manually when sysfs is uninformative.
- Optional: `--iperf3-probe` runs a single 5-second `iperf3` test
  between the two peer VMs *before* the workload starts, records
  the achieved throughput as the empirical cap, and passes it into
  the analyzer. This gives a *usable* saturation threshold even on
  virtualized NICs.
- If no cap is available, the analyzer reports absolute Mbps only
  and flags NIC saturation as "unknown-cap — please provide
  `--nic-cap-mbps`" rather than emitting a false NIC-BOUND verdict.

We do **not** use `docker stats`, `iostat`, or `sar` in the fast
loop — each spawns a subprocess (50-100 ms) and cannot sustain
100 ms cadence. They remain valid at 1 Hz as a *cross-check* and are
gathered in the slow loop below.

**(b) Fabric-internal metrics — slow loop (default 1000 ms,
parameterizable via `--slow-interval-ms`)**. `curl -s
localhost:<port>/metrics` on each peer/orderer Prometheus endpoint
(ports 9443-9453 for orderers, 9643-9673 for peers). Each scrape
serializes ~500 Fabric metrics — expensive enough on the peer side
(~10-30 ms of Go CPU per scrape) that pushing it below 500 ms would
itself perturb the measurement. We record:
- Ledger progress: `ledger_blockchain_height`, block-processing
  time histograms.
- Endorsement / broadcast counters.
- Goroutine count (proxy for gRPC handler saturation).
- Go runtime memory + CPU counters
  (`process_cpu_seconds_total`, `process_resident_memory_bytes`).

Both streams write JSONL to disk on the peer VMs; the driver
`rsync`s them back to a shared run directory. **No Fabric or
submitter code changes** — instrumentation is entirely external.

**Self-overhead budget and check.** The fast loop's own CPU
consumption is measured (self-`getrusage()` sampled at start and end)
and logged. Target: **< 5 % of one core** on each VM. If exceeded,
the collector auto-halves its rate and warns in the report. This is
how we enforce the "smallest interval that does not perturb the
measurement" requirement.

### 3.3 Run design

Single instrumented run at the paper configuration, ~90 seconds of
steady-state workload sandwiched between ~10 s of ramp-up and drain.
That is sufficient because the ceiling is already reproducible in
under 10 s of steady-state (see `experiments/p47-verify-reverted/`).

If the first run's verdict is ambiguous, follow-up runs will vary
one factor at a time to strengthen the attribution — for example,
re-running with `--num-submitters` reduced from 15 to 10 (which drops
throughput below the ceiling) provides a **contrast baseline** for
the same resource series, exposing the resource whose utilization
tracks throughput.

### 3.4 Sampling cadence

To catch sub-second phenomena (Fabric's 200 ms `BatchTimeout` puts
block-cut boundaries at ~5 Hz — a 1 Hz sampler would alias this away)
we use **two cadences, both parameterizable** via CLI flags:

| Source | Cadence | Default interval | Rationale |
|:-|:-|:-|:-|
| Per-container cgroup counters (CPU, mem, io) | Fast | 100 ms | Cheap `/sys/fs/cgroup/**` reads, no subprocess |
| Per-VM `/proc/{stat,diskstats,net/dev,loadavg,meminfo}` | Fast | 100 ms | Same reason |
| Fabric peer/orderer Prometheus scrape | Slow | 1000 ms | Each scrape serializes ~500 metrics; sub-second cadence would perturb the peer |
| `docker stats` / `iostat` / `sar` cross-check | Slow | 1000 ms | Subprocess spawns cost 50-100 ms; kept for validation only |

Flags: `--fast-interval-ms` (default 100), `--slow-interval-ms`
(default 1000). The fast loop self-throttles if its own CPU exceeds
5 % of one core — an explicit safeguard for the "smallest interval
that does not perturb the measurement" requirement.

## 4. Analysis

Data reduction happens in a post-hoc analyzer that reads the run's
per-transaction log (`tx.jsonl`) together with the collected
telemetry. Steps:

1. Bucket the client-side per-tx log and the fast-loop OS metrics to
   **100 ms buckets** (the native fast-loop cadence); upsample the
   slow-loop Prometheus series to 100 ms by hold-last-value. Align on
   wall-clock.
2. Identify the **steady-state window** — the interval during which
   commit-TPS is flat at the ceiling (trim ramp-up and drain).
3. Within that window, per (VM × container × resource) compute
   mean, p50, p95, and max utilization. Also produce **1-second
   rolling averages** for the summary tables (100 ms samples are for
   spike detection; 1 s averages are for the human-readable verdict).
   For each NIC, compute RX-Mbps and TX-Mbps series; when a link cap
   is known (from sysfs, `--nic-cap-mbps`, or the iperf3 probe),
   also emit RX-util% and TX-util% series.
4. Compute Pearson correlation between commit-TPS and each
   resource series at both cadences (100 ms and 1 s rolled). A
   resource that limits throughput often correlates *weakly* with
   commit-TPS at the ceiling — it is stuck at its own max while TPS
   is stuck at commit ceiling — but shows strong correlation during
   ramp-up. The analyzer reports both windows.
5. Apply a fixed decision table to declare a saturation verdict:

| Signature in steady-state window | Verdict |
|:-|:-|
| Any peer container CPU% ≥ 90% | **CPU-BOUND** on that peer |
| Peer container memory ≥ 90% | **MEMORY-BOUND** |
| VM data-disk `%util` ≥ 90% | **DISK-BANDWIDTH-BOUND** |
| Disk write IOPS at hardware ceiling | **IOPS-BOUND** |
| RX-util% or TX-util% ≥ 80% on any physical NIC (requires known link cap) | **NIC-BOUND** |
| Absolute NIC throughput within 20% of `iperf3`-measured empirical ceiling | **NIC-BOUND (empirical)** |
| NIC throughput high but no link-cap known | **NIC UTILIZATION UNKNOWN** — report Mbps, ask operator to supply `--nic-cap-mbps` |
| Goroutine count trending up monotonically | **GRPC HANDLER SATURATION** |
| Block-processing p95 latency > 500 ms with commit-TPS flat | **VALIDATION-BOUND** (compute or lock contention inside the peer) |
| Nothing crosses threshold | **NO SINGLE SATURATION** — pursue Fabric-internal serialization (channel locks, LevelDB WAL) |

## 5. Expected outcomes and follow-ups

We view the possible outcomes and what each implies:

1. **CPU-bound on a specific peer.** Next step: `perf top`/`pprof`
   of that peer during the ceiling to identify the hot function
   (validation, endorsement, gossip). Optimization targets are then
   scoped.
2. **Disk-write-bandwidth- or IOPS-bound.** Next step: profile the
   LevelDB write pattern; compare a redeploy with the ledger
   directory on a faster (or ramdisk) volume; consider LSM tuning.
3. **NIC-bound.** Next step: check MTU, offloading, `iperf3` between
   peers; only then consider a 25 GbE upgrade or peer collocation.
4. **gRPC handler saturation.** Next step: raise
   `GATEWAY_CONCURRENCY` and endorser worker pool; verify goroutine
   ceiling moves.
5. **Nothing single-resource-bound.** Then the bottleneck is
   Fabric-internal (per-channel serialization, ledger commit lock,
   MSP verify, block re-serialization). This becomes a *research*
   direction (Fabric-side patches) rather than a *deployment*
   direction (bigger hardware). This is the most scientifically
   interesting outcome and one of the plausible ones — Fabric's
   commit path holds a channel-wide lock in current versions.

## 6. Scope and non-scope

**In scope**:
- One instrumented paper-config run + at most three follow-up
  contrast runs.
- A reusable per-run correlation report.

**Out of scope for this experiment**:
- Any change to Fabric configuration, submitter, or scheduler code.
- Cross-machine comparisons (different hardware).
- Full workload sweeps — those are already covered by the auto-tuner
  verification history.

## 7. Effort and timeline

- Telemetry collector + analyzer implementation: ~1 working day.
- Instrumented run + analysis on paper config: ~1 hour (cluster
  redeploy + seed ~5 min; run ~2 min; analysis ~1 min; margin).
- Contrast runs, if needed: ~2 hours.

Total: **1-2 working days** to a defensible attribution of the
ceiling to a specific system resource (or to a defensible
"Fabric-internal" verdict that redirects follow-up work).

## 8. Deliverables

- Short measurement toolkit under `experiments/scripts/`
  (collector + analyzer + wrapper).
- One instrumented run archive under `experiments/p48-*/` with the
  standard `tx.jsonl` / `blocks.jsonl` / `counters.jsonl` /
  `summary.json` plus a new `resmon/` subtree.
- A verdict document (Markdown) with the correlation tables and
  the resource attribution.
