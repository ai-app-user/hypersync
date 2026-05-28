# HyperSync — NFS-to-NFS High Speed Transfer Engine
## Design Document v1.0

**Target:** 400 Gbit/s LAN • 100 Gbit/s WAN @ 60ms RTT  
**Stack:** C++20 — libnfs async — io_uring — BBR/TCP — NVMe cache

---

## 0. Mandatory Architecture Principles

These principles are requirements for all Hypersync production code. They are
not preferences, optimizations, or optional cleanup goals. If an implementation
cannot follow one of these rules, the design must be discussed and explicitly
approved before code is written.

### 0.1 Everything Is a Job Connected by Queues

Hypersync is built from independent Jobs connected only by bounded queues of
opaque buffer handles.

- Every Job has explicit input queue(s) and output queue(s).
- A Job must not know which specific Job produced its input.
- A Job must not know which specific Job consumes its output.
- A Job may have one or more worker threads, but workers execute the same Job
  responsibility and must not depend on each other for normal progress.
- A Job owns its internal settings: thread count, async depth, batching limits,
  hash mode, transport endpoint, or writer options.
- Pipeline topology is configured outside the Job. Reusing the same Job in a
  scan, diff, sync, generator, or benchmark pipeline must not require changing
  the Job implementation.
- Combining unrelated responsibilities into one worker because it is convenient
  is not allowed. For example, an NFS scanner must not write sockets, a differ
  must not write CSV/Parquet, and a sender/receiver must not understand file
  metadata.

The only allowed communication between Jobs is ownership transfer of existing
buffers through queues.

Command-line modes, benchmarks, and diagnostic performance paths are not exempt
from this rule. An option that bypasses the pipeline by directly invoking a
specific downstream job, retaining typed records outside buffer ownership, or
using a private non-queue side channel must be converted into normal Jobs or
removed. Compatibility flags may remain only as aliases for the compliant
pipeline topology.

#### 0.1.1 Jobs, Queues, Pipelines, and Scenarios

Hypersync diagrams must distinguish concrete implementation from conceptual
workflow names:

- `[JobName-N/options]` is a concrete job instance. `N` is the worker count,
  reactor count, lane count, or other concurrency value shown by that job.
- `(QueueName-N/options)` is a concrete queue or queue family. If `N` is depth,
  shard count, or lane count, the diagram or surrounding text must make that
  clear.
- `{PipelineName}` is a named, reusable pipeline block. It has input(s),
  output(s), configuration, and variations, but it expands into normal jobs and
  queues. A pipeline is composition, not a new execution primitive.
- `{{ScenarioName}}` is a user-facing or benchmark use case composed from
  pipelines, jobs, and queues.

This lets us describe workflows at the right level. For example:

```text
{{Scanner}}      : {MetaReader}->(MetaQueue)->{MetaWriter}
{{Hash-Scanner}} : {MetaReader}->(MetaQueue)->[DataReader]->{Hasher}->{MetaWriter}
```

`{MetaReader}` may expand differently for NFS, synthetic replay, or local FS,
but the scenario remains readable. When tuning or debugging, expand the
pipeline:

```text
{MetaReader-NFS} = [FolderSeeder-1]->(FolderQueue)->[MetaReader-NFS-96]
```

Pipeline variations must be named in design or config. Examples:
`{MetaWriter/stats-only}`, `{MetaWriter/partitioned-parquet}`,
`{DataWriter-NFS/small-reactor}`, and `{Transport/shared-nothing}`.

The expanded graph is authoritative for implementation. A scenario or pipeline
name must not hide direct calls between jobs, private non-queue side channels,
or unbounded buffers.

### 0.2 Queues Carry Ownership, Not Bytes

Queues store only buffer handles. A queue does not store file records, paths,
serialized payloads, `std::string`, `std::vector`, typed records, or data bytes.

Pushing to a queue transfers ownership of an already allocated buffer. Popping
from a queue transfers ownership to the consuming Job worker. The queue does not
copy, inspect, allocate, free, parse, or transform the buffer payload.

All production pipeline edges must use bounded queues. Bounded queues are the
normal flow-control mechanism. If a downstream stage cannot keep up, the output
queue fills and only then does the upstream producer slow down.

### 0.3 No Payload Memcpy Between Jobs

Large payload buffers must not be copied between Jobs. Hypersync moves ownership
of buffers, not bytes.

- Metadata and data buffers are preallocated at startup.
- A Job may modify a buffer only while it owns the handle.
- A Job forwards work by pushing the same handle to the next queue.
- A Job discards work by releasing the same handle back to the pool.
- File data must not be copied from one pipeline buffer into another pipeline
  buffer.
- Serialization into temporary strings or vectors is not allowed on hot
  pipeline paths.

The only acceptable payload copy is one that is unavoidable at an external API
boundary, such as libnfs copying data into memory it controls or the kernel
copying data for a non-zero-copy socket write. Even then, the copy must remain
inside the backend adapter or transport Job. It must not leak into generic Job
interfaces or become a Job-to-Job handoff mechanism.

For NFSv3 data reads, the preferred small-file fast path is:

`READDIRPLUS metadata scan -> FileSpec with opaque NFS handle -> raw READ by handle`

This avoids per-file `open()`/`ACCESS`/`close()` on the data path. The NFS file
handle is application-specific metadata carried inside metadata buffers. Generic
Jobs, queues, senders, receivers, and discarders must still treat the buffer as
opaque and must not interpret the handle.

### 0.4 No Runtime Allocation on Hot Paths

All buffers, queue cells, batch storage, and transport frame buffers required
for steady-state operation are allocated during startup. Hot-path code must not
allocate or free per file, per chunk, per folder, per network frame, or per
result record.

Small configuration objects, command-line parsing, test fixtures, and
non-production diagnostics may allocate. Production Jobs must use preallocated
buffers and reusable scratch state sized by configuration.

### 0.5 Universal Jobs, Domain-Specific Payload Views

Generic infrastructure must stay generic.

- Sender and Receiver Jobs operate on generic buffers only.
- Buffer pools and queues operate on generic buffers only.
- Discarder, generator, transport, and monitoring helpers must not depend on
  NFS, DuckDB, Parquet, hashes, file paths, or sync/diff semantics.
- Domain-specific code may interpret a buffer through payload view helpers only
  after a Job owns the handle.

If a Job needs special behavior, that behavior belongs in a domain-specific Job
or payload view, not in the generic queue, pool, sender, receiver, or monitoring
layer.

Job names describe the work performed, not the storage backend. Metadata and
data readers are generic Jobs:

`MetaReader`, `DataReader`

Backend-specific behavior belongs behind the backend interface selected by the
source URL:

`nfs://... -> NFS`, `synthetic-profile://... -> SYN`, local filesystem paths
`-> FS`.

Pipeline and status displays must include the backend suffix when ambiguity
matters:

`[MetaReader-NFS-8]`, `[DataReader-SYN-64]`, `[DataReader-FS-4]`.

Do not introduce parallel job names such as `NfsDataReader`,
`SyntheticDataReader`, or `FsDataReader` for the same logical pipeline role.
The real NFS, synthetic profile, and filesystem-specific code lives in backend
adapters, not in separate pipeline job types.

### 0.6 Waiting and Backpressure Rules

Jobs are expected to run at maximum useful speed. A Job must never wait for a
specific downstream Job or upstream Job. It only interacts with queues.

Allowed waits:

- A consumer worker may wait when its input queue is empty.
- A producer worker may wait when its output queue is full.
- An external-I/O Job may wait for the external API it owns, while keeping its
  configured async depth or transport concurrency full.
- Shutdown may wait for owned buffers to be forwarded, released, or flushed
  according to the pipeline's graceful-stop policy.

Not allowed:

- Scanner workers waiting on sockets.
- Differ/checker workers waiting on report writers.
- Sender/receiver Jobs interpreting metadata to decide custom behavior.
- Global mutexes that serialize unrelated workers on hot-path data transfer.
- Unbounded queues that hide backpressure by growing memory.
- Side channels where one Job calls another Job directly.

Metrics must make wait reasons visible: empty-input wait, full-output wait,
external-I/O wait, processing time, records processed, bytes processed, and
queue depth/high-water marks.

### 0.7 Remote Deployment Is Package-Only

Remote hosts must run Hypersync from the supported deploy bundle only.

- Do not copy individual source files to remote hosts for normal testing,
  benchmarking, production scans, diffs, or sync runs.
- Do not rebuild from an ad-hoc remote source tree as part of operational
  verification.
- The only supported remote artifact is the flat deployment folder or tarball
  produced by `hypersync/deploy/package-linux.sh`.
- Remote commands must use the bundle launcher `./hypersync`, not a source-tree
  binary path, so bundled libraries and `default.yaml` are the ones under test.
- Each remote test or production run should use a timestamped deployment
  directory under the host's deployment root. Reusing a mutable source checkout
  hides what version was actually tested.
- If a remote issue requires debugging with source, pause and discuss it first.
  Source-level remote debugging is an exception, not the deployment model.

The manifest and checksum files shipped in the bundle are part of the
reproducibility contract. Performance results must record the deployment path
and, when available, the manifest git commit/dirty state.

### 0.8 Linux Build Authority

Linux builds, package creation, and real NFS performance validation must run on
the primary Linux development host for the current environment. For this phase,
that host is `transfer1`.

- macOS workstations are coordination clients only for this project. Do not use
  macOS or macOS-hosted containers to produce Linux performance artifacts unless
  explicitly approved for a one-off diagnostic.
- Source checkouts on the Linux development host must come from Git, not from
  ad-hoc source-file copies.
- Runtime hosts still follow the package-only rule above. Having a source
  checkout on the primary Linux development host does not make source-tree
  runtime deployments acceptable.
- Release and benchmark manifests must identify the Linux host, git commits,
  dirty state, build path, deployment path, and the package archive used.
- If the primary Linux host changes, update this section and record the change
  in performance notes before comparing benchmark results.

### 0.8.1 Project Boundaries

Hypersync is the product and scenario layer. It composes jobs into user-facing
scenarios, owns CLI commands, profiler policy, scan/diff/copy/sync behavior,
performance gates, and product configuration.

Reusable lower layers live in sibling projects:

- Piper owns generic pipeline infrastructure: buffer pools, queues, generic
  jobs, monitoring, and autoscaling.
- Utils owns generic helper functions and small reusable algorithms such as
  content hashing over byte buffers.
- Connector owns generic buffer transport over sockets: TCP/Unix socket helpers
  and buffer sender/receiver jobs that know only buffers and lengths.
- Filer owns filesystem I/O jobs and backend adapters: metadata/data readers,
  target metadata/data writers, and NFS/NULL backend mechanics.

When a change is purely about filesystem read/write mechanics, prefer moving it
into Filer. When a change is about how a product scenario combines those jobs,
keep it in Hypersync.

### 0.8.2 Buffer Ownership and Self-Description

Hypersync scenarios must preserve the generic buffer model from Piper. Jobs may
have zero, one, or many input queues and zero, one, or many output queues, but
queues move `BufferHandle` ownership rather than typed records or payload bytes.
Most jobs should not inspect buffer contents.

Raw buffers are preallocated for the process lifetime. Current high-volume
buffer classes reserve extra metadata space beside payload data, for example:

```text
4KiB + 4KiB
128KiB + 4KiB
1MiB + 4KiB
```

When a raw buffer must describe its own logical payload, use Piper's generic
buffer metadata footer. The final bytes are:

```text
[data_size:u32][metadata_version:u16][metadata_size:u16][magic:u16]
```

Metadata can define checksum algorithm, data checksum, metadata checksum, and
packed sub-buffer descriptors. `checksum_algorithm=none` disables checksums;
an individual checksum value of `0` means that checksum is not used. Metadata
can also be copied immediately after logical data when reserve space allows a
shrunk `[data][metadata]` view. Product meanings such as file, NFS, scan, diff,
copy, sync, parquet, or profiler records must be layered above this generic
format by codecs.

### 0.9 Mandatory Generic Instrumentation

Every production Job must be observable through the shared monitor vocabulary.
Instrumentation is part of the architecture, not optional debug code.

- Jobs must expose cumulative processed count and byte count where those values
  are meaningful.
- Jobs that process domain records must expose the domain unit through monitor
  snapshots, for example records/s, files/s, folders/s, or logical bytes/s.
- Generic runtime metrics must classify worker wall time as processing, waiting
  for input, waiting for output capacity, waiting for free pool buffers, waiting
  in owned I/O, or stopped.
- Periodic reports must include cumulative rate, recent/current rate, first
  observed startup rate, mid-run historical rate, peak observed rate, tail rate
  after a job stops, queue depth/fullness, and worker wait-state percentages.
- The same metrics must be usable for live status, performance tests, and final
  summaries so scan, diff, sync, generator, transport, writer, and checker
  pipelines can be compared without bespoke reporting code.
- Instrumentation must avoid hot-path overhead. Queue/pool helpers should take a
  timestamp only after the non-blocking fast path fails, and domain Jobs should
  update counters with relaxed atomics or batch-local accumulation.

When a benchmark or real run is slower than expected, the first answer should be
visible from the generic status: which Jobs are busy, which are starved, which
are backpressured, which are pool-limited, and which are waiting inside external
I/O.

### 0.8 Prototypes Must Not Redefine the Architecture

Experimental code is allowed only when it is clearly isolated and documented as
non-production. Prototype shortcuts must not become the default architecture.
Before merging a feature into the production pipeline, it must be converted to
the Job + bounded queue + preallocated buffer ownership model described here.

### 0.9 Priority Streams and Autoscaling

Small-file and large-file data paths must be independent streams when they have
different performance goals.

Required pattern:

`Scanner -> FileClassifier -> SmallDataReader -> small data queue`

`Scanner -> FileClassifier -> LargeDataReader -> large data queue`

For very high-rate data read pipelines, the scanner/classifier may be split
into two independent scanner fleets over the same source namespace:

`SmallScanner(filter <= threshold) -> small reservoir -> SmallDataReader`

`LargeScanner(filter > threshold) -> large reservoir -> LargeDataReader`

This is allowed only when the queues remain bounded and each scanner fleet is
independently backpressured by its own reservoir. The purpose is to avoid a
full large-file queue pausing small-file discovery, or a full small-file
reservoir pausing large-file discovery. Classification boundaries must be
gapless: small is `size <= threshold`, large is `size > threshold`.

A third scanner fleet may run as background reconnaissance:

`ReconScanner(throttled full crawl) -> statistical accumulator`

The recon scanner never preserves raw NFS handles and never feeds data readers.
It extracts counts and logical sizes, updates relaxed atomic counters, and
releases metadata pages immediately. It must be explicitly throttled by
configuration, normally `async_depth=1` plus a per-page sleep, so it cannot
consume the RPC slots and NIC queues needed by the production scanner and data
reader lanes.

When the large scanner completes and the large reservoir is empty, bulk reader
workers may morph into small-file readers by pulling from the small reservoir.
This morph is a queue-provider transition only: the worker still owns generic
buffer handles, generic queues remain opaque, and callbacks must account bytes
and files according to the route of the file actually supplied.

Mixed small/large reads may enable the bucket-priority coordinator. The
coordinator is not a bandwidth throttle. It samples bucket progress, computes
rolling small-file and large-file rates over the last minute, estimates
remaining ETA for each bucket, and changes only queue selection priorities and
active worker limits. Small-file ETA is file-count based because the small path
is operation-rate limited. Large-file ETA is byte based, using remaining logical
large-file bytes divided by rolling large read bandwidth, because the large path
is bandwidth limited. If the small bucket has the longer ETA, large-reader
workers may borrow a configured percentage of their provider pulls for the
small reservoir while still falling back to large work whenever small work is
not ready. If the large bucket has the longer ETA, the borrow percentage returns
to zero and large readers stay on the large reservoir. The goal is equalized
small/large ETA while keeping all useful data-reader capacity busy.

Bucket-priority progress reporting prints one compact human progress line per
minute, for example:

`progress 4:33a , s: 4.7M/5% 33K/s eta:18.3h , L: 20.4T/7% 150Gbit/s eta:17.5h , T: 185Gbit/s`

`s` is the small-file bucket: total small files discovered so far, percent
processed, last-minute small files/sec, and ETA. `L` is the large-file bucket:
total large logical capacity discovered so far, percent processed, last-minute
large bandwidth, and ETA. `T` is last-minute total bandwidth. Until the
independent recon scanner finishes, totals are discovered-so-far values. Once
recon finishes, the same line becomes whole-tree progress. Production scanners
remain reservoir-backpressured; recon is the only source of full-tree totals
during a running transfer. In bucket-priority mode, the recon default is a fast
stats-only crawl unless the command line explicitly sets recon thread/depth/sleep
values.

The same ETA signal also shifts production scanner capacity. Reader borrowing
without scanner borrowing can starve the borrowed readers if the small reservoir
is not replenished fast enough. Reader borrowing is represented as a generic
Piper overload score, and scanner capacity follows that score. In normal state
the mixed profile keeps the production scanners at `96 small / 16 large`. When
the controller sets a non-zero large-reader small-priority percentage, it parks
enough large scanner workers to respect a floor of eight active large scanners
and wakes the same number of spare small scanner workers, yielding
`104 small / 8 large` for the current transfer1 profile. When the borrow
percentage returns to zero, the scanner split returns to `96 / 16`. Workers
park only between folder batches; no scanner is interrupted while it owns a
folder or is inside libnfs.

The transport sender drains the small-file data queue first and drains the
large-file queue only when the small queue is empty or below its configured low
watermark. This gives small files maximum operation rate while allowing large
files to consume all otherwise-idle bandwidth. A separate bandwidth throttle is
not the default control mechanism; bounded queues and priority selection are.

Thread-count autoscaling must be generic. Piper may adjust a Job's active worker
limit using queue fullness, worker wait-state percentages, and throughput
trends. Domain Jobs may expose extra metrics, but they must not embed custom
cross-Job scaling logic or call neighboring Jobs directly. Scale-down is
cooperative: workers park between file/buffer batches and are never interrupted
while owning a buffer or waiting inside libnfs.

Piper also accepts an optional `overload_score` in its generic autoscale metrics.
This is the only approved escape hatch for workload-specific pressure. A score
of `1.0` means the lane is balanced, `>1.0` asks Piper to scale the lane up, and
`<1.0` allows Piper to reclaim workers. The callback that computes this score
may look at Hypersync-domain facts such as ETA tension or reservoir depth, but
the callback must not resize jobs directly.

For bucket-priority transfers, Hypersync supplies two overload callbacks:

- Small-file lane: reports overload when small ETA is more than 10% later than
  large ETA, or when the small reservoir is below its low-watermark for three
  consecutive samples while readers are active. It reports underload when the
  reservoir is pinned at the high-watermark and scanners are sleeping most of
  the time.
- Large-file lane: reports overload when the large reservoir is full but
  aggregate bandwidth is materially below the configured line-rate baseline,
  indicating the large data path lacks active capacity. It reports underload
  when the large queue is shallow while the wire is already near line rate.

Piper consumes these scores through the same `AutoScaler` used by other jobs.
The mixed transfer profile still applies safe floors, including keeping the
small data-reader pool at or above the configured baseline during mixed phase.
This makes the mechanism hardware-agnostic: higher-spec hosts converge to more
active workers, while smaller hosts stop scaling before they thrash CPU caches,
NIC queues, or server RPC slots.

Pipeline autoscaling must tune one Job at a time in pipeline order. The first
stage is tuned by measuring how quickly it pushes to its output queue(s). When
that stage reaches its configured limit or added workers fail to improve output
throughput, the controller backs off and moves to the next stage. A failed probe
also reduces the next probe step: 100%, 50%, 25%, then 12.5% by default. All
autoscaling must obey each Job's configured min/max bounds.

Autoscale defaults are persisted per pipeline profile. If a Job is not present
in the selected profile, it is auto-added with:

- `autoscale: true`
- `min_workers: 1`
- `initial_workers: 1`
- `max_workers: auto`

`max_workers: auto` means `cpu_count * 2`, capped by any Job-specific safe
capacity configured by the pipeline. First runs may therefore start unknown
autoscalable Jobs at one active worker, discover steady state, and write learned
values to the profile. Later runs should start from the learned values for that
specific pipeline profile, while still allowing live correction if the host,
storage backend, or workload mix changes. Different pipelines, such as metadata
scan, small/large data read, diff, sync, and writer pipelines, must keep
separate learned profiles because the same Job can need different settings in
different contexts.

### 0.10 Synthetic Workload Engine

Hypersync must be able to validate autoscaling, queue thresholds, and mixed
small/large routing without depending on a live storage array. The synthetic
workload engine has two independent modes:

- **Profile capture:** one high-speed live metadata scan builds a compact,
  storage-agnostic workload profile. It does not persist paths or file handles.
- **Synthetic replay:** a zero-storage replay cursor emits deterministic
  metadata/data work shaped by that profile, usually faster than any NFS or
  local filesystem reader.

Profile capture groups the live namespace in chronological scan order. It
observes fixed-size blocks, normally one million files per block, and compares
the current block with the active phase. If the small-file ratio or future
distribution checks shift beyond the configured threshold, the current phase is
sealed and a new phase begins. Adjacent similar blocks are merged so a
multi-billion-file tree normally compresses to a small number of macro-phases.

Each phase stores histograms and totals only:

- explicit file-size buckets
- small and large file counts using the gapless boundary `small <= threshold`,
  `large > threshold`
- logical bytes by size bucket
- files-per-folder fanout buckets and depth totals
- filename length totals, UID/GID/mode distributions when enabled
- latency percentiles for READDIRPLUS pages, small reads, and large block reads

The replay hot path must not allocate heap memory, format variable strings into
heap objects, or track file handles in global lookup tables. It generates paths
into fixed stack/reusable buffers, creates deterministic 64-byte opaque NFS
handles from `(seed, phase_id, folder_id, file_id)`, samples size buckets from
the active phase, and returns pointer/length or view records to downstream
pipeline adapters. Payload data comes from preallocated fixed-size pools such as
4 KiB small buffers and 1 MiB large buffers. Rate limiting and latency emulation
are disabled by default; they are enabled only to reproduce a degraded backend.

Replay phases advance by counters: emitted files, bytes, folders, or elapsed
time depending on profile settings. Scale knobs may multiply file counts and
data volume so a compact profile captured from a smaller run can stress
multi-billion-file buffers and petabyte-scale data-path behavior.

---

## 1. Overview

HyperSync is a high-speed, NFS-to-NFS file transfer engine targeting 400 Gbit/s on LAN and 100 Gbit/s sustained throughput over a 60 ms WAN link. It is written in C++20 and built around a pipeline of self-contained Jobs that communicate exclusively through ownership transfer of pre-allocated memory buffer slots. No dynamic memory allocation occurs during operation.

The system is designed in two layers:

- **V1 (ship fast):** Standard TCP+BBR transport, XFS filesystem cache on NVMe, libnfs async I/O throughout. Full functional correctness with strong throughput.
- **V2 (optimized after profiling):** MSG_ZEROCOPY or DPDK+custom UDP transport, raw per-drive ring buffers on NVMe via io_uring. Applied only where profiling identifies real bottlenecks.

The core architecture — buffer pool, Job interfaces, pipeline topology, two-channel network design, watermark scheme, state machines — is identical between V1 and V2. Transport and storage backends are swappable without touching Job logic.

### 1.1 Key Performance Numbers

| Parameter | Value | Notes |
|---|---|---|
| LAN target throughput | 400 Gbit/s | Wire rate, limited by NIC and NFS |
| WAN target throughput | 100 Gbit/s sustained | 60 ms RTT design point |
| Bandwidth-delay product | 750 MB | In-flight data to fill 100G/60ms pipe |
| Small file throughput | 100,000 files/sec | Each file one buffer, priority channel metadata |
| RAM buffer pool | ~16.5 GB | 16K × 1MB + 16K × 4KB + 1M × 512B |
| NVMe cache | 16 TB across 7 drives | ~21 min backlog at 12.5 GB/s intake |
| NVMe aggregate write BW | 49 GB/s (7 × 7 GB/s) | 4× headroom over line rate intake |
| TCP data connections | 8–16 persistent | Matched to NIC RX/TX queue count |
| TCP priority connections | 1–2 persistent | Metadata, ACKs, control only |

### 1.2 Design Principles

- **Mandatory architecture.** Section 0 is the governing contract for all
  production implementation. Local shortcuts are not acceptable unless discussed
  and approved before implementation.
- **No runtime allocation.** All hot-path buffers are pre-allocated at startup
  from BufferPool. Jobs transfer slot ownership via opaque handles; they never
  allocate or free payload memory during steady-state operation.
- **No Job-to-Job payload copying.** Jobs transfer ownership of existing buffers.
  They do not copy metadata batches, file data, result records, or transport
  frames between pipeline stages.
- **Pull/push data flow through queues.** Each Job pulls owned buffers from input
  queues and pushes owned buffers to output queues. Backpressure is represented
  by bounded queues becoming full.
- **Queues carry ownership, not data.** A queue node contains only a handle to an
  existing pre-allocated buffer. It never contains `std::string`, `std::vector`,
  `FileSpec`, serialized payload bytes, or any dynamically allocated record.
- **Jobs are connected only by buffer queues.** Every Job has named input and
  output ports. A Job does not know which Job produced an input buffer or which
  Job will consume an output buffer.
- **Jobs own their threading.** Every Job has its own worker-thread count and
  internal parameters. Pipeline composition does not change the Job's
  implementation.
- **Generic transport.** Sender and Receiver Jobs move generic buffers. They do
  not contain NFS, metadata, diff, sync, hash, CSV, DuckDB, or Parquet-specific
  logic.
- **Two-channel network where needed.** Priority/control traffic and bulk data
  traffic use separate transport queues/connections so large data chunks cannot
  head-of-line block metadata or result messages.
- **One Job, one concern.** Each Job has a narrow interface and a single
  responsibility. Business logic is self-contained inside the appropriate Job,
  not spread across neighboring Jobs.
- **Self-describing domain payloads.** Domain-specific payload layouts are
  interpreted only through Hypersync payload view helpers after a Job owns a
  generic buffer handle.
- **Swappable backends.** Transport, NFS, DuckDB/Parquet, and NVMe cache
  implementations are hidden behind Job/backend abstractions. Backend
  optimizations do not touch generic Job or queue logic.

---

## 2. Operational Modes

HyperSync operates in three modes selected via `--mode` flag. All modes share the same Job pipeline skeleton; inactive Jobs are replaced by no-ops.

### 2.1 TRANSFER (default)

Full pipeline active. Source NFS read, optional check against target, data transfer over WAN/LAN, target NFS write. Computes xxHash64 of every file during DataReader reads. Receiver verifies hash before sending DONE ACK. Results appended to scan CSV on both sides.

### 2.2 SCAN

Source side only. Reads all file records, reads all file data to compute xxHash64, writes results to scan CSV. No network activity, no target NFS operations. Use to build a baseline hash database before first transfer or to audit source content independently.

```
hypersync --mode scan --source /mnt/nfs/data --output scan_source.csv
```

Pipeline active in SCAN mode: `InputProvider → NfsMetaReader → NfsDataReader (hash only) → ScanWriter`. Checker and all network Jobs are inactive.

### 2.3 DRY-RUN

Full pipeline logic, no data reads, no target NFS writes. Loads scan CSVs from both sides if available (avoids re-reading source data). Reports what would be transferred: new files, changed files, skipped files, total bytes, estimated transfer time. Output: human-readable report + machine-readable diff CSV.

```
hypersync --mode dry-run --source /mnt/nfs/data --source-scan scan_source.csv --target-scan scan_target.csv
```

### 2.4 Scan CSV Format

One row per file. Written atomically per-file as transfer completes (sender) or write completes (receiver). Both sides maintain independent scan CSV files.

```
folder_hash,file_hash,rel_path,size,mtime,mode,uid,gid,data_hash,hash_ts,scan_side
```

`scan_side` is either `S` (source) or `T` (target). The Checker loads both CSVs at startup for fast in-memory comparison before falling back to live NFS stat. V2 will replace CSV with a flat binary index for faster startup on large trees.

---

## 3. Memory Model

All memory is pre-allocated once at process startup from a central BufferPool. There are no calls to `malloc` or `new` during normal operation. Ownership of a slot is an atomic `uint32` index. When a Job acquires a slot it is the sole owner; no other entity reads or writes it until ownership is explicitly transferred or released back to the pool.

The hot pipeline uses two buffer families:

- **Metadata buffers** carry file records, folder records, control messages, ACKs, errors, and output records. The payload layout is interpreted by the consuming Job.
- **Data buffers** carry file bytes plus a self-describing trailer. The payload may be small-file or large-chunk sized, but the queue abstraction treats it as a data-buffer handle.

The buffer pool id is part of the handle. Core WSync code maps pool ids to domain payload families such as metadata buffers and data buffers, while the generic buffer primitives treat the id as an opaque ownership namespace. A queue may be configured to accept only metadata buffers, only data buffers, or both when a Job intentionally has a mixed control/data input. Generic Jobs such as a test discarder may consume any `BufferHandle` queue and release handles through the pool registry without knowing the concrete payload type.

Implementation split: generic ownership primitives (`BufferHandle`, `BufQueue`, `RawBufferPool`, raw-buffer view helpers, and the pool registry) live in `piper/src/common/`; WSync payload definitions (`MetadataBuffer`, `DataBuffer`, and payload-specific view functions) live in `hypersync/src/core/`.

### 3.0 Ownership Rules

Every buffer is in exactly one ownership state:

| Owner | Meaning |
|---|---|
| Free pool | The buffer is empty and available for acquisition. |
| Queue | The buffer handle is enqueued and waiting for a Job worker. |
| Job worker | A worker popped the handle and may read or modify the buffer. |
| Backend operation | An async NFS, network, or disk operation temporarily owns the buffer until completion. |
| Cache lease | The buffer represents data staged through NVMe and is owned by cache bookkeeping. |

Ownership transfer is always explicit:

1. A Job acquires an empty buffer from the pool or pops an owned buffer from an input queue.
2. The Job reads or modifies the buffer content according to its own implementation.
3. The Job either pushes the handle to an output queue, releases the handle to the free pool, or transfers it to an async backend operation.
4. A released buffer has no owner and its previous content is invalid. Consumers must never rely on stale bytes after release.

The pool tracks handle validity with a pool id and generation counter so wrong-pool handles, stale handles, and double releases can be detected in debug and test builds. The generation is not a routing mechanism; it exists only to protect ownership correctness.

### 3.0.1 Buffer Pool

The Buffer Pool is created during application startup from configured counts and byte sizes. It allocates raw fixed-size byte slots up front and never grows during runtime. Metadata/data access is done by element-level view functions that interpret an owned raw buffer handle as a payload structure; there are no typed pool classes.

Pool responsibilities:

- Store fixed-size byte slots.
- Maintain a free-list of buffer handles.
- Provide `try_acquire()` for non-blocking acquisition.
- Provide a wait/spin acquisition helper for Jobs that want backpressure.
- Validate handle pool id, index, and generation.
- Release/discard a handle back to the free list.
- Expose sizing and stats: element count, bytes per element, total pre-allocated bytes, available, in-use, and peak in-use.
- Support generic release by pool id through a pool registry so type-agnostic Jobs can discard buffers from mixed queues.

The free-list is itself a pre-allocated handle queue. Releasing a buffer pushes its handle back to the free-list; acquiring a buffer pops from the free-list. No payload bytes are copied when a buffer is acquired or released.

### 3.0.2 BufQueue

`BufQueue` is the standard Job-to-Job queue. It is a bounded, pre-allocated, thread-safe queue of buffer handles.

Queue responsibilities:

- Store only buffer handles.
- Allocate all ring cells at construction.
- Support multiple producer and multiple consumer threads.
- Provide non-blocking `try_push()` and `try_pop()` operations.
- When full, `try_push()` returns `false`; callers that want sleep-based backpressure use `push_wait()`, which blocks until a consumer frees space or the queue is closed.
- Provide optional spin/yield helpers for high-performance backpressure loops.
- Expose stats: configured max depth, available slots, current depth, high watermark, push count, pop count.
- Support close/stop signaling separately from buffer ownership.

The target implementation is a lock-free bounded MPMC ring using per-cell sequence numbers. This avoids a mutex on the hot handoff path while preserving bounded memory and deterministic startup allocation. Blocking waits may be layered above it for low-CPU control paths, but data and metadata hot paths should use non-blocking or spin/yield loops.

Queue close never releases buffers. If a queue is closed while it still contains handles, the pipeline owner must drain or explicitly release them according to the command's failure policy.

`ShardedBufQueue` is the standard high-throughput wrapper around multiple
`BufQueue` shards. Producers publish to a preferred shard, normally based on
worker index or lane id. Consumers first pop from their preferred shard, then
steal round-robin from other shards when their shard is empty. This keeps the
balanced fast path mostly contention-free while preventing long-tail stalls when
one producer or directory/file stream leaves a single shard with lots of
remaining buffers. A sharded queue is complete only when it is closed and every
shard is drained; an empty preferred shard alone is never an end-of-stream
signal.

### 3.0.3 Job Interface Contract

A Job implementation may have one or more input queues and one or more output queues. Inputs and outputs are configured at pipeline construction time. The Job implementation must not depend on a concrete upstream or downstream Job.

Required Job behavior:

- Own its worker thread count and internal concurrency settings.
- Pop buffer handles from input queues.
- Interpret buffer content according to the Job's responsibility.
- Modify buffer content only while it owns the handle.
- Push handles to output queues to transfer ownership.
- Release handles to discard work.
- Stop gracefully by ending new input work, draining owned buffers, and releasing or forwarding every owned handle.

Prohibited hot-path behavior:

- Queueing `std::any`, `std::string`, `std::vector`, `FileSpec`, or typed C++ records between Jobs.
- Allocating or freeing payload memory after startup.
- Copying file data between Jobs.
- Combining multiple Job responsibilities in one worker because it is convenient for a command.

Transitional utility types may exist for tests, configuration, command parsing, and backend adapters, but production pipeline edges must be buffer-handle queues.

### 3.0.4 Runtime Monitoring

Runtime monitoring is a helper layer, not part of a specific pipeline's business
logic. Jobs and queues expose small snapshot callbacks to a shared status
registry. A local query listener can render those snapshots on demand for a
separate command-line process.

Monitoring observes, but does not own, pipeline state:

- Job snapshots include running state, worker count, processed record or buffer
  count, byte count, rate, throughput, and job-specific details.
- Queue snapshots include depth, capacity, fullness, high-water mark, push/pop
  counters, and closed state.
- The listener must not allocate or copy file payload buffers. It reads only
  counters and small diagnostic strings.
- The listener must be optional and must not change pipeline topology,
  backpressure behavior, or ownership semantics.
- Monitoring helper code belongs in `piper/src/monitoring/`; pipeline code only
  registers the counters it already owns.

### 3.0.5 Buffer Transport Jobs

Generic buffer transport is modeled as normal Jobs, not as a special writer or
copy path:

```
producer job -> BufQueue -> BufferSenderJob -> transport -> BufferReceiverJob -> BufQueue -> consumer job
```

`BufferSenderJob` consumes raw buffer handles, writes a small frame header plus
the fixed-size raw payload to a stream, then releases the source buffer.
`BufferReceiverJob` listens on a configured endpoint, receives frames, acquires
local destination buffers, copies payload bytes into those buffers, and pushes
handles to its output queue. Payload interpretation remains the responsibility
of the consumer job. Current transports are:

- `tcp://host:port` for loopback or remote hosts.
- `unix:/path/to/socket` for same-host pipelines.

This makes same-host Parquet writer processes and remote NFS copy/sync use the
same conceptual transport boundary. Cross-process transport currently copies
payload bytes through the stream. A future shared-memory queue can replace the
transport implementation without changing producer or consumer Jobs.

Metadata output is also a Job. `MetadataRecordWriterJob` consumes metadata
buffers, decodes file/folder records, writes through the shared metadata writer,
and releases each buffer. Therefore a Parquet writer process can be assembled as:

```
BufferReceiverJob -> MetadataRecordWriterJob(part-N.parquet)
```

Parquet output is finalized through a temporary output file. DuckDB appends to a
temporary `.duckdb.tmp` staging database, commits the staging table, copies it to
`<part>.parquet.tmp`, and only then renames it to `<part>.parquet`. A process
that exits normally therefore exposes only finalized Parquet files; a killed
process may leave recoverable `.duckdb.tmp` state or a temporary Parquet file,
but it should not publish a partial file under the final `.parquet` name.

Metadata batches have two compatible payload shapes:

- **Record batch:** each file or folder record stores its full relative path.
  This is simple and remains useful for generic generated workloads.
- **Folder batch:** a flat folder path and folder metadata are stored once in
  the batch header, and child records store only their file or child-folder
  name plus metadata. A large flat folder may be split into multiple folder
  batches; only the first batch needs to emit the folder record itself.

The folder batch shape is the preferred scanner and future diff transport
format because it minimizes repeated path bytes on the wire and preserves the
natural flat-folder unit used by the metadata reader, checker, and differ.
Decoders reconstruct full relative paths for existing writers so the output
schema remains unchanged.

Live metadata diff uses the same flat-folder unit. The detailed-report path can
read a target folder directly while processing a source batch, but the
high-throughput summary/checker path keeps source and target metadata reading as
separate async jobs. Source workers read flat source folders and enqueue matching
target-folder work plus source batches. Target workers read those folders
independently and emit target batches. A separate sharded joiner/checker job
rendezvous batches by folder path and compares records with `size`, `time`, or
available content-hash semantics. Every folder is assigned to exactly one
checker shard, so source and target batches for that folder meet on the same
worker without a shared global rendezvous map. Source scanning does not wait for
target results; it only waits when a bounded output queue is full, which is
normal pipeline backpressure. This preserves the rule that scanner, remote
checker, and differ are independent jobs with their own parallelism and queues.
Checker worker count, target-request queue depth, and source/target batch queue
depth are job settings under `jobs.checker`; CLI flags may override them for a
single run but command code must not hardcode those operational limits.

The synthetic fake-remote benchmark follows the same independence rule. Fake
remote request receivers dequeue target-folder requests and immediately hand
them to a bounded fake-processor queue, releasing the request-side pipeline.
Separate fake processor workers apply `--remote-delay-us` and later publish the
target batch reply. This models asynchronous request/response latency instead
of making the sender wait inside the request receiver.

For full, untimed diffs, target-only child folders are scanned as target-only
subtrees so the report can include files that exist only on the target side
without building a whole-tree index first. For timer-limited performance runs,
target-only reporting is disabled because a timeout can stop the source reader
mid-directory and make the still-complete target batch look falsely ahead of the
source. This is intentionally checker/differ behavior layered on scanner
batches; scanner, batcher, sender, receiver, and writer jobs remain reusable.

Distributed live diff splits that same shape across hosts. `diff-source` owns
the source metadata reader and sends compact flat-folder batches over TCP.
`diff-target` owns the target metadata reader and compares each received source
folder against the matching target folder. The reply is a folder-level summary
record: timestamps for source scan, target scan, result send/receive, direct
file/folder counts, same/changed/source-only/target-only counts, logical-size
counters, status, and error text. This keeps the return path small and avoids
sending full file paths back for every child. Current implementation sends one
folder summary per frame; the next optimization is batching many summaries into
one frame and splitting extremely large flat folders across multiple source
frames with an end-of-folder marker.

### 3.1 Buffer Types

| Type | Data | Metadata trailer | Total | Pool size | RAM | Purpose |
|---|---|---|---|---|---|---|
| RecBuf | 512 B | — | 512 B | 1,000,000 | 512 MB | File path + stat attributes |
| DataBuf::S | 4 KB | 4 KB | 8 KB | 16,000 | 128 MB | Small files ≤ 4 KB |
| DataBuf::L | 1 MB | 4 KB | 1028 KB | 16,000 | ~16 GB | All files > 4 KB |

Total pre-allocated RAM: approximately 16.6 GB. This provides 16,000 × 1 MB = 16 GB of data pipeline depth, absorbing seconds-scale NFS jitter before the NVMe cache activates. The 128 KB mid-tier buffer is intentionally absent in V1 — the 4 KB / 1 MB split matches the actual file size distribution. Files between 4 KB and 1 MB consume one DataBuf::L slot; the unused portion is never sent on the wire.

### 3.2 DataBuf Trailer Layout

Every DataBuf carries a 4 KB metadata trailer immediately after the data bytes. This makes every buffer fully self-describing. Wire send length = `data_len + sizeof(trailer)`. Unused buffer space beyond `data_len` is never transmitted.

```cpp
struct DataBufTrailer {
    uint64_t  file_id;          // unique transfer ID for this file
    uint64_t  folder_hash;      // parent folder md_hash
    uint64_t  data_offset;      // offset of this chunk within the file
    uint64_t  data_len;         // actual data bytes in this buffer
    uint64_t  file_size;        // total file size
    uint64_t  data_hash;        // xxHash64 of full file (set on LAST_CHUNK)
    uint32_t  chunk_hash;       // xxHash64 of this chunk's data only
    uint32_t  flags;            // LAST_CHUNK | SMALL_FILE | HASH_VALID
    char      rel_path[3552];   // path relative to source root
    uint32_t  slot_valid;       // 0xDEADBEEF when fully written (NVMe cache)
    uint32_t  reserved;
};                              // total: 4096 bytes
```

`rel_path` is relative to the configured source root. The receiver rebases onto its own NFS target root. No separate path-mapping protocol needed. The `file_size` field lets the receiver know when the last chunk has arrived without requiring in-order delivery.

For small-file-heavy transfers, a large DataBuf may carry a packed-small-file
payload instead of one file chunk. The trailer sets `kFlagPackedSmallFiles`,
and the payload starts with a record count followed by repeated entries:
`file_id`, data length, logical file size, data hash, mtime, mode, uid, gid,
relative path length, relative path bytes, and file data bytes. The receiver
unpacks the large buffer, writes each file independently, verifies each file
hash, and sends one normal file ACK per original file id. This reduces wire
frames and per-buffer queue traffic for many tiny files while preserving the
same priority-channel metadata and per-file completion semantics.

The runtime implementation must not wait for a WAN round trip per file.
File records are pushed ahead of data, receiver decisions are collected as a
decision stream, and data ACKs are matched by `file_id` as they arrive. The
sender may have many files in flight; receiver slot exhaustion is flow control,
not an error.

### 3.3 RecBuf Layout

```cpp
struct RecBuf {
    // Identity
    uint64_t  own_hash;         // xxHash64(name + parent_hash)
    uint64_t  parent_hash;      // folder md_hash
    uint64_t  folder_hash;      // same as parent_hash, explicit for clarity
    char      name[128];        // filename only
    char      rel_path[256];    // full relative path (redundant, avoids chain lookup)

    // NFS attributes
    uint64_t  size;
    uint64_t  mtime;            // nanoseconds
    uint32_t  mode;
    uint32_t  uid;
    uint32_t  gid;
    uint32_t  pad;
    uint64_t  inode;            // reserved: hardlink detection V2

    // Integrity
    uint64_t  data_hash;        // xxHash64 of content, 0 = not computed
    uint64_t  hash_ts;          // when data_hash was computed

    // Transfer control
    bool      need_check;
    bool      need_data;
    bool      hash_verified;
    uint64_t  bytes_sent;       // bytes handed to TCP send buffer
    uint64_t  bytes_acked;      // bytes confirmed received

    FileState state;
    uint8_t   pad2[...];        // align to 512 bytes
};
```

---

## 4. Folder and File Records

### 4.1 FolderRecord

```cpp
struct FolderRecord {
    // Identity
    uint64_t  md_hash;          // xxHash64(name + parent_hash)
    uint64_t  parent_hash;      // 0 for root
    char      rel_path[512];

    // Discovery flags
    bool      recursive;        // queue child folders when discovered
    bool      need_check;       // folder-level: run Checker for all files

    // Progress counters (accumulated, not pre-known)
    uint64_t  files_discovered; // grows during READING
    uint64_t  files_total;      // set when READING finishes (FOLDER_READ_COMPLETE msg)
    uint64_t  files_completed;  // files with final ACK (includes skipped)
    uint64_t  files_skipped;    // Checker said skip
    uint64_t  flat_size_bytes;  // accumulated bytes of direct children
    uint64_t  bytes_transferred;// running ACKed bytes
    uint64_t  folder_data_hash; // hash of sorted (file_hash, data_hash) pairs
    uint64_t  last_scan_ts;

    // Receiver-side counters (independent progression)
    uint64_t  files_received;   // file records received on priority channel
    uint64_t  files_written;    // files fully written and closed on target NFS

    // Resume
    uint64_t  reading_offset;   // MetaReader directory position for resume
    uint8_t   priority;         // 0=normal, 255=highest

    // Timestamps
    uint64_t  created_ts;
    uint64_t  started_ts;
    uint64_t  completed_ts;
    uint64_t  state_ts;         // last state transition

    FolderState state;          // sender-side state
    FolderState remote_state;   // receiver-side state (updated via ACKs)
};
```

`folder_data_hash` enables folder-level quick comparison: if the hash matches between source and target scan CSVs, the entire folder is skipped in future transfers without checking individual files.

### 4.2 Folder State Machines

**Sender side:**

| State | Meaning |
|---|---|
| PENDING | In InputProvider queue, not yet assigned to MetaReader |
| READING | MetaReader scanning. Files may already be TRANSFERRING (pipeline overlap) |
| TRANSFERRING | All records read. Data in flight and/or in NVMe cache. Can be long. |
| AWAITING_ACK | All data sent. Waiting for per-file DONE ACKs from receiver. |
| DONE | All file ACKs received. FolderRecord released. |

**Receiver side:**

| State | Meaning |
|---|---|
| PENDING | Folder record received on priority channel. Awaiting file records. |
| RECEIVING | File records and data chunks arriving. Some files may be WRITING. |
| WRITING | All file records received. Data still being written to target NFS. |
| DONE | All files written and closed. Folder ACK sent on priority channel. |

A folder transitions to DONE only when every file in it reaches DONE (including SKIPPED). The sender knows `files_total` only after receiving `MSG_FOLDER_READ_COMPLETE`. Until that message arrives, the folder cannot transition to AWAITING_ACK regardless of how many ACKs have been received.

### 4.3 File State Machines

**Sender side:**

| State | Meaning |
|---|---|
| PENDING | RecBuf slot allocated. Record not yet sent to Checker. |
| CHECKING | Sent to remote Checker agent on priority channel. Awaiting decision. |
| SKIPPED | Checker said skip. Target has identical content. Counts toward files_completed. |
| READING | NfsDataReader has it. Async NFS reads in progress. Some chunks may already be sending. |
| TRANSFERRING | All data read. Chunks in flight on data channel or in NVMe cache. |
| DONE | FILE_ACK received on priority channel. RecBuf slot released. |

**Receiver side:**

| State | Meaning |
|---|---|
| PENDING | File record received on priority channel. DirCache/FileHandleCache lookup pending. |
| CHECKING | Verifying against local NFS (only if folder need_check=true and file need_check=true). |
| RECEIVING | Data chunks arriving on data channel. Some chunks may already be in DataWriter queue. |
| WRITING | All chunks received. DataWriter flushing to target NFS. |
| DONE | Written, closed, attributes set. FILE_ACK sent on priority channel. RecBuf released. |

RecBuf slots are held for the entire lifetime of a file's transfer, including time spent in NVMe cache. The slot is released only on sender receipt of FILE_ACK.

---

## 5. Two-Channel Network Design

All network communication uses two independent TCP connection pools. This is the single most important architectural decision for achieving both 400 Gbit/s bulk throughput and 100K small files/sec simultaneously: a large 1 MB data chunk can never head-of-line block a tiny metadata message.

### 5.1 Priority Channel

1–2 persistent TCP connections. Carries all control traffic. Messages are
small, sent immediately, never queued behind bulk data. The sender must batch
or pipeline metadata decisions and ACK collection; any implementation that
does a blocking request/response per file is not acceptable on WAN links.

| Message | Direction | Size | Purpose |
|---|---|---|---|
| MSG_SESSION_START | S→R | ~256 B | Mode, config params, protocol version |
| MSG_SESSION_END | S→R | ~64 B | Graceful shutdown |
| MSG_FOLDER_RECORD | S→R | ~640 B | FolderRecord, sent before any file records for this folder |
| MSG_FOLDER_READ_COMPLETE | S→R | ~32 B | files_total now known, folder scan finished |
| MSG_FOLDER_ACK | R→S | ~32 B | Receiver: folder DONE, all files written |
| MSG_FILE_RECORD | S→R | ~512 B | FileRecord metadata, sent before any data chunks for this file |
| MSG_FILE_ACK | R→S | ~48 B | File DONE + hash_verified flag + bytes_written |
| MSG_FILE_SKIP | R→S | ~24 B | Checker said skip this file |
| MSG_DIR_MANIFEST | S→R | variable | Directory tree pre-announcement before first file |
| MSG_PAUSE | R→S | ~16 B | Receiver RAM/NVMe pressure: stop sending data |
| MSG_RESUME | R→S | ~16 B | Receiver pressure relieved: resume |
| MSG_HASH_REQUEST | either | ~48 B | Request hash verification of specific file |
| MSG_HASH_RESPONSE | either | ~48 B | Hash result |
| MSG_HEARTBEAT | both | ~16 B | Connection liveness, every 1s |

**Ordering guarantee:** `MSG_FILE_RECORD` for a given file is always sent on the priority channel before any DataBuf for that file is sent on the data channel. This is guaranteed structurally: NfsDataReader only begins reading file data after the FileRecord has been handed to the priority channel sender. The receiver pre-creates the file on receipt of `MSG_FILE_RECORD` and is ready to write any subsequent data chunk regardless of arrival order.

At 100K small files/sec the priority channel carries 100K MSG_FILE_RECORD/sec × 512 B = ~50 MB/s. Well within a single TCP connection budget.

### 5.2 Data Channel

8–16 persistent TCP connections. Carries DataBuf bulk transfers only. Connection count matched to NIC RX/TX hardware queue count so each connection lands on a different core via RSS. Never closed for the lifetime of the session.

The DataSender dispatcher assigns each DataBuf to whichever connection has the most available send buffer space (work-stealing). Files are multiplexed freely across connections. The DataBuf trailer carries all identity information needed for reassembly, so chunk ordering across connections is irrelevant.

### 5.3 V1 Transport: TCP + BBR

Standard TCP with BBR congestion control. At 100 Gbit/s / 60 ms RTT the BDP is 750 MB. With RFC 1323 Window Scale the maximum TCP window is ~1 GB, comfortably covering this. BBR models bandwidth and RTT independently and does not react to loss as a congestion signal, making it far more stable than CUBIC on high-BDP WAN links.

Required kernel tuning (both sender and receiver):

```bash
net.core.rmem_max                 = 1073741824   # 1 GB
net.core.wmem_max                 = 1073741824   # 1 GB
net.ipv4.tcp_rmem                 = 4096 87380 1073741824
net.ipv4.tcp_wmem                 = 4096 87380 1073741824
net.ipv4.tcp_congestion_control   = bbr
net.core.default_qdisc            = fq           # mandatory for BBR pacing
```

`fq` (Fair Queue) qdisc is mandatory alongside BBR. BBR computes the target pacing rate; fq enforces it by spacing individual packet transmissions. Without fq, BBR degenerates to burst behavior.

Each of the 8–16 data connections gets a 1 GB socket buffer (`SO_SNDBUF` / `SO_RCVBUF`). Total kernel buffer memory: up to 16 × 1 GB = 16 GB.

Slow-start: on a fresh connection at 60 ms RTT, reaching 750 MB cwnd takes approximately 960 ms (~16 doublings from 14 KB). Connections are persistent and pre-warmed. This cost is paid once per session, not per file or folder.

### 5.4 V2 Option A: MSG_ZEROCOPY

Eliminates the kernel copy from DataBuf to socket buffer. The kernel pins the DataBuf and DMAs directly from it on send. Completion arrives via the socket error queue. The DataBuf slot must not be released to BufferPool until kernel signals completion.

At 100 Gbit/s this removes ~12.5 GB/s of memory copy bandwidth and meaningfully reduces CPU load. At 400 Gbit/s it removes ~50 GB/s — mandatory at that scale.

Estimated effort: 2–3 days. **Recommended first V2 optimization.**

### 5.5 V2 Option B: DPDK + Custom UDP

Full kernel bypass. DPDK owns the NIC via poll-mode driver. DataBufs are wrapped as external mbufs (`rte_pktmbuf_attach_extbuf`) with zero copy. PMD DMAs directly from DataBuf pool to NIC TX ring.

Requires a custom reliability layer: sequence numbers, SACK, BBR-style pacing, and XOR-based FEC (N data packets + 1 parity per group to recover one loss without retransmit). 2–4 dedicated cores spin at 100% on NIC rings.

Estimated effort: 4–8 weeks. Pursue only if profiling shows V2A insufficient or if 400 Gbit/s single-flow is required.

| Approach | Zero-copy | Kernel bypass | Custom protocol | Effort | Use when |
|---|---|---|---|---|---|
| V1: TCP + BBR | No | No | No | 0 | Baseline, always ship first |
| V2A: TCP + MSG_ZEROCOPY | Yes | No | No | 2–3 days | CPU bound on send copy |
| V2B: DPDK + UDP + FEC | Yes | Yes | SACK+FEC+BBR | 4–8 weeks | Need 400G single flow |

---

## 6. Job Pipeline

### 6.1 Base Job Interface

```cpp
class Job {
public:
    virtual void      start()           = 0;  // spin up threads
    virtual void      stop()            = 0;  // graceful drain + shutdown
    virtual JobStats  stats() const     = 0;  // metrics snapshot
    virtual          ~Job() = default;
};
```

Concrete jobs own their internal thread model and communicate through explicit
input/output `BufQueue` instances. A queue transfers ownership of existing
preallocated `BufferHandle` values; it does not allocate payload memory, copy
payload bytes, or interpret the buffer contents. Backpressure is controlled by
queue capacity: producers either fail fast or wait until consumers release space,
depending on which queue API the job chooses.

`ThreadedJob` owns the common lifecycle for jobs with N equivalent worker
threads: start, stop, wait, the shared stop flag, and the final-worker hook.
Concrete jobs implement only their worker body and queue wake/close hooks.

`BufferProducerJob` builds on `ThreadedJob` for jobs that acquire raw buffers
from one `RawBufferPool` and publish them to one output `BufQueue` or
`ShardedBufQueue`. It owns count limiting, sequence assignment, acquire/yield
retry, output push, stats, and release-on-failed-push. Concrete producers
implement only `fill_buffer()`.

`BufferConsumerJob` builds on `ThreadedJob` for jobs that consume raw buffers
from one input `BufQueue` or `ShardedBufQueue`. It owns pop/wait, steal/drain
after stop/close for sharded inputs, queue wakeup, pool lookup, and
consumed-buffer stats. Concrete consumers implement only `process_buffer()` and
must explicitly move ownership by releasing, forwarding, or retaining the
handle.

`BufferTransformJob` builds on `ThreadedJob` for jobs that consume buffers,
process their payload, and forward the same handle downstream. It owns input
pop/drain, output push, pool lookup, byte stats, and release-on-failed-forward.
Concrete transforms implement only their payload processing.

Jobs may expose extra testing helpers when useful, but the hot path should remain:

```cpp
class BufferJob {
public:
    virtual void      start()           = 0;
    virtual void      stop()            = 0;
    virtual JobStats  stats() const     = 0;  // metrics snapshot
    virtual          ~BufferJob() = default;
};
```

### 6.1.1 Buffer Test Jobs

Two simple raw-buffer jobs exist for pipeline testing and performance calibration:

- **BufferGeneratorJob:** acquires buffers from one `RawBufferPool`, fills them with a configured pattern, and pushes `BufferHandle` values to an output `BufQueue`. Supported patterns are `zero`, `fast_text`, and `xoshiro256`. `xoshiro256` is deterministic, fast, and intended to produce data that general-purpose compressors and dedupe systems should not reduce well when `compression_ratio` is `1.0`. Worker count, count limit, pattern, seed, and target compression ratio are configurable.
- **BufferDiscarderJob:** pops `BufferHandle` values from an input `BufQueue`, looks up each pool through `BufferPoolRegistry`, and releases the buffer. It does not know or care whether the buffer is metadata, data, hash state, or future payload type.
- **NfsDataBufferReaderJob:** reads `FileSpec` work from the current metadata adapter, fills preallocated `DataBuffer` slots through libnfs/local backend reads, and pushes buffer handles downstream. The output side follows the raw-buffer contract. The input side is intentionally marked transitional until metadata records also move through `BufQueue`. The reader has an explicit copy mode: `copy` copies borrowed libnfs callback payloads into owned data buffers for any real downstream consumer, while `no-copy` keeps only length/offset metadata and exists only for reader-to-discarder performance isolation. Small-file packing is supported as an explicit reader-owned fill mode so file bytes are copied only at the external backend boundary, never from one pipeline buffer into another. It remains configuration gated because the first libnfs multi-file async implementation measured slower than the raw one-file-per-buffer path on transfer1.
- **DataHasherJob:** consumes data buffers or generic raw buffers, hashes the payload without copying it, records hot-path stats, and forwards the same buffer handle. For true `DataBuffer` payloads it also stamps a cheap chunk hash marker into the trailer for downstream diagnostics. Packed-small-file buffers are iterated in place and each embedded payload is hashed without repacking. `work_factor` repeats the selected per-buffer hash inside hasher workers for performance proofs; bytes are still counted once and buffer ownership semantics do not change.

The metadata-only scanner path writes no file data and calculates no content
hashes. Its output records include `scan_run_id`, `run_started_at_utc`,
`run_started_unix_ns`, `source_root`, and `run_settings` so multiple scan runs
can be stored in the same queryable Parquet output and separated later by run id.

Observed transfer1 root-NFS scanner runs with 96 metadata threads, async depth
128, and a 10M record window:

| Mode | Duration | Records/s | Files Found | Folders Found | Output |
|---|---:|---:|---:|---:|---|
| Metadata scan + Parquet writer | 600s active, 720s including close | ~214K active, 203K final | 124.2M | 4.44M discovered, 394K finalized/written | 3.1GB Parquet |
| Metadata scan + stats discarder only | 120s active, 158s including drain | 4.5-6.7M active, 3.3M final | 512.8M | 15.3M | none |

The current Parquet path is therefore writer/appender limited, not NFS metadata
reader limited. NVMe utilization during the Parquet run was near zero and write
bandwidth was only tens of MB/s, so the bottleneck is synchronous row
serialization/appender work and the current writer lock, not disk bandwidth. The
next scanner optimization should move metadata output into its own writer job
with preallocated metadata buffers and batched columnar writes.

Synthetic metadata writer benchmarks on transfer1 local NVMe, using generated
file/folder metadata and DuckDB-backed Parquet output, showed:

| Writer Path | Records | Settings | Records/s | Notes |
|---|---:|---|---:|---|
| CSV writer | 1.01M | batch 65,536 | ~1.11M | Generator plus CSV output baseline |
| Parquet appender, scalar values | 5.05M | 32 DuckDB threads, zstd | ~552K | One DuckDB append call per value |
| Parquet data chunks | 5.05M | 32 DuckDB threads, zstd | ~590K | Vectorized chunk append |
| Parquet data chunks | 10.1M | 32 DuckDB threads, zstd | ~604K | Larger run, 45MB output |
| Parquet parts, thread partitions | 16.16M | 32 partitions, 1 DuckDB thread each, zstd | ~6.63M | One process, independent part writers |
| Parquet parts, process partitions | 16.16M | 32 partitions, 1 DuckDB thread each, zstd | ~11.15M | One process per part; 32 files queried as one dataset |
| Transported Parquet parts | 1.01M | parent generator, Unix sockets, 8 writer processes | ~326K | Real sender/receiver/writer pipeline |
| Transported Parquet parts | 1.01M | parent generator, Unix sockets, 32 writer processes | ~222K | More partitions increased per-record IPC overhead |
| Batched transported Parquet parts | 1.01M | 1MB metadata batches, 8 writer processes | ~1.81M | Parent generator, real sender/receiver/writer pipeline |
| Batched transported Parquet parts | 8.08M | 1MB metadata batches, 32 writer processes | ~3.54M | Verified with DuckDB count over all parts |
| Generate/discard | 8.08M | Fixed formatter only, no shard/pack/IPC/writer | ~37.0M | Synthetic path generation ceiling |
| Generate/hash/discard | 8.08M | Fixed formatter + xxHash64 shard hash only | ~23.1M | Replaced byte-at-a-time FNV routing hash |
| Parent pack/discard | 8.08M | Generate, shard, pack 1MB batches, no IPC/writer | ~13.9M | Parent-side generator+packer ceiling after fixed formatter and faster shard hash |
| Transport discard | 8.08M | Parent packer + Unix sender/receiver + child discarders | ~10.2M | Transport adds about 27% over pack-only |
| Batched transported Parquet parts | 8.08M | 1MB metadata batches, 32 writer processes | ~4.72M | Real sender/receiver/writer pipeline after fixed formatter and faster shard hash |
| Direct process Parquet | 8.08M | 32 processes generate and write independent parts | ~10.04M | Avoids single parent generator/packer bottleneck |
| Threaded Parquet | 8.08M | 32 in-process writers | ~2.97M | DuckDB/allocator contention inside one process |

Compression choice was not the dominant cost for the synthetic workload:
zstd, snappy, and uncompressed all measured near the same record rate for 5.05M
records. A CSV staging experiment wrote 5.05M records to CSV at ~1.14M
records/s and converted CSV to Parquet with DuckDB in 7.34s, for a slower total
than direct Parquet. Thread-partitioned Parquet improves throughput but still
appears limited by DuckDB/global allocator contention inside one process.
Process-partitioned output reaches the 10M records/s class by writing a Parquet
dataset directory (`part-*.parquet`) that DuckDB can query with `read_parquet`.
The first transported writer-process prototype was correct but much slower
because it sent one metadata record per socket frame. Packing many records into
1MB metadata-batch buffers improved the real sender/receiver/writer topology to
the multi-million records/s range. The parent process is now the likely limiter
because it performs all generation, path formatting, sharding, batch encoding,
queueing, and IPC writes. The next performance step is either parallel
metadata-batch production/sharding in the parent or shared-memory rings behind
the same `BufferSenderJob`/`BufferReceiverJob` boundary.

Stage-isolation measurements show the current bottleneck more precisely. The
synthetic generator alone can format about 37M records/s after replacing
`snprintf` with a fixed-width decimal formatter. Adding an xxHash64 shard hash
drops the parent path to about 23.1M records/s, and adding 1MB batch packing
drops it to about 13.9M records/s. Unix-socket transport to discarders reduces
that to about 10.2M records/s, and adding Parquet writers reduces the full
transported writer topology to about 4.72M records/s. Independent process
writers can still exceed 10M records/s when generation is distributed across processes, so scanner
topologies that target 10M+ records/s should avoid a single serialized
generator/router and should keep Parquet output partitioned across processes.

Raw buffer transport benchmarks isolate `BufferGeneratorJob -> BufferSenderJob
-> BufferReceiverJob -> BufferDiscarderJob` with 1MB buffers and no metadata
formatting, shard routing, or Parquet writer:

| Transport Path | Data Pattern | Lanes | Payload | GB/s | Gbit/s | Notes |
|---|---|---:|---:|---:|---:|---|
| None, same process | zero | 1 | 8GB | 72.5 | 580.0 | Generator -> queue -> discarder |
| None, same process | zero | 16 | 128GB | 994.3 | 7954.1 | Cache-hot pool reuse; not a DRAM bandwidth measurement |
| None, same process | xoshiro256 | 1 | 8GB | 10.1 | 81.1 | Includes pseudo-random fill cost |
| None, same process | xoshiro256 | 16 | 128GB | 105.8 | 846.8 | Queue handoff remains well above target |
| Unix socket | zero | 1 | 4GB | 2.47 | 19.7 | One sender/receiver connection |
| Unix socket | zero | 8 | 32GB | 22.0 | 175.7 | Scales with independent lanes |
| Unix socket | zero | 16 | 128GB | 35.0 | 280.1 | Longer repeat, 1 thread per job per lane |
| Unix socket | xoshiro256 | 16 | 128GB | 34.9 | 279.3 | Pseudo-data generation included |
| TCP loopback | zero | 16 | 64GB | 29.4 | 235.6 | Same job topology over TCP loopback |

The receiver no longer clears full-size destination buffers before reading a
full-size frame; it only clears the unused tail for partial frames. This avoids
an extra 1MB memory write per raw data buffer while keeping partial metadata
frames deterministic. The same-process `none` transport mode shows that the
generic buffer pool, queue, generator, and discarder are not limiting the raw
pipeline. The observed drop in Unix/TCP modes is the stream transport boundary:
kernel socket copies, scheduler handoff, and sender/receiver syscall overhead.

For the final transport topology, generation is independent from sending:

```
BufferGeneratorJob(32 workers) -> one BufQueue -> N BufferSenderJob sockets
```

With a fixed 128GB payload and 32 total xoshiro generator workers, transfer1
measured:

| Unix Sockets | Generator Workers | GB/s | Gbit/s | Notes |
|---:|---:|---:|---:|---|
| 1 | 32 | 2.94 | 23.6 | One socket is the bottleneck |
| 2 | 32 | 5.60 | 44.8 | Near 2x |
| 8 | 32 | 16.4 | 131.5 | Sender/socket scaling |
| 16 | 32 | 33.3 | 266.6 | Crosses 200G target |
| 32 | 32 | 37.4 | 299.3 | Best in this sweep |
| 64 | 32 | 26.9 | 214.9 | Shared-queue/scheduling contention visible |
| 128 | 32 | 29.9 | 239.1 | More sockets do not improve reliably |

This confirms that generator workers should feed a shared queue and sender
count should be tuned independently. For this host and implementation, 16-32
Unix sender sockets is the useful range for a 32-worker xoshiro producer.

TCP loopback with the same shared-input topology is slightly slower and noisier
than Unix sockets, but also reaches the 200G class:

| TCP Sockets | Generator Workers | GB/s | Gbit/s | Notes |
|---:|---:|---:|---:|---|
| 1 | 32 | 2.66 | 21.3 | One TCP stream |
| 2 | 32 | 4.64 | 37.1 | Scaling, still stream-bound |
| 8 | 32 | 13.4 | 107.3 | Below target |
| 16 | 32 | 24.6 | 197.1 | Longer repeat, near target |
| 24 | 32 | 32.2 | 257.2 | Deeper pool repeat |
| 32 | 32 | 36.1 | 288.5 | Deeper pool repeat |
| 64 | 32 | 24.3 | 194.7 | Too many sockets, scheduling overhead |

For local TCP transport on this host, 24-32 sockets is the useful range; 16 is
borderline, and 64 regresses.

Shared MPMC queue fan-in/fan-out is a real scaling limit. A stress test with
one pool and one queue, `BufferGeneratorJob(32 workers) -> BufQueue ->
BufferDiscarderJob(N workers)`, measured:

| Pattern | Discarder Workers | GB/s | Gbit/s | Notes |
|---|---:|---:|---:|---|
| zero | 1 | 181.4 | 1451.3 | One consumer, little dequeue contention |
| zero | 64 | 111.1 | 889.2 | More consumers made it slower |
| zero | 128 | 130.4 | 1043.1 | Still below one-consumer result |
| xoshiro256 | 1 | 179.1 | 1432.7 | Producer work plus one consumer |
| xoshiro256 | 64 | 88.5 | 707.6 | Shared queue/free-list contention |
| xoshiro256 | 128 | 118.9 | 951.2 | Partial recovery, still slower |

Increasing both sides on a single shared queue does not remove the bottleneck:

| Pattern | Generator Workers | Discarder Workers | GB/s | Gbit/s |
|---|---:|---:|---:|---:|
| zero | 32 | 128 | 135.2 | 1082.0 |
| zero | 64 | 128 | 136.4 | 1091.2 |
| zero | 128 | 128 | 141.9 | 1135.5 |
| xoshiro256 | 32 | 128 | 122.6 | 980.7 |
| xoshiro256 | 64 | 128 | 121.3 | 970.2 |
| xoshiro256 | 128 | 128 | 127.6 | 1021.1 |

The same total shape sharded across 32 independent queues/pools
(`32 x generator workers total, 64 x discard workers total`) measured:

| Pattern | Queues | Generator Workers | Discarder Workers | GB/s | Gbit/s |
|---|---:|---:|---:|---:|---:|
| zero | 32 | 32 | 64 | 1040.6 | 8324.8 |
| xoshiro256 | 32 | 32 | 64 | 215.4 | 1723.1 |

Therefore, the hot production pipeline should avoid a single shared MPMC queue
when many producers and many consumers are active. Use `ShardedBufQueue` per
sender/socket/worker group. Consumers keep affinity to their preferred shard
first and steal from other shards only when idle, so the design removes most
cache-line contention without stranding work in an imbalanced shard.

Direct shared-vs-sharded sender tests with 32 total xoshiro generator workers
and 64GB payload showed:

| Transport | Sockets | Shared Queue Gbit/s | Sharded Queues Gbit/s | Result |
|---|---:|---:|---:|---|
| Unix | 8 | 135.3 | 131.9 | Similar |
| Unix | 16 | 260.9 | 192.2 | Shared better in this run |
| Unix | 32 | 289.0 | 453.0 | Sharded much better |
| TCP | 8 | 106.6 | 117.0 | Sharded slightly better |
| TCP | 16 | 208.3 | 197.3 | Similar |
| TCP | 32 | 258.9 | 318.2 | Sharded better |

The practical interpretation is not "always shard everything". For moderate
socket counts, transport/syscall cost can dominate and shared-queue overhead is
acceptable. At higher socket counts, sharding removes enough queue contention to
matter. For internal communication on this host, 16 Unix sockets with shared
input is already above target, while 32 sockets benefits clearly from sharding.

TCP sender/receiver worker count per socket also has a small optimum. With
32 total xoshiro generator workers and 64GB payload:

| Topology | TCP Sockets | Sender/Receiver Workers per Socket | Gbit/s |
|---|---:|---:|---:|
| shared input | 16 | 1 | 207.5 |
| shared input | 16 | 2 | 276.6 |
| shared input | 16 | 4 | 188.3 |
| shared input | 32 | 1 | 254.1 |
| shared input | 32 | 2 | 193.0 |
| shared input | 32 | 4 | 188.7 |
| sharded | 16 | 1 | 230.1 |
| sharded | 16 | 2 | 287.3 |
| sharded | 16 | 4 | 265.3 |
| sharded | 32 | 1 | 312.7 |
| sharded | 32 | 2 | 329.9 |
| sharded | 32 | 4 | 210.3 |

For TCP on this host, 2 sender/receiver workers per socket can help, especially
with sharded queues, but 4 workers per socket usually adds too much scheduling
and queue contention.

The xoshiro256 generator scales with more independent generator lanes until it
approaches CPU/memory-system limits. On transfer1, same-process runs with 1MB
buffers and one generator plus one discarder thread per lane measured:

| Lanes | GB/s | Gbit/s | Notes |
|---:|---:|---:|---|
| 1 | 10.1 | 80.8 | Single xoshiro fill worker |
| 2 | 20.0 | 159.7 | Near-linear |
| 8 | 53.2 | 425.7 | Still scaling |
| 16 | 109.5 | 875.9 | Still scaling |
| 32 | 212.6 | 1700.7 | Still scaling |
| 64 | 424.6 | 3397.0 | Longer repeat |
| 96 | 540.0 | 4320.2 | Longer repeat |
| 128 | 646.8 | 5174.2 | Longer repeat |
| 160 | 756.3 | 6050.3 | Best longer repeat in this sweep |

Oversubscribed tests above the 160 hardware-thread count did not improve
reliably and were sensitive to pool size/cache effects, so 128-160 independent
generator lanes is the useful range for this host.

Observed one-thread `DataHasherJob` throughput on Apple M4 Pro with an
optimized build, 1 MiB zero-filled buffers, and the real
`BufferGeneratorJob -> DataHasherJob -> BufferDiscarderJob` path:

| Algorithm | GB/s | GiB/s | Gbit/s |
|---|---:|---:|---:|
| `xxh64` | 23.4 | 21.8 | 187.5 |
| `xxh3_64` | 16.8 | 15.7 | 134.7 |
| `sha256` | 2.33 | 2.17 | 18.6 |
| `md5` | 0.90 | 0.84 | 7.2 |

Normal performance and pipeline tests can place the generator at the beginning and the discarder at the end to measure queue, buffer, and intermediate-job behavior without external I/O.

Observed remote libnfs read plus hash measurements on transfer1
(`ice1-transfer-001`, 160 CPUs), reading
`nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
with 1 MiB data buffers:

| Pipeline | Data Threads | Outstanding Reads | Hash Threads | Hash | Work Factor | Gbit/s | CPU | Notes |
|---|---:|---:|---:|---|---:|---:|---:|---|
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 32 | 16 | 32 | `xxh3_64` | 1 | 81-110 | n/a | Lower concurrency; queues stayed empty. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 64 | 16 | 32 | `xxh3_64` | 1 | 152-168 | n/a | Scales with reader concurrency. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 64 | 32 | 32 | `xxh3_64` | 1 | ~180 | n/a | Best earlier hashed pipeline run before independent hasher tuning. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 96 | 32 | 48 | `xxh3_64` | 1 | ~180 | n/a | No meaningful gain over 64 x 32. |
| `NfsDataBufferReaderJob -> BufferDiscarderJob`, copy mode | 64 | 32 | n/a | n/a | n/a | 190.2 | 1812% | Current reader-only owned-buffer baseline. |
| `NfsDataBufferReaderJob -> BufferDiscarderJob`, copy mode | 64 | 32 | n/a | n/a | n/a | ~192 | n/a | Earlier reader-only run. |
| `NfsDataBufferReaderJob -> BufferDiscarderJob`, no-copy mode | 64 | 32 | n/a | n/a | n/a | ~192 | n/a | Same pipeline and read scheduling, but skips copying borrowed libnfs callback payloads into owned slots. |
| `NfsDataBufferReaderJob -> BufferDiscarderJob`, no-copy mode | 96 | 32 | n/a | n/a | n/a | ~192 | n/a | No gain from extra reader threads. |
| `NfsDataBufferReaderJob -> BufferDiscarderJob`, no-copy mode | 64 | 64 | n/a | n/a | n/a | ~189 | n/a | More per-file async depth did not help. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 64 | 32 | 128 | `xxh64` | 1 | 191.6 | 2377% | Read and hash throughput matched; queues stayed near empty. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 64 | 32 | 128 | `xxh64` | 4 | 192.0 | 3055% | CPU rose while read throughput stayed at the reader ceiling. |
| `NfsDataBufferReaderJob -> DataHasherJob -> BufferDiscarderJob` | 64 | 32 | 128 | `xxh64` | 16 | 181.6 | 5136% | Very heavy synthetic hash work started competing for host CPU/scheduling; read and hash rates still matched and queues stayed bounded. |
| Legacy libnfs discard read, no owned buffers/hash | 64 | 32 | n/a | n/a | n/a | ~191 | n/a | Old comparison path; does not exercise buffer pipeline ownership handoff. |

The reader-only pipeline is close to the NIC target but has not consistently
exceeded 200 Gbit/s. Copy and no-copy runs are close enough that the borrowed
libnfs callback payload copy is not the dominant limiter. Data queues remain
near empty, and increasing reader threads or per-file async depth did not
improve the ceiling, which points at libnfs/NFS server/NIC path behavior rather
than downstream buffer ownership transfer. Adding an independent hasher with
enough workers and moderate synthetic CPU work did not reduce read throughput:
the `xxh64` work-factor-4 run matched the reader-only ceiling while consuming
more CPU. At excessive synthetic work, the shared host CPU/scheduler can still
reduce the reader's achieved rate even though the queue handoff remains healthy.

Additional 2026-05-15 reader baselines after the transfer1 rebuild:

| Workload | Pipeline | Meta Threads | Data Threads | Outstanding Reads | Files | Bytes | Elapsed | Gbit/s | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Root mixed/large-file-biased | `NfsDataBufferReaderJob -> BufferDiscarderJob` | 64 | 32 | 16 | 15,614 read / 81,560 found | 1.87TB read | 120.1s | 124.7 | Early folders contained large files; data reader busy, output queue full, downstream discard not limiting. |
| Small-file flat folder | `NfsDataBufferReaderJob -> BufferDiscarderJob` | 1 | 64 | 1 | 50,000 | 1.36GB | 3.18s | 3.41 | Folder `catbear/run_20260218_042836/talking-head`, average file size ~27KiB. |

Small-file thread scaling on that folder:

| Data Threads | Files/s | Gbit/s |
|---:|---:|---:|
| 32 | 9.9K | 2.15 |
| 64 | 15.7K | 3.41 |
| 96 | 15.9K | 3.45 |
| 128 | 14.8K | 3.21 |
| 192 | 7.2K | 1.56 |
| 256 | 7.2K | 1.55 |

Conclusion: small-file data read is dominated by per-file open/read/close
sequencing. Per-file async read depth helps large files but not single-read
small files. A configurable packed-small-file mode now keeps multiple small
files open/read/close in flight per data-reader worker and fills pre-reserved
owned `DataBuffer` entries directly from libnfs callbacks. It is not the default
yet: the 2026-05-15 transfer1 test showed the raw one-file-per-buffer path still
winning on the 50,000-file `talking-head` folder. Treat packed mode as a
correctness-complete experimental path until its scheduling overhead is tuned.

#### Buffer Generator Compression Ratio

`compression_ratio` is expressed as original size divided by compressed size. A
ratio of `1.4` means the user expects roughly `7 / 1.4 = 5` compressed bytes for
7 original bytes. The generator approximates this by filling about
`1 / compression_ratio` of each buffer with high-entropy generated data and the
remaining bytes with highly compressible repeated bytes. Exact compression
depends on the compressor, block size, headers, and dataset size, so this is a
workload-shaping knob rather than a guarantee.

Observed one-thread throughput on Apple M4 Pro with an optimized build, 1 MiB
buffers, and the real `BufferGeneratorJob -> BufQueue -> BufferDiscarderJob`
path:

| Pattern | Compression Ratio | GB/s | GiB/s | Gbit/s |
|---|---:|---:|---:|---:|
| `zero` | n/a | 181.7 | 169.3 | 1454.0 |
| `fast_text` | 1.0 | 3.40 | 3.16 | 27.2 |
| `xoshiro256` | 1.0 | 11.0 | 10.3 | 88.2 |

### 6.2 Sender Side

#### InputProvider

Owns the in-memory queue of folder work items. No NFS, no network.

- In-memory queue: up to 1M FolderRecord entries.
- Thread 1: reads `input.csv` (128 entries per batch) and `input_overflow.csv` (drain overflow first). Remembers byte offset in each file for exact resume on restart.
- Refill trigger: queue drops below 100 items → read 128 more from disk immediately.
- Receives recursive child folder discoveries from NfsMetaReader. If queue full: appends to `input_overflow.csv`.
- Exposes `pull(FolderRecord&)` to NfsMetaReaders.

#### NfsMetaReader (N instances)

Each instance processes one flat folder at a time. Multiple instances run in parallel.

- Pulls one FolderRecord from InputProvider. Processes it fully before pulling the next.
- Uses libnfs async readdir to list all entries with full stat attributes.
- Fills RecBuf slots: one slot per file record (name, rel_path, size, mtime, mode, uid, gid, inode).
- **Extra-large folders (> 1M files):** streaming mode. As RecBuf slots are consumed by Checker, MetaReader continues reading from `reading_offset`. Files may be TRANSFERRING before folder finishes READING.
- On discovering child directories with recursive flag: injects back to InputProvider. If full: writes to `input_overflow.csv`.
- When folder scan completes: sends `MSG_FOLDER_READ_COMPLETE` on priority channel with final `files_total`.
- Exposes `pull(RecBuf&)` to Checkers.

#### Checker (N instances, one per MetaReader)

Determines which files need transfer. Two comparison strategies in priority order:

1. **Fast path:** scan CSV comparison (no NFS calls). If `folder_data_hash` matches → skip entire folder. If per-file `own_hash + size + mtime + data_hash` matches → SKIPPED.
2. **Slow path:** send batch of RecBufs to remote Checker agent via priority channel. Remote agent uses sync libnfs to stat target files. Responds with skip/transfer per file.

Skip condition: target file exists with identical size and mtime (or identical `data_hash` if in scan CSV).

- Folder-level `need_check = false`: all files bypass Checker and go directly to READING.
- Sends `MSG_FILE_RECORD` for each transfer file on priority channel before handing RecBuf to DataReader.
- Exposes `pull(RecBuf&)` to NfsDataReader.

#### NfsDataReader

Reads file data as fast as possible from source NFS. The primary bottleneck at high file counts is outstanding async request depth.

- Pulls RecBufs from Checkers. Selects DataBuf::S for size ≤ 4 KB, DataBuf::L for everything larger.
- Default **256 outstanding async requests** per instance. Key tuning parameter.
- Large files (multi-chunk): issues 4–8 outstanding async reads per file simultaneously.
- Computes xxHash64 streaming as data arrives. Finalizes on last chunk. Writes to RecBuf `data_hash` and DataBuf trailer `data_hash` with `HASH_VALID` flag.
- Backpressure: stops pulling from Checker when DataBuf::L pool usage ≥ 85%. Resumes at ≤ 70%.
- Exposes `pull(DataBuf&)` to DataSender and DataCacher.

#### DataCacher (sender side)

Decouples NfsDataReader from DataSender. Spills DataBuf slots to NVMe when RAM is under pressure. This enables the 21-minute backlog and allows the reader to run at full NFS speed regardless of WAN congestion or target NFS jitter.

- **V1:** writes DataBuf slots as fixed-size files to XFS on RAID0 NVMe using `O_DIRECT`. Filename = zero-padded `entry_id`. Directory tree: `/cache/XX/XX/entry_id` (two hex levels, ≤256 entries per dir).
- **V2:** 7 independent raw ring buffers on NVMe block devices, striped by `entry_id % 7`, written via io_uring with 256-deep submission queues per drive. One dedicated thread per drive.
- Spill trigger: DataBuf::L usage ≥ 85%. Drain trigger: ≤ 70%.
- Oldest-first drain via monotonic `entry_id`. Important for target NFS sequential access patterns.
- File manifest: in-memory map of `file_id → {total_chunks, chunks_cached}`.
- RecBuf slots are **not** cached. Only DataBuf slots are evicted. RecBuf stays in RAM for entire file lifetime.

#### DataSender

Pulls DataBuf slots and ships them to the remote DataReceiver at maximum speed.

- 8–16 persistent TCP connections on data channel. Work-stealing dispatcher: assigns each DataBuf to the connection with most available send buffer space.
- Wire format per DataBuf: `[16B frame header: seq64, data_len32, flags32] + [data bytes] + [4KB trailer]`. Single `writev()` call.
- **V1:** standard `send()` with 1 GB `SO_SNDBUF` per connection.
- **V2A:** `setsockopt(SO_ZEROCOPY)`. `MSG_ZEROCOPY` on every `sendmsg()`. Poll error queue for completion before releasing DataBuf slot.
- Sends `MSG_DIR_MANIFEST` on priority channel at session start: complete directory tree. Receiver DirCache pre-creates all directories before any file data arrives.

### 6.3 Receiver Side

#### DataReceiver

Receives data from the network into pre-allocated DataBuf slots. Never processes data content.

- Accepts 8–16 persistent TCP connections from sender data channel.
- Pre-posts DataBuf::L and DataBuf::S slots as receive buffers.
- If pool usage < 50%: pulls additional DataBuf slots back from receiver DataCacher.
- Exposes `pull(DataBuf&)` to DataWriter and DataCacher.

#### DataCacher (receiver side)

Same implementation as sender-side DataCacher. Absorbs receiver-side bursts when DataWriter cannot keep up.

- Spill trigger: DataReceiver DataBuf usage ≥ 90%.
- Drain trigger: DataWriter pulls from here when DataReceiver is below 70%.

#### DataWriter (N instances)

Pulls DataBuf slots and writes file data to target NFS.

- Uses libnfs async pwrite-equivalent at `data_offset` from trailer. Handles out-of-order chunk arrival naturally.
- **DirCache:** concurrent hash set of directory paths confirmed to exist on target. Pre-populated by `MSG_DIR_MANIFEST`. On cache miss: synchronous mkdir chain, then add to cache.
- **FileHandleCache:** concurrent map of `rel_path → open libnfs fh`. On first chunk: create/open file, cache handle. LRU eviction at 10,000 handles. On last chunk (`LAST_CHUNK` flag): flush, set mtime + mode + uid + gid from RecBuf, close, evict.
- **File completion:** file manifest `file_id → {total_chunks, chunks_written}`. Atomically incremented per chunk. When complete: close sequence begins.
- **Hash verification:** on file close, compare written data xxHash64 against trailer `data_hash`. On mismatch: mark FAILED, send negative FILE_ACK. Sender re-queues file.
- On success: sends `MSG_FILE_ACK` on priority channel with `hash_verified=true`.

#### ScanWriter (SCAN mode only)

Replaces DataSender in SCAN mode. Pulls DataBufs from NfsDataReader, extracts finalised hash, appends one CSV row per completed file, releases DataBuf immediately. No network activity.

---

## 7. Large Folder Handling and Sliding Window

Folders with more than 1M files cannot be held entirely in RecBuf memory simultaneously. HyperSync uses a sliding window: file records are released as soon as their transfer is ACKed, and new records pulled in to fill freed slots.

### 7.1 Window Mechanics

- NfsMetaReader maintains `reading_offset` in the folder's directory listing. Reads file records up to RecBuf pool high-water mark.
- When `FILE_ACK` arrives, sender releases that RecBuf slot. NfsMetaReader continues reading from `reading_offset`.
- Folder stays in READING state until `reading_offset` reaches end-of-directory. Files may be simultaneously in PENDING, CHECKING, READING, TRANSFERRING, and DONE.
- Folder transitions to TRANSFERRING only when: (1) `reading_offset == end-of-directory`, AND (2) all discovered files have been handed to Checker.
- `MSG_FOLDER_READ_COMPLETE` sent when MetaReader finishes directory scan. Carries `files_total`. Receiver needs this to know when no more file records are coming.

### 7.2 Memory Usage for Large Folders

For a 10M file folder with 1M RecBuf slots: at any moment ~1M files are in active pipeline state. The other 9M are still in the NFS directory, not yet read. Memory usage is flat at 1M RecBuf slots (~512 MB) throughout.

### 7.3 Receiver Large Folder Handling

Receiver mirrors this: operates in streaming mode, does not wait for all file records before starting to write data. Files go PENDING → RECEIVING → WRITING independently. Receiver folder state stays in RECEIVING until `files_total` file records have been processed (known after `MSG_FOLDER_READ_COMPLETE` arrives).

---

## 8. NVMe Cache

With 16 TB available, the system can absorb up to 21 minutes of full-rate intake before the cache is exhausted, completely insulating NfsDataReader from any target-side slowdowns.

### 8.1 Watermark Scheme

| Level | Threshold | Action |
|---|---|---|
| RAM (DataBuf::L pool) | < 70% | NfsDataReader pulls freely from Checkers |
| RAM | 70%–85% | NfsDataReader reduces pull rate (soft throttle) |
| RAM | > 85% | DataCacher begins evicting DataBufs to NVMe |
| RAM | > 95% | NfsDataReader stops pulling entirely (hard stop) |
| NVMe cache | < 20% | DataCacher may prefetch back to RAM ahead of DataWriter |
| NVMe cache | 20%–80% | Normal: drain at DataWriter speed |
| NVMe cache | > 80% | JobScheduler signals DataReader threads to throttle |
| NVMe cache | > 95% | Hard backpressure: NfsDataReader pauses |

RAM watermarks respond sub-second. NVMe watermarks operate on a minutes timescale. In the common case (reader and writer roughly matched) the NVMe cache is never touched.

### 8.2 V1: XFS on RAID0

- 7 NVMe drives → RAID0 via mdadm/LVM → single XFS volume mounted at `/cache`.
- `mkfs.xfs -f -d su=256k,sw=7 /dev/md0`
- Mount options: `noatime,nodiratime,nobarrier,logbufs=8`
- `O_DIRECT` on all cache file I/O (bypass page cache, must not compete with DataBuf pool).
- File layout: `/cache/XX/XX/entry_id_hex`. Two hex-char directory levels, ≤256 entries per directory.
- Each cache file is exactly 1028 KB (1 MB data + 4 KB trailer). Fixed size, zero fragmentation.
- DataCacher maintains `write_cursor` and `read_cursor`. Drain order: oldest-first.
- Aggregate write bandwidth: 7 × 7 GB/s = 49 GB/s. Intake at 100 Gbit/s: 12.5 GB/s. Headroom: 4×.

### 8.3 V2: Raw Ring Buffers on NVMe

One io_uring ring buffer per drive. Stripe by `entry_id % 7`.

```
Drive header at offset 0 (4 KB, O_SYNC):
    magic, drive_index, capacity_entries, write_head, read_head, sequence_base

Slots at offset:  4KB + N × 1028KB
```

```cpp
// Write
slot   = write_head % capacity_entries;
offset = sizeof(DriveHeader) + slot * SLOT_SIZE;
io_uring_prep_write(sqe, fd, databuf, SLOT_SIZE, offset);
write_head++;

// Drain
slot   = read_head % capacity_entries;
offset = sizeof(DriveHeader) + slot * SLOT_SIZE;
io_uring_prep_read(sqe, fd, databuf, SLOT_SIZE, offset);
read_head++;
```

Crash recovery: forward scan from `read_head` checking `slot_valid` and `chunk_hash` in trailer. Identifies true `write_head` in under 1 second even on 16 TB (only 4 KB trailers read, not 1 MB data).

| Property | V1: XFS/RAID0 | V2: Raw ring buffers |
|---|---|---|
| Write amplification | ~2× (XFS journal) | 1× |
| Peak aggregate write BW | ~35 GB/s | ~49 GB/s |
| Drive failure impact | Lose all 16 TB | Lose 1/7 (~2.3 TB), rest intact |
| Crash recovery | XFS journal replay | Trailer scan, < 1 second |
| Debugging | Standard tools | Custom tool required |
| Page cache interference | Eliminated via O_DIRECT | None |
| Implementation effort | Low (2–3 days) | Medium (~1000 lines) |

---

## 9. Hash and Integrity

xxHash64 is used throughout. Faster than CRC32 at large block sizes on modern CPUs (20–30 GB/s single-core on AVX2), 64-bit digest, no patent encumbrance. CPU cost is negligible relative to NFS and network I/O.

### 9.1 Hash Computation Points

- **NfsDataReader:** reads source NFS data into DataBuf slots and transfers slot ownership downstream. It does not calculate file content hashes.
- **Hasher:** consumes DataBuf slots, reorders per-file chunks by offset when needed, updates the selected per-file hash state, finalizes on end-of-file, and emits hash metadata. Standard full-file hashes must process bytes in exact file-offset order to match Linux tools.
- **DataWriter:** streaming xxHash64 as data is written to target NFS. Finalized on last chunk. Compared against trailer `data_hash` before sending `FILE_ACK`.
- **ScanWriter (SCAN mode):** writes finalized hash metadata produced by the Hasher.

### 9.2 Verification Flow

- On hash mismatch at DataWriter: file marked FAILED. Negative FILE_ACK sent on priority channel. Sender re-queues for retransfer. Up to 3 retry attempts before marking ERROR.
- `--skip-verify` flag: DataWriter skips hash comparison. For maximum throughput on trusted private LAN.
- Hash stored in scan CSV after every successful transfer. Incremental transfers use stored hash for fast comparison before live NFS stat.

### 9.3 Folder-Level Hash

`folder_data_hash` = xxHash64 of lexicographically sorted concatenation of `(own_hash || data_hash)` for all files in the folder. Computed when folder transitions to DONE. If source and target `folder_data_hash` match in their respective scan CSVs, the entire folder is skipped in future transfers.

### 9.4 Future Hash Capabilities (V2)

- **Integrity audit mode:** re-scan source and target independently, compare scan CSVs, report divergence. No data transfer.
- **Resume after crash:** scan CSV shows which files were fully ACKed. Restart skips those files.
- **Hardlink detection:** two FileRecords with same inode and same `data_hash` → HARDLINK message instead of data. `inode` field reserved in RecBuf.
- **Binary scan index:** replace CSV with flat binary format for O(1) lookup on 10M+ file trees.

---

## 10. Job Scheduler

The JobScheduler is a lightweight supervisor operating outside the data hot path.

- Reads TOML/JSON config at startup and on `SIGHUP` for live parameter tuning without restart: thread counts, watermark thresholds, buffer pool sizes, NVMe cache path, TCP connection counts, async read depth.
- Creates Jobs in dependency order. Calls `start()` in sequence. On shutdown: `stop()` in reverse order, waits for clean drain.
- Polls `stats()` from each Job every second. Writes to ring-buffer telemetry sink.
- Monitors NVMe cache fill level. Signals coarse throttle to NfsDataReader when NVMe > 80%.
- Handles `MSG_PAUSE` / `MSG_RESUME` from receiver priority channel. Forwards to DataSender dispatcher immediately.

---

## 11. Performance Critical Paths

### 11.1 NfsDataReader: Async Read Depth

The single most impactful tuning parameter. At 12.5 GB/s intake and 256 KB reads with 100 µs NFS RTT:

```
12.5 GB/s / (0.0001 s × 256 KB) = ~488 outstanding requests needed
```

Default 256 slots is conservative. For 100K small files/sec at 10 µs NFS RTT: 1 outstanding request in theory, but 256 slots absorbs all server-side jitter.

At 400 Gbit/s on LAN: 4–8 NfsDataReader instances × 256 slots = 1024–2048 outstanding reads total.

### 11.2 Small File Throughput: 100K Files/Sec

The bottleneck chain for tiny files:

- **NfsDataReader:** 100K async reads/sec. 256 slots / 10 µs RTT = 25.6M calls/sec theoretical. Not the bottleneck.
- **Priority channel:** 100K × 512 B = 50 MB/s. Not the bottleneck.
- **DataBuf::S pool:** 100K atomic CAS operations/sec for slot acquire/release. Negligible.
- **DataWriter:** 100K NFS creates + writes + closes/sec on target. This is the hard limit. Distribute across N DataWriter instances and N NFS mounts. Large distributed NFS clusters handle 100K–500K ops/sec.
- **FileHandleCache:** 100K lookups/sec with 10K handles. Use `tbb::concurrent_hash_map`. Lock contention at this rate is minimal.

### 11.3 Memory Copy Budget at 400 Gbit/s

At 400 Gbit/s = 50 GB/s, a memcpy from DataBuf to kernel socket buffer consumes 50 GB/s of memory bandwidth. DDR5 provides ~200–400 GB/s, so V1 is feasible but tight on LAN. V2A (MSG_ZEROCOPY) is mandatory for sustained 400 Gbit/s.

At 100 Gbit/s WAN: 12.5 GB/s copy = ~6% of DDR5 bandwidth. Manageable in V1.

### 11.4 CPU Pinning and NUMA

- Pin NIC interrupts and DPDK poll threads to cores local to NIC's NUMA node.
- Pin NfsDataReader threads to cores local to NFS client NIC's NUMA node.
- Pin DataCacher io_uring threads to cores local to NVMe drives' PCIe root complex.
- Use `SO_INCOMING_CPU` or RFS (Receive Flow Steering) to ensure TCP receive processing happens on the same core that owns the receiving DataBuf slot.

### 11.5 Transfer1 Network Runtime Profile

For transfer1-class source workers, the network profile is part of the
performance contract. If a VM is rebooted or replaced, reapply and verify this
profile before trusting scan/read benchmarks. The helper script is
`hypersync/deploy/tune-transfer1-network.sh`.

The current validated profile for `ens3` on transfer1 is:

| Setting | Required value | Reason |
|---|---:|---|
| MTU | `9000` | Reduces packet rate for 1 MiB NFS READ replies. |
| RX/TX ring | `8192/8192` | Avoids small driver ring bottlenecks at high packet rates. |
| RX coalescing | `adaptive-rx off`, `rx-usecs 12` | Predictable interrupt pacing. |
| RPS CPUs | NUMA node0 mask `00000000,00000000,0000ffff,ffffffff,ffffffff` | Keeps receive work near the NIC. |
| XPS CPUs | same NUMA node0 mask | Keeps transmit queue selection near the NIC. |
| `rx-*/rps_flow_cnt` | `32768` per RX queue | Enables RFS flow tracking at high connection counts. |
| `net.core.rps_sock_flow_entries` | `262144` | Global RFS flow table for 11 RX queues. |
| TCPMSS mangle rules | `0` | Jumbo frames must not be clamped to 1460-byte MSS on NFS paths. |
| `sunrpc.tcp_max_slot_table_entries` | `65536` | Matches high-concurrency NFS client profile. |
| `sunrpc.tcp_slot_table_entries` | `65536` | Matches high-concurrency NFS client profile. |
| NFS BDI read-ahead | `16384 KiB` | Keeps kernel-mounted NFS scan fallback aligned with high-throughput profile. |

Hypersync data-reader benchmark commands that target the 200 Gbit/s class should
also pin the process to NIC-local CPUs:

```bash
taskset -c 0-79 ./hypersync benchmark-data \
  --data-reader-threads 64 \
  --data-outstanding-requests 2
```

The node0-only RPS/XPS profile is intentionally different from the older
all-host rsync profile. On this libnfs workload it benchmarked faster than
all-CPU RPS/XPS, presumably because it avoids cross-NUMA traffic while still
spreading packet processing beyond the 11 hardware queues.

---

## 12. Implementation Phases

### Phase 1 — Core Pipeline

Goal: end-to-end transfer working correctly. Single folder, single MetaReader+Checker pair, no NVMe cache.

- BufferPool with RecBuf, DataBuf::S, DataBuf::L
- Job base class, JobScheduler skeleton
- InputProvider reading input.csv
- NfsMetaReader, single instance, sync libnfs initially
- Checker with remote agent, live NFS stat only
- NfsDataReader, async libnfs, 64 outstanding slots initially
- Two-channel TCP established: priority + data connections
- DataSender: 8 persistent connections, BBR, 1 GB socket buffers
- DataReceiver mirroring DataSender
- DataWriter: DirCache, FileHandleCache, pwrite by offset
- FILE_ACK and FOLDER_ACK on priority channel
- No DataCacher — pure in-memory pipeline

### Writer Job Contract

The target side is represented as explicit pipeline jobs, symmetric with the
source-side readers:

- `MetaWriter-<backend>` consumes flat-folder metadata buffers and creates or
  applies directory metadata on the target backend.
- `DataWriter-<backend>` consumes data buffers and writes file payloads on the
  target backend.
- Backend suffixes are visible in pipeline/status names: `NFS`, `SYN`, and `FS`.
- Multi-threaded data writes must use a sharded data queue keyed by file id.
  This preserves the invariant that all chunks for one large file are written by
  one writer lane/context, avoiding cross-lane truncation or open-handle races.
- Packed small-file buffers are written as independent whole files by
  `DataWriter-<backend>`; no extra unpacking job is required.
- Safe copy shape:
  `[MetaReader-NFS]->(FileQueue)->[DataReader-NFS]->(DataBufQueue-sharded-by-file)->[DataWriter-NFS]`
  with a parallel metadata stream:
  `[MetaReader-NFS]->(MetadataBufQueue)->[MetaWriter-NFS]`.

Small/large writer prioritization should reuse the same bucket policy as the
reader side: small-file writers optimize file completion rate, large-file writers
consume remaining bandwidth, and the controller shifts capacity according to ETA
tension while preserving per-file queue affinity.

### Use-Case Pipeline Profiles

Canonical pipeline shapes and important knobs are recorded in
`config/pipelines.yaml`. That file is the shared memory for use-case topology:

- `scan`: filesystem metadata scan, summary reporting, and optional DB/parquet
  persistence.
- `diff`: source/target comparison, optionally using recorded indexes and
  content hashes.
- `profile`: compact workload capture with phases, folder topology, size
  distributions, and optional sampled data-read latency.
- `generator`: synthetic/profile-based filesystem creation. The current
  small-file generator hot path is:
  `[FolderSeeder/MetaWork-1]->(FolderQueue)->[MetaReader-SYN-96]->(FolderReadyQueue-4096)->[FolderCreation-NFS-8]->(ReadyFileQueue-500000)->[DataReader-SYN-768/direct-submit]->[DataWriter-NFS/reactors=64 window=64]`.
- `copy`: source-to-target copy/sync/merge. This is still under active
  implementation and must reuse the same `MetaReader`, `DataReader`,
  `MetaWriter`, and `DataWriter` job contracts.

Hashing is a per-use-case option, not a separate mode that bypasses the
pipeline. When enabled, it is inserted as a normal job or backend operation in
the configured topology.

### Phase 2 — NVMe Cache

Goal: decouple reader and writer. Enable transfers larger than RAM.

- DataCacher V1: XFS on RAID0, O_DIRECT, entry_id-based filenames
- RAM watermark integration
- NVMe watermark reporting to JobScheduler
- File manifest for completion tracking
- MSG_PAUSE / MSG_RESUME on priority channel

### Phase 3 — Parallelism, Hash, Modes

Goal: multiple parallel folders, full hash pipeline, scan CSV, all three modes.

- N NfsMetaReader + N Checker instances in parallel
- Shared RecBuf pool across all instances
- xxHash64 streaming in NfsDataReader and DataWriter
- Hash verification before FILE_ACK
- Scan CSV read/write on both sides
- Folder-level hash for fast incremental skip
- SCAN mode and DRY-RUN mode
- Large folder sliding window (streaming MetaReader)

### Phase 4 — Profile and Optimize

| Metric | Bottleneck signal | V2 fix | Effort |
|---|---|---|---|
| TCP send CPU % | High on DataSender core | MSG_ZEROCOPY (V2A) | 2–3 days |
| NVMe write amplification | iostat shows 2× expected | Raw ring buffers (V2 cache) | 1–2 weeks |
| Single TCP flow throughput | < line rate at 400G | DPDK + UDP + FEC (V2B) | 4–8 weeks |
| NfsDataReader wait % | Async slots exhausted | Increase async_read_depth | Hours |
| DataWriter NFS ops/sec | < 100K/sec small files | More DataWriter instances | 1–2 days |
| NUMA memory latency | perf c2c shows cross-socket | CPU pinning + RFS | 1–2 days |

---

## 13. Open Items

| # | Item | Impact | Phase |
|---|---|---|---|
| 1 | Hardlink detection and preservation | Correctness for repos/source trees | V2 |
| 2 | Small file packing (N files per DataBuf::S) | Throughput if > 100K files/sec needed | V2 |
| 3 | Scan CSV → binary index format | Startup time on 10M+ file trees | V2 |
| 4 | Folder transfer priority scheduling | Policy for ordering work | Phase 3 |
| 5 | Partial resume after crash mid-folder | Correctness after unexpected shutdown | Phase 3 |
| 6 | Overwrite-in-place vs temp-rename-swap | Atomicity on target during live volume | Phase 1 decision |
| 7 | Jumbo frames (MTU 9000) availability | 6× fewer packets, lower interrupt load | Infrastructure |
| 8 | NFS protocol version and async support | libnfs async feature availability | Phase 1 validation |
| 9 | full_size accumulation and reporting | Accurate progress reporting | Phase 3 |
| 10 | Error handling and retry policy | Robustness for production use | Phase 2 |
