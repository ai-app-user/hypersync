# WSync Requirements

This document tracks the product requirements and current completion state for WSync. It describes what the application must do, not how it is implemented.

## Product Goal

WSync is a high-performance NFS tool suite for scanning, hashing, generating, comparing, measuring, copying, and synchronizing large NFS file trees. It must work with local or remote NFS endpoints and eventually support running coordinated work across many source and target servers in parallel.

## Core Requirements

### NFS Scanner

Status: Partial

The scanner must walk an NFS tree recursively and record discovered files and folders.

Requirements:
- Accept an NFS URL or local path as the source.
- Scan recursively by default, with an option for non-recursive operation.
- Record files, folders, or both.
- Record common metadata available during scan, including path, type, size, modified time, mode, owner, and group.
- Record folder-level metadata, including direct child file count and direct child logical file size.
- Write output in text, CSV, and Parquet formats.
- Include scan run metadata with output records so multiple runs can be kept in
  one queryable output table.
- Support graceful stop by elapsed-time limit.
- Print periodic progress and throughput stats.

Current state:
- Metadata scanning is implemented.
- Text and CSV output are implemented.
- Parquet output is implemented when the build includes DuckDB support.
- File and folder output selection is implemented.
- Scanner output records include a run id, UTC start time, source root, and
  command/settings JSON when using the shared metadata record writer.
- Periodic stats and graceful timer stop are implemented.
- Direct libnfs scanning is implemented.

Remaining:
- Apply partitioned Parquet writer throughput work to scanner output so
  metadata output does not bottleneck high-speed metadata scans.
- Define stable output schema versioning.
- Add documentation for all scanner output columns.
- Add resume/incremental scan behavior if required.

### NFS Hasher

Status: Partial

The hasher must walk an NFS tree and calculate data hashes for files.

Requirements:
- Accept an NFS URL or local path as the source.
- Support recursive and non-recursive hashing.
- Calculate file content hashes.
- Support a configurable hash algorithm, including standard Linux-verifiable hashes and faster inventory hashes.
- Support independent block-hash mode for faster parallel hashing of large files, with ordered block hashes recorded as file metadata.
- Optionally reuse scanner metadata output as input.
- Write hash inventory output in text, CSV, and Parquet formats.
- Include hash metadata on file records.
- Preserve folder-level direct child file count and direct child logical file size in hash inventory output.
- Only finalize a folder inventory record after the flat folder scan has discovered all direct child files.
- Support graceful stop by elapsed-time limit.
- Print periodic progress and throughput stats.
- Support skipping files that already have valid cached hashes.

Current state:
- Hash inventory is implemented as a first-class CLI command.
- Local paths and NFS URLs are accepted as sources.
- Recursive and non-recursive operation are supported.
- SHA-256 output is implemented as lowercase hex compatible with `sha256sum`.
- MD5 output is implemented as lowercase hex compatible with `md5sum`.
- MD5 and SHA-256 use runtime OpenSSL acceleration when `libcrypto` is available, with pure C++ fallback when it is not.
- XXH64, XXH3-64, and XXH3-128 output are implemented for faster non-cryptographic hashing.
- Text, CSV, and Parquet output use the shared metadata record writer.
- File records include hash algorithm and content hash columns for full-file mode.
- File records include block size, block count, block hash algorithm, and ordered block hash columns for block mode.
- Folder records include finalized direct child file count and direct child logical size.
- Metadata, data-reader, read-ahead, and timer settings are configurable from the CLI.
- File data is streamed through the hash calculation instead of requiring whole-file buffering.
- File data passed between pipeline stages moves by preallocated buffer-slot ownership transfer, not by copying payload bytes into new queue messages or allocating per-chunk payload strings.
- Borrowed backend data is copied once into an owned preallocated slot when asynchronous downstream processing needs payload bytes.
- Data reading and hash calculation run as separate pipeline stages: the reader emits data-buffer records and the hasher owns hash state, ordering, and finalization.
- Hash worker threads and hash queue depth are independently configurable.
- A first-pass threaded data hasher job exists that consumes preallocated buffer handles, hashes payload bytes without copying, forwards the same handles downstream, and records per-job throughput stats.
- A data-read plus hash benchmark pipeline exists for testing NFS data reader, data hasher, and discard stages together with independently configurable reader and hash concurrency.
- The data-read plus hash benchmark supports cooperative timer stop: metadata stops accepting new work, data readers stop queueing new async reads, already in-flight reads are drained safely, and final stats are printed.
- Full-file MD5, SHA-256, XXH64, XXH3-64, and XXH3-128 hashes are calculated by feeding file bytes to the algorithm in exact file-offset order.
- Block mode emits independent block hashes and does not claim that the joined block-hash list equals a standard whole-file hash.

Remaining:
- Define hash cache format and invalidation rules.
- Support scanner metadata output as input.
- Add periodic hash progress stats.

### NFS Generator

Status: Not Started

The generator must create synthetic NFS file trees for correctness and performance testing.

Requirements:
- Generate configurable directory depth and breadth.
- Generate configurable file counts and file size distributions.
- Generate deterministic datasets from a seed.
- Generate sparse or full-content files where supported.
- Optionally generate metadata patterns such as permissions, ownership, and timestamps.
- Print generation progress and final summary.

Remaining:
- Define generator configuration schema.
- Define built-in profiles for metadata-heavy, small-file, large-file, and mixed workloads.

### NFS Performance Tool

Status: Partial

The performance tool must measure NFS performance in different modes.

Requirements:
- Measure metadata scan throughput.
- Measure data read throughput without writing output data.
- Measure raw hash throughput per active worker/core independent of NFS and output writers.
- Support configurable metadata concurrency.
- Support configurable data reader concurrency.
- Support configurable hash algorithm, worker count, block size, duration, and minimum Gbit/s per core threshold for hash speed checks.
- Support bounded read-ahead so metadata scan does not hide data-reader bottlenecks.
- Print periodic throughput stats.
- Support graceful stop by elapsed-time limit.
- Report final counts, bytes, throughput, concurrency settings, and failures.

Current state:
- Metadata scan benchmark is implemented.
- Synthetic metadata writer benchmark is implemented for measuring output
  writer throughput independently of NFS scanning.
- Data read benchmark is implemented.
- Data read benchmark uses the raw-buffer pipeline `NFS reader -> discarder`.
- Data read benchmark can run in `copy` mode or explicit `no-copy` isolation mode to measure libnfs payload-copy cost; `no-copy` is valid only when the next stage discards buffers.
- Data read plus hash benchmark is implemented for the pipeline `NFS reader -> data hasher -> discarder`.
- Data read plus hash benchmark supports a hasher work factor so the hasher can burn extra CPU in its own thread pool while still counting each data buffer once.
- Hash speed benchmark is implemented for MD5, SHA-256, XXH64, XXH3-64, and XXH3-128 with per-core throughput reporting and optional pass/fail threshold.
- Metadata and data benchmark timers are implemented.
- Periodic stats are implemented.
- Raw buffer generation test jobs exist for pipeline calibration with `zero`, `fast_text`, and `xoshiro256` data patterns.
- Raw buffer generation supports an approximate `compression_ratio` workload target expressed as original size divided by compressed size.
- Metadata writer benchmark can generate deterministic file and folder records
  and write them through the shared metadata writer in text, CSV, or Parquet
  format.
- Metadata writer benchmark supports partitioned output. Thread partitions
  write multiple parts from one process, and process partitions write one
  Parquet part per process for higher DuckDB throughput.

Remaining:
- Add target write-path performance benchmark.
- Add mixed metadata plus data workload profiles.
- Add multi-host coordinated performance runs.
- Add machine-readable benchmark output.

### NFS Diff

Status: Partial

The diff tool must compare two NFS folders recursively, possibly on different NFS servers.

Requirements:
- Accept source and target NFS URLs or local paths.
- Compare file and folder metadata.
- Optionally compare file data hashes.
- Report files and folders that are missing, new, changed, or equal.
- Support large trees without requiring all file data to be loaded at once.
- Support output in text, CSV, and Parquet formats.
- Support graceful stop by elapsed-time limit.

Current state:
- Local scan index and dry-run comparison behavior exist.
- Basic scan CSV loading and comparison behavior exist.

Remaining:
- Make NFS-to-NFS recursive diff a first-class command.
- Add metadata-only and data-hash diff modes.
- Add resumable diff for very large trees.
- Add diff summaries by folder and change type.

### NFS Sync and Copy

Status: Partial

The sync/copy tool must copy or synchronize files between two NFS trees, possibly on different remote NFS servers.

Requirements:
- Accept source and target NFS URLs or local paths.
- Copy new files.
- Update changed files.
- Preserve file metadata where permitted.
- Preserve folder metadata where permitted.
- Support metadata-only skip decisions.
- Support optional data-hash verification.
- Support retry and failure reporting.
- Support large-file streaming.
- Support graceful stop and resumable operation.
- Eventually support running across many source and target servers in parallel.

Current state:
- Send and receive commands exist.
- NFS data reading exists.
- Target writing exists for local and NFS paths.
- Basic transfer, retry, skip, and verification behavior exist in tests.

Remaining:
- Define final copy vs sync semantics.
- Add first-class NFS-to-NFS copy command.
- Add first-class NFS-to-NFS sync command.
- Add delete handling policy for sync.
- Add resume manifest.
- Add multi-host orchestration.

## Cross-Cutting Requirements

### NFS Access

Status: Partial

Requirements:
- Use direct libnfs access for NFS URLs.
- Allow an NFS URL server component to name multiple equivalent source IPs,
  such as `nfs://172.27.255.2-172.27.255.17/export/path`; each libnfs
  connection may choose one endpoint independently.
- Support local filesystem paths for tests and local operation.
- Report whether direct async NFS support is active.
- Handle inaccessible files without aborting long-running inventory or benchmark jobs unless strict mode is requested.

### Runtime Monitoring

Status: Partial

Requirements:
- Long-running commands should optionally expose a query endpoint that can be
  inspected from another command-line process while the run is active.
- Status output must be human readable.
- Each observable job should report whether it is running, worker count,
  processed buffer or record count, processing rate, byte count, and GB/s.
- Each observable queue should report current depth, configured capacity,
  fullness percentage, high-water mark when available, push/pop counts when
  available, and closed/open state.
- Monitoring must be implemented as helper infrastructure so it can be reused
  by scanner, hasher, performance, diff, copy, and sync pipelines.

Current state:
- A local status socket can be enabled for metadata scan, data-read benchmark,
  and data-read plus hash benchmark runs.
- A `status` CLI command can query a running status socket and print the latest
  job and queue snapshot.
- Raw `BufQueue` depth, capacity, high-water mark, push count, and pop count are
  exposed in monitoring output.

Remaining:
- Extend runtime monitoring to final copy/sync/diff commands.
- Add machine-readable status output if automation needs it.
- Track high-water marks for non-`BufQueue` work queues.

Current state:
- Direct libnfs source support is implemented.
- Local fallback support is implemented.
- Data benchmark skips failed file reads and counts failures.

Remaining:
- Define strict vs best-effort error policies across all tools.
- Add consistent error output schema.

### Output Formats

Status: Partial

Requirements:
- Human-readable text output.
- CSV output.
- Parquet output.
- Machine-readable command summaries.
- Stable schemas for scan, hash, diff, benchmark, and transfer output.

Current state:
- Metadata text, CSV, and Parquet output exist.
- Hash inventory text, CSV, and Parquet output exist.
- Benchmark summary output exists as key-value text.

Remaining:
- Define schema versions.
- Add JSON or NDJSON summaries if needed.
- Document every output column and field.

### Portable Deployment

Status: Partial

Requirements:
- Hypersync must support a Linux deployment bundle that can be copied to a
  server and run without installing DuckDB, libnfs, or other optional runtime
  libraries globally.
- The bundle must be a simple flat folder, not a nested `bin/`, `lib/`,
  `config/`, and `doc/` tree.
- The bundle must include the launcher script, compiled executable, required
  staged shared libraries, default config, a README, a manifest, and checksums
  in that one folder.
- The README must name the expected runtime library families directly; do not
  create a separate runtime-library listing file for that.
- The launcher must set the runtime library path relative to itself.
- The packaging process must be repeatable from the repository, not manual.
- The package verification step must run the staged executable from the bundle
  before reporting success.
- There must be a one-command installer script for new servers that downloads,
  verifies, unpacks, and smoke-tests the bundle.

Current state:
- `hypersync/deploy/package-linux.sh` builds or stages a Linux release binary,
  copies selected runtime libraries reported by `ldd`, writes a manifest and
  checksums, verifies `--version`, and optionally creates a `.tar.gz` archive.
- `hypersync/deploy/package-linux.sh` produces a flat bundle with
  `hypersync`, `hypersync.bin`, `default.yaml`, copied `.so` files,
  `README.txt`, `manifest.txt`, and `checksums.sha256`.
- `hypersync/deploy/run-hypersync` is the relocatable runtime launcher copied
  to `hypersync` in the bundle.
- `hypersync/deploy/install-hypersync.sh` is the one-command new-server
  installer.

Remaining:
- Add CI or release automation that produces signed Linux artifacts.
- Decide whether production release bundles should include all resolved dynamic
  libraries or only optional non-system libraries.

### Configuration

Status: Partial

Requirements:
- All tools must be configurable by command-line arguments.
- All tools must be configurable by YAML config file.
- Command-line arguments must override config file values.
- Defaults must be safe for small tests and easy to scale for performance runs.
- Concurrency and read-ahead controls must be explicit.

Current state:
- Default YAML config exists.
- Command-specific CLI options exist for current tools.

Remaining:
- Normalize command naming and option naming across all tools.
- Add config examples for scanner, hasher, perf, diff, copy, and sync.

### Multi-Server Operation

Status: Not Started

Requirements:
- Support many source servers in one run.
- Support many target servers in one run.
- Support partitioning work across workers.
- Support coordinated progress and failure reporting.
- Support local-only and distributed execution modes.

Remaining:
- Define multi-server topology model.
- Define worker discovery and deployment model.
- Define aggregate reporting.

### Reliability and Safety

Status: Partial

Requirements:
- Long-running jobs must support graceful termination by timer.
- Long-running jobs must report partial progress.
- Copy and sync operations must avoid destructive behavior unless explicitly requested.
- Failures must be counted and reported.
- Tool behavior must be testable locally without NFS.

Current state:
- Timer-based graceful stop exists for metadata and data benchmarks.
- Timer-based graceful stop exists for hash inventory.
- The data-read-plus-hash benchmark supports an independent hasher work factor
  for proving that downstream CPU work does not change reader byte accounting
  or buffer ownership semantics.
- Local tests cover many existing behaviors.
- File read failures are counted in data benchmark.
- File hash failures are counted in hash inventory.

Remaining:
- Add resume semantics for scan, hash, diff, copy, and sync.
- Add strict and best-effort modes.
- Add dry-run mode for copy and sync.

## Current Commands

Implemented commands:
- `scan`
- `dry-run`
- `send`
- `receive`
- `benchmark-meta`
- `benchmark-data`
- `benchmark-data-hash`
- `benchmark-hash`
- `hash`
- `--version`

Future command groups:
- `nfs scan`
- `nfs hash`
- `nfs generate`
- `nfs perf`
- `nfs diff`
- `nfs copy`
- `nfs sync`
