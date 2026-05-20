# WSync Performance Baselines

This document records observed performance for independent jobs and common job
pipelines. These numbers are expectations for engineering work, not portable CI
thresholds. Automated performance tests use small smoke workloads and only check
that benchmark commands run and report sane metrics.

## Release 0.0.4 Summary

Release `0.0.4` promotes the saved-profile mixed generator replay result from
commit `16e81ad`. On agnopo, the profile-backed integrated mixed writer ran for
`60s`, wrote `4,134,954` files and `1.479 TB`, and completed with zero failures.
The full-run average was `189.47 Gbit/s`; the final sample reached
`193.61 Gbit/s` with small files at `61,767 files/s`, medium at `31.50 Gbit/s`,
and large at `131.16 Gbit/s`.

The run improved over time but did not reach `200 Gbit/s` in the integrated
mode. The governor stayed capped at `bulk_pacing_us=2000` and the small queue
remained mostly empty, so the remaining gap is most likely integrated
supply/orchestration rather than raw small-file writer capacity.

## Test Host

Current remote baseline host:

| Host | CPU | Hardware Threads | NUMA | Notes |
|---|---|---:|---:|---|
| transfer1 `216.86.174.100` | AMD EPYC 9454 | 160 | 2 nodes | Local NVMe workspace, loopback TCP/Unix tests |

NUMA topology:

| Node | CPUs |
|---|---|
| node0 | 0-79 |
| node1 | 80-159 |

NUMA distance is 10 local and 20 remote. NUMA pinning is not a default yet;
sharded queues are the default high-throughput strategy.

## Latest Transfer1 Baseline

Recorded on `ice1-transfer-001` at `2026-05-13T04:41:04Z`.

Build label:

```text
transfer1-baseline-d0414af-duckdb-libnfs
```

Artifacts on transfer1:

```text
/mnt/local-nvme/hypersync-baseline-20260513T043441Z/job-performance-report.txt
/mnt/local-nvme/hypersync-baseline-20260513T043441Z/job-performance-report.json
```

The report used 1MiB buffers, 512 pool slots, 128 buffers per lane, DuckDB
enabled, direct libnfs enabled, and uncompressed Parquet writer output. This is
the current regression checkpoint before folder batching and diff/checker
pipeline work.

Note: after this baseline, scanner partitioned metadata output was changed to
use folder-scoped metadata batches. The old baseline remains the pre-change
comparison point; new job-performance runs should include both `pack-discard`
and `folder-pack-discard` metadata writer modes.

## Transfer1 To Nopo1 Runtime Checks

Recorded on `2026-05-13` with transfer1 `216.86.174.100` and nopo1
`160.211.77.39`, using local NVMe on both hosts unless noted.

Raw network baseline with `iperf3 -P 16`:

| Direction | Sender Gbit/s | Receiver Gbit/s |
|---|---:|---:|
| transfer1 -> nopo1 | 196 | 179 |
| nopo1 -> transfer1 | 177 | 160 |

Hypersync runtime sync after batched metadata decisions, pipelined ACK
collection, 128KiB small-file packing, and blocking receiver slot acquire:

| Workload | Runtime Shape | Files | Bytes | Elapsed | Gbit/s | Notes |
|---|---|---:|---:|---:|---:|---|
| 128KiB files | single sync | 50,000 | 6.55GB | 6.03s | 8.70 | one sender/receiver pair |
| 128KiB files | 50 sharded flat-folder syncs | 50,000 | 6.55GB | 1.30s | 40.25 | parallel process lanes |
| 128KiB files | 50 sharded flat-folder syncs | 200,000 | 26.21GB | 6.48s | 32.35 | target file creation dominates |
| 1MiB files | single sync | 16,000 | 16.78GB | 17.38s | 7.72 | one sender/receiver pair |
| 1MiB files | 50 sharded flat-folder syncs | 16,000 | 16.78GB | 2.09s | 64.12 | exceeds 50Gbit/s target |

Real NFS source check from transfer1 to nopo1 local NVMe:

```text
source=nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/HaWoR/video/path/0/175593000
files_total=506 transferred=506 failed=0 bytes=9564146 chunks_sent=501 elapsed=2.91s
scan-to-scan diff after sync: same=506 changed=0 new=0 target_only=0
```

Conclusion: cross-site transport is not the current limiter. The single runtime
sync path still needs internal multi-lane readers/writers to reach 50Gbit/s on
small-file-heavy trees. External sharding by flat folder already proves the
jobs and network can exceed 50Gbit/s for 1MiB-class small files.

### Raw Buffer Pipeline

```text
BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob
```

| Threads | GB/s | Gbit/s |
|---:|---:|---:|
| 1 | 9.54 | 76.33 |
| 2 | 14.58 | 116.63 |
| 8 | 57.16 | 457.29 |
| 16 | 98.00 | 783.98 |
| 32 | 207.70 | 1661.59 |
| 64 | 277.31 | 2218.49 |
| 96 | 360.02 | 2880.13 |
| 128 | 397.39 | 3179.13 |

### Transport Pipeline

```text
BufferGeneratorJob -> BufferSenderJob -> transport -> BufferReceiverJob -> BufferDiscarderJob
```

| Transport | Endpoints | GB/s | Gbit/s |
|---|---:|---:|---:|
| unix | 1 | 4.75 | 37.97 |
| unix | 8 | 17.20 | 137.59 |
| unix | 16 | 24.60 | 196.77 |
| unix | 32 | 28.58 | 228.65 |
| unix | 64 | 23.95 | 191.63 |
| tcp | 1 | 2.45 | 19.61 |
| tcp | 8 | 12.37 | 98.92 |
| tcp | 16 | 22.28 | 178.20 |
| tcp | 32 | 26.05 | 208.43 |
| tcp | 64 | 26.96 | 215.67 |

### Hash Speed

| Algorithm | Threads | GB/s | Gbit/s |
|---|---:|---:|---:|
| xxh64 | 1 | 14.89 | 119.12 |
| xxh64 | 32 | 473.76 | 3790.07 |
| xxh3_64 | 1 | 28.04 | 224.32 |
| xxh3_64 | 32 | 822.80 | 6582.36 |
| sha256 | 1 | 1.84 | 14.73 |
| sha256 | 32 | 58.20 | 465.62 |
| md5 | 1 | 0.77 | 6.20 |
| md5 | 32 | 24.51 | 196.06 |

### Metadata Writer

Synthetic workload: `200,000` files and `2,000` folders.

| Format | Partition Mode | Partitions | Records/s |
|---|---|---:|---:|
| csv | threads | 1 | 1.10M |
| csv | threads | 8 | 6.08M |
| csv | threads | 32 | 8.69M |
| csv | processes | 1 | 1.12M |
| csv | processes | 8 | 6.70M |
| csv | processes | 32 | 12.60M |
| parquet | threads | 1 | 396K |
| parquet | threads | 8 | 800K |
| parquet | threads | 32 | 330K |
| parquet | processes | 1 | 400K |
| parquet | processes | 8 | 1.29M |
| parquet | processes | 32 | 1.36M |

## Basic Jobs

### BufferGeneratorJob

The generator creates fixed-size raw data buffers from a preallocated pool.

Observed same-process xoshiro256 scaling using:

```text
BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob
```

with one generator and one discarder worker per lane, 1MiB buffers:

| Lanes | GB/s | Gbit/s |
|---:|---:|---:|
| 1 | 10.1 | 80.8 |
| 2 | 20.0 | 159.7 |
| 8 | 53.2 | 425.7 |
| 16 | 109.5 | 875.9 |
| 32 | 212.6 | 1700.7 |
| 64 | 424.6 | 3397.0 |
| 96 | 540.0 | 4320.2 |
| 128 | 646.8 | 5174.2 |
| 160 | 756.3 | 6050.3 |

### BufferDiscarderJob

The discarder releases owned buffers back to their pool. It is primarily used
to terminate performance pipelines without downstream work.

One shared queue does not scale well with many consumers:

```text
BufferGeneratorJob[N workers] -> one BufQueue -> BufferDiscarderJob[64 workers]
```

| Generator Workers | Discarder Workers | Pattern | GB/s | Gbit/s |
|---:|---:|---|---:|---:|
| 1 | 64 | xoshiro256 | 9.84 | 78.7 |
| 2 | 64 | xoshiro256 | 19.80 | 158.4 |
| 8 | 64 | xoshiro256 | 73.06 | 584.5 |
| 16 | 64 | xoshiro256 | 96.87 | 775.0 |
| 32 | 64 | xoshiro256 | 90.76 | 726.1 |
| 64 | 64 | xoshiro256 | 99.91 | 799.3 |
| 96 | 64 | xoshiro256 | 104.48 | 835.9 |
| 128 | 64 | xoshiro256 | 109.13 | 873.1 |
| 160 | 64 | xoshiro256 | 112.98 | 903.8 |

Expected design conclusion: hot data paths should use sharded queues by
default. Single shared queues are acceptable for low-rate control paths and
small tests.

## Queue Topology

Sharded queues outperform one shared MPMC queue when many producer and consumer
workers are active.

| Pattern | Queue Topology | Generator Workers | Discarder Workers | GB/s | Gbit/s |
|---|---|---:|---:|---:|---:|
| zero | one shared queue | 32 | 64 | 111.0 | 888 |
| zero | 32 sharded queues | 32 | 64 | 1040.6 | 8324.8 |
| xoshiro256 | one shared queue | 32 | 64 | 90.5 | 724 |
| xoshiro256 | 32 sharded queues | 32 | 64 | 215.4 | 1723.1 |

Default recommendation:

```text
producer workers -> queue shard[N] -> consumer/socket/hash/writer shard[N]
```

## Buffer Transport Jobs

Transport requires both sides of the pipeline:

```text
BufferGeneratorJob -> BufferSenderJob -> transport -> BufferReceiverJob -> BufferDiscarderJob
```

For internal communication, use one stream per endpoint unless a benchmark
explicitly proves a need for more.

### Unix Sockets

Sharded queues, one stream per endpoint, xoshiro256, 1MiB buffers:

| Endpoints | Streams | GB/s | Gbit/s |
|---:|---:|---:|---:|
| 1 | 1 | 2.95 | 23.6 |
| 8 | 8 | 15.72 | 125.7 |
| 16 | 16 | 24.11 | 192.9 |
| 64 | 64 | 79.66 | 637.3 |
| 128 | 128 | 76.52 | 612.2 |

Recommended starting point for internal Unix transport: 64 endpoints, one
stream each.

### TCP Loopback

Sharded queues, one stream per endpoint, xoshiro256, 1MiB buffers:

| Endpoints | Streams | GB/s | Gbit/s |
|---:|---:|---:|---:|
| 1 | 1 | 2.67 | 21.4 |
| 8 | 8 | 15.40 | 123.2 |
| 16 | 16 | 28.14 | 225.1 |
| 64 | 64 | 47.61 | 380.9 |
| 128 | 128 | 54.71 | 437.7 |

Recommended starting point for local TCP transport: 16-32 endpoints for 200G
class performance, more endpoints when extra headroom is useful.

## Hash Jobs

Observed one-thread `DataHasherJob` throughput on Apple M4 Pro with an
optimized build, 1MiB zero-filled buffers:

| Algorithm | GB/s | GiB/s | Gbit/s |
|---|---:|---:|---:|
| `xxh64` | 23.4 | 21.8 | 187.5 |
| `xxh3_64` | 16.8 | 15.7 | 134.7 |
| `sha256` | 2.33 | 2.17 | 18.6 |
| `md5` | 0.90 | 0.84 | 7.2 |

Expected default hash for performance work is a fast non-cryptographic hash
such as `xxh3_64`, unless the workflow requires Linux-tool-compatible `sha256`
or `md5` verification.

## Metadata Writer

Synthetic metadata writer benchmarks on transfer1 local NVMe.

Latest CSV writer retest:

| Format | Partition Mode | Partitions | Records/s | Notes |
|---|---|---:|---:|---|
| CSV | threads | 1 | 1.22M | Single process, one writer thread |
| CSV | threads | 32 | 8.88M | Single process, 32 writer threads |
| CSV | processes | 1 | 1.18M | One writer process |
| CSV | processes | 32 | 24.9M | 32 independent part writer processes |

Parquet/DuckDB rows were included in the latest report, but transfer1 was
built without DuckDB support (`HYPERSYNC_HAS_DUCKDB=0`), so those rows failed
and should not be treated as parquet performance data.

Earlier synthetic parquet experiments on transfer1:

| Path | Records | Records/s | Notes |
|---|---:|---:|---|
| Direct process Parquet | 8.08M | ~10.04M | Distributed generation and one writer process per part |
| Transported Parquet parts | 8.08M | ~4.72M | Parent generator, 1MiB metadata batches, writer processes |
| Transport discard | 8.08M | ~10.2M | No Parquet writer |
| Parent pack/discard | 8.08M | ~13.9M | Generate, shard, pack only |

Expected design conclusion: Parquet writer/decode remains the limiter in the
transported metadata pipeline, not raw local transport.

## Real NFS Metadata Scan

Transfer1 real libnfs scan source:

```text
nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
```

Reader-only scan can reach the expected class of performance on warm runs:

| Pipeline | Threads | Async Depth | Output | Observed Live Rate |
|---|---:|---:|---|---:|
| NFS metadata reader -> stats discard | 64 | 256 | discard | 5.42M records/s |

Real scan plus DuckDB/parquet is still writer limited. Partitioned parquet
output is supported with `--metadata-output-partitions <n>` and
`--metadata-output-partition-mode processes`; it writes a directory of parquet
part files. Best observed active scan/write rate so far:

| Compression | Partitions | Reader Threads | Async Depth | Active Rate | End-to-End Rate | Output |
|---|---:|---:|---:|---:|---:|---|
| zstd | 64 | 64 | 256 | 3.67M records/s | 586K records/s | 64 parquet parts |
| uncompressed | 128 | 64 | 256 | 4.26M records/s | 1.09M records/s | 128 parquet parts |
| uncompressed | 128 | 80 | 256 | 4.00M records/s | 936K records/s | 128 parquet parts |

Current conclusion: the NFS metadata reader is capable of the 5M records/s
target, but DuckDB/parquet export is not yet keeping up. The main cost is in
writer/export completion after the scan timer, not in libnfs metadata discovery.

## Real NFS Data Read

Transfer1 direct-libnfs data-read baselines on `2026-05-15`, using the deploy
bundle at:

```text
/mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-diffpool-20260515T021459Z
```

Large/mixed source:

```text
nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
```

Command shape:

```text
benchmark-data
--meta-reader-threads 64
--metadata-async-depth 256
--data-reader-threads 32
--data-outstanding-requests 16
--max-files-queued 65536
--data-buffer-slots 32768
--data-queue-depth 16384
--data-copy-mode copy
--max-duration-seconds 120
--stats-interval-seconds 10
```

Result:

| Files Found | Folders Found | Files Read | Failed | Bytes Read | Elapsed | Gbit/s |
|---:|---:|---:|---:|---:|---:|---:|
| 81,560 | 75,129 | 15,614 | 0 | 1.87TB | 120.1s | 124.7 |

Small-file folder:

```text
nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/catbear/run_20260218_042836/talking-head
```

The folder contains 50,000 files with 1.36GB total logical size, about 27KiB per
file. It was scanned non-recursively with one metadata reader and data
`--data-outstanding-requests 1`.

| Data Threads | Elapsed | Files/s | Gbit/s |
|---:|---:|---:|---:|
| 32 | 5.04s | 9.9K | 2.15 |
| 64 | 3.18s | 15.7K | 3.41 |
| 96 | 3.14s | 15.9K | 3.45 |
| 128 | 3.37s | 14.8K | 3.21 |
| 192 | 6.97s | 7.2K | 1.56 |
| 256 | 6.99s | 7.2K | 1.55 |

Follow-up implementation test, 2026-05-15 on transfer1
`ubuntu@216.86.168.191`, same 50,000-file source folder. These runs used
`--data-buffer-slots 4096` and `--data-queue-depth 2048`; an earlier 65,536
data-slot command spent too long initializing an oversized 64GB data pool on the
replacement host and was not a useful reader measurement.

Raw one-file-per-buffer path, `--data-outstanding-requests 1`:

| Data Threads | Elapsed | Files/s | Gbit/s |
|---:|---:|---:|---:|
| 64 | 2.56s | 19.5K | 4.24 |
| 128 | 2.22s | 22.5K | 4.88 |
| 256 | 3.04s | 16.5K | 3.57 |

Experimental packed-small-file mode, `--pack-small-files`:

| Data Threads | Small-File Window | Elapsed | Files/s | Gbit/s |
|---:|---:|---:|---:|---:|
| 64 | 1 | timed at 30s | 591/s completed | 0.13 |
| 64 | 4 | 5.88s | 8.5K | 1.84 |
| 64 | 16 | 5.59s | 8.9K | 1.94 |
| 64 | 64 | 5.14s | 9.7K | 2.11 |
| 64 | 128 | 5.00s | 10.0K | 2.17 |
| 128 | 16 | 3.92s | 12.7K | 2.76 |
| 256 | 16 | 2.83s | 17.7K | 3.83 |

Additional larger-window sweep:

| Data Threads | Small-File Window | Elapsed | Files/s | Gbit/s |
|---:|---:|---:|---:|---:|
| 16 | 256 | 19.60s | 2.6K | 0.55 |
| 16 | 1024 | timed at 20s | 2.1K completed | 0.46 |
| 32 | 256 | 12.80s | 3.9K | 0.85 |
| 32 | 1024 | 11.89s | 4.2K | 0.91 |
| 64 | 256 | 9.30s | 5.4K | 1.17 |
| 128 | 256 | 5.84s | 8.6K | 1.86 |

Conclusion: packed small-file buffers are functionally implemented and preserve
generic buffer ownership, but this first libnfs multi-file async version is not
yet the preferred small-file read path. The raw one-file-per-buffer path remains
the default and the current best measurement for this folder. Packed mode should
stay explicit until its libnfs scheduling/packing overhead is lower than raw.

Async read latency instrumentation was added after the packed-mode regression
was observed. Same transfer1 host and same 50,000-file source folder:

| Mode | Data Threads | Window | Async Reads | Short Reads | Avg Completion | Avg Latency | Max Latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw | 128 | 1 | 49,991 | 0 | 27,103 B | 0.55 ms | 6.44 ms |
| packed | 128 | 16 | 50,000 | 0 | 27,104 B | 14.45 ms | 308.76 ms |

Conclusion from the latency counters: the server/libnfs path is not breaking
28KB files into many 4KB reads in these runs. Each file completed as one async
read on average, with no short reads. The packed path is slow because its
multi-file flow causes much higher read completion latency and a long latency
tail, not because the read buffer size is too small.

### 2026-05-15 Transfer1/Nopo1 Long Scan Calibration

Purpose: verify that scanner-class metadata throughput is restored after
disabling the aggressive per-directory `nfs_opendir_async` timeout recovery
path. The timeout path was previously causing one slow directory open to recover
and requeue an entire worker's pending libnfs batch.

Code/package:

```text
bundle:
  /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-no-opendir-timeout-20260515T004750Z
manifest:
  hypersync_git_commit=62cd045d987a32170beedd5ae5d28f081f2af82a
  hypersync_git_dirty=25
  piper_git_commit=219d453b737b5c26e0918976f4b2468d0633be84
  piper_git_dirty=17
runtime libraries:
  bundled libnfs.so.14, libduckdb.so, libstdc++.so.6, libgcc_s.so.1
```

Shared settings:

```text
command:
  benchmark-meta
metadata reader:
  --meta-reader-threads 64
  --metadata-async-depth 256
  --record-buffer-slots 10000000
writer:
  --metadata-output-format parquet
  --metadata-records all
  --metadata-output-partitions 32
  --metadata-output-partition-mode processes
  duckdb_memory_limit: 1GB
  duckdb_threads: 1
  duckdb_checkpoint_threshold: 2GB
  parquet_compression: uncompressed
stats:
  --metadata-stats-discarder
  --stats-interval-seconds 10
  track_unique_folders: false
duration:
  --max-duration-seconds 600
```

Regression check before disabling the per-directory open timeout:

| Pipeline | Threads | Async Depth | Record Slots | Result | Timeout Count |
|---|---:|---:|---:|---:|---:|
| reader-only | 64 | 256 | 10M | 1.17M records/s over 63.3s | 27,936 |
| scan + 32 DuckDB writers | 64 | 256 | 1M | 2.51M records/s over 390s | 110,992 at 325s sample |

Validation after disabling the per-directory open timeout:

| Host | Source | Files Seen | Folders Found | Logical Size | Records/s | Timeout Count | Skipped Folders | Output |
|---|---|---:|---:|---:|---:|---:|---:|---|
| transfer1 | `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5` | 3,727,219,286 | 37,599,501 | 3,864,323,502,965,916 B | 6.19M | 0 | 5,130 | 32 parquet parts, 682 GiB run dir |
| nopo1 | `nfs://172.27.255.2-172.27.255.17/volumes/dfb990b1-bf40-4378-85f1-26f9dfd0cd2c/data` | 4,280,160,904 | 38,512,878 | 4,745,584,050,812,557 B | 7.13M | 0 | 5,105 | 32 parquet parts, 777 GiB run dir |

Run directories:

```text
transfer1:
  /mnt/local-nvme/wsync-codex/long-scan-transfer1-20260515T005652Z
nopo1:
  /mnt/local-nvme/wsync-codex/long-scan-nopo1-dfb-20260515T005835Z
```

Notes:
- The nopo1 `b7ec3b01-0aba-49cc-b3d2-6692504cf6c5` export failed at libnfs
  mount time from that worker with `MNT3ERR_NOENT / Operation not permitted`;
  the long nopo scan used the currently mounted/exported `dfb990b1...` volume.
- The packaged `./hypersync` launcher must be used because it sets
  `LD_LIBRARY_PATH` and default bundle config. Running `hypersync.bin` directly
  can miss bundled shared libraries or config.
- The corrected runs show the scanner and parquet writer path can sustain
  6M-7M records/s for a 10-minute measurement window with no libnfs timeout
  churn.

## Checker And Data Transfer Smokes

Recorded on `2026-05-12 22:44 PDT` from commit `9b5c589`. These are bounded
remote smokes for the live checker and the runtime file-transfer path; they are
not full-host calibration runs.

### Live Checker Self-Diff

Both runs compare a source NFS tree to the same target NFS tree using
`--compare size`, 8 metadata-reader threads, async depth 16, and
`--max-duration-seconds 10`.

| Host | Source | Diff Records | Same | Changed | New | Target-Only | Elapsed | Max RSS |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| transfer1 | `nfs://nfs.crusoecloudcompute.com/.../HaWoR/video` | 46,239 | 46,239 | 0 | 0 | 0 | 1.16s | 54 MB |
| nopo1 | `nfs://172.27.255.2-172.27.255.17/.../data` | 282,060 | 282,060 | 0 | 0 | 0 | 17.47s | 605 MB |

Nopo reported several `NFS3ERR_PERM` folders; the checker skipped those folders
and did not emit false target-only rows when the timer stopped a partial folder.

### NFS Data Reader

Both runs used 1 metadata thread, metadata async depth 16, 32 data-reader
threads, 16 outstanding data requests per reader, copy mode, and
`--max-duration-seconds 10`.

| Host | Source | Bytes Read | Active Rate | Final Average | Notes |
|---|---|---:|---:|---:|---|
| transfer1 | `.../HaWoR/video` | 3.68 GB | 6.39 Gbit/s | 6.39 Gbit/s | Small tree completed before the timer. |
| nopo1 | `.../b7ec3b01.../data` | 245.31 GB | 193.6-195.8 Gbit/s | 100.2 Gbit/s | Active samples hit near 200G; final average includes drain/tail time. |

### Packed Small-File Runtime Transfer

Runtime transfer packs many small file payloads into large data buffers before
sending them. The smoke used local loopback TCP on each remote host with 1,000
3-byte files and a local NVMe target.

| Host | Files Sent | Files Received | Payload Bytes | Data Chunks Sent | Result |
|---|---:|---:|---:|---:|---|
| transfer1 | 1,000 | 1,000 | 3,000 | 85 | pass |
| nopo1 | 1,000 | 1,000 | 3,000 | 114 | pass |

## Automated Tests

Performance smoke tests live under `hypersync/tests/features/performance/` and are run
with:

```bash
make performance-test
```

Those tests intentionally use tiny local workloads. They verify that benchmark
commands run, move the expected buffers, and report non-zero throughput. They do
not enforce transfer1 production-class numbers.

Per-host job performance reports are produced with:

```bash
make job-performance-test
```

This target builds and uses `build/release/hypersync` with `-O3 -DNDEBUG`.
The target writes:

```text
build/job-performance-report.txt
build/job-performance-report.json
```

Unit tests can be listed and filtered by stable short id or by name substring:

```bash
./build/hypersync_tests --suite unit --list
./build/hypersync_tests --suite unit --include t011
./build/hypersync_tests --suite unit --exclude sharded
```

For a larger host calibration, run the script directly with host-appropriate
parallelism:

```bash
python3 hypersync/tests/job_performance_report.py \
  --app ./build/release/hypersync \
  --output-text /mnt/local-nvme/wsync-job-performance.txt \
  --output-json /mnt/local-nvme/wsync-job-performance.json \
  --build-label release \
  --buffer-size 1048576 \
  --pool-slots 4096 \
  --buffers-per-lane 4096 \
  --parallelism 1,2,8,16,32,64,96,128,160 \
  --discarder-threads 1,8,32,64,128 \
  --endpoints 1,8,16,64,128 \
  --transport-generator-threads 32 \
  --transport-buffers-total 131072 \
  --hash-duration-seconds 5 \
  --writer-formats csv,parquet \
  --writer-partition-modes threads,processes \
  --writer-partitions 1,32 \
  --writer-file-count 1000000 \
  --writer-folder-count 1000 \
  --writer-output-dir /mnt/local-nvme/wsync-job-performance-writers
```

## Latest Transfer1 Job Performance Report

Generated on transfer1 from `/mnt/local-nvme/wsync-codex` using the release
binary. The report artifacts are also stored locally at:

```text
build/job-performance-report-transfer1.txt
build/job-performance-report-transfer1.json
```

```text
wsync_job_performance
host=ice1-transfer-001
timestamp_utc=2026-05-09T00:09:32Z
cpu="AMD EPYC 9454 48-Core Processor"
hardware_threads=160
numa_nodes=2
buffer_size=1048576
pool_slots=128
build=transfer1-release

job=BufferGeneratorJob
pattern=xoshiro256
pipeline=BufferGeneratorJob -> BufQueue -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
threads  queues  consumers  status  gb_s     gbit_s    buffers_s
-------  ------  ---------  ------  -------  --------  ----------
      1       1          1  ok       10.110    80.880    9641.647
      2       2          2  ok       19.722   157.779   18808.651
      8       8          8  ok       78.902   631.213   75246.429
     16      16         16  ok      106.335   850.676  101408.958
     32      32         32  ok      216.021  1728.170  206013.680
     64      64         64  ok      410.326  3282.610  391317.368
     96      96         96  ok      526.606  4212.850  502210.617
    128     128        128  ok      640.115  5120.920  610461.235
    160     160        160  ok      750.966  6007.730  716176.987

job=BufferDiscarderJob
pattern=xoshiro256
pipeline=BufferGeneratorJob -> BufQueue -> BufferDiscarderJob
queue_topology=single_shared
units=GB/s,Gbit/s,buffers/s
generator_threads  discarder_threads  queues  status  gb_s     gbit_s    buffers_s
-----------------  -----------------  ------  ------  -------  --------  ----------
               32                  1       1  ok      136.593  1092.750  130265.236
               32                  8       1  ok       30.567   244.534   29150.772
               32                 32       1  ok       54.854   438.830   52312.565
               32                 64       1  ok       57.430   459.443   54769.897
               32                128       1  ok       72.201   577.611   68856.525

job=BufferSenderJob+BufferReceiverJob
pattern=xoshiro256
transport=unix
pipeline=BufferGeneratorJob -> BufferSenderJob -> unix -> BufferReceiverJob -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
endpoints  streams  generator_threads_total  status  gb_s    gbit_s   buffers_s
---------  -------  -----------------------  ------  ------  -------  ---------
        1        1                       32  ok       2.727   21.819   2601.070
        8        8                       32  ok      17.526  140.211  16714.382
       16       16                       32  ok      30.648  245.180  29227.734
       64       64                       64  ok      39.948  319.581  38097.000
      128      128                      128  ok      34.642  277.135  33037.090

job=BufferSenderJob+BufferReceiverJob
pattern=xoshiro256
transport=tcp
pipeline=BufferGeneratorJob -> BufferSenderJob -> tcp -> BufferReceiverJob -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
endpoints  streams  generator_threads_total  status  gb_s    gbit_s   buffers_s
---------  -------  -----------------------  ------  ------  -------  ---------
        1        1                       32  ok       2.510   20.081   2393.808
        8        8                       32  ok      12.965  103.719  12364.292
       16       16                       32  ok      20.053  160.423  19123.936
       64       64                       64  ok      34.337  274.700  32746.792
      128      128                      128  ok      30.989  247.913  29553.509

job=HashSpeed
pipeline=CPU hash benchmark
units=GB/s,Gbit/s
algorithm  threads  status  gb_s     gbit_s
---------  -------  ------  -------  --------
xxh64            1  ok       14.941   119.527
xxh64            2  ok       29.845   238.757
xxh64            4  ok       59.528   476.226
xxh64            8  ok      118.987   951.896
xxh64           16  ok      236.460  1891.680
xxh64           32  ok      476.937  3815.500
sha256           1  ok        1.852    14.816
sha256           2  ok        3.701    29.607
sha256           4  ok        7.389    59.114
sha256           8  ok       14.727   117.814
sha256          16  ok       29.282   234.252
sha256          32  ok       58.398   467.183
```

## Transfer1 Retest Candidate

Generated on transfer1 after making the development/performance local path use
`ShardedBufQueue` by default. This run was not promoted over the previous
transfer1 baseline because the 160-lane generator result was materially lower
than the prior run: 655.8 GB/s versus 751.0 GB/s. The report artifacts are
stored locally at:

```text
build/job-performance-report-transfer1-new.txt
build/job-performance-report-transfer1-new.json
```

The useful promoted conclusions from this retest are:

| Area | Result | Notes |
|---|---:|---|
| BufferDiscarderJob, 128 sharded lanes | 635.6 GB/s | Confirms the normal development path is sharded and no longer exercising the old single shared queue. |
| Unix transport, 64 endpoints | 42.7 GB/s | Slightly above the previous 39.9 GB/s run. |
| CSV writer, 32 threads | 8.88M records/s | Single process, threaded writer mode. |
| CSV writer, 32 processes | 24.9M records/s | Multi-process part writer mode. |
| Parquet/DuckDB writer | unavailable | Transfer1 release was built with `HYPERSYNC_HAS_DUCKDB=0`, so parquet rows failed and are not performance measurements. |

```text
job=BufferGeneratorJob
pipeline=BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob
queue_topology=sharded_lanes
threads  queues  consumers  status  gb_s     gbit_s
-------  ------  ---------  ------  -------  --------
      1       1          1  ok       10.248    81.985
      2       2          2  ok       20.064   160.509
      8       8          8  ok       53.360   426.883
     16      16         16  ok      106.213   849.705
     32      32         32  ok      212.196  1697.560
     64      64         64  ok      417.643  3341.140
     96      96         96  ok      524.792  4198.330
    128     128        128  ok      635.255  5082.040
    160     160        160  ok      655.773  5246.190

job=BufferDiscarderJob
pipeline=BufferGeneratorJob -> ShardedBufQueue -> BufferDiscarderJob
queue_topology=sharded_lanes
generator_threads  discarder_threads  queues  status  gb_s     gbit_s
-----------------  -----------------  ------  ------  -------  --------
                1                  1       1  ok       10.182    81.455
                8                  8       8  ok       54.483   435.867
               32                 32      32  ok      215.062  1720.490
               64                 64      64  ok      414.440  3315.520
              128                128     128  ok      635.611  5084.890

job=MetadataRecordWriterJob
pipeline=FileMetadataGenerator -> MetadataRecordWriter
format   partition_mode  partitions  status  records_s     files_s
-------  --------------  ----------  ------  ------------  ------------
csv      threads                  1  ok       1218150.000   1216930.000
csv      threads                 32  ok       8883870.000   8875000.000
csv      processes                1  ok       1179870.000   1178690.000
csv      processes               32  ok      24885100.000  24860300.000
parquet  threads                  1  failed         0.000         0.000
parquet  threads                 32  failed         0.000         0.000
parquet  processes                1  failed         0.000         0.000
parquet  processes               32  failed         0.000         0.000
```

## Latest Local Job Performance Report

This report was generated by `make job-performance-test` on the current local
release build. It is useful for validating report shape and local relative
behavior; run the same target on each server to capture that server's actual
job-level expectations.

```text
wsync_job_performance
host=MBP-Artem-Gavrilov
timestamp_utc=2026-05-08T23:48:56Z
cpu=""
hardware_threads=12
numa_nodes=0
buffer_size=1048576
pool_slots=16
build=release-local

job=BufferGeneratorJob
pattern=xoshiro256
pipeline=BufferGeneratorJob -> BufQueue -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
threads  queues  consumers  gb_s    gbit_s   buffers_s
-------  ------  ---------  ------  -------  ---------
      1       1          1  10.145   81.160   9675.026
      2       2          2  22.271  178.171  21239.662
      4       4          4  43.895  351.158  41861.343
      8       8          8  77.004  616.033  73436.832
     16      16         16  84.252  674.020  80349.445
     32      32         32  91.217  729.734  86991.119
     64      64         64  83.744  669.951  79864.407
    128     128        128  90.403  723.224  86215.019

job=BufferDiscarderJob
pattern=xoshiro256
pipeline=BufferGeneratorJob -> BufQueue -> BufferDiscarderJob
queue_topology=single_shared
units=GB/s,Gbit/s,buffers/s
generator_threads  discarder_threads  queues  gb_s    gbit_s   buffers_s
-----------------  -----------------  ------  ------  -------  ---------
               32                  1       1  76.228  609.828  72697.163
               32                  8       1  82.380  659.041  78563.786
               32                 32       1  87.058  696.462  83024.788
               32                 64       1  71.980  575.836  68645.000
               32                128       1  66.786  534.286  63691.807

job=BufferSenderJob+BufferReceiverJob
pattern=xoshiro256
transport=unix
pipeline=BufferGeneratorJob -> BufferSenderJob -> unix -> BufferReceiverJob -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
endpoints  streams  generator_threads_total  gb_s   gbit_s  buffers_s
---------  -------  -----------------------  -----  ------  ---------
        1        1                       32  0.493   3.941    469.803
        8        8                       32  2.328  18.622   2219.877
       16       16                       32  3.061  24.486   2918.940
       32       32                       32  2.958  23.666   2821.150

job=BufferSenderJob+BufferReceiverJob
pattern=xoshiro256
transport=tcp
pipeline=BufferGeneratorJob -> BufferSenderJob -> tcp -> BufferReceiverJob -> BufferDiscarderJob
queue_topology=sharded
units=GB/s,Gbit/s,buffers/s
endpoints  streams  generator_threads_total  gb_s   gbit_s  buffers_s
---------  -------  -----------------------  -----  ------  ---------
        1        1                       32  7.875  63.001   7510.252
        8        8                       32  7.498  59.984   7150.707
       16       16                       32  5.697  45.577   5433.149
       32       32                       32  4.800  38.396   4577.179

job=HashSpeed
pipeline=CPU hash benchmark
units=GB/s,Gbit/s
algorithm  threads  gb_s     gbit_s
---------  -------  -------  --------
xxh64            1   23.677   189.414
xxh64            2   47.542   380.338
xxh64            4   95.667   765.339
xxh64            8  166.092  1328.740
sha256           1    2.949    23.590
sha256           2    5.970    47.760
sha256           4   11.867    94.938
sha256           8   22.155   177.237
```

## Live NFS Diff/Checker Expectations

Summary-only live diff is the checker hot path: it compares flat-folder batches
and updates counters without materializing per-file maps or CSV output. For
time-limited runs, target-only reporting is disabled because a timer can stop a
source directory mid-read and make the target look falsely ahead of the source.
Run a full, untimed diff when target-only records are required.

Transfer1 root self-diff against
`nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
on 2026-05-13:

```text
command:
  hypersync diff --source <root> --target <root> --compare size \
    --summary-only --meta-reader-threads 96 --metadata-async-depth 128 \
    --max-duration-seconds 5 --stats-interval-seconds 5

observed:
  5s average:  668,902 records/s, 3,344,675 compared records
  10s average: 980,232 records/s, 9,804,137 compared records
  10s interval: 1,291,480 records/s
  15s average: 781,677 records/s, 11,726,688 compared records
  max RSS before timeout: about 9.7 GiB

scanner reference on the same root:
  5s average:  4,396,420 records/s
  10s average: 5,813,000 records/s
  final after drain: 1,504,640 records/s over 56,824,570 files

notes:
  - Checker now runs source and target metadata readers as separate async jobs
    and rendezvous flat-folder batches by folder path.
  - This is scanner-class order of magnitude, but still below the scanner
    target because it performs two NFS reads and a per-folder comparison.
  - The next hot spot is comparison CPU/allocation in very large flat folders.
```

Isolated fake-remote checker benchmark on local release build, 2026-05-13:

```text
note:
  jobs.checker.worker_count is the default checker parallelism. Use
  --checker-threads only when the run is intentionally testing a specific
  checker width.

single-checker baseline command:
  hypersync benchmark-fake-diff --file-count 1000000 --folder-count 10000 \
    --source-threads 4 --fake-remote-threads 4 --remote-delay-us 1000 \
    --checker-threads 1 --request-queue-depth 65536 --batch-queue-depth 65536 \
    --stats-interval-seconds 1

observed:
  source generated: 1,000,000 files, 10,000 folders
  fake target checked: 10,000 folders
  compared: 1,000,000 files, all same
  source elapsed: 0.032s, 31.37M records/s
  total elapsed: 3.136s, 318.9K records/s

notes:
  - Source generation completed before delayed fake-remote checking drained.
  - This confirms source-side pipeline progress is independent from remote
    answers until bounded queues fill.
  - Fake remote delay is modeled by a separate fake processor job: request
    receiver workers accept folder requests quickly, then delayed processor
    workers publish replies later.
  - The total rate is intentionally dominated by the 1 ms per-folder fake
    remote delay across four fake remote threads.
  - The number is a local synthetic sanity check, not a transfer-server NFS
    release baseline.
```

The checker bottleneck was then sharded by folder path so 16 checker workers
can compare independent flat folders in parallel:

```text
single checker, no fake delay:
  command:
    hypersync benchmark-fake-diff --file-count 20000000 --folder-count 100000 \
      --source-threads 8 --fake-remote-threads 8 --checker-threads 1 \
      --remote-delay-us 0 --request-queue-depth 8192 --batch-queue-depth 8192
  observed:
    total: 2.06M records/s
    source: 2.24M records/s
    checker process time: 598.9 thread-s over 614.9s in the 1.3B-record run

16 checker workers, no fake delay:
  command:
    hypersync benchmark-fake-diff --file-count 200000000 --folder-count 1000000 \
      --source-threads 8 --fake-remote-threads 8 --checker-threads 16 \
      --remote-delay-us 0 --request-queue-depth 8192 --batch-queue-depth 32768 \
      --stats-interval-seconds 10
  observed:
    total: 3.79M records/s
    source: 4.01M records/s
    compared: 200M files, all same
    checker process time: 811.0 thread-s over 52.8s
    process CPU samples: about 850-1000% on local Mac

notes:
  - The checker now uses many cores instead of one pegged core.
  - 16 source and 16 fake-remote workers were slower on this host because they
    increased queue/scheduler pressure without improving compare throughput.
  - The best local synthetic shape so far is 8 source workers, 8 fake-remote
    workers, and 16 checker workers.
```

Nopo1 self-diff against
`nfs://172.27.255.2-172.27.255.17/volumes/b7ec3b01-0aba-49cc-b3d2-6692504cf6c5/data`
with the same summary-only command shape:

```text
observed:
  5s average:  692,772 records/s, 3,464,003 compared records
  10s average: 817,302 records/s, 8,173,386 compared records
  10s interval: 941,831 records/s
  max RSS before timeout: about 6.9 GiB

notes:
  - The run encountered many permission-denied folders and continued by
    skipping those source folders.
  - Timed-run target-only suppression kept self-diff counters stable:
    `target_only=0`.
```

Distributed transfer1-to-nopo1 metadata diff, 2026-05-13:

```text
source host:
  ubuntu@216.86.168.191
  nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5

target host:
  ubuntu@160.211.77.39
  nfs://172.27.255.2-172.27.255.17/volumes/dfb990b1-bf40-4378-85f1-26f9dfd0cd2c/data

command shape:
  diff-target --target <target-url> --port 39172 --target-threads 96 \
    --metadata-async-depth 128 --stats-interval-seconds 5
  diff-source --source <source-url> --target-host 160.211.77.39 \
    --port 39172 --folder-report folder-report.csv --compare size \
    --meta-reader-threads 96 --metadata-async-depth 128 \
    --max-duration-seconds 60 --stats-interval-seconds 5

observed after target drain:
  folder reports: 321,264
  file decisions: 82,898,749
  same: 82,898,562
  changed: 0
  source-only/new: 187
  target-only: 0 (disabled by timed source run)
  failed file/folder status rows: present; mostly permission or target NOENT
  source logical size: 582.27 TB
  target logical size: 671.40 TB
  planned bytes: 169.27 GB
  elapsed: 157.98 s
  average: 524,745 file decisions/s, 2,034 folder reports/s
  peak interval in this run: about 608K file decisions/s
  report: /mnt/local-nvme/diff-transfer1-nopo1-current/folder-report.csv

notable large/hot folders:
  catbear/video_embeddings/cbembeddings/data:
    target scan 15.6 s, source scan 31.8 s, no direct files in report row
  videos/youtube/downloads:
    target scan 13.8 s, source scan 86.3 s, no direct files in report row
  user/ryan/video-dataloading-ablation-1-episode/results:
    target scan 1.45 s, 27,744 target files
  catbear/run_20260218_042836/talking-head:
    target scan 1.03 s, 50,000 target files

notes:
  - The first run aborted at about 330K folder reports because a target-side
    libnfs worker error killed the process. The implementation now turns that
    into a failed folder summary and continues.
  - This distributed implementation proves the two-host flow and per-folder
    reporting, but it is not scanner-class yet. The current bottlenecks are
    per-folder frames, one TCP stream/write mutex, and whole-folder compare
    units. Next work is result batching, source-batch splitting for large flat
    folders, and multiple transport streams.
```

Distributed transfer1-to-nopo1 metadata diff, 2026-05-15, after fixing
distributed diff source-pool starvation:

```text
code:
  hypersync commit: 7d6f202a0e8308dab43065f3204130ced9976e17
  piper commit: a8b2b5c9f73b1a62d95441018aaf622d8b88b82d
  deploy bundle:
    /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-diffpool-20260515T021459Z

source host:
  ubuntu@216.86.168.191
  nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5

target host:
  ubuntu@160.211.77.39
  nfs://172.27.255.2-172.27.255.17/volumes/dfb990b1-bf40-4378-85f1-26f9dfd0cd2c/data

command shape:
  diff-target --target <target-url> --listen-host 0.0.0.0 --port 39174 \
    --compare size --target-threads 64 --metadata-async-depth 256 \
    --stats-interval-seconds 10
  diff-source --source <source-url> --target-host 160.211.77.39 \
    --port 39174 --folder-report folder-report.csv --compare size \
    --meta-reader-threads 64 --metadata-async-depth 256 \
    --max-duration-seconds 240 --stats-interval-seconds 10

final observed result, including drain:
  folders sent/reported: 7,498,305 / 7,498,305
  source buffers sent: 7,505,674
  target result buffers sent: 33,649
  files compared: 1,480,818,403
  same: 1,425,520,157
  changed: 267
  source-only/new: 55,213,674
  target-only: 84,305
  failed folder rows: 5,215, mostly NFS3ERR_PERM permission-denied folders
  source logical size: 1,814,417,688,970,220 bytes
  target logical size: 1,819,626,621,592,633 bytes
  planned bytes: 14,246,266,287,859 bytes
  elapsed: 296.611 s
  average: 25,279.9 folders/s, 4.992M files/s
  hot interval examples: 5.82M files/s at 116 s, 5.77M files/s at 126 s
  source report:
    /mnt/local-nvme/wsync-codex/diff-source-transfer1-to-nopo1-diffpool-20260515T021730Z/folder-report.csv

job/queue notes:
  source_send_queue high watermark: 3,580 / 16,384
  result_queue high watermark: 40 / 16,384 on source, 45 / 16,384 on target
  previous deadlock signature disappeared: source/target queues drained and
    closed, and both processes exited without manual kill.
  target source_receiver accumulated about 3% wait_pool near tail only; it did
  not block progress and all source buffers were received.
```

NFS small-file data-read experiment, 2026-05-15, transfer1
`ubuntu@216.86.168.191`, source folder
`catbear/run_20260218_042836/talking-head`, 50,000 files,
1,355,189,671 logical bytes, release build with libnfs:

```text
code/worktree:
  staged at /mnt/local-nvme/wsync-codex/libnfs-sliding-20260515T052200Z

change tested:
  - service_nfs_context now drains immediately-ready libnfs socket events after
    the blocking poll wakeup.
  - added non-packed small-file sliding-window reader where each in-flight file
    owns one data buffer and publishes immediately after completion.

single data-reader thread:
  old one-file-at-a-time path:
    0.150 Gbit/s, avg read latency 0.538 ms, avg open latency 0.792 ms
  sliding window 2:
    0.031 Gbit/s, avg read latency 0.585 ms, avg open latency 6.22 ms
  sliding window 4:
    0.039 Gbit/s, avg read latency 5.18 ms, avg open latency 10.79 ms
  sliding window 8:
    0.044 Gbit/s, avg read latency 10.00 ms, avg open latency 21.64 ms
  sliding window 16:
    0.040 Gbit/s, avg read latency 27.43 ms, avg open latency 48.59 ms

128 data-reader threads:
  old one-file-at-a-time path:
    4.217 Gbit/s, completed all 50,000 files in 2.57 s,
    avg read latency 0.518 ms
  sliding window 2:
    3.792 Gbit/s, completed all 50,000 files in 2.86 s,
    avg read latency 0.705 ms

conclusion:
  the packed/batch HoL issue was real and the sliding implementation removes
  that architectural flaw, but this NFS server/context performs worse when a
  single libnfs context carries concurrent small-file open/read/close state.
  The fastest tested shape remains many independent reader workers/contexts
  with one file in flight per context. Keep packed mode and same-context sliding
  mode experimental until a raw READDIRPLUS/file-handle path or server-side
  open behavior is improved.
```

Open-only benchmark, same transfer1 host and same 50,000-file folder. This mode
does metadata scan -> file queue -> NFS open/close workers, with no data reads
or data buffers:

```text
command shape:
  benchmark-open --source <talking-head nfs url> --non-recursive \
    --meta-reader-threads 1 --metadata-async-depth 16 \
    --open-threads <n> --max-files-queued <4096|8192> \
    --max-duration-seconds 20 --stats-interval-seconds 20

open-only:
  32 threads:   14,158 opens/s, avg open 0.946 ms
  64 threads:   14,998 opens/s, avg open 1.000 ms
  128 threads:  20,455 opens/s, avg open 1.205 ms
  256 threads:  22,834 opens/s, avg open 1.189 ms
  512 threads:   6,806 opens/s, 1 failed, avg open 1.200 ms
  1024 threads:  3,556 opens/s, 488 failed, avg open 1.099 ms

full open+read, same build:
  128 threads: 2.81 Gbit/s, avg open 1.053 ms, avg read 1.633 ms
  256 threads: 2.86 Gbit/s, avg open 1.136 ms, avg read 0.575 ms

interpretation:
  for this folder, average file size is about 27 KiB. Even 22,834 opens/s only
  supplies about 5 Gbit/s of payload. That is far below the 200 Gbit/s NIC goal,
  so small-file reads are open/operation-rate limited before they are bandwidth
  limited. Splitting open and read can still help hide open latency for larger
  files, but it cannot make this specific tiny-file workload reach 200 Gbit/s
  unless we avoid per-file open cost with a lower-level file-handle path or
  batch/pack files at the source.
```

Raw READDIRPLUS handle data-read experiment, 2026-05-15, transfer1
`ubuntu@216.86.168.191`, same 50,000-file folder:

```text
standalone prototype, full file reads, no per-file ACCESS:
  1 context:     0.22 Gbit/s
  8 contexts:    1.71 Gbit/s
  32 contexts:   6.53 Gbit/s
  64 contexts:  10.91 Gbit/s
  128 contexts: 14.56 Gbit/s
  128 contexts, window 4: 14.66 Gbit/s
  192 contexts: 12.11 Gbit/s
  256+ contexts: worse, about 1.5-1.9 Gbit/s

integrated benchmark-data pipeline after preserving READDIRPLUS handles in
FileSpec and making NfsDataReader prefer raw READ by handle:
  source nfs://nfs.crusoecloudcompute.com/.../talking-head
    128 data-reader threads, copy:    3.75 Gbit/s
  source nfs://172.27.255.18-33/.../talking-head
    128 data-reader threads, copy:    4.31 Gbit/s
    256 data-reader threads, copy:    2.60 Gbit/s
    512 data-reader threads, copy:    1.68 Gbit/s, 338 failed files
    128 data-reader threads, no-copy: 3.11 Gbit/s
    256 data-reader threads, no-copy: 2.47 Gbit/s

verification:
  async_open_completed=0 and async_close_completed=0 in the integrated data
  benchmark. All successful reads used raw NFSv3 READ against file handles from
  READDIRPLUS.

interpretation:
  the raw file-handle path works and removes the high-level open/close tax. The
  integrated benchmark is currently below the standalone prototype because the
  scanner still opens each folder through the public libnfs directory path before
  raw-listing it for handles, and the benchmark feeds readers through one shared
  file queue. The next tuning target is therefore pipeline orchestration:
  sharded file queues and native raw READDIRPLUS folder scanning, not the raw
  NFS READ itself.
```

Raw-handle large/mixed window sweep, 2026-05-15, transfer1
`ubuntu@216.86.168.191`, source
`nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`,
30 second runs, 64 metadata threads, metadata async depth 256, 128 data-reader
threads, copy mode:

```text
window  Gbit/s   avg read latency   max latency   notes
1       99.85    10.06 ms           269 ms        best in this sweep
2       98.55    19.82 ms           1589 ms       same throughput, 2x latency
4       94.45    43.52 ms           713 ms        worse, queued at server/backend
8       95.90    85.26 ms           572 ms        worse, long queueing tail
16      95.17    150.98 ms          1117 ms       worse, severe queueing
```

Conclusion: increasing `data_outstanding_requests` beyond 1 did not fill more
network pipe on this source. It mostly created NFS/RPC queueing delay. The clean
BDP-looking window-1 math was coincidental for this source mix: the backend
appears to saturate around 95-105 Gbit/s for this client/source path before a
per-context chunk window helps. Keep the default raw-handle large-file window at
1 for now. Next scaling work should focus on independent endpoint/lane
orchestration and sharded file queues rather than larger per-context windows.

Sticky endpoint lane test, 2026-05-15, transfer1
`ubuntu@216.86.168.191`, same 16-IP source and root-like mixed workload.
Data-reader workers were assigned deterministic endpoint indexes instead of
random endpoint selection: worker `i` uses expanded source endpoint
`i % endpoint_count`.

```text
command shape:
  benchmark-data --source nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5 \
    --meta-reader-threads 64 --metadata-async-depth 256 \
    --data-reader-threads <n> --data-outstanding-requests 1 \
    --max-files-queued 1048576 --data-buffer-slots <4096|8192> \
    --data-queue-depth <4096|8192> --max-duration-seconds <30|60> \
    --stats-interval-seconds <30|10>

data_reader_threads  Gbit/s   avg read latency   notes
128                  101.45   9.89 ms            all 16 endpoints active; async opens/closes 0
256                   90.99   16.80 ms           worse; queueing latency increased
512                   65.88   53.77 ms           worse; 2 failed reads and slow drain
```

Socket sampling during the 128-thread run showed active connections to every
endpoint in `172.27.255.18-33`. Counts were not exactly 8 per IP because the
metadata scanner still opens its own random libnfs sessions, but the data
reader lane assignment is deterministic.

Conclusion: sticky data-reader endpoint assignment makes the lane topology
explicit and reproducible, but it does not move the observed wall beyond about
100 Gbit/s on this source. Increasing context count beyond 128 still hurts, so
the current best default remains 128 data readers, window 1. The next useful
test is outside per-reader endpoint choice: NIC queue/IRQ layout, raw metadata
scanner lane isolation, or independent client-side process/lane groups if we
need to prove whether the wall is client orchestration or backend/fabric.

RPS/XPS steering test, 2026-05-15, transfer1
`ens3` is a 200 Gbit/s mlx5 device, but the cloud NIC exposes only 11 combined
queues. The NIC is NUMA-local to node 0:

```text
lscpu:
  CPUs: 160
  NUMA node0: 0-79
  NUMA node1: 80-159

ethtool -l ens3:
  Combined max/current: 11 / 11

before:
  rx rps_cpus: 00000000,00000000,00000000,00000000,00000000
  rps_sock_flow_entries: 0
```

Two RPS/XPS masks were tested with the same sticky-lane 128-reader benchmark
above, using fixed RX coalescing `rx-usecs=12`:

```text
mask                         Gbit/s hot window   final Gbit/s   avg read latency
none / default               103.72 at 60s       101.45         9.89 ms
NUMA node0 CPUs 0-79         111.54 at 60s       no clean exit  8.74 ms
all CPUs 0-159               107.92 at 60s       106.74         9.40 ms
```

Node0 settings used:

```text
net.core.rps_sock_flow_entries=65536
rx-*/rps_cpus=00000000,00000000,0000ffff,ffffffff,ffffffff
rx-*/rps_flow_cnt=4096
tx-*/xps_cpus=00000000,00000000,0000ffff,ffffffff,ffffffff
ethtool -C ens3 adaptive-rx off rx-usecs 12
```

`NET_RX` counters showed activity on all CPUs during the RPS runs, but the
hardware IRQ counters are still anchored to the 11 mlx5 completion queues. RPS
helped, but did not unlock the second 100 Gbit/s. The best observed setting is
the NUMA-local node0 mask; the all-CPU mask was slightly worse, likely from
cross-NUMA traffic. Transfer1 was left with the node0 mask after the test.

NUMA pinning and jumbo MTU test, 2026-05-15, transfer1
Current MTU before testing was 1500. Jumbo frames were accepted on `ens3`, and a
DF ping with 8972-byte payload to `172.27.255.18` succeeded. Benchmarks used the
same RPS/XPS node0 settings above plus `taskset -c 0-79` to keep hypersync
workers on the NIC-local NUMA node. MTU is now kept at 9000 for transfer1
performance work.

```text
MTU   data readers  window  copy mode  Gbit/s hot/final  avg read latency  notes
1500  128           1       copy       124.28 / 92.98    7.45 ms           final includes drain
1500  256           1       copy       135.27 / 101.21   13.86 ms          faster hot, higher latency
9000  128           1       copy       158.49 / 157.21   5.26 ms           zero failures
9000  256           1       copy       184.46 / 183.62   11.28 ms          best stable copy run
9000  320           1       copy       177.95 / 177.07   14.11 ms          worse than 256
9000  384           1       copy       185.31 / 184.20   16.34 ms          similar throughput, worse latency
9000  256           2       copy       189.85 / 188.96   17.34 ms          best observed throughput
9000  256           3       copy       174.76 / 171.93   15.34 ms          worse
9000  256           4       copy       182.15 / 180.10   32.20 ms          worse queueing
9000  256           2       no-copy    182.74 / 181.10   21.06 ms          no-copy did not help
```

Conclusion: jumbo MTU is the largest networking lever observed so far. It moved
the best run from about 124-135 Gbit/s hot-window with MTU 1500 to about
189 Gbit/s with MTU 9000. The best setting in this sweep was MTU 9000,
NUMA-node0 RPS/XPS, `taskset -c 0-79`, 256 data readers, and
`data_outstanding_requests=2`. Going wider than 256 readers or deeper than
window 2 did not reach 200 Gbit/s and mostly increased latency. No-copy did not
improve throughput, so the remaining wall is not primarily the application
buffer copy in this benchmark.

Transfer1 optimized runtime profile, 2026-05-15
After comparing against the older high-throughput host settings, transfer1 was
updated and verified with this profile:

```text
host: ice1-transfer-001
interface: ens3 / mlx5_core
CPUs: 160
NIC NUMA node: 0, local CPUs 0-79
MTU: 9000
PMTU to NFS private endpoint 172.27.255.18: OK
RX/TX queues: 11/11
combined channels: 11/11 max/current
RX/TX ring: 8192/8192
irqbalance: inactive
RPS CPUs: 80/160, CPUs 0-79
RPS mask: 00000000,00000000,0000ffff,ffffffff,ffffffff
RPS flow cnt / RX queue: 32768
XPS CPUs: 80/160, CPUs 0-79
XPS mask: 00000000,00000000,0000ffff,ffffffff,ffffffff
net.core.rps_sock_flow_entries: 262144
net.core.rmem_max: 2147483647
net.core.wmem_max: 2147483647
net.ipv4.tcp_rmem: 4096 1048576 2147483647
net.ipv4.tcp_wmem: 4096 1048576 2147483647
TCP congestion control: bbr
Default qdisc: fq
tcp_mtu_probing: 1
TCPMSS mangle rules: 0
sunrpc.tcp_max_slot_table_entries: 65536
sunrpc.tcp_slot_table_entries: 65536
NFS mount: /mnt/crusoe-src
NFS mode: ro
NFS nconnect: 32
NFS rsize/wsize: 1048576/1048576
NFS flags: noatime,nodiratime,acregmax=600,acdirmax=600,spread_reads
NFS BDI read-ahead: 16384 KiB
```

The runtime profile can be reapplied with:

```bash
hypersync/deploy/tune-transfer1-network.sh ens3
```

The recommended benchmark shape for the data-reader path on this profile is:

```bash
taskset -c 0-79 ./build/release/hypersync benchmark-data \
  --source nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5 \
  --meta-reader-threads 64 --metadata-async-depth 256 \
  --data-reader-threads 256 --data-outstanding-requests 2 \
  --max-files-queued 1048576 --data-buffer-slots 12288 --data-queue-depth 12288
```

Optimized profile retest, 2026-05-15
After applying the full optimized runtime profile permanently on transfer1
(MTU 9000, rings 8192/8192, RPS flow table 262144, per-queue flow count 32768,
TCPMSS rules removed, sunrpc max slots 65536, BDI read-ahead 16384 KiB), the
same benchmark was rerun with lower data-reader counts and different per-reader
async windows. All runs used `taskset -c 0-79`, 64 metadata threads, metadata
async depth 256, and 30 second duration unless noted.

```text
data readers  window  final Gbit/s  avg read latency  failures  notes
64            1       192.09        2.58 ms           0         lowest latency near line rate
96            1       194.32        3.57 ms           0         best 30s final throughput
128           1       193.85        4.65 ms           0         similar throughput, more latency
192           1       191.69        7.99 ms           0         no throughput gain
256           1       192.06        10.60 ms          0         no throughput gain
64            2       194.18        5.25 ms           0         best low-thread windowed 30s run
96            2       193.08        7.88 ms           0         no gain over 96/window 1
128           2       191.99        8.94 ms           0         no gain
192           2       190.24        15.85 ms          0         too much queueing
256           2       187.22        20.94 ms          0         too much queueing
128           3       191.10        15.46 ms          0         too much queueing
192           3       192.24        22.79 ms          0         too much queueing
```

Longer 90 second confirmation:

```text
data readers  window  final Gbit/s  90s hot Gbit/s  avg read latency  failures
96            1       180.82        182.57          4.23 ms           0
64            2       190.97        191.96          5.24 ms           0
```

Conclusion: after the full host tuning, the system reaches the 190-195 Gbit/s
class with far fewer data-reader workers than before. The best sustained
low-resource shape is now 64 data readers with `data_outstanding_requests=2`.
The 96-reader/window-1 shape is attractive for lowest latency and strong short
runs, but it settled lower in the 90 second confirmation. Increasing readers
beyond 128 or increasing async window beyond 2 mostly raises queueing latency
without improving throughput.

Current recommended data-reader benchmark shape:

```bash
taskset -c 0-79 ./build/release/hypersync benchmark-data \
  --source nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5 \
  --meta-reader-threads 64 --metadata-async-depth 256 \
  --data-reader-threads 64 --data-outstanding-requests 2 \
  --max-files-queued 1048576 --data-buffer-slots 12288 --data-queue-depth 12288
```

Low-reader-count sweep, 2026-05-15
Same optimized transfer1 runtime profile and benchmark shape, with only the
data-reader worker count and async window changed:

```text
data readers  window  final Gbit/s  avg read latency  failures
4             1       28.53         1.08 ms           0
4             2       19.54         3.02 ms           0
8             1       93.62         0.64 ms           0
8             2       64.50         1.77 ms           0
16            1       42.71         2.98 ms           0
16            2       37.27         5.47 ms           0
32            1       65.40         3.91 ms           0
32            2       106.52        3.83 ms           0
```

These short runs are more sensitive to which source folders are sampled than the
64+ reader sweeps, but they still show the shape clearly: below 64 readers the
pipeline does not reliably fill the 200 Gbit/s NIC. A deeper per-reader window
does not compensate consistently at very low reader counts; the reliable
settings remain 64 readers/window 2 or 96 readers/window 1 depending on whether
we prefer sustained throughput or slightly lower latency.

Small-file-only data-read sweep, 2026-05-15
Source folder:
`nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/catbear/run_20260218_042836/talking-head`
with 50,000 files and 1,355,189,671 bytes total, average file size about
27 KiB. Runs used the optimized transfer1 profile, `taskset -c 0-79`,
non-recursive scan, one metadata thread, and metadata async depth 16.

```text
data readers  window  elapsed s  files/s  Gbit/s  avg read latency  opens
32            1       14.49      3,451    0.75    4.58 ms           0
64            1       10.76      4,648    1.01    2.58 ms           0
96            1        8.07      6,199    1.34    1.96 ms           0
128           1       10.04      4,980    1.08    2.02 ms           0
64            2       15.65      3,194    0.69    2.13 ms           50,000
96            2       10.51      4,759    1.03    1.60 ms           50,000
128           2        9.93      5,036    1.09    2.24 ms           50,000
128           4       10.40      4,810    1.04    5.20 ms           50,000
```

Interpretation: the best current small-file result is 96 readers/window 1,
about 6.2K files/s and 1.34 Gbit/s. This remains operation-rate limited, not
bandwidth limited. A critical implementation detail is visible in the telemetry:
window 1 used the raw READDIRPLUS handle path with zero async opens/closes, while
windowed small-file modes currently go through the high-level open/read/close
path and perform 50,000 opens. Before drawing conclusions about async windows
for small files, the windowed small-file path should be refactored to use the raw
NFSv3 file handles as well.

Small-file raw-window integration update, 2026-05-15
Changes tested on transfer1:

- Windowed small-file reads now use `FileSpec::nfs_handle` and raw
  `rpc_nfs_read_async`; the high-level open/read/close path is fallback only.
- Packed-small-file reads now also use raw handles when available, so packed
  batches avoid per-file open/close.
- The benchmark file provider refills a thread-local batch of 128 `FileSpec`
  records from the shared queue to reduce shared-queue mutex pressure.

Same source folder and host tuning as above. These runs confirm the raw path is
used in all tested modes:

```text
mode        meta threads  data readers  window  elapsed s  files/s  Gbit/s  avg read latency  opens
raw         1             96            1        4.53       11,047   2.40    2.85 ms           0
raw         1             96            2        5.03        9,949   2.16    6.06 ms           0
raw         1             128           2        4.45       11,226   2.43    5.52 ms           0
raw         1             96            4        7.46        6,702   1.45    8.84 ms           0
packed      1             96            1        4.70       10,641   2.31    0.97 ms           0
packed      1             128           1        5.44        9,191   1.99    1.01 ms           0
packed      1             128           2        8.05        6,209   1.35    4.40 ms           0
raw         4             96            1        4.88       10,237   2.22    1.01 ms           0
raw         4             128           1        5.94        8,411   1.82    1.03 ms           0
packed      4             96            1        6.16        8,118   1.76    1.14 ms           0
packed      4             128           1        5.86        8,529   1.85    1.08 ms           0
```

A 1 second stats-interval run showed the real remaining integration bottleneck:
for this huge flat folder, the data reader stayed idle for about four seconds
while metadata discovery built the full 50,000-entry `FlatFolderScanBatch`.
Only after that did `files_found` jump to 50,000 and data reads begin. At that
point 25,945 files and 689,725,162 bytes were read in the next one-second
sample, with a 1.01 ms average raw READ latency and zero opens.

Conclusion: raw small-file data reads are now correctly zero-open, but the
scanner still emits a whole flat folder as one batch. Very large flat folders
therefore create a start-up bubble that dominates short small-file benchmarks.
The next required fix is to make metadata scanning stream partial READDIRPLUS
pages downstream for data-read/hash/sync pipelines, while preserving whole-folder
batches for diff/checker modes that require complete flat-folder comparison.

Streaming READDIRPLUS page tests, 2026-05-15
The data-read benchmark now uses `scan_flat_folders_streaming`, which emits
READDIRPLUS page batches with `complete=false` and sends a final EOF batch with
`complete=true`. Diff/checker paths continue to use complete flat-folder batches.
The page size is configurable through `jobs.nfs_meta_reader.readdirplus_page_bytes`
and, for `benchmark-data`, `--readdirplus-page-bytes`; benchmark output records
the effective `readdirplus_page_bytes` value.

Same 50,000-file source, 96 data readers, window 1, one metadata reader:

```text
READDIRPLUS page  elapsed s  files/s  Gbit/s  startup/feed behavior
512 KiB           2.26       22,136   4.80    first page effectively held most/all folder records
256 KiB           2.08       24,049   5.21    best total time; active second read 49,441 files
128 KiB           4.72       10,599   2.30    progressive feed, but too many metadata RPCs
64 KiB            5.10        9,807   2.13    progressive feed, too chatty
```

With the selected 256 KiB page size, the second one-second sample had already
read 49,441 files and 1,339,397,703 bytes, with zero opens/closes and 1.08 ms
average raw READ latency. The remaining blended gap versus active throughput is
mostly the first READDIRPLUS-page startup and the fact that this specific folder
contains only 1.36 GB of data, so startup cost dominates the full run average.

Sustained small-file-only read, 2026-05-15
Source:
`nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
with recursive scan and `--max-file-size-bytes 131072`, so only files at or
below 128 KiB were enqueued to the data reader. Settings:

```text
meta_reader_threads=64
metadata_async_depth=256
readdirplus_page_bytes=262144
data_reader_threads=96
data_outstanding_requests=1
small_file_async_window=1
max_files_queued=1048576
data_buffer_slots=12288
data_queue_depth=12288
taskset=0-79
```

Selected telemetry:

```text
elapsed  files_read  files/s avg from start  bytes_read     Gbit/s avg  avg read latency  queued_files
30s      1,447,462   48,248                  77.4 GB        20.65       1.89 ms           1,048,576
60s      3,062,320   51,038                  193.1 GB       25.74       1.79 ms           1,048,576
90s      4,655,014   51,722                  285.1 GB       25.34       1.76 ms           1,048,576
120s     6,330,945   52,757                  355.0 GB       23.67       1.72 ms           1,048,576
180s     8,651,015   48,061                  438.0 GB       19.47       1.89 ms           0
```

Middle sustained rate from 30s to 120s:

```text
4,883,483 files / 90s = 54,261 files/s
277.6 GB / 90s = 24.68 Gbit/s
```

Telemetry stayed zero-open/zero-close throughout. The file queue remained full
from 30s through 170s, so this was no longer metadata starvation; during the
middle of the run, the data reader/libnfs/backend path was the limiter. The last
minute tailed down as the timer stopped new metadata work and the queued small
files drained.

Split small/large stream smoke, 2026-05-15
------------------------------------------

Purpose: verify the new classifier and independent small/large reader streams
under real libnfs before wiring priority transport into production sync.

Source:
`nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`

Settings:

```text
taskset                         0-79
meta_reader_threads             64
metadata_async_depth            256
readdirplus_page_bytes          262144
split_small_large               true
small_file_threshold_bytes      131072
small_data_reader_threads       96
small_file_async_window         1
large_data_reader_threads       32
large_data_outstanding_requests 2
max_files_queued                1048576 per stream
data_buffer_slots               24576
data_queue_depth                12288 per stream
duration                        60 s useful sample, outer timeout during drain
```

Useful 60-second sample:

```text
elapsed_s  Gbit/s  bytes_read  small_files_read  large_files_read  small_bytes  large_bytes  queued_small  queued_large
10         61.08   76.36 GB    138,196           33,526            5.45 GB      70.91 GB     0             1,048,576
20         53.69   134.23 GB   144,995           59,631            5.79 GB      128.44 GB    0             1,048,576
30         53.73   201.50 GB   159,606           88,434            6.76 GB      194.74 GB    0             1,048,576
40         59.08   295.43 GB   160,162           106,065           6.77 GB      288.65 GB    0             1,048,576
50         60.67   379.17 GB   161,019           118,319           6.84 GB      372.33 GB    0             1,048,576
60         60.12   450.90 GB   183,531           137,465           8.58 GB      442.32 GB    0             0
```

Interpretation: this tree segment was large-file dominated, so bandwidth came
mostly from the large stream. The small queue stayed drained while large work
backlogged, which confirms the stream split and small-first priority shape. The
run is a smoke test, not a final tuning result.

READDIRPLUS feed telemetry, 2026-05-16
--------------------------------------

Purpose: explain why the zero-open small-file reader sometimes fell below the
previous 50K+ files/s result even though raw READ latency stayed near 1 ms.

Build/deploy:

```text
build host      transfer1
deploy path     /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-transfer1-20260516T181124Z
hypersync git   dca0bdfe72ba5482100c8c40deb553a4e7b424b3
piper git       f1aecb4b5025b410082c572e894ba55ac95abea8
libnfs          5.0.2
```

Source:
`/catbear/run_20260218_042836/talking-head`, 50,000 files, same flat-folder
small-file test used for earlier reader tuning.

Telemetry added:

```text
readdirplus_pages
readdirplus_entries/files/directories
readdirplus_avg_entries_per_page
readdirplus_avg_page_latency_ms / max
readdirplus_avg_decode_ms / max
optional HYPERSYNC_NFS_PAGE_TRACE=1 per-page endpoint/folder/page line
```

Key result with `readdirplus_page_bytes=262144`, one metadata reader, 96 data
readers:

```text
files_read                         50,000
elapsed_s                          4.57
files_per_second                   10,939
async_read_avg_latency_ms          1.64
async_open_completed               0
readdirplus_pages                  84
readdirplus_avg_entries_per_page   ~595
readdirplus_avg_page_latency_ms    ~34 ms
readdirplus_max_page_latency_ms    ~50 ms
readdirplus_avg_decode_ms          ~0.1 ms
queued_files                       ~0 during samples
```

Interpretation: the data path was not the limiter. The reader stayed zero-open,
and data-read latency remained near 1 ms. The bottleneck was sequential
READDIRPLUS page supply for this one wide folder: most page time was server/RPC
latency, not client decode.

Direct endpoint sweep for the same flat folder, same settings:

```text
endpoint        files/s   Gbit/s   elapsed_s
172.27.255.18   15,068    3.27     3.32
172.27.255.19   24,966    5.41     2.00
172.27.255.20   28,573    6.20     1.75
172.27.255.21   11,062    2.40     4.52
172.27.255.22   27,972    6.07     1.79
172.27.255.23   10,822    2.35     4.62
172.27.255.24   10,667    2.31     4.69
172.27.255.25   24,698    5.36     2.02
172.27.255.26   12,698    2.75     3.94
172.27.255.27   10,485    2.27     4.77
172.27.255.28   11,782    2.55     4.24
172.27.255.29    9,706    2.10     5.15
172.27.255.30   10,607    2.30     4.71
172.27.255.31   10,454    2.27     4.78
172.27.255.32    9,614    2.08     5.20
172.27.255.33   10,409    2.26     4.80
```

Conclusion: endpoint/backend choice changes the flat-folder feed rate by about
3x while raw read latency stays close to 1 ms. For very wide folders, the
metadata stream is sequential by cookie and tied to the chosen endpoint/backend
owner. The next optimization should choose or rotate metadata endpoints for
wide-folder streaming, and for large recursive scans record per-endpoint
READDIRPLUS latency so slow metadata lanes are visible.

Small-file reservoir retest, 2026-05-16
---------------------------------------

Purpose: verify that more parallel metadata scanners plus a deeper file-handle
reservoir can hide READDIRPLUS endpoint/backend jitter and keep the small-file
reader fed.

Build/deploy:

```text
build host      transfer1
deploy path     /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-transfer1-20260516T181839Z
hypersync git   f555d3027b9b3d75a1852a87bcefb65e5845a7ce
piper git       f1aecb4b5025b410082c572e894ba55ac95abea8
```

Common settings:

```text
source                    nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
taskset                   0-79
metadata_async_depth      256
readdirplus_page_bytes    262144
data_reader_threads       96
data_outstanding_requests 1
small_file_async_window   1
max_file_size_bytes       131072
max_files_queued          5000000
data_buffer_slots         12288
data_queue_depth          12288
duration                  90 seconds useful sample, then drain/tail
```

Results:

```text
meta threads  30s files/s  60s files/s  90s files/s  queued_files behavior
64            37,778       31,366       34,356       mostly empty
96            46,687       43,131       42,854       built ~459K reservoir, drained by 90s
128           50,814       54,067       55,160       filled 5M reservoir through 60s
160           50,032       52,624       53,298       filled 5M reservoir through 60s
```

Recommendation for transfer1 small-file read benchmark profile:

```text
meta_reader_threads       128
metadata_async_depth      256
readdirplus_page_bytes    262144
data_reader_threads       96
data_outstanding_requests 1
small_file_async_window   1
max_files_queued          5000000
```

Interpretation: the deeper reservoir and 128 metadata scanners restored the old
50K+ files/s target. Increasing to 160 did not materially improve throughput and
raised metadata page latency, so 128 is the better default for this profile.
This validates the asymmetric reservoir approach: let fast metadata branches
stockpile file handles so slow READDIRPLUS branches do not immediately starve
the data readers.

Dual scanner split read test, 2026-05-16
----------------------------------------

Purpose: test independent small/large metadata scanner fleets. This avoids
coupling where one shared scanner pauses because one downstream reservoir is
full while the other stream still needs records.

Build/deploy:

```text
build host      transfer1
deploy path     /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-transfer1-20260516T200710Z
hypersync git   db08cb5
piper git       f1aecb4
```

Common settings:

```text
source                         nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
taskset                        0-79
metadata_async_depth           256
readdirplus_page_bytes         262144
small_file_threshold_bytes     131072
small_data_reader_threads      96
large_data_reader_threads      64
large_data_outstanding_requests 2
small_file_async_window        1
small_max_files_queued         5000000
large_max_files_queued         50000
data_buffer_slots              24576
data_queue_depth               12288
```

Useful interval results, before shutdown/tail dilution:

```text
mode / scanner threads     sample  total Gbit/s  small files/s  large Gbit/s  queued small  queued large
single scanner 128         60s     196.8         680            196.6         0             50000
single scanner 128         90s     197.2         477            197.1         0             0
dual small64 / large16     60s     194.0         21639          179.6         3196527       50000
dual small64 / large16     90s     194.0         22202          180.2         4226640       0
dual small96 / large16     30s     191.1         33668          179.8         5000000       50000
dual small96 / large16     60s     194.5         32726          183.7         5000000       50000
dual small96 / large16     90s     195.7         32326          184.9         0             0
dual small128 / large8     30s     188.2         27708          173.3         5000000       50000
dual small128 / large8     60s     193.0         29006          178.3         0             0
```

Interpretation:

- The dual-scanner design works: the small scanner can stockpile millions of
  small file handles while the large scanner keeps the large reservoir full.
- `small96 / large16` is the best measured mixed profile so far. It preserved
  near-line-rate total bandwidth while raising small-file processing from
  hundreds/s in the single-scanner mixed run to about 32K/s.
- `small128 / large8` did not improve the mix. It put more pressure on small
  metadata and reduced large bandwidth, so fewer large metadata threads went
  too far for this source shape.
- `small64 / large16` is viable when the goal is to minimize metadata threads,
  but it did not feed small readers as aggressively as `small96 / large16`.

Current recommendation for mixed small/large read tests on transfer1:

```text
--dual-scan-small-large
--small-meta-reader-threads 96
--large-meta-reader-threads 16
--small-data-reader-threads 96
--large-data-reader-threads 64
--large-data-outstanding-requests 2
--small-max-files-queued 5000000
--large-max-files-queued 50000
```

Note: the benchmark stop timer currently clears the data queues at
`max_duration_seconds`, so post-stop/tail samples show queue depth dropping to
zero and should not be used as steady-state performance. Use the last interval
before stop for throughput comparisons.

3-scanner recon split read test, 2026-05-16
-------------------------------------------

Purpose: validate a low-priority background recon scanner running concurrently
with independent small and large production scanners. The recon scanner updates
only a statistical accumulator and discards raw handles immediately.

Build/deploy:

```text
build host      transfer1
deploy path     /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-transfer1-20260516T211558Z
hypersync git   9a00fcc
piper git       f1aecb4
```

Common settings:

```text
source                         nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
taskset                        0-79
mode                           --background-recon-scan --morph-large-readers-to-small
small_meta_reader_threads      96
large_meta_reader_threads      16
recon_meta_reader_threads      1
recon_metadata_async_depth     1
recon_page_sleep_us            5000
metadata_async_depth           256
readdirplus_page_bytes         262144
small_data_reader_threads      96
large_data_reader_threads      64
large_data_outstanding_requests 2
small_file_async_window        1
small_max_files_queued         5000000
large_max_files_queued         50000
data_buffer_slots              24576
data_queue_depth               12288
```

Useful interval results:

```text
sample  total Gbit/s  small files/s  large Gbit/s  queued small  queued large  recon files  recon done
30s     191.3         31,440         172.0         4,260,672     50,000        120,068      false
60s     194.6         30,854         176.6         4,371,136     50,000        120,068      false
90s     195.7         30,237         178.5         0             0             123,333      false
```

Interpretation:

- The third recon lane works and remains bounded: it preserved no handles and
  advanced with one async request plus a 5 ms page sleep.
- The production path stayed close to the previous dual-scanner result:
  194-196 Gbit/s steady state with roughly 30K small files/s.
- Recon did not complete before the 90s stop point in the useful interval,
  which is expected for the throttled profile. That confirms it is not sprinting
  and stealing production scanner/data-reader capacity.
- The large-reader morph path is enabled in this profile. Route-aware accounting
  records bytes/files as small or large based on the queue that supplied the
  file, so morphed readers do not corrupt small/large stats.

Current 3-scanner test recommendation:

```text
--background-recon-scan
--morph-large-readers-to-small
--recon-meta-reader-threads 1
--recon-metadata-async-depth 1
--recon-page-sleep-us 5000
--small-meta-reader-threads 96
--large-meta-reader-threads 16
```

ETA bucket-priority split read test, 2026-05-16/17
-------------------------------------------------

Purpose: verify live small/large bucket priority using rolling files/sec and
ETA. The controller reports small and large rates, remaining work, ETA, active
reader counts, and the percent of large-reader pulls temporarily borrowed for
small-file work.

Build/deploy:

```text
build host      transfer1
deploy path     /mnt/local-nvme/wsync-codex/deployments/hypersync-linux-x86_64-transfer1-20260517T013315Z
hypersync git   ad90875
piper git       f1aecb4
```

Common settings:

```text
source                         nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5
taskset                        0-79
mode                           --background-recon-scan --bucket-priority
small_file_threshold_bytes     131072
small_meta_reader_threads      96
large_meta_reader_threads      16
recon_meta_reader_threads      1
recon_metadata_async_depth     1
recon_page_sleep_us            5000
metadata_async_depth           256
readdirplus_page_bytes         262144
small_data_reader_threads      128
large_data_reader_threads      64
large_data_outstanding_requests 2
small_file_async_window        1
small_max_files_queued         5000000
large_max_files_queued         50000
data_buffer_slots              24576
data_queue_depth               12288
```

Useful interval results:

```text
sample  total Gbit/s  small files/s  large files/s  large->small priority  queued small  queued large
15s     149.0         50,361         1,487          57%                    4,203,282     50,000
30s     164.0         55,198         1,220          48%                    4,246,464     50,000
45s     175.1         50,469           845          36%                    4,705,216     50,000
60s     180.9         47,995           634           0%                    4,095,936     50,000
75s     184.2         46,428           519           0%                    4,578,880     50,000
90s     186.5         45,421           433           0%                    0             0
```

Interpretation:

- The controller reacted within the first 10 seconds. When small ETA was worse
  than large ETA, `large_reader_small_priority_percent` rose to 57%, borrowing
  large-reader capacity for the small reservoir.
- As the rolling ETAs converged, the borrow percentage decayed back to zero,
  restoring large-reader priority.
- The run exposed both bucket rates in the normal stats line:
  `small_files_per_second`, `large_files_per_second`,
  `small_gigabits_per_second`, and `large_gigabits_per_second`.
- The best interval sustained more than 55K small files/s while still pushing
  large-file bandwidth. Total bandwidth reached 186.5 Gbit/s by the last useful
  interval before the stop/tail phase.
- Post-stop samples are not throughput evidence because the benchmark stop
  timer clears queues. Use the last interval before stop.
