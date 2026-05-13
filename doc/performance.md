# WSync Performance Baselines

This document records observed performance for independent jobs and common job
pipelines. These numbers are expectations for engineering work, not portable CI
thresholds. Automated performance tests use small smoke workloads and only check
that benchmark commands run and report sane metrics.

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
