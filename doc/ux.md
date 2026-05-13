# WSync User Experience

This document describes how users should install, configure, and run WSync. It focuses on the desired user experience and command behavior.

## Experience Principles

WSync should feel like one coherent NFS tool suite, not a collection of unrelated utilities.

Users should be able to:
- Install one executable on a server and run useful commands immediately.
- Point the tool at NFS URLs without mounting NFS through the kernel.
- Use local paths for testing.
- Start with simple commands.
- Add configuration only when they need scale, repeatability, or advanced behavior.
- See progress during long runs.
- Stop long runs gracefully by timer.
- Get clear final summaries that can be pasted into logs or scripts.
- Use the same source and target syntax across scan, hash, perf, diff, copy, and sync.

## Installation Experience

### Single-Host Installation

The preferred installation experience is:

```bash
wsync --version
```

The command should print the version and exit successfully.

For builds with NFS support, users should not need to mount the NFS export with the OS. They should be able to pass an NFS URL directly:

```bash
wsync nfs scan --source nfs://server/export/path --output scan.csv
```

### Portable Binary

The desired production experience is one deployable executable per target platform.

The executable should either:
- include everything needed at runtime, or
- clearly report missing runtime dependencies before starting work.

When optional features such as Parquet output are unavailable, the tool should say so directly and suggest a supported output format.

### Portable Linux Bundle

The current practical Linux deployment unit is a relocatable flat folder. The
folder intentionally avoids `bin/`, `lib/`, `config/`, and `doc/` subfolders
because there are only a few files and the user should immediately see the
launcher, binary, config, and runtime libraries.

```text
hypersync-linux-x86_64/
  hypersync
  hypersync.bin
  default.yaml
  libduckdb.so*
  libstdc++.so*, libgcc_s.so*   # only if needed by bundled DuckDB
  README.txt
  manifest.txt
  checksums.sha256
```

Users should be able to unpack it on a Linux server and run:

```bash
./hypersync --version
./hypersync scan --source nfs://server/export/path --output scan.parquet --output-format parquet
```

The `hypersync` launcher sets `LD_LIBRARY_PATH` to its own directory before
starting `hypersync.bin`, so the copied `.so` files are used without installing
anything globally. The bundle is produced by
`hypersync/deploy/package-linux.sh` and should be built on Linux so the staged
binary and `.so` files match the target platform.

### New Server Download And Run

A user setting up a new Linux server should not need to understand the build
system, DuckDB, libnfs, checksums, or dynamic linker details. The expected flow
is one command that downloads the installer script and runs it:

Example desired first-run flow:

```bash
curl -fsSL https://raw.githubusercontent.com/ai-app-user/hypersync/main/deploy/install-hypersync.sh | sh
```

The installer should:
- download the release archive for the current architecture
- download and verify the `.sha256` checksum
- unpack the flat bundle into `$HOME/hypersync` by default
- run `$HOME/hypersync/hypersync --version`
- print the exact command to use next

For a private GitHub release, users can export a token once and still run one
pipeline command:

```bash
export GITHUB_TOKEN=...
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://raw.githubusercontent.com/ai-app-user/hypersync/main/deploy/install-hypersync.sh | sh
```

After install, the smoke test is:

```bash
$HOME/hypersync/hypersync scan --source /tmp --output /tmp/hypersync-smoke.csv --output-format csv --max-duration-seconds 5
```

For a direct NFS smoke test:

```bash
$HOME/hypersync/hypersync scan \
  --source nfs://172.27.255.2/volumes/example/data \
  --output /mnt/local-nvme/hypersync-smoke.parquet \
  --output-format parquet \
  --max-duration-seconds 30 \
  --stats-interval-seconds 5
```

The command output should make the deployment state obvious:
- `--version` prints the Hypersync version and exits.
- A future `doctor` or extended `--version` output should report whether libnfs,
  DuckDB/Parquet, OpenSSL, and fast hash support are available.
- If a bundled library cannot be loaded, the wrapper or executable should fail
  before starting work and show the missing library name.
- If Parquet support is missing, scan/hash commands should reject
  `--output-format parquet` with a clear message instead of silently falling
  back to another format.

The user should be able to add Hypersync to the shell path without moving the
bundle internals:

```bash
export PATH="$HOME/hypersync:$PATH"
hypersync --version
```

When a server has no internet access, the same archive should be copied with
`scp`, `rsync`, or the site's artifact tool and unpacked in the same way. The
runtime experience is identical because all required non-system runtime
libraries live in the same folder as the `hypersync` launcher.

The desired bundle should statically link the app C++ runtime and libnfs, so
users do not need to install libnfs or compiler runtime packages. Parquet
support currently ships with bundled `libduckdb.so`; if that shared library
needs `libstdc++.so.6` or `libgcc_s.so.1`, those must be bundled too. The exact
linker view remains in `manifest.txt`.

### Local Test Installation

Users should be able to test the tool without NFS:

```bash
wsync nfs scan --source /tmp/source-tree --output scan.csv
```

Local paths and NFS URLs should use the same command structure.

## Source and Target Syntax

The same path syntax should be accepted everywhere:

```bash
/local/path
nfs://server/export
nfs://server/export/sub/path
nfs://172.27.255.2-172.27.255.17/export/sub/path
```

When the NFS server portion is an IPv4 range, every direct libnfs connection
should independently choose one IP from the range. This lets users mirror
kernel mount setups that spread reads across multiple remote NFS front ends.

Commands with one input should use `--source`.

Commands with two endpoints should use `--source` and `--target`.

Examples:

```bash
wsync nfs scan --source nfs://server/export --output scan.csv
wsync nfs diff --source nfs://server-a/export --target nfs://server-b/export
wsync nfs copy --source nfs://server-a/export/a --target nfs://server-b/export/b
```

## Configuration Experience

Users should be able to run without a config file for simple use.

For repeatable runs, users should be able to provide YAML:

```bash
wsync --config prod.yaml nfs perf read --source nfs://server/export
```

Command-line options should override config values.

The config file should support:
- source and target endpoints
- output format and output paths
- recursion settings
- concurrency settings
- queue and read-ahead limits
- timer limits
- retry policy
- verification policy
- error policy
- multi-server worker lists

Synthetic buffer generation should expose a small set of understandable data
patterns:
- `zero` for maximum-fill and compression baselines.
- `fast_text` for printable text-like data.
- `xoshiro256` for fast deterministic random-looking data that should not
  compress well when `compression_ratio` is `1.0`.

When a generator accepts `compression_ratio`, users provide it as
original/compressed. For example, `1.4` means approximately `7 / 1.4 = 5`
compressed bytes for 7 original bytes. The actual result depends on the
compressor, so users should treat it as an approximate workload target.

Configuration should be grouped by tool and shared defaults.

Example desired shape:

```yaml
defaults:
  recursive: true
  stats_interval_seconds: 5

nfs:
  source: nfs://source-server/export
  target: nfs://target-server/export

scan:
  output: scan.parquet
  format: parquet
  records: all

perf:
  duration_seconds: 30
  metadata_threads: 1
  metadata_async_depth: 16
  data_reader_threads: 64
  data_outstanding_requests: 32
```

## Runtime Experience

### Progress Output

Long-running commands should print periodic stats every few seconds.

Stats should include the values users care about for the active command.

For scan:
- records per second
- files found
- folders found
- logical size found
- elapsed time

For data read performance:
- bytes per second
- Gbit/s
- bytes read
- files read
- files found
- folders found
- queued files
- elapsed time

For hash:
- files found
- folders found
- files hashed
- files failed
- bytes hashed
- read bytes per second
- read Gbit/s
- bytes per second
- hash algorithm
- elapsed time

For copy and sync:
- files planned
- files copied
- files skipped
- files failed
- bytes copied
- current throughput
- elapsed time

### Live Status Query

For detailed pipeline visibility, users can start supported long-running
commands with a local status socket:

```bash
wsync scan --source nfs://server/export --output scan.parquet --status-socket /tmp/wsync-scan.sock
```

From another shell, users can request the current status:

```bash
wsync status --socket /tmp/wsync-scan.sock
```

The output should show each registered job with processed buffers or records,
current rate, byte throughput, worker count, and useful details. It should also
show each registered queue with depth, capacity, fullness percentage, push/pop
counts when available, and whether the queue is closed.

### Final Summary

Every command should print a final summary in stable key-value form.

Example:

```text
data_benchmark files_found=866 folders_found=165049 files_read=302 files_failed=0 bytes_read=1399897936252 gigabits_per_second=185.015 elapsed_s=60.5311
```

The summary should be easy to parse from shell scripts.

### Graceful Timer Stop

Long-running commands should support a timer:

```bash
wsync nfs perf read --source nfs://server/export --max-duration-seconds 30
```

When the timer expires, the command should stop accepting new work, finish or safely stop active work, print final stats, and exit normally.

### Error Handling

The default experience for inventory and performance tools should be best-effort:
- inaccessible files are counted
- errors are reported
- the run continues when possible

Copy and sync should be more conservative:
- failed files must be reported
- destructive actions require explicit options
- strict mode should fail the command on first critical error

## Command Experience

The final user-facing command surface should be organized around NFS tasks.

### Scan

Purpose: inventory files and folders.

Desired command:

```bash
wsync nfs scan \
  --source nfs://server/export \
  --output scan.parquet \
  --format parquet \
  --records all
```

Common options:
- `--source`
- `--output`
- `--format text|csv|parquet`
- `--records all|files|folders`
- `--non-recursive`
- `--max-duration-seconds`
- `--stats-interval-seconds`

Current equivalent:

```bash
hypersync scan \
  --source nfs://server/export \
  --output scan.parquet \
  --output-format parquet \
  --records all \
  --meta-reader-threads 96 \
  --metadata-async-depth 128 \
  --record-buffer-slots 10000000 \
  --max-duration-seconds 600 \
  --stats-interval-seconds 5
```

The current scan command is metadata-only: it does not read file data and does
not calculate hashes. It prints `metadata_stats` progress lines while running
and writes run metadata columns (`scan_run_id`, start time, source, settings)
with every output record.

### Hash

Purpose: calculate file content hashes.

Desired command:

```bash
wsync nfs hash \
  --source nfs://server/export \
  --output hashes.parquet \
  --format parquet
```

Common options:
- `--source`
- `--input-scan`
- `--output`
- `--format text|csv|parquet`
- `--hash-algorithm md5|sha256|xxh64|xxh3_64|xxh3_128`
- `--hash-mode file|blocks`
- `--hash-block-size`
- `--records all|files|folders`
- `--non-recursive`
- `--meta-reader-threads`
- `--metadata-async-depth`
- `--data-reader-threads`
- `--data-outstanding-requests`
- `--hash-threads`
- `--max-files-queued`
- `--max-hash-chunks-queued`
- `--max-duration-seconds`
- `--stats-interval-seconds`

Current equivalent:

```bash
hypersync hash \
  --source nfs://server/export \
  --output hashes.csv \
  --output-format csv \
  --records all \
  --hash sha256
```

For full-file mode, hash output uses the shared inventory columns plus `hash_algorithm` and `content_hash` on file rows. For block mode, file rows include `hash_block_size`, `hash_block_count`, `block_hash_algorithm`, and ordered `block_hashes`. Folder rows include direct child file count and direct child logical size.

Full-file `md5` and `sha256` output is compatible with Linux `md5sum` and `sha256sum` because the hasher feeds file bytes to the digest in exact file-offset order, even if the NFS backend completes reads out of order. Those algorithms use OpenSSL acceleration when runtime `libcrypto` is available, with built-in fallback otherwise. `xxh64`, `xxh3_64`, and `xxh3_128` are faster non-cryptographic inventory hashes. Block mode is intentionally different: each block hash can be verified against the same byte range from matching tools, but the ordered block list is not the same value as a whole-file checksum. Data payloads that cross pipeline stages are moved by preallocated buffer-slot ownership; borrowed backend buffers are copied into owned slots at the backend boundary when asynchronous downstream processing is required.

Hash runtime output reports NFS read bytes/s and Gbit/s using the read-stage elapsed time, plus total pipeline elapsed time so users can see whether hashing is draining after reads stop.

### Generate

Purpose: create synthetic NFS file trees.

Desired command:

```bash
wsync nfs generate \
  --target nfs://server/export/test-tree \
  --profile mixed \
  --files 1000000 \
  --seed 1234
```

Common options:
- `--target`
- `--profile`
- `--files`
- `--folders`
- `--depth`
- `--size-profile`
- `--seed`
- `--dry-run`

### Performance

Purpose: measure NFS metadata, data read, data write, and mixed workload performance.

Desired metadata command:

```bash
wsync nfs perf metadata \
  --source nfs://server/export \
  --metadata-threads 64 \
  --metadata-async-depth 64 \
  --max-duration-seconds 30
```

Desired data read command:

```bash
wsync nfs perf read \
  --source nfs://server/export \
  --metadata-threads 1 \
  --metadata-async-depth 16 \
  --data-reader-threads 64 \
  --data-outstanding-requests 32 \
  --max-files-queued 512 \
  --max-duration-seconds 30
```

Current metadata equivalent:

```bash
hypersync benchmark-meta \
  --source nfs://server/export \
  --metadata-stats-discarder \
  --meta-reader-threads 64 \
  --metadata-async-depth 64 \
  --max-duration-seconds 30
```

Current data read equivalent:

```bash
hypersync benchmark-data \
  --source nfs://server/export \
  --meta-reader-threads 1 \
  --metadata-async-depth 16 \
  --data-reader-threads 64 \
  --data-outstanding-requests 32 \
  --max-files-queued 512 \
  --data-buffer-slots 8192 \
  --data-queue-depth 4096 \
  --data-copy-mode copy \
  --max-duration-seconds 30
```

Use `--data-copy-mode no-copy` only for reader-to-discarder performance isolation. That mode still reads from NFS and transfers buffer handles through the pipeline, but payload bytes in those buffers are not valid for hashing, writing, or verification.

Current data read plus hash equivalent:

```bash
hypersync benchmark-data-hash \
  --source nfs://server/export \
  --hash xxh64 \
  --meta-reader-threads 1 \
  --metadata-async-depth 4 \
  --data-reader-threads 64 \
  --data-outstanding-requests 32 \
  --hash-threads 128 \
  --hash-work-factor 4 \
  --data-buffer-slots 4096 \
  --data-queue-depth 512 \
  --max-duration-seconds 30
```

This command measures the pipeline `NFS data reader -> data hasher -> discarder`.
It reports read throughput and hash throughput separately so users can see
whether hashing is slowing the reader. `--hash-work-factor` repeats the selected
per-buffer hash inside the independent hasher job. It is intended for performance
proofs where the hasher should burn more CPU without changing the amount of file
data read from NFS. For line-rate architecture checks, `--hash xxh64` with
`--hash-work-factor 4` is the current validated setting on transfer1.

Current hash speed checker:

```bash
hypersync benchmark-hash \
  --hash xxh64 \
  --threads 32 \
  --block-size 1048576 \
  --duration-seconds 10 \
  --min-gigabits-per-core 1.0
```

The hash speed checker does not read NFS. It measures raw CPU hashing throughput for the selected algorithm and reports total Gbit/s plus Gbit/s per active worker/core. When `--min-gigabits-per-core` is provided, the command exits non-zero if the measured per-core rate is below the threshold.

Current metadata writer benchmark:

```bash
hypersync benchmark-metadata-writer \
  --output /mnt/local-nvme/generated.parquet \
  --output-format parquet \
  --file-count 10000000 \
  --folder-count 100000 \
  --batch-size 131072 \
  --duckdb-memory-limit 32GB \
  --duckdb-threads 1 \
  --partitions 32 \
  --partition-mode processes
```

This command does not read NFS. It generates deterministic file and folder
metadata records, writes them through the shared metadata writer, and reports
record counts plus records per second.

When `--partitions` is greater than one, `--output` is treated as a dataset
directory and the command writes `part-00000.parquet`, `part-00001.parquet`,
and so on. `--partition-mode threads` keeps all writers in one process.
`--partition-mode processes` launches one process per partition and lets each
process generate its own benchmark records; it is the current high-throughput
DuckDB path. `--partition-mode transport-processes` keeps generation in the
parent process and sends metadata buffers over local transport to child writer
processes using 1MB metadata batches. This proves the final job topology and is
the path to use for scanner-to-writer pipeline experiments. The resulting
directory can be queried with `read_parquet('/path/to/dataset/*.parquet')`.

For bottleneck analysis, the same command also supports non-writing modes:
`generate-discard`, `generate-hash-discard`, `pack-discard`, and
`transport-discard`. These isolate synthetic path generation, shard hashing,
1MB batch packing, and local transport before Parquet writer cost is added.

Current raw buffer transport benchmark:

```bash
wsync benchmark-transport \
  --transport none \
  --pattern xoshiro256 \
  --transports 16 \
  --buffers-per-transport 8192 \
  --buffer-size 1048576 \
  --pool-slots 256
```

This command runs `BufferGeneratorJob -> BufferSenderJob -> BufferReceiverJob
-> BufferDiscarderJob` for `unix` or `tcp`. With `--transport none`, it runs the
same-process baseline `BufferGeneratorJob -> BufQueue -> BufferDiscarderJob`.
Use it to separate queue/generator cost from stream transport cost, without
metadata formatting, shard routing, hashing, NFS, or Parquet writer cost.

Use `--shared-input` when measuring the final sender topology: one generator
pool and one generated-buffer queue feeding all sender sockets.

### Diff

Purpose: compare two NFS trees.

Current scan-CSV command:

```bash
hypersync diff \
  --source-scan source.csv \
  --target-scan target.csv \
  --compare time \
  --output diff.csv
```

Live metadata command:

```bash
hypersync diff \
  --source nfs://source-server/export \
  --target nfs://target-server/export \
  --compare time \
  --meta-reader-threads 32 \
  --metadata-async-depth 16 \
  --max-duration-seconds 30 \
  --output diff.csv
```

The live command compares one flat source folder at a time and reads the
matching flat target folder for that unit. It does not read file data unless a
future hash/content mode explicitly requests hashes. `--compare content` uses
recorded hashes when scan records contain hashes; live metadata-only diff falls
back to size plus mtime until a content-hash pipeline is enabled.

Common options:
- `--source`
- `--target`
- `--source-scan`
- `--target-scan`
- `--compare size|time|content`
- `--output`
- `--format text|csv|parquet`
- `--non-recursive`
- `--meta-reader-threads`
- `--metadata-async-depth`
- `--max-duration-seconds`

### Copy

Purpose: copy files from source to target.

Desired command:

```bash
wsync nfs copy \
  --source nfs://source-server/export/path \
  --target nfs://target-server/export/path \
  --verify hash
```

Copy should not delete target files that are absent from source.

Common options:
- `--source`
- `--target`
- `--verify none|metadata|hash`
- `--overwrite changed|always|never`
- `--dry-run`
- `--max-duration-seconds`

Current related commands:

```bash
hypersync receive --target nfs://target-server/export/path
hypersync sync --source nfs://source-server/export/path --host receiver-host
```

`send`, `sync`, and `copy` are current aliases for the sender side of the same
runtime pipeline. Operators should use `sync` in normal runbooks, while `send`
remains useful when describing the pipeline internals. The receiver can write to
a local folder or a writable `nfs://...` target.

The runtime send/receive path packs eligible tiny files into larger data
buffers automatically. Users still get per-file results and per-file
verification; packing is an internal transport optimization for
small-file-heavy trees.

### Sync

Purpose: make target match source.

Desired command:

```bash
wsync nfs sync \
  --source nfs://source-server/export/path \
  --target nfs://target-server/export/path \
  --delete-extra \
  --verify hash
```

Sync should require explicit confirmation or explicit options for destructive behavior.

Common options:
- `--source`
- `--target`
- `--delete-extra`
- `--delete-policy never|trash|delete`
- `--verify none|metadata|hash`
- `--dry-run`
- `--resume`

## Multi-Server Experience

Eventually users should be able to run one command that coordinates many servers.

Desired shape:

```bash
wsync --config cluster.yaml nfs sync
```

Example config shape:

```yaml
cluster:
  coordinator: transfer1
  sources:
    - host: source-worker-1
      endpoint: nfs://source-a/export
    - host: source-worker-2
      endpoint: nfs://source-b/export
  targets:
    - host: target-worker-1
      endpoint: nfs://target-a/export
    - host: target-worker-2
      endpoint: nfs://target-b/export
```

The user should see one aggregate progress stream and one final summary.

## Naming Transition

The current executable and commands use `hypersync` with commands such as `benchmark-meta` and `benchmark-data`.

The desired final UX is a task-oriented `wsync nfs ...` command surface.

Until the final CLI is implemented, documentation may show both:
- current command
- desired final command
