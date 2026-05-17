# WSync Codex Session Knowledge Base

Last updated: 2026-05-12 21:00 PDT

## Persistent Context Rules

- Base context folder: `/Users/agavrilov/src/codex/wsync/`
- `chat.md`: timestamped conversation history with both User and Codex messages.
- `kb.md`: durable facts, commands, paths, hosts, decisions, scripts, configs, and operational state.
- `scripts/`: reusable scripts created or modified for this session.
- If the user sends exactly `sync`, update `chat.md` and `kb.md` with the latest conversation and important facts.
- Do not dump full command output into `kb.md`; summarize important results.

## Local Repo

- Workspace root: `/Users/agavrilov/src/wsync`
- The workspace root is not currently an active git repo. Its previous monorepo `.git` was moved aside to:
  - `/Users/agavrilov/src/wsync/.wsync-monorepo-git-backup-20260512-132252`
  - Pointer file: `/Users/agavrilov/src/wsync/MONOREPO_GIT_BACKUP.txt`
- The workspace now contains three independent projects, each with its own local `.git` and GitHub repo:
  - `infra/`: common project bootstrap, GitHub auth, docs, and shared scripts.
  - `piper/`: reusable asynchronous pipeline infrastructure.
  - `hypersync/`: WSync app-specific implementation.
- The IDE may still show stale `aspipe/` paths; the intended reusable library name is `piper`.
- Version reported by current release builds: `hypersync 0.0.3`.

## Workspace GitHub Projects

- `infra`
  - Local path: `/Users/agavrilov/src/wsync/infra`
  - GitHub repo: `https://github.com/ai-app-user/infra`
  - Visibility: private
  - Default branch: `main`
  - Initial commit: `d0ff326 Initial project infra toolkit`
- `piper`
  - Local path: `/Users/agavrilov/src/wsync/piper`
  - GitHub repo: `https://github.com/ai-app-user/piper`
  - Visibility: private
  - Default branch: `main`
  - Initial commit: `219d453 Initial piper pipeline library`
- `hypersync`
  - Local path: `/Users/agavrilov/src/wsync/hypersync`
  - GitHub repo: `https://github.com/ai-app-user/hypersync`
  - Visibility: private
  - Default branch: `main`
  - Initial commit: `1282a71 Initial hypersync application`
  - Latest deploy packaging commit: `1e1459b Add Linux deploy bundle packaging`
  - Latest UX commit: `e4bfd5e Document new server deployment UX`
  - Latest deploy simplification commit: `ea8488f Simplify Linux deploy UX`
  - Latest deploy README library cleanup commit:
    `354fa7d Document deploy libraries in README`
  - Latest mostly-static deploy README commit:
    `d0414af Update deploy README for mostly static bundle`

## Hypersync Deploy Packaging

- Deploy folder: `/Users/agavrilov/src/wsync/hypersync/deploy`
- `deploy/package-linux.sh` builds or stages a Linux release binary and creates
  a flat deploy folder. The user-facing bundle contains no `bin/`, `lib/`,
  `config/`, or `doc/` subfolders.
- Flat bundle layout:
  - `hypersync`: launcher script users run.
  - `hypersync.bin`: compiled executable.
  - `default.yaml`: default config.
  - `*.so*`: copied runtime libraries.
  - `README.txt`, `manifest.txt`, `checksums.sha256`.
- Do not create a separate `runtime-libraries.txt`; the deploy README names
  expected runtime library families directly, and `manifest.txt` records full
  `ldd` output for the packaged executable.
- `deploy/run-hypersync` is copied into bundles as `hypersync`; it sets
  `LD_LIBRARY_PATH` to its own directory and execs `hypersync.bin`.
- `deploy/install-hypersync.sh` is the one-command new-server installer. It
  downloads the release archive and checksum, verifies with `sha256sum`,
  unpacks into `$HOME/hypersync` by default, and runs `hypersync --version`.
- Default bundle output is ignored by git under `deploy/package/`.
- The packaging script must be run on Linux so ELF dependencies match the
  target platform. On macOS it intentionally refuses to build.
- Useful command from the workspace root:

```bash
hypersync/deploy/package-linux.sh
```

- For staged local DuckDB/libnfs builds, pass `DUCKDB_CFLAGS`, `DUCKDB_LIBS`,
  `LIBNFS_CFLAGS`, and `LIBNFS_LIBS` in the environment; the script forwards
  them as make command-line overrides.
- New-server UX is documented in `hypersync/doc/ux.md` and
  `hypersync/deploy/README.md`: the user runs one shell command such as
  `curl -fsSL .../install-hypersync.sh | sh`; after install the command is
  `$HOME/hypersync/hypersync ...`.

## Static Linking Experiment On transfer1

- Host: `ubuntu@216.86.174.100`
- Workspace staged at: `/mnt/local-nvme/hypersync-static-exp`
- Results dirs:
  - `/mnt/local-nvme/hypersync-static-results`
  - `/mnt/local-nvme/hypersync-static-results-2`
  - `/mnt/local-nvme/hypersync-static-results-3`
  - `/mnt/local-nvme/hypersync-static-results-4`
- Static archives found:
  - `/usr/lib/x86_64-linux-gnu/libnfs.a`
  - `/mnt/local-nvme/wsync-codex/third_party/duckdb-v1.5.2/extract/libduckdb_static.a`
- Successful stripped sizes:
  - No optional libs, dynamic runtime: `1.21 MiB`.
  - No optional libs, static C++ runtime: `2.41 MiB`, only glibc dynamic.
  - No optional libs, fully static: `3.37 MiB`, no dynamic deps.
  - Dynamic libnfs/runtime: `1.33 MiB`.
  - Static libnfs + static C++ runtime: `2.77 MiB`, only glibc dynamic.
  - Static libnfs fully static, no DuckDB: `3.74 MiB`, no dynamic deps.
  - Dynamic DuckDB + dynamic libnfs/runtime app: `1.36 MiB` app plus
    `67.04 MiB` `libduckdb.so`.
  - Dynamic DuckDB + static libnfs + static C++ runtime app: `3.19 MiB` app
    plus `67.04 MiB` `libduckdb.so`; executable depends on `libduckdb.so` and
    glibc, while DuckDB `.so` depends on host `libstdc++`, `libgcc_s`, `libm`,
    `libdl`, `libpthread`, and glibc.
- `--version` smoke tests passed for successful static and mostly-static
  variants.
- Static DuckDB link failed with the available archive:
  - unresolved symbol: `duckdb::ExtensionHelper::LoadAllExtensions(duckdb::DuckDB&)`
  - the provided `libduckdb_static.a` is not self-contained for this link.
- Practical near-term deployment recommendation:
  - statically link app C++ runtime and libnfs;
  - bundle `libduckdb.so`;
  - bundle `libstdc++.so.6` and `libgcc_s.so.1` too if relying on the current
    DuckDB shared object and needing broader target compatibility;
  - leave glibc/system dynamic loader as system-provided unless accepting fully
    static glibc/NSS caveats.
- Deploy README and UX now describe the expected practical bundle as:
  `hypersync`, `hypersync.bin`, `default.yaml`, `libduckdb.so*`, optional
  `libstdc++.so*`/`libgcc_s.so*` if DuckDB needs them, `README.txt`,
  `manifest.txt`, and `checksums.sha256`.

## Important Architecture Decisions

- Pipelines are composed of independent jobs connected by thread-safe queues.
- Jobs must not assume who provides or consumes buffers.
- Queues and buffer pools operate on generic preallocated memory buffers; typed interpretation happens at the element/codec level, not in queue/pool classes.
- Buffers are preallocated at application start and deallocated at application end.
- Large data buffers should not be copied or repeatedly allocated/deallocated after libnfs hands data to the app.
- Sharded queues are preferred/default for development/testing.
- For Hark scans going forward: never scan through kernel-mounted NFS. Use libnfs only, with DuckDB parquet output.
- For Hark runtime packaging: both DuckDB and libnfs shared libraries must be shipped/staged beside the executable; do not install system packages.

## Key Hosts

- transfer1: `ubuntu@216.86.174.100`
  - Hostname observed: `ice1-transfer-001`
  - Has RAID0 NVMe at `/mnt/local-nvme`
  - Has libnfs 5.0.2 and existing DuckDB bundles under `/mnt/local-nvme/wsync-codex/third_party/duckdb-v1.5.2`
- transfer2: `ubuntu@216.86.174.208`
- Hark target host: `ubuntu@216.86.175.96`
  - Hostname observed: `hark-data-transfer`
  - Hark target mounted at `/mnt/hark-target`

## Hark Target Mount

- Mounted on `216.86.175.96`:
  - Source: `nfs.crusoecloudcompute.com:/volumes/fcf433b4-ba9e-4db9-ae03-0bcf2fb4aa71`
  - Target: `/mnt/hark-target`
  - Mode: `rw`
  - Verified user-level write test succeeded.
  - Size observed: `500T`
- Top-level folders relevant to Hark:
  - `/mnt/hark-target/linjie`
  - `/mnt/hark-target/timcui`
  - `/mnt/hark-target/xfrui`
  - `/mnt/hark-target/yingrliu`
  - `/mnt/hark-target/zlei`

## Staged Libraries on Hark Host

On `ubuntu@216.86.175.96`:

- Working tree: `~/wsync-hark`
- DuckDB staged locally:
  - `~/wsync-hark/third_party/duckdb-v1.5.2/lib/libduckdb.so`
  - `~/wsync-hark/third_party/duckdb-v1.5.2/include/duckdb.h`
  - `~/wsync-hark/third_party/duckdb-v1.5.2/lib/pkgconfig/duckdb.pc`
- libnfs staged locally:
  - `~/wsync-hark/third_party/libnfs/lib/libnfs.so.14.0.0`
  - `~/wsync-hark/third_party/libnfs/lib/libnfs.so`
  - `~/wsync-hark/third_party/libnfs/include/nfsc/libnfs.h`
- Need next rebuild with both libraries enabled from these local paths.
- Expected runtime `LD_LIBRARY_PATH` should include both:
  - `~/wsync-hark/third_party/duckdb-v1.5.2/lib`
  - `~/wsync-hark/third_party/libnfs/lib`

## Useful Build Pattern for Hark Host

Do not install packages. Build with local library paths:

```bash
cd ~/wsync-hark
duck="$HOME/wsync-hark/third_party/duckdb-v1.5.2"
nfs="$HOME/wsync-hark/third_party/libnfs"
make clean
make release -j"$(nproc)" \
  DUCKDB_CFLAGS="-I$duck/include" \
  DUCKDB_LIBS="-L$duck/lib -lduckdb -Wl,-rpath,$duck/lib" \
  LIBNFS_CFLAGS="-I$nfs/include" \
  LIBNFS_LIBS="-L$nfs/lib -lnfs -Wl,-rpath,$nfs/lib"
LD_LIBRARY_PATH="$duck/lib:$nfs/lib" ./build/release/hypersync --version
```

Before any real scan, run a small libnfs metadata scan and verify output includes `async_backend=true`.

## Hark Scan Output State

- Current run dir: `/tmp/hark-scans-20260512-1429` on `216.86.175.96`
- Completed parquet outputs:
  - `/tmp/hark-scans-20260512-1429/linjie.parquet`
  - `/tmp/hark-scans-20260512-1429/timcui.parquet`
  - `/tmp/hark-scans-20260512-1429/xfrui.parquet`
  - `/tmp/hark-scans-20260512-1429/yingrliu.parquet`
- Partial/stuck `zlei` files preserved:
  - `/tmp/hark-scans-20260512-1429/partial-zlei-stuck-20260512-152259`

## Hark Scan Summary

```text
folder     status   files       folders     logical size
linjie     done     396,237     1,676       125.77 TB
timcui     done     267,125     2,260       36.87 TB
xfrui      done     20,719      50,377      74.25 TB
yingrliu   done     7,698,844   5,519,640   35.53 TB
zlei       partial  3,689,584   60,288      63.57 TB
```

Completed total excluding `zlei` partial:

```text
files: 8,382,925
folders: 5,573,953
logical size: 272.42 TB
```

Including `zlei` partial:

```text
files: 12,072,509
folders: 5,634,241
logical size: 335.99 TB
```

## zlei Incident

- Expected `zlei` full scan size from user: about `43M` files.
- Bad scan used kernel-mounted source path:
  - Source: `/mnt/hark-target/zlei`
  - Threads: `--meta-reader-threads 32`
  - Metadata async depth: `--metadata-async-depth 128`
  - Record buffer slots: `--record-buffer-slots 1000000`
  - Output: parquet via staged DuckDB
  - Reported `async_backend=false`
- It stalled around:
  - Files: `3,689,584`
  - Folders: `60,288`
  - Logical size: `63.57 TB`
- Process state before stop:
  - Many threads sleeping on `futex_wait_queue`
  - No D-state tasks observed
  - DuckDB temp file stopped changing
- Decision: do not use kernel NFS scan path again. Rerun `zlei` only after rebuilding with both local libnfs and DuckDB and verifying `async_backend=true`.

## Next Operational Step

1. Rebuild `~/wsync-hark` on `216.86.175.96` with both staged DuckDB and staged libnfs.
2. Verify the binary links/runs with both staged libraries:
   - `LD_LIBRARY_PATH="$duck/lib:$nfs/lib" ldd ./build/release/hypersync`
3. Run a short small-folder libnfs scan and verify `async_backend=true`.
4. Rerun `zlei` via a libnfs URL, not `/mnt/hark-target/zlei`.
5. Write parquet output and produce a final Slack table including full `zlei`.

## Common Project Infra Repo

- Local path: `/Users/agavrilov/src/wsync/infra`
- GitHub repo: `https://github.com/ai-app-user/infra`
- Visibility: private
- Default branch: `main`
- Initial commit: `d0ff326a0efa217c20610ba6e2a75290ab95c042`
- Purpose: reusable scripts/docs for GitHub auth and project bootstrap.

Included scripts:

- `scripts/gh-auth-token.sh`
  - Configures GitHub CLI using `GITHUB_TOKEN` or stdin.
  - Uses `gh auth login --with-token`.
  - Sets `gh` Git protocol to HTTPS.
  - Does not store tokens in the repo.
- `scripts/check-github-setup.sh`
  - Prints `gh auth status`, authenticated GitHub user, Git identity, remotes, and SSH public key fingerprints.
- `scripts/new-github-project.sh`
  - Initializes a local Git repo if needed.
  - Creates or reuses `OWNER/REPO` on GitHub.
  - Sets `origin` to `https://github.com/OWNER/REPO.git`.
  - Commits and pushes.
  - Supports `--no-gpg-sign`, useful on this machine because the 1Password SSH signing agent failed during commit signing.

Included docs:

- `docs/github-setup.md`
- `docs/project-guidelines.md`
- `docs/security.md`

## Hypersync Checker And Packed Transfer State

- Local repo: `/Users/agavrilov/src/wsync/hypersync`
- GitHub repo: `https://github.com/ai-app-user/hypersync`
- Latest pushed commit at handoff: `8c71c96 Verify scan diff and sync workflows`
- Relevant recent commits:
  - `93dd928 Add live folder metadata diff`
  - `5318d93 Avoid partial-folder target-only diff rows`
  - `9b5c589 Pack small files in runtime transfer`
  - `7e51ee4 Record checker and transfer smoke baselines`
  - `8c71c96 Verify scan diff and sync workflows`
- Current local `hypersync` working tree was clean after the push.

Implemented checker:

- CLI: `hypersync diff --source <dir|nfs-url> --target <dir|nfs-url>`
- Useful options:
  - `--compare size|time|content`
  - `--non-recursive`
  - `--meta-reader-threads <n>`
  - `--metadata-async-depth <n>`
  - `--max-duration-seconds <n>`
  - `--output <csv>`
- It compares flat folders as scan units and reports `same`, `changed`, `new`, and `target_only`.
- Timer handling avoids emitting target-only rows for a partial source folder stopped by timeout.
- Caveat: live diff currently accumulates report rows in memory before writing CSV; for billion-file production output, stream results to a writer/Parquet job instead.

Implemented packed small-file transfer:

- Runtime sender packs multiple small files into large `DataSlot` buffers when `record.size <= small_file_threshold`.
- Protocol flag: `kFlagPackedSmallFiles`.
- Receiver unpacks the batch, writes each file, verifies each file hash, and sends one ACK per file.
- Sender ACK handling is now FIFO (`std::deque<FileAckMessage>`) so a burst of ACKs from a packed batch does not overwrite earlier ACKs.
- CLI `send` reports `chunks_sent` so small-file packing efficiency is visible.

Current sync/copy UX:

- `send`, `sync`, and `copy` are aliases for the sender side of the same
  runtime pipeline.
- Normal runbook:
  - Target server: `hypersync receive --target <dir|nfs-url> --bind-host 0.0.0.0`
  - Source server: `hypersync sync --source <dir|nfs-url> --host <target-host>`
- If syncing remote-owned NFS data to a local filesystem and preserving owner/group,
  run the receiver as root (`sudo hypersync receive ...`) or use a writable NFS
  target with appropriate credentials.
- Receiver-side per-file write/metadata failures now send negative ACKs, so the
  sender reports failures instead of hanging forever.

Regression results:

- Local tests:
  - `make unit-test integration-test functional-test performance-test -j8`
  - Unit: 49/49 passed
  - Integration: 19/19 passed
  - Functional: 6/7 passed, 1 skipped on macOS because Linux NFS export tools are not present
  - Performance smoke: 4/4 passed
- Local packed transfer stress:
  - 1000 tiny files copied, sender/receiver rc 0, target files 1000, `chunks_sent=68`.

Remote source trees used for bounded tests:

- transfer1:
  - Host: `ubuntu@216.86.174.100`
  - Workspace: `/mnt/local-nvme/wsync-latest`
  - Source: `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/HaWoR/video`
- nopo1:
  - Host: `ubuntu@160.211.77.39`
  - Workspace: `/mnt/local-nvme/wsync-codex/wsync-latest`
  - Source: `nfs://172.27.255.2-172.27.255.17/volumes/b7ec3b01-0aba-49cc-b3d2-6692504cf6c5/data`

Remote bounded results:

- transfer1 live checker self-diff:
  - `diff_records=46239 same=46239 changed=0 new=0 target_only=0`
  - elapsed `1.16s`, max RSS `54 MB`
- nopo1 live checker self-diff:
  - `diff_records=282060 same=282060 changed=0 new=0 target_only=0`
  - elapsed `17.47s`, max RSS `605 MB`
  - Several `NFS3ERR_PERM` folders were skipped.
- transfer1 packed small-file transfer smoke:
  - 1000 files sent/received, 3000 payload bytes, `chunks_sent=85`, rc 0.
- nopo1 packed small-file transfer smoke:
  - 1000 files sent/received, 3000 payload bytes, `chunks_sent=114`, rc 0.
- transfer1 NFS data reader retest:
  - `files_found=46239`, `files_read=46239`, `bytes_read=3682117785`
  - average `6.39 Gbit/s`; small corpus completed before timer.
- nopo1 NFS data reader retest:
  - active samples `193.6-195.8 Gbit/s`
  - final `bytes_read=245313995009`, `files_read=335`, final average `100.2 Gbit/s` due drain/tail time.

Latest real-NFS scan/diff/sync acceptance (`8c71c96`):

- transfer1:
  - Source: `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/HaWoR/video/path/0/175593000`
  - Output dir: `/mnt/local-nvme/hypersync-accept-transfer1-final`
  - Scan: `files_found=506`, `folders_found=7`, `logical_size_bytes=9564146`, `async_backend=true`
  - Self-diff: `diff_records=506 same=506 changed=0 new=0 target_only=0`
  - Sync to local NVMe with `sudo receive`: `files_total=506 transferred=506 failed=0 bytes=9564146 chunks_sent=509`
  - Post-sync diff: `506 same`, no changes/new/target-only.
- nopo1:
  - Source: `nfs://172.27.255.2-172.27.255.17/volumes/b7ec3b01-0aba-49cc-b3d2-6692504cf6c5/data/bz_jy/external`
  - Output dir: `/mnt/local-nvme/wsync-codex/hypersync-accept-nopo1-external-final`
  - Scan: `files_found=41`, `folders_found=28`, `logical_size_bytes=436052`, `async_backend=true`
  - Self-diff: `diff_records=41 same=41 changed=0 new=0 target_only=0`
  - Sync to local NVMe with `sudo receive`: `files_total=41 transferred=41 failed=0 bytes=436052 chunks_sent=41`
  - Post-sync diff: `41 same`, no changes/new/target-only.
- Negative ownership check on transfer1 without sudo receiver:
  - Sender returned rc `2` and reported `failed=506`; receiver returned rc `1`.
  - It no longer times out/hangs.

Performance doc:

- New results are recorded in `/Users/agavrilov/src/wsync/hypersync/doc/performance.md` under "Checker And Data Transfer Smokes".

## 2026-05-12 Transfer1/Nopo1 Runtime Performance Follow-up

Pushed hypersync commit:

- `e4ad05b Pipeline runtime transfer acknowledgements`

Code changes made after `8c71c96`:

- Runtime sync no longer waits for one receiver decision per WAN round trip.
  Sender now sends file records ahead, then consumes file decisions as a stream.
- Data ACK handling is pipelined: sender records in-flight files and matches
  `MSG_FILE_ACK` by `file_id`.
- Receiver `read_data_slot` uses blocking preallocated-slot acquire, so a full
  slot pool is flow control instead of a fatal "no free data slots available".
- Default `small_file_threshold` and fixed small slot capacity increased from
  `4 KiB` to `128 KiB`.
- `ScanIndex::from_csv` now reads both the old internal scan CSV and the rich
  `scan --output-format csv` output, so scan CSVs can feed `diff --source-scan`
  / `--target-scan` directly.
- Updated docs:
  - `hypersync/doc/design.md`
  - `hypersync/doc/performance.md`
- Added test coverage in `hypersync/tests/test_main.cpp` for rich scan CSV
  loading.

Validation:

- Local `make -j8` passed:
  - Unit `49/49`
  - Integration `19/19`
  - Functional `6/7` passed, one expected skip on macOS
  - Performance smoke `4/4`
- Remote release binaries rebuilt on:
  - transfer1 `/mnt/local-nvme/wsync-latest/build/release/hypersync`
  - nopo1 `/mnt/local-nvme/wsync-codex/wsync-latest/build/release/hypersync`

Raw network:

- `iperf3 -P 16` transfer1 -> nopo1: about `196 Gbit/s` sender, `179 Gbit/s` receiver.
- `iperf3 -P 16` nopo1 -> transfer1: about `177 Gbit/s` sender, `160 Gbit/s` receiver.

Runtime sync measurements:

- Pre-fix 100 files x 128 KiB: `15.46s`, about `6.8 Mbit/s`.
- Post-fix 100 files x 128 KiB: `0.88s`, `chunks_sent=15`.
- Post-fix 50k files x 128 KiB, single sync: `6.55 GB` in `6.03s`, `8.70 Gbit/s`.
- Post-fix 50k files x 128 KiB, 50 sharded flat-folder syncs:
  `6.55 GB` in `1.303s`, `40.25 Gbit/s`.
- Post-fix 200k files x 128 KiB, 50 sharded flat-folder syncs:
  `26.21 GB` in `6.483s`, `32.35 Gbit/s`; target file creation dominates.
- Post-fix 16k files x 1 MiB, single sync:
  `16.78 GB` in `17.38s`, `7.72 Gbit/s`.
- Post-fix 16k files x 1 MiB, 50 sharded flat-folder syncs:
  `16.78 GB` in `2.093s`, `64.12 Gbit/s`.

Current conclusion:

- transfer1/nopo1 network is not the limiter.
- Current single runtime sync still needs internal multi-lane readers/writers to
  hit 50 Gbit/s for small-file-heavy trees.
- External sharding by flat folder can already exceed 50 Gbit/s for 1MiB-class
  files, but 128KiB files are limited by materializing huge file counts on the
  target filesystem.

Real NFS cross-host check:

- Source:
  `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/HaWoR/video/path/0/175593000`
- Target:
  `/mnt/local-nvme/wsync-codex/hypersync-xhost-test/target-transfer1-real-nfs-v2`
  on nopo1.
- Result: `files_total=506 transferred=506 failed=0 bytes=9564146 chunks_sent=501 elapsed=2.91s`.
- Scan-to-scan diff after copy:
  `diff_records=506 same=506 changed=0 new=0 target_only=0 bytes_planned=0`.

## Useful Previous Slack Table

```text
hark scan summary

folder     status   files       folders     logical size
linjie     done     396,237     1,676       125.77 TB
timcui     done     267,125     2,260       36.87 TB
xfrui      done     20,719      50,377      74.25 TB
yingrliu   done     7,698,844   5,519,640   35.53 TB
zlei       partial  3,689,584   60,288      63.57 TB

completed total excluding zlei partial:
files: 8,382,925
folders: 5,573,953
logical size: 272.42 TB

including zlei partial:
files: 12,072,509
folders: 5,634,241
logical size: 335.99 TB
```

## Latest Checker/Diff Work, 2026-05-13 00:38 PDT

- User clarified checker/diff performance should be scanner-like: millions of records/s.
- Implemented summary-only live diff/checker fast path:
  - `hypersync diff --summary-only` avoids materializing per-file maps/CSV.
  - Source and target metadata readers are independent async jobs.
  - Source scanner enqueues matching target flat-folder work.
  - Target scanner reads target folders independently.
  - Differ rendezvous batches by folder path and compares in flat-folder batches.
  - Timed runs suppress target-only reporting to avoid false target-only results when the source timer stops mid-directory.
  - Added `--stats-interval-seconds` to `diff` to monitor records/s while running.
  - Removed target `FileSpec` string copies in summary comparison hot path by using `std::string_view` keys into the target batch.
- Local validation after changes:
  - `make -j8` passed.
  - Unit `50/50`, integration `19/19`, functional `6/7` with expected macOS NFS export skip, performance smoke `4/4`.
- transfer1 release binary rebuilt in `/mnt/local-nvme/wsync-latest/build/release/hypersync`.
- nopo1 release binary rebuilt in `/mnt/local-nvme/wsync-codex/wsync-latest/build/release/hypersync`.
- Real transfer1 root checker performance sample:
  - Source/target: `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - Command used 96 meta-reader threads, async depth 128, `--max-duration-seconds 5`, `--stats-interval-seconds 5`, and outer `timeout 20`.
  - 5s: `668,902 records/s`, `3,344,675` compared.
  - 10s: `980,232 records/s` average, `1,291,480 records/s` interval, `9,804,137` compared.
  - 15s: `781,677 records/s`, `11,726,688` compared.
  - Max RSS before outer timeout: about `9.7 GiB`.
- Real nopo1 root checker performance sample:
  - Source/target: `nfs://172.27.255.2-172.27.255.17/volumes/b7ec3b01-0aba-49cc-b3d2-6692504cf6c5/data`
  - Same shape: 96 meta-reader threads, async depth 128, 5s internal timer, 5s stats, 20s outer timeout.
  - 5s: `692,772 records/s`, `3,464,003` compared.
  - 10s: `817,302 records/s` average, `941,831 records/s` interval, `8,173,386` compared.
  - Max RSS before outer timeout: about `6.9 GiB`.
  - Many permission-denied source folders were skipped; timed target-only suppression kept `target_only=0`.
- Scanner reference on same root:
  - 5s: `4,396,420 records/s`.
  - 10s: `5,813,000 records/s`.
  - Final after drain: `1,504,640 records/s` over `56,824,570` files.
- Current conclusion:
  - Checker is now around scanner-class order of magnitude and can exceed 1M records/s in interval samples.
  - It is still below scanner because it reads both source and target NFS metadata and performs per-folder comparison.
  - Remaining hot spots are comparison CPU/allocation for very large flat folders and memory use from queued/pending folder batches.
- Pushed hypersync commit `62cd045 Improve live diff summary checker pipeline`.
- Note: first commit attempt failed because 1Password signing could not fill its buffer; commit was created with `commit.gpgsign=false` and pushed successfully.

## Runtime Job Configuration Fix, 2026-05-13 06:25 PDT

- User rule: operational job settings must not be hardcoded in command code.
  Jobs should expose concurrency, queue depths, batching, rate limits, and
  backpressure limits through config when those knobs affect throughput,
  memory, or fairness. CLI flags are per-run overrides.
- Current changed files are local/uncommitted in `hypersync/`.
- Checker config now includes:
  - `jobs.checker.worker_count`
  - `jobs.checker.target_request_queue_depth`
  - `jobs.checker.batch_queue_depth`
- Default YAML now sets checker defaults to `16`, `65536`, `65536`.
- Live diff accepts CLI overrides:
  - `--checker-threads`
  - `--checker-request-queue-depth`
  - `--checker-batch-queue-depth`
- `benchmark-fake-diff` now uses `jobs.checker` when `--checker-threads`,
  `--request-queue-depth`, or `--batch-queue-depth` are omitted.
- Data/hash commands no longer force metadata reader `1` thread and async depth
  `16`; they now respect `jobs.nfs_meta_reader` unless CLI flags override.
- Data hash/inventory now use `jobs.data_hasher.algorithm`,
  `jobs.data_hasher.worker_count`, and `jobs.data_hasher.work_factor` when
  hash options are omitted.
- Validation after this change:
  - `make unit-test -j8`: `51/51` passed.
  - `make integration-test`: `19/19` passed.
  - `make functional-test performance-test`: functional `6/7` passed with the
    expected macOS NFS export skip; performance `4/4` passed.
  - Quick smoke:
    `./build/hypersync benchmark-fake-diff --file-count 1000 --folder-count 20 --source-threads 2 --fake-remote-threads 2 --remote-delay-us 0`
    reported `checker_threads=16 request_queue_depth=65536 batch_queue_depth=65536`.

## Compact Short Metadata DB, 2026-05-13 12:21 PDT

- Server: transfer1 `ubuntu@216.86.168.191`.
- Source large scan directory:
  `/mnt/local-nvme/scans/crusoe-full-20260513T164754Z`.
- The `metadata/*.parquet` files in that run are not valid finalized Parquet
  files; DuckDB reports missing footer magic bytes.
- The 32 `metadata/*.parquet.duckdb.tmp` files are readable DuckDB databases
  with table `metadata_records`.
- Created compressed short DB from those DuckDB shard databases:
  `/mnt/local-nvme/scans/crusoe-full-20260513T164754Z/short-from-duckdb`.
- Stable symlink:
  `/mnt/local-nvme/scans/crusoe-short-current`.
- Short DB format:
  - 32 ZSTD-compressed Parquet files named `part-000NN.short.parquet`.
  - Columns: `kind UTINYINT`, `parent_hash UBIGINT`,
    `metadata_hash UBIGINT`, `logical_size UBIGINT`.
  - `kind`: `0=file`, `1=folder`.
  - `parent_hash`: DuckDB `hash(parse_dirname(rel_path))`.
  - `metadata_hash`: DuckDB hash of `kind`, basename, logical size, mtime,
    mode, uid, gid, flat folder file count, and flat folder logical size.
  - `logical_size`: file `size` for files; `flat_logical_size_bytes` for
    folders.
- Validated counts from `read_parquet('/mnt/local-nvme/scans/crusoe-short-current/*.short.parquet')`:
  - records: `5,326,257,608`
  - files: `5,234,574,569`
  - folders: `91,683,039`
  - file logical size: `7,619,866,942,972,112` bytes
  - folder flat logical size: `7,619,866,942,972,112` bytes
  - compressed size: `53G`
- Conversion script:
  `/mnt/local-nvme/scans/crusoe-full-20260513T164754Z/create-short-from-duckdb.sh`.

## Distributed Diff Transfer1 vs Nopo1, 2026-05-13 14:43 PDT

- Implemented first distributed metadata diff roles:
  - `hypersync diff-target`: run on target-side host, listens on TCP, reads
    target NFS, compares received compact source folder batches, returns
    per-folder summaries.
  - `hypersync diff-source`: run on source-side host, scans source NFS, sends
    compact flat-folder batches, writes `folder-report.csv`.
- Reason: nopo1 cannot reach `nfs.crusoecloudcompute.com` TCP 2049/20048, and
  transfer1 cannot mount the nopo target export. One-process live diff cannot
  honestly compare these two NFS trees.
- Hosts and roots used:
  - transfer1: `ubuntu@216.86.168.191`
  - source: `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - nopo1: `ubuntu@160.211.77.39`
  - target: `nfs://172.27.255.2-172.27.255.17/volumes/dfb990b1-bf40-4378-85f1-26f9dfd0cd2c/data`
- Code changes:
  - Added `diff-source` / `diff-target` CLI commands.
  - Added per-folder summary CSV schema with source/target scan timestamps,
    result send/receive timestamps, file/folder counts, same/changed/new/
    target-only counts, logical-size counters, status, and error.
  - Added `FlatFolderScanBatch` scan timestamps.
  - Target-side per-folder libnfs errors are now returned as failed folder
    summaries instead of killing the whole target process.
  - Docs updated in `hypersync/doc/{design,requirement,ux,performance}.md`.
- Validation:
  - Local `make unit-test -j8`: `51/51` passed.
  - Linux release builds completed on transfer1 and nopo1 with libnfs/DuckDB.
  - Smoke non-recursive root diff completed and wrote a valid CSV.
- Main bounded recursive run:
  - Target command used port `39172`, `--target-threads 96`,
    `--metadata-async-depth 128`.
  - Source used `--meta-reader-threads 96`, `--metadata-async-depth 128`,
    `--max-duration-seconds 60`, `--stats-interval-seconds 5`.
  - Source report path:
    `/mnt/local-nvme/diff-transfer1-nopo1-current/folder-report.csv`
    on transfer1.
  - Results after target drain:
    - folder reports: `321,264`
    - file decisions: `82,898,749`
    - same: `82,898,562`
    - changed: `0`
    - source-only/new: `187`
    - target-only: `0` because timed source run disables target-only expansion
    - failed count in final summary: `6`; CSV status rows include permission
      and target `NOENT` errors (`103` rows had non-zero status or failed count)
    - source logical size: `582,265,457,888,951` bytes
    - target logical size: `671,396,133,533,424` bytes
    - planned bytes: `169,273,819,248`
    - elapsed: `157.98s`
    - average: `524,745` file decisions/s, `2,034` folder reports/s
    - peak interval: about `608K` file decisions/s
- Current bottlenecks:
  - Not scanner-class yet.
  - Per-folder frames, one TCP stream/write mutex, and whole-folder compare
    units limit throughput.
  - Next work: batch many folder summaries per reply frame, split huge flat
    source folders across multiple frames with an end-of-folder marker, use
    multiple transport streams, and expose in-flight request limits to prevent
    unbounded target queue growth.

## Generic Job Runtime Instrumentation, 2026-05-13 17:13 PDT

- Added reusable Piper runtime metrics:
  - files: `piper/src/monitoring/runtime_metrics.hpp/.cpp`
  - umbrella include: `piper/src/piper.hpp`
  - `ThreadedJob` now owns `ThreadedJobRuntimeMetrics` and exposes
    `runtime_metrics()`.
- Worker states:
  - `processing`
  - `wait_input`
  - `wait_output`
  - `wait_pool`
  - `wait_io`
  - `stopped`
- Low-overhead rule:
  - Queue/pool helpers try non-blocking fast paths first.
  - Timed state scopes are entered only after a queue/pool fast path fails or
    around owned blocking I/O.
  - This avoids a timestamp on every successful buffer push/pop.
- Generic helpers added to `ThreadedJob`:
  - `wait_for_input(worker, BufQueue/ShardedBufQueue, handle)`
  - `wait_for_output(worker, BufQueue/ShardedBufQueue, handle)`
  - `wait_for_pool(worker, RawBufferPool)`
  - `runtime_state_scope(worker, RuntimeState)`
- Piper jobs using shared metrics:
  - `BufferProducerJob`
  - `BufferConsumerJob`
  - `BufferTransformJob`
  - `BufferSenderJob`
  - `BufferReceiverJob`
  - `BufferStreamSenderJob`
  - `BufferStreamReceiverJob`
- Hypersync status providers now attach runtime metrics for:
  - `NfsDataBufferReaderJob`
  - `DataHasherJob`
  - `BufferDiscarderJob`
- `StatusRegistry::render_human()` now reports:
  - process CPU percent from `getrusage`
  - cumulative rate and throughput
  - recent/current rate and byte throughput since previous snapshot
  - first observed startup rate
  - mid-run historical rate, computed from completed intervals between startup
    and the latest/current interval
  - peak observed rate
  - tail rate once a job reports `running=false`
  - runtime percentages: busy, wait_in, wait_out, wait_pool, wait_io
  - current worker state counts via `now=state:n`
- Added `PeriodicStatusReporter`, a shared helper that prints the same
  `StatusRegistry` snapshots on an interval without command-specific reporting
  code.
- Tests:
  - `threaded_job_runtime_metrics_report_wait_states`
  - `periodic_status_reporter_reuses_status_registry`
  - Verified locally:
    - `make -j8 unit-test`: `55/55` passed
    - `make -j8 integration-test`: `19/19` passed
    - `make -j8 release`: passed
- Docs updated:
  - `piper/doc/design.md`
  - `hypersync/doc/design.md`
  - `hypersync/doc/guidlines.md`

## Distributed Diff Large-NFS Testing, 2026-05-14 00:27 PDT

- Active transfer1 host: `ubuntu@216.86.168.191`.
- transfer1 NFS root tested:
  `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  (`/mnt/crusoe-src`, NFSv3, `nconnect=32`, `rsize=1048576`, remoteports/dns).
- transfer1 local scratch/deploy path: `/mnt/local-nvme/wsync-codex/current`.
- Local NVMe on transfer1: `/mnt/local-nvme`, about `94T` total / `91T` free during test.
- Important fix: `TargetFolderScannerBufferJob` now uses `NfsBackend::scan_flat_folders`
  continuously per worker so each target worker keeps its libnfs async depth full.
  Previous target path effectively scanned one requested folder at a time per worker.
- Important fix: libnfs flat-folder scanner now aborts/discards in-flight async directory
  opens when `should_stop()` becomes true. This prevents timed runs from hanging in
  `wait_io` after the source timer expires.
- Verification:
  - Local build/tests after fixes: `make -j8 release unit-test`, `55/55` unit tests passed.
  - Remote release build on transfer1 links `-lnfs` and reports `hypersync 0.0.2`.
- 10+ minute observation run:
  - Run dir: `/mnt/local-nvme/wsync-codex/diff-root-target256-20260514T070113Z`
  - Command shape: diff source and target both pointed at the NFS root over localhost.
  - Config: source `96` scanner threads, target `256` scanner threads, async depth `128`.
  - Target mid-run intervals reached about `20K-22.5K folders/s`.
  - Source scanner could run ahead of target; compare/result jobs were not bottlenecks.
  - Timed source run exposed shutdown issue before the libnfs stop fix.
- Shutdown verification run after fix:
  - Run dir: `/mnt/local-nvme/wsync-codex/diff-root-stopfix-20260514T071557Z`
  - Source timer: `120s`; source and target both exited cleanly with exit code `0`.
  - Final report: `6,629,404` CSV lines.
  - Target drain intervals after source timer: `26.9K`, `27.6K`, `28.4K`, `29.1K`,
    and `29.8K folders/s`.
- Target thread tuning:
  - `256` target threads gave strong sustained target drain with lower overhead.
  - `512` target threads did not materially improve peak; short run peaked at
    about `29.9K folders/s`.
- Current bottleneck assessment:
  - Diff comparison and result writing are mostly idle; target flat-folder metadata
    scan is the limiting job in the single-host/same-NFS setup.
  - During timed source scans, source can still scan/send far ahead and then wait for
    target drain. If wall-clock timed diff must stop faster, add a configurable
    source-side in-flight/source-ahead cap or explicit timed-run discard mode.
## Direct Libnfs Diff Fix, 2026-05-14 16:16 PDT

- User requirement: do not use mounted folders for diff/scan; only direct libnfs is acceptable.
- Fixed `hypersync/src/core/nfs_backend.cpp`:
  - `LibNfsBackend::scan_flat_folders()` now creates a libnfs session lazily, after a worker receives a folder request. This avoids idle target scanner workers mounting random endpoints and blocking pipeline completion.
  - Added 5s timeout for `nfs_mount_async`; timed-out mounts are retried by reconnecting to a fresh random endpoint.
  - Added 5s timeout/retry for `nfs_opendir_async`.
  - Stuck libnfs contexts are abandoned/leaked intentionally in recovery instead of calling `nfs_destroy_context()`, which can block when the context is wedged.
- Verification:
  - Local: `make -j8 release unit-test`, `55/55` passed.
  - Remote transfer1 (`ubuntu@216.86.168.191`) and nopo1 (`ubuntu@160.211.77.39`) rebuilt `hypersync 0.0.2` with `HYPERSYNC_HAS_LIBNFS=1`.
  - Direct libnfs diff test used:
    - Source: `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/HaWoR/video`
    - Target: `nfs://172.27.255.2-172.27.255.17/volumes/dfb990b1-bf40-4378-85f1-26f9dfd0cd2c/data/HaWoR/video`
    - Source run dir: `/mnt/local-nvme/wsync-codex/diff-source-nopo-video-libnfs-lazy-20260514T231528Z`
    - Target run dir: `/mnt/local-nvme/wsync-codex/diff-target-video-libnfs-lazy-20260514T231515Z`
  - Result: source exit `0`, 526 report rows, statuses all `0`, 46,239 source files, 46,239 target files, 46,239 same, 0 changed/new/target-only/failed, source/target logical size both `3,682,117,785` bytes.
  - No mounted folder was used in this validation.

## Packed Small-File Data Read Experiment, 2026-05-14 21:05 PDT

- User asked to implement/test the small-file data reader approach after discussion of
  multi-file async windows and packing multiple small files per owned data buffer.
- Implemented:
  - `hypersync/src/core/data_buffer_codec.hpp/.cpp`: packed small-file
    `DataBuffer` codec.
  - `NfsDataBufferReaderJob`: optional `pack_small_files` mode.
  - `NfsBackend`: `read_file_into()` and `read_small_files_packed()`.
  - libnfs backend: configurable multi-file async open/read/close window per
    data-reader worker.
  - `DataHasherJob`: hashes packed entries in place without repacking.
  - CLI/config: `--pack-small-files`, `--small-file-async-window`.
- Local verification:
  - `make unit-test`: `57/57` passed on Mac (`HYPERSYNC_HAS_LIBNFS=0`).
- Remote transfer1 verification:
  - Host: `ubuntu@216.86.168.191`.
  - Remote build dir:
    `/mnt/local-nvme/wsync-codex/libnfs-build-check-20260515T035354Z`.
  - libnfs version: `5.0.2`.
  - `make release -j8` succeeded with `HYPERSYNC_HAS_LIBNFS=1`.
- Small-file test source:
  `nfs://nfs.crusoecloudcompute.com/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5/catbear/run_20260218_042836/talking-head`
  (`50,000` files, `1,355,189,671` bytes, non-recursive).
- Important pool-size note:
  - `--data-buffer-slots 65536` means about `64GB` of data-buffer pool and was
    not a useful quick benchmark on the replacement transfer1 host.
  - The useful comparison used `--data-buffer-slots 4096`,
    `--data-queue-depth 2048`.
- Measured raw one-file-per-buffer path:
  - 64 data threads: `2.56s`, `19.5K files/s`, `4.24 Gbit/s`.
  - 128 data threads: `2.22s`, `22.5K files/s`, `4.88 Gbit/s`.
  - 256 data threads: `3.04s`, `16.5K files/s`, `3.57 Gbit/s`.
- Measured packed-small-file mode:
  - 64 threads/window 128: `5.00s`, `10.0K files/s`, `2.17 Gbit/s`.
  - 128 threads/window 16: `3.92s`, `12.7K files/s`, `2.76 Gbit/s`.
  - 256 threads/window 16: `2.83s`, `17.7K files/s`, `3.83 Gbit/s`.
- Larger per-thread window sweep did not help:
  - 16 threads/window 256: `0.55 Gbit/s`.
  - 32 threads/window 1024: `0.91 Gbit/s`.
  - 64 threads/window 256: `1.17 Gbit/s`.
  - 128 threads/window 256: `1.86 Gbit/s`.
- Conclusion:
  - Packed small-file mode is correctness-complete enough for unit/perf
    testing, generic buffer ownership is preserved, and only the libnfs callback
    boundary copies payload bytes into owned buffers.
  - It is not faster than the proven raw path yet and must stay explicit/not
    default until tuned or replaced.
- Added async-read latency instrumentation:
  - counters: queued/completed async reads, failed, zero, short reads, bytes
    requested/completed, average/max latency, and latency buckets.
  - Exposed in `benchmark-data` periodic `data_read_stats` and final
    `data_benchmark` output.
  - Local unit tests after instrumentation: `57/57` passed.
- Instrumented transfer1 result on `talking-head`:
  - Raw 128 threads: no short reads, avg completion `27,103 B`, avg latency
    `0.55 ms`, max `6.44 ms`.
  - Packed 128 threads/window 16: no short reads, avg completion `27,104 B`,
    avg latency `14.45 ms`, max `308.76 ms`.
  - Conclusion: not a 4KB partial-read problem. Packed mode is slower because
    read completions have much higher latency and a long tail.

## Synthetic Profiler Benchmark, 2026-05-17 10:49 PDT

- Added CLI command:
  `hypersync benchmark-synthetic-profile [--file-count <n>] [--block-file-count <n>] [--small-ratio-shift-threshold <n>] [--seed <n>] [--output <profile.txt>]`.
- Purpose: benchmark compact phase-aware profile capture from generated
  metadata observations. This is not live NFS capture yet.
- Transfer1 release run:
  - Run dir:
    `/mnt/local-nvme/wsync-codex/synthetic-profiler-100m-20260517T174453Z`
  - Command:
    `./build/release/hypersync benchmark-synthetic-profile --file-count 100000000 --block-file-count 1000000 --small-ratio-shift-threshold 0.05 --output <run>/profile.txt`
  - Result: `100,000,000` files in `1.721s`, `58.1M files/s`,
    max RSS `3,840 KB`.
  - Compact profile output had 3 phases:
    - phase 0: `35M` files, small ratio `0.960`
    - phase 1: `35M` files, small ratio `0.650`
    - phase 2: `30M` files, small ratio `0.200`
  - Total generated logical size: `16,958,577,138,586,000` bytes.
- Verification:
  - `make unit-test`: `67/67` passed before CLI smoke addition.
  - `make integration-test`: `20/20` passed after adding
    `main_cli_benchmark_synthetic_profile_smoke`.

## Real NFS Profiler Benchmark, 2026-05-17 11:02 PDT

- Added CLI command:
  `hypersync benchmark-nfs-profile --source <nfs-url> [--max-records <n>] [--phase-count <n>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--small-file-threshold-bytes <n>] [--output <profile.txt>]`.
- Implementation uses direct libnfs via `NfsBackend::scan_flat_folders()` and
  accumulates fixed chronological phase histograms. It does not store per-file
  records or raw NFS handles.
- Transfer1 run:
  - Optimized run dir:
    `/mnt/local-nvme/wsync-codex/nfs-profile-100m-10phase-optimized-20260517T181351Z`
  - Source:
    `nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - Command settings:
    `--max-records 100000000 --phase-count 10 --meta-reader-threads 96 --metadata-async-depth 256 --readdirplus-page-bytes 262144`
  - Initial implementation result: `100,000,000` file records in `81.076s`,
    `1.233M files/s`; root cause was a global mutex taken for every file
    classification.
  - Optimized result: `100,000,000` file records in `18.545s`,
    `5.392M files/s`, max RSS `6,832,316 KB`.
  - Optimized observed: `786,201` folders, `8` failed folders,
    `46,111,232` small files, `53,888,768` large files,
    logical size `604,301,484,901,990` bytes.
  - Phase small ratios:
    `0.036, 0.068, 0.201, 0.436, 0.540, 0.481, 0.522, 0.719, 0.805, 0.802`.
- Verification:
  - `make integration-test`: `21/21` passed after adding
    `main_cli_benchmark_nfs_profile_smoke`.
  - `make unit-test`: `67/67` passed.

## Root Scanner-Only Baseline, 2026-05-17 11:28 PDT

- User asked to test scanner-only on the full source for at least 5 minutes.
- Important correction: the first run was accidentally non-root and invalid:
  - Run dir:
    `/mnt/local-nvme/wsync-codex/scanner-full-5min-20260517T181737Z`
  - It produced `5,212` `NFS3ERR_PERM` skips and was discarded.
- Valid root run:
  - Run dir:
    `/mnt/local-nvme/wsync-codex/scanner-full-root-5min-20260517T182221Z`
  - Source:
    `nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - Command:
    `sudo -n ./build/release/hypersync benchmark-meta --source <source> --metadata-stats-discarder --meta-reader-threads 96 --metadata-async-depth 256 --max-duration-seconds 300 --stats-interval-seconds 10`
  - Git/version: `cd73886`, `hypersync 0.0.3`
  - Permission errors: `0`
  - Final: `1,484,518,222` files, `29,612,855` folders,
    logical size `2,897,130,198,133,623` bytes, elapsed `314.329s`,
    final cumulative `4.738M files/s`.
  - Best cumulative sample: `6.581M files/s` at `100s`.
  - Top estimated 10-second intervals:
    `8.862M`, `8.837M`, `8.391M`, `8.021M`, `7.720M files/s`.
- Conclusion: scanner-only is still close to/above the old `~7M/s` territory
  during mid-run intervals. Final 5-minute average is lower due to slower/tail
  regions and timed stop/drain.

## Whole-Source NFS Profiler, 100M-File Phases, 2026-05-17 11:57 PDT

- Added live progress to `benchmark-nfs-profile`:
  - Option: `--stats-interval-seconds <n>`; default `10`.
  - Emits `nfs_profile_progress` records to stderr with elapsed seconds,
    files, folders, failed folders, cumulative files/sec, interval files/sec,
    phase index, and phase offset.
- Verification before remote run:
  - Local `make app && make integration-test`: `21/21` passed.
  - Transfer1 release build succeeded after patching `src/main.cpp`.
- Current running profile:
  - Host: transfer1 `ubuntu@216.86.168.191`, running as root via `sudo -n`.
  - Run dir:
    `/mnt/local-nvme/wsync-codex/nfs-profile-whole-100mphase-20260517T184054Z`
  - Source:
    `nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - Command settings:
    `--max-records 10000000000 --phase-count 100 --meta-reader-threads 96 --metadata-async-depth 256 --readdirplus-page-bytes 262144 --stats-interval-seconds 10`
  - Phase sizing: `10B / 100 = 100M files per phase`.
  - Status at about `16.5m`: still running; `4.713B` files,
    `44.8M` folders, `0` failed folders, cumulative `~4.81M files/s`.
- Completion observed later:
  - `profile.txt` was finalized by the old metadata-only binary.
  - Saved in repo:
    `hypersync/doc/profiles/source-nfs-whole-100mphase-20260517T184054Z.profile.txt`
    and timing in
    `hypersync/doc/profiles/source-nfs-whole-100mphase-20260517T184054Z.time.txt`.
  - Final: `5,473,804,389` files, `93,280,540` folders,
    `0` failed folders.
  - Wall time: `1970.15s`; max RSS: `33,263,404 KB`.
  - Note: this output does not include replay-quality topology/data-read fields
    because the run started before those profiler upgrades.
- How to check:
  - `ssh ubuntu@216.86.168.191 'run=$(cat /tmp/hypersync-last-whole-profile-run); tail -20 "$run/stderr.txt"; ps -eo pid,ppid,etime,args | awk "/[.]\\/build\\/release\\/hypersync benchmark-nfs-profile/ {print}"'`

## Replay-Quality Profiler Topology Capture, 2026-05-17 12:17 PDT

- Extended `benchmark-nfs-profile` output to capture compact topology and page
  behavior needed to mimic real NFS later:
  - true `files_per_folder_buckets`
  - `subdirs_per_folder_buckets`
  - `folder_depth_buckets`
  - `empty_folders`, `near_empty_folders`, `directories`, `max_depth`
  - `entries_per_page_buckets`
  - `readdirplus_pages`, `readdirplus_entries`,
    `readdirplus_requested_bytes`
  - approximate READDIRPLUS page/decode latency percentiles and latency buckets
- Important implementation detail:
  - Full-folder scans now use raw NFSv3 READDIRPLUS when the directory file
    handle is available, even when no streaming page visitor is supplied. This
    lets the profiler keep complete folder fanout while recording page timing.
- Validation:
  - Local tests: `make unit-test` `67/67`, `make integration-test` `21/21`.
  - Transfer1 separate binary:
    `/mnt/local-nvme/src/wsync/build/release/hypersync-profiler-topology`
    built successfully with libnfs while the old long profiler kept running.
  - Transfer1 smoke run:
    `/mnt/local-nvme/wsync-codex/nfs-profile-topology-smoke-20260517T191735Z`
    captured `1M` records as root and showed populated topology plus
    READDIRPLUS page latency fields.

## Sampled Data-Read Profiler, 2026-05-17 12:33 PDT

- Added optional data-read sampling to `benchmark-nfs-profile`.
- Default is metadata-only; data reads are enabled with `--profile-data-reads`.
- Sampling model:
  - deterministic sampling via `synthetic_splitmix64(global_file_index) % sample_rate`
  - per-phase sample count cap
  - per-phase sampled byte cap
  - small files are read fully
  - large files read only a bounded prefix, default `1MiB`
  - reads use raw NFS handles when available and `copy_payload_to_buffer=false`
    so payload copy is avoided; buffers are discarded immediately
- CLI knobs:
  - `--data-sample-rate <n>`
  - `--data-sample-max-files-per-phase <n>`
  - `--data-sample-max-bytes-per-phase <n>`
  - `--data-sample-large-read-bytes <n>`
  - `--data-sample-outstanding-requests <n>`
  - `--data-sample-pool-slots <n>`
- Profile output adds per phase:
  - `sampled_small_read_files`, `sampled_small_read_bytes`,
    `sampled_small_read_failures`, latency percentiles/buckets
  - `sampled_large_read_files`, `sampled_large_read_bytes`,
    `sampled_large_read_failures`, latency percentiles/buckets
- Validation:
  - Local integration tests: `21/21`.
  - Transfer1 separate binary:
    `/mnt/local-nvme/src/wsync/build/release/hypersync-profiler-data-sampling`
  - Transfer1 root smoke:
    `/mnt/local-nvme/wsync-codex/nfs-profile-data-sample-smoke-20260517T193112Z`
    captured `200K` metadata records with sampled small/large reads populated
    and `0` read failures.
- Fix, 2026-05-17 12:59 PDT:
  - Restored fast metadata profiler behavior by making raw READDIRPLUS page
    timing conditional on streaming/page profiling again.
  - Data-read samples no longer require raw handles; sparse samples read the
    same sizes the reader would use: full small files and bounded large-file
    prefixes (`1MiB` by default).
  - The progress reporter now wakes immediately on completion instead of
    adding up to one stats interval to command wall time.
  - Conservative defaults: `--data-sample-rate 1000000000` and
    `--data-sample-max-files-per-phase 1`.
  - Transfer1 check with `50M` records showed metadata progress still at about
    `5M files/s`; one sampled read added about `1.5s` to finalization, so the
    full run should stay within the `5%` slowdown budget with the conservative
    sample rate.
  - Committed and pushed to `hypersync/dev` as `40215d6`:
    `Keep data profiling off profiler hot path`.
  - Deployed `40215d6` on transfer1 and started full source profile with sparse
    data-read sampling:
    `/mnt/local-nvme/wsync-codex/nfs-profile-whole-100mphase-data-sampled-fastpath-20260517T200342Z`
  - Source:
    `nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
  - Runtime settings: `96` metadata reader threads, async depth `256`,
    READDIRPLUS page `262144`, `--profile-data-reads`,
    `--data-sample-rate 1000000000`,
    `--data-sample-max-files-per-phase 1`,
    `--data-sample-max-bytes-per-phase 67108864`,
    `--data-sample-large-read-bytes 1048576`, stats interval `10s`.
  - First healthy progress: `501.7M` files in `70s` (`7.17M files/s`
    cumulative), `0` failed folders, `0` permission errors.
  - 2026-05-17 13:07 PDT status: still running; `1.409B` files in `220s`,
    `6.40M files/s` cumulative, `0` failed folders, `0` permission errors.
    Decision: performance is good enough, so keep this as the full long test.
  - Completed full run:
    `files=5,474,075,969`, `folders=103,286,279`, `failed_folders=0`,
    `wall_seconds=1790.99`, `max_rss_kb=43,530,436`.
  - Data-read sampling was enabled and recorded sparse samples:
    `3` small-file samples (`202,580` bytes total) and `1` large-file sample
    (`254,056` bytes), with `0` sample read failures.
  - Archived in git under:
    `doc/profiles/source-nfs-whole-100mphase-data-sampled-20260517T200342Z.profile.txt`
    and matching `.time.txt`.
