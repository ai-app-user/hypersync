# Hypersync Deploy Bundle

This folder owns Linux deployment packaging for Hypersync.

The deployment goal is a relocatable folder or tarball that can be copied to a
Linux server and run without installing DuckDB, libnfs, or other optional
runtime libraries globally.

## Bundle Layout

`package-linux.sh` creates a flat folder. The release artifact should be simple
enough that users can inspect it with `ls` and immediately see what matters:

```text
hypersync-linux-<arch>/
  hypersync                 # launcher script; this is what users run
  hypersync.bin             # compiled executable, with libnfs and app C++ runtime linked statically
  default.yaml              # default config
  libduckdb.so*             # DuckDB and Parquet writer support
  libstdc++.so*             # optional: bundled if the DuckDB shared library needs it
  libgcc_s.so*              # optional: bundled if the DuckDB shared library needs it
  README.txt
  manifest.txt
  checksums.sha256
```

Run the deployed tool with one command from the bundle folder:

```bash
./hypersync --version
./hypersync scan --source nfs://server/export/path --output scan.parquet --output-format parquet
./hypersync diff --source nfs://source/export/path --target nfs://target/export/path --compare size --output diff.csv
```

## New Server UX

The operator experience on a new Linux server is one shell command. The command
downloads the release archive, verifies the checksum, unpacks it into
`$HOME/hypersync` by default, and runs `./hypersync --version`.

```bash
curl -fsSL https://raw.githubusercontent.com/ai-app-user/hypersync/main/deploy/install-hypersync.sh | sh
```

For a private GitHub release, export a token first so both the script download
and release asset download can authenticate:

```bash
export GITHUB_TOKEN=...
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://raw.githubusercontent.com/ai-app-user/hypersync/main/deploy/install-hypersync.sh | sh
```

Useful installer overrides:

```bash
curl -fsSL https://raw.githubusercontent.com/ai-app-user/hypersync/main/deploy/install-hypersync.sh | \
  HYPERSYNC_VERSION=0.0.2 INSTALL_DIR=/opt/hypersync sh
```

After install, users run:

```bash
$HOME/hypersync/hypersync --version
$HOME/hypersync/hypersync scan --source /tmp --output /tmp/hypersync-smoke.csv --output-format csv --max-duration-seconds 5
```

To copy or sync data, start a receiver on the target server and run `sync` from
the source server:

```bash
# target server
$HOME/hypersync/hypersync receive --target /mnt/target-data --bind-host 0.0.0.0

# source server
$HOME/hypersync/hypersync sync --source nfs://source-server/export/path --host target-server
```

`send`, `sync`, and `copy` are aliases for the sender side of the same runtime
pipeline. The receiver target can be a local folder or an `nfs://...` URL when
the target export is writable.

For servers without outbound internet, copy the same `.tar.gz` and checksum
with `scp`, `rsync`, or an internal artifact system. Then unpack into one
folder and run the launcher:

```bash
mkdir -p "$HOME/hypersync"
tar -xzf hypersync-linux-x86_64.tar.gz -C "$HOME/hypersync" --strip-components=1
$HOME/hypersync/hypersync --version
```

The `hypersync` launcher is the supported entrypoint. It keeps the bundle
relocatable by setting `LD_LIBRARY_PATH` to its own directory before starting
`hypersync.bin`. Keep the launcher, binary, and `.so` files together.

## Runtime Libraries

The target production bundle should be small:

```text
hypersync                 launcher
hypersync.bin             about 3.2 MiB stripped in the current mostly-static build
libduckdb.so              about 67 MiB in the current DuckDB 1.5.2 bundle
default.yaml
README.txt
manifest.txt
checksums.sha256
```

The app should statically link its own C++ runtime and `libnfs`, so users do not
need to install libnfs. The current practical blocker to a single binary is
DuckDB: the available `libduckdb_static.a` is not self-contained for this
project, so Parquet support currently requires bundling `libduckdb.so`.

If the bundled `libduckdb.so` depends on `libstdc++.so.6` or `libgcc_s.so.1`,
package those beside it as well. Do not make users install them. Avoid bundling
glibc or the dynamic loader by default; build on an old enough Linux baseline
for compatibility with modern distributions.

The packager records full `ldd` output in `manifest.txt`, and
`checksums.sha256` records every file shipped in the bundle.

Observed transfer1 build sizes:

```text
variant                                      stripped size
static libnfs + static app C++ runtime         2.77 MiB
fully static libnfs build without DuckDB       3.74 MiB
static libnfs + static app runtime + DuckDB    3.19 MiB app + 67.04 MiB libduckdb.so
```

Static DuckDB is still a release goal, but it requires building a proper
self-contained DuckDB static library or amalgamation with the needed Parquet
support included.

## Build A Bundle On Linux

From the workspace root that contains sibling `piper/` and `hypersync/`:

```bash
hypersync/deploy/package-linux.sh
```

The script builds `build/release/hypersync`, stages the binary, copies runtime
libraries reported by `ldd`, writes a manifest, verifies `--version` from the
bundle, and creates a `.tar.gz` archive.

When DuckDB and libnfs are staged locally instead of installed globally, pass
their build flags to `make` through the environment:

```bash
duck="$PWD/third_party/duckdb-v1.5.2"
nfs="$PWD/third_party/libnfs"

MAKE_ARGS='release -j64' \
DUCKDB_CFLAGS="-I$duck/include" \
DUCKDB_LIBS="-L$duck/lib -lduckdb -Wl,-rpath,$duck/lib" \
LIBNFS_CFLAGS="-I$nfs/include" \
LIBNFS_LIBS="-L$nfs/lib -lnfs -Wl,-rpath,$nfs/lib" \
hypersync/deploy/package-linux.sh
```

## Useful Environment Variables

- `WORKSPACE_ROOT`: workspace root; defaults to the parent of this repo.
- `PIPER_REPO`: piper repo path for manifest metadata; defaults to
  `$WORKSPACE_ROOT/piper`.
- `HYPERSYNC_BIN`: existing Linux binary to package instead of the default
  `build/release/hypersync`.
- `DEPLOY_OUTPUT_DIR`: bundle directory; defaults to
  `hypersync/deploy/package/hypersync-linux-<arch>`.
- `BUILD`: set to `0` to skip `make release` and package an existing binary.
- `MAKE_ARGS`: make arguments; defaults to `release -j$(nproc)`.
- `CREATE_ARCHIVE`: set to `0` to skip creating the `.tar.gz` archive.
- `EXTRA_LIBS`: colon-separated extra `.so` files to copy into the bundle.
- `COPY_SYSTEM_LIBS`: set to `1` to copy every resolved `ldd` library except
  the dynamic loader and `linux-vdso`. The default copies portable optional
  runtime libraries such as DuckDB, libnfs, OpenSSL, zstd, snappy, lz4, zlib,
  and related compression/crypto dependencies.

## Notes

Build the deployment bundle on the same Linux distribution family that will run
it. The bundle intentionally does not copy the glibc dynamic loader by default.
