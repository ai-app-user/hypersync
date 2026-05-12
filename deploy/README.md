# Hypersync Deploy Bundle

This folder owns Linux deployment packaging for Hypersync.

The deployment goal is a relocatable folder or tarball that can be copied to a
Linux server and run without installing DuckDB, libnfs, or other optional
runtime libraries globally.

## Bundle Layout

`package-linux.sh` creates this layout:

```text
hypersync-linux-<arch>/
  bin/
    hypersync        # wrapper that sets LD_LIBRARY_PATH
    hypersync.bin    # compiled executable
  lib/
    *.so*            # copied runtime libraries
  config/
    default.yaml
  doc/
    README.txt
  manifest.txt
  checksums.sha256
```

Run the deployed tool through the wrapper:

```bash
./bin/hypersync --version
./bin/hypersync scan --source nfs://server/export/path --output scan.parquet --output-format parquet
```

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
- `EXTRA_LIBS`: colon-separated extra `.so` files to copy into `lib/`.
- `COPY_SYSTEM_LIBS`: set to `1` to copy every resolved `ldd` library except
  the dynamic loader and `linux-vdso`. The default copies portable optional
  runtime libraries such as DuckDB, libnfs, OpenSSL, zstd, snappy, lz4, zlib,
  and related compression/crypto dependencies.

## Notes

Build the deployment bundle on the same Linux distribution family that will run
it. The bundle intentionally does not copy the glibc dynamic loader by default.
