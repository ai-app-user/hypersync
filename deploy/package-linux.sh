#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "package-linux: $*" >&2
  exit 1
}

info() {
  echo "package-linux: $*"
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
hypersync_repo="$(cd -- "$script_dir/.." && pwd -P)"
workspace_root="${WORKSPACE_ROOT:-$(cd -- "$hypersync_repo/.." && pwd -P)}"
piper_repo="${PIPER_REPO:-$workspace_root/piper}"

if [[ "$(uname -s)" != "Linux" ]]; then
  die "Linux deployment bundles must be produced on Linux so ldd and ELF dependencies match the target platform"
fi

build="${BUILD:-1}"
make_args="${MAKE_ARGS:-release -j$(nproc)}"
binary="${HYPERSYNC_BIN:-$workspace_root/build/release/hypersync}"
arch="$(uname -m)"
bundle_dir="${DEPLOY_OUTPUT_DIR:-$script_dir/package/hypersync-linux-$arch}"
archive_path="${ARCHIVE_PATH:-$bundle_dir.tar.gz}"
create_archive="${CREATE_ARCHIVE:-1}"
copy_system_libs="${COPY_SYSTEM_LIBS:-0}"
extra_libs="${EXTRA_LIBS:-}"

if [[ "$build" != "0" ]]; then
  [[ -f "$workspace_root/Makefile" ]] || die "cannot find Makefile at $workspace_root; set WORKSPACE_ROOT or BUILD=0"
  info "building release binary from $workspace_root"
  read -r -a make_words <<< "$make_args"
  make_cmd=(make -C "$workspace_root" "${make_words[@]}")
  for make_var in CXX CXXFLAGS LDFLAGS RELEASE_CXXFLAGS RELEASE_LDFLAGS LIBNFS_CFLAGS LIBNFS_LIBS DUCKDB_CFLAGS DUCKDB_LIBS; do
    if [[ -n "${!make_var:-}" ]]; then
      make_cmd+=("$make_var=${!make_var}")
    fi
  done
  "${make_cmd[@]}"
fi

[[ -x "$binary" ]] || die "expected executable binary at $binary"
command -v ldd >/dev/null 2>&1 || die "ldd is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"

rm -rf "$bundle_dir"
mkdir -p "$bundle_dir"

cp "$binary" "$bundle_dir/hypersync.bin"
cp "$script_dir/run-hypersync" "$bundle_dir/hypersync"
chmod 0755 "$bundle_dir/hypersync" "$bundle_dir/hypersync.bin"

if [[ -f "$hypersync_repo/config/default.yaml" ]]; then
  cp "$hypersync_repo/config/default.yaml" "$bundle_dir/default.yaml"
fi

should_copy_library() {
  local base="$1"
  if [[ "$copy_system_libs" == "1" ]]; then
    [[ "$base" != ld-linux* && "$base" != "linux-vdso.so.1" ]]
    return
  fi

  case "$base" in
    libduckdb.so*|libnfs.so*|libstdc++.so*|libgcc_s.so*|libssl.so*|libcrypto.so*|libzstd.so*|libsnappy.so*|liblz4.so*|libz.so*|libzlib.so*|libbrotli*.so*|libnghttp2.so*|libtirpc.so*|libkrb5*.so*|libgssapi*.so*|libk5crypto.so*|libcom_err.so*|libkeyutils.so*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

copy_library() {
  local path="$1"
  local soname="${2:-}"

  [[ -n "$path" && -e "$path" ]] || return 0

  local real
  real="$(readlink -f "$path")"
  local real_base
  real_base="$(basename "$real")"
  cp -L "$real" "$bundle_dir/$real_base"

  if [[ -n "$soname" && "$soname" != "$real_base" ]]; then
    ln -sf "$real_base" "$bundle_dir/$soname"
  fi

  local path_base
  path_base="$(basename "$path")"
  if [[ "$path_base" != "$real_base" && "$path_base" != "$soname" ]]; then
    ln -sf "$real_base" "$bundle_dir/$path_base"
  fi
}

info "copying runtime libraries"
while IFS= read -r line; do
  line="${line#"${line%%[![:space:]]*}"}"

  soname=""
  path=""
  if [[ "$line" == *" => "* ]]; then
    soname="${line%% => *}"
    rest="${line#* => }"
    path="${rest%% (*}"
  else
    first="${line%% (*}"
    if [[ "$first" == /* ]]; then
      path="$first"
      soname="$(basename "$first")"
    fi
  fi

  [[ -n "$path" && "$path" == /* ]] || continue
  base="${soname:-$(basename "$path")}"
  if should_copy_library "$base"; then
    copy_library "$path" "$base"
  fi
done < <(ldd "$binary")

if [[ -n "$extra_libs" ]]; then
  IFS=':' read -r -a extra_paths <<< "$extra_libs"
  for lib in "${extra_paths[@]}"; do
    [[ -n "$lib" ]] || continue
    [[ -e "$lib" ]] || die "EXTRA_LIBS entry does not exist: $lib"
    copy_library "$lib" "$(basename "$lib")"
  done
fi

cat > "$bundle_dir/README.txt" <<'README'
Hypersync Linux deploy bundle

Run:
  ./hypersync --version

The hypersync wrapper sets LD_LIBRARY_PATH to this directory before executing
hypersync.bin. Keep hypersync, hypersync.bin, and the .so files together.

For direct NFS scans, pass nfs:// URLs instead of kernel-mounted paths.

Runtime library families that may be present:
  libduckdb.so*              DuckDB and Parquet writer support
  libnfs.so*                 Direct libnfs access for nfs:// URLs
  libssl.so*, libcrypto.so*  OpenSSL runtime dependencies when linked
  libzstd.so*                Zstandard compression when linked
  libsnappy.so*              Snappy compression when linked
  liblz4.so*                 LZ4 compression when linked
  libz.so*                   zlib compression when linked
  libstdc++.so*              C++ runtime when needed on the target host
  libgcc_s.so*               GCC runtime when needed on the target host
  libtirpc.so*               RPC dependency when required
  libgssapi*.so*             Kerberos/GSSAPI dependency when required
  libkrb5*.so*               Kerberos dependency when required
  libk5crypto.so*            Kerberos crypto dependency when required
  libcom_err.so*             Kerberos/platform dependency when required
  libkeyutils.so*            Kerberos/platform dependency when required

Not every bundle contains every library family above. See manifest.txt for the
exact ldd output from the packaged executable, and checksums.sha256 for every
file shipped in this folder.
README

{
  echo "created_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "host=$(hostname)"
  echo "arch=$arch"
  echo "binary=$binary"
  echo "hypersync_git_commit=$(git -C "$hypersync_repo" rev-parse HEAD 2>/dev/null || true)"
  echo "hypersync_git_dirty=$(git -C "$hypersync_repo" status --short 2>/dev/null | wc -l | tr -d ' ')"
  echo "piper_git_commit=$(git -C "$piper_repo" rev-parse HEAD 2>/dev/null || true)"
  echo "piper_git_dirty=$(git -C "$piper_repo" status --short 2>/dev/null | wc -l | tr -d ' ')"
  echo
  echo "[ldd]"
  LD_LIBRARY_PATH="$bundle_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ldd "$bundle_dir/hypersync.bin" || true
} > "$bundle_dir/manifest.txt"

(
  cd "$bundle_dir"
  find . -type f -print0 | sort -z | xargs -0 sha256sum
) > "$bundle_dir/checksums.sha256"

info "verifying staged executable"
LD_LIBRARY_PATH="$bundle_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$bundle_dir/hypersync" --version

if [[ "$create_archive" != "0" ]]; then
  rm -f "$archive_path"
  tar -C "$(dirname "$bundle_dir")" -czf "$archive_path" "$(basename "$bundle_dir")"
  info "archive written to $archive_path"
fi

info "bundle written to $bundle_dir"
