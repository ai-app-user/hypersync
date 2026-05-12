#!/usr/bin/env sh
set -eu

fail() {
  echo "install-hypersync: $*" >&2
  exit 1
}

info() {
  echo "install-hypersync: $*"
}

version="${HYPERSYNC_VERSION:-0.0.2}"
arch="${HYPERSYNC_ARCH:-$(uname -m)}"
install_dir="${INSTALL_DIR:-$HOME/hypersync}"
archive_name="${HYPERSYNC_ARCHIVE_NAME:-hypersync-linux-$arch.tar.gz}"
base_url="${HYPERSYNC_BASE_URL:-https://github.com/ai-app-user/hypersync/releases/download/v$version}"
archive_url="${HYPERSYNC_ARCHIVE_URL:-$base_url/$archive_name}"
checksum_url="${HYPERSYNC_CHECKSUM_URL:-$archive_url.sha256}"

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum is required"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

curl_download() {
  url="$1"
  output="$2"
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fL --retry 3 -H "Authorization: Bearer $GITHUB_TOKEN" "$url" -o "$output"
  else
    curl -fL --retry 3 "$url" -o "$output"
  fi
}

info "downloading $archive_url"
curl_download "$archive_url" "$tmp_dir/$archive_name"

info "downloading checksum"
curl_download "$checksum_url" "$tmp_dir/$archive_name.sha256"

(
  cd "$tmp_dir"
  sha256sum -c "$archive_name.sha256"
)

info "installing to $install_dir"
rm -rf "$install_dir"
mkdir -p "$install_dir"
tar -xzf "$tmp_dir/$archive_name" -C "$install_dir" --strip-components=1

[ -x "$install_dir/hypersync" ] || fail "installed launcher is not executable: $install_dir/hypersync"

info "verifying installed executable"
"$install_dir/hypersync" --version

cat <<EOF

Hypersync is ready.

Run it with:
  $install_dir/hypersync --version

Optional PATH setup:
  export PATH="$install_dir:\$PATH"
EOF
