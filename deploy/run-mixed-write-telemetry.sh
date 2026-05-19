#!/usr/bin/env bash
set -euo pipefail

storage_probe_ip="${STORAGE_PROBE_IP:-172.27.255.2}"
profile_url="${PROFILE_URL:-synthetic-profile:///home/ubuntu/wsync-codex/wsync-latest/hypersync/doc/profiles/source-nfs-whole-100mphase-data-sampled-20260517T200342Z.profile.txt?payload=zero&files-per-batch=1024}"
target_base="${TARGET_BASE:-/volumes/8ed98ee4-b263-4319-be97-2093377beb65}"
target_mount="${TARGET_MOUNT:-/mnt/8ed98ee4-b263-4319-be97-2093377beb65}"
target_ips="${TARGET_IPS:-172.27.255.2-172.27.255.17}"
duration="${DURATION_SECONDS:-30}"
sample_seconds="${SAMPLE_SECONDS:-35}"
app="${HYPERSYNC_BIN:-./build/release/hypersync}"

nic="$(ip route get "$storage_probe_ip" | awk '{for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')"
if [[ -z "$nic" ]]; then
    echo "failed to resolve NIC for $storage_probe_ip" >&2
    exit 1
fi

test_name="${TEST_NAME:-mixed-telemetry-$(date +%Y%m%dT%H%M%S)}"
target_url="nfs://${target_ips}${target_base}/${test_name}"
target_path="${target_mount}/${test_name}"
outdir="${OUTDIR:-/tmp/hypersync-telemetry-${test_name}}"
mkdir -p "$outdir"

sudo -n rm -rf "$target_path" >/dev/null 2>&1 || true
sudo -n mkdir -p "$target_path"
sudo -n chmod 777 "$target_path" || true

echo "telemetry_dir=$outdir"
echo "nic=$nic"
echo "target=$target_path"
echo "pipeline: [FolderSeeder/MetaWork-1]->(FolderQueue)->[MetaReader-SYN-96]->(FolderReadyQueue-4096)->[FolderCreation-NFS-8]->[ReadyClassifier+Spillway]->(SmallReadyFileQueue-500000)->[DataReader-SYN-768/direct-submit]->[DataWriter-NFS/reactors=64 window=64] + (MediumReadyFileQueue-500000 + MediumSpillway)->[DataReader-SYN-96]->(DataQueue-96x512)->[DataWriter-NFS-96] + (LargeReadyFileQueue-500000 + LargeSpillway)->[DataReader-SYN-160]->(DataQueue-160x512)->[DataWriter-NFS-160]"

sudo -n ethtool -S "$nic" > "$outdir/nic_stats_before.log" || true
sar -n DEV 1 "$sample_seconds" > "$outdir/sweep_pps.log" 2>&1 &
sar_pid=$!
mpstat -P ALL 1 "$sample_seconds" > "$outdir/mpstat_all.log" 2>&1 &
mpstat_pid=$!

set +e
sudo -n "$app" benchmark-data-write \
    --mode folder-ready-mixed-write \
    --source "$profile_url" \
    --target "$target_url" \
    --meta-reader-threads 96 \
    --metadata-async-depth 256 \
    --data-writer-threads 8 \
    --data-writer-direct-submit \
    --data-reader-threads 768 \
    --data-outstanding-requests 256 \
    --small-file-async-window 256 \
    --max-file-size-bytes 1048575 \
    --max-files-queued 500000 \
    --data-queue-depth 512 \
    --pack-small-files \
    --skip-target-metadata \
    --no-target-fsync \
    --max-duration-seconds "$duration" \
    --stats-interval-seconds 5 | tee "$outdir/benchmark.log"
bench_status="${PIPESTATUS[0]}"
set -e

sudo -n ethtool -S "$nic" > "$outdir/nic_stats_after.log" || true
wait "$sar_pid" || true
wait "$mpstat_pid" || true

python3 - "$outdir" "$nic" <<'PY'
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])
nic = sys.argv[2]
stat_pattern = re.compile(r"^\s*([^:]+):\s*(-?\d+)")

def read_nic_stats(path):
    values = {}
    for line in path.read_text(errors="ignore").splitlines():
        match = stat_pattern.match(line)
        if match:
            values[match.group(1).strip()] = int(match.group(2))
    return values

before = read_nic_stats(out / "nic_stats_before.log")
after = read_nic_stats(out / "nic_stats_after.log")
error_words = ("drop", "fifo", "miss", "discard", "overrun", "alloc_fail")
print("NIC_ERROR_DELTAS")
printed = False
for key in sorted(set(before) | set(after)):
    if any(word in key.lower() for word in error_words):
        delta = after.get(key, 0) - before.get(key, 0)
        if delta:
            printed = True
            print(f"{key}={delta}")
if not printed:
    print("none_nonzero")

sar_rows = []
for line in (out / "sweep_pps.log").read_text(errors="ignore").splitlines():
    parts = line.split()
    if len(parts) >= 10 and parts[1] == nic and parts[0] != "Average:":
        try:
            sar_rows.append((float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5]), float(parts[9]), line))
        except ValueError:
            pass
print("SAR_AVERAGE")
for line in (out / "sweep_pps.log").read_text(errors="ignore").splitlines():
    parts = line.split()
    if len(parts) >= 10 and parts[0] == "Average:" and parts[1] == nic:
        print(line)
print("SAR_PEAK_TXPPS")
for row in sorted(sar_rows, key=lambda value: value[1], reverse=True)[:5]:
    print(row[-1])

averages = []
samples = []
for line in (out / "mpstat_all.log").read_text(errors="ignore").splitlines():
    parts = line.split()
    if len(parts) < 12:
        continue
    try:
        if parts[0] == "Average:":
            cpu = parts[1]
            averages.append((float(parts[7]), cpu, line))
        elif parts[0][0].isdigit() and parts[1] != "CPU":
            cpu = parts[1]
            if cpu != "all":
                samples.append((float(parts[7]), cpu, line))
    except ValueError:
        continue
print("MPSTAT_AVG_ALL")
for soft, cpu, line in averages:
    if cpu == "all":
        print(line)
print("MPSTAT_TOP_AVG_SOFT_CORES")
for _, _, line in sorted([row for row in averages if row[1] != "all"], reverse=True)[:16]:
    print(line)
print("MPSTAT_TOP_SAMPLE_SOFT_CORES")
for _, _, line in sorted(samples, reverse=True)[:16]:
    print(line)
PY

exit "$bench_status"
