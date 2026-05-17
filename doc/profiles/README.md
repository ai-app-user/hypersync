# Captured Profiles

This folder stores compact workload profiles that are small enough to keep in
git and useful as regression baselines or synthetic replay inputs.

## source-nfs-whole-100mphase-20260517T184054Z

Whole-source direct-libnfs metadata profile captured on transfer1 as root.

- Source: `nfs://172.27.255.18-33/volumes/e27faf8c-36a5-4571-8324-4c38a5dce0a5`
- Command: `benchmark-nfs-profile --max-records 10000000000 --phase-count 100 --meta-reader-threads 96 --metadata-async-depth 256 --readdirplus-page-bytes 262144`
- Phase target: `100M` files per phase
- Files observed: `5,473,804,389`
- Folders observed: `93,280,540`
- Failed folders: `0`
- Logical size: `7,548,229,507,007,918` bytes
- Small files: `3,970,514,982`
- Large files: `1,503,289,407`
- Wall time: `1970.15s`
- Max RSS: `33,263,404 KB`

Note: this run used the metadata-only profiler before replay-quality topology
and sampled data-read fields were added. Keep it as a historical whole-source
baseline; capture a newer profile when topology/data-read replay inputs are
needed.
