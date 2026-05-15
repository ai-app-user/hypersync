#!/usr/bin/env bash
set -euo pipefail

# Apply the current transfer1 high-throughput NFS reader network profile.
# This is intentionally host-runtime tuning: rebooting or replacing the VM can
# reset these values, so run this before performance-sensitive scans/reads.

IFACE="${1:-ens3}"
NODE0_MASK="${HYPERSYNC_RPS_MASK:-00000000,00000000,0000ffff,ffffffff,ffffffff}"
RPS_FLOW_CNT="${HYPERSYNC_RPS_FLOW_CNT:-32768}"
RPS_SOCK_FLOW_ENTRIES="${HYPERSYNC_RPS_SOCK_FLOW_ENTRIES:-262144}"
READ_AHEAD_KB="${HYPERSYNC_NFS_READ_AHEAD_KB:-16384}"

sudo ip link set dev "${IFACE}" mtu 9000
sudo ethtool -G "${IFACE}" rx 8192 tx 8192
sudo ethtool -C "${IFACE}" adaptive-rx off rx-usecs 12 >/dev/null 2>&1 || \
    sudo ethtool -C "${IFACE}" rx-usecs 12 >/dev/null 2>&1 || true

sudo sysctl -w "net.core.rps_sock_flow_entries=${RPS_SOCK_FLOW_ENTRIES}" >/dev/null

for queue in "/sys/class/net/${IFACE}"/queues/rx-*; do
    echo "${NODE0_MASK}" | sudo tee "${queue}/rps_cpus" >/dev/null
    echo "${RPS_FLOW_CNT}" | sudo tee "${queue}/rps_flow_cnt" >/dev/null
done

for queue in "/sys/class/net/${IFACE}"/queues/tx-*; do
    echo "${NODE0_MASK}" | sudo tee "${queue}/xps_cpus" >/dev/null
done

while sudo iptables -t mangle -S FORWARD | grep -q TCPMSS; do
    rule="$(sudo iptables -t mangle -S FORWARD | grep TCPMSS | head -1 | sed 's/^-A /-D /')"
    sudo iptables -t mangle ${rule} || break
done

for sysctl_file in \
    /proc/sys/sunrpc/tcp_max_slot_table_entries \
    /proc/sys/sunrpc/tcp_slot_table_entries; do
    if [[ -e "${sysctl_file}" ]]; then
        echo 65536 | sudo tee "${sysctl_file}" >/dev/null
    fi
done

for bdi in /sys/class/bdi/*; do
    [[ -f "${bdi}/read_ahead_kb" ]] || continue
    current="$(cat "${bdi}/read_ahead_kb")"
    if [[ "${current}" =~ ^[0-9]+$ ]] && (( current >= 1024 )); then
        echo "${READ_AHEAD_KB}" | sudo tee "${bdi}/read_ahead_kb" >/dev/null
    fi
done

echo "mtu=$(cat "/sys/class/net/${IFACE}/mtu")"
echo "ring=$(ethtool -g "${IFACE}" | awk '/Current hardware settings:/ {cur=1} cur && /RX:/ {rx=$2} cur && /TX:/ {tx=$2} END {print rx "/" tx}')"
echo "rps_sock=$(cat /proc/sys/net/core/rps_sock_flow_entries)"
echo "rps=$(cat "/sys/class/net/${IFACE}/queues/rx-0/rps_cpus")"
echo "rps_flow=$(cat "/sys/class/net/${IFACE}/queues/rx-0/rps_flow_cnt")"
echo "xps=$(cat "/sys/class/net/${IFACE}/queues/tx-0/xps_cpus")"
echo "tcp_mss_rules=$(sudo iptables -t mangle -S | grep -c TCPMSS || true)"
for sysctl_file in \
    /proc/sys/sunrpc/tcp_max_slot_table_entries \
    /proc/sys/sunrpc/tcp_slot_table_entries; do
    [[ -e "${sysctl_file}" ]] && echo "${sysctl_file}=$(cat "${sysctl_file}")"
done
for bdi in /sys/class/bdi/*; do
    [[ -f "${bdi}/read_ahead_kb" ]] || continue
    current="$(cat "${bdi}/read_ahead_kb")"
    if [[ "${current}" =~ ^[0-9]+$ ]] && (( current >= 1024 )); then
        echo "bdi $(basename "${bdi}") read_ahead_kb=${current}"
    fi
done
