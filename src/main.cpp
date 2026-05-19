#include "hypersync.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cctype>
#include <deque>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sstream>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <unistd.h>
#endif

namespace {

std::string require_option(const std::vector<std::string>& args, std::size_t& index, std::string_view name) {
    if (index + 1 >= args.size()) {
        throw std::runtime_error("missing value for option " + std::string(name));
    }
    ++index;
    return args[index];
}

std::uint16_t parse_port(const std::string& value, std::string_view flag_name) {
    const int port = std::stoi(value);
    if (port < 1 || port > 65535) {
        throw std::runtime_error("invalid port for " + std::string(flag_name));
    }
    return static_cast<std::uint16_t>(port);
}

std::size_t parse_size_t_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(value, &consumed);
    if (consumed != value.size() || parsed == 0) {
        throw std::runtime_error("invalid positive integer for " + std::string(flag_name));
    }
    return static_cast<std::size_t>(parsed);
}

std::uint64_t parse_u64_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) {
        throw std::runtime_error("invalid integer for " + std::string(flag_name));
    }
    return static_cast<std::uint64_t>(parsed);
}

double parse_positive_double_option(const std::string& value, std::string_view flag_name) {
    std::size_t consumed = 0;
    const double parsed = std::stod(value, &consumed);
    if (consumed != value.size() || parsed <= 0.0) {
        throw std::runtime_error("invalid positive number for " + std::string(flag_name));
    }
    return parsed;
}

std::string trim_copy(std::string value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string shell_quote(std::string_view value) {
    std::string quoted = "'";
    for (const char ch : value) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

std::string command_output(const std::string& command) {
    std::string output;
#if defined(_WIN32)
    (void)command;
#else
    FILE* pipe = popen((command + " 2>/dev/null").c_str(), "r");
    if (pipe == nullptr) {
        return output;
    }
    std::array<char, 4096> buffer {};
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output += buffer.data();
    }
    pclose(pipe);
#endif
    return output;
}

std::string read_text_file(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        return {};
    }
    std::stringstream buffer;
    buffer << input.rdbuf();
    return trim_copy(buffer.str());
}

std::string linux_online_cpu_mask() {
    std::string online = read_text_file("/sys/devices/system/cpu/online");
    std::size_t max_cpu = 0;
    bool found = false;
    std::stringstream stream(online.empty() ? "0" : online);
    std::string range;
    while (std::getline(stream, range, ',')) {
        const std::size_t dash = range.find('-');
        const std::string last = dash == std::string::npos ? range : range.substr(dash + 1U);
        if (!last.empty()) {
            max_cpu = std::max<std::size_t>(max_cpu,
                                            static_cast<std::size_t>(parse_u64_option(last, "cpu-online")));
            found = true;
        }
    }
    if (!found) {
        max_cpu = std::max<unsigned int>(1U, std::thread::hardware_concurrency()) - 1U;
    }

    const std::size_t group_count = (max_cpu / 32U) + 1U;
    std::vector<std::uint32_t> groups(group_count, 0);
    for (std::size_t cpu = 0; cpu <= max_cpu; ++cpu) {
        groups[cpu / 32U] |= (1U << (cpu % 32U));
    }

    std::ostringstream out;
    for (std::size_t index = group_count; index > 0; --index) {
        if (index != group_count) {
            out << ',';
        }
        out << std::hex << std::setw(8) << std::setfill('0') << groups[index - 1U];
    }
    return out.str();
}

struct TuningOptions {
    std::string iface = "ens3";
    bool apply = false;
    std::string cpu_mask;
    std::string peer;
    std::uint64_t rps_flow_cnt = 32768;
    std::uint64_t rps_sock_flow_entries = 262144;
    std::uint64_t mtu = 9000;
    std::uint64_t ring = 8192;
    std::uint64_t nfs_readahead_kb = 16384;
};

std::string sysctl_value(std::string_view key) {
    return trim_copy(command_output("sysctl -n " + std::string(key)));
}

bool output_contains_current_ring(const std::string& ethtool_ring, std::string_view key, std::string_view value) {
    const std::size_t current = ethtool_ring.find("Current hardware settings:");
    if (current == std::string::npos) {
        return false;
    }
    const std::string tail = ethtool_ring.substr(current);
    return tail.find(std::string(key) + ":\t\t\t" + std::string(value)) != std::string::npos ||
           tail.find(std::string(key) + ":\t" + std::string(value)) != std::string::npos;
}

std::optional<std::uint64_t> ethtool_named_value(const std::string& output, std::string_view key) {
    std::stringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        if (trim_copy(line.substr(0, colon)) != key) {
            continue;
        }
        const std::string value = trim_copy(line.substr(colon + 1U));
        if (value.empty()) {
            return std::nullopt;
        }
        return parse_u64_option(value, key);
    }
    return std::nullopt;
}

bool ethtool_flag_value(const std::string& output, std::string_view key, std::string_view expected) {
    std::stringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        if (trim_copy(line.substr(0, colon)) == key) {
            const std::string value = trim_copy(line.substr(colon + 1U));
            return value == expected ||
                   value.rfind(std::string(expected) + " ", 0) == 0;
        }
    }
    return false;
}

std::string normalize_cpu_mask(std::string mask) {
    mask = trim_copy(mask);
    std::vector<std::string> groups;
    std::stringstream stream(mask);
    std::string group;
    while (std::getline(stream, group, ',')) {
        group = trim_copy(group);
        const std::size_t non_zero = group.find_first_not_of('0');
        groups.push_back(non_zero == std::string::npos ? "0" : group.substr(non_zero));
    }
    while (groups.size() > 1U && groups.front() == "0") {
        groups.erase(groups.begin());
    }
    std::ostringstream out;
    for (std::size_t index = 0; index < groups.size(); ++index) {
        if (index != 0U) {
            out << ',';
        }
        out << groups[index];
    }
    return out.str();
}

std::uint64_t count_lines(const std::string& text) {
    if (text.empty()) {
        return 0;
    }
    return static_cast<std::uint64_t>(std::count(text.begin(), text.end(), '\n') +
                                      (text.back() == '\n' ? 0 : 1));
}

bool contains_mount_option(const std::string& options, std::string_view expected) {
    std::stringstream stream(options);
    std::string option;
    while (std::getline(stream, option, ',')) {
        if (trim_copy(option) == expected) {
            return true;
        }
    }
    return false;
}

std::string first_nfs_mount_peer() {
    const std::string mounts = command_output("findmnt -rn -t nfs,nfs4 -o OPTIONS | head -1");
    std::stringstream stream(mounts);
    std::string option;
    while (std::getline(stream, option, ',')) {
        option = trim_copy(option);
        constexpr std::string_view kAddr = "addr=";
        if (option.rfind(kAddr, 0) == 0) {
            return option.substr(kAddr.size());
        }
    }
    return {};
}

int run_tuning(const TuningOptions& options) {
    const std::string iface = options.iface;
    const std::string mask = options.cpu_mask.empty() ? linux_online_cpu_mask() : options.cpu_mask;
    const std::string qiface = shell_quote(iface);
    const std::string qmask = shell_quote(mask);
    const std::string sudo = [] {
#if defined(_WIN32)
        return std::string {};
#else
        return geteuid() == 0 ? std::string {} : std::string("sudo ");
#endif
    }();

    if (options.apply) {
        const std::vector<std::string> commands = {
            sudo + "ip link set dev " + qiface + " mtu " + std::to_string(options.mtu),
            sudo + "ethtool -G " + qiface + " rx " + std::to_string(options.ring) +
                " tx " + std::to_string(options.ring),
            sudo + "ethtool -C " + qiface + " adaptive-rx off rx-usecs 12",
            sudo + "sysctl -w net.core.rps_sock_flow_entries=" +
                std::to_string(options.rps_sock_flow_entries),
            sudo + "sysctl -w net.core.rmem_max=2147483647 net.core.wmem_max=2147483647",
            sudo + "sysctl -w 'net.ipv4.tcp_rmem=4096 1048576 2147483647' "
                "'net.ipv4.tcp_wmem=4096 1048576 2147483647'",
            sudo + "sysctl -w net.ipv4.tcp_congestion_control=bbr net.core.default_qdisc=fq "
                "net.ipv4.tcp_mtu_probing=1",
            sudo + "iptables -t mangle -D POSTROUTING -p tcp --tcp-flags SYN,RST SYN -j TCPMSS "
                "--clamp-mss-to-pmtu 2>/dev/null || true",
            "for q in /sys/class/net/" + iface + "/queues/rx-*; do echo " + qmask + " | " + sudo +
                "tee \"$q/rps_cpus\" >/dev/null; echo " + std::to_string(options.rps_flow_cnt) +
                " | " + sudo + "tee \"$q/rps_flow_cnt\" >/dev/null; done",
            "for q in /sys/class/net/" + iface + "/queues/tx-*; do echo " + qmask + " | " + sudo +
                "tee \"$q/xps_cpus\" >/dev/null; done",
            "for f in /proc/sys/sunrpc/tcp_max_slot_table_entries /proc/sys/sunrpc/tcp_slot_table_entries; "
                "do [ -e \"$f\" ] && echo 65536 | " + sudo + "tee \"$f\" >/dev/null || true; done",
            "for m in $(findmnt -rn -t nfs,nfs4 -o TARGET); do dev=$(stat -c %d \"$m\"); "
                "maj=$(( ((dev >> 20) & 0xfff) | ((dev >> 32) & ~0xfff) )); "
                "min=$(( (dev & 0xff) | ((dev >> 12) & ~0xff) )); "
                "f=/sys/class/bdi/$maj:$min/read_ahead_kb; "
                "[ -e \"$f\" ] && echo " + std::to_string(options.nfs_readahead_kb) + " | " + sudo +
                "tee \"$f\" >/dev/null || true; done",
        };
        for (const std::string& command : commands) {
            const int rc = std::system(command.c_str());
            if (rc != 0) {
                std::cerr << "tuning apply_failed command=" << command << " rc=" << rc << '\n';
            }
        }
    }

    std::size_t mismatches = 0;
    const auto check = [&](std::string name, std::string current, std::string expected) {
        const bool ok = trim_copy(current) == trim_copy(expected);
        if (!ok) {
            ++mismatches;
        }
        std::cout << "tuning " << (ok ? "ok" : "mismatch")
                  << " setting=" << name
                  << " current=" << shell_quote(trim_copy(current))
                  << " expected=" << shell_quote(trim_copy(expected)) << '\n';
    };

    check("mtu", read_text_file("/sys/class/net/" + iface + "/mtu"), std::to_string(options.mtu));
    check("net.core.rps_sock_flow_entries",
          read_text_file("/proc/sys/net/core/rps_sock_flow_entries"),
          std::to_string(options.rps_sock_flow_entries));
    check("net.core.rmem_max", sysctl_value("net.core.rmem_max"), "2147483647");
    check("net.core.wmem_max", sysctl_value("net.core.wmem_max"), "2147483647");
    check("net.ipv4.tcp_rmem", sysctl_value("net.ipv4.tcp_rmem"), "4096\t1048576\t2147483647");
    check("net.ipv4.tcp_wmem", sysctl_value("net.ipv4.tcp_wmem"), "4096\t1048576\t2147483647");
    check("net.ipv4.tcp_congestion_control", sysctl_value("net.ipv4.tcp_congestion_control"), "bbr");
    check("net.core.default_qdisc", sysctl_value("net.core.default_qdisc"), "fq");
    check("net.ipv4.tcp_mtu_probing", sysctl_value("net.ipv4.tcp_mtu_probing"), "1");

    const std::string driver = command_output("ethtool -i " + qiface + " | awk -F': ' '/^driver:/ {print $2}'");
    check("driver", driver, "mlx5_core");

    const std::string rings = command_output("ethtool -g " + qiface);
    const bool ring_ok = output_contains_current_ring(rings, "RX", std::to_string(options.ring)) &&
                         output_contains_current_ring(rings, "TX", std::to_string(options.ring));
    if (!ring_ok) {
        ++mismatches;
    }
    std::cout << "tuning " << (ring_ok ? "ok" : "mismatch")
              << " setting=rx_tx_ring expected='" << options.ring << "/" << options.ring << "'\n";

    const std::string coalesce = command_output("ethtool -c " + qiface);
    const bool coalesce_ok = ethtool_flag_value(coalesce, "Adaptive RX", "off") &&
                             ethtool_named_value(coalesce, "rx-usecs").value_or(0) == 12U;
    if (!coalesce_ok) {
        ++mismatches;
    }
    std::cout << "tuning " << (coalesce_ok ? "ok" : "mismatch")
              << " setting=rx_coalesce expected='adaptive-rx off rx-usecs 12'\n";

    const std::string rx_queue_count =
        trim_copy(command_output("ls -d /sys/class/net/" + iface + "/queues/rx-* 2>/dev/null | wc -l"));
    const std::string tx_queue_count =
        trim_copy(command_output("ls -d /sys/class/net/" + iface + "/queues/tx-* 2>/dev/null | wc -l"));
    std::cout << "tuning info setting=rx_tx_queues current="
              << shell_quote(rx_queue_count + "/" + tx_queue_count) << '\n';

    const std::string rx0 = read_text_file("/sys/class/net/" + iface + "/queues/rx-0/rps_cpus");
    const std::string rx0_flow = read_text_file("/sys/class/net/" + iface + "/queues/rx-0/rps_flow_cnt");
    const std::string tx0 = read_text_file("/sys/class/net/" + iface + "/queues/tx-0/xps_cpus");
    check("rx-0/rps_cpus", normalize_cpu_mask(rx0), normalize_cpu_mask(mask));
    check("rx-0/rps_flow_cnt", rx0_flow, std::to_string(options.rps_flow_cnt));
    check("tx-0/xps_cpus", normalize_cpu_mask(tx0), normalize_cpu_mask(mask));

    const std::string normalized_mask = normalize_cpu_mask(mask);
    const std::string all_rx_rps_ok = trim_copy(command_output(
        "for q in /sys/class/net/" + iface + "/queues/rx-*; do "
        "[ \"$(cat \"$q/rps_cpus\")\" = " + shell_quote(normalized_mask) + " ] || "
        "[ \"$(cat \"$q/rps_cpus\")\" = " + qmask + " ] || echo bad; done | wc -l"));
    const std::string all_rx_flow_ok = trim_copy(command_output(
        "for q in /sys/class/net/" + iface + "/queues/rx-*; do "
        "[ \"$(cat \"$q/rps_flow_cnt\")\" = " + shell_quote(std::to_string(options.rps_flow_cnt)) +
        " ] || echo bad; done | wc -l"));
    const std::string all_tx_xps_ok = trim_copy(command_output(
        "for q in /sys/class/net/" + iface + "/queues/tx-*; do "
        "[ \"$(cat \"$q/xps_cpus\")\" = " + shell_quote(normalized_mask) + " ] || "
        "[ \"$(cat \"$q/xps_cpus\")\" = " + qmask + " ] || echo bad; done | wc -l"));
    check("all_rx_rps_cpus", all_rx_rps_ok, "0");
    check("all_rx_rps_flow_cnt", all_rx_flow_ok, "0");
    check("all_tx_xps_cpus", all_tx_xps_ok, "0");

    check("irqbalance", trim_copy(command_output("systemctl is-active irqbalance || true")), "inactive");

    const std::string rpc_slots_max = read_text_file("/proc/sys/sunrpc/tcp_max_slot_table_entries");
    if (!rpc_slots_max.empty()) {
        check("rpc_slots_max", rpc_slots_max, "65536");
        std::cout << "tuning info setting=sunrpc.tcp_max_slot_table_entries current="
                  << shell_quote(rpc_slots_max) << '\n';
    }
    const std::string rpc_slots_current = read_text_file("/proc/sys/sunrpc/tcp_slot_table_entries");
    if (!rpc_slots_current.empty()) {
        check("rpc_slots_current", rpc_slots_current, "65536");
        std::cout << "tuning info setting=sunrpc.tcp_slot_table_entries current="
                  << shell_quote(rpc_slots_current) << '\n';
    }

    const std::string speed = command_output("ethtool " + qiface + " | awk -F': ' '/Speed:/ {print $2}'");
    if (!speed.empty()) {
        std::cout << "tuning info setting=speed current="
                  << shell_quote(trim_copy(speed)) << '\n';
    }
    const std::string channels = command_output("ethtool -l " + qiface);
    const std::optional<std::uint64_t> combined = ethtool_named_value(channels, "Combined");
    if (combined.has_value()) {
        std::cout << "tuning info setting=combined_channels current="
                  << combined.value() << '\n';
    }
    const std::string numa_node = read_text_file("/sys/class/net/" + iface + "/device/numa_node");
    if (!numa_node.empty()) {
        std::cout << "tuning info setting=nic_numa_node current="
                  << shell_quote(numa_node) << '\n';
    }
    const std::string peer = options.peer.empty() ? first_nfs_mount_peer() : options.peer;
    if (!peer.empty()) {
        const std::string pmtu_result = command_output("ping -c 1 -M do -s 8972 -W 1 " + shell_quote(peer) +
                                                       " >/dev/null && echo ok || echo fail");
        check("pmtu_9000_peer_" + peer, trim_copy(pmtu_result), "ok");
    }
    const std::string tcp_mss_rules =
        command_output("iptables -t mangle -S | grep -c TCPMSS || true");
    if (!tcp_mss_rules.empty() && trim_copy(tcp_mss_rules) != "0") {
        ++mismatches;
        std::cout << "tuning mismatch setting=tcpmss_rules current="
                  << shell_quote(trim_copy(tcp_mss_rules)) << " expected='0'\n";
    } else {
        std::cout << "tuning ok setting=tcpmss_rules current='0' expected='0'\n";
    }

    const std::string nfs_mounts =
        command_output("findmnt -rn -t nfs,nfs4 -o TARGET,SOURCE,OPTIONS || true");
    std::cout << "tuning info setting=optimized_nfs_mounts count="
              << count_lines(nfs_mounts) << '\n';
    if (!nfs_mounts.empty()) {
        std::cout << nfs_mounts;
        if (nfs_mounts.back() != '\n') {
            std::cout << '\n';
        }
    }
    std::stringstream mount_stream(nfs_mounts);
    std::string mount_line;
    std::size_t mount_index = 0;
    while (std::getline(mount_stream, mount_line)) {
        std::stringstream fields(mount_line);
        std::string target;
        std::string source;
        std::string mount_options;
        fields >> target >> source >> mount_options;
        if (target.empty() || mount_options.empty()) {
            continue;
        }
        const std::string prefix = "nfs_mount_" + std::to_string(mount_index) + "_";
        const auto check_mount_option = [&](std::string_view option) {
            const bool ok = contains_mount_option(mount_options, option);
            if (!ok) {
                ++mismatches;
            }
            std::cout << "tuning " << (ok ? "ok" : "mismatch")
                      << " setting=" << prefix << option
                      << " current=" << shell_quote(mount_options)
                      << " expected=" << shell_quote(option) << '\n';
        };
        check_mount_option("nconnect=32");
        check_mount_option("rsize=1048576");
        check_mount_option("wsize=1048576");
        check_mount_option("noatime");
        check_mount_option("nodiratime");
        check_mount_option("spread_reads");
        if (contains_mount_option(mount_options, "rw")) {
            check_mount_option("spread_writes");
        }
        const std::string read_ahead = trim_copy(command_output(
            "m=" + shell_quote(target) + "; dev=$(stat -c %d \"$m\"); "
            "maj=$(( ((dev >> 20) & 0xfff) | ((dev >> 32) & ~0xfff) )); "
            "min=$(( (dev & 0xff) | ((dev >> 12) & ~0xff) )); "
            "cat /sys/class/bdi/$maj:$min/read_ahead_kb 2>/dev/null"));
        if (!read_ahead.empty()) {
            check(prefix + "bdi_read_ahead_kb", read_ahead,
                  std::to_string(options.nfs_readahead_kb));
        }
        ++mount_index;
    }

    std::cout << "tuning summary iface=" << iface
              << " apply=" << (options.apply ? "true" : "false")
              << " mismatches=" << mismatches
              << " cpu_mask=" << shell_quote(mask) << '\n';
    return mismatches == 0U ? 0 : 2;
}

std::optional<std::string_view> profile_token(std::string_view line,
                                              std::string_view key) noexcept {
    const std::size_t begin = line.find(key);
    if (begin == std::string_view::npos) {
        return std::nullopt;
    }
    const std::size_t value_begin = begin + key.size();
    const std::size_t value_end = line.find(' ', value_begin);
    return line.substr(value_begin,
                       value_end == std::string_view::npos ? std::string_view::npos
                                                            : value_end - value_begin);
}

std::uint64_t profile_u64(std::string_view line,
                          std::string_view key,
                          std::uint64_t fallback = 0) {
    const std::optional<std::string_view> token = profile_token(line, key);
    if (!token.has_value()) {
        return fallback;
    }
    return parse_u64_option(std::string(token.value()), key);
}

void parse_profile_count_buckets(
    std::string_view line,
    std::string_view key,
    std::array<std::uint64_t, hypersync::kSyntheticSizeBucketCount>& counts,
    std::array<std::uint64_t, hypersync::kSyntheticSizeBucketCount>& bytes) {
    const std::optional<std::string_view> token = profile_token(line, key);
    if (!token.has_value()) {
        return;
    }
    std::stringstream stream(std::string(token.value()));
    std::string item;
    std::size_t index = 0;
    while (index < counts.size() && std::getline(stream, item, ',')) {
        const std::size_t colon = item.find(':');
        const std::size_t slash = item.find('/', colon == std::string::npos ? 0U : colon + 1U);
        if (colon != std::string::npos && slash != std::string::npos) {
            counts[index] = parse_u64_option(item.substr(colon + 1U, slash - colon - 1U),
                                             "size_buckets count");
            bytes[index] = parse_u64_option(item.substr(slash + 1U), "size_buckets bytes");
        }
        ++index;
    }
}

void parse_profile_simple_buckets(
    std::string_view line,
    std::string_view key,
    std::uint64_t* values,
    std::size_t value_count) {
    const std::optional<std::string_view> token = profile_token(line, key);
    if (!token.has_value()) {
        return;
    }
    std::stringstream stream(std::string(token.value()));
    std::string item;
    std::size_t index = 0;
    while (index < value_count && std::getline(stream, item, ',')) {
        const std::size_t colon = item.find(':');
        if (colon != std::string::npos) {
            values[index] = parse_u64_option(item.substr(colon + 1U), key);
        }
        ++index;
    }
}

hypersync::SyntheticWorkloadProfile load_synthetic_profile_text(
    const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open profile: " + path.string());
    }

    hypersync::SyntheticWorkloadProfile profile;
    std::string line;
    while (std::getline(input, line)) {
        if (line.rfind("phase ", 0) != 0) {
            continue;
        }
        hypersync::SyntheticPhaseProfile phase;
        phase.name = std::string(profile_token(line, "name=").value_or("phase"));
        phase.file_count = profile_u64(line, "files=");
        phase.folder_count = profile_u64(line, "folders=");
        phase.directory_count = profile_u64(line, "directories=");
        phase.empty_folder_count = profile_u64(line, "empty_folders=");
        phase.near_empty_folder_count = profile_u64(line, "near_empty_folders=");
        phase.small_file_count = profile_u64(line, "small=");
        phase.large_file_count = profile_u64(line, "large=");
        phase.logical_size_bytes = profile_u64(line, "logical_size_bytes=");
        phase.max_depth = profile_u64(line, "max_depth=");
        phase.readdirplus_page_count = profile_u64(line, "readdirplus_pages=");
        phase.readdirplus_page_entries = profile_u64(line, "readdirplus_entries=");
        phase.readdirplus_page_requested_bytes = profile_u64(line, "readdirplus_requested_bytes=");
        phase.readdirplus_page_latency.p50_us =
            profile_u64(line, "readdirplus_page_latency_p50_us=");
        phase.readdirplus_decode_latency.p50_us =
            profile_u64(line, "readdirplus_decode_latency_p50_us=");
        phase.sampled_small_read_files = profile_u64(line, "sampled_small_read_files=");
        phase.sampled_small_read_bytes = profile_u64(line, "sampled_small_read_bytes=");
        phase.sampled_small_read_failures = profile_u64(line, "sampled_small_read_failures=");
        phase.small_read_latency.p50_us = profile_u64(line, "sampled_small_read_latency_p50_us=");
        phase.sampled_large_read_files = profile_u64(line, "sampled_large_read_files=");
        phase.sampled_large_read_bytes = profile_u64(line, "sampled_large_read_bytes=");
        phase.sampled_large_read_failures = profile_u64(line, "sampled_large_read_failures=");
        phase.large_read_latency.p50_us = profile_u64(line, "sampled_large_read_latency_p50_us=");
        phase.small_read_latency.p90_us = phase.small_read_latency.p50_us;
        phase.small_read_latency.p99_us = phase.small_read_latency.p50_us;
        phase.small_read_latency.max_us = phase.small_read_latency.p50_us;
        phase.large_read_latency.p90_us = phase.large_read_latency.p50_us;
        phase.large_read_latency.p99_us = phase.large_read_latency.p50_us;
        phase.large_read_latency.max_us = phase.large_read_latency.p50_us;
        parse_profile_count_buckets(line,
                                    "size_buckets=",
                                    phase.size_file_counts,
                                    phase.size_logical_bytes);
        parse_profile_simple_buckets(line,
                                     "files_per_folder_buckets=",
                                     phase.files_per_folder_counts.data(),
                                     phase.files_per_folder_counts.size());
        parse_profile_simple_buckets(line,
                                     "subdirs_per_folder_buckets=",
                                     phase.subdirs_per_folder_counts.data(),
                                     phase.subdirs_per_folder_counts.size());
        parse_profile_simple_buckets(line,
                                     "folder_depth_buckets=",
                                     phase.folder_depth_counts.data(),
                                     phase.folder_depth_counts.size());
        parse_profile_simple_buckets(line,
                                     "entries_per_page_buckets=",
                                     phase.entries_per_page_counts.data(),
                                     phase.entries_per_page_counts.size());
        parse_profile_simple_buckets(line,
                                     "sampled_small_read_latency_buckets_us=",
                                     phase.sampled_small_read_latency_counts.data(),
                                     phase.sampled_small_read_latency_counts.size());
        parse_profile_simple_buckets(line,
                                     "sampled_large_read_latency_buckets_us=",
                                     phase.sampled_large_read_latency_counts.data(),
                                     phase.sampled_large_read_latency_counts.size());
        profile.phases.push_back(std::move(phase));
    }
    if (profile.phases.empty()) {
        throw std::runtime_error("profile has no phase records: " + path.string());
    }
    return profile;
}

hypersync::SyntheticObservedFile synthetic_profiler_observation(std::uint64_t file_id,
                                                                std::uint64_t file_count,
                                                                std::uint64_t seed) {
    const std::uint64_t phase_cut_a = file_count * 35U / 100U;
    const std::uint64_t phase_cut_b = file_count * 70U / 100U;
    const std::uint64_t random = hypersync::synthetic_splitmix64(seed ^ file_id);
    const std::uint64_t pick = random % 1000U;

    hypersync::SyntheticObservedFile file;
    if (file_id < phase_cut_a) {
        file.size_bytes = pick < 960U
                              ? 1U + (random % (64U * 1024U))
                              : (1U * 1024U * 1024U) + (random % (16U * 1024U * 1024U));
    } else if (file_id < phase_cut_b) {
        file.size_bytes = pick < 650U
                              ? 1U + (random % (128U * 1024U))
                              : (1U * 1024U * 1024U) + (random % (128U * 1024U * 1024U));
    } else {
        file.size_bytes = pick < 200U
                              ? 1U + (random % (128U * 1024U))
                              : (128U * 1024U * 1024ULL) + (random % (1024U * 1024U * 1024ULL));
    }
    file.uid = static_cast<std::uint32_t>(1000U + (random % 128U));
    file.gid = static_cast<std::uint32_t>(1000U + ((random >> 8U) % 64U));
    file.mode = 0644;
    file.filename_length = static_cast<std::uint16_t>(8U + (random % 80U));
    file.depth = static_cast<std::uint16_t>(1U + ((random >> 12U) % 8U));
    file.files_in_folder = static_cast<std::uint32_t>(1U + ((file_id / 4096U) % 100000U));
    return file;
}

void print_synthetic_profile(std::ostream& out,
                             const hypersync::SyntheticWorkloadProfile& profile,
                             std::uint64_t files_observed,
                             double elapsed_seconds) {
    std::uint64_t total_bytes = 0;
    std::uint64_t total_small = 0;
    std::uint64_t total_large = 0;
    for (const auto& phase : profile.phases) {
        total_bytes += phase.logical_size_bytes;
        total_small += phase.small_file_count;
        total_large += phase.large_file_count;
    }

    out << std::fixed << std::setprecision(3)
        << "synthetic_profile_benchmark files_observed=" << files_observed
        << " phases=" << profile.phases.size()
        << " elapsed_s=" << elapsed_seconds
        << " files_per_second=" << (elapsed_seconds > 0.0 ? static_cast<double>(files_observed) / elapsed_seconds : 0.0)
        << " logical_size_bytes=" << total_bytes
        << " small_files=" << total_small
        << " large_files=" << total_large << '\n';

    const auto bounds = hypersync::synthetic_size_bucket_bounds();
    const auto latency_bounds = hypersync::synthetic_latency_bucket_bounds_us();
    for (std::size_t phase_index = 0; phase_index < profile.phases.size(); ++phase_index) {
        const auto& phase = profile.phases[phase_index];
        const double small_ratio = phase.file_count == 0U
                                       ? 0.0
                                       : static_cast<double>(phase.small_file_count) /
                                             static_cast<double>(phase.file_count);
        const double average_filename_length = phase.file_count == 0U
                                                   ? 0.0
                                                   : static_cast<double>(phase.filename_length_sum) /
                                                         static_cast<double>(phase.file_count);
        const double average_depth = phase.file_count == 0U
                                         ? 0.0
                                         : static_cast<double>(phase.depth_sum) /
                                               static_cast<double>(phase.file_count);
        out << "phase index=" << phase_index
            << " name=" << phase.name
            << " files=" << phase.file_count
            << " folders=" << phase.folder_count
            << " directories=" << phase.directory_count
            << " empty_folders=" << phase.empty_folder_count
            << " near_empty_folders=" << phase.near_empty_folder_count
            << " small=" << phase.small_file_count
            << " large=" << phase.large_file_count
            << " small_ratio=" << small_ratio
            << " logical_size_bytes=" << phase.logical_size_bytes
            << " avg_filename_len=" << average_filename_length
            << " avg_depth=" << average_depth
            << " max_depth=" << phase.max_depth
            << " readdirplus_pages=" << phase.readdirplus_page_count
            << " readdirplus_entries=" << phase.readdirplus_page_entries
            << " readdirplus_requested_bytes=" << phase.readdirplus_page_requested_bytes
            << " readdirplus_page_latency_p50_us=" << phase.readdirplus_page_latency.p50_us
            << " p90_us=" << phase.readdirplus_page_latency.p90_us
            << " p99_us=" << phase.readdirplus_page_latency.p99_us
            << " max_us=" << phase.readdirplus_page_latency.max_us
            << " readdirplus_decode_latency_p50_us=" << phase.readdirplus_decode_latency.p50_us
            << " p90_us=" << phase.readdirplus_decode_latency.p90_us
            << " p99_us=" << phase.readdirplus_decode_latency.p99_us
            << " max_us=" << phase.readdirplus_decode_latency.max_us
            << " sampled_small_read_files=" << phase.sampled_small_read_files
            << " sampled_small_read_bytes=" << phase.sampled_small_read_bytes
            << " sampled_small_read_failures=" << phase.sampled_small_read_failures
            << " sampled_small_read_latency_p50_us=" << phase.small_read_latency.p50_us
            << " p90_us=" << phase.small_read_latency.p90_us
            << " p99_us=" << phase.small_read_latency.p99_us
            << " sampled_large_read_files=" << phase.sampled_large_read_files
            << " sampled_large_read_bytes=" << phase.sampled_large_read_bytes
            << " sampled_large_read_failures=" << phase.sampled_large_read_failures
            << " sampled_large_read_latency_p50_us=" << phase.large_read_latency.p50_us
            << " p90_us=" << phase.large_read_latency.p90_us
            << " p99_us=" << phase.large_read_latency.p99_us
            << " size_buckets=";
        for (std::size_t bucket = 0; bucket < bounds.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << "<=";
            if (bounds[bucket] == hypersync::kSyntheticUnboundedSize) {
                out << "inf";
            } else {
                out << bounds[bucket];
            }
            out << ':' << phase.size_file_counts[bucket]
                << '/' << phase.size_logical_bytes[bucket];
        }
        out << " files_per_folder_buckets=";
        for (std::size_t bucket = 0; bucket < phase.files_per_folder_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << bucket << ':' << phase.files_per_folder_counts[bucket];
        }
        out << " subdirs_per_folder_buckets=";
        for (std::size_t bucket = 0; bucket < phase.subdirs_per_folder_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << bucket << ':' << phase.subdirs_per_folder_counts[bucket];
        }
        out << " folder_depth_buckets=";
        for (std::size_t bucket = 0; bucket < phase.folder_depth_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << bucket << ':' << phase.folder_depth_counts[bucket];
        }
        out << " entries_per_page_buckets=";
        for (std::size_t bucket = 0; bucket < phase.entries_per_page_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << bucket << ':' << phase.entries_per_page_counts[bucket];
        }
        out << " readdirplus_page_latency_buckets_us=";
        for (std::size_t bucket = 0; bucket < phase.readdirplus_page_latency_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << "<=";
            if (latency_bounds[bucket] == hypersync::kSyntheticUnboundedSize) {
                out << "inf";
            } else {
                out << latency_bounds[bucket];
            }
            out << ':' << phase.readdirplus_page_latency_counts[bucket];
        }
        out << " readdirplus_decode_latency_buckets_us=";
        for (std::size_t bucket = 0; bucket < phase.readdirplus_decode_latency_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << "<=";
            if (latency_bounds[bucket] == hypersync::kSyntheticUnboundedSize) {
                out << "inf";
            } else {
                out << latency_bounds[bucket];
            }
            out << ':' << phase.readdirplus_decode_latency_counts[bucket];
        }
        out << " sampled_small_read_latency_buckets_us=";
        for (std::size_t bucket = 0; bucket < phase.sampled_small_read_latency_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << "<=";
            if (latency_bounds[bucket] == hypersync::kSyntheticUnboundedSize) {
                out << "inf";
            } else {
                out << latency_bounds[bucket];
            }
            out << ':' << phase.sampled_small_read_latency_counts[bucket];
        }
        out << " sampled_large_read_latency_buckets_us=";
        for (std::size_t bucket = 0; bucket < phase.sampled_large_read_latency_counts.size(); ++bucket) {
            if (bucket != 0U) {
                out << ',';
            }
            out << "<=";
            if (latency_bounds[bucket] == hypersync::kSyntheticUnboundedSize) {
                out << "inf";
            } else {
                out << latency_bounds[bucket];
            }
            out << ':' << phase.sampled_large_read_latency_counts[bucket];
        }
        out << '\n';
    }
}

struct DataReadProfileConfig {
    bool enabled = false;
    std::uint64_t sample_rate = 1'000'000'000;
    std::uint64_t max_samples_per_phase = 1;
    std::uint64_t max_sample_bytes_per_phase = 64ULL * 1024ULL * 1024ULL;
    std::uint64_t large_sample_bytes = hypersync::kLargeChunkBytes;
    std::size_t outstanding_requests = 1;
    std::size_t pool_slots = 64;
};

struct NfsProfileWorkQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<hypersync::FileSpec> folders;
    std::size_t active = 0;
    bool done = false;
    std::exception_ptr error;
};

struct FixedPhaseAccumulator {
    std::uint64_t file_count = 0;
    std::uint64_t folder_count = 0;
    std::uint64_t logical_size_bytes = 0;
    std::uint64_t small_file_count = 0;
    std::uint64_t large_file_count = 0;
    std::array<std::uint64_t, hypersync::kSyntheticSizeBucketCount> size_file_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticSizeBucketCount> size_logical_bytes {};
    std::array<std::uint64_t, hypersync::kSyntheticFolderFanoutBucketCount> folder_fanout_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticFolderFanoutBucketCount> files_per_folder_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticFolderFanoutBucketCount> subdirs_per_folder_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticFolderDepthBucketCount> folder_depth_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticEntriesPerPageBucketCount> entries_per_page_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticLatencyBucketCount> readdirplus_page_latency_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticLatencyBucketCount> readdirplus_decode_latency_counts {};
    std::uint64_t filename_length_sum = 0;
    std::uint64_t depth_sum = 0;
    std::uint64_t empty_folder_count = 0;
    std::uint64_t near_empty_folder_count = 0;
    std::uint64_t directory_count = 0;
    std::uint64_t max_depth = 0;
    std::uint64_t readdirplus_page_count = 0;
    std::uint64_t readdirplus_page_entries = 0;
    std::uint64_t readdirplus_page_requested_bytes = 0;
    std::uint64_t readdirplus_page_latency_sum_us = 0;
    std::uint64_t readdirplus_page_max_latency_us = 0;
    std::uint64_t readdirplus_decode_latency_sum_us = 0;
    std::uint64_t readdirplus_decode_max_latency_us = 0;
    std::uint64_t sampled_small_read_files = 0;
    std::uint64_t sampled_large_read_files = 0;
    std::uint64_t sampled_small_read_bytes = 0;
    std::uint64_t sampled_large_read_bytes = 0;
    std::uint64_t sampled_small_read_failures = 0;
    std::uint64_t sampled_large_read_failures = 0;
    std::uint64_t sampled_small_read_max_latency_us = 0;
    std::uint64_t sampled_large_read_max_latency_us = 0;
    std::array<std::uint64_t, hypersync::kSyntheticLatencyBucketCount> sampled_small_read_latency_counts {};
    std::array<std::uint64_t, hypersync::kSyntheticLatencyBucketCount> sampled_large_read_latency_counts {};
};

void merge_fixed_phase_accumulator(FixedPhaseAccumulator& target,
                                   const FixedPhaseAccumulator& source) noexcept {
    target.file_count += source.file_count;
    target.folder_count += source.folder_count;
    target.logical_size_bytes += source.logical_size_bytes;
    target.small_file_count += source.small_file_count;
    target.large_file_count += source.large_file_count;
    target.filename_length_sum += source.filename_length_sum;
    target.depth_sum += source.depth_sum;
    target.empty_folder_count += source.empty_folder_count;
    target.near_empty_folder_count += source.near_empty_folder_count;
    target.directory_count += source.directory_count;
    target.max_depth = std::max(target.max_depth, source.max_depth);
    target.readdirplus_page_count += source.readdirplus_page_count;
    target.readdirplus_page_entries += source.readdirplus_page_entries;
    target.readdirplus_page_requested_bytes += source.readdirplus_page_requested_bytes;
    target.readdirplus_page_latency_sum_us += source.readdirplus_page_latency_sum_us;
    target.readdirplus_page_max_latency_us =
        std::max(target.readdirplus_page_max_latency_us, source.readdirplus_page_max_latency_us);
    target.readdirplus_decode_latency_sum_us += source.readdirplus_decode_latency_sum_us;
    target.readdirplus_decode_max_latency_us =
        std::max(target.readdirplus_decode_max_latency_us, source.readdirplus_decode_max_latency_us);
    target.sampled_small_read_files += source.sampled_small_read_files;
    target.sampled_large_read_files += source.sampled_large_read_files;
    target.sampled_small_read_bytes += source.sampled_small_read_bytes;
    target.sampled_large_read_bytes += source.sampled_large_read_bytes;
    target.sampled_small_read_failures += source.sampled_small_read_failures;
    target.sampled_large_read_failures += source.sampled_large_read_failures;
    target.sampled_small_read_max_latency_us =
        std::max(target.sampled_small_read_max_latency_us, source.sampled_small_read_max_latency_us);
    target.sampled_large_read_max_latency_us =
        std::max(target.sampled_large_read_max_latency_us, source.sampled_large_read_max_latency_us);
    for (std::size_t index = 0; index < target.size_file_counts.size(); ++index) {
        target.size_file_counts[index] += source.size_file_counts[index];
        target.size_logical_bytes[index] += source.size_logical_bytes[index];
    }
    for (std::size_t index = 0; index < target.folder_fanout_counts.size(); ++index) {
        target.folder_fanout_counts[index] += source.folder_fanout_counts[index];
        target.files_per_folder_counts[index] += source.files_per_folder_counts[index];
        target.subdirs_per_folder_counts[index] += source.subdirs_per_folder_counts[index];
    }
    for (std::size_t index = 0; index < target.folder_depth_counts.size(); ++index) {
        target.folder_depth_counts[index] += source.folder_depth_counts[index];
    }
    for (std::size_t index = 0; index < target.entries_per_page_counts.size(); ++index) {
        target.entries_per_page_counts[index] += source.entries_per_page_counts[index];
    }
    for (std::size_t index = 0; index < target.readdirplus_page_latency_counts.size(); ++index) {
        target.readdirplus_page_latency_counts[index] += source.readdirplus_page_latency_counts[index];
        target.readdirplus_decode_latency_counts[index] += source.readdirplus_decode_latency_counts[index];
        target.sampled_small_read_latency_counts[index] += source.sampled_small_read_latency_counts[index];
        target.sampled_large_read_latency_counts[index] += source.sampled_large_read_latency_counts[index];
    }
}

std::uint16_t profile_path_depth(std::string_view path) noexcept {
    if (path.empty()) {
        return 0;
    }
    std::uint16_t depth = 1;
    for (const char ch : path) {
        if (ch == '/') {
            ++depth;
        }
    }
    return depth;
}

std::uint64_t ns_to_us_ceil(std::uint64_t value) noexcept {
    if (value == 0U) {
        return 0U;
    }
    return (value + 999U) / 1000U;
}

hypersync::SyntheticLatencyPercentiles approximate_latency_percentiles(
    const std::array<std::uint64_t, hypersync::kSyntheticLatencyBucketCount>& buckets,
    std::uint64_t max_us) noexcept {
    hypersync::SyntheticLatencyPercentiles result;
    result.max_us = max_us;
    std::uint64_t total = 0;
    for (const std::uint64_t count : buckets) {
        total += count;
    }
    if (total == 0U) {
        return result;
    }
    const auto bounds = hypersync::synthetic_latency_bucket_bounds_us();
    const auto pick = [&](std::uint64_t numerator) {
        const std::uint64_t rank = std::max<std::uint64_t>(1U, (total * numerator + 99U) / 100U);
        std::uint64_t cursor = 0;
        for (std::size_t index = 0; index < buckets.size(); ++index) {
            cursor += buckets[index];
            if (cursor >= rank) {
                return bounds[index] == hypersync::kSyntheticUnboundedSize ? max_us : bounds[index];
            }
        }
        return max_us;
    };
    result.p50_us = pick(50U);
    result.p90_us = pick(90U);
    result.p99_us = pick(99U);
    return result;
}

std::uint16_t profile_filename_length(std::string_view path) noexcept {
    const std::size_t slash = path.find_last_of('/');
    const std::size_t length = slash == std::string_view::npos ? path.size() : path.size() - slash - 1U;
    return static_cast<std::uint16_t>(std::min<std::size_t>(length, std::numeric_limits<std::uint16_t>::max()));
}

void add_file_to_fixed_phase(FixedPhaseAccumulator& accumulator,
                             const hypersync::FileSpec& file,
                             std::uint64_t small_threshold,
                             std::uint64_t files_in_folder) {
    const std::uint64_t size = file.declared_size != 0U ? file.declared_size : file.content.size();
    ++accumulator.file_count;
    accumulator.logical_size_bytes += size;
    if (size <= small_threshold) {
        ++accumulator.small_file_count;
    } else {
        ++accumulator.large_file_count;
    }
    const std::size_t size_bucket = hypersync::synthetic_size_bucket_index(size);
    ++accumulator.size_file_counts[size_bucket];
    accumulator.size_logical_bytes[size_bucket] += size;
    ++accumulator.folder_fanout_counts[hypersync::synthetic_folder_fanout_bucket_index(files_in_folder)];
    accumulator.filename_length_sum += profile_filename_length(file.rel_path);
    accumulator.depth_sum += profile_path_depth(file.rel_path);
}

void add_folder_to_fixed_phase(FixedPhaseAccumulator& accumulator,
                               const hypersync::FlatFolderScanBatch& batch) {
    const std::uint64_t files_in_folder = batch.files.size();
    const std::uint64_t subdirs_in_folder = batch.directories.size();
    const std::uint64_t entries_in_folder = files_in_folder + subdirs_in_folder;
    const std::uint64_t depth = profile_path_depth(batch.folder.rel_path);

    ++accumulator.folder_count;
    accumulator.directory_count += subdirs_in_folder;
    accumulator.max_depth = std::max(accumulator.max_depth, depth);
    if (entries_in_folder == 0U) {
        ++accumulator.empty_folder_count;
    }
    if (entries_in_folder <= 1U) {
        ++accumulator.near_empty_folder_count;
    }
    ++accumulator.files_per_folder_counts[hypersync::synthetic_folder_fanout_bucket_index(files_in_folder)];
    ++accumulator.subdirs_per_folder_counts[hypersync::synthetic_folder_fanout_bucket_index(subdirs_in_folder)];
    ++accumulator.folder_depth_counts[hypersync::synthetic_folder_depth_bucket_index(depth)];

    accumulator.readdirplus_page_count += batch.readdirplus_page_count;
    accumulator.readdirplus_page_entries += batch.readdirplus_page_entries;
    accumulator.readdirplus_page_requested_bytes += batch.readdirplus_page_requested_bytes;
    accumulator.readdirplus_page_latency_sum_us += ns_to_us_ceil(batch.readdirplus_page_latency_ns);
    accumulator.readdirplus_decode_latency_sum_us += ns_to_us_ceil(batch.readdirplus_decode_latency_ns);
    accumulator.readdirplus_page_max_latency_us =
        std::max(accumulator.readdirplus_page_max_latency_us, ns_to_us_ceil(batch.readdirplus_page_max_latency_ns));
    accumulator.readdirplus_decode_max_latency_us =
        std::max(accumulator.readdirplus_decode_max_latency_us, ns_to_us_ceil(batch.readdirplus_decode_max_latency_ns));

    if (batch.readdirplus_page_count == 0U) {
        return;
    }
    const std::uint64_t average_entries_per_page =
        (batch.readdirplus_page_entries + batch.readdirplus_page_count - 1U) / batch.readdirplus_page_count;
    const std::uint64_t average_page_latency_us =
        ns_to_us_ceil(batch.readdirplus_page_latency_ns / batch.readdirplus_page_count);
    const std::uint64_t average_decode_latency_us =
        ns_to_us_ceil(batch.readdirplus_decode_latency_ns / batch.readdirplus_page_count);
    accumulator.entries_per_page_counts[hypersync::synthetic_entries_per_page_bucket_index(average_entries_per_page)] +=
        batch.readdirplus_page_count;
    accumulator.readdirplus_page_latency_counts[hypersync::synthetic_latency_bucket_index_us(average_page_latency_us)] +=
        batch.readdirplus_page_count;
    accumulator.readdirplus_decode_latency_counts[hypersync::synthetic_latency_bucket_index_us(average_decode_latency_us)] +=
        batch.readdirplus_page_count;
}

void add_data_read_sample_to_fixed_phase(FixedPhaseAccumulator& accumulator,
                                         const hypersync::FileSpec& file,
                                         const hypersync::NfsBackend& backend,
                                         hypersync::RawBufferPool& pool,
                                         const DataReadProfileConfig& config,
                                         std::uint64_t small_threshold) {
    const std::uint64_t file_size = file.declared_size != 0U ? file.declared_size : file.content.size();
    if (file_size == 0U) {
        return;
    }

    hypersync::FileSpec sample = file;
    const bool small = file_size <= small_threshold;
    const std::uint64_t requested_bytes = small ? file_size : std::min(file_size, config.large_sample_bytes);
    sample.declared_size = requested_bytes;
    const auto started = std::chrono::steady_clock::now();
    try {
        const std::uint64_t bytes_read = backend.read_file_raw_chunks_by_handle(
            sample,
            config.outstanding_requests,
            pool,
            [&](hypersync::RawFileChunk&& chunk) {
                pool.release(chunk.handle);
            },
            {},
            false);
        const std::uint64_t latency_us = ns_to_us_ceil(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started).count()));
        if (small) {
            ++accumulator.sampled_small_read_files;
            accumulator.sampled_small_read_bytes += bytes_read;
            accumulator.sampled_small_read_max_latency_us =
                std::max(accumulator.sampled_small_read_max_latency_us, latency_us);
            ++accumulator.sampled_small_read_latency_counts[hypersync::synthetic_latency_bucket_index_us(latency_us)];
            if (bytes_read != requested_bytes) {
                ++accumulator.sampled_small_read_failures;
            }
        } else {
            ++accumulator.sampled_large_read_files;
            accumulator.sampled_large_read_bytes += bytes_read;
            accumulator.sampled_large_read_max_latency_us =
                std::max(accumulator.sampled_large_read_max_latency_us, latency_us);
            ++accumulator.sampled_large_read_latency_counts[hypersync::synthetic_latency_bucket_index_us(latency_us)];
            if (bytes_read != requested_bytes) {
                ++accumulator.sampled_large_read_failures;
            }
        }
    } catch (...) {
        if (small) {
            ++accumulator.sampled_small_read_failures;
        } else {
            ++accumulator.sampled_large_read_failures;
        }
    }
}

hypersync::SyntheticWorkloadProfile make_fixed_phase_profile(
    const std::vector<FixedPhaseAccumulator>& accumulators,
    std::uint64_t small_threshold) {
    hypersync::SyntheticWorkloadProfile profile;
    profile.small_file_threshold_bytes = small_threshold;
    profile.phases.reserve(accumulators.size());
    for (std::size_t index = 0; index < accumulators.size(); ++index) {
        const auto& accumulator = accumulators[index];
        hypersync::SyntheticPhaseProfile phase;
        phase.name = "phase_" + std::to_string(index);
        phase.file_count = accumulator.file_count;
        phase.folder_count = accumulator.folder_count;
        phase.logical_size_bytes = accumulator.logical_size_bytes;
        phase.small_file_count = accumulator.small_file_count;
        phase.large_file_count = accumulator.large_file_count;
        phase.size_file_counts = accumulator.size_file_counts;
        phase.size_logical_bytes = accumulator.size_logical_bytes;
        phase.folder_fanout_counts = accumulator.folder_fanout_counts;
        phase.files_per_folder_counts = accumulator.files_per_folder_counts;
        phase.subdirs_per_folder_counts = accumulator.subdirs_per_folder_counts;
        phase.folder_depth_counts = accumulator.folder_depth_counts;
        phase.entries_per_page_counts = accumulator.entries_per_page_counts;
        phase.readdirplus_page_latency_counts = accumulator.readdirplus_page_latency_counts;
        phase.readdirplus_decode_latency_counts = accumulator.readdirplus_decode_latency_counts;
        phase.filename_length_sum = accumulator.filename_length_sum;
        phase.depth_sum = accumulator.depth_sum;
        phase.empty_folder_count = accumulator.empty_folder_count;
        phase.near_empty_folder_count = accumulator.near_empty_folder_count;
        phase.directory_count = accumulator.directory_count;
        phase.max_depth = accumulator.max_depth;
        phase.readdirplus_page_count = accumulator.readdirplus_page_count;
        phase.readdirplus_page_entries = accumulator.readdirplus_page_entries;
        phase.readdirplus_page_requested_bytes = accumulator.readdirplus_page_requested_bytes;
        phase.readdirplus_page_latency_sum_us = accumulator.readdirplus_page_latency_sum_us;
        phase.readdirplus_page_latency =
            approximate_latency_percentiles(accumulator.readdirplus_page_latency_counts,
                                            accumulator.readdirplus_page_max_latency_us);
        phase.readdirplus_decode_latency_sum_us = accumulator.readdirplus_decode_latency_sum_us;
        phase.readdirplus_decode_latency =
            approximate_latency_percentiles(accumulator.readdirplus_decode_latency_counts,
                                            accumulator.readdirplus_decode_max_latency_us);
        phase.sampled_small_read_files = accumulator.sampled_small_read_files;
        phase.sampled_large_read_files = accumulator.sampled_large_read_files;
        phase.sampled_small_read_bytes = accumulator.sampled_small_read_bytes;
        phase.sampled_large_read_bytes = accumulator.sampled_large_read_bytes;
        phase.sampled_small_read_failures = accumulator.sampled_small_read_failures;
        phase.sampled_large_read_failures = accumulator.sampled_large_read_failures;
        phase.sampled_small_read_latency_counts = accumulator.sampled_small_read_latency_counts;
        phase.sampled_large_read_latency_counts = accumulator.sampled_large_read_latency_counts;
        phase.small_read_latency =
            approximate_latency_percentiles(accumulator.sampled_small_read_latency_counts,
                                            accumulator.sampled_small_read_max_latency_us);
        phase.large_read_latency =
            approximate_latency_percentiles(accumulator.sampled_large_read_latency_counts,
                                            accumulator.sampled_large_read_max_latency_us);
        profile.phases.push_back(std::move(phase));
    }
    return profile;
}

std::optional<hypersync::FileSpec> take_nfs_profile_folder_work(NfsProfileWorkQueue& queue,
                                                                bool wait_for_work) {
    std::unique_lock<std::mutex> lock(queue.mutex);
    const auto ready = [&queue]() {
        return queue.done || queue.error || !queue.folders.empty();
    };
    if (wait_for_work) {
        queue.cv.wait(lock, ready);
    } else if (!ready()) {
        return std::nullopt;
    }
    if (queue.done || queue.error || queue.folders.empty()) {
        return std::nullopt;
    }
    hypersync::FileSpec folder = std::move(queue.folders.front());
    queue.folders.pop_front();
    ++queue.active;
    return folder;
}

void finish_nfs_profile_folder_work(NfsProfileWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.active != 0U) {
            --queue.active;
        }
        if (queue.folders.empty() && queue.active == 0U) {
            queue.done = true;
        }
    }
    queue.cv.notify_all();
}

void request_nfs_profile_stop(NfsProfileWorkQueue& queue) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        queue.folders.clear();
    }
    queue.cv.notify_all();
}

void fail_nfs_profile_work(NfsProfileWorkQueue& queue, std::exception_ptr error) {
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.done = true;
        if (!queue.error) {
            queue.error = error;
        }
    }
    queue.cv.notify_all();
}

void enqueue_nfs_profile_folders(NfsProfileWorkQueue& queue,
                                 std::vector<hypersync::FileSpec>&& folders) {
    if (folders.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.done || queue.error) {
            return;
        }
        for (auto& folder : folders) {
            queue.folders.push_back(std::move(folder));
        }
    }
    queue.cv.notify_all();
}

struct NfsProfileCaptureResult {
    hypersync::SyntheticWorkloadProfile profile;
    std::uint64_t files_observed = 0;
    std::uint64_t folders_observed = 0;
    std::uint64_t failed_folders = 0;
    double elapsed_seconds = 0.0;
};

NfsProfileCaptureResult capture_nfs_profile(const std::filesystem::path& source_root,
                                            bool recursive,
                                            std::size_t meta_reader_threads,
                                            std::size_t metadata_async_depth,
                                            std::size_t readdirplus_page_bytes,
                                            std::uint64_t max_records,
                                            std::size_t phase_count,
                                            std::uint64_t small_threshold,
                                            std::uint32_t stats_interval_seconds,
                                            const DataReadProfileConfig& data_read_config) {
    NfsProfileWorkQueue queue;
    queue.folders.push_back(hypersync::FileSpec {});
    std::vector<FixedPhaseAccumulator> accumulators(std::max<std::size_t>(1U, phase_count));
    std::vector<std::mutex> accumulator_mutexes(accumulators.size());
    std::vector<std::atomic<std::uint64_t>> data_sample_counts(accumulators.size());
    std::vector<std::atomic<std::uint64_t>> data_sample_bytes(accumulators.size());
    std::atomic<std::uint64_t> files_reserved {0};
    std::atomic<std::uint64_t> files_recorded {0};
    std::atomic<std::uint64_t> folders_observed {0};
    std::atomic<std::uint64_t> failed_folders {0};
    const std::uint64_t records_per_phase =
        std::max<std::uint64_t>(1U, (max_records + accumulators.size() - 1U) / accumulators.size());
    const auto started = std::chrono::steady_clock::now();
    std::atomic<bool> progress_done {false};
    std::mutex progress_mutex;
    std::condition_variable progress_cv;

    const auto should_stop = [&]() {
        return files_reserved.load(std::memory_order_acquire) >= max_records;
    };

    std::thread progress_thread;
    if (stats_interval_seconds != 0U) {
        progress_thread = std::thread([&]() {
            std::uint64_t previous_files = 0;
            auto previous_time = started;
            while (!progress_done.load(std::memory_order_acquire)) {
                {
                    std::unique_lock<std::mutex> lock(progress_mutex);
                    progress_cv.wait_for(lock, std::chrono::seconds(stats_interval_seconds), [&]() {
                        return progress_done.load(std::memory_order_acquire);
                    });
                }
                if (progress_done.load(std::memory_order_acquire)) {
                    break;
                }
                const auto now = std::chrono::steady_clock::now();
                const std::uint64_t files = files_recorded.load(std::memory_order_relaxed);
                const std::uint64_t folders = folders_observed.load(std::memory_order_relaxed);
                const std::uint64_t failed = failed_folders.load(std::memory_order_relaxed);
                const double elapsed = std::chrono::duration<double>(now - started).count();
                const double interval_elapsed = std::chrono::duration<double>(now - previous_time).count();
                const double cumulative_files_per_second =
                    elapsed > 0.0 ? static_cast<double>(files) / elapsed : 0.0;
                const double interval_files_per_second =
                    interval_elapsed > 0.0
                        ? static_cast<double>(files - previous_files) / interval_elapsed
                        : 0.0;
                const std::uint64_t phase_index =
                    std::min<std::uint64_t>(accumulators.size() - 1U, files / records_per_phase);
                std::cerr << "nfs_profile_progress elapsed_s=" << elapsed
                          << " files=" << files
                          << " folders=" << folders
                          << " failed_folders=" << failed
                          << " cumulative_files_per_second=" << cumulative_files_per_second
                          << " interval_files_per_second=" << interval_files_per_second
                          << " phase_index=" << phase_index
                          << " phase_file_offset=" << (files % records_per_phase)
                          << '\n';
                previous_files = files;
                previous_time = now;
            }
        });
    }

    std::vector<std::thread> workers;
    workers.reserve(std::max<std::size_t>(1U, meta_reader_threads));
    for (std::size_t worker_index = 0; worker_index < std::max<std::size_t>(1U, meta_reader_threads); ++worker_index) {
        (void)worker_index;
        workers.emplace_back([&]() {
            try {
                auto backend = hypersync::make_nfs_backend(source_root.string(),
                                                           hypersync::kNfsEndpointAny,
                                                           readdirplus_page_bytes);
                hypersync::RawBufferPool data_sample_pool(
                    hypersync::kDataBufferPoolId,
                    std::max<std::size_t>(1U, data_read_config.pool_slots),
                    sizeof(hypersync::DataBuffer),
                    alignof(hypersync::DataBuffer));
                backend->scan_flat_folders(
                    std::max<std::size_t>(1U, metadata_async_depth),
                    [&queue](bool wait_for_work) {
                        return take_nfs_profile_folder_work(queue, wait_for_work);
                    },
                    [&should_stop]() {
                        return should_stop();
                    },
                    [&](hypersync::FlatFolderScanBatch batch) {
                        if (batch.failed) {
                            failed_folders.fetch_add(1U, std::memory_order_relaxed);
                            finish_nfs_profile_folder_work(queue);
                            return;
                        }

                        std::vector<hypersync::FileSpec> child_work;
                        if (recursive && !should_stop()) {
                            child_work.reserve(batch.directories.size());
                            for (auto& directory : batch.directories) {
                                child_work.push_back(std::move(directory));
                            }
                        }

                        const std::uint64_t batch_file_count = batch.files.size();
                        const std::uint64_t start_index =
                            files_reserved.fetch_add(batch_file_count, std::memory_order_acq_rel);
                        const std::uint64_t accepted_file_count =
                            start_index >= max_records
                                ? 0U
                                : std::min<std::uint64_t>(batch_file_count, max_records - start_index);

                        std::vector<FixedPhaseAccumulator> local(accumulators.size());
                        const std::uint64_t folder_file_count = batch.files.size();
                        for (std::uint64_t offset = 0; offset < accepted_file_count; ++offset) {
                            const std::uint64_t global_index = start_index + offset;
                            const std::size_t phase_index = std::min<std::size_t>(
                                accumulators.size() - 1U,
                                static_cast<std::size_t>(global_index / records_per_phase));
                            add_file_to_fixed_phase(local[phase_index],
                                                    batch.files[static_cast<std::size_t>(offset)],
                                                    small_threshold,
                                                    folder_file_count);
                            if (data_read_config.enabled && data_read_config.sample_rate != 0U &&
                                hypersync::synthetic_splitmix64(global_index) % data_read_config.sample_rate == 0U) {
                                const hypersync::FileSpec& file = batch.files[static_cast<std::size_t>(offset)];
                                const std::uint64_t file_size =
                                    file.declared_size != 0U ? file.declared_size : file.content.size();
                                const std::uint64_t requested_bytes =
                                    file_size <= small_threshold
                                        ? file_size
                                        : std::min(file_size, data_read_config.large_sample_bytes);
                                const std::uint64_t previous_samples =
                                    data_sample_counts[phase_index].fetch_add(1U, std::memory_order_relaxed);
                                const std::uint64_t previous_bytes =
                                    data_sample_bytes[phase_index].fetch_add(requested_bytes, std::memory_order_relaxed);
                                if (previous_samples < data_read_config.max_samples_per_phase &&
                                    previous_bytes + requested_bytes <= data_read_config.max_sample_bytes_per_phase) {
                                    add_data_read_sample_to_fixed_phase(local[phase_index],
                                                                        file,
                                                                        *backend,
                                                                        data_sample_pool,
                                                                        data_read_config,
                                                                        small_threshold);
                                }
                            }
                        }
                        const std::size_t folder_phase_index = std::min<std::size_t>(
                            accumulators.size() - 1U,
                            static_cast<std::size_t>(std::min(start_index, max_records - 1U) / records_per_phase));
                        add_folder_to_fixed_phase(local[folder_phase_index], batch);
                        folders_observed.fetch_add(1U, std::memory_order_relaxed);

                        if (accepted_file_count != 0U) {
                            files_recorded.fetch_add(accepted_file_count, std::memory_order_relaxed);
                        }
                        for (std::size_t phase_index = 0; phase_index < local.size(); ++phase_index) {
                            if (local[phase_index].file_count == 0U &&
                                local[phase_index].folder_count == 0U) {
                                continue;
                            }
                            std::lock_guard<std::mutex> lock(accumulator_mutexes[phase_index]);
                            merge_fixed_phase_accumulator(accumulators[phase_index], local[phase_index]);
                        }

                        if (should_stop()) {
                            request_nfs_profile_stop(queue);
                        } else {
                            enqueue_nfs_profile_folders(queue, std::move(child_work));
                        }
                        finish_nfs_profile_folder_work(queue);
                    });
                if (should_stop()) {
                    request_nfs_profile_stop(queue);
                }
            } catch (...) {
                fail_nfs_profile_work(queue, std::current_exception());
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    progress_done.store(true, std::memory_order_release);
    progress_cv.notify_all();
    if (progress_thread.joinable()) {
        progress_thread.join();
    }
    if (queue.error) {
        std::rethrow_exception(queue.error);
    }

    NfsProfileCaptureResult result;
    result.profile = make_fixed_phase_profile(accumulators, small_threshold);
    result.files_observed = files_recorded.load(std::memory_order_relaxed);
    result.folders_observed = folders_observed.load(std::memory_order_relaxed);
    result.failed_folders = failed_folders.load(std::memory_order_relaxed);
    result.elapsed_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    return result;
}

void print_usage() {
    std::cerr
        << "Usage:\n"
        << "  hypersync [--config <config.yaml>] receive --target <dir|nfs-url> [--bind-host <host>] [--priority-port <port>] [--data-port <port>] [--backpressure-window <bytes>] [--backpressure-pause-ms <ms>] [--skip-verify]\n"
        << "  hypersync status --socket <path>\n"
        << "  hypersync tuning [--iface <name>] [--cpu-mask <mask>] [--peer <ip>] [--apply]\n"
        << "  hypersync [--config <config.yaml>] send|sync|copy --source <dir|nfs-url> [--host <host>] [--priority-port <port>] [--data-port <port>] [--cache-path <dir>] [--cache-threshold <bytes>] [--skip-verify]\n"
        << "  hypersync [--config <config.yaml>] scan --source <dir|nfs-url> --output <scan.csv|txt|parquet> [--scan-side S|T] [--output-format text|csv|parquet] [--records all|files|folders] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--record-buffer-slots <n>] [--pipeline-autoscale|--no-pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] diff (--source <dir|nfs-url> --target <dir|nfs-url> | --source-scan <scan.csv> --target-scan <scan.csv>) [--compare size|time|content] [--summary-only] [--output <diff.csv>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--checker-threads <n>] [--checker-request-queue-depth <n>] [--checker-batch-queue-depth <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] diff-target --target <dir|nfs-url> [--listen-host <host>] --port <port> [--compare size|time|content] [--non-recursive] [--target-threads <n>] [--metadata-async-depth <n>] [--pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] diff-source --source <dir|nfs-url> --target-host <host> --port <port> --folder-report <report.csv> [--compare size|time|content] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] dry-run --source <dir|nfs-url> [--source-scan <scan.csv>] [--target-scan <scan.csv>] [--output <diff.csv>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-meta --source <dir|nfs-url> [--non-recursive] [--discard-after-checker|--metadata-stats-discarder] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--metadata-output <path>] [--metadata-output-format text|csv|parquet] [--metadata-records all|files|folders] [--metadata-output-partitions <n>] [--metadata-output-partition-mode single|processes|transport-discard|route-discard|sharded-discard] [--record-buffer-slots <n>] [--pipeline-autoscale|--no-pipeline-autoscale] [--autoscale-profile <name>] [--autoscale-settings <path>] [--autoscale-interval-ms <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-open --source <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--open-threads <n>] [--max-files-queued <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data --source <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--small-file-async-window <n>] [--split-small-large] [--dual-scan-small-large] [--background-recon-scan] [--bucket-priority] [--morph-large-readers-to-small] [--small-file-threshold-bytes <n>] [--recon-meta-reader-threads <n>] [--recon-metadata-async-depth <n>] [--recon-page-sleep-us <n>] [--small-meta-reader-threads <n>] [--large-meta-reader-threads <n>] [--small-data-reader-threads <n>] [--large-data-reader-threads <n>] [--large-data-outstanding-requests <n>] [--pipeline-autoscale] [--large-reader-autoscale] [--large-reader-initial-threads <n>] [--autoscale-interval-ms <n>] [--autoscale-profile <name>] [--autoscale-settings <path>] [--max-file-size-bytes <n>] [--pack-small-files] [--max-files-queued <n>] [--small-max-files-queued <n>] [--large-max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--data-copy-mode copy|no-copy] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data-write --source <dir|nfs-url|synthetic-profile-url> --target <dir|nfs-url> [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--data-reader-threads <n>] [--data-writer-threads <n>] [--data-writer-async-window <n>] [--data-writer-file-window <n>] [--data-writer-reactors <n>] [--reactors-per-ip <n>] [--data-writer-stable-small-writes] [--data-writer-direct-reactors] [--data-outstanding-requests <n>] [--small-file-async-window <n>] [--min-file-size-bytes <n>] [--max-file-size-bytes <n>] [--pack-small-files] [--skip-target-metadata] [--no-target-fsync] [--assume-target-directories] [--max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <per-shard>] [--data-copy-mode copy|no-copy] [--verify-hash] [--max-duration-seconds <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-data-hash --source <dir|nfs-url> [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--small-file-async-window <n>] [--pack-small-files] [--hash-threads <n>] [--hash-work-factor <n>] [--max-files-queued <n>] [--data-buffer-slots <n>] [--data-queue-depth <n>] [--max-duration-seconds <n>] [--stats-interval-seconds <n>] [--status-socket <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-synthetic-profile [--file-count <n>] [--block-file-count <n>] [--small-ratio-shift-threshold <n>] [--seed <n>] [--output <profile.txt>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-synthetic-replay --profile <profile.txt> [--max-files <n>] [--file-count-scale <n>] [--data-size-scale <n>] [--latency-emulation] [--with-payload] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-nfs-profile --source <nfs-url> [--max-records <n>] [--phase-count <n>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--readdirplus-page-bytes <n>] [--small-file-threshold-bytes <n>] [--profile-data-reads] [--data-sample-rate <n>] [--data-sample-max-files-per-phase <n>] [--data-sample-max-bytes-per-phase <n>] [--data-sample-large-read-bytes <n>] [--data-sample-outstanding-requests <n>] [--stats-interval-seconds <n>] [--output <profile.txt>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-hash [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--threads <n>] [--block-size <bytes>] [--duration-seconds <n>] [--min-gigabits-per-core <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-transport [--transports <n>] [--buffers-per-transport <n>] [--buffer-size <bytes>] [--pool-slots <n>] [--generator-threads <n>] [--sender-threads <n>] [--receiver-threads <n>] [--discarder-threads <n>] [--pattern zero|fast_text|xoshiro256] [--transport none|unix|tcp] [--shared-input] [--base-port <port>] [--socket-dir <path>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-fake-diff [--file-count <n>] [--folder-count <n>] [--average-file-size <bytes>] [--source-threads <n>] [--fake-remote-threads <n>] [--checker-threads <n>] [--remote-delay-us <n>] [--request-queue-depth <n>] [--batch-queue-depth <n>] [--stats-interval-seconds <n>]\n"
        << "  hypersync [--config <config.yaml>] benchmark-metadata-writer --output <records.parquet|dataset-dir|csv|txt> [--output-format text|csv|parquet] [--file-count <n>] [--folder-count <n>] [--batch-size <n>] [--average-file-size <bytes>] [--duckdb-memory-limit <value>] [--duckdb-threads <n>] [--duckdb-checkpoint-threshold <value>] [--parquet-compression zstd|snappy|uncompressed] [--partitions <n>] [--partition-mode threads|processes|transport-processes|generate-discard|generate-hash-discard|pack-discard|folder-pack-discard|transport-discard]\n"
        << "  hypersync [--config <config.yaml>] hash --source <dir|nfs-url> --output <records.csv|txt|parquet> [--output-format text|csv|parquet] [--records all|files|folders] [--hash md5|sha256|xxh64|xxh3_64|xxh3_128] [--hash-mode file|blocks] [--hash-block-size <bytes>] [--non-recursive] [--meta-reader-threads <n>] [--metadata-async-depth <n>] [--data-reader-threads <n>] [--data-outstanding-requests <n>] [--hash-threads <n>] [--max-files-queued <n>] [--max-hash-chunks-queued <n>] [--max-duration-seconds <n>]\n"
        << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            print_usage();
            return 1;
        }

        const std::vector<std::string> raw_args(argv + 1, argv + argc);
        if (raw_args.size() == 1 && raw_args.front() == "--version") {
            std::cout << "hypersync " << hypersync::kVersion << '\n';
            return 0;
        }

        std::string config_path;
        std::vector<std::string> args;
        args.reserve(raw_args.size());
        for (std::size_t i = 0; i < raw_args.size(); ++i) {
            if (raw_args[i] == "--config") {
                config_path = require_option(raw_args, i, "--config");
                continue;
            }
            args.push_back(raw_args[i]);
        }
        if (args.empty()) {
            print_usage();
            return 1;
        }

        const std::string command = args.front();

        if (command == "status") {
            std::filesystem::path socket_path;
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--socket" || args[i] == "--status-socket") {
                    socket_path = require_option(args, i, args[i]);
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (socket_path.empty()) {
                throw std::runtime_error("--socket is required");
            }
            std::cout << hypersync::request_status(socket_path);
            return 0;
        }

        if (command == "tuning" || command == "network-preflight") {
            TuningOptions tuning;
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--iface") {
                    tuning.iface = require_option(args, i, "--iface");
                } else if (args[i] == "--cpu-mask") {
                    tuning.cpu_mask = require_option(args, i, "--cpu-mask");
                } else if (args[i] == "--peer") {
                    tuning.peer = require_option(args, i, "--peer");
                } else if (args[i] == "--apply") {
                    tuning.apply = true;
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            return run_tuning(tuning);
        }

        hypersync::ConfigStore config_store(
            config_path.empty()
                ? std::vector<std::filesystem::path>{hypersync::default_config_path()}
                : std::vector<std::filesystem::path>{hypersync::default_config_path(), config_path});

        hypersync::EngineConfig engine_config = hypersync::load_engine_config(config_store);
        for (const auto& arg : args) {
            if (arg == "--skip-verify") {
                engine_config.skip_verify = true;
                break;
            }
        }
        hypersync::TransferEngine engine(engine_config, config_store);

        if (command == "benchmark-synthetic-profile") {
            std::uint64_t file_count = 100'000'000ULL;
            hypersync::SyntheticProfileCaptureConfig capture_config;
            std::filesystem::path output_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--file-count") {
                    file_count = parse_u64_option(require_option(args, i, "--file-count"),
                                                  "--file-count");
                } else if (args[i] == "--block-file-count") {
                    capture_config.block_file_count =
                        parse_u64_option(require_option(args, i, "--block-file-count"),
                                         "--block-file-count");
                } else if (args[i] == "--small-ratio-shift-threshold") {
                    capture_config.small_ratio_shift_threshold =
                        parse_positive_double_option(
                            require_option(args, i, "--small-ratio-shift-threshold"),
                            "--small-ratio-shift-threshold");
                } else if (args[i] == "--seed") {
                    capture_config.seed = parse_u64_option(require_option(args, i, "--seed"),
                                                           "--seed");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (file_count == 0U) {
                throw std::runtime_error("--file-count must be greater than zero");
            }

            hypersync::SyntheticProfileBuilder builder(capture_config);
            const auto start = std::chrono::steady_clock::now();
            for (std::uint64_t file_id = 0; file_id < file_count; ++file_id) {
                builder.observe_file(
                    synthetic_profiler_observation(file_id, file_count, capture_config.seed));
            }
            const hypersync::SyntheticWorkloadProfile profile = builder.finish();
            const auto finish = std::chrono::steady_clock::now();
            const double elapsed_seconds =
                std::chrono::duration<double>(finish - start).count();

            print_synthetic_profile(std::cout, profile, file_count, elapsed_seconds);
            if (!output_path.empty()) {
                std::ofstream output(output_path);
                if (!output) {
                    throw std::runtime_error("failed to open output: " + output_path.string());
                }
                print_synthetic_profile(output, profile, file_count, elapsed_seconds);
            }
            return 0;
        }

        if (command == "benchmark-synthetic-replay") {
            std::filesystem::path profile_path;
            std::uint64_t max_files = 0;
            double file_count_scale = 1.0;
            double data_size_scale = 1.0;
            bool latency_enabled = false;
            bool with_payload = false;
            std::uint64_t stats_interval_seconds = 0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--profile") {
                    profile_path = require_option(args, i, "--profile");
                } else if (args[i] == "--max-files") {
                    max_files = parse_u64_option(require_option(args, i, "--max-files"),
                                                 "--max-files");
                } else if (args[i] == "--file-count-scale") {
                    file_count_scale =
                        parse_positive_double_option(require_option(args, i, "--file-count-scale"),
                                                     "--file-count-scale");
                } else if (args[i] == "--data-size-scale") {
                    data_size_scale =
                        parse_positive_double_option(require_option(args, i, "--data-size-scale"),
                                                     "--data-size-scale");
                } else if (args[i] == "--latency-emulation") {
                    latency_enabled = true;
                } else if (args[i] == "--with-payload") {
                    with_payload = true;
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds =
                        parse_u64_option(require_option(args, i, "--stats-interval-seconds"),
                                         "--stats-interval-seconds");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (profile_path.empty()) {
                throw std::runtime_error("--profile is required");
            }

            hypersync::SyntheticReplayConfig replay_config;
            replay_config.profile = load_synthetic_profile_text(profile_path);
            replay_config.file_count_scale = file_count_scale;
            replay_config.data_size_scale = data_size_scale;
            replay_config.latency_enabled = latency_enabled;
            hypersync::SyntheticReplayCursor cursor(replay_config);
            hypersync::SyntheticPayloadPool payload_pool;

            std::uint64_t files = 0;
            std::uint64_t small_files = 0;
            std::uint64_t large_files = 0;
            std::uint64_t payload_views = 0;
            std::uint64_t payload_bytes = 0;
            std::uint64_t last_files = 0;
            const auto start = std::chrono::steady_clock::now();
            auto last_stats = start;
            hypersync::SyntheticFileView file;
            while (cursor.next_file(file)) {
                ++files;
                if (file.small) {
                    ++small_files;
                } else {
                    ++large_files;
                }
                if (with_payload) {
                    const hypersync::SyntheticPayloadView payload = payload_pool.payload_for(file);
                    payload_bytes += payload.size;
                    ++payload_views;
                }
                if (latency_enabled && file.latency_us != 0U) {
                    std::this_thread::sleep_for(std::chrono::microseconds(file.latency_us));
                }
                if (max_files != 0U && files >= max_files) {
                    break;
                }
                if (stats_interval_seconds != 0U) {
                    const auto now = std::chrono::steady_clock::now();
                    const double interval = std::chrono::duration<double>(now - last_stats).count();
                    if (interval >= static_cast<double>(stats_interval_seconds)) {
                        const double elapsed = std::chrono::duration<double>(now - start).count();
                        std::cerr << "synthetic_replay_progress elapsed_s=" << elapsed
                                  << " files=" << files
                                  << " interval_files_per_second="
                                  << (static_cast<double>(files - last_files) / interval)
                                  << " cumulative_files_per_second="
                                  << (elapsed > 0.0 ? static_cast<double>(files) / elapsed : 0.0)
                                  << " small_files=" << small_files
                                  << " large_files=" << large_files
                                  << " bytes=" << cursor.bytes_emitted() << '\n';
                        last_stats = now;
                        last_files = files;
                    }
                }
            }
            const auto finish = std::chrono::steady_clock::now();
            const double elapsed_seconds =
                std::chrono::duration<double>(finish - start).count();
            std::cout << "synthetic_replay_benchmark"
                      << " profile=" << profile_path.string()
                      << " phases=" << replay_config.profile.phases.size()
                      << " files=" << files
                      << " small_files=" << small_files
                      << " large_files=" << large_files
                      << " bytes=" << cursor.bytes_emitted()
                      << " payload_views=" << payload_views
                      << " payload_bytes=" << payload_bytes
                      << " elapsed_s=" << elapsed_seconds
                      << " files_per_second="
                      << (elapsed_seconds > 0.0 ? static_cast<double>(files) / elapsed_seconds : 0.0)
                      << " latency_emulation=" << (latency_enabled ? "true" : "false")
                      << " with_payload=" << (with_payload ? "true" : "false") << '\n';
            return 0;
        }

        if (command == "benchmark-nfs-profile") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::uint64_t max_records = 100'000'000ULL;
            std::size_t phase_count = 10;
            std::size_t meta_reader_threads = 96;
            std::size_t metadata_async_depth = 256;
            std::size_t readdirplus_page_bytes = 256U * 1024U;
            std::uint64_t small_threshold = hypersync::kSmallFileThreshold;
            std::uint32_t stats_interval_seconds = 10;
            DataReadProfileConfig data_read_config;
            std::filesystem::path output_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--max-records") {
                    max_records = parse_u64_option(require_option(args, i, "--max-records"),
                                                   "--max-records");
                } else if (args[i] == "--phase-count") {
                    phase_count = parse_size_t_option(require_option(args, i, "--phase-count"),
                                                      "--phase-count");
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--readdirplus-page-bytes") {
                    readdirplus_page_bytes =
                        parse_size_t_option(require_option(args, i, "--readdirplus-page-bytes"),
                                            "--readdirplus-page-bytes");
                } else if (args[i] == "--small-file-threshold-bytes") {
                    small_threshold =
                        parse_u64_option(require_option(args, i, "--small-file-threshold-bytes"),
                                         "--small-file-threshold-bytes");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--profile-data-reads") {
                    data_read_config.enabled = true;
                } else if (args[i] == "--data-sample-rate") {
                    data_read_config.sample_rate =
                        parse_u64_option(require_option(args, i, "--data-sample-rate"), "--data-sample-rate");
                } else if (args[i] == "--data-sample-max-files-per-phase") {
                    data_read_config.max_samples_per_phase =
                        parse_u64_option(require_option(args, i, "--data-sample-max-files-per-phase"),
                                         "--data-sample-max-files-per-phase");
                } else if (args[i] == "--data-sample-max-bytes-per-phase") {
                    data_read_config.max_sample_bytes_per_phase =
                        parse_u64_option(require_option(args, i, "--data-sample-max-bytes-per-phase"),
                                         "--data-sample-max-bytes-per-phase");
                } else if (args[i] == "--data-sample-large-read-bytes") {
                    data_read_config.large_sample_bytes =
                        parse_u64_option(require_option(args, i, "--data-sample-large-read-bytes"),
                                         "--data-sample-large-read-bytes");
                } else if (args[i] == "--data-sample-outstanding-requests") {
                    data_read_config.outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-sample-outstanding-requests"),
                                            "--data-sample-outstanding-requests");
                } else if (args[i] == "--data-sample-pool-slots") {
                    data_read_config.pool_slots =
                        parse_size_t_option(require_option(args, i, "--data-sample-pool-slots"),
                                            "--data-sample-pool-slots");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (max_records == 0U) {
                throw std::runtime_error("--max-records must be greater than zero");
            }

            const NfsProfileCaptureResult result = capture_nfs_profile(source_root,
                                                                       recursive,
                                                                       meta_reader_threads,
                                                                       metadata_async_depth,
                                                                       readdirplus_page_bytes,
                                                                       max_records,
                                                                       phase_count,
                                                                       small_threshold,
                                                                       stats_interval_seconds,
                                                                       data_read_config);
            print_synthetic_profile(std::cout,
                                    result.profile,
                                    result.files_observed,
                                    result.elapsed_seconds);
            std::cout << "nfs_profile source=" << source_root.string()
                      << " folders_observed=" << result.folders_observed
                      << " failed_folders=" << result.failed_folders
                      << " meta_reader_threads=" << meta_reader_threads
                      << " metadata_async_depth=" << metadata_async_depth
                      << " readdirplus_page_bytes=" << readdirplus_page_bytes
                      << " stats_interval_seconds=" << stats_interval_seconds
                      << " profile_data_reads=" << (data_read_config.enabled ? "true" : "false")
                      << " data_sample_rate=" << data_read_config.sample_rate
                      << " data_sample_max_files_per_phase=" << data_read_config.max_samples_per_phase
                      << " data_sample_max_bytes_per_phase=" << data_read_config.max_sample_bytes_per_phase
                      << " data_sample_large_read_bytes=" << data_read_config.large_sample_bytes
                      << " data_sample_outstanding_requests=" << data_read_config.outstanding_requests
                      << " recursive=" << (recursive ? "true" : "false") << '\n';
            if (!output_path.empty()) {
                std::ofstream output(output_path);
                if (!output) {
                    throw std::runtime_error("failed to open output: " + output_path.string());
                }
                print_synthetic_profile(output,
                                        result.profile,
                                        result.files_observed,
                                        result.elapsed_seconds);
                output << "nfs_profile source=" << source_root.string()
                       << " folders_observed=" << result.folders_observed
                       << " failed_folders=" << result.failed_folders
                       << " meta_reader_threads=" << meta_reader_threads
                       << " metadata_async_depth=" << metadata_async_depth
                       << " readdirplus_page_bytes=" << readdirplus_page_bytes
                       << " stats_interval_seconds=" << stats_interval_seconds
                       << " profile_data_reads=" << (data_read_config.enabled ? "true" : "false")
                       << " data_sample_rate=" << data_read_config.sample_rate
                       << " data_sample_max_files_per_phase=" << data_read_config.max_samples_per_phase
                       << " data_sample_max_bytes_per_phase=" << data_read_config.max_sample_bytes_per_phase
                       << " data_sample_large_read_bytes=" << data_read_config.large_sample_bytes
                       << " data_sample_outstanding_requests=" << data_read_config.outstanding_requests
                       << " recursive=" << (recursive ? "true" : "false") << '\n';
            }
            return 0;
        }

        if (command == "receive") {
            hypersync::ReceiverRuntimeConfig runtime = hypersync::load_receiver_runtime_config(config_store);
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--target") {
                    runtime.target_root = require_option(args, i, "--target");
                } else if (args[i] == "--bind-host") {
                    runtime.bind_host = require_option(args, i, "--bind-host");
                } else if (args[i] == "--priority-port") {
                    runtime.priority_port = parse_port(require_option(args, i, "--priority-port"), "--priority-port");
                } else if (args[i] == "--data-port") {
                    runtime.data_port = parse_port(require_option(args, i, "--data-port"), "--data-port");
                } else if (args[i] == "--backpressure-window") {
                    runtime.backpressure_window_bytes = static_cast<std::uint64_t>(
                        std::stoull(require_option(args, i, "--backpressure-window")));
                } else if (args[i] == "--backpressure-pause-ms") {
                    runtime.backpressure_pause_ms = static_cast<std::uint32_t>(
                        std::stoul(require_option(args, i, "--backpressure-pause-ms")));
                } else if (args[i] == "--skip-verify") {
                    // handled by pre-pass above
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (runtime.target_root.empty()) {
                throw std::runtime_error("--target is required");
            }
            engine.run_receiver(runtime);
            return 0;
        }

        if (command == "send" || command == "sync" || command == "copy") {
            hypersync::SenderRuntimeConfig runtime = hypersync::load_sender_runtime_config(config_store);
            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    runtime.source_root = require_option(args, i, "--source");
                } else if (args[i] == "--host") {
                    runtime.remote_host = require_option(args, i, "--host");
                } else if (args[i] == "--priority-port") {
                    runtime.priority_port = parse_port(require_option(args, i, "--priority-port"), "--priority-port");
                } else if (args[i] == "--data-port") {
                    runtime.data_port = parse_port(require_option(args, i, "--data-port"), "--data-port");
                } else if (args[i] == "--cache-path") {
                    runtime.cache_root = require_option(args, i, "--cache-path");
                } else if (args[i] == "--cache-threshold") {
                    runtime.cache_file_threshold_bytes = static_cast<std::uint64_t>(
                        std::stoull(require_option(args, i, "--cache-threshold")));
                } else if (args[i] == "--skip-verify") {
                    // handled by pre-pass above
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (runtime.source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.transfer_directory(runtime);
            std::cout << "files_total=" << report.files_total
                      << " transferred=" << report.files_transferred
                      << " skipped=" << report.files_skipped
                      << " failed=" << report.files_failed
                      << " bytes=" << report.bytes_transferred
                      << " chunks_sent=" << report.chunks_sent << '\n';
            return report.files_failed == 0 ? 0 : 2;
        }

        if (command == "scan") {
            std::filesystem::path output_path;
            char scan_side = 'S';
            std::filesystem::path source_root;
            bool recursive = true;
            bool metadata_scan = false;
            std::string output_format;
            std::string metadata_records = "all";
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t record_buffer_slots = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;
            bool pipeline_autoscale = true;
            std::string autoscale_profile;
            std::filesystem::path autoscale_settings_path;
            std::uint64_t autoscale_interval_ms = 1000;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                    metadata_scan = true;
                } else if (args[i] == "--scan-side") {
                    const std::string side = require_option(args, i, "--scan-side");
                    if (side.size() != 1U || (side[0] != 'S' && side[0] != 'T')) {
                        throw std::runtime_error("--scan-side must be S or T");
                    }
                    scan_side = side[0];
                } else if (args[i] == "--output-format" || args[i] == "--format") {
                    output_format = require_option(args, i, args[i]);
                    if (output_format == "auto") {
                        output_format.clear();
                    }
                    metadata_scan = true;
                } else if (args[i] == "--records") {
                    metadata_records = require_option(args, i, "--records");
                    metadata_scan = true;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                    metadata_scan = true;
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                    metadata_scan = true;
                } else if (args[i] == "--record-buffer-slots") {
                    record_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--record-buffer-slots"),
                                            "--record-buffer-slots");
                    metadata_scan = true;
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                    metadata_scan = true;
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                    metadata_scan = true;
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                    metadata_scan = true;
                } else if (args[i] == "--pipeline-autoscale") {
                    pipeline_autoscale = true;
                    metadata_scan = true;
                } else if (args[i] == "--no-pipeline-autoscale") {
                    pipeline_autoscale = false;
                    metadata_scan = true;
                } else if (args[i] == "--autoscale-profile") {
                    autoscale_profile = require_option(args, i, "--autoscale-profile");
                    metadata_scan = true;
                } else if (args[i] == "--autoscale-settings") {
                    autoscale_settings_path = require_option(args, i, "--autoscale-settings");
                    metadata_scan = true;
                } else if (args[i] == "--autoscale-interval-ms") {
                    autoscale_interval_ms =
                        parse_size_t_option(require_option(args, i, "--autoscale-interval-ms"),
                                            "--autoscale-interval-ms");
                    metadata_scan = true;
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const std::string output_extension = output_path.extension().string();
            if (output_extension == ".csv" || output_extension == ".parquet" || output_extension == ".txt") {
                metadata_scan = true;
            }

            if (metadata_scan) {
                const auto report = engine.benchmark_metadata_pipeline(source_root,
                                                                       recursive,
                                                                       true,
                                                                       true,
                                                                       meta_reader_threads,
                                                                       metadata_async_depth,
                                                                       output_path,
                                                                       output_format,
                                                                       metadata_records,
                                                                       max_duration_seconds,
                                                                       stats_interval_seconds,
                                                                       record_buffer_slots,
                                                                       1,
                                                                       "single",
                                                                       status_socket_path,
                                                                       pipeline_autoscale,
                                                                       autoscale_profile,
                                                                       autoscale_settings_path,
                                                                       autoscale_interval_ms);
                std::cout << "scan_metadata_records files_found=" << report.files_seen
                          << " folders_found=" << report.folders_found
                          << " logical_size_bytes=" << report.logical_size_bytes
                          << " records_per_second=" << report.records_per_second
                          << " meta_reader_threads=" << report.meta_reader_threads
                          << " metadata_async_depth=" << report.metadata_async_depth
                          << " record_buffer_slots=" << report.record_buffer_slots
                          << " stats_interval_seconds=" << report.stats_interval_seconds
                          << " metadata_files_written=" << report.metadata_files_written
                          << " metadata_folders_written=" << report.metadata_folders_written
                          << " scan_run_id=" << report.scan_run_id
                          << " pipeline_autoscale=" << (report.pipeline_autoscale ? "true" : "false")
                          << " autoscale_profile=" << report.autoscale_profile
                          << " autoscale_settings=" << report.autoscale_settings_path.string()
                          << " learned_meta_reader_threads=" << report.learned_meta_reader_threads
                          << " async_backend=" << (report.meta_reader_async ? "true" : "false")
                          << " elapsed_s=" << report.elapsed_seconds
                          << " output=" << output_path << '\n';
                return 0;
            }

            const auto index = engine.build_scan_index(source_root, scan_side, true);
            hypersync::TransferEngine::write_scan_csv(index, output_path);
            std::cout << "scan_rows=" << index.rows().size() << " output=" << output_path << '\n';
            return 0;
        }

        if (command == "dry-run") {
            std::filesystem::path source_root;
            std::string source_scan_path;
            std::string target_scan_path;
            std::string output_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--source-scan") {
                    source_scan_path = require_option(args, i, "--source-scan");
                } else if (args[i] == "--target-scan") {
                    target_scan_path = require_option(args, i, "--target-scan");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            std::optional<hypersync::ScanIndex> source_scan;
            std::optional<hypersync::ScanIndex> target_scan;
            if (!source_scan_path.empty()) {
                source_scan = hypersync::TransferEngine::load_scan_csv(source_scan_path);
            }
            if (!target_scan_path.empty()) {
                target_scan = hypersync::TransferEngine::load_scan_csv(target_scan_path);
            }

            const auto report = engine.dry_run_directory(source_root,
                                                         source_scan ? &*source_scan : nullptr,
                                                         target_scan ? &*target_scan : nullptr,
                                                         true);
            if (!output_path.empty()) {
                hypersync::TransferEngine::write_diff_csv(report, output_path);
            }
            std::cout << "files_total=" << report.files_total
                      << " skipped=" << report.files_skipped
                      << " bytes_planned=" << report.bytes_planned << '\n';
            if (output_path.empty()) {
                std::cout << report.diff_csv;
            }
            return 0;
        }

        if (command == "diff-target") {
            std::filesystem::path target_root;
            std::string listen_host = "0.0.0.0";
            std::uint16_t port = 0;
            std::string compare_mode = "size";
            bool recursive = true;
            std::size_t target_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::uint32_t stats_interval_seconds = 5;
            bool pipeline_autoscale = false;
            std::string autoscale_profile;
            std::filesystem::path autoscale_settings_path;
            std::uint64_t autoscale_interval_ms = 1000;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--target") {
                    target_root = require_option(args, i, "--target");
                } else if (args[i] == "--listen-host" || args[i] == "--bind-host") {
                    listen_host = require_option(args, i, args[i]);
                } else if (args[i] == "--port") {
                    port = parse_port(require_option(args, i, "--port"), "--port");
                } else if (args[i] == "--compare" || args[i] == "--mode") {
                    compare_mode = require_option(args, i, args[i]);
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--target-threads" || args[i] == "--threads") {
                    target_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth = parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                                               "--metadata-async-depth");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--pipeline-autoscale") {
                    pipeline_autoscale = true;
                } else if (args[i] == "--autoscale-profile") {
                    autoscale_profile = require_option(args, i, "--autoscale-profile");
                } else if (args[i] == "--autoscale-settings") {
                    autoscale_settings_path = require_option(args, i, "--autoscale-settings");
                } else if (args[i] == "--autoscale-interval-ms") {
                    autoscale_interval_ms = parse_size_t_option(require_option(args, i, "--autoscale-interval-ms"),
                                                                "--autoscale-interval-ms");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (target_root.empty()) {
                throw std::runtime_error("--target is required");
            }
            if (port == 0U) {
                throw std::runtime_error("--port is required");
            }
            engine.run_distributed_diff_target(target_root,
                                               listen_host,
                                               port,
                                               compare_mode,
                                               recursive,
                                               target_threads,
                                               metadata_async_depth,
                                               stats_interval_seconds,
                                               pipeline_autoscale,
                                               autoscale_profile,
                                               autoscale_settings_path,
                                               autoscale_interval_ms);
            return 0;
        }

        if (command == "diff-source") {
            std::filesystem::path source_root;
            std::string target_host;
            std::uint16_t port = 0;
            std::filesystem::path folder_report_path;
            std::string compare_mode = "size";
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            bool pipeline_autoscale = false;
            std::string autoscale_profile;
            std::filesystem::path autoscale_settings_path;
            std::uint64_t autoscale_interval_ms = 1000;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--target-host" || args[i] == "--host") {
                    target_host = require_option(args, i, args[i]);
                } else if (args[i] == "--port") {
                    port = parse_port(require_option(args, i, "--port"), "--port");
                } else if (args[i] == "--folder-report" || args[i] == "--output") {
                    folder_report_path = require_option(args, i, args[i]);
                } else if (args[i] == "--compare" || args[i] == "--mode") {
                    compare_mode = require_option(args, i, args[i]);
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads = parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                                              "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth = parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                                               "--metadata-async-depth");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds = parse_positive_double_option(
                        require_option(args, i, "--max-duration-seconds"),
                        "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--pipeline-autoscale") {
                    pipeline_autoscale = true;
                } else if (args[i] == "--autoscale-profile") {
                    autoscale_profile = require_option(args, i, "--autoscale-profile");
                } else if (args[i] == "--autoscale-settings") {
                    autoscale_settings_path = require_option(args, i, "--autoscale-settings");
                } else if (args[i] == "--autoscale-interval-ms") {
                    autoscale_interval_ms = parse_size_t_option(require_option(args, i, "--autoscale-interval-ms"),
                                                                "--autoscale-interval-ms");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (target_host.empty()) {
                throw std::runtime_error("--target-host is required");
            }
            if (port == 0U) {
                throw std::runtime_error("--port is required");
            }
            if (folder_report_path.empty()) {
                throw std::runtime_error("--folder-report is required");
            }
            const auto report = engine.run_distributed_diff_source(source_root,
                                                                   target_host,
                                                                   port,
                                                                   folder_report_path,
                                                                   compare_mode,
                                                                   recursive,
                                                                   meta_reader_threads,
                                                                   metadata_async_depth,
                                                                   max_duration_seconds,
                                                                   stats_interval_seconds,
                                                                   pipeline_autoscale,
                                                                   autoscale_profile,
                                                                   autoscale_settings_path,
                                                                   autoscale_interval_ms);
            std::cout << "distributed_diff"
                      << " folders_sent=" << report.folders_sent
                      << " folders_reported=" << report.folders_reported
                      << " files_compared=" << report.files_compared
                      << " same=" << report.files_same
                      << " changed=" << report.files_changed
                      << " new=" << report.files_new
                      << " target_only=" << report.files_target_only
                      << " failed=" << report.files_failed
                      << " source_logical_size_bytes=" << report.source_logical_size_bytes
                      << " target_logical_size_bytes=" << report.target_logical_size_bytes
                      << " bytes_planned=" << report.bytes_planned
                      << " folders_per_second=" << report.folders_per_second
                      << " files_per_second=" << report.files_per_second
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "diff") {
            std::filesystem::path source_root;
            std::filesystem::path target_root;
            std::string source_scan_path;
            std::string target_scan_path;
            std::string output_path;
            std::string compare_mode = "time";
            bool recursive = true;
            bool summary_only = false;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t checker_threads = 0;
            std::size_t checker_request_queue_depth = 0;
            std::size_t checker_batch_queue_depth = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--target") {
                    target_root = require_option(args, i, "--target");
                } else if (args[i] == "--source-scan") {
                    source_scan_path = require_option(args, i, "--source-scan");
                } else if (args[i] == "--target-scan") {
                    target_scan_path = require_option(args, i, "--target-scan");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--compare" || args[i] == "--mode") {
                    compare_mode = require_option(args, i, args[i]);
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--summary-only") {
                    summary_only = true;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads = parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                                              "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth = parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                                               "--metadata-async-depth");
                } else if (args[i] == "--checker-threads") {
                    checker_threads = parse_size_t_option(require_option(args, i, "--checker-threads"),
                                                          "--checker-threads");
                } else if (args[i] == "--checker-request-queue-depth" || args[i] == "--request-queue-depth") {
                    checker_request_queue_depth =
                        parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--checker-batch-queue-depth" || args[i] == "--batch-queue-depth") {
                    checker_batch_queue_depth =
                        parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds = parse_positive_double_option(
                        require_option(args, i, "--max-duration-seconds"),
                        "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if ((!source_root.empty() || !target_root.empty()) &&
                (!source_scan_path.empty() || !target_scan_path.empty())) {
                throw std::runtime_error("use either --source/--target or --source-scan/--target-scan, not both");
            }
            if (summary_only && !output_path.empty()) {
                throw std::runtime_error("--summary-only cannot be combined with --output");
            }

            hypersync::TransferReport report;
            if (!source_root.empty() || !target_root.empty()) {
                if (source_root.empty()) {
                    throw std::runtime_error("--source is required");
                }
                if (target_root.empty()) {
                    throw std::runtime_error("--target is required");
                }
                report = engine.diff_metadata_trees(source_root,
                                                    target_root,
                                                    compare_mode,
                                                    recursive,
                                                    meta_reader_threads,
                                                    metadata_async_depth,
                                                    max_duration_seconds,
                                                    !summary_only,
                                                    stats_interval_seconds,
                                                    checker_threads,
                                                    checker_request_queue_depth,
                                                    checker_batch_queue_depth);
            } else {
                if (source_scan_path.empty()) {
                    throw std::runtime_error("--source-scan is required");
                }
                if (target_scan_path.empty()) {
                    throw std::runtime_error("--target-scan is required");
                }
                const auto source_scan = hypersync::TransferEngine::load_scan_csv(source_scan_path);
                const auto target_scan = hypersync::TransferEngine::load_scan_csv(target_scan_path);
                report = engine.diff_scan_indexes(source_scan, target_scan, compare_mode);
            }
            if (!output_path.empty()) {
                hypersync::TransferEngine::write_diff_csv(report, output_path);
            }

            std::size_t changed = report.files_changed;
            std::size_t created = report.files_new;
            std::size_t target_only = report.files_target_only;
            if (report.files_changed == 0 && report.files_new == 0 && report.files_target_only == 0 &&
                !report.files.empty()) {
                for (const auto& [_, outcome] : report.files) {
                    if (outcome.diff == hypersync::DiffKind::changed) {
                        ++changed;
                    } else if (outcome.diff == hypersync::DiffKind::new_file) {
                        ++created;
                    } else if (outcome.diff == hypersync::DiffKind::target_only) {
                        ++target_only;
                    }
                }
            }
            std::cout << "diff_records=" << report.files_total
                      << " same=" << report.files_skipped
                      << " changed=" << changed
                      << " new=" << created
                      << " target_only=" << target_only
                      << " bytes_planned=" << report.bytes_planned
                      << " compare_mode=" << compare_mode << '\n';
            if (output_path.empty()) {
                std::cout << report.diff_csv;
            }
            return 0;
        }

        if (command == "benchmark-hash" || command == "hash-speed") {
            std::string hash_algorithm = "xxh64";
            std::size_t worker_threads = std::max<unsigned>(1U, std::thread::hardware_concurrency());
            std::size_t block_size = 1024U * 1024U;
            double duration_seconds = 5.0;
            double min_gigabits_per_core_second = 0.0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--hash" || args[i] == "--hash-algorithm") {
                    hash_algorithm = require_option(args, i, args[i]);
                } else if (args[i] == "--threads" || args[i] == "--hash-threads") {
                    worker_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--block-size" || args[i] == "--hash-block-size") {
                    block_size = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--duration-seconds" || args[i] == "--max-duration-seconds") {
                    duration_seconds =
                        parse_positive_double_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--min-gigabits-per-core" ||
                           args[i] == "--min-gbits-per-core" ||
                           args[i] == "--min-gigabits-per-core-second") {
                    min_gigabits_per_core_second =
                        parse_positive_double_option(require_option(args, i, args[i]), args[i]);
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            const auto report = engine.benchmark_hash_speed(hash_algorithm,
                                                            worker_threads,
                                                            block_size,
                                                            duration_seconds,
                                                            min_gigabits_per_core_second);
            std::cout << "hash_speed hash_algorithm=" << report.hash_algorithm
                      << " worker_threads=" << report.worker_threads
                      << " hardware_threads=" << report.hardware_threads
                      << " block_size=" << report.block_size
                      << " iterations=" << report.iterations
                      << " bytes_hashed=" << report.bytes_hashed
                      << " bytes_per_second=" << report.bytes_per_second
                      << " gigabits_per_second=" << report.gigabits_per_second
                      << " bytes_per_core_second=" << report.bytes_per_core_second
                      << " gigabits_per_core=" << report.gigabits_per_core_second
                      << " gigabits_per_hardware_core=" << report.gigabits_per_hardware_core_second
                      << " min_gigabits_per_core=" << report.min_gigabits_per_core_second
                      << " passed=" << (report.passed ? "true" : "false")
                      << " digest_mix=" << report.digest_mix
                      << " duration_s=" << report.duration_seconds
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return report.passed ? 0 : 2;
        }

        if (command == "benchmark-transport" || command == "transport-speed") {
            std::size_t transports = 1;
            std::uint64_t buffers_per_transport = 1024;
            std::size_t buffer_size = 1024U * 1024U;
            std::size_t pool_slots = 256;
            std::size_t generator_threads = 1;
            std::size_t sender_threads = 1;
            std::size_t receiver_threads = 1;
            std::size_t discarder_threads = 1;
            std::string pattern = "xoshiro256";
            std::string transport_kind = "unix";
            std::uint16_t base_port = 39000;
            std::filesystem::path socket_dir;
            bool shared_input_queue = false;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--transports" || args[i] == "--transport-count") {
                    transports = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--buffers-per-transport" ||
                           args[i] == "--buffer-count" ||
                           args[i] == "--buffers") {
                    buffers_per_transport = parse_u64_option(require_option(args, i, args[i]), args[i]);
                    if (buffers_per_transport == 0U) {
                        throw std::runtime_error("invalid positive integer for " + args[i]);
                    }
                } else if (args[i] == "--buffer-size" || args[i] == "--block-size") {
                    buffer_size = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--pool-slots" || args[i] == "--pool-slots-per-transport") {
                    pool_slots = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--generator-threads") {
                    generator_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--sender-threads") {
                    sender_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--receiver-threads") {
                    receiver_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--discarder-threads") {
                    discarder_threads = parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--pattern") {
                    pattern = require_option(args, i, "--pattern");
                } else if (args[i] == "--transport") {
                    transport_kind = require_option(args, i, "--transport");
                } else if (args[i] == "--shared-input" || args[i] == "--shared-input-queue") {
                    shared_input_queue = true;
                } else if (args[i] == "--base-port") {
                    base_port = parse_port(require_option(args, i, "--base-port"), "--base-port");
                } else if (args[i] == "--socket-dir") {
                    socket_dir = require_option(args, i, "--socket-dir");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            const auto report = engine.benchmark_buffer_transport(transports,
                                                                   buffers_per_transport,
                                                                   buffer_size,
                                                                   pool_slots,
                                                                   generator_threads,
                                                                   sender_threads,
                                                                   receiver_threads,
                                                                   discarder_threads,
                                                                   pattern,
                                                                   transport_kind,
                                                                   base_port,
                                                                   socket_dir,
                                                                   shared_input_queue);
            std::cout << "buffer_transport_benchmark"
                      << " transport=" << report.transport_kind
                      << " pattern=" << report.pattern
                      << " transports=" << report.transports
                      << " generator_threads=" << report.generator_threads
                      << " sender_threads=" << report.sender_threads
                      << " receiver_threads=" << report.receiver_threads
                      << " discarder_threads=" << report.discarder_threads
                      << " buffer_size=" << report.buffer_size
                      << " pool_slots_per_transport=" << report.pool_slots_per_transport
                      << " buffers_per_transport=" << report.buffers_per_transport
                      << " buffers_generated=" << report.buffers_generated
                      << " buffers_sent=" << report.buffers_sent
                      << " buffers_received=" << report.buffers_received
                      << " buffers_discarded=" << report.buffers_discarded
                      << " payload_bytes_sent=" << report.payload_bytes_sent
                      << " payload_bytes_received=" << report.payload_bytes_received
                      << " bytes_per_second=" << report.bytes_per_second
                      << " GB_per_second=" << report.gigabytes_per_second
                      << " GiB_per_second=" << report.gibibytes_per_second
                      << " Gbit_per_second=" << report.gigabits_per_second
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-fake-diff") {
            std::uint64_t file_count = 1'000'000;
            std::uint64_t folder_count = 1'000;
            std::uint64_t average_file_size = 32U * 1024U;
            std::size_t source_threads = 1;
            std::size_t fake_remote_threads = 1;
            std::size_t checker_threads = 0;
            std::uint64_t remote_delay_microseconds = 0;
            std::size_t request_queue_depth = 0;
            std::size_t batch_queue_depth = 0;
            std::uint32_t stats_interval_seconds = 0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--file-count" || args[i] == "--files") {
                    file_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--folder-count" || args[i] == "--folders") {
                    folder_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--average-file-size") {
                    average_file_size = parse_u64_option(require_option(args, i, "--average-file-size"),
                                                         "--average-file-size");
                } else if (args[i] == "--source-threads") {
                    source_threads =
                        parse_size_t_option(require_option(args, i, "--source-threads"), "--source-threads");
                } else if (args[i] == "--fake-remote-threads") {
                    fake_remote_threads =
                        parse_size_t_option(require_option(args, i, "--fake-remote-threads"),
                                            "--fake-remote-threads");
                } else if (args[i] == "--checker-threads") {
                    checker_threads =
                        parse_size_t_option(require_option(args, i, "--checker-threads"),
                                            "--checker-threads");
                } else if (args[i] == "--remote-delay-us") {
                    remote_delay_microseconds =
                        parse_u64_option(require_option(args, i, "--remote-delay-us"), "--remote-delay-us");
                } else if (args[i] == "--request-queue-depth") {
                    request_queue_depth =
                        parse_size_t_option(require_option(args, i, "--request-queue-depth"),
                                            "--request-queue-depth");
                } else if (args[i] == "--batch-queue-depth") {
                    batch_queue_depth =
                        parse_size_t_option(require_option(args, i, "--batch-queue-depth"),
                                            "--batch-queue-depth");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            const auto report = engine.benchmark_fake_remote_diff_pipeline(file_count,
                                                                           folder_count,
                                                                           average_file_size,
                                                                           source_threads,
                                                                           fake_remote_threads,
                                                                           remote_delay_microseconds,
                                                                           request_queue_depth,
                                                                           batch_queue_depth,
                                                                           stats_interval_seconds,
                                                                           checker_threads);
            std::cout << "fake_diff_benchmark"
                      << " source_files_generated=" << report.source_files_generated
                      << " source_folders_generated=" << report.source_folders_generated
                      << " target_folders_checked=" << report.target_folders_checked
                      << " files_compared=" << report.files_compared
                      << " files_same=" << report.files_same
                      << " bytes_compared=" << report.bytes_compared
                      << " source_records_per_second=" << report.source_records_per_second
                      << " total_records_per_second=" << report.total_records_per_second
                      << " source_threads=" << report.source_threads
                      << " fake_remote_threads=" << report.fake_remote_threads
                      << " checker_threads=" << report.checker_threads
                      << " remote_delay_us=" << report.remote_delay_microseconds
                      << " request_queue_depth=" << report.request_queue_depth
                      << " batch_queue_depth=" << report.batch_queue_depth
                      << " source_wait_target_queue_s=" << report.source_wait_target_queue_seconds
                      << " source_wait_batch_queue_s=" << report.source_wait_batch_queue_seconds
                      << " fake_remote_wait_request_s=" << report.fake_remote_wait_request_seconds
                      << " fake_remote_wait_processor_queue_s=" << report.fake_remote_wait_processor_queue_seconds
                      << " fake_remote_delay_s=" << report.fake_remote_delay_seconds
                      << " fake_remote_wait_batch_queue_s=" << report.fake_remote_wait_batch_queue_seconds
                      << " joiner_idle_s=" << report.joiner_idle_seconds
                      << " joiner_process_s=" << report.joiner_process_seconds
                      << " source_elapsed_s=" << report.source_elapsed_seconds
                      << " total_elapsed_s=" << report.total_elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-metadata-writer" || command == "benchmark-parquet") {
            std::filesystem::path output_path;
            std::string output_format = "parquet";
            std::uint64_t file_count = 1'000'000;
            std::uint64_t folder_count = 1'000;
            std::size_t batch_size = 65'536;
            std::uint64_t average_file_size = 32U * 1024U;
            std::string duckdb_memory_limit;
            std::size_t duckdb_threads = 0;
            std::string duckdb_checkpoint_threshold;
            std::string parquet_compression;
            std::size_t partitions = 1;
            std::string partition_mode = "threads";

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--output-format" || args[i] == "--format") {
                    output_format = require_option(args, i, args[i]);
                } else if (args[i] == "--file-count" || args[i] == "--files") {
                    file_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--folder-count" || args[i] == "--folders") {
                    folder_count = parse_u64_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--batch-size") {
                    batch_size = parse_size_t_option(require_option(args, i, "--batch-size"), "--batch-size");
                } else if (args[i] == "--average-file-size") {
                    average_file_size = parse_u64_option(require_option(args, i, "--average-file-size"),
                                                         "--average-file-size");
                } else if (args[i] == "--duckdb-memory-limit") {
                    duckdb_memory_limit = require_option(args, i, "--duckdb-memory-limit");
                } else if (args[i] == "--duckdb-threads") {
                    duckdb_threads = parse_size_t_option(require_option(args, i, "--duckdb-threads"),
                                                         "--duckdb-threads");
                } else if (args[i] == "--duckdb-checkpoint-threshold") {
                    duckdb_checkpoint_threshold = require_option(args, i, "--duckdb-checkpoint-threshold");
                } else if (args[i] == "--parquet-compression") {
                    parquet_compression = require_option(args, i, "--parquet-compression");
                } else if (args[i] == "--partitions") {
                    partitions = parse_size_t_option(require_option(args, i, "--partitions"), "--partitions");
                } else if (args[i] == "--partition-mode") {
                    partition_mode = require_option(args, i, "--partition-mode");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const auto report = engine.benchmark_metadata_writer(output_path,
                                                                  output_format,
                                                                  file_count,
                                                                  folder_count,
                                                                  batch_size,
                                                                  average_file_size,
                                                                  duckdb_memory_limit,
                                                                  duckdb_threads,
                                                                  duckdb_checkpoint_threshold,
                                                                  parquet_compression,
                                                                  partitions,
                                                                  partition_mode);
            std::cout << "metadata_writer_benchmark"
                      << " output_format=" << report.output_format
                      << " output=" << report.output_path
                      << " files_generated=" << report.files_generated
                      << " folders_generated=" << report.folders_generated
                      << " records_written=" << report.records_written
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " records_per_second=" << report.records_per_second
                      << " files_per_second=" << report.files_per_second
                      << " batch_size=" << report.batch_size
                      << " partitions=" << report.partitions
                      << " partition_mode=" << report.partition_mode
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "hash") {
            std::filesystem::path source_root;
            std::filesystem::path output_path;
            std::string output_format;
            std::string records = "all";
            std::string hash_algorithm;
            std::string hash_mode = "file";
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t hash_worker_threads = 0;
            std::size_t max_files_queued = 1024;
            std::size_t max_hash_chunks_queued = 4096;
            std::size_t hash_block_size = 1024U * 1024U;
            double max_duration_seconds = 0.0;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--output") {
                    output_path = require_option(args, i, "--output");
                } else if (args[i] == "--output-format" || args[i] == "--metadata-output-format") {
                    output_format = require_option(args, i, args[i]);
                } else if (args[i] == "--records" || args[i] == "--metadata-records") {
                    records = require_option(args, i, args[i]);
                } else if (args[i] == "--hash" || args[i] == "--hash-algorithm") {
                    hash_algorithm = require_option(args, i, args[i]);
                } else if (args[i] == "--hash-mode") {
                    hash_mode = require_option(args, i, "--hash-mode");
                } else if (args[i] == "--hash-block-size") {
                    hash_block_size =
                        parse_size_t_option(require_option(args, i, "--hash-block-size"),
                                            "--hash-block-size");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--hash-threads" || args[i] == "--hash-worker-threads") {
                    hash_worker_threads =
                        parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--max-hash-chunks-queued") {
                    max_hash_chunks_queued =
                        parse_size_t_option(require_option(args, i, "--max-hash-chunks-queued"),
                                            "--max-hash-chunks-queued");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (output_path.empty()) {
                throw std::runtime_error("--output is required");
            }

            const auto report = engine.hash_inventory_pipeline(source_root,
                                                               recursive,
                                                               hash_algorithm,
                                                               meta_reader_threads,
                                                               metadata_async_depth,
                                                               data_reader_threads,
                                                               data_outstanding_requests,
                                                               hash_worker_threads,
                                                               max_files_queued,
                                                               max_hash_chunks_queued,
                                                               hash_mode,
                                                               hash_block_size,
                                                               output_path,
                                                               output_format,
                                                               records,
                                                               max_duration_seconds);
            std::cout << "hash_inventory files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_hashed=" << report.files_hashed
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_hashed=" << report.bytes_hashed
                      << " read_bytes_per_second=" << report.read_bytes_per_second
                      << " read_gigabits_per_second=" << report.read_gigabits_per_second
                      << " bytes_per_second=" << report.bytes_per_second
                      << " hash_mode=" << report.hash_mode
                      << " hash_algorithm=" << report.hash_algorithm
                      << " metadata_files_written=" << report.metadata_files_written
                      << " metadata_folders_written=" << report.metadata_folders_written
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " hash_threads=" << report.hash_worker_threads
                      << " max_files_queued=" << report.max_files_queued
                      << " max_hash_chunks_queued=" << report.max_hash_chunks_queued
                      << " hash_block_size=" << report.hash_block_size
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " read_elapsed_s=" << report.read_elapsed_seconds
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return report.files_failed == 0 ? 0 : 2;
        }

        if (command == "benchmark-meta") {
            std::filesystem::path source_root;
            bool recursive = true;
            bool discard_after_checker =
                hypersync::load_checker_config(config_store).discard_checked_records;
            bool use_stats_discarder =
                hypersync::load_metadata_stats_discarder_config(config_store).enabled;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::filesystem::path metadata_output_path;
            std::string metadata_output_format;
            std::string metadata_records;
            std::size_t metadata_output_partitions = 1;
            std::string metadata_output_partition_mode = "single";
            std::size_t record_buffer_slots = 0;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;
            bool pipeline_autoscale = true;
            std::string autoscale_profile;
            std::filesystem::path autoscale_settings_path;
            std::uint64_t autoscale_interval_ms = 1000;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--discard-after-checker") {
                    discard_after_checker = true;
                } else if (args[i] == "--keep-after-checker") {
                    throw std::runtime_error("--keep-after-checker was removed; benchmark-meta now uses a pipeline buffer discarder");
                } else if (args[i] == "--metadata-stats-discarder") {
                    use_stats_discarder = true;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--metadata-output") {
                    metadata_output_path = require_option(args, i, "--metadata-output");
                    use_stats_discarder = true;
                } else if (args[i] == "--metadata-output-format") {
                    metadata_output_format = require_option(args, i, "--metadata-output-format");
                } else if (args[i] == "--metadata-records") {
                    metadata_records = require_option(args, i, "--metadata-records");
                } else if (args[i] == "--metadata-output-partitions") {
                    metadata_output_partitions =
                        parse_size_t_option(require_option(args, i, "--metadata-output-partitions"),
                                            "--metadata-output-partitions");
                } else if (args[i] == "--metadata-output-partition-mode") {
                    metadata_output_partition_mode = require_option(args, i, "--metadata-output-partition-mode");
                } else if (args[i] == "--record-buffer-slots") {
                    record_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--record-buffer-slots"),
                                            "--record-buffer-slots");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else if (args[i] == "--pipeline-autoscale") {
                    pipeline_autoscale = true;
                } else if (args[i] == "--no-pipeline-autoscale") {
                    pipeline_autoscale = false;
                } else if (args[i] == "--autoscale-profile") {
                    autoscale_profile = require_option(args, i, "--autoscale-profile");
                } else if (args[i] == "--autoscale-settings") {
                    autoscale_settings_path = require_option(args, i, "--autoscale-settings");
                } else if (args[i] == "--autoscale-interval-ms") {
                    autoscale_interval_ms =
                        parse_size_t_option(require_option(args, i, "--autoscale-interval-ms"),
                                            "--autoscale-interval-ms");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_metadata_pipeline(source_root,
                                                                   recursive,
                                                                   discard_after_checker,
                                                                   use_stats_discarder,
                                                                   meta_reader_threads,
                                                                   metadata_async_depth,
                                                                   metadata_output_path,
                                                                   metadata_output_format,
                                                                   metadata_records,
                                                                   max_duration_seconds,
                                                                   stats_interval_seconds,
                                                                   record_buffer_slots,
                                                                   metadata_output_partitions,
                                                                   metadata_output_partition_mode,
                                                                   status_socket_path,
                                                                   pipeline_autoscale,
                                                                   autoscale_profile,
                                                                   autoscale_settings_path,
                                                                   autoscale_interval_ms);
            std::cout << "files_seen=" << report.files_seen
                      << " checker_emitted=" << report.checker_emitted
                      << " checker_discarded=" << report.checker_discarded
                      << " folders_found=" << report.folders_found
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " records_per_second=" << report.records_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " record_buffer_slots=" << report.record_buffer_slots
                      << " metadata_queue_shards=" << report.metadata_queue_shards
                      << " metadata_queue_capacity=" << report.metadata_queue_capacity
                      << " metadata_queue_high_watermark=" << report.metadata_queue_high_watermark
                      << " metadata_queue_full=" << (report.metadata_queue_full ? "true" : "false")
                      << " stats_interval_seconds=" << report.stats_interval_seconds
                      << " metadata_files_written=" << report.metadata_files_written
                      << " metadata_folders_written=" << report.metadata_folders_written
                      << " metadata_output_partitions=" << report.metadata_output_partitions
                      << " scan_run_id=" << report.scan_run_id
                      << " pipeline_autoscale=" << (report.pipeline_autoscale ? "true" : "false")
                      << " autoscale_profile=" << report.autoscale_profile
                      << " autoscale_settings=" << report.autoscale_settings_path.string()
                      << " learned_meta_reader_threads=" << report.learned_meta_reader_threads
                      << " async_backend=" << (report.meta_reader_async ? "true" : "false")
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-data") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t readdirplus_page_bytes = 0;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t small_file_async_window = 0;
            bool split_small_large = false;
            bool dual_scan_small_large = false;
            bool recon_scan_enabled = false;
            bool morph_large_readers_to_small = false;
            bool bucket_priority_enabled = false;
            std::uint64_t split_small_file_threshold = 0;
            std::size_t recon_meta_reader_threads = 0;
            std::size_t recon_metadata_async_depth = 0;
            std::uint64_t recon_page_sleep_us = 0;
            std::size_t small_meta_reader_threads = 0;
            std::size_t large_meta_reader_threads = 0;
            std::size_t small_data_reader_threads = 0;
            std::size_t large_data_reader_threads = 0;
            std::size_t large_data_outstanding_requests = 0;
            bool pipeline_autoscale = false;
            bool large_reader_autoscale = false;
            std::size_t large_reader_initial_threads = 0;
            std::uint64_t autoscale_interval_ms = 1000;
            std::string autoscale_profile;
            std::filesystem::path autoscale_settings_path;
            std::uint64_t max_file_size_bytes = 0;
            std::size_t max_files_queued = 1024;
            std::size_t small_max_files_queued = 0;
            std::size_t large_max_files_queued = 0;
            std::size_t data_buffer_slots = 0;
            std::size_t data_queue_depth = 0;
            std::string data_copy_mode;
            bool pack_small_files = false;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--readdirplus-page-bytes") {
                    readdirplus_page_bytes =
                        parse_size_t_option(require_option(args, i, "--readdirplus-page-bytes"),
                                            "--readdirplus-page-bytes");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--small-file-async-window") {
                    small_file_async_window =
                        parse_size_t_option(require_option(args, i, "--small-file-async-window"),
                                            "--small-file-async-window");
                } else if (args[i] == "--split-small-large") {
                    split_small_large = true;
                } else if (args[i] == "--dual-scan-small-large") {
                    dual_scan_small_large = true;
                    split_small_large = true;
                } else if (args[i] == "--background-recon-scan" || args[i] == "--recon-scan") {
                    recon_scan_enabled = true;
                    dual_scan_small_large = true;
                    split_small_large = true;
                } else if (args[i] == "--morph-large-readers-to-small") {
                    morph_large_readers_to_small = true;
                    dual_scan_small_large = true;
                    split_small_large = true;
                } else if (args[i] == "--bucket-priority") {
                    bucket_priority_enabled = true;
                    morph_large_readers_to_small = true;
                    dual_scan_small_large = true;
                    split_small_large = true;
                } else if (args[i] == "--split-small-file-threshold" ||
                           args[i] == "--small-file-threshold-bytes") {
                    split_small_file_threshold =
                        parse_size_t_option(require_option(args, i, args[i]), args[i]);
                } else if (args[i] == "--recon-meta-reader-threads") {
                    recon_meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--recon-meta-reader-threads"),
                                            "--recon-meta-reader-threads");
                } else if (args[i] == "--recon-metadata-async-depth") {
                    recon_metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--recon-metadata-async-depth"),
                                            "--recon-metadata-async-depth");
                } else if (args[i] == "--recon-page-sleep-us") {
                    recon_page_sleep_us =
                        parse_size_t_option(require_option(args, i, "--recon-page-sleep-us"),
                                            "--recon-page-sleep-us");
                } else if (args[i] == "--small-meta-reader-threads") {
                    small_meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--small-meta-reader-threads"),
                                            "--small-meta-reader-threads");
                } else if (args[i] == "--large-meta-reader-threads") {
                    large_meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--large-meta-reader-threads"),
                                            "--large-meta-reader-threads");
                } else if (args[i] == "--small-data-reader-threads") {
                    small_data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--small-data-reader-threads"),
                                            "--small-data-reader-threads");
                } else if (args[i] == "--large-data-reader-threads") {
                    large_data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--large-data-reader-threads"),
                                            "--large-data-reader-threads");
                } else if (args[i] == "--large-data-outstanding-requests") {
                    large_data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--large-data-outstanding-requests"),
                                            "--large-data-outstanding-requests");
                } else if (args[i] == "--pipeline-autoscale") {
                    pipeline_autoscale = true;
                } else if (args[i] == "--large-reader-autoscale") {
                    large_reader_autoscale = true;
                } else if (args[i] == "--large-reader-initial-threads") {
                    large_reader_initial_threads =
                        parse_size_t_option(require_option(args, i, "--large-reader-initial-threads"),
                                            "--large-reader-initial-threads");
                } else if (args[i] == "--autoscale-interval-ms") {
                    autoscale_interval_ms = parse_size_t_option(require_option(args, i, "--autoscale-interval-ms"),
                                                                "--autoscale-interval-ms");
                } else if (args[i] == "--autoscale-profile") {
                    autoscale_profile = require_option(args, i, "--autoscale-profile");
                } else if (args[i] == "--autoscale-settings") {
                    autoscale_settings_path = require_option(args, i, "--autoscale-settings");
                } else if (args[i] == "--max-file-size-bytes") {
                    max_file_size_bytes =
                        parse_size_t_option(require_option(args, i, "--max-file-size-bytes"),
                                            "--max-file-size-bytes");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--small-max-files-queued") {
                    small_max_files_queued =
                        parse_size_t_option(require_option(args, i, "--small-max-files-queued"),
                                            "--small-max-files-queued");
                } else if (args[i] == "--large-max-files-queued") {
                    large_max_files_queued =
                        parse_size_t_option(require_option(args, i, "--large-max-files-queued"),
                                            "--large-max-files-queued");
                } else if (args[i] == "--data-buffer-slots") {
                    data_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--data-buffer-slots"),
                                            "--data-buffer-slots");
                } else if (args[i] == "--data-queue-depth") {
                    data_queue_depth =
                        parse_size_t_option(require_option(args, i, "--data-queue-depth"),
                                            "--data-queue-depth");
                } else if (args[i] == "--data-copy-mode") {
                    data_copy_mode = require_option(args, i, "--data-copy-mode");
                } else if (args[i] == "--pack-small-files") {
                    pack_small_files = true;
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_data_read_pipeline(source_root,
                                                                    recursive,
                                                                    meta_reader_threads,
                                                                    metadata_async_depth,
                                                                    readdirplus_page_bytes,
                                                                    data_reader_threads,
                                                                    data_outstanding_requests,
                                                                    small_file_async_window,
                                                                    split_small_large,
                                                                    dual_scan_small_large,
                                                                    recon_scan_enabled,
                                                                    morph_large_readers_to_small,
                                                                    bucket_priority_enabled,
                                                                    split_small_file_threshold,
                                                                    recon_meta_reader_threads,
                                                                    recon_metadata_async_depth,
                                                                    recon_page_sleep_us,
                                                                    small_meta_reader_threads,
                                                                    large_meta_reader_threads,
                                                                    small_data_reader_threads,
                                                                    large_data_reader_threads,
                                                                    large_data_outstanding_requests,
                                                                    pipeline_autoscale,
                                                                    large_reader_autoscale,
                                                                    large_reader_initial_threads,
                                                                    autoscale_interval_ms,
                                                                    autoscale_profile,
                                                                    autoscale_settings_path,
                                                                    max_file_size_bytes,
                                                                    max_files_queued,
                                                                    small_max_files_queued,
                                                                    large_max_files_queued,
                                                                    data_buffer_slots,
                                                                    data_queue_depth,
                                                                    data_copy_mode,
                                                                    pack_small_files,
                                                                    max_duration_seconds,
                                                                    stats_interval_seconds,
                                                                    status_socket_path);
            std::cout << "data_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_read=" << report.files_read
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " small_files_found=" << report.small_files_found
                      << " large_files_found=" << report.large_files_found
                      << " small_files_read=" << report.small_files_read
                      << " large_files_read=" << report.large_files_read
                      << " small_bytes_read=" << report.small_bytes_read
                      << " large_bytes_read=" << report.large_bytes_read
                      << " small_logical_size_bytes=" << report.small_logical_size_bytes
                      << " large_logical_size_bytes=" << report.large_logical_size_bytes
                      << " recon_files_found=" << report.recon_files_found
                      << " recon_folders_found=" << report.recon_folders_found
                      << " recon_small_files_found=" << report.recon_small_files_found
                      << " recon_large_files_found=" << report.recon_large_files_found
                      << " recon_logical_size_bytes=" << report.recon_logical_size_bytes
                      << " recon_small_logical_size_bytes=" << report.recon_small_logical_size_bytes
                      << " recon_large_logical_size_bytes=" << report.recon_large_logical_size_bytes
                      << " recon_completed=" << (report.recon_completed ? "true" : "false")
                      << " bytes_per_second=" << report.bytes_per_second
                      << " gigabits_per_second=" << report.gigabits_per_second
                      << " files_per_second=" << report.files_per_second
                      << " small_files_per_second=" << report.small_files_per_second
                      << " large_files_per_second=" << report.large_files_per_second
                      << " small_gigabits_per_second=" << report.small_gigabits_per_second
                      << " large_gigabits_per_second=" << report.large_gigabits_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " readdirplus_page_bytes=" << report.readdirplus_page_bytes
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " small_file_async_window=" << report.small_file_async_window
                      << " split_small_large=" << (report.split_small_large ? "true" : "false")
                      << " dual_scan_small_large=" << (report.dual_scan_small_large ? "true" : "false")
                      << " recon_scan_enabled=" << (report.recon_scan_enabled ? "true" : "false")
                      << " morph_large_readers_to_small=" << (report.morph_large_readers_to_small ? "true" : "false")
                      << " bucket_priority_enabled=" << (report.bucket_priority_enabled ? "true" : "false")
                      << " split_small_file_threshold=" << report.split_small_file_threshold
                      << " recon_meta_reader_threads=" << report.recon_meta_reader_threads
                      << " recon_metadata_async_depth=" << report.recon_metadata_async_depth
                      << " recon_page_sleep_us=" << report.recon_page_sleep_us
                      << " small_meta_reader_threads=" << report.small_meta_reader_threads
                      << " large_meta_reader_threads=" << report.large_meta_reader_threads
                      << " small_data_reader_threads=" << report.small_data_reader_threads
                      << " large_data_reader_threads=" << report.large_data_reader_threads
                      << " large_data_outstanding_requests=" << report.large_data_outstanding_requests
                      << " pipeline_autoscale=" << (report.pipeline_autoscale ? "true" : "false")
                      << " large_reader_autoscale=" << (report.large_reader_autoscale ? "true" : "false")
                      << " large_reader_initial_threads=" << report.large_reader_initial_threads
                      << " autoscale_interval_ms=" << report.autoscale_interval_ms
                      << " autoscale_profile=" << report.autoscale_profile
                      << " autoscale_settings=" << report.autoscale_settings_path.string()
                      << " max_file_size_bytes=" << report.max_file_size_bytes
                      << " max_files_queued=" << report.max_files_queued
                      << " small_max_files_queued=" << report.small_max_files_queued
                      << " large_max_files_queued=" << report.large_max_files_queued
                      << " data_buffer_slots=" << report.data_buffer_slots
                      << " data_queue_depth=" << report.data_queue_depth
                      << " data_copy_mode=" << report.data_copy_mode
                      << " pack_small_files=" << (report.pack_small_files ? "true" : "false")
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " async_read_queued=" << report.async_read_queued
                      << " async_read_completed=" << report.async_read_completed
                      << " async_read_short=" << report.async_read_short
                      << " async_read_failed=" << report.async_read_failed
                      << " async_read_zero=" << report.async_read_zero
                      << " async_read_bytes_requested=" << report.async_read_bytes_requested
                      << " async_read_bytes_completed=" << report.async_read_bytes_completed
                      << " async_read_avg_latency_ms=" << report.async_read_avg_latency_ms
                      << " async_read_max_latency_ms=" << report.async_read_max_latency_ms
                      << " async_open_completed=" << report.async_open_completed
                      << " async_open_failed=" << report.async_open_failed
                      << " async_open_avg_latency_ms=" << report.async_open_avg_latency_ms
                      << " async_open_max_latency_ms=" << report.async_open_max_latency_ms
                      << " async_close_completed=" << report.async_close_completed
                      << " async_close_failed=" << report.async_close_failed
                      << " async_close_avg_latency_ms=" << report.async_close_avg_latency_ms
                      << " async_close_max_latency_ms=" << report.async_close_max_latency_ms
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-data-write") {
            std::filesystem::path source_root;
            std::string target_root;
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t readdirplus_page_bytes = 0;
            std::size_t data_reader_threads = 0;
            std::size_t data_writer_threads = 0;
            std::size_t data_writer_async_window = 0;
            std::size_t data_writer_file_window = 0;
            std::size_t data_writer_reactors = 0;
            std::size_t reactors_per_ip = 1;
            std::size_t data_outstanding_requests = 0;
            std::size_t small_file_async_window = 0;
            std::uint64_t min_file_size_bytes = 0;
            std::uint64_t max_file_size_bytes = 0;
            std::size_t max_files_queued = 1024;
            std::size_t data_buffer_slots = 0;
            std::size_t data_queue_depth = 0;
            std::string data_copy_mode;
            bool pack_small_files = false;
            bool verify_hash = false;
            bool preserve_target_metadata = true;
            bool target_fsync = true;
            bool ensure_target_directories = true;
            bool stable_small_file_writes = false;
            bool direct_reactor_writes = false;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--target") {
                    target_root = require_option(args, i, "--target");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--readdirplus-page-bytes") {
                    readdirplus_page_bytes =
                        parse_size_t_option(require_option(args, i, "--readdirplus-page-bytes"),
                                            "--readdirplus-page-bytes");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-writer-threads") {
                    data_writer_threads =
                        parse_size_t_option(require_option(args, i, "--data-writer-threads"),
                                            "--data-writer-threads");
                } else if (args[i] == "--data-writer-async-window") {
                    data_writer_async_window =
                        parse_size_t_option(require_option(args, i, "--data-writer-async-window"),
                                            "--data-writer-async-window");
                } else if (args[i] == "--data-writer-file-window") {
                    data_writer_file_window =
                        parse_size_t_option(require_option(args, i, "--data-writer-file-window"),
                                            "--data-writer-file-window");
                } else if (args[i] == "--data-writer-reactors") {
                    data_writer_reactors =
                        parse_size_t_option(require_option(args, i, "--data-writer-reactors"),
                                            "--data-writer-reactors");
                } else if (args[i] == "--reactors-per-ip") {
                    reactors_per_ip =
                        parse_size_t_option(require_option(args, i, "--reactors-per-ip"),
                                            "--reactors-per-ip");
                } else if (args[i] == "--data-writer-stable-small-writes") {
                    stable_small_file_writes = true;
                } else if (args[i] == "--data-writer-direct-reactors") {
                    direct_reactor_writes = true;
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--small-file-async-window") {
                    small_file_async_window =
                        parse_size_t_option(require_option(args, i, "--small-file-async-window"),
                                            "--small-file-async-window");
                } else if (args[i] == "--min-file-size-bytes") {
                    min_file_size_bytes =
                        parse_size_t_option(require_option(args, i, "--min-file-size-bytes"),
                                            "--min-file-size-bytes");
                } else if (args[i] == "--max-file-size-bytes") {
                    max_file_size_bytes =
                        parse_size_t_option(require_option(args, i, "--max-file-size-bytes"),
                                            "--max-file-size-bytes");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--data-buffer-slots") {
                    data_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--data-buffer-slots"),
                                            "--data-buffer-slots");
                } else if (args[i] == "--data-queue-depth") {
                    data_queue_depth =
                        parse_size_t_option(require_option(args, i, "--data-queue-depth"),
                                            "--data-queue-depth");
                } else if (args[i] == "--data-copy-mode") {
                    data_copy_mode = require_option(args, i, "--data-copy-mode");
                } else if (args[i] == "--pack-small-files") {
                    pack_small_files = true;
                } else if (args[i] == "--skip-target-metadata") {
                    preserve_target_metadata = false;
                } else if (args[i] == "--no-target-fsync") {
                    target_fsync = false;
                } else if (args[i] == "--assume-target-directories") {
                    ensure_target_directories = false;
                } else if (args[i] == "--verify-hash") {
                    verify_hash = true;
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }
            if (target_root.empty()) {
                throw std::runtime_error("--target is required");
            }

            const auto report = engine.benchmark_data_write_pipeline(source_root,
                                                                     target_root,
                                                                     recursive,
                                                                     meta_reader_threads,
                                                                     metadata_async_depth,
                                                                     readdirplus_page_bytes,
                                                                     data_reader_threads,
                                                                     data_writer_threads,
                                                                     data_writer_async_window,
                                                                     data_outstanding_requests,
                                                                     small_file_async_window,
                                                                     min_file_size_bytes,
                                                                     max_file_size_bytes,
                                                                     max_files_queued,
                                                                     data_buffer_slots,
                                                                     data_queue_depth,
                                                                     data_copy_mode,
                                                                     pack_small_files,
                                                                     max_duration_seconds,
                                                                     stats_interval_seconds,
                                                                     verify_hash,
                                                                     preserve_target_metadata,
                                                                     target_fsync,
                                                                     ensure_target_directories,
                                                                     stable_small_file_writes,
                                                                     direct_reactor_writes,
                                                                     data_writer_reactors,
                                                                     reactors_per_ip,
                                                                     data_writer_file_window);
            std::cout << "data_write_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_read=" << report.files_read
                      << " files_written=" << report.files_written
                      << " files_failed=" << report.files_failed
                      << " write_failed=" << report.write_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_written=" << report.bytes_written
                      << " read_gigabits_per_second=" << report.gigabits_per_second
                      << " files_per_second=" << report.files_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " readdirplus_page_bytes=" << report.readdirplus_page_bytes
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_writer_threads=" << report.data_writer_threads
                      << " data_writer_async_window=" << report.data_writer_async_window
                      << " data_writer_file_window=" << report.data_writer_file_window
                      << " data_writer_reactors=" << report.data_writer_reactors
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " small_file_async_window=" << report.small_file_async_window
                      << " min_file_size_bytes=" << report.min_file_size_bytes
                      << " max_file_size_bytes=" << report.max_file_size_bytes
                      << " max_files_queued=" << report.max_files_queued
                      << " data_buffer_slots=" << report.data_buffer_slots
                      << " data_queue_depth=" << report.data_queue_depth
                      << " data_queue_shards=" << report.data_queue_shards
                      << " data_queue_capacity=" << report.data_queue_capacity
                      << " data_queue_high_watermark=" << report.data_queue_high_watermark
                      << " data_copy_mode=" << report.data_copy_mode
                      << " pack_small_files=" << (report.pack_small_files ? "true" : "false")
                      << " target=" << report.target_root
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " async_read_queued=" << report.async_read_queued
                      << " async_read_completed=" << report.async_read_completed
                      << " async_read_failed=" << report.async_read_failed
                      << " async_read_avg_latency_ms=" << report.async_read_avg_latency_ms
                      << " async_read_max_latency_ms=" << report.async_read_max_latency_ms
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-open") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t open_threads = 0;
            std::size_t max_files_queued = 1024;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--open-threads") {
                    open_threads =
                        parse_size_t_option(require_option(args, i, "--open-threads"), "--open-threads");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_nfs_open_pipeline(source_root,
                                                                   recursive,
                                                                   meta_reader_threads,
                                                                   metadata_async_depth,
                                                                   open_threads,
                                                                   max_files_queued,
                                                                   max_duration_seconds,
                                                                   stats_interval_seconds);
            const double files_per_second =
                report.elapsed_seconds > 0.0 ? static_cast<double>(report.files_read) / report.elapsed_seconds : 0.0;
            std::cout << "nfs_open_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_opened=" << report.files_read
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " files_per_second=" << files_per_second
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " open_threads=" << report.data_reader_threads
                      << " max_files_queued=" << report.max_files_queued
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " async_open_completed=" << report.async_open_completed
                      << " async_open_failed=" << report.async_open_failed
                      << " async_open_avg_latency_ms=" << report.async_open_avg_latency_ms
                      << " async_open_max_latency_ms=" << report.async_open_max_latency_ms
                      << " async_close_completed=" << report.async_close_completed
                      << " async_close_failed=" << report.async_close_failed
                      << " async_close_avg_latency_ms=" << report.async_close_avg_latency_ms
                      << " async_close_max_latency_ms=" << report.async_close_max_latency_ms
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        if (command == "benchmark-data-hash") {
            std::filesystem::path source_root;
            bool recursive = true;
            std::string hash_algorithm;
            std::size_t meta_reader_threads = 0;
            std::size_t metadata_async_depth = 0;
            std::size_t data_reader_threads = 0;
            std::size_t data_outstanding_requests = 0;
            std::size_t small_file_async_window = 0;
            std::size_t hash_worker_threads = 0;
            std::size_t hash_work_factor = 0;
            std::size_t max_files_queued = 1024;
            std::size_t data_buffer_slots = 0;
            std::size_t data_queue_depth = 0;
            bool pack_small_files = false;
            double max_duration_seconds = 0.0;
            std::uint32_t stats_interval_seconds = 5;
            std::filesystem::path status_socket_path;

            for (std::size_t i = 1; i < args.size(); ++i) {
                if (args[i] == "--source") {
                    source_root = require_option(args, i, "--source");
                } else if (args[i] == "--hash") {
                    hash_algorithm = require_option(args, i, "--hash");
                } else if (args[i] == "--non-recursive") {
                    recursive = false;
                } else if (args[i] == "--meta-reader-threads") {
                    meta_reader_threads =
                        parse_size_t_option(require_option(args, i, "--meta-reader-threads"),
                                            "--meta-reader-threads");
                } else if (args[i] == "--metadata-async-depth") {
                    metadata_async_depth =
                        parse_size_t_option(require_option(args, i, "--metadata-async-depth"),
                                            "--metadata-async-depth");
                } else if (args[i] == "--data-reader-threads") {
                    data_reader_threads =
                        parse_size_t_option(require_option(args, i, "--data-reader-threads"),
                                            "--data-reader-threads");
                } else if (args[i] == "--data-outstanding-requests") {
                    data_outstanding_requests =
                        parse_size_t_option(require_option(args, i, "--data-outstanding-requests"),
                                            "--data-outstanding-requests");
                } else if (args[i] == "--small-file-async-window") {
                    small_file_async_window =
                        parse_size_t_option(require_option(args, i, "--small-file-async-window"),
                                            "--small-file-async-window");
                } else if (args[i] == "--hash-threads") {
                    hash_worker_threads =
                        parse_size_t_option(require_option(args, i, "--hash-threads"),
                                            "--hash-threads");
                } else if (args[i] == "--hash-work-factor") {
                    hash_work_factor =
                        parse_size_t_option(require_option(args, i, "--hash-work-factor"),
                                            "--hash-work-factor");
                } else if (args[i] == "--max-files-queued") {
                    max_files_queued =
                        parse_size_t_option(require_option(args, i, "--max-files-queued"),
                                            "--max-files-queued");
                } else if (args[i] == "--data-buffer-slots") {
                    data_buffer_slots =
                        parse_size_t_option(require_option(args, i, "--data-buffer-slots"),
                                            "--data-buffer-slots");
                } else if (args[i] == "--data-queue-depth") {
                    data_queue_depth =
                        parse_size_t_option(require_option(args, i, "--data-queue-depth"),
                                            "--data-queue-depth");
                } else if (args[i] == "--pack-small-files") {
                    pack_small_files = true;
                } else if (args[i] == "--max-duration-seconds") {
                    max_duration_seconds =
                        parse_positive_double_option(require_option(args, i, "--max-duration-seconds"),
                                                     "--max-duration-seconds");
                } else if (args[i] == "--stats-interval-seconds") {
                    stats_interval_seconds = static_cast<std::uint32_t>(
                        parse_size_t_option(require_option(args, i, "--stats-interval-seconds"),
                                            "--stats-interval-seconds"));
                } else if (args[i] == "--status-socket") {
                    status_socket_path = require_option(args, i, "--status-socket");
                } else {
                    throw std::runtime_error("unknown option: " + args[i]);
                }
            }

            if (source_root.empty()) {
                throw std::runtime_error("--source is required");
            }

            const auto report = engine.benchmark_data_hash_pipeline(source_root,
                                                                    recursive,
                                                                    hash_algorithm,
                                                                    meta_reader_threads,
                                                                    metadata_async_depth,
                                                                    data_reader_threads,
                                                                    data_outstanding_requests,
                                                                    small_file_async_window,
                                                                    hash_worker_threads,
                                                                    max_files_queued,
                                                                    data_buffer_slots,
                                                                    data_queue_depth,
                                                                    hash_work_factor,
                                                                    pack_small_files,
                                                                    max_duration_seconds,
                                                                    stats_interval_seconds,
                                                                    status_socket_path);
            std::cout << "data_hash_benchmark files_found=" << report.files_found
                      << " folders_found=" << report.folders_found
                      << " files_read=" << report.files_read
                      << " files_failed=" << report.files_failed
                      << " logical_size_bytes=" << report.logical_size_bytes
                      << " bytes_read=" << report.bytes_read
                      << " bytes_hashed=" << report.bytes_hashed
                      << " read_bytes_per_second=" << report.read_bytes_per_second
                      << " read_gigabits_per_second=" << report.read_gigabits_per_second
                      << " hash_bytes_per_second=" << report.hash_bytes_per_second
                      << " hash_gigabits_per_second=" << report.hash_gigabits_per_second
                      << " hash_algorithm=" << report.hash_algorithm
                      << " meta_reader_threads=" << report.meta_reader_threads
                      << " metadata_async_depth=" << report.metadata_async_depth
                      << " data_reader_threads=" << report.data_reader_threads
                      << " data_outstanding_requests=" << report.data_outstanding_requests
                      << " small_file_async_window=" << report.small_file_async_window
                      << " hash_worker_threads=" << report.hash_worker_threads
                      << " hash_work_factor=" << report.hash_work_factor
                      << " pack_small_files=" << (report.pack_small_files ? "true" : "false")
                      << " max_files_queued=" << report.max_files_queued
                      << " data_buffer_slots=" << report.data_buffer_slots
                      << " data_queue_depth=" << report.data_queue_depth
                      << " meta_async=" << (report.meta_reader_async ? "true" : "false")
                      << " data_async=" << (report.data_reader_async ? "true" : "false")
                      << " elapsed_s=" << report.elapsed_seconds << '\n';
            return 0;
        }

        print_usage();
        return 1;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
