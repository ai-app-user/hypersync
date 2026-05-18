#include "core/nfs_backend.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__APPLE__)
extern "C" off_t lseek(int, off_t, int);
extern "C" ssize_t write(int, const void*, size_t);
#endif

#include "common/filesystem_utils.hpp"
#include "common/hash_utils.hpp"
#include "common/path_utils.hpp"
#include "common/records.hpp"
#include "common/socket_utils.hpp"
#include "core/data_buffer_codec.hpp"
#include "core/pipeline_buffers.hpp"
#include "jobs/synthetic_workload/synthetic_workload.hpp"

#ifndef HYPERSYNC_HAS_LIBNFS
#define HYPERSYNC_HAS_LIBNFS 0
#endif

#if HYPERSYNC_HAS_LIBNFS
#if defined(__has_include)
#if __has_include(<nfsc/libnfs.h>)
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw.h>
#include <nfsc/libnfs-raw-nfs.h>
#else
#include <libnfs.h>
#include <libnfs-raw.h>
#include <libnfs-raw-nfs.h>
#endif
#else
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw.h>
#include <nfsc/libnfs-raw-nfs.h>
#endif
#endif

namespace hypersync {

namespace {

struct NfsAsyncReadLatencyMetrics {
    std::atomic<std::uint64_t> queued {0};
    std::atomic<std::uint64_t> completed {0};
    std::atomic<std::uint64_t> failed {0};
    std::atomic<std::uint64_t> zero_reads {0};
    std::atomic<std::uint64_t> short_reads {0};
    std::atomic<std::uint64_t> bytes_requested {0};
    std::atomic<std::uint64_t> bytes_completed {0};
    std::atomic<std::uint64_t> latency_ns {0};
    std::atomic<std::uint64_t> max_latency_ns {0};
    std::array<std::atomic<std::uint64_t>, 8> latency_buckets {};
};

struct NfsAsyncCommandLatencyMetrics {
    std::atomic<std::uint64_t> open_completed {0};
    std::atomic<std::uint64_t> open_failed {0};
    std::atomic<std::uint64_t> open_latency_ns {0};
    std::atomic<std::uint64_t> open_max_latency_ns {0};
    std::atomic<std::uint64_t> close_completed {0};
    std::atomic<std::uint64_t> close_failed {0};
    std::atomic<std::uint64_t> close_latency_ns {0};
    std::atomic<std::uint64_t> close_max_latency_ns {0};
};

struct NfsReaddirplusPageMetrics {
    std::atomic<std::uint64_t> pages {0};
    std::atomic<std::uint64_t> failed_pages {0};
    std::atomic<std::uint64_t> entries {0};
    std::atomic<std::uint64_t> files {0};
    std::atomic<std::uint64_t> directories {0};
    std::atomic<std::uint64_t> requested_bytes {0};
    std::atomic<std::uint64_t> page_latency_ns {0};
    std::atomic<std::uint64_t> max_page_latency_ns {0};
    std::atomic<std::uint64_t> decode_latency_ns {0};
    std::atomic<std::uint64_t> max_decode_latency_ns {0};
};

enum class NfsAsyncCommandKind {
    other,
    open,
    close,
};

NfsAsyncReadLatencyMetrics& nfs_async_read_latency_metrics() {
    static NfsAsyncReadLatencyMetrics metrics;
    return metrics;
}

NfsAsyncCommandLatencyMetrics& nfs_async_command_latency_metrics() {
    static NfsAsyncCommandLatencyMetrics metrics;
    return metrics;
}

NfsReaddirplusPageMetrics& nfs_readdirplus_page_metrics() {
    static NfsReaddirplusPageMetrics metrics;
    return metrics;
}

[[maybe_unused]] bool nfs_page_trace_enabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("HYPERSYNC_NFS_PAGE_TRACE");
        return value != nullptr && value[0] != '\0' && std::string_view(value) != "0" &&
               std::string_view(value) != "false" && std::string_view(value) != "off";
    }();
    return enabled;
}

void reset_atomic_max(std::atomic<std::uint64_t>& value) {
    value.store(0, std::memory_order_relaxed);
}

void update_atomic_max(std::atomic<std::uint64_t>& target, std::uint64_t value) {
    std::uint64_t current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

std::size_t latency_bucket_index(std::uint64_t latency_ns) {
    if (latency_ns < 100'000ULL) {
        return 0;
    }
    if (latency_ns < 500'000ULL) {
        return 1;
    }
    if (latency_ns < 1'000'000ULL) {
        return 2;
    }
    if (latency_ns < 5'000'000ULL) {
        return 3;
    }
    if (latency_ns < 10'000'000ULL) {
        return 4;
    }
    if (latency_ns < 50'000'000ULL) {
        return 5;
    }
    if (latency_ns < 100'000'000ULL) {
        return 6;
    }
    return 7;
}

std::uint64_t steady_latency_ns(std::chrono::steady_clock::time_point start,
                                std::chrono::steady_clock::time_point end) {
    if (start == std::chrono::steady_clock::time_point{} || end <= start) {
        return 0;
    }
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
}

[[maybe_unused]] void pin_current_thread_to_cpu(std::size_t cpu_index) noexcept {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(cpu_index), &set);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
    (void)cpu_index;
#endif
}

[[maybe_unused]] void record_async_read_queued(std::size_t requested) {
    NfsAsyncReadLatencyMetrics& metrics = nfs_async_read_latency_metrics();
    metrics.queued.fetch_add(1, std::memory_order_relaxed);
    metrics.bytes_requested.fetch_add(requested, std::memory_order_relaxed);
}

[[maybe_unused]] void record_async_read_completed(const std::chrono::steady_clock::time_point& queued_at,
                                                  std::size_t requested,
                                                  int status) {
    NfsAsyncReadLatencyMetrics& metrics = nfs_async_read_latency_metrics();
    const auto completed_at = std::chrono::steady_clock::now();
    const std::uint64_t latency = steady_latency_ns(queued_at, completed_at);
    metrics.completed.fetch_add(1, std::memory_order_relaxed);
    metrics.latency_ns.fetch_add(latency, std::memory_order_relaxed);
    update_atomic_max(metrics.max_latency_ns, latency);
    metrics.latency_buckets[latency_bucket_index(latency)].fetch_add(1, std::memory_order_relaxed);

    if (status < 0) {
        metrics.failed.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (status == 0) {
        metrics.zero_reads.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    const auto bytes = static_cast<std::size_t>(status);
    metrics.bytes_completed.fetch_add(bytes, std::memory_order_relaxed);
    if (bytes < requested) {
        metrics.short_reads.fetch_add(1, std::memory_order_relaxed);
    }
}

[[maybe_unused]] void record_async_command_completed(NfsAsyncCommandKind kind,
                                                     const std::chrono::steady_clock::time_point& queued_at,
                                                     int status) {
    if (kind == NfsAsyncCommandKind::other) {
        return;
    }

    NfsAsyncCommandLatencyMetrics& metrics = nfs_async_command_latency_metrics();
    const std::uint64_t latency = steady_latency_ns(queued_at, std::chrono::steady_clock::now());
    auto& completed = kind == NfsAsyncCommandKind::open ? metrics.open_completed : metrics.close_completed;
    auto& failed = kind == NfsAsyncCommandKind::open ? metrics.open_failed : metrics.close_failed;
    auto& latency_ns = kind == NfsAsyncCommandKind::open ? metrics.open_latency_ns : metrics.close_latency_ns;
    auto& max_latency_ns = kind == NfsAsyncCommandKind::open ? metrics.open_max_latency_ns : metrics.close_max_latency_ns;
    completed.fetch_add(1, std::memory_order_relaxed);
    latency_ns.fetch_add(latency, std::memory_order_relaxed);
    update_atomic_max(max_latency_ns, latency);
    if (status < 0) {
        failed.fetch_add(1, std::memory_order_relaxed);
    }
}

[[maybe_unused]] void record_readdirplus_page_completed(
    const std::chrono::steady_clock::time_point& queued_at,
    const std::chrono::steady_clock::time_point& callback_started_at,
    const std::chrono::steady_clock::time_point& callback_finished_at,
    std::size_t requested_bytes,
    std::size_t entries,
    std::size_t files,
    std::size_t directories,
    bool failed) {
    NfsReaddirplusPageMetrics& metrics = nfs_readdirplus_page_metrics();
    const std::uint64_t page_latency = steady_latency_ns(queued_at, callback_started_at);
    const std::uint64_t decode_latency = steady_latency_ns(callback_started_at, callback_finished_at);
    metrics.pages.fetch_add(1, std::memory_order_relaxed);
    metrics.requested_bytes.fetch_add(requested_bytes, std::memory_order_relaxed);
    metrics.entries.fetch_add(entries, std::memory_order_relaxed);
    metrics.files.fetch_add(files, std::memory_order_relaxed);
    metrics.directories.fetch_add(directories, std::memory_order_relaxed);
    metrics.page_latency_ns.fetch_add(page_latency, std::memory_order_relaxed);
    metrics.decode_latency_ns.fetch_add(decode_latency, std::memory_order_relaxed);
    update_atomic_max(metrics.max_page_latency_ns, page_latency);
    update_atomic_max(metrics.max_decode_latency_ns, decode_latency);
    if (failed) {
        metrics.failed_pages.fetch_add(1, std::memory_order_relaxed);
    }
}

}  // namespace

void reset_nfs_async_read_latency_metrics() {
    NfsAsyncReadLatencyMetrics& metrics = nfs_async_read_latency_metrics();
    metrics.queued.store(0, std::memory_order_relaxed);
    metrics.completed.store(0, std::memory_order_relaxed);
    metrics.failed.store(0, std::memory_order_relaxed);
    metrics.zero_reads.store(0, std::memory_order_relaxed);
    metrics.short_reads.store(0, std::memory_order_relaxed);
    metrics.bytes_requested.store(0, std::memory_order_relaxed);
    metrics.bytes_completed.store(0, std::memory_order_relaxed);
    metrics.latency_ns.store(0, std::memory_order_relaxed);
    reset_atomic_max(metrics.max_latency_ns);
    for (auto& bucket : metrics.latency_buckets) {
        bucket.store(0, std::memory_order_relaxed);
    }

    NfsAsyncCommandLatencyMetrics& command_metrics = nfs_async_command_latency_metrics();
    command_metrics.open_completed.store(0, std::memory_order_relaxed);
    command_metrics.open_failed.store(0, std::memory_order_relaxed);
    command_metrics.open_latency_ns.store(0, std::memory_order_relaxed);
    reset_atomic_max(command_metrics.open_max_latency_ns);
    command_metrics.close_completed.store(0, std::memory_order_relaxed);
    command_metrics.close_failed.store(0, std::memory_order_relaxed);
    command_metrics.close_latency_ns.store(0, std::memory_order_relaxed);
    reset_atomic_max(command_metrics.close_max_latency_ns);

    NfsReaddirplusPageMetrics& page_metrics = nfs_readdirplus_page_metrics();
    page_metrics.pages.store(0, std::memory_order_relaxed);
    page_metrics.failed_pages.store(0, std::memory_order_relaxed);
    page_metrics.entries.store(0, std::memory_order_relaxed);
    page_metrics.files.store(0, std::memory_order_relaxed);
    page_metrics.directories.store(0, std::memory_order_relaxed);
    page_metrics.requested_bytes.store(0, std::memory_order_relaxed);
    page_metrics.page_latency_ns.store(0, std::memory_order_relaxed);
    reset_atomic_max(page_metrics.max_page_latency_ns);
    page_metrics.decode_latency_ns.store(0, std::memory_order_relaxed);
    reset_atomic_max(page_metrics.max_decode_latency_ns);
}

NfsAsyncReadLatencySnapshot snapshot_nfs_async_read_latency_metrics() {
    NfsAsyncReadLatencyMetrics& metrics = nfs_async_read_latency_metrics();
    NfsAsyncReadLatencySnapshot snapshot;
    snapshot.queued = metrics.queued.load(std::memory_order_relaxed);
    snapshot.completed = metrics.completed.load(std::memory_order_relaxed);
    snapshot.failed = metrics.failed.load(std::memory_order_relaxed);
    snapshot.zero_reads = metrics.zero_reads.load(std::memory_order_relaxed);
    snapshot.short_reads = metrics.short_reads.load(std::memory_order_relaxed);
    snapshot.bytes_requested = metrics.bytes_requested.load(std::memory_order_relaxed);
    snapshot.bytes_completed = metrics.bytes_completed.load(std::memory_order_relaxed);
    snapshot.latency_ns = metrics.latency_ns.load(std::memory_order_relaxed);
    snapshot.max_latency_ns = metrics.max_latency_ns.load(std::memory_order_relaxed);
    for (std::size_t index = 0; index < snapshot.latency_buckets.size(); ++index) {
        snapshot.latency_buckets[index] = metrics.latency_buckets[index].load(std::memory_order_relaxed);
    }
    return snapshot;
}

NfsAsyncCommandLatencySnapshot snapshot_nfs_async_command_latency_metrics() {
    NfsAsyncCommandLatencyMetrics& metrics = nfs_async_command_latency_metrics();
    NfsAsyncCommandLatencySnapshot snapshot;
    snapshot.open_completed = metrics.open_completed.load(std::memory_order_relaxed);
    snapshot.open_failed = metrics.open_failed.load(std::memory_order_relaxed);
    snapshot.open_latency_ns = metrics.open_latency_ns.load(std::memory_order_relaxed);
    snapshot.open_max_latency_ns = metrics.open_max_latency_ns.load(std::memory_order_relaxed);
    snapshot.close_completed = metrics.close_completed.load(std::memory_order_relaxed);
    snapshot.close_failed = metrics.close_failed.load(std::memory_order_relaxed);
    snapshot.close_latency_ns = metrics.close_latency_ns.load(std::memory_order_relaxed);
    snapshot.close_max_latency_ns = metrics.close_max_latency_ns.load(std::memory_order_relaxed);
    return snapshot;
}

NfsReaddirplusPageSnapshot snapshot_nfs_readdirplus_page_metrics() {
    NfsReaddirplusPageMetrics& metrics = nfs_readdirplus_page_metrics();
    NfsReaddirplusPageSnapshot snapshot;
    snapshot.pages = metrics.pages.load(std::memory_order_relaxed);
    snapshot.failed_pages = metrics.failed_pages.load(std::memory_order_relaxed);
    snapshot.entries = metrics.entries.load(std::memory_order_relaxed);
    snapshot.files = metrics.files.load(std::memory_order_relaxed);
    snapshot.directories = metrics.directories.load(std::memory_order_relaxed);
    snapshot.requested_bytes = metrics.requested_bytes.load(std::memory_order_relaxed);
    snapshot.page_latency_ns = metrics.page_latency_ns.load(std::memory_order_relaxed);
    snapshot.max_page_latency_ns = metrics.max_page_latency_ns.load(std::memory_order_relaxed);
    snapshot.decode_latency_ns = metrics.decode_latency_ns.load(std::memory_order_relaxed);
    snapshot.max_decode_latency_ns = metrics.max_decode_latency_ns.load(std::memory_order_relaxed);
    return snapshot;
}

namespace {

std::uint64_t current_unix_time_nanoseconds() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::uint64_t stat_mtime_ns(const struct stat& info) {
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(info.st_mtimespec.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtimespec.tv_nsec);
#else
    return static_cast<std::uint64_t>(info.st_mtim.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(info.st_mtim.tv_nsec);
#endif
}

[[maybe_unused]] std::uint64_t truncate_to_microseconds(std::uint64_t timestamp_ns) {
    return (timestamp_ns / 1000ULL) * 1000ULL;
}

[[maybe_unused]] std::size_t path_depth(std::string_view rel_path) {
    if (rel_path.empty()) {
        return 0;
    }
    return 1U + static_cast<std::size_t>(std::count(rel_path.begin(), rel_path.end(), '/'));
}

std::vector<std::string_view> split_string_view(std::string_view value, char separator) {
    std::vector<std::string_view> parts;
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const std::size_t end = value.find(separator, begin);
        parts.push_back(value.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin));
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1U;
    }
    return parts;
}

std::string_view trim_view(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.remove_prefix(1U);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        value.remove_suffix(1U);
    }
    return value;
}

std::optional<std::array<std::uint8_t, 4>> parse_ipv4_address(std::string_view value) {
    std::array<std::uint8_t, 4> octets {};
    const std::vector<std::string_view> parts = split_string_view(value, '.');
    if (parts.size() != octets.size()) {
        return std::nullopt;
    }
    for (std::size_t index = 0; index < parts.size(); ++index) {
        if (parts[index].empty()) {
            return std::nullopt;
        }
        std::uint32_t octet = 0;
        for (char ch : parts[index]) {
            if (std::isdigit(static_cast<unsigned char>(ch)) == 0) {
                return std::nullopt;
            }
            octet = (octet * 10U) + static_cast<std::uint32_t>(ch - '0');
            if (octet > 255U) {
                return std::nullopt;
            }
        }
        octets[index] = static_cast<std::uint8_t>(octet);
    }
    return octets;
}

std::uint32_t ipv4_to_u32(const std::array<std::uint8_t, 4>& octets) {
    return (static_cast<std::uint32_t>(octets[0]) << 24U) |
           (static_cast<std::uint32_t>(octets[1]) << 16U) |
           (static_cast<std::uint32_t>(octets[2]) << 8U) |
           static_cast<std::uint32_t>(octets[3]);
}

std::string ipv4_to_string(std::uint32_t value) {
    std::ostringstream output;
    output << ((value >> 24U) & 0xffU) << '.'
           << ((value >> 16U) & 0xffU) << '.'
           << ((value >> 8U) & 0xffU) << '.'
           << (value & 0xffU);
    return output.str();
}

std::optional<std::uint8_t> parse_ipv4_last_octet(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    std::uint32_t octet = 0;
    for (char ch : value) {
        if (std::isdigit(static_cast<unsigned char>(ch)) == 0) {
            return std::nullopt;
        }
        octet = (octet * 10U) + static_cast<std::uint32_t>(ch - '0');
        if (octet > 255U) {
            return std::nullopt;
        }
    }
    return static_cast<std::uint8_t>(octet);
}

std::vector<std::string> expand_server_token(std::string_view token) {
    token = trim_view(token);
    if (token.empty()) {
        return {};
    }

    const std::size_t dash = token.find('-');
    if (dash == std::string_view::npos) {
        return {std::string(token)};
    }

    const std::string_view lhs = trim_view(token.substr(0, dash));
    const std::string_view rhs = trim_view(token.substr(dash + 1U));
    const std::optional<std::array<std::uint8_t, 4>> start_octets = parse_ipv4_address(lhs);
    if (!start_octets.has_value()) {
        return {std::string(token)};
    }

    std::optional<std::array<std::uint8_t, 4>> end_octets = parse_ipv4_address(rhs);
    if (!end_octets.has_value()) {
        const std::optional<std::uint8_t> last_octet = parse_ipv4_last_octet(rhs);
        if (!last_octet.has_value()) {
            return {std::string(token)};
        }
        end_octets = start_octets;
        (*end_octets)[3] = *last_octet;
    }

    const std::uint32_t start = ipv4_to_u32(*start_octets);
    const std::uint32_t end = ipv4_to_u32(*end_octets);
    if (end < start) {
        throw std::invalid_argument("NFS server IP range must be ascending: " + std::string(token));
    }
    constexpr std::uint32_t kMaxExpandedServers = 4096U;
    if ((end - start) + 1U > kMaxExpandedServers) {
        throw std::invalid_argument("NFS server IP range expands to too many endpoints: " + std::string(token));
    }

    std::vector<std::string> servers;
    servers.reserve(static_cast<std::size_t>((end - start) + 1U));
    for (std::uint32_t value = start; value <= end; ++value) {
        servers.push_back(ipv4_to_string(value));
        if (value == end) {
            break;
        }
    }
    return servers;
}

std::vector<std::string> expand_server_expression(std::string_view expression) {
    std::vector<std::string> servers;
    for (std::string_view token : split_string_view(expression, ',')) {
        std::vector<std::string> expanded = expand_server_token(token);
        servers.insert(servers.end(),
                       std::make_move_iterator(expanded.begin()),
                       std::make_move_iterator(expanded.end()));
    }
    if (servers.empty()) {
        servers.push_back(std::string(expression));
    }
    return servers;
}

[[maybe_unused]] std::string nfs_endpoint_key(std::string_view url) {
    constexpr std::string_view kPrefix = "nfs://";
    if (url.rfind(kPrefix, 0) != 0) {
        return std::string(url);
    }
    const std::size_t server_begin = kPrefix.size();
    const std::size_t server_end = url.find('/', server_begin);
    return std::string(url.substr(server_begin, server_end == std::string_view::npos
                                                    ? std::string_view::npos
                                                    : server_end - server_begin));
}

struct NfsEndpointHealth {
    std::chrono::steady_clock::time_point retry_after {};
    std::uint32_t failures = 0;
};

std::mutex g_nfs_endpoint_health_mutex;
std::unordered_map<std::string, NfsEndpointHealth> g_nfs_endpoint_health;

[[maybe_unused]] bool nfs_endpoint_ready_for_probe(std::string_view url) {
    const std::string key = nfs_endpoint_key(url);
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(g_nfs_endpoint_health_mutex);
    const auto it = g_nfs_endpoint_health.find(key);
    return it == g_nfs_endpoint_health.end() || now >= it->second.retry_after;
}

[[maybe_unused]] void mark_nfs_endpoint_healthy(std::string_view url) {
    const std::string key = nfs_endpoint_key(url);
    std::lock_guard<std::mutex> lock(g_nfs_endpoint_health_mutex);
    g_nfs_endpoint_health.erase(key);
}

[[maybe_unused]] void mark_nfs_endpoint_unhealthy(std::string_view url) {
    const std::string key = nfs_endpoint_key(url);
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(g_nfs_endpoint_health_mutex);
    NfsEndpointHealth& health = g_nfs_endpoint_health[key];
    health.failures = std::min<std::uint32_t>(health.failures + 1U, 8U);
    const auto cooldown = std::chrono::seconds(std::min<int>(300, 15 * (1 << std::min<std::uint32_t>(health.failures - 1U, 4U))));
    health.retry_after = now + cooldown;
}

[[maybe_unused]] std::vector<std::string> ordered_nfs_connection_urls(std::string_view root_url,
                                                                      std::size_t endpoint_index) {
    std::vector<std::string> candidates = expand_nfs_url_server_candidates(root_url);
    if (candidates.size() <= 1U) {
        return candidates;
    }

    std::size_t first = 0;
    if (endpoint_index != kNfsEndpointAny) {
        first = endpoint_index % candidates.size();
    } else {
        thread_local std::mt19937_64 rng(std::random_device{}());
        std::uniform_int_distribution<std::size_t> distribution(0U, candidates.size() - 1U);
        first = distribution(rng);
    }

    std::vector<std::string> ordered;
    ordered.reserve(candidates.size());
    for (std::size_t offset = 0; offset < candidates.size(); ++offset) {
        ordered.push_back(std::move(candidates[(first + offset) % candidates.size()]));
    }
    return ordered;
}

void local_pwrite_all(int fd, std::string_view data, std::uint64_t offset) {
    std::size_t written_total = 0;
    while (written_total < data.size()) {
#if defined(__APPLE__)
        if (::lseek(fd, static_cast<off_t>(offset + written_total), SEEK_SET) < 0) {
            throw std::system_error(errno, std::generic_category(), "lseek failed");
        }
        const ssize_t written = ::write(fd, data.data() + written_total, data.size() - written_total);
#else
        const ssize_t written = ::pwrite(fd,
                                         data.data() + written_total,
                                         data.size() - written_total,
                                         static_cast<off_t>(offset + written_total));
#endif
        if (written < 0) {
            throw std::system_error(errno, std::generic_category(), "write failed");
        }
        written_total += static_cast<std::size_t>(written);
    }
}

FileSpec local_spec_from_absolute(const std::filesystem::path& root, const std::filesystem::path& absolute_path) {
    struct stat info {};
    if (::stat(absolute_path.c_str(), &info) != 0) {
        throw std::system_error(errno, std::generic_category(), "stat failed for " + absolute_path.string());
    }

    FileSpec spec;
    spec.rel_path = normalize_path(std::filesystem::relative(absolute_path, root).generic_string());
    spec.mtime = stat_mtime_ns(info);
    spec.mode = static_cast<std::uint32_t>(info.st_mode & 0777U);
    spec.uid = static_cast<std::uint32_t>(info.st_uid);
    spec.gid = static_cast<std::uint32_t>(info.st_gid);
    spec.declared_size = static_cast<std::uint64_t>(info.st_size);
    return spec;
}

class LocalFilesystemBackend final : public NfsBackend {
public:
    explicit LocalFilesystemBackend(std::string root) : root_(std::move(root)) {}

    [[nodiscard]] std::vector<FileSpec> list_files(bool recursive) const override {
        return collect_file_specs(root_, recursive);
    }

    [[nodiscard]] std::vector<FileSpec> list_directories(bool recursive) const override {
        return collect_directory_specs(root_, recursive);
    }

    void visit_metadata_at(std::string_view rel_path,
                           bool recursive,
                           const std::function<void(FileSpec)>& file_visitor,
                           const std::function<void(FileSpec)>& directory_visitor) const override {
        const std::filesystem::path absolute_root = root_ / normalize_path(rel_path);
        const auto emit_entry = [&](const std::filesystem::path& path) {
            struct stat info {};
            if (::stat(path.c_str(), &info) != 0) {
                throw std::system_error(errno, std::generic_category(), "stat failed for " + path.string());
            }
            if (S_ISDIR(info.st_mode)) {
                directory_visitor(local_spec_from_absolute(root_, path));
                return;
            }
            if (S_ISREG(info.st_mode)) {
                file_visitor(local_spec_from_absolute(root_, path));
            }
        };

        if (recursive) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(absolute_root)) {
                emit_entry(entry.path());
            }
            return;
        }
        for (const auto& entry : std::filesystem::directory_iterator(absolute_root)) {
            emit_entry(entry.path());
        }
    }

    void visit_folder(std::string_view rel_path,
                      const std::function<void(FileSpec)>& file_visitor,
                      const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        FileSpec spec = local_spec_from_absolute(root_, absolute_path);
        spec.content = read_file_contents(absolute_path);
        if (spec.declared_size == 0) {
            spec.declared_size = spec.content.size();
        }
        return spec;
    }

    [[nodiscard]] std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const override {
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> buffer(1024U * 1024U);
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                total += bytes_read;
                if (bytes_visitor) {
                    bytes_visitor(bytes_read);
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        if (declared_size != 0 && total < declared_size) {
            throw std::runtime_error("unexpected EOF while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_stream(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::string_view)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> buffer(1024U * 1024U);
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                total += bytes_read;
                if (data_visitor) {
                    data_visitor(std::string_view(buffer.data(), static_cast<std::size_t>(bytes)));
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_into(std::string_view rel_path,
                                               std::uint64_t declared_size,
                                               std::byte* destination,
                                               std::size_t destination_bytes) const override {
        (void)declared_size;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::uint64_t total = 0;
        while (input && total < destination_bytes) {
            const std::size_t remaining = destination_bytes - static_cast<std::size_t>(total);
            input.read(reinterpret_cast<char*>(destination + total), static_cast<std::streamsize>(remaining));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                total += static_cast<std::uint64_t>(bytes);
            }
        }
        if (total == destination_bytes) {
            return total;
        }
        if (!input.eof()) {
            throw std::runtime_error("destination buffer is too small while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_owned_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::uint64_t total = 0;
        while (input) {
            OwnedFileChunk chunk;
            chunk.offset = total;
            chunk.data.assign(1024U * 1024U, '\0');
            input.read(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                chunk.data.resize(static_cast<std::size_t>(bytes));
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                }
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::uint64_t total = 0;
        while (input) {
            DataSlotHandle handle = pool.acquire_wait_or_throw(DataSlotClass::large, kLargeChunkBytes);
            input.read(pool.data(handle), static_cast<std::streamsize>(pool.capacity(handle)));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                DataBufTrailer& trailer = pool.trailer(handle);
                trailer.data_offset = total;
                trailer.data_len = static_cast<std::uint64_t>(bytes);
                PooledFileChunk chunk;
                chunk.offset = total;
                chunk.handle = handle;
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                } else {
                    pool.release(handle);
                }
            } else {
                pool.release(handle);
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::vector<char> scratch;
        if (!copy_payload_to_buffer) {
            scratch.resize(kLargeChunkBytes);
        }
        std::uint64_t total = 0;
        while (input && !(should_stop && should_stop())) {
            std::optional<BufferHandle> acquired;
            while (!(should_stop && should_stop())) {
                acquired = pool.try_acquire();
                if (acquired.has_value()) {
                    break;
                }
                std::this_thread::yield();
            }
            if (!acquired.has_value()) {
                break;
            }
            BufferHandle handle = *acquired;
            DataBuffer& buffer = data_buffer(pool, handle);
            char* read_target = copy_payload_to_buffer ? reinterpret_cast<char*>(buffer.bytes.data())
                                                       : scratch.data();
            const std::size_t read_capacity = copy_payload_to_buffer ? buffer.bytes.size() : scratch.size();
            input.read(read_target, static_cast<std::streamsize>(read_capacity));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                buffer.trailer = {};
                buffer.trailer.data_offset = total;
                buffer.trailer.data_len = static_cast<std::uint64_t>(bytes);
                RawFileChunk chunk;
                chunk.offset = total;
                chunk.handle = handle;
                total += static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                } else {
                    pool.release(handle);
                }
            } else {
                pool.release(handle);
            }
        }
        if (!input.eof() && !(should_stop && should_stop())) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::uint64_t visit_file_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const override {
        (void)declared_size;
        (void)outstanding_requests;
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        std::ifstream input(absolute_path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("failed to open file for reading: " + absolute_path.string());
        }

        std::string buffer(1024U * 1024U, '\0');
        std::uint64_t total = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize bytes = input.gcount();
            if (bytes > 0) {
                const auto bytes_read = static_cast<std::uint64_t>(bytes);
                if (data_visitor) {
                    data_visitor(total, std::string_view(buffer.data(), static_cast<std::size_t>(bytes)));
                }
                total += bytes_read;
            }
        }
        if (!input.eof()) {
            throw std::runtime_error("failed while reading file: " + absolute_path.string());
        }
        return total;
    }

    [[nodiscard]] std::optional<FileSpec> stat_path(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        if (!std::filesystem::exists(absolute_path)) {
            return std::nullopt;
        }
        return local_spec_from_absolute(root_, absolute_path);
    }

    [[nodiscard]] bool metadata_matches(std::string_view rel_path,
                                        std::uint64_t size,
                                        std::uint64_t mtime) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        struct stat info {};
        if (::stat(absolute_path.c_str(), &info) != 0) {
            return false;
        }
        return static_cast<std::uint64_t>(info.st_size) == size && stat_mtime_ns(info) == mtime;
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / normalize_path(rel_path);
        return file_hash64(absolute_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

    [[nodiscard]] std::string description() const override {
        return root_;
    }

private:
    std::filesystem::path root_;
};

bool is_synthetic_profile_url(std::string_view path) {
    return path.rfind("synthetic-profile://", 0) == 0;
}

enum class SyntheticProfileLatencyMode {
    off,
    metadata,
    data,
    all,
};

enum class SyntheticProfilePayloadMode {
    zero,
    prng,
};

struct SyntheticProfileBackendOptions {
    std::filesystem::path profile_path;
    SyntheticProfileLatencyMode latency_mode = SyntheticProfileLatencyMode::off;
    double metadata_latency_scale = 1.0;
    double data_latency_scale = 1.0;
    SyntheticProfilePayloadMode payload_mode = SyntheticProfilePayloadMode::zero;
    std::uint64_t files_per_batch = 4096;
};

bool synthetic_latency_includes_metadata(SyntheticProfileLatencyMode mode) noexcept {
    return mode == SyntheticProfileLatencyMode::metadata || mode == SyntheticProfileLatencyMode::all;
}

bool synthetic_latency_includes_data(SyntheticProfileLatencyMode mode) noexcept {
    return mode == SyntheticProfileLatencyMode::data || mode == SyntheticProfileLatencyMode::all;
}

double synthetic_positive_double_or(std::string_view value, double fallback) {
    if (value.empty()) {
        return fallback;
    }
    const double parsed = std::stod(std::string(value));
    return parsed >= 0.0 && std::isfinite(parsed) ? parsed : fallback;
}

std::uint64_t synthetic_positive_u64_or(std::string_view value, std::uint64_t fallback) {
    if (value.empty()) {
        return fallback;
    }
    const std::uint64_t parsed = std::stoull(std::string(value));
    return parsed == 0U ? fallback : parsed;
}

SyntheticProfileLatencyMode parse_synthetic_latency_mode(std::string_view value) {
    if (value == "off" || value == "false" || value == "0") {
        return SyntheticProfileLatencyMode::off;
    }
    if (value == "metadata" || value == "meta") {
        return SyntheticProfileLatencyMode::metadata;
    }
    if (value == "data" || value == "read" || value == "reads") {
        return SyntheticProfileLatencyMode::data;
    }
    if (value == "all" || value == "true" || value == "1") {
        return SyntheticProfileLatencyMode::all;
    }
    throw std::runtime_error("unknown synthetic-profile latency mode: " + std::string(value));
}

SyntheticProfilePayloadMode parse_synthetic_payload_mode(std::string_view value) {
    if (value == "zero" || value == "zeros" || value == "none" || value == "0") {
        return SyntheticProfilePayloadMode::zero;
    }
    if (value == "prng" || value == "pseudo-random" || value == "pseudo_random" ||
        value == "random" || value == "xorshift64" || value == "fast-random") {
        return SyntheticProfilePayloadMode::prng;
    }
    throw std::runtime_error("unknown synthetic-profile payload mode: " + std::string(value));
}

void parse_synthetic_query_param(SyntheticProfileBackendOptions& options,
                                 std::string_view key,
                                 std::string_view value) {
    if (key == "latency") {
        options.latency_mode = parse_synthetic_latency_mode(value);
    } else if (key == "latency-scale") {
        const double scale = synthetic_positive_double_or(value, 1.0);
        options.metadata_latency_scale = scale;
        options.data_latency_scale = scale;
    } else if (key == "metadata-latency-scale" || key == "meta-latency-scale") {
        options.metadata_latency_scale = synthetic_positive_double_or(value, 1.0);
    } else if (key == "data-latency-scale" || key == "read-latency-scale") {
        options.data_latency_scale = synthetic_positive_double_or(value, 1.0);
    } else if (key == "payload" || key == "data-payload" || key == "payload-pattern" ||
               key == "data-pattern") {
        options.payload_mode = parse_synthetic_payload_mode(value);
    } else if (key == "files-per-batch" || key == "batch-files" || key == "files-per-folder") {
        options.files_per_batch = synthetic_positive_u64_or(value, options.files_per_batch);
    } else if (!key.empty()) {
        throw std::runtime_error("unknown synthetic-profile query parameter: " + std::string(key));
    }
}

SyntheticProfileBackendOptions parse_synthetic_profile_url(std::string_view url) {
    constexpr std::string_view kPrefix = "synthetic-profile://";
    std::string path(url.substr(kPrefix.size()));
    std::string query;
    const std::size_t query_pos = path.find('?');
    if (query_pos != std::string::npos) {
        query = path.substr(query_pos + 1U);
        path.resize(query_pos);
    }
    if (path.rfind("file://", 0) == 0) {
        path.erase(0, std::string("file://").size());
    }
    if (path.empty()) {
        throw std::runtime_error("synthetic-profile URL requires a profile path");
    }
    SyntheticProfileBackendOptions options;
    options.profile_path = std::filesystem::path(path);
    std::size_t begin = 0;
    while (begin < query.size()) {
        const std::size_t end = query.find('&', begin);
        const std::string_view item(query.data() + begin,
                                    end == std::string::npos ? query.size() - begin : end - begin);
        const std::size_t equals = item.find('=');
        parse_synthetic_query_param(options,
                                    equals == std::string_view::npos ? item : item.substr(0, equals),
                                    equals == std::string_view::npos ? std::string_view {} : item.substr(equals + 1U));
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1U;
    }
    return options;
}

std::optional<std::string_view> synthetic_profile_token(std::string_view line,
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

std::uint64_t synthetic_profile_u64(std::string_view line, std::string_view key) {
    const std::optional<std::string_view> token = synthetic_profile_token(line, key);
    if (!token.has_value()) {
        return 0;
    }
    return static_cast<std::uint64_t>(std::stoull(std::string(token.value())));
}

SyntheticLatencyPercentiles synthetic_profile_latency_after(std::string_view line,
                                                            std::string_view prefix) {
    SyntheticLatencyPercentiles latency;
    const std::size_t begin = line.find(prefix);
    if (begin == std::string_view::npos) {
        return latency;
    }
    const std::string_view suffix = line.substr(begin);
    latency.p50_us = synthetic_profile_u64(suffix, "p50_us=");
    latency.p90_us = synthetic_profile_u64(suffix, "p90_us=");
    latency.p99_us = synthetic_profile_u64(suffix, "p99_us=");
    latency.max_us = synthetic_profile_u64(suffix, "max_us=");
    return latency;
}

void parse_synthetic_size_buckets(std::string_view line,
                                  std::array<std::uint64_t, kSyntheticSizeBucketCount>& counts,
                                  std::array<std::uint64_t, kSyntheticSizeBucketCount>& bytes) {
    const std::optional<std::string_view> token = synthetic_profile_token(line, "size_buckets=");
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
            counts[index] = static_cast<std::uint64_t>(std::stoull(item.substr(colon + 1U, slash - colon - 1U)));
            bytes[index] = static_cast<std::uint64_t>(std::stoull(item.substr(slash + 1U)));
        }
        ++index;
    }
}

SyntheticWorkloadProfile load_synthetic_profile_for_backend(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open synthetic profile: " + path.string());
    }

    SyntheticWorkloadProfile profile;
    std::string line;
    while (std::getline(input, line)) {
        if (line.rfind("phase ", 0) != 0) {
            continue;
        }
        SyntheticPhaseProfile phase;
        phase.name = std::string(synthetic_profile_token(line, "name=").value_or("phase"));
        phase.file_count = synthetic_profile_u64(line, "files=");
        phase.folder_count = synthetic_profile_u64(line, "folders=");
        phase.small_file_count = synthetic_profile_u64(line, "small=");
        phase.large_file_count = synthetic_profile_u64(line, "large=");
        phase.logical_size_bytes = synthetic_profile_u64(line, "logical_size_bytes=");
        phase.readdirplus_page_latency =
            synthetic_profile_latency_after(line, "readdirplus_page_latency_p50_us=");
        phase.readdirplus_decode_latency =
            synthetic_profile_latency_after(line, "readdirplus_decode_latency_p50_us=");
        phase.small_read_latency =
            synthetic_profile_latency_after(line, "sampled_small_read_latency_p50_us=");
        phase.large_read_latency =
            synthetic_profile_latency_after(line, "sampled_large_read_latency_p50_us=");
        parse_synthetic_size_buckets(line, phase.size_file_counts, phase.size_logical_bytes);
        profile.phases.push_back(std::move(phase));
    }
    if (profile.phases.empty()) {
        throw std::runtime_error("synthetic profile has no phase records: " + path.string());
    }
    return profile;
}

FileSpec synthetic_file_spec_from_view(const SyntheticFileView& view) {
    FileSpec spec;
    spec.rel_path = std::string(view.path_view());
    spec.declared_size = view.size_bytes;
    spec.mtime = 1'700'000'000'000'000'000ULL + view.file_id;
    spec.mode = 0644;
    spec.uid = static_cast<std::uint32_t>(1000U + (view.file_id % 97U));
    spec.gid = static_cast<std::uint32_t>(1000U + (view.file_id % 89U));
    spec.nfs_handle.assign(view.nfs_handle.begin(), view.nfs_handle.end());
    return spec;
}

std::optional<std::uint64_t> synthetic_batch_index_from_path(std::string_view path) {
    constexpr std::string_view kPrefix = "synthetic/batch_";
    if (path.rfind(kPrefix, 0) != 0) {
        return std::nullopt;
    }
    const std::string_view digits = path.substr(kPrefix.size());
    if (digits.empty() || !std::all_of(digits.begin(), digits.end(), [](char ch) {
            return ch >= '0' && ch <= '9';
        })) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(std::stoull(std::string(digits)));
}

std::optional<std::uint64_t> synthetic_file_index_from_path(std::string_view path) {
    constexpr std::string_view kNeedle = "/file_";
    const std::size_t begin = path.rfind(kNeedle);
    if (begin == std::string_view::npos) {
        return std::nullopt;
    }
    const std::string_view digits = path.substr(begin + kNeedle.size());
    if (digits.empty() || !std::all_of(digits.begin(), digits.end(), [](char ch) {
            return ch >= '0' && ch <= '9';
        })) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(std::stoull(std::string(digits)));
}

std::uint64_t synthetic_profile_file_count(const SyntheticWorkloadProfile& profile) noexcept {
    std::uint64_t total = 0;
    for (const SyntheticPhaseProfile& phase : profile.phases) {
        total += phase.file_count;
    }
    return total;
}

FileSpec synthetic_batch_folder_spec(std::uint64_t batch_index, bool recursive) {
    FileSpec spec;
    spec.rel_path = "synthetic/batch_" + std::to_string(batch_index);
    spec.mode = 0755;
    spec.recursive = recursive;
    return spec;
}

const SyntheticPhaseProfile& synthetic_phase_for_file_index(const SyntheticWorkloadProfile& profile,
                                                            std::uint64_t file_index) {
    std::uint64_t cursor = 0;
    for (const SyntheticPhaseProfile& phase : profile.phases) {
        const std::uint64_t next = cursor + phase.file_count;
        if (file_index < next) {
            return phase;
        }
        cursor = next;
    }
    return profile.phases.back();
}

std::uint64_t sample_synthetic_latency_us(const SyntheticLatencyPercentiles& latency,
                                          std::uint64_t seed) noexcept {
    const std::uint64_t pick = synthetic_splitmix64(seed) % 100U;
    if (pick < 50U) {
        return latency.p50_us;
    }
    if (pick < 90U) {
        return latency.p90_us;
    }
    if (pick < 99U) {
        return latency.p99_us;
    }
    return latency.max_us;
}

void sleep_synthetic_latency(std::uint64_t latency_us, double scale) {
    if (latency_us == 0U || scale <= 0.0) {
        return;
    }
    const auto scaled = static_cast<std::uint64_t>(
        std::llround(static_cast<double>(latency_us) * scale));
    if (scaled != 0U) {
        std::this_thread::sleep_for(std::chrono::microseconds(scaled));
    }
}

class SyntheticProfileBackend final : public NfsBackend {
public:
    explicit SyntheticProfileBackend(std::string profile_url)
        : options_(parse_synthetic_profile_url(profile_url)),
          profile_(load_synthetic_profile_for_backend(options_.profile_path)) {}

    [[nodiscard]] std::vector<FileSpec> list_files(bool recursive) const override {
        std::vector<FileSpec> result;
        visit_files(recursive, [&](FileSpec spec) {
            result.push_back(std::move(spec));
        });
        return result;
    }

    [[nodiscard]] std::vector<FileSpec> list_directories(bool recursive) const override {
        (void)recursive;
        std::vector<FileSpec> result;
        for (std::size_t phase_index = 0; phase_index < profile_.phases.size(); ++phase_index) {
            FileSpec spec;
            spec.rel_path = "synthetic/phase_" + std::to_string(phase_index);
            spec.mode = 0755;
            result.push_back(std::move(spec));
        }
        return result;
    }

    void visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const override {
        (void)recursive;
        SyntheticReplayConfig config;
        config.profile = profile_;
        SyntheticReplayCursor cursor(std::move(config));
        SyntheticFileView view;
        while (cursor.next_file(view)) {
            visitor(synthetic_file_spec_from_view(view));
        }
    }

    void visit_metadata(bool recursive,
                        const std::function<void(FileSpec)>& file_visitor,
                        const std::function<void(FileSpec)>& directory_visitor) const override {
        (void)recursive;
        if (directory_visitor) {
            for (std::size_t phase_index = 0; phase_index < profile_.phases.size(); ++phase_index) {
                FileSpec spec;
                spec.rel_path = "synthetic/phase_" + std::to_string(phase_index);
                spec.mode = 0755;
                directory_visitor(std::move(spec));
            }
        }
        visit_files(recursive, file_visitor);
    }

    void scan_flat_folders(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const override {
        (void)outstanding_folders;
        const std::uint64_t files_per_batch = std::max<std::uint64_t>(1U, options_.files_per_batch);
        const std::uint64_t total_files = synthetic_profile_file_count(profile_);
        if (total_files == 0U) {
            return;
        }
        const std::uint64_t total_batches =
            (total_files + files_per_batch - 1U) / files_per_batch;
        const std::uint64_t lane_count = std::max<std::uint64_t>(1U, outstanding_folders);

        std::uint64_t internal_batch_index = 0;
        while (!(should_stop && should_stop())) {
            std::optional<FileSpec> requested_root;
            if (folder_provider) {
                requested_root = folder_provider(true);
                if (!requested_root.has_value()) {
                    return;
                }
            } else {
                requested_root = synthetic_batch_folder_spec(internal_batch_index++, true);
            }

            const std::optional<std::uint64_t> requested_batch =
                synthetic_batch_index_from_path(requested_root->rel_path);
            if (!requested_batch.has_value() && folder_provider) {
                FlatFolderScanBatch root_batch;
                root_batch.folder.rel_path = "synthetic";
                root_batch.folder.mode = 0755;
                root_batch.folder.recursive = requested_root->recursive;
                root_batch.scan_started_unix_ns = current_unix_time_nanoseconds();
                if (requested_root->recursive) {
                    const std::uint64_t seeded_lanes = std::min<std::uint64_t>(lane_count, total_batches);
                    root_batch.directories.reserve(static_cast<std::size_t>(seeded_lanes));
                    for (std::uint64_t lane = 0; lane < seeded_lanes; ++lane) {
                        root_batch.directories.push_back(synthetic_batch_folder_spec(lane, true));
                    }
                }
                root_batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
                if (!root_batch.directories.empty() && !(should_stop && should_stop())) {
                    folder_visitor(std::move(root_batch));
                }
                continue;
            }
            const std::uint64_t batch_index = requested_batch.value_or(0U);
            const std::uint64_t batch_first_file = batch_index * files_per_batch;
            if (batch_first_file >= total_files) {
                return;
            }

            SyntheticReplayConfig config;
            config.profile = profile_;
            SyntheticReplayCursor cursor(std::move(config));
            cursor.seek_file(batch_first_file);
            SyntheticFileView view;

            const bool recursive = requested_root->recursive;
            FlatFolderScanBatch batch;
            batch.folder = synthetic_batch_folder_spec(batch_index, recursive);
            batch.scan_started_unix_ns = current_unix_time_nanoseconds();
            batch.files.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(
                files_per_batch,
                total_files - batch_first_file)));
            std::uint64_t emitted_files = batch_first_file;
            for (std::uint64_t index = 0;
                 index < files_per_batch && emitted_files < total_files && !(should_stop && should_stop());
                 ++index) {
                if (!cursor.next_file(view)) {
                    break;
                }
                FileSpec file = synthetic_file_spec_from_view(view);
                file.rel_path = batch.folder.rel_path + "/file_" + std::to_string(emitted_files);
                batch.files.push_back(std::move(file));
                ++emitted_files;
            }
            if (batch.files.empty() && batch.directories.empty()) {
                return;
            }
            const std::uint64_t next_batch_index = batch_index + lane_count;
            if (recursive && folder_provider && next_batch_index < total_batches) {
                batch.directories.push_back(synthetic_batch_folder_spec(next_batch_index, recursive));
            }
            if (synthetic_latency_includes_metadata(options_.latency_mode)) {
                const SyntheticPhaseProfile& phase =
                    synthetic_phase_for_file_index(profile_, batch_first_file);
                const std::uint64_t page_latency_us =
                    sample_synthetic_latency_us(phase.readdirplus_page_latency,
                                                profile_.seed ^ (batch_first_file * 131U));
                sleep_synthetic_latency(page_latency_us, options_.metadata_latency_scale);
            }
            batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
            if (!(should_stop && should_stop())) {
                folder_visitor(std::move(batch));
            }

            if (!folder_provider && emitted_files >= total_files) {
                return;
            }
        }
    }

    void scan_flat_folders_streaming(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const override {
        scan_flat_folders(outstanding_folders, folder_provider, should_stop, folder_visitor);
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const override {
        FileSpec spec;
        spec.rel_path = std::string(rel_path);
        spec.declared_size = 1;
        spec.content.assign(1, '\0');
        return spec;
    }

    [[nodiscard]] std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const override {
        (void)outstanding_requests;
        apply_data_latency(rel_path, declared_size);
        std::uint64_t remaining = declared_size;
        while (remaining != 0U) {
            const std::uint64_t chunk = std::min<std::uint64_t>(remaining, kLargeChunkBytes);
            if (bytes_visitor) {
                bytes_visitor(chunk);
            }
            remaining -= chunk;
        }
        return declared_size;
    }

    [[nodiscard]] std::uint64_t read_file_into(std::string_view rel_path,
                                               std::uint64_t declared_size,
                                               std::byte* destination,
                                               std::size_t destination_bytes) const override {
        apply_data_latency(rel_path, declared_size);
        const std::size_t bytes = static_cast<std::size_t>(
            std::min<std::uint64_t>(declared_size, destination_bytes));
        if (bytes != 0U) {
            fill_payload(destination, bytes, rel_path, 0);
        }
        return bytes;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        (void)outstanding_requests;
        apply_data_latency(rel_path, declared_size);
        std::uint64_t total = 0;
        while (total < declared_size && !(should_stop && should_stop())) {
            const std::size_t chunk_size = static_cast<std::size_t>(
                std::min<std::uint64_t>(kLargeChunkBytes, declared_size - total));
            if (should_stop && should_stop()) {
                break;
            }
            std::optional<BufferHandle> acquired;
            while (!(should_stop && should_stop())) {
                acquired = pool.try_acquire();
                if (acquired.has_value()) {
                    break;
                }
                std::this_thread::yield();
            }
            if (!acquired.has_value()) {
                break;
            }
            BufferHandle handle = *acquired;
            if (should_stop && should_stop()) {
                pool.release(handle);
                break;
            }
            DataBuffer& buffer = data_buffer(pool, handle);
            if (copy_payload_to_buffer && chunk_size != 0U) {
                fill_payload(buffer.bytes.data(), chunk_size, rel_path, total);
            }
            buffer.trailer = {};
            buffer.trailer.data_offset = total;
            buffer.trailer.data_len = chunk_size;
            RawFileChunk chunk;
            chunk.offset = total;
            chunk.handle = handle;
            total += chunk_size;
            if (data_visitor) {
                data_visitor(std::move(chunk));
            } else {
                pool.release(handle);
            }
        }
        return total;
    }

    [[nodiscard]] std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const override {
        (void)outstanding_requests;
        apply_data_latency(rel_path, declared_size);
        std::uint64_t total = 0;
        while (total < declared_size) {
            const std::size_t chunk_size = static_cast<std::size_t>(
                std::min<std::uint64_t>(kLargeChunkBytes, declared_size - total));
            DataSlotHandle handle = pool.acquire_wait_or_throw(DataSlotClass::large, chunk_size);
            fill_payload(reinterpret_cast<std::byte*>(pool.data(handle)), chunk_size, rel_path, total);
            DataBufTrailer& trailer = pool.trailer(handle);
            trailer = {};
            trailer.data_offset = total;
            trailer.data_len = chunk_size;

            PooledFileChunk chunk;
            chunk.offset = total;
            chunk.handle = handle;
            total += chunk_size;
            if (data_visitor) {
                data_visitor(std::move(chunk));
            } else {
                pool.release(handle);
            }
        }
        return total;
    }

    [[nodiscard]] std::optional<FileSpec> stat_path(std::string_view rel_path) const override {
        FileSpec spec;
        spec.rel_path = std::string(rel_path);
        return spec;
    }

    [[nodiscard]] bool metadata_matches(std::string_view rel_path,
                                        std::uint64_t size,
                                        std::uint64_t mtime) const override {
        (void)rel_path;
        (void)size;
        (void)mtime;
        return true;
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        return hash64(std::string(rel_path));
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

    [[nodiscard]] std::string description() const override {
        return "synthetic-profile://" + options_.profile_path.string();
    }

private:
    [[nodiscard]] static std::uint64_t next_fast_prng(std::uint64_t& state) noexcept {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        return state * 2685821657736338717ULL;
    }

    void fill_payload(std::byte* destination,
                      std::size_t bytes,
                      std::string_view rel_path,
                      std::uint64_t offset) const {
        if (options_.payload_mode == SyntheticProfilePayloadMode::zero) {
            std::memset(destination, 0, bytes);
            return;
        }

        std::uint64_t state = profile_.seed ^
                              synthetic_splitmix64(hash64(rel_path)) ^
                              synthetic_splitmix64(offset + 0xd1b54a32d192ed03ULL);
        if (state == 0U) {
            state = 0x9e3779b97f4a7c15ULL;
        }

        std::size_t written = 0;
        while (bytes - written >= sizeof(std::uint64_t)) {
            const std::uint64_t word = next_fast_prng(state);
            std::memcpy(destination + written, &word, sizeof(word));
            written += sizeof(word);
        }
        if (written < bytes) {
            const std::uint64_t word = next_fast_prng(state);
            std::memcpy(destination + written, &word, bytes - written);
        }
    }

    void apply_data_latency(std::string_view rel_path, std::uint64_t declared_size) const {
        if (!synthetic_latency_includes_data(options_.latency_mode)) {
            return;
        }
        const std::uint64_t file_index = synthetic_file_index_from_path(rel_path).value_or(0U);
        const SyntheticPhaseProfile& phase = synthetic_phase_for_file_index(profile_, file_index);
        const SyntheticLatencyPercentiles& latency =
            declared_size <= profile_.small_file_threshold_bytes ? phase.small_read_latency
                                                                 : phase.large_read_latency;
        const std::uint64_t latency_us =
            sample_synthetic_latency_us(latency, profile_.seed ^ (file_index * 31U));
        sleep_synthetic_latency(latency_us, options_.data_latency_scale);
    }

    SyntheticProfileBackendOptions options_;
    SyntheticWorkloadProfile profile_;
};

class LocalTargetWriterBackend final : public TargetWriterBackend {
public:
    explicit LocalTargetWriterBackend(std::string root, Options options)
        : root_(std::move(root)), options_(options) {}

    ~LocalTargetWriterBackend() override = default;

    void ensure_directory(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        std::filesystem::create_directories(std::filesystem::path(root_) / rel_path);
    }

    void apply_directory_metadata(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        const std::filesystem::path absolute_path = std::filesystem::path(root_) / rel_path;
        std::filesystem::create_directories(absolute_path);
        if (!options_.preserve_metadata) {
            return;
        }
        FileSpec metadata = spec;
        metadata.rel_path = rel_path;
        hypersync::apply_directory_metadata(absolute_path, metadata);
    }

    void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        ScopedFd& handle = open_handles_[rel_path];
        if (!handle.valid()) {
            const std::filesystem::path absolute_path = std::filesystem::path(root_) / rel_path;
            if (options_.ensure_parent_directories) {
                ensure_parent_directories(absolute_path);
            }
            const int raw_fd =
                ::open(absolute_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, static_cast<mode_t>(spec.mode));
            if (raw_fd < 0) {
                throw std::system_error(errno, std::generic_category(), "open failed for " + absolute_path.string());
            }
            handle.reset(raw_fd);
        }
        local_pwrite_all(handle.get(), data, offset);
    }

    void finish_file(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        auto it = open_handles_.find(rel_path);
        if (it != open_handles_.end()) {
            it->second.reset();
            open_handles_.erase(it);
        }

        if (options_.preserve_metadata) {
            FileSpec metadata = spec;
            metadata.rel_path = rel_path;
            apply_file_metadata(std::filesystem::path(root_) / rel_path, metadata);
        }
    }

    void abort_file(std::string_view rel_path) noexcept override {
        const std::string normalized = normalize_path(rel_path);
        open_handles_.erase(normalized);
        try {
            std::filesystem::remove(std::filesystem::path(root_) / normalized);
        } catch (...) {}
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        return file_hash64(std::filesystem::path(root_) / normalize_path(rel_path));
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

private:
    std::filesystem::path root_;
    Options options_;
    std::unordered_map<std::string, ScopedFd> open_handles_;
};

class NullTargetWriterBackend final : public TargetWriterBackend {
public:
    explicit NullTargetWriterBackend(Options options)
        : options_(options) {}

    ~NullTargetWriterBackend() override = default;

    void ensure_directory(const FileSpec& spec) override {
        (void)spec;
    }

    void apply_directory_metadata(const FileSpec& spec) override {
        (void)spec;
    }

    void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) override {
        (void)spec;
        (void)data;
        (void)offset;
    }

    void write_chunks(const std::vector<WriteChunk>& chunks) override {
        (void)chunks;
    }

    void write_files(const std::vector<WriteChunk>& files) override {
        (void)files;
    }

    void finish_file(const FileSpec& spec) override {
        (void)spec;
    }

    void abort_file(std::string_view rel_path) noexcept override {
        (void)rel_path;
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        return hash64(rel_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return false;
    }

private:
    Options options_;
};

#if HYPERSYNC_HAS_LIBNFS

struct AsyncCommandState {
    bool done = false;
    int status = 0;
    int nfs_status = 0;
    std::size_t byte_count = 0;
    int committed = 0;
    void* data = nullptr;
    std::string error;
    NfsAsyncCommandKind kind = NfsAsyncCommandKind::other;
    std::chrono::steady_clock::time_point queued_at {};
};

struct AsyncStat64State {
    bool done = false;
    int status = 0;
    std::string error;
    struct nfs_stat_64 stat {};
};

struct AsyncDiscardReadState {
    bool done = false;
    int status = 0;
    std::string error;
};

struct AsyncVisitReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::exception_ptr visitor_error;
    std::uint64_t offset = 0;
    const std::function<void(std::uint64_t offset, std::string_view data)>* visitor = nullptr;
};

struct AsyncPooledReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
    DataSlotPool* pool = nullptr;
    DataSlotHandle handle;
};

struct AsyncRawPooledReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
    std::chrono::steady_clock::time_point queued_at {};
    RawBufferPool* pool = nullptr;
    BufferHandle handle;
    bool copy_payload_to_buffer = true;
};

struct AsyncReadIntoState {
    bool done = false;
    int status = 0;
    std::string error;
    std::byte* destination = nullptr;
    std::size_t destination_bytes = 0;
    std::chrono::steady_clock::time_point queued_at {};
};

// libnfs does not expose nfsdir's directory file handle in the public API.
// The first field has been stable in libnfs 5.x and is needed to run raw
// READDIRPLUS so downstream jobs can reuse NFS handles without per-file open().
struct LibNfsPrivateHandle {
    int len = 0;
    char* val = nullptr;
};

struct LibNfsPrivateAttr {
    uint32_t type = 0;
    uint32_t mode = 0;
    uint32_t nlink = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint64_t size = 0;
    uint64_t used = 0;
    struct timeval atime {};
    struct timeval mtime {};
    struct timeval ctime {};
    uint64_t nfsid = 0;
    uint64_t fileid = 0;
};

struct LibNfsPrivateDir {
    LibNfsPrivateHandle fh;
    LibNfsPrivateAttr attr;
};

struct AsyncRawRpcState {
    bool done = false;
    int status = 0;
    void* data = nullptr;
    std::string error;
};

struct RawReaddirplusState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t cookie = 0;
    bool eof = false;
    char cookie_verifier[NFS3_COOKIEVERFSIZE] {};
    FlatFolderScanBatch* batch = nullptr;
    std::string rel_prefix;
    std::string endpoint;
    std::chrono::steady_clock::time_point queued_at {};
    std::size_t requested_bytes = 0;
    std::size_t entries = 0;
    std::size_t files = 0;
    std::size_t directories = 0;
    std::uint64_t page_latency_ns = 0;
    std::uint64_t decode_latency_ns = 0;
};

struct AsyncRawHandleReadState {
    bool done = false;
    int status = 0;
    std::string error;
    std::uint64_t offset = 0;
    std::size_t buffer_offset = 0;
    std::size_t requested = 0;
    std::chrono::steady_clock::time_point queued_at {};
    RawBufferPool* pool = nullptr;
    BufferHandle handle;
    bool copy_payload_to_buffer = true;
};

struct PendingDiscardRead {
    AsyncDiscardReadState state;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
};

struct PendingVisitRead {
    AsyncVisitReadState state;
    std::uint64_t offset = 0;
    std::size_t requested = 0;
};

struct PendingPooledRead {
    AsyncPooledReadState state;
    bool in_use = false;
};

struct PendingRawPooledRead {
    AsyncRawPooledReadState state;
    bool in_use = false;
};

struct PendingRawHandleRead {
    AsyncRawHandleReadState state;
    bool in_use = false;
};

struct PendingPackedSmallFileRead {
    enum class Phase {
        empty,
        opening,
        reading,
        closing,
    };

    Phase phase = Phase::empty;
    FileSpec file;
    std::string remote_path;
    PackedSmallFileAppend append;
    AsyncCommandState open_state;
    AsyncReadIntoState read_state;
    AsyncRawHandleReadState raw_read_state;
    AsyncCommandState close_state;
    struct nfsfh* handle = nullptr;
    std::vector<std::uint8_t> nfs_handle_storage;
    nfs_fh3 raw_nfs_handle {};
    std::uint64_t logical_size = 0;
    std::uint64_t bytes_read = 0;
    bool failed = false;
    bool uses_raw_handle = false;
};

struct PendingRawSmallFileRead {
    enum class Phase {
        empty,
        opening,
        reading,
        closing,
    };

    Phase phase = Phase::empty;
    FileSpec file;
    std::string remote_path;
    BufferHandle handle;
    AsyncCommandState open_state;
    AsyncReadIntoState read_state;
    AsyncRawHandleReadState raw_read_state;
    AsyncCommandState close_state;
    struct nfsfh* nfs_handle = nullptr;
    std::vector<std::uint8_t> nfs_handle_storage;
    nfs_fh3 raw_nfs_handle {};
    std::uint64_t logical_size = 0;
    std::uint64_t bytes_read = 0;
    bool failed = false;
    bool uses_raw_handle = false;
};

struct PendingDirectoryOpen {
    AsyncCommandState state;
    FileSpec folder;
    std::string remote_path;
    std::chrono::steady_clock::time_point queued_at;
    std::uint64_t scan_started_unix_ns = 0;
    std::size_t retry_attempts = 0;
    bool in_use = false;
};

struct RetriedDirectoryOpen {
    FileSpec folder;
    std::size_t retry_attempts = 0;
};

std::uint64_t mtime_from_nfs_fattr3(const fattr3& attr);
std::vector<std::uint8_t> copy_nfs_handle(const nfs_fh3& handle);

void generic_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncCommandState*>(private_data);
    state->done = true;
    state->status = status;
    state->data = data;
    record_async_command_completed(state->kind, state->queued_at, status);
    if (status < 0 && data != nullptr) {
        state->error = static_cast<const char*>(data);
    }
}

void raw_write_callback(struct rpc_context* rpc, int status, void* data, void* private_data) {
    (void)rpc;
    auto* state = static_cast<AsyncCommandState*>(private_data);
    state->status = status;
    state->data = nullptr;
    record_async_command_completed(state->kind, state->queued_at, status);
    if (status != RPC_STATUS_SUCCESS) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        state->done = true;
        return;
    }

    auto* result = static_cast<WRITE3res*>(data);
    if (result == nullptr) {
        state->status = -EIO;
        state->error = "rpc_nfs_write_async completed without a WRITE3 result";
        state->done = true;
        return;
    }

    state->nfs_status = result->status;
    if (result->status == NFS3_OK) {
        state->byte_count = result->WRITE3res_u.resok.count;
        state->committed = result->WRITE3res_u.resok.committed;
    }
    state->done = true;
}

void raw_readdirplus_callback(struct rpc_context* rpc, int status, void* data, void* private_data) {
    (void)rpc;
    const auto callback_started_at = std::chrono::steady_clock::now();
    auto* state = static_cast<RawReaddirplusState*>(private_data);
    state->status = status;
    if (status != RPC_STATUS_SUCCESS) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        const auto callback_finished_at = std::chrono::steady_clock::now();
        record_readdirplus_page_completed(state->queued_at,
                                          callback_started_at,
                                          callback_finished_at,
                                          state->requested_bytes,
                                          state->entries,
                                          state->files,
                                          state->directories,
                                          true);
        state->done = true;
        return;
    }

    auto* result = static_cast<READDIRPLUS3res*>(data);
    if (result == nullptr || result->status != NFS3_OK) {
        state->status = -EIO;
        state->error = result == nullptr ? "NFS READDIRPLUS returned no result"
                                         : "NFS READDIRPLUS failed with status " + std::to_string(result->status);
        const auto callback_finished_at = std::chrono::steady_clock::now();
        record_readdirplus_page_completed(state->queued_at,
                                          callback_started_at,
                                          callback_finished_at,
                                          state->requested_bytes,
                                          state->entries,
                                          state->files,
                                          state->directories,
                                          true);
        state->done = true;
        return;
    }
    if (state->batch == nullptr) {
        state->status = -EIO;
        state->error = "NFS READDIRPLUS callback missing output batch";
        const auto callback_finished_at = std::chrono::steady_clock::now();
        record_readdirplus_page_completed(state->queued_at,
                                          callback_started_at,
                                          callback_finished_at,
                                          state->requested_bytes,
                                          state->entries,
                                          state->files,
                                          state->directories,
                                          true);
        state->done = true;
        return;
    }

    auto& ok = result->READDIRPLUS3res_u.resok;
    std::memcpy(state->cookie_verifier, ok.cookieverf, NFS3_COOKIEVERFSIZE);
    state->eof = ok.reply.eof != 0;
    for (entryplus3* entry = ok.reply.entries; entry != nullptr; entry = entry->nextentry) {
        state->cookie = entry->cookie;
        const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
        if (entry_name.empty() || entry_name == "." || entry_name == "..") {
            continue;
        }
        ++state->entries;
        if (!entry->name_attributes.attributes_follow) {
            continue;
        }

        const fattr3& attr = entry->name_attributes.post_op_attr_u.attributes;
        const std::string rel_path = state->rel_prefix.empty()
                                         ? normalize_path(entry_name)
                                         : normalize_path(state->rel_prefix + "/" + std::string(entry_name));
        if (attr.type == NF3DIR) {
            FileSpec spec;
            spec.rel_path = rel_path;
            spec.mtime = mtime_from_nfs_fattr3(attr);
            spec.mode = static_cast<std::uint32_t>(attr.mode & 0777U);
            spec.uid = static_cast<std::uint32_t>(attr.uid);
            spec.gid = static_cast<std::uint32_t>(attr.gid);
            if (entry->name_handle.handle_follows) {
                spec.nfs_handle = copy_nfs_handle(entry->name_handle.post_op_fh3_u.handle);
            }
            state->batch->directories.push_back(std::move(spec));
            ++state->directories;
            continue;
        }
        if (attr.type != NF3REG) {
            continue;
        }

        FileSpec spec;
        spec.rel_path = rel_path;
        spec.declared_size = attr.size;
        spec.mtime = mtime_from_nfs_fattr3(attr);
        spec.mode = static_cast<std::uint32_t>(attr.mode & 0777U);
        spec.uid = static_cast<std::uint32_t>(attr.uid);
        spec.gid = static_cast<std::uint32_t>(attr.gid);
        if (entry->name_handle.handle_follows) {
            spec.nfs_handle = copy_nfs_handle(entry->name_handle.post_op_fh3_u.handle);
        }
        state->batch->files.push_back(std::move(spec));
        ++state->files;
    }
    const auto callback_finished_at = std::chrono::steady_clock::now();
    state->page_latency_ns = steady_latency_ns(state->queued_at, callback_started_at);
    state->decode_latency_ns = steady_latency_ns(callback_started_at, callback_finished_at);
    state->batch->readdirplus_page_count += 1U;
    state->batch->readdirplus_page_entries += state->entries;
    state->batch->readdirplus_page_requested_bytes += state->requested_bytes;
    state->batch->readdirplus_page_latency_ns += state->page_latency_ns;
    state->batch->readdirplus_decode_latency_ns += state->decode_latency_ns;
    state->batch->readdirplus_page_max_latency_ns =
        std::max(state->batch->readdirplus_page_max_latency_ns, state->page_latency_ns);
    state->batch->readdirplus_decode_max_latency_ns =
        std::max(state->batch->readdirplus_decode_max_latency_ns, state->decode_latency_ns);
    record_readdirplus_page_completed(state->queued_at,
                                      callback_started_at,
                                      callback_finished_at,
                                      state->requested_bytes,
                                      state->entries,
                                      state->files,
                                      state->directories,
                                      false);
    if (nfs_page_trace_enabled()) {
        const double page_latency_ms =
            static_cast<double>(steady_latency_ns(state->queued_at, callback_started_at)) / 1'000'000.0;
        const double decode_latency_ms =
            static_cast<double>(steady_latency_ns(callback_started_at, callback_finished_at)) / 1'000'000.0;
        std::cerr << "nfs_readdirplus_page endpoint=" << state->endpoint
                  << " folder=" << (state->rel_prefix.empty() ? "/" : state->rel_prefix)
                  << " requested_bytes=" << state->requested_bytes
                  << " latency_ms=" << page_latency_ms
                  << " decode_ms=" << decode_latency_ms
                  << " entries=" << state->entries
                  << " files=" << state->files
                  << " directories=" << state->directories
                  << " eof=" << (state->eof ? 1 : 0) << '\n';
    }
    state->done = true;
}

void stat64_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncStat64State*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (data != nullptr) {
        state->stat = *static_cast<const struct nfs_stat_64*>(data);
    }
}

void raw_handle_read_callback(struct rpc_context* rpc, int status, void* data, void* private_data) {
    (void)rpc;
    auto* state = static_cast<AsyncRawHandleReadState*>(private_data);
    if (status != RPC_STATUS_SUCCESS) {
        state->done = true;
        state->status = -EIO;
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        record_async_read_completed(state->queued_at, state->requested, state->status);
        return;
    }

    auto* result = static_cast<READ3res*>(data);
    if (result == nullptr || result->status != NFS3_OK) {
        state->done = true;
        state->status = -EIO;
        state->error = result == nullptr ? "NFS raw READ returned no result"
                                         : "NFS raw READ failed with status " + std::to_string(result->status);
        record_async_read_completed(state->queued_at, state->requested, state->status);
        return;
    }

    const auto bytes_read = static_cast<std::size_t>(result->READ3res_u.resok.count);
    state->status = static_cast<int>(bytes_read);
    record_async_read_completed(state->queued_at, state->requested, state->status);
    if (bytes_read != 0U) {
        if (state->pool == nullptr) {
            state->error = "raw handle read callback missing buffer pool";
            state->status = -EIO;
        } else {
            const char* payload = result->READ3res_u.resok.data.data_val;
            if (state->copy_payload_to_buffer && payload == nullptr) {
                state->error = "raw handle read callback missing payload";
                state->status = -EIO;
            } else {
                DataBuffer& buffer = data_buffer(*state->pool, state->handle);
                if (state->copy_payload_to_buffer) {
                    std::memcpy(buffer.bytes.data() + static_cast<std::ptrdiff_t>(state->buffer_offset),
                                payload,
                                bytes_read);
                }
                buffer.trailer = {};
                buffer.trailer.data_offset = state->offset;
                buffer.trailer.data_len = bytes_read;
            }
        }
    }
    state->done = true;
}

void discard_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncDiscardReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0 && data != nullptr) {
        state->error = static_cast<const char*>(data);
    }
}

void visit_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncVisitReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0 && data != nullptr && state->visitor != nullptr) {
        try {
            // libnfs owns this buffer; the view is valid only for the callback.
            (*state->visitor)(state->offset,
                              std::string_view(static_cast<const char*>(data), static_cast<std::size_t>(status)));
        } catch (...) {
            state->visitor_error = std::current_exception();
        }
    }
}

void pooled_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncPooledReadState*>(private_data);
    state->done = true;
    state->status = status;
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0 && data != nullptr) {
        const auto bytes_read = static_cast<std::size_t>(status);
        if (state->pool == nullptr) {
            state->error = "pooled read callback missing buffer pool";
            state->status = -EIO;
            return;
        }
        std::memcpy(state->pool->data(state->handle), data, bytes_read);
        DataBufTrailer& trailer = state->pool->trailer(state->handle);
        trailer.data_offset = state->offset;
        trailer.data_len = bytes_read;
    }
}

void raw_pooled_read_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncRawPooledReadState*>(private_data);
    state->done = true;
    state->status = status;
    record_async_read_completed(state->queued_at, state->requested, status);
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0) {
        const auto bytes_read = static_cast<std::size_t>(status);
        if (state->pool == nullptr) {
            state->error = "raw pooled read callback missing buffer pool";
            state->status = -EIO;
            return;
        }
        if (state->copy_payload_to_buffer && data == nullptr) {
            state->error = "raw pooled read callback missing data";
            state->status = -EIO;
            return;
        }
        DataBuffer& buffer = data_buffer(*state->pool, state->handle);
        if (state->copy_payload_to_buffer) {
            std::memcpy(buffer.bytes.data(), data, bytes_read);
        }
        buffer.trailer = {};
        buffer.trailer.data_offset = state->offset;
        buffer.trailer.data_len = bytes_read;
    }
}

void read_into_nfs_callback(int status, struct nfs_context* nfs, void* data, void* private_data) {
    (void)nfs;
    auto* state = static_cast<AsyncReadIntoState*>(private_data);
    state->done = true;
    state->status = status;
    record_async_read_completed(state->queued_at, state->destination_bytes, status);
    if (status < 0) {
        if (data != nullptr) {
            state->error = static_cast<const char*>(data);
        }
        return;
    }
    if (status > 0) {
        if (data == nullptr || state->destination == nullptr) {
            state->error = "read-into callback missing data";
            state->status = -EIO;
            return;
        }
        const auto bytes_read = static_cast<std::size_t>(status);
        if (bytes_read > state->destination_bytes) {
            state->error = "read-into callback exceeded destination buffer";
            state->status = -EIO;
            return;
        }
        std::memcpy(state->destination, data, bytes_read);
    }
}

void pump_nfs_until_done(struct nfs_context* nfs, AsyncCommandState& state) {
    while (!state.done) {
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, 100) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            throw std::runtime_error("libnfs service failed: " + std::string(nfs_get_error(nfs)));
        }
    }
}

bool pump_nfs_until_done_until(struct nfs_context* nfs,
                               AsyncCommandState& state,
                               std::chrono::steady_clock::time_point deadline) {
    while (!state.done) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, 100) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            throw std::runtime_error("libnfs service failed: " + std::string(nfs_get_error(nfs)));
        }
    }
    return true;
}

void pump_nfs_until_done(struct nfs_context* nfs, AsyncStat64State& state) {
    while (!state.done) {
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, 100) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            throw std::runtime_error("libnfs service failed: " + std::string(nfs_get_error(nfs)));
        }
    }
}

void service_nfs_context(struct nfs_context* nfs, int timeout_ms) {
    constexpr int kMaxImmediateDrainPasses = 64;

    for (int pass = 0; pass < kMaxImmediateDrainPasses; ++pass) {
        const int fd = nfs_get_fd(nfs);
        const int events = nfs_which_events(nfs);

        struct pollfd descriptor {
            fd, static_cast<short>(events), 0
        };

        const int poll_timeout = pass == 0 ? timeout_ms : 0;
        const int poll_result = fd >= 0 ? ::poll(&descriptor, 1, poll_timeout) : 0;
        if (poll_result < 0) {
            throw std::system_error(errno, std::generic_category(), "poll failed for libnfs context");
        }
        if (poll_result == 0 && pass != 0) {
            break;
        }

        const int revents = poll_result > 0 ? descriptor.revents : 0;
        if (nfs_service(nfs, revents) < 0) {
            const char* error = nfs_get_error(nfs);
            std::ostringstream message;
            message << "libnfs service failed: " << ((error != nullptr && *error != '\0') ? error : "unknown error")
                    << " (fd=" << fd << ", events=" << events << ", revents=" << revents << ")";
            throw std::runtime_error(message.str());
        }

        if (poll_result == 0) {
            break;
        }
    }
}

template <typename StartFn>
AsyncCommandState run_async_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncCommandState state;
    if (operation == "nfs_open_async") {
        state.kind = NfsAsyncCommandKind::open;
    } else if (operation == "nfs_close_async") {
        state.kind = NfsAsyncCommandKind::close;
    }
    state.queued_at = std::chrono::steady_clock::now();
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    if (state.status < 0) {
        throw std::runtime_error(std::string(operation) + " failed: " + state.error);
    }
    return state;
}

template <typename StartFn>
AsyncStat64State run_stat64_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncStat64State state;
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    if (state.status < 0) {
        throw std::runtime_error(std::string(operation) + " failed: " + state.error);
    }
    return state;
}

template <typename StartFn>
AsyncStat64State execute_stat64_command(struct nfs_context* nfs, StartFn&& start_fn, std::string_view operation) {
    AsyncStat64State state;
    const int queue_result = start_fn(&state);
    if (queue_result != 0) {
        throw std::runtime_error(std::string(operation) + " queue failed: " + std::string(nfs_get_error(nfs)));
    }

    pump_nfs_until_done(nfs, state);
    return state;
}

std::uint64_t mtime_from_nfs_stat(const struct nfs_stat_64& stat) {
    return stat.nfs_mtime * 1'000'000'000ULL + stat.nfs_mtime_nsec;
}

std::uint64_t mtime_from_nfs_dirent(const struct nfsdirent& entry) {
    return static_cast<std::uint64_t>(entry.mtime.tv_sec) * 1'000'000'000ULL + entry.mtime_nsec;
}

std::uint64_t mtime_from_nfs_fattr3(const fattr3& attr) {
    return static_cast<std::uint64_t>(attr.mtime.seconds) * 1'000'000'000ULL + attr.mtime.nseconds;
}

std::string join_remote_path(std::string_view base, std::string_view name) {
    if (base.empty() || base == "/") {
        return "/" + std::string(name);
    }
    return std::string(base) + "/" + std::string(name);
}

std::vector<std::uint8_t> copy_nfs_handle(const nfs_fh3& handle) {
    const char* data = handle.data.data_val;
    const auto length = static_cast<std::size_t>(handle.data.data_len);
    if (data == nullptr || length == 0U) {
        return {};
    }
    return std::vector<std::uint8_t>(reinterpret_cast<const std::uint8_t*>(data),
                                     reinterpret_cast<const std::uint8_t*>(data) + length);
}

nfs_fh3 make_raw_nfs_handle(std::vector<std::uint8_t>& handle_storage) {
    nfs_fh3 handle {};
    handle.data.data_len = static_cast<u_int>(handle_storage.size());
    handle.data.data_val = reinterpret_cast<char*>(handle_storage.data());
    return handle;
}

std::optional<struct nfs_stat_64> try_stat64(struct nfs_context* nfs, std::string_view remote_path) {
    const std::string remote_path_string(remote_path);
    const AsyncStat64State state = execute_stat64_command(
        nfs,
        [&](AsyncStat64State* command_state) {
            return nfs_stat64_async(nfs, remote_path_string.c_str(), stat64_nfs_callback, command_state);
        },
        "nfs_stat64_async");

    if (state.status == -ENOENT) {
        return std::nullopt;
    }
    if (state.status < 0) {
        throw std::runtime_error("nfs_stat64_async failed: " + state.error);
    }
    return state.stat;
}

std::uint64_t nfs_stream_hash(struct nfs_context* nfs, const std::string& remote_path) {
    const AsyncStat64State stat_state = run_stat64_command(
        nfs,
        [&](AsyncStat64State* state) {
            return nfs_stat64_async(nfs, remote_path.c_str(), stat64_nfs_callback, state);
        },
        "nfs_stat64_async");
    const std::uint64_t file_size = stat_state.stat.nfs_size;

    const AsyncCommandState open_state = run_async_command(
        nfs,
        [&](AsyncCommandState* state) {
            return nfs_open_async(nfs, remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
        },
        "nfs_open_async");
    auto* handle = static_cast<struct nfsfh*>(open_state.data);
    Hash64State hasher;
    try {
        std::uint64_t offset = 0;
        while (offset < file_size) {
            const std::size_t chunk_bytes =
                std::min<std::size_t>(static_cast<std::size_t>(file_size - offset), 1024U * 1024U);
            std::string buffer(chunk_bytes, '\0');
            const int status = nfs_pread(nfs, handle, offset, chunk_bytes, buffer.data());
            if (status < 0) {
                const char* error = nfs_get_error(nfs);
                throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
            }
            if (status == 0 && chunk_bytes != 0U) {
                throw std::runtime_error("unexpected EOF while hashing NFS file: " + remote_path);
            }
            buffer.resize(static_cast<std::size_t>(status));
            hasher.update(buffer);
            offset += static_cast<std::uint64_t>(status);
        }
        run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_close_async(nfs, handle, generic_nfs_callback, state);
            },
            "nfs_close_async");
    } catch (...) {
        try {
            run_async_command(
                nfs,
                [&](AsyncCommandState* state) {
                    return nfs_close_async(nfs, handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {}
        throw;
    }
    return hasher.value();
}

class LibNfsSession {
public:
    explicit LibNfsSession(const std::string& root_url, std::size_t endpoint_index = kNfsEndpointAny)
        : root_url_(root_url) {
        std::vector<std::string> connection_urls = ordered_nfs_connection_urls(root_url, endpoint_index);
        if (connection_urls.empty()) {
            throw std::runtime_error("NFS URL has no candidate endpoints: " + root_url);
        }

        std::string last_error;
        bool skipped_unhealthy = false;
        for (const std::string& candidate : connection_urls) {
            if (!nfs_endpoint_ready_for_probe(candidate)) {
                skipped_unhealthy = true;
                continue;
            }
            try {
                mount_connection(candidate);
                mark_nfs_endpoint_healthy(candidate);
                return;
            } catch (const std::exception& error) {
                last_error = error.what();
                mark_nfs_endpoint_unhealthy(candidate);
                cleanup_context();
            }
        }

        if (skipped_unhealthy) {
            for (const std::string& candidate : connection_urls) {
                if (nfs_endpoint_ready_for_probe(candidate)) {
                    continue;
                }
                last_error = "all healthy NFS endpoints failed; unhealthy endpoints are cooling down";
                break;
            }
        }
        throw std::runtime_error(last_error.empty() ? "failed to mount any NFS endpoint for " + root_url
                                                    : last_error);
    }

    void mount_connection(const std::string& connection_url) {
        connection_url_ = connection_url;
        nfs_ = nfs_init_context();
        if (nfs_ == nullptr) {
            throw std::runtime_error("failed to initialize libnfs context");
        }

        url_ = nfs_parse_url_dir(nfs_, connection_url_.c_str());
        if (url_ == nullptr) {
            throw std::runtime_error("failed to parse NFS URL: " + connection_url_);
        }
        if (url_->server == nullptr || url_->path == nullptr) {
            throw std::runtime_error("NFS URL must include server and export path: " + connection_url_);
        }

        AsyncCommandState mount_state;
        const int queue_result =
            nfs_mount_async(nfs_, url_->server, url_->path, generic_nfs_callback, &mount_state);
        if (queue_result != 0) {
            const char* error = nfs_get_error(nfs_);
            throw std::runtime_error("nfs_mount_async queue failed: " +
                                     std::string(error != nullptr ? error : "unknown error"));
        }
        constexpr auto kMountTimeout = std::chrono::seconds(30);
        if (!pump_nfs_until_done_until(nfs_, mount_state, std::chrono::steady_clock::now() + kMountTimeout)) {
            const std::string timed_out_url = connection_url_;
            abandon_stuck_context();
            throw std::runtime_error("nfs_mount_async timed out for " + timed_out_url);
        }
        if (mount_state.status < 0) {
            throw std::runtime_error("nfs_mount_async failed: " + mount_state.error);
        }
    }

    ~LibNfsSession() {
        if (url_ != nullptr) {
            nfs_destroy_url(url_);
            url_ = nullptr;
        }
        if (nfs_ != nullptr) {
            nfs_destroy_context(nfs_);
            nfs_ = nullptr;
        }
    }

    LibNfsSession(const LibNfsSession&) = delete;
    LibNfsSession& operator=(const LibNfsSession&) = delete;

    [[nodiscard]] struct nfs_context* context() const {
        return nfs_;
    }

    [[nodiscard]] const std::string& connection_url() const {
        return connection_url_;
    }

    // A timed-out libnfs context may block inside nfs_destroy_context().
    // Recovery deliberately leaks that stuck context and reconnects with a
    // fresh one so one dead NFS endpoint cannot stall the whole pipeline.
    void abandon_stuck_context() noexcept {
        url_ = nullptr;
        nfs_ = nullptr;
    }

private:
    void cleanup_context() noexcept {
        if (url_ != nullptr) {
            nfs_destroy_url(url_);
            url_ = nullptr;
        }
        if (nfs_ != nullptr) {
            nfs_destroy_context(nfs_);
            nfs_ = nullptr;
        }
    }

    std::string root_url_;
    std::string connection_url_;
    struct nfs_context* nfs_ = nullptr;
    struct nfs_url* url_ = nullptr;
};

class LibNfsBackend final : public NfsBackend {
public:
    explicit LibNfsBackend(std::string root_url,
                           std::size_t endpoint_index = kNfsEndpointAny,
                           std::size_t readdirplus_page_bytes = 0)
        : root_url_(std::move(root_url)),
          endpoint_index_(endpoint_index),
          readdirplus_page_bytes_(readdirplus_page_bytes != 0U ? readdirplus_page_bytes : 256U * 1024U) {}

    [[nodiscard]] std::vector<FileSpec> list_files(bool recursive) const override {
        std::vector<FileSpec> files;
        collect_files(session().context(), "/", "", recursive, files);
        std::sort(files.begin(), files.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            return lhs.rel_path < rhs.rel_path;
        });
        return files;
    }

    [[nodiscard]] std::vector<FileSpec> list_directories(bool recursive) const override {
        std::vector<FileSpec> directories;
        collect_directories(session().context(), "/", "", recursive, directories);
        std::sort(directories.begin(), directories.end(), [](const FileSpec& lhs, const FileSpec& rhs) {
            if (path_depth(lhs.rel_path) != path_depth(rhs.rel_path)) {
                return path_depth(lhs.rel_path) < path_depth(rhs.rel_path);
            }
            return lhs.rel_path < rhs.rel_path;
        });
        return directories;
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path) const override {
        return load_file(rel_path, 1);
    }

    [[nodiscard]] FileSpec load_file(std::string_view rel_path, std::size_t outstanding_requests) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();

        const AsyncStat64State stat_state = run_stat64_command(
            active_session.context(),
            [&](AsyncStat64State* state) {
                return nfs_stat64_async(active_session.context(), remote_path.c_str(), stat64_nfs_callback, state);
            },
            "nfs_stat64_async");

        FileSpec spec;
        spec.rel_path = normalized_path;
        spec.declared_size = stat_state.stat.nfs_size;
        spec.mtime = mtime_from_nfs_stat(stat_state.stat);
        spec.mode = static_cast<std::uint32_t>(stat_state.stat.nfs_mode & 0777U);
        spec.uid = static_cast<std::uint32_t>(stat_state.stat.nfs_uid);
        spec.gid = static_cast<std::uint32_t>(stat_state.stat.nfs_gid);

        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        try {
            if (spec.declared_size != 0) {
                if (spec.declared_size > static_cast<std::uint64_t>(std::string().max_size())) {
                    throw std::runtime_error("NFS file is too large for in-memory buffering: " + normalized_path);
                }
                spec.content.assign(static_cast<std::size_t>(spec.declared_size), '\0');
            }

            (void)outstanding_requests;
            std::uint64_t total_read = 0;
            while (total_read < spec.declared_size) {
                const std::uint64_t remaining = spec.declared_size - total_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                const int status = nfs_pread(active_session.context(),
                                             handle,
                                             total_read,
                                             requested,
                                             spec.content.data() + static_cast<std::size_t>(total_read));
                if (status < 0) {
                    const char* error = nfs_get_error(active_session.context());
                    throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
                }
                if (status == 0) {
                    throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                }
                total_read += static_cast<std::uint64_t>(status);
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }

        return spec;
    }

    [[nodiscard]] std::uint64_t read_file_discard(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t)>& bytes_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
            std::vector<PendingDiscardRead> pending(max_in_flight);
            std::vector<bool> in_use(max_in_flight, false);
            std::size_t in_flight = 0;
            std::uint64_t next_offset = 0;

            while (next_offset < declared_size || in_flight != 0) {
                while (next_offset < declared_size && in_flight < max_in_flight) {
                    const auto free_it = std::find(in_use.begin(), in_use.end(), false);
                    if (free_it == in_use.end()) {
                        break;
                    }
                    const std::size_t index = static_cast<std::size_t>(std::distance(in_use.begin(), free_it));
                    PendingDiscardRead& read = pending[index];
                    read = {};
                    read.offset = next_offset;
                    const std::size_t remaining = static_cast<std::size_t>(declared_size - next_offset);
                    read.requested = std::min<std::size_t>(remaining, 1024U * 1024U);

                    const int queue_result = nfs_pread_async(active_session.context(),
                                                             handle,
                                                             read.offset,
                                                             read.requested,
                                                             discard_read_nfs_callback,
                                                             &read.state);
                    if (queue_result != 0) {
                        throw std::runtime_error("nfs_pread_async queue failed: " +
                                                 std::string(nfs_get_error(active_session.context())));
                    }
                    in_use[index] = true;
                    ++in_flight;
                    next_offset += read.requested;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                for (std::size_t index = 0; index < pending.size(); ++index) {
                    if (!in_use[index] || !pending[index].state.done) {
                        continue;
                    }
                    PendingDiscardRead& read = pending[index];
                    if (read.state.status < 0) {
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.requested != 0U) {
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    total_read += bytes_read;
                    if (bytes_read != 0U && bytes_visitor) {
                        bytes_visitor(static_cast<std::uint64_t>(bytes_read));
                    }
                    if (bytes_read < read.requested) {
                        read.offset += bytes_read;
                        read.requested -= bytes_read;
                        read.state = {};
                        const int queue_result = nfs_pread_async(active_session.context(),
                                                                 handle,
                                                                 read.offset,
                                                                 read.requested,
                                                                 discard_read_nfs_callback,
                                                                 &read.state);
                        if (queue_result != 0) {
                            throw std::runtime_error("nfs_pread_async queue failed: " +
                                                     std::string(nfs_get_error(active_session.context())));
                        }
                        continue;
                    }
                    in_use[index] = false;
                    --in_flight;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_stream(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::string_view)>& data_visitor) const override {
        return read_file_owned_chunks(rel_path, declared_size, outstanding_requests, [&](OwnedFileChunk&& chunk) {
            if (data_visitor && !chunk.data.empty()) {
                data_visitor(chunk.data);
            }
        });
    }

    [[nodiscard]] std::uint64_t read_file_into(std::string_view rel_path,
                                               std::uint64_t declared_size,
                                               std::byte* destination,
                                               std::size_t destination_bytes) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size > destination_bytes) {
            throw std::runtime_error("destination buffer is too small for NFS file: " + normalized_path);
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            while (total_read < declared_size) {
                const std::uint64_t remaining = declared_size - total_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                AsyncReadIntoState read_state;
                read_state.destination = destination + total_read;
                read_state.destination_bytes = requested;
                read_state.queued_at = std::chrono::steady_clock::now();
                const int queue_result = nfs_pread_async(active_session.context(),
                                                         handle,
                                                         total_read,
                                                         requested,
                                                         read_into_nfs_callback,
                                                         &read_state);
                if (queue_result != 0) {
                    throw std::runtime_error("nfs_pread_async queue failed: " +
                                             std::string(nfs_get_error(active_session.context())));
                }
                record_async_read_queued(requested);
                while (!read_state.done) {
                    service_nfs_context(active_session.context(), 100);
                }
                if (read_state.status < 0) {
                    throw std::runtime_error("nfs_pread_async failed: " + read_state.error);
                }
                if (read_state.status == 0) {
                    throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                }
                total_read += static_cast<std::uint64_t>(read_state.status);
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    void open_close_file(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
            },
            "nfs_close_async");
    }

    [[nodiscard]] PackedSmallFilesReadStats read_small_files_packed(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        std::size_t max_in_flight_files,
        const std::function<void(BufferHandle, std::uint64_t, std::uint64_t)>& buffer_visitor,
        const std::function<bool()>& should_stop) const override {
        if (pool.pool_id() != kDataBufferPoolId) {
            throw std::runtime_error("packed small-file reader requires the data buffer pool");
        }

        PackedSmallFilesReadStats stats;
        LibNfsSession& active_session = session();
        struct nfs_context* nfs = active_session.context();
        const std::size_t window = std::max<std::size_t>(1, max_in_flight_files);
        std::vector<PendingPackedSmallFileRead> pending(window);
        std::optional<FileSpec> carry_file;
        bool input_done = false;

        const auto logical_size_of = [](const FileSpec& file) {
            return file.declared_size != 0U ? file.declared_size : file.content.size();
        };

        const auto prepare_meta = [&](const FileSpec& file) {
            const RecBuf record = make_recbuf(file);
            PackedSmallFileMeta meta;
            meta.file_id = record.own_hash;
            meta.folder_hash = record.folder_hash;
            meta.file_size = logical_size_of(file);
            meta.mtime = record.mtime;
            meta.mode = record.mode;
            meta.uid = record.uid;
            meta.gid = record.gid;
            meta.rel_path = record.rel_path.view();
            return meta;
        };

        while (!(should_stop && should_stop()) && (!input_done || carry_file.has_value())) {
            BufferHandle batch_handle = pool.acquire_spin();
            DataBuffer& batch_buffer = data_buffer(pool, batch_handle);
            reset_packed_small_file_buffer(batch_buffer);

            std::uint64_t reserved_files = 0;
            std::uint64_t completed_files = 0;
            std::uint64_t completed_bytes = 0;
            std::uint64_t failed_files = 0;
            std::size_t in_flight = 0;
            bool batch_full = false;

            const auto close_handle_after_failure = [&](PendingPackedSmallFileRead& read) {
                read.failed = true;
                if (read.uses_raw_handle || read.handle == nullptr) {
                    read.phase = PendingPackedSmallFileRead::Phase::empty;
                    ++failed_files;
                    --in_flight;
                    return;
                }
                read.close_state = {};
                read.close_state.kind = NfsAsyncCommandKind::close;
                read.close_state.queued_at = std::chrono::steady_clock::now();
                const int close_result = nfs_close_async(nfs, read.handle, generic_nfs_callback, &read.close_state);
                read.handle = nullptr;
                if (close_result != 0) {
                    read.phase = PendingPackedSmallFileRead::Phase::empty;
                    ++failed_files;
                    --in_flight;
                    return;
                }
                read.phase = PendingPackedSmallFileRead::Phase::closing;
            };

            const auto queue_next_opened_read = [&](PendingPackedSmallFileRead& read) {
                const std::uint64_t remaining = read.logical_size - read.bytes_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                read.read_state = {};
                read.read_state.destination = read.append.data + read.bytes_read;
                read.read_state.destination_bytes = requested;
                read.read_state.queued_at = std::chrono::steady_clock::now();
                const int queue_result = nfs_pread_async(nfs,
                                                         read.handle,
                                                         read.bytes_read,
                                                         requested,
                                                         read_into_nfs_callback,
                                                         &read.read_state);
                if (queue_result != 0) {
                    read.read_state.error = nfs_get_error(nfs) != nullptr ? nfs_get_error(nfs) : "unknown error";
                    read.read_state.status = -EIO;
                    read.read_state.done = true;
                } else {
                    record_async_read_queued(requested);
                }
                read.phase = PendingPackedSmallFileRead::Phase::reading;
            };

            const auto queue_next_raw_handle_read = [&](PendingPackedSmallFileRead& read) {
                const std::uint64_t remaining = read.logical_size - read.bytes_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                read.raw_read_state = {};
                read.raw_read_state.offset = read.bytes_read;
                read.raw_read_state.buffer_offset =
                    static_cast<std::size_t>(read.append.data - batch_buffer.bytes.data()) +
                    static_cast<std::size_t>(read.bytes_read);
                read.raw_read_state.requested = requested;
                read.raw_read_state.pool = &pool;
                read.raw_read_state.handle = batch_handle;
                read.raw_read_state.copy_payload_to_buffer = true;
                read.raw_read_state.queued_at = std::chrono::steady_clock::now();
                struct rpc_context* rpc = nfs_get_rpc_context(nfs);
                const int queue_result = rpc_nfs_read_async(rpc,
                                                            raw_handle_read_callback,
                                                            &read.raw_nfs_handle,
                                                            read.bytes_read,
                                                            requested,
                                                            &read.raw_read_state);
                if (queue_result != 0) {
                    read.raw_read_state.error = rpc_get_error(rpc) != nullptr ? rpc_get_error(rpc) : "unknown error";
                    read.raw_read_state.status = -EIO;
                    read.raw_read_state.done = true;
                } else {
                    record_async_read_queued(requested);
                }
                read.phase = PendingPackedSmallFileRead::Phase::reading;
            };

            const auto try_schedule = [&]() {
                while (in_flight < window && !batch_full && !(should_stop && should_stop())) {
                    auto slot = std::find_if(pending.begin(), pending.end(), [](const PendingPackedSmallFileRead& read) {
                        return read.phase == PendingPackedSmallFileRead::Phase::empty;
                    });
                    if (slot == pending.end()) {
                        return;
                    }

                    std::optional<FileSpec> next_file;
                    if (carry_file.has_value()) {
                        next_file = std::move(carry_file);
                        carry_file.reset();
                    } else if (file_provider) {
                        next_file = file_provider();
                    }
                    if (!next_file.has_value()) {
                        input_done = true;
                        return;
                    }

                    const std::uint64_t logical_size = logical_size_of(*next_file);
                    PackedSmallFileAppend append;
                    if (!prepare_packed_small_file_append(batch_buffer,
                                                          prepare_meta(*next_file),
                                                          static_cast<std::size_t>(logical_size),
                                                          append)) {
                        if (reserved_files == 0U) {
                            ++stats.files_failed;
                            continue;
                        }
                        carry_file = std::move(*next_file);
                        batch_full = true;
                        return;
                    }

                    commit_packed_small_file_append(batch_buffer, append);
                    ++reserved_files;

                    PendingPackedSmallFileRead& read = *slot;
                    read = {};
                    read.file = std::move(*next_file);
                    read.logical_size = logical_size;
                    read.append = append;
                    if (logical_size == 0U) {
                        ++completed_files;
                        continue;
                    }

                    if (!read.file.nfs_handle.empty()) {
                        read.uses_raw_handle = true;
                        read.nfs_handle_storage = read.file.nfs_handle;
                        read.raw_nfs_handle = make_raw_nfs_handle(read.nfs_handle_storage);
                        queue_next_raw_handle_read(read);
                        ++in_flight;
                        continue;
                    }

                    const std::string normalized_path = normalize_path(read.file.rel_path);
                    if (normalized_path.empty()) {
                        ++failed_files;
                        continue;
                    }
                    read.remote_path = "/" + normalized_path;
                    read.open_state = {};
                    read.open_state.kind = NfsAsyncCommandKind::open;
                    read.open_state.queued_at = std::chrono::steady_clock::now();
                    const int open_result =
                        nfs_open_async(nfs,
                                       read.remote_path.c_str(),
                                       O_RDONLY,
                                       generic_nfs_callback,
                                       &read.open_state);
                    if (open_result != 0) {
                        ++failed_files;
                        continue;
                    }
                    read.phase = PendingPackedSmallFileRead::Phase::opening;
                    ++in_flight;
                }
            };

            try_schedule();
            while (in_flight != 0U) {
                service_nfs_context(nfs, 100);

                for (PendingPackedSmallFileRead& read : pending) {
                    if (read.phase == PendingPackedSmallFileRead::Phase::opening && read.open_state.done) {
                        if (read.open_state.status < 0) {
                            ++failed_files;
                            read.phase = PendingPackedSmallFileRead::Phase::empty;
                            --in_flight;
                            continue;
                        }
                        read.handle = static_cast<struct nfsfh*>(read.open_state.data);
                        queue_next_opened_read(read);
                        continue;
                    }

                    if (read.phase == PendingPackedSmallFileRead::Phase::reading &&
                        ((read.uses_raw_handle && read.raw_read_state.done) ||
                         (!read.uses_raw_handle && read.read_state.done))) {
                        const int read_status = read.uses_raw_handle
                                                    ? read.raw_read_state.status
                                                    : read.read_state.status;
                        if (read_status < 0) {
                            close_handle_after_failure(read);
                            continue;
                        }
                        if (read_status == 0) {
                            close_handle_after_failure(read);
                            continue;
                        }

                        read.bytes_read += static_cast<std::uint64_t>(read_status);
                        if (read.bytes_read < read.logical_size) {
                            if (read.uses_raw_handle) {
                                queue_next_raw_handle_read(read);
                            } else {
                                queue_next_opened_read(read);
                            }
                            continue;
                        }

                        if (read.uses_raw_handle) {
                            ++completed_files;
                            completed_bytes += read.bytes_read;
                            read = {};
                            --in_flight;
                            continue;
                        }

                        read.close_state = {};
                        read.close_state.kind = NfsAsyncCommandKind::close;
                        read.close_state.queued_at = std::chrono::steady_clock::now();
                        const int close_result = nfs_close_async(nfs,
                                                                 read.handle,
                                                                 generic_nfs_callback,
                                                                 &read.close_state);
                        read.handle = nullptr;
                        if (close_result != 0) {
                            ++failed_files;
                            read.phase = PendingPackedSmallFileRead::Phase::empty;
                            --in_flight;
                            continue;
                        }
                        read.phase = PendingPackedSmallFileRead::Phase::closing;
                        continue;
                    }

                    if (read.phase == PendingPackedSmallFileRead::Phase::closing && read.close_state.done) {
                        if (read.failed || read.close_state.status < 0) {
                            ++failed_files;
                        } else {
                            ++completed_files;
                            completed_bytes += read.bytes_read;
                        }
                        read.phase = PendingPackedSmallFileRead::Phase::empty;
                        --in_flight;
                    }
                }
                try_schedule();
            }

            if (reserved_files == 0U) {
                pool.release(batch_handle);
                if (input_done && !carry_file.has_value()) {
                    break;
                }
                continue;
            }

            if (failed_files != 0U || completed_files != reserved_files) {
                pool.release(batch_handle);
                stats.files_failed += reserved_files;
            } else {
                if (buffer_visitor) {
                    buffer_visitor(batch_handle, completed_files, completed_bytes);
                } else {
                    pool.release(batch_handle);
                }
                ++stats.buffers_published;
                stats.files_read += completed_files;
                stats.bytes_read += completed_bytes;
            }

            if (input_done && !carry_file.has_value()) {
                break;
            }
        }

        return stats;
    }

    [[nodiscard]] PackedSmallFilesReadStats read_small_files_raw_window(
        const std::function<std::optional<FileSpec>()>& file_provider,
        RawBufferPool& pool,
        std::size_t max_in_flight_files,
        const std::function<void(RawSmallFileRead&&)>& file_visitor,
        const std::function<bool()>& should_stop) const override {
        if (pool.pool_id() != kDataBufferPoolId) {
            throw std::runtime_error("raw small-file reader requires the data buffer pool");
        }

        PackedSmallFilesReadStats stats;
        LibNfsSession& active_session = session();
        struct nfs_context* nfs = active_session.context();
        const std::size_t window = std::max<std::size_t>(1, max_in_flight_files);
        std::vector<PendingRawSmallFileRead> pending(window);
        bool input_done = false;
        std::size_t in_flight = 0;

        const auto logical_size_of = [](const FileSpec& file) {
            return file.declared_size != 0U ? file.declared_size : file.content.size();
        };

        const auto release_pending = [&](PendingRawSmallFileRead& read) noexcept {
            if (read.handle.pool_id != 0U) {
                try {
                    pool.release(read.handle);
                } catch (...) {
                }
            }
            if (read.nfs_handle != nullptr) {
                try {
                    read.close_state = {};
                    read.close_state.kind = NfsAsyncCommandKind::close;
                    read.close_state.queued_at = std::chrono::steady_clock::now();
                    if (nfs_close_async(nfs, read.nfs_handle, generic_nfs_callback, &read.close_state) == 0) {
                        while (!read.close_state.done) {
                            service_nfs_context(nfs, 100);
                        }
                    }
                } catch (...) {
                }
            }
            read = {};
        };

        const auto close_after_failure = [&](PendingRawSmallFileRead& read) {
            read.failed = true;
            if (read.uses_raw_handle || read.nfs_handle == nullptr) {
                pool.release(read.handle);
                read = {};
                --in_flight;
                ++stats.files_failed;
                return;
            }
            read.close_state = {};
            read.close_state.kind = NfsAsyncCommandKind::close;
            read.close_state.queued_at = std::chrono::steady_clock::now();
            const int close_result = nfs_close_async(nfs, read.nfs_handle, generic_nfs_callback, &read.close_state);
            read.nfs_handle = nullptr;
            if (close_result != 0) {
                pool.release(read.handle);
                read = {};
                --in_flight;
                ++stats.files_failed;
                return;
            }
            read.phase = PendingRawSmallFileRead::Phase::closing;
        };

        const auto queue_next_opened_read = [&](PendingRawSmallFileRead& read) {
            const std::uint64_t remaining = read.logical_size - read.bytes_read;
            const std::size_t requested =
                static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes - read.bytes_read));
            DataBuffer& buffer = data_buffer(pool, read.handle);
            read.read_state = {};
            read.read_state.destination = buffer.bytes.data() + static_cast<std::ptrdiff_t>(read.bytes_read);
            read.read_state.destination_bytes = requested;
            read.read_state.queued_at = std::chrono::steady_clock::now();
            const int queue_result = nfs_pread_async(nfs,
                                                     read.nfs_handle,
                                                     read.bytes_read,
                                                     requested,
                                                     read_into_nfs_callback,
                                                     &read.read_state);
            if (queue_result != 0) {
                read.read_state.error = nfs_get_error(nfs) != nullptr ? nfs_get_error(nfs) : "unknown error";
                read.read_state.status = -EIO;
                read.read_state.done = true;
            } else {
                record_async_read_queued(requested);
            }
            read.phase = PendingRawSmallFileRead::Phase::reading;
        };

        const auto queue_next_raw_handle_read = [&](PendingRawSmallFileRead& read) {
            const std::uint64_t remaining = read.logical_size - read.bytes_read;
            const std::size_t requested =
                static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes - read.bytes_read));
            read.raw_read_state = {};
            read.raw_read_state.offset = read.bytes_read;
            read.raw_read_state.buffer_offset = static_cast<std::size_t>(read.bytes_read);
            read.raw_read_state.requested = requested;
            read.raw_read_state.pool = &pool;
            read.raw_read_state.handle = read.handle;
            read.raw_read_state.copy_payload_to_buffer = true;
            read.raw_read_state.queued_at = std::chrono::steady_clock::now();
            struct rpc_context* rpc = nfs_get_rpc_context(nfs);
            const int queue_result = rpc_nfs_read_async(rpc,
                                                        raw_handle_read_callback,
                                                        &read.raw_nfs_handle,
                                                        read.bytes_read,
                                                        requested,
                                                        &read.raw_read_state);
            if (queue_result != 0) {
                read.raw_read_state.error = rpc_get_error(rpc) != nullptr ? rpc_get_error(rpc) : "unknown error";
                read.raw_read_state.status = -EIO;
                read.raw_read_state.done = true;
            } else {
                record_async_read_queued(requested);
            }
            read.phase = PendingRawSmallFileRead::Phase::reading;
        };

        const auto try_schedule = [&]() {
            while (in_flight < window && !input_done && !(should_stop && should_stop())) {
                auto slot = std::find_if(pending.begin(), pending.end(), [](const PendingRawSmallFileRead& read) {
                    return read.phase == PendingRawSmallFileRead::Phase::empty;
                });
                if (slot == pending.end()) {
                    return;
                }

                std::optional<FileSpec> next_file = file_provider ? file_provider() : std::nullopt;
                if (!next_file.has_value()) {
                    input_done = true;
                    return;
                }

                const std::uint64_t logical_size = logical_size_of(*next_file);
                if (logical_size == 0U) {
                    ++stats.files_read;
                    continue;
                }
                if (logical_size > kLargeChunkBytes) {
                    ++stats.files_failed;
                    continue;
                }

                BufferHandle handle = pool.acquire_spin();
                DataBuffer& buffer = data_buffer(pool, handle);
                buffer.trailer = {};

                PendingRawSmallFileRead& read = *slot;
                read = {};
                read.file = std::move(*next_file);
                read.logical_size = logical_size;
                read.handle = handle;

                if (!read.file.nfs_handle.empty()) {
                    read.uses_raw_handle = true;
                    read.nfs_handle_storage = read.file.nfs_handle;
                    read.raw_nfs_handle = make_raw_nfs_handle(read.nfs_handle_storage);
                    queue_next_raw_handle_read(read);
                    ++in_flight;
                    continue;
                }

                const std::string normalized_path = normalize_path(read.file.rel_path);
                if (normalized_path.empty()) {
                    pool.release(read.handle);
                    read = {};
                    ++stats.files_failed;
                    continue;
                }
                read.remote_path = "/" + normalized_path;
                read.open_state = {};
                read.open_state.kind = NfsAsyncCommandKind::open;
                read.open_state.queued_at = std::chrono::steady_clock::now();
                const int open_result =
                    nfs_open_async(nfs,
                                   read.remote_path.c_str(),
                                   O_RDONLY,
                                   generic_nfs_callback,
                                   &read.open_state);
                if (open_result != 0) {
                    pool.release(read.handle);
                    read = {};
                    ++stats.files_failed;
                    continue;
                }
                read.phase = PendingRawSmallFileRead::Phase::opening;
                ++in_flight;
            }
        };

        try {
            try_schedule();
            while (in_flight != 0U || (!input_done && !(should_stop && should_stop()))) {
                service_nfs_context(nfs, in_flight == 0U ? 0 : 100);

                for (PendingRawSmallFileRead& read : pending) {
                    if (read.phase == PendingRawSmallFileRead::Phase::opening && read.open_state.done) {
                        if (read.open_state.status < 0) {
                            pool.release(read.handle);
                            read = {};
                            --in_flight;
                            ++stats.files_failed;
                            continue;
                        }
                        read.nfs_handle = static_cast<struct nfsfh*>(read.open_state.data);
                        queue_next_opened_read(read);
                        continue;
                    }

                    if (read.phase == PendingRawSmallFileRead::Phase::reading &&
                        ((read.uses_raw_handle && read.raw_read_state.done) ||
                         (!read.uses_raw_handle && read.read_state.done))) {
                        const int read_status = read.uses_raw_handle
                                                    ? read.raw_read_state.status
                                                    : read.read_state.status;
                        if (read_status < 0 || read_status == 0) {
                            close_after_failure(read);
                            continue;
                        }

                        read.bytes_read += static_cast<std::uint64_t>(read_status);
                        if (read.bytes_read < read.logical_size) {
                            if (read.uses_raw_handle) {
                                queue_next_raw_handle_read(read);
                            } else {
                                queue_next_opened_read(read);
                            }
                            continue;
                        }

                        if (read.uses_raw_handle) {
                            const BufferHandle completed_handle = read.handle;
                            const std::uint64_t completed_bytes = read.bytes_read;
                            FileSpec completed_file = std::move(read.file);
                            read = {};
                            --in_flight;

                            DataBuffer& buffer = data_buffer(pool, completed_handle);
                            buffer.trailer = {};
                            buffer.trailer.data_offset = 0;
                            buffer.trailer.data_len = static_cast<std::size_t>(completed_bytes);

                            if (file_visitor) {
                                RawSmallFileRead completed;
                                completed.file = std::move(completed_file);
                                completed.handle = completed_handle;
                                completed.bytes_read = completed_bytes;
                                file_visitor(std::move(completed));
                            } else {
                                pool.release(completed_handle);
                            }
                            ++stats.files_read;
                            ++stats.buffers_published;
                            stats.bytes_read += completed_bytes;
                            continue;
                        }

                        read.close_state = {};
                        read.close_state.kind = NfsAsyncCommandKind::close;
                        read.close_state.queued_at = std::chrono::steady_clock::now();
                        const int close_result = nfs_close_async(nfs,
                                                                 read.nfs_handle,
                                                                 generic_nfs_callback,
                                                                 &read.close_state);
                        read.nfs_handle = nullptr;
                        if (close_result != 0) {
                            pool.release(read.handle);
                            read = {};
                            --in_flight;
                            ++stats.files_failed;
                            continue;
                        }
                        read.phase = PendingRawSmallFileRead::Phase::closing;
                        continue;
                    }

                    if (read.phase == PendingRawSmallFileRead::Phase::closing && read.close_state.done) {
                        const BufferHandle completed_handle = read.handle;
                        const std::uint64_t completed_bytes = read.bytes_read;
                        FileSpec completed_file = std::move(read.file);
                        const bool failed = read.failed || read.close_state.status < 0;
                        read = {};
                        --in_flight;

                        if (failed) {
                            pool.release(completed_handle);
                            ++stats.files_failed;
                            continue;
                        }

                        DataBuffer& buffer = data_buffer(pool, completed_handle);
                        buffer.trailer = {};
                        buffer.trailer.data_offset = 0;
                        buffer.trailer.data_len = static_cast<std::size_t>(completed_bytes);

                        if (file_visitor) {
                            RawSmallFileRead completed;
                            completed.file = std::move(completed_file);
                            completed.handle = completed_handle;
                            completed.bytes_read = completed_bytes;
                            file_visitor(std::move(completed));
                        } else {
                            pool.release(completed_handle);
                        }
                        ++stats.files_read;
                        ++stats.buffers_published;
                        stats.bytes_read += completed_bytes;
                    }
                }

                try_schedule();
            }
        } catch (...) {
            for (PendingRawSmallFileRead& read : pending) {
                if (read.phase != PendingRawSmallFileRead::Phase::empty) {
                    release_pending(read);
                }
            }
            throw;
        }

        return stats;
    }

    [[nodiscard]] std::uint64_t read_file_owned_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(OwnedFileChunk&&)>& data_visitor) const override {
        (void)outstanding_requests;
        // libnfs async pread returns a borrowed callback buffer, so the owned-buffer
        // path uses sync pread into application-owned storage.
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            while (total_read < declared_size) {
                OwnedFileChunk chunk;
                chunk.offset = total_read;
                const std::uint64_t remaining = declared_size - total_read;
                const std::size_t requested =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining, 1024U * 1024U));
                chunk.data.assign(requested, '\0');

                const int status =
                    nfs_pread(active_session.context(), handle, chunk.offset, requested, chunk.data.data());
                if (status < 0) {
                    const char* error = nfs_get_error(active_session.context());
                    throw std::runtime_error("nfs_pread failed: " + std::string(error != nullptr ? error : "unknown error"));
                }
                if (status == 0) {
                    throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                }

                chunk.data.resize(static_cast<std::size_t>(status));
                total_read += static_cast<std::uint64_t>(status);
                if (data_visitor) {
                    data_visitor(std::move(chunk));
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_pooled_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        DataSlotPool& pool,
        const std::function<void(PooledFileChunk&&)>& data_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
        std::vector<PendingPooledRead> pending(max_in_flight);
        std::size_t in_flight = 0;
        const auto drain_pending_reads = [&]() noexcept {
            while (in_flight != 0U) {
                try {
                    service_nfs_context(active_session.context(), 100);
                } catch (...) {
                    return;
                }
                for (auto& read : pending) {
                    if (!read.in_use || !read.state.done) {
                        continue;
                    }
                    try {
                        pool.release(read.state.handle);
                    } catch (...) {
                    }
                    read.in_use = false;
                    --in_flight;
                }
            }
        };
        try {
            std::uint64_t next_offset = 0;
            std::uint64_t next_emit_offset = 0;

            const auto queue_read = [&](PendingPooledRead& read,
                                        std::uint64_t offset,
                                        std::size_t requested) {
                DataSlotHandle slot = pool.acquire_wait_or_throw(DataSlotClass::large, requested);
                read.state = {};
                read.state.offset = offset;
                read.state.requested = requested;
                read.state.pool = &pool;
                read.state.handle = slot;
                pool.trailer(slot) = {};
                const int queue_result = nfs_pread_async(active_session.context(),
                                                         handle,
                                                         offset,
                                                         requested,
                                                         pooled_read_nfs_callback,
                                                         &read.state);
                if (queue_result != 0) {
                    pool.release(slot);
                    throw std::runtime_error("nfs_pread_async queue failed: " +
                                             std::string(nfs_get_error(active_session.context())));
                }
                read.in_use = true;
            };

            while (next_offset < declared_size || in_flight != 0) {
                for (auto& read : pending) {
                    if (next_offset >= declared_size || in_flight >= max_in_flight) {
                        break;
                    }
                    if (read.in_use) {
                        continue;
                    }
                    const std::uint64_t remaining = declared_size - next_offset;
                    const std::size_t requested =
                        static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes));
                    queue_read(read, next_offset, requested);
                    next_offset += requested;
                    ++in_flight;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                bool drained_ready_read = true;
                while (drained_ready_read) {
                    drained_ready_read = false;
                    auto ready_read = std::find_if(pending.begin(), pending.end(), [&](const PendingPooledRead& read) {
                        return read.in_use && read.state.done && read.state.offset == next_emit_offset;
                    });
                    if (ready_read == pending.end()) {
                        break;
                    }

                    PendingPooledRead& read = *ready_read;
                    if (read.state.status < 0) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.state.requested != 0U) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    const std::uint64_t chunk_offset = read.state.offset;
                    const std::size_t requested = read.state.requested;
                    const DataSlotHandle handle_for_visitor = read.state.handle;
                    total_read += bytes_read;
                    next_emit_offset += bytes_read;

                    read.in_use = false;
                    if (bytes_read != 0U) {
                        PooledFileChunk chunk;
                        chunk.offset = chunk_offset;
                        chunk.handle = handle_for_visitor;
                        if (data_visitor) {
                            data_visitor(std::move(chunk));
                        } else {
                            pool.release(handle_for_visitor);
                        }
                    } else {
                        pool.release(handle_for_visitor);
                    }

                    if (bytes_read < requested) {
                        const std::uint64_t retry_offset = chunk_offset + bytes_read;
                        const std::size_t retry_bytes = requested - bytes_read;
                        queue_read(read, retry_offset, retry_bytes);
                    } else {
                        --in_flight;
                    }
                    drained_ready_read = true;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            drain_pending_reads();
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (pool.pool_id() != kDataBufferPoolId) {
            throw std::runtime_error("raw NFS data reader requires the data buffer pool");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
        std::vector<PendingRawPooledRead> pending(max_in_flight);
        std::size_t in_flight = 0;
        const auto drain_pending_reads = [&]() noexcept {
            while (in_flight != 0U) {
                try {
                    service_nfs_context(active_session.context(), 100);
                } catch (...) {
                    return;
                }
                for (auto& read : pending) {
                    if (!read.in_use || !read.state.done) {
                        continue;
                    }
                    try {
                        pool.release(read.state.handle);
                    } catch (...) {
                    }
                    read.in_use = false;
                    --in_flight;
                }
            }
        };
        try {
            std::uint64_t next_offset = 0;
            std::uint64_t next_emit_offset = 0;

            const auto queue_read = [&](PendingRawPooledRead& read,
                                        std::uint64_t offset,
                                        std::size_t requested) {
                BufferHandle slot = pool.acquire_spin();
                DataBuffer& buffer = data_buffer(pool, slot);
                buffer.trailer = {};
                read.state = {};
                read.state.offset = offset;
                read.state.requested = requested;
                read.state.pool = &pool;
                read.state.handle = slot;
                read.state.copy_payload_to_buffer = copy_payload_to_buffer;
                read.state.queued_at = std::chrono::steady_clock::now();
                const int queue_result = nfs_pread_async(active_session.context(),
                                                         handle,
                                                         offset,
                                                         requested,
                                                         raw_pooled_read_nfs_callback,
                                                         &read.state);
                if (queue_result != 0) {
                    pool.release(slot);
                    throw std::runtime_error("nfs_pread_async queue failed: " +
                                             std::string(nfs_get_error(active_session.context())));
                }
                record_async_read_queued(requested);
                read.in_use = true;
            };

            while ((next_offset < declared_size && !(should_stop && should_stop())) || in_flight != 0) {
                for (auto& read : pending) {
                    if (next_offset >= declared_size ||
                        in_flight >= max_in_flight ||
                        (should_stop && should_stop())) {
                        break;
                    }
                    if (read.in_use) {
                        continue;
                    }
                    const std::uint64_t remaining = declared_size - next_offset;
                    const std::size_t requested =
                        static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes));
                    queue_read(read, next_offset, requested);
                    next_offset += requested;
                    ++in_flight;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                bool drained_ready_read = true;
                while (drained_ready_read) {
                    drained_ready_read = false;
                    auto ready_read =
                        std::find_if(pending.begin(), pending.end(), [&](const PendingRawPooledRead& read) {
                            return read.in_use && read.state.done && read.state.offset == next_emit_offset;
                        });
                    if (ready_read == pending.end()) {
                        break;
                    }

                    PendingRawPooledRead& read = *ready_read;
                    if (read.state.status < 0) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.state.requested != 0U) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    const std::uint64_t chunk_offset = read.state.offset;
                    const std::size_t requested = read.state.requested;
                    const BufferHandle handle_for_visitor = read.state.handle;
                    total_read += bytes_read;
                    next_emit_offset += bytes_read;

                    read.in_use = false;
                    if (bytes_read != 0U) {
                        RawFileChunk chunk;
                        chunk.offset = chunk_offset;
                        chunk.handle = handle_for_visitor;
                        if (data_visitor) {
                            data_visitor(std::move(chunk));
                        } else {
                            pool.release(handle_for_visitor);
                        }
                    } else {
                        pool.release(handle_for_visitor);
                    }

                    if (bytes_read < requested) {
                        const std::uint64_t retry_offset = chunk_offset + bytes_read;
                        const std::size_t retry_bytes = requested - bytes_read;
                        queue_read(read, retry_offset, retry_bytes);
                    } else {
                        --in_flight;
                    }
                    drained_ready_read = true;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            drain_pending_reads();
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t read_file_raw_chunks_by_handle(
        const FileSpec& file,
        std::size_t outstanding_requests,
        RawBufferPool& pool,
        const std::function<void(RawFileChunk&&)>& data_visitor,
        const std::function<bool()>& should_stop,
        bool copy_payload_to_buffer) const override {
        if (file.nfs_handle.empty()) {
            return read_file_raw_chunks(file.rel_path,
                                        file.declared_size,
                                        outstanding_requests,
                                        pool,
                                        data_visitor,
                                        should_stop,
                                        copy_payload_to_buffer);
        }
        if (pool.pool_id() != kDataBufferPoolId) {
            throw std::runtime_error("raw NFS handle reader requires the data buffer pool");
        }
        if (file.declared_size == 0U) {
            return 0;
        }

        LibNfsSession& active_session = session();
        struct nfs_context* nfs = active_session.context();
        struct rpc_context* rpc = nfs_get_rpc_context(nfs);
        std::vector<std::uint8_t> handle_storage = file.nfs_handle;
        nfs_fh3 raw_handle = make_raw_nfs_handle(handle_storage);
        std::uint64_t total_read = 0;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
        std::vector<PendingRawHandleRead> pending(max_in_flight);
        std::size_t in_flight = 0;

        const auto drain_pending_reads = [&]() noexcept {
            while (in_flight != 0U) {
                try {
                    service_nfs_context(nfs, 100);
                } catch (...) {
                    return;
                }
                for (auto& read : pending) {
                    if (!read.in_use || !read.state.done) {
                        continue;
                    }
                    try {
                        pool.release(read.state.handle);
                    } catch (...) {
                    }
                    read.in_use = false;
                    --in_flight;
                }
            }
        };

        try {
            std::uint64_t next_offset = 0;
            std::uint64_t next_emit_offset = 0;

            const auto queue_read = [&](PendingRawHandleRead& read,
                                        std::uint64_t offset,
                                        std::size_t requested) {
                BufferHandle slot = pool.acquire_spin();
                DataBuffer& buffer = data_buffer(pool, slot);
                buffer.trailer = {};
                read.state = {};
                read.state.offset = offset;
                read.state.requested = requested;
                read.state.pool = &pool;
                read.state.handle = slot;
                read.state.copy_payload_to_buffer = copy_payload_to_buffer;
                read.state.queued_at = std::chrono::steady_clock::now();
                const int queue_result = rpc_nfs_read_async(rpc,
                                                            raw_handle_read_callback,
                                                            &raw_handle,
                                                            offset,
                                                            requested,
                                                            &read.state);
                if (queue_result != 0) {
                    pool.release(slot);
                    const char* error = rpc_get_error(rpc);
                    throw std::runtime_error("rpc_nfs_read_async queue failed: " +
                                             std::string(error != nullptr ? error : "unknown error"));
                }
                record_async_read_queued(requested);
                read.in_use = true;
            };

            while ((next_offset < file.declared_size && !(should_stop && should_stop())) || in_flight != 0) {
                for (auto& read : pending) {
                    if (next_offset >= file.declared_size ||
                        in_flight >= max_in_flight ||
                        (should_stop && should_stop())) {
                        break;
                    }
                    if (read.in_use) {
                        continue;
                    }
                    const std::uint64_t remaining = file.declared_size - next_offset;
                    const std::size_t requested =
                        static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kLargeChunkBytes));
                    queue_read(read, next_offset, requested);
                    next_offset += requested;
                    ++in_flight;
                }

                service_nfs_context(nfs, in_flight == 0 ? 0 : 100);

                bool drained_ready_read = true;
                while (drained_ready_read) {
                    drained_ready_read = false;
                    auto ready_read =
                        std::find_if(pending.begin(), pending.end(), [&](const PendingRawHandleRead& read) {
                            return read.in_use && read.state.done && read.state.offset == next_emit_offset;
                        });
                    if (ready_read == pending.end()) {
                        break;
                    }

                    PendingRawHandleRead& read = *ready_read;
                    if (read.state.status < 0) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("rpc_nfs_read_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.state.requested != 0U) {
                        pool.release(read.state.handle);
                        read.in_use = false;
                        throw std::runtime_error("unexpected EOF while raw-handle reading NFS file: " + file.rel_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    const std::uint64_t chunk_offset = read.state.offset;
                    const std::size_t requested = read.state.requested;
                    const BufferHandle handle_for_visitor = read.state.handle;
                    total_read += bytes_read;
                    next_emit_offset += bytes_read;

                    read.in_use = false;
                    if (bytes_read != 0U) {
                        RawFileChunk chunk;
                        chunk.offset = chunk_offset;
                        chunk.handle = handle_for_visitor;
                        if (data_visitor) {
                            data_visitor(std::move(chunk));
                        } else {
                            pool.release(handle_for_visitor);
                        }
                    } else {
                        pool.release(handle_for_visitor);
                    }

                    if (bytes_read < requested) {
                        const std::uint64_t retry_offset = chunk_offset + bytes_read;
                        const std::size_t retry_bytes = requested - bytes_read;
                        queue_read(read, retry_offset, retry_bytes);
                    } else {
                        --in_flight;
                    }
                    drained_ready_read = true;
                }
            }
        } catch (...) {
            drain_pending_reads();
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::uint64_t visit_file_chunks(
        std::string_view rel_path,
        std::uint64_t declared_size,
        std::size_t outstanding_requests,
        const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        if (declared_size == 0) {
            return 0;
        }

        const std::string remote_path = "/" + normalized_path;
        LibNfsSession& active_session = session();
        const AsyncCommandState open_state = run_async_command(
            active_session.context(),
            [&](AsyncCommandState* state) {
                return nfs_open_async(active_session.context(), remote_path.c_str(), O_RDONLY, generic_nfs_callback, state);
            },
            "nfs_open_async");

        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        std::uint64_t total_read = 0;
        try {
            const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_requests);
            std::vector<PendingVisitRead> pending(max_in_flight);
            std::vector<bool> in_use(max_in_flight, false);
            std::size_t in_flight = 0;
            std::uint64_t next_offset = 0;

            while (next_offset < declared_size || in_flight != 0) {
                while (next_offset < declared_size && in_flight < max_in_flight) {
                    const auto free_it = std::find(in_use.begin(), in_use.end(), false);
                    if (free_it == in_use.end()) {
                        break;
                    }
                    const std::size_t index = static_cast<std::size_t>(std::distance(in_use.begin(), free_it));
                    PendingVisitRead& read = pending[index];
                    read = {};
                    read.offset = next_offset;
                    read.state.offset = next_offset;
                    read.state.visitor = &data_visitor;
                    const std::size_t remaining = static_cast<std::size_t>(declared_size - next_offset);
                    read.requested = std::min<std::size_t>(remaining, 1024U * 1024U);

                    const int queue_result = nfs_pread_async(active_session.context(),
                                                             handle,
                                                             read.offset,
                                                             read.requested,
                                                             visit_read_nfs_callback,
                                                             &read.state);
                    if (queue_result != 0) {
                        throw std::runtime_error("nfs_pread_async queue failed: " +
                                                 std::string(nfs_get_error(active_session.context())));
                    }
                    in_use[index] = true;
                    ++in_flight;
                    next_offset += read.requested;
                }

                service_nfs_context(active_session.context(), in_flight == 0 ? 0 : 100);

                for (std::size_t index = 0; index < pending.size(); ++index) {
                    if (!in_use[index] || !pending[index].state.done) {
                        continue;
                    }
                    PendingVisitRead& read = pending[index];
                    if (read.state.visitor_error) {
                        std::rethrow_exception(read.state.visitor_error);
                    }
                    if (read.state.status < 0) {
                        throw std::runtime_error("nfs_pread_async failed: " + read.state.error);
                    }
                    if (read.state.status == 0 && read.requested != 0U) {
                        throw std::runtime_error("unexpected EOF while reading NFS file: " + normalized_path);
                    }

                    const std::size_t bytes_read = static_cast<std::size_t>(read.state.status);
                    total_read += bytes_read;
                    if (bytes_read < read.requested) {
                        read.offset += bytes_read;
                        read.requested -= bytes_read;
                        read.state = {};
                        read.state.offset = read.offset;
                        read.state.visitor = &data_visitor;
                        const int queue_result = nfs_pread_async(active_session.context(),
                                                                 handle,
                                                                 read.offset,
                                                                 read.requested,
                                                                 visit_read_nfs_callback,
                                                                 &read.state);
                        if (queue_result != 0) {
                            throw std::runtime_error("nfs_pread_async queue failed: " +
                                                     std::string(nfs_get_error(active_session.context())));
                        }
                        continue;
                    }
                    in_use[index] = false;
                    --in_flight;
                }
            }

            run_async_command(
                active_session.context(),
                [&](AsyncCommandState* state) {
                    return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                },
                "nfs_close_async");
        } catch (...) {
            try {
                run_async_command(
                    active_session.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_close_async(active_session.context(), handle, generic_nfs_callback, state);
                    },
                    "nfs_close_async");
            } catch (...) {
            }
            throw;
        }
        return total_read;
    }

    [[nodiscard]] std::optional<FileSpec> stat_path(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            return std::nullopt;
        }

        const auto stat = try_stat64(session().context(), "/" + normalized_path);
        if (!stat.has_value()) {
            return std::nullopt;
        }

        FileSpec spec;
        spec.rel_path = normalized_path;
        spec.declared_size = stat->nfs_size;
        spec.mtime = mtime_from_nfs_stat(*stat);
        spec.mode = static_cast<std::uint32_t>(stat->nfs_mode & 0777U);
        spec.uid = static_cast<std::uint32_t>(stat->nfs_uid);
        spec.gid = static_cast<std::uint32_t>(stat->nfs_gid);
        return spec;
    }

    [[nodiscard]] bool metadata_matches(std::string_view rel_path,
                                        std::uint64_t size,
                                        std::uint64_t mtime) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            return false;
        }

        const auto stat = try_stat64(session().context(), "/" + normalized_path);
        if (!stat.has_value()) {
            return false;
        }
        return stat->nfs_size == size &&
               truncate_to_microseconds(mtime_from_nfs_stat(*stat)) == truncate_to_microseconds(mtime);
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        return nfs_stream_hash(session().context(), "/" + normalized_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return true;
    }

    [[nodiscard]] std::string description() const override {
        return root_url_;
    }

    void visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const override {
        visit_files_impl(session().context(), "/", "", recursive, visitor);
    }

    void visit_metadata(bool recursive,
                        const std::function<void(FileSpec)>& file_visitor,
                        const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_impl(session().context(), "/", "", recursive, file_visitor, directory_visitor);
    }

    void visit_metadata_at(std::string_view rel_path,
                           bool recursive,
                           const std::function<void(FileSpec)>& file_visitor,
                           const std::function<void(FileSpec)>& directory_visitor) const override {
        const std::string normalized_path = normalize_path(rel_path);
        const std::string remote_path = normalized_path.empty() ? "/" : "/" + normalized_path;
        visit_metadata_impl(session().context(), remote_path, normalized_path, recursive, file_visitor, directory_visitor);
    }

    void visit_folder(std::string_view rel_path,
                      const std::function<void(FileSpec)>& file_visitor,
                      const std::function<void(FileSpec)>& directory_visitor) const override {
        visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
    }

    void scan_flat_folders(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const override {
        scan_flat_folders_impl(outstanding_folders, folder_provider, should_stop, folder_visitor, false);
    }

    void scan_flat_folders_streaming(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor) const override {
        scan_flat_folders_impl(outstanding_folders, folder_provider, should_stop, folder_visitor, true);
    }

private:
    void scan_flat_folders_impl(
        std::size_t outstanding_folders,
        const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
        const std::function<bool()>& should_stop,
        const std::function<void(FlatFolderScanBatch)>& folder_visitor,
        bool stream_pages) const {
        struct nfs_context* nfs = nullptr;
        const std::size_t max_in_flight = std::max<std::size_t>(1, outstanding_folders);
        constexpr std::size_t kMaxTransientFolderRetries = 3;
        // Directory opens can legitimately sit behind many other outstanding
        // libnfs operations on large scans. A short per-open timeout causes
        // whole-context recovery and requeues unrelated pending folders, which
        // destroys the reader's steady-state throughput. Keep the hook here for
        // future configurable failure handling, but default to the old behavior:
        // let libnfs complete queued opens unless the job itself is stopping.
        constexpr bool kDirectoryOpenTimeoutEnabled = false;
        constexpr auto kDirectoryOpenTimeout = std::chrono::seconds(5);
        std::vector<PendingDirectoryOpen> pending(max_in_flight);
        std::deque<RetriedDirectoryOpen> retry_folders;
        std::size_t in_flight = 0;
        bool provider_exhausted = false;

        auto close_completed_directory = [&](PendingDirectoryOpen& slot) {
            if (nfs != nullptr && slot.state.done && slot.state.status >= 0 && slot.state.data != nullptr) {
                nfs_closedir(nfs, static_cast<struct nfsdir*>(slot.state.data));
                slot.state.data = nullptr;
            }
        };

        auto requeue_or_fail = [&](PendingDirectoryOpen& slot, const std::string& error) {
            close_completed_directory(slot);
            if (slot.retry_attempts < kMaxTransientFolderRetries && !should_stop()) {
                retry_folders.push_back(RetriedDirectoryOpen{std::move(slot.folder), slot.retry_attempts + 1U});
                slot = {};
                return;
            }

            FlatFolderScanBatch batch;
            batch.folder = std::move(slot.folder);
            batch.scan_started_unix_ns = slot.scan_started_unix_ns;
            batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
            batch.failed = true;
            batch.error = error;
            if (!slot.remote_path.empty()) {
                batch.error += " while scanning " + slot.remote_path;
            }
            slot = {};
            folder_visitor(std::move(batch));
        };

        auto recover_session = [&]() {
            if (session_) {
                session_->abandon_stuck_context();
                session_.reset();
            }
            nfs = nullptr;
        };

        auto recover_pending = [&](const std::string& error) {
            for (PendingDirectoryOpen& slot : pending) {
                if (!slot.in_use) {
                    continue;
                }
                requeue_or_fail(slot, error);
            }

            in_flight = 0;
            recover_session();
        };

        auto stop_pending = [&]() {
            for (PendingDirectoryOpen& slot : pending) {
                if (!slot.in_use) {
                    continue;
                }
                close_completed_directory(slot);
                slot = {};
            }
            retry_folders.clear();
            in_flight = 0;
            provider_exhausted = true;
            if (session_) {
                session_->abandon_stuck_context();
                session_.reset();
            }
        };

        auto timed_out_open = [&]() -> bool {
            if (!kDirectoryOpenTimeoutEnabled) {
                return false;
            }
            const auto now = std::chrono::steady_clock::now();
            return std::any_of(pending.begin(), pending.end(), [&](const PendingDirectoryOpen& slot) {
                return slot.in_use && !slot.state.done && now - slot.queued_at >= kDirectoryOpenTimeout;
            });
        };

        while (in_flight != 0 || (!should_stop() && (!retry_folders.empty() || !provider_exhausted))) {
            if (should_stop()) {
                stop_pending();
                break;
            }

            while (!should_stop() && in_flight < max_in_flight) {
                std::optional<FileSpec> folder;
                std::size_t retry_attempts = 0;
                if (!retry_folders.empty()) {
                    RetriedDirectoryOpen retry = std::move(retry_folders.front());
                    retry_folders.pop_front();
                    folder = std::move(retry.folder);
                    retry_attempts = retry.retry_attempts;
                } else {
                    folder = folder_provider(in_flight == 0);
                }
                if (!folder.has_value()) {
                    if (in_flight == 0) {
                        provider_exhausted = true;
                    }
                    break;
                }

                auto free_it = std::find_if(pending.begin(), pending.end(), [](const PendingDirectoryOpen& slot) {
                    return !slot.in_use;
                });
                if (free_it == pending.end()) {
                    break;
                }

                PendingDirectoryOpen& slot = *free_it;
                slot = {};
                slot.in_use = true;
                slot.folder = std::move(*folder);
                slot.retry_attempts = retry_attempts;
                const std::string normalized_path = normalize_path(slot.folder.rel_path);
                slot.folder.rel_path = normalized_path;
                slot.remote_path = normalized_path.empty() ? "/" : "/" + normalized_path;
                slot.queued_at = std::chrono::steady_clock::now();
                slot.scan_started_unix_ns = current_unix_time_nanoseconds();

                if (nfs == nullptr) {
                    try {
                        nfs = session().context();
                    } catch (const std::exception& error) {
                        slot.in_use = false;
                        if (retry_attempts < kMaxTransientFolderRetries && !should_stop()) {
                            retry_folders.push_back(RetriedDirectoryOpen{std::move(slot.folder), retry_attempts + 1U});
                        } else {
                            FlatFolderScanBatch batch;
                            batch.folder = std::move(slot.folder);
                            batch.scan_started_unix_ns = slot.scan_started_unix_ns;
                            batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
                            batch.failed = true;
                            batch.error = std::string(error.what()) + " while connecting for " + slot.remote_path;
                            folder_visitor(std::move(batch));
                        }
                        slot = {};
                        continue;
                    }
                }

                const int queue_result =
                    nfs_opendir_async(nfs, slot.remote_path.c_str(), generic_nfs_callback, &slot.state);
                if (queue_result != 0) {
                    slot.in_use = false;
                    FlatFolderScanBatch batch;
                    batch.folder = std::move(slot.folder);
                    batch.scan_started_unix_ns = slot.scan_started_unix_ns;
                    batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
                    batch.failed = true;
                    const char* error = nfs_get_error(nfs);
                    batch.error = "nfs_opendir_async queue failed: " +
                                  std::string((error != nullptr && *error != '\0') ? error : "unknown error");
                    if (retry_attempts < kMaxTransientFolderRetries) {
                        retry_folders.push_back(RetriedDirectoryOpen{std::move(batch.folder), retry_attempts + 1U});
                        recover_session();
                    } else {
                        folder_visitor(std::move(batch));
                    }
                    continue;
                }
                ++in_flight;
            }

            if (in_flight == 0) {
                if (should_stop()) {
                    break;
                }
                continue;
            }

            try {
                service_nfs_context(nfs, 100);
            } catch (const std::exception& error) {
                recover_pending(error.what());
                continue;
            }
            if (should_stop()) {
                stop_pending();
                break;
            }
            if (timed_out_open()) {
                recover_pending("nfs_opendir_async timed out");
                continue;
            }

            for (PendingDirectoryOpen& slot : pending) {
                if (!slot.in_use || !slot.state.done) {
                    continue;
                }

                FlatFolderScanBatch batch;
                batch.folder = std::move(slot.folder);
                batch.scan_started_unix_ns = slot.scan_started_unix_ns;
                if (slot.state.status < 0) {
                    batch.failed = true;
                    batch.error = slot.state.error.empty() ? "nfs_opendir_async failed with unknown error"
                                                           : slot.state.error;
                } else {
                    auto* directory = static_cast<struct nfsdir*>(slot.state.data);
                    try {
                        const std::function<void(FlatFolderScanBatch)> page_visitor =
                            stream_pages ? folder_visitor : std::function<void(FlatFolderScanBatch)> {};
                        batch = read_flat_directory_batch(nfs,
                                                          directory,
                                                          std::move(batch),
                                                          should_stop,
                                                          page_visitor,
                                                          readdirplus_page_bytes_,
                                                          session().connection_url());
                    } catch (const std::exception& error) {
                        batch.failed = true;
                        batch.error = std::string(error.what()) + " while reading " + slot.remote_path;
                    }
                    if (directory != nullptr) {
                        nfs_closedir(nfs, directory);
                    }
                }
                batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
                slot.in_use = false;
                --in_flight;
                if (!stream_pages || batch.failed) {
                    folder_visitor(std::move(batch));
                }
            }
        }
    }

    [[nodiscard]] LibNfsSession& session() const {
        if (!session_) {
            session_ = std::make_unique<LibNfsSession>(root_url_, endpoint_index_);
        }
        return *session_;
    }

    static FlatFolderScanBatch read_flat_directory_batch(struct nfs_context* nfs,
                                                         struct nfsdir* directory,
                                                         FlatFolderScanBatch batch,
                                                         const std::function<bool()>& should_stop,
                                                         const std::function<void(FlatFolderScanBatch)>& page_visitor,
                                                         std::size_t readdirplus_page_bytes,
                                                         const std::string& endpoint) {
        if (directory == nullptr) {
            batch.failed = true;
            batch.error = "nfs_opendir_async returned no directory handle";
            return batch;
        }

        const std::string rel_prefix = normalize_path(batch.folder.rel_path);
        auto* private_directory = reinterpret_cast<LibNfsPrivateDir*>(directory);
        if (page_visitor && private_directory != nullptr && private_directory->fh.val != nullptr &&
            private_directory->fh.len > 0) {
            nfs_fh3 directory_handle {};
            directory_handle.data.data_len = static_cast<u_int>(private_directory->fh.len);
            directory_handle.data.data_val = private_directory->fh.val;

            std::uint64_t cookie = 0;
            char cookie_verifier[NFS3_COOKIEVERFSIZE] {};
            bool eof = false;
            while (!eof && !should_stop()) {
                FlatFolderScanBatch page_batch;
                if (page_visitor) {
                    page_batch.folder = batch.folder;
                    page_batch.scan_started_unix_ns = batch.scan_started_unix_ns;
                    page_batch.complete = false;
                }
                RawReaddirplusState state;
                state.cookie = cookie;
                state.eof = false;
                std::memcpy(state.cookie_verifier, cookie_verifier, NFS3_COOKIEVERFSIZE);
                state.batch = page_visitor ? &page_batch : &batch;
                state.rel_prefix = rel_prefix;
                state.endpoint = endpoint;
                state.queued_at = std::chrono::steady_clock::now();
                state.requested_bytes = readdirplus_page_bytes;
                const int queue_result = rpc_nfs_readdirplus_async(nfs_get_rpc_context(nfs),
                                                                   raw_readdirplus_callback,
                                                                   &directory_handle,
                                                                   cookie,
                                                                   cookie_verifier,
                                                                   static_cast<u_int>(readdirplus_page_bytes),
                                                                   &state);
                if (queue_result != 0) {
                    const char* error = rpc_get_error(nfs_get_rpc_context(nfs));
                    throw std::runtime_error("rpc_nfs_readdirplus_async queue failed: " +
                                             std::string(error != nullptr ? error : "unknown error"));
                }
                while (!state.done) {
                    service_nfs_context(nfs, 100);
                }
                if (state.status != RPC_STATUS_SUCCESS) {
                    throw std::runtime_error(state.error.empty() ? "rpc_nfs_readdirplus_async failed" : state.error);
                }
                cookie = state.cookie;
                std::memcpy(cookie_verifier, state.cookie_verifier, NFS3_COOKIEVERFSIZE);
                eof = state.eof;
                if (page_visitor) {
                    page_batch.complete = eof;
                    page_batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
                    if (!page_batch.files.empty() || !page_batch.directories.empty() || eof) {
                        page_visitor(std::move(page_batch));
                    }
                }
            }
            return batch;
        }

        FlatFolderScanBatch page_batch;
        std::size_t page_records = 0;
        const auto emit_fallback_page = [&](bool complete) {
            if (!page_visitor || (page_records == 0U && !complete)) {
                return;
            }
            page_batch.folder = batch.folder;
            page_batch.scan_started_unix_ns = batch.scan_started_unix_ns;
            page_batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
            page_batch.complete = complete;
            page_visitor(std::move(page_batch));
            page_batch = {};
            page_records = 0;
        };

        while (auto* entry = nfs_readdir(nfs, directory)) {
            if (should_stop()) {
                break;
            }

            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                FileSpec spec;
                spec.rel_path = rel_path;
                spec.mtime = mtime_from_nfs_dirent(*entry);
                spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
                spec.uid = entry->uid;
                spec.gid = entry->gid;
                if (page_visitor) {
                    page_batch.directories.push_back(std::move(spec));
                    ++page_records;
                    if (page_records >= 4096U) {
                        emit_fallback_page(false);
                    }
                } else {
                    batch.directories.push_back(std::move(spec));
                }
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            if (page_visitor) {
                page_batch.files.push_back(std::move(spec));
                ++page_records;
                if (page_records >= 4096U) {
                    emit_fallback_page(false);
                }
            } else {
                batch.files.push_back(std::move(spec));
            }
        }
        emit_fallback_page(true);
        return batch;
    }

    static void visit_files_impl(struct nfs_context* nfs,
                                 const std::string& remote_path,
                                 const std::string& rel_prefix,
                                 bool recursive,
                                 const std::function<void(FileSpec)>& visitor) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                if (recursive) {
                    visit_files_impl(nfs, join_remote_path(remote_path, entry_name), rel_path, true, visitor);
                }
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            visitor(std::move(spec));
        }
        nfs_closedir(nfs, directory);
    }

    static void visit_metadata_impl(struct nfs_context* nfs,
                                    const std::string& remote_path,
                                    const std::string& rel_prefix,
                                    bool recursive,
                                    const std::function<void(FileSpec)>& file_visitor,
                                    const std::function<void(FileSpec)>& directory_visitor) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const std::string rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (S_ISDIR(mode)) {
                FileSpec spec;
                spec.rel_path = rel_path;
                spec.mtime = mtime_from_nfs_dirent(*entry);
                spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
                spec.uid = entry->uid;
                spec.gid = entry->gid;
                directory_visitor(std::move(spec));

                if (recursive) {
                    visit_metadata_impl(nfs, join_remote_path(remote_path, entry_name), rel_path, true, file_visitor, directory_visitor);
                }
                continue;
            }
            if (!S_ISREG(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path = rel_path;
            spec.declared_size = entry->size;
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            file_visitor(std::move(spec));
        }
        nfs_closedir(nfs, directory);
    }

    static void collect_files(struct nfs_context* nfs,
                              const std::string& remote_path,
                              const std::string& rel_prefix,
                              bool recursive,
                              std::vector<FileSpec>& files) {
        visit_files_impl(nfs, remote_path, rel_prefix, recursive,
                         [&files](FileSpec spec) { files.push_back(std::move(spec)); });
    }

    static void collect_directories(struct nfs_context* nfs,
                                    const std::string& remote_path,
                                    const std::string& rel_prefix,
                                    bool recursive,
                                    std::vector<FileSpec>& directories) {
        const AsyncCommandState open_state = run_async_command(
            nfs,
            [&](AsyncCommandState* state) {
                return nfs_opendir_async(nfs, remote_path.c_str(), generic_nfs_callback, state);
            },
            "nfs_opendir_async");

        auto* directory = static_cast<struct nfsdir*>(open_state.data);
        while (auto* entry = nfs_readdir(nfs, directory)) {
            const std::string_view entry_name(entry->name != nullptr ? entry->name : "");
            if (entry_name.empty() || entry_name == "." || entry_name == "..") {
                continue;
            }

            const mode_t mode = static_cast<mode_t>(entry->mode);
            if (!S_ISDIR(mode)) {
                continue;
            }

            FileSpec spec;
            spec.rel_path =
                rel_prefix.empty() ? normalize_path(entry_name) : normalize_path(rel_prefix + "/" + std::string(entry_name));
            spec.mtime = mtime_from_nfs_dirent(*entry);
            spec.mode = static_cast<std::uint32_t>(entry->mode & 0777U);
            spec.uid = entry->uid;
            spec.gid = entry->gid;
            directories.push_back(spec);

            if (recursive) {
                collect_directories(nfs, join_remote_path(remote_path, entry_name), spec.rel_path, true, directories);
            }
        }
        nfs_closedir(nfs, directory);
    }

    std::string root_url_;
    std::size_t endpoint_index_;
    std::size_t readdirplus_page_bytes_;
    mutable std::unique_ptr<LibNfsSession> session_;
};

class NfsTargetWriteReactorFleet;

std::shared_ptr<NfsTargetWriteReactorFleet> shared_target_write_reactor_fleet(
    const std::string& root_url,
    const TargetWriterBackend::Options& options);

class NfsTargetWriteReactorFleet final : public std::enable_shared_from_this<NfsTargetWriteReactorFleet> {
public:
    explicit NfsTargetWriteReactorFleet(std::string root_url, TargetWriterBackend::Options options)
        : root_url_(std::move(root_url)), options_(options) {
        const std::vector<std::string> endpoints = expand_nfs_url_server_candidates(root_url_);
        const std::size_t reactor_count =
            std::max<std::size_t>(1U, std::min<std::size_t>(16U, endpoints.empty() ? 1U : endpoints.size()));
        reactors_.reserve(reactor_count);
        for (std::size_t index = 0; index < reactor_count; ++index) {
            reactors_.push_back(std::make_unique<Reactor>(*this, index));
        }
        for (auto& reactor : reactors_) {
            reactor->start();
        }
    }

    ~NfsTargetWriteReactorFleet() {
        stop();
    }

    NfsTargetWriteReactorFleet(const NfsTargetWriteReactorFleet&) = delete;
    NfsTargetWriteReactorFleet& operator=(const NfsTargetWriteReactorFleet&) = delete;

    void write_files(const std::vector<TargetWriterBackend::WriteChunk>& files) {
        if (files.empty()) {
            return;
        }

        auto completion = std::make_shared<BatchCompletion>();
        completion->remaining.store(files.size(), std::memory_order_release);
        for (const TargetWriterBackend::WriteChunk& file : files) {
            auto transaction = std::make_shared<FileTransaction>();
            transaction->completion = completion;
            transaction->spec = file.spec;
            transaction->spec.rel_path = normalize_path(transaction->spec.rel_path);
            transaction->remote_path = "/" + transaction->spec.rel_path;
            transaction->offset = file.offset;
            transaction->data = file.data;
            select_reactor(transaction->spec.rel_path).enqueue(std::move(transaction));
        }

        std::unique_lock<std::mutex> lock(completion->mutex);
        completion->cv.wait(lock, [&completion] {
            return completion->remaining.load(std::memory_order_acquire) == 0U;
        });
        if (!completion->first_error.empty()) {
            throw std::runtime_error(completion->first_error);
        }
    }

    [[nodiscard]] std::size_t reactor_count() const noexcept {
        return reactors_.size();
    }

private:
    struct BatchCompletion {
        std::atomic<std::size_t> remaining {0};
        std::mutex mutex;
        std::condition_variable cv;
        std::string first_error;
    };

    struct FileTransaction {
        enum class Phase { queued, creating, writing, syncing, closing, done };

        FileSpec spec;
        std::string remote_path;
        std::string_view data;
        std::uint64_t offset = 0;
        std::size_t bytes_written = 0;
        struct nfsfh* handle = nullptr;
        AsyncCommandState create_state;
        AsyncCommandState write_state;
        AsyncCommandState sync_state;
        AsyncCommandState close_state;
        Phase phase = Phase::queued;
        std::shared_ptr<BatchCompletion> completion;
        std::string error;
    };

    struct QueueNode {
        explicit QueueNode(std::shared_ptr<FileTransaction> value)
            : transaction(std::move(value)) {}

        std::shared_ptr<FileTransaction> transaction;
        QueueNode* next = nullptr;
    };

    class Reactor {
    public:
        Reactor(NfsTargetWriteReactorFleet& fleet, std::size_t index)
            : fleet_(fleet), index_(index), session_(fleet.root_url_, index) {}

        ~Reactor() {
            stop();
        }

        Reactor(const Reactor&) = delete;
        Reactor& operator=(const Reactor&) = delete;

        void start() {
            thread_ = std::thread([this] { run(); });
        }

        void stop() {
            stop_requested_.store(true, std::memory_order_release);
            if (thread_.joinable()) {
                thread_.join();
            }
        }

        void enqueue(std::shared_ptr<FileTransaction> transaction) {
            auto* node = new QueueNode(std::move(transaction));
            QueueNode* head = inbound_.load(std::memory_order_acquire);
            do {
                node->next = head;
            } while (!inbound_.compare_exchange_weak(head,
                                                     node,
                                                     std::memory_order_release,
                                                     std::memory_order_acquire));
        }

    private:
        void run() {
            pin_current_thread_to_cpu(index_);
            try {
                while (!stop_requested_.load(std::memory_order_acquire) || inbound_.load(std::memory_order_acquire) != nullptr ||
                       !backlog_.empty() || !active_.empty()) {
                    drain_inbound();
                    fill_window();
                    service_nfs_context(session_.context(), active_.empty() ? 1 : 0);
                    advance_active();
                    if (active_.empty() && backlog_.empty() && inbound_.load(std::memory_order_acquire) == nullptr) {
                        std::this_thread::sleep_for(std::chrono::microseconds(50));
                    }
                }
            } catch (const std::exception& error) {
                fail_all(error.what());
            } catch (...) {
                fail_all("NFS target writer reactor failed with an unknown error");
            }
        }

        void drain_inbound() {
            QueueNode* list = inbound_.exchange(nullptr, std::memory_order_acq_rel);
            QueueNode* reversed = nullptr;
            while (list != nullptr) {
                QueueNode* next = list->next;
                list->next = reversed;
                reversed = list;
                list = next;
            }
            while (reversed != nullptr) {
                QueueNode* next = reversed->next;
                backlog_.push_back(std::move(reversed->transaction));
                delete reversed;
                reversed = next;
            }
        }

        void fill_window() {
            const std::size_t max_active =
                std::max<std::size_t>(1U, fleet_.options_.max_concurrent_file_transactions);
            while (!backlog_.empty() && active_.size() < max_active) {
                std::shared_ptr<FileTransaction> transaction = std::move(backlog_.front());
                backlog_.pop_front();
                if (fleet_.options_.ensure_parent_directories) {
                    ensure_directory_chain(parent_path(transaction->spec.rel_path));
                }
                queue_create(*transaction);
                active_.push_back(std::move(transaction));
            }
        }

        void queue_create(FileTransaction& transaction) {
            transaction.phase = FileTransaction::Phase::creating;
            transaction.create_state = {};
            transaction.create_state.queued_at = std::chrono::steady_clock::now();
            const int queue_result = nfs_create_async(session_.context(),
                                                      transaction.remote_path.c_str(),
                                                      O_TRUNC,
                                                      static_cast<int>(transaction.spec.mode),
                                                      generic_nfs_callback,
                                                      &transaction.create_state);
            if (queue_result != 0) {
                throw std::runtime_error("nfs_create_async queue failed: " +
                                         std::string(nfs_get_error(session_.context())));
            }
        }

        void queue_write(FileTransaction& transaction) {
            transaction.phase = FileTransaction::Phase::writing;
            transaction.write_state = {};
            transaction.write_state.queued_at = std::chrono::steady_clock::now();
            int queue_result = 0;
            if (fleet_.options_.stable_small_file_writes) {
                auto* rpc = nfs_get_rpc_context(session_.context());
                auto* raw_handle = reinterpret_cast<struct nfs_fh3*>(nfs_get_fh(transaction.handle));
                if (rpc == nullptr || raw_handle == nullptr) {
                    throw std::runtime_error("nfs raw stable write handle is unavailable");
                }
                const std::size_t remaining = transaction.data.size() - transaction.bytes_written;
                queue_result = rpc_nfs_write_async(rpc,
                                                   raw_write_callback,
                                                   raw_handle,
                                                   const_cast<char*>(transaction.data.data() + transaction.bytes_written),
                                                   transaction.offset + transaction.bytes_written,
                                                   remaining,
                                                   FILE_SYNC,
                                                   &transaction.write_state);
            } else {
                queue_result = nfs_pwrite_async(session_.context(),
                                                transaction.handle,
                                                transaction.offset,
                                                transaction.data.size(),
                                                transaction.data.data(),
                                                generic_nfs_callback,
                                                &transaction.write_state);
            }
            if (queue_result != 0) {
                throw std::runtime_error(std::string(fleet_.options_.stable_small_file_writes
                                                         ? "rpc_nfs_write_async queue failed: "
                                                         : "nfs_pwrite_async queue failed: ") +
                                         std::string(nfs_get_error(session_.context())));
            }
        }

        void queue_sync(FileTransaction& transaction) {
            transaction.phase = FileTransaction::Phase::syncing;
            transaction.sync_state = {};
            transaction.sync_state.queued_at = std::chrono::steady_clock::now();
            const int queue_result =
                nfs_fsync_async(session_.context(), transaction.handle, generic_nfs_callback, &transaction.sync_state);
            if (queue_result != 0) {
                throw std::runtime_error("nfs_fsync_async queue failed: " +
                                         std::string(nfs_get_error(session_.context())));
            }
        }

        void queue_close(FileTransaction& transaction) {
            transaction.phase = FileTransaction::Phase::closing;
            transaction.close_state = {};
            transaction.close_state.queued_at = std::chrono::steady_clock::now();
            const int queue_result =
                nfs_close_async(session_.context(), transaction.handle, generic_nfs_callback, &transaction.close_state);
            if (queue_result != 0) {
                throw std::runtime_error("nfs_close_async queue failed: " +
                                         std::string(nfs_get_error(session_.context())));
            }
        }

        void advance_active() {
            for (auto it = active_.begin(); it != active_.end();) {
                FileTransaction& transaction = **it;
                try {
                    if (transaction.phase == FileTransaction::Phase::creating && transaction.create_state.done) {
                        if (transaction.create_state.status < 0) {
                            throw std::runtime_error("nfs_create_async failed: " + transaction.create_state.error);
                        }
                        transaction.handle = static_cast<struct nfsfh*>(transaction.create_state.data);
                        if (transaction.handle == nullptr) {
                            throw std::runtime_error("nfs_create_async succeeded without returning a file handle");
                        }
                        if (transaction.data.empty()) {
                            queue_close(transaction);
                        } else {
                            queue_write(transaction);
                        }
                    }
                    if (transaction.phase == FileTransaction::Phase::writing && transaction.write_state.done) {
                        if (fleet_.options_.stable_small_file_writes) {
                            if (transaction.write_state.status != RPC_STATUS_SUCCESS) {
                                throw std::runtime_error("rpc_nfs_write_async failed: " + transaction.write_state.error);
                            }
                            if (transaction.write_state.nfs_status != NFS3_OK) {
                                throw std::runtime_error("rpc_nfs_write_async returned NFS error " +
                                                         std::to_string(transaction.write_state.nfs_status));
                            }
                            const std::size_t remaining = transaction.data.size() - transaction.bytes_written;
                            const std::size_t written = transaction.write_state.byte_count;
                            if (written == 0U || written > remaining) {
                                throw std::runtime_error("rpc_nfs_write_async short write count=" +
                                                         std::to_string(written) + " remaining=" +
                                                         std::to_string(remaining));
                            }
                            transaction.bytes_written += written;
                            if (transaction.bytes_written < transaction.data.size()) {
                                queue_write(transaction);
                                ++it;
                                continue;
                            }
                        } else {
                            if (transaction.write_state.status < 0) {
                                throw std::runtime_error("nfs_pwrite_async failed: " + transaction.write_state.error);
                            }
                            if (transaction.write_state.status != static_cast<int>(transaction.data.size())) {
                                throw std::runtime_error("nfs_pwrite_async short write");
                            }
                        }
                        if (fleet_.options_.fsync_on_finish) {
                            queue_sync(transaction);
                        } else {
                            queue_close(transaction);
                        }
                    }
                    if (transaction.phase == FileTransaction::Phase::syncing && transaction.sync_state.done) {
                        if (transaction.sync_state.status < 0) {
                            throw std::runtime_error("nfs_fsync_async failed: " + transaction.sync_state.error);
                        }
                        queue_close(transaction);
                    }
                    if (transaction.phase == FileTransaction::Phase::closing && transaction.close_state.done) {
                        if (transaction.close_state.status < 0) {
                            throw std::runtime_error("nfs_close_async failed: " + transaction.close_state.error);
                        }
                        transaction.handle = nullptr;
                        if (fleet_.options_.preserve_metadata) {
                            apply_remote_metadata(transaction.remote_path, transaction.spec);
                        }
                        transaction.phase = FileTransaction::Phase::done;
                        complete_transaction(*it, {});
                        it = active_.erase(it);
                        continue;
                    }
                } catch (const std::exception& error) {
                    if (transaction.handle != nullptr) {
                        nfs_close(session_.context(), transaction.handle);
                        transaction.handle = nullptr;
                    }
                    complete_transaction(*it, error.what());
                    it = active_.erase(it);
                    continue;
                }
                ++it;
            }
        }

        void complete_transaction(const std::shared_ptr<FileTransaction>& transaction, std::string error) {
            std::shared_ptr<BatchCompletion> completion = transaction->completion;
            if (!completion) {
                return;
            }
            {
                std::lock_guard<std::mutex> lock(completion->mutex);
                if (!error.empty() && completion->first_error.empty()) {
                    completion->first_error = std::move(error);
                }
            }
            if (completion->remaining.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
                completion->cv.notify_all();
            }
        }

        void fail_all(std::string_view error) {
            drain_inbound();
            while (!backlog_.empty()) {
                complete_transaction(backlog_.front(), std::string(error));
                backlog_.pop_front();
            }
            for (const auto& transaction : active_) {
                if (transaction->handle != nullptr) {
                    nfs_close(session_.context(), transaction->handle);
                    transaction->handle = nullptr;
                }
                complete_transaction(transaction, std::string(error));
            }
            active_.clear();
        }

        void apply_remote_metadata(const std::string& remote_path, const FileSpec& spec) {
            const auto stat = try_stat64(session_.context(), remote_path);
            if (!stat.has_value()) {
                throw std::runtime_error("remote path missing while applying metadata: " + remote_path);
            }

            if (static_cast<std::uint32_t>(stat->nfs_uid) != spec.uid ||
                static_cast<std::uint32_t>(stat->nfs_gid) != spec.gid) {
                run_async_command(
                    session_.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_chown_async(session_.context(),
                                               remote_path.c_str(),
                                               static_cast<int>(spec.uid),
                                               static_cast<int>(spec.gid),
                                               generic_nfs_callback,
                                               state);
                    },
                    "nfs_chown_async");
            }

            if ((stat->nfs_mode & 0777U) != spec.mode) {
                run_async_command(
                    session_.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_chmod_async(session_.context(),
                                               remote_path.c_str(),
                                               static_cast<int>(spec.mode),
                                               generic_nfs_callback,
                                               state);
                    },
                    "nfs_chmod_async");
            }

            const std::uint64_t remote_mtime = truncate_to_microseconds(mtime_from_nfs_stat(*stat));
            if (remote_mtime != truncate_to_microseconds(spec.mtime)) {
                struct timeval times[2];
                times[0].tv_sec = static_cast<time_t>(spec.mtime / 1'000'000'000ULL);
                times[0].tv_usec = static_cast<suseconds_t>((spec.mtime % 1'000'000'000ULL) / 1000ULL);
                times[1] = times[0];
                run_async_command(
                    session_.context(),
                    [&](AsyncCommandState* state) {
                        return nfs_utimes_async(
                            session_.context(), remote_path.c_str(), times, generic_nfs_callback, state);
                    },
                    "nfs_utimes_async");
            }
        }

        void ensure_directory_chain(std::string_view rel_path) {
            const std::string normalized = normalize_path(rel_path);
            if (normalized.empty()) {
                return;
            }

            std::string current_path;
            std::string component;
            for (char ch : normalized) {
                if (ch == '/') {
                    if (!component.empty()) {
                        if (!current_path.empty()) {
                            current_path.push_back('/');
                        }
                        current_path += component;
                        ensure_single_directory(current_path);
                        component.clear();
                    }
                } else {
                    component.push_back(ch);
                }
            }
            if (!component.empty()) {
                if (!current_path.empty()) {
                    current_path.push_back('/');
                }
                current_path += component;
                ensure_single_directory(current_path);
            }
        }

        void ensure_single_directory(const std::string& rel_path) {
            if (known_directories_.find(rel_path) != known_directories_.end()) {
                return;
            }

            const std::string remote_path = "/" + rel_path;
            const auto stat = try_stat64(session_.context(), remote_path);
            if (!stat.has_value()) {
                AsyncCommandState mkdir_state;
                mkdir_state.queued_at = std::chrono::steady_clock::now();
                const int queue_result = nfs_mkdir2_async(session_.context(),
                                                          remote_path.c_str(),
                                                          0755,
                                                          generic_nfs_callback,
                                                          &mkdir_state);
                if (queue_result != 0) {
                    throw std::runtime_error("nfs_mkdir2_async queue failed: " +
                                             std::string(nfs_get_error(session_.context())));
                }
                pump_nfs_until_done(session_.context(), mkdir_state);
                if (mkdir_state.status < 0 && mkdir_state.status != -EEXIST) {
                    throw std::runtime_error("nfs_mkdir2_async failed: " + mkdir_state.error);
                }
            } else if (!S_ISDIR(stat->nfs_mode)) {
                throw std::runtime_error("remote path exists but is not a directory: " + remote_path);
            }
            known_directories_.insert(rel_path);
        }

        NfsTargetWriteReactorFleet& fleet_;
        std::size_t index_;
        LibNfsSession session_;
        std::atomic<QueueNode*> inbound_ {nullptr};
        std::deque<std::shared_ptr<FileTransaction>> backlog_;
        std::vector<std::shared_ptr<FileTransaction>> active_;
        std::unordered_set<std::string> known_directories_ {""};
        std::atomic<bool> stop_requested_ {false};
        std::thread thread_;
    };

    Reactor& select_reactor(std::string_view rel_path) {
        const std::uint64_t key = hash64(rel_path);
        return *reactors_[static_cast<std::size_t>(key % reactors_.size())];
    }

    void stop() {
        for (auto& reactor : reactors_) {
            reactor->stop();
        }
    }

    std::string root_url_;
    TargetWriterBackend::Options options_;
    std::vector<std::unique_ptr<Reactor>> reactors_;
};

std::shared_ptr<NfsTargetWriteReactorFleet> shared_target_write_reactor_fleet(
    const std::string& root_url,
    const TargetWriterBackend::Options& options) {
    struct FleetKey {
        std::string root_url;
        bool preserve_metadata = true;
        bool fsync_on_finish = true;
        bool ensure_parent_directories = true;
        bool stable_small_file_writes = false;
        std::size_t max_concurrent_file_transactions = 64;

        [[nodiscard]] std::string string() const {
            std::ostringstream out;
            out << root_url << "|pm=" << preserve_metadata << "|fs=" << fsync_on_finish
                << "|ep=" << ensure_parent_directories << "|sw=" << stable_small_file_writes
                << "|fw=" << max_concurrent_file_transactions;
            return out.str();
        }
    };

    static std::mutex mutex;
    static std::unordered_map<std::string, std::weak_ptr<NfsTargetWriteReactorFleet>> fleets;

    FleetKey key;
    key.root_url = root_url;
    key.preserve_metadata = options.preserve_metadata;
    key.fsync_on_finish = options.fsync_on_finish;
    key.ensure_parent_directories = options.ensure_parent_directories;
    key.stable_small_file_writes = options.stable_small_file_writes;
    key.max_concurrent_file_transactions = options.max_concurrent_file_transactions;
    const std::string key_text = key.string();

    std::lock_guard<std::mutex> lock(mutex);
    auto it = fleets.find(key_text);
    if (it != fleets.end()) {
        if (auto existing = it->second.lock()) {
            return existing;
        }
        fleets.erase(it);
    }

    auto created = std::make_shared<NfsTargetWriteReactorFleet>(root_url, options);
    fleets.emplace(key_text, created);
    return created;
}

class LibNfsTargetWriterBackend final : public TargetWriterBackend {
public:
    explicit LibNfsTargetWriterBackend(std::string root_url, std::size_t endpoint_index, Options options)
        : root_url_(std::move(root_url)),
          endpoint_index_(endpoint_index),
          options_(options),
          reactor_fleet_(shared_target_write_reactor_fleet(root_url_, options_)) {
        known_directories_.insert("");
    }

    ~LibNfsTargetWriterBackend() override {
        for (auto& [_, handle] : open_handles_) {
            if (handle != nullptr) {
                nfs_close(legacy_session().context(), handle);
            }
        }
    }

    void ensure_directory(const FileSpec& spec) override {
        ensure_directory_chain(spec.rel_path);
    }

    void apply_directory_metadata(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (rel_path.empty()) {
            return;
        }
        ensure_directory_chain(rel_path);
        if (!options_.preserve_metadata) {
            return;
        }
        apply_remote_metadata("/" + rel_path, spec);
    }

    void write_chunk(const FileSpec& spec, std::string_view data, std::uint64_t offset) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        if (options_.ensure_parent_directories) {
            ensure_directory_chain(parent_path(rel_path));
        }
        struct nfsfh* handle = open_handle(rel_path, spec.mode);

        std::size_t written_total = 0;
        while (written_total < data.size()) {
            const AsyncCommandState write_state = run_async_command(
                legacy_session().context(),
                [&](AsyncCommandState* state) {
                    return nfs_pwrite_async(legacy_session().context(),
                                            handle,
                                            offset + written_total,
                                            data.size() - written_total,
                                            data.data() + written_total,
                                            generic_nfs_callback,
                                            state);
                },
                "nfs_pwrite_async");

            if (write_state.status < 0) {
                throw std::runtime_error("nfs_pwrite_async failed");
            }
            written_total += static_cast<std::size_t>(write_state.status);
        }
    }

    void write_chunks(const std::vector<WriteChunk>& chunks) override {
        struct PendingWrite {
            AsyncCommandState state;
            const WriteChunk* chunk = nullptr;
        };

        std::vector<PendingWrite> pending;
        pending.reserve(chunks.size());
        std::vector<const WriteChunk*> finish_after_write;
        finish_after_write.reserve(chunks.size());

        for (const WriteChunk& chunk : chunks) {
            if (chunk.data.empty()) {
                if (chunk.last_chunk) {
                    finish_after_write.push_back(&chunk);
                }
                continue;
            }
            const std::string rel_path = normalize_path(chunk.spec.rel_path);
            if (options_.ensure_parent_directories) {
                ensure_directory_chain(parent_path(rel_path));
            }
            struct nfsfh* handle = open_handle(rel_path, chunk.spec.mode);
            pending.push_back(PendingWrite {});
            PendingWrite& write = pending.back();
            write.chunk = &chunk;
            write.state.queued_at = std::chrono::steady_clock::now();
            const int queue_result = nfs_pwrite_async(legacy_session().context(),
                                                      handle,
                                                      chunk.offset,
                                                      chunk.data.size(),
                                                      chunk.data.data(),
                                                      generic_nfs_callback,
                                                      &write.state);
            if (queue_result != 0) {
                throw std::runtime_error("nfs_pwrite_async queue failed: " +
                                         std::string(nfs_get_error(legacy_session().context())));
            }
            if (chunk.last_chunk) {
                finish_after_write.push_back(&chunk);
            }
        }

        std::size_t completed = 0;
        while (completed < pending.size()) {
            service_nfs_context(legacy_session().context(), 100);
            completed = 0;
            for (const PendingWrite& write : pending) {
                if (write.state.done) {
                    ++completed;
                }
            }
        }

        for (const PendingWrite& write : pending) {
            if (write.state.status < 0) {
                throw std::runtime_error("nfs_pwrite_async failed: " + write.state.error);
            }
            if (write.state.status != static_cast<int>(write.chunk->data.size())) {
                throw std::runtime_error("nfs_pwrite_async short write");
            }
        }

        for (const WriteChunk* chunk : finish_after_write) {
            finish_file(chunk->spec);
        }
    }

    void write_files(const std::vector<WriteChunk>& files) override {
        reactor_fleet_->write_files(files);
    }

    void finish_file(const FileSpec& spec) override {
        const std::string rel_path = normalize_path(spec.rel_path);
        const std::string remote_path = "/" + rel_path;
        auto it = open_handles_.find(rel_path);
        if (it == open_handles_.end()) {
            return;
        }

        struct nfsfh* handle = it->second;
        if (options_.fsync_on_finish) {
            run_async_command(
                legacy_session().context(),
                [&](AsyncCommandState* state) {
                    return nfs_fsync_async(legacy_session().context(), handle, generic_nfs_callback, state);
                },
                "nfs_fsync_async");
        }
        run_async_command(
            legacy_session().context(),
            [&](AsyncCommandState* state) {
                return nfs_close_async(legacy_session().context(), handle, generic_nfs_callback, state);
            },
            "nfs_close_async");
        open_handles_.erase(it);
        if (options_.preserve_metadata) {
            apply_remote_metadata(remote_path, spec);
        }
    }

    void abort_file(std::string_view rel_path) noexcept override {
        const std::string normalized = normalize_path(rel_path);
        auto it = open_handles_.find(normalized);
        if (it == open_handles_.end()) {
            return;
        }
        if (it->second != nullptr) {
            nfs_close(legacy_session().context(), it->second);
        }
        open_handles_.erase(it);
        try {
            const std::string remote_path = "/" + normalized;
            nfs_unlink(legacy_session().context(), remote_path.c_str());
        } catch (...) {}
    }

    [[nodiscard]] std::uint64_t file_hash(std::string_view rel_path) const override {
        const std::string normalized_path = normalize_path(rel_path);
        if (normalized_path.empty()) {
            throw std::runtime_error("relative path must not be empty");
        }
        return nfs_stream_hash(legacy_session().context(), "/" + normalized_path);
    }

    [[nodiscard]] bool uses_async_api() const override {
        return true;
    }

private:
    LibNfsSession& legacy_session() const {
        if (!legacy_session_) {
            legacy_session_ = std::make_unique<LibNfsSession>(root_url_, endpoint_index_);
        }
        return *legacy_session_;
    }

    void apply_remote_metadata(const std::string& remote_path, const FileSpec& spec) {
        const auto stat = try_stat64(legacy_session().context(), remote_path);
        if (!stat.has_value()) {
            throw std::runtime_error("remote path missing while applying metadata: " + remote_path);
        }

        if (static_cast<std::uint32_t>(stat->nfs_uid) != spec.uid ||
            static_cast<std::uint32_t>(stat->nfs_gid) != spec.gid) {
            run_async_command(
                legacy_session().context(),
                [&](AsyncCommandState* state) {
                    return nfs_chown_async(legacy_session().context(),
                                           remote_path.c_str(),
                                           static_cast<int>(spec.uid),
                                           static_cast<int>(spec.gid),
                                           generic_nfs_callback,
                                           state);
                },
                "nfs_chown_async");
        }

        if ((stat->nfs_mode & 0777U) != spec.mode) {
            run_async_command(
                legacy_session().context(),
                [&](AsyncCommandState* state) {
                    return nfs_chmod_async(legacy_session().context(),
                                           remote_path.c_str(),
                                           static_cast<int>(spec.mode),
                                           generic_nfs_callback,
                                           state);
                },
                "nfs_chmod_async");
        }

        const std::uint64_t remote_mtime = truncate_to_microseconds(mtime_from_nfs_stat(*stat));
        if (remote_mtime != truncate_to_microseconds(spec.mtime)) {
            struct timeval times[2];
            times[0].tv_sec = static_cast<time_t>(spec.mtime / 1'000'000'000ULL);
            times[0].tv_usec = static_cast<suseconds_t>((spec.mtime % 1'000'000'000ULL) / 1000ULL);
            times[1] = times[0];
            run_async_command(
                legacy_session().context(),
                [&](AsyncCommandState* state) {
                    return nfs_utimes_async(legacy_session().context(), remote_path.c_str(), times, generic_nfs_callback, state);
                },
                "nfs_utimes_async");
        }
    }

    void ensure_directory_chain(std::string_view rel_path) {
        const std::string normalized = normalize_path(rel_path);
        if (normalized.empty()) {
            return;
        }

        std::string current_path;
        std::string component;
        for (char ch : normalized) {
            if (ch == '/') {
                if (!component.empty()) {
                    if (!current_path.empty()) {
                        current_path.push_back('/');
                    }
                    current_path += component;
                    ensure_single_directory(current_path);
                    component.clear();
                }
            } else {
                component.push_back(ch);
            }
        }
        if (!component.empty()) {
            if (!current_path.empty()) {
                current_path.push_back('/');
            }
            current_path += component;
            ensure_single_directory(current_path);
        }
    }

    void ensure_single_directory(const std::string& rel_path) {
        if (known_directories_.find(rel_path) != known_directories_.end()) {
            return;
        }

        const std::string remote_path = "/" + rel_path;
        const auto stat = try_stat64(legacy_session().context(), remote_path);
        if (!stat.has_value()) {
            AsyncCommandState mkdir_state;
            mkdir_state.queued_at = std::chrono::steady_clock::now();
            const int queue_result = nfs_mkdir2_async(legacy_session().context(),
                                                      remote_path.c_str(),
                                                      0755,
                                                      generic_nfs_callback,
                                                      &mkdir_state);
            if (queue_result != 0) {
                throw std::runtime_error("nfs_mkdir2_async queue failed: " +
                                         std::string(nfs_get_error(legacy_session().context())));
            }
            pump_nfs_until_done(legacy_session().context(), mkdir_state);
            if (mkdir_state.status < 0 && mkdir_state.status != -EEXIST) {
                throw std::runtime_error("nfs_mkdir2_async failed: " + mkdir_state.error);
            }
        } else if (!S_ISDIR(stat->nfs_mode)) {
            throw std::runtime_error("remote path exists but is not a directory: " + remote_path);
        }
        known_directories_.insert(rel_path);
    }

    struct nfsfh* open_handle(const std::string& rel_path, std::uint32_t mode) {
        auto it = open_handles_.find(rel_path);
        if (it != open_handles_.end()) {
            return it->second;
        }

        const std::string remote_path = "/" + rel_path;
        const AsyncCommandState open_state = run_async_command(
            legacy_session().context(),
            [&](AsyncCommandState* state) {
                return nfs_create_async(legacy_session().context(),
                                        remote_path.c_str(),
                                        O_TRUNC,
                                        static_cast<int>(mode),
                                        generic_nfs_callback,
                                        state);
            },
            "nfs_create_async");
        auto* handle = static_cast<struct nfsfh*>(open_state.data);
        open_handles_.emplace(rel_path, handle);
        return handle;
    }

    std::string root_url_;
    std::size_t endpoint_index_;
    Options options_;
    std::shared_ptr<NfsTargetWriteReactorFleet> reactor_fleet_;
    mutable std::unique_ptr<LibNfsSession> legacy_session_;
    std::unordered_map<std::string, struct nfsfh*> open_handles_;
    std::unordered_set<std::string> known_directories_;
};

#endif

}  // namespace

bool is_nfs_url(std::string_view path) {
    return path.rfind("nfs://", 0) == 0;
}

bool is_null_url(std::string_view path) {
    return path == "null" || path == "null:" || path == "null://";
}

std::vector<std::string> expand_nfs_url_server_candidates(std::string_view root_url) {
    constexpr std::string_view kPrefix = "nfs://";
    if (root_url.rfind(kPrefix, 0) != 0) {
        return {std::string(root_url)};
    }

    const std::size_t server_begin = kPrefix.size();
    const std::size_t server_end = root_url.find('/', server_begin);
    if (server_end == std::string_view::npos || server_end == server_begin) {
        return {std::string(root_url)};
    }

    const std::string_view server_expression = root_url.substr(server_begin, server_end - server_begin);
    const std::vector<std::string> servers = expand_server_expression(server_expression);
    if (servers.size() == 1U && servers.front() == server_expression) {
        return {std::string(root_url)};
    }

    std::vector<std::string> urls;
    urls.reserve(servers.size());
    const std::string_view suffix = root_url.substr(server_end);
    for (const std::string& server : servers) {
        std::string url;
        url.reserve(kPrefix.size() + server.size() + suffix.size());
        url.append(kPrefix);
        url.append(server);
        url.append(suffix);
        urls.push_back(std::move(url));
    }
    return urls;
}

bool libnfs_support_enabled() {
#if HYPERSYNC_HAS_LIBNFS
    return true;
#else
    return false;
#endif
}

void TargetWriterBackend::write_chunks(const std::vector<WriteChunk>& chunks) {
    for (const WriteChunk& chunk : chunks) {
        write_chunk(chunk.spec, chunk.data, chunk.offset);
        if (chunk.last_chunk) {
            finish_file(chunk.spec);
        }
    }
}

void TargetWriterBackend::write_files(const std::vector<WriteChunk>& files) {
    write_chunks(files);
}

void NfsBackend::visit_files(bool recursive, const std::function<void(FileSpec)>& visitor) const {
    for (const auto& spec : list_files(recursive)) {
        visitor(spec);
    }
}

void NfsBackend::visit_metadata(bool recursive,
                                const std::function<void(FileSpec)>& file_visitor,
                                const std::function<void(FileSpec)>& directory_visitor) const {
    for (const auto& directory : list_directories(recursive)) {
        directory_visitor(directory);
    }
    visit_files(recursive, file_visitor);
}

void NfsBackend::visit_metadata_at(std::string_view rel_path,
                                   bool recursive,
                                   const std::function<void(FileSpec)>& file_visitor,
                                   const std::function<void(FileSpec)>& directory_visitor) const {
    const std::string normalized_path = normalize_path(rel_path);
    visit_metadata(recursive, [&](FileSpec spec) {
        if (normalized_path.empty() || spec.rel_path == normalized_path ||
            spec.rel_path.rfind(normalized_path + "/", 0) == 0) {
            file_visitor(std::move(spec));
        }
    }, [&](FileSpec spec) {
        if (normalized_path.empty() || spec.rel_path == normalized_path ||
            spec.rel_path.rfind(normalized_path + "/", 0) == 0) {
            directory_visitor(std::move(spec));
        }
    });
}

void NfsBackend::visit_folder(std::string_view rel_path,
                              const std::function<void(FileSpec)>& file_visitor,
                              const std::function<void(FileSpec)>& directory_visitor) const {
    visit_metadata_at(rel_path, false, file_visitor, directory_visitor);
}

void NfsBackend::scan_flat_folders(
    std::size_t outstanding_folders,
    const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
    const std::function<bool()>& should_stop,
    const std::function<void(FlatFolderScanBatch)>& folder_visitor) const {
    (void)outstanding_folders;
    while (!should_stop()) {
        std::optional<FileSpec> folder = folder_provider(true);
        if (!folder.has_value()) {
            return;
        }

        FlatFolderScanBatch batch;
        batch.folder = std::move(*folder);
        batch.scan_started_unix_ns = current_unix_time_nanoseconds();
        try {
            visit_metadata_at(batch.folder.rel_path,
                              false,
                              [&batch, &should_stop](FileSpec spec) {
                                  if (!should_stop()) {
                                      batch.files.push_back(std::move(spec));
                                  }
                              },
                              [&batch, &should_stop](FileSpec spec) {
                                  if (!should_stop()) {
                                      batch.directories.push_back(std::move(spec));
                                  }
                              });
        } catch (const std::exception& ex) {
            batch.failed = true;
            batch.error = ex.what();
        }
        batch.scan_finished_unix_ns = current_unix_time_nanoseconds();
        folder_visitor(std::move(batch));
    }
}

void NfsBackend::scan_flat_folders_streaming(
    std::size_t outstanding_folders,
    const std::function<std::optional<FileSpec>(bool wait_for_work)>& folder_provider,
    const std::function<bool()>& should_stop,
    const std::function<void(FlatFolderScanBatch)>& folder_visitor) const {
    scan_flat_folders(outstanding_folders, folder_provider, should_stop, folder_visitor);
}

FileSpec NfsBackend::load_file(std::string_view rel_path, std::size_t outstanding_requests) const {
    (void)outstanding_requests;
    return load_file(rel_path);
}

std::uint64_t NfsBackend::read_file_discard(std::string_view rel_path,
                                            std::uint64_t declared_size,
                                            std::size_t outstanding_requests) const {
    return read_file_discard(rel_path, declared_size, outstanding_requests, {});
}

std::uint64_t NfsBackend::read_file_discard(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::uint64_t)>& bytes_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    const std::uint64_t bytes_read = file.content.size();
    if (bytes_read != 0 && bytes_visitor) {
        bytes_visitor(bytes_read);
    }
    return bytes_read;
}

std::uint64_t NfsBackend::read_file_stream(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::string_view)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    if (!file.content.empty() && data_visitor) {
        data_visitor(file.content);
    }
    return file.content.size();
}

std::uint64_t NfsBackend::read_file_into(std::string_view rel_path,
                                         std::uint64_t declared_size,
                                         std::byte* destination,
                                         std::size_t destination_bytes) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path);
    if (file.content.size() > destination_bytes) {
        throw std::runtime_error("destination buffer is too small");
    }
    if (!file.content.empty()) {
        std::memcpy(destination, file.content.data(), file.content.size());
    }
    return file.content.size();
}

void NfsBackend::open_close_file(std::string_view rel_path) const {
    const FileSpec file = load_file(rel_path);
    (void)file;
}

PackedSmallFilesReadStats NfsBackend::read_small_files_packed(
    const std::function<std::optional<FileSpec>()>& file_provider,
    RawBufferPool& pool,
    std::size_t max_in_flight_files,
    const std::function<void(BufferHandle, std::uint64_t, std::uint64_t)>& buffer_visitor,
    const std::function<bool()>& should_stop) const {
    (void)max_in_flight_files;
    PackedSmallFilesReadStats stats;
    std::optional<BufferHandle> batch_handle;
    std::uint64_t batch_files = 0;

    const auto flush_batch = [&]() {
        if (!batch_handle.has_value()) {
            return;
        }
        const BufferHandle handle = *batch_handle;
        batch_handle.reset();
        const std::uint64_t batch_bytes = packed_small_file_payload_bytes(data_buffer(pool, handle));
        if (batch_files == 0U) {
            pool.release(handle);
            return;
        }
        if (buffer_visitor) {
            buffer_visitor(handle, batch_files, batch_bytes);
        } else {
            pool.release(handle);
        }
        ++stats.buffers_published;
        batch_files = 0;
    };

    while (!(should_stop && should_stop())) {
        std::optional<FileSpec> file = file_provider ? file_provider() : std::nullopt;
        if (!file.has_value()) {
            break;
        }

        const RecBuf record = make_recbuf(*file);
        const std::uint64_t logical_size = file->declared_size != 0U ? file->declared_size : file->content.size();
        PackedSmallFileMeta meta;
        meta.file_id = record.own_hash;
        meta.folder_hash = record.folder_hash;
        meta.file_size = logical_size;
        meta.mtime = record.mtime;
        meta.mode = record.mode;
        meta.uid = record.uid;
        meta.gid = record.gid;
        meta.rel_path = record.rel_path.view();

        if (!batch_handle.has_value()) {
            batch_handle = pool.acquire_spin();
            reset_packed_small_file_buffer(data_buffer(pool, *batch_handle));
        }

        PackedSmallFileAppend append;
        DataBuffer& buffer = data_buffer(pool, *batch_handle);
        if (!prepare_packed_small_file_append(buffer, meta, static_cast<std::size_t>(logical_size), append)) {
            flush_batch();
            batch_handle = pool.acquire_spin();
            DataBuffer& fresh = data_buffer(pool, *batch_handle);
            reset_packed_small_file_buffer(fresh);
            if (!prepare_packed_small_file_append(fresh, meta, static_cast<std::size_t>(logical_size), append)) {
                pool.release(*batch_handle);
                batch_handle.reset();
                ++stats.files_failed;
                continue;
            }
        }

        const std::uint64_t bytes_read = read_file_into(file->rel_path, logical_size, append.data, append.data_capacity);
        if (bytes_read != logical_size) {
            ++stats.files_failed;
            continue;
        }
        commit_packed_small_file_append(data_buffer(pool, *batch_handle), append);
        ++batch_files;
        ++stats.files_read;
        stats.bytes_read += bytes_read;
    }

    flush_batch();
    return stats;
}

PackedSmallFilesReadStats NfsBackend::read_small_files_raw_window(
    const std::function<std::optional<FileSpec>()>& file_provider,
    RawBufferPool& pool,
    std::size_t max_in_flight_files,
    const std::function<void(RawSmallFileRead&&)>& file_visitor,
    const std::function<bool()>& should_stop) const {
    (void)max_in_flight_files;
    PackedSmallFilesReadStats stats;
    while (!(should_stop && should_stop())) {
        std::optional<FileSpec> file = file_provider ? file_provider() : std::nullopt;
        if (!file.has_value()) {
            break;
        }

        const std::uint64_t logical_size = file->declared_size != 0U ? file->declared_size : file->content.size();
        if (logical_size == 0U) {
            ++stats.files_read;
            continue;
        }
        if (logical_size > kLargeChunkBytes) {
            ++stats.files_failed;
            continue;
        }

        BufferHandle handle = pool.acquire_spin();
        DataBuffer& buffer = data_buffer(pool, handle);
        buffer.trailer = {};
        const std::uint64_t bytes_read =
            read_file_into(file->rel_path, logical_size, buffer.bytes.data(), buffer.bytes.size());
        if (bytes_read != logical_size) {
            pool.release(handle);
            ++stats.files_failed;
            continue;
        }
        buffer.trailer.data_offset = 0;
        buffer.trailer.data_len = static_cast<std::size_t>(bytes_read);

        if (file_visitor) {
            RawSmallFileRead completed;
            completed.file = std::move(*file);
            completed.handle = handle;
            completed.bytes_read = bytes_read;
            file_visitor(std::move(completed));
        } else {
            pool.release(handle);
        }
        ++stats.files_read;
        ++stats.buffers_published;
        stats.bytes_read += bytes_read;
    }
    return stats;
}

std::uint64_t NfsBackend::read_file_owned_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(OwnedFileChunk&&)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    const std::uint64_t bytes_read = file.content.size();
    if (!file.content.empty() && data_visitor) {
        OwnedFileChunk chunk;
        chunk.offset = 0;
        chunk.data = std::move(file.content);
        data_visitor(std::move(chunk));
    }
    return bytes_read;
}

std::uint64_t NfsBackend::read_file_pooled_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    DataSlotPool& pool,
    const std::function<void(PooledFileChunk&&)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    std::uint64_t total = 0;
    while (total < file.content.size()) {
        const std::size_t chunk_size =
            std::min<std::size_t>(kLargeChunkBytes, file.content.size() - static_cast<std::size_t>(total));
        DataSlotHandle handle = pool.acquire_wait_or_throw(DataSlotClass::large, chunk_size);
        std::memcpy(pool.data(handle), file.content.data() + total, chunk_size);
        DataBufTrailer& trailer = pool.trailer(handle);
        trailer.data_offset = total;
        trailer.data_len = chunk_size;

        PooledFileChunk chunk;
        chunk.offset = total;
        chunk.handle = handle;
        total += chunk_size;
        if (data_visitor) {
            data_visitor(std::move(chunk));
        } else {
            pool.release(handle);
        }
    }
    return total;
}

std::uint64_t NfsBackend::read_file_raw_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    RawBufferPool& pool,
    const std::function<void(RawFileChunk&&)>& data_visitor,
    const std::function<bool()>& should_stop,
    bool copy_payload_to_buffer) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    std::uint64_t total = 0;
    while (total < file.content.size() && !(should_stop && should_stop())) {
        const std::size_t chunk_size =
            std::min<std::size_t>(kLargeChunkBytes, file.content.size() - static_cast<std::size_t>(total));
        BufferHandle handle = pool.acquire_spin();
        DataBuffer& buffer = data_buffer(pool, handle);
        if (copy_payload_to_buffer) {
            std::memcpy(buffer.bytes.data(), file.content.data() + total, chunk_size);
        }
        buffer.trailer = {};
        buffer.trailer.data_offset = total;
        buffer.trailer.data_len = chunk_size;

        RawFileChunk chunk;
        chunk.offset = total;
        chunk.handle = handle;
        total += chunk_size;
        if (data_visitor) {
            data_visitor(std::move(chunk));
        } else {
            pool.release(handle);
        }
    }
    return total;
}

std::uint64_t NfsBackend::read_file_raw_chunks_by_handle(
    const FileSpec& file,
    std::size_t outstanding_requests,
    RawBufferPool& pool,
    const std::function<void(RawFileChunk&&)>& data_visitor,
    const std::function<bool()>& should_stop,
    bool copy_payload_to_buffer) const {
    return read_file_raw_chunks(file.rel_path,
                                file.declared_size,
                                outstanding_requests,
                                pool,
                                data_visitor,
                                should_stop,
                                copy_payload_to_buffer);
}

std::uint64_t NfsBackend::visit_file_chunks(
    std::string_view rel_path,
    std::uint64_t declared_size,
    std::size_t outstanding_requests,
    const std::function<void(std::uint64_t offset, std::string_view data)>& data_visitor) const {
    (void)declared_size;
    FileSpec file = load_file(rel_path, outstanding_requests);
    if (!file.content.empty() && data_visitor) {
        data_visitor(0, file.content);
    }
    return file.content.size();
}

std::unique_ptr<NfsBackend> make_nfs_backend(std::string root,
                                             std::size_t endpoint_index,
                                             std::size_t readdirplus_page_bytes) {
    if (is_synthetic_profile_url(root)) {
        (void)endpoint_index;
        (void)readdirplus_page_bytes;
        return std::make_unique<SyntheticProfileBackend>(std::move(root));
    }
    if (is_nfs_url(root)) {
#if HYPERSYNC_HAS_LIBNFS
        return std::make_unique<LibNfsBackend>(std::move(root), endpoint_index, readdirplus_page_bytes);
#else
        (void)endpoint_index;
        (void)readdirplus_page_bytes;
        throw std::runtime_error("libnfs support is not available in this build; install libnfs and rebuild");
#endif
    }
    return std::make_unique<LocalFilesystemBackend>(std::move(root));
}

std::unique_ptr<TargetWriterBackend> make_target_writer_backend(std::string root,
                                                                std::size_t endpoint_index,
                                                                TargetWriterBackend::Options options) {
    if (is_null_url(root)) {
        (void)endpoint_index;
        return std::make_unique<NullTargetWriterBackend>(options);
    }
    if (is_nfs_url(root)) {
#if HYPERSYNC_HAS_LIBNFS
        return std::make_unique<LibNfsTargetWriterBackend>(std::move(root), endpoint_index, options);
#else
        (void)endpoint_index;
        (void)options;
        throw std::runtime_error("libnfs support is not available in this build; install libnfs and rebuild");
#endif
    }
    (void)endpoint_index;
    return std::make_unique<LocalTargetWriterBackend>(std::move(root), options);
}

}  // namespace hypersync
